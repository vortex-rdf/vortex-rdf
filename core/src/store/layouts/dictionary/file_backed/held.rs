//! A chunk decode's hold on a [`FileBackedDict`]'s windows: the terms of a
//! chunk's codes read in place, as a resident dictionary's cursor reads its
//! chunks, each window taken once and held until the decode ends.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use crate::error::{Result, VortexRdfError};
use crate::store::array::trusted_str;
use crate::store::schema::TermCode;

use super::super::storage::{TermChunk, chunk_of};
use super::super::term_dict::check_code;
use super::cache::WindowCache;
use super::{FileBackedDict, Windows};

/// The windows one chunk decode reads in place: each window a code falls in
/// is taken once, without waiting ([`Windows::window_chunk_now`]), and held
/// until the hold drops, charged as the window cache charges it and within
/// the cache's budget. The hold gives up ([`gave_up`](Self::gave_up)) when a
/// window's read would wait, or would take the held windows past the budget:
/// a chunk whose codes scatter over more windows than the cache holds is then
/// read another way, one window at a time.
pub(in crate::store::layouts::dictionary) struct HeldWindows<'a> {
    dict: &'a Windows,
    /// The windows taken so far, by window index.
    held: Vec<Option<Arc<TermChunk>>>,
    /// What the held windows are charged, and the most they may be.
    charged: usize,
    budget: usize,
    gave_up: bool,
}

impl FileBackedDict {
    /// A hold on this dictionary's windows for one chunk decode; `None` once
    /// a window read has had to wait ([`Windows::reads_wait`]).
    pub(in crate::store::layouts::dictionary) fn hold_windows(&self) -> Option<HeldWindows<'_>> {
        let dict = &*self.0;
        if dict.reads_wait.load(Relaxed) {
            return None;
        }
        Some(HeldWindows {
            dict,
            held: vec![None; dict.windows.len()],
            charged: 0,
            budget: dict.cache().budget(),
            gave_up: false,
        })
    }
}

impl HeldWindows<'_> {
    /// The term with `code`, decompressed into `scratch` from an FSST
    /// window: an error when `code` is outside the dictionary, or when the
    /// hold gives up on its window.
    #[inline]
    pub(in crate::store::layouts::dictionary) fn str_at<'s>(
        &'s mut self,
        code: TermCode,
        scratch: &'s mut Vec<u8>,
    ) -> Result<&'s str> {
        let rank = check_code(code, self.dict.code_space)?;
        let (w, local) = chunk_of(&self.dict.starts, rank);
        if self.held[w].is_none() {
            self.take(w)?;
        }
        let chunk = self.held[w].as_deref().expect("window taken above");
        Ok(trusted_str(chunk.bytes_into(local, scratch)))
    }

    /// Whether the hold gave up on a window.
    pub(in crate::store::layouts::dictionary) fn gave_up(&self) -> bool {
        self.gave_up
    }

    /// Take window `w` and hold it; an error, the hold given up, when its
    /// read would wait or the held windows would outgrow the budget.
    fn take(&mut self, w: usize) -> Result<()> {
        let Some(chunk) = self.dict.window_chunk_now(w) else {
            return Err(self.give_up("a window read would wait"));
        };
        let chunk = chunk?;
        let charged = self.charged.saturating_add(WindowCache::charge(&chunk));
        if charged > self.budget {
            return Err(self.give_up("the chunk's windows outgrow the window cache"));
        }
        self.charged = charged;
        self.held[w] = Some(chunk);
        Ok(())
    }

    fn give_up(&mut self, why: &str) -> VortexRdfError {
        self.gave_up = true;
        VortexRdfError::InvalidOperation(format!("in-place decode given up: {why}"))
    }
}
