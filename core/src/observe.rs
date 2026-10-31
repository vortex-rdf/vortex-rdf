//! Observation points: the places in the store's code that tests watch to
//! tell which mechanism answered a call, recorded as `Event`s on the test's
//! thread.
//!
//! Production code marks a point with [`observe!`], which compiles to nothing
//! outside tests; in them it builds its event only while one is recorded. A
//! test starts a [`Recorder`] on its thread: while one lives, every event the
//! thread reaches lands in a thread-local log, and the test reads the events
//! since its own start, or since its last [`take`](Recorder::take). With no
//! recorder alive nothing is kept. Recorders nest, each reading its own span
//! of the log.
//!
//! The log is the thread's own: a test runs its store calls on its thread's
//! current-thread runtime, which polls every task the calls spawn there too,
//! and [`Recorder::start`] refuses a multi-thread runtime, whose workers'
//! events it would miss. Values a test forces on production code (a split
//! limit, a failing cut, a row-id base) are guards of
//! [`Override`](crate::store::test_hooks::Override), not events.

#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
use std::ops::Range;
#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
use crate::kernels::Kernel;
#[cfg(all(test, feature = "file-io"))]
use crate::store::scan::read_plan::MaskedRead;

/// One point a test watches, reached.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    // ── I/O ──────────────────────────────────────────────────────────────
    /// A file mapped (`io::file`).
    FileMapped,
    /// A split future spawned onto the runtime (`io::read::spawned_splits`).
    SplitSpawned,

    // ── Kernels ──────────────────────────────────────────────────────────
    /// One of the store's own kernels answered an execution (`kernels`).
    Kernel(Kernel),

    // ── Building ─────────────────────────────────────────────────────────
    /// A slice cut off an array by `rechunk`.
    RechunkCut,
    /// An out-of-core build spilled its runs under this directory.
    Spilled(PathBuf),
    /// A term copied into a build's interner, on its first sight.
    TermInterned,
    /// A dictionary rebuilt from a build's codes (`rebuild_from_codes`).
    DictionaryRebuilt,
    /// An untouched file-backed store's bytes copied as they are.
    BytesCopied,
    /// A store's live rows gathered into raw quads (`live_raw_quads`).
    RawQuadsGathered,
    /// A shared code column decoded into its cache, once.
    SharedDecoded,

    // ── The dictionary ───────────────────────────────────────────────────
    /// A term read into a dictionary cursor's scratch.
    ScratchRead,
    /// A cursor's scratch grown to hold a term: `fresh` when it held nothing
    /// before.
    ScratchGrown { fresh: bool },
    /// A pattern term resolved to its code by the match prelude.
    TermResolved,

    // ── Matching ─────────────────────────────────────────────────────────
    /// An in-memory index component's `rid` column decoded, this many rows.
    RidsDecoded(usize),
    /// A pending selection's ids sorted into base order, this many: a
    /// `LazyRowIds` filling its sorted cache.
    IdsSorted(usize),
    /// A typed residual's or a component's probe bound a column: `cached`
    /// when the store's own probe answered.
    ProbeBound { cached: bool },
    /// A residual narrowed by zone bounds.
    ZoneNarrowed,
    /// A residual compare left pending.
    ResidualDeferred,
    /// A piece of a pending residual compared, of this many rows.
    ResidualPiece(usize),
    /// A chained match merged into one match of both patterns.
    ChainMerged,
    /// A tail matched: `keyed` through its keys, else by a scan.
    TailMatch { keyed: bool },
    /// Tail rows keyed, this many.
    TailKeyed(usize),
    /// A file index child's row ids scanned.
    RidScan,
    /// A served run read point by point.
    ServePointRead,
    /// A code-chunk stream read off its serving index's located run.
    ServedRunChunks,
    /// A served file run its index did not locate read once by row index,
    /// its rows' child positions and row ids.
    ServedRunLocated,
    /// A stream's pending filter read by its masks, a split at a time.
    MasksFirstStream,

    // ── Narrowing ────────────────────────────────────────────────────────
    /// A window built past the checks that find it is the view as it is.
    Window,
    /// An in-memory keep answered by a sorted column's runs.
    SortedRunKeep,
    /// An in-memory keep decoded a piece of its column.
    KeepPieceDecoded,
    /// An in-memory keep probed this many rows.
    KeepRowsProbed(usize),
    /// A live count made under tombstones.
    LiveCount,

    // ── Reading ──────────────────────────────────────────────────────────
    /// A chunk stream built.
    ChunkStream,
    /// A chunk decoded on its stream, its split task having left it, of this
    /// many rows.
    StreamDecode(usize),
    /// An in-memory gather of a window of ids: `width`, the columns of a row
    /// gather; `None` for a code gather.
    Gather { width: Option<usize> },
    /// A cut of an in-memory run's code buffers.
    RunCut,
    /// A gather from code primitives: all four columns, or one.
    CodeGather { all_four: bool },
    /// The columns a read by id or a masked read read.
    ColumnsRead(Vec<String>),
    /// A take by id: through the chunk probes, else split by split.
    Take { through_chunks: bool },
    /// A row scan run (`read_all_rows`, `stream_rows`).
    RowScan,
    /// A restricted file scan built with a filter pushed down.
    FilteredScan,
    /// An `ExactRead` chosen.
    ExactRead,
    /// A read of a filter's kept rows, of this kind.
    #[cfg(feature = "file-io")]
    MaskedRead(MaskedRead),
    /// A filter evaluated over one split.
    SplitEvaluated,
    /// A split's mask the in-order fold handed on, of this range.
    SplitConsumed(Range<u64>),
}

/// Marks a point a test watches: `observe!(Variant…)` records
/// `Event::Variant…` while a [`Recorder`] lives on this thread, and
/// `observe!(when cond, Variant…)` only where `cond` holds. In builds other
/// than tests it compiles to nothing, the event and the condition included.
macro_rules! observe {
    (when $cond:expr, $($event:tt)*) => {
        #[cfg(test)]
        {
            if $crate::observe::recording() && $cond {
                $crate::observe::note($crate::observe::Event::$($event)*);
            }
        }
    };
    ($($event:tt)*) => {
        #[cfg(test)]
        {
            if $crate::observe::recording() {
                $crate::observe::note($crate::observe::Event::$($event)*);
            }
        }
    };
}
pub(crate) use observe;

/// Whether a [`Recorder`] lives on this thread, so an event is kept.
#[cfg(test)]
pub(crate) fn recording() -> bool {
    LOG.with_borrow(|log| log.recorders > 0)
}

/// Records `event` on this thread; what [`observe!`] calls while
/// [`recording`].
#[cfg(test)]
pub(crate) fn note(event: Event) {
    LOG.with_borrow_mut(|log| log.events.push(event));
}

#[cfg(test)]
thread_local! {
    static LOG: RefCell<Log> = const {
        RefCell::new(Log {
            recorders: 0,
            events: Vec::new(),
        })
    };
}

/// This thread's events, kept while a recorder lives.
#[cfg(test)]
struct Log {
    recorders: usize,
    events: Vec<Event>,
}

/// While alive, this thread's events are recorded; the recorder reads the
/// ones since it started, or since its last [`take`](Self::take).
#[cfg(test)]
pub(crate) struct Recorder {
    from: usize,
}

#[cfg(test)]
impl Recorder {
    /// Records this thread's events from now on. Panics on a multi-thread
    /// runtime, whose workers record nothing here.
    pub(crate) fn start() -> Self {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            assert_eq!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread,
                "probes record on the test's thread: run the test on a current-thread runtime"
            );
        }
        LOG.with_borrow_mut(|log| {
            log.recorders += 1;
            Self {
                from: log.events.len(),
            }
        })
    }

    /// The events since this recorder started or last took.
    pub(crate) fn events(&self) -> Vec<Event> {
        LOG.with_borrow(|log| log.events[self.from..].to_vec())
    }

    /// The events since this recorder started or last took, after which it
    /// reads only later ones.
    pub(crate) fn take(&mut self) -> Vec<Event> {
        let events = self.events();
        self.from += events.len();
        events
    }

    /// How many of [`events`](Self::events) `event` is.
    pub(crate) fn count(&self, event: &Event) -> usize {
        self.count_where(|e| e == event)
    }

    /// How many of [`events`](Self::events) `matches`.
    pub(crate) fn count_where(&self, matches: impl Fn(&Event) -> bool) -> usize {
        LOG.with_borrow(|log| {
            log.events[self.from..]
                .iter()
                .filter(|e| matches(e))
                .count()
        })
    }

    /// What `pick` reads off each of [`events`](Self::events) it picks, in
    /// order.
    pub(crate) fn picked<T>(&self, pick: impl Fn(&Event) -> Option<T>) -> Vec<T> {
        LOG.with_borrow(|log| log.events[self.from..].iter().filter_map(pick).collect())
    }

    /// How many of the events since the last take `event` is, after which
    /// the recorder reads only later ones.
    pub(crate) fn take_count(&mut self, event: &Event) -> usize {
        self.take().iter().filter(|e| *e == event).count()
    }

    /// What `pick` reads off each event since the last take it picks, in
    /// order, after which the recorder reads only later ones.
    pub(crate) fn take_picked<T>(&mut self, pick: impl Fn(&Event) -> Option<T>) -> Vec<T> {
        self.take().iter().filter_map(pick).collect()
    }
}

#[cfg(test)]
impl Drop for Recorder {
    fn drop(&mut self) {
        LOG.with_borrow_mut(|log| {
            log.recorders -= 1;
            if log.recorders == 0 {
                log.events.clear();
            }
        });
    }
}
