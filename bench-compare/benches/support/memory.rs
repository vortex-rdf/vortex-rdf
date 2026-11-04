//! The memory role's helpers: a `/proc/self/status` sampler and the contract's
//! memory run and entry. Std and serde only, so `tests/memory.rs` can include this
//! file by `#[path]`: a `#[test]` inside the `harness = false` bench never runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

/// A process's resident set, split the way the kernel splits it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rss {
    /// `RssAnon`: heap and anonymous mappings, in kB.
    pub anon_kb: i64,
    /// `RssFile`: file-backed mappings (a mapped store), in kB.
    pub file_kb: i64,
}

impl Rss {
    pub fn total_kb(self) -> i64 {
        self.anon_kb + self.file_kb
    }
}

/// Reads the `kB` figure of `key` (`RssAnon:`) from a `/proc/<pid>/status` text.
fn status_kb(status: &str, key: &str) -> Option<i64> {
    let rest = status.lines().find_map(|l| l.strip_prefix(key))?;
    rest.split_whitespace().next()?.parse().ok()
}

/// The `RssAnon` and `RssFile` of a `/proc/<pid>/status` text; `None` unless both are present.
pub fn parse_rss(status: &str) -> Option<Rss> {
    Some(Rss {
        anon_kb: status_kb(status, "RssAnon:")?,
        file_kb: status_kb(status, "RssFile:")?,
    })
}

/// This process's current `RssAnon` and `RssFile`.
pub fn read_rss() -> Rss {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| parse_rss(&s))
        .expect("cannot read RssAnon and RssFile from /proc/self/status")
}

/// The sample with the higher `anon + file`; a tie keeps the earlier one.
pub fn keep_peak(peak: Rss, sample: Rss) -> Rss {
    if sample.total_kb() > peak.total_kb() {
        sample
    } else {
        peak
    }
}

pub const SAMPLE_EVERY: Duration = Duration::from_millis(2);

/// A thread sampling `/proc/self/status` every [`SAMPLE_EVERY`].
pub struct Sampler {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<Rss>,
}

impl Sampler {
    pub fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let mut peak = read_rss();
            loop {
                let last = flag.load(Ordering::Acquire);
                peak = keep_peak(peak, read_rss());
                if last {
                    return peak;
                }
                std::thread::sleep(SAMPLE_EVERY);
            }
        });
        Self { stop, thread }
    }

    /// Stops the loop, which samples once more after it sees the flag, so the
    /// result covers the memory held when this is called.
    pub fn finish(self) -> Rss {
        self.stop.store(true, Ordering::Release);
        self.thread.join().expect("the RSS sampler panicked")
    }
}

/// MiB to 1 decimal; `+ 0.0` turns -0.0 into 0.0.
pub fn mib(bytes: i64) -> f64 {
    (bytes as f64 / (1u64 << 20) as f64 * 10.0).round() / 10.0 + 0.0
}

/// One memory run's figures, in MiB: the contract's `runs` element.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRun {
    pub store_mb: f64,
    pub peak_anon_mb: f64,
    pub peak_file_mb: f64,
}

impl MemoryRun {
    /// Every figure is clamped at 0: allocator noise can make a difference negative.
    pub fn new(store_bytes: i64, base: Rss, peak: Rss) -> Self {
        let clamp = |v: f64| v.max(0.0);
        Self {
            store_mb: clamp(mib(store_bytes)),
            peak_anon_mb: clamp(mib((peak.anon_kb - base.anon_kb) * 1024)),
            peak_file_mb: clamp(mib((peak.file_kb - base.file_kb) * 1024)),
        }
    }
}

/// How an adapter got its store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Obtained {
    Mapped,
    Loaded,
    Built,
    Opened,
}

/// An adapter's `role: "memory"` entry; `runs` may be empty.
pub fn memory_entry(
    slug: &str,
    label: &str,
    obtained: Obtained,
    runs: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({"slug": slug, "label": label, "role": "memory", "obtained": obtained, "runs": runs})
}
