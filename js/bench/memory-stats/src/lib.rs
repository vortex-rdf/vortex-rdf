//! A counting global allocator for the benchmark dashboard's memory panels.
//!
//! [`CountingAlloc`] wraps an inner allocator and tracks live heap bytes. The
//! counters are signed: counting is off until [`set_counting`]`(true)`, so a
//! free of memory allocated before that point drives the count negative, and
//! only differences are meaningful. With counting off, the cost of an
//! allocation is one relaxed load.
//!
//! This crate is bench-only and never published. With the `wasm` feature it
//! also exports `setMemoryCounting`, `memoryStats` and `resetMemoryPeak` to JS.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};

struct Counters {
    counting: AtomicBool,
    live: AtomicI64,
    peak: AtomicI64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            counting: AtomicBool::new(false),
            live: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        }
    }

    fn set_counting(&self, on: bool) {
        self.counting.store(on, Relaxed);
    }

    fn allocated(&self, bytes: usize) {
        if self.counting.load(Relaxed) {
            let live = self.live.fetch_add(bytes as i64, Relaxed) + bytes as i64;
            self.peak.fetch_max(live, Relaxed);
        }
    }

    fn freed(&self, bytes: usize) {
        if self.counting.load(Relaxed) {
            self.live.fetch_sub(bytes as i64, Relaxed);
        }
    }

    fn live(&self) -> i64 {
        self.live.load(Relaxed)
    }

    fn peak(&self) -> i64 {
        self.peak.load(Relaxed)
    }

    fn reset_peak(&self) {
        self.peak.store(self.live.load(Relaxed), Relaxed);
    }
}

static COUNTERS: Counters = Counters::new();

/// Turn counting on or off. Off by default.
pub fn set_counting(on: bool) {
    COUNTERS.set_counting(on);
}

/// Heap bytes allocated minus freed since counting began (signed).
pub fn live_bytes() -> i64 {
    COUNTERS.live()
}

/// The highest [`live_bytes`] since counting began or the last [`reset_peak`].
pub fn peak_live_bytes() -> i64 {
    COUNTERS.peak()
}

/// Set the peak to the current live bytes.
pub fn reset_peak() {
    COUNTERS.reset_peak();
}

/// A global allocator that forwards to `A` and counts the bytes it hands out.
pub struct CountingAlloc<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if !ptr.is_null() {
            COUNTERS.allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc_zeroed(layout) };
        if !ptr.is_null() {
            COUNTERS.allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.0.dealloc(ptr, layout) };
        COUNTERS.freed(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { self.0.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            if new_size >= layout.size() {
                COUNTERS.allocated(new_size - layout.size());
            } else {
                COUNTERS.freed(layout.size() - new_size);
            }
        }
        new
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm {
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen(js_name = setMemoryCounting)]
    pub fn set_memory_counting(on: bool) {
        super::set_counting(on);
    }

    #[wasm_bindgen(
        js_name = memoryStats,
        unchecked_return_type = "{ liveBytes: number, peakLiveBytes: number, heapBytes: number }"
    )]
    pub fn memory_stats() -> Result<JsValue, JsValue> {
        let object = js_sys::Object::new();
        // `memory_size` is in 64 KiB pages and equals `memory.buffer.byteLength`;
        // the f64 product avoids a usize overflow at 4 GiB.
        let heap_bytes = core::arch::wasm32::memory_size::<0>() as f64 * 65_536.0;
        let fields = [
            ("liveBytes", super::live_bytes() as f64),
            ("peakLiveBytes", super::peak_live_bytes() as f64),
            ("heapBytes", heap_bytes),
        ];
        for (key, value) in fields {
            js_sys::Reflect::set(&object, &key.into(), &value.into())?;
        }
        Ok(object.into())
    }

    #[wasm_bindgen(js_name = resetMemoryPeak)]
    pub fn reset_memory_peak() {
        super::reset_peak();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::System;

    #[test]
    fn counting_is_off_by_default() {
        let c = Counters::new();
        c.allocated(4096);
        c.freed(1024);
        assert_eq!((c.live(), c.peak()), (0, 0));
        c.set_counting(true);
        c.allocated(4096);
        assert_eq!((c.live(), c.peak()), (4096, 4096));
    }

    #[test]
    fn live_bytes_are_signed() {
        let c = Counters::new();
        c.set_counting(true);
        c.freed(4096); // allocated before counting began
        assert_eq!((c.live(), c.peak()), (-4096, 0));
        c.allocated(1024);
        assert_eq!(c.live(), -3072);
    }

    #[test]
    fn peak_holds_the_highest_live_bytes_until_reset() {
        let c = Counters::new();
        c.set_counting(true);
        c.allocated(300);
        c.freed(200);
        assert_eq!((c.live(), c.peak()), (100, 300));
        c.reset_peak();
        assert_eq!(c.peak(), 100);
        c.allocated(10);
        c.freed(500);
        assert_eq!((c.live(), c.peak()), (-390, 110));
    }

    // The only test on the globals; this binary's own allocator is System.
    #[test]
    fn counting_alloc_moves_the_global_counters() {
        const MIB: usize = 1 << 20;
        let a = CountingAlloc(System);
        let at = |n| Layout::from_size_align(n, 8).unwrap();
        set_counting(true);
        let start = live_bytes();
        unsafe {
            let b = a.alloc(at(MIB));
            assert_eq!(live_bytes(), start + MIB as i64);
            let b = a.realloc(b, at(MIB), 3 * MIB);
            assert_eq!(live_bytes(), start + 3 * MIB as i64);
            let b = a.realloc(b, at(3 * MIB), MIB);
            assert_eq!(live_bytes(), start + MIB as i64);
            a.dealloc(b, at(MIB));
            let z = a.alloc_zeroed(at(MIB));
            assert_eq!(*z, 0);
            a.dealloc(z, at(MIB));
        }
        assert_eq!(
            (live_bytes(), peak_live_bytes()),
            (start, start + 3 * MIB as i64)
        );

        // A failed allocation or reallocation changes nothing.
        let failing = CountingAlloc(Failing);
        unsafe {
            assert!(failing.alloc(at(MIB)).is_null());
            assert_eq!(live_bytes(), start);
            // `Failing::realloc` never dereferences the pointer, so a dangling one is fine.
            let dangling = std::ptr::NonNull::<u8>::dangling().as_ptr();
            assert!(failing.realloc(dangling, at(MIB), 2 * MIB).is_null());
            assert_eq!(live_bytes(), start);
        }
        reset_peak();
        assert_eq!(peak_live_bytes(), start);
        set_counting(false);
    }

    /// An allocator whose every allocation fails.
    struct Failing;

    unsafe impl GlobalAlloc for Failing {
        unsafe fn alloc(&self, _: Layout) -> *mut u8 {
            std::ptr::null_mut()
        }
        unsafe fn dealloc(&self, _: *mut u8, _: Layout) {
            unreachable!()
        }
        unsafe fn realloc(&self, _: *mut u8, _: Layout, _: usize) -> *mut u8 {
            std::ptr::null_mut()
        }
    }
}
