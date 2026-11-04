#[path = "../benches/support/memory.rs"]
mod memory;

use memory::{
    MemoryRun, Obtained, Rss, Sampler, keep_peak, memory_entry, mib, parse_rss, read_rss,
};
use serde_json::{Value, json, to_value};

fn rss(anon_kb: i64, file_kb: i64) -> Rss {
    Rss { anon_kb, file_kb }
}

#[test]
fn parse_rss_reads_rss_anon_and_rss_file() {
    let status = "VmRSS:\t  150000 kB\nRssAnon:\t  100000 kB\nRssFile:\t   49000 kB\nRssShmem:\t    1000 kB\n";
    assert_eq!(parse_rss(status), Some(rss(100_000, 49_000)));
    assert_eq!(parse_rss("VmRSS:\t  150000 kB\n"), None);
}

#[test]
fn keep_peak_keeps_the_first_highest_anon_plus_file() {
    let s = [rss(100, 50), rss(90, 70), rss(120, 10), rss(80, 80)];
    assert_eq!(
        s[1..].iter().fold(s[0], |p, &x| keep_peak(p, x)),
        rss(90, 70)
    );
}

#[test]
fn mib_rounds_to_one_decimal_and_never_to_negative_zero() {
    let got = [8 << 20, 1_100_000, -1_572_864, -1].map(|b| mib(b).to_string());
    assert_eq!(got, ["8", "1", "-1.5", "0"]);
}

#[test]
fn memory_entry_has_the_contract_shape() {
    let run = to_value(MemoryRun::new(
        17_196_646,
        rss(100_000, 49_000),
        rss(151_200, 49_000),
    ))
    .unwrap();
    let want = json!({"slug": "vortex_dict_mem", "label": "Vortex Dict (in-memory)", "role": "memory",
        "obtained": "loaded", "runs": [{"storeMb": 16.4, "peakAnonMb": 50.0, "peakFileMb": 0.0}]});
    assert_eq!(
        memory_entry(
            "vortex_dict_mem",
            "Vortex Dict (in-memory)",
            Some(Obtained::Loaded),
            vec![run]
        ),
        want
    );
    // No run kept: `obtained` is null, as on every tab.
    assert_eq!(
        memory_entry("hdt", "hdt", None, Vec::new()),
        json!({"slug": "hdt", "label": "hdt", "role": "memory", "obtained": null, "runs": []})
    );
    let spelled = [
        Obtained::Mapped,
        Obtained::Loaded,
        Obtained::Built,
        Obtained::Opened,
    ]
    .map(|o| to_value(o).unwrap());
    assert_eq!(
        spelled,
        ["mapped", "loaded", "built", "opened"].map(Value::from)
    );
}

#[test]
fn memory_run_clamps_negative_differences_to_zero() {
    // Allocator noise: the dispose released less than the store held, and the peak sat below the baseline.
    let run = to_value(MemoryRun::new(
        -1_572_864,
        rss(100_000, 49_000),
        rss(99_000, 48_000),
    ))
    .unwrap();
    assert_eq!(
        run,
        json!({"storeMb": 0.0, "peakAnonMb": 0.0, "peakFileMb": 0.0})
    );
}

#[test]
#[cfg_attr(not(target_os = "linux"), ignore = "reads /proc/self/status")]
fn sampler_sees_a_block_held_when_it_finishes() {
    let base = read_rss();
    let sampler = Sampler::start();
    let block = std::hint::black_box(vec![1u8; 64 << 20]);
    let peak = sampler.finish();
    drop(block);
    // 48 of its 64 MiB: the kernel's RSS counters can lag by a few pages per CPU.
    assert!(
        peak.anon_kb - base.anon_kb >= 48 << 10,
        "{base:?} -> {peak:?}"
    );
}
