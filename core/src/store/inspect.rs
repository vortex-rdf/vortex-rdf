//! EXPERIMENT (branch exp/zone-map-runend-point-lookup), not for release: point lookups by term code from what a
//! store file already holds (Vortex zone maps, RunEnd runs, chunk probes), and two inspection helpers (the file's
//! layout and encodings; `(s, p)` pairs sampled from its rows). See EXPERIMENT.md at the repository root.
use super::native_file::NativeStoreFile;
use crate::io::read::{FileAccess, open_vortex_file};
use std::fmt::Write as _;

/// The quad table's layout tree (first `max_lines` lines), and per quad column its chunking and the encodings of
/// three sample chunks.
pub async fn layout_report(path: &str, max_lines: usize) -> crate::Result<String> {
    let file = NativeStoreFile::try_new(open_vortex_file(path, FileAccess::Mapped).await?)?;
    let mut out = String::new();
    let root = file.footer().layout().clone();
    let tree = root.display_tree_verbose(true).to_string();
    let lines: Vec<&str> = tree.lines().collect();
    writeln!(
        out,
        "== root layout tree: {} lines; first {max_lines}:",
        lines.len()
    )
    .ok();
    for l in lines.iter().take(max_lines) {
        writeln!(out, "{l}").ok();
    }
    writeln!(
        out,
        "== components: {:?}",
        file.components()
            .iter()
            .map(|c| (&c.name, &c.implementation))
            .collect::<Vec<_>>()
    )
    .ok();
    for col in ["s", "p", "o", "g"] {
        let Some(chunks) = file.column_chunks(col) else {
            writeln!(out, "== column {col}: no chunk probes (declined)").ok();
            continue;
        };
        let rows = chunks.debug_chunk_rows();
        let mut sizes: Vec<u64> = rows.iter().map(|r| r.1).collect();
        sizes.sort_unstable();
        writeln!(
            out,
            "== column {col}: {chunks:?}; chunk rows min {} median {} max {}",
            sizes[0],
            sizes[sizes.len() / 2],
            sizes[sizes.len() - 1]
        )
        .ok();
        let source = file.segment_source();
        for idx in [0, rows.len() / 2, rows.len() - 1] {
            let a = chunks
                .debug_chunk_array(idx, &source, file.session())
                .await
                .map_err(crate::VortexRdfError::Vortex)?;
            writeln!(
                out,
                "-- chunk {idx} ({} rows, {} bytes):\n{}",
                a.len(),
                a.nbytes(),
                a.display_tree_encodings_only()
            )
            .ok();
        }
    }
    Ok(out)
}

/// Codes read by the prototype for each call: `(o, g)` per matching row.
pub type ProtoRows = Vec<Vec<(u64, u64)>>;

/// EXPERIMENT (branch exp/zone-map-runend-point-lookup): point lookups `(s code, p code)` through today's chunk-probe path (`bounds` then
/// `bounds_in`, as `narrow_subject_run` does) and through a prototype that uses only what the file already
/// holds: the subject column's zone map to pick the chunk, one RunEnd search, then `value_at` reads of `p`, `o`,
/// `g` over the run's few rows. Stage times are means over a second (warm) pass.
pub async fn prototype(path: &str, codes: &[(u64, u64)]) -> crate::Result<(String, ProtoRows)> {
    use crate::VortexRdfError::Vortex as V;
    use crate::io::container::RdfStoreLayoutVTable;
    use std::time::Instant;
    let file = NativeStoreFile::try_new(open_vortex_file(path, FileAccess::Mapped).await?)?;
    let typed = file.footer().layout().as_::<RdfStoreLayoutVTable>();
    let quads = typed.slot(0).map_err(V)?.expect("quad child");
    let (src, ses) = (file.segment_source(), file.session().clone());
    let col = |c: &str| file.column_chunks(c).expect("chunk probes");
    let (s, p, o, g) = (col("s"), col("p"), col("o"), col("g"));
    let t = Instant::now();
    let dir = vortex_rdf_encoded_search::debug_zone_dir(&quads, "s", &src, &ses)
        .await
        .map_err(V)?
        .expect("zone map");
    let dir_ns = t.elapsed().as_nanos();
    let mut out = String::new();
    let mut rows_out = Vec::new();
    let (mut mismatches, mut rows_total) = (0usize, 0usize);
    let mut ns = [0u128; 5];
    for pass in 0..2 {
        ns = [0; 5];
        rows_out.clear();
        for &(sc, pc) in codes {
            let t0 = Instant::now();
            let run = s.bounds(sc, &src, &ses).await.map_err(V)?.expect("s probe");
            let t1 = Instant::now();
            let cur = if run.is_empty() {
                run.clone()
            } else {
                p.bounds_in(run.clone(), pc, &src, &ses)
                    .await
                    .map_err(V)?
                    .expect("p probe")
            };
            let t2 = Instant::now();
            let prun = s
                .debug_point_bounds(&dir, sc, &src, &ses)
                .await
                .map_err(V)?
                .expect("s probe");
            let t3 = Instant::now();
            // The run is sorted by p: the matching rows are contiguous. A linear pass over the run's few rows.
            let (mut a, mut b) = (prun.end, prun.end);
            for r in prun.clone() {
                let v = p.value_at(r, &src, &ses).await.map_err(V)?.expect("p");
                if v == pc && a == prun.end {
                    a = r;
                }
                if v > pc {
                    b = r;
                    break;
                }
            }
            if a == prun.end {
                b = a;
            } else if b == prun.end && a != prun.end {
                b = prun.end;
            }
            let t4 = Instant::now();
            let mut got = Vec::new();
            for r in a..b {
                got.push((
                    o.value_at(r, &src, &ses).await.map_err(V)?.expect("o"),
                    g.value_at(r, &src, &ses).await.map_err(V)?.expect("g"),
                ));
            }
            let t5 = Instant::now();
            for (i, (x, y)) in [(t0, t1), (t1, t2), (t2, t3), (t3, t4), (t4, t5)]
                .into_iter()
                .enumerate()
            {
                ns[i] += (y - x).as_nanos();
            }
            if pass == 1 {
                if (cur.start..cur.end) != (a..b) && !(cur.is_empty() && a == b) {
                    mismatches += 1;
                }
                rows_total += got.len();
            }
            rows_out.push(got);
        }
    }
    let n = codes.len() as f64;
    writeln!(
        out,
        "zone map of s: {} zones of {} rows, read in {:.1} us (once)",
        dir.mins.len(),
        dir.zone_len,
        dir_ns as f64 / 1e3
    )
    .ok();
    writeln!(
        out,
        "today:     subject bounds {:.0} ns + predicate bounds_in {:.0} ns = {:.0} ns",
        ns[0] as f64 / n,
        ns[1] as f64 / n,
        (ns[0] + ns[1]) as f64 / n
    )
    .ok();
    writeln!(out, "prototype: subject via zone map + one RunEnd search {:.0} ns + predicate linear pass {:.0} ns = {:.0} ns; then o,g value_at for {:.2} rows/call {:.0} ns",
        ns[2] as f64 / n, ns[3] as f64 / n, (ns[2] + ns[3]) as f64 / n, rows_total as f64 / n, ns[4] as f64 / n).ok();
    writeln!(
        out,
        "row ranges differing between today and prototype: {mismatches} of {}",
        codes.len()
    )
    .ok();
    Ok((out, rows_out))
}

/// EXPERIMENT (branch exp/zone-map-runend-point-lookup): `n` existing `(s, p)` pairs, read at pseudo-random rows of the file (deterministic), sorted
/// by row like nothing in particular: a stand-in for a workload's point lookups when no call log exists.
pub async fn sample_codes(path: &str, n: usize) -> crate::Result<Vec<(u64, u64)>> {
    use crate::VortexRdfError::Vortex as V;
    let file = NativeStoreFile::try_new(open_vortex_file(path, FileAccess::Mapped).await?)?;
    let (src, ses) = (file.segment_source(), file.session().clone());
    let (s, p) = (
        file.column_chunks("s").expect("s"),
        file.column_chunks("p").expect("p"),
    );
    let rows = s.row_count();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let r = x % rows;
        out.push((
            s.value_at(r, &src, &ses).await.map_err(V)?.expect("s"),
            p.value_at(r, &src, &ses).await.map_err(V)?.expect("p"),
        ));
    }
    Ok(out)
}

/// EXPERIMENT (branch exp/zone-map-runend-point-lookup): the prototype lookup for every bound shape, from the file's zone maps, RunEnd runs and
/// chunk probes. Subject-bound patterns read the quad table; `?p?`/`?po` read `index:posg`; `??o` reads
/// `index:ospg` (copy index: those components hold all four columns). Results are `[s, p, o, g]` rows in the
/// order of the structure read, windowed to `offset..offset+limit`, plus the matching row count.
pub struct Proto {
    file: NativeStoreFile,
    src: std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
    ses: vortex_session::VortexSession,
    quad: Table,
    posg: Option<Table>,
    ospg: Option<Table>,
}

struct Table {
    cols: [std::sync::Arc<vortex_rdf_encoded_search::ColumnChunks>; 4],
    /// Zone map of the table's sort-leading column.
    dir: vortex_rdf_encoded_search::DebugZoneDir,
}

/// Unbound / bound pattern of codes.
pub type Pat = (Option<u64>, Option<u64>, Option<u64>);

impl Proto {
    pub async fn open(path: &str) -> crate::Result<Self> {
        use crate::VortexRdfError::Vortex as V;
        use crate::io::container::RdfStoreLayoutVTable;
        let file = NativeStoreFile::try_new(open_vortex_file(path, FileAccess::Mapped).await?)?;
        let (src, ses) = (file.segment_source(), file.session().clone());
        let quads = file
            .footer()
            .layout()
            .as_::<RdfStoreLayoutVTable>()
            .slot(0)
            .map_err(V)?
            .expect("quads");
        let get = |c: &str| file.column_chunks(c).expect("quad chunks");
        let quad = Table {
            cols: [get("s"), get("p"), get("o"), get("g")],
            dir: vortex_rdf_encoded_search::debug_zone_dir(&quads, "s", &src, &ses)
                .await
                .map_err(V)?
                .expect("zones"),
        };
        let comp = async |name: &str, lead: &str| -> crate::Result<Option<Table>> {
            let Some(layout) = file.component_layout(name).map_err(V)? else {
                return Ok(None);
            };
            let c = |col: &str| file.component_column_chunks(name, col);
            let (Some(s), Some(p), Some(o), Some(g)) = (c("s"), c("p"), c("o"), c("g")) else {
                return Ok(None);
            };
            let Some(dir) = vortex_rdf_encoded_search::debug_zone_dir(&layout, lead, &src, &ses)
                .await
                .map_err(V)?
            else {
                return Ok(None);
            };
            Ok(Some(Table {
                cols: [s, p, o, g],
                dir,
            }))
        };
        let posg = comp("index:posg", "p").await?;
        let ospg = comp("index:ospg", "o").await?;
        Ok(Self {
            file,
            src,
            ses,
            quad,
            posg,
            ospg,
        })
    }

    /// `None` when the prototype does not handle the shape (nothing bound, `s?o`, or a missing component).
    pub async fn lookup(
        &self,
        (s, p, o): Pat,
        offset: u64,
        limit: u64,
    ) -> crate::Result<Option<(u64, Vec<[u64; 4]>)>> {
        use crate::VortexRdfError::Vortex as V;
        let (src, ses) = (&self.src, &self.ses);
        // (table, leading code, then the further bound columns in sort order, column order of the table rows)
        let (t, lead, rest): (&Table, u64, Vec<(usize, u64)>) = match (s, p, o) {
            (Some(s), p, o) => {
                let mut rest = Vec::new();
                if let Some(p) = p {
                    rest.push((1, p));
                    if let Some(o) = o {
                        rest.push((2, o));
                    }
                } else if o.is_some() {
                    return Ok(None);
                }
                (&self.quad, s, rest)
            }
            (None, Some(p), o) => match &self.posg {
                Some(t) => (t, p, o.map(|o| vec![(2, o)]).unwrap_or_default()),
                None => return Ok(None),
            },
            (None, None, Some(o)) => match &self.ospg {
                Some(t) => (t, o, vec![]),
                None => return Ok(None),
            },
            (None, None, None) => return Ok(None),
        };
        let lead_col = match (s, p) {
            (Some(_), _) => 0,
            (None, Some(_)) => 1,
            _ => 2,
        };
        let Some(mut run) = t.cols[lead_col]
            .debug_point_bounds(&t.dir, lead, src, ses)
            .await
            .map_err(V)?
        else {
            return Ok(None);
        };
        for (col, code) in rest {
            if run.is_empty() {
                break;
            }
            let Some(r) = t.cols[col]
                .bounds_in(run.clone(), code, src, ses)
                .await
                .map_err(V)?
            else {
                return Ok(None);
            };
            run = r;
        }
        let total = run.end - run.start;
        let from = (run.start + offset).min(run.end);
        let to = (from + limit).min(run.end);
        let mut rows = Vec::with_capacity((to - from) as usize);
        let bound = [s, p, o];
        for r in from..to {
            let mut row = [0u64; 4];
            for c in 0..4 {
                row[c] = match bound.get(c).copied().flatten() {
                    Some(v) => v,
                    None => t.cols[c]
                        .value_at(r, src, ses)
                        .await
                        .map_err(V)?
                        .expect("value"),
                };
            }
            rows.push(row);
        }
        Ok(Some((total, rows)))
    }

    /// Vortex's own `search_sorted` (generic `ArrayRef` path) for the lower and upper bound of `needle` in the
    /// quad table's subject chunk that the zone map picks: ns for both searches, and whether the run agrees with
    /// the probe's.
    pub async fn vortex_search_sorted(&self, needle: u64) -> crate::Result<(u128, bool)> {
        use crate::VortexRdfError::Vortex as V;
        use vortex_array::search_sorted::{SearchSorted as _, SearchSortedSide};
        let t = &self.quad;
        let z = t
            .dir
            .maxs
            .partition_point(|&m| m < needle)
            .min(t.dir.maxs.len() - 1);
        let row = z as u64 * t.dir.zone_len;
        let rows = t.cols[0].debug_chunk_rows();
        let idx = rows.partition_point(|c| c.0 + c.1 <= row);
        let array = t.cols[0]
            .debug_chunk_array(idx, &self.src, &self.ses)
            .await
            .map_err(V)?;
        let scalar = vortex_array::scalar::Scalar::from(needle);
        let t0 = std::time::Instant::now();
        let lo = array
            .search_sorted(&scalar, SearchSortedSide::Left)
            .map_err(V)?
            .to_index();
        let hi = array
            .search_sorted(&scalar, SearchSortedSide::Right)
            .map_err(V)?
            .to_index();
        let ns = t0.elapsed().as_nanos();
        let probe = t.cols[0]
            .bounds(needle, &self.src, &self.ses)
            .await
            .map_err(V)?
            .expect("bounds");
        Ok((
            ns,
            probe == (rows[idx].0 + lo as u64..rows[idx].0 + hi as u64)
                || (probe.is_empty() && lo == hi),
        ))
    }

    /// The file, for callers comparing against the store's own paths.
    pub fn file_rows(&self) -> u64 {
        self.file.row_count()
    }
}
