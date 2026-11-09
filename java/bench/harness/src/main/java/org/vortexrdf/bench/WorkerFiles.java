package org.vortexrdf.bench;

import java.nio.file.Path;

/**
 * The files a worker reads, all written by {@code java/bench/run.py}: the dataset, its triples projection, the
 * probes, the add and delete batches, and an empty file to build an empty store from.
 */
public record WorkerFiles(Path quads, Path triples, Path probes, Path fresh, Path prefix, Path empty) {
}
