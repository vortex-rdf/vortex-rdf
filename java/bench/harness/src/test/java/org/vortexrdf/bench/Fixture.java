package org.vortexrdf.bench;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

/**
 * The committed 512-row fixture (written by {@code python3 java/bench/run.py --fixture}) and the rows each of its
 * probes matches, counted by hand from the generator: row i is subject i % 51, predicate i % 32, object i % 257 and
 * graph i % 11, so S matches 11 rows, P 16, O 2, G 47, and each conjunction only row 0.
 */
public final class Fixture {
	public static final Map<String, Long> MATCHES = Map.of("S", 11L, "P", 16L, "O", 2L, "SP", 1L, "PO", 1L, "SPO",
			1L, "G", 47L, "SPOG", 1L, "full", 512L);

	private static final String[] FILES = { "quads.nq", "triples.nt", "probes.tsv", "fresh.nt", "prefix.nq" };

	private Fixture() {
	}

	/** Copy the fixture into {@code dir}, with an empty {@code empty.nt} beside it. */
	public static WorkerFiles extract(Path dir) throws IOException {
		Files.createDirectories(dir);
		for (String name : FILES) {
			try (InputStream in = Fixture.class.getResourceAsStream("/fixture/" + name)) {
				if (in == null) {
					throw new IOException("fixture file missing from the classpath: " + name);
				}
				Files.copy(in, dir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
			}
		}
		Files.writeString(dir.resolve("empty.nt"), "");
		return new WorkerFiles(dir.resolve("quads.nq"), dir.resolve("triples.nt"), dir.resolve("probes.tsv"),
				dir.resolve("fresh.nt"), dir.resolve("prefix.nq"), dir.resolve("empty.nt"));
	}
}
