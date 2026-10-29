package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * What every adapter must do on the fixture, whichever library it drives: answer every probe it supports with the
 * fixture's row count through both of its count paths, answer the same after a reopen, move the full count by
 * exactly the batch it adds or deletes, and read the terms it materializes. Each library module's test extends it.
 */
public abstract class AdapterContract {
	@TempDir
	protected Path tmp;

	/** The adapters under test, one dynamic test each. */
	protected abstract List<Adapter<?, ?>> adapters();

	@TestFactory
	Stream<DynamicTest> everyProbeCountsWhatTheFixtureHolds() throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		List<Pattern> probes = Inputs.probes(f.probes());
		return adapters().stream().map(a -> dynamicTest(a.slug(), () -> countsMatch(a, f, probes)));
	}

	@TestFactory
	Stream<DynamicTest> aReopenedStoreAnswersTheSame() throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		Pattern s = Inputs.probes(f.probes()).get(0);
		return adapters().stream().filter(Adapter::persistent)
				.map(a -> dynamicTest(a.slug(), () -> reopenedCountsMatch(a, f, s)));
	}

	@TestFactory
	Stream<DynamicTest> addAndDeleteMoveTheFullCount() throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		Pattern full = Inputs.probes(f.probes()).get(8);
		return adapters().stream().filter(a -> a.mutationUnsupported() == null)
				.map(a -> dynamicTest(a.slug(), () -> mutationsMoveTheCount(a, f, full)));
	}

	@TestFactory
	Stream<DynamicTest> materializingReadsTerms() throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		Pattern s = Inputs.probes(f.probes()).get(0);
		return adapters().stream().map(a -> dynamicTest(a.slug(), () -> termsAreRead(a, f, s)));
	}

	private <H, Q> void countsMatch(Adapter<H, Q> a, WorkerFiles f, List<Pattern> probes) throws Exception {
		H h = a.build(source(a, f), location(a, "counts"));
		try {
			for (Pattern p : probes) {
				if (p.kind() == Pattern.Kind.QUADS && !a.supportsQuads()) {
					continue;
				}
				Q q = a.prepare(p);
				assertEquals(Fixture.MATCHES.get(p.name()), a.count(h, q), a.slug() + " count " + p.name());
				assertEquals(Fixture.MATCHES.get(p.name()), a.countOnly(h, q), a.slug() + " countOnly " + p.name());
			}
		} finally {
			a.dispose(h);
		}
	}

	private <H, Q> void reopenedCountsMatch(Adapter<H, Q> a, WorkerFiles f, Pattern s) throws Exception {
		Path location = location(a, "reopen");
		a.dispose(a.build(source(a, f), location));
		H h = a.open(source(a, f), location);
		try {
			Q q = a.prepare(s);
			assertEquals(11L, a.count(h, q), a.slug() + " count S after a reopen");
			assertEquals(11L, a.countOnly(h, q), a.slug() + " countOnly S after a reopen");
		} finally {
			a.dispose(h);
		}
	}

	private <H, Q> void mutationsMoveTheCount(Adapter<H, Q> a, WorkerFiles f, Pattern full) throws Exception {
		H empty = a.build(f.empty(), location(a, "add"));
		try {
			a.add(empty, Inputs.rows(f.fresh()));
			Q q = a.prepare(full);
			assertEquals(16L, a.countOnly(empty, q), a.slug() + " full after adding 16 rows to an empty store");
			assertEquals(16L, a.count(empty, q), a.slug() + " materialized full after the add");
		} finally {
			a.dispose(empty);
		}
		H whole = a.build(source(a, f), location(a, "delete"));
		try {
			a.delete(whole, Inputs.rows(f.prefix()));
			assertEquals(496L, a.countOnly(whole, a.prepare(full)), a.slug() + " full after deleting 16 rows");
		} finally {
			a.dispose(whole);
		}
	}

	private <H, Q> void termsAreRead(Adapter<H, Q> a, WorkerFiles f, Pattern s) throws Exception {
		H h = a.build(source(a, f), location(a, "terms"));
		try {
			long before = Sink.published();
			a.count(h, a.prepare(s));
			assertTrue(Sink.published() > before, a.slug() + " folded no term lengths into the sink");
		} finally {
			a.dispose(h);
		}
	}

	private static Path source(Adapter<?, ?> a, WorkerFiles f) {
		return a.supportsQuads() ? f.quads() : f.triples();
	}

	private Path location(Adapter<?, ?> a, String use) throws Exception {
		return Files.createDirectories(tmp.resolve(a.slug()).resolve(use));
	}
}
