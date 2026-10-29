package org.vortexrdf.bench.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.sparql.core.DatasetGraph;
import org.junit.jupiter.api.Test;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;
import org.vortexrdf.bench.Fixture;
import org.vortexrdf.bench.Inputs;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.WorkerFiles;

class JenaAdapterTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		JenaAdapters factory = new JenaAdapters();
		return factory.slugs().stream().<Adapter<?, ?>>map(factory::create).toList();
	}

	/**
	 * TDB2 keeps an in-process connection per location. If dispose left it cached, a reopen would answer from the
	 * old dataset, and every Open and cold cell would time a warm, cached store instead of a reopen.
	 */
	@Test
	void aDisposedTdb2StoreReopensFromDisk() throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		Pattern full = Inputs.probes(f.probes()).get(8);
		JenaTdb2Adapter tdb2 = new JenaTdb2Adapter();
		Path location = Files.createDirectories(tmp.resolve("tdb2"));
		tdb2.dispose(tdb2.build(f.quads(), location));

		// A 16-row database takes the 512-row one's place on disk.
		Path other = Files.createDirectories(tmp.resolve("other"));
		tdb2.dispose(tdb2.build(f.fresh(), other));
		deleteTree(location);
		Files.move(other, location);

		DatasetGraph reopened = tdb2.open(f.quads(), location);
		try {
			assertEquals(16L, tdb2.countOnly(reopened, tdb2.prepare(full)));
		} finally {
			tdb2.dispose(reopened);
		}
	}

	private static void deleteTree(Path dir) throws IOException {
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
				Files.delete(p);
			}
		}
	}
}
