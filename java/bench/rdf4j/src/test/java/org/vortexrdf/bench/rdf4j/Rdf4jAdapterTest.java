package org.vortexrdf.bench.rdf4j;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;
import org.vortexrdf.bench.Fixture;
import org.vortexrdf.bench.WorkerFiles;

/**
 * The contract over RDF4J's two stores, and the indexes of the one on disk, LMDB. It defaults to spoc and posc,
 * under which an object- or graph-bound pattern is a full scan; every other store here serves those patterns from
 * an index, so it must hold ospc and cspo as well.
 */
class Rdf4jAdapterTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		Rdf4jAdapters factory = new Rdf4jAdapters();
		return factory.slugs().stream().<Adapter<?, ?>>map(factory::create).toList();
	}

	/** The LMDB store keeps each index as a named database of its triples environment, named after its order. */
	@Test
	void theLmdbStoreIndexesEveryPatternPosition() throws Exception {
		Path location = built("rdf4j_lmdb");

		String names = new String(Files.readAllBytes(location.resolve("triples").resolve("data.mdb")),
				StandardCharsets.ISO_8859_1);
		for (String index : List.of("spoc", "posc", "ospc", "cspo")) {
			assertTrue(names.contains(index + "-inf"), "no " + index + " index in the LMDB store");
		}
	}

	private Path built(String slug) throws Exception {
		WorkerFiles f = Fixture.extract(tmp.resolve("fixture"));
		Rdf4jAdapter adapter = (Rdf4jAdapter) new Rdf4jAdapters().create(slug);
		Path location = Files.createDirectories(tmp.resolve(slug));
		adapter.dispose(adapter.build(f.quads(), location));
		return location;
	}
}
