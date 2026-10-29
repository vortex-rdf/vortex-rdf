package org.vortexrdf.bench.rdf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;
import org.vortexrdf.bench.Fixture;
import org.vortexrdf.bench.WorkerFiles;

/**
 * The contract over RDF4J's three stores, and the indexes of the two on disk. They default to spoc and posc,
 * under which an object- or graph-bound pattern is a full scan; every other store here serves those patterns from
 * an index, so these must hold ospc and cspo as well.
 */
class Rdf4jAdapterTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		Rdf4jAdapters factory = new Rdf4jAdapters();
		return factory.slugs().stream().<Adapter<?, ?>>map(factory::create).toList();
	}

	/** The NativeStore records the indexes it built in its {@code triples.prop}. */
	@Test
	void theNativeStoreIndexesEveryPatternPosition() throws Exception {
		Path location = built("rdf4j_native");

		Properties props = new Properties();
		try (Reader r = Files.newBufferedReader(location.resolve("triples.prop"))) {
			props.load(r);
		}
		assertEquals("spoc,posc,ospc,cspo", props.getProperty("triple-indexes"));
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
