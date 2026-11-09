package org.vortexrdf.bench.vortex;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;

/**
 * The contract over all six Vortex builds. Needs this tree's release builds:
 * {@code -Dvortexrdf.library=target/release/libvortex_rdf_jni.so -Dvortexrdf.cli=target/release/vortex-rdf-cli}.
 */
class VortexAdapterTest extends AdapterContract {
	@BeforeAll
	static void nativeBuilds() {
		assumeTrue(System.getProperty("vortexrdf.library") != null, "set -Dvortexrdf.library");
		assumeTrue(System.getProperty("vortexrdf.cli") != null, "set -Dvortexrdf.cli");
	}

	@Override
	protected List<Adapter<?, ?>> adapters() {
		VortexAdapters factory = new VortexAdapters();
		return factory.slugs().stream().<Adapter<?, ?>>map(factory::create).toList();
	}
}
