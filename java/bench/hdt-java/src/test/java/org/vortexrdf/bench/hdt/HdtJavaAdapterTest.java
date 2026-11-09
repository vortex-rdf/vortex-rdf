package org.vortexrdf.bench.hdt;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;

/** The contract over hdt-java, which reads the fixture's triples projection. */
class HdtJavaAdapterTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		return List.of(new HdtJavaAdapters().create("hdt_java"));
	}
}
