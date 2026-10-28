package org.vortexrdf.bench.qendpoint;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterContract;

/** The contract over qEndpoint's HDT, graph probes included. */
class QendpointAdapterTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		return List.of(new QendpointAdapters().create("qendpoint_hdt"));
	}
}
