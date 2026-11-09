package org.vortexrdf.bench;

import java.util.List;

/** The contract holds for the test store in each of its shapes. */
class ListAdapterContractTest extends AdapterContract {
	@Override
	protected List<Adapter<?, ?>> adapters() {
		return List.of(ListAdapter.persistent("list"), ListAdapter.inMemory("list_mem"),
				ListAdapter.triplesOnly("list_triples"));
	}
}
