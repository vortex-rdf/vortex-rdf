package org.vortexrdf.bench.vortex;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterFactory;

/**
 * The six Vortex builds of the Python tab, with its slugs and labels, so the two tabs' Vortex rows line up one for
 * one: the Dictionary layout with no secondary index, by copy or by reference, each read from its file or loaded
 * into memory.
 */
public final class VortexAdapters implements AdapterFactory {
	private static final Map<String, VortexAdapter> ADAPTERS = new LinkedHashMap<>();

	static {
		add("vortex_dict", "vortex-rdf Dict (file)", List.of(), false);
		add("vortex_dict_mem", "vortex-rdf Dict (memory)", List.of(), true);
		add("vortex_dict_bycopy", "vortex-rdf Dict+ByCopy (file)", List.of("secondary-by-copy"), false);
		add("vortex_dict_bycopy_mem", "vortex-rdf Dict+ByCopy (memory)", List.of("secondary-by-copy"), true);
		add("vortex_dict_byref", "vortex-rdf Dict+ByRef (file)", List.of("secondary-by-reference"), false);
		add("vortex_dict_byref_mem", "vortex-rdf Dict+ByRef (memory)", List.of("secondary-by-reference"), true);
	}

	private static void add(String slug, String label, List<String> indexes, boolean inMemory) {
		ADAPTERS.put(slug, new VortexAdapter(slug, label, indexes, inMemory));
	}

	@Override
	public List<String> slugs() {
		return List.copyOf(ADAPTERS.keySet());
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		VortexAdapter adapter = ADAPTERS.get(slug);
		if (adapter == null) {
			throw new IllegalArgumentException("not a Vortex slug: " + slug);
		}
		return adapter;
	}
}
