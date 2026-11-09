package org.vortexrdf.bench;

import java.util.List;

/** The test stores `Worker.main` can find on the test classpath. */
public final class ListAdapters implements AdapterFactory {
	@Override
	public List<String> slugs() {
		return List.of("list_stdout");
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		return switch (slug) {
		case "list_stdout" -> ListAdapter.printingToStdout(slug);
		default -> throw new IllegalArgumentException(slug);
		};
	}
}
