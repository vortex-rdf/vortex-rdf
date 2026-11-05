package org.vortexrdf.bench.vortex;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.vortexrdf.QuadIterator;
import org.vortexrdf.VortexRdfStore;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Obtained;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Sink;

/**
 * vortex-rdf through its Java bindings: one Dictionary-layout build, read from its file or loaded into memory.
 *
 * <p>The bindings only read, so the build is the CLI's ({@code vortex-rdf-cli serialize}), the way a Java program
 * gets a store; the Build cell times that process, and the Open cell what the bindings pay to open its file.
 * Patterns are N-Triples spellings, which the bindings take as they are: a null position is free.
 */
public final class VortexAdapter implements Adapter<VortexRdfStore, Pattern> {
	private static final String STORE_FILE = "store.vortex";

	private final String slug;
	private final String label;
	private final List<String> indexes;
	private final boolean inMemory;

	/**
	 * @param indexes the secondary indexes the build adds, by the CLI's names
	 * @param inMemory whether {@link #open} loads the whole file, rather than reading its columns per query
	 */
	public VortexAdapter(String slug, String label, List<String> indexes, boolean inMemory) {
		this.slug = slug;
		this.label = label;
		this.indexes = List.copyOf(indexes);
		this.inMemory = inMemory;
	}

	@Override
	public String slug() {
		return slug;
	}

	@Override
	public String label() {
		return label;
	}

	@Override
	public String mutationUnsupported() {
		return "the Java bindings are read-only";
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public Obtained obtained() {
		return inMemory ? Obtained.LOADED : Obtained.MAPPED;
	}

	@Override
	public VortexRdfStore build(Path src, Path location) throws Exception {
		List<String> cmd = new ArrayList<>(List.of(cli(), "serialize", "--layout", "dictionary"));
		for (String index : indexes) {
			cmd.add("--indexes");
			cmd.add(index);
		}
		cmd.addAll(List.of("-i", src.toString(), "-o", location.resolve(STORE_FILE).toString()));
		// The worker's stdout is not its result channel, but nothing the CLI prints belongs on it either.
		Process cli = new ProcessBuilder(cmd).redirectOutput(Redirect.DISCARD).redirectError(Redirect.INHERIT).start();
		int code = cli.waitFor();
		if (code != 0) {
			throw new IOException("vortex-rdf-cli exited with " + code + ": " + String.join(" ", cmd));
		}
		return open(src, location);
	}

	@Override
	public VortexRdfStore open(Path src, Path location) {
		return VortexRdfStore.open(location.resolve(STORE_FILE), inMemory);
	}

	@Override
	public Pattern prepare(Pattern p) {
		return p;
	}

	@Override
	public long count(VortexRdfStore store, Pattern p) {
		// iterateQuads is the bindings' streaming term read: four N-Triples spellings per row, at most 65,536 rows a
		// chunk, so the read never holds the whole match.
		long acc = 0, n = 0;
		try (QuadIterator rows = store.iterateQuads(p.s(), p.p(), p.o(), p.g())) {
			while (rows.hasNext()) {
				for (String term : rows.next()) {
					acc += term.length();
				}
				n++;
			}
		}
		Sink.fold(acc);
		return n;
	}

	@Override
	public long countOnly(VortexRdfStore store, Pattern p) {
		return store.count(p.s(), p.p(), p.o(), p.g());
	}

	@Override
	public void dispose(VortexRdfStore store) {
		store.close();
	}

	private static String cli() {
		String cli = System.getProperty("vortexrdf.cli");
		if (cli == null) {
			throw new IllegalStateException("set -Dvortexrdf.cli to the vortex-rdf-cli binary");
		}
		return cli;
	}
}
