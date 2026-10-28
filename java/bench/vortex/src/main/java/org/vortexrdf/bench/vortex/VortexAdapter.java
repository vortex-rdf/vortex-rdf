package org.vortexrdf.bench.vortex;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.vortexrdf.VortexStore;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Sink;

/**
 * vortex-rdf through its Java bindings: one Dictionary-layout build, read from its file or loaded into memory.
 *
 * <p>The bindings only read, so the build is the CLI's ({@code vortex-rdf-cli serialize}), the way a Java program
 * gets a store; the Build cell times that process, and the Open cell what the bindings pay to open its file.
 * Patterns are N-Triples spellings, which the bindings take as they are: a null position is free.
 */
public final class VortexAdapter implements Adapter<VortexStore, Pattern> {
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
	public VortexStore build(Path src, Path location) throws Exception {
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
	public VortexStore open(Path src, Path location) {
		return VortexStore.open(location.resolve(STORE_FILE), inMemory);
	}

	@Override
	public Pattern prepare(Pattern p) {
		return p;
	}

	@Override
	public long count(VortexStore store, Pattern p) {
		// quads() is the bindings' term read: four N-Triples spellings per row, built in one native call.
		List<String[]> rows = store.quads(p.s(), p.p(), p.o(), p.g());
		long acc = 0;
		for (String[] row : rows) {
			for (String term : row) {
				acc += term.length();
			}
		}
		Sink.fold(acc);
		return rows.size();
	}

	@Override
	public long countOnly(VortexStore store, Pattern p) {
		return store.count(p.s(), p.p(), p.o(), p.g());
	}

	@Override
	public void dispose(VortexStore store) {
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
