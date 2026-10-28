package org.vortexrdf.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A test store: the rows of the file it is built from, in a list. Its flags make it behave like the kinds of
 * library the harness meets: persistent (it writes a copy of its source into the store location and reopens from
 * it), triples-only, failing on one pattern, chatty on stdout, unable to mutate, or locking its files so that a
 * second handle cannot open while one is open, as RDF4J's on-disk stores do.
 */
public final class ListAdapter implements Adapter<ListAdapter.Store, Pattern> {
	/** The store a handle reads: its rows. */
	public record Store(List<Quad> rows) {
	}

	private final String slug;
	private final boolean persistent;
	private final boolean triplesOnly;
	private final String failOn;
	private final boolean printsToStdout;
	private final boolean immutable;
	private final boolean exclusive;
	private int openHandles;

	private ListAdapter(String slug, boolean persistent, boolean triplesOnly, String failOn, boolean printsToStdout,
			boolean immutable, boolean exclusive) {
		this.slug = slug;
		this.persistent = persistent;
		this.triplesOnly = triplesOnly;
		this.failOn = failOn;
		this.printsToStdout = printsToStdout;
		this.immutable = immutable;
		this.exclusive = exclusive;
	}

	public static ListAdapter persistent(String slug) {
		return new ListAdapter(slug, true, false, null, false, false, false);
	}

	public static ListAdapter inMemory(String slug) {
		return new ListAdapter(slug, false, false, null, false, false, false);
	}

	/** Triples-only, and immutable as the triples-only libraries are. */
	public static ListAdapter triplesOnly(String slug) {
		return new ListAdapter(slug, true, true, null, false, true, false);
	}

	public static ListAdapter failingOn(String slug, String pattern) {
		return new ListAdapter(slug, true, false, pattern, false, false, false);
	}

	public static ListAdapter printingToStdout(String slug) {
		return new ListAdapter(slug, true, false, null, true, false, false);
	}

	public static ListAdapter immutable(String slug) {
		return new ListAdapter(slug, true, false, null, false, true, false);
	}

	/** Persistent, and refusing a second open handle, as a store that locks its directory does. */
	public static ListAdapter exclusive(String slug) {
		return new ListAdapter(slug, true, false, null, false, false, true);
	}

	@Override
	public String slug() {
		return slug;
	}

	@Override
	public String label() {
		return "List " + slug;
	}

	@Override
	public boolean supportsQuads() {
		return !triplesOnly;
	}

	@Override
	public String mutationUnsupported() {
		return immutable ? "a test store that cannot change" : null;
	}

	@Override
	public boolean persistent() {
		return persistent;
	}

	@Override
	public Store build(Path src, Path location) throws Exception {
		lock();
		if (persistent) {
			Files.copy(src, location.resolve("store.nq"), StandardCopyOption.REPLACE_EXISTING);
		}
		return new Store(new ArrayList<>(Inputs.rows(src)));
	}

	@Override
	public Store open(Path src, Path location) throws Exception {
		lock();
		return new Store(new ArrayList<>(Inputs.rows(persistent ? location.resolve("store.nq") : src)));
	}

	@Override
	public Pattern prepare(Pattern p) {
		return p;
	}

	@Override
	public long count(Store store, Pattern p) {
		check(p);
		if (printsToStdout) {
			System.out.println("noise from the adapter");
		}
		long acc = 0, rows = 0;
		for (Quad q : store.rows()) {
			if (matches(q, p)) {
				acc += q.s().length() + q.p().length() + q.o().length() + (q.g() == null ? 0 : q.g().length());
				rows++;
			}
		}
		Sink.fold(acc);
		return rows;
	}

	@Override
	public long countOnly(Store store, Pattern p) {
		check(p);
		return store.rows().stream().filter(q -> matches(q, p)).count();
	}

	@Override
	public void add(Store store, List<Quad> quads) {
		store.rows().addAll(quads);
	}

	@Override
	public void delete(Store store, List<Quad> quads) {
		store.rows().removeAll(new HashSet<>(quads));
	}

	@Override
	public void dispose(Store store) {
		openHandles--;
	}

	private void lock() {
		if (exclusive && openHandles > 0) {
			throw new IllegalStateException("the store is locked by an open handle");
		}
		openHandles++;
	}

	private void check(Pattern p) {
		if (p.name().equals(failOn)) {
			throw new IllegalStateException("failing on purpose at " + p.name());
		}
	}

	private static boolean matches(Quad q, Pattern p) {
		return (p.s() == null || p.s().equals(q.s())) && (p.p() == null || p.p().equals(q.p()))
				&& (p.o() == null || p.o().equals(q.o())) && (p.g() == null || p.g().equals(q.g()));
	}
}
