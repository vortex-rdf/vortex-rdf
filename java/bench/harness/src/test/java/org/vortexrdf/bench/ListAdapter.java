package org.vortexrdf.bench;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A test store: the rows of the file it is built from, in a list. Its flags make it behave like the kinds of
 * library the harness meets: persistent (it writes a copy of its source into the store location and reopens from
 * it), triples-only, failing on one pattern, chatty on stdout, unable to mutate, or locking its files so that a
 * second handle cannot open while one is open, as RDF4J's on-disk stores do. {@link #throwing} and {@link #slowOn}
 * add a chosen error or a delay at one pattern (or at {@code add} / {@code delete}), {@link #calls} counts how
 * often each pattern was materialized, and {@link #holding} has every store keep a known amount of memory, which
 * {@link #pinnedToTheQueryingThread} leaves reachable from the thread that queried it.
 */
public final class ListAdapter implements Adapter<ListAdapter.Store, Pattern> {
	/**
	 * The store a handle reads: its rows, the memory {@link #holding} has it keep until it is disposed, and the
	 * thread-local {@link #pinnedToTheQueryingThread} parks that memory in.
	 */
	public record Store(List<Quad> rows, byte[] heap, Arena offHeap, ThreadLocal<byte[]> pin) {
	}

	private final String slug;
	private final boolean persistent;
	private final boolean triplesOnly;
	private final String failOn;
	private final boolean printsToStdout;
	private final boolean immutable;
	private final boolean exclusive;
	private int openHandles;
	private int heldHeap;
	private long heldNative;
	private boolean pinned;
	private final Map<String, Supplier<? extends Throwable>> errors = new HashMap<>();
	private final Map<String, Long> delays = new HashMap<>();
	private final Map<String, Integer> calls = new HashMap<>();

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

	/** Throw what {@code error} makes whenever {@code what} (a pattern's name, {@code add} or {@code delete}) runs. */
	public ListAdapter throwing(String what, Supplier<? extends Throwable> error) {
		errors.put(what, error);
		return this;
	}

	/** Sleep {@code millis} in every count of pattern {@code name}. */
	public ListAdapter slowOn(String name, long millis) {
		delays.put(name, millis);
		return this;
	}

	/** How many times pattern {@code name} was materialized. */
	public int calls(String name) {
		return calls.getOrDefault(name, 0);
	}

	/** Every store this adapter builds or opens keeps {@code heapBytes} in the Java heap and {@code nativeBytes}
	 * outside it until it is disposed. */
	public ListAdapter holding(int heapBytes, long nativeBytes) {
		heldHeap = heapBytes;
		heldNative = nativeBytes;
		return this;
	}

	/**
	 * Every count parks the store's heap memory in a thread-local of the store's that is never cleared, as Jena's
	 * in-memory dataset does with its data: once the store is unreachable, that memory stays reachable from the
	 * querying thread until the thread ends.
	 */
	public ListAdapter pinnedToTheQueryingThread() {
		pinned = true;
		return this;
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
		return store(new ArrayList<>(Inputs.rows(src)));
	}

	@Override
	public Store open(Path src, Path location) throws Exception {
		lock();
		return store(new ArrayList<>(Inputs.rows(persistent ? location.resolve("store.nq") : src)));
	}

	@Override
	public Pattern prepare(Pattern p) {
		return p;
	}

	@Override
	public long count(Store store, Pattern p) throws Exception {
		check(p.name());
		calls.merge(p.name(), 1, Integer::sum);
		if (store.pin() != null) {
			store.pin().set(store.heap());
		}
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
	public long countOnly(Store store, Pattern p) throws Exception {
		check(p.name());
		return store.rows().stream().filter(q -> matches(q, p)).count();
	}

	@Override
	public void add(Store store, List<Quad> quads) throws Exception {
		check("add");
		store.rows().addAll(quads);
	}

	@Override
	public void delete(Store store, List<Quad> quads) throws Exception {
		check("delete");
		store.rows().removeAll(new HashSet<>(quads));
	}

	@Override
	public void dispose(Store store) {
		openHandles--;
		if (store.offHeap() != null) {
			store.offHeap().close();
		}
	}

	private Store store(List<Quad> rows) {
		Arena offHeap = null;
		if (heldNative > 0) {
			offHeap = Arena.ofShared();
			offHeap.allocate(heldNative);
		}
		return new Store(rows, heldHeap > 0 ? new byte[heldHeap] : null, offHeap, pinned ? new ThreadLocal<>() : null);
	}

	private void lock() {
		if (exclusive && openHandles > 0) {
			throw new IllegalStateException("the store is locked by an open handle");
		}
		openHandles++;
	}

	private void check(String what) throws Exception {
		if (what.equals(failOn)) {
			throw new IllegalStateException("failing on purpose at " + what);
		}
		Supplier<? extends Throwable> error = errors.get(what);
		if (error != null) {
			Throwable t = error.get();
			if (t instanceof Exception e) {
				throw e;
			}
			throw (Error) t;
		}
		Long delay = delays.get(what);
		if (delay != null) {
			Thread.sleep(delay);
		}
	}

	private static boolean matches(Quad q, Pattern p) {
		return (p.s() == null || p.s().equals(q.s())) && (p.p() == null || p.p().equals(q.p()))
				&& (p.o() == null || p.o().equals(q.o())) && (p.g() == null || p.g().equals(q.g()));
	}
}
