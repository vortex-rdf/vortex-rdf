# Vortex-RDF for Java

Java bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex. A store is opened memory-mapped from a `.vortex` file and queried in place, without loading the dataset into memory. The bindings are read-only: build `.vortex` files with the [CLI](../README.md) (`vortex-rdf-cli serialize`) or the Python and JavaScript bindings, then open and query them from Java.

They are a thin layer over the Rust core, for any JVM program that wants to query a store: a JNI library (`libvortex_rdf_jni`, this crate) and the small `org.vortexrdf` API on top of it. They are not tied to a query engine; `vortex-qendpoint`, a separate repository, is one integration built on them: it serves a store through qEndpoint (RDF4J) in place of an HDT file.

Requires Java 11 or later. The API is `org.vortexrdf.VortexStore` and `org.vortexrdf.TermDictionary`; every class is documented in its Javadoc.

## Build

The artifact is not published yet. From a checkout of this repository:

```bash
cargo build --release -p vortex-rdf-java        # target/release/libvortex_rdf_jni.so (.dylib on macOS, vortex_rdf_jni.dll on Windows)
cd java && mvn package -DskipTests              # target/vortex-rdf-X.Y.Z.jar, X.Y.Z the workspace version
```

The tests are skipped here because they read fixture stores built separately (see [Development](#development)).

The JVM finds the native library, in this order, at the path in the system property `vortexrdf.library`, at the path in the environment variable `VORTEX_RDF_JNI_LIB`, or under the name `vortex_rdf_jni` on `java.library.path`:

```bash
java -Dvortexrdf.library=/path/to/libvortex_rdf_jni.so -cp vortex-rdf-X.Y.Z.jar:. MyApp
```

On Java 24 and later, loading a native library is a restricted operation. Add `--enable-native-access=ALL-UNNAMED`, or name the module that holds the jar, so the JVM does not print a warning.

## Quick start

```java
import java.nio.file.Paths;
import org.vortexrdf.VortexStore;

try (VortexStore store = VortexStore.open(Paths.get("data.vortex"))) {   // memory-mapped, layout auto-detected
    store.size();                                                         // number of quads
    store.count(null, "<http://xmlns.com/foaf/0.1/name>", null, null);    // match count, no terms materialized
    for (String[] quad : store.quads(null, "<http://xmlns.com/foaf/0.1/name>", null, null)) {
        System.out.println(quad[0] + " " + quad[2]);                      // {s, p, o, g}
    }
}
```

## Reading quads

A pattern has four positions, `(s, p, o, g)`; a `null` position is free. Terms are N-Triples spellings (`<iri>`, `_:b0`, `"lit"@en`, `"3"^^<http://www.w3.org/2001/XMLSchema#integer>`); the graph of a quad in the default graph is the empty string `""`, which is also how a pattern selects it. A malformed term raises `VortexException` whose message names the position (`pattern subject: …`); a store that does not open or read, or a call its layout cannot serve, raises the same.

```java
store.layout();                      // "dictionary" | "default" | "typed-object"
store.indexes();                     // e.g. [secondary-by-reference]
store.count(s, p, o, g);             // long
store.quads(s, p, o, g);             // List<String[]>, rows {s, p, o, g}
store.iterateQuads(s, p, o, g);      // QuadIterator: the same rows, read a chunk at a time; close it when done
```

They work on every layout, with identical results. `quads` is served from the term-code columns where the store has them and from the matched quads otherwise. Each row is its own `String[4]`, which the caller may change; terms cross from the native library in chunks of up to 65,536 rows, and the rows of a chunk that hold the same term share one `String`.

## Term codes (low-level)

A store of the `dictionary` layout keeps each term once in a sorted dictionary and its columns hold integer **term codes**. A query engine that holds codes (a join's per-binding lookup, say) can ask for a pattern of codes and skip the terms altogether.

- A code is a non-negative `long`.
- `VortexStore.NONE` (`-1`) is a free position of a pattern.
- `encode` answers that same `NONE` for a term the store lacks. Check its answer before putting it into a pattern: there `NONE` is a free position, so an absent term would match every quad.

These calls raise `VortexException` on a store without a dictionary, which `store.dictionary()` tells in advance.

```java
TermDictionary dict = store.dictionary().orElseThrow();       // absent for other layouts
long name = dict.encode("<http://xmlns.com/foaf/0.1/name>");  // code of a term, or NONE
dict.decode(name);                                            // N-Triples spelling, or null past the dictionary
dict.decodeMany(codes);                                       // one native call for a whole column
dict.ranges().literals().contains(code);                      // a code's term kind: literals, IRIs or blank nodes

if (name != VortexStore.NONE) {                               // the store holds the term
    long n = store.countCodes(VortexStore.NONE, name, VortexStore.NONE, VortexStore.NONE);

    ByteBuffer rows = ByteBuffer.allocateDirect(4096 * VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
    int got = store.matchCodes(VortexStore.NONE, name, VortexStore.NONE, VortexStore.NONE, 0, rows, 4096);
    for (int i = 0; i < got; i++) {
        long s = rows.getLong(i * VortexStore.ROW_BYTES), o = rows.getLong(i * VortexStore.ROW_BYTES + 16);
    }

    try (CodeChunks chunks = store.matchCodeChunks(VortexStore.NONE, name, VortexStore.NONE, VortexStore.NONE)) {
        for (int n; (n = chunks.next(rows, 4096)) > 0; ) {          // the whole match, up to 4096 rows a call
            // rows 0 .. n-1 of the buffer, as above
        }
    }
}
```

`matchCodes` fills a reusable, writable direct buffer with `(s, p, o, g)` codes, four longs per row, from its start.

- A long match is read in windows by passing the offset. Every window of one pattern comes in the same row order.
- `matchCodeChunks` reads a whole match into the same kind of buffer, the next rows each call, with no offset to keep; close it, or drain it, when done.
- A pattern that cannot match gives no rows, not an error: a code past the dictionary, or a literal in the subject position.

When a file store can locate the pattern's rows by binary search (a prefix of the quad table's `(s, p, o, g)` order, or with `secondary-by-copy` the predicate- and object-led orders), a code lookup is one located run with no dictionary search; it takes the general path otherwise, and returns the same rows.

For measurements, two switches are read once when the native library loads: `VORTEX_RDF_LOCATE=0` makes every code lookup take the general path, and `VORTEX_RDF_COUNTERS=1` makes `VortexStore.engineCounters()` count the encoded-search work (leaves searched encoded or decoded, probes, gathers, equality scans; all zero without it).

## Threads, memory and lifetime

A store is safe to query from any number of threads. `close()` waits for the calls in progress, and later calls raise `IllegalStateException`. A file store is memory-mapped: what stays in RAM is the operating system's page cache plus a few MiB of rebuilt dictionary windows, and the file must not be modified while open (replace it by renaming a new file over it). `VortexStore.open(path, true)` loads the whole store instead. Besides its result, a read holds one chunk of terms in transit: at most 16 MiB of them, or one row when that row's terms are larger.

## Development

The Rust side (`src/engine.rs`, the logic, and `src/lib.rs`, the JNI glue) is tested by `tests/engine.rs` against a scan of the quads it was built from, for every pattern shape, layout and index set, by terms and by codes. The JUnit suite checks the same through the real library; it reads stores and an oracle file that the crate's `fixtures` example writes:

```bash
cargo build -p vortex-rdf-java
cargo run -p vortex-rdf-java --example fixtures -- java/target/fixtures
cd java && mvn test        # reads ../target/debug's library; -Dvortexrdf.library=<path> for another
```

`scripts/ci-check.sh java-tests` does both. The JNI functions live in `src/lib.rs`; their Java declarations are the package-private `Native` class.
