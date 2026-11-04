# Vortex-RDF for Java

Java bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex. A store is opened memory-mapped from a `.vortex` file and queried in place, without loading the dataset into memory. The bindings are read-only: build `.vortex` files with the [CLI](../README.md) (`vortex-rdf-cli serialize`) or the Python and JavaScript bindings, then open and query them from Java.

They are a thin layer over the Rust core, for any JVM program that wants to query a store: a JNI library (`libvortex_rdf_jni`, this crate) and the small `org.vortexrdf` API on top of it. They are not tied to a query engine; `vortex-qendpoint`, a separate repository, is one integration built on them: it serves a store through qEndpoint (RDF4J) in place of an HDT file.

Requires Java 17 or later. The API is `org.vortexrdf.VortexRdfStore`, with `TermDictionary` for term codes and `QuadIterator` and `CodeChunks` for streaming a match; every class is documented in its Javadoc.

## Install

From 0.14.0 the bindings are on Maven Central as `io.github.vortex-rdf:vortex-rdf` (the Java package stays `org.vortexrdf`). With Maven:

```xml
<dependency>
  <groupId>io.github.vortex-rdf</groupId>
  <artifactId>vortex-rdf</artifactId>
  <version>0.14.0</version>
</dependency>
```

With Gradle:

```kotlin
implementation("io.github.vortex-rdf:vortex-rdf:0.14.0")
```

The jar carries the native library for Linux on x86_64 and arm64 (glibc 2.28 or later), macOS on Intel and Apple Silicon, and Windows on x64, and loads the one for its platform with nothing to set. [Build](#build) says how the JVM finds the library, how to point it at another, and the flag Java 24 and later want.

## Build

To build the bindings from a checkout of this repository:

```bash
cargo build --release -p vortex-rdf-java        # target/release/libvortex_rdf_jni.so (.dylib on macOS, vortex_rdf_jni.dll on Windows)
cd java && mvn package -DskipTests              # target/vortex-rdf-X.Y.Z.jar, X.Y.Z the workspace version
```

The tests are skipped here because they read fixture stores built separately (see [Development](#development)).

The JVM finds the native library, in this order: at the path in the system property `vortexrdf.library`; at the path in the environment variable `VORTEX_RDF_JNI_LIB`; as the copy the jar carries for its platform; or under the name `vortex_rdf_jni` on `java.library.path`. A blank property or variable counts as unset. A jar built from a checkout as above carries no library, so it takes one of the other three:

```bash
java -Dvortexrdf.library=/path/to/libvortex_rdf_jni.so -cp vortex-rdf-X.Y.Z.jar:. MyApp
```

The jar's copy is unpacked into a private directory under `java.io.tmpdir`, which must allow executing files: where it is mounted `noexec`, set `-Djava.io.tmpdir` to a directory that does, or point `vortexrdf.library` at the library. The directory is removed when the JVM exits; on Windows the DLL stays until the next JVM to start removes it, and a JVM killed before it exits leaves its directory behind.

On Java 24 and later, loading a native library is a restricted operation. Add `--enable-native-access=ALL-UNNAMED` on the class path, or `--enable-native-access=org.vortexrdf` (the jar's module name) on the module path, so the JVM does not print a warning.

## Quick start

```java
import java.nio.file.Paths;
import org.vortexrdf.VortexRdfStore;

try (VortexRdfStore store = VortexRdfStore.open(Paths.get("data.vortex"))) {   // memory-mapped, layout auto-detected
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
- `VortexRdfStore.NONE` (`-1`) is a free position of a pattern.
- `encode` answers that same `NONE` for a term the store lacks. Check its answer before putting it into a pattern: there `NONE` is a free position, so an absent term would match every quad.

These calls raise `VortexException` on a store without a dictionary, which `store.dictionary()` tells in advance.

```java
TermDictionary dict = store.dictionary().orElseThrow();       // absent for other layouts
long name = dict.encode("<http://xmlns.com/foaf/0.1/name>");  // code of a term, or NONE
dict.decode(name);                                            // N-Triples spelling, or null past the dictionary
dict.decodeMany(codes);                                       // a whole column, its distinct terms once a chunk
dict.ranges().literals().contains(code);                      // a code's term kind: literals, IRIs or blank nodes

if (name != VortexRdfStore.NONE) {                            // the store holds the term
    long n = store.countCodes(VortexRdfStore.NONE, name, VortexRdfStore.NONE, VortexRdfStore.NONE);

    ByteBuffer rows = ByteBuffer.allocateDirect(4096 * VortexRdfStore.ROW_BYTES).order(ByteOrder.nativeOrder());
    int got = store.matchCodes(VortexRdfStore.NONE, name, VortexRdfStore.NONE, VortexRdfStore.NONE, 0, rows, 4096);
    for (int i = 0; i < got; i++) {
        long s = rows.getLong(i * VortexRdfStore.ROW_BYTES), o = rows.getLong(i * VortexRdfStore.ROW_BYTES + 16);
    }

    try (CodeChunks chunks = store.matchCodeChunks(VortexRdfStore.NONE, name, VortexRdfStore.NONE, VortexRdfStore.NONE)) {
        for (int n; (n = chunks.next(rows, 4096)) > 0; ) {          // the whole match, up to 4096 rows a call
            // rows 0 .. n-1 of the buffer, as above
        }
    }
}
```

`matchCodes` fills a reusable, writable direct buffer with `(s, p, o, g)` codes, four longs per row, from its start.

- A match can be read in windows by passing the offset. Every window of one pattern comes in the same row order, but each call finds its window anew.
- `matchCodeChunks` reads a whole match into the same kind of buffer, the next rows each call, with no offset to keep, and reads each row once; close it, or drain it, when done. Read a long match this way rather than in windows.
- A pattern that cannot match gives no rows, not an error: a code past the dictionary, or a literal in the subject position.

When a file store can locate the pattern's rows by binary search (a prefix of the quad table's `(s, p, o, g)` order, or with `secondary-by-copy` the predicate- and object-led orders), a code lookup is one located run with no dictionary search; it takes the general path otherwise, and returns the same rows. A pattern with a bound slot past the located prefix (a bound graph after a free position) checks the run's rows against it: every `matchCodes` window checks the run again from its start as far as its own rows, while `matchCodeChunks` checks each row once, as it reads it.

For measurements, two switches are read once when the native library loads: `VORTEX_RDF_LOCATE=0` makes every code lookup take the general path, and `VORTEX_RDF_COUNTERS=1` makes `VortexRdfStore.engineCounters()` count the encoded-search work (leaves searched encoded or decoded, probes, gathers, equality scans; all zero without it).

## Streaming a match

`quads` returns a whole match at once. `iterateQuads` reads it a chunk at a time instead, so a long match never sits in memory whole and a caller that stops early reads no more of it:

```java
try (QuadIterator rows = store.iterateQuads(null, "<http://xmlns.com/foaf/0.1/name>", null, null)) {
    while (rows.hasNext()) {
        String[] quad = rows.next();                                     // {s, p, o, g}, a fresh array
    }
}

try (Stream<String[]> rows = store.iterateQuads(s, p, o, g, 4096).stream()) {  // 4,096 rows a chunk, 65,536 by default
    rows.limit(10).forEach(quad -> System.out.println(quad[2]));                // closing the stream closes the iterator
}
```

The pattern is checked and matched when `iterateQuads` is called, so a malformed term raises there; the rows are read as the iterator is pulled, at most `batchRows` at a time: a smaller batch brings the first rows sooner, at the cost of more native calls over the whole match. `matchCodeChunks`, shown under [Term codes](#term-codes-low-level), does the same for a pattern of codes: each `next(out, maxRows)` writes the next rows into the buffer as `matchCodes` writes them, and returns 0 once the match is spent.

Both read a match in the order `quads` and `matchCodes` return it: base row order, `(s, p, o, g)`, except on a pattern a `secondary-by-copy` index serves, read in that index's order (a predicate's rows by object, then subject). The order is the same on every call.

## Threads, memory and lifetime

A store is safe to query from any number of threads. `close()` waits for the calls in progress, and later calls raise `IllegalStateException`. A file store is memory-mapped: what stays in RAM is the operating system's page cache plus a few MiB of rebuilt dictionary windows, and the file must not be modified while open (replace it by renaming a new file over it). `VortexRdfStore.open(path, true)` loads the whole store instead. Besides its result, a read holds one chunk of terms in transit: at most 16 MiB of them, or one row when that row's terms are larger.

An iterator (`QuadIterator` or `CodeChunks`) holds a native stream of its match until it ends: drain it or close it, best with try-with-resources. An open iterator holds one chunk of the match, at most `batchRows` rows, besides the rows it has handed out. Closing the store ends its iterators: one that has not ended then raises `IllegalStateException`, and one that has ended answers no more rows. An iterator left unreachable without being closed is freed by a cleaner, when the garbage collector finds it. An iterator is not thread-safe; give each thread its own.

## Development

The Rust side (`src/engine.rs` and the modules beside it, the logic, and `src/lib.rs`, the JNI glue) is tested by `tests/engine.rs`, `tests/payload.rs` and `tests/streams.rs` against a scan of the quads it was built from, for every pattern shape, layout and index set, by terms and by codes. The JUnit suite checks the same through the real library; it reads stores and an oracle file that the crate's `fixtures` example writes:

```bash
cargo build -p vortex-rdf-java
cargo run -p vortex-rdf-java --example fixtures -- java/target/fixtures
cd java && mvn test        # reads ../target/debug's library; -Dvortexrdf.library=<path> for another
```

`scripts/ci-check.sh java-tests` does both. It also copies the library to `target/natives/org/vortexrdf/native/<platform>/`, where the published jar carries its copy, and `BundledLibraryTest` loads it from there in a JVM of its own, with neither `vortexrdf.library` nor `VORTEX_RDF_JNI_LIB` set; a plain `mvn test` skips that test. The JNI functions live in `src/lib.rs`; their Java declarations are the package-private `Native` class.
