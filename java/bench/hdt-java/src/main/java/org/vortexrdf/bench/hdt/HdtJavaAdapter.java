package org.vortexrdf.bench.hdt;

import java.nio.file.Path;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Sink;
import org.vortexrdf.bench.Terms;

/**
 * rdfhdt's hdt-java: an HDT file built from the triples projection (HDT has no graphs), memory-mapped with its
 * HDT-FoQ index, which serves every triple pattern.
 *
 * <p>A pattern is HDT's own spelling of each term, the empty string at a wildcard: an IRI without its angle
 * brackets, a literal or a blank node as in N-Triples. The count-only path looks the bound terms up as IDs and
 * searches the triples by ID, reading no string.
 */
public final class HdtJavaAdapter implements Adapter<HDT, String[]> {
	private static final String BASE_URI = "http://example.org/";
	private static final String STORE_FILE = "store.hdt";

	@Override
	public String slug() {
		return "hdt_java";
	}

	@Override
	public String label() {
		return "hdt-java (file)";
	}

	@Override
	public boolean supportsQuads() {
		return false;
	}

	@Override
	public String mutationUnsupported() {
		return "an HDT file is immutable";
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public HDT build(Path src, Path location) throws Exception {
		String file = location.resolve(STORE_FILE).toString();
		try (HDT generated = HDTManager.generateHDT(src.toString(), BASE_URI, RDFNotation.NTRIPLES,
				new HDTSpecification(), null)) {
			generated.saveToHDT(file, null);
		}
		// Mapping it indexed writes the HDT-FoQ index beside it, so that every later open only maps.
		return HDTManager.mapIndexedHDT(file, null);
	}

	@Override
	public HDT open(Path src, Path location) throws Exception {
		return HDTManager.mapIndexedHDT(location.resolve(STORE_FILE).toString(), null);
	}

	@Override
	public String[] prepare(Pattern p) {
		return new String[] { term(p.s()), term(p.p()), term(p.o()) };
	}

	@Override
	public long count(HDT hdt, String[] q) {
		IteratorTripleString it;
		try {
			it = hdt.search(q[0], q[1], q[2]);
		} catch (NotFoundException e) {
			return 0;
		}
		long acc = 0, rows = 0;
		while (it.hasNext()) {
			TripleString t = it.next();
			acc += t.getSubject().toString().length() + t.getPredicate().toString().length()
					+ t.getObject().toString().length();
			rows++;
		}
		Sink.fold(acc);
		return rows;
	}

	@Override
	public long countOnly(HDT hdt, String[] q) {
		long s = id(hdt, q[0], TripleComponentRole.SUBJECT), p = id(hdt, q[1], TripleComponentRole.PREDICATE),
				o = id(hdt, q[2], TripleComponentRole.OBJECT);
		if (s < 0 || p < 0 || o < 0) {
			return 0;
		}
		IteratorTripleID it = hdt.getTriples().search(new TripleID(s, p, o));
		if (it.numResultEstimation() == ResultEstimationType.EXACT) {
			return it.estimatedNumResults();
		}
		long rows = 0;
		while (it.hasNext()) {
			it.next();
			rows++;
		}
		return rows;
	}

	@Override
	public void dispose(HDT hdt) throws Exception {
		hdt.close();
	}

	/** A bound term's ID, 0 for a wildcard, or -1 for a term the dictionary does not hold. */
	private static long id(HDT hdt, String term, TripleComponentRole role) {
		if (term.isEmpty()) {
			return 0;
		}
		long id = hdt.getDictionary().stringToId(term, role);
		return id <= 0 ? -1 : id;
	}

	/** HDT's spelling of an N-Triples term: an IRI loses its brackets; a wildcard is the empty string. */
	private static String term(String nTriples) {
		if (nTriples == null) {
			return "";
		}
		Terms.Term t = Terms.parse(nTriples);
		return t.kind() == Terms.Kind.IRI ? t.value() : nTriples;
	}
}
