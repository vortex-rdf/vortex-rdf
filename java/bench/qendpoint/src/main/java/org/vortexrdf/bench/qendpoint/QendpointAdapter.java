package org.vortexrdf.bench.qendpoint;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import java.nio.file.Path;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Sink;
import org.vortexrdf.bench.Terms;

/**
 * qEndpoint's maintained HDT library, qendpoint-core: a quad HDT (its four-section dictionary, with graphs) built
 * from the N-Quads file and memory-mapped with its index.
 *
 * <p>A pattern is HDT's own spelling of each term, the empty string at a wildcard: an IRI without its angle
 * brackets, a literal or a blank node as in N-Triples. The count-only path looks the bound terms up as IDs and
 * searches the quads by ID, reading no string.
 */
public final class QendpointAdapter implements Adapter<HDT, String[]> {
	private static final String BASE_URI = "http://example.org/";
	private static final String STORE_FILE = "store.hdt";

	@Override
	public String slug() {
		return "qendpoint_hdt";
	}

	@Override
	public String label() {
		return "qEndpoint HDT (file)";
	}

	@Override
	public String mutationUnsupported() {
		return "an HDT file is immutable; qEndpoint's updates live in its store layer, which is not under test";
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public HDT build(Path src, Path location) throws Exception {
		Path file = location.resolve(STORE_FILE);
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION);
		try (HDT generated = HDTManager.generateHDT(src, BASE_URI, RDFNotation.NQUAD, spec, ProgressListener.ignore())) {
			generated.saveToHDT(file);
		}
		// Mapping it indexed writes the index beside it, so that every later open only maps.
		return HDTManager.mapIndexedHDT(file);
	}

	@Override
	public HDT open(Path src, Path location) throws Exception {
		return HDTManager.mapIndexedHDT(location.resolve(STORE_FILE));
	}

	@Override
	public String[] prepare(Pattern p) {
		return new String[] { term(p.s()), term(p.p()), term(p.o()), term(p.g()) };
	}

	@Override
	public long count(HDT hdt, String[] q) {
		IteratorTripleString it;
		try {
			it = hdt.search(q[0], q[1], q[2], q[3]);
		} catch (NotFoundException e) {
			return 0;
		}
		long acc = 0, rows = 0;
		while (it.hasNext()) {
			TripleString t = it.next();
			CharSequence g = t.getGraph();
			acc += t.getSubject().toString().length() + t.getPredicate().toString().length()
					+ t.getObject().toString().length() + (g == null ? 0 : g.toString().length());
			rows++;
		}
		Sink.fold(acc);
		return rows;
	}

	@Override
	public long countOnly(HDT hdt, String[] q) {
		long s = id(hdt, q[0], TripleComponentRole.SUBJECT), p = id(hdt, q[1], TripleComponentRole.PREDICATE),
				o = id(hdt, q[2], TripleComponentRole.OBJECT), g = id(hdt, q[3], TripleComponentRole.GRAPH);
		if (s < 0 || p < 0 || o < 0 || g < 0) {
			return 0;
		}
		IteratorTripleID it = hdt.getTriples().search(new TripleID(s, p, o, g));
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
