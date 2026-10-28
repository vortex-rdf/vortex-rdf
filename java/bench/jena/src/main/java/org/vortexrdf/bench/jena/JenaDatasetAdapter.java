package org.vortexrdf.bench.jena;

import java.util.Iterator;
import java.util.List;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.Txn;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Sink;
import org.vortexrdf.bench.Terms;

/**
 * What Jena's two datasets share: a pattern is the {@code {g, s, p, o}} nodes of {@link DatasetGraph#find}, with
 * {@link Node#ANY} at its wildcards, and every read or write runs in a transaction, as a Jena program's does.
 */
abstract class JenaDatasetAdapter implements Adapter<DatasetGraph, Node[]> {
	private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";

	@Override
	public Node[] prepare(Pattern p) {
		return new Node[] { pattern(p.g()), pattern(p.s()), pattern(p.p()), pattern(p.o()) };
	}

	@Override
	public long count(DatasetGraph dsg, Node[] q) {
		return Txn.calculateRead(dsg, () -> {
			long acc = 0, rows = 0;
			for (Iterator<Quad> it = dsg.find(q[0], q[1], q[2], q[3]); it.hasNext();) {
				Quad quad = it.next();
				acc += text(quad.getSubject()) + text(quad.getPredicate()) + text(quad.getObject())
						+ text(quad.getGraph());
				rows++;
			}
			Sink.fold(acc);
			return rows;
		});
	}

	@Override
	public long countOnly(DatasetGraph dsg, Node[] q) {
		return Txn.calculateRead(dsg, () -> Iter.count(dsg.find(q[0], q[1], q[2], q[3])));
	}

	@Override
	public void add(DatasetGraph dsg, List<org.vortexrdf.bench.Quad> quads) {
		Txn.executeWrite(dsg, () -> {
			for (org.vortexrdf.bench.Quad q : quads) {
				dsg.add(quad(q));
			}
		});
	}

	@Override
	public void delete(DatasetGraph dsg, List<org.vortexrdf.bench.Quad> quads) {
		Txn.executeWrite(dsg, () -> {
			for (org.vortexrdf.bench.Quad q : quads) {
				dsg.delete(quad(q));
			}
		});
	}

	/** A row of an input file as a Jena quad; a row without a graph is in the default graph. */
	static Quad quad(org.vortexrdf.bench.Quad q) {
		return Quad.create(q.g() == null ? Quad.defaultGraphIRI : node(q.g()), node(q.s()), node(q.p()), node(q.o()));
	}

	private static Node pattern(String term) {
		return term == null ? Node.ANY : node(term);
	}

	static Node node(String nTriples) {
		Terms.Term t = Terms.parse(nTriples);
		return switch (t.kind()) {
		case IRI -> NodeFactory.createURI(t.value());
		case BLANK -> NodeFactory.createBlankNode(t.value());
		case LITERAL -> t.lang() != null ? NodeFactory.createLiteralLang(t.value(), t.lang())
				: t.datatype() != null
						? NodeFactory.createLiteralDT(t.value(), TypeMapper.getInstance().getSafeTypeByName(t.datatype()))
						: NodeFactory.createLiteralString(t.value());
		};
	}

	/** A term's text, as the harness reads every materialized term; the default graph has none. */
	private static long text(Node n) {
		if (n.isURI()) {
			return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated) ? 0 : n.getURI().length();
		}
		if (n.isLiteral()) {
			String lang = n.getLiteralLanguage();
			String datatype = n.getLiteralDatatypeURI();
			return n.getLiteralLexicalForm().length() + (lang != null && !lang.isEmpty() ? lang.length()
					: datatype != null && !datatype.equals(XSD_STRING) ? datatype.length() : 0);
		}
		return n.isBlank() ? n.getBlankNodeLabel().length() : 0;
	}
}
