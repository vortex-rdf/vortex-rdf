package org.vortexrdf.bench.rdf4j;

import java.nio.file.Path;
import java.util.List;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.lmdb.LmdbStore;
import org.eclipse.rdf4j.sail.lmdb.config.LmdbStoreConfig;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.Pattern;
import org.vortexrdf.bench.Quad;
import org.vortexrdf.bench.Sink;
import org.vortexrdf.bench.Terms;

/**
 * One of RDF4J's stores behind a {@link SailRepository}, read and written through a {@link RepositoryConnection},
 * as an RDF4J program does. A pattern without a graph searches every graph; one with a graph, that graph alone.
 *
 * <p>The two on-disk stores get the triple indexes {@value #TRIPLE_INDEXES}. Their default, spoc and posc, leaves
 * an object- or graph-bound pattern a full scan, where every other store here answers it from an index. A bulk
 * load runs in one transaction at isolation level NONE, as RDF4J's documentation advises for loading.
 */
public final class Rdf4jAdapter implements Adapter<Rdf4jAdapter.Handle, Rdf4jAdapter.Query> {
	/** The indexes that serve every pattern position: subject, predicate, object and graph first. */
	static final String TRIPLE_INDEXES = "spoc,posc,ospc,cspo";

	/** Which RDF4J store. */
	public enum Store {
		MEMORY, NATIVE, LMDB
	}

	/** An open store: its repository and the connection every call goes through. */
	public record Handle(SailRepository repo, RepositoryConnection conn) {
	}

	/** A pattern in RDF4J's terms; null is a wildcard, and a null graph searches every graph. */
	public record Query(Resource s, IRI p, Value o, Resource g) {
	}

	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final Resource[] ANY_GRAPH = {};

	private final String slug;
	private final String label;
	private final Store store;

	Rdf4jAdapter(String slug, String label, Store store) {
		this.slug = slug;
		this.label = label;
		this.store = store;
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
	public boolean persistent() {
		return store != Store.MEMORY;
	}

	@Override
	public Handle build(Path src, Path location) throws Exception {
		Handle h = connect(location);
		h.conn().begin(IsolationLevels.NONE);
		h.conn().add(src.toFile(), src.toString().endsWith(".nq") ? RDFFormat.NQUADS : RDFFormat.NTRIPLES);
		h.conn().commit();
		return h;
	}

	@Override
	public Handle open(Path src, Path location) throws Exception {
		return store == Store.MEMORY ? build(src, location) : connect(location);
	}

	@Override
	public Query prepare(Pattern p) {
		return new Query((Resource) value(p.s()), (IRI) value(p.p()), value(p.o()), (Resource) value(p.g()));
	}

	@Override
	public long count(Handle h, Query q) {
		long acc = 0, rows = 0;
		try (RepositoryResult<Statement> it = h.conn().getStatements(q.s(), q.p(), q.o(), false, graphs(q))) {
			for (Statement st : it) {
				acc += text(st.getSubject()) + text(st.getPredicate()) + text(st.getObject())
						+ (st.getContext() == null ? 0 : text(st.getContext()));
				rows++;
			}
		}
		Sink.fold(acc);
		return rows;
	}

	@Override
	public long countOnly(Handle h, Query q) {
		long rows = 0;
		try (RepositoryResult<Statement> it = h.conn().getStatements(q.s(), q.p(), q.o(), false, graphs(q))) {
			for (Statement ignored : it) {
				rows++;
			}
		}
		return rows;
	}

	@Override
	public void add(Handle h, List<Quad> quads) {
		h.conn().begin();
		for (Quad q : quads) {
			h.conn().add((Resource) value(q.s()), (IRI) value(q.p()), value(q.o()), (Resource) value(q.g()));
		}
		h.conn().commit();
	}

	@Override
	public void delete(Handle h, List<Quad> quads) {
		h.conn().begin();
		for (Quad q : quads) {
			// One context, null for the default graph: no context at all would remove from every graph.
			h.conn().remove((Resource) value(q.s()), (IRI) value(q.p()), value(q.o()), (Resource) value(q.g()));
		}
		h.conn().commit();
	}

	@Override
	public void dispose(Handle h) {
		h.conn().close();
		h.repo().shutDown();
	}

	private Handle connect(Path location) {
		Sail sail = switch (store) {
		case MEMORY -> new MemoryStore();
		case NATIVE -> new NativeStore(location.toFile(), TRIPLE_INDEXES);
		case LMDB -> new LmdbStore(location.toFile(), new LmdbStoreConfig(TRIPLE_INDEXES));
		};
		SailRepository repo = new SailRepository(sail);
		repo.init();
		return new Handle(repo, repo.getConnection());
	}

	private static Resource[] graphs(Query q) {
		return q.g() == null ? ANY_GRAPH : new Resource[] { q.g() };
	}

	private static Value value(String nTriples) {
		if (nTriples == null) {
			return null;
		}
		Terms.Term t = Terms.parse(nTriples);
		return switch (t.kind()) {
		case IRI -> VF.createIRI(t.value());
		case BLANK -> VF.createBNode(t.value());
		case LITERAL -> t.lang() != null ? VF.createLiteral(t.value(), t.lang())
				: t.datatype() != null ? VF.createLiteral(t.value(), VF.createIRI(t.datatype()))
						: VF.createLiteral(t.value());
		};
	}

	/** A term's text, as the harness reads every materialized term. */
	private static long text(Value v) {
		if (v instanceof Literal literal) {
			IRI datatype = literal.getDatatype();
			return literal.getLabel().length() + literal.getLanguage().map(String::length)
					.orElse(datatype == null || XSD.STRING.equals(datatype) ? 0 : datatype.stringValue().length());
		}
		return v.stringValue().length();
	}
}
