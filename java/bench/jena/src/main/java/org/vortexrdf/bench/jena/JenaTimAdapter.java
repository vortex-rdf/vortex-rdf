package org.vortexrdf.bench.jena;

import java.nio.file.Path;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.system.Txn;

/**
 * Jena's transactional in-memory dataset (TIM, {@link DatasetGraphFactory#createTxnMem}), the in-memory choice
 * Jena's documentation names. It lives only in memory, so a fresh process re-parses the source to open it.
 */
final class JenaTimAdapter extends JenaDatasetAdapter {
	@Override
	public String slug() {
		return "jena_tim";
	}

	@Override
	public String label() {
		return "Jena TIM (memory)";
	}

	@Override
	public boolean persistent() {
		return false;
	}

	@Override
	public DatasetGraph build(Path src, Path location) {
		DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
		Txn.executeWrite(dsg, () -> RDFDataMgr.read(dsg, src.toString()));
		return dsg;
	}

	@Override
	public DatasetGraph open(Path src, Path location) {
		return build(src, location);
	}

	@Override
	public void dispose(DatasetGraph dsg) {
		dsg.close();
	}
}
