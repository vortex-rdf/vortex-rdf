package org.vortexrdf.bench.jena;

import java.nio.file.Path;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.loader.DataLoader;
import org.apache.jena.tdb2.loader.LoaderFactory;
import org.apache.jena.tdb2.sys.TDBInternal;

/**
 * Jena TDB2, built with its bulk loader (the default plan, as {@code tdb2.tdbloader} uses) and keeping its default
 * indexes, which serve every pattern.
 *
 * <p>TDB2 caches one connection per location in the process. Dispose expels it, so each open, and each cold
 * reading's open, connects to the files again rather than receiving the cached dataset.
 */
final class JenaTdb2Adapter extends JenaDatasetAdapter {
	@Override
	public String slug() {
		return "jena_tdb2";
	}

	@Override
	public String label() {
		return "Jena TDB2 (file)";
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public DatasetGraph build(Path src, Path location) {
		DatasetGraph dsg = DatabaseMgr.connectDatasetGraph(location.toString());
		DataLoader loader = LoaderFactory.createLoader(dsg, (format, args) -> {
		});
		loader.startBulk();
		try {
			loader.load(src.toString());
			loader.finishBulk();
		} catch (RuntimeException e) {
			loader.finishException(e);
			throw e;
		}
		return dsg;
	}

	@Override
	public DatasetGraph open(Path src, Path location) {
		return DatabaseMgr.connectDatasetGraph(location.toString());
	}

	@Override
	public void dispose(DatasetGraph dsg) {
		TDBInternal.expel(dsg);
	}
}
