package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.vortexrdf.bench.Pattern.Kind;

class InputsTest {
	@TempDir
	Path tmp;

	@Test
	void aQuadLineKeepsTheSpacesInsideALiteral() {
		assertEquals(new Quad("<s>", "<p>", "\"a b\"", "<g>"), Inputs.line("<s> <p> \"a b\" <g> ."));
	}

	@Test
	void aTripleLineIsInTheDefaultGraph() {
		assertNull(Inputs.line("<s> <p> <o> .").g());
	}

	@Test
	void aSuffixedLiteralIsOneTerm() {
		assertEquals(new Quad("<s>", "<p>", "\"v\"@en", null), Inputs.line("<s> <p> \"v\"@en ."));
	}

	@Test
	void aLineWithoutItsTerminatorIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> Inputs.line("<s> <p> <o>"));
	}

	@Test
	void theFixtureProbesComeInTheirKinds() throws Exception {
		List<Pattern> probes = Inputs.probes(Fixture.extract(tmp).probes());

		assertEquals(List.of("S", "P", "O", "SP", "PO", "SPO", "G", "SPOG", "full"),
				probes.stream().map(Pattern::name).toList());
		assertEquals(List.of(Kind.TRIPLES, Kind.TRIPLES, Kind.TRIPLES, Kind.TRIPLES, Kind.TRIPLES, Kind.TRIPLES,
				Kind.QUADS, Kind.QUADS, Kind.FULL), probes.stream().map(Pattern::kind).toList());
		Pattern g = probes.get(6);
		assertNotNull(g.g());
		assertNull(g.s());
		assertNull(probes.get(8).s());
	}
}
