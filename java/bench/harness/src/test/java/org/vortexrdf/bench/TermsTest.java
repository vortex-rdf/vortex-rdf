package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.vortexrdf.bench.Terms.Kind;
import org.vortexrdf.bench.Terms.Term;

class TermsTest {
	@Test
	void anIriLosesItsBrackets() {
		assertEquals(new Term(Kind.IRI, "http://x/a", null, null), Terms.parse("<http://x/a>"));
	}

	@Test
	void aPlainLiteralKeepsItsSpaces() {
		assertEquals(new Term(Kind.LITERAL, "v w", null, null), Terms.parse("\"v w\""));
	}

	@Test
	void aLanguageTagIsSplitOff() {
		assertEquals(new Term(Kind.LITERAL, "v", "en", null), Terms.parse("\"v\"@en"));
	}

	@Test
	void aDatatypeIsSplitOffWithoutBrackets() {
		assertEquals(new Term(Kind.LITERAL, "1", null, "http://www.w3.org/2001/XMLSchema#integer"),
				Terms.parse("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
	}

	@Test
	void aBlankNodeKeepsItsLabel() {
		assertEquals(new Term(Kind.BLANK, "b1", null, null), Terms.parse("_:b1"));
	}

	@Test
	void anythingElseIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> Terms.parse("x"));
		assertThrows(IllegalArgumentException.class, () -> Terms.parse("\"v\"x"));
	}
}
