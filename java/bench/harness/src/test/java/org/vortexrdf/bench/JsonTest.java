package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {
	@Test
	void stringsEscapeQuotesBackslashesAndControlCharacters() {
		assertEquals("{\"a\":\"q\\\"\\\\\\n\\u0001\"}", Json.write(Map.of("a", "q\"\\\n\u0001")));
	}

	@Test
	void mapsKeepTheirInsertionOrder() {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("z", 1L);
		m.put("a", 2L);
		assertEquals("{\"z\":1,\"a\":2}", Json.write(m));
	}

	@Test
	void scalarsAndListsAreJson() {
		assertEquals("[null,7,2.5,true,\"µs\"]", Json.write(Arrays.asList(null, 7L, 2.5, true, "µs")));
		assertEquals("[]", Json.write(List.of()));
	}

	@Test
	void aNonFiniteNumberIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> Json.write(List.of(Double.NaN)));
	}
}
