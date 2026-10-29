package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class FootprintTest {
	/** A figure that reads higher after the release is the collectors' noise: nothing was released there. */
	@Test
	void aReleaseNeverReadsNegative() {
		assertEquals(new Footprint(0, 30), new Footprint(100, 50).minus(new Footprint(120, 20)));
	}
}
