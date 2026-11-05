package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import org.junit.jupiter.api.Test;

class RssTest {
	@Test
	void readsBothFields() {
		Rss rss = Rss.read();

		assertTrue(rss.anonBytes() > 0 && rss.fileBytes() > 0, rss.toString());
	}

	/** A block held between the sampler's start and its close is in its peak, and gone once freed. */
	@Test
	void theSamplerKeepsTheHighestSample() throws Exception {
		Rss base = Rss.read();
		Rss.Sampler s = Rss.Sampler.start();
		try (Arena arena = Arena.ofConfined()) {
			arena.allocate(64L << 20); // zeroed, so resident
			Thread.sleep(50);
		}
		s.close();

		assertTrue(s.peak().anonBytes() - base.anonBytes() >= 60L << 20, "peak " + s.peak() + " over " + base);
		assertTrue(Rss.read().anonBytes() - base.anonBytes() < 32L << 20, "after the free, over " + base);
	}
}
