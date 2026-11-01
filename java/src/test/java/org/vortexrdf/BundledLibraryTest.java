package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

/**
 * The jar's own library loads with nothing pointing at one. Surefire runs this class alone, in a JVM of its own with
 * {@code target/natives} on the class path (where {@code scripts/ci-check.sh java-tests} stages this platform's
 * library as the release build stages all five), and with neither {@code vortexrdf.library} nor
 * {@code VORTEX_RDF_JNI_LIB} set. Without a staged library it is skipped, unless {@code vortexrdf.bundled.required}.
 */
class BundledLibraryTest {
	@Test
	void loadsTheJarsCopyForThisPlatform() throws IOException {
		String platform = NativeLibrary.platform(System.getProperty("os.name"), System.getProperty("os.arch"))
				.orElse(null);
		boolean staged = platform != null && NativeLibrary.class.getResource(NativeLibrary.resource(platform)) != null;
		if (Boolean.getBoolean("vortexrdf.bundled.required")) {
			assertTrue(staged, "no native library staged under target/natives for " + platform);
		}
		assumeTrue(staged, "no native library staged for this platform");
		assertNull(System.getProperty("vortexrdf.library"));
		assertNull(System.getenv("VORTEX_RDF_JNI_LIB"));
		Path fixtures = Paths.get(System.getProperty("vortexrdf.fixtures"));
		long quads = Files.readAllLines(fixtures.resolve("oracle.tsv"), StandardCharsets.UTF_8).size();
		long handle = Native.open(fixtures.resolve("dictionary.vortex").toString(), false);
		try {
			assertEquals(quads, Native.size(handle));
		} finally {
			Native.close(handle);
		}
		assertEquals(NativeLibrary.Source.BUNDLED, NativeLibrary.source());
	}
}
