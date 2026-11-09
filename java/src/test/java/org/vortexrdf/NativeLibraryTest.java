package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.vortexrdf.NativeLibrary.choose;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.vortexrdf.NativeLibrary.Source;

/**
 * Where the native library comes from, without loading it: the platform names, the load order, unpacking the jar's
 * copy into a private directory, and clearing the copies earlier JVMs left behind.
 */
class NativeLibraryTest {
	@ParameterizedTest
	@CsvSource({ "Linux, amd64, linux-x86_64", "Linux, x86_64, linux-x86_64", "Linux, aarch64, linux-aarch64",
			"Mac OS X, x86_64, macos-x86_64", "Mac OS X, aarch64, macos-aarch64", "Darwin, arm64, macos-aarch64",
			"Windows 11, amd64, windows-x86_64", "Windows Server 2022, amd64, windows-x86_64" })
	void platformTable(String osName, String osArch, String platform) {
		assertEquals(Optional.of(platform), NativeLibrary.platform(osName, osArch));
	}

	@ParameterizedTest
	@CsvSource({ "Linux, ppc64le", "Linux, riscv64", "FreeBSD, amd64", "SunOS, sparcv9", "Windows 11, x86" })
	void unknownSystemsHaveNoPlatform(String osName, String osArch) {
		assertEquals(Optional.empty(), NativeLibrary.platform(osName, osArch));
	}

	@Test
	void fileNamesAndResources() {
		assertEquals("libvortex_rdf_jni.so", NativeLibrary.fileName("linux-aarch64"));
		assertEquals("libvortex_rdf_jni.dylib", NativeLibrary.fileName("macos-x86_64"));
		assertEquals("vortex_rdf_jni.dll", NativeLibrary.fileName("windows-x86_64"));
		assertEquals("/org/vortexrdf/native/linux-x86_64/libvortex_rdf_jni.so", NativeLibrary.resource("linux-x86_64"));
	}

	@Test
	void loadOrder() {
		assertEquals(Source.PROPERTY, choose("/a.so", "/b.so", true));
		assertEquals(Source.ENVIRONMENT, choose(null, "/b.so", true));
		assertEquals(Source.ENVIRONMENT, choose("  ", "/b.so", true));
		assertEquals(Source.BUNDLED, choose(null, "", true));
		assertEquals(Source.BUNDLED, choose("", " ", true));
		assertEquals(Source.SYSTEM, choose(null, null, false));
		assertEquals(Source.SYSTEM, choose("", "", false));
	}

	@Test
	void extractWritesAnOwnerOnlyCopy(@TempDir Path tmp) throws IOException {
		byte[] bytes = { 1, 2, 3, 4 };
		Path lib = NativeLibrary.extract(new ByteArrayInputStream(bytes), tmp, "libx.so");
		assertArrayEquals(bytes, Files.readAllBytes(lib));
		assertEquals("libx.so", lib.getFileName().toString());
		Path dir = lib.getParent();
		assertEquals(tmp, dir.getParent());
		assertTrue(dir.getFileName().toString().startsWith("vortex-rdf-jni-"));
		assertTrue(Files.exists(dir.resolve("in-use")));
		if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
			assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(dir)));
		}
	}

	@Test
	void extractRemovesItsDirectoryWhenTheCopyFails(@TempDir Path tmp) throws IOException {
		InputStream failing = new InputStream() {
			private int read;

			@Override
			public int read() throws IOException {
				if (read++ >= 3) {
					throw new IOException("read failed");
				}
				return 7;
			}
		};
		assertThrows(IOException.class, () -> NativeLibrary.extract(failing, tmp, "libx.so"));
		assertEquals(0, entries(tmp));
	}

	@Test
	void sweepRemovesLeftoversOnly(@TempDir Path tmp) throws IOException {
		Path leftover = dir(tmp, "vortex-rdf-jni-1", "libx.so");
		Path live = dir(tmp, "vortex-rdf-jni-2", "libx.so", "in-use");
		Path fresh = dir(tmp, "vortex-rdf-jni-3");
		Path other = dir(tmp, "something-else", "libx.so");
		NativeLibrary.sweep(tmp, "libx.so");
		assertFalse(Files.exists(leftover));
		assertTrue(Files.exists(live.resolve("libx.so")));
		assertTrue(Files.exists(fresh));
		assertTrue(Files.exists(other.resolve("libx.so")));
	}

	@Test
	void sweepSkipsSymbolicLinks(@TempDir Path tmp) throws IOException {
		Path target = dir(tmp, "target", "libx.so");
		Path link = tmp.resolve("vortex-rdf-jni-link");
		try {
			Files.createSymbolicLink(link, target);
		} catch (UnsupportedOperationException | IOException e) {
			assumeTrue(false, "no symbolic links on this file system");
		}
		NativeLibrary.sweep(tmp, "libx.so");
		assertTrue(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
		assertTrue(Files.exists(target.resolve("libx.so")));
	}

	@Test
	void sweepKeepsWhatExtractCreated(@TempDir Path tmp) throws IOException {
		Path lib = NativeLibrary.extract(new ByteArrayInputStream(new byte[] { 1 }), tmp, "libx.so");
		NativeLibrary.sweep(tmp, "libx.so");
		assertTrue(Files.exists(lib));
	}

	@Test
	void loadBundledNamesThePlatformAndCleansUp(@TempDir Path tmp) throws IOException {
		InputStream garbage = new ByteArrayInputStream(new byte[] { 0, 1, 2, 3 });
		UnsatisfiedLinkError e = assertThrows(UnsatisfiedLinkError.class,
				() -> NativeLibrary.loadBundled(garbage, tmp, "linux-x86_64"));
		assertTrue(e.getMessage().contains("linux-x86_64"), e.getMessage());
		assertTrue(e.getMessage().contains("java.io.tmpdir"), e.getMessage());
		assertTrue(e.getMessage().contains("vortexrdf.library"), e.getMessage());
		assertEquals(0, entries(tmp));
	}

	@Test
	void loadBundledSweepsLeftoversOnlyForWindows(@TempDir Path tmp) throws IOException {
		Path linuxLeftover = dir(tmp, "vortex-rdf-jni-1", "libvortex_rdf_jni.so");
		assertThrows(UnsatisfiedLinkError.class,
				() -> NativeLibrary.loadBundled(new ByteArrayInputStream(new byte[] { 0 }), tmp, "linux-x86_64"));
		assertTrue(Files.exists(linuxLeftover.resolve("libvortex_rdf_jni.so")));
		Path windowsLeftover = dir(tmp, "vortex-rdf-jni-2", "vortex_rdf_jni.dll");
		assertThrows(UnsatisfiedLinkError.class,
				() -> NativeLibrary.loadBundled(new ByteArrayInputStream(new byte[] { 0 }), tmp, "windows-x86_64"));
		assertFalse(Files.exists(windowsLeftover));
	}

	/** A directory {@code name} under {@code parent} holding an empty file per name in {@code files}. */
	private static Path dir(Path parent, String name, String... files) throws IOException {
		Path dir = Files.createDirectory(parent.resolve(name));
		for (String file : files) {
			Files.createFile(dir.resolve(file));
		}
		return dir;
	}

	private static long entries(Path dir) throws IOException {
		try (Stream<Path> listed = Files.list(dir)) {
			return listed.count();
		}
	}
}
