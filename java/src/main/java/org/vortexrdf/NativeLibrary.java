package org.vortexrdf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Where {@code libvortex_rdf_jni} comes from, and loading it. In order: the path in the system property
 * {@code vortexrdf.library}; the path in the environment variable {@code VORTEX_RDF_JNI_LIB}; the copy the jar
 * carries for this platform, under {@code org/vortexrdf/native/<platform>/}; {@link System#loadLibrary} under the name
 * {@code vortex_rdf_jni} ({@code java.library.path}). A blank property or variable counts as unset. The jar's copy
 * comes before {@code java.library.path} because it is the library this jar was released with.
 * <p>
 * The jar's copy is unpacked into a directory of its own under {@code java.io.tmpdir} (readable by its owner only on
 * POSIX file systems), beside an {@code in-use} marker, and all three are deleted at exit. Windows cannot delete a
 * loaded library, so there the directory outlives the JVM, without its marker, and the next JVM's sweep removes it;
 * only Windows sweeps, since elsewhere nothing is left to sweep, and a sweep of a shared temporary directory could be
 * raced into another user's files. A JVM killed before exit leaves its directory, marker included.
 */
final class NativeLibrary {
	/** The step that found the library. */
	enum Source {
		PROPERTY, ENVIRONMENT, BUNDLED, SYSTEM
	}

	/** Present while the JVM that unpacked a directory is running. */
	static final String MARKER = "in-use";
	static final String DIRECTORY_PREFIX = "vortex-rdf-jni-";
	private static final String NAME = "vortex_rdf_jni";
	private static final String REMEDY = "set -Dvortexrdf.library=<path to libvortex_rdf_jni> or VORTEX_RDF_JNI_LIB";

	private static Source source;

	private NativeLibrary() {
	}

	/** Loads the library once, from the first place in the order above that has one. */
	static synchronized void load() {
		if (source != null) {
			return;
		}
		String property = System.getProperty("vortexrdf.library");
		String environment = System.getenv("VORTEX_RDF_JNI_LIB");
		String osName = System.getProperty("os.name");
		String osArch = System.getProperty("os.arch");
		String platform = platform(osName, osArch).orElse(null);
		InputStream bundled = platform == null ? null : NativeLibrary.class.getResourceAsStream(resource(platform));
		try {
			Source chosen = choose(property, environment, bundled != null);
			switch (chosen) {
			case PROPERTY:
				loadFile(property, "-Dvortexrdf.library");
				break;
			case ENVIRONMENT:
				loadFile(environment, "VORTEX_RDF_JNI_LIB");
				break;
			case BUNDLED:
				loadBundled(bundled, Paths.get(System.getProperty("java.io.tmpdir")), platform);
				break;
			default:
				loadSystem(osName, osArch);
			}
			source = chosen;
		} finally {
			if (bundled != null) {
				try {
					bundled.close();
				} catch (IOException e) {
					// the jar's entry is read; nothing is left to release
				}
			}
		}
	}

	/** The step that loaded the library, or null before {@link #load()} has. */
	static synchronized Source source() {
		return source;
	}

	/** The jar's name for a system, {@code <os>-<arch>}, or empty for one the jar carries no library for. */
	static Optional<String> platform(String osName, String osArch) {
		if (osName == null || osArch == null) {
			return Optional.empty();
		}
		String os;
		if (osName.startsWith("Linux")) {
			os = "linux";
		} else if (osName.startsWith("Mac") || osName.equals("Darwin")) {
			os = "macos";
		} else if (osName.startsWith("Windows")) {
			os = "windows";
		} else {
			return Optional.empty();
		}
		String arch;
		if (osArch.equals("amd64") || osArch.equals("x86_64")) {
			arch = "x86_64";
		} else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
			arch = "aarch64";
		} else {
			return Optional.empty();
		}
		return Optional.of(os + "-" + arch);
	}

	/** The library's file name on a platform. */
	static String fileName(String platform) {
		if (platform.startsWith("windows-")) {
			return NAME + ".dll";
		}
		return "lib" + NAME + (platform.startsWith("macos-") ? ".dylib" : ".so");
	}

	/** The jar entry that holds a platform's library. */
	static String resource(String platform) {
		return "/org/vortexrdf/native/" + platform + "/" + fileName(platform);
	}

	/** The first step of the order that applies; a blank property or variable counts as unset. */
	static Source choose(String property, String environment, boolean bundled) {
		if (!isBlank(property)) {
			return Source.PROPERTY;
		}
		if (!isBlank(environment)) {
			return Source.ENVIRONMENT;
		}
		return bundled ? Source.BUNDLED : Source.SYSTEM;
	}

	/**
	 * Copies {@code library} into a new directory under {@code tmpdir}, beside the {@link #MARKER}, each registered for
	 * deletion at exit, and returns the copy. A failed copy leaves nothing behind.
	 */
	static Path extract(InputStream library, Path tmpdir, String fileName) throws IOException {
		Path dir = Files.createTempDirectory(tmpdir, DIRECTORY_PREFIX);
		dir.toFile().deleteOnExit();
		try {
			Files.createFile(dir.resolve(MARKER)).toFile().deleteOnExit();
			Path copy = dir.resolve(fileName);
			Files.copy(library, copy);
			copy.toFile().deleteOnExit();
			return copy;
		} catch (IOException e) {
			deleteDirectory(dir);
			throw e;
		}
	}

	/**
	 * Removes what JVMs that have exited left in {@code tmpdir}: directories named {@link #DIRECTORY_PREFIX}{@code *}
	 * that hold {@code fileName} and no {@link #MARKER}. A running JVM keeps its marker, and creates it before its
	 * copy, so its directory is never one of these. Symbolic links are skipped, and every failure is ignored.
	 */
	static void sweep(Path tmpdir, String fileName) {
		try (DirectoryStream<Path> dirs = Files.newDirectoryStream(tmpdir, DIRECTORY_PREFIX + "*")) {
			for (Path dir : dirs) {
				if (Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)
						&& Files.exists(dir.resolve(fileName), LinkOption.NOFOLLOW_LINKS)
						&& !Files.exists(dir.resolve(MARKER), LinkOption.NOFOLLOW_LINKS)) {
					deleteDirectory(dir);
				}
			}
		} catch (IOException | DirectoryIteratorException | SecurityException e) {
			// a sweep that cannot read tmpdir leaves it as it is
		}
	}

	/**
	 * Unpacks {@code library} into {@code tmpdir} for {@code platform}, after sweeping it on Windows, and loads the
	 * copy. On failure the copy's directory is removed and the error names the platform, the copy and what to set
	 * instead.
	 */
	static Path loadBundled(InputStream library, Path tmpdir, String platform) {
		String fileName = fileName(platform);
		if (platform.startsWith("windows-")) {
			sweep(tmpdir, fileName);
		}
		Path copy = null;
		try {
			copy = extract(library, tmpdir, fileName);
			System.load(copy.toAbsolutePath().toString());
			return copy;
		} catch (IOException | UnsatisfiedLinkError e) {
			if (copy != null) {
				deleteDirectory(copy.getParent());
			}
			String where = copy != null ? copy.toString() : "unpacking into " + tmpdir;
			UnsatisfiedLinkError error = new UnsatisfiedLinkError("cannot load the vortex-rdf native library bundled "
					+ "for " + platform + " (" + where + "): " + e.getMessage() + "; set -Djava.io.tmpdir to a "
					+ "directory that allows executing files, or -Dvortexrdf.library=<path to libvortex_rdf_jni>");
			error.initCause(e);
			throw error;
		}
	}

	private static void loadFile(String path, String origin) {
		try {
			System.load(Paths.get(path).toAbsolutePath().toString());
		} catch (UnsatisfiedLinkError e) {
			UnsatisfiedLinkError error = new UnsatisfiedLinkError(
					"cannot load the vortex-rdf native library " + path + " (from " + origin + "): " + e.getMessage());
			error.initCause(e);
			throw error;
		}
	}

	private static void loadSystem(String osName, String osArch) {
		try {
			System.loadLibrary(NAME);
		} catch (UnsatisfiedLinkError e) {
			UnsatisfiedLinkError error = new UnsatisfiedLinkError("cannot load the vortex-rdf native library: this jar "
					+ "holds none for " + osName + "/" + osArch + ", and " + NAME + " is not on java.library.path; "
					+ REMEDY + ": " + e.getMessage());
			error.initCause(e);
			throw error;
		}
	}

	private static boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

	/** Deletes a directory's entries and then the directory, ignoring every failure. */
	private static void deleteDirectory(Path dir) {
		try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
			for (Path entry : entries) {
				try {
					Files.deleteIfExists(entry);
				} catch (IOException | SecurityException e) {
					// what cannot be deleted now is left for a later sweep
				}
			}
		} catch (IOException | DirectoryIteratorException | SecurityException e) {
			// as above
		}
		try {
			Files.deleteIfExists(dir);
		} catch (IOException | SecurityException e) {
			// as above
		}
	}
}
