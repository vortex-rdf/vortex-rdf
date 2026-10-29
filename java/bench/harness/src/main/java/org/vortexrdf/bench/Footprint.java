package org.vortexrdf.bench;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.management.JMException;
import javax.management.ObjectName;

/**
 * What the process holds in memory once the collector has run: the live Java heap, and the native memory that is
 * not the JVM's own, which is where a JNI library keeps what lives outside the heap. Memory-mapped files are the
 * kernel's page cache and count in neither.
 *
 * <p>The native figure is glibc's allocator's bytes in use ({@code mallinfo2}'s {@code uordblks} plus
 * {@code hblkhd}), less the JVM's own allocations as its native memory tracking reports them, so the worker JVM
 * runs with {@code -XX:NativeMemoryTracking=summary}: its compilers' arenas alone swing by tens of megabytes as
 * they come and go. What Java code allocates outside the heap itself, as direct buffers and foreign memory do,
 * the tracking files under Other, and that stays in the figure.
 *
 * @param heapBytes the heap's used bytes after a full collection
 * @param nativeBytes the native bytes in use that are not the JVM's own
 */
public record Footprint(long heapBytes, long nativeBytes) {
	/** {@code struct mallinfo2}: ten {@code size_t} counters. */
	private static final StructLayout MALLINFO2 = MemoryLayout.structLayout(JAVA_LONG.withName("arena"),
			JAVA_LONG.withName("ordblks"), JAVA_LONG.withName("smblks"), JAVA_LONG.withName("hblks"),
			JAVA_LONG.withName("hblkhd"), JAVA_LONG.withName("usmblks"), JAVA_LONG.withName("fsmblks"),
			JAVA_LONG.withName("uordblks"), JAVA_LONG.withName("fordblks"), JAVA_LONG.withName("keepcost"));
	private static final long MMAPPED = MALLINFO2.byteOffset(PathElement.groupElement("hblkhd"));
	private static final long ALLOCATED = MALLINFO2.byteOffset(PathElement.groupElement("uordblks"));

	/** Resolved on first use, so a worker that never samples never looks for glibc. */
	private static final class Glibc {
		static final MethodHandle MALLINFO2_CALL = Linker.nativeLinker().defaultLookup().find("mallinfo2")
				.map(f -> Linker.nativeLinker().downcallHandle(f, FunctionDescriptor.of(MALLINFO2)))
				.orElseThrow(() -> new UnsupportedOperationException(
						"the native figure reads glibc's mallinfo2, which this C library does not have"));
	}

	/** The JVM's total malloc, and the part of it that Java code asked for, in a tracking summary. */
	private static final java.util.regex.Pattern JVM_MALLOC = java.util.regex.Pattern.compile("^\\s*malloc: (\\d+)KB",
			java.util.regex.Pattern.MULTILINE);
	private static final java.util.regex.Pattern JAVA_CODE_MALLOC = java.util.regex.Pattern
			.compile("^-\\s+Other \\(.*\\R\\s*\\(malloc=(\\d+)KB", java.util.regex.Pattern.MULTILINE);

	/** Collect, then read both figures. */
	public static Footprint sample() {
		collect();
		return new Footprint(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), nativeOutsideTheJvm());
	}

	/**
	 * What was released between this sample and {@code later}. A figure that reads higher later is the noise of
	 * the collectors and the JIT around a store that released nothing measurable, so it counts as nothing.
	 */
	public Footprint minus(Footprint later) {
		return new Footprint(Math.max(0, heapBytes - later.heapBytes), Math.max(0, nativeBytes - later.nativeBytes));
	}

	/** The worker's JSON shape. */
	public Map<String, Object> toJson() {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("heapBytes", heapBytes);
		out.put("nativeBytes", nativeBytes);
		return out;
	}

	/**
	 * Two full collections, with a pause between them for the cleaner thread: native memory that an unreachable
	 * object frees through a cleaner, as a direct buffer does, is released only after the collection that found it.
	 */
	private static void collect() {
		System.gc();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		System.gc();
	}

	private static long nativeOutsideTheJvm() {
		String summary = trackingSummary();
		java.util.regex.Matcher jvm = JVM_MALLOC.matcher(summary), javaCode = JAVA_CODE_MALLOC.matcher(summary);
		if (!jvm.find()) {
			throw new IllegalStateException("the native figure needs a JVM started with -XX:NativeMemoryTracking=summary");
		}
		long ownKb = Long.parseLong(jvm.group(1)) - (javaCode.find() ? Long.parseLong(javaCode.group(1)) : 0);
		return mallocInUse() - ownKb * 1024;
	}

	private static String trackingSummary() {
		try {
			return (String) ManagementFactory.getPlatformMBeanServer().invoke(
					new ObjectName("com.sun.management:type=DiagnosticCommand"), "vmNativeMemory",
					new Object[] { new String[] { "summary" } }, new String[] { String[].class.getName() });
		} catch (JMException e) {
			throw new IllegalStateException("could not read the JVM's native memory tracking", e);
		}
	}

	private static long mallocInUse() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment info = (MemorySegment) Glibc.MALLINFO2_CALL.invokeExact((SegmentAllocator) arena);
			return info.get(JAVA_LONG, ALLOCATED) + info.get(JAVA_LONG, MMAPPED);
		} catch (Throwable t) {
			throw new IllegalStateException("mallinfo2 failed", t);
		}
	}
}
