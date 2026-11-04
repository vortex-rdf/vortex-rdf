package org.vortexrdf.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * This process's resident memory, as {@code /proc/self/status} splits it: anonymous pages ({@code RssAnon}: the Java
 * heap, the native allocations, the thread stacks) and the pages of mapped files ({@code RssFile}: the code, and a
 * memory-mapped store's file). Linux only.
 *
 * @param anonBytes {@code RssAnon}, in bytes
 * @param fileBytes {@code RssFile}, in bytes
 */
public record Rss(long anonBytes, long fileBytes) {
	private static final Path STATUS = Path.of("/proc/self/status");
	private static final java.util.regex.Pattern ANON = java.util.regex.Pattern.compile("^RssAnon:\\s+(\\d+) kB",
			java.util.regex.Pattern.MULTILINE);
	private static final java.util.regex.Pattern FILE = java.util.regex.Pattern.compile("^RssFile:\\s+(\\d+) kB",
			java.util.regex.Pattern.MULTILINE);

	/** Both figures, now. */
	public static Rss read() {
		String status;
		try {
			status = Files.readString(STATUS);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new Rss(kilobytes(status, ANON, "RssAnon") * 1024, kilobytes(status, FILE, "RssFile") * 1024);
	}

	/** {@code RssAnon + RssFile}, the figure a {@link Sampler}'s peak is the highest of. */
	public long total() {
		return anonBytes + fileBytes;
	}

	private static long kilobytes(String status, java.util.regex.Pattern field, String name) {
		java.util.regex.Matcher m = field.matcher(status);
		if (!m.find()) {
			throw new IllegalStateException(STATUS + " has no " + name + " field");
		}
		return Long.parseLong(m.group(1));
	}

	/**
	 * Keeps the highest {@link #read} while the code it brackets runs: one sample at {@link #start}, one every
	 * {@link #INTERVAL} on a daemon thread of its own, and a last one at {@link #close}. The peak is the whole sample
	 * with the highest {@link #total}, so its two figures come from the same instant.
	 */
	public static final class Sampler implements AutoCloseable {
		/** The pause between two of the thread's samples. */
		public static final Duration INTERVAL = Duration.ofMillis(2);

		private final Thread thread = Thread.ofPlatform().daemon().name("rss-sampler").unstarted(this::sample);
		private volatile boolean stopped;
		private volatile Rss peak = read();
		/** What ended the thread early; read once it has been joined. */
		private Throwable error;

		private Sampler() {
		}

		/** A sampler that has taken its first sample and started its thread. */
		public static Sampler start() {
			Sampler s = new Sampler();
			s.thread.start();
			return s;
		}

		/** The highest sample so far; once closed, the highest of the whole run. */
		public Rss peak() {
			return peak;
		}

		/** Stop the thread, take a last sample, and throw what ended the thread early, if anything did. */
		@Override
		public void close() {
			stopped = true;
			try {
				thread.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("interrupted waiting for the RSS sampler's thread", e);
			}
			keep(read());
			if (error instanceof Error e) {
				throw e;
			}
			if (error != null) {
				throw error instanceof RuntimeException e ? e : new IllegalStateException(error);
			}
		}

		private void sample() {
			try {
				while (!stopped) {
					Thread.sleep(INTERVAL);
					keep(read());
				}
			} catch (Throwable t) {
				error = t;
			}
		}

		/** Keep {@code s} if it is the highest yet. One thread at a time keeps: the sampler's, then the closing one. */
		private void keep(Rss s) {
			if (s.total() > peak.total()) {
				peak = s;
			}
		}
	}
}
