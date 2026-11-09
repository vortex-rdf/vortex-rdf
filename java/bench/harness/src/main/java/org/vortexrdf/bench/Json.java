package org.vortexrdf.bench;

import java.util.Collection;
import java.util.Map;

/** The worker's result, as JSON: maps (in their iteration order), collections, strings, numbers, booleans, null. */
public final class Json {
	private Json() {
	}

	public static String write(Object value) {
		StringBuilder out = new StringBuilder();
		write(value, out);
		return out.toString();
	}

	private static void write(Object v, StringBuilder out) {
		if (v == null) {
			out.append("null");
		} else if (v instanceof String s) {
			string(s, out);
		} else if (v instanceof Boolean b) {
			out.append(b);
		} else if (v instanceof Double || v instanceof Float) {
			double d = ((Number) v).doubleValue();
			if (!Double.isFinite(d)) {
				throw new IllegalArgumentException("JSON has no " + d);
			}
			out.append(d);
		} else if (v instanceof Number n) {
			out.append(n.longValue());
		} else if (v instanceof Map<?, ?> m) {
			out.append('{');
			boolean first = true;
			for (Map.Entry<?, ?> e : m.entrySet()) {
				if (!first) {
					out.append(',');
				}
				first = false;
				string(e.getKey().toString(), out);
				out.append(':');
				write(e.getValue(), out);
			}
			out.append('}');
		} else if (v instanceof Collection<?> c) {
			out.append('[');
			boolean first = true;
			for (Object item : c) {
				if (!first) {
					out.append(',');
				}
				first = false;
				write(item, out);
			}
			out.append(']');
		} else {
			throw new IllegalArgumentException("no JSON form for " + v.getClass().getName());
		}
	}

	private static void string(String s, StringBuilder out) {
		out.append('"');
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
			case '"' -> out.append("\\\"");
			case '\\' -> out.append("\\\\");
			case '\n' -> out.append("\\n");
			case '\r' -> out.append("\\r");
			case '\t' -> out.append("\\t");
			default -> {
				if (c < 0x20) {
					out.append(String.format("\\u%04x", (int) c));
				} else {
					out.append(c);
				}
			}
			}
		}
		out.append('"');
	}
}
