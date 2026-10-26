//! JNI entry points for `org.vortexrdf.Native`, the package-private native methods behind the public
//! `org.vortexrdf` classes. Thin: every function converts its arguments, calls [`engine::Store`], and turns an
//! error or a panic into an `org.vortexrdf.VortexException` (never unwinds into the JVM). A function that raises
//! an exception, or finds one pending after a JVM allocation, returns at once, with no other JNI call made while
//! it is pending.
//!
//! Conventions: a store handle is the address of a boxed [`engine::Store`] (the Java side owns it and never calls
//! after `close`); a term code is a non-negative `long`, and `-1` means "no code" (a free pattern position, a term
//! the dictionary lacks); a `null` string is a free pattern position, while `""` is the default graph.
#![allow(non_snake_case, clippy::missing_safety_doc)]

pub mod engine;

use engine::{Codes, ROW_BYTES, Store};
use jni_sys::{JNIEnv, jboolean, jclass, jint, jlong, jlongArray, jobject, jobjectArray, jstring};
use std::ffi::CString;
use std::panic::{AssertUnwindSafe, catch_unwind};

unsafe fn throw(env: *mut JNIEnv, msg: &str) {
    unsafe {
        let mut class = ((**env).v1_1.FindClass)(env, c"org/vortexrdf/VortexException".as_ptr());
        if class.is_null() {
            // The class could not be loaded, which left its own exception pending: replace it.
            ((**env).v1_1.ExceptionClear)(env);
            class = ((**env).v1_1.FindClass)(env, c"java/lang/RuntimeException".as_ptr());
        }
        let msg = CString::new(msg.replace('\0', " ")).unwrap_or_default();
        ((**env).v1_1.ThrowNew)(env, class, msg.as_ptr());
    }
}

/// Runs `f`; an `Err` or a panic becomes a pending Java exception and `None`.
unsafe fn guard<T>(env: *mut JNIEnv, f: impl FnOnce() -> engine::Result<T>) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => Some(v),
        Ok(Err(e)) => {
            unsafe { throw(env, &e.0) };
            None
        }
        Err(_) => {
            unsafe { throw(env, "panic in the vortex-rdf native library") };
            None
        }
    }
}

/// UTF-16, copied out by `GetStringRegion` into a Rust buffer, so the JVM allocates nothing; not
/// `GetStringUTFChars`, Java's modified UTF-8 (supplementary characters as surrogate pairs, NUL as two bytes),
/// which would corrupt literals holding emoji or NULs.
unsafe fn rust_string(env: *mut JNIEnv, s: jstring) -> Option<String> {
    if s.is_null() {
        return None;
    }
    unsafe {
        let n = ((**env).v1_1.GetStringLength)(env, s);
        let mut units = vec![0u16; n as usize];
        ((**env).v1_2.GetStringRegion)(env, s, 0, n, units.as_mut_ptr());
        Some(String::from_utf16_lossy(&units))
    }
}

/// A Java string; null, with an `OutOfMemoryError` pending, when the JVM cannot allocate it.
unsafe fn java_string(env: *mut JNIEnv, s: &str) -> jstring {
    let units: Vec<u16> = s.encode_utf16().collect();
    unsafe { ((**env).v1_1.NewString)(env, units.as_ptr(), units.len() as jint) }
}

/// A `String[]` of `strings`, a `null` element for a `None`; null, with an exception pending, when the JVM
/// cannot allocate it or one of its strings.
unsafe fn java_strings<'a>(
    env: *mut JNIEnv,
    strings: impl ExactSizeIterator<Item = Option<&'a str>>,
) -> jobjectArray {
    unsafe {
        let string_class = ((**env).v1_1.FindClass)(env, c"java/lang/String".as_ptr());
        if string_class.is_null() {
            return std::ptr::null_mut();
        }
        let arr = ((**env).v1_1.NewObjectArray)(
            env,
            strings.len() as jint,
            string_class,
            std::ptr::null_mut(),
        );
        if arr.is_null() {
            return std::ptr::null_mut();
        }
        for (i, s) in strings.enumerate() {
            if let Some(s) = s {
                let js = java_string(env, s);
                if js.is_null() {
                    return std::ptr::null_mut();
                }
                ((**env).v1_1.SetObjectArrayElement)(env, arr, i as jint, js);
                ((**env).v1_1.DeleteLocalRef)(env, js);
            }
        }
        arr
    }
}

/// A `long[]` of `values`; null, with an `OutOfMemoryError` pending, when the JVM cannot allocate it.
unsafe fn java_longs(env: *mut JNIEnv, values: &[i64]) -> jlongArray {
    unsafe {
        let arr = ((**env).v1_1.NewLongArray)(env, values.len() as jint);
        if !arr.is_null() {
            ((**env).v1_1.SetLongArrayRegion)(env, arr, 0, values.len() as jint, values.as_ptr());
        }
        arr
    }
}

unsafe fn store<'a>(h: jlong) -> &'a Store {
    unsafe { &*(h as *const Store) }
}

/// A term code from a `long`: `None` for `-1` (and any negative value).
fn code(c: jlong) -> Option<u64> {
    (c >= 0).then_some(c as u64)
}

fn pattern(s: jlong, p: jlong, o: jlong, g: jlong) -> Codes {
    [code(s), code(p), code(o), code(g)]
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_open(
    env: *mut JNIEnv,
    _c: jclass,
    path: jstring,
    in_memory: jboolean,
) -> jlong {
    let path = unsafe { rust_string(env, path) }.unwrap_or_default();
    unsafe {
        guard(env, || {
            Ok(Box::into_raw(Box::new(Store::open(&path, in_memory)?)) as jlong)
        })
    }
    .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_close(
    _env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) {
    if h != 0 {
        drop(unsafe { Box::from_raw(h as *mut Store) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_size(
    _env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) -> jlong {
    unsafe { store(h) }.size() as jlong
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_layout(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) -> jstring {
    unsafe { java_string(env, &store(h).layout()) }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_indexes(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) -> jobjectArray {
    let indexes = unsafe { store(h) }.indexes();
    unsafe { java_strings(env, indexes.iter().map(|s| Some(s.as_str()))) }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_count(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    s: jstring,
    p: jstring,
    o: jstring,
    g: jstring,
) -> jlong {
    let [s, p, o, g] = unsafe { [s, p, o, g].map(|t| rust_string(env, t)) };
    unsafe {
        guard(env, || {
            Ok(store(h).count(s.as_deref(), p.as_deref(), o.as_deref(), g.as_deref())? as jlong)
        })
    }
    .unwrap_or(0)
}

/// The matching quads as a flat `String[]`, four terms per quad; `limit < 0` takes every row.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_quads(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    s: jstring,
    p: jstring,
    o: jstring,
    g: jstring,
    offset: jlong,
    limit: jint,
) -> jobjectArray {
    let [s, p, o, g] = unsafe { [s, p, o, g].map(|t| rust_string(env, t)) };
    let rows = unsafe {
        guard(env, || {
            store(h).quads(
                s.as_deref(),
                p.as_deref(),
                o.as_deref(),
                g.as_deref(),
                offset.max(0) as u64,
                (limit >= 0).then_some(limit as usize),
            )
        })
    };
    let Some(rows) = rows else {
        return std::ptr::null_mut();
    };
    let flat: Vec<&str> = rows.iter().flatten().map(String::as_str).collect();
    unsafe { java_strings(env, flat.into_iter().map(Some)) }
}

/// `-1` for a store without a term dictionary.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_termCount(
    _env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) -> jlong {
    let store = unsafe { store(h) };
    if store.has_dictionary() {
        store.term_count().map_or(-1, |n| n as jlong)
    } else {
        -1
    }
}

/// `[defaultGraph or -1, literals.start, literals.end, iris.start, iris.end, blanks.start, blanks.end]`, ends
/// exclusive.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_ranges(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
) -> jlongArray {
    let Some(r) = (unsafe { guard(env, || store(h).ranges()) }) else {
        return std::ptr::null_mut();
    };
    let values = [
        r.default_graph.map_or(-1, |c| c as i64),
        r.literals.0 as i64,
        r.literals.1 as i64,
        r.iris.0 as i64,
        r.iris.1 as i64,
        r.blanks.0 as i64,
        r.blanks.1 as i64,
    ];
    unsafe { java_longs(env, &values) }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_encode(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    term: jstring,
) -> jlong {
    let term = unsafe { rust_string(env, term) }.unwrap_or_default();
    unsafe {
        guard(env, || {
            Ok(store(h).encode(&term)?.map_or(-1, |c| c as jlong))
        })
    }
    .unwrap_or(-1)
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_decode(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    c: jlong,
) -> jstring {
    let Some(c) = code(c) else {
        return std::ptr::null_mut();
    };
    match unsafe { guard(env, || store(h).decode(c)) } {
        Some(Some(s)) => unsafe { java_string(env, &s) },
        _ => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_decodeMany(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    codes: jlongArray,
) -> jobjectArray {
    unsafe {
        let n = ((**env).v1_1.GetArrayLength)(env, codes) as usize;
        let mut buf = vec![0i64; n];
        ((**env).v1_1.GetLongArrayRegion)(env, codes, 0, n as jint, buf.as_mut_ptr());
        // A negative code is no code: u64::MAX is outside every dictionary, so it decodes to `None`.
        let codes: Vec<u64> = buf.iter().map(|&c| code(c).unwrap_or(u64::MAX)).collect();
        let Some(terms) = guard(env, || store(h).decode_many(&codes)) else {
            return std::ptr::null_mut();
        };
        java_strings(env, terms.iter().map(|t| t.as_deref()))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_countCodes(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    s: jlong,
    p: jlong,
    o: jlong,
    g: jlong,
) -> jlong {
    unsafe {
        guard(env, || {
            Ok(store(h).count_codes(pattern(s, p, o, g))? as jlong)
        })
    }
    .unwrap_or(0)
}

/// Up to `maxRows` matches after `offset`, as `(s, p, o, g)` codes written straight into the direct buffer `buf`
/// from its start (native byte order, `32 * maxRows` bytes at least). Returns the rows written.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_vortexrdf_Native_matchCodes(
    env: *mut JNIEnv,
    _c: jclass,
    h: jlong,
    s: jlong,
    p: jlong,
    o: jlong,
    g: jlong,
    buf: jobject,
    offset: jlong,
    max_rows: jint,
) -> jint {
    unsafe {
        let addr = ((**env).v1_4.GetDirectBufferAddress)(env, buf) as *mut u8;
        let cap = ((**env).v1_4.GetDirectBufferCapacity)(env, buf);
        let max_rows = max_rows.max(0) as usize;
        let len = max_rows.checked_mul(ROW_BYTES);
        let Some(len) = len.filter(|&len| !addr.is_null() && cap >= 0 && cap as usize >= len)
        else {
            throw(env, "matchCodes: not a direct buffer of 32 * maxRows bytes");
            return 0;
        };
        // The buffer's memory, valid for this call: the Java caller holds the buffer.
        let out = std::slice::from_raw_parts_mut(addr, len);
        guard(env, || {
            Ok(
                store(h).match_codes(pattern(s, p, o, g), offset.max(0) as u64, max_rows, out)?
                    as jint,
            )
        })
        .unwrap_or(0)
    }
}
