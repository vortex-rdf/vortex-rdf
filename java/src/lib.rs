//! JNI entry points for `org.vortexrdf.Native`, the package-private native methods behind the public
//! `org.vortexrdf` classes. Thin: every function converts its arguments, calls [`engine::Store`] inside
//! [`call`], and returns its answer; an error or a panic becomes an `org.vortexrdf.VortexException`
//! ([`ThrowVortexException`]) and never unwinds into the JVM.
//!
//! Conventions: a store handle is the address of a boxed [`engine::Store`] (the Java side owns it and never calls
//! after `close`); a term code is a non-negative `long`, and `-1` means "no code" (a free pattern position, a term
//! the dictionary lacks); a `null` string is a free pattern position, while `""` is the default graph.

pub mod engine;

use engine::{ROW_BYTES, Store};
use jni::errors::ErrorPolicy;
use jni::objects::{JByteBuffer, JClass, JLongArray, JObjectArray, JString};
use jni::strings::JNIString;
use jni::sys::{jboolean, jint, jlong};
use jni::{Env, EnvUnowned, jni_str};
use vortex_rdf_core::CodePattern;

/// A native call's failure: the store's own, or a JNI call's.
type Failure = Box<dyn std::error::Error>;

/// Raises a failure or a panic as an `org.vortexrdf.VortexException` and returns the default value. An exception
/// the JVM raised itself (a failed allocation, say) is left pending instead.
struct ThrowVortexException;

impl ThrowVortexException {
    fn throw<T: Default>(env: &mut Env<'_>, message: String) -> jni::errors::Result<T> {
        if !env.exception_check() {
            // An `Err` here only reports the exception now pending, which is the point.
            let _ = env.throw_new(
                jni_str!("org/vortexrdf/VortexException"),
                JNIString::from(message),
            );
        }
        Ok(T::default())
    }
}

impl<T: Default> ErrorPolicy<T, Failure> for ThrowVortexException {
    type Captures<'local: 'native_method, 'native_method> = ();

    fn on_error<'local: 'native_method, 'native_method>(
        env: &mut Env<'local>,
        _: &mut (),
        failure: Failure,
    ) -> jni::errors::Result<T> {
        Self::throw(env, failure.to_string())
    }

    fn on_panic<'local: 'native_method, 'native_method>(
        env: &mut Env<'local>,
        _: &mut (),
        _: Box<dyn std::any::Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        Self::throw(env, "panic in the vortex-rdf native library".into())
    }
}

/// Runs a native method's body with its [`Env`] under [`ThrowVortexException`].
fn call<'local, T: Default + 'local>(
    env: &mut EnvUnowned<'local>,
    body: impl FnOnce(&mut Env<'local>) -> Result<T, Failure>,
) -> T {
    env.with_env(body).resolve::<ThrowVortexException>()
}

/// The store a handle addresses.
fn store<'a>(h: jlong) -> &'a Store {
    // SAFETY: the Java side passes only handles `open` returned, never after `close`, and `close` waits for the
    // calls in progress.
    unsafe { &*(h as *const Store) }
}

/// A pattern position's term: `None` for a `null` string, a free position.
fn term(env: &Env<'_>, s: &JString<'_>) -> Result<Option<String>, Failure> {
    Ok(if s.is_null() {
        None
    } else {
        Some(s.try_to_string(env)?)
    })
}

/// A `String[]` of `strings`, a `null` element for a `None`.
fn java_strings<'local, 'a>(
    env: &mut Env<'local>,
    strings: impl ExactSizeIterator<Item = Option<&'a str>>,
) -> Result<JObjectArray<'local, JString<'local>>, Failure> {
    let array = JObjectArray::<JString>::new(env, strings.len(), JString::null())?;
    for (i, s) in strings.enumerate() {
        if let Some(s) = s {
            let element = JString::from_str(env, s)?;
            array.set_element(env, i, &element)?;
            env.delete_local_ref(element);
        }
    }
    Ok(array)
}

/// A term code from a `long`: `None` for `-1` (and any negative value).
fn code(c: jlong) -> Option<u64> {
    (c >= 0).then_some(c as u64)
}

fn pattern(s: jlong, p: jlong, o: jlong, g: jlong) -> CodePattern {
    CodePattern::new(code(s), code(p), code(o), code(g))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_open<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    path: JString<'local>,
    in_memory: jboolean,
) -> jlong {
    call(&mut env, |env| {
        let store = Store::open(&path.try_to_string(env)?, in_memory)?;
        Ok(Box::into_raw(Box::new(store)) as jlong)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_close<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) {
    call(&mut env, |_| {
        if h != 0 {
            // SAFETY: as for `store`, and nothing uses the handle after `close`.
            drop(unsafe { Box::from_raw(h as *mut Store) });
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_size<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> jlong {
    call(&mut env, |_| Ok(store(h).size() as jlong))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_layout<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> JString<'local> {
    call(&mut env, |env| {
        Ok(JString::from_str(env, store(h).layout())?)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_indexes<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> JObjectArray<'local, JString<'local>> {
    call(&mut env, |env| {
        let indexes = store(h).indexes();
        java_strings(env, indexes.iter().map(|s| Some(s.as_str())))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_count<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: JString<'local>,
    p: JString<'local>,
    o: JString<'local>,
    g: JString<'local>,
) -> jlong {
    call(&mut env, |env| {
        let [s, p, o, g] = [
            term(env, &s)?,
            term(env, &p)?,
            term(env, &o)?,
            term(env, &g)?,
        ];
        Ok(store(h).count(s.as_deref(), p.as_deref(), o.as_deref(), g.as_deref())? as jlong)
    })
}

/// The matching quads as a flat `String[]`, four terms per quad; `limit < 0` takes every row.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_quads<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: JString<'local>,
    p: JString<'local>,
    o: JString<'local>,
    g: JString<'local>,
    offset: jlong,
    limit: jint,
) -> JObjectArray<'local, JString<'local>> {
    call(&mut env, |env| {
        let [s, p, o, g] = [
            term(env, &s)?,
            term(env, &p)?,
            term(env, &o)?,
            term(env, &g)?,
        ];
        let rows = store(h).quads(
            s.as_deref(),
            p.as_deref(),
            o.as_deref(),
            g.as_deref(),
            offset.max(0) as u64,
            (limit >= 0).then_some(limit as usize),
        )?;
        let flat: Vec<&str> = rows.iter().flatten().map(String::as_str).collect();
        java_strings(env, flat.into_iter().map(Some))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_hasDictionary<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> jboolean {
    call(&mut env, |_| Ok(store(h).has_dictionary()))
}

/// vortex-rdf's encoded-search work counters (`VORTEX_RDF_COUNTERS=1`; all zero otherwise), cumulative in this
/// process, in [`Java_org_vortexrdf_Native_engineCounterNames`] order.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_engineCounters<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) -> JLongArray<'local> {
    call(&mut env, |env| {
        let values = vortex_rdf_encoded_search::counters::snapshot().map(|v| v as i64);
        let array = JLongArray::new(env, values.len())?;
        array.set_region(env, 0, &values)?;
        Ok(array)
    })
}

/// The names of [`Java_org_vortexrdf_Native_engineCounters`]'s values.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_engineCounterNames<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) -> JObjectArray<'local, JString<'local>> {
    call(&mut env, |env| {
        let names = vortex_rdf_encoded_search::counters::NAMES;
        java_strings(env, names.iter().map(|&n| Some(n)))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_termCount<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> jlong {
    call(&mut env, |_| Ok(store(h).term_count()? as jlong))
}

/// `[defaultGraph or -1, literals.start, literals.end, iris.start, iris.end, blanks.start, blanks.end]`, ends
/// exclusive.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_ranges<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
) -> JLongArray<'local> {
    call(&mut env, |env| {
        let r = store(h).ranges()?;
        let values = [
            r.default_graph.map_or(-1, |c| c as i64),
            r.literals.start as i64,
            r.literals.end as i64,
            r.iris.start as i64,
            r.iris.end as i64,
            r.blanks.start as i64,
            r.blanks.end as i64,
        ];
        let array = JLongArray::new(env, values.len())?;
        array.set_region(env, 0, &values)?;
        Ok(array)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_encode<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    term: JString<'local>,
) -> jlong {
    call(&mut env, |env| {
        let code = store(h).encode(&term.try_to_string(env)?)?;
        Ok(code.map_or(-1, |c| c as jlong))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_decode<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    c: jlong,
) -> JString<'local> {
    call(&mut env, |env| {
        Ok(
            match code(c).map(|c| store(h).decode(c)).transpose()?.flatten() {
                Some(term) => JString::from_str(env, term)?,
                None => JString::null(),
            },
        )
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_decodeMany<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    codes: JLongArray<'local>,
) -> JObjectArray<'local, JString<'local>> {
    call(&mut env, |env| {
        let mut buf = vec![0i64; codes.len(env)?];
        codes.get_region(env, 0, &mut buf)?;
        // A negative code is no code: u64::MAX is outside every dictionary, so it decodes to `None`.
        let codes: Vec<u64> = buf.iter().map(|&c| code(c).unwrap_or(u64::MAX)).collect();
        let terms = store(h).decode_many(&codes)?;
        java_strings(env, terms.iter().map(|t| t.as_deref()))
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_countCodes<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: jlong,
    p: jlong,
    o: jlong,
    g: jlong,
) -> jlong {
    call(&mut env, |_| {
        Ok(store(h).count_codes(pattern(s, p, o, g))? as jlong)
    })
}

/// Up to `maxRows` matches after `offset`, as `(s, p, o, g)` codes written straight into the direct buffer `buf`
/// from its start (native byte order, `32 * maxRows` bytes at least). Returns the rows written.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_matchCodes<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: jlong,
    p: jlong,
    o: jlong,
    g: jlong,
    buf: JByteBuffer<'local>,
    offset: jlong,
    max_rows: jint,
) -> jint {
    call(&mut env, |env| {
        const NOT_A_BUFFER: &str = "matchCodes: not a direct buffer of 32 * maxRows bytes";
        let addr = env
            .get_direct_buffer_address(&buf)
            .map_err(|_| NOT_A_BUFFER)?;
        let capacity = env
            .get_direct_buffer_capacity(&buf)
            .map_err(|_| NOT_A_BUFFER)?;
        let rows = max_rows.max(0) as usize;
        if rows.checked_mul(ROW_BYTES).is_none_or(|len| len > capacity) {
            return Err(NOT_A_BUFFER.into());
        }
        // SAFETY: the buffer's memory holds `rows` rows (checked above) and stays valid for this call, the Java
        // caller holding the buffer.
        let out = unsafe { std::slice::from_raw_parts_mut(addr.cast::<[u8; ROW_BYTES]>(), rows) };
        let written = store(h).match_codes(pattern(s, p, o, g), offset.max(0) as u64, out)?;
        Ok(written as jint)
    })
}
