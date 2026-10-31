//! JNI entry points for `org.vortexrdf.Native`, the package-private native methods behind the public
//! `org.vortexrdf` classes. Thin: every function converts its arguments, calls [`engine::Store`] inside
//! [`call`], and returns its answer; an error or a panic becomes an `org.vortexrdf.VortexException`
//! ([`ThrowVortexException`]) and never unwinds into the JVM.
//!
//! Conventions: a store handle is the address of a boxed [`engine::Store`] (the Java side owns it and never calls
//! after `close`); a stream handle is the address of a boxed [`Handle`], which the Java side frees once, within
//! the call that opened it for `quads()` and through its `NativeStream` otherwise; a term code is a non-negative
//! `long`, and `-1` means "no code" (a free pattern position, a term the dictionary lacks); a `null` string is a
//! free pattern position, while `""` is the default graph.
//!
//! Terms cross in packed chunks (`Payload.java`, [`java_payload`]): the UTF-8 text of a chunk's distinct terms
//! once, and an `int` per position naming its term.

pub mod engine;
pub mod payload;
pub mod streams;

use std::sync::atomic::{AtomicI64, Ordering};

use engine::{ROW_BYTES, Store};
use jni::errors::ErrorPolicy;
use jni::objects::{JByteBuffer, JClass, JIntArray, JLongArray, JObject, JObjectArray, JString};
use jni::strings::JNIString;
use jni::sys::{jboolean, jint, jlong};
use jni::{Env, EnvUnowned, jni_str};
use payload::{Limits, Pace, Payload};
use streams::{CodeStream, TermStream};
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

/// A pattern's four positions, `(s, p, o, g)`, as N-Triples spellings: `None` for a `null` string, a free
/// position.
fn pattern_terms(
    env: &Env<'_>,
    positions: [&JString<'_>; 4],
) -> Result<[Option<String>; 4], Failure> {
    let term = |s: &JString<'_>| -> Result<Option<String>, Failure> {
        Ok(if s.is_null() {
            None
        } else {
            Some(s.try_to_string(env)?)
        })
    };
    let [s, p, o, g] = positions;
    Ok([term(s)?, term(p)?, term(o)?, term(g)?])
}

/// Native objects the Java side holds by handle and has not freed: term and code streams.
static OPEN_HANDLES: AtomicI64 = AtomicI64::new(0);

/// A native object the Java side holds by handle, counted in [`OPEN_HANDLES`] from [`open`](Self::open) to
/// its drop.
struct Handle<T>(T);

impl<T> Handle<T> {
    /// Boxes `value` for the Java side; returns its handle.
    fn open(value: T) -> jlong {
        OPEN_HANDLES.fetch_add(1, Ordering::Relaxed);
        Box::into_raw(Box::new(Handle(value))) as jlong
    }

    /// The object a handle addresses.
    ///
    /// # Safety
    ///
    /// `h` came from [`open`](Self::open) with this `T`, is not closed, and no other call uses it meanwhile.
    unsafe fn get<'a>(h: jlong) -> &'a mut T {
        // SAFETY: as the caller promises.
        unsafe { &mut (*(h as *mut Handle<T>)).0 }
    }

    /// Frees a handle; `0` is a no-op.
    ///
    /// # Safety
    ///
    /// As for [`get`](Self::get), and nothing uses the handle afterwards.
    unsafe fn close(h: jlong) {
        if h != 0 {
            // SAFETY: as the caller promises.
            drop(unsafe { Box::from_raw(h as *mut Handle<T>) });
        }
    }
}

impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        OPEN_HANDLES.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The most rows a read hands over: what a Java `ArrayList` holds.
const MAX_JAVA_LIST: usize = i32::MAX as usize - 8;

/// The chunk limits a native call's `int`s give; out of range (a negative value included) is `InvalidInput`.
fn limits(rows: jint, bytes: jint, large_term: jint) -> Result<Limits, Failure> {
    let size = |value: jint| usize::try_from(value).unwrap_or(0);
    Ok(Limits::new(size(rows), size(bytes), size(large_term))?)
}

/// A chunk as the `Object[5]` `Payload.java` reads: the text (`byte[]`), the distinct terms' bounds and each
/// position's term (`int[]` each), the large terms (`String[]`, or `null` when there are none), and `tail`
/// (`long[]`, or `null`).
fn java_payload<'local>(
    env: &mut Env<'local>,
    payload: &Payload,
    tail: Option<&[i64]>,
) -> Result<JObjectArray<'local, JObject<'local>>, Failure> {
    let array = JObjectArray::<JObject>::new(env, 5, JObject::null())?;
    let text = env.byte_array_from_slice(&payload.text)?;
    array.set_element(env, 0, &text)?;
    for (slot, ints) in [(1, &payload.bounds), (2, &payload.terms)] {
        let java = JIntArray::new(env, ints.len())?;
        java.set_region(env, 0, ints)?;
        array.set_element(env, slot, &java)?;
    }
    if !payload.large.is_empty() {
        let large = java_strings(env, payload.large.iter().map(|t| Some(t.as_str())))?;
        array.set_element(env, 3, &large)?;
    }
    if let Some(tail) = tail {
        let tail = java_longs(env, tail)?;
        array.set_element(env, 4, &tail)?;
    }
    Ok(array)
}

/// A `long[]` of `values`.
fn java_longs<'local>(
    env: &mut Env<'local>,
    values: &[i64],
) -> Result<JLongArray<'local>, Failure> {
    let array = JLongArray::new(env, values.len())?;
    array.set_region(env, 0, values)?;
    Ok(array)
}

/// `count` codes of the `long[]` `codes` from `from`, each a term code; a negative one is no code, read as
/// `u64::MAX`, which is outside every dictionary and so decodes to `None`.
fn java_codes(
    env: &Env<'_>,
    codes: &JLongArray<'_>,
    from: jint,
    count: usize,
) -> Result<Vec<u64>, Failure> {
    let mut buf = vec![0i64; count];
    codes.get_region(env, from, &mut buf)?;
    Ok(buf
        .into_iter()
        .map(|c| code(c).unwrap_or(u64::MAX))
        .collect())
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

/// The first `max_rows` rows of the direct buffer `buf`, `ROW_BYTES` each, for `call` to write codes into; an
/// error naming `call` when `buf` is not a direct buffer of that many rows.
fn direct_rows<'a>(
    env: &Env<'_>,
    buf: &JByteBuffer<'_>,
    max_rows: jint,
    call: &str,
) -> Result<&'a mut [[u8; ROW_BYTES]], Failure> {
    let not_a_buffer = || format!("{call}: not a direct buffer of 32 * maxRows bytes");
    let addr = env
        .get_direct_buffer_address(buf)
        .map_err(|_| not_a_buffer())?;
    let capacity = env
        .get_direct_buffer_capacity(buf)
        .map_err(|_| not_a_buffer())?;
    let rows = max_rows.max(0) as usize;
    if rows.checked_mul(ROW_BYTES).is_none_or(|len| len > capacity) {
        return Err(not_a_buffer().into());
    }
    // SAFETY: the buffer's memory holds `rows` rows (checked above) and stays valid for the native call, the
    // Java caller holding the buffer.
    Ok(unsafe { std::slice::from_raw_parts_mut(addr.cast::<[u8; ROW_BYTES]>(), rows) })
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
        let [s, p, o, g] = pattern_terms(env, [&s, &p, &o, &g])?;
        Ok(store(h).count(s.as_deref(), p.as_deref(), o.as_deref(), g.as_deref())? as jlong)
    })
}

/// A read of every quad matching a pattern, as its first chunk; the last slot is `{stream, rows}`: the term stream
/// of the chunks left ([`Java_org_vortexrdf_Native_termStreamNext`]), `0` when this one ends the read, and the
/// read's rows. A chunk holds at most `chunkRows` rows and `chunkBytes` bytes of terms; terms over `largeTerm`
/// bytes cross on their own.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_quadsOpen<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: JString<'local>,
    p: JString<'local>,
    o: JString<'local>,
    g: JString<'local>,
    chunk_rows: jint,
    chunk_bytes: jint,
    large_term: jint,
) -> JObjectArray<'local, JObject<'local>> {
    call(&mut env, |env| {
        let [s, p, o, g] = pattern_terms(env, [&s, &p, &o, &g])?;
        let limits = limits(chunk_rows, chunk_bytes, large_term)?;
        let (mut stream, rows) = store(h).quads(
            s.as_deref(),
            p.as_deref(),
            o.as_deref(),
            g.as_deref(),
            limits,
        )?;
        if rows > MAX_JAVA_LIST {
            return Err("the match has more rows than a Java list holds".into());
        }
        let first = stream.next_payload()?.unwrap_or_default();
        let handle = if first.positions() / 4 < rows {
            Handle::open(Stream::Terms(stream))
        } else {
            0
        };
        java_payload(env, &first, Some(&[handle, rows as i64])).inspect_err(|_| {
            // SAFETY: the handle was just opened and never reached the Java side.
            unsafe { Handle::<Stream>::close(handle) }
        })
    })
}

/// The native stream behind a Java read: its terms, or its codes.
enum Stream {
    Terms(TermStream),
    Codes(CodeStream),
}

impl Stream {
    /// The term stream a handle addresses; an error naming `call` for a code stream.
    ///
    /// # Safety
    ///
    /// As for [`Handle::get`].
    unsafe fn terms<'a>(h: jlong, call: &str) -> Result<&'a mut TermStream, Failure> {
        // SAFETY: as the caller promises.
        match unsafe { Handle::<Stream>::get(h) } {
            Stream::Terms(terms) => Ok(terms),
            Stream::Codes(_) => Err(format!("{call}: not a term stream").into()),
        }
    }

    /// The code stream a handle addresses; an error naming `call` for a term stream.
    ///
    /// # Safety
    ///
    /// As for [`Handle::get`].
    unsafe fn codes<'a>(h: jlong, call: &str) -> Result<&'a mut CodeStream, Failure> {
        // SAFETY: as the caller promises.
        match unsafe { Handle::<Stream>::get(h) } {
            Stream::Codes(codes) => Ok(codes),
            Stream::Terms(_) => Err(format!("{call}: not a code stream").into()),
        }
    }
}

/// A read of the quads matching a pattern as a stream of chunks of at most `batchRows` rows, in the match's order
/// (a `secondary-by-copy` index's for a pattern it serves, else base row order), sent as
/// [`Java_org_vortexrdf_Native_quadsOpen`] sends its chunks; the pattern is parsed and matched now. Returns the
/// stream's handle.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_termStreamOpen<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: JString<'local>,
    p: JString<'local>,
    o: JString<'local>,
    g: JString<'local>,
    batch_rows: jint,
    chunk_rows: jint,
    chunk_bytes: jint,
    large_term: jint,
) -> jlong {
    call(&mut env, |env| {
        let [s, p, o, g] = pattern_terms(env, [&s, &p, &o, &g])?;
        let limits = limits(chunk_rows, chunk_bytes, large_term)?;
        let stream = store(h).term_stream(
            s.as_deref(),
            p.as_deref(),
            o.as_deref(),
            g.as_deref(),
            batch_rows.max(0) as usize,
            limits,
        )?;
        Ok(Handle::open(Stream::Terms(stream)))
    })
}

/// A term stream's next payload (`Payload.java`, its last slot `null`); `null` once every row was sent.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_termStreamNext<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    stream: jlong,
) -> JObjectArray<'local, JObject<'local>> {
    call(&mut env, |env| {
        // SAFETY: the Java side passes a stream `quadsOpen` or `termStreamOpen` returned, before freeing it, one
        // call at a time (the call that opened it, or its `NativeStream`, which serializes them).
        let terms = unsafe { Stream::terms(stream, "termStreamNext") }?;
        match terms.next_payload()? {
            Some(payload) => java_payload(env, &payload, None),
            None => Ok(JObjectArray::default()),
        }
    })
}

/// A read of the quads matching a code pattern (`-1` is free) as a stream of their codes, in chunks of at
/// most `batchRows` rows, in the match's order (a `secondary-by-copy` index's for a pattern it serves, else
/// base row order). Returns the stream's handle.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_codeStreamOpen<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    s: jlong,
    p: jlong,
    o: jlong,
    g: jlong,
    batch_rows: jint,
) -> jlong {
    call(&mut env, |_| {
        let stream = store(h).code_stream(pattern(s, p, o, g), batch_rows.max(0) as usize)?;
        Ok(Handle::open(Stream::Codes(stream)))
    })
}

/// Writes a code stream's next rows, up to `maxRows`, from the start of the direct buffer `buf` as
/// `matchCodes` writes them. Returns the rows written, 0 once every row was.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_codeStreamNext<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    stream: jlong,
    buf: JByteBuffer<'local>,
    max_rows: jint,
) -> jint {
    call(&mut env, |env| {
        let out = direct_rows(env, &buf, max_rows, "codeStreamNext")?;
        // SAFETY: the Java side passes a stream `codeStreamOpen` returned, before freeing it, one call at a
        // time (its `NativeStream` serializes them).
        let codes = unsafe { Stream::codes(stream, "codeStreamNext") }?;
        Ok(codes.next(out)? as jint)
    })
}

/// Frees a term or code stream; `0` is a no-op.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_streamClose<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    stream: jlong,
) {
    call(&mut env, |_| {
        // SAFETY: the Java side passes a stream `quadsOpen`, `termStreamOpen` or `codeStreamOpen` returned, once
        // (the call that opened it, or its `NativeStream`), and nothing uses the stream afterwards.
        unsafe { Handle::<Stream>::close(stream) };
        Ok(())
    })
}

/// The native objects the Java side holds by handle and has not freed, in this process.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_openHandles<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) -> jlong {
    call(&mut env, |_| Ok(OPEN_HANDLES.load(Ordering::Relaxed)))
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
        java_longs(env, &values)
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
        java_longs(env, &values)
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

/// Each code's term, one `String` apiece (`null` for a negative code or one outside the dictionary): the
/// per-term call, for a lookup too small for a chunk's fixed cost to pay off.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_decodeStrings<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    codes: JLongArray<'local>,
) -> JObjectArray<'local, JString<'local>> {
    call(&mut env, |env| {
        let codes = java_codes(env, &codes, 0, codes.len(env)?)?;
        let terms = store(h).decode_many(&codes)?;
        java_strings(env, terms.iter().map(|t| t.as_deref()))
    })
}

/// The terms of `codes[from, from + count)` from the first, as one chunk of at most `chunkBytes` bytes of terms
/// and at least one code (unless `count` is 0); a negative code and one outside the dictionary hold no term.
/// The last slot is `{next}`, the codes the next slice should offer ([`Pace::next_slice`]).
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_vortexrdf_Native_decodeMany<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    h: jlong,
    codes: JLongArray<'local>,
    from: jint,
    count: jint,
    chunk_bytes: jint,
    large_term: jint,
) -> JObjectArray<'local, JObject<'local>> {
    call(&mut env, |env| {
        let len = codes.len(env)?;
        let (Ok(start), Ok(n)) = (usize::try_from(from), usize::try_from(count)) else {
            return Err("decodeMany: from and count must not be negative".into());
        };
        if start.checked_add(n).is_none_or(|end| end > len) {
            return Err("decodeMany: from + count passes the end of codes".into());
        }
        let codes = java_codes(env, &codes, from, n)?;
        let limits = limits(count.max(1), chunk_bytes, large_term)?;
        let (payload, held) = store(h).decode_payload(&codes, limits)?;
        let next = Pace::next_slice(n, held, payload.bytes(), limits.chunk_bytes);
        java_payload(env, &payload, Some(&[next.min(i64::MAX as usize) as i64]))
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
        let out = direct_rows(env, &buf, max_rows, "matchCodes")?;
        let written = store(h).match_codes(pattern(s, p, o, g), offset.max(0) as u64, out)?;
        Ok(written as jint)
    })
}
