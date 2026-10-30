use super::*;
use crate::store::schema::TermCode;
use vortex_array::arrays::VarBinArray;
use vortex_array::dtype::{DType, NativePType, Nullability, PType};
use vortex_array::validity::Validity;
use vortex_layout::layouts::zoned::Zoned;

/// 32-bit code offsets are the very buffer of the array's offsets child:
/// viewed, not copied.
#[test]
fn thirty_two_bit_offsets_are_viewed_not_copied() {
    let terms: Vec<String> = (0..300)
        .map(|i| format!("<http://example.org/term/{i:04}>"))
        .collect();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let dict = TermDictionary::compress_windowed(plain, 100).unwrap();
    assert_eq!(
        dict.terms.chunks.len(),
        3,
        "a window of 100 cuts 300 terms in 3"
    );
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    for chunk in &dict.terms.chunks {
        let TermChunk::Fsst(fsst) = chunk else {
            panic!("a built dictionary holds FSST chunks");
        };
        let child = fsst
            .array
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap();
        assert_eq!(child.ptype(), PType::I32, "the built offsets are 32-bit");
        assert_eq!(fsst.offsets.len(), child.len());
        assert_eq!(
            fsst.offsets.as_ptr() as usize,
            child.as_slice::<i32>().as_ptr() as usize
        );
        assert_eq!(chunk.offsets_addr(), Some(fsst.offsets.as_ptr() as usize));
    }
}

/// `array` rebuilt with its code offsets stored as `T`.
fn with_offsets_as<T>(array: &FSSTArray, ctx: &mut vortex_array::ExecutionCtx) -> FSSTArray
where
    T: NativePType + TryFrom<u32>,
    <T as TryFrom<u32>>::Error: std::fmt::Debug,
{
    let offsets = array
        .codes_offsets()
        .clone()
        .execute::<PrimitiveArray>(ctx)
        .unwrap();
    let narrowed = PrimitiveArray::from_iter(
        offsets
            .as_slice::<i32>()
            .iter()
            .map(|&o| T::try_from(o as u32).unwrap()),
    );
    let codes = VarBinArray::new(
        narrowed.into_array(),
        array.codes_bytes().clone(),
        DType::Binary(Nullability::NonNullable),
        Validity::NonNullable,
        ctx,
    );
    FSST::try_new_with_symbol_table(
        array.dtype().clone(),
        array.symbol_table(),
        codes,
        array.uncompressed_lengths().clone(),
        ctx,
    )
    .unwrap()
}

/// A chunk whose code offsets are stored as `T` holds the same offsets and
/// decodes the same terms as the 32-bit original; it views the stored
/// buffer only when `T` is 32 bits wide, and converts into a copy
/// otherwise.
fn check_offsets_stored_as<T>(original: &FsstTerms, terms: &[String])
where
    T: NativePType + TryFrom<u32>,
    <T as TryFrom<u32>>::Error: std::fmt::Debug,
{
    let name = std::any::type_name::<T>();
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let rebuilt = with_offsets_as::<T>(&original.array, &mut ctx);
    let stored = rebuilt
        .codes_offsets()
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .unwrap();
    assert_eq!(
        stored.ptype(),
        T::PTYPE,
        "{name}: the offsets are stored so"
    );
    let chunk = FsstTerms::new(rebuilt).unwrap();
    assert_eq!(chunk.offsets[..], original.offsets[..], "{name}: offsets");
    assert_eq!(
        std::ptr::eq(
            chunk.offsets.as_ptr(),
            stored.as_slice::<T>().as_ptr().cast()
        ),
        T::PTYPE.byte_width() == 4,
        "{name}: viewed exactly when 32-bit"
    );
    let chunk = TermChunk::Fsst(chunk);
    assert_eq!(chunk.heap_bytes(), 4 * original.offsets.len(), "{name}");
    let mut cursor = chunk.cursor();
    for (i, term) in terms.iter().enumerate() {
        assert_eq!(cursor.bytes_at(i), term.as_bytes(), "{name}: term {i}");
    }
}

#[test]
fn fsst_code_offsets_of_every_width_decode_alike() {
    let terms: Vec<String> = (0..8).map(|i| format!("<http://e.org/{i}>")).collect();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let dict = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
    let [TermChunk::Fsst(original)] = &dict.terms.chunks[..] else {
        panic!("a small dictionary is one FSST chunk");
    };
    // Every stored offset must fit the narrowest width tried.
    assert!(original.offsets.iter().all(|&o| o < 128));
    check_offsets_stored_as::<u8>(original, &terms);
    check_offsets_stored_as::<i8>(original, &terms);
    check_offsets_stored_as::<u16>(original, &terms);
    check_offsets_stored_as::<i16>(original, &terms);
    check_offsets_stored_as::<u32>(original, &terms);
    check_offsets_stored_as::<i32>(original, &terms);
    check_offsets_stored_as::<u64>(original, &terms);
    check_offsets_stored_as::<i64>(original, &terms);
}

/// A two-term dictionary's one FSST chunk, with every code offset set to
/// `offset`. The constant child stands in for real offsets: FSST checks
/// them against the code bytes only once decoded, so the chunk needs no
/// 4 GiB of codes.
fn chunk_with_offsets(offset: vortex_array::scalar::Scalar) -> FSSTArray {
    use vortex_array::ArrayParts;
    use vortex_array::arrays::ConstantArray;
    use vortex_fsst::FSSTSlots;

    let terms = ["<http://e.org/a>", "<http://e.org/b>"];
    let dict = TermDictionary::compress_windowed(VarBinViewArray::from_iter_str(terms), usize::MAX)
        .unwrap();
    let [TermChunk::Fsst(original)] = &dict.terms.chunks[..] else {
        panic!("a small dictionary is one FSST chunk");
    };
    let array = &original.array;
    FSSTArray::try_from_parts(ArrayParts::new(
        FSST,
        array.dtype().clone(),
        array.len(),
        array.data().clone(),
        FSSTSlots {
            uncompressed_lengths: array.uncompressed_lengths().clone(),
            codes_offsets: ConstantArray::new(offset, array.len() + 1).into_array(),
            codes_validity: None,
        }
        .into_slots(),
    ))
    .unwrap()
}

/// A chunk whose offsets of another width than 32 bits a `u32` cannot
/// hold is refused, never read through wrapped offsets: a 64-bit offset
/// past `u32::MAX` (a foreign writer's chunk of more than 4 GiB of codes)
/// and a negative one.
#[test]
fn fsst_code_offsets_outside_u32_are_refused() {
    use vortex_array::scalar::Scalar;

    let past = u64::from(u32::MAX) + 1;
    for (offset, shown) in [
        (Scalar::from(past), past.to_string()),
        (Scalar::from(-1i64), "-1".to_owned()),
    ] {
        let error = FsstTerms::new(chunk_with_offsets(offset))
            .err()
            .unwrap_or_else(|| panic!("offset {shown}: refused"));
        assert!(
            matches!(&error, VortexRdfError::Deserialization(message)
                    if message.contains(&format!("offset {shown} "))),
            "{error}"
        );
    }
}

/// A negative `i32` offset, which no writer produces, is viewed as
/// written, a huge `u32`: its term's read panics at the slice's bounds
/// check, never reading other bytes.
#[test]
#[should_panic(expected = "out of range for slice")]
fn fsst_negative_i32_offset_panics_its_read() {
    use vortex_array::scalar::Scalar;

    let chunk = FsstTerms::new(chunk_with_offsets(Scalar::from(-1i32))).unwrap();
    chunk.decode_into(0, &mut Vec::new());
}

/// A cursor's scratch buffer grows to each term it decodes: terms far
/// longer than any before them, short ones after, every probe, batch and
/// positional read through one chunk or several.
#[test]
fn scratch_grows_to_terms_far_longer_than_the_first() {
    let terms = crate::tests::widely_varying_terms();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let n = terms.len();
    for window in [usize::MAX, 6] {
        let dict = TermDictionary::compress_windowed(plain.clone(), window).unwrap();
        // One cursor: short, longer, longest, short again, descending.
        let mut cursor = dict.cursor();
        let order: Vec<usize> = (0..n).chain((0..n).rev()).chain([0, 9, 1, 5, 9]).collect();
        for &i in &order {
            assert_eq!(cursor.str_at(i), terms[i], "window {window}: {i}");
        }
        let codes: Vec<TermCode> = order.iter().map(|&i| i as TermCode).collect();
        let decoded = dict.decode_many(&codes);
        for (&i, term) in order.iter().zip(&decoded) {
            assert_eq!(term.as_deref(), Some(terms[i].as_str()), "window {window}");
        }
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(
                dict.encode_exact(term),
                Some(i as TermCode),
                "window {window}"
            );
            assert_eq!(dict.lower_bound(term.as_bytes()), i as TermCode);
            assert_eq!(dict.decode(i as TermCode).as_deref(), Some(term.as_str()));
        }
        let past_the_longest = format!("{}~", terms[9]);
        assert_eq!(dict.encode_exact(&past_the_longest), None);
        assert_eq!(dict.lower_bound(past_the_longest.as_bytes()), 10);
    }
}

/// After every read the scratch holds room for the term's codes expanded
/// at 8 bytes each plus the headroom `decompress_into`'s block path needs:
/// a smaller buffer still decodes, through the byte-at-a-time tail loop,
/// so only the capacity shows it.
#[test]
fn scratch_keeps_the_decoders_block_path_headroom() {
    let terms = crate::tests::widely_varying_terms();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let dict = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
    let [chunk @ TermChunk::Fsst(fsst)] = &dict.terms.chunks[..] else {
        panic!("a small dictionary is one FSST chunk");
    };
    let mut cursor = chunk.cursor();
    let order: Vec<usize> = (0..terms.len()).chain((0..terms.len()).rev()).collect();
    for i in order {
        assert_eq!(cursor.bytes_at(i), terms[i].as_bytes());
        let codes = (fsst.offsets[i + 1] - fsst.offsets[i]) as usize;
        let ChunkCursor::Fsst { scratch, .. } = &cursor else {
            unreachable!("an FSST chunk gives an FSST cursor");
        };
        assert!(
            scratch.capacity() >= codes * FSST_SYMBOL_LEN + FSST_DECODE_HEADROOM,
            "term {i}: {codes} code bytes, scratch {}",
            scratch.capacity()
        );
    }
}

/// Multi-window compression is invisible to lookups: a dictionary
/// compressed in many small windows holds independent FSST chunks and
/// answers exactly like the single-window form — every term, both
/// directions, across window boundaries, and absent probes alike.
#[test]
fn windowed_compress_probe_parity() {
    let terms: Vec<String> = (0..1_000)
        .map(|i| format!("<http://example.org/term/{i:05}>"))
        .collect();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let windowed = TermDictionary::compress_windowed(plain.clone(), 64).unwrap();
    let c = &windowed.terms;
    assert_eq!(c.chunks.len(), 1_000usize.div_ceil(64));
    assert_eq!(c.len, 1_000);
    assert!(c.chunks.iter().all(|ch| matches!(ch, TermChunk::Fsst(_))));
    let single = TermDictionary::compress_windowed(plain, usize::MAX).unwrap();
    assert!(matches!(single.terms.chunks[..], [TermChunk::Fsst(_)]));
    for (i, term) in terms.iter().enumerate() {
        assert_eq!(windowed.encode_exact(term), Some(i as TermCode), "{term}");
        assert_eq!(
            windowed.decode(i as TermCode).as_deref(),
            Some(term.as_str())
        );
        assert_eq!(single.encode_exact(term), Some(i as TermCode));
    }
    assert_eq!(windowed.encode_exact("<http://absent>"), None);
    assert_eq!(windowed.decode(1_000), None);
}

/// A term column a producer wrote in plaintext is adopted canonical —
/// as one chunk, and chunk by chunk when it arrives chunked — and
/// answers exactly like a compressed dictionary would.
#[test]
fn plaintext_terms_adopt_canonical() {
    let terms: Vec<String> = (0..300)
        .map(|i| format!("<http://example.org/plain/{i:04}>"))
        .collect();
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let mut ctx = VORTEX_SESSION.create_execution_ctx();

    let single =
        TermDictionary::from_term_chunks(vec![plain.clone().into_array()], &mut ctx, true).unwrap();
    assert!(matches!(single.terms.chunks[..], [TermChunk::Canonical(_)]));
    // A canonical chunk has no FSST code offsets to point at.
    assert_eq!(single.terms.chunks[0].offsets_addr(), None);

    let column = plain.into_array();
    let pieces = vec![
        column.slice(0..120).unwrap(),
        column.slice(120..300).unwrap(),
    ];
    let chunked = TermDictionary::from_term_chunks(pieces, &mut ctx, true).unwrap();
    let c = &chunked.terms;
    assert_eq!(c.chunks.len(), 2);
    assert_eq!(c.starts, vec![0, 120]);
    assert_eq!(c.len, 300);
    assert!(
        c.chunks
            .iter()
            .all(|ch| matches!(ch, TermChunk::Canonical(_)))
    );

    for (i, term) in terms.iter().enumerate() {
        for d in [&single, &chunked] {
            assert_eq!(d.encode_exact(term), Some(i as TermCode), "{term}");
            assert_eq!(d.decode(i as TermCode).as_deref(), Some(term.as_str()));
        }
    }
    assert_eq!(chunked.encode_exact("<http://absent>"), None);
    assert_eq!(chunked.decode(300), None);
}

/// Chunks that are not one uniform window (adopted from a foreign file)
/// are written without zone maps, and read back unchanged.
#[tokio::test]
async fn non_uniform_chunks_write_without_zone_maps() {
    let terms: Vec<String> = (0..300)
        .map(|i| format!("<http://example.org/plain/{i:04}>"))
        .collect();
    let column = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)).into_array();
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let pieces = vec![
        column.slice(0..120).unwrap(),
        column.slice(120..300).unwrap(),
    ];
    let d = TermDictionary::from_term_chunks(pieces, &mut ctx, true).unwrap();
    assert_eq!(d.uniform_window(), None);
    let windowed = TermDictionary::compress_windowed(
        VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)),
        100,
    )
    .unwrap();
    assert_eq!(windowed.uniform_window(), std::num::NonZeroUsize::new(100));
    let bytes = crate::tests::write_dict_only_store(&d).await;
    let reread = crate::store::VortexRdfStore::from_bytes(&bytes)
        .await
        .unwrap();
    let snapshot = reread.dict_reader().unwrap().snapshot().unwrap();
    for (i, term) in terms.iter().enumerate() {
        assert_eq!(
            snapshot.decode(i as TermCode).as_deref(),
            Some(term.as_str())
        );
    }
    // And the term column carries no zone-map node at all.
    let native = crate::tests::open_native_bytes(bytes);
    assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());
}

/// A dictionary child without zone maps — what chunks of uneven length
/// write — opens file-backed and point-reads across its chunk leaves.
#[tokio::test]
async fn unzoned_dict_child_point_reads_file_backed() {
    use crate::store::layouts::dictionary::file_backed::{self, FileBackedDict};
    use crate::store::persist::native_file::NativeStoreFile;
    use vortex_buffer::ByteBuffer;
    use vortex_file::OpenOptionsSessionExt as _;

    let terms: Vec<String> = (0..300)
        .map(|i| format!("<http://example.org/plain/{i:04}>"))
        .collect();
    let column = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str)).into_array();
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let pieces = vec![
        column.slice(0..120).unwrap(),
        column.slice(120..300).unwrap(),
    ];
    let d = TermDictionary::from_term_chunks(pieces, &mut ctx, true).unwrap();
    assert_eq!(d.uniform_window(), None);
    let bytes = crate::tests::write_dict_only_store(&d).await;
    let file = VORTEX_SESSION
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))
        .unwrap();
    let native = NativeStoreFile::try_new(file).unwrap();
    assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());
    let fbd = FileBackedDict::open(&native)
        .await
        .unwrap()
        .expect("an unzoned chunked child is point-readable");
    for (i, term) in terms.iter().enumerate() {
        assert_eq!(
            fbd.encode_exact(term).await.unwrap(),
            Some(i as TermCode),
            "{term}"
        );
    }
    // The canonical windows are cached, each charged its array and the
    // per-entry overhead.
    assert_eq!(fbd.debug_cached_windows(), [0, 1]);
    assert!(
        fbd.debug_cached_bytes() > 2 * file_backed::WINDOW_ENTRY_OVERHEAD,
        "{}",
        fbd.debug_cached_bytes()
    );
    assert_eq!(fbd.encode_exact("<http://absent>").await.unwrap(), None);
    let codes = [0u64, 119, 120, 299];
    let decoded = fbd.decode_many(&codes).await.unwrap();
    for (code, term) in codes.iter().zip(&decoded) {
        assert_eq!(term.as_ref(), terms[*code as usize]);
    }
}

/// A build records whether every IRI is spelled `<`, its text, `>`: any
/// `<` term that does not close (a lone `<`, an IRI cut short, text past
/// the `>`) clears it, the terms around the IRIs never do, and a dictionary
/// with no IRI has it.
#[test]
fn a_build_records_whether_its_iris_are_bracketed() {
    let build = |extra: &[&str]| {
        let mut terms = vec!["", "\"a\"", ";", "<>", "<http://ex.org/a>", "=", "_:b0"];
        terms.extend_from_slice(extra);
        terms.sort_unstable();
        TermDictionary::compress_windowed(VarBinViewArray::from_iter_str(terms), 2).unwrap()
    };
    assert!(build(&[]).bracketed_iris());
    assert!(build(&["<a>", "<b>"]).bracketed_iris());
    for foreign in ["<", "<a", "<é", "<a>b"] {
        assert!(!build(&[foreign]).bracketed_iris(), "{foreign:?}");
    }
    let no_iri = VarBinViewArray::from_iter_str(["\"a\"", "_:b0"]);
    assert!(
        TermDictionary::compress_windowed(no_iri, 2)
            .unwrap()
            .bracketed_iris()
    );
    assert!(TermDictionary::empty().bracketed_iris());
}
