//! Term codes across the wasm boundary. Codes are [`TermCode`]s (`u64`) and
//! cross as JS numbers, which hold every integer up to 2^53 − 1
//! (`Number.MAX_SAFE_INTEGER`) exactly — not BigInts. Both directions are
//! checked here, so a code is never rounded or wrapped: a JS value that is no
//! such integer is refused on the way in, and a code past 2^53 − 1 is an
//! error on the way out. The conversions are pure functions over Rust values,
//! so they are tested natively; the bindings turn their errors into JS
//! exceptions, and [`code_array`] hands a converted column to JS.

use vortex_rdf_core::TermCode;
use wasm_bindgen::JsValue;

use crate::error::js_err;

/// The largest code a JS number holds exactly: 2^53 − 1.
pub(crate) const MAX_JS_CODE: TermCode = (1 << 53) - 1;

/// The integer `value` holds when it is one from 0 to 2^53 − 1, the
/// non-negative integers a JS number holds exactly; `None` for anything else
/// — negative, fractional, NaN, infinite, or past 2^53 − 1, where
/// neighbouring integers share one number.
pub(crate) fn safe_u64(value: f64) -> Option<u64> {
    // `MAX_JS_CODE` converts to f64 exactly (it is below 2^53).
    let whole =
        value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= MAX_JS_CODE as f64;
    // Returned only for an integer in range, where the conversion is exact.
    whole.then_some(value as u64)
}

/// A code from JS: a number that is an integer from 0 to 2^53 − 1
/// ([`safe_u64`]). Anything else is no code.
pub(crate) fn code_from_js(value: f64) -> Result<TermCode, String> {
    safe_u64(value).ok_or_else(|| {
        format!(
            "a term code is an integer from 0 to 2^53 - 1 (Number.MAX_SAFE_INTEGER), got {value}"
        )
    })
}

/// A code for JS: the number holding it exactly, or the error for a code past
/// 2^53 − 1, which no JS number holds.
pub(crate) fn code_to_js(code: TermCode) -> Result<f64, String> {
    if code <= MAX_JS_CODE {
        // At most 2^53 − 1, so the conversion is exact.
        Ok(code as f64)
    } else {
        Err(past_js_numbers(code))
    }
}

/// The error for `code`, past 2^53 − 1.
fn past_js_numbers(code: TermCode) -> String {
    format!(
        "term code {code} is past 2^53 - 1 (Number.MAX_SAFE_INTEGER): no JS number holds it \
         exactly"
    )
}

/// A code column as the numbers of a `Float64Array`, or the error for its
/// first code past 2^53 − 1 (see [`code_to_js`]). The column's largest code
/// is checked first, so the conversion is a plain cast into one allocation of
/// the column's length.
pub(crate) fn codes_to_js(codes: &[TermCode]) -> Result<Vec<f64>, String> {
    let largest = codes.iter().copied().max().unwrap_or(0);
    if largest > MAX_JS_CODE {
        let first = codes
            .iter()
            .copied()
            .find(|&code| code > MAX_JS_CODE)
            .unwrap_or(largest);
        return Err(past_js_numbers(first));
    }
    // Every code is at most 2^53 − 1, so each cast is exact.
    Ok(codes.iter().map(|&code| code as f64).collect())
}

/// A code column as a JS-owned `Float64Array` of its codes ([`codes_to_js`]);
/// a code past 2^53 − 1 is an error, never a rounded number.
pub(crate) fn code_array(codes: &[TermCode]) -> Result<js_sys::Float64Array, JsValue> {
    let numbers = codes_to_js(codes).map_err(js_err)?;
    // Copy into a JS-owned Float64Array (safe against wasm memory growth,
    // which would detach a zero-copy view). A wasm32 store holds fewer rows
    // than a u32 counts, so the length fits.
    let array = js_sys::Float64Array::new_with_length(numbers.len() as u32);
    array.copy_from(&numbers);
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Past `u32::MAX`: a 32-bit boundary would wrap it onto code 5.
    const WIDE: TermCode = (1 << 32) + 5;

    #[test]
    fn codes_up_to_2_pow_53_minus_1_cross_both_ways_exactly() {
        for code in [
            0,
            1,
            5,
            TermCode::from(u32::MAX),
            WIDE,
            MAX_JS_CODE - 1,
            MAX_JS_CODE,
        ] {
            let number = code_to_js(code).unwrap();
            assert_eq!(code_from_js(number), Ok(code), "{code}");
        }
        assert_eq!(code_from_js(-0.0), Ok(0));
        assert_eq!(
            codes_to_js(&[0, WIDE, MAX_JS_CODE]).unwrap(),
            vec![0.0, WIDE as f64, MAX_JS_CODE as f64]
        );
        assert!(codes_to_js(&[]).unwrap().is_empty());
    }

    #[test]
    fn what_is_no_code_is_refused_coming_in() {
        for value in [
            -1.0,
            -0.5,
            0.5,
            1.5,
            WIDE as f64 + 0.5,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            2f64.powi(53),
            2f64.powi(64),
            f64::MAX,
        ] {
            let err = code_from_js(value).unwrap_err();
            assert!(err.contains("term code"), "{value}: {err}");
        }
    }

    #[test]
    fn codes_past_2_pow_53_minus_1_are_refused_going_out() {
        for code in [MAX_JS_CODE + 1, 1 << 53, TermCode::MAX] {
            let err = code_to_js(code).unwrap_err();
            assert!(err.contains(&code.to_string()), "{err}");
        }
        assert!(codes_to_js(&[0, WIDE, MAX_JS_CODE + 1]).is_err());
    }

    /// A column converts into one allocation of exactly its length.
    #[test]
    fn a_code_column_converts_into_one_allocation() {
        let codes: Vec<TermCode> = (0..1_000).map(|i| i * 7 + WIDE).collect();
        let numbers = codes_to_js(&codes).unwrap();
        assert_eq!(numbers.len(), codes.len());
        assert_eq!(numbers.capacity(), codes.len());
        assert!(numbers.iter().zip(&codes).all(|(&n, &c)| n == c as f64));
    }

    /// With several codes past 2^53 − 1, the error names the first of them.
    #[test]
    fn the_error_names_the_first_code_past_the_bound() {
        let err = codes_to_js(&[5, MAX_JS_CODE + 2, 7, MAX_JS_CODE + 1]).unwrap_err();
        assert!(err.contains(&(MAX_JS_CODE + 2).to_string()), "{err}");
        assert!(!err.contains(&(MAX_JS_CODE + 1).to_string()), "{err}");
    }
}
