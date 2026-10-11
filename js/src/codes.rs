//! Term codes across the wasm boundary. Codes are [`TermCode`]s (`u64`) and
//! cross as JS numbers, which hold every integer up to 2^53 − 1
//! (`Number.MAX_SAFE_INTEGER`) exactly — not BigInts. Both directions are
//! checked here, so a code is never rounded or wrapped: a JS value that is no
//! such integer is refused on the way in, and a code past 2^53 − 1 is an
//! error on the way out. Pure functions over Rust values, so they are tested
//! natively; the bindings turn their errors into JS exceptions.

use vortex_rdf_core::TermCode;

/// The largest code a JS number holds exactly: 2^53 − 1.
pub(crate) const MAX_JS_CODE: TermCode = (1 << 53) - 1;

/// A code from JS: a number that is an integer from 0 to 2^53 − 1. Anything
/// else — negative, fractional, NaN, infinite, or past 2^53 − 1, where
/// neighbouring integers share one number — is no code.
pub(crate) fn code_from_js(value: f64) -> Result<TermCode, String> {
    // `MAX_JS_CODE` converts to f64 exactly (it is below 2^53).
    if value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= MAX_JS_CODE as f64 {
        // An integer in range, so the conversion is exact.
        Ok(value as TermCode)
    } else {
        Err(format!(
            "a term code is an integer from 0 to 2^53 - 1 (Number.MAX_SAFE_INTEGER), got {value}"
        ))
    }
}

/// A code for JS: the number holding it exactly, or the error for a code past
/// 2^53 − 1, which no JS number holds.
pub(crate) fn code_to_js(code: TermCode) -> Result<f64, String> {
    if code <= MAX_JS_CODE {
        // At most 2^53 − 1, so the conversion is exact.
        Ok(code as f64)
    } else {
        Err(format!(
            "term code {code} is past 2^53 - 1 (Number.MAX_SAFE_INTEGER): no JS number holds it \
             exactly"
        ))
    }
}

/// A code column as the numbers of a `Float64Array`, or the error for its
/// first code past 2^53 − 1 (see [`code_to_js`]).
pub(crate) fn codes_to_js(codes: &[TermCode]) -> Result<Vec<f64>, String> {
    codes.iter().map(|&code| code_to_js(code)).collect()
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
}
