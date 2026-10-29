//! The claims of an access token that snouty reads without a check of its
//! signature.

use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};

/// Try to parse the supplied string as a PASETO token and return the `exp` claim
pub(super) fn try_get_expiry_from_token(token: &str) -> Option<DateTime<Utc>> {
    let mut parts = token.splitn(4, '.');
    let _version = parts.next()?;
    let purpose = parts.next()?;
    let payload = parts.next()?;
    if purpose != "public" {
        return None;
    }
    expiry_of_claims(payload)
}

/// The `exp` claim of a JWT, such as a GitHub Actions OIDC token.
pub(super) fn try_get_expiry_from_jwt(token: &str) -> Option<DateTime<Utc>> {
    expiry_of_claims(token.split('.').nth(1)?)
}

/// The `exp` claim of `payload`, the base64url claims of a token. The JSON
/// object can be followed by other bytes, such as the signature of a PASETO.
fn expiry_of_claims(payload: &str) -> Option<DateTime<Utc>> {
    let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
    // A map, not a struct with an `exp` field, which serde would also read
    // from an array such as `[0]`.
    let claims = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<serde_json::Map<String, serde_json::Value>>()
        .next()?
        .ok()?;
    parse_exp_claim(claims.get("exp")?)
}

fn parse_exp_claim(exp: &serde_json::Value) -> Option<DateTime<Utc>> {
    if let Some(text) = exp.as_str() {
        return Some(DateTime::parse_from_rfc3339(text).ok()?.with_timezone(&Utc));
    }
    if let Some(secs) = exp.as_u64() {
        return DateTime::from_timestamp(i64::try_from(secs).ok()?, 0);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::auth::{jwt_with_claims, public_paseto_with_claims};
    use crate::testutils::fake_secrets::{FAKE_OIDC_TOKEN, FAKE_PASETO_A, FAKE_PASETO_B};
    use base64::prelude::{BASE64_STANDARD, BASE64_URL_SAFE};
    use hegel::generators;
    use serde_json::json;

    #[test]
    fn expiry_parsed_from_public_paseto_rfc3339_exp() {
        let token =
            public_paseto_with_claims(br#"{"sub":"user","exp":"2039-01-01T00:00:00+00:00"}"#);
        let expected = DateTime::parse_from_rfc3339("2039-01-01T00:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(try_get_expiry_from_token(&token), Some(expected));
    }

    #[test]
    fn expiry_parsed_from_numeric_exp() {
        let token = public_paseto_with_claims(br#"{"exp":2145916800}"#);
        assert_eq!(
            try_get_expiry_from_token(&token),
            DateTime::from_timestamp(2_145_916_800, 0)
        );
    }

    #[test]
    fn expiry_is_none_for_local_paseto() {
        // A local token's payload is encrypted, so its claims are unreadable.
        let token = format!(
            "v4.local.{}",
            BASE64_URL_SAFE_NO_PAD.encode(b"opaque-ciphertext")
        );
        assert_eq!(try_get_expiry_from_token(&token), None);
    }

    #[test]
    fn expiry_is_none_on_unparsable_or_missing_exp() {
        // Missing exp claim.
        assert_eq!(
            try_get_expiry_from_token(&public_paseto_with_claims(br#"{"sub":"user"}"#)),
            None
        );
        // Not a PASETO at all.
        assert_eq!(try_get_expiry_from_token("not-a-token"), None);
        // Public shape, but the payload isn't valid base64url / JSON.
        assert_eq!(try_get_expiry_from_token("v4.public.@@@@"), None);
        // exp present but not an RFC 3339 string or a number.
        assert_eq!(
            try_get_expiry_from_token(&public_paseto_with_claims(br#"{"exp":"whenever"}"#)),
            None
        );
    }

    // 2019-01-01T00:00:00+00:00, the `exp` claim in the canonical PASETO
    // v2.public test vectors below.
    const CANONICAL_VECTOR_EXP_UNIX: i64 = 1_546_300_800;

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector() {
        // Official PASETO 2-S-1 test vector: a real token with a genuine 64-byte
        // Ed25519 signature trailing the JSON claims (no footer). This exercises
        // the real base64url decode and the skip-the-signature parse — unlike the
        // synthetic tokens above whose "signature" is 64 zero bytes.
        assert_eq!(
            try_get_expiry_from_token(FAKE_PASETO_A),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX, 0)
        );
    }

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector_with_footer() {
        // Official PASETO 2-S-2 test vector: same claims, but with a footer
        // (`UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz` = "Paragon Initiative
        // Enterprises"). Confirms the 4th `.`-delimited segment is ignored.
        assert_eq!(
            try_get_expiry_from_token(FAKE_PASETO_B),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX, 0)
        );
    }

    #[test]
    fn expiry_is_none_for_a_jwt_without_a_readable_exp() {
        assert_eq!(try_get_expiry_from_jwt(FAKE_OIDC_TOKEN), None);
        assert_eq!(try_get_expiry_from_jwt("a.@@@@.b"), None);
        assert_eq!(
            try_get_expiry_from_jwt(&jwt_with_claims(br#"{"sub":"repo:acme/app"}"#)),
            None
        );
        assert_eq!(
            try_get_expiry_from_jwt(&jwt_with_claims(br#"{"exp":"whenever"}"#)),
            None
        );
    }

    /// A claims segment, and the expiry that a parser must read from it. The
    /// segment is the base64url of a JSON value, followed for a PASETO
    /// (`signed`) by the bytes of a signature, or text that is not
    /// base64url-no-pad. Only an object with an `exp` of an RFC
    /// 3339 string, or of a whole number of seconds that chrono can hold, has
    /// an expiry.
    fn claims_segment(tc: &hegel::TestCase, signed: bool) -> (String, Option<DateTime<Utc>>) {
        let instant = DateTime::from_timestamp(
            tc.draw(
                generators::integers::<i64>()
                    .min_value(0)
                    .max_value(1 << 35),
            ),
            0,
        )
        .unwrap();
        let n = tc.draw(generators::integers::<u64>());
        let (exp, mut expected) = match tc.draw(generators::integers::<u8>().max_value(7)) {
            0 => (None, None),
            1 => (
                Some(json!(n)),
                i64::try_from(n)
                    .ok()
                    .and_then(|s| DateTime::from_timestamp(s, 0)),
            ),
            2 => (Some(json!(instant.to_rfc3339())), Some(instant)),
            3 => (Some(json!(-instant.timestamp() - 1)), None),
            4 => (Some(json!(tc.draw(generators::floats::<f64>()))), None),
            5 => (
                Some(json!(format!("x{}", tc.draw(generators::text())))),
                None,
            ),
            6 => (Some(json!({ "exp": n })), None),
            _ => (Some(serde_json::Value::Null), None),
        };
        let mut claims = json!({ "sub": tc.draw(generators::text()), "inner": { "exp": n } });
        if let Some(exp) = exp {
            claims["exp"] = exp;
        }
        let claims = match tc.draw(generators::integers::<u8>().max_value(9)) {
            // Claims that are not an object have no expiry.
            0 => json!([claims["exp"]]),
            1 => json!(n),
            _ => claims,
        };
        if !claims.is_object() {
            expected = None;
        }

        let space = tc.draw(generators::sampled_from(&["", " ", "\n\t"]));
        let mut bytes = format!("{space}{claims}").into_bytes();
        if signed {
            bytes.extend(tc.draw(generators::binary()));
        }
        match tc.draw(generators::integers::<u8>().max_value(9)) {
            // Random bytes that do not start an object.
            0 => {
                let bytes = tc.draw(generators::binary());
                tc.assume(bytes.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{'));
                (BASE64_URL_SAFE_NO_PAD.encode(bytes), None)
            }
            // Text with a character outside base64url-no-pad.
            1 => {
                let text = tc.draw(generators::text().exclude_characters("."));
                let bad = tc.draw(generators::sampled_from(&["@", "+", "/", "=", " ", "é"]));
                (format!("{text}{bad}"), None)
            }
            2 => (String::new(), None),
            // The claims in another base64 alphabet, or with padding.
            3 => {
                let segment = if tc.draw(generators::booleans()) {
                    BASE64_STANDARD.encode(&bytes)
                } else {
                    BASE64_URL_SAFE.encode(&bytes)
                };
                let valid = segment == BASE64_URL_SAFE_NO_PAD.encode(&bytes);
                (segment, expected.filter(|_| valid))
            }
            _ => (BASE64_URL_SAFE_NO_PAD.encode(bytes), expected),
        }
    }

    /// A segment of a token other than its claims.
    fn other_segment(tc: &hegel::TestCase) -> String {
        tc.draw(generators::text().exclude_characters("."))
    }

    #[hegel::test]
    fn expiry_of_a_generated_jwt(tc: hegel::TestCase) {
        let (claims, expected) = claims_segment(&tc, false);
        let header = other_segment(&tc);
        assert_eq!(try_get_expiry_from_jwt(&header), None);
        let mut token = format!("{header}.{claims}");
        for _ in 0..tc.draw(generators::integers::<u8>().max_value(3)) {
            token = format!("{token}.{}", other_segment(&tc));
        }
        assert_eq!(try_get_expiry_from_jwt(&token), expected, "{token}");
    }

    #[hegel::test]
    fn expiry_of_a_generated_paseto(tc: hegel::TestCase) {
        let (claims, expected) = claims_segment(&tc, true);
        let version = other_segment(&tc);
        let purpose = tc.draw(generators::sampled_from(&["public", "local", "Public", ""]));
        assert_eq!(
            try_get_expiry_from_token(&format!("{version}.{purpose}")),
            None
        );
        let mut token = format!("{version}.{purpose}.{claims}");
        // A footer, which can hold any text.
        if tc.draw(generators::booleans()) {
            token = format!("{token}.{}", tc.draw(generators::text()));
        }
        let expected = expected.filter(|_| purpose == "public");
        assert_eq!(try_get_expiry_from_token(&token), expected, "{token}");
    }

    #[hegel::test]
    fn expiry_of_any_text_does_not_panic(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let _ = try_get_expiry_from_jwt(&text);
        let _ = try_get_expiry_from_token(&text);
    }
}
