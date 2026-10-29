//! The claims of an access token that snouty reads without a check of its
//! signature.

use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::Deserialize;

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
    #[derive(Deserialize)]
    struct Claims {
        exp: Option<serde_json::Value>,
    }

    let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<Claims>()
        .next()?
        .ok()?;
    parse_exp_claim(&claims.exp?)
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

    #[hegel::test]
    fn expiry_parsed_from_jwt_numeric_exp(tc: hegel::TestCase) {
        let exp = tc.draw(hegel::generators::integers::<u32>());
        let token = jwt_with_claims(serde_json::json!({ "exp": exp }).to_string().as_bytes());
        assert_eq!(
            try_get_expiry_from_jwt(&token),
            DateTime::from_timestamp(i64::from(exp), 0)
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
}
