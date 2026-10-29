//! Credentials for the unit tests: PASETO access tokens with a chosen expiry,
//! and OAuth credentials whose origin is a credentials file in a temporary
//! directory.

use std::path::Path;

use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::Utc;

use crate::auth::{AuthenticationInfo, OAuthRefreshInfo, RefreshableOAuthToken};

/// Build a `v4.public` PASETO whose payload is `claims_json ‖ signature`,
/// mirroring the real wire format (a 64-byte Ed25519 signature stand-in
/// trails the JSON claims).
pub(crate) fn public_paseto_with_claims(claims_json: &[u8]) -> String {
    let mut payload = claims_json.to_vec();
    payload.extend_from_slice(&[0u8; 64]);
    format!("v4.public.{}", BASE64_URL_SAFE_NO_PAD.encode(&payload))
}

/// A `v4.public` PASETO whose `exp` claim is `lifetime` after now.
pub(crate) fn paseto_expiring_in(lifetime: chrono::TimeDelta) -> String {
    let exp = (Utc::now() + lifetime).to_rfc3339();
    public_paseto_with_claims(serde_json::json!({ "exp": exp }).to_string().as_bytes())
}

/// The origin `credentials.toml` in `dir`, for the default profile.
pub(crate) fn credentials_file_in(dir: &Path) -> OAuthRefreshInfo {
    OAuthRefreshInfo::CredentialsFile {
        path: dir.join("credentials.toml"),
        profile: None,
    }
}

/// An OAuth credential whose origin is [`credentials_file_in`] `dir`.
pub(crate) fn oauth_credential_in(
    dir: &Path,
    access_token: impl Into<String>,
    refresh_token: Option<&str>,
) -> AuthenticationInfo {
    AuthenticationInfo::OAuth(RefreshableOAuthToken::new(
        credentials_file_in(dir),
        access_token.into(),
        refresh_token.map(str::to_owned),
    ))
}
