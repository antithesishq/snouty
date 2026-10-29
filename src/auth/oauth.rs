//! OAuth credentials from `snouty login`: an access token, and the refresh
//! token that replaces it when it expires.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Context, Result, eyre};
use keyring_core::Entry;
use log::warn;
use serde::Deserialize;

use super::{
    claims::try_get_expiry_from_token,
    store::{
        PersistableCredentials, construct_keychain_credential_name, parse_credentials_file_toml,
        persist_to_file,
    },
    to_header_value,
};
use crate::{
    error::user_error,
    settings::{global_settings_dir, mkdir, read_to_string_if_file_exists},
};

#[derive(Clone)]
struct OAuthCredential {
    antithesis_token: String,
    refresh_token: Option<String>,
}

impl std::fmt::Debug for OAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthCredential")
            .field("antithesis_token", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .finish()
    }
}

/// An OAuth access token and the refresh token that replaces it, with the
/// origin that stores them. Clones share one credential: a refresh through
/// any clone gives every clone the new token.
#[derive(Clone, Debug)]
pub struct RefreshableOAuthToken {
    origin: OAuthRefreshInfo,
    credential: Arc<RwLock<OAuthCredential>>,
}

impl RefreshableOAuthToken {
    pub(crate) fn new(
        origin: OAuthRefreshInfo,
        antithesis_token: String,
        refresh_token: Option<String>,
    ) -> Self {
        Self {
            origin,
            credential: Arc::new(RwLock::new(OAuthCredential {
                antithesis_token,
                refresh_token,
            })),
        }
    }

    fn current(&self) -> OAuthCredential {
        self.credential
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn store(&self, credential: OAuthCredential) {
        *self
            .credential
            .write()
            .unwrap_or_else(PoisonError::into_inner) = credential;
    }

    /// The current access token.
    pub(crate) fn access_token(&self) -> String {
        self.current().antithesis_token
    }

    /// When the expiry of the access token is known, the time at which it
    /// expires: its `exp` claim.
    pub(crate) fn expires_at(&self) -> Option<DateTime<Utc>> {
        try_get_expiry_from_token(&self.current().antithesis_token)
    }

    /// Whether the expiry of the access token is known and in the past.
    pub(crate) fn expired(&self) -> bool {
        self.expires_at()
            .is_some_and(|expires_at| Utc::now() >= expires_at)
    }

    /// Replaces the access token with a newer one, in every clone: the one
    /// that the origin holds, when another process or refresh stored it, or
    /// else a new one from the refresh endpoint at `base_url`, which the
    /// origin then stores. An advisory file lock serializes refreshes across
    /// processes, so that two do not both spend one refresh token.
    ///
    /// Fails when there is no newer token: the origin holds the same token,
    /// and there is no refresh token, or the refresh fails.
    pub(crate) async fn refresh(&self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        let held_access_token = self.access_token();

        // Best-effort cross-process lock, held across the reload + refresh. If
        // we can't take it we proceed unserialized rather than fail; the lock
        // is released when `_lock` drops at the end of this scope.
        let _lock = acquire_refresh_lock(&self.origin).await;

        // Adopt whatever is currently persisted — another process may have
        // refreshed while we waited for the lock — so any refresh below uses
        // the latest token.
        if let Ok(Some(PersistableCredentials::OAuth {
            antithesis_token,
            refresh_token,
        })) = self.origin.load()
        {
            self.store(OAuthCredential {
                antithesis_token,
                refresh_token,
            });
        }

        let current = self.current();
        // Someone (another process, or an earlier refresh) already replaced
        // the held token — use theirs instead of refreshing again.
        if current.antithesis_token != held_access_token {
            return Ok(());
        }
        let Some(refresh_token) = current.refresh_token else {
            return Err(user_error(
                "the stored OAuth token cannot be refreshed, because there is no refresh token; run `snouty login`",
            ));
        };

        let refreshed = refresh_oauth_token(client, base_url, &refresh_token).await?;
        let refreshed = OAuthCredential {
            antithesis_token: refreshed.antithesis_token,
            // Per RFC 6749 §6, a refresh response without a new refresh token
            // means that the previous token is still valid.
            refresh_token: Some(refreshed.refresh_token.unwrap_or(refresh_token)),
        };
        self.store(refreshed.clone());
        // Persist back to the origin so the refreshed tokens survive across runs.
        if let Err(err) = self.origin.persist(PersistableCredentials::OAuth {
            antithesis_token: refreshed.antithesis_token,
            refresh_token: refreshed.refresh_token,
        }) {
            warn!("Unable to persist refreshed OAuth credential to durable storage: {err:#}");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct OAuthRefreshResponse {
    antithesis_token: String,
    refresh_token: Option<String>,
}

#[derive(Clone, Debug)]
pub enum OAuthRefreshInfo {
    Keychain {
        entry_name: String,
    },
    CredentialsFile {
        path: PathBuf,
        profile: Option<String>,
    },
}

impl OAuthRefreshInfo {
    pub(super) fn persist(&self, credentials: PersistableCredentials) -> Result<()> {
        match self {
            Self::Keychain { entry_name } => {
                let entry = Entry::new("snouty", entry_name)
                    .wrap_err("opening keychain entry to store the refreshed credential")?;
                entry
                    .set_password(&serde_json::to_string(&credentials)?)
                    .wrap_err("writing the refreshed credential to the keychain")
            }
            Self::CredentialsFile { path, profile } => {
                persist_to_file(credentials, profile.as_deref(), Some(path)).map(|_| ())
            }
        }
    }

    /// Path of the advisory lock file guarding refreshes for this origin
    fn lock_path(&self) -> Option<PathBuf> {
        match self {
            Self::CredentialsFile { path, profile } => {
                let scope = construct_keychain_credential_name(profile.as_deref());
                Some(
                    path.parent()?
                        .join("locks")
                        .join(format!("{}.{scope}.refresh.lock", path_lock_token(path))),
                )
            }
            Self::Keychain { entry_name } => {
                Some(lock_dir()?.join(format!("{entry_name}.refresh.lock")))
            }
        }
    }

    pub(super) fn load(&self) -> Result<Option<PersistableCredentials>> {
        match self {
            Self::Keychain { entry_name } => {
                let entry = match Entry::new("snouty", entry_name) {
                    Ok(entry) => entry,
                    Err(keyring_core::Error::NoDefaultStore) => return Ok(None),
                    Err(other) => return Err(eyre!("opening keychain entry: {other}")),
                };
                match entry.get_password() {
                    Ok(json) => match serde_json::from_str(&json) {
                        Ok(deserialized) => Ok(deserialized),
                        Err(err) => {
                            warn!(
                                "Deserialization of the value in the keychain failed with error {err:#}"
                            );
                            Ok(None)
                        }
                    },
                    Err(keyring_core::Error::NoEntry) => Ok(None),
                    Err(err) => {
                        warn!("keychain lookup for entry {entry_name} failed: {err}");
                        Ok(None)
                    }
                }
            }
            Self::CredentialsFile { path, profile } => {
                let Some(contents) = read_to_string_if_file_exists(path)? else {
                    return Ok(None);
                };
                let parsed = parse_credentials_file_toml(&contents, path)?;
                Ok(parsed.for_profile(profile.as_deref()))
            }
        }
    }
}

/// Best-effort cross-process advisory lock serializing refreshes for a
/// credential, held for as long as the returned file is kept alive. The lock
/// file is a filesystem sidecar (on disk even when the credential lives in the
/// keychain), so it serializes regardless of where the credential is stored.
/// `None` means no lock could be established and the caller should proceed
/// unserialized rather than fail.
async fn acquire_refresh_lock(refresh_info: &OAuthRefreshInfo) -> Option<std::fs::File> {
    let path = refresh_info.lock_path()?;
    // `File::lock` blocks until granted, so acquire on the blocking pool to keep
    // the runtime thread moving while another process holds the lock.
    tokio::task::spawn_blocking(move || {
        if let Some(dir) = path.parent() {
            mkdir(dir, true, 0o700).ok()?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .ok()?;
        file.lock().ok()?;
        Some(file)
    })
    .await
    .ok()
    .flatten()
}

async fn refresh_oauth_token(
    client: &reqwest::Client,
    base_url: &str,
    refresh_token: &str,
) -> Result<OAuthRefreshResponse> {
    let mut request = reqwest::Request::new(
        reqwest::Method::POST,
        reqwest::Url::from_str(&format!("{base_url}/auth/cli/refresh"))?,
    );
    request.headers_mut().insert(
        reqwest::header::AUTHORIZATION,
        to_header_value(&format!("Bearer {refresh_token}"), true)?,
    );
    request.timeout_mut().replace(Duration::from_secs(30));

    client
        .execute(request)
        .await?
        .error_for_status()
        .wrap_err(
            "Unable to refresh OAuth credential. Please run `snouty login` to obtain a new token.",
        )?
        .json::<OAuthRefreshResponse>()
        .await
        .wrap_err("parsing the OAuth refresh response")
}

fn lock_dir() -> Option<PathBuf> {
    global_settings_dir().map(|dir| dir.join("locks"))
}

fn path_lock_token(path: &Path) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthenticationInfo;
    use crate::testutils::auth::{
        credentials_file_in, paseto_expiring_in, public_paseto_with_claims,
    };
    use crate::testutils::fake_secrets::{
        FAKE_ACCESS_TOKEN_A, FAKE_ACCESS_TOKEN_B, FAKE_REFRESH_TOKEN_A, FAKE_REFRESH_TOKEN_B,
    };

    #[tokio::test]
    async fn refresh_retains_prior_refresh_token_when_response_omits_one() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            // New access token, but NO refresh_token (and no expires_in).
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "antithesis_token": FAKE_ACCESS_TOKEN_B
            })))
            .expect(1)
            .mount(&server)
            .await;

        let creds_dir = tempfile::TempDir::new().unwrap();
        let token = RefreshableOAuthToken::new(
            credentials_file_in(creds_dir.path()),
            FAKE_ACCESS_TOKEN_A.to_owned(),
            Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        );

        let client = crate::testutils::test_client();
        token.refresh(&client, &server.uri()).await.unwrap();

        let in_memory = token.current();
        assert_eq!(in_memory.antithesis_token, FAKE_ACCESS_TOKEN_B);
        assert_eq!(
            in_memory.refresh_token.as_deref(),
            Some(FAKE_REFRESH_TOKEN_A),
            "the refresh token must be retained when the server didn't rotate it"
        );

        let persisted = std::fs::read_to_string(creds_dir.path().join("credentials.toml")).unwrap();
        assert!(persisted.contains(FAKE_ACCESS_TOKEN_B), "got:\n{persisted}");
        assert!(
            persisted.contains(FAKE_REFRESH_TOKEN_A),
            "got:\n{persisted}"
        );
    }

    #[tokio::test]
    async fn refresh_adopts_token_persisted_by_another_process() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // If our code refreshed instead of adopting the newer persisted token,
        // it would hit this endpoint — `.expect(0)` then fails the test.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/auth/cli/refresh"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        // The origin already holds a newer credential — as though a *different*
        // snouty process refreshed while we held the stale one.
        let creds_dir = tempfile::TempDir::new().unwrap();
        let refresh_info = credentials_file_in(creds_dir.path());
        refresh_info
            .persist(PersistableCredentials::OAuth {
                antithesis_token: FAKE_ACCESS_TOKEN_B.to_owned(),
                refresh_token: Some(FAKE_REFRESH_TOKEN_B.to_owned()),
            })
            .unwrap();

        // The token holds the stale token (as if it just expired / 401'd).
        let token = RefreshableOAuthToken::new(
            refresh_info,
            FAKE_ACCESS_TOKEN_A.to_owned(),
            Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        );
        let clone = token.clone();
        let client = crate::testutils::test_client();
        token.refresh(&client, &server.uri()).await.unwrap();

        // Adopted the persisted token, in every clone; no network refresh
        // happened.
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "must not refresh when the origin already holds a newer token"
        );
        let in_memory = clone.current();
        assert_eq!(in_memory.antithesis_token, FAKE_ACCESS_TOKEN_B);
        assert_eq!(
            in_memory.refresh_token.as_deref(),
            Some(FAKE_REFRESH_TOKEN_B)
        );
    }

    #[tokio::test]
    async fn auth_header_does_not_refresh_an_expired_access_token() {
        use wiremock::matchers::any;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        // The origin holds a newer credential, which `auth_header` must not
        // adopt either.
        let creds_dir = tempfile::TempDir::new().unwrap();
        let refresh_info = credentials_file_in(creds_dir.path());
        refresh_info
            .persist(PersistableCredentials::OAuth {
                antithesis_token: FAKE_ACCESS_TOKEN_B.to_owned(),
                refresh_token: Some(FAKE_REFRESH_TOKEN_B.to_owned()),
            })
            .unwrap();
        let expired_token = paseto_expiring_in(-chrono::TimeDelta::minutes(1));
        let token = RefreshableOAuthToken::new(
            refresh_info,
            expired_token.clone(),
            Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        );
        assert!(token.expired());
        let auth = AuthenticationInfo::OAuth(token);

        let header = auth.auth_header().await.unwrap();

        assert_eq!(header, format!("Bearer {expired_token}"));
    }

    #[test]
    fn expired_is_true_when_the_oauth_access_token_is_past_its_exp() {
        let refresh_info = OAuthRefreshInfo::Keychain {
            entry_name: "unused".to_owned(),
        };
        let oauth = |token: String| {
            RefreshableOAuthToken::new(
                refresh_info.clone(),
                token,
                Some(FAKE_REFRESH_TOKEN_A.to_owned()),
            )
        };
        assert!(oauth(paseto_expiring_in(-chrono::TimeDelta::minutes(1))).expired());
        assert!(!oauth(paseto_expiring_in(chrono::TimeDelta::minutes(1))).expired());
        // An access token whose expiry cannot be read.
        assert!(!oauth(FAKE_ACCESS_TOKEN_A.to_owned()).expired());
    }

    #[test]
    fn expires_at_is_the_exp_of_an_oauth_access_token() {
        let refresh_info = OAuthRefreshInfo::Keychain {
            entry_name: "unused".to_owned(),
        };
        let token = public_paseto_with_claims(br#"{"exp":"2039-01-01T00:00:00+00:00"}"#);
        let oauth = RefreshableOAuthToken::new(refresh_info.clone(), token, None);
        assert_eq!(
            oauth.expires_at(),
            Some("2039-01-01T00:00:00Z".parse().unwrap())
        );
        // An access token whose expiry cannot be read.
        let opaque = RefreshableOAuthToken::new(refresh_info, FAKE_ACCESS_TOKEN_A.to_owned(), None);
        assert_eq!(opaque.expires_at(), None);
    }
}
