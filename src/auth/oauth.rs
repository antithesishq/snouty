//! OAuth credentials from `snouty login`: an access token, and the refresh
//! token that replaces it when it expires.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use chrono::{DateTime, Utc};
use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use log::warn;
use reqwest::StatusCode;
use serde::Deserialize;

use super::{
    claims::try_get_expiry_from_token,
    login_command,
    store::{CredentialStore, PersistableCredentials, keychain_entry_name},
    to_header_value,
};
use crate::settings::{global_settings_dir, mkdir};

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
/// store that holds them. Clones share one credential: a refresh through
/// any clone gives every clone the new token.
#[derive(Clone, Debug)]
pub struct RefreshableOAuthToken {
    store: CredentialStore,
    credential: Arc<RwLock<OAuthCredential>>,
}

impl RefreshableOAuthToken {
    pub(crate) fn new(
        store: CredentialStore,
        antithesis_token: String,
        refresh_token: Option<String>,
    ) -> Self {
        Self {
            store,
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

    fn set_credential(&self, credential: OAuthCredential) {
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
    /// that the store holds, when another process or refresh stored it, or
    /// else a new one from the refresh endpoint at `base_url`, which the
    /// store then holds. An advisory file lock serializes refreshes across
    /// processes, so that two do not both spend one refresh token.
    ///
    /// Fails when there is no newer token: the store holds the same token,
    /// and there is no refresh token, or the refresh fails.
    pub(crate) async fn refresh(&self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        let held_access_token = self.access_token();

        // Best-effort cross-process lock, held across the reload + refresh. If
        // we can't take it we proceed unserialized rather than fail; the lock
        // is released when `_lock` drops at the end of this scope.
        let _lock = acquire_refresh_lock(&self.store).await;

        // Adopt whatever is currently persisted — another process may have
        // refreshed while we waited for the lock — so any refresh below uses
        // the latest token.
        if let Ok(Some(PersistableCredentials::OAuth {
            antithesis_token,
            refresh_token,
        })) = self.store.load()
        {
            self.set_credential(OAuthCredential {
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
            return Err(RenewalError::refused(self.store.profile()).report(None));
        };

        let refreshed =
            refresh_oauth_token(client, base_url, &refresh_token, self.store.profile()).await?;
        let refreshed = OAuthCredential {
            antithesis_token: refreshed.antithesis_token,
            // Per RFC 6749 §6, a refresh response without a new refresh token
            // means that the previous token is still valid.
            refresh_token: Some(refreshed.refresh_token.unwrap_or(refresh_token)),
        };
        self.set_credential(refreshed.clone());
        // Persist back to the store so the refreshed tokens survive across runs.
        if let Err(err) = self.store.persist(PersistableCredentials::OAuth {
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

impl CredentialStore {
    /// The advisory lock file that serializes refreshes of this store.
    fn lock_path(&self) -> Option<PathBuf> {
        let scope = keychain_entry_name(self.profile());
        match self {
            Self::CredentialsFile { path, .. } => Some(
                path.parent()?
                    .join("locks")
                    .join(format!("{}.{scope}.refresh.lock", path_lock_token(path))),
            ),
            Self::Keychain { .. } => Some(lock_dir()?.join(format!("{scope}.refresh.lock"))),
        }
    }
}

/// Best-effort cross-process advisory lock serializing refreshes for a
/// credential, held for as long as the returned file is kept alive. The lock
/// file is a filesystem sidecar (on disk even when the credential lives in the
/// keychain), so it serializes regardless of where the credential is stored.
/// `None` means no lock could be established and the caller should proceed
/// unserialized rather than fail.
async fn acquire_refresh_lock(store: &CredentialStore) -> Option<std::fs::File> {
    let path = store.lock_path()?;
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

/// Why the tenant did not renew the OAuth sign-in of `profile`. The message
/// also names the next step, because an API request keeps only the message
/// of its error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RenewalError {
    /// The sign-in needs a new login: the tenant answered a 4xx other than
    /// 408 and 429, or there is no refresh token.
    #[error("{}; {}", self.summary(), self.next_step())]
    Refused { profile: Option<String> },
    /// A later try can succeed: the tenant answered a 5xx, a 408 or a 429,
    /// did not answer, or sent an answer that snouty cannot read.
    #[error("{}; {}", self.summary(), self.next_step())]
    Temporary { profile: Option<String> },
}

impl RenewalError {
    fn refused(profile: Option<&str>) -> Self {
        Self::Refused {
            profile: profile.map(str::to_owned),
        }
    }

    fn temporary(profile: Option<&str>) -> Self {
        Self::Temporary {
            profile: profile.map(str::to_owned),
        }
    }

    /// The error for a refresh request that failed with `err`.
    fn of_request(err: &reqwest::Error, profile: Option<&str>) -> Self {
        match err.status() {
            Some(status)
                if status.is_client_error()
                    && !matches!(
                        status,
                        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
                    ) =>
            {
                Self::refused(profile)
            }
            _ => Self::temporary(profile),
        }
    }

    /// A report of `self`, caused by `cause`. It is the user's to fix, so it
    /// has no backtrace.
    fn report(self, cause: Option<Report>) -> Report {
        match cause {
            Some(cause) => cause.wrap_err(self),
            None => Report::new(self),
        }
        .suppress_backtrace(true)
    }

    /// The message without the next step.
    pub(crate) fn summary(&self) -> &'static str {
        match self {
            Self::Refused { .. } => "the sign-in expired and could not be renewed",
            Self::Temporary { .. } => "the tenant could not renew the sign-in",
        }
    }

    pub(crate) fn next_step(&self) -> String {
        match self {
            Self::Refused { profile } => {
                format!("run {} to sign in again", login_command(profile.as_deref()))
            }
            Self::Temporary { profile } => format!(
                "try again later, or run {} to sign in again",
                login_command(profile.as_deref())
            ),
        }
    }
}

async fn refresh_oauth_token(
    client: &reqwest::Client,
    base_url: &str,
    refresh_token: &str,
    profile: Option<&str>,
) -> Result<OAuthRefreshResponse> {
    let mut request = reqwest::Request::new(
        reqwest::Method::POST,
        reqwest::Url::from_str(&format!("{base_url}/auth/cli/refresh"))
            .expect("a base URL with a path is a URL"),
    );
    // Only a damaged credentials file holds a refresh token that is not a
    // header value, and a new sign-in replaces it.
    let header = to_header_value(&format!("Bearer {refresh_token}"), true)
        .map_err(|e| RenewalError::refused(profile).report(Some(e)))?;
    request
        .headers_mut()
        .insert(reqwest::header::AUTHORIZATION, header);
    request.timeout_mut().replace(Duration::from_secs(30));

    let renewal_error =
        |err: reqwest::Error| RenewalError::of_request(&err, profile).report(Some(err.into()));
    client
        .execute(request)
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(renewal_error)?
        .json()
        .await
        .map_err(renewal_error)
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
    use wiremock::ResponseTemplate;

    #[tokio::test]
    async fn refresh_retains_prior_refresh_token_when_response_omits_one() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer};

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

    /// Refreshes a token of `profile` against a tenant that answers
    /// `response`, or that does not answer when `response` is `None`, and
    /// returns the error.
    async fn refresh_error(profile: Option<&str>, response: Option<ResponseTemplate>) -> Report {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer};

        // The server answers only while it lives, so it outlives the refresh.
        let server = match response {
            Some(response) => {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/auth/cli/refresh"))
                    .respond_with(response)
                    .mount(&server)
                    .await;
                Some(server)
            }
            None => None,
        };
        let base_url = server
            .as_ref()
            .map_or_else(|| "http://127.0.0.1:1".to_owned(), MockServer::uri);
        let creds_dir = tempfile::TempDir::new().unwrap();
        let store = CredentialStore::CredentialsFile {
            path: creds_dir.path().join("credentials.toml"),
            profile: profile.map(str::to_owned),
        };
        let token = RefreshableOAuthToken::new(
            store,
            FAKE_ACCESS_TOKEN_A.to_owned(),
            Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        );
        let client = crate::testutils::test_client();
        token.refresh(&client, &base_url).await.unwrap_err()
    }

    #[tokio::test]
    async fn a_refused_refresh_says_to_sign_in_again() {
        for status in [401, 403] {
            let err = refresh_error(None, Some(ResponseTemplate::new(status))).await;
            assert_eq!(
                err.to_string(),
                "the sign-in expired and could not be renewed; \
                 run `snouty login` to sign in again"
            );
        }
        let err = refresh_error(Some("staging"), Some(ResponseTemplate::new(401))).await;
        assert_eq!(
            err.to_string(),
            "the sign-in expired and could not be renewed; \
             run `snouty login --profile staging` to sign in again"
        );
    }

    #[tokio::test]
    async fn a_sign_in_without_a_refresh_token_says_to_sign_in_to_its_profile() {
        let creds_dir = tempfile::TempDir::new().unwrap();
        let store = CredentialStore::CredentialsFile {
            path: creds_dir.path().join("credentials.toml"),
            profile: Some("staging".to_owned()),
        };
        let token = RefreshableOAuthToken::new(store, FAKE_ACCESS_TOKEN_A.to_owned(), None);
        let client = crate::testutils::test_client();
        let err = token
            .refresh(&client, "http://127.0.0.1:1")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "the sign-in expired and could not be renewed; \
             run `snouty login --profile staging` to sign in again"
        );
    }

    #[tokio::test]
    async fn a_refresh_that_the_tenant_cannot_answer_says_to_try_again_later() {
        let unreadable = ResponseTemplate::new(200).set_body_string("<html>");
        let responses = [500, 503, 408, 429]
            .map(|status| Some(ResponseTemplate::new(status)))
            .into_iter()
            .chain([Some(unreadable), None]);
        for response in responses {
            let err = refresh_error(None, response).await;
            assert_eq!(
                err.to_string(),
                "the tenant could not renew the sign-in; \
                 try again later, or run `snouty login` to sign in again",
                "{err:#}"
            );
        }
    }

    #[tokio::test]
    async fn refresh_adopts_token_persisted_by_another_process() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer};

        // If our code refreshed instead of adopting the newer persisted token,
        // it would hit this endpoint — `.expect(0)` then fails the test.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/auth/cli/refresh"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        // The store already holds a newer credential — as though a *different*
        // snouty process refreshed while we held the stale one.
        let creds_dir = tempfile::TempDir::new().unwrap();
        let store = credentials_file_in(creds_dir.path());
        store
            .persist(PersistableCredentials::OAuth {
                antithesis_token: FAKE_ACCESS_TOKEN_B.to_owned(),
                refresh_token: Some(FAKE_REFRESH_TOKEN_B.to_owned()),
            })
            .unwrap();

        // The token holds the stale token, as if the API just rejected it.
        let token = RefreshableOAuthToken::new(
            store,
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
            "must not refresh when the store already holds a newer token"
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
        use wiremock::{Mock, MockServer};

        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        // The store holds a newer credential, which `auth_header` must not
        // adopt either.
        let creds_dir = tempfile::TempDir::new().unwrap();
        let store = credentials_file_in(creds_dir.path());
        store
            .persist(PersistableCredentials::OAuth {
                antithesis_token: FAKE_ACCESS_TOKEN_B.to_owned(),
                refresh_token: Some(FAKE_REFRESH_TOKEN_B.to_owned()),
            })
            .unwrap();
        let expired_token = paseto_expiring_in(-chrono::TimeDelta::minutes(1));
        let token = RefreshableOAuthToken::new(
            store,
            expired_token.clone(),
            Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        );
        assert!(token.expired());
        let auth = AuthenticationInfo::OAuth(token);

        let header = auth.auth_header().unwrap();

        assert_eq!(header, format!("Bearer {expired_token}"));
    }

    #[test]
    fn expired_is_true_when_the_oauth_access_token_is_past_its_exp() {
        let store = CredentialStore::Keychain { profile: None };
        let oauth = |token: String| {
            RefreshableOAuthToken::new(store.clone(), token, Some(FAKE_REFRESH_TOKEN_A.to_owned()))
        };
        assert!(oauth(paseto_expiring_in(-chrono::TimeDelta::minutes(1))).expired());
        assert!(!oauth(paseto_expiring_in(chrono::TimeDelta::minutes(1))).expired());
        // An access token whose expiry cannot be read.
        assert!(!oauth(FAKE_ACCESS_TOKEN_A.to_owned()).expired());
    }

    #[test]
    fn expires_at_is_the_exp_of_an_oauth_access_token() {
        let store = CredentialStore::Keychain { profile: None };
        let token = public_paseto_with_claims(br#"{"exp":"2039-01-01T00:00:00+00:00"}"#);
        let oauth = RefreshableOAuthToken::new(store.clone(), token, None);
        assert_eq!(
            oauth.expires_at(),
            Some("2039-01-01T00:00:00Z".parse().unwrap())
        );
        // An access token whose expiry cannot be read.
        let opaque = RefreshableOAuthToken::new(store, FAKE_ACCESS_TOKEN_A.to_owned(), None);
        assert_eq!(opaque.expires_at(), None);
    }
}
