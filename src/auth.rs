use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};

use base64::{
    Engine,
    prelude::{BASE64_STANDARD, BASE64_URL_SAFE_NO_PAD},
};
use chrono::{DateTime, Utc};
use color_eyre::{
    Section,
    eyre::{Context, OptionExt, Result, eyre},
};
use http::HeaderValue;
use keyring_core::Entry;
use log::warn;
use progenitor_client::OperationInfo;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::sync::OnceCell;

use crate::{
    attributed_value::AttributedValue,
    env,
    error::user_error,
    settings::{
        back_up_unparsable_file, global_settings_dir, mkdir, read_to_string_if_file_exists,
    },
};

pub(crate) const API_KEY_VAR_NAME: &str = "ANTITHESIS_API_KEY";
pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";
const CREDENTIALS_FILENAME: &str = "credentials.toml";

const OIDC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct OAuthCredential {
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

impl OAuthCredential {
    fn is_expired(&self) -> bool {
        match try_get_expiry_from_token(&self.antithesis_token) {
            Some(expiry) => Utc::now() >= expiry,
            None => false,
        }
    }
}

/// Try to parse the supplied string as a PASETO token and return the `exp` claim
fn try_get_expiry_from_token(token: &str) -> Option<DateTime<Utc>> {
    #[derive(Deserialize)]
    struct PasetoClaims {
        exp: Option<serde_json::Value>,
    }

    let mut parts = token.splitn(4, '.');
    let _version = parts.next()?;
    let purpose = parts.next()?;
    let payload = parts.next()?;
    if purpose != "public" {
        return None;
    }

    let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims = serde_json::Deserializer::from_slice(&bytes)
        .into_iter::<PasetoClaims>()
        .next()?
        .ok()?;

    parse_exp_claim(&claims.exp?)
}

fn parse_exp_claim(exp: &serde_json::Value) -> Option<DateTime<Utc>> {
    if let Some(text) = exp.as_str() {
        return Some(DateTime::parse_from_rfc3339(text).ok()?.with_timezone(&Utc));
    }
    if let Some(secs) = exp.as_u64() {
        return DateTime::from_timestamp(secs as i64, 0);
    }
    None
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
    fn persist(&self, credentials: PersistableCredentials) -> Result<()> {
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

    fn load(&self) -> Result<Option<PersistableCredentials>> {
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
                let parsed = parse_credentials_file_toml(contents, path)?;
                Ok(match profile {
                    Some(profile) => parsed
                        .profile
                        .and_then(|by_profile| by_profile.get(profile).cloned()),
                    None => parsed.default,
                })
            }
        }
    }
}

#[derive(Clone)]
pub enum AuthenticationInfo {
    ApiKey {
        api_key: String,
    },
    GithubActionsOidc {
        url: String,
        request_token: String,
        // Feels pretty sus to stick our secrets on the heap
        cached: Arc<OnceCell<String>>,
    },
    OAuth {
        refresh_info: OAuthRefreshInfo,
        active_credential: Arc<RwLock<OAuthCredential>>,
    },
    Password {
        username: String,
        password: String,
    },
}

impl std::fmt::Debug for AuthenticationInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey { .. } => f
                .debug_struct("ApiKey")
                .field("api_key", &"[REDACTED]")
                .finish(),
            Self::GithubActionsOidc { url, cached, .. } => f
                .debug_struct("GithubActionsOidc")
                .field("url", url)
                .field("request_token", &"[REDACTED]")
                .field("cached", &cached.initialized())
                .finish(),
            Self::OAuth {
                refresh_info,
                active_credential,
                ..
            } => f
                .debug_struct("OAuth")
                .field("refresh_info", refresh_info)
                .field("active_credential", active_credential)
                .finish(),
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

impl AuthenticationInfo {
    fn try_from_env() -> Result<Option<AttributedValue<Self>>> {
        if let Some(api_key) = env::var(API_KEY_VAR_NAME)? {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::ApiKey { api_key },
                environment_variable_names: vec![API_KEY_VAR_NAME],
            }));
        }

        if let Some(username) = env::var(USERNAME_VAR_NAME)?
            && let Some(password) = env::var(PASSWORD_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::Password { username, password },
                environment_variable_names: vec![USERNAME_VAR_NAME, PASSWORD_VAR_NAME],
            }));
        }

        Ok(None)
    }

    fn try_from_keychain(profile: Option<&str>) -> Result<Option<AttributedValue<Self>>> {
        let entry_name = construct_keychain_credential_name(profile);
        let refresh_info = OAuthRefreshInfo::Keychain {
            entry_name: entry_name.clone(),
        };
        if let Some(found) = refresh_info.load()? {
            Ok(Some(AttributedValue::Keychain {
                value: found.convert_to_authentication_info(refresh_info),
                entry_name,
            }))
        } else {
            Ok(None)
        }
    }

    fn try_from_github_actions_environment() -> Result<Option<AttributedValue<Self>>> {
        const TARGET_URL_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_URL";
        const REQ_TOKEN_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_TOKEN";

        if let Some(actions_id_request_token) = env::var(REQ_TOKEN_VAR_NAME)?
            && let Some(actions_id_url) = env::var(TARGET_URL_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::GithubActionsOidc {
                    url: actions_id_url,
                    request_token: actions_id_request_token,
                    cached: Arc::new(OnceCell::new()),
                },
                environment_variable_names: vec![TARGET_URL_VAR_NAME, REQ_TOKEN_VAR_NAME],
            }));
        }

        Ok(None)
    }

    pub(crate) fn for_ambient_configuration_with_attribution(
        profile: Option<&str>,
        allow_password: bool,
    ) -> Result<AttributedValue<Self>> {
        if let Some(from_env) = Self::try_from_env()? {
            return reject_password_if_unsupported(from_env, allow_password);
        }

        let credentials_file: Option<(PathBuf, CredentialsFile)>;
        if let Some(profile_name) = profile {
            if let Some(from_keychain) = Self::try_from_keychain(profile)? {
                return reject_password_if_unsupported(from_keychain, allow_password);
            }

            credentials_file = match try_load_credentials_file()? {
                Some((path, parsed)) => {
                    if let Some(from_credentials_file) = parsed
                        .profile
                        .as_ref()
                        .and_then(|by_profile| by_profile.get(profile_name))
                    {
                        return reject_password_if_unsupported(
                            AttributedValue::SettingsFile {
                                value: from_credentials_file
                                    .clone()
                                    .convert_to_authentication_info(
                                        OAuthRefreshInfo::CredentialsFile {
                                            path: path.clone(),
                                            profile: Some(profile_name.to_owned()),
                                        },
                                    ),
                                settings_file_path: path,
                                profile: Some(profile_name.to_owned()),
                            },
                            allow_password,
                        );
                    }
                    Some((path, parsed))
                }
                None => None,
            };
        } else {
            credentials_file = try_load_credentials_file()?;
        }

        if let Some(from_keychain) = Self::try_from_keychain(None)? {
            return reject_password_if_unsupported(from_keychain, allow_password);
        }

        if let Some((path, parsed)) = credentials_file
            && let Some(from_credentials_file) = parsed.default
        {
            return reject_password_if_unsupported(
                AttributedValue::SettingsFile {
                    value: from_credentials_file.convert_to_authentication_info(
                        OAuthRefreshInfo::CredentialsFile {
                            path: path.clone(),
                            profile: None,
                        },
                    ),
                    settings_file_path: path,
                    profile: None,
                },
                allow_password,
            );
        }

        if let Some(from_github_actions_environment) = Self::try_from_github_actions_environment()?
        {
            return Ok(from_github_actions_environment);
        }

        Err(user_error("No Antithesis credentials found").suggestion(
            "set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one",
        ))
    }

    pub(crate) fn for_ambient_configuration(
        profile: Option<&str>,
        allow_password: bool,
    ) -> Result<Self> {
        Ok(Self::for_ambient_configuration_with_attribution(profile, allow_password)?.extract())
    }

    pub(crate) async fn authenticate_request<E>(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        request: &mut reqwest::Request,
        _info: &OperationInfo,
    ) -> std::result::Result<(), progenitor_client::Error<E>> {
        let header = self
            .auth_header(client, base_url)
            .await
            .map_err(|e| progenitor_client::Error::Custom(e.to_string()))?;
        request
            .headers_mut()
            .insert(reqwest::header::AUTHORIZATION, header);
        Ok(())
    }

    async fn auth_header(&self, client: &reqwest::Client, base_url: &str) -> Result<HeaderValue> {
        match self {
            Self::ApiKey { api_key } => to_header_value(&format!("Bearer {api_key}"), true),
            Self::GithubActionsOidc {
                url,
                request_token,
                cached,
            } => to_header_value(
                &format!(
                    "GHA {}",
                    cached
                        .get_or_try_init(|| fetch_github_actions_oidc_credentials(
                            url,
                            request_token
                        ))
                        .await?
                ),
                true,
            ),
            Self::OAuth {
                refresh_info,
                active_credential,
            } => oauth_auth_header(client, base_url, refresh_info, active_credential).await,
            Self::Password { username, password } => {
                let credentials = format!("{username}:{password}");
                let encoded = BASE64_STANDARD.encode(credentials);
                to_header_value(&format!("Basic {encoded}"), true)
            }
        }
    }

    pub(crate) fn can_refresh(&self) -> bool {
        match self {
            Self::OAuth {
                active_credential, ..
            } => active_credential
                .read()
                .map(|credential| credential.refresh_token.is_some())
                .unwrap_or(false),
            _ => false,
        }
    }

    pub(crate) async fn refresh_after_unauthorized(
        &self,
        client: &reqwest::Client,
        base_url: &str,
    ) -> Result<Option<HeaderValue>> {
        let Self::OAuth {
            refresh_info,
            active_credential,
        } = self
        else {
            return Ok(None);
        };

        // The access token that just got rejected.
        let stale_access_token = {
            let current = active_credential
                .read()
                .map_err(|err| eyre!("the OAuth credential lock is poisoned: {err}"))?;
            current.antithesis_token.clone()
        };

        match refresh_if_still_current(
            client,
            base_url,
            refresh_info,
            active_credential,
            &stale_access_token,
        )
        .await?
        {
            Some(access_token) => Ok(Some(to_header_value(
                &format!("Bearer {access_token}"),
                true,
            )?)),
            // No refresh token to try — let the caller surface the original 401.
            None => Ok(None),
        }
    }
}

#[cfg(test)]
impl AuthenticationInfo {
    pub(crate) fn oauth_for_test(
        antithesis_token: impl Into<String>,
        refresh_token: Option<&str>,
        refresh_info: OAuthRefreshInfo,
    ) -> Self {
        Self::OAuth {
            refresh_info,
            active_credential: Arc::new(RwLock::new(OAuthCredential {
                antithesis_token: antithesis_token.into(),
                refresh_token: refresh_token.map(str::to_owned),
            })),
        }
    }
}

/// Build the `Authorization` header for an OAuth credential, proactively
/// refreshing if the token has expired and a refresh token is available.
async fn oauth_auth_header(
    client: &reqwest::Client,
    base_url: &str,
    refresh_info: &OAuthRefreshInfo,
    active_credential: &Arc<RwLock<OAuthCredential>>,
) -> Result<HeaderValue> {
    // Fast path: use the current access token unless it has expired.
    let (expired, access_token) = {
        let current = active_credential
            .read()
            .map_err(|err| eyre!("the OAuth credential lock is poisoned: {err}"))?;
        (current.is_expired(), current.antithesis_token.clone())
    };

    let access_token = if expired {
        // Fall back to the (expired) token if there's nothing to refresh with —
        // the server will reject it and the reactive path can take over.
        refresh_if_still_current(
            client,
            base_url,
            refresh_info,
            active_credential,
            &access_token,
        )
        .await?
        .unwrap_or(access_token)
    } else {
        access_token
    };
    to_header_value(&format!("Bearer {access_token}"), true)
}

/// Refresh the access token, serialized *across processes* by an advisory file
/// lock and reconciled against the persisted credential.
///
/// `stale_access_token` is the token the caller found unusable (expired, or
/// rejected with 401). Returns `Some(token)` to (re)try with — freshly minted,
/// or one another process/refresh already produced — or `None` when nothing
/// better is available (no refresh token on hand).
async fn refresh_if_still_current(
    client: &reqwest::Client,
    base_url: &str,
    refresh_info: &OAuthRefreshInfo,
    active_credential: &Arc<RwLock<OAuthCredential>>,
    stale_access_token: &str,
) -> Result<Option<String>> {
    // Best-effort cross-process lock, held across the reload + refresh. If we
    // can't take it we proceed unserialized rather than fail the request; the
    // lock is released when `_lock` drops at the end of this scope.
    let _lock = acquire_refresh_lock(refresh_info).await;

    // Adopt whatever is currently persisted — another process may have refreshed
    // while we waited for the lock — so any refresh below uses the latest token.
    if let Ok(Some(PersistableCredentials::OAuth {
        antithesis_token,
        refresh_token,
    })) = refresh_info.load()
    {
        let mut writer = active_credential
            .write()
            .map_err(|err| eyre!("the OAuth credential lock is poisoned: {err}"))?;
        writer.antithesis_token = antithesis_token;
        writer.refresh_token = refresh_token;
    }

    let (current_access_token, current_refresh_token) = {
        let current = active_credential
            .read()
            .map_err(|err| eyre!("the OAuth credential lock is poisoned: {err}"))?;
        (
            current.antithesis_token.clone(),
            current.refresh_token.clone(),
        )
    };

    // Someone (another process, or an earlier refresh) already replaced the token
    // we were unhappy with — use theirs instead of refreshing again.
    if current_access_token != stale_access_token {
        return Ok(Some(current_access_token));
    }

    let Some(refresh_token) = current_refresh_token else {
        return Ok(None);
    };

    refresh_and_store(
        client,
        base_url,
        refresh_info,
        active_credential,
        &refresh_token,
    )
    .await
    .map(Some)
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

/// Refresh the access token using `refresh_token`, swap the new tokens into
/// `active_credential`, persist them back to their origin, and return the new
/// access token.
async fn refresh_and_store(
    client: &reqwest::Client,
    base_url: &str,
    refresh_info: &OAuthRefreshInfo,
    active_credential: &Arc<RwLock<OAuthCredential>>,
    refresh_token: &str,
) -> Result<String> {
    let refreshed = refresh_oauth_token(client, base_url, refresh_token).await?;
    let new_access_token = refreshed.antithesis_token;
    // Per RFC 6749 §6, a refresh response without a new refresh token means that the previous token is still valid
    let new_refresh_token = Some(
        refreshed
            .refresh_token
            .unwrap_or_else(|| refresh_token.to_owned()),
    );

    // Swap the new tokens into memory under a short-lived write lock.
    {
        let mut writer = active_credential
            .write()
            .map_err(|err| eyre!("the OAuth credential lock is poisoned: {err}"))?;
        writer.antithesis_token = new_access_token.clone();
        writer.refresh_token = new_refresh_token.clone();
    }

    // Persist back to the origin so the refreshed tokens survive across runs
    if let Err(err) = refresh_info.persist(PersistableCredentials::OAuth {
        antithesis_token: new_access_token.clone(),
        refresh_token: new_refresh_token,
    }) {
        warn!("Unable to persist refreshed OAuth credential to durable storage: {err:#}");
    }

    Ok(new_access_token)
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

fn try_load_credentials_file() -> Result<Option<(PathBuf, CredentialsFile)>> {
    if let Some((_dir, path)) = try_get_credentials_file_path()
        && let Some(contents) = read_to_string_if_file_exists(&path)?
    {
        let parsed = parse_credentials_file_toml(contents, &path)?;
        return Ok(Some((path, parsed)));
    }

    Ok(None)
}

fn try_get_credentials_file_path() -> Option<(PathBuf, PathBuf)> {
    if let Some(snouty_settings_dir) = global_settings_dir() {
        let path = snouty_settings_dir.join(CREDENTIALS_FILENAME);
        Some((snouty_settings_dir, path))
    } else {
        None
    }
}

/// Exchange the GitHub Actions OIDC *request* token for an Antithesis-audience
/// OIDC token by calling the Actions token endpoint.
///
/// Split out from [`Credentials::try_from_github_actions_environment`] so the
/// HTTP exchange can be unit-tested against a local server without mutating the
/// process environment (which would race other tests under threaded
/// `cargo test`). The request URL already carries a query string, so the
/// audience is appended with `&`.
async fn fetch_github_actions_oidc_credentials(
    actions_id_url: &str,
    actions_id_request_token: &str,
) -> Result<String> {
    #[derive(Deserialize)]
    struct OidcTokenResponse {
        value: String,
    }

    let client = reqwest::Client::builder()
        .timeout(OIDC_REQUEST_TIMEOUT)
        .build()?;
    let response: OidcTokenResponse = client
        .get(format!("{actions_id_url}&audience=antithesis"))
        .bearer_auth(actions_id_request_token)
        .send()
        .await?
        .error_for_status()
        .wrap_err("failed to fetch a GitHub Actions OIDC token")?
        .json()
        .await?;

    Ok(response.value)
}

fn to_header_value(content: &str, sensitive: bool) -> Result<HeaderValue> {
    let mut hv = HeaderValue::from_str(content).wrap_err("failed to build Authorization header")?;
    hv.set_sensitive(sensitive);
    Ok(hv)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum PersistableCredentials {
    ApiKey {
        api_key: String,
    },
    OAuth {
        antithesis_token: String,
        refresh_token: Option<String>,
    },
    Password {
        username: String,
        password: String,
    },
}

impl std::fmt::Debug for PersistableCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey { .. } => f
                .debug_struct("ApiKey")
                .field("api_key", &"[REDACTED]")
                .finish(),
            Self::OAuth {
                antithesis_token, ..
            } => f
                .debug_struct("OAuth")
                .field("antithesis_token", &"[REDACTED]")
                .field("refresh_token", &"[REDACTED]")
                .field("expiry", &try_get_expiry_from_token(antithesis_token))
                .finish(),
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

impl PersistableCredentials {
    fn convert_to_authentication_info(self, refresh_info: OAuthRefreshInfo) -> AuthenticationInfo {
        match self {
            Self::ApiKey { api_key } => AuthenticationInfo::ApiKey { api_key },
            Self::OAuth {
                antithesis_token,
                refresh_token,
            } => AuthenticationInfo::OAuth {
                refresh_info,
                active_credential: Arc::new(RwLock::new(OAuthCredential {
                    antithesis_token,
                    refresh_token,
                })),
            },
            Self::Password { username, password } => {
                AuthenticationInfo::Password { username, password }
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub fn initialize_credential_store() -> Result<()> {
    if env::var("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE").is_ok_and(|v| v.is_some()) {
        return Ok(());
    }

    keyring_core::set_default_store(apple_native_keyring_store::keychain::Store::new()?);

    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn initialize_credential_store() -> Result<()> {
    // pass
    Ok(())
}

pub(crate) fn persist(
    credentials: PersistableCredentials,
    profile: Option<&str>,
) -> Result<AttributedValue<()>> {
    match try_persist_to_keychain(&credentials, profile) {
        Err(err) => Err(err),
        Ok(Some(entry_name)) => Ok(AttributedValue::Keychain {
            value: (),
            entry_name,
        }),
        Ok(None) => {
            persist_to_file(credentials, profile, None).map(|path| AttributedValue::SettingsFile {
                value: (),
                settings_file_path: path,
                profile: profile.map(|p| p.to_owned()),
            })
        }
    }
}

fn try_persist_to_keychain(
    credentials: &PersistableCredentials,
    profile: Option<&str>,
) -> Result<Option<String>> {
    let credential_name = construct_keychain_credential_name(profile);

    let credential = match Entry::new("snouty", credential_name.as_str()) {
        Ok(cred) => Ok(cred),
        // A NoDefaultStore error indicates that the version of initialize_credential_store() selected by the compiler was a no-op
        Err(keyring_core::Error::NoDefaultStore) => return Ok(None),
        Err(other) => Err(other),
    }?;

    credential.set_password(serde_json::to_string(credentials)?.as_str())?;

    clear_from_file_if_present(profile);

    Ok(Some(credential_name))
}

fn construct_keychain_credential_name(profile: Option<&str>) -> String {
    profile
        .map(|p| format!("profile_{p}"))
        .unwrap_or_else(|| "_default_".to_owned())
}

fn clear_from_file_if_present(profile: Option<&str>) {
    let Some((parent_dir, path)) = try_get_credentials_file_path() else {
        return;
    };

    if let Ok(Some(contents)) = read_to_string_if_file_exists(&path)
        && let Ok(mut creds_file) = parse_credentials_file_toml(contents, &path)
    {
        let mut changed = false;
        if let Some(profile) = profile {
            if let Some(by_profile) = creds_file.profile.as_mut() {
                changed = by_profile.remove(profile).is_some();
            }
        } else {
            changed = creds_file.default.is_some();
            creds_file.default = None;
        }

        if changed
            && let Ok(mut temp) = NamedTempFile::new_in(&parent_dir)
            && let Ok(to_write) = toml::to_string_pretty(&creds_file)
            && temp.write_all(to_write.as_bytes()).is_ok()
        {
            warn!(
                "The supplied credentials were stored in the keychain, but an entry under {} profile name was also present in the user credentials file. Clearing the entry from the credentials file in favor of what was committed to the keychain.",
                if profile.is_some() { "the same" } else { "no" }
            );
            let _ = temp.persist(&path);
        }
    }
}

fn persist_to_file(
    credentials: PersistableCredentials,
    profile: Option<&str>,
    path: Option<&PathBuf>,
) -> Result<PathBuf> {
    let (settings_dir, path) = match path {
        None =>  try_get_credentials_file_path().ok_or_eyre(
            "Could not determine settings directory. Please ensure $XDG_CONFIG_HOME or $HOME is set",
        )?,
        Some(explicit_path) => match explicit_path.parent() {
            None => return Err(eyre!("Unable to determine parent directory of {}", explicit_path.to_str().unwrap_or("[invalid path]"))),
            Some(parent_dir) => (parent_dir.to_path_buf(), explicit_path.to_path_buf()),
        }
    };

    mkdir(&settings_dir, true, 0o700)?;

    let mut current_contents = match read_to_string_if_file_exists(&path)? {
        Some(contents) => match parse_credentials_file_toml(contents, &path) {
            Ok(file) => file,
            Err(_) => {
                let backup = back_up_unparsable_file(&path)?;
                // Same shape as the settings repair note: paths on their own
                // lines, since they are what the user may copy and what made
                // the one-line form overflow.
                eprintln!(
                    "warning: the existing credentials file could not be parsed; a new one will be written.\n  kept as a backup: {}\n  will be rewritten: {}",
                    backup.display(),
                    path.display(),
                );
                CredentialsFile {
                    default: None,
                    profile: None,
                }
            }
        },
        None => CredentialsFile {
            default: None,
            profile: None,
        },
    };

    if let Some(profile) = profile {
        if current_contents.profile.is_none() {
            current_contents.profile = Some(HashMap::new());
        }

        current_contents
            .profile
            .as_mut()
            .unwrap()
            .insert(profile.to_owned(), credentials);
    } else {
        current_contents.default = Some(credentials);
    }

    let mut temp = NamedTempFile::new_in(&settings_dir)?;
    temp.write_all(toml::to_string_pretty(&current_contents)?.as_bytes())?;

    temp.persist(&path)?;

    Ok(path)
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

fn parse_credentials_file_toml(contents: String, path: &Path) -> Result<CredentialsFile> {
    toml::from_str::<CredentialsFile>(&contents).wrap_err(format!(
        "{:?} is not valid TOML or cannot be parsed as a Snouty credentials file.",
        path
    ))
}

/// The one-line remediation for the username/password deprecation, shared by
/// every message that states it (the rejection suggestion here and the doctor
/// warning note) so the wording cannot drift apart.
pub(crate) const PASSWORD_DEPRECATION_SUGGESTION: &str = "username/password authentication is deprecated; run `snouty login` to switch to another authentication method";

/// Reject username/password credentials for commands that don't support them —
/// every endpoint other than the launch webhooks answers them with an opaque
/// 403, so failing here gives the user an actionable message instead.
fn reject_password_if_unsupported(
    authn_info: AttributedValue<AuthenticationInfo>,
    allow_password: bool,
) -> Result<AttributedValue<AuthenticationInfo>> {
    if !allow_password && matches!(authn_info.value(), AuthenticationInfo::Password { .. }) {
        return Err(user_error(
            "This command does not accept username/password authentication, which is only supported when launching runs (`snouty launch`, `snouty debug`)",
        )
        .suggestion(PASSWORD_DEPRECATION_SUGGESTION));
    }

    Ok(authn_info)
}

/// Deprecation notice for username/password authentication, printed when a
/// command proceeds with those credentials (see
/// [`crate::api::AntithesisApi::new_for_launch`]).
pub(crate) fn warn_password_auth_deprecated() {
    eprintln!(
        "warning: username/password authentication is deprecated and will be removed in a future release.\n  \
         Run `snouty login` to switch to another authentication method."
    );
}

#[derive(Serialize, Deserialize)]
struct CredentialsFile {
    default: Option<PersistableCredentials>,
    profile: Option<HashMap<String, PersistableCredentials>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;
    use std::time::Duration;

    #[tokio::test]
    async fn refresh_retains_prior_refresh_token_when_response_omits_one() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            // New access token, but NO refresh_token (and no expires_in).
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "antithesis_token": "new-access-token"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let active_credential = Arc::new(RwLock::new(OAuthCredential {
            antithesis_token: "old-access-token".to_owned(),
            refresh_token: Some("keep-me".to_owned()),
        }));

        let creds_dir = tempfile::TempDir::new().unwrap();
        let refresh_info = OAuthRefreshInfo::CredentialsFile {
            path: creds_dir.path().join("credentials.toml"),
            profile: None,
        };

        let client = reqwest::Client::new();
        let new_access = refresh_and_store(
            &client,
            &server.uri(),
            &refresh_info,
            &active_credential,
            "keep-me",
        )
        .await
        .unwrap();

        assert_eq!(new_access, "new-access-token");

        let in_memory = active_credential.read().unwrap();
        assert_eq!(in_memory.antithesis_token, "new-access-token");
        assert_eq!(
            in_memory.refresh_token.as_deref(),
            Some("keep-me"),
            "the refresh token must be retained when the server didn't rotate it"
        );

        let persisted = std::fs::read_to_string(creds_dir.path().join("credentials.toml")).unwrap();
        assert!(persisted.contains("new-access-token"), "got:\n{persisted}");
        assert!(persisted.contains("keep-me"), "got:\n{persisted}");
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

        // In-memory holds the stale token (as if it just expired / 401'd).
        let active_credential = Arc::new(RwLock::new(OAuthCredential {
            antithesis_token: "stale-access".to_owned(),
            refresh_token: Some("stale-refresh".to_owned()),
        }));

        // The origin already holds a newer credential — as though a *different*
        // snouty process refreshed while we held the stale one.
        let creds_dir = tempfile::TempDir::new().unwrap();
        let refresh_info = OAuthRefreshInfo::CredentialsFile {
            path: creds_dir.path().join("credentials.toml"),
            profile: None,
        };
        refresh_info
            .persist(PersistableCredentials::OAuth {
                antithesis_token: "fresh-from-other-process".to_owned(),
                refresh_token: Some("fresh-refresh".to_owned()),
            })
            .unwrap();

        let client = reqwest::Client::new();
        let result = refresh_if_still_current(
            &client,
            &server.uri(),
            &refresh_info,
            &active_credential,
            "stale-access",
        )
        .await
        .unwrap();

        // Adopted the persisted token; no network refresh happened. The
        // network assertion runs before the read lock is taken, so no guard
        // is held across an await point (clippy::await_holding_lock).
        assert_eq!(result.as_deref(), Some("fresh-from-other-process"));
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "must not refresh when the origin already holds a newer token"
        );
        let in_memory = active_credential.read().unwrap();
        assert_eq!(in_memory.antithesis_token, "fresh-from-other-process");
        assert_eq!(in_memory.refresh_token.as_deref(), Some("fresh-refresh"));
    }

    /// Build a `v4.public` PASETO whose payload is `claims_json ‖ signature`,
    /// mirroring the real wire format (a 64-byte Ed25519 signature stand-in
    /// trails the JSON claims).
    fn public_paseto_with_claims(claims_json: &[u8]) -> String {
        let mut payload = claims_json.to_vec();
        payload.extend_from_slice(&[0u8; 64]);
        format!("v4.public.{}", BASE64_URL_SAFE_NO_PAD.encode(&payload))
    }

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
    const CANONICAL_VECTOR_EXP_UNIX: u64 = 1_546_300_800;

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector() {
        // Official PASETO 2-S-1 test vector: a real token with a genuine 64-byte
        // Ed25519 signature trailing the JSON claims (no footer). This exercises
        // the real base64url decode and the skip-the-signature parse — unlike the
        // synthetic tokens above whose "signature" is 64 zero bytes.
        let token = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9HQr8URrGntTu7Dz9J2IF23d1M7-9lH9xiqdGyJNvzp4angPW5Esc7C5huy_M8I8_DjJK2ZXC2SUYuOFM-Q_5Cw";
        assert_eq!(
            try_get_expiry_from_token(token),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX as i64, 0)
        );
    }

    #[test]
    fn expiry_parsed_from_canonical_v2_public_vector_with_footer() {
        // Official PASETO 2-S-2 test vector: same claims, but with a footer
        // (`UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz` = "Paragon Initiative
        // Enterprises"). Confirms the 4th `.`-delimited segment is ignored.
        let token = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9flsZsx_gYCR0N_Ec2QxJFFpvQAs7h9HtKwbVK2n1MJ3Rz-hwe8KUqjnd8FAnIJZ601tp7lGkguU63oGbomhoBw.UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz";
        assert_eq!(
            try_get_expiry_from_token(token),
            DateTime::from_timestamp(CANONICAL_VECTOR_EXP_UNIX as i64, 0)
        );
    }

    /// The parts of an inbound HTTP request the OIDC exchange test asserts on.
    struct CapturedRequest {
        request_line: String,
        authorization: Option<String>,
    }

    /// Spawn a one-shot HTTP server that records the request it receives and
    /// answers it with `status` (e.g. `"200 OK"`) and a JSON `body`. Returns the
    /// request URL — already carrying a query string, like the real Actions
    /// endpoint — and a channel that yields the captured request once it arrives.
    fn spawn_oidc_token_server(
        status: &'static str,
        body: &'static str,
    ) -> (String, Receiver<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock OIDC server");
        let addr = listener.local_addr().expect("mock server address");
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut response_stream = stream.try_clone().expect("clone stream");
            let mut reader = BufReader::new(stream);

            let mut request_line = String::new();
            reader
                .read_line(&mut request_line)
                .expect("read request line");

            let mut authorization = None;
            loop {
                let mut line = String::new();
                let read = reader.read_line(&mut line).expect("read header line");
                if read == 0 || line == "\r\n" || line == "\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.trim().eq_ignore_ascii_case("authorization")
                {
                    authorization = Some(value.trim().to_owned());
                }
            }

            tx.send(CapturedRequest {
                request_line: request_line.trim().to_owned(),
                authorization,
            })
            .expect("send captured request");

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            );
            response_stream
                .write_all(response.as_bytes())
                .expect("write response");
            response_stream.flush().expect("flush response");
        });

        (format!("http://{addr}/token?api-version=2.0"), rx)
    }

    #[tokio::test]
    async fn github_actions_oidc_exchange_sends_bearer_token_and_audience() {
        // The endpoint returns the JWT wrapped in a JSON envelope, exactly as
        // GitHub's Actions OIDC endpoint does.
        let (url, requests) =
            spawn_oidc_token_server("200 OK", r#"{"count":1,"value":"oidc-jwt-token-value"}"#);

        let token = fetch_github_actions_oidc_credentials(&url, "actions-request-token")
            .await
            .unwrap();

        assert_eq!(token, "oidc-jwt-token-value");

        let request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("server should have received a request");

        // The Antithesis audience is appended onto the (already query-bearing) URL.
        assert!(
            request.request_line.contains("audience=antithesis"),
            "request line missing audience: {:?}",
            request.request_line
        );
        // The Actions request token is presented as a bearer credential.
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer actions-request-token")
        );
    }

    #[tokio::test]
    async fn github_actions_oidc_exchange_errors_on_non_success_status() {
        // A rejected request token (or any non-2xx) must surface as an error
        // rather than letting the error body be mistaken for a token.
        let (url, _requests) =
            spawn_oidc_token_server("403 Forbidden", r#"{"message":"bad credentials"}"#);

        let result = fetch_github_actions_oidc_credentials(&url, "actions-request-token").await;
        assert!(result.is_err(), "expected an error for a 403 response");
    }

    #[tokio::test]
    async fn github_actions_oidc_token_is_fetched_once_and_cached() {
        // The mock server accepts exactly one connection and then shuts down, so
        // a second exchange would fail with a connection error. Both
        // `auth_header` calls succeeding proves the token is reused from the
        // cache rather than re-fetched per request.
        let (url, requests) =
            spawn_oidc_token_server("200 OK", r#"{"count":1,"value":"oidc-jwt-token-value"}"#);

        let auth = AuthenticationInfo::GithubActionsOidc {
            url,
            request_token: "actions-request-token".to_owned(),
            cached: Arc::new(OnceCell::new()),
        };

        let client = reqwest::ClientBuilder::new().build().unwrap();
        let first = auth
            .auth_header(&client, "https://snouty.example")
            .await
            .unwrap();
        let second = auth
            .auth_header(&client, "https://snouty.example")
            .await
            .unwrap();
        assert_eq!(first.to_str().unwrap(), "GHA oidc-jwt-token-value");
        assert_eq!(first, second);

        // Exactly one request reached the server: the first fetch is captured,
        // and no second request ever arrives.
        requests
            .recv_timeout(Duration::from_secs(5))
            .expect("first fetch should hit the server");
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "second auth_header call should be served from cache, not re-fetched"
        );
    }
}
