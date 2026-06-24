use std::{
    collections::HashMap, fs, io::Write, os::unix::fs::DirBuilderExt, path::Path, time::Duration,
};

use base64::{Engine, prelude::BASE64_STANDARD};
use color_eyre::{
    Section,
    eyre::{Context, OptionExt, Result, eyre},
};
use http::HeaderValue;
use keyring_core::{Entry, set_default_store};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::{
    attributed_value::AttributedValue,
    env,
    error::user_error,
    settings::{global_settings_dir, read_to_string_if_file_exists},
};

pub(crate) const API_KEY_VAR_NAME: &str = "ANTITHESIS_API_KEY";
pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";
const CREDENTIALS_FILENAME: &str = "credentials.toml";

const OIDC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Serialize, Deserialize)]
pub struct ApiKeyCredentials {
    pub(crate) api_key: String,
}

impl std::fmt::Debug for ApiKeyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyCredentials")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PasswordCredentials {
    pub username: String,
    pub(crate) password: String,
}

impl std::fmt::Debug for PasswordCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasswordCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct GithubActionsOidcCredentials {
    token: String,
}

impl std::fmt::Debug for GithubActionsOidcCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubActionsOidcCredentials")
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum Credentials {
    GithubActionsOidc(GithubActionsOidcCredentials),
    ApiKey(ApiKeyCredentials),
    Password(PasswordCredentials),
}

impl Credentials {
    pub(crate) fn for_api_key(api_key: String) -> Self {
        Self::ApiKey(ApiKeyCredentials { api_key })
    }

    pub(crate) fn for_password(username: String, password: String) -> Self {
        Self::Password(PasswordCredentials { username, password })
    }

    fn try_from_env() -> Result<Option<AttributedValue<Self>>> {
        if let Some(api_key) = env::var(API_KEY_VAR_NAME)? {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::for_api_key(api_key),
                environment_variable_names: vec![API_KEY_VAR_NAME],
            }));
        }

        if let Some(username) = env::var(USERNAME_VAR_NAME)?
            && let Some(password) = env::var(PASSWORD_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: Self::for_password(username, password),
                environment_variable_names: vec![USERNAME_VAR_NAME, PASSWORD_VAR_NAME],
            }));
        }

        Ok(None)
    }

    fn try_from_keychain(profile: Option<&str>) -> Result<Option<AttributedValue<Self>>> {
        let credential_name = construct_keychain_credential_name(profile);
        let credential = match Entry::new("snouty", credential_name.as_str()) {
            Ok(cred) => Ok(cred),
            // A NoDefaultStore error indicates that the version of initialize_credential_store() selected by the compiler was a no-op
            Err(keyring_core::Error::NoDefaultStore) => return Ok(None),
            Err(other) => Err(other),
        }?;

        if let Ok(persisted) = credential.get_password() {
            return Ok(Some(AttributedValue::Keychain {
                value: serde_json::from_str::<PersistableCredentials>(&persisted)?
                    .convert_to_credentials(),
                entry_name: credential_name,
            }));
        }

        Ok(None)
    }

    fn try_from_credentials_file(profile: Option<&str>) -> Result<Option<AttributedValue<Self>>> {
        if let Some(snouty_settings_dir) = global_settings_dir() {
            let path = snouty_settings_dir.join(CREDENTIALS_FILENAME);
            return Ok(match read_to_string_if_file_exists(&path)? {
                Some(contents) => Self::try_from_credentials_file_toml(contents, &path, profile)?,
                None => None,
            });
        }

        Ok(None)
    }

    fn try_from_credentials_file_toml(
        contents: String,
        path: &Path,
        profile: Option<&str>,
    ) -> Result<Option<AttributedValue<Self>>> {
        let parsed = parse_credentials_file_toml(contents, path)?;

        if let Some(requested_profile) = profile
            && let Some(credentials_for_profile) = parsed
                .profile
                .as_ref()
                .and_then(|t| t.get(requested_profile))
        {
            return Ok(Some(AttributedValue::SettingsFile {
                value: credentials_for_profile.clone().convert_to_credentials(),
                settings_file_path: path.to_path_buf(),
                profile: Some(requested_profile.to_owned()),
            }));
        }

        if let Some(default_credentials) = parsed.default {
            return Ok(Some(AttributedValue::SettingsFile {
                value: default_credentials.convert_to_credentials(),
                settings_file_path: path.to_path_buf(),
                profile: None,
            }));
        }

        Ok(None)
    }

    fn try_from_github_actions_environment() -> Result<Option<AttributedValue<Self>>> {
        const TARGET_URL_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_URL";
        const REQ_TOKEN_VAR_NAME: &str = "ACTIONS_ID_TOKEN_REQUEST_TOKEN";

        if let Some(actions_id_request_token) = env::var(REQ_TOKEN_VAR_NAME)?
            && let Some(actions_id_url) = env::var(TARGET_URL_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::EnvironmentVariable {
                value: fetch_github_actions_oidc_credentials(
                    &actions_id_url,
                    &actions_id_request_token,
                )?,
                environment_variable_names: vec![TARGET_URL_VAR_NAME, REQ_TOKEN_VAR_NAME],
            }));
        }

        Ok(None)
    }

    pub(crate) fn for_ambient_credentials_with_attribution(
        profile: Option<&str>,
        allow_basic: bool,
        offline: bool,
    ) -> Result<AttributedValue<Self>> {
        if let Some(from_env) = Self::try_from_env()? {
            return to_result(from_env, allow_basic);
        }

        if let Some(from_keychain) = Self::try_from_keychain(profile)? {
            return to_result(from_keychain, allow_basic);
        }

        if let Some(from_credentials_file) = Self::try_from_credentials_file(profile)? {
            return to_result(from_credentials_file, allow_basic);
        }

        if !offline
            && let Some(from_github_actions_environment) =
                Self::try_from_github_actions_environment()?
        {
            return Ok(from_github_actions_environment);
        }

        Err(user_error("No Antithesis credentials found").suggestion(
            "set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one",
        ))
    }

    pub(crate) fn for_ambient_credentials(
        profile: Option<&str>,
        allow_basic: bool,
    ) -> Result<Self> {
        match Self::for_ambient_credentials_with_attribution(profile, allow_basic, false)? {
            AttributedValue::EnvironmentVariable { value, .. } => Ok(value),
            AttributedValue::SettingsFile { value, .. } => Ok(value),
            AttributedValue::Keychain { value, .. } => Ok(value),
        }
    }

    pub(crate) fn auth_header(&self) -> Result<HeaderValue> {
        let value = match self {
            Credentials::Password(PasswordCredentials { username, password }) => {
                let credentials = format!("{username}:{password}");
                let encoded = BASE64_STANDARD.encode(credentials);
                format!("Basic {encoded}")
            }
            Credentials::ApiKey(ApiKeyCredentials { api_key }) => format!("Bearer {api_key}"),
            Credentials::GithubActionsOidc(GithubActionsOidcCredentials { token }) => {
                format!("GHA {token}")
            }
        };
        let mut hv =
            HeaderValue::from_str(&value).wrap_err("failed to build Authorization header")?;
        hv.set_sensitive(true);
        Ok(hv)
    }

    fn convert_to_peristable_credentials(self) -> Result<PersistableCredentials> {
        match self {
            Self::ApiKey(api_key_credentials) => {
                Ok(PersistableCredentials::ApiKey(api_key_credentials))
            }
            Self::Password(password_credentials) => {
                Ok(PersistableCredentials::Password(password_credentials))
            }
            Self::GithubActionsOidc(_) => Err(eyre!(
                "Github Actions OIDC tokens cannot be persisted by Snouty"
            )),
        }
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
fn fetch_github_actions_oidc_credentials(
    actions_id_url: &str,
    actions_id_request_token: &str,
) -> Result<Credentials> {
    #[derive(Deserialize)]
    struct OidcTokenResponse {
        value: String,
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(OIDC_REQUEST_TIMEOUT)
        .build()?;
    let response: OidcTokenResponse = client
        .get(format!("{actions_id_url}&audience=antithesis"))
        .bearer_auth(actions_id_request_token)
        .send()?
        .error_for_status()
        .wrap_err("failed to fetch a GitHub Actions OIDC token")?
        .json()?;

    Ok(Credentials::GithubActionsOidc(
        GithubActionsOidcCredentials {
            token: response.value,
        },
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum PersistableCredentials {
    ApiKey(ApiKeyCredentials),
    Password(PasswordCredentials),
}

impl PersistableCredentials {
    fn convert_to_credentials(self) -> Credentials {
        match self {
            Self::ApiKey(api_key_credentials) => Credentials::ApiKey(api_key_credentials),
            Self::Password(password_credentials) => Credentials::Password(password_credentials),
        }
    }
}

#[cfg(target_os = "macos")]
pub fn initialize_credential_store() -> Result<()> {
    if matches!(
        env::var("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE"),
        Ok(Some(_))
    ) {
        return Ok(());
    }

    use apple_native_keyring_store::keychain::Store;
    set_default_store(Store::new()?);

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn initialize_credential_store() -> Result<()> {
    if matches!(
        env::var("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE"),
        Ok(Some(_))
    ) {
        return Ok(());
    }

    use dbus_secret_service_keyring_store::Store;
    set_default_store(Store::new()?);

    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn initialize_credential_store() -> Result<()> {
    // pass
    Ok(())
}

pub(crate) fn persist(credentials: Credentials, profile: Option<&str>) -> Result<()> {
    let persistable = credentials.convert_to_peristable_credentials()?;
    match try_persist_to_keychain(&persistable, profile) {
        Err(err) => Err(err),
        Ok(Some(())) => Ok(()),
        Ok(None) => persist_to_file(persistable, profile),
    }
}

fn try_persist_to_keychain(
    credentials: &PersistableCredentials,
    profile: Option<&str>,
) -> Result<Option<()>> {
    let credential_name = construct_keychain_credential_name(profile);

    let credential = match Entry::new("snouty", credential_name.as_str()) {
        Ok(cred) => Ok(cred),
        // A NoDefaultStore error indicates that the version of initialize_credential_store() selected by the compiler was a no-op
        Err(keyring_core::Error::NoDefaultStore) => return Ok(None),
        Err(other) => Err(other),
    }?;

    credential.set_password(serde_json::to_string(credentials)?.as_str())?;
    Ok(Some(()))
}

fn construct_keychain_credential_name(profile: Option<&str>) -> String {
    profile
        .map(|p| format!("profile_{p}"))
        .unwrap_or_else(|| "_default_".to_owned())
}

fn persist_to_file(credentials: PersistableCredentials, profile: Option<&str>) -> Result<()> {
    let settings_dir = global_settings_dir().ok_or_eyre(eyre!(
        "Could not determine settings directory. Please ensure $XDG_CONFIG_DIR or $HOME is set"
    ))?;
    let path = settings_dir.join(CREDENTIALS_FILENAME);
    let mut current_contents = match read_to_string_if_file_exists(&path)? {
        Some(contents) => parse_credentials_file_toml(contents, &path)?,
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

    fs::DirBuilder::new()
        .mode(0o700)
        .recursive(true)
        .create(&settings_dir)?;
    let mut temp = NamedTempFile::new_in(&settings_dir)?;
    temp.write_all(toml::to_string_pretty(&current_contents)?.as_bytes())?;

    temp.persist(&path)?;

    Ok(())
}

fn parse_credentials_file_toml(contents: String, path: &Path) -> Result<CredentialsFile> {
    toml::from_str::<CredentialsFile>(&contents).wrap_err(format!(
        "{:?} is not valid TOML or cannot be parsed as a Snouty credentials file.",
        path
    ))
}

fn to_result(
    credentials: AttributedValue<Credentials>,
    allow_basic: bool,
) -> Result<AttributedValue<Credentials>> {
    if !allow_basic && matches!(credentials.unwrap(), Credentials::Password(_)) {
        return Err(user_error(
            "This command does not accept username/password authentication, which is only supported when launching runs (`snouty launch`, `snouty debug`)",
        ));
    }

    Ok(credentials)
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

    #[test]
    fn github_actions_oidc_exchange_sends_bearer_token_and_audience() {
        // The endpoint returns the JWT wrapped in a JSON envelope, exactly as
        // GitHub's Actions OIDC endpoint does.
        let (url, requests) =
            spawn_oidc_token_server("200 OK", r#"{"count":1,"value":"oidc-jwt-token-value"}"#);

        let credentials =
            fetch_github_actions_oidc_credentials(&url, "actions-request-token").unwrap();

        // The JWT is lifted out of the `value` field, not the raw body.
        match credentials {
            Credentials::GithubActionsOidc(GithubActionsOidcCredentials { token }) => {
                assert_eq!(token, "oidc-jwt-token-value");
            }
            other => panic!("expected GithubActionsOidc credentials, got {other:?}"),
        }

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

    #[test]
    fn github_actions_oidc_exchange_errors_on_non_success_status() {
        // A rejected request token (or any non-2xx) must surface as an error
        // rather than letting the error body be mistaken for a token.
        let (url, _requests) =
            spawn_oidc_token_server("403 Forbidden", r#"{"message":"bad credentials"}"#);

        let result = fetch_github_actions_oidc_credentials(&url, "actions-request-token");
        assert!(result.is_err(), "expected an error for a 403 response");
    }

    #[test]
    fn github_actions_oidc_auth_header_uses_gha_scheme() {
        let credentials = Credentials::GithubActionsOidc(GithubActionsOidcCredentials {
            token: "oidc-jwt-token-value".to_owned(),
        });

        let header = credentials.auth_header().unwrap();
        assert_eq!(header.to_str().unwrap(), "GHA oidc-jwt-token-value");
        assert!(header.is_sensitive());
    }

    #[test]
    fn can_read_from_credential_file_defaults() {
        let path = Path::new("./credentials.toml");
        let api_key_credentials = Credentials::try_from_credentials_file_toml(
            "[default]\ntype=\"ApiKey\"\napi_key=\"foo\"".to_owned(),
            path,
            None,
        );

        assert!(matches!(
            api_key_credentials.unwrap().unwrap().unwrap(),
            &Credentials::ApiKey(ApiKeyCredentials { api_key: _ })
        ));

        let password_credentials = Credentials::try_from_credentials_file_toml(
            "[default]\ntype=\"Password\"\nusername=\"user\"\npassword=\"pass\"".to_owned(),
            path,
            None,
        );

        assert!(matches!(
            password_credentials.unwrap().unwrap().unwrap(),
            &Credentials::Password(PasswordCredentials {
                username: _,
                password: _
            })
        ));
    }

    #[test]
    fn can_read_from_credential_file_profile() {
        let path = Path::new("./credentials.toml");
        let api_key_credentials = Credentials::try_from_credentials_file_toml(
            "[profile.foo]\ntype=\"ApiKey\"\napi_key=\"foo\"".to_owned(),
            path,
            Some("foo"),
        );

        assert!(matches!(
            api_key_credentials.unwrap().unwrap().unwrap(),
            &Credentials::ApiKey(ApiKeyCredentials { api_key: _ })
        ));

        let password_credentials = Credentials::try_from_credentials_file_toml(
            "[profile.foo]\ntype=\"Password\"\nusername=\"user\"\npassword=\"pass\"".to_owned(),
            path,
            Some("foo"),
        );

        assert!(matches!(
            password_credentials.unwrap().unwrap().unwrap(),
            &Credentials::Password(PasswordCredentials {
                username: _,
                password: _
            })
        ));
    }

    #[test]
    fn will_fall_back_to_defaults_if_profile_not_found() {
        let path = Path::new("./credentials.toml");
        let api_key_credentials = Credentials::try_from_credentials_file_toml(
            "[default]\ntype=\"ApiKey\"\napi_key=\"foo\"\n\n[profile.foo]\ntype=\"Password\"\nusername=\"user\"\npassword=\"pass\"".to_owned(),
            path,
            Some("bar"),
        );

        assert!(matches!(
            api_key_credentials.unwrap().unwrap().unwrap(),
            &Credentials::ApiKey(ApiKeyCredentials { api_key: _ })
        ));

        let password_credentials = Credentials::try_from_credentials_file_toml(
            "[default]\ntype=\"Password\"\nusername=\"user\"\npassword=\"pass\"\n\n[profile.foo]\ntype=\"ApiKey\"\napi_key=\"foo\"".to_owned(),
            path,
            Some("bar"),
        );

        assert!(matches!(
            password_credentials.unwrap().unwrap().unwrap(),
            &Credentials::Password(PasswordCredentials {
                username: _,
                password: _
            })
        ));
    }
}
