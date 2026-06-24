use std::{collections::HashMap, fs, io::Write, path::Path};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Credentials {
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
            return Ok(Some(AttributedValue::FromEnvironmentVariable {
                value: Self::for_api_key(api_key),
                environment_variable_name: API_KEY_VAR_NAME,
            }));
        }

        if let Some(username) = env::var(USERNAME_VAR_NAME)?
            && let Some(password) = env::var(PASSWORD_VAR_NAME)?
        {
            return Ok(Some(AttributedValue::FromEnvironmentVariable {
                value: Self::for_password(username, password),
                environment_variable_name: PASSWORD_VAR_NAME,
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
            return Ok(Some(AttributedValue::FromKeychain {
                value: serde_json::from_str::<Credentials>(&persisted)?,
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
            return Ok(Some(AttributedValue::FromSettingsFile {
                value: credentials_for_profile.clone(),
                settings_file_path: path.to_path_buf(),
                profile: Some(requested_profile.to_owned()),
            }));
        }

        if let Some(default_credentials) = parsed.default {
            return Ok(Some(AttributedValue::FromSettingsFile {
                value: default_credentials.clone(),
                settings_file_path: path.to_path_buf(),
                profile: None,
            }));
        }

        Ok(None)
    }

    pub(crate) fn for_ambient_credentials_with_attribution(
        profile: Option<&str>,
        allow_basic: bool,
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

        Err(user_error("No Antithesis credentials found").suggestion(
            "set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one",
        ))
    }

    pub(crate) fn for_ambient_credentials(
        profile: Option<&str>,
        allow_basic: bool,
    ) -> Result<Self> {
        match Self::for_ambient_credentials_with_attribution(profile, allow_basic)? {
            AttributedValue::FromEnvironmentVariable { value, .. } => Ok(value),
            AttributedValue::FromSettingsFile { value, .. } => Ok(value),
            AttributedValue::FromKeychain { value, .. } => Ok(value),
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
        };
        let mut hv =
            HeaderValue::from_str(&value).wrap_err("failed to build Authorization header")?;
        hv.set_sensitive(true);
        Ok(hv)
    }
}

#[cfg(target_os = "macos")]
pub fn initialize_credential_store() -> Result<()> {
    use apple_native_keyring_store::keychain::Store;
    set_default_store(Store::new()?);

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn initialize_credential_store() -> Result<()> {
    if matches!(
        env::var("SNOUTY_DISABLE_DBUS_CREDENTIAL_STORAGE"),
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
    match try_persist_to_keychain(&credentials, profile) {
        Err(err) => Err(err),
        Ok(Some(())) => Ok(()),
        Ok(None) => persist_to_file(credentials, profile),
    }
}

fn try_persist_to_keychain(credentials: &Credentials, profile: Option<&str>) -> Result<Option<()>> {
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

fn persist_to_file(credentials: Credentials, profile: Option<&str>) -> Result<()> {
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
    default: Option<Credentials>,
    profile: Option<HashMap<String, Credentials>>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
