use std::{collections::HashMap, fs, io::ErrorKind, path::Path};

use base64::{Engine, prelude::BASE64_STANDARD};
use color_eyre::{
    Section,
    eyre::{Context, Result, eyre},
};
use http::HeaderValue;
use serde::{Deserialize, Serialize};

use crate::{
    attributed_value::AttributedValue,
    env,
    error::user_error,
    settings::{Settings, global_settings_dir},
};

pub(crate) const API_KEY_VAR_NAME: &str = "ANTITHESIS_API_KEY";
pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";
const CREDENTIALS_FILENAME: &str = "credentials.toml";

#[derive(Clone, Serialize, Deserialize)]
pub struct ApiKeyCredentials {
    api_key: String,
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
    password: String,
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

    fn try_from_credentials_file(settings: &Settings) -> Result<Option<AttributedValue<Self>>> {
        if let Some(snouty_settings_dir) = global_settings_dir() {
            let path = snouty_settings_dir.join(CREDENTIALS_FILENAME);
            return match fs::read_to_string(&path) {
                Ok(contents) => Ok(Self::try_from_credentials_file_toml(
                    contents,
                    &path,
                    settings.profile(),
                )?),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
                Err(err) => Err(eyre!("File at {:?} could not be read: {err:#}", &path)),
            };
        }

        Ok(None)
    }

    fn try_from_credentials_file_toml(
        contents: String,
        path: &Path,
        profile: Option<&str>,
    ) -> Result<Option<AttributedValue<Self>>> {
        let parsed = toml::from_str::<CredentialsFile>(&contents).wrap_err(format!(
            "{:?} is not valid TOML or cannot be parsed as a Snouty credentials file.",
            path
        ))?;

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
        settings: &Settings,
        allow_basic: bool,
    ) -> Result<AttributedValue<Self>> {
        if let Some(from_env) = Self::try_from_env()? {
            return Ok(if allow_basic {
                from_env
            } else {
                ensure_non_password_credentials(from_env)?
            });
        }

        if let Some(from_credentials_file) = Self::try_from_credentials_file(settings)? {
            return Ok(if allow_basic {
                from_credentials_file
            } else {
                ensure_non_password_credentials(from_credentials_file)?
            });
        }

        Err(user_error("No Antithesis credentials found").suggestion(
            "set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one",
        ))
    }

    pub(crate) fn for_ambient_credentials(settings: &Settings, allow_basic: bool) -> Result<Self> {
        match Self::for_ambient_credentials_with_attribution(settings, allow_basic)? {
            AttributedValue::FromEnvironmentVariable {
                value,
                environment_variable_name: _,
            } => Ok(value),
            AttributedValue::FromSettingsFile {
                value,
                settings_file_path: _,
                profile: _,
            } => Ok(value),
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

fn ensure_non_password_credentials(
    credentials: AttributedValue<Credentials>,
) -> Result<AttributedValue<Credentials>> {
    if matches!(credentials.unwrap(), Credentials::Password(_)) {
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
