use base64::{Engine, prelude::BASE64_STANDARD};
use color_eyre::{
    Section,
    eyre::{Context, Result},
};
use http::HeaderValue;
use serde::{Deserialize, Serialize};

use crate::{env, error::user_error, settings::Settings};

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct PasswordCredentials {
    username: String,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Credentials {
    ApiKey(ApiKeyCredentials),
    Password(PasswordCredentials),
}

impl Credentials {
    fn for_api_key(api_key: String) -> Self {
        Self::ApiKey(ApiKeyCredentials { api_key })
    }

    pub(crate) fn for_password(username: String, password: String) -> Self {
        Self::Password(PasswordCredentials { username, password })
    }

    fn try_from_env() -> Result<Option<Self>> {
        if let Some(api_key) = env::var("ANTITHESIS_API_KEY")? {
            return Ok(Some(Self::for_api_key(api_key)));
        }

        if let Some(username) = env::var("ANTITHESIS_USERNAME")?
            && let Some(password) = env::var("ANTITHESIS_PASSWORD")?
        {
            return Ok(Some(Self::for_password(username, password)));
        }

        Ok(None)
    }

    pub(crate) fn for_ambient_credentials(_settings: &Settings, allow_basic: bool) -> Result<Self> {
        if let Some(from_env) = Self::try_from_env()? {
            if !allow_basic && matches!(from_env, Self::Password(_)) {
                return Err(user_error(
                    "This command does not accept username/password authentication, which is only supported when launching runs (`snouty launch`, `snouty debug`)",
                ));
            }

            return Ok(from_env);
        }

        Err(user_error("No Antithesis credentials found").suggestion(
            "set ANTITHESIS_API_KEY; ask Antithesis support for an API key if you don't have one",
        ))
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
