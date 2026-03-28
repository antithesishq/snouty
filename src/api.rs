use std::env;

use log::debug;
use reqwest::{Client, RequestBuilder};

use color_eyre::eyre::{Result, eyre};

fn required_env(name: &'static str) -> Result<String> {
    env::var(name).map_err(|e| match e {
        env::VarError::NotPresent => eyre!("missing environment variable: {name}"),
        _ => eyre!(e).wrap_err(format!("invalid environment variable {name}")),
    })
}

fn optional_env(name: &'static str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(eyre!(e).wrap_err(format!("invalid environment variable {name}"))),
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    Basic { username: String, password: String },
    Bearer { api_key: String },
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
            Self::Bearer { .. } => f
                .debug_struct("Bearer")
                .field("api_key", &"[REDACTED]")
                .finish(),
        }
    }
}

impl Auth {
    pub fn basic(username: String, password: String) -> Self {
        Self::Basic { username, password }
    }

    pub fn bearer(api_key: String) -> Self {
        Self::Bearer { api_key }
    }

    fn from_env() -> Result<Self> {
        if let Some(api_key) = optional_env("ANTITHESIS_API_KEY")? {
            return Ok(Self::bearer(api_key));
        }
        Ok(Self::basic(
            required_env("ANTITHESIS_USERNAME")?,
            required_env("ANTITHESIS_PASSWORD")?,
        ))
    }

    fn authenticate(&self, request: RequestBuilder) -> RequestBuilder {
        match self {
            Self::Basic { username, password } => request.basic_auth(username, Some(password)),
            Self::Bearer { api_key } => request.bearer_auth(api_key),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub auth: Auth,
    pub tenant: String,
}

impl Config {
    pub fn new(auth: Auth, tenant: String) -> Self {
        Self { auth, tenant }
    }

    pub fn from_env() -> Result<Self> {
        debug!("loading config from environment");
        Ok(Self {
            auth: Auth::from_env()?,
            tenant: required_env("ANTITHESIS_TENANT")?,
        })
    }
}

pub struct AntithesisApi {
    client: Client,
    base_url: String,
    auth: Auth,
}

impl AntithesisApi {
    pub fn new(config: Config) -> Result<Self> {
        let base_url = format!("https://{}.antithesis.com/api/v1", config.tenant);
        debug!("using default base URL: {}", base_url);
        Self::with_base_url(config, base_url)
    }

    pub fn from_env() -> Result<Self> {
        let config = Config::from_env()?;
        // Allow base URL override for testing
        if let Ok(base_url) = env::var("ANTITHESIS_BASE_URL") {
            debug!("using ANTITHESIS_BASE_URL override: {}", base_url);
            Self::with_base_url(config, base_url)
        } else {
            Self::new(config)
        }
    }

    pub fn with_base_url(config: Config, base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        debug!("initializing API client for {}", base_url);
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            base_url,
            auth: config.auth,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn get(&self, path: &str) -> RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        debug!("GET {}", url);
        self.auth.authenticate(self.client.get(url))
    }

    pub fn post(&self, path: &str) -> RequestBuilder {
        let url = format!("{}{}", self.base_url, path);
        debug!("POST {}", url);
        self.auth.authenticate(self.client.post(url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::AUTHORIZATION;

    #[test]
    fn api_uses_basic_auth() {
        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, "http://example.com").unwrap();
        let request = api.get("/test").build().unwrap();

        assert_eq!(request.headers()[AUTHORIZATION], "Basic dXNlcjpwYXNz");
    }

    #[test]
    fn api_uses_bearer_auth() {
        let config = Config::new(Auth::bearer("api-key".to_string()), "tenant".to_string());
        let api = AntithesisApi::with_base_url(config, "http://example.com").unwrap();
        let request = api.post("/test").build().unwrap();

        assert_eq!(request.headers()[AUTHORIZATION], "Bearer api-key");
    }

    #[test]
    fn with_base_url_trims_trailing_slash() {
        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, "http://example.com/").unwrap();
        assert_eq!(api.base_url(), "http://example.com");
    }

    #[test]
    fn auth_debug_redacts_password() {
        let auth = Auth::basic("user".to_string(), "secret".to_string());
        let debug = format!("{:?}", auth);
        assert!(debug.contains("user"));
        assert!(!debug.contains("secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn auth_debug_redacts_api_key() {
        let auth = Auth::bearer("secret-key".to_string());
        let debug = format!("{:?}", auth);
        assert!(!debug.contains("secret-key"));
        assert!(debug.contains("[REDACTED]"));
    }
}
