use std::collections::{HashMap, VecDeque};
use std::env;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use color_eyre::eyre::{Context, Report, Result, eyre};
use futures_util::stream;
use log::debug;
use progenitor_client::Error as ClientError;
use reqwest::Client;

use crate::params::Params;

#[allow(dead_code, unused_imports)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/antithesis_api.rs"));
}

pub use generated::types::{LaunchResponse, RunSummary};

const API_VERSION: &str = "v1";
const CLIENT_TIMEOUT_SECS: u64 = 30;

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
    client: generated::Client,
    http_client: Client,
    base_url: String,
}

impl AntithesisApi {
    pub fn new(config: Config) -> Result<Self> {
        let base_url = format!("https://{}.antithesis.com", config.tenant);
        debug!("using default base URL: {}", base_url);
        Self::with_base_url(config, base_url)
    }

    pub fn from_env() -> Result<Self> {
        let config = Config::from_env()?;
        if let Ok(base_url) = env::var("ANTITHESIS_BASE_URL") {
            debug!("using ANTITHESIS_BASE_URL override: {}", base_url);
            Self::with_base_url(config, base_url)
        } else {
            Self::new(config)
        }
    }

    pub fn with_base_url(config: Config, base_url: impl Into<String>) -> Result<Self> {
        let base_url = normalize_base_url(base_url);
        debug!("initializing API client for {}", base_url);

        let http_client = build_http_client(&config)?;
        let client = generated::Client::new_with_client(&base_url, http_client.clone());

        Ok(Self {
            client,
            http_client,
            base_url,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn launch_test(&self, launcher: &str, params: &Params) -> Result<LaunchResponse> {
        let body = launch_request(params)?;
        match self
            .client
            .launch_test()
            .version(API_VERSION)
            .launcher_name(launcher)
            .body(body)
            .send()
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub fn stream_runs(&self) -> impl futures_util::Stream<Item = Result<RunSummary>> + '_ {
        stream::try_unfold(
            RunStreamState {
                api: self,
                after: None,
                buffered_runs: VecDeque::new(),
                finished: false,
            },
            |mut state| async move {
                loop {
                    if let Some(run) = state.buffered_runs.pop_front() {
                        return Ok(Some((run, state)));
                    }

                    if state.finished {
                        return Ok(None);
                    }

                    let page = state.api.fetch_runs_page(state.after.as_deref()).await?;
                    let generated::types::RunListResponse { data, next_cursor } = page;
                    state.buffered_runs = data.into();
                    state.finished = next_cursor.is_none();
                    state.after = next_cursor;
                }
            },
        )
    }

    pub async fn launch_debugging(&self, params: &Params) -> Result<String> {
        let url = format!("{}/api/{}/launch/debugging", self.base_url, API_VERSION);
        debug!("POST {}", url);

        let response = self
            .http_client
            .post(url)
            .json(&serde_json::json!({ "params": params.to_value() }))
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        debug!("response status: {}, body length: {}", status, body.len());

        if status.is_success() {
            Ok(body)
        } else {
            Err(eyre!("API error: {} - {}", status.as_u16(), body))
        }
    }

    async fn fetch_runs_page(
        &self,
        after: Option<&str>,
    ) -> Result<generated::types::RunListResponse> {
        let mut request = self.client.list_runs().version(API_VERSION).limit(100_u64);
        if let Some(cursor) = after {
            request = request.after(cursor);
        }

        match request.send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }
}

struct RunStreamState<'a> {
    api: &'a AntithesisApi,
    after: Option<String>,
    buffered_runs: VecDeque<RunSummary>,
    finished: bool,
}

fn normalize_base_url(base_url: impl Into<String>) -> String {
    let base_url = base_url.into();
    let trimmed = base_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/v1")
        .unwrap_or(trimmed)
        .to_string()
}

fn build_http_client(config: &Config) -> Result<Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        auth_header(&config.auth)?,
    );

    Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(CLIENT_TIMEOUT_SECS))
        .build()
        .wrap_err("failed to build API client")
}

fn auth_header(auth: &Auth) -> Result<reqwest::header::HeaderValue> {
    let value = match auth {
        Auth::Basic { username, password } => {
            let credentials = format!("{username}:{password}");
            let encoded = BASE64_STANDARD.encode(credentials);
            format!("Basic {encoded}")
        }
        Auth::Bearer { api_key } => format!("Bearer {api_key}"),
    };
    reqwest::header::HeaderValue::from_str(&value).wrap_err("failed to build Authorization header")
}

fn launch_request(params: &Params) -> Result<generated::types::LaunchRequest> {
    let mut builder = generated::types::builder::Params::default();
    let mut extra = HashMap::new();

    for (key, value) in params.as_map() {
        let value = value
            .as_str()
            .ok_or_else(|| eyre!("launch params must be strings: {key}"))?;

        builder = match key.as_str() {
            "antithesis.config_image" => builder.antithesis_config_image(Some(value.to_string())),
            "antithesis.description" => builder.antithesis_description(Some(value.to_string())),
            "antithesis.duration" => builder.antithesis_duration(Some(value.to_string())),
            "antithesis.images" => builder.antithesis_images(Some(value.to_string())),
            "antithesis.is_ephemeral" => builder.antithesis_is_ephemeral(Some(
                generated::types::ParamsAntithesisIsEphemeral::try_from(value)
                    .wrap_err("invalid antithesis.is_ephemeral value")?,
            )),
            "antithesis.report.recipients" => {
                builder.antithesis_report_recipients(Some(value.to_string()))
            }
            "antithesis.source" => builder.antithesis_source(Some(value.to_string())),
            _ => {
                extra.insert(key.clone(), value.to_string());
                builder
            }
        };
    }

    let typed_params = generated::types::Params::try_from(builder.extra(extra))
        .wrap_err("failed to build params")?;
    generated::types::LaunchRequest::try_from(
        generated::types::builder::LaunchRequest::default().params(typed_params),
    )
    .wrap_err("failed to build launch request")
}

async fn format_api_client_error(err: ClientError<generated::types::ErrorResponse>) -> Report {
    match err {
        ClientError::ErrorResponse(response) => {
            let status = response.status().as_u16();
            let body = response.into_inner();
            let body =
                serde_json::to_string(&body).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
            eyre!("API error: {} - {}", status, body)
        }
        ClientError::UnexpectedResponse(response) => {
            let status = response.status().as_u16();
            let body = response
                .text()
                .await
                .unwrap_or_else(|e| format!("failed to read response body: {e}"));
            eyre!("API error: {} - {}", status, body)
        }
        ClientError::InvalidRequest(message) => eyre!("invalid API request: {message}"),
        ClientError::CommunicationError(err) => eyre!(err).wrap_err("failed to contact API"),
        ClientError::InvalidUpgrade(err) => eyre!(err).wrap_err("invalid API upgrade response"),
        ClientError::ResponseBodyError(err) => {
            eyre!(err).wrap_err("failed to read API response body")
        }
        ClientError::InvalidResponsePayload(body, err) => eyre!(
            "invalid API response payload: {err}; body: {}",
            String::from_utf8_lossy(&body)
        ),
        ClientError::Custom(message) => eyre!(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
    fn with_base_url_strips_legacy_api_suffix() {
        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, "http://example.com/api/v1/").unwrap();
        assert_eq!(api.base_url(), "http://example.com");
    }

    #[tokio::test]
    async fn launch_test_uses_basic_auth() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v1/launch/basic_test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "run_id": "run-123"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, mock_server.uri()).unwrap();
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

        let response = api.launch_test("basic_test", &params).await.unwrap();
        let requests = mock_server.received_requests().await.unwrap();

        assert_eq!(response.run_id, "run-123");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/api/v1/launch/basic_test");
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(
            requests[0]
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Basic dXNlcjpwYXNz"
        );
        assert_eq!(
            requests[0].body_json::<serde_json::Value>().unwrap(),
            serde_json::json!({
                "params": {
                    "antithesis.duration": "30"
                }
            })
        );
    }

    #[tokio::test]
    async fn stream_runs_follows_next_cursor() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("after"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {
                        "run_id": "run-1",
                        "status": "completed",
                        "created_at": "2025-03-20T02:00:00Z",
                        "launcher": "nightly"
                    }
                ],
                "next_cursor": "cursor-1"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs"))
            .and(query_param("limit", "100"))
            .and(query_param("after", "cursor-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {
                        "run_id": "run-2",
                        "status": "in_progress",
                        "created_at": "2025-03-19T02:00:00Z",
                        "launcher": "debug"
                    }
                ],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, mock_server.uri()).unwrap();

        let runs = api.stream_runs().try_collect::<Vec<_>>().await.unwrap();

        let run_ids = runs.into_iter().map(|run| run.run_id).collect::<Vec<_>>();
        assert_eq!(run_ids, vec!["run-1", "run-2"]);
    }

    #[tokio::test]
    async fn stream_runs_returns_empty_when_no_runs_exist() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("after"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, mock_server.uri()).unwrap();

        let runs = api.stream_runs().try_collect::<Vec<_>>().await.unwrap();

        assert!(runs.is_empty());
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
