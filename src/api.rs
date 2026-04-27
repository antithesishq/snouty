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

pub use generated::types::{
    BuildLogLine, Event, LaunchResponse, Moment, Property, PropertyStatus, RunDetail, RunStatus,
    RunSummary,
};
pub use progenitor_client::ByteStream;

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
            Err(format_api_error(status.as_u16(), &body))
        }
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunDetail> {
        match self
            .client
            .get_run()
            .version(API_VERSION)
            .run_id(run_id)
            .send()
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub async fn get_run_build_logs(&self, run_id: &str) -> Result<ByteStream> {
        match self
            .client
            .get_run_build_logs()
            .version(API_VERSION)
            .run_id(run_id)
            .send()
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub async fn get_run_logs(
        &self,
        run_id: &str,
        input_hash: &str,
        vtime: &str,
        begin_input_hash: Option<&str>,
        begin_vtime: Option<&str>,
    ) -> Result<ByteStream> {
        let mut request = self
            .client
            .get_run_logs()
            .version(API_VERSION)
            .run_id(run_id)
            .input_hash(input_hash)
            .vtime(vtime);
        if let Some(v) = begin_input_hash {
            request = request.begin_input_hash(v);
        }
        if let Some(v) = begin_vtime {
            request = request.begin_vtime(v);
        }

        match request.send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub fn stream_run_properties(
        &self,
        run_id: &str,
        status: Option<PropertyStatus>,
    ) -> impl futures_util::Stream<Item = Result<Property>> + '_ {
        stream::try_unfold(
            RunPropertyStreamState {
                api: self,
                run_id: run_id.to_string(),
                status,
                after: None,
                buffered_properties: VecDeque::new(),
                finished: false,
            },
            |mut state| async move {
                loop {
                    if let Some(property) = state.buffered_properties.pop_front() {
                        return Ok(Some((property, state)));
                    }

                    if state.finished {
                        return Ok(None);
                    }

                    let page = state
                        .api
                        .fetch_run_properties_page(
                            &state.run_id,
                            state.after.as_deref(),
                            state.status,
                        )
                        .await?;
                    let generated::types::PropertyListResponse { data, next_cursor } = page;
                    state.buffered_properties = data.into();
                    state.finished = next_cursor.is_none();
                    state.after = next_cursor;
                }
            },
        )
    }

    pub async fn search_run_events(&self, run_id: &str, query: &str) -> Result<ByteStream> {
        match self
            .client
            .search_run_events()
            .version(API_VERSION)
            .run_id(run_id)
            .q(query)
            .send()
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub fn stream_runs_filtered(
        &self,
        opts: &RunsFilterOptions,
    ) -> impl futures_util::Stream<Item = Result<RunSummary>> + '_ {
        let opts = opts.clone();
        stream::try_unfold(
            FilteredRunStreamState {
                api: self,
                opts,
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

                    let page = state
                        .api
                        .fetch_runs_page_filtered(state.after.as_deref(), &state.opts)
                        .await?;
                    let generated::types::RunListResponse { data, next_cursor } = page;
                    state.buffered_runs = data.into();
                    state.finished = next_cursor.is_none();
                    state.after = next_cursor;
                }
            },
        )
    }

    async fn fetch_runs_page(
        &self,
        after: Option<&str>,
    ) -> Result<generated::types::RunListResponse> {
        self.fetch_runs_page_filtered(after, &RunsFilterOptions::default())
            .await
    }

    async fn fetch_runs_page_filtered(
        &self,
        after: Option<&str>,
        opts: &RunsFilterOptions,
    ) -> Result<generated::types::RunListResponse> {
        let mut request = self.client.list_runs().version(API_VERSION).limit(100_u64);
        if let Some(cursor) = after {
            request = request.after(cursor);
        }
        if let Some(ref status) = opts.status {
            request = request.status(*status);
        }
        if let Some(ref launcher) = opts.launcher {
            request = request.launcher(launcher.clone());
        }
        if let Some(ref ts) = opts.created_after {
            request = request.created_after(*ts);
        }
        if let Some(ref ts) = opts.created_before {
            request = request.created_before(*ts);
        }

        match request.send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    async fn fetch_run_properties_page(
        &self,
        run_id: &str,
        after: Option<&str>,
        status: Option<PropertyStatus>,
    ) -> Result<generated::types::PropertyListResponse> {
        let mut request = self
            .client
            .list_run_properties()
            .version(API_VERSION)
            .run_id(run_id)
            .limit(100_u64);
        if let Some(status) = status {
            request = request.status(status);
        }
        if let Some(cursor) = after {
            request = request.after(cursor);
        }

        match request.send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }
}

#[derive(Clone, Default)]
pub struct RunsFilterOptions {
    pub status: Option<generated::types::RunStatus>,
    pub launcher: Option<String>,
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
}

struct RunStreamState<'a> {
    api: &'a AntithesisApi,
    after: Option<String>,
    buffered_runs: VecDeque<RunSummary>,
    finished: bool,
}

struct FilteredRunStreamState<'a> {
    api: &'a AntithesisApi,
    opts: RunsFilterOptions,
    after: Option<String>,
    buffered_runs: VecDeque<RunSummary>,
    finished: bool,
}

struct RunPropertyStreamState<'a> {
    api: &'a AntithesisApi,
    run_id: String,
    status: Option<PropertyStatus>,
    after: Option<String>,
    buffered_properties: VecDeque<Property>,
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
    headers.insert(reqwest::header::AUTHORIZATION, auth_header(&config.auth)?);
    // HACK: the API rejects GET requests without a Content-Type header. Remove
    // this once the server is fixed.
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
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

fn format_api_error(status: u16, body: &str) -> Report {
    let reason = reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("");
    let body = body.trim();

    let mut msg = format!("API error: {status}");
    if !reason.is_empty() {
        msg.push(' ');
        msg.push_str(reason);
    }
    if !body.is_empty() {
        msg.push_str(" — ");
        msg.push_str(body);
    }
    if matches!(status, 401 | 403) {
        msg.push_str(
            "\n\nCheck that ANTITHESIS_API_KEY (or ANTITHESIS_USERNAME/ANTITHESIS_PASSWORD) \
             is set correctly and has access to this tenant.",
        );
    }
    eyre!("{msg}")
}

fn format_payload_snippet(body: &str, line: usize, column: usize) -> String {
    const WINDOW: usize = 60;

    let offset = char_pos_to_byte_offset(body, line, column);
    let start_target = offset.saturating_sub(WINDOW);
    let end_target = offset.saturating_add(WINDOW).min(body.len());
    let start = (0..=start_target)
        .rev()
        .find(|&i| body.is_char_boundary(i))
        .unwrap_or(0);
    let end = (end_target..=body.len())
        .find(|&i| body.is_char_boundary(i))
        .unwrap_or(body.len());

    let prefix = if start > 0 { "..." } else { "" };
    let suffix = if end < body.len() { "..." } else { "" };

    let snippet: String = body[start..end]
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let caret_col = prefix.chars().count() + body[start..offset].chars().count();
    let caret = format!("{:width$}^", "", width = caret_col);

    format!("  {prefix}{snippet}{suffix}\n  {caret}")
}

fn char_pos_to_byte_offset(body: &str, line: usize, column: usize) -> usize {
    let mut cur_line = 1;
    let mut cur_col = 1;
    for (i, c) in body.char_indices() {
        if cur_line == line && cur_col == column {
            return i;
        }
        if c == '\n' {
            cur_line += 1;
            cur_col = 1;
        } else {
            cur_col += 1;
        }
    }
    body.len()
}

async fn format_api_client_error(err: ClientError<generated::types::ErrorResponse>) -> Report {
    match err {
        ClientError::ErrorResponse(response) => {
            let status = response.status().as_u16();
            let body = response.into_inner();
            format_api_error(status, &body.message)
        }
        ClientError::UnexpectedResponse(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            format_api_error(status, &body)
        }
        ClientError::InvalidRequest(message) => eyre!("invalid API request: {message}"),
        ClientError::CommunicationError(err) => eyre!(err).wrap_err("failed to contact API"),
        ClientError::InvalidUpgrade(err) => eyre!(err).wrap_err("invalid API upgrade response"),
        ClientError::ResponseBodyError(err) => {
            eyre!(err).wrap_err("failed to read API response body")
        }
        ClientError::InvalidResponsePayload(body, err) => {
            let body = String::from_utf8_lossy(&body);
            let snippet = format_payload_snippet(&body, err.line(), err.column());
            eyre!("invalid API response payload: {err}\n{snippet}")
        }
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

    #[tokio::test]
    async fn stream_run_properties_follows_next_cursor() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs/run-1/properties"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("status"))
            .and(query_param_is_missing("after"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {
                        "name": "Counter value stays below limit",
                        "status": "Failing",
                        "is_event": true,
                        "is_existential": false,
                        "is_universal": true
                    }
                ],
                "next_cursor": "props-cursor-1"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs/run-1/properties"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("status"))
            .and(query_param("after", "props-cursor-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {
                        "name": "Setup completes",
                        "status": "Passing",
                        "is_event": false,
                        "is_existential": true,
                        "is_universal": false
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

        let properties = api
            .stream_run_properties("run-1", None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let names = properties
            .into_iter()
            .map(|property| property.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "Counter value stays below limit".to_string(),
                "Setup completes".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn stream_run_properties_forwards_status_filter() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs/run-1/properties"))
            .and(query_param("limit", "100"))
            .and(query_param("status", "Failing"))
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

        let properties = api
            .stream_run_properties("run-1", Some(PropertyStatus::Failing))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(properties.is_empty());
    }

    #[tokio::test]
    async fn search_run_events_passes_query_through() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v1/runs/run-1/events"))
            .and(query_param("q", "slow request"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"output_text":"{\"level\":\"warn\",\"msg\":\"slow request\"}","moment":{"input_hash":"-456","vtime":"2.0"}}"#,
            ))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, mock_server.uri()).unwrap();

        let mut stream = api
            .search_run_events("run-1", "slow request")
            .await
            .unwrap()
            .into_inner();
        let mut body = Vec::new();
        while let Some(chunk) = futures_util::StreamExt::next(&mut stream).await {
            body.extend_from_slice(&chunk.unwrap());
        }
        let body = String::from_utf8(body).unwrap();

        assert!(body.contains("slow request"));
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
