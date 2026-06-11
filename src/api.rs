use std::collections::{HashMap, VecDeque};
use std::env;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use color_eyre::Section;
use color_eyre::eyre::{Context, Report, Result, eyre};
use futures_util::stream;
use log::debug;
use progenitor_client::{ClientHooks, ClientInfo, Error as ClientError, OperationInfo};
use reqwest::Client;
use reqwest_middleware::ClientWithMiddleware;

use crate::api_cache;
use crate::params::Params;

#[allow(dead_code, unused_imports, private_interfaces)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/antithesis_api.rs"));
}

pub use generated::types::{
    BuildLogLine, Event, EventProperty, LaunchResponse, Moment, NonEventProperty, Property,
    PropertyStatus, RunDetail, RunStatus, RunSummary,
};
pub use progenitor_client::ByteStream;

impl Property {
    pub fn name(&self) -> &str {
        match self {
            Self::EventProperty(p) => &p.name,
            Self::NonEventProperty(p) => &p.name,
        }
    }

    pub fn status(&self) -> PropertyStatus {
        match self {
            Self::EventProperty(p) => p.status,
            Self::NonEventProperty(p) => p.status,
        }
    }

    /// Sampled example events for an event-based property. Returns an empty
    /// slice for non-event properties, whose examples are arbitrary values
    /// without a `moment`.
    pub fn event_examples(&self) -> &[Event] {
        match self {
            Self::EventProperty(p) => &p.examples,
            Self::NonEventProperty(_) => &[],
        }
    }

    /// Sampled counterexample events for an event-based property. Returns an
    /// empty slice for non-event properties.
    pub fn event_counterexamples(&self) -> &[Event] {
        match self {
            Self::EventProperty(p) => &p.counterexamples,
            Self::NonEventProperty(_) => &[],
        }
    }
}

/// `Property` is an untagged `oneOf` whose variants are structurally similar:
/// a `NonEventProperty` whose examples happen to fit `Event`'s shape (or that
/// has no examples at all) silently deserializes as `EventProperty`. Coerce
/// each property into the variant indicated by its `is_event` flag.
fn normalize_property(property: Property) -> Result<Property> {
    match property {
        Property::EventProperty(p) if !p.is_event => {
            let counterexamples = p
                .counterexamples
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .wrap_err("re-serializing property counterexamples")?;
            let examples = p
                .examples
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .wrap_err("re-serializing property examples")?;
            Ok(Property::NonEventProperty(NonEventProperty {
                counterexample_count: p.counterexample_count,
                counterexamples,
                description: p.description,
                example_count: p.example_count,
                examples,
                group: p.group,
                is_event: p.is_event,
                is_group: p.is_group,
                name: p.name,
                status: p.status,
            }))
        }
        other => Ok(other),
    }
}

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

    /// Like [`Auth::from_env`], but only accepts an API key. The API rejects
    /// basic auth everywhere except the launch endpoints, so commands that hit
    /// other endpoints should fail fast with a clear message instead of
    /// sending a request destined for a 403.
    fn api_key_from_env() -> Result<Self> {
        if let Some(api_key) = optional_env("ANTITHESIS_API_KEY")? {
            return Ok(Self::bearer(api_key));
        }
        let has_basic = optional_env("ANTITHESIS_USERNAME")?.is_some()
            || optional_env("ANTITHESIS_PASSWORD")?.is_some();
        let mut err = eyre!("missing environment variable: ANTITHESIS_API_KEY");
        if has_basic {
            err = err.note(
                "ANTITHESIS_USERNAME/ANTITHESIS_PASSWORD are set, but this command only \
                 accepts API key authentication; username/password authentication is only \
                 supported when launching runs (`snouty launch`, `snouty debug`, \
                 `snouty api webhook`)",
            );
        }
        Err(err.suggestion(
            "set ANTITHESIS_API_KEY; ask your Antithesis contact for an API key if you \
             don't have one",
        ))
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub auth: Auth,
    pub tenant: String,
    pub verbose: bool,
}

impl Config {
    pub fn new(auth: Auth, tenant: String) -> Self {
        Self {
            auth,
            tenant,
            verbose: false,
        }
    }

    pub fn from_env() -> Result<Self> {
        debug!("loading config from environment");
        Ok(Self {
            auth: Auth::from_env()?,
            tenant: required_env("ANTITHESIS_TENANT")?,
            verbose: false,
        })
    }

    /// Like [`Config::from_env`], but only accepts API key authentication.
    pub fn from_env_requiring_api_key() -> Result<Self> {
        debug!("loading config from environment (API key required)");
        Ok(Self {
            auth: Auth::api_key_from_env()?,
            tenant: required_env("ANTITHESIS_TENANT")?,
            verbose: false,
        })
    }
}

pub struct AntithesisApi {
    client: generated::Client,
    base_url: String,
}

impl AntithesisApi {
    pub fn new(config: Config) -> Result<Self> {
        let base_url = format!("https://{}.antithesis.com", config.tenant);
        debug!("using default base URL: {}", base_url);
        Self::with_base_url(config, base_url)
    }

    pub fn from_env(verbose: bool) -> Result<Self> {
        Self::from_config(Config::from_env()?, verbose)
    }

    /// Like [`AntithesisApi::from_env`], but fails fast unless an API key is
    /// configured. Every endpoint other than launch requires one.
    pub fn from_env_requiring_api_key(verbose: bool) -> Result<Self> {
        Self::from_config(Config::from_env_requiring_api_key()?, verbose)
    }

    fn from_config(mut config: Config, verbose: bool) -> Result<Self> {
        config.verbose = verbose;
        if let Ok(base_url) = env::var("ANTITHESIS_BASE_URL") {
            debug!("using ANTITHESIS_BASE_URL override: {}", base_url);
            Self::with_base_url(config, base_url)
        } else {
            Self::new(config)
        }
    }

    pub fn with_base_url(config: Config, base_url: impl Into<String>) -> Result<Self> {
        Self::build(config, base_url, api_cache::build_cached_client)
    }

    #[cfg(test)]
    fn with_base_url_and_cache_dir(
        config: Config,
        base_url: impl Into<String>,
        cache_dir: std::path::PathBuf,
    ) -> Result<Self> {
        Self::build(config, base_url, move |client| {
            Some(api_cache::build_cached_client_at(client, cache_dir))
        })
    }

    fn build(
        config: Config,
        base_url: impl Into<String>,
        build_cache: impl FnOnce(Client) -> Option<ClientWithMiddleware>,
    ) -> Result<Self> {
        let base_url = normalize_base_url(base_url);
        debug!("initializing API client for {}", base_url);

        let default_headers = default_request_headers(&config)?;
        let http_client = build_http_client(default_headers.clone())?;
        let cached = build_cache(http_client.clone());
        let state = ClientState {
            cached,
            default_headers: config.verbose.then_some(default_headers),
        };
        let client = generated::Client::new_with_client(&base_url, http_client, state);

        Ok(Self { client, base_url })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn launch_test(&self, launcher: &str, params: &Params) -> Result<LaunchResponse> {
        let body = launch_request(params)?;
        match self
            .client
            .launch_test()
            .launcher_name(launcher)
            .body(body)
            .send()
            .await
        {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_launch_client_error(err).await),
        }
    }

    pub async fn launch_debugging(&self, params: &Params) -> Result<LaunchResponse> {
        let body = launch_mvd_request(params)?;
        match self.client.launch_mvd().body(body).send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_launch_client_error(err).await),
        }
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunDetail> {
        match self.client.get_run().run_id(run_id).send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub async fn get_run_build_logs(&self, run_id: &str) -> Result<ByteStream> {
        ensure_resource_supported(run_id, MIN_BUILD_LOGS_VERSION, "build logs")?;
        match self.client.get_run_build_logs().run_id(run_id).send().await {
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
        let run_id = run_id.to_string();
        paginate(move |after| {
            let run_id = run_id.clone();
            async move {
                ensure_resource_supported(&run_id, MIN_PROPERTIES_VERSION, "run properties")?;
                let page = self
                    .fetch_run_properties_page(&run_id, after.as_deref(), status)
                    .await?;
                let generated::types::PropertyListResponse { data, next_cursor } = page;
                let normalized = data
                    .into_iter()
                    .map(normalize_property)
                    .collect::<Result<Vec<_>>>()?;
                Ok((normalized, next_cursor))
            }
        })
    }

    pub async fn search_run_events(&self, run_id: &str, query: &str) -> Result<ByteStream> {
        match self
            .client
            .search_run_events()
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
        paginate(move |after| {
            let opts = opts.clone();
            async move {
                let page = self
                    .fetch_runs_page_filtered(after.as_deref(), &opts)
                    .await?;
                let generated::types::RunListResponse { data, next_cursor } = page;
                Ok((data, next_cursor))
            }
        })
    }

    async fn fetch_runs_page_filtered(
        &self,
        after: Option<&str>,
        opts: &RunsFilterOptions,
    ) -> Result<generated::types::RunListResponse> {
        let mut request = self.client.list_runs().limit(100_u64);
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
            .run_id(run_id)
            .limit(100_u64);
        if let Some(cursor) = after {
            request = request.after(cursor);
        }
        if let Some(status) = status {
            request = request.status(status);
        }

        match request.send().await {
            Ok(response) => Ok(response.into_inner()),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientState {
    pub(crate) cached: Option<ClientWithMiddleware>,
    /// Default headers reqwest will merge into the outgoing request at
    /// `Client::execute` time (after our `exec` hook runs). `Some` enables
    /// verbose request/response logging to stderr; we hold the headers here
    /// so the log matches what's actually sent.
    pub(crate) default_headers: Option<reqwest::header::HeaderMap>,
}

impl ClientHooks<ClientState> for generated::Client {
    async fn exec(
        &self,
        request: reqwest::Request,
        _info: &OperationInfo,
    ) -> reqwest::Result<reqwest::Response> {
        let state = self.inner();
        let verbose_headers = state.default_headers.as_ref();
        if let Some(default_headers) = verbose_headers {
            let mut out = String::new();
            format_request(&request, default_headers, &mut out);
            eprint!("{out}");
        }

        let result = send_request(self.client(), state.cached.as_ref(), request).await;

        if verbose_headers.is_some()
            && let Ok(response) = &result
        {
            let mut out = String::new();
            format_response(response, &mut out);
            eprint!("{out}");
        }
        result
    }
}

async fn send_request(
    client: &Client,
    cached: Option<&ClientWithMiddleware>,
    request: reqwest::Request,
) -> reqwest::Result<reqwest::Response> {
    let Some(cached) = cached else {
        return client.execute(request).await;
    };

    // Bypass the cache for non-cloneable (streaming) bodies so the remaining
    // path always has a fallback to retry against on cache I/O failures
    // (which surface as `Error::Middleware` and can't be re-packaged as a
    // `reqwest::Error`).
    let Some(fallback) = request.try_clone() else {
        return client.execute(request).await;
    };

    match cached.execute(request).await {
        Ok(response) => Ok(response),
        Err(reqwest_middleware::Error::Reqwest(err)) => Err(err),
        Err(reqwest_middleware::Error::Middleware(err)) => {
            log::warn!("API cache failure, bypassing cache: {err}");
            client.execute(fallback).await
        }
    }
}

fn format_response(response: &reqwest::Response, out: &mut String) {
    use std::fmt::Write;

    let status = response.status();
    match status.canonical_reason() {
        Some(reason) => {
            let _ = writeln!(out, "< {} {reason}", status.as_u16());
        }
        None => {
            let _ = writeln!(out, "< {}", status.as_u16());
        }
    }
    for (name, value) in response.headers() {
        let value = value.to_str().unwrap_or("[non-ascii]");
        if is_sensitive_header(name) {
            let _ = writeln!(out, "< {name}: {}", redact_sensitive_value(name, value));
        } else {
            let _ = writeln!(out, "< {name}: {value}");
        }
    }
}

fn format_request(
    request: &reqwest::Request,
    default_headers: &reqwest::header::HeaderMap,
    out: &mut String,
) {
    use std::fmt::Write;

    let _ = writeln!(out, "> {} {}", request.method(), request.url());

    // reqwest merges `default_headers` at `Client::execute` time, after this
    // hook runs. Merge them in explicitly so the verbose log matches what's
    // actually sent, with sensitive values redacted.
    let mut emit = |name: &reqwest::header::HeaderName, value: &reqwest::header::HeaderValue| {
        let value = value.to_str().unwrap_or("[non-ascii]");
        if is_sensitive_header(name) {
            let _ = writeln!(out, "> {name}: {}", redact_sensitive_value(name, value));
        } else {
            let _ = writeln!(out, "> {name}: {value}");
        }
    };
    for (name, value) in request.headers() {
        emit(name, value);
    }
    for (name, value) in default_headers {
        if !request.headers().contains_key(name) {
            emit(name, value);
        }
    }
    let Some(body) = request.body() else {
        return;
    };
    let Some(bytes) = body.as_bytes() else {
        let _ = writeln!(out, "> <streaming body>");
        return;
    };
    if bytes.is_empty() {
        return;
    }
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            out.push_str(">\n");
            out.push_str(text);
            if !text.ends_with('\n') {
                out.push('\n');
            }
        }
        Err(_) => {
            let _ = writeln!(out, "> <{} bytes>", bytes.len());
        }
    }
}

fn is_sensitive_header(name: &reqwest::header::HeaderName) -> bool {
    use reqwest::header::{AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION, SET_COOKIE};
    matches!(name, n if n == AUTHORIZATION || n == PROXY_AUTHORIZATION || n == COOKIE || n == SET_COOKIE)
}

/// Redact a sensitive header value. For `Authorization` /
/// `Proxy-Authorization` the auth scheme is preserved so the log still shows
/// what kind of credential was sent (`Bearer secret-token` becomes
/// `bearer sec...`). Other sensitive headers (cookies) are reduced to their
/// first three chars.
fn redact_sensitive_value(name: &reqwest::header::HeaderName, value: &str) -> String {
    use reqwest::header::{AUTHORIZATION, PROXY_AUTHORIZATION};
    let take_prefix = |s: &str| s.chars().take(3).collect::<String>();
    let is_auth = name == AUTHORIZATION || name == PROXY_AUTHORIZATION;
    match value.split_once(' ') {
        Some((scheme, rest)) if is_auth => {
            format!("{} {}...", scheme.to_ascii_lowercase(), take_prefix(rest))
        }
        _ => format!("{}...", take_prefix(value)),
    }
}

#[derive(Clone, Default)]
pub struct RunsFilterOptions {
    pub status: Option<generated::types::RunStatus>,
    pub launcher: Option<String>,
    pub created_after: Option<chrono::DateTime<chrono::Utc>>,
    pub created_before: Option<chrono::DateTime<chrono::Utc>>,
}

fn paginate<'a, T, F, Fut>(fetch: F) -> impl futures_util::Stream<Item = Result<T>> + 'a
where
    F: FnMut(Option<String>) -> Fut + 'a,
    Fut: std::future::Future<Output = Result<(Vec<T>, Option<String>)>> + 'a,
    T: 'a,
{
    stream::try_unfold(
        (None::<String>, VecDeque::<T>::new(), false, fetch),
        |(mut after, mut buffer, mut finished, mut fetch)| async move {
            loop {
                if let Some(item) = buffer.pop_front() {
                    return Ok(Some((item, (after, buffer, finished, fetch))));
                }
                if finished {
                    return Ok(None);
                }
                let (items, next) = fetch(after.take()).await?;
                buffer.extend(items);
                finished = next.is_none();
                after = next;
            }
        },
    )
}

fn normalize_base_url(base_url: impl Into<String>) -> String {
    let base_url = base_url.into();
    let trimmed = base_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/api/v1")
        .unwrap_or(trimmed)
        .to_string()
}

fn default_request_headers(config: &Config) -> Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::AUTHORIZATION, auth_header(&config.auth)?);
    headers.insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_str(&crate::user_agent())
            .wrap_err("failed to build User-Agent header")?,
    );
    Ok(headers)
}

fn build_http_client(default_headers: reqwest::header::HeaderMap) -> Result<Client> {
    Client::builder()
        .default_headers(default_headers)
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

fn launch_mvd_request(params: &Params) -> Result<generated::types::LaunchMvdRequest> {
    let mut builder = generated::types::builder::MvdParams::default();

    for (key, value) in params.as_map() {
        let value = value
            .as_str()
            .ok_or_else(|| eyre!("debugging params must be strings: {key}"))?;

        builder = match key.as_str() {
            "antithesis.debugging.input_hash" => {
                builder.antithesis_debugging_input_hash(value.to_string())
            }
            "antithesis.debugging.session_id" => {
                builder.antithesis_debugging_session_id(value.to_string())
            }
            "antithesis.debugging.vtime" => builder.antithesis_debugging_vtime(value.to_string()),
            "antithesis.event_description" => {
                builder.antithesis_event_description(Some(value.to_string()))
            }
            "antithesis.report.recipients" => {
                builder.antithesis_report_recipients(Some(value.to_string()))
            }
            _ => return Err(eyre!("unknown debugging param: {key}")),
        };
    }

    let typed_params = generated::types::MvdParams::try_from(builder)
        .wrap_err("failed to build debugging params")?;
    generated::types::LaunchMvdRequest::try_from(
        generated::types::builder::LaunchMvdRequest::default().params(typed_params),
    )
    .wrap_err("failed to build debugging request")
}

/// The tenant version that first served the run properties resource. Runs
/// created on older tenants 404 on `/runs/{run_id}/properties`.
const MIN_PROPERTIES_VERSION: u32 = 52;

/// The tenant version that first served the run build logs resource. Runs
/// created on older tenants 404 on `/runs/{run_id}/build_logs`.
///
/// Note the other nested run resources have different (or no) cutoffs: run
/// properties arrived in v52, while logs and events are served for every
/// version we can produce, so neither needs a guard.
const MIN_BUILD_LOGS_VERSION: u32 = 54;

/// Run IDs encode the tenant version that produced them as their second
/// dash-delimited field — e.g. the `40` in
/// `e88ec3ec6cdb7b31ea08718616e04849-40-11`, which is structured as
/// `{hash}-{version}-{tenant_version}`. Returns an error when that version
/// predates `min_version`, since `resource` does not exist for those runs and
/// the server would otherwise answer with an opaque 404.
///
/// When the run ID doesn't match the expected structure the run is allowed
/// through, letting the server respond authoritatively rather than guessing
/// from the format.
fn ensure_resource_supported(run_id: &str, min_version: u32, resource: &str) -> Result<()> {
    if let Some(version) = run_version(run_id)
        && version < min_version
    {
        return Err(eyre!(
            "run {run_id} was generated on a tenant before v{min_version}, which introduced the {resource} API being used. Re-run {run_id} on a more recent version to access {resource}."
        ));
    }
    Ok(())
}

/// Extracts the tenant version encoded in a run ID structured as
/// `{hash}-{version}-{tenant_version}`, where the hash is a 32-character hex
/// string. Returns `None` when the ID doesn't match that structure (e.g. test
/// fixtures or future formats), so callers don't act on a misread version.
fn run_version(run_id: &str) -> Option<u32> {
    let parts: Vec<&str> = run_id.split('-').collect();
    let [hash, version, _tenant_version] = parts.as_slice() else {
        return None;
    };
    if hash.len() != 32 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    version.parse::<u32>().ok()
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
            let body = response
                .text()
                .await
                .unwrap_or_else(|err| format!("<failed to read response body: {err}>"));
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
            if body.trim().is_empty() {
                eyre!("API returned an empty response body where a JSON payload was expected")
            } else {
                let snippet = format_payload_snippet(&body, err.line(), err.column());
                eyre!("invalid API response payload: {err}\n{snippet}")
            }
        }
        ClientError::Custom(message) => eyre!(message),
    }
}

/// Same as [`format_api_client_error`] but adds a launch-specific suggestion
/// when the server returned an empty body where a launch response (with
/// `run_id`) was expected. Call from launch_test / launch_debugging only —
/// other endpoints have their own meaningful responses for empty bodies.
async fn format_launch_client_error(err: ClientError<generated::types::ErrorResponse>) -> Report {
    let body_is_empty = matches!(
        &err,
        ClientError::InvalidResponsePayload(body, _)
            if body.iter().all(u8::is_ascii_whitespace)
    );
    let report = format_api_client_error(err).await;
    if body_is_empty {
        report.with_suggestion(|| {
            "this can happen when the Antithesis server is on an older version that omits expected fields (for example, run_id from a launch response). Contact Antithesis support to confirm whether your tenant needs to be upgraded."
        })
    } else {
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config() -> Config {
        Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        )
    }

    fn test_api_with_cache(mock_server: &MockServer, cache_dir: &TempDir) -> AntithesisApi {
        AntithesisApi::with_base_url_and_cache_dir(
            test_config(),
            mock_server.uri(),
            cache_dir.path().join("api-cache"),
        )
        .unwrap()
    }

    #[test]
    fn format_request_redacts_authorization_and_dumps_text_body() {
        use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};

        let mut request = reqwest::Request::new(
            reqwest::Method::POST,
            "http://example.com/api/v1/launch".parse().unwrap(),
        );
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-rest-of-token"),
        );
        request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        *request.body_mut() = Some(r#"{"hello":"world"}"#.into());

        let mut out = String::new();
        format_request(&request, &HeaderMap::new(), &mut out);

        assert!(out.contains("POST http://example.com/api/v1/launch"));
        assert!(out.contains("authorization: bearer sec...\n"));
        assert!(!out.contains("secret-rest"));
        assert!(out.contains("content-type: application/json"));
        assert!(out.contains(r#"{"hello":"world"}"#));
    }

    #[test]
    fn format_request_merges_default_headers_with_redaction() {
        use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "http://example.com/api/v1/runs".parse().unwrap(),
        );
        let mut defaults = HeaderMap::new();
        defaults.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );

        let mut out = String::new();
        format_request(&request, &defaults, &mut out);

        assert!(out.contains("authorization: basic dXN...\n"));
        assert!(!out.contains("dXNlcjpwYXNz"));
    }

    #[test]
    fn redact_sensitive_value_handles_bearer_basic_and_cookies() {
        use reqwest::header::{AUTHORIZATION, COOKIE, HeaderName};
        let set_cookie = HeaderName::from_static("set-cookie");

        assert_eq!(
            redact_sensitive_value(&AUTHORIZATION, "Bearer secret-token-12345"),
            "bearer sec..."
        );
        assert_eq!(
            redact_sensitive_value(&AUTHORIZATION, "Basic dXNlcjpwYXNz"),
            "basic dXN..."
        );
        assert_eq!(
            redact_sensitive_value(&COOKIE, "sessionid=abcdef"),
            "ses..."
        );
        // Set-Cookie values often contain spaces (e.g. attributes), so the
        // scheme-detection heuristic must not apply.
        assert_eq!(
            redact_sensitive_value(&set_cookie, "session=very-secret; Path=/"),
            "ses..."
        );
    }

    #[test]
    fn format_request_does_not_duplicate_request_headers() {
        use reqwest::header::{HeaderMap, HeaderValue};

        let mut request = reqwest::Request::new(
            reqwest::Method::GET,
            "http://example.com/api/v1/runs".parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("api-version", HeaderValue::from_static("2.0"));
        let mut defaults = HeaderMap::new();
        defaults.insert("api-version", HeaderValue::from_static("1.0"));

        let mut out = String::new();
        format_request(&request, &defaults, &mut out);

        assert_eq!(out.matches("api-version").count(), 1);
        assert!(out.contains("api-version: 2.0"));
        assert!(!out.contains("api-version: 1.0"));
    }

    #[tokio::test]
    async fn format_api_client_error_describes_empty_invalid_payload() {
        let parse_err = serde_json::from_slice::<serde_json::Value>(b"").unwrap_err();
        let err = ClientError::<generated::types::ErrorResponse>::InvalidResponsePayload(
            Default::default(),
            parse_err,
        );

        let report = format_api_client_error(err).await;
        let message = format!("{report}");
        let debug = format!("{report:?}");

        assert_eq!(
            message,
            "API returned an empty response body where a JSON payload was expected"
        );
        assert!(!message.contains("EOF while parsing"));
        assert!(!message.contains('^'));
        assert!(
            !debug.contains("Antithesis support"),
            "generic formatter must not attach the launch-specific suggestion, got: {debug}"
        );
    }

    #[tokio::test]
    async fn format_launch_client_error_attaches_suggestion_for_empty_body() {
        let parse_err = serde_json::from_slice::<serde_json::Value>(b"").unwrap_err();
        let err = ClientError::<generated::types::ErrorResponse>::InvalidResponsePayload(
            Default::default(),
            parse_err,
        );

        let report = format_launch_client_error(err).await;
        let debug = format!("{report:?}");

        assert!(
            debug.contains("Antithesis support"),
            "expected launch-specific suggestion, got: {debug}"
        );
        assert!(
            debug.contains("run_id from a launch response"),
            "expected run_id wording in suggestion, got: {debug}"
        );
    }

    #[tokio::test]
    async fn format_launch_client_error_skips_suggestion_for_non_empty_body() {
        let body: &[u8] = b"not json";
        let parse_err = serde_json::from_slice::<serde_json::Value>(body).unwrap_err();
        let err = ClientError::<generated::types::ErrorResponse>::InvalidResponsePayload(
            body.to_vec().into(),
            parse_err,
        );

        let report = format_launch_client_error(err).await;
        let debug = format!("{report:?}");

        assert!(
            !debug.contains("Antithesis support"),
            "non-empty body must not attach the launch suggestion, got: {debug}"
        );
    }

    #[tokio::test]
    async fn format_api_client_error_keeps_snippet_for_non_empty_invalid_payload() {
        let body: &[u8] = b"not json";
        let parse_err = serde_json::from_slice::<serde_json::Value>(body).unwrap_err();
        let err = ClientError::<generated::types::ErrorResponse>::InvalidResponsePayload(
            body.to_vec().into(),
            parse_err,
        );

        let report = format_api_client_error(err).await;
        let message = format!("{report}");

        assert!(message.starts_with("invalid API response payload: "));
        assert!(message.contains("not json"));
        assert!(message.contains('^'));
    }

    #[tokio::test]
    async fn format_response_dumps_status_and_redacts_set_cookie() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(
                ResponseTemplate::new(418)
                    .insert_header("content-type", "text/plain")
                    .insert_header("set-cookie", "session=very-secret-token; Path=/"),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let response = reqwest::Client::new()
            .get(format!("{}/health", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let mut out = String::new();
        format_response(&response, &mut out);

        assert!(out.contains("< 418 I'm a teapot"));
        assert!(out.contains("< content-type: text/plain"));
        assert!(out.contains("< set-cookie: ses..."));
        assert!(!out.contains("very-secret-token"));
    }

    #[test]
    fn format_request_summarizes_binary_body() {
        let mut request = reqwest::Request::new(
            reqwest::Method::POST,
            "http://example.com/upload".parse().unwrap(),
        );
        *request.body_mut() = Some(vec![0xff_u8, 0xfe, 0xfd].into());

        let mut out = String::new();
        format_request(&request, &reqwest::header::HeaderMap::new(), &mut out);

        assert!(out.contains("<3 bytes>"));
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
    fn with_base_url_strips_legacy_api_suffix() {
        let config = Config::new(
            Auth::basic("user".to_string(), "pass".to_string()),
            "tenant".to_string(),
        );
        let api = AntithesisApi::with_base_url(config, "http://example.com/api/v1/").unwrap();
        assert_eq!(api.base_url(), "http://example.com");
    }

    #[tokio::test]
    async fn launch_test_sends_snouty_user_agent() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v1/launch/basic_test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "runId": "run-123",
                "statusCode": 200
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = AntithesisApi::with_base_url(test_config(), mock_server.uri()).unwrap();
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();
        api.launch_test("basic_test", &params).await.unwrap();

        let requests = mock_server.received_requests().await.unwrap();
        let user_agent = requests[0]
            .headers
            .get("user-agent")
            .expect("request should carry a User-Agent")
            .to_str()
            .unwrap();
        assert_eq!(user_agent, crate::user_agent());
        assert!(user_agent.starts_with("snouty/"));
    }

    #[tokio::test]
    async fn launch_test_uses_basic_auth() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v1/launch/basic_test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "runId": "run-123",
                "statusCode": 200
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
        assert_eq!(response.status_code, 200);
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
            .and(path("/api/v0/runs"))
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
            .and(path("/api/v0/runs"))
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

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let run_ids = runs.into_iter().map(|run| run.run_id).collect::<Vec<_>>();
        assert_eq!(run_ids, vec!["run-1", "run-2"]);
    }

    // Some historical run data stored is_ephemeral as "on"/"off" instead of
    // "true"/"false"; parsing must accept those as aliases (#122).
    #[tokio::test]
    async fn stream_runs_accepts_on_off_booleans_in_parameters() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [
                    {
                        "run_id": "run-on",
                        "status": "completed",
                        "created_at": "2025-03-20T02:00:00Z",
                        "launcher": "nightly",
                        "parameters": {"antithesis.is_ephemeral": "on"}
                    },
                    {
                        "run_id": "run-off",
                        "status": "completed",
                        "created_at": "2025-03-19T02:00:00Z",
                        "launcher": "nightly",
                        "parameters": {"antithesis.is_ephemeral": "off"}
                    }
                ],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = AntithesisApi::with_base_url(test_config(), mock_server.uri()).unwrap();

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let is_ephemeral = runs
            .iter()
            .map(|run| {
                run.parameters
                    .as_ref()
                    .unwrap()
                    .antithesis_is_ephemeral
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            is_ephemeral,
            vec![
                generated::types::ParamsAntithesisIsEphemeral::True,
                generated::types::ParamsAntithesisIsEphemeral::False,
            ]
        );
    }

    #[tokio::test]
    async fn stream_runs_returns_empty_when_no_runs_exist() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs"))
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

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(runs.is_empty());
    }

    #[tokio::test]
    async fn cache_serves_repeated_get_with_cache_control() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Cache-Control", "max-age=60")
                    .set_body_json(serde_json::json!({
                        "run_id": "run-1",
                        "status": "completed",
                        "created_at": "2025-03-20T02:00:00Z",
                        "launcher": "nightly"
                    })),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_with_cache(&mock_server, &cache_dir);

        let first = api.get_run("run-1").await.unwrap();
        let second = api.get_run("run-1").await.unwrap();

        assert_eq!(first.run_id, "run-1");
        assert_eq!(second.run_id, "run-1");
    }

    #[tokio::test]
    async fn stream_run_properties_follows_next_cursor() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/properties"))
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
            .and(path("/api/v0/runs/run-1/properties"))
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
            .iter()
            .map(|property| property.name().to_string())
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
            .and(path("/api/v0/runs/run-1/properties"))
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
            .and(path("/api/v0/runs/run-1/events"))
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

    fn rid(version: u32) -> String {
        format!("e88ec3ec6cdb7b31ea08718616e04849-{version}-11")
    }

    #[test]
    fn properties_rejected_before_v52() {
        let err = ensure_resource_supported(&rid(40), MIN_PROPERTIES_VERSION, "run properties")
            .unwrap_err()
            .to_string();
        assert!(err.contains("before v52"));
        assert!(err.contains("run properties"));
        assert!(err.contains("Re-run") && err.contains("more recent version"));
        // v51 is the last version without properties.
        assert!(
            ensure_resource_supported(&rid(51), MIN_PROPERTIES_VERSION, "run properties").is_err()
        );
    }

    #[test]
    fn properties_allowed_at_and_after_v52() {
        ensure_resource_supported(&rid(52), MIN_PROPERTIES_VERSION, "run properties").unwrap();
        ensure_resource_supported(&rid(60), MIN_PROPERTIES_VERSION, "run properties").unwrap();
    }

    #[test]
    fn build_logs_rejected_before_v54() {
        // build logs arrive two versions after properties, so v52/v53 are still rejected.
        assert!(ensure_resource_supported(&rid(52), MIN_BUILD_LOGS_VERSION, "build logs").is_err());
        let err = ensure_resource_supported(&rid(53), MIN_BUILD_LOGS_VERSION, "build logs")
            .unwrap_err()
            .to_string();
        assert!(err.contains("before v54"));
        assert!(err.contains("build logs"));
    }

    #[test]
    fn build_logs_allowed_at_and_after_v54() {
        ensure_resource_supported(&rid(54), MIN_BUILD_LOGS_VERSION, "build logs").unwrap();
        ensure_resource_supported(&rid(60), MIN_BUILD_LOGS_VERSION, "build logs").unwrap();
    }

    #[test]
    fn resource_allowed_when_version_unparsable() {
        // Unexpected formats are allowed through so the server can respond.
        for id in ["run-1", "no-dashes", "plainrunid"] {
            ensure_resource_supported(id, MIN_PROPERTIES_VERSION, "run properties").unwrap();
            ensure_resource_supported(id, MIN_BUILD_LOGS_VERSION, "build logs").unwrap();
        }
    }
}
