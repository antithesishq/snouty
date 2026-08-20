use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU64;
use std::time::Duration;

use color_eyre::eyre::{Context, Report, Result, eyre};
use color_eyre::{Section, SectionExt};
use futures_util::{StreamExt, stream};
use log::debug;
use progenitor_client::{
    ClientHooks, ClientInfo, Error as ClientError, OperationInfo, ResponseValue,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Proxy};
use serde::de::DeserializeOwned;

use crate::api_cache::{self, ApiCache, CachePolicy};
use crate::auth::{AuthenticationInfo, PasswordPolicy};
use crate::env;
use crate::error::{ApiError, user_error};
use crate::params::{
    ANT_CONFIG_IMAGE, ANT_DEBUGGING_INPUT_HASH, ANT_DEBUGGING_RUN_ID, ANT_DEBUGGING_SESSION_ID,
    ANT_DEBUGGING_VTIME, ANT_DESCRIPTION, ANT_DURATION, ANT_EVENT_DESCRIPTION,
    ANT_FILTER_LOGS_MATCHING, ANT_IMAGES, ANT_IS_EPHEMERAL, ANT_REPORT_RECIPIENTS, ANT_SOURCE,
    ANT_TEST_NAME, Params,
};
use crate::render::sanitize;
use crate::settings::Settings;
use crate::tag::{Tag, Tagged};
use crate::util::source_error;
use crate::vtime::VTime;
use snouty_macros::cached;

#[allow(dead_code, unused_imports, private_interfaces)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/antithesis_api.rs"));
}

use crate::jsonl::{JsonStream, json_lines};
pub(crate) use generated::types::Params as RunParams;
pub use generated::types::{
    BuildLogLine, Event, EventProperty, Moment, NonEventProperty, Property, PropertyStatus,
    RunDetail, RunStatus, RunSummary,
};

/// The outcome of a launch or debugging-launch request, and the `--json` output
/// of `snouty launch` / `snouty debug`.
///
/// snouty owns this shape rather than re-exporting a generated response type,
/// so the `--json` contract is a deliberate choice rather than whatever the
/// vendored schema happens to say. The run id is the only thing a caller can
/// act on: success or failure is the exit code, and the launch responses'
/// body-level `statusCode` is a field the API team has confirmed clients should
/// ignore (#180). The HTTP status is still visible with `--verbose`, which
/// dumps the response as it arrived.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchResponse {
    pub run_id: Option<String>,
}

/// API and tenant release version, from `GET /api/version`.
#[derive(Debug, Clone)]
pub struct ApiVersion {
    pub latest_api_version: String,
    pub release_version: String,
    /// `release_version`'s comparable form, parsed eagerly: the leading
    /// `major.minor` pair of a string like `"60.1"`. `None` when the string
    /// does not lead with integers.
    pub release: Option<(u64, u64)>,
}

/// The tenant release the events-search API ships with. `runs events` and
/// `runs search` assume an enabled `runs-search` feature means the tenant
/// serves the endpoint; `snouty doctor` verifies the assumption against
/// this, and the search 404 error names it.
pub const MIN_SEARCH_RELEASE: (u64, u64) = (58, 11);

impl ApiVersion {
    /// Parses `release_version` eagerly, so no consumer ever parses it.
    pub fn new(latest_api_version: String, release_version: String) -> Self {
        let release = parse_release(&release_version);
        Self {
            latest_api_version,
            release_version,
            release,
        }
    }
}

fn parse_release(version: &str) -> Option<(u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next()?.trim().parse().ok()?;
    let minor = parts.next().unwrap_or("0").trim().parse().ok()?;
    Some((major, minor))
}

/// What an events-search request asks the server to do, mirroring the
/// `Search_Request` body of `POST /runs/{run_id}/events/search` minus the
/// required `query`. (The body's `count_only` switch is not exposed and
/// build.rs strips it from the generated type: the API team is moving the
/// count into a separate endpoint, and current tenants ignore the switch
/// anyway.)
#[derive(Debug, Clone, Copy)]
pub enum SearchMode {
    /// Validate the query's syntax without executing it: an empty 200 body
    /// when the query parses, a 400 when it does not.
    Validate,
    /// Execute the query. `stream: true` keeps the connection open and new
    /// matches arrive live; `false` closes it after the current matching
    /// set. `limit` caps the returned events (server-validated range
    /// 1..=999); `None` sends no limit — the server default of 50 applies to
    /// a closed request, and a streaming one stays unbounded.
    Query {
        stream: bool,
        limit: Option<NonZeroU64>,
    },
}

/// The server's default for the search `limit`, applied when the request
/// names none. A non-streaming caller that enforces the limit client-side
/// caps at this value when no explicit limit was given.
pub const SEARCH_DEFAULT_LIMIT: NonZeroU64 = NonZeroU64::new(50).unwrap();

/// Why a `/api/version` probe failed, classified for `snouty doctor`.
#[derive(Debug)]
pub enum VersionError {
    /// The server replied with a non-success HTTP status (e.g. 404 when the
    /// endpoint is missing on an older backend, or 401/403 when auth is rejected).
    Http(u16),
    /// The server answered with a success status, but the body did not parse
    /// as the version payload — typically a proxy or captive portal serving
    /// its own page. Reachability is proven; the response is wrong.
    BadResponse(String),
    /// The API could not be reached at all (DNS, connection, TLS, timeout).
    Unreachable(String),
}

impl RunStatus {
    /// Whether the run has stopped and its status will not change again.
    /// `unknown` is not terminal: it is the server saying it cannot classify
    /// the run.
    pub(crate) fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Cancelled | RunStatus::Incomplete
        )
    }
}

fn params_test_name(params: Option<&RunParams>) -> Option<&str> {
    params.and_then(|p| p.extra.get(ANT_TEST_NAME).map(String::as_str))
}

fn params_test_description(params: Option<&RunParams>) -> Option<&str> {
    params.and_then(|p| p.antithesis_description.as_deref())
}

impl RunSummary {
    pub(crate) fn test_name(&self) -> Option<&str> {
        params_test_name(self.parameters.as_ref())
    }

    /// Human-readable description: prefer the server-provided top-level
    /// `description` field, falling back to the `antithesis.description`
    /// parameter for runs predating that field.
    pub(crate) fn test_description(&self) -> Option<&str> {
        self.description
            .as_deref()
            .or_else(|| params_test_description(self.parameters.as_ref()))
    }
}

impl RunDetail {
    pub(crate) fn test_name(&self) -> Option<&str> {
        params_test_name(self.parameters.as_ref())
    }

    pub(crate) fn test_description(&self) -> Option<&str> {
        self.description
            .as_deref()
            .or_else(|| params_test_description(self.parameters.as_ref()))
    }

    /// The requested run duration as launched (`antithesis.duration`, a count of
    /// minutes), if the run recorded one. This is the configured workload length,
    /// distinct from the wall-clock time derived from the run's timestamps.
    pub(crate) fn requested_duration(&self) -> Option<&str> {
        self.parameters.as_ref()?.antithesis_duration.as_deref()
    }

    /// The source the run was launched from (`antithesis.source`), if recorded.
    pub(crate) fn source(&self) -> Option<&str> {
        self.parameters.as_ref()?.antithesis_source.as_deref()
    }

    /// The failure moment if it pins a real point in the run, otherwise `None`.
    ///
    /// A timed-out or killed run has no moment-pinned failure, so the API reports
    /// a placeholder `0/0` moment that streams no logs. Treat that placeholder as
    /// "no moment" so callers neither show empty Failure Hash/VTime rows nor a
    /// `runs logs` hint that would point at an empty stream.
    pub(crate) fn real_failure_moment(&self) -> Option<&Moment> {
        self.failure_moment
            .as_ref()
            .filter(|m| m.input_hash != "0" || m.vtime != VTime::ZERO)
    }
}

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

/// Connect-phase cap (DNS + TCP + TLS). Bounds connection setup so no command
/// hangs on a black-holed or unresolvable host. There is deliberately no read or
/// total timeout: once connected, an Antithesis request may take a truly long time
/// to return (e.g. massive log files) and must not be aborted — the user can ctrl-c.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times a GET whose transport failed is re-sent before the failure
/// surfaces, with [`transient_retry_backoff`] between attempts.
const MAX_TRANSIENT_RETRIES: u32 = 3;

/// The wait before transient-retry attempt `retry` (0-based): 1, 2, 4, ... seconds
fn transient_retry_backoff(retry: u32) -> Duration {
    #[cfg(not(test))]
    const UNIT: Duration = Duration::from_secs(1);
    #[cfg(test)]
    const UNIT: Duration = Duration::from_millis(1);
    UNIT * (1 << retry)
}

/// Whether a failed request is safe to re-send: true only for transport-level
/// failures — connect errors and timeouts, resets, and connections that
/// dropped mid-message — where no HTTP response arrived.
fn is_transient_transport_error(error: &reqwest::Error) -> bool {
    if error.is_timeout() || error.is_connect() {
        true
    } else if error.is_body() || error.is_decode() || error.is_builder() || error.is_redirect() {
        false
    } else if error.is_request() {
        // reqwest does not classify hyper's transport failures, so unwrap
        // them here
        if let Some(hyper_error) = source_error::<hyper::Error>(error) {
            hyper_error.is_incomplete_message()
                || hyper_error.is_canceled()
                || source_error::<std::io::Error>(hyper_error).is_some_and(|io_error| {
                    matches!(
                        io_error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    )
                })
        } else {
            false
        }
    } else {
        false
    }
}

/// Send `request`, re-sending a GET whose transport failed: up to
/// [`MAX_TRANSIENT_RETRIES`] extra attempts with [`transient_retry_backoff`]
/// between them. [`MAX_TRANSIENT_RETRIES`] has the retry contract.
async fn execute_with_transient_retry(
    client: &Client,
    verbose: bool,
    mut request: reqwest::Request,
) -> reqwest::Result<reqwest::Response> {
    if request.method() != reqwest::Method::GET {
        return client.execute(request).await;
    }
    let mut retries = 0;
    loop {
        // GETs carry no body, so try_clone always succeeds.
        let retry_clone = (retries < MAX_TRANSIENT_RETRIES)
            .then(|| request.try_clone())
            .flatten();
        match (client.execute(request).await, retry_clone) {
            (Err(err), Some(clone)) if is_transient_transport_error(&err) => {
                let wait = transient_retry_backoff(retries);
                retries += 1;
                debug!(
                    "transient network error, retry {retries} of {MAX_TRANSIENT_RETRIES} \
                     in {wait:?}: {err}"
                );
                if verbose {
                    eprintln!("* transient network error, retrying in {wait:?}: {err}");
                }
                tokio::time::sleep(wait).await;
                request = clone;
            }
            (result, _) => return result,
        }
    }
}

/// Where API responses are cached. The cache itself is the logical,
/// domain-aware cache in [`crate::api_cache`]: each handler that serves an
/// immutable resource consults it, and every cache error is a miss.
#[derive(Debug)]
pub(crate) enum ResponseCache {
    /// `$XDG_RUNTIME_DIR/snouty/api-cache-v3`, falling back to a per-user
    /// directory under the system temp dir — the default for every command.
    Default,
    /// No response cache: every request hits the server. The handlers
    /// already keep mutable resources out of the cache, so this exists for
    /// callers that want no cache I/O at all (status polls, tests).
    Disabled,
    /// Cache under this directory.
    #[cfg(test)]
    Dir(std::path::PathBuf),
}

/// Where [`AntithesisApi::get_run_logs`] starts streaming from, instead of
/// the root. The vtime alone is enough; the input
/// hash is an optimization the endpoint accepts only alongside it, which is
/// why this is not a [`Moment`].
#[derive(Clone, Debug, serde::Serialize)]
pub struct LogsBegin {
    pub vtime: VTime,
    pub input_hash: Option<String>,
}

/// End `stream` at the first line whose `moment.vtime` is past `end`. The
/// endpoint streams in chronological order, so nothing at or before the end
/// follows a line past it. A line with no vtime (a `Stream_Error`) is never
/// past the end: error objects must reach the user.
fn truncate_at_end_vtime(stream: JsonStream, end: VTime) -> JsonStream {
    stream
        .take_while(move |item| {
            let keep = match item {
                Ok(value) => {
                    VTime::from_json(&value["moment"]["vtime"]).is_none_or(|vtime| vtime <= end)
                }
                Err(_) => true,
            };
            futures_util::future::ready(keep)
        })
        .boxed()
}

pub struct AntithesisApi {
    client: generated::Client,
    base_url: String,
    cache: ApiCache,
}

impl AntithesisApi {
    /// Rejects username/password credentials with a friendly error
    /// ([`PasswordPolicy::Reject`]); the launch commands use
    /// [`AntithesisApi::new_for_launch`].
    pub fn new(settings: &Settings, verbose: bool) -> Result<Self> {
        Self::build(
            settings,
            AuthenticationInfo::for_ambient_configuration(
                settings.profile(),
                PasswordPolicy::Reject,
            )?,
            verbose,
            ResponseCache::Default,
        )
    }

    /// Like [`AntithesisApi::new`], but with [`ResponseCache::Disabled`].
    pub fn new_uncached(settings: &Settings, verbose: bool) -> Result<Self> {
        Self::build(
            settings,
            AuthenticationInfo::for_ambient_configuration(
                settings.profile(),
                PasswordPolicy::Reject,
            )?,
            verbose,
            ResponseCache::Disabled,
        )
    }

    /// Like [`AntithesisApi::new`], but accepts the deprecated
    /// username/password credentials and warns on use
    /// ([`PasswordPolicy::WarnDeprecated`]) — the launch webhooks are the
    /// only endpoints that still accept them.
    pub fn new_for_launch(settings: &Settings, verbose: bool) -> Result<Self> {
        Self::build(
            settings,
            AuthenticationInfo::for_ambient_configuration(
                settings.profile(),
                PasswordPolicy::WarnDeprecated,
            )?,
            verbose,
            ResponseCache::Default,
        )
    }

    pub(crate) fn build(
        settings: &Settings,
        authn_info: AuthenticationInfo,
        verbose: bool,
        cache: ResponseCache,
    ) -> Result<Self> {
        // base_url() is None exactly when neither an explicit base_url nor a
        // tenant resolved; surface the tenant diagnostic, since that's what a
        // user normally sets.
        let base_url = normalize_base_url(crate::settings::require(settings.base_url(), "tenant")?);
        debug!("initializing API client for {}", base_url);

        let default_headers = default_request_headers()?;
        let http_client = build_http_client(default_headers.clone(), settings)?;
        let cache_dir = match cache {
            ResponseCache::Default => api_cache::default_dir(),
            ResponseCache::Disabled => None,
            #[cfg(test)]
            ResponseCache::Dir(dir) => Some(dir),
        };
        let cache = ApiCache::new(
            cache_dir,
            settings.api_cache_max_file_size(),
            settings.api_cache_respect_headers(),
            base_url.clone(),
            verbose,
        );
        let state = ClientState {
            authn_info,
            default_headers: verbose.then_some(default_headers),
        };
        let client = generated::Client::new_with_client(&base_url, http_client, state);

        Ok(Self {
            client,
            base_url,
            cache,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The host of the configured base URL (no scheme, port, or path), for
    /// user-facing messages. Falls back to the full base URL if it won't parse.
    pub fn host(&self) -> String {
        reqwest::Url::parse(&self.base_url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_string))
            .unwrap_or_else(|| self.base_url.clone())
    }

    pub async fn launch_test(&self, launcher: &str, params: &Params) -> Result<LaunchResponse> {
        let body = launch_request(params)?;
        let result = self
            .client
            .launch_test()
            .launcher_name(launcher)
            .body(body)
            .send()
            .await;
        let response: generated::types::LaunchResponse = finish_launch(result).await?;
        Ok(LaunchResponse {
            run_id: response.run_id,
        })
    }

    pub async fn launch_debugging(&self, params: &Params) -> Result<LaunchResponse> {
        let body = launch_mvd_request(params)?;
        let result = self.client.launch_mvd().body(body).send().await;
        let response: generated::types::LaunchMvdResponse = finish_launch(result).await?;
        Ok(LaunchResponse {
            run_id: response.run_id,
        })
    }

    #[cached(value)]
    pub async fn get_run(&self, run_id: &str) -> Result<Tagged<RunDetail, CachePolicy>> {
        match self.client.get_run().run_id(run_id).send().await {
            Ok(response) => {
                let headers_admit = self.cache.headers_admit(response.headers());
                let detail = response.into_inner();
                // A run detail is immutable only once the run reaches a
                // terminal status.
                let cache_policy =
                    CachePolicy::cache_if(headers_admit && detail.status.is_terminal());
                Ok(detail.with_tag(cache_policy))
            }
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    /// Probe `GET /api/version` for the API and tenant release versions. The
    /// endpoint authenticates like every endpoint other than launch, so the
    /// probe runs only when an API key is configured; receiving any HTTP
    /// response (success or error) doubles as proof the API is reachable, and
    /// `snouty doctor` uses it as a connectivity check. Errors are classified
    /// (HTTP status vs unreachable) rather than rendered, so the caller can
    /// decide how to present each case.
    pub async fn get_version(&self) -> std::result::Result<ApiVersion, VersionError> {
        // The client's connect timeout (CONNECT_TIMEOUT) keeps a black-holed or
        // unresolvable host from hanging this probe.
        match self.client.get_version().send().await {
            Ok(response) => {
                let v = response.into_inner();
                Ok(ApiVersion::new(v.latest_api_version, v.release_version))
            }
            Err(err) => Err(match err.status() {
                Some(code) => VersionError::Http(code.as_u16()),
                // A success response whose body did not parse also carries no
                // status, but the server answered — most likely a proxy or
                // captive portal serving its own page. Calling that
                // "unreachable" would send the user debugging their network.
                None => match err {
                    ClientError::InvalidResponsePayload(_, err) => {
                        VersionError::BadResponse(err.to_string())
                    }
                    err => {
                        // The top-level display ("Communication Error: error
                        // sending request for url (...)") only restates that
                        // the request failed; the actionable cause —
                        // connection refused, DNS failure, timeout — is the
                        // last error in the source chain.
                        let mut cause: &dyn std::error::Error = &err;
                        while let Some(source) = cause.source() {
                            cause = source;
                        }
                        VersionError::Unreachable(cause.to_string())
                    }
                },
            }),
        }
    }

    #[cached(stream)]
    pub async fn get_run_build_logs(
        &self,
        run_id: &str,
    ) -> Result<Tagged<JsonStream, CachePolicy>> {
        // The version gate runs on misses only: a run it rejects never got a
        // 200 from the server, so no cache entry can exist for it.
        ensure_resource_supported(run_id, MIN_BUILD_LOGS_VERSION, "build logs")?;
        match self.client.get_run_build_logs().run_id(run_id).send().await {
            Ok(response) => {
                let cache_policy =
                    CachePolicy::cache_if(self.cache.headers_admit(response.headers()));
                Ok(json_lines(response.into_inner().into_inner()).with_tag(cache_policy))
            }
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    /// The endpoint takes each vtime as a decimal-seconds query parameter. The
    /// generated setters accept anything that converts to `String`, and
    /// [`VTime`]'s conversion is its exact `Display` text.
    ///
    /// The endpoint streams the branch's logs past the moment's vtime, so a
    /// nonzero vtime is enforced here as the stream's end, upstream of the
    /// `#[cached]` tee — the truncation is what makes the cached stream
    /// immutable. The `0` placeholder asks for the branch's current end,
    /// which can still move forward: never cached.
    #[cached(stream)]
    pub async fn get_run_logs(
        &self,
        run_id: &str,
        moment: Moment,
        begin: Option<LogsBegin>,
    ) -> Result<Tagged<JsonStream, CachePolicy>> {
        let end = (moment.vtime != VTime::ZERO).then_some(moment.vtime);
        let mut request = self
            .client
            .get_run_logs()
            .run_id(run_id)
            .input_hash(moment.input_hash)
            .vtime(moment.vtime);
        if let Some(begin) = begin {
            request = request.begin_vtime(begin.vtime);
            if let Some(hash) = begin.input_hash {
                request = request.begin_input_hash(hash);
            }
        }

        match request.send().await {
            Ok(response) => {
                // `end.is_some()` first: without an end vtime the stream is
                // uncacheable regardless, so skip the header parse.
                let cache_policy = CachePolicy::cache_if(
                    end.is_some() && self.cache.headers_admit(response.headers()),
                );
                let stream = json_lines(response.into_inner().into_inner());
                let stream = match end {
                    Some(end) => truncate_at_end_vtime(stream, end),
                    None => stream,
                };
                Ok(stream.with_tag(cache_policy))
            }
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    /// Execute a bash script in the run's live session, starting at `moment`.
    /// Returns the NDJSON response stream: `output` events, then a terminal
    /// `exited` or `timed_out` event.
    ///
    /// The request body carries `moment.vtime` as a JSON number ([`VTime`]'s
    /// wire form), where the spec documents a string. The server accepts both
    /// — verified against the live API (orbitinghail, release 60.0) with this
    /// exact request path — and the number form keeps the moment value-exact
    /// end to end instead of round-tripping through a second text form.
    pub async fn execute_command(
        &self,
        run_id: &str,
        moment: Moment,
        script: String,
        timeout: Duration,
    ) -> Result<JsonStream> {
        // No `use_otis`: `build.rs` drops that field from the generated type,
        // so snouty says nothing about it and the server applies its default.
        let body = generated::types::ExecuteCommandRequest {
            moment,
            script,
            // The wire field is a whole number of seconds.
            timeout_seconds: timeout.as_secs(),
        };
        let request = self.client.execute_command().run_id(run_id).body(body);
        match request.send().await {
            Ok(response) => Ok(json_lines(response.into_inner().into_inner())),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub fn stream_run_properties(
        &self,
        run_id: &str,
        status: Option<PropertyStatus>,
    ) -> impl futures_util::Stream<Item = Result<Property>> + '_ {
        const MAX_PAGE_LIMIT: u64 = 100;
        let run_id = run_id.to_string();
        paginate(move |after| {
            let run_id = run_id.clone();
            async move {
                ensure_resource_supported(&run_id, MIN_PROPERTIES_VERSION, "run properties")?;
                let page = self
                    .fetch_run_properties_page(&run_id, after.as_deref(), status, MAX_PAGE_LIMIT)
                    .await?
                    .untag();
                let generated::types::PropertyListResponse { data, next_cursor } = page;
                let normalized = data
                    .into_iter()
                    .map(normalize_property)
                    .collect::<Result<Vec<_>>>()?;
                Ok((normalized, next_cursor))
            }
        })
    }

    pub async fn search_run_events(
        &self,
        run_id: &str,
        query: &str,
        limit: Option<NonZeroU64>,
    ) -> Result<JsonStream> {
        // The endpoint caps the returned events at `limit`. Only send the
        // parameter when the user asked for one: tenants that predate it would
        // otherwise receive a query param they may not accept, and omitting it
        // lets the server apply its default. The server validates the range;
        // `NonZeroU64` (the generated parameter's own type) keeps a zero from
        // ever reaching the request builder.
        let mut request = self.client.search_run_events().run_id(run_id).q(query);
        if let Some(limit) = limit {
            request = request.limit(limit);
        }
        match request.send().await {
            Ok(response) => Ok(json_lines(response.into_inner().into_inner())),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    /// POST an event-set DSL query to the events-search endpoint and return
    /// the response stream. Every mode's answer arrives through the stream
    /// (see [`SearchMode`]): matching events as JSONL, the `Validate` answer
    /// as an empty body. build.rs drops the operation's `application/json`
    /// response variant so the generated method exposes the stream (see
    /// `untype_search_count_response` there).
    pub async fn search_run_events_query(
        &self,
        run_id: &str,
        query: &str,
        mode: SearchMode,
    ) -> Result<JsonStream> {
        let mut body = generated::types::SearchRequest::builder()
            .query(query)
            .is_streaming(matches!(mode, SearchMode::Query { stream: true, .. }))
            .validate_only(matches!(mode, SearchMode::Validate));
        // Only a caller-named limit reaches the request. An omitted limit is
        // meaningful: a closed request falls to the server default (50), and
        // a streaming one stays unbounded — the server is starting to honor
        // `limit` together with `is_streaming`, so naming a default here
        // would cut a `--follow` off at 50 events. build.rs strips the
        // schema's `default: 50` so the generated field is omittable at all
        // (see `unrequire_search_limit_default` there).
        if let SearchMode::Query {
            limit: Some(limit), ..
        } = mode
        {
            body = body.limit(limit);
        }
        match self.client.search().run_id(run_id).body(body).send().await {
            Ok(response) => Ok(json_lines(response.into_inner().into_inner())),
            Err(err) => Err(format_api_client_error(err).await),
        }
    }

    pub fn stream_runs_filtered(
        &self,
        opts: &RunsFilterOptions,
        page_limit: u64,
    ) -> impl futures_util::Stream<Item = Result<RunSummary>> + '_ {
        let opts = opts.clone();
        paginate(move |after| {
            let opts = opts.clone();
            async move {
                let page = self
                    .fetch_runs_page_filtered(after.as_deref(), &opts, page_limit)
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
        page_limit: u64,
    ) -> Result<generated::types::RunListResponse> {
        let mut request = self.client.list_runs().limit(page_limit);
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

    #[cached(value)]
    async fn fetch_run_properties_page(
        &self,
        run_id: &str,
        after: Option<&str>,
        status: Option<PropertyStatus>,
        limit: u64,
    ) -> Result<Tagged<generated::types::PropertyListResponse, CachePolicy>> {
        let mut request = self
            .client
            .list_run_properties()
            .run_id(run_id)
            .limit(limit);
        if let Some(cursor) = after {
            request = request.after(cursor);
        }
        if let Some(status) = status {
            request = request.status(status);
        }

        match request.send().await {
            Ok(response) => {
                let cache_policy =
                    CachePolicy::cache_if(self.cache.headers_admit(response.headers()));
                Ok(response.into_inner().with_tag(cache_policy))
            }
            Err(err) => Err(format_api_client_error(err).await),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientState {
    authn_info: AuthenticationInfo,
    /// Default headers reqwest will merge into the outgoing request at
    /// `Client::execute` time (after our `exec` hook runs). `Some` enables
    /// verbose request/response logging to stderr; we hold the headers here
    /// so the log matches what's actually sent.
    default_headers: Option<HeaderMap>,
}

impl ClientHooks<ClientState> for generated::Client {
    async fn pre<E>(
        &self,
        request: &mut reqwest::Request,
        info: &OperationInfo,
    ) -> std::result::Result<(), ClientError<E>> {
        self.inner()
            .authn_info
            .authenticate_request(self.client(), self.baseurl(), request, info)
            .await?;
        Ok(())
    }

    async fn exec(
        &self,
        request: reqwest::Request,
        _info: &OperationInfo,
    ) -> reqwest::Result<reqwest::Response> {
        let state = self.inner();
        let verbose_headers = state.default_headers.as_ref();

        // Keep a resendable copy so a token refresh can retry the request once if
        // the first attempt is rejected as unauthorized but we have a refreshable credential
        let retry_request = if state.authn_info.can_refresh() {
            request.try_clone()
        } else {
            None
        };

        if let Some(default_headers) = verbose_headers {
            let mut out = String::new();
            format_request(&request, default_headers, &mut out);
            eprint!("{out}");
        }

        let result =
            execute_with_transient_retry(self.client(), verbose_headers.is_some(), request).await;

        let result = match (result, retry_request) {
            (Ok(response), Some(mut retry_request))
                if response.status() == reqwest::StatusCode::UNAUTHORIZED =>
            {
                match state
                    .authn_info
                    .refresh_after_unauthorized(self.client(), self.baseurl())
                    .await
                {
                    Ok(Some(header)) => {
                        retry_request
                            .headers_mut()
                            .insert(reqwest::header::AUTHORIZATION, header);
                        if let Some(default_headers) = verbose_headers {
                            let mut out = String::new();
                            format_request(&retry_request, default_headers, &mut out);
                            eprint!("{out}");
                        }
                        execute_with_transient_retry(
                            self.client(),
                            verbose_headers.is_some(),
                            retry_request,
                        )
                        .await
                    }
                    Ok(None) => Ok(response),
                    Err(err) => {
                        log::warn!("reactive OAuth token refresh failed: {err:#}");
                        Ok(response)
                    }
                }
            }
            (result, _) => result,
        };

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

fn format_request(request: &reqwest::Request, default_headers: &HeaderMap, out: &mut String) {
    use std::fmt::Write;

    let _ = writeln!(out, "> {} {}", request.method(), request.url());

    // reqwest merges `default_headers` at `Client::execute` time, after this
    // hook runs. Merge them in explicitly so the verbose log matches what's
    // actually sent, with sensitive values redacted.
    let mut emit = |name: &HeaderName, value: &HeaderValue| {
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

fn is_sensitive_header(name: &HeaderName) -> bool {
    use reqwest::header::{AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION, SET_COOKIE};
    matches!(name, n if n == AUTHORIZATION || n == PROXY_AUTHORIZATION || n == COOKIE || n == SET_COOKIE)
}

/// Redact a sensitive header value. For `Authorization` /
/// `Proxy-Authorization` the auth scheme is preserved so the log still shows
/// what kind of credential was sent (`Bearer secret-token` becomes
/// `bearer sec...`). Other sensitive headers (cookies) are reduced to their
/// first three chars.
fn redact_sensitive_value(name: &HeaderName, value: &str) -> String {
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
    pub status: Option<RunStatus>,
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

fn default_request_headers() -> Result<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::USER_AGENT,
        HeaderValue::from_str(&crate::user_agent())
            .wrap_err("failed to build User-Agent header")?,
    );
    for (name, value) in extra_headers_from_env()? {
        headers.insert(name, value);
    }
    Ok(headers)
}

fn extra_headers_from_env() -> Result<Vec<(HeaderName, HeaderValue)>> {
    if let Some(extra_headers) = env::var("ANTITHESIS_EXTRA_HEADERS")? {
        extra_headers
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let (name, value) = line.split_once(':').ok_or_else(|| {
                    eyre!("ANTITHESIS_EXTRA_HEADERS entry missing ':' separator: {line:?}")
                })?;
                let name = HeaderName::from_bytes(name.trim().as_bytes()).wrap_err_with(|| {
                    format!("invalid header name in ANTITHESIS_EXTRA_HEADERS: {name:?}")
                })?;
                let value = HeaderValue::from_str(value.trim()).wrap_err_with(|| {
                    format!("invalid header value in ANTITHESIS_EXTRA_HEADERS for {name}")
                })?;
                Ok((name, value))
            })
            .collect()
    } else {
        Ok(vec![])
    }
}

fn build_http_client(default_headers: HeaderMap, settings: &Settings) -> Result<Client> {
    // Only a connect timeout (see CONNECT_TIMEOUT): no read or total timeout, so a
    // slow-but-alive Antithesis request is never aborted no matter how long it runs.
    let mut builder = Client::builder()
        .default_headers(default_headers)
        .connect_timeout(CONNECT_TIMEOUT);

    if let Some(proxy_address) = settings.https_proxy() {
        let proxy = Proxy::all(proxy_address)
            .wrap_err_with(|| eyre!("invalid proxy URL: {proxy_address}"))?;
        builder = builder.proxy(proxy);
    }

    builder.build().wrap_err("failed to build API client")
}

fn launch_request(params: &Params) -> Result<generated::types::LaunchRequest> {
    let mut builder = generated::types::builder::Params::default();
    let mut extra = HashMap::new();

    for (key, value) in params.as_map() {
        let value = value
            .as_str()
            .ok_or_else(|| user_error(format!("launch params must be strings: {key}")))?;

        builder = match key.as_str() {
            ANT_CONFIG_IMAGE => builder.antithesis_config_image(Some(value.to_string())),
            ANT_DESCRIPTION => builder.antithesis_description(Some(value.to_string())),
            ANT_DURATION => builder.antithesis_duration(Some(value.to_string())),
            ANT_IMAGES => builder.antithesis_images(Some(value.to_string())),
            ANT_IS_EPHEMERAL => builder.antithesis_is_ephemeral(Some(
                generated::types::ParamsAntithesisIsEphemeral::try_from(value)
                    .wrap_err("invalid antithesis.is_ephemeral value")?,
            )),
            ANT_FILTER_LOGS_MATCHING => {
                builder.antithesis_filter_logs_matching(Some(value.to_string()))
            }
            ANT_REPORT_RECIPIENTS => builder.antithesis_report_recipients(Some(value.to_string())),
            ANT_SOURCE => builder.antithesis_source(Some(value.to_string())),
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

/// Resolve a launch / debugging-launch webhook response.
///
/// These webhooks return an HTTP status the OpenAPI spec under-documents (it
/// lists a single 2xx, while the live API has been seen to answer 200). The
/// generated client accepts only the documented code and surfaces any other 2xx
/// as `UnexpectedResponse`, so we treat **any** 2xx as success and decode the
/// body ourselves on that path. Genuine failures (4xx/5xx) are formatted as
/// errors, status first — `build.rs` leaves them undocumented, so the status is
/// always there to lead with.
async fn finish_launch<T: DeserializeOwned>(
    result: std::result::Result<ResponseValue<T>, ClientError<()>>,
) -> Result<T> {
    match result {
        Ok(response) => Ok(response.into_inner()),
        Err(ClientError::UnexpectedResponse(response)) if response.status().is_success() => {
            response
                .json::<T>()
                .await
                .wrap_err("parsing launch response body")
        }
        Err(err) => Err(format_api_client_error(err).await),
    }
}

fn launch_mvd_request(params: &Params) -> Result<generated::types::LaunchMvdRequest> {
    use generated::types::MvdParams;

    let map = params.as_map();
    let get = |key: &str| -> Result<Option<String>> {
        match map.get(key) {
            None => Ok(None),
            Some(value) => value
                .as_str()
                .map(|s| Some(s.to_string()))
                .ok_or_else(|| eyre!("debugging params must be strings: {key}")),
        }
    };

    let input_hash = get(ANT_DEBUGGING_INPUT_HASH)?
        .ok_or_else(|| eyre!("missing {ANT_DEBUGGING_INPUT_HASH}"))?;
    let vtime = get(ANT_DEBUGGING_VTIME)?.ok_or_else(|| eyre!("missing {ANT_DEBUGGING_VTIME}"))?;
    let event_description = get(ANT_EVENT_DESCRIPTION)?;
    let recipients = get(ANT_REPORT_RECIPIENTS)?;
    let run_id = get(ANT_DEBUGGING_RUN_ID)?;
    let session_id = get(ANT_DEBUGGING_SESSION_ID)?;

    // The MVD_Params schema is a oneOf, so the target run is identified by
    // exactly one of run_id or session_id. cmd_debug enforces this with a
    // friendly message before we get here; the both/neither arms below are a
    // defensive backstop.
    let typed_params = match (run_id, session_id) {
        (Some(run_id), None) => MvdParams::RunId {
            antithesis_debugging_input_hash: input_hash,
            antithesis_debugging_run_id: run_id,
            antithesis_debugging_vtime: vtime,
            antithesis_event_description: event_description,
            antithesis_report_recipients: recipients,
        },
        (None, Some(session_id)) => MvdParams::SessionId {
            antithesis_debugging_input_hash: input_hash,
            antithesis_debugging_session_id: session_id,
            antithesis_debugging_vtime: vtime,
            antithesis_event_description: event_description,
            antithesis_report_recipients: recipients,
        },
        (Some(_), Some(_)) => return Err(eyre!("specify exactly one of --run-id / --session-id")),
        (None, None) => return Err(eyre!("specify --run-id or --session-id")),
    };

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
        return Err(
            user_error(format!("{resource} is not available for run {run_id}"))
                .note(format!(
                    "the {resource} API was introduced in tenant version v{min_version}; \
                 run {run_id} was generated on an earlier version"
                ))
                .suggestion(format!(
                    "re-run {run_id} on a more recent version to access {resource}"
                )),
        );
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
    // Servers often echo the status reason at the front of the body
    // ("Bad Request — Bad request: invalid vtime"); drop the redundant echo
    // and keep only the informative remainder.
    let body = match body.get(..reason.len()) {
        Some(prefix) if !reason.is_empty() && prefix.eq_ignore_ascii_case(reason) => body
            [reason.len()..]
            .trim_start_matches([':', '-', ' '])
            .trim_start(),
        _ => body,
    };

    let mut msg = format!("API error: {status}");
    if !reason.is_empty() {
        msg.push(' ');
        msg.push_str(reason);
    }
    if !body.is_empty() {
        msg.push_str(" — ");
        msg.push_str(body);
    }
    // Carry the HTTP status structurally so callers can classify the failure
    // (e.g. "was this a 404?") without sniffing the rendered message string.
    let report = Report::new(ApiError {
        status,
        message: msg,
    });
    // The "what to check" for an auth failure is guidance, not part of the error
    // statement, so it rides along as a suggestion note.
    let report = if matches!(status, 401 | 403) {
        report.suggestion(
            "check that credentials have been configured correctly (either via running `snouty login` or by setting the ANTITHESIS_API_KEY (or ANTITHESIS_USERNAME/ANTITHESIS_PASSWORD) environment variable) for this tenant",
        )
    } else {
        report
    };
    // A 4xx is the user's to fix (bad credentials, unknown run id, invalid
    // filter, …), so it prints as a clean message — no backtrace, even under
    // `RUST_BACKTRACE`. 5xx and other statuses are genuine faults and keep theirs.
    if (400..500).contains(&status) {
        report.suppress_backtrace(true)
    } else {
        report
    }
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

/// Render a failed API call for the user, status first.
async fn format_api_client_error(err: ClientError<()>) -> Report {
    match classify_client_error(err).await {
        ApiFailure::Response { status, message } => format_api_error(status, &message),
        ApiFailure::Other(report) => report,
    }
}

/// A client error reduced to what a user-facing message is built from.
enum ApiFailure {
    /// The server answered. `status` is the transport status — the only
    /// authority on success or failure, per the API team's confirmation on
    /// #180 — and `message` the human-readable text of the body it came with.
    Response { status: u16, message: String },
    /// No usable response: a transport failure, or a success body snouty could
    /// not parse.
    Other(Report),
}

/// Classify a client error. Anything the server answered keeps its HTTP status:
/// `build.rs` documents no error responses, so the generated client never types
/// an error body and a failure can no longer degrade into the status-less
/// `InvalidResponsePayload` — whatever shape the body arrives in.
async fn classify_client_error(err: ClientError<()>) -> ApiFailure {
    match err {
        // Every HTTP failure lands here, carrying both its status and its body
        // exactly as the server sent them.
        ClientError::UnexpectedResponse(response) => {
            let status = response.status().as_u16();
            ApiFailure::Response {
                status,
                message: error_body_message(&read_error_body(response).await),
            }
        }
        // Unreachable: progenitor only emits this arm for a *documented* error
        // response, and `build.rs` asserts the generated client has none.
        ClientError::ErrorResponse(_) => {
            unreachable!("no error response is documented; see build.rs untype_error_responses")
        }
        ClientError::InvalidRequest(message) => {
            ApiFailure::Other(eyre!("invalid API request: {message}"))
        }
        ClientError::CommunicationError(err) => {
            ApiFailure::Other(eyre!(err).wrap_err("failed to contact API"))
        }
        ClientError::InvalidUpgrade(err) => {
            ApiFailure::Other(eyre!(err).wrap_err("invalid API upgrade response"))
        }
        ClientError::ResponseBodyError(err) => {
            ApiFailure::Other(eyre!(err).wrap_err("failed to read API response body"))
        }
        // Only a *success* body can land here: no error response is typed, so
        // none of them is ever parsed.
        ClientError::InvalidResponsePayload(body, err) => {
            let body = String::from_utf8_lossy(&body);
            ApiFailure::Other(if body.trim().is_empty() {
                eyre!("API returned an empty response body where a JSON payload was expected")
            } else {
                let snippet = format_payload_snippet(&body, err.line(), err.column());
                eyre!("invalid API response payload: {err}").section(snippet.header("payload:"))
            })
        }
        ClientError::Custom(message) => ApiFailure::Other(eyre!(message)),
    }
}

/// How much of an error response body to read. Error bodies are meant to be a
/// sentence; anything far past that is an intermediary's web page, and only the
/// first line of it will ever be shown. Bounding the read keeps a misbehaving
/// proxy from making snouty buffer an arbitrarily large body to print 200
/// characters of it.
const MAX_ERROR_BODY: usize = 4 * 1024;

/// Read up to [`MAX_ERROR_BODY`] of an error response, chunk by chunk, and drop
/// the rest. Truncating mid-character is fine — the result is only ever
/// displayed, and `from_utf8_lossy` renders a split character as `U+FFFD`.
async fn read_error_body(mut response: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BODY {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = MAX_ERROR_BODY - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Ok(None) => break,
            Err(err) => return format!("<failed to read response body: {err}>"),
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// The human-readable text of an error response body, as one terminal line.
///
/// The standard endpoints answer with `{"message": "…"}`, so unwrap that when
/// it's there. Anything else — an empty body, an HTML error page from an
/// intermediary, the launch endpoints' `{"statusCode", "runId"}` envelope, or a
/// shape the spec never described — is shown as it came, so the user still has
/// something to debug with. Echoing the launch envelope is worth it even though
/// its `statusCode` duplicates the status line: whether `runId` is present
/// distinguishes a rejection the test launcher produced from one an intermediary
/// made on its behalf.
///
/// Runs of whitespace collapse to a single space before the cut, so that an
/// indented error page spends the 200 characters on its text rather than on its
/// margins. [`sanitize`] is the repo's policy for untrusted text on one line; it
/// runs after the collapse, which leaves it nothing to do about newlines and
/// only control characters to escape.
fn error_body_message(body: &str) -> String {
    const MAX_LEN: usize = 200;

    let text = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| body.split_whitespace().collect::<Vec<_>>().join(" "));

    let text = sanitize(text.trim());
    match text.char_indices().nth(MAX_LEN) {
        Some((offset, _)) => format!("{}…", &text[..offset]),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::TryStreamExt;
    use tempfile::TempDir;
    use wiremock::matchers::{body_json, method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_api_optionally_with_cache(
        mock_server: &MockServer,
        cache_dir: Option<&TempDir>,
    ) -> AntithesisApi {
        test_api_at_url(mock_server.uri(), cache_dir)
    }

    /// Like [`test_api_optionally_with_cache`], but against a raw base URL —
    /// the transport-fault tests serve from a raw TCP listener, not wiremock.
    fn test_api_at_url(base_url: String, cache_dir: Option<&TempDir>) -> AntithesisApi {
        test_api_with_settings(&Settings::for_test_base_url(base_url), cache_dir)
    }

    /// Like [`test_api_at_url`], but with the caller's own [`Settings`] — for
    /// tests that vary a setting beyond the base URL.
    fn test_api_with_settings(settings: &Settings, cache_dir: Option<&TempDir>) -> AntithesisApi {
        AntithesisApi::build(
            settings,
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: "pass".to_owned(),
            },
            false,
            cache_dir.map_or(ResponseCache::Disabled, |d| {
                ResponseCache::Dir(d.path().to_path_buf())
            }),
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
        let err = ClientError::<()>::InvalidResponsePayload(Default::default(), parse_err);

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

    /// A served error, in the shape the generated client hands every HTTP
    /// failure to the formatters: no error response is documented, so the
    /// status and the raw body both survive.
    fn served_error(status: u16, body: &str) -> ClientError<()> {
        let response = http::Response::builder()
            .status(status)
            .body(body.to_owned())
            .map(reqwest::Response::from)
            .unwrap();
        ClientError::UnexpectedResponse(response)
    }

    // The status is the whole message when the server sent no body — which is
    // what the gateway does for some rejections, on any endpoint. The point of
    // #180 is that the status survives that; nothing further is added.
    #[tokio::test]
    async fn format_api_client_error_reports_the_status_for_an_empty_body() {
        for status in [401, 429, 502] {
            let report = format_api_client_error(served_error(status, "")).await;
            let debug = format!("{report:?}");

            assert!(
                debug.contains(&format!("API error: {status}")),
                "the transport status must survive an empty body, got: {debug}"
            );
        }
    }

    #[tokio::test]
    async fn format_api_client_error_shows_a_non_empty_body() {
        let report = format_api_client_error(served_error(400, "not json")).await;
        let debug = format!("{report:?}");

        assert!(debug.contains("API error: 400"), "got: {debug}");
        assert!(debug.contains("not json"), "got: {debug}");
    }

    // The read is bounded, so a proxy answering an error with a huge page cannot
    // make snouty buffer all of it just to print 200 characters.
    #[tokio::test]
    async fn read_error_body_stops_at_the_cap() {
        let mock_server =
            mock_endpoint("GET", "/api/version", 500, &"x".repeat(MAX_ERROR_BODY * 3)).await;
        let response = reqwest::get(format!("{}/api/version", mock_server.uri()))
            .await
            .unwrap();

        assert_eq!(read_error_body(response).await.len(), MAX_ERROR_BODY);
    }

    // A body under the cap is returned whole, so the bound never truncates a
    // real error message.
    #[tokio::test]
    async fn read_error_body_keeps_a_short_body_intact() {
        let mock_server = mock_endpoint("GET", "/api/version", 500, "upstream is down").await;
        let response = reqwest::get(format!("{}/api/version", mock_server.uri()))
            .await
            .unwrap();

        assert_eq!(read_error_body(response).await, "upstream is down");
    }

    // The standard `{"message": …}` envelope is unwrapped; anything else is
    // shown verbatim so the user can still see what the server said.
    #[test]
    fn error_body_message_unwraps_the_standard_envelope() {
        assert_eq!(
            error_body_message(r#"{"message":"limit must be 1..100"}"#),
            "limit must be 1..100"
        );
        assert_eq!(
            error_body_message(r#"{"statusCode":404,"runId":null}"#),
            r#"{"statusCode":404,"runId":null}"#
        );
        assert_eq!(error_body_message(""), "");
    }

    // An intermediary's error page is multi-line and can be enormous; collapse
    // it onto one line and cap it so it stays a usable one-line diagnostic.
    #[test]
    fn error_body_message_collapses_and_truncates_html() {
        let message = error_body_message(&format!(
            "<html>\n  <body>{}</body>\n</html>\n",
            "x".repeat(500)
        ));
        assert!(message.starts_with("<html> <body>xxx"), "got: {message}");
        assert!(!message.contains('\n'), "got: {message}");
        assert!(message.ends_with('…'), "got: {message}");
        assert_eq!(message.chars().count(), 201);
    }

    // The body goes straight to the terminal, so it gets the same control-char
    // treatment as every other piece of untrusted API text: an escape sequence
    // in an intermediary's error page must not reach the user's terminal
    // unescaped.
    #[test]
    fn error_body_message_escapes_control_characters() {
        assert_eq!(
            error_body_message("oops\u{1b}[31m\u{7}"),
            r"oops\x1B[31m\x07"
        );
        // JSON-escaped inside the envelope, so it parses; the unwrapped message
        // is sanitized just like an unparsed body.
        assert_eq!(
            error_body_message(r#"{"message":"bad\u0007request"}"#),
            r"bad\x07request"
        );
    }

    #[tokio::test]
    async fn format_api_client_error_keeps_snippet_for_non_empty_invalid_payload() {
        let body: &[u8] = b"not json";
        let parse_err = serde_json::from_slice::<serde_json::Value>(body).unwrap_err();
        let err = ClientError::<()>::InvalidResponsePayload(body.to_vec().into(), parse_err);

        let report = format_api_client_error(err).await;
        // The message is the bare statement …
        let message = format!("{report}");
        assert!(message.starts_with("invalid API response payload: "));
        // … and the payload snippet (with its caret) rides along as a section,
        // rendered by the full report rather than the message.
        let full = format!("{report:?}");
        assert!(full.contains("not json"), "got: {full}");
        assert!(full.contains('^'), "got: {full}");
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
        format_request(&request, &HeaderMap::new(), &mut out);

        assert!(out.contains("<3 bytes>"));
    }

    #[test]
    fn with_base_url_trims_trailing_slash() {
        let api = AntithesisApi::build(
            &Settings::for_test_base_url("http://example.com/".to_owned()),
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: "pass".to_owned(),
            },
            true,
            ResponseCache::Disabled,
        )
        .unwrap();
        assert_eq!(api.base_url, "http://example.com");
    }

    #[test]
    fn with_base_url_strips_legacy_api_suffix() {
        let api = AntithesisApi::build(
            &Settings::for_test_base_url("http://example.com/api/v1/".to_owned()),
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: "pass".to_owned(),
            },
            true,
            ResponseCache::Disabled,
        )
        .unwrap();
        assert_eq!(api.base_url, "http://example.com");
    }

    #[tokio::test]
    async fn launch_test_sends_snouty_user_agent() {
        let mock_server = mock_launch_test(202, LAUNCH_OK_BODY).await;

        let api = test_api_optionally_with_cache(&mock_server, None);
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
        let mock_server = mock_launch_test(202, LAUNCH_OK_BODY).await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

        let response = api.launch_test("basic_test", &params).await.unwrap();
        let requests = mock_server.received_requests().await.unwrap();

        assert_eq!(response.run_id.as_deref(), Some("run-123"));
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

    // The launch webhooks answer an undocumented HTTP 200 whose body claims 202.
    // snouty accepts any 2xx as success and reports the transport status.
    #[tokio::test]
    async fn launch_test_accepts_200_webhook_envelope() {
        let mock_server = mock_launch_test(200, LAUNCH_OK_BODY).await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

        let response = api.launch_test("basic_test", &params).await.unwrap();
        assert_eq!(response.run_id.as_deref(), Some("run-123"));
    }

    /// Params for a debug launch against a fixed run.
    fn debug_params() -> Params {
        Params::from_key_value_pairs([
            "antithesis.debugging.run_id=a2a4-53-1",
            "antithesis.debugging.input_hash=-1",
            "antithesis.debugging.vtime=1.0",
        ])
        .unwrap()
    }

    /// A mock server answering one endpoint with a fixed status and body. The
    /// endpoint-specific wrappers below name the routes the tests care about.
    async fn mock_endpoint(http_method: &str, route: &str, status: u16, body: &str) -> MockServer {
        let mock_server = MockServer::start().await;
        Mock::given(method(http_method))
            .and(path(route.to_owned()))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .expect(1)
            .mount(&mock_server)
            .await;
        mock_server
    }

    async fn mock_debug_launch(status: u16, body: &str) -> MockServer {
        mock_endpoint("POST", "/api/v1/launch/debugging", status, body).await
    }

    async fn mock_launch_test(status: u16, body: &str) -> MockServer {
        mock_endpoint("POST", "/api/v1/launch/basic_test", status, body).await
    }

    /// The launch envelope the live webhook returns on success. Its body-level
    /// `statusCode` deliberately disagrees with the HTTP status the mocks pair
    /// it with, since snouty must ignore the body's copy (#180).
    const LAUNCH_OK_BODY: &str = r#"{"runId":"run-123","statusCode":202}"#;

    // The body's own statusCode is ignored outright (#180): snouty reads the run
    // id and nothing else, so a body claiming 202 over an HTTP 200 has no way to
    // influence what is reported.
    #[tokio::test]
    async fn launch_debugging_ignores_the_body_status_code() {
        let mock_server = mock_debug_launch(200, r#"{"runId":"x","statusCode":202}"#).await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        let response = api.launch_debugging(&debug_params()).await.unwrap();
        assert_eq!(response.run_id.as_deref(), Some("x"));
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({"runId": "x"}),
            "the launch --json contract is the run id alone"
        );
    }

    // …and the converse, which is the regression guard for the removed
    // "recover when the body self-reports a 2xx" gate: a body claiming success
    // can never turn an error status into a launch.
    #[tokio::test]
    async fn launch_debugging_rejects_success_body_on_error_status() {
        for status in [400, 403, 500] {
            let mock_server = mock_debug_launch(status, r#"{"statusCode":202}"#).await;
            let api = test_api_optionally_with_cache(&mock_server, None);

            let report = api.launch_debugging(&debug_params()).await.unwrap_err();
            assert_eq!(crate::error::api_error_status(&report), Some(status));
        }
    }

    // Error bodies are taken as they come — empty, HTML, the live envelope, or
    // the pre-58.6 `{message}` shape — and every one keeps the HTTP status.
    #[tokio::test]
    async fn launch_test_reports_the_status_for_any_error_body() {
        for body in [
            "",
            "<html><body>404 Not Found</body></html>",
            r#"{"statusCode":404,"runId":null}"#,
        ] {
            let mock_server = mock_launch_test(404, body).await;
            let api = test_api_optionally_with_cache(&mock_server, None);
            let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

            let report = api.launch_test("basic_test", &params).await.unwrap_err();
            assert_eq!(
                crate::error::api_error_status(&report),
                Some(404),
                "body {body:?} lost its status"
            );
            assert!(
                format!("{report:#}").contains("API error: 404 Not Found"),
                "body {body:?} produced: {report:#}"
            );
        }
    }

    // The pre-58.6 launch endpoints answered with the standard `{message}`
    // envelope, which `Launch_Error_Response` cannot represent. Surface the text
    // anyway rather than reducing the failure to a bare status line.
    #[tokio::test]
    async fn launch_test_surfaces_the_error_message_body() {
        let mock_server = mock_launch_test(404, r#"{"message":"bad request"}"#).await;
        let api = test_api_optionally_with_cache(&mock_server, None);
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

        let report = api.launch_test("basic_test", &params).await.unwrap_err();
        assert_eq!(
            format!("{report:#}"),
            "API error: 404 Not Found — bad request"
        );
    }

    #[tokio::test]
    async fn launch_debugging_accepts_200_webhook_envelope() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v1/launch/debugging"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statusCode": 202,
                "runId": "debug-run-123"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let params = Params::from_key_value_pairs([
            "antithesis.debugging.run_id=a2a4-53-1",
            "antithesis.debugging.input_hash=-1",
            "antithesis.debugging.vtime=1.0",
        ])
        .unwrap();

        let response = api.launch_debugging(&params).await.unwrap();
        assert_eq!(response.run_id.as_deref(), Some("debug-run-123"));
    }

    // A success body may legally omit runId under the tenant-58.6 schema. When it
    // arrives via the undocumented-200 fallback path (not the documented-202
    // deserializer), we must still accept it as success with run_id: None rather
    // than hard-erroring on the missing field.
    #[tokio::test]
    async fn launch_debugging_accepts_200_without_run_id() {
        let mock_server = mock_debug_launch(200, r#"{"statusCode":202}"#).await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        let response = api.launch_debugging(&debug_params()).await.unwrap();
        assert_eq!(response.run_id, None);
    }

    // The documented 202 body and the live webhook envelope have converged on
    // `{ statusCode, runId }`, which the generated client parses directly.
    #[tokio::test]
    async fn launch_debugging_accepts_202_documented() {
        let mock_server =
            mock_debug_launch(202, r#"{"statusCode":202,"runId":"debug-run-456"}"#).await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        let response = api.launch_debugging(&debug_params()).await.unwrap();
        assert_eq!(response.run_id.as_deref(), Some("debug-run-456"));
    }

    // A documented error status carries the `Launch_Error_Response` envelope
    // (`{ statusCode, runId }`). Its `runId` is informational only and must never
    // be mistaken for a successful launch: the non-2xx HTTP status keeps this an
    // error even though the body parses cleanly. The body is still echoed for
    // debugging, so assert on the outcome rather than on the absent run id.
    #[tokio::test]
    async fn launch_debugging_rejects_error_body_carrying_run_id() {
        let mock_server =
            mock_debug_launch(403, r#"{"statusCode":403,"runId":"not-a-launch"}"#).await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        let report = api.launch_debugging(&debug_params()).await.unwrap_err();
        assert_eq!(crate::error::api_error_status(&report), Some(403));
    }

    async fn mock_version(status: u16, body: &str) -> MockServer {
        mock_endpoint("GET", "/api/version", status, body).await
    }

    #[tokio::test]
    async fn get_version_reads_the_documented_body() {
        let mock_server = mock_version(
            200,
            r#"{"latest_api_version":"v1","release_version":"58.6"}"#,
        )
        .await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        let version = api.get_version().await.unwrap();
        assert_eq!(version.latest_api_version, "v1");
        assert_eq!(version.release_version, "58.6");
    }

    // The gateway rejects a bad token with a documented status and an *empty*
    // body (content-type: text/plain). Before the transport-level normalization
    // that body failed `Error_Response` parsing, which cost the status and left
    // `snouty doctor` reporting the API as unreachable instead of as rejecting
    // authentication. Every non-conforming error body must keep its status:
    // empty, an intermediary's HTML page, or an unexpected JSON shape.
    //
    // The mocks' `.expect(1)` also pins the retry contract: a served error
    // status — including 429 and 5xx — is final and never re-sent.
    #[tokio::test]
    async fn get_version_keeps_the_status_for_non_conforming_error_bodies() {
        let cases = [
            (401, ""),
            (401, "<html><body>Unauthorized</body></html>"),
            (406, r#"{"statusCode":406}"#),
            (429, ""),
            (500, "<html><body>502 upstream</body></html>"),
        ];
        for (status, body) in cases {
            let mock_server = mock_version(status, body).await;
            let api = test_api_optionally_with_cache(&mock_server, None);

            match api.get_version().await {
                Err(VersionError::Http(code)) => assert_eq!(code, status),
                other => panic!("expected Http({status}) for body {body:?}, got {other:?}"),
            }
        }
    }

    // A 404 is undocumented for this operation, so it never went through the
    // typed-body path; keep it covered so the normalization can't regress it.
    #[tokio::test]
    async fn get_version_keeps_the_status_for_an_undocumented_404() {
        let mock_server = mock_version(404, "").await;
        let api = test_api_optionally_with_cache(&mock_server, None);

        match api.get_version().await {
            Err(VersionError::Http(404)) => {}
            other => panic!("expected Http(404), got {other:?}"),
        }
    }

    /// A raw TCP server for transport-fault tests. It resets its first
    /// `resets` connections — it reads one byte, then drops the socket with
    /// the rest of the request unread, so the kernel sends an RST and the
    /// client sees a connection reset rather than a clean close. Every later
    /// connection reads the request, writes `response`, and closes. Returns
    /// the base URL and a counter of accepted connections — the counter is
    /// how the retry tests observe how many attempts reached the server.
    async fn raw_server(resets: usize, response: String) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&connections);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let n = counter.fetch_add(1, Ordering::SeqCst);
                if n < resets {
                    let mut byte = [0u8; 1];
                    let _ = socket.read(&mut byte).await;
                    drop(socket);
                    continue;
                }
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (base_url, connections)
    }

    /// A server whose first `resets` connections reset and whose later ones
    /// answer `GET /api/version` completely.
    async fn reset_then_serve_version(resets: usize) -> (String, Arc<AtomicUsize>) {
        let body = r#"{"latest_api_version":"v1","release_version":"60"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        raw_server(resets, response).await
    }

    /// Assert the version probe classified the failure as unreachable.
    async fn expect_unreachable(api: &AntithesisApi) {
        match api.get_version().await {
            Err(VersionError::Unreachable(_)) => {}
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }

    // A transport-level failure on a GET is retried: the first connection is
    // reset, the second attempt succeeds, and the caller never sees the blip.
    // The same holds with the response cache enabled — retry sits under the
    // cache, so a cache miss's network fetch is retried too.
    #[tokio::test]
    async fn get_retries_a_transport_failure() {
        let cache_dir = TempDir::new().unwrap();
        for cache_dir in [None, Some(&cache_dir)] {
            let (base_url, connections) = reset_then_serve_version(1).await;
            let api = test_api_at_url(base_url, cache_dir);

            let version = api.get_version().await.unwrap();

            assert_eq!(version.release_version, "60");
            assert_eq!(connections.load(Ordering::SeqCst), 2);
        }
    }

    // Retries are bounded: when every attempt fails at the transport level the
    // error surfaces after 1 + MAX_TRANSIENT_RETRIES attempts, classified as
    // unreachable (no HTTP status), not swallowed.
    #[tokio::test]
    async fn get_surfaces_a_transport_failure_after_bounded_retries() {
        let (base_url, connections) = reset_then_serve_version(usize::MAX).await;
        let api = test_api_at_url(base_url, None);

        expect_unreachable(&api).await;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1 + MAX_TRANSIENT_RETRIES as usize
        );
    }

    // A connection dropped while the body downloads is not retried: the
    // response headers already arrived, so the retry layer has already
    // returned. The failure surfaces after a single attempt.
    #[tokio::test]
    async fn get_body_truncation_is_not_retried() {
        let (base_url, connections) = raw_server(
            0,
            "HTTP/1.1 200 OK\r\ncontent-length: 4096\r\n\r\n{\"latest".to_owned(),
        )
        .await;
        let api = test_api_at_url(base_url, None);

        expect_unreachable(&api).await;
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    // Mutations are never replayed: a launch POST that dies to a transport
    // failure is sent exactly once.
    #[tokio::test]
    async fn launch_post_is_not_retried_on_transport_failure() {
        let (base_url, connections) = reset_then_serve_version(usize::MAX).await;
        let api = test_api_at_url(base_url, None);
        let params = Params::from_key_value_pairs(["antithesis.duration=30"]).unwrap();

        let report = api.launch_test("basic_test", &params).await.unwrap_err();

        assert!(
            crate::error::api_error_status(&report).is_none(),
            "a transport failure carries no HTTP status, got: {report:#}"
        );
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    // The retry layer trusts that every GET in the API is a pure read. Walk
    // openapi.json and pin the spec's full method↔operation mapping, so a
    // regenerated spec with a new or changed operation — a new GET included —
    // forces this safety review to run again.
    #[test]
    fn every_operation_has_a_reviewed_method() {
        let spec: serde_json::Value = serde_json::from_str(include_str!("openapi.json")).unwrap();
        let methods = [
            "get", "post", "put", "patch", "delete", "head", "options", "trace",
        ];
        let mut operations: Vec<String> = spec["paths"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|operations| operations.as_object().unwrap())
            .filter(|(method, _)| methods.contains(&method.as_str()))
            .map(|(method, operation)| {
                format!("{method} {}", operation["operationId"].as_str().unwrap())
            })
            .collect();
        operations.sort_unstable();
        assert_eq!(
            operations,
            [
                "get getRun",
                "get getRunBuildLogs",
                "get getRunLogs",
                "get getVersion",
                "get listRunProperties",
                "get listRuns",
                "get searchRunEvents",
                "post executeCommand",
                "post launchMvd",
                "post launchTest",
                "post search",
            ],
            "openapi.json changed its method↔operation mapping; re-verify \
             that every GET is a pure read before trusting the retry gating"
        );
    }

    // The cached operations' stores rely on the generated client accepting
    // only the documented success status. Pin that status to exactly 200, so
    // a spec refresh that documents another 2xx for a cached operation
    // forces this admission review to run again.
    #[test]
    fn cached_operations_document_only_a_200() {
        let spec: serde_json::Value = serde_json::from_str(include_str!("openapi.json")).unwrap();
        let cached = [
            "getRun",
            "getRunBuildLogs",
            "getRunLogs",
            "listRunProperties",
        ];
        let mut seen = Vec::new();
        for operations in spec["paths"].as_object().unwrap().values() {
            for operation in operations.as_object().unwrap().values() {
                let Some(id) = operation["operationId"].as_str() else {
                    continue;
                };
                if !cached.contains(&id) {
                    continue;
                }
                seen.push(id.to_owned());
                let statuses: Vec<&String> = operation["responses"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .filter(|status| status.starts_with('2'))
                    .collect();
                assert_eq!(
                    statuses,
                    ["200"],
                    "{id} documents 2xx responses other than 200; \
                     re-verify the cache admission before trusting its store"
                );
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, cached, "a cached operation left the spec");
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

        let api = test_api_optionally_with_cache(&mock_server, None);

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default(), 100)
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

        let api = test_api_optionally_with_cache(&mock_server, None);

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default(), 100)
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

        let api = test_api_optionally_with_cache(&mock_server, None);

        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default(), 100)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(runs.is_empty());
    }

    #[tokio::test]
    async fn stream_runs_requests_the_supplied_page_limit() {
        let mock_server = MockServer::start().await;

        // The page limit is forwarded to the API rather than fetching 100 and
        // trimming client-side.
        Mock::given(method("GET"))
            .and(path("/api/v0/runs"))
            .and(query_param("limit", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let runs = api
            .stream_runs_filtered(&RunsFilterOptions::default(), 5)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(runs.is_empty());
    }

    #[test]
    fn format_api_error_carries_structured_status() {
        use crate::error::api_error_status;
        // The status is read structurally, not sniffed from the message — so a
        // 500 whose body mentions "404" still classifies as a 500.
        assert_eq!(
            api_error_status(&format_api_error(404, "run not found")),
            Some(404)
        );
        assert_eq!(
            api_error_status(&format_api_error(500, "upstream returned a 404 page")),
            Some(500)
        );
        // And the rendered message still contains the body for the user.
        let rendered = format!("{:#}", format_api_error(404, "run not found"));
        assert!(rendered.contains("API error: 404"));
        assert!(rendered.contains("run not found"));
    }

    #[test]
    fn format_api_error_dedupes_reason_echoed_in_body() {
        // "Bad Request — Bad request: …" reads twice; the echo is dropped.
        let rendered = format!(
            "{:#}",
            format_api_error(400, "Bad request: Invalid input_hash or vtime")
        );
        assert_eq!(
            rendered,
            "API error: 400 Bad Request — Invalid input_hash or vtime"
        );
        // A body that is nothing but the reason echo adds nothing.
        let rendered = format!("{:#}", format_api_error(400, "Bad Request"));
        assert_eq!(rendered, "API error: 400 Bad Request");
        // Unrelated bodies pass through untouched.
        let rendered = format!("{:#}", format_api_error(400, "vtime out of range"));
        assert_eq!(rendered, "API error: 400 Bad Request — vtime out of range");
    }

    // ---- logical response cache ------------------------------------------
    //
    // Key construction and the stream tee are unit-tested in
    // `crate::api_cache`; these tests pin each handler's caching end to end:
    // lookup before send, admission per handler, commit only on a fully read
    // stream.

    fn run_detail_body(status: &str) -> serde_json::Value {
        serde_json::json!({
            "run_id": "run-1",
            "status": status,
            "created_at": "2025-03-20T02:00:00Z",
            "launcher": "nightly"
        })
    }

    async fn read_stream(stream: crate::jsonl::JsonStream) -> Vec<serde_json::Value> {
        stream.try_collect::<Vec<_>>().await.unwrap()
    }

    /// A 200 response stamped with the `Cache-Control` the live API sends on
    /// a cacheable read ([`CACHEABLE_CACHE_CONTROL`]). The cache admits
    /// nothing without it.
    fn cacheable() -> ResponseTemplate {
        use crate::testutils::CACHEABLE_CACHE_CONTROL;
        ResponseTemplate::new(200).insert_header("cache-control", CACHEABLE_CACHE_CONTROL)
    }

    /// A moment with a pinned end vtime, past every mock log line: the
    /// stream is admissible to the cache and passes the truncation whole.
    fn logs_moment() -> Moment {
        Moment {
            input_hash: "hash-1".to_owned(),
            vtime: "500.0".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn terminal_run_detail_is_served_from_the_cache() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(cacheable().set_body_json(run_detail_body("completed")))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        let first = api.get_run("run-1").await.unwrap().untag();
        let second = api.get_run("run-1").await.unwrap().untag();

        assert_eq!(first.run_id, "run-1");
        assert_eq!(second.run_id, "run-1");
    }

    #[tokio::test]
    async fn live_run_detail_is_not_cached() {
        // Cacheable headers alone are not enough: the handler's own check
        // (a terminal status) must also pass.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(cacheable().set_body_json(run_detail_body("in_progress")))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        api.get_run("run-1").await.unwrap();
        api.get_run("run-1").await.unwrap();
    }

    #[tokio::test]
    async fn a_response_without_cacheable_headers_is_not_cached() {
        // The logical check (a terminal status) alone is not enough: the
        // server's cache headers must also allow caching.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(run_detail_body("completed")))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        api.get_run("run-1").await.unwrap();
        api.get_run("run-1").await.unwrap();
    }

    #[tokio::test]
    async fn the_header_requirement_has_an_opt_out() {
        // `api_cache_respect_headers = false`: a response without cacheable
        // headers is admitted on the logical checks alone.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(run_detail_body("completed")))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_with_settings(
            &Settings::builder()
                .base_url(&mock_server.uri())
                .api_cache_respect_headers(false)
                .build(),
            Some(&cache_dir),
        );

        api.get_run("run-1").await.unwrap();
        api.get_run("run-1").await.unwrap();
    }

    #[tokio::test]
    async fn run_logs_replay_from_the_cache_once_fully_read() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/logs"))
            .respond_with(cacheable().set_body_string("{\"text\":\"log line\"}\n"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        let first = api
            .get_run_logs("run-1", logs_moment(), None)
            .await
            .unwrap()
            .untag();
        assert_eq!(
            read_stream(first).await,
            [serde_json::json!({"text": "log line"})]
        );
        let second = api
            .get_run_logs("run-1", logs_moment(), None)
            .await
            .unwrap()
            .untag();
        assert_eq!(
            read_stream(second).await,
            [serde_json::json!({"text": "log line"})]
        );
    }

    #[tokio::test]
    async fn run_logs_without_an_end_vtime_are_not_cached() {
        // The `0` placeholder means "no end vtime": the branch can still
        // grow, so every request hits the server.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/logs"))
            .respond_with(cacheable().set_body_string("{\"text\":\"log line\"}\n"))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        let moment = || Moment {
            input_hash: "hash-1".to_owned(),
            vtime: VTime::ZERO,
        };
        for _ in 0..2 {
            let tagged = api.get_run_logs("run-1", moment(), None).await.unwrap();
            assert!(matches!(tagged.tag(), CachePolicy::Uncacheable));
            assert_eq!(
                read_stream(tagged.untag()).await,
                [serde_json::json!({"text": "log line"})]
            );
        }
    }

    #[tokio::test]
    async fn run_logs_truncate_at_the_end_vtime_and_cache_the_truncation() {
        // The server streams the branch past the end vtime; snouty cuts the
        // stream at the first line past it, and the cache stores the
        // truncated stream.
        let line = |vtime: &str| {
            format!(r#"{{"text":"at {vtime}","moment":{{"input_hash":"-1","vtime":"{vtime}"}}}}"#)
        };
        let body = format!("{}\n{}\n{}\n", line("1.0"), line("2.0"), line("3.0"));
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/logs"))
            .respond_with(cacheable().set_body_string(body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        let moment = || Moment {
            input_hash: "-1".to_owned(),
            // The end vtime is inclusive: the moment's own events must print.
            vtime: "2.0".parse().unwrap(),
        };
        let expected: Vec<serde_json::Value> = [line("1.0"), line("2.0")]
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        // First read truncates the live stream and commits it; the second is
        // the cached replay of the truncated stream.
        for _ in 0..2 {
            let stream = api
                .get_run_logs("run-1", moment(), None)
                .await
                .unwrap()
                .untag();
            assert_eq!(read_stream(stream).await, expected);
        }
    }

    // ---- end-vtime truncation ---------------------------------------------

    /// One parsed log line at `vtime`.
    fn log_value(vtime: &str) -> serde_json::Value {
        serde_json::json!({"text": format!("at {vtime}"), "moment": {"input_hash": "-1", "vtime": vtime}})
    }

    #[tokio::test]
    async fn truncation_keeps_lines_at_or_before_the_end_and_stops_the_source() {
        let source = stream::iter([
            Ok(log_value("1.0")),
            Ok(log_value("2.0")),
            Ok(log_value("3.0")),
        ])
        // The tail panics if polled: reaching the line past the end must
        // end the stream, not read on.
        .chain(stream::once(async {
            panic!("the source was polled past the truncation point")
        }))
        .boxed();
        let kept = read_stream(truncate_at_end_vtime(source, "2.0".parse().unwrap())).await;
        // The end vtime is inclusive: the moment's own events must print.
        assert_eq!(kept, [log_value("1.0"), log_value("2.0")]);
    }

    #[tokio::test]
    async fn truncation_passes_lines_without_a_vtime_through() {
        // A Stream_Error has no moment; it must reach the user, not be
        // swallowed by the truncation.
        let error_line = serde_json::json!({"error": "boom"});
        let source = stream::iter([Ok(log_value("1.0")), Ok(error_line.clone())]).boxed();
        let kept = read_stream(truncate_at_end_vtime(source, "5.0".parse().unwrap())).await;
        assert_eq!(kept, [log_value("1.0"), error_line]);
    }

    #[tokio::test]
    async fn a_partially_read_body_is_never_committed() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/logs"))
            .respond_with(cacheable().set_body_string("{\"text\":\"log line\"}\n"))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        // Drop the stream without reading it: nothing may be committed.
        let abandoned = api
            .get_run_logs("run-1", logs_moment(), None)
            .await
            .unwrap()
            .untag();
        drop(abandoned);

        // The next request must hit the server again.
        let replay = api
            .get_run_logs("run-1", logs_moment(), None)
            .await
            .unwrap()
            .untag();
        assert_eq!(
            read_stream(replay).await,
            [serde_json::json!({"text": "log line"})]
        );
    }

    #[tokio::test]
    async fn oversized_bodies_are_not_cached() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/logs"))
            .respond_with(cacheable().set_body_string("{\"text\":\"log line\"}\n"))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_with_settings(
            &Settings::builder()
                .base_url(&mock_server.uri())
                .api_cache_max_file_size(4)
                .build(),
            Some(&cache_dir),
        );

        for _ in 0..2 {
            let stream = api
                .get_run_logs("run-1", logs_moment(), None)
                .await
                .unwrap()
                .untag();
            assert_eq!(
                read_stream(stream).await,
                [serde_json::json!({"text": "log line"})]
            );
        }
    }

    #[tokio::test]
    async fn the_run_list_is_never_cached() {
        // The live API sends `no-cache` on the list; the test stamps
        // cacheable headers anyway to prove the exclusion is by design (the
        // list is not a cached handler), not by the header gate.
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs"))
            .respond_with(cacheable().set_body_json(serde_json::json!({
                "data": [run_detail_body("completed")],
                "next_cursor": null
            })))
            .expect(2)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        for _ in 0..2 {
            let runs = api
                .stream_runs_filtered(&RunsFilterOptions::default(), 100)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(runs.len(), 1);
        }
    }

    #[tokio::test]
    async fn build_logs_replay_from_the_cache_once_fully_read() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/build_logs"))
            .respond_with(cacheable().set_body_string("{\"text\":\"built\"}\n"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        for _ in 0..2 {
            let stream = api.get_run_build_logs("run-1").await.unwrap().untag();
            assert_eq!(
                read_stream(stream).await,
                [serde_json::json!({"text": "built"})]
            );
        }
    }

    // Properties cache page by page: the second listing replays both
    // pages, cursor chaining included.
    #[tokio::test]
    async fn run_properties_replay_from_the_cache_per_page() {
        let mock_server = MockServer::start().await;
        let page = |name: &str, cursor: Option<&str>| {
            serde_json::json!({
                "data": [{
                    "name": name,
                    "status": "Passing",
                    "is_event": false,
                    "is_existential": true,
                    "is_universal": false
                }],
                "next_cursor": cursor
            })
        };
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/properties"))
            .and(query_param_is_missing("after"))
            .respond_with(cacheable().set_body_json(page("first", Some("c1"))))
            .expect(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/properties"))
            .and(query_param("after", "c1"))
            .respond_with(cacheable().set_body_json(page("second", None)))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        for _ in 0..2 {
            let names: Vec<String> = api
                .stream_run_properties("run-1", None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(|p| p.name().to_owned())
                .collect();
            assert_eq!(names, ["first", "second"]);
        }
    }

    #[tokio::test]
    async fn error_responses_are_not_cached() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "message": "not yet"
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(cacheable().set_body_json(run_detail_body("completed")))
            .expect(1)
            .mount(&mock_server)
            .await;

        let cache_dir = TempDir::new().unwrap();
        let api = test_api_optionally_with_cache(&mock_server, Some(&cache_dir));

        let error = api.get_run("run-1").await.unwrap_err();
        assert!(error.to_string().contains("404"), "got: {error:#}");
        // The 404 was not cached; this hits the server, succeeds, and caches.
        api.get_run("run-1").await.unwrap();
        // …and this one replays from the cache (the mocks allow no third hit).
        api.get_run("run-1").await.unwrap();
    }

    #[tokio::test]
    async fn cache_errors_degrade_to_misses() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(cacheable().set_body_json(run_detail_body("completed")))
            .expect(2)
            .mount(&mock_server)
            .await;

        // A cache "directory" that is actually a file: every cache read and
        // write fails, and every request must still succeed via the server.
        let dir = TempDir::new().unwrap();
        let bogus = dir.path().join("not-a-dir");
        std::fs::write(&bogus, b"").unwrap();
        let api = AntithesisApi::build(
            &Settings::for_test_base_url(mock_server.uri()),
            AuthenticationInfo::Password {
                username: "user".to_owned(),
                password: "pass".to_owned(),
            },
            false,
            ResponseCache::Dir(bogus),
        )
        .unwrap();

        api.get_run("run-1").await.unwrap();
        api.get_run("run-1").await.unwrap();
    }

    #[tokio::test]
    async fn oauth_401_triggers_refresh_and_retries_once() {
        use wiremock::matchers::header;

        let mock_server = MockServer::start().await;

        // Refresh endpoint: given the current refresh token, hands back a brand
        // new access + refresh token pair.
        Mock::given(method("POST"))
            .and(path("/auth/cli/refresh"))
            .and(header("authorization", "Bearer old-refresh-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "antithesis_token": "new-access-token",
                "refresh_token": "new-refresh-token",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        // The API rejects the stale access token …
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .and(header("authorization", "Bearer old-access-token"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&mock_server)
            .await;

        // … and accepts the refreshed one on the retry.
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .and(header("authorization", "Bearer new-access-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "run_id": "run-1",
                "status": "completed",
                "created_at": "2025-03-20T02:00:00Z",
                "launcher": "nightly"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Persist the refreshed credential to a temp credentials file so the
        // write-back can be checked without touching the real config.
        let creds_dir = TempDir::new().unwrap();
        let creds_path = creds_dir.path().join("credentials.toml");
        let auth = AuthenticationInfo::oauth_for_test(
            "old-access-token",
            Some("old-refresh-token"),
            crate::auth::OAuthRefreshInfo::CredentialsFile {
                path: creds_path.clone(),
                profile: None,
            },
        );

        let api = AntithesisApi::build(
            &Settings::for_test_base_url(mock_server.uri()),
            auth,
            false,
            ResponseCache::Disabled,
        )
        .unwrap();

        // The initial 401 drives a refresh and a single retry, which succeeds.
        let run = api.get_run("run-1").await.unwrap().untag();
        assert_eq!(run.run_id, "run-1");

        // Request sequence: stale token → refresh → retry with the new token.
        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 3, "expected initial + refresh + retry");
        let auth_header = |i: usize| {
            requests[i]
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
        };
        assert_eq!(requests[0].url.path(), "/api/v0/runs/run-1");
        assert_eq!(auth_header(0), "Bearer old-access-token");
        assert_eq!(requests[1].url.path(), "/auth/cli/refresh");
        assert_eq!(auth_header(1), "Bearer old-refresh-token");
        assert_eq!(requests[2].url.path(), "/api/v0/runs/run-1");
        assert_eq!(auth_header(2), "Bearer new-access-token");

        // The refreshed tokens were persisted to the file.
        let persisted = std::fs::read_to_string(&creds_path).unwrap();
        assert!(persisted.contains("new-access-token"), "got:\n{persisted}");
        assert!(persisted.contains("new-refresh-token"), "got:\n{persisted}");
    }

    #[tokio::test]
    async fn oauth_without_refresh_token_does_not_retry_on_401() {
        let mock_server = MockServer::start().await;

        // `.expect(1)` asserts the endpoint is hit exactly once — i.e. no retry.
        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&mock_server)
            .await;

        // OAuth, but with no refresh token — there's nothing to refresh with, so
        // `can_refresh()` is false and the 401 must pass straight through.
        let auth = AuthenticationInfo::oauth_for_test(
            "access-token",
            None,
            crate::auth::OAuthRefreshInfo::Keychain {
                entry_name: "unused".to_owned(),
            },
        );
        let api = AntithesisApi::build(
            &Settings::for_test_base_url(mock_server.uri()),
            auth,
            false,
            ResponseCache::Disabled,
        )
        .unwrap();

        let result = api.get_run("run-1").await;
        assert!(
            result.is_err(),
            "a 401 with no refresh token should surface as an error, not be retried away"
        );

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "no retry should be attempted");
        assert_eq!(requests[0].url.path(), "/api/v0/runs/run-1");
        assert!(
            requests.iter().all(|r| r.url.path() != "/auth/cli/refresh"),
            "no refresh call should be made"
        );
    }

    #[tokio::test]
    async fn api_key_credential_does_not_retry_on_401() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&mock_server)
            .await;

        // An API key can't refresh, so `can_refresh()` is false: no clone, no
        // retry, no refresh call.
        let api = AntithesisApi::build(
            &Settings::for_test_base_url(mock_server.uri()),
            AuthenticationInfo::ApiKey {
                api_key: "some-key".to_owned(),
            },
            false,
            ResponseCache::Disabled,
        )
        .unwrap();

        let result = api.get_run("run-1").await;
        assert!(result.is_err());

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "an API key can't refresh, so no retry");
        assert_eq!(requests[0].url.path(), "/api/v0/runs/run-1");
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

        let api = test_api_optionally_with_cache(&mock_server, None);

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

        let api = test_api_optionally_with_cache(&mock_server, None);

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

        let api = test_api_optionally_with_cache(&mock_server, None);

        let mut stream = api
            .search_run_events("run-1", "slow request", None)
            .await
            .unwrap();
        let mut body = String::new();
        while let Some(value) = futures_util::TryStreamExt::try_next(&mut stream)
            .await
            .unwrap()
        {
            body.push_str(&value.to_string());
        }

        assert!(body.contains("slow request"));
    }

    #[tokio::test]
    async fn search_run_events_forwards_limit() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/events"))
            .and(query_param("q", "slow"))
            .and(query_param("limit", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let _stream = api
            .search_run_events("run-1", "slow", NonZeroU64::new(5))
            .await
            .unwrap();
    }

    // Tenants that predate the `limit` parameter must not receive it, so an
    // unset `--limit` leaves the query param off entirely.
    #[tokio::test]
    async fn search_run_events_omits_limit_when_unset() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/v0/runs/run-1/events"))
            .and(query_param("q", "slow"))
            .and(query_param_is_missing("limit"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let _stream = api.search_run_events("run-1", "slow", None).await.unwrap();
    }

    // The DSL search wrapper POSTs the Search_Request body: the query and
    // both mode switches. `count_only` is omitted entirely (build.rs drops
    // it from the generated type), and an unset limit is OMITTED, not
    // defaulted: the omission is meaningful (a streaming request stays
    // unbounded; see build.rs's `unrequire_search_limit_default`).
    #[tokio::test]
    async fn search_run_events_query_posts_full_body() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v0/runs/run-1/events/search"))
            .and(body_json(serde_json::json!({
                "query": "contains({output_text: \"raft\"})",
                "is_streaming": false,
                "validate_only": false,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"output_text":"raft","moment":{"input_hash":"-456","vtime":"2.0"}}"#,
            ))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let mut stream = api
            .search_run_events_query(
                "run-1",
                "contains({output_text: \"raft\"})",
                SearchMode::Query {
                    stream: false,
                    limit: None,
                },
            )
            .await
            .unwrap();
        // The stream arrives parsed: the one response line is a Value.
        use futures_util::TryStreamExt;
        let value = stream.try_next().await.unwrap().expect("one parsed line");
        assert_eq!(value["output_text"], "raft");
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn search_run_events_query_forwards_each_mode() {
        let mock_server = MockServer::start().await;

        // A streaming query names its limit and is not validate-only.
        Mock::given(method("POST"))
            .and(path("/api/v0/runs/run-1/events/search"))
            .and(body_json(serde_json::json!({
                "query": "contains({output_text: \"raft\"})",
                "is_streaming": true,
                "validate_only": false,
                "limit": 7,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let _stream = api
            .search_run_events_query(
                "run-1",
                "contains({output_text: \"raft\"})",
                SearchMode::Query {
                    stream: true,
                    limit: NonZeroU64::new(7),
                },
            )
            .await
            .unwrap();

        // Validate is its own mode: no stream, no limit.
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/runs/run-1/events/search"))
            .and(body_json(serde_json::json!({
                "query": "contains({output_text: \"raft\"})",
                "is_streaming": false,
                "validate_only": true,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let _stream = api
            .search_run_events_query(
                "run-1",
                "contains({output_text: \"raft\"})",
                SearchMode::Validate,
            )
            .await
            .unwrap();
    }

    // An error must keep its HTTP status so callers can classify it — the 404
    // path feeds the runs-events fallback for tenants that predate the
    // endpoint.
    #[tokio::test]
    async fn search_run_events_query_keeps_error_status() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v0/runs/run-1/events/search"))
            .respond_with(ResponseTemplate::new(404).set_body_string(""))
            .expect(1)
            .mount(&mock_server)
            .await;

        let api = test_api_optionally_with_cache(&mock_server, None);
        let Err(err) = api
            .search_run_events_query(
                "run-1",
                "contains({x: \"y\"})",
                SearchMode::Query {
                    stream: false,
                    limit: None,
                },
            )
            .await
        else {
            panic!("expected the 404 to surface as an error");
        };
        assert_eq!(crate::error::api_error_status(&err), Some(404));
    }

    fn rid(version: u32) -> String {
        format!("e88ec3ec6cdb7b31ea08718616e04849-{version}-11")
    }

    #[test]
    fn properties_rejected_before_v52() {
        let report = ensure_resource_supported(&rid(40), MIN_PROPERTIES_VERSION, "run properties")
            .unwrap_err();
        // The message states the error; the version detail + remediation are notes.
        let msg = format!("{report}");
        assert!(
            msg.contains("run properties") && msg.contains("not available"),
            "got: {msg}"
        );
        let full = format!("{report:?}");
        assert!(full.contains("v52"), "got: {full}");
        assert!(
            full.contains("re-run") && full.contains("more recent version"),
            "got: {full}"
        );
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
        let report =
            ensure_resource_supported(&rid(53), MIN_BUILD_LOGS_VERSION, "build logs").unwrap_err();
        let msg = format!("{report}");
        assert!(
            msg.contains("build logs") && msg.contains("not available"),
            "got: {msg}"
        );
        assert!(format!("{report:?}").contains("v54"), "got: {report:?}");
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
