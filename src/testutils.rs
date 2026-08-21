use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::container::{ContainerRuntime, DockerRuntime, PodmanRuntime, is_podman_in_disguise};

/// Return all container runtimes that are actually usable on this machine.
/// Skips `docker` if it is actually podman in disguise.
pub fn available_runtimes() -> Vec<Box<dyn ContainerRuntime>> {
    // A pure test-harness knob (selects podman vs docker), never a setting.
    let requested = std::env::var("SNOUTY_TEST_RUNTIME").ok();
    let mut runtimes: Vec<Box<dyn ContainerRuntime>> = Vec::new();
    let want_podman = requested.as_deref().is_none_or(|r| r == "podman");
    let want_docker = requested.as_deref().is_none_or(|r| r == "docker");

    if want_podman
        && Command::new("podman")
            .arg("info")
            .output()
            .is_ok_and(|o| o.status.success())
    {
        runtimes.push(Box::new(PodmanRuntime::new("podman")));
    }
    if want_docker
        && Command::new("docker")
            .arg("info")
            .output()
            .is_ok_and(|o| o.status.success())
        && !is_podman_in_disguise("docker")
    {
        runtimes.push(Box::new(DockerRuntime::new("docker")));
    }
    runtimes
}

/// Points tests at a registry the environment already runs, instead of each one
/// starting its own (see `scripts/setup-test-images.sh`, which CI runs).
///
/// The value is a `host:port`, and the host has to be `127.0.0.1` or
/// `localhost`: snouty only disables TLS verification for those, so a registry
/// reached any other way fails the push (see `ContainerRuntime::image_push`).
pub const TEST_REGISTRY_VAR: &str = "SNOUTY_TEST_REGISTRY";

pub struct OCIRegistry {
    host_port: String,
    /// The container this handle started, or `None` when the registry came from
    /// [`TEST_REGISTRY_VAR`] — someone else's to stop.
    owned: Option<OwnedRegistry>,
}

struct OwnedRegistry {
    child: Child,
    runtime: String,
    container_name: String,
    /// The container's stderr, kept so a start failure can say why.
    log: tempfile::NamedTempFile,
}

impl OCIRegistry {
    /// A registry to push test images to: the one [`TEST_REGISTRY_VAR`] names,
    /// or a fresh container. Returns `None` when neither is usable.
    pub fn start(runtime: &dyn ContainerRuntime) -> Option<Self> {
        if let Some(host_port) = std::env::var(TEST_REGISTRY_VAR)
            .ok()
            .filter(|v| !v.is_empty())
        {
            // A configured-but-dead registry is a broken environment, not a
            // reason to quietly start a second one: fall through to skip_or_fail
            // so CI fails loudly on its own setup.
            if registry_v2_ping_addr(&host_port) {
                return Some(Self {
                    host_port,
                    owned: None,
                });
            }
            skip_or_fail(&format!(
                "{TEST_REGISTRY_VAR} is set to {host_port} but nothing answers /v2/ there"
            ));
            return None;
        }

        if !runtime_supports_linux_registry_image(runtime.name()) {
            eprintln!(
                "skipping: OCI registry image requires Linux containers for {}",
                runtime.name()
            );
            return None;
        }
        if let Err(reason) = ensure_registry_image_available(runtime.name()) {
            skip_or_fail(&format!(
                "OCI registry image could not be pulled with {}: {reason}",
                runtime.name()
            ));
            return None;
        }

        let container_name = format!(
            "snouty-test-registry-{}-{}",
            std::process::id(),
            next_registry_nonce()
        );
        let runtime_name = runtime.name().to_owned();
        let log = tempfile::NamedTempFile::new().expect("create OCI registry log file");

        // Let the engine pick the host port. Asking the kernel for a free port
        // first and publishing it by number leaves a window where another test
        // takes it before the engine binds.
        let child = Command::new(&runtime_name)
            .args([
                "run",
                "--rm",
                "-p",
                "127.0.0.1:0:5000",
                "--name",
                &container_name,
                "registry:2",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                log.reopen().expect("reopen OCI registry log file"),
            ))
            .spawn()
            .unwrap_or_else(|e| {
                panic!("failed to start OCI registry with {}: {e}", runtime.name())
            });

        let mut registry = Self {
            host_port: String::new(),
            owned: Some(OwnedRegistry {
                child,
                runtime: runtime_name,
                container_name,
                log,
            }),
        };
        match registry.wait_until_ready() {
            Ok(host_port) => {
                registry.host_port = host_port;
                Some(registry)
            }
            Err(reason) => {
                registry.cleanup_container();
                skip_or_fail(&format!(
                    "OCI registry could not start with {}: {reason}",
                    runtime.name()
                ));
                None
            }
        }
    }

    pub fn host_port(&self) -> String {
        self.host_port.clone()
    }

    /// Wait for the container to publish its port and answer `/v2/`, returning
    /// the `host:port` it landed on. `Err` carries what the container said, so a
    /// failure to start explains itself.
    fn wait_until_ready(&mut self) -> Result<String, String> {
        let owned = self
            .owned
            .as_mut()
            .expect("wait_until_ready is only for a container this handle started");
        let mut host_port = None;
        for _ in 0..200 {
            if host_port.is_none() {
                host_port = published_host_port(&owned.runtime, &owned.container_name);
            }
            if let Some(addr) = &host_port
                && registry_v2_ping_addr(addr)
            {
                return Ok(addr.clone());
            }
            if owned
                .child
                .try_wait()
                .expect("failed to poll OCI registry child process")
                .is_some()
            {
                return Err(container_log_tail(&owned.log));
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err(format!(
            "timed out waiting for the registry to answer{}: {}",
            host_port
                .map(|a| format!(" on {a}"))
                .unwrap_or_else(|| " (it never published a port)".to_string()),
            container_log_tail(&owned.log)
        ))
    }

    fn cleanup_container(&self) {
        let Some(owned) = &self.owned else {
            return;
        };
        let _ = Command::new(&owned.runtime)
            .args(["rm", "-f", &owned.container_name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for OCIRegistry {
    fn drop(&mut self) {
        // A registry from TEST_REGISTRY_VAR outlives every test that used it.
        let Some(owned) = &mut self.owned else {
            return;
        };
        if owned.child.try_wait().ok().flatten().is_none() {
            let _ = owned.child.kill();
        }
        let _ = owned.child.wait();
        self.cleanup_container();
    }
}

/// Distinguishes registries started by the same process, since one test can
/// start several (one per container runtime).
fn next_registry_nonce() -> u32 {
    static NONCE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// The `host:port` the engine published the registry's port 5000 on, once the
/// container is running.
fn published_host_port(runtime: &str, container_name: &str) -> Option<String> {
    let output = Command::new(runtime)
        .args(["port", container_name, "5000/tcp"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    // Both engines answer `127.0.0.1:49154`, one mapping per line.
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("127.0.0.1:"))
        .map(str::to_string)
}

/// The last thing the registry container wrote, for a start-failure message.
fn container_log_tail(log: &tempfile::NamedTempFile) -> String {
    const MAX_TAIL: usize = 400;
    let Ok(contents) = std::fs::read_to_string(log.path()) else {
        return "no container output".to_string();
    };
    let tail: String = contents
        .lines()
        .rev()
        .take(3)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("; ");
    if tail.trim().is_empty() {
        return "no container output".to_string();
    }
    tail.chars().take(MAX_TAIL).collect()
}

/// A repo prefix unique to this call, so tests that share one registry — every
/// test does when [`TEST_REGISTRY_VAR`] is set — never push to the same repo.
/// That matters beyond hygiene: `pin_images` skips the push when the registry
/// already serves the digest, so a shared repo would silently stop exercising
/// the push path. Include the runtime in `label` when a test loops over engines.
pub fn unique_image_prefix(label: &str) -> String {
    format!(
        "snouty-test-{label}-{}-{}",
        std::process::id(),
        next_registry_nonce()
    )
}

/// Returns `true` when running inside GitHub Actions (or any CI that sets `CI=true`).
pub fn is_ci() -> bool {
    std::env::var("CI").is_ok_and(|v| v == "true" || v == "1")
}

/// In CI this panics so silent skips don't hide missing test coverage.
/// Locally it prints a message and returns so the test can exit early.
#[track_caller]
pub fn skip_or_fail(msg: &str) {
    if is_ci() {
        panic!("{msg}");
    }
    eprintln!("skipping: {msg}");
}

/// Check whether Docker Compose v2 is available (the standalone
/// `docker-compose` binary or the `docker compose` CLI plugin).
pub fn has_compose() -> bool {
    crate::compose::DockerCompose::probe().is_ok()
}

/// Return available runtimes, or skip/fail if none are found.
/// Convenience wrapper for tests that require at least one runtime.
#[track_caller]
pub fn require_runtimes() -> Vec<Box<dyn ContainerRuntime>> {
    let runtimes = available_runtimes();
    if runtimes.is_empty() {
        skip_or_fail("no container runtime available");
    }
    runtimes
}

/// Return available runtimes that have compose support, or skip/fail if none.
#[track_caller]
pub fn require_runtimes_with_compose() -> Vec<Box<dyn ContainerRuntime>> {
    let runtimes = require_runtimes();
    if !has_compose() {
        skip_or_fail("docker-compose (Docker Compose v2) is not available");
    }
    runtimes
}

/// Whether an OCI registry answers `/v2/` at `addr` (a `host:port`).
fn registry_v2_ping_addr(addr: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return false;
    };

    let request = format!("GET /v2/ HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    if std::io::Write::write_all(&mut stream, request.as_bytes()).is_err() {
        return false;
    }

    let mut response = String::new();
    if std::io::Read::read_to_string(&mut stream, &mut response).is_err() {
        return false;
    }

    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
}

fn runtime_supports_linux_registry_image(runtime: &str) -> bool {
    if !runtime.ends_with("docker") {
        return true;
    }

    let Ok(output) = Command::new(runtime)
        .args(["info", "--format", "{{.OSType}}"])
        .output()
    else {
        return true;
    };
    if !output.status.success() {
        return true;
    }

    docker_info_supports_linux_registry(&String::from_utf8_lossy(&output.stdout))
}

fn docker_info_supports_linux_registry(stdout: &str) -> bool {
    let os_type = stdout.trim();
    os_type.is_empty() || os_type.eq_ignore_ascii_case("linux")
}

/// How many times to try pulling the registry image before giving up.
const REGISTRY_PULL_ATTEMPTS: u32 = 3;

/// Pause between pull attempts, giving a saturated network proxy or a
/// rate-limiting registry a moment to recover.
const REGISTRY_PULL_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Pull `registry:2`, retrying a failed pull before giving up.
///
/// One attempt is enough on a healthy machine, but on the macOS podman runner
/// the pull crosses the VM's network proxy and intermittently fails outright,
/// which fails every registry-backed test in the run at once. Retrying is close
/// to free — after a success the image is local, so a later pull is a no-op —
/// and turns a transient blip into a slower success.
///
/// Returns the last failure's own words on `Err`, so the skip explains what went
/// wrong rather than only that something did.
fn ensure_registry_image_available(runtime: &str) -> Result<(), String> {
    let mut last_failure = String::new();
    for attempt in 1..=REGISTRY_PULL_ATTEMPTS {
        if attempt > 1 {
            thread::sleep(REGISTRY_PULL_RETRY_DELAY);
        }
        last_failure = match Command::new(runtime)
            .args(["pull", "registry:2"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .output()
        {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) => describe_pull_failure(&output),
            // The runtime binary itself is missing or unrunnable; retrying will
            // not change that, so report it immediately.
            Err(err) => return Err(format!("could not run `{runtime} pull`: {err}")),
        };
        eprintln!(
            "registry image pull attempt {attempt}/{REGISTRY_PULL_ATTEMPTS} failed: {last_failure}"
        );
    }
    Err(last_failure)
}

/// A failed pull reduced to one line: the runtime's last word on stderr plus the
/// exit status. Truncated because a pull failure can carry a wall of progress
/// output, and this ends up inside a one-line skip message.
fn describe_pull_failure(output: &std::process::Output) -> String {
    const MAX_DETAIL: usize = 200;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("no stderr output")
        .trim();
    let truncated: String = detail.chars().take(MAX_DETAIL).collect();
    let ellipsis = if truncated.chars().count() < detail.chars().count() {
        "…"
    } else {
        ""
    };
    format!("{} ({})", truncated + ellipsis, output.status)
}

pub fn filtered_path_without_binary(binary: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    let filtered = std::env::split_paths(&path)
        .filter(|dir| !dir.join(binary).is_file())
        .collect::<Vec<_>>();
    std::env::join_paths(filtered)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

/// A mock Antithesis API server for development and testing.
///
/// Handles:
/// - `GET  /api/v0/runs` — paginated run listing
/// - `GET  /api/v0/runs/{run_id}` — run detail (keyed by run id; 404 unknown)
/// - `GET  /api/v0/runs/{run_id}/{properties,logs,events,build_logs}` — nested
///   run resources (404 for an unknown run id, like the real API; properties
///   additionally 404 for runs that aren't `completed`)
/// - `POST /api/v1/launch/{launcher_name}` — returns a mock launch response
pub struct MockApiServer {
    url: String,
    token: String,
    handle: Option<JoinHandle<()>>,
}

impl MockApiServer {
    /// Start a mock server with sample run data (two runs, paginated).
    pub fn start() -> Self {
        Self::start_inner(false)
    }

    /// Start a mock server with no runs.
    pub fn start_empty() -> Self {
        Self::start_inner(true)
    }

    /// Return the base URL of the mock server (e.g. `http://127.0.0.1:12345`).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Return the token the server expects in Authorization headers.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Block until the server thread stops.
    pub fn wait(mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    fn start_inner(empty: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}", addr);
        let token = MOCK_API_TOKEN.to_string();
        let expected_token = MOCK_API_TOKEN.to_string();

        let handle = thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let Some(request) = mock_read_request(&mut stream) else {
                    continue;
                };

                let (status, body, content_type, cache_control) =
                    if !mock_check_user_agent(&request) {
                        (
                            400,
                            r#"{"message":"Missing User-Agent header."}"#.to_string(),
                            "application/json",
                            NO_CACHE_CACHE_CONTROL,
                        )
                    } else if !mock_check_auth(&request, &expected_token) {
                        (
                            401,
                            r#"{"message":"Invalid or expired bearer token."}"#.to_string(),
                            "application/json",
                            NO_CACHE_CACHE_CONTROL,
                        )
                    } else {
                        let (method, path) = mock_parse_request_line(&request);
                        let req_body = mock_request_body(&request);
                        mock_route(&method, &path, req_body, empty)
                    };

                let response = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nCache-Control: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                    status,
                    content_type,
                    cache_control,
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        Self {
            url,
            token,
            handle: Some(handle),
        }
    }
}

const MOCK_API_TOKEN: &str = "snouty-mock-api-token";

/// The `Cache-Control` the real API sends on a cacheable read (observed on
/// tenant release 61). The response cache admits nothing without such a
/// header.
pub const CACHEABLE_CACHE_CONTROL: &str = "private, max-age=3600";

/// The `Cache-Control` the real API sends on everything else — writes,
/// listings, errors (same observation).
const NO_CACHE_CACHE_CONTROL: &str = "no-cache";

/// Read one HTTP request off the socket: headers, then as many body bytes as
/// Content-Length declares. A single `read` is not enough — the client may
/// deliver a POST's headers and body in separate packets, and routing on a
/// truncated body would be racy.
fn mock_read_request(stream: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(header_end) = mock_find_header_end(&buf) {
            let headers = String::from_utf8_lossy(&buf[..header_end]);
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() >= header_end + content_length {
                break;
            }
        }
    }
    if buf.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// Byte offset just past the `\r\n\r\n` header terminator, if present.
fn mock_find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

/// The request body: everything after the blank line separating the headers.
fn mock_request_body(request: &str) -> &str {
    match request.split_once("\r\n\r\n") {
        Some((_, body)) => body,
        None => "",
    }
}

fn mock_check_auth(request: &str, expected_token: &str) -> bool {
    let expected = format!("Bearer {expected_token}");
    request.lines().any(|line| {
        let line = line.trim();
        line.strip_prefix("Authorization:")
            .or_else(|| line.strip_prefix("authorization:"))
            .is_some_and(|val| val.trim() == expected)
    })
}

fn mock_check_user_agent(request: &str) -> bool {
    request.lines().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("user-agent") && !value.trim().is_empty()
    })
}

/// Parse the first line of an HTTP request into (method, path).
fn mock_parse_request_line(request: &str) -> (String, String) {
    let first_line = request.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    (method, path)
}

/// Each route states its own `Cache-Control`, mirroring the real API
/// (observed on tenant release 61): successful run-scoped GET reads carry
/// [`CACHEABLE_CACHE_CONTROL`]; the run list, errors, and everything else
/// carry `no-cache`. The real API also sends `no-cache` for a non-terminal
/// run's detail; the mock skips that distinction — snouty's own admission
/// checks already keep a non-terminal detail out of the cache.
fn mock_route(
    method: &str,
    path: &str,
    req_body: &str,
    empty: bool,
) -> (u16, String, &'static str, &'static str) {
    // Split path and query string
    let (path_part, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path, None),
    };

    let json = "application/json";
    let ndjson = "application/x-ndjson";

    match (method, path_part) {
        ("POST", p) if p.starts_with("/api/v0/runs/") && p.ends_with("/events/search") => {
            let run_id = &p["/api/v0/runs/".len()..p.len() - "/events/search".len()];
            let (s, b, ct) = mock_route_search_events(run_id, req_body);
            (s, b, ct, NO_CACHE_CACHE_CONTROL)
        }
        ("GET", "/api/v0/runs") => {
            let (s, b) = mock_route_list_runs(query, empty);
            (s, b, json, NO_CACHE_CACHE_CONTROL)
        }
        ("GET", p) if p.starts_with("/api/v0/runs/") => {
            let rest = &p["/api/v0/runs/".len()..];
            let (s, b, ct) = if let Some(run_id) = rest.strip_suffix("/build_logs") {
                let (s, b) = mock_route_get_run_build_logs(run_id);
                (s, b, ndjson)
            } else if let Some(run_id) = rest.strip_suffix("/logs") {
                let (s, b) = mock_route_get_run_logs(run_id);
                (s, b, ndjson)
            } else if let Some(run_id) = rest.strip_suffix("/properties") {
                let (s, b) = mock_route_list_run_properties(run_id, query);
                (s, b, json)
            } else if let Some(run_id) = rest.strip_suffix("/events") {
                let (s, b) = mock_route_search_run_events(run_id, query);
                (s, b, ndjson)
            } else {
                let (s, b) = mock_route_get_run(rest);
                (s, b, json)
            };
            let cache_control = if s == 200 {
                CACHEABLE_CACHE_CONTROL
            } else {
                NO_CACHE_CACHE_CONTROL
            };
            (s, b, ct, cache_control)
        }
        ("POST", p) if p.starts_with("/api/v0/runs/") => {
            let rest = &p["/api/v0/runs/".len()..];
            if let Some(run_id) = rest.strip_suffix("/execute_command") {
                let (s, b) = mock_route_execute_command(run_id, req_body);
                (s, b, ndjson, NO_CACHE_CACHE_CONTROL)
            } else {
                (
                    404,
                    r#"{"message":"not found"}"#.to_string(),
                    json,
                    NO_CACHE_CACHE_CONTROL,
                )
            }
        }
        ("POST", p) if p.starts_with("/api/v1/launch/") => {
            let (s, b) = mock_route_launch();
            (s, b, json, NO_CACHE_CACHE_CONTROL)
        }
        _ => (
            404,
            r#"{"message":"not found"}"#.to_string(),
            json,
            NO_CACHE_CACHE_CONTROL,
        ),
    }
}

fn mock_query_param(query: Option<&str>, key: &str) -> Option<String> {
    form_urlencoded::parse(query?.as_bytes())
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

// (run_id, status, created_at, launcher, description). An empty description
// stands in for a run with no description (the field is omitted from the JSON).
const MOCK_RUNS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "run-1",
        "completed",
        "2025-03-20T02:00:00Z",
        "nightly",
        "nightly smoke on main",
    ),
    ("run-2", "in_progress", "2025-03-19T14:00:00Z", "debug", ""),
    (
        "run-3",
        "incomplete",
        "2025-03-18T08:00:00Z",
        "nightly",
        "incomplete recovery test",
    ),
];

fn mock_route_list_runs(query: Option<&str>, empty: bool) -> (u16, String) {
    if empty {
        return (200, r#"{"data":[],"next_cursor":null}"#.to_string());
    }

    let after = mock_query_param(query, "after");
    let status_filter = mock_query_param(query, "status");
    let launcher_filter = mock_query_param(query, "launcher");

    // Determine which runs to consider based on cursor position.
    let start = match after.as_deref() {
        Some("cursor-1") => 1,
        _ => 0,
    };

    let mut runs = Vec::new();
    for &(id, status, created, launcher, description) in &MOCK_RUNS[start..] {
        if let Some(f) = status_filter.as_deref()
            && status != f
        {
            continue;
        }
        if let Some(f) = launcher_filter.as_deref()
            && launcher != f
        {
            continue;
        }
        let description_field = if description.is_empty() {
            String::new()
        } else {
            format!(r#","description":"{description}""#)
        };
        runs.push(format!(
            r#"{{"run_id":"{id}","status":"{status}","created_at":"{created}","launcher":"{launcher}"{description_field}}}"#,
        ));
    }

    // Paginate: return one run per page when no filters are active and starting from the beginning.
    let (data, next_cursor) =
        if status_filter.is_none() && launcher_filter.is_none() && start == 0 && runs.len() > 1 {
            (vec![runs[0].clone()], Some("cursor-1"))
        } else {
            (runs, None)
        };

    let data_json = data.join(",");
    let cursor_json = match next_cursor {
        Some(c) => format!("\"{c}\""),
        None => "null".to_string(),
    };
    (
        200,
        format!(r#"{{"data":[{data_json}],"next_cursor":{cursor_json}}}"#),
    )
}

/// Run ids that exist on the mock server. Nested endpoints (properties, logs,
/// events, build logs) 404 for any other id, matching the real API and letting
/// run-scoped commands disambiguate "run not found" from an empty resource.
/// `run-empty` and `run-no-events` are properties-only fixtures and are
/// recognised separately by `mock_route_list_run_properties`; `run-stream-error`
/// is recognised separately by the streaming routes, whose response ends with
/// a `Stream_Error` line.
fn mock_run_known(run_id: &str) -> bool {
    MOCK_RUNS.iter().any(|(id, ..)| *id == run_id)
}

fn mock_run_not_found(run_id: &str) -> (u16, String) {
    (404, format!(r#"{{"message":"run not found: {run_id}"}}"#))
}

fn mock_route_get_run(run_id: &str) -> (u16, String) {
    // `run-unknown-status` is kept out of MOCK_RUNS so the list-oriented specs
    // don't see it.
    if run_id == "run-unknown-status" {
        return (
            200,
            r#"{"run_id":"run-unknown-status","status":"unknown","created_at":"2025-03-20T02:00:00Z","launcher":"nightly"}"#
                .to_string(),
        );
    }
    let Some(&(_, status, created, launcher, description)) =
        MOCK_RUNS.iter().find(|(id, ..)| *id == run_id)
    else {
        return mock_run_not_found(run_id);
    };

    let mut fields = vec![
        format!(r#""run_id":"{run_id}""#),
        format!(r#""status":"{status}""#),
        format!(r#""created_at":"{created}""#),
    ];
    if !description.is_empty() {
        fields.push(format!(r#""description":"{description}""#));
    }
    if status == "completed" || status == "incomplete" {
        fields.push(r#""started_at":"2025-03-20T02:01:12Z""#.to_string());
        fields.push(r#""completed_at":"2025-03-20T02:31:45Z""#.to_string());
    }
    fields.push(format!(r#""launcher":"{launcher}""#));
    // run-1 carries launch parameters so `runs show` can surface the requested
    // Duration and Source alongside the timestamp-derived Elapsed; other runs
    // omit them, exercising the "field absent" path.
    if run_id == "run-1" {
        fields.push(
            r#""parameters":{"antithesis.duration":"30","antithesis.source":"demo-harness"}"#
                .to_string(),
        );
    }
    fields.push(format!(
        r#""links":{{"triage_report":"https://demo.antithesis.com/reports/{run_id}"}}"#
    ));
    if status == "incomplete" {
        fields.push(
            r#""failure_moment":{"input_hash":"-3625518438076122494","vtime":"398.4898056755774"}"#
                .to_string(),
        );
    }

    (200, format!("{{{}}}", fields.join(",")))
}

/// The `Stream_Error` line the `run-stream-error` fixture ends its streams
/// with: the server's shape for a failure that happens after the `200 OK` is
/// already committed (see `parse_stream_error` in runs.rs).
const MOCK_STREAM_ERROR_LINE: &str = r#"{"error":"mock stream failure"}"#;

fn mock_route_get_run_build_logs(run_id: &str) -> (u16, String) {
    // `run-empty` models a completed run with no build logs: a successful but
    // empty stream, which exercises the "No build logs for this run." note.
    if run_id == "run-empty" {
        return (200, String::new());
    }
    // `run-stream-error` models the server failing mid-stream: one good line,
    // then a `Stream_Error` line ends the stream.
    if run_id == "run-stream-error" {
        let lines = [
            r#"{"timestamp":"2025-03-20T02:01:12Z","stream":"stdout","text":"Building image payments-service..."}"#,
            MOCK_STREAM_ERROR_LINE,
        ];
        return (200, lines.join("\n") + "\n");
    }
    if !mock_run_known(run_id) {
        return mock_run_not_found(run_id);
    }
    let lines = [
        r#"{"timestamp":"2025-03-20T02:01:12Z","stream":"stdout","text":"Building image payments-service..."}"#,
        r#"{"timestamp":"2025-03-20T02:01:15Z","stream":"stderr","text":"Warning: deprecated feature"}"#,
        r#"{"timestamp":"2025-03-20T02:01:20Z","stream":"stdout","text":"Build complete"}"#,
    ];
    (200, lines.join("\n") + "\n")
}

fn mock_route_get_run_logs(run_id: &str) -> (u16, String) {
    // See the `run-stream-error` fixture note in `mock_route_get_run_build_logs`.
    if run_id == "run-stream-error" {
        let lines = [
            r#"{"output_text":"{\"level\":\"info\",\"msg\":\"starting\"}","source":{"container":"app","name":"app","stream":"out"},"moment":{"input_hash":"-123","vtime":"1.0","session_id":"sess-1"}}"#,
            MOCK_STREAM_ERROR_LINE,
        ];
        return (200, lines.join("\n") + "\n");
    }
    if !mock_run_known(run_id) {
        return mock_run_not_found(run_id);
    }
    let lines = [
        r#"{"output_text":"{\"level\":\"info\",\"msg\":\"starting\"}","source":{"container":"app","name":"app","stream":"out"},"moment":{"input_hash":"-123","vtime":"1.0","session_id":"sess-1"}}"#,
        r#"{"output_text":"{\"level\":\"warn\",\"msg\":\"slow request\"}","source":{"container":"app","name":"app","stream":"error"},"moment":{"input_hash":"-456","vtime":"2.0","session_id":"sess-1"}}"#,
        // Record whose output_text contains a JSON-escaped newline (\n).
        // Verifies that --json emits this as a single output line.
        r#"{"output_text":"line one\nline two","source":{"container":"app","name":"app","stream":"out"},"moment":{"input_hash":"-789","vtime":"3.0"}}"#,
        r#"{"IPT_bytes_out":563000,"output_text":"W0320 15:07:26.913251       1 control.go:315] Error setting vault 10.0.1.123:8003 value to 64: Post \"http://10.0.1.123:8003/\": dial tcp 10.0.1.123:8003: i/o timeout (Client.Timeout exceeded while awaiting headers)","source":{"container":"control","name":"service_control","stream":"error"},"moment":{"input_hash":"-7835669064649885519","vtime":"73.94233945617452"}}"#,
        r#"{"antithesis_assert":{"assert_type":"always","condition":false,"details":null,"display_type":"AlwaysOrUnreachable","hit":false,"id":"Counter's value retrieved","location":{"begin_column":0,"begin_line":87,"class":"","file":"/go/src/antithesis/control/control.go","function":"get"},"message":"Counter's value retrieved","must_hit":false},"IPT_bytes_out":1837376,"source":{"container":"control","name":"control","pid":1},"moment":{"input_hash":"-4735081784258020614","vtime":"311.8487535319291"}}"#,
        // Platform events with no container; source.name is used as the
        // bracketed label and curated fields render as <path>=<value> pairs.
        r#"{"started_task":"abc_parallel_driver_fetch","task_status":"started","command":"core/parallel_driver_fetch","container_id":"d700ef3d05a263","tasks_len":"1","source":{"name":"antithesis_test_composer","pid":974},"moment":{"input_hash":"5181922178177328213","vtime":"400.5"}}"#,
        r#"{"fault":{"name":"clog","type":"network","details":{"disruption_type":"Stopped"},"affected_nodes":["client2","setup"],"max_duration":0.267},"source":{"name":"fault_injector","pid":1086},"moment":{"input_hash":"5181922178177328213","vtime":"401.5"}}"#,
        r#"{"started_task":"abc_parallel_driver_fetch","task_status":"progressing","command":"core/parallel_driver_fetch","container_id":"d700ef3d05a263","tasks_len":"1","source":{"name":"antithesis_test_composer","pid":974},"moment":{"input_hash":"5181922178177328213","vtime":"401.75"}}"#,
        r#"{"started_task":"abc_parallel_driver_fetch","task_status":"completed","command":"core/parallel_driver_fetch","container_id":"d700ef3d05a263","tasks_len":"1","source":{"name":"antithesis_test_composer","pid":974},"moment":{"input_hash":"5181922178177328213","vtime":"402.0"}}"#,
    ];
    (200, lines.join("\n") + "\n")
}

fn mock_route_list_run_properties(run_id: &str, query: Option<&str>) -> (u16, String) {
    if run_id == "run-empty" {
        return (200, r#"{"data":[],"next_cursor":null}"#.to_string());
    }

    if run_id == "run-no-events" {
        return (
            200,
            r#"{"data":[{"name":"No events property","status":"Passing","is_event":false,"example_count":0,"counterexample_count":0}],"next_cursor":null}"#.to_string(),
        );
    }

    // The properties endpoint 404s for an unknown run id and for a real run that
    // isn't `completed` yet (its triage report hasn't been generated). Only
    // completed runs return property data, matching the real API and exercising
    // the "run not found" vs "this run is incomplete" disambiguation.
    match MOCK_RUNS.iter().find(|(id, ..)| *id == run_id) {
        Some(&(_, "completed", ..)) => {}
        _ => return mock_run_not_found(run_id),
    }

    let status = mock_query_param(query, "status");
    let after = mock_query_param(query, "after");

    // Full-precision vtimes so a precision fault fails a spec (see specs/runs.txt).
    let failing = r#"{"name":"Counter value stays below limit","description":"Counter stays within safe bounds","status":"Failing","is_event":true,"group":"Safety","example_count":12,"counterexample_count":3,"examples":[{"moment":{"input_hash":"-300","vtime":"398.4898056755774"}}],"counterexamples":[{"moment":{"input_hash":"-200","vtime":"313.15126806590706"}},{"moment":{"input_hash":"-100","vtime":"45.334635781589895"}}]}"#.to_string();
    // A failing non-event ("system") property: the violating value lives in
    // `counterexamples`, so `--detail` must label it apart from the satisfying
    // `examples`.
    let failing_nonevent = r#"{"name":"Peak memory stays below cap","description":"Peak memory never exceeds the configured limit","status":"Failing","is_event":false,"group":"Resources","example_count":2,"counterexample_count":1,"examples":[820,910],"counterexamples":[1340]}"#.to_string();
    let passing = r#"{"name":"Setup completes","description":"Setup eventually succeeds","status":"Passing","is_event":false,"example_count":1,"counterexample_count":0,"examples":[{"final_counter":42}]}"#.to_string();

    let (data, next_cursor) = match (status.as_deref(), after.as_deref()) {
        (Some("Failing"), _) => (vec![failing, failing_nonevent], None),
        (Some("Passing"), _) => (vec![passing], None),
        (None, None) => (vec![failing, failing_nonevent], Some("props-cursor-1")),
        (None, Some("props-cursor-1")) => (vec![passing], None),
        (None, _) => (vec![], None),
        _ => (vec![], None),
    };

    let data_json = data.join(",");
    let cursor_json = match next_cursor {
        Some(cursor) => format!("\"{cursor}\""),
        None => "null".to_string(),
    };
    (
        200,
        format!(r#"{{"data":[{data_json}],"next_cursor":{cursor_json}}}"#),
    )
}

/// The text an events route matches a needle against: an event's log text,
/// an assertion's message and source function, and a test-composer command
/// and task, NUL-joined so a needle cannot span two fields. Both events
/// routes share it, because `runs events` returns the same events whichever
/// backend serves it.
///
/// The gates the live endpoints put on those fields — an assertion counts
/// only once it was hit, a command only on a composer event — are not
/// modelled here. The specs pin which FIELDS a needle reaches; which events
/// the server then considers is the server's own answer, and a fixture
/// written here is not evidence of it.
fn mock_event_haystack(line: &str) -> String {
    let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else {
        return line.to_lowercase();
    };
    [
        &event["output_text"],
        &event["antithesis_assert"]["message"],
        &event["antithesis_assert"]["location"]["function"],
        &event["source"]["name"],
        &event["command"],
        &event["started_task"],
    ]
    .iter()
    .filter_map(|field| field.as_str())
    .collect::<Vec<_>>()
    .join("\0")
    .to_lowercase()
}

/// Every double-quoted string literal in `query`, with `\"` and `\\` escapes
/// resolved. The mock server uses this to "interpret" a query without a DSL
/// engine: requiring each literal somewhere in an event covers
/// `contains({...})` needles and substring-filter pipelines alike. Test-only
/// by construction — it lives here so nothing outside the mock reaches it.
fn extract_quoted_literals(query: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let mut chars = query.chars();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut literal = String::new();
        loop {
            match chars.next() {
                Some('\\') => {
                    if let Some(escaped) = chars.next() {
                        literal.push(escaped);
                    }
                }
                Some('"') | None => break,
                Some(other) => literal.push(other),
            }
        }
        literals.push(literal);
    }
    literals
}

/// The needles a query requires, lowercased to match the JS `.toLowerCase()`
/// in the filter this mock models.
///
/// `runs events` applies [`event_set_dsl::NEEDLE_FILTER`] to a JSON array of
/// needles, so cutting the query at that expression leaves the array; read it
/// as JSON and the needles arrive exactly, however they are spelled. Any
/// other query keeps every string literal it holds, which is what
/// `contains({...})` and a hand-written filter need.
fn query_needles(query: &str) -> Vec<String> {
    let literals = match query.split_once(crate::event_set_dsl::NEEDLE_FILTER.trim()) {
        Some((_, applied)) => {
            let array = applied
                .find('[')
                .zip(applied.rfind(']'))
                .map(|(open, close)| &applied[open..=close])
                .expect("`runs events` applies the filter to a JSON array");
            serde_json::from_str(array).expect("the array snouty wrote is JSON")
        }
        None => extract_quoted_literals(query),
    };
    literals
        .into_iter()
        .filter(|literal| !literal.trim().is_empty())
        .map(|literal| literal.to_lowercase())
        .collect()
}

fn mock_route_search_run_events(run_id: &str, query_str: Option<&str>) -> (u16, String) {
    // The `run-stream-error` stream is returned unfiltered: a `Stream_Error`
    // line is a failure signal the server emits regardless of the query, not
    // a match, so the needle filter below must not consume it.
    if run_id == "run-stream-error" {
        return mock_route_get_run_logs(run_id);
    }
    if !mock_run_known(run_id) {
        return mock_run_not_found(run_id);
    }
    let Some(needle) = mock_query_param(query_str, "q") else {
        return (400, r#"{"message":"missing q"}"#.to_string());
    };

    let (_, logs) = mock_route_get_run_logs(run_id);
    let needle = needle.to_lowercase();
    let mut matches = logs
        .lines()
        .filter(|line| mock_event_haystack(line).contains(&needle))
        .collect::<Vec<_>>();

    // Cap the returned events at `limit` when present, mirroring the real
    // endpoint's `limit` query parameter (the subset is the first N matches).
    if let Some(limit) = mock_query_param(query_str, "limit").and_then(|l| l.parse::<usize>().ok())
    {
        matches.truncate(limit);
    }

    if matches.is_empty() {
        (200, String::new())
    } else {
        (200, matches.join("\n") + "\n")
    }
}

/// The branch input hash every mock execution lands on: executing a command
/// branches the multiverse, so response moments never carry the requested
/// input hash (verified against the live API).
const MOCK_EXEC_BRANCH_HASH: &str = "-8206006569229276678";

/// One `output` event of a mock execution.
fn mock_exec_output(stream: &str, text: &str, vtime: &str) -> String {
    format!(
        r#"{{"type":"output","stream":"{stream}","text":"{text}","moment":{{"input_hash":"{MOCK_EXEC_BRANCH_HASH}","vtime":"{vtime}"}}}}"#
    )
}

/// The terminal `exited` event of a mock execution.
fn mock_exec_exited(exit_code: i64) -> String {
    format!(
        r#"{{"type":"exited","exit_code":{exit_code},"end_moment":{{"input_hash":"{MOCK_EXEC_BRANCH_HASH}","vtime":"398.492"}}}}"#
    )
}

fn mock_route_execute_command(run_id: &str, req_body: &str) -> (u16, String) {
    // See the `run-stream-error` fixture note in `mock_route_get_run_build_logs`.
    if run_id == "run-stream-error" {
        let lines = [
            mock_exec_output("stdout", "Linux antithesis 6.12.0", "398.491"),
            MOCK_STREAM_ERROR_LINE.to_string(),
        ];
        return (200, lines.join("\n") + "\n");
    }
    if !mock_run_known(run_id) {
        // The live endpoint's 404 body, which — unlike the friendlier body
        // the other nested routes mock — never names the run. snouty's "run
        // not found" translation has to do the work, and the spec can only
        // prove that against the unhelpful body.
        return (404, r#"{"message":"Resource not found"}"#.to_string());
    }
    // The mock treats only an in-progress run as having a live session.
    // The real API is looser — a session outlives the run for a while (see
    // the header of specs/runs_exec.txt) — but this rule drives both paths.
    // Anything else answers with the live API's verbatim session-less error.
    let live = MOCK_RUNS
        .iter()
        .any(|&(id, status, ..)| id == run_id && status == "in_progress");
    if !live {
        return (
            400,
            r#"{"message":"Bad request: minting session token failed: Invalid status code: 503 Service Unavailable"}"#.to_string(),
        );
    }

    // snouty's generated client always sends a well-formed body, so a
    // missing script or unparsable JSON just falls through to the default
    // script rather than modelling a validation error nothing exercises.
    let request = serde_json::from_str::<serde_json::Value>(req_body).unwrap_or_default();
    let script = request["script"].as_str().unwrap_or_default();
    let timeout = request["timeout_seconds"].as_u64().unwrap_or(30);

    let lines = match script.trim() {
        "true" => vec![mock_exec_exited(0)],
        "exit 5" => vec![mock_exec_exited(5)],
        // A command the session killed reports no exit code (`exit_code` is
        // nullable in the spec).
        "no-exit-code" => {
            vec![format!(
                r#"{{"type":"exited","exit_code":null,"end_moment":{{"input_hash":"{MOCK_EXEC_BRANCH_HASH}","vtime":"398.492"}}}}"#
            )]
        }
        "sleep 60" => vec![
            mock_exec_output("stdout", "still working", "398.491"),
            r#"{"type":"timed_out"}"#.to_string(),
        ],
        "print-timeout" => vec![
            mock_exec_output("stdout", &format!("timeout_seconds={timeout}"), "398.491"),
            mock_exec_exited(0),
        ],
        "truncate-stream" => vec![mock_exec_output("stdout", "partial output", "398.491")],
        // The API also labels stderr output with the short form `err`
        // (observed on the live endpoint). snouty must route it like
        // `stderr`.
        "short-stream-err" => vec![
            mock_exec_output("err", "short-form stderr line", "398.491"),
            mock_exec_exited(0),
        ],
        // A frame type this build does not know, and a known frame carrying a
        // field it does not know. The stream must survive both.
        "unknown-frames" => vec![
            format!(
                r#"{{"type":"heartbeat","at":"398.4905","input_hash":"{MOCK_EXEC_BRANCH_HASH}"}}"#
            ),
            format!(
                r#"{{"type":"output","stream":"stdout","text":"known with extras","truncated":true,"moment":{{"input_hash":"{MOCK_EXEC_BRANCH_HASH}","vtime":"398.491"}}}}"#
            ),
            mock_exec_exited(0),
        ],
        _ => vec![
            mock_exec_output("stdout", "Linux antithesis 6.12.0", "398.491"),
            mock_exec_output(
                "stderr",
                "warning: virtual clock drift detected",
                "398.4915",
            ),
            mock_exec_exited(0),
        ],
    };
    (200, lines.join("\n") + "\n")
}

/// `POST /runs/{id}/events/search` — the events-search endpoint.
/// `validate_only` returns an empty 200, and matching events stream back as
/// NDJSON. The mock ends the stream at `limit`, as the live server does:
/// releases 58.11 and 60.0 ignored the field and streamed every match, and
/// release 61 honors it (observed on tenant `orbitinghail`, run
/// `bafc3d6cb0ff883696153e2a8e30aee7-61-1`: a query matching six events
/// returned three under `-n 3`). The one divergence from the live server: the
/// mock's stream always closes (it is a plain HTTP response), where a live
/// run's stream stays open forever.
/// `count_only` is not modelled: snouty does not send it (the count is
/// moving to a separate endpoint).
///
/// The mock carries no DSL engine. A query is "interpreted" by taking its
/// needles (see [`query_needles`]) and requiring each, case-insensitively,
/// in the text [`mock_event_haystack`] collects — enough for
/// `contains({...})` needles and for the multi-needle JS filter `runs events`
/// builds. Validity checking is one rule: the pipeline must start with a
/// known verb.
fn mock_route_search_events(run_id: &str, body: &str) -> (u16, String, &'static str) {
    let json = "application/json";
    let ndjson = "application/x-ndjson";

    // Like the other streaming routes: `run-stream-error` answers with a
    // stream that ends in a `Stream_Error` line, whatever the query.
    if run_id == "run-stream-error" {
        let (status, body) = mock_route_get_run_logs(run_id);
        return (status, body, ndjson);
    }
    if !mock_run_known(run_id) {
        let (status, body) = mock_run_not_found(run_id);
        return (status, body, json);
    }

    let Ok(request) = serde_json::from_str::<serde_json::Value>(body) else {
        return (
            400,
            r#"{"message":"invalid request body"}"#.to_string(),
            json,
        );
    };
    let Some(query) = request["query"].as_str() else {
        return (400, r#"{"message":"missing query"}"#.to_string(), json);
    };
    let starts_with_verb = crate::event_set_dsl::VERBS
        .iter()
        .any(|verb| query.trim_start().starts_with(&format!("{verb}(")));
    if !starts_with_verb {
        // The live server lays the rejection out over three lines: the query,
        // a caret under the offending token, and the reason (observed on
        // tenant `orbitinghail`, release 61 — `bogus_verb({x: "y"})` answers
        // 400 with `Bad request: failed to execute pangolin query due to a
        // runtime error: Event set DSL error: <query>`, then `^`, then
        // `invalid with_next`). The breaks are real newlines in the JSON
        // string, so the mock writes them the same way.
        let message = format!(
            "Bad request: failed to execute pangolin query due to a runtime error: \
             Event set DSL error: {query}\n^\ninvalid with_next"
        );
        let body = serde_json::json!({ "message": message }).to_string();
        return (400, body, json);
    }
    if request["validate_only"].as_bool().unwrap_or(false) {
        return (200, String::new(), ndjson);
    }

    // The mock EXECUTES every query the same way: every needle must be a
    // substring of the event's text, all at once. Those are the semantics
    // of `contains` and of the substring `filter` that `runs events` builds
    // — but not of `excludes`, `not_matches`, `fold`, and friends, which
    // would be answered with wrong (even inverted) results and let a spec
    // pass for the wrong reason. Refuse them loudly instead. Validation
    // above stays permissive: query VALIDITY spans the full verb list;
    // execution is what the mock only partially models.
    const EXECUTABLE_VERBS: &[&str] = &["contains", "filter", "matches"];
    let executable = EXECUTABLE_VERBS
        .iter()
        .any(|verb| query.trim_start().starts_with(&format!("{verb}(")));
    if !executable {
        return (
            501,
            r#"{"message":"the mock API server does not model this verb's execution semantics"}"#
                .to_string(),
            json,
        );
    }

    let needles = query_needles(query);
    // The haystack the query asked for. `runs events` reads an event's text
    // fields; a query that stringifies the event asks for the event's JSON,
    // field names and all. The mock honors what was asked, so a spec sees
    // the difference between the two — that difference is what made `runs
    // events` answer differently on each backend (issue #252).
    let reads_event_json = !query.contains(crate::event_set_dsl::NEEDLE_FILTER.trim())
        && query.contains("JSON.stringify(ev)");
    let (_, logs) = mock_route_get_run_logs(run_id);
    let mut matches: Vec<&str> = logs
        .lines()
        .filter(|line| {
            let haystack = if reads_event_json {
                line.to_lowercase()
            } else {
                mock_event_haystack(line)
            };
            needles.iter().all(|needle| haystack.contains(needle))
        })
        .collect();
    // Release 61 ends the stream at `limit`; see the route's doc comment.
    if let Some(limit) = request["limit"].as_u64() {
        matches.truncate(limit as usize);
    }

    if matches.is_empty() {
        (200, String::new(), ndjson)
    } else {
        (200, matches.join("\n") + "\n", ndjson)
    }
}

fn mock_route_launch() -> (u16, String) {
    (
        200,
        r#"{"runId":"mock-run-id","statusCode":200}"#.to_string(),
    )
}

#[cfg(test)]
#[ctor::ctor]
fn init_test_eyre() {
    let _ = color_eyre::install();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn extract_quoted_literals_resolves_escapes() {
        assert_eq!(
            extract_quoted_literals(r#"contains({output_text: "slow request"})"#),
            vec!["slow request".to_string()]
        );
        // Escaped quotes and backslashes arrive resolved; the JS filter's
        // bare " " separators come out as literals too (callers drop blanks).
        assert_eq!(
            extract_quoted_literals(r#"filter(ev => (a + " ").includes("say \"hi\"\\"))"#),
            vec![" ".to_string(), r#"say "hi"\"#.to_string()]
        );
        assert!(extract_quoted_literals("matches({a: 1})").is_empty());
    }

    // The needles of a `runs events` query are its search terms, never the
    // literals its own helpers hold — even when a term reads like part of
    // the filter's syntax.
    #[test]
    fn query_needles_takes_the_search_terms_only() {
        let query = crate::event_set_dsl::substring_filter(&[
            "Raft".to_string(),
            r#"a "]) quote"#.to_string(),
        ]);
        assert_eq!(
            query_needles(&query),
            vec!["raft".to_string(), r#"a "]) quote"#.to_string()]
        );
        // A query snouty did not build keeps every literal it holds.
        assert_eq!(
            query_needles(r#"contains({output_text: "slow request"})"#),
            vec!["slow request".to_string()]
        );
    }

    #[test]
    fn mock_query_param_decodes_form_encoded_value() {
        // reqwest form-encodes spaces as '+' and other bytes as %XX.
        let q = Some("q=slow+request&other=a%26b");
        assert_eq!(mock_query_param(q, "q"), Some("slow request".to_string()));
        assert_eq!(mock_query_param(q, "other"), Some("a&b".to_string()));
        assert_eq!(mock_query_param(q, "missing"), None);
        assert_eq!(mock_query_param(None, "q"), None);
    }

    #[test]
    fn mock_check_user_agent_requires_non_empty_header() {
        let with = "GET /api/v0/runs HTTP/1.1\r\nUser-Agent: snouty/0.0.0\r\n\r\n";
        let lowercase = "GET /api/v0/runs HTTP/1.1\r\nuser-agent: snouty/0.0.0\r\n\r\n";
        let empty = "GET /api/v0/runs HTTP/1.1\r\nUser-Agent:   \r\n\r\n";
        let missing = "GET /api/v0/runs HTTP/1.1\r\n\r\n";

        assert!(mock_check_user_agent(with));
        assert!(mock_check_user_agent(lowercase));
        assert!(!mock_check_user_agent(empty));
        assert!(!mock_check_user_agent(missing));
    }

    #[test]
    fn mock_route_search_run_events_matches_after_decoding() {
        let (status, body) = mock_route_search_run_events("run-1", Some("q=slow+request"));
        assert_eq!(status, 200);
        assert!(
            body.contains("slow request"),
            "expected match for decoded query, got: {body}"
        );
    }

    #[test]
    fn mock_route_search_run_events_caps_at_limit() {
        // parallel_driver_fetch matches three events; limit=1 keeps the first.
        let (status, all) = mock_route_search_run_events("run-1", Some("q=parallel_driver_fetch"));
        assert_eq!(status, 200);
        assert_eq!(all.lines().count(), 3, "fixture should match three events");

        let (status, capped) =
            mock_route_search_run_events("run-1", Some("q=parallel_driver_fetch&limit=1"));
        assert_eq!(status, 200);
        assert_eq!(capped.lines().count(), 1);
        assert!(
            capped.contains(r#""vtime":"400.5""#),
            "the first match should be retained, got: {capped}"
        );
    }

    #[test]
    fn mock_route_search_events_ends_the_stream_at_limit() {
        let body = |limit: Option<u64>| {
            let mut request = serde_json::json!({
                "query": r#"contains({output_text: "parallel_driver_fetch"})"#,
            });
            if let Some(limit) = limit {
                request["limit"] = limit.into();
            }
            request.to_string()
        };

        let (status, all, _) = mock_route_search_events("run-1", &body(None));
        assert_eq!(status, 200);
        assert_eq!(all.lines().count(), 3, "fixture should match three events");

        let (status, capped, _) = mock_route_search_events("run-1", &body(Some(1)));
        assert_eq!(status, 200);
        assert_eq!(capped.lines().count(), 1);
        assert!(
            capped.contains(r#""vtime":"400.5""#),
            "the first match should be retained, got: {capped}"
        );
    }

    #[test]
    fn mock_route_execute_command_branches_on_script() {
        let body = |script: &str, timeout: u64| {
            format!(
                r#"{{"moment":{{"input_hash":"-1","vtime":1.0}},"script":"{script}","timeout_seconds":{timeout}}}"#
            )
        };

        // The default script succeeds with output on both streams, and the
        // response moments carry the branch hash, never the requested one.
        let (status, out) = mock_route_execute_command("run-2", &body("uname -a", 30));
        assert_eq!(status, 200);
        assert!(out.contains(r#""stream":"stderr""#), "got: {out}");
        assert!(out.ends_with("\n"));
        assert!(out.contains(MOCK_EXEC_BRANCH_HASH));
        assert!(!out.contains(r#""input_hash":"-1""#), "got: {out}");

        let (_, out) = mock_route_execute_command("run-2", &body("exit 5", 30));
        assert!(out.contains(r#""exit_code":5"#), "got: {out}");

        let (_, out) = mock_route_execute_command("run-2", &body("sleep 60", 30));
        assert!(out.ends_with("{\"type\":\"timed_out\"}\n"), "got: {out}");

        // print-timeout echoes the timeout_seconds the server received, so a
        // spec can verify the --timeout flag reaches the wire.
        let (_, out) = mock_route_execute_command("run-2", &body("print-timeout", 45));
        assert!(out.contains("timeout_seconds=45"), "got: {out}");

        let (_, out) = mock_route_execute_command("run-2", &body("truncate-stream", 30));
        assert!(!out.contains("exited"), "got: {out}");
        assert!(!out.contains("timed_out"), "got: {out}");
    }

    #[test]
    fn mock_route_execute_command_requires_a_live_session() {
        let body = r#"{"moment":{"input_hash":"-1","vtime":1.0},"script":"uname -a"}"#;
        // run-1 is completed: no live session, the API's verbatim 400.
        let (status, out) = mock_route_execute_command("run-1", body);
        assert_eq!(status, 400);
        assert!(out.contains("minting session token failed"), "got: {out}");
        // Unknown runs 404 with the live endpoint's unhelpful body, which
        // never names the run — snouty's "run not found" translation depends
        // on the status, not the message.
        let (status, out) = mock_route_execute_command("no-such-run", body);
        assert_eq!(status, 404);
        assert_eq!(out, r#"{"message":"Resource not found"}"#);
    }

    #[test]
    fn mock_request_body_splits_after_headers() {
        let request = "POST /x HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(mock_request_body(request), "{}");
        assert_eq!(mock_request_body("GET /x HTTP/1.1\r\n\r\n"), "");
        assert_eq!(mock_request_body("garbage"), "");
    }

    #[test]
    fn mock_find_header_end_locates_the_blank_line() {
        assert_eq!(
            mock_find_header_end(b"POST /x HTTP/1.1\r\nA: b\r\n\r\nbody"),
            Some(26)
        );
        assert_eq!(mock_find_header_end(b"POST /x HTTP/1.1\r\nA: b\r\n"), None);
    }

    #[test]
    fn docker_info_supports_linux_registry_decides_by_os_type() {
        // Empty output is tolerated: an old or terse runtime still counts.
        assert!(docker_info_supports_linux_registry(""));
        assert!(docker_info_supports_linux_registry("\n"));
        // Linux matches case-insensitively.
        assert!(docker_info_supports_linux_registry("linux\n"));
        assert!(docker_info_supports_linux_registry("Linux\n"));
        // Any other OSType is rejected.
        assert!(!docker_info_supports_linux_registry("windows\n"));
    }

    #[cfg(unix)]
    #[test]
    fn oci_registry_drop_removes_container_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("runtime.log");
        let runtime_path = dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                "#!/bin/sh\nprintf '%s\n' \"$*\" >> \"{}\"\nexit 0\n",
                log_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&runtime_path, perms).unwrap();

        let child = Command::new("sh").args(["-c", "sleep 30"]).spawn().unwrap();
        let container_name = "snouty-test-registry-drop".to_string();

        {
            let _registry = OCIRegistry {
                host_port: "127.0.0.1:5000".to_string(),
                owned: Some(OwnedRegistry {
                    child,
                    runtime: runtime_path.display().to_string(),
                    container_name: container_name.clone(),
                    log: tempfile::NamedTempFile::new().unwrap(),
                }),
            };
        }

        let log = fs::read_to_string(log_path).unwrap();
        assert!(
            log.lines()
                .any(|line| line == format!("rm -f {container_name}")),
            "expected cleanup command in log, got: {log}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn ensure_registry_image_available_pulls_registry_image() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("runtime.log");
        let runtime_path = dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                "#!/bin/sh\nprintf '%s\n' \"$*\" >> \"{}\"\nexit 0\n",
                log_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&runtime_path, perms).unwrap();

        assert_eq!(
            ensure_registry_image_available(runtime_path.to_str().unwrap()),
            Ok(())
        );

        let log = fs::read_to_string(log_path).unwrap();
        assert!(
            log.lines().any(|line| line == "pull registry:2"),
            "expected registry pull command in log, got: {log}"
        );
        assert_eq!(
            log.lines()
                .filter(|line| *line == "pull registry:2")
                .count(),
            1,
            "a pull that works must not be retried, got: {log}"
        );
    }
}
