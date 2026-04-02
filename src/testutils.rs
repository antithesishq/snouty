use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::container::{ContainerRuntime, DockerRuntime, PodmanRuntime, is_podman_in_disguise};

/// Return all container runtimes that are actually usable on this machine.
/// Skips `docker` if it is actually podman in disguise.
pub fn available_runtimes() -> Vec<Box<dyn ContainerRuntime>> {
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

pub struct OCIRegistry {
    child: Child,
    runtime: String,
    container_name: String,
    port: u16,
}

impl OCIRegistry {
    /// Try to start a local OCI registry container.  Returns `None` when the
    /// container cannot be launched.
    pub fn start(runtime: &dyn ContainerRuntime) -> Option<Self> {
        if !runtime_supports_linux_registry_image(runtime.name()) {
            eprintln!(
                "skipping: OCI registry image requires Linux containers for {}",
                runtime.name()
            );
            return None;
        }
        if !ensure_registry_image_available(runtime.name()) {
            skip_or_fail(&format!(
                "OCI registry image could not be pulled with {}",
                runtime.name()
            ));
            return None;
        }

        let port = reserve_local_port();
        let container_name = format!("snouty-test-registry-{}-{}", std::process::id(), port);
        let publish = format!("127.0.0.1:{port}:5000");
        let runtime_name = runtime.name().to_owned();

        let child = Command::new(&runtime_name)
            .args([
                "run",
                "--rm",
                "-p",
                &publish,
                "--name",
                &container_name,
                "registry:2",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!("failed to start OCI registry with {}: {e}", runtime.name())
            });

        let mut registry = Self {
            child,
            runtime: runtime_name,
            container_name,
            port,
        };
        if !registry.wait_until_ready() {
            registry.cleanup_container();
            skip_or_fail(&format!(
                "OCI registry could not start with {}",
                runtime.name()
            ));
            return None;
        }
        Some(registry)
    }

    pub fn host_port(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Returns `true` when the registry is ready, `false` when it exited or
    /// timed out.
    fn wait_until_ready(&mut self) -> bool {
        for _ in 0..200 {
            if registry_v2_ping(self.port) {
                return true;
            }
            if let Some(_status) = self
                .child
                .try_wait()
                .expect("failed to poll OCI registry child process")
            {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
        false
    }

    fn cleanup_container(&self) {
        let _ = Command::new(&self.runtime)
            .args(["rm", "-f", &self.container_name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for OCIRegistry {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        self.cleanup_container();
    }
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

/// Check whether `{runtime} compose version` succeeds.
pub fn has_compose(runtime: &str) -> bool {
    Command::new(runtime)
        .args(["compose", "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
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
    let with_compose: Vec<_> = runtimes
        .into_iter()
        .filter(|rt| has_compose(rt.name()))
        .collect();
    if with_compose.is_empty() {
        skip_or_fail("no runtime has docker compose support");
    }
    with_compose
}

fn reserve_local_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn registry_v2_ping(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };

    let request =
        format!("GET /v2/ HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
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

fn ensure_registry_image_available(runtime: &str) -> bool {
    Command::new(runtime)
        .args(["pull", "registry:2"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn filtered_path_without_binary(binary: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    let filtered = std::env::split_paths(&path)
        .filter(|dir| !directory_contains_binary(dir, binary))
        .collect::<Vec<_>>();
    std::env::join_paths(filtered)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

fn directory_contains_binary(dir: &Path, binary: &str) -> bool {
    dir.join(binary).is_file()
}

/// A mock Antithesis API server for development and testing.
///
/// Handles:
/// - `GET  /api/v1/runs` — paginated run listing
/// - `GET  /api/v1/runs/{run_id}` — run detail (returns a fixed completed run)
/// - `POST /api/v1/launch/{launcher_name}` — returns a mock run_id
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
        let token = mock_generate_token();
        let expected_token = token.clone();

        let handle = thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 8192];
                let bytes_read = match stream.read(&mut buf) {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                let request = String::from_utf8_lossy(&buf[..bytes_read]);

                let (status, body, content_type) = if !mock_check_auth(&request, &expected_token) {
                    (
                        401,
                        r#"{"message":"Invalid or expired bearer token."}"#.to_string(),
                        "application/json",
                    )
                } else {
                    let (method, path) = mock_parse_request_line(&request);
                    mock_route(&method, &path, empty)
                };

                let response = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                    status,
                    content_type,
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

fn mock_generate_token() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("failed to generate random token");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

/// Parse the first line of an HTTP request into (method, path).
fn mock_parse_request_line(request: &str) -> (String, String) {
    let first_line = request.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    (method, path)
}

/// Route a request and return (status_code, response_body, content_type).
fn mock_route(method: &str, path: &str, empty: bool) -> (u16, String, &'static str) {
    // Split path and query string
    let (path_part, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path.as_ref(), None),
    };

    let json = "application/json";
    let ndjson = "application/x-ndjson";

    match (method, path_part) {
        ("GET", "/api/v1/runs") => {
            let (s, b) = mock_route_list_runs(query, empty);
            (s, b, json)
        }
        ("GET", p) if p.starts_with("/api/v1/runs/") => {
            let rest = &p["/api/v1/runs/".len()..];
            if let Some(run_id) = rest.strip_suffix("/build_logs") {
                let (s, b) = mock_route_get_run_build_logs(run_id);
                (s, b, ndjson)
            } else if let Some(run_id) = rest.strip_suffix("/logs") {
                let (s, b) = mock_route_get_run_logs(run_id);
                (s, b, ndjson)
            } else {
                let (s, b) = mock_route_get_run(rest);
                (s, b, json)
            }
        }
        ("POST", p) if p.starts_with("/api/v1/launch/") => {
            let (s, b) = mock_route_launch();
            (s, b, json)
        }
        _ => (404, r#"{"message":"not found"}"#.to_string(), json),
    }
}

fn mock_query_param<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == key).then_some(v)
        })
    })
}

const MOCK_RUNS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "run-1",
        "completed",
        "test",
        "2025-03-20T02:00:00Z",
        "nightly",
    ),
    (
        "run-2",
        "in_progress",
        "mvd",
        "2025-03-19T14:00:00Z",
        "debug",
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
    let start = match after {
        Some("cursor-1") => 1,
        _ => 0,
    };

    let mut runs = Vec::new();
    for &(id, status, type_, created, launcher) in &MOCK_RUNS[start..] {
        if let Some(f) = status_filter {
            if status != f {
                continue;
            }
        }
        if let Some(f) = launcher_filter {
            if launcher != f {
                continue;
            }
        }
        runs.push(format!(
            r#"{{"run_id":"{id}","status":"{status}","type":"{type_}","created_at":"{created}","launcher":"{launcher}"}}"#,
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

fn mock_route_get_run(run_id: &str) -> (u16, String) {
    (
        200,
        format!(
            r#"{{"run_id":"{}","status":"completed","type":"test","created_at":"2025-03-20T02:00:00Z","started_at":"2025-03-20T02:01:12Z","completed_at":"2025-03-20T02:31:45Z","launcher":"nightly","links":{{"triage_report":"https://demo.antithesis.com/reports/{}"}}}}"#,
            run_id, run_id
        ),
    )
}

fn mock_route_get_run_build_logs(_run_id: &str) -> (u16, String) {
    let lines = [
        r#"{"timestamp":"2025-03-20T02:01:12Z","stream":"out","text":"Building image payments-service..."}"#,
        r#"{"timestamp":"2025-03-20T02:01:15Z","stream":"err","text":"Warning: deprecated feature"}"#,
        r#"{"timestamp":"2025-03-20T02:01:20Z","stream":"out","text":"Build complete"}"#,
    ];
    (200, lines.join("\n") + "\n")
}

fn mock_route_get_run_logs(_run_id: &str) -> (u16, String) {
    let lines = [
        r#"{"output_text":"{\"level\":\"info\",\"msg\":\"starting\"}","source":{"container":"app","name":"app","stream":"out"},"moment":{"input_hash":"-123","vtime":"1.0","session_id":"sess-1"}}"#,
        r#"{"output_text":"{\"level\":\"warn\",\"msg\":\"slow request\"}","source":{"container":"app","name":"app","stream":"error"},"moment":{"input_hash":"-456","vtime":"2.0","session_id":"sess-1"}}"#,
        // Record whose output_text contains a JSON-escaped newline (\n).
        // Verifies that --json emits this as a single output line.
        r#"{"output_text":"line one\nline two","source":{"container":"app","name":"app","stream":"out"},"moment":{"input_hash":"-789","vtime":"3.0"}}"#,
        r#"{"IPT_bytes_out":563000,"output_text":"W0320 15:07:26.913251       1 control.go:315] Error setting vault 10.0.1.123:8003 value to 64: Post \"http://10.0.1.123:8003/\": dial tcp 10.0.1.123:8003: i/o timeout (Client.Timeout exceeded while awaiting headers)","source":{"container":"control","name":"service_control","stream":"error"},"moment":{"input_hash":"-7835669064649885519","vtime":"73.94233945617452"}}"#,
        r#"{"antithesis_assert":{"assert_type":"always","condition":false,"details":null,"display_type":"AlwaysOrUnreachable","hit":false,"id":"Counter's value retrieved","location":{"begin_column":0,"begin_line":87,"class":"","file":"/go/src/antithesis/control/control.go","function":"get"},"message":"Counter's value retrieved","must_hit":false},"IPT_bytes_out":1837376,"source":{"container":"control","name":"control","pid":1},"moment":{"input_hash":"-4735081784258020614","vtime":"311.8487535319291"}}"#,
        r#"{"input_byte":225,"input_count":659,"IPT_bytes_out":2130016,"prev_input_hash":"335559623669971735","moment":{"input_hash":"4869836942553022903","vtime":"498.2362972020637"}}"#,
    ];
    (200, lines.join("\n") + "\n")
}

fn mock_route_launch() -> (u16, String) {
    (200, r#"{"run_id":"mock-run-id"}"#.to_string())
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
    fn directory_contains_plain_binary() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snouty-update"), "").unwrap();
        assert!(directory_contains_binary(dir.path(), "snouty-update"));
    }

    #[test]
    fn docker_info_with_linux_os_type_supports_registry() {
        assert!(docker_info_supports_linux_registry("linux\n"));
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
                child,
                runtime: runtime_path.display().to_string(),
                container_name: container_name.clone(),
                port: 5000,
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

        assert!(ensure_registry_image_available(
            runtime_path.to_str().unwrap()
        ));

        let log = fs::read_to_string(log_path).unwrap();
        assert!(
            log.lines().any(|line| line == "pull registry:2"),
            "expected registry pull command in log, got: {log}"
        );
    }
}
