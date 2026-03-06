use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;

#[derive(Debug, Default)]
struct MockDocsServerState {
    current_etag: String,
    if_none_match_headers: Vec<Option<String>>,
    include_etag: bool,
    user_agent: Option<String>,
}

struct MockDocsServer {
    url: String,
    state: Arc<Mutex<MockDocsServerState>>,
}

impl MockDocsServer {
    fn start() -> Self {
        Self::start_with_etag("test-etag")
    }

    fn start_with_etag(initial_etag: &str) -> Self {
        Self::start_with_config(initial_etag, true)
    }

    fn start_without_etag() -> Self {
        Self::start_with_config("test-etag", false)
    }

    fn start_with_config(initial_etag: &str, include_etag: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockDocsServerState {
            current_etag: initial_etag.to_string(),
            if_none_match_headers: Vec::new(),
            include_etag,
            user_agent: None,
        }));
        let observed_state = Arc::clone(&state);
        let db = include_bytes!("fixtures/docs.db");

        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 8192];
                let bytes_read = stream.read(&mut buf).unwrap();
                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                let user_agent = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("user-agent") {
                        Some(value.trim().to_owned())
                    } else {
                        None
                    }
                });
                let if_none_match = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("if-none-match") {
                        Some(value.trim().to_owned())
                    } else {
                        None
                    }
                });

                let (etag, include_etag, not_modified) = {
                    let mut state = observed_state.lock().unwrap();
                    state.user_agent = user_agent;
                    state.if_none_match_headers.push(if_none_match.clone());
                    let etag = state.current_etag.clone();
                    let include_etag = state.include_etag;
                    let not_modified = if_none_match.as_deref() == Some(etag.as_str());
                    (etag, include_etag, not_modified)
                };

                if not_modified {
                    stream
                        .write_all(b"HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\n\r\n")
                        .unwrap();
                    continue;
                }

                let response = if include_etag {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nETag: {}\r\n\r\n",
                        db.len(),
                        etag
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
                        db.len()
                    )
                };
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(db).unwrap();
            }
        });

        Self {
            url: format!("http://{}", addr),
            state,
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn user_agent(&self) -> Option<String> {
        self.state.lock().unwrap().user_agent.clone()
    }

    fn if_none_match_headers(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().if_none_match_headers.clone()
    }

    fn set_etag(&self, etag: &str) {
        self.state.lock().unwrap().current_etag = etag.to_string();
    }
}

fn snouty() -> Command {
    let mut cmd = cargo_bin_cmd!("snouty");
    cmd.env("RUST_LOG", "debug");
    // Clear all ANTITHESIS_* inputs the CLI reads so tests don't depend on
    // env leaked in from the caller or CI runner.
    for env_var in [
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
        "ANTITHESIS_TENANT",
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_REPOSITORY",
        "ANTITHESIS_DOCS_URL",
        "ANTITHESIS_DOCS_DB_PATH",
    ] {
        cmd.env_remove(env_var);
    }
    cmd
}

/// Start a simple mock HTTP server that returns a fixed response.
/// Returns the server URL and a handle to stop it.
fn start_mock_server(response_body: &'static str, status: u16) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    thread::spawn(move || {
        if let Some(mut stream) = listener.incoming().flatten().next() {
            // Read request (we don't care about the content for these tests)
            let mut buf = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut buf);

            // Send response
            let response = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                status,
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });

    url
}

fn expected_docs_user_agent() -> String {
    format!(
        "snouty/{} ({}; {}; rust{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

fn cached_docs_db_path(cache_dir: &TempDir) -> std::path::PathBuf {
    cache_dir.path().join("snouty").join("docs.db")
}

fn docs_search_json(query_args: &[&str]) -> Vec<serde_json::Value> {
    let mut args = vec!["docs", "--offline", "search", "--json"];
    args.extend_from_slice(query_args);

    let output = snouty_docs()
        .args(&args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    serde_json::from_slice::<serde_json::Value>(&output)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

fn snouty_with_mock(mock_url: &str) -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_USERNAME", "testuser")
        .env("ANTITHESIS_PASSWORD", "testpass")
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", mock_url);
    cmd
}

// === Tests that don't need API (version, help) ===

#[test]
fn version_prints_version() {
    snouty()
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^snouty \d+\.\d+\.\d+").unwrap());
}

#[test]
fn help_shows_subcommands() {
    snouty()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("debug"))
        .stdout(predicate::str::contains("version"));
}

#[test]
fn help_shows_api_subcommand() {
    snouty()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("api"));
}

#[test]
fn api_help_shows_webhook() {
    snouty()
        .args(["api", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("webhook"));
}

// === Tests for `run` command (typed flags) ===

#[test]
fn run_with_typed_flags() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "run",
            "-w",
            "basic_test",
            "--test-name",
            "my-test",
            "--description",
            "nightly test run",
            "--config-image",
            "config:latest",
            "--duration",
            "30",
            "--recipients",
            "team@example.com",
            "--source",
            "ci-pipeline",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.test_name": "my-test""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.description": "nightly test run""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.config_image": "config:latest""#,
        ))
        .stderr(predicate::str::contains(r#""antithesis.duration": "30""#))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.source": "ci-pipeline""#,
        ));
}

#[test]
fn run_with_ephemeral_flag() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30", "--ephemeral"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.is_ephemeral": "true""#,
        ));
}

#[test]
fn run_without_ephemeral_omits_key() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains("is_ephemeral").not());
}

#[test]
fn run_with_param_flag() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "run",
            "-w",
            "basic_test",
            "--duration",
            "30",
            "--param",
            "my.custom.prop=value",
            "--param",
            "antithesis.integrations.github.callback_url=https://example.com/cb",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""my.custom.prop": "value""#))
        .stderr(predicate::str::contains(
            r#""antithesis.integrations.github.callback_url": "https://example.com/cb""#,
        ));
}

#[test]
fn run_param_cannot_override_typed_flag() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "run",
            "-w",
            "basic_test",
            "--duration",
            "30",
            "--param",
            "antithesis.duration=60",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be set via --param"));
}

#[test]
fn run_duration_rejects_non_numeric() {
    snouty()
        .args(["run", "-w", "basic_test", "--duration", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid value"));
}

#[test]
fn run_no_stdin_flag() {
    // --stdin should not be accepted on `run`
    snouty()
        .args(["run", "-w", "basic_test", "--stdin", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

#[test]
fn run_no_trailing_raw_args() {
    // Old-style --antithesis.duration 30 should not be accepted on `run`
    snouty()
        .args(["run", "-w", "basic_test", "--antithesis.duration", "30"])
        .assert()
        .failure();
}

#[test]
fn run_fails_without_webhook() {
    snouty()
        .args(["run", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--webhook"));
}

#[test]
fn run_fails_without_parameters() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no parameters provided"));
}

#[test]
fn run_reports_api_errors() {
    let mock_url = start_mock_server(r#"{"error": "bad request"}"#, 400);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("API error: 400"));
}

#[test]
fn run_fails_without_credentials() {
    snouty()
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing environment variable"));
}

// === Config directory tests (on `run` with typed flags) ===

#[test]
fn run_config_rejects_nonexistent_dir() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            "/nonexistent/path",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a directory"));
}

#[test]
fn run_config_rejects_dir_without_compose() {
    let dir = TempDir::new().unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("docker-compose"));
}

#[test]
fn run_config_rejects_yml_extension() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires docker-compose.yaml"));
}

#[test]
fn run_config_conflicts_with_config_image_param() {
    // clap conflicts_with should reject --config + --config-image
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--config-image",
            "some-image:latest",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--config"));
}

#[test]
fn run_config_requires_registry_env() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ANTITHESIS_REPOSITORY"));
}

#[test]
fn run_config_long_flag_accepted() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "--config",
            "/nonexistent/path",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a directory"));
}

#[test]
fn run_config_conflicts_with_param_config_image() {
    // --config/-c + --param antithesis.config_image=X should error
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be set via --param"));
}

// === Tests for `api webhook` command (raw args) ===

#[test]
fn api_webhook_with_cli_args() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.test_name",
            "my-test",
            "--antithesis.description",
            "nightly test run",
            "--antithesis.config_image",
            "config:latest",
            "--antithesis.images",
            "app:latest",
            "--antithesis.duration",
            "30",
            "--antithesis.report.recipients",
            "team@example.com",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.test_name": "my-test""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.description": "nightly test run""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.config_image": "config:latest""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.images": "app:latest""#,
        ))
        .stderr(predicate::str::contains(r#""antithesis.duration": "30""#))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ));
}

#[test]
fn api_webhook_with_stdin_json() {
    let mock_url = start_mock_server(r#"{"launched": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["api", "webhook", "-w", "basic_test", "--stdin"])
        .write_stdin(r#"{"antithesis.duration": "60", "antithesis.is_ephemeral": "true"}"#)
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "60""#))
        .stderr(predicate::str::contains(
            r#""antithesis.is_ephemeral": "true""#,
        ));
}

#[test]
fn api_webhook_with_custom_properties() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
            "--my.custom.prop",
            "value",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""my.custom.prop": "value""#));
}

#[test]
fn api_webhook_stdin_flag_required_for_stdin_input() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .write_stdin(r#"{"antithesis.duration": "SHOULD_BE_IGNORED"}"#)
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "30""#))
        .stderr(predicate::str::contains("SHOULD_BE_IGNORED").not());
}

#[test]
fn api_webhook_with_k8s_webhook() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "--webhook",
            "basic_k8s_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .success();
}

#[test]
fn api_webhook_with_custom_webhook() {
    let mock_url = start_mock_server(r#"{"status": "ok"}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "my_custom_webhook",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .success();
}

#[test]
fn api_webhook_fails_on_invalid_json_stdin() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["api", "webhook", "-w", "basic_test", "--stdin"])
        .write_stdin("not valid json")
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid JSON"));
}

#[test]
fn api_webhook_fails_on_missing_value() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing value"));
}

#[test]
fn api_webhook_fails_on_unexpected_arg() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["api", "webhook", "-w", "basic_test", "notaflag"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

#[test]
fn api_webhook_fails_without_webhook() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["api", "webhook", "--antithesis.duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--webhook"));
}

#[test]
fn api_webhook_reports_api_errors() {
    let mock_url = start_mock_server(r#"{"error": "bad request"}"#, 400);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("API error: 400"));
}

#[test]
fn api_webhook_fails_without_credentials() {
    snouty()
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing environment variable"));
}

#[test]
fn api_webhook_fails_without_parameters() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["api", "webhook", "-w", "basic_test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no parameters provided"));
}

#[test]
fn api_webhook_merges_stdin_json_with_cli_args() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--stdin",
            "--antithesis.report.recipients",
            "team@example.com",
        ])
        .write_stdin(r#"{"antithesis.duration": "60", "antithesis.description": "from stdin"}"#)
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "60""#))
        .stderr(predicate::str::contains(
            r#""antithesis.description": "from stdin""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ));
}

#[test]
fn api_webhook_cli_args_override_stdin_json() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--stdin",
            "--antithesis.duration",
            "120",
        ])
        .write_stdin(r#"{"antithesis.duration": "60", "antithesis.description": "from stdin"}"#)
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "120""#))
        .stderr(predicate::str::contains(
            r#""antithesis.description": "from stdin""#,
        ));
}

#[test]
fn api_webhook_success_outputs_valid_json() {
    let mock_url = start_mock_server(r#"{"session_id": "abc123", "status": "launched"}"#, 200);

    let output = snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let stdout = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should be valid JSON");
    assert!(parsed.is_object());
}

#[test]
fn api_webhook_error_outputs_string_on_stderr() {
    let mock_url = start_mock_server(r#"{"error": "something went wrong"}"#, 500);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("API error: 500"))
        .stdout(predicate::str::is_empty());
}

#[test]
fn api_webhook_success_does_not_print_email_eta() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("Expect a report email").not());
}

#[test]
fn run_prints_email_eta() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Expect a report email"));
}

#[test]
fn api_webhook_no_config_flag() {
    // -c/--config should not be accepted on `api webhook`
    snouty()
        .args([
            "api",
            "webhook",
            "-w",
            "basic_test",
            "-c",
            "/some/path",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .failure();
}

// === Tests for debug command (unchanged) ===

#[test]
fn debug_with_cli_args() {
    let mock_url = start_mock_server(r#"{"session": "started"}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--antithesis.debugging.input_hash",
            "abc123",
            "--antithesis.debugging.session_id",
            "sess-456",
            "--antithesis.debugging.vtime",
            "1234567890",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "abc123""#,
        ));
}

#[test]
fn debug_with_moment_from_format() {
    let mock_url = start_mock_server(r#"{"debugging": true}"#, 200);
    let moment_input = r#"Moment.from({ session_id: "f89d5c11f5e3bf5e4bb3641809800cee-44-22", input_hash: "6057726200491963783", vtime: 329.8037810830865 })"#;

    snouty_with_mock(&mock_url)
        .args(["debug", "--stdin"])
        .write_stdin(moment_input)
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.session_id": "f89d5c11f5e3bf5e4bb3641809800cee-44-22""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "6057726200491963783""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.vtime": "329.8037810830865""#,
        ));
}

#[test]
fn debug_with_stdin_json() {
    let mock_url = start_mock_server(r#"{"ok": true}"#, 200);
    let json = r#"{
        "antithesis.debugging.input_hash": "abc",
        "antithesis.debugging.session_id": "sess",
        "antithesis.debugging.vtime": "123"
    }"#;

    snouty_with_mock(&mock_url)
        .args(["debug", "--stdin"])
        .write_stdin(json)
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "abc""#,
        ));
}

// === Validation error tests ===

#[test]
fn debug_fails_missing_required_fields() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["debug", "--antithesis.debugging.input_hash", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("validation failed"));
}

#[test]
fn debug_rejects_custom_properties() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--antithesis.debugging.input_hash",
            "abc",
            "--antithesis.debugging.session_id",
            "sess",
            "--antithesis.debugging.vtime",
            "123",
            "--my.custom.prop",
            "value",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("validation failed"));
}

// === Completions tests ===

#[test]
fn completions_bash_outputs_script() {
    snouty()
        .args(["completions", "bash"])
        .assert()
        .success()
        .stdout(predicate::str::contains("complete"))
        .stdout(predicate::str::contains("snouty"));
}

#[test]
fn completions_unsupported_shell_fails() {
    snouty()
        .args(["completions", "nushell"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unsupported shell: nushell"));
}

// === Tests for merging stdin and CLI args (debug) ===

#[test]
fn debug_merges_moment_with_cli_args() {
    let mock_url = start_mock_server(r#"{"debugging": true}"#, 200);
    let moment_input = r#"Moment.from({ session_id: "f89d5c11f5e3bf5e4bb3641809800cee-44-22", input_hash: "6057726200491963783", vtime: 329.8037810830865 })"#;

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--stdin",
            "--antithesis.report.recipients",
            "team@example.com",
        ])
        .write_stdin(moment_input)
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.session_id": "f89d5c11f5e3bf5e4bb3641809800cee-44-22""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "6057726200491963783""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.vtime": "329.8037810830865""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ));
}

// === Tests for `docs` commands ===

fn fixture_db() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/docs.db")
        .to_str()
        .unwrap()
        .to_string()
}

fn snouty_docs() -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_DOCS_DB_PATH", fixture_db());
    cmd
}

#[test]
fn docs_search_returns_results() {
    snouty_docs()
        .args(["docs", "--offline", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stdout(predicate::str::contains("Docker basics"));
}

#[test]
fn docs_env_db_path_implies_offline() {
    snouty_docs()
        .env("ANTITHESIS_DOCS_URL", "http://127.0.0.1:1")
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stderr(predicate::str::contains("failed to update docs").not());
}

#[test]
fn docs_update_sets_custom_user_agent() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    assert_eq!(
        mock_server.user_agent().as_deref(),
        Some(expected_docs_user_agent().as_str()),
    );
}

#[test]
fn docs_update_failure_with_cached_db_warns_and_uses_cache() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", "http://127.0.0.1:1")
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stderr(predicate::str::contains(
            "Warning: failed to update docs, falling back to cached docs",
        ));
}

#[test]
fn docs_auto_update_reuses_cached_db_until_etag_changes() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start_with_etag("test-etag-1");

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    mock_server.set_etag("test-etag-2");

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    assert_eq!(
        mock_server.if_none_match_headers(),
        vec![
            None,
            Some("test-etag-1".to_string()),
            Some("test-etag-1".to_string())
        ]
    );

    let etag_path = cache_dir.path().join("snouty").join("docs.db.etag");
    assert_eq!(std::fs::read_to_string(etag_path).unwrap(), "test-etag-2");
}

#[test]
fn docs_downloaded_db_is_read_only() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success();

    let metadata = std::fs::metadata(cached_docs_db_path(&cache_dir)).unwrap();
    assert!(metadata.permissions().readonly());
}

#[test]
fn docs_update_requires_etag_header() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start_without_etag();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "server did not include an ETag header in the response",
        ));
}

#[test]
fn docs_search_json_flag() {
    let results = docs_search_json(&["sdk"]);
    assert!(!results.is_empty());

    let sdk_entry = results
        .iter()
        .find(|entry| entry.get("path").and_then(|v| v.as_str()) == Some("/docs/sdk/python_sdk/"));
    let sdk_entry = sdk_entry.expect("expected sdk result in JSON output");

    assert_eq!(
        sdk_entry.get("title").and_then(|v| v.as_str()),
        Some("Python SDK")
    );
    assert!(
        sdk_entry
            .get("snippet")
            .and_then(|v| v.as_str())
            .is_some_and(|snippet| snippet.contains("sdk-related result"))
    );
}

#[test]
fn docs_search_respects_limit() {
    let full_results = docs_search_json(&["test"]);
    assert!(full_results.len() > 2);

    let limited_output = snouty_docs()
        .args(["docs", "--offline", "search", "--json", "-n", "2", "test"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let limited_results = serde_json::from_slice::<serde_json::Value>(&limited_output)
        .unwrap()
        .as_array()
        .unwrap()
        .clone();

    assert_eq!(limited_results.len(), 2);
    assert_eq!(limited_results, full_results[..2]);
}

#[test]
fn docs_search_format_flag_is_rejected() {
    snouty_docs()
        .args(["docs", "--offline", "search", "--format", "json", "sdk"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument '--format'"));
}

#[test]
fn docs_search_list_outputs_only_paths() {
    snouty_docs()
        .args(["docs", "--offline", "search", "--list", "-n", "2", "test"])
        .assert()
        .success()
        .stdout(concat!(
            "/docs/reference/test_patterns/\n",
            "/docs/environment/fault_injection/\n",
        ))
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_list_json_outputs_path_array() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "search",
            "--list",
            "--json",
            "-n",
            "2",
            "test",
        ])
        .assert()
        .success()
        .stdout(concat!(
            "[\n",
            "  \"/docs/reference/test_patterns/\",\n",
            "  \"/docs/environment/fault_injection/\"\n",
            "]\n",
        ))
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_no_query_fails() {
    snouty_docs()
        .args(["docs", "--offline", "search"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("search query required"));
}

#[test]
fn docs_search_no_results() {
    snouty_docs()
        .args(["docs", "--offline", "search", "xyznonexistent999"])
        .assert()
        .success()
        .stderr(predicate::str::contains("No results found"));
}

#[test]
fn docs_search_json_no_results_returns_empty_array() {
    snouty_docs()
        .args(["docs", "--offline", "search", "--json", "xyznonexistent999"])
        .assert()
        .success()
        .stdout("[]\n")
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_list_json_no_results_returns_empty_array() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "search",
            "--list",
            "--json",
            "xyznonexistent999",
        ])
        .assert()
        .success()
        .stdout("[]\n")
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_missing_db_with_offline_tells_user_to_remove_offline() {
    let cache_dir = TempDir::new().unwrap();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .args(["docs", "--offline", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Documentation database not found. Remove --offline to download it.",
        ));
}

#[test]
fn docs_sqlite_missing_db_with_env_path_tells_user_to_fix_path() {
    snouty()
        .env("ANTITHESIS_DOCS_DB_PATH", "/tmp/does-not-exist-docs.db")
        .args(["docs", "sqlite"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Documentation database not found at /tmp/does-not-exist-docs.db. Point ANTITHESIS_DOCS_DB_PATH at an existing file.",
        ));
}

#[test]
fn docs_show_existing_page() {
    snouty_docs()
        .args(["docs", "--offline", "show", "getting_started"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"))
        .stdout(predicate::str::contains("Docker Compose"));
}

#[test]
fn docs_show_strips_leading_slash() {
    snouty_docs()
        .args(["docs", "--offline", "show", "/getting_started/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_strips_docs_prefix() {
    snouty_docs()
        .args(["docs", "--offline", "show", "docs/getting_started"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_missing_page_suggests() {
    snouty_docs()
        .args(["docs", "--offline", "show", "nonexistent_page"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "page not found: docs/nonexistent_page",
        ));
}

#[test]
fn docs_show_partial_match_suggests() {
    snouty_docs()
        .args(["docs", "--offline", "show", "sdk"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Did you mean"))
        .stderr(predicate::str::contains("/docs/sdk/python_sdk/"));
}

#[test]
fn docs_tree_omits_docs_root_and_shows_titles() {
    snouty_docs()
        .args(["docs", "--offline", "tree"])
        .assert()
        .success()
        .stdout(predicate::str::contains("docs\n").not())
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains("docker_basics - Docker basics\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("overview - Overview\n"))
        .stdout(predicate::str::contains("python_sdk - Python SDK\n"))
        .stdout(predicate::str::contains("┗"))
        .stdout(predicate::str::contains("━"));
}

#[test]
fn docs_tree_depth_limits_output() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "--depth", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("docker_basics - Docker basics").not())
        .stdout(predicate::str::contains("overview - Overview").not());
}

#[test]
fn docs_tree_depth_short_flag_limits_output() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "-d", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("docker_basics - Docker basics").not())
        .stdout(predicate::str::contains("overview - Overview").not());
}

#[test]
fn docs_tree_filter_matches_paths_and_preserves_ancestors() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "overview"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("overview - Overview\n"))
        .stdout(predicate::str::contains("guides").not());
}

#[test]
fn docs_tree_filter_matches_titles_case_insensitively() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "setup GUIDE"])
        .assert()
        .success()
        .stdout(predicate::str::contains("getting_started - Setup guide\n"))
        .stdout(predicate::str::contains("docker_basics").not());
}

#[test]
fn docs_tree_no_results_prints_message() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "no-such-doc-page"])
        .assert()
        .success()
        .stderr(predicate::str::contains("No results found"));
}

#[test]
fn docs_sqlite_prints_path() {
    snouty_docs()
        .args(["docs", "--offline", "sqlite"])
        .assert()
        .success()
        .stdout(predicate::str::contains(fixture_db()));
}

#[test]
fn docs_search_multi_word_query() {
    snouty_docs()
        .args(["docs", "--offline", "search", "fault", "injection"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "/docs/environment/fault_injection/",
        ))
        .stdout(predicate::str::contains("Fault injection"));
}

#[test]
fn docs_search_conversational_query_prefers_content_terms() {
    let results = docs_search_json(&["what", "is", "antithesis"]);

    let first_path = results[0].get("path").and_then(|v| v.as_str());
    assert_ne!(first_path, Some("/docs/faq/what_is_a_poc/"));
}

#[test]
fn debug_reports_api_errors() {
    let mock_url = start_mock_server(r#"{"error": "unauthorized"}"#, 401);

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--antithesis.debugging.input_hash",
            "abc123",
            "--antithesis.debugging.session_id",
            "sess-456",
            "--antithesis.debugging.vtime",
            "1234567890",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("API error: 401"));
}
