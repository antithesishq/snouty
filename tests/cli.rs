use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::io::Write;
use std::net::TcpListener;
use std::thread;
use tempfile::TempDir;

fn snouty() -> Command {
    let mut cmd = cargo_bin_cmd!("snouty");
    cmd.env("RUST_LOG", "debug");
    cmd
}

/// Start a simple mock HTTP server that returns a fixed response.
/// Returns the server URL and a handle to stop it.
fn start_mock_server(response_body: &'static str, status: u16) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    thread::spawn(move || {
        for stream in listener.incoming() {
            if let Ok(mut stream) = stream {
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
                break; // Only handle one request
            }
        }
    });

    url
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
        .env_remove("ANTITHESIS_USERNAME")
        .env_remove("ANTITHESIS_PASSWORD")
        .env_remove("ANTITHESIS_TENANT")
        .env_remove("ANTITHESIS_BASE_URL")
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
        .env_remove("ANTITHESIS_REPOSITORY")
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
        .env_remove("ANTITHESIS_USERNAME")
        .env_remove("ANTITHESIS_PASSWORD")
        .env_remove("ANTITHESIS_TENANT")
        .env_remove("ANTITHESIS_BASE_URL")
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
