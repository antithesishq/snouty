mod support;

use predicates::prelude::*;
use support::*;

#[test]
fn version_prints_version() {
    snouty()
        .arg("version")
        .assert()
        .success()
        .stdout(predicate::str::is_match(r"^snouty \d+\.\d+\.\d+").unwrap());
}

#[test]
fn version_flag_prints_version() {
    snouty()
        .arg("--version")
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
        .stdout(predicate::str::contains("launch"))
        .stdout(predicate::str::contains("debug"))
        .stdout(predicate::str::contains("version"));
}

#[test]
fn validate_help_documents_compose_and_kubernetes() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Compose configs:"))
        .stdout(predicate::str::contains("docker-compose locally"))
        .stdout(predicate::str::contains("setup-complete"))
        .stdout(predicate::str::contains("Test commands are not executed"))
        .stdout(predicate::str::contains("Kubernetes configs:"))
        .stdout(predicate::str::contains("k8s-validator"))
        .stdout(predicate::str::contains("manifests/"));
}

#[test]
fn runs_lists_all_pages() {
    let mock = start_runs_server(false);

    snouty_with_mock_server(&mock)
        .arg("runs")
        .assert()
        .success()
        .stdout(predicate::str::contains("RUN ID"))
        .stdout(predicate::str::contains("run-1"))
        .stdout(predicate::str::contains("run-2"));
}

#[test]
fn runs_prints_empty_state() {
    let mock = start_runs_server(true);

    snouty_with_mock_server(&mock)
        .arg("runs")
        .assert()
        .success()
        .stdout(predicate::str::contains("No runs found."));
}

#[test]
fn launch_with_typed_flags() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
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
        ))
        .stderr(predicate::str::contains("is_ephemeral").not());
}

#[test]
fn launch_with_ephemeral_flag() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "-w",
            "basic_test",
            "--duration",
            "30",
            "--ephemeral",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.is_ephemeral": "true""#,
        ));
}

#[test]
fn launch_without_source_sets_ephemeral() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.is_ephemeral": "true""#,
        ))
        .stderr(predicate::str::contains(
            "Starting an ephemeral run; its findings will not be retained",
        ));
}

#[test]
fn launch_with_source_omits_ephemeral() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "-w",
            "basic_test",
            "--duration",
            "30",
            "--source",
            "ci",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("is_ephemeral").not())
        .stderr(predicate::str::contains("Starting an ephemeral run").not());
}

#[test]
fn launch_with_param_flag() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
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
fn launch_param_cannot_override_typed_flag() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "-w",
            "basic_test",
            "--duration",
            "30",
            "--param",
            "antithesis.duration=60",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be overridden via --param"));
}

#[test]
fn launch_duration_rejects_non_numeric() {
    snouty()
        .args(["launch", "-w", "basic_test", "--duration", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("validation failed"));
}

#[test]
fn launch_duration_accepts_fractional() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test", "--duration", "0.05"])
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "0.05""#));
}

#[test]
fn launch_no_stdin_flag() {
    snouty()
        .args(["launch", "-w", "basic_test", "--stdin", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

#[test]
fn launch_no_trailing_raw_args() {
    snouty()
        .args(["launch", "-w", "basic_test", "--antithesis.duration", "30"])
        .assert()
        .failure();
}

#[test]
fn launch_fails_without_webhook() {
    snouty()
        .args(["launch", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--webhook"));
}

#[test]
fn launch_fails_without_parameters() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no parameters provided"));
}

#[test]
fn launch_reports_api_errors() {
    let mock_url = start_mock_server(r#"{"message":"bad request"}"#, 400);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("API error: 400"));
}

#[test]
fn launch_fails_without_credentials() {
    snouty()
        .args(["launch", "-w", "basic_test", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("missing environment variable"));
}

#[test]
fn run_prints_deprecation_warning() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "`snouty run` is deprecated, use `snouty launch` instead",
        ));
}

#[test]
fn debug_with_cli_args() {
    let mock_url = start_mock_server(r#"{"runId": "run-abc-1-1", "statusCode": 202}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--input-hash",
            "abc123",
            "--session-id",
            "sess-456",
            "--vtime",
            "1234567890",
            "--description",
            "debug this moment",
            "--recipients",
            "team@example.com",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "abc123""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.session_id": "sess-456""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.vtime": "1234567890""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.event_description": "debug this moment""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ))
        .stdout(predicate::str::contains(
            "Debugging session started: run_id run-abc-1-1",
        ));
}

#[test]
fn debug_with_json_flag() {
    let mock_url = start_mock_server(r#"{"runId": "run-abc-1-1", "statusCode": 202}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "--json",
            "debug",
            "--input-hash",
            "abc123",
            "--session-id",
            "sess-456",
            "--vtime",
            "1234567890",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""runId": "run-abc-1-1""#));
}

#[test]
fn debug_with_moment_from_format() {
    let mock_url = start_mock_server(r#"{"runId": "run-1", "statusCode": 202}"#, 200);
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
    let mock_url = start_mock_server(r#"{"runId": "run-1", "statusCode": 202}"#, 200);
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

#[test]
fn debug_fails_missing_required_fields() {
    let mock_url = start_mock_server(r#"{}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["debug", "--input-hash", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("validation failed"));
}

#[test]
fn debug_rejects_old_raw_args() {
    snouty()
        .args([
            "debug",
            "--antithesis.debugging.input_hash",
            "abc",
            "--antithesis.debugging.session_id",
            "sess",
            "--antithesis.debugging.vtime",
            "123",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

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
        .stderr(predicate::str::contains("invalid value 'nushell'"));
}

#[test]
fn debug_merges_moment_with_cli_args() {
    let mock_url = start_mock_server(r#"{"runId": "run-1", "statusCode": 202}"#, 200);
    let moment_input = r#"Moment.from({ session_id: "f89d5c11f5e3bf5e4bb3641809800cee-44-22", input_hash: "6057726200491963783", vtime: 329.8037810830865 })"#;

    snouty_with_mock(&mock_url)
        .args([
            "debug",
            "--stdin",
            "--input-hash",
            "override",
            "--recipients",
            "team@example.com",
        ])
        .write_stdin(moment_input)
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.session_id": "f89d5c11f5e3bf5e4bb3641809800cee-44-22""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.input_hash": "override""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.vtime": "329.8037810830865""#,
        ))
        .stderr(predicate::str::contains(
            r#""antithesis.report.recipients": "[REDACTED]""#,
        ));
}

#[test]
fn doctor_help_shows_description() {
    snouty()
        .args(["doctor", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Check environment configuration"));
}

#[test]
fn doctor_reports_missing_env_vars() {
    snouty()
        .arg("doctor")
        .assert()
        .failure()
        .stderr(predicate::str::contains("tenant"))
        .stderr(predicate::str::contains("repository"));
}

#[test]
fn validate_help_shows_keep_running() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--keep-running"));
}

// `snouty runs list | head` closes stdout before snouty finishes writing;
// snouty must exit cleanly instead of panicking on the broken pipe (#121).
#[test]
fn runs_list_exits_cleanly_when_stdout_closes_early() {
    let mock = start_runs_server(false);

    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_snouty"));
    for env_var in [
        "ANTITHESIS_API_KEY",
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
        "ANTITHESIS_TENANT",
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_REPOSITORY",
    ] {
        cmd.env_remove(env_var);
    }
    // Hand snouty a pipe whose read end is already closed before it spawns,
    // so its very first stdout write deterministically hits a broken pipe.
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);

    let child = cmd
        .env("ANTITHESIS_API_KEY", &mock.token)
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", &mock.url)
        .args(["runs", "list"])
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected clean exit on broken pipe, got {:?}; stderr:\n{stderr}",
        output.status
    );
    assert!(!stderr.contains("panicked"), "stderr:\n{stderr}");
}
