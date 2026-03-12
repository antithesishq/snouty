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
        .stderr(predicate::str::contains("cannot be overridden via --param"));
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
    snouty()
        .args(["run", "-w", "basic_test", "--stdin", "--duration", "30"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

#[test]
fn run_no_trailing_raw_args() {
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
