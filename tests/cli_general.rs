mod support;

use predicates::prelude::*;
use snouty::run_moment::RunMoment;
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
        .stdout(predicate::str::contains("runs"))
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
fn validate_help_explains_setup_complete_detection() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/tmp/antithesis"))
        .stdout(predicate::str::contains("ANTITHESIS_OUTPUT_DIR"))
        .stdout(predicate::str::contains("ANTITHESIS_SDK_LOCAL_OUTPUT"))
        .stdout(predicate::str::contains("JSONL"))
        .stdout(predicate::str::contains("not by scraping compose logs"))
        .stdout(predicate::str::contains("one random first_ script"))
        .stdout(predicate::str::contains(
            "additional first_ scripts are skipped",
        ));
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
fn runs_logs_accepts_positional_moment_token() {
    let mock = start_runs_server(false);

    snouty_with_mock_server(&mock)
        .args(["runs", "logs", "run-1", "Fa84QWiAxKpsvhP7fglpSa"])
        .assert()
        .success()
        .stdout(predicate::str::contains("VTIME"))
        .stdout(predicate::str::contains("1.0"))
        .stdout(predicate::str::contains("2.0"));
}

#[test]
fn runs_logs_accepts_begin_vtime() {
    let mock = start_runs_server(false);

    snouty_with_mock_server(&mock)
        .args([
            "runs",
            "logs",
            "run-1",
            "Fa84QWiAxKpsvhP7fglpSa",
            "--begin",
            "10.0",
        ])
        .assert()
        .success();
}

#[test]
fn runs_logs_accepts_begin_moment_token() {
    let mock = start_runs_server(false);

    snouty_with_mock_server(&mock)
        .args([
            "runs",
            "logs",
            "run-1",
            "Fa84QWiAxKpsvhP7fglpSa",
            "--begin-moment",
            "Fa84QWiAxIvq0ayfqGCnei",
        ])
        .assert()
        .success();
}

#[test]
fn run_moment_formats_wire_values_as_token() {
    let moment = RunMoment::from_wire("-123", "1.0").unwrap();

    assert_eq!(moment.to_token(), "Fa84QWiAxKpsvhP7fglpSa");
}

#[test]
fn launch_with_typed_flags() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            r#""antithesis.is_ephemeral": "true""#,
        ))
        .stderr(predicate::str::contains(
            "Starting ephemeral run, Findings will not be available (provide --source)",
        ));
}

#[test]
fn launch_with_source_omits_ephemeral() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
        .stderr(predicate::str::contains("Starting ephemeral run").not());
}

#[test]
fn launch_with_param_flag() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
        .stderr(predicate::str::contains("invalid value"));
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
    let mock_url = start_mock_server(r#"{"error":"bad_request","message":"bad request"}"#, 400);

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
fn api_webhook_with_cli_args() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"error":"bad_request","message":"bad request"}"#, 400);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
    assert_eq!(parsed["run_id"], "run-123");
}

#[test]
fn api_webhook_error_outputs_string_on_stderr() {
    let mock_url = start_mock_server(
        r#"{"error":"internal_error","message":"something went wrong"}"#,
        500,
    );

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
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

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
fn launch_prints_email_eta() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Expect a report email"));
}

#[test]
fn run_prints_deprecation_warning() {
    let mock_url = start_mock_server(r#"{"run_id": "run-123"}"#, 200);

    snouty_with_mock(&mock_url)
        .args(["run", "-w", "basic_test", "--duration", "30"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "`snouty run` is deprecated, use `snouty launch` instead",
        ));
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
        .stderr(predicate::str::contains("ANTITHESIS_TENANT"))
        .stderr(predicate::str::contains("ANTITHESIS_REPOSITORY"));
}

#[test]
fn validate_help_shows_keep_running() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--keep-running"));
}
