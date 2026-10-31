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
            "--launcher",
            "basic_test",
            "--test-name",
            "my-test",
            "--description",
            "nightly test run",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
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
        .stderr(predicate::str::contains(
            r#""antithesis.max_compute_hours": "24""#,
        ))
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
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
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
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
        ])
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
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
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
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
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
            "--launcher",
            "basic_test",
            "--max-compute-hours",
            "24",
            "--param",
            "antithesis.max_compute_hours=60",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be overridden via --param"));
}

#[test]
fn launch_duration_rejects_non_numeric() {
    // Invalid durations now fail at flag-parse time with a message attributed to
    // --duration, instead of a deep schema "validation failed" cascade.
    snouty()
        .args(["launch", "--launcher", "basic_test", "--duration", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--duration"))
        .stderr(predicate::str::contains("a bare number is not permitted"));
}

#[test]
fn launch_duration_accepts_fractional() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    // Fractional minutes are kept for backwards compatibility (1.05 -> 63s),
    // with a deprecation warning that writes the value with units.
    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--duration",
            "1.05",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "1.05""#))
        .stderr(predicate::str::contains(
            "warning: a bare number for --duration is deprecated; write 1m3s",
        ));
}

#[test]
fn launch_duration_accepts_unit_suffixes() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    // 1h30m is sent to the API as a bare 90 minutes.
    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--duration",
            "1h30m",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains(r#""antithesis.duration": "90""#));
}

#[test]
fn launch_no_stdin_flag() {
    snouty()
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--stdin",
            "--max-compute-hours",
            "24",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}

#[test]
fn launch_no_trailing_raw_args() {
    snouty()
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--antithesis.duration",
            "30",
        ])
        .assert()
        .failure();
}

#[test]
fn launch_fails_without_parameters() {
    let mock_url = start_mock_server(r"{}", 200);

    snouty_with_mock(&mock_url)
        .args(["launch", "--launcher", "basic_test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no parameters provided"));
}

#[test]
fn launch_reports_api_errors() {
    // A launcher-reached failure (e.g. unknown launcher -> 404) carries the
    // `Launch_Error_Response` envelope (`{ statusCode, runId: null }`), not the
    // `{ message }` shape the other endpoints use. The launcher does no request
    // validation beyond auth, so 404 — not 400 — is the reachable failure.
    // (Gateway rejections instead return an empty body; that path is covered in
    // specs/debug.txt.)
    let mock_url = start_mock_server(r#"{"statusCode":404,"runId":null}"#, 404);

    snouty_with_mock(&mock_url)
        .args([
            "launch",
            "--launcher",
            "no_such_launcher",
            "--max-compute-hours",
            "24",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "unknown launcher: no_such_launcher",
        ));
}

#[test]
fn launch_fails_without_credentials() {
    snouty()
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("No Antithesis credentials found"));
}

#[test]
fn run_prints_deprecation_warning() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    snouty_with_mock(&mock_url)
        .args([
            "run",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--max-compute-hours",
            "24",
        ])
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
        // The vtime is sent as the exact print of the parsed `VTime`.
        .stderr(predicate::str::contains(
            r#""antithesis.debugging.vtime": "1234567890.0""#,
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
fn debug_fails_missing_required_fields() {
    let mock_url = start_mock_server(r"{}", 200);

    snouty_with_mock(&mock_url)
        .args(["debug", "--input-hash", "abc", "--run-id", "run-1"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--vtime <VTIME>"));
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
    let stderr = run_with_closed_stdout(
        &[
            ("ANTITHESIS_API_KEY", &mock.token),
            ("ANTITHESIS_TENANT", "testtenant"),
            ("ANTITHESIS_BASE_URL", &mock.url),
        ],
        &["runs", "list"],
    );
    assert!(stderr.is_empty(), "stderr:\n{stderr}");
}

// `snouty completions bash | head -1`. clap_complete panics on a failed
// write, and this needs no network.
#[test]
fn completions_exit_cleanly_when_stdout_closes_early() {
    let stderr = run_with_closed_stdout(&[], &["completions", "bash"]);
    assert!(stderr.is_empty(), "stderr:\n{stderr}");
}

/// Runs snouty with only `envs` set and a stdout whose read end is already
/// closed, so its first stdout write hits a broken pipe. Asserts that it
/// exits 0, and returns its stderr.
fn run_with_closed_stdout(envs: &[(&str, &str)], args: &[&str]) -> String {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snouty"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .envs(envs.iter().copied())
        .args(args)
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "expected clean exit on broken pipe, got {:?}; stderr:\n{stderr}",
        output.status
    );
    stderr
}

// `snouty launch | head -1` takes the run ID line and closes stdout before the
// next-step lines are written. snouty must exit cleanly, not panic.
#[test]
fn launch_exits_cleanly_when_stdout_closes_early() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);
    let stderr = run_with_closed_stdout(
        &[
            (
                "ANTITHESIS_API_KEY",
                snouty::testutils::fake_secrets::FAKE_API_KEY_A,
            ),
            ("ANTITHESIS_TENANT", "testtenant"),
            ("ANTITHESIS_BASE_URL", &mock_url),
        ],
        &[
            "launch",
            "--launcher",
            "advanced_test",
            "--max-compute-hours",
            "24",
            "--source",
            "ci",
            "--no-vcs",
        ],
    );
    // launch writes its request params to stderr, but no error or panic.
    assert!(!stderr.contains("Error"), "stderr:\n{stderr}");
    assert!(!stderr.contains("panicked"), "stderr:\n{stderr}");
}

/// Users copy the help examples verbatim, so every `launch` and `debug`
/// example must pass argument parsing and the pre-flight checks.
#[test]
fn help_examples_pass_preflight() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("config")).unwrap();
    std::fs::write(dir.path().join("config/docker-compose.yaml"), "").unwrap();

    for subcommand in ["launch", "debug"] {
        let output = snouty().args([subcommand, "--help"]).output().unwrap();
        let help = String::from_utf8(output.stdout).unwrap();
        let prefix = format!("  snouty {subcommand} ");

        let mut count = 0;
        let mut lines = help.lines();
        while let Some(line) = lines.next() {
            if !line.starts_with(&prefix) {
                continue;
            }
            let mut example = line.trim().to_string();
            while let Some(head) = example.strip_suffix('\\') {
                example = format!("{head} {}", lines.next().unwrap().trim());
            }
            // A `|` pipes into a command that is not snouty.
            let example = example
                .split('|')
                .next()
                .unwrap()
                .replace("<run_id>", "run-1")
                .replace("<hash>", "1")
                .replace("<vtime>", "1");
            count += 1;

            let args = shlex::split(&example).unwrap();
            let mock_url = start_mock_server(r#"{"runId":"run-123"}"#, 200);
            // An engine that does not exist stops a --config launch after the
            // pre-flight checks, before it builds an image.
            let output = snouty_with_mock(&mock_url)
                .current_dir(dir.path())
                .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
                .env(snouty::settings::CONTAINER_ENGINE_VAR_NAME, "none")
                .args(&args[1..])
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success() || stderr.contains("unsupported container engine 'none'"),
                "help example failed: {example}\nstderr:\n{stderr}"
            );
        }
        assert!(count > 0, "no examples in `{subcommand} --help`");
    }
}
