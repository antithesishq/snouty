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
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
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
        .args([
            "launch",
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
            "--duration",
            "30",
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
            "--launcher",
            "basic_test",
            "--config-image",
            "config:latest",
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
            "--launcher",
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
    // Invalid durations now fail at flag-parse time with a message attributed to
    // --duration, instead of a deep schema "validation failed" cascade.
    snouty()
        .args(["launch", "--launcher", "basic_test", "--duration", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--duration"))
        .stderr(predicate::str::contains("number of minutes"));
}

#[test]
fn launch_duration_accepts_fractional() {
    let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);

    // Fractional minutes are kept for backwards compatibility (1.05 -> 63s).
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
        .stderr(predicate::str::contains(r#""antithesis.duration": "1.05""#));
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
            "--duration",
            "30",
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
    let mock_url = start_mock_server(r#"{}"#, 200);

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
            "--duration",
            "30",
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
            "--duration",
            "30",
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
            "--duration",
            "30",
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
    let mock_url = start_mock_server(r#"{}"#, 200);

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

/// Asserts that `cmd` exits cleanly, without a panic, when its stdout is a
/// closed pipe, as in `snouty … | head` after `head` exits.
///
/// `cmd` must clear its environment, as [`snouty`] does. The copy cannot
/// detect that, so it clears its own.
fn assert_exits_cleanly_with_closed_stdout(cmd: &assert_cmd::Command) {
    let mut closed = std::process::Command::new(cmd.get_program());
    closed.args(cmd.get_args()).env_clear().envs(
        cmd.get_envs()
            .filter_map(|(key, value)| Some((key, value?))),
    );
    if let Some(dir) = cmd.get_current_dir() {
        closed.current_dir(dir);
    }

    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let output = closed
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let args: Vec<_> = cmd.get_args().collect();
    assert!(
        output.status.success(),
        "{args:?}: expected clean exit on broken pipe, got {:?}; stderr:\n{stderr}",
        output.status
    );
    assert!(!stderr.contains("panicked"), "{args:?}: stderr:\n{stderr}");
}

#[test]
fn runs_list_exits_cleanly_when_stdout_closes_early() {
    let mock = start_runs_server(false);
    assert_exits_cleanly_with_closed_stdout(snouty_with_mock_server(&mock).args(["runs", "list"]));
}

#[test]
fn launch_exits_cleanly_when_stdout_closes_early() {
    for json in [&[][..], &["--json"]] {
        let mock_url = start_mock_server(r#"{"runId":"run-123","statusCode":200}"#, 200);
        assert_exits_cleanly_with_closed_stdout(
            snouty_with_mock(&mock_url)
                .args(json)
                .args(["launch", "--launcher", "advanced_test", "--duration", "30"])
                .args(["--source", "ci", "--no-vcs"]),
        );
    }
}

#[test]
fn debug_exits_cleanly_when_stdout_closes_early() {
    for json in [&[][..], &["--json"]] {
        let mock_url = start_mock_server(r#"{"runId":"run-abc-1-1","statusCode":202}"#, 200);
        assert_exits_cleanly_with_closed_stdout(
            snouty_with_mock(&mock_url)
                .args(json)
                .args(["debug", "--session-id", "sess-456"])
                .args(["--input-hash", "abc123", "--vtime", "1234567890"]),
        );
    }
}

#[test]
fn local_commands_exit_cleanly_when_stdout_closes_early() {
    assert_exits_cleanly_with_closed_stdout(snouty().arg("version"));
    assert_exits_cleanly_with_closed_stdout(snouty().args(["completions", "bash"]));
    assert_exits_cleanly_with_closed_stdout(snouty().args(["--json", "doctor", "--offline"]));

    let home = tempfile::TempDir::new().unwrap();
    assert_exits_cleanly_with_closed_stdout(snouty().env("HOME", home.path()).args([
        "login",
        "--tenant",
        "acme",
        "--repository",
        "registry.example.com",
    ]));

    for args in [
        &["search", "docker"][..],
        &["search", "--list", "docker"],
        &["search", "--json", "docker"],
        &["search", "--json", "--list", "docker"],
        &["show", "getting_started"],
        &["tree"],
        &["sqlite"],
    ] {
        assert_exits_cleanly_with_closed_stdout(
            snouty_docs().args(["docs", "--offline"]).args(args),
        );
    }
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
