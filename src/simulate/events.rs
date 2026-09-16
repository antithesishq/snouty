use std::sync::LazyLock;

use color_eyre::eyre::{Context, Result, eyre};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::render::sanitize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    Assertion,
    Command,
}

#[derive(Debug, Serialize)]
pub struct Event {
    pub timestamp: String,
    pub source: String,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    SetupComplete {
        message: Option<String>,
    },
    Assertion {
        message: String,
        verdict: AssertionVerdict,
        details: Value,
    },
    CommandStarted {
        command: String,
    },
    CommandFinished {
        command: String,
        exit_code: i64,
        runtime: Option<f64>,
        stdout: String,
        stderr: String,
    },
    RolloutComplete {
        message: Option<String>,
    },
    Log {
        message: String,
        level: LogLevel,
    },
    ProcessSignal {
        signal: i64,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssertionVerdict {
    Failed,
    Reached,
    Satisfied,
    Observed { display_type: String },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Info,
    Error,
}

impl Event {
    pub fn failure(&self) -> Option<Failure> {
        match self.kind {
            EventKind::Assertion {
                verdict: AssertionVerdict::Failed,
                ..
            } => Some(Failure::Assertion),
            EventKind::CommandFinished { exit_code, .. } if exit_code != 0 => {
                Some(Failure::Command)
            }
            _ => None,
        }
    }

    pub fn render(&self) -> String {
        let text = |s: &str| {
            sanitize(
                &console::strip_ansi_codes(s)
                    .replace('\r', "")
                    .replace(['\n', '\t'], " "),
            )
        };
        let message_suffix = |message: &Option<String>| {
            message
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| format!(" ({})", text(s)))
                .unwrap_or_default()
        };
        match &self.kind {
            EventKind::SetupComplete { message } => {
                let suffix = message
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .map(|s| format!(" (\"{}\")", text(s)))
                    .unwrap_or_default();
                format!("Reached setup_complete{suffix}")
            }
            EventKind::Assertion {
                message, verdict, ..
            } => {
                let message = text(message);
                match verdict {
                    AssertionVerdict::Failed => {
                        console::style(format!("ASSERTION FAILED: {message}"))
                            .red()
                            .to_string()
                    }
                    AssertionVerdict::Reached => {
                        console::style(format!("assertion reached: {message}"))
                            .green()
                            .bright()
                            .to_string()
                    }
                    AssertionVerdict::Satisfied => format!("assertion satisfied: {message}"),
                    AssertionVerdict::Observed { display_type } => {
                        format!("assertion {}: {message}", text(display_type))
                    }
                }
            }
            EventKind::CommandStarted { command } => {
                format!("test composer: started {}", text(command))
            }
            EventKind::CommandFinished {
                command,
                exit_code,
                runtime,
                stdout,
                stderr,
            } => {
                let mut output = if *exit_code == 0 {
                    format!(
                        "test composer: passed {}{}",
                        text(command),
                        runtime.map(|r| format!(" ({r:.2}s)")).unwrap_or_default()
                    )
                } else {
                    console::style(format!(
                        "test composer: FAILED {} (exit {exit_code}{})",
                        text(command),
                        runtime.map(|r| format!(", {r:.2}s")).unwrap_or_default()
                    ))
                    .red()
                    .to_string()
                };
                for (label, value) in [("stdout", stdout), ("stderr", stderr)] {
                    if !value.is_empty() {
                        output.push_str(&format!("\n  {label}: {}", text(value)));
                    }
                }
                output
            }
            EventKind::RolloutComplete { message } => {
                format!("test composer: run complete{}", message_suffix(message))
            }
            EventKind::Log { message, level } => {
                let label = self
                    .source
                    .rsplit("/commands/")
                    .next()
                    .unwrap_or(&self.source);
                let label = label
                    .strip_suffix(".out")
                    .or_else(|| label.strip_suffix(".err"))
                    .unwrap_or(label);
                let label = text(label);
                let message = text(message);
                match level {
                    LogLevel::Error => console::style(format!("{label}: error: {message}"))
                        .red()
                        .to_string(),
                    LogLevel::Info if message.starts_with(&format!("{label}: ")) => message,
                    LogLevel::Info => format!("{label}: {message}"),
                }
            }
            EventKind::ProcessSignal { signal } => {
                let name = match signal {
                    6 => "SIGABRT".into(),
                    9 => "SIGKILL".into(),
                    11 => "SIGSEGV".into(),
                    15 => "SIGTERM".into(),
                    _ => format!("signal {signal}"),
                };
                console::style(format!("process terminated by {name} ({signal})"))
                    .red()
                    .to_string()
            }
        }
    }
}

fn platform_source(source: &str) -> bool {
    matches!(
        source,
        "bootstrap"
            | "fuzzpipe"
            | "create_container_script"
            | "create_runtime_script"
            | "post_stop_script"
            | "antithesis_prestart_hook"
            | "fault_injector"
            | "fault_injector_command"
            | "antithesis_test_composer"
            | "soft_terminate_composer"
    )
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum AssertionType {
    Always,
    Sometimes,
    Reachability,
    Unreachable,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct AssertionPayload {
    assert_type: AssertionType,
    hit: bool,
    condition: Option<bool>,
    must_hit: Option<bool>,
    #[serde(default)]
    display_type: String,
    message: Option<String>,
    id: Option<String>,
}

pub fn parse_line(line: &str) -> Result<Option<Event>> {
    static FRAME: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^\s*([0-9]+(?:\.[0-9]+)?) \[([^\]]+)\] \[([A-Z][A-Z0-9 _-]*)\](?: (.*))?$")
            .unwrap()
    });
    // C1 characters are valid JSON text; terminal stripping would corrupt them.
    static CSI: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]").unwrap());
    let line = CSI.replace_all(line, "").replace('\r', "");
    let Some(parts) = FRAME.captures(&line) else {
        return Ok(None);
    };
    let source = &parts[2];
    let kind = &parts[3];
    let payload = parts.get(4).map_or("", |m| m.as_str());
    let payload = payload
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(payload);
    if matches!(
        kind,
        "COVERAGE" | "COVERAGE MODULE" | "SEGMENT" | "STARTUP" | "TELEMETRY" | "WAITING_FOR_INPUT"
    ) || matches!(
        source,
        "bootstrap" | "fault_injector_command" | "coredump_script"
    ) || matches!(
        payload,
        "Environment variable 'ANTITHESIS_ASSERTION_CATALOG' must refer to an accessible directory"
            | "Ignoring it because it is set to PosixPath('/.antithesis-produced/catalog')"
    ) || (matches!(source, "create_container_script" | "post_stop_script")
        && matches!(kind, "INFO" | "JSON"))
        || (source == "create_runtime_script"
            && (kind == "JSON"
                || payload
                    .contains("dmesg(1) may have more information after failed mount system call")
                || [
                    "tacticslang-sheap",
                    "tacticslang-shepherd",
                    "running_tactics_lang_processes",
                ]
                .iter()
                .any(|name| {
                    payload.contains(&format!(
                        "special device /opt/antithesis/{name} does not exist"
                    ))
                })))
    {
        return Ok(None);
    }
    let event = |kind| {
        Some(Event {
            timestamp: parts[1].into(),
            source: source.into(),
            kind,
        })
    };
    if kind == "JSON" || source == "processes_terminated_with_signal" {
        let value: Value = serde_json::from_str(payload)
            .wrap_err_with(|| format!("invalid instrumentation JSON from {source}"))?;
        let string = |key: &str| value[key].as_str().unwrap_or_default().to_owned();
        if let Some(assertion) = value.get("antithesis_assert") {
            let a: AssertionPayload =
                serde_json::from_value(assertion.clone()).wrap_err("invalid assertion event")?;
            if !a.hit {
                return Ok(None);
            }
            if a.assert_type == AssertionType::Sometimes && a.condition.is_none() {
                return Err(eyre!("sometimes evaluation has no condition"));
            }
            if a.assert_type == AssertionType::Reachability
                && a.must_hit.is_none()
                && !matches!(a.display_type.as_str(), "Unreachable" | "Reachable")
            {
                return Err(eyre!("reachability evaluation has no must_hit field"));
            }
            let verdict = if a.assert_type == AssertionType::Always {
                if !a
                    .condition
                    .ok_or_else(|| eyre!("assertion evaluation has no condition"))?
                {
                    AssertionVerdict::Failed
                } else if !a.display_type.is_empty() {
                    AssertionVerdict::Observed {
                        display_type: a.display_type,
                    }
                } else {
                    return Ok(None);
                }
            } else if a.display_type.eq_ignore_ascii_case("unreachable")
                || a.assert_type == AssertionType::Unreachable
                || (a.assert_type == AssertionType::Reachability && a.must_hit == Some(false))
            {
                AssertionVerdict::Failed
            } else if a.assert_type == AssertionType::Reachability && a.must_hit == Some(true) {
                AssertionVerdict::Reached
            } else if a.assert_type == AssertionType::Sometimes && a.condition == Some(true) {
                AssertionVerdict::Satisfied
            } else if !a.display_type.is_empty() {
                AssertionVerdict::Observed {
                    display_type: a.display_type,
                }
            } else {
                return Ok(None);
            };
            return Ok(event(EventKind::Assertion {
                message: a
                    .message
                    .or(a.id)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "unnamed assertion".into()),
                verdict,
                details: assertion.clone(),
            }));
        }
        if value.get("antithesis_sdk").is_some() || value.get("racebench_driver_started").is_some()
        {
            return Ok(None);
        }
        if let Some(setup) = value.get("antithesis_setup") {
            if !setup["status"].is_string() {
                return Err(eyre!("setup event has no status"));
            }
            if setup["status"] == "complete" {
                return Ok(event(EventKind::SetupComplete {
                    message: setup
                        .get("message")
                        .or_else(|| setup.get("details").and_then(|d| d.get("message")))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                }));
            }
        }
        if let Some(error) = value.get("antithesis_error") {
            if platform_source(source) {
                return Ok(None);
            }
            let message = error["message"].as_str().unwrap_or(payload);
            let suffix = error
                .get("code")
                .map(|code| format!(" (code {code})"))
                .unwrap_or_default();
            return Ok(event(EventKind::Log {
                message: format!("{message}{suffix}"),
                level: LogLevel::Error,
            }));
        }
        if source == "antithesis_test_composer" {
            let command = value
                .get("command")
                .or_else(|| value.get("started_task"))
                .or_else(|| value.get("finished_task"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            if matches!(value["task_status"].as_str(), Some("started" | "finished"))
                && command.is_empty()
            {
                return Err(eyre!("composer task event has no command"));
            }
            return match value["task_status"].as_str() {
                Some("started") => Ok(event(EventKind::CommandStarted { command })),
                Some("finished") => {
                    let code = &value["command_return_code"];
                    let exit_code = code
                        .as_i64()
                        .or_else(|| code.as_str().and_then(|s| s.parse().ok()))
                        .ok_or_else(|| eyre!("finished composer command has no valid exit code"))?;
                    let runtime = value
                        .get("command_runtime")
                        .map(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                .filter(|n| n.is_finite() && *n >= 0.0)
                                .ok_or_else(|| eyre!("invalid composer command runtime"))
                        })
                        .transpose()?;
                    Ok(event(EventKind::CommandFinished {
                        command,
                        exit_code,
                        runtime,
                        stdout: string("additional_stdout"),
                        stderr: string("additional_stderr"),
                    }))
                }
                _ => Ok(None),
            };
        }
        if source == "soft_terminate_composer" {
            return Ok(event(EventKind::RolloutComplete {
                message: value["message"].as_str().map(str::to_owned),
            }));
        }
        if source == "processes_terminated_with_signal" {
            return Ok(event(EventKind::ProcessSignal {
                signal: value["signal"]
                    .as_i64()
                    .ok_or_else(|| eyre!("invalid process signal event"))?,
            }));
        }
    }
    if platform_source(source) {
        return Ok(None);
    }
    Ok(event(EventKind::Log {
        message: payload.into(),
        level: if kind == "ERROR" || source.ends_with(".err") {
            LogLevel::Error
        } else {
            LogLevel::Info
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;
    use serde_json::json;

    #[test]
    fn reference_fixture_matches() {
        let output = include_str!("../../tests/fixtures/simulate/input.log")
            .lines()
            .filter_map(|line| parse_line(line).unwrap())
            .map(|event| format!("{}\n", console::strip_ansi_codes(&event.render())))
            .collect::<String>();
        assert_eq!(
            output,
            include_str!("../../tests/fixtures/simulate/expected.log")
        );
    }

    // Instrumentation serial output from local antithesis-guest:v61.2 under TCG.
    #[test]
    fn observed_composer_failure_and_output_are_preserved() {
        let events: Vec<_> = include_str!("../../tests/fixtures/simulate/observed.log")
            .lines()
            .filter_map(|line| parse_line(line).unwrap())
            .collect();
        assert_eq!(events.len(), 4);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.failure() == Some(Failure::Command))
                .count(),
            1
        );
        assert_eq!(
            events.last().unwrap().render(),
            "smoke/serial_driver_smoke: snouty-simulation-composer-smoke"
        );
    }

    #[hegel::test]
    fn assertion_message_survives_json_escaping(tc: hegel::TestCase) {
        let message = tc.draw(generators::text());
        let input = json!({"antithesis_assert": {"assert_type":"always", "hit":true, "condition":false, "message":message}});
        let event = parse_line(&format!("0.5 [app] [JSON] {input}"))
            .unwrap()
            .unwrap();
        assert_eq!(event.failure(), Some(Failure::Assertion));
        if let EventKind::Assertion {
            message: actual, ..
        } = event.kind
        {
            assert_eq!(
                actual,
                if message.is_empty() {
                    "unnamed assertion"
                } else {
                    &message
                }
            );
        } else {
            panic!("expected assertion")
        }
    }

    #[hegel::test]
    fn assertion_evaluations_follow_family_semantics(tc: hegel::TestCase) {
        let hit = tc.draw(generators::booleans());
        let condition = tc.draw(generators::booleans());
        let must_hit = tc.draw(generators::booleans());
        for (family, failed) in [
            ("always", hit && !condition),
            ("sometimes", false),
            ("reachability", hit && !must_hit),
        ] {
            let payload = json!({"antithesis_assert": {
                "assert_type": family, "hit": hit, "condition": condition,
                "must_hit": must_hit, "message": "test"
            }});
            let event = parse_line(&format!("0 [app] [JSON] {payload}")).unwrap();
            assert_eq!(
                event.and_then(|e| e.failure()),
                failed.then_some(Failure::Assertion)
            );
        }
    }

    #[hegel::test]
    fn composer_string_and_numeric_codes_agree(tc: hegel::TestCase) {
        let code = tc.draw(generators::integers::<i64>());
        for wire in [json!(code), json!(code.to_string())] {
            let payload =
                json!({"task_status":"finished", "command":"test", "command_return_code":wire});
            let event = parse_line(&format!("1 [antithesis_test_composer] [JSON] {payload}"))
                .unwrap()
                .unwrap();
            assert_eq!(event.failure(), (code != 0).then_some(Failure::Command));
        }
    }

    #[hegel::test]
    fn arbitrary_serial_input_does_not_panic(tc: hegel::TestCase) {
        let input = tc.draw(generators::text());
        let _ = parse_line(&input);
        let _ = parse_line(&format!("0 [app] [JSON] {input}"));
    }

    #[test]
    fn catalog_and_liveness_observations_do_not_fail() {
        for payload in [
            json!({"assert_type":"always", "hit":false, "condition":false}),
            json!({"assert_type":"sometimes", "hit":true, "condition":false}),
            json!({"assert_type":"reachability", "hit":false, "must_hit":true}),
        ] {
            let line = format!("1 [app] [JSON] {}", json!({"antithesis_assert":payload}));
            assert!(parse_line(&line).unwrap().is_none());
        }
        let event = parse_line(r#"1 [app] [JSON] {"antithesis_assert":{"assert_type":"reachability","hit":true,"must_hit":false}}"#).unwrap().unwrap();
        assert_eq!(event.failure(), Some(Failure::Assertion));
    }

    #[test]
    fn unicode_terminal_controls_are_preserved_in_json_only() {
        let payload = json!({"antithesis_assert": {"assert_type":"always", "hit":true,
            "condition":false, "message":"\u{9b}0"}});
        let event = parse_line(&format!("0 [app] [JSON] {payload}"))
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(&event).unwrap()["message"], "\u{9b}0");
        assert!(!event.render().contains('\u{9b}'));
    }

    #[test]
    fn malformed_recognized_events_are_errors() {
        for line in [
            r#"1 [app] [JSON] {"antithesis_assert":{}}"#,
            r#"1 [antithesis_test_composer] [JSON] {"task_status":"finished"}"#,
            "1 [app] [JSON] {bad",
        ] {
            assert!(parse_line(line).is_err());
        }
    }
}
