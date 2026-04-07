use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;

use color_eyre::eyre::{Result, eyre};
use futures_util::TryStreamExt;
use jsonschema::Validator;
use log::info;
use serde::Deserialize;
use serde_json::Value;

use crate::api::{AntithesisApi, RunDetail, RunStatus, RunSummary, RunsFilterOptions};
use crate::cli::{RunsCommands, RunsListArgs};

static ASSERTION_VALIDATOR: LazyLock<Validator> = LazyLock::new(build_assertion_validator);

pub async fn cmd_runs(command: Option<RunsCommands>) -> Result<()> {
    match command {
        None => cmd_runs_list(RunsListArgs::default()).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args).await,
        Some(RunsCommands::Show { run_id, json }) => cmd_runs_show(&run_id, json).await,
        Some(RunsCommands::BuildLogs { run_id, json }) => cmd_runs_build_logs(&run_id, json).await,
        Some(RunsCommands::Logs {
            run_id,
            input_hash,
            vtime,
            begin_vtime,
            begin_input_hash,
            json,
        }) => {
            cmd_runs_logs(
                &run_id,
                &input_hash,
                &vtime,
                begin_input_hash.as_deref(),
                begin_vtime.as_deref(),
                json,
            )
            .await
        }
    }
}

async fn cmd_runs_list(args: RunsListArgs) -> Result<()> {
    info!("listing runs");

    let api = AntithesisApi::from_env()?;

    let status = args
        .status
        .as_deref()
        .map(|s| s.parse::<RunStatus>())
        .transpose()
        .map_err(|_| {
            eyre!(
                "invalid status: '{}'\nvalid values: starting, in_progress, debugger_ready, completed, cancelled, failed",
                args.status.as_deref().unwrap_or_default()
            )
        })?;

    let opts = RunsFilterOptions {
        status,
        launcher: args.launcher,
        created_after: args
            .created_after
            .as_deref()
            .map(|s| s.parse())
            .transpose()
            .map_err(|e| eyre!("invalid --created-after timestamp: {e}"))?,
        created_before: args
            .created_before
            .as_deref()
            .map(|s| s.parse())
            .transpose()
            .map_err(|e| eyre!("invalid --created-before timestamp: {e}"))?,
    };

    let has_filters = opts.status.is_some()
        || opts.launcher.is_some()
        || opts.created_after.is_some()
        || opts.created_before.is_some();

    let mut runs: Vec<RunSummary> = if has_filters {
        api.stream_runs_filtered(&opts)
            .try_collect::<Vec<_>>()
            .await?
    } else {
        api.stream_runs().try_collect::<Vec<_>>().await?
    };

    // Apply client-side limit
    runs.truncate(args.limit);

    if runs.is_empty() {
        println!("No runs found.");
        return Ok(());
    }

    runs.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then(a.status.cmp(&b.status))
    });

    println!("{}", render_runs_table(&runs));
    Ok(())
}

async fn cmd_runs_show(run_id: &str, json: bool) -> Result<()> {
    info!("showing run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let run = api.get_run(run_id).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&run)?);
    } else {
        print_run_detail(&run);
    }

    Ok(())
}

fn print_run_detail(run: &RunDetail) {
    let mut rows: Vec<(&str, String)> = vec![
        ("Run ID", run.run_id.clone()),
        ("Status", render_enum(&run.status)),
    ];

    if let Some(ref t) = run.type_ {
        rows.push(("Type", render_enum(t)));
    }

    rows.push(("Created", run.created_at.to_rfc3339()));

    if let Some(ref t) = run.started_at {
        rows.push(("Started", t.to_rfc3339()));
    }
    if let Some(ref t) = run.completed_at {
        rows.push(("Completed", t.to_rfc3339()));
    }

    rows.push(("Launcher", run.launcher.clone()));

    if let Some(ref links) = run.links
        && let Some(ref url) = links.triage_report
    {
        rows.push(("Report", url.clone()));
    }

    if let Some(ref creator) = run.creator
        && let Some(ref name) = creator.name
    {
        rows.push(("Creator", name.clone()));
    }

    let label_width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in &rows {
        println!("{label:label_width$}  {}", sanitize(value));
    }
}

async fn cmd_runs_build_logs(run_id: &str, json: bool) -> Result<()> {
    info!("streaming build logs for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let response = api.get_run_build_logs(run_id).await?;
    let mut stdout = std::io::stdout().lock();

    if json {
        stream_ndjson_lines(response, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(response, |line| {
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                let ts = entry["timestamp"].as_str().unwrap_or("");
                let stream = entry["stream"].as_str().unwrap_or("out");
                let text = sanitize(entry["text"].as_str().unwrap_or(""));
                writeln!(stdout, "{ts} [{stream}] {text}")?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    }
}

async fn cmd_runs_logs(
    run_id: &str,
    input_hash: &str,
    vtime: &str,
    begin_input_hash: Option<&str>,
    begin_vtime: Option<&str>,
    json: bool,
) -> Result<()> {
    info!("streaming logs for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let response = api
        .get_run_logs(run_id, input_hash, vtime, begin_input_hash, begin_vtime)
        .await?;

    let mut stdout = std::io::stdout().lock();
    if json {
        stream_ndjson_lines(response, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        writeln!(stdout, "{:<22}  {:<20}  OUTPUT", "VTIME", "SOURCE")?;
        stream_ndjson_lines(response, |line| {
            if let Ok(entry) = serde_json::from_str::<Value>(line) {
                let rendered = render_log_entry(&entry);
                let vtime = rendered.vtime;
                let source = rendered.source;
                let output = rendered.output;
                writeln!(stdout, "{vtime:<22}  {source:<20}  {output}")?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    }
}

async fn stream_ndjson_lines(
    response: reqwest::Response,
    mut process_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    use futures_util::StreamExt;

    let mut stream = response.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let text = std::str::from_utf8(&chunk)
            .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
        buf.push_str(text);

        while let Some(pos) = buf.find('\n') {
            let line = &buf[..pos];
            if !line.is_empty() {
                process_line(line)?;
            }
            buf = buf[pos + 1..].to_string();
        }
    }

    // Process any remaining data without a trailing newline
    if !buf.is_empty() {
        process_line(&buf)?;
    }

    Ok(())
}

fn build_assertion_validator() -> Validator {
    let root_schema: Value =
        serde_json::from_str(include_str!("../assertions.json")).expect("valid assertion schema");
    let mut assertion_schema = root_schema["properties"]["antithesis_assert"].clone();

    if let Some(schema_object) = assertion_schema.as_object_mut()
        && let Some(draft) = root_schema.get("$schema").cloned()
    {
        schema_object.insert("$schema".to_string(), draft);

        // The published schema documents `details: null` as valid and real payloads use it.
        if let Some(properties) = schema_object
            .get_mut("properties")
            .and_then(Value::as_object_mut)
            && let Some(details) = properties.get_mut("details").and_then(Value::as_object_mut)
        {
            details.remove("type");
            details.insert(
                "anyOf".to_string(),
                serde_json::json!([
                    {"type": "object"},
                    {"type": "null"}
                ]),
            );
        }
    }

    Validator::new(&assertion_schema).expect("valid nested assertion schema")
}

#[derive(Debug, PartialEq, Eq)]
struct RenderedLogEntry {
    vtime: String,
    source: String,
    output: String,
}

#[derive(Debug, Deserialize)]
struct AssertionPayload {
    hit: Option<bool>,
    condition: Option<bool>,
    #[serde(default)]
    must_hit: bool,
    message: Option<String>,
    assert_type: Option<String>,
    display_type: Option<String>,
    #[serde(default)]
    location: Option<AssertionLocation>,
}

#[derive(Debug, Deserialize)]
struct AssertionLocation {
    file: Option<String>,
    function: Option<String>,
    begin_line: Option<serde_json::Number>,
}

#[derive(Debug, PartialEq, Eq)]
struct AssertionSummary {
    label: String,
    status: AssertionStatus,
    message: String,
    must_hit: bool,
    location: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssertionStatus {
    Pass,
    Fail,
    Unhit,
}

impl AssertionStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Unhit => "UNHIT",
        }
    }
}

impl TryFrom<AssertionPayload> for AssertionSummary {
    type Error = ();

    fn try_from(payload: AssertionPayload) -> std::result::Result<Self, Self::Error> {
        let hit = payload.hit.ok_or(())?;
        let condition = payload.condition.ok_or(())?;
        let message = payload
            .message
            .map(|message| message.trim().to_string())
            .filter(|message| !message.is_empty())
            .ok_or(())?;
        let label = payload
            .display_type
            .map(|label| label.trim().to_string())
            .filter(|label| !label.is_empty())
            .or_else(|| {
                payload
                    .assert_type
                    .map(|label| label.trim().to_string())
                    .filter(|label| !label.is_empty())
            })
            .ok_or(())?;

        let status = if !hit {
            AssertionStatus::Unhit
        } else if condition {
            AssertionStatus::Pass
        } else {
            AssertionStatus::Fail
        };

        Ok(Self {
            label,
            status,
            message,
            must_hit: payload.must_hit,
            location: payload.location.and_then(render_assertion_location),
        })
    }
}

fn render_log_entry(entry: &Value) -> RenderedLogEntry {
    let vtime = entry["moment"]["vtime"].as_str().unwrap_or("").to_string();
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let stream = entry["source"]["stream"].as_str().unwrap_or("");

    if let Some(summary) = parse_assertion_summary(entry) {
        return RenderedLogEntry {
            vtime,
            source: render_source(container, Some("assert")),
            output: render_assertion_summary(&summary),
        };
    }

    RenderedLogEntry {
        vtime,
        source: render_source(container, (!stream.is_empty()).then_some(stream)),
        output: sanitize(entry["output_text"].as_str().unwrap_or("")),
    }
}

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let assertion = entry.get("antithesis_assert")?;
    if !ASSERTION_VALIDATOR.is_valid(assertion) {
        return None;
    }

    let payload: AssertionPayload = serde_json::from_value(assertion.clone()).ok()?;
    AssertionSummary::try_from(payload).ok()
}

fn render_source(container: &str, stream: Option<&str>) -> String {
    let container = sanitize(container);
    let stream = stream.map(sanitize).filter(|stream| !stream.is_empty());

    match (container.is_empty(), stream) {
        (false, Some(stream)) => format!("[{container}:{stream}]"),
        (false, None) => format!("[{container}]"),
        (true, Some(stream)) => format!("[{stream}]"),
        (true, None) => "[]".to_string(),
    }
}

fn render_assertion_summary(summary: &AssertionSummary) -> String {
    let mut output = format!(
        "{} {} \"{}\"",
        summary.status.as_str(),
        sanitize(&summary.label),
        sanitize(&summary.message),
    );

    if summary.must_hit {
        output.push_str(" must-hit");
    }

    if let Some(location) = &summary.location {
        output.push_str(" @ ");
        output.push_str(location);
    }

    output
}

fn render_assertion_location(location: AssertionLocation) -> Option<String> {
    let file = location.file.as_deref().and_then(file_basename);
    let function = location
        .function
        .as_deref()
        .map(str::trim)
        .filter(|function| !function.is_empty())
        .map(sanitize);
    let line = location.begin_line.map(|line| line.to_string());

    let mut rendered = String::new();

    if let Some(file) = file {
        rendered.push_str(&sanitize(file));
    }
    if let Some(function) = function {
        if !rendered.is_empty() {
            rendered.push(':');
        }
        rendered.push_str(&function);
    }
    if let Some(line) = line {
        if !rendered.is_empty() {
            rendered.push(':');
        }
        rendered.push_str(&line);
    }

    (!rendered.is_empty()).then_some(rendered)
}

fn file_basename(file: &str) -> Option<&str> {
    let trimmed = file.trim();
    if trimmed.is_empty() {
        return None;
    }

    Path::new(trimmed)
        .file_name()
        .and_then(|name| name.to_str())
        .or(Some(trimmed))
}

fn render_runs_table(runs: &[RunSummary]) -> String {
    let headers = vec![
        "RUN ID".to_string(),
        "STATUS".to_string(),
        "TYPE".to_string(),
        "CREATED AT".to_string(),
        "LAUNCHER".to_string(),
    ];
    let rows = runs
        .iter()
        .map(|run| {
            vec![
                sanitize(&run.run_id),
                sanitize(&render_enum(&run.status)),
                sanitize(&run.type_.as_ref().map(render_enum).unwrap_or_default()),
                run.created_at.to_rfc3339(),
                sanitize(&run.launcher),
            ]
        })
        .collect::<Vec<_>>();

    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let mut output = String::new();
    push_table_row(&mut output, &headers, &widths);
    for row in rows {
        push_table_row(&mut output, &row, &widths);
    }

    output.trim_end().to_string()
}

fn push_table_row(output: &mut String, row: &[String], widths: &[usize]) {
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            output.push_str("  ");
        }
        output.push_str(&format!("{cell:<width$}", width = widths[index]));
    }
    output.push('\n');
}

fn sanitize(s: &str) -> String {
    let mut escaped = String::new();
    for ch in s.chars() {
        match ch {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push('\t'),
            '\0'..='\u{08}' | '\u{0B}'..='\u{1F}' | '\u{7F}' => {
                escaped.push_str(&format!(r"\x{:02X}", ch as u32));
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn render_enum(value: &impl serde::Serialize) -> String {
    let json = serde_json::to_string(value).unwrap_or_default();
    json.trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_assertion_records_in_compact_form() {
        let entry = json!({
            "antithesis_assert": {
                "assert_type": "always",
                "condition": false,
                "details": null,
                "display_type": "AlwaysOrUnreachable",
                "hit": false,
                "id": "Counter's value retrieved",
                "location": {
                    "begin_column": 0,
                    "begin_line": 87,
                    "class": "",
                    "file": "/go/src/antithesis/control/control.go",
                    "function": "get"
                },
                "message": "Counter's value retrieved",
                "must_hit": false
            },
            "source": {
                "container": "control",
                "name": "control",
                "pid": 1
            },
            "moment": {
                "input_hash": "-4735081784258020614",
                "vtime": "311.8487535319291"
            }
        });

        assert_eq!(
            render_log_entry(&entry),
            RenderedLogEntry {
                vtime: "311.8487535319291".to_string(),
                source: "[control:assert]".to_string(),
                output:
                    "UNHIT AlwaysOrUnreachable \"Counter's value retrieved\" @ control.go:get:87"
                        .to_string(),
            }
        );
    }

    #[test]
    fn falls_back_to_plain_output_for_non_assertion_records() {
        let entry = json!({
            "output_text": "starting",
            "source": {
                "container": "app",
                "stream": "out"
            },
            "moment": {
                "vtime": "1.0"
            }
        });

        assert_eq!(
            render_log_entry(&entry),
            RenderedLogEntry {
                vtime: "1.0".to_string(),
                source: "[app:out]".to_string(),
                output: "starting".to_string(),
            }
        );
    }

    #[test]
    fn ignores_schema_valid_but_incomplete_assertions() {
        let entry = json!({
            "antithesis_assert": {},
            "output_text": "raw log line",
            "source": {
                "container": "control"
            },
            "moment": {
                "vtime": "5.0"
            }
        });

        assert_eq!(
            render_log_entry(&entry),
            RenderedLogEntry {
                vtime: "5.0".to_string(),
                source: "[control]".to_string(),
                output: "raw log line".to_string(),
            }
        );
    }

    #[test]
    fn renders_must_hit_and_partial_location_details() {
        let summary = AssertionSummary::try_from(AssertionPayload {
            hit: Some(false),
            condition: Some(false),
            must_hit: true,
            message: Some("setup reached".to_string()),
            assert_type: Some("reachability".to_string()),
            display_type: Some("SetupReached".to_string()),
            location: Some(AssertionLocation {
                file: None,
                function: Some("run_setup".to_string()),
                begin_line: Some(serde_json::Number::from(42)),
            }),
        })
        .unwrap();

        assert_eq!(
            render_assertion_summary(&summary),
            "UNHIT SetupReached \"setup reached\" must-hit @ run_setup:42"
        );
    }

    #[test]
    fn prefers_display_type_but_falls_back_to_assert_type() {
        let summary = AssertionSummary::try_from(AssertionPayload {
            hit: Some(true),
            condition: Some(true),
            must_hit: false,
            message: Some("first_setup ran".to_string()),
            assert_type: Some("sometimes".to_string()),
            display_type: Some("".to_string()),
            location: None,
        })
        .unwrap();

        assert_eq!(
            render_assertion_summary(&summary),
            "PASS sometimes \"first_setup ran\""
        );
    }

    #[test]
    fn source_without_stream_omits_trailing_colon() {
        assert_eq!(render_source("control", None), "[control]");
    }

    #[test]
    fn sanitize_preserves_printable_unicode_and_punctuation() {
        assert_eq!(
            sanitize("Grüße λ 😸 \"quoted\" C:\\temp\tok"),
            "Grüße λ 😸 \"quoted\" C:\\temp\tok"
        );
    }

    #[test]
    fn sanitize_escapes_newline_and_carriage_return() {
        assert_eq!(sanitize("one\ntwo\rthree"), "one\\ntwo\\rthree");
    }

    #[test]
    fn sanitize_escapes_non_printable_ascii_except_tab() {
        assert_eq!(
            sanitize("a\u{0001}b\u{000B}c\u{007F}d\te"),
            r"a\x01b\x0Bc\x7Fd	e"
        );
    }
}
