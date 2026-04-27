use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;

use color_eyre::eyre::{Result, eyre};
use futures_util::{StreamExt, TryStreamExt};
use jsonschema::Validator;
use log::info;
use serde::Deserialize;
use serde_json::Value;

use crate::api::{
    AntithesisApi, Property, PropertyStatus, RunDetail, RunStatus, RunSummary, RunsFilterOptions,
};
#[cfg(test)]
use crate::api::{Event, Moment};
use crate::cli::{RunsCommands, RunsListArgs};

static ASSERTION_VALIDATOR: LazyLock<Validator> = LazyLock::new(build_assertion_validator);

pub async fn cmd_runs(command: Option<RunsCommands>, json: bool) -> Result<()> {
    match command {
        None => cmd_runs_list(RunsListArgs::default(), json).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args, json).await,
        Some(RunsCommands::Show { run_id }) => cmd_runs_show(&run_id, json).await,
        Some(RunsCommands::Properties {
            run_id,
            passing,
            failing,
        }) => {
            let status = if passing {
                Some(PropertyStatus::Passing)
            } else if failing {
                Some(PropertyStatus::Failing)
            } else {
                None
            };
            cmd_runs_properties(&run_id, status, json).await
        }
        Some(RunsCommands::BuildLogs { run_id }) => cmd_runs_build_logs(&run_id, json).await,
        Some(RunsCommands::Logs {
            run_id,
            input_hash,
            vtime,
            begin_vtime,
            begin_input_hash,
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
        Some(RunsCommands::Events { run_id, query }) => {
            cmd_runs_events(&run_id, &query, json).await
        }
    }
}

async fn cmd_runs_list(args: RunsListArgs, json: bool) -> Result<()> {
    info!("listing runs");

    let api = AntithesisApi::from_env()?;

    let status = args
        .status
        .as_deref()
        .map(|s| s.parse::<RunStatus>())
        .transpose()
        .map_err(|_| {
            eyre!(
                "invalid status: '{}'\nvalid values: starting, in_progress, completed, cancelled, failed, unknown",
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

    // Server returns runs newest-first; .take(limit) short-circuits pagination
    // so we don't materialise the entire run history just to drop most of it.
    let mut runs: Vec<RunSummary> = if has_filters {
        api.stream_runs_filtered(&opts)
            .take(args.limit)
            .try_collect::<Vec<_>>()
            .await?
    } else {
        api.stream_runs()
            .take(args.limit)
            .try_collect::<Vec<_>>()
            .await?
    };

    runs.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then(a.status.cmp(&b.status))
    });

    if json {
        for run in &runs {
            println!("{}", serde_json::to_string(run)?);
        }
        return Ok(());
    }

    if runs.is_empty() {
        println!("No runs found.");
        return Ok(());
    }

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

async fn cmd_runs_properties(
    run_id: &str,
    status: Option<PropertyStatus>,
    json: bool,
) -> Result<()> {
    info!("listing properties for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let mut properties = api
        .stream_run_properties(run_id, status)
        .try_collect::<Vec<_>>()
        .await?;

    if json {
        properties.sort_by(|a, b| {
            property_status_rank(a.status)
                .cmp(&property_status_rank(b.status))
                .then(a.name.cmp(&b.name))
        });
        for property in &properties {
            println!("{}", serde_json::to_string(property)?);
        }
    } else if properties.is_empty() {
        println!("No properties found.");
    } else {
        properties.sort_by(|a, b| {
            property_status_rank(a.status)
                .cmp(&property_status_rank(b.status))
                .then(a.name.cmp(&b.name))
        });
        let rows = flatten_property_events(&properties);
        if rows.is_empty() {
            println!("No sampled property events found.");
        } else {
            println!("{}", render_property_events_table(&rows));
        }
    }

    Ok(())
}

fn print_run_detail(run: &RunDetail) {
    let mut rows: Vec<(&str, String)> = vec![
        ("Run ID", run.run_id.clone()),
        ("Status", render_enum(&run.status)),
    ];

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
    let stream = api.get_run_build_logs(run_id).await?.into_inner();
    let mut stdout = std::io::stdout().lock();

    if json {
        stream_ndjson_lines(stream, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(stream, |line| {
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

async fn cmd_runs_events(run_id: &str, query: &[String], json: bool) -> Result<()> {
    info!("searching events for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let stream = api
        .search_run_events(run_id, &query.join(" "))
        .await?
        .into_inner();

    let mut stdout = std::io::stdout().lock();
    if json {
        stream_ndjson_lines(stream, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        let mut saw_rows = false;
        stream_ndjson_lines(stream, |line| {
            if !saw_rows {
                writeln!(
                    stdout,
                    "{:<22}  {:<22}  {:<20}  OUTPUT",
                    "HASH", "VTIME", "SOURCE"
                )?;
                saw_rows = true;
            }

            if let Ok(entry) = serde_json::from_str::<Value>(line) {
                let rendered = render_event_entry(&entry);
                let input_hash = rendered.input_hash;
                let vtime = rendered.vtime;
                let source = rendered.source;
                let output = rendered.output;
                writeln!(
                    stdout,
                    "{input_hash:<22}  {vtime:<22}  {source:<20}  {output}"
                )?;
            } else {
                writeln!(stdout, "{:<22}  {:<22}  {:<20}  {line}", "", "", "")?;
            }
            Ok(())
        })
        .await?;

        if !saw_rows {
            writeln!(stdout, "No matching events found.")?;
        }

        Ok(())
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
    let stream = api
        .get_run_logs(run_id, input_hash, vtime, begin_input_hash, begin_vtime)
        .await?
        .into_inner();

    let mut stdout = std::io::stdout().lock();
    if json {
        stream_ndjson_lines(stream, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        writeln!(stdout, "{:<22}  {:<20}  OUTPUT", "VTIME", "SOURCE")?;
        stream_ndjson_lines(stream, |line| {
            if let Ok(entry) = serde_json::from_str::<Value>(line) {
                let rendered = render_event_entry(&entry);
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

async fn stream_ndjson_lines<S, C>(
    mut stream: S,
    mut process_line: impl FnMut(&str) -> Result<()>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    use futures_util::StreamExt;

    let mut buf = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let text = std::str::from_utf8(chunk.as_ref())
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
struct RenderedEventEntry {
    input_hash: String,
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

fn render_event_entry(entry: &Value) -> RenderedEventEntry {
    let input_hash = entry["moment"]["input_hash"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let vtime = entry["moment"]["vtime"].as_str().unwrap_or("").to_string();
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let stream = entry["source"]["stream"].as_str().unwrap_or("");

    if let Some(summary) = parse_assertion_summary(entry) {
        return RenderedEventEntry {
            input_hash,
            vtime,
            source: render_source(container, Some("assert")),
            output: render_assertion_summary(&summary),
        };
    }

    RenderedEventEntry {
        input_hash,
        vtime,
        source: render_source(container, (!stream.is_empty()).then_some(stream)),
        output: render_event_output(entry),
    }
}

fn render_event_output(entry: &Value) -> String {
    if let Some(output_text) = entry.get("output_text").and_then(Value::as_str) {
        sanitize(output_text)
    } else {
        sanitize(&serde_json::to_string(entry).unwrap_or_default())
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
        "CREATED AT".to_string(),
        "LAUNCHER".to_string(),
    ];
    let rows = runs
        .iter()
        .map(|run| {
            vec![
                sanitize(&run.run_id),
                sanitize(&render_enum(&run.status)),
                run.created_at.to_rfc3339(),
                sanitize(&run.launcher),
            ]
        })
        .collect::<Vec<_>>();

    render_table(&headers, &rows)
}

struct PropertyEventRow<'a> {
    example: &'static str,
    hash: &'a str,
    vtime: &'a str,
    name: &'a str,
}

fn flatten_property_events(properties: &[Property]) -> Vec<PropertyEventRow<'_>> {
    let mut rows = Vec::new();
    for property in properties {
        let start = rows.len();
        for event in &property.counterexamples {
            rows.push(PropertyEventRow {
                example: "Failing",
                hash: &event.moment.input_hash,
                vtime: &event.moment.vtime,
                name: &property.name,
            });
        }
        for event in &property.examples {
            rows.push(PropertyEventRow {
                example: "Passing",
                hash: &event.moment.input_hash,
                vtime: &event.moment.vtime,
                name: &property.name,
            });
        }
        rows[start..].sort_by(|a, b| {
            example_rank(a.example)
                .cmp(&example_rank(b.example))
                .then(a.vtime.cmp(b.vtime))
        });
    }
    rows
}

fn example_rank(example: &str) -> u8 {
    match example {
        "Failing" => 0,
        _ => 1,
    }
}

fn render_property_events_table(rows: &[PropertyEventRow]) -> String {
    let headers = vec![
        "EXAMPLE".to_string(),
        "HASH".to_string(),
        "VTIME".to_string(),
        "NAME".to_string(),
    ];
    let table_rows = rows
        .iter()
        .map(|row| {
            vec![
                row.example.to_string(),
                sanitize(row.hash),
                sanitize(row.vtime),
                sanitize(row.name),
            ]
        })
        .collect::<Vec<_>>();
    render_table(&headers, &table_rows)
}

fn render_table(headers: &[String], rows: &[Vec<String>]) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let mut output = String::new();
    push_table_row(&mut output, headers, &widths);
    for row in rows {
        push_table_row(&mut output, row, &widths);
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

fn property_status_rank(status: PropertyStatus) -> u8 {
    match status {
        PropertyStatus::Failing => 0,
        PropertyStatus::Passing => 1,
    }
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
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "-4735081784258020614".to_string(),
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
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "".to_string(),
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
            render_event_entry(&entry),
            RenderedEventEntry {
                input_hash: "".to_string(),
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

    fn event(input_hash: &str, vtime: &str) -> Event {
        Event {
            moment: Moment {
                input_hash: input_hash.to_string(),
                vtime: vtime.to_string(),
            },
        }
    }

    #[test]
    fn renders_flattened_property_events_table() {
        let properties = vec![
            Property {
                counterexample_count: Some(3),
                counterexamples: vec![event("-100", "5.0"), event("-200", "10.0")],
                description: None,
                example_count: Some(12),
                examples: vec![event("-300", "15.0")],
                group: Some("Safety".to_string()),
                is_event: true,
                is_existential: false,
                is_group: None,
                is_universal: true,
                name: "Counter value stays below limit".to_string(),
                status: PropertyStatus::Failing,
            },
            Property {
                counterexample_count: Some(0),
                counterexamples: Vec::new(),
                description: None,
                example_count: Some(1),
                examples: vec![event("-400", "1.0")],
                group: None,
                is_event: false,
                is_existential: true,
                is_group: None,
                is_universal: false,
                name: "Setup completes".to_string(),
                status: PropertyStatus::Passing,
            },
        ];

        let rows = flatten_property_events(&properties);
        let table = render_property_events_table(&rows);

        assert!(table.contains("EXAMPLE"));
        assert!(table.contains("HASH"));
        assert!(table.contains("VTIME"));
        assert!(table.contains("NAME"));

        let lines: Vec<&str> = table.lines().collect();
        // Header + 2 Failing rows + 2 Passing rows = 5 lines
        assert_eq!(lines.len(), 5);

        // Failing rows come first, sorted by vtime (lexicographic: "10.0" < "5.0")
        assert!(
            lines[1].contains("Failing")
                && lines[1].contains("-200")
                && lines[1].contains("10.0")
                && lines[1].contains("Counter value stays below limit")
        );
        assert!(
            lines[2].contains("Failing")
                && lines[2].contains("-100")
                && lines[2].contains("5.0")
                && lines[2].contains("Counter value stays below limit")
        );

        // Passing rows come after, grouped by property (Counter value first, then Setup completes)
        assert!(
            lines[3].contains("Passing")
                && lines[3].contains("-300")
                && lines[3].contains("15.0")
                && lines[3].contains("Counter value stays below limit")
        );
        assert!(
            lines[4].contains("Passing")
                && lines[4].contains("-400")
                && lines[4].contains("1.0")
                && lines[4].contains("Setup completes")
        );
    }

    #[test]
    fn flatten_returns_empty_when_no_sampled_events() {
        let properties = vec![Property {
            counterexample_count: Some(0),
            counterexamples: Vec::new(),
            description: None,
            example_count: Some(0),
            examples: Vec::new(),
            group: None,
            is_event: false,
            is_existential: false,
            is_group: None,
            is_universal: true,
            name: "No events property".to_string(),
            status: PropertyStatus::Passing,
        }];

        let rows = flatten_property_events(&properties);
        assert!(rows.is_empty());
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
