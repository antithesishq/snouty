use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::OnceLock;

use color_eyre::eyre::{Result, eyre};
use futures_util::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use indexmap::map::Entry;
use log::debug;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use chrono::{DateTime, Local, Utc};
use chrono_humanize::{Accuracy, HumanTime, Tense};

use crate::api::{
    AntithesisApi, Property, PropertyStatus, RunDetail, RunStatus, RunSummary, RunsFilterOptions,
};
#[cfg(test)]
use crate::api::{Event, EventProperty, Moment, NonEventProperty};
use crate::cli::{RunsCommands, RunsListArgs};
use crate::error::user_error;

/// Event stream classification. Variants match the canonical values that
/// appear in an event's `source.stream` field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
    Info,
    Error,
}

impl Stream {
    /// Three-character display abbreviation used in the logs viewer.
    pub fn abbreviated(self) -> &'static str {
        match self {
            Self::Stdout => "out",
            Self::Stderr => "err",
            Self::Info => "inf",
            Self::Error => "err",
        }
    }
}

impl std::str::FromStr for Stream {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            "info" => Ok(Self::Info),
            "error" => Ok(Self::Error),
            other => Err(format!(
                "invalid stream '{other}' (expected one of: stdout, stderr, info, error)"
            )),
        }
    }
}

pub async fn cmd_runs(command: Option<RunsCommands>, json: bool, verbose: bool) -> Result<()> {
    match command {
        None => cmd_runs_list(RunsListArgs::default(), json, verbose).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args, json, verbose).await,
        Some(RunsCommands::Show { run_id }) => cmd_runs_show(&run_id, json, verbose).await,
        Some(RunsCommands::Open { run_id }) => cmd_runs_open(&run_id, json, verbose).await,
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
            cmd_runs_properties(&run_id, status, json, verbose).await
        }
        Some(RunsCommands::Property { run_id, name }) => {
            cmd_runs_property(&run_id, &name, json, verbose).await
        }
        Some(RunsCommands::BuildLogs { run_id }) => {
            cmd_runs_build_logs(&run_id, json, verbose).await
        }
        Some(RunsCommands::Logs {
            run_id,
            input_hash,
            vtime,
            begin_vtime,
            begin_input_hash,
            disable_fault_annotation,
        }) => {
            cmd_runs_logs(
                &run_id,
                &input_hash,
                &vtime,
                begin_input_hash.as_deref(),
                begin_vtime.as_deref(),
                LogOutputOptions {
                    json,
                    verbose,
                    annotate_faults: !disable_fault_annotation,
                },
            )
            .await
        }
        Some(RunsCommands::Events {
            run_id,
            matches,
            source,
            stream,
            vtime_min,
            vtime_max,
        }) => {
            let filters = EventFilters {
                matches: &matches,
                sources: &source,
                stream,
                vtime_min,
                vtime_max,
            };
            cmd_runs_events(&run_id, filters, json, verbose).await
        }
    }
}

async fn cmd_runs_list(args: RunsListArgs, json: bool, verbose: bool) -> Result<()> {
    debug!("listing runs");

    let api = AntithesisApi::from_env(verbose)?;

    // clap parsed and validated the filter flags into their real types, so the
    // options struct is built directly with no further string parsing here.
    let opts = RunsFilterOptions {
        status: args.status,
        launcher: args.launcher,
        created_after: args.created_after,
        created_before: args.created_before,
    };

    // The API caps `limit` at 100, so request only as many as we'll display and
    // let the server do the trimming. For limits above 100 we still paginate,
    // capping the total client-side with `.take(limit)`.
    let page_limit = args.limit.clamp(1, 100) as u64;

    // Server returns runs newest-first; .take(limit) short-circuits pagination
    // so we don't materialise the entire run history just to drop most of it.
    let mut runs: Vec<RunSummary> = api
        .stream_runs_filtered(&opts, page_limit)
        .take(args.limit)
        .try_collect::<Vec<_>>()
        .await?;

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

    if args.long {
        print!("{}", render_runs_long(&runs));
    } else {
        let width = terminal_width();
        println!("{}", render_runs_table(&runs, width));
    }
    Ok(())
}

fn terminal_width() -> usize {
    let term = console::Term::stdout();
    if !term.is_term() {
        return 100;
    }
    term.size().1 as usize
}

/// Short human-readable run status word (e.g. `completed`, `in_progress`),
/// reusing the canonical `RunStatus` display string so the term matches the
/// API and `snouty runs show`.
fn status_label(status: RunStatus) -> String {
    status.to_string()
}

fn relative_time(then: DateTime<Utc>) -> String {
    HumanTime::from(then - Utc::now()).to_text_en(Accuracy::Rough, Tense::Past)
}

/// Format an absolute timestamp in the user's local timezone, without a
/// timezone suffix (the times in snouty's output are always local, so showing
/// the offset would just be noise). Example: `2026-05-27 08:25:13`.
fn format_local(dt: DateTime<Utc>) -> String {
    dt.with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Reformat an RFC 3339 timestamp string into the local, suffix-less format.
/// Falls back to the original string if it can't be parsed.
fn format_local_str(raw: &str) -> String {
    match DateTime::parse_from_rfc3339(raw) {
        Ok(dt) => format_local(dt.with_timezone(&Utc)),
        Err(_) => raw.to_string(),
    }
}

async fn cmd_runs_show(run_id: &str, json: bool, verbose: bool) -> Result<()> {
    debug!("showing run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let run = api.get_run(run_id).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&run)?);
    } else {
        print_run_detail(&run);
    }

    Ok(())
}

async fn cmd_runs_open(run_id: &str, json: bool, verbose: bool) -> Result<()> {
    debug!("opening report for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let run = api.get_run(run_id).await?;

    let url = run
        .links
        .as_ref()
        .and_then(|l| l.triage_report.as_deref())
        .ok_or_else(|| {
            user_error(format!(
                "no report available for run {} with status {}",
                run_id, run.status
            ))
        })?;

    if json {
        println!("{}", serde_json::json!({ "url": url }));
        return Ok(());
    }

    let launched = launch_browser(url);
    if launched {
        println!("Opening report for run {run_id}…");
        println!("If your browser didn't open, manually visit:");
        println!("  {url}");
    } else {
        println!("Open this URL to view the report:");
        println!("  {url}");
    }
    Ok(())
}

fn launch_browser(url: &str) -> bool {
    use std::process::{Command, Stdio};

    let (program, args): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("open", vec![url])
    } else if cfg!(target_os = "windows") {
        ("cmd", vec!["/C", "start", "", url])
    } else {
        ("xdg-open", vec![url])
    };

    Command::new(program)
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn cmd_runs_properties(
    run_id: &str,
    status: Option<PropertyStatus>,
    json: bool,
    verbose: bool,
) -> Result<()> {
    debug!("listing properties for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let mut properties = api
        .stream_run_properties(run_id, status)
        .try_collect::<Vec<_>>()
        .await?;

    properties.sort_by(|a, b| {
        property_group(a)
            .unwrap_or("")
            .cmp(property_group(b).unwrap_or(""))
            .then(a.name().cmp(b.name()))
    });

    if json {
        for property in &properties {
            println!("{}", serde_json::to_string(property)?);
        }
    } else if properties.is_empty() {
        let message = match status {
            Some(PropertyStatus::Passing) => "No passing properties found.",
            Some(PropertyStatus::Failing) => "No failing properties found.",
            None => "No properties found.",
        };
        println!("{message}");
    } else {
        println!("{}", render_properties_table(&properties));
    }

    Ok(())
}

fn property_group(p: &Property) -> Option<&str> {
    match p {
        Property::EventProperty(p) => p.group.as_deref(),
        Property::NonEventProperty(p) => p.group.as_deref(),
    }
}

fn property_is_group(p: &Property) -> bool {
    match p {
        Property::EventProperty(p) => p.is_group.unwrap_or(false),
        Property::NonEventProperty(p) => p.is_group.unwrap_or(false),
    }
}

fn property_example_total(p: &Property) -> u64 {
    let (ex, cex) = match p {
        Property::EventProperty(p) => (p.example_count, p.counterexample_count),
        Property::NonEventProperty(p) => (p.example_count, p.counterexample_count),
    };
    u64::from(ex.unwrap_or(0)) + u64::from(cex.unwrap_or(0))
}

fn property_status_label(status: PropertyStatus) -> &'static str {
    match status {
        PropertyStatus::Passing => "passing",
        PropertyStatus::Failing => "failing",
    }
}

async fn cmd_runs_property(run_id: &str, name: &str, json: bool, verbose: bool) -> Result<()> {
    debug!("looking up property '{}' for run: {}", name, run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let properties = api
        .stream_run_properties(run_id, None)
        .try_collect::<Vec<_>>()
        .await?;

    let resolved = resolve_property(&properties, name)?;
    let property = match resolved {
        Resolved::Exact(p) => p,
        Resolved::Fuzzy(p) => {
            if !json {
                eprintln!(
                    "note: no exact match for '{}', using closest property: '{}'",
                    name,
                    p.name()
                );
            }
            p
        }
    };

    if json {
        println!("{}", serde_json::to_string_pretty(property)?);
        return Ok(());
    }

    print_property_header(property);
    println!("{}", render_property_examples(property));
    Ok(())
}

#[derive(Debug)]
enum Resolved<'a> {
    Exact(&'a Property),
    Fuzzy(&'a Property),
}

fn resolve_property<'a>(properties: &'a [Property], query: &str) -> Result<Resolved<'a>> {
    // Match case-insensitively throughout: an exact name in any case is an
    // exact hit (no "closest property" note), and the substring fallback
    // ignores case too.
    let needle = query.to_lowercase();

    if let Some(p) = properties
        .iter()
        .find(|p| p.name().to_lowercase() == needle)
    {
        return Ok(Resolved::Exact(p));
    }

    let matches: Vec<&Property> = properties
        .iter()
        .filter(|p| p.name().to_lowercase().contains(&needle))
        .collect();

    match matches.as_slice() {
        [] => Err(user_error(format!("no property matches '{query}'"))),
        [only] => Ok(Resolved::Fuzzy(only)),
        many => {
            let names = many
                .iter()
                .map(|p| format!("  - {}", p.name()))
                .collect::<Vec<_>>()
                .join("\n");
            Err(user_error(format!(
                "multiple properties match '{query}', did you mean one of:\n{names}"
            )))
        }
    }
}

fn print_property_header(property: &Property) {
    println!("Name      {}", sanitize(property.name()));
    println!("Status    {}", property_status_label(property.status()));
    if let Some(group) = property_group(property) {
        println!("Group     {}", sanitize(group));
    }
    let description = match property {
        Property::EventProperty(p) => p.description.as_deref(),
        Property::NonEventProperty(p) => p.description.as_deref(),
    };
    if let Some(desc) = description {
        println!("Details   {}", sanitize(desc));
    }
    println!();
}

fn render_property_examples(property: &Property) -> String {
    match property {
        Property::EventProperty(p) => {
            let mut rows: Vec<Vec<String>> = Vec::new();
            for event in &p.counterexamples {
                rows.push(vec![
                    "failing".to_string(),
                    sanitize(&event.moment.input_hash),
                    sanitize(&event.moment.vtime),
                ]);
            }
            for event in &p.examples {
                rows.push(vec![
                    "passing".to_string(),
                    sanitize(&event.moment.input_hash),
                    sanitize(&event.moment.vtime),
                ]);
            }
            if rows.is_empty() {
                rows.push(vec![
                    "unreachable".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ]);
            }
            let headers = vec![
                "STATUS".to_string(),
                "HASH".to_string(),
                "VTIME".to_string(),
            ];
            render_table(&headers, &rows)
        }
        Property::NonEventProperty(p) => {
            let mut rows: Vec<Vec<String>> = Vec::new();
            let mut detail_blocks: Vec<(usize, String)> = Vec::new();

            for value in &p.counterexamples {
                push_value_row(&mut rows, &mut detail_blocks, "failing", value);
            }
            for value in &p.examples {
                push_value_row(&mut rows, &mut detail_blocks, "passing", value);
            }
            if rows.is_empty() {
                rows.push(vec!["unreachable".to_string(), "-".to_string()]);
            }
            let headers = vec!["STATUS".to_string(), "VALUE".to_string()];
            let mut out = render_table(&headers, &rows);
            for (row_index, block) in detail_blocks {
                out.push_str(&format!(
                    "\n\nrow {} details:\n{}",
                    row_index + 1,
                    indent_lines(&block, "  ")
                ));
            }
            out
        }
    }
}

fn push_value_row(
    rows: &mut Vec<Vec<String>>,
    detail_blocks: &mut Vec<(usize, String)>,
    status: &str,
    value: &Value,
) {
    let row_index = rows.len();
    let (cell, detail) = render_property_value(value);
    rows.push(vec![status.to_string(), cell]);
    if let Some(detail) = detail {
        detail_blocks.push((row_index, detail));
    }
}

fn render_property_value(value: &Value) -> (String, Option<String>) {
    match value {
        Value::Null => ("null".to_string(), None),
        Value::Bool(b) => (b.to_string(), None),
        Value::Number(n) => (n.to_string(), None),
        Value::String(s) => (sanitize(s), None),
        Value::Array(_) | Value::Object(_) => {
            let summary = match value {
                Value::Array(items) => format!("[{} items]", items.len()),
                Value::Object(map) => format!("{{{} fields}}", map.len()),
                _ => unreachable!(),
            };
            let pretty = serde_json::to_string_pretty(value).unwrap_or_default();
            (summary, Some(pretty))
        }
    }
}

fn indent_lines(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_properties_table(properties: &[Property]) -> String {
    let headers = vec![
        "STATUS".to_string(),
        "GROUP".to_string(),
        "NAME".to_string(),
        "EXAMPLES".to_string(),
    ];
    let rows = properties
        .iter()
        .map(|p| {
            let name = if property_is_group(p) {
                format!("▸ {}", sanitize(p.name()))
            } else {
                sanitize(p.name())
            };
            vec![
                property_status_label(p.status()).to_string(),
                sanitize(property_group(p).unwrap_or("")),
                name,
                property_example_total(p).to_string(),
            ]
        })
        .collect::<Vec<_>>();
    render_table(&headers, &rows)
}

fn print_run_detail(run: &RunDetail) {
    let mut rows: Vec<(&str, String)> = Vec::new();

    if let Some(name) = run.test_name() {
        rows.push(("Test Name", name.to_string()));
    }
    if let Some(desc) = run.test_description() {
        rows.push(("Description", desc.to_string()));
    }

    rows.push(("Run ID", run.run_id.clone()));
    rows.push(("Status", status_label(run.status)));
    rows.push(("Created", format_local(run.created_at)));

    if let Some(t) = run.started_at {
        rows.push(("Started", format_local(t)));
    }
    if let Some(t) = run.completed_at {
        rows.push(("Completed", format_local(t)));
    }

    rows.push(("Launcher", run.launcher.clone()));

    if let Some(ref moment) = run.failure_moment {
        rows.push(("Failure VTime", moment.vtime.clone()));
        rows.push(("Failure Hash", moment.input_hash.clone()));
    }

    if let Some(ref creator) = run.creator
        && let Some(ref name) = creator.name
    {
        rows.push(("Creator", name.clone()));
    }

    print!("{}", render_kv(&rows));
}

/// Render aligned `Label  value` lines, sqlite `.mode line`–style. Each line is
/// terminated with a newline; values are sanitized.
fn render_kv(rows: &[(&str, String)]) -> String {
    let label_width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    let mut out = String::new();
    for (label, value) in rows {
        out.push_str(&format!("{label:label_width$}  {}\n", sanitize(value)));
    }
    out
}

/// `runs list --long`: one aligned key-value block per run (no table),
/// separated by blank lines. Empty optional fields are omitted.
fn render_runs_long(runs: &[RunSummary]) -> String {
    let blocks: Vec<String> = runs
        .iter()
        .map(|run| {
            let mut rows: Vec<(&str, String)> = vec![
                ("Run ID", run.run_id.clone()),
                ("Status", status_label(run.status)),
                ("Created", format_local(run.created_at)),
                ("Launcher", run.launcher.clone()),
            ];
            if let Some(name) = run.test_name() {
                rows.push(("Test Name", name.to_string()));
            }
            if let Some(description) = run.test_description() {
                rows.push(("Description", description.to_string()));
            }
            render_kv(&rows)
        })
        .collect();

    // Each block already ends in a newline; joining with "\n" inserts one blank
    // line between consecutive runs.
    blocks.join("\n")
}

struct LogOutputOptions {
    json: bool,
    verbose: bool,
    annotate_faults: bool,
}

async fn cmd_runs_build_logs(run_id: &str, json: bool, verbose: bool) -> Result<()> {
    debug!("streaming build logs for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let stream = api.get_run_build_logs(run_id).await?.into_inner();
    let mut stdout = BufWriter::new(std::io::stdout().lock());

    let result = if json {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                let ts = format_local_str(entry["timestamp"].as_str().unwrap_or(""));
                let stream = entry["stream"].as_str().unwrap_or("out");
                let text = sanitize(entry["text"].as_str().unwrap_or(""));
                writeln!(stdout, "{ts} [{stream}] {text}")?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    };

    stdout.flush()?;
    result
}

#[derive(Clone, Copy)]
struct EventFilters<'a> {
    matches: &'a [String],
    sources: &'a [String],
    stream: Option<Stream>,
    vtime_min: Option<f64>,
    vtime_max: Option<f64>,
}

async fn cmd_runs_events(
    run_id: &str,
    filters: EventFilters<'_>,
    json: bool,
    verbose: bool,
) -> Result<()> {
    debug!("searching events for run: {}", run_id);

    // The server endpoint takes a single combined `q` substring. Send the first
    // --match to maximize server-side filtering; AND-combine the rest of the
    // matches plus the structural filters client-side on the streamed NDJSON.
    let server_query = filters.matches.first().cloned().unwrap_or_default();

    let api = AntithesisApi::from_env(verbose)?;
    let stream = api
        .search_run_events(run_id, &server_query)
        .await?
        .into_inner();

    let lowered_matches: Vec<String> = filters
        .matches
        .iter()
        .map(|m| m.to_ascii_lowercase())
        .collect();
    let has_structural_filters = !filters.sources.is_empty()
        || filters.stream.is_some()
        || filters.vtime_min.is_some()
        || filters.vtime_max.is_some();

    let mut stdout = BufWriter::new(std::io::stdout().lock());
    // The table header is printed lazily, on the first matching row, so a run
    // with no matches shows a friendly message instead of a bare header.
    let mut emitted: usize = 0;
    let result = stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
        // Cheap path: substring AND across all --match needles on the raw line.
        let line_lower = line.to_ascii_lowercase();
        if !lowered_matches.iter().all(|n| line_lower.contains(n)) {
            return Ok(());
        }

        // JSON mode with only --match filters: pass through without parsing.
        if json && !has_structural_filters {
            writeln!(stdout, "{line}")?;
            emitted += 1;
            return Ok(());
        }

        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                if json {
                    writeln!(stdout, "{line}")?;
                    emitted += 1;
                }
                return Ok(());
            }
        };

        if has_structural_filters && !entry_matches_structural_filters(&entry, filters) {
            return Ok(());
        }

        if json {
            writeln!(stdout, "{line}")?;
        } else {
            if emitted == 0 {
                writeln!(
                    stdout,
                    "{:<22}  {:<22}  {:<20}  OUTPUT",
                    "HASH", "VTIME", "SOURCE"
                )?;
            }
            let rendered = render_event_entry(&entry);
            let input_hash = rendered.input_hash;
            let vtime = rendered.vtime;
            let source = rendered.source;
            let output = rendered.output;
            writeln!(
                stdout,
                "{input_hash:<22}  {vtime:<22}  {source:<20}  {output}"
            )?;
        }
        emitted += 1;
        Ok(())
    })
    .await;

    // The closure's borrows of `stdout`/`emitted` end once the stream future
    // above resolves, so it's safe to use them again here.
    if result.is_ok() && !json && emitted == 0 {
        let query = filters.matches.join(" ");
        writeln!(stdout, "No events matched \"{query}\".")?;
    }
    stdout.flush()?;
    result
}

#[cfg(test)]
fn event_matches_filters(entry: &Value, raw_line: &str, filters: EventFilters) -> bool {
    let line_lower = raw_line.to_ascii_lowercase();
    let all_match = filters
        .matches
        .iter()
        .all(|n| line_lower.contains(&n.to_ascii_lowercase()));
    all_match && entry_matches_structural_filters(entry, filters)
}

fn entry_matches_structural_filters(entry: &Value, filters: EventFilters) -> bool {
    if !filters.sources.is_empty() {
        let container = entry["source"]["container"].as_str().unwrap_or("");
        let name = entry["source"]["name"].as_str().unwrap_or("");
        if !filters
            .sources
            .iter()
            .any(|wanted| wanted == container || wanted == name)
        {
            return false;
        }
    }

    if let Some(wanted) = filters.stream {
        let raw = entry["source"]["stream"].as_str().unwrap_or("");
        if raw.parse::<Stream>().ok() != Some(wanted) {
            return false;
        }
    }

    if filters.vtime_min.is_some() || filters.vtime_max.is_some() {
        let vtime = entry["moment"]["vtime"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok());
        let Some(vtime) = vtime else {
            return false;
        };
        if let Some(min) = filters.vtime_min
            && vtime < min
        {
            return false;
        }
        if let Some(max) = filters.vtime_max
            && vtime > max
        {
            return false;
        }
    }

    true
}

async fn cmd_runs_logs(
    run_id: &str,
    input_hash: &str,
    vtime: &str,
    begin_input_hash: Option<&str>,
    begin_vtime: Option<&str>,
    LogOutputOptions {
        json,
        verbose,
        annotate_faults,
    }: LogOutputOptions,
) -> Result<()> {
    debug!("streaming logs for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let stream = api
        .get_run_logs(run_id, input_hash, vtime, begin_input_hash, begin_vtime)
        .await?
        .into_inner();

    let mut stdout = BufWriter::new(std::io::stdout().lock());
    let result = if json {
        if annotate_faults {
            stream_ndjson_lines(
                stream,
                FaultAnnotator {
                    active_fault_windows: ActiveFaultWindows::new(),
                    active_faults: json!({}),
                },
                |line| {
                    writeln!(stdout, "{line}")?;
                    Ok(())
                },
            )
            .await
        } else {
            stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
                writeln!(stdout, "{line}")?;
                Ok(())
            })
            .await
        }
    } else {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
            if let Ok(entry) = serde_json::from_str::<Value>(line) {
                writeln!(stdout, "{}", format_log_line(&entry))?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    };
    stdout.flush()?;
    result
}

const LOG_SOURCE_MIN_WIDTH: usize = 20;
const LOG_VTIME_WIDTH: usize = 14;
const LOG_STREAM_WIDTH: usize = 3;

fn format_log_line(entry: &Value) -> String {
    let vtime = entry["moment"]["vtime"].as_str().unwrap_or("");
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let source = if !container.is_empty() {
        container
    } else {
        name
    };
    let stream_raw = entry["source"]["stream"].as_str().unwrap_or("");
    let stream = abbreviate_stream(stream_raw);

    // Web UI format: text records sit one space after the stream bracket.
    // JSON records get an extra " - " separator before the body.
    let payload = if let Some(text) = entry.get("output_text").and_then(Value::as_str) {
        text.to_string()
    } else {
        format!(" - {}", strip_log_envelope(entry))
    };

    format!(
        "[{vtime:>vw$}] [{source:>sw$}] [{stream:<stw$}] {payload}",
        vw = LOG_VTIME_WIDTH,
        sw = LOG_SOURCE_MIN_WIDTH,
        stw = LOG_STREAM_WIDTH,
    )
}

fn abbreviate_stream(stream: &str) -> std::borrow::Cow<'static, str> {
    if let Ok(s) = stream.parse::<Stream>() {
        return std::borrow::Cow::Borrowed(s.abbreviated());
    }
    if stream.is_empty() {
        return std::borrow::Cow::Borrowed("   ");
    }
    std::borrow::Cow::Owned(stream.chars().take(LOG_STREAM_WIDTH).collect())
}

fn strip_log_envelope(entry: &Value) -> String {
    let mut clone = entry.clone();
    if let Some(obj) = clone.as_object_mut() {
        obj.remove("moment");
        obj.remove("source");
        obj.remove("IPT_bytes_out");
    }
    serde_json::to_string(&clone).unwrap_or_default()
}

const TICKS_PER_SECOND: f64 = (1u64 << 32) as f64;

async fn stream_ndjson_lines<S, C>(
    mut stream: S,
    mut line_transformer: impl LineTransformer,
    mut process_line: impl FnMut(&str) -> Result<()>,
) -> Result<()>
where
    S: futures_util::Stream<Item = reqwest::Result<C>> + Unpin,
    C: AsRef<[u8]>,
{
    use futures_util::StreamExt;

    let mut buf: Vec<u8> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(chunk.as_ref());

        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes = &buf[..pos];
            if !line_bytes.is_empty() {
                let line = std::str::from_utf8(line_bytes)
                    .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
                if let Some(transformed) = line_transformer.try_transform(line) {
                    process_line(&transformed)?;
                } else {
                    process_line(line)?;
                }
            }
            buf.drain(..=pos);
        }
    }

    if !buf.is_empty() {
        let line = std::str::from_utf8(&buf)
            .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
        if let Some(transformed) = line_transformer.try_transform(line) {
            process_line(&transformed)?;
        } else {
            process_line(line)?;
        }
    }

    Ok(())
}

trait LineTransformer {
    fn try_transform(&mut self, line: &str) -> Option<String>;
}

struct NoOpTransformer {}

impl LineTransformer for NoOpTransformer {
    fn try_transform(&mut self, _: &str) -> Option<String> {
        None
    }
}

struct FaultAnnotator {
    active_fault_windows: ActiveFaultWindows,
    active_faults: Value,
}

impl LineTransformer for FaultAnnotator {
    fn try_transform(&mut self, line: &str) -> Option<String> {
        if let Ok(mut entry) = serde_json::from_str::<Value>(line) {
            let mut update_faults = false;

            let vtime_ticks_node = entry["moment"]["_vtime_ticks"].as_u64();
            let vtime_node = entry["moment"]["vtime"]
                .as_str()
                .and_then(|seconds_string| seconds_string.parse::<f64>().ok());
            let event_vtime_ticks = vtime_node
                .map(|seconds| (seconds * TICKS_PER_SECOND) as u64)
                .or(vtime_ticks_node)
                .unwrap_or(0);
            let fault_name = entry["fault"]["name"].as_str();
            let is_fault_injector = entry["source"]["name"]
                .as_str()
                .map(|source| source.eq("fault_injector"))
                .unwrap_or(false);

            // Clear network and node faults if the fault injector was paused
            if is_fault_injector
                && entry["info"]["message"]
                    .as_str()
                    .map(|message| message.eq("status"))
                    .unwrap_or(false)
                && entry["info"]["details"]["paused"]
                    .as_bool()
                    .unwrap_or(false)
            {
                update_faults = self.active_fault_windows.clear_network_faults() || update_faults;
                update_faults = self.active_fault_windows.clear_node_faults() || update_faults;
            }

            // Clear network faults if the network was restored
            if is_fault_injector && fault_name.map(|n| n.eq("restore")).unwrap_or(false) {
                update_faults = self.active_fault_windows.clear_network_faults() || update_faults;
            }

            // Clear any expired faults
            update_faults = self
                .active_fault_windows
                .clear_expired_faults(event_vtime_ticks)
                || update_faults;

            if is_fault_injector && let Some(fault_name) = fault_name {
                let max_duration_ticks = entry["fault"]["max_duration"]
                    .as_f64()
                    .filter(|d| *d > 0.0)
                    .map(|d| (d * TICKS_PER_SECOND) as u64);
                let end_vtime = max_duration_ticks.map(|duration| duration + event_vtime_ticks);
                let fault_type = entry["fault"]["type"].as_str().unwrap_or("");

                if let Some(target) = entry["fault"]["affected_nodes"]
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|first| first.as_str())
                {
                    if fault_name.eq("partition") || fault_name.eq("clog") {
                        update_faults = self.active_fault_windows.add_network_fault(
                            fault_name.to_string(),
                            FaultWindowBounds {
                                start_vtime: event_vtime_ticks,
                                end_vtime,
                            },
                        ) || update_faults;
                    }

                    if fault_type.eq("node")
                        && (fault_name.eq("pause") || fault_name.eq("throttle"))
                    {
                        update_faults = self.active_fault_windows.add_node_fault(
                            fault_name.to_string(),
                            target.to_string(),
                            FaultWindowBounds {
                                start_vtime: event_vtime_ticks,
                                end_vtime,
                            },
                        ) || update_faults;
                    }
                }

                if fault_name.eq("skip")
                    && fault_type.eq("clock")
                    && let Some(offset) = entry["fault"]["details"]["offset"].as_f64()
                {
                    update_faults = self.active_fault_windows.add_clock_fault(
                        offset,
                        FaultWindowBounds {
                            start_vtime: event_vtime_ticks,
                            end_vtime,
                        },
                    ) || update_faults;
                }
            }

            if update_faults {
                self.active_faults = self.active_fault_windows.to_json();
            }

            if let Some(output_text) = entry["output_text"].as_str() {
                entry["output_text"] = Value::String(strip_ansi(output_text));
            }
            if vtime_ticks_node.is_some() || vtime_node.is_some() {
                entry["vtime_seconds"] = json!((event_vtime_ticks as f64) / TICKS_PER_SECOND);
            }

            if entry.is_object() {
                entry["active_faults"] = self.active_faults.clone();
                return Some(format!("{}", entry));
            }
        }

        None
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RenderedEventEntry {
    input_hash: String,
    vtime: String,
    source: String,
    output: String,
}

fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(concat!(
            r"\x1b\[[\x20-\x3f]*[\x40-\x7e]",      // CSI: ESC [ ... final
            r"|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", // OSC: ESC ] ... (BEL | ESC \)
            r"|\x1b[\x20-\x7e]",                   // two-byte: ESC + single printable
        ))
        .unwrap()
    })
}

fn strip_ansi(text: &str) -> String {
    ansi_re().replace_all(text, "").to_string()
}

struct FaultWindowBounds {
    start_vtime: u64,
    end_vtime: Option<u64>,
}

impl FaultWindowBounds {
    fn is_expired(&self, latest_vtime_ticks: &u64) -> bool {
        self.end_vtime
            .map(|expiry| expiry.lt(latest_vtime_ticks))
            .unwrap_or(false)
    }
}

struct ActiveFaultWindows {
    network: IndexMap<String, FaultWindowBounds>,
    node: IndexMap<String, IndexMap<String, FaultWindowBounds>>,
    clock: Vec<(f64, FaultWindowBounds)>,
}

impl ActiveFaultWindows {
    fn new() -> ActiveFaultWindows {
        ActiveFaultWindows {
            network: IndexMap::new(),
            node: IndexMap::new(),
            clock: Vec::new(),
        }
    }

    fn clear_network_faults(&mut self) -> bool {
        let did_something = !self.network.is_empty();
        self.network.clear();
        did_something
    }

    fn clear_node_faults(&mut self) -> bool {
        let did_something = !self.node.is_empty();
        self.node.clear();
        did_something
    }

    fn clear_expired_faults(&mut self, latest_vtime_ticks: u64) -> bool {
        let mut did_something;

        let clock_faults_length = self.clock.len();
        self.clock
            .retain(|fault| !fault.1.is_expired(&latest_vtime_ticks));
        did_something = self.clock.len() != clock_faults_length;

        for _ in self
            .network
            .extract_if(.., |_k, window| window.is_expired(&latest_vtime_ticks))
        {
            did_something = true;
        }

        let mut dropped_categories_of_node_faults = false;
        for _ in self.node.extract_if(.., |_k, windows_by_container| {
            for _ in windows_by_container
                .extract_if(.., |_c, window| window.is_expired(&latest_vtime_ticks))
            {
                did_something = true;
            }

            windows_by_container.is_empty()
        }) {
            dropped_categories_of_node_faults = true;
        }
        did_something = did_something || dropped_categories_of_node_faults;

        did_something
    }

    fn add_network_fault(&mut self, name: String, window: FaultWindowBounds) -> bool {
        match self.network.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(window);
                true
            }
            Entry::Occupied(mut entry) => {
                if let Some(updated) = merge_fault_windows(entry.get(), window) {
                    entry.insert(updated);
                    return true;
                }

                false
            }
        }
    }

    fn add_node_fault(
        &mut self,
        name: String,
        container_name: String,
        window: FaultWindowBounds,
    ) -> bool {
        match self.node.entry(name) {
            Entry::Vacant(entry) => {
                let mut by_container_name_map = IndexMap::new();
                by_container_name_map.insert(container_name, window);
                entry.insert(by_container_name_map);
                true
            }
            Entry::Occupied(mut container_name_to_window_map) => {
                match container_name_to_window_map.get_mut().entry(container_name) {
                    Entry::Vacant(entry) => {
                        entry.insert(window);
                        true
                    }
                    Entry::Occupied(mut entry) => {
                        if let Some(updated) = merge_fault_windows(entry.get(), window) {
                            entry.insert(updated);
                            return true;
                        }

                        false
                    }
                }
            }
        }
    }

    fn add_clock_fault(&mut self, offset: f64, window: FaultWindowBounds) -> bool {
        self.clock.push((offset, window));
        true
    }

    fn to_json(&self) -> Value {
        let mut result = Map::new();

        for entry in &self.network {
            result.insert(
                format!("network_{}", entry.0),
                json!({"vtime": (entry.1.start_vtime as f64) / TICKS_PER_SECOND}),
            );
        }

        for entry in &self.node {
            let mut node_fault_starts_by_container = Map::new();
            for entry in entry.1 {
                node_fault_starts_by_container.insert(
                    entry.0.to_string(),
                    json!((entry.1.start_vtime as f64) / TICKS_PER_SECOND),
                );
            }

            result.insert(
                format!("node_{}", entry.0),
                Value::Object(node_fault_starts_by_container),
            );
        }

        if !&self.clock.is_empty() {
            let mut offset_sum = 0f64;
            let mut max_clock_fault_start = 0u64;

            for entry in &self.clock {
                offset_sum += entry.0;
                max_clock_fault_start = max_clock_fault_start.max(entry.1.start_vtime);
            }

            result.insert("clock_skip".to_string(), json!({"cumulative_offset": offset_sum, "vtime": (max_clock_fault_start as f64) / TICKS_PER_SECOND}));
        }

        Value::Object(result)
    }
}

fn merge_fault_windows(
    incumbent: &FaultWindowBounds,
    new: FaultWindowBounds,
) -> Option<FaultWindowBounds> {
    if new.start_vtime.lt(&incumbent.start_vtime) {
        return Some(FaultWindowBounds {
            start_vtime: new.start_vtime,
            end_vtime: incumbent.end_vtime.and_then(|prev_expiry| {
                new.end_vtime.map(|new_expiry| new_expiry.max(prev_expiry))
            }),
        });
    }

    match incumbent.end_vtime {
        None => None,
        Some(prev_expiry) => match new.end_vtime {
            None => Some(FaultWindowBounds {
                start_vtime: incumbent.start_vtime,
                end_vtime: None,
            }),
            Some(new_expiry) => {
                if new_expiry.gt(&prev_expiry) {
                    return Some(FaultWindowBounds {
                        start_vtime: incumbent.start_vtime,
                        end_vtime: Some(new_expiry),
                    });
                }

                None
            }
        },
    }
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
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let stream = entry["source"]["stream"].as_str().unwrap_or("");

    if let Some(summary) = parse_assertion_summary(entry) {
        return RenderedEventEntry {
            input_hash,
            vtime,
            source: render_source(container, name, Some("assert")),
            output: render_assertion_summary(&summary),
        };
    }

    RenderedEventEntry {
        input_hash,
        vtime,
        source: render_source(container, name, (!stream.is_empty()).then_some(stream)),
        output: render_event_output(entry),
    }
}

fn render_event_output(entry: &Value) -> String {
    if let Some(rendered) = render_known_event(entry) {
        return rendered;
    }
    if let Some(output_text) = entry.get("output_text").and_then(Value::as_str) {
        return sanitize(output_text);
    }
    sanitize(&serde_json::to_string(entry).unwrap_or_default())
}

struct EventKind {
    source_name: &'static str,
    fields: &'static [&'static str],
}

const EVENT_KINDS: &[EventKind] = &[
    EventKind {
        source_name: "antithesis_test_composer",
        fields: &[
            "task_status",
            "command",
            "container_id",
            "command_return_code",
            "command_runtime",
            "additional_stderr",
            "added_task",
            "got_pid_back",
            "tasks_len",
            "weight",
            "weight_type",
        ],
    },
    EventKind {
        source_name: "fault_injector",
        fields: &[
            "fault.name",
            "fault.type",
            "fault.details.disruption_type",
            "fault.affected_nodes",
            "fault.max_duration",
        ],
    },
];

fn render_known_event(entry: &Value) -> Option<String> {
    let source_name = entry["source"]["name"].as_str()?;
    let kind = EVENT_KINDS
        .iter()
        .find(|kind| kind.source_name == source_name)?;

    let parts: Vec<String> = kind
        .fields
        .iter()
        .filter_map(|path| {
            let value = lookup_path(entry, path)?;
            let rendered = format_event_value(value)?;
            Some(format!("{path}={rendered}"))
        })
        .collect();

    (!parts.is_empty()).then(|| parts.join(" "))
}

fn lookup_path<'a>(entry: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(entry, |current, segment| current.get(segment))
}

fn format_event_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(sanitize(s)),
        Value::Array(items) => {
            let scalars: Option<Vec<String>> = items
                .iter()
                .map(|item| match item {
                    Value::Null => Some(String::new()),
                    Value::Bool(b) => Some(b.to_string()),
                    Value::Number(n) => Some(n.to_string()),
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            let scalars = scalars?;
            if scalars.is_empty() {
                return None;
            }
            Some(sanitize(&scalars.join(",")))
        }
        Value::Object(_) => None,
    }
}

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let assertion = entry.get("antithesis_assert")?;
    let payload = AssertionPayload::deserialize(assertion).ok()?;
    AssertionSummary::try_from(payload).ok()
}

fn render_source(container: &str, name: &str, stream: Option<&str>) -> String {
    let label = if !container.trim().is_empty() {
        sanitize(container)
    } else {
        sanitize(name.trim().strip_prefix("antithesis_").unwrap_or(name))
    };
    let stream = stream.map(sanitize).filter(|stream| !stream.is_empty());

    match (label.is_empty(), stream) {
        (false, Some(stream)) => format!("[{label}:{stream}]"),
        (false, None) => format!("[{label}]"),
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

fn render_runs_table(runs: &[RunSummary], width: usize) -> String {
    let headers = vec![
        "RUN ID".to_string(),
        "STATUS".to_string(),
        "CREATED".to_string(),
        "TEST NAME".to_string(),
        "DESCRIPTION".to_string(),
    ];

    let prepared: Vec<(Vec<String>, Option<String>)> = runs
        .iter()
        .map(|run| {
            let test_name = run.test_name().map(sanitize).unwrap_or_else(|| "-".into());
            let description = run.test_description().map(sanitize);
            let row = vec![
                sanitize(&run.run_id),
                status_label(run.status),
                relative_time(run.created_at),
                test_name,
                String::new(),
            ];
            (row, description)
        })
        .collect();

    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for (row, _) in &prepared {
        for (index, cell) in row.iter().enumerate().take(headers.len() - 1) {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }
    // Description gets whatever room remains. Two-space separators between
    // columns; reserve at least 8 chars for description so the column header
    // remains readable on narrow terminals.
    let fixed_width: usize =
        widths.iter().take(headers.len() - 1).sum::<usize>() + 2 * (headers.len() - 1);
    let desc_width = width.saturating_sub(fixed_width).max(8);
    widths[headers.len() - 1] = desc_width;

    let mut output = String::new();
    push_table_row(&mut output, &headers, &widths);

    for (mut row, description) in prepared {
        let description = description.unwrap_or_else(|| "-".into());
        row[4] = console::truncate_str(&description, desc_width, "…").into_owned();
        push_table_row(&mut output, &row, &widths);
    }

    output.trim_end().to_string()
}

fn render_table(headers: &[String], rows: &[Vec<String>]) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.chars().count())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
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
    let last = row.len().saturating_sub(1);
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            output.push_str("  ");
        }
        if index == last {
            output.push_str(cell);
        } else {
            output.push_str(&format!("{cell:<width$}", width = widths[index]));
        }
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
        assert_eq!(render_source("control", "", None), "[control]");
    }

    #[test]
    fn source_falls_back_to_name_when_container_empty() {
        assert_eq!(
            render_source("", "fault_injector", None),
            "[fault_injector]"
        );
    }

    #[test]
    fn source_strips_antithesis_prefix_from_name() {
        assert_eq!(
            render_source("", "antithesis_test_composer", None),
            "[test_composer]"
        );
    }

    #[test]
    fn source_prefers_container_over_name() {
        assert_eq!(render_source("client1", "python3.11", None), "[client1]");
    }

    #[test]
    fn source_combines_name_fallback_with_stream() {
        assert_eq!(
            render_source("", "antithesis_test_composer", Some("info")),
            "[test_composer:info]"
        );
    }

    #[test]
    fn renders_test_composer_event_with_name_fallback() {
        let entry = json!({
            "added_task": "parallel_driver_fetch",
            "tasks_len": "1",
            "source": {
                "name": "antithesis_test_composer",
                "pid": 974
            },
            "moment": {
                "input_hash": "5181922178177328213",
                "vtime": "315.41654103668407"
            }
        });

        assert_eq!(
            render_event_entry(&entry).source,
            "[test_composer]".to_string()
        );
    }

    #[test]
    fn renders_started_task_as_key_value_pairs() {
        let entry = json!({
            "command": "core/parallel_driver_fetch",
            "container_id": "d700ef3d05a263877d0d0c175f2954bdc8bc098faf501211b34bb20ba09f4435",
            "started_task": "d700ef3d_parallel_driver_fetch",
            "task_status": "started",
            "tasks_len": "1",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "1.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "task_status=started command=core/parallel_driver_fetch container_id=d700ef3d05a263877d0d0c175f2954bdc8bc098faf501211b34bb20ba09f4435 tasks_len=1"
        );
    }

    #[test]
    fn renders_finished_task_omitting_empty_stderr() {
        let entry = json!({
            "additional_stderr": "",
            "additional_stdout": "",
            "command": "core/parallel_driver_fetch",
            "command_return_code": "0",
            "command_runtime": "2.1254637241363525",
            "finished_task": "abc",
            "task_status": "finished",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "2.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "task_status=finished command=core/parallel_driver_fetch command_return_code=0 command_runtime=2.1254637241363525"
        );
    }

    #[test]
    fn renders_weight_event_as_key_value_pairs() {
        let entry = json!({
            "command": "abc_/opt/antithesis/test/v1/core/parallel_driver_fetch",
            "weight": "0.157917609630634",
            "weight_type": "masked_for_step",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"vtime": "7.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "command=abc_/opt/antithesis/test/v1/core/parallel_driver_fetch weight=0.157917609630634 weight_type=masked_for_step"
        );
    }

    #[test]
    fn renders_fault_event_with_nested_paths_and_array() {
        let entry = json!({
            "fault": {
                "name": "clog",
                "type": "network",
                "details": {"disruption_type": "Stopped"},
                "affected_nodes": ["client2", "setup"],
                "max_duration": 0.267319258
            },
            "source": {"name": "fault_injector"},
            "moment": {"vtime": "3.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "fault.name=clog fault.type=network fault.details.disruption_type=Stopped fault.affected_nodes=client2,setup fault.max_duration=0.267319258"
        );
    }

    #[test]
    fn renders_fault_event_with_empty_affected_nodes() {
        let entry = json!({
            "fault": {
                "name": "clog",
                "type": "network",
                "details": {"disruption_type": "Stopped"},
                "affected_nodes": [],
                "max_duration": 0.259267334
            },
            "source": {"name": "fault_injector"},
            "moment": {"vtime": "4.0"}
        });

        assert_eq!(
            render_event_entry(&entry).output,
            "fault.name=clog fault.type=network fault.details.disruption_type=Stopped fault.max_duration=0.259267334"
        );
    }

    #[test]
    fn falls_back_to_json_dump_for_unknown_source() {
        let entry = json!({
            "antithesis_sdk": {"sdk_version": "0.2.0"},
            "source": {"container": "client1", "name": "python3.11"},
            "moment": {"vtime": "6.0"}
        });

        // source.name is not in EVENT_KINDS; no output_text; fall back to JSON.
        let output = render_event_entry(&entry).output;
        assert!(output.starts_with('{'), "expected JSON dump, got: {output}");
        assert!(output.contains("antithesis_sdk"));
    }

    fn event(input_hash: &str, vtime: &str) -> Event {
        Event {
            moment: Moment {
                input_hash: input_hash.to_string(),
                vtime: vtime.to_string(),
            },
        }
    }

    fn event_property(
        name: &str,
        status: PropertyStatus,
        group: Option<&str>,
        examples: Vec<Event>,
        counterexamples: Vec<Event>,
    ) -> Property {
        let ex_count = examples.len() as u32;
        let cex_count = counterexamples.len() as u32;
        Property::EventProperty(EventProperty {
            counterexample_count: Some(cex_count),
            counterexamples,
            description: None,
            example_count: Some(ex_count),
            examples,
            group: group.map(str::to_string),
            is_event: true,
            is_group: None,
            name: name.to_string(),
            status,
        })
    }

    fn non_event_property(
        name: &str,
        status: PropertyStatus,
        examples: Vec<Value>,
        counterexamples: Vec<Value>,
    ) -> Property {
        let ex_count = examples.len() as u32;
        let cex_count = counterexamples.len() as u32;
        Property::NonEventProperty(NonEventProperty {
            counterexample_count: Some(cex_count),
            counterexamples,
            description: None,
            example_count: Some(ex_count),
            examples,
            group: None,
            is_event: false,
            is_group: None,
            name: name.to_string(),
            status,
        })
    }

    #[test]
    fn properties_table_groups_status_word_and_totals() {
        let properties = vec![
            event_property(
                "Counter value stays below limit",
                PropertyStatus::Failing,
                Some("Safety"),
                vec![event("-300", "15.0")],
                vec![event("-100", "5.0"), event("-200", "10.0")],
            ),
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![event("-400", "1.0")],
                vec![],
            ),
        ];

        let table = render_properties_table(&properties);
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[0].contains("STATUS"));
        assert!(lines[0].contains("GROUP"));
        assert!(lines[0].contains("NAME"));
        assert!(lines[0].contains("EXAMPLES"));

        // Two property rows. Counter property has 1 example + 2 counterexamples = 3 total.
        let counter_row = lines.iter().find(|l| l.contains("Counter value")).unwrap();
        assert!(counter_row.contains("failing"));
        assert!(counter_row.contains("Safety"));
        assert!(counter_row.contains("3"));

        let setup_row = lines
            .iter()
            .find(|l| l.contains("Setup completes"))
            .unwrap();
        assert!(setup_row.contains("passing"));
        assert!(setup_row.contains("1"));
    }

    #[test]
    fn resolve_property_prefers_exact_match() {
        let properties = vec![
            event_property("Setup ran", PropertyStatus::Passing, None, vec![], vec![]),
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![],
                vec![],
            ),
        ];
        let resolved = resolve_property(&properties, "Setup ran").unwrap();
        assert!(matches!(resolved, Resolved::Exact(_)));
    }

    #[test]
    fn resolve_property_exact_match_ignores_case() {
        let properties = vec![
            event_property("Setup ran", PropertyStatus::Passing, None, vec![], vec![]),
            event_property(
                "Counter limit",
                PropertyStatus::Passing,
                None,
                vec![],
                vec![],
            ),
        ];
        // Differs only by case: still an exact hit, not a fuzzy "closest" match.
        let resolved = resolve_property(&properties, "setup RAN").unwrap();
        match resolved {
            Resolved::Exact(p) => assert_eq!(p.name(), "Setup ran"),
            other => panic!("expected exact match, got {other:?}"),
        }
    }

    #[test]
    fn resolve_property_falls_back_to_single_substring_match() {
        let properties = vec![
            event_property("Setup ran", PropertyStatus::Passing, None, vec![], vec![]),
            event_property(
                "Counter limit",
                PropertyStatus::Passing,
                None,
                vec![],
                vec![],
            ),
        ];
        let resolved = resolve_property(&properties, "counter").unwrap();
        match resolved {
            Resolved::Fuzzy(p) => assert_eq!(p.name(), "Counter limit"),
            _ => panic!("expected fuzzy match"),
        }
    }

    #[test]
    fn resolve_property_errors_on_multiple_substring_matches() {
        let properties = vec![
            event_property("Setup ran", PropertyStatus::Passing, None, vec![], vec![]),
            event_property(
                "Setup completes",
                PropertyStatus::Passing,
                None,
                vec![],
                vec![],
            ),
        ];
        let err = resolve_property(&properties, "setup").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("did you mean"));
        assert!(msg.contains("Setup ran"));
        assert!(msg.contains("Setup completes"));
    }

    #[test]
    fn render_event_property_examples_uses_status_column() {
        let property = event_property(
            "Counter",
            PropertyStatus::Failing,
            None,
            vec![event("ex", "2.0")],
            vec![event("cex", "1.0")],
        );
        let out = render_property_examples(&property);
        assert!(out.contains("STATUS"));
        assert!(out.contains("HASH"));
        assert!(out.contains("VTIME"));
        assert!(out.contains("passing"));
        assert!(out.contains("failing"));
    }

    #[test]
    fn render_non_event_property_examples_pretty_prints_objects() {
        let property = non_event_property(
            "Determinator Max Memory",
            PropertyStatus::Passing,
            vec![json!({
                "maximum_used_bytes": 17012928512u64,
                "percent_used": "0.04"
            })],
            vec![],
        );
        let out = render_property_examples(&property);
        // Summary cell collapses the object…
        assert!(out.contains("{2 fields}"));
        // …with the full pretty body shown below.
        assert!(out.contains("row 1 details:"));
        assert!(out.contains("maximum_used_bytes"));
    }

    #[test]
    fn format_log_line_renders_json_record_with_stripped_envelope() {
        let entry = json!({
            "moment": {"input_hash": "6409410329507290816", "vtime": "9.093"},
            "IPT_bytes_out": 126952,
            "source": {"name": "fault_injector", "pid": 924},
            "info": {"details": {"started": true}, "message": "status"}
        });
        assert_eq!(
            format_log_line(&entry),
            "[         9.093] [      fault_injector] [   ]  - {\"info\":{\"details\":{\"started\":true},\"message\":\"status\"}}"
        );
    }

    #[test]
    fn format_log_line_renders_text_record_with_inf_stream() {
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "15.174"},
            "source": {"container": "bank/first_setup.sh", "name": "bank/first_setup.sh", "stream": "info"},
            "output_text": "NbmXgEki  INFO main lsm_tree::tree::ingest: Finished ingestion writer"
        });
        assert_eq!(
            format_log_line(&entry),
            "[        15.174] [ bank/first_setup.sh] [inf] NbmXgEki  INFO main lsm_tree::tree::ingest: Finished ingestion writer"
        );
    }

    #[test]
    fn format_log_line_preserves_ansi_in_output_text() {
        let entry = json!({
            "moment": {"input_hash": "1", "vtime": "14.118"},
            "source": {"name": "setup", "stream": "error"},
            "output_text": "\x1B[4m>>>> hello"
        });
        let rendered = format_log_line(&entry);
        assert!(rendered.contains("\x1B[4m>>>> hello"));
        assert!(rendered.contains("[err]"));
    }

    #[test]
    fn format_log_line_overflows_source_column_when_too_long() {
        let entry = json!({
            "moment": {"vtime": "14.284"},
            "source": {
                "container": "antithesis/pods/client/sdk.jsonl",
                "name": "antithesis/pods/client/sdk.jsonl"
            },
            "antithesis_setup": {"details": null, "status": "complete"}
        });
        assert_eq!(
            format_log_line(&entry),
            "[        14.284] [antithesis/pods/client/sdk.jsonl] [   ]  - {\"antithesis_setup\":{\"details\":null,\"status\":\"complete\"}}"
        );
    }

    #[test]
    fn render_event_property_examples_marks_unreachable_when_empty() {
        let property = event_property("Maybe ran", PropertyStatus::Passing, None, vec![], vec![]);
        let out = render_property_examples(&property);
        assert!(out.contains("unreachable"));
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
    #[test]
    fn ansi_sgr() {
        assert_eq!(strip_ansi("\x1b[1mbold\x1b[0m"), "bold");
        assert_eq!(strip_ansi("\x1b[38;5;196mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[38;2;255;0;0mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[1;31;42mtext\x1b[0m"), "text");
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T08:19:54Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mfoobar\x1b[0m\x1b[2m:\x1b[0m ready"
            ),
            "2026-04-03T08:19:54Z  INFO foobar: ready"
        );
    }

    #[test]
    fn ansi_csi_non_sgr() {
        assert_eq!(strip_ansi("left\x1b[2Aright"), "leftright");
        assert_eq!(strip_ansi("text\x1b[2K"), "text");
        assert_eq!(strip_ansi("\x1b[?25hvisible"), "visible");
        assert_eq!(strip_ansi("\x1b[?25l hidden"), " hidden");
    }

    #[test]
    fn ansi_osc() {
        assert_eq!(
            strip_ansi("\x1b]0;my window title\x07text after"),
            "text after"
        );
        assert_eq!(strip_ansi("\x1b]0;my title\x1b\\text after"), "text after");
    }

    #[test]
    fn ansi_two_byte() {
        assert_eq!(strip_ansi("\x1bcafter reset"), "after reset");
        assert_eq!(strip_ansi("before\x1b7after"), "beforeafter");
    }

    #[test]
    fn ansi_passthrough() {
        let cases = [
            "no escapes here",
            r#"{"key": "value", "nested": {"a": [1,2,3]}}"#,
            r#"{"url": "http://example.com/path?q=1&r=2", "count": 42}"#,
            r#"Options { address: Some(0.0.0.0:3307), deployment: "mydb", mode: Standalone }"#,
            r#"Config { inner: Inner { values: [1, 2, 3] }, name: "test" }"#,
            "[2026-04-03] [INFO] [main] started",
            r#"path: "/nix/store/abc-pkg/bin/cmd""#,
            r#"{"msg": "he said \"hello\""}"#,
        ];
        for c in cases {
            assert_eq!(strip_ansi(c), c, "passthrough failed: {c:?}");
        }
    }

    #[test]
    fn ansi_mixed() {
        assert_eq!(
            strip_ansi("\x1b[2m{\"key\": \"value\"}\x1b[0m"),
            r#"{"key": "value"}"#
        );
        assert_eq!(
            strip_ansi("\x1b[3mOptions { mode: Standalone }\x1b[0m"),
            "Options { mode: Standalone }"
        );
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T00:00:00Z\x1b[0m \x1b[32m INFO\x1b[0m request completed {\"status\": 200, \"latency_ms\": 42}"
            ),
            r#"2026-04-03T00:00:00Z  INFO request completed {"status": 200, "latency_ms": 42}"#
        );
    }

    #[test]
    fn tracks_which_faults_are_active_based_on_vtime_and_max_duration() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // No faults yet
        assert_eq!(
            transformer.try_transform("{\"foo\":\"bar\"}"),
            Some("{\"foo\":\"bar\",\"active_faults\":{}}".to_string())
        );

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 1u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"],
                    "max_duration": 10
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":4294967296},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"],\"max_duration\":10},\"vtime_seconds\":1.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Another log message; should retain active window state since the log message had no timestamp
        assert_eq!(
            transformer.try_transform("{\"foo\":\"bar\"}"),
            Some(
                "{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}"
                    .to_string()
            )
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 2u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"],
                    "max_duration": 9
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":8589934592},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"],\"max_duration\":9},\"vtime_seconds\":2.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Another non-fault injector message; should retain state
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 11u64 << 32
                },
                "foo": "bar"
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":47244640256},\"foo\":\"bar\",\"vtime_seconds\":11.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Expire both windows
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": {
                        "_vtime_ticks": (11u64 << 32) + 1
                    },
                    "foo": "bar"
                })
            )),
            Some(
                "{\"moment\":{\"_vtime_ticks\":47244640257},\"foo\":\"bar\",\"vtime_seconds\":11.00000000023283,\"active_faults\":{}}"
                    .to_string()
            )
        );
    }

    #[test]
    fn restore_closes_network_faults() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 1u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":4294967296},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"]},\"vtime_seconds\":1.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 2u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":8589934592},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"]},\"vtime_seconds\":2.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 3u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["b", "c"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":12884901888},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"clog\",\"type\":\"network\",\"affected_nodes\":[\"b\",\"c\"]},\"vtime_seconds\":3.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Verify that state is retained for a non-control log message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"foo": "bar"}))),
            Some("{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Send a network restore message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "restore",
                    "type": "network"
                }
            }))),
            Some("{\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"restore\",\"type\":\"network\"},\"active_faults\":{\"node_throttle\":{\"c\":2.0}}}".to_string())
        );
    }

    #[test]
    fn fault_injector_pause_clears_network_and_node_faults() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 1u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["a", "b"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":4294967296},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"partition\",\"type\":\"network\",\"affected_nodes\":[\"a\",\"b\"]},\"vtime_seconds\":1.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 2u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["c"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":8589934592},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"throttle\",\"type\":\"node\",\"affected_nodes\":[\"c\"]},\"vtime_seconds\":2.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 3u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["b", "c"]
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":12884901888},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"clog\",\"type\":\"network\",\"affected_nodes\":[\"b\",\"c\"]},\"vtime_seconds\":3.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0}}}".to_string())
        );

        // Open a clock fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 4u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 10.5
                    }
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":17179869184},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":10.5}},\"vtime_seconds\":4.0,\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0},\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );

        // Verify that state is retained for a non-control log message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"foo": "bar"}))),
            Some("{\"foo\":\"bar\",\"active_faults\":{\"network_partition\":{\"vtime\":1.0},\"network_clog\":{\"vtime\":3.0},\"node_throttle\":{\"c\":2.0},\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );

        // Send a fault injector pause message
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "source": {
                    "name": "fault_injector"
                },
                "info": {
                    "message": "status",
                    "details": {
                        "paused": true
                    }
                }
            }))),
            Some("{\"source\":{\"name\":\"fault_injector\"},\"info\":{\"message\":\"status\",\"details\":{\"paused\":true}},\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":4.0}}}".to_string())
        );
    }

    #[test]
    fn clock_offsets_are_combined() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a network partition fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 1u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 10.5
                    }
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":4294967296},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":10.5}},\"vtime_seconds\":1.0,\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.5,\"vtime\":1.0}}}".to_string())
        );

        // Open a node throttled fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 2u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": 0.1
                    }
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":8589934592},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":0.1}},\"vtime_seconds\":2.0,\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":10.6,\"vtime\":2.0}}}".to_string())
        );

        // Open a network clog fault window
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": {
                    "_vtime_ticks": 3u64 << 32
                },
                "source": {
                    "name": "fault_injector"
                },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": {
                        "offset": -2.3
                    }
                }
            }))),
            Some("{\"moment\":{\"_vtime_ticks\":12884901888},\"source\":{\"name\":\"fault_injector\"},\"fault\":{\"name\":\"skip\",\"type\":\"clock\",\"details\":{\"offset\":-2.3}},\"vtime_seconds\":3.0,\"active_faults\":{\"clock_skip\":{\"cumulative_offset\":8.3,\"vtime\":3.0}}}".to_string())
        );
    }

    #[test]
    fn empty_affected_nodes_does_not_open_network_window() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a real window first
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["node-1"],
                    "max_duration": 100
                }
            })
        ));

        // Empty affected_nodes: network and node faults are only considered active if at least one node is affected,
        // so no new window is pushed and the existing one is unchanged.
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 2u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "clog",
                        "type": "network",
                        "affected_nodes": []
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":8589934592},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"clog","type":"network","affected_nodes":[]},"#,
                    r#""vtime_seconds":2.0,"active_faults":{"network_clog":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    #[test]
    fn missing_affected_nodes_does_not_open_network_window() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a real window first
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        // No affected_nodes field at all: same result — no new window
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 2u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "partition",
                        "type": "network"
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":8589934592},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"partition","type":"network"},"#,
                    r#""vtime_seconds":2.0,"active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: untracked fault names produce no window
    // -----------------------------------------------------------------------

    #[test]
    fn untracked_fault_names_produce_empty_active_faults() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 1u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": { "name": "kill" }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":4294967296},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"kill"},"#,
                    r#""vtime_seconds":1.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 2u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": { "name": "stop" }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":8589934592},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"stop"},"#,
                    r#""vtime_seconds":2.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    #[test]
    fn restore_after_only_untracked_faults_is_noop() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": { "name": "kill" }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 2u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "restore",
                        "type": "network",
                        "affected_nodes": ["ALL"]
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":8589934592},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"restore","type":"network","affected_nodes":["ALL"]},"#,
                    r#""vtime_seconds":2.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: non-fault_injector sources do not open windows
    // -----------------------------------------------------------------------

    #[test]
    fn fault_fields_from_non_fault_injector_source_are_ignored() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "some_other_source" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"]
                }
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":4294967296},"source":{"name":"some_other_source"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"]},"#,
                r#""vtime_seconds":1.0,"active_faults":{}}"#
            ).to_string())
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: event without _vtime_ticks still gets active_faults
    // (and does not get vtime_seconds)
    // -----------------------------------------------------------------------

    #[test]
    fn event_without_vtime_ticks_still_gets_active_faults() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Open a partition window at a known vtime
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"]
                }
            })
        ));

        // Event with no moment at all: no expiry check, no vtime_seconds, but active_faults injected
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({"output_text": "no moment here"}))),
            Some(
                concat!(
                    r#"{"output_text":"no moment here","#,
                    r#""active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: natural expiration — boundary semantics
    //
    // is_expired uses strict less-than: end_vtime < latest_vtime.
    // So at exactly end_vtime ticks the window is still active; it expires
    // only when the next message arrives with a strictly greater vtime.
    // -----------------------------------------------------------------------

    #[test]
    fn fault_window_active_at_exact_end_vtime_expires_one_tick_later() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // partition at 5<<32, max_duration=5s → end_vtime = (5+5)<<32 = 10<<32
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 5u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            })
        ));

        // At exactly end_vtime (10<<32): window is still active (end < latest is false when equal)
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 10u64 << 32 },
                    "output_text": "at exact end"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":42949672960},"output_text":"at exact end","#,
                    r#""vtime_seconds":10.0,"active_faults":{"network_partition":{"vtime":5.0}}}"#
                )
                .to_string()
            )
        );

        // One tick past end_vtime: now expired
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": (10u64 << 32) + 1 },
                    "output_text": "just past end"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":42949672961},"output_text":"just past end","#,
                    r#""vtime_seconds":10.00000000023283,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: partition without max_duration never expires naturally
    // -----------------------------------------------------------------------

    #[test]
    fn partition_without_max_duration_never_expires() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"]
                    // no max_duration → end_vtime = None → is_expired always false
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 1000u64 << 32 },
                    "output_text": "much later"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":4294967296000},"output_text":"much later","#,
                    r#""vtime_seconds":1000.0,"active_faults":{"network_partition":{"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: restore before natural expiration clears network faults
    // -----------------------------------------------------------------------

    #[test]
    fn restore_before_natural_expiration_clears_network_faults() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 5u64 << 32 },
                    "source": { "name": "fault_injector" },
                    "fault": {
                        "name": "restore",
                        "type": "network",
                        "affected_nodes": ["ALL"]
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":21474836480},"source":{"name":"fault_injector"},"#,
                    r#""fault":{"name":"restore","type":"network","affected_nodes":["ALL"]},"#,
                    r#""vtime_seconds":5.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: partition and clog expire independently
    // -----------------------------------------------------------------------

    #[test]
    fn partition_and_clog_expire_independently() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Partition at vtime 5, max_duration=20 → expires after 25<<32
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 5u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 20
                }
            })
        ));

        // Clog at vtime 10, max_duration=3 → expires after 13<<32
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 10u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "clog",
                    "type": "network",
                    "affected_nodes": ["A"],
                    "max_duration": 3
                }
            })
        ));

        // At vtime 14: clog's end_vtime (13<<32) < 14<<32, so it expires; partition still active
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 14u64 << 32 },
                    "output_text": "clog expired, partition still active"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":60129542144},"#,
                    r#""output_text":"clog expired, partition still active","#,
                    r#""vtime_seconds":14.0,"active_faults":{"network_partition":{"vtime":5.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: non-overlapping windows — first expires, new one
    // starts fresh with the new start_vtime
    // -----------------------------------------------------------------------

    #[test]
    fn non_overlapping_windows_start_fresh_after_expiry() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // First window: vtime 1, max_duration=3 → expires after 4<<32
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 3
                }
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":4294967296},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":3},"#,
                r#""vtime_seconds":1.0,"active_faults":{"network_partition":{"vtime":1.0}}}"#
            ).to_string())
        );

        // Second window at vtime 5, after the first has expired (5<<32 > 4<<32):
        // the old window is pruned before the new one is pushed, so the snapshot
        // reflects only the new window's start_vtime.
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 5u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 3
                }
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":21474836480},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":3},"#,
                r#""vtime_seconds":5.0,"active_faults":{"network_partition":{"vtime":5.0}}}"#
            ).to_string())
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: overlapping same-name windows — active_fault_dictionary
    // reports the earliest start_vtime among all live windows
    // -----------------------------------------------------------------------

    #[test]
    fn overlapping_windows_report_earliest_start_vtime() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // First partition at vtime 10, max_duration=5 → expires after 15<<32
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 10u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            })
        ));

        // Second partition at vtime 14 (overlapping), max_duration=5 → expires after 19<<32
        // Both windows are alive; active_fault_dictionary picks the min start_vtime (10)
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 14u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 5
                }
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":60129542144},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"partition","type":"network","affected_nodes":["ALL"],"max_duration":5},"#,
                r#""vtime_seconds":14.0,"active_faults":{"network_partition":{"vtime":10.0}}}"#
            ).to_string())
        );

        // At vtime 16: first window expired (15<<32 < 16<<32), second still alive (19<<32 not < 16<<32)
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 16u64 << 32 },
                "output_text": "after first window expired"
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":68719476736},"output_text":"after first window expired","#,
                r#""vtime_seconds":16.0,"active_faults":{"network_partition":{"vtime":10.0}}}"#
            ).to_string())
        );

        // At vtime 20: second window also expired (19<<32 < 20<<32)
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 20u64 << 32 },
                    "output_text": "after both expired"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":85899345920},"output_text":"after both expired","#,
                    r#""vtime_seconds":20.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: pause preserves clock windows
    // -----------------------------------------------------------------------

    #[test]
    fn fault_injector_pause_preserves_clock_windows() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "skip",
                    "type": "clock",
                    "details": { "offset": 10.0 }
                }
            })
        ));

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 2u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "partition",
                    "type": "network",
                    "affected_nodes": ["ALL"],
                    "max_duration": 100
                }
            })
        ));

        // Pause clears network and node windows; clock survives
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "source": { "name": "fault_injector" },
                    "info": {
                        "message": "status",
                        "details": { "paused": true }
                    }
                })
            )),
            Some(
                concat!(
                    r#"{"source":{"name":"fault_injector"},"#,
                    r#""info":{"message":"status","details":{"paused":true}},"#,
                    r#""active_faults":{"clock_skip":{"cumulative_offset":10.0,"vtime":1.0}}}"#
                )
                .to_string()
            )
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: multiple node containers tracked simultaneously
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_containers_paused_simultaneously() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "_vtime_ticks": 1u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "pause",
                    "type": "node",
                    "affected_nodes": ["A"],
                    "max_duration": 100
                }
            })
        ));

        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 2u64 << 32 },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "pause",
                    "type": "node",
                    "affected_nodes": ["B"],
                    "max_duration": 100
                }
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":8589934592},"source":{"name":"fault_injector"},"#,
                r#""fault":{"name":"pause","type":"node","affected_nodes":["B"],"max_duration":100},"#,
                r#""vtime_seconds":2.0,"active_faults":{"node_pause":{"A":1.0,"B":2.0}}}"#
            ).to_string())
        );
    }

    // -----------------------------------------------------------------------
    // active_faults: node fault expires via max_duration
    // -----------------------------------------------------------------------

    #[test]
    fn node_fault_expires_via_max_duration() {
        let mut transformer = FaultAnnotator {
            active_fault_windows: ActiveFaultWindows::new(),
            active_faults: json!({}),
        };

        // Throttle C at vtime 1, max_duration=5 → expires after 6<<32
        transformer.try_transform(&format!(
            "{}",
            json!({
                "moment": { "vtime": "1" },
                "source": { "name": "fault_injector" },
                "fault": {
                    "name": "throttle",
                    "type": "node",
                    "affected_nodes": ["C"],
                    "max_duration": 5
                }
            })
        ));

        // Mid-window at vtime 3: still active
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "vtime": "3.0" },
                    "output_text": "mid-window"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"vtime":"3.0"},"output_text":"mid-window","#,
                    r#""vtime_seconds":3.0,"active_faults":{"node_throttle":{"C":1.0}}}"#
                )
                .to_string()
            )
        );

        // After expiry at vtime 7 (6<<32 < 7<<32): empty
        assert_eq!(
            transformer.try_transform(&format!(
                "{}",
                json!({
                    "moment": { "_vtime_ticks": 7u64 << 32 },
                    "output_text": "after expiry"
                })
            )),
            Some(
                concat!(
                    r#"{"moment":{"_vtime_ticks":30064771072},"output_text":"after expiry","#,
                    r#""vtime_seconds":7.0,"active_faults":{}}"#
                )
                .to_string()
            )
        );
    }

    #[test]
    fn status_label_covers_every_variant() {
        assert_eq!(status_label(RunStatus::Completed), "completed");
        assert_eq!(status_label(RunStatus::Incomplete), "incomplete");
        assert_eq!(status_label(RunStatus::InProgress), "in_progress");
        assert_eq!(status_label(RunStatus::Starting), "starting");
        assert_eq!(status_label(RunStatus::Cancelled), "cancelled");
        assert_eq!(status_label(RunStatus::Unknown), "unknown");
    }

    fn summary(
        run_id: &str,
        status: RunStatus,
        created: &str,
        launcher: &str,
        test_name: Option<&str>,
        description: Option<&str>,
    ) -> RunSummary {
        let parameters = if test_name.is_some() || description.is_some() {
            let mut extra = std::collections::HashMap::new();
            if let Some(name) = test_name {
                extra.insert("antithesis.test_name".to_string(), name.to_string());
            }
            Some(crate::api::RunParams {
                antithesis_config_image: None,
                antithesis_description: description.map(str::to_string),
                antithesis_duration: None,
                antithesis_images: None,
                antithesis_is_ephemeral: None,
                antithesis_report_recipients: None,
                antithesis_source: None,
                extra,
            })
        } else {
            None
        };
        RunSummary {
            run_id: run_id.to_string(),
            status,
            created_at: created.parse().unwrap(),
            started_at: None,
            completed_at: None,
            launcher: launcher.to_string(),
            creator: None,
            description: None,
            parameters,
            links: None,
        }
    }

    #[test]
    fn runs_table_default_view_shows_truncated_description() {
        // Use a fixed past time so relative_time produces a stable-ish value.
        let runs = vec![summary(
            "abc-54-1",
            RunStatus::Completed,
            "2024-01-01T00:00:00Z",
            "basic_test",
            Some("snouty-empty-tt"),
            Some("issue #91: probe Antithesis behavior with empty test template dir"),
        )];

        let table = render_runs_table(&runs, 80);
        let lines: Vec<&str> = table.lines().collect();

        assert!(lines[0].contains("RUN ID"));
        assert!(lines[0].contains("STATUS"));
        assert!(lines[0].contains("CREATED"));
        assert!(lines[0].contains("TEST NAME"));
        assert!(lines[0].contains("DESCRIPTION"));
        assert!(!lines[0].contains("LAUNCHER"));

        assert!(lines[1].contains("abc-54-1"));
        assert!(lines[1].contains("completed"));
        assert!(lines[1].contains("snouty-empty-tt"));
        // Description is truncated with an ellipsis on a narrow terminal.
        assert!(lines[1].contains('…'));
    }

    #[test]
    fn runs_long_view_renders_aligned_key_value_block() {
        let runs = vec![
            summary(
                "abc-54-1",
                RunStatus::Completed,
                "2024-01-01T00:00:00Z",
                "basic_test",
                Some("snouty-empty-tt"),
                Some("full description goes here"),
            ),
            summary(
                "def-54-2",
                RunStatus::Incomplete,
                "2024-01-02T00:00:00Z",
                "spanner",
                None,
                None,
            ),
        ];

        let out = render_runs_long(&runs);
        // No table header — each field sits on its own aligned line. Labels are
        // padded to the widest label *within each block*, so the first block
        // (which has a "Description" label) is wider than the second.
        assert!(!out.contains("RUN ID  "));
        assert!(out.contains("Run ID       abc-54-1"));
        assert!(out.contains("Status       completed"));
        assert!(out.contains("Test Name    snouty-empty-tt"));
        assert!(out.contains("Description  full description goes here"));
        // Second run omits the empty Test Name / Description fields.
        assert!(out.contains("def-54-2"));
        assert!(out.contains("incomplete"));
        // A blank line separates the two run blocks.
        assert!(out.contains("\n\n"));
    }

    #[test]
    fn event_matches_anding_of_multiple_needles() {
        let entry = json!({
            "source": {"name": "fault_injector"},
            "moment": {"vtime": "1.0"},
            "info": {"message": "network partition started"}
        });
        let line = entry.to_string();

        let needles = vec!["network".to_string(), "partition".to_string()];
        let filters = EventFilters {
            matches: &needles,
            sources: &[],
            stream: None,
            vtime_min: None,
            vtime_max: None,
        };
        assert!(event_matches_filters(&entry, &line, filters));

        let missing = vec!["network".to_string(), "missing".to_string()];
        let filters = EventFilters {
            matches: &missing,
            ..filters
        };
        assert!(!event_matches_filters(&entry, &line, filters));
    }

    #[test]
    fn event_filters_by_source_and_stream_and_vtime() {
        let entry = json!({
            "source": {"name": "control", "container": "control", "stream": "stderr"},
            "moment": {"vtime": "12.5"},
            "output_text": "boom"
        });
        let line = entry.to_string();

        let needles = vec!["boom".to_string()];
        let sources = vec!["control".to_string()];
        let filters = EventFilters {
            matches: &needles,
            sources: &sources,
            stream: Some(Stream::Stderr),
            vtime_min: Some(10.0),
            vtime_max: Some(15.0),
        };
        assert!(event_matches_filters(&entry, &line, filters));

        let wrong_source = vec!["other".to_string()];
        let filters = EventFilters {
            sources: &wrong_source,
            ..filters
        };
        assert!(!event_matches_filters(&entry, &line, filters));
    }

    #[test]
    fn runs_table_renders_dashes_when_test_name_and_description_missing() {
        let runs = vec![summary(
            "abc-54-1",
            RunStatus::Incomplete,
            "2024-01-01T00:00:00Z",
            "basic_test",
            None,
            None,
        )];
        let table = render_runs_table(&runs, 100);
        let lines: Vec<&str> = table.lines().collect();
        assert!(lines[1].contains("incomplete"));
        // Placeholder dashes stand in for the missing test name and description.
        assert!(
            lines[1].contains("-  ") || lines[1].trim_end().ends_with('-'),
            "expected dash placeholders, got: {}",
            lines[1]
        );
    }
}
