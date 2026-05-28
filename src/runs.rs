use std::path::Path;
use std::{io::Write, sync::OnceLock};

use color_eyre::eyre::{Result, eyre};
use futures_util::{StreamExt, TryStreamExt};
use indexmap::IndexMap;
use indexmap::map::Entry;
use log::debug;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::api::{
    AntithesisApi, Property, PropertyStatus, RunDetail, RunStatus, RunSummary, RunsFilterOptions,
};
#[cfg(test)]
use crate::api::{Event, EventProperty, Moment, NonEventProperty};
use crate::cli::{RunsCommands, RunsListArgs};

pub async fn cmd_runs(command: Option<RunsCommands>, json: bool, verbose: bool) -> Result<()> {
    match command {
        None => cmd_runs_list(RunsListArgs::default(), json, verbose).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args, json, verbose).await,
        Some(RunsCommands::Show { run_id }) => cmd_runs_show(&run_id, json, verbose).await,
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
        Some(RunsCommands::Events { run_id, query }) => {
            cmd_runs_events(&run_id, &query, json, verbose).await
        }
    }
}

async fn cmd_runs_list(args: RunsListArgs, json: bool, verbose: bool) -> Result<()> {
    debug!("listing runs");

    let api = AntithesisApi::from_env(verbose)?;

    let status = args
        .status
        .as_deref()
        .map(|s| s.parse::<RunStatus>())
        .transpose()
        .map_err(|_| {
            eyre!(
                "invalid status: '{}'\nvalid values: starting, in_progress, completed, cancelled, incomplete, unknown",
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

    // Server returns runs newest-first; .take(limit) short-circuits pagination
    // so we don't materialise the entire run history just to drop most of it.
    let mut runs: Vec<RunSummary> = api
        .stream_runs_filtered(&opts)
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

    println!("{}", render_runs_table(&runs));
    Ok(())
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
        property_status_rank(a.status())
            .cmp(&property_status_rank(b.status()))
            .then(a.name().cmp(b.name()))
    });

    if json {
        for property in &properties {
            println!("{}", serde_json::to_string(property)?);
        }
    } else if properties.is_empty() {
        println!("No properties found.");
    } else {
        let event_rows = flatten_property_events(&properties);
        let non_event_rows = flatten_non_event_property_values(&properties);

        let mut sections = Vec::new();
        if !event_rows.is_empty() {
            sections.push(render_property_events_table(&event_rows));
        }
        if !non_event_rows.is_empty() {
            sections.push(render_property_values_table(&non_event_rows));
        }
        println!("{}", sections.join("\n\n"));
    }

    Ok(())
}

fn print_run_detail(run: &RunDetail) {
    let mut rows: Vec<(&str, String)> = vec![
        ("Run ID", run.run_id.clone()),
        ("Status", run.status.to_string()),
    ];

    rows.push(("Created", run.created_at.to_rfc3339()));

    if let Some(ref t) = run.started_at {
        rows.push(("Started", t.to_rfc3339()));
    }
    if let Some(ref t) = run.completed_at {
        rows.push(("Completed", t.to_rfc3339()));
    }

    rows.push(("Launcher", run.launcher.clone()));

    if let Some(ref moment) = run.failure_moment {
        rows.push(("Failure VTime", moment.vtime.clone()));
        rows.push(("Failure Hash", moment.input_hash.clone()));
    }

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

struct LogOutputOptions {
    json: bool,
    verbose: bool,
    annotate_faults: bool,
}

async fn cmd_runs_build_logs(run_id: &str, json: bool, verbose: bool) -> Result<()> {
    debug!("streaming build logs for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let stream = api.get_run_build_logs(run_id).await?.into_inner();
    let mut stdout = std::io::stdout().lock();

    if json {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
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

async fn cmd_runs_events(run_id: &str, query: &[String], json: bool, verbose: bool) -> Result<()> {
    debug!("searching events for run: {}", run_id);

    let api = AntithesisApi::from_env(verbose)?;
    let stream = api
        .search_run_events(run_id, &query.join(" "))
        .await?
        .into_inner();

    let mut stdout = std::io::stdout().lock();
    if json {
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        writeln!(
            stdout,
            "{:<22}  {:<22}  {:<20}  OUTPUT",
            "HASH", "VTIME", "SOURCE"
        )?;
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
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
        .await
    }
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

    let mut stdout = std::io::stdout().lock();
    if json {
        if annotate_faults {
            stream_ndjson_lines(
                stream,
                FaultAnnotator {
                    active_fault_windows: Vec::new(),
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
        writeln!(stdout, "{:<22}  {:<20}  OUTPUT", "VTIME", "SOURCE")?;
        stream_ndjson_lines(stream, NoOpTransformer {}, |line| {
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
    active_fault_windows: Vec<FaultWindow>,
    active_faults: Value,
}

impl LineTransformer for FaultAnnotator {
    fn try_transform(&mut self, line: &str) -> Option<String> {
        if let Ok(mut entry) = serde_json::from_str::<Value>(line) {
            let mut update_faults;

            let vtime_ticks_node = entry["moment"]["_vtime_ticks"].as_u64();
            let vtime_node = entry["moment"]["vtime"]
                .as_str()
                .and_then(|seconds_string| seconds_string.parse::<f64>().ok());
            let event_vtime_ticks = vtime_ticks_node
                .or_else(|| vtime_node.map(|seconds| (seconds * TICKS_PER_SECOND) as u64))
                .unwrap_or(0);
            let fault_name = entry["fault"]["name"].as_str();
            let is_fault_injector = entry["source"]["name"]
                .as_str()
                .map(|source| source.eq("fault_injector"))
                .unwrap_or(false);
            let faults_were_paused = is_fault_injector
                && entry["info"]["message"]
                    .as_str()
                    .map(|message| message.eq("status"))
                    .unwrap_or(false)
                && entry["info"]["details"]["paused"]
                    .as_bool()
                    .unwrap_or(false);
            let is_restore_event =
                is_fault_injector && fault_name.map(|n| n.eq("restore")).unwrap_or(false);

            // clear any fault windows that are expired or mooted by fault injector pauses
            let length_before_cleanup = self.active_fault_windows.len();
            self.active_fault_windows.retain(|w| {
                if w.is_expired(event_vtime_ticks) {
                    return false;
                }

                if faults_were_paused && (w.is_network_fault() || w.is_node_fault()) {
                    return false;
                }

                if is_restore_event && w.is_network_fault() {
                    return false;
                }

                true
            });
            update_faults = length_before_cleanup != self.active_fault_windows.len();

            if is_fault_injector
                && let Some(new_window) =
                    try_get_fault_window_definition(&entry, event_vtime_ticks, fault_name)
            {
                self.active_fault_windows.push(new_window);
                update_faults = true;
            }

            if update_faults {
                self.active_faults = active_fault_dictionary(&self.active_fault_windows);
            }

            if let Some(output_text) = entry["output_text"].as_str() {
                entry["output_text"] = Value::String(strip_ansi(output_text));
            }
            if vtime_ticks_node.is_some() || vtime_node.is_some() {
                entry["vtime_seconds"] = json!((event_vtime_ticks as f64) / TICKS_PER_SECOND);
            }
            entry["active_faults"] = self.active_faults.clone();

            return Some(format!("{}", entry));
        }

        None
    }
}

fn try_get_fault_window_definition(
    entry: &Value,
    event_vtime_ticks: u64,
    maybe_fault_name: Option<&str>,
) -> Option<FaultWindow> {
    if let Some(fault_name) = maybe_fault_name {
        let max_duration_ticks = entry["fault"]["max_duration"]
            .as_f64()
            .filter(|d| *d >= 0.0)
            .map(|d| (d * TICKS_PER_SECOND) as u64);
        let end_vtime = max_duration_ticks.map(|duration| duration + event_vtime_ticks);
        let fault_type = entry["fault"]["type"].as_str().unwrap_or("");

        if let Some(target) = entry["fault"]["affected_nodes"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|first| first.as_str())
        {
            if fault_name.eq("partition") || fault_name.eq("clog") {
                return Some(FaultWindow::Network {
                    name: fault_name.to_string(),
                    start_vtime: event_vtime_ticks,
                    end_vtime,
                });
            }

            if fault_type.eq("node") && (fault_name.eq("pause") || fault_name.eq("throttle")) {
                return Some(FaultWindow::Node {
                    name: fault_name.to_string(),
                    start_vtime: event_vtime_ticks,
                    end_vtime,
                    container: target.to_string(),
                });
            }
        }

        if fault_name.eq("skip")
            && fault_type.eq("clock")
            && let Some(offset) = entry["fault"]["details"]["offset"].as_f64()
        {
            return Some(FaultWindow::Clock {
                name: fault_name.to_string(),
                start_vtime: event_vtime_ticks,
                offset,
                end_vtime,
            });
        }
    }

    None
}

#[derive(Debug, PartialEq, Eq)]
struct RenderedEventEntry {
    input_hash: String,
    vtime: String,
    source: String,
    output: String,
}

#[derive(Debug, PartialEq)]
enum FaultWindow {
    Network {
        name: String,
        start_vtime: u64,
        end_vtime: Option<u64>,
    },
    Node {
        name: String,
        start_vtime: u64,
        end_vtime: Option<u64>,
        container: String,
    },
    Clock {
        name: String,
        start_vtime: u64,
        offset: f64,
        end_vtime: Option<u64>,
    },
}

impl FaultWindow {
    fn get_end_vtime(&self) -> &Option<u64> {
        match self {
            Self::Network {
                name: _,
                start_vtime: _,
                end_vtime,
            } => end_vtime,
            Self::Node {
                name: _,
                start_vtime: _,
                end_vtime,
                container: _,
            } => end_vtime,
            Self::Clock {
                name: _,
                start_vtime: _,
                offset: _,
                end_vtime,
            } => end_vtime,
        }
    }

    fn is_expired(&self, latest_vtime: u64) -> bool {
        self.get_end_vtime()
            .map(|t| t < latest_vtime)
            .unwrap_or(false)
    }

    fn is_network_fault(&self) -> bool {
        matches!(
            self,
            Self::Network {
                name: _,
                start_vtime: _,
                end_vtime: _,
            }
        )
    }

    fn is_node_fault(&self) -> bool {
        matches!(
            self,
            Self::Node {
                name: _,
                start_vtime: _,
                end_vtime: _,
                container: _,
            }
        )
    }
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

fn active_fault_dictionary(open_windows: &Vec<FaultWindow>) -> Value {
    let mut result = Map::new();
    let mut offset_sum = 0f64;
    let mut max_clock_fault_start: Option<u64> = None;
    let mut network_fault_starts = IndexMap::<String, u64>::new();
    let mut node_faults = IndexMap::<String, Map<String, Value>>::new();

    for fault_window in open_windows {
        if let FaultWindow::Clock {
            name: _,
            start_vtime,
            offset,
            end_vtime: _,
        } = fault_window
        {
            max_clock_fault_start = max_clock_fault_start
                .map(|prev| prev.max(*start_vtime))
                .or(Some(*start_vtime));
            offset_sum += offset;
        } else if let FaultWindow::Network {
            name,
            start_vtime,
            end_vtime: _,
        } = fault_window
        {
            match network_fault_starts.entry(format!("network_{}", name)) {
                Entry::Vacant(entry) => {
                    entry.insert(*start_vtime);
                }
                Entry::Occupied(mut entry) => {
                    if entry.get().ge(start_vtime) {
                        entry.insert(*start_vtime);
                    }
                }
            }
        } else if let FaultWindow::Node {
            name,
            start_vtime,
            end_vtime: _,
            container,
        } = fault_window
        {
            node_faults
                .entry(format!("node_{}", name))
                .or_default()
                .insert(
                    container.clone(),
                    json!((*start_vtime as f64) / TICKS_PER_SECOND),
                );
        }
    }

    for entry in network_fault_starts {
        result.insert(
            entry.0,
            json!({"vtime": (entry.1 as f64) / TICKS_PER_SECOND}),
        );
    }

    for entry in node_faults {
        result.insert(entry.0, Value::Object(entry.1));
    }

    if let Some(latest_vtime) = max_clock_fault_start {
        result.insert("clock_skip".to_string(), json!({"cumulative_offset": offset_sum, "vtime": (latest_vtime as f64) / TICKS_PER_SECOND}));
    }

    Value::Object(result)
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
    let payload: AssertionPayload = serde_json::from_value(assertion.clone()).ok()?;
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
                run.status.to_string(),
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

struct PropertyValueRow<'a> {
    example: &'static str,
    name: &'a str,
    value: String,
}

fn flatten_property_events(properties: &[Property]) -> Vec<PropertyEventRow<'_>> {
    let mut rows = Vec::new();
    for property in properties {
        let start = rows.len();
        for event in property.event_counterexamples() {
            rows.push(PropertyEventRow {
                example: "Failing",
                hash: &event.moment.input_hash,
                vtime: &event.moment.vtime,
                name: property.name(),
            });
        }
        for event in property.event_examples() {
            rows.push(PropertyEventRow {
                example: "Passing",
                hash: &event.moment.input_hash,
                vtime: &event.moment.vtime,
                name: property.name(),
            });
        }
        rows[start..].sort_by(|a, b| {
            example_rank(a.example)
                .cmp(&example_rank(b.example))
                .then_with(|| compare_vtime(a.vtime, b.vtime))
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

fn compare_vtime(a: &str, b: &str) -> std::cmp::Ordering {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(a), Ok(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.cmp(b),
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

fn flatten_non_event_property_values(properties: &[Property]) -> Vec<PropertyValueRow<'_>> {
    let mut rows = Vec::new();
    for property in properties {
        let Property::NonEventProperty(p) = property else {
            continue;
        };
        let mut emitted = false;
        for value in &p.counterexamples {
            rows.push(PropertyValueRow {
                example: "Failing",
                name: &p.name,
                value: serde_json::to_string(value).unwrap_or_default(),
            });
            emitted = true;
        }
        for value in &p.examples {
            rows.push(PropertyValueRow {
                example: "Passing",
                name: &p.name,
                value: serde_json::to_string(value).unwrap_or_default(),
            });
            emitted = true;
        }
        if !emitted {
            rows.push(PropertyValueRow {
                example: "-",
                name: &p.name,
                value: "-".to_string(),
            });
        }
    }
    rows
}

fn render_property_values_table(rows: &[PropertyValueRow]) -> String {
    let headers = vec![
        "EXAMPLE".to_string(),
        "NAME".to_string(),
        "VALUE".to_string(),
    ];
    let table_rows = rows
        .iter()
        .map(|row| {
            vec![
                row.example.to_string(),
                sanitize(row.name),
                sanitize(&row.value),
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

    #[test]
    fn renders_flattened_property_events_table() {
        let properties = vec![
            Property::EventProperty(EventProperty {
                counterexample_count: Some(3),
                counterexamples: vec![event("-100", "5.0"), event("-200", "10.0")],
                description: None,
                example_count: Some(12),
                examples: vec![event("-300", "15.0")],
                group: Some("Safety".to_string()),
                is_event: true,
                is_group: None,
                name: "Counter value stays below limit".to_string(),
                status: PropertyStatus::Failing,
            }),
            Property::EventProperty(EventProperty {
                counterexample_count: Some(0),
                counterexamples: Vec::new(),
                description: None,
                example_count: Some(1),
                examples: vec![event("-400", "1.0")],
                group: None,
                is_event: true,
                is_group: None,
                name: "Setup completes".to_string(),
                status: PropertyStatus::Passing,
            }),
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

        // Failing rows come first, sorted by vtime numerically (5.0 < 10.0)
        assert!(
            lines[1].contains("Failing")
                && lines[1].contains("-100")
                && lines[1].contains("5.0")
                && lines[1].contains("Counter value stays below limit")
        );
        assert!(
            lines[2].contains("Failing")
                && lines[2].contains("-200")
                && lines[2].contains("10.0")
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
        let properties = vec![Property::NonEventProperty(NonEventProperty {
            counterexample_count: Some(0),
            counterexamples: Vec::new(),
            description: None,
            example_count: Some(0),
            examples: Vec::new(),
            group: None,
            is_event: false,
            is_group: None,
            name: "No events property".to_string(),
            status: PropertyStatus::Passing,
        })];

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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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

        // Empty affected_nodes: try_get_fault_window_definition returns None,
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
        // Now only the second window remains; start_vtime becomes 14
        assert_eq!(
            transformer.try_transform(&format!("{}", json!({
                "moment": { "_vtime_ticks": 16u64 << 32 },
                "output_text": "after first window expired"
            }))),
            Some(concat!(
                r#"{"moment":{"_vtime_ticks":68719476736},"output_text":"after first window expired","#,
                r#""vtime_seconds":16.0,"active_faults":{"network_partition":{"vtime":14.0}}}"#
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
            active_fault_windows: Vec::new(),
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
}
