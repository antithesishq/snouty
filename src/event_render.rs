//! The one renderer for human-facing event streams (`runs logs`,
//! `runs events`, `runs search`, and any future stream of moment-stamped
//! events).
//!
//! Every stream entry is classified into an [`EventKind`] and rendered as a
//! concise block, git-log style: a yellow `moment HASH VTIME` divider opens
//! each timeline segment (the moment is the commit, its events are the body),
//! one line per event beneath it, and dim indented detail lines where a kind
//! has more to say. Blocks for consecutive segments are separated by a blank
//! line. Colors go through [`console::style`], which disables itself off-tty
//! and under `NO_COLOR`.
//!
//! Two rendering modes:
//! - classified (the default): recognize Antithesis event shapes (SDK
//!   assertions, guidance, fault injections, container lifecycle, test
//!   composer chatter) and render each in its own concise form; everything
//!   else falls back to the log-text or raw-JSON renderer.
//! - raw (`--raw`): no classification, no colors, no dividers — the legacy
//!   `[vtime] [source] [stream] payload` line with the text payload verbatim
//!   (ANSI and control bytes intact) and structured payloads as their raw
//!   JSON. The uninterpreted view.
//!
//! The renderer also owns the display conventions the streams share: vtime is
//! normalized through [`VTime`] and shown truncated (never rounded) to 3
//! decimals so a value copied off the screen and pasted back lands on — never
//! past — the line you saw; the divider carries the segment's full-precision
//! moment for exact `runs logs`/`runs exec`/`snouty debug` follow-ups.

use std::path::Path;
use std::sync::OnceLock;

use console::style;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::render::sanitize;
use crate::vtime::VTime;

/// vtime is shown truncated to 3 decimals. Sized for runs up to ~9999 vsec
/// (`"9999.999"`, 8 chars), which covers the vast majority; longer runs
/// overflow this width on their lines rather than padding every shorter line
/// to match.
const VTIME_WIDTH: usize = 8;

/// Detail lines indent under the source column: vtime column plus its
/// two-space gap.
const DETAIL_INDENT: usize = VTIME_WIDTH + 2;

/// The raw-mode source column is sized to fit `antithesis_test_composer` —
/// the built-in test-composer source present in nearly every run's logs — so
/// those lines align instead of overflowing. Longer sources still overflow on
/// their own lines rather than widening the column for everyone.
const RAW_SOURCE_MIN_WIDTH: usize = "antithesis_test_composer".len();
const RAW_STREAM_WIDTH: usize = 3;

/// Long opaque values (container ids, image digests) are truncated to this
/// many columns in classified key=value renderings; the full value is always
/// in `--json`.
const VALUE_TRUNCATE_WIDTH: usize = 40;

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
        // Accept the short forms too: the events/logs API reports app
        // stdout/stderr as `out`/`err` (see `abbreviated`), so the logs viewer
        // can normalize either form when rendering a stream label.
        match s {
            "stdout" | "out" => Ok(Self::Stdout),
            "stderr" | "err" => Ok(Self::Stderr),
            "info" | "inf" => Ok(Self::Info),
            "error" => Ok(Self::Error),
            other => Err(format!(
                "invalid stream '{other}' (expected one of: stdout, stderr, info, error)"
            )),
        }
    }
}

fn abbreviate_stream(stream: &str) -> std::borrow::Cow<'static, str> {
    if let Ok(s) = stream.parse::<Stream>() {
        return std::borrow::Cow::Borrowed(s.abbreviated());
    }
    if stream.is_empty() {
        return std::borrow::Cow::Borrowed("   ");
    }
    std::borrow::Cow::Owned(stream.chars().take(RAW_STREAM_WIDTH).collect())
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

pub(crate) fn strip_ansi(text: &str) -> String {
    ansi_re().replace_all(text, "").to_string()
}

/// Single choke point for terminal-bound free text: strip ANSI escape
/// sequences first, then escape any remaining control bytes so stray
/// `\r`/`\x08`/BEL can't corrupt the terminal. Every classified payload's
/// free text goes through here so the streams render container output
/// identically.
pub(crate) fn normalize_terminal_text(text: &str) -> String {
    sanitize(&strip_ansi(text))
}

/// Keys that wrap a log record's payload; dropped before rendering the body.
const LOG_ENVELOPE_KEYS: [&str; 3] = ["moment", "source", "IPT_bytes_out"];

/// Serialize-only view over a JSON object that emits every key except the
/// envelope keys, borrowing the retained values rather than cloning them.
struct StrippedEnvelope<'a>(&'a Map<String, Value>);

impl serde::Serialize for StrippedEnvelope<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_map(
            self.0
                .iter()
                .filter(|(key, _)| !LOG_ENVELOPE_KEYS.contains(&key.as_str())),
        )
    }
}

fn strip_log_envelope(entry: &Value) -> String {
    // Serialize a borrowed, filtered view of the object rather than deep-cloning
    // the whole Value just to drop three envelope keys — this runs per JSON log
    // line. The view preserves the original key order (matching serde_json's
    // preserve_order) without copying any of the retained subtrees.
    match entry.as_object() {
        Some(obj) => serde_json::to_string(&StrippedEnvelope(obj)).unwrap_or_default(),
        None => serde_json::to_string(entry).unwrap_or_default(),
    }
}

/// Truncate (never round) the decimal string `s` to exactly `decimals`
/// fractional digits, zero-padding a short or missing fraction (`"19"` ->
/// `"19.000"`, `"14.78"` -> `"14.780"`, `"1814.71357"` -> `"1814.713"`). Fixed
/// width keeps a column of these aligned on the decimal point. Only a plain
/// decimal string is sliced; anything else (scientific notation) falls back to
/// rounded fixed-point, and a non-number is passed through verbatim.
fn truncate_decimals(s: &str, decimals: usize) -> String {
    let t = s.trim();
    let (int_part, frac_part) = match t.split_once('.') {
        Some((i, f)) => (i, f),
        None => (t, ""),
    };
    let is_plain = !int_part.is_empty()
        && int_part
            .char_indices()
            .all(|(i, c)| c.is_ascii_digit() || (i == 0 && (c == '-' || c == '+')))
        && frac_part.chars().all(|c| c.is_ascii_digit());
    if !is_plain {
        return match t.parse::<f64>() {
            Ok(v) => format!("{v:.decimals$}"),
            Err(_) => t.to_string(),
        };
    }
    if decimals == 0 {
        return int_part.to_string();
    }
    let mut frac: String = frac_part.chars().take(decimals).collect();
    while frac.len() < decimals {
        frac.push('0');
    }
    format!("{int_part}.{frac}")
}

/// Render an event's vtime in seconds with exactly 3 decimal places,
/// truncated — never rounded. Fixed precision keeps the decimal point and
/// right edge aligned down the fixed [`VTIME_WIDTH`] column. Truncating
/// rather than rounding means a vtime copied off the screen and pasted back
/// as `--begin-vtime` lands on — never just past — the line you saw.
fn format_vtime_cell(entry: &Value) -> String {
    let raw = &entry["moment"]["vtime"];
    match raw.as_str() {
        // The API sends vtime as a seconds string; truncate it directly so f64
        // round-trips can't nudge the displayed value.
        Some(s) => truncate_decimals(s, 3),
        // A JSON-number vtime: VTime's Display is the exact text the number
        // was printed from, so truncating it cuts — never rounds.
        None => match VTime::from_json(raw) {
            Some(v) => truncate_decimals(&v.to_string(), 3),
            None => String::new(),
        },
    }
}

// ---------------------------------------------------------------------------
// Assertion summaries (the `antithesis_assert` payload)
// ---------------------------------------------------------------------------

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

    fn styled(self) -> console::StyledObject<&'static str> {
        let styled = style(self.as_str()).bold();
        match self {
            Self::Pass => styled.green(),
            Self::Fail => styled.red(),
            Self::Unhit => styled.yellow(),
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

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let assertion = entry.get("antithesis_assert")?;
    let payload = AssertionPayload::deserialize(assertion).ok()?;
    AssertionSummary::try_from(payload).ok()
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

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

/// What an event is, decided from its payload keys and `source.name`. The
/// classifier is deliberately shallow — a key's presence, not its schema —
/// so a shape the server grows fields on still classifies; unknown shapes
/// land in [`EventKind::Other`] and render as their raw payload JSON.
enum EventKind<'a> {
    /// `antithesis_assert`: an SDK assertion evaluation.
    Assert(AssertionSummary),
    /// `antithesis_guidance`: SDK exploration guidance.
    Guidance(&'a Value),
    /// `antithesis_sdk`: the SDK announcing itself.
    Sdk(&'a Value),
    /// `antithesis_setup`: the workload's setup-complete signal.
    Setup(&'a Value),
    /// `fault` from the fault injector: an injected fault.
    Fault(&'a Value),
    /// `info` from the fault injector: injector status chatter.
    FaultInjectorInfo(&'a Value),
    /// A `containers_meta` record: container lifecycle (create/start/died/…).
    ContainerMeta(&'a Value),
    /// A test-composer record with `task_status`: a test command's lifecycle.
    ComposerTask(&'a Value),
    /// Any other structured test-composer record: internal composer chatter.
    ComposerKv(&'a Map<String, Value>),
    /// `output_text`: a plain log line from a container or script.
    Log(&'a str),
    /// Anything else — an unknown structured payload, or a row reshaped by an
    /// event-set DSL pipeline. Rendered as its raw payload JSON.
    Other(&'a Value),
}

fn classify(entry: &Value) -> EventKind<'_> {
    let source_name = entry["source"]["name"].as_str().unwrap_or("");
    if let Some(summary) = parse_assertion_summary(entry) {
        return EventKind::Assert(summary);
    }
    if let Some(guidance) = entry.get("antithesis_guidance") {
        return EventKind::Guidance(guidance);
    }
    if let Some(sdk) = entry.get("antithesis_sdk") {
        return EventKind::Sdk(sdk);
    }
    if let Some(setup) = entry.get("antithesis_setup") {
        return EventKind::Setup(setup);
    }
    if source_name == "fault_injector" {
        if let Some(fault) = entry.get("fault") {
            return EventKind::Fault(fault);
        }
        if let Some(info) = entry.get("info") {
            return EventKind::FaultInjectorInfo(info);
        }
    }
    if source_name == "containers_meta" && entry.get("event").is_some() {
        return EventKind::ContainerMeta(entry);
    }
    if let Some(text) = entry.get("output_text").and_then(Value::as_str) {
        return EventKind::Log(text);
    }
    if source_name == "antithesis_test_composer" {
        if entry.get("task_status").is_some() {
            return EventKind::ComposerTask(entry);
        }
        if let Some(obj) = entry.as_object() {
            return EventKind::ComposerKv(obj);
        }
    }
    EventKind::Other(entry)
}

// ---------------------------------------------------------------------------
// Per-kind rendering
// ---------------------------------------------------------------------------

/// One rendered event: the headline that shares the event's own line, plus
/// any detail lines to indent beneath it.
struct RenderedPayload {
    headline: String,
    details: Vec<String>,
}

impl RenderedPayload {
    fn line(headline: String) -> Self {
        Self {
            headline,
            details: Vec::new(),
        }
    }
}

fn render_payload(entry: &Value) -> RenderedPayload {
    match classify(entry) {
        EventKind::Assert(summary) => render_assert(&summary),
        EventKind::Guidance(guidance) => render_guidance(guidance),
        EventKind::Sdk(sdk) => render_sdk(sdk),
        EventKind::Setup(setup) => render_setup(setup),
        EventKind::Fault(fault) => render_fault(fault),
        EventKind::FaultInjectorInfo(info) => render_fault_injector_info(info),
        EventKind::ContainerMeta(entry) => render_container_meta(entry),
        EventKind::ComposerTask(entry) => render_composer_task(entry),
        EventKind::ComposerKv(obj) => render_composer_kv(obj),
        EventKind::Log(text) => render_log_text(text),
        EventKind::Other(entry) => RenderedPayload::line(
            style(sanitize(&strip_log_envelope(entry)))
                .dim()
                .to_string(),
        ),
    }
}

fn render_assert(summary: &AssertionSummary) -> RenderedPayload {
    let mut headline = format!(
        "{} {} \"{}\"",
        summary.status.styled(),
        sanitize(&summary.label),
        sanitize(&summary.message),
    );
    if summary.must_hit {
        headline.push_str(" must-hit");
    }
    let details = summary
        .location
        .iter()
        .map(|location| style(format!("@ {location}")).dim().to_string())
        .collect();
    RenderedPayload { headline, details }
}

fn render_guidance(guidance: &Value) -> RenderedPayload {
    let goal = match guidance["maximize"].as_bool() {
        Some(true) => " maximize",
        Some(false) => " minimize",
        None => "",
    };
    let kind = guidance["guidance_type"].as_str().unwrap_or("");
    let message = guidance["message"].as_str().unwrap_or("").trim();
    RenderedPayload::line(format!(
        "{} {}{goal} \"{}\"",
        style("GUIDANCE").magenta(),
        sanitize(kind),
        sanitize(message),
    ))
}

fn render_sdk(sdk: &Value) -> RenderedPayload {
    let language = format!(
        "{} {}",
        sdk["language"]["name"].as_str().unwrap_or("?"),
        sdk["language"]["version"].as_str().unwrap_or(""),
    );
    RenderedPayload::line(format!(
        "{} connected: {} (sdk {}, protocol {})",
        style("SDK").green(),
        sanitize(language.trim()),
        sanitize(sdk["sdk_version"].as_str().unwrap_or("?")),
        sanitize(sdk["protocol_version"].as_str().unwrap_or("?")),
    ))
}

fn render_setup(setup: &Value) -> RenderedPayload {
    let mut headline = format!(
        "{} {}",
        style("SETUP").green().bold(),
        sanitize(setup["status"].as_str().unwrap_or("")),
    );
    if let Some(details) = setup["details"].as_object() {
        for (key, value) in details {
            if let Some(rendered) = format_scalar(value) {
                headline.push_str(&format!(" {}={rendered}", sanitize(key)));
            }
        }
    }
    RenderedPayload::line(headline.trim_end().to_string())
}

fn render_fault(fault: &Value) -> RenderedPayload {
    let name = fault["name"].as_str().unwrap_or("");
    let kind = fault["type"].as_str().unwrap_or("");
    // `restore` ends the disruption; everything else starts one.
    let color = |text: String| {
        if name == "restore" {
            style(text).green()
        } else {
            style(text).red()
        }
    };

    let mut bits: Vec<String> = Vec::new();
    let details = &fault["details"];
    if let Some(disruption) = details["disruption_type"].as_str() {
        bits.push(sanitize(disruption));
    }
    if details["asymmetric"].as_bool() == Some(true) {
        bits.push("asymmetric".to_string());
    }
    if let Some(offset) = details["offset"].as_f64() {
        bits.push(format!("offset={offset:+.2}s"));
    }
    if let Some(latency) = details["latency"].as_object() {
        let mean = latency.get("mean").and_then(Value::as_f64).unwrap_or(0.0);
        let deviation = latency
            .get("deviation")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        bits.push(format!("latency={mean:.0}ms±{deviation:.0}"));
    }
    if let Some(drop_rate) = details["drop_rate"].as_f64()
        && drop_rate > 0.0
    {
        bits.push(format!("drop={drop_rate}"));
    }
    if let Some(nodes) = fault["affected_nodes"].as_array()
        && !nodes.is_empty()
    {
        let names: Vec<String> = nodes
            .iter()
            .map(|node| sanitize(node.as_str().unwrap_or("?")))
            .collect();
        bits.push(format!("nodes={}", names.join(",")));
    }
    if let Some(duration) = format_duration(&fault["max_duration"]) {
        bits.push(format!("max={duration}"));
    }

    let mut headline = format!(
        "{} {}",
        color("FAULT".to_string()).bold(),
        color(format!("{}/{}", sanitize(kind), sanitize(name))),
    );
    if !bits.is_empty() {
        headline.push(' ');
        headline.push_str(&bits.join(" "));
    }
    RenderedPayload::line(headline)
}

fn render_fault_injector_info(info: &Value) -> RenderedPayload {
    let mut text = format!(
        "fault-injector {}",
        sanitize(info["message"].as_str().unwrap_or("")),
    );
    if let Some(details) = info["details"].as_object() {
        for (key, value) in details {
            if let Some(rendered) = format_scalar(value) {
                text.push_str(&format!(" {}={rendered}", sanitize(key)));
            }
        }
    }
    RenderedPayload::line(style(text.trim_end().to_string()).dim().to_string())
}

fn render_container_meta(entry: &Value) -> RenderedPayload {
    let event = entry["event"].as_str().unwrap_or("");
    let name = entry["name"].as_str().unwrap_or("");
    let mut text = format!("container {} {}", sanitize(event), sanitize(name));
    if let Some(code) = entry.get("container_exit_code")
        && let Some(rendered) = format_scalar(code)
    {
        text.push_str(&format!(" exit={rendered}"));
    }
    // Deaths pop red; the routine lifecycle chatter stays blue.
    let headline = match event {
        "died" | "kill" | "oom" => style(text).red().to_string(),
        _ => style(text).blue().to_string(),
    };
    // The image (repository only — the digest is noise at a glance) matters
    // once, when the container is created.
    let details = match (event, entry["image"].as_str()) {
        ("create", Some(image)) => {
            let repository = image.split('@').next().unwrap_or(image);
            vec![
                style(format!("image {}", sanitize(repository)))
                    .dim()
                    .to_string(),
            ]
        }
        _ => Vec::new(),
    };
    RenderedPayload { headline, details }
}

fn render_composer_task(entry: &Value) -> RenderedPayload {
    let status = entry["task_status"].as_str().unwrap_or("");
    let command = entry["command"].as_str().unwrap_or("");
    match status {
        "finished" => {
            let return_code = entry["command_return_code"].as_str().unwrap_or("");
            let ok = return_code == "0";
            let mut text = format!("task finished {}", sanitize(command));
            if !return_code.is_empty() {
                text.push_str(&format!(" exit={}", sanitize(return_code)));
            }
            if let Some(duration) = format_duration(&entry["command_runtime"]) {
                text.push_str(&format!(" in {duration}"));
            }
            let headline = if ok {
                style(text).blue().to_string()
            } else {
                style(text).red().to_string()
            };
            let mut details = Vec::new();
            for (key, prefix) in [("additional_stdout", "out"), ("additional_stderr", "err")] {
                let Some(text) = entry[key].as_str() else {
                    continue;
                };
                for line in strip_ansi(text).lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    details.push(
                        style(format!("{prefix}| {}", sanitize(line)))
                            .dim()
                            .to_string(),
                    );
                }
            }
            RenderedPayload { headline, details }
        }
        _ => RenderedPayload::line(
            style(format!("task {} {}", sanitize(status), sanitize(command)))
                .blue()
                .to_string(),
        ),
    }
}

fn render_composer_kv(obj: &Map<String, Value>) -> RenderedPayload {
    let mut text = "composer".to_string();
    for (key, value) in obj {
        if LOG_ENVELOPE_KEYS.contains(&key.as_str()) {
            continue;
        }
        if let Some(rendered) = format_scalar(value) {
            let truncated = console::truncate_str(&rendered, VALUE_TRUNCATE_WIDTH, "…");
            text.push_str(&format!(" {}={truncated}", sanitize(key)));
        }
    }
    RenderedPayload::line(style(text).dim().to_string())
}

fn render_log_text(text: &str) -> RenderedPayload {
    // Strip ANSI color codes before escaping controls so colorized container
    // output shows the plain text, not visible `\x1B[…` escape noise. The
    // payload is one line in practice (the server splits on newlines); a
    // multi-line payload would have its break escaped to a visible `\n` by
    // the sanitizer, staying one terminal line either way.
    RenderedPayload::line(normalize_terminal_text(text))
}

/// Render a scalar JSON value for a key=value cell; `None` for empties and
/// nested structures (arrays of scalars join with commas). Mirrors what the
/// composer/fault chatter actually carries: stringified scalars.
fn format_scalar(value: &Value) -> Option<String> {
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

/// Render a duration in seconds (the API sends both numbers and stringified
/// numbers) as a short `1.2s`; `None` when absent or unparsable.
fn format_duration(value: &Value) -> Option<String> {
    let seconds = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    Some(format!("{seconds:.1}s"))
}

/// The bracketed source label: the container when the record names one,
/// otherwise the source name with the `antithesis_` prefix dropped, plus the
/// abbreviated stream when present.
fn render_source(entry: &Value) -> String {
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let stream = entry["source"]["stream"].as_str().unwrap_or("");

    let label = if !container.trim().is_empty() {
        sanitize(container)
    } else {
        sanitize(name.trim().strip_prefix("antithesis_").unwrap_or(name))
    };
    let stream = (!stream.is_empty()).then(|| abbreviate_stream(stream));

    match (label.is_empty(), stream) {
        (false, Some(stream)) => format!("[{label}:{stream}]"),
        (false, None) => format!("[{label}]"),
        (true, Some(stream)) => format!("[{stream}]"),
        (true, None) => "[]".to_string(),
    }
}

// ---------------------------------------------------------------------------
// The stream renderer
// ---------------------------------------------------------------------------

/// Stateful renderer for one event stream. Feed it entries (and raw lines) in
/// stream order; it returns the exact text to print for each, dividers and
/// blank-line separation included.
pub(crate) struct EventStreamRenderer {
    raw: bool,
    last_input_hash: Option<String>,
    wrote_block: bool,
}

impl EventStreamRenderer {
    pub(crate) fn new(raw: bool) -> Self {
        Self {
            raw,
            last_input_hash: None,
            wrote_block: false,
        }
    }

    /// Render one NDJSON entry (vtime already normalized by the stream).
    /// The returned text may span several lines — a `moment` divider when the
    /// entry opens a new timeline segment, the event line, and any indented
    /// detail lines — and carries no trailing newline.
    pub(crate) fn render_entry(&mut self, entry: &Value) -> String {
        if self.raw {
            return raw_line(entry);
        }

        // A full event carries both its moment and its source envelope. A
        // row reshaped by an event-set DSL pipeline can lack either (narrow
        // can keep `moment` while dropping the rest); rendering it through
        // the event form would produce a half-empty hybrid line, so its JSON
        // is the row — print it as such.
        let hash = entry["moment"]["input_hash"].as_str();
        let Some(hash) = hash.filter(|_| entry.get("source").is_some()) else {
            return sanitize(&entry.to_string());
        };

        let mut out = String::new();
        if self.last_input_hash.as_deref() != Some(hash) {
            if self.wrote_block {
                out.push('\n');
            }
            // The divider carries the segment's full-precision moment —
            // exactly what `runs logs`/`runs exec`/`snouty debug` take.
            let vtime = VTime::from_json(&entry["moment"]["vtime"])
                .map(|v| v.to_string())
                .unwrap_or_default();
            let divider = format!("moment {} {}", sanitize(hash), vtime);
            out.push_str(&style(divider.trim_end().to_string()).yellow().to_string());
            out.push('\n');
            self.last_input_hash = Some(hash.to_string());
        }

        let vtime_cell = format!("{:>VTIME_WIDTH$}", format_vtime_cell(entry));
        let rendered = render_payload(entry);
        out.push_str(
            format!(
                "{}  {} {}",
                style(vtime_cell).dim(),
                style(render_source(entry)).cyan(),
                rendered.headline
            )
            .trim_end(),
        );
        for detail in rendered.details {
            out.push('\n');
            out.push_str(&format!("{:DETAIL_INDENT$}{detail}", ""));
        }
        self.wrote_block = true;
        out
    }
}

/// The `--raw` line: the legacy `[vtime] [source] [stream] payload` format
/// with no classification and no colors. Text payloads are verbatim (ANSI and
/// control bytes intact); structured payloads show their raw JSON after a
/// ` - ` separator.
fn raw_line(entry: &Value) -> String {
    let vtime = format_vtime_cell(entry);
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");
    let source = if !container.is_empty() {
        container
    } else {
        name
    };
    let stream_raw = entry["source"]["stream"].as_str().unwrap_or("");
    let stream = abbreviate_stream(stream_raw);

    let payload = match entry.get("output_text").and_then(Value::as_str) {
        Some(text) => text.to_string(),
        None => format!(" - {}", strip_log_envelope(entry)),
    };

    format!(
        "[{vtime:>vw$}] [{source:>sw$}] [{stream:<stw$}] {payload}",
        vw = VTIME_WIDTH,
        sw = RAW_SOURCE_MIN_WIDTH,
        stw = RAW_STREAM_WIDTH,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every assertion below is on the un-colored text: colors are a terminal
    /// affordance layered by `console`, which the test harness (a pipe, not a
    /// tty) disables. Force that off so a color-forcing environment can't
    /// flake the suite.
    fn renderer(raw: bool) -> EventStreamRenderer {
        console::set_colors_enabled(false);
        EventStreamRenderer::new(raw)
    }

    fn render_one(entry: Value) -> String {
        renderer(false).render_entry(&entry)
    }

    #[test]
    fn opens_each_timeline_segment_with_a_full_precision_moment_divider() {
        let mut r = renderer(false);
        let first = r.render_entry(&json!({
            "moment": {"input_hash": "-123", "vtime": "311.8487535319291"},
            "source": {"container": "app", "name": "app", "stream": "out"},
            "output_text": "starting"
        }));
        // The divider carries the exact moment (pasteable into `runs logs`);
        // the event line shows the truncated vtime.
        assert_eq!(
            first,
            "moment -123 311.8487535319291\n 311.848  [app:out] starting"
        );

        // Same hash: no divider, no blank line.
        let second = r.render_entry(&json!({
            "moment": {"input_hash": "-123", "vtime": "312.0"},
            "source": {"container": "app", "name": "app", "stream": "out"},
            "output_text": "still here"
        }));
        assert_eq!(second, " 312.000  [app:out] still here");

        // New hash: blank line, then the next divider.
        let third = r.render_entry(&json!({
            "moment": {"input_hash": "456", "vtime": "313.5"},
            "source": {"container": "app", "name": "app", "stream": "out"},
            "output_text": "branched"
        }));
        assert_eq!(third, "\nmoment 456 313.5\n 313.500  [app:out] branched");
    }

    #[test]
    fn renders_assertions_with_status_and_location_detail() {
        let block = render_one(json!({
            "antithesis_assert": {
                "assert_type": "always",
                "condition": false,
                "display_type": "AlwaysOrUnreachable",
                "hit": false,
                "location": {"begin_line": 87, "file": "/go/src/antithesis/control/control.go", "function": "get"},
                "message": "Counter's value retrieved",
                "must_hit": true
            },
            "source": {"container": "control", "name": "control"},
            "moment": {"input_hash": "-1", "vtime": "311.8487535319291"}
        }));
        let mut lines = block.lines().skip(1); // divider
        assert_eq!(
            lines.next().unwrap(),
            " 311.848  [control] UNHIT AlwaysOrUnreachable \"Counter's value retrieved\" must-hit"
        );
        assert_eq!(lines.next().unwrap(), "          @ control.go:get:87");
    }

    #[test]
    fn an_incomplete_assertion_falls_back_to_the_log_text() {
        // Schema-valid but incomplete assertion payload: not classifiable as
        // an assert, so the record's log text wins.
        let block = render_one(json!({
            "antithesis_assert": {},
            "output_text": "raw log line",
            "source": {"container": "control"},
            "moment": {"input_hash": "-1", "vtime": "5.0"}
        }));
        assert!(block.ends_with("[control] raw log line"), "got: {block}");
    }

    #[test]
    fn renders_guidance_sdk_and_setup_events() {
        let guidance = render_one(json!({
            "antithesis_guidance": {"guidance_type": "numeric", "maximize": true, "message": "wal grew long", "hit": false},
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            guidance.ends_with("GUIDANCE numeric maximize \"wal grew long\""),
            "got: {guidance}"
        );

        let sdk = render_one(json!({
            "antithesis_sdk": {"language": {"name": "Rust", "version": "1.97.1"}, "protocol_version": "1.1.0", "sdk_version": "0.2.9"},
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            sdk.ends_with("SDK connected: Rust 1.97.1 (sdk 0.2.9, protocol 1.1.0)"),
            "got: {sdk}"
        );

        let setup = render_one(json!({
            "antithesis_setup": {"status": "complete", "details": {"db": "/data/test.db"}},
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            setup.ends_with("SETUP complete db=/data/test.db"),
            "got: {setup}"
        );
    }

    #[test]
    fn renders_faults_with_their_details() {
        let partition = render_one(json!({
            "fault": {
                "name": "partition", "type": "network",
                "details": {"asymmetric": true, "disruption_type": "Jammed", "partitions": [[], []]},
                "affected_nodes": ["ALL"], "max_duration": 6.215980759
            },
            "source": {"name": "fault_injector"},
            "moment": {"input_hash": "-1", "vtime": "73.111804416636"}
        }));
        assert!(
            partition.ends_with("FAULT network/partition Jammed asymmetric nodes=ALL max=6.2s"),
            "got: {partition}"
        );

        let clog = render_one(json!({
            "fault": {
                "name": "clog", "type": "network",
                "details": {"disruption_type": "Slowed", "drop_rate": 0, "latency": {"deviation": 910, "mean": 513.154606}},
                "affected_nodes": [], "max_duration": 5.894239544
            },
            "source": {"name": "fault_injector"},
            "moment": {"input_hash": "-1", "vtime": "73.9"}
        }));
        // drop_rate 0 and empty affected_nodes are omitted.
        assert!(
            clog.ends_with("FAULT network/clog Slowed latency=513ms±910 max=5.9s"),
            "got: {clog}"
        );

        let skip = render_one(json!({
            "fault": {
                "name": "skip", "type": "clock",
                "details": {"offset": -1.318698262234826},
                "affected_nodes": ["ALL"], "max_duration": 0.5434929354995208
            },
            "source": {"name": "fault_injector"},
            "moment": {"input_hash": "-1", "vtime": "87.4"}
        }));
        assert!(
            skip.ends_with("FAULT clock/skip offset=-1.32s nodes=ALL max=0.5s"),
            "got: {skip}"
        );

        // A stringified max_duration (seen live) still renders.
        let stop = render_one(json!({
            "fault": {"name": "stop", "type": "node", "affected_nodes": ["prefill-1"], "max_duration": "0"},
            "source": {"name": "fault_injector"},
            "moment": {"input_hash": "-1", "vtime": "349.0"}
        }));
        assert!(
            stop.ends_with("FAULT node/stop nodes=prefill-1 max=0.0s"),
            "got: {stop}"
        );
    }

    #[test]
    fn renders_container_lifecycle_with_image_only_on_create() {
        let create = render_one(json!({
            "event": "create", "name": "sqlite-init", "id": "3babcd",
            "image": "pkg.dev/repo/sqlite-antithesis@sha256:45abbf",
            "source": {"name": "containers_meta"},
            "moment": {"input_hash": "-1", "vtime": "12.6"}
        }));
        let mut lines = create.lines().skip(1);
        assert_eq!(
            lines.next().unwrap(),
            "  12.600  [containers_meta] container create sqlite-init"
        );
        // The digest is dropped; the repository stays.
        assert_eq!(
            lines.next().unwrap(),
            "          image pkg.dev/repo/sqlite-antithesis"
        );

        let died = render_one(json!({
            "event": "died", "name": "sqlite-init", "container_exit_code": 0,
            "image": "pkg.dev/repo/sqlite-antithesis@sha256:45abbf",
            "source": {"name": "containers_meta"},
            "moment": {"input_hash": "-1", "vtime": "12.7"}
        }));
        assert!(
            died.ends_with("container died sqlite-init exit=0"),
            "got: {died}"
        );
    }

    #[test]
    fn renders_composer_task_lifecycle() {
        let started = render_one(json!({
            "task_status": "started", "command": "git_walk/parallel_driver_walk",
            "command_type": "parallel_driver_", "container_id": "74ef1b", "tasks_len": "1",
            "started_task": "74ef1b_parallel_driver_walk",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "15.3"}
        }));
        assert!(
            started.ends_with("[test_composer] task started git_walk/parallel_driver_walk"),
            "got: {started}"
        );

        let finished = render_one(json!({
            "task_status": "finished", "command": "git_walk/parallel_driver_walk",
            "command_return_code": "1", "command_runtime": "0.03181171417236328",
            "additional_stdout": "", "additional_stderr": "boom\nsecond line",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "16.1"}
        }));
        let mut lines = finished.lines().skip(1);
        assert_eq!(
            lines.next().unwrap(),
            "  16.100  [test_composer] task finished git_walk/parallel_driver_walk exit=1 in 0.0s"
        );
        // stderr lines surface as indented details; empty stdout is omitted.
        assert_eq!(lines.next().unwrap(), "          err| boom");
        assert_eq!(lines.next().unwrap(), "          err| second line");
    }

    #[test]
    fn renders_other_composer_chatter_as_truncated_key_values() {
        let block = render_one(json!({
            "new_command_path": "git_walk/anytime_invariants",
            "container_id": "22453394531ae33a6df72b8119fb9fd8338f8854d6a271fe736417fb35975013",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "13.3"}
        }));
        assert!(
            block.contains("composer new_command_path=git_walk/anytime_invariants"),
            "got: {block}"
        );
        // The 64-hex container id is truncated for the eye; --json has it whole.
        assert!(
            block.contains("container_id=22453394531ae33a6df72b8119fb9fd8338f885…"),
            "got: {block}"
        );
    }

    #[test]
    fn strips_ansi_and_escapes_controls_in_log_text() {
        let block = render_one(json!({
            "output_text": "\x1B[4mhello\x1B[0m\u{0008}world\r\n",
            "source": {"container": "app", "stream": "out"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            block.ends_with(r"[app:out] hello\x08world\r\n"),
            "got: {block}"
        );
        assert!(!block.contains('\x1B'));
    }

    #[test]
    fn renders_unknown_payloads_and_reshaped_rows_as_json() {
        // Unknown structured payload under a moment: envelope stripped, JSON shown.
        let block = render_one(json!({
            "mystery": {"a": 1},
            "source": {"container": "app", "name": "app"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            block.ends_with(r#"[app] {"mystery":{"a":1}}"#),
            "got: {block}"
        );

        // A row reshaped by map/narrow/fold has no moment: its JSON is the row.
        let mut r = renderer(false);
        let row = r.render_entry(&json!({"count": 3, "container": "etcd0"}));
        assert_eq!(row, r#"{"count":3,"container":"etcd0"}"#);
    }

    #[test]
    fn source_label_prefers_container_and_strips_the_antithesis_prefix() {
        let source = |container: &str, name: &str, stream: Option<&str>| {
            let mut entry = json!({"source": {"container": container, "name": name}});
            if let Some(stream) = stream {
                entry["source"]["stream"] = json!(stream);
            }
            render_source(&entry)
        };
        assert_eq!(source("control", "", None), "[control]");
        assert_eq!(source("", "fault_injector", None), "[fault_injector]");
        assert_eq!(
            source("", "antithesis_test_composer", None),
            "[test_composer]"
        );
        assert_eq!(source("client1", "python3.11", None), "[client1]");
        // Streams are abbreviated to the logs viewer's three-letter forms.
        assert_eq!(
            source("", "antithesis_test_composer", Some("info")),
            "[test_composer:inf]"
        );
        assert_eq!(source("app", "app", Some("error")), "[app:err]");
    }

    #[test]
    fn raw_mode_is_the_legacy_line_with_verbatim_payload() {
        let mut r = renderer(true);
        // Text payload: ANSI and control bytes reach the terminal verbatim,
        // and there is no divider.
        let text = r.render_entry(&json!({
            "moment": {"input_hash": "1", "vtime": "14.118"},
            "source": {"name": "setup", "stream": "error"},
            "output_text": "\x1B[4m>>>> hello\x1B[0m"
        }));
        assert_eq!(
            text,
            "[  14.118] [                   setup] [err] \x1B[4m>>>> hello\x1B[0m"
        );

        // Structured payload: the raw JSON after the " - " separator.
        let structured = r.render_entry(&json!({
            "moment": {"input_hash": "1", "vtime": "2.0"},
            "source": {"name": "fault_injector"},
            "fault": {"name": "clog", "type": "network"}
        }));
        assert!(
            structured.ends_with(r#"[   ]  - {"fault":{"name":"clog","type":"network"}}"#),
            "got: {structured}"
        );
    }

    #[test]
    fn vtime_cell_truncates_never_rounds_in_both_forms() {
        // A vtime rendered from the JSON-number form (snouty's own --json
        // output) must land on the same truncated text as the server's string
        // form, so a value copied off the screen and fed back as
        // --begin-vtime lands on the line you saw, never past it.
        for s in ["398.4898056755774", "311.8487535319291", "402.0", "1.9995"] {
            let vtime: VTime = s.parse().unwrap();
            let from_string = format_vtime_cell(&json!({"moment": {"vtime": s}}));
            let from_number = format_vtime_cell(&json!({"moment": {"vtime": vtime}}));
            assert_eq!(from_string, from_number, "for {s}");
            assert_eq!(from_string, truncate_decimals(s, 3), "for {s}");
        }
        // Spot-check the truncation: .4898… cuts to .489 (rounding gives .490).
        assert_eq!(
            format_vtime_cell(&json!({"moment": {"vtime": "398.4898056755774"}})),
            "398.489"
        );
    }

    #[test]
    fn truncate_decimals_keeps_fixed_precision_without_rounding() {
        // Always exactly 3 decimals, zero-padded, so a column aligns.
        assert_eq!(truncate_decimals("19", 3), "19.000");
        assert_eq!(truncate_decimals("19.0", 3), "19.000");
        assert_eq!(truncate_decimals("14.78", 3), "14.780");
        // Truncates, never rounds: 1814.7135… -> .713 (rounding would give .714).
        assert_eq!(truncate_decimals("1814.7135719023645", 3), "1814.713");
        assert_eq!(truncate_decimals("18.9148034489", 3), "18.914");
        // Non-plain input: scientific notation falls back to fixed-point, and a
        // non-number is passed through untouched.
        assert_eq!(truncate_decimals("1e3", 3), "1000.000");
        assert_eq!(truncate_decimals("n/a", 3), "n/a");
    }

    #[test]
    fn assertion_summary_prefers_display_type_and_renders_partial_locations() {
        let entry = json!({
            "antithesis_assert": {
                "hit": false, "condition": false, "must_hit": true,
                "message": "setup reached", "assert_type": "reachability",
                "display_type": "SetupReached",
                "location": {"function": "run_setup", "begin_line": 42}
            }
        });
        let summary = parse_assertion_summary(&entry).unwrap();
        assert_eq!(summary.status, AssertionStatus::Unhit);
        assert_eq!(summary.label, "SetupReached");
        assert_eq!(summary.location.as_deref(), Some("run_setup:42"));

        // Empty display_type falls back to assert_type.
        let entry = json!({
            "antithesis_assert": {
                "hit": true, "condition": true,
                "message": "first_setup ran", "assert_type": "sometimes",
                "display_type": ""
            }
        });
        let summary = parse_assertion_summary(&entry).unwrap();
        assert_eq!(summary.label, "sometimes");
        assert_eq!(summary.status, AssertionStatus::Pass);
        assert_eq!(summary.location, None);
    }
}
