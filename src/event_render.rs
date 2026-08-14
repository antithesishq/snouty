//! The one renderer for human-facing event streams (`runs logs`,
//! `runs events`, `runs search`, `runs build-logs`, and any future stream of
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
//! Two rendering depths:
//! - default: one line per event. Antithesis event shapes (SDK assertions,
//!   guidance, fault injections, container lifecycle, test composer chatter)
//!   each render in their own concise form; everything else falls back to
//!   the log-text or raw-JSON renderer.
//! - detail (`--detail`): full-precision vtime on every line, assertion and
//!   guidance source locations, the payload's attached `details` JSON, the
//!   composer's captured stdout/stderr, and composer chatter expanded to one
//!   key=value per line, untruncated.
//!
//! There is no "raw" rendering here: `--raw` on the commands requires
//! `--json` and passes the server's NDJSON stream through verbatim, without
//! touching this module.
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

/// Long opaque values (container ids, image digests) are truncated to this
/// many columns in one-line key=value renderings; `--detail` and `--json`
/// carry the full value.
const VALUE_TRUNCATE_WIDTH: usize = 40;

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
/// `\r`/`\x08`/BEL can't corrupt the terminal.
pub(crate) fn normalize_terminal_text(text: &str) -> String {
    sanitize(&strip_ansi(text))
}

/// Is `seq` an SGR sequence (`ESC [ params m`, digits and `;` only)? SGR only
/// restyles characters — it cannot move the cursor, clear the screen, or
/// retitle the window — so it is the one escape family log text may keep.
fn is_sgr(seq: &str) -> bool {
    seq.strip_prefix("\x1b[")
        .and_then(|s| s.strip_suffix('m'))
        .is_some_and(|params| params.bytes().all(|b| b.is_ascii_digit() || b == b';'))
}

/// Sanitize a SUT log line for the terminal while keeping its colors: SGR
/// sequences pass through (when the terminal takes colors at all), every
/// other escape sequence is dropped, remaining control bytes are escaped, and
/// a reset is appended after the last kept sequence so a dangling color can't
/// bleed into the rest of the stream.
fn sanitize_log_text(text: &str) -> String {
    sanitize_log_text_inner(text, console::colors_enabled())
}

fn sanitize_log_text_inner(text: &str, keep_colors: bool) -> String {
    let mut out = String::new();
    let mut kept_any = false;
    let mut last = 0;
    for m in ansi_re().find_iter(text) {
        out.push_str(&sanitize(&text[last..m.start()]));
        if keep_colors && is_sgr(m.as_str()) {
            out.push_str(m.as_str());
            kept_any = true;
        }
        last = m.end();
    }
    out.push_str(&sanitize(&text[last..]));
    if kept_any {
        out.push_str("\x1b[0m");
    }
    out
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

/// An event's vtime at full precision: the server's own string when it is
/// still one, otherwise [`VTime`]'s exact print of the normalized number.
fn full_vtime(entry: &Value) -> String {
    let raw = &entry["moment"]["vtime"];
    match raw.as_str() {
        Some(s) => s.to_string(),
        None => VTime::from_json(raw)
            .map(|v| v.to_string())
            .unwrap_or_default(),
    }
}

/// Render an event's vtime in seconds with exactly 3 decimal places,
/// truncated — never rounded. Fixed precision keeps the decimal point and
/// right edge aligned down the fixed [`VTIME_WIDTH`] column. Truncating
/// rather than rounding means a vtime copied off the screen and pasted back
/// as `--begin-vtime` lands on — never just past — the line you saw.
fn format_vtime_cell(entry: &Value) -> String {
    // Truncate the server's seconds string directly so f64 round-trips can't
    // nudge the displayed value; a normalized JSON-number vtime goes through
    // VTime's exact Display first, so truncating still cuts — never rounds.
    truncate_decimals(&full_vtime(entry), 3)
}

// ---------------------------------------------------------------------------
// Assertion summaries (the `antithesis_assert` payload)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AssertionPayload {
    hit: Option<bool>,
    condition: Option<bool>,
    message: Option<String>,
    /// Fallback for a payload without a `message` — the SDKs set the two to
    /// the same text.
    id: Option<String>,
    assert_type: Option<String>,
    display_type: Option<String>,
    #[serde(default)]
    location: Option<AssertionLocation>,
    #[serde(default)]
    details: Option<Value>,
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
    verdict: AssertVerdict,
    message: String,
    location: Option<String>,
    /// The payload's attached `details`, pre-rendered as compact JSON.
    /// `None` when absent, null, or an empty container.
    details: Option<String>,
}

/// What one assertion event means. `must_hit` is deliberately not surfaced —
/// it is an internal aggregation concept, not something a reader of one event
/// acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssertVerdict {
    /// `hit: false` — the SDK registering the assertion in the catalog at
    /// startup, not an evaluation.
    Catalog,
    /// A passing `always`-family evaluation.
    Pass,
    /// A hit `sometimes`/`reachable` evaluation: informational, neither good
    /// nor bad on its own.
    Hit,
    /// A failing `always`-family evaluation, or a hit `unreachable`.
    Fail,
}

impl AssertVerdict {
    fn classify(hit: bool, condition: bool, assert_type: &str, display_type: &str) -> Self {
        if !hit {
            return Self::Catalog;
        }
        let assert_type = assert_type.to_ascii_lowercase();
        let display_type = display_type.to_ascii_lowercase();
        if assert_type == "unreachable" || display_type == "unreachable" {
            return Self::Fail;
        }
        // `always` covers AlwaysOrUnreachable too (its assert_type is
        // "always"); everything else (sometimes, reachability, unknown
        // future types) is informational.
        if assert_type == "always" || display_type.starts_with("always") {
            return if condition { Self::Pass } else { Self::Fail };
        }
        Self::Hit
    }

    /// The badge: FAIL only on a failing always/unreachable; CATALOG on a
    /// catalog registration; HIT otherwise.
    fn badge(self) -> &'static str {
        match self {
            Self::Catalog => "CATALOG",
            Self::Fail => "FAIL",
            Self::Pass | Self::Hit => "HIT",
        }
    }
}

impl TryFrom<AssertionPayload> for AssertionSummary {
    type Error = ();

    fn try_from(payload: AssertionPayload) -> std::result::Result<Self, Self::Error> {
        let hit = payload.hit.ok_or(())?;
        // `condition` only means something on an evaluation; a catalog
        // registration (`hit: false`) classifies without one, so a missing
        // condition must not knock it down to the raw-JSON fallback.
        let condition = match payload.condition {
            Some(condition) => condition,
            None if !hit => false,
            None => return Err(()),
        };
        let message = payload
            .message
            .or(payload.id)
            .map(|message| message.trim().to_string())
            .filter(|message| !message.is_empty())
            .ok_or(())?;
        let assert_type = payload.assert_type.unwrap_or_default();
        let display_type = payload.display_type.unwrap_or_default();
        let label = Some(display_type.trim())
            .filter(|label| !label.is_empty())
            .or_else(|| Some(assert_type.trim()).filter(|label| !label.is_empty()))
            .ok_or(())?
            .to_string();

        Ok(Self {
            verdict: AssertVerdict::classify(hit, condition, &assert_type, &display_type),
            label,
            message,
            location: payload.location.and_then(render_assertion_location),
            details: payload.details.as_ref().and_then(render_details_json),
        })
    }
}

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let assertion = entry.get("antithesis_assert")?;
    let payload = AssertionPayload::deserialize(assertion).ok()?;
    AssertionSummary::try_from(payload).ok()
}

/// A payload's attached `details` as compact JSON — `None` for null and empty
/// containers, so callers only emit a detail line when there is something to
/// read. The details are user-controlled, arbitrarily nested JSON, so they
/// are never flattened to key=value; the JSON itself (sanitized) is the
/// rendering.
fn render_details_json(details: &Value) -> Option<String> {
    let empty = match details {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    };
    if empty {
        return None;
    }
    Some(sanitize(
        &serde_json::to_string(details).unwrap_or_default(),
    ))
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

fn render_payload(entry: &Value, detail: bool) -> RenderedPayload {
    match classify(entry) {
        EventKind::Assert(summary) => render_assert(&summary, detail),
        EventKind::Guidance(guidance) => render_guidance(guidance, detail),
        EventKind::Sdk(sdk) => render_sdk(sdk),
        EventKind::Setup(setup) => render_setup(setup, detail),
        EventKind::Fault(fault) => render_fault(fault, detail),
        EventKind::FaultInjectorInfo(info) => render_fault_injector_info(info),
        EventKind::ContainerMeta(entry) => render_container_meta(entry),
        EventKind::ComposerTask(entry) => render_composer_task(entry, detail),
        EventKind::ComposerKv(obj) => render_composer_kv(obj, detail),
        EventKind::Log(text) => RenderedPayload::line(sanitize_log_text(text)),
        EventKind::Other(entry) => RenderedPayload::line(
            style(sanitize(&strip_log_envelope(entry)))
                .dim()
                .to_string(),
        ),
    }
}

fn render_assert(summary: &AssertionSummary, detail: bool) -> RenderedPayload {
    let body = format!(
        "{} \"{}\"",
        sanitize(&summary.label),
        sanitize(&summary.message)
    );
    let badge = summary.verdict.badge();
    let headline = match summary.verdict {
        // A catalog registration is chatter: the whole line recedes.
        AssertVerdict::Catalog => style(format!("{badge} {body}")).dim().to_string(),
        AssertVerdict::Fail => format!("{} {body}", style(badge).red().bold()),
        AssertVerdict::Pass => format!("{} {body}", style(badge).green().bold()),
        // A hit sometimes/reachable is neither good nor bad on its own.
        AssertVerdict::Hit => format!("{} {body}", style(badge).bold()),
    };
    let mut details = Vec::new();
    if detail {
        if let Some(location) = &summary.location {
            details.push(style(format!("@ {location}")).dim().to_string());
        }
        if let Some(json) = &summary.details {
            details.push(style(format!("details {json}")).dim().to_string());
        }
    }
    RenderedPayload { headline, details }
}

/// Guidance rendering follows ~/guidance-rendering.md: `hit: false` is the
/// catalog registration (dim, no data); an observation reconstructs the
/// source expression from `guidance_data` — best effort, with the raw
/// operands as the fallback when the expression cannot be recovered.
fn render_guidance(guidance: &Value, detail: bool) -> RenderedPayload {
    // The message doubles as the display label and the key into the
    // assertion map (`id` and `message` carry identical values).
    let message = guidance["message"]
        .as_str()
        .or(guidance["id"].as_str())
        .unwrap_or("")
        .trim();

    // A catalog registration: no data to show, the whole line recedes.
    if guidance["hit"].as_bool() == Some(false) {
        let headline = style(format!("CATALOG GUIDANCE \"{}\"", sanitize(message)))
            .dim()
            .to_string();
        let details = if detail {
            guidance_location(guidance).into_iter().collect()
        } else {
            Vec::new()
        };
        return RenderedPayload { headline, details };
    }

    let mut headline = format!("{} \"{}\"", style("GUIDANCE").bold(), sanitize(message));
    if let Some(expression) = render_guidance_expression(guidance) {
        headline.push_str(&format!(": {expression}"));
    }
    let mut details = Vec::new();
    if detail {
        let mut info = sanitize(guidance["guidance_type"].as_str().unwrap_or(""));
        match guidance["maximize"].as_bool() {
            Some(true) => info.push_str(" maximize"),
            Some(false) => info.push_str(" minimize"),
            None => {}
        }
        if let Some(data) = render_details_json(&guidance["guidance_data"]) {
            info.push(' ');
            info.push_str(&data);
        }
        let info = info.trim().to_string();
        if !info.is_empty() {
            details.push(style(info).dim().to_string());
        }
        details.extend(guidance_location(guidance));
    }
    RenderedPayload { headline, details }
}

fn guidance_location(guidance: &Value) -> Option<String> {
    AssertionLocation::deserialize(&guidance["location"])
        .ok()
        .and_then(render_assertion_location)
        .map(|location| style(format!("@ {location}")).dim().to_string())
}

/// What a guidance observation says, reconstructed from `guidance_data` —
/// best effort, `None` when there is nothing to show.
///
/// Numeric guidance carries the two operands in source order, and the
/// explorer drives their difference: `maximize: true` pushes `left` up
/// relative to `right`, `false` pushes it down — in every case the event
/// worth reaching is `left` crossing `right`. That movement is what renders:
/// `20 ↗ 1000` is the value being driven up toward its bound, `1 ↘ 0` down
/// toward it. The source inequality is deliberately NOT reconstructed — its
/// strictness is unrecoverable (`>` and `>=` emit identical guidance), and
/// its direction additionally depends on the assertion's `always`/`sometimes`
/// type, which lives on a different event a mid-stream reader may never see.
/// The drive direction is self-contained and tells the reader what the
/// explorer is doing with the number.
///
/// Boolean guidance: `maximize` alone picks the connective (`&&` when true,
/// `||` when false), and each named proposition renders with its observed
/// value inline.
fn render_guidance_expression(guidance: &Value) -> Option<String> {
    let data = &guidance["guidance_data"];
    match guidance["guidance_type"].as_str() {
        Some("numeric") => {
            let left = guidance_operand(&data["left"])?;
            let right = guidance_operand(&data["right"])?;
            let arrow = match guidance["maximize"].as_bool() {
                Some(true) => "↗",
                Some(false) => "↘",
                // No direction: show the operands without claiming one.
                None => return Some(format!("left={left} right={right}")),
            };
            Some(format!("{left} {arrow} {right}"))
        }
        Some("boolean") => {
            let propositions = data.as_object().filter(|map| !map.is_empty())?;
            let connective = if guidance["maximize"].as_bool()? {
                " && "
            } else {
                " || "
            };
            let terms: Option<Vec<String>> = propositions
                .iter()
                .map(|(name, value)| {
                    value
                        .as_bool()
                        .map(|value| format!("{}({value})", sanitize(name)))
                })
                .collect();
            Some(terms?.join(connective))
        }
        // Unknown guidance type: the raw data is the best available view.
        _ => render_details_json(data),
    }
}

/// One numeric guidance operand. The macros emit JSON numbers of the source
/// type; anything else (a shape this build does not know) renders as its
/// compact JSON rather than failing the whole expression.
fn guidance_operand(value: &Value) -> Option<String> {
    match value {
        Value::Number(n) => Some(n.to_string()),
        Value::Null => None,
        other => render_details_json(other),
    }
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

fn render_setup(setup: &Value, detail: bool) -> RenderedPayload {
    let headline = format!(
        "{} {}",
        style("SETUP").green().bold(),
        sanitize(setup["status"].as_str().unwrap_or("")),
    );
    // The details are user-controlled, arbitrarily nested JSON: never
    // flattened to key=value, shown as JSON under --detail.
    let details = if detail {
        render_details_json(&setup["details"])
            .map(|json| style(format!("details {json}")).dim().to_string())
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    RenderedPayload {
        headline: headline.trim_end().to_string(),
        details,
    }
}

fn render_fault(fault: &Value, detail: bool) -> RenderedPayload {
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
    let fault_details = &fault["details"];
    if let Some(disruption) = fault_details["disruption_type"].as_str() {
        bits.push(sanitize(disruption));
    }
    if fault_details["asymmetric"].as_bool() == Some(true) {
        bits.push("asymmetric".to_string());
    }
    if let Some(offset) = fault_details["offset"].as_f64() {
        bits.push(format!("offset={offset:+.2}s"));
    }
    if let Some(latency) = fault_details["latency"].as_object() {
        let mean = latency.get("mean").and_then(Value::as_f64).unwrap_or(0.0);
        let deviation = latency
            .get("deviation")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        bits.push(format!("latency={mean:.0}ms±{deviation:.0}"));
    }
    if let Some(drop_rate) = fault_details["drop_rate"].as_f64()
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
    let details = if detail {
        render_details_json(fault_details)
            .map(|json| style(format!("details {json}")).dim().to_string())
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };
    RenderedPayload { headline, details }
}

fn render_fault_injector_info(info: &Value) -> RenderedPayload {
    let mut text = format!(
        "fault-injector {}",
        sanitize(info["message"].as_str().unwrap_or("")),
    );
    if let Some(details) = info["details"].as_object() {
        for (key, value) in details {
            if let Some(rendered) = format_value(value) {
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
        && let Some(rendered) = format_value(code)
    {
        text.push_str(&format!(" exit={rendered}"));
    }
    // Deaths pop red; health probes are pure chatter (the record carries no
    // healthy/unhealthy verdict) and recede; the rest of the lifecycle stays
    // blue.
    let headline = match event {
        "died" | "kill" | "oom" => style(text).red().to_string(),
        event if event.starts_with("health_status") => style(text).dim().to_string(),
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

fn render_composer_task(entry: &Value, detail: bool) -> RenderedPayload {
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
            if detail {
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

fn render_composer_kv(obj: &Map<String, Value>, detail: bool) -> RenderedPayload {
    let pairs = obj.iter().filter_map(|(key, value)| {
        if LOG_ENVELOPE_KEYS.contains(&key.as_str()) {
            return None;
        }
        format_value(value).map(|rendered| (key, rendered))
    });
    if detail {
        // One pair per line, untruncated.
        let details = pairs
            .map(|(key, rendered)| {
                style(format!("{}={rendered}", sanitize(key)))
                    .dim()
                    .to_string()
            })
            .collect();
        RenderedPayload {
            headline: style("composer".to_string()).dim().to_string(),
            details,
        }
    } else {
        let mut text = "composer".to_string();
        for (key, rendered) in pairs {
            let truncated = console::truncate_str(&rendered, VALUE_TRUNCATE_WIDTH, "…");
            text.push_str(&format!(" {}={truncated}", sanitize(key)));
        }
        RenderedPayload::line(style(text).dim().to_string())
    }
}

/// Render a JSON value for a key=value cell. Scalars render bare, arrays of
/// scalars join with commas, and anything nested renders as its compact JSON
/// rather than being dropped — the value may be user-controlled and
/// arbitrarily shaped. `None` only for null and empties.
fn format_value(value: &Value) -> Option<String> {
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
            match scalars {
                Some(scalars) if scalars.is_empty() => None,
                Some(scalars) => Some(sanitize(&scalars.join(","))),
                // An array with nested members: compact JSON, whole.
                None => render_details_json(value),
            }
        }
        Value::Object(_) => render_details_json(value),
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
/// otherwise the source name with the `antithesis_` prefix dropped. The
/// stream (info/error) is deliberately not shown; `--json` carries it.
fn render_source(entry: &Value) -> String {
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");

    let label = if !container.trim().is_empty() {
        sanitize(container)
    } else {
        sanitize(name.trim().strip_prefix("antithesis_").unwrap_or(name))
    };
    format!("[{label}]")
}

// ---------------------------------------------------------------------------
// The stream renderer
// ---------------------------------------------------------------------------

/// Stateful renderer for one event stream. Feed it entries (and raw lines) in
/// stream order; it returns the exact text to print for each, dividers and
/// blank-line separation included.
pub(crate) struct EventStreamRenderer {
    detail: bool,
    last_input_hash: Option<String>,
    wrote_block: bool,
}

impl EventStreamRenderer {
    pub(crate) fn new(detail: bool) -> Self {
        Self {
            detail,
            last_input_hash: None,
            wrote_block: false,
        }
    }

    /// Render one NDJSON entry (vtime already normalized by the stream).
    /// The returned text may span several lines — a `moment` divider when the
    /// entry opens a new timeline segment, the event line, and any indented
    /// detail lines — and carries no trailing newline.
    pub(crate) fn render_entry(&mut self, entry: &Value) -> String {
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
            let divider = format!("moment {} {}", sanitize(hash), full_vtime(entry));
            out.push_str(&style(divider.trim_end().to_string()).yellow().to_string());
            out.push('\n');
            self.last_input_hash = Some(hash.to_string());
        }

        // Detail mode shows the exact vtime on every line; the default shows
        // the aligned 3-decimal truncation.
        let vtime = if self.detail {
            full_vtime(entry)
        } else {
            format_vtime_cell(entry)
        };
        let vtime_cell = format!("{vtime:>VTIME_WIDTH$}");
        let rendered = render_payload(entry, self.detail);
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

/// One build-log line, in the renderer's shared visual grammar: dim
/// timestamp, cyan bracketed source, sanitized text (SUT colors kept). Build
/// logs are wall-clock events with no moment, so there is no divider and no
/// classification — the stream label stands in for the source.
pub(crate) fn render_build_log_line(timestamp: &str, stream: &str, text: &str) -> String {
    format!(
        "{} {} {}",
        style(timestamp.to_string()).dim(),
        style(format!("[{}]", sanitize(stream))).cyan(),
        sanitize_log_text(text)
    )
    .trim_end()
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every assertion below is on the un-colored text: colors are a terminal
    /// affordance layered by `console`, which the test harness (a pipe, not a
    /// tty) disables. Force that off so a color-forcing environment can't
    /// flake the suite.
    fn renderer(detail: bool) -> EventStreamRenderer {
        console::set_colors_enabled(false);
        EventStreamRenderer::new(detail)
    }

    fn render_one(entry: Value) -> String {
        renderer(false).render_entry(&entry)
    }

    fn render_one_detailed(entry: Value) -> String {
        renderer(true).render_entry(&entry)
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
        // the event line shows the truncated vtime and the source without
        // its stream.
        assert_eq!(
            first,
            "moment -123 311.8487535319291\n 311.848  [app] starting"
        );

        // Same hash: no divider, no blank line.
        let second = r.render_entry(&json!({
            "moment": {"input_hash": "-123", "vtime": "312.0"},
            "source": {"container": "app", "name": "app", "stream": "out"},
            "output_text": "still here"
        }));
        assert_eq!(second, " 312.000  [app] still here");

        // New hash: blank line, then the next divider.
        let third = r.render_entry(&json!({
            "moment": {"input_hash": "456", "vtime": "313.5"},
            "source": {"container": "app", "name": "app", "stream": "out"},
            "output_text": "branched"
        }));
        assert_eq!(third, "\nmoment 456 313.5\n 313.500  [app] branched");
    }

    #[test]
    fn detail_mode_shows_the_full_vtime_on_every_line() {
        let block = render_one_detailed(json!({
            "moment": {"input_hash": "-123", "vtime": "311.8487535319291"},
            "source": {"container": "app", "name": "app"},
            "output_text": "starting"
        }));
        assert_eq!(
            block,
            "moment -123 311.8487535319291\n311.8487535319291  [app] starting"
        );
    }

    #[test]
    fn assert_verdicts_follow_the_family_rules() {
        let entry = |assert_type: &str, display_type: &str, hit: bool, condition: bool| {
            json!({
                "antithesis_assert": {
                    "assert_type": assert_type, "display_type": display_type,
                    "hit": hit, "condition": condition, "message": "m"
                },
                "source": {"container": "app"},
                "moment": {"input_hash": "-1", "vtime": "1.0"}
            })
        };
        // hit:false is a catalog registration, whatever the family.
        let block = render_one(entry("always", "AlwaysOrUnreachable", false, false));
        assert!(
            block.ends_with(r#"[app] CATALOG AlwaysOrUnreachable "m""#),
            "got: {block}"
        );
        // A failing always is the FAIL badge; a passing one says HIT.
        let block = render_one(entry("always", "Always", true, false));
        assert!(block.ends_with(r#"[app] FAIL Always "m""#), "got: {block}");
        let block = render_one(entry("always", "Always", true, true));
        assert!(block.ends_with(r#"[app] HIT Always "m""#), "got: {block}");
        // A hit unreachable is a failure regardless of condition.
        let block = render_one(entry("reachability", "Unreachable", true, true));
        assert!(
            block.ends_with(r#"[app] FAIL Unreachable "m""#),
            "got: {block}"
        );
        // sometimes/reachable evaluations are informational HITs — never FAIL,
        // whatever the condition.
        let block = render_one(entry("sometimes", "Sometimes", true, false));
        assert!(
            block.ends_with(r#"[app] HIT Sometimes "m""#),
            "got: {block}"
        );
        let block = render_one(entry("reachability", "Reachable", true, true));
        assert!(
            block.ends_with(r#"[app] HIT Reachable "m""#),
            "got: {block}"
        );
    }

    #[test]
    fn assert_location_and_details_appear_only_in_detail_mode() {
        let entry = json!({
            "antithesis_assert": {
                "assert_type": "always", "display_type": "Always",
                "hit": true, "condition": false,
                "message": "Counter's value retrieved",
                "location": {"begin_line": 87, "file": "/go/src/antithesis/control/control.go", "function": "get"},
                "details": {"left": 7, "right": {"limit": 5}}
            },
            "source": {"container": "control", "name": "control"},
            "moment": {"input_hash": "-1", "vtime": "311.8487535319291"}
        });
        // Default: one line, no location, no details, no must-hit.
        let block = render_one(entry.clone());
        assert_eq!(
            block.lines().nth(1).unwrap(),
            " 311.848  [control] FAIL Always \"Counter's value retrieved\""
        );
        assert_eq!(block.lines().count(), 2, "got: {block}");

        // Detail: location line and the attached details JSON.
        let block = render_one_detailed(entry);
        let mut lines = block.lines().skip(2);
        assert_eq!(lines.next().unwrap(), "          @ control.go:get:87");
        assert_eq!(
            lines.next().unwrap(),
            r#"          details {"left":7,"right":{"limit":5}}"#
        );
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
    fn guidance_catalog_entries_render_dim_without_data() {
        let entry = json!({
            "antithesis_guidance": {
                "guidance_type": "numeric", "maximize": true, "hit": false,
                "message": "wal grew long", "guidance_data": null,
                "location": {"begin_line": 439, "file": "src/actions.rs", "function": "checkpoint"}
            },
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        });
        let block = render_one(entry.clone());
        assert!(
            block.ends_with(r#"[w] CATALOG GUIDANCE "wal grew long""#),
            "got: {block}"
        );
        assert_eq!(block.lines().count(), 2, "got: {block}");

        // --detail adds the location; there is no data on a registration.
        let block = render_one_detailed(entry);
        assert!(
            block.ends_with("          @ actions.rs:checkpoint:439"),
            "got: {block}"
        );
    }

    #[test]
    fn numeric_guidance_renders_the_drive_direction() {
        let guidance = |maximize: Value| {
            render_one(json!({
                "antithesis_guidance": {
                    "guidance_type": "numeric", "maximize": maximize, "hit": true,
                    "message": "Positive x", "id": "Positive x",
                    "guidance_data": {"left": -3, "right": 0}
                },
                "source": {"container": "w", "name": "w"},
                "moment": {"input_hash": "-1", "vtime": "2.0"}
            }))
        };
        // The arrow is the explorer's push on `left` relative to `right`:
        // up when maximizing, down when minimizing. No inequality is claimed
        // — its strictness and direction are not recoverable from the event.
        let block = guidance(json!(true));
        assert!(
            block.ends_with(r#"GUIDANCE "Positive x": -3 ↗ 0"#),
            "got: {block}"
        );
        let block = guidance(json!(false));
        assert!(
            block.ends_with(r#"GUIDANCE "Positive x": -3 ↘ 0"#),
            "got: {block}"
        );
        // Without a direction, show the operands without claiming one.
        let block = guidance(json!(null));
        assert!(
            block.ends_with(r#"GUIDANCE "Positive x": left=-3 right=0"#),
            "got: {block}"
        );
    }

    #[test]
    fn boolean_guidance_renders_propositions_with_the_connective() {
        let guidance = |maximize: bool, data: Value| {
            render_one(json!({
                "antithesis_guidance": {
                    "guidance_type": "boolean", "maximize": maximize, "hit": true,
                    "message": "m", "guidance_data": data
                },
                "source": {"container": "w", "name": "w"},
                "moment": {"input_hash": "-1", "vtime": "1.0"}
            }))
        };
        // maximize picks the connective; no assertion lookup is needed.
        let block = guidance(false, json!({"queue_empty": false, "worker_idle": false}));
        assert!(
            block.ends_with(r#"GUIDANCE "m": queue_empty(false) || worker_idle(false)"#),
            "got: {block}"
        );
        let block = guidance(true, json!({"acked": true, "durable": false}));
        assert!(
            block.ends_with(r#"GUIDANCE "m": acked(true) && durable(false)"#),
            "got: {block}"
        );
    }

    #[test]
    fn guidance_detail_mode_keeps_the_raw_data_and_location() {
        let mut r = renderer(true);
        let block = r.render_entry(&json!({
            "antithesis_guidance": {
                "guidance_type": "numeric", "maximize": true, "hit": true,
                "message": "wal grew long",
                "guidance_data": {"left": 48, "right": 1000},
                "location": {"begin_line": 439, "file": "src/actions.rs", "function": "checkpoint"}
            },
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        let mut lines = block.lines().skip(2);
        assert_eq!(
            lines.next().unwrap(),
            r#"          numeric maximize {"left":48,"right":1000}"#
        );
        assert_eq!(
            lines.next().unwrap(),
            "          @ actions.rs:checkpoint:439"
        );
    }

    #[test]
    fn renders_sdk_and_setup_events() {
        let sdk = render_one(json!({
            "antithesis_sdk": {"language": {"name": "Rust", "version": "1.97.1"}, "protocol_version": "1.1.0", "sdk_version": "0.2.9"},
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            sdk.ends_with("SDK connected: Rust 1.97.1 (sdk 0.2.9, protocol 1.1.0)"),
            "got: {sdk}"
        );

        // The setup details are arbitrary user JSON: never flattened into the
        // headline, shown as JSON only under --detail.
        let entry = json!({
            "antithesis_setup": {"status": "complete", "details": {"db": "/data/test.db", "nested": {"a": 1}}},
            "source": {"container": "w", "name": "w"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        });
        let setup = render_one(entry.clone());
        assert!(setup.ends_with("SETUP complete"), "got: {setup}");
        let setup = render_one_detailed(entry);
        assert!(
            setup.ends_with(r#"          details {"db":"/data/test.db","nested":{"a":1}}"#),
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

        // --detail adds the fault's raw details JSON.
        let detailed = render_one_detailed(json!({
            "fault": {
                "name": "skip", "type": "clock",
                "details": {"offset": -1.5},
                "affected_nodes": [], "max_duration": 1.0
            },
            "source": {"name": "fault_injector"},
            "moment": {"input_hash": "-1", "vtime": "87.4"}
        }));
        assert!(
            detailed.ends_with(r#"          details {"offset":-1.5}"#),
            "got: {detailed}"
        );
    }

    #[test]
    fn renders_container_lifecycle_with_dim_health_probes() {
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

        // Health probes carry no verdict and recede as dim chatter (the text
        // survives; only the styling changes, invisible with colors off).
        let health = render_one(json!({
            "event": "health_status", "name": "sqlite-init",
            "source": {"name": "containers_meta"},
            "moment": {"input_hash": "-1", "vtime": "12.8"}
        }));
        assert!(
            health.ends_with("container health_status sqlite-init"),
            "got: {health}"
        );
    }

    #[test]
    fn composer_captured_output_appears_only_in_detail_mode() {
        let entry = json!({
            "task_status": "finished", "command": "git_walk/parallel_driver_walk",
            "command_return_code": "1", "command_runtime": "0.03181171417236328",
            "additional_stdout": "", "additional_stderr": "boom\nsecond line",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "16.1"}
        });
        // Default: the one-line summary only.
        let block = render_one(entry.clone());
        assert_eq!(
            block.lines().nth(1).unwrap(),
            "  16.100  [test_composer] task finished git_walk/parallel_driver_walk exit=1 in 0.0s"
        );
        assert_eq!(block.lines().count(), 2, "got: {block}");

        // Detail: stderr lines surface as indented details; empty stdout is
        // omitted.
        let block = render_one_detailed(entry);
        let mut lines = block.lines().skip(2);
        assert_eq!(lines.next().unwrap(), "          err| boom");
        assert_eq!(lines.next().unwrap(), "          err| second line");

        let started = render_one(json!({
            "task_status": "started", "command": "git_walk/parallel_driver_walk",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "15.3"}
        }));
        assert!(
            started.ends_with("[test_composer] task started git_walk/parallel_driver_walk"),
            "got: {started}"
        );
    }

    #[test]
    fn composer_chatter_truncates_inline_and_expands_under_detail() {
        let entry = json!({
            "new_command_path": "git_walk/anytime_invariants",
            "container_id": "22453394531ae33a6df72b8119fb9fd8338f8854d6a271fe736417fb35975013",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "13.3"}
        });
        let block = render_one(entry.clone());
        assert!(
            block.contains("composer new_command_path=git_walk/anytime_invariants"),
            "got: {block}"
        );
        // The 64-hex container id is truncated for the eye.
        assert!(
            block.contains("container_id=22453394531ae33a6df72b8119fb9fd8338f885…"),
            "got: {block}"
        );

        // --detail: one pair per line, untruncated.
        let block = render_one_detailed(entry);
        let mut lines = block.lines().skip(1);
        assert!(
            lines.next().unwrap().ends_with("[test_composer] composer"),
            "got: {block}"
        );
        assert_eq!(
            lines.next().unwrap(),
            "          new_command_path=git_walk/anytime_invariants"
        );
        assert_eq!(
            lines.next().unwrap(),
            "          container_id=22453394531ae33a6df72b8119fb9fd8338f8854d6a271fe736417fb35975013"
        );
    }

    #[test]
    fn kv_values_render_nested_json_rather_than_dropping_it() {
        // User-controlled values are not always scalars; a nested value shows
        // as its compact JSON instead of vanishing.
        assert_eq!(
            format_value(&json!({"paused": {"since": 3}})),
            Some(r#"{"paused":{"since":3}}"#.to_string())
        );
        assert_eq!(
            format_value(&json!([1, {"a": 2}])),
            Some(r#"[1,{"a":2}]"#.to_string())
        );
        assert_eq!(format_value(&json!(["a", "b"])), Some("a,b".to_string()));
        assert_eq!(format_value(&json!(null)), None);
        assert_eq!(format_value(&json!({})), None);
    }

    #[test]
    fn log_text_keeps_sgr_colors_but_nothing_else() {
        console::set_colors_enabled(false);
        // Colors off (a pipe): everything is stripped and controls escaped.
        assert_eq!(
            sanitize_log_text("\x1B[31mred\x1B[0m\u{0008}done"),
            r"red\x08done"
        );

        // Colors on: SGR passes through with a trailing reset; cursor moves,
        // OSC retitles, and other escapes are still dropped, and stray
        // control bytes are still escaped.
        assert_eq!(
            sanitize_log_text_inner("\x1B[31mred\x1B[0m plain", true),
            "\x1B[31mred\x1B[0m plain\x1B[0m"
        );
        assert_eq!(
            sanitize_log_text_inner("a\x1B[2Ab\x1B]0;title\x07c\rd", true),
            r"abc\rd"
        );
        // is_sgr admits only `ESC[…m` with digit/semicolon params.
        assert!(is_sgr("\x1b[31;1m"));
        assert!(is_sgr("\x1b[m"));
        assert!(!is_sgr("\x1b[2A"));
        assert!(!is_sgr("\x1b[?25lm"));
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
    fn source_label_prefers_container_and_never_shows_the_stream() {
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
        // The stream is not part of the label; --json carries it.
        assert_eq!(source("app", "app", Some("error")), "[app]");
        assert_eq!(
            source("", "antithesis_test_composer", Some("info")),
            "[test_composer]"
        );
    }

    #[test]
    fn build_log_lines_share_the_visual_grammar() {
        console::set_colors_enabled(false);
        assert_eq!(
            render_build_log_line("2025-03-20 02:01:12", "out", "pulling image\r"),
            r"2025-03-20 02:01:12 [out] pulling image\r"
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
    fn catalog_entries_classify_without_a_condition_and_fall_back_to_id() {
        // A registration (`hit: false`) carries no meaningful condition, and
        // some payloads carry only `id`; neither gap may knock the event down
        // to the raw-JSON fallback.
        let block = render_one(json!({
            "antithesis_assert": {
                "assert_type": "sometimes", "display_type": "Sometimes",
                "hit": false, "id": "precept fault: before prepare"
            },
            "source": {"container": "client"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            block.ends_with(r#"[client] CATALOG Sometimes "precept fault: before prepare""#),
            "got: {block}"
        );

        // A hit evaluation without a condition is still unclassifiable.
        let entry = json!({
            "antithesis_assert": {
                "assert_type": "always", "display_type": "Always",
                "hit": true, "message": "m"
            }
        });
        assert_eq!(parse_assertion_summary(&entry), None);
    }

    #[test]
    fn assertion_summary_prefers_display_type_and_renders_partial_locations() {
        let entry = json!({
            "antithesis_assert": {
                "hit": false, "condition": false,
                "message": "setup reached", "assert_type": "reachability",
                "display_type": "SetupReached",
                "location": {"function": "run_setup", "begin_line": 42}
            }
        });
        let summary = parse_assertion_summary(&entry).unwrap();
        assert_eq!(summary.verdict, AssertVerdict::Catalog);
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
        assert_eq!(summary.verdict, AssertVerdict::Hit);
        assert_eq!(summary.location, None);
        assert_eq!(summary.details, None);
    }
}
