//! The one renderer for human-facing event streams (`runs logs`,
//! `runs events`, `runs search`, `runs build-logs`, and any future stream of
//! events).
//!
//! Every stream entry is classified into an event type and rendered as a
//! concise block, git-log style: a yellow `moment HASH VTIME` divider opens
//! each timeline segment (the moment is the commit, its events are the body),
//! one line per event beneath it, and dim indented detail lines where a kind
//! has more to say. Blocks for consecutive segments are separated by a blank
//! line. Colors go through [`console::style`], which disables itself off-tty
//! and under `NO_COLOR`.
//!
//! Each event type lives in its own submodule and implements [`Event`],
//! which couples the type's classification (does this entry have my shape?)
//! to its rendering. [`ansi`] owns the ANSI-escape handling the log-text
//! kinds share.
//!
//! Two rendering depths:
//! - default: one line per event. Antithesis event shapes (SDK assertions,
//!   guidance, fault injections, container lifecycle, test composer chatter)
//!   each render in their own concise form; everything else falls back to
//!   the log-text or raw-JSON renderer.
//! - detail (`--detail`): a full-width vtime cell that holds every moment a
//!   real run reports whole (see [`VTIME_WIDTH_DETAIL`]), assertion and
//!   guidance source locations, the payload's attached `details` JSON, the
//!   composer's captured stdout/stderr, and composer chatter expanded to one
//!   key=value per line, untruncated.
//!
//! There is no "raw" rendering here: `--raw` on the commands requires
//! `--json` and passes the server's NDJSON stream through verbatim, without
//! touching this module.
//!
//! The renderer also owns the display conventions the streams share: vtime is
//! normalized through [`VTime`] and shown in a fixed-width cell, truncated —
//! never rounded — so a value copied off the screen and pasted back lands on,
//! never past, the line you saw; the divider carries the segment's
//! full-precision moment for exact `runs logs`/`runs exec`/`snouty debug`
//! follow-ups.
//!
//! Rendering writes into a caller-supplied buffer (see [`Block`]), in the
//! style of `Display`/`Debug` formatters: one pass over each input, no
//! intermediate per-piece strings.

mod ansi;
mod assert;
mod composer;
mod container;
mod fault;
mod guidance;
mod log;
mod sdk;

pub(crate) use ansi::{normalize_terminal_text, strip_ansi};

use std::fmt::{self, Write};

use console::style;
use serde_json::{Map, Value};

use crate::render::sanitize;
use crate::time::format_local_str;
use crate::vtime::VTime;

/// The vtime cell is a fixed character budget, truncated — never rounded — so
/// a value copied off the screen and pasted back as `--begin-vtime` lands on,
/// never past, the line you saw. The default 8 columns hold `"1234.567"`,
/// which covers runs up to ~9999 vsec at millisecond resolution; a wider
/// integer part eats into the fraction rather than widening every line.
const VTIME_WIDTH: usize = 8;

/// `--detail` shows the vtime whole from one second up, which is every moment
/// a real run reports: an `f64` round-trips in at most 17 significant digits,
/// and a vtime tops out at 2^21 seconds (2^53 ticks, ~24.3 days), so those 17
/// digits plus the decimal point are the widest such a value can print — e.g.
/// `"2097151.9999999998"`. Below one second the leading zeros are not
/// significant digits and the text can outrun the cell
/// (`"0.0009999999310821295"`), so a sub-second vtime truncates here.
const VTIME_WIDTH_DETAIL: usize = 18;

/// The vtime column for a rendering depth. Detail lines indent under the
/// source column, which sits one gap past this.
const fn vtime_width(detail: bool) -> usize {
    if detail {
        VTIME_WIDTH_DETAIL
    } else {
        VTIME_WIDTH
    }
}

/// The gap between the vtime cell and the source cell.
const VTIME_GAP: usize = 2;

/// Long opaque values (container ids, image digests) are truncated to this
/// many columns in one-line key=value renderings; `--detail` and `--json`
/// carry the full value.
const VALUE_TRUNCATE_WIDTH: usize = 40;

/// Source labels longer than this (generated k8s pod names run 60+ columns)
/// are truncated in the event line, with the full value on a dim detail line
/// beneath. Wide enough that compose container names and script paths fit
/// whole; `--detail` keeps the label inline and untruncated.
const SOURCE_TRUNCATE_WIDTH: usize = 40;

// ---------------------------------------------------------------------------
// The write target
// ---------------------------------------------------------------------------

/// The write target for one event's block. The headline continues the
/// event's own line — write it through `std::fmt::Write` — and each call to
/// [`Block::detail_line`] adds a dim line beneath it, indented under the
/// source column. `--detail` is exposed as [`Block::detail`], so a kind with
/// more to say can decide what to add.
///
/// The block owns the layout invariants so no kind has to: the headline's
/// trailing whitespace is trimmed exactly once (when the first detail line
/// starts, or at [`Block::finish`]), and a truncated source label's full
/// value is held back to lead the detail lines, right under the cell it
/// overflowed.
struct Block<'a> {
    out: &'a mut String,
    detail: bool,
    /// A truncated source's `field=full-label`, still owed to the output.
    overflow: Option<String>,
    headline_done: bool,
}

impl fmt::Write for Block<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.out.write_str(s)
    }
}

impl Block<'_> {
    fn detail(&self) -> bool {
        self.detail
    }

    /// How far a detail line indents to sit under the source column. The
    /// vtime cell is narrower in the default depth, so this follows it.
    fn indent(&self) -> usize {
        vtime_width(self.detail) + VTIME_GAP
    }

    /// One dim detail line under the headline.
    fn detail_line(&mut self, content: impl fmt::Display) -> fmt::Result {
        self.end_headline();
        let indent = self.indent();
        if let Some(overflow) = self.overflow.take() {
            write!(self.out, "\n{:indent$}{}", "", style(overflow).dim())?;
        }
        write!(self.out, "\n{:indent$}{}", "", style(content).dim())
    }

    /// The dim `details {json}` line every kind with attached user JSON
    /// shares: only under `--detail`, and only when there is something to
    /// read.
    fn details_json(&mut self, value: &Value) -> fmt::Result {
        if !self.detail {
            return Ok(());
        }
        match render_details_json(value) {
            Some(json) => self.detail_line(format_args!("details {json}")),
            None => Ok(()),
        }
    }

    /// The dim `@ location` source-location line the assertion-shaped kinds
    /// share: only under `--detail`.
    fn location_line(&mut self, location: &str) -> fmt::Result {
        if !self.detail {
            return Ok(());
        }
        self.detail_line(format_args!("@ {location}"))
    }

    /// Close the block: trim the headline if nothing did yet, and emit a
    /// still-pending source overflow even when no detail line followed.
    fn finish(mut self) -> fmt::Result {
        self.end_headline();
        let indent = self.indent();
        if let Some(overflow) = self.overflow.take() {
            write!(self.out, "\n{:indent$}{}", "", style(overflow).dim())?;
        }
        Ok(())
    }

    /// Trim the headline's trailing whitespace, exactly once. Detail lines
    /// never end in whitespace (they close with the style reset), so the
    /// trim can only ever touch the headline.
    fn end_headline(&mut self) {
        if !self.headline_done {
            self.out.truncate(self.out.trim_end().len());
            self.headline_done = true;
        }
    }
}

/// Render a closure through `Display`, so a styled span can be written
/// incrementally without building an intermediate `String` (the same trick
/// as `format_args!` and serde's `collect_str`).
struct DisplayWith<F: Fn(&mut fmt::Formatter<'_>) -> fmt::Result>(F);

impl<F: Fn(&mut fmt::Formatter<'_>) -> fmt::Result> fmt::Display for DisplayWith<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.0)(f)
    }
}

// ---------------------------------------------------------------------------
// Classification + rendering
// ---------------------------------------------------------------------------

/// One event shape: how to recognize it in a stream entry, and how to write
/// its block. Classifiers are deliberately shallow — a key's presence, not
/// its schema — so a shape the server grows fields on still classifies; an
/// entry no type claims falls back to its raw payload JSON in
/// [`render_payload`].
trait Event<'a>: Sized {
    /// `Some` when `entry` is this shape.
    fn classify(entry: &'a Value) -> Option<Self>;
    /// Write the event's headline (and any detail lines) into the block.
    fn render(&self, block: &mut Block<'_>) -> fmt::Result;
}

fn render_payload(entry: &Value, block: &mut Block<'_>) -> fmt::Result {
    macro_rules! dispatch {
        ($($ty:ty),* $(,)?) => {
            $(if let Some(event) = <$ty>::classify(entry) {
                return event.render(block);
            })*
        };
    }
    // The order is load-bearing: SDK shapes before the injector's, plain log
    // text before the composer's structured chatter (a composer record with
    // `output_text` is a log line), and the raw-JSON fallback last.
    dispatch!(
        assert::Assertion,
        guidance::Guidance,
        sdk::Sdk,
        sdk::Setup,
        fault::Fault,
        fault::InjectorInfo,
        container::Lifecycle,
        log::Log,
        composer::Task,
        composer::Chatter,
    );
    // Anything else — an unknown structured payload, or a row reshaped by an
    // event-set DSL pipeline. Its raw payload JSON is the rendering.
    write!(
        block,
        "{}",
        style(sanitize(&strip_log_envelope(entry))).dim()
    )
}

// ---------------------------------------------------------------------------
// Shared payload helpers
// ---------------------------------------------------------------------------

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

/// A rendered source label: the bracketed cell for the event line, plus the
/// full value for a detail line when the cell had to truncate.
struct RenderedSource {
    cell: String,
    /// `field=full-label`, present only when the cell is truncated.
    overflow: Option<String>,
}

/// The bracketed source label: the container when the record names one,
/// otherwise the source name with the `antithesis_` prefix dropped. The
/// stream (info/error) is deliberately not shown; `--json` carries it.
///
/// A label past [`SOURCE_TRUNCATE_WIDTH`] (generated k8s pod names) truncates
/// in the cell and surfaces whole on a detail line, named by the field it
/// came from — unless `detail`, which keeps every value inline and whole.
fn render_source(entry: &Value, detail: bool) -> RenderedSource {
    let container = entry["source"]["container"].as_str().unwrap_or("");
    let name = entry["source"]["name"].as_str().unwrap_or("");

    let (field, label) = if !container.trim().is_empty() {
        ("container", sanitize(container))
    } else {
        (
            "name",
            sanitize(name.trim().strip_prefix("antithesis_").unwrap_or(name)),
        )
    };
    let truncated = console::truncate_str(&label, SOURCE_TRUNCATE_WIDTH, "…");
    if detail || truncated == label {
        return RenderedSource {
            cell: format!("[{label}]"),
            overflow: None,
        };
    }
    RenderedSource {
        cell: format!("[{truncated}]"),
        overflow: Some(format!("{field}={label}")),
    }
}

// ---------------------------------------------------------------------------
// The stream renderer
// ---------------------------------------------------------------------------

/// Stateful renderer for one event stream. Feed it entries in stream order;
/// it writes the exact text to print for each — dividers and blank-line
/// separation included, no trailing newline — into the buffer the caller
/// provides.
pub(crate) struct EventStreamRenderer {
    detail: bool,
    /// The current segment's hash; `Some` also means at least one block was
    /// written, so the next divider needs a blank line above it.
    last_input_hash: Option<String>,
    /// The full label of the last truncated source whose overflow line was
    /// emitted; consecutive events from the same source don't repeat it.
    last_overflow: Option<String>,
}

impl EventStreamRenderer {
    pub(crate) fn new(detail: bool) -> Self {
        Self {
            detail,
            last_input_hash: None,
            last_overflow: None,
        }
    }

    /// Render one NDJSON entry (vtime already normalized by the stream) into
    /// `out`. The written text may span several lines — a `moment` divider
    /// when the entry opens a new timeline segment, the event line, and any
    /// indented detail lines.
    pub(crate) fn render_entry(&mut self, entry: &Value, out: &mut String) -> fmt::Result {
        // A full event carries both its moment and its source envelope. A
        // row reshaped by an event-set DSL pipeline can lack either (narrow
        // can keep `moment` while dropping the rest); rendering it through
        // the event form would produce a half-empty hybrid line, so its JSON
        // is the row — print it as such. A build-log record (`runs
        // build-logs`) has no moment either — a wall-clock
        // `timestamp`/`stream`/`text` triple instead — and renders in the
        // shared visual grammar without a divider or classification.
        let hash = entry["moment"]["input_hash"].as_str();
        let Some(hash) = hash.filter(|_| entry.get("source").is_some()) else {
            if let (Some(timestamp), Some(text)) =
                (entry["timestamp"].as_str(), entry["text"].as_str())
            {
                // `stream` is optional in the build-log schema.
                let stream = entry["stream"].as_str().unwrap_or("out");
                log::render_build_log(out, &format_local_str(timestamp), stream, text)?;
            } else {
                write!(out, "{}", sanitize(&entry.to_string()))?;
            }
            out.truncate(out.trim_end().len());
            return Ok(());
        };

        // The stream normalizes this to a JSON number, but an unparsable
        // vtime is left as the server sent it — so this can still be `None`,
        // and the cell below has to say so rather than print server text.
        let vtime = VTime::from_json(&entry["moment"]["vtime"]);

        if self.last_input_hash.as_deref() != Some(hash) {
            if self.last_input_hash.is_some() {
                out.push('\n');
            }
            // The divider carries the segment's full-precision moment —
            // exactly what `runs logs`/`runs exec`/`snouty debug` take.
            let divider = DisplayWith(|f: &mut fmt::Formatter<'_>| {
                write!(f, "moment {}", sanitize(hash))?;
                if let Some(vtime) = vtime.as_ref() {
                    write!(f, " {vtime}")?;
                }
                Ok(())
            });
            writeln!(out, "{}", style(divider).yellow())?;
            self.last_input_hash = Some(hash.to_string());
        }

        let source = render_source(entry, self.detail);
        // A truncated source's full value is worth one detail line per
        // contiguous run of events from that source, not one per event — a
        // long-named pod would otherwise double its whole stream.
        let overflow = match source.overflow {
            Some(overflow) if self.last_overflow.as_deref() == Some(overflow.as_str()) => None,
            overflow => {
                self.last_overflow.clone_from(&overflow);
                overflow
            }
        };
        write!(
            out,
            "{}  {} ",
            style(DisplayWith(|f: &mut fmt::Formatter<'_>| {
                let width = vtime_width(self.detail);
                match vtime {
                    Some(vtime) => write!(f, "{vtime:<width$.width$}"),
                    None => write!(f, "{:<width$}", "NULL"),
                }
            }))
            .dim(),
            style(&source.cell).cyan(),
        )?;
        let mut block = Block {
            out,
            detail: self.detail,
            // The truncated source's full value leads the detail lines,
            // right under the cell it overflowed.
            overflow,
            headline_done: false,
        };
        render_payload(entry, &mut block)?;
        block.finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test helpers the per-kind modules share: render entries through the full
/// renderer, colors off.
#[cfg(test)]
pub(super) mod testkit {
    use super::EventStreamRenderer;
    use serde_json::Value;

    /// Every rendering assertion is on the un-colored text: colors are a
    /// terminal affordance layered by `console`, which the test harness (a
    /// pipe, not a tty) disables. Force that off so a color-forcing
    /// environment can't flake the suite.
    pub(crate) fn renderer(detail: bool) -> EventStreamRenderer {
        console::set_colors_enabled(false);
        EventStreamRenderer::new(detail)
    }

    pub(crate) fn render_entry(renderer: &mut EventStreamRenderer, entry: &Value) -> String {
        let mut out = String::new();
        renderer
            .render_entry(entry, &mut out)
            .expect("writing to a String cannot fail");
        out
    }

    pub(crate) fn render_one(entry: Value) -> String {
        render_entry(&mut renderer(false), &entry)
    }

    pub(crate) fn render_one_detailed(entry: Value) -> String {
        render_entry(&mut renderer(true), &entry)
    }
}

#[cfg(test)]
mod tests {
    use super::testkit::*;
    use super::*;
    use serde_json::json;

    #[test]
    fn opens_each_timeline_segment_with_a_full_precision_moment_divider() {
        let mut r = renderer(false);
        let first = render_entry(
            &mut r,
            &json!({
                "moment": {"input_hash": "-123", "vtime": "311.8487535319291"},
                "source": {"container": "app", "name": "app", "stream": "out"},
                "output_text": "starting"
            }),
        );
        // The divider carries the exact moment (pasteable into `runs logs`);
        // the event line shows the vtime truncated to the cell width and the
        // source without its stream.
        assert_eq!(
            first,
            "moment -123 311.8487535319291\n311.8487  [app] starting"
        );

        // Same hash: no divider, no blank line.
        let second = render_entry(
            &mut r,
            &json!({
                "moment": {"input_hash": "-123", "vtime": "312.0"},
                "source": {"container": "app", "name": "app", "stream": "out"},
                "output_text": "still here"
            }),
        );
        assert_eq!(second, "312.0     [app] still here");

        // New hash: blank line, then the next divider.
        let third = render_entry(
            &mut r,
            &json!({
                "moment": {"input_hash": "456", "vtime": "313.5"},
                "source": {"container": "app", "name": "app", "stream": "out"},
                "output_text": "branched"
            }),
        );
        assert_eq!(third, "\nmoment 456 313.5\n313.5     [app] branched");
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
            "moment -123 311.8487535319291\n311.8487535319291   [app] starting"
        );
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
        let row = render_entry(&mut r, &json!({"count": 3, "container": "etcd0"}));
        assert_eq!(row, r#"{"count":3,"container":"etcd0"}"#);
    }

    #[test]
    fn source_label_prefers_container_and_never_shows_the_stream() {
        let source = |container: &str, name: &str, stream: Option<&str>| {
            let mut entry = json!({"source": {"container": container, "name": name}});
            if let Some(stream) = stream {
                entry["source"]["stream"] = json!(stream);
            }
            render_source(&entry, false).cell
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
    fn long_sources_truncate_with_the_full_value_on_a_detail_line() {
        console::set_colors_enabled(false);
        let pod = "dynamo-platform-dynamo-operator-controller-manager-7c94cc7dfxqh/manager";
        let entry = json!({
            "output_text": "Applied CRD",
            "source": {"container": pod, "name": pod},
            "moment": {"input_hash": "-1", "vtime": "25.6"}
        });
        let block = render_entry(&mut renderer(false), &entry);
        let mut lines = block.lines().skip(1);
        assert_eq!(
            lines.next().unwrap(),
            "25.6      [dynamo-platform-dynamo-operator-control…] Applied CRD"
        );
        assert_eq!(lines.next().unwrap(), format!("          container={pod}"));

        // --detail keeps the label inline and whole.
        let block = render_entry(&mut renderer(true), &entry);
        assert!(
            block.contains(&format!("[{pod}] Applied CRD")),
            "got: {block}"
        );
        assert!(!block.contains("container="), "got: {block}");

        // A label that fits stays whole with no overflow line.
        let block = render_entry(
            &mut renderer(false),
            &json!({
                "output_text": "hi",
                "source": {"container": "bank/parallel_driver_tx.sh"},
                "moment": {"input_hash": "-1", "vtime": "1.0"}
            }),
        );
        assert!(
            block.ends_with("[bank/parallel_driver_tx.sh] hi"),
            "got: {block}"
        );
    }

    #[test]
    fn an_unparsable_vtime_renders_as_a_placeholder() {
        // A vtime the stream could not normalize is never displayed as the
        // server's own text: the cell holds a placeholder and the divider
        // drops the vtime, so escape bytes carried in the value have no path
        // to the terminal.
        let block = render_one(json!({
            "moment": {"input_hash": "-1", "vtime": "1.0\u{1b}[2J"},
            "source": {"container": "app"},
            "output_text": "hi"
        }));
        assert_eq!(block, "moment -1\nNULL      [app] hi");
        assert!(!block.contains('\u{1b}'), "escape byte leaked: {block:?}");
    }

    #[test]
    fn source_overflow_prints_once_per_contiguous_source_run() {
        let long = "dynamo-platform-vllm-v1-agg-router-6cf9b55458-7c94cc7dfxqh/main";
        let mut r = renderer(false);
        let entry = json!({
            "moment": {"input_hash": "-1", "vtime": "1.0"},
            "source": {"container": long},
            "output_text": "hi"
        });
        let first = render_entry(&mut r, &entry);
        assert!(first.contains("container="), "got: {first}");
        // The same source again: the cell still truncates, but the full
        // value does not repeat.
        let second = render_entry(&mut r, &entry);
        assert!(!second.contains("container="), "got: {second}");
        // A different source in between resets the memo.
        render_entry(
            &mut r,
            &json!({
                "moment": {"input_hash": "-1", "vtime": "1.1"},
                "source": {"container": "app"},
                "output_text": "hi"
            }),
        );
        let again = render_entry(&mut r, &entry);
        assert!(again.contains("container="), "got: {again}");
    }
}
