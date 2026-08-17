//! SDK assertions (`antithesis_assert`): each evaluation renders with a
//! verdict badge, and the SDK's startup registrations render as dim
//! `CATALOG` chatter.

use std::fmt::{self, Write};
use std::path::Path;

use console::style;
use serde::Deserialize;
use serde_json::Value;

use crate::render::sanitize;

use super::{Block, DisplayWith, Event, render_details_json};

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
pub(super) struct AssertionLocation {
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
        if assert_type.eq_ignore_ascii_case("unreachable")
            || display_type.eq_ignore_ascii_case("unreachable")
        {
            return Self::Fail;
        }
        // `always` covers AlwaysOrUnreachable too (its assert_type is
        // "always"); everything else (sometimes, reachability, unknown
        // future types) is informational.
        let always_prefixed = display_type
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("always"));
        if assert_type.eq_ignore_ascii_case("always") || always_prefixed {
            return if condition { Self::Pass } else { Self::Fail };
        }
        Self::Hit
    }
}

impl AssertionSummary {
    fn from_payload(payload: AssertionPayload) -> Option<Self> {
        let hit = payload.hit?;
        // `condition` only means something on an evaluation; a catalog
        // registration (`hit: false`) classifies without one, so a missing
        // condition must not knock it down to the raw-JSON fallback.
        let condition = match payload.condition {
            Some(condition) => condition,
            None if !hit => false,
            None => return None,
        };
        let message = payload
            .message
            .or(payload.id)
            .map(|message| message.trim().to_string())
            .filter(|message| !message.is_empty())?;
        let assert_type = payload.assert_type.unwrap_or_default();
        let display_type = payload.display_type.unwrap_or_default();
        let label = Some(display_type.trim())
            .filter(|label| !label.is_empty())
            .or_else(|| Some(assert_type.trim()).filter(|label| !label.is_empty()))?
            .to_string();

        Some(Self {
            verdict: AssertVerdict::classify(hit, condition, &assert_type, &display_type),
            label,
            message,
            location: payload.location.and_then(render_assertion_location),
            details: payload.details.as_ref().and_then(render_details_json),
        })
    }
}

pub(super) struct Assertion(AssertionSummary);

fn parse_assertion_summary(entry: &Value) -> Option<AssertionSummary> {
    let payload = AssertionPayload::deserialize(entry.get("antithesis_assert")?).ok()?;
    AssertionSummary::from_payload(payload)
}

impl Event<'_> for Assertion {
    fn classify(entry: &Value) -> Option<Self> {
        parse_assertion_summary(entry).map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let summary = &self.0;
        let body = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(
                f,
                "{} \"{}\"",
                sanitize(&summary.label),
                sanitize(&summary.message)
            )
        });
        // FAIL only on a failing always/unreachable; CATALOG on a catalog
        // registration; HIT otherwise.
        match summary.verdict {
            // A catalog registration is chatter: the whole line recedes.
            AssertVerdict::Catalog => write!(
                block,
                "{}",
                style(DisplayWith(|f: &mut fmt::Formatter<'_>| write!(
                    f,
                    "CATALOG {body}"
                )))
                .dim()
            )?,
            AssertVerdict::Fail => write!(block, "{} {body}", style("FAIL").red().bold())?,
            AssertVerdict::Pass => write!(block, "{} {body}", style("HIT").green().bold())?,
            // A hit sometimes/reachable is neither good nor bad on its own.
            AssertVerdict::Hit => write!(block, "{} {body}", style("HIT").bold())?,
        }
        if block.detail() {
            if let Some(location) = &summary.location {
                block.detail_line(format_args!("@ {location}"))?;
            }
            if let Some(json) = &summary.details {
                block.detail_line(format_args!("details {json}"))?;
            }
        }
        Ok(())
    }
}

pub(super) fn render_assertion_location(location: AssertionLocation) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_render::testkit::*;
    use serde_json::json;

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
