//! SDK exploration guidance (`antithesis_guidance`): `hit: false` is the
//! catalog registration (dim, no data); an observation shows the drive on
//! the tracked value, reconstructed from `guidance_data` — best effort, with
//! the raw operands as the fallback when the direction cannot be recovered
//! (see [`render_guidance_expression`]).

use std::fmt::{self, Write};

use console::style;
use serde::Deserialize;
use serde_json::Value;

use crate::render::sanitize;

use super::assert::{AssertionLocation, render_assertion_location};
use super::{Block, DisplayWith, Event, format_value, render_details_json};

pub(super) struct Guidance<'a>(&'a Value);

impl<'a> Event<'a> for Guidance<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        entry.get("antithesis_guidance").map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let guidance = self.0;
        // The message doubles as the display label and the key into the
        // assertion map (`id` and `message` carry identical values).
        let message = guidance["message"]
            .as_str()
            .or(guidance["id"].as_str())
            .unwrap_or("")
            .trim();

        // A catalog registration: no data to show, the whole line recedes.
        if guidance["hit"].as_bool() == Some(false) {
            write!(
                block,
                "{}",
                style(DisplayWith(|f: &mut fmt::Formatter<'_>| write!(
                    f,
                    "CATALOG GUIDANCE \"{}\"",
                    sanitize(message)
                )))
                .dim()
            )?;
            if block.detail() {
                location_line(block, guidance)?;
            }
            return Ok(());
        }

        write!(
            block,
            "{} \"{}\"",
            style("GUIDANCE").bold(),
            sanitize(message)
        )?;
        if let Some(expression) = render_guidance_expression(guidance) {
            write!(block, ": {expression}")?;
        }
        if block.detail() {
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
            let info = info.trim();
            if !info.is_empty() {
                block.detail_line(info)?;
            }
            location_line(block, guidance)?;
        }
        Ok(())
    }
}

fn location_line(block: &mut Block<'_>, guidance: &Value) -> fmt::Result {
    let location = AssertionLocation::deserialize(&guidance["location"])
        .ok()
        .and_then(render_assertion_location);
    match location {
        Some(location) => block.detail_line(format_args!("@ {location}")),
        None => Ok(()),
    }
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
            // Degrade, don't drop: a missing `maximize` still shows the
            // propositions (space-separated, claiming no connective), and a
            // non-bool value renders as itself rather than erasing every
            // other proposition alongside it.
            let connective = match guidance["maximize"].as_bool() {
                Some(true) => " && ",
                Some(false) => " || ",
                None => " ",
            };
            let terms: Vec<String> = propositions
                .iter()
                .map(|(name, value)| {
                    let value = format_value(value).unwrap_or_else(|| "null".to_string());
                    format!("{}({value})", sanitize(name))
                })
                .collect();
            Some(terms.join(connective))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_render::testkit::*;
    use serde_json::json;

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
    fn boolean_guidance_degrades_instead_of_dropping() {
        // A non-bool proposition value renders as itself rather than erasing
        // every proposition alongside it.
        let mixed = render_one(json!({
            "antithesis_guidance": {
                "guidance_type": "boolean", "hit": true, "maximize": true,
                "message": "m",
                "guidance_data": {"acked": true, "durable": null}
            },
            "source": {"name": "antithesis_sdk"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            mixed.ends_with("GUIDANCE \"m\": acked(true) && durable(null)"),
            "got: {mixed}"
        );

        // A missing `maximize` shows the propositions without claiming a
        // connective.
        let no_direction = render_one(json!({
            "antithesis_guidance": {
                "guidance_type": "boolean", "hit": true,
                "message": "m",
                "guidance_data": {"acked": true, "durable": false}
            },
            "source": {"name": "antithesis_sdk"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            no_direction.ends_with("GUIDANCE \"m\": acked(true) durable(false)"),
            "got: {no_direction}"
        );
    }

    #[test]
    fn guidance_detail_mode_keeps_the_raw_data_and_location() {
        let mut r = renderer(true);
        let block = render_entry(
            &mut r,
            &json!({
                "antithesis_guidance": {
                    "guidance_type": "numeric", "maximize": true, "hit": true,
                    "message": "wal grew long",
                    "guidance_data": {"left": 48, "right": 1000},
                    "location": {"begin_line": 439, "file": "src/actions.rs", "function": "checkpoint"}
                },
                "source": {"container": "w", "name": "w"},
                "moment": {"input_hash": "-1", "vtime": "1.0"}
            }),
        );
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
}
