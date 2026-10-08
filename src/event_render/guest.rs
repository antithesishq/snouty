//! The guest command service's record of a command it injects into the
//! guest (`guest_command_service`), such as the script of `runs exec`.

use std::fmt::{self, Write};

use console::style;
use serde_json::{Map, Number, Value};

use crate::render::sanitize;

use super::{Block, Event, payload_pairs};

pub(super) struct CommandInjected<'a> {
    record: &'a Map<String, Value>,
    input_count: &'a Number,
}

impl<'a> Event<'a> for CommandInjected<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if entry["source"]["name"].as_str() != Some("guest_command_service") {
            return None;
        }
        let Value::Number(input_count) = &entry["input_count"] else {
            return None;
        };
        Some(Self {
            record: entry.as_object()?,
            input_count,
        })
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        write!(
            block,
            "{}",
            style(format_args!(
                "command injected (input {})",
                self.input_count
            ))
            .dim()
        )?;
        if block.detail() {
            // One pair per line, untruncated.
            for (key, rendered) in payload_pairs(self.record) {
                block.detail_line(format_args!("{}={rendered}", sanitize(key)))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::event_render::testkit::*;
    use serde_json::{Value, json};

    // The shape `runs exec --events` printed against a live tenant.
    fn injected() -> Value {
        json!({
            "tags": {"campaign_id": "c1", "rollout_id": "7"},
            "input_command": "bash -c 'echo hi'",
            "input_count": 15,
            "input_injection_vtime": 67416981399u64,
            "metadata": {"logging_config": "default_antithesis"},
            "prev_input_hash": "9016394059448172899",
            "source": {"name": "guest_command_service"},
            "moment": {"input_hash": "-1", "vtime": "15.69"}
        })
    }

    #[test]
    fn injected_command_renders_as_one_line() {
        let block = render_one(injected());
        assert_eq!(
            block.lines().nth(1).unwrap(),
            "15.69     [guest_command_service] command injected (input 15)"
        );
        assert_eq!(block.lines().count(), 2, "got: {block}");
    }

    #[test]
    fn injected_command_keeps_every_field_under_detail() {
        let block = render_one_detailed(injected());
        let lines: Vec<&str> = block.lines().skip(1).collect();
        assert!(
            lines[0].ends_with("[guest_command_service] command injected (input 15)"),
            "got: {block}"
        );
        let indent = " ".repeat(20);
        for pair in [
            r#"tags={"campaign_id":"c1","rollout_id":"7"}"#,
            "input_command=bash -c 'echo hi'",
            "input_count=15",
            "input_injection_vtime=67416981399",
            r#"metadata={"logging_config":"default_antithesis"}"#,
            "prev_input_hash=9016394059448172899",
        ] {
            assert!(
                lines.contains(&format!("{indent}{pair}").as_str()),
                "missing {pair}: {block}"
            );
        }
    }

    #[test]
    fn a_record_without_input_count_keeps_the_raw_json() {
        let mut entry = injected();
        entry.as_object_mut().unwrap().remove("input_count");
        let block = render_one(entry);
        assert!(
            block.contains(r#"[guest_command_service] {"tags":"#),
            "got: {block}"
        );
    }
}
