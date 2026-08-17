//! Test-composer records: a test command's lifecycle (`task_status`) and the
//! composer's other structured chatter, both classified only on records
//! whose `source.name` is `antithesis_test_composer`.

use std::fmt::{self, Write};

use console::style;
use serde_json::{Map, Value};

use crate::render::sanitize;

use super::ansi::strip_ansi;
use super::{
    Block, DisplayWith, Event, LOG_ENVELOPE_KEYS, VALUE_TRUNCATE_WIDTH, format_duration,
    format_value,
};

fn from_composer(entry: &Value) -> bool {
    entry["source"]["name"].as_str() == Some("antithesis_test_composer")
}

pub(super) struct Task<'a>(&'a Value);

impl<'a> Event<'a> for Task<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if !from_composer(entry) {
            return None;
        }
        entry.get("task_status").is_some().then_some(Self(entry))
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let entry = self.0;
        let status = entry["task_status"].as_str().unwrap_or("");
        let command = entry["command"].as_str().unwrap_or("");
        if status != "finished" {
            let text = DisplayWith(|f: &mut fmt::Formatter<'_>| {
                write!(f, "task {} {}", sanitize(status), sanitize(command))
            });
            return write!(block, "{}", style(text).blue());
        }

        let return_code = entry["command_return_code"].as_str().unwrap_or("");
        let ok = return_code == "0";
        let text = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "task finished {}", sanitize(command))?;
            if !return_code.is_empty() {
                write!(f, " exit={}", sanitize(return_code))?;
            }
            if let Some(duration) = format_duration(&entry["command_runtime"]) {
                write!(f, " in {duration}")?;
            }
            Ok(())
        });
        if ok {
            write!(block, "{}", style(text).blue())?;
        } else {
            write!(block, "{}", style(text).red())?;
        }
        if block.detail() {
            for (key, prefix) in [("additional_stdout", "out"), ("additional_stderr", "err")] {
                let Some(text) = entry[key].as_str() else {
                    continue;
                };
                for line in strip_ansi(text).lines() {
                    if line.trim().is_empty() {
                        continue;
                    }
                    block.detail_line(format_args!("{prefix}| {}", sanitize(line)))?;
                }
            }
        }
        Ok(())
    }
}

pub(super) struct Chatter<'a>(&'a Map<String, Value>);

impl<'a> Event<'a> for Chatter<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if !from_composer(entry) {
            return None;
        }
        entry.as_object().map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let pairs = self.0.iter().filter_map(|(key, value)| {
            if LOG_ENVELOPE_KEYS.contains(&key.as_str()) {
                return None;
            }
            format_value(value).map(|rendered| (key, rendered))
        });
        if block.detail() {
            // One pair per line, untruncated.
            write!(block, "{}", style("composer").dim())?;
            for (key, rendered) in pairs {
                block.detail_line(format_args!("{}={rendered}", sanitize(key)))?;
            }
        } else {
            let line = DisplayWith(|f: &mut fmt::Formatter<'_>| {
                write!(f, "composer")?;
                for (key, rendered) in self.0.iter().filter_map(|(key, value)| {
                    if LOG_ENVELOPE_KEYS.contains(&key.as_str()) {
                        return None;
                    }
                    format_value(value).map(|rendered| (key, rendered))
                }) {
                    let truncated = console::truncate_str(&rendered, VALUE_TRUNCATE_WIDTH, "…");
                    write!(f, " {}={truncated}", sanitize(key))?;
                }
                Ok(())
            });
            write!(block, "{}", style(line).dim())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_render::testkit::*;
    use serde_json::json;

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
}
