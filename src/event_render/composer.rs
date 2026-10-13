//! Test-composer records: a test command's lifecycle (`task_status`) and the
//! composer's other structured chatter, both classified only on records
//! whose `source.name` is `antithesis_test_composer`.

use std::borrow::Cow;
use std::fmt::{self, Write};

use console::style;
use serde_json::{Map, Value};

use crate::render::sanitize;

use super::ansi::strip_ansi;
use super::{
    Block, DisplayWith, Event, VALUE_TRUNCATE_WIDTH, format_duration, format_value, payload_pairs,
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

        // The API mixes wire forms (command_runtime likewise): the return
        // code arrives as a string or a number. Red is reserved for a KNOWN
        // non-zero exit — an absent or unreadable code stays neutral rather
        // than reporting a passing command as a failure. TODO: drop the string
        // form once the API sends numbers only.
        let return_code = match &entry["command_return_code"] {
            Value::String(code) => Some(sanitize(code)),
            Value::Number(code) => Some(code.to_string()),
            _ => None,
        };
        let failed = matches!(return_code.as_deref(), Some(code) if code != "0");
        let text = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "task finished {}", sanitize(command))?;
            if let Some(code) = &return_code {
                write!(f, " exit={code}")?;
            }
            if let Some(duration) = format_duration(&entry["command_runtime"]) {
                write!(f, " in {duration}")?;
            }
            Ok(())
        });
        if failed {
            write!(block, "{}", style(text).red())?;
        } else {
            write!(block, "{}", style(text).blue())?;
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
        // A `new_command` names the script, so `new_command_path` only
        // repeats it. Only `--detail` shows it, on the line after.
        let path_after_new_command = self
            .0
            .get("new_command")
            .and_then(Value::as_str)
            .is_some_and(|command| !command.is_empty());
        if block.detail() {
            // One pair per line, untruncated.
            write!(block, "{}", style("composer").dim())?;
            for (key, rendered) in payload_pairs(self.0) {
                if path_after_new_command && key == "new_command_path" {
                    continue;
                }
                block.detail_line(format_args!("{}={rendered}", sanitize(key)))?;
                if path_after_new_command
                    && key == "new_command"
                    && let Some(path) = self.0.get("new_command_path").and_then(format_value)
                {
                    block.detail_line(format_args!("new_command_path={path}"))?;
                }
            }
            return Ok(());
        }

        let container_id = self.0.get("container_id").and_then(Value::as_str);
        let line = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "composer")?;
            for (key, rendered) in payload_pairs(self.0) {
                if path_after_new_command && key == "new_command_path" {
                    continue;
                }
                let shown = shorten(key, &rendered, container_id);
                write!(f, " {}={shown}", sanitize(key))?;
            }
            Ok(())
        });
        write!(block, "{}", style(line).dim())
    }
}

/// `docker ps` shows this many characters of a container id.
const CONTAINER_ID_WIDTH: usize = 12;

/// Shortens one chatter value for the one-line form. A path keeps only its
/// final segment, so that one script always reads the same. A container id
/// keeps its start, as `docker ps` does. A `new_command` is the record's
/// container id, `_`, and the script name, so it drops the id. A `weight` is
/// a probability, so three decimals are sufficient.
fn shorten<'a>(key: &str, rendered: &'a str, container_id: Option<&str>) -> Cow<'a, str> {
    match key {
        "command" | "removed_command" | "new_command_path" => {
            truncate_path(rendered, VALUE_TRUNCATE_WIDTH)
        }
        "container_id" => console::truncate_str(rendered, CONTAINER_ID_WIDTH, ""),
        "new_command" => match container_id
            .and_then(|id| rendered.strip_prefix(id))
            .and_then(|rest| rest.strip_prefix('_'))
        {
            Some(script) if !script.is_empty() => truncate_path(script, VALUE_TRUNCATE_WIDTH),
            _ => console::truncate_str(rendered, VALUE_TRUNCATE_WIDTH, "…"),
        },
        "weight" => match rendered.parse::<f64>() {
            Ok(weight) if weight.is_finite() => format!("{weight:.3}").into(),
            _ => console::truncate_str(rendered, VALUE_TRUNCATE_WIDTH, "…"),
        },
        _ => console::truncate_str(rendered, VALUE_TRUNCATE_WIDTH, "…"),
    }
}

/// Shortens a path to `…` and its final segment, such as `…/check.py`, so
/// that each path has the same form. A name with no `/` that fits `width` is
/// unchanged. When the final segment does not fit, the result keeps the end
/// of it.
fn truncate_path(path: &str, width: usize) -> Cow<'_, str> {
    const HEAD: &str = "…";
    if !path.contains('/') && console::measure_text_width(path) <= width {
        return path.into();
    }
    let budget = width.saturating_sub(console::measure_text_width(HEAD));
    let tail = path.rfind('/').map_or(path, |slash| &path[slash..]);
    // Measure each suffix as a whole. Measuring one char at a time gives a
    // different width when the text holds escape sequences. A shorter suffix
    // can be wider, because a cut can split an escape sequence, so check the
    // whole tail first. Bisect the char starts, so that a long tail of
    // zero-width chars does not cost one measure for each char.
    let fits = |start: usize| console::measure_text_width(&tail[start..]) <= budget;
    let start = if fits(0) {
        0
    } else {
        let starts: Vec<usize> = tail.char_indices().map(|(index, _)| index).collect();
        let at = |i: usize| starts.get(i).copied().unwrap_or(tail.len());
        // The suffix at `lo` does not fit, and the suffix at `hi` fits.
        let (mut lo, mut hi) = (0, starts.len());
        while hi - lo > 1 {
            let mid = lo + (hi - lo) / 2;
            if fits(at(mid)) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        at(hi)
    };
    format!("{HEAD}{}", &tail[start..]).into()
}

#[cfg(test)]
mod tests {
    use super::{VALUE_TRUNCATE_WIDTH, truncate_path};
    use crate::event_render::format_value;
    use crate::event_render::testkit::*;
    use serde_json::{Value, json};

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
            "16.1      [test_composer] task finished git_walk/parallel_driver_walk exit=1 in 0.0s"
        );
        assert_eq!(block.lines().count(), 2, "got: {block}");

        // Detail: stderr lines surface as indented details; empty stdout is
        // omitted.
        let block = render_one_detailed(entry);
        let mut lines = block.lines().skip(2);
        assert_eq!(lines.next().unwrap(), "                    err| boom");
        assert_eq!(
            lines.next().unwrap(),
            "                    err| second line"
        );

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
    fn a_missing_or_numeric_return_code_never_renders_as_failure() {
        // A JSON-number 0 is a passing exit, not a wire-form accident.
        let numeric = render_one(json!({
            "task_status": "finished", "command": "ok.sh",
            "command_return_code": 0,
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(
            numeric.ends_with("task finished ok.sh exit=0"),
            "got: {numeric}"
        );

        // No return code at all: neutral (the text carries no exit=), never
        // styled as a failure. Colors are off in tests, so the invariant
        // that matters here is the missing exit= suffix.
        let absent = render_one(json!({
            "task_status": "finished", "command": "ok.sh",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "1.0"}
        }));
        assert!(absent.ends_with("task finished ok.sh"), "got: {absent}");
    }

    #[test]
    fn composer_chatter_truncates_inline_and_expands_under_detail() {
        let entry = json!({
            "new_command_path": "git_walk/anytime_invariants",
            "container_id": "22453394531ae33a6df72b8119fb9fd8338f8854d6a271fe736417fb35975013",
            "source": {"name": "antithesis_test_composer"},
            "moment": {"input_hash": "-1", "vtime": "13.3"}
        });
        // A path keeps only its final segment, even when it is short.
        let block = render_one(entry.clone());
        assert!(
            block.contains("composer new_command_path=…/anytime_invariants "),
            "got: {block}"
        );
        // The 64-hex container id keeps 12 characters, as `docker ps` does.
        assert!(block.ends_with("container_id=22453394531a"), "got: {block}");

        // --detail: one pair per line, untruncated.
        let block = render_one_detailed(entry);
        let mut lines = block.lines().skip(1);
        assert!(
            lines.next().unwrap().ends_with("[test_composer] composer"),
            "got: {block}"
        );
        assert_eq!(
            lines.next().unwrap(),
            "                    new_command_path=git_walk/anytime_invariants"
        );
        assert_eq!(
            lines.next().unwrap(),
            "                    container_id=22453394531ae33a6df72b8119fb9fd8338f8854d6a271fe736417fb35975013"
        );
    }

    #[test]
    fn composer_command_keeps_its_end_and_weight_rounds() {
        let chatter = |weight: Value, command: &str| {
            json!({
                "weight_type": "configured", "weight": weight, "command": command,
                "source": {"name": "antithesis_test_composer"},
                "moment": {"input_hash": "-1", "vtime": "15.7"}
            })
        };
        let long = "/opt/antithesis/test/v1/wellnest/parallel_driver_check_ins.py";

        // The wire form is a string; a long command keeps its final segment.
        let block = render_one(chatter(json!("0.037937902697115555"), long));
        assert!(
            block.ends_with(
                "composer weight_type=configured weight=0.038 command=…/parallel_driver_check_ins.py"
            ),
            "got: {block}"
        );

        // A numeric weight rounds the same; a short command is cut the same.
        let block = render_one(chatter(json!(0.0555555), "/opt/t/a.py"));
        assert!(
            block.ends_with("weight=0.056 command=…/a.py"),
            "got: {block}"
        );
        let block = render_one(chatter(json!(0.0555555), "a.py"));
        assert!(block.ends_with("weight=0.056 command=a.py"), "got: {block}");

        // An unparsable weight is not rounded, and is truncated as other
        // values are.
        let block = render_one(chatter(json!("high"), "/opt/t/a.py"));
        assert!(block.contains(" weight=high "), "got: {block}");
        let block = render_one(chatter(json!("x".repeat(50)), "/opt/t/a.py"));
        assert!(
            block.contains(&format!(" weight={}… ", "x".repeat(39))),
            "got: {block}"
        );

        // --detail: full values.
        let block = render_one_detailed(chatter(json!("0.037937902697115555"), long));
        assert!(
            block.contains("\n                    weight=0.037937902697115555\n"),
            "got: {block}"
        );
        assert!(
            block.ends_with(&format!("\n                    command={long}")),
            "got: {block}"
        );
    }

    #[test]
    fn composer_paths_keep_the_script_name_and_new_command_drops_the_container_id() {
        let id = "89e64f6a3e9f09e966b298ba707d46ee2bc3f8c8624ac7fc0c498afc66253642";
        let chatter = |pairs: Value| {
            let mut entry = pairs;
            entry["source"] = json!({"name": "antithesis_test_composer"});
            entry["moment"] = json!({"input_hash": "-1", "vtime": "15.7"});
            entry
        };

        // Shapes observed in release 64.0 run
        // cf98800188c5d337790acd0a0e00e9d1-64-0.
        let block = render_one(chatter(json!({
            "removed_command": "/opt/antithesis/test/v1/wellnest/parallel_driver_journal.py"
        })));
        assert!(
            block.ends_with("composer removed_command=…/parallel_driver_journal.py"),
            "got: {block}"
        );
        let block = render_one(chatter(json!({
            "new_command_path": "wellnest/parallel_driver_check_ins.py",
            "container_id": id,
            "new_command": format!("{id}_parallel_driver_check_ins.py"),
        })));
        // The new_command names the script, so new_command_path is left out.
        assert!(
            block.ends_with(
                "composer container_id=89e64f6a3e9f new_command=parallel_driver_check_ins.py"
            ),
            "got: {block}"
        );
        // --detail shows new_command_path on the line after new_command.
        let block = render_one_detailed(chatter(json!({
            "new_command_path": "wellnest/parallel_driver_check_ins.py",
            "container_id": id,
            "new_command": format!("{id}_parallel_driver_check_ins.py"),
        })));
        assert!(
            block.ends_with(&format!(
                "[test_composer] composer\n                    \
                 container_id={id}\n                    \
                 new_command={id}_parallel_driver_check_ins.py\n                    \
                 new_command_path=wellnest/parallel_driver_check_ins.py"
            )),
            "got: {block}"
        );

        // The script after the id is cut as a path.
        let block = render_one(chatter(json!({
            "container_id": id,
            "new_command": format!("{id}_/opt/antithesis/test/v1/wellnest/parallel_driver_journal.py"),
        })));
        assert!(
            block.ends_with("new_command=…/parallel_driver_journal.py"),
            "got: {block}"
        );

        // A new_command without the record's container id in front is
        // truncated as other values are, and new_command_path stays out.
        let other = "0123456789abcdef0123456789abcdef0123456789abcdef_x.py";
        let block = render_one(chatter(json!({
            "new_command_path": "t/x.py", "container_id": id, "new_command": other,
        })));
        assert!(
            block.ends_with(&format!(
                "composer container_id=89e64f6a3e9f new_command={}…",
                &other[..39]
            )),
            "got: {block}"
        );

        // A final segment wider than the budget keeps its end.
        let name = format!("{}.py", "n".repeat(50));
        let block = render_one(chatter(json!({"command": format!("/opt/t/{name}")})));
        assert!(
            block.ends_with(&format!("command=…{}", &name[name.len() - 39..])),
            "got: {block}"
        );

        // --detail: full values.
        let block = render_one_detailed(chatter(json!({
            "container_id": id,
            "new_command": format!("{id}_parallel_driver_check_ins.py"),
        })));
        assert!(
            block.contains(&format!("\n                    container_id={id}\n")),
            "got: {block}"
        );
        assert!(
            block.ends_with(&format!(
                "\n                    new_command={id}_parallel_driver_check_ins.py"
            )),
            "got: {block}"
        );
    }

    /// For any path, the cut form fits the width and keeps the end of the
    /// input. It starts at the final `/` when that segment fits, and else
    /// holds no `/`. Only a name with no `/` that fits stays whole.
    #[hegel::test]
    fn truncate_path_fits_and_keeps_the_final_segment(tc: hegel::TestCase) {
        let segments = tc.draw(hegel::generators::integers::<usize>().max_value(6));
        let s = (0..segments)
            .map(|_| tc.draw(hegel::generators::text()))
            .collect::<Vec<_>>()
            .join("/");
        let out = truncate_path(&s, VALUE_TRUNCATE_WIDTH);
        assert!(console::measure_text_width(&out) <= VALUE_TRUNCATE_WIDTH);
        if !s.contains('/') && console::measure_text_width(&s) <= VALUE_TRUNCATE_WIDTH {
            assert_eq!(out, s);
            return;
        }
        let kept = out.strip_prefix('…').expect("a cut path starts with `…`");
        assert!(s.ends_with(kept));
        let tail = s.rfind('/').map(|slash| &s[slash..]);
        match tail {
            Some(tail) if console::measure_text_width(tail) < VALUE_TRUNCATE_WIDTH => {
                assert_eq!(kept, tail);
            }
            _ => assert!(!kept.contains('/')),
        }
    }

    /// `console` reads ESC and the char after it as an escape sequence of
    /// zero width. The cut must measure the kept text as a whole, so that
    /// the final segment fits here.
    #[test]
    fn truncate_path_measures_escape_sequences_as_the_whole_value_does() {
        let path = format!("00/{}\x1b0:000", "0".repeat(34));
        let out = truncate_path(&path, VALUE_TRUNCATE_WIDTH);
        assert_eq!(out, format!("…/{}\x1b0:000", "0".repeat(34)));
    }

    /// An escape sequence makes the width of a suffix not monotonic in its
    /// length. Wherever ESC sits, also at each bisect midpoint, the cut keeps
    /// a suffix that fits, and the suffix one char longer does not fit.
    #[test]
    fn truncate_path_keeps_a_maximal_suffix_wherever_an_escape_sits() {
        let budget = VALUE_TRUNCATE_WIDTH - console::measure_text_width("…");
        let plain = format!("/{}", "0".repeat(60));
        for at in 1..=plain.len() {
            let path = format!("{}\x1b{}", &plain[..at], &plain[at..]);
            let out = truncate_path(&path, VALUE_TRUNCATE_WIDTH);
            let kept = out.strip_prefix('…').expect("a cut path starts with `…`");
            assert!(path.ends_with(kept), "ESC at {at}");
            assert!(console::measure_text_width(kept) <= budget, "ESC at {at}");
            let rest = &path[..path.len() - kept.len()];
            let longer = rest.char_indices().last().map(|(index, _)| &path[index..]);
            let longer = longer.expect("the whole path does not fit");
            assert!(console::measure_text_width(longer) > budget, "ESC at {at}");
        }
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
