//! The fault injector's events: injected faults (`fault`) and the
//! injector's own status chatter (`info`). Both classify only on records
//! whose `source.name` is `fault_injector`.

use std::fmt::{self, Write};

use console::style;
use serde_json::Value;

use crate::render::sanitize;

use super::{Block, DisplayWith, Event, format_duration, format_value};

fn from_injector(entry: &Value) -> bool {
    entry["source"]["name"].as_str() == Some("fault_injector")
}

pub(super) struct Fault<'a>(&'a Value);

impl<'a> Event<'a> for Fault<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if !from_injector(entry) {
            return None;
        }
        entry.get("fault").map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let fault = self.0;
        let name = fault["name"].as_str().unwrap_or("");
        let kind = fault["type"].as_str().unwrap_or("");
        // `restore` ends the disruption; everything else starts one.
        let restore = name == "restore";
        fn tint<D>(restore: bool, d: D) -> console::StyledObject<D> {
            if restore {
                style(d).green()
            } else {
                style(d).red()
            }
        }

        let fault_details = &fault["details"];
        write!(
            block,
            "{} {}",
            tint(restore, "FAULT").bold(),
            tint(
                restore,
                DisplayWith(|f: &mut fmt::Formatter<'_>| write!(
                    f,
                    "{}/{}",
                    sanitize(kind),
                    sanitize(name)
                ))
            ),
        )?;
        if let Some(disruption) = fault_details["disruption_type"].as_str() {
            write!(block, " {}", sanitize(disruption))?;
        }
        if fault_details["asymmetric"].as_bool() == Some(true) {
            write!(block, " asymmetric")?;
        }
        if let Some(offset) = fault_details["offset"].as_f64() {
            write!(block, " offset={offset:+.2}s")?;
        }
        if let Some(latency) = fault_details["latency"].as_object() {
            let mean = latency.get("mean").and_then(Value::as_f64).unwrap_or(0.0);
            let deviation = latency
                .get("deviation")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            write!(block, " latency={mean:.0}ms±{deviation:.0}")?;
        }
        if let Some(drop_rate) = fault_details["drop_rate"].as_f64()
            && drop_rate > 0.0
        {
            write!(block, " drop={drop_rate}")?;
        }
        if let Some(nodes) = fault["affected_nodes"].as_array()
            && !nodes.is_empty()
        {
            write!(block, " nodes=")?;
            for (i, node) in nodes.iter().enumerate() {
                let separator = if i == 0 { "" } else { "," };
                write!(
                    block,
                    "{separator}{}",
                    sanitize(node.as_str().unwrap_or("?"))
                )?;
            }
        }
        if let Some(duration) = format_duration(&fault["max_duration"]) {
            write!(block, " max={duration}")?;
        }
        block.details_json(fault_details)
    }
}

pub(super) struct InjectorInfo<'a>(&'a Value);

impl<'a> Event<'a> for InjectorInfo<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if !from_injector(entry) {
            return None;
        }
        entry.get("info").map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let info = self.0;
        // Injector chatter is dim, whole-line.
        let line = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "fault-injector")?;
            let message = sanitize(info["message"].as_str().unwrap_or(""));
            if !message.is_empty() {
                write!(f, " {message}")?;
            }
            if let Some(details) = info["details"].as_object() {
                for (key, value) in details {
                    if let Some(rendered) = format_value(value) {
                        write!(f, " {}={rendered}", sanitize(key))?;
                    }
                }
            }
            Ok(())
        });
        write!(block, "{}", style(line).dim())
    }
}

#[cfg(test)]
mod tests {
    use crate::event_render::testkit::*;
    use serde_json::json;

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
}
