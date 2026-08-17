//! The SDK's protocol events: the handshake announcing the SDK
//! (`antithesis_sdk`) and the workload's setup-complete signal
//! (`antithesis_setup`).

use std::fmt::{self, Write};

use console::style;
use serde_json::Value;

use crate::render::sanitize;

use super::{Block, Event};

pub(super) struct Sdk<'a>(&'a Value);

impl<'a> Event<'a> for Sdk<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        entry.get("antithesis_sdk").map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let sdk = self.0;
        let language = format!(
            "{} {}",
            sdk["language"]["name"].as_str().unwrap_or("?"),
            sdk["language"]["version"].as_str().unwrap_or(""),
        );
        write!(
            block,
            "{} connected: {} (sdk {}, protocol {})",
            style("SDK").green(),
            sanitize(language.trim()),
            sanitize(sdk["sdk_version"].as_str().unwrap_or("?")),
            sanitize(sdk["protocol_version"].as_str().unwrap_or("?")),
        )
    }
}

pub(super) struct Setup<'a>(&'a Value);

impl<'a> Event<'a> for Setup<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        entry.get("antithesis_setup").map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let setup = self.0;
        write!(block, "{}", style("SETUP").green().bold())?;
        let status = sanitize(setup["status"].as_str().unwrap_or(""));
        if !status.is_empty() {
            write!(block, " {status}")?;
        }
        // The details are user-controlled, arbitrarily nested JSON: never
        // flattened to key=value, shown as JSON under --detail.
        block.details_json(&setup["details"])
    }
}

#[cfg(test)]
mod tests {
    use crate::event_render::testkit::*;
    use serde_json::json;

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
}
