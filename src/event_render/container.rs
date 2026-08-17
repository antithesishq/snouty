//! Container lifecycle records (`containers_meta`): create/start/died and
//! friends, with exit codes, the image on create, and health probes receding
//! as chatter.

use std::fmt::{self, Write};

use console::style;
use serde_json::Value;

use crate::render::sanitize;

use super::{Block, DisplayWith, Event, format_value};

pub(super) struct Lifecycle<'a>(&'a Value);

impl<'a> Event<'a> for Lifecycle<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        if entry["source"]["name"].as_str() != Some("containers_meta") {
            return None;
        }
        entry.get("event").is_some().then_some(Self(entry))
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let entry = self.0;
        let event = entry["event"].as_str().unwrap_or("");
        let name = entry["name"].as_str().unwrap_or("");
        let text = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "container {} {}", sanitize(event), sanitize(name))?;
            if let Some(code) = entry.get("container_exit_code")
                && let Some(rendered) = format_value(code)
            {
                write!(f, " exit={rendered}")?;
            }
            Ok(())
        });
        // Deaths pop red; health probes are pure chatter (the record carries
        // no healthy/unhealthy verdict) and recede; the rest of the lifecycle
        // stays blue.
        let styled = match event {
            "died" | "kill" | "oom" => style(text).red(),
            event if event.starts_with("health_status") => style(text).dim(),
            _ => style(text).blue(),
        };
        write!(block, "{styled}")?;
        // The image (repository only — the digest is noise at a glance)
        // matters once, when the container is created.
        if let ("create", Some(image)) = (event, entry["image"].as_str()) {
            let repository = image.split('@').next().unwrap_or(image);
            block.detail_line(format_args!("image {}", sanitize(repository)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::event_render::testkit::*;
    use serde_json::json;

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
            "12.6      [containers_meta] container create sqlite-init"
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
}
