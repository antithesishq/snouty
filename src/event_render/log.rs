//! Plain log text (`output_text`) and the build-log line: the free-text
//! kinds, sanitized through [`super::ansi`] so SUT colors survive but
//! nothing else does.

use std::fmt::{self, Write};

use console::style;
use serde_json::Value;

use crate::render::sanitize;

use super::ansi::sanitize_log_text;
use super::{Block, Event};

pub(super) struct Log<'a>(&'a str);

impl<'a> Event<'a> for Log<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        entry.get("output_text")?.as_str().map(Self)
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        write!(block, "{}", sanitize_log_text(self.0))
    }
}

/// One build-log line, in the renderer's shared visual grammar: dim
/// timestamp, cyan bracketed source, sanitized text (SUT colors kept). Build
/// logs are wall-clock events with no moment, so there is no divider and no
/// classification — the stream label stands in for the source.
pub(super) fn render_build_log(
    out: &mut String,
    timestamp: &str,
    stream: &str,
    text: &str,
) -> fmt::Result {
    write!(
        out,
        "{} {} {}",
        style(timestamp).dim(),
        style(super::DisplayWith(|f: &mut fmt::Formatter<'_>| write!(
            f,
            "[{}]",
            sanitize(stream)
        )))
        .cyan(),
        sanitize_log_text(text)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_render::testkit::*;
    use serde_json::json;

    #[test]
    fn build_log_lines_share_the_visual_grammar() {
        console::set_colors_enabled(false);
        let mut out = String::new();
        render_build_log(&mut out, "2025-03-20 02:01:12", "out", "pulling image\r").unwrap();
        assert_eq!(out, r"2025-03-20 02:01:12 [out] pulling image\r");

        // Through the renderer: a record with no moment but a wall-clock
        // timestamp/stream/text triple is a build-log line — no divider, and
        // an empty text leaves no trailing space.
        let line = render_one(json!({
            "timestamp": "2025-03-20T02:01:12Z", "stream": "err", "text": ""
        }));
        assert!(line.ends_with("[err]"), "got: {line}");
        assert!(!line.contains("moment"), "got: {line}");

        // `stream` is optional in the build-log schema: a record without it
        // still renders as a log line, defaulting to [out].
        let streamless = render_one(json!({
            "timestamp": "2025-03-20T02:01:12Z", "text": "compiling"
        }));
        assert!(streamless.ends_with("[out] compiling"), "got: {streamless}");
    }
}
