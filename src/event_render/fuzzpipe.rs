//! The records `fuzzpipe` writes about a command it runs in the guest, such
//! as the script of `runs exec`. The source is the command's own name (for
//! example `bash_command`), so these classify on shape alone. Seen live on
//! orbitinghail (release 64.0) from `runs exec --events`:
//! `{"fuzzpipe": {"event_type": "Child started", "pid": 2103}}`.

use std::fmt::{self, Write};

use console::style;
use serde_json::{Map, Value};

use crate::render::sanitize;

use super::{Block, DisplayWith, Event, payload_pairs};

pub(super) struct Fuzzpipe<'a> {
    event_type: &'a str,
    record: &'a Map<String, Value>,
}

impl<'a> Event<'a> for Fuzzpipe<'a> {
    fn classify(entry: &'a Value) -> Option<Self> {
        let record = entry.get("fuzzpipe")?.as_object()?;
        Some(Self {
            event_type: record.get("event_type")?.as_str()?,
            record,
        })
    }

    fn render(&self, block: &mut Block<'_>) -> fmt::Result {
        let line = DisplayWith(|f: &mut fmt::Formatter<'_>| {
            write!(f, "fuzzpipe {}", sanitize(self.event_type))?;
            for (key, rendered) in payload_pairs(self.record) {
                if key != "event_type" {
                    write!(f, " {}={rendered}", sanitize(key))?;
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
    use serde_json::{Value, json};

    fn record(fuzzpipe: Value) -> Value {
        json!({
            "fuzzpipe": fuzzpipe,
            "source": {"meta_for": "echidna-cmd-1", "name": "bash_command"},
            "moment": {"input_hash": "-1", "vtime": "15.79"}
        })
    }

    #[test]
    fn renders_the_event_type_and_its_fields_on_one_line() {
        let received = render_one(record(json!({"event_type": "Command received"})));
        assert!(
            received.ends_with("[bash_command] fuzzpipe Command received"),
            "got: {received}"
        );

        let started = render_one(record(json!({"event_type": "Child started", "pid": 2103})));
        assert!(
            started.ends_with("[bash_command] fuzzpipe Child started pid=2103"),
            "got: {started}"
        );
    }

    #[test]
    fn a_record_without_an_event_type_keeps_the_raw_json() {
        let block = render_one(record(json!({"pid": 2103})));
        assert!(
            block.ends_with(r#"[bash_command] {"fuzzpipe":{"pid":2103}}"#),
            "got: {block}"
        );
    }
}
