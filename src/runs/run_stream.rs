//! `runs stream`: the run status stream, rendered one line per change.
//!
//! Each NDJSON line decodes into a [`Record`]. To support a new notification
//! shape, add a `Record` variant, decode it in [`Record::parse`], and render
//! it in [`render_record`]; the exhaustive matches fail the build until both
//! are done. A `type` that this snouty does not know yet decodes as
//! [`Record::Unknown`] and prints as its JSON, so a server that adds a shape
//! does not break an older snouty.

use std::fmt::Write;

use color_eyre::eyre::{Result, WrapErr};
use console::style;
use serde::Deserialize;
use serde_json::Value;

use crate::api::{Event, Property, RunStreamNotificationType, normalize_property};
use crate::render::sanitize;

use super::{
    mid_stream_error, normalize_vtime_field, property_examples_cell, property_status_label,
    stream_error_message,
};

/// One line of the run status stream.
#[derive(Debug)]
pub(super) enum Record {
    /// A notification whose data lists the properties that changed.
    Properties {
        change: PropertyChange,
        properties: Vec<Property>,
    },
    /// RUN_FINALIZED: the run is final, and the stream ends successfully.
    Finalized,
    /// A notification `type` that this snouty does not know.
    Unknown(Value),
}

/// What happened to the properties of a [`Record::Properties`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PropertyChange {
    Added,
    Passed,
    Failed,
    NewExamples,
}

impl PropertyChange {
    fn label(self) -> console::StyledObject<&'static str> {
        match self {
            Self::Added => style("added").cyan(),
            Self::Passed => style("passed").green(),
            Self::Failed => style("failed").red(),
            Self::NewExamples => style("examples").blue(),
        }
    }
}

impl Record {
    /// Decode one stream line. A `Stream_Error` line fails: the server ended
    /// the stream before the run finalized.
    pub(super) fn parse(line: &Value) -> Result<Self> {
        if let Some(message) = stream_error_message(line) {
            return Err(mid_stream_error(message));
        }
        let Ok(kind) = RunStreamNotificationType::deserialize(&line["type"]) else {
            return Ok(Self::Unknown(line.clone()));
        };
        let change = match kind {
            RunStreamNotificationType::RunFinalized => return Ok(Self::Finalized),
            RunStreamNotificationType::NewPropertiesAdded => PropertyChange::Added,
            RunStreamNotificationType::PropertiesPassed => PropertyChange::Passed,
            RunStreamNotificationType::PropertiesFailed => PropertyChange::Failed,
            RunStreamNotificationType::NewExamplesFound => PropertyChange::NewExamples,
        };
        let properties = Vec::<Property>::deserialize(&line["data"])
            .wrap_err_with(|| format!("decoding a {kind} notification"))?
            .into_iter()
            .map(normalize_property)
            .collect::<Result<_>>()?;
        Ok(Self::Properties { change, properties })
    }
}

/// The line `--json` prints for `record`: the server's line, with each event
/// sample's vtime normalized to an exact JSON number, as `runs properties
/// --json` prints it.
pub(super) fn json_line(record: &Record, mut line: Value) -> String {
    if let Record::Properties { .. } = record {
        let properties = line["data"].as_array_mut().into_iter().flatten();
        for property in properties.filter(|p| p["is_event"] == true) {
            for key in ["examples", "counterexamples"] {
                for sample in property[key].as_array_mut().into_iter().flatten() {
                    normalize_vtime_field(sample, "moment");
                }
            }
        }
    }
    line.to_string()
}

/// The width of the change label column: `finalized` is the widest label.
const LABEL_WIDTH: usize = 9;
/// The width of the status column: `undetermined` is the widest status.
const STATUS_WIDTH: usize = 12;
/// The width of the right-aligned example count column (`186k/12k`).
const EXAMPLES_WIDTH: usize = 8;

/// Render one record for the terminal, without a trailing newline. A property
/// change is one line per property: the change, the property's status, its
/// example counts (✓ or ✓/✗), and its name. `detail` adds the property's ID
/// and the samples the notification carries, one dim line each.
pub(super) fn render_record(record: &Record, detail: bool) -> String {
    let mut out = String::new();
    match record {
        Record::Properties { change, properties } => {
            for property in properties {
                write_property(&mut out, *change, property, detail);
            }
        }
        Record::Finalized => {
            let label = style("finalized").bold();
            writeln!(out, "{label:<LABEL_WIDTH$}  the run is final").unwrap();
        }
        Record::Unknown(line) => {
            writeln!(out, "{}", style(sanitize(&line.to_string())).dim()).unwrap();
        }
    }
    out.truncate(out.trim_end().len());
    out
}

fn write_property(out: &mut String, change: PropertyChange, property: &Property, detail: bool) {
    let label = change.label();
    let status = property_status_label(property.status());
    let examples = property_examples_cell(property);
    let name = sanitize(property.name());
    writeln!(
        out,
        "{label:<LABEL_WIDTH$}  {status:<STATUS_WIDTH$}  {examples:>EXAMPLES_WIDTH$}  {name}"
    )
    .unwrap();
    if !detail {
        return;
    }
    let indent = LABEL_WIDTH + 2;
    if let Some(id) = property.id() {
        writeln!(
            out,
            "{:indent$}{}",
            "",
            style(format!("id {}", sanitize(id))).dim()
        )
        .unwrap();
    }
    for (kind, sample) in samples(property) {
        writeln!(
            out,
            "{:indent$}{}",
            "",
            style(format!("{kind:<7}  {sample}")).dim()
        )
        .unwrap();
    }
}

/// The samples a property carries, counterexamples first, each as the text
/// `--detail` prints: an event's moment hash and vtime, or a non-event
/// property's value as JSON.
fn samples(property: &Property) -> Vec<(&'static str, String)> {
    match property {
        Property::EventProperty(p) => {
            let moment = |event: &Event| {
                format!(
                    "{}  {}",
                    sanitize(&event.moment.input_hash),
                    event.moment.vtime
                )
            };
            let failing = p.counterexamples.iter().map(|e| ("failing", moment(e)));
            let passing = p.examples.iter().map(|e| ("passing", moment(e)));
            failing.chain(passing).collect()
        }
        Property::NonEventProperty(p) => {
            let value = |v: &Value| sanitize(&v.to_string());
            let failing = p.counterexamples.iter().map(|v| ("failing", value(v)));
            let passing = p.examples.iter().map(|v| ("passing", value(v)));
            failing.chain(passing).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn render(line: Value, detail: bool) -> String {
        console::set_colors_enabled(false);
        render_record(&Record::parse(&line).unwrap(), detail)
    }

    fn property(status: &str) -> Value {
        json!({
            "id": "27a7cb0b311c8481c4ce12f3347fadbf",
            "name": "Balance stays nonnegative",
            "status": status,
            "is_event": true,
            "example_count": 24,
            "counterexample_count": 2,
            "examples": [{"moment": {"input_hash": "-300", "vtime": "398.4898056755774"}}],
            "counterexamples": [{"moment": {"input_hash": "-100", "vtime": "401.25"}}]
        })
    }

    #[test]
    fn each_property_change_renders_its_label() {
        for (kind, status, label) in [
            ("NEW_PROPERTIES_ADDED", "Undetermined", "added"),
            ("PROPERTIES_PASSED", "Passing", "passed"),
            ("PROPERTIES_FAILED", "Failing", "failed"),
            ("NEW_EXAMPLES_FOUND", "Undetermined", "examples"),
        ] {
            let out = render(json!({"type": kind, "data": [property(status)]}), false);
            let status = status.to_lowercase();
            assert_eq!(
                out,
                format!("{label:<9}  {status:<12}      24/2  Balance stays nonnegative"),
            );
        }
    }

    #[test]
    fn a_notification_renders_one_line_per_property() {
        let line = json!({"type": "PROPERTIES_PASSED", "data": [property("Passing"), property("Passing")]});
        assert_eq!(render(line, false).lines().count(), 2);
    }

    #[test]
    fn detail_adds_the_id_and_the_samples_counterexamples_first() {
        let line = json!({"type": "NEW_EXAMPLES_FOUND", "data": [property("Failing")]});
        let out = render(line, true);
        let detail: Vec<&str> = out.lines().skip(1).map(str::trim).collect();
        assert_eq!(
            detail,
            [
                "id 27a7cb0b311c8481c4ce12f3347fadbf",
                "failing  -100  401.25",
                "passing  -300  398.4898056755774",
            ]
        );
    }

    #[test]
    fn a_non_event_sample_renders_as_json() {
        let mut p = property("Failing");
        p["is_event"] = json!(false);
        p["counterexamples"] = json!([{"peak": 1340}]);
        p["examples"] = json!([]);
        let out = render(json!({"type": "PROPERTIES_FAILED", "data": [p]}), true);
        assert!(out.contains(r#"failing  {"peak":1340}"#), "got: {out}");
    }

    #[test]
    fn run_finalized_ends_the_stream() {
        let line = json!({"type": "RUN_FINALIZED", "data": []});
        let record = Record::parse(&line).unwrap();
        assert!(matches!(record, Record::Finalized));
        assert_eq!(render(line, false), "finalized  the run is final");
    }

    #[test]
    fn an_unknown_type_renders_its_json() {
        let line = json!({"type": "RUN_PAUSED", "data": []});
        let record = Record::parse(&line).unwrap();
        assert!(matches!(record, Record::Unknown(_)));
        assert_eq!(render(line, false), r#"{"type":"RUN_PAUSED","data":[]}"#);
    }

    #[test]
    fn a_stream_error_fails_the_parse() {
        let err = Record::parse(&json!({"error": "subscriber fell behind"})).unwrap_err();
        assert!(
            format!("{err}").contains("subscriber fell behind"),
            "got: {err}"
        );
    }

    #[test]
    fn a_known_type_with_malformed_data_fails_the_parse() {
        let line = json!({"type": "PROPERTIES_FAILED", "data": [{"status": "Failing"}]});
        let err = Record::parse(&line).unwrap_err();
        assert!(format!("{err}").contains("PROPERTIES_FAILED"), "got: {err}");
    }

    #[test]
    fn json_lines_normalize_event_sample_vtimes() {
        let line = json!({"type": "PROPERTIES_FAILED", "data": [property("Failing")]});
        let record = Record::parse(&line).unwrap();
        let out: Value = serde_json::from_str(&json_line(&record, line)).unwrap();
        assert_eq!(
            out["data"][0]["counterexamples"][0]["moment"]["vtime"],
            json!(401.25)
        );
    }
}
