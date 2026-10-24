//! `runs stream`: the run status stream, denormalized to one line per event.
//!
//! Each NDJSON line decodes into a [`Record`], and [`Denormalizer::lines`]
//! flattens it into [`Line`]s: a notification lists many properties, and a
//! property carries many samples, but every output line names one thing.
//!
//! - `catalogued <property id> <name>`: a property appeared.
//! - `failed|passed <property id> <input hash> <vtime>`: one counterexample
//!   or example. A non-event property's sample is its value, as JSON.
//! - `completed <run id>`: the run is final, and the stream ends.
//!
//! A line that snouty cannot decode, an unknown `type` or data of an
//! unexpected shape, decodes as [`Record::Unknown`] and prints as its JSON, so
//! a server that adds a shape does not break an older snouty, and the stream
//! continues.
//!
//! To support a new notification shape, add its type to the match in
//! [`Record::parse`] (the generated type enum fails the build until it is
//! there), add a `Record` variant for it, and turn it into lines in
//! [`Denormalizer::lines`]. A new kind of output line is a [`Line`] variant,
//! which [`Line::render`] and [`Line::json`] must handle.

use std::collections::HashSet;

use color_eyre::eyre::Result;
use console::style;
use log::debug;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{Property, RunStreamNotificationType, decode_properties};
use crate::render::sanitize;
use crate::vtime::VTime;

use super::{mid_stream_error, stream_error_message};

#[derive(Debug)]
pub(super) enum Record {
    /// NEW_PROPERTIES_ADDED.
    Catalogued(Vec<Property>),
    /// PROPERTIES_PASSED, PROPERTIES_FAILED, or NEW_EXAMPLES_FOUND: the
    /// properties carry their samples.
    Samples(Vec<Property>),
    /// RUN_FINALIZED: the run is final, and the stream ends successfully.
    Finalized,
    Unknown(Value),
}

impl Record {
    /// A `Stream_Error` line returns an error: the server ended the stream
    /// before the run was final.
    pub(super) fn parse(line: &Value) -> Result<Self> {
        // A notification's `type` marks it as data, even if it also carries
        // an `error` key.
        if line.get("type").is_none()
            && let Some(message) = stream_error_message(line)
        {
            return Err(mid_stream_error(message));
        }
        let Ok(kind) = RunStreamNotificationType::deserialize(&line["type"]) else {
            return Ok(Self::Unknown(line.clone()));
        };
        let catalogued = match kind {
            RunStreamNotificationType::RunFinalized => return Ok(Self::Finalized),
            RunStreamNotificationType::NewPropertiesAdded => true,
            RunStreamNotificationType::PropertiesPassed
            | RunStreamNotificationType::PropertiesFailed
            | RunStreamNotificationType::NewExamplesFound => false,
        };
        match decode_properties(&line["data"]) {
            Ok(properties) if catalogued => Ok(Self::Catalogued(properties)),
            Ok(properties) => Ok(Self::Samples(properties)),
            Err(err) => {
                debug!("decoding a {kind} notification: {err:#}");
                Ok(Self::Unknown(line.clone()))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Outcome {
    Passed,
    Failed,
}

/// One sample of a property: an event property's moment, or a non-event
/// property's value.
#[derive(Debug, PartialEq)]
pub(super) enum Sample {
    Moment { input_hash: String, vtime: VTime },
    Value(Value),
}

/// One output line of `runs stream`.
#[derive(Debug, PartialEq)]
pub(super) enum Line {
    Catalogued {
        property_id: String,
        name: String,
    },
    Sample {
        outcome: Outcome,
        property_id: String,
        sample: Sample,
    },
    Completed {
        run_id: String,
    },
    Unknown(Value),
}

/// Turns records into lines. A status notification repeats the samples that
/// the server keeps for a property, so a sample prints only the first time it
/// arrives.
#[derive(Default)]
pub(super) struct Denormalizer {
    seen: HashSet<(String, Outcome, String)>,
}

impl Denormalizer {
    pub(super) fn lines(&mut self, record: Record, run_id: &str) -> Vec<Line> {
        match record {
            Record::Catalogued(properties) => properties
                .into_iter()
                .map(|property| Line::Catalogued {
                    property_id: property_id(&property),
                    name: property.name().to_string(),
                })
                .collect(),
            Record::Samples(properties) => properties
                .into_iter()
                .flat_map(samples)
                .filter(|line| self.first_time(line))
                .collect(),
            Record::Finalized => vec![Line::Completed {
                run_id: run_id.to_string(),
            }],
            Record::Unknown(line) => vec![Line::Unknown(line)],
        }
    }

    fn first_time(&mut self, line: &Line) -> bool {
        let Line::Sample {
            outcome,
            property_id,
            sample,
        } = line
        else {
            return true;
        };
        let key = match sample {
            Sample::Moment { input_hash, vtime } => format!("{input_hash} {vtime}"),
            Sample::Value(value) => value.to_string(),
        };
        self.seen.insert((property_id.clone(), *outcome, key))
    }
}

/// Every run that serves the stream (release 65.3 and later) gives each
/// property an ID, but the field stays optional in the spec.
fn property_id(property: &Property) -> String {
    property.id().unwrap_or("-").to_string()
}

/// A property's samples as lines, counterexamples first.
fn samples(property: Property) -> Vec<Line> {
    let property_id = property_id(&property);
    let line = |outcome, sample| Line::Sample {
        outcome,
        property_id: property_id.clone(),
        sample,
    };
    let moment = |event: crate::api::Event| Sample::Moment {
        input_hash: event.moment.input_hash,
        vtime: event.moment.vtime,
    };
    match property {
        Property::EventProperty(p) => {
            let failed = p
                .counterexamples
                .into_iter()
                .map(|e| line(Outcome::Failed, moment(e)));
            let passed = p
                .examples
                .into_iter()
                .map(|e| line(Outcome::Passed, moment(e)));
            failed.chain(passed).collect()
        }
        Property::NonEventProperty(p) => {
            let failed = p
                .counterexamples
                .into_iter()
                .map(|v| line(Outcome::Failed, Sample::Value(v)));
            let passed = p
                .examples
                .into_iter()
                .map(|v| line(Outcome::Passed, Sample::Value(v)));
            failed.chain(passed).collect()
        }
    }
}

/// The width of the event column: `catalogued` is the widest event.
const EVENT_WIDTH: usize = 10;

impl Line {
    /// The line for the terminal, without a trailing newline.
    pub(super) fn render(&self) -> String {
        match self {
            Self::Catalogued { property_id, name } => format!(
                "{:<EVENT_WIDTH$}  {}  {}",
                style("catalogued").cyan(),
                sanitize(property_id),
                sanitize(name)
            ),
            Self::Sample {
                outcome,
                property_id,
                sample,
            } => {
                let event = match outcome {
                    Outcome::Passed => style("passed").green(),
                    Outcome::Failed => style("failed").red(),
                };
                let sample = match sample {
                    Sample::Moment { input_hash, vtime } => {
                        format!("{}  {vtime}", sanitize(input_hash))
                    }
                    Sample::Value(value) => sanitize(&value.to_string()),
                };
                format!("{event:<EVENT_WIDTH$}  {}  {sample}", sanitize(property_id))
            }
            Self::Completed { run_id } => format!(
                "{:<EVENT_WIDTH$}  {}",
                style("completed").bold(),
                sanitize(run_id)
            ),
            Self::Unknown(line) => style(sanitize(&line.to_string())).dim().to_string(),
        }
    }

    /// The line for `--json`. An unknown notification prints as the server
    /// sent it.
    pub(super) fn json(&self) -> Value {
        match self {
            Self::Catalogued { property_id, name } => {
                json!({"event": "catalogued", "property_id": property_id, "name": name})
            }
            Self::Sample {
                outcome,
                property_id,
                sample,
            } => {
                let event = match outcome {
                    Outcome::Passed => "passed",
                    Outcome::Failed => "failed",
                };
                match sample {
                    Sample::Moment { input_hash, vtime } => json!({
                        "event": event,
                        "property_id": property_id,
                        "input_hash": input_hash,
                        "vtime": vtime,
                    }),
                    Sample::Value(value) => {
                        json!({"event": event, "property_id": property_id, "value": value})
                    }
                }
            }
            Self::Completed { run_id } => json!({"event": "completed", "run_id": run_id}),
            Self::Unknown(line) => line.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUN: &str = "run-1";

    fn property(id: &str, status: &str, examples: Value, counterexamples: Value) -> Value {
        json!({
            "id": id,
            "name": format!("Property {id}"),
            "status": status,
            "is_event": true,
            "example_count": 24,
            "counterexample_count": 2,
            "examples": examples,
            "counterexamples": counterexamples,
        })
    }

    fn moment(hash: &str, vtime: &str) -> Value {
        json!({"moment": {"input_hash": hash, "vtime": vtime}})
    }

    fn rendered(denormalizer: &mut Denormalizer, line: Value) -> Vec<String> {
        console::set_colors_enabled(false);
        let record = Record::parse(&line).unwrap();
        denormalizer
            .lines(record, RUN)
            .iter()
            .map(Line::render)
            .collect()
    }

    fn render(line: Value) -> Vec<String> {
        rendered(&mut Denormalizer::default(), line)
    }

    #[test]
    fn a_new_property_is_catalogued() {
        let line = json!({"type": "NEW_PROPERTIES_ADDED", "data": [
            property("a1", "Undetermined", json!([]), json!([])),
            property("b2", "Undetermined", json!([]), json!([])),
        ]});
        assert_eq!(
            render(line),
            ["catalogued  a1  Property a1", "catalogued  b2  Property b2"]
        );
    }

    #[test]
    fn each_sample_is_one_line_counterexamples_first() {
        for kind in [
            "NEW_EXAMPLES_FOUND",
            "PROPERTIES_FAILED",
            "PROPERTIES_PASSED",
        ] {
            let p = property(
                "a1",
                "Failing",
                json!([moment("-300", "398.4898056755774")]),
                json!([moment("-100", "401.25"), moment("-200", "402.5")]),
            );
            assert_eq!(
                render(json!({"type": kind, "data": [p]})),
                [
                    "failed      a1  -100  401.25",
                    "failed      a1  -200  402.5",
                    "passed      a1  -300  398.4898056755774",
                ],
                "{kind}"
            );
        }
    }

    #[test]
    fn a_repeated_sample_prints_once() {
        let mut denormalizer = Denormalizer::default();
        let first = property(
            "a1",
            "Undetermined",
            json!([]),
            json!([moment("-1", "1.5")]),
        );
        let again = property(
            "a1",
            "Failing",
            json!([]),
            json!([moment("-1", "1.5"), moment("-2", "2.5")]),
        );
        let found = json!({"type": "NEW_EXAMPLES_FOUND", "data": [first]});
        let failed = json!({"type": "PROPERTIES_FAILED", "data": [again]});
        assert_eq!(
            rendered(&mut denormalizer, found),
            ["failed      a1  -1  1.5"]
        );
        assert_eq!(
            rendered(&mut denormalizer, failed),
            ["failed      a1  -2  2.5"]
        );
    }

    #[test]
    fn a_non_event_sample_renders_as_json() {
        let mut p = property("a1", "Failing", json!([820]), json!([{"peak": 1340}]));
        p["is_event"] = json!(false);
        assert_eq!(
            render(json!({"type": "PROPERTIES_FAILED", "data": [p]})),
            [r#"failed      a1  {"peak":1340}"#, "passed      a1  820"]
        );
    }

    #[test]
    fn run_finalized_completes_the_run() {
        let line = json!({"type": "RUN_FINALIZED", "data": []});
        assert!(matches!(Record::parse(&line).unwrap(), Record::Finalized));
        assert_eq!(render(line), ["completed   run-1"]);
    }

    #[test]
    fn colors_do_not_change_the_column_widths() {
        let line = json!({"type": "RUN_FINALIZED", "data": []});
        let lines = Denormalizer::default().lines(Record::parse(&line).unwrap(), RUN);
        console::set_colors_enabled(true);
        let colored = lines[0].render();
        console::set_colors_enabled(false);
        assert_ne!(colored, lines[0].render());
        assert_eq!(console::strip_ansi_codes(&colored), "completed   run-1");
    }

    #[test]
    fn an_unknown_type_renders_its_json() {
        let line = json!({"type": "RUN_PAUSED", "data": []});
        assert!(matches!(Record::parse(&line).unwrap(), Record::Unknown(_)));
        assert_eq!(render(line), [r#"{"type":"RUN_PAUSED","data":[]}"#]);
    }

    #[test]
    fn a_known_type_with_malformed_data_renders_its_json() {
        let line = json!({"type": "PROPERTIES_FAILED", "data": [{"status": "Skipped"}]});
        assert!(matches!(Record::parse(&line).unwrap(), Record::Unknown(_)));
    }

    #[test]
    fn a_notification_with_no_properties_prints_nothing() {
        assert!(render(json!({"type": "PROPERTIES_PASSED", "data": []})).is_empty());
    }

    #[test]
    fn a_notification_with_an_error_key_is_data() {
        let line = json!({"type": "RUN_FINALIZED", "data": [], "error": "none"});
        assert!(matches!(Record::parse(&line).unwrap(), Record::Finalized));
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
    fn json_lines_name_the_event_and_its_fields() {
        let p = property(
            "a1",
            "Failing",
            json!([]),
            json!([moment("-100", "401.25")]),
        );
        let record = Record::parse(&json!({"type": "PROPERTIES_FAILED", "data": [p]})).unwrap();
        let lines = Denormalizer::default().lines(record, RUN);
        assert_eq!(
            lines[0].json(),
            json!({"event": "failed", "property_id": "a1", "input_hash": "-100", "vtime": 401.25})
        );
        let done = Denormalizer::default().lines(Record::Finalized, RUN);
        assert_eq!(
            done[0].json(),
            json!({"event": "completed", "run_id": "run-1"})
        );
    }
}
