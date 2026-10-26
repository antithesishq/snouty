//! `runs stream`: the run status stream, denormalized to one line per event.
//!
//! Each NDJSON line decodes into a [`Record`], and [`Denormalizer::lines`]
//! flattens it into [`Line`]s. A notification lists whole properties, and the
//! server repeats properties and samples across notifications, so the
//! denormalizer remembers what it printed:
//!
//! - `property <property id> <name>`: the first time a property appears.
//! - `sample <property id> example|counterexample <moment|value>`: the first
//!   time a sample appears. An event property's sample is a moment, which is
//!   unique across the run; a non-event property's sample is a JSON value,
//!   unique only within its property.
//! - `verdict <property id> passed|failed`: a `PROPERTIES_PASSED` or
//!   `PROPERTIES_FAILED` notification, unless it repeats the last verdict.
//! - `completed <run id>`: `RUN_FINALIZED`; the stream ends.
//! - `unknown <type> <json>`: a notification that snouty cannot decode, an
//!   unknown `type` or data of an unexpected shape. It prints as its JSON, so
//!   a server that adds a shape does not break an older snouty.
//!
//! To support a new notification shape, add its type to the match in
//! [`Record::parse`] (the generated type enum fails the build until it is
//! there), add a `Record` variant for it, and turn it into lines in
//! [`Denormalizer::lines`]. A new kind of output line is a [`Line`] variant,
//! which [`Line::render`] and [`Line::json`] must handle.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use color_eyre::eyre::Result;
use console::style;
use log::debug;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{Event, Property, RunStreamNotificationType, decode_properties};
use crate::render::sanitize;
use crate::vtime::VTime;

use super::{mid_stream_error, stream_error_message};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Verdict {
    Passed,
    Failed,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SampleKind {
    Example,
    Counterexample,
}

impl fmt::Display for SampleKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(match self {
            Self::Example => "example",
            Self::Counterexample => "counterexample",
        })
    }
}

/// One sample of a property: an event property's moment, or a non-event
/// property's value.
#[derive(Debug, PartialEq)]
pub(super) enum Sample {
    Moment { input_hash: String, vtime: VTime },
    Value(Value),
}

#[derive(Debug)]
pub(super) enum Record {
    /// `NEW_PROPERTIES_ADDED` or `NEW_EXAMPLES_FOUND` (no verdict), or
    /// `PROPERTIES_PASSED` or `PROPERTIES_FAILED` (a verdict).
    Properties {
        verdict: Option<Verdict>,
        properties: Vec<Property>,
    },
    /// `RUN_FINALIZED`: the run is final, and the stream ends successfully.
    Finalized,
    Unknown(Value),
}

impl Record {
    /// A `Stream_Error` line returns an error: the server ended the stream
    /// before the run was final.
    pub(super) fn parse(line: Value) -> Result<Self> {
        if let Some(message) = stream_error_message(&line) {
            return Err(mid_stream_error(message));
        }
        let Ok(kind) = RunStreamNotificationType::deserialize(&line["type"]) else {
            return Ok(Self::Unknown(line));
        };
        let verdict = match kind {
            RunStreamNotificationType::RunFinalized => return Ok(Self::Finalized),
            RunStreamNotificationType::NewPropertiesAdded
            | RunStreamNotificationType::NewExamplesFound => None,
            RunStreamNotificationType::PropertiesPassed => Some(Verdict::Passed),
            RunStreamNotificationType::PropertiesFailed => Some(Verdict::Failed),
        };
        match decode_properties(&line["data"]) {
            Ok(properties) => Ok(Self::Properties {
                verdict,
                properties,
            }),
            Err(err) => {
                debug!("decoding a {kind} notification: {err:#}");
                Ok(Self::Unknown(line))
            }
        }
    }
}

/// The width of the event column: `completed` is the widest event.
const EVENT_WIDTH: usize = 9;
/// The width of the sample kind column: `counterexample` is the widest kind.
const KIND_WIDTH: usize = 14;

#[derive(Debug, PartialEq)]
pub(super) enum Line {
    Property {
        property_id: String,
        name: String,
    },
    Sample {
        property_id: String,
        kind: SampleKind,
        sample: Sample,
    },
    Verdict {
        property_id: String,
        verdict: Verdict,
    },
    Completed {
        run_id: String,
    },
    Unknown(Value),
}

impl Line {
    /// The line for the terminal, without a trailing newline.
    pub(super) fn render(&self) -> String {
        match self {
            Self::Property { property_id, name } => format!(
                "{:<EVENT_WIDTH$}  {}  {}",
                style("property").cyan(),
                sanitize(property_id),
                sanitize(name)
            ),
            Self::Sample {
                property_id,
                kind,
                sample,
            } => {
                let sample = match sample {
                    Sample::Moment { input_hash, vtime } => {
                        format!("{}  {vtime}", sanitize(input_hash))
                    }
                    Sample::Value(value) => sanitize(&value.to_string()),
                };
                format!(
                    "{:<EVENT_WIDTH$}  {}  {kind:<KIND_WIDTH$}  {sample}",
                    "sample",
                    sanitize(property_id),
                )
            }
            Self::Verdict {
                property_id,
                verdict,
            } => {
                let word = match verdict {
                    Verdict::Passed => style(verdict).green(),
                    Verdict::Failed => style(verdict).red(),
                };
                format!(
                    "{:<EVENT_WIDTH$}  {}  {word}",
                    style("verdict").bold(),
                    sanitize(property_id)
                )
            }
            Self::Completed { run_id } => format!(
                "{:<EVENT_WIDTH$}  {}",
                style("completed").bold(),
                sanitize(run_id)
            ),
            Self::Unknown(line) => {
                let kind = line["type"].as_str().unwrap_or("-");
                format!(
                    "{:<EVENT_WIDTH$}  {}  {}",
                    style("unknown").yellow(),
                    sanitize(kind),
                    style(sanitize(&line.to_string())).dim()
                )
            }
        }
    }

    /// The line for `--json`.
    pub(super) fn json(&self) -> Value {
        match self {
            Self::Property { property_id, name } => {
                json!({"event": "property", "property_id": property_id, "name": name})
            }
            Self::Sample {
                property_id,
                kind,
                sample,
            } => match sample {
                Sample::Moment { input_hash, vtime } => json!({
                    "event": "sample",
                    "property_id": property_id,
                    "kind": kind.to_string(),
                    "input_hash": input_hash,
                    "vtime": vtime,
                }),
                Sample::Value(value) => json!({
                    "event": "sample",
                    "property_id": property_id,
                    "kind": kind.to_string(),
                    "value": value,
                }),
            },
            Self::Verdict {
                property_id,
                verdict,
            } => {
                json!({"event": "verdict", "property_id": property_id, "verdict": verdict.to_string()})
            }
            Self::Completed { run_id } => json!({"event": "completed", "run_id": run_id}),
            Self::Unknown(line) => json!({"event": "unknown", "data": line}),
        }
    }
}

/// The spec makes the ID optional, but every run with a stream (release 65.3
/// and later) has one.
fn property_id(property: &Property) -> String {
    property.id().unwrap_or("-").to_string()
}

/// A property's samples, counterexamples first.
fn samples(property: Property) -> impl Iterator<Item = (SampleKind, Sample)> {
    let moments = |events: Vec<Event>| -> Vec<Sample> {
        events
            .into_iter()
            .map(|event| Sample::Moment {
                input_hash: event.moment.input_hash,
                vtime: event.moment.vtime,
            })
            .collect()
    };
    let (counterexamples, examples) = match property {
        Property::EventProperty(p) => (moments(p.counterexamples), moments(p.examples)),
        Property::NonEventProperty(p) => (
            p.counterexamples.into_iter().map(Sample::Value).collect(),
            p.examples.into_iter().map(Sample::Value).collect(),
        ),
    };
    let counterexamples = counterexamples
        .into_iter()
        .map(|sample| (SampleKind::Counterexample, sample));
    let examples = examples
        .into_iter()
        .map(|sample| (SampleKind::Example, sample));
    counterexamples.chain(examples)
}

/// Turns records into lines, and remembers what it printed.
#[derive(Default)]
pub(super) struct Denormalizer {
    properties: HashSet<String>,
    moments: BTreeSet<(String, VTime)>,
    values: HashSet<(String, String)>,
    verdicts: HashMap<String, Verdict>,
}

impl Denormalizer {
    pub(super) fn lines(&mut self, record: Record, run_id: &str) -> Vec<Line> {
        match record {
            Record::Properties {
                verdict,
                properties,
            } => properties
                .into_iter()
                .flat_map(|property| self.property_lines(property, verdict))
                .collect(),
            Record::Finalized => vec![Line::Completed {
                run_id: run_id.to_string(),
            }],
            Record::Unknown(line) => vec![Line::Unknown(line)],
        }
    }

    /// A property's new lines: the property, then its new samples, then its
    /// verdict if it changed.
    fn property_lines(&mut self, property: Property, verdict: Option<Verdict>) -> Vec<Line> {
        let property_id = property_id(&property);
        let mut lines = Vec::new();
        if !self.properties.contains(&property_id) {
            self.properties.insert(property_id.clone());
            lines.push(Line::Property {
                property_id: property_id.clone(),
                name: property.name().to_string(),
            });
        }
        for (kind, sample) in samples(property) {
            let new = match &sample {
                Sample::Moment { input_hash, vtime } => {
                    self.moments.insert((input_hash.clone(), *vtime))
                }
                Sample::Value(value) => {
                    self.values.insert((property_id.clone(), value.to_string()))
                }
            };
            if new {
                lines.push(Line::Sample {
                    property_id: property_id.clone(),
                    kind,
                    sample,
                });
            }
        }
        if let Some(verdict) = verdict
            && self.verdicts.insert(property_id.clone(), verdict) != Some(verdict)
        {
            lines.push(Line::Verdict {
                property_id,
                verdict,
            });
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUN: &str = "run-1";

    /// An event property. The status shows in no line.
    fn property(id: &str, examples: &Value, counterexamples: &Value) -> Value {
        json!({
            "id": id,
            "name": format!("Property {id}"),
            "status": "Passing",
            "is_event": true,
            "example_count": 24,
            "counterexample_count": 2,
            "examples": examples,
            "counterexamples": counterexamples,
        })
    }

    fn non_event(id: &str, examples: &Value, counterexamples: &Value) -> Value {
        let mut property = property(id, examples, counterexamples);
        property["is_event"] = json!(false);
        property
    }

    fn moment(hash: &str, vtime: &str) -> Value {
        json!({"moment": {"input_hash": hash, "vtime": vtime}})
    }

    fn rendered(denormalizer: &mut Denormalizer, line: &Value) -> Vec<String> {
        console::set_colors_enabled(false);
        let record = Record::parse(line.clone()).unwrap();
        denormalizer
            .lines(record, RUN)
            .iter()
            .map(Line::render)
            .collect()
    }

    fn render(line: &Value) -> Vec<String> {
        rendered(&mut Denormalizer::default(), line)
    }

    #[test]
    fn a_property_prints_the_first_time_it_appears() {
        let mut denormalizer = Denormalizer::default();
        let p = property("a1", &json!([]), &json!([]));
        let added = json!({"type": "NEW_PROPERTIES_ADDED", "data": [p]});
        assert_eq!(
            rendered(&mut denormalizer, &added),
            ["property   a1  Property a1"]
        );
        assert!(rendered(&mut denormalizer, &added).is_empty());
    }

    #[test]
    fn a_property_first_seen_in_a_sample_notification_prints() {
        let p = property("a1", &json!([moment("-1", "1.5")]), &json!([]));
        assert_eq!(
            render(&json!({"type": "NEW_EXAMPLES_FOUND", "data": [p]})),
            [
                "property   a1  Property a1",
                "sample     a1  example         -1  1.5",
            ]
        );
    }

    #[test]
    fn samples_print_counterexamples_first_then_the_verdict() {
        let p = property(
            "a1",
            &json!([moment("-300", "398.4898056755774")]),
            &json!([moment("-100", "401.25"), moment("-200", "402.5")]),
        );
        assert_eq!(
            render(&json!({"type": "PROPERTIES_FAILED", "data": [p]})),
            [
                "property   a1  Property a1",
                "sample     a1  counterexample  -100  401.25",
                "sample     a1  counterexample  -200  402.5",
                "sample     a1  example         -300  398.4898056755774",
                "verdict    a1  failed",
            ]
        );
    }

    #[test]
    fn a_moment_prints_once_across_notifications() {
        let mut denormalizer = Denormalizer::default();
        let p = property("a1", &json!([moment("-1", "1.5")]), &json!([]));
        let found = json!({"type": "NEW_EXAMPLES_FOUND", "data": [p.clone()]});
        let passed = json!({"type": "PROPERTIES_PASSED", "data": [p]});
        rendered(&mut denormalizer, &found);
        assert_eq!(
            rendered(&mut denormalizer, &passed),
            ["verdict    a1  passed"]
        );
    }

    #[test]
    fn a_repeated_verdict_prints_once() {
        let mut denormalizer = Denormalizer::default();
        let p = property("a1", &json!([]), &json!([]));
        let passed = json!({"type": "PROPERTIES_PASSED", "data": [p]});
        rendered(&mut denormalizer, &passed);
        assert!(rendered(&mut denormalizer, &passed).is_empty());
    }

    #[test]
    fn a_non_event_sample_is_unique_within_its_property() {
        let a = non_event("a1", &json!([true]), &json!([]));
        let b = non_event("b2", &json!([true]), &json!([]));
        let lines = render(&json!({"type": "NEW_EXAMPLES_FOUND", "data": [a.clone(), b, a]}));
        let samples: Vec<&String> = lines.iter().filter(|l| l.starts_with("sample")).collect();
        assert_eq!(
            samples,
            [
                "sample     a1  example         true",
                "sample     b2  example         true",
            ]
        );
    }

    #[test]
    fn a_passing_property_can_carry_a_counterexample() {
        let p = non_event("a1", &json!([]), &json!(["old image"]));
        assert_eq!(
            render(&json!({"type": "PROPERTIES_PASSED", "data": [p]})),
            [
                "property   a1  Property a1",
                r#"sample     a1  counterexample  "old image""#,
                "verdict    a1  passed",
            ]
        );
    }

    #[test]
    fn run_finalized_completes_the_run() {
        let line = json!({"type": "RUN_FINALIZED", "data": []});
        assert!(matches!(
            Record::parse(line.clone()).unwrap(),
            Record::Finalized
        ));
        assert_eq!(render(&line), ["completed  run-1"]);
    }

    #[test]
    fn colors_do_not_change_the_column_widths() {
        let p = property("a1", &json!([]), &json!([]));
        let line = json!({"type": "PROPERTIES_FAILED", "data": [p]});
        let lines = Denormalizer::default().lines(Record::parse(line).unwrap(), RUN);
        console::set_colors_enabled(true);
        let colored: Vec<String> = lines.iter().map(Line::render).collect();
        console::set_colors_enabled(false);
        let plain: Vec<String> = lines.iter().map(Line::render).collect();
        assert_ne!(colored, plain);
        let stripped: Vec<String> = colored
            .iter()
            .map(|l| console::strip_ansi_codes(l).to_string())
            .collect();
        assert_eq!(stripped, plain);
    }

    #[test]
    fn an_unknown_type_prints_its_type_and_json() {
        let line = json!({"type": "RUN_PAUSED", "data": []});
        assert!(matches!(
            Record::parse(line.clone()).unwrap(),
            Record::Unknown(_)
        ));
        assert_eq!(
            render(&line),
            [r#"unknown    RUN_PAUSED  {"type":"RUN_PAUSED","data":[]}"#]
        );
    }

    #[test]
    fn a_known_type_with_malformed_data_is_unknown() {
        let line = json!({"type": "PROPERTIES_FAILED", "data": [{"status": "Skipped"}]});
        assert!(matches!(Record::parse(line).unwrap(), Record::Unknown(_)));
    }

    #[test]
    fn a_notification_with_no_properties_prints_nothing() {
        assert!(render(&json!({"type": "PROPERTIES_PASSED", "data": []})).is_empty());
    }

    #[test]
    fn a_notification_with_an_error_key_is_data() {
        let line = json!({"type": "RUN_FINALIZED", "data": [], "error": "none"});
        assert!(matches!(Record::parse(line).unwrap(), Record::Finalized));
    }

    #[test]
    fn a_stream_error_fails_the_parse() {
        let err = Record::parse(json!({"error": "subscriber fell behind"})).unwrap_err();
        assert!(
            format!("{err}").contains("subscriber fell behind"),
            "got: {err}"
        );
    }

    #[test]
    fn json_lines_name_the_event_and_its_fields() {
        let p = property("a1", &json!([]), &json!([moment("-100", "401.25")]));
        let record = Record::parse(json!({"type": "PROPERTIES_FAILED", "data": [p]})).unwrap();
        let lines: Vec<Value> = Denormalizer::default()
            .lines(record, RUN)
            .iter()
            .map(Line::json)
            .collect();
        assert_eq!(
            lines,
            [
                json!({"event": "property", "property_id": "a1", "name": "Property a1"}),
                json!({"event": "sample", "property_id": "a1", "kind": "counterexample", "input_hash": "-100", "vtime": 401.25}),
                json!({"event": "verdict", "property_id": "a1", "verdict": "failed"}),
            ]
        );
        let unknown = json!({"type": "RUN_PAUSED"});
        assert_eq!(
            Line::Unknown(unknown.clone()).json(),
            json!({"event": "unknown", "data": unknown})
        );
        assert_eq!(
            Line::Completed {
                run_id: RUN.to_string()
            }
            .json(),
            json!({"event": "completed", "run_id": "run-1"})
        );
    }
}
