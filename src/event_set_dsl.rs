//! The event-set DSL.
//!
//! An event set is a pipeline of verbs separated by `.`, applied left to
//! right to a run's event stream — e.g. `contains({output_text: "raft"})` or
//! `matches({container: "etcd0"}).with_next()`. The events-search endpoint
//! (`POST /runs/{run_id}/events/search`) executes one such pipeline
//! server-side; the API is expected to grow more places that take an event
//! set, so everything snouty knows about the DSL lives here.

use crate::vtime::VTime;

/// The DSL's verbs. A pipeline starts with one of these.
pub const VERBS: &[&str] = &[
    "matches",
    "contains",
    "not_matches",
    "excludes",
    "map",
    "filter",
    "flatmap",
    "fold",
    "narrow",
    "union",
    "intersect",
    "difference",
    "distinct_by_moment",
    "with_last",
    "with_next",
];

/// The event-set expression `(ev, needles)` that decides whether one event
/// holds every needle.
pub(crate) const NEEDLE_FILTER: &str = include_str!("event_set_dsl/needle_filter.pangolin.js");

/// The condition that drops bootstrap events. They come before a run's log
/// timeline starts, so their moments have no log stream: a hash taken from
/// one leads `runs logs` nowhere.
pub(crate) const SKIP_BOOTSTRAP: &str = r#"ev.source?.name !== "bootstrap""#;

/// Build the event set that keeps the events holding every needle, bootstrap
/// events aside.
pub fn substring_filter(needles: &[String]) -> String {
    let lowercased: Vec<String> = needles.iter().map(|needle| needle.to_lowercase()).collect();
    let needles = serde_json::to_string(&lowercased).expect("strings serialize to JSON");
    format!(
        "filter(ev => {SKIP_BOOTSTRAP} && ({})(ev, {needles}))",
        NEEDLE_FILTER.trim()
    )
}

/// The event-set expression `(ev, moment)` that decides whether one event
/// sits under `moment.input_hash`, at most `moment.tolerance` seconds from
/// `moment.vtime`. The hash compares as a string: as a number it loses
/// precision, and hashes that differ in their last digits compare equal.
pub(crate) const MOMENT_FILTER: &str = "(ev, moment) => ev.moment.input_hash == moment.input_hash \
     && Math.abs(ev.moment.vtime - moment.vtime) <= moment.tolerance";

/// Build the event set that keeps the events under `input_hash` at most
/// `tolerance` seconds from `vtime`.
pub fn moment_filter(input_hash: &str, vtime: VTime, tolerance: f64) -> String {
    let moment = serde_json::json!({
        "input_hash": input_hash,
        "vtime": vtime,
        "tolerance": tolerance,
    });
    format!("filter(ev => ({MOMENT_FILTER})(ev, {moment}))")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The needles ride as one JSON array, lowercased, with quotes and
    // backslashes escaped rather than breaking the query.
    #[test]
    fn substring_filter_passes_lowercased_escaped_needles() {
        let query = substring_filter(&["Raft".to_string(), r#"say "hi"\"#.to_string()]);
        assert!(
            query.ends_with(r#")(ev, ["raft","say \"hi\"\\"]))"#),
            "got: {query}"
        );
    }

    // Bootstrap events are dropped before any needle is tried.
    #[test]
    fn substring_filter_skips_bootstrap_events() {
        let query = substring_filter(&["error".to_string()]);
        assert!(
            query.starts_with(&format!("filter(ev => {SKIP_BOOTSTRAP} && (")),
            "got: {query}"
        );
    }

    // A needle is matched against the event's text fields, never against the
    // event's JSON: matching that string made a field name a needle.
    #[test]
    fn substring_filter_reads_fields_not_the_event_json() {
        let query = substring_filter(&["raft".to_string()]);
        assert!(query.contains(NEEDLE_FILTER.trim()), "got: {query}");
        for field in [
            "ev.output_text",
            "ev.antithesis_assert.message",
            "ev.antithesis_assert.location?.function",
            "ev.command",
            "ev.started_task",
        ] {
            assert!(query.contains(field), "{field} missing from: {query}");
        }
    }
}
