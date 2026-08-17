//! The event-set DSL.
//!
//! An event set is a pipeline of verbs separated by `.`, applied left to
//! right to a run's event stream — e.g. `contains({output_text: "raft"})` or
//! `matches({container: "etcd0"}).with_next()`. The events-search endpoint
//! (`POST /runs/{run_id}/events/search`) executes one such pipeline
//! server-side; the API is expected to grow more places that take an event
//! set, so everything snouty knows about the DSL lives here.

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

/// Defines `has(needle)`: whether one lowercased needle appears in the text
/// an event carries. This is a port of the matcher behind
/// `GET /runs/{run_id}/events`, field for field and gate for gate, so
/// `runs events` answers the same on either backend: log text, an
/// assertion's message and source function, and a test-composer command.
///
/// The event's own JSON is NOT the haystack. Matching that string matched
/// field names and text spanning two fields, so `-m IPT_bytes_out` returned
/// every event while the same command on the other backend returned none.
///
/// [`substring_filter`] emits this before the needles, so a reader of the
/// query can cut it away and be left with the needles alone.
pub(crate) const HAS_NEEDLE: &str = concat!(
    r#"const m = (s, needle) => s != null && String(s).toLowerCase().includes(needle); "#,
    r#"const assert_hit = ev.antithesis_assert?.hit === true; "#,
    r#"const not_catalog = !ev.antithesis_assert || ev.antithesis_assert.hit !== false; "#,
    r#"const composer = ev.source?.name === "antithesis_test_composer" "#,
    r#"|| /^antithesis\/pods\/.*\/commands/.test(ev.source?.name ?? ""); "#,
    r#"const has = needle => (not_catalog && m(ev.output_text, needle)) "#,
    r#"|| (assert_hit && (m(ev.antithesis_assert.message, needle) "#,
    r#"|| m(ev.antithesis_assert.location?.function, needle))) "#,
    r#"|| (composer && (m(ev.source?.name, needle) || m(ev.command, needle) "#,
    r#"|| m(ev.started_task, needle))); "#,
);

/// Build the event set that ANDs every needle as a case-insensitive
/// substring of the fields [`HAS_NEEDLE`] searches. Needles arrive
/// lowercased, because `has` folds only the haystack.
pub fn substring_filter(needles: &[String]) -> String {
    let clauses = needles
        .iter()
        .map(|needle| {
            let literal =
                serde_json::to_string(&needle.to_lowercase()).expect("a string serializes to JSON");
            format!("has({literal})")
        })
        .collect::<Vec<_>>()
        .join(" && ");
    format!("filter(ev => {{ {HAS_NEEDLE}return {clauses}; }})")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The substring filter defines `has` once, then applies it to each
    // needle, embedding needles as JSON string literals so quotes and
    // backslashes arrive escaped rather than breaking the query.
    #[test]
    fn substring_filter_conjoins_lowercased_escaped_needles() {
        let query = substring_filter(&["Raft".to_string(), r#"say "hi"\"#.to_string()]);
        assert!(
            query.starts_with("filter(ev => { const m = "),
            "got: {query}"
        );
        assert!(
            query.ends_with(r#"return has("raft") && has("say \"hi\"\\"); })"#),
            "got: {query}"
        );
        assert_eq!(query.matches("has(\"").count(), 2, "got: {query}");
    }

    // A needle is matched against the event's text fields, never against the
    // event's JSON: matching that string made a field name a needle.
    #[test]
    fn substring_filter_reads_fields_not_the_event_json() {
        let query = substring_filter(&["raft".to_string()]);
        assert!(!query.contains("JSON.stringify"), "got: {query}");
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
