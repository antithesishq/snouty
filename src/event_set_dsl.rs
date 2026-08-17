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

/// Build the event set that ANDs every needle as a case-insensitive
/// substring of the raw event JSON. The raw JSON — not snouty's rendered
/// form — is the haystack: a needle that exists only in the rendering finds
/// nothing, and a match inside a field the rendering elides still returns
/// its event.
pub fn substring_filter(needles: &[String]) -> String {
    let clauses = needles
        .iter()
        .map(|needle| {
            let literal =
                serde_json::to_string(&needle.to_lowercase()).expect("a string serializes to JSON");
            format!("h.includes({literal})")
        })
        .collect::<Vec<_>>()
        .join(" && ");
    format!("filter(ev => ((h) => {clauses})(JSON.stringify(ev).toLowerCase()))")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The substring filter binds the lowercased raw-JSON haystack once, then
    // matches each needle against it, embedding needles as JSON string
    // literals so quotes and backslashes arrive escaped rather than breaking
    // the query.
    #[test]
    fn substring_filter_conjoins_lowercased_escaped_needles() {
        let query = substring_filter(&["Raft".to_string(), r#"say "hi"\"#.to_string()]);
        assert!(query.starts_with("filter(ev => ((h) => "), "got: {query}");
        assert!(
            query.ends_with(r#")(JSON.stringify(ev).toLowerCase()))"#),
            "got: {query}"
        );
        assert!(query.contains(r#"h.includes("raft")"#), "got: {query}");
        assert!(
            query.contains(r#"h.includes("say \"hi\"\\")"#),
            "got: {query}"
        );
        assert_eq!(query.matches("h.includes(").count(), 2, "got: {query}");
        assert!(query.contains(" && "), "got: {query}");
    }
}
