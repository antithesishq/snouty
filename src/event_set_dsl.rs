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
/// substring of the raw event. The JS predicate matches each needle against
/// the whole raw event JSON (`JSON.stringify`), lowercased on both sides, so
/// the server prefilter sees every field — hash, vtime, source, output text,
/// and the structured payloads (assertions, composer/fault fields) that
/// carry no `output_text` at all. The haystack is bound once per event
/// through an immediately-invoked arrow function, so the server stringifies
/// and lowercases each event once, not once per needle (the form is accepted
/// by the server's validator; verified on tenant release 60.1). The raw JSON
/// — not snouty's rendered form — is the only haystack: a needle that exists
/// only in the rendering (e.g. the assertion summary's `must-hit` marker for
/// the raw `must_hit` field) finds nothing, and conversely a match inside a
/// field the rendering elides still returns its event. That asymmetry is the
/// accepted cost of filtering entirely server-side. Each needle is embedded
/// as a JSON string literal (also a valid JS string literal), so escaping is
/// serde's problem, not string surgery here.
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

/// Every double-quoted string literal in `query`, with `\"` and `\\` escapes
/// resolved. The mock API server uses this to "interpret" a query without a
/// DSL engine: requiring each literal somewhere in an event covers
/// `contains({...})` needles and [`substring_filter`] pipelines alike.
pub fn extract_quoted_literals(query: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let mut chars = query.chars();
    while let Some(c) = chars.next() {
        if c != '"' {
            continue;
        }
        let mut literal = String::new();
        loop {
            match chars.next() {
                Some('\\') => {
                    if let Some(escaped) = chars.next() {
                        literal.push(escaped);
                    }
                }
                Some('"') | None => break,
                Some(other) => literal.push(other),
            }
        }
        literals.push(literal);
    }
    literals
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

    #[test]
    fn extract_quoted_literals_resolves_escapes() {
        assert_eq!(
            extract_quoted_literals(r#"contains({output_text: "slow request"})"#),
            vec!["slow request".to_string()]
        );
        // Escaped quotes and backslashes arrive resolved; the JS filter's
        // bare " " separators come out as literals too (callers drop blanks).
        assert_eq!(
            extract_quoted_literals(r#"filter(ev => (a + " ").includes("say \"hi\"\\"))"#),
            vec![" ".to_string(), r#"say "hi"\"#.to_string()]
        );
        assert!(extract_quoted_literals("matches({a: 1})").is_empty());
    }
}
