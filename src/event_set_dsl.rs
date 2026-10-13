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

/// Whether `fragment` holds a JavaScript comment opener (`//` or `/*`).
///
/// No query snouty sends may hold one. A tenant can rewrite a query before it
/// runs: on a customer tenant (release 62), a 400 from
/// `POST /runs/{run_id}/events/search` showed a `runs events` query inside a
/// server-built `filter(e => …)`, where a comment can hide the rest of the query.
const fn has_comment(fragment: &str) -> bool {
    let bytes = fragment.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && matches!(bytes[i + 1], b'/' | b'*') {
            return true;
        }
        i += 1;
    }
    false
}

/// Declare a query fragment, and fail the build if it holds a comment.
macro_rules! query_fragment {
    ($text:expr) => {
        const {
            assert!(
                !has_comment($text),
                "a query fragment holds a comment; move it into the Rust source"
            );
            $text
        }
    };
}

/// The event-set expression `(ev, needles)` that decides whether one event
/// holds every needle. The needles must be lowercase: `m` folds only the
/// haystack. An assertion catalog entry (`antithesis_assert.hit === false`)
/// never matches on `output_text`, because it is no evidence that anything ran.
pub(crate) const NEEDLE_FILTER: &str = query_fragment!(
    r#"(ev, needles) => {
  const m = (s, needle) => s != null && String(s).toLowerCase().includes(needle);
  const assert_hit = ev.antithesis_assert?.hit === true;
  const not_catalog = !ev.antithesis_assert || ev.antithesis_assert.hit !== false;
  const composer = ev.source?.name === "antithesis_test_composer"
    || /^antithesis\/pods\/.*\/commands/.test(ev.source?.name ?? "");
  const has = (needle) =>
    (not_catalog && m(ev.output_text, needle))
    || (assert_hit
      && (m(ev.antithesis_assert.message, needle)
        || m(ev.antithesis_assert.location?.function, needle)))
    || (composer
      && (m(ev.source?.name, needle)
        || m(ev.command, needle)
        || m(ev.started_task, needle)));
  return needles.every(has);
}"#
);

/// The condition that drops bootstrap events. They come before a run's log
/// timeline starts, so their moments have no log stream: a hash taken from
/// one leads `runs logs` nowhere.
pub(crate) const SKIP_BOOTSTRAP: &str = query_fragment!(r#"ev.source?.name !== "bootstrap""#);

/// `value` as a JavaScript literal, with every `/` written `\u002f` so user
/// text such as `http://` or `/*` puts no comment opener in a query.
fn js_literal(value: &(impl serde::Serialize + ?Sized)) -> String {
    serde_json::to_string(value)
        .expect("strings serialize to JSON")
        .replace('/', "\\u002f")
}

/// Build the event set that keeps the events holding every needle, bootstrap
/// events aside.
pub fn substring_filter(needles: &[String]) -> String {
    let lowercased: Vec<String> = needles.iter().map(|needle| needle.to_lowercase()).collect();
    let needles = js_literal(&lowercased);
    format!("filter(ev => {SKIP_BOOTSTRAP} && ({NEEDLE_FILTER})(ev, {needles}))")
}

/// Build the event set that keeps the events under `input_hash`. The hash
/// compares as a string: as a number it loses precision, and hashes that
/// differ in their last digits compare equal.
pub fn input_hash_filter(input_hash: &str) -> String {
    let input_hash = js_literal(input_hash);
    format!("filter(ev => ev.moment.input_hash == {input_hash})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    #[test]
    fn has_comment_finds_both_comment_openers() {
        assert!(has_comment("a // note"));
        assert!(has_comment("a /* note */"));
        assert!(has_comment("//"));
        assert!(!has_comment("a / b * c"));
        assert!(!has_comment(r"/^a\/b\/.*\/c/.test(s)"));
        assert!(!has_comment("/"));
        assert!(!has_comment(""));
    }

    #[hegel::test]
    fn has_comment_matches_a_substring_search(tc: hegel::TestCase) {
        let pieces = tc.draw(generators::vecs(generators::sampled_from(vec![
            "/", "*", "a", " ", "\\", "\n",
        ])));
        let text: String = pieces.concat();
        assert_eq!(
            has_comment(&text),
            text.contains("//") || text.contains("/*")
        );
    }

    #[hegel::test]
    fn built_queries_hold_no_comment(tc: hegel::TestCase) {
        let needles = tc.draw(generators::vecs(generators::text()));
        let needles: Vec<String> = needles
            .into_iter()
            .chain(["http://etcd0:2379".to_string(), "/*".to_string()])
            .collect();
        assert!(!has_comment(&substring_filter(&needles)));
        assert!(!has_comment(&input_hash_filter(
            &tc.draw(generators::text())
        )));
    }

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
        assert!(query.contains(NEEDLE_FILTER), "got: {query}");
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
