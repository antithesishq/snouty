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
/// No query snouty sends may hold a comment: the comments are snouty's, and a
/// tenant can rewrite a query before it runs (on a customer tenant, release
/// 62, a 400 from `POST /runs/{run_id}/events/search` showed a `runs events`
/// query inside a server-built `filter(e => …)`).
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

/// `src` on one line: each `\n` or `\r` becomes a space. `N` must be
/// `src.len()`.
///
/// A query goes out on one line, so a server that rewrites it cannot be
/// tripped by a line break.
const fn one_line<const N: usize>(src: &str) -> [u8; N] {
    let src = src.as_bytes();
    let mut out = [0u8; N];
    let mut i = 0;
    while i < N {
        out[i] = match src[i] {
            b'\n' | b'\r' => b' ',
            byte => byte,
        };
        i += 1;
    }
    out
}

/// Declare a query fragment: fail the build if it holds a comment, and put it
/// on one line. A fragment on one line gets no automatic semicolons, so it
/// ends its statements with `;`.
macro_rules! query_fragment {
    ($text:expr) => {{
        const SRC: &str = $text.trim_ascii();
        const _: () = assert!(
            !has_comment(SRC),
            "a query fragment holds a comment; move it into the Rust source"
        );
        const LINE: [u8; SRC.len()] = one_line(SRC);
        const TEXT: &str = match core::str::from_utf8(&LINE) {
            Ok(text) => text,
            Err(_) => panic!("a space replaced part of a UTF-8 character"),
        };
        TEXT
    }};
}

/// The event-set expression `(ev, needles)` that decides whether one event
/// holds every needle. The needles must be lowercase: `m` folds only the
/// haystack. An assertion catalog entry (`antithesis_assert.hit === false`)
/// never matches on `output_text`, because it is no evidence that anything ran.
pub(crate) const NEEDLE_FILTER: &str =
    query_fragment!(include_str!("event_set_dsl/needle_filter.pangolin.js"));

/// The condition that drops bootstrap events. They come before a run's log
/// timeline starts, so their moments have no log stream: a hash taken from
/// one leads `runs logs` nowhere.
pub(crate) const SKIP_BOOTSTRAP: &str = query_fragment!(r#"ev.source?.name !== "bootstrap""#);

/// Build the event set that keeps the events holding every needle, bootstrap
/// events aside.
pub fn substring_filter(needles: &[String]) -> String {
    let lowercased: Vec<String> = needles.iter().map(|needle| needle.to_lowercase()).collect();
    let needles = serde_json::to_string(&lowercased).expect("strings serialize to JSON");
    format!("filter(ev => {SKIP_BOOTSTRAP} && ({NEEDLE_FILTER})(ev, {needles}))")
}

/// Build the event set that keeps the events under `input_hash`. The hash
/// compares as a string: as a number it loses precision, and hashes that
/// differ in their last digits compare equal.
pub fn input_hash_filter(input_hash: &str) -> String {
    let input_hash = serde_json::to_string(input_hash).expect("strings serialize to JSON");
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

    // The templates the builders wrap around a needle or a hash hold no
    // comment either.
    #[test]
    fn built_queries_hold_no_comment() {
        assert!(!has_comment(&substring_filter(&["raft".to_string()])));
        assert!(!has_comment(&input_hash_filter("123")));
    }

    #[test]
    fn one_line_turns_line_breaks_into_spaces() {
        assert_eq!(&one_line::<11>("a {\r\n  b;\n}"), b"a {    b; }");
        assert_eq!(&one_line::<0>(""), b"");
    }

    // A query goes out on one line, whatever the needles hold.
    #[test]
    fn built_queries_are_one_line() {
        let query = substring_filter(&["two\nlines".to_string()]);
        assert!(!query.contains('\n'), "got: {query}");
        assert!(query.contains("const m = "), "got: {query}");
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
