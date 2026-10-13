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

/// Strip the JavaScript comments out of `src`, along with the trailing
/// whitespace and blank lines they leave. Returns the stripped text in the
/// first `.1` bytes of the buffer; `N` must be at least `src.len()`.
///
/// Queries never carry comments: a tenant can rewrite a query before it runs
/// (on a customer tenant, release 62, a 400 from
/// `POST /runs/{run_id}/events/search` showed a `runs events` query inside a
/// server-built `filter(e => …)`), and the comments are snouty's, not the
/// server's.
///
/// `'`, `"` and `` ` `` strings are kept whole. Regex literals are not
/// recognized, so one that holds a quote, `//` or `/*` is misread.
pub(crate) const fn strip_comments<const N: usize>(src: &str) -> ([u8; N], usize) {
    const fn is_blank(byte: u8) -> bool {
        matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
    }

    let src = src.as_bytes();
    let mut out = [0u8; N];
    let mut n = 0;
    // Where the current line starts in `out`. A newline inside a string is
    // the string's own, and starts no line.
    let mut line_start = 0;
    // The quote of the string `i` is inside, if any.
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < src.len() {
        let byte = src[i];
        let next = if i + 1 < src.len() { src[i + 1] } else { 0 };
        if let Some(q) = quote {
            out[n] = byte;
            n += 1;
            if byte == b'\\' && i + 1 < src.len() {
                out[n] = next;
                n += 1;
                i += 1;
            } else if byte == q {
                quote = None;
            }
            i += 1;
        } else if byte == b'/' && next == b'/' {
            while i < src.len() && src[i] != b'\n' {
                i += 1;
            }
        } else if byte == b'/' && next == b'*' {
            i += 2;
            while i < src.len() && !(src[i] == b'*' && i + 1 < src.len() && src[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
        } else if byte == b'\n' {
            while n > line_start && is_blank(out[n - 1]) {
                n -= 1;
            }
            // A line left blank goes, newline and all.
            if n > line_start {
                out[n] = b'\n';
                n += 1;
                line_start = n;
            }
            i += 1;
        } else {
            if matches!(byte, b'"' | b'\'' | b'`') {
                quote = Some(byte);
            }
            out[n] = byte;
            n += 1;
            i += 1;
        }
    }
    while n > 0 && is_blank(out[n - 1]) {
        n -= 1;
    }
    (out, n)
}

/// A query fragment from JavaScript source: `$src` with its comments
/// stripped at compile time.
macro_rules! query_fragment {
    ($src:expr) => {{
        const SRC: &str = $src;
        const STRIPPED: ([u8; SRC.len()], usize) = strip_comments(SRC);
        const TEXT: &str = match core::str::from_utf8(STRIPPED.0.split_at(STRIPPED.1).0) {
            Ok(text) => text,
            Err(_) => panic!("stripping comments split a UTF-8 character"),
        };
        TEXT
    }};
}

/// The event-set expression `(ev, needles)` that decides whether one event
/// holds every needle.
pub(crate) const NEEDLE_FILTER: &str =
    query_fragment!(include_str!("event_set_dsl/needle_filter.pangolin.js"));

/// The condition that drops bootstrap events. They come before a run's log
/// timeline starts, so their moments have no log stream: a hash taken from
/// one leads `runs logs` nowhere.
pub(crate) const SKIP_BOOTSTRAP: &str = r#"ev.source?.name !== "bootstrap""#;

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

    fn strip(src: &str) -> String {
        let (out, n) = strip_comments::<256>(src);
        String::from_utf8(out[..n].to_vec()).unwrap()
    }

    #[test]
    fn strip_comments_drops_both_comment_kinds_and_the_lines_they_leave() {
        assert_eq!(
            strip("// head\n//\na; // tail\n  /* x */ b\n\nc"),
            "a;\n   b\nc"
        );
        assert_eq!(strip("a /* one\ntwo */ b"), "a  b");
        assert_eq!(strip("a\n// end\n"), "a");
    }

    #[test]
    fn strip_comments_keeps_strings_and_division_whole() {
        let code = r#"f("http://x", 'a /* b */', `c
// d`, "say \"//\"") / 2 * 3"#;
        assert_eq!(strip(code), code);
        assert_eq!(strip(r"/^a\/b\/.*\/c/.test(s)"), r"/^a\/b\/.*\/c/.test(s)");
    }

    // A template string's line breaks, and the spaces before them, are its
    // own.
    #[test]
    fn strip_comments_trims_up_to_a_string_only() {
        assert_eq!(strip("`a  \n` // x\nb"), "`a  \n`\nb");
    }

    // Text without a slash or a quote loses only its trailing whitespace and
    // blank lines, and a second strip changes nothing.
    #[hegel::test]
    fn strip_comments_leaves_slashless_text_alone(tc: hegel::TestCase) {
        let pieces = tc.draw(generators::vecs(generators::sampled_from(vec![
            "a", "*", " ", "\n", "\\",
        ])));
        let text: String = pieces.concat();
        let expected = text
            .lines()
            .map(str::trim_end)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        let stripped = strip(&text);
        assert_eq!(stripped, expected);
        assert_eq!(strip(&stripped), stripped);
    }

    // The filter goes out without the comments its source file carries.
    #[test]
    fn needle_filter_is_sent_without_its_comments() {
        assert!(
            NEEDLE_FILTER.starts_with("(ev, needles) => {"),
            "got: {NEEDLE_FILTER}"
        );
        assert!(
            NEEDLE_FILTER.ends_with("return needles.every(has);\n}"),
            "got: {NEEDLE_FILTER}"
        );
        assert!(!NEEDLE_FILTER.contains("//"), "got: {NEEDLE_FILTER}");
        assert!(!substring_filter(&["raft".to_string()]).contains("//"));
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
