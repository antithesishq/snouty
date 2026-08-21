//! Terminal-safe rendering helpers shared across human-facing output: aligned
//! key/value blocks and control-character sanitization.

/// The global output flags (`--json`, `--verbose`), resolved once at the
/// dispatch boundary in main.rs and passed by value through the command
/// layer. Carrying them as one named-field struct keeps the two bools from
/// threading positionally, where a swapped pair compiles silently.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutputOptions {
    /// Emit machine-readable JSON instead of human-facing text.
    pub json: bool,
    /// Log HTTP request/response detail to stderr.
    pub verbose: bool,
}

/// Render aligned `Label  value` lines, sqlite `.mode line`–style. Each line is
/// terminated with a newline; values are sanitized. Labels are padded to the
/// widest label, but never narrower than `min_label_width` so a caller that also
/// renders a wider prose label below the block can keep every row aligned.
pub(crate) fn render_kv<S: AsRef<str>>(rows: &[(S, String)], min_label_width: usize) -> String {
    let label_width = rows
        .iter()
        .map(|(label, _)| label.as_ref().len())
        .chain(std::iter::once(min_label_width))
        .max()
        .unwrap_or(0);
    let mut out = String::new();
    for (label, value) in rows {
        out.push_str(&format!(
            "{:label_width$}  {}\n",
            label.as_ref(),
            sanitize(value)
        ));
    }
    out
}

/// Prefix every line of `text` with `prefix`, so a multi-line block sits under
/// the line that introduces it. Shared by the run/property renderers and by the
/// API error formatter.
pub(crate) fn indent_lines(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| {
            // Don't indent blank lines — that would leave trailing whitespace.
            if line.is_empty() {
                String::new()
            } else {
                format!("{prefix}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Escape one character into `out`, sharing the control-char policy between
/// [`sanitize`] and [`sanitize_multiline`]. `newline` decides how `\n`/`\r` are
/// handled: single-line callers escape them to visible `\n`/`\r`, multi-line
/// callers keep `\n` as a real break and drop `\r`. Everything else — tab passes
/// through, other C0/DEL controls become `\xNN`, printable chars pass through —
/// is identical for both.
fn sanitize_char(out: &mut String, ch: char, newline: NewlinePolicy) {
    match ch {
        '\n' | '\r' => match newline {
            NewlinePolicy::Escape => {
                out.push_str(if ch == '\n' { "\\n" } else { "\\r" });
            }
            // Multi-line prose keeps real newlines and drops lone carriage
            // returns (so `\r\n` collapses to `\n`).
            NewlinePolicy::KeepNewlineDropReturn => {
                if ch == '\n' {
                    out.push('\n');
                }
            }
        },
        '\t' => out.push('\t'),
        '\0'..='\u{08}' | '\u{0B}'..='\u{1F}' | '\u{7F}' => {
            out.push_str(&format!(r"\x{:02X}", ch as u32));
        }
        _ => out.push(ch),
    }
}

#[derive(Clone, Copy)]
enum NewlinePolicy {
    /// Escape `\n`/`\r` to literal `\n`/`\r` (single-line table cells).
    Escape,
    /// Keep `\n` as a real break, drop `\r` (multi-line prose).
    KeepNewlineDropReturn,
}

/// The widest measure user-facing prose wraps to. Even on a wider terminal a
/// wrapped message reads better as a paragraph than as full-width lines; on a
/// narrower terminal [`wrap_if_tty`] wraps to the terminal's own width so the
/// terminal never re-wraps mid-word.
const PROSE_WIDTH: usize = 100;

/// Wrap prose for stderr when a person is reading it.
///
/// Wrapping is a property of printing, not of the message, and it applies only
/// on a terminal: piped and captured output keeps whole lines, because a wrap
/// point that moves with an embedded path length breaks any multi-word match
/// that straddles it. The measure is the terminal's width, capped at
/// [`PROSE_WIDTH`].
pub fn wrap_if_tty(text: &str) -> String {
    let term = console::Term::stderr();
    if !term.is_term() {
        return text.to_string();
    }
    let width = PROSE_WIDTH.min(term.size().1 as usize);
    wrap_text(text, width).join("\n")
}

/// The one wrapping engine every snouty renderer shares. Greedy word-wrap of
/// `text` to `width` display columns, one output line per element.
///
/// Each `\n` starts a new paragraph and blank lines are kept. A paragraph that
/// already fits passes through byte-identical, which keeps aligned content
/// (tables, caret markers, indented listings) exactly as built. An overlong
/// paragraph keeps its leading-space indent on every wrapped line, has tabs
/// normalized to spaces (textwrap's separator only breaks on spaces), and
/// never splits a word — an overlong token overflows instead. Width is
/// measured with `textwrap`'s `display_width`: ANSI escape sequences count as
/// zero columns and wide glyphs count as two.
pub(crate) fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for paragraph in text.split('\n') {
        if textwrap::core::display_width(paragraph) <= width {
            lines.push(paragraph.to_string());
            continue;
        }
        // Overlong yet nothing to wrap: whitespace-only collapses to a blank
        // line rather than emitting an invisible overlong run.
        if paragraph.trim().is_empty() {
            lines.push(String::new());
            continue;
        }
        let indent: String = paragraph.chars().take_while(|c| *c == ' ').collect();
        // Words are never split mid-token (no hard breaks, no hyphenation) —
        // an overlong token overflows instead.
        let options = textwrap::Options::new(width.max(1))
            .break_words(false)
            .word_splitter(textwrap::WordSplitter::NoHyphenation)
            .initial_indent(&indent)
            .subsequent_indent(&indent);
        for line in textwrap::wrap(paragraph.replace('\t', " ").trim_start(), options) {
            lines.push(line.into_owned());
        }
    }
    lines
}

pub(crate) fn sanitize(s: &str) -> String {
    let mut escaped = String::new();
    for ch in s.chars() {
        sanitize_char(&mut escaped, ch, NewlinePolicy::Escape);
    }
    escaped
}

/// Like [`sanitize`] but preserves real newlines instead of escaping them to
/// literal `\n`. For multi-line free text (e.g. property descriptions) that is
/// meant to be read as prose, not as a single table cell.
pub(crate) fn sanitize_multiline(s: &str) -> String {
    let mut out = String::new();
    for ch in s.chars() {
        sanitize_char(&mut out, ch, NewlinePolicy::KeepNewlineDropReturn);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    /// `wrap_text` preserves the exact sequence of words — wrapping only inserts
    /// line breaks, it never drops, splits, reorders, or invents a word.
    #[hegel::test]
    fn wrap_text_preserves_word_sequence(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let width = tc.draw(generators::integers::<usize>().min_value(1).max_value(40));
        let lines = wrap_text(&text, width);
        let words_in: Vec<&str> = text.split_whitespace().collect();
        let words_out: Vec<&str> = lines.iter().flat_map(|l| l.split_whitespace()).collect();
        assert_eq!(words_in, words_out);
    }

    /// Every wrapped line fits within `width` display columns (ANSI escapes
    /// and control characters count as zero width, wide glyphs as two), with
    /// the one documented exception: a single word longer than the remaining
    /// width is kept intact rather than split mid-token (after the preserved
    /// leading-space indent, such a line has no internal space).
    #[hegel::test]
    fn wrap_text_respects_width(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        // Include 0 to exercise the `width.max(1)` clamp.
        let width = tc.draw(generators::integers::<usize>().max_value(40));
        let effective = width.max(1);
        for line in wrap_text(&text, width) {
            assert!(
                textwrap::core::display_width(&line) <= effective
                    || !line.trim_start().contains(' '),
                "line {line:?} exceeds width {effective} but contains a space",
            );
        }
    }

    #[test]
    fn wrap_text_wraps_words_and_preserves_blank_lines() {
        let wrapped = wrap_text("the quick brown fox\n\njumps", 9);
        assert_eq!(wrapped, vec!["the quick", "brown fox", "", "jumps"]);
        // A word longer than the width is kept intact rather than split.
        assert_eq!(
            wrap_text("supercalifragilistic", 5),
            vec!["supercalifragilistic"]
        );
    }

    #[test]
    fn wrap_text_keeps_fitting_paragraphs_byte_identical() {
        // A paragraph that fits passes through untouched: internal alignment,
        // leading spaces, and tabs all survive.
        assert_eq!(wrap_text("  a\tb   c", 20), vec!["  a\tb   c"]);
        // A tab in an overlong paragraph becomes a break opportunity.
        assert_eq!(wrap_text("aaaa\tbbbb", 5), vec!["aaaa", "bbbb"]);
    }

    #[test]
    fn render_kv_aligns_to_widest_label_and_min_width() {
        let rows = vec![("a", "1".to_string()), ("longer", "2".to_string())];
        // min_label_width below the widest label has no effect; labels pad to 6.
        assert_eq!(render_kv(&rows, 0), "a       1\nlonger  2\n");
        // a larger min_label_width widens every row.
        assert_eq!(render_kv(&[("a", "1".to_string())], 4), "a     1\n");
    }

    #[test]
    fn render_kv_sanitizes_values() {
        let rows = vec![("k", "a\nb".to_string())];
        assert_eq!(render_kv(&rows, 0), "k  a\\nb\n");
    }

    /// The prose shape [`wrap_if_tty`] produces on a wide terminal, minus the
    /// tty detection, so the tests run identically under a captured stdout.
    fn wrap(text: &str) -> String {
        wrap_text(text, PROSE_WIDTH).join("\n")
    }

    #[test]
    fn wrap_reflows_only_overlong_lines() {
        let long = format!("Warning: {}", "word ".repeat(30));
        let wrapped = wrap(&long);
        assert!(wrapped.lines().count() > 1);
        assert!(wrapped.lines().all(|l| l.len() <= 100), "got: {wrapped}");
        // A short line keeps its exact bytes, including internal alignment.
        assert_eq!(wrap("  profile     (none)"), "  profile     (none)");
        assert_eq!(wrap(""), "");
    }

    #[test]
    fn wrap_keeps_the_indent_and_never_splits_words() {
        let path = format!("/very/long/{}", "seg-x/".repeat(30));
        let wrapped = wrap(&format!(
            "   note: backed up to {path} {}",
            "word ".repeat(20)
        ));
        for line in wrapped.lines().skip(1) {
            assert!(line.starts_with("   "), "got: {wrapped}");
        }
        assert!(wrapped.contains(&path), "paths must never be split");
    }

    #[test]
    fn sanitize_preserves_printable_unicode_and_punctuation() {
        assert_eq!(
            sanitize("Grüße λ 😸 \"quoted\" C:\\temp\tok"),
            "Grüße λ 😸 \"quoted\" C:\\temp\tok"
        );
    }

    #[test]
    fn sanitize_escapes_newline_and_carriage_return() {
        assert_eq!(sanitize("one\ntwo\rthree"), "one\\ntwo\\rthree");
    }

    #[test]
    fn sanitize_escapes_non_printable_ascii_except_tab() {
        assert_eq!(
            sanitize("a\u{0001}b\u{000B}c\u{007F}d\te"),
            r"a\x01b\x0Bc\x7Fd	e"
        );
    }

    #[test]
    fn sanitize_multiline_keeps_newlines_but_escapes_other_controls() {
        // Real newlines survive (so Details renders as prose), \r is dropped,
        // and other control chars are still escaped.
        assert_eq!(
            sanitize_multiline("one\ntwo\r\nthree\u{0001}"),
            "one\ntwo\nthree\\x01"
        );
    }
}
