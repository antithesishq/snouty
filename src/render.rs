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
pub(crate) fn render_kv(rows: &[(&str, String)], min_label_width: usize) -> String {
    let label_width = rows
        .iter()
        .map(|(label, _)| label.len())
        .chain(std::iter::once(min_label_width))
        .max()
        .unwrap_or(0);
    let mut out = String::new();
    for (label, value) in rows {
        out.push_str(&format!("{label:label_width$}  {}\n", sanitize(value)));
    }
    out
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

/// The measure user-facing prose wraps to. Narrower than most terminals, so a
/// wrapped message reads as a paragraph instead of hitting the terminal's own
/// mid-word wrap; wide enough that short messages stay on one line.
const PROSE_WIDTH: usize = 100;

/// Wrap prose for stderr when a person is reading it.
///
/// Wrapping is a property of printing, not of the message, and it applies only
/// on a terminal: piped and captured output keeps whole lines, because a wrap
/// point that moves with an embedded path length breaks any multi-word match
/// that straddles it.
pub fn wrap_if_tty(text: &str) -> String {
    use std::io::IsTerminal;
    if std::io::stderr().is_terminal() {
        wrap(text)
    } else {
        text.to_string()
    }
}

/// Word-wrap each overlong line of `text` to [`PROSE_WIDTH`] visible columns.
///
/// A line that already fits is returned byte-identical, which keeps aligned
/// content (tables, caret markers, indented listings) exactly as built.
/// Continuation lines inherit the line's indent, ANSI escape sequences count
/// as zero width, and words are never split — a path overflows rather than
/// breaking.
fn wrap(text: &str) -> String {
    text.lines().map(wrap_line).collect::<Vec<_>>().join("\n")
}

fn wrap_line(line: &str) -> String {
    if textwrap::core::display_width(line) <= PROSE_WIDTH {
        return line.to_string();
    }
    let indent: String = line.chars().take_while(|c| *c == ' ').collect();
    let options = textwrap::Options::new(PROSE_WIDTH)
        .initial_indent(&indent)
        .subsequent_indent(&indent)
        .break_words(false)
        .word_splitter(textwrap::WordSplitter::NoHyphenation);
    textwrap::fill(line.trim_start(), options)
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
