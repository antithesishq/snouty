//! Terminal-safe rendering helpers shared across human-facing output: aligned
//! key/value blocks and control-character sanitization.

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
pub(crate) const PROSE_WIDTH: usize = 100;

/// Word-wrap `text` to `width` visible columns, for prose the user reads.
///
/// Wrapping is a property of *printing*, not of the message: callers apply this
/// at the print site, and the stored/built message stays a single logical line.
/// Each input line wraps independently, continuation lines inherit the line's
/// leading indent, and ANSI escape sequences count as zero width so a colored
/// report wraps the same as a plain one. Words longer than the width (paths,
/// URLs) are left intact and overflow rather than being split.
pub(crate) fn wrap_text(text: &str, width: usize) -> String {
    text.lines()
        .map(|line| wrap_line(line, width))
        .collect::<Vec<_>>()
        .join("\n")
}

fn wrap_line(line: &str, width: usize) -> String {
    // A line that already fits is returned byte-identical, which keeps aligned
    // content (tables, caret markers, indented listings) exactly as built.
    // Re-flowing only ever applies to overlong prose.
    if visible_width(line) <= width {
        return line.to_string();
    }
    let indent: String = line.chars().take_while(|c| *c == ' ').collect();
    let mut out = String::new();
    let mut column = 0;
    for word in line.split_whitespace() {
        let word_width = visible_width(word);
        if column == 0 {
            out.push_str(&indent);
            column = indent.len();
        } else if column + 1 + word_width > width {
            out.push('\n');
            out.push_str(&indent);
            column = indent.len();
        } else {
            out.push(' ');
            column += 1;
        }
        out.push_str(word);
        column += word_width;
    }
    if out.is_empty() {
        line.to_string()
    } else {
        out
    }
}

/// The number of terminal columns `s` occupies, counting ANSI escape sequences
/// (`ESC [ … m` and similar) as zero.
fn visible_width(s: &str) -> usize {
    let mut width = 0;
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            // Skip to the final byte of the escape sequence (`@` through `~`).
            for esc in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&esc) && esc != '[' {
                    break;
                }
            }
        } else {
            width += 1;
        }
    }
    width
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
    fn wrap_text_reflows_only_overlong_lines() {
        let long = format!("Warning: {}", "word ".repeat(30));
        let wrapped = wrap_text(&long, 40);
        assert!(wrapped.lines().count() > 1);
        assert!(wrapped.lines().all(|l| l.len() <= 40), "got: {wrapped}");
        // A short line keeps its exact bytes, including internal alignment.
        assert_eq!(
            wrap_text("  profile     (none)", 100),
            "  profile     (none)"
        );
        assert_eq!(wrap_text("", 100), "");
    }

    #[test]
    fn wrap_text_keeps_the_indent_on_continuations() {
        let wrapped = wrap_text(&format!("   0: {}", "word ".repeat(30)), 40);
        for line in wrapped.lines().skip(1) {
            assert!(line.starts_with("   "), "got: {wrapped}");
        }
    }

    #[test]
    fn wrap_text_counts_ansi_escapes_as_zero_width() {
        // 10 visible chars dressed in color codes must not trigger a wrap.
        let colored = format!("\u{1b}[31m{}\u{1b}[0m", "x".repeat(10));
        assert_eq!(wrap_text(&colored, 20), colored);
    }

    #[test]
    fn wrap_text_leaves_unsplittable_words_intact() {
        let path = format!("/very/long/{}", "seg/".repeat(30));
        let wrapped = wrap_text(&format!("backed up to {path}"), 40);
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
