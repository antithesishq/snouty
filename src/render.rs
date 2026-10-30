//! Terminal-safe rendering helpers shared across text output: aligned
//! key/value blocks and control-character sanitization.
use std::fmt::Write as _;

/// The global output flags (`--json`, `--verbose`), resolved once at the
/// dispatch boundary in main.rs and passed by value through the command
/// layer. Carrying them as one named-field struct keeps the two bools from
/// threading positionally, where a swapped pair compiles silently.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutputOptions {
    /// Emit machine-readable JSON instead of text output.
    pub json: bool,
    /// Log HTTP request/response detail to stderr.
    pub verbose: bool,
}

/// Render aligned `Label  value` lines, sqlite `.mode line`–style. Each line is
/// terminated with a newline; labels and values are sanitized. Labels are padded to the
/// widest label, but never narrower than `min_label_width` so a caller that also
/// renders a wider prose label below the block can keep every row aligned.
pub(crate) fn render_kv<L: AsRef<str>, V: AsRef<str>>(
    rows: &[(L, V)],
    min_label_width: usize,
) -> String {
    let rows: Vec<(String, String)> = rows
        .iter()
        .map(|(label, value)| (sanitize(label.as_ref()), sanitize(value.as_ref())))
        .collect();
    let label_width = rows
        .iter()
        .map(|(label, _)| label.len())
        .chain(std::iter::once(min_label_width))
        .max()
        .unwrap_or(0);
    let mut out = String::new();
    for (label, value) in rows {
        let _ = writeln!(out, "{label:label_width$}  {value}");
    }
    out
}

/// Prefix every line of `text` with `prefix`. A blank line stays blank, so no
/// line ends in trailing whitespace.
pub(crate) fn indent_lines(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| {
            if line.is_empty() {
                String::new()
            } else {
                format!("{prefix}{line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format a command for the user to copy. With two or more flags, each flag
/// goes on its own line after a shell line continuation, so a long command
/// fits the terminal and still pastes as one command. Each value is quoted
/// for the shell.
pub(crate) fn shell_command(program: &str, flags: &[(&str, &str)]) -> String {
    let separator = if flags.len() > 1 { " \\\n  " } else { " " };
    let mut command = program.to_string();
    for (flag, value) in flags {
        let value = shlex::try_quote(value).unwrap_or((*value).into());
        let _ = write!(command, "{separator}{flag} {value}");
    }
    command
}

/// Escape one character into `out` under the control-character policy that
/// [`sanitize`] and [`sanitize_multiline`] share. Tab passes through, and every
/// other Unicode control (Cc) except `\n` and `\r` becomes `\xNN`.
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
        _ if ch.is_control() => {
            let _ = write!(out, r"\x{:02X}", ch as u32);
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
    wrap_for(&console::Term::stderr(), text)
}

/// As [`wrap_if_tty`], for prose printed to stdout.
pub(crate) fn wrap_stdout_if_tty(text: &str) -> String {
    wrap_for(&console::Term::stdout(), text)
}

fn wrap_for(term: &console::Term, text: &str) -> String {
    match prose_width_of(term) {
        Some(width) => wrap_text(text, width, CodeSpans::Keep).join("\n"),
        None => text.to_string(),
    }
}

/// The measure [`wrap_if_tty`] wraps stderr prose to: the terminal's width,
/// capped at [`PROSE_WIDTH`]. `None` when stderr is not a terminal, where
/// output keeps whole lines.
pub(crate) fn prose_width() -> Option<usize> {
    prose_width_of(&console::Term::stderr())
}

/// As [`prose_width`], for stdout.
pub(crate) fn stdout_prose_width() -> Option<usize> {
    prose_width_of(&console::Term::stdout())
}

fn prose_width_of(term: &console::Term) -> Option<usize> {
    term.is_term()
        .then(|| PROSE_WIDTH.min(term.size().1 as usize))
}

/// Whether [`wrap_text`] keeps a backtick-delimited span on one line.
#[derive(Clone, Copy)]
pub(crate) enum CodeSpans {
    /// Keep each span whole. For prose that snouty writes, where every
    /// backtick has a pair.
    Keep,
    /// Treat a backtick as a plain character. For text that snouty does not
    /// write, where two stray backticks would join every word between them.
    Ignore,
}

/// The one wrapping engine every snouty renderer shares. Greedy word-wrap of
/// `text` to `width` display columns, one output line per element.
///
/// Each `\n` starts a new paragraph and blank lines are kept. A paragraph that
/// already fits passes through byte-identical, which keeps aligned content
/// (tables, caret markers, indented listings) exactly as built. So does a
/// paragraph directly above a caret line (one that trims to `^`), whatever its
/// width: the caret points at one of its columns. So does a line of a command
/// continued over several lines (one that ends in `\`, or follows one that
/// does), because a break inside it would split the pasted command. An overlong
/// paragraph keeps its leading-space indent on every wrapped line, has tabs
/// normalized to spaces (textwrap's separator only breaks on spaces), and
/// never splits a word — an overlong token overflows instead. With
/// [`CodeSpans::Keep`], a backtick-delimited span, such as a command to copy,
/// counts as one word, so it moves whole to the next line or overflows on its
/// own line. Width is
/// measured with `textwrap`'s `display_width`: ANSI escape sequences count as
/// zero columns and wide glyphs count as two.
pub(crate) fn wrap_text(text: &str, width: usize, spans: CodeSpans) -> Vec<String> {
    let mut lines = Vec::new();
    let paragraphs: Vec<&str> = text.split('\n').collect();
    for (i, paragraph) in paragraphs.iter().copied().enumerate() {
        // A caret marker points at a column of the line above it, so that line
        // keeps its layout even when it overflows.
        let marked = paragraphs.get(i + 1).is_some_and(|next| next.trim() == "^");
        if marked
            || continues_a_command(&paragraphs, i)
            || textwrap::core::display_width(paragraph) <= width
        {
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
            .word_separator(match spans {
                CodeSpans::Keep => textwrap::WordSeparator::Custom(find_words_keeping_code_spans),
                CodeSpans::Ignore => textwrap::WordSeparator::AsciiSpace,
            })
            .initial_indent(&indent)
            .subsequent_indent(&indent);
        for line in textwrap::wrap(paragraph.replace('\t', " ").trim_start(), options) {
            lines.push(line.into_owned());
        }
    }
    lines
}

/// Whether line `i` of `lines` is part of a shell command continued over
/// several lines: it ends in a backslash, or the line before it does.
fn continues_a_command<S: AsRef<str>>(lines: &[S], i: usize) -> bool {
    let continued = |line: &S| line.as_ref().ends_with('\\');
    continued(&lines[i]) || i.checked_sub(1).is_some_and(|prev| continued(&lines[prev]))
}

/// Split `line` at spaces like `WordSeparator::AsciiSpace`, but join the words
/// of a backtick-delimited span into one word. From an unmatched backtick to
/// the end of the line, words split at every space.
fn find_words_keeping_code_spans(
    line: &str,
) -> Box<dyn Iterator<Item = textwrap::core::Word<'_>> + '_> {
    let words: Vec<_> = textwrap::WordSeparator::AsciiSpace
        .find_words(line)
        .collect();
    let mut merged = Vec::with_capacity(words.len());
    // Byte offset in `line` of `words[i]`: the words tile `line` exactly.
    let mut start = 0;
    let mut i = 0;
    while i < words.len() {
        let mut end = start;
        let mut ticks = 0;
        let mut j = i;
        while j < words.len() {
            end += words[j].len() + words[j].whitespace.len();
            ticks += words[j].matches('`').count();
            j += 1;
            if ticks.is_multiple_of(2) {
                break;
            }
        }
        if !ticks.is_multiple_of(2) {
            merged.extend_from_slice(&words[i..]);
            break;
        }
        merged.push(textwrap::core::Word::from(&line[start..end]));
        start = end;
        i = j;
    }
    Box::new(merged.into_iter())
}

pub(crate) fn sanitize(s: &str) -> String {
    let mut escaped = String::new();
    for ch in s.chars() {
        sanitize_char(&mut escaped, ch, NewlinePolicy::Escape);
    }
    escaped
}

/// Like [`sanitize`] but preserves real newlines instead of escaping them to
/// literal `\n`. For multi-line free text (e.g. run descriptions) that is
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
    use hegel::generators::{self, Generator};

    /// `wrap_text` preserves the exact sequence of words — wrapping only inserts
    /// line breaks, it never drops, splits, reorders, or invents a word.
    #[hegel::test]
    fn wrap_text_preserves_word_sequence(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        let width = tc.draw(generators::integers::<usize>().min_value(1).max_value(40));
        let lines = wrap_text(&text, width, CodeSpans::Keep);
        let words_in: Vec<&str> = text.split_whitespace().collect();
        let words_out: Vec<&str> = lines.iter().flat_map(|l| l.split_whitespace()).collect();
        assert_eq!(words_in, words_out);
    }

    /// Every wrapped line fits within `width` display columns (ANSI escapes
    /// and control characters count as zero width, wide glyphs as two), with
    /// two documented exceptions: a single word longer than the remaining
    /// width is kept intact rather than split mid-token, and a line directly
    /// above a caret line is kept whole. After the preserved
    /// leading-space indent, such a line is one word or one code span.
    #[hegel::test]
    fn wrap_text_respects_width(tc: hegel::TestCase) {
        let text = tc.draw(generators::text());
        // Include 0 to exercise the `width.max(1)` clamp.
        let width = tc.draw(generators::integers::<usize>().max_value(40));
        let effective = width.max(1);
        let lines = wrap_text(&text, width, CodeSpans::Keep);
        for (i, line) in lines.iter().enumerate() {
            // A caret line below keeps the line whole, at any width.
            let marked = lines.get(i + 1).is_some_and(|next| next.trim() == "^");
            assert!(
                marked
                    || continues_a_command(&lines, i)
                    || textwrap::core::display_width(line) <= effective
                    || find_words_keeping_code_spans(line.trim_start()).count() == 1,
                "line {line:?} exceeds width {effective} but holds more than one word",
            );
        }
    }

    #[test]
    fn wrap_text_wraps_words_and_preserves_blank_lines() {
        let wrapped = wrap_text("the quick brown fox\n\njumps", 9, CodeSpans::Keep);
        assert_eq!(wrapped, vec!["the quick", "brown fox", "", "jumps"]);
        // A word longer than the width is kept intact rather than split.
        assert_eq!(
            wrap_text("supercalifragilistic", 5, CodeSpans::Keep),
            vec!["supercalifragilistic"]
        );
    }

    #[test]
    fn wrap_text_keeps_fitting_paragraphs_byte_identical() {
        // A paragraph that fits passes through untouched: internal alignment,
        // leading spaces, and tabs all survive.
        assert_eq!(
            wrap_text("  a\tb   c", 20, CodeSpans::Keep),
            vec!["  a\tb   c"]
        );
        // A tab in an overlong paragraph becomes a break opportunity.
        assert_eq!(
            wrap_text("aaaa\tbbbb", 5, CodeSpans::Keep),
            vec!["aaaa", "bbbb"]
        );
    }

    #[test]
    fn wrap_text_keeps_the_line_a_caret_points_into() {
        let query = format!("  {}", "word ".repeat(30).trim_end());
        let caret = format!("{}^", " ".repeat(100));
        let text = format!("{query}\n{caret}\ninvalid word");
        assert_eq!(
            wrap_text(&text, 80, CodeSpans::Keep),
            [query.as_str(), caret.as_str(), "invalid word"]
        );
    }

    #[test]
    fn wrap_text_keeps_the_lines_of_a_continued_command() {
        let command = "  snouty debug \\\n    --session-id a-long-session-id \\\n    --vtime 2.5";
        let text = format!("start a session with:\n{command}");
        let mut want = vec!["start a", "session", "with:"];
        want.extend(command.lines());
        assert_eq!(wrap_text(&text, 10, CodeSpans::Keep), want);
    }

    #[test]
    fn wrap_text_keeps_a_code_span_on_one_line() {
        // A span that does not fit moves whole to the next line, with the
        // indent.
        assert_eq!(
            wrap_text("  then `unset A B` now", 12, CodeSpans::Keep),
            vec!["  then", "  `unset A B`", "  now"]
        );
        // A span wider than the whole width overflows on its own line.
        assert_eq!(
            wrap_text("run `unset LONG_A LONG_B` now", 8, CodeSpans::Keep),
            vec!["run", "`unset LONG_A LONG_B`", "now"]
        );
        // A span may start or end inside a word.
        assert_eq!(
            wrap_text("set (`a b`) ok", 6, CodeSpans::Keep),
            vec!["set", "(`a b`)", "ok"]
        );
    }

    /// Text without a backtick splits into the same words as before.
    #[hegel::test]
    fn text_without_backticks_splits_at_every_space(tc: hegel::TestCase) {
        let line = tc.draw(generators::text().filter(|s: &String| !s.contains('`')));
        let words: Vec<_> = find_words_keeping_code_spans(&line).collect();
        let before: Vec<_> = textwrap::WordSeparator::AsciiSpace
            .find_words(&line)
            .collect();
        assert_eq!(words, before);
    }

    #[test]
    fn wrap_text_ignoring_code_spans_splits_at_every_space() {
        // Two stray backticks in text that snouty does not write must not
        // join the words between them.
        assert_eq!(
            wrap_text("don`t stop, it won`t fit", 10, CodeSpans::Ignore),
            vec!["don`t", "stop, it", "won`t fit"]
        );
    }

    #[test]
    fn wrap_text_splits_after_an_unmatched_backtick() {
        // The matched span stays whole; from the stray backtick on, words
        // split at every space as before.
        assert_eq!(
            wrap_text("`a b` c `d e f", 5, CodeSpans::Keep),
            vec!["`a b`", "c `d", "e f"]
        );
    }

    /// A backtick-delimited span is one word: when every backtick in the
    /// text is matched, no wrapped line holds an odd number of backticks.
    /// Wrapping still only inserts line breaks.
    #[hegel::test]
    fn wrap_text_never_splits_a_code_span(tc: hegel::TestCase) {
        let pieces = tc.draw(generators::vecs(generators::sampled_from(vec![
            "a", "bb", " ", " ", "`",
        ])));
        let text: String = pieces.concat();
        let width = tc.draw(generators::integers::<usize>().min_value(1).max_value(12));
        let lines = wrap_text(&text, width, CodeSpans::Keep);
        if text.matches('`').count().is_multiple_of(2) {
            for line in &lines {
                assert_eq!(line.matches('`').count() % 2, 0, "split span in {lines:?}");
            }
        }
        let words_in: Vec<&str> = text.split_whitespace().collect();
        let words_out: Vec<&str> = lines.iter().flat_map(|l| l.split_whitespace()).collect();
        assert_eq!(words_in, words_out);
    }

    #[test]
    fn shell_command_puts_each_of_several_flags_on_its_own_line() {
        assert_eq!(
            shell_command("snouty debug", &[("--run-id", "r-1"), ("--vtime", "2.5")]),
            "snouty debug \\\n  --run-id r-1 \\\n  --vtime 2.5"
        );
        assert_eq!(
            shell_command("snouty runs show", &[("--run-id", "r-1")]),
            "snouty runs show --run-id r-1"
        );
        assert_eq!(shell_command("snouty doctor", &[]), "snouty doctor");
        assert_eq!(
            shell_command("snouty runs show", &[("--run-id", "a b;c")]),
            "snouty runs show --run-id 'a b;c'"
        );
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
    fn render_kv_sanitizes_labels_and_values() {
        let rows = vec![("k", "a\nb".to_string())];
        assert_eq!(render_kv(&rows, 0), "k  a\\nb\n");
        let rows = vec![("\x1b[2Jk", "v".to_string()), ("kk", "w".to_string())];
        assert_eq!(render_kv(&rows, 0), "\\x1B[2Jk  v\nkk        w\n");
    }

    /// The prose shape [`wrap_if_tty`] produces on a wide terminal, minus the
    /// tty detection, so the tests run identically under a captured stdout.
    fn wrap(text: &str) -> String {
        wrap_text(text, PROSE_WIDTH, CodeSpans::Keep).join("\n")
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
    fn sanitize_escapes_control_characters_except_tab() {
        assert_eq!(
            sanitize("a\u{0001}b\u{000B}c\u{007F}d\te\u{009B}f"),
            r"a\x01b\x0Bc\x7Fd	e\x9Bf"
        );
    }

    #[test]
    fn sanitize_multiline_keeps_newlines_but_escapes_other_controls() {
        // Real newlines survive (so a description renders as prose), \r is dropped,
        // and other control chars are still escaped.
        assert_eq!(
            sanitize_multiline("one\ntwo\r\nthree\u{0001}"),
            "one\ntwo\nthree\\x01"
        );
    }
}
