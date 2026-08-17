//! ANSI escape handling for terminal-bound text: recognizing escape
//! sequences, stripping them, and the keep-colors sanitizer that lets SGR
//! (and only SGR) survive into the output.

use std::borrow::Cow;
use std::sync::OnceLock;

use regex::Regex;

use crate::render::sanitize;

fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(concat!(
            r"\x1b\[[\x20-\x3f]*[\x40-\x7e]",      // CSI: ESC [ ... final
            r"|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", // OSC: ESC ] ... (BEL | ESC \)
            r"|\x1b[\x20-\x7e]",                   // two-byte: ESC + single printable
        ))
        .unwrap()
    })
}

pub(crate) fn strip_ansi(text: &str) -> Cow<'_, str> {
    ansi_re().replace_all(text, "")
}

/// Terminal-bound free text with no color policy: every escape sequence is
/// dropped and the remaining control bytes are escaped so stray
/// `\r`/`\x08`/BEL can't corrupt the terminal. The keep-colors case of the
/// same mechanism is [`sanitize_log_text`].
pub(crate) fn normalize_terminal_text(text: &str) -> String {
    sanitize_log_text_inner(text, false)
}

/// Is `seq` an SGR sequence (`ESC [ params m`, digits and `;` only)? SGR only
/// restyles characters — it cannot move the cursor, clear the screen, or
/// retitle the window — so it is the one escape family log text may keep.
fn is_sgr(seq: &str) -> bool {
    seq.strip_prefix("\x1b[")
        .and_then(|s| s.strip_suffix('m'))
        .is_some_and(|params| params.bytes().all(|b| b.is_ascii_digit() || b == b';'))
}

/// Sanitize a SUT log line for the terminal while keeping its colors: SGR
/// sequences pass through (when the terminal takes colors at all), every
/// other escape sequence is dropped, remaining control bytes are escaped, and
/// a reset is appended after the last kept sequence so a dangling color can't
/// bleed into the rest of the stream.
pub(super) fn sanitize_log_text(text: &str) -> String {
    sanitize_log_text_inner(text, console::colors_enabled())
}

fn sanitize_log_text_inner(text: &str, keep_colors: bool) -> String {
    let mut out = String::new();
    let mut kept_any = false;
    let mut last = 0;
    for m in ansi_re().find_iter(text) {
        out.push_str(&sanitize(&text[last..m.start()]));
        if keep_colors && is_sgr(m.as_str()) {
            out.push_str(m.as_str());
            kept_any = true;
        }
        last = m.end();
    }
    out.push_str(&sanitize(&text[last..]));
    if kept_any {
        out.push_str("\x1b[0m");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_text_keeps_sgr_colors_but_nothing_else() {
        console::set_colors_enabled(false);
        // Colors off (a pipe): everything is stripped and controls escaped.
        assert_eq!(
            sanitize_log_text("\x1B[31mred\x1B[0m\u{0008}done"),
            r"red\x08done"
        );

        // Colors on: SGR passes through with a trailing reset; cursor moves,
        // OSC retitles, and other escapes are still dropped, and stray
        // control bytes are still escaped.
        assert_eq!(
            sanitize_log_text_inner("\x1B[31mred\x1B[0m plain", true),
            "\x1B[31mred\x1B[0m plain\x1B[0m"
        );
        assert_eq!(
            sanitize_log_text_inner("a\x1B[2Ab\x1B]0;title\x07c\rd", true),
            r"abc\rd"
        );
        // is_sgr admits only `ESC[…m` with digit/semicolon params.
        assert!(is_sgr("\x1b[31;1m"));
        assert!(is_sgr("\x1b[m"));
        assert!(!is_sgr("\x1b[2A"));
        assert!(!is_sgr("\x1b[?25lm"));
    }

    #[test]
    fn ansi_sgr() {
        assert_eq!(strip_ansi("\x1b[1mbold\x1b[0m"), "bold");
        assert_eq!(strip_ansi("\x1b[38;5;196mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[38;2;255;0;0mred\x1b[0m"), "red");
        assert_eq!(strip_ansi("\x1b[1;31;42mtext\x1b[0m"), "text");
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T08:19:54Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mfoobar\x1b[0m\x1b[2m:\x1b[0m ready"
            ),
            "2026-04-03T08:19:54Z  INFO foobar: ready"
        );
    }

    #[test]
    fn ansi_csi_non_sgr() {
        assert_eq!(strip_ansi("left\x1b[2Aright"), "leftright");
        assert_eq!(strip_ansi("text\x1b[2K"), "text");
        assert_eq!(strip_ansi("\x1b[?25hvisible"), "visible");
        assert_eq!(strip_ansi("\x1b[?25l hidden"), " hidden");
    }

    #[test]
    fn ansi_osc() {
        assert_eq!(
            strip_ansi("\x1b]0;my window title\x07text after"),
            "text after"
        );
        assert_eq!(strip_ansi("\x1b]0;my title\x1b\\text after"), "text after");
    }

    #[test]
    fn ansi_two_byte() {
        assert_eq!(strip_ansi("\x1bcafter reset"), "after reset");
        assert_eq!(strip_ansi("before\x1b7after"), "beforeafter");
    }

    #[test]
    fn ansi_passthrough() {
        let cases = [
            "no escapes here",
            r#"{"key": "value", "nested": {"a": [1,2,3]}}"#,
            r#"{"url": "http://example.com/path?q=1&r=2", "count": 42}"#,
            r#"Options { address: Some(0.0.0.0:3307), deployment: "mydb", mode: Standalone }"#,
            r#"Settings { inner: Inner { values: [1, 2, 3] }, name: "test" }"#,
            "[2026-04-03] [INFO] [main] started",
            r#"path: "/nix/store/abc-pkg/bin/cmd""#,
            r#"{"msg": "he said \"hello\""}"#,
        ];
        for c in cases {
            assert_eq!(strip_ansi(c), c, "passthrough failed: {c:?}");
        }
    }

    #[test]
    fn ansi_mixed() {
        assert_eq!(
            strip_ansi("\x1b[2m{\"key\": \"value\"}\x1b[0m"),
            r#"{"key": "value"}"#
        );
        assert_eq!(
            strip_ansi("\x1b[3mOptions { mode: Standalone }\x1b[0m"),
            "Options { mode: Standalone }"
        );
        assert_eq!(
            strip_ansi(
                "\x1b[2m2026-04-03T00:00:00Z\x1b[0m \x1b[32m INFO\x1b[0m request completed {\"status\": 200, \"latency_ms\": 42}"
            ),
            r#"2026-04-03T00:00:00Z  INFO request completed {"status": 200, "latency_ms": 42}"#
        );
    }
}
