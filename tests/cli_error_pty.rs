//! A clap error wraps only on a terminal, so this runs snouty on a
//! pseudo-terminal. specs/launch.txt covers the piped case.

#![cfg(unix)]

mod support;

use std::time::Duration;

use expectrl::Expect;
use support::pty::PtyCommand;

const PTY_COLS: u16 = 60;

/// A long argument error wraps at word boundaries within the terminal, and
/// keeps clap's colors and its closing `--help` line.
#[test]
fn long_argument_error_wraps_at_words() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = PtyCommand::new(r#"exec "$0" launch --setup-timeout 10"#);
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color");

    let mut session = command.spawn(PTY_COLS, 40);
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect("For more information")
        .expect("the error ends with the --help line");
    let raw = String::from_utf8_lossy(captures.as_bytes()).replace('\r', "");
    assert!(
        raw.split_once("error:")
            .is_some_and(|(before, _)| before.contains('\x1b')),
        "error: is not colored:\n{raw:?}"
    );
    let screen = String::from_utf8_lossy(&strip_ansi_escapes::strip(raw.as_bytes())).into_owned();

    let lines: Vec<&str> = screen.lines().collect();
    for line in &lines {
        assert!(
            line.chars().count() <= usize::from(PTY_COLS),
            "line wider than the terminal: {line:?}\n{screen}"
        );
    }
    let first = lines
        .iter()
        .position(|line| line.starts_with("error: "))
        .unwrap_or_else(|| panic!("no error line in:\n{screen}"));
    let message = lines[first..]
        .iter()
        .take_while(|line| !line.is_empty())
        .copied()
        .collect::<Vec<_>>();
    assert!(message.len() > 1, "the error did not wrap:\n{screen}");
    assert_eq!(
        message.join(" "),
        "error: invalid value '10' for '--setup-timeout <SETUP_TIMEOUT>': must be a \
         whole number with an h, m or s unit, or several in that order (e.g. `10m`, \
         `30m`); a bare number is not permitted",
        "a word was split across lines:\n{screen}"
    );
}
