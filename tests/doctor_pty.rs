//! `snouty doctor` wraps its notes only on a terminal, so this runs it on a
//! real pseudo-terminal. The spec-test harness has no TTY: there, notes keep
//! whole lines (covered by specs/doctor.txt).

#![cfg(unix)]

use std::process::Command;
use std::time::Duration;

use expectrl::Expect;
use expectrl::session::OsSession;

/// The PTY's column count. `stty` sets it in the child before snouty starts,
/// so no read of the terminal size can race the resize.
const PTY_COLS: usize = 80;

/// A note too long for the terminal wraps at a word boundary, with its
/// continuation lines hung under the note's text, never wider than the
/// terminal.
#[test]
fn long_notes_wrap_under_their_text() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            &format!("stty cols {PTY_COLS} rows 40; exec \"$0\" doctor --offline"),
            env!("CARGO_BIN_EXE_snouty"),
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env("ANTITHESIS_TENANT", "pty-tenant")
        .env("ANTITHESIS_REPOSITORY", "pty-repo")
        // Username/password credentials draw the long deprecation warning.
        .env("ANTITHESIS_USERNAME", "pty-user")
        .env("ANTITHESIS_PASSWORD", "pty-password");

    let mut session = OsSession::spawn(command).expect("spawn snouty doctor on a PTY");
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect("Resolved settings")
        .expect("doctor prints its settings after the checks");
    let screen =
        String::from_utf8_lossy(&strip_ansi_escapes::strip(captures.as_bytes())).replace('\r', "");

    let lines: Vec<&str> = screen.lines().collect();
    for line in &lines {
        assert!(
            line.chars().count() <= PTY_COLS,
            "line wider than the terminal: {line:?}\n{screen}"
        );
    }
    let warning = lines
        .iter()
        .position(|line| line.contains("WARNING: username/password authentication is deprecated"))
        .unwrap_or_else(|| panic!("no deprecation warning in:\n{screen}"));
    // `      WARNING: ` is 15 columns: the continuation starts under the text.
    let hang = " ".repeat(15);
    let continuation = lines[warning + 1];
    assert!(
        continuation.starts_with(&hang) && !continuation[hang.len()..].starts_with(' '),
        "continuation not hung under the text: {continuation:?}\n{screen}"
    );
    assert!(
        screen.contains("authentication method"),
        "a word was split across lines:\n{screen}"
    );
}
