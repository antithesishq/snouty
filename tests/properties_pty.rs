//! `snouty runs properties --detail` wraps a long Name only on a terminal, so
//! this runs it on a real pseudo-terminal. The spec-test harness has no TTY.

#![cfg(unix)]

mod support;

use std::time::Duration;

use expectrl::Expect;
use snouty::testutils::MockApiServer;
use support::pty::PtyCommand;

/// Run `snouty <args>` against the mock server on a PTY `cols` wide, and
/// return the screen up to and including `last`, without ANSI escapes.
fn run_on_pty(args: &str, cols: u16, last: &str) -> String {
    let server = MockApiServer::start();
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = PtyCommand::new(&format!(r#"exec "$0" {args}"#));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env("ANTITHESIS_TENANT", "pty-tenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());

    let mut session = command.spawn(cols, 40);
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect(last)
        .unwrap_or_else(|e| panic!("no {last:?} on the screen: {e}"));
    String::from_utf8_lossy(&strip_ansi_escapes::strip(captures.as_bytes())).replace('\r', "")
}

/// Narrow enough that the mock's "Counter value stays below limit" does not
/// fit after the 10-column label.
const PTY_COLS: u16 = 36;

#[test]
fn long_name_wraps_under_its_value() {
    let screen = run_on_pty(
        "runs properties run-1 --name counter --detail",
        PTY_COLS,
        "<hash> <vtime>",
    );

    let lines: Vec<&str> = screen.lines().collect();
    let name = lines
        .iter()
        .position(|line| line.starts_with("Name"))
        .unwrap_or_else(|| panic!("no Name line in:\n{screen}"));
    assert_eq!(
        lines[name], "Name      Counter value stays below",
        "{screen}"
    );
    assert_eq!(lines[name + 1], "          limit", "{screen}");
    assert!(lines[name + 2].starts_with("ID"), "{screen}");
}

/// The pending note wraps at word boundaries on a terminal, and keeps the
/// command to run again on one line.
#[test]
fn pending_note_wraps_on_a_terminal() {
    let screen = run_on_pty("runs properties run-1", 48, "final results.");
    let lines: Vec<&str> = screen.lines().collect();
    let start = lines
        .iter()
        .position(|line| line.starts_with("Some properties"))
        .unwrap_or_else(|| panic!("no pending note in:\n{screen}"));
    assert_eq!(
        lines[start..],
        [
            "Some properties are still being processed. Run",
            "`snouty runs properties run-1` again later for",
            "the final results.",
        ],
        "{screen}"
    );
}
