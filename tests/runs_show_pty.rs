//! `snouty runs show` wraps a long value only on a terminal, so this runs it
//! on a real pseudo-terminal. The spec-test harness has no TTY.

#![cfg(unix)]

mod support;

use std::time::Duration;

use expectrl::Expect;
use snouty::testutils::MockApiServer;
use support::pty::PtyCommand;

/// Narrow enough that the mock's "the workload container exited early" does
/// not fit after the 16-column label.
const PTY_COLS: u16 = 40;

#[test]
fn long_failure_reason_wraps_under_its_value() {
    let server = MockApiServer::start();
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = PtyCommand::new(r#"exec "$0" runs show run-3"#);
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env("ANTITHESIS_TENANT", "pty-tenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());

    let mut session = command.spawn(PTY_COLS, 60);
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect("--web")
        .expect("the detail ends with the browser next step");
    let screen =
        String::from_utf8_lossy(&strip_ansi_escapes::strip(captures.as_bytes())).replace('\r', "");

    let lines: Vec<&str> = screen.lines().collect();
    let reason = lines
        .iter()
        .position(|line| line.starts_with("Failure Reason"))
        .unwrap_or_else(|| panic!("no Failure Reason line in:\n{screen}"));
    assert_eq!(
        lines[reason], "Failure Reason  the workload container",
        "{screen}"
    );
    assert_eq!(
        lines[reason + 1],
        "                exited early",
        "{screen}"
    );
}
