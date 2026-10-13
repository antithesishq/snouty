//! `snouty runs properties --detail` wraps a long Name only on a terminal, so
//! this runs it on a real pseudo-terminal. The spec-test harness has no TTY.

#![cfg(unix)]

use std::process::Command;
use std::time::Duration;

use expectrl::Expect;
use expectrl::session::OsSession;
use snouty::testutils::MockApiServer;

/// Narrow enough that the mock's "Counter value stays
/// below limit" does not fit after the 10-column label. `stty` sets it in the
/// child before snouty starts, so no read of the terminal size can race the
/// resize.
const PTY_COLS: usize = 36;

#[test]
fn long_name_wraps_under_its_value() {
    let server = MockApiServer::start();
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            &format!(
                "stty rows 40 cols {PTY_COLS}; exec \"$0\" runs properties run-1 --name counter --detail"
            ),
            env!("CARGO_BIN_EXE_snouty"),
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env("ANTITHESIS_TENANT", "pty-tenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());

    let mut session = OsSession::spawn(command).expect("spawn snouty on a PTY");
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect("<hash> <vtime>")
        .expect("the detail ends with the runs logs next step");
    let screen =
        String::from_utf8_lossy(&strip_ansi_escapes::strip(captures.as_bytes())).replace('\r', "");

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
    assert_eq!(lines[name + 2], "Group     Safety", "{screen}");
}
