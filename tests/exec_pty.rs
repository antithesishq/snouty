//! `snouty runs exec` draws rewarm progress as a bar only on a terminal, so
//! this runs it on a real pseudo-terminal. The spec-test harness has no TTY:
//! there, progress prints one line per record (covered by
//! specs/runs_exec.txt).

#![cfg(unix)]

use std::process::Command;
use std::time::Duration;

use expectrl::Expect;
use expectrl::session::OsSession;
use snouty::testutils::MockApiServer;

/// The bar redraws in place, and once the rewarm finishes it gives way to a
/// line of its own, so the script's output starts on a fresh line.
#[test]
fn rewarm_bar_gives_way_to_the_output() {
    let server = MockApiServer::start();
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = Command::new(env!("CARGO_BIN_EXE_snouty"));
    command
        // The mock rewarms its cold moment when a source is named.
        .args([
            "runs",
            "exec",
            "run-2",
            "1002528785118888238",
            "278.1311443040613",
            "uname -a",
            "--source-run-id",
            "source-run",
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env("SNOUTY_UNSTABLE_FEATURES", "runs-exec")
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());

    let mut session = OsSession::spawn(command).expect("spawn snouty runs exec on a PTY");
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session
        .expect("end moment")
        .expect("exec prints the end-moment trailer");
    let raw = String::from_utf8_lossy(captures.as_bytes()).into_owned();
    assert!(raw.contains("rewarming moment ["), "no bar in:\n{raw}");

    // What each line shows once drawn: a carriage return inside a line
    // redraws it. The PTY ends each line with `\r\n`. Strip escapes after the
    // split, since stripping drops the `\r`.
    let lines: Vec<String> = raw
        .split("\r\n")
        .map(|line| strip_ansi_escapes::strip_str(line.rsplit('\r').next().unwrap_or_default()))
        .collect();
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("rewarming moment: 0%")),
        "progress lines on a terminal:\n{raw}"
    );
    let done = lines
        .iter()
        .position(|line| line.trim_end() == "rewarming moment: done")
        .unwrap_or_else(|| panic!("no done line in:\n{raw}"));
    assert_eq!(
        lines[done + 1].trim_end(),
        "Linux antithesis 6.12.0",
        "output does not follow the done line:\n{raw}"
    );
}
