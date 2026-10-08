//! `runs exec` draws its rewarm bar only on a terminal, so these tests run it
//! on a pseudo-terminal. specs/runs_exec.txt covers the line-per-record
//! output.

#![cfg(unix)]

use std::process::Command;
use std::time::Duration;

use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;
use expectrl::{Eof, Expect};
use snouty::features::{Feature, UNSTABLE_FEATURES_VAR_NAME};
use snouty::testutils::{MOCK_COLD_HASH, MockApiServer};

/// Runs `runs exec` on a PTY at the mock's cold moment, with a source, and
/// asserts that it draws a bar, not progress lines. Returns the final text of
/// each terminal line, the raw capture, and the exit code.
fn exec_on_pty(script: &str) -> (Vec<String>, String, i32) {
    let server = MockApiServer::start();
    let home = tempfile::TempDir::new().expect("temp HOME");
    let mut command = Command::new(env!("CARGO_BIN_EXE_snouty"));
    command
        .args(["runs", "exec", "run-2", MOCK_COLD_HASH, "278.1311443040613"])
        .args([script, "--source-run-id", "source-run"])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        .env(UNSTABLE_FEATURES_VAR_NAME, Feature::RUNS_EXEC)
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());

    let mut session = OsSession::spawn(command).expect("spawn snouty runs exec on a PTY");
    session.set_expect_timeout(Some(Duration::from_secs(30)));
    let captures = session.expect(Eof).expect("snouty runs exec ends");
    let raw = String::from_utf8_lossy(captures.as_bytes()).into_owned();
    let code = match session.get_process().wait().expect("wait for snouty") {
        WaitStatus::Exited(_, code) => code,
        status => panic!("snouty did not exit: {status:?}\n{raw}"),
    };
    assert!(raw.contains("rewarming moment ["), "no bar in:\n{raw}");
    assert!(
        !raw.contains("rewarming moment: 0%"),
        "progress lines on a terminal:\n{raw}"
    );

    // The PTY ends each line with `\r\n`, and a bare `\r` redraws a line, so
    // keep the text after the last `\r`. Strip escapes after the split,
    // because stripping drops `\r`.
    let lines = raw
        .split("\r\n")
        .map(|line| {
            let shown = line.rsplit('\r').next().unwrap_or_default();
            strip_ansi_escapes::strip_str(shown).trim_end().to_owned()
        })
        .collect();
    (lines, raw, code)
}

/// The line after the first line that `is_first` matches.
fn line_after<'a>(lines: &'a [String], raw: &str, is_first: impl Fn(&str) -> bool) -> &'a str {
    let first = lines
        .iter()
        .position(|line| is_first(line))
        .unwrap_or_else(|| panic!("no such line in:\n{raw}"));
    lines
        .get(first + 1)
        .unwrap_or_else(|| panic!("nothing after line {first} in:\n{raw}"))
}

/// A finished rewarm replaces its bar with a `done` line, and the output
/// starts on the next line.
#[test]
fn rewarm_bar_gives_way_to_the_output() {
    let (lines, raw, code) = exec_on_pty("uname -a");
    assert_eq!(code, 0, "{raw}");
    let after = line_after(&lines, &raw, |line| line == "rewarming moment: done");
    assert_eq!(after, "Linux antithesis 6.12.0", "{raw}");
}

/// A rewarm the stream abandons keeps its bar where it stopped, and the error
/// starts on the next line.
#[test]
fn abandoned_rewarm_bar_stays_in_view() {
    let (lines, raw, code) = exec_on_pty("truncate-rewarm");
    assert_eq!(code, 1, "{raw}");
    let after = line_after(&lines, &raw, |line| {
        line.starts_with("rewarming moment [") && line.ends_with("] 40%")
    });
    assert!(
        after.contains("stream ended before the command reported completion"),
        "{raw}"
    );
}
