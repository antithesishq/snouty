//! Drives `snouty login`'s interactive prompts end-to-end through a real
//! pseudo-terminal, using `expectrl` for the PTY plumbing and the
//! expect/send dialogue.
//!
//! The prompts only engage when stdin is a terminal, so these tests spawn the
//! binary on a PTY and script the exchange. The spec-test harness cannot host
//! this: testscript runs commands without a TTY, which sends `snouty login`
//! down its non-interactive path (covered by specs/login.txt).
//!
//! Determinism notes: `expectrl` disables terminal echo inside the child
//! before exec, so the transcript contains only what snouty renders, and the
//! `inquire` prompts hold the terminal in raw mode for a prompt's whole
//! lifetime, so bytes sent while a prompt is on screen are never line-edited
//! by the kernel. Every `send` below is therefore gated on an `expect` that
//! proves its prompt is rendered.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use expectrl::Expect;
use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;

/// How long to wait for any single expected string. Generous for CI; a healthy
/// exchange completes in milliseconds.
const EXPECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The PTY's column count: narrower than the long-key test's input, so that
/// test exercises input wider than the terminal.
const PTY_COLS: u16 = 80;

/// Answer every HTTP request with 200 and `body` — enough for login's
/// `GET /auth/cli/config` probe. Returns the server's base URL.
fn spawn_config_server(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind config server");
    let addr = listener.local_addr().expect("config server addr");
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    format!("http://{addr}")
}

/// Spawn `snouty login --tenant … --repository …` on a PTY against an isolated
/// `$HOME` and the given mock backend.
fn spawn_login(home: &Path, base_url: &str) -> OsSession {
    let mut command = Command::new(env!("CARGO_BIN_EXE_snouty"));
    command
        .args([
            "login",
            "--tenant",
            "pty-tenant",
            "--repository",
            "pty-repo",
        ])
        .env_clear()
        .env("HOME", home)
        .env("TERM", "xterm-256color")
        .env("ANTITHESIS_BASE_URL", base_url)
        // Force file-based credential storage so a macOS run doesn't touch
        // the real keychain.
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1");

    let mut session = OsSession::spawn(command).expect("spawn snouty login on a PTY");
    // The size is set while the child is still probing /auth/cli/config, well
    // before any prompt reads the terminal size.
    session
        .get_process_mut()
        .set_window_size(PTY_COLS, 24)
        .expect("set the PTY window size");
    session.set_expect_timeout(Some(EXPECT_TIMEOUT));
    session
}

/// Wait until `needle` appears in the session output, consuming through it.
fn expect(session: &mut OsSession, needle: &str) {
    if let Err(err) = Expect::expect(session, needle) {
        panic!("gave up waiting for {needle:?}: {err}");
    }
}

/// Type `input` at the terminal.
fn send(session: &mut OsSession, input: &str) {
    Expect::send(session, input).expect("write to the PTY");
}

/// Wait for the child to exit and assert it succeeded.
fn finish(mut session: OsSession) {
    expect(&mut session, "Run `snouty doctor` to verify your setup.");
    let status = session.get_process().wait().expect("wait for snouty login");
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "login failed: {status:?}"
    );
}

fn credentials(home: &Path) -> String {
    std::fs::read_to_string(home.join(".config/snouty/credentials.toml")).unwrap_or_default()
}

const OAUTH_DISABLED: &str = r#"{"port_strategy":"disabled"}"#;

/// The first menu option is selected without any keypress: a bare Enter picks
/// "API Key" (the menu order is [API Key, Username & password] with OAuth
/// disabled, so reaching the API-key prompt proves the default). The key input
/// echoes one `*` per pasted character and never echoes the key itself.
#[test]
fn menu_defaults_to_first_option_and_masks_the_api_key() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = spawn_login(home.path(), &base_url);

    expect(
        &mut session,
        "What kind of credentials would you like to use?",
    );
    send(&mut session, "\r");

    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123");
    expect(&mut session, &"*".repeat("sk-pty-key-123".len()));
    // The plaintext key must not have been rendered ahead of the stars.
    send(&mut session, "\r");
    finish(session);

    let creds = credentials(home.path());
    assert!(creds.contains(r#"api_key = "sk-pty-key-123""#), "{creds}");
}

/// An API key wider than the terminal is accepted whole. `inquire` renders
/// the masked input as a scrolling one-row viewport, so what must appear is a
/// row's worth of stars — never the key itself — while backspacing plus
/// retyping edits the real value.
#[test]
fn accepts_an_api_key_longer_than_the_terminal_width() {
    // The widest star run the viewport can show is one row (PTY_COLS); assert
    // most of a row so the test doesn't couple to the exact viewport padding.
    let visible_stars = "*".repeat(PTY_COLS as usize - 20);

    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = spawn_login(home.path(), &base_url);

    expect(
        &mut session,
        "What kind of credentials would you like to use?",
    );
    send(&mut session, "\r");

    expect(&mut session, "Please enter your API Key");
    let long_key = format!("sk-{}", "a".repeat(197));
    send(&mut session, &long_key);
    expect(&mut session, &visible_stars);
    // Erase the last four characters and retype a distinctive tail; the edit
    // must land on the value, not just the rendering.
    send(&mut session, "\u{7f}\u{7f}\u{7f}\u{7f}WXYZ");
    expect(&mut session, &visible_stars);
    send(&mut session, "\r");
    finish(session);

    let expected = format!("{}WXYZ", &long_key[..long_key.len() - 4]);
    let creds = credentials(home.path());
    assert!(
        creds.contains(&format!(r#"api_key = "{expected}""#)),
        "{creds}"
    );
}

/// The username/password flow (last menu entry) confirms the password and
/// retries on a mismatch.
#[test]
fn password_confirmation_retries_on_mismatch() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = spawn_login(home.path(), &base_url);

    expect(&mut session, "Username & password (deprecated)");
    send(&mut session, "\x1b[B\r"); // arrow down to the last entry, select it

    expect(&mut session, "What username would you like to use?");
    send(&mut session, "pty-user\r");

    expect(&mut session, "Please enter your password");
    send(&mut session, "hunter2\r");
    expect(&mut session, "Please reenter your password to confirm");
    send(&mut session, "hunter3\r");
    expect(&mut session, "Passwords did not match");

    // The whole password prompt restarts after a mismatch; answer it
    // consistently this time.
    expect(&mut session, "Please enter your password");
    send(&mut session, "hunter2\r");
    expect(&mut session, "Please reenter your password to confirm");
    send(&mut session, "hunter2\r");
    finish(session);

    let creds = credentials(home.path());
    assert!(creds.contains(r#"username = "pty-user""#), "{creds}");
    assert!(creds.contains(r#"password = "hunter2""#), "{creds}");
}
