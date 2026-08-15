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
//! proves its prompt is rendered. The echo-off guarantee also makes the
//! no-plaintext assertions exact: a secret can only appear in the transcript
//! if snouty itself renders it.

#![cfg(unix)]

mod support;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use expectrl::Expect;
use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;

/// How long to wait for any single expected string. Generous for CI; a healthy
/// exchange completes in milliseconds.
const EXPECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The PTY's column count.
const PTY_COLS: u16 = 80;

/// The longest contiguous star run a masked prompt can show: `inquire`
/// renders masked input in a one-row scrolling viewport, so the run is capped
/// by the row. Half a row keeps the assertion far from the exact prompt and
/// viewport padding, which belong to `inquire`.
fn row_of_stars() -> String {
    "*".repeat(PTY_COLS as usize / 2)
}

/// The `GET /auth/cli/config` body that reports OAuth disabled for the CLI.
const OAUTH_DISABLED: &str = r#"{"port_strategy":"disabled"}"#;

/// Spawn `snouty login --tenant … --repository …` on a PTY against an
/// isolated `$HOME` and a mock backend that answers the `/auth/cli/config`
/// probe with [`OAUTH_DISABLED`].
fn start_login() -> (tempfile::TempDir, OsSession) {
    start_login_with(&[])
}

/// As [`start_login`], with each `(path, contents)` in `seed` written under the
/// temp `$HOME` first — the pre-existing state a fresh login would not have.
fn start_login_with(seed: &[(&str, &str)]) -> (tempfile::TempDir, OsSession) {
    let home = tempfile::TempDir::new().expect("temp HOME");
    for (path, contents) in seed {
        let path = home.path().join(path);
        std::fs::create_dir_all(path.parent().expect("seed path has a parent"))
            .expect("create the seed directory");
        std::fs::write(path, contents).expect("write the seed file");
    }
    let base_url = support::start_mock_server(OAUTH_DISABLED, 200);

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
        .env("HOME", home.path())
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
    (home, session)
}

/// Wait until `needle` appears in the session output, consuming through it.
/// Returns the consumed text so tests can assert on what was rendered.
///
/// On failure the expectrl error itself carries no context
/// (zhiburt/expectrl#75), so this drains everything the child rendered since
/// the last match — `try_read` empties the session's unmatched buffer first,
/// the drain pattern the expectrl maintainer recommends — and panics with it.
/// ANSI escape sequences are stripped for readability (the prompts re-render
/// on every keystroke, so the raw stream is mostly cursor movements); the
/// panic message says so, since a needle can legitimately fail to match
/// *because of* escape sequences that the stripped view no longer shows.
fn expect(session: &mut OsSession, needle: &str) -> String {
    match Expect::expect(session, needle) {
        Ok(captures) => String::from_utf8_lossy(captures.as_bytes()).into_owned(),
        Err(err) => {
            let mut pending = Vec::new();
            let mut chunk = [0u8; 4096];
            while let Ok(n) = session.try_read(&mut chunk) {
                if n == 0 {
                    break;
                }
                pending.extend(&chunk[..n]);
            }
            let stripped = strip_ansi_escapes::strip(&pending);
            panic!(
                "gave up waiting for {needle:?}: {err}\n\
                 unmatched output since the last expect — note: ANSI escape sequences \
                 have been stripped, so styled text (e.g. the menu cursor) may differ \
                 from the raw byte stream:\n{}",
                String::from_utf8_lossy(&stripped)
            );
        }
    }
}

/// Type `input` at the terminal.
fn send(session: &mut OsSession, input: &str) {
    Expect::send(session, input).expect("write to the PTY");
}

/// Select the API-key entry: the first menu option is highlighted without any
/// keypress, so a bare Enter picks it (the menu is [API Key, Username &
/// password] with OAuth disabled — reaching the API-key prompt proves the
/// default; the unit tests own the default-index logic directly).
fn choose_api_key(session: &mut OsSession) {
    expect(session, "What kind of credentials would you like to use?");
    send(session, "\r");
    expect(session, "Please enter your API Key");
}

/// Wait for the summary and a clean exit. Returns the consumed text.
fn finish(mut session: OsSession) -> String {
    let seen = expect(&mut session, "Run `snouty doctor` to verify your setup.");
    let status = session.get_process().wait().expect("wait for snouty login");
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "login failed: {status:?}"
    );
    seen
}

fn credentials(home: &Path) -> String {
    std::fs::read_to_string(home.join(".config/snouty/credentials.toml")).unwrap_or_default()
}

/// A pasted API key echoes one `*` per character, and the key itself never
/// reaches the screen.
#[test]
fn bare_enter_selects_api_key_and_input_is_masked() {
    let (home, mut session) = start_login();
    choose_api_key(&mut session);

    let key = "sk-pty-key-123";
    send(&mut session, key);
    let mut seen = expect(&mut session, &"*".repeat(key.len()));
    send(&mut session, "\r");
    seen += &finish(session);

    assert!(!seen.contains(key), "the key must never be rendered");
    let creds = credentials(home.path());
    assert!(creds.contains(&format!(r#"api_key = "{key}""#)), "{creds}");
}

/// An API key wider than the terminal is accepted whole: the scrolling
/// viewport keeps echoing stars, backspacing plus retyping edits the real
/// value, and no fragment of the key reaches the screen.
#[test]
fn accepts_an_api_key_longer_than_the_terminal_width() {
    let (home, mut session) = start_login();
    choose_api_key(&mut session);

    let long_key = format!("sk-{}", "a".repeat(197));
    assert!(
        long_key.len() > PTY_COLS as usize,
        "the key must be wider than the terminal for this test to mean anything"
    );
    send(&mut session, &long_key);
    let mut seen = expect(&mut session, &row_of_stars());
    // Erase the last four characters and retype a distinctive tail; the edit
    // must land on the value, not just the rendering.
    send(&mut session, "\u{7f}\u{7f}\u{7f}\u{7f}WXYZ");
    seen += &expect(&mut session, &row_of_stars());
    send(&mut session, "\r");
    seen += &finish(session);

    assert!(
        !seen.contains("aaaa") && !seen.contains("WXYZ"),
        "no fragment of the key may be rendered"
    );
    let expected = format!("{}WXYZ", &long_key[..long_key.len() - 4]);
    let creds = credentials(home.path());
    assert!(
        creds.contains(&format!(r#"api_key = "{expected}""#)),
        "{creds}"
    );
}

/// A stored API key is offered back at the prompt as a masked hint, and a bare
/// Enter keeps it. The stored key never reaches the screen — only its first few
/// characters, which is what makes the hint recognizable.
#[test]
fn bare_enter_keeps_the_stored_api_key() {
    let stored = "antithesis_api_key_v2_PTYSTORED_9Pgw";
    let (home, mut session) = start_login_with(&[(
        ".config/snouty/credentials.toml",
        &format!("[default]\ntype = \"ApiKey\"\napi_key = \"{stored}\"\n"),
    )]);

    expect(
        &mut session,
        "What kind of credentials would you like to use?",
    );
    send(&mut session, "\r");
    // The hint: stars, then the key's own last characters. Antithesis keys
    // share a constant prefix, so the tail is what tells two of them apart.
    let mut seen = expect(&mut session, "Please enter your API Key (********9Pgw)");
    send(&mut session, "\r");
    seen += &finish(session);

    assert!(
        !seen.contains(stored),
        "the stored key must never be rendered whole"
    );
    let creds = credentials(home.path());
    assert!(
        creds.contains(&format!(r#"api_key = "{stored}""#)),
        "a bare Enter must keep the stored key: {creds}"
    );
}

/// The username/password flow (last menu entry) collects the password exactly
/// once, masked: an Antithesis password is a long generated string pasted like
/// an API key, so there is no confirmation round.
#[test]
fn password_flow_asks_once_and_masks_the_password() {
    let (home, mut session) = start_login();

    expect(&mut session, "Username & password (deprecated)");
    send(&mut session, "\x1b[B\r"); // arrow down to the last entry, select it

    expect(&mut session, "What username would you like to use?");
    send(&mut session, "pty-user\r");

    expect(&mut session, "Please enter your password");
    let password = format!("pw-{}", "b".repeat(60));
    send(&mut session, &password);
    let mut seen = expect(&mut session, &row_of_stars());
    send(&mut session, "\r");
    seen += &finish(session);

    assert!(
        !seen.contains("bbbb"),
        "no fragment of the password may be rendered"
    );
    let creds = credentials(home.path());
    assert!(creds.contains(r#"username = "pty-user""#), "{creds}");
    assert!(
        creds.contains(&format!(r#"password = "{password}""#)),
        "{creds}"
    );
}
