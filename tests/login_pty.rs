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

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use expectrl::Expect;
use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;
use support::pty::PtyCommand;

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

/// A username and password as an older snouty stored them.
const STORED_PASSWORD: &str =
    "[default]\ntype = \"Password\"\nusername = \"puser\"\npassword = \"pty-pass\"\n";

const CREDENTIALS_FILE: &str = ".config/snouty/credentials.toml";

const REPLACE_PASSWORD: &str = "Replace your stored username and password?";

const EMPTY_REFUSED: &str = "An empty value is not valid.";

const RETAINED: &str = "Retained your previously stored credentials";

/// Spawn `snouty login --tenant … --repository …` on a PTY against an
/// isolated `$HOME` and a mock backend that answers the `/auth/cli/config`
/// probe with [`OAUTH_DISABLED`].
fn start_login() -> (tempfile::TempDir, OsSession) {
    start_login_with(&[])
}

/// As [`start_login`], with each `(path, contents)` in `seed` written under the
/// temp `$HOME` first — the pre-existing state a fresh login would not have.
fn start_login_with(seed: &[(&str, &str)]) -> (tempfile::TempDir, OsSession) {
    start_login_with_env(seed, &[])
}

/// As [`start_login_with`], with each `(name, value)` in `env` also set for
/// the child.
fn start_login_with_env(
    seed: &[(&str, &str)],
    env: &[(&str, &str)],
) -> (tempfile::TempDir, OsSession) {
    spawn_login(Login {
        seed,
        env,
        ..Login::default()
    })
}

/// Where the child's stdout goes.
#[derive(Default)]
enum Stdout {
    #[default]
    Pty,
    /// The file [`STDOUT_FILE`] under the temp `$HOME`. stdin and stderr stay on
    /// the PTY.
    File,
}

const STDOUT_FILE: &str = "stdout.txt";

/// How [`spawn_login`] runs `snouty login --tenant … --repository …`.
#[derive(Default)]
struct Login<'a> {
    /// Each `(path, contents)` is written under the temp `$HOME` first.
    seed: &'a [(&'a str, &'a str)],
    /// Each `(name, value)` is set for the child. Without `ANTITHESIS_BASE_URL`
    /// here, a mock backend answers the `/auth/cli/config` probe with
    /// [`OAUTH_DISABLED`].
    env: &'a [(&'a str, &'a str)],
    stdout: Stdout,
    /// Added to the command line.
    args: &'a [&'a str],
}

fn spawn_login(login: Login) -> (tempfile::TempDir, OsSession) {
    let home = tempfile::TempDir::new().expect("temp HOME");
    for (path, contents) in login.seed {
        let path = home.path().join(path);
        std::fs::create_dir_all(path.parent().expect("seed path has a parent"))
            .expect("create the seed directory");
        std::fs::write(path, contents).expect("write the seed file");
    }

    let mut command = match login.stdout {
        Stdout::Pty => PtyCommand::new(r#"exec "$0" "$@""#),
        Stdout::File => PtyCommand::new(&format!(r#"exec "$0" "$@" > "$HOME/{STDOUT_FILE}""#)),
    };
    command
        .args([
            "login",
            "--tenant",
            "pty-tenant",
            "--repository",
            "pty-repo",
        ])
        .args(login.args)
        .env_clear()
        .env("HOME", home.path())
        .env("TERM", "xterm-256color")
        // Force file-based credential storage so a macOS run doesn't touch
        // the real keychain.
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .envs(login.env.iter().copied());
    if !login
        .env
        .iter()
        .any(|&(name, _)| name == "ANTITHESIS_BASE_URL")
    {
        command.env(
            "ANTITHESIS_BASE_URL",
            support::start_mock_server(OAUTH_DISABLED, 200),
        );
    }

    let mut session = command.spawn(PTY_COLS, 24);
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

/// The lines snouty rendered in `seen`, without ANSI escape sequences or the
/// PTY's carriage returns.
fn rendered_lines(seen: &str) -> Vec<String> {
    let stripped = strip_ansi_escapes::strip(seen.as_bytes());
    String::from_utf8_lossy(&stripped)
        .lines()
        .map(|line| line.trim_end_matches('\r').to_string())
        .collect()
}

fn credentials(home: &Path) -> String {
    std::fs::read_to_string(home.join(CREDENTIALS_FILE)).unwrap_or_default()
}

/// A pasted API key echoes one `*` per character, and the key itself never
/// reaches the screen.
#[test]
fn a_pasted_api_key_is_masked() {
    let (home, mut session) = start_login();
    expect(&mut session, "Please enter your API Key");

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
    expect(&mut session, "Please enter your API Key");

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
        CREDENTIALS_FILE,
        &format!("[default]\ntype = \"ApiKey\"\napi_key = \"{stored}\"\n"),
    )]);

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
    assert!(seen.contains(RETAINED), "{seen}");
    assert!(!seen.contains("replacing"), "{seen}");
}

/// A bare Enter at the API key prompt is refused, and the prompt asks again.
#[test]
fn bare_enter_without_a_stored_key_asks_again() {
    let (home, mut session) = start_login();
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "\r");
    expect(&mut session, EMPTY_REFUSED);
    send(&mut session, "sk-pty-key-123\r");
    finish(session);

    let creds = credentials(home.path());
    assert!(creds.contains(r#"api_key = "sk-pty-key-123""#), "{creds}");
}

/// A bare Enter does not replace a stored username and password with an empty
/// key: the prompt refuses it, and Esc then keeps the password.
#[test]
fn bare_enter_keeps_a_stored_password() {
    let (home, mut session) = start_login_with(&[(CREDENTIALS_FILE, STORED_PASSWORD)]);
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "\r");
    expect(&mut session, EMPTY_REFUSED);
    send(&mut session, "\x1b");
    let seen = finish(session);

    assert!(seen.contains(RETAINED), "{seen}");
    assert_eq!(credentials(home.path()), STORED_PASSWORD);
}

/// Login asks before a new API key replaces stored OAuth tokens, and a bare
/// Enter answers no.
#[test]
fn an_api_key_asks_before_it_replaces_stored_oauth_tokens() {
    let stored = "[default]\ntype = \"OAuth\"\nantithesis_token = \"pty-token\"\n";
    let (home, mut session) = start_login_with(&[(CREDENTIALS_FILE, stored)]);
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123\r");
    expect(&mut session, "Replace your stored OAuth credentials?");
    send(&mut session, "\r");
    let seen = finish(session);

    assert!(seen.contains(RETAINED), "{seen}");
    assert_eq!(credentials(home.path()), stored);
}

/// Login asks before a new API key replaces a stored username and password,
/// and a bare Enter answers no: the credentials file stays as it was.
#[test]
fn declining_keeps_a_stored_password() {
    let (home, mut session) = start_login_with(&[(CREDENTIALS_FILE, STORED_PASSWORD)]);
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123\r");
    expect(&mut session, REPLACE_PASSWORD);
    send(&mut session, "\r");
    let seen = finish(session);

    assert!(seen.contains(RETAINED), "{seen}");
    assert_eq!(credentials(home.path()), STORED_PASSWORD);
}

/// Login asks before a new API key replaces a different stored key, and the
/// summary says that it replaced it.
#[test]
fn a_different_api_key_asks_before_it_replaces_the_stored_key() {
    let (home, mut session) = start_login_with(&[(
        CREDENTIALS_FILE,
        "[default]\ntype = \"ApiKey\"\napi_key = \"antithesis_api_key_v2_PTYSTORED_9Pgw\"\n",
    )]);
    expect(&mut session, "Please enter your API Key (********9Pgw)");
    send(&mut session, "sk-pty-key-123\r");
    expect(&mut session, "Replace your stored API key?");
    send(&mut session, "y\r");
    let seen = finish(session);

    assert!(
        rendered_lines(&seen)
            .join(" ")
            .contains("credentials.toml, replacing your stored API key."),
        "{seen}"
    );
    let creds = credentials(home.path());
    assert!(creds.contains(r#"api_key = "sk-pty-key-123""#), "{creds}");
}

/// A new API key that overwrites a stored username and password says so in the
/// summary, because a script that still uses the password will no longer work.
#[test]
fn an_api_key_that_replaces_a_stored_password_says_so() {
    let (home, mut session) = start_login_with(&[(CREDENTIALS_FILE, STORED_PASSWORD)]);
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123\r");
    expect(&mut session, REPLACE_PASSWORD);
    send(&mut session, "y\r");
    let seen = finish(session);

    // The temp path makes this line longer than the PTY, so snouty wraps it.
    let lines = rendered_lines(&seen);
    let start = lines
        .iter()
        .position(|line| line.starts_with("Stored your API key in"))
        .unwrap_or_else(|| panic!("no credentials line: {seen}"));
    let end = start
        + lines[start..]
            .iter()
            .position(|line| line.ends_with('.'))
            .unwrap_or_else(|| panic!("the credentials line does not end: {seen}"));
    assert!(end > start, "the credentials line is not wrapped: {seen}");
    // A temp path longer than the PTY overflows on a line of its own.
    for line in &lines[start..=end] {
        assert!(
            line.chars().count() <= PTY_COLS as usize || !line.contains(' '),
            "{line:?}: {seen}"
        );
    }
    assert!(
        lines[start..=end]
            .join(" ")
            .ends_with("credentials.toml, replacing your stored username and password."),
        "the credentials line is split mid-word: {seen}"
    );
    let creds = credentials(home.path());
    assert!(creds.contains(r#"api_key = "sk-pty-key-123""#), "{creds}");
}

/// A username and password from the environment is still there after login,
/// so the summary does not claim to replace it.
#[test]
fn an_api_key_does_not_claim_to_replace_a_password_from_the_environment() {
    let (_home, mut session) = start_login_with_env(
        &[],
        &[
            ("ANTITHESIS_USERNAME", "puser"),
            ("ANTITHESIS_PASSWORD", "pty-pass"),
        ],
    );
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123\r");
    let seen = finish(session);

    assert!(seen.contains("Stored your API key in"), "{seen}");
    assert!(!seen.contains("replacing"), "{seen}");
}

/// With stdout redirected and stderr on a terminal, the summary keeps whole
/// lines, because stdout is not a terminal.
#[test]
fn a_redirected_summary_keeps_whole_lines() {
    let (home, mut session) = spawn_login(Login {
        seed: &[(CREDENTIALS_FILE, STORED_PASSWORD)],
        stdout: Stdout::File,
        ..Login::default()
    });
    expect(&mut session, "Please enter your API Key");
    send(&mut session, "sk-pty-key-123\r");
    expect(&mut session, REPLACE_PASSWORD);
    send(&mut session, "y\r");
    // Read the PTY to its end before the wait: on macOS a child cannot exit
    // while its terminal output is unread.
    Expect::expect(&mut session, expectrl::Eof).expect("read the PTY to its end");
    let status = session.get_process().wait().expect("wait for snouty login");
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "login failed: {status:?}"
    );

    let summary =
        std::fs::read_to_string(home.path().join(STDOUT_FILE)).expect("read the redirected stdout");
    assert!(
        summary
            .lines()
            .any(|line| line.starts_with("Stored your API key in ")
                && line
                    .ends_with("credentials.toml, replacing your stored username and password.")),
        "{summary}"
    );
}

/// Esc at the API key prompt skips credential storage and writes no
/// credentials file.
#[test]
fn esc_at_the_api_key_prompt_skips_credential_storage() {
    let (home, mut session) = start_login();
    expect(&mut session, "hit Esc to skip");
    send(&mut session, "\x1b");
    let seen = finish(session);

    assert!(seen.contains("Skipped credential storage"), "{seen}");
    assert!(
        !home.path().join(CREDENTIALS_FILE).exists(),
        "Esc must write no credentials file"
    );
}

/// The tokens the mock tenant issues for the OAuth sign-in.
const ACCESS_TOKEN: &str = "pty-access-token";
const REFRESH_TOKEN: &str = "pty-refresh-token";

/// The `code` and `state` the test's callback request hands snouty.
const AUTH_CODE: &str = "pty-auth-code";
const FLOW_TOKEN: &str = "pty-flow-token";

/// What snouty sent the mock tenant of [`start_oauth_tenant`].
#[derive(Default)]
struct OAuthRequests {
    /// The `POST /auth/cli/login` body.
    login: Option<serde_json::Value>,
    /// The `Authorization` header of `POST /auth/cli/token`.
    token_authorization: Option<String>,
}

/// Start a mock tenant that offers OAuth on an ephemeral port and completes
/// each step of the flow. Returns its base URL and the requests it records.
fn start_oauth_tenant() -> (String, Arc<Mutex<OAuthRequests>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind the mock tenant");
    let url = format!(
        "http://{}",
        listener.local_addr().expect("mock tenant address")
    );
    let requests = Arc::new(Mutex::new(OAuthRequests::default()));
    let recorded = Arc::clone(&requests);
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let (head, body) = read_request(&mut stream);
            let request_line = head.lines().next().unwrap_or_default();
            let response = if request_line.starts_with("GET /auth/cli/config ") {
                r#"{"port_strategy":"ephemeral"}"#.to_owned()
            } else if request_line.starts_with("POST /auth/cli/login ") {
                recorded.lock().unwrap().login =
                    Some(serde_json::from_slice(&body).expect("a JSON login body"));
                r#"{"location":"https://idp.example.com/authorize"}"#.to_owned()
            } else if request_line.starts_with("POST /auth/cli/token ") {
                recorded.lock().unwrap().token_authorization = head.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_owned())
                });
                format!(
                    r#"{{"antithesis_token":"{ACCESS_TOKEN}","refresh_token":"{REFRESH_TOKEN}"}}"#
                )
            } else {
                panic!("unexpected request to the mock tenant: {request_line}");
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{response}",
                response.len()
            );
        }
    });
    (url, requests)
}

/// Read one HTTP request: its head as text, and its `Content-Length` body.
fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        let n = stream.read(&mut chunk).expect("read the request");
        assert!(n > 0, "the request ended before its head did");
        buf.extend(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().expect("a numeric Content-Length"))
        })
        .unwrap_or(0);
    let mut body = buf.split_off(head_end + 4);
    while body.len() < length {
        let n = stream.read(&mut chunk).expect("read the request body");
        assert!(n > 0, "the request ended before its body did");
        body.extend(&chunk[..n]);
    }
    (head, body)
}

/// `--verbose` logs the three OAuth requests of a single sign-on to stderr,
/// and no token, code, verifier, or state reaches the log or stdout.
///
/// Linux only: the macOS browser opener has a fixed path, so the sign-in would
/// open a real browser. On Linux, a `PATH` with no opener on it stops that.
#[cfg(target_os = "linux")]
#[test]
fn verbose_logs_the_oauth_requests_without_secrets() {
    let (url, requests) = start_oauth_tenant();
    let no_opener = tempfile::TempDir::new().expect("an empty PATH directory");
    let no_opener = no_opener.path().to_str().expect("a UTF-8 temp path");
    let (home, mut session) = spawn_login(Login {
        env: &[("ANTITHESIS_BASE_URL", &url), ("PATH", no_opener)],
        stdout: Stdout::File,
        args: &["--verbose"],
        ..Login::default()
    });

    let mut stderr = expect(&mut session, "What kind of credentials");
    // Single sign-on is the default.
    send(&mut session, "\r");
    stderr += &expect(&mut session, &format!("> POST {url}/auth/cli/login"));
    // The mock records the login request before it answers.
    stderr += &expect(&mut session, "< 200 OK");
    let port = requests
        .lock()
        .unwrap()
        .login
        .as_ref()
        .expect("a login request")["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .expect("a port number");

    let mut callback = TcpStream::connect(("127.0.0.1", port)).expect("reach the callback");
    write!(
        callback,
        "GET /callback?code={AUTH_CODE}&state={FLOW_TOKEN} HTTP/1.1\r\n\
         Host: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("send the callback");
    let mut page = String::new();
    callback
        .read_to_string(&mut page)
        .expect("read the callback page");
    assert!(page.starts_with("HTTP/1.1 200 "), "{page}");

    let rest = Expect::expect(&mut session, expectrl::Eof).expect("read the PTY to its end");
    stderr += &String::from_utf8_lossy(rest.as_bytes());
    let status = session.get_process().wait().expect("wait for snouty login");
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "login failed: {status:?}\n{stderr}"
    );
    let stdout = std::fs::read_to_string(home.path().join(STDOUT_FILE)).expect("read stdout");

    for request in [
        format!("> GET {url}/auth/cli/config"),
        format!("> POST {url}/auth/cli/login"),
        format!("> POST {url}/auth/cli/token"),
        "> authorization: Bearer [redacted]".to_owned(),
    ] {
        assert!(stderr.contains(&request), "no {request:?} in:\n{stderr}");
    }
    assert_eq!(stderr.matches("> <body withheld>").count(), 2, "{stderr}");

    let requests = requests.lock().unwrap();
    let cli_state = requests.login.as_ref().unwrap()["cli_state"]
        .as_str()
        .expect("a cli_state")
        .to_owned();
    let bearer = requests
        .token_authorization
        .as_deref()
        .and_then(|value| value.strip_prefix("Bearer "))
        .expect("a bearer token exchange");
    let decoded = BASE64_URL_SAFE_NO_PAD.decode(bearer).expect("base64url");
    let code_verifier = String::from_utf8(decoded)
        .expect("UTF-8 credentials")
        .strip_prefix(&format!("{AUTH_CODE}:"))
        .expect("the code, then the verifier")
        .to_owned();
    for secret in [
        ACCESS_TOKEN,
        REFRESH_TOKEN,
        AUTH_CODE,
        FLOW_TOKEN,
        cli_state.as_str(),
        code_verifier.as_str(),
        bearer,
    ] {
        assert!(!stderr.contains(secret), "{secret:?} in stderr:\n{stderr}");
        assert!(!stdout.contains(secret), "{secret:?} in stdout:\n{stdout}");
    }

    let creds = credentials(home.path());
    assert!(
        creds.contains(&format!(r#"antithesis_token = "{ACCESS_TOKEN}""#)),
        "{creds}"
    );
}
