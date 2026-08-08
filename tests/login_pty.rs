//! Drives `snouty login`'s interactive prompts end-to-end through a real
//! pseudo-terminal.
//!
//! The prompts only engage when stdin is a terminal, so each test allocates a
//! PTY, wires the child's stdio to the slave end, and scripts the dialogue
//! expect-style over the master end. The spec-test harness cannot host this:
//! testscript runs commands without a TTY, which sends `snouty login` down its
//! non-interactive path (covered by specs/login.txt).

#![cfg(unix)]

use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

/// How long to wait for any single expected string. Generous for CI; a healthy
/// exchange completes in milliseconds.
const EXPECT_TIMEOUT: Duration = Duration::from_secs(15);

/// The PTY's column count. The masked API-key echo is capped at the space left
/// on the prompt row, so the long-key test derives its expected star count
/// from this.
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

/// Allocate a PTY pair sized 24 x [`PTY_COLS`].
fn open_pty() -> (File, OwnedFd) {
    let mut master: libc::c_int = 0;
    let mut slave: libc::c_int = 0;
    let winsize = libc::winsize {
        ws_row: 24,
        ws_col: PTY_COLS,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &winsize,
        )
    };
    assert_eq!(rc, 0, "openpty failed: {}", std::io::Error::last_os_error());

    // Put the PTY in raw mode. Its default is canonical mode with echo, and
    // the prompt code only enters raw mode around each key read — bytes that
    // arrive between reads would be line-edited by the kernel (a 0x7f 'sent'
    // as backspace gets interpreted, not delivered) and echoed back to the
    // master, making the transcript racy. Raw from the start, the child reads
    // every byte exactly as the test sent it and the transcript contains only
    // what snouty itself wrote.
    unsafe {
        let mut termios: libc::termios = std::mem::zeroed();
        assert_eq!(libc::tcgetattr(slave, &mut termios), 0);
        libc::cfmakeraw(&mut termios);
        assert_eq!(libc::tcsetattr(slave, libc::TCSANOW, &termios), 0);
    }

    // SAFETY: openpty succeeded, so both fds are valid and owned by us.
    unsafe { (File::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) }
}

/// A `snouty login` child on the slave end of a PTY, scripted over the master.
struct PtySession {
    child: Child,
    master: File,
    chunks: Receiver<Vec<u8>>,
    transcript: String,
}

impl PtySession {
    /// Spawn `snouty login --tenant … --repository …` against an isolated
    /// `$HOME` and the given mock backend.
    fn spawn(home: &Path, base_url: &str) -> Self {
        let (master, slave) = open_pty();
        let stdout = slave.try_clone().expect("dup slave for stdout");
        let stderr = slave.try_clone().expect("dup slave for stderr");

        let child = Command::new(env!("CARGO_BIN_EXE_snouty"))
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
            .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
            .stdin(Stdio::from(slave))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn snouty login on the PTY");

        // Read the master on a thread: reads block, and the read fails with
        // EIO once the child exits and the slave closes — that ends the
        // thread and hangs up the channel.
        let mut reader = master.try_clone().expect("dup master for reading");
        let (tx, chunks) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });

        Self {
            child,
            master,
            chunks,
            transcript: String::new(),
        }
    }

    /// Wait until `needle` has appeared in the child's output.
    fn expect(&mut self, needle: &str) {
        let deadline = Instant::now() + EXPECT_TIMEOUT;
        while !self.transcript.contains(needle) {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            match self.chunks.recv_timeout(remaining) {
                // The output is ASCII, so chunk boundaries can't split a char.
                Ok(chunk) => self.transcript.push_str(&String::from_utf8_lossy(&chunk)),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => panic!(
                    "gave up waiting for {needle:?}; transcript so far:\n{}",
                    self.transcript
                ),
            }
        }
    }

    /// Type `input` at the terminal.
    fn send(&mut self, input: &str) {
        self.master
            .write_all(input.as_bytes())
            .expect("write to the PTY master");
        self.master.flush().expect("flush the PTY master");
    }

    /// Wait for the child to exit successfully, draining any remaining output.
    fn finish(mut self) -> String {
        let deadline = Instant::now() + EXPECT_TIMEOUT;
        loop {
            match self.child.try_wait().expect("wait for snouty login") {
                Some(status) => {
                    while let Ok(chunk) = self.chunks.try_recv() {
                        self.transcript.push_str(&String::from_utf8_lossy(&chunk));
                    }
                    assert!(status.success(), "login failed:\n{}", self.transcript);
                    return std::mem::take(&mut self.transcript);
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                None => panic!(
                    "login did not exit; transcript so far:\n{}",
                    self.transcript
                ),
            }
        }
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn credentials(home: &Path) -> String {
    std::fs::read_to_string(home.join(".config/snouty/credentials.toml")).unwrap_or_default()
}

const OAUTH_DISABLED: &str = r#"{"port_strategy":"disabled"}"#;

/// The first menu option is highlighted without any keypress, and Enter alone
/// selects it; the API key echoes one star per pasted character.
#[test]
fn menu_defaults_to_first_option_and_masks_the_api_key() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = PtySession::spawn(home.path(), &base_url);

    session.expect("What kind of credentials would you like to use?");
    session.expect("> API Key");
    session.send("\r");

    session.expect("Please enter your API Key: ");
    session.send("sk-pty-key-123");
    session.expect(&"*".repeat("sk-pty-key-123".len()));
    session.send("\r");

    session.expect("Run `snouty doctor` to verify your setup.");
    let transcript = session.finish();

    let creds = credentials(home.path());
    assert!(creds.contains(r#"api_key = "sk-pty-key-123""#), "{creds}");
    // Only stars reached the screen, never the key itself.
    assert!(
        !transcript.contains("sk-pty-key-123"),
        "the key must not be echoed:\n{transcript}"
    );
}

/// An API key longer than the terminal row is accepted whole: the echo stops
/// at the end of the row (a wrapped row cannot be un-echoed on erase), and
/// backspacing plus retyping still edits the real value.
#[test]
fn accepts_an_api_key_longer_than_the_terminal_width() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = PtySession::spawn(home.path(), &base_url);

    session.expect("> API Key");
    session.send("\r");

    let prompt = "Please enter your API Key: ";
    session.expect(prompt);
    let long_key = format!("sk-{}", "a".repeat(197));
    session.send(&long_key);
    // The echo budget is the rest of the row, minus one column so the cursor
    // never wraps.
    let budget = PTY_COLS as usize - prompt.len() - 1;
    session.expect(&"*".repeat(budget));
    // Erase the last four characters (all past the echo budget, so no stars
    // are cleared) and retype a distinctive tail.
    session.send("\u{7f}\u{7f}\u{7f}\u{7f}WXYZ\r");

    session.expect("Run `snouty doctor` to verify your setup.");
    let transcript = session.finish();

    let expected = format!("{}WXYZ", &long_key[..long_key.len() - 4]);
    let creds = credentials(home.path());
    assert!(
        creds.contains(&format!(r#"api_key = "{expected}""#)),
        "{creds}"
    );
    // Nothing was echoed past the row: exactly `budget` stars, ever.
    assert_eq!(
        transcript.matches('*').count(),
        budget,
        "stars must stop at the end of the row:\n{transcript}"
    );
    assert!(
        !transcript.contains("aaaa"),
        "the key must not be echoed:\n{transcript}"
    );
}

/// The username/password flow (last menu entry) confirms the password and
/// retries on a mismatch.
#[test]
fn password_confirmation_retries_on_mismatch() {
    let home = tempfile::TempDir::new().expect("temp HOME");
    let base_url = spawn_config_server(OAUTH_DISABLED);
    let mut session = PtySession::spawn(home.path(), &base_url);

    session.expect("Username & password (deprecated)");
    session.send("\x1b[B\r"); // arrow down to the last entry, select it

    session.expect("What username would you like to use?");
    session.send("pty-user\r");

    session.expect("Please enter your password: ");
    session.send("hunter2\r");
    session.expect("Please reenter your password to confirm: ");
    session.send("hunter3\r");
    session.expect("Passwords did not match");

    // The whole password prompt repeats after a mismatch; answer it
    // consistently this time.
    session.send("hunter2\rhunter2\r");

    session.expect("Run `snouty doctor` to verify your setup.");
    let transcript = session.finish();

    // The prompt was asked twice: once for the mismatched pair, once again
    // for the accepted one.
    assert_eq!(
        transcript.matches("Please enter your password: ").count(),
        2,
        "{transcript}"
    );
    assert!(
        !transcript.contains("hunter"),
        "passwords must not be echoed:\n{transcript}"
    );

    let creds = credentials(home.path());
    assert!(creds.contains(r#"username = "pty-user""#), "{creds}");
    assert!(creds.contains(r#"password = "hunter2""#), "{creds}");
}
