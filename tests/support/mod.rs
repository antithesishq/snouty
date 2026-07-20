#![allow(dead_code)]

use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use tempfile::{NamedTempFile, TempDir};

#[derive(Debug, Default)]
struct MockDocsServerState {
    current_etag: String,
    if_none_match_headers: Vec<Option<String>>,
    include_etag: bool,
    user_agent: Option<String>,
}

pub(crate) struct MockDocsServer {
    url: String,
    state: Arc<Mutex<MockDocsServerState>>,
}

impl MockDocsServer {
    pub(crate) fn start() -> Self {
        Self::start_with_etag("test-etag")
    }

    pub(crate) fn start_with_etag(initial_etag: &str) -> Self {
        Self::start_with_config(initial_etag, true)
    }

    pub(crate) fn start_without_etag() -> Self {
        Self::start_with_config("test-etag", false)
    }

    fn start_with_config(initial_etag: &str, include_etag: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockDocsServerState {
            current_etag: initial_etag.to_string(),
            if_none_match_headers: Vec::new(),
            include_etag,
            user_agent: None,
        }));
        let observed_state = Arc::clone(&state);
        let db = include_bytes!("../fixtures/docs.db");

        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buf = [0u8; 8192];
                let bytes_read = stream.read(&mut buf).unwrap();
                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                let user_agent = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("user-agent") {
                        Some(value.trim().to_owned())
                    } else {
                        None
                    }
                });
                let if_none_match = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("if-none-match") {
                        Some(value.trim().to_owned())
                    } else {
                        None
                    }
                });

                let (etag, include_etag, not_modified) = {
                    let mut state = observed_state.lock().unwrap();
                    state.user_agent = user_agent;
                    state.if_none_match_headers.push(if_none_match.clone());
                    let etag = state.current_etag.clone();
                    let include_etag = state.include_etag;
                    let not_modified = if_none_match.as_deref() == Some(etag.as_str());
                    (etag, include_etag, not_modified)
                };

                if not_modified {
                    stream
                        .write_all(b"HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\n\r\n")
                        .unwrap();
                    continue;
                }

                let response = if include_etag {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nETag: {}\r\n\r\n",
                        db.len(),
                        etag
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
                        db.len()
                    )
                };
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(db).unwrap();
            }
        });

        Self {
            url: format!("http://{}", addr),
            state,
        }
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn user_agent(&self) -> Option<String> {
        self.state.lock().unwrap().user_agent.clone()
    }

    pub(crate) fn if_none_match_headers(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().if_none_match_headers.clone()
    }

    pub(crate) fn set_etag(&self, etag: &str) {
        self.state.lock().unwrap().current_etag = etag.to_string();
    }
}

/// System environment variables forwarded to the snouty binary under test.
///
/// We start every command from an empty environment and add back only these,
/// rather than inheriting the parent env and blacklisting known config vars.
/// A whitelist can't drift: anything snouty reads as configuration
/// (`ANTITHESIS_*`, `SNOUTY_*`) — and the `HOME`/`XDG_*` vars that locate the
/// global settings file and caches — is withheld by construction, so no host
/// configuration can leak into a test. A test that needs a setting provides it
/// explicitly (see [`snouty_with_mock`], [`snouty_docs`], [`set_docs_cache_env`]).
///
/// `PATH` lets the binary find `podman`/`docker`; `TMPDIR`/`LLVM_PROFILE_FILE`
/// keep macOS temp handling and coverage instrumentation working.
///
/// DBus configuration is deliberately omitted from FORWARDED_ENV_VARS since
/// it represents a global state that might leak into or out of tests.
const FORWARDED_ENV_VARS: &[&str] = &["PATH", "TMPDIR", "LLVM_PROFILE_FILE"];

pub(crate) fn snouty() -> Command {
    let mut cmd = cargo_bin_cmd!("snouty");
    cmd.env_clear();
    for var in FORWARDED_ENV_VARS {
        if let Ok(value) = std::env::var(var) {
            cmd.env(var, value);
        }
    }
    cmd.env("RUST_LOG", "debug");
    // Disable keychain access -- this isn't something we can mock for each test case
    cmd.env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1");

    // Global settings can't leak (HOME/XDG_CONFIG_HOME aren't forwarded, so the
    // lookup resolves to nothing). Pin the project settings file to an empty
    // but existing file so a ./.snouty.toml in the working tree can't leak in
    // either — empty so it contributes no values, existing so the explicit-path
    // resolution doesn't error. A test wanting project settings overrides it.
    let empty_settings =
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("empty.snouty.toml");
    let _ = std::fs::write(&empty_settings, "");
    cmd.env("SNOUTY_SETTINGS_PATH", &empty_settings);
    cmd
}

pub(crate) fn start_mock_server(response_body: &'static str, status: u16) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    thread::spawn(move || {
        if let Some(mut stream) = listener.incoming().flatten().next() {
            let mut buf = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut buf);

            let response = format!(
                "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                status,
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });

    url
}

/// The docs DB the binary resolves under a given cache home: `<dir>/snouty/docs.db`
/// (snouty appends `snouty` to `$XDG_CACHE_HOME`).
pub(crate) fn cached_docs_db_path(cache_home: &TempDir) -> PathBuf {
    cache_home.path().join("snouty").join("docs.db")
}

pub(crate) fn fixture_db() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/docs.db")
        .to_str()
        .unwrap()
        .to_string()
}

/// A shared, read-only cache home seeded with the fixture docs database, exposed
/// to the binary via `XDG_CACHE_HOME`, so the parallel `--offline` docs tests can
/// share it.
///
/// The `OnceLock` only dedupes within a process, but `cargo nextest` runs every
/// test in its own process, so many processes seed the same fixed path
/// concurrently. The seeding must therefore be safe under concurrent writers:
/// we copy into a process-unique temp file and atomically rename it into place,
/// so a reader always observes a complete database, never a half-written one.
/// Every writer produces byte-identical content, so the racing renames are
/// harmless.
fn docs_fixture_cache_home() -> &'static Path {
    static CACHE_HOME: OnceLock<PathBuf> = OnceLock::new();
    CACHE_HOME
        .get_or_init(|| {
            let home = Path::new(env!("CARGO_TARGET_TMPDIR")).join("docs-fixture-cache");
            let snouty_dir = home.join("snouty");
            std::fs::create_dir_all(&snouty_dir).unwrap();

            let tmp = NamedTempFile::new_in(&snouty_dir).unwrap();
            std::fs::copy(fixture_db(), tmp.path()).unwrap();
            // Atomic rename: replaces the destination in one step, so concurrent
            // readers never see a truncated or partially-copied database.
            tmp.persist(snouty_dir.join("docs.db")).unwrap();

            home
        })
        .as_path()
}

/// The docs DB path inside [`docs_fixture_cache_home`], for asserting on
/// `docs sqlite` output.
pub(crate) fn docs_fixture_db_path() -> PathBuf {
    docs_fixture_cache_home().join("snouty").join("docs.db")
}

pub(crate) fn snouty_docs() -> Command {
    let mut cmd = snouty();
    cmd.env("XDG_CACHE_HOME", docs_fixture_cache_home());
    cmd
}

pub(crate) fn set_docs_cache_env<'a>(
    cmd: &'a mut Command,
    cache_home: &TempDir,
) -> &'a mut Command {
    cmd.env("XDG_CACHE_HOME", cache_home.path());
    cmd
}

pub(crate) fn docs_search_json(query_args: &[&str]) -> Vec<serde_json::Value> {
    let mut args = vec!["docs", "--offline", "search", "--json"];
    args.extend_from_slice(query_args);

    let output = snouty_docs()
        .args(&args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    serde_json::from_slice::<serde_json::Value>(&output)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

pub(crate) struct MockServer {
    pub url: String,
    pub token: String,
}

pub(crate) fn start_runs_server(empty: bool) -> MockServer {
    let server = if empty {
        snouty::testutils::MockApiServer::start_empty()
    } else {
        snouty::testutils::MockApiServer::start()
    };
    let mock = MockServer {
        url: server.url().to_string(),
        token: server.token().to_string(),
    };
    std::mem::forget(server);
    mock
}

pub(crate) fn snouty_with_mock(mock_url: &str) -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_USERNAME", "testuser")
        .env("ANTITHESIS_PASSWORD", "testpass")
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", mock_url);
    cmd
}

pub(crate) fn snouty_with_mock_server(mock: &MockServer) -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_API_KEY", &mock.token)
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", &mock.url);
    cmd
}
