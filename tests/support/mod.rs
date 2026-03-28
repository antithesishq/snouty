#![allow(dead_code)]

use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::TempDir;

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

pub(crate) fn snouty() -> Command {
    let mut cmd = cargo_bin_cmd!("snouty");
    cmd.env("RUST_LOG", "debug");
    for env_var in [
        "ANTITHESIS_API_KEY",
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
        "ANTITHESIS_TENANT",
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_REPOSITORY",
        "ANTITHESIS_DOCS_URL",
        "ANTITHESIS_DOCS_DB_PATH",
    ] {
        cmd.env_remove(env_var);
    }
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

pub(crate) fn expected_docs_user_agent() -> String {
    format!(
        "snouty/{} ({}; {}; rust{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

pub(crate) fn cached_docs_db_path(cache_dir: &TempDir) -> PathBuf {
    cache_dir.path().join("snouty").join("docs.db")
}

pub(crate) fn fixture_db() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/docs.db")
        .to_str()
        .unwrap()
        .to_string()
}

pub(crate) fn snouty_docs() -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_DOCS_DB_PATH", fixture_db());
    cmd
}

pub(crate) fn set_docs_cache_env<'a>(cmd: &'a mut Command, cache_dir: &TempDir) -> &'a mut Command {
    cmd.env("SNOUTY_TEST_CACHE_DIR", cache_dir.path());
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

pub(crate) fn snouty_with_mock(mock_url: &str) -> Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_USERNAME", "testuser")
        .env("ANTITHESIS_PASSWORD", "testpass")
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", mock_url);
    cmd
}
