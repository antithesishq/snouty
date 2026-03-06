use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
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

struct MockDocsServer {
    url: String,
    state: Arc<Mutex<MockDocsServerState>>,
}

impl MockDocsServer {
    fn start() -> Self {
        Self::start_with_etag("test-etag")
    }

    fn start_with_etag(initial_etag: &str) -> Self {
        Self::start_with_config(initial_etag, true)
    }

    fn start_without_etag() -> Self {
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
        let db = include_bytes!("fixtures/docs.db");

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

    fn url(&self) -> &str {
        &self.url
    }

    fn user_agent(&self) -> Option<String> {
        self.state.lock().unwrap().user_agent.clone()
    }

    fn if_none_match_headers(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().if_none_match_headers.clone()
    }

    fn set_etag(&self, etag: &str) {
        self.state.lock().unwrap().current_etag = etag.to_string();
    }
}

fn snouty() -> assert_cmd::Command {
    let mut cmd = cargo_bin_cmd!("snouty");
    cmd.env("RUST_LOG", "debug");
    // Clear all ANTITHESIS_* inputs the CLI reads so tests don't depend on
    // env leaked in from the caller or CI runner.
    for env_var in [
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

fn expected_docs_user_agent() -> String {
    format!(
        "snouty/{} ({}; {}; rust{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

fn cached_docs_db_path(cache_dir: &TempDir) -> std::path::PathBuf {
    cache_dir.path().join("snouty").join("docs.db")
}

fn fixture_db() -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/docs.db")
        .to_str()
        .unwrap()
        .to_string()
}

fn snouty_docs() -> assert_cmd::Command {
    let mut cmd = snouty();
    cmd.env("ANTITHESIS_DOCS_DB_PATH", fixture_db());
    cmd
}

fn docs_search_json(query_args: &[&str]) -> Vec<serde_json::Value> {
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

// === Tests for `docs` commands (kept: ETag, User-Agent, JSON structure, result ordering) ===

#[test]
fn docs_update_sets_custom_user_agent() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    assert_eq!(
        mock_server.user_agent().as_deref(),
        Some(expected_docs_user_agent().as_str()),
    );
}

#[test]
fn docs_update_failure_with_cached_db_warns_and_uses_cache() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", "http://127.0.0.1:1")
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stderr(predicate::str::contains(
            "Warning: failed to update docs, falling back to cached docs",
        ));
}

#[test]
fn docs_auto_update_reuses_cached_db_until_etag_changes() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start_with_etag("test-etag-1");

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    mock_server.set_etag("test-etag-2");

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    assert_eq!(
        mock_server.if_none_match_headers(),
        vec![
            None,
            Some("test-etag-1".to_string()),
            Some("test-etag-1".to_string())
        ]
    );

    let etag_path = cache_dir.path().join("snouty").join("docs.db.etag");
    assert_eq!(std::fs::read_to_string(etag_path).unwrap(), "test-etag-2");
}

#[test]
fn docs_downloaded_db_is_read_only() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success();

    let metadata = std::fs::metadata(cached_docs_db_path(&cache_dir)).unwrap();
    assert!(metadata.permissions().readonly());
}

#[test]
fn docs_update_requires_etag_header() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start_without_etag();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "server did not include an ETag header in the response",
        ));
}

#[test]
fn docs_search_missing_db_with_offline_tells_user_to_remove_offline() {
    let cache_dir = TempDir::new().unwrap();

    snouty()
        .env("XDG_CACHE_HOME", cache_dir.path())
        .args(["docs", "--offline", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Documentation database not found. Remove --offline to download it.",
        ));
}

#[test]
fn docs_search_json_flag() {
    let results = docs_search_json(&["sdk"]);
    assert!(!results.is_empty());

    let sdk_entry = results
        .iter()
        .find(|entry| entry.get("path").and_then(|v| v.as_str()) == Some("/docs/sdk/python_sdk/"));
    let sdk_entry = sdk_entry.expect("expected sdk result in JSON output");

    assert_eq!(
        sdk_entry.get("title").and_then(|v| v.as_str()),
        Some("Python SDK")
    );
    assert!(
        sdk_entry
            .get("snippet")
            .and_then(|v| v.as_str())
            .is_some_and(|snippet| snippet.contains("sdk-related result"))
    );
}

#[test]
fn docs_search_respects_limit() {
    let full_results = docs_search_json(&["test"]);
    assert!(full_results.len() > 2);

    let limited_output = snouty_docs()
        .args(["docs", "--offline", "search", "--json", "-n", "2", "test"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let limited_results = serde_json::from_slice::<serde_json::Value>(&limited_output)
        .unwrap()
        .as_array()
        .unwrap()
        .clone();

    assert_eq!(limited_results.len(), 2);
    assert_eq!(limited_results, full_results[..2]);
}

#[test]
fn docs_search_list_json_outputs_path_array() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "search",
            "--list",
            "--json",
            "-n",
            "2",
            "test",
        ])
        .assert()
        .success()
        .stdout(concat!(
            "[\n",
            "  \"/docs/reference/test_patterns/\",\n",
            "  \"/docs/environment/fault_injection/\"\n",
            "]\n",
        ))
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_conversational_query_prefers_content_terms() {
    let results = docs_search_json(&["what", "is", "antithesis"]);

    let first_path = results[0].get("path").and_then(|v| v.as_str());
    assert_ne!(first_path, Some("/docs/faq/what_is_a_poc/"));
}
