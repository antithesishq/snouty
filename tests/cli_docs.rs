mod support;

use predicates::prelude::*;
use support::*;
use tempfile::TempDir;

#[test]
fn docs_search_returns_results() {
    snouty_docs()
        .args(["docs", "--offline", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stdout(predicate::str::contains("Docker basics"));
}

#[test]
fn docs_env_db_path_implies_offline() {
    snouty_docs()
        .env("ANTITHESIS_DOCS_URL", "http://127.0.0.1:1")
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"))
        .stderr(predicate::str::contains("failed to update docs").not());
}

#[test]
fn docs_update_sets_custom_user_agent() {
    let cache_dir = TempDir::new().unwrap();
    let mock_server = MockDocsServer::start();

    set_docs_cache_env(&mut snouty(), &cache_dir)
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

    set_docs_cache_env(&mut snouty(), &cache_dir)
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    set_docs_cache_env(&mut snouty(), &cache_dir)
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

    set_docs_cache_env(&mut snouty(), &cache_dir)
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    set_docs_cache_env(&mut snouty(), &cache_dir)
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .success()
        .stdout(predicate::str::contains("/docs/guides/docker_basics/"));

    mock_server.set_etag("test-etag-2");

    set_docs_cache_env(&mut snouty(), &cache_dir)
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

    set_docs_cache_env(&mut snouty(), &cache_dir)
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

    set_docs_cache_env(&mut snouty(), &cache_dir)
        .env("ANTITHESIS_DOCS_URL", mock_server.url())
        .args(["docs", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "server did not include an ETag header in the response",
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
fn docs_search_no_query_fails() {
    snouty_docs()
        .args(["docs", "--offline", "search"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("search query required"));
}

#[test]
fn docs_search_no_results() {
    snouty_docs()
        .args(["docs", "--offline", "search", "xyznonexistent999"])
        .assert()
        .success()
        .stderr(predicate::str::contains("No results found"));
}

#[test]
fn docs_search_json_no_results_returns_empty_array() {
    snouty_docs()
        .args(["docs", "--offline", "search", "--json", "xyznonexistent999"])
        .assert()
        .success()
        .stdout("[]\n")
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_list_json_no_results_returns_empty_array() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "search",
            "--list",
            "--json",
            "xyznonexistent999",
        ])
        .assert()
        .success()
        .stdout("[]\n")
        .stderr(predicate::str::is_empty());
}

#[test]
fn docs_search_missing_db_with_offline_tells_user_to_remove_offline() {
    let cache_dir = TempDir::new().unwrap();

    set_docs_cache_env(&mut snouty(), &cache_dir)
        .args(["docs", "--offline", "search", "docker"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Documentation database not found. Remove --offline to download it.",
        ));
}

#[test]
fn docs_sqlite_missing_db_with_env_path_tells_user_to_fix_path() {
    snouty()
        .env("ANTITHESIS_DOCS_DB_PATH", "/tmp/does-not-exist-docs.db")
        .args(["docs", "sqlite"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Documentation database not found at /tmp/does-not-exist-docs.db. Point ANTITHESIS_DOCS_DB_PATH at an existing file.",
        ));
}

#[test]
fn docs_show_existing_page() {
    snouty_docs()
        .args(["docs", "--offline", "show", "getting_started"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"))
        .stdout(predicate::str::contains("Docker Compose"));
}

#[test]
fn docs_show_strips_leading_slash() {
    snouty_docs()
        .args(["docs", "--offline", "show", "/getting_started/"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_strips_docs_prefix() {
    snouty_docs()
        .args(["docs", "--offline", "show", "docs/getting_started"])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_accepts_full_antithesis_docs_url() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "show",
            "https://antithesis.com/docs/getting_started/",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_accepts_full_antithesis_docs_url_with_md_suffix() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "show",
            "https://antithesis.com/docs/getting_started.md",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("# Setup guide"));
}

#[test]
fn docs_show_missing_page_suggests() {
    snouty_docs()
        .args(["docs", "--offline", "show", "nonexistent_page"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "page not found: docs/nonexistent_page",
        ));
}

#[test]
fn docs_show_partial_match_suggests() {
    snouty_docs()
        .args(["docs", "--offline", "show", "sdk"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Did you mean"))
        .stderr(predicate::str::contains("/docs/sdk/python_sdk/"));
}

#[test]
fn docs_show_generated_page_hints_live_url() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "show",
            "/docs/generated/sdk/golang/assert/",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "generated pages (e.g. SDK references) are not included in the offline docs.",
        ))
        .stderr(predicate::str::contains(
            "If this is a valid page, try: https://antithesis.com/docs/generated/sdk/golang/assert/",
        ));
}

#[test]
fn docs_show_generated_page_from_full_url_hints_live_url() {
    snouty_docs()
        .args([
            "docs",
            "--offline",
            "show",
            "https://antithesis.com/docs/generated/sdk/golang/assert/",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "generated pages (e.g. SDK references) are not included in the offline docs.",
        ))
        .stderr(predicate::str::contains(
            "If this is a valid page, try: https://antithesis.com/docs/generated/sdk/golang/assert/",
        ));
}

#[test]
fn docs_show_generated_page_respects_custom_docs_url() {
    let mut cmd = snouty_docs();
    cmd.env("ANTITHESIS_DOCS_URL", "https://custom.example.com/docs");
    cmd.args([
        "docs",
        "--offline",
        "show",
        "generated/sdk/golang/assert",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains(
        "If this is a valid page, try: https://custom.example.com/docs/generated/sdk/golang/assert/",
    ));
}

#[test]
fn docs_show_generated_root_hints_live_url() {
    snouty_docs()
        .args(["docs", "--offline", "show", "/docs/generated/"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "generated pages (e.g. SDK references) are not included in the offline docs.",
        ))
        .stderr(predicate::str::contains(
            "If this is a valid page, try: https://antithesis.com/docs/generated/",
        ));
}

#[test]
fn docs_show_non_generated_missing_page_no_generated_hint() {
    snouty_docs()
        .args(["docs", "--offline", "show", "nonexistent_page"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Generated pages").not());
}

#[test]
fn docs_tree_omits_docs_root_and_shows_titles() {
    snouty_docs()
        .args(["docs", "--offline", "tree"])
        .assert()
        .success()
        .stdout(predicate::str::contains("docs\n").not())
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains("docker_basics - Docker basics\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("overview - Overview\n"))
        .stdout(predicate::str::contains("python_sdk - Python SDK\n"))
        .stdout(predicate::str::contains("┗"))
        .stdout(predicate::str::contains("━"));
}

#[test]
fn docs_tree_depth_limits_output() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "--depth", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("docker_basics - Docker basics").not())
        .stdout(predicate::str::contains("overview - Overview").not());
}

#[test]
fn docs_tree_depth_short_flag_limits_output() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "-d", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("guides\n"))
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("docker_basics - Docker basics").not())
        .stdout(predicate::str::contains("overview - Overview").not());
}

#[test]
fn docs_tree_filter_matches_paths_and_preserves_ancestors() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "overview"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "multiverse_debugging - Multiverse debugging\n",
        ))
        .stdout(predicate::str::contains("overview - Overview\n"))
        .stdout(predicate::str::contains("guides").not());
}

#[test]
fn docs_tree_filter_matches_titles_case_insensitively() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "setup GUIDE"])
        .assert()
        .success()
        .stdout(predicate::str::contains("getting_started - Setup guide\n"))
        .stdout(predicate::str::contains("docker_basics").not());
}

#[test]
fn docs_tree_no_results_prints_message() {
    snouty_docs()
        .args(["docs", "--offline", "tree", "no-such-doc-page"])
        .assert()
        .success()
        .stderr(predicate::str::contains("No results found"));
}

#[test]
fn docs_sqlite_prints_path() {
    snouty_docs()
        .args(["docs", "--offline", "sqlite"])
        .assert()
        .success()
        .stdout(predicate::str::contains(fixture_db()));
}

#[test]
fn docs_search_multi_word_query() {
    snouty_docs()
        .args(["docs", "--offline", "search", "fault", "injection"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "/docs/environment/fault_injection/",
        ))
        .stdout(predicate::str::contains("Fault injection"));
}

#[test]
fn docs_search_conversational_query_prefers_content_terms() {
    let results = docs_search_json(&["what", "is", "antithesis"]);

    let first_path = results[0].get("path").and_then(|v| v.as_str());
    assert_ne!(first_path, Some("/docs/faq/what_is_a_poc/"));
}
