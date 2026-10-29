//! Drives `snouty mcp` with rmcp's own MCP client, over stdio, against the
//! mock API.

#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use rmcp::ClientHandler;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResult, JsonObject, ProgressNotificationParam,
    ProtocolVersion,
};
use rmcp::service::{
    ClientLifecycleMode, ClientServiceExt, NotificationContext, RoleClient, RunningService,
    ServiceExt,
};
use rmcp::transport::TokioChildProcess;
use serde_json::{Value, json};
use snouty::testutils::MockApiServer;
use tempfile::TempDir;

/// `snouty mcp` with its feature on, the mock API, and the fixture docs
/// database in an isolated cache.
fn snouty_mcp(server: &MockApiServer, home: &Path) -> tokio::process::Command {
    snouty_mcp_from(Path::new(env!("CARGO_BIN_EXE_snouty")), server, home)
}

/// [`snouty_mcp`], from the snouty executable at `exe`.
fn snouty_mcp_from(exe: &Path, server: &MockApiServer, home: &Path) -> tokio::process::Command {
    let cache = home.join("cache").join("snouty");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/docs.db"),
        cache.join("docs.db"),
    )
    .unwrap();
    let mut command = tokio::process::Command::new(exe);
    command
        .arg("mcp")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("XDG_CACHE_HOME", home.join("cache"))
        .env("SNOUTY_UNSTABLE_FEATURES", "mcp")
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .env("ANTITHESIS_TENANT", "testtenant")
        .env("ANTITHESIS_BASE_URL", server.url())
        .env("ANTITHESIS_API_KEY", server.token());
    command
}

/// [`snouty_mcp`], with an API that accepts connections and never answers.
/// The listener must outlive the command.
fn snouty_mcp_silent(
    server: &MockApiServer,
    home: &Path,
) -> (tokio::process::Command, std::net::TcpListener) {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut command = snouty_mcp(server, home);
    command.env(
        "ANTITHESIS_BASE_URL",
        format!("http://{}", silent.local_addr().unwrap()),
    );
    (command, silent)
}

async fn connect(command: tokio::process::Command) -> RunningService<RoleClient, ()> {
    let transport = TokioChildProcess::new(command).expect("spawn snouty mcp");
    ().serve(transport).await.expect("initialize snouty mcp")
}

async fn call(client: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> CallToolResult {
    let Value::Object(args) = args else {
        panic!("args are an object");
    };
    let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(JsonObject::from(args));
    client.call_tool(params).await.expect("tools/call")
}

/// The one text block of a result.
fn text(result: &CallToolResult) -> &str {
    let [block] = result.content.as_slice() else {
        panic!("not one content block: {result:?}");
    };
    &block.as_text().expect("a text block").text
}

#[tokio::test]
async fn tools_run_their_commands_in_json_mode() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let client = connect(snouty_mcp(&server, home.path())).await;

    let tools = client.list_all_tools().await.expect("tools/list");
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "runs_list",
            "runs_show",
            "runs_wait",
            "runs_properties",
            "runs_property",
            "runs_build_logs",
            "runs_logs",
            "runs_search",
            "runs_usage",
            "runs_exec",
            "debug",
            "docs_tree",
            "docs_show",
            "docs_search",
            "doctor",
            "settings_show",
            "tool_help",
        ]
    );

    // runs_list returns what `snouty --json runs list` prints: one run per
    // line.
    let result = call(&client, "runs_list", json!({ "limit": 2 })).await;
    assert_eq!(result.is_error, Some(false));
    let runs: Vec<Value> = text(&result)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0]["run_id"], "run-1");

    // settings_show returns the resolved settings, which doctor no longer
    // reports.
    let result = call(&client, "settings_show", json!({})).await;
    assert_eq!(result.is_error, Some(false), "{}", text(&result));
    let shown: Value = serde_json::from_str(text(&result)).unwrap();
    assert!(shown["settings"]["base_url"].is_string(), "{shown}");

    // A command that fails marks the result isError, with its error text.
    let result = call(&client, "runs_show", json!({ "run_id": "no-such-run" })).await;
    assert_eq!(result.is_error, Some(true));
    assert!(text(&result).contains("no-such-run"), "{}", text(&result));

    // A bad param fails before any command runs.
    let result = call(
        &client,
        "runs_show",
        json!({ "run_id": "run-1", "web": true }),
    )
    .await;
    assert_eq!(result.is_error, Some(true));
    assert_eq!(text(&result), "invalid params: unknown param: web");

    // runs_property reads one property of a completed run, and runs_usage the
    // usage hours of runs.
    let result = call(
        &client,
        "runs_list",
        json!({ "status": "completed", "limit": 1 }),
    )
    .await;
    let run: Value = serde_json::from_str(text(&result).trim()).unwrap();
    let call_args = json!({
        "run_id": run["run_id"],
        "property_id": "97cf5fcafa500ed62bf109d81834c5b881a2c854",
    });
    let result = call(&client, "runs_property", call_args).await;
    assert_eq!(result.is_error, Some(false), "{}", text(&result));
    let property: Value = serde_json::from_str(text(&result)).unwrap();
    assert_eq!(property["id"], "97cf5fcafa500ed62bf109d81834c5b881a2c854");
    let result = call(&client, "runs_usage", json!({ "limit": 1, "utc": true })).await;
    assert_eq!(result.is_error, Some(false), "{}", text(&result));
    let usage: Value = serde_json::from_str(text(&result).trim()).unwrap();
    assert!(usage["hours"].is_number(), "{usage}");

    // tool_help returns a tool's help page without running a command.
    let result = call(&client, "tool_help", json!({ "tool": "runs_logs" })).await;
    assert_eq!(result.is_error, Some(false));
    assert!(text(&result).contains("`begin_vtime`"), "{}", text(&result));

    // A docs tool reads the cached database.
    let result = call(
        &client,
        "docs_search",
        json!({ "query": ["docker"], "offline": true }),
    )
    .await;
    assert_eq!(result.is_error, Some(false), "{}", text(&result));
    let matches: Value = serde_json::from_str(text(&result)).unwrap();
    assert!(
        matches.as_array().is_some_and(|m| !m.is_empty()),
        "{matches}"
    );

    client.cancel().await.expect("close snouty mcp");
}

/// `snouty --help` lists `mcp` only while its feature is on.
#[tokio::test]
async fn mcp_is_listed_only_with_its_feature_on() {
    for (features, listed) in [("mcp", true), ("", false)] {
        let mut root = tokio::process::Command::new(env!("CARGO_BIN_EXE_snouty"));
        root.arg("--help")
            .env_clear()
            .env("SNOUTY_UNSTABLE_FEATURES", features);
        let output = root.output().await.expect("run snouty --help");
        let help = String::from_utf8_lossy(&output.stdout);
        let has_mcp = help
            .lines()
            .any(|line| line.trim_start().starts_with("mcp "));
        assert_eq!(
            has_mcp, listed,
            "SNOUTY_UNSTABLE_FEATURES={features}\n{help}"
        );
    }
}

#[tokio::test]
async fn tools_list_carries_cache_hints() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    // The cache hints are part of protocol 2026-07-28, which a client reaches
    // through server/discover.
    let transport = TokioChildProcess::new(snouty_mcp(&server, home.path())).unwrap();
    let lifecycle = ClientLifecycleMode::Discover {
        preferred_versions: vec![ProtocolVersion::V_2026_07_28],
    };
    let client = ().serve_with_lifecycle(transport, lifecycle).await.unwrap();
    let result = client.list_tools(None).await.expect("tools/list");
    assert_eq!(result.ttl_ms, Some(0));
    assert_eq!(result.cache_scope, Some(CacheScope::Public));
    client.cancel().await.expect("close snouty mcp");
}

/// SIGTERM stops the server while the client still holds its stdin open.
#[tokio::test]
async fn a_signal_stops_the_server() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let mut child = snouty_mcp(&server, home.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let initialize = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": { "name": "test", "version": "1" },
        },
    });
    stdin
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut response = String::new();
    stdout.read_line(&mut response).await.unwrap();
    assert!(response.contains("\"result\""), "{response}");

    let status = std::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("snouty mcp exits on SIGTERM")
        .unwrap();
    assert!(status.success(), "{status}");
    drop(stdin);
}

/// The live children of `pid`, from /proc.
#[cfg(target_os = "linux")]
fn children(pid: u32) -> Vec<u32> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(child) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{child}/stat")) else {
            continue;
        };
        let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
        if fields[1] == pid.to_string() && fields[0] != "Z" {
            found.push(child);
        }
    }
    found
}

/// A tool call runs its command with stdin null, and a cancelled call kills
/// the command.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_cancelled_call_kills_its_command() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let (command, _silent) = snouty_mcp_silent(&server, home.path());
    let transport = TokioChildProcess::new(command).expect("spawn snouty mcp");
    let pid = transport.id().unwrap();
    let client = ().serve(transport).await.expect("initialize snouty mcp");

    let mut call = Box::pin(call(&client, "runs_show", json!({ "run_id": "r" })));
    let child = loop {
        tokio::select! {
            result = &mut call => panic!("the call returned: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        if let [child] = children(pid)[..] {
            break child;
        }
    };
    let stdin = std::fs::read_link(format!("/proc/{child}/fd/0")).unwrap();
    assert_eq!(stdin, Path::new("/dev/null"));

    drop(call);
    let mut killed = false;
    for _ in 0..100 {
        if children(pid).is_empty() {
            killed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client.cancel().await.expect("close snouty mcp");
    assert!(killed, "the command outlived its cancelled call");
}

struct ProgressClient(tokio::sync::mpsc::UnboundedSender<ProgressNotificationParam>);

impl ClientHandler for ProgressClient {
    async fn on_progress(
        &self,
        params: ProgressNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        let _ = self.0.send(params);
    }
}

/// A long tool call sends its first progress notification after 10 seconds.
#[tokio::test]
async fn a_long_call_reports_progress() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let (command, _silent) = snouty_mcp_silent(&server, home.path());
    let (sender, mut progress) = tokio::sync::mpsc::unbounded_channel();
    let transport = TokioChildProcess::new(command).expect("spawn snouty mcp");
    let client = ProgressClient(sender)
        .serve(transport)
        .await
        .expect("initialize snouty mcp");

    // rmcp's client puts a progress token on each request.
    let params = CallToolRequestParams::new("runs_show")
        .with_arguments(JsonObject::from_iter([("run_id".to_owned(), json!("r"))]));
    let first = tokio::select! {
        result = client.call_tool_once(params) => panic!("the call returned: {result:?}"),
        first = tokio::time::timeout(Duration::from_secs(20), progress.recv()) => first,
    };
    let first = first
        .expect("progress within 20s")
        .expect("progress channel open");
    assert_eq!(first.progress, 10.0);
    assert_eq!(first.message.as_deref(), Some("running for 10s"));
    client.cancel().await.expect("close snouty mcp");
}

/// A command exits when its server is killed, which leaves no time to kill
/// the command.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_command_exits_when_its_server_is_killed() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let (command, _silent) = snouty_mcp_silent(&server, home.path());
    let transport = TokioChildProcess::new(command).expect("spawn snouty mcp");
    let pid = transport.id().unwrap();
    let client = ().serve(transport).await.expect("initialize snouty mcp");

    let mut call = Box::pin(call(&client, "runs_show", json!({ "run_id": "r" })));
    let child = loop {
        tokio::select! {
            result = &mut call => panic!("the call returned: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        if let [child] = children(pid)[..] {
            break child;
        }
    };
    let status = std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    let mut exited = false;
    for _ in 0..100 {
        let alive = std::fs::read_to_string(format!("/proc/{child}/stat"))
            .is_ok_and(|stat| !stat.contains(") Z "));
        if !alive {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(call);
    drop(client);
    assert!(exited, "the command outlived its killed server");
}

/// The tool calls still work after an update replaces the server's
/// executable.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn tools_work_after_the_executable_is_replaced() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    // In the target dir, so that a hard link to the binary works.
    let bin = TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let exe = bin.path().join("snouty");
    std::fs::hard_link(env!("CARGO_BIN_EXE_snouty"), &exe).unwrap();
    let command = snouty_mcp_from(&exe, &server, home.path());
    let client = connect(command).await;

    let update = bin.path().join("update");
    std::fs::write(&update, "#!/bin/sh\necho replaced\n").unwrap();
    std::fs::rename(&update, &exe).unwrap();

    let result = call(&client, "docs_tree", json!({ "offline": true, "depth": 1 })).await;
    assert_eq!(result.is_error, Some(false), "{}", text(&result));
    assert_ne!(text(&result).trim(), "replaced");
    client.cancel().await.expect("close snouty mcp");
}

/// The server's `RUST_LOG` stays out of the tool calls, so a debug log does not
/// reach a tool result.
#[tokio::test]
async fn the_server_debug_log_stays_out_of_tool_results() {
    let server = MockApiServer::start();
    let home = TempDir::new().unwrap();
    let mut command = snouty_mcp(&server, home.path());
    command.env("RUST_LOG", "debug");
    let client = connect(command).await;
    let result = call(&client, "runs_list", json!({ "limit": 1 })).await;
    assert_eq!(result.is_error, Some(false), "{result:?}");
    // One block: stdout, and no stderr.
    text(&result);
    client.cancel().await.expect("close snouty mcp");
}
