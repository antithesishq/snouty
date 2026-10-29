//! `snouty mcp`: an MCP server on the stdio transport. Each tool runs one
//! snouty command as a child process, in JSON mode, and returns what the
//! command prints. The tool schemas come from the commands' clap definitions.

use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{Result, WrapErr};
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
    Implementation, ListToolsResult, PaginatedRequestParams, ProgressNotificationParam,
    ProtocolVersion, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{RequestContext, ServerInitializeError, serve_server};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::Semaphore;

mod call;
mod tools;

pub use tools::ToolName;
pub(crate) use tools::{ArgRole, ParamKind, ValueType, summaries};

pub(crate) use call::command_args;
use tools::{TOOL_HELP, tool_help, tool_help_tool};

/// The server sets this to its own PID on each tool call's child. See
/// [`exit_with_server`].
const SERVER_PID_ENV: &str = "SNOUTY_MCP_SERVER_PID";

/// How often a tool call reports progress. Many clients end a request
/// after 60 seconds unless progress resets their time limit.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);

/// Context for the client. The tool list describes each tool, so this does
/// not.
const INSTRUCTIONS: &str = "Antithesis tests software in a deterministic simulation that injects \
faults. A run explores a tree of timelines, and an event, a property example or a failure names \
one moment of it: a run_id, an input_hash and a vtime. These tools read the runs of an \
Antithesis tenant, run commands at a moment of a run, and search the Antithesis documentation. \
tool_help returns the full help of a tool.";

/// The server's own options. Every tool call passes `settings`, `profile`
/// and `verbose` to its child. A client cannot set them.
pub struct Globals {
    pub settings: Option<PathBuf>,
    pub profile: Option<String>,
    pub verbose: bool,
    pub max_concurrent_calls: NonZeroUsize,
}

fn own_exe() -> Result<PathBuf> {
    // Unlike `current_exe`, /proc/self/exe opens the running file after an
    // update replaces or removes it.
    #[cfg(target_os = "linux")]
    {
        let proc = std::path::Path::new("/proc/self/exe");
        if proc.exists() {
            return Ok(proc.to_owned());
        }
    }
    std::env::current_exe().wrap_err("cannot find the snouty executable")
}

/// The command line as a shell would type it, for the debug log.
fn shell_line(argv: &[OsString]) -> String {
    let words: Vec<String> = std::iter::once("snouty".into())
        .chain(argv.iter().map(|arg| arg.to_string_lossy().into_owned()))
        .collect();
    shlex::try_join(words.iter().map(String::as_str)).unwrap_or_else(|_| words.join(" "))
}

#[derive(Clone)]
struct Snouty {
    globals: Arc<Globals>,
    exe: Arc<PathBuf>,
    running: Arc<Semaphore>,
}

impl Snouty {
    fn new(globals: Globals) -> Result<Self> {
        let running = Arc::new(Semaphore::new(globals.max_concurrent_calls.get()));
        Ok(Snouty {
            globals: Arc::new(globals),
            exe: Arc::new(own_exe()?),
            running,
        })
    }

    /// Runs `snouty <argv>` and returns what it prints: stdout, then stderr
    /// when there is any.
    async fn run(&self, argv: Vec<OsString>) -> Result<CallToolResponse, ErrorData> {
        log::debug!("running: {}", shell_line(&argv));
        let output = tokio::process::Command::new(self.exe.as_path())
            .arg0("snouty")
            .args(argv)
            .env(SERVER_PID_ENV, std::process::id().to_string())
            // The server's own debug log would flood the tool result.
            .env_remove("RUST_LOG")
            .stdin(Stdio::null())
            // A cancelled call drops the future, and with it the child.
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| ErrorData::internal_error(format!("cannot run snouty: {e}"), None))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim_end();
        let mut blocks = Vec::new();
        if !stdout.is_empty() || stderr.is_empty() {
            blocks.push(stdout.into_owned());
        }
        if !stderr.is_empty() {
            blocks.push(stderr.to_owned());
        }
        Ok(text_result(blocks, !output.status.success()))
    }
}

impl ServerHandler for Snouty {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("snouty", env!("SNOUTY_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        ctx: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        let tools = ToolName::ALL.iter().map(|name| name.spec().tool.clone());
        let result = ListToolsResult::with_all_items(tools.chain([tool_help_tool()]).collect());
        // Protocol 2026-07-28 requires cache hints on a list result, and
        // `with_all_items` leaves them out. These are the values that rmcp's
        // own `#[tool_handler]` list_tools sends.
        let cache_hints = ctx
            .protocol_version()
            .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28);
        std::future::ready(Ok(if cache_hints {
            result.with_ttl_ms(0).with_cache_scope(CacheScope::Public)
        } else {
            result
        }))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        if name == TOOL_HELP {
            return Some(tool_help_tool());
        }
        let name: ToolName = name.parse().ok()?;
        Some(name.spec().tool.clone())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name == TOOL_HELP {
            return Ok(match tool_help(request.arguments.unwrap_or_default()) {
                Ok(help) => text_result(vec![help], false),
                Err(e) => text_result(vec![format!("invalid params: {e}")], true),
            });
        }
        let Ok(name) = request.name.parse::<ToolName>() else {
            return Err(ErrorData::invalid_params(
                format!("unknown tool: {}", request.name),
                None,
            ));
        };
        let argv = match command_args(name, &self.globals, request.arguments.unwrap_or_default()) {
            Ok(argv) => argv,
            // The MCP spec reports a bad input as a tool error, so the model
            // can correct its call.
            Err(e) => return Ok(text_result(vec![format!("invalid params: {e}")], true)),
        };
        let call = async {
            let _slot = self
                .running
                .acquire()
                .await
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
            self.run(argv).await
        };
        tokio::pin!(call);
        let progress = ctx.meta.get_progress_token();
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + PROGRESS_INTERVAL,
            PROGRESS_INTERVAL,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut seconds = 0;
        // The client sets the time limit: it cancels the call, or closes
        // stdin. Either drops `call`, which kills the child.
        loop {
            tokio::select! {
                result = &mut call => return result,
                () = ctx.ct.cancelled() => return Err(ErrorData::internal_error("cancelled", None)),
                _ = ticks.tick(), if progress.is_some() => {
                    let Some(token) = &progress else { continue };
                    seconds += PROGRESS_INTERVAL.as_secs();
                    #[allow(clippy::cast_precision_loss)] // A call does not run for 2^52 seconds.
                    let param = ProgressNotificationParam::new(token.clone(), seconds as f64)
                        .with_message(format!("running for {seconds}s"));
                    // Await the send: the MCP spec forbids a notification
                    // after the response.
                    if let Err(e) = ctx.peer.notify_progress(param).await {
                        log::debug!("cannot send progress: {e}");
                    }
                }
            }
        }
    }
}

fn text_result(blocks: Vec<String>, is_error: bool) -> CallToolResponse {
    let content = blocks.into_iter().map(ContentBlock::text).collect();
    if is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    }
    .into()
}

/// Serves one client over stdin and stdout until stdin closes or a signal
/// arrives. stdout carries only MCP messages.
pub async fn cmd_mcp(globals: Globals) -> Result<()> {
    let mut stop = StopSignals::install()?;
    let running = tokio::select! {
        running = serve_server(Snouty::new(globals)?, rmcp::transport::stdio()) => running,
        () = stop.recv() => return Ok(()),
    };
    let running = match running {
        Ok(running) => running,
        // A client that closes stdin before `initialize` sent no requests.
        Err(ServerInitializeError::ConnectionClosed(_)) => return Ok(()),
        Err(e) => return Err(e).wrap_err("MCP initialization failed"),
    };
    // A signal cancels the service, and with it the `ctx.ct` of every tool
    // call in progress.
    let cancel = running.cancellation_token();
    let waiting = running.waiting();
    tokio::pin!(waiting);
    let quit = tokio::select! {
        quit = &mut waiting => quit,
        () = stop.recv() => {
            cancel.cancel();
            waiting.await
        }
    };
    quit.wrap_err("MCP server failed")?;
    Ok(())
}

/// In a tool call's child, exits when the server that started it is gone.
///
/// `kill_on_drop` kills the children of a server that stops in order. A
/// server that is killed (SIGKILL, a crash) cannot, and its children would
/// run on and use API quota. The parent of an orphan changes, so the child
/// checks its parent each second.
pub fn exit_with_server() {
    let Some(server) = std::env::var(SERVER_PID_ENV)
        .ok()
        .and_then(|pid| pid.parse::<u32>().ok())
    else {
        return;
    };
    let exit_if_orphaned = move || {
        if std::os::unix::process::parent_id() != server {
            std::process::exit(1);
        }
    };
    // The server can be gone before the child gets here.
    exit_if_orphaned();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(1));
            exit_if_orphaned();
        }
    });
}

/// Ctrl-C, SIGTERM and SIGHUP, which all stop the server. Install them before
/// the server can get requests: without a handler, a signal kills the process.
struct StopSignals {
    interrupt: Signal,
    terminate: Signal,
    hangup: Signal,
}

impl StopSignals {
    fn install() -> std::io::Result<Self> {
        Ok(StopSignals {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
            _ = self.hangup.recv() => {}
        }
    }
}

#[cfg(test)]
pub(crate) fn object(value: serde_json::Value) -> rmcp::model::JsonObject {
    match value {
        serde_json::Value::Object(object) => object,
        _ => panic!("params are an object"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_debug_log_line_reruns_the_command() {
        let argv = vec![
            OsString::from("--json"),
            "runs".into(),
            "--".into(),
            "a b".into(),
        ];
        assert_eq!(shell_line(&argv), "snouty --json runs -- 'a b'");
    }
}
