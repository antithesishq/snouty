//! `snouty mcp`: an MCP server on the stdio transport. Each tool runs one
//! snouty command as a child process, in JSON mode, and returns what the
//! command prints. The tool schemas come from the commands' clap definitions.

use std::any::TypeId;
use std::ffi::OsString;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use clap::{Arg, CommandFactory};
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
    Implementation, JsonObject, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ServerCapabilities, ServerConfig, Tool, ToolAnnotations,
};
use rmcp::service::{RequestContext, ServerInitializeError, serve_server};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::{Value, json};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::Semaphore;

use crate::cli::Cli;
use crate::vtime::VTime;

/// The server sets this to its own PID on each tool call's child. See
/// [`exit_with_server`].
const SERVER_PID_ENV: &str = "SNOUTY_MCP_SERVER_PID";

const INSTRUCTIONS: &str = "Find a run with runs_list, then read it with runs_show and \
runs_properties, or wait for it with runs_wait. A property example or a run failure names a \
moment (run_id, input_hash and vtime). runs_logs returns the events of a moment's timeline, \
runs_search queries the events of a run, runs_exec runs a bash script at a moment of a run that \
is in progress, and debug starts a debugging session at a moment. docs_search, docs_tree and \
docs_show read the Antithesis documentation, and doctor checks the local setup.";

/// The server's own options. Every tool call passes `settings`, `profile`
/// and `verbose` to its child. A client cannot set them.
pub struct Globals {
    pub settings: Option<PathBuf>,
    pub profile: Option<String>,
    pub verbose: bool,
    pub max_concurrent_calls: NonZeroUsize,
}

/// What a tool's command can do, which sets the tool's annotations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Effect {
    /// Reads from the API, the docs or the local setup. `runs exec` is here
    /// too: its script runs in a branch of the run, which it cannot change.
    Reads,
    /// Starts an Antithesis session, which uses the tenant's resources.
    StartsSessions,
}

impl Effect {
    fn annotations(self) -> ToolAnnotations {
        match self {
            Effect::Reads => ToolAnnotations::new().read_only(true),
            Effect::StartsSessions => ToolAnnotations::new()
                .read_only(false)
                .destructive(false)
                .open_world(true),
        }
    }
}

/// One row per tool: the variant, the tool name, the path of its snouty
/// command, its effect, the args that the tool leaves out, and the args that
/// the tool requires although the command does not.
macro_rules! tools {
    ($(
        $variant:ident => $name:literal, [$($part:literal),+], $effect:ident,
        hidden: [$($hidden:literal),*], required: [$($required:literal),*];
    )*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum ToolName {
            $($variant,)*
        }

        impl ToolName {
            pub const ALL: [ToolName; [$($name),*].len()] = [$(ToolName::$variant,)*];

            /// The server runs only these commands.
            fn path(self) -> &'static [&'static str] {
                match self {
                    $(ToolName::$variant => &[$($part),+],)*
                }
            }

            fn effect(self) -> Effect {
                match self {
                    $(ToolName::$variant => Effect::$effect,)*
                }
            }

            /// Args that open a browser, stream with no end, or print human
            /// output only.
            fn hidden(self) -> &'static [&'static str] {
                match self {
                    $(ToolName::$variant => &[$($hidden),*],)*
                }
            }

            fn required(self) -> &'static [&'static str] {
                match self {
                    $(ToolName::$variant => &[$($required),*],)*
                }
            }
        }

        impl fmt::Display for ToolName {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self {
                    $(ToolName::$variant => $name,)*
                })
            }
        }
    };
}

tools! {
    RunsList => "runs_list", ["runs", "list"], Reads, hidden: ["detail"], required: [];
    RunsShow => "runs_show", ["runs", "show"], Reads, hidden: ["web"], required: [];
    RunsWait => "runs_wait", ["runs", "wait"], Reads, hidden: [], required: [];
    RunsProperties => "runs_properties", ["runs", "properties"], Reads,
        hidden: ["detail"], required: [];
    RunsBuildLogs => "runs_build_logs", ["runs", "build-logs"], Reads, hidden: [], required: [];
    RunsLogs => "runs_logs", ["runs", "logs"], Reads, hidden: ["detail"], required: [];
    RunsSearch => "runs_search", ["runs", "search"], Reads,
        hidden: ["follow", "detail"], required: [];
    // With no script, `runs exec` reads it from stdin, and a child's stdin is
    // null.
    RunsExec => "runs_exec", ["runs", "exec"], Reads, hidden: [], required: ["script"];
    Debug => "debug", ["debug"], StartsSessions, hidden: [], required: [];
    DocsTree => "docs_tree", ["docs", "tree"], Reads, hidden: [], required: [];
    DocsShow => "docs_show", ["docs", "show"], Reads, hidden: [], required: [];
    DocsSearch => "docs_search", ["docs", "search"], Reads, hidden: [], required: [];
    Doctor => "doctor", ["doctor"], Reads, hidden: [], required: [];
}

struct ToolSpec {
    name: ToolName,
    args: Vec<Arg>,
    tool: Tool,
}

impl ToolSpec {
    /// `root_globals` are the ids of the root command's args (--json,
    /// --settings, …). A built subcommand carries them, and they are the
    /// server's to set.
    fn new(name: ToolName, cli: &clap::Command, root_globals: &[String]) -> Self {
        let command = name
            .path()
            .iter()
            .try_fold(cli, |command, part| command.find_subcommand(part))
            .unwrap_or_else(|| panic!("{name}: no command at {:?}", name.path()));
        let args: Vec<Arg> = command
            .get_arguments()
            .filter(|arg| {
                let id = arg.get_id().as_str();
                id != "help"
                    && !arg.is_hide_set()
                    && !root_globals.iter().any(|global| global == id)
                    && !name.hidden().contains(&id)
            })
            .cloned()
            .collect();
        let about = command
            .get_about()
            .map(ToString::to_string)
            .unwrap_or_default();
        let description = format!(
            "{about}. Returns what `snouty --json {}` prints.",
            name.path().join(" ")
        );
        let tool = Tool::new(name.to_string(), description, input_schema(name, &args))
            .with_annotations(name.effect().annotations());
        ToolSpec { name, args, tool }
    }

    fn is_required(&self, arg: &Arg) -> bool {
        arg.is_required_set() || self.name.required().contains(&arg.get_id().as_str())
    }
}

fn is_flag(arg: &Arg) -> bool {
    !arg.get_action().takes_values()
}

fn takes_many(arg: &Arg) -> bool {
    matches!(arg.get_action(), clap::ArgAction::Append)
        || arg
            .get_num_args()
            .is_some_and(|range| range.max_values() > 1)
}

fn value_type(arg: &Arg) -> &'static str {
    let parsed = arg.get_value_parser().type_id();
    let integers = [
        TypeId::of::<usize>(),
        TypeId::of::<u64>(),
        TypeId::of::<NonZeroUsize>(),
        TypeId::of::<NonZeroU64>(),
    ];
    if integers.into_iter().any(|integer| parsed == integer) {
        "integer"
    } else if parsed == TypeId::of::<VTime>() {
        "number"
    } else {
        "string"
    }
}

fn input_schema(name: ToolName, args: &[Arg]) -> Arc<JsonObject> {
    let mut properties = JsonObject::new();
    let mut required = Vec::new();
    for arg in args {
        let id = arg.get_id().as_str();
        let mut schema = if is_flag(arg) {
            json!({ "type": "boolean" })
        } else {
            let values: Vec<String> = arg
                .get_possible_values()
                .iter()
                .filter(|value| !value.is_hide_set())
                .map(|value| value.get_name().to_owned())
                .collect();
            let item = if values.is_empty() {
                json!({ "type": value_type(arg) })
            } else {
                json!({ "type": "string", "enum": values })
            };
            if takes_many(arg) {
                json!({ "type": "array", "items": item })
            } else {
                item
            }
        };
        if let Some(help) = arg.get_long_help().or(arg.get_help()) {
            schema["description"] = Value::String(help.to_string());
        }
        if !is_flag(arg)
            && let [default, ..] = arg.get_default_values()
        {
            let default = default.to_string_lossy();
            schema["default"] = match value_type(arg) {
                "string" => Value::String(default.into_owned()),
                _ => serde_json::from_str(&default)
                    .unwrap_or_else(|_| panic!("{name}: the default of {id} is not a number")),
            };
        }
        if arg.is_required_set() || name.required().contains(&id) {
            required.push(Value::String(id.to_owned()));
        }
        properties.insert(id.to_owned(), schema);
    }
    let mut schema = JsonObject::new();
    schema.insert("type".to_owned(), "object".into());
    schema.insert("properties".to_owned(), properties.into());
    schema.insert("required".to_owned(), required.into());
    schema.insert("additionalProperties".to_owned(), false.into());
    Arc::new(schema)
}

/// The command line of a tool call, after `snouty`.
///
/// The child runs a command from the fixed tool table, never one that the
/// request names. Each param must be an exposed arg of that command. An option
/// goes in as one `--long=value` token and the positionals go after `--`, so a
/// value never becomes a flag or a subcommand.
fn command_args(spec: &ToolSpec, globals: &Globals, params: JsonObject) -> Result<Vec<OsString>> {
    for arg in &spec.args {
        let id = arg.get_id().as_str();
        if spec.is_required(arg) && params.get(id).is_none_or(Value::is_null) {
            bail!("missing param: {id}");
        }
    }
    let mut options: Vec<OsString> = Vec::new();
    let mut positionals: Vec<(usize, Vec<String>)> = Vec::new();
    for (key, value) in params {
        let arg = spec
            .args
            .iter()
            .find(|arg| arg.get_id() == key.as_str())
            .ok_or_else(|| eyre!("unknown param: {key}"))?;
        if is_flag(arg) {
            match value {
                Value::Bool(true) => options.push(format!("--{}", long(arg)).into()),
                Value::Bool(false) | Value::Null => {}
                _ => bail!("{key}: must be true or false"),
            }
            continue;
        }
        let values = match value {
            Value::Null => continue,
            Value::Array(items) if takes_many(arg) => items
                .into_iter()
                .map(|item| scalar(&key, item))
                .collect::<Result<_>>()?,
            value => vec![scalar(&key, value)?],
        };
        if let Some(index) = arg.get_index() {
            positionals.push((index, values));
        } else {
            let long = long(arg);
            options.extend(
                values
                    .iter()
                    .map(|value| format!("--{long}={value}").into()),
            );
        }
    }
    positionals.sort_by_key(|(index, _)| *index);
    // A missing optional positional before a given one would shift the given
    // values into the wrong args.
    if let Some((last, _)) = positionals.last() {
        for arg in &spec.args {
            if arg.get_index().is_some_and(|index| index < *last)
                && !positionals
                    .iter()
                    .any(|(index, _)| Some(*index) == arg.get_index())
            {
                bail!("missing param: {}", arg.get_id());
            }
        }
    }

    let mut argv: Vec<OsString> = vec!["--json".into()];
    if globals.verbose {
        argv.push("--verbose".into());
    }
    if let Some(settings) = &globals.settings {
        let mut flag = OsString::from("--settings=");
        flag.push(settings);
        argv.push(flag);
    }
    if let Some(profile) = &globals.profile {
        argv.push(format!("--profile={profile}").into());
    }
    argv.extend(spec.name.path().iter().map(OsString::from));
    argv.extend(options);
    argv.push("--".into());
    argv.extend(
        positionals
            .into_iter()
            .flat_map(|(_, values)| values)
            .map(OsString::from),
    );
    Ok(argv)
}

fn long(arg: &Arg) -> &str {
    arg.get_long()
        .unwrap_or_else(|| panic!("{}: an option has a long name", arg.get_id()))
}

/// The largest magnitude at which every integer has an exact f64.
const MAX_EXACT_FLOAT: f64 = 9_007_199_254_740_992.0;

fn scalar(key: &str, value: Value) -> Result<String> {
    let text = match value {
        Value::String(text) => text,
        Value::Number(number) if number.is_i64() || number.is_u64() => number.to_string(),
        // serde_json reads an integer too large for u64 as an f64, which has
        // lost digits by now.
        Value::Number(number)
            if number
                .as_f64()
                .is_some_and(|float| float.abs() < MAX_EXACT_FLOAT) =>
        {
            number.to_string()
        }
        Value::Number(_) => bail!("{key}: the number is too large, pass it as a string"),
        _ => bail!("{key}: must be a string or a number"),
    };
    if text.contains('\0') {
        bail!("{key}: must not contain NUL");
    }
    Ok(text)
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
    tools: Arc<[ToolSpec]>,
    globals: Arc<Globals>,
    exe: Arc<PathBuf>,
    running: Arc<Semaphore>,
}

impl Snouty {
    fn new(globals: Globals) -> Result<Self> {
        let root_globals: Vec<String> = Cli::command()
            .get_arguments()
            .map(|arg| arg.get_id().to_string())
            .collect();
        let mut cli = Cli::command();
        cli.build();
        let tools = ToolName::ALL
            .into_iter()
            .map(|name| ToolSpec::new(name, &cli, &root_globals))
            .collect();
        let running = Arc::new(Semaphore::new(globals.max_concurrent_calls.get()));
        Ok(Snouty {
            tools,
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
        let result = ListToolsResult::with_all_items(
            self.tools.iter().map(|spec| spec.tool.clone()).collect(),
        );
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
        self.tools
            .iter()
            .find(|spec| spec.tool.name == name)
            .map(|spec| spec.tool.clone())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(spec) = self
            .tools
            .iter()
            .find(|spec| spec.tool.name == request.name)
        else {
            return Err(ErrorData::invalid_params(
                format!("unknown tool: {}", request.name),
                None,
            ));
        };
        let argv = match command_args(spec, &self.globals, request.arguments.unwrap_or_default()) {
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
        // The client sets the time limit: it cancels the call, or closes
        // stdin. Either drops `call`, which kills the child.
        tokio::select! {
            result = call => result,
            () = ctx.ct.cancelled() => Err(ErrorData::internal_error("cancelled", None)),
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
mod tests {
    use super::*;
    use crate::cli::{Commands, RunsCommands};
    use clap::Parser;
    use hegel::generators;
    use serde_json::json;

    fn snouty() -> Snouty {
        Snouty::new(Globals {
            settings: None,
            profile: None,
            verbose: false,
            max_concurrent_calls: NonZeroUsize::new(8).unwrap(),
        })
        .unwrap()
    }

    fn spec(snouty: &Snouty, name: ToolName) -> &ToolSpec {
        snouty.tools.iter().find(|spec| spec.name == name).unwrap()
    }

    /// Parses a tool call's command line as snouty would.
    fn parse(argv: Vec<OsString>) -> Cli {
        Cli::try_parse_from(std::iter::once(OsString::from("snouty")).chain(argv))
            .expect("the command line parses")
    }

    fn params(value: Value) -> JsonObject {
        match value {
            Value::Object(object) => object,
            _ => panic!("params are an object"),
        }
    }

    /// The hidden and required ids name real args, so a renamed arg cannot
    /// slip past the table.
    #[test]
    fn table_ids_name_real_args() {
        let mut cli = Cli::command();
        cli.build();
        for name in ToolName::ALL {
            let command = name
                .path()
                .iter()
                .try_fold(&cli, |command, part| command.find_subcommand(part))
                .unwrap();
            for id in name.hidden().iter().chain(name.required()) {
                assert!(
                    command.get_arguments().any(|arg| arg.get_id() == *id),
                    "{name}: no arg {id}"
                );
            }
        }
    }

    /// The root's globals are the server's to set, and every tool but debug
    /// is read-only.
    #[test]
    fn tools_expose_no_globals_and_only_debug_writes() {
        for spec in snouty().tools.iter() {
            let properties = spec.tool.input_schema["properties"].as_object().unwrap();
            for global in ["json", "verbose", "settings", "profile", "help"] {
                assert!(!properties.contains_key(global), "{}: {global}", spec.name);
            }
            for hidden in spec.name.hidden() {
                assert!(!properties.contains_key(*hidden), "{}: {hidden}", spec.name);
            }
            let annotations = spec.tool.annotations.as_ref().unwrap();
            let read_only = spec.name != ToolName::Debug;
            assert_eq!(annotations.read_only_hint, Some(read_only), "{}", spec.name);
        }
    }

    /// An option value reaches its arg unchanged, whatever its text, and
    /// cannot set another flag.
    #[hegel::test]
    fn an_option_value_stays_one_value(tc: hegel::TestCase) {
        // A NUL is refused, as `bad_params_are_refused_before_any_command`
        // checks.
        let value = tc.draw(generators::text().exclude_characters("\0"));
        let snouty = snouty();
        let call = params(json!({ "launcher": value }));
        let argv = command_args(spec(&snouty, ToolName::RunsList), &snouty.globals, call);
        let cli = parse(argv.unwrap());
        assert!(cli.json && cli.settings.is_none() && cli.profile.is_none());
        let Commands::Runs {
            command: Some(RunsCommands::List(args)),
        } = cli.command
        else {
            panic!("not runs list");
        };
        assert_eq!(args.launcher.as_deref(), Some(value.as_str()));
    }

    /// A positional value reaches its arg unchanged, whatever its text, and
    /// cannot set another flag or name another command.
    #[hegel::test]
    fn a_positional_value_stays_one_value(tc: hegel::TestCase) {
        let script = tc.draw(generators::text().min_size(1).exclude_characters("\0"));
        let snouty = snouty();
        let call = params(json!({
            "run_id": "r", "input_hash": "-1", "vtime": 1.5, "script": script,
        }));
        let argv = command_args(spec(&snouty, ToolName::RunsExec), &snouty.globals, call);
        let cli = parse(argv.unwrap());
        assert!(cli.json && cli.settings.is_none() && cli.profile.is_none());
        let Commands::Runs {
            command:
                Some(RunsCommands::Exec {
                    script: parsed,
                    input_hash,
                    ..
                }),
        } = cli.command
        else {
            panic!("not runs exec");
        };
        assert_eq!(parsed.as_deref(), Some(script.as_str()));
        assert_eq!(input_hash, "-1");
    }

    #[test]
    fn the_server_globals_reach_the_child() {
        let snouty = Snouty::new(Globals {
            settings: Some("s.toml".into()),
            profile: Some("p".to_owned()),
            verbose: true,
            max_concurrent_calls: NonZeroUsize::new(1).unwrap(),
        })
        .unwrap();
        let argv = command_args(
            spec(&snouty, ToolName::Doctor),
            &snouty.globals,
            params(json!({})),
        );
        let cli = parse(argv.unwrap());
        assert_eq!(
            cli.settings.as_deref(),
            Some(std::path::Path::new("s.toml"))
        );
        assert_eq!(cli.profile.as_deref(), Some("p"));
        assert!(cli.verbose);
    }

    #[test]
    fn bad_params_are_refused_before_any_command() {
        let snouty = snouty();
        let cases = [
            (
                ToolName::RunsShow,
                json!({ "run_id": "r", "web": true }),
                "unknown param: web",
            ),
            (
                ToolName::RunsList,
                json!({ "settings": "x" }),
                "unknown param: settings",
            ),
            (
                ToolName::RunsSearch,
                json!({ "run_id": "r", "query": "q", "count": "yes" }),
                "count: must be true or false",
            ),
            (
                ToolName::RunsList,
                json!({ "launcher": ["a"] }),
                "launcher: must be a string or a number",
            ),
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "-1", "vtime": 1 }),
                "missing param: script",
            ),
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "-1", "script": "ls" }),
                "missing param: vtime",
            ),
            (ToolName::RunsShow, json!({}), "missing param: run_id"),
            (
                ToolName::RunsLogs,
                json!({ "run_id": "r", "vtime": 1 }),
                "missing param: input_hash",
            ),
            (
                ToolName::RunsList,
                json!({ "launcher": "x\u{0}y" }),
                "launcher: must not contain NUL",
            ),
            (
                ToolName::RunsShow,
                json!({ "run_id": 12345678901234567890123.0 }),
                "run_id: the number is too large, pass it as a string",
            ),
            (
                ToolName::RunsList,
                json!({ "limit": 1e300 }),
                "limit: the number is too large, pass it as a string",
            ),
        ];
        for (name, call, error) in cases {
            let result = command_args(spec(&snouty, name), &snouty.globals, params(call.clone()));
            let error_text = result.err().map(|e| e.to_string());
            assert_eq!(error_text.as_deref(), Some(error), "{name} {call}");
        }
    }

    #[test]
    fn schemas_follow_the_clap_args() {
        let snouty = snouty();
        let exec = &spec(&snouty, ToolName::RunsExec).tool.input_schema;
        assert_eq!(
            exec["required"],
            json!(["run_id", "input_hash", "vtime", "script"])
        );
        let search = &spec(&snouty, ToolName::RunsSearch).tool.input_schema;
        assert_eq!(search["properties"]["count"]["type"], "boolean");
        let list = &spec(&snouty, ToolName::RunsList).tool.input_schema;
        assert!(
            list["properties"]["status"]["description"]
                .as_str()
                .unwrap()
                .contains("completed")
        );
        assert_eq!(list["properties"]["limit"]["type"], "integer");
        assert_eq!(list["properties"]["limit"]["default"], 10);
        assert_eq!(list["properties"]["launcher"]["type"], "string");
        let logs = &spec(&snouty, ToolName::RunsLogs).tool.input_schema;
        assert_eq!(logs["properties"]["input_hash"]["type"], "string");
        assert_eq!(logs["properties"]["vtime"]["type"], "number");
        assert_eq!(list["additionalProperties"], false);
        let debug = &spec(&snouty, ToolName::Debug).tool.input_schema;
        assert_eq!(debug["required"], json!(["input_hash", "vtime"]));
        assert_eq!(debug["properties"]["params"]["type"], "array");
        let wait = &spec(&snouty, ToolName::RunsWait).tool.input_schema;
        assert_eq!(
            wait["properties"]["until"]["enum"],
            json!(["ready", "complete"])
        );
        assert_eq!(wait["properties"]["poll_interval"]["default"], "1m");
    }

    #[test]
    fn debug_params_reach_the_command() {
        let snouty = snouty();
        let call = params(json!({
            "run_id": "r", "input_hash": "-5", "vtime": 2.5, "params": ["a=b", "c=d"],
        }));
        let argv = command_args(spec(&snouty, ToolName::Debug), &snouty.globals, call);
        let cli = parse(argv.unwrap());
        let Commands::Debug(debug) = cli.command else {
            panic!("not debug");
        };
        assert_eq!(debug.run_id.as_deref(), Some("r"));
        assert_eq!(debug.input_hash, "-5");
        assert_eq!(debug.params.len(), 2);
    }

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
