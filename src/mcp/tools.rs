//! The MCP tools. Each tool except tool_help parses its params into the clap
//! command of its subcommand, runs the subcommand's code in JSON mode, and
//! returns what the subcommand writes.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{Report, Result};
use rmcp::handler::server::common::schema_for_input;
use rmcp::model::{
    CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
    Implementation, JsonObject, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ServerCapabilities, ServerConfig, Tool, ToolAnnotations,
};
use rmcp::schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::OutputOptions;
use crate::api::{RunStatus, SEARCH_MAX_LIMIT};
use crate::cli::{
    ALL_RUN_STATUSES, DEFAULT_DOCS_SEARCH_LIMIT, DEFAULT_EXEC_TIMEOUT_SECS, DEFAULT_RUNS_LIMIT,
    DocsCommands, EventOutputArgs, RunsCommands, RunsListArgs, RunsSearchArgs, check_search_limit,
    parse_run_status, validate_non_empty,
};
use crate::docs::Refresh;
use crate::error::render_report;
use crate::event_render::strip_ansi;
use crate::features::Feature;
use crate::help::{HelpPage, Target};
use crate::settings::Settings;
use crate::vtime::VTime;

/// Declares [`ToolName`] with one row per tool: the variant and its name.
/// The name also selects the short description, `help/mcp_tools/$name.txt`
/// as build.rs rendered it. `ALL`, `Display` and `description` come from the
/// rows, so a tool cannot be left out of one of them.
macro_rules! tools {
    ($($variant:ident => $name:literal,)*) => {
        /// Every MCP tool. The tool list, the dispatch and the feature gate
        /// each match on it exhaustively.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum ToolName {
            $($variant,)*
        }

        impl ToolName {
            pub const ALL: [ToolName; [$($name),*].len()] = [$(ToolName::$variant,)*];

            /// The short description that tools/list sends. Every listed
            /// tool costs its description on each request, so the full help
            /// is in tool_help.
            pub fn description(self) -> &'static str {
                match self {
                    $(ToolName::$variant => include_str!(concat!(
                        env!("OUT_DIR"), "/help/mcp_tools/", $name, ".txt"
                    )),)*
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
    RunsList => "runs_list",
    RunsShow => "runs_show",
    RunsProperties => "runs_properties",
    RunsBuildLogs => "runs_build_logs",
    RunsLogs => "runs_logs",
    RunsSearch => "runs_search",
    RunsExec => "runs_exec",
    DocsTree => "docs_tree",
    DocsShow => "docs_show",
    DocsSearch => "docs_search",
    Doctor => "doctor",
    ToolHelp => "tool_help",
}

impl ToolName {
    /// The full help page, whose MCP render tool_help returns. tool_help
    /// has none: its description is all of its help.
    pub fn page(self) -> Option<HelpPage> {
        Some(match self {
            ToolName::RunsList => HelpPage::RunsList,
            ToolName::RunsShow => HelpPage::RunsShow,
            ToolName::RunsProperties => HelpPage::RunsProperties,
            ToolName::RunsBuildLogs => HelpPage::RunsBuildLogs,
            ToolName::RunsLogs => HelpPage::RunsLogs,
            ToolName::RunsSearch => HelpPage::RunsSearch,
            ToolName::RunsExec => HelpPage::RunsExec,
            ToolName::DocsTree => HelpPage::DocsTree,
            ToolName::DocsShow => HelpPage::DocsShow,
            ToolName::DocsSearch => HelpPage::DocsSearch,
            ToolName::Doctor => HelpPage::Doctor,
            ToolName::ToolHelp => return None,
        })
    }

    fn feature(self) -> Option<Feature> {
        match self {
            ToolName::RunsExec => Some(Feature::RunsExec),
            ToolName::RunsList
            | ToolName::RunsShow
            | ToolName::RunsProperties
            | ToolName::RunsBuildLogs
            | ToolName::RunsLogs
            | ToolName::RunsSearch
            | ToolName::DocsTree
            | ToolName::DocsShow
            | ToolName::DocsSearch
            | ToolName::Doctor
            | ToolName::ToolHelp => None,
        }
    }

    fn input_schema(self) -> Arc<JsonObject> {
        let schema = match self {
            ToolName::RunsList => schema_for_input::<RunsListParams>(),
            ToolName::RunsShow => schema_for_input::<RunParams>(),
            ToolName::RunsProperties => schema_for_input::<RunsPropertiesParams>(),
            ToolName::RunsBuildLogs => schema_for_input::<RunParams>(),
            ToolName::RunsLogs => schema_for_input::<RunsLogsParams>(),
            ToolName::RunsSearch => schema_for_input::<RunsSearchParams>(),
            ToolName::RunsExec => schema_for_input::<RunsExecParams>(),
            ToolName::DocsTree => schema_for_input::<DocsTreeParams>(),
            ToolName::DocsShow => schema_for_input::<DocsShowParams>(),
            ToolName::DocsSearch => schema_for_input::<DocsSearchParams>(),
            ToolName::Doctor => schema_for_input::<DoctorParams>(),
            ToolName::ToolHelp => schema_for_input::<ToolHelpParams>(),
        };
        // Every params type is a struct, so its schema is an object.
        schema.unwrap_or_else(|e| panic!("{self} input schema: {e}"))
    }
}

impl FromStr for ToolName {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        ToolName::ALL
            .into_iter()
            .find(|tool| tool.to_string() == name)
            .ok_or_else(|| format!("unknown tool: {name}"))
    }
}

/// The MCP server handler. rmcp clones the handler for each request. Thus,
/// the fields are `Arc`s, and a clone is cheap.
#[derive(Clone)]
pub struct Snouty {
    settings: Arc<Settings>,
    verbose: bool,
    tools: Arc<[(ToolName, Tool)]>,
}

impl Snouty {
    /// `enabled` names the unstable features that are on. A tool whose
    /// feature is off is not listed and cannot be called.
    pub fn new(settings: Settings, verbose: bool, enabled: &[Feature]) -> Self {
        let listed: Vec<ToolName> = ToolName::ALL
            .iter()
            .copied()
            .filter(|tool| tool.feature().is_none_or(|f| enabled.contains(&f)))
            .collect();
        // tool_help takes the name of a listed tool that has a full page.
        let help_names: Vec<Value> = listed
            .iter()
            .filter(|tool| tool.page().is_some())
            .map(|tool| Value::String(tool.to_string()))
            .collect();
        let tools = listed
            .into_iter()
            .map(|tool| {
                let mut schema = tool.input_schema();
                if tool == ToolName::ToolHelp {
                    Arc::make_mut(&mut schema)["properties"]["tool"]["enum"] =
                        Value::Array(help_names.clone());
                }
                let tool_def = Tool::new(tool.to_string(), tool.description(), schema)
                    .with_annotations(ToolAnnotations::new().read_only(true));
                (tool, tool_def)
            })
            .collect();
        Snouty {
            settings: Arc::new(settings),
            verbose,
            tools,
        }
    }

    /// Returns whether the call succeeded. Only doctor can fail with no
    /// error: its report says what failed.
    async fn execute(&self, call: Call, out: &mut Vec<u8>) -> Result<bool> {
        let output = OutputOptions {
            json: true,
            verbose: self.verbose,
        };
        match call {
            Call::Runs(command) => {
                crate::runs::cmd_runs(Some(command), &self.settings, output, out)
                    .await
                    .map(|()| true)
            }
            Call::Docs(command) => {
                crate::docs::cmd_docs(command, Refresh::OnlineQuiet, output, out)
                    .await
                    .map(|()| true)
            }
            Call::Doctor { offline } => {
                crate::doctor::cmd_doctor(&self.settings, output, offline, out).await
            }
            Call::Help(page) => {
                out.extend_from_slice(page.text(Target::Mcp).as_bytes());
                Ok(true)
            }
        }
    }
}

impl ServerHandler for Snouty {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("snouty", env!("SNOUTY_VERSION")))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let result = ListToolsResult::with_all_items(
            self.tools.iter().map(|(_, tool)| tool.clone()).collect(),
        );
        // Protocol 2026-07-28 requires cache hints on a list result, and
        // `with_all_items` leaves them out. These are the values that rmcp's
        // own `#[tool_handler]` list_tools sends.
        let cache_hints = ctx
            .protocol_version()
            .is_some_and(|version| version >= ProtocolVersion::V_2026_07_28);
        Ok(if cache_hints {
            result.with_ttl_ms(0).with_cache_scope(CacheScope::Public)
        } else {
            result
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools
            .iter()
            .find(|(_, tool)| tool.name == name)
            .map(|(_, tool)| tool.clone())
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        // Only a listed tool can be called, so a tool whose feature is off
        // is unknown.
        let Some(&(tool, _)) = self
            .tools
            .iter()
            .find(|(_, tool)| tool.name == request.name)
        else {
            return Err(ErrorData::invalid_params(
                format!("unknown tool: {}", request.name),
                None,
            ));
        };
        let args = Value::Object(request.arguments.unwrap_or_default());
        let is_listed = |name| self.tools.iter().any(|&(listed, _)| listed == name);
        let call = match Call::parse(tool, args, is_listed) {
            Ok(call) => call,
            // The MCP spec reports a bad input as a tool error, so the model
            // can correct its call.
            Err(message) => return Ok(text_result(message, true)),
        };
        let mut buf = Vec::new();
        // rmcp cancels `ctx.ct` when the client disconnects or the server
        // stops. Without this, the call keeps its API connection open.
        let result = tokio::select! {
            result = self.execute(call, &mut buf) => result,
            _ = ctx.ct.cancelled() => return Err(ErrorData::internal_error("cancelled", None)),
        };
        // The text is what the command wrote, then the error text when it
        // failed, as the CLI writes stdout and then stderr.
        let mut text = String::from_utf8(buf)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        let is_error = match result {
            Ok(succeeded) => !succeeded,
            Err(report) => {
                append_error(&mut text, &report);
                true
            }
        };
        Ok(text_result(text, is_error))
    }
}

fn append_error(text: &mut String, report: &Report) {
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    // The server's stderr can be a terminal, which colors the report.
    text.push_str(&strip_ansi(&render_report(report)));
}

fn text_result(text: String, is_error: bool) -> CallToolResponse {
    let content = vec![ContentBlock::text(text)];
    if is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    }
    .into()
}

/// A tool call, parsed into what its subcommand takes.
enum Call {
    Runs(RunsCommands),
    Docs(DocsCommands),
    Doctor { offline: bool },
    Help(HelpPage),
}

impl Call {
    /// Parses and checks the params before any work starts. The error is the
    /// text of the tool result.
    /// `is_listed` says whether tool_help can name a tool.
    fn parse(
        tool: ToolName,
        args: Value,
        is_listed: impl Fn(ToolName) -> bool,
    ) -> Result<Call, String> {
        Ok(match tool {
            ToolName::RunsList => {
                let p: RunsListParams = params(args)?;
                Call::Runs(RunsCommands::List(RunsListArgs {
                    status: p.status,
                    launcher: p.launcher,
                    created_after: p.created_after,
                    created_before: p.created_before,
                    limit: p.limit.unwrap_or(DEFAULT_RUNS_LIMIT),
                    detail: false,
                }))
            }
            ToolName::RunsShow => {
                let p: RunParams = params(args)?;
                Call::Runs(RunsCommands::Show {
                    run_id: p.run_id,
                    web: false,
                })
            }
            ToolName::RunsProperties => {
                let p: RunsPropertiesParams = params(args)?;
                if p.passing && p.failing {
                    return Err("invalid params: passing and failing cannot both be true".into());
                }
                Call::Runs(RunsCommands::Properties {
                    run_id: p.run_id,
                    passing: p.passing,
                    failing: p.failing,
                    name: p.name,
                    group: p.group,
                    detail: false,
                })
            }
            ToolName::RunsBuildLogs => {
                let p: RunParams = params(args)?;
                Call::Runs(RunsCommands::BuildLogs { run_id: p.run_id })
            }
            ToolName::RunsLogs => {
                let p: RunsLogsParams = params(args)?;
                Call::Runs(RunsCommands::Logs {
                    run_id: p.run_id,
                    input_hash: p.input_hash,
                    vtime: p.vtime,
                    begin_vtime: p.begin_vtime,
                    render: EventOutputArgs {
                        raw: false,
                        detail: false,
                    },
                })
            }
            ToolName::RunsSearch => {
                let p: RunsSearchParams = params(args)?;
                Call::Runs(RunsCommands::Search(RunsSearchArgs {
                    run_id: p.run_id,
                    query: p.query,
                    limit: p.limit,
                    follow: false,
                    check: false,
                    render: EventOutputArgs {
                        raw: false,
                        detail: false,
                    },
                }))
            }
            ToolName::RunsExec => {
                let p: RunsExecParams = params(args)?;
                Call::Runs(RunsCommands::Exec {
                    run_id: p.run_id,
                    input_hash: p.input_hash,
                    vtime: p.vtime,
                    // Always `Some`: with no script, the command reads stdin.
                    script: Some(p.script),
                    timeout: p.timeout.map_or(DEFAULT_EXEC_TIMEOUT_SECS, NonZeroU64::get),
                })
            }
            ToolName::DocsTree => {
                let p: DocsTreeParams = params(args)?;
                Call::Docs(DocsCommands::Tree {
                    depth: p.depth,
                    filter: p.filter,
                })
            }
            ToolName::DocsShow => {
                let p: DocsShowParams = params(args)?;
                Call::Docs(DocsCommands::Show { path: p.path })
            }
            ToolName::DocsSearch => {
                let p: DocsSearchParams = params(args)?;
                Call::Docs(DocsCommands::Search {
                    list: false,
                    limit: p.limit.map_or(DEFAULT_DOCS_SEARCH_LIMIT, NonZeroUsize::get),
                    match_mode: p.match_mode,
                    query: vec![p.query],
                })
            }
            ToolName::Doctor => {
                let p: DoctorParams = params(args)?;
                Call::Doctor { offline: p.offline }
            }
            ToolName::ToolHelp => {
                let p: ToolHelpParams = params(args)?;
                let page = p.tool.page().filter(|_| is_listed(p.tool));
                Call::Help(
                    page.ok_or_else(|| format!("invalid params: tool: no help for {}", p.tool))?,
                )
            }
        })
    }
}

/// The error names the path of the bad field, for example
/// `invalid params: limit: invalid type`.
fn params<P: DeserializeOwned>(args: Value) -> Result<P, String> {
    serde_path_to_error::deserialize(args).map_err(|e| format!("invalid params: {e}"))
}

// The params types have no doc comment, because rmcp strips the root title
// and description of an input schema. MCP rejects an empty or blank id or
// query, which the CLI sends to the server: a model sends such a value by
// mistake, and the server error says less.

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunsListParams {
    /// Only runs with this status
    #[serde(default, deserialize_with = "run_status")]
    #[schemars(schema_with = "run_status_schema")]
    status: Option<RunStatus>,
    /// Only runs whose launcher has this name
    launcher: Option<String>,
    /// Only runs created after this ISO 8601 timestamp
    created_after: Option<DateTime<Utc>>,
    /// Only runs created before this ISO 8601 timestamp
    created_before: Option<DateTime<Utc>>,
    /// The maximum number of runs to return (default: 10)
    limit: Option<NonZeroU64>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunParams {
    /// The run ID
    #[serde(deserialize_with = "non_empty")]
    run_id: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunsPropertiesParams {
    /// The run ID
    #[serde(deserialize_with = "non_empty")]
    run_id: String,
    /// Only passing properties. Cannot be true together with failing.
    #[serde(default)]
    passing: bool,
    /// Only failing properties. Cannot be true together with passing.
    #[serde(default)]
    failing: bool,
    /// Only properties whose name contains this substring (case-insensitive)
    name: Option<String>,
    /// Only properties whose group contains this substring (case-insensitive)
    group: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunsLogsParams {
    /// The run ID
    #[serde(deserialize_with = "non_empty")]
    run_id: String,
    /// The timeline to stream. A string, as it appears in the output of
    /// runs_properties, runs_search and runs_logs. A JSON number cannot hold
    /// every input hash exactly.
    #[serde(deserialize_with = "non_empty")]
    input_hash: String,
    /// End the logs at this virtual time. Without it, the logs go to the
    /// current end of the timeline.
    #[serde(default)]
    #[schemars(schema_with = "optional_vtime_schema")]
    vtime: Option<VTime>,
    /// Start the logs at this virtual time instead of the root
    #[serde(default)]
    #[schemars(schema_with = "optional_vtime_schema")]
    begin_vtime: Option<VTime>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunsSearchParams {
    /// The run ID
    #[serde(deserialize_with = "non_empty")]
    run_id: String,
    /// An event-set DSL query
    #[serde(deserialize_with = "non_empty")]
    query: String,
    /// The maximum number of events to return (default: 50, maximum: 999)
    #[serde(default, deserialize_with = "search_limit")]
    #[schemars(schema_with = "search_limit_schema")]
    limit: Option<NonZeroU64>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct RunsExecParams {
    /// The run ID. The run must have a live session.
    #[serde(deserialize_with = "non_empty")]
    run_id: String,
    /// The input hash of the moment to execute at. A string, as it appears in
    /// the output of runs_properties, runs_search and runs_logs.
    #[serde(deserialize_with = "non_empty")]
    input_hash: String,
    /// The virtual time of the moment to execute at
    #[schemars(schema_with = "vtime_schema")]
    vtime: VTime,
    /// The bash script to execute
    #[serde(deserialize_with = "non_empty")]
    script: String,
    /// Seconds to wait for the script to exit (default: 30)
    timeout: Option<NonZeroU64>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct DocsTreeParams {
    /// A case-insensitive filter on page paths and titles
    filter: Option<String>,
    /// Only nodes at this depth or less. The top-level nodes are depth 1.
    depth: Option<NonZeroUsize>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct DocsShowParams {
    /// The docs page to show, for example "getting_started/overview"
    #[serde(deserialize_with = "non_empty")]
    path: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct DocsSearchParams {
    /// The search text
    #[serde(deserialize_with = "non_empty")]
    query: String,
    /// The maximum number of results to return (default: 10)
    limit: Option<NonZeroUsize>,
    /// Treat the query as a raw SQLite FTS5 expression instead of literal
    /// text
    #[serde(default, rename = "match")]
    match_mode: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct DoctorParams {
    /// Skip the Antithesis API connectivity check
    #[serde(default)]
    offline: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
struct ToolHelpParams {
    /// The tool to document, for example "runs_search"
    #[serde(deserialize_with = "tool_name")]
    #[schemars(schema_with = "tool_name_schema")]
    tool: ToolName,
}

fn tool_name<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ToolName, D::Error> {
    String::deserialize(deserializer)?
        .parse()
        .map_err(D::Error::custom)
}

/// `Snouty::new` adds the enum of the listed tools.
fn tool_name_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": "string" })
}

fn non_empty<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    validate_non_empty(&String::deserialize(deserializer)?).map_err(D::Error::custom)
}

fn run_status<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<RunStatus>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|status| parse_run_status(&status).map_err(D::Error::custom))
        .transpose()
}

fn search_limit<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<NonZeroU64>, D::Error> {
    Option::<u64>::deserialize(deserializer)?
        .map(|limit| check_search_limit(limit).map_err(D::Error::custom))
        .transpose()
}

// A `schema_with` field that is optional also gets `"default": null` from
// its `#[serde(default)]`, so its schema must allow null.

fn run_status_schema(_: &mut SchemaGenerator) -> Schema {
    let statuses: Vec<Value> = ALL_RUN_STATUSES
        .iter()
        .map(|status| Value::String(status.to_string()))
        .chain([Value::Null])
        .collect();
    json_schema!({ "type": ["string", "null"], "enum": statuses })
}

/// A vtime is seconds as a JSON number, or its decimal-string form.
fn vtime_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": ["number", "string"] })
}

fn optional_vtime_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": ["number", "string", "null"] })
}

fn search_limit_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": ["integer", "null"], "minimum": 1, "maximum": SEARCH_MAX_LIMIT })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::SEARCH_DEFAULT_LIMIT;
    use hegel::generators;
    use serde_json::json;

    /// Parses with every tool listed.
    fn parse(tool: ToolName, args: Value) -> Result<Call, String> {
        Call::parse(tool, args, |_| true)
    }

    fn all_tools() -> Snouty {
        Snouty::new(Settings::default(), false, &[Feature::RunsExec])
    }

    fn tool(snouty: &Snouty, name: ToolName) -> &Tool {
        let (_, tool) = snouty.tools.iter().find(|(t, _)| *t == name).unwrap();
        tool
    }

    /// A valid set of arguments for each tool.
    fn valid_args(tool: ToolName) -> Value {
        match tool {
            ToolName::RunsList | ToolName::DocsTree | ToolName::Doctor => json!({}),
            ToolName::RunsShow | ToolName::RunsProperties | ToolName::RunsBuildLogs => {
                json!({"run_id": "r"})
            }
            ToolName::RunsLogs => json!({"run_id": "r", "input_hash": "-1"}),
            ToolName::RunsSearch => json!({"run_id": "r", "query": "q"}),
            ToolName::RunsExec => {
                json!({"run_id": "r", "input_hash": "-1", "vtime": 1, "script": "ls"})
            }
            ToolName::DocsShow => json!({"path": "p"}),
            ToolName::DocsSearch => json!({"query": "q"}),
            ToolName::ToolHelp => json!({"tool": "runs_search"}),
        }
    }

    /// Every tool is read-only, and its schema is an object with no root
    /// title or description.
    #[test]
    fn tools_are_read_only_with_clean_schemas() {
        for (_, tool) in all_tools().tools.iter() {
            let annotations = tool.annotations.as_ref().unwrap();
            assert_eq!(annotations.read_only_hint, Some(true), "{}", tool.name);
            let schema = &tool.input_schema;
            assert!(!schema.contains_key("title"), "{}", tool.name);
            assert!(!schema.contains_key("description"), "{}", tool.name);
            assert_eq!(schema["type"], "object", "{}", tool.name);
        }
    }

    /// A `schema_with` field is required unless it has `#[serde(default)]`,
    /// so the schema can require a param that the tool does not need.
    #[test]
    fn schemas_require_only_the_needed_params() {
        let tools = all_tools();
        for name in ToolName::ALL {
            let schema = &tool(&tools, name).input_schema;
            let mut required: Vec<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .map(|r| r.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            required.sort_unstable();
            let args = valid_args(name);
            let mut needed: Vec<&str> = args
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            needed.sort_unstable();
            assert_eq!(required, needed, "{name}");
            // A default must fit the type of its field.
            for (field, prop) in schema["properties"].as_object().unwrap() {
                if prop.get("default") == Some(&Value::Null) {
                    let types = prop["type"].as_array();
                    let nullable = types.is_some_and(|t| t.contains(&json!("null")));
                    assert!(nullable, "{name}.{field}: {prop}");
                }
            }
        }
        let search = tool(&tools, ToolName::DocsSearch);
        assert!(search.input_schema["properties"].get("match").is_some());
    }

    /// The defaults in the schema descriptions and the tool descriptions
    /// are text, so this test keeps them equal to the constants.
    #[test]
    fn descriptions_name_the_default_values() {
        let tools = all_tools();
        let cases = [
            (ToolName::RunsList, "limit", format!("{DEFAULT_RUNS_LIMIT}")),
            (
                ToolName::RunsSearch,
                "limit",
                format!("{SEARCH_DEFAULT_LIMIT}, maximum {SEARCH_MAX_LIMIT}"),
            ),
            (
                ToolName::DocsSearch,
                "limit",
                format!("{DEFAULT_DOCS_SEARCH_LIMIT}"),
            ),
            (
                ToolName::RunsExec,
                "timeout",
                format!("{DEFAULT_EXEC_TIMEOUT_SECS}"),
            ),
        ];
        for (name, field, values) in cases {
            let tool = tool(&tools, name);
            let expected = format!("(default {values})");
            // The schema description writes `default: 10, maximum: 999`.
            let schema = tool.input_schema["properties"][field]["description"]
                .as_str()
                .unwrap()
                .replace(": ", " ");
            assert!(schema.contains(&expected), "{name}.{field}: {schema}");
            let description = tool.description.as_deref().unwrap();
            assert!(description.contains(&expected), "{name}: {expected}");
        }
    }

    #[test]
    fn bad_params_fail_before_any_work() {
        let mut cases = vec![
            (ToolName::RunsShow, json!({})),
            (ToolName::RunsShow, json!({"run_id": " "})),
            (ToolName::RunsList, json!({"status": "bogus"})),
            (ToolName::RunsList, json!({"limit": 0})),
            (ToolName::RunsList, json!({"created_after": "yesterday"})),
            (
                ToolName::RunsLogs,
                json!({"run_id": "r", "input_hash": -3625518438076122494i64}),
            ),
            (ToolName::RunsLogs, json!({"run_id": "r", "input_hash": ""})),
            (
                ToolName::RunsProperties,
                json!({"run_id": "r", "passing": true, "failing": true}),
            ),
            (ToolName::RunsSearch, json!({"run_id": "r", "query": " "})),
            (
                ToolName::RunsExec,
                json!({"run_id": "r", "input_hash": "-1", "vtime": 1}),
            ),
            (
                ToolName::RunsExec,
                json!({"run_id": "r", "input_hash": "-1", "vtime": 1, "script": " "}),
            ),
            (ToolName::DocsTree, json!({"depth": 0})),
            (ToolName::DocsSearch, json!({"query": ""})),
            (ToolName::ToolHelp, json!({"tool": "nope"})),
            (ToolName::ToolHelp, json!({"tool": "tool_help"})),
        ];
        for tool in ToolName::ALL {
            assert!(parse(tool, valid_args(tool)).is_ok(), "{tool}");
            let mut args = valid_args(tool);
            args["bogus"] = json!(1);
            cases.push((tool, args));
        }
        for (tool, args) in cases {
            let err = parse(tool, args.clone()).err();
            let err = err.unwrap_or_else(|| panic!("{tool} accepted {args}"));
            assert!(err.starts_with("invalid params: "), "{tool} {args}: {err}");
        }
    }

    #[test]
    fn param_errors_name_the_field() {
        let err = parse(ToolName::RunsList, json!({"created_after": "2025-01-01"}));
        assert!(
            err.as_ref()
                .is_err_and(|e| e.starts_with("invalid params: created_after: ")),
            "{:?}",
            err.err()
        );
    }

    #[test]
    fn params_reach_the_cli_command() {
        let Ok(Call::Runs(RunsCommands::Exec {
            script, timeout, ..
        })) = parse(
            ToolName::RunsExec,
            json!({"run_id": "r", "input_hash": "-1", "vtime": "1.5", "script": "ls", "timeout": 5}),
        )
        else {
            panic!("runs_exec did not parse");
        };
        assert_eq!(script.as_deref(), Some("ls"));
        assert_eq!(timeout, 5);

        let Ok(Call::Docs(DocsCommands::Search { match_mode, .. })) =
            parse(ToolName::DocsSearch, json!({"query": "x", "match": true}))
        else {
            panic!("docs_search did not parse");
        };
        assert!(match_mode);
    }

    #[test]
    fn error_text_follows_the_output_with_no_color() {
        let report = crate::error::user_error("\u{1b}[31mstream broke\u{1b}[0m");
        let mut text = "{\"a\":1}".to_owned();
        append_error(&mut text, &report);
        assert!(text.starts_with("{\"a\":1}\nError: stream broke"), "{text}");
        assert!(!text.contains('\u{1b}'), "{text:?}");
    }

    #[hegel::test]
    fn search_limit_param_accepts_exactly_the_cli_range(tc: hegel::TestCase) {
        let limit = tc.draw(generators::integers::<u64>());
        let args = json!({"run_id": "r", "query": "q", "limit": limit});
        let accepted = parse(ToolName::RunsSearch, args).is_ok();
        assert_eq!(accepted, (1..=SEARCH_MAX_LIMIT).contains(&limit));
    }

    #[test]
    fn tool_help_returns_the_full_page_of_a_listed_tool() {
        let Ok(Call::Help(page)) = parse(ToolName::ToolHelp, json!({"tool": "runs_search"})) else {
            panic!("tool_help did not parse");
        };
        assert_eq!(page, HelpPage::RunsSearch);
        // A tool whose feature is off is not listed, so it has no help.
        let gated = Call::parse(ToolName::ToolHelp, json!({"tool": "runs_exec"}), |tool| {
            tool != ToolName::RunsExec
        });
        assert!(gated.is_err());
    }

    #[test]
    fn tool_help_schema_lists_the_listed_tools() {
        let without_exec = Snouty::new(Settings::default(), false, &[]);
        let names =
            tool(&without_exec, ToolName::ToolHelp).input_schema["properties"]["tool"]["enum"]
                .clone();
        let names: Vec<String> = serde_json::from_value(names).unwrap();
        let expected: Vec<String> = ToolName::ALL
            .into_iter()
            .filter(|tool| tool.page().is_some() && *tool != ToolName::RunsExec)
            .map(|tool| tool.to_string())
            .collect();
        assert_eq!(names, expected);
    }
}
