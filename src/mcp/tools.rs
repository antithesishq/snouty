//! The tool table, and the params and schema of each tool, which come from
//! the clap definition of its command.

use std::any::TypeId;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};
use clap::Arg;
use color_eyre::eyre::{Result, bail, eyre};
use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use serde_json::{Value, json};

use crate::api::{SEARCH_MAX_LIMIT, SearchLimit};
use crate::cli::{KVPair, tree};
use crate::help::HelpPage;
use crate::runs::usage::TimeBound;
use crate::time::{BareUnit, HumanDuration};
use crate::vtime::VTime;

pub(super) const TOOL_HELP: &str = "tool_help";

const TOOL_HELP_ABOUT: &str = "Return the full help of a tool";

/// The one param of [`TOOL_HELP`].
const TOOL_PARAM: &str = "tool";

/// What a tool's command can do, which sets the tool's annotations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Effect {
    /// Reads from the API, the docs or the local setup. `runs exec` is here
    /// too: its script runs in a branch of the run, which it cannot change.
    Reads,
    /// Starts an Antithesis session, which uses the tenant's resources.
    StartsSessions,
    /// Ends a run, which cannot be undone.
    Destroys,
}

impl Effect {
    fn annotations(self) -> ToolAnnotations {
        // The MCP default for openWorldHint is true. Every tool reaches only
        // the tenant, the docs or the local setup, which is a closed world.
        let annotations = ToolAnnotations::new().open_world(false);
        match self {
            Effect::Reads => annotations.read_only(true),
            Effect::StartsSessions => annotations.read_only(false).destructive(false),
            Effect::Destroys => annotations.read_only(false).destructive(true),
        }
    }
}

/// One row per tool: the variant, which also names its [`HelpPage`] and so
/// its command, the tool name, its effect, the args that the tool leaves out,
/// and the args that the tool requires although the command does not.
macro_rules! tools {
    ($(
        $variant:ident => $name:literal, $effect:ident,
        hidden: [$($hidden:literal),*], required: [$($required:literal),*];
    )*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum ToolName {
            $($variant,)*
        }

        impl ToolName {
            pub const ALL: [ToolName; [$($name),*].len()] = [$(ToolName::$variant,)*];

            pub fn help(self) -> HelpPage {
                match self {
                    $(ToolName::$variant => HelpPage::$variant,)*
                }
            }

            fn effect(self) -> Effect {
                match self {
                    $(ToolName::$variant => Effect::$effect,)*
                }
            }

            /// Args that open a browser, stream with no end, or have no
            /// effect, such as `--json` of `docs show`.
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
    RunsList => "runs_list", Reads, hidden: [], required: [];
    RunsShow => "runs_show", Reads, hidden: ["web"], required: [];
    RunsWait => "runs_wait", Reads, hidden: [], required: [];
    RunsCancel => "runs_cancel", Destroys, hidden: [], required: [];
    RunsProperties => "runs_properties", Reads, hidden: [], required: [];
    RunsProperty => "runs_property", Reads, hidden: [], required: [];
    RunsBuildLogs => "runs_build_logs", Reads, hidden: [], required: [];
    RunsLogs => "runs_logs", Reads, hidden: [], required: [];
    RunsSearch => "runs_search", Reads, hidden: ["follow"], required: [];
    RunsUsage => "runs_usage", Reads, hidden: [], required: [];
    // With no script, `runs exec` reads it from stdin, and a child's stdin is
    // null.
    RunsExec => "runs_exec", Reads, hidden: [], required: ["script"];
    // `query` repeats `match` as a positional. The command needs one of
    // them, so the tool requires `matches`.
    RunsEvents => "runs_events", Reads, hidden: ["query"], required: ["matches"];
    Debug => "debug", StartsSessions, hidden: [], required: [];
    DocsTree => "docs_tree", Reads, hidden: [], required: [];
    DocsShow => "docs_show", Reads, hidden: ["json"], required: [];
    DocsSearch => "docs_search", Reads, hidden: [], required: [];
    Doctor => "doctor", Reads, hidden: [], required: [];
    Version => "version", Reads, hidden: [], required: [];
    SettingsShow => "settings_show", Reads, hidden: [], required: [];
}

impl FromStr for ToolName {
    type Err = color_eyre::Report;

    fn from_str(name: &str) -> Result<Self> {
        ToolName::ALL
            .into_iter()
            .find(|tool| tool.to_string() == name)
            .ok_or_else(|| eyre!("unknown tool: {name}"))
    }
}

impl ToolName {
    /// The path below `snouty` of the tool's command. The server runs only
    /// these commands.
    pub(crate) fn path(self) -> &'static [&'static str] {
        self.help().path()
    }

    /// The tool whose command is at `path`.
    pub(crate) fn at<S: AsRef<str>>(path: &[S]) -> Option<ToolName> {
        ToolName::ALL.into_iter().find(|tool| {
            tool.path().len() == path.len()
                && tool.path().iter().zip(path).all(|(a, b)| *a == b.as_ref())
        })
    }

    pub(crate) fn spec(self) -> &'static ToolSpec {
        static SPECS: OnceLock<Vec<ToolSpec>> = OnceLock::new();
        let specs = SPECS.get_or_init(|| ToolName::ALL.into_iter().map(ToolSpec::new).collect());
        &specs[self as usize]
    }

    /// What an arg of the tool's command is to the tool.
    pub(crate) fn role(self, id: &str) -> ArgRole {
        match self.spec().param(id) {
            Some(param) => ArgRole::Param(param),
            None if id == "json" || id == "detail" => ArgRole::Implied,
            None => ArgRole::Excluded,
        }
    }
}

/// One row for each tool, `tool_help` too: its name and what it does.
pub(crate) fn summaries() -> Vec<(String, String)> {
    ToolName::ALL
        .into_iter()
        .map(|tool| (tool.to_string(), tool.spec().about.clone()))
        .chain([(TOOL_HELP.to_owned(), TOOL_HELP_ABOUT.to_owned())])
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ArgRole {
    Param(&'static Param),
    /// An arg that an example may carry and the call drops: `--detail`, which
    /// a call without `json` adds, and `--json` of a tool that prints the
    /// same in both modes.
    Implied,
    /// The root's args but `json`, which are the server's to set, and the
    /// args that the tool hides, but `json`.
    Excluded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueType {
    String,
    Integer,
    Number,
}

/// Matches what [`HumanDuration`] accepts with [`BareUnit::Refused`]. The
/// pattern does not check a param's minimum.
const DURATION_PATTERN: &str =
    r"^\s*([0-9]+[hH]([0-9]+[mM])?([0-9]+[sS])?|[0-9]+[mM]([0-9]+[sS])?|[0-9]+[sS])\s*$";

const KV_PAIR_PATTERN: &str = "^[^=]+=";

impl ValueType {
    fn of(arg: &Arg) -> Self {
        value_schema(arg).0
    }

    fn json_type(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Integer => "integer",
            ValueType::Number => "number",
        }
    }
}

/// The type of one value of `arg`, and the schema keywords for the checks
/// that its parser makes.
fn value_schema(arg: &Arg) -> (ValueType, Value) {
    let parsed = arg.get_value_parser().type_id();
    let is = |ty: TypeId| parsed == ty;
    if is(TypeId::of::<usize>()) || is(TypeId::of::<u64>()) {
        (ValueType::Integer, json!({ "minimum": 0 }))
    } else if is(TypeId::of::<NonZeroUsize>()) || is(TypeId::of::<NonZeroU64>()) {
        (ValueType::Integer, json!({ "minimum": 1 }))
    } else if is(TypeId::of::<SearchLimit>()) {
        let range = json!({ "minimum": 1, "maximum": SEARCH_MAX_LIMIT });
        (ValueType::Integer, range)
    } else if is(TypeId::of::<VTime>()) {
        (ValueType::Number, json!({}))
    } else if is(TypeId::of::<DateTime<Utc>>()) {
        (ValueType::String, json!({ "format": "date-time" }))
    } else if is(TypeId::of::<TimeBound>()) {
        let formats = json!({ "anyOf": [{ "format": "date" }, { "format": "date-time" }] });
        (ValueType::String, formats)
    } else if is_duration(arg) {
        (ValueType::String, json!({ "pattern": DURATION_PATTERN }))
    } else if is(TypeId::of::<KVPair>()) {
        (ValueType::String, json!({ "pattern": KV_PAIR_PATTERN }))
    } else {
        (ValueType::String, json!({}))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParamKind {
    Flag,
    One(ValueType),
    Many(ValueType),
}

/// An arg of a tool's command that the tool's caller sets.
#[derive(Debug)]
pub(crate) struct Param {
    pub(crate) arg: &'static Arg,
    pub(crate) kind: ParamKind,
    pub(crate) required: bool,
}

impl Param {
    pub(crate) fn id(&self) -> &'static str {
        self.arg.get_id().as_str()
    }

    /// Rejects a value of a [`HumanDuration`] param that has no unit. A value
    /// of any other param passes.
    pub(crate) fn check(&self, value: &str) -> Result<()> {
        if is_duration(self.arg) {
            HumanDuration::parse(value, BareUnit::Refused)
                .map_err(|e| eyre!("{}: {e}", self.id()))?;
        }
        Ok(())
    }
}

fn is_duration(arg: &Arg) -> bool {
    arg.get_value_parser().type_id() == TypeId::of::<HumanDuration>()
}

pub(crate) struct ToolSpec {
    pub(crate) tool: Tool,
    about: String,
    pub(crate) params: Vec<Param>,
    /// Whether the command has `--detail`, which a call without `json`
    /// passes.
    pub(crate) supports_detail: bool,
}

impl ToolSpec {
    fn new(name: ToolName) -> Self {
        let command = tree::find(name.path())
            .unwrap_or_else(|| panic!("{name}: no command at {:?}", name.path()));
        let root = tree::root();
        let supports_detail = command.get_arguments().any(|arg| arg.get_id() == "detail");
        let params: Vec<Param> = command
            .get_arguments()
            .filter(|arg| {
                let id = arg.get_id().as_str();
                let global = root.get_arguments().any(|root| root.get_id() == id);
                id != "help"
                    && id != "detail"
                    && !arg.is_hide_set()
                    && (!global || id == "json")
                    && !name.hidden().contains(&id)
            })
            .map(|arg| {
                let ty = ValueType::of(arg);
                let many = matches!(arg.get_action(), clap::ArgAction::Append)
                    || arg
                        .get_num_args()
                        .is_some_and(|range| range.max_values() > 1);
                let kind = if !arg.get_action().takes_values() {
                    ParamKind::Flag
                } else if many {
                    ParamKind::Many(ty)
                } else {
                    ParamKind::One(ty)
                };
                let required =
                    arg.is_required_set() || name.required().contains(&arg.get_id().as_str());
                Param {
                    arg,
                    kind,
                    required,
                }
            })
            .collect();
        let about = command
            .get_about()
            .map(ToString::to_string)
            .unwrap_or_default();
        let description = format!(
            "{about}. Returns what {} prints.",
            command_lines(name, supports_detail, &params)
        );
        let tool = Tool::new(name.to_string(), description, input_schema(name, &params))
            .with_annotations(name.effect().annotations());
        ToolSpec {
            tool,
            about,
            params,
            supports_detail,
        }
    }

    pub(crate) fn param(&self, id: &str) -> Option<&Param> {
        self.params.iter().find(|param| param.id() == id)
    }
}

fn input_schema(name: ToolName, params: &[Param]) -> Arc<JsonObject> {
    let item = |param: &Param, ty: ValueType| {
        let values: Vec<String> = param
            .arg
            .get_possible_values()
            .iter()
            .filter(|value| !value.is_hide_set())
            .map(|value| value.get_name().to_owned())
            .collect();
        if values.is_empty() {
            let (_, mut schema) = value_schema(param.arg);
            schema["type"] = ty.json_type().into();
            schema
        } else {
            json!({ "type": "string", "enum": values })
        }
    };
    let mut properties = JsonObject::new();
    let mut required = Vec::new();
    for param in params {
        let mut schema = match param.kind {
            ParamKind::Flag => json!({ "type": "boolean" }),
            ParamKind::One(ty) => item(param, ty),
            ParamKind::Many(ty) => json!({ "type": "array", "items": item(param, ty) }),
        };
        if let Some(help) = param.arg.get_long_help().or(param.arg.get_help()) {
            schema["description"] = Value::String(help.to_string());
        }
        if let ParamKind::One(ty) | ParamKind::Many(ty) = param.kind
            && let [default, ..] = param.arg.get_default_values()
        {
            let default = default.to_string_lossy();
            schema["default"] = match ty {
                ValueType::String => Value::String(default.into_owned()),
                ValueType::Integer | ValueType::Number => serde_json::from_str(&default)
                    .unwrap_or_else(|_| {
                        panic!("{name}: the default of {} is not a number", param.id())
                    }),
            };
        }
        if param.required {
            required.push(Value::String(param.id().to_owned()));
        }
        properties.insert(param.id().to_owned(), schema);
    }
    let mut schema = JsonObject::new();
    schema.insert("type".to_owned(), "object".into());
    schema.insert("properties".to_owned(), properties.into());
    schema.insert("required".to_owned(), required.into());
    schema.insert("additionalProperties".to_owned(), false.into());
    Arc::new(schema)
}

/// The command lines that a call of `name` runs, as its description and its
/// help name them.
fn command_lines(name: ToolName, detail: bool, params: &[Param]) -> String {
    let path = name.path().join(" ");
    let detail = if detail { " --detail" } else { "" };
    let default = format!("`snouty {path}{detail}`");
    if params.iter().any(|param| param.id() == "json") {
        format!("{default}, or with `json`, `snouty --json {path}`")
    } else {
        default
    }
}

pub(super) fn tool_help_tool() -> Tool {
    let names: Vec<String> = ToolName::ALL.iter().map(ToString::to_string).collect();
    let schema = json!({
        "type": "object",
        "properties": { TOOL_PARAM: { "type": "string", "enum": names } },
        "required": [TOOL_PARAM],
        "additionalProperties": false,
    });
    let Value::Object(schema) = schema else {
        unreachable!("the schema is an object")
    };
    Tool::new(
        TOOL_HELP,
        format!(
            "{TOOL_HELP_ABOUT}: what its command does, what its params mean, and the shape of \
             its output."
        ),
        Arc::new(schema),
    )
    .with_annotations(Effect::Reads.annotations())
}

/// The MCP render of a tool's help page, after a line that names the tool's
/// command.
pub(super) fn tool_help(mut params: JsonObject) -> Result<String> {
    let tool = match params.remove(TOOL_PARAM) {
        Some(Value::String(tool)) => tool,
        Some(_) => bail!("{TOOL_PARAM}: must be a string"),
        None => bail!("missing param: {TOOL_PARAM}"),
    };
    if let Some(key) = params.keys().next() {
        bail!("unknown param: {key}");
    }
    let name: ToolName = tool.parse()?;
    let spec = name.spec();
    Ok(format!(
        "The help of {name}, which runs {}.\n\n{}",
        command_lines(name, spec.supports_detail, &spec.params),
        name.help().mcp()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::object;
    use crate::time::DURATION_FORMAT;

    /// The hidden and required ids name real args, so a renamed arg cannot
    /// slip past the table.
    #[test]
    fn table_ids_name_real_args() {
        for name in ToolName::ALL {
            let command = tree::find(name.path()).unwrap();
            for id in name.hidden().iter().chain(name.required()) {
                assert!(
                    command.get_arguments().any(|arg| arg.get_id() == *id),
                    "{name}: no arg {id}"
                );
            }
        }
    }

    /// A param's description serves the command line and MCP, so it names
    /// no flag and no snouty command.
    #[test]
    fn param_descriptions_name_no_flags_or_commands() {
        for name in ToolName::ALL {
            let properties = name.spec().tool.input_schema["properties"]
                .as_object()
                .unwrap();
            for (param, schema) in properties {
                let description = schema["description"].as_str().unwrap_or_default();
                assert!(
                    !crate::testutils::names_a_flag_or_command(description),
                    "{name}.{param}: {description}"
                );
            }
        }
    }

    /// `detail` and the root's globals but `json` are the server's to set,
    /// every tool but `debug` and `runs_cancel` is read-only, and only
    /// `runs_cancel` is destructive.
    #[test]
    fn tools_expose_no_globals_and_only_debug_and_cancel_write() {
        for name in ToolName::ALL {
            let tool = &name.spec().tool;
            let properties = tool.input_schema["properties"].as_object().unwrap();
            for global in ["verbose", "settings", "profile", "help", "detail"] {
                assert!(!properties.contains_key(global), "{name}: {global}");
            }
            assert_eq!(
                properties.contains_key("json"),
                name != ToolName::DocsShow,
                "{name}"
            );
            for hidden in name.hidden() {
                assert!(!properties.contains_key(*hidden), "{name}: {hidden}");
            }
            let annotations = tool.annotations.as_ref().unwrap();
            let (read_only, destructive) = match name {
                ToolName::Debug => (false, Some(false)),
                ToolName::RunsCancel => (false, Some(true)),
                _ => (true, None),
            };
            assert_eq!(annotations.read_only_hint, Some(read_only), "{name}");
            assert_eq!(annotations.destructive_hint, destructive, "{name}");
            assert_eq!(annotations.open_world_hint, Some(false), "{name}");
        }
    }

    #[test]
    fn schemas_follow_the_clap_args() {
        let schema = |name: ToolName| &name.spec().tool.input_schema;
        let exec = schema(ToolName::RunsExec);
        assert_eq!(
            exec["required"],
            json!(["run_id", "input_hash", "vtime", "script"])
        );
        let search = schema(ToolName::RunsSearch);
        assert_eq!(search["properties"]["count"]["type"], "boolean");
        assert_eq!(search["properties"]["limit"]["minimum"], 1);
        assert_eq!(search["properties"]["limit"]["maximum"], SEARCH_MAX_LIMIT);
        let list = schema(ToolName::RunsList);
        assert_eq!(
            list["properties"]["status"]["enum"],
            json!([
                "starting",
                "in_progress",
                "completed",
                "cancelled",
                "incomplete",
                "unknown"
            ])
        );
        assert_eq!(list["properties"]["limit"]["type"], "integer");
        assert_eq!(list["properties"]["limit"]["minimum"], 1);
        assert_eq!(list["properties"]["limit"]["default"], 10);
        assert_eq!(list["properties"]["created_after"]["format"], "date-time");
        assert_eq!(list["properties"]["launcher"]["type"], "string");
        let logs = schema(ToolName::RunsLogs);
        assert_eq!(logs["properties"]["input_hash"]["type"], "string");
        assert_eq!(logs["properties"]["vtime"]["type"], "number");
        assert_eq!(list["additionalProperties"], false);
        let debug = schema(ToolName::Debug);
        assert_eq!(debug["required"], json!(["input_hash", "vtime"]));
        assert_eq!(debug["properties"]["param"]["type"], "array");
        assert_eq!(
            debug["properties"]["param"]["items"]["pattern"],
            KV_PAIR_PATTERN
        );
        let property = schema(ToolName::RunsProperty);
        assert_eq!(property["required"], json!(["run_id", "property_id"]));
        let usage = schema(ToolName::RunsUsage);
        assert_eq!(usage["properties"]["rollup"]["type"], "string");
        assert!(
            usage["properties"]["rollup"]["enum"].is_array(),
            "{usage:?}"
        );
        assert_eq!(
            usage["properties"]["after"]["anyOf"],
            json!([{ "format": "date" }, { "format": "date-time" }])
        );
        assert_eq!(usage["properties"]["utc"]["type"], "boolean");
        assert_eq!(usage["properties"]["limit"]["type"], "integer");
        let wait = schema(ToolName::RunsWait);
        assert_eq!(
            wait["properties"]["until"]["enum"],
            json!(["ready", "complete"])
        );
        assert_eq!(wait["properties"]["poll_interval"]["default"], "1m");
        assert_eq!(
            wait["properties"]["poll_interval"]["pattern"],
            DURATION_PATTERN
        );
        let events = schema(ToolName::RunsEvents);
        assert_eq!(events["required"], json!(["run_id", "matches"]));
        assert_eq!(events["properties"]["matches"]["type"], "array");
        assert_eq!(events["properties"]["limit"]["type"], "integer");
        assert!(events["properties"].get("query").is_none(), "{events:?}");
        let version = schema(ToolName::Version)["properties"].as_object().unwrap();
        assert_eq!(version.keys().collect::<Vec<_>>(), ["json"]);
        let tree = schema(ToolName::DocsTree);
        assert_eq!(tree["properties"]["depth"]["minimum"], 1);
    }

    /// The pattern matches a string exactly when it parses with
    /// [`BareUnit::Refused`], and a string that matches parses to the same
    /// duration with every [`BareUnit`]. Short strings over the duration
    /// alphabet reach every parser branch and do not overflow.
    #[hegel::test]
    fn duration_pattern_matches_what_parses(tc: hegel::TestCase) {
        let text = tc.draw(hegel::generators::from_regex(r"\s?[0-9hHmMsS.x]{0,8}\s?"));
        let pattern = regex::Regex::new(DURATION_PATTERN).unwrap();
        let parsed = HumanDuration::parse(&text, BareUnit::Refused).ok();
        assert_eq!(pattern.is_match(&text), parsed.is_some(), "{text:?}");
        if let Some(parsed) = parsed {
            for bare in [BareUnit::Minutes, BareUnit::Seconds] {
                assert_eq!(HumanDuration::parse(&text, bare), Ok(parsed), "{text:?}");
            }
        }
    }

    /// The table names every duration param, and the schema of each one has
    /// the unit pattern and a description that ends with the duration format.
    #[test]
    fn duration_params_need_a_unit() {
        let durations = [
            (ToolName::Debug, "duration"),
            (ToolName::RunsWait, "poll_interval"),
            (ToolName::RunsWait, "timeout"),
            (ToolName::RunsExec, "timeout"),
        ];
        for name in ToolName::ALL {
            for param in &name.spec().params {
                assert_eq!(
                    is_duration(param.arg),
                    durations.contains(&(name, param.id())),
                    "{name}.{}",
                    param.id()
                );
            }
        }
        for (name, id) in durations {
            let schema = &name.spec().tool.input_schema["properties"][id];
            assert_eq!(schema["pattern"], DURATION_PATTERN, "{name}.{id}");
            let description = schema["description"].as_str().unwrap();
            assert!(
                description.ends_with(DURATION_FORMAT),
                "{name}.{id}: {description}"
            );
        }
    }

    #[hegel::test]
    fn kv_pair_pattern_matches_what_parses(tc: hegel::TestCase) {
        let text = tc.draw(hegel::generators::from_regex("[a=]{0,4}"));
        let pattern = regex::Regex::new(KV_PAIR_PATTERN).unwrap();
        assert_eq!(
            pattern.is_match(&text),
            text.parse::<KVPair>().is_ok(),
            "{text:?}"
        );
    }

    #[test]
    fn tool_help_returns_the_help_page_of_each_tool() {
        for name in ToolName::ALL {
            let help = tool_help(object(json!({ "tool": name.to_string() }))).unwrap();
            assert!(help.ends_with(&name.help().mcp()), "{name}");
        }
        let help = tool_help(object(json!({ "tool": "runs_logs" }))).unwrap();
        assert!(
            help.starts_with(
                "The help of runs_logs, which runs `snouty runs logs --detail`, or with `json`, \
                 `snouty --json runs logs`."
            ),
            "{help}"
        );
        let cases = [
            (json!({}), "missing param: tool"),
            (json!({ "tool": 1 }), "tool: must be a string"),
            (json!({ "tool": "mcp" }), "unknown tool: mcp"),
            (json!({ "tool": "doctor", "x": 1 }), "unknown param: x"),
        ];
        for (call, error) in cases {
            let result = tool_help(object(call.clone()));
            let error_text = result.err().map(|e| e.to_string());
            assert_eq!(error_text.as_deref(), Some(error), "{call}");
        }
    }
}
