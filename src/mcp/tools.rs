//! The tool table, and the params and schema of each tool, which come from
//! the clap definition of its command.

use std::any::TypeId;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use clap::Arg;
use color_eyre::eyre::{Result, bail, eyre};
use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use serde_json::{Value, json};

use crate::cli::tree;
use crate::help::HelpPage;
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
/// its command, the tool name, its effect, the args that the tool leaves
/// out, and the args that the tool requires although the command does not.
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
    RunsList => "runs_list", Reads, hidden: ["detail"], required: [];
    RunsShow => "runs_show", Reads, hidden: ["web"], required: [];
    RunsWait => "runs_wait", Reads, hidden: [], required: [];
    RunsCancel => "runs_cancel", Destroys, hidden: [], required: [];
    RunsProperties => "runs_properties", Reads, hidden: ["detail"], required: [];
    RunsProperty => "runs_property", Reads, hidden: [], required: [];
    RunsStream => "runs_stream", Reads, hidden: [], required: [];
    RunsBuildLogs => "runs_build_logs", Reads, hidden: [], required: [];
    RunsLogs => "runs_logs", Reads, hidden: ["detail"], required: [];
    RunsSearch => "runs_search", Reads, hidden: ["follow", "detail"], required: [];
    RunsUsage => "runs_usage", Reads, hidden: [], required: [];
    // With no script, `runs exec` reads it from stdin, and a child's stdin is
    // null.
    RunsExec => "runs_exec", Reads, hidden: [], required: ["script"];
    // `query` repeats `match` as a positional, and the command needs one of
    // them.
    RunsEvents => "runs_events", Reads, hidden: ["query", "detail"], required: ["matches"];
    Debug => "debug", StartsSessions, hidden: [], required: [];
    DocsTree => "docs_tree", Reads, hidden: [], required: [];
    DocsShow => "docs_show", Reads, hidden: [], required: [];
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
            None if id == "json" => ArgRole::Implied,
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
    /// `--json`: every tool call runs in JSON mode.
    Implied,
    /// The root's args, which are the server's to set, and the args that
    /// the tool hides.
    Excluded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueType {
    String,
    Integer,
    Number,
}

impl ValueType {
    fn of(arg: &Arg) -> Self {
        let parsed = arg.get_value_parser().type_id();
        let integers = [
            TypeId::of::<usize>(),
            TypeId::of::<u64>(),
            TypeId::of::<NonZeroUsize>(),
            TypeId::of::<NonZeroU64>(),
        ];
        if integers.into_iter().any(|integer| parsed == integer) {
            ValueType::Integer
        } else if parsed == TypeId::of::<VTime>() {
            ValueType::Number
        } else {
            ValueType::String
        }
    }

    fn json_type(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Integer => "integer",
            ValueType::Number => "number",
        }
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
}

pub(crate) struct ToolSpec {
    pub(crate) tool: Tool,
    about: String,
    pub(crate) params: Vec<Param>,
}

impl ToolSpec {
    fn new(name: ToolName) -> Self {
        let command = tree::find(name.path())
            .unwrap_or_else(|| panic!("{name}: no command at {:?}", name.path()));
        let root = tree::root();
        let params: Vec<Param> = command
            .get_arguments()
            .filter(|arg| {
                let id = arg.get_id().as_str();
                id != "help"
                    && !arg.is_hide_set()
                    && root.get_arguments().all(|root| root.get_id() != id)
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
            "{about}. Returns what `snouty --json {}` prints.",
            name.path().join(" ")
        );
        let tool = Tool::new(name.to_string(), description, input_schema(name, &params))
            .with_annotations(name.effect().annotations());
        ToolSpec {
            tool,
            about,
            params,
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
            json!({ "type": ty.json_type() })
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
    Ok(format!(
        "The help of {name}, which runs `snouty --json {}`.\n\n{}",
        name.path().join(" "),
        name.help().mcp()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::object;

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

    /// The root's globals are the server's to set, every tool but `debug`
    /// and `runs_cancel` is read-only, and only `runs_cancel` is
    /// destructive.
    #[test]
    fn tools_expose_no_globals_and_only_debug_and_cancel_write() {
        for name in ToolName::ALL {
            let tool = &name.spec().tool;
            let properties = tool.input_schema["properties"].as_object().unwrap();
            for global in ["json", "verbose", "settings", "profile", "help"] {
                assert!(!properties.contains_key(global), "{name}: {global}");
            }
            for hidden in name.hidden() {
                assert!(!properties.contains_key(*hidden), "{name}: {hidden}");
            }
            let annotations = tool.annotations.as_ref().unwrap();
            let destructive = name == ToolName::RunsCancel;
            let read_only = name != ToolName::Debug && !destructive;
            assert_eq!(annotations.read_only_hint, Some(read_only), "{name}");
            if !read_only {
                assert_eq!(annotations.destructive_hint, Some(destructive), "{name}");
            }
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
        let list = schema(ToolName::RunsList);
        assert!(
            list["properties"]["status"]["description"]
                .as_str()
                .unwrap()
                .contains("completed")
        );
        assert_eq!(list["properties"]["limit"]["type"], "integer");
        assert_eq!(list["properties"]["limit"]["default"], 10);
        assert_eq!(list["properties"]["launcher"]["type"], "string");
        let logs = schema(ToolName::RunsLogs);
        assert_eq!(logs["properties"]["input_hash"]["type"], "string");
        assert_eq!(logs["properties"]["vtime"]["type"], "number");
        assert_eq!(list["additionalProperties"], false);
        let debug = schema(ToolName::Debug);
        assert_eq!(debug["required"], json!(["input_hash", "vtime"]));
        assert_eq!(debug["properties"]["param"]["type"], "array");
        let property = schema(ToolName::RunsProperty);
        assert_eq!(property["required"], json!(["run_id", "property_id"]));
        let usage = schema(ToolName::RunsUsage);
        assert_eq!(usage["properties"]["rollup"]["type"], "string");
        assert!(
            usage["properties"]["rollup"]["enum"].is_array(),
            "{usage:?}"
        );
        assert_eq!(usage["properties"]["utc"]["type"], "boolean");
        assert_eq!(usage["properties"]["limit"]["type"], "integer");
        let wait = schema(ToolName::RunsWait);
        assert_eq!(
            wait["properties"]["until"]["enum"],
            json!(["ready", "complete"])
        );
        assert_eq!(wait["properties"]["poll_interval"]["default"], "1m");
        let events = schema(ToolName::RunsEvents);
        assert_eq!(events["required"], json!(["run_id", "matches"]));
        assert_eq!(events["properties"]["matches"]["type"], "array");
        assert_eq!(events["properties"]["limit"]["type"], "integer");
        assert!(events["properties"].get("query").is_none(), "{events:?}");
        let stream = schema(ToolName::RunsStream);
        assert_eq!(stream["properties"]["property_ids"]["type"], "array");
        assert_eq!(schema(ToolName::Version)["properties"], json!({}));
    }

    #[test]
    fn tool_help_returns_the_help_page_of_each_tool() {
        for name in ToolName::ALL {
            let help = tool_help(object(json!({ "tool": name.to_string() }))).unwrap();
            assert!(help.ends_with(&name.help().mcp()), "{name}");
        }
        let help = tool_help(object(json!({ "tool": "runs_logs" }))).unwrap();
        assert!(
            help.starts_with("The help of runs_logs, which runs `snouty --json runs logs`."),
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
