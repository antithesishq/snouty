//! An example command line in a help page: snouty commands, perhaps in a
//! shell pipeline, whose values can be `<placeholders>`.

use std::ffi::OsStr;
use std::fmt;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use clap::builder::{TypedValueParser, ValueParser};
use clap::parser::ValueSource;
use clap::{Arg, ArgMatches, Command, CommandFactory};
use serde_json::Value;

use super::error;
use crate::cli::{Cli, tree};
use crate::mcp::{ArgRole, ParamKind, ToolName, ValueType};

/// The first snouty command of an example, parsed.
pub(super) struct Example {
    /// The path below `snouty` of the command.
    path: Vec<String>,
    /// The matches of the command's own args and of the globals.
    matches: ArgMatches,
    /// Whether the command reads from or writes to another command or a
    /// file.
    in_pipeline: bool,
}

/// Parses each snouty command of `text`. An example can span lines, each but the last
/// ending in `\`.
pub(super) fn parse(text: &str) -> askama::Result<Example> {
    let words = shlex::split(&text.replace("\\\n", " "))
        .ok_or_else(|| error(format!("{text}: not shell words")))?;
    let mut commands = words
        .split(|word| is_shell_operator(word))
        .filter_map(snouty_argv);
    let argv = commands
        .next()
        .ok_or_else(|| error(format!("{text}: runs no snouty command")))?;
    let (path, matches) = parse_argv(text, &argv)?;
    for argv in commands {
        parse_argv(text, &argv)?;
    }
    Ok(Example {
        path,
        matches,
        in_pipeline: words.iter().any(|word| is_shell_operator(word)),
    })
}

impl Example {
    /// The call of the tool whose command the example runs.
    pub(super) fn to_call(&self, text: &str) -> askama::Result<ToolCall> {
        let tool = ToolName::at(&self.path).ok_or_else(|| {
            error(format!(
                "{text}: `snouty {}` has no MCP tool",
                self.path.join(" ")
            ))
        })?;
        // A tool call has no stdin, and its output goes to no other command.
        if self.in_pipeline {
            return Err(error(format!("{text}: is a shell pipeline")));
        }
        let mut params = Vec::new();
        let command = tree::find(&self.path).expect("the example's command exists");
        for arg in command.get_arguments() {
            let id = arg.get_id().as_str();
            let Some(values) = from_command_line(&self.matches, id) else {
                continue;
            };
            let param = match tool.role(id) {
                ArgRole::Param(param) => param,
                ArgRole::Implied => continue,
                ArgRole::Excluded => {
                    return Err(error(format!("{text}: {id} is not a param of {tool}")));
                }
            };
            let value = match param.kind {
                ParamKind::Flag => ParamValue::Json(Value::Bool(true)),
                ParamKind::One(ty) => ParamValue::new(ty, values.last().expect("a value")),
                ParamKind::Many(ty) => {
                    ParamValue::List(values.iter().map(|v| ParamValue::new(ty, v)).collect())
                }
            };
            params.push((param.id(), value));
        }
        Ok(ToolCall { tool, params })
    }
}

/// A tool call as a help page writes it: `tool {"param": value, …}`, or
/// `tool` when the call has no params.
pub(super) struct ToolCall {
    tool: ToolName,
    params: Vec<(&'static str, ParamValue)>,
}

enum ParamValue {
    Json(Value),
    /// A `<placeholder>` where a number goes, which stays unquoted, as in
    /// `"vtime": <vtime>`.
    Placeholder(String),
    List(Vec<ParamValue>),
}

impl ParamValue {
    fn new(ty: ValueType, value: &str) -> Self {
        if ty != ValueType::String {
            if is_placeholder(value) {
                return ParamValue::Placeholder(value.to_owned());
            }
            if let Ok(number) = value.parse::<serde_json::Number>() {
                return ParamValue::Json(Value::Number(number));
            }
        }
        ParamValue::Json(Value::String(value.to_owned()))
    }
}

impl fmt::Display for ToolCall {
    /// `{call}` writes the call on one line. `{call:#}` puts one param on
    /// each line, indented two spaces, with the closing brace unindented.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tool)?;
        if self.params.is_empty() {
            return Ok(());
        }
        let (open, sep, close) = if f.alternate() {
            (" {\n  ", ",\n  ", "\n}")
        } else {
            (" {", ", ", "}")
        };
        for (i, (id, value)) in self.params.iter().enumerate() {
            let lead = if i == 0 { open } else { sep };
            write!(f, "{lead}{}: {value}", Value::String((*id).to_owned()))?;
        }
        f.write_str(close)
    }
}

impl fmt::Display for ParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParamValue::Json(value) => write!(f, "{value}"),
            ParamValue::Placeholder(placeholder) => f.write_str(placeholder),
            ParamValue::List(values) => {
                let values: Vec<String> = values.iter().map(ToString::to_string).collect();
                write!(f, "[{}]", values.join(", "))
            }
        }
    }
}

fn is_placeholder(value: &str) -> bool {
    value.starts_with('<') && value.ends_with('>')
}

/// Shell words that end the snouty part of an example: a pipe, a list
/// operator, or a redirect such as `>out`, `2>&1` or `<in`. A `<placeholder>`
/// is a value.
fn is_shell_operator(word: &str) -> bool {
    matches!(word, "|" | "|&" | "&" | "&&" | "||" | ";")
        || (!is_placeholder(word)
            && word
                .trim_start_matches(|c: char| c.is_ascii_digit())
                .starts_with(['<', '>']))
}

/// The argv of a snouty command, when `words` is one. `snouty` is optional
/// when the first word is a subcommand, as in `runs logs`.
fn snouty_argv(words: &[String]) -> Option<Vec<String>> {
    let first = words.first()?;
    if first == "snouty" {
        Some(words.to_vec())
    } else if tree::find(&[first]).is_some() {
        Some(
            std::iter::once("snouty".to_owned())
                .chain(words.iter().cloned())
                .collect(),
        )
    } else {
        None
    }
}

/// A value parser that takes a `<placeholder>` as any value, and checks
/// each other value with the arg's own parser.
#[derive(Clone)]
struct OrPlaceholder(ValueParser);

impl TypedValueParser for OrPlaceholder {
    type Value = String;

    fn parse_ref(
        &self,
        _command: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<String, clap::Error> {
        let text = value.to_string_lossy().into_owned();
        if !is_placeholder(&text) {
            // clap's `ValueParser::parse_ref` is private, so the value goes
            // through a command that has only this arg, alone, so that the
            // arg's relations to other args do not apply.
            let id = arg.map_or_else(|| clap::Id::from("value"), |arg| arg.get_id().clone());
            let check = Arg::new(id)
                .value_parser(self.0.clone())
                .allow_hyphen_values(true);
            Command::new("check")
                .arg(check)
                .try_get_matches_from([OsStr::new("check"), value])?;
        }
        Ok(text)
    }
}

/// `snouty`'s clap definition, in which each arg also takes a placeholder
/// such as `<vtime>`. A parse borrows it mutably, because cloning it for
/// each example would cost a millisecond on a page.
fn placeholder_root() -> MutexGuard<'static, Command> {
    fn or_placeholder(command: Command) -> Command {
        command
            .mut_args(|arg| {
                if arg.get_action().takes_values() {
                    let parser = OrPlaceholder(arg.get_value_parser().clone());
                    arg.value_parser(parser)
                } else {
                    arg
                }
            })
            .mut_subcommands(or_placeholder)
    }
    static ROOT: OnceLock<Mutex<Command>> = OnceLock::new();
    ROOT.get_or_init(|| Mutex::new(or_placeholder(Cli::command())))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Parses `argv` as snouty would. Returns the command path and its matches.
fn parse_argv(text: &str, argv: &[String]) -> askama::Result<(Vec<String>, ArgMatches)> {
    let mut leaf = placeholder_root()
        .try_get_matches_from_mut(argv)
        .map_err(|err| error(format!("{text}: {}", err.render().to_string().trim())))?;
    let mut path = Vec::new();
    while let Some((name, sub)) = leaf.remove_subcommand() {
        path.push(name);
        leaf = sub;
    }
    Ok((path, leaf))
}

/// The values of `id` that the command line gave, as text.
fn from_command_line(matches: &ArgMatches, id: &str) -> Option<Vec<String>> {
    if !matches.ids().any(|known| known == id)
        || matches.value_source(id) != Some(ValueSource::CommandLine)
    {
        return None;
    }
    let raw = matches.get_raw(id).into_iter().flatten();
    Some(
        raw.map(|value| value.to_string_lossy().into_owned())
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use hegel::generators;
    use rmcp::model::JsonObject;

    use super::*;
    use crate::mcp::{Globals, command_args};

    impl ToolCall {
        /// The call's params, which hold no placeholder.
        fn to_json(&self) -> JsonObject {
            fn json(value: &ParamValue) -> Value {
                match value {
                    ParamValue::Json(value) => value.clone(),
                    ParamValue::Placeholder(placeholder) => panic!("{placeholder}"),
                    ParamValue::List(values) => values.iter().map(json).collect(),
                }
            }
            self.params
                .iter()
                .map(|(id, value)| ((*id).to_owned(), json(value)))
                .collect()
        }
    }

    /// Each arg that a command line sets, and its values, down to the last
    /// subcommand.
    fn values(matches: &ArgMatches, command: &Command) -> Vec<(String, Vec<String>)> {
        let mut values: Vec<_> = command
            .get_arguments()
            .filter_map(|arg| {
                let id = arg.get_id().as_str();
                Some((id.to_owned(), from_command_line(matches, id)?))
            })
            .collect();
        if let Some((name, sub)) = matches.subcommand() {
            values.push((name.to_owned(), Vec::new()));
            values.extend(super::tests::values(
                sub,
                command.find_subcommand(name).unwrap(),
            ));
        }
        values
    }

    #[test]
    fn a_multiline_call_puts_one_param_on_each_line() {
        let text = "snouty runs list --limit <limit> --launcher nightly";
        let call = parse(text).unwrap().to_call(text).unwrap();
        assert_eq!(
            format!("{call:#}"),
            "runs_list {\n  \"launcher\": \"nightly\",\n  \"limit\": <limit>\n}"
        );
        let call = parse("snouty doctor").unwrap().to_call("").unwrap();
        assert_eq!(format!("{call:#}"), "doctor");
    }

    /// An example runs as its tool call does: the command line of the call
    /// parses as the example does, with `--detail` where a call without
    /// `json` adds it.
    #[hegel::test]
    fn an_example_runs_as_its_tool_call(tc: hegel::TestCase) {
        // A word that starts with `-` is a flag on the command line.
        let value = tc.draw(generators::text().exclude_characters("\0<>"));
        let value = shlex::try_quote(&format!("v{value}")).unwrap().into_owned();
        let list = format!("runs list --launcher {value} --limit 5 --status completed");
        // The bool: the call adds `--detail`, so the example gets it at its
        // end.
        let examples = [
            (format!("snouty {list}"), true),
            (format!("snouty --json {list}"), false),
            (
                format!("snouty runs list --detail --launcher {value}"),
                false,
            ),
            (
                format!("snouty runs exec r -1 1.5 --events --container c {value}"),
                true,
            ),
            // `--count` conflicts with `--detail`.
            (format!("snouty runs search r --count {value}"), false),
            (format!("snouty runs search r {value}"), true),
            (
                format!("snouty debug --run-id r --input-hash h --vtime 2.5 --param a={value}"),
                false,
            ),
            (format!("snouty docs search --offline {value} x"), false),
        ];
        let globals = Globals {
            settings: None,
            profile: None,
            verbose: false,
            max_concurrent_calls: NonZeroUsize::new(1).unwrap(),
        };
        for (text, adds_detail) in examples {
            let example = parse(&text).unwrap();
            let call = example.to_call(&text).unwrap();
            let argv = command_args(call.tool, &globals, call.to_json()).unwrap();
            let from_call = Cli::command()
                .try_get_matches_from(std::iter::once("snouty".into()).chain(argv))
                .unwrap();
            let runs = if adds_detail {
                format!("{text} --detail")
            } else {
                text.clone()
            };
            let words = shlex::split(&runs).unwrap();
            let from_example = Cli::command().try_get_matches_from(words).unwrap();
            assert_eq!(
                values(&from_call, tree::root()),
                values(&from_example, tree::root()),
                "{text}"
            );
        }
    }
}
