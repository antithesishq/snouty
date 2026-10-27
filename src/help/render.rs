//! Defines `t()`, the one helper of the help templates, which names a flag,
//! a positional arg or a command. `t()` looks each name up in the clap
//! definition, so a page cannot name an arg or a command that does not exist,
//! and it writes the name for the reader of the page.

use std::ffi::OsStr;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use askama::Error;
use clap::parser::ValueSource;
use clap::{Arg, ArgMatches, Command, CommandFactory};
use serde_json::Value;

use super::HelpPage;
use crate::cli::Cli;
use crate::mcp::{self, ToolName};

/// Who reads a render of a help page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reader {
    /// `--help` on the command line.
    Cli,
    /// The `tool_help` MCP tool, whose reader passes params by name.
    Mcp,
}

/// One line for each MCP tool: its name and what it does.
pub(super) fn tool_table() -> String {
    let mut rows: Vec<(String, String)> = ToolName::ALL
        .into_iter()
        .map(|tool| {
            let command = command_at(tool.path()).expect("a tool's command exists");
            let about = command
                .get_about()
                .map(ToString::to_string)
                .unwrap_or_default();
            (tool.to_string(), about)
        })
        .collect();
    rows.push(("tool_help".to_owned(), mcp::TOOL_HELP_ABOUT.to_owned()));
    let width = rows.iter().map(|(name, _)| name.len()).max().unwrap_or(0);
    rows.iter()
        .map(|(name, about)| format!("  {name:width$}  {about}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `snouty`'s clap definition, with no long help. It is not built: a
/// lookup needs only the args that each command defines.
fn root() -> &'static Command {
    static ROOT: OnceLock<Command> = OnceLock::new();
    ROOT.get_or_init(Cli::command)
}

/// [`root`] with every value parsed as a string, so that an example can hold
/// a placeholder such as `<vtime>`. Each literal value is checked against the
/// real parser afterwards.
fn placeholder_root() -> MutexGuard<'static, Command> {
    static ROOT: OnceLock<Mutex<Command>> = OnceLock::new();
    ROOT.get_or_init(|| Mutex::new(any_value(Cli::command())))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

fn any_value(command: Command) -> Command {
    command
        .mut_args(|arg| {
            if arg.get_action().takes_values() {
                arg.value_parser(clap::builder::ValueParser::string())
            } else {
                arg
            }
        })
        .mut_subcommands(any_value)
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

/// The command at `path` below `snouty`.
fn command_at<S: AsRef<std::ffi::OsStr>>(path: &[S]) -> Option<&'static Command> {
    path.iter()
        .try_fold(root(), |command, part| command.find_subcommand(part))
}

/// The args of the command at `path`, and the global args of the commands
/// above it, which clap gives the command when it builds it.
fn args_at<S: AsRef<std::ffi::OsStr>>(path: &[S]) -> Vec<&'static Arg> {
    let command = command_at(path).expect("the command exists");
    let globals = (0..path.len())
        .filter_map(|depth| command_at(&path[..depth]))
        .flat_map(Command::get_arguments)
        .filter(|arg| arg.is_global_set());
    command.get_arguments().chain(globals).collect()
}

fn error(message: String) -> Error {
    Error::custom(message)
}

/// The page that a template renders, and its reader.
#[derive(Clone, Copy, Debug)]
pub(super) struct Context {
    pub(super) page: HelpPage,
    pub(super) reader: Reader,
}

impl Context {
    pub(super) fn is_cli(self) -> bool {
        self.reader == Reader::Cli
    }

    /// `t("--events")` is a flag of the page's command, `t("input_hash")` a
    /// positional arg of it, and anything else a command line.
    pub(super) fn t(self, text: &str) -> Result<String, Error> {
        if text.starts_with('-') {
            return self.flag(text);
        }
        if !text.contains(char::is_whitespace)
            && let Some(arg) = self
                .command()
                .get_positionals()
                .find(|arg| arg.get_id() == text)
        {
            return self.positional(arg);
        }
        self.command_line(text)
    }

    fn command(self) -> &'static Command {
        command_at(self.page.path())
            .unwrap_or_else(|| panic!("{}: no command at {:?}", self.page, self.page.path()))
    }

    fn command_name(self) -> String {
        std::iter::once("snouty")
            .chain(self.page.path().iter().copied())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn flag(self, text: &str) -> Result<String, Error> {
        let short = text.strip_prefix('-').and_then(|short| {
            let mut chars = short.chars();
            chars.next().filter(|_| chars.next().is_none())
        });
        let arg = args_at(self.page.path())
            .into_iter()
            .find(|arg| match text.strip_prefix("--") {
                Some(long) => arg.get_long() == Some(long),
                None => short.is_some() && arg.get_short() == short,
            })
            .ok_or_else(|| {
                error(format!(
                    "{text}: `{}` has no such flag",
                    self.command_name()
                ))
            })?;
        match self.reader {
            Reader::Cli => Ok(text.to_owned()),
            Reader::Mcp => Ok(format!("`{}`", self.param(arg)?)),
        }
    }

    fn positional(self, arg: &Arg) -> Result<String, Error> {
        match self.reader {
            Reader::Cli => Ok(arg
                .get_value_names()
                .and_then(|names| names.first())
                .map_or_else(|| arg.get_id().as_str().to_uppercase(), ToString::to_string)),
            Reader::Mcp => Ok(format!("`{}`", self.param(arg)?)),
        }
    }

    fn param(self, arg: &Arg) -> Result<String, Error> {
        let tool = self.tool()?;
        let id = arg.get_id().as_str();
        if !tool.is_param(arg) {
            return Err(error(format!("{id} is not a param of {tool}")));
        }
        Ok(id.to_owned())
    }

    fn tool(self) -> Result<ToolName, Error> {
        tool_at(self.page.path()).ok_or_else(|| {
            error(format!(
                "`{}` has no MCP tool; put this text in {{% if cli %}}",
                self.command_name()
            ))
        })
    }

    /// A command line: a command, or an example with its args. On the command
    /// line it stays as written. Over MCP, a command is its tool's name and an
    /// example is a call of its tool.
    fn command_line(self, text: &str) -> Result<String, Error> {
        // An example can span lines, each but the last ending in `\`.
        let words = shlex::split(&text.replace("\\\n", " "))
            .ok_or_else(|| error(format!("{text}: not shell words")))?;
        let mut segments = words.split(|word| is_shell_operator(word));
        let first = segments.next().unwrap_or_default();
        let call = snouty_argv(first)
            .map(|argv| parse(text, &argv))
            .transpose()?;
        for segment in segments {
            if let Some(argv) = snouty_argv(segment) {
                parse(text, &argv)?;
            }
        }
        match self.reader {
            Reader::Cli => Ok(text.to_owned()),
            Reader::Mcp => {
                let (path, matches) =
                    call.ok_or_else(|| error(format!("{text}: does not start with snouty")))?;
                // A tool call has no stdin, and its output goes to no other
                // command.
                if words.iter().any(|word| is_shell_operator(word)) {
                    return Err(error(format!("{text}: is a shell pipeline")));
                }
                mcp_call(text, &path, &matches)
            }
        }
    }
}

fn tool_at(path: &[&str]) -> Option<ToolName> {
    ToolName::ALL.into_iter().find(|tool| tool.path() == path)
}

/// The argv of a snouty command, when `words` is one. `snouty` is optional
/// when the first word is a subcommand, as in `runs logs`.
fn snouty_argv(words: &[String]) -> Option<Vec<String>> {
    let first = words.first()?;
    if first == "snouty" {
        Some(words.to_vec())
    } else if root().find_subcommand(first).is_some() {
        Some(
            std::iter::once("snouty".to_owned())
                .chain(words.iter().cloned())
                .collect(),
        )
    } else {
        None
    }
}

/// Parses `argv` as snouty would, and checks each literal value with the
/// real value parser. Returns the subcommand path and its matches.
///
/// A line that lacks a required arg can still be a reference: a command and
/// flags with no values, such as `runs properties --detail`. Its flags must
/// exist, and over MCP they drop out.
fn parse(text: &str, argv: &[String]) -> Result<(Vec<String>, ArgMatches), Error> {
    let parsed = placeholder_root().try_get_matches_from_mut(argv);
    let matches = match parsed {
        Ok(matches) => matches,
        Err(err) => {
            if err.kind() == clap::error::ErrorKind::MissingRequiredArgument
                && let Some(path) = reference(text, &argv[1..])?
            {
                return Ok((path, ArgMatches::default()));
            }
            return Err(error(format!(
                "{text}: {}",
                err.render().to_string().trim()
            )));
        }
    };
    let mut path = Vec::new();
    let mut leaf = matches;
    while let Some((name, sub)) = leaf.remove_subcommand() {
        path.push(name);
        leaf = sub;
    }
    for arg in args_at(&path) {
        let id = arg.get_id().as_str();
        let Some(values) = from_command_line(&leaf, id) else {
            continue;
        };
        for value in values.filter(|value| !is_placeholder(value)) {
            check_value(arg, &value)
                .map_err(|err| error(format!("{text}: {}", err.render().to_string().trim())))?;
        }
    }
    Ok((path, leaf))
}

/// Parses `value` with the value parser of `arg`, alone, so that the arg's
/// relations to other args do not apply.
fn check_value(arg: &Arg, value: &str) -> Result<(), clap::Error> {
    let check = Arg::new(arg.get_id())
        .value_parser(arg.get_value_parser().clone())
        .allow_hyphen_values(true);
    Command::new("check")
        .arg(check)
        .try_get_matches_from([OsStr::new("check"), OsStr::new(value)])
        .map(drop)
}

/// The command path of `words` when they are a reference: subcommands, then
/// flags that take no value.
fn reference(text: &str, words: &[String]) -> Result<Option<Vec<String>>, Error> {
    let split = words
        .iter()
        .position(|word| word.starts_with('-'))
        .unwrap_or(words.len());
    let (path, flags) = words.split_at(split);
    if command_at(path).is_none() {
        return Ok(None);
    }
    let args = args_at(path);
    for flag in flags {
        let Some(long) = flag.strip_prefix("--") else {
            return Ok(None);
        };
        let arg = args.iter().find(|arg| arg.get_long() == Some(long));
        match arg {
            Some(arg) if mcp::is_flag(arg) => {}
            Some(_) => return Ok(None),
            None => return Err(error(format!("{text}: no flag {flag}"))),
        }
    }
    Ok(Some(path.to_vec()))
}

/// The values of `id` that the command line gave, as text.
fn from_command_line(matches: &ArgMatches, id: &str) -> Option<impl Iterator<Item = String>> {
    if !matches.ids().any(|known| known == id)
        || matches.value_source(id) != Some(ValueSource::CommandLine)
    {
        return None;
    }
    let raw = matches.get_raw(id).into_iter().flatten();
    Some(raw.map(|value| value.to_string_lossy().into_owned()))
}

/// `tool {"param": value, …}`, or `tool` when the call has no params.
fn mcp_call(text: &str, path: &[String], matches: &ArgMatches) -> Result<String, Error> {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    let tool = tool_at(&path).ok_or_else(|| {
        error(format!(
            "{text}: `snouty {}` has no MCP tool",
            path.join(" ")
        ))
    })?;
    let mut params = Vec::new();
    for arg in args_at(&path) {
        let id = arg.get_id().as_str();
        // Every tool call runs in JSON mode.
        if id == "json" {
            continue;
        }
        let Some(values) = from_command_line(matches, id) else {
            continue;
        };
        if !tool.is_param(arg) {
            return Err(error(format!("{text}: {id} is not a param of {tool}")));
        }
        let value = if mcp::is_flag(arg) {
            Value::Bool(true)
        } else {
            let mut values: Vec<Value> = values.map(|value| json_value(arg, value)).collect();
            if mcp::takes_many(arg) {
                Value::Array(values)
            } else {
                values.pop().unwrap_or(Value::Null)
            }
        };
        params.push(format!(
            "{}: {}",
            Value::String(id.to_owned()),
            spaced(&value)
        ));
    }
    if params.is_empty() {
        Ok(tool.to_string())
    } else {
        Ok(format!("{tool} {{{}}}", params.join(", ")))
    }
}

/// JSON with a space after each comma of an array, as in the params.
fn spaced(value: &Value) -> String {
    match value {
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(Value::to_string).collect();
            format!("[{}]", items.join(", "))
        }
        value => value.to_string(),
    }
}

/// A number arg's literal value is a JSON number, and a placeholder or any
/// other value is a string.
fn json_value(arg: &Arg, value: String) -> Value {
    if mcp::value_type(arg) != "string"
        && !is_placeholder(&value)
        && let Ok(number) = value.parse::<serde_json::Number>()
    {
        return Value::Number(number);
    }
    Value::String(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(page: HelpPage, reader: Reader, text: &str) -> Result<String, String> {
        Context { page, reader }
            .t(text)
            .map_err(|err| err.to_string())
    }

    #[test]
    fn a_lone_dash_is_no_flag() {
        let result = t(HelpPage::RunsLogs, Reader::Cli, "-");
        assert!(result.is_err(), "{result:?}");
    }

    /// A global that a parent command declares is a param, and a root global
    /// is the server's.
    #[test]
    fn globals_are_params_only_below_the_root() {
        let call = t(
            HelpPage::DocsSearch,
            Reader::Mcp,
            "snouty docs search --offline x",
        );
        assert_eq!(
            call.as_deref(),
            Ok(r#"docs_search {"query": ["x"], "offline": true}"#)
        );
        let call = t(
            HelpPage::RunsLogs,
            Reader::Mcp,
            "snouty runs logs --profile prod <run_id> <hash>",
        );
        assert!(call.is_err_and(|err| err.contains("profile is not a param")));
    }

    #[test]
    fn a_redirect_has_no_mcp_form() {
        for example in [
            "snouty docs search x >out.txt",
            "snouty docs search x 2>/dev/null",
            "snouty docs search x 2>&1",
        ] {
            let call = t(HelpPage::DocsSearch, Reader::Mcp, example);
            assert!(call.is_err_and(|err| err.contains("pipeline")), "{example}");
        }
        let call = t(
            HelpPage::RunsLogs,
            Reader::Mcp,
            "snouty runs logs <run_id> <hash>",
        );
        assert!(call.is_ok(), "{call:?}");
    }
}
