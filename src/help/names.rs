//! What `t()` names: a flag, a positional arg, a command or an example. A
//! name is looked up in the clap definition, so a page cannot name an arg or
//! a command that does not exist, and is then written for the page's reader.

use clap::Arg;

use super::example::{self, Example};
use super::{HelpPage, Reader, error};
use crate::cli::tree;
use crate::mcp::{ArgRole, ToolName};

pub(super) enum Name {
    Flag(&'static Arg),
    Positional(&'static Arg),
    /// A command, perhaps with flags that take no value, such as
    /// `runs properties --detail`.
    Command(Vec<String>),
    Example(Example),
}

impl Name {
    /// The name of `text` on `page`: a flag or a positional arg of the
    /// page's command, or else a command line.
    pub(super) fn resolve(page: HelpPage, text: &str) -> askama::Result<Name> {
        let command = tree::find(page.path())
            .unwrap_or_else(|| panic!("{page}: no command at {:?}", page.path()));
        if text.starts_with('-') {
            let short = text.strip_prefix('-').and_then(|short| {
                let mut chars = short.chars();
                chars.next().filter(|_| chars.next().is_none())
            });
            return command
                .get_arguments()
                .find(|arg| match text.strip_prefix("--") {
                    Some(long) => arg.get_long() == Some(long),
                    None => short.is_some() && arg.get_short() == short,
                })
                .map(Name::Flag)
                .ok_or_else(|| error(format!("{text}: `{}` has no such flag", name(page))));
        }
        if let Some(arg) = command.get_positionals().find(|arg| arg.get_id() == text) {
            return Ok(Name::Positional(arg));
        }
        // A line that lacks a required arg can still name a command.
        match example::parse(text) {
            Ok(example) => Ok(Name::Example(example)),
            Err(err) => reference(text)?.map(Name::Command).ok_or(err),
        }
    }

    /// The text that names `self` to the reader of `page`. On the command
    /// line, a name stays as `text` writes it, but a positional arg is its
    /// value name. Over MCP, an arg is its param, a command its tool, and an
    /// example a call of its tool.
    pub(super) fn write(
        &self,
        page: HelpPage,
        reader: Reader,
        text: &str,
    ) -> askama::Result<String> {
        match (self, reader) {
            (Name::Positional(arg), Reader::Cli) => Ok(arg
                .get_value_names()
                .and_then(|names| names.first())
                .map_or_else(|| arg.get_id().as_str().to_uppercase(), ToString::to_string)),
            (Name::Flag(_) | Name::Command(_) | Name::Example(_), Reader::Cli) => {
                Ok(text.to_owned())
            }
            (Name::Flag(arg) | Name::Positional(arg), Reader::Mcp) => {
                let tool = ToolName::at(page.path()).ok_or_else(|| {
                    error(format!(
                        "`{}` has no MCP tool; put this text in {{% if cli %}}",
                        name(page)
                    ))
                })?;
                match tool.role(arg.get_id().as_str()) {
                    ArgRole::Param(param) => Ok(format!("`{}`", param.id())),
                    ArgRole::Implied | ArgRole::Excluded => {
                        Err(error(format!("{} is not a param of {tool}", arg.get_id())))
                    }
                }
            }
            (Name::Command(path), Reader::Mcp) => ToolName::at(path)
                .map(|tool| tool.to_string())
                .ok_or_else(|| {
                    error(format!(
                        "{text}: `snouty {}` has no MCP tool",
                        path.join(" ")
                    ))
                }),
            (Name::Example(example), Reader::Mcp) => Ok(example.to_call(text)?.to_string()),
        }
    }
}

/// `snouty` and the path of the page's command.
fn name(page: HelpPage) -> String {
    std::iter::once("snouty")
        .chain(page.path().iter().copied())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The command path of `text` when it is a reference: subcommands, then
/// flags that take no value.
fn reference(text: &str) -> askama::Result<Option<Vec<String>>> {
    let Some(mut words) = shlex::split(text) else {
        return Ok(None);
    };
    if words.first().is_some_and(|word| word == "snouty") {
        words.remove(0);
    }
    let split = words
        .iter()
        .position(|word| word.starts_with('-'))
        .unwrap_or(words.len());
    let (path, flags) = words.split_at(split);
    let Some(command) = tree::find(path).filter(|_| !path.is_empty()) else {
        return Ok(None);
    };
    for flag in flags {
        let Some(long) = flag.strip_prefix("--") else {
            return Ok(None);
        };
        match command
            .get_arguments()
            .find(|arg| arg.get_long() == Some(long))
        {
            Some(arg) if !arg.get_action().takes_values() => {}
            Some(_) => return Ok(None),
            None => return Err(error(format!("{text}: no flag {flag}"))),
        }
    }
    Ok(Some(path.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(page: HelpPage, reader: Reader, text: &str) -> Result<String, String> {
        Name::resolve(page, text)
            .and_then(|name| name.write(page, reader, text))
            .map_err(|err| err.to_string())
    }

    #[test]
    fn a_lone_dash_is_no_flag() {
        let result = t(HelpPage::RunsLogs, Reader::Cli, "-");
        assert!(result.is_err(), "{result:?}");
    }

    /// An unknown name is an error for every reader, so the CLI render
    /// cannot print it as written.
    #[test]
    fn an_unknown_name_is_an_error() {
        for text in [
            "--nosuch",
            "nosuch search",
            "runs nosuch",
            "snouty runs list --nosuch 5",
        ] {
            for reader in [Reader::Cli, Reader::Mcp] {
                let result = t(HelpPage::RunsExec, reader, text);
                assert!(result.is_err(), "{text} for {reader:?}: {result:?}");
            }
        }
    }

    /// A literal value must parse, and a placeholder can stand for any
    /// value.
    #[test]
    fn a_literal_value_must_parse() {
        for text in [
            "snouty runs wait <run_id> --until never",
            "snouty runs list --limit five",
            "snouty runs logs <run_id> <hash> soon",
        ] {
            let result = t(HelpPage::RunsLogs, Reader::Cli, text);
            assert!(
                result.is_err_and(|err| err.contains("invalid value")),
                "{text}"
            );
        }
        let call = t(
            HelpPage::RunsList,
            Reader::Mcp,
            "snouty runs list --limit <limit>",
        );
        assert_eq!(call.as_deref(), Ok(r#"runs_list {"limit": <limit>}"#));
    }

    #[test]
    fn names_are_written_for_the_reader() {
        let cases = [
            ("--events", "--events", "`events`"),
            ("input_hash", "INPUT_HASH", "`input_hash`"),
            ("runs logs", "runs logs", "runs_logs"),
            (
                "runs properties --detail",
                "runs properties --detail",
                "runs_properties",
            ),
            (
                "snouty doctor --offline",
                "snouty doctor --offline",
                r#"doctor {"offline": true}"#,
            ),
        ];
        for (text, cli, mcp) in cases {
            assert_eq!(t(HelpPage::RunsExec, Reader::Cli, text).as_deref(), Ok(cli));
            assert_eq!(t(HelpPage::RunsExec, Reader::Mcp, text).as_deref(), Ok(mcp));
        }
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

    /// A number placeholder stands where the number goes.
    #[test]
    fn a_number_placeholder_is_unquoted() {
        let call = t(
            HelpPage::RunsLogs,
            Reader::Mcp,
            "snouty runs logs <run_id> <hash> <vtime>",
        );
        assert_eq!(
            call.as_deref(),
            Ok(r#"runs_logs {"run_id": "<run_id>", "input_hash": "<hash>", "vtime": <vtime>}"#)
        );
    }
}
