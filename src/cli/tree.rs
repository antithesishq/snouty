//! `snouty`'s clap definition for lookups, by the help pages and the MCP
//! tools.

use std::sync::OnceLock;

use clap::{Command, CommandFactory};

use super::Cli;

/// The built definition, so each command also holds the global args of the
/// commands above it. It has no long help: a page that looks up a name
/// renders while clap builds the command that shows it.
pub(crate) fn root() -> &'static Command {
    static ROOT: OnceLock<Command> = OnceLock::new();
    ROOT.get_or_init(|| {
        let mut root = Cli::command();
        root.build();
        root
    })
}

/// The command at `path` below `snouty`.
pub(crate) fn find<S: AsRef<str>>(path: &[S]) -> Option<&'static Command> {
    path.iter().try_fold(root(), |command, part| {
        command.find_subcommand(part.as_ref())
    })
}

/// Applies `change` to the command at `path` below `command`.
pub(crate) fn mut_at(
    command: Command,
    path: &[&str],
    change: impl FnOnce(Command) -> Command,
) -> Command {
    match path.split_first() {
        None => change(command),
        Some((name, rest)) => command.mut_subcommand(name, |sub| mut_at(sub, rest, change)),
    }
}
