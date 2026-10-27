//! Snapshot tests of every command's `--help`, with insta. A change to a help
//! page, a flag or a subcommand must show up as a snapshot diff. Review a
//! change with `cargo insta review`, or accept every change with
//! `INSTA_UPDATE=always`.

use clap::{CommandFactory, ValueEnum};
use snouty::cli::Cli;
use snouty::features::Feature;
use snouty::help::{self, Attach};
use snouty::testutils::names_a_flag_or_command;

/// The width of `--help` off a terminal, where the layout is fixed.
const PIPE_WIDTH: usize = 80;

/// `snouty` as a user sees it off a terminal, with no unstable feature on.
/// Whether a gated command is listed depends on `SNOUTY_UNSTABLE_FEATURES`,
/// which a developer's shell can set.
fn features_off() -> clap::Command {
    let cli = help::attach(Cli::command(), Attach::Eager(PIPE_WIDTH));
    let cli = Feature::value_variants().iter().fold(cli, |cli, feature| {
        cli.mut_subcommand(feature.gated_command(), |command| command.hide(true))
    });
    at_pipe_width(cli)
}

fn at_pipe_width(command: clap::Command) -> clap::Command {
    command
        .term_width(PIPE_WIDTH)
        .mut_subcommands(at_pipe_width)
}

/// Collects the command path of `command` and of each subcommand. Hidden
/// commands are included.
fn collect(command: &clap::Command, path: &[String], paths: &mut Vec<Vec<String>>) {
    paths.push(path.to_vec());
    // clap adds a `help` subcommand that prints the same pages again.
    for sub in command
        .get_subcommands()
        .filter(|sub| sub.get_name() != "help")
    {
        let mut sub_path = path.to_vec();
        sub_path.push(sub.get_name().to_owned());
        collect(sub, &sub_path, paths);
    }
}

/// `snouty <path> --help` as it prints off a terminal, under a snapshot
/// named for the path, joined with `_`. A `-` in a name also becomes `_`.
#[test]
fn help() {
    let mut paths = Vec::new();
    collect(&Cli::command(), &[], &mut paths);
    insta::with_settings!({
        snapshot_path => "snapshots/help",
        prepend_module_to_snapshot => false,
        omit_expression => true,
    }, {
        for path in paths {
            let args = std::iter::once("snouty")
                .chain(path.iter().map(String::as_str))
                .chain(["--help"]);
            let err = features_off()
                .try_get_matches_from(args)
                .expect_err("--help prints the help");
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
            let name = std::iter::once("snouty".to_owned())
                .chain(path.iter().map(|part| part.replace('-', "_")))
                .collect::<Vec<_>>()
                .join("_");
            insta::assert_snapshot!(name, err.render().to_string());
        }
    });
}

/// What the `tool_help` MCP tool returns for each tool: the MCP render of its
/// command's help page, under a snapshot named for the tool.
#[test]
fn mcp_tool_help() {
    insta::with_settings!({
        snapshot_path => "snapshots/mcp_help",
        prepend_module_to_snapshot => false,
        omit_expression => true,
    }, {
        for tool in snouty::mcp::ToolName::ALL {
            insta::assert_snapshot!(tool.to_string(), tool.help().mcp());
        }
    });
}

/// An MCP render names each arg by its param, never by its flag, names no
/// snouty command, and shows each example as a tool call.
#[test]
fn mcp_tool_help_names_no_flags() {
    for tool in snouty::mcp::ToolName::ALL {
        for line in tool.help().mcp().lines() {
            assert!(!names_a_flag_or_command(line), "{tool}: {line}");
        }
    }
}
