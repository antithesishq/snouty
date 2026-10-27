//! Snapshot tests of every command's `--help`, with insta. A change to a help
//! page, a flag or a subcommand must show up as a snapshot diff. Review a
//! change with `cargo insta review`, or accept every change with
//! `INSTA_UPDATE=always`.

use clap::{CommandFactory, ValueEnum};
use snouty::cli::Cli;
use snouty::features::Feature;
use snouty::help::HelpPage;

/// The width of `--help` off a terminal, where the layout is fixed.
const PIPE_WIDTH: usize = 80;

/// `snouty` as a user sees it with no unstable feature on. Whether a gated
/// command is listed depends on `SNOUTY_UNSTABLE_FEATURES`, which a
/// developer's shell can set.
fn features_off() -> clap::Command {
    let mut cli = snouty::cli::command();
    // A long help is set when its command is built, wrapped to the terminal.
    // at_pipe_width sets it again at a fixed width.
    cli.build();
    Feature::value_variants().iter().fold(cli, |cli, feature| {
        cli.mut_subcommand(feature.gated_command(), |command| command.hide(true))
    })
}

/// Lays `command` and its subcommands out at [`PIPE_WIDTH`], as `--help`
/// prints off a terminal. The long help wraps when the command is built, to
/// the width of the terminal on stdout, so each long help is set again here.
fn at_pipe_width(mut command: clap::Command) -> clap::Command {
    let page = command
        .get_long_about()
        .map(ToString::to_string)
        .and_then(|about| HelpPage::ALL.into_iter().find(|page| page.cli() == about));
    if let Some(page) = page {
        command = command.long_about(page.wrapped(PIPE_WIDTH));
    }
    let names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_owned())
        .collect();
    for name in names {
        command = command.mut_subcommand(name, at_pipe_width);
    }
    command.term_width(PIPE_WIDTH)
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
            let err = at_pipe_width(features_off())
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

/// Every page is the long help of exactly one command, so no template is
/// orphaned and no command shows another command's help.
#[test]
fn every_page_is_one_commands_long_help() {
    fn collect(command: &clap::Command, abouts: &mut Vec<String>) {
        for sub in command.get_subcommands() {
            if let Some(about) = sub.get_long_about() {
                abouts.push(about.to_string());
            }
            collect(sub, abouts);
        }
    }
    let mut abouts = Vec::new();
    let mut cli = snouty::cli::command();
    // A long help is set when its command is built.
    cli.build();
    collect(&cli, &mut abouts);
    for page in HelpPage::ALL {
        let uses = abouts.iter().filter(|about| **about == page.cli()).count();
        assert_eq!(uses, 1, "{page} is the long help of {uses} commands");
    }
    assert_eq!(
        abouts.len(),
        HelpPage::ALL.len(),
        "a long help is not a page"
    );
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

/// An MCP render names each arg by its param, never by its flag, and shows
/// each example as a tool call.
#[test]
fn mcp_tool_help_names_no_flags() {
    let flag = regex::Regex::new(r"(^|[\s(`])--[a-z]").unwrap();
    for tool in snouty::mcp::ToolName::ALL {
        for line in tool.help().mcp().lines() {
            assert!(!flag.is_match(line), "{tool}: {line}");
        }
    }
}
