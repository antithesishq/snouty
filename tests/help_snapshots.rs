//! Snapshot tests of every command's `--help` and every MCP tool text, with
//! insta. A change to a help page, a flag or a subcommand must show up as a
//! snapshot diff. Review a change with `cargo insta review`, or accept every
//! change with `INSTA_UPDATE=always`.

use clap::CommandFactory;
use snouty::cli::Cli;
use snouty::help::{HelpPage, Target};
use snouty::mcp::ToolName;

/// The width of `--help` off a terminal, where the layout is fixed.
const PIPE_WIDTH: usize = 80;

/// `snouty` as a user sees it with no unstable feature on. Whether a gated
/// command is listed depends on `SNOUTY_UNSTABLE_FEATURES`, which a
/// developer's shell can set.
fn features_off() -> clap::Command {
    Cli::command().mut_subcommand("mcp", |mcp| mcp.hide(true))
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
    collect(&Cli::command(), &mut abouts);
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

/// Snapshots each MCP text as `tests/snapshots/help/<dir>/<name>.snap`. An
/// agent gets the text unwrapped, so the snapshot is unwrapped too.
fn assert_mcp_snapshots(dir: &str, texts: impl Iterator<Item = (ToolName, &'static str)>) {
    insta::with_settings!({
        snapshot_path => format!("snapshots/help/{dir}"),
        prepend_module_to_snapshot => false,
        omit_expression => true,
    }, {
        for (tool, text) in texts {
            insta::assert_snapshot!(tool.to_string(), text);
        }
    });
}

#[test]
fn mcp_tool_help() {
    assert_mcp_snapshots(
        "mcp",
        ToolName::ALL
            .into_iter()
            .filter_map(|tool| Some((tool, tool.page()?.text(Target::Mcp)))),
    );
}

#[test]
fn mcp_tool_descriptions() {
    assert_mcp_snapshots(
        "mcp_tools",
        ToolName::ALL
            .into_iter()
            .map(|tool| (tool, tool.description())),
    );
}

/// The MCP texts of each tool: its description and its full help.
fn mcp_texts(tool: ToolName) -> impl Iterator<Item = &'static str> {
    std::iter::once(tool.description()).chain(tool.page().map(|page| page.text(Target::Mcp)))
}

/// An MCP tool text tells an agent how to call the tool and read its JSON.
/// It must not document flags, command lines, or human output.
/// `snouty login` is the exception: the user runs it to store credentials.
#[test]
fn mcp_help_is_cli_free() {
    const FORBIDDEN: [&str; 3] = ["--", "snouty ", "human"];
    for tool in ToolName::ALL {
        for text in mcp_texts(tool) {
            let text = text.replace("`snouty login`", "");
            for word in FORBIDDEN {
                assert!(
                    !text.contains(word),
                    "MCP help for {tool} contains {word:?}"
                );
            }
        }
    }
}

/// Clients cap a tool description at 1 KiB, and every listed description
/// is sent on each request.
#[test]
fn mcp_tool_descriptions_are_short() {
    const MAX_BYTES: usize = 1024;
    for tool in ToolName::ALL {
        let len = tool.description().len();
        assert!(len <= MAX_BYTES, "{tool} description is {len} bytes");
    }
}

/// A description that points to tool_help names its own tool, so the agent
/// gets the right page.
#[test]
fn mcp_tool_descriptions_point_to_their_own_help() {
    let pointer = regex::Regex::new(r#"tool_help \{"tool": "(\w+)"\}"#).unwrap();
    for tool in ToolName::ALL {
        for found in pointer.captures_iter(tool.description()) {
            assert_eq!(&found[1], tool.to_string(), "{tool} points to other help");
        }
    }
}
