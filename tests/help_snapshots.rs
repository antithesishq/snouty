//! Snapshot tests for every command's long help and every MCP tool text. A change to a help page must show up as a fixture diff. Run
//! with `SNOUTY_BLESS=1` to rewrite the fixtures.

use std::collections::BTreeMap;
use std::path::Path;

use clap::CommandFactory;
use snouty::cli::Cli;
use snouty::help::Target;
use snouty::mcp::ToolName;

const BLESS_ENV: &str = "SNOUTY_BLESS";

/// Collects each long help under its fixture name: the command path below
/// `snouty`, joined with `_`. A `-` in a command name also becomes `_`.
/// Hidden commands are included, because `hide` does not remove a command.
fn collect(command: &clap::Command, prefix: &str, pages: &mut BTreeMap<String, String>) {
    for sub in command.get_subcommands() {
        let name = sub.get_name().replace('-', "_");
        let path = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}_{name}")
        };
        if let Some(about) = sub.get_long_about() {
            pages.insert(path.clone(), about.to_string());
        }
        collect(sub, &path, pages);
    }
}

/// Compares each page to `tests/fixtures/help/<target>/<name>.txt`, or
/// rewrites the fixtures when `SNOUTY_BLESS` is set.
fn check_fixtures(target: &str, pages: &BTreeMap<String, String>) {
    assert!(!pages.is_empty(), "no {target} help pages");

    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/help")
        .join(target);
    let bless = std::env::var_os(BLESS_ENV).is_some();
    if bless {
        std::fs::create_dir_all(&dir).unwrap();
    }

    let mut failures = Vec::new();
    for (name, text) in pages {
        let file = dir.join(format!("{name}.txt"));
        if bless {
            std::fs::write(&file, text).unwrap();
            continue;
        }
        match std::fs::read_to_string(&file) {
            Ok(expected) if expected == *text => {}
            Ok(_) => failures.push(format!("{name}: help differs from {}", file.display())),
            Err(err) => failures.push(format!("{name}: cannot read {}: {err}", file.display())),
        }
    }

    // A fixture with no page is stale: its page was renamed or removed.
    for entry in std::fs::read_dir(&dir).unwrap() {
        let file = entry.unwrap().path();
        // Only `.txt` files are fixtures. Bless must not delete other files.
        if file.extension().is_none_or(|ext| ext != "txt") {
            continue;
        }
        let Some(name) = file.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if !pages.contains_key(name) {
            if bless {
                std::fs::remove_file(&file).unwrap();
            } else {
                failures.push(format!(
                    "{}: no {target} page has this name",
                    file.display()
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{target} help snapshots differ (run with {BLESS_ENV}=1 to rewrite them):\n{}",
        failures.join("\n")
    );
}

#[test]
fn cli_long_help_matches_fixtures() {
    let mut pages = BTreeMap::new();
    collect(&Cli::command(), "", &mut pages);
    check_fixtures("cli", &pages);
}

#[test]
fn mcp_tool_help_matches_fixtures() {
    let pages = ToolName::ALL
        .into_iter()
        .filter_map(|tool| Some((tool.to_string(), tool.page()?.text(Target::Mcp).to_owned())))
        .collect();
    check_fixtures("mcp", &pages);
}

#[test]
fn mcp_tool_descriptions_match_fixtures() {
    let pages = ToolName::ALL
        .into_iter()
        .map(|tool| (tool.to_string(), tool.description().to_owned()))
        .collect();
    check_fixtures("mcp_tools", &pages);
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
