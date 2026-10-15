//! Snapshot tests for every command's long help. A change to a help page must
//! show up as a fixture diff. Run with `SNOUTY_BLESS=1` to rewrite the
//! fixtures.

use std::collections::BTreeMap;
use std::path::Path;

use clap::CommandFactory;
use snouty::cli::Cli;

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
