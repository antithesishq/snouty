//! Opt-in unstable features.
//!
//! A feature gates a command that isn't ready to be on for everyone — because
//! the Antithesis API it depends on is still changing shape, or because most
//! tenants can't serve it yet. Gating lets such a command ship in a release
//! instead of waiting on the API, without putting it in front of users who
//! would only hit a wall.
//!
//! Enable features by id in `SNOUTY_UNSTABLE_FEATURES`, a comma-separated
//! list (see [`enabled`]). The variable says "unstable" because that is the
//! promise: a feature here can change its behaviour, its flags, or its id, or
//! go away, in any release. Nothing behind this gate is covered by whatever
//! stability the rest of the CLI has.
//!
//! # Gating
//!
//! The gate stays in the code when no feature uses it. To gate something, add
//! a [`Feature`] variant whose doc names what it gates, then:
//!
//! - A command: put `hide = !features::is_enabled(Feature::X)` in its
//!   `#[command(...)]`, and make [`require`] the first call of its dispatch
//!   arm: `features::require(Feature::X, "snouty runs x")?;`. `hide` keeps
//!   the command out of the help, and [`require`] refuses it, because a
//!   hidden command is still callable.
//! - A flag: put the same `hide` in its `#[arg(...)]`, and call [`require`]
//!   in the dispatch arm when the flag is set:
//!   `features::require(Feature::X, "snouty runs x --y")?;`. The MCP tool of
//!   the command leaves out a hidden flag.
//! - An MCP tool: end its row in the `tools!` table of `src/mcp/tools.rs`
//!   with `unstable: X`. The MCP server leaves the tool out of its tools, of
//!   `tool_help` and of the `snouty mcp` help while the feature is off.
//!   Another help page that names the command must put that text in
//!   `{% if cli %}`; its MCP render fails otherwise.
//!
//! When a feature becomes stable, delete its variant and each use that the
//! build then names. Keep the rest of this module and the `Example` variant,
//! which tests the gate.
//!
//! # Why an environment variable
//!
//! Deliberately an environment variable and not a setting. The gate has to be
//! known before the command line is parsed, because clap reads `hide` when it
//! builds the parser — and a setting cannot be read that early without first
//! parsing `--settings`/`--profile`, which would mean parsing the command
//! line twice. An environment variable has no such dependency.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use clap::ValueEnum;
use color_eyre::Section;
use color_eyre::eyre::Result;
use log::debug;

use crate::env;
use crate::error::user_error;

/// The environment variable that enables unstable features, as a
/// comma-separated list of ids.
pub const UNSTABLE_FEATURES_VAR_NAME: &str = "SNOUTY_UNSTABLE_FEATURES";

/// The id of a feature is its variant name in kebab-case.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum Feature {
    /// Stands in for a real feature in tests.
    #[cfg(test)]
    Example,
}

impl FromStr for Feature {
    type Err = String;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        <Self as ValueEnum>::from_str(id, false)
    }
}

impl Display for Feature {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let value = self
            .to_possible_value()
            .expect("no Feature variant is skipped");
        f.write_str(value.get_name())
    }
}

/// Refuses the gated `command` while `feature` is off. `command` is what the
/// user typed to use it, such as `snouty runs x` or `snouty runs x --y`.
pub fn require(feature: Feature, command: &str) -> Result<()> {
    if is_enabled(feature) {
        return Ok(());
    }
    Err(user_error(format!(
        "`{command}` is an unstable feature and is not enabled"
    ))
    .note(format!(
        "enable it by setting {UNSTABLE_FEATURES_VAR_NAME}={feature}"
    ))
    .note("an unstable feature can change or go away in any release"))
}

/// Whether `feature` is enabled. Cheap enough for a clap `hide` attribute.
pub fn is_enabled(feature: Feature) -> bool {
    enabled().contains(&feature)
}

/// The features `SNOUTY_UNSTABLE_FEATURES` enables, in the order listed.
/// Empty when the variable is unset or names no feature this build knows.
///
/// Drops an unknown id: every snouty on the machine shares one exported
/// variable, which can hold an id from a newer build or a retired one.
///
/// A non-Unicode value is treated as unset rather than failing the command:
/// this is read before the parse, where there is no good way to report an
/// error, and the cost of ignoring it is only that a feature stays off.
pub fn enabled() -> Vec<Feature> {
    match env::var(UNSTABLE_FEATURES_VAR_NAME) {
        Ok(Some(value)) => parse_features_list(&value),
        _ => Vec::new(),
    }
}

/// Parses a comma-separated list of ids into features, in order, and drops
/// blank entries and unknown ids. Separate from [`enabled`] so tests need not
/// set process-global state.
fn parse_features_list(value: &str) -> Vec<Feature> {
    value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .filter_map(|id| {
            let feature = id.parse().ok();
            if feature.is_none() {
                debug!("ignoring unknown feature `{id}` in {UNSTABLE_FEATURES_VAR_NAME}");
            }
            feature
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_keeps_known_ids_and_drops_the_rest() {
        assert_eq!(
            parse_features_list(" from-the-future ,, example , "),
            [Feature::Example]
        );
        assert!(parse_features_list("from-the-future").is_empty());
        assert!(parse_features_list("").is_empty());
        assert!(parse_features_list(" , ").is_empty());
    }

    /// The tests do not set `SNOUTY_UNSTABLE_FEATURES=example`.
    #[test]
    fn a_gated_command_is_refused_while_its_feature_is_off() {
        let report = require(Feature::Example, "snouty runs x").unwrap_err();
        let rendered = crate::error::render_report(&report);
        let rendered = crate::event_render::strip_ansi(&rendered);
        assert!(
            rendered.starts_with(
                "Error: `snouty runs x` is an unstable feature and is not enabled\n\n\
                 Note: enable it by setting SNOUTY_UNSTABLE_FEATURES=example\n\
                 Note: an unstable feature can change or go away in any release\n"
            ),
            "{rendered}"
        );
    }
}
