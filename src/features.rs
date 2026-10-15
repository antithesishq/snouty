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
//! A gated command needs a [`Feature`] variant, a clap `hide` attribute, and a
//! check before dispatch: a hidden subcommand is still callable, and
//! clap_complete still lists it.
//!
//! Deliberately an environment variable and not a setting. The gate has to be
//! known before the command line is parsed, because it decides which
//! subcommands the parser has — and a setting cannot be read that early
//! without first parsing `--settings`/`--profile`, which would mean parsing the
//! command line twice. An environment variable has no such dependency.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use clap::ValueEnum;
use log::debug;

use crate::env;

/// The environment variable that enables unstable features, as a
/// comma-separated list of ids.
pub const UNSTABLE_FEATURES_VAR_NAME: &str = "SNOUTY_UNSTABLE_FEATURES";

/// A feature that can be turned on by id. `ValueEnum` derives the id from the
/// variant name in kebab-case, so a `RunsExec` variant has the id `runs-exec`.
///
/// No command is gated now, so only tests have a variant.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum Feature {
    /// Stands in for a real feature, so tests can turn one on.
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

/// The features `SNOUTY_UNSTABLE_FEATURES` enables, in the order listed.
/// Empty when the variable is unset or names no feature this build knows.
///
/// An unknown id is dropped, not rejected: every snouty on the machine shares
/// one exported variable, so it can hold an id from a newer build or a retired
/// one.
///
/// A non-Unicode value is treated as unset rather than failing the command:
/// this is read before the parse, where there is no good way to report an
/// error, and the cost of ignoring it is only that a feature stays off.
pub fn enabled() -> Vec<Feature> {
    match env::var(UNSTABLE_FEATURES_VAR_NAME) {
        Ok(Some(value)) => parse_list(&value),
        _ => Vec::new(),
    }
}

/// The features one comma-separated list names. Whitespace and empty entries
/// are dropped, so `"a, b,"` names `a` and `b`. Factored out of the
/// environment read so the rule can be unit-tested without changing
/// process-global state — the same split `crate::env` makes for its own pure
/// part.
fn parse_list(value: &str) -> Vec<Feature> {
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
            parse_list(" from-the-future ,, example , "),
            [Feature::Example]
        );
        assert!(parse_list("from-the-future").is_empty());
        assert!(parse_list("").is_empty());
        assert!(parse_list(" , ").is_empty());
    }
}
