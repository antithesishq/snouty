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
//! To gate a command, parse the ids [`enabled`] returns into a feature enum
//! with a constant for each id. Hide the command from `--help` with a clap
//! `hide` attribute, and refuse it before dispatch while its feature is off:
//! a hidden subcommand is still callable, and clap_complete still lists it.
//!
//! Deliberately an environment variable and not a setting. The gate has to be
//! known before the command line is parsed, because it decides which
//! subcommands the parser has — and a setting cannot be read that early
//! without first parsing `--settings`/`--profile`, which would mean parsing the
//! command line twice. An environment variable has no such dependency.

use crate::env;

/// The environment variable that enables unstable features, as a
/// comma-separated list of ids.
pub const UNSTABLE_FEATURES_VAR_NAME: &str = "SNOUTY_UNSTABLE_FEATURES";

/// The feature ids `SNOUTY_UNSTABLE_FEATURES` lists, in order. Empty when the
/// variable is unset or holds nothing usable; whitespace and empty entries are
/// dropped, so `"a, b,"` is `[a, b]`.
///
/// Every id is kept, known or not: one exported `SNOUTY_UNSTABLE_FEATURES` is
/// shared by every snouty on the machine, so an id a newer build knows about —
/// or one whose feature has graduated and had its id retired — must not break
/// the build that reads it.
///
/// A non-Unicode value is treated as unset rather than failing the command:
/// this is read before the parse, where there is no good way to report an
/// error, and the cost of ignoring it is only that a feature stays off.
pub fn enabled() -> Vec<String> {
    match env::var(UNSTABLE_FEATURES_VAR_NAME) {
        Ok(Some(value)) => parse_list(&value),
        _ => Vec::new(),
    }
}

/// The ids in one comma-separated list. Factored out of the environment read
/// so the splitting rule can be unit-tested without changing process-global
/// state — the same split `crate::env` makes for its own pure part.
fn parse_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_drops_blanks_and_whitespace() {
        assert_eq!(parse_list("one"), ["one"]);
        assert_eq!(parse_list(" one ,, other , "), ["one", "other"]);
        assert!(parse_list("").is_empty());
        assert!(parse_list(" , ").is_empty());
    }
}
