//! Opt-in features.
//!
//! A feature gates a command that isn't ready to be on for everyone — because
//! the Antithesis API it depends on is still changing shape, or because most
//! tenants can't serve it yet. Gating lets such a command ship in a release
//! instead of waiting on the API, without putting it in front of users who
//! would only hit a wall.
//!
//! Enable features by id in `SNOUTY_FEATURES`, a comma-separated list (see
//! [`enabled`]). A gated command is hidden from `--help` until its feature is
//! on, and invoking it while it is off fails as an unrecognized subcommand.
//! (Hiding is not removal: `runs exec --help` still prints its help, which
//! names the feature, and clap_complete lists hidden subcommands anyway.)
//!
//! Deliberately an environment variable and not a setting. The gate has to be
//! known before the command line is parsed, because it decides which
//! subcommands the parser has — and a setting cannot be read that early
//! without first parsing `--settings`/`--profile`, which would mean parsing the
//! command line twice. An environment variable has no such dependency.

use crate::env;

/// The environment variable that enables features, as a comma-separated list
/// of ids.
pub const FEATURES_VAR_NAME: &str = "SNOUTY_FEATURES";

/// Whether `feature` is enabled.
///
/// Cheap enough to call from anywhere — including a clap `hide` attribute,
/// which is how a gated command decides whether to show itself.
pub fn is_enabled(feature: Feature) -> bool {
    enabled().contains(&feature)
}

/// The features `SNOUTY_FEATURES` enables, in the order listed. Empty when the
/// variable is unset or holds nothing usable; whitespace and empty entries are
/// dropped, so `"a, b,"` is `[a, b]`.
///
/// A non-Unicode value is treated as unset rather than failing the command:
/// this is read before the parse, where there is no good way to report an
/// error, and the cost of ignoring it is only that a feature stays off.
pub fn enabled() -> Vec<Feature> {
    match env::var(FEATURES_VAR_NAME) {
        Ok(Some(value)) => parse_list(&value),
        _ => Vec::new(),
    }
}

/// The ids in one comma-separated list. Factored out of the environment read
/// so the splitting rule can be unit-tested without changing process-global
/// state — the same split `crate::env` makes for its own pure part.
fn parse_list(value: &str) -> Vec<Feature> {
    value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(Feature::from_id)
        .collect()
}

/// A feature that can be turned on by id.
///
/// Unknown ids are kept as [`Feature::Unknown`] rather than rejected: one
/// exported `SNOUTY_FEATURES` is shared by every snouty on the machine, so an
/// id a newer build knows about — or one whose feature has graduated and had
/// its id retired — must not break the build that reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Feature {
    /// `snouty runs exec`, which drives the execute-command API. That API is
    /// unstable and is unavailable on most tenants.
    RunsExec,
    /// An id this build does not recognize.
    Unknown(String),
}

impl Feature {
    pub const RUNS_EXEC: &'static str = "runs-exec";

    /// The feature an id names. Every id maps to a feature, so this is total:
    /// one this build does not know becomes [`Feature::Unknown`].
    pub fn from_id(id: &str) -> Self {
        match id {
            Self::RUNS_EXEC => Feature::RunsExec,
            other => Feature::Unknown(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Feature::RunsExec => Self::RUNS_EXEC,
            Feature::Unknown(id) => id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_id_parses_to_its_variant() {
        assert_eq!(Feature::from_id("runs-exec"), Feature::RunsExec);
        assert_eq!(Feature::RunsExec.as_str(), "runs-exec");
    }

    #[test]
    fn unknown_id_is_kept_rather_than_rejected() {
        // One exported SNOUTY_FEATURES is shared by every snouty on the
        // machine: an id from a newer build, or one whose feature has
        // graduated, must not break this one.
        let parsed = Feature::from_id("from-the-future");
        assert_eq!(parsed, Feature::Unknown("from-the-future".to_string()));
        assert_eq!(parsed.as_str(), "from-the-future");
    }

    #[test]
    fn a_list_drops_blanks_and_whitespace() {
        assert_eq!(parse_list("runs-exec"), vec![Feature::RunsExec]);
        assert_eq!(
            parse_list(" runs-exec ,, other , "),
            vec![Feature::RunsExec, Feature::Unknown("other".to_string())]
        );
        assert!(parse_list("").is_empty());
        assert!(parse_list(" , ").is_empty());
    }
}
