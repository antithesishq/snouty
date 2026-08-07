//! Opt-in features.
//!
//! A feature gates a command that isn't ready to be on for everyone — because
//! the Antithesis API it depends on is still changing shape, or because most
//! tenants can't serve it yet. Gating lets such a command ship in a release
//! instead of waiting on the API, without putting it in front of users who
//! would only hit a wall.
//!
//! Enable features by id in the `features` setting (a TOML array in
//! `.snouty.toml`, or a comma-separated `SNOUTY_FEATURES`), and see
//! [`crate::settings::Settings::features`] for how it resolves. A disabled
//! feature's command is absent from the CLI: it is missing from `--help` and
//! from completions, and invoking it fails the same way any unknown
//! subcommand does.

use std::fmt;
use std::str::FromStr;

/// A feature that can be turned on by id.
///
/// Unknown ids are kept as [`Feature::Unknown`] rather than rejected: a
/// settings file is shared across snouty versions, and a feature that a newer
/// snouty knows about — or one that has since graduated and had its id
/// removed — must not break an older or newer binary that reads the same file.
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

    pub fn as_str(&self) -> &str {
        match self {
            Feature::RunsExec => Self::RUNS_EXEC,
            Feature::Unknown(id) => id,
        }
    }
}

impl FromStr for Feature {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            Self::RUNS_EXEC => Feature::RunsExec,
            other => Feature::Unknown(other.to_string()),
        })
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_id_parses_to_its_variant() {
        assert_eq!("runs-exec".parse::<Feature>().unwrap(), Feature::RunsExec);
        assert_eq!(Feature::RunsExec.to_string(), "runs-exec");
    }

    #[test]
    fn unknown_id_is_kept_rather_than_rejected() {
        // A settings file is shared across snouty versions: an id from a newer
        // build, or one whose feature has graduated, must not break this one.
        let parsed = "from-the-future".parse::<Feature>().unwrap();
        assert_eq!(parsed, Feature::Unknown("from-the-future".to_string()));
        assert_eq!(parsed.to_string(), "from-the-future");
    }
}
