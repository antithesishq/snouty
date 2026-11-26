//! Username and password credentials, which snouty no longer accepts. It
//! finds them only to name the fix.

use color_eyre::{Section, eyre::Report};

use super::login_command;

pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";

/// Ambient credential resolution found a username and password. `snouty
/// doctor` reads this error to report it as a credentials check.
#[derive(Debug)]
pub(crate) enum UnsupportedPassword {
    Environment,
    /// An older snouty stored it for `profile`.
    Stored {
        profile: Option<String>,
    },
}

impl UnsupportedPassword {
    /// The error with its suggestion, as every command reports it.
    pub(crate) fn into_report(self) -> Report {
        let suggestion = self.suggestion();
        Report::new(self)
            .suppress_backtrace(true)
            .suggestion(suggestion)
    }

    pub(crate) fn suggestion(&self) -> String {
        match self {
            Self::Environment => format!(
                "`unset {USERNAME_VAR_NAME} {PASSWORD_VAR_NAME}`, then run `snouty login` \
                 or set ANTITHESIS_API_KEY"
            ),
            Self::Stored { profile } => format!(
                "create an API key, then run {} to replace the username and password",
                login_command(profile.as_deref())
            ),
        }
    }
}

impl std::fmt::Display for UnsupportedPassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Environment => write!(
                f,
                "snouty no longer accepts a username and password from \
                 {USERNAME_VAR_NAME} and {PASSWORD_VAR_NAME}"
            ),
            Self::Stored { .. } => {
                f.write_str("snouty no longer accepts the stored username and password")
            }
        }
    }
}

impl std::error::Error for UnsupportedPassword {}
