//! Username and password credentials, and the policy for their deprecation.

use color_eyre::{Section, eyre::Result};

use super::{
    API_KEY_VAR_NAME, AuthenticationInfo, CredentialSource, SourcedCredential, login_command,
};
use crate::error::user_error;

pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";

/// How a command treats username/password credentials when ambient
/// resolution finds them. The other credential kinds are always accepted.
#[derive(Clone, Copy)]
pub(crate) enum PasswordPolicy {
    /// Fail with a friendly error: every endpoint other than the
    /// launch endpoints answers username/password with an opaque 403.
    Reject,
    /// Accept them and print the deprecation warning: the launch commands
    /// (`snouty launch`, `snouty debug`) take this path.
    WarnDeprecated,
}

/// The remediation for the username/password deprecation, for a credential
/// from `source`. The rejection and the warning both render it, so their
/// wording cannot drift apart.
fn password_deprecation_suggestion(source: &CredentialSource) -> String {
    let next_step = match source {
        // `snouty login` alone cannot help: the environment hides what it
        // stores. The environment's API key comes before its password.
        CredentialSource::Environment { variables } => format!(
            "set {API_KEY_VAR_NAME}, or run `snouty login` and then `unset {}`",
            variables.join(" ")
        ),
        CredentialSource::Stored(store) => format!(
            "run {} to switch to another authentication method",
            login_command(store.profile())
        ),
    };
    format!("username/password authentication is deprecated; {next_step}")
}

/// Apply the caller's [`PasswordPolicy`] to a resolved credential. This is
/// the only place that accepts or rejects username/password credentials, so
/// the whole deprecation policy lives here.
pub(super) fn apply_password_policy(
    authn_info: SourcedCredential,
    policy: PasswordPolicy,
) -> Result<SourcedCredential> {
    if !matches!(authn_info.value(), AuthenticationInfo::Password { .. }) {
        return Ok(authn_info);
    }
    match policy {
        PasswordPolicy::Reject => Err(user_error(
            "This command does not accept username/password authentication, which is only supported when launching runs (`snouty launch`, `snouty debug`)",
        )
        .suggestion(password_deprecation_suggestion(authn_info.tag()))),
        PasswordPolicy::WarnDeprecated => {
            // A bare eprintln, not log::warn!: the notice must reach stderr
            // on every run, independent of RUST_LOG.
            eprintln!(
                "warning: {} (support will be removed in a future release)",
                password_deprecation_suggestion(authn_info.tag())
            );
            Ok(authn_info)
        }
    }
}
