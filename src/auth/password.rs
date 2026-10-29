//! Username and password credentials, and the policy for their deprecation.

use color_eyre::{Section, eyre::Result};

use super::AuthenticationInfo;
use crate::{attributed_value::AttributedValue, error::user_error};

pub(crate) const USERNAME_VAR_NAME: &str = "ANTITHESIS_USERNAME";
pub(crate) const PASSWORD_VAR_NAME: &str = "ANTITHESIS_PASSWORD";

/// How a command treats username/password credentials when ambient
/// resolution finds them. The other credential kinds are always accepted.
#[derive(Clone, Copy)]
pub(crate) enum PasswordPolicy {
    /// Fail with a friendly error: every endpoint other than the launch
    /// webhooks answers username/password with an opaque 403.
    Reject,
    /// Accept them and print the deprecation warning: the launch commands
    /// (`snouty launch`, `snouty debug`) take this path.
    WarnDeprecated,
}

/// The one-line remediation for the username/password deprecation. The
/// rejection suggestion and the warning on use both render this constant, so
/// the wording cannot drift apart. Doctor states the deprecation in its own
/// check, with a next step that depends on where the password came from.
const PASSWORD_DEPRECATION_SUGGESTION: &str = "username/password authentication is deprecated; run `snouty login` to switch to another authentication method";

/// Apply the caller's [`PasswordPolicy`] to a resolved credential. This is
/// the only place that accepts or rejects username/password credentials, so
/// the whole deprecation policy lives here.
pub(super) fn apply_password_policy(
    authn_info: AttributedValue<AuthenticationInfo>,
    policy: PasswordPolicy,
) -> Result<AttributedValue<AuthenticationInfo>> {
    if !matches!(authn_info.value(), AuthenticationInfo::Password { .. }) {
        return Ok(authn_info);
    }
    match policy {
        PasswordPolicy::Reject => Err(user_error(
            "This command does not accept username/password authentication, which is only supported when launching runs (`snouty launch`, `snouty debug`)",
        )
        .suggestion(PASSWORD_DEPRECATION_SUGGESTION)),
        PasswordPolicy::WarnDeprecated => {
            // A bare eprintln, not log::warn!: the notice must reach stderr
            // on every run, independent of RUST_LOG.
            eprintln!(
                "warning: {PASSWORD_DEPRECATION_SUGGESTION} (support will be removed in a future release)"
            );
            Ok(authn_info)
        }
    }
}
