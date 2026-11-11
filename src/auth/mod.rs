//! The credentials that snouty authenticates with: where it finds them, and
//! the `Authorization` header that each gives. Each kind of credential has a
//! module of its own.

mod api_key;
mod claims;
mod oauth;
mod oidc;
mod password;
mod store;

use base64::{Engine, prelude::BASE64_STANDARD};
use color_eyre::{
    Section,
    eyre::{Context, OptionExt, Result, eyre},
};
use http::HeaderValue;

use crate::{
    env,
    error::user_error,
    tag::{Tag, Tagged},
};

pub(crate) use api_key::{API_KEY_VAR_NAME, is_blank_api_key};
pub use oauth::RefreshableOAuthToken;
pub use oidc::GithubActionsOidcToken;
#[cfg(test)]
use oidc::{OIDC_TOKEN_VAR_NAME, OIDC_URL_VAR_NAME};
use password::apply_password_policy;
pub(crate) use password::{PASSWORD_VAR_NAME, PasswordPolicy, USERNAME_VAR_NAME};
pub use store::initialize_credential_store;
pub(crate) use store::{CredentialStore, PersistableCredentials, keychain_entry_name, persist};

/// Every environment variable that ambient credential resolution reads. A test
/// harness that isolates itself from the developer's shell must clear all of
/// these, or an exported credential wins over the fixture it seeds.
#[cfg(test)]
pub(crate) const CREDENTIAL_ENV_VARS: [&str; 5] = [
    API_KEY_VAR_NAME,
    USERNAME_VAR_NAME,
    PASSWORD_VAR_NAME,
    OIDC_URL_VAR_NAME,
    OIDC_TOKEN_VAR_NAME,
];

/// Every command and `snouty doctor` state these words for an empty result.
pub(crate) const NO_CREDENTIALS_MESSAGE: &str = "No Antithesis credentials found";

/// Whether snouty runs in a GitHub Actions job, where `snouty login` cannot
/// run. A job without the `id-token: write` permission has no OIDC token, so
/// its first error names that permission.
pub(crate) fn in_github_actions() -> Result<bool> {
    Ok(env::var("GITHUB_ACTIONS")?.as_deref() == Some("true"))
}

/// The next step when no credential is found.
pub(crate) fn no_credentials_suggestion(github_actions: bool) -> &'static str {
    if github_actions {
        "add `id-token: write` to the job's `permissions` to use GitHub Actions OIDC, \
         or set ANTITHESIS_API_KEY from a secret"
    } else {
        "run `snouty login` or set ANTITHESIS_API_KEY"
    }
}

#[derive(Clone)]
pub enum AuthenticationInfo {
    ApiKey { api_key: String },
    GithubActionsOidc(GithubActionsOidcToken),
    OAuth(RefreshableOAuthToken),
    Password { username: String, password: String },
}

impl std::fmt::Debug for AuthenticationInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey { .. } => f
                .debug_struct("ApiKey")
                .field("api_key", &"[REDACTED]")
                .finish(),
            Self::GithubActionsOidc(token) => {
                f.debug_tuple("GithubActionsOidc").field(token).finish()
            }
            Self::OAuth(token) => f.debug_tuple("OAuth").field(token).finish(),
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

/// The kind of credential, never the secret.
impl std::fmt::Display for AuthenticationInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ApiKey { .. } => "API key",
            Self::GithubActionsOidc(_) => "GitHub Actions OIDC token",
            Self::OAuth(_) => "OAuth credentials",
            Self::Password { .. } => "username and password",
        })
    }
}

/// Where a credential that snouty found came from.
#[derive(Clone, Debug)]
pub(crate) enum CredentialSource {
    Environment { variables: Vec<&'static str> },
    Stored(CredentialStore),
}

pub(crate) type SourcedCredential = Tagged<AuthenticationInfo, CredentialSource>;

/// A place snouty looks for credentials. More than one can hold a credential
/// at once, and the first in resolution order then wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CredentialLookup<'a> {
    Environment,
    Keychain { profile: Option<&'a str> },
    CredentialsFile { profile: Option<&'a str> },
    GithubActionsOidc,
}

/// Whether the lookup for a named profile reads the stored credential of the
/// default profile after its own.
#[derive(Clone, Copy)]
enum Fallback {
    DefaultProfile,
    None,
}

impl<'a> CredentialLookup<'a> {
    fn in_resolution_order(
        profile: Option<&'a str>,
        fallback: Fallback,
    ) -> impl Iterator<Item = Self> {
        let default_entries = (profile.is_none() || matches!(fallback, Fallback::DefaultProfile))
            .then_some([
                Self::Keychain { profile: None },
                Self::CredentialsFile { profile: None },
            ]);
        [Self::Environment]
            .into_iter()
            .chain(profile.into_iter().flat_map(|profile| {
                [
                    Self::Keychain {
                        profile: Some(profile),
                    },
                    Self::CredentialsFile {
                        profile: Some(profile),
                    },
                ]
            }))
            .chain(default_entries.into_iter().flatten())
            .chain([Self::GithubActionsOidc])
    }

    fn resolve(self) -> Result<Option<SourcedCredential>> {
        match self {
            Self::Environment => AuthenticationInfo::try_from_env(),
            Self::Keychain { .. } | Self::CredentialsFile { .. } => Ok(self
                .resolve_stored()?
                .map(|found| found.map_tag(CredentialSource::Stored))),
            Self::GithubActionsOidc => AuthenticationInfo::try_from_github_actions_environment(),
        }
    }

    /// The credential in the store that this place names, if it is one.
    fn resolve_stored(self) -> Result<Option<Tagged<AuthenticationInfo, CredentialStore>>> {
        let store = match self {
            Self::Keychain { profile } => Some(CredentialStore::keychain(profile)),
            Self::CredentialsFile { profile } => CredentialStore::credentials_file(profile),
            Self::Environment | Self::GithubActionsOidc => None,
        };
        store.map_or(Ok(None), CredentialStore::resolve)
    }
}

impl AuthenticationInfo {
    fn try_from_env() -> Result<Option<SourcedCredential>> {
        if let Some(api_key) = env::var(API_KEY_VAR_NAME)? {
            return Ok(Some(Self::ApiKey { api_key }.with_tag(
                CredentialSource::Environment {
                    variables: vec![API_KEY_VAR_NAME],
                },
            )));
        }

        if let Some(username) = env::var(USERNAME_VAR_NAME)?
            && let Some(password) = env::var(PASSWORD_VAR_NAME)?
        {
            return Ok(Some(Self::Password { username, password }.with_tag(
                CredentialSource::Environment {
                    variables: vec![USERNAME_VAR_NAME, PASSWORD_VAR_NAME],
                },
            )));
        }

        Ok(None)
    }

    /// The credential of `profile` itself: the environment, then the
    /// keychain entry and the credentials-file entry of `profile`, then the
    /// GitHub Actions OIDC environment. Unlike the lookup of every command,
    /// a named profile does not use the stored credential of the default
    /// profile. The environment applies to every profile. It makes no network
    /// call, and `policy` applies to a username and password.
    pub(crate) fn find_own(profile: Option<&str>, policy: PasswordPolicy) -> Result<Option<Self>> {
        Self::find(profile, Fallback::None)?
            .map(|found| Ok(apply_password_policy(found, policy)?.untag()))
            .transpose()
    }

    fn find(profile: Option<&str>, fallback: Fallback) -> Result<Option<SourcedCredential>> {
        for lookup in CredentialLookup::in_resolution_order(profile, fallback) {
            if let Some(found) = lookup.resolve()? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// Every credential that snouty finds, in resolution order: the first is
    /// the one every command uses, and each later one is ignored.
    ///
    /// This always opens the system keychain, and the [`PasswordPolicy`] gate
    /// does not run. Only a command that reports on the credentials may call
    /// it, and only one that never authenticates with the result.
    pub(crate) fn available_ambient_credentials(
        profile: Option<&str>,
    ) -> Result<Vec<SourcedCredential>> {
        let mut found = Vec::new();
        for lookup in CredentialLookup::in_resolution_order(profile, Fallback::DefaultProfile) {
            match lookup.resolve() {
                Ok(Some(credential)) => found.push(credential),
                Ok(None) => {}
                // Past the first credential no command reads any further, so
                // an unreadable place there hides nothing. Before it, the
                // failure is the one every command meets.
                Err(_) if !found.is_empty() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(found)
    }

    /// Whether this is a blank API key (see [`is_blank_api_key`]).
    pub(crate) fn has_blank_api_key(&self) -> bool {
        matches!(self, Self::ApiKey { api_key } if is_blank_api_key(api_key))
    }

    /// The credential stored for `profile`: its keychain entry, else its
    /// section of the credentials file. This is what [`persist`] for `profile`
    /// replaces. The environment and other profiles are not read.
    ///
    /// The [`PasswordPolicy`] gate does not run, so only a caller that never
    /// authenticates with the result may call it.
    pub(crate) fn stored_for_profile(
        profile: Option<&str>,
    ) -> Result<Option<Tagged<Self, CredentialStore>>> {
        for lookup in [
            CredentialLookup::Keychain { profile },
            CredentialLookup::CredentialsFile { profile },
        ] {
            if let Some(found) = lookup.resolve_stored()? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    pub(crate) fn for_ambient_configuration(
        profile: Option<&str>,
        policy: PasswordPolicy,
    ) -> Result<Self> {
        match Self::find(profile, Fallback::DefaultProfile)? {
            Some(found) => Ok(apply_password_policy(found, policy)?.untag()),
            None => Err(user_error(NO_CREDENTIALS_MESSAGE)
                .suggestion(no_credentials_suggestion(in_github_actions()?))),
        }
    }

    /// Whether the credential needs a [`refresh`](Self::refresh) before its
    /// next request. An API key and a password never expire.
    pub(crate) fn expired(&self) -> bool {
        match self {
            Self::ApiKey { .. } | Self::Password { .. } => false,
            Self::GithubActionsOidc(token) => token.expired(),
            Self::OAuth(token) => token.expired(),
        }
    }

    /// Replaces the token of the credential with a newer one, in every clone.
    /// An OAuth credential calls the refresh endpoint at `base_url` with
    /// `client`, and a GitHub Actions OIDC credential fetches a new token
    /// from the Actions token endpoint.
    ///
    /// Fails when there is no newer token. An API key and a password always
    /// fail, because nothing replaces them.
    pub(crate) async fn refresh(&self, client: &reqwest::Client, base_url: &str) -> Result<()> {
        match self {
            Self::ApiKey { .. } | Self::Password { .. } => Err(eyre!("cannot refresh the {self}")),
            Self::GithubActionsOidc(token) => token.refresh().await,
            Self::OAuth(token) => token.refresh(client, base_url).await,
        }
    }

    /// The `Authorization` header for the current token of the credential. It
    /// never refreshes: an expired token gives its header as it is. A GitHub
    /// Actions OIDC credential has no token, and no header, until its first
    /// refresh.
    pub(crate) fn auth_header(&self) -> Result<HeaderValue> {
        match self {
            Self::ApiKey { api_key } => to_header_value(&format!("Bearer {api_key}"), true),
            Self::GithubActionsOidc(token) => {
                let token = token
                    .token()
                    .ok_or_eyre("no GitHub Actions OIDC token was fetched")?;
                to_header_value(&format!("GHA {token}"), true)
            }
            Self::OAuth(token) => {
                to_header_value(&format!("Bearer {}", token.access_token()), true)
            }
            Self::Password { username, password } => {
                let credentials = format!("{username}:{password}");
                let encoded = BASE64_STANDARD.encode(credentials);
                to_header_value(&format!("Basic {encoded}"), true)
            }
        }
    }
}

fn to_header_value(content: &str, sensitive: bool) -> Result<HeaderValue> {
    let mut hv = HeaderValue::from_str(content).wrap_err("failed to build Authorization header")?;
    hv.set_sensitive(sensitive);
    Ok(hv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_named_profile_uses_the_default_profile_only_with_the_fallback() {
        use CredentialLookup::{CredentialsFile, Environment, GithubActionsOidc, Keychain};
        let own = [
            Environment,
            Keychain {
                profile: Some("acme-prod"),
            },
            CredentialsFile {
                profile: Some("acme-prod"),
            },
        ];
        let default = [
            Keychain { profile: None },
            CredentialsFile { profile: None },
        ];
        assert_eq!(
            CredentialLookup::in_resolution_order(Some("acme-prod"), Fallback::None)
                .collect::<Vec<_>>(),
            [own.as_slice(), &[GithubActionsOidc]].concat(),
        );
        assert_eq!(
            CredentialLookup::in_resolution_order(Some("acme-prod"), Fallback::DefaultProfile)
                .collect::<Vec<_>>(),
            [own.as_slice(), &default, &[GithubActionsOidc]].concat(),
        );
    }

    #[test]
    fn the_default_profile_always_uses_its_own_entries() {
        for fallback in [Fallback::None, Fallback::DefaultProfile] {
            assert_eq!(
                CredentialLookup::in_resolution_order(None, fallback).collect::<Vec<_>>(),
                [
                    CredentialLookup::Environment,
                    CredentialLookup::Keychain { profile: None },
                    CredentialLookup::CredentialsFile { profile: None },
                    CredentialLookup::GithubActionsOidc,
                ],
            );
        }
    }
}
