//! The credentials that `snouty login` stores: in the system keychain, or
//! else in the credentials file.

use std::path::{Path, PathBuf};

use color_eyre::{
    Section,
    eyre::{Context, OptionExt, Result, eyre},
};
use keyring_core::Entry;
use log::warn;
use serde::{Deserialize, Serialize};

use super::{AuthenticationInfo, RefreshableOAuthToken, claims::try_get_expiry_from_token};
use crate::{
    known_path::read_to_string_if_file_exists,
    settings::{back_up_unparsable_file, global_settings_dir, mkdir, write_atomically},
    tag::{Tag, Tagged},
};

const CREDENTIALS_FILENAME: &str = "credentials.toml";
/// The name of snouty's entry in the system keychain.
pub(crate) const KEYCHAIN_ENTRY_NAME: &str = "_default_";

/// A place that `snouty login` stores the credential. An OAuth refresh writes
/// the new token back to the same place.
#[derive(Clone, Debug)]
pub(crate) enum CredentialStore {
    Keychain,
    CredentialsFile { path: PathBuf },
}

impl CredentialStore {
    pub(super) fn load(&self) -> Result<Option<PersistableCredentials>> {
        match self {
            Self::Keychain => {
                let Some(entry) = keychain_entry()? else {
                    return Ok(None);
                };
                match entry.get_password() {
                    Ok(json) => match serde_json::from_str(&json) {
                        Ok(deserialized) => Ok(deserialized),
                        Err(err) => {
                            warn!(
                                "Deserialization of the value in the keychain failed with error {err:#}"
                            );
                            Ok(None)
                        }
                    },
                    Err(keyring_core::Error::NoEntry) => Ok(None),
                    Err(err) => {
                        warn!("keychain lookup for entry {KEYCHAIN_ENTRY_NAME} failed: {err}");
                        Ok(None)
                    }
                }
            }
            Self::CredentialsFile { path } => {
                let Some(contents) = read_to_string_if_file_exists(path)? else {
                    return Ok(None);
                };
                Ok(parse_credentials_file_toml(&contents, path)?.default)
            }
        }
    }

    pub(super) fn persist(&self, credentials: PersistableCredentials) -> Result<()> {
        match self {
            Self::Keychain => keychain_entry()?
                .ok_or_eyre("the system keychain is not available")?
                .set_password(&serde_json::to_string(&credentials)?)
                .wrap_err("writing the credential to the keychain"),
            Self::CredentialsFile { path } => persist_to_file(credentials, path),
        }
    }

    pub(super) fn resolve(self) -> Result<Option<Tagged<AuthenticationInfo, Self>>> {
        Ok(self
            .load()?
            .map(|found| found.convert_to_authentication_info(&self).with_tag(self)))
    }

    /// The credentials file, or `None` when the global settings dir is
    /// unknown.
    pub(super) fn credentials_file() -> Option<Self> {
        credentials_file_path().map(|path| Self::CredentialsFile { path })
    }
}

/// Where the credentials file is, or `None` when the global settings dir is
/// unknown. The file need not exist.
pub(crate) fn credentials_file_path() -> Option<PathBuf> {
    global_settings_dir().map(|dir| dir.join(CREDENTIALS_FILENAME))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum PersistableCredentials {
    ApiKey {
        api_key: String,
    },
    OAuth {
        antithesis_token: String,
        refresh_token: Option<String>,
    },
    Password {
        username: String,
        password: String,
    },
}

impl std::fmt::Debug for PersistableCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey { .. } => f
                .debug_struct("ApiKey")
                .field("api_key", &"[REDACTED]")
                .finish(),
            Self::OAuth {
                antithesis_token, ..
            } => f
                .debug_struct("OAuth")
                .field("antithesis_token", &"[REDACTED]")
                .field("refresh_token", &"[REDACTED]")
                .field("expiry", &try_get_expiry_from_token(antithesis_token))
                .finish(),
            Self::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .finish(),
        }
    }
}

impl PersistableCredentials {
    /// `self` as a credential that authenticates without a store: it never
    /// refreshes, so a request with it reads and writes no store. An OAuth
    /// access token goes out as a bearer token, the same header that a stored
    /// OAuth credential sends.
    pub(crate) fn to_unrefreshable_authentication_info(&self) -> AuthenticationInfo {
        match self {
            Self::ApiKey { api_key }
            | Self::OAuth {
                antithesis_token: api_key,
                ..
            } => AuthenticationInfo::ApiKey {
                api_key: api_key.clone(),
            },
            Self::Password { username, password } => AuthenticationInfo::Password {
                username: username.clone(),
                password: password.clone(),
            },
        }
    }

    /// An OAuth credential keeps a copy of `store`, to write a refreshed token
    /// back to it.
    fn convert_to_authentication_info(self, store: &CredentialStore) -> AuthenticationInfo {
        match self {
            Self::ApiKey { api_key } => AuthenticationInfo::ApiKey { api_key },
            Self::OAuth {
                antithesis_token,
                refresh_token,
            } => AuthenticationInfo::OAuth(RefreshableOAuthToken::new(
                store.clone(),
                antithesis_token,
                refresh_token,
            )),
            Self::Password { username, password } => {
                AuthenticationInfo::Password { username, password }
            }
        }
    }
}

#[cfg(target_os = "macos")]
pub fn initialize_credential_store() -> Result<()> {
    if crate::env::var("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE").is_ok_and(|v| v.is_some()) {
        return Ok(());
    }

    keyring_core::set_default_store(apple_native_keyring_store::keychain::Store::new()?);

    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn initialize_credential_store() -> Result<()> {
    // pass
    Ok(())
}

/// Stores `credentials` in the keychain, or in the credentials file when there
/// is no keychain.
pub(crate) fn persist(credentials: PersistableCredentials) -> Result<CredentialStore> {
    let store = if keychain_entry()?.is_some() {
        CredentialStore::Keychain
    } else {
        CredentialStore::credentials_file().ok_or_eyre(
            "Could not determine settings directory. Please ensure $XDG_CONFIG_HOME or $HOME is set",
        )?
    };
    store.persist(credentials)?;
    if matches!(store, CredentialStore::Keychain) {
        clear_from_file_if_present();
    }
    Ok(store)
}

/// snouty's keychain entry, or `None` when there is no keychain.
fn keychain_entry() -> Result<Option<Entry>> {
    match Entry::new("snouty", KEYCHAIN_ENTRY_NAME) {
        Ok(entry) => Ok(Some(entry)),
        // No default store: `initialize_credential_store` is a no-op on this
        // platform.
        Err(keyring_core::Error::NoDefaultStore) => Ok(None),
        Err(other) => Err(eyre!("opening keychain entry: {other}")),
    }
}

/// Deletes the credentials file when it holds a credential, which the one
/// in the keychain replaces.
fn clear_from_file_if_present() {
    let Some(path) = credentials_file_path() else {
        return;
    };
    if let Ok(Some(contents)) = read_to_string_if_file_exists(&path)
        && let Ok(creds_file) = parse_credentials_file_toml(&contents, &path)
        && creds_file.default.is_some()
    {
        match std::fs::remove_file(&path) {
            Ok(()) => warn!(
                "The supplied credentials were stored in the keychain, but the user credentials file also held a credential. Deleted the credentials file in favor of what was committed to the keychain."
            ),
            Err(err) => warn!(
                "The supplied credentials were stored in the keychain, but the stale credentials file at {} could not be deleted: {err:#}. The plaintext credential remains on disk.",
                path.display()
            ),
        }
    }
}

fn persist_to_file(credentials: PersistableCredentials, path: &Path) -> Result<()> {
    let settings_dir = path
        .parent()
        .ok_or_else(|| eyre!("Unable to determine parent directory of {}", path.display()))?;

    mkdir(settings_dir, true, 0o700)?;

    if let Some(contents) = read_to_string_if_file_exists(path)?
        && parse_credentials_file_toml(&contents, path).is_err()
    {
        let backup = back_up_unparsable_file(path)?;
        // Same shape as the settings repair note: paths on their own
        // lines, since they are what the user may copy and what made
        // the one-line form overflow.
        eprintln!(
            "warning: the existing credentials file could not be parsed; a new one will be written.\n  kept as a backup: {}\n  will be rewritten: {}",
            backup.display(),
            path.display(),
        );
    }
    let file = CredentialsFile {
        default: Some(credentials),
    };
    write_atomically(path, toml::to_string_pretty(&file)?.as_bytes())
}

pub(super) fn parse_credentials_file_toml(contents: &str, path: &Path) -> Result<CredentialsFile> {
    toml::from_str::<CredentialsFile>(contents)
        .wrap_err_with(|| {
            format!(
                "\"{}\" is not valid TOML or cannot be parsed as a Snouty credentials file.",
                path.display()
            )
        })
        .with_suggestion(|| {
            format!(
                "fix {}, or delete it and run `snouty login`",
                path.display()
            )
        })
}

/// The credentials file. It keeps the credential in a `[default]` table, the
/// layout of earlier releases.
#[derive(Serialize, Deserialize)]
pub(super) struct CredentialsFile {
    default: Option<PersistableCredentials>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::fake_secrets::{FAKE_ACCESS_TOKEN_A, FAKE_REFRESH_TOKEN_A};

    /// A new OAuth credential is checked with the header that it sends once
    /// it is stored, and without a store that a refresh could write to.
    #[test]
    fn an_oauth_credential_is_checked_as_its_bearer_token() -> Result<()> {
        let credentials = PersistableCredentials::OAuth {
            antithesis_token: FAKE_ACCESS_TOKEN_A.to_owned(),
            refresh_token: Some(FAKE_REFRESH_TOKEN_A.to_owned()),
        };
        let check = credentials.to_unrefreshable_authentication_info();
        assert!(matches!(check, AuthenticationInfo::ApiKey { .. }));
        assert_eq!(
            check.auth_header()?,
            format!("Bearer {FAKE_ACCESS_TOKEN_A}").as_str()
        );
        Ok(())
    }
}
