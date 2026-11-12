//! The credentials that `snouty login` stores: in the system keychain, or
//! else in the credentials file.

use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Context, OptionExt, Result, eyre};
use keyring_core::Entry;
use log::warn;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::{AuthenticationInfo, RefreshableOAuthToken, claims::try_get_expiry_from_token};
use crate::{
    settings::{
        back_up_unparsable_file, global_settings_dir, mkdir, read_to_string_if_file_exists,
    },
    tag::{Tag, Tagged},
};

const CREDENTIALS_FILENAME: &str = "credentials.toml";

/// A place that `snouty login` stores the credential of one profile. An
/// OAuth refresh writes the new token back to the same place.
#[derive(Clone, Debug)]
pub(crate) enum CredentialStore {
    Keychain {
        profile: Option<String>,
    },
    CredentialsFile {
        path: PathBuf,
        profile: Option<String>,
    },
}

impl CredentialStore {
    /// The named profile, or `None` for the default profile.
    pub(crate) fn profile(&self) -> Option<&str> {
        match self {
            Self::Keychain { profile } | Self::CredentialsFile { profile, .. } => {
                profile.as_deref()
            }
        }
    }

    pub(super) fn load(&self) -> Result<Option<PersistableCredentials>> {
        match self {
            Self::Keychain { profile } => {
                let Some(entry) = keychain_entry(profile.as_deref())? else {
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
                        warn!(
                            "keychain lookup for entry {} failed: {err}",
                            keychain_entry_name(profile.as_deref())
                        );
                        Ok(None)
                    }
                }
            }
            Self::CredentialsFile { path, profile } => {
                let Some(contents) = read_to_string_if_file_exists(path)? else {
                    return Ok(None);
                };
                let parsed = parse_credentials_file_toml(&contents, path)?;
                Ok(parsed.for_profile(profile.as_deref()))
            }
        }
    }

    pub(super) fn persist(&self, credentials: PersistableCredentials) -> Result<()> {
        match self {
            Self::Keychain { profile } => keychain_entry(profile.as_deref())?
                .ok_or_eyre("the system keychain is not available")?
                .set_password(&serde_json::to_string(&credentials)?)
                .wrap_err("writing the credential to the keychain"),
            Self::CredentialsFile { path, profile } => {
                persist_to_file(credentials, profile.as_deref(), path)
            }
        }
    }

    /// Where `snouty login` stores the credential of `profile`: the
    /// keychain, or the credentials file when there is no keychain.
    pub(crate) fn for_login(profile: Option<&str>) -> Result<Self> {
        if keychain_entry(profile)?.is_some() {
            Ok(Self::keychain(profile))
        } else {
            Self::credentials_file(profile).ok_or_eyre(
                "Could not determine settings directory. Please ensure $XDG_CONFIG_HOME or $HOME is set",
            )
        }
    }

    /// Stores `credentials` for `snouty login`. A keychain entry replaces the
    /// credentials-file entry of the same profile.
    pub(crate) fn store(&self, credentials: PersistableCredentials) -> Result<()> {
        self.persist(credentials)?;
        if matches!(self, Self::Keychain { .. }) {
            clear_from_file_if_present(self.profile());
        }
        Ok(())
    }

    pub(super) fn resolve(self) -> Result<Option<Tagged<AuthenticationInfo, Self>>> {
        Ok(self
            .load()?
            .map(|found| found.convert_to_authentication_info(&self).with_tag(self)))
    }

    pub(super) fn keychain(profile: Option<&str>) -> Self {
        Self::Keychain {
            profile: profile.map(str::to_owned),
        }
    }

    /// The credentials file for `profile`, or `None` when the global settings
    /// dir is unknown.
    pub(super) fn credentials_file(profile: Option<&str>) -> Option<Self> {
        credentials_file_path().map(|path| Self::CredentialsFile {
            path,
            profile: profile.map(str::to_owned),
        })
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
    /// An OAuth credential keeps a copy of `store`, to write a refreshed token
    /// back to it.
    pub(crate) fn convert_to_authentication_info(
        self,
        store: &CredentialStore,
    ) -> AuthenticationInfo {
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

/// The keychain entry of `profile`, or `None` when there is no keychain.
fn keychain_entry(profile: Option<&str>) -> Result<Option<Entry>> {
    match Entry::new("snouty", &keychain_entry_name(profile)) {
        Ok(entry) => Ok(Some(entry)),
        // No default store: `initialize_credential_store` is a no-op on this
        // platform.
        Err(keyring_core::Error::NoDefaultStore) => Ok(None),
        Err(other) => Err(eyre!("opening keychain entry: {other}")),
    }
}

pub(crate) fn keychain_entry_name(profile: Option<&str>) -> String {
    profile.map_or_else(|| "_default_".to_owned(), |p| format!("profile_{p}"))
}

fn clear_from_file_if_present(profile: Option<&str>) {
    let Some(path) = credentials_file_path() else {
        return;
    };
    let parent_dir = path
        .parent()
        .expect("the credentials file path is a file name joined to a dir");

    if let Ok(Some(contents)) = read_to_string_if_file_exists(&path)
        && let Ok(mut creds_file) = parse_credentials_file_toml(&contents, &path)
    {
        let mut changed = false;
        if let Some(profile) = profile {
            if let Some(by_profile) = creds_file.profile.as_mut() {
                changed = by_profile.remove(profile).is_some();
            }
        } else {
            changed = creds_file.default.is_some();
            creds_file.default = None;
        }

        if changed {
            let profile_phrase = if profile.is_some() { "the same" } else { "no" };
            match rewrite_credentials_file(&creds_file, parent_dir, &path) {
                Ok(()) => warn!(
                    "The supplied credentials were stored in the keychain, but an entry under {profile_phrase} profile name was also present in the user credentials file. Cleared the entry from the credentials file in favor of what was committed to the keychain."
                ),
                Err(err) => warn!(
                    "The supplied credentials were stored in the keychain, but a stale entry under {profile_phrase} profile name could not be removed from the credentials file at {}: {err:#}. The plaintext entry remains on disk.",
                    path.display()
                ),
            }
        }
    }
}

/// Atomically rewrite the credentials file with `creds_file` via a temp file
/// in `parent_dir`.
fn rewrite_credentials_file(
    creds_file: &CredentialsFile,
    parent_dir: &Path,
    path: &Path,
) -> Result<()> {
    let mut temp = NamedTempFile::new_in(parent_dir)?;
    temp.write_all(toml::to_string_pretty(creds_file)?.as_bytes())?;
    temp.persist(path)?;
    Ok(())
}

fn persist_to_file(
    credentials: PersistableCredentials,
    profile: Option<&str>,
    path: &Path,
) -> Result<()> {
    let settings_dir = path
        .parent()
        .ok_or_else(|| eyre!("Unable to determine parent directory of {}", path.display()))?;

    mkdir(settings_dir, true, 0o700)?;

    let mut current_contents = match read_to_string_if_file_exists(path)? {
        Some(contents) => {
            if let Ok(file) = parse_credentials_file_toml(&contents, path) {
                file
            } else {
                let backup = back_up_unparsable_file(path)?;
                // Same shape as the settings repair note: paths on their own
                // lines, since they are what the user may copy and what made
                // the one-line form overflow.
                eprintln!(
                    "warning: the existing credentials file could not be parsed; a new one will be written.\n  kept as a backup: {}\n  will be rewritten: {}",
                    backup.display(),
                    path.display(),
                );
                CredentialsFile {
                    default: None,
                    profile: None,
                }
            }
        }
        None => CredentialsFile {
            default: None,
            profile: None,
        },
    };

    if let Some(profile) = profile {
        if current_contents.profile.is_none() {
            current_contents.profile = Some(HashMap::new());
        }

        current_contents
            .profile
            .as_mut()
            .unwrap()
            .insert(profile.to_owned(), credentials);
    } else {
        current_contents.default = Some(credentials);
    }

    rewrite_credentials_file(&current_contents, settings_dir, path)
}

pub(super) fn parse_credentials_file_toml(contents: &str, path: &Path) -> Result<CredentialsFile> {
    toml::from_str::<CredentialsFile>(contents).wrap_err(format!(
        "\"{}\" is not valid TOML or cannot be parsed as a Snouty credentials file.",
        path.display()
    ))
}

#[derive(Serialize, Deserialize)]
pub(super) struct CredentialsFile {
    default: Option<PersistableCredentials>,
    profile: Option<HashMap<String, PersistableCredentials>>,
}

impl CredentialsFile {
    /// A named profile reads its own `[profile.<name>]` section, and never
    /// falls back to `[default]`.
    pub(super) fn for_profile(self, profile: Option<&str>) -> Option<PersistableCredentials> {
        match profile {
            Some(name) => self
                .profile
                .and_then(|by_profile| by_profile.get(name).cloned()),
            None => self.default,
        }
    }
}
