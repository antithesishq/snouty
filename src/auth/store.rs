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

use super::{
    AuthenticationInfo, OAuthRefreshInfo, RefreshableOAuthToken, claims::try_get_expiry_from_token,
};
use crate::{
    attributed_value::AttributedValue,
    settings::{
        back_up_unparsable_file, global_settings_dir, mkdir, read_to_string_if_file_exists,
    },
};

const CREDENTIALS_FILENAME: &str = "credentials.toml";

impl AuthenticationInfo {
    pub(super) fn try_from_keychain(
        profile: Option<&str>,
    ) -> Result<Option<AttributedValue<Self>>> {
        let entry_name = construct_keychain_credential_name(profile);
        let refresh_info = OAuthRefreshInfo::Keychain {
            entry_name: entry_name.clone(),
        };
        if let Some(found) = refresh_info.load()? {
            Ok(Some(AttributedValue::Keychain {
                value: found.convert_to_authentication_info(refresh_info),
                entry_name,
            }))
        } else {
            Ok(None)
        }
    }

    pub(super) fn try_from_credentials_file(
        profile: Option<&str>,
    ) -> Result<Option<AttributedValue<Self>>> {
        let Some((path, parsed)) = try_load_credentials_file()? else {
            return Ok(None);
        };
        Ok(parsed
            .for_profile(profile)
            .map(|credentials| AttributedValue::SettingsFile {
                value: credentials.convert_to_authentication_info(
                    OAuthRefreshInfo::CredentialsFile {
                        path: path.clone(),
                        profile: profile.map(str::to_owned),
                    },
                ),
                settings_file_path: path,
                profile: profile.map(str::to_owned),
            }))
    }
}

fn try_load_credentials_file() -> Result<Option<(PathBuf, CredentialsFile)>> {
    if let Some((_dir, path)) = try_get_credentials_file_path()
        && let Some(contents) = read_to_string_if_file_exists(&path)?
    {
        let parsed = parse_credentials_file_toml(&contents, &path)?;
        return Ok(Some((path, parsed)));
    }

    Ok(None)
}

fn try_get_credentials_file_path() -> Option<(PathBuf, PathBuf)> {
    if let Some(snouty_settings_dir) = global_settings_dir() {
        let path = snouty_settings_dir.join(CREDENTIALS_FILENAME);
        Some((snouty_settings_dir, path))
    } else {
        None
    }
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
    fn convert_to_authentication_info(self, refresh_info: OAuthRefreshInfo) -> AuthenticationInfo {
        match self {
            Self::ApiKey { api_key } => AuthenticationInfo::ApiKey { api_key },
            Self::OAuth {
                antithesis_token,
                refresh_token,
            } => AuthenticationInfo::OAuth(RefreshableOAuthToken::new(
                refresh_info,
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

pub(crate) fn persist(
    credentials: PersistableCredentials,
    profile: Option<&str>,
) -> Result<AttributedValue<()>> {
    match try_persist_to_keychain(&credentials, profile) {
        Err(err) => Err(err),
        Ok(Some(entry_name)) => Ok(AttributedValue::Keychain {
            value: (),
            entry_name,
        }),
        Ok(None) => {
            persist_to_file(credentials, profile, None).map(|path| AttributedValue::SettingsFile {
                value: (),
                settings_file_path: path,
                profile: profile.map(str::to_owned),
            })
        }
    }
}

fn try_persist_to_keychain(
    credentials: &PersistableCredentials,
    profile: Option<&str>,
) -> Result<Option<String>> {
    let credential_name = construct_keychain_credential_name(profile);

    let credential = match Entry::new("snouty", credential_name.as_str()) {
        Ok(cred) => Ok(cred),
        // A NoDefaultStore error indicates that the version of initialize_credential_store() selected by the compiler was a no-op
        Err(keyring_core::Error::NoDefaultStore) => return Ok(None),
        Err(other) => Err(other),
    }?;

    credential.set_password(serde_json::to_string(credentials)?.as_str())?;

    clear_from_file_if_present(profile);

    Ok(Some(credential_name))
}

pub(super) fn construct_keychain_credential_name(profile: Option<&str>) -> String {
    profile.map_or_else(|| "_default_".to_owned(), |p| format!("profile_{p}"))
}

fn clear_from_file_if_present(profile: Option<&str>) {
    let Some((parent_dir, path)) = try_get_credentials_file_path() else {
        return;
    };

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
            match rewrite_credentials_file(&creds_file, &parent_dir, &path) {
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

pub(super) fn persist_to_file(
    credentials: PersistableCredentials,
    profile: Option<&str>,
    path: Option<&PathBuf>,
) -> Result<PathBuf> {
    let (settings_dir, path) = match path {
        None =>  try_get_credentials_file_path().ok_or_eyre(
            "Could not determine settings directory. Please ensure $XDG_CONFIG_HOME or $HOME is set",
        )?,
        Some(explicit_path) => match explicit_path.parent() {
            None => return Err(eyre!("Unable to determine parent directory of {}", explicit_path.to_str().unwrap_or("[invalid path]"))),
            Some(parent_dir) => (parent_dir.to_path_buf(), explicit_path.clone()),
        }
    };

    mkdir(&settings_dir, true, 0o700)?;

    let mut current_contents = match read_to_string_if_file_exists(&path)? {
        Some(contents) => {
            if let Ok(file) = parse_credentials_file_toml(&contents, &path) {
                file
            } else {
                let backup = back_up_unparsable_file(&path)?;
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

    rewrite_credentials_file(&current_contents, &settings_dir, &path)?;

    Ok(path)
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
