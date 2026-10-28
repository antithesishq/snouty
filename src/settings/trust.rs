//! Trust on first use for the project settings file.
//!
//! A project settings file can set `base_url` and `https_proxy`, which decide
//! where snouty sends the user's credentials, and a cloned repository can ship
//! one. So snouty uses `./.snouty.toml` only after the user trusts it, and asks
//! again when its content changes. The decisions live in the global settings
//! dir, keyed by the file's canonical path.

use std::{
    collections::BTreeMap,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    str::FromStr,
};

use color_eyre::Section;
use color_eyre::eyre::{OptionExt, Report, Result, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    NO_GLOBAL_SETTINGS_DIR, ProjectFileSource, global_settings_dir, read_to_string_if_file_exists,
    write_global_toml,
};
use crate::env;
use crate::error::user_error;

pub const SETTINGS_TRUST_VAR_NAME: &str = "SNOUTY_SETTINGS_TRUST";
const TRUST_FILENAME: &str = "trust.toml";

/// The values of `SNOUTY_SETTINGS_TRUST`.
#[derive(Clone, Copy)]
pub(super) enum TrustOverride {
    /// Use every project settings file, as if the user trusted it.
    All,
}

impl FromStr for TrustOverride {
    type Err = Report;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "all" => Ok(Self::All),
            _ => Err(user_error(format!(
                "invalid {SETTINGS_TRUST_VAR_NAME} value `{value}`: expected `all`"
            ))),
        }
    }
}

/// The user's decision about one settings file.
#[derive(Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "lowercase")]
enum Decision {
    /// Use the file while its content has this SHA-256 digest.
    Trust { sha256: String },
    /// Ignore the file, whatever its content.
    Deny,
}

#[derive(Default, Serialize, Deserialize)]
struct TrustStore {
    #[serde(default)]
    files: BTreeMap<PathBuf, Decision>,
}

impl TrustStore {
    /// The store in the global settings dir `dir`; empty when there is none yet.
    fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(TRUST_FILENAME);
        match read_to_string_if_file_exists(&path)? {
            Some(contents) => toml::from_str(&contents).map_err(|err| {
                user_error(format!(
                    "the trust file {} is not valid: {err}",
                    path.display()
                ))
                .suggestion(format!(
                    "Delete the file, then trust or deny your project settings files again. \
                         To skip the check, set {SETTINGS_TRUST_VAR_NAME}=all."
                ))
            }),
            None => Ok(Self::default()),
        }
    }
}

impl TrustOverride {
    /// `None` when `SNOUTY_SETTINGS_TRUST` is not set.
    pub(super) fn from_env() -> Result<Option<Self>> {
        env::var(SETTINGS_TRUST_VAR_NAME)?
            .map(|value| value.parse())
            .transpose()
    }
}

/// The verdict for the project settings file at `path`, whose content is
/// `contents`: [`ProjectFileSource::Trusted`], [`ProjectFileSource::TrustAll`]
/// or [`ProjectFileSource::Denied`]. A file that the user has not trusted, or
/// that changed since the user trusted it, is an error that tells how to
/// decide.
pub(super) fn verdict(
    path: &Path,
    contents: &[u8],
    trust_override: Option<TrustOverride>,
) -> Result<ProjectFileSource> {
    if let Some(TrustOverride::All) = trust_override {
        return Ok(ProjectFileSource::TrustAll);
    }
    let path = canonicalize(path)?;
    let Some(dir) = global_settings_dir() else {
        return Err(user_error(format!(
            "the settings file {} is not trusted, and snouty has no place to record trust",
            path.display()
        ))
        .suggestion(format!(
            "Set $XDG_CONFIG_HOME or $HOME. To skip the check, set {SETTINGS_TRUST_VAR_NAME}=all."
        )));
    };
    match TrustStore::load(&dir)?.files.get(&path) {
        Some(Decision::Deny) => Ok(ProjectFileSource::Denied),
        Some(Decision::Trust { sha256 }) if *sha256 == digest(contents) => {
            Ok(ProjectFileSource::Trusted)
        }
        Some(Decision::Trust { .. }) => Err(untrusted(&path, "changed since you trusted it")),
        None => Err(untrusted(&path, "is not trusted")),
    }
}

fn untrusted(path: &Path, problem: &str) -> Report {
    user_error(format!("the settings file {} {problem}", path.display()))
        .note("A project settings file can set where snouty sends your credentials.")
        .suggestion(format!(
            "Review the file. Then run `snouty settings trust` to use it, or `snouty settings deny` \
             to ignore it. To skip this check, for example in CI, set {SETTINGS_TRUST_VAR_NAME}=all."
        ))
}

/// Trust the settings file at `path` with its current content. Returns the
/// canonical path that the decision is recorded for.
pub fn trust(path: &Path) -> Result<PathBuf> {
    let path = canonicalize(path)?;
    let contents = read_to_string_if_file_exists(&path)?.ok_or_else(|| not_found(&path))?;
    record(
        path,
        Decision::Trust {
            sha256: digest(contents.as_bytes()),
        },
    )
}

/// Deny the settings file at `path`. Returns the canonical path that the
/// decision is recorded for.
pub fn deny(path: &Path) -> Result<PathBuf> {
    record(canonicalize(path)?, Decision::Deny)
}

fn record(path: PathBuf, decision: Decision) -> Result<PathBuf> {
    let dir = global_settings_dir().ok_or_eyre(NO_GLOBAL_SETTINGS_DIR)?;
    let mut store = TrustStore::load(&dir)?;
    store.files.insert(path.clone(), decision);
    write_global_toml(TRUST_FILENAME, &store)?;
    Ok(path)
}

/// The canonical form of `path`; an error when `path` does not name a file.
fn canonicalize(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).map_err(|err| {
        if err.kind() == ErrorKind::NotFound {
            not_found(path)
        } else {
            eyre!("File at {path:?} could not be read: {err:#}")
        }
    })?;
    if !canonical.is_file() {
        return Err(user_error(format!(
            "{} is not a settings file",
            path.display()
        )));
    }
    Ok(canonical)
}

fn not_found(path: &Path) -> Report {
    user_error(format!(
        "the settings file {} was not found",
        path.display()
    ))
}

fn digest(contents: &[u8]) -> String {
    format!("{:x}", Sha256::digest(contents))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_round_trips_both_decisions() {
        let mut store = TrustStore::default();
        store.files.insert(
            PathBuf::from("/a/.snouty.toml"),
            Decision::Trust {
                sha256: digest(b"tenant = \"acme\""),
            },
        );
        store
            .files
            .insert(PathBuf::from("/b/.snouty.toml"), Decision::Deny);
        let text = toml::to_string_pretty(&store).unwrap();
        let back: TrustStore = toml::from_str(&text).unwrap();
        assert!(matches!(
            back.files.get(Path::new("/a/.snouty.toml")),
            Some(Decision::Trust { sha256 }) if *sha256 == digest(b"tenant = \"acme\"")
        ));
        assert!(matches!(
            back.files.get(Path::new("/b/.snouty.toml")),
            Some(Decision::Deny)
        ));
    }
}
