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
use color_eyre::eyre::{Report, Result, eyre};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{global_settings_dir, read_to_string_if_file_exists, write_global_toml};
use crate::env;
use crate::error::user_error;

pub const CONFIG_TRUST_VAR_NAME: &str = "SNOUTY_CONFIG_TRUST";
const TRUST_FILENAME: &str = "trust.toml";

/// The values of `SNOUTY_CONFIG_TRUST`.
enum TrustOverride {
    /// Use every project settings file, as if the user trusted it.
    All,
}

impl FromStr for TrustOverride {
    type Err = Report;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "all" => Ok(Self::All),
            _ => Err(user_error(format!(
                "invalid {CONFIG_TRUST_VAR_NAME} value `{value}`: expected `all`"
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
    /// The store in the global settings dir; empty when there is none yet.
    fn load() -> Result<Self> {
        let Some(path) = global_settings_dir().map(|dir| dir.join(TRUST_FILENAME)) else {
            return Ok(Self::default());
        };
        match read_to_string_if_file_exists(&path)? {
            Some(contents) => toml::from_str(&contents)
                .map_err(|err| eyre!("Trust file at {path:?} was not valid: {err:#}")),
            None => Ok(Self::default()),
        }
    }
}

/// What snouty does with a project settings file that it may use.
pub(super) enum Verdict {
    Use,
    Ignore,
}

/// The verdict for the project settings file at `path`, whose content is
/// `contents`. A file that the user has not trusted, or that changed since the
/// user trusted it, is an error that tells how to decide.
pub(super) fn verdict(path: &Path, contents: &[u8]) -> Result<Verdict> {
    if let Some(TrustOverride::All) = env::var(CONFIG_TRUST_VAR_NAME)?
        .map(|value| value.parse())
        .transpose()?
    {
        return Ok(Verdict::Use);
    }
    let path = canonicalize(path)?;
    match TrustStore::load()?.files.get(&path) {
        Some(Decision::Deny) => Ok(Verdict::Ignore),
        Some(Decision::Trust { sha256 }) if *sha256 == digest(contents) => Ok(Verdict::Use),
        Some(Decision::Trust { .. }) => Err(untrusted(&path, "changed since you trusted it")),
        None => Err(untrusted(&path, "is not trusted")),
    }
}

fn untrusted(path: &Path, problem: &str) -> Report {
    user_error(format!("the settings file {} {problem}", path.display()))
        .note("A project settings file can set where snouty sends your credentials.")
        .suggestion(format!(
            "Review the file. Then run `snouty config trust` to use it, or `snouty config deny` \
             to ignore it. To skip this check, for example in CI, set {CONFIG_TRUST_VAR_NAME}=all."
        ))
}

/// Trust the settings file at `path` with its current content. Returns the
/// canonical path that the decision is recorded for.
pub fn trust(path: &Path) -> Result<PathBuf> {
    let path = canonicalize(path)?;
    let contents = fs::read(&path).map_err(|err| read_error(&path, &err))?;
    record(
        path,
        Decision::Trust {
            sha256: digest(&contents),
        },
    )
}

/// Deny the settings file at `path`. Returns the canonical path that the
/// decision is recorded for.
pub fn deny(path: &Path) -> Result<PathBuf> {
    record(canonicalize(path)?, Decision::Deny)
}

fn record(path: PathBuf, decision: Decision) -> Result<PathBuf> {
    let mut store = TrustStore::load()?;
    store.files.insert(path.clone(), decision);
    write_global_toml(TRUST_FILENAME, &store)?;
    Ok(path)
}

fn canonicalize(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|err| {
        if err.kind() == ErrorKind::NotFound {
            user_error(format!(
                "the settings file {} was not found",
                path.display()
            ))
        } else {
            read_error(path, &err)
        }
    })
}

fn read_error(path: &Path, err: &std::io::Error) -> Report {
    eyre!("File at {path:?} could not be read: {err:#}")
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
