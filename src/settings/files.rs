//! The settings files that [`super::Settings::resolve`] read.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::{SETTINGS_TRUST_VAR_NAME, SNOUTY_SETTINGS_PATH_VAR_NAME};

/// Why snouty read, or skipped, the project settings file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectFileSource {
    /// Named with the `--settings` flag.
    SettingsFlag,
    /// Named with `SNOUTY_SETTINGS_PATH`.
    SettingsPathEnv,
    /// `./.snouty.toml`, which the user trusts.
    Trusted,
    /// `./.snouty.toml`, used without a check because of `SNOUTY_SETTINGS_TRUST=all`.
    TrustAll,
    /// `./.snouty.toml`, which the user denied. snouty ignores it.
    Denied,
}

impl fmt::Display for ProjectFileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SettingsFlag => f.write_str("--settings"),
            Self::SettingsPathEnv => f.write_str(SNOUTY_SETTINGS_PATH_VAR_NAME),
            Self::Trusted => f.write_str("trusted"),
            Self::TrustAll => write!(f, "{SETTINGS_TRUST_VAR_NAME}=all"),
            Self::Denied => f.write_str("denied, ignored"),
        }
    }
}

/// A project settings file that exists, and why snouty read or skipped it.
#[derive(Debug, Serialize)]
pub struct ProjectFile {
    pub path: PathBuf,
    pub source: ProjectFileSource,
}

impl ProjectFile {
    /// `path` as the user should read it: absolute, with symlinks resolved.
    pub(super) fn new(path: &Path, source: ProjectFileSource) -> Self {
        Self {
            path: absolute(path),
            source,
        }
    }
}

/// The settings files that exist: the project file, and the global
/// `settings.toml`.
#[derive(Debug, Default, Serialize)]
pub struct SettingsFiles {
    pub project: Option<ProjectFile>,
    pub global: Option<PathBuf>,
}

/// `path` made absolute, for display. The file exists, so `canonicalize` fails
/// only in a race; the path as given still names the file then.
pub(super) fn absolute(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
