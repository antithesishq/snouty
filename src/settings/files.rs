//! The settings files that [`super::Settings::resolve`] read.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::SNOUTY_SETTINGS_PATH_VAR_NAME;

/// Where snouty found a settings file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingsFileSource {
    /// The global `settings.toml`.
    Global,
    /// `./.snouty.toml`.
    CurrentDirectory,
    /// Named with the `--settings` flag.
    SettingsFlag,
    /// Named with `SNOUTY_SETTINGS_PATH`.
    SettingsPathEnv,
}

impl fmt::Display for SettingsFileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Global => "global",
            Self::CurrentDirectory => "current directory",
            Self::SettingsFlag => "--settings",
            Self::SettingsPathEnv => SNOUTY_SETTINGS_PATH_VAR_NAME,
        })
    }
}

/// A settings file that snouty read.
#[derive(Debug, Serialize)]
pub struct SettingsFile {
    pub path: PathBuf,
    pub source: SettingsFileSource,
}

impl SettingsFile {
    /// Keeps `path` absolute, with symlinks resolved. The file exists, so
    /// `canonicalize` fails only in a race; the path as given still names the
    /// file then.
    pub(super) fn new(path: &Path, source: SettingsFileSource) -> Self {
        Self {
            path: fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
            source,
        }
    }
}
