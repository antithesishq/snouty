//! The settings files that [`super::Settings::resolve`] read.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

/// Which of snouty's two settings files a file is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SettingsFileType {
    /// `--settings`, `SNOUTY_SETTINGS_PATH`, or `./.snouty.toml`.
    Project,
    /// `settings.toml` in the global settings dir.
    Global,
}

impl fmt::Display for SettingsFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Project => "project",
            Self::Global => "global",
        })
    }
}

/// A settings file that snouty read.
#[derive(Debug, Serialize)]
pub struct SettingsFile {
    #[serde(rename = "type")]
    pub file_type: SettingsFileType,
    pub path: PathBuf,
}

impl SettingsFile {
    /// Keeps `path` absolute, with symlinks resolved. The file exists, so
    /// `canonicalize` fails only in a race; the path as given still names the
    /// file then.
    pub(super) fn new(file_type: SettingsFileType, path: &Path) -> Self {
        Self {
            file_type,
            path: fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        }
    }
}
