//! The files that `settings show` lists: the settings files that
//! [`super::Settings::resolve`] read, and the credentials file.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

/// Which of snouty's files a file is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SettingsFileType {
    /// `--settings`, `SNOUTY_SETTINGS_PATH`, or `./.snouty.toml`.
    Project,
    /// `settings.toml` in the global settings dir.
    Global,
    /// `credentials.toml` in the global settings dir, which `snouty login`
    /// writes.
    Credentials,
}

impl fmt::Display for SettingsFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Project => "project",
            Self::Global => "global",
            Self::Credentials => "credentials",
        })
    }
}

/// A file that snouty read.
#[derive(Clone, Debug, Serialize)]
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
