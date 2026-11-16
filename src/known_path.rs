//! The TOML files that snouty reads from known places, and the tables it
//! loads from them.

use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use color_eyre::Section;
use color_eyre::eyre::{Result, eyre};
use serde::Serialize;
use toml::Table;

use crate::error::user_error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KnownPathKind {
    /// `--settings`, `SNOUTY_SETTINGS_PATH`, or `./.snouty.toml`.
    Project,
    /// `settings.toml` in the global settings dir.
    Global,
    /// `credentials.toml` in the global settings dir.
    Credentials,
    /// `trust.toml` in the global settings dir.
    Trust,
}

impl KnownPathKind {
    /// What a message calls a file of this type.
    fn noun(self) -> &'static str {
        match self {
            Self::Project | Self::Global => "settings file",
            Self::Credentials => "credentials file",
            Self::Trust => "trust file",
        }
    }
}

impl fmt::Display for KnownPathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Project => "project",
            Self::Global => "global",
            Self::Credentials => "credentials",
            Self::Trust => "trust",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KnownPath {
    #[serde(rename = "type")]
    pub kind: KnownPathKind,
    /// Absolute, with symlinks resolved, when the file exists. Otherwise the
    /// path as given.
    pub path: PathBuf,
}

impl KnownPath {
    pub(crate) fn new(kind: KnownPathKind, path: &Path) -> Self {
        Self {
            kind,
            path: fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        }
    }

    /// The file's text. `Ok(None)` when the file does not exist and is not
    /// `required`.
    pub(crate) fn read(self, required: bool) -> Result<Option<KnownFile>> {
        match read_to_string_if_file_exists(&self.path)? {
            Some(text) => Ok(Some(KnownFile { path: self, text })),
            None if required => Err(user_error(format!(
                "the {} {} was not found",
                self.kind.noun(),
                self.path.display()
            ))),
            None => Ok(None),
        }
    }

    /// The file as a TOML table. `Ok(None)` when the file does not exist and
    /// is not `required`.
    pub(crate) fn load(self, required: bool) -> Result<Option<LoadedFile>> {
        self.read(required)?.map(|file| file.load()).transpose()
    }
}

/// The text of a file, as [`KnownPath::read`] read it.
pub(crate) struct KnownFile {
    pub(crate) path: KnownPath,
    pub(crate) text: String,
}

impl KnownFile {
    /// The text as a TOML table.
    pub(crate) fn load(&self) -> Result<LoadedFile> {
        let path = self.path.clone();
        match self.text.parse::<Table>() {
            Ok(table) => Ok(LoadedFile { path, table }),
            Err(err) => Err(user_error(format!(
                "{} {} is not valid TOML: {}",
                path.kind.noun(),
                path.path.display(),
                format!("{err:#}").trim_end()
            ))
            .suggestion(format!("fix or delete {}", path.path.display()))),
        }
    }
}

/// A TOML file that snouty read.
#[derive(Debug)]
pub(crate) struct LoadedFile {
    pub(crate) path: KnownPath,
    pub(crate) table: Table,
}

pub(crate) fn read_to_string_if_file_exists(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(eyre!("File at {:?} could not be read: {err:#}", path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_file_loads_as_none_unless_it_is_required() {
        let path = || KnownPath::new(KnownPathKind::Project, Path::new("/no/such/.snouty.toml"));
        assert!(path().load(false).unwrap().is_none());
        let err = path().load(true).unwrap_err();
        assert_eq!(
            err.to_string(),
            "the settings file /no/such/.snouty.toml was not found"
        );
    }

    #[test]
    fn invalid_toml_is_reported_with_path() {
        let file = KnownFile {
            path: KnownPath::new(KnownPathKind::Project, Path::new("/some/.snouty.toml")),
            text: "this is = = not toml".to_owned(),
        };
        let msg = file.load().unwrap_err().to_string();
        assert!(msg.contains("not valid TOML"), "unexpected error: {msg}");
        assert!(msg.contains(".snouty.toml"), "unexpected error: {msg}");
    }
}
