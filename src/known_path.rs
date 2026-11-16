//! The TOML files that snouty reads from known places, and the tables it
//! loads from them.

use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use color_eyre::Section;
use color_eyre::eyre::{Report, Result, eyre};
use serde::Serialize;
use toml::Table;

use crate::error::user_error;

/// Which file a [`KnownPath`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KnownPathType {
    /// `--settings`, `SNOUTY_SETTINGS_PATH`, or `./.snouty.toml`.
    Project,
    /// `settings.toml` in the global settings dir.
    Global,
    /// `credentials.toml` in the global settings dir.
    Credentials,
    /// `trust.toml` in the global settings dir.
    Trust,
}

impl KnownPathType {
    /// What a message calls a file of this type.
    fn noun(self) -> &'static str {
        match self {
            Self::Project | Self::Global => "settings file",
            Self::Credentials => "credentials file",
            Self::Trust => "trust file",
        }
    }
}

impl fmt::Display for KnownPathType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Project => "project",
            Self::Global => "global",
            Self::Credentials => "credentials",
            Self::Trust => "trust",
        })
    }
}

/// A file that snouty reads, and which file it is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KnownPath {
    #[serde(rename = "type")]
    pub kind: KnownPathType,
    /// Absolute, with symlinks resolved, when the file exists. Otherwise the
    /// path as given.
    pub path: PathBuf,
}

impl KnownPath {
    pub fn new(kind: KnownPathType, path: &Path) -> Self {
        Self {
            kind,
            path: fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        }
    }

    /// The file's text. `Ok(None)` when the file does not exist and is not
    /// `required`.
    pub fn read(&self, required: bool) -> Result<Option<String>> {
        match read_to_string_if_file_exists(&self.path)? {
            None if required => Err(user_error(format!(
                "the {} {} was not found",
                self.kind.noun(),
                self.path.display()
            ))),
            contents => Ok(contents),
        }
    }

    /// `contents`, the text of this file, as a TOML table.
    pub fn parse(self, contents: &str) -> Result<LoadedFile> {
        match contents.parse::<Table>() {
            Ok(table) => Ok(LoadedFile { path: self, table }),
            Err(err) => Err(InvalidFile {
                message: format!(
                    "{} {} is not valid TOML: {}",
                    self.kind.noun(),
                    self.path.display(),
                    format!("{err:#}").trim_end()
                ),
                path: self,
            }
            .report()),
        }
    }

    /// The file as a TOML table. `Ok(None)` when the file does not exist and
    /// is not `required`.
    pub fn load(self, required: bool) -> Result<Option<LoadedFile>> {
        self.read(required)?
            .map(|contents| self.parse(&contents))
            .transpose()
    }
}

/// A TOML file that snouty read.
#[derive(Debug)]
pub struct LoadedFile {
    pub path: KnownPath,
    pub table: Table,
}

impl LoadedFile {
    /// `err`, a fault in this file, as an [`InvalidFile`].
    pub fn error(&self, err: &Report) -> Report {
        InvalidFile {
            path: self.path.clone(),
            message: format!("{}: {err}", self.path.path.display()),
        }
        .report()
    }
}

/// A file that snouty cannot use as a whole, such as TOML that does not
/// parse. The message names the file.
#[derive(Debug)]
pub struct InvalidFile {
    pub path: KnownPath,
    message: String,
}

impl InvalidFile {
    fn report(self) -> Report {
        Report::new(self).suppress_backtrace(true)
    }
}

impl fmt::Display for InvalidFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InvalidFile {}

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
        let path = || KnownPath::new(KnownPathType::Project, Path::new("/no/such/.snouty.toml"));
        assert!(path().load(false).unwrap().is_none());
        let err = path().load(true).unwrap_err();
        assert_eq!(
            err.to_string(),
            "the settings file /no/such/.snouty.toml was not found"
        );
    }
}
