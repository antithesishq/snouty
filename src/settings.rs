//! Snouty's settings, read from a project settings file (`.snouty.toml`, in the
//! current working directory by default) and a global `settings.toml`, with
//! environment variables taking precedence over both.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Context, Report, Result, eyre};
use toml::Table;

pub const ANTITHESIS_PROFILE_ENV_VAR_NAME: &str = "ANTITHESIS_PROFILE";
pub const SNOUTY_SETTINGS_PATH_VAR_NAME: &str = "SNOUTY_SETTINGS_PATH";
pub const ANTITHESIS_TENANT_VAR_NAME: &str = "ANTITHESIS_TENANT";
pub const ANTITHESIS_REPOSITORY_VAR_NAME: &str = "ANTITHESIS_REPOSITORY";
pub const ANTITHESIS_BASE_URL_VAR_NAME: &str = "ANTITHESIS_BASE_URL";
pub const CONTAINER_ENGINE_VAR_NAME: &str = "SNOUTY_CONTAINER_ENGINE";
/// The project settings file, looked up in the current working directory unless
/// overridden by `--settings`/`SNOUTY_SETTINGS_PATH`.
const DEFAULT_SETTINGS_FILENAME: &str = ".snouty.toml";
/// The global settings file, under the user's config directory.
const GLOBAL_SETTINGS_FILENAME: &str = "settings.toml";
const PROFILE_KEY: &str = "profile";

/// Fully-resolved snouty settings.
///
/// Built once per invocation by [`Settings::resolve`], which merges — in
/// descending precedence — environment variables, the active profile, and
/// top-level defaults from the project settings file and the global
/// `settings.toml`. Every command shares the same resolved instance (threaded
/// by reference), so a value resolves identically no matter which code path
/// reads it.
#[derive(Debug, Default)]
pub struct Settings {
    tenant: Option<String>,
    repository: Option<String>,
    base_url: Option<String>,
    container_engine: Option<String>,
}

impl Settings {
    /// Resolve settings from the environment, the project settings file, and the
    /// global `settings.toml`.
    ///
    /// The project settings file is located by, in descending precedence:
    /// `SNOUTY_SETTINGS_PATH`, the `--settings` flag (`settings_path`), then
    /// `./.snouty.toml` in the current working directory. The active profile is
    /// `ANTITHESIS_PROFILE` if set, otherwise the `--profile` flag (`profile`).
    ///
    /// A settings file that exists but cannot be read or parsed is a hard error,
    /// as is an explicitly-requested file (via flag or env var) that is missing:
    /// silently ignoring it would resurface later as a confusing "No tenant
    /// found". A missing *default* `./.snouty.toml` is fine.
    pub fn resolve(settings_path: Option<PathBuf>, profile: Option<String>) -> Result<Self> {
        // An empty value means "unset", not "a profile named the empty string".
        let profile = env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME)
            .ok()
            .filter(|p| !p.is_empty())
            .or_else(|| profile.filter(|p| !p.is_empty()));

        // Settings file location: env var, then `--settings`, then
        // `./.snouty.toml` (CWD). Read with `var` like every other env var
        // here — settings values are UTF-8 (they can come from a TOML file),
        // so the path is too.
        let settings_path = env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)
            .ok()
            .map(PathBuf::from)
            .or(settings_path);
        let settings_path_is_explicit = settings_path.is_some();
        let settings_path =
            settings_path.unwrap_or_else(|| PathBuf::from(DEFAULT_SETTINGS_FILENAME));

        let project = load_settings_file(&settings_path, settings_path_is_explicit)?;
        let global = match global_settings_dir() {
            Some(dir) => load_settings_file(&dir.join(GLOBAL_SETTINGS_FILENAME), false)?,
            None => None,
        };

        let resolve = |key: &str, env_var: &str| {
            resolve_value(
                key,
                env_var,
                profile.as_deref(),
                project.as_ref(),
                global.as_ref(),
            )
        };

        Ok(Self {
            tenant: resolve("tenant", ANTITHESIS_TENANT_VAR_NAME),
            repository: resolve("repository", ANTITHESIS_REPOSITORY_VAR_NAME),
            base_url: resolve("base_url", ANTITHESIS_BASE_URL_VAR_NAME),
            container_engine: resolve("container_engine", CONTAINER_ENGINE_VAR_NAME),
        })
    }

    pub fn tenant(&self) -> Result<&str> {
        self.tenant
            .as_deref()
            .ok_or_else(|| missing_setting_error("tenant", ANTITHESIS_TENANT_VAR_NAME))
    }

    pub fn repository(&self) -> Result<&str> {
        self.repository
            .as_deref()
            .ok_or_else(|| missing_setting_error("repository", ANTITHESIS_REPOSITORY_VAR_NAME))
    }

    /// The base URL to talk to: an explicit `base_url` setting if present,
    /// otherwise derived from the tenant. Errors with the tenant's diagnostic
    /// when neither exists.
    pub fn resolve_base_url(&self) -> Result<String> {
        match self.base_url.as_deref() {
            Some(base_url) => Ok(base_url.to_owned()),
            None => Ok(format!("https://{}.antithesis.com", self.tenant()?)),
        }
    }

    pub fn container_engine(&self) -> Option<&str> {
        self.container_engine.as_deref()
    }

    /// Test-only: `Settings` with an explicit base URL and everything else
    /// unset, for driving [`crate::api::AntithesisApi`] against a mock server
    /// without touching the environment.
    #[cfg(test)]
    pub(crate) fn for_test_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
            ..Self::default()
        }
    }
}

/// The snouty cache directory: `$XDG_CACHE_HOME/snouty` (falling back to
/// `$HOME/.cache/snouty`). Used for the docs database and the API response
/// cache. There is no snouty-specific override — point `XDG_CACHE_HOME`
/// elsewhere to relocate it. `None` only when neither `XDG_CACHE_HOME` nor
/// `HOME` is set.
pub fn cache_dir() -> Option<PathBuf> {
    xdg_snouty_dir("XDG_CACHE_HOME", ".cache")
}

fn missing_setting_error(setting_key: &str, environment_variable_name: &str) -> Report {
    eyre!(
        "No {setting_key} found: set the {environment_variable_name} environment variable, \
         or add `{setting_key} = \"...\"` to your snouty settings file (.snouty.toml)"
    )
}

/// `$XDG_CONFIG_HOME`/`$XDG_CACHE_HOME` if set, else `$HOME/<home_subdir>`,
/// then joined with `snouty`. `None` only when neither the XDG var nor `HOME`
/// is set (e.g. Windows).
fn xdg_snouty_dir(xdg_var: &str, home_subdir: &str) -> Option<PathBuf> {
    let base_dir = if let Ok(xdg_dir) = env::var(xdg_var) {
        Some(PathBuf::from(xdg_dir))
    } else if let Ok(home) = env::var("HOME") {
        Some(PathBuf::from(home).join(home_subdir))
    } else {
        None
    };

    base_dir.map(|dir| dir.join("snouty"))
}

fn global_settings_dir() -> Option<PathBuf> {
    xdg_snouty_dir("XDG_CONFIG_HOME", ".config")
}

/// Load and parse a settings file. `Ok(None)` when the file simply does not
/// exist and was not explicitly requested; an error when it exists but cannot
/// be read or parsed, or when an explicitly-requested file is missing.
fn load_settings_file(path: &Path, required: bool) -> Result<Option<Table>> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            let table = contents
                .parse::<Table>()
                .wrap_err_with(|| format!("failed to parse settings file {}", path.display()))?;
            Ok(Some(table))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if required {
                Err(eyre!("settings file not found: {}", path.display()))
            } else {
                Ok(None)
            }
        }
        Err(err) => {
            Err(err).wrap_err_with(|| format!("failed to read settings file {}", path.display()))
        }
    }
}

/// Resolve a single value with the precedence: environment variable, then the
/// active profile (project file before global file), then top-level defaults
/// (project file before global file).
fn resolve_value(
    key: &str,
    environment_variable_name: &str,
    profile: Option<&str>,
    project: Option<&Table>,
    global: Option<&Table>,
) -> Option<String> {
    // The environment variable always has highest precedence.
    if let Ok(value) = env::var(environment_variable_name) {
        return Some(value);
    }

    // A named profile is consulted before defaults, project before global.
    if let Some(profile) = profile {
        for table in [project, global].into_iter().flatten() {
            if let Some(value) = profile_value(table, profile, key) {
                return Some(value);
            }
        }
    }

    // Finally fall back to top-level defaults, project before global.
    for table in [project, global].into_iter().flatten() {
        if let Some(value) = default_value(table, key) {
            return Some(value);
        }
    }

    None
}

fn profile_value(table: &Table, profile: &str, key: &str) -> Option<String> {
    table
        .get(PROFILE_KEY)?
        .as_table()?
        .get(profile)?
        .as_table()?
        .get(key)?
        .as_str()
        .map(str::to_owned)
}

fn default_value(table: &Table, key: &str) -> Option<String> {
    table.get(key)?.as_str().map(str::to_owned)
}
