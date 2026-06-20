use std::{
    env, fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, eyre};
use toml::Table;

pub const ANTITHESIS_PROFILE_ENV_VAR_NAME: &str = "ANTITHESIS_PROFILE";
pub const SNOUTY_SETTINGS_PATH_VAR_NAME: &str = "SNOUTY_SETTINGS_PATH";
pub const ANTITHESIS_TENANT_VAR_NAME: &str = "ANTITHESIS_TENANT";
pub const ANTITHESIS_REPOSITORY_VAR_NAME: &str = "ANTITHESIS_REPOSITORY";
pub const ANTITHESIS_BASE_URL_VAR_NAME: &str = "ANTITHESIS_BASE_URL";
pub const CONTAINER_ENGINE_VAR_NAME: &str = "SNOUTY_CONTAINER_ENGINE";
const PROJECT_SETTINGS_FILENAME: &str = ".snouty.toml";
const GLOBAL_SETTINGS_FILENAME: &str = "settings.toml";
const PROFILE_KEY: &str = "profile";

/// snouty's subdirectory under an XDG base dir: `$<xdg_var>/snouty`, falling
/// back to `$HOME/<home_subdir>/snouty`. `None` when neither is set (e.g.
/// Windows). Deliberately hand-rolled rather than via the `dirs` crate, which on
/// macOS resolves to `~/Library/...` instead of the XDG layout snouty wants.
fn xdg_snouty_dir(xdg_var: &str, home_subdir: &str) -> Option<PathBuf> {
    let base = if let Ok(dir) = env::var(xdg_var) {
        PathBuf::from(dir)
    } else {
        PathBuf::from(env::var("HOME").ok()?).join(home_subdir)
    };
    Some(base.join("snouty"))
}

/// Directory holding the global settings file: `$XDG_CONFIG_HOME/snouty`,
/// falling back to `$HOME/.config/snouty`. `None` only when neither
/// `XDG_CONFIG_HOME` nor `HOME` is set (e.g. Windows).
fn global_settings_dir() -> Option<PathBuf> {
    xdg_snouty_dir("XDG_CONFIG_HOME", ".config")
}

pub fn cache_dir() -> Option<PathBuf> {
    xdg_snouty_dir("XDG_CACHE_HOME", ".cache")
}

/// snouty's resolved settings.
///
/// Everything is resolved eagerly in [`Settings::resolve`]: the environment,
/// the project settings file (`.snouty.toml`), and the global `settings.toml`
/// are read once, each setting is resolved through the precedence chain, and
/// the result is plain owned data. A settings file that exists but can't be read
/// or parsed is a hard error at construction — so by the time a `Settings`
/// exists, every value is either resolved or simply absent. No value is
/// recomputed and nothing fails later, which is what lets the accessors hand out
/// `&str`/`Option<&str>` instead of cached `Result`s.
///
/// Every command shares the same resolved instance (threaded by reference), so a
/// value resolves identically no matter which code path reads it.
pub struct Settings {
    profile: Option<String>,
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
    /// `SNOUTY_SETTINGS_PATH`, the `--settings` flag (`project_settings_path`),
    /// then `./.snouty.toml` in the current working directory. The active profile
    /// is the `--profile` flag if set, otherwise `ANTITHESIS_PROFILE`.
    ///
    /// A settings file that exists but cannot be read or parsed is a hard error,
    /// as is an explicitly-requested file (via flag or env var) that is missing:
    /// silently ignoring it would resurface later as a confusing "tenant not
    /// set". A missing *default* `./.snouty.toml` is fine.
    pub fn resolve(
        project_settings_path: Option<PathBuf>,
        profile: Option<String>,
    ) -> Result<Self> {
        // An empty value means "unset", not "a profile named the empty string".
        let profile = profile
            .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok())
            .filter(|profile| !profile.is_empty());

        let project_settings_path = project_settings_path.or_else(|| {
            env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)
                .ok()
                .map(PathBuf::from)
        });

        // An explicitly-requested project file must exist; the default
        // `./.snouty.toml` is optional. (Climbing the directory tree, if ever
        // wanted, would go here.)
        let project = match &project_settings_path {
            Some(path) => load_settings_file(path, true)?,
            None => load_settings_file(Path::new(PROJECT_SETTINGS_FILENAME), false)?,
        };
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

        let tenant = resolve("tenant", ANTITHESIS_TENANT_VAR_NAME)?;
        let repository = resolve("repository", ANTITHESIS_REPOSITORY_VAR_NAME)?;
        let base_url = resolve("base_url", ANTITHESIS_BASE_URL_VAR_NAME)?;
        let container_engine = resolve("container_engine", CONTAINER_ENGINE_VAR_NAME)?;

        Ok(Self::assemble(
            profile,
            tenant,
            repository,
            base_url,
            container_engine,
        ))
    }

    /// Assemble the final `Settings` from already-resolved layers, applying the
    /// one derived value: `base_url` falls back to a tenant-derived host. Shared
    /// by [`Settings::resolve`] and the test constructors so the derivation is
    /// exercised the same way everywhere.
    fn assemble(
        profile: Option<String>,
        tenant: Option<String>,
        repository: Option<String>,
        base_url: Option<String>,
        container_engine: Option<String>,
    ) -> Self {
        let base_url = base_url.or_else(|| {
            tenant
                .as_ref()
                .map(|tenant| format!("https://{tenant}.antithesis.com"))
        });

        Self {
            profile,
            tenant,
            repository,
            base_url,
            container_engine,
        }
    }

    pub fn tenant(&self) -> Result<&str> {
        self.tenant.as_deref().ok_or_else(|| {
            eyre!("Could not resolve Antithesis tenant. Run snouty doctor to debug.")
        })
    }

    pub fn repository(&self) -> Result<&str> {
        self.repository.as_deref().ok_or_else(|| {
            eyre!("Could not resolve Antithesis repository. Run snouty doctor to debug.")
        })
    }

    /// The base URL to talk to: an explicit `base_url` if set, otherwise one
    /// derived from the tenant. Errors with the tenant's diagnostic when neither
    /// exists (a derived `base_url` is present exactly when the tenant is).
    pub fn base_url(&self) -> Result<&str> {
        self.base_url.as_deref().ok_or_else(|| {
            eyre!("Could not resolve Antithesis tenant. Run snouty doctor to debug.")
        })
    }

    pub fn container_engine(&self) -> Option<&str> {
        self.container_engine.as_deref()
    }

    pub(crate) fn settings_profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    pub(crate) fn tenant_setting(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    pub(crate) fn repository_setting(&self) -> Option<&str> {
        self.repository.as_deref()
    }

    /// Test-only: a `Settings` built from explicit values, with no environment or
    /// filesystem IO. `base_url` still derives from the tenant when left `None`.
    #[cfg(test)]
    pub(crate) fn for_test(
        profile: Option<&str>,
        tenant: Option<&str>,
        repository: Option<&str>,
        base_url: Option<&str>,
        container_engine: Option<&str>,
    ) -> Self {
        Self::assemble(
            profile.map(str::to_string),
            tenant.map(str::to_string),
            repository.map(str::to_string),
            base_url.map(str::to_string),
            container_engine.map(str::to_string),
        )
    }

    /// Test-only: a `Settings` with an explicit base URL and everything else
    /// unset, for driving [`crate::api::AntithesisApi`] against a mock server
    /// without touching the environment.
    #[cfg(test)]
    pub(crate) fn for_test_base_url(base_url: String) -> Self {
        Self::for_test(None, None, None, Some(&base_url), None)
    }
}

/// Read an environment variable, treating an exported-but-empty value as unset
/// (common in CI / wrapper shells, where it would otherwise mask a real
/// settings-file value). A non-unicode value is an error.
fn load_env_var(key: &str) -> Result<Option<String>> {
    match env::var(key) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(eyre!(
            "The value of environment variable [{key}] was not valid unicode"
        )),
    }
}

/// Parse settings-file contents into a TOML table, attributing parse errors to
/// the file's path.
fn parse_settings(contents: &str, path: &Path) -> Result<Table> {
    contents
        .parse::<Table>()
        .map_err(|err| eyre!("Settings file at {:?} was not valid TOML: {err:#}", path))
}

/// Load and parse a settings file. `Ok(None)` when the file simply does not
/// exist and was not explicitly requested; an error when it exists but cannot be
/// read or parsed, or when an explicitly-`required` file is missing.
fn load_settings_file(path: &Path, required: bool) -> Result<Option<Table>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => return Ok(None),
        Err(err) if required => {
            return Err(eyre!(
                "Settings file at {:?} could not be found or failed to be read: {err:#}",
                path
            ));
        }
        Err(err) => {
            return Err(eyre!("File at {:?} could not be read: {err:#}", path));
        }
    };
    parse_settings(&contents, path).map(Some)
}

/// Resolve a single setting with the precedence: environment variable, then the
/// active profile (project file before global file), then top-level defaults
/// (project file before global file). The first layer that has the key wins.
fn resolve_value(
    key: &str,
    env_var: &str,
    profile: Option<&str>,
    project: Option<&Table>,
    global: Option<&Table>,
) -> Result<Option<String>> {
    // The environment variable always has highest precedence.
    if let Some(value) = load_env_var(env_var)? {
        return Ok(Some(value));
    }

    // A named profile is consulted before defaults, project before global.
    if let Some(profile) = profile {
        if let Some(value) = project.and_then(|table| profile_value(table, profile, key)) {
            return Ok(Some(value));
        }
        if let Some(value) = global.and_then(|table| profile_value(table, profile, key)) {
            return Ok(Some(value));
        }
    }

    // Finally fall back to top-level defaults, project before global.
    if let Some(value) = project.and_then(|table| default_value(table, key)) {
        return Ok(Some(value));
    }
    if let Some(value) = global.and_then(|table| default_value(table, key)) {
        return Ok(Some(value));
    }

    Ok(None)
}

/// A `[profile.<name>]` value: `profile.<profile>.<key>`.
fn profile_value(table: &Table, profile: &str, key: &str) -> Option<String> {
    table
        .get(PROFILE_KEY)
        .and_then(|profiles| profiles.as_table())
        .and_then(|profiles| profiles.get(profile))
        .and_then(|profile| profile.as_table())
        .and_then(|profile| profile.get(key))
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

/// A top-level default value: `<key>`.
fn default_value(table: &Table, key: &str) -> Option<String> {
    table
        .get(key)
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An env var name guaranteed not to be set, so `resolve_value` exercises
    /// the file layers deterministically under parallel execution (the
    /// environment-wins case is covered end-to-end by `specs/settings.txt`).
    const UNSET_ENV: &str = "SNOUTY_DEFINITELY_NOT_SET_ENV_VAR_98f3a";

    fn settings_file(contents: &str) -> Table {
        contents.parse().expect("test TOML should parse")
    }

    /// Resolve `tenant` against an env var guaranteed not to be set, so the file
    /// layers decide the outcome. Each layer in the precedence tests uses a
    /// distinct value, so the resolved value alone proves which layer won.
    fn resolve_tenant(
        profile: Option<&str>,
        project: Option<&Table>,
        global: Option<&Table>,
    ) -> Option<String> {
        resolve_value("tenant", UNSET_ENV, profile, project, global).unwrap()
    }

    // ---- load_env_var --------------------------------------------------

    #[test]
    fn missing_environment_variable_resolves_to_none() {
        assert!(matches!(load_env_var(UNSET_ENV), Ok(None)));
    }

    // ---- parse_settings ------------------------------------------------

    #[test]
    fn invalid_toml_is_reported_with_path() {
        let err =
            parse_settings("this is = = not toml", Path::new("/some/.snouty.toml")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not valid TOML"), "unexpected error: {msg}");
        assert!(msg.contains(".snouty.toml"), "unexpected error: {msg}");
    }

    // ---- load_settings_file (filesystem) -------------------------------

    #[test]
    fn missing_default_file_is_ok() {
        let result = load_settings_file(Path::new("/no/such/.snouty.toml"), false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn missing_required_file_is_an_error() {
        let err = load_settings_file(Path::new("/no/such/.snouty.toml"), true).unwrap_err();
        assert!(err.to_string().contains("could not be found"));
    }

    // ---- profile_value / default_value ---------------------------------

    #[test]
    fn profile_value_reads_nested_table() {
        let table: Table = "[profile.staging]\ntenant = \"staging-tenant\"\n"
            .parse()
            .unwrap();
        assert_eq!(
            profile_value(&table, "staging", "tenant").as_deref(),
            Some("staging-tenant")
        );
        // missing profile and missing key both resolve to None
        assert_eq!(profile_value(&table, "prod", "tenant"), None);
        assert_eq!(profile_value(&table, "staging", "repository"), None);
    }

    #[test]
    fn default_value_reads_top_level_key() {
        let table: Table = "tenant = \"acme\"\n".parse().unwrap();
        assert_eq!(default_value(&table, "tenant").as_deref(), Some("acme"));
        assert_eq!(default_value(&table, "missing"), None);
    }

    // ---- resolve_value precedence --------------------------------------

    #[test]
    fn project_profile_beats_global_profile_and_all_defaults() {
        let project =
            settings_file("tenant = \"proj-default\"\n[profile.p]\ntenant = \"proj-profile\"\n");
        let global = settings_file(
            "tenant = \"global-default\"\n[profile.p]\ntenant = \"global-profile\"\n",
        );
        assert_eq!(
            resolve_tenant(Some("p"), Some(&project), Some(&global)).as_deref(),
            Some("proj-profile")
        );
    }

    #[test]
    fn global_profile_beats_project_default() {
        let project = settings_file("tenant = \"proj-default\"\n");
        let global = settings_file("[profile.p]\ntenant = \"global-profile\"\n");
        assert_eq!(
            resolve_tenant(Some("p"), Some(&project), Some(&global)).as_deref(),
            Some("global-profile")
        );
    }

    #[test]
    fn project_default_beats_global_default() {
        let project = settings_file("tenant = \"proj-default\"\n");
        let global = settings_file("tenant = \"global-default\"\n");
        assert_eq!(
            resolve_tenant(None, Some(&project), Some(&global)).as_deref(),
            Some("proj-default")
        );
    }

    #[test]
    fn global_default_is_the_last_resort() {
        let global = settings_file("tenant = \"global-default\"\n");
        assert_eq!(
            resolve_tenant(None, None, Some(&global)).as_deref(),
            Some("global-default")
        );
    }

    #[test]
    fn nothing_set_resolves_to_none() {
        assert!(resolve_tenant(None, None, None).is_none());
    }

    #[test]
    fn an_unselected_profile_falls_back_to_defaults() {
        // No `--profile`, so a `[profile.p]` value is ignored in favor of the
        // top-level default.
        let project =
            settings_file("tenant = \"proj-default\"\n[profile.p]\ntenant = \"proj-profile\"\n");
        assert_eq!(
            resolve_tenant(None, Some(&project), None).as_deref(),
            Some("proj-default")
        );
    }

    // ---- accessors -----------------------------------------------------

    #[test]
    fn tenant_accessor_errors_point_at_doctor() {
        let settings = Settings::for_test(None, None, None, None, None);
        let err = settings.tenant().unwrap_err();
        assert!(err.to_string().contains("snouty doctor"));
    }

    #[test]
    fn base_url_falls_back_to_tenant_host() {
        let settings = Settings::for_test(None, Some("acme"), None, None, None);
        assert_eq!(settings.base_url().unwrap(), "https://acme.antithesis.com");
    }

    #[test]
    fn explicit_base_url_overrides_tenant_host() {
        let settings =
            Settings::for_test(None, Some("acme"), None, Some("https://example.test"), None);
        assert_eq!(settings.base_url().unwrap(), "https://example.test");
    }

    #[test]
    fn base_url_without_tenant_is_an_error() {
        let settings = Settings::for_test(None, None, None, None, None);
        assert!(settings.base_url().is_err());
    }

    #[test]
    fn container_engine_absent_resolves_to_none() {
        let settings = Settings::for_test(None, None, None, None, None);
        assert_eq!(settings.container_engine(), None);
    }

    #[test]
    fn container_engine_resolves_when_set() {
        let settings = Settings::for_test(None, None, None, None, Some("podman"));
        assert_eq!(settings.container_engine(), Some("podman"));
    }
}
