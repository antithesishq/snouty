use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use color_eyre::eyre::{OptionExt, Result, eyre};
use tempfile::NamedTempFile;
use toml::{Table, Value, map::Entry};

use crate::env;
use crate::error::user_error;

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
///
/// Reads through [`env::var`], so an exported-but-empty `XDG_*`/`HOME` is treated
/// as unset (per the XDG spec) rather than yielding a bogus relative path; a
/// non-Unicode value is likewise treated as unset here rather than aborting the
/// command.
fn xdg_snouty_dir(xdg_var: &str, home_subdir: &str) -> Option<PathBuf> {
    xdg_base(
        env::var(xdg_var).ok().flatten(),
        env::var("HOME").ok().flatten(),
        home_subdir,
    )
}

/// The `$base/snouty` directory given already-resolved (empty-collapsed) env
/// values: the XDG dir if set, else `$HOME/<home_subdir>`; `None` when neither
/// is set. Pure, so the XDG-vs-`HOME` fallback is unit-testable without touching
/// the process environment.
fn xdg_base(xdg_dir: Option<String>, home: Option<String>, home_subdir: &str) -> Option<PathBuf> {
    let base = match xdg_dir {
        Some(dir) => PathBuf::from(dir),
        None => PathBuf::from(home?).join(home_subdir),
    };
    Some(base.join("snouty"))
}

/// Directory holding the global settings file: `$XDG_CONFIG_HOME/snouty`,
/// falling back to `$HOME/.config/snouty`. `None` only when neither
/// `XDG_CONFIG_HOME` nor `HOME` is set (e.g. Windows).
pub fn global_settings_dir() -> Option<PathBuf> {
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
        // `env::var` already collapses an empty env var to `None`; the trailing
        // filter also covers an explicitly-empty `--profile ""` flag.
        let profile = match profile {
            Some(flag) => Some(flag),
            None => env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME)?,
        }
        .filter(|profile| !profile.is_empty());

        // Likewise an empty `SNOUTY_SETTINGS_PATH` is "unset" (it would otherwise
        // become an explicitly-requested empty path that must — and can't — exist).
        let project_settings_path = match project_settings_path {
            Some(path) => Some(path),
            None => env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)?.map(PathBuf::from),
        };

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

        // A derived base URL interpolates the tenant into the request host
        // (`https://{tenant}.antithesis.com`) and we attach the API key to that
        // host, so a malformed tenant would silently send credentials to an
        // unintended endpoint. Validate it as a hostname before deriving. An
        // explicit base_url bypasses the tenant, so only check when we'd derive.
        if base_url.is_none()
            && let Some(tenant) = &tenant
        {
            validate_tenant_host(tenant)?;
        }

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

    /// The resolved tenant, or `None` if unset. Call sites that require it turn
    /// the `None` into an error (see [`require`]); doctor reports it as-is.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// The resolved container registry, or `None` if unset.
    pub fn repository(&self) -> Option<&str> {
        self.repository.as_deref()
    }

    /// The base URL to talk to: an explicit `base_url` if set, otherwise one
    /// derived from the tenant; `None` when neither exists (a derived `base_url`
    /// is present exactly when the tenant is).
    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    pub fn container_engine(&self) -> Option<&str> {
        self.container_engine.as_deref()
    }

    pub(crate) fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
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

pub(crate) fn update_settings_in_global_file(
    tenant: Option<String>,
    repository: Option<String>,
    base_url: Option<String>,
    container_engine: Option<String>,
    profile_to_update: Option<&str>,
) -> Result<()> {
    let settings_dir = global_settings_dir().ok_or_eyre("Could not determine global settings directory. Ensure either $XDG_CONFIG_DIR or $HOME is set.")?;
    let path = settings_dir.join(GLOBAL_SETTINGS_FILENAME);
    let mut contents = match read_to_string_if_file_exists(&path)? {
        Some(contents) => parse_settings(&contents, &path)?,
        None => Table::new(),
    };

    if let Some(profile) = profile_to_update {
        let mut target = Table::new();
        update_table(&mut target, tenant, repository, base_url, container_engine);
        let profile_value = Value::Table(target);

        match contents.entry(PROFILE_KEY) {
            Entry::Vacant(vacant) => {
                let mut outer = Table::new();
                outer.insert(profile.to_owned(), profile_value);
                vacant.insert(Value::Table(outer));
            }
            Entry::Occupied(mut occupied) => {
                let outer = occupied.get_mut().as_table_mut().ok_or_eyre(eyre!("The settings file at {:?} is malformed: `profile` should be a table of named profiles", &path))?;
                outer.insert(profile.to_owned(), profile_value);
            }
        }
    } else {
        update_table(
            &mut contents,
            tenant,
            repository,
            base_url,
            container_engine,
        );
    }

    fs::DirBuilder::new()
        .recursive(true)
        .create(&settings_dir)?;
    let mut temp = NamedTempFile::new_in(&settings_dir)?;
    temp.write_all(toml::to_string_pretty(&contents)?.as_bytes())?;

    temp.persist(&path)?;

    Ok(())
}

fn update_table(
    target: &mut Table,
    tenant: Option<String>,
    repository: Option<String>,
    base_url: Option<String>,
    container_engine: Option<String>,
) {
    if let Some(tenant) = tenant {
        target.insert("tenant".to_owned(), Value::String(tenant));
    }

    if let Some(repository) = repository {
        target.insert("repository".to_owned(), Value::String(repository));
    }

    if let Some(base_url) = base_url {
        target.insert("base_url".to_owned(), Value::String(base_url));
    }

    if let Some(container_engine) = container_engine {
        target.insert(
            "container_engine".to_owned(),
            Value::String(container_engine),
        );
    }
}

/// Validate that `tenant` is safe to interpolate into the derived base URL
/// `https://{tenant}.antithesis.com`. The tenant becomes the request host, so
/// it must be a valid DNS hostname — one or more labels of ASCII letters,
/// digits, and hyphens (hyphens not leading/trailing). This rejects values
/// carrying URL-significant characters (`/`, `#`, `?`, `@`, `:`, whitespace,
/// …) that would otherwise redirect requests — with the API key attached — to
/// an unintended host. Dots are allowed so a multi-label tenant still works;
/// set `ANTITHESIS_BASE_URL` directly for any URL this rejects.
pub(crate) fn validate_tenant_host(tenant: &str) -> Result<()> {
    fn is_valid_label(label: &str) -> bool {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    }

    let valid = !tenant.is_empty() && tenant.len() <= 253 && tenant.split('.').all(is_valid_label);
    if !valid {
        return Err(user_error(format!(
            "invalid tenant `{tenant}`: a tenant must be a valid hostname \
             (letters, digits, and hyphens) because it becomes the host in \
             `https://{tenant}.antithesis.com`. Set ANTITHESIS_BASE_URL directly \
             if you need a custom API URL."
        )));
    }
    Ok(())
}

/// Turn a missing required setting into snouty's standard "run doctor" error.
/// The `Option` accessors stay the single source of truth; the few call sites
/// that cannot proceed without a value funnel the `None` through here, so the
/// message lives in one place instead of being duplicated per accessor.
pub fn require<'a>(value: Option<&'a str>, setting: &str) -> Result<&'a str> {
    value
        .ok_or_else(|| eyre!("Could not resolve Antithesis {setting}. Run snouty doctor to debug."))
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
    let contents = match read_to_string_if_file_exists(path)? {
        Some(contents) => contents,
        None if !required => return Ok(None),
        None => return Err(eyre!("Settings file at {:?} was not found", path)),
    };
    parse_settings(&contents, path).map(Some)
}

/// Resolve a single setting with the precedence: environment variable, then the
/// active profile (project file before global file), then top-level defaults
/// (project file before global file). The first layer that has the key wins.
///
/// A layer that *has* the key but with a non-string value (or a malformed
/// `profile` section) is a hard error rather than a silent skip — a typo like
/// `tenant = 123` should be reported, not quietly ignored in favour of a
/// lower-precedence value.
fn resolve_value(
    key: &str,
    env_var: &str,
    profile: Option<&str>,
    project: Option<&Table>,
    global: Option<&Table>,
) -> Result<Option<String>> {
    // The environment variable always has highest precedence.
    if let Some(value) = env::var(env_var)? {
        return Ok(Some(value));
    }

    // A named profile is consulted before defaults, project before global.
    if let Some(profile) = profile {
        for table in [project, global].into_iter().flatten() {
            if let Some(value) = profile_value(table, profile, key)? {
                return Ok(Some(value));
            }
        }
    }

    // Finally fall back to top-level defaults, project before global.
    for table in [project, global].into_iter().flatten() {
        if let Some(value) = default_value(table, key)? {
            return Ok(Some(value));
        }
    }

    Ok(None)
}

/// A `[profile.<name>]` value: `profile.<profile>.<key>`. `Ok(None)` when the
/// `profile` section, the named profile, or the key is absent; an error when
/// `profile`/`profile.<name>` is present but not a table, or the value is
/// present but not a string.
fn profile_value(table: &Table, profile: &str, key: &str) -> Result<Option<String>> {
    let Some(profiles) = table.get(PROFILE_KEY) else {
        return Ok(None);
    };
    let profiles = profiles
        .as_table()
        .ok_or_else(|| eyre!("`{PROFILE_KEY}` must be a table of profiles"))?;
    let Some(selected) = profiles.get(profile) else {
        return Ok(None);
    };
    let selected = selected
        .as_table()
        .ok_or_else(|| eyre!("profile `{profile}` must be a table"))?;
    string_value(selected, key, &format!("{PROFILE_KEY}.{profile}.{key}"))
}

/// A top-level default value: `<key>`. `Ok(None)` when absent; an error when
/// present but not a string.
fn default_value(table: &Table, key: &str) -> Result<Option<String>> {
    string_value(table, key, key)
}

/// Read `key` from `table` as a string, naming the offending value `display` in
/// the error. `Ok(None)` when the key is absent; an error when it is present but
/// holds a non-string TOML value.
fn string_value(table: &Table, key: &str, display: &str) -> Result<Option<String>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => match value.as_str() {
            Some(value) => Ok(Some(value.to_string())),
            None => Err(eyre!(
                "setting `{display}` must be a string, but found {}",
                value.type_str()
            )),
        },
    }
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
        assert!(err.to_string().contains("was not found"));
    }

    // ---- profile_value / default_value ---------------------------------

    #[test]
    fn profile_value_reads_nested_table() {
        let table: Table = "[profile.staging]\ntenant = \"staging-tenant\"\n"
            .parse()
            .unwrap();
        assert_eq!(
            profile_value(&table, "staging", "tenant")
                .unwrap()
                .as_deref(),
            Some("staging-tenant")
        );
        // missing profile and missing key both resolve to None
        assert_eq!(profile_value(&table, "prod", "tenant").unwrap(), None);
        assert_eq!(
            profile_value(&table, "staging", "repository").unwrap(),
            None
        );
    }

    #[test]
    fn default_value_reads_top_level_key() {
        let table: Table = "tenant = \"acme\"\n".parse().unwrap();
        assert_eq!(
            default_value(&table, "tenant").unwrap().as_deref(),
            Some("acme")
        );
        assert_eq!(default_value(&table, "missing").unwrap(), None);
    }

    // ---- strict TOML typing --------------------------------------------

    #[test]
    fn non_string_default_value_is_an_error() {
        let table: Table = "tenant = 123\n".parse().unwrap();
        let err = default_value(&table, "tenant").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("tenant"), "unexpected error: {msg}");
        assert!(msg.contains("must be a string"), "unexpected error: {msg}");
    }

    #[test]
    fn non_string_profile_value_is_an_error() {
        let table: Table = "[profile.p]\ntenant = true\n".parse().unwrap();
        let err = profile_value(&table, "p", "tenant").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("profile.p.tenant"), "unexpected error: {msg}");
        assert!(msg.contains("must be a string"), "unexpected error: {msg}");
    }

    #[test]
    fn malformed_profile_section_is_an_error() {
        // `profile` present but not a table of profiles.
        let table: Table = "profile = \"oops\"\n".parse().unwrap();
        let err = profile_value(&table, "p", "tenant").unwrap_err();
        assert!(err.to_string().contains("table of profiles"));
    }

    #[test]
    fn non_table_profile_is_an_error() {
        // The named profile exists but isn't a table.
        let table: Table = "[profile]\np = \"oops\"\n".parse().unwrap();
        let err = profile_value(&table, "p", "tenant").unwrap_err();
        assert!(err.to_string().contains("profile `p` must be a table"));
    }

    // ---- xdg_base (path resolution) ------------------------------------

    #[test]
    fn xdg_base_prefers_the_xdg_dir() {
        let dir = xdg_base(
            Some("/xdg".to_string()),
            Some("/home/u".to_string()),
            ".config",
        );
        assert_eq!(dir, Some(PathBuf::from("/xdg/snouty")));
    }

    #[test]
    fn xdg_base_falls_back_to_home_subdir() {
        let dir = xdg_base(None, Some("/home/u".to_string()), ".config");
        assert_eq!(dir, Some(PathBuf::from("/home/u/.config/snouty")));
    }

    #[test]
    fn xdg_base_is_none_without_xdg_or_home() {
        assert_eq!(xdg_base(None, None, ".config"), None);
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
    fn require_missing_setting_points_at_doctor() {
        let err = require(None, "tenant").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("tenant"), "unexpected error: {msg}");
        assert!(msg.contains("snouty doctor"), "unexpected error: {msg}");
    }

    #[test]
    fn require_passes_a_present_setting_through() {
        assert_eq!(require(Some("acme"), "tenant").unwrap(), "acme");
    }

    #[test]
    fn base_url_falls_back_to_tenant_host() {
        let settings = Settings::for_test(None, Some("acme"), None, None, None);
        assert_eq!(settings.base_url(), Some("https://acme.antithesis.com"));
    }

    #[test]
    fn explicit_base_url_overrides_tenant_host() {
        let settings =
            Settings::for_test(None, Some("acme"), None, Some("https://example.test"), None);
        assert_eq!(settings.base_url(), Some("https://example.test"));
    }

    #[test]
    fn base_url_without_tenant_is_none() {
        let settings = Settings::for_test(None, None, None, None, None);
        assert_eq!(settings.base_url(), None);
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

    // ---- validate_tenant_host -----------------------------------------

    #[test]
    fn valid_tenants_pass_host_validation() {
        for tenant in ["orbitinghail", "acme", "my-tenant", "t123", "foo.bar"] {
            assert!(
                validate_tenant_host(tenant).is_ok(),
                "expected `{tenant}` to be a valid tenant host"
            );
        }
    }

    #[test]
    fn url_significant_tenants_are_rejected() {
        // Each of these would otherwise redirect requests (with the API key) to
        // an unintended host or mangle the URL.
        for tenant in [
            "evil.com#",
            "evil.com/x",
            "a b",
            "acme#",
            "foo/../bar",
            "host:8080",
            "tenant?x=1",
            "user@host",
            "",
            "-leadinghyphen",
            "trailinghyphen-",
        ] {
            assert!(
                validate_tenant_host(tenant).is_err(),
                "expected `{tenant}` to be rejected as a tenant host"
            );
        }
    }

    #[test]
    fn explicit_base_url_bypasses_tenant_host_validation() {
        // An explicit base_url bypasses tenant-host validation (the tenant isn't
        // interpolated into the host), so an otherwise-invalid tenant still
        // constructs. The derive-path validation itself is covered by
        // validate_tenant_host's unit tests and specs/settings_tenant.txt.
        let s = Settings::for_test(
            None,
            Some("evil.com#"),
            None,
            Some("https://ok.example"),
            None,
        );
        assert_eq!(s.base_url(), Some("https://ok.example"));
    }
}
