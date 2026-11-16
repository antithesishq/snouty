use std::{
    fmt, fs,
    io::Write,
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

use color_eyre::Section;
use color_eyre::eyre::{Context, OptionExt, Report, Result, eyre};
use log::debug;
use tempfile::NamedTempFile;
use toml::{Table, Value};
use url::Url;

use crate::env;
use crate::error::user_error;
use crate::known_path::{KnownPath, KnownPathKind, LoadedFile};
use crate::registry::Registry;
use crate::tag::{Tag, Tagged};

mod choice;
mod show;
mod trust;
pub use choice::{ContainerEngine, UpdateChannel};
pub use show::cmd_settings_show;
pub use trust::{SETTINGS_TRUST_VAR_NAME, UntrustedSettingsFile, deny, trust};

pub const ANTITHESIS_PROFILE_ENV_VAR_NAME: &str = "ANTITHESIS_PROFILE";
pub const SNOUTY_SETTINGS_PATH_VAR_NAME: &str = "SNOUTY_SETTINGS_PATH";
pub const ANTITHESIS_TENANT_VAR_NAME: &str = "ANTITHESIS_TENANT";
pub const ANTITHESIS_REGISTRY_VAR_NAME: &str = "ANTITHESIS_REGISTRY";
/// An alias for [`ANTITHESIS_REGISTRY_VAR_NAME`].
const ANTITHESIS_REPOSITORY_VAR_NAME: &str = "ANTITHESIS_REPOSITORY";
pub const ANTITHESIS_BASE_URL_VAR_NAME: &str = "ANTITHESIS_BASE_URL";
pub const ANTITHESIS_HTTPS_PROXY_VAR_NAME: &str = "ANTITHESIS_HTTPS_PROXY";
pub const CONTAINER_ENGINE_VAR_NAME: &str = "SNOUTY_CONTAINER_ENGINE";
pub const UPDATE_CHANNEL_VAR_NAME: &str = "SNOUTY_UPDATE_CHANNEL";
pub const API_CACHE_MAX_FILE_SIZE_VAR_NAME: &str = "SNOUTY_API_CACHE_MAX_FILE_SIZE";
pub const PROJECT_SETTINGS_FILENAME: &str = ".snouty.toml";
const GLOBAL_SETTINGS_FILENAME: &str = "settings.toml";
const PROFILE_KEY: &str = "profile";
const REGISTRY_KEY: &str = "registry";
/// An alias for [`REGISTRY_KEY`].
const REPOSITORY_KEY: &str = "repository";
const NO_GLOBAL_SETTINGS_DIR: &str = "Could not determine global settings directory. Ensure either $XDG_CONFIG_HOME or $HOME is set.";

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
/// Everything is resolved eagerly in [`Settings::resolve`]: it reads the
/// environment, the project settings file (`.snouty.toml`) and the global
/// `settings.toml` once, resolves each setting through the precedence chain,
/// and parses it into its type. A settings file that cannot be read or
/// parsed, or a value that does not parse, is an error there, so nothing
/// fails later.
///
/// Each value but the tenant keeps the [`SettingSource`] that it came from.
///
/// Every command shares the same resolved instance (threaded by reference), so a
/// value resolves identically no matter which code path reads it.
#[derive(Clone)]
#[cfg_attr(test, derive(Default))]
pub struct Settings {
    profile: Option<String>,
    /// Untagged: login replaces it with a tenant that no setting gives.
    tenant: Option<Tenant>,
    registry: Option<Tagged<Registry, SettingSource>>,
    /// The `base_url` setting. [`Settings::base_url`] falls back to the
    /// tenant's host.
    base_url: Option<Tagged<Url, SettingSource>>,
    https_proxy: Option<Tagged<Url, SettingSource>>,
    container_engine: Option<Tagged<ContainerEngine, SettingSource>>,
    update_channel: Option<Tagged<UpdateChannel, SettingSource>>,
    api_cache_max_file_size: Option<Tagged<u64, SettingSource>>,
    files: Vec<KnownPath>,
}

impl Settings {
    /// Resolve settings from the environment, the project settings file, and the
    /// global `settings.toml`.
    ///
    /// The project settings file is located by, in descending precedence:
    /// the `--settings` flag (`project_settings_path`), `SNOUTY_SETTINGS_PATH`,
    /// then `./.snouty.toml` in the current working directory. The active profile
    /// is the `--profile` flag if set, otherwise `ANTITHESIS_PROFILE`.
    ///
    /// A settings file that exists but cannot be read or parsed is a hard error,
    /// as is an explicitly-requested file (via flag or env var) that is missing:
    /// silently ignoring it would resurface later as a confusing "tenant not
    /// set". A missing *default* `./.snouty.toml` is fine. The default file is
    /// used only when the user trusts it (see [`trust::verdict`]).
    pub fn resolve(
        project_settings_path: Option<PathBuf>,
        profile: Option<String>,
    ) -> Result<Self> {
        SettingsRequest::from_flags(project_settings_path, profile)?.resolve()
    }
}

/// Where a setting's value comes from: the place to change it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettingSource {
    Env(&'static str),
    /// `key`, such as `profile.prod.registry`, in the settings file at
    /// `path`.
    File {
        path: KnownPath,
        key: String,
    },
}

/// An error for the value from `source`, which snouty cannot use because of
/// `err`.
fn invalid_setting(source: &SettingSource, err: impl fmt::Display) -> Report {
    match source {
        SettingSource::Env(var) => {
            user_error(format!("invalid {var}: {err}")).suggestion(format!("fix or unset {var}"))
        }
        SettingSource::File { path, key } => user_error(format!(
            "invalid setting `{key}` in {}: {err}",
            path.display()
        ))
        .suggestion(format!("fix or delete {}", path.display())),
    }
}

/// Parse the text of `setting` with `parse`, and keep its source tag. An
/// error names the source.
fn parse_setting<T, E: fmt::Display>(
    setting: Option<Tagged<String, SettingSource>>,
    parse: impl FnOnce(&str) -> Result<T, E>,
) -> Result<Option<Tagged<T, SettingSource>>> {
    let Some(setting) = setting else {
        return Ok(None);
    };
    let (text, source) = setting.into_parts();
    match parse(&text) {
        Ok(value) => Ok(Some(value.with_tag(source))),
        Err(err) => Err(invalid_setting(&source, err)),
    }
}

/// The settings file and the profile that the user names, by flag or by
/// environment variable.
pub struct SettingsRequest {
    project_settings_path: Option<PathBuf>,
    profile: Option<String>,
}

impl SettingsRequest {
    /// The `--settings` flag, else `SNOUTY_SETTINGS_PATH`; the `--profile`
    /// flag, else `ANTITHESIS_PROFILE`.
    pub fn from_flags(
        project_settings_path: Option<PathBuf>,
        profile: Option<String>,
    ) -> Result<Self> {
        let project_settings_path = explicit_settings_path(project_settings_path)?;
        // An empty value means "unset", not "a profile named the empty string".
        // `env::var` already collapses an empty env var to `None`; the trailing
        // filter also covers an explicitly-empty `--profile ""` flag.
        let profile = match profile {
            Some(flag) => Some(flag),
            None => env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME)?,
        }
        .filter(|profile| !profile.is_empty());
        Ok(Self {
            project_settings_path,
            profile,
        })
    }

    /// The profile that the user names.
    pub fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    /// See [`Settings::resolve`].
    pub fn resolve(self) -> Result<Settings> {
        SettingsSources::from_ambient(self.project_settings_path.as_deref())?.resolve(self.profile)
    }

    /// The settings of the top-level defaults, then of each named profile:
    /// first the profiles of the project settings file, then the other
    /// profiles of the global settings file, each file in name order. This
    /// reads the same files as [`Self::resolve`], in the same order: a value
    /// in the project file wins over the same value in the global file. An
    /// environment variable still overrides every profile. The profile that
    /// the user names is not used.
    ///
    /// Only the profiles whose tenant and registry `wanted` accepts are
    /// resolved. A profile whose tenant or registry fails to resolve is left
    /// out: a caller that searches the profiles must not fail on a profile it
    /// does not use. A wanted profile that fails to resolve gives its error.
    pub fn resolve_every_profile(
        &self,
        wanted: impl Fn(Option<&Tenant>, Option<&Registry>) -> bool,
    ) -> Result<Vec<Result<Settings, UnresolvedProfile>>> {
        SettingsSources::from_ambient(self.project_settings_path.as_deref())?
            .resolve_every_profile(wanted)
    }
}

/// A profile that failed to resolve: its name (`None` for the top-level
/// defaults) and its error.
pub struct UnresolvedProfile {
    pub profile: Option<String>,
    pub error: Report,
}

/// The settings that name the place that a profile pushes to.
struct Destination {
    tenant: Option<Tenant>,
    registry: Option<Tagged<Registry, SettingSource>>,
}

/// One source of settings.
enum Layer {
    Env,
    File(LoadedFile),
}

/// The sources of settings, most specific first. The first source that has a
/// setting wins. Only the first layer can be [`Layer::Env`]:
/// [`resolve_value_with`] looks for it there.
struct SettingsSources(Vec<Layer>);

impl SettingsSources {
    /// The environment variables, then the project file at
    /// `project_settings_path`, else the optional `./.snouty.toml`, then the
    /// global settings file.
    fn from_ambient(project_settings_path: Option<&Path>) -> Result<Self> {
        // Parse this even without a project file, so a bad value fails in
        // every directory.
        let trust_override = trust::TrustOverride::from_env()?;
        // A named project file must exist and needs no trust; the default
        // `./.snouty.toml` is optional and needs trust. (Climbing the directory
        // tree, if ever wanted, would go here.)
        let project = match project_settings_path {
            Some(path) => {
                KnownPath::new(KnownPathKind::Project { explicit: true }, path).load(true)?
            }
            None => load_default_project_file(trust_override)?,
        };
        let global = load_global_file()?;
        // `--settings` can name the global settings file. Then it is the
        // global file only.
        let project = project.filter(|project| {
            global
                .as_ref()
                .is_none_or(|global| *global.path != *project.path)
        });
        Ok(Self(
            std::iter::once(Layer::Env)
                .chain(project.into_iter().chain(global).map(Layer::File))
                .collect(),
        ))
    }

    /// The global settings file alone, without the environment variables.
    fn global_file() -> Result<Self> {
        Ok(Self(
            load_global_file()?.map(Layer::File).into_iter().collect(),
        ))
    }

    /// The settings files, most specific first.
    fn files(&self) -> impl Iterator<Item = &LoadedFile> {
        self.0.iter().filter_map(|layer| match layer {
            Layer::Env => None,
            Layer::File(file) => Some(file),
        })
    }

    /// See [`SettingsRequest::resolve_every_profile`].
    fn resolve_every_profile(
        &self,
        wanted: impl Fn(Option<&Tenant>, Option<&Registry>) -> bool,
    ) -> Result<Vec<Result<Settings, UnresolvedProfile>>> {
        let mut names: Vec<&String> = Vec::new();
        for file in self.files() {
            let profiles = profiles(file)?;
            for name in profiles.into_iter().flat_map(Table::keys) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        Ok(std::iter::once(None)
            .chain(names.into_iter().map(|name| Some(name.clone())))
            .filter(|profile| {
                self.resolve_destination(profile.as_deref())
                    .is_ok_and(|to| {
                        wanted(to.tenant.as_ref(), to.registry.as_ref().map(Tagged::value))
                    })
            })
            .map(|profile| {
                self.resolve(profile.clone())
                    .map_err(|error| UnresolvedProfile { profile, error })
            })
            .collect())
    }

    /// The tenant and the registry of `profile`.
    fn resolve_destination(&self, profile: Option<&str>) -> Result<Destination> {
        // The tenant is a host name even when an explicit base_url means
        // that no request goes to the tenant's host.
        let tenant = parse_setting(
            resolve_value_with(
                string_value,
                &["tenant"],
                &[ANTITHESIS_TENANT_VAR_NAME],
                profile,
                self,
            )?,
            str::parse,
        )?;
        let registry = parse_setting(
            resolve_value_with(
                string_value,
                &[REGISTRY_KEY, REPOSITORY_KEY],
                &[ANTITHESIS_REGISTRY_VAR_NAME, ANTITHESIS_REPOSITORY_VAR_NAME],
                profile,
                self,
            )?,
            str::parse,
        )?;
        Ok(Destination {
            tenant: tenant.map(Tagged::untag),
            registry,
        })
    }

    fn resolve(&self, profile: Option<String>) -> Result<Settings> {
        let name = profile.as_deref();
        let text = |key: &str, env_var: &'static str| {
            resolve_value_with(string_value, &[key], &[env_var], name, self)
        };
        let Destination { tenant, registry } = self.resolve_destination(name)?;
        Ok(Settings {
            tenant,
            registry,
            base_url: parse_setting(
                text("base_url", ANTITHESIS_BASE_URL_VAR_NAME)?,
                parse_base_url,
            )?,
            https_proxy: parse_setting(
                text("https_proxy", ANTITHESIS_HTTPS_PROXY_VAR_NAME)?,
                parse_proxy_url,
            )?,
            container_engine: parse_setting(
                text("container_engine", CONTAINER_ENGINE_VAR_NAME)?,
                str::parse,
            )?,
            update_channel: parse_setting(
                text("update_channel", UPDATE_CHANNEL_VAR_NAME)?,
                str::parse,
            )?,
            api_cache_max_file_size: parse_setting(
                resolve_value_with(
                    integer_value,
                    &["api_cache_max_file_size"],
                    &[API_CACHE_MAX_FILE_SIZE_VAR_NAME],
                    name,
                    self,
                )?,
                parse_byte_size,
            )?,
            files: self.files().map(|file| file.path.clone()).collect(),
            profile,
        })
    }
}

impl Settings {
    /// The resolved tenant, or `None` if unset.
    pub fn tenant(&self) -> Option<&Tenant> {
        self.tenant.as_ref()
    }

    /// The resolved container registry, tagged with where it comes from, or
    /// `None` if unset.
    pub fn registry(&self) -> Option<&Tagged<Registry, SettingSource>> {
        self.registry.as_ref()
    }

    /// The container registry, for a command that pushes to it: an error
    /// when the setting is unset.
    pub fn require_registry(&self) -> Result<&Registry> {
        self.registry()
            .map(Tagged::value)
            .ok_or_else(|| missing_setting("registry"))
    }

    /// The base URL to talk to: an explicit `base_url` if set, otherwise one
    /// derived from the tenant; `None` when neither exists (a derived `base_url`
    /// is present exactly when the tenant is).
    pub fn base_url(&self) -> Option<Url> {
        self.explicit_base_url()
            .cloned()
            .or_else(|| self.tenant().map(tenant_base_url))
    }

    /// The base URL, or an error that names the tenant when there is none. A
    /// user normally sets the tenant, not the base URL.
    pub fn require_base_url(&self) -> Result<Url> {
        self.base_url().ok_or_else(|| missing_setting("tenant"))
    }

    /// The `base_url` setting, without the fallback to the tenant's host.
    pub(crate) fn explicit_base_url(&self) -> Option<&Url> {
        self.base_url.as_ref().map(Tagged::value)
    }

    pub fn https_proxy(&self) -> Option<&Url> {
        self.https_proxy.as_ref().map(Tagged::value)
    }

    /// The container engine that the user chose, or `None` for
    /// auto-detection.
    pub fn container_engine(&self) -> Option<ContainerEngine> {
        self.container_engine.as_ref().map(|engine| *engine.value())
    }

    /// The resolved update channel; `stable` when unset.
    pub fn update_channel(&self) -> UpdateChannel {
        self.update_channel
            .as_ref()
            .map_or_default(|channel| *channel.value())
    }

    /// The largest API response body, in bytes, the response cache stores;
    /// [`crate::api_cache::DEFAULT_MAX_FILE_SIZE`] when unset.
    pub fn api_cache_max_file_size(&self) -> u64 {
        self.api_cache_max_file_size
            .as_ref()
            .map_or(crate::api_cache::DEFAULT_MAX_FILE_SIZE, |size| {
                *size.value()
            })
    }

    /// The settings files that snouty read, most specific first.
    pub fn files(&self) -> &[KnownPath] {
        &self.files
    }

    /// The features that these settings use and that the next release
    /// removes: `./.snouty.toml` and a named profile.
    pub fn removed_features(&self) -> Vec<RemovedFeature> {
        let project = self
            .files
            .iter()
            .any(|file| file.kind == KnownPathKind::Project { explicit: false })
            .then_some(RemovedFeature::DefaultProjectFile);
        let profile = self.profile.is_some().then_some(RemovedFeature::Profile);
        project.into_iter().chain(profile).collect()
    }

    pub(crate) fn profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    /// The settings that the global settings file alone gives for `profile`,
    /// without the environment variables and the project file: the values
    /// that `snouty login` writes.
    pub(crate) fn resolve_global_file(profile: Option<String>) -> Result<Self> {
        SettingsSources::global_file()?.resolve(profile)
    }

    /// These settings with `tenant` in place of the tenant setting. `tenant`
    /// comes from `snouty login --tenant` or its prompt.
    pub(crate) fn with_tenant(&self, tenant: Tenant) -> Self {
        Settings {
            tenant: Some(tenant),
            ..self.clone()
        }
    }

    /// Start building a `Settings` from explicit values, with no environment
    /// or filesystem IO. Set only what's needed and finish with
    /// [`SettingsBuilder::build`].
    #[cfg(test)]
    pub fn builder() -> SettingsBuilder {
        SettingsBuilder(Settings::default())
    }

    /// A `Settings` with an explicit base URL and everything else unset, for
    /// driving [`crate::api::AntithesisApi`] against a mock server without
    /// touching the environment.
    #[cfg(test)]
    pub(crate) fn for_test_base_url(base_url: &str) -> Self {
        Self::builder().base_url(base_url).build()
    }
}

/// Builds a test `Settings` without resolution. Each tagged value except
/// `registry` is tagged as if its environment variable set it.
#[cfg(test)]
pub struct SettingsBuilder(Settings);

#[cfg(test)]
impl SettingsBuilder {
    #[must_use]
    pub fn profile(mut self, value: &str) -> Self {
        self.0.profile = Some(value.to_string());
        self
    }

    #[must_use]
    pub fn tenant(mut self, value: Tenant) -> Self {
        self.0.tenant = Some(value);
        self
    }

    #[must_use]
    pub fn registry(mut self, value: Tagged<Registry, SettingSource>) -> Self {
        self.0.registry = Some(value);
        self
    }

    #[must_use]
    pub fn base_url(mut self, value: &str) -> Self {
        self.0.base_url = Some(
            Url::parse(value)
                .expect("a test base URL parses")
                .with_tag(SettingSource::Env(ANTITHESIS_BASE_URL_VAR_NAME)),
        );
        self
    }

    #[must_use]
    pub fn api_cache_max_file_size(mut self, value: u64) -> Self {
        self.0.api_cache_max_file_size =
            Some(value.with_tag(SettingSource::Env(API_CACHE_MAX_FILE_SIZE_VAR_NAME)));
        self
    }

    pub fn build(self) -> Settings {
        self.0
    }
}

/// A feature that the next release removes.
#[derive(Debug, PartialEq, Eq)]
pub enum RemovedFeature {
    DefaultProjectFile,
    Profile,
    Trust,
    Deny,
}

impl fmt::Display for RemovedFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DefaultProjectFile => write!(f, "./{PROJECT_SETTINGS_FILENAME}"),
            Self::Profile => f.write_str("settings profiles"),
            Self::Trust => f.write_str("`snouty settings trust`"),
            Self::Deny => f.write_str("`snouty settings deny`"),
        }
    }
}

/// A warning that the next release removes `features`; `None` when there
/// are none.
pub fn removal_warning(features: &[RemovedFeature]) -> Option<String> {
    let (last, rest) = features.split_last()?;
    let features = if rest.is_empty() {
        last.to_string()
    } else {
        let rest: Vec<String> = rest.iter().map(ToString::to_string).collect();
        format!("{} and {last}", rest.join(", "))
    };
    Some(format!(
        "warning: the next snouty release removes support for {features}. If you \
         rely on this, file an issue at https://github.com/antithesishq/snouty/issues \
         or contact Antithesis support."
    ))
}

/// The project settings file that the user named: the `--settings` flag
/// (`flag`), otherwise `SNOUTY_SETTINGS_PATH`. An empty `SNOUTY_SETTINGS_PATH`
/// is unset; it would otherwise name an empty path that must, and cannot, exist.
fn explicit_settings_path(flag: Option<PathBuf>) -> Result<Option<PathBuf>> {
    match flag {
        Some(path) => Ok(Some(path)),
        None => Ok(env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)?.map(PathBuf::from)),
    }
}

/// Writes `tenant` and `registry` into the global `settings.toml`, in the
/// table of `profile_to_update` or else the top-level table. Returns what
/// puts the file back as it was.
pub(crate) fn update_settings_in_global_file(
    tenant: &str,
    registry: &str,
    profile_to_update: Option<&str>,
) -> Result<PreviousSettingsFile> {
    let path = global_settings_dir()
        .ok_or_eyre(NO_GLOBAL_SETTINGS_DIR)?
        .join(GLOBAL_SETTINGS_FILENAME);
    let previous = KnownPath::new(KnownPathKind::Global, &path).read(false)?;
    let mut contents = match &previous {
        Some(file) => file.clone().load()?.table,
        None => Table::new(),
    };

    if let Some(profile) = profile_to_update {
        let profiles = contents
            .entry(PROFILE_KEY)
            .or_insert_with(|| Value::Table(Table::new()))
            .as_table_mut()
            .ok_or_else(|| eyre!(
                "The settings file at {:?} is malformed: `profile` should be a table of named profiles",
                &path
            ))?;
        update_table(entry_as_table_mut(profiles, profile), tenant, registry);
    } else {
        update_table(&mut contents, tenant, registry);
    }

    write_global_toml(GLOBAL_SETTINGS_FILENAME, &contents)?;
    Ok(PreviousSettingsFile {
        path,
        contents: previous.map(|file| file.text),
    })
}

/// The global settings file before [`update_settings_in_global_file`]
/// changed it: its text, or `None` when there was no file.
#[must_use]
pub(crate) struct PreviousSettingsFile {
    path: PathBuf,
    contents: Option<String>,
}

impl PreviousSettingsFile {
    /// Put the file back as it was.
    pub(crate) fn restore(self) -> Result<()> {
        match self.contents {
            Some(contents) => write_atomically(&self.path, contents.as_bytes()),
            None => fs::remove_file(&self.path).map_err(Into::into),
        }
        .wrap_err_with(|| format!("could not restore {}", self.path.display()))
    }
}

/// Atomically write `value` as TOML to `filename` in the global settings dir,
/// and return the path it wrote.
fn write_global_toml(filename: &str, value: &impl serde::Serialize) -> Result<PathBuf> {
    let settings_dir = global_settings_dir().ok_or_eyre(NO_GLOBAL_SETTINGS_DIR)?;
    let path = settings_dir.join(filename);
    mkdir(&settings_dir, true, 0o700)?;
    write_atomically(&path, toml::to_string_pretty(value)?.as_bytes())?;
    Ok(path)
}

/// Replace the file at `path` with `contents` through a temporary file in
/// the same directory, so that a reader or a crash never leaves a partial
/// settings or credentials file. An error means that the file at `path` was
/// not replaced.
pub(crate) fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| eyre!("{} names no directory", path.display()))?;
    let mut temp = NamedTempFile::new_in(dir)?;
    temp.write_all(contents)?;
    // The contents reach the disk before the rename, so that a power loss
    // leaves the old file or the new one.
    temp.as_file().sync_all()?;
    temp.persist(path)?;
    // The file is replaced, so a failure to sync the rename is not an error:
    // some file systems refuse to sync a directory.
    #[cfg(unix)]
    if let Err(err) = fs::File::open(dir).and_then(|dir| dir.sync_all()) {
        debug!("could not sync {}: {err}", dir.display());
    }
    Ok(())
}

fn entry_as_table_mut<'a>(table: &'a mut Table, key: &str) -> &'a mut Table {
    let slot = table
        .entry(key.to_owned())
        .or_insert_with(|| Value::Table(Table::new()));
    if slot.as_table_mut().is_none() {
        *slot = Value::Table(Table::new());
    }
    slot.as_table_mut()
        .expect("slot was just ensured to hold a table")
}

pub(crate) fn mkdir(path: &Path, recursive: bool, permissions: u32) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(permissions);
    }
    builder.recursive(recursive).create(path)?;
    Ok(())
}

fn update_table(target: &mut Table, tenant: &str, registry: &str) {
    target.insert("tenant".to_owned(), Value::from(tenant));
    target.insert(REGISTRY_KEY.to_owned(), Value::from(registry));
    // The table never holds the alias beside `registry`.
    target.remove(REPOSITORY_KEY);
}

/// The base URL of `tenant` when no `base_url` is set.
pub(crate) fn tenant_base_url(tenant: &Tenant) -> Url {
    tenant_url(tenant).expect("Tenant::from_str checks that the tenant's URL parses")
}

fn tenant_url(tenant: &str) -> Result<Url, url::ParseError> {
    Url::parse(&format!("https://{tenant}.antithesis.com"))
}

/// An Antithesis tenant. The tenant becomes the host of the derived base URL
/// `https://{tenant}.antithesis.com`, with the API key attached, so it must be
/// a valid DNS host name: one or more labels of ASCII letters, digits, and
/// hyphens, with no hyphen at the start or end of a label. This refuses the
/// URL-significant characters (`/`, `#`, `?`, `@`, `:`, whitespace, …) that
/// would send requests to another host. The derived base URL must also
/// parse. This refuses a label that is not valid punycode, such as `xn--a`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tenant(String);

impl FromStr for Tenant {
    type Err = Report;

    fn from_str(tenant: &str) -> Result<Self> {
        let is_valid_label = |label: &str| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        };
        if tenant.is_empty()
            || tenant.len() > 253
            || !tenant.split('.').all(is_valid_label)
            || tenant_url(tenant).is_err()
        {
            return Err(user_error(format!(
                "`{tenant}` is not a valid tenant: a tenant is a host name of \
                 letters, digits, hyphens and dots"
            )));
        }
        Ok(Self(tenant.to_owned()))
    }
}

impl Deref for Tenant {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Tenant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// snouty's standard "run doctor" error for a missing required setting.
fn missing_setting(setting: &str) -> color_eyre::Report {
    eyre!("Could not resolve Antithesis {setting}. Run snouty doctor to debug.")
}

/// Load the global settings file, when it exists.
fn load_global_file() -> Result<Option<LoadedFile>> {
    match global_settings_dir() {
        Some(dir) => {
            KnownPath::new(KnownPathKind::Global, &dir.join(GLOBAL_SETTINGS_FILENAME)).load(false)
        }
        None => Ok(None),
    }
}

/// Load `./.snouty.toml` when it exists and [`trust::verdict`] allows it.
fn load_default_project_file(
    trust_override: Option<trust::TrustOverride>,
) -> Result<Option<LoadedFile>> {
    let Some(file) = KnownPath::new(
        KnownPathKind::Project { explicit: false },
        Path::new(PROJECT_SETTINGS_FILENAME),
    )
    .read(false)?
    else {
        return Ok(None);
    };
    match trust::verdict(&file.path, file.text.as_bytes(), trust_override)? {
        trust::Verdict::Use => file.load().map(Some),
        trust::Verdict::Ignore => {
            debug!("ignoring the denied settings file {}", file.path.display());
            Ok(None)
        }
    }
}

/// Reads a setting's value out of a settings file: [`string_value`] for
/// text settings, [`integer_value`] for numeric ones.
type ValueReader = fn(&Value) -> Result<String, String>;

/// Resolve the text of a setting from `sources` with the precedence:
/// environment variable, then the active profile in each file, then the
/// top-level defaults in each file, most specific file first. The first layer
/// that has the key wins.
///
/// A setting can have more than one name, such as a name and its alias. Each
/// layer reads the names in order, so the first name wins over a later one
/// in the same layer, and a higher layer wins over both.
///
/// A layer that has the key with a value of the wrong type, or a malformed
/// `profile` section, is an error. Resolution does not skip to a lower layer,
/// so a typo such as `tenant = 123` is reported.
fn resolve_value_with(
    read: ValueReader,
    keys: &[&str],
    env_vars: &[&'static str],
    profile: Option<&str>,
    sources: &SettingsSources,
) -> Result<Option<Tagged<String, SettingSource>>> {
    if matches!(sources.0.first(), Some(Layer::Env)) {
        for &env_var in env_vars {
            let value = env::var(env_var)
                .map_err(|err| err.suggestion(format!("fix or unset {env_var}")))?;
            if let Some(value) = value {
                return Ok(Some(value.with_tag(SettingSource::Env(env_var))));
            }
        }
    }

    // `key` in `table`: the table of `profile`, or the top-level table when
    // `profile` is `None`, of the file at `path`.
    let found = |path: &KnownPath, table: &Table, key: &str, profile: Option<&str>| {
        let Some(value) = table.get(key) else {
            return Ok(None);
        };
        let source = SettingSource::File {
            path: path.clone(),
            key: match profile {
                Some(profile) => format!("{PROFILE_KEY}.{profile}.{key}"),
                None => key.to_owned(),
            },
        };
        match read(value) {
            Ok(value) => Ok(Some(value.with_tag(source))),
            Err(err) => Err(invalid_setting(&source, err)),
        }
    };

    // A named profile is consulted before defaults, project before global.
    if let Some(profile) = profile {
        for file in sources.files() {
            let Some(table) = profile_table(file, profile)? else {
                continue;
            };
            for key in keys {
                if let Some(found) = found(&file.path, table, key, Some(profile))? {
                    return Ok(Some(found));
                }
            }
        }
    }

    // Finally fall back to top-level defaults, project before global.
    for file in sources.files() {
        for key in keys {
            if let Some(found) = found(&file.path, &file.table, key, None)? {
                return Ok(Some(found));
            }
        }
    }

    Ok(None)
}

/// The `profile` table of a settings file. `Ok(None)` when it is absent; an
/// error when it is present but not a table.
fn profiles(file: &LoadedFile) -> Result<Option<&Table>> {
    file.table
        .get(PROFILE_KEY)
        .map(|profiles| {
            profiles.as_table().ok_or_else(|| {
                user_error(format!(
                    "{}: `{PROFILE_KEY}` must be a table of profiles",
                    file.path.display()
                ))
                .suggestion(format!("fix or delete {}", file.path.display()))
            })
        })
        .transpose()
}

/// The `[profile.<profile>]` table of a settings file. `Ok(None)` when the
/// `profile` section or the named profile is absent; an error when either is
/// present but not a table.
fn profile_table<'a>(file: &'a LoadedFile, profile: &str) -> Result<Option<&'a Table>> {
    let Some(selected) = profiles(file)?.and_then(|profiles| profiles.get(profile)) else {
        return Ok(None);
    };
    selected.as_table().map(Some).ok_or_else(|| {
        user_error(format!(
            "{}: profile `{profile}` must be a table",
            file.path.display()
        ))
        .suggestion(format!("fix or delete {}", file.path.display()))
    })
}

/// A text setting's value; an error when it is not a TOML string.
fn string_value(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("must be a string, but found {}", value.type_str()))
}

/// A number-like setting's value, a bare TOML integer or a quoted string, as
/// text; the caller parses it.
fn integer_value(value: &Value) -> Result<String, String> {
    match value {
        Value::Integer(number) => Ok(number.to_string()),
        Value::String(text) => Ok(text.clone()),
        value => Err(format!(
            "must be an integer or a string, but found {}",
            value.type_str()
        )),
    }
}

/// Parse a `base_url` setting: a URL with a host.
fn parse_base_url(text: &str) -> Result<Url, String> {
    match Url::parse(text) {
        Ok(url) if url.has_host() => Ok(url),
        Ok(_) => Err("expected a URL such as https://acme.antithesis.com".to_owned()),
        Err(err) => Err(err.to_string()),
    }
}

/// Parse an `https_proxy` setting as reqwest reads a proxy: a value without
/// a scheme, such as `proxy.corp:3128`, is an `http://` URL. reqwest, built
/// without its `socks` feature, accepts only an http or https proxy.
fn parse_proxy_url(text: &str) -> Result<Url, String> {
    let url = match Url::parse(text) {
        Ok(url) if url.has_host() => Ok(url),
        Ok(_) | Err(url::ParseError::RelativeUrlWithoutBase) => {
            Url::parse(&format!("http://{text}"))
        }
        Err(err) => Err(err),
    }
    .map_err(|err| err.to_string())?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        scheme => Err(format!(
            "expected an http or https proxy URL, got the scheme `{scheme}`"
        )),
    }
}

/// Parse a byte-size setting — a size such as "10 MB" or "1.5GiB", or a bare
/// byte count — into bytes. SI units are decimal (MB = 10^6 bytes); IEC
/// units are binary (MiB = 2^20 bytes).
fn parse_byte_size(value: &str) -> Result<u64, String> {
    value
        .parse::<bytesize::ByteSize>()
        .map(|size| size.as_u64())
        .map_err(|err| format!("expected a size such as \"10 MB\": {err}"))
}

pub(crate) fn back_up_unparsable_file(path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| eyre!("Cannot back up file with no name: {:?}", path))?;
    let backup = path.with_file_name(format!("{file_name}.bak"));
    fs::rename(path, &backup).wrap_err_with(|| {
        format!(
            "Failed to back up \"{}\" to \"{}\"",
            path.display(),
            backup.display()
        )
    })?;
    Ok(backup)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A restore replaces the settings file with a new one, so that a write
    /// that stops partway leaves the file whole.
    #[test]
    fn restore_replaces_the_settings_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.toml");
        fs::write(&path, "tenant = \"new\"\n").unwrap();
        // A hard link sees a write into the file, but not a new file.
        let link = dir.path().join("link.toml");
        fs::hard_link(&path, &link).unwrap();

        PreviousSettingsFile {
            path: path.clone(),
            contents: Some("tenant = \"old\"\n".to_owned()),
        }
        .restore()
        .unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "tenant = \"old\"\n");
        assert_eq!(fs::read_to_string(&link).unwrap(), "tenant = \"new\"\n");
    }

    fn loaded_file(kind: KnownPathKind, path: &str, contents: &str) -> LoadedFile {
        LoadedFile {
            path: KnownPath {
                kind,
                path: PathBuf::from(path),
            },
            table: contents.parse().expect("test TOML should parse"),
        }
    }

    fn settings_file(contents: &str) -> LoadedFile {
        loaded_file(
            KnownPathKind::Project { explicit: true },
            "/test/settings.toml",
            contents,
        )
    }

    /// Leaves out the environment, so the files alone decide the result.
    fn sources(project: Option<LoadedFile>, global: Option<LoadedFile>) -> SettingsSources {
        SettingsSources(project.into_iter().chain(global).map(Layer::File).collect())
    }

    /// The text of the tenant setting.
    fn resolve_text(
        profile: Option<&str>,
        sources: &SettingsSources,
    ) -> Result<Option<Tagged<String, SettingSource>>> {
        resolve_value_with(
            string_value,
            &["tenant"],
            &[ANTITHESIS_TENANT_VAR_NAME],
            profile,
            sources,
        )
    }

    /// Each layer in the precedence tests uses a distinct value, so the
    /// resolved value proves which layer won.
    fn resolve_tenant(
        profile: Option<&str>,
        project: Option<LoadedFile>,
        global: Option<LoadedFile>,
    ) -> Option<String> {
        resolve_text(profile, &sources(project, global))
            .unwrap()
            .map(Tagged::untag)
    }

    // ---- strict TOML typing --------------------------------------------

    /// The error of resolving the tenant from a project file with
    /// `contents`.
    fn resolve_error(profile: Option<&str>, contents: &str) -> String {
        resolve_text(profile, &sources(Some(settings_file(contents)), None))
            .expect_err("the value should not resolve")
            .to_string()
    }

    #[test]
    fn a_profile_value_is_read_from_its_table() {
        let file = "[profile.staging]\ntenant = \"staging-tenant\"\n";
        assert_eq!(
            resolve_tenant(Some("staging"), Some(settings_file(file)), None).as_deref(),
            Some("staging-tenant")
        );
        assert_eq!(
            resolve_tenant(Some("prod"), Some(settings_file(file)), None),
            None
        );
    }

    #[test]
    fn a_non_string_value_is_an_invalid_setting() {
        assert_eq!(
            resolve_error(None, "tenant = 123\n"),
            "invalid setting `tenant` in /test/settings.toml: must be a string, but found integer"
        );
        assert_eq!(
            resolve_error(Some("p"), "[profile.p]\ntenant = true\n"),
            "invalid setting `profile.p.tenant` in /test/settings.toml: must be a string, but found boolean"
        );
    }

    #[test]
    fn integer_value_accepts_a_bare_toml_integer_or_a_quoted_string() {
        assert_eq!(integer_value(&Value::Integer(1234)).unwrap(), "1234");
        assert_eq!(integer_value(&Value::from("1234")).unwrap(), "1234");
        assert_eq!(
            integer_value(&Value::Boolean(true)).unwrap_err(),
            "must be an integer or a string, but found boolean"
        );
    }

    #[test]
    fn a_profile_section_that_is_not_a_table_is_a_fault_in_the_file() {
        assert_eq!(
            resolve_error(Some("p"), "profile = \"oops\"\n"),
            "/test/settings.toml: `profile` must be a table of profiles"
        );
        assert_eq!(
            resolve_error(Some("p"), "[profile]\np = \"oops\"\n"),
            "/test/settings.toml: profile `p` must be a table"
        );
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

    // ---- precedence ----------------------------------------------------

    #[test]
    fn project_profile_beats_global_profile_and_all_defaults() {
        let project =
            settings_file("tenant = \"proj-default\"\n[profile.p]\ntenant = \"proj-profile\"\n");
        let global = settings_file(
            "tenant = \"global-default\"\n[profile.p]\ntenant = \"global-profile\"\n",
        );
        assert_eq!(
            resolve_tenant(Some("p"), Some(project), Some(global)).as_deref(),
            Some("proj-profile")
        );
    }

    #[test]
    fn global_profile_beats_project_default() {
        let project = settings_file("tenant = \"proj-default\"\n");
        let global = settings_file("[profile.p]\ntenant = \"global-profile\"\n");
        assert_eq!(
            resolve_tenant(Some("p"), Some(project), Some(global)).as_deref(),
            Some("global-profile")
        );
    }

    #[test]
    fn project_default_beats_global_default() {
        let project = settings_file("tenant = \"proj-default\"\n");
        let global = settings_file("tenant = \"global-default\"\n");
        assert_eq!(
            resolve_tenant(None, Some(project), Some(global)).as_deref(),
            Some("proj-default")
        );
    }

    #[test]
    fn global_default_is_the_last_resort() {
        let global = settings_file("tenant = \"global-default\"\n");
        assert_eq!(
            resolve_tenant(None, None, Some(global)).as_deref(),
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
            resolve_tenant(None, Some(project), None).as_deref(),
            Some("proj-default")
        );
    }

    // ---- removal_warning -----------------------------------------------

    #[test]
    fn removal_warning_lists_each_feature() {
        let features = |features: &[RemovedFeature]| {
            removal_warning(features).map(|warning| {
                warning
                    .strip_prefix("warning: the next snouty release removes support for ")
                    .and_then(|rest| rest.split_once(". ").map(|(list, _)| list.to_owned()))
                    .unwrap()
            })
        };
        assert_eq!(features(&[]), None);
        assert_eq!(
            features(&[RemovedFeature::Trust]).as_deref(),
            Some("`snouty settings trust`")
        );
        assert_eq!(
            features(&[RemovedFeature::DefaultProjectFile, RemovedFeature::Profile]).as_deref(),
            Some("./.snouty.toml and settings profiles")
        );
        assert_eq!(
            features(&[
                RemovedFeature::DefaultProjectFile,
                RemovedFeature::Profile,
                RemovedFeature::Deny
            ])
            .as_deref(),
            Some("./.snouty.toml, settings profiles and `snouty settings deny`")
        );
    }

    // ---- resolve_every_profile -----------------------------------------

    /// The profiles that `resolve_every_profile` gives, in order, with
    /// `defaults` for the top-level defaults and `error` for a profile that
    /// fails to resolve. `wanted` accepts the profiles of the tenant `acme`,
    /// or every profile when `acme_only` is false.
    fn every_profile(project: &str, global: &str, acme_only: bool) -> Vec<String> {
        sources(Some(settings_file(project)), Some(settings_file(global)))
            .resolve_every_profile(|tenant, _| !acme_only || tenant.is_some_and(|t| **t == *"acme"))
            .unwrap()
            .iter()
            .map(|settings| match settings {
                Ok(settings) => settings.profile().unwrap_or("defaults").to_owned(),
                Err(_) => "error".to_owned(),
            })
            .collect()
    }

    #[test]
    fn every_profile_is_the_defaults_then_the_project_then_the_global_profiles() {
        let project = "[profile.zulu]\n[profile.mike]\n";
        let global = "[profile.alpha]\n[profile.zulu]\n[profile.kilo]\n";
        assert_eq!(
            every_profile(project, global, false),
            ["defaults", "mike", "zulu", "alpha", "kilo"]
        );
    }

    #[test]
    fn every_profile_skips_a_profile_whose_tenant_fails_to_resolve() {
        let project = "[profile.a]\n[profile.broken]\ntenant = \"a/b\"\n[profile.c]\n";
        assert_eq!(every_profile(project, "", false), ["defaults", "a", "c"]);
    }

    /// A bad value that the search does not read hides no profile: the
    /// wanted profile gives its error, and another profile is left out.
    #[test]
    fn a_wanted_profile_that_fails_to_resolve_gives_its_error() {
        let project = "[profile.acme]\ntenant = \"acme\"\ncontainer_engine = \"none\"\n\
                       [profile.other]\ntenant = \"other\"\ncontainer_engine = \"none\"\n";
        assert_eq!(every_profile(project, "", true), ["error"]);
    }

    #[test]
    fn the_registry_source_names_the_file_and_the_key() {
        let source = |project: &str, profile: Option<&str>| {
            sources(
                Some(loaded_file(
                    KnownPathKind::Project { explicit: true },
                    "/work/.snouty.toml",
                    project,
                )),
                Some(loaded_file(
                    KnownPathKind::Global,
                    "/home/settings.toml",
                    "registry = \"global.example.com\"\n",
                )),
            )
            .resolve(profile.map(str::to_owned))
            .unwrap()
            .registry()
            .map(|registry| registry.tag().clone())
        };
        assert_eq!(
            source("", None),
            Some(SettingSource::File {
                path: KnownPath {
                    kind: KnownPathKind::Global,
                    path: PathBuf::from("/home/settings.toml"),
                },
                key: "registry".to_owned(),
            })
        );
        assert_eq!(
            source(
                "[profile.p]\nregistry = \"project.example.com\"\n",
                Some("p")
            ),
            Some(SettingSource::File {
                path: KnownPath {
                    kind: KnownPathKind::Project { explicit: true },
                    path: PathBuf::from("/work/.snouty.toml"),
                },
                key: "profile.p.registry".to_owned(),
            })
        );
    }

    // ---- accessors -----------------------------------------------------

    #[test]
    fn a_missing_base_url_points_at_doctor() {
        let msg = Settings::default()
            .require_base_url()
            .unwrap_err()
            .to_string();
        assert!(msg.contains("tenant"), "unexpected error: {msg}");
        assert!(msg.contains("snouty doctor"), "unexpected error: {msg}");
    }

    #[test]
    fn base_url_falls_back_to_tenant_host() {
        let settings = Settings::builder().tenant("acme".parse().unwrap()).build();
        assert_eq!(
            settings.base_url().map(String::from).as_deref(),
            Some("https://acme.antithesis.com/")
        );
    }

    #[test]
    fn explicit_base_url_overrides_tenant_host() {
        let settings = Settings::builder()
            .tenant("acme".parse().unwrap())
            .base_url("https://example.test")
            .build();
        assert_eq!(
            settings.base_url().map(String::from).as_deref(),
            Some("https://example.test/")
        );
    }

    #[test]
    fn base_url_without_tenant_is_none() {
        assert_eq!(Settings::default().base_url(), None);
    }

    /// The settings that a project file with `contents` gives.
    fn resolve_file(contents: &str) -> Result<Settings> {
        sources(Some(settings_file(contents)), None).resolve(None)
    }

    #[test]
    fn each_setting_parses_into_its_type() {
        let settings = resolve_file(
            "https_proxy = \"http://proxy.corp:8080\"\n\
             container_engine = \"podman\"\n\
             update_channel = \"unstable\"\n\
             api_cache_max_file_size = 1234\n",
        )
        .unwrap();
        assert_eq!(
            settings.https_proxy().map(Url::as_str),
            Some("http://proxy.corp:8080/")
        );
        assert_eq!(settings.container_engine(), Some(ContainerEngine::Podman));
        assert_eq!(settings.update_channel(), UpdateChannel::Unstable);
        assert_eq!(settings.api_cache_max_file_size(), 1234);
    }

    #[test]
    fn a_proxy_without_a_scheme_is_an_http_url() {
        for (proxy, url) in [
            ("10.0.0.1:3128", "http://10.0.0.1:3128/"),
            ("proxy.corp", "http://proxy.corp/"),
            ("localhost:3128", "http://localhost:3128/"),
            ("https://proxy.corp:8443", "https://proxy.corp:8443/"),
        ] {
            let settings = resolve_file(&format!("https_proxy = \"{proxy}\"\n")).unwrap();
            assert_eq!(
                settings.https_proxy().map(Url::as_str),
                Some(url),
                "{proxy}"
            );
        }
    }

    #[test]
    fn an_unset_setting_takes_its_default() {
        let settings = resolve_file("").unwrap();
        assert_eq!(settings.https_proxy(), None);
        assert_eq!(settings.container_engine(), None);
        assert_eq!(settings.update_channel(), UpdateChannel::Stable);
        assert_eq!(
            settings.api_cache_max_file_size(),
            crate::api_cache::DEFAULT_MAX_FILE_SIZE
        );
    }

    #[test]
    fn a_value_that_does_not_parse_names_its_source() {
        let error = |contents: &str| resolve_file(contents).err().unwrap().to_string();
        assert_eq!(
            error("base_url = \"example.com\"\n"),
            "invalid setting `base_url` in /test/settings.toml: relative URL without a base"
        );
        assert_eq!(
            error("base_url = \"localhost:8080\"\n"),
            "invalid setting `base_url` in /test/settings.toml: \
             expected a URL such as https://acme.antithesis.com"
        );
        assert_eq!(
            error("https_proxy = \"htps://proxy.corp:3128\"\n"),
            "invalid setting `https_proxy` in /test/settings.toml: \
             expected an http or https proxy URL, got the scheme `htps`"
        );
        assert_eq!(
            error("https_proxy = \"http://\"\n"),
            "invalid setting `https_proxy` in /test/settings.toml: empty host"
        );
        assert_eq!(
            error("container_engine = \"none\"\n"),
            "invalid setting `container_engine` in /test/settings.toml: \
             expected `podman` or `docker`, got `none`"
        );
    }

    // ---- parse_byte_size -------------------------------------------------

    #[test]
    fn byte_sizes_parse_with_si_and_iec_units() {
        assert_eq!(parse_byte_size("10 MB").unwrap(), 10_000_000);
        assert_eq!(parse_byte_size("10 MiB").unwrap(), 10 * 1024 * 1024);
        // Units are case-insensitive and the space is optional.
        assert_eq!(parse_byte_size("1gb").unwrap(), 1_000_000_000);
        assert_eq!(parse_byte_size("1.5 KB").unwrap(), 1_500);
    }

    #[test]
    fn a_bare_number_parses_as_bytes() {
        assert_eq!(parse_byte_size("10485760").unwrap(), 10_485_760);
    }

    #[test]
    fn a_malformed_byte_size_names_the_expected_form() {
        for bad in ["ten megabytes", "-1", ""] {
            let msg = parse_byte_size(bad).unwrap_err();
            assert!(msg.contains("10 MB"), "unexpected error for {bad:?}: {msg}");
        }
    }

    // ---- Tenant -----------------------------------------------------

    #[test]
    fn valid_tenants_pass_host_validation() {
        for tenant in ["orbitinghail", "acme", "my-tenant", "t123", "foo.bar"] {
            assert!(
                tenant.parse::<Tenant>().is_ok(),
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
            "xn--a",
        ] {
            assert!(
                tenant.parse::<Tenant>().is_err(),
                "expected `{tenant}` to be rejected as a tenant host"
            );
        }
    }
}
