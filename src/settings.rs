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
pub use choice::{ContainerEngine, UpdateChannel};
pub use show::cmd_settings_show;

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
const GLOBAL_SETTINGS_FILENAME: &str = "settings.toml";
const REGISTRY_KEY: &str = "registry";
/// An alias for [`REGISTRY_KEY`].
const REPOSITORY_KEY: &str = "repository";

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
/// environment, the settings file that the user names and the global
/// `settings.toml` once, resolves each setting through the precedence chain,
/// and parses it into its type. A settings file that cannot be read or
/// parsed, or a value that does not parse, is an error there, so nothing
/// fails later.
///
/// Each value keeps the [`SettingSource`] that it came from.
///
/// Every command shares the same resolved instance (threaded by reference), so a
/// value resolves identically no matter which code path reads it.
#[cfg_attr(test, derive(Default))]
pub struct Settings {
    tenant: Option<Tagged<Tenant, SettingSource>>,
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
    /// Resolve settings from the environment, the settings file that the
    /// user names, and the global `settings.toml`.
    ///
    /// The user names a settings file with the `--settings` flag
    /// (`settings_path`), else with `SNOUTY_SETTINGS_PATH`. That file must
    /// exist. A settings file that exists but cannot be read or parsed is an
    /// error.
    pub fn resolve(settings_path: Option<PathBuf>) -> Result<Self> {
        // An empty `SNOUTY_SETTINGS_PATH` is unset; it would otherwise name
        // an empty path that must, and cannot, exist.
        let settings_path = match settings_path {
            Some(path) => Some(path),
            None => env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)?.map(PathBuf::from),
        };
        SettingsSources::from_ambient(settings_path.as_deref())?.resolve()
    }
}

/// Where a setting's value comes from: the place to change it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettingSource {
    Env(&'static str),
    /// `key` in the settings file at `path`.
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
    /// The environment variables, then the settings file at `settings_path`,
    /// then the global settings file.
    fn from_ambient(settings_path: Option<&Path>) -> Result<Self> {
        let named = match settings_path {
            Some(path) => KnownPath::new(KnownPathKind::Project, path).load(true)?,
            None => None,
        };
        let global = load_global_file()?;
        // `--settings` can name the global settings file. Then it is the
        // global file only.
        let named = named.filter(|named| {
            global
                .as_ref()
                .is_none_or(|global| *global.path != *named.path)
        });
        Ok(Self(
            std::iter::once(Layer::Env)
                .chain(named.into_iter().chain(global).map(Layer::File))
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

    fn resolve(&self) -> Result<Settings> {
        let text = |key: &str, env_var: &'static str| {
            resolve_value_with(string_value, &[key], &[env_var], self)
        };
        Ok(Settings {
            // The tenant is a host name even when an explicit base_url means
            // that no request goes to the tenant's host.
            tenant: parse_setting(text("tenant", ANTITHESIS_TENANT_VAR_NAME)?, str::parse)?,
            registry: parse_setting(
                resolve_value_with(
                    string_value,
                    &[REGISTRY_KEY, REPOSITORY_KEY],
                    &[ANTITHESIS_REGISTRY_VAR_NAME, ANTITHESIS_REPOSITORY_VAR_NAME],
                    self,
                )?,
                str::parse,
            )?,
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
                    self,
                )?,
                parse_byte_size,
            )?,
            files: self.files().map(|file| file.path.clone()).collect(),
        })
    }
}

impl Settings {
    /// The resolved tenant, or `None` if unset.
    pub fn tenant(&self) -> Option<&Tenant> {
        self.tenant.as_ref().map(Tagged::value)
    }

    /// The resolved container registry, tagged with where it comes from, or
    /// `None` if unset.
    pub fn registry(&self) -> Option<&Tagged<Registry, SettingSource>> {
        self.registry.as_ref()
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

    /// The settings that the global settings file alone gives, without the
    /// environment variables and the file that the user names: the values
    /// that `snouty login` writes.
    pub(crate) fn resolve_global_file() -> Result<Self> {
        SettingsSources::global_file()?.resolve()
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

/// Builds a test `Settings` without resolution. Each value is tagged as if
/// its environment variable set it.
#[cfg(test)]
pub struct SettingsBuilder(Settings);

#[cfg(test)]
impl SettingsBuilder {
    #[must_use]
    pub fn tenant(mut self, value: Tenant) -> Self {
        self.0.tenant = Some(value.with_tag(SettingSource::Env(ANTITHESIS_TENANT_VAR_NAME)));
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

/// Writes `tenant`, `registry` and, when given, `base_url` into the global
/// `settings.toml`. Returns what puts the file back as it was.
pub(crate) fn update_settings_in_global_file(
    tenant: &str,
    registry: &str,
    base_url: Option<&str>,
) -> Result<PreviousSettingsFile> {
    let settings_dir = global_settings_dir().ok_or_eyre(
        "Could not determine global settings directory. Ensure either $XDG_CONFIG_HOME or $HOME is set.",
    )?;
    let path = settings_dir.join(GLOBAL_SETTINGS_FILENAME);
    let previous = KnownPath::new(KnownPathKind::Global, &path).read(false)?;
    let mut contents = match &previous {
        Some(file) => file.clone().load()?.table,
        None => Table::new(),
    };

    contents.insert("tenant".to_owned(), Value::from(tenant));
    contents.insert(REGISTRY_KEY.to_owned(), Value::from(registry));
    if let Some(base_url) = base_url {
        contents.insert("base_url".to_owned(), Value::from(base_url));
    }
    // The file never holds the alias beside `registry`.
    contents.remove(REPOSITORY_KEY);
    mkdir(&settings_dir, true, 0o700)?;
    write_atomically(&path, toml::to_string_pretty(&contents)?.as_bytes())?;
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

/// Reads a setting's value out of a settings file: [`string_value`] for
/// text settings, [`integer_value`] for numeric ones.
type ValueReader = fn(&Value) -> Result<String, String>;

/// Resolve the text of a setting from `sources` with the precedence:
/// environment variable, then each file, most specific file first. The first
/// layer that has the key wins.
///
/// A setting can have more than one name, such as a name and its alias. Each
/// layer reads the names in order, so the first name wins over a later one
/// in the same layer, and a higher layer wins over both.
///
/// A layer that has the key with a value of the wrong type is an error.
/// Resolution does not skip to a lower layer, so a typo such as
/// `tenant = 123` is reported.
fn resolve_value_with(
    read: ValueReader,
    keys: &[&str],
    env_vars: &[&'static str],
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

    for file in sources.files() {
        for &key in keys {
            let Some(value) = file.table.get(key) else {
                continue;
            };
            let source = SettingSource::File {
                path: file.path.clone(),
                key: key.to_owned(),
            };
            return match read(value) {
                Ok(value) => Ok(Some(value.with_tag(source))),
                Err(err) => Err(invalid_setting(&source, err)),
            };
        }
    }

    Ok(None)
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
pub fn parse_base_url(text: &str) -> Result<Url, String> {
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
        loaded_file(KnownPathKind::Project, "/test/settings.toml", contents)
    }

    /// Leaves out the environment, so the files alone decide the result.
    fn sources(named: Option<LoadedFile>, global: Option<LoadedFile>) -> SettingsSources {
        SettingsSources(named.into_iter().chain(global).map(Layer::File).collect())
    }

    /// The text of the tenant setting.
    fn resolve_text(sources: &SettingsSources) -> Result<Option<Tagged<String, SettingSource>>> {
        resolve_value_with(
            string_value,
            &["tenant"],
            &[ANTITHESIS_TENANT_VAR_NAME],
            sources,
        )
    }

    /// Each layer in the precedence tests uses a distinct value, so the
    /// resolved value proves which layer won.
    fn resolve_tenant(named: Option<LoadedFile>, global: Option<LoadedFile>) -> Option<String> {
        resolve_text(&sources(named, global))
            .unwrap()
            .map(Tagged::untag)
    }

    // ---- strict TOML typing --------------------------------------------

    #[test]
    fn a_non_string_value_is_an_invalid_setting() {
        assert_eq!(
            resolve_text(&sources(Some(settings_file("tenant = 123\n")), None))
                .expect_err("the value should not resolve")
                .to_string(),
            "invalid setting `tenant` in /test/settings.toml: must be a string, but found integer"
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
    fn the_named_file_beats_the_global_file() {
        let named = settings_file("tenant = \"named\"\n");
        let global = settings_file("tenant = \"global\"\n");
        assert_eq!(
            resolve_tenant(Some(named), Some(global)).as_deref(),
            Some("named")
        );
    }

    #[test]
    fn the_global_file_is_the_last_resort() {
        let global = settings_file("tenant = \"global\"\n");
        assert_eq!(
            resolve_tenant(None, Some(global)).as_deref(),
            Some("global")
        );
    }

    #[test]
    fn nothing_set_resolves_to_none() {
        assert!(resolve_tenant(None, None).is_none());
    }

    #[test]
    fn the_registry_source_names_the_file_and_the_key() {
        let source = |named: &str| {
            sources(
                Some(loaded_file(
                    KnownPathKind::Project,
                    "/work/settings.toml",
                    named,
                )),
                Some(loaded_file(
                    KnownPathKind::Global,
                    "/home/settings.toml",
                    "registry = \"global.example.com\"\n",
                )),
            )
            .resolve()
            .unwrap()
            .registry()
            .map(|registry| registry.tag().clone())
        };
        assert_eq!(
            source(""),
            Some(SettingSource::File {
                path: KnownPath {
                    kind: KnownPathKind::Global,
                    path: PathBuf::from("/home/settings.toml"),
                },
                key: "registry".to_owned(),
            })
        );
        assert_eq!(
            source("repository = \"named.example.com\"\n"),
            Some(SettingSource::File {
                path: KnownPath {
                    kind: KnownPathKind::Project,
                    path: PathBuf::from("/work/settings.toml"),
                },
                key: "repository".to_owned(),
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

    /// The settings that a named settings file with `contents` gives.
    fn resolve_file(contents: &str) -> Result<Settings> {
        sources(Some(settings_file(contents)), None).resolve()
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
