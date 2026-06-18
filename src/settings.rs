use std::{
    cell::OnceCell,
    env,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use color_eyre::eyre::{Report, Result, eyre};
use toml::Table;

pub const ANTITHESIS_PROFILE_ENV_VAR_NAME: &str = "ANTITHESIS_PROFILE";
pub const SNOUTY_SETTINGS_PATH_VAR_NAME: &str = "SNOUTY_SETTINGS_PATH";
pub const ANTITHESIS_TENANT_VAR_NAME: &str = "ANTITHESIS_TENANT";
pub const ANTITHESIS_REPOSITORY_VAR_NAME: &str = "ANTITHESIS_REPOSITORY";
pub const ANTITHESIS_BASE_URL_VAR_NAME: &str = "ANTITHESIS_BASE_URL";
pub const CONTAINER_ENGINE_VAR_NAME: &str = "SNOUTY_CONTAINER_ENGINE";
const PROJECT_SETTINGS_FILENAME: &str = ".snouty.toml";
const GLOBAL_CONFIG_SETTINGS_FILENAME: &str = "settings.toml";
const PROFILE_KEY: &str = "profile";

fn global_config_dir() -> Option<PathBuf> {
    let base_dir = if let Ok(xdg_config_home) = env::var("XDG_CONFIG_HOME") {
        Some(PathBuf::from(xdg_config_home))
    } else if let Ok(home) = env::var("HOME") {
        Some(PathBuf::from(home).join(".config"))
    } else {
        None // No global config for Windows users :(
    };

    base_dir.map(|dir| dir.join("snouty"))
}

pub fn cache_dir() -> Option<PathBuf> {
    let base_dir = if let Ok(xdg_config_home) = env::var("XDG_CACHE_HOME") {
        Some(PathBuf::from(xdg_config_home))
    } else if let Ok(home) = env::var("HOME") {
        Some(PathBuf::from(home).join(".cache"))
    } else {
        None // No cache for Windows users :(
    };

    base_dir.map(|dir| dir.join("snouty"))
}

#[derive(Debug, Clone)]
pub(crate) struct SharedReport(Arc<Report>);

impl Display for SharedReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&*self.0, f)
    }
}

impl std::error::Error for SharedReport {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.chain().nth(1)
    }
}

pub(crate) enum ValueSource {
    EnvironmentVariable,
    ProjectProfile,
    GlobalProfile,
    ProjectDefault,
    GlobalDefault,
}

pub(crate) struct AttributedValue<T> {
    pub(crate) value: T,
    pub(crate) attribution: ValueSource,
}

fn attribute<T>(value: T, attribution: ValueSource) -> AttributedValue<T> {
    AttributedValue { value, attribution }
}

pub(crate) struct LoadedSettings {
    tenant: Result<Option<String>, SharedReport>,
    repository: Result<Option<String>, SharedReport>,
    base_url: Result<Option<String>, SharedReport>,
    container_engine: Result<Option<String>, SharedReport>,
}

pub(crate) struct SettingsFromFile {
    pub(crate) resolved_path: PathBuf,
    for_profile: Option<LoadedSettings>,
    defaults: LoadedSettings,
}

pub struct Settings {
    profile: Option<String>,
    project_settings_path: Option<PathBuf>,
    settings_from_environment: OnceCell<LoadedSettings>,
    settings_from_project_config: OnceCell<Result<Option<SettingsFromFile>, SharedReport>>,
    settings_from_global_config: OnceCell<Result<Option<SettingsFromFile>, SharedReport>>,
    tenant: OnceCell<Result<String, SharedReport>>,
    repository: OnceCell<Result<String, SharedReport>>,
    base_url: OnceCell<Result<String, SharedReport>>,
    container_engine: OnceCell<Result<Option<String>, SharedReport>>,
}

fn load_environment_variable(key: &str) -> Result<Option<String>, SharedReport> {
    match env::var(key) {
        Ok(value) => Ok(if value.is_empty() { None } else { Some(value) }),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(SharedReport(Arc::new(eyre!(
            "The value of environment variable [{key}] was not valid unicode"
        )))),
    }
}

fn load_settings_from_toml(
    contents: String,
    path: &Path,
    profile: Option<&str>,
) -> Result<SettingsFromFile, SharedReport> {
    match contents.parse::<Table>() {
        Ok(parsed) => Ok(SettingsFromFile {
            resolved_path: path.to_path_buf(),
            for_profile: profile.map(|profile| LoadedSettings {
                tenant: Ok(try_resolve_from_profile(&parsed, "tenant", profile)),
                repository: Ok(try_resolve_from_profile(&parsed, "repository", profile)),
                base_url: Ok(try_resolve_from_profile(&parsed, "base_url", profile)),
                container_engine: Ok(try_resolve_from_profile(
                    &parsed,
                    "container_engine",
                    profile,
                )),
            }),
            defaults: LoadedSettings {
                tenant: Ok(try_resolve_from_defaults(&parsed, "tenant")),
                repository: Ok(try_resolve_from_defaults(&parsed, "repository")),
                base_url: Ok(try_resolve_from_defaults(&parsed, "base_url")),
                container_engine: Ok(try_resolve_from_defaults(&parsed, "container_engine")),
            },
        }),
        Err(err) => Err(SharedReport(Arc::new(eyre!(
            "Config file at {:?} was not valid TOML: {err:#}",
            path
        )))),
    }
}

fn load_settings_from_config_file(
    path: &Path,
    profile: Option<&str>,
) -> Result<SettingsFromFile, SharedReport> {
    match fs::read_to_string(path) {
        Ok(contents) => load_settings_from_toml(contents, path, profile),
        Err(err) => Err(SharedReport(Arc::new(eyre!(
            "Config file at {:?} could not be found or failed to be read: {err:#}",
            path
        )))),
    }
}

fn try_load_file_contents(path: &Path) -> Result<Option<String>, SharedReport> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(SharedReport(Arc::new(eyre!(
                "File at {:?} could not be read: {err:#}",
                path
            )))),
        },
    }
}

fn try_unwrap<F, F1, T>(
    all_settings: &Result<Option<SettingsFromFile>, SharedReport>,
    deref_settings: F1,
    deref_setting: F,
) -> Result<Option<&T>, SharedReport>
where
    F1: Fn(&SettingsFromFile) -> Option<&LoadedSettings>,
    F: Fn(&LoadedSettings) -> &Result<Option<T>, SharedReport>,
{
    match all_settings {
        Err(err) => Err(err.clone()),
        Ok(None) => Ok(None),
        Ok(Some(settings)) => match deref_settings(settings) {
            None => Ok(None),
            Some(settings) => match deref_setting(settings) {
                Err(err) => Err(err.clone()),
                Ok(None) => Ok(None),
                Ok(Some(value)) => Ok(Some(value)),
            },
        },
    }
}

impl Settings {
    pub fn new(project_settings_path: Option<PathBuf>, profile: Option<String>) -> Self {
        Self {
            profile: profile.or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok()),
            project_settings_path: project_settings_path.or_else(|| {
                env::var(SNOUTY_SETTINGS_PATH_VAR_NAME)
                    .ok()
                    .map(PathBuf::from)
            }),
            settings_from_environment: OnceCell::new(),
            settings_from_project_config: OnceCell::new(),
            settings_from_global_config: OnceCell::new(),
            tenant: OnceCell::new(),
            repository: OnceCell::new(),
            base_url: OnceCell::new(),
            container_engine: OnceCell::new(),
        }
    }

    pub fn tenant(&self) -> Result<&str> {
        self.tenant_with_shared_error()
            .map_err(|err| Report::new(err.clone()))
    }

    fn tenant_with_shared_error(&self) -> Result<&str, &SharedReport> {
        self.tenant
            .get_or_init(|| match self.try_resolve_tenant() {
                Ok(Some(tenant)) => Ok(tenant.value.clone()),
                Ok(None) => Err(SharedReport(Arc::new(eyre!(
                    "Could not resolve Antithesis tenant. Run snouty doctor to debug."
                )))),
                Err(err) => Err(SharedReport(Arc::new(eyre!(
                    "Error reading configuration for [tenant]: {err:#}"
                )))),
            })
            .as_deref()
    }

    pub(crate) fn try_resolve_tenant(
        &self,
    ) -> Result<Option<AttributedValue<&String>>, SharedReport> {
        self.try_resolve(|settings| &settings.tenant)
    }

    pub fn repository(&self) -> Result<&str> {
        self.repository
            .get_or_init(|| match self.try_resolve_repository() {
                Ok(Some(repository)) => Ok(repository.value.clone()),
                Ok(None) => Err(SharedReport(Arc::new(eyre!(
                    "Could not resolve Antithesis repository. Run snouty doctor to debug."
                )))),
                Err(err) => Err(SharedReport(Arc::new(eyre!(
                    "Error reading configuration for [repository]: {err:#}"
                )))),
            })
            .as_deref()
            .map_err(|err| Report::new(err.clone()))
    }

    pub(crate) fn try_resolve_repository(
        &self,
    ) -> Result<Option<AttributedValue<&String>>, SharedReport> {
        self.try_resolve(|settings| &settings.repository)
    }

    pub fn base_url(&self) -> Result<&str> {
        self.base_url
            .get_or_init(|| match &self.try_resolve_base_url() {
                Ok(Some(ok)) => Ok(ok.value.clone()),
                Err(err) => Err(err.clone()),
                Ok(None) => match self.tenant_with_shared_error() {
                    Ok(tenant) => Ok(format!("https://{}.antithesis.com", tenant)),
                    Err(err) => Err(err.clone()),
                },
            })
            .as_deref()
            .map_err(|err| Report::new(err.clone()))
    }

    pub(crate) fn try_resolve_base_url(
        &self,
    ) -> Result<Option<AttributedValue<&String>>, SharedReport> {
        self.try_resolve(|settings| &settings.base_url)
    }

    pub fn container_engine(&self) -> Result<Option<&str>> {
        self.container_engine
            .get_or_init(|| match self.try_resolve_container_engine() {
                Ok(Some(container_engine)) => Ok(Some(container_engine.value.clone())),
                Ok(None) => Ok(None),
                Err(err) => Err(err),
            })
            .as_ref()
            .map(|o| o.as_deref())
            .map_err(|err| Report::new(err.clone()))
    }

    pub(crate) fn try_resolve_container_engine(
        &self,
    ) -> Result<Option<AttributedValue<&String>>, SharedReport> {
        self.try_resolve(|settings| &settings.container_engine)
    }

    fn try_resolve<F, T>(&self, deref_fn: F) -> Result<Option<AttributedValue<&T>>, SharedReport>
    where
        F: Fn(&LoadedSettings) -> &Result<Option<T>, SharedReport>,
    {
        match deref_fn(self.load_settings_from_env()) {
            Ok(Some(found)) => Ok(Some(attribute(found, ValueSource::EnvironmentVariable))),
            Err(err) => Err(err.clone()),
            // not found on the environment; moving on to project profile settings
            Ok(None) => match try_unwrap(
                self.load_project_settings(),
                |project_settings| project_settings.for_profile.as_ref(),
                &deref_fn,
            ) {
                Ok(Some(found)) => Ok(Some(attribute(found, ValueSource::ProjectProfile))),
                Err(err) => Err(err.clone()),
                // Not found in project profile; moving on to global profile
                Ok(None) => match try_unwrap(
                    self.load_global_settings(),
                    |global_settings| global_settings.for_profile.as_ref(),
                    &deref_fn,
                ) {
                    Ok(Some(found)) => Ok(Some(attribute(found, ValueSource::GlobalProfile))),
                    Err(err) => Err(err.clone()),
                    // Not found in global profile; moving on to project defaults
                    Ok(None) => match try_unwrap(
                        self.load_project_settings(),
                        |project_settings| Some(&project_settings.defaults),
                        &deref_fn,
                    ) {
                        Ok(Some(found)) => Ok(Some(attribute(found, ValueSource::ProjectDefault))),
                        Err(err) => Err(err.clone()),
                        // Not found in project defaults; falling back to global defaults
                        Ok(None) => match try_unwrap(
                            self.load_global_settings(),
                            |global_settings| Some(&global_settings.defaults),
                            &deref_fn,
                        ) {
                            Ok(Some(found)) => {
                                Ok(Some(attribute(found, ValueSource::GlobalDefault)))
                            }
                            Ok(None) => Ok(None),
                            Err(err) => Err(err.clone()),
                        },
                    },
                },
            },
        }
    }

    fn load_settings_from_env(&self) -> &LoadedSettings {
        self.settings_from_environment
            .get_or_init(|| LoadedSettings {
                tenant: load_environment_variable(ANTITHESIS_TENANT_VAR_NAME),
                repository: load_environment_variable(ANTITHESIS_REPOSITORY_VAR_NAME),
                base_url: load_environment_variable(ANTITHESIS_BASE_URL_VAR_NAME),
                container_engine: load_environment_variable(CONTAINER_ENGINE_VAR_NAME),
            })
    }

    pub(crate) fn load_project_settings(&self) -> &Result<Option<SettingsFromFile>, SharedReport> {
        self.settings_from_project_config.get_or_init(|| {
            if let Some(project_settings_path) = &self.project_settings_path {
                return load_settings_from_config_file(
                    project_settings_path,
                    self.profile.as_deref(),
                )
                .map(Some);
            }

            // check the current directory. If we want to climb the directory tree in the future, this would be where to do it
            let default_path = PathBuf::from(PROJECT_SETTINGS_FILENAME);
            match try_load_file_contents(&default_path) {
                Ok(Some(contents)) => {
                    load_settings_from_toml(contents, &default_path, self.profile.as_deref())
                        .map(Some)
                }
                Ok(None) => Ok(None),
                Err(err) => Err(err),
            }
        })
    }

    pub(crate) fn load_global_settings(&self) -> &Result<Option<SettingsFromFile>, SharedReport> {
        self.settings_from_global_config.get_or_init(|| {
            if let Some(config_dir) = global_config_dir() {
                let path = config_dir.join(GLOBAL_CONFIG_SETTINGS_FILENAME);
                match try_load_file_contents(&path) {
                    Ok(Some(contents)) => {
                        load_settings_from_toml(contents, &path, self.profile.as_deref()).map(Some)
                    }
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                }
            } else {
                Ok(None)
            }
        })
    }

    pub(crate) fn settings_profile(&self) -> Option<&str> {
        self.profile.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn for_test_base_url(base_url: String) -> Self {
        let error = SharedReport(Arc::new(eyre!("Shouldn't have read from me!")));
        Self {
            profile: None,
            project_settings_path: None,
            base_url: OnceCell::from(Ok(base_url)),
            tenant: OnceCell::from(Err(error.clone())),
            repository: OnceCell::from(Err(error.clone())),
            container_engine: OnceCell::from(Err(error.clone())),
            settings_from_environment: OnceCell::from(LoadedSettings {
                tenant: Err(error.clone()),
                repository: Err(error.clone()),
                base_url: Err(error.clone()),
                container_engine: Err(error.clone()),
            }),
            settings_from_project_config: OnceCell::from(Ok(None)),
            settings_from_global_config: OnceCell::from(Ok(None)),
        }
    }
}

fn try_resolve_from_profile(config: &Table, key: &str, profile: &str) -> Option<String> {
    config
        .get(PROFILE_KEY)
        .and_then(|profiles| profiles.as_table())
        .and_then(|profiles_table| profiles_table.get(profile))
        .and_then(|profile_value| profile_value.as_table())
        .and_then(|profile_table| profile_table.get(key))
        .and_then(|profile_tenant| profile_tenant.as_str())
        .map(|value| value.to_string())
}

fn try_resolve_from_defaults(config: &Table, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(|profile_tenant| profile_tenant.as_str())
        .map(|value| value.to_string())
}
