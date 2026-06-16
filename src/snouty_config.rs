use std::{
    cell::{OnceCell, RefCell},
    collections::{HashMap, hash_map::Entry},
    env,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use color_eyre::eyre::{Report, Result, eyre};
use toml::Table;

pub const ANTITHESIS_PROFILE_ENV_VAR_NAME: &str = "ANTITHESIS_PROFILE";
pub const ANTITHESIS_TENANT_VAR_NAME: &str = "ANTITHESIS_TENANT";
pub const ANTITHESIS_REPOSITORY_VAR_NAME: &str = "ANTITHESIS_REPOSITORY";
pub const ANTITHESIS_BASE_URL_VAR_NAME: &str = "ANTITHESIS_BASE_URL";
pub const ANTITHESIS_DOCS_DB_PATH_VAR_NAME: &str = "ANTITHESIS_DOCS_DB_PATH";
pub const ANTITHESIS_DOCS_URL_VAR_NAME: &str = "ANTITHESIS_DOCS_URL";
pub const CONTAINER_ENGINE_VAR_NAME: &str = "SNOUTY_CONTAINER_ENGINE";
pub const TEST_RUNTIME_VAR_NAME: &str = "SNOUTY_TEST_RUNTIME";
pub const CACHE_DIR_VAR_NAME: &str = "SNOUTY_TEST_CACHE_DIR";
pub const TEMP_DIR_VAR_NAME: &str = "SNOUTY_TEMP_DIR";
const PROJECT_CONFIG_SETTINGS_FILENAME: &str = ".snouty.toml";
const GLOBAL_CONFIG_SETTINGS_FILENAME: &str = "settings.toml";
const PROFILES_KEY: &str = "profiles";

pub trait SnoutyConfig {
    fn tenant(&self) -> Result<&str>;
    fn repository(&self) -> Result<&str>;
    fn base_url(&self) -> Option<&str>;
    fn resolve_base_url(&self) -> Result<String> {
        if let Some(explicit_base_url) = self.base_url() {
            Ok(explicit_base_url.to_owned())
        } else {
            match self.tenant() {
                Ok(tenant) => Ok(format!("https://{}.antithesis.com", tenant)),
                Err(report) => Err(eyre!("{report:#}")),
            }
        }
    }
    fn docs_url(&self) -> Option<&str>;
    fn docs_db_path(&self) -> Option<&str>;
    fn container_engine(&self) -> Option<&str>;
    fn test_runtime(&self) -> Option<&str>;
    fn cache_dir(&self) -> Option<&Path>;
    fn temp_dir(&self) -> Option<&Path>;
}

pub fn default_config(project_config_location: Option<PathBuf>) -> impl SnoutyConfig {
    DefaultSnoutyConfig {
        profile: env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok(),
        project_config_location,
        cache: RefCell::new(HashMap::new()),
        tenant: OnceCell::new(),
        repository: OnceCell::new(),
        base_url: OnceCell::new(),
        docs_url: OnceCell::new(),
        docs_db_path: OnceCell::new(),
        container_engine: OnceCell::new(),
        test_runtime: OnceCell::new(),
        cache_dir: OnceCell::new(),
        temp_dir: OnceCell::new(),
    }
}

struct DefaultSnoutyConfig {
    profile: Option<String>,
    project_config_location: Option<PathBuf>,
    cache: RefCell<HashMap<PathBuf, Option<Table>>>,
    tenant: OnceCell<Result<String, SharedReport>>,
    repository: OnceCell<Result<String, SharedReport>>,
    base_url: OnceCell<Option<String>>,
    docs_url: OnceCell<Option<String>>,
    docs_db_path: OnceCell<Option<String>>,
    container_engine: OnceCell<Option<String>>,
    test_runtime: OnceCell<Option<String>>,
    cache_dir: OnceCell<Option<PathBuf>>,
    temp_dir: OnceCell<Option<PathBuf>>,
}

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

fn default_cache_dir() -> Option<PathBuf> {
    let base_dir = if let Ok(xdg_config_home) = env::var("XDG_CACHE_HOME") {
        Some(PathBuf::from(xdg_config_home))
    } else if let Ok(home) = env::var("HOME") {
        Some(PathBuf::from(home).join(".cache"))
    } else {
        None // No cache for Windows users :(
    };

    base_dir.map(|dir| dir.join("snouty"))
}

impl SnoutyConfig for DefaultSnoutyConfig {
    fn tenant(&self) -> Result<&str> {
        self.tenant
            .get_or_init(|| {
                resolve_required_config_value_from_environment_or_project(
                    "tenant",
                    Some(ANTITHESIS_TENANT_VAR_NAME),
                    &self.profile,
                    global_config_dir(),
                    self.project_config_location.as_ref(),
                    &mut self.cache.borrow_mut(),
                )
                .map_err(|e| SharedReport(Arc::new(e)))
            })
            .as_deref()
            .map_err(|e| Report::new(e.clone()))
    }

    fn repository(&self) -> Result<&str> {
        self.repository
            .get_or_init(|| {
                resolve_required_config_value_from_environment_or_project(
                    "repository",
                    Some(ANTITHESIS_REPOSITORY_VAR_NAME),
                    &self.profile,
                    global_config_dir(),
                    self.project_config_location.as_ref(),
                    &mut self.cache.borrow_mut(),
                )
                .map_err(|e| SharedReport(Arc::new(e)))
            })
            .as_deref()
            .map_err(|e| Report::new(e.clone()))
    }

    fn base_url(&self) -> Option<&str> {
        self.base_url
            .get_or_init(|| env::var(ANTITHESIS_BASE_URL_VAR_NAME).ok())
            .as_deref()
    }

    fn docs_url(&self) -> Option<&str> {
        self.docs_url
            .get_or_init(|| env::var(ANTITHESIS_DOCS_URL_VAR_NAME).ok())
            .as_deref()
    }

    fn docs_db_path(&self) -> Option<&str> {
        self.docs_db_path
            .get_or_init(|| env::var(ANTITHESIS_DOCS_DB_PATH_VAR_NAME).ok())
            .as_deref()
    }

    fn container_engine(&self) -> Option<&str> {
        self.container_engine
            .get_or_init(|| env::var(CONTAINER_ENGINE_VAR_NAME).ok())
            .as_deref()
    }

    fn test_runtime(&self) -> Option<&str> {
        self.test_runtime
            .get_or_init(|| env::var(TEST_RUNTIME_VAR_NAME).ok())
            .as_deref()
    }

    fn cache_dir(&self) -> Option<&Path> {
        self.cache_dir
            .get_or_init(|| {
                env::var(CACHE_DIR_VAR_NAME)
                    .ok()
                    .map(PathBuf::from)
                    .or_else(default_cache_dir)
            })
            .as_deref()
    }

    fn temp_dir(&self) -> Option<&Path> {
        self.temp_dir
            .get_or_init(|| env::var(TEMP_DIR_VAR_NAME).ok().map(PathBuf::from))
            .as_deref()
    }
}

#[derive(Debug, Clone)]
struct SharedReport(Arc<Report>);

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

fn resolve_required_config_value_from_environment_or_project(
    config_key: &str,
    environment_variable_name: Option<&str>,
    profile: &Option<String>,
    global_config_location: Option<PathBuf>,
    project_config_location: Option<&PathBuf>,
    cache: &mut HashMap<PathBuf, Option<Table>>,
) -> Result<String> {
    if let Some(found) = resolve_config_value_from_environment_or_project(
        config_key,
        environment_variable_name,
        profile,
        global_config_location,
        project_config_location,
        cache,
    ) {
        Ok(found)
    } else {
        Err(eyre!("No {} found", config_key))
    }
}

fn resolve_config_value_from_environment_or_project(
    config_key: &str,
    environment_variable_name: Option<&str>,
    profile: &Option<String>,
    global_config_location: Option<PathBuf>,
    project_config_location: Option<&PathBuf>,
    cache: &mut HashMap<PathBuf, Option<Table>>,
) -> Option<String> {
    if let Some(value_from_env_var) =
        environment_variable_name.and_then(|var_name| env::var(var_name).ok())
    {
        // environment variable always has highest precedence
        return Some(value_from_env_var);
    }

    if let Some(profile_name) = profile {
        // If a profile name was specified, we need to check both the project and global config files for that specific profile
        // before falling back to either project or global defaults
        if let Some(project_dir) = project_config_location {
            let cached =
                try_load_toml_file(cache, &project_dir.join(PROJECT_CONFIG_SETTINGS_FILENAME));

            if let Some(value_from_project_profile) =
                cached.and_then(|config| try_resolve_from_profile(config, config_key, profile_name))
            {
                return Some(value_from_project_profile);
            }
        }

        if let Some(global_config_path) = global_config_location
            .as_ref()
            .map(|dir| dir.join(GLOBAL_CONFIG_SETTINGS_FILENAME))
        {
            let cached = try_load_toml_file(cache, &global_config_path);

            if let Some(value_from_global_profile) =
                cached.and_then(|config| try_resolve_from_profile(config, config_key, profile_name))
            {
                return Some(value_from_global_profile);
            }
        }
    }

    if let Some(path_to_project_config) = project_config_location
        .as_ref()
        .map(|d| d.join(PROJECT_CONFIG_SETTINGS_FILENAME))
    {
        let cached = try_load_toml_file(cache, &path_to_project_config);

        if let Some(value_from_project_defaults) =
            cached.and_then(|config| try_resolve_from_defaults(config, config_key))
        {
            return Some(value_from_project_defaults);
        }
    }

    if let Some(global_config_path) = global_config_location
        .as_ref()
        .map(|dir| dir.join(GLOBAL_CONFIG_SETTINGS_FILENAME))
    {
        let cached = try_load_toml_file(cache, &global_config_path);

        if let Some(value_from_global_defaults) =
            cached.and_then(|config| try_resolve_from_defaults(config, config_key))
        {
            return Some(value_from_global_defaults);
        }
    }

    None
}

fn try_load_toml_file<'a>(
    cache: &'a mut HashMap<PathBuf, Option<Table>>,
    path: &PathBuf,
) -> Option<&'a Table> {
    match cache.entry(path.clone()) {
        Entry::Occupied(already_loaded) => already_loaded.into_mut().as_ref(),
        Entry::Vacant(entry) => entry
            .insert(
                fs::read_to_string(path)
                    .ok()
                    .and_then(|contents| contents.parse().ok()),
            )
            .as_ref(),
    }
}

fn try_resolve_from_profile(config: &Table, key: &str, profile: &str) -> Option<String> {
    config
        .get(PROFILES_KEY)
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Mutex, MutexGuard};

    use tempfile::TempDir;

    // Serializes tests that mutate process-wide env vars so parallel tests
    // in this binary don't race. Poisoning is recovered from so a panicking
    // test doesn't take the rest of the suite down.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    const ENV_VARS_TOUCHED: &[&str] = &[
        ANTITHESIS_PROFILE_ENV_VAR_NAME,
        ANTITHESIS_TENANT_VAR_NAME,
        ANTITHESIS_REPOSITORY_VAR_NAME,
        ANTITHESIS_BASE_URL_VAR_NAME,
        ANTITHESIS_DOCS_DB_PATH_VAR_NAME,
        ANTITHESIS_DOCS_URL_VAR_NAME,
        CONTAINER_ENGINE_VAR_NAME,
        TEST_RUNTIME_VAR_NAME,
        CACHE_DIR_VAR_NAME,
        TEMP_DIR_VAR_NAME,
        "XDG_CONFIG_HOME",
    ];

    /// Holds ENV_LOCK and snapshots/clears the env vars the resolver reads,
    /// restoring them on drop. XDG_CONFIG_HOME is pointed at an empty
    /// tempdir so `directories_next` can't pick up a real settings.toml
    /// from the developer's home directory (Linux only — on macOS/Windows
    /// the global-config lookup is not isolated by these tests).
    struct TestEnv {
        _lock: MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<String>)>,
        _xdg: TempDir,
    }

    impl TestEnv {
        fn new() -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
            let saved: Vec<_> = ENV_VARS_TOUCHED
                .iter()
                .map(|k| (*k, env::var(k).ok()))
                .collect();
            for (k, _) in &saved {
                // SAFETY: ENV_LOCK is held for the lifetime of this guard,
                // so no other test thread mutates or reads env vars concurrently.
                unsafe { env::remove_var(k) };
            }
            let xdg = TempDir::new().unwrap();
            // SAFETY: see above.
            unsafe { env::set_var("XDG_CONFIG_HOME", xdg.path()) };
            Self {
                _lock: lock,
                saved,
                _xdg: xdg,
            }
        }

        fn set(&self, key: &str, value: &str) {
            // SAFETY: ENV_LOCK held via self._lock.
            unsafe { env::set_var(key, value) };
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                // SAFETY: ENV_LOCK held until this Drop completes.
                match v {
                    Some(value) => unsafe { env::set_var(k, value) },
                    None => unsafe { env::remove_var(k) },
                }
            }
        }
    }

    fn write_project_config(dir: &TempDir, contents: &str) {
        fs::write(dir.path().join(PROJECT_CONFIG_SETTINGS_FILENAME), contents).unwrap();
    }

    #[test]
    fn tenant_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_TENANT_VAR_NAME, "from-env");

        let config = default_config(None);
        assert_eq!(config.tenant().unwrap(), "from-env");
    }

    #[test]
    fn tenant_resolves_from_project_config_defaults() {
        let _env = TestEnv::new();
        let project = TempDir::new().unwrap();
        write_project_config(&project, r#"tenant = "from-file""#);

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.tenant().unwrap(), "from-file");
    }

    #[test]
    fn tenant_resolves_from_project_config_profile() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_PROFILE_ENV_VAR_NAME, "prod");
        let project = TempDir::new().unwrap();
        write_project_config(
            &project,
            r#"
                tenant = "default"

                [profiles.prod]
                tenant = "prod-only"
            "#,
        );

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.tenant().unwrap(), "prod-only");
    }

    #[test]
    fn tenant_env_var_takes_precedence_over_project_file() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_TENANT_VAR_NAME, "from-env");
        let project = TempDir::new().unwrap();
        write_project_config(&project, r#"tenant = "from-file""#);

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.tenant().unwrap(), "from-env");
    }

    #[test]
    fn tenant_unknown_profile_falls_back_to_defaults() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_PROFILE_ENV_VAR_NAME, "missing");
        let project = TempDir::new().unwrap();
        write_project_config(&project, r#"tenant = "fallback""#);

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.tenant().unwrap(), "fallback");
    }

    #[test]
    fn tenant_missing_everywhere_returns_error() {
        let _env = TestEnv::new();
        let project = TempDir::new().unwrap();

        let config = default_config(Some(project.path().to_path_buf()));
        assert!(config.tenant().is_err());
    }

    #[test]
    fn tenant_result_is_memoized_across_calls() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_TENANT_VAR_NAME, "first");
        let config = default_config(None);

        let first = config.tenant().unwrap();
        // Mutating the env after the first call must not affect the cached value.
        env.set(ANTITHESIS_TENANT_VAR_NAME, "second");
        let second = config.tenant().unwrap();

        assert_eq!(first, "first");
        assert_eq!(second, "first");
    }

    #[test]
    fn repository_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_REPOSITORY_VAR_NAME, "acme/repo");

        let config = default_config(None);
        assert_eq!(config.repository().unwrap(), "acme/repo");
    }

    #[test]
    fn base_url_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_BASE_URL_VAR_NAME, "https://example.invalid");

        let config = default_config(None);
        assert_eq!(
            config.base_url().as_deref(),
            Some("https://example.invalid"),
        );
    }

    #[test]
    fn base_url_is_none_when_unset() {
        let _env = TestEnv::new();
        let config = default_config(None);
        assert!(config.base_url().is_none());
    }

    #[test]
    fn repository_resolves_from_project_config_defaults() {
        let _env = TestEnv::new();
        let project = TempDir::new().unwrap();
        write_project_config(
            &project,
            r#"
                tenant = "acme"
                repository = "acme/repo"
            "#,
        );

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.repository().unwrap(), "acme/repo");
    }

    #[test]
    fn repository_resolves_from_project_config_profile() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_PROFILE_ENV_VAR_NAME, "prod");
        let project = TempDir::new().unwrap();
        write_project_config(
            &project,
            r#"
                repository = "default-repo"

                [profiles.prod]
                repository = "prod-repo"
            "#,
        );

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.repository().unwrap(), "prod-repo");
    }

    #[test]
    fn repository_env_var_takes_precedence_over_project_file() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_REPOSITORY_VAR_NAME, "from-env/repo");
        let project = TempDir::new().unwrap();
        write_project_config(&project, r#"repository = "from-file/repo""#);

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.repository().unwrap(), "from-env/repo");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn tenant_resolves_from_global_profile_config_over_project_config() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_PROFILE_ENV_VAR_NAME, "staging");
        let xdg_config_home = TempDir::new().unwrap();
        env.set("XDG_CONFIG_HOME", xdg_config_home.path().to_str().unwrap());

        let global_config_dir = xdg_config_home.path().join("snouty");
        fs::create_dir(xdg_config_home.path().join("snouty")).unwrap();
        fs::write(
            global_config_dir.join(GLOBAL_CONFIG_SETTINGS_FILENAME),
            r#"
                tenant = "foo"

                [profiles.staging]
                tenant = "bar"
            "#,
        )
        .unwrap();

        let project = TempDir::new().unwrap();
        write_project_config(
            &project,
            r#"
                tenant = "baz"
            "#,
        );

        let config = default_config(Some(project.path().to_path_buf()));
        assert_eq!(config.tenant().unwrap(), "bar",);
    }

    #[test]
    fn docs_url_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_DOCS_URL_VAR_NAME, "https://docs.invalid");

        let config = default_config(None);
        assert_eq!(config.docs_url().as_deref(), Some("https://docs.invalid"));
    }

    #[test]
    fn docs_db_path_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(ANTITHESIS_DOCS_DB_PATH_VAR_NAME, "/tmp/docs.sqlite");

        let config = default_config(None);
        assert_eq!(config.docs_db_path().as_deref(), Some("/tmp/docs.sqlite"));
    }

    #[test]
    fn container_engine_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(CONTAINER_ENGINE_VAR_NAME, "podman");

        let config = default_config(None);
        assert_eq!(config.container_engine().as_deref(), Some("podman"));
    }

    #[test]
    fn test_runtime_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(TEST_RUNTIME_VAR_NAME, "antithesis");

        let config = default_config(None);
        assert_eq!(config.test_runtime().as_deref(), Some("antithesis"));
    }

    #[test]
    fn cache_dir_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(CACHE_DIR_VAR_NAME, "/var/cache/snouty");

        let config = default_config(None);
        assert_eq!(
            config.cache_dir().as_deref(),
            Some(Path::new("/var/cache/snouty"))
        );
    }

    #[test]
    fn temp_dir_resolves_from_env_var() {
        let env = TestEnv::new();
        env.set(TEMP_DIR_VAR_NAME, "/var/tmp/snouty");

        let config = default_config(None);
        assert_eq!(
            config.temp_dir().as_deref(),
            Some(Path::new("/var/tmp/snouty"))
        );
    }
}
