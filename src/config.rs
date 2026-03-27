use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result, bail};
use log::debug;
use serde::{Deserialize, Serialize};

const LOCAL_FILENAME: &str = ".snouty.yaml";
const GLOBAL_DIR: &str = "snouty";
const GLOBAL_FILENAME: &str = "config.yaml";

#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

pub struct MergedConfig {
    pub merged: Config,
    pub global_path: Option<PathBuf>,
    pub global: Config,
    pub local_path: Option<PathBuf>,
    pub local: Config,
}

impl Config {
    /// Merge two configs. The overlay's `Some` values override self.
    pub fn merge(self, overlay: Config) -> Config {
        Config {
            source: overlay.source.or(self.source),
        }
    }
}

/// Load and parse a YAML config file. Empty files produce a default config.
fn load_file(path: &Path) -> Result<Config> {
    let contents = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read config file: {}", path.display()))?;
    let contents = contents.trim();
    if contents.is_empty() {
        return Ok(Config::default());
    }
    serde_yaml::from_str(contents)
        .wrap_err_with(|| format!("invalid config file: {}", path.display()))
}

/// Walk up from `start` to the filesystem root looking for `.snouty.yaml`.
pub fn find_local_from(start: &Path) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        let candidate = dir.join(LOCAL_FILENAME);
        if candidate.is_file() {
            return Some(candidate);
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return None,
        }
    }
}

/// Return the path for the global config file.
pub fn global_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join(GLOBAL_DIR).join(GLOBAL_FILENAME))
}

/// Load the merged config from global + local files, using `start_dir` as the
/// root for local `.snouty.yaml` discovery.
pub fn load(start_dir: &Path) -> Result<MergedConfig> {
    let global_path = global_path();
    let global = match &global_path {
        Some(path) if path.is_file() => {
            debug!("loading global config: {}", path.display());
            load_file(path)?
        }
        Some(path) => {
            debug!("no global config at {}", path.display());
            Config::default()
        }
        None => {
            debug!("could not determine config directory");
            Config::default()
        }
    };

    let start_dir = std::fs::canonicalize(start_dir).wrap_err_with(|| {
        format!(
            "failed to resolve config lookup root: {}",
            start_dir.display()
        )
    })?;
    let local_path = find_local_from(&start_dir);
    let local = match &local_path {
        Some(path) => {
            debug!("loading local config: {}", path.display());
            load_file(path)?
        }
        None => {
            debug!("no local config found");
            Config::default()
        }
    };

    let merged = global.clone().merge(local.clone());

    // Only report the paths of files that actually exist
    let global_path = global_path.filter(|p| p.is_file());

    Ok(MergedConfig {
        merged,
        global_path,
        global,
        local_path,
        local,
    })
}

/// Write a config file to the given path.
pub fn write_config(path: &Path, config: &Config) -> Result<()> {
    let yaml = serde_yaml::to_string(config).wrap_err("failed to serialize config")?;
    std::fs::write(path, yaml)
        .wrap_err_with(|| format!("failed to write config file: {}", path.display()))
}

/// Create a new `.snouty.yaml` in the current directory.
pub fn init(source: String) -> Result<()> {
    let path = std::env::current_dir()?.join(LOCAL_FILENAME);
    if path.exists() {
        bail!("{} already exists", path.display());
    }
    let config = Config {
        source: Some(source),
    };
    write_config(&path, &config)?;
    eprintln!("Created {}", path.display());
    Ok(())
}

/// Display the merged config with provenance information.
pub fn show() -> Result<()> {
    let cwd = std::env::current_dir()?;
    let mc = load(&cwd)?;

    // Global
    match &mc.global_path {
        Some(path) => {
            eprintln!("global\t{}", path.display());
        }
        None => {
            eprintln!("global\t(not found)");
        }
    }

    // Local
    match &mc.local_path {
        Some(path) => {
            eprintln!("local\t{}", path.display());
        }
        None => {
            eprintln!("local\t(not found)");
        }
    }

    print_config(&mc.merged);

    Ok(())
}

fn print_config(config: &Config) {
    if config.source.is_none() {
        println!("  (empty)");
        return;
    }
    if let Some(source) = &config.source {
        println!("  source: {source}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn parse_valid_config() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".snouty.yaml");
        std::fs::write(&path, "source: my-branch\n").unwrap();
        let config = load_file(&path).unwrap();
        assert_eq!(config.source, Some("my-branch".to_string()));
    }

    #[test]
    fn parse_empty_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".snouty.yaml");
        std::fs::write(&path, "").unwrap();
        let config = load_file(&path).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn parse_whitespace_only_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".snouty.yaml");
        std::fs::write(&path, "  \n  \n").unwrap();
        let config = load_file(&path).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn reject_unknown_fields() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".snouty.yaml");
        std::fs::write(&path, "source: x\nunknown_field: y\n").unwrap();
        let err = load_file(&path).unwrap_err();
        assert!(
            err.to_string().contains("invalid config file"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merge_overlay_wins() {
        let base = Config {
            source: Some("base".to_string()),
        };
        let overlay = Config {
            source: Some("overlay".to_string()),
        };
        let merged = base.merge(overlay);
        assert_eq!(merged.source, Some("overlay".to_string()));
    }

    #[test]
    fn merge_none_does_not_override() {
        let base = Config {
            source: Some("base".to_string()),
        };
        let overlay = Config { source: None };
        let merged = base.merge(overlay);
        assert_eq!(merged.source, Some("base".to_string()));
    }

    #[test]
    fn find_local_in_current_dir() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join(".snouty.yaml");
        std::fs::write(&config_path, "source: here\n").unwrap();
        let found = find_local_from(dir.path());
        assert_eq!(found, Some(config_path));
    }

    #[test]
    fn find_local_walks_up() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join(".snouty.yaml");
        std::fs::write(&config_path, "source: parent\n").unwrap();
        let child = dir.path().join("subdir");
        std::fs::create_dir(&child).unwrap();
        let found = find_local_from(&child);
        assert_eq!(found, Some(config_path));
    }

    #[test]
    fn find_local_stops_at_first_match() {
        let dir = TempDir::new().unwrap();
        // Config in parent
        std::fs::write(dir.path().join(".snouty.yaml"), "source: parent\n").unwrap();
        // Config in child
        let child = dir.path().join("subdir");
        std::fs::create_dir(&child).unwrap();
        let child_config = child.join(".snouty.yaml");
        std::fs::write(&child_config, "source: child\n").unwrap();

        let found = find_local_from(&child);
        assert_eq!(found, Some(child_config));
    }

    #[test]
    fn find_local_returns_none_when_missing() {
        let dir = TempDir::new().unwrap();
        let found = find_local_from(dir.path());
        assert!(found.is_none());
    }
}
