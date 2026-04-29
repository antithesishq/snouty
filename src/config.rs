use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result, bail};

/// A docker-compose config directory.
///
/// Construct via [`detect_config`]; the type guarantees the directory contains
/// a `docker-compose.yaml` file.
#[derive(Debug)]
pub struct ComposeConfig {
    dir: PathBuf,
}

impl ComposeConfig {
    /// The compose config directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// A Kubernetes config directory: the parent directory containing a
/// `manifests/` subfolder, per the Antithesis k8s setup convention.
#[derive(Debug)]
pub struct KubernetesConfig {
    dir: PathBuf,
}

impl KubernetesConfig {
    /// The root config directory (parent of `manifests/`). This is what gets
    /// packaged into the config image, so `manifests/` ends up at `/manifests`.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The `manifests/` subdirectory containing Kubernetes manifest files.
    pub fn manifests_dir(&self) -> PathBuf {
        self.dir.join("manifests")
    }
}

/// A snouty config directory, either docker-compose or Kubernetes.
#[derive(Debug)]
pub enum Config {
    Compose(ComposeConfig),
    Kubernetes(KubernetesConfig),
}

impl Config {
    /// The root config directory.
    pub fn dir(&self) -> &Path {
        match self {
            Config::Compose(c) => c.dir(),
            Config::Kubernetes(c) => c.dir(),
        }
    }
}

/// Inspect a config directory and decide whether it is a docker-compose or
/// Kubernetes config.
///
/// - Contains `docker-compose.yaml` → [`Config::Compose`].
/// - Contains a `manifests/` subdirectory with at least one regular file →
///   [`Config::Kubernetes`].
/// - Both present → ambiguous (error).
/// - Neither present → error mentioning both.
/// - `docker-compose.yml` (lowercase `.yml`) → rename hint.
pub fn detect_config(config_dir: &Path) -> Result<Config> {
    if !config_dir.is_dir() {
        bail!("'{}' is not a directory", config_dir.display());
    }

    if config_dir.join("docker-compose.yml").is_file() {
        bail!(
            "directory '{}' contains docker-compose.yml, but Antithesis requires docker-compose.yaml (rename the file)",
            config_dir.display()
        );
    }

    let has_compose = config_dir.join("docker-compose.yaml").is_file();
    let manifests_dir = config_dir.join("manifests");
    let has_manifests_dir = manifests_dir.is_dir();

    if has_compose && has_manifests_dir {
        bail!(
            "directory '{}' contains both docker-compose.yaml and a manifests/ subdirectory; pick one",
            config_dir.display()
        );
    }

    if has_compose {
        return Ok(Config::Compose(ComposeConfig {
            dir: config_dir.to_path_buf(),
        }));
    }

    if has_manifests_dir {
        let has_files = std::fs::read_dir(&manifests_dir)
            .wrap_err_with(|| format!("failed to read {}", manifests_dir.display()))?
            .filter_map(Result::ok)
            .any(|e| e.file_type().is_ok_and(|t| t.is_file()));
        if !has_files {
            bail!(
                "directory '{}' contains an empty manifests/ subdirectory",
                config_dir.display()
            );
        }
        return Ok(Config::Kubernetes(KubernetesConfig {
            dir: config_dir.to_path_buf(),
        }));
    }

    bail!(
        "directory '{}' does not contain a docker-compose.yaml file or a manifests/ subdirectory",
        config_dir.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_config_compose() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yaml"), "services: {}\n").unwrap();
        match detect_config(dir.path()).unwrap() {
            Config::Compose(c) => assert_eq!(c.dir(), dir.path()),
            other => panic!("expected Compose, got {other:?}"),
        }
    }

    #[test]
    fn detect_config_kubernetes() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir(&manifests).unwrap();
        std::fs::write(manifests.join("ns.yaml"), "kind: Namespace\n").unwrap();
        match detect_config(dir.path()).unwrap() {
            Config::Kubernetes(c) => {
                assert_eq!(c.dir(), dir.path());
                assert_eq!(c.manifests_dir(), manifests);
            }
            other => panic!("expected Kubernetes, got {other:?}"),
        }
    }

    #[test]
    fn detect_config_both_is_ambiguous() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yaml"), "services: {}\n").unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir(&manifests).unwrap();
        std::fs::write(manifests.join("ns.yaml"), "kind: Namespace\n").unwrap();
        let err = detect_config(dir.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("both"), "unexpected error: {msg}");
        assert!(msg.contains("manifests/"), "unexpected error: {msg}");
    }

    #[test]
    fn detect_config_neither_mentions_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let err = detect_config(dir.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("docker-compose.yaml"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("manifests/"), "unexpected error: {msg}");
    }

    #[test]
    fn detect_config_empty_manifests_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("manifests")).unwrap();
        let err = detect_config(dir.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("empty manifests/"), "unexpected error: {msg}");
    }

    #[test]
    fn detect_config_lowercase_yml_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yml"), "services: {}\n").unwrap();
        let err = detect_config(dir.path()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("rename"), "unexpected error: {msg}");
    }

    #[test]
    fn detect_config_not_a_directory() {
        let err = detect_config(Path::new("/nonexistent/snouty/path")).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("not a directory"), "unexpected error: {msg}");
    }
}
