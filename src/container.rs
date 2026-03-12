use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use chrono::Utc;
use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};

/// Trait representing a container runtime (podman or docker).
pub trait ContainerRuntime: Send + Sync {
    /// The CLI command name (e.g. "podman" or "docker").
    fn name(&self) -> &str;

    /// Clone into a boxed trait object.
    fn clone_box(&self) -> Box<dyn ContainerRuntime>;

    /// Push the image to the registry, returning the pinned image reference
    /// (e.g. `example.com/foo/image@sha256:...`).
    fn image_push(&self, image_ref: &str) -> Result<String>;

    /// Build a scratch image containing the directory contents.
    fn build_image(&self, config_dir: &Path, image_ref: &str) -> Result<()> {
        let runtime = self.name();
        let mut child = runtime_command(runtime)
            .args(["build", "-t", image_ref, "-f", "-", "."])
            .current_dir(config_dir)
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .wrap_err(format!("failed to start '{runtime} build'"))?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(b"FROM scratch\nCOPY . /\n")
                .wrap_err("failed to write Dockerfile to stdin")?;
        }

        let output = child
            .wait_with_output()
            .wrap_err(format!("failed to wait for '{runtime} build'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'{runtime} build' failed"))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        Ok(())
    }

    /// Build and push a config image from a local directory.
    /// The directory must contain a `docker-compose.yaml` file.
    /// Returns the pinned image reference.
    fn build_and_push_config_image(&self, config_dir: &Path, image_ref: &str) -> Result<String> {
        let runtime = self.name();
        validate_compose_file(runtime, config_dir)?;

        eprintln!("Building config image: {}", image_ref);
        self.build_image(config_dir, image_ref)?;

        eprintln!("Pushing config image: {}", image_ref);
        let pinned = self.image_push(image_ref)?;
        eprintln!("Config image pushed successfully: {pinned}");
        Ok(pinned)
    }

    /// Push compose images that match the registry.
    /// Returns the pinned image reference for each pushed image.
    fn push_compose_images(&self, config_dir: &Path, registry: &str) -> Result<Vec<String>> {
        let runtime = self.name();
        let images = extract_image_refs(runtime, config_dir)?;
        let pushable = filter_pushable_images(&images, registry);

        let mut pinned = Vec::new();
        for image in pushable {
            eprintln!("Pushing image: {image}");
            let p = self.image_push(image)?;
            eprintln!("Image pushed: {p}");
            pinned.push(p);
        }

        Ok(pinned)
    }
}

#[derive(Clone)]
pub struct PodmanRuntime {
    cmd: String,
}

impl PodmanRuntime {
    pub(crate) fn new(cmd: impl Into<String>) -> Self {
        Self { cmd: cmd.into() }
    }
}

impl ContainerRuntime for PodmanRuntime {
    fn name(&self) -> &str {
        &self.cmd
    }

    fn clone_box(&self) -> Box<dyn ContainerRuntime> {
        Box::new(self.clone())
    }

    fn image_push(&self, image_ref: &str) -> Result<String> {
        let mut args = vec!["push"];

        // Podman requires --tls-verify=false for plain HTTP registries.
        if image_ref.starts_with("localhost") || image_ref.starts_with("127.0.0.1") {
            args.push("--tls-verify=false");
        }

        let digestfile =
            tempfile::NamedTempFile::new().wrap_err("failed to create temporary digest file")?;
        let digestfile_arg = format!("--digestfile={}", digestfile.path().display());
        args.push(&digestfile_arg);

        args.push(image_ref);

        let output = runtime_command(&self.cmd)
            .args(&args)
            .output()
            .wrap_err(format!("failed to run '{} push'", self.cmd))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'{} push' failed", self.cmd))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let digest = std::fs::read_to_string(digestfile.path())
            .wrap_err("failed to read digest file")?
            .trim()
            .to_string();
        Ok(pinned_image_ref(image_ref, &digest))
    }
}

#[derive(Clone)]
pub struct DockerRuntime {
    cmd: String,
}

impl DockerRuntime {
    pub(crate) fn new(cmd: impl Into<String>) -> Self {
        Self { cmd: cmd.into() }
    }
}

impl ContainerRuntime for DockerRuntime {
    fn clone_box(&self) -> Box<dyn ContainerRuntime> {
        Box::new(self.clone())
    }

    fn name(&self) -> &str {
        &self.cmd
    }

    fn image_push(&self, image_ref: &str) -> Result<String> {
        let output = runtime_command(&self.cmd)
            .args(["push", image_ref])
            .output()
            .wrap_err(format!("failed to run '{} push'", self.cmd))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'{} push' failed", self.cmd))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        // docker push prints "digest: sha256:... size: ..." on the last relevant line.
        let stdout = String::from_utf8_lossy(&output.stdout);
        let digest = parse_docker_push_digest(&stdout)?;
        Ok(pinned_image_ref(image_ref, &digest))
    }
}

/// Check that the directory exists and contains a docker-compose file.
pub fn validate_config_dir(config_dir: &Path) -> Result<()> {
    if !config_dir.is_dir() {
        bail!(
            "config directory error: '{}' is not a directory",
            config_dir.display()
        );
    }

    if config_dir.join("docker-compose.yml").is_file() {
        bail!(
            "config directory error: directory '{}' contains docker-compose.yml, but Antithesis requires docker-compose.yaml (rename the file)",
            config_dir.display()
        );
    }

    if !config_dir.join("docker-compose.yaml").is_file() {
        bail!(
            "config directory error: directory '{}' does not contain a docker-compose.yaml file",
            config_dir.display()
        );
    }

    Ok(())
}

/// Run `{runtime} compose config` to validate the compose file.
fn validate_compose_file(runtime: &str, config_dir: &Path) -> Result<()> {
    let output = runtime_command(runtime)
        // Keep compose validation independent of directory naming quirks.
        .env("COMPOSE_PROJECT_NAME", "snouty")
        .args(["compose", "config", "--quiet"])
        .current_dir(config_dir)
        .output()
        .wrap_err(format!("failed to run '{runtime} compose config'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("docker-compose file validation failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    Ok(())
}

/// Generate a unique image reference with a timestamp + random suffix tag.
pub fn generate_image_ref(registry: &str) -> String {
    let ts = Utc::now().format("%Y%m%d-%H%M%S");
    let mut buf = [0u8; 2];
    getrandom::fill(&mut buf).expect("failed to get random bytes");
    let suffix = format!("{:02x}{:02x}", buf[0], buf[1]);
    format!(
        "{}/snouty-config:{}-{}",
        registry.trim_end_matches('/'),
        ts,
        suffix
    )
}

/// Check whether a binary is genuinely docker or podman-in-disguise.
/// `docker version` (the subcommand) prints "Podman Engine" in the Client field
/// when docker is actually podman, while `docker --version` does not.
pub(crate) fn is_podman_in_disguise(cmd: &str) -> bool {
    runtime_command(cmd)
        .arg("version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|v| v.to_lowercase().contains("podman"))
}

/// Return the auto-detected global container runtime, preferring podman over docker.
///
/// The result is cached so detection only runs once.
///
/// Set `SNOUTY_CONTAINER_ENGINE=podman` or `=docker` to force a specific runtime.
pub fn runtime() -> Result<&'static dyn ContainerRuntime> {
    static INSTANCE: OnceLock<Result<Box<dyn ContainerRuntime>, String>> = OnceLock::new();

    INSTANCE
        .get_or_init(|| {
            if let Ok(engine) = std::env::var("SNOUTY_CONTAINER_ENGINE") {
                return match engine.as_str() {
                    "podman" => Ok(Box::new(PodmanRuntime::new("podman"))),
                    "docker" => Ok(Box::new(DockerRuntime::new("docker"))),
                    other => Err(format!(
                        "SNOUTY_CONTAINER_ENGINE={other}: expected 'podman' or 'docker'"
                    )),
                };
            }

            // Try podman first
            match runtime_command("podman").arg("--version").output() {
                Ok(output) if output.status.success() => {
                    return Ok(Box::new(PodmanRuntime::new("podman")));
                }
                Ok(_) => {} // podman found but failed, try docker
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // not installed
                Err(e) => return Err(format!("failed to check podman: {e}")),
            }

            // Fall back to docker
            match runtime_command("docker").arg("--version").output() {
                Ok(output) if output.status.success() => {
                    if is_podman_in_disguise("docker") {
                        log::warn!("podman not found as 'podman', but 'docker' is podman");
                        return Ok(Box::new(PodmanRuntime::new("docker")));
                    }
                    log::warn!("podman not found, falling back to docker");
                    Ok(Box::new(DockerRuntime::new("docker")))
                }
                Ok(_) => Err(
                    "'docker --version' failed; unable to find working container runtime"
                        .to_string(),
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err("neither podman nor docker is installed".to_string())
                }
                Err(e) => Err(format!("failed to check docker: {e}")),
            }
        })
        .as_ref()
        .map(|b| b.as_ref())
        .map_err(|e| eyre!("{e}"))
}

/// Return all container runtimes available on this machine.
/// Skips `docker` if it is actually podman in disguise.
pub fn available_engines() -> Vec<Box<dyn ContainerRuntime>> {
    let mut engines: Vec<Box<dyn ContainerRuntime>> = Vec::new();
    if Command::new("podman")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        engines.push(Box::new(PodmanRuntime::new("podman")));
    }
    if Command::new("docker")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
        && !is_podman_in_disguise("docker")
    {
        engines.push(Box::new(DockerRuntime::new("docker")));
    }
    engines
}

/// Build a pinned image reference (`name@digest`) from a tagged ref and a digest.
/// Strips the tag (`:tag`) if present, keeping the repository name.
fn pinned_image_ref(image_ref: &str, digest: &str) -> String {
    if let Some(at) = image_ref.rfind('@') {
        return format!("{}@{}", &image_ref[..at], digest);
    }

    // A colon is a tag separator only if it appears after the last `/`.
    // Any colon before or without a `/` is a host:port separator.
    let name = match image_ref.rfind('/') {
        Some(slash) => match image_ref[slash..].rfind(':') {
            Some(offset) => &image_ref[..slash + offset],
            None => image_ref,
        },
        None => match image_ref.rfind(':') {
            Some(colon) => &image_ref[..colon],
            None => image_ref,
        },
    };
    format!("{name}@{digest}")
}

/// Parse the image digest from `docker push` stdout.
///
/// Docker prints a line like: `latest: digest: sha256:abc123... size: 1234`
fn parse_docker_push_digest(stdout: &str) -> Result<String> {
    let mut found: Option<String> = None;
    for line in stdout.lines() {
        if let Some(rest) = line.find("digest: ").map(|i| &line[i + 8..]) {
            let digest = rest.split_whitespace().next().unwrap_or("");
            if digest.starts_with("sha256:") {
                if found.is_some() {
                    return Err(eyre!(
                        "ambiguous: multiple digests found in 'docker push' output"
                    ))
                    .with_section(|| stdout.trim().to_string().header("Stdout:"));
                }
                found = Some(digest.to_string());
            }
        }
    }
    found.ok_or_else(|| {
        eyre!("failed to parse digest from 'docker push' output")
            .section(stdout.trim().to_string().header("Stdout:"))
    })
}

/// Extract image references from the docker-compose.yaml in the given config directory.
/// Uses `{runtime} compose config` to resolve env variable substitutions.
fn extract_image_refs(runtime: &str, config_dir: &Path) -> Result<Vec<String>> {
    let output = runtime_command(runtime)
        // Keep image extraction independent of directory naming quirks.
        .env("COMPOSE_PROJECT_NAME", "snouty")
        .args(["compose", "config"])
        .current_dir(config_dir)
        .output()
        .wrap_err(format!("failed to run '{runtime} compose config'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("'{runtime} compose config' failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    let contents = String::from_utf8_lossy(&output.stdout);
    parse_compose_images(&contents)
}

/// Parse image references from resolved compose config YAML.
fn parse_compose_images(yaml: &str) -> Result<Vec<String>> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).wrap_err("failed to parse docker-compose.yaml")?;

    let mut images = Vec::new();
    if let Some(services) = doc.get("services").and_then(|s| s.as_mapping()) {
        for (_name, service) in services {
            if let Some(image) = service.get("image").and_then(|i| i.as_str()) {
                let image = image.to_string();
                if !images.contains(&image) {
                    images.push(image);
                }
            }
        }
    }

    Ok(images)
}

/// Filter images to only those that should be pushed: images whose name
/// starts with the given registry prefix. Bare images (no `/`) are skipped.
fn filter_pushable_images<'a>(images: &'a [String], registry: &str) -> Vec<&'a str> {
    let registry = registry.trim_end_matches('/');
    let prefix = format!("{registry}/");
    images
        .iter()
        .filter(|img| img.starts_with(&prefix))
        .map(|s| s.as_str())
        .collect()
}

fn runtime_command(cmd: &str) -> Command {
    Command::new(resolve_runtime_command(cmd))
}

#[cfg(not(windows))]
fn resolve_runtime_command(cmd: &str) -> PathBuf {
    PathBuf::from(cmd)
}

#[cfg(windows)]
fn resolve_runtime_command(cmd: &str) -> PathBuf {
    let path = Path::new(cmd);
    if path.components().count() > 1 || path.extension().is_some() {
        return path.to_path_buf();
    }

    find_on_path(path).unwrap_or_else(|| path.to_path_buf())
}

#[cfg(windows)]
fn find_on_path(cmd: &Path) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let pathext = std::env::var_os("PATHEXT")
        .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".into())
        .to_string_lossy()
        .split(';')
        .filter(|ext| !ext.is_empty())
        .map(|ext| ext.to_ascii_lowercase())
        .collect::<Vec<_>>();

    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(cmd);
        if candidate.is_file() {
            return Some(candidate);
        }

        for ext in &pathext {
            let candidate = dir.join(format!("{}{}", cmd.display(), ext));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::{
        OCIRegistry, has_compose, require_runtimes, require_runtimes_with_compose, skip_or_fail,
    };

    #[tokio::test]
    async fn build_and_push_to_mock_registry() {
        let runtimes = require_runtimes();
        if runtimes.is_empty() {
            return;
        }

        for rt in &runtimes {
            eprintln!("testing with runtime: {}", rt.name());
            if !has_compose(rt.name()) {
                skip_or_fail(&format!("{}: no compose support", rt.name()));
                continue;
            }
            let registry = match OCIRegistry::start(rt.as_ref()) {
                Some(r) => r,
                None => continue,
            };
            let addr = registry.host_port();

            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("docker-compose.yaml"),
                "services:\n  app:\n    image: test:latest\n",
            )
            .unwrap();

            let image_ref = format!("{addr}/test/snouty-config:test");
            rt.build_and_push_config_image(dir.path(), &image_ref)
                .unwrap_or_else(|e| panic!("{}: {e:?}", rt.name()));

            // Clean up the local image.
            let _ = Command::new(rt.name()).args(["rmi", &image_ref]).output();
        }
    }

    #[test]
    fn validate_config_dir_nonexistent() {
        let result = validate_config_dir(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not a directory"), "got: {err}");
    }

    #[test]
    fn generate_image_ref_format() {
        let image_ref = generate_image_ref("us-central1-docker.pkg.dev/proj/repo");
        assert!(
            image_ref.starts_with("us-central1-docker.pkg.dev/proj/repo/snouty-config:"),
            "got: {image_ref}"
        );
        // Tag should be YYYYMMDD-HHMMSS-XXXX format (20 chars)
        let tag = image_ref.rsplit(':').next().unwrap();
        assert!(
            tag.len() == 20 && tag.chars().nth(8) == Some('-') && tag.chars().nth(15) == Some('-'),
            "tag format should be YYYYMMDD-HHMMSS-XXXX, got: {tag}"
        );
    }

    #[test]
    fn generate_image_ref_strips_trailing_slash() {
        let image_ref = generate_image_ref("registry.example.com/repo/");
        assert!(
            image_ref.starts_with("registry.example.com/repo/snouty-config:"),
            "got: {image_ref}"
        );
    }

    #[test]
    fn parse_compose_images_basic() {
        let yaml = "\
services:
  app:
    image: us-central1-docker.pkg.dev/proj/repo/app:v1
  sidecar:
    image: us-central1-docker.pkg.dev/proj/repo/sidecar:latest
  builder:
    build:
      context: ./builder
";
        let refs = parse_compose_images(yaml).unwrap();
        assert_eq!(
            refs,
            vec![
                "us-central1-docker.pkg.dev/proj/repo/app:v1",
                "us-central1-docker.pkg.dev/proj/repo/sidecar:latest",
            ]
        );
    }

    #[test]
    fn parse_compose_images_deduplicates() {
        let yaml = "\
services:
  a:
    image: myimage:latest
  b:
    image: myimage:latest
";
        let refs = parse_compose_images(yaml).unwrap();
        assert_eq!(refs, vec!["myimage:latest"]);
    }

    #[test]
    fn parse_compose_images_no_services() {
        let yaml = "version: '3'\n";
        let refs = parse_compose_images(yaml).unwrap();
        assert!(refs.is_empty());
    }

    #[test]
    fn extract_image_refs_resolves_env() {
        let runtimes = require_runtimes_with_compose();
        if runtimes.is_empty() {
            return;
        }

        for rt in &runtimes {
            eprintln!("testing with runtime: {}", rt.name());
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join(".env"),
                "REPOSITORY=us-central1-docker.pkg.dev/proj/repo\nIMAGES_TAG=v2\n",
            )
            .unwrap();
            std::fs::write(
                dir.path().join("docker-compose.yaml"),
                "\
services:
  app:
    image: ${REPOSITORY}/app:${IMAGES_TAG}
  sidecar:
    image: docker.io/library/nginx:latest
",
            )
            .unwrap();

            let refs = extract_image_refs(rt.name(), dir.path()).unwrap();
            assert_eq!(
                refs,
                vec![
                    "us-central1-docker.pkg.dev/proj/repo/app:v2",
                    "docker.io/library/nginx:latest",
                ],
                "failed for runtime: {}",
                rt.name()
            );
        }
    }

    #[test]
    fn filter_pushable_images_matching_registry() {
        let images = vec![
            "us-central1-docker.pkg.dev/proj/repo/app:v1".to_string(),
            "ghcr.io/other/image:latest".to_string(),
            "myorg/foo:bar".to_string(),
            "app:latest".to_string(),
        ];
        let result = filter_pushable_images(&images, "us-central1-docker.pkg.dev/proj/repo");
        assert_eq!(result, vec!["us-central1-docker.pkg.dev/proj/repo/app:v1"]);
    }

    #[test]
    fn filter_pushable_images_trailing_slash() {
        let images = vec!["us-central1-docker.pkg.dev/proj/repo/app:v1".to_string()];
        let result = filter_pushable_images(&images, "us-central1-docker.pkg.dev/proj/repo/");
        assert_eq!(result, vec!["us-central1-docker.pkg.dev/proj/repo/app:v1"]);
    }

    #[test]
    fn filter_pushable_images_empty() {
        let images: Vec<String> = vec![];
        let result = filter_pushable_images(&images, "registry.example.com/repo");
        assert!(result.is_empty());
    }

    #[test]
    fn parse_docker_push_digest_typical() {
        let stdout = "\
The push refers to repository [registry.example.com/myimage]
5f70bf18a086: Layer already exists
latest: digest: sha256:abc123def456 size: 1234
";
        let digest = parse_docker_push_digest(stdout).unwrap();
        assert_eq!(digest, "sha256:abc123def456");
    }

    #[test]
    fn parse_docker_push_digest_no_digest() {
        let stdout = "The push refers to repository [registry.example.com/myimage]\n";
        let result = parse_docker_push_digest(stdout);
        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(err.contains("failed to parse digest"), "got: {err}");
    }

    #[test]
    fn parse_docker_push_digest_ambiguous() {
        let stdout = "\
tag1: digest: sha256:aaa111 size: 100
tag2: digest: sha256:bbb222 size: 200
";
        let result = parse_docker_push_digest(stdout);
        assert!(result.is_err());
        let err = format!("{:?}", result.unwrap_err());
        assert!(err.contains("ambiguous"), "got: {err}");
    }

    #[test]
    fn parse_docker_push_digest_empty() {
        let result = parse_docker_push_digest("");
        assert!(result.is_err());
    }

    #[test]
    fn pinned_image_ref_with_tag() {
        assert_eq!(
            pinned_image_ref("example.com/foo/image:v1", "sha256:abc123"),
            "example.com/foo/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_without_tag() {
        assert_eq!(
            pinned_image_ref("example.com/foo/image", "sha256:abc123"),
            "example.com/foo/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_with_port() {
        assert_eq!(
            pinned_image_ref("localhost:5000/image:latest", "sha256:abc123"),
            "localhost:5000/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_port_no_tag() {
        assert_eq!(
            pinned_image_ref("localhost:5000/image", "sha256:abc123"),
            "localhost:5000/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_host_port_nested_path() {
        assert_eq!(
            pinned_image_ref("myregistry:5000/org/repo/image:v2", "sha256:abc123"),
            "myregistry:5000/org/repo/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_host_port_nested_no_tag() {
        assert_eq!(
            pinned_image_ref("myregistry:5000/org/repo/image", "sha256:abc123"),
            "myregistry:5000/org/repo/image@sha256:abc123"
        );
    }

    #[test]
    fn pinned_image_ref_replaces_existing_digest() {
        assert_eq!(
            pinned_image_ref("registry.example.com/team/service@sha256:old", "sha256:new"),
            "registry.example.com/team/service@sha256:new"
        );
    }
}
