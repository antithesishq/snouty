use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use chrono::Utc;
use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};
use tokio::process::Child;

/// Bundles a compose config directory with optional overlay files (e.g. overrides).
///
/// Used by compose session operations (`up`, `ps`, `exec`,
/// `down`) that run against a live `compose up`.
#[derive(Debug)]
pub struct ComposeConfig {
    dir: PathBuf,
    extra_files: Vec<PathBuf>,
}

impl ComposeConfig {
    /// Validate and wrap a compose config directory.
    ///
    /// Checks that the directory exists and contains `docker-compose.yaml`.
    pub fn new(dir: PathBuf) -> Result<Self> {
        if !dir.is_dir() {
            bail!(
                "config directory error: '{}' is not a directory",
                dir.display()
            );
        }

        if dir.join("docker-compose.yml").is_file() {
            bail!(
                "config directory error: directory '{}' contains docker-compose.yml, but Antithesis requires docker-compose.yaml (rename the file)",
                dir.display()
            );
        }

        if !dir.join("docker-compose.yaml").is_file() {
            bail!(
                "config directory error: directory '{}' does not contain a docker-compose.yaml file",
                dir.display()
            );
        }

        Ok(Self {
            dir,
            extra_files: Vec::new(),
        })
    }

    /// Add an overlay file (e.g. a compose override) to the config.
    pub fn with_overlay(mut self, path: PathBuf) -> Self {
        self.extra_files.push(path);
        self
    }

    /// The compose config directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Return the `-f` flags for `compose` subcommands.
    ///
    /// Always includes `-f docker-compose.yaml`, followed by `-f <path>` for
    /// each overlay in `extra_files`.
    fn file_args(&self) -> Vec<String> {
        let mut args = vec!["-f".to_string(), "docker-compose.yaml".to_string()];
        for f in &self.extra_files {
            args.push("-f".to_string());
            args.push(f.display().to_string());
        }
        args
    }
}

/// Trait representing a container runtime (podman or docker).
pub trait ContainerRuntime: Send + Sync {
    /// The CLI command name (e.g. "podman" or "docker").
    fn name(&self) -> &str;

    /// Clone into a boxed trait object.
    fn clone_box(&self) -> Box<dyn ContainerRuntime>;

    /// Return a `Command` pre-configured with the runtime binary and given args.
    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(self.name());
        cmd.args(args);
        cmd
    }

    /// Like [`command`](Self::command) but returns a [`tokio::process::Command`].
    fn tokio_command(&self, args: &[&str]) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(self.name());
        cmd.args(args);
        cmd
    }

    /// Return a compose backend appropriate for this runtime.
    fn compose(&self) -> Box<dyn Compose + '_>;

    /// Push the image to the registry, returning the pinned image reference
    /// (e.g. `example.com/foo/image@sha256:...`).
    fn image_push(&self, image_ref: &str) -> Result<String>;

    /// Tag an image with a new reference.
    fn image_tag(&self, src: &str, dst: &str) -> Result<()> {
        let runtime = self.name();
        let output = Command::new(runtime)
            .args(["tag", src, dst])
            .output()
            .wrap_err(format!("failed to run '{runtime} tag'"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'{runtime} tag {src} {dst}' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }
        Ok(())
    }

    /// Build a container image from a directory.
    ///
    /// When `dockerfile` is `Some`, the given path is passed via `-f`.
    /// When `None`, a scratch image containing the directory contents is built
    /// via an implicit `FROM scratch\nCOPY . /\n` Dockerfile piped to stdin.
    fn build_image(&self, dir: &Path, image_ref: &str, dockerfile: Option<&Path>) -> Result<()> {
        let runtime = self.name();
        let scratch = dockerfile.is_none();

        let mut cmd = Command::new(runtime);
        cmd.args(["build", "-t", image_ref]);
        if let Some(df) = dockerfile {
            cmd.args(["-f", &df.display().to_string()]);
        } else {
            cmd.args(["-f", "-"]);
        }
        cmd.arg(".")
            .current_dir(dir)
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        if scratch {
            cmd.stdin(std::process::Stdio::piped());
        }

        let mut child = cmd
            .spawn()
            .wrap_err(format!("failed to start '{runtime} build'"))?;

        if scratch {
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(b"FROM scratch\nCOPY . /\n")
                    .wrap_err("failed to write Dockerfile to stdin")?;
            }
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
        self.build_image(config_dir, image_ref, None)?;

        eprintln!("Pushing config image: {}", image_ref);
        let pinned = self.image_push(image_ref)?;
        eprintln!("Config image pushed successfully: {pinned}");
        Ok(pinned)
    }

    /// Push compose images that match the registry.
    /// Returns the pinned image reference for each pushed image.
    fn push_compose_images(&self, config_dir: &Path, registry: &str) -> Result<Vec<String>> {
        let yaml = self.compose().config(config_dir)?;
        let contents = parse_compose_config(&yaml)?;
        let registry_trimmed = registry.trim_end_matches('/');
        let prefix = format!("{registry_trimmed}/");

        // Phase 1: Build the image list. Local build images get tagged with
        // the registry prefix so they become pushable.
        let mut tagged = HashSet::new();
        let mut images = Vec::new();
        for (name, image) in &contents.services {
            if contents.build_services.contains(name)
                && is_local_image(image)
                && !image.starts_with(&prefix)
            {
                let dest = format!("{prefix}{image}");
                if tagged.insert(image.clone()) {
                    self.image_tag(image, &dest)?;
                }
                images.push(dest);
            } else {
                images.push(image.clone());
            }
        }

        // Phase 2: Push images matching registry prefix (existing logic).
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

    /// Copy files from a container to the local filesystem.
    ///
    /// Runs `{runtime} cp {container_id}:{src} {dst}`.
    fn container_cp(&self, container_id: &str, src: &str, dst: &Path) -> Result<()> {
        let runtime = self.name();
        let src_arg = format!("{container_id}:{src}");
        let output = Command::new(runtime)
            .args(["cp", &src_arg, &dst.display().to_string()])
            .output()
            .wrap_err_with(|| format!("failed to run '{runtime} cp'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'{runtime} cp' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        Ok(())
    }
}

/// Compose backend abstraction.
///
/// Implementations customize behavior via hook methods (`extra_args`,
/// `up_extra_args`, `logs_extra_args`). The default method implementations
/// build commands using `self.runtime().command()`.
pub trait Compose: Send + Sync {
    /// Access the underlying container runtime.
    fn runtime(&self) -> &dyn ContainerRuntime;

    // --- customization hooks (override per-backend) ---

    /// Extra arguments inserted between file args and the subcommand.
    fn extra_args(&self) -> &[&str] {
        &[]
    }

    /// Extra arguments appended after `up --detach --no-build`.
    fn up_extra_args(&self) -> &[&str] {
        &[]
    }

    /// Extra arguments appended after `logs --follow`.
    fn logs_extra_args(&self) -> &[&str] {
        &[]
    }

    // --- default implementations ---

    /// Run `compose config` to resolve the compose file with env substitutions,
    /// returning the resolved YAML as a string.
    fn config(&self, config_dir: &Path) -> Result<String> {
        let runtime = self.runtime().name();
        let output = self
            .runtime()
            .command(&["compose", "-f", "docker-compose.yaml", "config"])
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

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Parse `compose ps --format json` to get `(service_name, container_id)` pairs.
    fn ps(&self, config: &ComposeConfig) -> Result<Vec<(String, String)>> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().command(&["compose"]);
        cmd.current_dir(&config.dir);
        cmd.args(config.file_args());
        cmd.args(["ps", "--format", "json"]);

        let output = cmd
            .output()
            .wrap_err_with(|| format!("failed to run '{runtime} compose ps'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'{runtime} compose ps' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_compose_ps(&stdout)
    }

    /// Run a command inside a running compose service container.
    ///
    /// Runs `{runtime} compose exec -T [--workdir workdir] {service} {cmd...}`.
    /// The `-T` flag disables TTY allocation for non-interactive use.
    /// If `workdir` is `Some`, sets the working directory inside the container.
    /// Stdout and stderr are captured in the returned `Output`.
    fn exec(
        &self,
        config: &ComposeConfig,
        service: &str,
        workdir: Option<&str>,
        env: &[(&str, &str)],
        cmd: &[&str],
    ) -> Result<std::process::Output> {
        let runtime = self.runtime().name();
        let mut command = self.runtime().command(&["compose"]);
        command.current_dir(&config.dir);
        command.args(config.file_args());
        command.args(["exec", "-T"]);
        for (k, v) in env {
            command.args(["-e", &format!("{k}={v}")]);
        }
        if let Some(w) = workdir {
            command.args(["--workdir", w]);
        }
        command.arg(service);
        command.args(cmd);

        command
            .output()
            .wrap_err_with(|| format!("failed to run '{runtime} compose exec'"))
    }

    /// Run `compose up --detach` to start services in detached mode.
    ///
    /// stdout and stderr are inherited so progress is visible during pulls.
    fn up_detached(&self, config: &ComposeConfig) -> Result<()> {
        let runtime = self.runtime().name();
        let status = self
            .runtime()
            .command(&["compose"])
            .current_dir(&config.dir)
            .args(config.file_args())
            .args(self.extra_args())
            .args(["up", "--detach", "--no-build"])
            .args(self.up_extra_args())
            .status()
            .wrap_err_with(|| format!("failed to run '{runtime} compose up --detach'"))?;

        if !status.success() {
            bail!("'{runtime} compose up --detach' failed (exit status: {status})");
        }
        Ok(())
    }

    /// Spawn `compose logs --follow` and return the child process.
    ///
    /// stdout and stderr are inherited so compose log output goes straight
    /// to the terminal. stdin is null. The process exits when all
    /// containers stop.
    fn logs_follow(&self, config: &ComposeConfig) -> Result<Child> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().tokio_command(&["compose"]);
        cmd.current_dir(&config.dir);
        cmd.args(config.file_args());
        cmd.args(["logs", "--follow"]);
        cmd.args(self.logs_extra_args());
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);

        cmd.spawn()
            .wrap_err_with(|| format!("failed to start '{runtime} compose logs --follow'"))
    }

    /// Run `compose down` for cleanup. Best-effort, ignores errors.
    fn down(&self, config: &ComposeConfig) {
        let mut cmd = self.runtime().command(&["compose"]);
        cmd.current_dir(&config.dir);
        cmd.args(config.file_args());
        cmd.args(["down", "--timeout", "0"]);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        let _ = cmd.status();
    }
}

struct DockerCompose<'a> {
    rt: &'a dyn ContainerRuntime,
}

impl Compose for DockerCompose<'_> {
    fn runtime(&self) -> &dyn ContainerRuntime {
        self.rt
    }

    fn up_extra_args(&self) -> &[&str] {
        &["--pull=never"]
    }
}

struct PodmanCompose<'a> {
    rt: &'a dyn ContainerRuntime,
}

impl Compose for PodmanCompose<'_> {
    fn runtime(&self) -> &dyn ContainerRuntime {
        self.rt
    }

    fn extra_args(&self) -> &[&str] {
        &["--podman-run-args=--pull=never"]
    }

    fn logs_extra_args(&self) -> &[&str] {
        &["--names"]
    }
}

/// Which compose implementation podman dispatches to.
#[derive(Clone, Copy)]
enum ComposeFlavor {
    /// Podman dispatches to `docker-compose` (standalone binary).
    DockerCompose,
    /// Podman dispatches to native `podman-compose`.
    PodmanCompose,
}

/// Parse `compose version` output to detect which compose backend is in use.
///
/// Returns `DockerCompose` when the output contains "docker compose"
/// (case-insensitive), otherwise returns `PodmanCompose` as the conservative
/// fallback.
fn parse_compose_version(output: &str) -> ComposeFlavor {
    if output.to_lowercase().contains("docker compose") {
        ComposeFlavor::DockerCompose
    } else {
        ComposeFlavor::PodmanCompose
    }
}

#[derive(Clone)]
pub struct PodmanRuntime {
    cmd: String,
    compose_flavor: ComposeFlavor,
}

impl PodmanRuntime {
    pub(crate) fn new(cmd: impl Into<String>) -> Self {
        let cmd = cmd.into();
        let compose_flavor = Command::new(&cmd)
            .args(["compose", "version"])
            .output()
            .ok()
            .and_then(|o| {
                if o.status.success() {
                    String::from_utf8(o.stdout).ok()
                } else {
                    None
                }
            })
            .map(|s| parse_compose_version(&s))
            .unwrap_or(ComposeFlavor::PodmanCompose);
        Self {
            cmd,
            compose_flavor,
        }
    }
}

impl ContainerRuntime for PodmanRuntime {
    fn name(&self) -> &str {
        &self.cmd
    }

    fn clone_box(&self) -> Box<dyn ContainerRuntime> {
        Box::new(self.clone())
    }

    fn compose(&self) -> Box<dyn Compose + '_> {
        match self.compose_flavor {
            ComposeFlavor::DockerCompose => Box::new(DockerCompose { rt: self }),
            ComposeFlavor::PodmanCompose => Box::new(PodmanCompose { rt: self }),
        }
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

        let output = Command::new(&self.cmd)
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

    fn compose(&self) -> Box<dyn Compose + '_> {
        Box::new(DockerCompose { rt: self })
    }

    fn image_push(&self, image_ref: &str) -> Result<String> {
        let output = Command::new(&self.cmd)
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

/// Run `{runtime} compose config` to validate the compose file.
fn validate_compose_file(runtime: &str, config_dir: &Path) -> Result<()> {
    let output = Command::new(runtime)
        // Keep compose validation independent of directory naming quirks.
        .env("COMPOSE_PROJECT_NAME", "snouty")
        .args(["compose", "-f", "docker-compose.yaml", "config"])
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
    Command::new(cmd)
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
            match Command::new("podman").arg("--version").output() {
                Ok(output) if output.status.success() => {
                    return Ok(Box::new(PodmanRuntime::new("podman")));
                }
                Ok(_) => {} // podman found but failed, try docker
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // not installed
                Err(e) => return Err(format!("failed to check podman: {e}")),
            }

            // Fall back to docker
            match Command::new("docker").arg("--version").output() {
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

/// Parsed contents of a compose config file.
#[derive(Debug)]
pub struct ComposeContents {
    /// `(service_name, image)` pairs. Services without `image` are omitted.
    pub services: Vec<(String, String)>,
    /// Service names that have a `build:` stanza.
    pub build_services: HashSet<String>,
    /// Explicitly declared network names (from the top-level `networks` key).
    pub networks: Vec<String>,
}

/// Parse services and networks from resolved compose config YAML.
/// Services without an `image` key are omitted.
pub fn parse_compose_config(yaml: &str) -> Result<ComposeContents> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).wrap_err("failed to parse docker-compose.yaml")?;

    let mut services = Vec::new();
    let mut build_services = HashSet::new();
    if let Some(svc_map) = doc.get("services").and_then(|s| s.as_mapping()) {
        for (name, service) in svc_map {
            if let Some(name_str) = name.as_str() {
                if service.get("build").is_some() {
                    build_services.insert(name_str.to_string());
                }
                if let Some(image) = service.get("image").and_then(|i| i.as_str()) {
                    services.push((name_str.to_string(), image.to_string()));
                }
            }
        }
    }

    let mut networks = Vec::new();
    if let Some(net_map) = doc.get("networks").and_then(|s| s.as_mapping()) {
        for (name, value) in net_map {
            if let Some(name) = name.as_str() {
                let is_external = value
                    .get("external")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_external {
                    bail!("network '{name}' is declared as external and won't work on Antithesis");
                }
                networks.push(name.to_string());
            }
        }
    }
    networks.sort();

    Ok(ComposeContents {
        services,
        build_services,
        networks,
    })
}

/// Filter images to only those that should be pushed: images whose name
/// starts with the given registry prefix. Bare images (no `/`) are skipped.
fn filter_pushable_images<'a>(images: &'a [String], registry: &str) -> Vec<&'a str> {
    let registry = registry.trim_end_matches('/');
    let prefix = format!("{registry}/");
    let mut seen = HashSet::new();
    images
        .iter()
        .filter(|img| img.starts_with(&prefix) && seen.insert(img.as_str()))
        .map(|s| s.as_str())
        .collect()
}

/// Check whether an image reference is "local" (not from a remote registry).
///
/// An image is local if its first path component has no dots, no colons, and
/// is not literally "localhost". Examples:
/// - `myapp:latest` -> local
/// - `org/myapp:latest` -> local
/// - `docker.io/library/nginx:latest` -> not local
/// - `localhost/myapp:latest` -> not local
/// - `registry:5000/myapp:latest` -> not local
fn is_local_image(image: &str) -> bool {
    match image.split_once('/') {
        None => true,
        Some((first, _)) => !(first.contains('.') || first.contains(':') || first == "localhost"),
    }
}

/// Parse the JSON output of `compose ps --format json`.
///
/// Handles both NDJSON (one object per line) and JSON array formats.
///
/// Two schemas are supported:
/// - **docker compose**: `{"Service": "...", "ID": "..."}`
/// - **podman-compose** (native podman ps): `{"Id": "...", "Labels": {"com.docker.compose.service": "..."}}`
fn parse_compose_ps(stdout: &str) -> Result<Vec<(String, String)>> {
    let stdout = stdout.trim();
    if stdout.is_empty() {
        return Ok(Vec::new());
    }

    let entries: Vec<serde_json::Value> = if stdout.starts_with('[') {
        serde_json::from_str(stdout).wrap_err("failed to parse compose ps JSON array")?
    } else {
        stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<std::result::Result<Vec<_>, _>>()
            .wrap_err("failed to parse compose ps NDJSON")?
    };

    entries
        .iter()
        .map(|v| {
            let id = v
                .get("ID")
                .or_else(|| v.get("Id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| eyre!("missing container ID in compose ps output"))?;

            let service = v
                .get("Service")
                .or_else(|| v.get("service"))
                .and_then(|v| v.as_str())
                .or_else(|| {
                    v.get("Labels")
                        .and_then(|l| l.get("com.docker.compose.service"))
                        .and_then(|s| s.as_str())
                })
                .ok_or_else(|| eyre!("missing service name in compose ps output"))?;

            Ok((service.to_string(), id.to_string()))
        })
        .collect()
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
    fn parse_compose_ps_ndjson() {
        let stdout = "{\"ID\":\"abc123\",\"Service\":\"app\",\"State\":\"running\"}\n\
                      {\"ID\":\"def456\",\"Service\":\"sidecar\",\"State\":\"running\"}\n";
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![
                ("app".to_string(), "abc123".to_string()),
                ("sidecar".to_string(), "def456".to_string()),
            ]
        );
    }

    #[test]
    fn parse_compose_ps_json_array() {
        let stdout = r#"[{"ID":"abc123","Service":"app"},{"ID":"def456","Service":"sidecar"}]"#;
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![
                ("app".to_string(), "abc123".to_string()),
                ("sidecar".to_string(), "def456".to_string()),
            ]
        );
    }

    #[test]
    fn parse_compose_ps_empty() {
        let result = parse_compose_ps("").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_compose_ps_podman_format() {
        let stdout = r#"[
            {
                "Id": "abc123",
                "Names": ["project-app-1"],
                "Labels": {"com.docker.compose.service": "app"}
            },
            {
                "Id": "def456",
                "Names": ["project-sidecar-1"],
                "Labels": {"com.docker.compose.service": "sidecar"}
            }
        ]"#;
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![
                ("app".to_string(), "abc123".to_string()),
                ("sidecar".to_string(), "def456".to_string()),
            ]
        );
    }

    #[test]
    fn compose_config_nonexistent() {
        let result = ComposeConfig::new(PathBuf::from("/nonexistent/path/that/does/not/exist"));
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
    fn parse_compose_config_basic() {
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
        let contents = parse_compose_config(yaml).unwrap();
        assert_eq!(
            contents.services,
            vec![
                (
                    "app".to_string(),
                    "us-central1-docker.pkg.dev/proj/repo/app:v1".to_string()
                ),
                (
                    "sidecar".to_string(),
                    "us-central1-docker.pkg.dev/proj/repo/sidecar:latest".to_string()
                ),
            ]
        );
        assert_eq!(
            contents.build_services,
            HashSet::from(["builder".to_string()])
        );
        assert!(contents.networks.is_empty());
    }

    #[test]
    fn parse_compose_config_no_services() {
        let yaml = "version: '3'\n";
        let contents = parse_compose_config(yaml).unwrap();
        assert!(contents.services.is_empty());
        assert!(contents.build_services.is_empty());
    }

    #[test]
    fn parse_compose_config_with_networks() {
        let yaml = "\
services:
  app:
    image: myapp:latest
networks:
  backend: {}
  frontend:
    driver: bridge
";
        let contents = parse_compose_config(yaml).unwrap();
        assert_eq!(contents.services.len(), 1);
        assert!(contents.build_services.is_empty());
        assert_eq!(contents.networks, vec!["backend", "frontend"]);
    }

    #[test]
    fn parse_compose_config_rejects_external_network() {
        let yaml = "\
services:
  app:
    image: myapp:latest
networks:
  shared_net:
    external: true
";
        let err = parse_compose_config(yaml).unwrap_err();
        assert!(
            err.to_string().contains("external"),
            "expected error about external network, got: {err}"
        );
    }

    #[test]
    fn compose_config_resolves_env() {
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

            let yaml = rt.compose().config(dir.path()).unwrap();
            let contents = parse_compose_config(&yaml).unwrap();
            let images: Vec<&str> = contents
                .services
                .iter()
                .map(|(_, img)| img.as_str())
                .collect();
            assert_eq!(
                images,
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
    fn filter_pushable_images_deduplicates_non_consecutive() {
        let images = vec![
            "registry.example.com/repo/app:v1".to_string(),
            "registry.example.com/repo/sidecar:latest".to_string(),
            "registry.example.com/repo/app:v1".to_string(),
        ];
        let result = filter_pushable_images(&images, "registry.example.com/repo");
        assert_eq!(
            result,
            vec![
                "registry.example.com/repo/app:v1",
                "registry.example.com/repo/sidecar:latest",
            ]
        );
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

    #[test]
    fn is_local_image_bare_name() {
        assert!(is_local_image("myapp:latest"));
    }

    #[test]
    fn is_local_image_org_name() {
        assert!(is_local_image("myorg/myapp:latest"));
    }

    #[test]
    fn is_local_image_registry_with_dot() {
        assert!(!is_local_image("registry.example.com/myapp:latest"));
    }

    #[test]
    fn is_local_image_docker_io() {
        assert!(!is_local_image("docker.io/library/nginx:latest"));
    }

    #[test]
    fn is_local_image_localhost() {
        assert!(!is_local_image("localhost/myapp:latest"));
    }

    #[test]
    fn is_local_image_localhost_port() {
        assert!(!is_local_image("localhost:5000/myapp:latest"));
    }

    #[test]
    fn is_local_image_host_port() {
        assert!(!is_local_image("myregistry:5000/myapp:latest"));
    }

    #[test]
    fn parse_compose_config_build_with_image() {
        let yaml = "\
services:
  app:
    build: .
    image: myapp:latest
  sidecar:
    image: docker.io/library/nginx:latest
";
        let contents = parse_compose_config(yaml).unwrap();
        assert_eq!(
            contents.services,
            vec![
                ("app".to_string(), "myapp:latest".to_string()),
                (
                    "sidecar".to_string(),
                    "docker.io/library/nginx:latest".to_string()
                ),
            ]
        );
        assert_eq!(contents.build_services, HashSet::from(["app".to_string()]));
    }

    #[test]
    fn parse_compose_version_docker() {
        assert!(matches!(
            parse_compose_version("Docker Compose version v2.24.5"),
            ComposeFlavor::DockerCompose
        ));
    }

    #[test]
    fn parse_compose_version_podman() {
        assert!(matches!(
            parse_compose_version("podman-compose version 1.0.6"),
            ComposeFlavor::PodmanCompose
        ));
    }

    #[test]
    fn parse_compose_version_unknown() {
        assert!(matches!(
            parse_compose_version(""),
            ComposeFlavor::PodmanCompose
        ));
    }
}
