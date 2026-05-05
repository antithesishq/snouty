use std::collections::HashSet;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

use chrono::Utc;
use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};
use tokio::process::Child;

use crate::config::ComposeConfig;

/// RAII wrapper around a [`Child`] spawned with `process_group(0)`.
///
/// Ensures the entire process group is killed on drop, not just the leader.
/// The inner child is `Option<Child>` so `Drop` can handle partially-consumed state.
pub struct ProcessGroupChild {
    inner: Option<Child>,
}

impl ProcessGroupChild {
    /// Wrap a freshly-spawned child that was created with `process_group(0)`.
    pub fn new(child: Child) -> Self {
        Self { inner: Some(child) }
    }

    /// Send `SIGKILL` to the entire process group, then reap the child.
    pub async fn kill_group(&mut self) -> std::io::Result<()> {
        if let Some(ref mut child) = self.inner {
            if let Some(pid) = child.id() {
                // Safety: negative PID targets the entire process group.
                unsafe {
                    libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
                }
            }
            child.wait().await?;
        }
        Ok(())
    }

    /// Delegate to the inner [`Child::wait()`].
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.inner
            .as_mut()
            .expect("ProcessGroupChild already consumed")
            .wait()
            .await
    }

    /// Delegate to the inner [`Child::id()`].
    pub fn id(&self) -> Option<u32> {
        self.inner.as_ref().and_then(|c| c.id())
    }
}

impl Drop for ProcessGroupChild {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.inner {
            if let Some(pid) = child.id() {
                // Safety: best-effort cleanup of the process group.
                unsafe {
                    libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
                }
            }
            // Best-effort synchronous reap — we can't .await in Drop.
            let _ = child.try_wait();
        }
    }
}

/// Build the `-f` flags for `compose` subcommands: always
/// `-f docker-compose.yaml`, plus `-f <overlay>` if an overlay was provided.
fn compose_file_args(overlay: Option<&Path>) -> Vec<String> {
    let mut args = vec!["-f".to_string(), "docker-compose.yaml".to_string()];
    if let Some(path) = overlay {
        args.push("-f".to_string());
        args.push(path.display().to_string());
    }
    args
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

    /// Return whether the image is available in the local image store.
    fn image_exists(&self, image_ref: &str) -> Result<bool> {
        let runtime = self.name();
        let output = Command::new(runtime)
            .args(["image", "inspect", image_ref])
            .output()
            .wrap_err(format!("failed to run '{runtime} image inspect'"))?;
        image_exists_from_inspect_output(runtime, image_ref, output)
    }

    /// Return the local image architecture (for example `amd64` or `arm64`).
    fn image_architecture(&self, image_ref: &str) -> Result<String> {
        let runtime = self.name();
        let output = Command::new(runtime)
            .args([
                "image",
                "inspect",
                "--format",
                "{{.Architecture}}",
                image_ref,
            ])
            .output()
            .wrap_err(format!("failed to run '{runtime} image inspect'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'{runtime} image inspect {image_ref}' failed"))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let architecture = std::str::from_utf8(&output.stdout)
            .wrap_err("failed to parse image inspect output")?
            .trim();

        if architecture.is_empty() {
            bail!("empty image inspect output");
        }

        Ok(architecture.to_string())
    }

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
    ///
    /// When `platform` is `Some`, passes `--platform <platform>` to the build.
    fn build_image(
        &self,
        dir: &Path,
        image_ref: &str,
        dockerfile: Option<&Path>,
        platform: Option<&str>,
    ) -> Result<()> {
        let runtime = self.name();
        let scratch = dockerfile.is_none();

        let mut cmd = Command::new(runtime);
        cmd.args(["build", "-t", image_ref]);
        if let Some(platform) = platform {
            cmd.args(["--platform", platform]);
        }
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

        if scratch && let Some(mut stdin) = child.stdin.take() {
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

    /// Build a scratch image from the contents of `config_dir` and push it.
    /// Callers are responsible for validating the directory beforehand
    /// (typically via [`crate::config::detect_config`]). Returns the pinned
    /// image reference.
    fn build_and_push_config_image(&self, config_dir: &Path, image_ref: &str) -> Result<String> {
        eprintln!("Building config image: {}", image_ref);
        self.build_image(config_dir, image_ref, None, None)?;

        eprintln!("Pushing config image: {}", image_ref);
        let pinned = self.image_push(image_ref)?;
        eprintln!("Config image pushed successfully: {pinned}");
        Ok(pinned)
    }

    /// Push compose images that match the registry.
    /// Returns the pinned image reference for each pushed image.
    fn push_compose_images(&self, config: &ComposeConfig, registry: &str) -> Result<Vec<String>> {
        let compose = self.compose();
        let contents = compose.contents(config, None)?;
        let registry_trimmed = registry.trim_end_matches('/');
        let prefix = format!("{registry_trimmed}/");

        // Phase 1: Build the image list. Local build images get tagged with
        // the registry prefix so they become pushable.
        let mut tagged = HashSet::new();
        let mut images = Vec::new();
        for service in &contents.services {
            if contents.build_services.contains(&service.name)
                && is_local_image(&service.image)
                && !service.image.starts_with(&prefix)
            {
                let dest = format!("{prefix}{}", service.image);
                if tagged.insert(service.image.clone()) {
                    self.image_tag(&service.image, &dest)?;
                }
                images.push(dest);
            } else {
                images.push(service.image.clone());
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
/// Implementations customize behavior via hook methods (`up_extra_args`,
/// `logs_extra_args`, `ps_extra_args`). The default method implementations
/// build commands using `self.runtime().command()`.
pub trait Compose: Send + Sync {
    /// Access the underlying container runtime.
    fn runtime(&self) -> &dyn ContainerRuntime;

    // --- customization hooks (override per-backend) ---

    /// Extra arguments for `compose up`.
    /// Returns `(pre_args, post_args)` — pre_args go before the `up` subcommand,
    /// post_args go after it.
    fn up_extra_args(&self) -> (&[&str], &[&str]) {
        (&[], &[])
    }

    /// Extra arguments for `compose logs`.
    /// Returns `(pre_args, post_args)` — pre_args go before the `logs` subcommand,
    /// post_args go after it.
    fn logs_extra_args(&self) -> (&[&str], &[&str]) {
        (&[], &[])
    }

    /// Arguments to get JSON output from `compose ps`.
    /// Returns `(pre_args, post_args)` — pre_args go before the `ps` subcommand,
    /// post_args go after it.
    fn ps_extra_args(&self) -> (&[&str], &[&str]) {
        (&[], &["--format", "json"])
    }

    // --- default implementations ---

    /// Run `compose config` to resolve the compose file with env substitutions,
    /// returning the resolved YAML as a string.
    fn raw_contents(&self, config: &ComposeConfig, overlay: Option<&Path>) -> Result<String> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().command(&["compose"]);
        cmd.env("COMPOSE_PROJECT_NAME", "snouty");
        cmd.current_dir(config.dir());
        cmd.args(compose_file_args(overlay));
        cmd.arg("config");
        let output = cmd
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

    /// Resolve and parse a compose config into structured contents.
    fn contents(&self, config: &ComposeConfig, overlay: Option<&Path>) -> Result<ComposeContents> {
        let yaml = self.raw_contents(config, overlay)?;
        parse_compose_config(&yaml)
    }

    /// Parse `compose ps --format json` to get `(service_name, container_id)` pairs.
    fn ps(&self, config: &ComposeConfig, overlay: Option<&Path>) -> Result<Vec<(String, String)>> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().command(&["compose"]);
        cmd.current_dir(config.dir());
        let (pre, post) = self.ps_extra_args();
        cmd.args(compose_file_args(overlay));
        cmd.args(pre);
        cmd.args(["ps"]);
        cmd.args(post);

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
        overlay: Option<&Path>,
        service: &str,
        workdir: Option<&str>,
        env: &[(&str, &str)],
        cmd: &[&str],
    ) -> Result<std::process::Output> {
        let runtime = self.runtime().name();
        let mut command = self.runtime().command(&["compose"]);
        command.current_dir(config.dir());
        command.args(compose_file_args(overlay));
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

    /// Spawn `compose up --detach` and return the child process.
    ///
    /// stdout and stderr are inherited so progress is visible during pulls.
    /// The caller is responsible for awaiting the child and checking its exit
    /// status. Uses `process_group(0)` so the whole group can be killed on
    /// timeout.
    fn up_detached(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<ProcessGroupChild> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().tokio_command(&["compose"]);
        cmd.current_dir(config.dir());
        let (pre, post) = self.up_extra_args();
        cmd.args(compose_file_args(overlay));
        cmd.args(pre);
        cmd.args(["up", "--detach", "--no-build"]);
        cmd.args(post);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);

        cmd.spawn()
            .map(ProcessGroupChild::new)
            .wrap_err_with(|| format!("failed to start '{runtime} compose up --detach'"))
    }

    /// Spawn `compose logs --follow` and return the child process.
    ///
    /// stdout and stderr are inherited so compose log output goes straight
    /// to the terminal. stdin is null. The process exits when all
    /// containers stop.
    fn logs_follow(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<ProcessGroupChild> {
        let runtime = self.runtime().name();
        let mut cmd = self.runtime().tokio_command(&["compose"]);
        cmd.current_dir(config.dir());
        let (pre, post) = self.logs_extra_args();
        cmd.args(compose_file_args(overlay));
        cmd.args(pre);
        cmd.args(["logs", "--follow"]);
        cmd.args(post);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);

        cmd.spawn()
            .map(ProcessGroupChild::new)
            .wrap_err_with(|| format!("failed to start '{runtime} compose logs --follow'"))
    }

    /// Run `compose down` for cleanup. Best-effort, ignores errors.
    fn down(&self, config: &ComposeConfig, overlay: Option<&Path>) {
        let mut cmd = self.runtime().command(&["compose"]);
        cmd.current_dir(config.dir());
        cmd.args(compose_file_args(overlay));
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

    fn up_extra_args(&self) -> (&[&str], &[&str]) {
        (&[], &["--pull=never"])
    }
}

struct PodmanCompose<'a> {
    rt: &'a dyn ContainerRuntime,
}

impl Compose for PodmanCompose<'_> {
    fn runtime(&self) -> &dyn ContainerRuntime {
        self.rt
    }

    fn up_extra_args(&self) -> (&[&str], &[&str]) {
        (&["--podman-run-args=--pull=never"], &[])
    }

    fn logs_extra_args(&self) -> (&[&str], &[&str]) {
        (&[], &["--names"])
    }

    fn ps_extra_args(&self) -> (&[&str], &[&str]) {
        (&["--podman-args=--format=json"], &[])
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeService {
    pub name: String,
    pub image: String,
}

/// Parsed contents of a compose config file.
#[derive(Debug)]
pub struct ComposeContents {
    /// Services with an explicit `image:` value. Services without `image` are omitted.
    pub services: Vec<ComposeService>,
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
                    services.push(ComposeService {
                        name: name_str.to_string(),
                        image: image.to_string(),
                    });
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

/// Ensure the referenced images are available in the local image store.
pub fn validate_images_are_available(
    runtime: &dyn ContainerRuntime,
    services: &[ComposeService],
) -> Result<()> {
    let mut seen = HashSet::new();
    let mut missing = Vec::new();

    for service in services {
        if !seen.insert(service.image.as_str()) {
            continue;
        }

        if !runtime.image_exists(&service.image)? {
            missing.push(service.image.clone());
        }
    }

    if missing.is_empty() {
        return Ok(());
    }

    let mut err = eyre!("some images are not available locally");
    for image in missing {
        err = err.with_note(|| format!("image: {image}"));
    }
    err = err.with_suggestion(|| "pull or build the missing images, then retry");
    Err(err)
}

/// Ensure the referenced images all use the amd64 architecture.
pub fn validate_image_architectures(
    runtime: &dyn ContainerRuntime,
    services: &[ComposeService],
) -> Result<()> {
    let mut seen = HashSet::new();
    let mut unsupported = Vec::new();

    for service in services {
        if !seen.insert(service.image.as_str()) {
            continue;
        }

        let arch = runtime.image_architecture(&service.image)?;
        if arch != "amd64" {
            unsupported.push(format!(
                "service '{}' uses image '{}' with architecture '{arch}'",
                service.name, service.image
            ));
        }
    }

    if unsupported.is_empty() {
        return Ok(());
    }

    let mut err = eyre!("Antithesis requires x86-64 (amd64) container images");
    for detail in unsupported {
        err = err.with_note(|| detail);
    }
    err = err.with_suggestion(|| "use x86-64 (amd64) images, then retry");
    Err(err)
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

fn image_exists_from_inspect_output(
    runtime: &str,
    image_ref: &str,
    output: std::process::Output,
) -> Result<bool> {
    if output.status.success() {
        return Ok(true);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    if image_inspect_reports_missing_image(&stderr) {
        return Ok(false);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(eyre!("'{runtime} image inspect {image_ref}' failed"))
        .with_section(move || stdout.trim().to_string().header("Stdout:"))
        .with_section(move || stderr.trim().to_string().header("Stderr:"))
}

fn image_inspect_reports_missing_image(stderr: &str) -> bool {
    let stderr = stderr.trim().to_ascii_lowercase();
    stderr.contains("no such image")
        || stderr.contains("no such object")
        || stderr.contains("image not known")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::{
        OCIRegistry, has_compose, require_runtimes, require_runtimes_with_compose, skip_or_fail,
    };
    use std::collections::BTreeMap;
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;

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
                ComposeService {
                    name: "app".to_string(),
                    image: "us-central1-docker.pkg.dev/proj/repo/app:v1".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "us-central1-docker.pkg.dev/proj/repo/sidecar:latest".to_string(),
                },
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

            let compose = rt.compose();
            let config = match crate::config::detect_config(dir.path()).unwrap() {
                crate::config::Config::Compose(c) => c,
                other => panic!("expected Compose, got {other:?}"),
            };
            let contents = compose.contents(&config, None).unwrap();
            let images: Vec<&str> = contents
                .services
                .iter()
                .map(|service| service.image.as_str())
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
    fn compose_contents_apply_overlays() {
        let runtimes = require_runtimes_with_compose();
        if runtimes.is_empty() {
            return;
        }

        for rt in &runtimes {
            eprintln!("testing with runtime: {}", rt.name());
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("docker-compose.yaml"),
                "\
services:
  app:
    image: base:latest
",
            )
            .unwrap();
            let overlay = dir.path().join("override.yaml");
            std::fs::write(
                &overlay,
                "\
services:
  app:
    image: overlay:latest
",
            )
            .unwrap();

            let compose = rt.compose();
            let config = match crate::config::detect_config(dir.path()).unwrap() {
                crate::config::Config::Compose(c) => c,
                other => panic!("expected Compose, got {other:?}"),
            };
            let yaml = compose.raw_contents(&config, Some(&overlay)).unwrap();
            let contents = compose.contents(&config, Some(&overlay)).unwrap();

            assert!(
                yaml.contains("overlay:latest"),
                "expected overlay image in resolved yaml for runtime {}: {yaml}",
                rt.name()
            );
            assert_eq!(contents.services.len(), 1);
            assert_eq!(contents.services[0].image, "overlay:latest");
        }
    }

    #[derive(Clone)]
    struct FakeRuntime {
        available_images: BTreeMap<String, bool>,
        architectures: BTreeMap<String, String>,
    }

    impl ContainerRuntime for FakeRuntime {
        fn name(&self) -> &str {
            "fake"
        }

        fn clone_box(&self) -> Box<dyn ContainerRuntime> {
            Box::new(self.clone())
        }

        fn compose(&self) -> Box<dyn Compose + '_> {
            panic!("compose() not used in this test");
        }

        fn image_push(&self, _image_ref: &str) -> Result<String> {
            panic!("image_push() not used in this test");
        }

        fn image_exists(&self, image_ref: &str) -> Result<bool> {
            Ok(*self.available_images.get(image_ref).unwrap_or(&false))
        }

        fn image_architecture(&self, image_ref: &str) -> Result<String> {
            self.architectures
                .get(image_ref)
                .cloned()
                .ok_or_else(|| eyre!("missing fake architecture for {image_ref}"))
        }
    }

    #[test]
    fn validate_image_architectures_accepts_amd64_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::new(),
            architectures: BTreeMap::from([
                ("app:latest".to_string(), "amd64".to_string()),
                ("sidecar:latest".to_string(), "amd64".to_string()),
            ]),
        };

        validate_image_architectures(
            &runtime,
            &[
                ComposeService {
                    name: "app".to_string(),
                    image: "app:latest".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "sidecar:latest".to_string(),
                },
            ],
        )
        .unwrap();
    }

    #[test]
    fn validate_image_architectures_rejects_non_amd64_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::new(),
            architectures: BTreeMap::from([
                ("app:latest".to_string(), "arm64".to_string()),
                ("sidecar:latest".to_string(), "amd64".to_string()),
            ]),
        };

        let err = validate_image_architectures(
            &runtime,
            &[
                ComposeService {
                    name: "app".to_string(),
                    image: "app:latest".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "sidecar:latest".to_string(),
                },
            ],
        )
        .unwrap_err();

        let msg = err.to_string();
        let debug = format!("{err:?}");
        assert!(
            msg.contains("x86-64 (amd64)"),
            "expected architecture guidance, got: {msg}"
        );
        assert!(
            debug.contains("service 'app' uses image 'app:latest' with architecture 'arm64'"),
            "expected offending image details, got: {debug}"
        );
    }

    #[test]
    fn validate_images_are_available_accepts_local_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::from([
                ("app:latest".to_string(), true),
                ("sidecar:latest".to_string(), true),
            ]),
            architectures: BTreeMap::new(),
        };

        validate_images_are_available(
            &runtime,
            &[
                ComposeService {
                    name: "app".to_string(),
                    image: "app:latest".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "sidecar:latest".to_string(),
                },
            ],
        )
        .unwrap();
    }

    #[test]
    fn validate_images_are_available_reports_all_missing_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::from([
                ("present:latest".to_string(), true),
                ("missing-a:latest".to_string(), false),
                ("missing-b:latest".to_string(), false),
            ]),
            architectures: BTreeMap::new(),
        };

        let err = validate_images_are_available(
            &runtime,
            &[
                ComposeService {
                    name: "present".to_string(),
                    image: "present:latest".to_string(),
                },
                ComposeService {
                    name: "app".to_string(),
                    image: "missing-a:latest".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "missing-b:latest".to_string(),
                },
            ],
        )
        .unwrap_err();

        let msg = err.to_string();
        let debug = format!("{err:?}");
        assert!(
            msg.contains("some images are not available locally"),
            "expected missing-image guidance, got: {msg}"
        );
        assert!(
            debug.contains("image: missing-a:latest"),
            "expected first missing image details, got: {debug}"
        );
        assert!(
            debug.contains("image: missing-b:latest"),
            "expected second missing image details, got: {debug}"
        );
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
    fn image_exists_from_inspect_output_accepts_success() {
        let result = image_exists_from_inspect_output(
            "docker",
            "app:latest",
            std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
        )
        .unwrap();

        assert!(result);
    }

    #[test]
    fn image_exists_from_inspect_output_reports_missing_image() {
        let result = image_exists_from_inspect_output(
            "docker",
            "missing:latest",
            std::process::Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"Error response from daemon: No such image: missing:latest".to_vec(),
            },
        )
        .unwrap();

        assert!(!result);
    }

    #[test]
    fn image_exists_from_inspect_output_surfaces_runtime_errors() {
        let err = image_exists_from_inspect_output(
            "docker",
            "app:latest",
            std::process::Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: b"Cannot connect to the Docker daemon at unix:///var/run/docker.sock"
                    .to_vec(),
            },
        )
        .unwrap_err();

        let debug = format!("{err:?}");
        assert!(
            debug.contains("Cannot connect to the Docker daemon"),
            "expected daemon error details, got: {debug}"
        );
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
                ComposeService {
                    name: "app".to_string(),
                    image: "myapp:latest".to_string(),
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "docker.io/library/nginx:latest".to_string(),
                },
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

    #[test]
    fn compose_file_args_no_overlay() {
        assert_eq!(
            compose_file_args(None),
            vec!["-f".to_string(), "docker-compose.yaml".to_string()]
        );
    }

    #[test]
    fn compose_file_args_with_overlay() {
        let overlay = PathBuf::from("/tmp/override.yaml");
        assert_eq!(
            compose_file_args(Some(&overlay)),
            vec![
                "-f".to_string(),
                "docker-compose.yaml".to_string(),
                "-f".to_string(),
                "/tmp/override.yaml".to_string(),
            ]
        );
    }
}
