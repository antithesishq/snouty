use std::collections::{BTreeMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};
use tokio::process::Child;

use crate::config::ComposeConfig;
use crate::settings::Settings;

/// Wall-clock budget for each synchronous docker/podman call made while
/// discovering test commands (`ps`, `exec test -d`, `cp`). Discovery runs on
/// the current-thread runtime between the two `validate` timeout windows, so a
/// wedged daemon or a flapping container could otherwise block it — and the
/// whole CLI — forever, with neither `--timeout` nor ctrl+c able to interrupt a
/// blocking `Command`. These calls are normally sub-second; the generous bound
/// only exists to convert an indefinite hang into a clear error.
const DISCOVERY_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Run a command to completion with a wall-clock timeout, killing it (and
/// returning an error) if it overruns. Reader threads drain stdout/stderr so a
/// chatty child can't deadlock on a full pipe while we wait. Used for the
/// synchronous discovery commands, which would otherwise be uninterruptible.
///
/// Deliberately kills only the leader process — not the process group — so it
/// needs no `libc::kill(-pid, …)` `unsafe`, unlike [`ProcessGroupChild`]. That
/// wrapper exists for the long-running `docker-compose up`/`logs` commands,
/// which fork and manage a tree of children that must all die on timeout. The
/// callers here are one-shot docker/podman *client* invocations (`cp`,
/// `exec test -d`, `ps`) whose only child is the client itself: killing it
/// closes the pipes (so the reader threads finish) and the work we were waiting
/// on ends. The daemon-side operation is intentionally not ours to kill.
fn output_with_timeout(mut cmd: Command, timeout: Duration) -> Result<Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().wrap_err("failed to spawn command")?;

    // Drain both pipes on their own threads; otherwise a child that fills a pipe
    // buffer would block on write while we block on wait — a deadlock.
    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stdout_reader = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        buf
    });
    let stderr_reader = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().wrap_err("failed to wait for command")? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            // The killed leader's pipe write-ends are now closed, so the reader
            // threads hit EOF and finish; join them rather than detaching them.
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(eyre!(
                "command timed out after {}s (the container runtime may be unresponsive)",
                timeout.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    };

    let stdout = stdout_reader.join().unwrap_or_default();
    let stderr = stderr_reader.join().unwrap_or_default();
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

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

    /// The underlying engine ("podman" or "docker"), independent of which binary
    /// invokes it. These diverge only for podman-in-disguise (podman installed
    /// as the `docker` binary), where [`name`](Self::name) is `"docker"` but the
    /// engine is really podman.
    fn engine_kind(&self) -> &'static str;

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

    /// The `DOCKER_HOST` value `docker-compose` should use to drive this
    /// runtime's container engine, or `None` to use docker-compose's default
    /// (the Docker daemon). Returns `Err` when the engine's API socket is
    /// required but unavailable. The default targets the Docker daemon.
    fn engine_docker_host(&self) -> Result<Option<String>> {
        Ok(None)
    }

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
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(eyre!(
                "'{runtime} image inspect {image_ref}' returned empty architecture"
            ))
            .with_section(move || stderr.header("Stderr:"));
        }

        Ok(architecture.to_string())
    }

    /// Read the `repo@sha256:...` digest references the local store associates
    /// with an image.
    ///
    /// Entries recorded by pulls are ground truth (the digest the registry
    /// actually served), but both podman and docker (containerd store) also
    /// synthesize entries for names an image was merely tagged with — so an
    /// entry does NOT prove the named registry serves the digest. Verify with
    /// [`remote_manifest`](Self::remote_manifest) before relying on one.
    fn image_repo_digests(&self, image_ref: &str) -> Result<Vec<String>> {
        let runtime = self.name();
        let output = Command::new(runtime)
            .args(["image", "inspect", image_ref])
            .output()
            .wrap_err_with(|| format!("failed to run '{runtime} image inspect'"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'{runtime} image inspect {image_ref}' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }
        let parsed: serde_json::Value = serde_json::from_slice(&output.stdout)
            .wrap_err_with(|| format!("failed to parse '{runtime} image inspect' output"))?;
        Ok(parsed
            .get(0)
            .and_then(|img| img.get("RepoDigests"))
            .and_then(|d| d.as_array())
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Ask the registry named by `image_ref` (typically `repo@sha256:...`)
    /// whether it serves that manifest — a manifest-only API round trip via
    /// `{runtime} manifest inspect`, using the runtime's own auth and TLS
    /// configuration. Never pulls or pushes anything.
    ///
    /// Every failure (missing manifest, auth, unreachable registry, or a
    /// runtime that can't inspect this manifest kind — podman errors on
    /// single-platform manifests) maps to [`RemoteManifest::NotFound`]:
    /// callers fall back to pushing, so degraded networks or runtime quirks
    /// can only cause extra uploads, never a bad pin.
    fn remote_manifest(&self, image_ref: &str) -> RemoteManifest;

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
    ///
    /// The image is always built for `linux/amd64` (x86-64) because Antithesis
    /// does not support arm64, and the host may well be an arm machine.
    fn build_and_push_config_image(&self, config_dir: &Path, image_ref: &str) -> Result<String> {
        eprintln!("Building config image: {}", image_ref);
        self.build_image(config_dir, image_ref, None, Some("linux/amd64"))?;

        eprintln!("Pushing config image: {}", image_ref);
        // image_push pins to `name:tag@digest`, but Antithesis's config-image
        // validator rejects a reference carrying both a :tag and an @digest
        // (a bug in the validator), so identify the image by digest alone.
        let pinned = digest_only_ref(&self.image_push(image_ref)?);
        eprintln!("Config image pushed successfully: {pinned}");
        Ok(pinned)
    }

    /// Copy the Antithesis test-template tree (`/opt/antithesis/test/v1`)
    /// out of a container into `dst`. Returns [`TestTemplates::Absent`]
    /// when the directory does not exist inside the container — a normal
    /// outcome for services that don't define any test commands. Real cp
    /// failures (permission denied, runtime errors, etc.) are returned as
    /// `Err` with stderr attached.
    ///
    /// When `running` is true, performs an unambiguous pre-flight existence
    /// check via `runtime exec <id> test -d <path>`. When false (stopped
    /// containers — exec is unavailable), falls back to attempting the cp
    /// and inspecting stderr to distinguish absence from real errors.
    fn extract_test_templates(
        &self,
        container_id: &str,
        dst: &Path,
        running: bool,
    ) -> Result<TestTemplates> {
        if running {
            match self.container_path_kind(container_id, TEST_TEMPLATES_PATH)? {
                PathKind::Directory => {}
                PathKind::Missing => return Ok(TestTemplates::Absent),
                PathKind::OtherOrUnknown => {
                    // exec succeeded but path isn't a directory, or the
                    // pre-flight is inconclusive. Fall through to cp and let
                    // it produce the diagnostic.
                }
            }
        }

        let runtime = self.name();
        let src_arg = format!("{container_id}:{TEST_TEMPLATES_PATH}");
        let mut cp_cmd = Command::new(runtime);
        cp_cmd.args(["cp", &src_arg, &dst.display().to_string()]);
        let output = output_with_timeout(cp_cmd, DISCOVERY_COMMAND_TIMEOUT)
            .wrap_err_with(|| format!("failed to run '{runtime} cp'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr_indicates_missing_source(&stderr) {
                return Ok(TestTemplates::Absent);
            }
            return Err(eyre!("'{runtime} cp' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        Ok(TestTemplates::Present)
    }

    /// Probe whether `path` exists inside `container_id` (must be running).
    /// Runs `runtime exec <id> test -d <path>`; exit 0 → Directory, exit 1 →
    /// Missing, any other failure (no shell, container died, etc.) →
    /// OtherOrUnknown so callers fall back to cp.
    fn container_path_kind(&self, container_id: &str, path: &str) -> Result<PathKind> {
        let runtime = self.name();
        let mut exec_cmd = Command::new(runtime);
        exec_cmd.args(["exec", container_id, "test", "-d", path]);
        let output = output_with_timeout(exec_cmd, DISCOVERY_COMMAND_TIMEOUT)
            .wrap_err_with(|| format!("failed to run '{runtime} exec'"))?;
        match output.status.code() {
            Some(0) => Ok(PathKind::Directory),
            Some(1) => Ok(PathKind::Missing),
            _ => Ok(PathKind::OtherOrUnknown),
        }
    }
}

/// Result of [`ContainerRuntime::container_path_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    /// Path exists and is a directory.
    Directory,
    /// Path does not exist inside the container.
    Missing,
    /// Pre-flight inconclusive (no `test` binary, exec failed, etc.).
    OtherOrUnknown,
}

/// Standard path inside a container where Antithesis test templates live.
pub const TEST_TEMPLATES_PATH: &str = "/opt/antithesis/test/v1";

/// Result of [`ContainerRuntime::extract_test_templates`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestTemplates {
    /// Templates were present and copied into the destination directory.
    Present,
    /// The container does not have an Antithesis test-templates directory.
    Absent,
}

/// Match patterns `docker cp` and `podman cp` emit when the source path
/// doesn't exist inside the container. Used only as a fallback for stopped
/// containers where the `test -d` pre-flight can't run.
///
/// The loose substring `"no such file"` (without "or directory") is
/// deliberately excluded — it matches destination-side messages too.
fn stderr_indicates_missing_source(stderr: &str) -> bool {
    let s = stderr.to_ascii_lowercase();
    s.contains("could not find the file")
        || s.contains("no such file or directory")
        || s.contains("does not exist in container")
        || s.contains("could not be found on container")
}

/// The `docker-compose` (Docker Compose v2) binary. snouty always drives
/// Compose through this binary, independent of the image runtime, pointing it
/// at the right engine via `DOCKER_HOST`.
const DOCKER_COMPOSE: &str = "docker-compose";

/// Build a [`DockerCompose`] handle targeting `rt`'s container engine.
///
/// Verifies the `docker-compose` binary is installed (Docker Compose v2) with a
/// clear error otherwise. An explicit `DOCKER_HOST` already set in the
/// environment is always respected; otherwise, for a podman runtime,
/// docker-compose is pointed at podman's API socket so podman backs Compose.
pub fn docker_compose(rt: &dyn ContainerRuntime) -> Result<DockerCompose<'_>> {
    ensure_docker_compose()?;
    let compose_binary = docker_compose_binary()?;
    let docker_host = if std::env::var_os("DOCKER_HOST").is_some() {
        None
    } else {
        rt.engine_docker_host()?
    };
    Ok(DockerCompose {
        rt,
        docker_host,
        compose_binary,
    })
}

/// Resolve `docker-compose` before clearing the environment for a hermetic
/// render. [`Command`] otherwise needs `PATH` to find it.
fn docker_compose_binary() -> Result<PathBuf> {
    which::which(DOCKER_COMPOSE).wrap_err("failed to resolve the `docker-compose` binary on PATH")
}

/// Verify the `docker-compose` binary is present and is Docker Compose v2.
fn ensure_docker_compose() -> Result<()> {
    docker_compose_version().map(|_| ())
}

/// Return the `docker-compose` version banner, with a clear error when the
/// binary is missing or is not Docker Compose v2.
pub fn docker_compose_version() -> Result<String> {
    match Command::new(DOCKER_COMPOSE).arg("version").output() {
        Ok(o) if o.status.success() => {
            let version = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if is_compose_v2_version(&version) {
                Ok(version)
            } else {
                Err(eyre!(
                    "snouty requires Docker Compose v2 (the `docker-compose` binary)"
                ))
                .with_section(move || version.header("docker-compose version:"))
                .with_suggestion(|| {
                    "install Docker Compose v2: https://docs.docker.com/compose/install/"
                })
            }
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            Err(eyre!(
                "`docker-compose version` failed; snouty requires the `docker-compose` binary (Docker Compose v2)"
            ))
            .with_section(move || stderr.trim().to_string().header("Stderr:"))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(eyre!(
            "snouty requires the `docker-compose` binary (Docker Compose v2), but it was not found on PATH"
        ))
        .with_suggestion(|| "install Docker Compose v2: https://docs.docker.com/compose/install/"),
        Err(e) => Err(eyre!("failed to run `docker-compose version`: {e}")),
    }
}

/// Whether `docker-compose version` output identifies Docker Compose v2.
fn is_compose_v2_version(output: &str) -> bool {
    output.to_lowercase().contains("docker compose")
}

/// Drives Docker Compose v2 via the `docker-compose` binary.
///
/// All compose operations go through `docker-compose`, independent of which
/// runtime built or pushed the images. `docker_host`, when set, points
/// docker-compose at a specific engine (e.g. podman's API socket); when `None`,
/// docker-compose uses its default (the Docker daemon, or an explicit
/// `DOCKER_HOST` inherited from the environment).
pub struct DockerCompose<'a> {
    rt: &'a dyn ContainerRuntime,
    docker_host: Option<String>,
    compose_binary: PathBuf,
}

impl DockerCompose<'_> {
    /// The container runtime that owns the compose containers (used for
    /// container-level operations such as `cp`/`exec`).
    pub fn runtime(&self) -> &dyn ContainerRuntime {
        self.rt
    }

    /// The `DOCKER_HOST` docker-compose is wired to, if any. Used to reproduce
    /// the compose invocation in user-facing hints.
    pub fn docker_host(&self) -> Option<&str> {
        self.docker_host.as_deref()
    }

    /// Base `docker-compose` command with engine wiring applied.
    fn command(&self, config: &ComposeConfig) -> Command {
        let mut cmd = Command::new(&self.compose_binary);
        cmd.current_dir(config.dir());
        if let Some(host) = &self.docker_host {
            cmd.env("DOCKER_HOST", host);
        }
        cmd
    }

    /// Async variant of [`command`](Self::command).
    fn tokio_command(&self, config: &ComposeConfig) -> tokio::process::Command {
        self.command(config).into()
    }

    /// Run `docker-compose config [extra_args]`, returning the resolved YAML
    /// as a string.
    fn config_yaml(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
        extra_args: &[&str],
    ) -> Result<String> {
        // No COMPOSE_PROJECT_NAME override: the project name must resolve
        // exactly as it does when the user runs `docker compose` in the
        // config dir, because default build tags are derived from it.
        let mut cmd = self.command(config);
        cmd.args(compose_file_args(overlay));
        cmd.arg("config");
        cmd.args(extra_args);
        let output = cmd
            .output()
            .wrap_err("failed to run 'docker-compose config'")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'docker-compose config' failed"))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Resolve and parse a compose config into structured contents.
    pub fn contents(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<ComposeContents> {
        let yaml = self.config_yaml(config, overlay, &[])?;
        parse_compose_config(&yaml)
    }

    /// Resolve the compose file to JSON using the normal (local) environment —
    /// the same interpolation `snouty` sees when it runs compose on this machine.
    pub fn config_json(&self, config: &ComposeConfig) -> Result<String> {
        self.config_yaml(config, None, &["--format", "json"])
    }

    /// Resolve the compose file to JSON under a scrubbed process environment
    /// that mimics the hermetic Antithesis environment: none of the user's shell
    /// variables, so `${VAR}` interpolation resolves only from the config dir's
    /// `.env` file, explicit env files, and inline defaults. The compose binary
    /// was resolved before the environment is cleared, so no shell variables
    /// need to be retained.
    ///
    /// Returns the raw output rather than a string: a required `${VAR:?}` with no
    /// value makes compose abort (non-zero exit, empty stdout), which the caller
    /// reads as a definite "won't resolve in Antithesis".
    pub fn config_json_hermetic_env(&self, config: &ComposeConfig) -> Result<Output> {
        // Reuse the normal command (binary + working directory), then clear the
        // whole environment — including the DOCKER_HOST that command() sets.
        // Those are all valid interpolation inputs Antithesis will not inherit,
        // so scrubbing them is the point; only the binary and directory carry
        // over. (env_clear() drops anything command() set via .env().)
        let mut cmd = self.command(config);
        cmd.env_clear();
        cmd.args(compose_file_args(None));
        cmd.args(["config", "--format", "json"]);
        cmd.output()
            .wrap_err("failed to run 'docker-compose config' for the environment check")
    }

    /// Canonicalized compose file for baking into the config image.
    ///
    /// `docker-compose config` itself does the canonicalization: anchors,
    /// aliases, and merge keys are inlined, and the structure is normalized.
    /// `--no-interpolate` keeps `${VAR}` references for the platform to
    /// resolve in its own environment, and `--no-path-resolution` keeps
    /// relative paths relative — both would otherwise be baked with values
    /// from this machine.
    fn canonical_contents(&self, config: &ComposeConfig) -> Result<String> {
        self.config_yaml(config, None, &["--no-interpolate", "--no-path-resolution"])
    }

    /// Parse `docker-compose ps -a --format json` into the list of containers,
    /// including stopped/exited ones so callers can flag stranded test
    /// commands. Inspect [`ComposeContainer::stopped`] to tell them apart.
    pub fn ps(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<Vec<ComposeContainer>> {
        let mut cmd = self.command(config);
        cmd.args(compose_file_args(overlay));
        cmd.args(["ps", "-a", "--format", "json"]);

        let output = output_with_timeout(cmd, DISCOVERY_COMMAND_TIMEOUT)
            .wrap_err("failed to run 'docker-compose ps'")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'docker-compose ps' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_compose_ps(&stdout)
    }

    /// Run a command inside a running compose service container.
    ///
    /// Runs `docker-compose exec -T [--workdir workdir] {service} {cmd...}`.
    /// The `-T` flag disables TTY allocation for non-interactive use. If
    /// `workdir` is `Some`, sets the working directory inside the container.
    /// Stdout and stderr are captured in the returned `Output`.
    pub fn exec(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
        service: &str,
        workdir: Option<&str>,
        env: &[(&str, &str)],
        cmd: &[&str],
    ) -> Result<std::process::Output> {
        let mut command = self.command(config);
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
            .wrap_err("failed to run 'docker-compose exec'")
    }

    /// Spawn `docker-compose up --detach` and return the child process.
    ///
    /// stdout and stderr are inherited so progress is visible during pulls. The
    /// caller awaits the child and checks its exit status. Uses
    /// `process_group(0)` so the whole group can be killed on timeout.
    pub fn up_detached(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<ProcessGroupChild> {
        let mut cmd = self.tokio_command(config);
        cmd.args(compose_file_args(overlay));
        cmd.args(["up", "--detach", "--no-build", "--pull=never"]);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);

        cmd.spawn()
            .map(ProcessGroupChild::new)
            .wrap_err("failed to start 'docker-compose up --detach'")
    }

    /// Spawn `docker-compose logs --follow` and return the child process.
    ///
    /// stdout and stderr are inherited so log output goes straight to the
    /// terminal. stdin is null. The process exits when all containers stop.
    pub fn logs_follow(
        &self,
        config: &ComposeConfig,
        overlay: Option<&Path>,
    ) -> Result<ProcessGroupChild> {
        let mut cmd = self.tokio_command(config);
        cmd.args(compose_file_args(overlay));
        cmd.args(["logs", "--follow"]);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);

        cmd.spawn()
            .map(ProcessGroupChild::new)
            .wrap_err("failed to start 'docker-compose logs --follow'")
    }

    /// Run `docker-compose down` for cleanup. Best-effort, ignores errors.
    pub fn down(&self, config: &ComposeConfig, overlay: Option<&Path>) {
        let mut cmd = self.command(config);
        cmd.args(compose_file_args(overlay));
        cmd.args(["down", "--timeout", "0"]);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        let _ = cmd.status();
    }

    /// Resolve every compose service image to a digest-pinned reference and
    /// return the `docker-compose.yaml` contents canonicalized and rewritten
    /// with those pins (`name:tag@sha256:...`).
    ///
    /// The local image store is the single source of truth for what a launch
    /// runs — snouty never pulls. Every service must resolve to an image that
    /// is present locally (built via its `build:` stanza, built or loaded out
    /// of band, or previously pulled). Each image is then pinned to its local
    /// digest in a registry confirmed to serve it ([`Self::find_remote_pin`]),
    /// or — when no registry has it — tagged with the `registry` prefix and
    /// pushed, so the platform always pulls exactly what was resolved here.
    pub fn pin_images(&self, config: &ComposeConfig, registry: &str) -> Result<String> {
        let contents = self.contents(config, None)?;
        with_config_image_escape_hatch(validate_images_are_available(self.rt, &contents))?;

        let prefix = format!("{}/", registry.trim_end_matches('/'));

        // Resolve each distinct image once: pin it from a registry that
        // already serves the local digest, or schedule it for push.
        let mut resolution: BTreeMap<&str, Option<String>> = BTreeMap::new();
        for service in &contents.services {
            let image = service.image.as_str();
            if !resolution.contains_key(image) {
                let pin = self.find_remote_pin(image, &prefix)?;
                if let Some(pinned_ref) = &pin {
                    eprintln!("Image already in a registry, skipping push: {pinned_ref}");
                }
                resolution.insert(image, pin);
            }
        }

        // service name -> final pinned reference (filled in for push targets
        // after their pushes complete).
        let mut pinned: BTreeMap<String, String> = BTreeMap::new();
        // (service name, registry reference) for images we push ourselves.
        let mut push_targets: Vec<(String, String)> = Vec::new();
        let mut tagged: HashSet<&str> = HashSet::new();
        for service in &contents.services {
            let image = service.image.as_str();
            if let Some(remote) = &resolution[image] {
                pinned.insert(service.name.clone(), remote.clone());
                continue;
            }
            let dest = if image.starts_with(&prefix) {
                image.to_string()
            } else {
                format!("{prefix}{image}")
            };
            if dest != image && tagged.insert(image) {
                self.rt.image_tag(image, &dest)?;
            }
            push_targets.push((service.name.clone(), dest));
        }

        // Arch-check and push each distinct image, pinning every service to
        // its push digest (the push already reports the digest). The local
        // architecture check applies exactly to the images whose local bytes
        // we upload; remote pins were already amd64-verified.
        let mut seen = HashSet::new();
        let dests: Vec<&str> = push_targets
            .iter()
            .map(|(_, dest)| dest.as_str())
            .filter(|dest| seen.insert(*dest))
            .collect();
        validate_image_architectures(self.rt, &dests)?;
        let mut digests: BTreeMap<&str, String> = BTreeMap::new();
        for dest in &dests {
            eprintln!("Pushing image: {dest}");
            let pinned_ref = self.rt.image_push(dest)?;
            eprintln!("Image pushed: {pinned_ref}");
            digests.insert(dest, pinned_ref);
        }
        for (name, dest) in &push_targets {
            pinned.insert(name.clone(), digests[dest.as_str()].clone());
        }

        rewrite_compose_images(&self.canonical_contents(config)?, &pinned)
    }

    /// Find a registry that already serves `image`'s local bytes, returning
    /// the digest-pinned reference to use, or `None` when the image must be
    /// pushed.
    ///
    /// Candidate digests come from the local store's repo digests, for two
    /// repositories: the image's own (e.g. `docker.io/library/redis` for
    /// `redis:7`) and its `prefix`ed name from a previous snouty push. A
    /// candidate counts only when the registry confirms it serves the digest
    /// (a manifest-only round trip — never a pull or push) AND the platform
    /// can run amd64 from it: a manifest list must offer an amd64 entry,
    /// while a single manifest shares the local image's architecture, so the
    /// local image must be amd64.
    fn find_remote_pin(&self, image: &str, prefix: &str) -> Result<Option<String>> {
        let repo_digests = self.rt.image_repo_digests(image)?;
        let tag = image_ref_tag(image);

        let mut repos = vec![normalize_repo(image_repo(image))];
        if !image.starts_with(prefix) {
            repos.push(normalize_repo(image_repo(&format!("{prefix}{image}"))));
        }

        for repo in &repos {
            // A pull typically records several digests per repo (the
            // per-arch manifest and the manifest list) — try them all.
            for digest in digests_for_repo(repo, &repo_digests) {
                let amd64_ok = match self.rt.remote_manifest(&format!("{repo}@{digest}")) {
                    RemoteManifest::NotFound => continue,
                    RemoteManifest::List { has_amd64 } => has_amd64,
                    RemoteManifest::Single => self.rt.image_architecture(image)? == "amd64",
                };
                if amd64_ok {
                    return Ok(Some(format!("{repo}:{tag}@{digest}")));
                }
                // Served, but not runnable as amd64 — keep looking; the push
                // path's local arch check produces the actionable error.
            }
        }
        Ok(None)
    }
}

/// Rewrite each service's `image:` field to its pinned digest reference.
///
/// Every service in the document must have an entry in `pinned` — the whole
/// point of the rewrite is that the platform runs only digest-pinned images,
/// so a service this function can't pin means pinning lost track of it
/// somewhere upstream, which is a bug.
fn rewrite_compose_images(yaml: &str, pinned: &BTreeMap<String, String>) -> Result<String> {
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).wrap_err("failed to parse docker-compose.yaml")?;
    let services = doc
        .get_mut("services")
        .and_then(|s| s.as_mapping_mut())
        .ok_or_else(|| eyre!("compose config has no services — this is a bug in snouty"))?;
    for (name, svc) in services.iter_mut() {
        let name = name
            .as_str()
            .ok_or_else(|| eyre!("compose config has a non-string service name: {name:?}"))?;
        let pinned_ref = pinned.get(name).ok_or_else(|| {
            eyre!("service '{name}' did not resolve to a pinned image reference — this is a bug in snouty")
        })?;
        let svc_map = svc
            .as_mapping_mut()
            .ok_or_else(|| eyre!("compose service '{name}' is not a mapping"))?;
        svc_map.insert(
            serde_yaml::Value::String("image".to_string()),
            serde_yaml::Value::String(pinned_ref.clone()),
        );
    }
    serde_yaml::to_string(&doc).wrap_err("failed to serialize pinned docker-compose.yaml")
}

/// Every compose service must carry an explicit `image:` reference; pinning
/// (and the Antithesis platform) addresses services by image. In particular a
/// `build:`-only service would otherwise silently run under a compose-generated
/// default name that snouty never pushed.
fn ensure_services_have_images(contents: &ComposeContents) -> Result<()> {
    if contents.services_without_image.is_empty() {
        return Ok(());
    }
    let mut err = eyre!("every compose service needs an explicit `image:` reference");
    for name in &contents.services_without_image {
        err = err.with_note(|| format!("service '{name}' has no `image:` field"));
    }
    Err(err.with_suggestion(|| {
        "add an `image:` field to each service (for `build:` services, the tag the build produces)"
    }))
}

/// Stage a copy of `config_dir` with `docker-compose.yaml` replaced by
/// `pinned_yaml`, so the config image is built from the digest-pinned compose.
/// The returned [`tempfile::TempDir`] must be kept alive until the image build
/// completes.
///
/// Symlinks are recreated as-is (not dereferenced): a `docker build` context
/// tars symlinks verbatim too, so the staged tree produces the same image
/// content as building from `config_dir` directly.
pub fn stage_pinned_config(config_dir: &Path, pinned_yaml: &str) -> Result<tempfile::TempDir> {
    let staged = tempfile::tempdir().wrap_err("failed to create config staging directory")?;
    copy_dir_recursive(config_dir, staged.path())?;
    std::fs::write(staged.path().join("docker-compose.yaml"), pinned_yaml)
        .wrap_err("failed to write pinned docker-compose.yaml")?;
    Ok(staged)
}

/// Recursively copy the contents of `src` into `dst` (which must already exist).
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    for entry in
        std::fs::read_dir(src).wrap_err_with(|| format!("failed to read {}", src.display()))?
    {
        let entry = entry.wrap_err_with(|| format!("failed to read entry in {}", src.display()))?;
        let file_type = entry.file_type().wrap_err("failed to read file type")?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            std::fs::create_dir_all(&to)
                .wrap_err_with(|| format!("failed to create {}", to.display()))?;
            copy_dir_recursive(&from, &to)?;
        } else if file_type.is_symlink() {
            let target = std::fs::read_link(&from)
                .wrap_err_with(|| format!("failed to read symlink {}", from.display()))?;
            std::os::unix::fs::symlink(&target, &to)
                .wrap_err_with(|| format!("failed to create symlink {}", to.display()))?;
        } else {
            std::fs::copy(&from, &to).wrap_err_with(|| {
                format!("failed to copy {} to {}", from.display(), to.display())
            })?;
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct PodmanRuntime {
    cmd: String,
}

impl PodmanRuntime {
    pub(crate) fn new(cmd: impl Into<String>) -> Self {
        Self { cmd: cmd.into() }
    }

    /// The host-side path of the podman machine's forwarded API socket, or
    /// `None` when there's no machine (e.g. native Linux podman). Used on macOS,
    /// where podman runs in a VM and the host can't reach the in-VM socket.
    fn podman_machine_socket(&self) -> Option<String> {
        let output = Command::new(&self.cmd)
            .args([
                "machine",
                "inspect",
                "--format",
                "{{.ConnectionInfo.PodmanSocket.Path}}",
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() || path == "<no value>" {
            None
        } else {
            Some(resolve_machine_socket_path(path))
        }
    }
}

/// Resolve the podman machine API socket path to one that actually exists.
///
/// podman recomputes this path from the current `TMPDIR` on every invocation
/// (falling back to `/tmp` when unset), while the socket itself was bound
/// under the `TMPDIR` in effect at `podman machine start`. When those
/// environments differ, the reported path doesn't exist — probe the sibling
/// locations the path is derived from before giving up. If no candidate
/// exists, return the reported path unchanged and let docker-compose surface
/// its connection error naming it.
fn resolve_machine_socket_path(reported: String) -> String {
    let reported_path = Path::new(&reported);
    if reported_path.exists() {
        return reported;
    }
    let Some(file_name) = reported_path.file_name() else {
        return reported;
    };
    let mut candidates = Vec::new();
    if let Some(tmpdir) = std::env::var_os("TMPDIR") {
        candidates.push(
            std::path::PathBuf::from(tmpdir)
                .join("podman")
                .join(file_name),
        );
    }
    candidates.push(std::path::PathBuf::from("/tmp/podman").join(file_name));
    for candidate in candidates {
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    reported
}

impl ContainerRuntime for PodmanRuntime {
    fn name(&self) -> &str {
        &self.cmd
    }

    fn engine_kind(&self) -> &'static str {
        "podman"
    }

    fn clone_box(&self) -> Box<dyn ContainerRuntime> {
        Box::new(self.clone())
    }

    /// Point docker-compose at podman's Docker-compatible API socket so podman
    /// backs Compose. Only the socket path is needed here; if the socket isn't
    /// actually listening, docker-compose surfaces its own clear, platform-
    /// agnostic connection error naming the path.
    fn engine_docker_host(&self) -> Result<Option<String>> {
        // On macOS, podman runs inside a VM. `podman info` reports the
        // in-VM socket path, which the host can't reach — the host-forwarded
        // socket comes from `podman machine inspect` instead.
        if cfg!(target_os = "macos")
            && let Some(path) = self.podman_machine_socket()
        {
            return Ok(Some(format!("unix://{path}")));
        }

        let output = Command::new(&self.cmd)
            .args(["info", "--format", "{{.Host.RemoteSocket.Path}}"])
            .output()
            .wrap_err_with(|| format!("failed to run '{} info'", self.cmd))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!(
                "'{} info' failed while detecting the podman API socket",
                self.cmd
            ))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            return Err(eyre!(
                "could not determine podman's API socket path; set DOCKER_HOST to a reachable container engine"
            ));
        }
        Ok(Some(format!("unix://{path}")))
    }

    fn remote_manifest(&self, image_ref: &str) -> RemoteManifest {
        remote_manifest_via_cli(&self.cmd, "--tls-verify=false", image_ref)
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

    fn engine_kind(&self) -> &'static str {
        "docker"
    }

    fn remote_manifest(&self, image_ref: &str) -> RemoteManifest {
        remote_manifest_via_cli(&self.cmd, "--insecure", image_ref)
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
/// The result is NOT cached, so make sure you hold on to the result if you need to use it more than once
///
/// Set `SNOUTY_CONTAINER_ENGINE=podman` or `=docker` to force a specific runtime.
pub fn runtime(settings: &Settings) -> Result<Box<dyn ContainerRuntime>> {
    // An explicit engine setting (from SNOUTY_CONTAINER_ENGINE or a settings
    // file) short-circuits auto-detection.
    if let Some(engine) = settings.container_engine() {
        return match engine {
            "podman" => Ok(Box::new(PodmanRuntime::new("podman"))),
            "docker" => Ok(Box::new(DockerRuntime::new("docker"))),
            other => Err(eyre!(
                "unsupported container engine '{other}': expected 'podman' or 'docker'"
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
        Err(e) => return Err(eyre!("failed to check podman: {e}")),
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
        Ok(_) => Err(eyre!(
            "'docker --version' failed; unable to find working container runtime"
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(eyre!("neither podman nor docker is installed"))
        }
        Err(e) => Err(eyre!("failed to check docker: {e}")),
    }
}

/// Tell the user which container engine snouty picked, but only when it was
/// auto-detected. An explicit `SNOUTY_CONTAINER_ENGINE` / `container_engine`
/// setting means the user already chose, so there's nothing to announce, and
/// machine-readable (`json`) output stays silent. Prints to stderr (never
/// stdout, so it can't contaminate `--json`). The note points at the override
/// so a surprising pick is easy to fix.
pub fn announce_auto_detected_engine(settings: &Settings, rt: &dyn ContainerRuntime, json: bool) {
    if json || settings.container_engine().is_some() {
        return;
    }
    // Report the real engine, not the binary name: for podman-in-disguise the
    // command is `docker` but the engine is podman.
    let engine = rt.engine_kind();
    eprintln!(
        "Using auto-detected container engine '{engine}'. If that's not what you \
         expect, select one explicitly with SNOUTY_CONTAINER_ENGINE={engine} (or \
         `container_engine = \"{engine}\"` in a snouty settings file)."
    );
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

/// Build a pinned image reference (`name:tag@digest`) from a tagged ref and a
/// digest. The tag is kept as human-readable provenance — when inspecting an
/// Antithesis run, the reference still shows which tag the digest came from.
/// If the ref already carries a digest, it's replaced.
fn pinned_image_ref(image_ref: &str, digest: &str) -> String {
    match image_ref.rfind('@') {
        Some(at) => format!("{}@{}", &image_ref[..at], digest),
        None => format!("{image_ref}@{digest}"),
    }
}

/// Drop the `:tag` from a digest-pinned reference, leaving `name@digest`.
/// The digest fully identifies the image, and Antithesis's config-image
/// validator rejects a reference that carries both a tag and a digest.
fn digest_only_ref(image_ref: &str) -> String {
    match image_ref.rfind('@') {
        Some(at) => format!("{}{}", image_repo(image_ref), &image_ref[at..]),
        None => image_ref.to_string(),
    }
}

/// Result of [`ContainerRuntime::remote_manifest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteManifest {
    /// The registry doesn't serve this manifest, or it couldn't be checked.
    NotFound,
    /// A multi-platform manifest list. `has_amd64` reports whether it offers
    /// an amd64 entry for the platform to pull.
    List { has_amd64: bool },
    /// A single-platform manifest. Its architecture is the architecture of
    /// the local image carrying the same digest.
    Single,
}

/// Run `{runtime} manifest inspect` and classify the result. `insecure_flag`
/// is the runtime's spelling for plain-HTTP registries (`--insecure` for
/// docker, `--tls-verify=false` for podman), applied only to local registries.
fn remote_manifest_via_cli(runtime: &str, insecure_flag: &str, image_ref: &str) -> RemoteManifest {
    let mut args = vec!["manifest", "inspect"];
    if image_ref.starts_with("localhost") || image_ref.starts_with("127.0.0.1") {
        args.push(insecure_flag);
    }
    args.push(image_ref);
    match Command::new(runtime).args(&args).output() {
        Ok(output) if output.status.success() => classify_manifest_json(&output.stdout),
        _ => RemoteManifest::NotFound,
    }
}

/// Classify `manifest inspect` JSON output: a `manifests` array marks a
/// manifest list (scan its platforms for amd64); any other valid manifest
/// JSON is a single-platform manifest.
fn classify_manifest_json(stdout: &[u8]) -> RemoteManifest {
    let Ok(doc) = serde_json::from_slice::<serde_json::Value>(stdout) else {
        return RemoteManifest::NotFound;
    };
    match doc.get("manifests").and_then(|m| m.as_array()) {
        Some(entries) => RemoteManifest::List {
            has_amd64: entries.iter().any(|entry| {
                entry
                    .get("platform")
                    .and_then(|p| p.get("architecture"))
                    .and_then(|a| a.as_str())
                    == Some("amd64")
            }),
        },
        None => RemoteManifest::Single,
    }
}

/// The repository part of an image reference: strips any `@digest` suffix and
/// any `:tag` (a colon counts as a tag separator only after the last `/`;
/// before it, it's a registry port).
fn image_repo(image_ref: &str) -> &str {
    let no_digest = match image_ref.rfind('@') {
        Some(at) => &image_ref[..at],
        None => image_ref,
    };
    match no_digest.rfind('/') {
        Some(slash) => match no_digest[slash..].rfind(':') {
            Some(colon) => &no_digest[..slash + colon],
            None => no_digest,
        },
        None => match no_digest.rfind(':') {
            Some(colon) => &no_digest[..colon],
            None => no_digest,
        },
    }
}

/// The tag of an image reference, or `latest` when untagged.
fn image_ref_tag(image_ref: &str) -> &str {
    let no_digest = match image_ref.rfind('@') {
        Some(at) => &image_ref[..at],
        None => image_ref,
    };
    let repo = image_repo(image_ref);
    if no_digest.len() > repo.len() {
        &no_digest[repo.len() + 1..]
    } else {
        "latest"
    }
}

/// Expand Docker Hub shorthand so repository names compare reliably across
/// runtimes and reference styles: `nginx` → `docker.io/library/nginx`,
/// `user/app` → `docker.io/user/app`, `index.docker.io/...` → `docker.io/...`.
/// Repositories naming any other registry (first component with a dot, a
/// port, or `localhost`) pass through unchanged.
fn normalize_repo(repo: &str) -> String {
    let (registry, rest) = match repo.split_once('/') {
        Some((first, rest))
            if first.contains('.') || first.contains(':') || first == "localhost" =>
        {
            let registry = if first == "index.docker.io" {
                "docker.io"
            } else {
                first
            };
            (registry, rest)
        }
        _ => ("docker.io", repo),
    };
    if registry == "docker.io" && !rest.contains('/') {
        format!("{registry}/library/{rest}")
    } else {
        format!("{registry}/{rest}")
    }
}

/// All digests the local store records for `repo` among `repo_digests`
/// entries, comparing normalized repository names. Multiple entries per repo
/// are common — a pull records both the per-arch manifest digest and the
/// manifest-list digest.
fn digests_for_repo(repo: &str, repo_digests: &[String]) -> Vec<String> {
    let want = normalize_repo(repo);
    repo_digests
        .iter()
        .filter_map(|entry| {
            let (entry_repo, digest) = entry.rsplit_once('@')?;
            (digest.starts_with("sha256:") && normalize_repo(entry_repo) == want)
                .then(|| digest.to_string())
        })
        .collect()
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
    /// True when `image` is the compose-generated default build tag
    /// (`<project>-<service>:latest`) synthesized for a `build:`-only
    /// service, rather than an explicit `image:` value.
    pub default_image: bool,
}

/// Parsed contents of a compose config file.
#[derive(Debug)]
pub struct ComposeContents {
    /// One entry per service, each resolved to an image reference: the
    /// explicit `image:` value, or for `build:`-only services the
    /// compose-default build tag (`<project>-<service>:latest`, flagged via
    /// [`ComposeService::default_image`]).
    pub services: Vec<ComposeService>,
    /// Names of services whose image reference couldn't be resolved — no
    /// explicit `image:` and no way to derive compose's default name. A
    /// backstop: compose itself rejects services with neither `image` nor
    /// `build`, and `docker-compose config` always reports a project `name`.
    pub services_without_image: Vec<String>,
    /// Service names that have a `build:` stanza.
    pub build_services: HashSet<String>,
    /// Explicitly declared network names (from the top-level `networks` key).
    pub networks: Vec<String>,
}

/// Parse services and networks from resolved compose config YAML.
pub fn parse_compose_config(yaml: &str) -> Result<ComposeContents> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).wrap_err("failed to parse docker-compose.yaml")?;

    // `docker-compose config` resolves the project name (file `name:`, else
    // the config directory's basename) and reports it at the top level. It's
    // what compose prefixes onto default build tags, so resolution here
    // matches what `docker compose build` produced — provided nothing
    // overrides the project name out from under us.
    let project_name = doc.get("name").and_then(|n| n.as_str());

    let mut services = Vec::new();
    let mut services_without_image = Vec::new();
    let mut build_services = HashSet::new();
    if let Some(svc_map) = doc.get("services").and_then(|s| s.as_mapping()) {
        for (name, service) in svc_map {
            if let Some(name_str) = name.as_str() {
                let has_build = service.get("build").is_some();
                if has_build {
                    build_services.insert(name_str.to_string());
                }
                if let Some(image) = service.get("image").and_then(|i| i.as_str()) {
                    services.push(ComposeService {
                        name: name_str.to_string(),
                        image: image.to_string(),
                        default_image: false,
                    });
                } else if has_build && let Some(project) = project_name {
                    // `docker compose build` tags a build-only service as
                    // `<project>-<service>` (implicitly `:latest`).
                    services.push(ComposeService {
                        name: name_str.to_string(),
                        image: format!("{project}-{name_str}:latest"),
                        default_image: true,
                    });
                } else {
                    services_without_image.push(name_str.to_string());
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
        services_without_image,
        build_services,
        networks,
    })
}

/// Ensure the referenced images are available in the local image store.
/// snouty never pulls — what runs (validate) and what gets pushed (launch)
/// is exactly what's in the local store.
///
/// The error is intentionally context-free: it explains only why local presence
/// is required. Command-specific escape hatches (e.g. launch's `--config-image`,
/// see [`with_config_image_escape_hatch`]) are layered on by the caller so this
/// shared check doesn't have to know who called it.
pub fn validate_images_are_available(
    runtime: &dyn ContainerRuntime,
    contents: &ComposeContents,
) -> Result<()> {
    ensure_services_have_images(contents)?;

    let mut seen = HashSet::new();
    let mut missing = Vec::new();
    let mut missing_refs = Vec::new();

    for service in &contents.services {
        if !seen.insert(service.image.as_str()) {
            continue;
        }

        if !runtime.image_exists(&service.image)? {
            let hint = if service.default_image {
                format!(
                    " (compose's default build tag for service '{}' — run `docker compose build`, \
                     or add an explicit `image:` if it was built under another name)",
                    service.name
                )
            } else if contents.build_services.contains(&service.name) {
                " (service has a `build:` stanza — build it first; snouty doesn't build)"
                    .to_string()
            } else {
                String::new()
            };
            missing.push(format!("image: {}{hint}", service.image));
            missing_refs.push(service.image.clone());
        }
    }

    if missing.is_empty() {
        return Ok(());
    }

    let mut err = eyre!("some images are not available locally");
    for note in missing {
        err = err.with_note(|| note);
    }
    err = err.with_note(|| {
        "snouty never pulls — what it validates and launches is exactly what's in your \
         local image store, so every referenced image must already be present there"
    });

    // A missing image is often just sitting in the *other* installed engine's
    // store — the usual cause is `docker compose build` landing it in docker
    // while snouty auto-selected podman (or vice versa), since the two keep
    // separate image stores. Surface that instead of leaving the generic
    // "build it first" note to mislead someone who already built the image.
    // Best-effort: any probe failure counts as "not there".
    let elsewhere = images_available_in_other_engines(runtime, &missing_refs);
    if let Some((warnings, suggestion)) = cross_engine_guidance(runtime.name(), &elsewhere) {
        for warning in warnings {
            err = err.with_warning(move || warning);
        }
        err = err.with_suggestion(move || suggestion);
    }

    Err(err.with_suggestion(|| "pull or build the missing images, then retry"))
}

/// Probe every installed engine other than `active` for the given images.
/// Returns, for each other engine that holds at least one, its name and the
/// images it has. Best-effort — a probe error counts as "absent", since this
/// only enriches an error that is already being returned.
fn images_available_in_other_engines(
    active: &dyn ContainerRuntime,
    images: &[String],
) -> Vec<(String, Vec<String>)> {
    if images.is_empty() {
        return Vec::new();
    }
    let active_name = active.name();
    available_engines()
        .into_iter()
        .filter(|engine| engine.name() != active_name)
        .filter_map(|engine| {
            let present: Vec<String> = images
                .iter()
                .filter(|image| engine.image_exists(image).unwrap_or(false))
                .cloned()
                .collect();
            (!present.is_empty()).then(|| (engine.name().to_string(), present))
        })
        .collect()
}

/// Build cross-engine guidance for images missing from the active engine
/// (`active`) but present in another installed one. `elsewhere` pairs each
/// other engine's name with the missing images it holds. Returns the per-image
/// warning lines plus one suggestion pointing at the engine override, or `None`
/// when nothing turned up elsewhere. Pure, so it is unit-tested without real
/// engines.
fn cross_engine_guidance(
    active: &str,
    elsewhere: &[(String, Vec<String>)],
) -> Option<(Vec<String>, String)> {
    let (source, _) = elsewhere.first()?;

    let warnings = elsewhere
        .iter()
        .flat_map(|(engine, images)| {
            images.iter().map(move |image| {
                format!(
                    "image '{image}' is in {engine}'s local image store but not {active}'s — \
                     snouty is using {active}, and podman and docker keep separate image stores"
                )
            })
        })
        .collect();

    let suggestion = format!(
        "to use {source} instead, set SNOUTY_CONTAINER_ENGINE={source} \
         (or add `container_engine = \"{source}\"` to a snouty settings file)"
    );

    Some((warnings, suggestion))
}

/// Layer launch's escape hatch onto a [`validate_images_are_available`] failure:
/// a caller that already has a pre-built config image can skip local packaging
/// entirely by launching with `--config-image <ref>`. Because `--config-image`
/// conflicts with `--config`, the suggestion tells users to *replace* `--config`,
/// not add the flag alongside it. Only the launch path has this alternative, so
/// only the launch caller wraps its check with this.
fn with_config_image_escape_hatch<T>(result: Result<T>) -> Result<T> {
    result.with_suggestion(|| {
        "if you already have a pre-built config image, launch with `--config-image <ref>` \
         in place of `--config <dir>` to reuse it and skip local packaging"
    })
}

/// Ensure the given image references all use the amd64 architecture.
fn validate_image_architectures<R>(runtime: &R, images: &[&str]) -> Result<()>
where
    R: ContainerRuntime + ?Sized,
{
    let mut seen = HashSet::new();
    let mut unsupported = Vec::new();

    for image in images {
        if !seen.insert(*image) {
            continue;
        }

        let arch = runtime.image_architecture(image)?;
        if arch != "amd64" {
            unsupported.push(format!("image '{image}' has architecture '{arch}'"));
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

/// A container reported by `compose ps`, with just the fields snouty needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeContainer {
    /// Compose service name (e.g. `"app"`).
    pub service: String,
    /// Container ID (whatever the runtime emitted — short or full).
    pub id: String,
    /// Whether the container's entrypoint has exited. Antithesis can't run
    /// test commands in stopped containers, so validate flags any service
    /// that has test commands defined but ended up in this state. Only
    /// `exited` and `dead` count as stopped — transient states like
    /// `created`, `restarting`, `paused`, and missing State are treated as
    /// not-stopped to avoid false positives on healthy setups still settling.
    pub stopped: bool,
}

/// Determine whether a `State` field value reports a stopped container.
/// Only `exited` and `dead` qualify — every other value (including missing
/// `State`, transient `created`/`restarting`/`paused`, and human-readable
/// forms like "Up 5 seconds") is treated as not-stopped.
fn state_is_stopped(state: Option<&str>) -> bool {
    match state {
        Some(s) => s.eq_ignore_ascii_case("exited") || s.eq_ignore_ascii_case("dead"),
        None => false,
    }
}

/// Parse the JSON output of `docker-compose ps --format json`.
///
/// Handles both NDJSON (one object per line) and JSON array formats. The
/// schema is Docker Compose v2: `{"Service": "...", "ID": "...", "State": "running"}`.
fn parse_compose_ps(stdout: &str) -> Result<Vec<ComposeContainer>> {
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
                .and_then(|v| v.as_str())
                .ok_or_else(|| eyre!("missing container ID in compose ps output"))?;

            let service = v
                .get("Service")
                .and_then(|v| v.as_str())
                .ok_or_else(|| eyre!("missing service name in compose ps output"))?;

            let state = v.get("State").and_then(|v| v.as_str());

            Ok(ComposeContainer {
                service: service.to_string(),
                id: id.to_string(),
                stopped: state_is_stopped(state),
            })
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
    use hegel::generators::{self, Generator};
    use std::collections::BTreeMap;
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;

    /// A text generator whose output never contains `@` — for digest values,
    /// which never carry an `@` in practice (`sha256:...`), so the
    /// re-pinning/idempotence reasoning holds.
    fn no_at() -> impl Generator<String> {
        generators::text().filter(|s: &String| !s.contains('@'))
    }

    /// Expanding a repository to its canonical `registry/path` form is stable:
    /// normalizing an already-normalized repo is a no-op. Run over arbitrary
    /// text so odd inputs (empty, leading slash, multi-byte) are covered too.
    #[hegel::test]
    fn normalize_repo_is_idempotent(tc: hegel::TestCase) {
        let repo = tc.draw(generators::text());
        let once = normalize_repo(&repo);
        let twice = normalize_repo(&once);
        assert_eq!(once, twice);
    }

    /// `image_repo` and `image_ref_tag` slice on byte offsets found with
    /// `rfind`; on arbitrary (multi-byte) input they must never panic on a
    /// non-char-boundary, and the repo must always be a prefix of the input.
    #[hegel::test]
    fn image_repo_is_a_prefix_and_never_panics(tc: hegel::TestCase) {
        let image_ref = tc.draw(generators::text());
        let repo = image_repo(&image_ref);
        assert!(
            image_ref.starts_with(repo),
            "repo {repo:?} is not a prefix of {image_ref:?}"
        );
        // Just exercising the slicing — the assertion is "it returned".
        let _ = image_ref_tag(&image_ref);
    }

    /// Re-pinning replaces the digest rather than appending a second one:
    /// pinning to `d1` then `d2` is the same as pinning to `d2` directly, and
    /// the result always ends with `@{d2}`.
    #[hegel::test]
    fn pinned_image_ref_replaces_digest(tc: hegel::TestCase) {
        let image_ref = tc.draw(no_at());
        let d1 = tc.draw(no_at());
        let d2 = tc.draw(no_at());
        let repinned = pinned_image_ref(&pinned_image_ref(&image_ref, &d1), &d2);
        assert_eq!(repinned, pinned_image_ref(&image_ref, &d2));
        assert!(repinned.ends_with(&format!("@{d2}")));
    }

    /// Stripping the tag from a digest-pinned reference is stable: a reference
    /// already in `name@digest` form is left unchanged by a second pass.
    #[hegel::test]
    fn digest_only_ref_is_idempotent(tc: hegel::TestCase) {
        let image_ref = tc.draw(generators::text());
        let once = digest_only_ref(&image_ref);
        assert_eq!(digest_only_ref(&once), once);
    }

    /// Classifying `manifest inspect` output must never panic on arbitrary
    /// bytes — malformed JSON classifies as `NotFound`, never a crash.
    #[hegel::test]
    fn classify_manifest_json_never_panics(tc: hegel::TestCase) {
        let bytes = tc.draw(generators::binary());
        let _ = classify_manifest_json(&bytes);
    }

    /// Parsing a `docker push` digest line must never panic on arbitrary text —
    /// it returns an error when no `sha256:` digest is present, never a crash.
    #[hegel::test]
    fn parse_docker_push_digest_never_panics(tc: hegel::TestCase) {
        let stdout = tc.draw(generators::text());
        let _ = parse_docker_push_digest(&stdout);
    }

    #[tokio::test]
    async fn build_and_push_to_mock_registry() {
        let runtimes = require_runtimes();
        if runtimes.is_empty() {
            return;
        }

        for rt in &runtimes {
            eprintln!("testing with runtime: {}", rt.name());
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

            // The config image must always be amd64: Antithesis does not support
            // arm64, and this build runs unchanged on arm hosts.
            let arch = rt
                .image_architecture(&image_ref)
                .unwrap_or_else(|e| panic!("{}: {e:?}", rt.name()));
            assert_eq!(arch, "amd64", "{}: config image must be amd64", rt.name());

            // Clean up the local image.
            let _ = Command::new(rt.name()).args(["rmi", &image_ref]).output();
        }
    }

    fn cc(service: &str, id: &str, stopped: bool) -> ComposeContainer {
        ComposeContainer {
            service: service.to_string(),
            id: id.to_string(),
            stopped,
        }
    }

    #[test]
    fn parse_compose_ps_ndjson() {
        let stdout = "{\"ID\":\"abc123\",\"Service\":\"app\",\"State\":\"running\"}\n\
                      {\"ID\":\"def456\",\"Service\":\"sidecar\",\"State\":\"exited\"}\n";
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![cc("app", "abc123", false), cc("sidecar", "def456", true)]
        );
    }

    #[test]
    fn parse_compose_ps_json_array() {
        let stdout = r#"[
            {"ID":"abc123","Service":"app","State":"running"},
            {"ID":"def456","Service":"sidecar","State":"running"}
        ]"#;
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![cc("app", "abc123", false), cc("sidecar", "def456", false)]
        );
    }

    #[test]
    fn parse_compose_ps_empty() {
        let result = parse_compose_ps("").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_compose_ps_missing_state_is_not_stopped() {
        // A container with no State field — typical during early startup —
        // must NOT be classified as stopped, or the stranded-test-commands
        // diagnostic fires on healthy containers.
        let stdout = r#"[{"ID":"abc","Service":"app"}]"#;
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(result, vec![cc("app", "abc", false)]);
    }

    #[test]
    fn parse_compose_ps_transient_states_are_not_stopped() {
        // created / restarting / paused are not "stopped" — Antithesis may
        // still see them recover. Only `exited` and `dead` count as stopped.
        let stdout = r#"[
            {"ID":"a","Service":"svc","State":"created"},
            {"ID":"b","Service":"svc","State":"restarting"},
            {"ID":"c","Service":"svc","State":"paused"},
            {"ID":"d","Service":"svc","State":"Up 5 seconds"},
            {"ID":"e","Service":"svc","State":"dead"},
            {"ID":"f","Service":"svc","State":"EXITED"}
        ]"#;
        let result = parse_compose_ps(stdout).unwrap();
        let stopped: Vec<(&str, bool)> =
            result.iter().map(|c| (c.id.as_str(), c.stopped)).collect();
        assert_eq!(
            stopped,
            vec![
                ("a", false),
                ("b", false),
                ("c", false),
                ("d", false),
                ("e", true),
                ("f", true),
            ]
        );
    }

    #[test]
    fn parse_compose_ps_returns_one_entry_per_replica() {
        // Scaled services (`replicas: N`) emit one entry per container, all
        // sharing the same Service value but with distinct IDs. Validate
        // keys per-container work by container.id rather than service.
        let stdout = r#"[
            {"ID":"a1","Service":"worker","State":"running"},
            {"ID":"a2","Service":"worker","State":"running"},
            {"ID":"a3","Service":"worker","State":"exited"}
        ]"#;
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![
                cc("worker", "a1", false),
                cc("worker", "a2", false),
                cc("worker", "a3", true),
            ]
        );
    }

    #[test]
    fn stderr_indicates_missing_source_matches_docker_and_podman() {
        assert!(stderr_indicates_missing_source(
            "Error response from daemon: Could not find the file /opt/x in container abc"
        ));
        assert!(stderr_indicates_missing_source(
            "Error: \"/opt/antithesis/test/v1\": no such file or directory"
        ));
        // Podman 4.x phrasings.
        assert!(stderr_indicates_missing_source(
            "Error: \"/opt/antithesis/test/v1\" does not exist in container"
        ));
        assert!(stderr_indicates_missing_source(
            "Error: /opt/antithesis/test/v1 could not be found on container abc"
        ));
        // Real cp errors must not be misclassified.
        assert!(!stderr_indicates_missing_source("Error: permission denied"));
        // The loose "no such file" (without "or directory") is deliberately
        // excluded — it matches destination-side errors and shadows real
        // failures.
        assert!(!stderr_indicates_missing_source(
            "Error: writing to destination: no such file"
        ));
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
                    default_image: false,
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "us-central1-docker.pkg.dev/proj/repo/sidecar:latest".to_string(),
                    default_image: false,
                },
            ]
        );
        assert_eq!(
            contents.build_services,
            HashSet::from(["builder".to_string()])
        );
        // No top-level `name:` → the builder service can't be resolved to
        // compose's default build tag.
        assert_eq!(contents.services_without_image, vec!["builder".to_string()]);
        assert!(contents.networks.is_empty());
    }

    #[test]
    fn parse_compose_config_synthesizes_default_build_tags() {
        // `docker-compose config` output always carries the resolved project
        // name; build-only services resolve to `<project>-<service>:latest`,
        // exactly the tag `docker compose build` produces.
        let yaml = "\
name: myproj
services:
  app:
    image: myapp:latest
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
                    image: "myapp:latest".to_string(),
                    default_image: false,
                },
                ComposeService {
                    name: "builder".to_string(),
                    image: "myproj-builder:latest".to_string(),
                    default_image: true,
                },
            ]
        );
        assert!(contents.services_without_image.is_empty());
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

            let compose = docker_compose(rt.as_ref()).unwrap();
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

            let compose = docker_compose(rt.as_ref()).unwrap();
            let config = match crate::config::detect_config(dir.path()).unwrap() {
                crate::config::Config::Compose(c) => c,
                other => panic!("expected Compose, got {other:?}"),
            };
            let yaml = compose.config_yaml(&config, Some(&overlay), &[]).unwrap();
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

    #[test]
    fn rewrite_compose_images_pins_every_service() {
        // Input mirrors `docker-compose config --no-interpolate` output:
        // machine-generated YAML where every service must end up pinned.
        // Non-image fields (build, volumes, environment) are preserved.
        let yaml = "\
services:
  app:
    build:
      context: .
    image: ${REPO}/app:${TAG}
    volumes:
      - ./data:/data
  nginx:
    image: docker.io/library/nginx:latest
    environment:
      FOO: bar
";
        let pinned = BTreeMap::from([
            (
                "app".to_string(),
                "reg.example.com/app:v1@sha256:aaa".to_string(),
            ),
            (
                "nginx".to_string(),
                "docker.io/library/nginx:latest@sha256:bbb".to_string(),
            ),
        ]);

        let out = rewrite_compose_images(yaml, &pinned).unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        let services = doc.get("services").unwrap();

        let image = |svc: &str| {
            services
                .get(svc)
                .and_then(|s| s.get("image"))
                .and_then(|i| i.as_str())
                .unwrap()
                .to_string()
        };
        assert_eq!(image("app"), "reg.example.com/app:v1@sha256:aaa");
        assert_eq!(image("nginx"), "docker.io/library/nginx:latest@sha256:bbb");
        // Surrounding structure is preserved.
        assert!(services.get("app").unwrap().get("build").is_some());
        assert!(services.get("app").unwrap().get("volumes").is_some());
        assert!(services.get("nginx").unwrap().get("environment").is_some());
    }

    #[test]
    fn rewrite_compose_images_rejects_unpinned_service() {
        // A service the pinning pass lost track of must fail loudly instead of
        // shipping an unpinned image reference to the platform.
        let yaml = "\
services:
  app:
    image: app:latest
  forgotten:
    image: forgotten:latest
";
        let pinned = BTreeMap::from([(
            "app".to_string(),
            "reg.example.com/app:latest@sha256:aaa".to_string(),
        )]);

        let err = rewrite_compose_images(yaml, &pinned).unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("'forgotten'") && msg.contains("bug in snouty"),
            "expected the unpinned service to be flagged as a bug, got: {msg}"
        );
    }

    #[test]
    fn normalize_repo_expands_docker_hub_shorthand() {
        assert_eq!(normalize_repo("nginx"), "docker.io/library/nginx");
        assert_eq!(normalize_repo("user/app"), "docker.io/user/app");
        assert_eq!(normalize_repo("docker.io/nginx"), "docker.io/library/nginx");
        assert_eq!(
            normalize_repo("index.docker.io/library/nginx"),
            "docker.io/library/nginx"
        );
        assert_eq!(normalize_repo("localhost:5000/app"), "localhost:5000/app");
        assert_eq!(normalize_repo("ghcr.io/org/app"), "ghcr.io/org/app");
    }

    #[test]
    fn image_repo_and_tag_split_references() {
        assert_eq!(image_repo("redis:7"), "redis");
        assert_eq!(image_ref_tag("redis:7"), "7");
        assert_eq!(image_repo("nginx"), "nginx");
        assert_eq!(image_ref_tag("nginx"), "latest");
        assert_eq!(image_repo("localhost:5000/app:v1"), "localhost:5000/app");
        assert_eq!(image_ref_tag("localhost:5000/app:v1"), "v1");
        assert_eq!(image_repo("localhost:5000/app"), "localhost:5000/app");
        assert_eq!(image_ref_tag("localhost:5000/app"), "latest");
        assert_eq!(
            image_repo("ghcr.io/org/app:v2@sha256:abc"),
            "ghcr.io/org/app"
        );
        assert_eq!(image_ref_tag("ghcr.io/org/app:v2@sha256:abc"), "v2");
    }

    #[test]
    fn digests_for_repo_matches_normalized_repos() {
        let entries = vec![
            "redis@sha256:child".to_string(),
            "docker.io/library/redis@sha256:list".to_string(),
            "mirror.example.com/redis@sha256:other".to_string(),
        ];
        // Both Hub spellings match each other; the mirror doesn't.
        assert_eq!(
            digests_for_repo("docker.io/library/redis", &entries),
            vec!["sha256:child".to_string(), "sha256:list".to_string()]
        );
        assert_eq!(
            digests_for_repo("mirror.example.com/redis", &entries),
            vec!["sha256:other".to_string()]
        );
        assert!(digests_for_repo("ghcr.io/redis", &entries).is_empty());
    }

    #[test]
    fn classify_manifest_json_shapes() {
        let list_amd64 = br#"{"manifests": [{"platform": {"architecture": "arm64"}}, {"platform": {"architecture": "amd64"}}]}"#;
        assert_eq!(
            classify_manifest_json(list_amd64),
            RemoteManifest::List { has_amd64: true }
        );
        let list_arm = br#"{"manifests": [{"platform": {"architecture": "arm64"}}]}"#;
        assert_eq!(
            classify_manifest_json(list_arm),
            RemoteManifest::List { has_amd64: false }
        );
        let single = br#"{"schemaVersion": 2, "config": {}, "layers": []}"#;
        assert_eq!(classify_manifest_json(single), RemoteManifest::Single);
        assert_eq!(
            classify_manifest_json(b"not json"),
            RemoteManifest::NotFound
        );
    }

    /// Run pin_images over `yaml` with a [`FakeRuntime`] (real docker-compose
    /// binary for config resolution, fake image/registry operations).
    fn pin_with_fake(rt: &FakeRuntime, yaml: &str, registry: &str) -> Result<String> {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yaml"), yaml).unwrap();
        let config = match crate::config::detect_config(dir.path()).unwrap() {
            crate::config::Config::Compose(c) => c,
            other => panic!("expected Compose, got {other:?}"),
        };
        let compose = docker_compose(rt).unwrap();
        compose.pin_images(&config, registry)
    }

    #[test]
    fn pin_images_skips_push_when_registry_serves_digest() {
        if !has_compose() {
            // Loud in CI (skip_or_fail panics there) so a runner missing
            // docker-compose can't silently drop this coverage.
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // A pulled third-party image: the registry confirms the local digest
        // (a multi-arch list with amd64), so it's pinned there without a push.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("redis:7".to_string(), true)]),
            repo_digests: BTreeMap::from([(
                "redis:7".to_string(),
                vec![
                    // The per-arch entry can't be verified (podman can't
                    // inspect single manifests) — the list entry can.
                    "docker.io/library/redis@sha256:child".to_string(),
                    "docker.io/library/redis@sha256:list".to_string(),
                ],
            )]),
            remote_manifests: BTreeMap::from([(
                "docker.io/library/redis@sha256:list".to_string(),
                RemoteManifest::List { has_amd64: true },
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: redis:7\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("docker.io/library/redis:7@sha256:list"),
            "expected the verified list digest pin, got: {out}"
        );
        assert!(
            rt.pushed.lock().unwrap().is_empty(),
            "nothing should be pushed"
        );
    }

    #[test]
    fn pin_images_pushes_when_no_registry_serves_digest() {
        if !has_compose() {
            // Loud in CI (skip_or_fail panics there) so a runner missing
            // docker-compose can't silently drop this coverage.
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // The local store fabricates digest entries for registry-qualified
        // tags that were never pushed; the registry round trip rejects them
        // (NotFound) and the image is pushed to our registry instead.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("ghcr.io/org/app:v1".to_string(), true)]),
            architectures: BTreeMap::from([(
                "reg.example.com/ghcr.io/org/app:v1".to_string(),
                "amd64".to_string(),
            )]),
            repo_digests: BTreeMap::from([(
                "ghcr.io/org/app:v1".to_string(),
                vec!["ghcr.io/org/app@sha256:fabricated".to_string()],
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: ghcr.io/org/app:v1\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("reg.example.com/ghcr.io/org/app:v1@sha256:fakepushdigest"),
            "expected push-pinned reference, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["reg.example.com/ghcr.io/org/app:v1".to_string()]
        );
    }

    #[test]
    fn pin_images_skips_push_for_previously_pushed_image() {
        if !has_compose() {
            // Loud in CI (skip_or_fail panics there) so a runner missing
            // docker-compose can't silently drop this coverage.
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // A bare local image pushed by an earlier launch: the
        // registry-prefixed candidate verifies, so no re-push. The manifest
        // is single-platform, so the local architecture must be amd64.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("myapp:latest".to_string(), true)]),
            architectures: BTreeMap::from([("myapp:latest".to_string(), "amd64".to_string())]),
            repo_digests: BTreeMap::from([(
                "myapp:latest".to_string(),
                vec!["reg.example.com/myapp@sha256:pushedearlier".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "reg.example.com/myapp@sha256:pushedearlier".to_string(),
                RemoteManifest::Single,
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: myapp:latest\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("reg.example.com/myapp:latest@sha256:pushedearlier"),
            "expected pin to the previously pushed digest, got: {out}"
        );
        assert!(
            rt.pushed.lock().unwrap().is_empty(),
            "nothing should be pushed"
        );
    }

    #[test]
    fn pin_images_rejects_arm_only_images() {
        if !has_compose() {
            // Loud in CI (skip_or_fail panics there) so a runner missing
            // docker-compose can't silently drop this coverage.
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // The registry serves the digest, but only as arm (a list without an
        // amd64 entry). The pin is refused and the push path's local arch
        // check produces the amd64 guidance before anything is pushed.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("armthing:latest".to_string(), true)]),
            architectures: BTreeMap::from([
                ("armthing:latest".to_string(), "arm64".to_string()),
                (
                    "reg.example.com/armthing:latest".to_string(),
                    "arm64".to_string(),
                ),
            ]),
            repo_digests: BTreeMap::from([(
                "armthing:latest".to_string(),
                vec!["docker.io/library/armthing@sha256:armlist".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "docker.io/library/armthing@sha256:armlist".to_string(),
                RemoteManifest::List { has_amd64: false },
            )]),
            ..Default::default()
        };
        let err = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: armthing:latest\n",
            "reg.example.com",
        )
        .unwrap_err();
        let debug = format!("{err:?}");
        assert!(
            debug.contains("amd64"),
            "expected amd64 guidance, got: {debug}"
        );
        assert!(
            rt.pushed.lock().unwrap().is_empty(),
            "nothing should be pushed"
        );
    }

    #[test]
    fn pin_images_pushes_every_local_image() {
        let runtimes = require_runtimes_with_compose();
        if runtimes.is_empty() {
            return;
        }

        for rt in &runtimes {
            eprintln!("testing with runtime: {}", rt.name());
            let registry = match OCIRegistry::start(rt.as_ref()) {
                Some(r) => r,
                None => continue,
            };
            let addr = registry.host_port();
            let compose = docker_compose(rt.as_ref())
                .unwrap_or_else(|e| panic!("{}: docker_compose: {e:?}", rt.name()));

            // Build a purely-local image (present locally, in no registry).
            let img_dir = tempfile::tempdir().unwrap();
            std::fs::write(
                img_dir.path().join("Dockerfile"),
                "FROM scratch\nCOPY . /\n",
            )
            .unwrap();
            std::fs::write(img_dir.path().join("file"), "x").unwrap();
            let local = "snouty-pin-test:latest";
            rt.build_image(img_dir.path(), local, None, Some("linux/amd64"))
                .unwrap_or_else(|e| panic!("{}: build: {e:?}", rt.name()));

            // Resolve `app`'s pinned image after running pin_images over `yaml`.
            let pinned_app = |yaml: &str| -> Result<String> {
                let dir = tempfile::tempdir().unwrap();
                std::fs::write(dir.path().join("docker-compose.yaml"), yaml).unwrap();
                let config = match crate::config::detect_config(dir.path()).unwrap() {
                    crate::config::Config::Compose(c) => c,
                    other => panic!("expected Compose, got {other:?}"),
                };
                let out = compose.pin_images(&config, &addr)?;
                Ok(serde_yaml::from_str::<serde_yaml::Value>(&out)
                    .unwrap()
                    .get("services")
                    .and_then(|s| s.get("app"))
                    .and_then(|s| s.get("image"))
                    .and_then(|i| i.as_str())
                    .unwrap()
                    .to_string())
            };
            let pushed_prefix = format!("{addr}/snouty-pin-test:latest@sha256:");

            // Case 1 — build stanza: the local build is pushed and pinned.
            let built = pinned_app(&format!(
                "services:\n  app:\n    build: .\n    image: {local}\n"
            ))
            .unwrap_or_else(|e| panic!("{}: build case: {e:?}", rt.name()));
            assert!(
                built.starts_with(&pushed_prefix),
                "{}: build image should be pushed, got: {built}",
                rt.name()
            );

            // Case 2 — local without a build stanza (prebuilt/loaded out of
            // band): local availability is enough; pushed and pinned the same.
            let local_only = pinned_app(&format!("services:\n  app:\n    image: {local}\n"))
                .unwrap_or_else(|e| panic!("{}: local-only case: {e:?}", rt.name()));
            assert!(
                local_only.starts_with(&pushed_prefix),
                "{}: local-only image should be pushed, got: {local_only}",
                rt.name()
            );

            // Case 3 — not present locally: hard error before anything is
            // pushed. snouty never pulls, even for registry-qualified refs.
            let err = pinned_app("services:\n  app:\n    image: snouty-bare-local-xyz:latest\n")
                .expect_err(&format!("{}: expected pin_images to fail", rt.name()));
            let debug = format!("{err:?}");
            assert!(
                debug.contains("snouty-bare-local-xyz:latest")
                    && debug.contains("not available locally"),
                "{}: error should name the missing image, got: {debug}",
                rt.name()
            );

            // Case 4 — `build:`-only service with no `image:` resolves to
            // compose's default build tag (`<project>-<service>:latest`);
            // when that tag was never built, the error names it with
            // guidance instead of silently launching an image the platform
            // can't pull.
            let err = pinned_app("services:\n  app:\n    build: .\n").expect_err(&format!(
                "{}: expected unbuilt default-tag service to fail",
                rt.name()
            ));
            let debug = format!("{err:?}");
            assert!(
                debug.contains("-app:latest") && debug.contains("default build tag"),
                "{}: error should name the default build tag, got: {debug}",
                rt.name()
            );

            let _ = Command::new(rt.name())
                .args(["rmi", local, &format!("{addr}/{local}")])
                .output();
        }
    }

    #[derive(Clone, Default)]
    struct FakeRuntime {
        available_images: BTreeMap<String, bool>,
        architectures: BTreeMap<String, String>,
        repo_digests: BTreeMap<String, Vec<String>>,
        remote_manifests: BTreeMap<String, RemoteManifest>,
        pushed: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ContainerRuntime for FakeRuntime {
        fn name(&self) -> &str {
            "fake"
        }

        fn engine_kind(&self) -> &'static str {
            "fake"
        }

        fn clone_box(&self) -> Box<dyn ContainerRuntime> {
            Box::new(self.clone())
        }

        fn image_push(&self, image_ref: &str) -> Result<String> {
            self.pushed.lock().unwrap().push(image_ref.to_string());
            Ok(pinned_image_ref(image_ref, "sha256:fakepushdigest"))
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

        fn image_repo_digests(&self, image_ref: &str) -> Result<Vec<String>> {
            Ok(self
                .repo_digests
                .get(image_ref)
                .cloned()
                .unwrap_or_default())
        }

        fn image_tag(&self, _src: &str, _dst: &str) -> Result<()> {
            Ok(())
        }

        fn remote_manifest(&self, image_ref: &str) -> RemoteManifest {
            self.remote_manifests
                .get(image_ref)
                .cloned()
                .unwrap_or(RemoteManifest::NotFound)
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
            ..Default::default()
        };

        validate_image_architectures(&runtime, &["app:latest", "sidecar:latest"]).unwrap();
    }

    #[test]
    fn validate_image_architectures_rejects_non_amd64_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::new(),
            architectures: BTreeMap::from([
                ("app:latest".to_string(), "arm64".to_string()),
                ("sidecar:latest".to_string(), "amd64".to_string()),
            ]),
            ..Default::default()
        };

        let err =
            validate_image_architectures(&runtime, &["app:latest", "sidecar:latest"]).unwrap_err();

        let msg = err.to_string();
        let debug = format!("{err:?}");
        assert!(
            msg.contains("x86-64 (amd64)"),
            "expected architecture guidance, got: {msg}"
        );
        assert!(
            debug.contains("image 'app:latest' has architecture 'arm64'"),
            "expected offending image details, got: {debug}"
        );
    }

    /// Build a [`ComposeContents`] from `(service, image)` pairs and the names
    /// of services that have a `build:` stanza.
    fn contents_of(services: &[(&str, &str)], build_services: &[&str]) -> ComposeContents {
        ComposeContents {
            services: services
                .iter()
                .map(|(name, image)| ComposeService {
                    name: name.to_string(),
                    image: image.to_string(),
                    default_image: false,
                })
                .collect(),
            services_without_image: Vec::new(),
            build_services: build_services.iter().map(|s| s.to_string()).collect(),
            networks: Vec::new(),
        }
    }

    #[test]
    fn validate_images_are_available_accepts_local_images() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::from([
                ("app:latest".to_string(), true),
                ("sidecar:latest".to_string(), true),
            ]),
            architectures: BTreeMap::new(),
            ..Default::default()
        };

        validate_images_are_available(
            &runtime,
            &contents_of(&[("app", "app:latest"), ("sidecar", "sidecar:latest")], &[]),
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
            ..Default::default()
        };

        let err = validate_images_are_available(
            &runtime,
            &contents_of(
                &[
                    ("present", "present:latest"),
                    ("app", "missing-a:latest"),
                    ("sidecar", "missing-b:latest"),
                ],
                &["sidecar"],
            ),
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
            debug.contains("image: missing-b:latest (service has a `build:` stanza"),
            "expected build-stanza hint on the second missing image, got: {debug}"
        );
        assert!(
            debug.contains("snouty never pulls"),
            "expected the why-it's-required-locally note, got: {debug}"
        );
        // The shared check is context-free: the launch-only `--config-image`
        // escape hatch is layered on by the caller, not emitted here.
        assert!(
            !debug.contains("--config-image"),
            "shared check should stay context-free, got: {debug}"
        );
    }

    #[test]
    fn with_config_image_escape_hatch_tells_users_to_replace_config() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::from([("missing:latest".to_string(), false)]),
            architectures: BTreeMap::new(),
            ..Default::default()
        };

        let err = with_config_image_escape_hatch(validate_images_are_available(
            &runtime,
            &contents_of(&[("app", "missing:latest")], &[]),
        ))
        .unwrap_err();

        let debug = format!("{err:?}");
        assert!(
            debug.contains("--config-image <ref>"),
            "expected the config-image escape hatch, got: {debug}"
        );
        // --config-image conflicts with --config, so the hint must say to replace
        // it, not add it alongside (which clap would reject).
        assert!(
            debug.contains("in place of `--config <dir>`"),
            "expected the hint to replace --config, not add it, got: {debug}"
        );
    }

    #[test]
    fn validate_images_are_available_rejects_imageless_services() {
        let runtime = FakeRuntime {
            available_images: BTreeMap::new(),
            architectures: BTreeMap::new(),
            ..Default::default()
        };

        let mut contents = contents_of(&[], &["app"]);
        contents.services_without_image = vec!["app".to_string()];

        let err = validate_images_are_available(&runtime, &contents).unwrap_err();
        let debug = format!("{err:?}");
        assert!(
            debug.contains("service 'app' has no `image:` field"),
            "expected imageless-service guidance, got: {debug}"
        );
        // The imageless check bails before the missing-local-image guidance, so
        // its unrelated "pull or build" suggestion must not leak onto this error.
        assert!(
            !debug.contains("pull or build the missing images"),
            "imageless error should not carry missing-image guidance, got: {debug}"
        );
    }

    #[test]
    fn cross_engine_guidance_is_none_when_nothing_found_elsewhere() {
        assert!(cross_engine_guidance("podman", &[]).is_none());
    }

    #[test]
    fn cross_engine_guidance_names_the_engine_and_override() {
        let elsewhere = vec![(
            "docker".to_string(),
            vec!["local-benchmark-driver:local".to_string()],
        )];
        let (warnings, suggestion) = cross_engine_guidance("podman", &elsewhere).unwrap();

        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("local-benchmark-driver:local")
                && warnings[0].contains("docker's local image store")
                && warnings[0].contains("snouty is using podman"),
            "unexpected warning: {}",
            warnings[0]
        );
        assert!(
            suggestion.contains("SNOUTY_CONTAINER_ENGINE=docker")
                && suggestion.contains("container_engine = \"docker\""),
            "expected the engine override and setting, got: {suggestion}"
        );
        // We point at the override, not at copying images between stores.
        assert!(
            !suggestion.contains("save") && !suggestion.contains("load"),
            "suggestion should not include a copy command, got: {suggestion}"
        );
    }

    #[test]
    fn cross_engine_guidance_warns_once_per_image() {
        let elsewhere = vec![(
            "docker".to_string(),
            vec!["a:latest".to_string(), "b:latest".to_string()],
        )];
        let (warnings, suggestion) = cross_engine_guidance("podman", &elsewhere).unwrap();

        assert_eq!(warnings.len(), 2);
        assert!(
            suggestion.contains("SNOUTY_CONTAINER_ENGINE=docker"),
            "expected the engine override, got: {suggestion}"
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
    fn pinned_image_ref_keeps_tag() {
        assert_eq!(
            pinned_image_ref("example.com/foo/image:v1", "sha256:abc123"),
            "example.com/foo/image:v1@sha256:abc123"
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
    fn pinned_image_ref_with_port_keeps_tag() {
        assert_eq!(
            pinned_image_ref("localhost:5000/image:latest", "sha256:abc123"),
            "localhost:5000/image:latest@sha256:abc123"
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
    fn pinned_image_ref_host_port_nested_path_keeps_tag() {
        assert_eq!(
            pinned_image_ref("myregistry:5000/org/repo/image:v2", "sha256:abc123"),
            "myregistry:5000/org/repo/image:v2@sha256:abc123"
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
    fn digest_only_ref_strips_tag() {
        assert_eq!(
            digest_only_ref("example.com/foo/snouty-config:20260615-abcd@sha256:abc123"),
            "example.com/foo/snouty-config@sha256:abc123"
        );
    }

    #[test]
    fn digest_only_ref_with_port_strips_tag() {
        assert_eq!(
            digest_only_ref("localhost:5000/snouty-config:latest@sha256:abc123"),
            "localhost:5000/snouty-config@sha256:abc123"
        );
    }

    #[test]
    fn digest_only_ref_already_digest_only() {
        assert_eq!(
            digest_only_ref("example.com/foo/snouty-config@sha256:abc123"),
            "example.com/foo/snouty-config@sha256:abc123"
        );
    }

    #[test]
    fn digest_only_ref_no_digest_unchanged() {
        assert_eq!(
            digest_only_ref("example.com/foo/snouty-config:v1"),
            "example.com/foo/snouty-config:v1"
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
                    default_image: false,
                },
                ComposeService {
                    name: "sidecar".to_string(),
                    image: "docker.io/library/nginx:latest".to_string(),
                    default_image: false,
                },
            ]
        );
        assert_eq!(contents.build_services, HashSet::from(["app".to_string()]));
    }

    #[test]
    fn is_compose_v2_version_accepts_docker_compose() {
        assert!(is_compose_v2_version("Docker Compose version v2.24.5"));
        assert!(is_compose_v2_version("docker compose version 5.1.4"));
    }

    #[test]
    fn is_compose_v2_version_rejects_others() {
        assert!(!is_compose_v2_version("podman-compose version 1.0.6"));
        assert!(!is_compose_v2_version(""));
    }

    #[test]
    fn config_json_hermetic_env_scrubs_process_variables() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("docker-compose.yaml"),
            "\
services:
  app:
    image: alpine
    environment:
      HOME_VALUE: \"${HOME}\"
      PATH_VALUE: \"${PATH}\"
      DOCKER_HOST_VALUE: \"${DOCKER_HOST}\"
",
        )
        .unwrap();
        let config = match crate::config::detect_config(dir.path()).unwrap() {
            crate::config::Config::Compose(config) => config,
            other => panic!("expected Compose config, got {other:?}"),
        };
        let runtime = FakeRuntime::default();
        let compose = DockerCompose {
            rt: &runtime,
            docker_host: Some("unix:///tmp/snouty-hermetic-test.sock".to_string()),
            compose_binary: docker_compose_binary().unwrap(),
        };

        let output = compose.config_json_hermetic_env(&config).unwrap();
        assert!(output.status.success(), "compose config failed: {output:?}");
        let resolved: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let environment = &resolved["services"]["app"]["environment"];
        assert_eq!(environment["HOME_VALUE"], "");
        assert_eq!(environment["PATH_VALUE"], "");
        assert_eq!(environment["DOCKER_HOST_VALUE"], "");
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

    #[test]
    fn output_with_timeout_returns_quick_command_output() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf hi; printf oops 1>&2; exit 3"]);
        let out = output_with_timeout(cmd, Duration::from_secs(10)).unwrap();
        assert_eq!(out.status.code(), Some(3));
        assert_eq!(out.stdout, b"hi");
        assert_eq!(out.stderr, b"oops");
    }

    #[test]
    fn output_with_timeout_kills_and_errors_on_overrun() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let err = output_with_timeout(cmd, Duration::from_millis(150)).unwrap_err();
        assert!(
            format!("{err}").contains("timed out"),
            "expected a timeout error, got: {err}"
        );
    }
}
