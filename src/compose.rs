//! Docker Compose v2 integration: resolving which Compose form is installed,
//! driving it as a typed API, and pinning service images for the platform.
//!
//! Runtime/registry mechanics live in [`crate::container`]; this module owns
//! everything that reads or manipulates a `docker-compose.yaml`.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};

use crate::config::ComposeConfig;
use crate::container::{
    Architecture, ContainerRuntime, DISCOVERY_COMMAND_TIMEOUT, RemoteManifest, available_engines,
    digests_for_repo, image_ref_tag, image_repo, is_podman_in_disguise, mirror_path,
    normalize_repo, registry_host, strip_registry,
};
use crate::error::user_error;
use crate::process::{ProcessGroupChild, output_with_timeout};

/// How Docker Compose v2 is invoked on this machine.
///
/// Compose v2 ships two ways — the standalone `docker-compose` binary and the
/// `docker compose` CLI plugin — and snouty drives whichever it finds. Modeling
/// the two as an enum (rather than a program plus a free-form argument prefix)
/// makes the wrong combinations unrepresentable: each variant fixes its own
/// invocation. The wrapped path is always absolute so the command survives the
/// `env_clear()` in the hermetic render, where `PATH` is gone.
#[derive(Clone, Debug)]
enum ComposeForm {
    /// The standalone `docker-compose` binary; the path is `docker-compose`.
    Standalone(PathBuf),
    /// The `docker compose` CLI plugin; the path is the `docker` binary.
    Plugin(PathBuf),
}

impl std::fmt::Display for ComposeForm {
    /// The invocation as a user would type it, for user-facing hints and errors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ComposeForm::Standalone(_) => "docker-compose",
            ComposeForm::Plugin(_) => "docker compose",
        })
    }
}

impl ComposeForm {
    /// A fresh [`Command`] for this invocation, positioned so callers append
    /// only the compose subcommand and its arguments. The program (and the
    /// plugin's leading `compose`) are fixed by the variant and can't be
    /// clobbered by later `args`.
    fn command(&self) -> Command {
        match self {
            ComposeForm::Standalone(program) => Command::new(program),
            ComposeForm::Plugin(program) => {
                let mut cmd = Command::new(program);
                cmd.arg("compose");
                cmd
            }
        }
    }

    /// Run `<form> version --short` and, if it is new enough for snouty, return
    /// the reported version; otherwise a clear error. This is the only place
    /// compose is version-probed — [`ComposeCli`] captures the result so nothing
    /// has to spawn `version` again.
    fn detect_supported(&self) -> Result<String> {
        let mut cmd = self.command();
        cmd.args(["version", "--short"]);
        let output = cmd
            .output()
            .wrap_err_with(|| format!("failed to run `{self} version`"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("`{self} version` failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
        match compose_version_parts(&version) {
            Some(parts) if parts >= MIN_COMPOSE_VERSION => Ok(version),
            _ => Err(eyre!(
                "`{self}` is Docker Compose {version}, but snouty requires v{} or newer",
                min_compose_version()
            ))
            .with_suggestion(|| "upgrade Docker Compose: https://docs.docker.com/compose/install/"),
        }
    }
}

/// A resolved Docker Compose v2 invocation and the version it reports.
///
/// [`resolve`](Self::resolve) both confirms v2 and captures the version in one
/// `version --short` call, so [`version`](Self::version) is a cheap accessor
/// rather than another subprocess.
#[derive(Clone, Debug)]
struct ComposeCli {
    form: ComposeForm,
    version: String,
}

impl std::fmt::Display for ComposeCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.form.fmt(f)
    }
}

impl ComposeCli {
    /// Resolve a usable Docker Compose invocation, with a clear error when none
    /// is available.
    ///
    /// Prefers the standalone `docker-compose` binary when it is present and new
    /// enough (the historical contract). Otherwise falls back to the `docker
    /// compose` CLI plugin — but only when `docker` is real Docker, never podman
    /// in disguise, whose compose provider may not implement the features snouty
    /// relies on.
    fn resolve() -> Result<ComposeCli> {
        // 1. Standalone docker-compose, when it meets the version bar.
        if let Ok(program) = which::which("docker-compose") {
            let form = ComposeForm::Standalone(program);
            match form.detect_supported() {
                Ok(version) => return Ok(ComposeCli { form, version }),
                // Present but too old (Compose v1, or a v2 below
                // MIN_COMPOSE_VERSION — a stale standalone alongside a current
                // plugin is a common Docker Desktop layout). Prefer the plugin
                // if it's usable; only surface the standalone's error if not.
                Err(standalone_err) => return Self::plugin().or(Err(standalone_err)),
            }
        }

        // 2. The `docker compose` CLI plugin. Preserve the plugin's own error
        // (podman-in-disguise, too old, or docker genuinely absent) as the
        // cause, rather than collapsing every case to a generic "not found".
        Self::plugin().map_err(move |plugin_err| {
            eyre!(
                "snouty requires Docker Compose v{} or newer, but neither the `docker-compose` binary nor the `docker compose` CLI plugin is usable",
                min_compose_version()
            )
            .with_section(move || format!("{plugin_err:#}").header("docker compose:"))
            .with_suggestion(|| {
                "install Docker Compose: https://docs.docker.com/compose/install/"
            })
        })
    }

    /// The `docker compose` CLI plugin, if usable. `Err` when docker is absent,
    /// is podman in disguise, or its compose plugin is too old — callers treat
    /// any of these as "no usable plugin".
    fn plugin() -> Result<ComposeCli> {
        let program = which::which("docker").wrap_err("`docker` not found on PATH")?;
        // podman-in-disguise routes `docker compose` to a provider that may not
        // implement Compose v2; never trust it as a source.
        if is_podman_in_disguise("docker") {
            bail!("`docker` is podman in disguise; its `compose` is not Docker Compose v2");
        }
        let form = ComposeForm::Plugin(program);
        let version = form.detect_supported()?;
        Ok(ComposeCli { form, version })
    }

    /// A fresh [`Command`] for this invocation; see [`ComposeForm::command`].
    fn command(&self) -> Command {
        self.form.command()
    }

    /// The Compose version captured during [`resolve`](Self::resolve) (e.g.
    /// `2.40.3`). No subprocess — it was recorded when v2 was confirmed.
    fn version(&self) -> &str {
        &self.version
    }
}

/// The lowest Docker Compose version snouty works with, as `(major, minor, patch)`.
///
/// Set by `compose config --no-path-resolution`, introduced in v2.18.0
/// ("introduce --no-path-resolution to skip relative path to be resolved",
/// docker/compose#10557). Without it compose rewrites every relative build
/// context and bind-mount source to an absolute path on the developer's
/// machine, and those paths get baked into the config image and shipped to
/// Antithesis, where they mean nothing.
///
/// The bar is v2.24.7 rather than v2.18.0 because the flag was ignored for
/// files pulled in with `include:` until then (docker/compose#11508). Since
/// `include:` resolves each file's relative paths against that file's own
/// directory, a project using it on v2.20 (where `include:` landed) through
/// v2.24.6 would pass a looser check and still ship absolute local paths —
/// silently, and only visible once the run is on the platform. A version check
/// that lets a broken config through is worse than no check.
const MIN_COMPOSE_VERSION: (u64, u64, u64) = (2, 24, 7);

/// `MIN_COMPOSE_VERSION` as it appears in messages: `2.24.7`.
fn min_compose_version() -> String {
    let (major, minor, patch) = MIN_COMPOSE_VERSION;
    format!("{major}.{minor}.{patch}")
}

/// The major, minor, and patch components of a `compose version --short`
/// string: `2.40.3` → `(2, 40, 3)`, `v2.24` → `(2, 24, 0)`. `None` when it
/// doesn't begin with a number.
fn compose_version_parts(version: &str) -> Option<(u64, u64, u64)> {
    // Any component may carry a distro or pre-release suffix (`40+ds1`,
    // `7-rc1`); keep its leading digits. A missing component reads as zero, so
    // `2.24` is `(2, 24, 0)` — correctly below `(2, 24, 7)`.
    fn leading_number(part: Option<&str>) -> Option<u64> {
        match part {
            Some(part) => part
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok(),
            None => Some(0),
        }
    }

    let mut parts = version.trim_start_matches('v').split('.');
    let major = leading_number(Some(parts.next()?))?;
    let minor = leading_number(parts.next())?;
    let patch = leading_number(parts.next())?;
    Some((major, minor, patch))
}

/// Variables that Antithesis sets in the environment of the docker-compose
/// process and that a compose file can interpolate. Observed on tenant
/// `orbitinghail` (release 62.2, run `af47ad2e140c441e4a3fc795aa4b41c4-62-2`),
/// with a compose file that interpolated a list of common variable names.
/// Antithesis also sets platform-internal variables, which are left out.
/// `DOCKER_CONFIG` has its own handling, and compose sets `PWD` and
/// `COMPOSE_PROJECT_NAME` itself.
const ANTITHESIS_COMPOSE_ENV: &[&str] = &[
    "ANTITHESIS_OUTPUT_DIR",
    "DOCKER_HOST",
    "HOME",
    "LANG",
    "LOGNAME",
    "PATH",
    "SHELL",
    "TMPDIR",
    "USER",
];

/// The docker CLI config directory (which holds `cli-plugins/`): `$DOCKER_CONFIG`
/// if set, else `$HOME/.docker`. Read from snouty's own (un-scrubbed) environment.
/// `None` when neither is set, in which case plugin lookup falls back to the
/// system directories.
fn docker_config_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("DOCKER_CONFIG") {
        return Some(PathBuf::from(dir));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".docker"))
}
/// Drives Docker Compose v2, independent of which runtime built or pushed the
/// images. Compose is invoked through whichever v2 form is available (see
/// [`ComposeCli`]); `docker_host`, when set, points it at a specific engine
/// (e.g. podman's API socket); when `None`, compose uses its default (the
/// Docker daemon, or an explicit `DOCKER_HOST` inherited from the environment).
///
/// Image operations (tag, push, pin) are not compose's concern — the methods
/// that need a container engine (e.g. [`pin_images`](Self::pin_images)) take a
/// [`ContainerRuntime`] argument rather than this type owning one.
pub struct DockerCompose {
    cli: ComposeCli,
    docker_host: Option<String>,
    config: ComposeConfig,
}
impl DockerCompose {
    /// Resolve Docker Compose v2 for `config`, wired to `rt`'s container engine.
    ///
    /// The handle is bound to one config directory — the whole snouty run drives
    /// a single project — so per-call methods take only the compose overlay,
    /// which genuinely varies across a run (see [`down`](Self::down)).
    ///
    /// An explicit `DOCKER_HOST` already set in the environment is always
    /// respected; otherwise, for a podman runtime, compose is pointed at
    /// podman's API socket so podman backs Compose.
    pub fn resolve(rt: &dyn ContainerRuntime, config: ComposeConfig) -> Result<DockerCompose> {
        let cli = ComposeCli::resolve()?;
        let docker_host = if std::env::var_os("DOCKER_HOST").is_some() {
            None
        } else {
            rt.engine_docker_host()?
        };
        Ok(DockerCompose {
            cli,
            docker_host,
            config,
        })
    }

    /// Locate a usable Docker Compose v2 without binding a config or engine, for
    /// diagnostics and availability checks (`snouty doctor`, tests). Returns the
    /// command name (`docker-compose` / `docker compose`) and version banner.
    pub fn probe() -> Result<(String, String)> {
        let cli = ComposeCli::resolve()?;
        Ok((cli.to_string(), cli.version().to_string()))
    }

    /// The resolved compose command name (`docker-compose` / `docker compose`),
    /// for error messages that need to name what actually ran.
    pub fn cli_name(&self) -> String {
        self.cli.to_string()
    }

    /// A copy-pasteable `... down` command that reproduces what [`down`](Self::down)
    /// runs — same engine wiring, compose form, and files — for the
    /// `--keep-running` hint. Uses absolute file paths so it works from any
    /// directory (unlike [`down`](Self::down), which sets the working directory).
    pub fn down_hint(&self, overlay: Option<&Path>) -> String {
        // Make every path absolute so the printed command is copy-pasteable from
        // any directory. `config.dir()` (and hence the overlay) is whatever —
        // often relative — path the user passed to `snouty validate`, so a bare
        // join would only work from the original working directory.
        let abs = |p: &Path| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf());
        let host = self
            .docker_host
            .as_deref()
            .map(|h| format!("DOCKER_HOST={h} "))
            .unwrap_or_default();
        let compose_file = abs(&self.config.dir().join("docker-compose.yaml"));
        let mut hint = format!("{host}{} -f {}", self.cli, compose_file.display());
        if let Some(overlay) = overlay {
            let _ = write!(hint, " -f {}", abs(overlay).display());
        }
        hint.push_str(" down");
        hint
    }

    /// Base compose command wired to the engine and config directory, with the
    /// `-f` file flags and the given subcommand appended. The program, compose
    /// prefix, and file flags are all fixed here so no caller hand-assembles a
    /// compose invocation.
    fn command(&self, overlay: Option<&Path>, subcommand: &[&str]) -> Command {
        let mut cmd = self.cli.command();
        cmd.current_dir(self.config.dir());
        if let Some(host) = &self.docker_host {
            cmd.env("DOCKER_HOST", host);
        }
        cmd.args(["-f", "docker-compose.yaml"]);
        if let Some(overlay) = overlay {
            cmd.arg("-f").arg(overlay);
        }
        cmd.args(subcommand);
        cmd
    }

    /// Spawn a long-running compose subcommand (`up`) with inherited stdio, in
    /// its own process group so the whole tree can be killed on timeout. stdin
    /// is null so compose stays non-interactive and never blocks on a prompt.
    fn spawn_inherited(
        &self,
        overlay: Option<&Path>,
        subcommand: &[&str],
    ) -> Result<ProcessGroupChild> {
        let mut cmd = tokio::process::Command::from(self.command(overlay, subcommand));
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
        cmd.process_group(0);
        cmd.spawn()
            .map(ProcessGroupChild::new)
            .wrap_err_with(|| format!("failed to start '{} {}'", self.cli, subcommand.join(" ")))
    }

    /// Run `compose config [extra_args]`, returning the resolved YAML as a
    /// string.
    fn config(&self, overlay: Option<&Path>, extra_args: &[&str]) -> Result<String> {
        let cli = &self.cli;
        let mut cmd = self.command(overlay, &["config"]);
        cmd.args(extra_args);
        let output = cmd
            .output()
            .wrap_err_with(|| format!("failed to run '{cli} config'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(eyre!("'{cli} config' failed"))
                .with_section(move || stdout.trim().to_string().header("Stdout:"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Resolve and parse the compose config into structured contents.
    pub fn contents(&self, overlay: Option<&Path>) -> Result<ComposeContents> {
        let yaml = self.config(overlay, &[])?;
        parse_compose_config(&yaml)
    }

    /// Resolve the compose file to JSON using the normal (local) environment —
    /// the same interpolation `snouty` sees when it runs compose on this machine.
    pub fn config_json(&self) -> Result<String> {
        self.config(None, &["--format", "json"])
    }

    /// Resolve the compose file to JSON under a scrubbed process environment
    /// that mimics the hermetic Antithesis environment: only the variables
    /// Antithesis also sets (see [`ANTITHESIS_COMPOSE_ENV`]), so any other
    /// `${VAR}` resolves only from the config dir's `.env` file, explicit env
    /// files, and inline defaults.
    ///
    /// Returns the raw output rather than a string: a required `${VAR:?}` with no
    /// value makes compose abort (non-zero exit, empty stdout), which the caller
    /// inspects to tell a genuine environment dependency apart from compose
    /// failing for some unrelated reason.
    pub fn config_json_hermetic_env(&self) -> Result<Output> {
        // Build the normal command (binary + working directory + DOCKER_HOST),
        // then clear the whole environment. Those shell values are all valid
        // interpolation inputs Antithesis will not inherit, so scrubbing them is
        // the point.
        let mut cmd = self.command(None, &["config", "--format", "json"]);
        cmd.env_clear();
        // Put back the local value of each variable Antithesis also sets. A
        // `${VAR}` for one of them resolves in Antithesis too, so it is not a
        // divergence even though the value differs (`${HOME}` is `/root`
        // there). PATH is among them, and compose also needs it to find
        // `docker`.
        for name in ANTITHESIS_COMPOSE_ENV {
            if let Some(value) = std::env::var_os(name) {
                cmd.env(name, value);
            }
        }
        // The local render gets DOCKER_HOST from `self.docker_host` when
        // the shell does not set it, and env_clear() removed that value.
        if let Some(host) = &self.docker_host {
            cmd.env("DOCKER_HOST", host);
        }
        // Antithesis sets DOCKER_CONFIG to an empty value. The docker CLI
        // finds the compose plugin under $DOCKER_CONFIG/cli-plugins (default
        // $HOME/.docker/cli-plugins), so keep the local directory to keep a
        // user-directory install (e.g. Docker Desktop) discoverable.
        if let Some(config_dir) = docker_config_dir() {
            cmd.env("DOCKER_CONFIG", config_dir);
        }
        cmd.output().wrap_err_with(|| {
            format!(
                "failed to run '{} config' for the environment check",
                self.cli
            )
        })
    }

    /// Canonicalized compose file for baking into the config image.
    ///
    /// `docker-compose config` itself does the canonicalization: anchors,
    /// aliases, and merge keys are inlined, and the structure is normalized.
    /// `--no-interpolate` keeps `${VAR}` references for the platform to
    /// resolve in its own environment, and `--no-path-resolution` keeps
    /// relative paths relative — both would otherwise be baked with values
    /// from this machine.
    fn canonical_contents(&self) -> Result<String> {
        self.config(None, &["--no-interpolate", "--no-path-resolution"])
    }

    /// Parse `compose ps -a --format json` into the list of containers,
    /// including stopped/exited ones so callers can flag stranded test
    /// commands. Inspect [`ComposeContainer::stopped`] to tell them apart.
    pub fn ps(&self, overlay: Option<&Path>) -> Result<Vec<ComposeContainer>> {
        let cli = &self.cli;
        let cmd = self.command(overlay, &["ps", "-a", "--format", "json"]);

        let output = output_with_timeout(cmd, DISCOVERY_COMMAND_TIMEOUT)
            .wrap_err_with(|| format!("failed to run '{cli} ps'"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(eyre!("'{cli} ps' failed"))
                .with_section(move || stderr.trim().to_string().header("Stderr:"));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_compose_ps(&stdout)
    }

    /// Spawn `compose up` in the foreground and return the child process.
    ///
    /// Attached rather than detached because only an attached `up` interleaves
    /// container logs in real time. A detached `up` followed by `compose logs`
    /// replays each container's buffered history as its own block, so the
    /// startup window — the part worth reading — arrives grouped by container
    /// rather than in the order things actually happened.
    ///
    /// stdout and stderr are inherited so compose writes straight to the
    /// terminal and keeps its per-service colors; stdin is null (see
    /// [`spawn_inherited`](Self::spawn_inherited)) so compose never decides it
    /// is interactive and blocks on a prompt that nothing can answer.
    ///
    /// Deliberately no `--abort-on-container-exit`: a one-shot setup container
    /// that exits 0 is a legitimate compose pattern (`depends_on:
    /// service_completed_successfully`), and tearing the project down on the
    /// first exit would break exactly those setups. The process exits on its
    /// own once every container has stopped.
    ///
    /// Uses `process_group(0)` so the whole group can be killed once the
    /// setup-complete event lands. Killing it leaves the containers running —
    /// they belong to the engine, not to this process tree.
    pub fn up_attached(&self, overlay: Option<&Path>) -> Result<ProcessGroupChild> {
        self.spawn_inherited(overlay, &["up", "--no-build", "--pull=never"])
    }

    /// Run `compose down` for cleanup. Best-effort, ignores errors.
    pub fn down(&self, overlay: Option<&Path>) {
        let mut cmd = self.command(overlay, &["down", "--timeout", "0"]);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        let _ = cmd.status();
    }

    /// Return the `docker-compose.yaml` contents canonicalized, with each
    /// local image rewritten to a digest reference (`name:tag@sha256:...`).
    ///
    /// The local image store is the single source of truth for what a launch
    /// runs — snouty never pulls. A local image is rewritten to the digest in
    /// a registry that already serves its bytes, or — when no registry has
    /// it — pushed to `registry` and rewritten to the pushed digest. So
    /// Antithesis always pulls exactly what was resolved here. `missing`
    /// decides what happens to an image that is not local.
    ///
    /// A reference into `registry` itself comes back bare, because
    /// Antithesis resolves such a name against the tenant's own repository.
    pub fn pin_images(
        &self,
        rt: &dyn ContainerRuntime,
        registry: &str,
        missing: MissingImages,
    ) -> Result<String> {
        let prefix = format!("{}/", registry.trim_end_matches('/'));
        let contents = self.contents(None)?;

        let (local, absent) = partition_by_presence(rt, distinct_images(&contents)?)?;
        if !absent.is_empty() {
            match missing {
                MissingImages::Reject => {
                    return Err(launch_missing_images_error(rt, &contents, &absent));
                }
                MissingImages::AssumeRemote => warn_remote_images(rt, &contents, &absent),
            }
        }

        let (mut references, unserved) = find_served_images(rt, local, &prefix)?;
        references.extend(push_images(rt, &unserved, &absent, &prefix)?);

        let services = service_rewrites(&contents, &references, &prefix);
        rewrite_compose_images(&self.canonical_contents()?, &services)
    }
}

/// Each distinct image that the compose services name, in service order.
fn distinct_images(contents: &ComposeContents) -> Result<Vec<&str>> {
    ensure_services_have_images(contents)?;
    let mut seen = HashSet::new();
    Ok(contents
        .services
        .iter()
        .map(|service| service.image.as_str())
        .filter(|image| seen.insert(*image))
        .collect())
}

/// Split `images` into those in the local image store and those not in it.
fn partition_by_presence<'a>(
    rt: &dyn ContainerRuntime,
    images: Vec<&'a str>,
) -> Result<(Vec<&'a str>, Vec<&'a str>)> {
    let mut local = Vec::new();
    let mut absent = Vec::new();
    for image in images {
        if rt.image_exists(image)? {
            local.push(image);
        } else {
            absent.push(image);
        }
    }
    Ok((local, absent))
}

/// The missing-image error, with the escape hatches that only launch has.
fn launch_missing_images_error(
    rt: &dyn ContainerRuntime,
    contents: &ComposeContents,
    absent: &[&str],
) -> color_eyre::Report {
    let err = missing_images_error(rt, contents, absent).with_suggestion(|| {
        "if a registry that Antithesis can reach serves the missing images, add \
         `--allow-remote-images` to launch without local validation and pinning"
    });
    // `--config-image` conflicts with `--config`, so the suggestion says to
    // replace `--config`, not to add a flag.
    err.with_suggestion(|| {
        "if you already have a pre-built config image, launch with `--config-image <ref>` \
         in place of `--config <dir>` to reuse it and skip local packaging"
    })
}

/// Warn about the images in `absent`, which the launch leaves as written.
fn warn_remote_images(rt: &dyn ContainerRuntime, contents: &ComposeContents, absent: &[&str]) {
    let warn = |text: String| eprintln!("{}", crate::render::wrap_if_tty(&text));
    let listing = |items: Vec<&str>| {
        items
            .iter()
            .map(|item| format!("  {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    // An image in the store of another engine was likely built for this
    // launch, so Antithesis cannot pull it.
    let absent_refs: Vec<String> = absent.iter().map(ToString::to_string).collect();
    let elsewhere = images_available_in_other_engines(rt, &absent_refs);
    if let Some((warnings, suggestion)) = cross_engine_guidance(rt.name(), &elsewhere) {
        for warning in warnings {
            warn(format!("warning: {warning}"));
        }
        warn(suggestion);
    }

    warn(format!(
        "warning: these images are not in the local image store, so snouty cannot validate \
         and pin them:\n{}",
        listing(absent.to_vec())
    ));
    let unbuilt: Vec<&str> = contents
        .services
        .iter()
        .filter(|service| {
            contents.build_services.contains(&service.name)
                && absent.contains(&service.image.as_str())
        })
        .map(|service| service.name.as_str())
        .collect();
    if !unbuilt.is_empty() {
        warn(format!(
            "warning: these services have a `build:` stanza, but their images are not in the \
             local image store. To launch the local code, run `docker compose build`:\n{}",
            listing(unbuilt)
        ));
    }
}

/// Split local `images` into those a registry already serves, each mapped to
/// its digest reference there ([`find_remote_pin`]), and those it does not.
fn find_served_images<'a>(
    rt: &dyn ContainerRuntime,
    images: Vec<&'a str>,
    prefix: &str,
) -> Result<(BTreeMap<&'a str, String>, Vec<&'a str>)> {
    let mut served = BTreeMap::new();
    let mut unserved = Vec::new();
    for image in images {
        match find_remote_pin(rt, image, prefix)? {
            Some(reference) => {
                eprintln!("Image already in a registry, skipping push: {reference}");
                served.insert(image, reference);
            }
            None => unserved.push(image),
        }
    }
    Ok((served, unserved))
}

/// Push each local image in `images` into the tenant repository at `prefix`,
/// and map it to its pushed digest reference. No push overwrites the tenant
/// tag that Antithesis reads for an image in `remote`.
fn push_images<'a>(
    rt: &dyn ContainerRuntime,
    images: &[&'a str],
    remote: &[&str],
    prefix: &str,
) -> Result<BTreeMap<&'a str, String>> {
    let reserved: BTreeMap<String, &str> = remote
        .iter()
        .filter_map(|&image| Some((tenant_tag(image, prefix)?, image)))
        .collect();
    // destination -> image. One destination holds one image, because snouty
    // tags each image onto its destination and pushes that path once.
    let mut sources: BTreeMap<String, &str> = BTreeMap::new();
    for &image in images {
        let dest = push_destination(image, prefix)?;
        if let Some(other) = reserved.get(&dest) {
            bail!(user_error(format!(
                "snouty would push `{image}` to `{dest}`, over the image that a service names \
                 as `{other}`. Rename one of them."
            )));
        }
        if let Some(other) = sources.insert(dest.clone(), image) {
            bail!(user_error(format!(
                "`{other}` and `{image}` both push to `{dest}`, so snouty would push one of \
                 them and pin both services to it. Rename one of them."
            )));
        }
    }
    for (dest, image) in &sources {
        if dest != image {
            rt.image_tag(image, dest)?;
        }
    }
    let dests: Vec<&str> = sources.keys().map(String::as_str).collect();
    validate_image_architectures(rt, &dests)?;
    sources
        .into_iter()
        .map(|(dest, image)| {
            eprintln!("Pushing image: {dest}");
            let reference = rt.image_push(&dest)?;
            eprintln!("Image pushed: {reference}");
            Ok((image, reference))
        })
        .collect()
}

/// The tag in the tenant repository at `prefix` that Antithesis reads first
/// for `image`, or `None` when `image` names another registry. Antithesis
/// resolves a bare name against the tenant repository before public
/// registries.
fn tenant_tag(image: &str, prefix: &str) -> Option<String> {
    let path = strip_registry(image, prefix);
    (path != image || registry_host(image).is_none()).then(|| format!("{prefix}{path}"))
}

/// The new `image:` reference of each service whose reference changes. An
/// image that is not in `references` keeps its reference as written, and
/// every reference loses the address of the tenant repository at `prefix`.
fn service_rewrites(
    contents: &ComposeContents,
    references: &BTreeMap<&str, String>,
    prefix: &str,
) -> BTreeMap<String, String> {
    contents
        .services
        .iter()
        .filter_map(|service| {
            let image = service.image.as_str();
            let reference = references.get(image).map_or(image, String::as_str);
            let reference = strip_registry(reference, prefix);
            (reference != image).then(|| (service.name.clone(), reference))
        })
        .collect()
}

/// What [`DockerCompose::pin_images`] does with a compose image that is not
/// in the local image store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingImages {
    /// Fail the launch: snouty never pulls, so it cannot pin the image.
    Reject,
    /// Leave the reference unpinned, so Antithesis pulls it by name at run
    /// time.
    AssumeRemote,
}
/// Where snouty pushes `image` inside the tenant repository at `prefix`.
///
/// The path below `prefix` never opens with a registry host, so the compose
/// pin can drop `prefix` and still name the same bytes. A host the author
/// wrote below `prefix` is mirrored too: `{prefix}ghcr.io/org/app` goes to
/// `{prefix}snouty-mirror/ghcr.io/org/app`.
fn push_destination(image: &str, prefix: &str) -> Result<String> {
    Ok(format!(
        "{prefix}{}",
        mirror_path(&strip_registry(image, prefix))?
    ))
}

/// Find a registry that already serves `image`'s local bytes, returning
/// the digest-pinned reference to use, or `None` when the image must be
/// pushed.
///
/// Candidate digests come from the local store's repo digests, for two
/// repositories: the image's own (e.g. `docker.io/library/redis` for
/// `redis:7`) and its push destination under `prefix`, where a previous
/// snouty push would have put it. A candidate counts only when the registry
/// confirms it serves the digest (a manifest-only round trip — never a pull or
/// push) AND the platform can run amd64 from it: a manifest list must offer an
/// amd64 entry, while a single manifest shares the local image's architecture,
/// so the local image must be amd64.
///
/// Depends only on the container engine, not on compose state, so it is a
/// free function rather than a [`DockerCompose`] method.
fn find_remote_pin(rt: &dyn ContainerRuntime, image: &str, prefix: &str) -> Result<Option<String>> {
    let repo_digests = rt.image_repo_digests(image)?;
    let tag = image_ref_tag(image);

    let mut repos = Vec::new();
    // The caller strips `prefix` off the pin, and only the push destination
    // is spelled so that it survives the strip. A repository at the
    // registry's own address never survives it either, because the strip
    // matches `prefix` whole — and that address is the one a test run cannot
    // resolve, so those bytes go to the push path.
    let registry_address = registry_host(prefix);
    if registry_address.is_none() || registry_host(image) != registry_address {
        repos.push(normalize_repo(image_repo(image)));
    }
    let dest_repo = normalize_repo(image_repo(&push_destination(image, prefix)?));
    if !repos.contains(&dest_repo) {
        repos.push(dest_repo);
    }

    for repo in &repos {
        // A pull typically records several digests per repo (the
        // per-arch manifest and the manifest list) — try them all.
        for digest in digests_for_repo(repo, &repo_digests) {
            let amd64_ok = match rt.remote_manifest(&format!("{repo}@{digest}")) {
                RemoteManifest::NotFound => continue,
                RemoteManifest::List { has_amd64 } => has_amd64,
                RemoteManifest::Single => rt.image_architecture(image)? == Architecture::Amd64,
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
/// Set the `image:` field of each service named in `references` to its
/// reference. Other services keep their `image:` field.
fn rewrite_compose_images(yaml: &str, references: &BTreeMap<String, String>) -> Result<String> {
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
        let Some(reference) = references.get(name) else {
            continue;
        };
        let svc_map = svc
            .as_mapping_mut()
            .ok_or_else(|| eyre!("compose service '{name}' is not a mapping"))?;
        svc_map.insert(
            serde_yaml::Value::String("image".to_string()),
            serde_yaml::Value::String(reference.clone()),
        );
    }
    serde_yaml::to_string(&doc).wrap_err("failed to serialize pinned docker-compose.yaml")
}
/// Every compose service must carry an explicit `image:` reference.
/// Antithesis runs compose under its own project name, so the default tag that
/// `docker compose build` gives a `build:`-only service names no image there.
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
    crate::util::copy_dir_recursive(config_dir, staged.path())?;
    std::fs::write(staged.path().join("docker-compose.yaml"), pinned_yaml)
        .wrap_err("failed to write pinned docker-compose.yaml")?;
    Ok(staged)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeService {
    pub name: String,
    pub image: String,
}
/// Parsed contents of a compose config file.
#[derive(Debug)]
pub struct ComposeContents {
    /// One entry per service with an explicit `image:` field.
    pub services: Vec<ComposeService>,
    /// Names of services without an explicit `image:` field.
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
                    .and_then(serde_yaml::Value::as_bool)
                    .unwrap_or(false);
                if is_external {
                    return Err(user_error(format!(
                        "network '{name}' is declared as external and won't work on Antithesis"
                    ))
                    .suggestion(
                        "remove `external: true` and declare the network normally — Antithesis \
                         provisions every network inside the test environment",
                    ));
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
/// snouty never pulls, so validate checks exactly what is in the local store.
pub fn validate_images_are_available(
    runtime: &dyn ContainerRuntime,
    contents: &ComposeContents,
) -> Result<()> {
    let (_, absent) = partition_by_presence(runtime, distinct_images(contents)?)?;
    if absent.is_empty() {
        return Ok(());
    }
    Err(missing_images_error(runtime, contents, &absent))
}
/// The error for the images in `absent`. It explains only why the images
/// must be local, and each caller adds its own suggestions.
fn missing_images_error(
    runtime: &dyn ContainerRuntime,
    contents: &ComposeContents,
    absent: &[&str],
) -> color_eyre::Report {
    let mut err = eyre!("some images are not available locally");
    for &image in absent {
        let service = contents
            .services
            .iter()
            .find(|service| service.image == image);
        let hint = match service {
            Some(service) if contents.build_services.contains(&service.name) => {
                " (service has a `build:` stanza — build it first; snouty doesn't build)"
                    .to_string()
            }
            _ => String::new(),
        };
        err = err.with_note(|| format!("image: {image}{hint}"));
    }
    err = err.with_note(|| "snouty does not pull or build images");

    // A missing image is often just sitting in the *other* installed engine's
    // store — the usual cause is `docker compose build` landing it in docker
    // while snouty auto-selected podman (or vice versa), since the two keep
    // separate image stores. Surface that instead of leaving the generic
    // "build it first" note to mislead someone who already built the image.
    // Best-effort: any probe failure counts as "not there".
    let absent_refs: Vec<String> = absent.iter().map(ToString::to_string).collect();
    let elsewhere = images_available_in_other_engines(runtime, &absent_refs);
    if let Some((warnings, suggestion)) = cross_engine_guidance(runtime.name(), &elsewhere) {
        for warning in warnings {
            err = err.with_warning(move || warning);
        }
        err = err.with_suggestion(move || suggestion);
    }

    err.with_suggestion(|| "pull or build the missing images, then retry")
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
        if arch != Architecture::Amd64 {
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
    /// Lifecycle state, reduced to the distinctions snouty acts on.
    pub state: ContainerState,
}
/// A container's lifecycle state as reported by `compose ps`, reduced to
/// the three distinctions snouty acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerState {
    /// The entrypoint is running: `exec` works in this container.
    Running,
    /// The entrypoint has exited (`exited` or `dead`). Antithesis can't run
    /// test commands in stopped containers, so validate flags any service
    /// that has test commands defined but ended up in this state.
    Stopped,
    /// Everything else: `created`, `restarting`, `paused`, a missing
    /// `State`, or human-readable forms like "Up 5 seconds". Healthy setups
    /// still settling pass through these states, so they must not trip the
    /// stranded-test-commands diagnostic — and `exec` fails in all of them,
    /// so discovery must fall back to `cp`, which works in any state.
    Transient,
}
/// Reduce a `State` field value to a [`ContainerState`].
fn parse_container_state(state: Option<&str>) -> ContainerState {
    match state {
        Some(s) if s.eq_ignore_ascii_case("running") => ContainerState::Running,
        Some(s) if s.eq_ignore_ascii_case("exited") || s.eq_ignore_ascii_case("dead") => {
            ContainerState::Stopped
        }
        _ => ContainerState::Transient,
    }
}
/// Parse the JSON output of `compose ps --format json`.
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
                state: parse_container_state(state),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::container::pinned_image_ref;
    use crate::testutils::{OCIRegistry, has_compose, require_runtimes_with_compose, skip_or_fail};
    use std::path::PathBuf;

    fn cc(service: &str, id: &str, state: ContainerState) -> ComposeContainer {
        ComposeContainer {
            service: service.to_string(),
            id: id.to_string(),
            state,
        }
    }
    #[test]
    fn parse_compose_ps_ndjson() {
        let stdout = "{\"ID\":\"abc123\",\"Service\":\"app\",\"State\":\"running\"}\n\
                      {\"ID\":\"def456\",\"Service\":\"sidecar\",\"State\":\"exited\"}\n";
        let result = parse_compose_ps(stdout).unwrap();
        assert_eq!(
            result,
            vec![
                cc("app", "abc123", ContainerState::Running),
                cc("sidecar", "def456", ContainerState::Stopped)
            ]
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
            vec![
                cc("app", "abc123", ContainerState::Running),
                cc("sidecar", "def456", ContainerState::Running)
            ]
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
        assert_eq!(result, vec![cc("app", "abc", ContainerState::Transient)]);
    }
    #[test]
    fn parse_compose_ps_transient_states_are_not_stopped() {
        // created / restarting / paused are not "stopped" — Antithesis may
        // still see them recover. Only `exited` and `dead` count as stopped.
        // None of them are "running" either: exec fails in every one of
        // these states, so discovery must take the cp path for them.
        let stdout = r#"[
            {"ID":"a","Service":"svc","State":"created"},
            {"ID":"b","Service":"svc","State":"restarting"},
            {"ID":"c","Service":"svc","State":"paused"},
            {"ID":"d","Service":"svc","State":"Up 5 seconds"},
            {"ID":"e","Service":"svc","State":"dead"},
            {"ID":"f","Service":"svc","State":"EXITED"}
        ]"#;
        let result = parse_compose_ps(stdout).unwrap();
        let states: Vec<(&str, ContainerState)> =
            result.iter().map(|c| (c.id.as_str(), c.state)).collect();
        assert_eq!(
            states,
            vec![
                ("a", ContainerState::Transient),
                ("b", ContainerState::Transient),
                ("c", ContainerState::Transient),
                ("d", ContainerState::Transient),
                ("e", ContainerState::Stopped),
                ("f", ContainerState::Stopped),
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
                cc("worker", "a1", ContainerState::Running),
                cc("worker", "a2", ContainerState::Running),
                cc("worker", "a3", ContainerState::Stopped),
            ]
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
        assert_eq!(contents.services_without_image, vec!["builder".to_string()]);
        assert!(contents.networks.is_empty());
    }
    #[test]
    fn parse_compose_config_needs_an_explicit_image_even_with_a_project_name() {
        // `docker compose build` tags a build-only service
        // `<project>-<service>`, but Antithesis runs compose under its own
        // project name, so that tag names no image there.
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
            vec![ComposeService {
                name: "app".to_string(),
                image: "myapp:latest".to_string(),
            }]
        );
        assert_eq!(contents.services_without_image, vec!["builder".to_string()]);
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
        // The suggestion names the fix, matching the other validate errors.
        assert!(
            format!("{err:?}").contains("remove `external: true`"),
            "expected the removal suggestion, got: {err:?}"
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

            let config = match crate::config::detect_config(dir.path()).unwrap() {
                crate::config::Config::Compose(c) => c,
                other @ crate::config::Config::Kubernetes(_) => {
                    panic!("expected Compose, got {other:?}")
                }
            };
            let compose = DockerCompose::resolve(rt.as_ref(), config).unwrap();
            let contents = compose.contents(None).unwrap();
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

            let config = match crate::config::detect_config(dir.path()).unwrap() {
                crate::config::Config::Compose(c) => c,
                other @ crate::config::Config::Kubernetes(_) => {
                    panic!("expected Compose, got {other:?}")
                }
            };
            let compose = DockerCompose::resolve(rt.as_ref(), config).unwrap();
            let yaml = compose.config(Some(&overlay), &[]).unwrap();
            let contents = compose.contents(Some(&overlay)).unwrap();

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
    fn rewrite_compose_images_keeps_services_it_does_not_name() {
        let yaml = "\
services:
  app:
    image: app:latest
  remote:
    image: ghcr.io/org/remote:v1
";
        let references = BTreeMap::from([("app".to_string(), "app:latest@sha256:aaa".to_string())]);

        let out = rewrite_compose_images(yaml, &references).unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(doc["services"]["app"]["image"], "app:latest@sha256:aaa");
        assert_eq!(doc["services"]["remote"]["image"], "ghcr.io/org/remote:v1");
    }
    /// Run `pin_images` over `yaml` with a [`FakeRuntime`] (real docker-compose
    /// binary for config resolution, fake image/registry operations).
    fn pin_with_fake(rt: &FakeRuntime, yaml: &str, registry: &str) -> Result<String> {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yaml"), yaml).unwrap();
        let config = match crate::config::detect_config(dir.path()).unwrap() {
            crate::config::Config::Compose(c) => c,
            other @ crate::config::Config::Kubernetes(_) => {
                panic!("expected Compose, got {other:?}")
            }
        };
        let compose = DockerCompose::resolve(rt, config).unwrap();
        compose.pin_images(rt, registry, MissingImages::Reject)
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
        // The pin names a registry that is not ours, so it stays fully qualified.
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
    fn pin_images_mirrors_a_registry_host_into_the_path() {
        if !has_compose() {
            // Loud in CI (skip_or_fail panics there) so a runner missing
            // docker-compose can't silently drop this coverage.
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // The local store fabricates digest entries for registry-qualified
        // tags that were never pushed; the registry round trip rejects them
        // (NotFound), so the image needs a push and therefore a mirror path.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("ghcr.io/org/app:v1".to_string(), true)]),
            architectures: BTreeMap::from([(
                "reg.example.com/snouty-mirror/ghcr.io/org/app:v1".to_string(),
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
        // A pin of `ghcr.io/org/app` would send the platform to the real
        // ghcr.io for a digest only we hold.
        assert!(
            out.contains("image: snouty-mirror/ghcr.io/org/app:v1@sha256:fakepushdigest"),
            "expected the mirrored pin without our prefix, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["reg.example.com/snouty-mirror/ghcr.io/org/app:v1".to_string()]
        );
    }

    #[test]
    fn pin_images_mirrors_a_host_the_author_wrote_below_our_registry() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // The registry serves the author's path, but a pin of it would open
        // with `ghcr.io`, which the platform reads as an address.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([(
                "reg.example.com/ghcr.io/org/app:v1".to_string(),
                true,
            )]),
            architectures: BTreeMap::from([(
                "reg.example.com/snouty-mirror/ghcr.io/org/app:v1".to_string(),
                "amd64".to_string(),
            )]),
            repo_digests: BTreeMap::from([(
                "reg.example.com/ghcr.io/org/app:v1".to_string(),
                vec!["reg.example.com/ghcr.io/org/app@sha256:served".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "reg.example.com/ghcr.io/org/app@sha256:served".to_string(),
                RemoteManifest::List { has_amd64: true },
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: reg.example.com/ghcr.io/org/app:v1\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("image: snouty-mirror/ghcr.io/org/app:v1@sha256:fakepushdigest"),
            "expected the mirrored pin, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["reg.example.com/snouty-mirror/ghcr.io/org/app:v1".to_string()]
        );
    }

    #[test]
    fn pin_images_allows_one_path_when_only_one_image_is_pushed() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // `redis:7` and `reg.example.com/redis:7` share a destination, and
        // the registry already serves `redis:7`.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([
                ("redis:7".to_string(), true),
                ("reg.example.com/redis:7".to_string(), true),
            ]),
            architectures: BTreeMap::from([(
                "reg.example.com/redis:7".to_string(),
                "amd64".to_string(),
            )]),
            repo_digests: BTreeMap::from([(
                "redis:7".to_string(),
                vec!["docker.io/library/redis@sha256:list".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "docker.io/library/redis@sha256:list".to_string(),
                RemoteManifest::List { has_amd64: true },
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  a:\n    image: redis:7\n  b:\n    image: reg.example.com/redis:7\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("docker.io/library/redis:7@sha256:list"),
            "expected the remote pin, got: {out}"
        );
        assert!(
            out.contains("image: redis:7@sha256:fakepushdigest"),
            "expected the pushed image pinned bare, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["reg.example.com/redis:7".to_string()]
        );
    }

    #[test]
    fn pin_images_rejects_two_images_that_want_one_path() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        // An author who copies a pinned reference back into the source has
        // both spellings of one mirror path.
        let rt = FakeRuntime {
            available_images: BTreeMap::from([
                ("ghcr.io/org/app:v1".to_string(), true),
                (
                    "reg.example.com/snouty-mirror/ghcr.io/org/app:v1".to_string(),
                    true,
                ),
            ]),
            ..Default::default()
        };
        let err = pin_with_fake(
            &rt,
            "services:\n  a:\n    image: ghcr.io/org/app:v1\n  \
             b:\n    image: reg.example.com/snouty-mirror/ghcr.io/org/app:v1\n",
            "reg.example.com",
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("both push to `reg.example.com/snouty-mirror/ghcr.io/org/app:v1`"),
            "expected a collision error, got: {err}"
        );
    }

    #[test]
    fn pin_images_strips_prefix_from_an_already_prefixed_source_image() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("reg.example.com/myapp:v1".to_string(), true)]),
            architectures: BTreeMap::from([(
                "reg.example.com/myapp:v1".to_string(),
                "amd64".to_string(),
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: reg.example.com/myapp:v1\n",
            "reg.example.com",
        )
        .unwrap();
        assert!(
            out.contains("image: myapp:v1@sha256:fakepushdigest"),
            "expected the push digest pinned without our prefix, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["reg.example.com/myapp:v1".to_string()]
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
            out.contains("image: myapp:latest@sha256:pushedearlier"),
            "expected pin to the previously pushed digest, got: {out}"
        );
        assert!(
            rt.pushed.lock().unwrap().is_empty(),
            "nothing should be pushed"
        );
    }
    /// A second launch of a mirrored image finds the copy the first launch
    /// pushed, so the mirror path is a stable address rather than a new path
    /// per run.
    #[test]
    fn pin_images_skips_push_for_a_previously_mirrored_image() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("ghcr.io/org/app:v1".to_string(), true)]),
            architectures: BTreeMap::from([(
                "ghcr.io/org/app:v1".to_string(),
                "amd64".to_string(),
            )]),
            repo_digests: BTreeMap::from([(
                "ghcr.io/org/app:v1".to_string(),
                vec!["reg.example.com/snouty-mirror/ghcr.io/org/app@sha256:mirrored".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "reg.example.com/snouty-mirror/ghcr.io/org/app@sha256:mirrored".to_string(),
                RemoteManifest::Single,
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
            out.contains("image: snouty-mirror/ghcr.io/org/app:v1@sha256:mirrored"),
            "expected the pin to the earlier copy, got: {out}"
        );
        assert!(
            rt.pushed.lock().unwrap().is_empty(),
            "nothing should be pushed"
        );
    }

    /// A repository beside the tenant's own, at the same address: the
    /// registry serves those bytes, but only under the address this machine
    /// uses, and a test run cannot resolve it. snouty copies the image into
    /// the tenant repository instead of pinning the address.
    #[test]
    fn pin_images_copies_a_neighbour_repository_at_our_own_address() {
        if !has_compose() {
            skip_or_fail("docker-compose (Docker Compose v2) is not available");
            return;
        }
        let rt = FakeRuntime {
            available_images: BTreeMap::from([("proxy.local/other-team/app:v1".to_string(), true)]),
            architectures: BTreeMap::from([(
                "proxy.local/tenant/snouty-mirror/proxy.local/other-team/app:v1".to_string(),
                "amd64".to_string(),
            )]),
            repo_digests: BTreeMap::from([(
                "proxy.local/other-team/app:v1".to_string(),
                vec!["proxy.local/other-team/app@sha256:served".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "proxy.local/other-team/app@sha256:served".to_string(),
                RemoteManifest::List { has_amd64: true },
            )]),
            ..Default::default()
        };
        let out = pin_with_fake(
            &rt,
            "services:\n  app:\n    image: proxy.local/other-team/app:v1\n",
            "proxy.local/tenant",
        )
        .unwrap();
        assert!(
            out.contains(
                "image: snouty-mirror/proxy.local/other-team/app:v1@sha256:fakepushdigest"
            ),
            "expected the copy pinned bare, got: {out}"
        );
        assert_eq!(
            *rt.pushed.lock().unwrap(),
            vec!["proxy.local/tenant/snouty-mirror/proxy.local/other-team/app:v1".to_string()]
        );
    }

    #[test]
    fn push_destination_puts_every_image_below_our_prefix() {
        let dest = |image: &str| push_destination(image, "reg.example.com/tenant/").unwrap();
        assert_eq!(dest("myapp:v1"), "reg.example.com/tenant/myapp:v1");
        assert_eq!(
            dest("reg.example.com/tenant/myapp:v1"),
            "reg.example.com/tenant/myapp:v1"
        );
        assert_eq!(
            dest("ghcr.io/org/app:v1"),
            "reg.example.com/tenant/snouty-mirror/ghcr.io/org/app:v1"
        );
        // The strip runs first, so a host below our prefix is mirrored once,
        // not twice.
        assert_eq!(
            dest("reg.example.com/tenant/ghcr.io/org/app:v1"),
            "reg.example.com/tenant/snouty-mirror/ghcr.io/org/app:v1"
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
            let Some(registry) = OCIRegistry::start(rt.as_ref()) else {
                continue;
            };
            let addr = registry.host_port();

            // Build a purely-local image (present locally, in no registry).
            let img_dir = tempfile::tempdir().unwrap();
            std::fs::write(
                img_dir.path().join("Dockerfile"),
                "FROM scratch\nCOPY . /\n",
            )
            .unwrap();
            std::fs::write(img_dir.path().join("file"), "x").unwrap();
            // Unique per runtime: every iteration builds byte-identical content,
            // so a shared name would make the second push a no-op (the registry
            // already serves that digest) and stop testing the push path. The
            // pushed repo is `{registry}/{local name}`, so this covers both the
            // local image store and the registry.
            let local = format!(
                "{}:latest",
                crate::testutils::unique_image_prefix(&format!("pin-{}", rt.name()))
            );
            let local = local.as_str();
            rt.build_image(
                img_dir.path(),
                local,
                None,
                Some(crate::container::AMD64_PLATFORM),
            )
            .unwrap_or_else(|e| panic!("{}: build: {e:?}", rt.name()));

            // Resolve `app`'s pinned image after running pin_images over `yaml`.
            let pinned_app = |yaml: &str| -> Result<String> {
                let dir = tempfile::tempdir().unwrap();
                std::fs::write(dir.path().join("docker-compose.yaml"), yaml).unwrap();
                let config = match crate::config::detect_config(dir.path()).unwrap() {
                    crate::config::Config::Compose(c) => c,
                    other @ crate::config::Config::Kubernetes(_) => {
                        panic!("expected Compose, got {other:?}")
                    }
                };
                let compose = DockerCompose::resolve(rt.as_ref(), config)
                    .unwrap_or_else(|e| panic!("{}: DockerCompose::resolve: {e:?}", rt.name()));
                let out = compose.pin_images(rt.as_ref(), &addr, MissingImages::Reject)?;
                Ok(serde_yaml::from_str::<serde_yaml::Value>(&out)
                    .unwrap()
                    .get("services")
                    .and_then(|s| s.get("app"))
                    .and_then(|s| s.get("image"))
                    .and_then(|i| i.as_str())
                    .unwrap()
                    .to_string())
            };
            let pinned_prefix = format!("{local}@sha256:");

            // Case 1 — build stanza: the local build is pushed and pinned.
            let built = pinned_app(&format!(
                "services:\n  app:\n    build: .\n    image: {local}\n"
            ))
            .unwrap_or_else(|e| panic!("{}: build case: {e:?}", rt.name()));
            assert!(
                built.starts_with(&pinned_prefix),
                "{}: build image should be pushed, got: {built}",
                rt.name()
            );
            // The pin no longer names the destination, so ask the registry
            // itself whether the bytes arrived.
            let digest = built.rsplit_once('@').unwrap().1;
            let repo = image_repo(local);
            assert!(
                registry.serves_digest(repo, digest),
                "{}: registry {addr} should serve {repo}@{digest}",
                rt.name()
            );

            // Case 2 — local without a build stanza (prebuilt/loaded out of
            // band): local availability is enough; pushed and pinned the same.
            let local_only = pinned_app(&format!("services:\n  app:\n    image: {local}\n"))
                .unwrap_or_else(|e| panic!("{}: local-only case: {e:?}", rt.name()));
            assert!(
                local_only.starts_with(&pinned_prefix),
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

            // Case 4 — a `build:`-only service with no `image:` fails before
            // anything is pushed.
            let err = pinned_app("services:\n  app:\n    build: .\n").expect_err(&format!(
                "{}: expected a service without `image:` to fail",
                rt.name()
            ));
            let debug = format!("{err:?}");
            assert!(
                debug.contains("service 'app' has no `image:` field"),
                "{}: error should name the service, got: {debug}",
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
        fn name(&self) -> &'static str {
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

        fn image_architecture(&self, image_ref: &str) -> Result<Architecture> {
            self.architectures
                .get(image_ref)
                .map(|arch| Architecture::from(arch.as_str()))
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

        fn insecure_registry_flag(&self) -> &'static str {
            unreachable!("the fake answers remote_manifest itself")
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
                })
                .collect(),
            services_without_image: Vec::new(),
            build_services: build_services.iter().map(ToString::to_string).collect(),
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
            debug.contains("snouty does not pull or build images"),
            "expected the no-pull note, got: {debug}"
        );
        assert!(
            !debug.contains("--config-image"),
            "shared check should stay context-free, got: {debug}"
        );
    }
    #[test]
    fn distinct_images_keeps_the_first_of_each_image_in_service_order() {
        let contents = contents_of(&[("b", "two:1"), ("a", "one:1"), ("c", "two:1")], &[]);
        assert_eq!(distinct_images(&contents).unwrap(), vec!["two:1", "one:1"]);
    }
    #[test]
    fn find_served_images_splits_served_from_unserved_images() {
        let rt = FakeRuntime {
            repo_digests: BTreeMap::from([(
                "redis:7".to_string(),
                vec!["docker.io/library/redis@sha256:list".to_string()],
            )]),
            remote_manifests: BTreeMap::from([(
                "docker.io/library/redis@sha256:list".to_string(),
                RemoteManifest::List { has_amd64: true },
            )]),
            ..Default::default()
        };
        let (served, unserved) =
            find_served_images(&rt, vec!["redis:7", "myapp:latest"], "reg.example.com/").unwrap();
        assert_eq!(
            served,
            BTreeMap::from([(
                "redis:7",
                "docker.io/library/redis:7@sha256:list".to_string()
            )])
        );
        assert_eq!(unserved, vec!["myapp:latest"]);
    }
    #[test]
    fn launch_missing_images_error_adds_the_launch_escape_hatches() {
        let contents = contents_of(&[("app", "missing:latest")], &[]);
        let err =
            launch_missing_images_error(&FakeRuntime::default(), &contents, &["missing:latest"]);
        let debug = format!("{err:?}");
        assert!(
            debug.contains("image: missing:latest"),
            "expected the missing image, got: {debug}"
        );
        assert!(
            debug.contains("add `--allow-remote-images`"),
            "expected the allow-remote-images escape hatch, got: {debug}"
        );
        // --config-image conflicts with --config, so the hint must say to replace
        // it, not add it alongside (which clap would reject).
        assert!(
            debug.contains("in place of `--config <dir>`"),
            "expected the hint to replace --config, not add it, got: {debug}"
        );
    }
    #[test]
    fn push_images_does_not_push_over_a_tenant_tag_that_a_remote_image_reads() {
        // A reference into the tenant repository, and a bare name, which
        // Antithesis resolves against the tenant repository first.
        for (local, remote) in [
            ("own:v2", "reg.example.com/own:v2"),
            ("reg.example.com/own:v2", "own:v2"),
        ] {
            let rt = FakeRuntime::default();
            let err = push_images(&rt, &[local], &[remote], "reg.example.com/").unwrap_err();
            let msg = format!("{err:?}");
            assert!(
                msg.contains(&format!(
                    "over the image that a service names as `{remote}`"
                )),
                "expected the push of {local} to be refused, got: {msg}"
            );
            assert!(
                rt.pushed.lock().unwrap().is_empty(),
                "nothing should be pushed"
            );
        }
    }
    #[test]
    fn service_rewrites_names_only_the_services_whose_reference_changes() {
        let contents = contents_of(
            &[
                ("app", "myapp:latest"),
                ("db", "ghcr.io/org/db:v1"),
                ("hub", "postgres:16"),
                ("own", "reg.example.com/own:v2"),
            ],
            &[],
        );
        let references = BTreeMap::from([(
            "myapp:latest",
            "reg.example.com/myapp:latest@sha256:a".to_string(),
        )]);
        assert_eq!(
            service_rewrites(&contents, &references, "reg.example.com/"),
            BTreeMap::from([
                ("app".to_string(), "myapp:latest@sha256:a".to_string()),
                // A remote image in the tenant repository loses the address
                // too, so Antithesis does not need to resolve it.
                ("own".to_string(), "own:v2".to_string()),
            ])
        );
    }
    #[test]
    fn push_images_maps_each_image_to_its_pushed_reference() {
        let rt = FakeRuntime {
            architectures: BTreeMap::from([
                (
                    "reg.example.com/myapp:latest".to_string(),
                    "amd64".to_string(),
                ),
                (
                    "reg.example.com/sidecar:v1".to_string(),
                    "amd64".to_string(),
                ),
            ]),
            ..Default::default()
        };
        let references = push_images(
            &rt,
            &["myapp:latest", "sidecar:v1"],
            &[],
            "reg.example.com/",
        )
        .unwrap();
        assert_eq!(
            references,
            BTreeMap::from([
                (
                    "myapp:latest",
                    "reg.example.com/myapp:latest@sha256:fakepushdigest".to_string()
                ),
                (
                    "sidecar:v1",
                    "reg.example.com/sidecar:v1@sha256:fakepushdigest".to_string()
                ),
            ])
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
    fn compose_version_parts_parses_major_minor_and_patch() {
        assert_eq!(compose_version_parts("2.40.3"), Some((2, 40, 3)));
        assert_eq!(compose_version_parts("v2.40.3"), Some((2, 40, 3)));
        // Distro build-metadata and pre-release suffixes must not throw off the
        // parse, on any component.
        assert_eq!(
            compose_version_parts("2.40.3+ds1-0ubuntu1~24.04.1"),
            Some((2, 40, 3))
        );
        assert_eq!(compose_version_parts("2.24.7-rc1"), Some((2, 24, 7)));
        // Absent components read as zero, so a bare minor sorts below any patch.
        assert_eq!(compose_version_parts("2.24"), Some((2, 24, 0)));
        assert_eq!(compose_version_parts("2"), Some((2, 0, 0)));
        assert_eq!(compose_version_parts("1.29.2"), Some((1, 29, 2))); // Compose v1
        assert_eq!(compose_version_parts(""), None);
        assert_eq!(compose_version_parts("garbage"), None);
    }

    /// The gate has to order by patch within the same minor, because the
    /// release that matters — 2.24.7, which fixed `--no-path-resolution` for
    /// `include:` — is a patch release. Accepting 2.24.0 would let a silently
    /// broken config through.
    #[test]
    fn min_compose_version_rejects_releases_below_the_include_fix() {
        let supported =
            |v: &str| compose_version_parts(v).is_some_and(|p| p >= MIN_COMPOSE_VERSION);

        assert!(!supported("1.29.2"), "Compose v1 must be rejected");
        assert!(
            !supported("2.18.0"),
            "the release that introduced the flag is still too old for `include:`"
        );
        assert!(
            !supported("2.24.6"),
            "the release just below the fix must be rejected"
        );
        assert!(supported("2.24.7"), "the bar itself must be accepted");
        assert!(supported("2.40.3"));
        assert!(supported("5.3.1"), "a future major must be accepted");
        assert!(
            !supported("garbage"),
            "an unparsable version must be rejected"
        );
    }
    #[test]
    fn compose_form_display_standalone_and_plugin() {
        let standalone = ComposeForm::Standalone(PathBuf::from("/usr/local/bin/docker-compose"));
        assert_eq!(standalone.to_string(), "docker-compose");

        let plugin = ComposeForm::Plugin(PathBuf::from("/usr/bin/docker"));
        assert_eq!(plugin.to_string(), "docker compose");
    }

    #[test]
    fn down_hint_reproduces_the_down_invocation() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("docker-compose.yaml"), "services: {}\n").unwrap();
        let config = match crate::config::detect_config(dir.path()).unwrap() {
            crate::config::Config::Compose(c) => c,
            other @ crate::config::Config::Kubernetes(_) => {
                panic!("expected Compose, got {other:?}")
            }
        };

        // Plugin form, an engine override, and an overlay all appear.
        let compose = DockerCompose {
            cli: ComposeCli {
                form: ComposeForm::Plugin(PathBuf::from("/usr/bin/docker")),
                version: "2.40.3".to_string(),
            },
            docker_host: Some("unix:///run/podman.sock".to_string()),
            config: config.clone(),
        };
        assert_eq!(
            compose.down_hint(Some(Path::new("/tmp/override.yml"))),
            format!(
                "DOCKER_HOST=unix:///run/podman.sock docker compose \
                 -f {}/docker-compose.yaml -f /tmp/override.yml down",
                dir.path().display()
            ),
        );

        // Standalone, no engine override, no overlay.
        let compose = DockerCompose {
            cli: ComposeCli {
                form: ComposeForm::Standalone(PathBuf::from("/usr/local/bin/docker-compose")),
                version: "2.40.3".to_string(),
            },
            docker_host: None,
            config,
        };
        assert_eq!(
            compose.down_hint(None),
            format!(
                "docker-compose -f {}/docker-compose.yaml down",
                dir.path().display()
            ),
        );
    }
    #[test]
    fn config_json_hermetic_env_keeps_only_antithesis_variables() {
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
      SCRUBBED_VALUE: \"${CARGO_MANIFEST_DIR}\"
      HOME_VALUE: \"${HOME}\"
      PATH_VALUE: \"${PATH}\"
      DOCKER_HOST_VALUE: \"${DOCKER_HOST}\"
",
        )
        .unwrap();
        let config = match crate::config::detect_config(dir.path()).unwrap() {
            crate::config::Config::Compose(config) => config,
            other @ crate::config::Config::Kubernetes(_) => {
                panic!("expected Compose config, got {other:?}")
            }
        };
        let compose = DockerCompose {
            cli: ComposeCli::resolve().unwrap(),
            docker_host: Some("unix:///tmp/snouty-hermetic-test.sock".to_string()),
            config,
        };

        let output = compose.config_json_hermetic_env().unwrap();
        assert!(output.status.success(), "compose config failed: {output:?}");
        let resolved: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let environment = &resolved["services"]["app"]["environment"];
        // Cargo sets CARGO_MANIFEST_DIR for the test process, and Antithesis
        // does not set it.
        assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
        assert_eq!(environment["SCRUBBED_VALUE"], "");
        // Antithesis sets HOME, PATH, and DOCKER_HOST, so the hermetic render
        // keeps the values the local render uses.
        assert_eq!(
            environment["HOME_VALUE"],
            std::env::var("HOME").unwrap_or_default()
        );
        assert_eq!(
            environment["PATH_VALUE"],
            std::env::var("PATH").unwrap_or_default()
        );
        assert_eq!(
            environment["DOCKER_HOST_VALUE"],
            "unix:///tmp/snouty-hermetic-test.sock"
        );
    }
}
