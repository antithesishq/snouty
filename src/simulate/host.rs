use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::{BufRead, IsTerminal, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use color_eyre::eyre::{Context, OptionExt, Result, bail, eyre};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{Signal, SignalKind, signal};

use super::{
    events::{self, Event, Failure},
    images::Images,
    serial::{LogStream, MAX_RECORD_BYTES, serial_lines},
    vm::{BootOutput, EnvironmentConnection, Vm},
};
use crate::{
    OutputOptions,
    cli::{SimulateArgs, SimulationId},
    compose,
    config::ComposeConfig,
    container,
    settings::Settings,
};

const PREPARE: &str = include_str!("assets/prepare.sh");
const DISABLE_FAULTS: &str = include_str!("assets/disable-faults.sh");
const START: &str = include_str!("assets/start.sh");
const START_COMPOSER: &str = include_str!("assets/start-composer.sh");
const WORKLOAD_KIND: &str = include_str!("assets/workload-kind.sh");
const RECREATE: &str = include_str!("assets/recreate.sh");
const STATUS: &str = include_str!("assets/status.sh");
const ATTACH_READY: &str = "attach-ready";
const ENVIRONMENT_VERSION: &str = "v67.3";
const ENVIRONMENT_ISO_ENV: &str = "SNOUTY_SIMULATE_GUEST_ISO";

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComposerStatus {
    Running,
    Completed,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkloadKind {
    Autonomous,
    Composed,
}

enum WorkloadCycle {
    WaitingForSetup,
    Running {
        entropy_at: tokio::time::Instant,
        workload: WorkloadKind,
    },
    ComposerCompleted {
        entropy_at: tokio::time::Instant,
    },
}

async fn add_entropy(environment: &Vm) -> Result<tokio::time::Instant> {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).map_err(|error| eyre!("generating entropy seed: {error}"))?;
    let next = tokio::time::Instant::now() + Duration::from_secs(30);
    environment
        .run_script(&format!(
            "/etc/fuzzing/add_entropy {} true\n",
            u64::from_ne_bytes(bytes)
        ))
        .await
        .wrap_err("adding entropy")?;
    Ok(next)
}

fn session_root() -> Result<PathBuf> {
    let base = crate::settings::cache_dir()
        .ok_or_eyre("cannot determine simulation state directory; set XDG_CACHE_HOME or HOME")?;
    let root = base.join("simulate");
    std::fs::create_dir_all(base.parent().expect("cache directory has a parent"))?;
    for directory in [base.clone(), root.clone()] {
        if !directory.exists() {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            if let Err(error) = builder.create(&directory)
                && error.kind() != std::io::ErrorKind::AlreadyExists
            {
                return Err(error.into());
            }
        }
        // Other commands share the cache directory and create it with the
        // umask. Each run directory is private, so its parents only need to
        // be safe from other users.
        let metadata = std::fs::symlink_metadata(&directory)?;
        if !metadata.is_dir()
            || metadata.uid() != nix::unistd::geteuid().as_raw()
            || metadata.permissions().mode() & 0o022 != 0
        {
            bail!(
                "simulation state directory must be owned by you and not writable by other users: {}",
                directory.display()
            );
        }
    }
    Ok(root)
}

fn interactive_terminal(option: &str) -> Result<()> {
    if !std::io::stdin().is_terminal() || !crate::stdio::stdout().is_terminal() {
        bail!("simulate {option} requires an interactive terminal");
    }
    Ok(())
}

pub(super) fn attach(id: &SimulationId) -> Result<ExitCode> {
    interactive_terminal("--attach")?;
    let path = session_root()?.join(id.to_string());
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|_| color_eyre::eyre::eyre!("simulation {id} is no longer running"))?;
    if !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
        || !path.join(ATTACH_READY).is_file()
    {
        bail!("simulation {id} is no longer running");
    }
    EnvironmentConnection::new(&path).exec_interactive_shell()
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Termination {
    Interrupted,
    Terminated,
    OutputClosed,
    InfrastructureFailure,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunPhase {
    Running,
    Cleanup,
}

#[derive(Default, Serialize)]
#[allow(
    clippy::struct_field_names,
    reason = "field names are public summary JSON keys"
)]
struct Summary {
    assertion_failures: u64,
    command_failures: u64,
    infrastructure_failures: u64,
}

impl Summary {
    fn observe(&mut self, event: &Event, phase: RunPhase) {
        match event.failure() {
            Some(Failure::Assertion) => self.assertion_failures += 1,
            Some(Failure::Command) if phase == RunPhase::Running => self.command_failures += 1,
            _ => {}
        }
    }

    fn failed(&self) -> bool {
        self.assertion_failures != 0
            || self.command_failures != 0
            || self.infrastructure_failures != 0
    }

    fn exit_code(&self, reason: Termination) -> ExitCode {
        if self.failed() {
            ExitCode::FAILURE
        } else {
            ExitCode::from(match reason {
                Termination::Interrupted => 130,
                Termination::Terminated => 143,
                Termination::OutputClosed => 0,
                Termination::InfrastructureFailure => 1,
            })
        }
    }
}

struct Output {
    json: bool,
    closed: bool,
    sender: tokio::sync::mpsc::Sender<OutputLine>,
    annotator: crate::runs::FaultAnnotator,
    renderer: crate::event_render::EventStreamRenderer,
}

struct OutputLine {
    text: String,
    written: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

impl Output {
    fn new(json: bool) -> Result<Self> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<OutputLine>(1);
        // A blocked pipe must not block signals or Tokio runtime shutdown.
        std::thread::Builder::new()
            .name("simulate-output".into())
            .spawn(move || {
                let mut stdout = crate::stdio::stdout().lock();
                while let Some(line) = receiver.blocking_recv() {
                    let result = writeln!(stdout, "{}", line.text).and_then(|()| stdout.flush());
                    let failed = result.is_err();
                    let _ = line.written.send(result);
                    if failed {
                        break;
                    }
                }
            })?;
        Ok(Self {
            json,
            closed: false,
            sender,
            annotator: crate::runs::FaultAnnotator::default(),
            renderer: crate::event_render::EventStreamRenderer::linear(false),
        })
    }

    async fn line(&mut self, line: String) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let (written, received) = tokio::sync::oneshot::channel();
        if self
            .sender
            .send(OutputLine {
                text: line,
                written,
            })
            .await
            .is_err()
        {
            self.closed = true;
            return Ok(());
        }
        match received.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.kind() != std::io::ErrorKind::BrokenPipe => Err(error.into()),
            _ => {
                self.closed = true;
                Ok(())
            }
        }
    }

    /// Record one instrumentation line in `summary`, then print it. Recording
    /// happens before the first await, so a cancelled call never loses a
    /// failure. A malformed line counts as an infrastructure failure, and
    /// the simulation goes on.
    async fn event(
        &mut self,
        line: &str,
        summary: &mut Summary,
        phase: RunPhase,
    ) -> Result<Option<Event>> {
        let parsed = events::parse_line(line).and_then(|event| {
            let entry = if event.is_some() || events::is_fault_injector_line(line) {
                events::log_entry(line)?
            } else {
                None
            };
            Ok((event, entry))
        });
        let (event, mut entry) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => {
                summary.infrastructure_failures += 1;
                eprintln!("Invalid instrumentation event: {error:#}");
                return Ok(None);
            }
        };
        if let Some(entry) = &mut entry {
            self.annotator.annotate(entry);
        }
        if let Some(event) = &event {
            summary.observe(event, phase);
            if !self.closed {
                let line = if self.json {
                    let mut value = serde_json::to_value(event)?;
                    if let Some(entry) = &entry {
                        value["active_faults"] = entry["active_faults"].clone();
                    }
                    value.to_string()
                } else {
                    let entry = entry.as_mut().expect("parsed events have a log entry");
                    entry
                        .as_object_mut()
                        .expect("log entries are objects")
                        .remove("active_faults");
                    if matches!(
                        &event.kind,
                        events::EventKind::RolloutComplete { .. }
                            | events::EventKind::ProcessSignal { .. }
                    ) || matches!(&event.kind, events::EventKind::Log { .. })
                        && entry.get("antithesis_error").is_some()
                    {
                        event.render()
                    } else {
                        self.renderer.render_entry(entry)
                    }
                };
                self.line(line).await?;
            }
        }
        Ok(event)
    }
}

struct Signals {
    interrupt: Signal,
    terminate: Signal,
}
impl Signals {
    fn install() -> Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }
    async fn receive(&mut self) -> Termination {
        tokio::select! {
            _ = self.interrupt.recv() => Termination::Interrupted,
            _ = self.terminate.recv() => Termination::Terminated,
        }
    }
}

pub(super) async fn run(
    args: SimulateArgs,
    config: ComposeConfig,
    settings: &Settings,
    options: OutputOptions,
) -> Result<ExitCode> {
    let environment = resolve_environment(args.environment_image, settings)?;
    let runtime = container::runtime(settings)?;
    container::warn_ambiguous_engine(settings, runtime.as_ref(), options.json);
    let compose = compose::DockerCompose::resolve(runtime.as_ref(), config.clone())?;
    crate::validate::check_compose_divergence(&compose, false)?;
    let contents = compose.contents(None)?;
    compose::validate_images_are_available(runtime.as_ref(), &contents)?;
    if !contents.services_without_image.is_empty() {
        bail!(
            "simulate requires an explicit image for each service; add image entries and build them first"
        );
    }
    if contents.services.is_empty() {
        bail!("docker-compose.yaml has no services");
    }
    let references: BTreeSet<_> = contents
        .services
        .into_iter()
        .map(|service| service.image)
        .collect();
    let mut signals = Signals::install()?;
    let images = Images::from_runtime(runtime.as_ref());
    let run_dir = tempfile::Builder::new()
        .prefix("simulate-")
        .tempdir_in(session_root()?)?;
    std::fs::set_permissions(run_dir.path(), std::fs::Permissions::from_mode(0o700))?;
    let run_id = run_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut vm = None;
    let mut logs: Option<LogStream> = None;
    let mut summary = Summary::default();
    let mut output = Output::new(options.json)?;
    let mut consumed = 0;
    let mut instrumentation = None;
    let execution = async {
        eprintln!("Preparing simulation environment image...");
        let iso = environment
            .iso(&images, run_dir.path(), args.platform)
            .await?;
        eprintln!("Preparing workload images...");
        let inject_disk =
            workload_inject_disk(&images, references, run_dir.path(), args.platform).await?;
        eprintln!("Booting simulation environment...");
        vm = Some(
            Vm::boot(
                &iso,
                run_dir.path(),
                args.timeout.into(),
                BootOutput::Hidden,
                args.memory,
                args.platform,
                &inject_disk,
            )
            .await?,
        );
        let environment = vm.as_mut().expect("environment booted");
        environment
            .run_script(PREPARE)
            .await
            .wrap_err("preparing the simulation environment")?;
        let mut configure = format!(
            "set -euo pipefail\n/opt/antithesis/bin/fault_scripts/randomization/configure.py --fault-tolerance {}",
            shell_quote(&args.fault_tolerance.to_string())
        );
        if let Some(exclusions) = &args.fault_exclusions {
            write!(
                configure,
                " --fault-exclusions={}",
                shell_quote(&exclusions.to_string())
            )
            .expect("writing to a String cannot fail");
        }
        configure.push('\n');
        environment
            .run_script(&configure)
            .await
            .wrap_err("configuring faults with configure.py")?;
        if args.fault_tolerance == crate::api::FaultTolerance::None {
            environment
                .run_script(DISABLE_FAULTS)
                .await
                .wrap_err("disabling faults")?;
        } else {
            environment
                .run_script("fault_injector_update --pause\n")
                .await
                .wrap_err("pausing faults before setup")?;
        }
        std::fs::write(run_dir.path().join(ATTACH_READY), "")?;
        if output.json {
            output
                .line(serde_json::json!({"type":"simulation_started", "run_id":run_id}).to_string())
                .await?;
        } else {
            eprintln!("Run ID: {run_id}");
            eprintln!("Attach: snouty simulate --attach {run_id}");
        }
        if output.closed {
            return Ok::<_, color_eyre::Report>(());
        }
        eprintln!("Uploading Compose configuration...");
        environment.upload_config(config.dir()).await?;
        instrumentation = Some(environment.instrumentation_log().to_owned());
        consumed = std::fs::metadata(environment.instrumentation_log())?.len();
        logs = Some(serial_lines(environment.instrumentation_log(), consumed)?);
        environment
            .run_script(START)
            .await
            .wrap_err("starting Compose project")?;
        eprintln!("Streaming simulation logs. Interrupt to stop simulation.");
        let mut cycle = WorkloadCycle::WaitingForSetup;
        let mut health = tokio::time::interval(Duration::from_secs(2));
        let logs = logs.as_mut().expect("log stream opened");
        loop {
            tokio::select! {
                line = logs.next() => {
                    let (end, line) = line.ok_or_eyre("instrumentation stream ended")??;
                    consumed = end;
                    let event = output.event(&line, &mut summary, RunPhase::Running).await?;
                    if output.closed { return Ok::<_, color_eyre::Report>(()); }
                    if let Some(event) = event
                        && matches!(event.kind, events::EventKind::SetupComplete { .. })
                        && matches!(cycle, WorkloadCycle::WaitingForSetup)
                    {
                        if args.fault_tolerance != crate::api::FaultTolerance::None {
                            environment.run_script("fault_injector_update --unpause\n").await.wrap_err("unpausing faults")?;
                        }
                        let workload = environment.run_script(WORKLOAD_KIND).await.wrap_err("classifying workload")?;
                        let workload = serde_json::from_str::<WorkloadKind>(&workload).wrap_err("reading workload kind")?;
                        environment.run_script(START_COMPOSER).await.wrap_err("starting test composer")?;
                        cycle = WorkloadCycle::Running { entropy_at: add_entropy(environment).await?, workload };
                    }
                },
                () = async {
                    tokio::select! {
                        _ = health.tick() => {},
                        () = async {
                            match cycle {
                                WorkloadCycle::Running { entropy_at, .. } | WorkloadCycle::ComposerCompleted { entropy_at } => tokio::time::sleep_until(entropy_at).await,
                                WorkloadCycle::WaitingForSetup => std::future::pending().await,
                            }
                        } => {},
                    }
                } => {
                    if !environment.is_running()? { bail!("QEMU exited unexpectedly\n{}", environment.boot_diagnostics()); }
                    if let WorkloadCycle::Running { entropy_at, ref workload } = cycle {
                        let status = tokio::time::timeout(Duration::from_secs(15), environment.run_script(STATUS)).await.wrap_err("health check timed out")?.wrap_err("checking service health")?;
                        if matches!(serde_json::from_str::<ComposerStatus>(&status).wrap_err("reading test composer status")?, ComposerStatus::Completed) {
                            if matches!(workload, WorkloadKind::Composed) && !args.disable_restart {
                                environment.run_script(RECREATE).await.wrap_err("recreating Compose project")?;
                                cycle = WorkloadCycle::WaitingForSetup;
                            } else {
                                eprintln!("Test composer completed; leaving the local Compose project running");
                                cycle = WorkloadCycle::ComposerCompleted { entropy_at };
                            }
                        }
                    }
                    match &mut cycle {
                        WorkloadCycle::Running { entropy_at, .. } | WorkloadCycle::ComposerCompleted { entropy_at } => {
                            if tokio::time::Instant::now() >= *entropy_at {
                                *entropy_at = add_entropy(environment).await?;
                            }
                        }
                        WorkloadCycle::WaitingForSetup => {},
                    }
                },
            }
        }
    };
    let reason = tokio::select! {
        reason = signals.receive() => reason,
        result = execution => match result {
            Ok(()) => Termination::OutputClosed,
            Err(error) => {
                summary.infrastructure_failures += 1;
                eprintln!("Simulation failed: {error:?}");
                Termination::InfrastructureFailure
            },
        },
    };
    let _ = std::fs::remove_file(run_dir.path().join(ATTACH_READY));
    // Records already written belong to the workload, even if output is behind.
    let cleanup_boundary = instrumentation
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map_or(consumed, |m| m.len());
    if let Some(environment) = &mut vm {
        eprintln!("Stopping simulation...");
        let shutdown = tokio::select! {
            result = environment.shutdown() => result,
            _ = signals.receive() => Ok(()),
        };
        if let Err(error) = shutdown {
            summary.infrastructure_failures += 1;
            eprintln!("VM shutdown failed: {error:#}");
        }
    }
    // Drop force-kills and reaps a VM if a second signal interrupted shutdown.
    drop(vm.take());
    drop(logs.take());
    let output_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    if let Some(path) = instrumentation
        && let Err(error) = drain(
            &path,
            consumed,
            cleanup_boundary,
            &mut output,
            &mut summary,
            output_deadline,
        )
        .await
    {
        summary.infrastructure_failures += 1;
        eprintln!("Instrumentation read failed: {error:#}");
    }
    for name in ["ssh_config", "images.tar", "images.erofs"] {
        let path = run_dir.path().join(name);
        if let Err(error) = std::fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::debug!("removing {}: {error}", path.display());
        }
    }
    let diagnostics = if summary.failed() {
        let path = run_dir.keep();
        eprintln!("Simulation logs: {}", path.display());
        Some(path)
    } else {
        None
    };
    let final_line = if output.json {
        serde_json::to_string(
            &serde_json::json!({"type":"summary", "termination":reason, "failures":summary, "diagnostics":diagnostics}),
        )?
    } else {
        format!(
            "Simulation stopped: {} assertion failures, {} command failures, {} infrastructure failures.",
            summary.assertion_failures, summary.command_failures, summary.infrastructure_failures
        )
    };
    match tokio::time::timeout_at(output_deadline, output.line(final_line)).await {
        Ok(result) => result?,
        Err(_) => output.closed = true,
    }
    Ok(summary.exit_code(reason))
}

/// Save the workload images and convert them into the inject drive.
async fn workload_inject_disk(
    images: &Images,
    references: BTreeSet<String>,
    run_dir: &Path,
    platform: super::SimulatePlatform,
) -> Result<PathBuf> {
    let references: Vec<_> = references.into_iter().collect();
    for reference in &references {
        images.inspect(reference, platform)?;
    }
    let archive = run_dir.join("images.tar");
    images.save(&references, &archive, platform).await?;
    let disk = inject_disk(vec![archive.clone()], run_dir).await?;
    std::fs::remove_file(archive)?;
    Ok(disk)
}

/// Convert image archives into the read-only EROFS drive that the environment
/// imports at boot. With no archives the drive holds an empty store.
async fn inject_disk(archives: Vec<PathBuf>, run_dir: &Path) -> Result<PathBuf> {
    let disk = run_dir.join("images.erofs");
    let output = std::fs::File::create(&disk)?;
    let converted = tokio::task::spawn_blocking(move || {
        let archives: Vec<&Path> = archives.iter().map(PathBuf::as_path).collect();
        crate::containers_storage::convert(&archives, output)
    })
    .await?
    .wrap_err("cannot convert workload images")?;
    log::debug!("converted workload images: {converted:?}");
    Ok(disk)
}

enum Environment {
    Iso(PathBuf),
    Image(String),
}

impl Environment {
    async fn iso(
        self,
        images: &Images,
        run_dir: &Path,
        platform: super::SimulatePlatform,
    ) -> Result<PathBuf> {
        match self {
            Self::Iso(path) => Ok(path),
            Self::Image(image) => images.environment_iso(&image, run_dir, platform).await,
        }
    }
}

fn resolve_environment(image: Option<String>, settings: &Settings) -> Result<Environment> {
    if let Some(path) = std::env::var_os(ENVIRONMENT_ISO_ENV) {
        let path = PathBuf::from(path);
        let iso = std::fs::canonicalize(&path).wrap_err_with(|| {
            format!("cannot use {ENVIRONMENT_ISO_ENV} path {}", path.display())
        })?;
        if !iso.is_file() {
            bail!(
                "{ENVIRONMENT_ISO_ENV} path is not a file: {}",
                path.display()
            );
        }
        return Ok(Environment::Iso(iso));
    }
    Ok(Environment::Image(if let Some(image) = image {
        with_default_tag(image)
    } else {
        let repository = crate::settings::require_setting(settings.repository(), "repository")?;
        format!(
            "{}/antithesis-guest:{ENVIRONMENT_VERSION}",
            repository.trim_end_matches('/')
        )
    }))
}

/// Tag a `--environment-image` that names neither a tag nor a digest with
/// [`ENVIRONMENT_VERSION`], so `REPOSITORY/antithesis-guest` means the default
/// release instead of `latest`.
fn with_default_tag(image: String) -> String {
    if crate::container::image_repo(&image) == image {
        format!("{image}:{ENVIRONMENT_VERSION}")
    } else {
        image
    }
}

pub(super) async fn shell(args: SimulateArgs, settings: &Settings) -> Result<ExitCode> {
    interactive_terminal("--shell")?;
    let environment = resolve_environment(args.environment_image, settings)?;
    let run_dir = tempfile::Builder::new()
        .prefix("snouty-simulate-")
        .tempdir()?;
    eprintln!("Preparing simulation environment image...");
    // Only an environment image needs a container engine.
    let iso = match environment {
        Environment::Iso(iso) => iso,
        Environment::Image(image) => {
            let runtime = container::runtime(settings)?;
            container::warn_ambiguous_engine(settings, runtime.as_ref(), false);
            Images::from_runtime(runtime.as_ref())
                .environment_iso(&image, run_dir.path(), args.platform)
                .await?
        }
    };
    let inject_disk = inject_disk(Vec::new(), run_dir.path()).await?;
    eprintln!("Booting simulation environment...");
    let mut signals = Signals::install()?;
    let mut environment = tokio::select! {
        biased;
        reason = signals.receive() => return Ok(Summary::default().exit_code(reason)),
        result = Vm::boot(
            &iso,
            run_dir.path(),
            args.timeout.into(),
            BootOutput::Visible,
            args.memory,
            args.platform,
            &inject_disk,
        ) => match result {
            Ok(environment) => environment,
            // The error names this directory for its logs.
            Err(error) => {
                let _ = run_dir.keep();
                return Err(error);
            }
        },
    };
    eprintln!("Opening a shell in the simulation environment. Exit the shell to stop it.");
    let shell = tokio::select! {
        biased;
        reason = signals.receive() => Err(reason),
        result = environment.interactive_shell() => Ok(result?),
    };
    let powered_off = match shell {
        Ok(status) if !status.success() => {
            environment
                .wait_for_poweroff(Duration::from_secs(2))
                .await?
        }
        _ => false,
    };
    eprintln!("Stopping simulation...");
    let shutdown_result = tokio::select! {
        result = environment.shutdown() => result,
        _ = signals.receive() => Ok(()),
    };
    if let Ok(status) = shell
        && !status.success()
        && !powered_off
    {
        bail!("shell exited with status: {status}");
    }
    shutdown_result?;
    Ok(match shell {
        Ok(_) => ExitCode::SUCCESS,
        Err(reason) => Summary::default().exit_code(reason),
    })
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

async fn drain(
    path: &Path,
    consumed: u64,
    cleanup_boundary: u64,
    output: &mut Output,
    summary: &mut Summary,
    output_deadline: tokio::time::Instant,
) -> Result<()> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(consumed))?;
    let mut reader = std::io::BufReader::new(file);
    let mut offset = consumed;
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let count = reader
            .by_ref()
            .take(MAX_RECORD_BYTES as u64 + 1)
            .read_until(b'\n', &mut bytes)?;
        if count == 0 {
            return Ok(());
        }
        if count > MAX_RECORD_BYTES {
            bail!("instrumentation line exceeds 4 MiB");
        }
        let phase = if offset < cleanup_boundary {
            RunPhase::Running
        } else {
            RunPhase::Cleanup
        };
        offset += count as u64;
        let line = String::from_utf8_lossy(&bytes);
        match tokio::time::timeout_at(
            output_deadline,
            output.event(line.trim_end_matches('\n'), summary, phase),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                summary.infrastructure_failures += 1;
                eprintln!("Writing instrumentation event failed: {error:#}");
            }
            Err(_) => output.closed = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    #[test]
    fn environment_image_without_tag_gets_default_version() {
        for (image, expected) in [
            ("environment", format!("environment:{ENVIRONMENT_VERSION}")),
            (
                "localhost:5000/team/antithesis-guest",
                format!("localhost:5000/team/antithesis-guest:{ENVIRONMENT_VERSION}"),
            ),
            ("environment:test", "environment:test".to_string()),
            (
                "environment@sha256:abc",
                "environment@sha256:abc".to_string(),
            ),
            (
                "environment:test@sha256:abc",
                "environment:test@sha256:abc".to_string(),
            ),
        ] {
            assert_eq!(with_default_tag(image.to_string()), expected, "{image}");
        }
    }

    #[hegel::test]
    fn recorded_failures_survive_interruption(tc: hegel::TestCase) {
        let assertions = tc.draw(generators::integers::<u16>());
        let commands = tc.draw(generators::integers::<u16>());
        let infrastructure = tc.draw(generators::integers::<u16>());
        let summary = Summary {
            assertion_failures: assertions.into(),
            command_failures: commands.into(),
            infrastructure_failures: infrastructure.into(),
        };
        for reason in [
            Termination::Interrupted,
            Termination::Terminated,
            Termination::OutputClosed,
        ] {
            if assertions != 0 || commands != 0 || infrastructure != 0 {
                assert_eq!(summary.exit_code(reason), ExitCode::FAILURE);
            }
        }
    }

    #[tokio::test]
    async fn final_drain_accounts_for_backlog_and_partial_last_record_after_output_closes() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let command = "1 [antithesis_test_composer] [JSON] '{\"task_status\":\"finished\",\"command\":\"test\",\"command_return_code\":\"1\"}'\n";
        let mut before_cleanup = "1 [workload] [INFO] 'buffered output'\n".repeat(32768);
        before_cleanup.push_str(command);
        let cutoff = before_cleanup.len() as u64;
        let assertion = "2 [workload] [JSON] '{\"antithesis_assert\":{\"assert_type\":\"always\",\"hit\":true,\"condition\":false}}'";
        std::fs::write(file.path(), format!("{before_cleanup}{command}{assertion}")).unwrap();
        let mut output = Output::new(true).unwrap();
        output.closed = true;
        let mut summary = Summary::default();
        drain(
            file.path(),
            0,
            cutoff,
            &mut output,
            &mut summary,
            tokio::time::Instant::now(),
        )
        .await
        .unwrap();
        assert_eq!(summary.command_failures, 1);
        assert_eq!(summary.assertion_failures, 1);
        assert_eq!(summary.infrastructure_failures, 0);
    }

    #[test]
    fn cleanup_does_not_turn_terminated_commands_into_failures() {
        let mut summary = Summary::default();
        let event = events::parse_line("1 [antithesis_test_composer] [JSON] '{\"task_status\":\"finished\",\"command\":\"test\",\"command_return_code\":\"143\"}'").unwrap().unwrap();
        summary.observe(&event, RunPhase::Cleanup);
        assert!(!summary.failed());
        summary.observe(&event, RunPhase::Running);
        assert_eq!(
            summary.exit_code(Termination::Interrupted),
            ExitCode::FAILURE
        );
    }
}
