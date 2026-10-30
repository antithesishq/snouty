use std::collections::BTreeSet;
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
    images::{ImageId, Images},
    serial::{LogStream, MAX_RECORD_BYTES, serial_lines},
    vm::{BootOutput, GuestConnection, Vm},
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
const RECREATE: &str = include_str!("assets/recreate.sh");
const STATUS: &str = include_str!("assets/status.sh");
const ATTACH_READY: &str = "attach-ready";
const GUEST_VERSION: &str = "v66.0";
const GUEST_ISO_ENV: &str = "SNOUTY_SIMULATE_GUEST_ISO";

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComposerStatus {
    Running,
    Completed,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecreateStatus {
    Restarted,
    Idle,
}

enum WorkloadCycle {
    WaitingForSetup,
    Running { entropy_at: tokio::time::Instant },
    Finished,
}

async fn add_entropy(guest: &Vm) -> Result<tokio::time::Instant> {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).map_err(|error| eyre!("generating entropy seed: {error}"))?;
    let next = tokio::time::Instant::now() + Duration::from_secs(30);
    guest
        .run_script(&format!(
            "/etc/fuzzing/add_entropy {} true\n",
            u64::from_ne_bytes(bytes)
        ))
        .await
        .wrap_err("adding guest entropy")?;
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
        let metadata = std::fs::symlink_metadata(&directory)?;
        if !metadata.is_dir()
            || metadata.uid() != nix::unistd::geteuid().as_raw()
            || metadata.permissions().mode() & 0o077 != 0
        {
            bail!(
                "simulation state directory is not private: {}",
                directory.display()
            );
        }
    }
    Ok(root)
}

fn interactive_terminal(option: &str) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("simulate {option} requires an interactive terminal");
    }
    Ok(())
}

pub(super) fn attach(id: SimulationId) -> Result<ExitCode> {
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
    GuestConnection::new(&path).exec_interactive_shell()
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
                let mut stdout = std::io::stdout().lock();
                while let Some(line) = receiver.blocking_recv() {
                    let result = writeln!(stdout, "{}", line.text).and_then(|_| stdout.flush());
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

    async fn event(&mut self, line: &str, summary: &mut Summary, phase: RunPhase) -> Result<()> {
        let event = events::parse_line(line)?;
        let mut entry = if event.is_some() || events::is_fault_injector_line(line) {
            events::log_entry(line)?
        } else {
            None
        };
        if let Some(entry) = &mut entry {
            self.annotator.annotate(entry);
        }
        if let Some(event) = event {
            summary.observe(&event, phase);
            if !self.closed {
                let line = if self.json {
                    let mut value = serde_json::to_value(&event)?;
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
                        let mut rendered = String::new();
                        self.renderer.render_entry(entry, &mut rendered)?;
                        rendered
                    }
                };
                self.line(line).await?;
            }
        }
        Ok(())
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
    let guest = resolve_guest(args.guest_image, settings)?;
    let runtime = container::runtime(settings)?;
    container::warn_ambiguous_engine(settings, runtime.as_ref(), options.json);
    let compose = compose::DockerCompose::resolve(runtime.as_ref(), config.clone())?;
    crate::validate::check_compose_divergence(&compose, false)?;
    let contents = compose.contents(None)?;
    compose::validate_images_are_available(runtime.as_ref(), &contents)?;
    if contents.services.is_empty() {
        bail!("docker-compose.yaml has no services");
    }
    if contents
        .services
        .iter()
        .any(|service| service.default_image)
    {
        bail!(
            "simulate requires an explicit image for each service; add image entries and build them first"
        );
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
        eprintln!("Preparing guest image...");
        let iso = guest.iso(&images, run_dir.path(), args.platform).await?;
        eprintln!("Booting guest...");
        let scratch_disk = create_scratch_disk(run_dir.path()).await?;
        vm = Some(
            Vm::boot(
                &iso,
                run_dir.path(),
                args.timeout.into(),
                BootOutput::Hidden,
                args.memory,
                args.platform,
                Some(&scratch_disk),
            )
            .await?,
        );
        let guest = vm.as_mut().expect("guest booted");
        guest
            .run_script(SCRATCH_SETUP)
            .await
            .wrap_err("setting up guest scratch storage")?;
        guest
            .run_script(PREPARE)
            .await
            .wrap_err("preparing guest")?;
        let mut configure = format!(
            "set -euo pipefail\n/opt/antithesis/bin/fault_scripts/randomization/configure.py --fault-tolerance {}",
            shell_quote(&args.fault_tolerance.to_string())
        );
        if let Some(exclusions) = &args.fault_exclusions {
            configure.push_str(&format!(" --fault-exclusions={}", shell_quote(exclusions)));
        }
        configure.push('\n');
        guest
            .run_script(&configure)
            .await
            .wrap_err("configuring guest faults with configure.py")?;
        if args.fault_tolerance == super::FaultTolerance::None {
            guest
                .run_script(DISABLE_FAULTS)
                .await
                .wrap_err("disabling guest faults")?;
        } else {
            guest
                .run_script("fault_injector_update --pause\n")
                .await
                .wrap_err("pausing guest faults before setup")?;
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
        eprintln!("Uploading Compose configuration and images...");
        guest.upload_config(config.dir()).await?;
        let mut changed = Vec::new();
        for reference in references {
            let local = images.inspect(&reference, args.platform).await?;
            let script = format!(
                "set -euo pipefail\nif podman image exists {image}; then podman image inspect {image}; else printf '[]'; fi\n",
                image = shell_quote(&reference)
            );
            #[derive(Deserialize)]
            struct RemoteImage {
                #[serde(rename = "Id")]
                id: ImageId,
            }
            let remote: Vec<RemoteImage> = serde_json::from_str(
                &guest
                    .run_script(&script)
                    .await
                    .wrap_err_with(|| format!("inspecting guest image {reference}"))?,
            )?;
            let same = remote.first().is_some_and(|image| image.id == local.id);
            if !same {
                changed.push(reference);
            }
        }
        if !changed.is_empty() {
            let archive = run_dir.path().join("images.tar");
            images.save(&changed, &archive, args.platform).await?;
            guest.load_images(&archive).await?;
            std::fs::remove_file(archive)?;
        }
        instrumentation = Some(guest.instrumentation_log().to_owned());
        consumed = std::fs::metadata(guest.instrumentation_log())?.len();
        logs = Some(serial_lines(guest.instrumentation_log(), consumed)?);
        guest
            .run_script(START)
            .await
            .wrap_err("starting guest Compose project")?;
        eprintln!("Streaming guest logs. Interrupt to stop simulation.");
        let mut cycle = WorkloadCycle::WaitingForSetup;
        let mut health = tokio::time::interval(Duration::from_secs(2));
        let logs = logs.as_mut().expect("log stream opened");
        loop {
            tokio::select! {
                line = logs.next() => {
                    let (end, line) = line.ok_or_eyre("instrumentation stream ended")??;
                    consumed = end;
                    if let Some(event) = events::parse_line(&line)?
                        && matches!(event.kind, events::EventKind::SetupComplete { .. })
                        && matches!(cycle, WorkloadCycle::WaitingForSetup)
                    {
                        if args.fault_tolerance != super::FaultTolerance::None {
                            guest.run_script("fault_injector_update --unpause\n").await.wrap_err("unpausing guest faults")?;
                        }
                        guest.run_script(START_COMPOSER).await.wrap_err("starting guest test composer")?;
                        cycle = WorkloadCycle::Running { entropy_at: add_entropy(guest).await? };
                    }
                    output.event(&line, &mut summary, RunPhase::Running).await?;
                    if output.closed { return Ok::<_, color_eyre::Report>(()); }
                },
                _ = async {
                    tokio::select! {
                        _ = health.tick() => {},
                        _ = async {
                            match cycle {
                                WorkloadCycle::Running { entropy_at } => tokio::time::sleep_until(entropy_at).await,
                                _ => std::future::pending().await,
                            }
                        } => {},
                    }
                } => {
                    if !guest.is_running()? { bail!("QEMU exited unexpectedly\n{}", guest.boot_diagnostics()); }
                    if let WorkloadCycle::Running { entropy_at } = cycle {
                        let status = tokio::time::timeout(Duration::from_secs(15), guest.run_script(STATUS)).await.wrap_err("guest health check timed out")?.wrap_err("checking guest service health")?;
                        match serde_json::from_str::<ComposerStatus>(&status).wrap_err("reading test composer status")? {
                            ComposerStatus::Running => {
                                if tokio::time::Instant::now() >= entropy_at {
                                    cycle = WorkloadCycle::Running { entropy_at: add_entropy(guest).await? };
                                }
                            },
                            ComposerStatus::Completed => {
                                cycle = WorkloadCycle::Finished;
                                if !args.disable_restart {
                                    let status = guest.run_script(RECREATE).await.wrap_err("recreating guest Compose project")?;
                                    match serde_json::from_str::<RecreateStatus>(&status).wrap_err("reading Compose recreation status")? {
                                        RecreateStatus::Restarted => cycle = WorkloadCycle::WaitingForSetup,
                                        RecreateStatus::Idle => eprintln!("Test composer exited without runnable commands; leaving the local Compose project running"),
                                    }
                                }
                            },
                        }
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
                eprintln!("Simulation failed: {error:#}");
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
    if let Some(guest) = &mut vm {
        eprintln!("Stopping simulation...");
        let shutdown = tokio::select! {
            result = guest.shutdown() => result,
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
    for name in ["ssh_config", "images.tar", "scratch.qcow2"] {
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

const SCRATCH_SETUP: &str = include_str!("assets/scratch-storage.sh");

async fn create_scratch_disk(run_dir: &Path) -> Result<PathBuf> {
    let disk = run_dir.join("scratch.qcow2");
    let output = tokio::process::Command::new("qemu-img")
        .args(["create", "-f", "qcow2"])
        .arg(&disk)
        .arg(super::vm::SCRATCH_DISK_SIZE)
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "cannot create simulation scratch disk: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(disk)
}

enum Guest {
    Iso(PathBuf),
    Image(String),
}

impl Guest {
    async fn iso(
        self,
        images: &Images,
        run_dir: &Path,
        platform: super::SimulatePlatform,
    ) -> Result<PathBuf> {
        match self {
            Self::Iso(path) => Ok(path),
            Self::Image(image) => images.guest_iso(&image, run_dir, platform).await,
        }
    }
}

fn resolve_guest(image: Option<String>, settings: &Settings) -> Result<Guest> {
    if let Some(path) = std::env::var_os(GUEST_ISO_ENV) {
        let path = PathBuf::from(path);
        let iso = std::fs::canonicalize(&path)
            .wrap_err_with(|| format!("cannot use {GUEST_ISO_ENV} path {}", path.display()))?;
        if !iso.is_file() {
            bail!("{GUEST_ISO_ENV} path is not a file: {}", path.display());
        }
        return Ok(Guest::Iso(iso));
    }
    Ok(Guest::Image(match image {
        Some(image) => image,
        None => {
            let repository = crate::settings::require(settings.repository(), "repository")?;
            format!(
                "{}/antithesis-guest:{GUEST_VERSION}",
                repository.trim_end_matches('/')
            )
        }
    }))
}

pub(super) async fn shell(args: SimulateArgs, settings: &Settings) -> Result<ExitCode> {
    interactive_terminal("--shell")?;
    let guest = resolve_guest(args.guest_image, settings)?;
    let runtime = container::runtime(settings)?;
    container::warn_ambiguous_engine(settings, runtime.as_ref(), false);
    let images = Images::from_runtime(runtime.as_ref());
    let run_dir = tempfile::Builder::new()
        .prefix("snouty-simulate-")
        .tempdir()?;
    eprintln!("Preparing guest image...");
    let iso = guest.iso(&images, run_dir.path(), args.platform).await?;
    let scratch_disk = create_scratch_disk(run_dir.path()).await?;
    eprintln!("Booting guest...");
    let mut signals = Signals::install()?;
    let mut guest = tokio::select! {
        biased;
        reason = signals.receive() => return Ok(Summary::default().exit_code(reason)),
        result = Vm::boot(
            &iso,
            run_dir.path(),
            args.timeout.into(),
            BootOutput::Visible,
            args.memory,
            args.platform,
            Some(&scratch_disk),
        ) => result?,
    };
    guest
        .run_script(SCRATCH_SETUP)
        .await
        .wrap_err("setting up guest scratch storage")?;
    eprintln!("Opening guest shell. Exit the shell to stop the VM.");
    let shell = tokio::select! {
        biased;
        reason = signals.receive() => Err(reason),
        result = guest.interactive_shell() => Ok(result?),
    };
    let powered_off = match shell {
        Ok(status) if !status.success() => guest.wait_for_poweroff(Duration::from_secs(2)).await?,
        _ => false,
    };
    eprintln!("Stopping simulation...");
    let shutdown_result = tokio::select! {
        result = guest.shutdown() => result,
        _ = signals.receive() => Ok(()),
    };
    if let Ok(status) = shell
        && !status.success()
        && !powered_off
    {
        bail!("guest shell exited with status: {status}");
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
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                summary.infrastructure_failures += 1;
                eprintln!("Invalid instrumentation event: {error:#}");
            }
            Err(_) => output.closed = true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

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
