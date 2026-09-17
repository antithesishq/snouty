use std::collections::BTreeSet;
use std::io::{BufRead, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::pin::Pin;
use std::process::ExitCode;
use std::time::Duration;

use color_eyre::eyre::{OptionExt, Result, bail};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{Signal, SignalKind, signal};

use super::{
    events::{self, Event, Failure},
    images::{ImageId, Images},
    vm::{BootOutput, ShutdownSignal, Vm},
};
use crate::{
    OutputOptions, cli::SimulateArgs, compose, config::ComposeConfig, container, settings::Settings,
};

const PREPARE: &str = include_str!("assets/prepare.sh");
const START: &str = include_str!("assets/start.sh");
const STOP: &str = include_str!("assets/stop.sh");
const STATUS: &str = include_str!("assets/status.sh");
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

type LogStream = Pin<Box<dyn Stream<Item = Result<(u64, String)>> + Send>>;
const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;

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
        if let Some(event) = events::parse_line(line)? {
            summary.observe(&event, phase);
            if !self.closed {
                self.line(if self.json {
                    serde_json::to_string(&event)?
                } else {
                    event.render()
                })
                .await?;
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
    let guest_image = resolve_guest_image(args.guest_image, settings, options.verbose).await?;
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
        .prefix("snouty-simulate-")
        .tempdir()?;
    let mut vm = None;
    let mut logs: Option<LogStream> = None;
    let mut summary = Summary::default();
    let mut output = Output::new(options.json)?;
    let mut consumed = 0;
    let mut instrumentation = None;
    let execution = async {
        eprintln!("Preparing guest image...");
        let iso = images.guest_iso(&guest_image, run_dir.path()).await?;
        eprintln!("Booting guest...");
        vm = Some(
            Vm::boot(
                &iso,
                run_dir.path(),
                args.timeout.into(),
                BootOutput::Hidden,
            )
            .await?,
        );
        let guest = vm.as_mut().expect("guest booted");
        guest.run_script(PREPARE).await?;
        eprintln!("Uploading Compose configuration and images...");
        guest.upload_config(config.dir()).await?;
        let mut changed = Vec::new();
        for reference in references {
            let local = images.inspect(&reference).await?;
            let script = format!(
                "set -euo pipefail\nif podman image exists {image}; then podman image inspect {image}; else printf '[]'; fi\n",
                image = shell_quote(&reference)
            );
            #[derive(Deserialize)]
            struct RemoteImage {
                #[serde(rename = "Id")]
                id: ImageId,
            }
            let remote: Vec<RemoteImage> = serde_json::from_str(&guest.run_script(&script).await?)?;
            let same = remote.first().is_some_and(|image| image.id == local.id);
            if !same {
                changed.push((reference, local.id.to_string()));
            }
        }
        if !changed.is_empty() {
            let archive = run_dir.path().join("images.tar");
            let ids: Vec<_> = changed.iter().map(|(_, id)| id.clone()).collect();
            images.save(&ids, &archive).await?;
            guest.load_images(&archive).await?;
            for (reference, id) in changed {
                guest
                    .run_script(&format!(
                        "podman tag {} {}\n",
                        shell_quote(&id),
                        shell_quote(&reference)
                    ))
                    .await?;
            }
            std::fs::remove_file(archive)?;
        }
        instrumentation = Some(guest.instrumentation_log().to_owned());
        consumed = std::fs::metadata(guest.instrumentation_log())?.len();
        logs = Some(serial_lines(guest.instrumentation_log(), consumed)?);
        let start = format!(
            "restart_enabled={}\n{START}",
            if args.disable_restart { "no" } else { "yes" }
        );
        guest.run_script(&start).await?;
        eprintln!("Streaming guest logs. Interrupt to stop simulation.");
        let status = if args.disable_restart {
            format!("unit=antithesis-test-composer.service\n{STATUS}")
        } else {
            STATUS.to_owned()
        };
        let mut health = tokio::time::interval(Duration::from_secs(2));
        let logs = logs.as_mut().expect("log stream opened");
        loop {
            tokio::select! {
                line = logs.next() => {
                    let (end, line) = line.ok_or_eyre("instrumentation stream ended")??;
                    consumed = end;
                    output.event(&line, &mut summary, RunPhase::Running).await?;
                    if output.closed { return Ok::<_, color_eyre::Report>(()); }
                },
                _ = health.tick() => {
                    if !guest.is_running()? { bail!("QEMU exited unexpectedly\n{}", guest.boot_diagnostics()); }
                    tokio::time::timeout(Duration::from_secs(15), guest.run_script(&status)).await??;
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
    // Records already written belong to the workload, even if output is behind.
    let cleanup_boundary = instrumentation
        .as_ref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map_or(consumed, |m| m.len());
    if let Some(guest) = &mut vm {
        eprintln!("Stopping simulation...");
        let cleanup = tokio::select! {
            result = tokio::time::timeout(CLEANUP_TIMEOUT, guest.run_script(STOP)) => Some(result),
            _ = signals.receive() => None,
        };
        let force_shutdown = cleanup.is_none();
        match cleanup {
            Some(Ok(Ok(_))) | None => {}
            Some(Ok(Err(error))) => {
                summary.infrastructure_failures += 1;
                eprintln!("Guest cleanup failed: {error:#}");
            }
            Some(Err(_)) => {
                summary.infrastructure_failures += 1;
                eprintln!(
                    "Guest cleanup timed out after {} seconds",
                    CLEANUP_TIMEOUT.as_secs()
                );
            }
        }
        let signal = match reason {
            Termination::Interrupted => ShutdownSignal::Interrupt,
            _ => ShutdownSignal::Terminate,
        };
        let shutdown = if force_shutdown {
            Ok(())
        } else {
            tokio::select! {
                result = guest.shutdown(signal) => result,
                _ = signals.receive() => Ok(()),
            }
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
    for name in ["guest-dev-key", "ssh_config", "known_hosts", "images.tar"] {
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

async fn resolve_guest_image(
    image: Option<String>,
    settings: &Settings,
    verbose: bool,
) -> Result<String> {
    Ok(match image {
        Some(image) => image,
        None => {
            let repository = crate::settings::require(settings.repository(), "repository")?;
            let api = crate::api::AntithesisApi::new(settings, verbose)?;
            let version = api.get_version().await.map_err(|error| {
                use crate::api::VersionError;
                let reason = match error {
                    VersionError::Http(status) => format!("GET /api/version returned HTTP {status}"),
                    VersionError::BadResponse(reason) | VersionError::Unreachable(reason) => reason,
                };
                color_eyre::eyre::eyre!("cannot determine guest image from tenant release: {reason}; use --guest-image to select an image explicitly")
            })?;
            format!(
                "{}/antithesis-guest:v{}",
                repository.trim_end_matches('/'),
                version.release_version
            )
        }
    })
}

pub(super) async fn shell(
    args: SimulateArgs,
    settings: &Settings,
    verbose: bool,
) -> Result<ExitCode> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("simulate --shell requires an interactive terminal");
    }
    let guest_image = resolve_guest_image(args.guest_image, settings, verbose).await?;
    let runtime = container::runtime(settings)?;
    container::warn_ambiguous_engine(settings, runtime.as_ref(), false);
    let images = Images::from_runtime(runtime.as_ref());
    let run_dir = tempfile::Builder::new()
        .prefix("snouty-simulate-")
        .tempdir()?;
    eprintln!("Preparing guest image...");
    let iso = images.guest_iso(&guest_image, run_dir.path()).await?;
    eprintln!("Booting guest...");
    let mut guest = Vm::boot(
        &iso,
        run_dir.path(),
        args.timeout.into(),
        BootOutput::Visible,
    )
    .await?;
    eprintln!("Opening guest shell. Exit the shell to stop the VM.");
    let shell_status = guest.interactive_shell().await?;
    let powered_off =
        !shell_status.success() && guest.wait_for_poweroff(Duration::from_secs(2)).await?;
    eprintln!("Stopping simulation...");
    let shutdown_result = guest.shutdown(ShutdownSignal::Terminate).await;
    if !shell_status.success() && !powered_off {
        bail!("guest shell exited with status: {shell_status}");
    }
    shutdown_result?;
    Ok(ExitCode::SUCCESS)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn serial_lines(path: &Path, start: u64) -> Result<LogStream> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    Ok(Box::pin(futures_util::stream::try_unfold(
        (file, Vec::<u8>::new(), start),
        |(mut file, mut pending, mut offset)| async move {
            loop {
                if let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                    let bytes: Vec<_> = pending.drain(..=end).collect();
                    let line = String::from_utf8_lossy(&bytes[..end]).into_owned();
                    offset += bytes.len() as u64;
                    return Ok(Some(((offset, line), (file, pending, offset))));
                }
                let mut buffer = [0; 8192];
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    pending.extend_from_slice(&buffer[..count]);
                    if pending.len() > MAX_RECORD_BYTES {
                        bail!("instrumentation line exceeds 4 MiB");
                    }
                }
            }
        },
    )))
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
    async fn tail_preserves_partial_lines_across_cancelled_reads() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "old boot event\n").unwrap();
        let mut stream =
            serial_lines(file.path(), std::fs::metadata(file.path()).unwrap().len()).unwrap();
        let mut writer = std::fs::OpenOptions::new()
            .append(true)
            .open(file.path())
            .unwrap();
        write!(writer, "10 [workload] [INFO] 'part").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(80), stream.next())
                .await
                .is_err()
        );
        writeln!(writer, "ial'").unwrap();
        let line = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(line.1, "10 [workload] [INFO] 'partial'");
        assert!(events::parse_line(&line.1).unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_ends_after_an_oversized_record() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut stream =
            serial_lines(file.path(), std::fs::metadata(file.path()).unwrap().len()).unwrap();
        std::fs::write(file.path(), vec![b'x'; 4 * 1024 * 1024 + 1]).unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
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
