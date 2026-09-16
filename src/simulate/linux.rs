use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom, Write};
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
    vm::{ShutdownSignal, Vm},
};
use crate::{
    OutputOptions, cli::SimulateArgs, compose, config::ComposeConfig, container, settings::Settings,
};

const PREPARE: &str = include_str!("assets/prepare.sh");
const START: &str = include_str!("assets/start.sh");
const STOP: &str = include_str!("assets/stop.sh");
const STATUS: &str = include_str!("assets/status.sh");
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

type LogStream = Pin<Box<dyn Stream<Item = Result<String>> + Send>>;

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
}

impl Output {
    fn line(&mut self, line: &str) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let mut stdout = std::io::stdout().lock();
        match writeln!(stdout, "{line}").and_then(|_| stdout.flush()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                self.closed = true;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn event(&mut self, line: &str, summary: &mut Summary, phase: RunPhase) -> Result<()> {
        if let Some(event) = events::parse_line(line)? {
            summary.observe(&event, phase);
            self.line(&if self.json {
                serde_json::to_string(&event)?
            } else {
                event.render()
            })?;
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
    let mut signals = Signals::install()?;
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
    let images = Images::from_runtime(runtime.as_ref());
    let run_dir = tempfile::Builder::new()
        .prefix("snouty-simulate-")
        .tempdir()?;
    let mut vm = None;
    let mut logs: Option<LogStream> = None;
    let mut summary = Summary::default();
    let mut output = Output {
        json: options.json,
        closed: false,
    };
    let execution = async {
        eprintln!("Preparing guest image...");
        let iso = images.guest_iso(&args.guest_image, run_dir.path()).await?;
        eprintln!("Booting guest (1 CPU, 15000 MiB)...");
        vm = Some(Vm::boot(&iso, run_dir.path(), args.timeout.into()).await?);
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
        logs = Some(serial_lines(guest.instrumentation_log())?);
        let start = format!(
            "restart_enabled={}\n{START}",
            if args.disable_restart { "no" } else { "yes" }
        );
        guest.run_script(&start).await?;
        eprintln!("Streaming guest logs. Interrupt to stop simulation.");
        let mut health = tokio::time::interval(Duration::from_secs(2));
        let logs = logs.as_mut().expect("log stream opened");
        loop {
            tokio::select! {
                line = logs.next() => {
                    let line = line.ok_or_eyre("instrumentation stream ended")??;
                    output.event(&line, &mut summary, RunPhase::Running)?;
                    if output.closed { return Ok::<_, color_eyre::Report>(()); }
                },
                _ = health.tick() => {
                    if !guest.is_running()? { bail!("QEMU exited unexpectedly\n{}", guest.boot_diagnostics()); }
                    if !args.disable_restart { tokio::time::timeout(Duration::from_secs(15), guest.run_script(STATUS)).await??; }
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
    // Drain records already emitted before cleanup can terminate test commands.
    drain(&mut logs, &mut output, &mut summary, RunPhase::Running).await;
    if let Some(guest) = &mut vm {
        eprintln!("Stopping simulation...");
        let cleanup = tokio::select! {
            result = tokio::time::timeout(CLEANUP_TIMEOUT, guest.run_script(STOP)) => Some(result),
            _ = signals.receive() => None,
        };
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
        if let Err(error) = guest.shutdown(signal).await {
            summary.infrastructure_failures += 1;
            eprintln!("VM shutdown failed: {error:#}");
        }
    }
    drain(&mut logs, &mut output, &mut summary, RunPhase::Cleanup).await;
    for name in [
        "guest-dev-key",
        "ssh_config",
        "known_hosts",
        "images.tar",
        "qmp.sock",
    ] {
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
    output.line(&final_line)?;
    Ok(summary.exit_code(reason))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn serial_lines(path: &Path) -> Result<LogStream> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(Box::pin(futures_util::stream::try_unfold(
        (file, Vec::<u8>::new()),
        |(mut file, mut pending)| async move {
            loop {
                if let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                    let bytes: Vec<_> = pending.drain(..=end).collect();
                    let line = String::from_utf8_lossy(&bytes[..end]).into_owned();
                    return Ok(Some((line, (file, pending))));
                }
                let mut buffer = [0; 8192];
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    pending.extend_from_slice(&buffer[..count]);
                    if pending.len() > 4 * 1024 * 1024 {
                        bail!("instrumentation line exceeds 4 MiB");
                    }
                }
            }
        },
    )))
}

async fn drain(
    logs: &mut Option<LogStream>,
    output: &mut Output,
    summary: &mut Summary,
    phase: RunPhase,
) {
    let Some(logs) = logs else {
        return;
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(150), logs.next()).await {
            Ok(Some(Ok(line))) => {
                if let Err(error) = output.event(&line, summary, phase) {
                    summary.infrastructure_failures += 1;
                    eprintln!("Invalid instrumentation event: {error:#}");
                }
            }
            Ok(Some(Err(error))) => {
                summary.infrastructure_failures += 1;
                eprintln!("Instrumentation read failed: {error:#}");
                break;
            }
            _ => break,
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
        let mut stream = serial_lines(file.path()).unwrap();
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
        assert_eq!(line, "10 [workload] [INFO] 'partial'");
        assert!(events::parse_line(&line).unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_ends_after_an_oversized_record() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut stream = serial_lines(file.path()).unwrap();
        std::fs::write(file.path(), vec![b'x'; 4 * 1024 * 1024 + 1]).unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
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
