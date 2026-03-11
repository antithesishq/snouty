use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result, bail};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Duration, sleep};

use crate::cli::ValidateArgs;
use crate::container;

#[derive(Deserialize)]
struct SetupEvent {
    antithesis_setup: Option<SetupStatus>,
}

#[derive(Deserialize)]
struct SetupStatus {
    status: String,
}

/// Generate a compose override file that injects Antithesis SDK output monitoring.
///
/// For each service in the resolved compose YAML, adds:
/// - A volume mount: `{temp_dir}/antithesis:/tmp/antithesis`
/// - An environment variable: `ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/sdk_output.jsonl`
///
/// The SDK creates the output file; we mount the parent directory so it can do so.
///
/// `compose_yaml` should be the resolved output of `compose_config()`.
/// Returns the path to the generated override file.
fn generate_setup_override(compose_yaml: &str, temp_dir: &Path) -> Result<PathBuf> {
    let entries = container::parse_compose_config(compose_yaml)?;
    if entries.is_empty() {
        bail!("no services found in docker-compose.yaml");
    }

    let antithesis_dir = temp_dir.join("antithesis");
    std::fs::create_dir_all(&antithesis_dir)
        .wrap_err("failed to create antithesis output directory")?;

    let vol = format!("{}:/tmp/antithesis", antithesis_dir.display());

    let mut services = serde_yaml::Mapping::new();
    for (name, _) in &entries {
        let mut svc = serde_yaml::Mapping::new();
        svc.insert(
            serde_yaml::Value::String("volumes".to_string()),
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::String(vol.clone())]),
        );
        let mut env = serde_yaml::Mapping::new();
        env.insert(
            serde_yaml::Value::String("ANTITHESIS_SDK_LOCAL_OUTPUT".to_string()),
            serde_yaml::Value::String("/tmp/antithesis/sdk_output.jsonl".to_string()),
        );
        svc.insert(
            serde_yaml::Value::String("environment".to_string()),
            serde_yaml::Value::Mapping(env),
        );
        services.insert(
            serde_yaml::Value::String(name.clone()),
            serde_yaml::Value::Mapping(svc),
        );
    }

    let mut doc = serde_yaml::Mapping::new();
    doc.insert(
        serde_yaml::Value::String("services".to_string()),
        serde_yaml::Value::Mapping(services),
    );

    let override_yaml =
        serde_yaml::to_string(&doc).wrap_err("failed to serialize compose override")?;

    let override_path = temp_dir.join("override.yml");
    std::fs::write(&override_path, &override_yaml)
        .wrap_err("failed to write compose override file")?;

    Ok(override_path)
}

pub async fn cmd_validate(args: ValidateArgs) -> Result<()> {
    container::validate_config_dir(&args.config)?;
    let rt = container::runtime()?;

    let temp_dir = tempfile::tempdir()?;
    let compose_yaml = rt.compose_config(&args.config)?;
    let override_path = generate_setup_override(&compose_yaml, temp_dir.path())?;

    eprintln!("Starting compose services...");
    let (mut child, mut output) = container::compose_up(
        rt.name(),
        &args.config,
        &[override_path.as_path()],
        &["--abort-on-container-exit"],
    )?;

    // Spawn task to forward compose output to stderr
    let forward_handle = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match output.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let _ = tokio::io::stderr().write_all(&buf[..n]).await;
                }
                Err(_) => break,
            }
        }
    });

    let sdk_output_file = temp_dir.path().join("antithesis/sdk_output.jsonl");
    let timeout = Duration::from_secs(args.timeout);

    let result = tokio::select! {
        status = child.wait() => {
            match status {
                Ok(status) if !status.success() => {
                    bail!("compose exited with status: {}", status);
                }
                Ok(_) => {
                    bail!("compose exited before setup-complete event was detected");
                }
                Err(e) => {
                    bail!("failed to wait for compose: {}", e);
                }
            }
        }
        result = watch_for_setup_complete(&sdk_output_file, timeout) => {
            result
        }
        _ = tokio::signal::ctrl_c() => {
            Err(color_eyre::eyre::eyre!("interrupted"))
        }
    };

    // Cleanup: kill child and compose down
    forward_handle.abort();
    let _ = child.kill().await;
    let _ = child.wait().await;
    container::compose_down(rt.name(), &args.config, &[override_path.as_path()]).await;

    match result {
        Ok(()) => {
            eprintln!("\nSetup validation successful! The setup-complete event was detected.");
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Watch a JSONL file for the `{"antithesis_setup": {"status": "complete"}}` event.
///
/// Two-phase polling: first waits for the file to appear (250ms interval), then
/// tails it for new data (100ms idle interval). Returns an error if the event is
/// not found within the given timeout.
///
/// Uses blocking `std::fs` calls (open, seek, read) intentionally — reads are
/// small and infrequent, and this avoids pulling in tokio::fs for a simple poll loop.
async fn watch_for_setup_complete(output_file: &Path, timeout: Duration) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let deadline = tokio::time::Instant::now() + timeout;
    let mut offset: u64 = 0;
    let mut buf = vec![0u8; 4096];
    let mut remainder = String::new();

    // Phase 1: wait for the file to appear.
    let mut f = loop {
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out after {}s waiting for setup-complete event",
                timeout.as_secs()
            );
        }
        sleep(Duration::from_millis(250)).await;
        match std::fs::File::open(output_file) {
            Ok(f) => break f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    };

    // Phase 2: tail the file for the setup-complete event.
    loop {
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "timed out after {}s waiting for setup-complete event",
                timeout.as_secs()
            );
        }
        f.seek(SeekFrom::Start(offset))?;
        let n = match f.read(&mut buf) {
            Ok(0) => {
                sleep(Duration::from_millis(100)).await;
                continue;
            }
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };
        offset += n as u64;

        remainder.push_str(&String::from_utf8_lossy(&buf[..n]));

        while let Some(newline) = remainder.find('\n') {
            let line = &remainder[..newline];
            let line = line.trim();
            if !line.is_empty()
                && let Ok(event) = serde_json::from_str::<SetupEvent>(line)
                && let Some(setup) = event.antithesis_setup
                && setup.status == "complete"
            {
                return Ok(());
            }
            remainder.drain(..=newline);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn generate_setup_override_basic() {
        let compose_yaml = "\
services:
  app:
    image: myapp:latest
  sidecar:
    image: sidecar:latest
";
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(compose_yaml, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        // Parse the override as YAML to verify it's valid
        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let services = doc.get("services").unwrap().as_mapping().unwrap();

        // Both services should be present
        assert!(services.contains_key(&serde_yaml::Value::String("app".to_string())));
        assert!(services.contains_key(&serde_yaml::Value::String("sidecar".to_string())));

        let antithesis_dir = dir.path().join("antithesis");
        let expected_vol = format!("{}:/tmp/antithesis", antithesis_dir.display());

        for name in ["app", "sidecar"] {
            let svc = services
                .get(&serde_yaml::Value::String(name.to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();

            // Check volume
            let volumes = svc
                .get(&serde_yaml::Value::String("volumes".to_string()))
                .unwrap()
                .as_sequence()
                .unwrap();
            assert_eq!(volumes[0].as_str().unwrap(), expected_vol);

            // Check environment
            let env = svc
                .get(&serde_yaml::Value::String("environment".to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();
            assert_eq!(
                env.get(&serde_yaml::Value::String(
                    "ANTITHESIS_SDK_LOCAL_OUTPUT".to_string()
                ))
                .unwrap()
                .as_str()
                .unwrap(),
                "/tmp/antithesis/sdk_output.jsonl"
            );
        }
    }

    #[test]
    fn generate_setup_override_special_service_name() {
        let compose_yaml = "\
services:
  \"a: b\":
    image: myapp:latest
";
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(compose_yaml, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        // Must parse as valid YAML even with special characters in service name
        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let services = doc.get("services").unwrap().as_mapping().unwrap();
        assert!(services.contains_key(&serde_yaml::Value::String("a: b".to_string())));
    }

    #[test]
    fn generate_setup_override_no_services() {
        let compose_yaml = "version: '3'\n";
        let dir = tempfile::tempdir().unwrap();
        let err = generate_setup_override(compose_yaml, dir.path()).unwrap_err();
        assert!(err.to_string().contains("no services"), "got: {err}");
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);

    /// Write the setup-complete event before the watcher starts.
    #[tokio::test]
    async fn detects_setup_complete() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");
        std::fs::write(
            &file,
            "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
        )
        .unwrap();

        watch_for_setup_complete(&file, TEST_TIMEOUT)
            .await
            .expect("watch failed");
    }

    /// The file appears after the watcher starts polling.
    #[tokio::test]
    async fn detects_late_file_creation() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");

        let path = file.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            std::fs::write(
                &path,
                "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
            )
            .unwrap();
        });

        watch_for_setup_complete(&file, TEST_TIMEOUT)
            .await
            .expect("watch failed");
    }

    /// The event arrives in a later append, after unrelated lines.
    #[tokio::test]
    async fn detects_appended_event() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");
        std::fs::write(&file, "{\"unrelated\": true}\n").unwrap();

        let path = file.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"antithesis_setup\": {{\"status\": \"complete\"}}}}").unwrap();
        });

        watch_for_setup_complete(&file, TEST_TIMEOUT)
            .await
            .expect("watch failed");
    }

    /// Non-complete status values are ignored.
    #[tokio::test]
    async fn ignores_non_complete_status() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");
        std::fs::write(&file, "{\"antithesis_setup\": {\"status\": \"running\"}}\n").unwrap();

        let path = file.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"antithesis_setup\": {{\"status\": \"complete\"}}}}").unwrap();
        });

        watch_for_setup_complete(&file, TEST_TIMEOUT)
            .await
            .expect("watch failed");
    }

    /// The event is split across two writes (partial line buffering).
    #[tokio::test]
    async fn handles_partial_line() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");

        let path = file.clone();
        tokio::spawn(async move {
            // Write first half without newline.
            std::fs::write(&path, "{\"antithesis_setup\":").unwrap();
            sleep(Duration::from_millis(500)).await;
            // Append second half with newline.
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, " {{\"status\": \"complete\"}}}}").unwrap();
        });

        watch_for_setup_complete(&file, TEST_TIMEOUT)
            .await
            .expect("watch failed");
    }

    /// Times out when the event never arrives.
    #[tokio::test]
    async fn times_out_without_event() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("output.jsonl");
        std::fs::write(&file, "{\"unrelated\": true}\n").unwrap();

        let err = watch_for_setup_complete(&file, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected timeout error, got: {err}"
        );
    }
}
