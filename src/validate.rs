use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result, bail};
use log::{debug, info};
use serde::Deserialize;
use tokio::time::{Duration, sleep};

use crate::cli::ValidateArgs;
use crate::container::{self, ComposeConfig};
use crate::scripts::{ScriptType, TestScript, scan_scripts};

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
/// - A volume mount: `{temp_dir}/antithesis:/tmp/antithesis:z`
///   (`:z` relabels for SELinux shared access; no-op without SELinux)
/// - Environment variables: `ANTITHESIS_OUTPUT_DIR=/tmp/antithesis` and
///   `ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/sdk.jsonl`
///
/// The SDK creates the output file; we mount the parent directory so it can do so.
///
/// `compose_yaml` should be the resolved output of `compose_config()`.
/// Returns the path to the generated override file.
fn generate_setup_override(compose_yaml: &str, temp_dir: &Path) -> Result<PathBuf> {
    let contents = container::parse_compose_config(compose_yaml)?;
    if contents.services.is_empty() {
        bail!("no services found in docker-compose.yaml");
    }

    let antithesis_dir = temp_dir.join("antithesis");
    std::fs::create_dir_all(&antithesis_dir)
        .wrap_err("failed to create antithesis output directory")?;

    let vol = format!("{}:/tmp/antithesis:z", antithesis_dir.display());

    let mut services = serde_yaml::Mapping::new();
    for (name, _) in &contents.services {
        let mut svc = serde_yaml::Mapping::new();
        svc.insert(
            serde_yaml::Value::String("volumes".to_string()),
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::String(vol.clone())]),
        );
        let mut env = serde_yaml::Mapping::new();
        env.insert(
            serde_yaml::Value::String("ANTITHESIS_OUTPUT_DIR".to_string()),
            serde_yaml::Value::String("/tmp/antithesis".to_string()),
        );
        env.insert(
            serde_yaml::Value::String("ANTITHESIS_SDK_LOCAL_OUTPUT".to_string()),
            serde_yaml::Value::String("/tmp/antithesis/sdk.jsonl".to_string()),
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

    // Build network overrides: always include "default", plus any explicit networks.
    let mut net_names = contents.networks;
    if !net_names.contains(&"default".to_string()) {
        net_names.push("default".to_string());
    }
    net_names.sort();

    eprintln!("Isolating networks: {}", net_names.join(", "));

    let mut networks = serde_yaml::Mapping::new();
    for name in &net_names {
        let mut net = serde_yaml::Mapping::new();
        net.insert(
            serde_yaml::Value::String("internal".to_string()),
            serde_yaml::Value::Bool(true),
        );
        networks.insert(
            serde_yaml::Value::String(name.clone()),
            serde_yaml::Value::Mapping(net),
        );
    }

    let mut doc = serde_yaml::Mapping::new();
    doc.insert(
        serde_yaml::Value::String("services".to_string()),
        serde_yaml::Value::Mapping(services),
    );
    doc.insert(
        serde_yaml::Value::String("networks".to_string()),
        serde_yaml::Value::Mapping(networks),
    );

    let override_yaml =
        serde_yaml::to_string(&doc).wrap_err("failed to serialize compose override")?;

    let override_path = temp_dir.join("override.yml");
    std::fs::write(&override_path, &override_yaml)
        .wrap_err("failed to write compose override file")?;

    Ok(override_path)
}

struct ComposeDownGuard<'a> {
    rt: &'static dyn container::ContainerRuntime,
    config: &'a ComposeConfig,
}

impl Drop for ComposeDownGuard<'_> {
    fn drop(&mut self) {
        self.rt.compose_down(self.config);
    }
}

pub async fn cmd_validate(args: ValidateArgs) -> Result<()> {
    let config = ComposeConfig::new(args.config)?;
    let rt = container::runtime()?;

    let temp_dir = tempfile::tempdir()?;
    let compose_yaml = rt.compose_config(config.dir())?;
    let override_path = generate_setup_override(&compose_yaml, temp_dir.path())?;
    let config = config.with_overlay(override_path);

    eprintln!("Starting compose services...");
    rt.compose_up_detached(&config)?;
    let _guard = ComposeDownGuard {
        rt,
        config: &config,
    };

    // Discover scripts early so we can use them for both the success path
    // and the timeout diagnostic.
    let scripts = discover_scripts(rt, &config, temp_dir.path())?;

    let mut logs_child = rt.compose_logs_follow(&config)?;

    let sdk_output_dir = temp_dir.path().join("antithesis");
    let timeout = Duration::from_secs(args.timeout);

    let result = tokio::select! {
        result = watch_for_setup_complete(&sdk_output_dir, timeout) => result,
        status = logs_child.wait() => {
            match status {
                Ok(s) if !s.success() => Err(color_eyre::eyre::eyre!("compose exited with status: {s}")),
                Ok(_) => Err(color_eyre::eyre::eyre!("compose exited before setup-complete event was detected")),
                Err(e) => Err(color_eyre::eyre::eyre!("failed to wait for compose: {e}")),
            }
        }
        _ = tokio::signal::ctrl_c() => Err(color_eyre::eyre::eyre!("interrupted")),
    };

    // Stop the entire compose logs process group so child processes
    // (e.g. per-service `podman logs`) don't keep writing to the terminal.
    if let Some(pid) = logs_child.id() {
        unsafe {
            libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
        }
    }
    let _ = logs_child.wait().await;

    let test_result = match result {
        Ok(true) => {
            eprintln!("Setup-complete event detected.");
            run_test_scripts(rt, &config, &scripts)
        }
        Ok(false) => {
            diagnose_setup_in_first_scripts(rt, &config, &scripts, temp_dir.path());
            bail!("timed out waiting for setup-complete event");
        }
        Err(e) => Err(e),
    };

    if test_result.is_ok() {
        eprintln!("Setup validation successful.");
    }

    test_result
}

/// Check whether a reader contains the setup-complete event.
///
/// Reads all content, checks each complete line for
/// `{"antithesis_setup": {"status": "complete"}}`.
fn contains_setup_complete(reader: &mut impl std::io::Read) -> Result<bool> {
    let mut buf = String::new();
    reader
        .read_to_string(&mut buf)
        .wrap_err("failed to read setup output")?;
    for line in buf.lines() {
        let line = line.trim();
        if !line.is_empty()
            && let Ok(event) = serde_json::from_str::<SetupEvent>(line)
            && let Some(setup) = event.antithesis_setup
            && setup.status == "complete"
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Run first scripts with a redirected output dir and check if they emit setup_complete.
/// If so, print a diagnostic explaining the chicken-and-egg problem.
fn diagnose_setup_in_first_scripts(
    rt: &dyn container::ContainerRuntime,
    config: &ComposeConfig,
    scripts: &[TestScript],
    temp_dir: &Path,
) {
    let first_scripts: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::First)
        .collect();
    if first_scripts.is_empty() {
        return;
    }

    let check_dir = temp_dir.join("antithesis").join("first-check");
    if std::fs::create_dir_all(&check_dir).is_err() {
        return;
    }

    let container_output_dir = "/tmp/antithesis/first-check";
    let sdk_output = format!("{container_output_dir}/sdk.jsonl");
    let env = [
        ("ANTITHESIS_OUTPUT_DIR", container_output_dir),
        ("ANTITHESIS_SDK_LOCAL_OUTPUT", sdk_output.as_str()),
    ];

    let sdk_file = check_dir.join("sdk.jsonl");

    for s in &first_scripts {
        let script_dir = format!("/opt/antithesis/test/v1/{}", s.test_name);
        let container_path = format!("{}/{}", script_dir, s.command_name);
        let _ = rt.compose_exec(
            config,
            &s.service,
            Some(&script_dir),
            &env,
            &[&container_path],
        );

        if let Ok(mut f) = std::fs::File::open(&sdk_file) {
            if contains_setup_complete(&mut f).unwrap_or(false) {
                eprintln!(
                    "\nDiagnostic: {}/{} in service {} emits setup_complete, but first \
                     scripts only run after setup_complete is detected — this is a deadlock. \
                     Move the setup_complete event to the container entrypoint \
                     (CMD/ENTRYPOINT) instead.",
                    s.test_name, s.command_name, s.service
                );
                return;
            }

            // Remove any output from the previous script.
            let _ = std::fs::remove_file(&sdk_file);
        }
    }
}

/// Discover test scripts from running containers.
fn discover_scripts(
    rt: &dyn container::ContainerRuntime,
    config: &ComposeConfig,
    temp_dir: &Path,
) -> Result<Vec<TestScript>> {
    let services = rt.compose_ps(config)?;

    let scripts_dir = temp_dir.join("scripts");
    std::fs::create_dir_all(&scripts_dir).wrap_err("failed to create scripts directory")?;

    let mut all_scripts: Vec<TestScript> = Vec::new();
    for (service_name, container_id) in &services {
        let service_dir = scripts_dir.join(service_name);
        match rt.container_cp(container_id, "/opt/antithesis/test/v1", &service_dir) {
            Ok(()) => {
                let result = scan_scripts(&service_dir, service_name)?;
                if !result.unrecognized.is_empty() {
                    bail!(
                        "unrecognized command names in service {service_name} \
                         (not a known prefix or helper_):\n  {}",
                        result.unrecognized.join("\n  ")
                    );
                }
                if result.scripts.is_empty() {
                    bail!("No scripts found in {service_name}");
                }
                info!(
                    "Found {} scripts in service '{service_name}'",
                    result.scripts.len()
                );
                all_scripts.extend(result.scripts);
            }
            Err(_) => {
                info!("No test scripts in service '{service_name}'");
                continue;
            }
        }
    }

    Ok(all_scripts)
}

/// Categorize and execute pre-discovered test scripts.
fn run_test_scripts(
    rt: &dyn container::ContainerRuntime,
    config: &ComposeConfig,
    scripts: &[TestScript],
) -> Result<()> {
    // Categorize scripts
    let first: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::First)
        .collect();
    let drivers: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type.is_driver())
        .collect();
    let anytime: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Anytime)
        .collect();
    let eventually: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Eventually)
        .collect();
    let finally: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Finally)
        .collect();

    eprintln!(
        "Found {} first, {} driver, {} anytime, {} eventually, {} finally scripts",
        first.len(),
        drivers.len(),
        anytime.len(),
        eventually.len(),
        finally.len(),
    );

    if scripts.is_empty() {
        debug!("no services contained test scripts");
        return Ok(());
    }

    if drivers.is_empty() && anytime.is_empty() {
        bail!("test scripts found but no driver or anytime scripts");
    }

    let mut ok = true;

    // Execute first scripts (sorted by path — already sorted from scan_scripts)
    for s in &first {
        ok &= exec_script(rt, config, s, &[])?;
    }

    // Execute drivers + anytime (shuffled together)
    let mut runnable: Vec<_> = drivers.iter().chain(anytime.iter()).copied().collect();
    shuffle(&mut runnable);
    for s in &runnable {
        ok &= exec_script(rt, config, s, &[])?;
    }

    // Execute eventually scripts (sorted)
    for s in &eventually {
        ok &= exec_script(rt, config, s, &[])?;
    }

    // Execute finally scripts (sorted)
    for s in &finally {
        ok &= exec_script(rt, config, s, &[])?;
    }

    if !ok {
        bail!("one or more test scripts failed");
    }

    Ok(())
}

/// Execute a single test script via `compose exec`.
///
/// Returns `true` if the script succeeded, `false` if it failed.
fn exec_script(
    rt: &dyn container::ContainerRuntime,
    config: &ComposeConfig,
    script: &TestScript,
    env: &[(&str, &str)],
) -> Result<bool> {
    let script_dir = format!("/opt/antithesis/test/v1/{}", script.test_name);
    let container_path = format!("{}/{}", script_dir, script.command_name);
    eprint!(
        "Running [{}/{}] in service {}...",
        script.test_name, script.command_name, script.service
    );

    let output = rt.compose_exec(
        config,
        &script.service,
        Some(&script_dir),
        env,
        &[&container_path],
    )?;

    if output.status.success() {
        eprintln!(" ok");
        Ok(true)
    } else {
        eprintln!(" failed ({})", output.status);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stdout.is_empty() {
            eprint!("{stdout}");
        }
        if !stderr.is_empty() {
            eprint!("{stderr}");
        }
        Ok(false)
    }
}

/// Fisher-Yates shuffle using `getrandom` for randomness.
fn shuffle<T>(slice: &mut [T]) {
    for i in (1..slice.len()).rev() {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("getrandom failed");
        let r = u64::from_le_bytes(buf);
        let j = (r % (i as u64 + 1)) as usize;
        slice.swap(i, j);
    }
}

/// Watch a directory of JSONL files for the `{"antithesis_setup": {"status": "complete"}}` event.
///
/// Polls the directory for `.jsonl` files (100ms interval), tailing each for new data.
/// Returns `Ok(true)` when the event is found, `Ok(false)` on timeout.
///
/// Uses blocking `std::fs` calls intentionally — reads are small and infrequent,
/// and this avoids pulling in tokio::fs for a simple poll loop.
async fn watch_for_setup_complete(output_dir: &Path, timeout: Duration) -> Result<bool> {
    use std::collections::HashMap;
    use std::io::{Read, Seek, SeekFrom};

    struct TailedFile {
        file: std::fs::File,
        offset: u64,
        remainder: String,
    }

    let deadline = tokio::time::Instant::now() + timeout;
    let mut files: HashMap<PathBuf, TailedFile> = HashMap::new();
    let mut buf = vec![0u8; 4096];

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }

        // Discover new .jsonl files in the directory.
        match std::fs::read_dir(output_dir) {
            Ok(entries) => {
                for entry in entries {
                    let path = entry?.path();
                    if path.extension().is_some_and(|e| e == "jsonl") && !files.contains_key(&path)
                    {
                        match std::fs::File::open(&path) {
                            Ok(f) => {
                                files.insert(
                                    path,
                                    TailedFile {
                                        file: f,
                                        offset: 0,
                                        remainder: String::new(),
                                    },
                                );
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        // Tail each known file for new data.
        let mut progress = false;
        for tailed in files.values_mut() {
            tailed.file.seek(SeekFrom::Start(tailed.offset))?;
            let n = match tailed.file.read(&mut buf) {
                Ok(0) => continue,
                Ok(n) => n,
                Err(e) => return Err(e.into()),
            };
            tailed.offset += n as u64;
            progress = true;

            tailed
                .remainder
                .push_str(&String::from_utf8_lossy(&buf[..n]));

            while let Some(newline) = tailed.remainder.find('\n') {
                let line = &tailed.remainder[..newline];
                let line = line.trim();
                if !line.is_empty()
                    && let Ok(event) = serde_json::from_str::<SetupEvent>(line)
                    && let Some(setup) = event.antithesis_setup
                    && setup.status == "complete"
                {
                    return Ok(true);
                }
                tailed.remainder.drain(..=newline);
            }
        }

        if !progress {
            sleep(Duration::from_millis(100)).await;
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
        let expected_vol = format!("{}:/tmp/antithesis:z", antithesis_dir.display());

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
                    "ANTITHESIS_OUTPUT_DIR".to_string()
                ))
                .unwrap()
                .as_str()
                .unwrap(),
                "/tmp/antithesis"
            );
            assert_eq!(
                env.get(&serde_yaml::Value::String(
                    "ANTITHESIS_SDK_LOCAL_OUTPUT".to_string()
                ))
                .unwrap()
                .as_str()
                .unwrap(),
                "/tmp/antithesis/sdk.jsonl"
            );
        }

        // Check networks — default should always be present and internal
        let networks = doc.get("networks").unwrap().as_mapping().unwrap();
        let default_net = networks
            .get(&serde_yaml::Value::String("default".to_string()))
            .unwrap()
            .as_mapping()
            .unwrap();
        assert_eq!(
            default_net
                .get(&serde_yaml::Value::String("internal".to_string()))
                .unwrap()
                .as_bool()
                .unwrap(),
            true
        );
    }

    #[test]
    fn generate_setup_override_custom_networks() {
        let compose_yaml = "\
services:
  app:
    image: myapp:latest
networks:
  backend: {}
  frontend:
    driver: bridge
";
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(compose_yaml, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let networks = doc.get("networks").unwrap().as_mapping().unwrap();

        // All three networks should be present: backend, default, frontend
        for name in ["backend", "default", "frontend"] {
            let net = networks
                .get(&serde_yaml::Value::String(name.to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();
            assert_eq!(
                net.get(&serde_yaml::Value::String("internal".to_string()))
                    .unwrap()
                    .as_bool()
                    .unwrap(),
                true,
                "network '{name}' should be internal"
            );
        }
        assert_eq!(networks.len(), 3);
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

    #[test]
    fn contains_setup_complete_found() {
        let data = "{\"antithesis_setup\": {\"status\": \"complete\"}}\n";
        assert!(contains_setup_complete(&mut data.as_bytes()).unwrap());
    }

    #[test]
    fn contains_setup_complete_not_found() {
        let data = "{\"antithesis_setup\": {\"status\": \"running\"}}\n";
        assert!(!contains_setup_complete(&mut data.as_bytes()).unwrap());
    }

    #[test]
    fn contains_setup_complete_empty() {
        let data = "";
        assert!(!contains_setup_complete(&mut data.as_bytes()).unwrap());
    }

    #[test]
    fn contains_setup_complete_mixed_lines() {
        let data = "{\"unrelated\": true}\n\
                    not json at all\n\
                    {\"antithesis_setup\": {\"status\": \"complete\"}}\n\
                    {\"more\": \"stuff\"}\n";
        assert!(contains_setup_complete(&mut data.as_bytes()).unwrap());
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(3);

    /// Write the setup-complete event before the watcher starts.
    #[tokio::test]
    async fn detects_setup_complete() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("output.jsonl"),
            "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
        )
        .unwrap();

        assert!(
            watch_for_setup_complete(dir.path(), TEST_TIMEOUT)
                .await
                .expect("watch failed")
        );
    }

    /// The file appears after the watcher starts polling.
    #[tokio::test]
    async fn detects_late_file_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            std::fs::write(
                path.join("sdk.jsonl"),
                "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
            )
            .unwrap();
        });

        assert!(
            watch_for_setup_complete(dir.path(), TEST_TIMEOUT)
                .await
                .expect("watch failed")
        );
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

        assert!(
            watch_for_setup_complete(dir.path(), TEST_TIMEOUT)
                .await
                .expect("watch failed")
        );
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

        assert!(
            watch_for_setup_complete(dir.path(), TEST_TIMEOUT)
                .await
                .expect("watch failed")
        );
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

        assert!(
            watch_for_setup_complete(dir.path(), TEST_TIMEOUT)
                .await
                .expect("watch failed")
        );
    }

    /// Times out when the event never arrives.
    #[tokio::test]
    async fn times_out_without_event() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("output.jsonl"), "{\"unrelated\": true}\n").unwrap();

        let found = watch_for_setup_complete(dir.path(), Duration::from_secs(1))
            .await
            .expect("watch failed");
        assert!(!found, "expected timeout (false), got true");
    }
}
