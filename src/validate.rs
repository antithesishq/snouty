use std::collections::BTreeMap;
use std::io::Seek;
use std::path::{Path, PathBuf};

use color_eyre::Section;
use color_eyre::eyre::{Context, Result, bail, eyre};
use log::{debug, info};
use serde::Deserialize;
use tokio::time::{Duration, sleep};

use crate::cli::ValidateArgs;
use crate::config::{self, ComposeConfig, Config, KubernetesConfig};
use crate::container;
use crate::scripts::{ScriptType, TestScript, scan_scripts};

const K8S_VALIDATOR_IMAGE: &str = "docker.io/antithesishq/k8s-validator:1.0.0";

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
/// - A per-service volume mount:
///   `{temp_dir}/antithesis/{service}:/tmp/antithesis:z`
///   (`:z` relabels for SELinux shared access; no-op without SELinux)
/// - Environment variables:
///   `ANTITHESIS_OUTPUT_DIR=/tmp/antithesis` and
///   `ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/antithesis/sdk.jsonl`
///
/// The SDK creates the output file; we mount the parent directory so it can do so.
///
/// `contents` should be the parsed output of `compose.contents()`.
/// Returns the path to the generated override file.
fn generate_setup_override(
    contents: &container::ComposeContents,
    temp_dir: &Path,
) -> Result<PathBuf> {
    if contents.services.is_empty() {
        bail!("no services found in docker-compose.yaml");
    }

    let antithesis_dir = temp_dir.join("antithesis");
    std::fs::create_dir_all(&antithesis_dir)
        .wrap_err("failed to create antithesis output directory")?;

    let mut services = serde_yaml::Mapping::new();
    for service in &contents.services {
        let service_dir = antithesis_dir.join(&service.name);
        std::fs::create_dir_all(&service_dir).wrap_err_with(|| {
            format!(
                "failed to create antithesis output directory for service '{}'",
                service.name
            )
        })?;
        let vol = format!("{}:/tmp/antithesis:z", service_dir.display());
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
            serde_yaml::Value::String(service.name.clone()),
            serde_yaml::Value::Mapping(svc),
        );
    }

    // Build network overrides: always include "default", plus any explicit networks.
    let mut net_names = contents.networks.clone();
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

/// Create a directory at the requested path if it does not already exist.
/// If it already exists, ensure it is a directory, and ensure there is nothing in it.
fn mkdir_or_require_empty(path: &Path) -> Result<()> {
    if path.exists() {
        if !path.is_dir() {
            bail!("{} exists but is not a directory", path.display());
        }
        let mut entries = std::fs::read_dir(path)
            .wrap_err_with(|| format!("failed to read directory {}", path.display()))?;
        if entries.next().is_some() {
            bail!("{} exists but is not empty", path.display());
        }
        Ok(())
    } else {
        std::fs::create_dir_all(path)
            .wrap_err_with(|| format!("failed to create directory {}", path.display()))
    }
}

struct ComposeDownGuard<'a> {
    compose: &'a dyn container::Compose,
    config: &'a ComposeConfig,
    overlay: Option<&'a Path>,
}

impl Drop for ComposeDownGuard<'_> {
    fn drop(&mut self) {
        self.compose.down(self.config, self.overlay);
    }
}

pub async fn cmd_validate(args: ValidateArgs) -> Result<()> {
    match std::env::var("SNOUTY_TEMP_DIR") {
        Ok(out_dir) => {
            let temp_dir = Path::new(&out_dir);
            // To avoid conflating results of subsequent runs, we require that the provided
            // SNOUTY_TEMP_DIR is empty or non-existent
            mkdir_or_require_empty(temp_dir)?;
            validate_with_temp_dir(args, temp_dir).await
        }
        _ => {
            let mut temp_dir = tempfile::tempdir()?;
            temp_dir.disable_cleanup(args.keep_running);
            validate_with_temp_dir(args, temp_dir.path()).await
        }
    }
}

async fn validate_with_temp_dir(args: ValidateArgs, temp_dir: &Path) -> Result<()> {
    let ValidateArgs {
        config: config_path,
        timeout,
        keep_running,
    } = args;
    let rt = container::runtime()?;
    match config::detect_config(&config_path)? {
        Config::Compose(cfg) => validate_compose(rt, cfg, timeout, keep_running, temp_dir).await,
        Config::Kubernetes(cfg) => validate_kubernetes(rt, &cfg, keep_running).await,
    }
}

async fn validate_compose(
    rt: &dyn container::ContainerRuntime,
    config: ComposeConfig,
    timeout: u64,
    keep_running: bool,
    temp_dir: &Path,
) -> Result<()> {
    let compose = rt.compose();
    let contents = compose.contents(&config, None)?;
    container::validate_images_are_available(rt, &contents.services)?;
    container::validate_image_architectures(rt, &contents.services)?;
    let override_path = generate_setup_override(&contents, temp_dir)?;
    let overlay = Some(override_path.as_path());

    if keep_running {
        eprintln!(
            "Note: --keep-running is set. When done, bring containers down with:\n  \
             {} compose -f {}/docker-compose.yaml -f {} down\n",
            rt.name(),
            config.dir().display(),
            override_path.display(),
        );
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);

    eprintln!("Starting compose services...");
    let mut up_child = compose.up_detached(&config, overlay)?;
    let _guard = if keep_running {
        None
    } else {
        Some(ComposeDownGuard {
            compose: &*compose,
            config: &config,
            overlay,
        })
    };

    // Wait for compose up to finish, but respect the timeout and ctrl+c.
    tokio::select! {
        status = up_child.wait() => {
            let status = status.wrap_err("failed to wait for compose up")?;
            if !status.success() {
                bail!("compose up --detach failed (exit status: {status})");
            }
        }
        _ = tokio::time::sleep_until(deadline) => {
            up_child.kill_group().await.ok();
            bail!("timed out during 'compose up --detach'");
        }
        _ = tokio::signal::ctrl_c() => {
            up_child.kill_group().await.ok();
            bail!("interrupted");
        }
    };

    // Discover scripts early so we can use them for both the success path
    // and the timeout diagnostic.
    let scripts = discover_scripts(&*compose, &config, overlay, temp_dir)?;

    let mut logs_child = compose.logs_follow(&config, overlay)?;

    let sdk_output_dir = temp_dir.join("antithesis");

    let result = tokio::select! {
        result = watch_for_setup_complete(&sdk_output_dir, deadline) => result,
        status = logs_child.wait() => {
            match status {
                Ok(s) if !s.success() => Err(eyre!("compose exited with status: {s}")),
                Ok(_) => Err(eyre!("compose exited before setup-complete event was detected")),
                Err(e) => Err(eyre!("failed to wait for compose: {e}")),
            }
        }
        _ = tokio::signal::ctrl_c() => Err(eyre!("interrupted")),
    };

    // Stop the entire compose logs process group so child processes
    // (e.g. per-service `podman logs`) don't keep writing to the terminal.
    logs_child.kill_group().await.ok();

    let test_result = match result {
        Ok(true) => {
            eprintln!("Setup-complete event detected.");
            validate_test_scripts(&scripts)
        }
        Ok(false) => {
            bail!("timed out waiting for setup-complete event");
        }
        Err(e) => Err(e),
    };

    if test_result.is_ok() {
        eprintln!("Setup validation successful.");
    }

    test_result
}

async fn validate_kubernetes(
    rt: &dyn container::ContainerRuntime,
    config: &KubernetesConfig,
    keep_running: bool,
) -> Result<()> {
    if keep_running {
        eprintln!("Note: --keep-running has no effect for Kubernetes configs.");
    }

    let manifests_dir = config.manifests_dir();
    eprintln!(
        "Running k8s-validator against manifests in {}...",
        manifests_dir.display()
    );

    // Bind-mount the host path. Podman interprets relative paths as named
    // volumes, so always pass an absolute path. Include an SELinux relabel
    // option so the validator can read the manifests directory on
    // SELinux-enabled systems.
    let manifests_abs = std::fs::canonicalize(&manifests_dir).wrap_err_with(|| {
        format!(
            "failed to resolve manifests directory '{}'",
            manifests_dir.display()
        )
    })?;
    let mount = format!("{}:/manifests:ro,z", manifests_abs.display());
    let mut cmd = rt.tokio_command(&["run", "--rm", "-v", &mount, K8S_VALIDATOR_IMAGE]);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::inherit());
    cmd.stderr(std::process::Stdio::inherit());
    cmd.process_group(0);

    let runtime_name = rt.name();
    let mut child = cmd
        .spawn()
        .map(container::ProcessGroupChild::new)
        .wrap_err_with(|| format!("failed to start '{runtime_name} run' for k8s-validator"))?;

    tokio::select! {
        status = child.wait() => {
            let status = status.wrap_err("failed to wait for k8s-validator")?;
            if !status.success() {
                bail!("k8s-validator failed (exit status: {status})");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            child.kill_group().await.ok();
            bail!("interrupted");
        }
    };

    eprintln!("k8s manifests valid.");
    Ok(())
}

/// Check whether a reader contains the setup-complete event.
///
/// Reads from the current position, checks each complete line for
/// `{"antithesis_setup": {"status": "complete"}}`, and seeks back over any
/// partial trailing line so it will be re-read on the next call.
fn contains_setup_complete(reader: &mut (impl std::io::Read + std::io::Seek)) -> Result<bool> {
    let mut content = String::new();
    reader.read_to_string(&mut content)?;

    if content.is_empty() {
        return Ok(false);
    }

    // Seek back over any partial trailing line so it's re-read next call.
    if !content.ends_with('\n') {
        let partial_len = match content.rfind('\n') {
            Some(pos) => content.len() - pos - 1,
            None => content.len(),
        };
        reader.seek(std::io::SeekFrom::Current(-(partial_len as i64)))?;
        content.truncate(content.len() - partial_len);
    }

    for line in content.lines() {
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

/// Discover test scripts from running containers.
fn discover_scripts(
    compose: &dyn container::Compose,
    config: &ComposeConfig,
    overlay: Option<&Path>,
    temp_dir: &Path,
) -> Result<Vec<TestScript>> {
    let services = compose.ps(config, overlay)?;

    let scripts_dir = temp_dir.join("scripts");
    std::fs::create_dir_all(&scripts_dir).wrap_err("failed to create scripts directory")?;

    let mut all_scripts: Vec<TestScript> = Vec::new();
    for (service_name, container_id) in &services {
        let service_dir = scripts_dir.join(service_name);
        match compose
            .runtime()
            .container_cp(container_id, "/opt/antithesis/test/v1", &service_dir)
        {
            Ok(()) => {
                let result = scan_scripts(&service_dir, service_name)?;
                if !result.unrecognized.is_empty() {
                    let mut err = eyre!(
                        "unrecognized command names in service {service_name} (not a known prefix or helper_)"
                    );
                    for command in result.unrecognized {
                        err = err.with_note(|| command);
                    }
                    return Err(err);
                }
                if !result.not_executable.is_empty() {
                    let mut err = eyre!("scripts in service {service_name} are not executable");
                    for command in result.not_executable {
                        err = err.with_note(|| command);
                    }
                    return Err(err);
                }
                if result.scripts.is_empty() {
                    eprintln!("No scripts found in {service_name}");
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

/// Validate the structure of discovered test scripts without executing them.
fn validate_test_scripts(scripts: &[TestScript]) -> Result<()> {
    let first = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::First)
        .count();
    let drivers = scripts.iter().filter(|s| s.script_type.is_driver()).count();
    let anytime = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Anytime)
        .count();
    let eventually = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Eventually)
        .count();
    let finally = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Finally)
        .count();

    eprintln!(
        "Found {} first, {} driver, {} anytime, {} eventually, {} finally scripts",
        first, drivers, anytime, eventually, finally,
    );

    if scripts.is_empty() {
        debug!("no services contained test scripts");
        return Ok(());
    }

    Ok(())
}

/// Return all `.jsonl` files recursively under `root`.
fn find_jsonl_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut dirs = vec![root.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|ext| ext == "jsonl") {
                paths.push(path);
            }
        }
    }

    paths.sort();
    Ok(paths)
}

fn open_jsonl_file(path: &Path) -> Result<Option<std::fs::File>> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Watch `.jsonl` files anywhere under the given directory for setup-complete.
///
/// Polls recursively for new files (100ms interval), tails each file for new
/// data, and tolerates truncation/recreation by tracking offsets per path.
/// Returns `Ok(true)` when the event is found, `Ok(false)` on timeout.
///
/// Uses blocking `std::fs` calls intentionally — reads are small and infrequent,
/// and this avoids pulling in tokio::fs for a simple poll loop.
async fn watch_for_setup_complete(
    output_dir: &Path,
    deadline: tokio::time::Instant,
) -> Result<bool> {
    let mut offsets = BTreeMap::new();

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }

        for path in find_jsonl_files(output_dir)? {
            let previous_offset = offsets.get(&path).copied().unwrap_or(0);
            let Some(mut file) = open_jsonl_file(&path)? else {
                continue;
            };
            let start_offset = previous_offset.min(file.metadata()?.len());
            file.seek(std::io::SeekFrom::Start(start_offset))?;

            if contains_setup_complete(&mut file)? {
                return Ok(true);
            }

            offsets.insert(path, file.stream_position()?);
        }

        sleep(Duration::from_millis(100)).await;
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
        let contents = container::parse_compose_config(compose_yaml).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(&contents, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        // Parse the override as YAML to verify it's valid
        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let services = doc.get("services").unwrap().as_mapping().unwrap();

        // Both services should be present
        assert!(services.contains_key(serde_yaml::Value::String("app".to_string())));
        assert!(services.contains_key(serde_yaml::Value::String("sidecar".to_string())));

        let antithesis_dir = dir.path().join("antithesis");

        for name in ["app", "sidecar"] {
            let expected_vol = format!("{}:/tmp/antithesis:z", antithesis_dir.join(name).display());
            assert!(
                antithesis_dir.join(name).is_dir(),
                "expected output directory for service '{name}'"
            );
            let svc = services
                .get(serde_yaml::Value::String(name.to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();

            // Check volume
            let volumes = svc
                .get(serde_yaml::Value::String("volumes".to_string()))
                .unwrap()
                .as_sequence()
                .unwrap();
            assert_eq!(volumes[0].as_str().unwrap(), expected_vol);

            // Check environment
            let env = svc
                .get(serde_yaml::Value::String("environment".to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();
            assert_eq!(
                env.get(serde_yaml::Value::String(
                    "ANTITHESIS_OUTPUT_DIR".to_string()
                ))
                .unwrap()
                .as_str()
                .unwrap(),
                "/tmp/antithesis"
            );
            assert_eq!(
                env.get(serde_yaml::Value::String(
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
            .get(serde_yaml::Value::String("default".to_string()))
            .unwrap()
            .as_mapping()
            .unwrap();
        assert!(
            default_net
                .get(serde_yaml::Value::String("internal".to_string()))
                .unwrap()
                .as_bool()
                .unwrap()
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
        let contents = container::parse_compose_config(compose_yaml).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(&contents, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let networks = doc.get("networks").unwrap().as_mapping().unwrap();

        // All three networks should be present: backend, default, frontend
        for name in ["backend", "default", "frontend"] {
            let net = networks
                .get(serde_yaml::Value::String(name.to_string()))
                .unwrap()
                .as_mapping()
                .unwrap();
            assert!(
                net.get(serde_yaml::Value::String("internal".to_string()))
                    .unwrap()
                    .as_bool()
                    .unwrap(),
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
        let contents = container::parse_compose_config(compose_yaml).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = generate_setup_override(&contents, dir.path()).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        // Must parse as valid YAML even with special characters in service name
        let doc: serde_yaml::Value = serde_yaml::from_str(&content).unwrap();
        let services = doc.get("services").unwrap().as_mapping().unwrap();
        assert!(services.contains_key(serde_yaml::Value::String("a: b".to_string())));
    }

    #[test]
    fn generate_setup_override_no_services() {
        let contents = container::parse_compose_config("version: '3'\n").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let err = generate_setup_override(&contents, dir.path()).unwrap_err();
        assert!(err.to_string().contains("no services"), "got: {err}");
    }

    #[test]
    fn contains_setup_complete_found() {
        let data = "{\"antithesis_setup\": {\"status\": \"complete\"}}\n";
        assert!(contains_setup_complete(&mut std::io::Cursor::new(data)).unwrap());
    }

    #[test]
    fn contains_setup_complete_not_found() {
        let data = "{\"antithesis_setup\": {\"status\": \"running\"}}\n";
        assert!(!contains_setup_complete(&mut std::io::Cursor::new(data)).unwrap());
    }

    #[test]
    fn contains_setup_complete_empty() {
        let data = "";
        assert!(!contains_setup_complete(&mut std::io::Cursor::new(data)).unwrap());
    }

    #[test]
    fn contains_setup_complete_mixed_lines() {
        let data = "{\"unrelated\": true}\n\
                    not json at all\n\
                    {\"antithesis_setup\": {\"status\": \"complete\"}}\n\
                    {\"more\": \"stuff\"}\n";
        assert!(contains_setup_complete(&mut std::io::Cursor::new(data)).unwrap());
    }

    #[test]
    fn find_jsonl_files_finds_nested_jsonl_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("service-a").join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(dir.path().join("root.jsonl"), "").unwrap();
        std::fs::write(nested.join("events.jsonl"), "").unwrap();
        std::fs::write(nested.join("ignore.txt"), "").unwrap();

        let files = find_jsonl_files(dir.path()).unwrap();
        assert_eq!(
            files,
            vec![dir.path().join("root.jsonl"), nested.join("events.jsonl")]
        );
    }

    #[test]
    fn open_jsonl_file_returns_none_for_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.jsonl");

        let file = open_jsonl_file(&path).unwrap();
        assert!(file.is_none(), "missing files should be skipped");
    }

    fn test_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(3)
    }

    /// Write the setup-complete event before the watcher starts.
    #[tokio::test]
    async fn detects_setup_complete() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("app")).unwrap();
        std::fs::write(
            dir.path().join("app").join("sdk.jsonl"),
            "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
        )
        .unwrap();

        assert!(
            watch_for_setup_complete(dir.path(), test_deadline())
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
            std::fs::create_dir_all(path.join("app")).unwrap();
            std::fs::write(
                path.join("app").join("sdk.jsonl"),
                "{\"antithesis_setup\": {\"status\": \"complete\"}}\n",
            )
            .unwrap();
        });

        assert!(
            watch_for_setup_complete(dir.path(), test_deadline())
                .await
                .expect("watch failed")
        );
    }

    /// The event arrives in a later append, after unrelated lines.
    #[tokio::test]
    async fn detects_appended_event() {
        let dir = tempfile::tempdir().unwrap();
        let service_dir = dir.path().join("app");
        std::fs::create_dir_all(&service_dir).unwrap();
        let file = service_dir.join("events.jsonl");
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
            watch_for_setup_complete(dir.path(), test_deadline())
                .await
                .expect("watch failed")
        );
    }

    /// Non-complete status values are ignored.
    #[tokio::test]
    async fn ignores_non_complete_status() {
        let dir = tempfile::tempdir().unwrap();
        let service_dir = dir.path().join("app");
        std::fs::create_dir_all(&service_dir).unwrap();
        let file = service_dir.join("setup.jsonl");
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
            watch_for_setup_complete(dir.path(), test_deadline())
                .await
                .expect("watch failed")
        );
    }

    /// The event is split across two writes (partial line buffering).
    #[tokio::test]
    async fn handles_partial_line() {
        let dir = tempfile::tempdir().unwrap();
        let service_dir = dir.path().join("app");
        std::fs::create_dir_all(&service_dir).unwrap();
        let file = service_dir.join("setup.jsonl");

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
            watch_for_setup_complete(dir.path(), test_deadline())
                .await
                .expect("watch failed")
        );
    }

    /// Times out when the event never arrives.
    #[tokio::test]
    async fn times_out_without_event() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("app")).unwrap();
        std::fs::write(
            dir.path().join("app").join("events.jsonl"),
            "{\"unrelated\": true}\n",
        )
        .unwrap();

        let found = watch_for_setup_complete(dir.path(), test_deadline())
            .await
            .expect("watch failed");
        assert!(!found, "expected timeout (false), got true");
    }

    #[test]
    fn mkdir_or_require_empty_creates_new_dir() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("new");
        mkdir_or_require_empty(&target).unwrap();
        assert!(target.is_dir());
    }

    #[test]
    fn mkdir_or_require_empty_accepts_existing_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        mkdir_or_require_empty(dir.path()).unwrap();
    }

    #[test]
    fn mkdir_or_require_empty_rejects_non_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("file.txt"), "content").unwrap();
        let err = mkdir_or_require_empty(dir.path()).unwrap_err();
        assert!(
            format!("{err}").contains("not empty"),
            "expected 'not empty' error, got: {err}"
        );
    }

    #[test]
    fn mkdir_or_require_empty_rejects_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not_a_dir");
        std::fs::write(&file, "content").unwrap();
        let err = mkdir_or_require_empty(&file).unwrap_err();
        assert!(
            format!("{err}").contains("not a directory"),
            "expected 'not a directory' error, got: {err}"
        );
    }
}
