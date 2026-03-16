use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use crate::container::{ContainerRuntime, DockerRuntime, PodmanRuntime, is_podman_in_disguise};

/// Return all container runtimes that are actually usable on this machine.
/// Skips `docker` if it is actually podman in disguise.
pub fn available_runtimes() -> Vec<Box<dyn ContainerRuntime>> {
    let requested = std::env::var("SNOUTY_TEST_RUNTIME").ok();
    let mut runtimes: Vec<Box<dyn ContainerRuntime>> = Vec::new();
    let want_podman = requested.as_deref().is_none_or(|r| r == "podman");
    let want_docker = requested.as_deref().is_none_or(|r| r == "docker");

    if want_podman
        && Command::new("podman")
            .arg("info")
            .output()
            .is_ok_and(|o| o.status.success())
    {
        runtimes.push(Box::new(PodmanRuntime::new("podman")));
    }
    if want_docker
        && Command::new("docker")
            .arg("info")
            .output()
            .is_ok_and(|o| o.status.success())
        && !is_podman_in_disguise("docker")
    {
        runtimes.push(Box::new(DockerRuntime::new("docker")));
    }
    runtimes
}

pub struct OCIRegistry {
    child: Child,
    runtime: String,
    container_name: String,
    port: u16,
}

impl OCIRegistry {
    /// Try to start a local OCI registry container.  Returns `None` when the
    /// container cannot be launched.
    pub fn start(runtime: &dyn ContainerRuntime) -> Option<Self> {
        if !runtime_supports_linux_registry_image(runtime.name()) {
            eprintln!(
                "skipping: OCI registry image requires Linux containers for {}",
                runtime.name()
            );
            return None;
        }
        if !ensure_registry_image_available(runtime.name()) {
            skip_or_fail(&format!(
                "OCI registry image could not be pulled with {}",
                runtime.name()
            ));
            return None;
        }

        let port = reserve_local_port();
        let container_name = format!("snouty-test-registry-{}-{}", std::process::id(), port);
        let publish = format!("127.0.0.1:{port}:5000");
        let runtime_name = runtime.name().to_owned();

        let child = Command::new(&runtime_name)
            .args([
                "run",
                "--rm",
                "-p",
                &publish,
                "--name",
                &container_name,
                "registry:2",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!("failed to start OCI registry with {}: {e}", runtime.name())
            });

        let mut registry = Self {
            child,
            runtime: runtime_name,
            container_name,
            port,
        };
        if !registry.wait_until_ready() {
            registry.cleanup_container();
            skip_or_fail(&format!(
                "OCI registry could not start with {}",
                runtime.name()
            ));
            return None;
        }
        Some(registry)
    }

    pub fn host_port(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Returns `true` when the registry is ready, `false` when it exited or
    /// timed out.
    fn wait_until_ready(&mut self) -> bool {
        for _ in 0..200 {
            if registry_v2_ping(self.port) {
                return true;
            }
            if let Some(_status) = self
                .child
                .try_wait()
                .expect("failed to poll OCI registry child process")
            {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
        false
    }

    fn cleanup_container(&self) {
        let _ = Command::new(&self.runtime)
            .args(["rm", "-f", &self.container_name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for OCIRegistry {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        self.cleanup_container();
    }
}

/// Returns `true` when running inside GitHub Actions (or any CI that sets `CI=true`).
pub fn is_ci() -> bool {
    std::env::var("CI").is_ok_and(|v| v == "true" || v == "1")
}

/// In CI this panics so silent skips don't hide missing test coverage.
/// Locally it prints a message and returns so the test can exit early.
#[track_caller]
pub fn skip_or_fail(msg: &str) {
    if is_ci() {
        panic!("{msg}");
    }
    eprintln!("skipping: {msg}");
}

/// Check whether `{runtime} compose version` succeeds.
pub fn has_compose(runtime: &str) -> bool {
    Command::new(runtime)
        .args(["compose", "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Return available runtimes, or skip/fail if none are found.
/// Convenience wrapper for tests that require at least one runtime.
#[track_caller]
pub fn require_runtimes() -> Vec<Box<dyn ContainerRuntime>> {
    let runtimes = available_runtimes();
    if runtimes.is_empty() {
        skip_or_fail("no container runtime available");
    }
    runtimes
}

/// Return available runtimes that have compose support, or skip/fail if none.
#[track_caller]
pub fn require_runtimes_with_compose() -> Vec<Box<dyn ContainerRuntime>> {
    let runtimes = require_runtimes();
    let with_compose: Vec<_> = runtimes
        .into_iter()
        .filter(|rt| has_compose(rt.name()))
        .collect();
    if with_compose.is_empty() {
        skip_or_fail("no runtime has docker compose support");
    }
    with_compose
}

fn reserve_local_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn registry_v2_ping(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };

    let request =
        format!("GET /v2/ HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    if std::io::Write::write_all(&mut stream, request.as_bytes()).is_err() {
        return false;
    }

    let mut response = String::new();
    if std::io::Read::read_to_string(&mut stream, &mut response).is_err() {
        return false;
    }

    response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200")
}

fn runtime_supports_linux_registry_image(runtime: &str) -> bool {
    if !runtime.ends_with("docker") {
        return true;
    }

    let Ok(output) = Command::new(runtime)
        .args(["info", "--format", "{{.OSType}}"])
        .output()
    else {
        return true;
    };
    if !output.status.success() {
        return true;
    }

    docker_info_supports_linux_registry(&String::from_utf8_lossy(&output.stdout))
}

fn docker_info_supports_linux_registry(stdout: &str) -> bool {
    let os_type = stdout.trim();
    os_type.is_empty() || os_type.eq_ignore_ascii_case("linux")
}

fn ensure_registry_image_available(runtime: &str) -> bool {
    Command::new(runtime)
        .args(["pull", "registry:2"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn filtered_path_without_binary(binary: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    let filtered = std::env::split_paths(&path)
        .filter(|dir| !directory_contains_binary(dir, binary))
        .collect::<Vec<_>>();
    std::env::join_paths(filtered)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

fn directory_contains_binary(dir: &Path, binary: &str) -> bool {
    dir.join(binary).is_file()
}

#[cfg(test)]
#[ctor::ctor]
fn init_test_eyre() {
    let _ = color_eyre::install();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn directory_contains_plain_binary() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snouty-update"), "").unwrap();
        assert!(directory_contains_binary(dir.path(), "snouty-update"));
    }

    #[test]
    fn docker_info_with_linux_os_type_supports_registry() {
        assert!(docker_info_supports_linux_registry("linux\n"));
    }

    #[cfg(unix)]
    #[test]
    fn oci_registry_drop_removes_container_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("runtime.log");
        let runtime_path = dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                "#!/bin/sh\nprintf '%s\n' \"$*\" >> \"{}\"\nexit 0\n",
                log_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&runtime_path, perms).unwrap();

        let child = Command::new("sh").args(["-c", "sleep 30"]).spawn().unwrap();
        let container_name = "snouty-test-registry-drop".to_string();

        {
            let _registry = OCIRegistry {
                child,
                runtime: runtime_path.display().to_string(),
                container_name: container_name.clone(),
                port: 5000,
            };
        }

        let log = fs::read_to_string(log_path).unwrap();
        assert!(
            log.lines()
                .any(|line| line == format!("rm -f {container_name}")),
            "expected cleanup command in log, got: {log}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn ensure_registry_image_available_pulls_registry_image() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("runtime.log");
        let runtime_path = dir.path().join("fake-runtime.sh");
        fs::write(
            &runtime_path,
            format!(
                "#!/bin/sh\nprintf '%s\n' \"$*\" >> \"{}\"\nexit 0\n",
                log_path.display()
            ),
        )
        .unwrap();
        let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&runtime_path, perms).unwrap();

        assert!(ensure_registry_image_available(
            runtime_path.to_str().unwrap()
        ));

        let log = fs::read_to_string(log_path).unwrap();
        assert!(
            log.lines().any(|line| line == "pull registry:2"),
            "expected registry pull command in log, got: {log}"
        );
    }
}
