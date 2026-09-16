#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tempfile::TempDir;

struct Simulation {
    directory: TempDir,
    child: Child,
}

impl Simulation {
    fn start(mode: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let bin = root.join("bin");
        fs::create_dir(&bin).unwrap();
        let python = which::which("python3").expect("simulation fixtures require Python 3");
        let script = bin.join("mock_tools.py");
        fs::write(
            &script,
            format!(
                "#!{}\n{}",
                python.display(),
                include_str!("fixtures/simulate/mock_tools.py")
            ),
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        for tool in ["docker", "docker-compose", "ssh", "qemu-system-x86_64"] {
            symlink(&script, bin.join(tool)).unwrap();
        }
        let config = root.join("config");
        fs::create_dir(&config).unwrap();
        fs::write(
            config.join("docker-compose.yaml"),
            "services:\n  workload:\n    image: workload:test\n",
        )
        .unwrap();
        let home = root.join("home");
        fs::create_dir_all(home.join(".ssh")).unwrap();
        fs::write(
            home.join(".ssh/config"),
            "Host existing\n HostName example.invalid\n",
        )
        .unwrap();
        fs::write(home.join(".ssh/known_hosts"), "existing host key\n").unwrap();
        fs::write(root.join("mode"), mode).unwrap();
        let mut paths = vec![bin];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let child = Command::new(assert_cmd::cargo::cargo_bin!("snouty"))
            .env_clear()
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", root.join("settings"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("SNOUTY_UNSTABLE_FEATURES", "simulate")
            .env("SNOUTY_CONTAINER_ENGINE", "docker")
            .env("SNOUTY_TEMP_DIR", root)
            .env("TERM", "dumb")
            .args(["--json", "simulate"])
            .arg(config)
            .args([
                "--guest-image",
                "guest:test",
                "--timeout",
                if mode == "startup-timeout" {
                    "1s"
                } else {
                    "10s"
                },
            ])
            .args(if mode == "clean-once" {
                vec!["--disable-restart"]
            } else {
                Vec::new()
            })
            .stdin(Stdio::null())
            .stdout(if mode.starts_with("blocked-output") {
                Stdio::piped()
            } else {
                Stdio::from(fs::File::create(root.join("stdout")).unwrap())
            })
            .stderr(fs::File::create(root.join("stderr")).unwrap())
            .spawn()
            .unwrap();
        Self { directory, child }
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn read(&self, name: &str) -> String {
        fs::read_to_string(self.root().join(name)).unwrap_or_default()
    }

    fn wait_for(&mut self, name: &str, text: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.read(name).contains(text) {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "simulation exited early: {}",
                self.read("stderr")
            );
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {text}: {}",
                self.read("stderr")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn finish(&mut self) -> Output {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return Output {
                    status,
                    stdout: self.read("stdout").into_bytes(),
                    stderr: self.read("stderr").into_bytes(),
                };
            }
            assert!(
                Instant::now() < deadline,
                "simulation did not exit: {}",
                self.read("stderr")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let run_dir = self.read("run_dir");
        if !run_dir.is_empty() {
            let _ = fs::remove_dir_all(run_dir);
        }
        for file in ["qemu_pid", "qemu_child_pid"] {
            if let Ok(pid) = self.read(file).parse::<i32>() {
                let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
            }
        }
    }
}

#[test]
fn assertion_failure_is_latched_until_interruption_and_children_are_reaped() {
    let mut simulation = Simulation::start("assertion");
    simulation.wait_for("stdout", "still running after assertion");
    assert!(simulation.child.try_wait().unwrap().is_none());
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    let output = simulation.finish();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "assertion" && event["verdict"] == "failed")
    );
    let summary = events
        .iter()
        .find(|event| event["type"] == "summary")
        .unwrap();
    assert_eq!(summary["failures"]["assertion_failures"], 1);
    assert_eq!(summary["failures"]["infrastructure_failures"], 0);
    let run_dir = simulation.read("run_dir");
    for name in ["guest-dev-key", "ssh_config", "known_hosts", "qmp.sock"] {
        assert!(
            !Path::new(&run_dir).join(name).exists(),
            "runtime file {name} retained"
        );
    }
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private boot console"));
    assert_eq!(simulation.read("qemu_signal"), "15");
    for file in ["qemu_pid", "qemu_child_pid"] {
        let pid: i32 = simulation.read(file).parse().unwrap();
        assert!(
            kill(Pid::from_raw(pid), None).is_err(),
            "child {pid} survived shutdown"
        );
    }
    assert_eq!(
        simulation.read("home/.ssh/config"),
        "Host existing\n HostName example.invalid\n"
    );
    assert_eq!(
        simulation.read("home/.ssh/known_hosts"),
        "existing host key\n"
    );
    assert_eq!(simulation.read("key_mode"), "0o600");
    assert!(
        simulation
            .read("ssh_config")
            .contains("GlobalKnownHostsFile /dev/null")
    );
    assert_eq!(
        simulation.read("qmp_requests"),
        "qmp_capabilities\nsend-key\n"
    );
}

#[test]
fn catastrophic_startup_reports_hidden_boot_console() {
    let mut simulation = Simulation::start("boot-failure");
    let output = simulation.finish();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("fatal boot fixture"), "{stderr}");
    assert!(
        stderr.contains("QEMU exited before the guest booted"),
        "{stderr}"
    );
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn supervisor_failure_is_reported_after_workload_start() {
    let mut simulation = Simulation::start("supervisor-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("supervisor failed fixture"), "{stderr}");
    assert!(simulation.root().join("rollout_started").exists());
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let summary = events
        .iter()
        .find(|event| event["type"] == "summary")
        .unwrap();
    assert_eq!(summary["failures"]["infrastructure_failures"], 1);
    assert_eq!(simulation.read("qemu_signal"), "15");
}

#[test]
fn malformed_instrumentation_fails_and_cleans_up_vm() {
    let mut simulation = Simulation::start("malformed-json");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid instrumentation JSON"), "{stderr}");
    assert_eq!(simulation.read("qemu_signal"), "15");
}

#[test]
fn clean_single_rollout_keeps_streaming_and_preserves_signal_exit_status() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "workload running");
    assert!(simulation.child.try_wait().unwrap().is_none());
    assert_eq!(simulation.read("restart_mode"), "restart_enabled=no");
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(143));
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let summary = events
        .iter()
        .find(|event| event["type"] == "summary")
        .unwrap();
    assert_eq!(
        summary["failures"],
        serde_json::json!({"assertion_failures":0,"command_failures":0,"infrastructure_failures":0})
    );
    assert_eq!(summary["termination"], "terminated");
    assert!(summary["diagnostics"].is_null());
    assert!(!Path::new(&simulation.read("run_dir")).exists());
}

#[test]
fn assertion_written_at_shutdown_is_included_in_final_status() {
    use std::io::Write;

    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "workload running");
    let log = Path::new(&simulation.read("run_dir")).join("instrumentation.log");
    let mut file = fs::OpenOptions::new().append(true).open(log).unwrap();
    writeln!(file, "14.0 [workload] [JSON] '{{\"antithesis_assert\":{{\"message\":\"late failure\",\"assert_type\":\"always\",\"hit\":true,\"condition\":false}}}}'").unwrap();
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let summary = events
        .iter()
        .find(|event| event["type"] == "summary")
        .unwrap();
    assert_eq!(summary["failures"]["assertion_failures"], 1);
}

#[test]
fn missing_guest_image_is_pulled_before_boot() {
    let mut simulation = Simulation::start("missing-guest");
    simulation.wait_for("stdout", "workload running");
    let args: serde_json::Value = serde_json::from_str(&simulation.read("pull_args")).unwrap();
    assert_eq!(
        args,
        serde_json::json!(["pull", "--platform", "linux/amd64", "guest:test"])
    );
    assert_eq!(simulation.read("pulled"), "guest:test");
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn failed_guest_pull_never_starts_qemu() {
    let mut simulation = Simulation::start("pull-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("cannot pull guest image guest:test"),
        "{stderr}"
    );
    assert!(stderr.contains("registry denied fixture"), "{stderr}");
    assert!(!simulation.root().join("qemu_pid").exists());
    assert!(!simulation.root().join("pulled").exists());
}

#[test]
fn startup_timeout_reaps_qemu_and_reports_boot_diagnostics() {
    let mut simulation = Simulation::start("startup-timeout");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("guest startup timed out after 1 seconds"),
        "{stderr}"
    );
    assert!(stderr.contains("private boot console"), "{stderr}");
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(
        kill(Pid::from_raw(pid), None).is_err(),
        "QEMU survived startup timeout"
    );
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn unread_output_pipe_does_not_block_signal_cleanup() {
    let mut simulation = Simulation::start("blocked-output");
    simulation.wait_for("stderr", "Streaming guest logs");
    std::thread::sleep(Duration::from_millis(200));
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    let output = simulation.finish();
    assert_eq!(
        output.status.code(),
        Some(143),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
}

#[test]
fn unread_output_backlog_preserves_command_and_assertion_failures() {
    let mut simulation = Simulation::start("blocked-output-failures");
    simulation.wait_for("stderr", "Streaming guest logs");
    std::thread::sleep(Duration::from_millis(200));
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    let output = simulation.finish();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Simulation logs:"), "{stderr}");
    assert!(!stderr.contains("failed:"), "{stderr}");
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
}
