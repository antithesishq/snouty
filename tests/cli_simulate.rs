#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;
use expectrl::{ControlCode, Expect};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tempfile::TempDir;

struct Simulation {
    directory: TempDir,
    child: Child,
}

impl Simulation {
    fn start(mode: &str) -> Self {
        Self::start_with_api(mode, "http://127.0.0.1:1")
    }

    fn start_with_api(mode: &str, api_url: &str) -> Self {
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
        for tool in [
            "docker",
            "docker-compose",
            "ssh",
            "ssh-keygen",
            "qemu-system-x86_64",
        ] {
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
            .env("TMPDIR", root)
            .current_dir(root)
            .env("TERM", "dumb")
            .env("ANTITHESIS_BASE_URL", api_url)
            .env("ANTITHESIS_API_KEY", "test-key")
            .env("ANTITHESIS_REPOSITORY", "registry.example/team/")
            .args(["--json", "simulate"])
            .arg(config)
            .args(if mode == "default-image" {
                vec![]
            } else {
                vec!["--guest-image", "guest:test"]
            })
            .args([
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
        for file in ["qemu_pid", "qemu_child_pid"] {
            if let Ok(pid) = self.read(file).parse::<i32>() {
                let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
            }
        }
    }
}

#[test]
fn interrupted_startup_cleanup_ignores_unowned_paths() {
    let checkout = tempfile::tempdir().unwrap();
    let sentinel = checkout.path().join("sentinel");
    fs::write(&sentinel, "keep").unwrap();
    let mut simulation = Simulation::start("interrupt-before-boot");
    let root = simulation.root().to_path_buf();
    simulation.wait_for("before_boot", root.to_str().unwrap());
    assert!(!simulation.root().join("qemu_pid").exists());
    fs::write(
        simulation.root().join("run_dir"),
        checkout.path().as_os_str().as_encoded_bytes(),
    )
    .unwrap();
    kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
    simulation.finish();
    drop(simulation);
    assert!(!root.exists());
    assert_eq!(fs::read_to_string(sentinel).unwrap(), "keep");
}

#[test]
fn qemu_probe_and_unknown_arguments_do_not_write_vm_state() {
    let directory = tempfile::tempdir().unwrap();
    let bin = directory.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let script = bin.join("qemu-system-x86_64");
    fs::write(&script, include_str!("fixtures/simulate/mock_tools.py")).unwrap();
    fs::write(directory.path().join("mode"), "clean-once").unwrap();
    for args in [
        vec![
            "-accel",
            "kvm",
            "-machine",
            "none",
            "-display",
            "none",
            "-nodefaults",
            "-qmp",
            "stdio",
        ],
        vec!["--unexpected"],
    ] {
        let output = Command::new("python3")
            .arg(&script)
            .args(&args)
            .current_dir(directory.path())
            .output()
            .unwrap();
        if args[0] == "-accel" {
            assert!(output.status.success());
            let greeting: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert!(greeting.get("QMP").is_some());
        } else {
            assert!(!output.status.success());
        }
        for name in [
            "run_dir",
            "qemu_pid",
            "qemu_args",
            "boot.log",
            "instrumentation.log",
        ] {
            assert!(!directory.path().join(name).exists(), "unexpected {name}");
        }
    }
}

fn start_shell_simulation(mode: &str) -> (TempDir, OsSession) {
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
    for tool in ["docker", "ssh", "ssh-keygen", "qemu-system-x86_64"] {
        symlink(&script, bin.join(tool)).unwrap();
    }
    fs::write(root.join("mode"), mode).unwrap();
    let home = root.join("home");
    fs::create_dir(&home).unwrap();
    let mut paths = vec![bin];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let mut command = Command::new(assert_cmd::cargo::cargo_bin!("snouty"));
    command
        .env_clear()
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", root.join("settings"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("SNOUTY_UNSTABLE_FEATURES", "simulate")
        .env("SNOUTY_CONTAINER_ENGINE", "docker")
        .env("TMPDIR", root)
        .current_dir(root)
        .env("TERM", "xterm-256color")
        .args([
            "simulate",
            "--shell",
            "--guest-image",
            "guest:test",
            "--timeout",
            "10s",
        ]);
    let mut session = OsSession::spawn(command).expect("spawn shell simulation on a PTY");
    session.set_expect_timeout(Some(Duration::from_secs(15)));
    (directory, session)
}

#[test]
fn hidden_shell_bypasses_compose_and_owns_the_terminal() {
    let (directory, mut session) = start_shell_simulation("shell");
    let root = directory.path();
    let boot =
        Expect::expect(&mut session, "private boot console").expect("boot console is visible");
    assert!(!boot.as_bytes().windows(5).any(|bytes| bytes == b"\x1b[18t"));
    assert!(!boot.as_bytes().windows(4).any(|bytes| bytes == b"\x1b[6n"));
    Expect::expect(&mut session, "guest shell ready").expect("guest shell opened");
    Expect::send_line(&mut session, "whoami").expect("write to guest shell");
    Expect::expect(&mut session, "root").expect("guest shell replied");
    Expect::send_line(&mut session, "poweroff").expect("power off guest");
    let closing =
        Expect::expect(&mut session, "Stopping simulation...").expect("simulation stopped");
    let tail = Expect::expect(&mut session, expectrl::Eof).expect("simulation closed its terminal");
    let status = session
        .get_process()
        .wait()
        .expect("wait for shell simulation");
    let poweroff = fs::read_to_string(root.join("guest_poweroff")).unwrap_or_default();
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "shell simulation failed: {status:?}; guest_poweroff={poweroff:?}\n{}",
        String::from_utf8_lossy(tail.as_bytes())
    );
    assert_eq!(
        fs::read_to_string(root.join("shell_opened")).unwrap(),
        "yes"
    );
    assert_eq!(
        fs::read_to_string(root.join("shell_input")).unwrap(),
        "whoami\npoweroff"
    );
    assert!(!root.join("batch_script").exists());
    assert!(!root.join("rollout_started").exists());
    assert_eq!(poweroff, "yes");
    let closing = String::from_utf8_lossy(closing.as_bytes());
    assert_eq!(
        closing.matches("Connection to 127.0.0.1 closed.").count(),
        1
    );
    assert!(!closing.contains("closed by remote host"));
    assert_eq!(fs::read_to_string(root.join("qemu_signal")).unwrap(), "15");
    let run_dir = fs::read_to_string(root.join("run_dir")).unwrap();
    assert!(!Path::new(&run_dir).exists());
}

#[test]
fn interrupting_shell_boot_stops_qemu_and_removes_runtime_files() {
    let (directory, mut session) = start_shell_simulation("shell-booting");
    let root = directory.path();
    Expect::expect(&mut session, "private boot console").expect("guest started booting");
    Expect::send(&mut session, ControlCode::ETX).expect("interrupt guest boot");
    let tail = Expect::expect(&mut session, expectrl::Eof).expect("simulation closed its terminal");
    let status = session
        .get_process()
        .wait()
        .expect("wait for shell simulation");
    assert!(
        matches!(status, WaitStatus::Exited(_, 130)),
        "shell simulation failed: {status:?}\n{}",
        String::from_utf8_lossy(tail.as_bytes())
    );
    for file in ["qemu_pid", "qemu_child_pid"] {
        let pid: i32 = fs::read_to_string(root.join(file))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            kill(Pid::from_raw(pid), None).is_err(),
            "child {pid} survived interrupt"
        );
    }
    let run_dir = fs::read_to_string(root.join("run_dir")).unwrap();
    assert!(!Path::new(&run_dir).exists());
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
    assert!(
        !Path::new(&run_dir).join("ssh_config").exists(),
        "runtime SSH config retained"
    );
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
    assert_eq!(simulation.read("authorized_key"), "fixture public key\n");
    let private_key = simulation.read("private_key_path");
    let public_key = simulation.read("public_key_path");
    assert!(!Path::new(&private_key).exists());
    assert!(!Path::new(&public_key).exists());
    let qemu_args: Vec<String> = serde_json::from_str(&simulation.read("qemu_args")).unwrap();
    let fw_cfg = qemu_args
        .windows(2)
        .find(|args| args[0] == "-fw_cfg")
        .map(|args| &args[1])
        .unwrap();
    assert_eq!(
        fw_cfg,
        &format!("name=opt/antithesis/authorized_key,file={public_key}")
    );
    assert!(
        !qemu_args
            .iter()
            .any(|arg| arg == &format!("name=opt/antithesis/authorized_key,file={private_key}"))
    );
    assert!(
        qemu_args
            .iter()
            .any(|arg| arg.contains("hostfwd=tcp:127.0.0.1:") && arg.ends_with("-:22,restrict=yes"))
    );
    let ssh_config = simulation.read("ssh_config");
    assert!(ssh_config.contains("IdentitiesOnly yes"));
    assert!(ssh_config.contains("IdentityAgent none"));
    assert!(ssh_config.contains("StrictHostKeyChecking no"));
    assert!(ssh_config.contains("UserKnownHostsFile /dev/null"));
    assert!(ssh_config.contains("GlobalKnownHostsFile /dev/null"));
    assert!(!ssh_config.contains(".pub"));
    assert!(!simulation.read("qemu_args").contains("-qmp"));
}

#[test]
fn catastrophic_startup_reports_hidden_boot_console() {
    let mut simulation = Simulation::start("boot-failure");
    let output = simulation.finish();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("fatal boot fixture"), "{stderr}");
    assert!(
        stderr.contains("QEMU exited before SSH became ready"),
        "{stderr}"
    );
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn ssh_authentication_failure_stops_qemu_without_waiting_for_timeout() {
    let mut simulation = Simulation::start("auth-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("guest SSH authentication failed"),
        "{stderr}"
    );
    assert!(stderr.contains("Permission denied (publickey)"), "{stderr}");
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
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
    let run_dir = std::path::PathBuf::from(simulation.read("run_dir"));
    assert!(run_dir.starts_with(simulation.root()));
    assert!(run_dir.join("instrumentation.log").exists());
    assert!(!stderr.contains("failed:"), "{stderr}");
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
    drop(simulation);
    assert!(!run_dir.exists());
}

#[tokio::test]
async fn guest_image_defaults_to_configured_repository() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/version"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "latest_api_version": "v1", "release_version": "62.2"
            })),
        )
        .expect(1)
        .mount(&server)
        .await;
    let api_url = server.uri();
    tokio::task::spawn_blocking(move || {
        let mut simulation = Simulation::start_with_api("default-image", &api_url);
        simulation.wait_for("stdout", "workload running");
        assert_eq!(
            simulation.read("guest_image"),
            "registry.example/team/antithesis-guest:v62.2"
        );
        kill(Pid::from_raw(simulation.child.id() as i32), Signal::SIGTERM).unwrap();
        simulation.finish();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_release_lookup_requires_explicit_image() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/api/version"))
        .respond_with(wiremock::ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let api_url = server.uri();
    tokio::task::spawn_blocking(move || {
        let mut simulation = Simulation::start_with_api("default-image", &api_url);
        let output = simulation.finish();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("HTTP 403"), "{stderr}");
        assert!(stderr.contains("--guest-image"), "{stderr}");
        assert!(!simulation.root().join("qemu_pid").exists());
    })
    .await
    .unwrap();
}
