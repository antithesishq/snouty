#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::fs;
use std::io::Write as _;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use expectrl::Expect;
use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;
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
            "qemu-system-aarch64",
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
        let qemu_data = root.join("share/qemu");
        fs::create_dir_all(qemu_data.join("firmware")).unwrap();
        let x86_code = qemu_data.join("environment-x86-code.fd");
        let x86_vars = qemu_data.join("environment-x86-vars.fd");
        fs::write(&x86_code, "x86 firmware code").unwrap();
        fs::write(&x86_vars, "x86 firmware variables").unwrap();
        fs::write(
            qemu_data.join("firmware/00-environment-x86.json"),
            serde_json::json!({
                "interface-types": ["uefi"],
                "mapping": {
                    "device": "flash",
                    "executable": {"filename": x86_code, "format": "raw"},
                    "nvram-template": {"filename": x86_vars, "format": "raw"}
                },
                "targets": [{"architecture": "x86_64", "machines": ["pc-q35-*"]}]
            })
            .to_string(),
        )
        .unwrap();
        let code = qemu_data.join("environment-arm-code.fd");
        let vars = qemu_data.join("environment-arm-vars.fd");
        fs::write(&code, "QEMU firmware code").unwrap();
        fs::write(&vars, "QEMU firmware variables").unwrap();
        let secure_code = qemu_data.join("secure-arm-code.fd");
        fs::write(&secure_code, "secure firmware code").unwrap();
        fs::write(
            qemu_data.join("firmware/00-secure-arm.json"),
            serde_json::json!({
                "interface-types": ["uefi"],
                "mapping": {"device": "flash", "executable": {"filename": secure_code, "format": "raw"}},
                "targets": [{"architecture": "aarch64", "machines": ["virt-*"]}],
                "features": ["secure-boot"]
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            qemu_data.join("firmware/01-environment-arm.json"),
            serde_json::json!({
                "interface-types": ["uefi"],
                "mapping": {
                    "device": "flash",
                    "executable": {"filename": code, "format": "raw"},
                    "nvram-template": {"filename": vars, "format": "raw"}
                },
                "targets": [{"architecture": "aarch64", "machines": ["virt-*"]}],
                "features": ["verbose-static"]
            })
            .to_string(),
        )
        .unwrap();
        if mode == "arm64-firmware-combined" {
            let combined_code = qemu_data.join("combined-arm-code.fd");
            fs::write(&combined_code, "combined firmware code").unwrap();
            fs::write(
                qemu_data.join("firmware/00-combined-arm.json"),
                serde_json::json!({
                    "interface-types": ["uefi"],
                    "mapping": {
                        "device": "flash",
                        "mode": "combined",
                        "executable": {"filename": combined_code, "format": "raw"}
                    },
                    "targets": [{"architecture": "aarch64", "machines": ["virt-*"]}]
                })
                .to_string(),
            )
            .unwrap();
        }
        if mode == "shared-cache" {
            // `snouty docs` and `simulate --shell` create these with the umask.
            let simulate = root.join("cache/snouty/simulate");
            fs::create_dir_all(&simulate).unwrap();
            for directory in [simulate.parent().unwrap(), &simulate] {
                fs::set_permissions(directory, fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        let mut paths = vec![bin];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let child = Command::new(assert_cmd::cargo::cargo_bin!("snouty"))
            .env_clear()
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", root.join("settings"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env("XDG_RUNTIME_DIR", root.join("runtime"))
            .env("SNOUTY_UNSTABLE_FEATURES", "simulate")
            .env("SNOUTY_CONTAINER_ENGINE", "docker")
            .env("TMPDIR", root)
            .current_dir(root)
            .env("TERM", "dumb")
            .env("ANTITHESIS_BASE_URL", api_url)
            .env("ANTITHESIS_REPOSITORY", "registry.example/team/")
            .envs(match mode {
                "missing-local-iso" => vec![("SNOUTY_SIMULATE_GUEST_ISO", "missing.iso")],
                "directory-local-iso" => vec![("SNOUTY_SIMULATE_GUEST_ISO", "config")],
                "empty-local-iso" => vec![("SNOUTY_SIMULATE_GUEST_ISO", "")],
                _ => vec![],
            })
            .args(if mode == "human-blocked-upload" {
                vec!["simulate"]
            } else {
                vec!["--json", "simulate"]
            })
            .arg(config)
            .args(if mode == "default-image" {
                vec![]
            } else {
                vec!["--environment-image", "environment:test"]
            })
            .args([
                "--platform",
                if mode.starts_with("arm64") {
                    "arm64"
                } else {
                    "amd64"
                },
            ])
            .args([
                "--timeout",
                if mode == "login-timeout" { "1s" } else { "10s" },
            ])
            .args(if mode == "clean-once" {
                vec!["--disable-restart"]
            } else {
                Vec::new()
            })
            .args(if matches!(mode, "no-faults" | "fault-pause-failure") {
                vec!["--fault-tolerance", "none"]
            } else {
                Vec::new()
            })
            .args(if mode == "legacy-faults" {
                vec!["--fault-tolerance", "legacy"]
            } else if mode == "resilient-faults" {
                vec!["--fault-tolerance", "resilient"]
            } else if mode == "configure-faults" {
                vec![
                    "--fault-tolerance",
                    "high_availability",
                    "--fault-exclusions=-db'$(touch injected);\ncache",
                ]
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
    /// Asks snouty to stop first, so that it removes its socket directory in
    /// /tmp, which ignores TMPDIR.
    fn drop(&mut self) {
        if self.child.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = kill(
                Pid::from_raw(self.child.id().cast_signed()),
                Signal::SIGTERM,
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.child.try_wait().is_ok_and(|status| status.is_none())
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
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
fn arm64_environment_uses_uefi_and_arm64_images() {
    let mut simulation = Simulation::start("arm64");
    simulation.wait_for("stdout", "workload running");
    assert_eq!(simulation.read("qemu_tool"), "qemu-system-aarch64");
    let args: Vec<String> = serde_json::from_str(&simulation.read("qemu_args")).unwrap();
    let accelerator = if cfg!(target_os = "macos") {
        "hvf:tcg"
    } else {
        "kvm:tcg"
    };
    assert!(args.windows(2).any(|args| {
        args == [
            "-machine",
            &format!("virt,gic-version=max,accel={accelerator}"),
        ]
    }));
    assert!(args.windows(2).any(|args| args == ["-cpu", "max"]));
    assert!(
        args.windows(2)
            .any(|args| args == ["-device", "pci-serial,chardev=instrumentation"])
    );
    assert!(args.windows(2).any(|args| {
        args == [
            "-chardev",
            "file,id=instrumentation,path=instrumentation.log",
        ]
    }));
    assert!(
        args.windows(2)
            .any(|args| args == ["-device", "virtio-net-pci,netdev=net0"])
    );
    let run_dir = Path::new(simulation.read("run_dir").trim()).to_path_buf();
    assert_eq!(
        args.windows(2)
            .find(|args| args[0] == "-bios")
            .map(|args| fs::read_to_string(&args[1]).unwrap()),
        Some("QEMU firmware code".to_owned())
    );
    assert!(!run_dir.join("uefi_vars.fd").exists());
    let pull: serde_json::Value = serde_json::from_str(&simulation.read("pull_args")).unwrap();
    assert_eq!(
        pull,
        serde_json::json!(["pull", "--platform", "linux/arm64", "environment:test"])
    );
    let create: Vec<String> = serde_json::from_str(&simulation.read("create_args")).unwrap();
    assert!(
        create
            .windows(2)
            .any(|args| args == ["--platform", "linux/arm64"])
    );
    let save: Vec<String> = serde_json::from_str(&simulation.read("save_args")).unwrap();
    assert!(
        save.windows(2)
            .any(|args| args == ["--platform", "linux/arm64"])
    );
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn combined_firmware_uses_bios_without_copying_image() {
    let mut simulation = Simulation::start("arm64-firmware-combined");
    simulation.wait_for("qemu_args", "-bios");
    let args: Vec<String> = serde_json::from_str(&simulation.read("qemu_args")).unwrap();
    assert_eq!(
        args.windows(2)
            .find(|args| args[0] == "-bios")
            .map(|args| Path::new(&args[1]).to_path_buf()),
        Some(simulation.root().join("share/qemu/combined-arm-code.fd"))
    );
    let run_dir = Path::new(simulation.read("run_dir").trim()).to_path_buf();
    assert!(!run_dir.join("uefi_vars.fd").exists());
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn arm64_environment_rejects_amd64_workload_images() {
    let mut simulation = Simulation::start("arm64-wrong-workload");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("simulate requires arm64"), "{stderr}");
    assert!(!simulation.root().join("compose_started").exists());
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
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    drop(simulation);
    assert!(!root.exists());
    assert_eq!(fs::read_to_string(sentinel).unwrap(), "keep");
}

fn attach_command(root: &Path, id: &str) -> Command {
    let mut paths = vec![root.join("bin")];
    paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    let mut command = Command::new(assert_cmd::cargo::cargo_bin!("snouty"));
    command
        .env_clear()
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("settings"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("XDG_RUNTIME_DIR", root.join("runtime"))
        .env("SNOUTY_UNSTABLE_FEATURES", "simulate")
        .env("TMPDIR", root)
        .env("TERM", "xterm-256color")
        .current_dir(root)
        .args(["simulate", "--attach", id]);
    command
}

#[test]
fn attach_opens_shell_without_stopping_compose_simulation() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "simulation_started");
    let start: serde_json::Value = simulation
        .read("stdout")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|event| event["type"] == "simulation_started")
        .unwrap();
    let id = start["run_id"].as_str().unwrap();
    let run_dir = Path::new(&simulation.read("run_dir")).to_owned();
    assert!(
        run_dir.starts_with(simulation.root()),
        "{}",
        run_dir.display()
    );
    assert_eq!(run_dir.file_name().unwrap(), id);
    assert_eq!(
        run_dir,
        simulation.root().join("cache/snouty/simulate").join(id)
    );
    assert!(run_dir.join("attach-ready").exists());

    let mut shell = OsSession::spawn(attach_command(simulation.root(), id)).unwrap();
    shell.set_expect_timeout(Some(Duration::from_secs(15)));
    Expect::expect(&mut shell, "environment shell ready").unwrap();
    Expect::send_line(&mut shell, "whoami").unwrap();
    Expect::expect(&mut shell, "root").unwrap();
    Expect::send_line(&mut shell, "exit").unwrap();
    Expect::expect(&mut shell, expectrl::Eof).unwrap();
    assert!(matches!(
        shell.get_process().wait().unwrap(),
        WaitStatus::Exited(_, 0)
    ));
    assert!(simulation.child.try_wait().unwrap().is_none());
    assert!(run_dir.join("attach-ready").exists());

    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(143));
    assert!(!run_dir.exists());
}

#[test]
fn human_simulation_prints_the_attach_command() {
    let mut simulation = Simulation::start("human-blocked-upload");
    simulation.wait_for("stderr", "Attach: snouty simulate --attach simulate-");
    simulation.wait_for("upload_blocked", "yes");
    let run_dir = simulation.read("run_dir");
    let id = Path::new(&run_dir).file_name().unwrap().to_str().unwrap();
    assert!(simulation.read("stderr").contains(&format!("Run ID: {id}")));
    assert!(
        simulation
            .read("stderr")
            .contains(&format!("Attach: snouty simulate --attach {id}"))
    );
    assert!(!simulation.read("compose_started").contains("yes"));
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn json_simulation_emits_run_id_before_upload_finishes() {
    let mut simulation = Simulation::start("blocked-upload");
    simulation.wait_for("stdout", "simulation_started");
    simulation.wait_for("upload_blocked", "yes");
    let start: serde_json::Value =
        serde_json::from_str(simulation.read("stdout").lines().next().unwrap()).unwrap();
    let id = start["run_id"].as_str().unwrap();
    assert_eq!(start["type"], "simulation_started");
    assert_eq!(
        Path::new(&simulation.read("run_dir"))
            .file_name()
            .unwrap()
            .to_str()
            .unwrap(),
        id
    );
    assert!(!simulation.read("compose_started").contains("yes"));
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn attach_rejects_a_stopped_simulation_with_retained_logs() {
    let mut simulation = Simulation::start("assertion");
    simulation.wait_for("stdout", "simulation_started");
    simulation.wait_for("stdout", "balance stays positive");
    let start: serde_json::Value =
        serde_json::from_str(simulation.read("stdout").lines().next().unwrap()).unwrap();
    let id = start["run_id"].as_str().unwrap();
    let run_dir = Path::new(&simulation.read("run_dir")).to_owned();
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(1));
    assert!(run_dir.exists());
    assert!(!run_dir.join("attach-ready").exists());

    let mut shell = OsSession::spawn(attach_command(simulation.root(), id)).unwrap();
    shell.set_expect_timeout(Some(Duration::from_secs(15)));
    Expect::expect(&mut shell, "is no longer running").unwrap();
    Expect::expect(&mut shell, expectrl::Eof).unwrap();
    assert!(matches!(
        shell.get_process().wait().unwrap(),
        WaitStatus::Exited(_, 1)
    ));
}

#[test]
fn assertion_failure_is_latched_until_interruption_and_children_are_reaped() {
    let mut simulation = Simulation::start("assertion");
    simulation.wait_for("stdout", "still running after assertion");
    assert!(simulation.child.try_wait().unwrap().is_none());
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
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
    assert_eq!(simulation.read("poweroff_requested"), "yes");
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
    let private_key = simulation.read("private_key_path");
    let public_key = simulation.read("public_key_path");
    assert!(!Path::new(&private_key).exists());
    assert!(!Path::new(&public_key).exists());
    let ssh_config = simulation.read("ssh_config");
    assert!(ssh_config.contains("IdentitiesOnly yes"));
    assert!(ssh_config.contains("IdentityAgent none"));
    assert!(ssh_config.contains("StrictHostKeyChecking no"));
    assert!(ssh_config.contains("UserKnownHostsFile /dev/null"));
    assert!(ssh_config.contains("GlobalKnownHostsFile /dev/null"));
    assert!(ssh_config.contains("Compression no"));
    assert!(ssh_config.contains("ProxyUseFdpass yes"));
    assert!(ssh_config.contains(" internal-ssh-proxy "));
    assert!(!ssh_config.contains("\n Port "));
    assert!(!ssh_config.contains(".pub"));
}

#[test]
fn json_events_are_annotated_with_environment_faults() {
    let mut simulation = Simulation::start("fault-annotation");
    simulation.wait_for("stdout", "balance stays positive");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    let output = simulation.finish();
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let assertion = events
        .iter()
        .find(|event| event["type"] == "assertion")
        .unwrap();
    assert_eq!(
        assertion["active_faults"],
        serde_json::json!({"network_clog": {"vtime": 11.0}})
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
        stderr.contains("QEMU exited before SSH became ready"),
        "{stderr}"
    );
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn ssh_waits_for_a_complete_readiness_record() {
    let mut simulation = Simulation::start("fragmented-ready");
    simulation.wait_for("marker_pending", "yes");
    std::thread::sleep(Duration::from_millis(150));
    assert!(!simulation.root().join("ssh_invoked").exists());
    fs::write(simulation.root().join("emit_marker"), "").unwrap();
    simulation.wait_for("marker_partial", "yes");
    std::thread::sleep(Duration::from_millis(150));
    assert!(!simulation.root().join("ssh_invoked").exists());
    fs::write(simulation.root().join("finish_marker"), "").unwrap();
    simulation.wait_for("rollout_started", "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    let output = simulation.finish();
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ssh_ready"));
}

#[test]
fn ssh_login_check_allows_a_slow_connection() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("rollout_started", "yes");
    let options: Vec<String> = serde_json::from_str(&simulation.read("readiness_options")).unwrap();
    assert_eq!(options, ["-o", "ConnectTimeout=30"]);
    assert!(simulation.read("ssh_config").contains("ConnectTimeout 2"));
}

#[test]
fn first_ssh_login_can_take_longer_than_five_seconds() {
    let mut simulation = Simulation::start("slow-login");
    simulation.wait_for("rollout_started", "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
}

#[test]
fn ssh_login_keeps_startup_deadline_and_qemu_exit_detection() {
    for (mode, expected) in [
        (
            "login-timeout",
            "simulation environment startup timed out waiting for SSH login after readiness marker",
        ),
        ("login-qemu-exit", "QEMU exited before SSH became ready"),
    ] {
        let mut simulation = Simulation::start(mode);
        let output = simulation.finish();
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{stderr}");
        assert!(stderr.contains("private boot console"), "{stderr}");
        assert!(!simulation.root().join("rollout_started").exists());
    }
}

#[test]
fn ssh_authentication_failure_stops_qemu_without_waiting_for_timeout() {
    let mut simulation = Simulation::start("auth-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("SSH login to the simulation environment failed"),
        "{stderr}"
    );
    assert!(stderr.contains("Permission denied (publickey)"), "{stderr}");
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
}

#[test]
fn composer_failure_is_reported_after_workload_start() {
    let mut simulation = Simulation::start("composer-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("composer failed fixture"), "{stderr}");
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
    assert_eq!(simulation.read("poweroff_requested"), "yes");
}

#[test]
fn malformed_instrumentation_is_counted_and_streaming_continues() {
    let mut simulation = Simulation::start("malformed-json");
    simulation.wait_for("stdout", "still running after malformed line");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid instrumentation JSON"), "{stderr}");
    let summary: serde_json::Value = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|event: &serde_json::Value| event["type"] == "summary")
        .unwrap();
    assert_eq!(summary["failures"]["infrastructure_failures"], 1);
    assert_eq!(simulation.read("poweroff_requested"), "yes");
}

#[test]
fn shared_cache_directory_created_by_other_commands_is_accepted() {
    let mut simulation = Simulation::start("shared-cache");
    simulation.wait_for("rollout_started", "yes");
}

#[test]
fn clean_single_rollout_keeps_streaming_and_preserves_signal_exit_status() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "workload running");
    assert!(simulation.child.try_wait().unwrap().is_none());
    assert_eq!(simulation.read("thread_pause_enabled"), "yes");
    assert!(
        simulation
            .read("start_script")
            .contains("export COMPOSE_PROJECT_NAME=antithesis")
    );
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
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
fn interrupt_powers_off_environment_and_drains_final_output() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "workload running");
    let start = Instant::now();
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGINT,
    )
    .unwrap();
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(130));
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(simulation.read("poweroff_requested"), "yes");
    assert_eq!(
        simulation.read("qemu_signal"),
        (Signal::SIGUSR1 as i32).to_string()
    );
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("last output before poweroff")
    );
}

#[test]
fn failed_or_stalled_poweroff_is_force_killed() {
    for mode in ["poweroff-fails", "poweroff-hangs"] {
        let mut simulation = Simulation::start(mode);
        simulation.wait_for("compose_started", "yes");
        let start = Instant::now();
        kill(
            Pid::from_raw(simulation.child.id().cast_signed()),
            Signal::SIGINT,
        )
        .unwrap();
        let output = simulation.finish();
        assert_eq!(output.status.code(), Some(130));
        assert!(start.elapsed() < Duration::from_secs(4));
        assert_eq!(simulation.read("poweroff_requested"), "yes");
        let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
        assert!(
            kill(Pid::from_raw(pid), None).is_err(),
            "QEMU survived {mode}"
        );
    }
}

#[test]
fn second_interrupt_skips_poweroff_wait() {
    let mut simulation = Simulation::start("poweroff-second-interrupt");
    simulation.wait_for("compose_started", "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGINT,
    )
    .unwrap();
    simulation.wait_for("poweroff_requested", "yes");
    let start = Instant::now();
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGINT,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(130));
    assert!(start.elapsed() < Duration::from_secs(1));
    let pid: i32 = simulation.read("qemu_pid").parse().unwrap();
    assert!(kill(Pid::from_raw(pid), None).is_err());
}

#[test]
fn assertion_written_at_shutdown_is_included_in_final_status() {
    let mut simulation = Simulation::start("clean-once");
    simulation.wait_for("stdout", "workload running");
    let log = Path::new(&simulation.read("run_dir")).join("instrumentation.log");
    let mut file = fs::OpenOptions::new().append(true).open(log).unwrap();
    writeln!(file, "14.0 [workload] [JSON] '{{\"antithesis_assert\":{{\"message\":\"late failure\",\"assert_type\":\"always\",\"hit\":true,\"condition\":false}}}}'").unwrap();
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
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
fn invalid_local_environment_iso_aborts_without_falling_back_to_an_image() {
    for mode in [
        "missing-local-iso",
        "directory-local-iso",
        "empty-local-iso",
    ] {
        let mut simulation = Simulation::start(mode);
        let output = simulation.finish();
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("SNOUTY_SIMULATE_GUEST_ISO")
        );
        assert!(!simulation.root().join("qemu_pid").exists());
        assert!(!simulation.root().join("environment_image").exists());
    }
}

#[test]
fn missing_environment_image_is_pulled_before_boot() {
    let mut simulation = Simulation::start("missing-environment");
    simulation.wait_for("stdout", "workload running");
    let args: serde_json::Value = serde_json::from_str(&simulation.read("pull_args")).unwrap();
    assert_eq!(
        args,
        serde_json::json!(["pull", "--platform", "linux/amd64", "environment:test"])
    );
    assert_eq!(simulation.read("pulled"), "environment:test");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn environment_inspect_failure_is_reported_without_pulling() {
    let mut simulation = Simulation::start("inspect-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Cannot connect to the Docker daemon fixture"),
        "{stderr}"
    );
    assert!(!simulation.root().join("pull_args").exists());
    assert!(!simulation.root().join("qemu_pid").exists());
}

#[test]
fn failed_environment_pull_never_starts_qemu() {
    let mut simulation = Simulation::start("pull-failure");
    let output = simulation.finish();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("cannot pull simulation environment image environment:test"),
        "{stderr}"
    );
    assert!(stderr.contains("registry denied fixture"), "{stderr}");
    assert!(!simulation.root().join("qemu_pid").exists());
    assert!(!simulation.root().join("pulled").exists());
}

#[test]
fn unread_output_pipe_does_not_block_signal_cleanup() {
    let mut simulation = Simulation::start("blocked-output");
    simulation.wait_for("stderr", "Streaming simulation logs");
    std::thread::sleep(Duration::from_millis(200));
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
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
    simulation.wait_for("stderr", "Streaming simulation logs");
    std::thread::sleep(Duration::from_millis(200));
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
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
async fn environment_image_defaults_to_configured_repository() {
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/api/version"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "latest_api_version": "v1", "release_version": "62.2"
            })),
        )
        .expect(0)
        .mount(&server)
        .await;
    let api_url = server.uri();
    tokio::task::spawn_blocking(move || {
        let mut simulation = Simulation::start_with_api("default-image", &api_url);
        simulation.wait_for("stdout", "workload running");
        assert_eq!(
            simulation.read("environment_image"),
            "registry.example/team/antithesis-environment:v67.3"
        );
        kill(
            Pid::from_raw(simulation.child.id().cast_signed()),
            Signal::SIGTERM,
        )
        .unwrap();
        simulation.finish();
    })
    .await
    .unwrap();
}

#[test]
fn default_environment_image_works_without_api_access() {
    let mut simulation = Simulation::start("default-image");
    simulation.wait_for("stdout", "workload running");
    assert_eq!(
        simulation.read("environment_image"),
        "registry.example/team/antithesis-environment:v67.3"
    );
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    assert_eq!(simulation.finish().status.code(), Some(143));
}

#[test]
fn composer_waits_for_setup_complete_and_wait_is_interruptible() {
    let mut simulation = Simulation::start("waiting-setup");
    simulation.wait_for("stdout", "still running after assertion");
    assert!(simulation.root().join("compose_started").exists());
    assert!(!simulation.root().join("rollout_started").exists());
    assert!(!simulation.root().join("faults_unpaused").exists());
    assert!(simulation.read("entropy_calls").is_empty());
    assert_eq!(simulation.read("faults_paused"), "yes");
    let log = Path::new(&simulation.read("run_dir")).join("instrumentation.log");
    writeln!(
        fs::OpenOptions::new().append(true).open(log).unwrap(),
        "14 [workload] [JSON] '{{\"antithesis_setup\":{{\"status\":\"complete\"}}}}'"
    )
    .unwrap();
    simulation.wait_for("rollout_started", "yes");
    assert_eq!(simulation.read("faults_unpaused"), "yes");
    assert_eq!(simulation.read("faults_paused"), "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();

    let mut simulation = Simulation::start("waiting-setup");
    simulation.wait_for("stdout", "still running after assertion");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn fault_tolerance_none_keeps_faults_paused_and_starts_composer() {
    let mut simulation = Simulation::start("no-faults");
    simulation.wait_for("compose_started", "yes");
    let args: Vec<String> = serde_json::from_str(&simulation.read("configure_args")).unwrap();
    assert_eq!(
        args,
        [
            "/opt/antithesis/bin/fault_scripts/randomization/configure.py",
            "--fault-tolerance",
            "none"
        ]
    );
    assert_eq!(simulation.read("faults_paused"), "yes");
    simulation.wait_for("rollout_started", "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert!(!simulation.root().join("faults_unpaused").exists());
}

#[test]
fn fault_configuration_is_forwarded_before_compose() {
    let mut simulation = Simulation::start("configure-faults");
    simulation.wait_for("compose_started", "yes");
    let args: Vec<String> = serde_json::from_str(&simulation.read("configure_args")).unwrap();
    assert_eq!(
        args,
        vec![
            "/opt/antithesis/bin/fault_scripts/randomization/configure.py",
            "--fault-tolerance",
            "high_availability",
            "--fault-exclusions=-db'$(touch injected);\ncache",
        ]
    );
    simulation.wait_for("faults_unpaused", "yes");
    assert_eq!(simulation.read("faults_paused"), "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();

    for (mode, tolerance) in [
        ("clean-once", "resilient"),
        ("legacy-faults", "legacy"),
        ("resilient-faults", "resilient"),
    ] {
        let mut simulation = Simulation::start(mode);
        simulation.wait_for("compose_started", "yes");
        let args: Vec<String> = serde_json::from_str(&simulation.read("configure_args")).unwrap();
        assert_eq!(
            args,
            [
                "/opt/antithesis/bin/fault_scripts/randomization/configure.py",
                "--fault-tolerance",
                tolerance
            ]
        );
        simulation.wait_for("faults_unpaused", "yes");
        assert_eq!(simulation.read("faults_paused"), "yes");
        kill(
            Pid::from_raw(simulation.child.id().cast_signed()),
            Signal::SIGTERM,
        )
        .unwrap();
        simulation.finish();
    }
}

#[test]
fn configure_failure_stops_simulation_before_compose() {
    let mut simulation = Simulation::start("configure-failure");
    let output = simulation.finish();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    for expected in [
        "0: configuring faults with configure.py",
        "1: command in the simulation environment failed (exit status: 42)",
        "stdout:\n      configure.py configuration details",
        "stderr:\n      configure.py rejected configuration",
    ] {
        assert!(
            stderr.contains(expected),
            "missing {expected:?} in {stderr}"
        );
    }
    assert!(!simulation.root().join("compose_started").exists());
}

#[test]
fn fault_injector_failure_stops_simulation_before_composer() {
    let mut simulation = Simulation::start("fault-injector-failure");
    let output = simulation.finish();
    assert!(!output.status.success());
    assert_eq!(simulation.read("faults_unpaused"), "yes");
    assert!(!simulation.root().join("rollout_started").exists());
}

#[test]
fn composer_config_disables_fault_control_and_preserves_other_settings() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    let original = serde_json::json!({
        "unpause_fault_injector": true,
        "other": {"enabled": true, "values": [1, "two", null]}
    });
    let script = include_str!("../src/simulate/assets/disable-faults.sh")
        .replace(
            "/run/antithesis-local-injection",
            directory.path().join("state").to_str().unwrap(),
        )
        .replace("/opt/antithesis/test/config.json", config.to_str().unwrap())
        .replace(
            "/run/systemd/system",
            directory.path().join("systemd").to_str().unwrap(),
        )
        .replace("/run/current-system/sw/bin/bash", "bash");
    fs::create_dir(directory.path().join("state")).unwrap();
    let script = format!(
        "systemctl() {{ test \"$*\" = daemon-reload; }}\nfault_injector_update() {{ test \"$*\" = --pause; }}\n{script}"
    );
    let output = Command::new("bash").args(["-c", &script]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!config.exists());
    let unit = fs::read_to_string(
        directory
            .path()
            .join("systemd/antithesis-test-composer.service.d/fault-control.conf"),
    )
    .unwrap();
    let script = unit
        .lines()
        .find_map(|line| line.strip_prefix("ExecStartPre="))
        .unwrap();
    fs::write(&config, original.to_string()).unwrap();
    let output = Command::new("bash").args(["-c", script]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let updated: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&config).unwrap()).unwrap();
    assert_eq!(
        updated,
        serde_json::json!({
            "unpause_fault_injector": false,
            "other": original["other"]
        })
    );

    fs::write(&config, "{invalid").unwrap();
    let output = Command::new("bash").args(["-c", script]).output().unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read_to_string(config).unwrap(), "{invalid");
}

#[test]
fn fault_pause_failure_stops_simulation_before_compose() {
    let mut simulation = Simulation::start("fault-pause-failure");
    let output = simulation.finish();
    assert!(!output.status.success());
    assert_eq!(simulation.read("faults_paused"), "yes");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("0: disabling faults\n   1: command in the simulation environment failed (exit status: 1)"),
        "{stderr}"
    );
    assert!(!simulation.root().join("compose_started").exists());
}

#[test]
fn entropy_waits_for_composer_readiness_and_wakes_it_immediately() {
    let mut simulation = Simulation::start("readiness-blocked");
    simulation.wait_for("composer_starting", "yes");
    assert!(simulation.read("entropy_calls").is_empty());
    let ready = Instant::now();
    fs::write(simulation.root().join("ready"), "").unwrap();
    simulation.wait_for("entropy_calls", "true");
    assert!(ready.elapsed() < Duration::from_secs(2));
    let events = simulation.read("cycle_events");
    assert!(
        events.starts_with("pause\nunpause\ncomposer-start\ncomposer-ready\nentropy "),
        "{events}"
    );
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert_eq!(simulation.read("entropy_calls").lines().count(), 1);
}

#[test]
fn composer_readiness_failure_never_adds_entropy() {
    let mut simulation = Simulation::start("readiness-failure");
    assert!(!simulation.finish().status.success());
    assert!(simulation.read("entropy_calls").is_empty());
}

#[test]
fn composer_readiness_wait_is_interruptible() {
    let mut simulation = Simulation::start("readiness-blocked");
    simulation.wait_for("composer_starting", "yes");
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert!(simulation.read("entropy_calls").is_empty());
}

#[test]
fn entropy_is_forced_every_thirty_seconds_and_stops_on_shutdown() {
    let mut simulation = Simulation::start("periodic-entropy");
    simulation.wait_for("entropy_calls", "true");
    std::thread::sleep(Duration::from_secs(31));
    let calls = simulation.read("entropy_calls");
    let calls: Vec<Vec<&str>> = calls
        .lines()
        .map(|line| line.split_whitespace().collect())
        .collect();
    assert_eq!(calls.len(), 2, "{calls:?}");
    let elapsed = calls[1][0].parse::<f64>().unwrap() - calls[0][0].parse::<f64>().unwrap();
    assert!((30.0..31.0).contains(&elapsed), "{elapsed}");
    assert_ne!(calls[0][1], calls[1][1]);
    assert!(
        calls
            .iter()
            .all(|call| call[1].parse::<u64>().is_ok() && call[2] == "true")
    );
    // Composer owns the first_ and terminal fault pauses between entropy calls.
    assert_eq!(
        simulation
            .read("cycle_events")
            .lines()
            .filter(|line| *line == "unpause")
            .count(),
        1
    );
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert_eq!(simulation.read("entropy_calls").lines().count(), 2);
}

#[test]
fn entropy_stops_after_exit_until_next_setup_and_readiness() {
    let mut simulation = Simulation::start("entropy-restart");
    simulation.wait_for("entropy_calls", "true");
    fs::write(simulation.root().join("composer_exit"), "").unwrap();
    simulation.wait_for("recreate_started", "yes");
    std::thread::sleep(Duration::from_secs(31));
    assert_eq!(simulation.read("entropy_calls").lines().count(), 1);
    let events = simulation.read("cycle_events");
    assert!(events.contains("true\npause\nrecreate\n"), "{events}");
    assert_eq!(events.lines().filter(|line| *line == "unpause").count(), 1);
    fs::remove_file(simulation.root().join("composer_exit")).unwrap();
    let log = Path::new(&simulation.read("run_dir")).join("instrumentation.log");
    writeln!(
        fs::OpenOptions::new().append(true).open(log).unwrap(),
        "14 [workload] [JSON] '{{\"antithesis_setup\":{{\"status\":\"complete\"}}}}'"
    )
    .unwrap();
    simulation.wait_for(
        "cycle_events",
        "recreate\nunpause\ncomposer-start\ncomposer-ready\nentropy ",
    );
    assert_eq!(simulation.read("entropy_calls").lines().count(), 2);
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
}

#[test]
fn restart_wait_cancels_entropy_and_is_interruptible() {
    let mut simulation = Simulation::start("restart-blocked");
    simulation.wait_for("entropy_calls", "true");
    fs::write(simulation.root().join("composer_exit"), "").unwrap();
    simulation.wait_for("recreate_started", "yes");
    std::thread::sleep(Duration::from_secs(31));
    assert_eq!(simulation.read("entropy_calls").lines().count(), 1);
    kill(
        Pid::from_raw(simulation.child.id().cast_signed()),
        Signal::SIGTERM,
    )
    .unwrap();
    simulation.finish();
    assert_eq!(simulation.read("entropy_calls").lines().count(), 1);
}

#[test]
fn completed_composer_keeps_entropy_without_restarting_autonomous_or_single_rollouts() {
    let mut simulations = Vec::new();
    for mode in ["clean-once", "no-commands", "first-only"] {
        let mut simulation = Simulation::start(mode);
        simulation.wait_for("entropy_calls", "true");
        fs::write(simulation.root().join("composer_exit"), "").unwrap();
        simulation.wait_for("composer_completed", "yes");
        simulation.wait_for(
            "stderr",
            "Test composer completed; leaving the local Compose project running",
        );
        assert!(!simulation.root().join("recreate_started").exists());
        simulations.push(simulation);
    }
    std::thread::sleep(Duration::from_secs(31));
    for mut simulation in simulations {
        assert!(simulation.child.try_wait().unwrap().is_none());
        kill(
            Pid::from_raw(simulation.child.id().cast_signed()),
            Signal::SIGTERM,
        )
        .unwrap();
        simulation.finish();
        assert_eq!(simulation.read("entropy_calls").lines().count(), 2);
        assert!(!simulation.root().join("recreate_started").exists());
        let events = simulation.read("cycle_events");
        assert_eq!(events.lines().filter(|line| *line == "pause").count(), 1);
        assert_eq!(events.lines().filter(|line| *line == "unpause").count(), 1);
    }
}
