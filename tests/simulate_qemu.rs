//! `snouty simulate` against real QEMU and a real simulation environment ISO.
//!
//! `SNOUTY_TEST_ENVIRONMENT_ISO` names the environment ISO. `mise run
//! test:qemu` builds the toy environment in tests/environment and sets it. Any
//! ISO that implements the contract in tests/environment/init.sh works, such as
//! the `/guest.iso` of the simulation environment image. These tests are the
//! `qemu` group in .config/nextest.toml.
#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use expectrl::process::unix::WaitStatus;
use expectrl::session::OsSession;
use expectrl::{Eof, Expect, Regex};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use snouty::testutils::{skip_or_fail, write_script};
use tempfile::TempDir;

const ENVIRONMENT_ISO_ENV: &str = "SNOUTY_TEST_ENVIRONMENT_ISO";
const SHELL_OPENED: &str = "Opening a shell in the simulation environment.";
const MEMORY_MIB: u64 = 2048;

fn qemu_available() -> bool {
    if which::which("qemu-system-x86_64").is_ok() {
        return true;
    }
    skip_or_fail("qemu-system-x86_64 is not available");
    false
}

fn environment_iso() -> Option<PathBuf> {
    if !qemu_available() {
        return None;
    }
    let iso = std::env::var_os(ENVIRONMENT_ISO_ENV)
        .filter(|iso| !iso.is_empty())
        .map(PathBuf::from);
    if iso.is_none() {
        skip_or_fail(&format!(
            "{ENVIRONMENT_ISO_ENV} is not set; run `mise run test:qemu`"
        ));
    }
    iso
}

/// `snouty simulate --shell` on a PTY, in a private HOME and TMPDIR.
struct Simulation {
    directory: TempDir,
    session: OsSession,
}

impl Simulation {
    fn shell(iso: &Path, timeout: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        fs::create_dir_all(root.join("home")).unwrap();
        fs::create_dir_all(root.join("tmp")).unwrap();
        // A local ISO needs no container engine, so any engine call fails.
        for engine in ["podman", "docker"] {
            write_script(
                &bin.join(engine),
                "echo 'no container engine' >&2; exit 1\n",
            );
        }
        let mut paths = vec![bin];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let mut command = Command::new(assert_cmd::cargo::cargo_bin!("snouty"));
        command
            .env_clear()
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("HOME", root.join("home"))
            .env("XDG_CONFIG_HOME", root.join("settings"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("TMPDIR", root.join("tmp"))
            .env("TERM", "xterm-256color")
            .env("SNOUTY_UNSTABLE_FEATURES", "simulate")
            .env("SNOUTY_SIMULATE_GUEST_ISO", iso)
            .current_dir(&root)
            .args(["simulate", "--shell", "--memory"])
            .arg(MEMORY_MIB.to_string())
            .args(["--timeout", timeout]);
        let mut session = OsSession::spawn(command).expect("spawn snouty on a PTY");
        // Boot under TCG on a slow runner can take a minute.
        session.set_expect_timeout(Some(Duration::from_secs(180)));
        Self { directory, session }
    }

    fn root(&self) -> PathBuf {
        fs::canonicalize(self.directory.path()).unwrap()
    }

    fn signal(&self, signal: Signal) {
        kill(
            Pid::from_raw(self.session.get_process().pid().as_raw()),
            signal,
        )
        .unwrap();
    }

    /// Waits for the shell prompt, and returns the running environment.
    fn open_shell(&mut self) -> (String, Environment) {
        let boot = self.session.expect(SHELL_OPENED).expect("shell opens");
        let boot = String::from_utf8_lossy(boot.before()).into_owned();
        let environment = Environment::find(&self.root().join("tmp"));
        (boot, environment)
    }

    /// Runs a command in the environment. Its output must print `expected`,
    /// which the echo of the command must not contain.
    fn run(&mut self, command: &str, expected: &str) {
        self.session.send_line(command).unwrap();
        self.session
            .expect(expected)
            .unwrap_or_else(|error| panic!("{command}: {error}"));
    }

    fn finish(&mut self) -> (i32, String) {
        let output = self.session.expect(Eof).expect("snouty exits");
        let output = String::from_utf8_lossy(output.as_bytes()).into_owned();
        match self.session.get_process().wait().unwrap() {
            WaitStatus::Exited(_, code) => (code, output),
            status => panic!("snouty did not exit: {status:?}\n{output}"),
        }
    }
}

/// The QEMU process of a running simulation and the paths that it owns.
struct Environment {
    run_dir: PathBuf,
    qemu: Pid,
}

impl Environment {
    /// Finds the QEMU process whose working directory is the run directory
    /// in `tmp`.
    fn find(tmp: &Path) -> Self {
        let run_dir = fs::read_dir(tmp)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("snouty-simulate-")
            })
            .expect("run directory exists");
        let qemu = qemu_processes(&run_dir)
            .pop()
            .expect("QEMU runs in the run directory");
        Self { run_dir, qemu }
    }

    fn public_key(&self) -> String {
        let key_directory = fs::read_dir(&self.run_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("ssh-key-")
            })
            .expect("run directory has the SSH key");
        fs::read_to_string(key_directory.join("id_ed25519.pub")).unwrap()
    }

    /// Asserts that QEMU has exited and that its runtime files are gone.
    fn assert_stopped(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while kill(self.qemu, None).is_ok() {
            assert!(Instant::now() < deadline, "QEMU {} survived", self.qemu);
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(!self.run_dir.exists(), "{}", self.run_dir.display());
    }
}

impl Drop for Environment {
    /// Stops QEMU when a test fails before snouty stops it.
    fn drop(&mut self) {
        let _ = kill(self.qemu, Signal::SIGKILL);
    }
}

/// The QEMU processes that run in `run_dir`. A packaged QEMU can be a
/// wrapper that runs `.qemu-system-x86_64-wrapped`.
fn qemu_processes(run_dir: &Path) -> Vec<Pid> {
    fs::read_dir("/proc")
        .unwrap()
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
        .filter(|pid| {
            fs::read_to_string(format!("/proc/{pid}/comm"))
                .is_ok_and(|name| name.contains("qemu-system"))
                && fs::read_link(format!("/proc/{pid}/cwd")).is_ok_and(|cwd| cwd == run_dir)
        })
        .map(Pid::from_raw)
        .collect()
}

#[test]
fn shell_shows_boot_console_runs_commands_and_poweroff_stops_qemu() {
    let Some(iso) = environment_iso() else { return };
    let mut simulation = Simulation::shell(&iso, "3m");
    let (boot, environment) = simulation.open_shell();
    let boot_log = fs::read_to_string(environment.run_dir.join("boot.log")).unwrap();
    let line = boot_log
        .lines()
        .map(str::trim)
        .find(|line| {
            line.len() > 10
                && line
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        })
        .expect("boot console has a line of text");
    assert!(boot.contains(line), "{line:?} missing from\n{boot}");
    simulation.run("echo snouty-$((6 * 7))", "snouty-42");
    simulation.session.send_line("poweroff").unwrap();
    // snouty exits zero only if QEMU stops within its poweroff grace period.
    let (code, output) = simulation.finish();
    assert_eq!(code, 0, "{output}");
    assert!(output.contains("Stopping simulation..."), "{output}");
    environment.assert_stopped();
    // The SSH client keeps no state in HOME.
    assert!(!simulation.root().join("home/.ssh").exists());
}

#[test]
fn environment_receives_the_key_inject_drive_memory_and_ssh_commands() {
    let Some(iso) = environment_iso() else { return };
    let mut simulation = Simulation::shell(&iso, "3m");
    let (_, environment) = simulation.open_shell();
    let public_key = environment.public_key();
    let key = public_key.split_whitespace().nth(1).unwrap();
    simulation.run(
        "cat /sys/firmware/qemu_fw_cfg/by_name/opt/antithesis/authorized_key/raw",
        key,
    );

    let drive = "/dev/disk/by-id/virtio-antithesis-inject";
    simulation.run(
        &format!("echo read-only=$(cat /sys/class/block/$(basename $(readlink -f {drive}))/ro)"),
        "read-only=1",
    );
    // `--shell` attaches an empty image store.
    simulation.run(
        &format!(
            "d=$(mktemp -d) && mount -t erofs -o ro {drive} $d && echo erofs=$(ls $d) && umount $d"
        ),
        "erofs=containers",
    );

    simulation
        .session
        .send_line("echo memory=$(($(sed -n 's/^MemTotal: *\\([0-9]*\\) kB/\\1/p' /proc/meminfo) / 1024))MiB")
        .unwrap();
    let memory = simulation
        .session
        .expect(Regex("memory=[0-9]+MiB"))
        .unwrap();
    let memory: u64 = String::from_utf8_lossy(&memory[0])
        .trim_start_matches("memory=")
        .trim_end_matches("MiB")
        .parse()
        .unwrap();
    assert!(
        (MEMORY_MIB * 3 / 4..=MEMORY_MIB).contains(&memory),
        "{memory} MiB"
    );

    // snouty runs scripts with `bash -s`, and uploads with this tar command.
    simulation.run("echo 'echo bash-$((40 + 2))' | bash -s", "bash-42");
    simulation.run(
        "mkdir -p /opt/config && tar -cf - -C /etc passwd | tar --extract --file=- --directory=/opt/config && echo tar-$((40 + 2))",
        "tar-42",
    );

    simulation.session.send_line("exit").unwrap();
    let (code, output) = simulation.finish();
    assert_eq!(code, 0, "{output}");
    environment.assert_stopped();
}

#[test]
fn terminating_the_shell_powers_off_the_environment_and_cleans_up() {
    let Some(iso) = environment_iso() else { return };
    let mut simulation = Simulation::shell(&iso, "3m");
    let (_, environment) = simulation.open_shell();
    simulation.signal(Signal::SIGTERM);
    let (code, output) = simulation.finish();
    assert_eq!(code, 143, "{output}");
    assert!(output.contains("Stopping simulation..."), "{output}");
    environment.assert_stopped();
}

#[test]
fn interrupting_boot_stops_qemu_and_cleans_up() {
    let Some(iso) = environment_iso() else { return };
    let mut simulation = Simulation::shell(&iso, "3m");
    simulation
        .session
        .expect("Booting simulation environment...")
        .unwrap();
    let tmp = simulation.root().join("tmp");
    let deadline = Instant::now() + Duration::from_secs(30);
    let environment = loop {
        let started = fs::read_dir(&tmp)
            .unwrap()
            .any(|entry| !qemu_processes(&entry.unwrap().path()).is_empty());
        if started {
            break Environment::find(&tmp);
        }
        assert!(Instant::now() < deadline, "QEMU did not start");
        std::thread::sleep(Duration::from_millis(20));
    };
    simulation.signal(Signal::SIGINT);
    let (code, output) = simulation.finish();
    assert_eq!(code, 130, "{output}");
    assert!(!output.contains(SHELL_OPENED), "{output}");
    environment.assert_stopped();
}

/// The error names the run directory, which keeps the logs.
fn logs_directory(output: &str) -> PathBuf {
    let line = output
        .lines()
        .find_map(|line| line.split_once("Simulation environment logs: "))
        .unwrap_or_else(|| panic!("no logs directory in\n{output}"))
        .1;
    PathBuf::from(line.trim())
}

#[test]
fn unbootable_iso_times_out_and_keeps_boot_logs() {
    if !qemu_available() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let iso = directory.path().join("blank.iso");
    fs::write(&iso, vec![0; 1 << 20]).unwrap();
    let mut simulation = Simulation::shell(&iso, "3s");
    let tmp = simulation.root().join("tmp");
    let (code, output) = simulation.finish();
    assert_eq!(code, 1, "{output}");
    assert!(
        output.contains("startup timed out waiting for SSH readiness marker"),
        "{output}"
    );
    let logs = logs_directory(&output);
    assert!(logs.starts_with(&tmp), "{}", logs.display());
    for name in ["boot.log", "qemu.log"] {
        assert!(logs.join(name).is_file(), "{name}");
    }
    assert!(
        qemu_processes(&logs).is_empty(),
        "QEMU survived the timeout"
    );
}

#[test]
fn qemu_failure_reports_the_qemu_log() {
    if !qemu_available() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let iso = directory.path().join("unreadable.iso");
    fs::write(&iso, "").unwrap();
    fs::set_permissions(&iso, fs::Permissions::from_mode(0o000)).unwrap();
    let mut simulation = Simulation::shell(&iso, "3m");
    let (code, output) = simulation.finish();
    assert_eq!(code, 1, "{output}");
    assert!(
        output.contains("QEMU exited before SSH became ready"),
        "{output}"
    );
    assert!(output.contains("qemu.log:"), "{output}");
    assert!(output.contains("Permission denied"), "{output}");
    assert!(logs_directory(&output).join("qemu.log").is_file());
}
