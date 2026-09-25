use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crate::process::output_async;
use color_eyre::eyre::{Context, Result, bail, eyre};
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{Instant, sleep, timeout};

const AUTHORIZED_KEY_FW_CFG: &str = "opt/antithesis/authorized_key";
const BOOT_CONSOLE_ESCAPE_CODES: [&[u8]; 6] = [
    b"\x1bc",
    b"\x1b[?7l",
    b"\x1b[2J",
    b"\x1b[0m",
    b"\x1b[18t",
    b"\x1b[6n",
];

#[derive(Clone, Copy)]
pub enum ShutdownSignal {
    Interrupt,
    Terminate,
}

#[derive(Clone, Copy)]
pub enum BootOutput {
    Hidden,
    Visible,
}

struct BootConsole {
    output: BootOutput,
    offset: u64,
    filter: SeaBiosConsoleFilter,
}

impl BootConsole {
    fn new(output: BootOutput) -> Self {
        Self {
            output,
            offset: 0,
            filter: SeaBiosConsoleFilter::default(),
        }
    }

    fn flush(&mut self, path: &Path) -> Result<()> {
        if matches!(self.output, BootOutput::Hidden) {
            return Ok(());
        }
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        file.seek(SeekFrom::Start(self.offset))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        self.offset += bytes.len() as u64;
        self.filter.write(&bytes, &mut io::stderr().lock())?;
        io::stderr().flush()?;
        Ok(())
    }
}

#[derive(Default)]
struct SeaBiosConsoleFilter {
    pending: Vec<u8>,
}

impl SeaBiosConsoleFilter {
    fn write(&mut self, bytes: &[u8], output: &mut impl Write) -> io::Result<()> {
        self.pending.extend_from_slice(bytes);
        let mut clean = Vec::with_capacity(self.pending.len());
        let mut consumed = 0;
        while consumed < self.pending.len() {
            let remaining = &self.pending[consumed..];
            if let Some(code) = BOOT_CONSOLE_ESCAPE_CODES
                .iter()
                .find(|code| remaining.starts_with(code))
            {
                consumed += code.len();
            } else if BOOT_CONSOLE_ESCAPE_CODES
                .iter()
                .any(|code| code.starts_with(remaining))
            {
                break;
            } else {
                clean.push(self.pending[consumed]);
                consumed += 1;
            }
        }
        self.pending.drain(..consumed);
        output.write_all(&clean)
    }

    fn finish(&mut self) {
        self.pending.clear();
    }
}

pub struct Vm {
    child: Child,
    client_key: Option<ClientKey>,
    ssh_config: PathBuf,
    run_dir: PathBuf,
    instrumentation: PathBuf,
}

pub struct GuestConnection<'a> {
    run_dir: &'a Path,
}

impl<'a> GuestConnection<'a> {
    pub fn new(run_dir: &'a Path) -> Self {
        Self { run_dir }
    }

    fn shell_command(&self) -> Command {
        let mut command = Command::new("ssh");
        command
            .current_dir(self.run_dir)
            .arg("-F")
            .arg(self.run_dir.join("ssh_config"))
            .args(["-o", "LogLevel=ERROR", "-tt", "guest_vm"])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        command
    }

    pub async fn interactive_shell(&self) -> Result<std::process::ExitStatus> {
        let status = tokio::process::Command::from(self.shell_command())
            .kill_on_drop(true)
            .status()
            .await
            .wrap_err("failed to open guest shell")?;
        Ok(status)
    }

    pub fn exec_interactive_shell(&self) -> Result<std::process::ExitCode> {
        Err(self.shell_command().exec()).wrap_err("failed to open guest shell")
    }
}

impl Vm {
    pub async fn boot(
        iso: &Path,
        run_dir: &Path,
        startup_timeout: Duration,
        boot_output: BootOutput,
        memory_mib: u64,
    ) -> Result<Self> {
        let run_dir = fs::canonicalize(run_dir)?;
        let accelerated = kvm_available().await;
        if !accelerated {
            eprintln!("Warning: KVM is not accessible; simulation will run more slowly.");
        }
        let deadline = Instant::now()
            .checked_add(startup_timeout)
            .ok_or_else(|| eyre!("guest startup timeout is too large"))?;
        for attempt in 0..5 {
            let client_key = ClientKey::generate(&run_dir).await?;
            let listener = TcpListener::bind(("127.0.0.1", 0))?;
            let port = listener.local_addr()?.port();
            let ssh_config = run_dir.join("ssh_config");
            fs::write(
                &ssh_config,
                format!(
                    "Host guest_vm\n HostName 127.0.0.1\n Port {port}\n User root\n IdentityFile {}\n IdentitiesOnly yes\n IdentityAgent none\n UserKnownHostsFile /dev/null\n GlobalKnownHostsFile /dev/null\n StrictHostKeyChecking no\n UpdateHostKeys no\n LogLevel ERROR\n BatchMode yes\n ConnectTimeout 2\n ServerAliveInterval 5\n ServerAliveCountMax 3\n Compression no\n ControlMaster no\n ControlPath none\n",
                    client_key.identity_file.display()
                ),
            )?;
            let mut command = Command::new("qemu-system-x86_64");
            command
                .current_dir(&run_dir)
                .args([
                    "-accel",
                    if accelerated { "kvm" } else { "tcg" },
                    "-cpu",
                    if accelerated { "host" } else { "max" },
                    "-machine",
                    "q35,i8042=off",
                    "-nodefaults",
                    "-smp",
                    "1",
                    "-m",
                ])
                .arg(memory_mib.to_string())
                .args(["-boot", "d", "-cdrom"])
                .arg(iso)
                .args([
                    "-display",
                    "none",
                    "-vga",
                    "none",
                    "-monitor",
                    "none",
                    "-serial",
                    "file:boot.log",
                    "-serial",
                    "file:instrumentation.log",
                    "-fw_cfg",
                ])
                .arg(format!(
                    "name={AUTHORIZED_KEY_FW_CFG},file={}",
                    client_key.public_key.display()
                ))
                .args(["-device", "virtio-net-pci,netdev=net0,addr=3", "-netdev"])
                .arg(format!(
                    "user,id=net0,hostfwd=tcp:127.0.0.1:{port}-:22,restrict=yes"
                ))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(File::create(run_dir.join("qemu.log"))?)
                .process_group(0);
            drop(listener);
            let mut vm = Self {
                child: command.spawn().wrap_err("failed to start QEMU")?,
                client_key: Some(client_key),
                ssh_config,
                instrumentation: run_dir.join("instrumentation.log"),
                run_dir: run_dir.clone(),
            };
            let mut boot_console = BootConsole::new(boot_output);
            let result = timeout(
                deadline.saturating_duration_since(Instant::now()),
                vm.wait_ready(&mut boot_console),
            )
            .await;
            boot_console.flush(&vm.run_dir.join("boot.log"))?;
            boot_console.filter.finish();
            match result {
                Ok(Ok(())) => return Ok(vm),
                Ok(Err(error)) => {
                    if attempt < 4
                        && !vm.is_running()?
                        && TcpListener::bind(("127.0.0.1", port))
                            .is_err_and(|error| error.kind() == std::io::ErrorKind::AddrInUse)
                    {
                        continue;
                    }
                    return Err(error).wrap_err(vm.boot_diagnostics());
                }
                Err(_) => bail!(
                    "guest startup timed out after {} seconds\n{}",
                    startup_timeout.as_secs(),
                    vm.boot_diagnostics()
                ),
            }
        }
        unreachable!("the final startup attempt returns its error")
    }

    async fn wait_ready(&mut self, boot_console: &mut BootConsole) -> Result<()> {
        loop {
            boot_console.flush(&self.run_dir.join("boot.log"))?;
            if !self.is_running()? {
                bail!("QEMU exited before SSH became ready");
            }
            let mut command = self.ssh();
            command.arg("true");
            let output = match output_async(command, Duration::from_secs(5)).await {
                Ok(output) => output,
                Err(error) => {
                    log::debug!("guest SSH readiness check failed: {error:#}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            if output.status.success() {
                boot_console.flush(&self.run_dir.join("boot.log"))?;
                return Ok(());
            }
            let error = String::from_utf8_lossy(&output.stderr);
            if error.contains("Permission denied") || error.contains("no such identity") {
                bail!("guest SSH authentication failed: {}", error.trim());
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    fn ssh_base(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("ssh");
        command
            .current_dir(&self.run_dir)
            .arg("-F")
            .arg(&self.ssh_config);
        command
    }

    fn ssh(&self) -> tokio::process::Command {
        let mut command = self.ssh_base();
        command
            .arg("guest_vm")
            .stdin(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        command
    }

    pub async fn interactive_shell(&self) -> Result<std::process::ExitStatus> {
        GuestConnection::new(&self.run_dir)
            .interactive_shell()
            .await
    }

    pub async fn run_script(&self, script: &str) -> Result<String> {
        let mut input = tempfile::tempfile()?;
        std::io::Write::write_all(&mut input, script.as_bytes())?;
        input.seek(SeekFrom::Start(0))?;
        let mut command = self.ssh();
        command.arg("bash -s").stdin(input);
        let output = output_async(command, Duration::from_secs(120)).await?;
        if !output.status.success() {
            bail!(
                "guest command failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).wrap_err("guest command returned invalid UTF-8")
    }

    pub async fn upload_config(&self, directory: &Path) -> Result<()> {
        let archive = tempfile::NamedTempFile::new_in(&self.run_dir)?;
        let mut tar = tokio::process::Command::new("tar");
        tar.args(["--dereference", "--create", "--file"])
            .arg(archive.path())
            .arg("--directory")
            .arg(directory)
            .arg(".")
            .stdin(Stdio::null());
        let output = output_async(tar, Duration::from_secs(120)).await?;
        if !output.status.success() {
            bail!(
                "config archive failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let mut ssh = self.ssh();
        ssh.arg("tar --extract --file=- --directory=/opt/config")
            .stdin(File::open(archive.path())?);
        let output = output_async(ssh, Duration::from_secs(120)).await?;
        if !output.status.success() {
            bail!(
                "config upload failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub async fn load_images(&self, archive: &Path) -> Result<()> {
        let mut command = self.ssh();
        command.arg("podman load").stdin(File::open(archive)?);
        let output = output_async(command, Duration::from_secs(1800)).await?;
        if !output.status.success() {
            bail!(
                "guest image import failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub fn instrumentation_log(&self) -> &Path {
        &self.instrumentation
    }

    pub fn is_running(&mut self) -> Result<bool> {
        let running = self.child.try_wait()?.is_none();
        if !running {
            self.client_key.take();
        }
        Ok(running)
    }

    pub async fn wait_for_poweroff(&mut self, grace: Duration) -> Result<bool> {
        let deadline = Instant::now() + grace;
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.client_key.take();
                return Ok(status.success());
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn boot_diagnostics(&self) -> String {
        let mut result = format!("Guest logs: {}", self.run_dir.display());
        for name in ["qemu.log", "boot.log"] {
            if let Ok(mut file) = File::open(self.run_dir.join(name)) {
                let _ = file
                    .seek(SeekFrom::End(-4096))
                    .or_else(|_| file.seek(SeekFrom::Start(0)));
                let mut bytes = Vec::new();
                if file.read_to_end(&mut bytes).is_ok() {
                    result.push_str(&format!("\n{name}:\n{}", String::from_utf8_lossy(&bytes)));
                }
            }
        }
        result
    }

    pub async fn shutdown(&mut self, signal: ShutdownSignal) -> Result<()> {
        if !self.is_running()? {
            return Ok(());
        }
        let signal = match signal {
            ShutdownSignal::Interrupt => Signal::SIGINT,
            ShutdownSignal::Terminate => Signal::SIGTERM,
        };
        killpg(Pid::from_raw(self.child.id() as i32), signal)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.is_running()? {
            if Instant::now() >= deadline {
                killpg(Pid::from_raw(self.child.id() as i32), Signal::SIGKILL)?;
                self.child.wait()?;
                self.client_key.take();
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

struct ClientKey {
    _directory: tempfile::TempDir,
    identity_file: PathBuf,
    public_key: PathBuf,
}

impl ClientKey {
    async fn generate(run_dir: &Path) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("ssh-key-")
            .tempdir_in(run_dir)
            .wrap_err("failed to create temporary SSH key directory")?;
        let private_key = directory.path().join("id_ed25519");
        let mut command = tokio::process::Command::new("ssh-keygen");
        command
            .args(["-q", "-N", "", "-t", "ed25519", "-f"])
            .arg(&private_key)
            .stdin(Stdio::null());
        let output = output_async(command, Duration::from_secs(10))
            .await
            .wrap_err("failed to generate temporary SSH key")?;
        if !output.status.success() {
            bail!(
                "failed to generate temporary SSH key: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let identity_file = private_key;
        let public_key = directory.path().join("id_ed25519.pub");
        Ok(Self {
            _directory: directory,
            identity_file,
            public_key,
        })
    }
}

impl Drop for Vm {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = killpg(Pid::from_raw(self.child.id() as i32), Signal::SIGKILL);
        let _ = self.child.wait();
    }
}

async fn kvm_available() -> bool {
    if OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .is_err()
    {
        return false;
    }
    let input = (|| -> std::io::Result<File> {
        let mut input = tempfile::tempfile()?;
        std::io::Write::write_all(
            &mut input,
            b"{\"execute\":\"qmp_capabilities\"}\n{\"execute\":\"quit\"}\n",
        )?;
        input.seek(SeekFrom::Start(0))?;
        Ok(input)
    })();
    let Ok(input) = input else {
        return false;
    };
    let mut command = tokio::process::Command::new("qemu-system-x86_64");
    command
        .args([
            "-accel",
            "kvm",
            "-machine",
            "none",
            "-display",
            "none",
            "-nodefaults",
            "-qmp",
            "stdio",
        ])
        .stdin(input);
    match output_async(command, Duration::from_secs(5)).await {
        Ok(output) if output.status.success() => std::str::from_utf8(&output.stdout)
            .ok()
            .and_then(|text| text.lines().next())
            .is_some_and(|line| serde_json::from_str::<QmpGreeting>(line).is_ok()),
        _ => false,
    }
}

#[derive(Deserialize)]
struct QmpGreeting {
    #[serde(rename = "QMP")]
    _qmp: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    #[test]
    fn seabios_console_filter_removes_screen_control_codes() {
        let mut output = Vec::new();
        let mut filter = SeaBiosConsoleFilter::default();
        filter
            .write(
                b"one\x1bc two\x1b[?7l three\x1b[2J four\x1b[0m five\x1b[18t six\x1b[6n seven",
                &mut output,
            )
            .unwrap();
        assert_eq!(output, b"one two three four five six seven");
    }

    #[hegel::test]
    fn seabios_console_filter_is_independent_of_chunk_boundaries(tc: hegel::TestCase) {
        let prefix = tc.draw(generators::binary());
        let suffix = tc.draw(generators::binary());
        let code = tc.draw(generators::sampled_from(
            BOOT_CONSOLE_ESCAPE_CODES.map(<[u8]>::to_vec).to_vec(),
        ));
        let split = tc.draw(generators::integers::<usize>().max_value(code.len()));
        let input: Vec<_> = prefix.iter().chain(&code).chain(&suffix).copied().collect();
        let mut whole = Vec::new();
        let mut whole_filter = SeaBiosConsoleFilter::default();
        whole_filter.write(&input, &mut whole).unwrap();
        whole_filter.finish();
        let mut chunked = Vec::new();
        let mut filter = SeaBiosConsoleFilter::default();
        filter.write(&prefix, &mut chunked).unwrap();
        filter.write(&code[..split], &mut chunked).unwrap();
        filter.write(&code[split..], &mut chunked).unwrap();
        filter.write(&suffix, &mut chunked).unwrap();
        filter.finish();
        assert_eq!(chunked, whole);
    }

    #[test]
    fn dropping_vm_kills_and_reaps_its_child() {
        let run_dir = tempfile::tempdir().unwrap();
        let child = Command::new("sleep")
            .arg("60")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = Pid::from_raw(child.id() as i32);
        let vm = Vm {
            child,
            client_key: Some(ClientKey {
                _directory: tempfile::tempdir_in(run_dir.path()).unwrap(),
                identity_file: PathBuf::new(),
                public_key: PathBuf::new(),
            }),
            ssh_config: run_dir.path().join("ssh_config"),
            run_dir: run_dir.path().to_path_buf(),
            instrumentation: run_dir.path().join("instrumentation.log"),
        };
        drop(vm);
        assert_eq!(
            nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)),
            Err(nix::errno::Errno::ECHILD)
        );
    }

    #[test]
    fn reaping_qemu_removes_its_client_key_directory() {
        let run_dir = tempfile::tempdir().unwrap();
        let key_directory = tempfile::tempdir_in(run_dir.path()).unwrap();
        let key_path = key_directory.path().to_path_buf();
        let mut child = Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        let mut vm = Vm {
            child,
            client_key: Some(ClientKey {
                _directory: key_directory,
                identity_file: PathBuf::new(),
                public_key: PathBuf::new(),
            }),
            ssh_config: run_dir.path().join("ssh_config"),
            run_dir: run_dir.path().to_path_buf(),
            instrumentation: run_dir.path().join("instrumentation.log"),
        };
        assert!(!vm.is_running().unwrap());
        assert!(!key_path.exists());
    }
}
