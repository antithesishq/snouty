//! Generic child-process plumbing shared across snouty: running a command with
//! a wall-clock timeout, and an RAII wrapper that kills a whole process group.
//!
//! These are engine-agnostic — the container runtime and Docker Compose both
//! build on them, but nothing here knows about docker or podman.

use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Context, Result, eyre};
use tokio::process::Child;

/// Run a command to completion with a wall-clock timeout, killing it (and
/// returning an error) if it overruns. Reader threads drain stdout/stderr so a
/// chatty child can't deadlock on a full pipe while we wait. Used for the
/// synchronous discovery commands, which would otherwise be uninterruptible by
/// `--timeout` or ctrl+c since a blocking `Command` can't be interrupted.
///
/// Deliberately kills only the leader process — not the process group — so it
/// needs no `libc::kill(-pid, …)` `unsafe`, unlike [`ProcessGroupChild`]. That
/// wrapper exists for long-running commands that fork and manage a tree of
/// children which must all die on timeout. The callers here spawn one-shot
/// client invocations whose only child is the client itself: killing it closes
/// the pipes (so the reader threads finish) and the work we were waiting on ends.
pub fn output_with_timeout(mut cmd: Command, timeout: Duration) -> Result<Output> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().wrap_err("failed to spawn command")?;

    // Drain both pipes on their own threads; otherwise a child that fills a pipe
    // buffer would block on write while we block on wait — a deadlock.
    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stdout_reader = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        buf
    });
    let stderr_reader = thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        buf
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().wrap_err("failed to wait for command")? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            // The killed leader's pipe write-ends are now closed, so the reader
            // threads hit EOF and finish; join them rather than detaching them.
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(eyre!(
                "command timed out after {}s (the process may be unresponsive)",
                timeout.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(50));
    };

    let stdout = stdout_reader.join().unwrap_or_default();
    let stderr = stderr_reader.join().unwrap_or_default();
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

/// RAII wrapper around a [`Child`] spawned with `process_group(0)`.
///
/// Ensures the entire process group is killed on drop, not just the leader.
/// The inner child is `Option<Child>` so `Drop` can handle partially-consumed state.
pub struct ProcessGroupChild {
    inner: Option<Child>,
}

impl ProcessGroupChild {
    /// Wrap a freshly-spawned child that was created with `process_group(0)`.
    pub fn new(child: Child) -> Self {
        Self { inner: Some(child) }
    }

    /// Send `SIGKILL` to the entire process group, then reap the child.
    pub async fn kill_group(&mut self) -> std::io::Result<()> {
        if let Some(ref mut child) = self.inner {
            if let Some(pid) = child.id() {
                // Safety: negative PID targets the entire process group.
                unsafe {
                    libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
                }
            }
            child.wait().await?;
        }
        Ok(())
    }

    /// Delegate to the inner [`Child::wait()`].
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.inner
            .as_mut()
            .expect("ProcessGroupChild already consumed")
            .wait()
            .await
    }

    /// Delegate to the inner [`Child::id()`].
    pub fn id(&self) -> Option<u32> {
        self.inner.as_ref().and_then(|c| c.id())
    }
}

impl Drop for ProcessGroupChild {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.inner {
            if let Some(pid) = child.id() {
                // Safety: best-effort cleanup of the process group.
                unsafe {
                    libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
                }
            }
            // Best-effort synchronous reap — we can't .await in Drop.
            let _ = child.try_wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_with_timeout_returns_quick_command_output() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf hi; printf oops 1>&2; exit 3"]);
        let out = output_with_timeout(cmd, Duration::from_secs(10)).unwrap();
        assert_eq!(out.status.code(), Some(3));
        assert_eq!(out.stdout, b"hi");
        assert_eq!(out.stderr, b"oops");
    }

    #[test]
    fn output_with_timeout_kills_and_errors_on_overrun() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let err = output_with_timeout(cmd, Duration::from_millis(150)).unwrap_err();
        assert!(
            format!("{err}").contains("timed out"),
            "expected a timeout error, got: {err}"
        );
    }
}
