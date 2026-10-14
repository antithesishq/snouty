//! Runs snouty on a pseudo-terminal of a known size.

use std::process::Command;

use expectrl::Expect;
use expectrl::session::OsSession;

/// A `sh -c` command that runs `script`, with `$0` set to the snouty binary,
/// once [`spawn`] has sized the PTY.
pub(crate) fn command(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        &format!("read _ && {script}"),
        env!("CARGO_BIN_EXE_snouty"),
    ]);
    command
}

/// Spawns `command`, from [`command`], on a PTY of `cols` by `rows`.
///
/// `ptyprocess` sets the size to 80x24 from the parent after the child execs,
/// so a size the child sets itself (with `stty`) can be overwritten after
/// snouty has read it. The child therefore waits for a line on stdin, and the
/// parent sends it only after it has set the final size.
pub(crate) fn spawn(command: Command, cols: u16, rows: u16) -> OsSession {
    let mut session = OsSession::spawn(command).expect("spawn on a PTY");
    session
        .get_process_mut()
        .set_window_size(cols, rows)
        .expect("set the PTY size");
    session.send_line("").expect("let the child start");
    session
}
