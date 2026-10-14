//! Runs snouty on a pseudo-terminal of a known size.

use std::ops::{Deref, DerefMut};
use std::process::Command;

use expectrl::Expect;
use expectrl::session::OsSession;

/// A `sh -c` script with `$0` set to the snouty binary and `$1`… set to the
/// added args. The script starts only after [`PtyCommand::spawn`] sizes the PTY.
pub(crate) struct PtyCommand(Command);

impl PtyCommand {
    pub(crate) fn new(script: &str) -> Self {
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            &format!("read _ || exit\n{script}"),
            env!("CARGO_BIN_EXE_snouty"),
        ]);
        Self(command)
    }

    pub(crate) fn spawn(self, cols: u16, rows: u16) -> OsSession {
        let mut session = OsSession::spawn(self.0).expect("spawn on a PTY");
        session
            .get_process_mut()
            .set_window_size(cols, rows)
            .expect("set the PTY size");
        // `ptyprocess` resets the size to 80x24 after the child execs. The
        // script waits for this line, so snouty reads only the size set above.
        session.send_line("").expect("let the script start");
        session
    }
}

impl Deref for PtyCommand {
    type Target = Command;

    fn deref(&self) -> &Command {
        &self.0
    }
}

impl DerefMut for PtyCommand {
    fn deref_mut(&mut self) -> &mut Command {
        &mut self.0
    }
}
