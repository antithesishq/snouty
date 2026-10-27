//! `docker-credential-snouty`: the name Docker gives the snouty credential
//! helper (`"credHelpers": {"<registry>": "snouty"}`). It runs
//! `snouty docker-credential <args>`, with the same stdin, stdout, stderr and
//! exit status.
//!
//! It uses no part of the snouty library, so it stays small and starts fast.

use std::env;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    // The installers put `snouty` in the same directory as this binary.
    let name = format!("snouty{}", env::consts::EXE_SUFFIX);
    let snouty = env::current_exe()
        .ok()
        .and_then(|exe| Some(exe.parent()?.join(&name)))
        .filter(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from(&name));

    let mut command = Command::new(&snouty);
    command
        .arg("docker-credential")
        .args(env::args_os().skip(1));

    #[cfg(unix)]
    let err = {
        use std::os::unix::process::CommandExt;
        // `exec` returns only when it fails.
        command.exec()
    };
    #[cfg(not(unix))]
    let err = match command.status() {
        Ok(status) => {
            return status
                .code()
                .and_then(|code| u8::try_from(code).ok())
                .map_or(ExitCode::FAILURE, ExitCode::from);
        }
        Err(err) => err,
    };

    // The protocol reads an error from stdout.
    println!("failed to run {}: {err}", snouty.display());
    ExitCode::FAILURE
}
