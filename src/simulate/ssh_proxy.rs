use std::path::Path;

use color_eyre::eyre::{Context, Result, bail};

pub fn pass_ssh_socket(path: &Path) -> Result<()> {
    use std::io::{IoSlice, stdout};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};

    let socket = UnixStream::connect(path).wrap_err("failed to connect to guest SSH socket")?;
    sendmsg::<()>(
        stdout().as_raw_fd(),
        &[IoSlice::new(&[0])],
        &[ControlMessage::ScmRights(&[socket.as_raw_fd()])],
        MsgFlags::empty(),
        None,
    )
    .wrap_err("failed to pass guest SSH socket to OpenSSH")?;
    Ok(())
}

pub(super) fn proxy_command(executable: &Path, socket: &Path) -> Result<String> {
    let mut quoted = Vec::new();
    for path in [executable, socket] {
        let Some(path) = path.to_str() else {
            bail!("SSH proxy paths must be valid UTF-8");
        };
        if path.contains(['\n', '\r']) {
            bail!("SSH proxy paths must not contain newlines");
        }
        // OpenSSH expands percent tokens before the shell reads the command.
        quoted.push(format!(
            "'{}'",
            path.replace('%', "%%").replace('\'', "'\\''")
        ));
    }
    Ok(format!("{} internal-ssh-proxy {}", quoted[0], quoted[1]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[hegel::test]
    fn proxy_command_preserves_shell_arguments(tc: hegel::TestCase) {
        let suffix = tc.draw(hegel::generators::text());
        let path = format!("/tmp/space ' \" $HOME `echo bad` %h \\ {suffix}");
        let command = proxy_command(Path::new("/bin/printf"), Path::new(&path));
        if path.contains(['\n', '\r']) {
            assert!(command.is_err());
            return;
        }
        if path.contains('\0') {
            return;
        }
        let command = command.unwrap().replace("%%", "%");
        // Substitute printf's format for the internal subcommand to inspect argv.
        let command = command.replacen(" internal-ssh-proxy ", " '%s' ", 1);
        let output = std::process::Command::new("sh")
            .args(["-c", &command])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, path.as_bytes());
    }
}
