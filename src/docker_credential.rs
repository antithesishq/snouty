//! `snouty docker-credential`: the Docker credential helper protocol
//! (<https://github.com/docker/docker-credential-helpers>) for the tenant
//! registry. Docker and Podman run [`HELPER_NAME`], an alias of `snouty`, which
//! runs `snouty docker-credential`.

use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use color_eyre::eyre::{Context, Report, Result};
use serde::Serialize;

use crate::api::AntithesisApi;
use crate::cli::DockerCredentialAction;
use crate::error::{print_report, user_error};
use crate::registry::{self, RegistryHost};
use crate::settings::{DefaultProjectFile, Settings};
use crate::stdio;

/// No profile uses the requested registry as its tenant registry.
#[derive(Debug)]
struct CredentialsNotFound;

impl std::fmt::Display for CredentialsNotFound {
    /// The exact message that Docker and Podman read as "this helper has no
    /// credential for the server", rather than as a failure.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("credentials not found in native keychain")
    }
}

impl std::error::Error for CredentialsNotFound {}

/// The value of a `"credHelpers"` entry that selects this helper.
pub const CRED_HELPER: &str = "snouty";

/// The name that Docker and Podman run for a [`CRED_HELPER`] entry. `snouty`
/// runs `snouty docker-credential` when it starts by this name.
pub const HELPER_NAME: &str = "docker-credential-snouty";

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Credential<'a> {
    #[serde(rename = "ServerURL")]
    server_url: &'a str,
    username: &'a str,
    secret: &'a str,
}

/// Runs one action of the protocol, and returns the exit status. `active` is
/// the settings of the selected profile, or `None` when they fail to resolve;
/// the search skips such a profile. `settings_path` is the `--settings` path,
/// which finds the other profiles. Neither reads a `./.snouty.toml`.
///
/// The protocol reads an error from stdout. Docker reads "not found" as an
/// answer, so that error does not go to stderr. A closed stdout fails the
/// action, and never panics.
pub async fn run(
    action: DockerCredentialAction,
    active: Option<Settings>,
    settings_path: Option<PathBuf>,
) -> ExitCode {
    let Err(report) = cmd_docker_credential(action, active, settings_path).await else {
        return ExitCode::SUCCESS;
    };
    // The exit status fails the action, and each error but "not found" also
    // goes to stderr, so a failed write here loses nothing.
    let _ = writeln!(stdio::stdout(), "{report}");
    if !report.is::<CredentialsNotFound>() {
        print_report(&report);
    }
    ExitCode::FAILURE
}

async fn cmd_docker_credential(
    action: DockerCredentialAction,
    active: Option<Settings>,
    project_settings_path: Option<PathBuf>,
) -> Result<()> {
    match action {
        DockerCredentialAction::Get => {
            let server_url = read_stdin()?;
            let server_url = server_url.trim();
            let host: RegistryHost = server_url
                .parse()
                .map_err(|err| user_error(format!("invalid server URL on stdin: {err}")))?;
            let every =
                || Settings::resolve_every_profile(project_settings_path, DefaultProjectFile::None);
            let Some(settings) = registry::select_profile(active, every, &host)? else {
                return Err(Report::new(CredentialsNotFound));
            };
            let secret = AntithesisApi::new(&settings, false)?
                .registry_secret()
                .await?;
            let credential = serde_json::to_string(&Credential {
                server_url,
                username: registry::USERNAME,
                secret: &secret,
            })?;
            writeln!(stdio::stdout(), "{credential}").wrap_err("failed to write the credential")?;
            Ok(())
        }
        // Docker and Podman call store only for a login. Snouty owns the
        // credential, so store fails and tells why.
        DockerCredentialAction::Store => Err(user_error(
            "snouty gives the credential for this registry, so a login is not necessary; to sign in, run `snouty login` or set ANTITHESIS_API_KEY",
        )),
        // Erase changes nothing, but it must succeed: when it fails,
        // `podman logout --all` keeps the credentials of every registry, and
        // podman 4.9 crashes on `podman logout <registry>`.
        DockerCredentialAction::Erase => {
            read_stdin()?;
            Ok(())
        }
        // Neither engine calls list for a "credHelpers" entry.
        DockerCredentialAction::List => Err(user_error(
            "list is not necessary: Docker and Podman do not call it for a \"credHelpers\" entry",
        )),
    }
}

fn read_stdin() -> Result<String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .wrap_err("failed to read stdin")?;
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_name_is_the_engine_name_for_cred_helper() {
        assert_eq!(HELPER_NAME, format!("docker-credential-{CRED_HELPER}"));
    }
}
