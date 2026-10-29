//! `snouty docker-credential`: the Docker credential helper protocol
//! (<https://github.com/docker/docker-credential-helpers>) for the tenant
//! registry. Docker and Podman run [`HELPER_NAME`], an alias of `snouty`, which
//! runs `snouty docker-credential`.

use std::io::{self, Read, Write};
use std::path::PathBuf;

use color_eyre::eyre::{Context, Report, Result, eyre};
use serde::Serialize;

use crate::api::AntithesisApi;
use crate::cli::DockerCredentialAction;
use crate::error::user_error;
use crate::registry::{self, TenantRegistry, TenantRegistryError};
use crate::stdio;

/// The requested registry is not a tenant registry, or no profile uses it as
/// its tenant registry.
#[derive(Debug)]
pub struct CredentialsNotFound;

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

/// Runs one action of the protocol. `settings_path` and `profile` are the
/// `--settings` and `--profile` flags: [`registry::select_settings`] tells
/// from them, and from their environment variables, which settings give the
/// credential.
///
/// The protocol reads an error from stdout, so an error prints its message
/// there before it returns. The caller prints the full report on stderr,
/// except for [`CredentialsNotFound`]: Docker reads "not found" as an answer.
pub async fn run(
    action: DockerCredentialAction,
    settings_path: Option<PathBuf>,
    profile: Option<String>,
) -> Result<()> {
    let result = cmd_docker_credential(action, settings_path, profile).await;
    if let Err(report) = &result {
        // The returned error fails the action, so a failed write loses nothing.
        let _ = writeln!(stdio::stdout(), "{report}");
    }
    result
}

async fn cmd_docker_credential(
    action: DockerCredentialAction,
    settings_path: Option<PathBuf>,
    profile: Option<String>,
) -> Result<()> {
    match action {
        DockerCredentialAction::Get => {
            let server_url = read_stdin()?;
            let server_url = server_url.trim();
            // A server URL of another registry reads no settings, so a
            // broken profile does not fail it.
            let tenant_registry: TenantRegistry = match server_url.parse() {
                Ok(tenant_registry) => tenant_registry,
                Err(TenantRegistryError::NotTenantRegistry) => {
                    return Err(Report::new(CredentialsNotFound));
                }
                Err(err) => {
                    return Err(user_error(format!("invalid server URL on stdin: {err}")));
                }
            };
            let Some(settings) =
                registry::select_settings(settings_path, profile, &tenant_registry)?
            else {
                return Err(Report::new(CredentialsNotFound));
            };
            let secret = registry::password(&AntithesisApi::new(&settings, false)?).await?;
            let credential = serde_json::to_string(&Credential {
                server_url,
                username: registry::USERNAME,
                secret: &secret,
            })?;
            // A closed stdout must fail the action. The error keeps only the
            // message, so `suppress_broken_pipe` does not find a closed stream.
            writeln!(stdio::stdout(), "{credential}")
                .map_err(|err| eyre!("failed to write the credential: {err}"))?;
            Ok(())
        }
        // Docker and Podman call store only for a login. Snouty owns the
        // credential, so store fails and tells why.
        DockerCredentialAction::Store => Err(user_error(
            "snouty provides credentials for this registry; to sign in, run `snouty login` or set ANTITHESIS_API_KEY",
        )),
        // Erase changes nothing, but it must succeed: when it fails,
        // `podman logout --all` keeps the credentials of every registry, and
        // podman 4.9 crashes on `podman logout <registry>`.
        DockerCredentialAction::Erase => {
            read_stdin()?;
            Ok(())
        }
        // Docker and Podman call list only for a "credsStore" helper, which
        // serves every registry, and never for a "credHelpers" entry.
        DockerCredentialAction::List => Err(user_error(
            "the snouty credential helper is not a general-purpose credential store; use it only as a \"credHelpers\" entry for registry.<tenant>.antithesis.com, not as \"credsStore\"",
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
