//! `snouty docker-credential`: the Docker credential helper protocol
//! (<https://github.com/docker/docker-credential-helpers>) for the tenant
//! registry. Docker runs it through the `docker-credential-snouty` binary.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::PathBuf;

use color_eyre::eyre::{Context, Report, Result};
use serde::Serialize;

use crate::api::AntithesisApi;
use crate::cli::DockerCredentialAction;
use crate::error::user_error;
use crate::registry::{self, RegistryHost};
use crate::settings::Settings;

/// No profile has a credential for the requested registry.
#[derive(Debug)]
pub struct CredentialsNotFound;

impl std::fmt::Display for CredentialsNotFound {
    /// The exact message that Docker reads as "this helper has no credential
    /// for the server", rather than as a failure.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("credentials not found in native keychain")
    }
}

impl std::error::Error for CredentialsNotFound {}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct Credential<'a> {
    #[serde(rename = "ServerURL")]
    server_url: &'a str,
    username: &'a str,
    secret: &'a str,
}

/// Run `action`. `active` is the settings of the selected profile, and
/// `project_settings_path` is the `--settings` flag, to find every other
/// profile.
///
/// The protocol puts an error on stdout, so the caller prints the message of
/// an error there (see [`CredentialsNotFound`]).
pub async fn cmd_docker_credential(
    action: DockerCredentialAction,
    active: Settings,
    project_settings_path: Option<PathBuf>,
) -> Result<()> {
    match action {
        DockerCredentialAction::Get => {
            let server_url = read_stdin()?;
            let server_url = server_url.trim();
            let host: RegistryHost = server_url
                .parse()
                .map_err(|err| user_error(format!("invalid server URL on stdin: {err}")))?;
            let every = || Settings::resolve_every_profile(project_settings_path);
            let Some(settings) = registry::select_profile(active, every, &host)? else {
                return Err(Report::new(CredentialsNotFound));
            };
            // Credentials resolve as for any other command, so
            // `ANTITHESIS_API_KEY` wins over a stored credential.
            let secret = AntithesisApi::new(&settings, false)?
                .registry_secret()
                .await?;
            println!(
                "{}",
                serde_json::to_string(&Credential {
                    server_url,
                    username: registry::USERNAME,
                    secret: &secret,
                })?
            );
        }
        // Snouty owns the credential, so it keeps nothing that Docker gives it
        // and erases nothing. A failure here would fail `docker login` and
        // `docker logout`, which change nothing that snouty uses.
        DockerCredentialAction::Store | DockerCredentialAction::Erase => {
            read_stdin()?;
        }
        DockerCredentialAction::List => {
            let hosts: BTreeMap<String, &str> = std::iter::once(active)
                .chain(Settings::resolve_every_profile(project_settings_path)?)
                .filter_map(|settings| registry::tenant_registry(&settings))
                .map(|host| (host.to_string(), registry::USERNAME))
                .collect();
            println!("{}", serde_json::to_string(&hosts)?);
        }
    }
    Ok(())
}

fn read_stdin() -> Result<String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .wrap_err("failed to read stdin")?;
    Ok(input)
}
