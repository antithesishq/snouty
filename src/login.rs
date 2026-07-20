use std::io::{self, IsTerminal, Write};
use std::path::Path;

use color_eyre::eyre::{Context, Result, eyre};

use crate::{
    attributed_value::AttributedValue,
    auth::{AuthenticationInfo, CredentialStorage, PersistableCredentials, persist},
    env,
    settings::{
        ANTITHESIS_PROFILE_ENV_VAR_NAME, Settings, update_settings_in_global_file,
        validate_tenant_host,
    },
};

pub fn cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    profile: Option<&str>,
    current_settings: Result<Settings>,
) -> Result<()> {
    if let Err(report) = &current_settings {
        eprintln!("The current settings failed to load with the following error: {report:#}");
        eprintln!(
            "Would you like to proceed with the login command? Doing so may cause your existing settings file to be replaced rather than updated."
        );
        eprintln!("1. Yes, please proceed");
        eprintln!("2. No, please exit immediately");
        eprintln!(
            "Please enter either '1' or '2'. Any other input will cause the program to exit."
        );

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        match input.trim() {
            "1" => {}
            _ => {
                return Err(eyre!(
                    "Exiting login command without completing per user request."
                ));
            }
        }
    }

    let profile_to_use = profile
        .map(|p| p.to_owned())
        .or_else(|| env::var(ANTITHESIS_PROFILE_ENV_VAR_NAME).ok().flatten())
        // this .filter is **after** the or_else because `--profile ""` should cause login to not use a profile, regardless of the contents of the env variable
        .filter(|p| !p.is_empty());

    let tenant_to_use = match tenant {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            "Antithesis tenant",
            current_settings.as_ref().ok().and_then(|s| s.tenant()),
        )?,
    };
    validate_tenant_host(&tenant_to_use)?;

    let repository_to_use = match repository {
        Some(arg_value) if !arg_value.is_empty() => arg_value,
        Some(_) | None => prompt_for_value(
            "container repository",
            current_settings.as_ref().ok().and_then(|s| s.repository()),
        )?,
    };

    // Capture the credential kind and where it was stored so the summary can name
    // both; `None` when the user chose to skip credential setup.
    let credential_summary = match prompt_for_auth(profile_to_use.as_deref())? {
        Some(credentials) => {
            let kind = match &credentials {
                PersistableCredentials::ApiKey { .. } => "API key",
                PersistableCredentials::Password { .. } => "username and password",
            };
            Some((kind, persist(credentials, profile_to_use.as_deref())?))
        }
        None => None,
    };

    let settings_path = update_settings_in_global_file(
        Some(tenant_to_use.clone()),
        Some(repository_to_use.clone()),
        None,
        None,
        profile_to_use.as_deref(),
    )?;

    print_login_summary(
        &tenant_to_use,
        &repository_to_use,
        profile_to_use.as_deref(),
        &settings_path,
        credential_summary,
    );

    Ok(())
}

/// Confirm what `snouty login` persisted, where, and the obvious next step —
/// otherwise a successful login exits silently, leaving the user unsure it took.
fn print_login_summary(
    tenant: &str,
    repository: &str,
    profile: Option<&str>,
    settings_path: &Path,
    credentials: Option<(&str, CredentialStorage)>,
) {
    let scope = match profile {
        Some(p) => format!(" under profile `{p}`"),
        None => String::new(),
    };
    // Only mention what was actually recorded: a blank repository is intentionally
    // not persisted (see `insert_key_if_non_empty` in settings.rs), so don't claim
    // we saved one.
    let mut saved = format!("tenant `{tenant}`");
    if !repository.is_empty() {
        saved.push_str(&format!(" and repository `{repository}`"));
    }
    println!("\nSaved {saved}{scope} to {}.", settings_path.display());
    match credentials {
        Some((kind, CredentialStorage::Keychain)) => {
            println!("Stored your {kind}{scope} in the system keychain.");
        }
        Some((kind, CredentialStorage::File(path))) => {
            println!("Stored your {kind}{scope} in {}.", path.display());
        }
        None => {
            println!(
                "Skipped credential storage — snouty will use the ANTITHESIS_API_KEY or ANTITHESIS_USERNAME/PASSWORD environment variables."
            );
        }
    }
    println!("Run `snouty doctor` to verify your setup.");
}

fn prompt_for_value(value_name: &str, previous_value: Option<&str>) -> Result<String> {
    println!("What {value_name} would you like to use?");
    if let Some(prev) = previous_value
        && !prev.is_empty()
    {
        println!("(Hit enter to use the previous value of [{prev}])");
    }

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    input = input.trim().to_owned();

    if input.is_empty()
        && let Some(prev) = previous_value
    {
        input.push_str(prev);
    }

    Ok(input)
}

enum AuthSetupType {
    Skip,
    ApiKey,
    Password,
}

impl AuthSetupType {
    fn try_from_str(to_parse: &str) -> Option<Self> {
        match to_parse {
            "1" => Some(AuthSetupType::Skip),
            "2" => Some(AuthSetupType::ApiKey),
            "3" => Some(AuthSetupType::Password),
            _ => None,
        }
    }
}

fn prompt_for_auth(profile: Option<&str>) -> Result<Option<PersistableCredentials>> {
    let previous_value =
        AuthenticationInfo::for_ambient_configuration_with_attribution(profile, true);

    let default_selection = match &previous_value {
        Err(_) => '1',
        Ok(creds) => match creds {
            AttributedValue::EnvironmentVariable { .. } => '1',
            _ => match creds.value() {
                AuthenticationInfo::ApiKey { .. } => '2',
                AuthenticationInfo::Password { .. } => '3',
                _ => '1',
            },
        },
    };

    println!("What kind of credentials would you like to use?");
    println!(
        "1. Skip setup (Select this option if you plan to use environment variables instead of persisted credentials.)"
    );
    println!("2. API key");
    println!("3. Username/password");
    println!("(Hit enter to use the default value of [{default_selection}])");

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    input = input.trim().to_owned();

    if input.is_empty() {
        input.push(default_selection);
    }

    match AuthSetupType::try_from_str(&input) {
        None => Err(eyre!("Unrecognized input.")),
        Some(AuthSetupType::Skip) => Ok(None),
        Some(AuthSetupType::ApiKey) => match previous_value.map(|attr| attr.extract()) {
            Ok(AuthenticationInfo::ApiKey { api_key }) => prompt_for_api_key(Some(&api_key)),
            _ => prompt_for_api_key(None),
        }
        .map(Some),
        Some(AuthSetupType::Password) => match previous_value.map(|attr| attr.extract()) {
            Ok(AuthenticationInfo::Password { username, password }) => {
                prompt_for_username_password(Some(&username), Some(&password))
            }
            _ => prompt_for_username_password(None, None),
        }
        .map(Some),
    }
}

fn prompt_for_api_key(previous_api_key: Option<&str>) -> Result<PersistableCredentials> {
    Ok(PersistableCredentials::ApiKey {
        api_key: prompt_for_sensitive_value("API key", previous_api_key)?,
    })
}

fn prompt_for_sensitive_value(value_name: &str, previous_value: Option<&str>) -> Result<String> {
    let prompt_str = match previous_value {
        Some(prev) if !prev.is_empty() => {
            format!("Please enter your {value_name} (leave blank to use previous value): ")
        }
        Some(_) | None => format!("Please enter your {value_name}: "),
    };

    let entered = read_secret(value_name, &prompt_str)?;
    if entered.is_empty() {
        match previous_value {
            Some(prev) if !prev.is_empty() => Ok(prev.to_owned()),
            Some(_) | None => Err(eyre!("{value_name} cannot be empty")),
        }
    } else {
        Ok(entered)
    }
}

/// Read a secret, hiding it from the terminal when one is attached.
///
/// Interactively (stdin is a TTY) the value is read with [`rpassword`] so it is
/// never echoed. When stdin is *not* a terminal — piped input from a script or
/// the spec tests — `rpassword` would try to open `/dev/tty` (failing, or
/// blocking on the real terminal) instead of reading the pipe, so we read the
/// secret as an ordinary line from stdin. There is no terminal echo to suppress
/// in that case, so nothing is lost.
fn read_secret(value_name: &str, prompt: &str) -> Result<String> {
    if io::stdin().is_terminal() {
        return rpassword::prompt_password(prompt).wrap_err(format!("Unable to read {value_name}"));
    }

    print!("{prompt}");
    io::stdout().flush().ok();

    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .wrap_err(format!("Unable to read {value_name}"))?;
    // Strip only the line terminator; a secret may legitimately contain
    // surrounding whitespace.
    Ok(line.trim_end_matches(['\r', '\n']).to_owned())
}

fn prompt_for_username_password(
    previous_username: Option<&str>,
    previous_password: Option<&str>,
) -> Result<PersistableCredentials> {
    Ok(PersistableCredentials::Password {
        username: prompt_for_value("username", previous_username)?,
        password: prompt_for_sensitive_value("password", previous_password)?,
    })
}
