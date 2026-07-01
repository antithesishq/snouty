use std::io::{self, IsTerminal, Write};

use color_eyre::eyre::{Context, Result, eyre};

use crate::{
    attributed_value::AttributedValue,
    credentials::{Credentials, persist},
    settings::{Settings, update_settings_in_global_file, validate_tenant_host},
};

pub async fn cmd_login(
    tenant: Option<String>,
    repository: Option<String>,
    current_settings: Result<Settings>,
) -> Result<()> {
    let profile = current_settings.as_ref().ok().and_then(|s| s.profile());

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

    if let Some(credentials) = prompt_for_auth(profile).await? {
        persist(credentials, profile)?;
    }

    update_settings_in_global_file(
        Some(tenant_to_use),
        Some(repository_to_use),
        None,
        None,
        profile,
    )?;

    Ok(())
}

fn prompt_for_value(value_name: &str, previous_value: Option<&str>) -> Result<String> {
    println!("What {value_name} would you like to use?");
    if let Some(prev) = previous_value
        && !prev.is_empty()
    {
        println!("(Hit enter to use the previous value of [{prev}])");
    }

    let mut input = String::new();
    while input.is_empty() {
        io::stdin().read_line(&mut input)?;
        input = input.trim().to_owned();

        if input.is_empty()
            && let Some(prev) = previous_value
        {
            input.push_str(prev);
        }
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

async fn prompt_for_auth(profile: Option<&str>) -> Result<Option<Credentials>> {
    let previous_value =
        Credentials::for_ambient_credentials_with_attribution(profile, true, false).await;

    let default_selection = match &previous_value {
        Err(_) => '1',
        Ok(creds) => match creds {
            AttributedValue::EnvironmentVariable { .. } => '1',
            _ => match creds.unwrap() {
                Credentials::ApiKey(_) => '2',
                Credentials::Password(_) => '3',
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
    while AuthSetupType::try_from_str(&input).is_none() {
        io::stdin().read_line(&mut input)?;
        input = input.trim().to_owned();

        if input.is_empty() {
            input.push(default_selection);
        }
    }

    match AuthSetupType::try_from_str(&input).unwrap_or(AuthSetupType::ApiKey) {
        AuthSetupType::Skip => Ok(None),
        AuthSetupType::ApiKey => match previous_value.map(|attr| attr.extract()) {
            Ok(Credentials::ApiKey(creds)) => prompt_for_api_key(Some(&creds.api_key)),
            _ => prompt_for_api_key(None),
        }
        .map(Some),
        AuthSetupType::Password => match previous_value.map(|attr| attr.extract()) {
            Ok(Credentials::Password(creds)) => {
                prompt_for_username_password(Some(&creds.username), Some(&creds.password))
            }
            _ => prompt_for_username_password(None, None),
        }
        .map(Some),
    }
}

fn prompt_for_api_key(previous_api_key: Option<&str>) -> Result<Credentials> {
    Ok(Credentials::for_api_key(prompt_for_sensitive_value(
        "API key",
        previous_api_key,
    )?))
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
) -> Result<Credentials> {
    Ok(Credentials::for_password(
        prompt_for_value("username", previous_username)?,
        prompt_for_sensitive_value("password", previous_password)?,
    ))
}
