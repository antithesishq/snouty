pub mod api;
pub mod cli;
pub mod error;
pub mod moment;
pub mod params;

use std::io::{self, ErrorKind, Read};
use std::process::Command;

use chrono::{Duration, Local};
use clap::Parser;
use log::{debug, info};

use crate::api::AntithesisApi;
use crate::cli::{Cli, Commands};
use crate::error::{Error, Result};
use crate::params::Params;

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .map_err(|e| error::Error::InvalidArgs(format!("failed to read stdin: {}", e)))?;
    let buf = buf.trim().to_string();
    Ok(buf)
}

fn get_params(args: Vec<String>, use_stdin: bool, support_moment: bool) -> Result<Params> {
    // Parse stdin params if --stdin flag is set
    let stdin_params = if use_stdin {
        let input = read_stdin()?;
        if support_moment && moment::is_moment_format(&input) {
            debug!("detected Moment.from on stdin");
            Some(moment::parse(&input)?)
        } else {
            debug!("parsing input as JSON");
            let value: serde_json::Value = json5::from_str(&input)
                .map_err(|e| error::Error::InvalidArgs(format!("invalid JSON: {}", e)))?;
            Some(Params::from_json(&value)?)
        }
    } else {
        None
    };

    // Parse CLI args if provided
    let args_params = if !args.is_empty() {
        Some(Params::from_args(&args)?)
    } else {
        None
    };

    // Merge params: CLI args take priority over stdin
    match (stdin_params, args_params) {
        (Some(mut stdin), Some(args)) => {
            stdin.merge(args);
            Ok(stdin)
        }
        (Some(stdin), None) => Ok(stdin),
        (None, Some(args)) => Ok(args),
        (None, None) => Err(Error::InvalidArgs("no parameters provided".to_string())),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Run {
            webhook,
            stdin,
            args,
        } => {
            info!("running test with webhook: {}", webhook);
            cmd_run(webhook, args, stdin).await
        }
        Commands::Debug { stdin, args } => {
            info!("starting debug session");
            cmd_debug(args, stdin).await
        }
        Commands::Completions { shell } => cmd_completions(shell),
        Commands::Version => {
            println!("snouty {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Commands::Update => cmd_update(),
    };

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

async fn cmd_run(webhook: String, args: Vec<String>, use_stdin: bool) -> Result<()> {
    let params = get_params(args, use_stdin, false)?;
    params.validate_test_params()?;

    // Print params to stderr for user visibility (with sensitive values redacted)
    eprintln!(
        "\nRequesting Antithesis test run with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map()).unwrap()
    );

    let api = AntithesisApi::from_env()?;
    let response = api
        .post(&format!("/launch/{}", webhook))
        .json(&serde_json::json!({ "params": params.to_value() }))
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    debug!("response status: {}, body:\n{}", status, body);

    if status.is_success() {
        // Estimate when the report email will arrive
        let duration_mins: i64 = params
            .as_map()
            .get("antithesis.duration")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let eta = Local::now() + Duration::minutes(duration_mins + 10);
        eprintln!(
            "\nExpect a report email from Antithesis around {}",
            eta.format("%b %-d at %-I:%M %p")
        );

        Ok(())
    } else {
        Err(error::Error::Api {
            status: status.as_u16(),
            message: body,
        })
    }
}

async fn cmd_debug(args: Vec<String>, use_stdin: bool) -> Result<()> {
    let params = get_params(args, use_stdin, true)?;
    params.validate_debugging_params()?;

    // Print params to stderr for user visibility (with sensitive values redacted)
    eprintln!(
        "\nRequesting the Antithesis multiverse debugger with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map()).unwrap()
    );

    let api = AntithesisApi::from_env()?;
    let response = api
        .post("/launch/debugging")
        .json(&serde_json::json!({ "params": params.to_value() }))
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    debug!("response status: {}, body length: {}", status, body.len());

    if status.is_success() {
        println!("{}", body);

        // Estimate when the debugging session email will arrive
        let eta = Local::now() + Duration::minutes(10);
        eprintln!(
            "\nExpect a debugging session email from Antithesis around {}",
            eta.format("%b %-d at %-I:%M %p")
        );

        Ok(())
    } else {
        Err(error::Error::Api {
            status: status.as_u16(),
            message: body,
        })
    }
}

fn cmd_completions(shell: String) -> Result<()> {
    let output = match shell.as_str() {
        "bash" => include_str!(concat!(env!("OUT_DIR"), "/snouty.bash")),
        "zsh" => include_str!(concat!(env!("OUT_DIR"), "/_snouty")),
        "fish" => include_str!(concat!(env!("OUT_DIR"), "/snouty.fish")),
        "powershell" => include_str!(concat!(env!("OUT_DIR"), "/_snouty.ps1")),
        "elvish" => include_str!(concat!(env!("OUT_DIR"), "/snouty.elv")),
        _ => {
            return Err(Error::InvalidArgs(format!(
                "unsupported shell: {shell}\nsupported: bash, zsh, fish, powershell, elvish"
            )));
        }
    };
    print!("{output}");
    Ok(())
}

fn cmd_update() -> Result<()> {
    // Attempt to spawn snouty-update and wait for it to finish
    match Command::new("snouty-update").status() {
        Ok(status) if status.success() => {
            std::process::exit(0);
        }
        Ok(status) => {
            log::error!("snouty-update failed with exit code {}\n", status);
        }
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                log::warn!("snouty-update command not found\n");
            } else {
                log::error!("failed to run snouty-update: {}\n", err);
            }
        }
    }

    // Updater not found, show manual update instructions
    eprintln!(
        "You are running snouty {}.\n\n\
         To check for updates, visit:\n\
         https://github.com/antithesishq/snouty/releases",
        env!("CARGO_PKG_VERSION")
    );
    Ok(())
}
