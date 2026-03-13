use std::io::{self, ErrorKind, Read};
use std::process::Command;

use chrono::{Duration, Local};
use clap::Parser;
use log::{debug, info};

use color_eyre::eyre::{Context, Result, bail};
use snouty::api::AntithesisApi;
use snouty::cli::{ApiCommands, Cli, Commands, RunArgs};
use snouty::container;
use snouty::docs;
use snouty::moment;
use snouty::params::Params;

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .wrap_err("invalid arguments: failed to read stdin")?;
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
            let value: serde_json::Value =
                json5::from_str(&input).wrap_err("invalid arguments: invalid JSON")?;
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
        (None, None) => bail!("invalid arguments: no parameters provided"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    color_eyre::install().unwrap();
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run(args) => {
            info!("running test with webhook: {}", args.webhook);
            cmd_run(args).await
        }
        Commands::Api(ApiCommands::Webhook {
            webhook,
            stdin,
            args,
        }) => {
            info!("running api webhook with webhook: {}", webhook);
            cmd_api_webhook(webhook, args, stdin).await
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
        Commands::Docs { offline, command } => docs::cmd_docs(command, offline).await,
    }
}

async fn cmd_run(args: RunArgs) -> Result<()> {
    let mut params = Params::new();

    // Insert typed flags into params (skip None/false)
    if let Some(test_name) = args.test_name {
        params.insert("antithesis.test_name", test_name);
    }
    if let Some(description) = args.description {
        params.insert("antithesis.description", description);
    }
    if let Some(duration) = args.duration {
        params.insert("antithesis.duration", duration.to_string());
    }
    if args.ephemeral {
        params.insert("antithesis.is_ephemeral", "true");
    }
    if let Some(source) = args.source {
        params.insert("antithesis.source", source);
    }
    if let Some(recipients) = args.recipients {
        params.insert("antithesis.report.recipients", recipients);
    }

    // Process config_image and config flags
    assert!(
        !(args.config_image.is_some() && args.config.is_some()),
        "config and config_image are mutually exclusive"
    );

    // TODO: enable config directory support for k8s manifests
    if args.webhook == "basic_k8s_test" && args.config.is_some() {
        bail!(
            "The 'basic_k8s_test' webhook does not support the --config flag. Please use --config-image with a pre-built config image instead."
        );
    }

    if let Some(config_image) = args.config_image {
        params.insert("antithesis.config_image", config_image);
    }

    let config_image_ref = if let Some(config_dir) = args.config {
        container::validate_config_dir(&config_dir)?;

        let registry = std::env::var("ANTITHESIS_REPOSITORY")
            .wrap_err("missing environment variable: ANTITHESIS_REPOSITORY")?;

        let image_ref = container::generate_image_ref(&registry);
        params.insert("antithesis.config_image", &image_ref);
        Some((config_dir, registry, image_ref))
    } else {
        None
    };

    // Parse --param key=value pairs
    if !args.params.is_empty() {
        let extra = Params::from_key_value_pairs(&args.params)?;

        // Check for conflicts with typed flags already set in params
        for key in extra.as_map().keys() {
            if params.contains_key(key) {
                bail!(
                    "invalid arguments: '{}' cannot be overridden via --param (use the dedicated flag instead)",
                    key
                );
            }
        }

        params.merge(extra);
    }

    if params.contains_key("antithesis.images") {
        bail!(
            "invalid argument: do not specify antithesis.images as --param, use api webhook instead"
        );
    }

    if params.is_empty() {
        bail!("invalid arguments: no parameters provided");
    }

    params.validate_test_params()?;

    // Build and push config image (after validation passes)
    if let Some((config_dir, registry, config_image)) = config_image_ref {
        let rt = container::runtime()?;

        let pinned_images = rt.push_compose_images(&config_dir, &registry)?;
        if !pinned_images.is_empty() {
            params.insert("antithesis.images", pinned_images.join(";"));
        }

        let pinned_config = rt.build_and_push_config_image(&config_dir, &config_image)?;
        params.insert("antithesis.config_image", pinned_config);
    }

    let duration_mins: i64 = params
        .as_map()
        .get("antithesis.duration")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    launch_webhook(&args.webhook, params).await?;

    let eta = Local::now() + Duration::minutes(duration_mins + 10);
    eprintln!(
        "\nExpect a report email from Antithesis around {}",
        eta.format("%b %-d at %-I:%M %p")
    );

    Ok(())
}

async fn cmd_api_webhook(webhook: String, args: Vec<String>, use_stdin: bool) -> Result<()> {
    let params = get_params(args, use_stdin, false)?;
    let body = launch_webhook(&webhook, params).await?;
    println!("{}", body);
    Ok(())
}

async fn launch_webhook(webhook: &str, params: Params) -> Result<String> {
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
        Ok(body)
    } else {
        bail!("API error: {} - {}", status.as_u16(), body)
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
        bail!("API error: {} - {}", status.as_u16(), body)
    }
}

fn cmd_completions(shell: String) -> Result<()> {
    let output = match shell.as_str() {
        "bash" => include_str!(concat!(env!("OUT_DIR"), "/snouty.bash")),
        "zsh" => include_str!(concat!(env!("OUT_DIR"), "/_snouty")),
        "fish" => include_str!(concat!(env!("OUT_DIR"), "/snouty.fish")),
        "elvish" => include_str!(concat!(env!("OUT_DIR"), "/snouty.elv")),
        _ => {
            bail!(
                "invalid arguments: unsupported shell: {shell}\nsupported: bash, zsh, fish, elvish"
            );
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
