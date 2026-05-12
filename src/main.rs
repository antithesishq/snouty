use std::io::{self, ErrorKind, Read};
use std::process::Command;

use clap::Parser;
use log::{debug, info};

use color_eyre::eyre::{Context, Result, bail};
use snouty::api::AntithesisApi;
use snouty::cli::{ApiCommands, Cli, Commands, DebugArgs, LaunchArgs};
use snouty::config;
use snouty::container;
use snouty::docs;
use snouty::moment;
use snouty::params::Params;
use snouty::validate;

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .wrap_err("invalid arguments: failed to read stdin")?;
    let buf = buf.trim().to_string();
    Ok(buf)
}

fn get_stdin_params(use_stdin: bool, support_moment: bool) -> Result<Option<Params>> {
    if use_stdin {
        let input = read_stdin()?;
        if support_moment && moment::is_moment_format(&input) {
            debug!("detected Moment.from on stdin");
            Ok(Some(moment::parse(&input)?))
        } else {
            debug!("parsing input as JSON");
            let value: serde_json::Value =
                json5::from_str(&input).wrap_err("invalid arguments: invalid JSON")?;
            Ok(Some(Params::from_json(&value)?))
        }
    } else {
        Ok(None)
    }
}

fn get_params(args: Vec<String>, use_stdin: bool, support_moment: bool) -> Result<Params> {
    let stdin_params = get_stdin_params(use_stdin, support_moment)?;

    let args_params = if !args.is_empty() {
        Some(Params::from_args(&args)?)
    } else {
        None
    };

    // CLI args take priority over stdin
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
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(buf, "{}", record.args())
        })
        .init();
    let cli = Cli::parse();

    let json = cli.json;
    if json && let Some(name) = json_unaware_command_name(&cli.command) {
        eprintln!("warning: --json has no effect for `snouty {name}`");
    }
    match cli.command {
        Commands::Launch(args) => {
            info!("launching test with webhook: {}", args.webhook);
            cmd_launch(args, json).await
        }
        Commands::Run(args) => {
            eprintln!("warning: `snouty run` is deprecated, use `snouty launch` instead");
            info!("launching test with webhook: {}", args.webhook);
            cmd_launch(args, json).await
        }
        Commands::Runs { command } => snouty::runs::cmd_runs(command, json).await,
        Commands::Api(ApiCommands::Webhook {
            webhook,
            stdin,
            args,
        }) => {
            info!("running api webhook with webhook: {}", webhook);
            cmd_api_webhook(webhook, args, stdin).await
        }
        Commands::Debug(args) => {
            info!("starting debug session");
            cmd_debug(args, json).await
        }
        Commands::Validate(args) => validate::cmd_validate(args).await,
        Commands::Doctor => snouty::doctor::cmd_doctor(),
        Commands::Completions { shell } => cmd_completions(shell),
        Commands::Version => {
            println!("snouty {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Commands::Update => cmd_update(),
        Commands::Docs { offline, command } => docs::cmd_docs(command, offline, json).await,
    }
}

fn json_unaware_command_name(command: &Commands) -> Option<&'static str> {
    match command {
        Commands::Launch(_)
        | Commands::Run(_)
        | Commands::Runs { .. }
        | Commands::Docs { .. }
        | Commands::Debug { .. } => None,
        Commands::Api(ApiCommands::Webhook { .. }) => Some("api webhook"),
        Commands::Validate(_) => Some("validate"),
        Commands::Doctor => Some("doctor"),
        Commands::Completions { .. } => Some("completions"),
        Commands::Version => Some("version"),
        Commands::Update => Some("update"),
    }
}

async fn cmd_launch(args: LaunchArgs, json: bool) -> Result<()> {
    let mut params = Params::new();

    if let Some(test_name) = args.test_name {
        params.insert("antithesis.test_name", test_name);
    }
    if let Some(description) = args.description {
        params.insert("antithesis.description", description);
    }
    if let Some(duration) = args.duration {
        params.insert("antithesis.duration", duration);
    }
    let has_source = if let Some(source) = args.source {
        params.insert("antithesis.source", source);
        true
    } else {
        false
    };
    if args.ephemeral {
        params.insert("antithesis.is_ephemeral", "true");
    }
    if let Some(recipients) = args.recipients {
        params.insert("antithesis.report.recipients", recipients);
    }

    if let Some(config_image) = args.config_image {
        params.insert("antithesis.config_image", config_image);
    }

    let config_image_ref = if let Some(config_dir) = args.config {
        let config = config::detect_config(&config_dir)?;

        let registry = std::env::var("ANTITHESIS_REPOSITORY")
            .wrap_err("missing environment variable: ANTITHESIS_REPOSITORY")?;

        let image_ref = container::generate_image_ref(&registry);
        params.insert("antithesis.config_image", &image_ref);
        Some((config, registry, image_ref))
    } else {
        None
    };

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

    if !has_source {
        params.insert("antithesis.is_ephemeral", "true");
        eprintln!("Starting ephemeral run, Findings will not be available (provide --source)");
    }

    params.validate_test_params()?;

    if let Some((config, registry, config_image)) = config_image_ref {
        let rt = container::runtime()?;

        // Compose configs reference local image tags that need to be pushed to
        // the registry; k8s configs reference images by name in the manifests
        // and the platform pulls them itself.
        if let config::Config::Compose(compose_config) = &config {
            let pinned_images = rt.push_compose_images(compose_config, &registry)?;
            if !pinned_images.is_empty() {
                params.insert("antithesis.images", pinned_images.join(";"));
            }
        }

        let pinned_config = rt.build_and_push_config_image(config.dir(), &config_image)?;
        params.insert("antithesis.config_image", pinned_config);
    }

    let response = launch_webhook(&args.webhook, params).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("Test run started: run_id {}", response.run_id);
    }

    Ok(())
}

async fn cmd_api_webhook(webhook: String, args: Vec<String>, use_stdin: bool) -> Result<()> {
    let params = get_params(args, use_stdin, false)?;
    let body = launch_webhook(&webhook, params).await?;
    println!("{}", serde_json::to_string(&body)?);
    Ok(())
}

async fn launch_webhook(webhook: &str, params: Params) -> Result<snouty::api::LaunchResponse> {
    params.validate_test_params()?;

    eprintln!(
        "\nRequesting Antithesis test run with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map())?
    );

    let api = AntithesisApi::from_env()?;
    api.launch_test(webhook, &params).await
}

fn debug_typed_params(args: &DebugArgs) -> Params {
    let mut params = Params::new();

    if let Some(session_id) = &args.session_id {
        params.insert("antithesis.debugging.session_id", session_id.as_str());
    }
    if let Some(input_hash) = &args.input_hash {
        params.insert("antithesis.debugging.input_hash", input_hash.as_str());
    }
    if let Some(vtime) = &args.vtime {
        params.insert("antithesis.debugging.vtime", vtime.as_str());
    }
    if let Some(description) = &args.description {
        params.insert("antithesis.event_description", description.as_str());
    }
    if let Some(recipients) = &args.recipients {
        params.insert("antithesis.report.recipients", recipients.as_str());
    }

    params
}

fn debug_params(args: DebugArgs) -> Result<Params> {
    let mut params = get_stdin_params(args.stdin, true)?.unwrap_or_default();
    params.merge(debug_typed_params(&args));

    if params.is_empty() {
        bail!("invalid arguments: no parameters provided");
    }

    Ok(params)
}

async fn cmd_debug(args: DebugArgs, json: bool) -> Result<()> {
    let params = debug_params(args)?;
    params.validate_debugging_params()?;

    eprintln!(
        "\nRequesting the Antithesis multiverse debugger with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map())?
    );

    let api = AntithesisApi::from_env()?;
    let response = api.launch_debugging(&params).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        println!("Debugging session started: run_id {}", response.run_id);
    }

    Ok(())
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
