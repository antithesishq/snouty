use std::ffi::OsStr;
use std::io::{self, ErrorKind, Read};
use std::path::{Path, PathBuf};
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
use snouty::validate;

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

fn infer_default_run_source(config_dir: Option<&Path>, cwd: Option<&Path>) -> Option<String> {
    config_dir
        .and_then(infer_git_source_from_probe)
        .or_else(|| cwd.and_then(infer_git_source_from_probe))
}

fn git_rev_parse_path(probe_dir: &Path, flag: &str) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(probe_dir)
        .args(["rev-parse", flag])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    if path.is_empty() {
        return None;
    }

    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        probe_dir.join(path)
    };
    path.canonicalize().ok()
}

fn infer_git_source_from_probe(probe_dir: &Path) -> Option<String> {
    if git_rev_parse_path(probe_dir, "--show-superproject-working-tree").is_some() {
        let submodule_root = git_rev_parse_path(probe_dir, "--show-toplevel")?;
        let source = submodule_root.file_name()?.to_str()?.to_string();
        debug!(
            "inferred antithesis.source={} from submodule probe {}",
            source,
            probe_dir.display()
        );
        return Some(source);
    }

    let git_common_dir = git_rev_parse_path(probe_dir, "--git-common-dir")?;
    if git_common_dir.file_name() != Some(OsStr::new(".git")) {
        debug!(
            "skipping antithesis.source inference for git probe {} because git-common-dir {} is unsupported",
            probe_dir.display(),
            git_common_dir.display()
        );
        return None;
    }

    let repo_dir = git_common_dir.parent()?;
    let source = repo_dir.file_name()?.to_str()?.to_string();
    debug!(
        "inferred antithesis.source={} from git probe {}",
        source,
        probe_dir.display()
    );
    Some(source)
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
        Commands::Validate(args) => validate::cmd_validate(args).await,
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
    let current_dir = std::env::current_dir().ok();
    let inferred_source = infer_default_run_source(args.config.as_deref(), current_dir.as_deref());
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
        let config = container::ComposeConfig::new(config_dir)?;

        let registry = std::env::var("ANTITHESIS_REPOSITORY")
            .wrap_err("missing environment variable: ANTITHESIS_REPOSITORY")?;

        let image_ref = container::generate_image_ref(&registry);
        params.insert("antithesis.config_image", &image_ref);
        Some((config, registry, image_ref))
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

    if !params.contains_key("antithesis.source") {
        if let Some(source) = inferred_source {
            params.insert("antithesis.source", source);
        } else {
            bail!(
                "invalid arguments: --source is required when source cannot be inferred from --config or the current working directory"
            );
        }
    }

    params.validate_test_params()?;

    // Build and push config image (after validation passes)
    if let Some((config, registry, config_image)) = config_image_ref {
        let rt = container::runtime()?;

        let pinned_images = rt.push_compose_images(config.dir(), &registry)?;
        if !pinned_images.is_empty() {
            params.insert("antithesis.images", pinned_images.join(";"));
        }

        let pinned_config = rt.build_and_push_config_image(config.dir(), &config_image)?;
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
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn git(dir: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "git {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn init_git_repo(parent: &TempDir, name: &str) -> PathBuf {
        let repo = parent.path().join(name);
        std::fs::create_dir(&repo).unwrap();

        git(&repo, &["init", "."]);
        git(
            &repo,
            &["config", "--local", "user.email", "test@example.com"],
        );
        git(&repo, &["config", "--local", "user.name", "test"]);
        std::fs::write(repo.join("README.md"), "test\n").unwrap();
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "-m", "init"]);

        repo
    }

    fn init_git_repo_with_separate_git_dir(
        parent: &TempDir,
        name: &str,
        git_dir_name: &str,
    ) -> PathBuf {
        let repo = parent.path().join(name);
        let git_dir = parent.path().join(git_dir_name);
        std::fs::create_dir(&repo).unwrap();

        git(
            parent.path(),
            &[
                "init",
                "--separate-git-dir",
                git_dir.to_str().unwrap(),
                repo.to_str().unwrap(),
            ],
        );
        git(
            &repo,
            &["config", "--local", "user.email", "test@example.com"],
        );
        git(&repo, &["config", "--local", "user.name", "test"]);
        std::fs::write(repo.join("README.md"), "test\n").unwrap();
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "-m", "init"]);

        repo
    }

    fn add_git_worktree(repo: &Path, branch: &str, worktree_name: &str) -> PathBuf {
        let worktree = repo.parent().unwrap().join(worktree_name);
        git(repo, &["branch", branch]);
        git(
            repo,
            &["worktree", "add", worktree.to_str().unwrap(), branch],
        );
        worktree
    }

    fn add_git_submodule(repo: &Path, submodule_repo: &Path, submodule_path: &str) -> PathBuf {
        git(
            repo,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                submodule_repo.to_str().unwrap(),
                submodule_path,
            ],
        );
        repo.join(submodule_path)
    }

    #[test]
    fn infer_default_run_source_uses_repo_name_from_nested_dir() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo(&temp, "plain-repo");
        let nested = repo.join("nested");
        std::fs::create_dir(&nested).unwrap();

        assert_eq!(
            infer_default_run_source(None, Some(nested.as_path())),
            Some("plain-repo".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_uses_common_git_dir_for_worktree() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo(&temp, "common-repo");
        let worktree = add_git_worktree(&repo, "linked", "linked-worktree");

        assert_eq!(
            infer_default_run_source(None, Some(worktree.as_path())),
            Some("common-repo".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_returns_none_for_separate_git_dir_repo() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo_with_separate_git_dir(&temp, "separate-repo", "git-storage");

        assert_eq!(infer_default_run_source(None, Some(repo.as_path())), None);
    }

    #[test]
    fn infer_default_run_source_uses_submodule_root_name() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo(&temp, "super-repo");
        let submodule_repo = init_git_repo(&temp, "child-repo");
        let submodule = add_git_submodule(&repo, &submodule_repo, "libs/renamed-child");

        assert_eq!(
            infer_default_run_source(None, Some(submodule.as_path())),
            Some("renamed-child".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_uses_nested_submodule_root_name() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo(&temp, "super-repo");
        let submodule_repo = init_git_repo(&temp, "child-repo");
        let nested_submodule_repo = init_git_repo(&temp, "grandchild-repo");
        let submodule = add_git_submodule(&repo, &submodule_repo, "deps/child");
        let nested_submodule =
            add_git_submodule(&submodule, &nested_submodule_repo, "vendor/grandchild");

        assert_eq!(
            infer_default_run_source(None, Some(nested_submodule.as_path())),
            Some("grandchild".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_prefers_config_dir_over_cwd() {
        let temp = TempDir::new().unwrap();
        let config_repo = init_git_repo(&temp, "config-repo");
        let cwd_repo = init_git_repo(&temp, "cwd-repo");
        let config_dir = config_repo.join("config");
        let cwd_subdir = cwd_repo.join("subdir");
        std::fs::create_dir(&config_dir).unwrap();
        std::fs::create_dir(&cwd_subdir).unwrap();

        assert_eq!(
            infer_default_run_source(Some(config_dir.as_path()), Some(cwd_subdir.as_path())),
            Some("config-repo".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_falls_back_to_cwd_when_config_probe_fails() {
        let temp = TempDir::new().unwrap();
        let repo = init_git_repo(&temp, "cwd-repo");
        let repo_subdir = repo.join("subdir");
        let missing_config = temp.path().join("missing-config");
        std::fs::create_dir(&repo_subdir).unwrap();

        assert_eq!(
            infer_default_run_source(Some(missing_config.as_path()), Some(repo_subdir.as_path())),
            Some("cwd-repo".to_string())
        );
    }

    #[test]
    fn infer_default_run_source_returns_none_outside_git_repo() {
        let temp = TempDir::new().unwrap();

        assert_eq!(infer_default_run_source(None, Some(temp.path())), None);
    }
}
