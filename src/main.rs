use std::io::{self, ErrorKind, Write};
use std::path::Path;
use std::process::Command;

use clap::CommandFactory;
use clap_complete::Shell;
use indoc::writedoc;
use log::info;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use semver::Version;
use snouty::OutputOptions;
use snouty::api::AntithesisApi;
use snouty::auth::initialize_credential_store;
use snouty::cli::{
    Cli, Commands, DebugArgs, LaunchArgs, UpdateArgs, UpdateChannel, gated_command_error,
};
use snouty::compose;
use snouty::config;
use snouty::container;
use snouty::docs;
use snouty::error::user_error;
use snouty::features;
use snouty::git::{self, GitHubRepo};
use snouty::login::cmd_login;
use snouty::params::{
    ANT_CONFIG_IMAGE, ANT_DEBUGGING_INPUT_HASH, ANT_DEBUGGING_RUN_ID, ANT_DEBUGGING_SESSION_ID,
    ANT_DEBUGGING_VTIME, ANT_DESCRIPTION, ANT_DURATION, ANT_EVENT_DESCRIPTION,
    ANT_FAULT_EXCLUSIONS, ANT_FAULT_TOLERANCE, ANT_FILTER_LOGS_MATCHING,
    ANT_FILTER_SOURCE_MATCHING, ANT_IS_EPHEMERAL, ANT_PERFORMANCE_TIER, ANT_REPORT_RECIPIENTS,
    ANT_SOURCE, ANT_TEST_NAME, DuplicateKey, Params, RUN_CALLER_TYPE, VCS_PARAMS, VCS_REPO_BRANCH,
    VCS_REPO_NAME, VCS_REPO_OWNER, VCS_REPO_TYPE, VCS_VERSION_ID, VCS_VERSION_LINK,
};
use snouty::settings::Settings;
use snouty::stdio::{self, StreamClosed};
use snouty::validate;

// musl's malloc is slow, and the Linux release binaries are static musl builds.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Drop the "Backtrace omitted. Run with RUST_BACKTRACE=1…" footer: it's noise
    // on every error, and outright misleading on user errors built with
    // `suppress_backtrace` (where RUST_BACKTRACE does nothing). A genuine fault
    // still prints its backtrace when RUST_BACKTRACE is set.
    //
    // Color the report only on a real terminal — piped/redirected stderr (and the
    // test/gallery captures) gets plain text, not ANSI escapes.
    let theme = if stdio::stderr().is_terminal() {
        color_eyre::config::Theme::dark()
    } else {
        color_eyre::config::Theme::new()
    };
    color_eyre::config::HookBuilder::default()
        .theme(theme)
        .display_env_section(false)
        .install()
        .unwrap();
    env_logger::Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
    snouty::mcp::exit_with_server();
    let cli = snouty::cli::parse();
    if let Err(report) = run(cli).await {
        // One rendering for every error: `render_report` collapses the chain
        // index for single errors and wraps overlong prose, both printing
        // concerns that belong here rather than in the messages. User-facing
        // failures are built with `user_error`/4xx `suppress_backtrace`, so they
        // print message + any `.note()`/`.suggestion()` hints with no backtrace;
        // genuine internal faults keep theirs.
        let rendered = snouty::error::render_report(&report);
        eprintln!("{}", snouty::wrap_if_tty(&rendered));
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let Cli {
        json,
        verbose,
        settings: settings_path,
        profile,
        command,
    } = cli;
    if json && let Some(name) = json_unaware_command_name(&command) {
        eprintln!("warning: --json has no effect for `snouty {name}`");
    }
    // The global output flags travel together from here on; every command
    // takes them as one value instead of a swappable positional bool pair.
    let output = OutputOptions { json, verbose };

    if let Some(report) = gated_command_error(&command, &features::enabled()) {
        return Err(report);
    }

    if let Err(err) = initialize_credential_store() {
        eprintln!("warning: Could not initialize system keychain credential storage: {err:?}");
    }

    // Resolve all settings up front. A broken/unreadable settings file fails
    // here, cleanly and once, before any command runs — including commands that
    // don't read settings. That keeps dispatch a single flat match.
    let settings = Settings::resolve(settings_path.clone(), profile.clone());
    let result = match command {
        Commands::Completions { shell } => cmd_completions(shell).map_err(Report::new),
        Commands::Version => {
            // SNOUTY_VERSION (set by build.rs) is the crate version plus the
            // build's git sha when known — the same string clap's --version
            // prints, so the two stay consistent.
            println!("snouty {}", env!("SNOUTY_VERSION"));
            Ok(())
        }
        Commands::Update(args) => cmd_update(&args, &settings?),
        Commands::Docs { offline, command } => docs::cmd_docs(command, offline, output).await,
        Commands::Login { tenant, repository } => {
            cmd_login(tenant, repository, profile.as_deref(), &settings?, output).await
        }
        Commands::Launch(args) => cmd_launch(args, &settings?, output).await,
        Commands::Run(args) => {
            eprintln!("warning: `snouty run` is deprecated, use `snouty launch` instead");
            cmd_launch(args, &settings?, output).await
        }
        Commands::Runs { command } => snouty::runs::cmd_runs(command, &settings?, output).await,
        Commands::Debug(args) => {
            info!("starting debug session");
            cmd_debug(args, &settings?, output).await
        }
        Commands::Validate(args) => validate::cmd_validate(args, &settings?).await,
        Commands::Doctor(args) => {
            snouty::doctor::cmd_doctor(&settings?, output, args.offline).await
        }
        Commands::Mcp(args) => {
            // The tool calls resolve the settings again, in each child.
            settings?;
            snouty::mcp::cmd_mcp(snouty::mcp::Globals {
                settings: settings_path,
                profile,
                verbose,
                max_concurrent_calls: args.max_concurrent_calls,
            })
            .await?;
            // rmcp reads stdin on a blocking thread, and the runtime waits for
            // that thread as it shuts down. After a stop signal, stdin can
            // still be open, so exit here. The tool calls' children are
            // already killed.
            std::process::exit(0)
        }
    };

    suppress_broken_pipe(result)
}

/// When our output is piped into something that exits early (e.g. `snouty
/// runs list | head`), writes to stdout fail with `BrokenPipe`. That's a
/// normal way for a pipeline to end, not an error — exit quietly.
///
/// Only an error that [`stdio`] marked as a closed stdout or stderr is
/// suppressed. Any other `BrokenPipe`, such as from a child's stdin or a
/// socket, is a real failure and must be reported. A closed stderr is
/// suppressed too: the report would go to that same closed stderr, so there
/// is nowhere left to say anything.
fn suppress_broken_pipe(result: Result<()>) -> Result<()> {
    match result {
        Err(err)
            if err.chain().any(|e| {
                e.downcast_ref::<io::Error>()
                    .and_then(StreamClosed::of)
                    .is_some()
            }) =>
        {
            Ok(())
        }
        other => other,
    }
}

fn json_unaware_command_name(command: &Commands) -> Option<&'static str> {
    match command {
        Commands::Launch(_)
        | Commands::Run(_)
        | Commands::Runs { .. }
        | Commands::Docs { .. }
        | Commands::Debug { .. }
        | Commands::Doctor(_) => None,
        Commands::Validate(_) => Some("validate"),
        Commands::Completions { .. } => Some("completions"),
        Commands::Version => Some("version"),
        Commands::Update(_) => Some("update"),
        Commands::Login { .. } => Some("login"),
        Commands::Mcp(_) => Some("mcp"),
    }
}

async fn cmd_launch(
    args: LaunchArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let launcher = match args.webhook {
        Some(webhook) => {
            eprintln!("warning: --webhook is deprecated, use --launcher instead");
            webhook
        }
        None => args.launcher,
    };
    info!("launching test with launcher: {launcher}");

    let mut params = Params::new();

    if let Some(test_name) = args.test_name {
        params.insert(ANT_TEST_NAME, test_name);
    }
    if let Some(description) = args.description {
        params.insert(ANT_DESCRIPTION, description);
    }
    if let Some(duration) = args.duration {
        // The API wants a bare minute count, not the human `1h30m` Display form.
        params.insert(ANT_DURATION, duration.minutes().to_string());
    }
    let has_source = if let Some(source) = args.source {
        params.insert(ANT_SOURCE, source);
        true
    } else {
        false
    };
    if args.ephemeral {
        params.insert(ANT_IS_EPHEMERAL, "true");
    }
    if let Some(recipients) = args.recipients {
        params.insert(ANT_REPORT_RECIPIENTS, recipients);
    }

    if let Some(config_image) = args.config_image {
        params.insert(ANT_CONFIG_IMAGE, config_image);
    }

    if let Some(filter_logs_matching) = args.filter_logs_matching {
        params.insert(ANT_FILTER_LOGS_MATCHING, filter_logs_matching);
    }

    if let Some(filter_source_matching) = args.filter_source_matching {
        params.insert(ANT_FILTER_SOURCE_MATCHING, filter_source_matching);
    }

    if let Some(performance_tier) = args.performance_tier {
        params.insert(ANT_PERFORMANCE_TIER, performance_tier.to_string());
    }

    if let Some(fault_tolerance) = args.fault_tolerance {
        params.insert(ANT_FAULT_TOLERANCE, fault_tolerance.to_string());
    }

    if let Some(fault_exclusions) = args.fault_exclusions {
        params.insert(ANT_FAULT_EXCLUSIONS, fault_exclusions);
    }

    let config_image_ref = if let Some(config_dir) = &args.config {
        let detected = config::detect_config(config_dir)?;
        let registry = snouty::settings::require(settings.repository(), "repository")?;

        let image_ref = container::generate_image_ref(registry);
        params.insert(ANT_CONFIG_IMAGE, &image_ref);
        Some((detected, registry.to_owned(), image_ref))
    } else {
        None
    };

    params
        .extend_checked(args.params.into_iter().collect())
        .map_err(param_overrides_flag)?;

    if params.is_empty() {
        return Err(user_error("invalid arguments: no parameters provided"));
    }

    // A user value for any detected key turns detection off, so that a
    // detected commit link cannot point at a different commit. A pre-built
    // config image has no config dir, and the current directory need not hold
    // its source, so it turns detection off too.
    let prebuilt_image = args.config.is_none() && params.contains_key(ANT_CONFIG_IMAGE);
    if !args.no_vcs
        && !prebuilt_image
        && !VCS_PARAMS.iter().any(|&(key, _)| params.contains_key(key))
    {
        params.merge(detect_vcs_params(
            args.config.as_deref().unwrap_or(Path::new(".")),
        ));
    }

    if !params.contains_key(RUN_CALLER_TYPE) {
        params.insert(RUN_CALLER_TYPE, "snouty");
    }

    params.validate_test_params()?;

    if matches!(launcher.as_str(), "basic_test" | "basic_k8s_test")
        && !params.contains_key(ANT_CONFIG_IMAGE)
    {
        return Err(user_error(format!(
            "invalid arguments: the {launcher} launcher requires a config image"
        ))
        .suggestion("pass --config or --config-image"));
    }

    if let Some((detected, registry, config_image)) = config_image_ref {
        let rt = container::runtime(settings)?;
        container::warn_ambiguous_engine(settings, rt.as_ref(), json);

        // k8s configs reference images by name in the manifests and the
        // platform pulls them itself.
        let pinned_config = match &detected {
            config::Config::Compose(compose_config) => {
                let compose = compose::DockerCompose::resolve(rt.as_ref(), compose_config.clone())?;
                let pinned_yaml = compose.pin_images(
                    rt.as_ref(),
                    &registry,
                    if args.allow_remote_images {
                        compose::MissingImages::AssumeRemote
                    } else {
                        compose::MissingImages::Reject
                    },
                )?;
                let staged = compose::stage_pinned_config(compose_config.dir(), &pinned_yaml)?;
                rt.build_and_push_config_image(staged.path(), &config_image)?
            }
            config::Config::Kubernetes(_) => {
                rt.build_and_push_config_image(detected.dir(), &config_image)?
            }
        };
        params.insert(
            ANT_CONFIG_IMAGE,
            container::strip_registry(&pinned_config, &registry),
        );
    }

    // No --source means the run is ephemeral. Set the flag and tell the user
    // now — after validation and the config image build have succeeded — so the
    // notice lands right before the run is sent rather than ahead of work that
    // might still fail.
    if !has_source {
        params.insert(ANT_IS_EPHEMERAL, "true");
        eprintln!(
            "Starting an ephemeral run; its findings will not be retained as historic \
             results. Pass --source to record property history across runs."
        );
    }

    let response = submit_launch(&launcher, params, settings, verbose).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else if let Some(run_id) = response.run_id.as_deref() {
        // `writedoc!` and not `println!`: a reader that stops after the run ID
        // line (`| head -1`) closes the pipe, and `println!` panics on that.
        writedoc!(
            stdio::stdout(),
            r"
                Test run started: run_id {run_id}

                Wait for the run to finish with:
                  snouty runs wait {run_id}

                Or interact with the run once it is ready:
                  snouty runs wait {run_id} --until ready
                  snouty runs search {run_id} --follow <query>
                  snouty runs events {run_id} <query>
                  snouty runs exec {run_id} <input_hash> <vtime> <script>
            "
        )?;
    } else {
        println!("Test run started: run_id (unknown)");
    }

    Ok(())
}

/// The `vcs.*` params for the git checkout that holds `dir`, in the shape the
/// Antithesis GitHub trigger action sends them. Empty outside a git checkout.
fn detect_vcs_params(dir: &Path) -> Params {
    let mut vcs = Params::new();
    let Some(checkout) = git::checkout(dir) else {
        return vcs;
    };
    if let Some(repo) = checkout
        .origin
        .as_ref()
        .and_then(|url| GitHubRepo::try_from(url).ok())
    {
        vcs.insert(VCS_REPO_TYPE, "github");
        vcs.insert(VCS_VERSION_LINK, repo.commit_url(&checkout.commit));
        vcs.insert(VCS_REPO_OWNER, repo.owner);
        vcs.insert(VCS_REPO_NAME, repo.name);
    }
    if let Some(branch) = checkout.branch {
        vcs.insert(VCS_REPO_BRANCH, branch);
    }
    vcs.insert(VCS_VERSION_ID, checkout.commit);
    vcs
}

async fn submit_launch(
    launcher: &str,
    params: Params,
    settings: &Settings,
    verbose: bool,
) -> Result<snouty::api::LaunchResponse> {
    params.validate_test_params()?;

    eprintln!(
        "\nRequesting Antithesis test run with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map())?
    );

    let api = AntithesisApi::new_for_launch(settings, verbose)?;
    api.launch_test(launcher, &params).await
}

fn debug_typed_params(args: &DebugArgs) -> Params {
    let mut params = Params::new();

    if let Some(run_id) = &args.run_id {
        params.insert(ANT_DEBUGGING_RUN_ID, run_id.as_str());
    }
    if let Some(session_id) = &args.session_id {
        params.insert(ANT_DEBUGGING_SESSION_ID, session_id.as_str());
    }
    params.insert(ANT_DEBUGGING_INPUT_HASH, args.input_hash.as_str());
    params.insert(ANT_DEBUGGING_VTIME, args.vtime.to_string());
    if let Some(title) = &args.title {
        params.insert(ANT_TEST_NAME, title.as_str());
    }
    if let Some(description) = &args.description {
        params.insert(ANT_EVENT_DESCRIPTION, description.as_str());
    }
    if let Some(recipients) = &args.recipients {
        params.insert(ANT_REPORT_RECIPIENTS, recipients.as_str());
    }

    params
}

/// The error for a `--param` that sets a key a typed flag already set.
fn param_overrides_flag(DuplicateKey(key): DuplicateKey) -> Report {
    user_error(format!(
        "invalid arguments: '{key}' cannot be overridden via --param"
    ))
}

async fn cmd_debug(
    args: DebugArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let mut params = debug_typed_params(&args);
    params
        .extend_checked(args.params.into_iter().collect())
        .map_err(param_overrides_flag)?;
    // clap takes exactly one of --run-id and --session-id, but a --param can
    // add the other.
    if params.contains_key(ANT_DEBUGGING_RUN_ID) && params.contains_key(ANT_DEBUGGING_SESSION_ID) {
        return Err(user_error(format!(
            "invalid arguments: pass only one of {ANT_DEBUGGING_RUN_ID} and \
             {ANT_DEBUGGING_SESSION_ID}"
        )));
    }
    params.validate_debugging_params()?;

    let api = AntithesisApi::new_for_launch(settings, verbose)?;
    // The launch endpoint accepts an unknown run or input hash and starts a
    // billable session, so check them first. The runs endpoints refuse a
    // password, so with one there is no way to check.
    let param = |key| params.as_map().get(key).and_then(|v| v.as_str());
    if let Some(run_id) = param(ANT_DEBUGGING_RUN_ID)
        && !api.uses_password()
    {
        let run = snouty::runs::ensure_run_debuggable(&api, run_id).await?;
        if let Some(input_hash) = param(ANT_DEBUGGING_INPUT_HASH) {
            snouty::runs::ensure_input_hash_exists(&api, &run, input_hash).await?;
        }
    }

    eprintln!(
        "\nRequesting the Antithesis multiverse debugger with params:\n{}",
        serde_json::to_string_pretty(&params.to_redacted_map())?
    );

    let response = api.launch_debugging(&params).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else if let Some(run_id) = response.run_id.as_deref() {
        println!("Debugging session started: run_id {run_id}");
        println!("\nThe session takes a few minutes to be ready.");
        // Both come from typed flags, so they are strings.
        if let (Some(input_hash), Some(vtime)) =
            (param(ANT_DEBUGGING_INPUT_HASH), param(ANT_DEBUGGING_VTIME))
        {
            println!(
                "Run a script at this moment; `runs exec` waits until the session is ready:\n  \
                 snouty runs exec {run_id} {input_hash} {vtime} '<script>'"
            );
        }
    } else {
        println!("Debugging session started: run_id (unknown)");
    }

    Ok(())
}

fn cmd_completions(shell: Shell) -> io::Result<()> {
    let mut cmd = Cli::command();
    let bin_name = cmd.get_name().to_string();
    // clap_complete panics when a write fails, so generate into memory and
    // write that, so that a closed stdout is an error and not a panic.
    let mut script = Vec::new();
    clap_complete::generate(shell, &mut cmd, bin_name, &mut script);
    stdio::stdout().write_all(&script)
}

/// Validate a requested update target against the running version.
///
/// Errors if `requested` isn't valid semver, or if it's older than `current`
/// and the downgrade wasn't forced. `current` is snouty's own
/// `CARGO_PKG_VERSION`, which is always valid semver. Semver pre-release
/// ordering applies, so e.g. `0.6.0-rc.1` is a downgrade from `0.6.0`.
fn check_update_target(requested: &str, current: &str, force: bool) -> Result<()> {
    let target = Version::parse(requested).map_err(|_| {
        user_error(format!(
            "invalid version `{requested}`: expected a semver release like 0.6.0 or 0.6.0-rc.1"
        ))
    })?;
    let current = Version::parse(current).expect("snouty's own version is always valid semver");
    if target < current && !force {
        return Err(user_error(format!(
            "{requested} is older than the installed snouty {current}; this would be a downgrade"
        ))
        .suggestion("re-run with --force to install an older version"));
    }
    Ok(())
}

fn cmd_update(args: &UpdateArgs, settings: &Settings) -> Result<()> {
    // When a specific version is requested, validate it and refuse an unforced
    // downgrade up front, before bothering to spawn the helper.
    if let Some(version) = &args.version {
        check_update_target(version, env!("CARGO_PKG_VERSION"), args.force)?;
    }

    // The --channel flag overrides the (already validated) setting.
    let channel = args.channel.unwrap_or(settings.update_channel());

    // Attempt to spawn snouty-update and wait for it to finish. An explicit
    // version is forwarded via --version; the helper installs it directly
    // (pre-releases included), so --prerelease is only needed when the
    // unstable channel picks "latest". The helper then installs the greatest
    // version across releases and pre-releases, so a release newer than every
    // pre-release still wins.
    let mut updater = Command::new("snouty-update");
    if let Some(version) = &args.version {
        updater.arg("--version").arg(version);
    } else if channel == UpdateChannel::Unstable {
        updater.arg("--prerelease");
    }
    match updater.status() {
        Ok(status) if status.success() => {
            std::process::exit(0);
        }
        Ok(status) => {
            log::error!("snouty-update failed with exit code {status}\n");
        }
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                log::warn!("snouty-update command not found\n");
            } else {
                log::error!("failed to run snouty-update: {err}\n");
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
    use color_eyre::eyre::{Report, eyre};

    fn closed(stream: stdio::Stream) -> Report {
        Report::new(io::Error::new(ErrorKind::BrokenPipe, StreamClosed(stream)))
    }

    #[test]
    fn suppress_broken_pipe_swallows_closed_stdout_and_stderr() {
        for stream in [stdio::Stream::Stdout, stdio::Stream::Stderr] {
            assert!(suppress_broken_pipe(Err(closed(stream))).is_ok());
            let wrapped = closed(stream).wrap_err("writing the table");
            assert!(suppress_broken_pipe(Err(wrapped)).is_ok());
        }
    }

    /// A broken pipe that is not stdout or stderr, such as a child's stdin
    /// that closed early, is a real error.
    #[test]
    fn suppress_broken_pipe_reports_unmarked_broken_pipe() {
        let err = io::Error::new(ErrorKind::BrokenPipe, "write to docker build stdin");
        assert!(suppress_broken_pipe(Err(Report::new(err))).is_err());
        let bare = || Report::new(io::Error::from(ErrorKind::BrokenPipe));
        assert!(suppress_broken_pipe(Err(bare())).is_err());
        let wrapped = bare().wrap_err("failed to write Dockerfile to stdin");
        assert!(suppress_broken_pipe(Err(wrapped)).is_err());
    }

    #[test]
    fn suppress_broken_pipe_passes_through_other_errors() {
        let io_err = io::Error::other("disk on fire");
        assert!(suppress_broken_pipe(Err(Report::new(io_err))).is_err());
        assert!(suppress_broken_pipe(Err(eyre!("plain error"))).is_err());
    }

    /// A broken connection during an API call must not be mistaken for a
    /// closed stdout pipe. The socket's `io::Error` sits in reqwest's source
    /// chain without a [`StreamClosed`] marker, so it is never suppressed.
    #[tokio::test]
    async fn suppress_broken_pipe_ignores_network_socket_errors() {
        // Grab a port that refuses connections by binding then dropping.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let reqwest_err = snouty::testutils::http_get(format!("http://{addr}/api/v0/runs"))
            .await
            .unwrap_err();

        // Sanity check: the socket failure really is an io::Error, buried in
        // reqwest's source chain.
        let mut source = std::error::Error::source(&reqwest_err);
        let mut found_io = false;
        while let Some(err) = source {
            found_io |= err.downcast_ref::<io::Error>().is_some();
            source = err.source();
        }
        assert!(
            found_io,
            "expected an io::Error in the reqwest source chain"
        );

        // Wrap it the way api.rs wraps communication errors.
        let report = eyre!(reqwest_err).wrap_err("failed to contact API");

        assert!(suppress_broken_pipe(Err(report)).is_err());
    }

    #[test]
    fn check_update_target_allows_upgrade() {
        assert!(check_update_target("0.6.0", "0.5.0", false).is_ok());
    }

    #[test]
    fn check_update_target_allows_reinstalling_same_version() {
        assert!(check_update_target("0.5.0", "0.5.0", false).is_ok());
    }

    #[test]
    fn check_update_target_blocks_unforced_downgrade() {
        let err = check_update_target("0.4.0", "0.5.0", false).unwrap_err();
        let rendered = format!("{err}");
        assert!(rendered.contains("downgrade"), "got: {rendered}");
    }

    #[test]
    fn check_update_target_allows_forced_downgrade() {
        assert!(check_update_target("0.4.0", "0.5.0", true).is_ok());
    }

    #[test]
    fn check_update_target_treats_prerelease_as_downgrade_of_release() {
        // Semver orders 0.6.0-rc.1 before the final 0.6.0, so installing the rc
        // over the release is a downgrade.
        let err = check_update_target("0.6.0-rc.1", "0.6.0", false).unwrap_err();
        assert!(format!("{err}").contains("downgrade"));
    }

    #[test]
    fn check_update_target_allows_prerelease_above_current() {
        assert!(check_update_target("0.6.0-rc.1", "0.5.0", false).is_ok());
    }

    #[test]
    fn check_update_target_rejects_invalid_version() {
        let err = check_update_target("not-a-version", "0.5.0", false).unwrap_err();
        assert!(format!("{err}").contains("invalid version"));
    }
}
