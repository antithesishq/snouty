use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU64;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};

use color_eyre::Section;
use color_eyre::eyre::Report;

use crate::api::{RunStatus, SEARCH_DEFAULT_LIMIT, SEARCH_MAX_LIMIT};
use crate::error::user_error;
use crate::features::{self, Feature};
use crate::help::{HelpPage, Target};
use crate::time::HumanDuration;
use crate::vtime::VTime;

/// Every `RunStatus` variant, used to enumerate valid `--status` values in
/// error output. The generated enum offers no iteration, so this array is the
/// source of truth; [`assert_run_statuses_complete`] forces an update here
/// whenever the generated enum gains a variant.
pub(crate) const ALL_RUN_STATUSES: [RunStatus; 6] = [
    RunStatus::Starting,
    RunStatus::InProgress,
    RunStatus::Completed,
    RunStatus::Cancelled,
    RunStatus::Incomplete,
    RunStatus::Unknown,
];

/// Compile-time guard: matching every variant with no wildcard arm makes this
/// fail to compile when the generated `RunStatus` gains a variant, which is
/// the cue to extend [`ALL_RUN_STATUSES`].
const fn assert_run_statuses_complete(status: RunStatus) {
    match status {
        RunStatus::Starting
        | RunStatus::InProgress
        | RunStatus::Completed
        | RunStatus::Cancelled
        | RunStatus::Incomplete
        | RunStatus::Unknown => {}
    }
}

const _: () = assert_run_statuses_complete(RunStatus::Starting);

/// clap value parser for `--status` that keeps a friendly, enumerated error
/// message (the generated `RunStatus::from_str` only says "invalid value").
pub(crate) fn parse_run_status(value: &str) -> Result<RunStatus, String> {
    value.parse::<RunStatus>().map_err(|_| {
        let valid = ALL_RUN_STATUSES.map(|s| s.to_string()).join(", ");
        format!("invalid status: '{value}'\nvalid values: {valid}")
    })
}

/// clap value parser for `runs wait --poll-interval`: a [`HumanDuration`] of
/// at least 1 minute — polling faster cannot observe a run (which takes
/// minutes to hours) any sooner, and only hammers the API.
fn parse_poll_interval(value: &str) -> Result<HumanDuration, String> {
    let interval = value.parse::<HumanDuration>().map_err(|e| e.to_string())?;
    if interval.seconds() < 60 {
        return Err("poll interval must be at least 1 minute".to_string());
    }
    Ok(interval)
}

/// clap value parser for the event-search `--limit`: 1 to
/// [`SEARCH_MAX_LIMIT`], so an out-of-range value fails before any request.
fn parse_search_limit(value: &str) -> Result<NonZeroU64, String> {
    // Text that is not a whole number gets the same error as a number out of
    // range.
    check_search_limit(value.parse().unwrap_or(0))
}

/// The range check of [`parse_search_limit`], shared with the MCP params,
/// which get the limit as a JSON number.
pub(crate) fn check_search_limit(limit: u64) -> Result<NonZeroU64, String> {
    NonZeroU64::new(limit)
        .filter(|limit| limit.get() <= SEARCH_MAX_LIMIT)
        .ok_or_else(|| format!("must be a whole number from 1 to {SEARCH_MAX_LIMIT}"))
}

#[derive(Parser)]
#[command(name = "snouty")]
#[command(about = "CLI for the Antithesis API", long_about = None)]
// SNOUTY_VERSION (from build.rs) is the crate version plus the build's git sha
// when known, so `--version` and the `version` subcommand print the same string.
#[command(version = env!("SNOUTY_VERSION"))]
pub struct Cli {
    /// Output JSON where supported (NDJSON for list/stream commands, pretty JSON otherwise)
    // High display_order so the two global flags sort to the bottom of every
    // command's option list instead of wedging between that command's own flags.
    #[arg(long, global = true, display_order = 1000)]
    pub json: bool,

    /// Log API requests to stderr (authentication tokens redacted)
    #[arg(long, global = true, display_order = 1001)]
    pub verbose: bool,

    /// Path to the snouty settings file (default: ./.snouty.toml; overrides SNOUTY_SETTINGS_PATH)
    #[arg(long, global = true, display_order = 1002)]
    pub settings: Option<std::path::PathBuf>,

    /// Settings profile to select (overrides ANTITHESIS_PROFILE)
    #[arg(long, global = true, value_parser = validate_non_empty, display_order = 1003)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Launch a test run
    #[command(long_about = HelpPage::Launch.text(Target::Cli))]
    Launch(LaunchArgs),

    /// Deprecated: use `launch` instead
    #[command(hide = true)]
    Run(LaunchArgs),

    /// Interact with test runs
    #[command(
        long_about = HelpPage::Runs.text(Target::Cli),
        subcommand_required = false
    )]
    Runs {
        #[command(subcommand)]
        command: Option<RunsCommands>,
    },

    /// Launch a debugging session
    #[command(long_about = HelpPage::Debug.text(Target::Cli))]
    Debug(DebugArgs),

    /// Output shell completions
    #[command(long_about = HelpPage::Completions.text(Target::Cli))]
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Validate local Antithesis setup
    #[command(long_about = HelpPage::Validate.text(Target::Cli))]
    Validate(ValidateArgs),

    /// Check environment configuration
    #[command(long_about = HelpPage::Doctor.text(Target::Cli))]
    Doctor(DoctorArgs),

    /// Print version information
    Version,

    /// Check for and install updates
    #[command(long_about = HelpPage::Update.text(Target::Cli))]
    Update(UpdateArgs),

    /// Search Antithesis documentation
    #[command(long_about = HelpPage::Docs.text(Target::Cli))]
    Docs {
        /// Don't check for documentation updates
        #[arg(long)]
        offline: bool,

        #[command(subcommand)]
        command: DocsCommands,
    },

    /// Sign in and store your snouty configuration
    #[command(long_about = HelpPage::Login.text(Target::Cli))]
    Login {
        #[arg(long, value_parser = validate_non_empty)]
        tenant: Option<String>,

        #[arg(long, value_parser = validate_non_empty)]
        repository: Option<String>,
    },

    /// Serve snouty's run, docs and doctor commands to AI agents over MCP
    // Gated: hidden while its feature is off, and refused by
    // [`gated_command_error`].
    #[command(
        hide = !features::is_enabled(Feature::Mcp),
        long_about = HelpPage::Mcp.text(Target::Cli)
    )]
    Mcp(McpArgs),
}

/// The port `snouty mcp` listens on when `--port` names none.
pub const DEFAULT_MCP_PORT: u16 = 8765;

#[derive(Args, Debug)]
pub struct McpArgs {
    /// Serve one client over stdin and stdout instead of HTTP
    #[arg(long, conflicts_with_all = ["host", "port", "allowed_hosts"])]
    pub stdio: bool,

    /// IP address to listen on
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub host: IpAddr,

    /// Port to listen on (0 picks a free port)
    #[arg(long, default_value_t = DEFAULT_MCP_PORT)]
    pub port: u16,

    /// Also accept requests whose Host header is this value (repeatable).
    /// Without a port, any port matches.
    #[arg(long = "allowed-host", value_name = "HOST[:PORT]")]
    pub allowed_hosts: Vec<AllowedHost>,
}

/// A Host header value that `snouty mcp` accepts: a host name or IP address,
/// with an optional port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedHost(http::uri::Authority);

impl FromStr for AllowedHost {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let authority: http::uri::Authority = value
            .parse()
            .map_err(|e| format!("not a valid host: {e}"))?;
        // A Host header has no user info, so an entry with one never matches.
        if authority.as_str().contains('@') {
            return Err("a host must not contain '@'".to_owned());
        }
        if authority.host().is_empty() {
            return Err("the host name is empty".to_owned());
        }
        // rmcp lets an entry with no port match every port, so a port that
        // is empty or not a u16 must not become no port.
        let has_port = authority.as_str().len() > authority.host().len();
        if has_port && authority.port_u16().is_none() {
            return Err("the port must be a number from 0 to 65535".to_owned());
        }
        Ok(AllowedHost(authority))
    }
}

impl fmt::Display for AllowedHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub(crate) fn validate_non_empty(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("Value may not be empty or whitespace".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

/// The number of results `docs search` returns when `--limit` names none.
pub const DEFAULT_DOCS_SEARCH_LIMIT: usize = 10;

#[derive(Subcommand)]
pub enum DocsCommands {
    /// Search the documentation
    #[command(long_about = HelpPage::DocsSearch.text(Target::Cli))]
    Search {
        /// Print only matching page paths, one per line
        #[arg(short = 'l', long)]
        list: bool,

        /// Maximum number of results to return
        #[arg(short = 'n', long, default_value_t = DEFAULT_DOCS_SEARCH_LIMIT)]
        limit: usize,

        /// Treat the query as a raw FTS5 expression (AND/OR/NOT/NEAR, "phrases",
        /// title: filters, prefix*) instead of literal text
        #[arg(short = 'm', long = "match")]
        match_mode: bool,

        /// Search query
        query: Vec<String>,
    },
    /// Print a tree of documentation paths
    #[command(long_about = HelpPage::DocsTree.text(Target::Cli))]
    Tree {
        /// Limit output to nodes at this depth or shallower
        #[arg(short = 'd', long)]
        depth: Option<std::num::NonZeroUsize>,

        /// Optional case-insensitive filter applied to page paths and titles
        filter: Option<String>,
    },

    /// Show full contents of a documentation page
    #[command(long_about = HelpPage::DocsShow.text(Target::Cli))]
    Show {
        /// Page path (e.g. "getting_started/overview")
        path: String,
    },

    /// Print the path to the cached SQLite database
    #[command(long_about = HelpPage::DocsSqlite.text(Target::Cli))]
    Sqlite,
}

#[derive(Args)]
pub struct UpdateArgs {
    /// Release to install (e.g. 0.6.0 or 0.6.0-rc.1). Defaults to the latest release.
    pub version: Option<String>,

    /// Install the requested version even if it is older than the current one (a downgrade)
    #[arg(long)]
    pub force: bool,

    /// Update channel to use, overriding the `update_channel` setting
    #[arg(long, value_enum)]
    pub channel: Option<UpdateChannel>,
}

/// Which releases `snouty update` considers when no explicit version is given.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, ValueEnum)]
pub enum UpdateChannel {
    /// Install the latest release
    #[default]
    Stable,
    /// Also consider pre-releases, but prefer the latest release when it is newer
    Unstable,
}

impl UpdateChannel {
    pub const STABLE: &'static str = "stable";
    pub const UNSTABLE: &'static str = "unstable";

    pub fn as_str(self) -> &'static str {
        match self {
            UpdateChannel::Stable => Self::STABLE,
            UpdateChannel::Unstable => Self::UNSTABLE,
        }
    }
}

impl std::str::FromStr for UpdateChannel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            Self::STABLE => Ok(UpdateChannel::Stable),
            Self::UNSTABLE => Ok(UpdateChannel::Unstable),
            other => Err(format!(
                "expected `{}` or `{}`, got `{other}`",
                Self::STABLE,
                Self::UNSTABLE
            )),
        }
    }
}

#[derive(Args)]
pub struct ValidateArgs {
    /// Path to config directory containing either docker-compose.yaml or a
    /// manifests/ subdirectory (Kubernetes manifests).
    pub config: std::path::PathBuf,

    /// Maximum seconds to wait for containers to start and reach setup-complete
    #[arg(long, default_value = "120")]
    pub timeout: u64,

    /// Leave containers running after validation for manual inspection
    #[arg(long)]
    pub keep_running: bool,

    /// Warn instead of failing when docker-compose.yaml renders differently in
    /// the hermetic Antithesis environment than it does on this machine
    #[arg(long)]
    pub allow_compose_divergence: bool,
}

#[derive(Args)]
pub struct DoctorArgs {
    /// Skip the network check (don't contact the Antithesis API for versions)
    #[arg(long)]
    pub offline: bool,
}

#[derive(Args)]
pub struct LaunchArgs {
    /// Webhook endpoint name (e.g., basic_test, basic_k8s_test)
    #[arg(short, long)]
    pub webhook: String,

    /// Local config dir (docker-compose.yaml or a manifests/ subdir), auto-built
    /// and pushed as the config image. Compose service images must already exist
    /// locally — snouty never pulls.
    #[arg(short, long, conflicts_with = "config_image")]
    pub config: Option<std::path::PathBuf>,

    /// Pre-built config image reference (e.g., us-central1-docker.pkg.dev/proj/repo/config:latest)
    #[arg(long)]
    pub config_image: Option<String>,

    /// Test name
    #[arg(long)]
    pub test_name: Option<String>,

    /// Test description
    #[arg(long)]
    pub description: Option<String>,

    /// Test duration in minutes, or h/m units (e.g. 90m, 2h, 1h30m)
    // `HumanDuration: FromStr` gives clap the parser; we send it to the API as
    // `.minutes().to_string()`, the (possibly fractional) minute count it wants.
    #[arg(long)]
    pub duration: Option<HumanDuration>,

    /// Mark the test run as ephemeral. Ephemeral runs will not appear in future reports as a historic result.
    #[arg(long)]
    pub ephemeral: bool,

    /// Identifier that groups property history in reports — runs sharing a
    /// --source share history (e.g. per-branch)
    #[arg(long)]
    pub source: Option<String>,

    /// Report recipients (semicolon-delimited email addresses)
    #[arg(long)]
    pub recipients: Option<String>,

    /// Suppress log lines matching this RE2 pattern during fuzzing. Matching is
    /// unanchored and case-sensitive by default (use `(?i)` for case-insensitive).
    /// Suppressed lines stay available in multiverse debugging. RE2 syntax: no
    /// lookahead/lookbehind or backreferences. Max 1023 bytes. Requires tenant
    /// release 59 or newer.
    #[arg(long)]
    pub filter_logs_matching: Option<String>,

    /// Extra parameters as key=value pairs (repeatable)
    #[arg(long = "param")]
    pub params: Vec<String>,
}

#[derive(Args)]
pub struct DebugArgs {
    /// Read parameters from stdin (JSON)
    #[arg(long)]
    pub stdin: bool,

    /// Run ID of the test run to debug (preferred; mutually exclusive with --session-id)
    #[arg(long)]
    pub run_id: Option<String>,

    /// Session ID of the test run to debug (mutually exclusive with --run-id)
    #[arg(long)]
    pub session_id: Option<String>,

    /// Input hash identifying the moment to debug
    #[arg(long, allow_hyphen_values = true)]
    pub input_hash: Option<String>,

    /// Virtual time identifying the moment to debug
    #[arg(long)]
    pub vtime: Option<VTime>,

    /// Debugging session description
    #[arg(long)]
    pub description: Option<String>,

    /// Report recipients (semicolon-delimited email addresses)
    #[arg(long)]
    pub recipients: Option<String>,
}

#[derive(Subcommand)]
pub enum RunsCommands {
    /// List all runs
    #[command(long_about = HelpPage::RunsList.text(Target::Cli))]
    List(RunsListArgs),

    /// Show details of a specific run
    #[command(long_about = HelpPage::RunsShow.text(Target::Cli))]
    Show {
        /// Run ID
        run_id: String,

        /// Open the run's triage report in a browser instead of printing details
        #[arg(short = 'w', long)]
        web: bool,
    },

    /// Wait for a run to reach a terminal state
    #[command(long_about = HelpPage::RunsWait.text(Target::Cli))]
    Wait {
        /// Run ID
        run_id: String,

        /// Time between status checks, in minutes or h/m/s units (e.g. 90s;
        /// minimum 1 minute)
        #[arg(long, default_value = "1m", value_parser = parse_poll_interval)]
        poll_interval: HumanDuration,

        /// Give up after this long (minutes, or h/m/s units, e.g. 2h);
        /// without it the wait is unbounded
        #[arg(long)]
        timeout: Option<HumanDuration>,
    },

    /// List property results for a run
    #[command(long_about = HelpPage::RunsProperties.text(Target::Cli))]
    Properties {
        /// Run ID
        run_id: String,

        /// Show only passing properties
        #[arg(long, conflicts_with = "failing")]
        passing: bool,

        /// Show only failing properties
        #[arg(long)]
        failing: bool,

        /// Only properties whose name contains this substring (case-insensitive)
        #[arg(long)]
        name: Option<String>,

        /// Only properties whose group contains this substring (case-insensitive)
        #[arg(long)]
        group: Option<String>,

        /// Expand each matching property into its examples / counter-example
        /// moments, instead of the summary table
        #[arg(short = 'd', long)]
        detail: bool,
    },

    /// Stream build logs for a run
    #[command(long_about = HelpPage::RunsBuildLogs.text(Target::Cli))]
    BuildLogs {
        /// Run ID
        run_id: String,
    },

    /// Stream moment logs for a run
    #[command(long_about = HelpPage::RunsLogs.text(Target::Cli))]
    Logs {
        /// Run ID
        run_id: String,

        /// Input hash identifying the timeline to stream
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// Virtual time of the moment to end the stream at; omit it to stream to the timeline's current end
        // Typed, so a malformed vtime is rejected by clap instead of by the
        // server. `allow_hyphen_values` is kept here (unlike `runs exec`),
        // because this command has always accepted a hyphen-led vtime.
        #[arg(allow_hyphen_values = true)]
        vtime: Option<VTime>,

        /// Start from this virtual time instead of the root
        #[arg(long, allow_hyphen_values = true)]
        begin_vtime: Option<VTime>,

        #[command(flatten)]
        render: EventOutputArgs,
    },

    /// Execute a command in a run's live session
    // Gated behind the `runs-exec` feature. `hide` is an expression, so the
    // decision is made when the command is built — the feature comes from the
    // environment, which needs no parse to read. Hiding only keeps it out of
    // `--help`; invoking it while disabled is refused by
    // [`gated_command_error`].
    #[command(
        hide = !features::is_enabled(Feature::RunsExec),
        long_about = HelpPage::RunsExec.text(Target::Cli)
    )]
    Exec {
        /// Run ID
        run_id: String,

        /// Input hash of the moment to execute at (with VTIME, picks the timeline)
        // A moment's input hash is routinely negative.
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// Virtual time of the moment to execute at
        // Parsed by `VTime` so a malformed value is rejected by clap, before
        // any API call. No `allow_hyphen_values`: a vtime is seconds since the
        // run began and is never negative, and accepting hyphen-led values
        // here would swallow a misplaced `--timeout` as the vtime.
        vtime: VTime,

        /// Bash script to execute; omit it to read the script from stdin
        script: Option<String>,

        /// Maximum seconds the server waits for the script to exit before
        /// reporting a timeout
        // The API's own default is 30 with a minimum of 0 and no maximum. A
        // 0-second timeout can only ever time out, so the floor here is 1; the
        // ceiling is left to the server rather than guessed at.
        #[arg(long, default_value_t = DEFAULT_EXEC_TIMEOUT_SECS, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: u64,
    },

    /// Search events in a run
    #[command(long_about = HelpPage::RunsEvents.text(Target::Cli))]
    Events {
        /// Run ID
        run_id: String,

        /// Substring to search for (repeatable; all matches must be present)
        #[arg(short = 'm', long = "match")]
        matches: Vec<String>,

        /// Maximum number of events to print, at most 999. Raise it to make a
        /// search more exhaustive.
        #[arg(short = 'n', long, default_value_t = SEARCH_DEFAULT_LIMIT, value_parser = parse_search_limit)]
        limit: NonZeroU64,

        /// Substrings to match, as a positional alias for `-m` (all must match).
        /// At least one needle (via `-m` or here) is required.
        query: Vec<String>,

        #[command(flatten)]
        render: EventOutputArgs,
    },

    /// Query events with the event-set DSL
    #[command(long_about = HelpPage::RunsSearch.text(Target::Cli))]
    Search(RunsSearchArgs),
}

/// The seconds `runs exec` waits for the script when `--timeout` names none.
pub const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 30;

#[derive(Args)]
pub struct RunsSearchArgs {
    /// Run ID
    pub run_id: String,

    /// Event-set DSL query
    pub query: String,

    /// Maximum number of events to print (default 50, maximum 999)
    #[arg(short = 'n', long, value_parser = parse_search_limit)]
    pub limit: Option<NonZeroU64>,

    /// Keep the connection open and print new matches as they arrive
    /// (the limit still caps the total)
    #[arg(short = 'f', long)]
    pub follow: bool,

    /// Check the query's syntax without running it
    #[arg(long, conflicts_with = "follow")]
    pub check: bool,

    #[command(flatten)]
    pub render: EventOutputArgs,
}

/// The output-depth flags every event-stream command (`runs logs`,
/// `runs events`, `runs search`) shares. One definition keeps the flags, the
/// help text, and the raw/detail conflict from drifting between commands.
#[derive(Args)]
pub struct EventOutputArgs {
    /// Print the server's events untouched, one JSON object per line,
    /// skipping snouty's normalization (vtime, and fault annotation on
    /// `runs logs`); requires --json
    #[arg(short = 'r', long)]
    pub raw: bool,

    /// Detailed rendering: a full-width vtime on every line, source
    /// locations, and each event's attached details JSON
    #[arg(short = 'd', long, conflicts_with = "raw")]
    pub detail: bool,
}

/// The number of runs `runs list` prints when `--limit` names none.
pub const DEFAULT_RUNS_LIMIT: NonZeroU64 = NonZeroU64::new(10).unwrap();

#[derive(Args)]
pub struct RunsListArgs {
    /// Filter by status (starting, in_progress, completed, cancelled, incomplete, unknown)
    #[arg(short, long, value_parser = parse_run_status)]
    pub status: Option<RunStatus>,

    /// Filter by launcher name
    #[arg(short, long)]
    pub launcher: Option<String>,

    /// Only show runs created after this timestamp (ISO 8601)
    #[arg(long)]
    pub created_after: Option<DateTime<Utc>>,

    /// Only show runs created before this timestamp (ISO 8601)
    #[arg(long)]
    pub created_before: Option<DateTime<Utc>>,

    /// Maximum number of runs to display
    #[arg(short = 'n', long, default_value_t = DEFAULT_RUNS_LIMIT)]
    pub limit: NonZeroU64,

    /// Show a detailed key-value block per run, including the full description
    #[arg(short, long)]
    pub detail: bool,
}

impl Default for RunsListArgs {
    fn default() -> Self {
        Self {
            status: None,
            launcher: None,
            created_after: None,
            created_before: None,
            limit: DEFAULT_RUNS_LIMIT,
            detail: false,
        }
    }
}

/// The error for invoking a gated command whose feature is off.
///
/// This is the half of the gate that hiding cannot do: a hidden subcommand is
/// still callable. Anyone who types the command already knows it exists, so
/// the error says what is actually wrong and how to fix it, rather than
/// pretending the command is not there. `enabled` names the features that are
/// on — the caller passes them so the decision is testable without touching
/// the environment.
pub fn gated_command_error(command: &Commands, enabled: &[Feature]) -> Option<Report> {
    let (feature, path) = match command {
        Commands::Runs {
            command: Some(RunsCommands::Exec { .. }),
        } => (Feature::RunsExec, "snouty runs exec"),
        Commands::Mcp(_) => (Feature::Mcp, "snouty mcp"),
        _ => return None,
    };
    if enabled.contains(&feature) {
        return None;
    }

    Some(
        user_error(format!(
            "`{path}` is an unstable feature and is not enabled"
        ))
        .note(format!(
            "enable it by setting {}={}",
            features::UNSTABLE_FEATURES_VAR_NAME,
            feature
        ))
        .note("an unstable feature can change or go away in any release")
        .suggestion(format!("run `{path} --help` for what it does")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("args should parse")
    }

    #[test]
    fn update_channel_parses_its_named_values() {
        assert_eq!(
            UpdateChannel::STABLE.parse::<UpdateChannel>().unwrap(),
            UpdateChannel::Stable
        );
        assert_eq!(
            UpdateChannel::UNSTABLE.parse::<UpdateChannel>().unwrap(),
            UpdateChannel::Unstable
        );
    }

    #[test]
    fn update_channel_rejects_unknown_values() {
        let err = "nightly".parse::<UpdateChannel>().unwrap_err();
        assert_eq!(err, "expected `stable` or `unstable`, got `nightly`");
    }

    #[test]
    fn a_gated_off_command_is_refused_and_an_enabled_one_runs() {
        let exec = parse(&["snouty", "runs", "exec", "RUN", "1", "2.0", "true"]).command;

        // Off: refused with a message that says what is wrong and how to fix
        // it. Whoever typed the command knows it exists, so pretending it does
        // not would only waste their time.
        let err = gated_command_error(&exec, &[]).expect("a gated-off command is refused");
        let rendered = format!("{err:?}");
        assert!(rendered.contains("`snouty runs exec`"), "{rendered}");
        assert!(rendered.contains("unstable feature"), "{rendered}");
        assert!(
            rendered.contains("SNOUTY_UNSTABLE_FEATURES=runs-exec"),
            "{rendered}"
        );
        assert!(rendered.contains("snouty runs exec --help"), "{rendered}");

        // On: allowed through.
        assert!(gated_command_error(&exec, &[Feature::RunsExec]).is_none());
        // An unrelated feature does not enable it.
        assert!(gated_command_error(&exec, &[Feature::Unknown("other".to_string())]).is_some());

        let mcp = parse(&["snouty", "mcp", "--stdio"]).command;
        let err = gated_command_error(&mcp, &[]).expect("mcp is gated");
        assert!(format!("{err:?}").contains("SNOUTY_UNSTABLE_FEATURES=mcp"));
        assert!(gated_command_error(&mcp, &[Feature::Mcp]).is_none());

        // Sibling subcommands are never gated.
        for args in [
            &["snouty", "runs", "logs", "RUN", "1", "2.0"][..],
            &["snouty", "runs", "search", "RUN", "q"][..],
            &["snouty", "runs"][..],
        ] {
            assert!(gated_command_error(&parse(args).command, &[]).is_none());
        }
    }

    #[test]
    fn duration_flag_parses_into_human_duration() {
        // Parsing/validation lives in `crate::time`; here we just confirm clap
        // wires `--duration` through `HumanDuration: FromStr`.
        let cli = parse(&[
            "snouty",
            "launch",
            "-w",
            "basic_test",
            "--duration",
            "1h30m",
        ]);
        let Commands::Launch(args) = cli.command else {
            panic!("expected launch command");
        };
        assert_eq!(args.duration.unwrap().minutes(), 90.0);
    }

    #[test]
    fn duration_flag_rejects_invalid_value() {
        // `.err()` avoids requiring `Cli: Debug` (which `unwrap_err` would).
        let err =
            Cli::try_parse_from(["snouty", "launch", "-w", "basic_test", "--duration", "1.5h"])
                .err()
                .expect("invalid duration should fail to parse")
                .to_string();
        assert!(err.contains("--duration"), "got: {err}");
        assert!(err.contains("number of minutes"), "got: {err}");
    }

    // The positional `input_hash`/`vtime` and `--begin-vtime` must all
    // accept hyphen-led values: moment coordinates are routinely negative
    // (e.g. `snouty runs logs RUN -123 -2.0`).
    #[test]
    fn logs_accepts_negative_begin_vtime() {
        let cli = parse(&[
            "snouty",
            "runs",
            "logs",
            "RUN",
            "-123",
            "-2.0",
            "--begin-vtime",
            "-2.0",
        ]);
        let Commands::Runs {
            command:
                Some(RunsCommands::Logs {
                    input_hash,
                    vtime,
                    begin_vtime,
                    ..
                }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert_eq!(input_hash, "-123");
        assert_eq!(vtime, Some("-2.0".parse::<VTime>().unwrap()));
        assert_eq!(begin_vtime, Some("-2.0".parse::<VTime>().unwrap()));
    }

    // VTIME is optional; without it the stream runs to the branch's current
    // end.
    #[test]
    fn logs_accepts_a_missing_vtime() {
        let cli = parse(&["snouty", "runs", "logs", "RUN", "-123"]);
        let Commands::Runs {
            command: Some(RunsCommands::Logs {
                input_hash, vtime, ..
            }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert_eq!(input_hash, "-123");
        assert_eq!(vtime, None);
    }

    // `-r` is the short form of `--raw`; note `-r` must not swallow the
    // hyphen-led positionals that follow it.
    #[test]
    fn logs_accepts_raw_short_flag() {
        let cli = parse(&["snouty", "runs", "logs", "-r", "RUN", "-123", "-2.0"]);
        let Commands::Runs {
            command: Some(RunsCommands::Logs { render, vtime, .. }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert!(render.raw);
        assert_eq!(vtime, Some("-2.0".parse::<VTime>().unwrap()));

        let cli = parse(&["snouty", "runs", "logs", "RUN", "-123", "-2.0"]);
        let Commands::Runs {
            command: Some(RunsCommands::Logs { render, .. }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert!(!render.raw);
    }

    // `runs events` accepts both the documented `-m/--match` form and a
    // backward-compatible trailing positional query; the two are merged.
    #[test]
    fn events_accepts_match_and_positional_query() {
        let cli = parse(&["snouty", "runs", "events", "RUN", "-m", "request"]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { matches, query, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert_eq!(matches, vec!["request".to_string()]);
        assert!(query.is_empty());

        let cli = parse(&["snouty", "runs", "events", "RUN", "request", "slow"]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { matches, query, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert!(matches.is_empty());
        assert_eq!(query, vec!["request".to_string(), "slow".to_string()]);
    }

    // `runs events --limit` defaults to 50; the server enforces the ceiling.
    #[test]
    fn events_limit_defaults_and_rejects_zero() {
        let cli = parse(&["snouty", "runs", "events", "RUN", "-m", "request"]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { limit, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert_eq!(limit, SEARCH_DEFAULT_LIMIT);

        let cli = parse(&[
            "snouty", "runs", "events", "RUN", "-m", "x", "--limit", "998",
        ]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { limit, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert_eq!(limit.get(), 998);

        // The limit is a `NonZeroU64`, so clap rejects 0 up front with a plain
        // message.
        let parsed = Cli::try_parse_from(["snouty", "runs", "events", "RUN", "-n", "0"]);
        assert!(parsed.is_err(), "expected --limit 0 to be rejected");
    }

    // `runs search --limit` stays unset unless given, and rejects 0.
    #[test]
    fn search_limit_rejects_zero() {
        let cli = parse(&[
            "snouty", "runs", "search", "RUN", "q", "--follow", "--limit", "998",
        ]);
        let Commands::Runs {
            command: Some(RunsCommands::Search(args)),
        } = cli.command
        else {
            panic!("expected `runs search`");
        };
        assert_eq!(args.limit.map(NonZeroU64::get), Some(998));

        let parsed = Cli::try_parse_from(["snouty", "runs", "search", "RUN", "q", "-n", "0"]);
        assert!(parsed.is_err(), "expected --limit 0 to be rejected");
    }

    // Both event-search commands accept 1 to 999, the endpoint's own range.
    #[test]
    fn search_limit_accepts_the_endpoint_range_only() {
        for (args, ok) in [
            (
                &["snouty", "runs", "search", "RUN", "q", "-n", "999"][..],
                true,
            ),
            (
                &["snouty", "runs", "search", "RUN", "q", "-n", "1000"][..],
                false,
            ),
            (
                &["snouty", "runs", "events", "RUN", "x", "-n", "999"][..],
                true,
            ),
            (
                &["snouty", "runs", "events", "RUN", "x", "-n", "1000"][..],
                false,
            ),
            (
                &["snouty", "runs", "events", "RUN", "x", "-n", "abc"][..],
                false,
            ),
        ] {
            assert_eq!(Cli::try_parse_from(args).is_ok(), ok, "{args:?}");
        }
    }

    // `runs search` takes the run id and one raw DSL query positionally; the
    // mode switches default off and the limit stays unset unless given.
    #[test]
    fn search_parses_query_and_defaults() {
        let cli = parse(&[
            "snouty",
            "runs",
            "search",
            "RUN",
            r#"contains({output_text: "raft"})"#,
        ]);
        let Commands::Runs {
            command: Some(RunsCommands::Search(args)),
        } = cli.command
        else {
            panic!("expected `runs search`");
        };
        assert_eq!(args.run_id, "RUN");
        assert_eq!(args.query, r#"contains({output_text: "raft"})"#);
        assert_eq!(args.limit, None);
        assert!(!args.follow && !args.check);

        let cli = parse(&["snouty", "runs", "search", "RUN", "q", "-n", "7", "-f"]);
        let Commands::Runs {
            command: Some(RunsCommands::Search(args)),
        } = cli.command
        else {
            panic!("expected `runs search`");
        };
        assert_eq!(args.limit.map(NonZeroU64::get), Some(7));
        assert!(args.follow);
    }

    // The two mode switches pick different response modes, so clap rejects
    // the pairing rather than letting the server precedence rules silently
    // ignore one of them.
    #[test]
    fn search_mode_switches_conflict() {
        let parsed = Cli::try_parse_from([
            "snouty", "runs", "search", "RUN", "q", "--check", "--follow",
        ]);
        assert!(parsed.is_err(), "expected --check --follow to conflict");
    }

    #[hegel::test]
    fn allowed_host_round_trips_and_has_no_user_info(tc: hegel::TestCase) {
        let value = tc.draw(hegel::generators::text().alphabet("ab1.:@[]-"));
        if let Ok(host) = value.parse::<AllowedHost>() {
            assert!(!value.contains('@'), "accepted {value:?}");
            assert_eq!(host.to_string(), value);
            assert_eq!(host.to_string().parse::<AllowedHost>(), Ok(host));
        }
    }

    #[test]
    fn allowed_host_takes_a_name_with_an_optional_port() {
        for value in [
            "myhost.example",
            "myhost.example:8765",
            "[::1]:8765",
            "10.0.0.1",
        ] {
            assert!(value.parse::<AllowedHost>().is_ok(), "{value}");
        }
        for value in [
            "",
            "user@myhost.example",
            "http://myhost.example",
            ":8765",
            "myhost:99999",
            "myhost:",
        ] {
            assert!(value.parse::<AllowedHost>().is_err(), "{value}");
        }
    }

    // The help names every verb in [`event_set_dsl::VERBS`] and stays wrapped,
    // for the CLI and for tool_help: clap prints long_about verbatim, so an
    // over-long line sticks out of the ~78-column help text.
    #[test]
    fn search_long_about_names_every_verb_and_wraps() {
        for target in [Target::Cli, Target::Mcp] {
            let about = HelpPage::RunsSearch.text(target);
            for verb in crate::event_set_dsl::VERBS {
                assert!(about.contains(verb), "{target:?}: missing verb {verb}");
            }
            for line in about.lines() {
                assert!(line.len() <= 78, "{target:?}: over-long help line: {line}");
            }
        }
    }

    /// `--json` is a global flag, so every long help says what the command
    /// does with it — prints JSON, or ignores the flag. The commands that
    /// warn "--json has no effect" at runtime are the exception: their help
    /// never raises the subject.
    #[test]
    fn every_long_help_says_what_json_does() {
        const NO_JSON: [&str; 6] = [
            "validate",
            "completions",
            "version",
            "update",
            "login",
            "mcp",
        ];

        fn walk(command: &clap::Command) {
            for sub in command.get_subcommands() {
                if !NO_JSON.contains(&sub.get_name())
                    && let Some(about) = sub.get_long_about()
                {
                    let about = about.to_string();
                    assert!(
                        about.contains("--json"),
                        "`{}` long help says nothing about --json",
                        sub.get_name()
                    );
                }
                walk(sub);
            }
        }

        walk(&<Cli as clap::CommandFactory>::command());
    }
}
