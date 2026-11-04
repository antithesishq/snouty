// The doc comments here are clap help text, where backticks print literally.
#![allow(clippy::doc_markdown)]

use std::ffi::{OsStr, OsString};
use std::io::Write;
use std::num::NonZeroU64;
use std::path::Path;

use chrono::{DateTime, Utc};
use clap::error::ErrorKind;
use clap::{ArgGroup, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};

use crate::api::{
    FaultTolerance, PerformanceTier, RunStatus, SearchLimit, SourceRunId, SourceSessionId,
};
use color_eyre::Section;
use color_eyre::eyre::Report;

use crate::docker_credential::HELPER_NAME;
use crate::error::user_error;
use crate::features::{self, Feature};
use crate::help::{self, Attach};
use crate::params::MAX_SETUP_TIMEOUT_MINUTES;
use crate::render::{self, CodeSpans};
use crate::runs::usage::Rollup;
use crate::stdio;
use crate::time::{
    BareUnit, DURATION_EXAMPLES, HumanDuration, duration_error, duration_help, parse_timestamp_arg,
};
use crate::vtime::VTime;

pub(crate) mod tree;

/// The generated `RunStatus` offers no iteration, so this array lists its
/// variants for clap. A new variant fails the build in `to_possible_value`,
/// which is the cue to add it here too.
static RUN_STATUSES: [RunStatus; 6] = [
    RunStatus::Starting,
    RunStatus::InProgress,
    RunStatus::Completed,
    RunStatus::Cancelled,
    RunStatus::Incomplete,
    RunStatus::Unknown,
];

impl ValueEnum for RunStatus {
    fn value_variants<'a>() -> &'a [Self] {
        &RUN_STATUSES
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        let name = match self {
            RunStatus::Starting => "starting",
            RunStatus::InProgress => "in_progress",
            RunStatus::Completed => "completed",
            RunStatus::Cancelled => "cancelled",
            RunStatus::Incomplete => "incomplete",
            RunStatus::Unknown => "unknown",
            RunStatus::Other(_) => return None,
        };
        Some(clap::builder::PossibleValue::new(name))
    }
}

/// A `key=value` argument, such as `--param`. The value may contain `=`:
/// only the first `=` splits.
#[derive(Debug, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub enum ParseKVPairError {
    MissingEquals,
    EmptyKey,
}

impl std::fmt::Display for ParseKVPairError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ParseKVPairError::MissingEquals => "expected key=value",
            ParseKVPairError::EmptyKey => "the key is empty",
        })
    }
}

impl std::error::Error for ParseKVPairError {}

impl std::str::FromStr for KVPair {
    type Err = ParseKVPairError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (key, value) = s.split_once('=').ok_or(ParseKVPairError::MissingEquals)?;
        if key.is_empty() {
            return Err(ParseKVPairError::EmptyKey);
        }
        Ok(Self {
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }
}

/// A bare number counts `bare` units and prints a deprecation warning;
/// [`BareUnit::Refused`] makes it an error. `check` must accept each of
/// `examples`, which the parse error shows, as does [`DurationFlag::help`].
struct DurationFlag {
    name: &'static str,
    bare: BareUnit,
    examples: &'static [&'static str],
    check: fn(HumanDuration) -> Result<(), String>,
}

impl DurationFlag {
    fn help(&self, about: &str) -> String {
        format!("{about} {}", duration_help(self.examples))
    }

    fn parse(&self, value: &str) -> Result<HumanDuration, String> {
        let (duration, was_bare) = HumanDuration::parse_flag(value, self.bare)
            .map_err(|_| duration_error(self.examples))?;
        (self.check)(duration)?;
        if was_bare {
            eprintln!("{}", bare_number_warning(self.name, duration));
        }
        Ok(duration)
    }
}

fn bare_number_warning(flag: &str, duration: HumanDuration) -> String {
    format!("warning: a bare number for {flag} is deprecated; write {duration}")
}

fn require(ok: bool, message: &str) -> Result<(), String> {
    ok.then_some(()).ok_or_else(|| message.to_string())
}

/// Refuses a duration outside `minutes`, with `range_message`, or one that is
/// not a whole number of minutes.
fn require_whole_minutes(
    duration: HumanDuration,
    minutes: std::ops::RangeInclusive<u64>,
    range_message: &str,
) -> Result<(), String> {
    let seconds = duration.seconds();
    require(
        (minutes.start() * 60..=minutes.end() * 60).contains(&seconds),
        range_message,
    )?;
    require(
        seconds.is_multiple_of(60),
        "must be a whole number of minutes",
    )
}

/// A run takes minutes to hours, so polling more often than once a minute
/// sees nothing sooner and only loads the API.
const POLL_INTERVAL: DurationFlag = DurationFlag {
    name: "--poll-interval",
    bare: BareUnit::Minutes,
    examples: DURATION_EXAMPLES,
    check: |interval| {
        require(
            interval.seconds() >= 60,
            "poll interval must be at least 1 minute",
        )
    },
};

const WAIT_TIMEOUT: DurationFlag = DurationFlag {
    name: "--timeout",
    bare: BareUnit::Minutes,
    examples: DURATION_EXAMPLES,
    check: |_| Ok(()),
};

/// At least 1 minute, so that a typo such as `0` fails before it starts a
/// run.
const LAUNCH_DURATION: DurationFlag = DurationFlag {
    name: "--duration",
    bare: BareUnit::Minutes,
    examples: DURATION_EXAMPLES,
    check: |duration| require(duration.seconds() >= 60, "must be at least 1 minute"),
};

/// Refuses a bare number, because a reader could expect seconds.
const SETUP_TIMEOUT: DurationFlag = DurationFlag {
    name: "--setup-timeout",
    bare: BareUnit::Refused,
    examples: &["10m", "30m"],
    check: |timeout| {
        require_whole_minutes(
            timeout,
            1..=MAX_SETUP_TIMEOUT_MINUTES,
            &format!("must be from 1 to {MAX_SETUP_TIMEOUT_MINUTES} minutes"),
        )
    },
};

/// The range that the API accepts for `antithesis.debugging.duration`, a
/// whole number of minutes.
const DEBUG_DURATION: DurationFlag = DurationFlag {
    name: "--duration",
    bare: BareUnit::Minutes,
    examples: &["45m", "2h", "1h30m"],
    check: |duration| {
        require_whole_minutes(duration, 30..=6 * 60, "must be from 30 minutes to 6 hours")
    },
};

const VALIDATE_TIMEOUT: DurationFlag = DurationFlag {
    name: "--timeout",
    bare: BareUnit::Seconds,
    examples: DURATION_EXAMPLES,
    check: |_| Ok(()),
};

/// The floor is 1 second, because a 0-second timeout always times out. The
/// server sets the ceiling.
const EXEC_TIMEOUT: DurationFlag = DurationFlag {
    name: "--timeout",
    bare: BareUnit::Seconds,
    examples: DURATION_EXAMPLES,
    check: |timeout| require(timeout.seconds() > 0, "must be at least 1 second"),
};

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
    #[command(arg = json(JsonShape::Object))]
    Launch(LaunchArgs),

    /// Deprecated: use `launch` instead
    #[command(hide = true, arg = json(JsonShape::Object))]
    Run(LaunchArgs),

    /// Interact with test runs
    #[command(subcommand_required = false)]
    Runs {
        #[command(subcommand)]
        command: Option<RunsCommands>,
    },

    /// Launch a debugging session
    #[command(arg = json(JsonShape::Object))]
    Debug(DebugArgs),

    /// Output shell completions
    #[command(arg = json(JsonShape::None))]
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Validate local Antithesis setup
    #[command(arg = json(JsonShape::None))]
    Validate(ValidateArgs),

    /// Check environment configuration
    #[command(arg = json(JsonShape::Object))]
    Doctor(DoctorArgs),

    /// Print version information
    #[command(arg = json(JsonShape::Object))]
    Version,

    /// Check for and install updates
    #[command(arg = json(JsonShape::None))]
    Update(UpdateArgs),

    /// Search Antithesis documentation
    Docs {
        /// Don't check for documentation updates
        // Sorts after each docs subcommand's own flags and before the root
        // globals (1000 and up).
        #[arg(long, global = true, display_order = 999)]
        offline: bool,

        #[command(subcommand)]
        command: DocsCommands,
    },

    /// Show snouty's settings, and trust or deny a project settings file
    Settings {
        #[command(subcommand)]
        command: SettingsCommands,
    },

    /// Sign in and store your snouty configuration
    #[command(arg = json(JsonShape::None))]
    Login {
        #[arg(long, value_parser = validate_non_empty)]
        tenant: Option<String>,

        #[arg(long, value_parser = validate_non_empty)]
        repository: Option<String>,
    },

    /// Give Docker or Podman the credential for the tenant's container registry
    #[command(arg = json(JsonShape::None))]
    DockerCredential {
        /// The credential helper action that the engine requests
        action: DockerCredentialAction,
    },

    /// Serve snouty's run, docs, doctor and settings commands to AI agents over MCP
    #[command(
        hide = !features::is_enabled(Feature::Mcp),
        arg = json(JsonShape::None),
    )]
    Mcp(McpArgs),
}

/// What a command prints with `--json`.
#[derive(Clone, Copy)]
enum JsonShape {
    /// No JSON: the command's `--json` is hidden, and [`parse`] refuses it.
    None,
    Object,
    Array,
    /// NDJSON.
    Lines,
    /// A command whose shape depends on its flags, in its own words.
    Custom(&'static str),
}

/// The `--json` of a command, with help that names its JSON shape. It
/// shadows the global flag, which clap does not copy to a command that
/// defines its own. A group, such as `runs`, keeps the global flag, so its
/// subcommands still get it.
fn json(shape: JsonShape) -> clap::Arg {
    let arg = clap::Arg::new("json")
        .long("json")
        .action(clap::ArgAction::SetTrue)
        .display_order(1000);
    match shape {
        JsonShape::None => arg.hide(true),
        JsonShape::Object => arg.help("Print one JSON object"),
        JsonShape::Array => arg.help("Print one JSON array"),
        JsonShape::Lines => arg.help("Print NDJSON: one JSON object per line"),
        JsonShape::Custom(help) => arg.help(help),
    }
}

/// Refuses `--json` on a command that prints no JSON.
fn reject_json(matches: &clap::ArgMatches) -> Result<(), clap::Error> {
    let mut path = Vec::new();
    let mut leaf = matches;
    while let Some((name, sub)) = leaf.subcommand() {
        path.push(name);
        leaf = sub;
    }
    let no_json = (1..=path.len()).any(|depth| {
        tree::find(&path[..depth])
            .and_then(|command| command.get_arguments().find(|arg| arg.get_id() == "json"))
            .is_some_and(clap::Arg::is_hide_set)
    });
    if matches.get_flag("json") && no_json {
        let name = path.join(" ");
        let mut root = command();
        root.build();
        let command = path.iter().fold(&mut root, |command, name| {
            command
                .find_subcommand_mut(name)
                .expect("clap parsed this subcommand")
        });
        return Err(command.error(
            ErrorKind::ArgumentConflict,
            format!("`snouty {name}` has no JSON output; run it without --json"),
        ));
    }
    Ok(())
}

/// `snouty`'s clap command, with the long help of each command. clap
/// renders a page only when it builds that page's command.
pub fn command() -> clap::Command {
    help::attach(Cli::command(), Attach::Deferred)
}

/// Parses the command line, with the long help of [`command`]. The long help
/// is rendered at run time, so the derive's `Cli::parse` cannot set it.
///
/// Docker and Podman run [`HELPER_NAME`], an alias of `snouty`: when the file
/// name of argv[0] is that name, the command line is `snouty
/// docker-credential` and the rest of the arguments.
pub fn parse() -> Cli {
    let mut args: Vec<OsString> = std::env::args_os().collect();
    let as_helper =
        args.first().map(Path::new).and_then(Path::file_name) == Some(OsStr::new(HELPER_NAME));
    if as_helper {
        args.splice(..1, ["snouty", "docker-credential"].map(OsString::from));
    }
    let parsed = command().try_get_matches_from(args).and_then(|matches| {
        reject_json(&matches)?;
        Cli::from_arg_matches(&matches).map_err(|err| err.format(&mut command()))
    });
    match parsed {
        Ok(cli) => cli,
        // The helper protocol reads an error from stdout. The exit status
        // still fails the action when stdout is closed.
        Err(err) if as_helper && err.use_stderr() => {
            let _ = write!(stdio::stdout(), "{err}");
            std::process::exit(1)
        }
        // The terminal would re-wrap a long error mid-word, so wrap it here.
        // Only the `error:` line wraps: clap lays out the usage and tip lines.
        // It quotes the user's input, so backticks are plain characters.
        // anstream drops the colors when clap would not color.
        Err(err)
            if err.use_stderr()
                && err.kind() != ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand =>
        {
            let rendered = err.render().ansi().to_string();
            let (message, rest) = rendered.split_once('\n').unwrap_or((&rendered, ""));
            let message = match render::prose_width() {
                Some(width) => render::wrap_text(message, width, CodeSpans::Ignore).join("\n"),
                None => message.to_string(),
            };
            let _ = write!(anstream::stderr(), "{message}\n{rest}");
            std::process::exit(err.exit_code())
        }
        // --help, --version, and the help page for a bare `snouty`.
        Err(err) => err.exit(),
    }
}

/// Refuses a gated command or flag while its feature is off. A hidden
/// subcommand or flag is still callable, so dispatch checks this first.
pub fn gated_command_error(command: &Commands, enabled: &[Feature]) -> Option<Report> {
    let (feature, path) = match command {
        Commands::Mcp(_) => (Feature::Mcp, "snouty mcp".to_owned()),
        Commands::Launch(args) | Commands::Run(args) => {
            let flag = if args.max_compute_hours.is_some() {
                "--max-compute-hours"
            } else if args.setup_timeout.is_some() {
                "--setup-timeout"
            } else {
                return None;
            };
            (Feature::Teams, format!("snouty launch {flag}"))
        }
        Commands::Debug(args) if args.duration.is_some() => {
            (Feature::Teams, "snouty debug --duration".to_owned())
        }
        _ => return None,
    };
    if enabled.contains(&feature) {
        return None;
    }
    let report = user_error(format!(
        "`{path}` is an unstable feature and is not enabled"
    ))
    .note(format!(
        "enable it by setting {}={}",
        features::UNSTABLE_FEATURES_VAR_NAME,
        feature
    ))
    .note("an unstable feature can change or go away in any release");
    // A hidden flag has no help to point to, so only a command gets the suggestion.
    Some(if matches!(command, Commands::Mcp(_)) {
        report.suggestion(format!("run `{path} --help` for what it does"))
    } else {
        report
    })
}

/// clap value parser for a run ID. The ID is one segment of an API path, and
/// a URL parser removes an empty, `.` or `..` segment, so the request would go
/// to a different endpoint.
fn parse_run_id(value: &str) -> Result<String, String> {
    parse_path_segment(value, "run ID")
}

/// clap value parser for a property ID, which is a path segment too. See
/// [`parse_run_id`].
fn parse_property_id(value: &str) -> Result<String, String> {
    parse_path_segment(value, "property ID")
}

/// The client percent-encodes `/` in a path segment, but not `\`, which a URL
/// parser reads as `/`. So `..\x` would also leave the segment.
fn parse_path_segment(value: &str, what: &str) -> Result<String, String> {
    if matches!(value, "" | "." | "..") || value.contains('\\') {
        return Err(format!("invalid {what}: '{value}'"));
    }
    Ok(value.to_owned())
}

/// The actions of the Docker credential helper protocol.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum DockerCredentialAction {
    /// Print the credential for the server URL on stdin
    Get,
    /// Fail: snouty provides credentials for this registry
    Store,
    /// Read a server URL from stdin and erase nothing
    Erase,
    /// Fail: snouty is not a general-purpose credential store
    List,
}

fn validate_non_empty(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        Err("Value may not be empty or whitespace".to_owned())
    } else {
        Ok(value.to_owned())
    }
}

#[derive(Subcommand)]
pub enum SettingsCommands {
    /// Print the settings that snouty resolved
    #[command(arg = json(JsonShape::Object))]
    Show,

    /// Use a project settings file while its content stays the same
    #[command(arg = json(JsonShape::None))]
    Trust(SettingsFileArgs),

    /// Ignore a project settings file
    #[command(arg = json(JsonShape::None))]
    Deny(SettingsFileArgs),
}

#[derive(Args)]
pub struct SettingsFileArgs {
    /// The project settings file
    #[arg(default_value = crate::settings::PROJECT_SETTINGS_FILENAME)]
    pub path: std::path::PathBuf,
}

#[derive(Subcommand)]
pub enum DocsCommands {
    /// Search the documentation
    #[command(arg = json(JsonShape::Array))]
    Search {
        /// Print only matching page paths, one per line
        #[arg(short = 'l', long)]
        list: bool,

        /// Maximum number of results to return
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,

        /// Treat the query as a raw FTS5 expression (AND/OR/NOT/NEAR, "phrases",
        /// title: filters, prefix*) instead of literal text
        #[arg(id = "match", short = 'm', long = "match")]
        match_mode: bool,

        /// Search query
        #[arg(required = true)]
        query: Vec<String>,
    },
    /// Print a tree of documentation paths
    #[command(arg = json(JsonShape::Object))]
    Tree {
        /// Limit output to nodes at this depth or shallower
        #[arg(short = 'd', long)]
        depth: Option<std::num::NonZeroUsize>,

        /// Optional case-insensitive filter applied to page paths and titles
        filter: Option<String>,
    },

    /// Show full contents of a documentation page
    #[command(arg = json(JsonShape::None))]
    Show {
        /// Page path (e.g. "product/writing_tests/controlling_faults")
        path: String,
    },

    /// Print the path to the cached SQLite database
    #[command(arg = json(JsonShape::None))]
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

/// The point in a run's lifecycle that `runs wait --until` waits for.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum WaitUntil {
    /// The run is ready to use, or already complete
    Ready,
    /// The run is completed, cancelled, or incomplete
    Complete,
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

    #[arg(
        long,
        default_value = "2m",
        value_parser = |v: &str| VALIDATE_TIMEOUT.parse(v),
        help = VALIDATE_TIMEOUT.help(
            "Maximum time to wait for containers to start and reach setup-complete.",
        ),
    )]
    pub timeout: HumanDuration,

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
    /// Launcher name (e.g., basic_test, basic_k8s_test)
    #[arg(short, long, default_value = "basic_test", conflicts_with = "webhook")]
    pub launcher: String,

    /// Deprecated alias for --launcher
    #[arg(short, long, hide = true)]
    pub webhook: Option<String>,

    /// Local config dir (docker-compose.yaml or a manifests/ subdir), auto-built
    /// and pushed as the config image.
    #[arg(short, long, conflicts_with = "config_image")]
    pub config: Option<std::path::PathBuf>,

    /// Pre-built config image reference (e.g., us-central1-docker.pkg.dev/proj/repo/config:latest)
    #[arg(long)]
    pub config_image: Option<String>,

    /// Launch a compose config even when some of its images are not in the
    /// local image store. snouty will be unable to validate the image and pin
    /// it to an exact digest. Has no effect on a Kubernetes config.
    #[arg(long, requires = "config", conflicts_with = "config_image")]
    pub allow_remote_images: bool,

    /// Test name
    #[arg(long)]
    pub test_name: Option<String>,

    /// Test description
    #[arg(long)]
    pub description: Option<String>,

    // We send it to the API as `.minutes().to_string()`, the (possibly
    // fractional) minute count it wants.
    #[arg(
        long,
        value_parser = |v: &str| LAUNCH_DURATION.parse(v),
        conflicts_with = "max_compute_hours",
        help = LAUNCH_DURATION.help(if features::is_enabled(Feature::Teams) {
            "Deprecated: use --max-compute-hours. Test duration, at least 1 minute."
        } else {
            "Test duration, at least 1 minute."
        }),
    )]
    pub duration: Option<HumanDuration>,

    /// Compute hours for the run, setup included, at most 1000. One compute
    /// hour is one core running for one hour. The minimum depends on
    /// --performance-tier. The server default is 48. Requires tenant version
    /// 66 or newer.
    #[arg(
        long,
        value_parser = clap::value_parser!(u16).range(1..=1000),
        hide = !features::is_enabled(Feature::Teams),
    )]
    pub max_compute_hours: Option<u16>,

    #[arg(
        long,
        value_parser = |v: &str| SETUP_TIMEOUT.parse(v),
        hide = !features::is_enabled(Feature::Teams),
        help = "Fail the run immediately if setup takes longer than this, in whole \
                minutes with a unit, such as 10m, from 1m to 30m. The default is 5m. \
                Requires tenant version 66 or newer",
    )]
    pub setup_timeout: Option<HumanDuration>,

    /// Mark the test run as ephemeral, so it does not appear in future reports
    /// as a historic result. A launch without --source is ephemeral already.
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

    /// Suppress all output from log sources matching this RE2 pattern during
    /// fuzzing. A source is usually the name of the process that wrote the
    /// line, as shown in its `source` field. Matching is unanchored and
    /// case-sensitive (use `^name$` to match one source exactly). Suppressed
    /// output stays available in multiverse debugging. RE2 syntax: no
    /// lookahead/lookbehind or backreferences. Max 1023 bytes.
    #[arg(long)]
    pub filter_source_matching: Option<String>,

    /// Performance tier for the run. Higher tiers explore system states faster
    /// through more parallelism. They also consume compute hours faster. The
    /// server default is `standard`. To send a tier that is not listed, use
    /// `--param=antithesis.performance_tier=TIER`.
    #[arg(long, value_enum)]
    pub performance_tier: Option<PerformanceTier>,

    /// Which faults to inject, by how much disruption the system must survive.
    /// `none`: no faults. `resilient`: degraded network links, thread pauses,
    /// container pauses of up to 10 seconds, CPU throttling and clock jitter.
    /// `high_availability`: everything in `resilient`, plus network
    /// partitions and killed or stopped containers or pods. `legacy`: all
    /// network faults and thread pausing, for a custom notebook only. To send
    /// a level that is not listed, use
    /// `--param=antithesis.fault_tolerance=LEVEL`.
    #[arg(long, value_enum)]
    pub fault_tolerance: Option<FaultTolerance>,

    /// Regular expression for the containers (or pods, on Kubernetes) that
    /// faults must not touch, matched against the container name. Thread
    /// pausing is machine-wide, so it still applies. Rust regex syntax: no
    /// lookaround or backreferences. Max 1024 characters.
    #[arg(long)]
    pub fault_exclusions: Option<String>,

    /// Do not record the commit, branch, repository and pull request in run
    /// metadata
    #[arg(long)]
    pub no_vcs: bool,

    /// Extra parameters as key=value pairs (repeatable)
    #[arg(long = "param")]
    pub params: Vec<KVPair>,
}

#[derive(Args)]
pub struct McpArgs {
    /// The most tool calls that run at the same time; a call waits for a free
    /// slot
    #[arg(long, default_value = "8")]
    pub max_concurrent_calls: std::num::NonZeroUsize,
}

#[derive(Args)]
#[command(group(ArgGroup::new("target").args(["run_id", "session_id"]).required(true)))]
pub struct DebugArgs {
    /// Run ID of the test run to debug (preferred; give it or the session
    /// ID, not both)
    #[arg(long, value_parser = parse_run_id)]
    pub run_id: Option<String>,

    /// Session ID of the test run to debug, in place of the run ID. Use it
    /// only for a moment that names a session ID rather than a run ID
    #[arg(long)]
    pub session_id: Option<String>,

    /// Input hash identifying the moment to debug
    #[arg(long, allow_hyphen_values = true)]
    pub input_hash: String,

    /// Virtual time identifying the moment to debug
    #[arg(long)]
    pub vtime: VTime,

    #[arg(
        long,
        value_parser = |v: &str| DEBUG_DURATION.parse(v),
        hide = !features::is_enabled(Feature::Teams),
        help = DEBUG_DURATION.help(
            "How long the session stays available, from 30 minutes to 6 hours. Requires \
             tenant version 66 or newer.",
        ),
    )]
    pub duration: Option<HumanDuration>,

    /// Debugging session title, shown as the test name in the run list
    #[arg(long)]
    pub title: Option<String>,

    /// Debugging session description
    #[arg(long)]
    pub description: Option<String>,

    /// Report recipients (semicolon-delimited email addresses)
    #[arg(long)]
    pub recipients: Option<String>,

    /// Extra parameters as key=value pairs (repeatable)
    #[arg(long = "param", id = "param", value_name = "PARAMS")]
    pub params: Vec<KVPair>,
}

#[derive(Subcommand)]
pub enum RunsCommands {
    /// List recent runs
    #[command(arg = json(JsonShape::Lines))]
    List(RunsListArgs),

    /// Show details of a specific run
    #[command(arg = json(JsonShape::Object))]
    Show {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Open the run's page in a browser instead of printing details
        #[arg(short = 'w', long)]
        web: bool,
    },

    /// Wait until a run is ready or complete
    #[command(arg = json(JsonShape::Object))]
    Wait {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        #[arg(
            long,
            default_value = "1m",
            value_parser = |v: &str| POLL_INTERVAL.parse(v),
            help = POLL_INTERVAL.help(
                "Time between status checks, at least 1 minute.",
            ),
        )]
        poll_interval: HumanDuration,

        #[arg(
            long,
            value_parser = |v: &str| WAIT_TIMEOUT.parse(v),
            help = WAIT_TIMEOUT.help(
                "Give up after this long; without it the wait is unbounded.",
            ),
        )]
        timeout: Option<HumanDuration>,

        /// The status to wait for; a later status also ends the wait
        #[arg(long, value_enum, default_value_t = WaitUntil::Complete)]
        until: WaitUntil,
    },

    /// Cancel a run that has not finished
    #[command(arg = json(JsonShape::Object))]
    Cancel {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,
    },

    /// List property results for a run
    #[command(arg = json(JsonShape::Lines))]
    Properties {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Show passing properties; combine with the other status filters
        #[arg(long)]
        passing: bool,

        /// Show failing properties; combine with the other status filters
        #[arg(long)]
        failing: bool,

        /// Show pending properties; combine with the other status filters
        #[arg(long)]
        pending: bool,

        /// Filter properties by name (matched by substring, case-insensitive)
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

    /// Show one property of a run, by its ID
    #[command(arg = json(JsonShape::Object))]
    Property {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Property ID, as the run's property list shows it
        #[arg(value_parser = parse_property_id)]
        property_id: String,
    },

    /// Stream a run's property changes until the run is final
    #[command(arg = json(JsonShape::Lines))]
    Stream {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Stream only the events of this property (repeatable)
        #[arg(long = "property", value_name = "PROPERTY_ID")]
        property_ids: Vec<String>,
    },

    /// Stream build logs for a run
    #[command(arg = json(JsonShape::Lines))]
    BuildLogs {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,
    },

    /// Stream moment logs for a run
    #[command(arg = json(JsonShape::Lines))]
    Logs {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Input hash of the moment to stream to
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// Virtual time of the moment to end the stream at; omit it to stream to the last event with the input hash
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

    /// Execute a command in a run that is in progress
    #[command(arg = json(JsonShape::Lines))]
    Exec {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Input hash of the moment to execute at (with the vtime, picks the
        /// timeline)
        // A moment's input hash is routinely negative.
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// Virtual time of the moment to execute at
        // Parsed by `VTime` so a malformed value is rejected by clap, before
        // any API call. No `allow_hyphen_values`: a vtime is seconds since the
        // run began and is never negative, and accepting hyphen-led values
        // here would swallow a misplaced `--timeout` as the vtime.
        vtime: VTime,

        /// Bash script to execute
        script: Option<String>,

        /// Name or ID of the container to execute in; omit it to execute on
        /// the host
        #[arg(long, value_parser = clap::builder::NonEmptyStringValueParser::new())]
        container: Option<String>,

        /// Emit every event rather than just the script's output.
        #[arg(long)]
        events: bool,

        /// Detailed rendering of the events: a full-width vtime on every
        /// line, source locations, and each event's attached details JSON
        #[arg(short = 'd', long, requires = "events")]
        detail: bool,

        /// Run that a cold moment comes from; the server loads the moment
        /// into the run. Omit it in a debugging session for a moment of the
        /// run that the debugging session was launched from
        #[arg(long, conflicts_with = "source_session_id")]
        source_run_id: Option<SourceRunId>,

        /// Session ID that a cold moment comes from, in place of the source
        /// run. Use it only for a moment that names a session ID rather than a
        /// run ID
        #[arg(long)]
        source_session_id: Option<SourceSessionId>,

        #[arg(
            long,
            value_parser = |v: &str| EXEC_TIMEOUT.parse(v),
            help = EXEC_TIMEOUT.help(
                "Maximum time the server waits for the script to exit before reporting a \
                 timeout [default: the server's, 10m as of release 64.0].",
            ),
        )]
        timeout: Option<HumanDuration>,
    },

    /// Search events in a run
    #[command(arg = json(JsonShape::Lines))]
    Events {
        /// Run ID
        #[arg(value_parser = parse_run_id)]
        run_id: String,

        /// Substring to search for (repeatable; all matches must be present)
        #[arg(short = 'm', long = "match")]
        matches: Vec<String>,

        /// Maximum number of events to print, at most 999. Raise it to make a
        /// search more exhaustive.
        #[arg(short = 'n', long, default_value_t = SearchLimit::DEFAULT)]
        limit: SearchLimit,

        /// Substrings to match, as a positional alias for `-m` (all must match).
        /// At least one needle (via `-m` or here) is required.
        query: Vec<String>,

        #[command(flatten)]
        render: EventOutputArgs,
    },

    /// Query events with the event-set DSL
    #[command(arg = json(JsonShape::Custom(
        "Print NDJSON: one JSON object per event. A count prints {\"count\": N}, \
         and a query check prints {\"valid\": true}"
    )))]
    Search(RunsSearchArgs),

    /// Show the compute hours that recent runs used
    #[command(arg = json(JsonShape::Lines))]
    Usage(RunsUsageArgs),
}

#[derive(Args)]
pub struct RunsUsageArgs {
    /// Total the runs of each day, week, or month
    #[arg(long, value_enum)]
    pub rollup: Option<Rollup>,

    /// Only show runs that ended at or after this date (e.g. 2026-10-01),
    /// time (e.g. 2026-10-01 12:00:00) or RFC 3339 time
    #[arg(long, value_parser = parse_timestamp_arg)]
    pub after: Option<DateTime<Utc>>,

    /// Only show runs that ended before this date (e.g. 2026-11-01), time
    /// (e.g. 2026-11-01 12:00:00) or RFC 3339 time
    #[arg(long, value_parser = parse_timestamp_arg)]
    pub before: Option<DateTime<Utc>>,

    /// Maximum number of rows to display: runs, or periods with a rollup
    /// [default: 10; not allowed with both an after and a before time]
    #[arg(short = 'n', long)]
    pub limit: Option<NonZeroU64>,
}

#[derive(Args)]
pub struct RunsSearchArgs {
    /// Run ID
    #[arg(value_parser = parse_run_id)]
    pub run_id: String,

    /// Event-set DSL query
    pub query: String,

    /// Maximum number of events to print (default 50, maximum 999)
    #[arg(short = 'n', long)]
    pub limit: Option<SearchLimit>,

    /// Keep the connection open and print new matches as they arrive
    /// (the limit still caps the total)
    #[arg(short = 'f', long)]
    pub follow: bool,

    /// Check the query's syntax without running it
    #[arg(long, conflicts_with = "follow")]
    pub check: bool,

    /// Print the number of events in the run that match the query, instead
    /// of a sample of them
    #[arg(long, conflicts_with_all = ["follow", "check", "limit", "raw", "detail"])]
    pub count: bool,

    #[command(flatten)]
    pub render: EventOutputArgs,
}

/// The output-depth flags every event-stream command (`runs logs`,
/// `runs events`, `runs search`) shares. One definition keeps the flags, the
/// help text, and the raw/detail conflict from drifting between commands.
#[derive(Args)]
pub struct EventOutputArgs {
    /// Deprecated: print the server's events untouched, one JSON object per
    /// line, skipping snouty's normalization (vtime, and fault annotation on
    /// `runs logs`); requires --json
    #[arg(short = 'r', long, hide = true)]
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
    /// Filter by status
    #[arg(short, long, value_enum)]
    pub status: Option<RunStatus>,

    /// Filter by launcher name
    #[arg(short, long)]
    pub launcher: Option<String>,

    /// Only show runs created after this date (e.g. 2026-10-01), time
    /// (e.g. 2026-10-01 12:00:00) or RFC 3339 time
    #[arg(long, value_parser = parse_timestamp_arg)]
    pub created_after: Option<DateTime<Utc>>,

    /// Only show runs created at or before this date (e.g. 2026-11-01), time
    /// (e.g. 2026-11-01 12:00:00) or RFC 3339 time
    #[arg(long, value_parser = parse_timestamp_arg)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("args should parse")
    }

    /// Each command without subcommands names its JSON shape with
    /// [`json`], so a new command cannot fall back to the generic help of
    /// the global flag.
    #[test]
    fn every_command_names_its_json_shape() {
        fn check(command: &clap::Command, path: &str) {
            let subcommands: Vec<_> = command
                .get_subcommands()
                .filter(|sub| sub.get_name() != "help")
                .collect();
            if subcommands.is_empty() {
                let json = command
                    .get_arguments()
                    .find(|arg| arg.get_id() == "json")
                    .unwrap_or_else(|| panic!("{path}: no --json"));
                assert!(
                    !json.is_global_set(),
                    "{path}: add #[command(arg = json(..))]"
                );
            }
            for sub in subcommands {
                check(sub, &format!("{path} {}", sub.get_name()));
            }
        }
        check(tree::root(), "snouty");
    }

    /// clap parses the value name, and `launch` sends `Display`, the wire
    /// value. They differ for a tier such as `ultra_fast`, which clap would
    /// name `ultra-fast`.
    #[test]
    fn performance_tier_value_names_are_the_wire_values() {
        for tier in PerformanceTier::value_variants() {
            let name = tier.to_possible_value().expect("no variant is skipped");
            assert_eq!(name.get_name(), tier.to_string());
        }
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
    fn duration_flag_parses_into_human_duration() {
        // Parsing lives in `crate::time`; here we just confirm clap wires
        // `--duration` through it.
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
        let err = Cli::try_parse_from([
            "snouty",
            "launch",
            "--launcher",
            "basic_test",
            "--duration",
            "1.5h",
        ])
        .err()
        .expect("invalid duration should fail to parse")
        .to_string();
        assert!(err.contains("--duration"), "got: {err}");
        assert!(err.contains("a bare number is not permitted"), "got: {err}");
    }

    #[test]
    fn bare_number_warning_shows_the_value_with_a_unit() {
        let warning =
            |seconds| bare_number_warning("--timeout", HumanDuration::from_seconds(seconds));
        assert_eq!(
            warning(1800),
            "warning: a bare number for --timeout is deprecated; write 30m"
        );
        assert_eq!(
            warning(90),
            "warning: a bare number for --timeout is deprecated; write 1m30s"
        );
    }

    #[test]
    fn validate_timeout_reads_a_bare_number_as_seconds() {
        let timeout = |extra: &[&str]| {
            let cli = parse(&[&["snouty", "validate", "config"], extra].concat());
            let Commands::Validate(args) = cli.command else {
                panic!("expected validate command");
            };
            args.timeout.seconds()
        };
        assert_eq!(timeout(&[]), 120);
        assert_eq!(timeout(&["--timeout", "90"]), 90);
        assert_eq!(timeout(&["--timeout", "2m"]), 120);
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

    /// The name that clap accepts for each status is the name the API uses.
    #[test]
    fn run_status_values_are_the_api_names() {
        for status in &RUN_STATUSES {
            let value = status.to_possible_value().unwrap();
            assert_eq!(value.get_name(), status.to_string());
        }
    }

    // `runs events --limit` defaults to 50 and rejects 0.
    #[test]
    fn events_limit_defaults_and_rejects_zero() {
        let cli = parse(&["snouty", "runs", "events", "RUN", "-m", "request"]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { limit, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert_eq!(limit, SearchLimit::DEFAULT);

        let cli = parse(&[
            "snouty", "runs", "events", "RUN", "-m", "x", "--limit", "998",
        ]);
        let Commands::Runs {
            command: Some(RunsCommands::Events { limit, .. }),
        } = cli.command
        else {
            panic!("expected `runs events`");
        };
        assert_eq!(NonZeroU64::from(limit).get(), 998);

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
        assert_eq!(args.limit.map(|l| NonZeroU64::from(l).get()), Some(998));

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
        assert_eq!(args.limit.map(|l| NonZeroU64::from(l).get()), Some(7));
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

    /// Every duration flag, with the path of its command.
    const DURATION_FLAGS: [(&[&str], DurationFlag); 9] = [
        (&["validate"], VALIDATE_TIMEOUT),
        (&["launch"], LAUNCH_DURATION),
        (&["launch"], SETUP_TIMEOUT),
        (&["run"], LAUNCH_DURATION),
        (&["run"], SETUP_TIMEOUT),
        (&["debug"], DEBUG_DURATION),
        (&["runs", "wait"], POLL_INTERVAL),
        (&["runs", "wait"], WAIT_TIMEOUT),
        (&["runs", "exec"], EXEC_TIMEOUT),
    ];

    /// Each example of a duration flag, which its error shows,
    /// passes the flag's own parser and the parser of its clap arg, so it lies
    /// in the flag's range.
    #[test]
    fn duration_examples_pass_their_flag() {
        for (path, flag) in &DURATION_FLAGS {
            for example in flag.examples {
                if let Err(e) = flag.parse(example) {
                    panic!("{path:?} {} refuses its example {example}: {e}", flag.name);
                }
                let argv = ["snouty"].iter().chain(*path).chain([&flag.name, example]);
                if let Err(e) = Cli::command().try_get_matches_from(argv)
                    && e.kind() == clap::error::ErrorKind::ValueValidation
                {
                    panic!("{path:?} {} refuses its example {example}: {e}", flag.name);
                }
            }
        }
    }

    /// Each arg that parses into a [`HumanDuration`] is in [`DURATION_FLAGS`].
    #[test]
    fn duration_flags_lists_every_duration_arg() {
        fn visit(command: &clap::Command, path: &[&str]) {
            for arg in command.get_arguments() {
                if arg.get_value_parser().type_id() != TypeId::of::<HumanDuration>() {
                    continue;
                }
                let flag = format!("--{}", arg.get_long().unwrap_or_default());
                assert!(
                    DURATION_FLAGS
                        .iter()
                        .any(|(p, f)| *p == path && f.name == flag),
                    "{path:?} {flag} is not in DURATION_FLAGS"
                );
            }
            for sub in command.get_subcommands() {
                visit(sub, &[path, &[sub.get_name()]].concat());
            }
        }
        visit(tree::root(), &[]);
    }
}
