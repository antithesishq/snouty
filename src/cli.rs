use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};

use crate::api::RunStatus;
use crate::time::ReportDuration;

/// clap value parser for `--status` that keeps a friendly, enumerated error
/// message (the generated `RunStatus::from_str` only says "invalid value").
fn parse_run_status(value: &str) -> Result<RunStatus, String> {
    value.parse::<RunStatus>().map_err(|_| {
        format!(
            "invalid status: '{value}'\n\
             valid values: starting, in_progress, completed, cancelled, incomplete, unknown"
        )
    })
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
    #[arg(long, global = true, display_order = 1003)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Launch a test run
    #[command(long_about = r#"Launch a test run

Example:
  snouty launch --webhook basic_test --config ./config \
    --test-name "my-test" \
    --description "nightly test run" \
    --duration 30 \
    --recipients "team@example.com"

The -c/--config flag points at a local directory containing docker-compose.yaml
(this is the config image source, unrelated to snouty's own settings file).
Images required for the run need to have been built already. Pushing happens
automatically.

Alternatively, pass a pre-built config image directly:
  snouty launch --webhook basic_test \
    --config-image us-central1-docker.pkg.dev/proj/repo/config:latest \
    --duration 30

Extra parameters can be passed with --param:
  snouty launch -w basic_test --duration 30 \
    --param antithesis.integrations.github.token=TOKEN \
    --param my.custom.property=value

Additional container images that the config parser can't discover (e.g. an
image referenced only in a Kubernetes CRD field) can be registered with the
antithesis.images param, a semicolon-delimited [REGISTRY/]NAME(:TAG|@DIGEST)
list:
  snouty launch -w basic_k8s_test --config ./config --duration 30 \
    --param 'antithesis.images=app@sha256:...;db:latest'

Tenant and repository may be set via the environment variables below, or in a
settings file (./.snouty.toml by default; see the global --settings/--profile
flags and the README). Environment variables take precedence.

Environment variables (override any settings file):
  ANTITHESIS_TENANT       Your Antithesis tenant name (required).
  ANTITHESIS_API_KEY      API key authentication (preferred).
  ANTITHESIS_USERNAME     Username (required when API key is not set).
  ANTITHESIS_PASSWORD     Password (required when API key is not set).
  ANTITHESIS_REPOSITORY   Container registry for pushing images (required with --config).
  SNOUTY_CONTAINER_ENGINE Force "docker" or "podman" (auto-detected by default)."#)]
    Launch(LaunchArgs),

    /// Deprecated: use `launch` instead
    #[command(hide = true)]
    Run(LaunchArgs),

    /// Interact with test runs
    #[command(
        long_about = r#"Interact with test runs

List, inspect, and view logs for Antithesis test runs.

When no subcommand is given, lists all runs (same as `snouty runs list`).

Examples:
  snouty runs
  snouty runs list --status completed --launcher nightly
  snouty runs show <run_id>
  snouty runs properties <run_id>
  snouty runs properties --failing <run_id>
  snouty runs properties <run_id> --name <substring> --detail
  snouty runs build-logs <run_id>
  snouty runs logs <run_id> <hash> <vtime>
  snouty runs events <run_id> -m <query>"#,
        subcommand_required = false
    )]
    Runs {
        #[command(subcommand)]
        command: Option<RunsCommands>,
    },

    /// Launch a debugging session
    #[command(long_about = r#"Launch a debugging session

Identify the target run with exactly one of --run-id (preferred) or
--session-id.

Using CLI arguments:
  snouty debug \
    --run-id 9043254f65c9c65d63fe043a0abfc7fc-53-1 \
    --input-hash 6057726200491963783 \
    --vtime 329.8037810830865 \
    --description "debug this moment" \
    --recipients "team@example.com"

Using Moment.from (copy from triage report):
  echo 'Moment.from({ run_id: "...", input_hash: "...", vtime: ... })' | \
    snouty debug --stdin --recipients "team@example.com""#)]
    Debug(DebugArgs),

    /// Output shell completions
    #[command(long_about = r#"Output shell completions

Writes a completion script for SHELL to stdout; install it by sourcing it from
your shell config, e.g.:
  snouty completions zsh > ~/.zfunc/_snouty
  snouty completions bash | sudo tee /etc/bash_completion.d/snouty"#)]
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },

    /// Validate local Antithesis setup
    #[command(long_about = r#"Validate local Antithesis setup

Compose configs:
  Runs docker-compose locally and watches for the setup-complete event to
  confirm instrumentation is working. After setup-complete is detected,
  discovers test commands from /opt/antithesis/test/v1 inside the
  running containers and validates their structure.

  Before starting anything, validate resolves the compose file twice — with
  your shell and under a scrubbed environment matching the hermetic Antithesis
  environment — and fails if any ${VAR} resolves differently, catching setups
  that only work locally because a value came from your shell. Use
  --allow-compose-divergence to downgrade that to a warning.

  Test commands are discovered by scanning /opt/antithesis/test/v1 from each
  running container for {test_name}/{command} entries. Test commands are
  validated to have recognized prefixes and at least one driver or anytime
  test command when any are present. Test commands are not executed.

Kubernetes configs:
  Runs docker.io/antithesishq/k8s-validator against the manifests/
  directory to perform static analysis of the manifests. --timeout,
  --keep-running, and --allow-compose-divergence have no effect here (no
  workloads or containers are started, and there is no docker-compose config
  to render).

Example:
  snouty validate ./config
  snouty validate ./config --timeout 10
  snouty validate ./k8s-config"#)]
    Validate(ValidateArgs),

    /// Check environment configuration
    #[command(long_about = r#"Check environment configuration

Verifies that your environment is properly configured for Antithesis testing.
Runs health checks — container runtime, docker compose, the ANTITHESIS_*
environment variables for authentication, and API connectivity — then prints
the resolved settings (tenant, repository, container engine) so you can confirm
what snouty will use.

snouty prefers an API key (full API access); a username and password is legacy
auth, accepted only by `snouty launch` and `snouty debug`.

When credentials are configured, doctor also contacts the Antithesis API to
report the API and tenant versions and confirm connectivity. Pass --offline to
skip that network call.

Exits non-zero if any required check fails. Pass --json for a machine-readable
report (e.g. to gate CI).

Example:
  snouty doctor
  snouty doctor --json
  snouty doctor --offline"#)]
    Doctor(DoctorArgs),

    /// Print version information
    Version,

    /// Check for and install updates
    #[command(long_about = r#"Check for and install updates

Runs the bundled `snouty-update` helper, which checks for a newer release and
replaces the snouty binary in place. Does nothing if `snouty-update` is not
installed alongside snouty.

Pass a version to install a specific release instead of the latest, including
pre-releases:
  snouty update 0.6.0
  snouty update 0.6.0-rc.1

Installing a version older than the one you're running is a downgrade and
requires --force."#)]
    Update(UpdateArgs),

    /// Search Antithesis documentation
    #[command(long_about = r#"Search Antithesis documentation

Full-text search over a local copy of the Antithesis docs, auto-updated before
each use unless --offline. Subcommands: search, tree, show, sqlite.

Examples:
  snouty docs search fault injection
  snouty docs tree sdk
  snouty docs show getting_started"#)]
    Docs {
        /// Don't check for documentation updates
        #[arg(long)]
        offline: bool,

        #[command(subcommand)]
        command: DocsCommands,
    },

    /// Initialize Snouty configuration
    #[command(long_about = r#"Initialize Snouty configuration and authentication

Provide configuration and authentication information to persist in the global 
Snouty settings file, optionally under a named profile. Sensitive information and
information not provided via args will be queried over stdin.

NOTE: `snouty login` will offer to reuse your existing configuration values, including
any sourced from a local .snouty.toml file or the file specified by --settings or via
the SNOUTY_SETTINGS_PATH environment variable. However, snouty login will save the
specified configuration and credentials to the "global" files in your home directory.

Examples:
  snouty login
  snouty login --tenant "mytenant" --repository "repository"
  snouty login --profile "profile""#)]
    Login {
        #[arg(long)]
        tenant: Option<String>,

        #[arg(long)]
        repository: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum DocsCommands {
    /// Search the documentation
    #[command(long_about = r#"Search the documentation

Uses full-text search across the Antithesis documentation database.
The database is automatically updated before each search unless --offline is passed to the docs command.

Prints ranked matches (title and page path); pass a path to `snouty docs show`.
Use --list to print only the paths.

By default the query is searched as literal text. Pass --match to treat the
query as a raw SQLite FTS5 expression instead, enabling operators like
AND/OR/NOT/NEAR, "quoted phrases", `title:` column filters, and `prefix*`.

Examples:
  snouty docs search fault injection
  snouty docs search "config image"
  snouty docs search moment.branch
  snouty docs search sdk setup
  snouty docs search --match 'sdk NOT java'"#)]
    Search {
        /// Print only matching page paths, one per line
        #[arg(short = 'l', long)]
        list: bool,

        /// Maximum number of results to return
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,

        /// Treat the query as a raw FTS5 expression (AND/OR/NOT/NEAR, "phrases",
        /// title: filters, prefix*) instead of literal text
        #[arg(short = 'm', long = "match")]
        match_mode: bool,

        /// Search query
        query: Vec<String>,
    },
    /// Print the path to the cached SQLite database
    #[command(long_about = r#"Print the path to the cached SQLite database

Useful for directly querying the documentation database with external tools."#)]
    Sqlite,

    /// Print a tree of documentation paths
    #[command(long_about = r#"Print a tree of documentation paths

Builds a directory-like tree from all page paths stored in the documentation database.

Examples:
  snouty docs tree
  snouty docs tree --depth 2
  snouty docs tree -d 2
  snouty docs tree sdk"#)]
    Tree {
        /// Limit output to nodes at this depth or shallower
        #[arg(short = 'd', long)]
        depth: Option<std::num::NonZeroUsize>,

        /// Optional case-insensitive filter applied to page paths and titles
        filter: Option<String>,
    },

    /// Show full contents of a documentation page
    #[command(long_about = r#"Show full contents of a documentation page

Displays the full markdown content of a page by its path.
If the exact path is not found, suggests similar pages."#)]
    Show {
        /// Page path (e.g. "getting_started/overview")
        path: String,
    },
}

#[derive(Args)]
pub struct UpdateArgs {
    /// Release to install (e.g. 0.6.0 or 0.6.0-rc.1). Defaults to the latest release.
    pub version: Option<String>,

    /// Install the requested version even if it is older than the current one (a downgrade)
    #[arg(long)]
    pub force: bool,
}

#[derive(Args)]
pub struct ValidateArgs {
    /// Path to config directory containing either docker-compose.yaml or a
    /// manifests/ subdirectory (Kubernetes manifests).
    pub config: std::path::PathBuf,

    /// Maximum seconds to wait for the setup-complete event
    #[arg(long, default_value = "60")]
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
    // `ReportDuration: FromStr` gives clap the parser; we send it to the API as
    // `.minutes().to_string()`, the (possibly fractional) minute count it wants.
    #[arg(long)]
    pub duration: Option<ReportDuration>,

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

    /// Extra parameters as key=value pairs (repeatable)
    #[arg(long = "param")]
    pub params: Vec<String>,
}

#[derive(Args)]
pub struct DebugArgs {
    /// Read parameters from stdin (JSON or Moment.from format)
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
    pub vtime: Option<String>,

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
    #[command(
        long_about = r#"List recent runs (the default when `snouty runs` runs with no subcommand).

Columns: RUN ID, STATUS, CREATED, TEST NAME. Use --detail or --json for the
full description and launcher."#
    )]
    List(RunsListArgs),

    /// Show details of a specific run
    #[command(
        long_about = r#"Show a run's metadata: id, status, timestamps, launcher, and description.

Incomplete runs also show the failure moment (Failure Hash/VTime) to pass to
`runs logs`. Use --web to open the triage report in a browser."#
    )]
    Show {
        /// Run ID
        run_id: String,

        /// Open the run's triage report in a browser instead of printing details
        #[arg(short = 'w', long)]
        web: bool,
    },

    /// List property results for a run
    #[command(
        long_about = r#"List a run's property (assertion) results, one table per group.

Each table is headed by its group; columns are STATUS, EXAMPLES, NAME (failing
first). EXAMPLES is the example count, shown as examples/counterexamples when a
property has counterexamples.

Narrow with --name and/or --group (both case-insensitive substring matches);
add --detail to expand the matches into their examples and counter-example
moments instead of the table. Use --json for automation. --json is mutually
exclusive with --detail.

Examples:
  snouty runs properties <run_id> --failing
  snouty runs properties <run_id> --name eventually_validate --detail
  snouty runs properties <run_id> --group Unreachable --detail"#
    )]
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
    #[command(long_about = "Stream a run's build and setup logs.\n\n\
        Output: `timestamp [stream] line`.")]
    BuildLogs {
        /// Run ID
        run_id: String,
    },

    /// Stream moment logs for a run
    #[command(long_about = r#"Stream a timeline's logs up to a moment.

INPUT_HASH and VTIME identify the moment and its timeline; logs are streamed up
to that moment. Without --begin-vtime, streaming starts at the timeline's
earliest log entry.

Output: `[vtime] [source] [stream] message`. A moment (HASH/VTIME) comes from
`runs properties --detail` or `runs events`."#)]
    Logs {
        /// Run ID
        run_id: String,

        /// Input hash of the moment to stream up to (with VTIME, picks the timeline)
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// Virtual time of the moment to stream up to
        #[arg(allow_hyphen_values = true)]
        vtime: String,

        /// Start from this virtual time instead of the timeline's earliest log entry
        #[arg(long, allow_hyphen_values = true)]
        begin_vtime: Option<String>,

        /// Start from this input hash (optimization; must be paired with --begin-vtime)
        #[arg(long, allow_hyphen_values = true, requires = "begin_vtime")]
        begin_input_hash: Option<String>,

        /// Skip post-processing: with --json, pass NDJSON through unannotated;
        /// otherwise print the text payload verbatim (keep ANSI/control bytes)
        #[arg(short = 'r', long)]
        raw: bool,
    },

    /// Search events in a run
    #[command(
        long_about = r#"Search a run's events for one or more substrings (all must match).

Columns: HASH, VTIME, SOURCE ([container:stream]), OUTPUT. Feed a row's HASH
and VTIME into `runs logs` to see the surrounding logs."#
    )]
    Events {
        /// Run ID
        run_id: String,

        /// Substring to search for (repeatable; all matches must be present)
        #[arg(short = 'm', long = "match")]
        matches: Vec<String>,

        /// Substrings to match, as a positional alias for `-m` (all must match).
        /// At least one needle (via `-m` or here) is required.
        query: Vec<String>,
    },
}

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
    #[arg(short = 'n', long, default_value = "10")]
    pub limit: usize,

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
            limit: 10,
            detail: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("args should parse")
    }

    #[test]
    fn duration_flag_parses_into_report_duration() {
        // Parsing/validation lives in `crate::time`; here we just confirm clap
        // wires `--duration` through `ReportDuration: FromStr`.
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

    // The positional `input_hash`/`vtime` and the `--begin-*` flags must all
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
            "--begin-input-hash",
            "0",
        ]);
        let Commands::Runs {
            command:
                Some(RunsCommands::Logs {
                    input_hash,
                    vtime,
                    begin_vtime,
                    begin_input_hash,
                    ..
                }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert_eq!(input_hash, "-123");
        assert_eq!(vtime, "-2.0");
        assert_eq!(begin_vtime.as_deref(), Some("-2.0"));
        assert_eq!(begin_input_hash.as_deref(), Some("0"));
    }

    // `-r` is the short form of `--raw`; note `-r` must not swallow the
    // hyphen-led positionals that follow it.
    #[test]
    fn logs_accepts_raw_short_flag() {
        let cli = parse(&["snouty", "runs", "logs", "-r", "RUN", "-123", "-2.0"]);
        let Commands::Runs {
            command: Some(RunsCommands::Logs { raw, vtime, .. }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert!(raw);
        assert_eq!(vtime, "-2.0");

        let cli = parse(&["snouty", "runs", "logs", "RUN", "-123", "-2.0"]);
        let Commands::Runs {
            command: Some(RunsCommands::Logs { raw, .. }),
        } = cli.command
        else {
            panic!("expected `runs logs`");
        };
        assert!(!raw);
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
}
