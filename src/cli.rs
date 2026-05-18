use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "snouty")]
#[command(about = "CLI for the Antithesis API", long_about = None)]
#[command(version)]
pub struct Cli {
    /// Output JSON where supported (NDJSON for list/stream commands, pretty JSON otherwise)
    #[arg(long, global = true)]
    pub json: bool,

    /// Log API requests to stderr (authentication tokens redacted)
    #[arg(long, global = true)]
    pub verbose: bool,

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

The -c/--config flag points at a local directory containing docker-compose.yaml.
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

Environment variables:
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
        hide = true,
        long_about = r#"Interact with test runs

List, inspect, and view logs for Antithesis test runs.

When no subcommand is given, lists all runs (same as `snouty runs list`).

Examples:
  snouty runs
  snouty runs list --status completed --launcher nightly
  snouty runs show <run_id>
  snouty runs properties <run_id>
  snouty runs properties --failing <run_id>
  snouty runs properties --passing <run_id>
  snouty runs build-logs <run_id>
  snouty runs logs <run_id> <hash> <vtime>
  snouty runs events <run_id> <query>"#,
        subcommand_required = false
    )]
    Runs {
        #[command(subcommand)]
        command: Option<RunsCommands>,
    },

    /// Access raw API endpoints
    #[command(subcommand)]
    Api(ApiCommands),

    /// Launch a debugging session
    #[command(long_about = r#"Launch a debugging session

Using CLI arguments:
  snouty debug \
    --session-id f89d5c11f5e3bf5e4bb3641809800cee-44-22 \
    --input-hash 6057726200491963783 \
    --vtime 329.8037810830865 \
    --description "debug this moment" \
    --recipients "team@example.com"

Using Moment.from (copy from triage report):
  echo 'Moment.from({ session_id: "...", input_hash: "...", vtime: ... })' | \
    snouty debug --stdin --recipients "team@example.com""#)]
    Debug(DebugArgs),

    /// Output shell completions
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, elvish)
        shell: String,
    },

    /// Validate local Antithesis setup
    #[command(long_about = r#"Validate local Antithesis setup

Compose configs:
  Runs docker-compose locally and watches for the setup-complete event to
  confirm instrumentation is working. After setup-complete is detected,
  discovers Test Composer scripts from /opt/antithesis/test/v1 inside the
  running containers and validates their structure.


  Scripts are discovered by scanning /opt/antithesis/test/v1 from each running
  container for {test_name}/{command} entries. Scripts are
  validated to have recognized prefixes and at least one driver or anytime
  script when test scripts are present. Scripts are not executed.

Kubernetes configs:
  Runs docker.io/antithesishq/k8s-validator against the manifests/
  directory to perform static analysis of the manifests. --timeout has no
  effect (the validator does not start any workloads), and --keep-running
  has no effect (no containers are launched).

Example:
  snouty validate ./config
  snouty validate ./config --timeout 10
  snouty validate ./k8s-config"#)]
    Validate(ValidateArgs),

    /// Check environment configuration
    #[command(long_about = r#"Check environment configuration

Verifies that your environment is properly configured for Antithesis testing.
Checks container runtime availability and required environment variables.

Example:
  snouty doctor"#)]
    Doctor,

    /// Print version information
    Version,

    /// Check for and install updates
    Update,

    /// Search Antithesis documentation
    Docs {
        /// Don't check for documentation updates
        #[arg(long)]
        offline: bool,

        #[command(subcommand)]
        command: DocsCommands,
    },
}

#[derive(Subcommand)]
pub enum DocsCommands {
    /// Search the documentation
    #[command(long_about = r#"Search the documentation

Uses full-text search across the Antithesis documentation database.
The database is automatically updated before each search unless --offline is passed to the docs command.

Examples:
  snouty docs search fault injection
  snouty docs search "config image"
  snouty docs --offline search sdk setup"#)]
    Search {
        /// Print only matching page paths, one per line
        #[arg(short = 'l', long)]
        list: bool,

        /// Maximum number of results to return
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,

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
}

#[derive(Args)]
pub struct LaunchArgs {
    /// Webhook endpoint name (e.g., basic_test, basic_k8s_test)
    #[arg(short, long)]
    pub webhook: String,

    /// Path to a local config directory containing either docker-compose.yaml
    /// or a manifests/ subdirectory (Kubernetes manifests). Builds and pushes
    /// a config image automatically, setting antithesis.config_image. For
    /// docker-compose configs, locally-built service images are also pushed
    /// and exposed via antithesis.images. For Kubernetes configs, images are
    /// pulled by the platform from the references in the manifests.
    /// Requires ANTITHESIS_REPOSITORY environment variable.
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

    /// Test duration
    // The unit is determined by the webhook configuration on the platform side,
    // so snouty intentionally stays unit-agnostic. A string lets callers pass
    // fractional values when the unit is something coarse like minutes; the
    // numeric format is enforced by the params schema.
    #[arg(long)]
    pub duration: Option<String>,

    /// Mark the test run as ephemeral. Ephemeral runs will not appear in future reports as a historic result.
    #[arg(long)]
    pub ephemeral: bool,

    /// An optional identifier to separate property history in reports.
    ///
    /// In the resulting report, each property’s history is generated from all
    /// previous runs with the same antithesis.source parameter. This allows you
    /// to (for example) easily see the history of tests on a single branch.
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

    /// Session ID of the test run to debug
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
    List(RunsListArgs),

    /// Show details of a specific run
    Show {
        /// Run ID
        run_id: String,
    },

    /// List property results for a run
    Properties {
        /// Run ID
        run_id: String,

        /// Show only passing properties
        #[arg(long, conflicts_with = "failing")]
        passing: bool,

        /// Show only failing properties
        #[arg(long)]
        failing: bool,
    },

    /// Stream build logs for a run
    BuildLogs {
        /// Run ID
        run_id: String,
    },

    /// Stream moment logs for a run
    Logs {
        /// Run ID
        run_id: String,

        /// The input hash value identifying the moment
        #[arg(allow_hyphen_values = true)]
        input_hash: String,

        /// The virtual time value identifying the moment
        vtime: String,

        /// Start streaming from this virtual time (defaults to the root)
        #[arg(long)]
        begin_vtime: Option<String>,

        /// Start streaming from this input hash (optimization; must be paired with --begin-vtime)
        #[arg(long, allow_hyphen_values = true, requires = "begin_vtime")]
        begin_input_hash: Option<String>,
    },

    /// Search events in a run
    Events {
        /// Run ID
        run_id: String,

        /// Search query
        #[arg(required = true, num_args = 1..)]
        query: Vec<String>,
    },
}

#[derive(Args)]
pub struct RunsListArgs {
    /// Filter by status (starting, in_progress, completed, cancelled, incomplete, unknown)
    #[arg(short, long)]
    pub status: Option<String>,

    /// Filter by launcher name
    #[arg(short, long)]
    pub launcher: Option<String>,

    /// Only show runs created after this timestamp (ISO 8601)
    #[arg(long)]
    pub created_after: Option<String>,

    /// Only show runs created before this timestamp (ISO 8601)
    #[arg(long)]
    pub created_before: Option<String>,

    /// Maximum number of runs to display
    #[arg(short = 'n', long, default_value = "50")]
    pub limit: usize,
}

impl Default for RunsListArgs {
    fn default() -> Self {
        Self {
            status: None,
            launcher: None,
            created_after: None,
            created_before: None,
            limit: 50,
        }
    }
}

#[derive(Subcommand)]
pub enum ApiCommands {
    /// Send a raw webhook request
    #[command(long_about = r#"Send a raw webhook request

Example:
  snouty api webhook -w basic_test \
    --antithesis.config_image us-central1-docker.pkg.dev/proj/repo/config:latest \
    --antithesis.test_name "my-test" \
    --antithesis.duration 30

Parameters can also be passed via stdin as JSON:
  echo '{"antithesis.duration": "30"}' | snouty api webhook -w basic_test --stdin"#)]
    Webhook {
        /// Webhook endpoint name (e.g., basic_test, basic_k8s_test)
        #[arg(short, long)]
        webhook: String,

        /// Read parameters from stdin (JSON or Moment.from format)
        #[arg(long)]
        stdin: bool,

        /// Parameters as `--key value` pairs
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}
