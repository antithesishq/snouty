use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "snouty")]
#[command(about = "CLI for the Antithesis API", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Launch a test run
    #[command(long_about = r#"Launch a test run

Example:
  snouty run --webhook basic_test --config ./config \
    --test-name "my-test" \
    --description "nightly test run" \
    --duration 30 \
    --recipients "team@example.com"

The -c/--config flag points at a local directory containing docker-compose.yaml.
Images required for the run need to have been built already. Pushing happens
automatically.

Alternatively, pass a pre-built config image directly:
  snouty run --webhook basic_test \
    --config-image us-central1-docker.pkg.dev/proj/repo/config:latest \
    --duration 30

Extra parameters can be passed with --param:
  snouty run -w basic_test --duration 30 \
    --param antithesis.integrations.github.token=TOKEN \
    --param my.custom.property=value"#)]
    Run(RunArgs),

    /// Access raw API endpoints
    #[command(subcommand)]
    Api(ApiCommands),

    /// Launch a debugging session
    #[command(long_about = r#"Launch a debugging session

Using CLI arguments:
  snouty debug \
    --antithesis.debugging.session_id f89d5c11f5e3bf5e4bb3641809800cee-44-22 \
    --antithesis.debugging.input_hash 6057726200491963783 \
    --antithesis.debugging.vtime 329.8037810830865 \
    --antithesis.report.recipients "team@example.com"

Using Moment.from (copy from triage report):
  echo 'Moment.from({ session_id: "...", input_hash: "...", vtime: ... })' | \
    snouty debug --stdin --antithesis.report.recipients "team@example.com""#)]
    Debug {
        /// Read parameters from stdin (JSON or Moment.from format)
        #[arg(long)]
        stdin: bool,

        /// Parameters as `--key value` pairs
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Output shell completions
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, elvish)
        shell: String,
    },

    /// Validate local Antithesis setup
    #[command(long_about = r#"Validate local Antithesis setup

Runs docker-compose locally and watches for the setup-complete event to confirm
instrumentation is working. After setup-complete is detected, discovers and
executes Test Composer scripts from /opt/antithesis/test/v1 inside the running
containers via compose exec.

Scripts are discovered by copying /opt/antithesis/test/v1 from each running
container and scanning for {test_name}/{command} entries. Scripts from all
services are merged into a single pool and executed in order:

  1. first_ scripts (sorted)
  2. drivers + anytime (shuffled together)
  3. eventually_ scripts (sorted)
  4. finally_ scripts (sorted)

Every script runs regardless of earlier failures. The exit code is nonzero
if any script fails. If no test scripts are found, validation still succeeds
(only setup-complete is checked).

Requires at least one driver or anytime script when test scripts are present.

Example:
  snouty validate ./config
  snouty validate ./config --timeout 120"#)]
    Validate(ValidateArgs),

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
        /// Print search results as JSON
        #[arg(short = 'j', long)]
        json: bool,

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
    /// Path to config directory containing docker-compose.yaml
    pub config: std::path::PathBuf,

    /// Maximum seconds to wait for the setup-complete event
    #[arg(long, default_value = "60")]
    pub timeout: u64,
}

#[derive(Args)]
pub struct RunArgs {
    /// Webhook endpoint name (e.g., basic_test, basic_k8s_test)
    #[arg(short, long)]
    pub webhook: String,

    /// Path to a local config directory containing docker-compose.yaml.
    /// Builds and pushes a config image automatically, setting antithesis.config_image.
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

    /// Test duration in minutes
    #[arg(long)]
    pub duration: Option<u32>,

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
