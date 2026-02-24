use clap::{Parser, Subcommand};

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
  snouty run -w basic_test \
    --antithesis.test_name "my-test" \
    --antithesis.description "nightly test run" \
    --antithesis.config_image config:latest \
    --antithesis.images app:latest \
    --antithesis.duration 30 \
    --antithesis.report.recipients "team@example.com""#)]
    Run {
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
        /// Shell to generate completions for (bash, zsh, fish, powershell, elvish)
        shell: String,
    },
    /// Print version information
    Version,
    /// Check for and install updates
    Update,
}
