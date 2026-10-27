mod api_cache;
mod attributed_value;

pub mod api;
pub mod auth;
pub mod browser;
pub mod cli;
pub mod compose;
pub mod config;
pub mod container;
pub mod docker_credential;
pub mod docs;
pub mod doctor;
pub(crate) mod env;
pub mod error;
pub mod event_render;
pub mod event_set_dsl;
pub mod features;
pub mod git;
pub mod help;
pub mod jsonl;
pub mod login;
pub mod mcp;
pub mod params;
pub mod process;
pub mod registry;
pub(crate) mod render;
pub use render::{OutputOptions, wrap_if_tty};
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod settings;
pub mod stdio;
pub(crate) mod table;
pub mod tag;
#[doc(hidden)]
pub mod testutils;
pub mod time;
pub mod user_agent;
pub mod util;
pub mod validate;
pub mod vtime;

/// Installs the error report hook and the `RUST_LOG` logger. Each binary calls
/// it first.
pub fn install_hooks() {
    use std::io::Write;
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
}
