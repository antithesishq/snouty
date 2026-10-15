// Stdout goes through `out!`/`outln!`, so that a closed pipe is an error and
// not a panic.
#![deny(clippy::print_stdout)]

/// A failed write to stdout. A closed pipe here is a normal end to a pipeline;
/// the distinct type keeps a broken pipe elsewhere, such as a child's stdin, an
/// error.
#[doc(hidden)]
#[derive(Debug)]
pub struct StdoutError(pub std::io::Error);

impl StdoutError {
    pub fn is_closed_pipe(&self) -> bool {
        self.0.kind() == std::io::ErrorKind::BrokenPipe
    }
}

impl std::fmt::Display for StdoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to write to stdout")
    }
}

impl std::error::Error for StdoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// `print!` that returns a [`StdoutError`] on a failed write instead of
/// panicking.
#[doc(hidden)]
#[macro_export]
macro_rules! out {
    ($($arg:tt)*) => {{
        use ::std::io::Write as _;
        ::std::write!(::std::io::stdout(), $($arg)*).map_err($crate::StdoutError)
    }};
}

/// `println!`, with the same error behavior as [`out!`].
#[doc(hidden)]
#[macro_export]
macro_rules! outln {
    ($($arg:tt)*) => {{
        use ::std::io::Write as _;
        ::std::writeln!(::std::io::stdout(), $($arg)*).map_err($crate::StdoutError)
    }};
}

mod api_cache;
mod attributed_value;

pub mod api;
pub mod auth;
pub mod browser;
pub mod cli;
pub mod compose;
pub mod config;
pub mod container;
pub mod docs;
pub mod doctor;
pub(crate) mod env;
pub mod error;
pub mod event_render;
pub mod event_set_dsl;
pub mod features;
pub mod git;
pub mod jsonl;
pub mod login;
pub mod params;
pub mod process;
pub(crate) mod render;
pub use render::{OutputOptions, wrap_if_tty};
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod settings;
pub(crate) mod table;
pub mod tag;
#[doc(hidden)]
pub mod testutils;
pub mod time;
pub mod user_agent;
pub mod util;
pub mod validate;
pub mod vtime;
