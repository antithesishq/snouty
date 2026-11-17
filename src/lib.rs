mod api_cache;

pub mod agent;
pub mod api;
pub mod auth;
pub mod browser;
pub mod cli;
pub mod compose;
pub mod config;
pub mod container;
pub mod containers_storage;
pub mod digest;
pub mod docker_credential;
pub mod docs;
pub mod doctor;
pub(crate) mod env;
/// The vendored erofs-builder, a git subtree under vendor/ that its
/// VENDORED.md describes. snouty compiles its sources in, instead of
/// depending on the crate by path, so that `cargo publish` takes snouty.
#[doc(hidden)]
#[path = "../vendor/erofs-builder/src/lib.rs"]
#[allow(clippy::all, clippy::pedantic)]
#[rustfmt::skip]
pub mod erofs_builder;
pub mod error;
pub mod event_render;
pub mod event_set_dsl;
pub mod features;
pub mod help;
pub mod known_path;
pub mod login;
pub mod mcp;
pub mod ndjson;
pub mod params;
pub mod process;
pub mod registry;
pub(crate) mod render;
pub mod run_metadata;
pub use render::{OutputOptions, wrap_if_tty};
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod settings;
pub mod simulate;
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
