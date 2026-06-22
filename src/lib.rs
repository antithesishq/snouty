mod api_cache;
mod attributed_value;

pub mod api;
pub mod cli;
pub mod config;
pub mod container;
pub mod credentials;
pub mod docs;
pub mod doctor;
pub(crate) mod env;
pub mod error;
pub mod moment;
pub mod params;
pub(crate) mod render;
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod settings;
#[doc(hidden)]
pub mod testutils;
pub mod time;
pub mod validate;

/// User-Agent string sent with every HTTP request snouty makes.
pub fn user_agent() -> String {
    format!(
        "snouty/{} ({}; {}; rust{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}
