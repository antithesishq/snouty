mod api_cache;

pub mod api;
pub mod cli;
pub mod config;
pub mod container;
pub mod docs;
pub mod doctor;
pub mod moment;
pub mod params;
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod testutils;
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
