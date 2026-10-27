//! `docker-credential-snouty`: the name Docker and Podman give the snouty
//! credential helper (`"credHelpers": {"<registry>": "snouty"}`).

use std::process::ExitCode;

// The same allocator as `snouty`. See the global_allocator in main.rs.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    snouty::docker_credential::main()
}
