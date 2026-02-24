use std::fs;

use clap::CommandFactory;
use clap_complete::{Shell, generate_to};

include!("src/cli.rs");

fn main() {
    let outdir = std::env::var_os("SHELL_COMPLETIONS_DIR")
        .or_else(|| std::env::var_os("OUT_DIR"))
        .unwrap();
    fs::create_dir_all(&outdir).unwrap();

    let mut command = Cli::command();
    for shell in [
        Shell::Bash,
        Shell::Fish,
        Shell::Zsh,
        Shell::PowerShell,
        Shell::Elvish,
    ] {
        generate_to(shell, &mut command, "snouty", &outdir).unwrap();
    }
}
