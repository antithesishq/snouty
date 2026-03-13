use std::fs;
use std::process::Command;

use clap::CommandFactory;
use clap_complete::{Shell, generate_to};

include!("src/cli.rs");

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!(
        "cargo:rustc-env=SNOUTY_RUSTC_VERSION={}",
        rustc_version().unwrap()
    );

    let outdir = std::env::var_os("SHELL_COMPLETIONS_DIR")
        .or_else(|| std::env::var_os("OUT_DIR"))
        .unwrap();
    fs::create_dir_all(&outdir).unwrap();

    let mut command = Cli::command();
    for shell in [Shell::Bash, Shell::Fish, Shell::Zsh, Shell::Elvish] {
        generate_to(shell, &mut command, "snouty", &outdir).unwrap();
    }
}

fn rustc_version() -> Result<String, Box<dyn std::error::Error>> {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(rustc).arg("-V").output()?;
    let stdout = String::from_utf8(output.stdout)?;

    stdout
        .split_whitespace()
        .nth(1)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "rustc -V did not return a parseable version".into())
}
