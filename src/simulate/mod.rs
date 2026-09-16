//! Local guest simulation. The guest owns Compose and test-composer execution.

mod events;
#[cfg(target_os = "linux")]
mod images;
#[cfg(target_os = "linux")]
mod vm;

use crate::{OutputOptions, cli::SimulateArgs, settings::Settings};
use color_eyre::eyre::Result;
use std::process::ExitCode;

pub async fn cmd_simulate(
    args: SimulateArgs,
    settings: &Settings,
    output: OutputOptions,
) -> Result<ExitCode> {
    let config = match crate::config::detect_config(&args.config)? {
        crate::config::Config::Compose(config) => config,
        crate::config::Config::Kubernetes(_) => color_eyre::eyre::bail!(
            "Kubernetes is not supported by simulate; provide docker-compose.yaml"
        ),
    };
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        run(args, config, settings, output).await
    }
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    {
        let _ = (args, config, settings, output);
        color_eyre::eyre::bail!("simulate requires Linux x86_64")
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use linux::run;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod linux;
