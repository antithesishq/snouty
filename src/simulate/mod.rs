//! Local guest simulation. The guest owns Compose and test-composer execution.

mod events;
#[cfg(target_os = "linux")]
mod images;
#[cfg(target_os = "linux")]
mod vm;

use crate::{
    OutputOptions,
    cli::{SimulateArgs, SimulationId},
    settings::Settings,
};
use color_eyre::eyre::Result;
use std::process::ExitCode;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum SimulatePlatform {
    Amd64,
    Arm64,
}

impl Default for SimulatePlatform {
    fn default() -> Self {
        if cfg!(target_arch = "aarch64") {
            Self::Arm64
        } else {
            Self::Amd64
        }
    }
}

impl std::fmt::Display for SimulatePlatform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Amd64 => f.write_str("amd64"),
            Self::Arm64 => f.write_str("arm64"),
        }
    }
}

impl From<SimulatePlatform> for crate::container::Architecture {
    fn from(platform: SimulatePlatform) -> Self {
        match platform {
            SimulatePlatform::Amd64 => Self::Amd64,
            SimulatePlatform::Arm64 => Self::Arm64,
        }
    }
}

enum SimulationMode {
    Compose,
    Shell,
    Attach(SimulationId),
}

pub async fn cmd_simulate(
    mut args: SimulateArgs,
    settings: &Settings,
    output: OutputOptions,
) -> Result<ExitCode> {
    let mode = match (args.shell, args.attach.take()) {
        (false, None) => SimulationMode::Compose,
        (true, None) => SimulationMode::Shell,
        (false, Some(id)) => SimulationMode::Attach(id),
        (true, Some(_)) => unreachable!("clap rejects --shell with --attach"),
    };
    if let SimulationMode::Attach(id) = mode {
        if output.json {
            color_eyre::eyre::bail!("--attach cannot be combined with --json");
        }
        #[cfg(target_os = "linux")]
        {
            return attach(id);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            color_eyre::eyre::bail!("simulate requires Linux");
        }
    }
    if let SimulationMode::Shell = mode {
        if output.json {
            color_eyre::eyre::bail!("--shell cannot be combined with --json");
        }
        #[cfg(target_os = "linux")]
        {
            return shell(args, settings, output.verbose).await;
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (args, settings, output);
            color_eyre::eyre::bail!("simulate requires Linux");
        }
    }
    let config = match crate::config::detect_config(
        args.config
            .as_deref()
            .expect("clap requires config unless --shell or --attach is present"),
    )? {
        crate::config::Config::Compose(config) => config,
        crate::config::Config::Kubernetes(_) => color_eyre::eyre::bail!(
            "Kubernetes is not supported by simulate; provide docker-compose.yaml"
        ),
    };
    #[cfg(target_os = "linux")]
    {
        run(args, config, settings, output).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (args, config, settings, output);
        color_eyre::eyre::bail!("simulate requires Linux")
    }
}

#[cfg(target_os = "linux")]
use linux::{attach, run, shell};
#[cfg(target_os = "linux")]
mod linux;
