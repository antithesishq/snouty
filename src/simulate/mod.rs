//! Local guest simulation. The guest owns Compose and test-composer execution.

mod events;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod firmware;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod images;
#[cfg(any(target_os = "linux", target_os = "macos"))]
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

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum FaultTolerance {
    Legacy,
    None,
    Resilient,
    HighAvailability,
}

impl std::fmt::Display for FaultTolerance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use clap::ValueEnum;
        f.write_str(
            self.to_possible_value()
                .expect("fault group is visible")
                .get_name(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct CustomScripts(serde_json::Value);

impl std::str::FromStr for CustomScripts {
    type Err = color_eyre::Report;

    fn from_str(value: &str) -> Result<Self> {
        let scripts: serde_json::Value = serde_json::from_str(value)?;
        let entries = scripts
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("custom scripts must be a JSON array"))?;
        for entry in entries {
            // configure.py also accepts booleans as Python numeric intervals.
            color_eyre::eyre::ensure!(
                entry
                    .get("command")
                    .is_some_and(serde_json::Value::is_string)
                    && entry
                        .get("interval")
                        .is_some_and(|interval| interval.is_number() || interval.is_boolean()),
                "each custom script must have a string command and a numeric interval"
            );
        }
        Ok(Self(scripts))
    }
}

impl std::fmt::Display for CustomScripts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            return attach(id);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = id;
            color_eyre::eyre::bail!("simulate requires Linux or macOS");
        }
    }
    if let SimulationMode::Shell = mode {
        if output.json {
            color_eyre::eyre::bail!("--shell cannot be combined with --json");
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            return shell(args, settings, output.verbose).await;
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (args, settings, output);
            color_eyre::eyre::bail!("simulate requires Linux or macOS");
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
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        run(args, config, settings, output).await
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (args, config, settings, output);
        color_eyre::eyre::bail!("simulate requires Linux or macOS")
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
use host::{attach, run, shell};
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod host;

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;

    #[hegel::test]
    fn custom_scripts_preserve_commands_intervals_and_extra_fields(tc: hegel::TestCase) {
        let command = tc.draw(generators::text());
        let interval = if tc.draw(generators::booleans()) {
            serde_json::Value::from(tc.draw(generators::integers::<i64>()))
        } else {
            serde_json::Value::from(tc.draw(generators::booleans()))
        };
        let extra = tc.draw(generators::text());
        let value = serde_json::json!([{"command": command, "interval": interval, "extra": extra}]);
        let scripts: CustomScripts = value.to_string().parse().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&scripts.to_string()).unwrap(),
            value
        );
        for field in ["command", "interval"] {
            let mut invalid = value.clone();
            invalid[0].as_object_mut().unwrap().remove(field);
            assert!(invalid.to_string().parse::<CustomScripts>().is_err());
        }
    }
}
