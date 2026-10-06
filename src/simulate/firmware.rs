use std::fs;
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::Deserialize;

use super::SimulatePlatform;

// QEMU defines the firmware descriptor fields here:
// https://qemu.googlesource.com/qemu/+/refs/heads/master/docs/interop/firmware.json
#[derive(Deserialize)]
struct Descriptor {
    #[serde(rename = "interface-types")]
    interfaces: Vec<Interface>,
    mapping: Mapping,
    targets: Vec<Target>,
    #[serde(default)]
    features: Vec<Feature>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Interface {
    Uefi,
    #[serde(other)]
    Other,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Feature {
    SecureBoot,
    RequiresSmm,
    HostUefiVars,
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(tag = "device", rename_all = "kebab-case")]
enum Mapping {
    Flash {
        executable: FlashFile,
    },
    Memory {
        filename: PathBuf,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct FlashFile {
    filename: PathBuf,
    format: Format,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum Format {
    Raw,
    Qcow2,
}

#[derive(Deserialize)]
struct Target {
    architecture: Architecture,
    machines: Vec<String>,
}

#[derive(Deserialize, PartialEq, Eq)]
enum Architecture {
    #[serde(rename = "aarch64")]
    Arm64,
    #[serde(rename = "x86_64")]
    Amd64,
    #[serde(other)]
    Other,
}

pub(super) fn platform_firmware(platform: SimulatePlatform) -> Result<PathBuf> {
    let (qemu, architecture, machines): (&str, Architecture, &[&str]) = match platform {
        SimulatePlatform::Amd64 => (
            "qemu-system-x86_64",
            Architecture::Amd64,
            &["pc-q35-*", "q35"],
        ),
        SimulatePlatform::Arm64 => (
            "qemu-system-aarch64",
            Architecture::Arm64,
            &["virt-*", "virt"],
        ),
    };
    let executable = fs::canonicalize(which::which(qemu)?)?;
    let prefix = executable.parent().and_then(Path::parent).ok_or_else(|| {
        eyre!(
            "cannot locate QEMU firmware beside {}",
            executable.display()
        )
    })?;
    let directory = prefix.join("share/qemu/firmware");
    let mut descriptors = fs::read_dir(&directory)
        .wrap_err_with(|| {
            format!(
                "cannot read QEMU firmware descriptors in {}",
                directory.display()
            )
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    descriptors.sort();

    for path in descriptors {
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        let descriptor: Descriptor = match serde_json::from_slice(&fs::read(&path)?) {
            Ok(descriptor) => descriptor,
            Err(error) => {
                log::debug!("ignoring firmware descriptor {}: {error}", path.display());
                continue;
            }
        };
        if !descriptor.interfaces.contains(&Interface::Uefi)
            || [
                Feature::SecureBoot,
                Feature::RequiresSmm,
                Feature::HostUefiVars,
            ]
            .iter()
            .any(|feature| descriptor.features.contains(feature))
            || !descriptor.targets.iter().any(|target| {
                target.architecture == architecture
                    && target
                        .machines
                        .iter()
                        .any(|machine| machines.contains(&machine.as_str()))
            })
        {
            continue;
        }
        let code = match descriptor.mapping {
            Mapping::Memory { filename } => filename,
            Mapping::Flash { executable } if executable.format == Format::Raw => {
                // QEMU specifies pflash here. We use -bios and do not persist UEFI variables.
                executable.filename
            }
            _ => continue,
        };
        if code.is_file() {
            return Ok(code);
        }
    }
    bail!("UEFI firmware for {platform} is unavailable; install QEMU UEFI firmware")
}
