use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use color_eyre::eyre::{Context, Result};
use log::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptType {
    First,
    ParallelDriver,
    SerialDriver,
    SingletonDriver,
    Anytime,
    Eventually,
    Finally,
}

impl ScriptType {
    fn from_prefix(name: &str) -> Option<Self> {
        if name.starts_with("first_") {
            Some(Self::First)
        } else if name.starts_with("parallel_driver_") {
            Some(Self::ParallelDriver)
        } else if name.starts_with("serial_driver_") {
            Some(Self::SerialDriver)
        } else if name.starts_with("singleton_driver_") {
            Some(Self::SingletonDriver)
        } else if name.starts_with("anytime_") {
            Some(Self::Anytime)
        } else if name.starts_with("eventually_") {
            Some(Self::Eventually)
        } else if name.starts_with("finally_") {
            Some(Self::Finally)
        } else {
            None
        }
    }

    pub fn is_driver(self) -> bool {
        matches!(
            self,
            Self::ParallelDriver | Self::SerialDriver | Self::SingletonDriver
        )
    }
}

#[derive(Debug, Clone)]
pub struct TestScript {
    pub service: String,
    pub test_name: String,
    pub script_type: ScriptType,
    pub command_name: String,
    pub path: std::path::PathBuf,
}

/// Result of scanning a directory for test scripts.
pub struct ScanResult {
    /// Recognized test scripts.
    pub scripts: Vec<TestScript>,
    /// Unrecognized file paths (e.g. `"suite/readme.txt"`).
    pub unrecognized: Vec<String>,
    /// Scripts that lack an executable bit (e.g. `"suite/first_setup"`).
    pub not_executable: Vec<String>,
}

/// Scan `base` for test scripts organized as `{test_name}/{command}`.
///
/// Each discovered script is tagged with the given `service` name.
/// Files with unrecognized prefixes (not a known type or `helper_`) are
/// collected in `ScanResult::unrecognized` — the caller decides whether
/// to treat them as errors.
pub fn scan_scripts(base: &Path, service: &str) -> Result<ScanResult> {
    let mut scripts = Vec::new();
    let mut unrecognized: Vec<String> = Vec::new();
    let mut not_executable: Vec<String> = Vec::new();

    let entries = fs::read_dir(base).wrap_err_with(|| format!("reading {}", base.display()))?;

    for test_dir_entry in entries {
        let test_dir_entry = test_dir_entry.wrap_err("reading test directory entry")?;
        let test_dir_path = test_dir_entry.path();
        if !test_dir_path.is_dir() {
            continue;
        }
        let test_name = test_dir_entry.file_name().to_string_lossy().into_owned();

        let commands = fs::read_dir(&test_dir_path)
            .wrap_err_with(|| format!("reading {}", test_dir_path.display()))?;

        for cmd_entry in commands {
            let cmd_entry = cmd_entry.wrap_err("reading command entry")?;
            let cmd_path = cmd_entry.path();
            if cmd_path.is_dir() {
                continue;
            }

            let command_name = cmd_entry.file_name().to_string_lossy().into_owned();
            let script_type = match ScriptType::from_prefix(&command_name) {
                Some(t) => t,
                None => {
                    if command_name.starts_with("helper_") {
                        debug!("skipping helper: {}", command_name);
                    } else {
                        unrecognized.push(format!("{}/{}", test_name, command_name));
                    }
                    continue;
                }
            };

            let metadata =
                fs::metadata(&cmd_path).wrap_err_with(|| format!("stat {}", cmd_path.display()))?;
            if metadata.permissions().mode() & 0o111 == 0 {
                not_executable.push(format!("{}/{}", test_name, command_name));
            }

            scripts.push(TestScript {
                service: service.to_string(),
                test_name: test_name.clone(),
                script_type,
                command_name,
                path: cmd_path,
            });
        }
    }

    unrecognized.sort();
    not_executable.sort();
    scripts.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(ScanResult {
        scripts,
        unrecognized,
        not_executable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    fn make_script(base: &Path, test_name: &str, command: &str, executable: bool) {
        let dir = base.join(test_name);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(command);
        fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        let mode = if executable { 0o755 } else { 0o644 };
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn scan_scripts_basic() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "first_setup", true);
        make_script(base, "mytest", "parallel_driver_load", true);
        make_script(base, "mytest", "serial_driver_seq", true);
        make_script(base, "mytest", "singleton_driver_one", true);
        make_script(base, "mytest", "anytime_check", true);
        make_script(base, "mytest", "eventually_verify", true);
        make_script(base, "mytest", "finally_cleanup", true);

        let result = scan_scripts(base, "svc").unwrap();
        assert_eq!(result.scripts.len(), 7);
        assert!(result.unrecognized.is_empty());

        let types: Vec<_> = result.scripts.iter().map(|s| s.script_type).collect();
        assert!(types.contains(&ScriptType::First));
        assert!(types.contains(&ScriptType::ParallelDriver));
        assert!(types.contains(&ScriptType::SerialDriver));
        assert!(types.contains(&ScriptType::SingletonDriver));
        assert!(types.contains(&ScriptType::Anytime));
        assert!(types.contains(&ScriptType::Eventually));
        assert!(types.contains(&ScriptType::Finally));

        assert!(result.scripts.iter().all(|s| s.service == "svc"));
    }

    #[test]
    fn scan_scripts_helper_ignored() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "first_ok", true);
        make_script(base, "mytest", "helper_utils", true);

        let result = scan_scripts(base, "svc").unwrap();
        assert_eq!(result.scripts.len(), 1);
        assert_eq!(result.scripts[0].command_name, "first_ok");
        assert!(result.unrecognized.is_empty());
    }

    #[test]
    fn scan_scripts_unknown_prefix() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "unknown_thing", true);
        make_script(base, "mytest", "first_ok", true);

        let result = scan_scripts(base, "svc").unwrap();
        assert_eq!(result.scripts.len(), 1);
        assert_eq!(result.unrecognized, vec!["mytest/unknown_thing"]);
    }

    #[test]
    fn scan_scripts_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let result = scan_scripts(tmp.path(), "svc").unwrap();
        assert!(result.scripts.is_empty());
        assert!(result.unrecognized.is_empty());
    }

    #[test]
    fn scan_scripts_nonexistent_dir() {
        let result = scan_scripts(Path::new("/nonexistent/path/that/does/not/exist"), "svc");
        assert!(result.is_err());
    }

    #[test]
    fn scan_scripts_not_executable() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "first_setup", false);
        make_script(base, "mytest", "parallel_driver_load", true);

        let result = scan_scripts(base, "svc").unwrap();
        assert_eq!(result.scripts.len(), 2);
        assert_eq!(result.not_executable, vec!["mytest/first_setup"]);
    }
}
