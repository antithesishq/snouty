use std::fs;
use std::path::Path;
use std::process::Command;

use color_eyre::eyre::{Context, Result, bail};
use log::{debug, info};

use crate::cli::TestStubArgs;

const DEFAULT_TEST_DIR: &str = "/opt/antithesis/test/v1";

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

    fn is_driver(self) -> bool {
        matches!(
            self,
            Self::ParallelDriver | Self::SerialDriver | Self::SingletonDriver
        )
    }
}

#[derive(Debug, Clone)]
pub struct TestScript {
    pub test_name: String,
    pub script_type: ScriptType,
    pub command_name: String,
    pub path: std::path::PathBuf,
}

/// Scan `base` for executable test scripts organized as `{test_name}/{command}`.
pub fn scan_scripts(base: &Path) -> Result<Vec<TestScript>> {
    let mut scripts = Vec::new();
    let mut unrecognized: Vec<String> = Vec::new();

    let entries = match fs::read_dir(base) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(scripts),
        Err(e) => return Err(e).wrap_err_with(|| format!("reading {}", base.display())),
    };

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
                        unrecognized.push(format!("  {}/{}", test_name, command_name));
                    }
                    continue;
                }
            };

            scripts.push(TestScript {
                test_name: test_name.clone(),
                script_type,
                command_name,
                path: cmd_path,
            });
        }
    }

    if !unrecognized.is_empty() {
        unrecognized.sort();
        bail!(
            "unrecognized command names (not a known prefix or helper_):\n{}",
            unrecognized.join("\n")
        );
    }

    scripts.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(scripts)
}

/// Fisher-Yates shuffle using `getrandom` for randomness.
fn shuffle<T>(slice: &mut [T]) {
    for i in (1..slice.len()).rev() {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("getrandom failed");
        let r = u64::from_le_bytes(buf);
        let j = (r % (i as u64 + 1)) as usize;
        slice.swap(i, j);
    }
}

/// Run a script and return whether it succeeded.
fn run_script(script: &TestScript, extra_env: &[(&str, &std::ffi::OsStr)]) -> Result<bool> {
    eprintln!("Running [{}/{}]", script.test_name, script.command_name,);
    let mut cmd = Command::new(&script.path);
    cmd.env_clear();
    for var in ["PATH", "HOME"] {
        if let Some(val) = std::env::var_os(var) {
            cmd.env(var, val);
        }
    }
    if let Some(dir) = script.path.parent() {
        cmd.current_dir(dir);
    }
    for &(key, val) in extra_env {
        cmd.env(key, val);
    }
    let status = cmd
        .status()
        .wrap_err_with(|| format!("executing {}", script.path.display()))?;
    if !status.success() {
        eprintln!(
            "FAIL [{}/{}] {}",
            script.test_name, script.command_name, status
        );
        return Ok(false);
    }
    Ok(true)
}

pub async fn cmd_test_stub(args: TestStubArgs) -> Result<()> {
    let base_dir = std::env::var_os("SNOUTY_TEST_TEMPLATES_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_TEST_DIR));
    let base = base_dir.as_path();
    let scripts = scan_scripts(base)?;

    let first: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::First)
        .collect();
    let drivers: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type.is_driver())
        .collect();
    let anytime: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Anytime)
        .collect();
    let eventually: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Eventually)
        .collect();
    let finally: Vec<_> = scripts
        .iter()
        .filter(|s| s.script_type == ScriptType::Finally)
        .collect();

    eprintln!(
        "Found {} first, {} driver, {} anytime, {} eventually, {} finally",
        first.len(),
        drivers.len(),
        anytime.len(),
        eventually.len(),
        finally.len(),
    );

    if drivers.is_empty() && anytime.is_empty() {
        bail!("no driver or anytime scripts found in {}", base.display());
    }

    if args.entrypoint {
        antithesis_sdk::lifecycle::setup_complete(
            &serde_json::json!({"source": "snouty test-stub"}),
        );
    }

    let mut ok = true;

    if std::env::var_os("ANTITHESIS_OUTPUT_DIR").is_some() {
        info!("ANTITHESIS_OUTPUT_DIR is set; skipping local script execution");
    } else {
        // Run first_ scripts with a temp output dir to detect setup_complete calls.
        let first_output_dir =
            tempfile::tempdir().wrap_err("creating temp directory for first_ script output")?;
        let first_env: [(&str, &std::ffi::OsStr); 1] =
            [("ANTITHESIS_OUTPUT_DIR", first_output_dir.path().as_os_str())];
        for s in &first {
            ok &= run_script(s, &first_env)?;
        }

        // Detect first_ scripts calling setup_complete (causes deadlock in real composer).
        let sdk_jsonl = first_output_dir.path().join("sdk.jsonl");
        if sdk_jsonl.exists() {
            let content = fs::read_to_string(&sdk_jsonl)
                .wrap_err("reading sdk.jsonl from first_ script output")?;
            for line in content.lines() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                    if v.get("antithesis_setup").is_some() {
                        bail!(
                            "first_ scripts must not call setup_complete (causes deadlock in real composer)"
                        );
                    }
                }
            }
        }

        let mut runnable: Vec<_> = drivers.iter().chain(anytime.iter()).copied().collect();
        shuffle(&mut runnable);

        for s in &runnable {
            ok &= run_script(s, &[])?;
        }

        for s in &eventually {
            ok &= run_script(s, &[])?;
        }

        for s in &finally {
            ok &= run_script(s, &[])?;
        }
    }

    if args.entrypoint {
        eprintln!("Waiting forever");
        std::future::pending::<()>().await;
    }

    if !ok {
        bail!("one or more scripts failed");
    }

    Ok(())
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

        let scripts = scan_scripts(base).unwrap();
        assert_eq!(scripts.len(), 7);

        let types: Vec<_> = scripts.iter().map(|s| s.script_type).collect();
        assert!(types.contains(&ScriptType::First));
        assert!(types.contains(&ScriptType::ParallelDriver));
        assert!(types.contains(&ScriptType::SerialDriver));
        assert!(types.contains(&ScriptType::SingletonDriver));
        assert!(types.contains(&ScriptType::Anytime));
        assert!(types.contains(&ScriptType::Eventually));
        assert!(types.contains(&ScriptType::Finally));
    }

    #[test]
    fn scan_scripts_helper_ignored() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "first_ok", true);
        make_script(base, "mytest", "helper_utils", true);

        let scripts = scan_scripts(base).unwrap();
        assert_eq!(scripts.len(), 1);
        assert_eq!(scripts[0].command_name, "first_ok");
    }

    #[test]
    fn scan_scripts_unknown_prefix() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        make_script(base, "mytest", "unknown_thing", true);
        make_script(base, "mytest", "first_ok", true);

        let err = scan_scripts(base).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unrecognized command names"), "{msg}");
        assert!(msg.contains("mytest/unknown_thing"), "{msg}");
    }

    #[test]
    fn scan_scripts_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let scripts = scan_scripts(tmp.path()).unwrap();
        assert!(scripts.is_empty());
    }

    #[test]
    fn scan_scripts_nonexistent_dir() {
        let scripts = scan_scripts(Path::new("/nonexistent/path/that/does/not/exist")).unwrap();
        assert!(scripts.is_empty());
    }
}
