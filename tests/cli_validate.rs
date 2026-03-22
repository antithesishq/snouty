mod support;

use predicates::prelude::*;
use support::*;

#[test]
fn validate_help_shows_out_dir_flag() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--out-dir"));
}

/// The --out-dir flag is accepted alongside a valid config path.
/// The out dir doesn't need to exist (it's created by copy_dir_recursive).
#[test]
fn validate_accepts_out_dir_flag() {
    snouty()
        .args([
            "validate",
            "--out-dir",
            "/tmp/nonexistent-output-dir",
            "/nonexistent/path",
        ])
        .assert()
        .failure()
        // Fails on config path validation, not on the out dir
        .stderr(predicate::str::contains("not a directory"));
}
