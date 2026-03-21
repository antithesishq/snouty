mod support;

use predicates::prelude::*;
use support::*;

#[test]
fn validate_help_shows_sdk_out_dir_flag() {
    snouty()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--antithesis-sdk-out-dir"));
}

/// The --antithesis-sdk-out-dir flag is accepted alongside a valid config path.
/// The sdk out dir doesn't need to exist (it's created by copy_dir_recursive).
#[test]
fn validate_accepts_sdk_out_dir_flag() {
    snouty()
        .args([
            "validate",
            "--antithesis-sdk-out-dir",
            "/tmp/nonexistent-output-dir",
            "/nonexistent/path",
        ])
        .assert()
        .failure()
        // Fails on config path validation, not on the sdk out dir
        .stderr(predicate::str::contains("not a directory"));
}
