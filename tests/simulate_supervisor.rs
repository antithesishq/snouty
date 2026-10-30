#![cfg(target_os = "linux")]

fn run_case(mode: &str) {
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/simulate/supervisor_test.py"
    );
    let output = std::process::Command::new("python3")
        .arg(script)
        .arg(mode)
        .output()
        .expect("run supervisor test fixture");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn composer_start_waits_for_inotify_watch() {
    run_case("readiness");
}

#[test]
fn composer_exit_pauses_faults_on_success_and_failure() {
    run_case("status");
}

#[test]
fn recreation_requires_commands_and_removes_volumes_and_stale_registrations() {
    run_case("recreate");
}
