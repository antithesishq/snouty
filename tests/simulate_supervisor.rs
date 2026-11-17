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
fn composer_status_preserves_faults_on_success_and_pauses_on_failure() {
    run_case("status");
}

#[test]
fn recreation_pauses_faults_and_removes_volumes_and_stale_registrations() {
    run_case("recreate");
}

#[test]
fn workload_kind_uses_live_commands_and_preserves_setup_only_workloads() {
    run_case("workload-kind");
}
