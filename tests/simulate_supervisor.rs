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
fn long_running_workload_gets_periodic_entropy() {
    run_case("long-running");
}

#[test]
fn single_rollout_keeps_entropy_timer_after_supervisor_exit() {
    run_case("once");
}

#[test]
fn composer_workload_gets_entropy_after_restart() {
    run_case("composer");
}
