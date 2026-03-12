mod support;

use predicates::prelude::*;
use support::*;
use tempfile::TempDir;

#[test]
fn run_config_rejects_nonexistent_dir() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            "/nonexistent/path",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a directory"));
}

#[test]
fn run_config_rejects_dir_without_compose() {
    let dir = TempDir::new().unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("docker-compose"));
}

#[test]
fn run_config_rejects_yml_extension() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires docker-compose.yaml"));
}

#[test]
fn run_config_conflicts_with_config_image_param() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--config-image",
            "some-image:latest",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--config"));
}

#[test]
fn run_config_requires_registry_env() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ANTITHESIS_REPOSITORY"));
}

#[test]
fn run_config_long_flag_accepted() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "--config",
            "/nonexistent/path",
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a directory"));
}

#[test]
fn run_config_conflicts_with_param_config_image() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be overridden via --param"));
}

#[test]
fn run_config_submits_pinned_images_with_podman_shim() {
    let (stderr, log) = run_config_with_runtime(ShimRuntime::Podman);

    assert!(stderr.contains(
        r#""antithesis.images": "registry.example.com/repo/app@sha256:1111111111111111111111111111111111111111111111111111111111111111;registry.example.com/repo/sidecar@sha256:2222222222222222222222222222222222222222222222222222222222222222""#
    ));
    assert!(stderr.contains(
        r#""antithesis.config_image": "registry.example.com/repo/snouty-config@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc""#
    ));
    assert!(log.contains("podman --version"));
    assert!(log.contains("podman compose config"));
    assert!(log.contains("podman build"));
    assert!(log.contains("podman push"));
    assert!(log.contains("registry.example.com/repo/app:1.2.3"));
    assert!(log.contains("registry.example.com/repo/sidecar@sha256:oldsidecar"));
}

#[test]
fn run_config_falls_back_to_docker_shim() {
    let (stderr, log) = run_config_with_runtime(ShimRuntime::Docker);

    assert!(stderr.contains(
        r#""antithesis.images": "registry.example.com/repo/app@sha256:1111111111111111111111111111111111111111111111111111111111111111;registry.example.com/repo/sidecar@sha256:2222222222222222222222222222222222222222222222222222222222222222""#
    ));
    assert!(stderr.contains(
        r#""antithesis.config_image": "registry.example.com/repo/snouty-config@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc""#
    ));
    assert!(log.contains("docker --version"));
    assert!(log.contains("docker version"));
    assert!(log.contains("docker compose config"));
    assert!(log.contains("docker build"));
    assert!(log.contains("docker push registry.example.com/repo/app:1.2.3"));
    assert!(!log.contains("podman "));
}

#[test]
fn run_config_uses_docker_shim_when_it_is_podman_in_disguise() {
    let (stderr, log) = run_config_with_runtime(ShimRuntime::PodmanAsDocker);

    assert!(stderr.contains(
        r#""antithesis.images": "registry.example.com/repo/app@sha256:1111111111111111111111111111111111111111111111111111111111111111;registry.example.com/repo/sidecar@sha256:2222222222222222222222222222222222222222222222222222222222222222""#
    ));
    assert!(stderr.contains(
        r#""antithesis.config_image": "registry.example.com/repo/snouty-config@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc""#
    ));
    assert!(log.contains("docker --version"));
    assert!(log.contains("docker version"));
    assert!(log.contains("docker compose config"));
    assert!(log.contains("docker build"));
    assert!(log.contains("docker push"));
    assert!(log.contains("registry.example.com/repo/sidecar@sha256:oldsidecar"));
    assert!(!log.contains("podman "));
}
