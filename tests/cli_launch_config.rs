mod support;

use predicates::prelude::*;
use std::path::Path;
use support::*;
use tempfile::TempDir;

/// Create `dir/manifests/ns.yaml` so the directory looks like a k8s config.
fn write_manifest(dir: &Path) {
    let manifests = dir.join("manifests");
    std::fs::create_dir(&manifests).unwrap();
    std::fs::write(manifests.join("ns.yaml"), "kind: Namespace\n").unwrap();
}

#[test]
fn launch_config_rejects_nonexistent_dir() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_rejects_dir_without_compose() {
    let dir = TempDir::new().unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_rejects_yml_extension() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_conflicts_with_config_image_param() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_requires_registry_env() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .args([
            "launch",
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
fn launch_config_long_flag_accepted() {
    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_conflicts_with_param_config_image() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
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
fn launch_config_k8s_dir_is_accepted() {
    // Directory with manifests/ subdirectory should pass detect_config and
    // proceed to the ANTITHESIS_REPOSITORY env-var check.
    let dir = TempDir::new().unwrap();
    write_manifest(dir.path());

    snouty()
        .args([
            "launch",
            "-w",
            "basic_k8s_test",
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
fn launch_config_rejects_empty_manifests_dir() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("manifests")).unwrap();

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
            "-w",
            "basic_k8s_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("empty manifests/"));
}

#[test]
fn launch_config_rejects_ambiguous_dir() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("docker-compose.yaml"), "services: {}\n").unwrap();
    write_manifest(dir.path());

    snouty()
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "launch",
            "-w",
            "basic_test",
            "-c",
            dir.path().to_str().unwrap(),
            "--duration",
            "30",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "both docker-compose.yaml and a manifests/ subdirectory",
        ));
}
