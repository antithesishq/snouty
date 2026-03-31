mod support;

use predicates::prelude::*;
use support::*;
use tempfile::TempDir;

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
