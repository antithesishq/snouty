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
            "--source",
            "ci",
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
            "--source",
            "ci",
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
            "--source",
            "ci",
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
            "--source",
            "ci",
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
            "--source",
            "ci",
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
            "--source",
            "ci",
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be overridden via --param"));
}

#[test]
fn run_config_loads_local_config_from_passed_directory() {
    let workspace = TempDir::new().unwrap();
    let caller_dir = workspace.path().join("caller");
    let config_dir = workspace.path().join("repo").join("config");
    std::fs::create_dir_all(&caller_dir).unwrap();
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(caller_dir.join(".snouty.yaml"), "source: from-cwd\n").unwrap();
    std::fs::write(config_dir.join("docker-compose.yaml"), "version: '3'\n").unwrap();
    std::fs::write(config_dir.join(".snouty.yaml"), "source: x\nbogus: y\n").unwrap();

    snouty()
        .current_dir(&caller_dir)
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            "../repo/config",
            "--duration",
            "30",
            "--source",
            "ci",
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid config file"))
        .stderr(predicate::str::contains(
            config_dir.join(".snouty.yaml").to_str().unwrap(),
        ));
}

#[test]
fn run_config_walks_up_from_passed_directory_to_parent() {
    let workspace = TempDir::new().unwrap();
    let caller_dir = workspace.path().join("caller");
    let repo_dir = workspace.path().join("repo");
    let config_dir = repo_dir.join("config");
    std::fs::create_dir_all(&caller_dir).unwrap();
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(caller_dir.join(".snouty.yaml"), "source: from-cwd\n").unwrap();
    std::fs::write(config_dir.join("docker-compose.yaml"), "version: '3'\n").unwrap();
    std::fs::write(repo_dir.join(".snouty.yaml"), "source: x\nbogus: y\n").unwrap();

    snouty()
        .current_dir(&caller_dir)
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            "../repo/config",
            "--duration",
            "30",
            "--source",
            "ci",
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid config file"))
        .stderr(predicate::str::contains(
            repo_dir.join(".snouty.yaml").to_str().unwrap(),
        ));
}

#[test]
fn run_config_does_not_fall_back_to_caller_directory_tree() {
    let workspace = TempDir::new().unwrap();
    let caller_dir = workspace.path().join("caller");
    let config_dir = workspace.path().join("repo").join("config");
    std::fs::create_dir_all(&caller_dir).unwrap();
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(caller_dir.join(".snouty.yaml"), "source: x\nbogus: y\n").unwrap();
    std::fs::write(config_dir.join("docker-compose.yaml"), "version: '3'\n").unwrap();

    snouty()
        .current_dir(&caller_dir)
        .env("ANTITHESIS_REPOSITORY", "registry.example.com/repo")
        .args([
            "run",
            "-w",
            "basic_test",
            "-c",
            "../repo/config",
            "--duration",
            "30",
            "--source",
            "ci",
            "--param",
            "antithesis.config_image=some-image:latest",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be overridden via --param"))
        .stderr(predicate::str::contains("invalid config file").not());
}
