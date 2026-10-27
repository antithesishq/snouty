//! `docker-credential-snouty`, as Docker runs it: the shim runs the `snouty`
//! next to it, with the same stdin, stdout and exit status.

use assert_cmd::Command;
use tempfile::TempDir;

/// The shim, in an empty working directory and home, with only the `acme`
/// tenant settings and an API key set.
fn shim(home: &TempDir) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_docker-credential-snouty"));
    command
        .current_dir(home.path())
        .env_clear()
        .env("HOME", home.path())
        // No PATH: the shim must find the `snouty` next to it.
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .env("ANTITHESIS_TENANT", "acme")
        .env("ANTITHESIS_REPOSITORY", "registry.acme.antithesis.com/app")
        .env("ANTITHESIS_API_KEY", "shim-key");
    command
}

#[test]
fn get_prints_the_credential() {
    let home = TempDir::new().unwrap();
    shim(&home)
        .arg("get")
        .write_stdin("registry.acme.antithesis.com")
        .assert()
        .success()
        .stdout(
            "{\"ServerURL\":\"registry.acme.antithesis.com\",\"Username\":\"snouty-token\",\"Secret\":\"shim-key\"}\n",
        );
}

#[test]
fn not_found_passes_through_with_its_exit_status() {
    let home = TempDir::new().unwrap();
    shim(&home)
        .arg("get")
        .write_stdin("registry.example.com")
        .assert()
        .code(1)
        .stdout("credentials not found in native keychain\n")
        .stderr("");
}

/// store and list fail with exit status 1, and with the message on stdout,
/// where the engine shows it.
#[test]
fn store_and_list_fail_with_status_1() {
    let home = TempDir::new().unwrap();
    for (action, message) in [
        ("store", "snouty gives the credential for this registry"),
        ("list", "list is not necessary"),
    ] {
        shim(&home)
            .arg(action)
            .write_stdin("registry.acme.antithesis.com")
            .assert()
            .code(1)
            .stdout(predicates::str::starts_with(message));
    }
}

#[test]
fn erase_reads_stdin_and_succeeds() {
    let home = TempDir::new().unwrap();
    shim(&home)
        .arg("erase")
        .write_stdin("registry.acme.antithesis.com")
        .assert()
        .success()
        .stdout("");
}

/// The test registry's password: the API key that the helper gives.
#[cfg(unix)]
const REGISTRY_API_KEY: &str = "snouty-test-api-key";

/// `snouty-token` with a bcrypt hash of [`REGISTRY_API_KEY`], as
/// `htpasswd -nbB` writes it.
#[cfg(unix)]
const REGISTRY_HTPASSWD: &str =
    "snouty-token:$2a$05$vr70JMlnyI5b0YbJ0mm4v.RmWKdovtX9xcIDaLMU5/ixzJX6Z5Y5O";

/// Each engine pushes to and pulls from a registry that takes basic auth,
/// through `"credHelpers": {"<registry>": "snouty"}` in the file that the
/// engine reads: `config.json` in `DOCKER_CONFIG` for Docker, and the
/// `REGISTRY_AUTH_FILE` for podman.
///
/// The helper gives a credential only for `registry.<tenant>.antithesis.com`,
/// and the engine must reach the registry at `127.0.0.1`. So the
/// `docker-credential-snouty` that the engine finds on PATH is a script that
/// changes the address to `registry.acme.antithesis.com` on stdin, in the
/// format the engine wrote it, and then runs the real shim.
#[cfg(unix)]
#[test]
fn engines_push_and_pull_through_the_helper() {
    for runtime in snouty::testutils::require_runtimes() {
        push_and_pull_through_the_helper(runtime.as_ref());
    }
}

#[cfg(unix)]
fn push_and_pull_through_the_helper(runtime: &dyn snouty::container::ContainerRuntime) {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::Output;

    let engine = runtime.name();
    let Some(registry) =
        snouty::testutils::OCIRegistry::start_with_basic_auth(runtime, REGISTRY_HTPASSWD)
    else {
        return;
    };
    let addr = registry.host_port();
    let dir = TempDir::new().unwrap();

    let context = dir.path().join("context");
    std::fs::create_dir(&context).unwrap();
    std::fs::write(context.join("hello.txt"), "hello").unwrap();
    let repo = snouty::testutils::unique_image_prefix(&format!("credential-{engine}"));
    let image = format!("{addr}/{repo}/app:latest");
    runtime
        .build_image(&context, &image, None, None)
        .unwrap_or_else(|e| panic!("{engine}: {e:?}"));

    let bin = dir.path().join("bin");
    let home = dir.path().join("home");
    std::fs::create_dir(&bin).unwrap();
    std::fs::create_dir(&home).unwrap();
    let helper = bin.join("docker-credential-snouty");
    std::fs::write(
        &helper,
        format!(
            "#!/bin/sh\n\
             sed 's/{addr}/registry.acme.antithesis.com/' | exec env -i \
             HOME='{home}' SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1 \
             ANTITHESIS_TENANT=acme \
             ANTITHESIS_REPOSITORY=registry.acme.antithesis.com/app \
             ANTITHESIS_API_KEY={REGISTRY_API_KEY} '{shim}' \"$@\"\n",
            addr = addr.replace('.', r"\."),
            home = home.display(),
            shim = env!("CARGO_BIN_EXE_docker-credential-snouty"),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = std::env::join_paths(
        std::iter::once(bin).chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();

    let with_helper = dir.path().join("with-helper");
    let without_helper = dir.path().join("without-helper");
    for (config, contents) in [
        (
            &with_helper,
            serde_json::json!({"credHelpers": {&addr: "snouty"}}),
        ),
        (&without_helper, serde_json::json!({})),
    ] {
        std::fs::create_dir(config).unwrap();
        std::fs::write(config.join("config.json"), contents.to_string()).unwrap();
    }

    let run = |config: &Path, action: &str| -> Output {
        let mut command = if runtime.engine_kind() == "podman" {
            let mut command = runtime.command(&[action, "--tls-verify=false", &image]);
            command.env("REGISTRY_AUTH_FILE", config.join("config.json"));
            command
        } else {
            let mut command = runtime.command(&[action, &image]);
            command.env("DOCKER_CONFIG", config);
            command
        };
        command.env("PATH", &path).output().unwrap()
    };
    let remove_image = || {
        let _ = runtime.command(&["rmi", &image]).output();
    };
    let describe = |output: &Output| {
        format!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };

    let output = run(&without_helper, "push");
    assert!(
        !output.status.success(),
        "{engine}: push without the helper\n{}",
        describe(&output)
    );
    let output = run(&with_helper, "push");
    assert!(
        output.status.success(),
        "{engine}: push with the helper\n{}",
        describe(&output)
    );

    remove_image();
    let output = run(&without_helper, "pull");
    assert!(
        !output.status.success(),
        "{engine}: pull without the helper\n{}",
        describe(&output)
    );
    let output = run(&with_helper, "pull");
    assert!(
        output.status.success(),
        "{engine}: pull with the helper\n{}",
        describe(&output)
    );
    remove_image();
}
