//! `docker-credential-snouty`, as Docker runs it.

use std::path::Path;

use assert_cmd::Command;
use tempfile::TempDir;

/// The helper at `bin`, in an empty working directory and home, with only the
/// `acme` tenant settings and an API key set.
fn helper_at(bin: &Path, home: &TempDir) -> Command {
    Command::from_std(helper_process(bin, home))
}

/// [`helper_at`], for a test that sets up the helper's stdout itself.
fn helper_process(bin: &Path, home: &TempDir) -> std::process::Command {
    let mut command = std::process::Command::new(bin);
    command
        .current_dir(home.path())
        .env_clear()
        .env("HOME", home.path())
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .env("ANTITHESIS_TENANT", "acme")
        .env("ANTITHESIS_REPOSITORY", "registry.acme.antithesis.com/app")
        .env("ANTITHESIS_API_KEY", "helper-key");
    command
}

fn helper(home: &TempDir) -> Command {
    helper_at(
        Path::new(env!("CARGO_BIN_EXE_docker-credential-snouty")),
        home,
    )
}

/// get prints the credential. The helper runs no other binary: it works alone
/// in a directory, with no `snouty` on PATH, and a `snouty` next to it or on
/// PATH does not change its answer.
#[cfg(unix)]
#[test]
fn get_prints_the_credential_and_runs_no_snouty() {
    use std::os::unix::fs::PermissionsExt;

    let home = TempDir::new().unwrap();
    let dir = TempDir::new().unwrap();
    let bin = dir.path().join("docker-credential-snouty");
    std::fs::copy(env!("CARGO_BIN_EXE_docker-credential-snouty"), &bin).unwrap();
    let get = || {
        helper_at(&bin, &home)
            .env("PATH", dir.path())
            .arg("get")
            .write_stdin("registry.acme.antithesis.com")
            .assert()
            .success()
            .stdout(
                "{\"ServerURL\":\"registry.acme.antithesis.com\",\"Username\":\"snouty-token\",\"Secret\":\"helper-key\"}\n",
            );
    };
    get();

    let snouty = dir.path().join("snouty");
    std::fs::write(&snouty, "#!/bin/sh\necho decoy\n").unwrap();
    std::fs::set_permissions(&snouty, std::fs::Permissions::from_mode(0o755)).unwrap();
    get();
}

#[test]
fn not_found_exits_1_with_no_stderr() {
    let home = TempDir::new().unwrap();
    helper(&home)
        .arg("get")
        .write_stdin("registry.example.com")
        .assert()
        .code(1)
        .stdout("credentials not found in native keychain\n")
        .stderr("");
}

/// Docker can close the pipe before it reads the answer. The helper then
/// fails with exit status 1, and does not panic.
#[test]
fn a_closed_stdout_fails_get_without_a_panic() {
    let home = TempDir::new().unwrap();
    for server in ["registry.acme.antithesis.com", "registry.example.com"] {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let output = helper_process(
            Path::new(env!("CARGO_BIN_EXE_docker-credential-snouty")),
            &home,
        )
        .arg("get")
        .stdin(std::process::Stdio::piped())
        .stdout(writer)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(server.as_bytes())?;
            child.wait_with_output()
        })
        .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "stderr:\n{stderr}");
        assert!(!stderr.contains("panicked"), "stderr:\n{stderr}");
    }
}

/// store and list fail with exit status 1, and with the message on stdout,
/// where the engine shows it, and on stderr.
#[test]
fn store_and_list_fail_with_status_1() {
    let home = TempDir::new().unwrap();
    for (action, message) in [
        ("store", "snouty gives the credential for this registry"),
        ("list", "list is not necessary"),
    ] {
        helper(&home)
            .arg(action)
            .write_stdin("registry.acme.antithesis.com")
            .assert()
            .code(1)
            .stdout(predicates::str::starts_with(message))
            .stderr(predicates::str::starts_with(format!("Error: {message}")));
    }
}

/// An action that the helper does not have fails like every other error.
#[test]
fn an_unknown_action_fails_with_status_1() {
    let home = TempDir::new().unwrap();
    helper(&home)
        .arg("version")
        .assert()
        .code(1)
        .stdout(predicates::str::starts_with(
            "error: invalid value 'version'",
        ));
}

#[test]
fn erase_reads_stdin_and_succeeds() {
    let home = TempDir::new().unwrap();
    helper(&home)
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
/// format the engine wrote it, and then runs the real helper.
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
             ANTITHESIS_API_KEY={REGISTRY_API_KEY} '{helper}' \"$@\"\n",
            addr = addr.replace('.', r"\."),
            home = home.display(),
            helper = env!("CARGO_BIN_EXE_docker-credential-snouty"),
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
