//! `docker-credential-snouty`, an alias of `snouty`, as Docker runs it.
#![cfg(unix)]

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use snouty::docker_credential::HELPER_NAME;
use snouty::testutils::fake_secrets::{FAKE_API_KEY_A, FAKE_BCRYPT_HASH};
use tempfile::TempDir;

const SNOUTY: &str = env!("CARGO_BIN_EXE_snouty");

/// A `docker-credential-snouty` alias of `snouty`, alone in a directory.
struct Alias {
    dir: TempDir,
    path: PathBuf,
}

impl Alias {
    fn symlink() -> Self {
        Self::new(|path| std::os::unix::fs::symlink(SNOUTY, path))
    }

    /// A copy has no link to follow back to `snouty`, so only argv[0] can
    /// select the helper.
    fn copy() -> Self {
        Self::new(|path| std::fs::copy(SNOUTY, path).map(drop))
    }

    fn new(make: impl FnOnce(&Path) -> std::io::Result<()>) -> Self {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(HELPER_NAME);
        make(&path).unwrap();
        Self { dir, path }
    }
}

/// The process at `bin`, in an empty working directory and home, with only the
/// `acme` tenant settings and an API key set.
fn helper_process(bin: &Path, home: &TempDir) -> std::process::Command {
    let mut command = std::process::Command::new(bin);
    command
        .current_dir(home.path())
        .env_clear()
        .env("HOME", home.path())
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .env("ANTITHESIS_TENANT", "acme")
        .env("ANTITHESIS_REPOSITORY", "registry.acme.antithesis.com/app")
        .env("ANTITHESIS_API_KEY", FAKE_API_KEY_A);
    command
}

/// [`helper_process`] for an alias that lives as long as the command.
fn helper(alias: &Alias, home: &TempDir) -> Command {
    Command::from_std(helper_process(&alias.path, home))
}

fn assert_get_prints_the_credential(command: &mut Command) {
    command
        .arg("get")
        .write_stdin("registry.acme.antithesis.com")
        .assert()
        .success()
        .stdout(format!(
            "{{\"ServerURL\":\"registry.acme.antithesis.com\",\"Username\":\"snouty-token\",\"Secret\":\"{FAKE_API_KEY_A}\"}}\n"
        ));
}

/// A symlink and a copy of `snouty` named `docker-credential-snouty` run the
/// helper. Each works alone in a directory, with no `snouty` on PATH.
#[test]
fn an_alias_of_snouty_prints_the_credential() {
    let home = TempDir::new().unwrap();
    for alias in [Alias::symlink(), Alias::copy()] {
        assert_get_prints_the_credential(helper(&alias, &home).env("PATH", alias.dir.path()));
    }
}

/// The file name of argv[0] selects the helper, not the path of the binary:
/// a full path and a bare name both run the helper.
#[test]
fn argv0_named_as_the_helper_runs_the_helper() {
    use std::os::unix::process::CommandExt;

    let home = TempDir::new().unwrap();
    let full_path = home.path().join("some/dir").join(HELPER_NAME);
    for argv0 in [full_path.as_os_str(), HELPER_NAME.as_ref()] {
        let mut command = helper_process(Path::new(SNOUTY), &home);
        command.arg0(argv0);
        assert_get_prints_the_credential(&mut Command::from_std(command));
    }
}

/// `snouty` runs the CLI under its own name, also through a symlink named as
/// the helper, and when only a directory of argv[0] has the helper's name.
#[test]
fn other_argv0_runs_the_cli() {
    use std::os::unix::process::CommandExt;

    let home = TempDir::new().unwrap();
    let alias = Alias::symlink();
    let in_helper_dir = home.path().join(HELPER_NAME).join("snouty");
    for (bin, argv0) in [
        (Path::new(SNOUTY), None),
        (alias.path.as_path(), Some(Path::new("snouty"))),
        (Path::new(SNOUTY), Some(in_helper_dir.as_path())),
    ] {
        let mut command = helper_process(bin, &home);
        if let Some(argv0) = argv0 {
            command.arg0(argv0);
        }
        Command::from_std(command)
            .arg("get")
            .write_stdin("registry.acme.antithesis.com")
            .assert()
            .code(2)
            .stdout("")
            .stderr(predicates::str::contains(
                "error: unrecognized subcommand 'get'",
            ));
    }
}

#[test]
fn not_found_exits_1_with_no_stderr() {
    let home = TempDir::new().unwrap();
    helper(&Alias::symlink(), &home)
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
    let alias = Alias::symlink();
    for server in ["registry.acme.antithesis.com", "registry.example.com"] {
        let (reader, writer) = std::io::pipe().unwrap();
        drop(reader);
        let output = helper_process(&alias.path, &home)
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
        helper(&Alias::symlink(), &home)
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
    helper(&Alias::symlink(), &home)
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
    helper(&Alias::symlink(), &home)
        .arg("erase")
        .write_stdin("registry.acme.antithesis.com")
        .assert()
        .success()
        .stdout("");
}

/// Each engine pushes to and pulls from a registry that takes basic auth,
/// through `"credHelpers": {"<registry>": "snouty"}` in the file that the
/// engine reads: `config.json` in `DOCKER_CONFIG` for Docker, and the
/// `REGISTRY_AUTH_FILE` for podman.
///
/// The helper gives a credential only for `registry.<tenant>.antithesis.com`,
/// and the engine must reach the registry at `127.0.0.1`. So the
/// `docker-credential-snouty` that the engine finds on PATH is a script that
/// changes the address to `registry.acme.antithesis.com` on stdin, in the
/// format the engine wrote it, and then runs a symlink to `snouty` named
/// `docker-credential-snouty`.
#[test]
fn engines_push_and_pull_through_the_helper() {
    for runtime in snouty::testutils::require_runtimes() {
        push_and_pull_through_the_helper(runtime.as_ref());
    }
}

fn push_and_pull_through_the_helper(runtime: &dyn snouty::container::ContainerRuntime) {
    use std::os::unix::fs::PermissionsExt;
    use std::process::Output;

    let engine = runtime.name();
    let Some(registry) = snouty::testutils::OCIRegistry::start_with_basic_auth(
        runtime,
        &format!("snouty-token:{FAKE_BCRYPT_HASH}"),
    ) else {
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
    let alias = Alias::symlink();
    let helper = bin.join(HELPER_NAME);
    std::fs::write(
        &helper,
        format!(
            "#!/bin/sh\n\
             sed 's/{addr}/registry.acme.antithesis.com/' | exec env -i \
             HOME='{home}' SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1 \
             ANTITHESIS_TENANT=acme \
             ANTITHESIS_REPOSITORY=registry.acme.antithesis.com/app \
             ANTITHESIS_API_KEY={FAKE_API_KEY_A} '{helper}' \"$@\"\n",
            addr = addr.replace('.', r"\."),
            home = home.display(),
            helper = alias.path.display(),
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

    // The image stays in the local store, so that no test removes an image
    // while the others build (see `remove_test_images`). A pull still asks the
    // registry for the manifest, so it still needs the credential.
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
    snouty::testutils::remove_test_images(engine, &[&image]);
}
