//! `docker-credential-snouty`, an alias of `snouty`, as Docker runs it.
#![cfg(unix)]

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use snouty::docker_credential::{CRED_HELPER, HELPER_NAME};
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
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(HELPER_NAME);
        std::os::unix::fs::symlink(SNOUTY, &path).unwrap();
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

/// A symlink to `snouty` named `docker-credential-snouty` runs the helper. It
/// works alone in a directory, with no `snouty` on PATH.
#[test]
fn an_alias_of_snouty_prints_the_credential() {
    let home = TempDir::new().unwrap();
    let alias = Alias::symlink();
    assert_get_prints_the_credential(helper(&alias, &home).env("PATH", alias.dir.path()));
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

/// `snouty` runs the CLI when argv[0] is `snouty`, also through a symlink
/// named as the helper, and when only a directory of argv[0] has the helper's
/// name.
#[test]
fn other_argv0_runs_the_cli() {
    use std::os::unix::process::CommandExt;

    let home = TempDir::new().unwrap();
    let alias = Alias::symlink();
    let in_helper_dir = home.path().join(HELPER_NAME).join("snouty");
    for (bin, argv0) in [
        (alias.path.as_path(), Path::new("snouty")),
        (Path::new(SNOUTY), in_helper_dir.as_path()),
    ] {
        let mut command = helper_process(bin, &home);
        command.arg0(argv0);
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
        ("store", "snouty provides credentials for this registry"),
        (
            "list",
            "the snouty credential helper is not a general-purpose",
        ),
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
/// through a `"credHelpers"` entry for [`CRED_HELPER`] in the file that the
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
    snouty::testutils::write_script(
        &helper,
        &format!(
            "sed 's/{addr}/registry.acme.antithesis.com/' | exec env -i \
             HOME='{home}' SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1 \
             ANTITHESIS_TENANT=acme \
             ANTITHESIS_REPOSITORY=registry.acme.antithesis.com/app \
             ANTITHESIS_API_KEY={FAKE_API_KEY_A} '{helper}' \"$@\"\n",
            addr = addr.replace('.', r"\."),
            home = home.display(),
            helper = alias.path.display(),
        ),
    );
    let path = std::env::join_paths(
        std::iter::once(bin).chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();

    let with_helper = dir.path().join("with-helper");
    let without_helper = dir.path().join("without-helper");
    for (config, contents) in [
        (
            &with_helper,
            serde_json::json!({"credHelpers": {&addr: CRED_HELPER}}),
        ),
        (&without_helper, serde_json::json!({})),
    ] {
        std::fs::create_dir(config).unwrap();
        std::fs::write(config.join("config.json"), contents.to_string()).unwrap();
    }

    let run = |config: &Path, action: &str| -> Output {
        let mut command = std::process::Command::new(runtime.name());
        if runtime.engine_kind() == "podman" {
            command.args([action, "--tls-verify=false", &image]);
            command.env("REGISTRY_AUTH_FILE", config.join("config.json"));
        } else {
            command.args([action, &image]);
            command.env("DOCKER_CONFIG", config);
        }
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

    // The image is still in the local store. A pull still asks the registry
    // for the manifest, so it still needs the credential.
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

/// `snouty launch --config` pushes the config image to the tenant registry
/// with the credential of the launch, in each way that the engine can get it:
///
/// - Docker 28.3.0 and later get it in `DOCKER_AUTH_CONFIG`. On an older
///   Docker, the test skips this case.
/// - A Docker before 28.3.0, which the engine script simulates on any Docker
///   as 28.0.4, ignores that variable, and gets it from
///   `docker-credential-snouty` through `"credHelpers": {"<registry>":
///   "snouty"}` in the `config.json` of `DOCKER_CONFIG`.
/// - That old Docker with no entry for the registry in its config has no
///   credential, so the registry refuses the push, and the launch fails with
///   the error of Docker before it submits the run.
/// - Podman gets it from `docker-credential-snouty`, which the launch links
///   in the global settings dir and puts first on the `PATH` of podman,
///   because no helper is on the `PATH` of the launch.
///
/// The tenant and the repository come from the trusted `./.snouty.toml` of
/// the launch, which each helper reads, because it runs in the working
/// directory of the launch. During the push, no file in `TMPDIR` (where
/// snouty keeps the auth file), `$XDG_CONFIG_HOME` (where it links the
/// helper) or the working directory holds the secret.
///
/// The registry is at `127.0.0.1`, but the credential is only for
/// `registry.acme.antithesis.com`. So the engine that snouty finds on PATH is
/// a script that changes the host to the address in each argument, in
/// `DOCKER_AUTH_CONFIG`, in the test's Docker config, and in the auth file,
/// and then runs the real engine. Each `docker-credential-snouty` of the test
/// changes the address back on stdin. For podman, the script puts that helper
/// first on PATH, and the helper then runs the helper of the directory that is
/// next on PATH, which must be the one snouty added.
#[test]
fn engines_push_with_the_registry_auth() {
    for runtime in snouty::testutils::require_runtimes() {
        launch_pushes_with_the_registry_auth(runtime.as_ref());
    }
}

const TENANT_HOST: &str = "registry.acme.antithesis.com";

/// How the engine of a launch gets the credential.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AuthCase {
    Podman,
    /// The real Docker, with `DOCKER_AUTH_CONFIG`.
    DockerAuthConfig,
    /// A simulated old Docker, with a `credHelpers` entry in its config.
    OldDockerCredHelpers,
    /// A simulated old Docker, with no entry for the registry in its config.
    OldDockerNoCredential,
}

impl AuthCase {
    fn old_docker(self) -> bool {
        matches!(
            self,
            Self::OldDockerCredHelpers | Self::OldDockerNoCredential
        )
    }
}

fn launch_pushes_with_the_registry_auth(runtime: &dyn snouty::container::ContainerRuntime) {
    use base64::{Engine as _, prelude::BASE64_STANDARD};

    let engine = runtime.name();
    let Some(registry) = snouty::testutils::OCIRegistry::start_with_basic_auth(
        runtime,
        &format!("snouty-token:{FAKE_BCRYPT_HASH}"),
    ) else {
        return;
    };
    let addr = registry.host_port();
    let real_engine = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|dir| dir.join(engine))
        .find(|path| path.is_file())
        .unwrap();
    let dir = TempDir::new().unwrap();
    let [
        work,
        wrap_bin,
        rewrite_bin,
        docker_helper_bin,
        tmp,
        xdg_config,
        cred_helpers_config,
        engine_cred_helpers_config,
        empty_config,
    ] = [
        "work",
        "wrap-bin",
        "rewrite-bin",
        "docker-helper-bin",
        "tmp",
        "xdg-config",
        "cred-helpers-config",
        "engine-cred-helpers-config",
        "empty-config",
    ]
    .map(|name| {
        let path = dir.path().join(name);
        std::fs::create_dir(&path).unwrap();
        path
    });
    std::fs::write(
        cred_helpers_config.join("config.json"),
        serde_json::json!({"credHelpers": {TENANT_HOST: "snouty"}}).to_string(),
    )
    .unwrap();
    // Rootless podman reads its configuration, such as its storage, here.
    if let Some(home) = std::env::var_os("HOME") {
        let containers = Path::new(&home).join(".config/containers");
        if containers.is_dir() {
            std::os::unix::fs::symlink(containers, xdg_config.join("containers")).unwrap();
        }
    }
    let manifests = work.join("k8s_config/manifests");
    std::fs::create_dir_all(&manifests).unwrap();
    std::fs::write(manifests.join("ns.yaml"), "kind: Namespace\n").unwrap();
    let repo = snouty::testutils::unique_image_prefix(&format!("launch-auth-{engine}"));
    std::fs::write(
        work.join(".snouty.toml"),
        format!("tenant = \"acme\"\nrepository = \"{TENANT_HOST}/{repo}\"\n"),
    )
    .unwrap();
    // The launch and each helper use ./.snouty.toml only because the user
    // trusts it: the trust decisions are in the global settings dir, which
    // both read.
    let trust = std::process::Command::new(SNOUTY)
        .current_dir(&work)
        .env("XDG_CONFIG_HOME", &xdg_config)
        .env_remove("SNOUTY_SETTINGS_TRUST")
        .args(["settings", "trust"])
        .output()
        .unwrap();
    assert!(
        trust.status.success(),
        "{}",
        String::from_utf8_lossy(&trust.stderr)
    );

    let to_addr = format!("s/{}/{addr}/g", TENANT_HOST.replace('.', r"\."));
    let to_host = format!("s/{}/{TENANT_HOST}/", addr.replace('.', r"\."));
    let found = dir.path().join("secret-found");
    let helper_log = dir.path().join("docker-helper-log");
    let basic_auth = BASE64_STANDARD.encode(format!("snouty-token:{FAKE_API_KEY_A}"));
    let write_engine = |case: AuthCase| {
        let old_docker = if case.old_docker() {
            "if [ \"$1\" = --version ]; then echo 'Docker version 28.0.4, build b8034c0'; exit; fi\n\
             unset DOCKER_AUTH_CONFIG\n"
        } else {
            ""
        };
        snouty::testutils::write_script(
            &wrap_bin.join(engine),
            &format!(
                r#"{old_docker}for arg; do shift; set -- "$@" "$(printf '%s' "$arg" | sed '{to_addr}')"; done
if [ -n "${{DOCKER_AUTH_CONFIG-}}" ]; then
  DOCKER_AUTH_CONFIG=$(printf '%s' "$DOCKER_AUTH_CONFIG" | sed '{to_addr}')
fi
if [ "${{DOCKER_CONFIG-}}" = '{cred_helpers_config}' ]; then
  sed '{to_addr}' '{cred_helpers_config}/config.json' > '{engine_cred_helpers_config}/config.json'
  DOCKER_CONFIG='{engine_cred_helpers_config}'
fi
if [ -n "${{REGISTRY_AUTH_FILE-}}" ]; then
  sed '{to_addr}' "$REGISTRY_AUTH_FILE" > '{auth}'
  REGISTRY_AUTH_FILE='{auth}'
  PATH='{rewrite_bin}':$PATH
  if [ "$1" = push ]; then shift; set -- push --tls-verify=false "$@"; fi
fi
if [ "$1" = push ]; then
  grep -rlF -D skip -e '{FAKE_API_KEY_A}' -e '{basic_auth}' "$TMPDIR" "$XDG_CONFIG_HOME" '{work}' >> '{found}'
fi
exec '{real}' "$@"
"#,
                cred_helpers_config = cred_helpers_config.display(),
                engine_cred_helpers_config = engine_cred_helpers_config.display(),
                auth = dir.path().join("auth.json").display(),
                rewrite_bin = rewrite_bin.display(),
                found = found.display(),
                work = work.display(),
                real = real_engine.display(),
            ),
        );
    };
    snouty::testutils::write_script(
        &rewrite_bin.join(HELPER_NAME),
        &format!(
            r#"PATH=${{PATH#'{rewrite_bin}':}}
sed '{to_host}' | exec "${{PATH%%:*}}/{HELPER_NAME}" "$@"
"#,
            rewrite_bin = rewrite_bin.display(),
        ),
    );
    // Docker runs the helper of a credHelpers entry with the environment and
    // the working directory of the launch.
    let alias = Alias::symlink();
    snouty::testutils::write_script(
        &docker_helper_bin.join(HELPER_NAME),
        &format!(
            r#"printf '%s\n' "$1" >> '{log}'
sed '{to_host}' | exec '{helper}' "$@"
"#,
            log = helper_log.display(),
            helper = alias.path.display(),
        ),
    );
    // Podman must not find the Docker helper of the test, so that the launch
    // links its own.
    let path = |helper_bin: Option<PathBuf>| {
        std::env::join_paths(
            std::iter::once(wrap_bin.clone())
                .chain(helper_bin)
                .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
        )
        .unwrap()
    };

    let cases = if runtime.engine_kind() == "podman" {
        vec![AuthCase::Podman]
    } else if docker_reads_auth_config(&real_engine) {
        vec![
            AuthCase::DockerAuthConfig,
            AuthCase::OldDockerCredHelpers,
            AuthCase::OldDockerNoCredential,
        ]
    } else {
        eprintln!(
            "note: skip the DockerAuthConfig case: Docker before 28.3.0 ignores DOCKER_AUTH_CONFIG"
        );
        vec![
            AuthCase::OldDockerCredHelpers,
            AuthCase::OldDockerNoCredential,
        ]
    };
    let mut built_images = Vec::new();
    for case in cases {
        write_engine(case);
        let mut command = std::process::Command::new(SNOUTY);
        for var in [
            "ANTITHESIS_TENANT",
            "ANTITHESIS_REPOSITORY",
            "ANTITHESIS_PROFILE",
            "ANTITHESIS_USERNAME",
            "ANTITHESIS_PASSWORD",
            "ANTITHESIS_HTTPS_PROXY",
            "SNOUTY_SETTINGS_PATH",
            "SNOUTY_SETTINGS_TRUST",
            "REGISTRY_AUTH_FILE",
            "DOCKER_AUTH_CONFIG",
        ] {
            command.env_remove(var);
        }
        match case {
            AuthCase::OldDockerCredHelpers => {
                command.env("DOCKER_CONFIG", &cred_helpers_config);
            }
            AuthCase::OldDockerNoCredential => {
                command.env("DOCKER_CONFIG", &empty_config);
            }
            AuthCase::Podman | AuthCase::DockerAuthConfig => {}
        }
        let output = command
            .current_dir(&work)
            .env(
                "PATH",
                path((case != AuthCase::Podman).then(|| docker_helper_bin.clone())),
            )
            .env("TMPDIR", &tmp)
            .env("XDG_CONFIG_HOME", &xdg_config)
            .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
            .env("SNOUTY_CONTAINER_ENGINE", engine)
            .env("ANTITHESIS_API_KEY", FAKE_API_KEY_A)
            // Nothing answers there, so the launch fails after the push.
            .env("ANTITHESIS_BASE_URL", "http://127.0.0.1:9")
            .args([
                "launch",
                "--launcher",
                "basic_k8s_test",
                "-c",
                "k8s_config",
                "--duration",
                "30",
            ])
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        // The engine script stores each config image under the registry address.
        built_images.extend(
            stderr
                .lines()
                .find_map(|line| line.strip_prefix("Building config image: "))
                .map(|image| image.replace(TENANT_HOST, &addr)),
        );
        if case == AuthCase::OldDockerNoCredential {
            // The error of the push ends the launch, so it submits no run.
            assert!(
                !output.status.success()
                    && stderr.contains("Error: 'docker push' failed")
                    && stderr.contains("no basic auth credentials")
                    && !stderr.contains("Config image pushed successfully"),
                "{engine} {case:?}: a push with no credential\n{stderr}"
            );
            continue;
        }
        assert!(
            stderr.contains("Config image pushed successfully"),
            "{engine} {case:?}: a push with the key\n{stderr}"
        );
        // Only the credHelpers entry makes Docker run the helper of the test.
        let calls = std::fs::read_to_string(&helper_log).unwrap_or_default();
        if case == AuthCase::OldDockerCredHelpers {
            assert!(
                calls.lines().any(|action| action == "get"),
                "{engine} {case:?}: Docker did not get the credential from the helper: {calls:?}"
            );
        } else {
            assert_eq!(calls, "", "{engine} {case:?}: calls to the Docker helper");
        }
    }

    let built_images: Vec<&str> = built_images.iter().map(String::as_str).collect();
    snouty::testutils::remove_test_images(engine, &built_images);

    let found = std::fs::read_to_string(&found).unwrap_or_default();
    assert_eq!(
        found, "",
        "{engine}: files that hold the secret during the push"
    );
}

/// Whether the Docker client at `docker` is 28.3.0 or later, and so reads
/// `DOCKER_AUTH_CONFIG`.
fn docker_reads_auth_config(docker: &Path) -> bool {
    let output = std::process::Command::new(docker)
        .args(["version", "--format", "{{.Client.Version}}"])
        .output()
        .unwrap();
    let version = String::from_utf8_lossy(&output.stdout);
    let version = semver::Version::parse(version.trim())
        .unwrap_or_else(|err| panic!("Docker client version {version:?}: {err}"));
    version >= semver::Version::new(28, 3, 0)
}
