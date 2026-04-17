use snouty::testutils::{
    MockApiServer, OCIRegistry, available_runtimes, filtered_path_without_binary, skip_or_fail,
};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Stdio;
use std::thread;
use testscript_rs::testscript;

fn err(msg: String) -> testscript_rs::Error {
    testscript_rs::Error::Generic(msg)
}

// --- Engine context (thread-local so fn-pointer commands can access it) ---

struct EngineContext {
    engine: Box<dyn snouty::container::ContainerRuntime>,
    built_images: Vec<String>,
}

#[derive(Clone, Copy)]
struct EngineSpecCase {
    file: &'static str,
    needs_registry: bool,
}

thread_local! {
    static ENGINE_CTX: RefCell<Option<EngineContext>> = const { RefCell::new(None) };
}

// --- Shared command handlers (function pointers for testscript CommandFn) ---

/// System env vars forwarded to child processes (container tools, coverage).
const FORWARDED_ENV_VARS: &[&str] = &["PATH", "HOME", "LLVM_PROFILE_FILE"];

/// Build a `Command` for the snouty binary with a clean environment.
///
/// Clears the parent env, forwards [`FORWARDED_ENV_VARS`], and applies
/// the test environment's `env_vars`.
fn snouty_cmd(env: &testscript_rs::TestEnvironment, args: &[String]) -> std::process::Command {
    let bin = env!("CARGO_BIN_EXE_snouty");
    let mut cmd = std::process::Command::new(bin);
    cmd.args(args)
        .current_dir(&env.current_dir)
        .env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for var in FORWARDED_ENV_VARS {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
    for (k, v) in &env.env_vars {
        cmd.env(k, v);
    }
    cmd
}

fn cmd_snouty(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let start = std::time::Instant::now();
    let expanded: Vec<String> = args.iter().map(|a| env.substitute_env_vars(a)).collect();
    let label = expanded.join(" ");
    let mut cmd = snouty_cmd(env, &expanded);
    if env.next_stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }

    let mut child = cmd.spawn().map_err(|e| err(format!("spawn snouty: {e}")))?;
    if let Some(data) = env.next_stdin.take() {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&data)
            .map_err(|e| err(format!("write stdin: {e}")))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|e| err(format!("wait snouty: {e}")))?;
    eprintln!("[{:.1}s] snouty {label}", start.elapsed().as_secs_f64());
    let success = output.status.success();
    let stderr_str = String::from_utf8_lossy(&output.stderr).into_owned();
    let stdout_str = String::from_utf8_lossy(&output.stdout).into_owned();
    env.last_output = Some(output);
    if !success {
        return Err(err(format!(
            "snouty exited with non-zero status\nstderr:\n{stderr_str}\nstdout:\n{stdout_str}"
        )));
    }
    Ok(())
}

fn cmd_mock_server(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: mock-server <status> <body>
    // Starts a TCP mock HTTP server, sets ANTITHESIS_BASE_URL and auth env vars.
    if args.len() < 2 {
        return Err(err("mock-server requires <status> <body>".to_string()));
    }

    if is_staging() {
        propagate_antithesis_env(env)?;
        return Ok(());
    }

    let status: u16 = args[0]
        .parse()
        .map_err(|e| err(format!("invalid status code: {e}")))?;
    let body = args[1..].join(" ");

    let listener =
        TcpListener::bind("127.0.0.1:0").map_err(|e| err(format!("failed to bind: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| err(format!("failed to get addr: {e}")))?;
    let url = format!("http://{addr}");

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = Read::read(&mut stream, &mut buf);

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len(),
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });

    env.set_env_var("ANTITHESIS_BASE_URL", &url);
    env.set_env_var("ANTITHESIS_USERNAME", "testuser");
    env.set_env_var("ANTITHESIS_PASSWORD", "testpass");
    env.set_env_var("ANTITHESIS_TENANT", "testtenant");
    Ok(())
}

fn cmd_env_from_json(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: env_from_json <line_index> <json_key>
    // Parses the previous command's stdout as NDJSON, extracts <json_key>
    // from line <line_index> (0-based) and stores it as $R_<json_key>.
    if args.len() != 2 {
        return Err(err(
            "env_from_json requires <line_index> <json_key>".to_string(),
        ));
    }
    let line_idx: usize = args[0]
        .parse()
        .map_err(|_| err("line_index must be a non-negative integer".to_string()))?;
    let key = &args[1];

    let output = env
        .last_output
        .as_ref()
        .ok_or_else(|| err("no previous command output".to_string()))?;
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|e| err(format!("stdout is not valid UTF-8: {e}")))?;
    let lines: Vec<&str> = stdout.lines().collect();
    let line = lines.get(line_idx).ok_or_else(|| {
        err(format!(
            "stdout has only {} line(s); cannot read line {}",
            lines.len(),
            line_idx
        ))
    })?;
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|e| err(format!("parse JSON line: {e}")))?;
    let extracted = match value.get(key) {
        Some(serde_json::Value::Null) | None => {
            return Err(err(format!("key '{key}' not found in JSON")));
        }
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    };
    env.set_env_var(&format!("R_{key}"), &extracted);
    Ok(())
}

fn cmd_mock_runs_server(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let empty = match args {
        [] => false,
        [mode] if mode == "empty" => true,
        _ => {
            return Err(err(
                "mock-runs-server accepts either no arguments or 'empty'".to_string(),
            ));
        }
    };

    if is_staging() {
        if empty {
            return Err(err(
                "mock-runs-server empty is not supported against staging; gate the block with [!staging]".to_string(),
            ));
        }
        propagate_antithesis_env(env)?;
        return Ok(());
    }

    let server = if empty {
        MockApiServer::start_empty()
    } else {
        MockApiServer::start()
    };
    env.set_env_var("ANTITHESIS_BASE_URL", server.url());
    env.set_env_var("ANTITHESIS_API_KEY", server.token());
    env.set_env_var("ANTITHESIS_TENANT", "testtenant");
    env.last_output = Some(std::process::Output {
        status: std::process::ExitStatus::default(),
        stdout: format!("{}\n", server.token()).into_bytes(),
        stderr: Vec::new(),
    });
    std::mem::forget(server);
    Ok(())
}

fn is_staging() -> bool {
    std::env::var("SNOUTY_STAGING")
        .ok()
        .is_some_and(|v| !v.is_empty() && v != "0")
}

fn propagate_antithesis_env(
    env: &mut testscript_rs::TestEnvironment,
) -> testscript_rs::Result<()> {
    let tenant = std::env::var("ANTITHESIS_TENANT")
        .map_err(|_| err("SNOUTY_STAGING is set but ANTITHESIS_TENANT is not".to_string()))?;
    let has_bearer = std::env::var("ANTITHESIS_API_KEY").is_ok();
    let has_basic = std::env::var("ANTITHESIS_USERNAME").is_ok()
        && std::env::var("ANTITHESIS_PASSWORD").is_ok();
    if !has_bearer && !has_basic {
        return Err(err(
            "SNOUTY_STAGING is set but no credentials found (set ANTITHESIS_API_KEY or ANTITHESIS_USERNAME+ANTITHESIS_PASSWORD)"
                .to_string(),
        ));
    }
    for var in [
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_API_KEY",
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
        "ANTITHESIS_TENANT",
    ] {
        env.env_vars.remove(var);
    }
    env.set_env_var("ANTITHESIS_TENANT", &tenant);
    for var in [
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_API_KEY",
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
    ] {
        if let Ok(v) = std::env::var(var) {
            env.set_env_var(var, &v);
        }
    }
    Ok(())
}

fn cmd_build_image(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: build-image [--platform <platform>] <name:tag> <dir>
    // Builds a container image from <dir> (relative to work_dir), tagged as
    // {registry}/<name:tag> so it matches compose references.
    // If <dir> contains a Dockerfile it is used; otherwise a scratch image
    // containing the directory contents is built.
    // Registry and engine come from the ENGINE_CTX thread-local.
    let (platform, image_ref, dir_arg) = match args {
        [image_ref, dir_arg] => (None, image_ref.to_string(), dir_arg.to_string()),
        [flag, platform, image_ref, dir_arg] if flag == "--platform" => (
            Some(platform.to_string()),
            image_ref.to_string(),
            dir_arg.to_string(),
        ),
        _ => {
            return Err(err(
                "build-image requires [--platform <platform>] <name:tag> <dir>".to_string(),
            ));
        }
    };
    let start = std::time::Instant::now();
    let label = args.join(" ");
    ENGINE_CTX.with_borrow_mut(|ctx| {
        let ctx = ctx
            .as_mut()
            .ok_or_else(|| err("ENGINE_CTX not set".to_string()))?;
        let dir = env.work_dir.join(dir_arg);
        let dockerfile = dir.join("Dockerfile");
        let dockerfile = dockerfile.exists().then_some(dockerfile.as_path());
        ctx.engine
            .build_image(&dir, &image_ref, dockerfile, platform.as_deref())
            .map_err(|e| err(format!("build-image: {e}")))?;
        eprintln!(
            "[{:.1}s] build-image {label}",
            start.elapsed().as_secs_f64()
        );
        ctx.built_images.push(image_ref);
        Ok(())
    })
}

fn requested_runtime_matches(runtime_name: &str) -> Result<bool, String> {
    match std::env::var("SNOUTY_TEST_RUNTIME") {
        Ok(requested) => match requested.as_str() {
            "docker" | "podman" => Ok(requested == runtime_name),
            _ => Err(format!(
                "invalid SNOUTY_TEST_RUNTIME `{requested}`: expected `docker` or `podman`"
            )),
        },
        Err(std::env::VarError::NotPresent) => Ok(true),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err("SNOUTY_TEST_RUNTIME must be valid UTF-8".to_string())
        }
    }
}

fn find_runtime(runtime_name: &str) -> Option<Box<dyn snouty::container::ContainerRuntime>> {
    available_runtimes()
        .into_iter()
        .find(|runtime| runtime.name() == runtime_name)
}

fn cleanup_engine_images(runtime_name: &str, built_images: &[String], registry_addr: Option<&str>) {
    for image in built_images {
        let _ = std::process::Command::new(runtime_name)
            .args(["rmi", "-f", image])
            .output();
        if let Some(registry_addr) = registry_addr {
            let prefixed = format!("{registry_addr}/{image}");
            let _ = std::process::Command::new(runtime_name)
                .args(["rmi", "-f", &prefixed])
                .output();
        }
    }
}

fn run_engine_spec_case(runtime_name: &'static str, case: EngineSpecCase) {
    if !requested_runtime_matches(runtime_name)
        .unwrap_or_else(|e| panic!("invalid test runtime selection: {e}"))
    {
        return;
    }

    let engine = match find_runtime(runtime_name) {
        Some(engine) => engine,
        None => {
            skip_or_fail(&format!("{runtime_name}: no container runtime available"));
            return;
        }
    };

    eprintln!("=== engine specs with: {runtime_name} ({}) ===", case.file);

    let registry = if case.needs_registry {
        match OCIRegistry::start(engine.as_ref()) {
            Some(registry) => Some(registry),
            None => return,
        }
    } else {
        None
    };
    let registry_addr = registry.as_ref().map(OCIRegistry::host_port);

    ENGINE_CTX.set(Some(EngineContext {
        engine: engine.clone_box(),
        built_images: Vec::new(),
    }));

    let name = runtime_name.to_string();
    let registry_addr_for_setup = registry_addr.clone();

    let result = testscript::run("specs_engine")
        .files([case.file])
        .setup(move |env| {
            env.set_env_var("RUST_LOG", "debug");
            env.set_env_var("SNOUTY_CONTAINER_ENGINE", &name);
            if let Some(addr) = registry_addr_for_setup.as_deref() {
                env.set_env_var("ANTITHESIS_REPOSITORY", addr);
            }
            Ok(())
        })
        .command("snouty", cmd_snouty)
        .command("mock-server", cmd_mock_server)
        .command("env_from_json", cmd_env_from_json)
        .command("build-image", cmd_build_image)
        .execute();

    let built_images = ENGINE_CTX
        .with_borrow_mut(|ctx| ctx.take().map(|ctx| ctx.built_images).unwrap_or_default());
    cleanup_engine_images(engine.name(), &built_images, registry_addr.as_deref());

    if let Err(e) = result {
        panic!("\n{runtime_name} {}: {e}", case.file);
    }
}

// --- Test functions ---

#[test]
fn spec_tests() {
    let staging = is_staging();
    let specs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("specs");
    let mut files: Vec<String> = std::fs::read_dir(&specs_dir)
        .expect("read specs/")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "txt"))
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    files.sort();

    for file in files {
        let result = testscript::run("specs")
            .files([file.clone()])
            .condition("staging", staging)
            .setup(|env| {
                env.set_env_var("RUST_LOG", "debug");
                if let Some(path) = filtered_path_without_binary("snouty-update") {
                    env.set_env_var("PATH", &path);
                }
                Ok(())
            })
            .command("snouty", cmd_snouty)
            .command("mock-server", cmd_mock_server)
            .command("mock-runs-server", cmd_mock_runs_server)
            .command("env_from_json", cmd_env_from_json)
            .command("set-env", |env, args| {
                // Usage: set-env KEY value...
                // Interpolates ${VAR} references in value using env.env_vars.
                if args.len() < 2 {
                    return Err(err("set-env requires KEY and value".to_string()));
                }
                let key = &args[0];
                let raw_value = args[1..].join(" ");
                let value = env.substitute_env_vars(&raw_value);
                env.set_env_var(key, &value);
                Ok(())
            })
            .command("snouty-bg", |env, args| {
                let child = snouty_cmd(env, args)
                    .spawn()
                    .map_err(|e| err(format!("spawn snouty-bg: {e}")))?;
                env.background_processes.insert("snouty".to_string(), child);
                Ok(())
            })
            .command("setup-docs-db", |env, args| {
                // Usage: setup-docs-db
                // Copies the fixture docs.db into the workdir and sets ANTITHESIS_DOCS_DB_PATH.
                let fixture =
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/docs.db");
                let dest = env.work_dir.join("docs.db");
                std::fs::copy(&fixture, &dest)
                    .map_err(|e| err(format!("failed to copy fixture docs.db: {e}")))?;
                let var_name = if args.is_empty() {
                    "ANTITHESIS_DOCS_DB_PATH"
                } else {
                    &args[0]
                };
                env.set_env_var(var_name, dest.to_str().unwrap());
                Ok(())
            })
            .execute();

        match result {
            Ok(()) => {}
            Err(e) if e.to_string().contains("SKIP:") => {
                eprintln!("skipping {file}");
            }
            Err(e) => panic!("\n{e}"),
        }
    }
}

macro_rules! engine_spec_case_test {
    ($name:ident, $runtime:literal, $file:literal, $needs_registry:expr) => {
        #[test]
        fn $name() {
            run_engine_spec_case(
                $runtime,
                EngineSpecCase {
                    file: $file,
                    needs_registry: $needs_registry,
                },
            );
        }
    };
}

engine_spec_case_test!(
    podman_engine_launch_config_push_specs,
    "podman",
    "launch_config_push.txt",
    true
);
engine_spec_case_test!(
    podman_engine_validate_setup_specs,
    "podman",
    "validate_setup.txt",
    false
);
engine_spec_case_test!(
    podman_engine_validate_failures_specs,
    "podman",
    "validate_failures.txt",
    false
);
engine_spec_case_test!(
    podman_engine_validate_network_arch_specs,
    "podman",
    "validate_network_arch.txt",
    false
);
engine_spec_case_test!(
    podman_engine_validate_timeout_specs,
    "podman",
    "validate_timeout.txt",
    false
);
engine_spec_case_test!(
    docker_engine_launch_config_push_specs,
    "docker",
    "launch_config_push.txt",
    true
);
engine_spec_case_test!(
    docker_engine_validate_setup_specs,
    "docker",
    "validate_setup.txt",
    false
);
engine_spec_case_test!(
    docker_engine_validate_failures_specs,
    "docker",
    "validate_failures.txt",
    false
);
engine_spec_case_test!(
    docker_engine_validate_network_arch_specs,
    "docker",
    "validate_network_arch.txt",
    false
);
engine_spec_case_test!(
    docker_engine_validate_timeout_specs,
    "docker",
    "validate_timeout.txt",
    false
);
