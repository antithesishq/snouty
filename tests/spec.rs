use snouty::testutils::{OCIRegistry, filtered_path_without_binary, skip_or_fail};
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
    let label = args.join(" ");
    let mut cmd = snouty_cmd(env, args);
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

fn cmd_build_image(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: build-image <name:tag> <dir>
    // Builds a container image from <dir> (relative to work_dir), tagged as
    // {registry}/<name:tag> so it matches compose references.
    // If <dir> contains a Dockerfile it is used; otherwise a scratch image
    // containing the directory contents is built.
    // Registry and engine come from the ENGINE_CTX thread-local.
    if args.len() < 2 {
        return Err(err("build-image requires <name:tag> <dir>".to_string()));
    }
    let start = std::time::Instant::now();
    let label = args.join(" ");
    ENGINE_CTX.with_borrow_mut(|ctx| {
        let ctx = ctx
            .as_mut()
            .ok_or_else(|| err("ENGINE_CTX not set".to_string()))?;
        let image_ref = args[0].to_string();
        let dir = env.work_dir.join(&args[1]);
        let dockerfile = dir.join("Dockerfile");
        let dockerfile = dockerfile.exists().then_some(dockerfile.as_path());
        ctx.engine
            .build_image(&dir, &image_ref, dockerfile)
            .map_err(|e| err(format!("build-image: {e}")))?;
        eprintln!(
            "[{:.1}s] build-image {label}",
            start.elapsed().as_secs_f64()
        );
        ctx.built_images.push(image_ref);
        Ok(())
    })
}

// --- Test functions ---

#[test]
fn spec_tests() {
    let result = testscript::run("specs")
        .setup(|env| {
            env.set_env_var("RUST_LOG", "debug");
            if let Some(path) = filtered_path_without_binary("snouty-update") {
                env.set_env_var("PATH", &path);
            }
            Ok(())
        })
        .command("snouty", cmd_snouty)
        .command("mock-server", cmd_mock_server)
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

    if let Err(e) = result {
        panic!("\n{e}");
    }
}

#[test]
fn engine_spec_tests() {
    let engines = snouty::container::available_engines();
    if engines.is_empty() {
        skip_or_fail("no container runtime available");
        return;
    }

    for engine in &engines {
        let engine_name = engine.name().to_string();
        eprintln!("=== engine specs with: {engine_name} ===");
        let registry = match OCIRegistry::start(engine.as_ref()) {
            Some(r) => r,
            None => continue,
        };
        let registry_addr = registry.host_port();

        ENGINE_CTX.set(Some(EngineContext {
            engine: engine.clone_box(),
            built_images: Vec::new(),
        }));

        let name = engine_name.clone();
        let addr = registry_addr.clone();

        let result = testscript::run("specs_engine")
            .setup(move |env| {
                env.set_env_var("RUST_LOG", "debug");
                env.set_env_var("SNOUTY_CONTAINER_ENGINE", &name);
                env.set_env_var("ANTITHESIS_REPOSITORY", &addr);
                Ok(())
            })
            .command("snouty", cmd_snouty)
            .command("mock-server", cmd_mock_server)
            .command("build-image", cmd_build_image)
            .execute();

        // Best-effort cleanup of built images.
        ENGINE_CTX.with_borrow(|ctx| {
            if let Some(ctx) = ctx.as_ref() {
                for image in &ctx.built_images {
                    let _ = std::process::Command::new(engine.name())
                        .args(["rmi", "-f", image])
                        .output();
                    // Remove registry-prefixed copy created by push_compose_images.
                    let prefixed = format!("{registry_addr}/{image}");
                    let _ = std::process::Command::new(engine.name())
                        .args(["rmi", "-f", &prefixed])
                        .output();
                }
            }
        });

        if let Err(e) = result {
            panic!("\n{engine_name}: {e}");
        }
    }
}
