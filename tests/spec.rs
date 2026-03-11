use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Stdio;
use std::thread;
use testscript_rs::testscript;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn err(msg: String) -> testscript_rs::Error {
    testscript_rs::Error::Generic(msg)
}

// --- Engine context (thread-local so fn-pointer commands can access it) ---

struct EngineContext {
    registry: String,
    engine: Box<dyn snouty::container::ContainerRuntime>,
}

thread_local! {
    static ENGINE_CTX: RefCell<Option<EngineContext>> = const { RefCell::new(None) };
}

// --- Shared command handlers (function pointers for testscript CommandFn) ---

fn cmd_snouty(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // env_clear() so tests don't depend on the parent environment.
    // testscript-rs's built-in execute_command uses Command::envs
    // (additive), which leaks the parent env.
    let bin = env!("CARGO_BIN_EXE_snouty");
    let mut cmd = std::process::Command::new(bin);
    cmd.args(args)
        .current_dir(&env.current_dir)
        .env_clear()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Forward system env vars needed by container tools and coverage.
    for var in ["PATH", "HOME", "LLVM_PROFILE_FILE"] {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
    for (k, v) in &env.env_vars {
        cmd.env(k, v);
    }
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
    ENGINE_CTX.with_borrow(|ctx| {
        let ctx = ctx
            .as_ref()
            .ok_or_else(|| err("ENGINE_CTX not set".to_string()))?;
        let image_ref = format!("{}/{}", ctx.registry, args[0]);
        let dir = env.work_dir.join(&args[1]);
        ctx.engine
            .build_image(&dir, &image_ref)
            .map_err(|e| err(format!("build-image: {e}")))?;
        Ok(())
    })
}

// --- Test functions ---

#[test]
fn spec_tests() {
    let result = testscript::run("specs")
        .setup(|env| {
            env.set_env_var("RUST_LOG", "debug");
            Ok(())
        })
        .command("snouty", cmd_snouty)
        .command("mock-server", cmd_mock_server)
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
        eprintln!("skipping engine specs: no container runtime available");
        return;
    }

    // Start ONE mock registry for all engine runs.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = rt.block_on(mock_oci_registry());
    let registry_addr = server.uri().replace("http://", "");

    for engine in &engines {
        let engine_name = engine.name().to_string();
        eprintln!("=== engine specs with: {engine_name} ===");

        ENGINE_CTX.set(Some(EngineContext {
            registry: registry_addr.clone(),
            engine: engine.clone_box(),
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

        if let Err(e) = result {
            panic!("\n{engine_name}: {e}");
        }
    }
}

/// Start a minimal mock OCI registry that accepts image pushes.
///
/// Handles the OCI Distribution Spec endpoints needed for `podman push`:
/// - `GET  /v2/`                        → 200 (API version check)
/// - `HEAD /v2/<name>/blobs/<digest>`   → 200 (pretend blobs exist)
/// - `HEAD /v2/<name>/manifests/<ref>`  → 404 (manifest not found)
/// - `POST /v2/<name>/blobs/uploads/`   → 202 (initiate upload)
/// - `PATCH  /v2/_uploads/<uuid>`       → 202 (chunked upload)
/// - `PUT    /v2/_uploads/<uuid>?digest=…` → 201 (complete upload)
/// - `PUT  /v2/<name>/manifests/<ref>`  → 201 (push manifest)
///
/// Use `server.uri().replace("http://", "")` as the registry
/// host:port in image references.
async fn mock_oci_registry() -> MockServer {
    let server = MockServer::start().await;
    let upload_url = format!("{}/v2/_uploads/test-uuid", server.uri());

    // V2 API version check
    Mock::given(method("GET"))
        .and(path_regex("^/v2/?$"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Docker-Distribution-API-Version", "registry/2.0"),
        )
        .mount(&server)
        .await;

    // Blob existence check → pretend all blobs exist.
    // This satisfies both podman and docker: they skip uploading
    // and go straight to the manifest push.
    Mock::given(method("HEAD"))
        .and(path_regex("^/v2/.+/blobs/"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Length", "0")
                .insert_header("Docker-Content-Digest", "sha256:dummy"),
        )
        .mount(&server)
        .await;

    // Manifest existence check → not found
    Mock::given(method("HEAD"))
        .and(path_regex("^/v2/.+/manifests/"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    // Initiate blob upload
    Mock::given(method("POST"))
        .and(path_regex("^/v2/.+/blobs/uploads"))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", &upload_url)
                .insert_header("Docker-Upload-UUID", "test-uuid")
                .insert_header("Range", "0-0"),
        )
        .mount(&server)
        .await;

    // Chunked blob upload
    Mock::given(method("PATCH"))
        .and(path_regex("^/v2/_uploads/"))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("Location", &upload_url)
                .insert_header("Range", "0-999999"),
        )
        .mount(&server)
        .await;

    // Complete blob upload
    Mock::given(method("PUT"))
        .and(path_regex("^/v2/_uploads/"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&server)
        .await;

    // Push manifest
    Mock::given(method("PUT"))
        .and(path_regex("^/v2/.+/manifests/"))
        .respond_with(ResponseTemplate::new(201).insert_header(
            "Docker-Content-Digest",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ))
        .mount(&server)
        .await;

    server
}
