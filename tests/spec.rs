use snouty::testutils::{
    MockApiServer, OCIRegistry, available_runtimes, filtered_path_without_binary, skip_or_fail,
};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Stdio;
use std::thread;
use testscript_rs::testscript;

fn err(msg: String) -> testscript_rs::Error {
    testscript_rs::Error::Generic(msg)
}

/// Resolve a spec-supplied path to a concrete filesystem path.
///
/// `${VAR}` references are expanded from the test environment first, then a
/// leading `~` (bare or `~/...`) expands to the test's isolated `$HOME` — the
/// same `HOME` the snouty subprocess sees, so a spec can point at the global
/// `settings.toml` that `snouty login` writes under it. A remaining relative
/// path is resolved against the spec's working directory, matching where inline
/// `-- file --` fixtures land.
fn resolve_spec_path(
    env: &testscript_rs::TestEnvironment,
    raw: &str,
) -> testscript_rs::Result<std::path::PathBuf> {
    let expanded = env.substitute_env_vars(raw);
    if let Some(rest) = expanded.strip_prefix('~') {
        let home = env
            .env_vars
            .get("HOME")
            .ok_or_else(|| err("`~` used in a path but HOME is not set".to_string()))?;
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        return Ok(std::path::Path::new(home).join(rest));
    }

    let path = std::path::PathBuf::from(expanded);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(env.current_dir.join(path))
    }
}

/// `file <path> <pattern>`: assert the contents of the file at `<path>` match the
/// regex `<pattern>`, mirroring the built-in `stdout`/`stderr` matchers (combine
/// with a leading `!` to assert the pattern is absent). `<path>` may start with
/// `~` to reference the test's isolated `$HOME` (see [`resolve_spec_path`]).
fn cmd_file(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let (path_arg, pattern) = match args {
        [path, rest @ ..] if !rest.is_empty() => (path, rest.join(" ")),
        _ => return Err(err("file requires <path> <pattern>".to_string())),
    };
    let path = resolve_spec_path(env, path_arg)?;
    let re = regex::Regex::new(&pattern)
        .map_err(|e| err(format!("invalid file pattern `{pattern}`: {e}")))?;
    let contents = std::fs::read_to_string(&path)
        .map_err(|e| err(format!("could not read {}: {e}", path.display())))?;
    if !re.is_match(&contents) {
        return Err(err(format!(
            "file {} does not match /{pattern}/\ncontents:\n{contents}",
            path.display()
        )));
    }
    Ok(())
}

fn cmd_set_env(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
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
}

fn cmd_substitute(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: substitute <path>
    // Replaces ${VAR} references inside <path> with the environment's values.
    // A txtar fixture is written verbatim, so this is the only way a compose
    // fixture can name the registry the harness starts on a free port.
    let [path_arg] = args else {
        return Err(err("substitute requires <path>".to_string()));
    };
    let path = resolve_spec_path(env, path_arg)?;
    let contents = std::fs::read_to_string(&path)
        .map_err(|e| err(format!("could not read {}: {e}", path.display())))?;
    let substituted = env.substitute_env_vars(&contents);
    std::fs::write(&path, substituted)
        .map_err(|e| err(format!("could not write {}: {e}", path.display())))?;
    Ok(())
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
///
/// `TMPDIR` matters on macOS: podman recomputes the machine API socket path
/// from it on every invocation, so dropping it makes `podman machine inspect`
/// report a `/tmp` fallback path the socket was never bound at.
///
/// DBus configuration is deliberately omitted from FORWARDED_ENV_VARS since
/// it represents a global state that might leak into or out of tests.
const FORWARDED_ENV_VARS: &[&str] = &["PATH", "HOME", "LLVM_PROFILE_FILE", "TMPDIR"];

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
    // Disable keychain access -- this isn't something we can mock for each test case
    cmd.env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1");
    cmd.env(
        "XDG_CONFIG_HOME",
        isolated_xdg_config_home(&env.current_dir),
    );
    // Isolate the API response cache per spec. Without this, snouty falls back
    // to the real system temp dir, and a cached response from an earlier spec
    // run (mock servers reuse ephemeral localhost ports) could leak in. The
    // snouty-specific variable leaves XDG_RUNTIME_DIR alone: rootless podman
    // resolves its API socket under XDG_RUNTIME_DIR, so overriding that would
    // break the podman engine specs.
    let cache_dir = env.current_dir.join("api-cache-isolation");
    std::fs::create_dir_all(&cache_dir).expect("create API cache isolation dir");
    cmd.env("SNOUTY_API_CACHE_DIR", &cache_dir);
    for (k, v) in &env.env_vars {
        cmd.env(k, v);
    }
    cmd
}

/// The isolated `XDG_CONFIG_HOME` for a spec's snouty subprocess: a per-spec dir
/// with no `snouty/` subdir, so a developer's or CI's real
/// `~/.config/snouty/settings.toml` can't leak in and change resolved
/// tenant/repository/etc. (The project file is already isolated — each spec runs
/// in its own work dir, so `./.snouty.toml` doesn't exist unless the spec makes
/// it.)
///
/// `XDG_CONFIG_HOME` is shared, though: on macOS, podman keeps its *machine
/// connection* under `$XDG_CONFIG_HOME/containers`, so an empty dir makes the
/// subprocess's `podman info` lose the VM and fail with a bogus local-socket
/// path — breaking the podman engine specs. So we re-expose the real podman
/// config (written under `$HOME/.config/containers` at machine-init, before this
/// override takes effect) via a symlink: podman still resolves its connection
/// while snouty stays isolated. Harmless on Linux, where podman reaches its
/// socket via `XDG_RUNTIME_DIR` and never consults this directory.
fn isolated_xdg_config_home(current_dir: &std::path::Path) -> std::path::PathBuf {
    let dir = current_dir.join("xdg-config-isolation");
    std::fs::create_dir_all(&dir).expect("create XDG_CONFIG_HOME isolation dir");

    if let Some(home) = std::env::var_os("HOME") {
        let real_containers = std::path::PathBuf::from(home)
            .join(".config")
            .join("containers");
        let link = dir.join("containers");
        // `symlink_metadata` (unlike `exists`) detects an existing link even if
        // its target is gone, so repeated calls within one spec don't re-link.
        if real_containers.is_dir() && link.symlink_metadata().is_err() {
            std::os::unix::fs::symlink(&real_containers, &link)
                .expect("symlink podman containers config into isolated XDG_CONFIG_HOME");
        }
    }
    dir
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
        // snouty is free to exit before it reads all of stdin — `runs exec` with
        // a SCRIPT argument never reads it at all — and that breaks the pipe.
        // A broken pipe here is not a failure, so let the spec's own assertions
        // on exit status and output judge the run. Every other error still
        // fails, because it means the write itself went wrong.
        //
        // Linux nearly always hides this: a small write lands in the pipe buffer
        // and returns before snouty exits. macOS loses that race often enough to
        // fail a spec about one run in three.
        match child.stdin.take().unwrap().write_all(&data) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(e) => return Err(err(format!("write stdin: {e}"))),
        }
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
            "env_from_json requires <line_index> <json_key>".to_string()
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

fn cmd_mock_proxy(
    env: &mut testscript_rs::TestEnvironment,
    _args: &[String],
) -> testscript_rs::Result<()> {
    // Usage: mock-proxy
    //
    // Starts the mock Antithesis API behind an in-process HTTP forward proxy and
    // points snouty's proxy env var (ANTITHESIS_HTTPS_PROXY) at it. It sets the
    // API key and tenant but deliberately does NOT set ANTITHESIS_BASE_URL: the
    // spec sets that to an unresolvable host, so a request can only reach the
    // mock by traversing the proxy. That makes a successful `snouty runs` proof
    // that the proxy setting is honored end to end.
    if is_staging() {
        return Err(err(
            "mock-proxy is not supported against staging; guard the block with [!staging]"
                .to_string(),
        ));
    }

    let server = MockApiServer::start();
    let mock_addr = server
        .url()
        .strip_prefix("http://")
        .ok_or_else(|| err("mock server url missing http scheme".to_string()))?
        .to_string();
    let token = server.token().to_string();
    // Keep the mock server (and its listener thread) alive for the process.
    std::mem::forget(server);

    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| err(format!("bind proxy: {e}")))?;
    let proxy_addr = listener
        .local_addr()
        .map_err(|e| err(format!("proxy addr: {e}")))?;

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mock_addr = mock_addr.clone();
            thread::spawn(move || {
                // A forwarding failure just drops the connection; snouty then
                // reports a request error and the spec's assertion fails loudly.
                let _ = proxy_forward(stream, &mock_addr);
            });
        }
    });

    env.set_env_var("ANTITHESIS_HTTPS_PROXY", &format!("http://{proxy_addr}"));
    env.set_env_var("ANTITHESIS_API_KEY", &token);
    env.set_env_var("ANTITHESIS_TENANT", "testtenant");
    Ok(())
}

/// A minimal HTTP forward proxy for one request/response exchange.
///
/// reqwest, configured with an HTTP proxy for an `http://` target, sends the
/// proxy a request line in absolute form (`GET http://host/path HTTP/1.1`). We
/// rewrite it to origin form (`GET /path HTTP/1.1`), relay it to `mock_addr`
/// (ignoring the — unresolvable — target host), and copy the response back
/// verbatim. The mock closes the connection after responding (`Connection:
/// close`), so `read_to_end` returns the full response and reqwest opens a
/// fresh connection per request (one exchange per proxy connection).
fn proxy_forward(mut client: TcpStream, mock_addr: &str) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = client.read(&mut chunk)?;
        if n == 0 {
            return Ok(()); // client closed before sending a full request head
        }
        buf.extend_from_slice(&chunk[..n]);
        let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };

        let headers_end = pos + 4;
        let head = String::from_utf8_lossy(&buf[..headers_end]).into_owned();
        let content_length = proxy_content_length(&head);
        let mut body = buf[headers_end..].to_vec();
        while body.len() < content_length {
            let n = client.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }

        let mut upstream = TcpStream::connect(mock_addr)?;
        upstream.write_all(proxy_rewrite_request_line(&head).as_bytes())?;
        upstream.write_all(&body)?;
        let mut response = Vec::new();
        upstream.read_to_end(&mut response)?;
        client.write_all(&response)?;
        return Ok(());
    }
}

/// Parse `Content-Length` from an HTTP header block (0 if absent).
fn proxy_content_length(head: &str) -> usize {
    head.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0)
}

/// Rewrite the request line of `head` from absolute form to origin form,
/// leaving every subsequent header (and the terminating blank line) untouched.
fn proxy_rewrite_request_line(head: &str) -> String {
    let request_line = head.split("\r\n").next().unwrap_or("");
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or("GET");
    let uri = parts.next().unwrap_or("/");
    let version = parts.next().unwrap_or("HTTP/1.1");

    // `http://host[:port]/path?query` -> `/path?query`.
    let origin = match uri.split_once("://") {
        Some((_, authority_and_path)) => match authority_and_path.find('/') {
            Some(idx) => &authority_and_path[idx..],
            None => "/",
        },
        None => uri,
    };

    // `head[request_line.len()..]` keeps the leading "\r\n", the remaining
    // headers, and the trailing "\r\n\r\n" exactly as received.
    format!("{method} {origin} {version}{}", &head[request_line.len()..])
}

fn is_staging() -> bool {
    std::env::var("SNOUTY_STAGING")
        .ok()
        .is_some_and(|v| !v.is_empty() && v != "0")
}

fn propagate_antithesis_env(env: &mut testscript_rs::TestEnvironment) -> testscript_rs::Result<()> {
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
        "ANTITHESIS_EXTRA_HEADERS",
        "ANTITHESIS_HTTPS_PROXY",
    ] {
        env.env_vars.remove(var);
    }
    env.set_env_var("ANTITHESIS_TENANT", &tenant);
    for var in [
        "ANTITHESIS_BASE_URL",
        "ANTITHESIS_API_KEY",
        "ANTITHESIS_USERNAME",
        "ANTITHESIS_PASSWORD",
        "ANTITHESIS_EXTRA_HEADERS",
        "ANTITHESIS_HTTPS_PROXY",
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
    // The engine spec cases run as separate `#[test]` processes in parallel and
    // share one local `containers/storage`. A forced image removal here mutates
    // that store's layer database while a sibling test's `podman build` is
    // enumerating images for its build-cache check ("getting top layer info"),
    // intermittently yielding `layer not known` (issue #136). On CI the runner
    // is ephemeral and thrown away after the job, so there is nothing to clean
    // up — skip the `rmi` entirely there to remove the writer that races those
    // concurrent builds. Locally we still clean up so a dev's image store does
    // not accumulate junk across runs.
    if std::env::var_os("CI").is_some() {
        eprintln!(
            "CI is set: skipping {} image cleanup (ephemeral runner; avoids racing concurrent builds, issue #136)",
            runtime_name
        );
        return;
    }
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
    let is_docker = runtime_name == "docker";

    let result = testscript::run("specs_engine")
        .files([case.file])
        .condition("docker", is_docker)
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
        .command("set-env", cmd_set_env)
        .command("substitute", cmd_substitute)
        .execute();

    let built_images = ENGINE_CTX
        .with_borrow_mut(|ctx| ctx.take().map(|ctx| ctx.built_images).unwrap_or_default());
    cleanup_engine_images(engine.name(), &built_images, registry_addr.as_deref());

    if let Err(e) = result {
        panic!("\n{runtime_name} {}: {e}", case.file);
    }
}

/// The `snouty mcp` process that `mcp-server` started. The spec runs on one
/// thread, so a thread-local can hold it for the fn-pointer commands.
struct McpServer {
    child: std::process::Child,
    stdout_path: std::path::PathBuf,
    stderr_path: std::path::PathBuf,
}

impl McpServer {
    /// The process output so far, with `status` as its exit status.
    fn output(&self, status: std::process::ExitStatus) -> std::process::Output {
        std::process::Output {
            status,
            stdout: std::fs::read(&self.stdout_path).unwrap_or_default(),
            stderr: std::fs::read(&self.stderr_path).unwrap_or_default(),
        }
    }
}

thread_local! {
    static MCP_SERVER: RefCell<Option<McpServer>> = const { RefCell::new(None) };
}

/// The connection of the tool call that `mcp-call -bg` sent, so that
/// `mcp-disconnect` can close it. The call runs on its own thread, so a
/// thread-local cannot hold it.
static MCP_BG_CALL: std::sync::Mutex<Option<TcpStream>> = std::sync::Mutex::new(None);

const MCP_PROTOCOL_VERSION: rmcp::model::ProtocolVersion =
    rmcp::model::ProtocolVersion::LATEST_WITH_INITIALIZE;

/// How long `mcp-server` waits for `Listening on`, and `wait-file` for its
/// file.
const MCP_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long `mcp-stop` waits for the server to exit after the signal.
const MCP_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const MCP_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Kills the server that a spec did not stop, so it does not outlive its spec.
fn kill_mcp_server() {
    if let Some(mut server) = MCP_SERVER.take() {
        let _ = server.child.kill();
        let _ = server.child.wait();
    }
    MCP_BG_CALL.lock().unwrap().take();
}

/// Calls `check` every `MCP_POLL_INTERVAL` until it returns `Some`, for up
/// to `timeout`. `None` means that the time ran out.
fn poll<T>(
    timeout: std::time::Duration,
    mut check: impl FnMut() -> testscript_rs::Result<Option<T>>,
) -> testscript_rs::Result<Option<T>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(value) = check()? {
            return Ok(Some(value));
        }
        if std::time::Instant::now() > deadline {
            return Ok(None);
        }
        thread::sleep(MCP_POLL_INTERVAL);
    }
}

/// Waits up to `timeout` for `child` to exit. When the time runs out, kills
/// it and returns `Err` with the status after the kill.
fn wait_exit(
    child: &mut std::process::Child,
    timeout: std::time::Duration,
) -> testscript_rs::Result<Result<std::process::ExitStatus, std::process::ExitStatus>> {
    let wait_err = |e: std::io::Error| err(format!("wait snouty: {e}"));
    if let Some(status) = poll(timeout, || child.try_wait().map_err(wait_err))? {
        return Ok(Ok(status));
    }
    let _ = child.kill();
    Ok(Err(child.wait().map_err(wait_err)?))
}

/// `mcp-server <snouty args…>`: starts snouty in the background, with stdout
/// and stderr in the files `mcp.stdout` and `mcp.stderr`. When it prints
/// `Listening on ADDR`, sets `MCP_ADDR` and `MCP_PORT`.
/// If the process exits first, its output becomes the last output and the
/// command fails.
fn cmd_mcp_server(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    if MCP_SERVER.with_borrow(Option::is_some) {
        return Err(err(
            "mcp-server: a server is already running; stop it with mcp-stop".to_string(),
        ));
    }
    let expanded: Vec<String> = args.iter().map(|a| env.substitute_env_vars(a)).collect();
    let stdout_path = env.current_dir.join("mcp.stdout");
    let stderr_path = env.current_dir.join("mcp.stderr");
    let create = |path: &std::path::Path| {
        std::fs::File::create(path).map_err(|e| err(format!("create {}: {e}", path.display())))
    };
    let mut cmd = snouty_cmd(env, &expanded);
    // The spec setup turns on debug logging, which would bury the request
    // log that the specs read.
    cmd.env_remove("RUST_LOG")
        .stdout(create(&stdout_path)?)
        .stderr(create(&stderr_path)?);
    let child = cmd.spawn().map_err(|e| err(format!("spawn snouty: {e}")))?;
    let mut server = McpServer {
        child,
        stdout_path,
        stderr_path,
    };

    let listening = regex::Regex::new(r"(?m)^Listening on (\S+)$").unwrap();
    // `Ok` is the listen address, and `Err` the status of an early exit.
    let started = poll(MCP_START_TIMEOUT, || {
        let stdout = std::fs::read_to_string(&server.stdout_path).unwrap_or_default();
        if let Some(found) = listening.captures(&stdout) {
            return Ok(Some(Ok(found[1].to_string())));
        }
        let exited = server.child.try_wait();
        Ok(exited
            .map_err(|e| err(format!("wait snouty: {e}")))?
            .map(Err))
    })?;
    let addr = match started {
        Some(Ok(addr)) => addr,
        Some(Err(status)) => {
            let output = server.output(status);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            env.last_output = Some(output);
            return Err(err(format!(
                "snouty exited before it listened ({status})\nstderr:\n{stderr}"
            )));
        }
        None => {
            let _ = server.child.kill();
            let status = server
                .child
                .wait()
                .map_err(|e| err(format!("wait snouty: {e}")))?;
            env.last_output = Some(server.output(status));
            return Err(err(format!(
                "snouty did not print `Listening on` in {MCP_START_TIMEOUT:?}"
            )));
        }
    };
    let port = addr
        .parse::<std::net::SocketAddr>()
        .map_err(|e| err(format!("listen address {addr}: {e}")))?
        .port()
        .to_string();

    env.set_env_var("MCP_ADDR", &addr);
    env.set_env_var("MCP_PORT", &port);
    env.last_output = Some(server.output(std::process::ExitStatus::default()));
    MCP_SERVER.set(Some(server));
    Ok(())
}

/// How long `mcp-stdio` waits for each response, and for the exit.
const MCP_STDIO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// `mcp-stdio <file> <snouty args…>`: runs snouty and writes each line of
/// the file to its stdin. When it has written one response line for each
/// request with an `id`, closes stdin and waits for the exit. The output
/// becomes the last output. Fails when the exit status is not 0. snouty
/// stops when stdin closes, so the command reads the responses first.
fn cmd_mcp_stdio(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let [file, rest @ ..] = args else {
        return Err(err("mcp-stdio requires <file> <snouty args…>".to_string()));
    };
    let path = resolve_spec_path(env, file)?;
    let input =
        std::fs::read_to_string(&path).map_err(|e| err(format!("read {}: {e}", path.display())))?;
    let requests = input
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message.get("id").is_some())
        .count();
    let expanded: Vec<String> = rest.iter().map(|a| env.substitute_env_vars(a)).collect();
    // stderr goes to a file, so a long request log cannot fill the pipe and
    // block snouty.
    let stderr_path = env.current_dir.join("mcp-stdio.stderr");
    let stderr = std::fs::File::create(&stderr_path)
        .map_err(|e| err(format!("create {}: {e}", stderr_path.display())))?;
    let mut cmd = snouty_cmd(env, &expanded);
    cmd.env_remove("RUST_LOG")
        .stdin(Stdio::piped())
        .stderr(stderr);
    let mut child = cmd.spawn().map_err(|e| err(format!("spawn snouty: {e}")))?;

    let mut stdin = child.stdin.take().expect("stdin is piped");
    // The server reads one message per line, and a fixture file can end
    // with no newline.
    for line in input.lines() {
        writeln!(stdin, "{line}").map_err(|e| err(format!("write snouty stdin: {e}")))?;
    }
    let stdout = child.stdout.take().expect("stdout is piped");
    let (lines_tx, lines_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut stdout = String::new();
    for _ in 0..requests {
        match lines_rx.recv_timeout(MCP_STDIO_TIMEOUT) {
            Ok(line) => {
                stdout.push_str(&line);
                stdout.push('\n');
            }
            Err(_) => break,
        }
    }
    drop(stdin);

    let Ok(status) = wait_exit(&mut child, MCP_STDIO_TIMEOUT)? else {
        return Err(err(format!(
            "snouty did not exit in {MCP_STDIO_TIMEOUT:?} after stdin closed"
        )));
    };
    // The lines that came after the last response, such as a message the
    // server wrote by mistake.
    stdout.extend(lines_rx.try_iter().map(|line| line + "\n"));
    env.last_output = Some(std::process::Output {
        status,
        stdout: stdout.into_bytes(),
        stderr: std::fs::read(&stderr_path).unwrap_or_default(),
    });
    if !status.success() {
        return Err(err(format!("snouty exited with {status}")));
    }
    Ok(())
}

/// `mcp-stop [-TERM|-INT|-HUP]`: sends the signal (default TERM) and waits
/// for the server to exit. The server output becomes the last output. Fails
/// when the exit status is not 0, or when the server does not exit in time.
fn cmd_mcp_stop(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let signal = match args {
        [] => "TERM",
        [flag] if matches!(flag.as_str(), "-TERM" | "-INT" | "-HUP") => &flag[1..],
        _ => {
            return Err(err(
                "mcp-stop accepts one of -TERM, -INT or -HUP".to_string()
            ));
        }
    };
    let mut server = MCP_SERVER
        .take()
        .ok_or_else(|| err("mcp-stop: no server is running".to_string()))?;
    let sent = std::process::Command::new("kill")
        .args(["-s", signal, &server.child.id().to_string()])
        .status()
        .map_err(|e| err(format!("run kill: {e}")))?;
    if !sent.success() {
        let _ = server.child.kill();
        let _ = server.child.wait();
        return Err(err(format!("kill -s {signal} failed: {sent}")));
    }

    let status = match wait_exit(&mut server.child, MCP_STOP_TIMEOUT)? {
        Ok(status) => status,
        Err(killed) => {
            env.last_output = Some(server.output(killed));
            return Err(err(format!(
                "snouty did not exit in {MCP_STOP_TIMEOUT:?} after SIG{signal}"
            )));
        }
    };
    let output = server.output(status);
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    env.last_output = Some(output);
    if !status.success() {
        return Err(err(format!(
            "snouty exited with {status} after SIG{signal}\nstderr:\n{stderr}"
        )));
    }
    Ok(())
}

/// One HTTP response, as the harness client reads it.
struct HttpResponse {
    status: u16,
    body: String,
}

impl HttpResponse {
    /// The JSON-RPC message in the body. The server runs in JSON response
    /// mode, so the body is one JSON value, not an SSE stream.
    fn message(&self) -> Result<serde_json::Value, String> {
        serde_json::from_str(&self.body)
            .map_err(|e| format!("parse JSON-RPC body: {e}\n{}", self.body))
    }
}

/// What an MCP request sends, apart from its JSON-RPC body.
#[derive(Clone, Copy)]
struct McpRequest<'a> {
    addr: &'a str,
    /// The Host header. `None` sends `addr`.
    host: Option<&'a str>,
    path: &'a str,
    origin: Option<&'a str>,
    /// Put a handle to the connection in [`MCP_BG_CALL`] before it sends.
    /// `mcp_call` applies it to the tool call only.
    share: bool,
}

impl<'a> McpRequest<'a> {
    fn to(addr: &'a str) -> Self {
        McpRequest {
            addr,
            host: None,
            path: snouty::mcp::MCP_PATH,
            origin: None,
            share: false,
        }
    }

    /// POSTs `body` over a new connection with `Connection: close`, and reads
    /// the response to its end. The harness has its own client, because it
    /// must send any Host header the spec names.
    fn post(&self, body: &serde_json::Value) -> Result<HttpResponse, String> {
        let body = body.to_string();
        let mut head = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             MCP-Protocol-Version: {MCP_PROTOCOL_VERSION}\r\n\
             Content-Length: {}\r\nConnection: close\r\n",
            self.path,
            self.host.unwrap_or(self.addr),
            body.len(),
        );
        if let Some(origin) = self.origin {
            head.push_str(&format!("Origin: {origin}\r\n"));
        }
        head.push_str("\r\n");

        let mut stream =
            TcpStream::connect(self.addr).map_err(|e| format!("connect {}: {e}", self.addr))?;
        if self.share {
            let handle = stream
                .try_clone()
                .map_err(|e| format!("clone connection: {e}"))?;
            *MCP_BG_CALL.lock().unwrap() = Some(handle);
        }
        stream
            .write_all(head.as_bytes())
            .and_then(|()| stream.write_all(body.as_bytes()))
            .map_err(|e| format!("send request: {e}"))?;
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|e| format!("read response: {e}"))?;
        let raw = String::from_utf8_lossy(&raw);
        let (head, body) = raw
            .split_once("\r\n\r\n")
            .ok_or_else(|| format!("response has no header end:\n{raw}"))?;
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| format!("bad status line:\n{head}"))?;
        let chunked = lines.filter_map(|line| line.split_once(':')).any(|(n, v)| {
            n.trim().eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked")
        });
        if chunked {
            return Err("chunked responses are not supported".to_string());
        }
        Ok(HttpResponse {
            status,
            body: body.to_string(),
        })
    }
}

/// The `result` of a JSON-RPC response. The error is the text a spec reads
/// on stderr: `HTTP <status>` or `error <code>: <message>`.
fn rpc_result(response: &HttpResponse) -> Result<serde_json::Value, String> {
    if !(200..300).contains(&response.status) {
        return Err(format!("HTTP {}", response.status));
    }
    let mut message = response.message()?;
    if let Some(error) = message.get("error") {
        let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        let text = error.get("message").and_then(|m| m.as_str()).unwrap_or("");
        return Err(format!("error {code}: {text}"));
    }
    message
        .get_mut("result")
        .map(serde_json::Value::take)
        .ok_or_else(|| format!("response has no result or error: {message}"))
}

fn spec_output(success: bool, stdout: String, stderr: String) -> std::process::Output {
    use std::os::unix::process::ExitStatusExt;
    std::process::Output {
        status: std::process::ExitStatus::from_raw(if success { 0 } else { 1 << 8 }),
        stdout: stdout.into_bytes(),
        stderr: stderr.into_bytes(),
    }
}

/// Parses `<method|tool> [json]`, the shape both `mcp-call` and `mcp-rpc`
/// end with.
fn name_and_params(
    env: &testscript_rs::TestEnvironment,
    directive: &str,
    args: &[String],
) -> testscript_rs::Result<(String, Option<serde_json::Value>)> {
    let (name, json) = match args {
        [name] => (name, None),
        [name, json] => (name, Some(json)),
        _ => return Err(err(format!("{directive} requires <name> [json]"))),
    };
    let params = json
        .map(|json| {
            let json = env.substitute_env_vars(json);
            serde_json::from_str(&json)
                .map_err(|e| err(format!("{directive}: bad JSON {json}: {e}")))
        })
        .transpose()?;
    Ok((name.clone(), params))
}

fn mcp_addr(env: &testscript_rs::TestEnvironment) -> testscript_rs::Result<String> {
    env.env_vars
        .get("MCP_ADDR")
        .cloned()
        .ok_or_else(|| err("MCP_ADDR is not set; start a server with mcp-server".to_string()))
}

/// Initializes the server and calls one tool. The error is
/// `(stdout, stderr)` for the last output.
fn mcp_call(
    call: McpRequest,
    tool: &str,
    arguments: Option<serde_json::Value>,
) -> Result<String, (String, String)> {
    let fail = |stderr: String| (String::new(), stderr);
    let request = McpRequest {
        share: false,
        ..call
    };
    let initialize = request
        .post(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "snouty-spec", "version": "0"},
            },
        }))
        .map_err(fail)?;
    rpc_result(&initialize).map_err(|e| fail(format!("initialize: {e}")))?;
    let initialized = request
        .post(&serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .map_err(fail)?;
    if !(200..300).contains(&initialized.status) {
        return Err(fail(format!("initialized: HTTP {}", initialized.status)));
    }

    let mut params = serde_json::json!({"name": tool});
    if let Some(arguments) = arguments {
        params["arguments"] = arguments;
    }
    let request = McpRequest {
        share: call.share,
        ..request
    };
    let call = request
        .post(&serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": params}))
        .map_err(fail)?;
    let result = rpc_result(&call).map_err(fail)?;
    let text = match result
        .get("content")
        .and_then(|c| c.as_array())
        .map(Vec::as_slice)
    {
        Some([block]) if block.get("type").and_then(|t| t.as_str()) == Some("text") => block
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or_default()
            .to_string(),
        _ => return Err(fail(format!("expected exactly one text block: {result}"))),
    };
    if result.get("isError").and_then(|e| e.as_bool()) == Some(true) {
        return Err((text, "isError".to_string()));
    }
    Ok(text)
}

/// `mcp-call [-bg] <tool> [arguments-json]`: initializes a session with the
/// server at `MCP_ADDR`, then calls the tool. stdout gets the one text block
/// of the result. A result with `isError` fails, with the text on stdout. A
/// JSON-RPC error fails with `error <code>: <message>` on stderr, and an HTTP
/// error with `HTTP <status>`. With `-bg`, the call runs on a thread and the
/// command returns at once, with no output; `mcp-disconnect` closes its
/// connection.
fn cmd_mcp_call(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let (background, args) = match args {
        [flag, rest @ ..] if flag == "-bg" => (true, rest),
        _ => (false, args),
    };
    let (tool, arguments) = name_and_params(env, "mcp-call", args)?;
    let addr = mcp_addr(env)?;
    if background {
        thread::spawn(move || {
            let call = McpRequest {
                share: true,
                ..McpRequest::to(&addr)
            };
            mcp_call(call, &tool, arguments)
        });
        return Ok(());
    }
    match mcp_call(McpRequest::to(&addr), &tool, arguments) {
        Ok(text) => {
            env.last_output = Some(spec_output(true, text, String::new()));
            Ok(())
        }
        Err((stdout, stderr)) => {
            let message = format!("mcp-call {tool} failed\nstderr:\n{stderr}\nstdout:\n{stdout}");
            env.last_output = Some(spec_output(false, stdout, stderr));
            Err(err(message))
        }
    }
}

/// `mcp-rpc [-host H] [-path P] [-origin O] <method> [params-json]`: sends one
/// JSON-RPC request to `MCP_ADDR`, with no session. stdout gets the `result`
/// as compact JSON. An HTTP error fails with `HTTP <status>` on stderr, and a
/// JSON-RPC error with `error <code>: <message>`.
fn cmd_mcp_rpc(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let addr = mcp_addr(env)?;
    let mut request = McpRequest::to(&addr);
    let mut rest = args;
    let (mut host, mut path, mut origin) = (None, None, None);
    while let [flag, value, tail @ ..] = rest {
        let slot = match flag.as_str() {
            "-host" => &mut host,
            "-path" => &mut path,
            "-origin" => &mut origin,
            _ => break,
        };
        *slot = Some(env.substitute_env_vars(value));
        rest = tail;
    }
    request.host = host.as_deref();
    request.path = path.as_deref().unwrap_or(request.path);
    request.origin = origin.as_deref();
    let (method, params) = name_and_params(env, "mcp-rpc", rest)?;

    let mut body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method});
    if let Some(params) = params {
        body["params"] = params;
    }
    let response = request.post(&body).map_err(err)?;
    match rpc_result(&response) {
        Ok(result) => {
            env.last_output = Some(spec_output(true, format!("{result}\n"), String::new()));
            Ok(())
        }
        Err(stderr) => {
            let message = format!("mcp-rpc {method} failed: {stderr}");
            env.last_output = Some(spec_output(false, String::new(), format!("{stderr}\n")));
            Err(err(message))
        }
    }
}

/// `mcp-disconnect`: closes the connection of the tool call that
/// `mcp-call -bg` sent, as a client that goes away does.
fn cmd_mcp_disconnect(
    _env: &mut testscript_rs::TestEnvironment,
    _args: &[String],
) -> testscript_rs::Result<()> {
    let stream = MCP_BG_CALL
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| err("mcp-disconnect: no mcp-call -bg connection".to_string()))?;
    stream
        .shutdown(std::net::Shutdown::Both)
        .map_err(|e| err(format!("close connection: {e}")))
}

/// `mock-blackhole-server [-docs] <name>`: an API server that accepts
/// connections and never answers. It writes the file `<name>.hit` when a
/// connection arrives, and `<name>.closed` when the client closes one. It
/// points the API env vars at itself, or with `-docs`, the docs URL.
fn cmd_mock_blackhole_server(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let (docs, name) = match args {
        [name] => (false, name),
        [flag, name] if flag == "-docs" => (true, name),
        _ => {
            return Err(err(
                "mock-blackhole-server requires [-docs] <name>".to_string()
            ));
        }
    };
    if is_staging() {
        return Err(err(
            "mock-blackhole-server is not supported against staging; guard the block with [!staging]"
                .to_string(),
        ));
    }
    let listener =
        TcpListener::bind("127.0.0.1:0").map_err(|e| err(format!("failed to bind: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| err(format!("failed to get addr: {e}")))?;
    let hit = env.current_dir.join(format!("{name}.hit"));
    let closed = env.current_dir.join(format!("{name}.closed"));
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let _ = std::fs::write(&hit, "");
            let closed = closed.clone();
            // Read and never answer, until the client closes the connection.
            thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while matches!(stream.read(&mut buf), Ok(n) if n > 0) {}
                let _ = std::fs::write(&closed, "");
            });
        }
    });
    if docs {
        env.set_env_var("ANTITHESIS_DOCS_URL", &format!("http://{addr}"));
    } else {
        env.set_env_var("ANTITHESIS_BASE_URL", &format!("http://{addr}"));
        env.set_env_var("ANTITHESIS_API_KEY", "blackhole-key");
        env.set_env_var("ANTITHESIS_TENANT", "testtenant");
    }
    Ok(())
}

/// `wait-file <path>`: waits until the file exists, for up to
/// [`MCP_START_TIMEOUT`].
fn cmd_wait_file(
    env: &mut testscript_rs::TestEnvironment,
    args: &[String],
) -> testscript_rs::Result<()> {
    let [path_arg] = args else {
        return Err(err("wait-file requires <path>".to_string()));
    };
    let path = resolve_spec_path(env, path_arg)?;
    match poll(MCP_START_TIMEOUT, || Ok(path.exists().then_some(())))? {
        Some(()) => Ok(()),
        None => Err(err(format!(
            "{} did not appear in {MCP_START_TIMEOUT:?}",
            path.display()
        ))),
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
            .command("mock-proxy", cmd_mock_proxy)
            .command("env_from_json", cmd_env_from_json)
            .command("file", cmd_file)
            .command("set-env", cmd_set_env)
            .command("mcp-server", cmd_mcp_server)
            .command("mcp-call", cmd_mcp_call)
            .command("mcp-rpc", cmd_mcp_rpc)
            .command("mcp-stop", cmd_mcp_stop)
            .command("mcp-stdio", cmd_mcp_stdio)
            .command("mcp-disconnect", cmd_mcp_disconnect)
            .command("mock-blackhole-server", cmd_mock_blackhole_server)
            .command("wait-file", cmd_wait_file)
            .command("snouty-bg", |env, args| {
                let child = snouty_cmd(env, args)
                    .spawn()
                    .map_err(|e| err(format!("spawn snouty-bg: {e}")))?;
                env.background_processes.insert("snouty".to_string(), child);
                Ok(())
            })
            .command("isolate-home", |env, args| {
                // Usage: isolate-home <name>
                //
                // `snouty login` persists credentials.toml and settings.toml
                // under the global settings dir, which is `$XDG_CONFIG_HOME/snouty`
                // when that var is set and otherwise `$HOME/.config/snouty`. The
                // shared spec setup pins an isolated XDG_CONFIG_HOME, so here we
                // point HOME at a fresh per-section temp dir and clear
                // XDG_CONFIG_HOME (snouty treats an empty value as unset) so the
                // login writes land under — and are read back from — that HOME.
                // Each <name> gives a section of the spec its own home, keeping its
                // writes isolated from sibling sections and from the developer's
                // real ~/.config.
                let name = args.first().map(String::as_str).unwrap_or("home");
                let home = env.work_dir.join(name);
                std::fs::create_dir_all(&home)
                    .map_err(|e| err(format!("failed to create isolated HOME: {e}")))?;
                let home = home
                    .to_str()
                    .ok_or_else(|| err("isolated HOME path is not valid UTF-8".to_string()))?;
                env.set_env_var("HOME", home);
                env.set_env_var("XDG_CONFIG_HOME", "");
                Ok(())
            })
            .command("setup-docs-db", |env, _args| {
                // Usage: setup-docs-db
                // Seeds an isolated cache home with the fixture docs.db and points
                // the binary at it via XDG_CACHE_HOME (snouty reads the DB from
                // <cache home>/snouty/docs.db).
                let fixture =
                    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/docs.db");
                let cache_home = env.work_dir.join("cache");
                let snouty_dir = cache_home.join("snouty");
                std::fs::create_dir_all(&snouty_dir)
                    .map_err(|e| err(format!("failed to create cache dir: {e}")))?;
                std::fs::copy(&fixture, snouty_dir.join("docs.db"))
                    .map_err(|e| err(format!("failed to copy fixture docs.db: {e}")))?;
                env.set_env_var("XDG_CACHE_HOME", cache_home.to_str().unwrap());
                Ok(())
            })
            .execute();
        kill_mcp_server();

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
    podman_engine_launch_config_mirror_specs,
    "podman",
    "launch_config_mirror.txt",
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
    podman_engine_validate_env_specs,
    "podman",
    "validate_env.txt",
    false
);
engine_spec_case_test!(
    podman_engine_validate_k8s_specs,
    "podman",
    "validate_k8s.txt",
    false
);
engine_spec_case_test!(
    docker_engine_launch_config_push_specs,
    "docker",
    "launch_config_push.txt",
    true
);
engine_spec_case_test!(
    docker_engine_launch_config_mirror_specs,
    "docker",
    "launch_config_mirror.txt",
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
    docker_engine_validate_env_specs,
    "docker",
    "validate_env.txt",
    false
);
engine_spec_case_test!(
    docker_engine_validate_k8s_specs,
    "docker",
    "validate_k8s.txt",
    false
);
