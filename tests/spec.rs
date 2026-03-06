use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Stdio;
use std::thread;
use testscript_rs::testscript;

fn err(msg: String) -> testscript_rs::Error {
    testscript_rs::Error::Generic(msg)
}

#[test]
fn spec_tests() {
    let result = testscript::run("specs")
        .setup(|env| {
            env.set_env_var("RUST_LOG", "debug");
            Ok(())
        })
        .command("snouty", |env, args| {
            // Build the child process manually with env_clear() so that
            // tests run in a clean environment containing only variables
            // explicitly set via `env.set_env_var` or the `env` script
            // command.  testscript-rs's built-in execute_command uses
            // Command::envs (additive), which leaks the parent env.
            let bin = env!("CARGO_BIN_EXE_snouty");
            let mut cmd = std::process::Command::new(bin);
            cmd.args(args)
                .current_dir(&env.current_dir)
                .env_clear()
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            // Forward LLVM_PROFILE_FILE so cargo-llvm-cov captures coverage
            // from the spawned snouty binary.
            if let Ok(v) = std::env::var("LLVM_PROFILE_FILE") {
                cmd.env("LLVM_PROFILE_FILE", v);
            }
            for (k, v) in &env.env_vars {
                cmd.env(k, v);
            }
            if env.next_stdin.is_some() {
                cmd.stdin(Stdio::piped());
            }
            let mut child = cmd.spawn().map_err(|e| err(format!("failed to spawn snouty: {e}")))?;
            if let Some(stdin_content) = env.next_stdin.take() {
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(&stdin_content)
                    .map_err(|e| err(format!("failed to write stdin: {e}")))?;
            }
            let output = child
                .wait_with_output()
                .map_err(|e| err(format!("failed to wait for snouty: {e}")))?;
            let success = output.status.success();
            env.last_output = Some(output);
            if !success {
                return Err(err("snouty exited with non-zero status".to_string()));
            }
            Ok(())
        })
        .command("mock-server", |env, args| {
            // Usage: mock-server <status> <body>
            // Starts a TCP mock HTTP server, sets ANTITHESIS_BASE_URL and auth env vars.
            if args.len() < 2 {
                return Err(err("mock-server requires <status> <body>".to_string()));
            }
            let status: u16 = args[0]
                .parse()
                .map_err(|e| err(format!("invalid status code: {e}")))?;
            let body = args[1..].join(" ");

            let listener = TcpListener::bind("127.0.0.1:0")
                .map_err(|e| err(format!("failed to bind: {e}")))?;
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
        })
        .command("setup-docs-db", |env, args| {
            // Usage: setup-docs-db
            // Copies the fixture docs.db into the workdir and sets ANTITHESIS_DOCS_DB_PATH.
            let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/docs.db");
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
        .command("mkfile", |env, args| {
            // Usage: mkfile <path> [content...]
            // Creates a file at the given path (relative to work_dir), creating
            // parent directories as needed.
            if args.is_empty() {
                return Err(err("mkfile requires <path> [content...]".to_string()));
            }
            let path = env.work_dir.join(&args[0]);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| err(format!("mkfile: {e}")))?;
            }
            let content = if args.len() > 1 {
                args[1..].join(" ")
            } else {
                String::new()
            };
            std::fs::write(&path, content).map_err(|e| err(format!("mkfile: {e}")))?;
            Ok(())
        })
        .execute();

    if let Err(e) = result {
        panic!("\n{e}");
    }
}
