//! Exercises `snouty login`'s **real macOS Keychain** path end-to-end.
//!
//! Every other test forces `SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1` — the
//! keychain "isn't something we can mock for each test case" (see
//! `tests/spec.rs` / `tests/support/mod.rs`) — so the Security-framework path is
//! otherwise never covered. This test drives it for real.
//!
//! It only runs for real on a **GitHub macOS runner**. To stay hermetic it has
//! to redirect snouty away from the developer's `login.keychain`: snouty's store
//! resolves the User-domain *default* keychain (`SecKeychain::default_for_domain`
//! in `apple-native-keyring-store`), which honors
//! `security default-keychain -d user -s`. So the test creates a throwaway
//! keychain, points the user default at it, and restores + deletes it afterward
//! (even on panic). Changing the machine's default keychain is why it's gated to
//! CI — we don't want to touch a real Mac's keychain during `cargo test`. It's
//! safe alongside the parallel test suite because every *other* test disables the
//! keychain, so nothing else reads or writes it during the window.
//!
//! The body compiles on every platform (so Linux CI still type-checks it) and
//! no-ops unless it is actually running on a macOS GitHub runner.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn security(args: &[&str]) -> Output {
    Command::new("/usr/bin/security")
        .args(args)
        .output()
        .expect("run /usr/bin/security")
}

/// Restores the user default keychain and deletes the throwaway one — even if the
/// test panics partway through.
struct KeychainGuard {
    keychain: PathBuf,
    original_default: String,
}

impl Drop for KeychainGuard {
    fn drop(&mut self) {
        security(&[
            "default-keychain",
            "-d",
            "user",
            "-s",
            &self.original_default,
        ]);
        if let Some(path) = self.keychain.to_str() {
            security(&["delete-keychain", path]);
        }
    }
}

/// Run `snouty <args>` under an isolated `$HOME`, feeding `stdin` to the prompts,
/// with the keychain **enabled** (unlike the rest of the suite).
fn run_snouty(home: &Path, args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_snouty"))
        .args(args)
        .env("HOME", home)
        // Empty XDG_CONFIG_HOME is treated as unset, so settings/credentials land
        // under our temp HOME's ~/.config/snouty rather than the runner's.
        .env("XDG_CONFIG_HOME", "")
        .env_remove("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE")
        .env_remove("ANTITHESIS_API_KEY")
        .env_remove("ANTITHESIS_USERNAME")
        .env_remove("ANTITHESIS_PASSWORD")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn snouty");
    child
        .stdin
        .take()
        .expect("snouty stdin")
        .write_all(stdin.as_bytes())
        .expect("write snouty stdin");
    child.wait_with_output().expect("wait for snouty")
}

fn combined(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn login_persists_to_real_macos_keychain() {
    // Runs for real only on a macOS GitHub runner (it mutates the machine's user
    // default keychain, which we won't do on a developer's Mac).
    if !cfg!(target_os = "macos") || std::env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("skipping real-keychain test: not a macOS GitHub Actions runner");
        return;
    }

    let base = std::env::temp_dir().join(format!("snouty-keychain-it-{}", std::process::id()));
    let home = base.join("home");
    std::fs::create_dir_all(&home).expect("create temp HOME");
    let keychain = base.join("snouty-test.keychain-db");
    let kc = keychain.to_str().expect("keychain path utf-8");
    let password = "snouty-test-keychain";

    // Create the throwaway keychain and capture the current default before we
    // repoint it, so the guard can put everything back.
    assert!(
        security(&["create-keychain", "-p", password, kc])
            .status
            .success(),
        "create-keychain failed"
    );
    let original_default = {
        let out = security(&["default-keychain", "-d", "user"]);
        // Prints e.g. `    "/Users/runner/Library/Keychains/login.keychain-db"`.
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .trim_matches('"')
            .to_string()
    };
    // From here on, always restore the default and delete the keychain.
    let _guard = KeychainGuard {
        keychain: keychain.clone(),
        original_default,
    };
    // No auto-lock timeout (so it can't relock mid-test), unlocked, and made the
    // user default so snouty's store resolves to it.
    assert!(
        security(&["set-keychain-settings", kc]).status.success(),
        "set-keychain-settings failed"
    );
    assert!(
        security(&["unlock-keychain", "-p", password, kc])
            .status
            .success(),
        "unlock-keychain failed"
    );
    assert!(
        security(&["default-keychain", "-d", "user", "-s", kc])
            .status
            .success(),
        "setting default keychain failed"
    );

    // --- Write an API key; it must land in the keychain, not a file. ---
    let out = run_snouty(
        &home,
        &["login"],
        "acme\nregistry.example.com/acme/app\n2\nsk-KEYCHAIN-TEST\n",
    );
    let text = combined(&out);
    assert!(out.status.success(), "login failed:\n{text}");
    assert!(
        text.contains("in the system keychain"),
        "login summary should name the keychain, got:\n{text}"
    );
    assert!(
        !home.join(".config/snouty/credentials.toml").exists(),
        "credentials.toml must not be written when the keychain is used"
    );

    // The credential is really in the keychain, stored as snouty's JSON blob.
    let found = security(&[
        "find-generic-password",
        "-s",
        "snouty",
        "-a",
        "_default_",
        "-w",
        kc,
    ]);
    assert!(found.status.success(), "credential not found in keychain");
    let stored = String::from_utf8_lossy(&found.stdout);
    assert!(
        stored.contains("sk-KEYCHAIN-TEST") && stored.contains("ApiKey"),
        "unexpected keychain payload: {stored}"
    );

    // --- Read-back: a second login reusing the stored key (all blank) succeeds
    // only if snouty read it back out of the keychain. ---
    let reuse = run_snouty(&home, &["login"], "\n\n\n\n");
    let reuse_text = combined(&reuse);
    assert!(reuse.status.success(), "reuse login failed:\n{reuse_text}");
    assert!(
        reuse_text.contains("in the system keychain"),
        "reuse should still resolve/store via the keychain, got:\n{reuse_text}"
    );

    // --- Profile scoping uses a distinct keychain entry (`profile_<name>`). ---
    let prof = run_snouty(
        &home,
        &["--profile", "prod", "login"],
        "acme\nregistry.example.com/acme/app\n2\nsk-PROD-KEY\n",
    );
    let prof_text = combined(&prof);
    assert!(prof.status.success(), "profile login failed:\n{prof_text}");
    let prof_found = security(&[
        "find-generic-password",
        "-s",
        "snouty",
        "-a",
        "profile_prod",
        "-w",
        kc,
    ]);
    assert!(
        prof_found.status.success(),
        "profile credential not found under `profile_prod`"
    );
    assert!(
        String::from_utf8_lossy(&prof_found.stdout).contains("sk-PROD-KEY"),
        "profile keychain entry has the wrong payload"
    );

    // Best-effort cleanup of the temp tree (the keychain itself is handled by the
    // guard via `security delete-keychain`).
    let _ = std::fs::remove_dir_all(&base);
}
