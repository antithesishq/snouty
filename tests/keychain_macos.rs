//! Exercises `snouty login`'s **real macOS Keychain** path end-to-end.
//!
//! Every other test forces `SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE=1` — the
//! keychain "isn't something we can mock for each test case" (see
//! `tests/spec.rs` / `tests/support/mod.rs`) — so the Security-framework path is
//! otherwise never covered. This test drives it for real.
//!
//! It only runs for real on a **GitHub macOS runner**. To stay hermetic it runs
//! *both* the `security` setup and snouty under one throwaway `$HOME`: macOS
//! resolves the default keychain and keychain search list from
//! `$HOME/Library/Preferences` (and `apple-native-keyring-store`'s store reads the
//! default via `SecKeychainCopyDefault`), so the keychain snouty finds is only the
//! one we configured if we configure it under the *same* HOME snouty runs with. A
//! HOME mismatch — `security` under the runner's real home, snouty under a temp
//! home — is why an earlier version failed with "A default keychain could not be
//! found": snouty looked in a home where nothing had been set up. Running entirely
//! under the temp HOME also means the machine's real login keychain is never
//! touched. It's gated to CI so `cargo test` on a developer's Mac doesn't spawn
//! keychains at all. It's safe alongside the parallel suite because every *other*
//! test disables the keychain, so nothing else reads or writes one during the
//! window.
//!
//! The body compiles on every platform (so Linux CI still type-checks it) and
//! no-ops unless it is actually running on a macOS GitHub runner.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// Run `/usr/bin/security` under `home` so its keychain preferences (default
/// keychain, search list) land in the same `$HOME/Library/Preferences` snouty
/// reads back — see the module docs.
fn security(home: &Path, args: &[&str]) -> Output {
    Command::new("/usr/bin/security")
        .args(args)
        .env("HOME", home)
        .output()
        .expect("run /usr/bin/security")
}

/// Restores the default keychain and deletes the throwaway one — even if the test
/// panics partway through. Everything lives under the throwaway `$HOME`, so this
/// is mostly belt-and-suspenders on top of the temp-tree cleanup.
struct KeychainGuard {
    home: PathBuf,
    keychain: PathBuf,
    original_default: String,
}

impl Drop for KeychainGuard {
    fn drop(&mut self) {
        // A fresh temp HOME has no prior default, so only restore when we actually
        // captured one (otherwise `default-keychain -s ""` just errors noisily).
        if !self.original_default.is_empty() {
            security(
                &self.home,
                &["default-keychain", "-s", &self.original_default],
            );
        }
        if let Some(path) = self.keychain.to_str() {
            security(&self.home, &["delete-keychain", path]);
        }
    }
}

/// Run `snouty <args>` under an isolated `$HOME`, feeding `stdin` to the prompts,
/// with the keychain **enabled** (unlike the rest of the suite).
fn run_snouty(home: &Path, args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_snouty"))
        .args(args)
        // Same throwaway HOME the `security` setup ran under, so snouty resolves
        // the default keychain we configured (see module docs).
        .env("HOME", home)
        // Unset XDG_CONFIG_HOME so settings/credentials land under this HOME's
        // ~/.config/snouty rather than the runner's.
        .env_remove("XDG_CONFIG_HOME")
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
    // `security` writes the default-keychain / search-list preferences under
    // `$HOME/Library/Preferences`; on a fresh temp HOME that directory does not
    // exist yet, so create it (and Library/Keychains) before configuring anything.
    std::fs::create_dir_all(home.join("Library/Preferences")).expect("create Library/Preferences");
    std::fs::create_dir_all(home.join("Library/Keychains")).expect("create Library/Keychains");
    let keychain = base.join("snouty-test.keychain-db");
    let kc = keychain.to_str().expect("keychain path utf-8");
    let password = "snouty-test-keychain";

    // Create the throwaway keychain and capture the current default before we
    // repoint it, so the guard can put everything back. (Under a fresh temp HOME
    // there is usually no prior default, so this is typically empty.)
    assert!(
        security(&home, &["create-keychain", "-p", password, kc])
            .status
            .success(),
        "create-keychain failed"
    );
    let original_default = {
        let out = security(&home, &["default-keychain"]);
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .trim_matches('"')
            .to_string()
    };
    // From here on, always restore the default and delete the keychain.
    let _guard = KeychainGuard {
        home: home.clone(),
        keychain: keychain.clone(),
        original_default,
    };
    // No auto-lock timeout (so it can't relock mid-test), unlocked, and made the
    // default so snouty's store resolves to it.
    assert!(
        security(&home, &["set-keychain-settings", kc])
            .status
            .success(),
        "set-keychain-settings failed"
    );
    assert!(
        security(&home, &["unlock-keychain", "-p", password, kc])
            .status
            .success(),
        "unlock-keychain failed"
    );
    assert!(
        security(&home, &["default-keychain", "-s", kc])
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
    let found = security(
        &home,
        &[
            "find-generic-password",
            "-s",
            "snouty",
            "-a",
            "_default_",
            "-w",
            kc,
        ],
    );
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
    let prof_found = security(
        &home,
        &[
            "find-generic-password",
            "-s",
            "snouty",
            "-a",
            "profile_prod",
            "-w",
            kc,
        ],
    );
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
