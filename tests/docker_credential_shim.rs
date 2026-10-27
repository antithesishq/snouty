//! `docker-credential-snouty`, as Docker runs it: the shim runs the `snouty`
//! next to it, with the same stdin, stdout and exit status.

use assert_cmd::Command;
use tempfile::TempDir;

/// The shim, in an empty working directory and home, with only `vars` set.
fn shim(home: &TempDir, vars: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_docker-credential-snouty"));
    command
        .current_dir(home.path())
        .env_clear()
        .env("HOME", home.path())
        // No PATH: the shim must find the `snouty` next to it.
        .env("SNOUTY_DISABLE_KEYCHAIN_CREDENTIAL_STORAGE", "1")
        .envs(vars.iter().copied());
    command
}

const ACME: [(&str, &str); 3] = [
    ("ANTITHESIS_TENANT", "acme"),
    ("ANTITHESIS_REPOSITORY", "registry.acme.antithesis.com/app"),
    ("ANTITHESIS_API_KEY", "shim-key"),
];

#[test]
fn get_prints_the_credential() {
    let home = TempDir::new().unwrap();
    shim(&home, &ACME)
        .arg("get")
        .write_stdin("registry.acme.antithesis.com")
        .assert()
        .success()
        .stdout(
            "{\"ServerURL\":\"registry.acme.antithesis.com\",\"Username\":\"snouty-token\",\"Secret\":\"shim-key\"}\n",
        );
}

#[test]
fn not_found_passes_through_with_its_exit_status() {
    let home = TempDir::new().unwrap();
    shim(&home, &ACME)
        .arg("get")
        .write_stdin("registry.example.com")
        .assert()
        .code(1)
        .stdout("credentials not found in native keychain\n")
        .stderr("");
}

#[test]
fn store_reads_stdin_and_succeeds() {
    let home = TempDir::new().unwrap();
    shim(&home, &ACME)
        .arg("store")
        .write_stdin(r#"{"ServerURL":"registry.acme.antithesis.com","Username":"u","Secret":"s"}"#)
        .assert()
        .success()
        .stdout("");
}
