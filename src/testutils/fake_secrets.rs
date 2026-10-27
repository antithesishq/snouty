//! The fake secrets that the tests use: API keys, passwords, OAuth tokens, and
//! the other credentials. Tests use only these values.
//!
//! Each value contains [`MARKER`], so `git grep fake-` finds every use. The
//! only exceptions are in the `unmarked` list, each with a reason. Rust code
//! refers to the constants. A spec gets each value as the environment variable
//! `FAKE_<NAME>`, for example `${FAKE_API_KEY}` (see `tests/spec.rs`). A
//! Python script cannot import the constants, so it writes the value itself.
//!
//! The `scan` test below fails when a file in the repository holds a
//! secret-shaped literal that is not in this registry. To add a fake secret,
//! add a line to the `fake_secrets!` list, with a doc comment that says what
//! it stands for.

/// The text that each fake secret contains, except the `unmarked` ones.
pub const MARKER: &str = "fake-";

macro_rules! fake_secrets {
    (
        marked { $($(#[$doc:meta])* $name:ident = $value:expr;)* }
        unmarked { $($(#[$udoc:meta])* $uname:ident = $uvalue:expr;)* }
    ) => {
        $($(#[$doc])* pub const $name: &str = $value;)*
        $($(#[$udoc])* pub const $uname: &str = $uvalue;)*

        /// Each fake secret, by the name of its constant.
        pub const ALL: &[(&str, &str)] = &[
            $((stringify!($name), $name),)*
            $((stringify!($uname), $uname),)*
        ];

        /// The fake secrets that do not contain [`MARKER`].
        #[cfg(test)]
        const UNMARKED: &[&str] = &[$($uname),*];
    };
}

fake_secrets! {
    marked {
        /// An API key that a test exports, types at a prompt, or stores.
        API_KEY = "fake-api-key";
        /// An API key in `ANTITHESIS_API_KEY` when a test also types or stores
        /// [`API_KEY`].
        API_KEY_ENV = "fake-env-api-key";
        /// An API key under a named profile, next to [`API_KEY`] in `[default]`.
        API_KEY_PROFILE = "fake-profile-api-key";
        /// An API key that `credentials.toml` already holds. It has the shape of
        /// a real key, because the login prompt shows the last four characters
        /// of a stored key as a hint.
        API_KEY_STORED = "antithesis_api_key_v2_fake-stored_9Pgw";
        /// An API key that replaces [`API_KEY_STORED`]. It has the same shape.
        API_KEY_NEW = "antithesis_api_key_v2_fake-new_7Qxz";
        /// The API key that `MockApiServer` accepts.
        MOCK_SERVER_API_KEY = "fake-mock-server-api-key";
        /// A username for basic authentication. It is not a secret, but
        /// [`BASIC_AUTHORIZATION`] encodes it.
        USERNAME = "fake-user";
        /// A password for basic authentication.
        PASSWORD = "fake-password";
        /// A password that `credentials.toml` already holds, when a test also
        /// uses [`PASSWORD`].
        PASSWORD_STORED = "fake-stored-password";
        /// An OAuth access token.
        ACCESS_TOKEN = "fake-access-token";
        /// An OAuth refresh token.
        REFRESH_TOKEN = "fake-refresh-token";
        /// The access token that a refresh issues in place of [`ACCESS_TOKEN`].
        ACCESS_TOKEN_NEW = "fake-new-access-token";
        /// The refresh token that a refresh issues in place of
        /// [`REFRESH_TOKEN`].
        REFRESH_TOKEN_NEW = "fake-new-refresh-token";
        /// An OAuth authorization code.
        OAUTH_CODE = "fake-oauth-code";
        /// The OAuth flow token, which the sign-in sends as `state`.
        OAUTH_FLOW_TOKEN = "fake-oauth-flow-token";
        /// The request token that GitHub Actions gives a job to fetch an OIDC
        /// token.
        OIDC_REQUEST_TOKEN = "fake-oidc-request-token";
        /// A GitHub Actions OIDC token.
        OIDC_TOKEN = "fake-oidc-token";
        /// The `antithesis.integrations.github.token` launch parameter.
        GITHUB_TOKEN = "fake-github-token";
        /// A session cookie.
        SESSION_COOKIE = "fake-session-cookie";
        /// The password of the throwaway macOS keychain that the keychain test
        /// makes.
        KEYCHAIN_PASSWORD = "fake-keychain-password";
    }
    unmarked {
        /// The `Authorization` header for [`USERNAME`] and [`PASSWORD`]. Base64
        /// hides the marker.
        BASIC_AUTHORIZATION = "Basic ZmFrZS11c2VyOmZha2UtcGFzc3dvcmQ=";
        /// The PKCE code verifier from RFC 7636, Appendix B. A published test
        /// vector must not change.
        PKCE_VERIFIER_RFC7636 = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        /// The PASETO test vector 2-S-1: a `v2.public` token with a real
        /// signature. A published test vector must not change.
        PASETO_V2_PUBLIC_VECTOR = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9HQr8URrGntTu7Dz9J2IF23d1M7-9lH9xiqdGyJNvzp4angPW5Esc7C5huy_M8I8_DjJK2ZXC2SUYuOFM-Q_5Cw";
        /// The PASETO test vector 2-S-2: the claims of 2-S-1, with a footer. A
        /// published test vector must not change.
        PASETO_V2_PUBLIC_VECTOR_WITH_FOOTER = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9flsZsx_gYCR0N_Ec2QxJFFpvQAs7h9HtKwbVK2n1MJ3Rz-hwe8KUqjnd8FAnIJZ601tp7lGkguU63oGbomhoBw.UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz";
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use regex::Regex;
    use std::path::Path;
    use std::sync::LazyLock;

    /// Values in the documentation that show where a real secret goes.
    const PLACEHOLDERS: &[&str] = &["your-api-key", "replaced-by-proxy", "…"];

    /// Files that the scan does not read: this registry, which defines the
    /// values, and the server's API schema, which `scripts/refresh-openapi.py`
    /// replaces. The schema's examples hold report links from the server.
    const SKIPPED: &[&str] = &["src/testutils/fake_secrets.rs", "src/openapi.json"];

    /// The key names that mark a value as a secret.
    const SECRET_KEYS: &str = "api[_-]?key|password|passwd|secret|token";

    #[test]
    fn each_value_has_the_marker_or_is_unmarked() {
        for (name, value) in ALL {
            assert!(
                value.contains(MARKER) != UNMARKED.contains(value),
                "{name} must contain `{MARKER}` if and only if it is unmarked"
            );
        }
    }

    /// No value contains another, so a test that checks for one value with
    /// `contains` cannot match a different one.
    #[test]
    fn no_value_contains_another() {
        for (i, (name, value)) in ALL.iter().enumerate() {
            for (j, (other_name, other)) in ALL.iter().enumerate() {
                assert!(
                    i == j || !value.contains(other),
                    "{name} contains {other_name}"
                );
            }
        }
    }

    #[test]
    fn basic_authorization_encodes_the_username_and_password() {
        let encoded = BASE64_STANDARD.encode(format!("{USERNAME}:{PASSWORD}"));
        assert_eq!(BASIC_AUTHORIZATION, format!("Basic {encoded}"));
    }

    fn registered(value: &str) -> bool {
        ALL.iter().any(|(_, known)| *known == value)
    }

    /// A secret-shaped value is allowed when the registry has it, when it is
    /// a documented placeholder, or when it only names another value:
    /// `${VAR}`, `$VAR`, `$(command)`, `{var}`, `<VALUE>`, `[REDACTED]`, or
    /// `VALUE`. A Python `True`, `False`, or `None` is not a value either, and
    /// nor is a GitHub Actions permission such as `"id-token": "write"`.
    fn allowed(value: &str) -> bool {
        let names_a_variable = value
            .strip_prefix('$')
            .and_then(|rest| rest.chars().next())
            .is_some_and(|c| matches!(c, '{' | '(') || c.is_ascii_alphabetic());
        registered(value)
            || PLACEHOLDERS.contains(&value)
            || names_a_variable
            || value.starts_with(['{', '<'])
            || value
                .trim_matches(['\\', '[', ']'])
                .eq_ignore_ascii_case("redacted")
            || ["True", "False", "None", "read", "write", "none"].contains(&value)
            || value.chars().all(|c| c.is_ascii_uppercase() || c == '_')
    }

    /// A word that contains [`MARKER`].
    static MARKED: LazyLock<Regex> = LazyLock::new(|| {
        let word = "[A-Za-z0-9_.~+/=-]*";
        Regex::new(&format!("{word}{}{word}", regex::escape(MARKER))).unwrap()
    });

    /// A spec variable without braces. testscript replaces `$FAKE_API_KEY`
    /// inside `$FAKE_API_KEY_ENV` too, so a spec must write `${FAKE_...}`.
    static BARE_SPEC_VAR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"\$FAKE_[A-Z_]+").unwrap());

    /// The shapes of a secret. Each pattern captures the value in group 1.
    static SHAPES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            // An `Authorization` header value. A `(` after it makes it a
            // description, such as `Bearer base64url(code:verifier)`.
            r"\b(?:Bearer|Basic|GHA) +([A-Za-z0-9._~+/=-]{6,})(?:[^(A-Za-z0-9._~+/=-]|$)".to_owned(),
            // A quoted value for a secret-named key, in Rust, TOML, JSON, or
            // Python. `auth` is the key of a Docker config credential.
            format!(
                r#"(?i)\b[a-z_]*(?:{SECRET_KEYS}|auth)\\?["']?\s*[:=]\s*\\?["']([^"'\s\\]{{3,}})\\?["']"#
            ),
            // An unquoted value: an environment variable, a launch parameter,
            // or a URL query. `auth` is not here, because `X_AUTH=htpasswd`
            // names a method.
            format!(r#"(?i)\b[a-z_]*(?:{SECRET_KEYS})=([^\s"'`&;,)\]]+)"#),
            // A value passed next to a secret-named environment variable.
            r#""[A-Z_]*(?:API_KEY|PASSWORD|TOKEN|SECRET)[A-Z_]*"\s*,\s*"([^"\s]+)""#.to_owned(),
            // A session cookie.
            r#"(?i)\bsession(?:id)?=([^\s;"'&]+)"#.to_owned(),
            // A PASETO.
            r"\b(v[1-4]\.(?:public|local)\.[A-Za-z0-9_-]{16,}(?:\.[A-Za-z0-9_-]+)?)".to_owned(),
            // A JWT.
            r"\b(eyJ[A-Za-z0-9_-]{4,}\.eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*)".to_owned(),
            // A bcrypt hash.
            r"(\$2[aby]?\$\d\d\$[./A-Za-z0-9]{53})".to_owned(),
            // A GitHub token.
            r"\b(gh[pousr]_[A-Za-z0-9]{20,})".to_owned(),
            // A private key.
            r"(-----BEGIN [A-Z ]*PRIVATE KEY-----)".to_owned(),
        ]
        .iter()
        .map(|pattern| Regex::new(pattern).unwrap())
        .collect()
    });

    /// Each place in `text` that holds a secret it should not: a value with
    /// the marker that is not in the registry (in Rust, any literal with the
    /// marker, because Rust uses the constants), a secret-shaped value that
    /// [`allowed`] rejects, or a spec variable without braces.
    fn findings(text: &str, rust: bool) -> Vec<String> {
        let mut found = Vec::new();
        for (number, line) in text.lines().enumerate() {
            let number = number + 1;
            for value in MARKED.find_iter(line).map(|m| m.as_str()) {
                if rust {
                    found.push(format!(
                        "{number}: `{value}`: use the fake_secrets constant"
                    ));
                } else if !registered(value) && value != MARKER {
                    found.push(format!("{number}: `{value}` is not in the registry"));
                }
            }
            for value in SHAPES
                .iter()
                .flat_map(|shape| shape.captures_iter(line))
                .map(|captures| captures.get(1).unwrap().as_str())
                // The marker check above reports a value with the marker.
                .filter(|value| !value.contains(MARKER))
            {
                if rust && registered(value) {
                    found.push(format!(
                        "{number}: `{value}`: use the fake_secrets constant"
                    ));
                } else if !allowed(value) {
                    found.push(format!("{number}: `{value}` looks like a secret"));
                }
            }
            for var in BARE_SPEC_VAR.find_iter(line).map(|m| m.as_str()) {
                found.push(format!("{number}: `{var}`: write it with braces"));
            }
        }
        found
    }

    /// The scan that keeps every fake secret in the registry. When it fails,
    /// put the value in the `fake_secrets!` list and use it from there.
    ///
    /// It reads each file that git tracks, or does not ignore.
    #[test]
    fn scan() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        // A copy of the source without git, such as a crates.io package, is
        // not where a secret gets committed.
        if !root.join(".git").exists() {
            eprintln!("skipping: {} is not a git checkout", root.display());
            return;
        }
        let output = std::process::Command::new("git")
            .args([
                "ls-files",
                "-z",
                "--cached",
                "--others",
                "--exclude-standard",
            ])
            .current_dir(root)
            .output()
            .expect("run git ls-files");
        assert!(output.status.success(), "git ls-files failed");

        let mut report = Vec::new();
        for relative in String::from_utf8(output.stdout)
            .unwrap()
            .split_terminator('\0')
        {
            if SKIPPED.contains(&relative) {
                continue;
            }
            // A binary file is not text, and a deleted file has no text.
            let Ok(text) = std::fs::read_to_string(root.join(relative)) else {
                continue;
            };
            let rust = Path::new(relative).extension().is_some_and(|e| e == "rs");
            for finding in findings(&text, rust) {
                report.push(format!("{relative}:{finding}"));
            }
        }
        assert!(
            report.is_empty(),
            "secret-shaped literals outside src/testutils/fake_secrets.rs:\n{}",
            report.join("\n")
        );
    }
}
