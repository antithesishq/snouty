//! The fake secrets that the tests use: API keys, passwords, OAuth tokens, and
//! the other credentials. Tests use only these values.
//!
//! Each value contains [`MARKER`], so `git grep fake-` finds every use. The
//! only exceptions are published test vectors, which must stay byte-for-byte
//! the same as their source. Rust code refers to the constants. A spec gets
//! each value as the environment variable `FAKE_<NAME>`, for example
//! `${FAKE_API_KEY}` (see `tests/spec.rs`). A Python script cannot import the
//! constants, so it writes the value itself.
//!
//! The `scan` test below fails when a source file holds a secret-shaped
//! literal that is not in this registry. To add a fake secret, add a line to
//! the `fake_secrets!` list, with a doc comment that says what it stands for.

/// The text that each fake secret contains, except the published test
/// vectors.
pub const MARKER: &str = "fake-";

/// The environment variable that a spec reads a fake secret from.
pub fn spec_var(name: &str) -> String {
    format!("FAKE_{name}")
}

macro_rules! fake_secrets {
    ($($(#[$doc:meta])* $name:ident = $value:expr;)*) => {
        $($(#[$doc])* pub const $name: &str = $value;)*

        /// Each fake secret, by the name of its constant.
        pub const ALL: &[(&str, &str)] = &[$((stringify!($name), $name)),*];
    };
}

fake_secrets! {
    /// An API key that a test exports, types at a prompt, or stores.
    API_KEY = "fake-api-key";
    /// An API key in `ANTITHESIS_API_KEY` when a test also types or stores
    /// [`API_KEY`].
    API_KEY_ENV = "fake-api-key-env";
    /// An API key under a named profile, next to [`API_KEY`] in `[default]`.
    API_KEY_PROFILE = "fake-api-key-profile";
    /// An API key that `credentials.toml` already holds. It has the shape of
    /// a real key, because the login prompt shows the last four characters of
    /// a stored key as a hint.
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
    PASSWORD_STORED = "fake-password-stored";
    /// The `Authorization` header for [`USERNAME`] and [`PASSWORD`]. Base64
    /// hides the marker.
    BASIC_AUTHORIZATION = "Basic ZmFrZS11c2VyOmZha2UtcGFzc3dvcmQ=";
    /// An OAuth access token.
    ACCESS_TOKEN = "fake-access-token";
    /// An OAuth refresh token.
    REFRESH_TOKEN = "fake-refresh-token";
    /// The access token that a refresh issues in place of [`ACCESS_TOKEN`].
    ACCESS_TOKEN_NEW = "fake-access-token-new";
    /// The refresh token that a refresh issues in place of [`REFRESH_TOKEN`].
    REFRESH_TOKEN_NEW = "fake-refresh-token-new";
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
    /// The PKCE code verifier from RFC 7636, Appendix B. A published test
    /// vector, so it has no marker.
    PKCE_VERIFIER_RFC7636 = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    /// The PASETO test vector 2-S-1: a `v2.public` token with a real
    /// signature. A published test vector, so it has no marker.
    PASETO_V2_PUBLIC_VECTOR = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9HQr8URrGntTu7Dz9J2IF23d1M7-9lH9xiqdGyJNvzp4angPW5Esc7C5huy_M8I8_DjJK2ZXC2SUYuOFM-Q_5Cw";
    /// The PASETO test vector 2-S-2: the claims of 2-S-1, with a footer. A
    /// published test vector, so it has no marker.
    PASETO_V2_PUBLIC_VECTOR_WITH_FOOTER = "v2.public.eyJkYXRhIjoidGhpcyBpcyBhIHNpZ25lZCBtZXNzYWdlIiwiZXhwIjoiMjAxOS0wMS0wMVQwMDowMDowMCswMDowMCJ9flsZsx_gYCR0N_Ec2QxJFFpvQAs7h9HtKwbVK2n1MJ3Rz-hwe8KUqjnd8FAnIJZ601tp7lGkguU63oGbomhoBw.UGFyYWdvbiBJbml0aWF0aXZlIEVudGVycHJpc2Vz";
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use regex::Regex;
    use std::path::{Path, PathBuf};
    use std::sync::LazyLock;

    /// The registry entries with no [`MARKER`]: each one has a reason in its
    /// doc.
    const UNMARKED: &[&str] = &[
        BASIC_AUTHORIZATION,
        PKCE_VERIFIER_RFC7636,
        PASETO_V2_PUBLIC_VECTOR,
        PASETO_V2_PUBLIC_VECTOR_WITH_FOOTER,
    ];

    /// Values in the documentation that show where a real secret goes.
    const PLACEHOLDERS: &[&str] = &["your-api-key", "replaced-by-proxy", "…"];

    /// The files and directories that the scan reads, from the crate root.
    const SCANNED: &[&str] = &[
        "src",
        "tests",
        "examples",
        "specs",
        "specs_engine",
        "scripts",
        "help",
        "README.md",
        "COOKBOOK.md",
        "CONTRIBUTING.md",
        "AGENTS.md",
    ];

    /// Generated from the server's schema, and too large to scan quickly.
    const SKIPPED: &[&str] = &["src/openapi.json"];

    #[test]
    fn each_value_has_the_marker_or_a_reason() {
        for (name, value) in ALL {
            assert!(
                value.contains(MARKER) || UNMARKED.contains(value),
                "{name} has no `{MARKER}` marker"
            );
        }
    }

    #[test]
    fn each_value_is_unique() {
        for (i, (name, value)) in ALL.iter().enumerate() {
            assert!(
                ALL[..i].iter().all(|(_, other)| other != value),
                "{name} repeats a value"
            );
        }
    }

    #[test]
    fn basic_authorization_encodes_the_username_and_password() {
        let encoded = BASE64_STANDARD.encode(format!("{USERNAME}:{PASSWORD}"));
        assert_eq!(BASIC_AUTHORIZATION, format!("Basic {encoded}"));
    }

    fn files_under(path: &Path, out: &mut Vec<PathBuf>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                files_under(&entry.unwrap().path(), out);
            }
        } else if path.is_file() {
            out.push(path.to_owned());
        }
    }

    /// A value is allowed when the registry has it, when it is a documented
    /// placeholder or the bare marker, or when it only names another value:
    /// `${VAR}`, `{var}`, `$VAR`, `<VALUE>`, `[REDACTED]`, or `VALUE`. A Python
    /// `True`, `False`, or `None` is not a value either.
    fn allowed(value: &str) -> bool {
        ALL.iter().any(|(_, known)| *known == value)
            || PLACEHOLDERS.contains(&value)
            || value == MARKER
            || value.starts_with(['$', '{', '<', '[', '\\'])
            || ["True", "False", "None"].contains(&value)
            || value.chars().all(|c| c.is_ascii_uppercase() || c == '_')
    }

    /// Each place in `text` that holds a secret it should not: a value with
    /// the marker that is not in the registry (in Rust, any literal with the
    /// marker, because Rust uses the constants), or a secret-shaped value that
    /// [`allowed`] rejects.
    fn findings(text: &str, rust: bool) -> Vec<String> {
        let mut found = Vec::new();
        for (number, line) in text.lines().enumerate() {
            let number = number + 1;
            // A Rust comment can show the shape of a header, such as
            // `Bearer base64url(code:verifier)`.
            if rust && line.trim_start().starts_with("//") {
                continue;
            }
            for value in MARKED.find_iter(line).map(|m| m.as_str()) {
                if rust {
                    found.push(format!(
                        "{number}: `{value}`: use the fake_secrets constant"
                    ));
                } else if !allowed(value) {
                    found.push(format!("{number}: `{value}` is not in the registry"));
                }
            }
            for captures in SHAPES.iter().flat_map(|shape| shape.captures_iter(line)) {
                let value = &captures[1];
                if value.contains(MARKER) {
                    // The marker check above reports it.
                } else if rust && ALL.iter().any(|(_, known)| *known == value) {
                    found.push(format!(
                        "{number}: `{value}`: use the fake_secrets constant"
                    ));
                } else if !allowed(value) {
                    found.push(format!("{number}: `{value}` looks like a secret"));
                }
            }
        }
        found
    }

    /// A word that contains [`MARKER`].
    static MARKED: LazyLock<Regex> = LazyLock::new(|| {
        let word = "[A-Za-z0-9_.~+/=-]*";
        Regex::new(&format!("{word}{}{word}", regex::escape(MARKER))).unwrap()
    });

    /// The shapes of a secret. Each pattern captures the value in group 1.
    static SHAPES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            // An `Authorization` header value.
            r"\b(?:Bearer|Basic|GHA) +([A-Za-z0-9._~+/=-]{6,})",
            // A quoted value for a secret-named key, in Rust, TOML, JSON, or
            // Python.
            r#"(?i)\b[a-z_]*(?:api[_-]?key|password|passwd|secret|auth|token)\\?["']?\s*[:=]\s*\\?["']([^"'\s\\]{3,})\\?["']"#,
            // An unquoted value: an environment variable, a launch parameter,
            // or a URL query.
            r#"(?i)\b[a-z_]*(?:api[_-]?key|password|passwd|secret|token)=([^\s"'`&;,)\]]+)"#,
            // A value passed next to a secret-named environment variable.
            r#""[A-Z_]*(?:API_KEY|PASSWORD|TOKEN|SECRET)[A-Z_]*"\s*,\s*"([^"\s]+)""#,
            // A session cookie.
            r#"(?i)\bsession(?:id)?=([^\s;"'&]+)"#,
            // A PASETO.
            r"\b(v[1-4]\.(?:public|local)\.[A-Za-z0-9_-]{16,}(?:\.[A-Za-z0-9_-]+)?)",
            // A JWT.
            r"\b(eyJ[A-Za-z0-9_-]{4,}\.eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*)",
            // A bcrypt hash.
            r"(\$2[aby]?\$\d\d\$[./A-Za-z0-9]{53})",
            // A GitHub token.
            r"\b(gh[pousr]_[A-Za-z0-9]{20,})",
            // A private key.
            r"(-----BEGIN [A-Z ]*PRIVATE KEY-----)",
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).unwrap())
        .collect()
    });

    /// The scan that keeps every fake secret in the registry. When it fails,
    /// put the value in the `fake_secrets!` list and use it from there.
    #[test]
    fn scan() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let registry = root.join("src/testutils/fake_secrets.rs");
        let mut files = Vec::new();
        for path in SCANNED {
            files_under(&root.join(path), &mut files);
        }

        let mut report = Vec::new();
        for file in files {
            let relative = file.strip_prefix(root).unwrap();
            if file == registry || SKIPPED.iter().any(|s| relative == Path::new(s)) {
                continue;
            }
            // A binary fixture is not text, and holds no literal.
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let rust = file.extension().is_some_and(|e| e == "rs");
            for finding in findings(&text, rust) {
                report.push(format!("{}:{finding}", relative.display()));
            }
        }
        assert!(
            report.is_empty(),
            "secret-shaped literals outside src/testutils/fake_secrets.rs:\n{}",
            report.join("\n")
        );
    }
}
