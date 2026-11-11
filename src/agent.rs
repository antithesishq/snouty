//! The detection of the AI agent harness that runs snouty.

const AGENT_HINT_MAX_LEN: usize = 64;

/// A harness and the variables it exports to the commands it runs. Each
/// variable is verified in the harness's own documentation or source, or
/// observed in its environment.
struct Harness {
    name: &'static str,
    /// Any one of these marks the harness.
    markers: &'static [&'static str],
    version: Option<&'static str>,
    /// The first of these that is set holds the session ID.
    session_id: &'static [&'static str],
}

impl Harness {
    const fn new(name: &'static str, markers: &'static [&'static str]) -> Self {
        Self {
            name,
            markers,
            version: None,
            session_id: &[],
        }
    }
}

/// Agents come before IDE terminals so an agent that runs inside an IDE
/// wins. Claude Code puts its version in `AI_AGENT`.
const HARNESSES: &[Harness] = &[
    Harness {
        session_id: &["CLAUDE_CODE_SESSION_ID"],
        ..Harness::new("claude-code", &["CLAUDECODE"])
    },
    Harness {
        version: Some("CODEX_VERSION"),
        session_id: &["CODEX_THREAD_ID", "CODEX_SESSION_ID"],
        ..Harness::new(
            "codex",
            &[
                "CODEX_THREAD_ID",
                "CODEX_SESSION_ID",
                "CODEX_VERSION",
                "CODEX_SANDBOX",
                "CODEX_SANDBOX_NETWORK_DISABLED",
            ],
        )
    },
    Harness::new("gemini", &["GEMINI_CLI"]),
    Harness::new("opencode", &["OPENCODE"]),
    Harness::new("pi", &["PI_CODING_AGENT"]),
    Harness::new("auggie", &["AUGMENT_AGENT"]),
    Harness::new("goose", &["GOOSE_TERMINAL"]),
    Harness::new("kiro", &["AGENT_CONTEXT_OUT"]),
    Harness::new("junie", &["JUNIE_DATA", "JUNIE_SHIM_PATH"]),
    Harness::new("cursor", &["CURSOR_AGENT"]),
    Harness::new("copilot", &["COPILOT_AGENT"]),
    Harness::new("cline", &["CLINE_ACTIVE"]),
];

/// Identify the AI agent harness that runs snouty, if any. `AI_AGENT` wins;
/// Claude Code sets it to a value such as `claude-code_2-1-267_agent`, and Pi
/// sets it to `pi`. `env` replaces [`crate::env::var`] so a test does not change the
/// process environment.
pub fn hint(env: &impl Fn(&str) -> Option<String>) -> Option<String> {
    env("AI_AGENT")
        .and_then(|raw| sanitize_hint(&raw))
        .or_else(|| marker_harness(env).map(|h| h.name.to_owned()))
}

/// The value of `var` with surrounding whitespace removed, if any is left.
fn read_trimmed(env: &impl Fn(&str) -> Option<String>, var: &str) -> Option<String> {
    let value = env(var)?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

/// The AI agent harness that runs snouty.
#[derive(Debug, PartialEq, Eq)]
pub struct Agent {
    pub name: String,
    pub version: Option<String>,
    pub session_id: Option<String>,
}

/// Identify the AI agent harness that runs snouty, with its version and
/// session ID when the harness exports them. The harness's own variables win
/// over `AI_AGENT`, which any harness can set and a child process inherits.
/// `AI_AGENT` has the form `<name>_<version>_<kind>`, with dashes in the
/// version, for example `claude-code_2-1-267_agent`, or is only a name, such
/// as `pi`.
pub fn detect(env: &impl Fn(&str) -> Option<String>) -> Option<Agent> {
    let ai_agent = read_trimmed(env, "AI_AGENT").and_then(|raw| {
        let mut fields = raw.split('_');
        let name = fields.next().and_then(sanitize_hint)?;
        let version = fields
            .next()
            .filter(|v| {
                v.starts_with(|c: char| c.is_ascii_digit())
                    && v.chars()
                        .all(|c| c.is_ascii_digit() || matches!(c, '-' | '.'))
            })
            .map(|v| v.replace('-', "."));
        Some((name, version))
    });
    let Some(harness) = marker_harness(env) else {
        let (name, version) = ai_agent?;
        return Some(Agent {
            name,
            version,
            session_id: None,
        });
    };
    // Claude Code exports its version only in `AI_AGENT`.
    let ai_agent_version = ai_agent
        .filter(|(name, _)| name == harness.name)
        .and_then(|(_, version)| version);
    Some(Agent {
        name: harness.name.to_owned(),
        version: harness
            .version
            .and_then(|var| read_trimmed(env, var))
            .or(ai_agent_version),
        session_id: harness
            .session_id
            .iter()
            .find_map(|var| read_trimmed(env, var)),
    })
}

/// The first harness in [`HARNESSES`] that one of its markers marks.
fn marker_harness(env: &impl Fn(&str) -> Option<String>) -> Option<&'static Harness> {
    HARNESSES
        .iter()
        .find(|harness| harness.markers.iter().any(|var| env(var).is_some()))
}

/// The User-Agent comment grammar reserves `;`, `(` and `)`, so only
/// `[A-Za-z0-9._/-]` survives, and the result is capped at
/// [`AGENT_HINT_MAX_LEN`] so it cannot bloat the request.
fn sanitize_hint(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        .take(AGENT_HINT_MAX_LEN)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn hint_prefers_ai_agent_and_strips_unsafe_chars() {
        let env = env_of(&[
            ("AI_AGENT", "claude-code_2-1-267_agent"),
            ("CLAUDECODE", "1"),
        ]);
        assert_eq!(hint(&env).as_deref(), Some("claude-code_2-1-267_agent"));

        let env = env_of(&[("AI_AGENT", "bad agent;\r\n(1.0)")]);
        assert_eq!(hint(&env).as_deref(), Some("badagent1.0"));
    }

    #[test]
    fn hint_falls_back_to_harness_markers() {
        assert_eq!(
            hint(&env_of(&[("CLAUDECODE", "1")])).as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            hint(&env_of(&[("CODEX_THREAD_ID", "abc")])).as_deref(),
            Some("codex")
        );
        assert_eq!(hint(&env_of(&[("AI_AGENT", "  ")])), None);
        assert_eq!(
            hint(&env_of(&[("AI_AGENT", "()"), ("CLAUDECODE", "1")])).as_deref(),
            Some("claude-code")
        );
        assert_eq!(hint(&env_of(&[])), None);
    }

    fn agent(name: &str, version: Option<&str>, session_id: Option<&str>) -> Agent {
        Agent {
            name: name.to_owned(),
            version: version.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
        }
    }

    #[test]
    fn detect_reads_name_version_and_session() {
        let env = env_of(&[
            ("AI_AGENT", "claude-code_2-1-267_agent"),
            ("CLAUDECODE", "1"),
            (
                "CLAUDE_CODE_SESSION_ID",
                "d83903be-202d-46c7-8831-50dfd8cdf5bc",
            ),
        ]);
        assert_eq!(
            detect(&env),
            Some(agent(
                "claude-code",
                Some("2.1.267"),
                Some("d83903be-202d-46c7-8831-50dfd8cdf5bc")
            ))
        );
        let env = env_of(&[
            ("CODEX_THREAD_ID", "019a-thread"),
            ("CODEX_VERSION", "0.98.0"),
            ("CODEX_SESSION_ID", "019a-session"),
        ]);
        let long_id = format!("id:{}", "x".repeat(100));
        assert_eq!(
            detect(&env_of(&[
                ("CLAUDECODE", "1"),
                ("CLAUDE_CODE_SESSION_ID", &long_id)
            ])),
            Some(agent("claude-code", None, Some(&long_id)))
        );
        assert_eq!(
            detect(&env),
            Some(agent("codex", Some("0.98.0"), Some("019a-thread")))
        );
        let env = env_of(&[
            ("CODEX_SANDBOX", "seatbelt"),
            ("CODEX_SESSION_ID", "019a-session"),
        ]);
        assert_eq!(
            detect(&env),
            Some(agent("codex", None, Some("019a-session")))
        );
    }

    #[test]
    fn detect_prefers_harness_variables_to_ai_agent() {
        // Codex that runs inside an agent that set `AI_AGENT`.
        let env = env_of(&[
            ("AI_AGENT", "acme_9-9-9_agent"),
            ("CODEX_THREAD_ID", "019a-thread"),
        ]);
        assert_eq!(
            detect(&env),
            Some(agent("codex", None, Some("019a-thread")))
        );
    }

    #[test]
    fn detect_without_version_or_session() {
        assert_eq!(
            detect(&env_of(&[("AI_AGENT", "pi")])),
            Some(agent("pi", None, None))
        );
        // A second field that is not a version is not one.
        assert_eq!(
            detect(&env_of(&[("AI_AGENT", "acme_beta")])),
            Some(agent("acme", None, None))
        );
        // A session variable of another harness is not read.
        assert_eq!(
            detect(&env_of(&[
                ("GEMINI_CLI", "1"),
                ("CLAUDE_CODE_SESSION_ID", "abc")
            ])),
            Some(agent("gemini", None, None))
        );
        assert_eq!(
            detect(&env_of(&[("AI_AGENT", "_1-0"), ("CLAUDECODE", "1")])),
            Some(agent("claude-code", None, None))
        );
        // Without a harness variable, `AI_AGENT` names a harness, but its
        // session variable is not read.
        assert_eq!(
            detect(&env_of(&[
                ("AI_AGENT", "claude-code_2-1-267_agent"),
                ("CLAUDE_CODE_SESSION_ID", "abc")
            ])),
            Some(agent("claude-code", Some("2.1.267"), None))
        );
        assert_eq!(detect(&env_of(&[])), None);
    }

    #[test]
    fn hint_is_capped() {
        let long = "x".repeat(500);
        let hint = hint(&env_of(&[("AI_AGENT", &long)])).unwrap();
        assert_eq!(hint.len(), AGENT_HINT_MAX_LEN);
    }

    /// Whatever the environment holds, the hint that reaches the wire fits
    /// the User-Agent comment grammar and the length cap, and a hint that
    /// already passed is left alone.
    #[hegel::test]
    fn sanitize_hint_is_safe_and_idempotent(tc: hegel::TestCase) {
        let raw = tc.draw(hegel::generators::text());
        let Some(once) = sanitize_hint(&raw) else {
            return;
        };
        assert!(once.len() <= AGENT_HINT_MAX_LEN);
        assert!(
            once.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        );
        assert_eq!(sanitize_hint(&once).as_deref(), Some(once.as_str()));
    }
}
