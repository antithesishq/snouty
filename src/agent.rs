//! The detection of the AI agent harness that runs snouty.

/// A harness and the variables it exports to the commands it runs. Each
/// variable is verified in the harness's own documentation or source, or
/// observed in its environment.
struct AgentHarness {
    name: &'static str,
    /// Any one of these marks the harness. It includes the version and
    /// session variables.
    markers: &'static [&'static str],
    version: Option<&'static str>,
    /// The first of these that is set holds the session ID.
    session_id: &'static [&'static str],
}

/// Declares an [`AgentHarness`] whose markers include its version and session
/// variables.
macro_rules! harness {
    (
        $name:literal,
        markers: [$($marker:literal),* $(,)?]
        $(, version: $version:literal)?
        $(, session_id: [$($session:literal),* $(,)?])?
        $(,)?
    ) => {
        AgentHarness {
            name: $name,
            markers: &[$($marker,)* $($version,)? $($($session,)*)?],
            version: harness!(@option $($version)?),
            session_id: &[$($($session),*)?],
        }
    };
    (@option) => { None };
    (@option $version:literal) => { Some($version) };
}

/// Agents come before IDE terminals so an agent that runs inside an IDE
/// wins.
const HARNESSES: &[AgentHarness] = &[
    harness!(
        "claude-code",
        markers: [
            "CLAUDECODE",
            "CLAUDE_CODE_EXECPATH",
            "CLAUDE_CODE_CHILD_SESSION",
            "CLAUDE_CODE_SESSION_ATTENDED",
            "CLAUDE_PID",
            "CLAUDE_CODE_MESSAGING_SOCKET",
            "CLAUDE_CODE_MESSAGING_TOKEN",
        ],
        session_id: ["CLAUDE_CODE_SESSION_ID"],
    ),
    harness!(
        "codex",
        markers: ["CODEX_SANDBOX", "CODEX_SANDBOX_NETWORK_DISABLED"],
        version: "CODEX_VERSION",
        session_id: ["CODEX_THREAD_ID", "CODEX_SESSION_ID"],
    ),
    harness!("gemini", markers: ["GEMINI_CLI"]),
    // Kilo Code is a fork of opencode that also sets `OPENCODE`.
    harness!("kilo", markers: ["KILO_PID"]),
    harness!("opencode", markers: ["OPENCODE", "OPENCODE_PID"]),
    harness!(
        "pi",
        markers: ["PI_CODING_AGENT"],
        session_id: ["PI_SESSION_ID"],
    ),
    harness!("auggie", markers: ["AUGMENT_AGENT"]),
    harness!(
        "kiro",
        markers: ["AGENT_CONTEXT_OUT"],
        session_id: ["KIRO_SESSION_ID"],
    ),
    harness!(
        "junie",
        markers: ["JUNIE_DATA", "JUNIE_SHIM_PATH", "JUNIE_TMPDIR", "MATTERHORN_TASK"],
        session_id: ["MATTERHORN_SESSION_ID"],
    ),
    harness!(
        "cursor",
        markers: ["CURSOR_AGENT", "CURSOR_INVOKED_AS"],
        session_id: ["CURSOR_CONVERSATION_ID"],
    ),
    harness!(
        "copilot",
        markers: ["COPILOT_CLI"],
        session_id: ["COPILOT_AGENT_SESSION_ID"],
    ),
    harness!("crush", markers: ["CRUSH"]),
    harness!(
        "qwen-code",
        markers: ["QWEN_CODE"],
        session_id: ["QWEN_CODE_SESSION_ID"],
    ),
    harness!("droid", markers: [], session_id: ["FACTORY_SESSION_ID"]),
    // Goose 1.30 and later set only `AGENT_SESSION_ID` on shell commands.
    // That name is generic, so goose comes after every other agent.
    harness!(
        "goose",
        markers: ["GOOSE_TERMINAL"],
        session_id: ["AGENT_SESSION_ID"],
    ),
    harness!("cline", markers: ["CLINE_ACTIVE"]),
    harness!("roo", markers: ["ROO_ACTIVE"]),
];

impl AgentHarness {
    /// The first harness in [`HARNESSES`] that a marker marks.
    fn detect(env: &impl Fn(&str) -> Option<String>) -> Option<&'static Self> {
        HARNESSES
            .iter()
            .find(|harness| harness.markers.iter().any(|var| env(var).is_some()))
    }
}

fn read_trimmed(env: &impl Fn(&str) -> Option<String>, var: &str) -> Option<String> {
    let value = env(var)?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

#[derive(Debug, PartialEq, Eq)]
pub struct Agent {
    pub name: String,
    pub version: Option<String>,
    pub session_id: Option<String>,
}

/// Parse `AI_AGENT`, a shared convention that any harness can set:
/// `<name>_<version>_<kind>` with dashes in the version
/// (`claude-code_2-1-267_agent`), or only a name (`pi`).
fn parse_ai_agent(raw: &str) -> Option<Agent> {
    let mut fields = raw.split('_');
    let name = fields.next().filter(|name| !name.is_empty())?.to_owned();
    let version = fields
        .next()
        .filter(|v| {
            v.starts_with(|c: char| c.is_ascii_digit())
                && v.chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '-' | '.'))
        })
        .map(|v| v.replace('-', "."));
    Some(Agent {
        name,
        version,
        session_id: None,
    })
}

/// Identify the AI agent harness that runs snouty, with its version and
/// session ID when the harness exports them. `env` replaces
/// [`crate::env::var`] so a test does not change the process environment.
///
/// A harness's own variables win. `AI_AGENT` gives the version of a harness
/// that exports none of its own, and names a harness that snouty does not
/// know.
pub fn detect(env: &impl Fn(&str) -> Option<String>) -> Option<Agent> {
    let ai_agent = read_trimmed(env, "AI_AGENT").and_then(|raw| parse_ai_agent(&raw));
    let Some(harness) = AgentHarness::detect(env) else {
        return ai_agent;
    };
    let ai_agent_version = ai_agent
        .filter(|agent| agent.name == harness.name)
        .and_then(|agent| agent.version);
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

    fn agent(name: &str, version: Option<&str>, session_id: Option<&str>) -> Agent {
        Agent {
            name: name.to_owned(),
            version: version.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
        }
    }

    #[test]
    fn parse_ai_agent_reads_name_and_version() {
        assert_eq!(
            parse_ai_agent("claude-code_2-1-267_agent"),
            Some(agent("claude-code", Some("2.1.267"), None))
        );
        assert_eq!(parse_ai_agent("pi"), Some(agent("pi", None, None)));
        // A non-numeric second field gives no version.
        assert_eq!(parse_ai_agent("acme_beta"), Some(agent("acme", None, None)));
        assert_eq!(parse_ai_agent("_1-0"), None);
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
        // A session ID is recorded as the harness exports it.
        let long_id = format!("id:{}", "x".repeat(100));
        assert_eq!(
            detect(&env_of(&[("CLAUDE_CODE_SESSION_ID", &long_id)])),
            Some(agent("claude-code", None, Some(&long_id)))
        );
        let env = env_of(&[
            ("CODEX_THREAD_ID", "019a-thread"),
            ("CODEX_VERSION", "0.98.0"),
            ("CODEX_SESSION_ID", "019a-session"),
        ]);
        assert_eq!(
            detect(&env),
            Some(agent("codex", Some("0.98.0"), Some("019a-thread")))
        );
        assert_eq!(
            detect(&env_of(&[("CODEX_SESSION_ID", "019a-session")])),
            Some(agent("codex", None, Some("019a-session")))
        );
    }

    #[test]
    fn detect_orders_harnesses_that_share_variables() {
        // Kilo Code also sets opencode's marker.
        assert_eq!(
            detect(&env_of(&[("OPENCODE", "1"), ("KILO_PID", "42")])),
            Some(agent("kilo", None, None))
        );
        // Goose's generic session variable loses to another agent's marker.
        assert_eq!(
            detect(&env_of(&[
                ("AGENT_SESSION_ID", "20261111_3"),
                ("QWEN_CODE", "1"),
                ("QWEN_CODE_SESSION_ID", "qwen-session"),
            ])),
            Some(agent("qwen-code", None, Some("qwen-session")))
        );
        assert_eq!(
            detect(&env_of(&[("AGENT_SESSION_ID", "20261111_3")])),
            Some(agent("goose", None, Some("20261111_3")))
        );
    }

    #[test]
    fn detect_prefers_harness_variables_to_ai_agent() {
        // `AI_AGENT` that names another agent gives no version.
        let env = env_of(&[
            ("AI_AGENT", "acme_9-9-9_agent"),
            ("CODEX_THREAD_ID", "019a-thread"),
        ]);
        assert_eq!(
            detect(&env),
            Some(agent("codex", None, Some("019a-thread")))
        );
        // A harness without `AI_AGENT` is still detected.
        assert_eq!(
            detect(&env_of(&[("GEMINI_CLI", "1")])),
            Some(agent("gemini", None, None))
        );
        assert_eq!(
            detect(&env_of(&[("AI_AGENT", "pi")])),
            Some(agent("pi", None, None))
        );
        assert_eq!(detect(&env_of(&[])), None);
    }
}
