//! The User-Agent header snouty sends, and the AI agent harness detection
//! that feeds it.

use crate::env;

/// User-Agent string sent with every HTTP request snouty makes.
///
/// When an AI agent harness runs snouty, the string ends with an
/// `agent=<value>` field that names the harness.
pub fn user_agent() -> String {
    user_agent_with(agent_hint(|name| env::var(name).ok().flatten()).as_deref())
}

/// The User-Agent string with `agent` as the trailing `agent=` field. `None`
/// omits the field.
pub fn user_agent_with(agent: Option<&str>) -> String {
    let agent = agent.map(|a| format!("; agent={a}")).unwrap_or_default();
    format!(
        "snouty/{} ({}; {}; rust{}{agent})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

const AGENT_HINT_MAX_LEN: usize = 64;

/// The variable each harness exports to the commands it runs, paired with
/// the harness name. Each row is verified in the harness's own documentation
/// or source. Agents come before IDE terminals so an agent that runs inside an
/// IDE wins.
const AGENT_MARKERS: &[(&str, &str)] = &[
    ("CLAUDECODE", "claude-code"),
    ("CODEX_THREAD_ID", "codex"),
    ("CODEX_SANDBOX", "codex"),
    ("GEMINI_CLI", "gemini"),
    ("OPENCODE", "opencode"),
    ("PI_CODING_AGENT", "pi"),
    ("AUGMENT_AGENT", "auggie"),
    ("GOOSE_TERMINAL", "goose"),
    ("AGENT_CONTEXT_OUT", "kiro"),
    ("JUNIE_DATA", "junie"),
    ("JUNIE_SHIM_PATH", "junie"),
    ("CURSOR_AGENT", "cursor"),
    ("COPILOT_AGENT", "copilot"),
    ("CLINE_ACTIVE", "cline"),
];

/// Identify the AI agent harness that runs snouty, if any. `AI_AGENT` wins;
/// Claude Code sets it to a value such as `claude-code_2-1-267_agent`, and Pi
/// sets it to `pi`. `env` replaces [`env::var`] so a test does not change the
/// process environment.
fn agent_hint(env: impl Fn(&str) -> Option<String>) -> Option<String> {
    env("AI_AGENT")
        .and_then(|raw| sanitize_agent_hint(&raw))
        .or_else(|| {
            AGENT_MARKERS
                .iter()
                .find(|(var, _)| env(var).is_some())
                .map(|(_, name)| name.to_string())
        })
}

/// The User-Agent comment grammar reserves `;`, `(` and `)`, so only
/// `[A-Za-z0-9._/-]` survives, and the result is capped at
/// [`AGENT_HINT_MAX_LEN`] so it cannot bloat the request.
fn sanitize_agent_hint(raw: &str) -> Option<String> {
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
    fn agent_hint_prefers_ai_agent_and_strips_unsafe_chars() {
        let env = env_of(&[
            ("AI_AGENT", "claude-code_2-1-267_agent"),
            ("CLAUDECODE", "1"),
        ]);
        assert_eq!(
            agent_hint(env).as_deref(),
            Some("claude-code_2-1-267_agent")
        );

        let env = env_of(&[("AI_AGENT", "bad agent;\r\n(1.0)")]);
        assert_eq!(agent_hint(env).as_deref(), Some("badagent1.0"));
    }

    #[test]
    fn agent_hint_falls_back_to_harness_markers() {
        assert_eq!(
            agent_hint(env_of(&[("CLAUDECODE", "1")])).as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            agent_hint(env_of(&[("CODEX_THREAD_ID", "abc")])).as_deref(),
            Some("codex")
        );
        assert_eq!(agent_hint(env_of(&[("AI_AGENT", "  ")])), None);
        assert_eq!(
            agent_hint(env_of(&[("AI_AGENT", "()"), ("CLAUDECODE", "1")])).as_deref(),
            Some("claude-code")
        );
        assert_eq!(agent_hint(env_of(&[])), None);
    }

    #[test]
    fn agent_hint_is_capped() {
        let long = "x".repeat(500);
        let hint = agent_hint(env_of(&[("AI_AGENT", &long)])).unwrap();
        assert_eq!(hint.len(), AGENT_HINT_MAX_LEN);
    }

    /// Whatever the environment holds, the hint that reaches the wire fits
    /// the User-Agent comment grammar and the length cap, and a hint that
    /// already passed is left alone.
    #[hegel::test]
    fn sanitize_agent_hint_is_safe_and_idempotent(tc: hegel::TestCase) {
        let raw = tc.draw(hegel::generators::text());
        let Some(once) = sanitize_agent_hint(&raw) else {
            return;
        };
        assert!(once.len() <= AGENT_HINT_MAX_LEN);
        assert!(
            once.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        );
        assert_eq!(sanitize_agent_hint(&once).as_deref(), Some(once.as_str()));
    }
}
