mod api_cache;
mod attributed_value;

pub mod api;
pub mod auth;
pub mod browser;
pub mod cli;
pub mod compose;
pub mod config;
pub mod container;
pub mod docs;
pub mod doctor;
pub(crate) mod env;
pub mod error;
pub mod event_render;
pub mod event_set_dsl;
pub mod features;
pub mod jsonl;
pub mod login;
pub mod params;
pub mod process;
pub(crate) mod render;
pub use render::{OutputOptions, wrap_if_tty};
pub mod runs;
pub mod scripts;
#[doc(hidden)]
pub mod settings;
pub mod tag;
#[doc(hidden)]
pub mod testutils;
pub mod time;
pub mod util;
pub mod validate;
pub mod vtime;

/// User-Agent string sent with every HTTP request snouty makes.
///
/// When an AI agent harness runs snouty, the harness name (and version, when
/// the harness exposes one) is appended as a trailing `agent=<value>` field.
pub fn user_agent() -> String {
    user_agent_with(agent_hint(|name| env::var(name).ok().flatten()).as_deref())
}

/// The User-Agent string for a given agent hint. Factored out of the
/// environment read so a test can compute the exact string it expects.
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

/// Longest agent hint we put on the wire; longer values are truncated.
const AGENT_HINT_MAX_LEN: usize = 64;

/// Harness-specific marker variables and the bare harness name each implies.
/// Consulted in order when `AI_AGENT` is unset.
const AGENT_MARKERS: &[(&str, &str)] = &[
    ("CLAUDECODE", "claude-code"),
    ("CODEX_SANDBOX", "codex"),
    ("CODEX_THREAD_ID", "codex"),
];

/// Identify the AI agent harness running snouty, if any, from its environment.
///
/// `AI_AGENT` is the cross-harness convention (Claude Code sets it to e.g.
/// `claude-code_2-1-267_agent`, name and version included) and is passed
/// through as-is. Without a usable value we fall back to [`AGENT_MARKERS`].
/// The User-Agent comment grammar reserves `;`, `(` and `)`, so the value is
/// restricted to `[A-Za-z0-9._/-]`, and it is capped so it cannot bloat the
/// request. `env` stands in for [`env::var`] so the selection can be tested
/// without mutating process-global state.
fn agent_hint(env: impl Fn(&str) -> Option<String>) -> Option<String> {
    env("AI_AGENT")
        .and_then(|raw| sanitize_agent_hint(&raw))
        .or_else(|| {
            AGENT_MARKERS
                .iter()
                .find(|(marker, _)| env(marker).is_some())
                .map(|(_, name)| name.to_string())
        })
}

fn sanitize_agent_hint(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        .take(AGENT_HINT_MAX_LEN)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

#[cfg(test)]
mod user_agent_tests {
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
}
