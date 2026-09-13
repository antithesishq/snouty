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

use std::io::IsTerminal;

/// User-Agent string sent with every HTTP request snouty makes.
///
/// When an AI agent harness runs snouty, the string ends with an
/// `agent=<value>` field that names the harness.
pub fn user_agent() -> String {
    let stdout_is_tty = std::io::stdout().is_terminal();
    user_agent_with(agent_hint(|name| env::var(name).ok().flatten(), stdout_is_tty).as_deref())
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

/// One marker per harness: the variable it exports to the commands it runs,
/// the text that variable must contain (empty for any value), and the harness
/// name. The table follows `unjs/std-env`, whose rows the harness maintainers
/// verified. Agents come before IDEs so an agent that runs inside an IDE wins.
const AGENT_MARKERS: &[(&str, &str, &str)] = &[
    ("CLAUDECODE", "", "claude-code"),
    ("CLAUDE_CODE", "", "claude-code"),
    ("CODEX_SANDBOX", "", "codex"),
    ("CODEX_THREAD_ID", "", "codex"),
    ("GEMINI_CLI", "", "gemini"),
    ("OPENCODE", "", "opencode"),
    ("REPL_ID", "", "replit"),
    ("PATH", ".pi/agent", "pi"),
    ("PATH", ".pi\\agent", "pi"),
    ("AUGMENT_AGENT", "", "auggie"),
    ("GOOSE_PROVIDER", "", "goose"),
    ("JUNIE_DATA", "", "junie"),
    ("JUNIE_SHIM_PATH", "", "junie"),
    ("EDITOR", "devin", "devin"),
    ("CURSOR_AGENT", "", "cursor"),
    ("TERM_PROGRAM", "kiro", "kiro"),
];

/// Identify the AI agent harness that runs snouty, if any. `AI_AGENT` wins;
/// Claude Code sets it to a value such as `claude-code_2-1-267_agent`. `env`
/// replaces [`env::var`] so a test does not change the process environment.
/// The Kiro IDE terminal sets the same `TERM_PROGRAM` as the Kiro agent, so
/// that row only counts when `stdout_is_tty` is false.
fn agent_hint(env: impl Fn(&str) -> Option<String>, stdout_is_tty: bool) -> Option<String> {
    env("AI_AGENT")
        .and_then(|raw| sanitize_agent_hint(&raw))
        .or_else(|| {
            AGENT_MARKERS
                .iter()
                .filter(|(_, _, name)| *name != "kiro" || !stdout_is_tty)
                .find(|(var, needle, _)| env(var).is_some_and(|v| v.contains(needle)))
                .map(|(_, _, name)| name.to_string())
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
            agent_hint(env, false).as_deref(),
            Some("claude-code_2-1-267_agent")
        );

        let env = env_of(&[("AI_AGENT", "bad agent;\r\n(1.0)")]);
        assert_eq!(agent_hint(env, false).as_deref(), Some("badagent1.0"));
    }

    #[test]
    fn agent_hint_falls_back_to_harness_markers() {
        assert_eq!(
            agent_hint(env_of(&[("CLAUDECODE", "1")]), false).as_deref(),
            Some("claude-code")
        );
        assert_eq!(
            agent_hint(env_of(&[("CODEX_THREAD_ID", "abc")]), false).as_deref(),
            Some("codex")
        );
        assert_eq!(agent_hint(env_of(&[("AI_AGENT", "  ")]), false), None);
        assert_eq!(
            agent_hint(env_of(&[("AI_AGENT", "()"), ("CLAUDECODE", "1")]), false).as_deref(),
            Some("claude-code")
        );
        assert_eq!(agent_hint(env_of(&[]), false), None);
    }

    #[test]
    fn agent_hint_matches_substrings_and_gates_kiro_on_tty() {
        assert_eq!(
            agent_hint(env_of(&[("PATH", "/usr/bin:/home/u/.pi/agent/bin")]), false).as_deref(),
            Some("pi")
        );
        assert_eq!(
            agent_hint(env_of(&[("EDITOR", "/opt/devin/editor")]), false).as_deref(),
            Some("devin")
        );
        assert_eq!(
            agent_hint(env_of(&[("TERM_PROGRAM", "kiro")]), false).as_deref(),
            Some("kiro")
        );
        assert_eq!(agent_hint(env_of(&[("TERM_PROGRAM", "kiro")]), true), None);
        assert_eq!(
            agent_hint(env_of(&[("TERM_PROGRAM", "kiro"), ("OPENCODE", "1")]), true).as_deref(),
            Some("opencode")
        );
    }

    #[test]
    fn agent_hint_is_capped() {
        let long = "x".repeat(500);
        let hint = agent_hint(env_of(&[("AI_AGENT", &long)]), false).unwrap();
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
