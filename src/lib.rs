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
    let base = format!(
        "snouty/{} ({}; {}; rust{}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    );
    match agent_hint(|name: &str| std::env::var_os(name)) {
        Some(agent) => format!("{base}; agent={agent})"),
        None => format!("{base})"),
    }
}

/// Longest agent hint we put on the wire; longer values are truncated.
const AGENT_HINT_MAX_LEN: usize = 64;

/// Identify the AI agent harness running snouty, if any, from its environment.
///
/// `AI_AGENT` is the cross-harness convention (Claude Code sets it to e.g.
/// `claude-code_2-1-267_agent`, name and version included) and is passed
/// through as-is. Without it we fall back to a bare harness name from the
/// harness-specific marker variables. The value is restricted to
/// `[A-Za-z0-9._/-]` and capped at [`AGENT_HINT_MAX_LEN`] so it is always a
/// valid header value and cannot bloat the request.
fn agent_hint(env: impl Fn(&str) -> Option<std::ffi::OsString>) -> Option<String> {
    let raw = env("AI_AGENT")
        .map(|v| v.to_string_lossy().into_owned())
        .filter(|v| !v.trim().is_empty())
        .or_else(|| env("CLAUDECODE").map(|_| "claude-code".to_string()))
        .or_else(|| {
            env("CODEX_SANDBOX")
                .or_else(|| env("CODEX_THREAD_ID"))
                .map(|_| "codex".to_string())
        })?;
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
    use std::ffi::OsString;

    fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, OsString> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), OsString::from(v)))
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
        assert_eq!(agent_hint(env_of(&[])), None);
    }

    #[test]
    fn agent_hint_is_capped() {
        let long = "x".repeat(500);
        let hint = agent_hint(env_of(&[("AI_AGENT", &long)])).unwrap();
        assert_eq!(hint.len(), AGENT_HINT_MAX_LEN);
    }
}
