//! The User-Agent header snouty sends.

use crate::{agent, env};

const AGENT_FIELD_MAX_LEN: usize = 64;

/// User-Agent string sent with every HTTP request snouty makes.
///
/// When an AI agent harness runs snouty, the string ends with an
/// `agent=<name>[/<version>]` field.
pub fn user_agent() -> String {
    let agent = agent::detect(&|name| env::var(name).ok().flatten()).and_then(|agent| {
        let field = match agent.version {
            Some(version) => format!("{}/{version}", agent.name),
            None => agent.name,
        };
        sanitize_agent_field(&field)
    });
    user_agent_with(agent.as_deref())
}

/// The User-Agent string with `agent` as the trailing `agent=` field. `None`
/// omits the field.
pub fn user_agent_with(agent: Option<&str>) -> String {
    let agent = agent.map_or_default(|a| format!("; agent={a}"));
    format!(
        "snouty/{} ({}; {}; rust{}{agent})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

/// The User-Agent comment grammar reserves `;`, `(` and `)`, so only
/// `[A-Za-z0-9._/-]` survives, and the result is capped at
/// [`AGENT_FIELD_MAX_LEN`] so it cannot bloat the request.
fn sanitize_agent_field(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        .take(AGENT_FIELD_MAX_LEN)
        .collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_agent_field_strips_unsafe_chars_and_caps() {
        assert_eq!(
            sanitize_agent_field("bad agent;\r\n(1.0)").as_deref(),
            Some("badagent1.0")
        );
        assert_eq!(sanitize_agent_field("()"), None);
        assert_eq!(
            sanitize_agent_field(&"x".repeat(500)).unwrap().len(),
            AGENT_FIELD_MAX_LEN
        );
    }

    /// Whatever the environment holds, the field that reaches the wire fits
    /// the User-Agent comment grammar and the length cap, and a field that
    /// already passed is left alone.
    #[hegel::test]
    fn sanitize_agent_field_is_safe_and_idempotent(tc: hegel::TestCase) {
        let raw = tc.draw(hegel::generators::text());
        let Some(once) = sanitize_agent_field(&raw) else {
            return;
        };
        assert!(once.len() <= AGENT_FIELD_MAX_LEN);
        assert!(
            once.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        );
        assert_eq!(sanitize_agent_field(&once).as_deref(), Some(once.as_str()));
    }
}
