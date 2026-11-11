//! The User-Agent header snouty sends.

use crate::{agent, env};

/// User-Agent string sent with every HTTP request snouty makes.
///
/// When an AI agent harness runs snouty, the string ends with an
/// `agent=<value>` field that names the harness.
pub fn user_agent() -> String {
    user_agent_with(agent::hint(|name| env::var(name).ok().flatten()).as_deref())
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
