use std::env;

use color_eyre::eyre::{Result, bail};

use crate::container;

struct Check {
    name: &'static str,
    passed: bool,
    message: String,
}

impl Check {
    fn env(name: &'static str) -> Self {
        let set = env::var(name).is_ok_and(|v| !v.is_empty());
        Self {
            name,
            passed: set,
            message: if set { "is set" } else { "is not set" }.into(),
        }
    }

    fn print(&self) {
        let icon = if self.passed {
            console::style("✓").green()
        } else {
            console::style("✗").red()
        };
        eprintln!("  {} {} {}", icon, self.name, self.message);
    }
}

pub fn cmd_doctor() -> Result<()> {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime
    match container::runtime() {
        Ok(rt) => checks.push(Check {
            name: "Container runtime",
            passed: true,
            message: format!("{} detected", rt.name()),
        }),
        Err(e) => checks.push(Check {
            name: "Container runtime",
            passed: false,
            message: e.to_string(),
        }),
    }

    // Environment variables
    checks.push(Check::env("ANTITHESIS_TENANT"));
    checks.push(Check::env("ANTITHESIS_REPOSITORY"));

    // Auth: api key OR both username and password
    let api_key = Check::env("ANTITHESIS_API_KEY");
    if api_key.passed {
        checks.push(api_key);
    } else {
        checks.push(Check::env("ANTITHESIS_USERNAME"));
        checks.push(Check::env("ANTITHESIS_PASSWORD"));
    }

    // Print all checks and check for failures
    for check in &checks {
        check.print();
    }

    eprintln!();
    if checks.iter().any(|c| !c.passed) {
        bail!("doctor found problems");
    }
    eprintln!("All checks passed");

    Ok(())
}
