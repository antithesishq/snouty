use color_eyre::eyre::Result;

use crate::{
    api::Auth,
    container,
    error::user_error,
    snouty_config::{self, SnoutyConfig},
};

struct Check {
    name: &'static str,
    passed: bool,
    message: String,
}

impl Check {
    fn for_result<T>(name: &'static str, result: Result<T>) -> Self {
        Self {
            name,
            passed: result.is_ok(),
            message: match result {
                Ok(_) => "found".to_owned(),
                Err(err) => format!("error: {err}"),
            },
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
    let config = snouty_config::default_config(None);

    // Container runtime (for building/pushing images)
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

    // Docker Compose v2 (required for compose configs)
    match container::docker_compose_version() {
        Ok(version) => checks.push(Check {
            name: "docker-compose",
            passed: true,
            message: version,
        }),
        Err(e) => checks.push(Check {
            name: "docker-compose",
            passed: false,
            message: e.to_string(),
        }),
    }

    // required configuration
    checks.push(Check::for_result("tenant", config.tenant()));
    checks.push(Check::for_result("repository", config.repository()));

    // Auth: api key OR both username and password
    let auth = Auth::from_env();
    checks.push(Check {
        name: "auth",
        passed: auth.is_ok(),
        message: match auth {
            Ok(Auth::Bearer { api_key: _ }) => "API key configured".to_owned(),
            Ok(_) => "Basic authentication configured. This is only valid for the `launch` command"
                .to_owned(),
            Err(err) => format!("error: {err}"),
        },
    });

    // Print all checks and check for failures
    for check in &checks {
        check.print();
    }

    eprintln!();
    if checks.iter().any(|c| !c.passed) {
        return Err(user_error("doctor found problems"));
    }
    eprintln!("All checks passed");

    Ok(())
}
