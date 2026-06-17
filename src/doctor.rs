use color_eyre::eyre::Result;

use crate::{api::Auth, container, error::user_error, settings::Settings};

struct Check {
    name: &'static str,
    passed: bool,
    message: String,
}

impl Check {
    /// A pass/fail check whose message is the resolved value (or the error).
    fn for_value(name: &'static str, result: Result<&str>) -> Self {
        Self {
            name,
            passed: result.is_ok(),
            message: match result {
                Ok(value) => value.to_owned(),
                Err(err) => format!("error: {err}"),
            },
        }
    }

    /// An informational row that always passes; `message` is the resolved value.
    fn info(name: &'static str, message: String) -> Self {
        Self {
            name,
            passed: true,
            message,
        }
    }

    fn print(&self) {
        let icon = if self.passed {
            console::style("✓").green()
        } else {
            console::style("✗").red()
        };
        eprintln!("  {} {}: {}", icon, self.name, self.message);
    }
}

pub fn cmd_doctor(settings: &Settings) -> Result<()> {
    let mut checks: Vec<Check> = Vec::new();

    // Container runtime (for building/pushing images)
    match container::runtime(settings) {
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

    // Resolved settings. tenant and repository are required, so a miss is a
    // failed check; base url and container engine are informational. These rows
    // print the fully-resolved values (env > profile > settings file).
    checks.push(Check::for_value("tenant", settings.tenant()));
    checks.push(Check::for_value("repository", settings.repository()));
    checks.push(Check::info(
        "base url",
        settings
            .resolve_base_url()
            .unwrap_or_else(|_| "(unset: provide tenant or base_url)".to_owned()),
    ));
    checks.push(Check::info(
        "container engine",
        settings
            .container_engine()
            .map(str::to_owned)
            .unwrap_or_else(|| "(auto-detected)".to_owned()),
    ));

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
