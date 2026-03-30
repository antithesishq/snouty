use std::env;

use color_eyre::eyre::{Result, bail};

use crate::container;

enum Status {
    Pass,
    Fail,
}

struct Check {
    name: String,
    status: Status,
    message: String,
}

impl Check {
    fn pass(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Pass,
            message: message.into(),
        }
    }

    fn fail(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: Status::Fail,
            message: message.into(),
        }
    }
}

fn check_env(name: &'static str) -> Check {
    match env::var(name) {
        Ok(_) => Check::pass(name, "is set"),
        Err(_) => Check::fail(name, "is not set"),
    }
}

fn check_container_runtime() -> Check {
    match container::runtime() {
        Ok(rt) => Check::pass("Container runtime", format!("{} detected", rt.name())),
        Err(e) => Check::fail("Container runtime", e.to_string()),
    }
}

fn check_env_vars() -> Vec<Check> {
    let mut checks = Vec::new();

    checks.push(check_env("ANTITHESIS_TENANT"));

    match env::var("ANTITHESIS_API_KEY") {
        Ok(_) => {
            checks.push(Check::pass("ANTITHESIS_API_KEY", "is set"));
        }
        Err(_) => {
            let has_username = env::var("ANTITHESIS_USERNAME").is_ok();
            let has_password = env::var("ANTITHESIS_PASSWORD").is_ok();
            if has_username && has_password {
                checks.push(Check::pass(
                    "ANTITHESIS_USERNAME / ANTITHESIS_PASSWORD",
                    "are set",
                ));
            } else {
                checks.push(Check::fail(
                    "Authentication",
                    "neither ANTITHESIS_API_KEY nor ANTITHESIS_USERNAME + ANTITHESIS_PASSWORD are set",
                ));
            }
        }
    }

    checks.push(check_env("ANTITHESIS_REPOSITORY"));

    checks
}

fn count_section(checks: &[Check]) -> (usize, usize) {
    let mut passes = 0;
    let mut failures = 0;
    for check in checks {
        match check.status {
            Status::Pass => passes += 1,
            Status::Fail => failures += 1,
        }
    }
    (passes, failures)
}

fn print_section(checks: &[Check]) {
    for check in checks {
        let (icon, style_fn): (&str, fn(&str) -> String) = match check.status {
            Status::Pass => ("✓", |s| console::style(s).green().to_string()),
            Status::Fail => ("✗", |s| console::style(s).red().to_string()),
        };
        eprintln!("  {} {} {}", style_fn(icon), check.name, check.message);
    }
}

pub fn cmd_doctor() -> Result<()> {
    let mut passes = 0;
    let mut failures = 0;

    eprintln!("Container runtime");
    let runtime_checks = [check_container_runtime()];
    print_section(&runtime_checks);
    let (p, f) = count_section(&runtime_checks);
    passes += p;
    failures += f;

    eprintln!("Environment variables");
    let env_checks = check_env_vars();
    print_section(&env_checks);
    let (p, f) = count_section(&env_checks);
    passes += p;
    failures += f;

    eprintln!();
    if failures > 0 {
        eprintln!(
            "{} passed, {} failed",
            console::style(passes).green(),
            console::style(failures).red(),
        );
        bail!("environment check failed");
    } else {
        eprintln!("All {} checks passed", console::style(passes).green());
    }

    Ok(())
}
