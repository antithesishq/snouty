use color_eyre::eyre::Result;

use crate::{
    api::Auth,
    container,
    error::user_error,
    settings::{AttributedValue, Settings, SharedReport, ValueSource},
};

struct Check {
    name: &'static str,
    passed: CheckResult,
    message: String,
}

#[derive(PartialEq, Eq)]
enum CheckResult {
    Pass,
    Fail,
    Inconclusive,
}

fn to_check_string(source: &ValueSource) -> &str {
    match source {
        ValueSource::EnvironmentVariable => "environment variable",
        ValueSource::ProjectProfile => "settings profile in project configuration file",
        ValueSource::GlobalProfile => "settings profile in global configuration file",
        ValueSource::ProjectDefault => "default setting in project configuration file",
        ValueSource::GlobalDefault => "default setting in global configuration file",
    }
}

impl Check {
    /// A pass/fail check whose message is the resolved value (or the error).
    fn for_value(name: &'static str, result: Result<&str>) -> Self {
        Self {
            name,
            passed: if result.is_ok() {
                CheckResult::Pass
            } else {
                CheckResult::Fail
            },
            message: match result {
                Ok(value) => value.to_owned(),
                Err(err) => format!("error: {err}"),
            },
        }
    }

    fn for_setting(
        name: &'static str,
        result: &Result<Option<AttributedValue<&String>>, SharedReport>,
        required: bool,
    ) -> Self {
        match result {
            Ok(None) => Self {
                name,
                passed: if required {
                    CheckResult::Fail
                } else {
                    CheckResult::Inconclusive
                },
                message: "<Not found>".to_owned(),
            },
            Ok(Some(attributed_value)) => Self {
                name,
                passed: CheckResult::Pass,
                message: format!(
                    "{} (from {})",
                    attributed_value.value,
                    to_check_string(&attributed_value.attribution)
                ),
            },
            Err(err) => Self {
                name,
                passed: CheckResult::Fail,
                message: format!("error: {err}"),
            },
        }
    }

    fn print(&self) {
        let icon = match self.passed {
            CheckResult::Pass => console::style("✓").green(),
            CheckResult::Inconclusive => console::style("~").yellow(),
            CheckResult::Fail => console::style("✗").red(),
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
            passed: CheckResult::Pass,
            message: format!("{} detected", rt.name()),
        }),
        Err(e) => checks.push(Check {
            name: "Container runtime",
            passed: CheckResult::Fail,
            message: e.to_string(),
        }),
    }

    // Docker Compose v2 (required for compose configs)
    match container::docker_compose_version() {
        Ok(version) => checks.push(Check {
            name: "docker-compose",
            passed: CheckResult::Pass,
            message: version,
        }),
        Err(e) => checks.push(Check {
            name: "docker-compose",
            passed: CheckResult::Fail,
            message: e.to_string(),
        }),
    }

    match settings.load_project_settings() {
        Ok(None) => (),
        Ok(Some(project_settings)) => checks.push(Check {
            name: "project config file",
            passed: CheckResult::Pass,
            message: format!(
                "found project configuration at {}",
                project_settings.resolved_path.to_str().unwrap()
            ),
        }),
        Err(err) => checks.push(Check {
            name: "project config file",
            passed: CheckResult::Fail,
            message: format!("error: {err}"),
        }),
    }

    match settings.load_global_settings() {
        Ok(None) => (),
        Ok(Some(project_settings)) => checks.push(Check {
            name: "global config file",
            passed: CheckResult::Pass,
            message: format!(
                "found project configuration at {}",
                project_settings.resolved_path.to_str().unwrap()
            ),
        }),
        Err(err) => checks.push(Check {
            name: "global config file",
            passed: CheckResult::Fail,
            message: format!("error: {err}"),
        }),
    }

    checks.push(Check {
        name: "settings profile",
        passed: CheckResult::Pass,
        message: match settings.settings_profile() {
            None => "Using default settings (no profile set)".to_owned(),
            Some(profile_name) => format!("Using profile [{profile_name}]"),
        },
    });

    // Resolved settings. tenant and repository are required, so a miss is a
    // failed check; base url and container engine are informational. These rows
    // print the fully-resolved values (env > profile > settings file).
    let tenant_setting = settings.try_resolve_tenant();
    let base_url_setting = settings.try_resolve_base_url();
    checks.push(Check::for_setting(
        "tenant",
        &tenant_setting,
        !base_url_setting.as_ref().is_ok_and(|o| o.is_some()),
    ));
    checks.push(Check::for_setting(
        "repository",
        &settings.try_resolve_repository(),
        true,
    ));
    checks.push(Check::for_setting(
        "base_url",
        &base_url_setting,
        !tenant_setting.as_ref().is_ok_and(|o| o.is_some()),
    ));
    checks.push(Check::for_value("resolved base url", settings.base_url()));
    checks.push(Check::for_setting(
        "container engine",
        &settings.try_resolve_container_engine(),
        false,
    ));

    // Auth: api key OR both username and password
    let auth = Auth::from_env();
    checks.push(Check {
        name: "auth",
        passed: if auth.is_ok() {
            CheckResult::Pass
        } else {
            CheckResult::Fail
        },
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
    if checks.iter().any(|c| c.passed == CheckResult::Fail) {
        return Err(user_error("doctor found problems"));
    }
    eprintln!("All checks passed");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use color_eyre::eyre::eyre;

    #[test]
    fn check_strings_cover_every_value_source() {
        assert_eq!(
            to_check_string(&ValueSource::EnvironmentVariable),
            "environment variable"
        );
        assert_eq!(
            to_check_string(&ValueSource::ProjectProfile),
            "settings profile in project configuration file"
        );
        assert_eq!(
            to_check_string(&ValueSource::GlobalProfile),
            "settings profile in global configuration file"
        );
        assert_eq!(
            to_check_string(&ValueSource::ProjectDefault),
            "default setting in project configuration file"
        );
        assert_eq!(
            to_check_string(&ValueSource::GlobalDefault),
            "default setting in global configuration file"
        );
    }

    #[test]
    fn for_value_ok_passes_with_value_as_message() {
        let check = Check::for_value("base url", Ok("https://acme.antithesis.com"));
        assert!(check.passed == CheckResult::Pass);
        assert_eq!(check.message, "https://acme.antithesis.com");
    }

    #[test]
    fn for_value_err_fails_with_prefixed_message() {
        let check = Check::for_value("base url", Err(eyre!("boom")));
        assert!(check.passed == CheckResult::Fail);
        assert_eq!(check.message, "error: boom");
    }

    #[test]
    fn for_setting_some_passes_with_attribution() {
        let value = "acme".to_string();
        let result = Ok(Some(AttributedValue {
            value: &value,
            attribution: ValueSource::ProjectProfile,
        }));
        let check = Check::for_setting("tenant", &result, true);
        assert!(check.passed == CheckResult::Pass);
        assert_eq!(
            check.message,
            "acme (from settings profile in project configuration file)"
        );
    }

    #[test]
    fn for_setting_missing_required_fails() {
        let result = Ok(None);
        let check = Check::for_setting("tenant", &result, true);
        assert!(check.passed == CheckResult::Fail);
        assert_eq!(check.message, "<Not found>");
    }

    #[test]
    fn for_setting_missing_optional_is_inconclusive() {
        let result = Ok(None);
        let check = Check::for_setting("container engine", &result, false);
        assert!(check.passed == CheckResult::Inconclusive);
        assert_eq!(check.message, "<Not found>");
    }

    #[test]
    fn for_setting_error_fails_with_prefixed_message() {
        let result = Err(SharedReport::for_test("could not read config"));
        let check = Check::for_setting("tenant", &result, true);
        assert!(check.passed == CheckResult::Fail);
        assert_eq!(check.message, "error: could not read config");
    }
}
