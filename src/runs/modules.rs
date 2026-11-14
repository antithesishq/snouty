//! `runs modules`: the modules that a run loaded, and where.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io::Write;

use color_eyre::eyre::{Report, Result};
use futures_util::TryStreamExt;
use serde::Serialize;

use crate::OutputOptions;
use crate::api::{AntithesisApi, CoverageModule, CoverageModuleInstance, RunStatus};
use crate::render::sanitize;
use crate::settings::Settings;
use crate::table::{Align, Column, Fit, Table};

use super::{count, run_status, terminal_width};

/// How many instance lists `runs modules` reads at the same time.
const INSTANCE_REQUESTS: usize = 8;

/// A module and the programs that loaded it: one `runs modules --json` line.
#[derive(Serialize)]
struct ModuleInstances {
    #[serde(flatten)]
    module: CoverageModule,
    instances: Vec<CoverageModuleInstance>,
}

pub(super) async fn cmd_runs_modules(
    run_id: &str,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let api = AntithesisApi::new(settings, verbose)?;
    let modules = api
        .stream_run_modules(run_id)
        .map_ok(|module| {
            let api = &api;
            async move {
                let instances = api
                    .stream_module_instances(run_id, &module.module_id)
                    .try_collect()
                    .await?;
                Ok::<_, Report>(ModuleInstances { module, instances })
            }
        })
        .try_buffered(INSTANCE_REQUESTS);
    let mut modules = std::pin::pin!(modules);

    if json {
        while let Some(module) = modules.try_next().await? {
            outln!("{}", serde_json::to_string(&module)?)?;
        }
        return Ok(());
    }

    // Read the status concurrently: a status older than the listing still
    // makes "more modules may load" true.
    let (modules, run): (Vec<ModuleInstances>, _) =
        futures_util::try_join!(modules.try_collect(), api.get_run(run_id))?;
    if !modules.is_empty() {
        outln!("{}", render_modules_table(&modules, terminal_width()))?;
    }
    eprintln!("{}", modules_status_line(&modules, &run.untag().status));
    Ok(())
}

/// One row for each program that loaded a module, sorted by module, container
/// and program. A module that no program has loaded yet gets one row with
/// blank CONTAINER and PROGRAM.
fn render_modules_table(modules: &[ModuleInstances], width: usize) -> String {
    let mut pairs: Vec<(&CoverageModule, &str, &str)> = Vec::new();
    for ModuleInstances { module, instances } in modules {
        if instances.is_empty() {
            pairs.push((module, "", ""));
        }
        for instance in instances {
            pairs.push((module, &instance.container_name, &instance.program_path));
        }
    }
    pairs.sort_by_key(|&(module, container, program)| (&module.module_id, container, program));
    let rows = pairs.into_iter().map(|(module, container, program)| {
        vec![
            sanitize(&module.module_id),
            sanitize(&module.name),
            sanitize(program),
            sanitize(container),
            module.language.as_deref().map_or_default(sanitize),
            module.num_locations.to_string(),
            if module.has_catalog { "yes" } else { "no" }.to_string(),
        ]
    });
    Table::builder()
        .column(Column::new("MODULE"))
        .column(
            Column::new("NAME")
                .fit(Fit::Truncate { floor: 12 })
                .priority(1),
        )
        .column(Column::new("PROGRAM").fit(Fit::TruncatePath { floor: 19 }))
        .column(
            Column::new("CONTAINER")
                .fit(Fit::Truncate { floor: 10 })
                .priority(1),
        )
        .column(Column::new("LANG"))
        .column(Column::new("LOCATIONS").align(Align::Right))
        .column(Column::new("SYMBOLS"))
        .render(rows.collect(), width)
}

/// `7 modules in 6 containers · run in progress (more modules may load)`.
fn modules_status_line(modules: &[ModuleInstances], status: &RunStatus) -> String {
    let containers: BTreeSet<&str> = modules
        .iter()
        .flat_map(|m| &m.instances)
        .map(|instance| instance.container_name.as_str())
        .collect();
    let mut line = count(modules.len(), "module");
    if !modules.is_empty() {
        let _ = write!(line, " in {}", count(containers.len(), "container"));
    }
    let _ = write!(line, " · {}", run_status(status));
    if !status.is_terminal() {
        line.push_str(" (more modules may load)");
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module(id: &str, name: &str, language: Option<&str>, has_catalog: bool) -> CoverageModule {
        serde_json::from_value(serde_json::json!({
            "module_id": id,
            "name": name,
            "has_catalog": has_catalog,
            "language": language,
            "num_locations": 9,
        }))
        .unwrap()
    }

    fn instance(container: &str, program: &str) -> CoverageModuleInstance {
        serde_json::from_value(serde_json::json!({
            "instance_id": format!("{container}{program}"),
            "container_name": container,
            "program_path": program,
        }))
        .unwrap()
    }

    fn modules() -> Vec<ModuleInstances> {
        vec![
            ModuleInstances {
                module: module(
                    "go-20e55e2f1623",
                    "instrhello/internal/work",
                    Some("Go"),
                    true,
                ),
                instances: vec![
                    instance("goworker", "/usr/local/bin/worker"),
                    instance("driver", "/usr/local/bin/driver"),
                ],
            },
            ModuleInstances {
                module: module("dotnet-e798e24c32e9", "dotnethello", None, false),
                instances: vec![instance("dotnet", "/usr/share/dotnet/dotnet")],
            },
            ModuleInstances {
                module: module("go-cd4dd5ec3daf", "instrhello/cmd/worker", Some("Go"), true),
                instances: vec![],
            },
        ]
    }

    #[test]
    fn modules_table_has_a_row_per_program_sorted_by_module() {
        insta::assert_snapshot!(render_modules_table(&modules(), usize::MAX), @r"
        MODULE               NAME                      PROGRAM                   CONTAINER  LANG  LOCATIONS  SYMBOLS
        dotnet-e798e24c32e9  dotnethello               /usr/share/dotnet/dotnet  dotnet                   9  no
        go-20e55e2f1623      instrhello/internal/work  /usr/local/bin/driver     driver     Go            9  yes
        go-20e55e2f1623      instrhello/internal/work  /usr/local/bin/worker     goworker   Go            9  yes
        go-cd4dd5ec3daf      instrhello/cmd/worker                                          Go            9  yes
        ");
    }

    #[test]
    fn modules_table_shortens_the_program_from_the_left() {
        let table = render_modules_table(&modules(), 1);
        assert!(table.contains("…/dotnet/dotnet"), "{table}");
        assert!(table.contains("go-20e55e2f1623 "), "{table}");
    }

    #[test]
    fn modules_status_line_counts_modules_and_containers() {
        assert_eq!(
            modules_status_line(&modules(), &RunStatus::InProgress),
            "3 modules in 3 containers · run in progress (more modules may load)"
        );
        assert_eq!(
            modules_status_line(&[], &RunStatus::InProgress),
            "0 modules · run in progress (more modules may load)"
        );
        assert_eq!(
            modules_status_line(&modules()[1..2], &RunStatus::Completed),
            "1 module in 1 container · run completed"
        );
    }
}
