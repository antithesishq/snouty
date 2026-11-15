//! `runs modules`: the modules that a run loaded, and, with `--detail`,
//! where.

use std::io::Write;

use color_eyre::eyre::{Report, Result};
use futures_util::TryStreamExt;
use serde::Serialize;

use crate::OutputOptions;
use crate::api::{self, AntithesisApi, CoverageModule, CoverageModuleInstance, RunStatus};
use crate::render::{count, sanitize};
use crate::settings::Settings;
use crate::table::{Align, Column, Fit, Table};

use super::terminal_width;

/// How many instance lists `runs modules --detail` reads at the same time.
const INSTANCE_REQUESTS: usize = 8;

/// A module and, with `--detail`, the programs that loaded it: one
/// `runs modules --json` line.
#[derive(Serialize)]
struct ModuleInstances {
    #[serde(flatten)]
    module: CoverageModule,
    #[serde(skip_serializing_if = "Option::is_none")]
    instances: Option<Vec<CoverageModuleInstance>>,
}

pub(super) async fn cmd_runs_modules(
    run_id: &str,
    detail: bool,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    api::ensure_module_list_supported(run_id)?;
    let api = AntithesisApi::new(settings, verbose)?;
    // The status says whether more modules may load, and whether the pages
    // can be cached for good. A run ID that names no run fails here.
    let status = api.get_run(run_id).await?.untag().status;
    let finished = status.is_terminal();
    let api = &api;
    let modules = api
        .stream_run_modules(run_id, finished)
        .map_ok(|module| async move {
            let instances = if detail {
                Some(
                    api.stream_module_instances(run_id, &module.module_id, finished)
                        .try_collect()
                        .await?,
                )
            } else {
                None
            };
            Ok::<_, Report>(ModuleInstances { module, instances })
        })
        .try_buffered(INSTANCE_REQUESTS);
    let mut modules = std::pin::pin!(modules);

    if json {
        while let Some(module) = modules.try_next().await? {
            outln!("{}", serde_json::to_string(&module)?)?;
        }
        return Ok(());
    }

    let modules: Vec<ModuleInstances> = modules.try_collect().await?;
    if !modules.is_empty() {
        outln!(
            "{}",
            render_modules_table(&modules, detail, terminal_width())
        )?;
    }
    eprintln!("{}", modules_status_line(modules.len(), &status));
    Ok(())
}

/// One row for each module, sorted by module. With `detail`, one row for each
/// program that loaded a module, sorted by module, container and program; a
/// module that no program is listed for yet gets one row with blank
/// CONTAINER and PROGRAM.
fn render_modules_table(modules: &[ModuleInstances], detail: bool, width: usize) -> String {
    let mut rows: Vec<(&CoverageModule, &str, &str)> = Vec::new();
    for ModuleInstances { module, instances } in modules {
        match instances.as_deref() {
            Some([]) | None => rows.push((module, "", "")),
            Some(instances) => rows.extend(instances.iter().map(|instance| {
                (
                    module,
                    instance.container_name.as_str(),
                    instance.program_path.as_str(),
                )
            })),
        }
    }
    rows.sort_by_key(|&(module, container, program)| (&module.module_id, container, program));
    let rows = rows.into_iter().map(|(module, container, program)| {
        let mut row = vec![sanitize(&module.module_id), sanitize(&module.name)];
        if detail {
            row.extend([sanitize(container), sanitize(program)]);
        }
        row.extend([
            module.language.as_deref().map_or_default(sanitize),
            module.num_locations.to_string(),
            if module.has_catalog { "yes" } else { "no" }.to_string(),
        ]);
        row
    });
    let mut table = Table::builder().column(Column::new("MODULE")).column(
        Column::new("NAME")
            .fit(Fit::TruncatePath { floor: 12 })
            .priority(1),
    );
    if detail {
        table = table
            .column(
                Column::new("CONTAINER")
                    .fit(Fit::Truncate { floor: 10 })
                    .priority(1),
            )
            .column(Column::new("PROGRAM").fit(Fit::TruncatePath { floor: 19 }));
    }
    table
        .column(Column::new("LANG"))
        .column(Column::new("LOCATIONS").align(Align::Right))
        .column(Column::new("SYMBOLS"))
        .render(rows.collect(), width)
}

/// `7 modules · run in progress (more modules may load)`.
fn modules_status_line(modules: usize, status: &RunStatus) -> String {
    // The generated `Display` of `RunStatus` is the wire value, `in_progress`.
    let status_words = status.to_string().replace('_', " ");
    let mut line = format!("{} · run {status_words}", count(modules, "module"));
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
                instances: Some(vec![
                    instance("goworker", "/usr/local/bin/worker"),
                    instance("driver", "/usr/local/bin/driver"),
                ]),
            },
            ModuleInstances {
                module: module("dotnet-e798e24c32e9", "dotnethello", None, false),
                instances: Some(vec![instance("dotnet", "/usr/share/dotnet/dotnet")]),
            },
            ModuleInstances {
                module: module("go-cd4dd5ec3daf", "instrhello/cmd/worker", Some("Go"), true),
                instances: Some(vec![]),
            },
        ]
    }

    #[test]
    fn modules_table_has_a_row_per_module() {
        let modules: Vec<_> = modules()
            .into_iter()
            .map(|m| ModuleInstances {
                instances: None,
                ..m
            })
            .collect();
        insta::assert_snapshot!(render_modules_table(&modules, false, usize::MAX), @r"
        MODULE               NAME                      LANG  LOCATIONS  SYMBOLS
        dotnet-e798e24c32e9  dotnethello                             9  no
        go-20e55e2f1623      instrhello/internal/work  Go            9  yes
        go-cd4dd5ec3daf      instrhello/cmd/worker     Go            9  yes
        ");
    }

    #[test]
    fn detail_table_has_a_row_per_program_sorted_by_module() {
        insta::assert_snapshot!(render_modules_table(&modules(), true, usize::MAX), @r"
        MODULE               NAME                      CONTAINER  PROGRAM                   LANG  LOCATIONS  SYMBOLS
        dotnet-e798e24c32e9  dotnethello               dotnet     /usr/share/dotnet/dotnet                9  no
        go-20e55e2f1623      instrhello/internal/work  driver     /usr/local/bin/driver     Go            9  yes
        go-20e55e2f1623      instrhello/internal/work  goworker   /usr/local/bin/worker     Go            9  yes
        go-cd4dd5ec3daf      instrhello/cmd/worker                                          Go            9  yes
        ");
    }

    #[test]
    fn detail_table_shortens_paths_from_the_left() {
        let table = render_modules_table(&modules(), true, 1);
        assert!(table.contains("…/dotnet/dotnet"), "{table}");
        assert!(table.contains("…/work"), "{table}");
        assert!(table.contains("dotnethello "), "{table}");
        assert!(table.contains("go-20e55e2f1623 "), "{table}");
    }

    #[test]
    fn modules_status_line_counts_modules() {
        assert_eq!(
            modules_status_line(3, &RunStatus::InProgress),
            "3 modules · run in progress (more modules may load)"
        );
        assert_eq!(
            modules_status_line(1, &RunStatus::Completed),
            "1 module · run completed"
        );
    }
}
