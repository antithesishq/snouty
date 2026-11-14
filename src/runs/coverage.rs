//! `runs modules` and `runs coverage`: the run coverage API.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;
use std::io::Write;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use console::truncate_str;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Serialize;

use crate::OutputOptions;
use crate::api::{
    AntithesisApi, CoverageModule, CoverageModuleInstance, LocationCoverage, Reach, RunStatus,
};
use crate::cli::{ReachArg, RunsCoverageArgs};
use crate::error::api_error_status;
use crate::render::sanitize;
use crate::settings::Settings;
use crate::stdio;
use crate::table::{Align, Column, Fit, Table, truncate_path};
use crate::vtime::VTime;

use super::ready::{self, Needs, RetryDecision, RetryPolicy};
use super::{MID_STREAM_ERROR, MID_STREAM_NOTE, READY_POLL, stream_error_message, terminal_width};

/// The failure of `runs coverage` when the server ends the locations with a
/// `Stream_Error` line. snouty exits with status 3 for it, so that a script
/// can tell a partial download from a failed request or a usage error.
#[derive(Debug)]
pub struct CoverageCut(String);

impl std::fmt::Display for CoverageCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{MID_STREAM_ERROR}: {}", self.0)
    }
}

impl std::error::Error for CoverageCut {}

/// The locations endpoint answers 404 while the run starts (orbitinghail,
/// release 67.6, for a run launched a moment before). It answers 200 with no
/// lines for a module that the run has not loaded, and for a run from before
/// the coverage API, so a 404 from a ready run fails at once. An unknown run
/// fails when snouty reads it after the 404.
pub(super) const COVERAGE_RETRY: RetryPolicy = RetryPolicy {
    needs: Needs::Live,
    waiting: "waiting for the run's coverage",
    decide: |run, err| {
        RetryDecision::retry_if(api_error_status(err) == Some(404) && !run.is_ready())
    },
};

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

fn count(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

fn run_status(status: &RunStatus) -> String {
    format!("run {}", status.to_string().replace('_', " "))
}

pub(super) async fn cmd_runs_coverage(
    args: RunsCoverageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let RunsCoverageArgs {
        run_id,
        module_ids,
        reach: ReachArg(reach),
        follow,
        detail,
    } = args;

    let api = AntithesisApi::new(settings, verbose)?;
    // The endpoint takes one module, so each --module is its own request.
    let modules: Vec<Option<&str>> = if module_ids.is_empty() {
        vec![None]
    } else {
        let unique: indexmap::IndexSet<&str> = module_ids.iter().map(String::as_str).collect();
        unique.into_iter().map(Some).collect()
    };
    let (api, run_id) = (&api, run_id.as_str());
    let open = |module_id| {
        ready::send_when_ready(
            api,
            run_id,
            READY_POLL,
            COVERAGE_RETRY,
            move || api.get_coverage_locations(run_id, module_id, reach, follow),
            |_, err| err,
        )
    };
    // A followed stream stays open until the run ends, so open every module
    // at once. Otherwise read one module at a time.
    let mut lines = if follow {
        let streams = futures_util::future::try_join_all(modules.into_iter().map(open)).await?;
        stream::select_all(streams).boxed()
    } else {
        stream::iter(modules).then(open).try_flatten().boxed()
    };

    let layout = LocationLayout {
        moment: reach != Some(Reach::Unreached),
        tty: stdio::stdout().is_terminal(),
        detail,
    };
    // Without --reached, a followed stream sends a location again when the run
    // reaches it, so the count is of distinct locations.
    let mut locations: HashSet<(String, String)> = HashSet::new();
    while let Some(value) = lines.try_next().await? {
        if let Some(message) = stream_error_message(&value) {
            if json {
                outln!("{value}")?;
            }
            return Err(Report::new(CoverageCut(message.to_string())).note(MID_STREAM_NOTE));
        }
        // A followed stream also sends the unreached locations of a module
        // that loads later (the getCoverageLocations description says so).
        if reach == Some(Reach::Reached) && value.get("first_reached").is_none() {
            continue;
        }
        let first = locations.is_empty();
        locations.insert((
            value["module_id"].as_str().unwrap_or_default().to_string(),
            value["address"].as_str().unwrap_or_default().to_string(),
        ));
        if json {
            outln!("{value}")?;
        } else {
            if first {
                outln!("{}", console::style(layout.header()).bold())?;
            }
            let location: LocationCoverage = serde_json::from_value(value)?;
            outln!("{}", layout.row(&location))?;
        }
    }
    if json {
        return Ok(());
    }

    let run = api.get_run(run_id).await?.untag();
    eprintln!(
        "{} · {}",
        count(locations.len(), "location"),
        run_status(&run.status)
    );
    if !run.status.is_terminal() && !follow {
        // --follow does not combine with --unreached, and the MCP tool has no
        // follow param.
        let follow_hint = if reach == Some(Reach::Unreached) || crate::mcp::in_tool_call() {
            ""
        } else {
            "use --follow or "
        };
        eprintln!(
            "this may be a partial snapshot, {follow_hint}re-run the command for more locations"
        );
    }
    Ok(())
}

/// The columns of `runs coverage`. Rows print as they arrive, so the columns
/// have fixed widths, and a wider cell pushes the rest of its row right.
struct LocationLayout {
    /// The HASH and VTIME columns. `--unreached` rows have no moment.
    moment: bool,
    /// On a terminal, LOCATION and FUNCTION shorten to their columns.
    tty: bool,
    /// Print each vtime whole.
    detail: bool,
}

const MODULE_WIDTH: usize = 40;
const LOCATION_WIDTH: usize = 36;
const FUNCTION_WIDTH: usize = 32;
/// The widest input hash: `-9223372036854775808`.
const HASH_WIDTH: usize = 20;

impl LocationLayout {
    fn header(&self) -> String {
        self.join(["MODULE", "LOCATION", "FUNCTION", "HASH", "VTIME"])
    }

    fn row(&self, location: &LocationCoverage) -> String {
        let mut place = location.file.as_deref().map_or_default(sanitize);
        if let Some(line) = location.begin_line {
            let _ = write!(place, ":{line}");
            if let Some(column) = location.begin_column {
                let _ = write!(place, ":{column}");
            }
        }
        let mut function = sanitize(&location.function);
        if self.tty {
            place = truncate_path(&place, LOCATION_WIDTH);
            function = truncate_str(&function, FUNCTION_WIDTH, "…").into_owned();
        }
        let (hash, vtime) = match &location.first_reached {
            Some(first) => (
                sanitize(&first.moment.input_hash),
                self.vtime(first.moment.vtime),
            ),
            None => (String::new(), String::new()),
        };
        self.join([
            &sanitize(&location.module_id),
            &place,
            &function,
            &hash,
            &vtime,
        ])
    }

    /// The vtime truncated to five decimals, or whole with `detail`. It
    /// never rounds.
    fn vtime(&self, vtime: VTime) -> String {
        let mut text = vtime.to_string();
        if !self.detail
            && !text.contains('e')
            && let Some(dot) = text.find('.')
        {
            text.truncate((dot + 6).min(text.len()));
        }
        text
    }

    fn join(&self, [module, location, function, hash, vtime]: [&str; 5]) -> String {
        let mut line = String::new();
        let _ = write!(
            line,
            "{module:<MODULE_WIDTH$}  {location:<LOCATION_WIDTH$}  "
        );
        if self.moment {
            let _ = write!(
                line,
                "{function:<FUNCTION_WIDTH$}  {hash:<HASH_WIDTH$}  {vtime}"
            );
        } else {
            line += function;
        }
        line.truncate(line.trim_end().len());
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

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

    fn location(first_reached: Option<Value>) -> LocationCoverage {
        let mut value = serde_json::json!({
            "module_id": "d85d838a38496ad7233432355829abc7d2333c33",
            "address": "4856880",
            "file": "src/main.rs",
            "function": "rusthello::main",
            "begin_line": 13,
            "begin_column": 5,
        });
        if let Some(first_reached) = first_reached {
            value["first_reached"] = first_reached;
        }
        serde_json::from_value(value).unwrap()
    }

    fn reached() -> Value {
        serde_json::json!({
            "run_seconds": 31.4,
            "moment": {"input_hash": "-3625518438076122494", "vtime": "12.846102431882170"},
        })
    }

    #[test]
    fn location_rows_cut_the_vtime_to_five_decimals() {
        let layout = LocationLayout {
            moment: true,
            tty: false,
            detail: false,
        };
        insta::assert_snapshot!(
            [layout.header(), layout.row(&location(Some(reached()))), layout.row(&location(None))].join("\n"),
            @r"
        MODULE                                    LOCATION                              FUNCTION                          HASH                  VTIME
        d85d838a38496ad7233432355829abc7d2333c33  src/main.rs:13:5                      rusthello::main                   -3625518438076122494  12.84610
        d85d838a38496ad7233432355829abc7d2333c33  src/main.rs:13:5                      rusthello::main
        "
        );
        let detail = LocationLayout {
            detail: true,
            ..layout
        };
        assert!(
            detail
                .row(&location(Some(reached())))
                .ends_with(" 12.84610243188217")
        );
    }

    #[test]
    fn unreached_rows_have_no_moment_columns() {
        let layout = LocationLayout {
            moment: false,
            tty: false,
            detail: false,
        };
        assert_eq!(
            layout.header(),
            format!("{:<42}{:<38}FUNCTION", "MODULE", "LOCATION")
        );
    }
}
