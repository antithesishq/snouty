//! `runs coverage`: which instrumented code a run reached, by file, function
//! or location.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::Write;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use console::{measure_text_width, style, truncate_str};
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::OutputOptions;
use crate::api::{self, AntithesisApi, LocationCoverage, MIN_COVERAGE_RELEASE, RunDetail};
use crate::cli::{CoverageLevel, RunsCoverageArgs};
use crate::error::{api_error_status, user_error};
use crate::render::{count, sanitize};
use crate::settings::Settings;
use crate::table::{Column, Fit, Table};
use crate::vtime::VTime;

use super::ready::{self, Needs, RetryDecision, RetryPolicy};
use super::{
    READY_POLL, mid_stream_error, normalize_vtime_field, stream_error_message, terminal_width,
};

/// Like the other run-scoped streams, the locations endpoint serves a run once
/// it is ready. It can still answer 404 for a minute or more after the run
/// reports that it is ready (orbitinghail, release 67.6). A run from before
/// the coverage API answers 404 too, so only a run created on
/// [`MIN_COVERAGE_RELEASE`] or later retries it. A finished run fails at
/// once, and an unknown run fails when snouty reads it after the 404.
pub(super) const COVERAGE_RETRY: RetryPolicy = RetryPolicy {
    needs: Needs::Ready,
    waiting: "waiting for the run's coverage",
    decide: |run, err| {
        RetryDecision::retry_if(
            api_error_status(err) == Some(404) && run.created_on_or_after(MIN_COVERAGE_RELEASE),
        )
    },
};

/// Annotate a locations error for `run`: a 404 from a run that will never
/// have coverage.
pub(super) fn explain_coverage_error(run: &RunDetail, err: Report) -> Report {
    if api_error_status(&err) != Some(404) || run.created_on_or_after(MIN_COVERAGE_RELEASE) {
        return err;
    }
    let (major, minor) = MIN_COVERAGE_RELEASE;
    user_error(format!("run {} has no coverage locations", run.run_id)).note(format!(
        "coverage needs a run created on release {major}.{minor} or later"
    ))
}

pub(super) async fn cmd_runs_coverage(
    args: RunsCoverageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let RunsCoverageArgs {
        run_id,
        module_ids,
        by,
        detail,
    } = args;
    if json && by.is_some() {
        return Err(user_error("--by sets the rows of the text output")
            .suggestion("drop --by: --json prints every location"));
    }

    let api = AntithesisApi::new(settings, verbose)?;
    // The endpoint takes one module, so each --module is its own request.
    let modules: Vec<Option<&str>> = if module_ids.is_empty() {
        vec![None]
    } else {
        let unique: indexmap::IndexSet<&str> = module_ids.iter().map(String::as_str).collect();
        unique.into_iter().map(Some).collect()
    };
    let (api, run_id) = (&api, run_id.as_str());
    let mut lines = stream::iter(modules)
        .then(|module_id| {
            ready::send_when_ready(
                api,
                run_id,
                READY_POLL,
                COVERAGE_RETRY,
                move || api.get_coverage_locations(run_id, module_id),
                explain_coverage_error,
            )
        })
        .try_flatten()
        .map(|line| {
            let value = line?;
            match stream_error_message(&value) {
                Some(message) => Err(mid_stream_error(message)),
                None => Ok(value),
            }
        })
        .boxed();

    if json {
        while let Some(mut value) = lines.try_next().await? {
            if let Some(first) = value.get_mut("first_reached") {
                normalize_vtime_field(first, "moment");
            }
            outln!("{value}")?;
        }
        return Ok(());
    }

    let locations = async {
        let mut locations = Vec::new();
        while let Some(value) = lines.try_next().await? {
            locations.push(serde_json::from_value::<LocationCoverage>(value)?);
        }
        Ok::<_, Report>(locations)
    };
    let (locations, names) = futures_util::join!(locations, module_names(api, run_id));
    let locations = locations?;
    let run = api.get_run(run_id).await?.untag();

    if !locations.is_empty() {
        let report = CoverageReport {
            names: &names,
            detail,
            width: terminal_width(),
        };
        outln!("{}", report.render(&locations, by.unwrap_or_default()))?;
    }
    let reached = locations
        .iter()
        .filter(|l| l.first_reached.is_some())
        .count();
    // The generated `Display` of `RunStatus` is the wire value, `in_progress`.
    let status_words = run.status.to_string().replace('_', " ");
    eprintln!(
        "{} · {} reached · run {status_words}",
        count(locations.len(), "location"),
        percent(reached, locations.len())
    );
    if !run.status.is_terminal() {
        eprintln!("the run is in progress, so re-run the command later for more coverage");
    }
    Ok(())
}

/// The name and language of each module, for the module headers. The
/// headers only label the modules, so a run without a module list, or a
/// failed request, leaves them out.
async fn module_names(api: &AntithesisApi, run_id: &str) -> HashMap<String, String> {
    if api::ensure_module_list_supported(run_id).is_err() {
        return HashMap::new();
    }
    let modules: Result<Vec<_>> = api.stream_run_modules(run_id).try_collect().await;
    modules
        .unwrap_or_default()
        .into_iter()
        .map(|module| {
            let mut label = sanitize(&module.name);
            if let Some(language) = &module.language {
                label = format!("{label}  {}", sanitize(language));
            }
            (module.module_id, label)
        })
        .collect()
}

/// `50.8%`: one decimal, so that a little coverage does not round to 0%.
fn percent(reached: usize, total: usize) -> String {
    if total == 0 {
        return "–".to_string();
    }
    #[allow(clippy::cast_precision_loss)]
    let percent = 100.0 * reached as f64 / total as f64;
    format!("{percent:.1}%")
}

/// The COVERAGE cell: the percent, right-aligned, then reached/total. 0% is
/// yellow and 100% green, on a terminal.
fn coverage(reached: usize, total: usize) -> String {
    let text = format!("{:>6}  {reached}/{total}", percent(reached, total));
    if reached == 0 {
        style(text).yellow().to_string()
    } else if reached == total {
        style(text).green().to_string()
    } else {
        text
    }
}

/// The location's function, after the short name of its class or type when
/// it has one: `Handler.do_GET`. The class is the last segment of a Java
/// binary name or a C# type name; the file already says where it lives.
fn function_name(location: &LocationCoverage) -> String {
    let function = sanitize(&location.function);
    match location.class.as_deref() {
        Some(class) if !class.is_empty() => {
            let short = class.rsplit('.').next().unwrap_or(class);
            format!("{}.{function}", sanitize(short))
        }
        _ => function,
    }
}

fn reached_count(locations: &[&LocationCoverage]) -> usize {
    locations
        .iter()
        .filter(|l| l.first_reached.is_some())
        .count()
}

/// A module's locations, by file. Locations without a file sort first, under
/// [`NO_FILE`].
type Files<'a> = BTreeMap<String, Vec<&'a LocationCoverage>>;

/// The text output: a header per module, then a table of its files, or a
/// header per file and a table of its functions or locations. Modules sort
/// by ID and files by path. The tables of one output share their column
/// widths, so they line up.
struct CoverageReport<'a> {
    names: &'a HashMap<String, String>,
    detail: bool,
    width: usize,
}

/// The file of the locations that the compiler recorded no file for. The
/// parentheses sort it before any path.
const NO_FILE: &str = "(no file)";

const FILE_INDENT: usize = 2;
const ROW_INDENT: usize = 4;

impl CoverageReport<'_> {
    fn render(&self, locations: &[LocationCoverage], level: CoverageLevel) -> String {
        let mut modules: BTreeMap<&str, Files<'_>> = BTreeMap::new();
        for location in locations {
            let file = location
                .file
                .as_deref()
                .map_or_else(|| NO_FILE.to_string(), sanitize);
            modules
                .entry(&location.module_id)
                .or_default()
                .entry(file)
                .or_default()
                .push(location);
        }
        match level {
            CoverageLevel::File => self.by_file(&modules),
            CoverageLevel::Function => {
                self.by_row(&modules, &Self::function_table(), Self::function_rows)
            }
            CoverageLevel::Location => self.by_row(&modules, &Self::location_table(), |rows| {
                self.location_rows(rows)
            }),
        }
    }

    /// `module <id>  <name>  <language>  <coverage>`. On a terminal the name
    /// and language are cut to fit. The module ID is never cut.
    fn module_header(&self, module_id: &str, files: &Files<'_>) -> String {
        let all: Vec<&LocationCoverage> = files.values().flatten().copied().collect();
        let head = format!("module {}", sanitize(module_id));
        let tail = format!("  {}", coverage(reached_count(&all), all.len()));
        let mut label = self
            .names
            .get(module_id)
            .map_or_else(String::new, |label| format!("  {label}"));
        let room = self
            .width
            .saturating_sub(measure_text_width(&head) + measure_text_width(&tail));
        label = truncate_str(&label, room, "…").into_owned();
        format!("{}{}{tail}", style(head).bold(), style(label).dim())
    }

    /// `<file>  <coverage>`, indented. On a terminal the file loses its
    /// start, so that the header fits.
    fn file_header(&self, file: &str, locations: &[&LocationCoverage]) -> String {
        let tail = format!("  {}", coverage(reached_count(locations), locations.len()));
        let room = self
            .width
            .saturating_sub(FILE_INDENT + measure_text_width(&tail))
            .max(8);
        let file = crate::table::truncate_start(file, room);
        format!("{:FILE_INDENT$}{}{tail}", "", style(file).bold())
    }

    fn by_file(&self, modules: &BTreeMap<&str, Files<'_>>) -> String {
        let table = Table::builder()
            .column(Column::new("FILE").fit(Fit::Truncate {
                floor: 16,
                from_left: true,
            }))
            .column(Column::new("COVERAGE"));
        let groups = modules
            .values()
            .map(|files| {
                files
                    .iter()
                    .map(|(file, locations)| {
                        vec![
                            file.clone(),
                            coverage(reached_count(locations), locations.len()),
                        ]
                    })
                    .collect()
            })
            .collect();
        let tables = table.render_groups(groups, self.width.saturating_sub(FILE_INDENT));
        let blocks = modules
            .iter()
            .zip(tables)
            .map(|((module_id, files), table)| {
                format!(
                    "{}\n{}",
                    self.module_header(module_id, files),
                    indent(&table, FILE_INDENT)
                )
            });
        blocks.collect::<Vec<_>>().join("\n\n")
    }

    /// A header per module, then a header per file, each over a table of
    /// `rows` of the file's locations.
    fn by_row(
        &self,
        modules: &BTreeMap<&str, Files<'_>>,
        table: &Table,
        rows: impl Fn(&[&LocationCoverage]) -> Vec<Vec<String>>,
    ) -> String {
        let groups: Vec<Vec<Vec<String>>> = modules
            .values()
            .flat_map(Files::values)
            .map(|locations| rows(locations))
            .collect();
        let mut tables = table
            .render_groups(groups, self.width.saturating_sub(ROW_INDENT))
            .into_iter();
        let mut blocks = Vec::new();
        for (module_id, files) in modules {
            let mut block = self.module_header(module_id, files);
            for (file, locations) in files {
                let table = tables.next().unwrap_or_default();
                let _ = write!(
                    block,
                    "\n\n{}\n{}",
                    self.file_header(file, locations),
                    indent(&table, ROW_INDENT)
                );
            }
            blocks.push(block);
        }
        blocks.join("\n\n")
    }

    fn function_table() -> Table {
        Table::builder()
            .column(Column::new("FUNCTION").fit(Fit::Truncate {
                floor: 16,
                from_left: false,
            }))
            .column(Column::new("COVERAGE"))
    }

    /// One row per function of a class, in source order: by the first line
    /// of its locations, then by name. Methods of different classes stay
    /// apart even when they share a name.
    fn function_rows(locations: &[&LocationCoverage]) -> Vec<Vec<String>> {
        let mut functions: HashMap<String, Vec<&LocationCoverage>> = HashMap::new();
        for location in locations {
            functions
                .entry(function_name(location))
                .or_default()
                .push(location);
        }
        let mut functions: Vec<_> = functions.into_iter().collect();
        let first_line =
            |locations: &[&LocationCoverage]| locations.iter().filter_map(|l| l.begin_line).min();
        functions.sort_by(|(a_name, a), (b_name, b)| {
            (first_line(a).is_none(), first_line(a), a_name).cmp(&(
                first_line(b).is_none(),
                first_line(b),
                b_name,
            ))
        });
        functions
            .into_iter()
            .map(|(function, locations)| {
                vec![
                    function,
                    coverage(reached_count(&locations), locations.len()),
                ]
            })
            .collect()
    }

    fn location_table() -> Table {
        Table::builder()
            .column(Column::new("INPUT_HASH"))
            .column(Column::new("VTIME"))
            .column(Column::new("LOCATION").fit(Fit::Truncate {
                floor: 20,
                from_left: false,
            }))
    }

    /// One row per location, in source order: by line and column, then by
    /// address, so that the locations at one place in the source stay
    /// together. LOCATION is `function:line:column`. An unreached location
    /// has no moment, and is dim.
    fn location_rows(&self, locations: &[&LocationCoverage]) -> Vec<Vec<String>> {
        let mut locations = locations.to_vec();
        locations.sort_by_key(|l| {
            (
                l.begin_line.is_none(),
                l.begin_line,
                l.begin_column,
                l.address.parse::<u64>().ok(),
            )
        });
        locations
            .into_iter()
            .map(|location| {
                let mut place = function_name(location);
                if let Some(line) = location.begin_line {
                    let _ = write!(place, ":{line}");
                    if let Some(column) = location.begin_column {
                        let _ = write!(place, ":{column}");
                    }
                }
                match &location.first_reached {
                    Some(first) => vec![
                        sanitize(&first.moment.input_hash),
                        vtime(first.moment.vtime, self.detail),
                        place,
                    ],
                    None => vec![String::new(), String::new(), style(place).dim().to_string()],
                }
            })
            .collect()
    }
}

fn indent(text: &str, width: usize) -> String {
    text.lines()
        .map(|line| format!("{:width$}{line}", ""))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The vtime cut to five decimals, or whole with `detail`. It never rounds:
/// a cut vtime still names a moment at or before the location's first reach,
/// so `runs logs` takes it.
fn vtime(vtime: VTime, detail: bool) -> String {
    let mut text = vtime.to_string();
    if !detail
        && !text.contains('e')
        && let Some(dot) = text.find('.')
    {
        text.truncate((dot + 6).min(text.len()));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn location(
        module: &str,
        file: &str,
        function: &str,
        line: u32,
        column: u32,
        reached: Option<&str>,
    ) -> LocationCoverage {
        let mut value = json!({
            "module_id": module,
            "address": format!("{line}{column}"),
            "file": file,
            "function": function,
            "begin_line": line,
            "begin_column": column,
            "end_line": line,
            "end_column": column,
        });
        if let Some(vtime) = reached {
            value["first_reached"] = json!({
                "run_seconds": 1.5,
                "moment": {"input_hash": "-7", "vtime": vtime},
            });
        }
        serde_json::from_value(value).unwrap()
    }

    fn report(names: &HashMap<String, String>) -> CoverageReport<'_> {
        CoverageReport {
            names,
            detail: false,
            width: usize::MAX,
        }
    }

    fn sample() -> Vec<LocationCoverage> {
        vec![
            location("m2", "b.go", "B", 3, 1, None),
            location("m1", "z.go", "Late", 30, 2, Some("1.0")),
            location("m1", "a.go", "Second", 20, 1, None),
            location("m1", "a.go", "First", 10, 5, Some("12.846102431882170")),
            location("m1", "a.go", "First", 10, 1, Some("2.5")),
        ]
    }

    fn plain(text: &str) -> String {
        console::strip_ansi_codes(text).into_owned()
    }

    #[test]
    fn file_level_groups_files_under_sorted_modules() {
        let names = HashMap::from([("m1".to_string(), "one  Go".to_string())]);
        let out = plain(&report(&names).render(&sample(), CoverageLevel::File));
        assert_eq!(
            out,
            "module m1  one  Go   75.0%  3/4\n  \
             FILE  COVERAGE\n  \
             a.go   66.7%  2/3\n  \
             z.go  100.0%  1/1\n\
             \n\
             module m2    0.0%  0/1\n  \
             FILE  COVERAGE\n  \
             b.go    0.0%  0/1"
        );
    }

    #[test]
    fn function_level_lists_functions_in_source_order() {
        let names = HashMap::new();
        let out = plain(&report(&names).render(&sample(), CoverageLevel::Function));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[2], "  a.go   66.7%  2/3");
        assert_eq!(lines[3], "    FUNCTION  COVERAGE");
        assert_eq!(lines[4], "    First     100.0%  2/2");
        assert_eq!(lines[5], "    Second      0.0%  0/1");
    }

    #[test]
    fn location_level_sorts_by_line_and_column() {
        let names = HashMap::new();
        let out = plain(&report(&names).render(&sample(), CoverageLevel::Location));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[3], "    INPUT_HASH  VTIME     LOCATION");
        assert_eq!(lines[4], "    -7          2.5       First:10:1");
        assert_eq!(lines[5], "    -7          12.84610  First:10:5");
        assert_eq!(lines[6].trim(), "Second:20:1");
    }

    #[test]
    fn headers_fit_a_narrow_terminal() {
        let names = HashMap::from([("m1".to_string(), "a-very-long-module-name  Go".to_string())]);
        let report = CoverageReport {
            names: &names,
            detail: false,
            width: 30,
        };
        let locations = vec![location(
            "m1",
            "/src/deep/path/to/the/file.go",
            "F",
            1,
            1,
            None,
        )];
        let out = plain(&report.render(&locations, CoverageLevel::Function));
        for line in out.lines() {
            assert!(measure_text_width(line) <= 30, "{line:?}");
        }
        assert!(out.contains("…"), "{out}");
    }

    #[test]
    fn methods_of_different_classes_get_their_own_rows() {
        let with_class = |class: &str, reached| {
            let mut value =
                serde_json::to_value(location("m", "a.java", "get", 1, 1, reached)).unwrap();
            value["class"] = json!(class);
            serde_json::from_value::<LocationCoverage>(value).unwrap()
        };
        let locations = vec![
            with_class("com.example.Cache$Entry", Some("1.0")),
            with_class("com.example.Store", None),
        ];
        let names = HashMap::new();
        let out = plain(&report(&names).render(&locations, CoverageLevel::Function));
        assert!(out.contains("Cache$Entry.get  100.0%  1/1"), "{out}");
        assert!(out.contains("Store.get          0.0%  0/1"), "{out}");
        let out = plain(&report(&names).render(&locations, CoverageLevel::Location));
        assert!(out.contains("Store.get:1:1"), "{out}");
    }

    #[test]
    fn vtime_is_cut_without_rounding() {
        let vtime_of = |text: &str| serde_json::from_value::<VTime>(Value::from(text)).unwrap();
        assert_eq!(vtime(vtime_of("12.846109"), false), "12.84610");
        assert_eq!(vtime(vtime_of("12.846109"), true), "12.846109");
    }
}
