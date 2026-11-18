//! `runs coverage`: the run's coverage locations, as a tree for each module
//! of files, classes, functions and locations.
//!
//! The locations arrive as NDJSON, by module. Each module becomes a
//! [`Module`] tree, then prints as text through [`tree::render`] or as one
//! JSON line, before the next module is read.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::iter::Sum;
use std::ops::Add;
use std::pin::pin;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use console::{Alignment, Style, measure_text_width, pad_str};
use futures_util::{Stream, StreamExt, TryStreamExt, future, stream};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::OutputOptions;
use crate::api::{
    self, AntithesisApi, CoverageModule, LocationCoverage, MIN_COVERAGE_RELEASE, RunDetail,
};
use crate::cli::{CoverageRollup, RunsCoverageArgs};
use crate::error::{api_error_status, user_error};
use crate::render::{Truncate, count, sanitize};
use crate::settings::Settings;
use crate::tree::{self, Node, Span};
use crate::vtime::VTime;

use super::ready::{self, Needs, RetryDecision, RetryPolicy};
use super::{
    READY_POLL, mid_stream_error, normalize_vtime_field, stream_error_message, terminal_width,
};

/// One location: typed for the tree, and whole for `--json`, with its vtime
/// as a JSON number.
struct Location {
    coverage: LocationCoverage,
    json: Value,
}

impl TryFrom<Value> for Location {
    type Error = Report;

    /// A `Stream_Error` line fails.
    fn try_from(mut json: Value) -> Result<Self> {
        if let Some(message) = stream_error_message(&json) {
            return Err(mid_stream_error(message));
        }
        if let Some(first_reached) = json.get_mut("first_reached") {
            normalize_vtime_field(first_reached, "moment");
        }
        let coverage = LocationCoverage::deserialize(&json)?;
        Ok(Self { coverage, json })
    }
}

/// The locations reached, of all the locations.
#[derive(Clone, Copy, Default)]
struct Counts {
    reached: u64,
    total: u64,
}

impl Counts {
    fn of(locations: &[Location]) -> Self {
        let reached = locations
            .iter()
            .filter(|location| location.coverage.first_reached.is_some())
            .count();
        Self {
            reached: reached as u64,
            total: locations.len() as u64,
        }
    }

    /// `66.7%`, or `–` of no locations.
    fn percent(self) -> String {
        if self.total == 0 {
            "–".to_string()
        } else {
            format!("{:.1}%", self.reached as f64 * 100.0 / self.total as f64)
        }
    }
}

impl Add for Counts {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            reached: self.reached + other.reached,
            total: self.total + other.total,
        }
    }
}

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}

/// A class's name as the API or the demangled name gives it, and its last
/// segment.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ClassName {
    full: String,
    short: String,
}

/// The offset of the `>` that closes the `<` at the start of `text`.
fn closing_bracket(text: &str) -> Option<usize> {
    let mut depth = 0;
    for (index, c) in text.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// The offsets where `pattern` starts in `text`, outside angle brackets.
fn top_level_matches<'a>(text: &'a str, pattern: &'a str) -> impl Iterator<Item = usize> + 'a {
    let mut depth = 0i32;
    text.char_indices().filter_map(move |(index, c)| {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ => {}
        }
        (depth == 0 && text[index..].starts_with(pattern)).then_some(index)
    })
}

/// The last `::`-separated segment of `path`, outside angle brackets.
fn last_segment(path: &str) -> &str {
    top_level_matches(path, "::")
        .last()
        .map_or(path, |at| &path[at + "::".len()..])
}

/// The class of a location's function, and the function's name in it.
fn split_class(location: &LocationCoverage) -> (Option<ClassName>, String) {
    let function = &location.function;
    if let Some(class) = location.class.as_deref().filter(|class| !class.is_empty()) {
        let short = class.rsplit('.').next().unwrap_or(class);
        let name = ClassName {
            full: class.to_string(),
            short: short.to_string(),
        };
        return (Some(name), function.clone());
    }
    // A demangled `<Type>::rest` or `<Type as Trait>::rest`.
    let Some(close) = function
        .starts_with('<')
        .then(|| closing_bracket(function))
        .flatten()
    else {
        return (None, function.clone());
    };
    let Some(rest) = function[close + 1..].strip_prefix("::") else {
        return (None, function.clone());
    };
    let inner = &function[1..close];
    let (class, function) = match top_level_matches(inner, " as ").next() {
        Some(at) => {
            let trait_name = last_segment(&inner[at + " as ".len()..]);
            (&inner[..at], format!("{trait_name}::{rest}"))
        }
        None => (inner, rest.to_string()),
    };
    let name = ClassName {
        full: class.to_string(),
        short: last_segment(class).to_string(),
    };
    (Some(name), function)
}

fn none_last(value: Option<i64>) -> (bool, Option<i64>) {
    (value.is_none(), value)
}

struct Function {
    name: String,
    counts: Counts,
    locations: Vec<Location>,
}

impl Function {
    fn new(name: String, mut locations: Vec<Location>) -> Self {
        locations.sort_by_key(|location| {
            let location = &location.coverage;
            (
                none_last(location.begin_line),
                none_last(location.begin_column),
                location.address.parse::<u64>().ok(),
            )
        });
        Self {
            name,
            counts: Counts::of(&locations),
            locations,
        }
    }

    fn first_line(&self) -> Option<i64> {
        self.locations[0].coverage.begin_line
    }

    /// Source order: by the first line, with no line last, then by name.
    fn order(&self) -> impl Ord + '_ {
        (none_last(self.first_line()), self.name.as_str())
    }

    /// The `first_reached` of the location that the run reached first.
    fn first_reached(&self) -> Option<&Value> {
        self.locations
            .iter()
            .filter_map(|location| {
                let seconds = location.coverage.first_reached.as_ref()?.run_seconds;
                Some((seconds, &location.json["first_reached"]))
            })
            .min_by(|(a, _), (b, _)| a.total_cmp(b))
            .map(|(_, first_reached)| first_reached)
    }
}

struct Class {
    /// `None` for the free functions.
    name: Option<ClassName>,
    counts: Counts,
    functions: Vec<Function>,
}

impl Class {
    fn new(name: Option<ClassName>, functions: BTreeMap<String, Vec<Location>>) -> Self {
        let mut functions: Vec<Function> = functions
            .into_iter()
            .map(|(name, locations)| Function::new(name, locations))
            .collect();
        functions.sort_by(|a, b| a.order().cmp(&b.order()));
        Self {
            name,
            counts: functions.iter().map(|function| function.counts).sum(),
            functions,
        }
    }

    fn first_line(&self) -> Option<i64> {
        self.functions[0].first_line()
    }

    /// The free functions first, then the classes in source order: by the
    /// first line, with no line last, then by name.
    fn order(&self) -> impl Ord + '_ {
        let name = self.name.as_ref().map(|name| name.full.as_str());
        (name.is_some(), none_last(self.first_line()), name)
    }
}

struct File {
    /// `None` for the locations with no file.
    path: Option<String>,
    counts: Counts,
    classes: Vec<Class>,
}

impl File {
    fn new(path: Option<String>, locations: Vec<Location>) -> Self {
        let mut classes: BTreeMap<Option<ClassName>, BTreeMap<String, Vec<Location>>> =
            BTreeMap::new();
        for location in locations {
            let (class, function) = split_class(&location.coverage);
            classes
                .entry(class)
                .or_default()
                .entry(function)
                .or_default()
                .push(location);
        }
        let mut classes: Vec<Class> = classes
            .into_iter()
            .map(|(name, functions)| Class::new(name, functions))
            .collect();
        classes.sort_by(|a, b| a.order().cmp(&b.order()));
        Self {
            path,
            counts: classes.iter().map(|class| class.counts).sum(),
            classes,
        }
    }
}

/// A module's locations, by file, class and function. Each level is in
/// the order that the text and JSON print.
struct Module {
    id: String,
    name: Option<String>,
    language: Option<String>,
    counts: Counts,
    files: Vec<File>,
}

impl Module {
    /// `locations` is not empty, and all of one module.
    fn new(locations: Vec<Location>, labels: &HashMap<String, CoverageModule>) -> Self {
        let id = locations[0].coverage.module_id.clone();
        let mut files: BTreeMap<Option<String>, Vec<Location>> = BTreeMap::new();
        for location in locations {
            files
                .entry(location.coverage.file.clone())
                .or_default()
                .push(location);
        }
        let files: Vec<File> = files
            .into_iter()
            .map(|(path, locations)| File::new(path, locations))
            .collect();
        let label = labels.get(&id);
        Self {
            name: label.map(|module| module.name.clone()),
            language: label.and_then(|module| module.language.clone()),
            counts: files.iter().map(|file| file.counts).sum(),
            id,
            files,
        }
    }

    fn locations(&self) -> impl Iterator<Item = &Location> {
        self.files
            .iter()
            .flat_map(|file| &file.classes)
            .flat_map(|class| &class.functions)
            .flat_map(|function| &function.locations)
    }
}

/// `n` in at most four characters: `7`, `1.1k`, `13k`, `1.2M`.
fn si(n: u64) -> String {
    let Some((unit, suffix)) = [(1_000_000, "M"), (1_000, "k")]
        .into_iter()
        .find(|&(unit, _)| n >= unit)
    else {
        return n.to_string();
    };
    let whole = n / unit;
    let tenth = n % unit * 10 / unit;
    if whole < 10 && tenth > 0 {
        format!("{whole}.{tenth}{suffix}")
    } else {
        format!("{whole}{suffix}")
    }
}

/// The fewest columns that a cut label keeps, however narrow the terminal.
const MIN_LABEL: usize = 8;

/// A node with the COVERAGE cell in its gutter.
fn node(counts: Counts, label: Vec<Span>, children: Vec<Node>) -> Node {
    let cell = format!(
        "{:>6}  {:<6}",
        counts.percent(),
        format!("{}/{}", si(counts.reached), si(counts.total))
    );
    let style = if counts.reached == 0 {
        Style::new().yellow()
    } else if counts.reached == counts.total {
        Style::new().green()
    } else {
        Style::new()
    };
    Node {
        gutter: style.apply_to(cell).to_string(),
        label,
        body: Vec::new(),
        children,
    }
}

/// `vtime` cut to five decimals, or whole with `detail`. Cutting never
/// rounds up, so the text names a moment at or before the vtime. An
/// exponent form prints whole: cutting it would change its value.
fn vtime_text(vtime: VTime, detail: bool) -> String {
    let mut text = vtime.to_string();
    if let Some(point) = text.find('.')
        && !detail
        && !text.contains('e')
    {
        text.truncate(point + 6);
    }
    text
}

/// The cells of a row: `LOCATION`, `INPUT_HASH` and `VTIME`, or `None` for an empty cell.
fn location_cells(location: &Location, detail: bool) -> [Option<String>; 3] {
    let location = &location.coverage;
    let place = match (location.begin_line, location.begin_column) {
        (Some(line), Some(column)) => Some(format!("{line}:{column}")),
        (Some(line), None) => Some(line.to_string()),
        (None, _) => None,
    };
    let moment = location.first_reached.as_ref().map(|first| &first.moment);
    [
        place,
        moment.map(|moment| sanitize(&moment.input_hash)),
        moment.map(|moment| vtime_text(moment.vtime, detail)),
    ]
}

const LOCATION_HEADER: [&str; 3] = ["LOCATION", "INPUT_HASH", "VTIME"];

/// The location tables of one module, which share their column widths.
struct LocationTable {
    widths: [usize; 3],
    detail: bool,
}

impl LocationTable {
    fn new(module: &Module, detail: bool) -> Self {
        let mut widths = LOCATION_HEADER.map(measure_text_width);
        for location in module.locations() {
            for (width, cell) in widths.iter_mut().zip(location_cells(location, detail)) {
                *width = (*width).max(cell.as_deref().map_or(1, measure_text_width));
            }
        }
        Self { widths, detail }
    }

    /// The header, then a row for each location. A `-` is an empty cell. It
    /// is dim, and so is each cell of a location that the run did not reach.
    fn lines(&self, locations: &[Location]) -> Vec<String> {
        let header = self.line(LOCATION_HEADER.map(|cell| (cell, Style::new())));
        let rows = locations.iter().map(|location| {
            let reached = location.coverage.first_reached.is_some();
            let cells = location_cells(location, self.detail);
            self.line(cells.each_ref().map(|cell| match cell {
                Some(text) if reached => (text.as_str(), Style::new()),
                Some(text) => (text.as_str(), Style::new().dim()),
                None => ("-", Style::new().dim()),
            }))
        });
        std::iter::once(header).chain(rows).collect()
    }

    fn line(&self, cells: [(&str, Style); 3]) -> String {
        let last = cells.len() - 1;
        cells
            .into_iter()
            .zip(self.widths)
            .enumerate()
            .map(|(index, ((text, style), width))| {
                let width = if index == last { 0 } else { width };
                style
                    .apply_to(pad_str(text, width, Alignment::Left, None))
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("  ")
    }
}

/// How deep the text tree goes.
enum Depth {
    Files,
    Functions,
    Locations(LocationTable),
}

fn function_node(function: &Function, depth: &Depth) -> Node {
    let label = vec![Span::new(sanitize(&function.name)).truncate(Truncate::end(MIN_LABEL))];
    let body = match depth {
        Depth::Locations(table) => table.lines(&function.locations),
        _ => Vec::new(),
    };
    Node {
        body,
        ..node(function.counts, label, Vec::new())
    }
}

fn class_node(class: &Class, depth: &Depth) -> Node {
    let label = match &class.name {
        Some(name) => Span::new(sanitize(&name.short))
            .style(Style::new().cyan())
            .truncate(Truncate::end(MIN_LABEL)),
        None => Span::new("(no class)").style(Style::new().dim()),
    };
    let children = class
        .functions
        .iter()
        .map(|function| function_node(function, depth))
        .collect();
    node(class.counts, vec![label], children)
}

fn file_node(file: &File, depth: &Depth) -> Node {
    let path = file.path.as_deref().map_or("(no file)".into(), sanitize);
    let label = vec![
        Span::new(path)
            .style(Style::new().bold())
            .truncate(Truncate::start(MIN_LABEL)),
    ];
    let children = match (depth, file.classes.as_slice()) {
        (Depth::Files, _) => Vec::new(),
        // Free functions only: no class level.
        (_, [free]) if free.name.is_none() => free
            .functions
            .iter()
            .map(|function| function_node(function, depth))
            .collect(),
        (_, classes) => classes
            .iter()
            .map(|class| class_node(class, depth))
            .collect(),
    };
    node(file.counts, label, children)
}

fn module_node(module: &Module, depth: &Depth) -> Node {
    let id = Span::new(format!("module {}", sanitize(&module.id))).style(Style::new().bold());
    let about = [&module.name, &module.language]
        .into_iter()
        .flatten()
        .map(|text| {
            Span::new(format!("  {}", sanitize(text)))
                .style(Style::new().dim())
                .truncate(Truncate::end(MIN_LABEL))
        });
    let label = std::iter::once(id).chain(about).collect();
    let children = module
        .files
        .iter()
        .map(|file| file_node(file, depth))
        .collect();
    node(module.counts, label, children)
}

fn function_json(function: Function, rollup: CoverageRollup) -> Value {
    let mut json = json!({
        "function": function.name,
        "reached": function.counts.reached,
        "total": function.counts.total,
    });
    if let Some(first_reached) = function.first_reached() {
        json["first_reached"] = first_reached.clone();
    }
    if rollup == CoverageRollup::None {
        json["locations"] = function
            .locations
            .into_iter()
            .map(|location| location.json)
            .collect();
    }
    json
}

fn class_json(class: Class, rollup: CoverageRollup) -> Value {
    let functions: Value = class
        .functions
        .into_iter()
        .map(|function| function_json(function, rollup))
        .collect();
    json!({
        "class": class.name.map(|name| name.full),
        "reached": class.counts.reached,
        "total": class.counts.total,
        "functions": functions,
    })
}

fn file_json(file: File, rollup: CoverageRollup) -> Value {
    let mut json = json!({
        "file": file.path,
        "reached": file.counts.reached,
        "total": file.counts.total,
    });
    if rollup != CoverageRollup::File {
        json["classes"] = file
            .classes
            .into_iter()
            .map(|class| class_json(class, rollup))
            .collect();
    }
    json
}

fn module_json(module: Module, rollup: CoverageRollup) -> Value {
    let files: Value = module
        .files
        .into_iter()
        .map(|file| file_json(file, rollup))
        .collect();
    json!({
        "module_id": module.id,
        "module_name": module.name,
        "language": module.language,
        "reached": module.counts.reached,
        "total": module.counts.total,
        "files": files,
    })
}

/// The locations endpoint answers 404 for a while after the run is ready. A
/// run created before [`MIN_COVERAGE_RELEASE`] answers 404 for good. The
/// command waits for the run once, before its first request.
pub(super) const COVERAGE_RETRY: RetryPolicy = RetryPolicy {
    needs: Needs::Live,
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
    if api_error_status(&err) == Some(404) && !run.created_on_or_after(MIN_COVERAGE_RELEASE) {
        let (major, minor) = MIN_COVERAGE_RELEASE;
        user_error(format!("run {} has no coverage locations", run.run_id)).note(format!(
            "coverage needs a run created on release {major}.{minor} or later"
        ))
    } else {
        err
    }
}

/// The locations, which arrive by module, as one `Vec` for each module.
fn by_module(
    locations: impl Stream<Item = Result<Location>>,
) -> impl Stream<Item = Result<Vec<Location>>> {
    stream::try_unfold(Box::pin(locations.peekable()), |mut locations| async move {
        let Some(first) = locations.next().await.transpose()? else {
            return Ok(None);
        };
        let module_id = first.coverage.module_id.clone();
        let mut module = vec![first];
        while let Some(location) = locations
            .as_mut()
            .next_if(|next| {
                next.as_ref()
                    .is_ok_and(|next| next.coverage.module_id == module_id)
            })
            .await
        {
            module.push(location?);
        }
        Ok(Some((module, locations)))
    })
}

/// The name and language of each module, by ID. Without them the tree
/// shows the IDs only, so a failure gives none.
async fn module_labels(api: &AntithesisApi, run_id: &str) -> HashMap<String, CoverageModule> {
    let labels = async {
        api::ensure_module_list_supported(run_id)?;
        api.stream_run_modules(run_id)
            .map_ok(|module| (module.module_id.clone(), module))
            .try_collect()
            .await
    };
    labels.await.unwrap_or_else(|err: Report| {
        log::debug!("no module names: {err:#}");
        HashMap::new()
    })
}

pub(super) async fn cmd_runs_coverage(
    args: RunsCoverageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let RunsCoverageArgs {
        run_id,
        mut module_ids,
        rollup,
        detail,
    } = args;
    let run_id = run_id.as_str();
    let api = &AntithesisApi::new(settings, verbose)?;
    ready::wait_until_ready(api, run_id, READY_POLL).await?;

    let mut seen = HashSet::new();
    module_ids.retain(|module_id| seen.insert(module_id.clone()));
    let requests: Vec<Option<&str>> = if module_ids.is_empty() {
        vec![None]
    } else {
        module_ids.iter().map(|id| Some(id.as_str())).collect()
    };
    let locations =
        stream::iter(requests)
            .then(|module_id| {
                ready::send_when_ready(
                    api,
                    run_id,
                    READY_POLL,
                    COVERAGE_RETRY,
                    move || async move {
                        Ok(api.get_coverage_locations(run_id, module_id).await?.untag())
                    },
                    explain_coverage_error,
                )
            })
            .try_flatten()
            .and_then(|line| future::ready(Location::try_from(line)));
    let mut modules = pin!(by_module(locations));
    let (labels, first) = future::join(module_labels(api, run_id), modules.try_next()).await;

    let width = terminal_width();
    let mut counts = Counts::default();
    let mut first_module = true;
    let mut next = first?;
    while let Some(locations) = next {
        let module = Module::new(locations, &labels);
        if json {
            outln!("{}", module_json(module, rollup))?;
        } else {
            let depth = match rollup {
                CoverageRollup::File => Depth::Files,
                CoverageRollup::Function => Depth::Functions,
                CoverageRollup::None => Depth::Locations(LocationTable::new(&module, detail)),
            };
            if !first_module {
                outln!()?;
            }
            outln!("{}", tree::render(&module_node(&module, &depth), width))?;
            counts = counts + module.counts;
            first_module = false;
        }
        next = modules.try_next().await?;
    }
    if json {
        return Ok(());
    }

    let status = api.get_run(run_id).await?.untag().status;
    eprintln!(
        "{} · {} reached · run {}",
        count(counts.total, "location"),
        counts.percent(),
        status.to_string().replace('_', " ")
    );
    if !status.is_terminal() {
        eprintln!("the run is in progress, so re-run the command later for more coverage");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(json: Value) -> Location {
        Location::try_from(json).unwrap()
    }

    fn at(function: &str, line: i64, reached: bool) -> Location {
        let mut json = json!({
            "module_id": "m", "address": line.to_string(), "file": "a.rs",
            "function": function, "begin_line": line, "begin_column": 1,
        });
        if reached {
            json["first_reached"] = json!({
                "run_seconds": line,
                "moment": {"input_hash": "h", "vtime": "12.3456789"},
            });
        }
        location(json)
    }

    fn class_of(function: &str, class: Option<&str>) -> (Option<(String, String)>, String) {
        let mut json = json!({"module_id": "m", "address": "1", "function": function});
        if let Some(class) = class {
            json["class"] = json!(class);
        }
        let (name, function) = split_class(&location(json).coverage);
        (name.map(|name| (name.full, name.short)), function)
    }

    fn named(full: &str, short: &str, function: &str) -> (Option<(String, String)>, String) {
        (
            Some((full.to_string(), short.to_string())),
            function.to_string(),
        )
    }

    #[test]
    fn si_keeps_three_significant_characters() {
        let cases = [
            (7, "7"),
            (999, "999"),
            (1148, "1.1k"),
            (4000, "4k"),
            (13003, "13k"),
            (428_460, "428k"),
            (1_250_000, "1.2M"),
        ];
        for (n, expected) in cases {
            assert_eq!(si(n), expected, "{n}");
        }
    }

    #[test]
    fn the_class_field_names_the_class() {
        assert_eq!(
            class_of("get", Some("com.x.Cache$Entry")),
            named("com.x.Cache$Entry", "Cache$Entry", "get")
        );
        assert_eq!(class_of("main", Some("")), (None, "main".to_string()));
    }

    #[test]
    fn a_demangled_type_names_the_class() {
        assert_eq!(
            class_of("<rusthello::Spinner>::spin", None),
            named("rusthello::Spinner", "Spinner", "spin")
        );
        assert_eq!(
            class_of("<a::Harness as a::disk::Component>::deliver", None),
            named("a::Harness", "Harness", "Component::deliver")
        );
        assert_eq!(
            class_of("<alloc::Box<dyn core::Fn<(), Output = a::B>>>::call", None),
            named(
                "alloc::Box<dyn core::Fn<(), Output = a::B>>",
                "Box<dyn core::Fn<(), Output = a::B>>",
                "call"
            )
        );
    }

    #[test]
    fn other_functions_are_free() {
        for function in ["a::b", "<a::B>", "<a::B>x", "<a::B", "<a::B> as c"] {
            assert_eq!(class_of(function, None), (None, function.to_string()));
        }
    }

    #[test]
    fn a_file_puts_free_functions_first_then_classes_in_source_order() {
        let module = Module::new(
            vec![
                at("<a::Late>::f", 30, true),
                at("<a::Early>::g", 20, false),
                at("<a::Early>::f", 10, true),
                at("free", 40, false),
            ],
            &HashMap::new(),
        );
        let file = &module.files[0];
        let order: Vec<(Option<&str>, Vec<&str>)> = file
            .classes
            .iter()
            .map(|class| {
                (
                    class.name.as_ref().map(|name| name.short.as_str()),
                    class.functions.iter().map(|f| f.name.as_str()).collect(),
                )
            })
            .collect();
        assert_eq!(
            order,
            [
                (None, vec!["free"]),
                (Some("Early"), vec!["f", "g"]),
                (Some("Late"), vec!["f"]),
            ]
        );
        assert_eq!((module.counts.reached, module.counts.total), (2, 4));
    }

    #[test]
    fn a_function_puts_its_locations_without_a_line_last() {
        let no_line = location(json!({"module_id": "m", "address": "9", "function": "f"}));
        let function = Function::new(
            "f".to_string(),
            vec![no_line, at("f", 5, false), at("f", 3, false)],
        );
        let lines: Vec<Option<i64>> = function
            .locations
            .iter()
            .map(|location| location.coverage.begin_line)
            .collect();
        assert_eq!(lines, [Some(3), Some(5), None]);
    }

    #[test]
    fn the_text_tree_elides_a_class_level_of_free_functions_only() {
        let module = Module::new(
            vec![at("main", 1, true), at("step", 2, false)],
            &HashMap::new(),
        );
        assert_eq!(
            tree::render(&module_node(&module, &Depth::Functions), usize::MAX),
            " 50.0%  1/2     module m\n \
             50.0%  1/2     ┗━━ a.rs\n\
             100.0%  1/1         ┣━━ main\n  \
             0.0%  0/1         ┗━━ step"
        );
    }

    #[test]
    fn location_tables_share_their_widths_and_dash_empty_cells() {
        let module = Module::new(
            vec![at("f", 1, true), at("long_line", 1000, false)],
            &HashMap::new(),
        );
        let table = LocationTable::new(&module, false);
        let function = &module.files[0].classes[0].functions[0];
        assert_eq!(
            table.lines(&function.locations),
            [
                "LOCATION  INPUT_HASH  VTIME",
                "1:1       h           12.34567",
            ]
        );
        let other = &module.files[0].classes[0].functions[1];
        assert_eq!(table.lines(&other.locations)[1], "1000:1    -           -");
    }

    #[test]
    fn vtime_text_cuts_to_five_decimals_without_rounding() {
        let vtime = |text: &str| text.parse::<VTime>().unwrap();
        assert_eq!(vtime_text(vtime("12.846102431882170"), false), "12.84610");
        assert_eq!(
            vtime_text(vtime("12.846102431882170"), true),
            "12.84610243188217"
        );
        assert_eq!(vtime_text(vtime("13.5"), false), "13.5");
        assert_eq!(
            vtime_text(vtime("2.3283064365386963e-10"), false),
            "2.3283064365386963e-10"
        );
    }

    #[test]
    fn a_function_json_has_the_first_reached_of_its_earliest_location() {
        let module = Module::new(
            vec![at("f", 9, true), at("f", 4, true), at("f", 6, false)],
            &HashMap::new(),
        );
        let json = module_json(module, CoverageRollup::Function);
        let function = &json["files"][0]["classes"][0]["functions"][0];
        assert_eq!(function["first_reached"]["run_seconds"], 4);
        assert_eq!(function["first_reached"]["moment"]["vtime"], 12.3456789);
        assert!(function.get("locations").is_none());
    }
}
