//! `runs coverage`: which instrumented code a run reached, as a tree of
//! modules, files, classes, functions and locations.

mod filter;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;

use color_eyre::Section;
use color_eyre::eyre::{Report, Result};
use console::{measure_text_width, style, truncate_str};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use ptree::print_config::UTF_CHARS_BOLD;
use serde_json::{Value, json};

use crate::OutputOptions;
use crate::api::{
    self, AntithesisApi, CoverageModule, LocationCoverage, MIN_COVERAGE_RELEASE, RunDetail,
};
use crate::cli::{CoverageRollup, RunsCoverageArgs};
use crate::error::{api_error_status, user_error};
use crate::render::{count, sanitize};
use crate::settings::Settings;
use crate::table::{Column, Table, truncate_start};
use crate::vtime::VTime;

use filter::{Filter, ModuleIds};

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

/// The most modules that snouty requests one by one. The endpoint takes one
/// module, so more modules read the whole run once and keep theirs.
const MAX_MODULE_REQUESTS: usize = 8;

/// A module's name and language, from the module list.
#[derive(Clone, Default)]
struct ModuleInfo {
    name: Option<String>,
    language: Option<String>,
}

/// One location: the record that the API sent, and its parsed fields.
struct Location {
    raw: Value,
    coverage: LocationCoverage,
}

pub(super) async fn cmd_runs_coverage(
    args: RunsCoverageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    let filter = Filter::new(
        &args.module_names,
        &args.files,
        &args.classes,
        &args.functions,
    )?;
    let ids = ModuleIds::new(&args.module_ids);
    let api = AntithesisApi::new(settings, verbose)?;
    let (api, run_id) = (&api, args.run_id.as_str());
    // Wait once, before the module list and the locations: a run ID that
    // names no run fails as such, and a starting run has no module list yet.
    ready::wait_until_ready(api, run_id, READY_POLL).await?;

    // A module name filter needs the module list first, and becomes the IDs
    // of the modules that match. Otherwise the list only labels the modules,
    // and snouty reads it while the locations download.
    let mut infos = None;
    let mut selected: Option<HashSet<String>> = None;
    let requests: Vec<Option<String>> = if filter.module_name.is_empty() {
        if ids.include.is_empty() {
            vec![None]
        } else {
            ids.include
                .iter()
                .filter(|id| ids.passes(id))
                .cloned()
                .map(Some)
                .collect()
        }
    } else {
        api::ensure_module_list_supported(run_id)?;
        let modules: Vec<CoverageModule> = api.stream_run_modules(run_id).try_collect().await?;
        let matching: HashSet<String> = modules
            .iter()
            .filter(|m| ids.passes(&m.module_id) && filter.module_name.passes(&[m.name.as_str()]))
            .map(|m| m.module_id.clone())
            .collect();
        infos = Some(module_infos(modules));
        let requests = if matching.len() <= MAX_MODULE_REQUESTS {
            let mut ids: Vec<String> = matching.iter().cloned().collect();
            ids.sort();
            ids.into_iter().map(Some).collect()
        } else {
            vec![None]
        };
        selected = Some(matching);
        requests
    };
    let keep_module =
        |id: &str| ids.passes(id) && selected.as_ref().is_none_or(|ids| ids.contains(id));

    let lines = stream::iter(requests)
        .then(|module_id| async move {
            ready::send_when_ready(
                api,
                run_id,
                READY_POLL,
                COVERAGE_RETRY,
                || api.get_coverage_locations(run_id, module_id.as_deref()),
                explain_coverage_error,
            )
            .await
        })
        .try_flatten()
        .map(|line| {
            let value = line?;
            match stream_error_message(&value) {
                Some(message) => Err(mid_stream_error(message)),
                None => Ok(value),
            }
        });
    let mut modules = Modules {
        lines: lines.boxed(),
        filter: &filter,
        keep_module: &keep_module,
        pending: None,
    };

    // The API sends a module's locations together, so each module prints as
    // soon as the next one starts, and only one is in memory.
    let (infos, mut next) = if let Some(infos) = infos {
        (infos, modules.next().await?)
    } else {
        let (infos, first) = futures_util::join!(module_infos_or_none(api, run_id), modules.next());
        (infos, first?)
    };
    let render = Render {
        rollup: args.rollup,
        detail: args.detail,
        width: terminal_width(),
    };
    let (mut reached, mut total) = (0, 0);
    while let Some((id, locations)) = next {
        let info = infos.get(&id).cloned().unwrap_or_default();
        let tree = Node::module(id, info, &locations);
        if json {
            outln!("{}", tree.to_json(args.rollup))?;
        } else {
            if total > 0 {
                outln!()?;
            }
            outln!("{}", render.module(&tree))?;
        }
        reached += tree.reached;
        total += tree.total;
        next = modules.next().await?;
    }

    if json {
        return Ok(());
    }
    let run = api.get_run(run_id).await?.untag();
    // The generated `Display` of `RunStatus` is the wire value, `in_progress`.
    let status_words = run.status.to_string().replace('_', " ");
    eprintln!(
        "{} · {} reached · run {status_words}",
        count(total, "location"),
        percent(reached, total)
    );
    if !run.status.is_terminal() {
        eprintln!("the run is in progress, so re-run the command later for more coverage");
    }
    Ok(())
}

fn module_infos(modules: Vec<CoverageModule>) -> HashMap<String, ModuleInfo> {
    modules
        .into_iter()
        .map(|m| {
            let info = ModuleInfo {
                name: Some(m.name),
                language: m.language,
            };
            (m.module_id, info)
        })
        .collect()
}

/// The module list, to label the modules. A run without one, or a failed
/// request, leaves the labels out.
async fn module_infos_or_none(api: &AntithesisApi, run_id: &str) -> HashMap<String, ModuleInfo> {
    if api::ensure_module_list_supported(run_id).is_err() {
        return HashMap::new();
    }
    let modules: Result<Vec<CoverageModule>> = api.stream_run_modules(run_id).try_collect().await;
    modules.map_or_else(|_| HashMap::new(), module_infos)
}

/// The locations of the stream that pass the filters, a module at a time.
struct Modules<'a, S> {
    lines: S,
    filter: &'a Filter,
    keep_module: &'a dyn Fn(&str) -> bool,
    /// The first location of the next module, read while finishing the last.
    pending: Option<Location>,
}

impl<S: Stream<Item = Result<Value>> + Unpin> Modules<'_, S> {
    /// The next module with a location that passes the filters, and those
    /// locations.
    async fn next(&mut self) -> Result<Option<(String, Vec<Location>)>> {
        let mut module: Option<(String, Vec<Location>)> = None;
        loop {
            let location = match self.pending.take() {
                Some(location) => location,
                None => match self.lines.try_next().await? {
                    Some(value) => Location::parse(value)?,
                    None => return Ok(module),
                },
            };
            match &mut module {
                Some((id, _)) if *id != location.coverage.module_id => {
                    self.pending = Some(location);
                    return Ok(module);
                }
                Some((_, locations)) => {
                    if self.passes(&location) {
                        locations.push(location);
                    }
                }
                None => {
                    if self.passes(&location) {
                        module = Some((location.coverage.module_id.clone(), vec![location]));
                    }
                }
            }
        }
    }

    fn passes(&self, location: &Location) -> bool {
        let c = &location.coverage;
        let (class, function) = split_function(c);
        let class_names: Vec<&str> = class
            .iter()
            .flat_map(|(full, short)| [full.as_str(), short.as_str()])
            .collect();
        let file: Vec<&str> = c.file.as_deref().into_iter().collect();
        (self.keep_module)(&c.module_id)
            && self.filter.file.passes(&file)
            && self.filter.class.passes(&class_names)
            && self
                .filter
                .function
                .passes(&[c.function.as_str(), function.as_str()])
    }
}

impl Location {
    /// The location, with the vtime of its moment as an exact JSON number,
    /// as other commands normalize moments.
    fn parse(mut raw: Value) -> Result<Self> {
        let coverage: LocationCoverage = serde_json::from_value(raw.clone())?;
        if let Some(first) = raw.get_mut("first_reached") {
            normalize_vtime_field(first, "moment");
        }
        Ok(Self { raw, coverage })
    }
}

/// The class of a location's function, as its full and short names, and the
/// function without it. The class is the API's `class` field: a Java binary
/// name, a C# type name, a Go receiver's base type or a Python class; its
/// short name is the last dot-separated segment. Without one, a demangled
/// native name `<Type>::method` or `<Type as Trait>::method` gives the type,
/// and the function is `method` or `Trait::method`.
fn split_function(location: &LocationCoverage) -> (Option<(String, String)>, String) {
    let function = sanitize(&location.function);
    if let Some(class) = location.class.as_deref().filter(|c| !c.is_empty()) {
        let class = sanitize(class);
        let short = class.rsplit('.').next().unwrap_or(&class).to_string();
        return (Some((class, short)), function);
    }
    match native_impl(&function) {
        Some((ty, method)) => {
            let short = last_segment(&ty).to_string();
            (Some((ty, short)), method)
        }
        None => (None, function),
    }
}

/// `<Type>::rest` or `<Type as Trait>::rest`, with nested brackets: the type,
/// and `rest` or `Trait::rest`, with the trait's last path segment.
fn native_impl(function: &str) -> Option<(String, String)> {
    let inner = function.strip_prefix('<')?;
    let mut depth = 1;
    let end = inner.char_indices().find_map(|(i, c)| {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ => {}
        }
        (depth == 0).then_some(i)
    })?;
    let rest = inner[end + 1..].strip_prefix("::")?;
    let inner = &inner[..end];
    Some(match inner.split_once(" as ") {
        Some((ty, tr)) => (ty.to_string(), format!("{}::{rest}", last_segment(tr))),
        None => (inner.to_string(), rest.to_string()),
    })
}

/// The last `::`-separated segment of a path, outside angle brackets.
fn last_segment(path: &str) -> &str {
    let bytes = path.as_bytes();
    let (mut depth, mut start, mut i) = (0, 0, 0);
    while i < bytes.len() {
        match bytes[i] {
            b'<' => depth += 1,
            b'>' => depth -= 1,
            b':' if depth == 0 && bytes.get(i + 1) == Some(&b':') => {
                start = i + 2;
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    &path[start..]
}

/// A node of the tree: a module, file, class or function, with the coverage
/// of its locations.
struct Node<'a> {
    kind: Kind,
    reached: usize,
    total: usize,
    kids: Vec<Node<'a>>,
    /// A function's locations, in source order.
    locations: Vec<&'a Location>,
}

enum Kind {
    Module {
        id: String,
        info: ModuleInfo,
    },
    /// A file, or none when the compiler recorded none.
    File(Option<String>),
    /// A class's full and short names, or none for free functions.
    Class(Option<(String, String)>),
    Function(String),
}

impl<'a> Node<'a> {
    fn new(kind: Kind, locations: &[&'a Location], kids: Vec<Node<'a>>) -> Self {
        let reached = locations
            .iter()
            .filter(|l| l.coverage.first_reached.is_some())
            .count();
        Self {
            kind,
            reached,
            total: locations.len(),
            kids,
            locations: Vec::new(),
        }
    }

    /// A module's tree: files sorted by path, then classes and functions in
    /// source order. Free functions come first, under a class of none.
    fn module(id: String, info: ModuleInfo, locations: &'a [Location]) -> Self {
        let all: Vec<&Location> = locations.iter().collect();
        let mut files: BTreeMap<Option<String>, Vec<&Location>> = BTreeMap::new();
        for location in &all {
            let file = location.coverage.file.as_deref().map(sanitize);
            files.entry(file).or_default().push(location);
        }
        let files = files
            .into_iter()
            .map(|(file, locations)| {
                let mut classes = in_source_order(&locations, |l| split_function(&l.coverage).0);
                // Free functions first, then the classes in source order.
                classes.sort_by_key(|(class, _)| class.is_some());
                let classes = classes
                    .into_iter()
                    .map(|(class, locations)| {
                        Node::new(Kind::Class(class), &locations, function_nodes(&locations))
                    })
                    .collect();
                Node::new(Kind::File(file), &locations, classes)
            })
            .collect();
        Node::new(Kind::Module { id, info }, &all, files)
    }

    /// The tree as one JSON object, down to `rollup`.
    fn to_json(&self, rollup: CoverageRollup) -> Value {
        let mut value = match &self.kind {
            Kind::Module { id, info } => json!({
                "module_id": id,
                "module_name": info.name,
                "language": info.language,
            }),
            Kind::File(file) => json!({ "file": file }),
            Kind::Class(class) => json!({ "class": class.as_ref().map(|(full, _)| full) }),
            Kind::Function(function) => json!({ "function": function }),
        };
        value["reached"] = json!(self.reached);
        value["total"] = json!(self.total);
        let kids = || -> Value { self.kids.iter().map(|k| k.to_json(rollup)).collect() };
        match &self.kind {
            Kind::Module { .. } => value["files"] = kids(),
            Kind::File(_) if rollup != CoverageRollup::File => value["classes"] = kids(),
            Kind::File(_) => {}
            Kind::Class(_) => value["functions"] = kids(),
            Kind::Function(_) => {
                // The first moment that reached any of the function's
                // locations, for `runs logs`.
                let seconds = |v: &&Value| v["run_seconds"].as_f64().unwrap_or(f64::MAX);
                let first = self
                    .locations
                    .iter()
                    .filter_map(|l| l.raw.get("first_reached"))
                    .min_by(|a, b| seconds(a).total_cmp(&seconds(b)));
                if let Some(first) = first {
                    value["first_reached"] = first.clone();
                }
                if rollup == CoverageRollup::None {
                    value["locations"] = self.locations.iter().map(|l| l.raw.clone()).collect();
                }
            }
        }
        value
    }
}

/// A node per function, in source order, each with its locations in source
/// order: by line and column, then by address.
fn function_nodes<'a>(locations: &[&'a Location]) -> Vec<Node<'a>> {
    in_source_order(locations, |l| split_function(&l.coverage).1)
        .into_iter()
        .map(|(function, mut locations)| {
            locations.sort_by_key(|l| {
                let c = &l.coverage;
                let address = c.address.parse::<u64>().ok();
                (
                    c.begin_line.is_none(),
                    c.begin_line,
                    c.begin_column,
                    address,
                )
            });
            let mut node = Node::new(Kind::Function(function), &locations, Vec::new());
            node.locations = locations;
            node
        })
        .collect()
}

/// `locations` grouped by `key`, the groups in source order: by their first
/// line, then by key.
fn in_source_order<'a, K: Ord>(
    locations: &[&'a Location],
    key: impl Fn(&Location) -> K,
) -> Vec<(K, Vec<&'a Location>)> {
    let mut groups: BTreeMap<K, Vec<&Location>> = BTreeMap::new();
    for location in locations {
        groups.entry(key(location)).or_default().push(location);
    }
    let first_line = |ls: &[&Location]| ls.iter().filter_map(|l| l.coverage.begin_line).min();
    let mut groups: Vec<_> = groups.into_iter().collect();
    groups.sort_by(|(a_key, a), (b_key, b)| {
        let (a_line, b_line) = (first_line(a), first_line(b));
        (a_line.is_none(), a_line, a_key).cmp(&(b_line.is_none(), b_line, b_key))
    });
    groups
}

/// The width of the COVERAGE cell: the percent in 6 columns, two spaces, then
/// reached over total in 9, which SI notation is enough for.
const COUNTS_WIDTH: usize = 9;
const COVERAGE_WIDTH: usize = 6 + 2 + COUNTS_WIDTH;

/// `50.8%`: one decimal, so that a little coverage does not round to 0%.
fn percent(reached: usize, total: usize) -> String {
    if total == 0 {
        return "–".to_string();
    }
    #[allow(clippy::cast_precision_loss)]
    let percent = 100.0 * reached as f64 / total as f64;
    format!("{percent:.1}%")
}

/// A count in SI notation, as a gist: 7, 1.1k, 13k, 428k, 1.2M. It is cut,
/// never rounded up, and a trailing `.0` is dropped.
fn si(n: usize) -> String {
    for (unit, size) in [("M", 1_000_000), ("k", 1_000)] {
        if n >= size {
            let whole = n / size;
            let tenth = n % size * 10 / size;
            return if whole < 10 && tenth > 0 {
                format!("{whole}.{tenth}{unit}")
            } else {
                format!("{whole}{unit}")
            };
        }
    }
    n.to_string()
}

/// The COVERAGE cell: `66.7%       2/3`. 0% is yellow and 100% green.
fn coverage(reached: usize, total: usize) -> String {
    let counts = format!("{}/{}", si(reached), si(total));
    let text = format!("{:>6}  {counts:>COUNTS_WIDTH$}", percent(reached, total));
    if reached == 0 {
        style(text).yellow().to_string()
    } else if reached == total {
        style(text).green().to_string()
    } else {
        text
    }
}

/// The text of a module's tree: COVERAGE first on each line, then the node,
/// under its parent with the connectors of `snouty docs tree`. On a terminal,
/// a file loses its start and other names their end, to fit the width.
struct Render {
    rollup: CoverageRollup,
    detail: bool,
    width: usize,
}

impl Render {
    fn module(&self, tree: &Node<'_>) -> String {
        let Kind::Module { id, info } = &tree.kind else {
            unreachable!("a tree's root is its module")
        };
        let head = format!("module {}", sanitize(id));
        let mut label = String::new();
        for part in [&info.name, &info.language].into_iter().flatten() {
            label.push_str("  ");
            label.push_str(&sanitize(part));
        }
        let room = self.room("").saturating_sub(measure_text_width(&head));
        let mut lines = vec![format!(
            "{}  {}{}",
            coverage(tree.reached, tree.total),
            style(head).bold(),
            style(truncate_str(&label, room, "…")).dim()
        )];

        // The location tables of one module share their column widths.
        let mut tables = if self.rollup == CoverageRollup::None {
            let groups = functions(tree).map(|f| self.location_rows(f)).collect();
            location_table().render_groups(groups, usize::MAX)
        } else {
            Vec::new()
        }
        .into_iter();
        for (i, file) in tree.kids.iter().enumerate() {
            let last = i + 1 == tree.kids.len();
            self.node(file, "", last, &mut tables, &mut lines);
        }
        lines.join("\n")
    }

    /// The node's line, then its kids. A file whose only class is the free
    /// functions has no class level: its functions hang from the file.
    fn node(
        &self,
        node: &Node<'_>,
        lead: &str,
        last: bool,
        tables: &mut impl Iterator<Item = String>,
        lines: &mut Vec<String>,
    ) {
        let c = UTF_CHARS_BOLD;
        let branch = if last { c.turn_right } else { c.down_and_right };
        let prefix = format!("{lead}{branch}{0}{0} ", c.right);
        let kid_lead = if last {
            format!("{lead}    ")
        } else {
            format!("{lead}{}   ", c.down)
        };
        let room = self.room(&prefix);
        let text = match &node.kind {
            Kind::Module { .. } => unreachable!("a module is a root"),
            Kind::File(file) => {
                let file = file.as_deref().unwrap_or("(no file)");
                style(truncate_start(file, room)).bold().to_string()
            }
            Kind::Class(Some((_, short))) => {
                style(truncate_str(short, room, "…")).cyan().to_string()
            }
            Kind::Class(None) => style("(no class)").dim().to_string(),
            Kind::Function(function) => truncate_str(function, room, "…").into_owned(),
        };
        lines.push(format!(
            "{}  {}{text}",
            coverage(node.reached, node.total),
            style(prefix).dim()
        ));

        match &node.kind {
            Kind::Function(_) => {
                if let Some(table) = tables.next() {
                    let indent = " ".repeat(COVERAGE_WIDTH + 2);
                    let lead = style(&kid_lead).dim();
                    for line in table.lines() {
                        lines.push(format!("{indent}{lead}{line}").trim_end().to_string());
                    }
                }
                return;
            }
            Kind::File(_) if self.rollup == CoverageRollup::File => return,
            _ => {}
        }
        let kids: Vec<&Node<'_>> = match (&node.kind, node.kids.as_slice()) {
            (Kind::File(_), [only]) if matches!(only.kind, Kind::Class(None)) => {
                only.kids.iter().collect()
            }
            _ => node.kids.iter().collect(),
        };
        for (i, kid) in kids.iter().enumerate() {
            self.node(kid, &kid_lead, i + 1 == kids.len(), tables, lines);
        }
    }

    /// The room for a node's text after the COVERAGE cell and `prefix`, on a
    /// terminal. Off one, the width is unbounded, and so is the room.
    fn room(&self, prefix: &str) -> usize {
        self.width
            .saturating_sub(COVERAGE_WIDTH + 2 + measure_text_width(prefix))
            .max(8)
    }

    /// A function's locations: START and END of the span, then the moment
    /// where the run first reached it. An empty cell is `-`, and an
    /// unreached location is dim.
    fn location_rows(&self, function: &Node<'_>) -> Vec<Vec<String>> {
        function
            .locations
            .iter()
            .map(|location| {
                let c = &location.coverage;
                let first = c.first_reached.as_ref();
                let cells = [
                    span(c.begin_line, c.begin_column),
                    span(c.end_line, c.end_column),
                    first.map(|f| sanitize(&f.moment.input_hash)),
                    first.map(|f| vtime(f.moment.vtime, self.detail)),
                ];
                cells
                    .into_iter()
                    .map(|cell| match cell {
                        Some(text) if first.is_some() => text,
                        Some(text) => style(text).dim().to_string(),
                        None => style("-").dim().to_string(),
                    })
                    .collect()
            })
            .collect()
    }
}

/// The functions of a tree, in the order that it prints them.
fn functions<'t, 'a>(tree: &'t Node<'a>) -> Box<dyn Iterator<Item = &'t Node<'a>> + 't> {
    match tree.kind {
        Kind::Function(_) => Box::new(std::iter::once(tree)),
        _ => Box::new(tree.kids.iter().flat_map(functions)),
    }
}

fn location_table() -> Table {
    Table::builder()
        .column(Column::new("START"))
        .column(Column::new("END"))
        .column(Column::new("INPUT_HASH"))
        .column(Column::new("VTIME"))
}

/// `line:column`, `line`, or none when the API recorded no line.
fn span<N: std::fmt::Display>(line: Option<N>, column: Option<N>) -> Option<String> {
    let line = line?;
    Some(match column {
        Some(column) => format!("{line}:{column}"),
        None => line.to_string(),
    })
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

    fn location(
        module: &str,
        file: &str,
        class: Option<&str>,
        function: &str,
        line: u32,
        reached: Option<&str>,
    ) -> Location {
        let mut value = json!({
            "module_id": module,
            "address": format!("{line}"),
            "file": file,
            "function": function,
            "begin_line": line,
            "begin_column": 5,
            "end_line": line,
            "end_column": 9,
        });
        if let Some(class) = class {
            value["class"] = json!(class);
        }
        if let Some(vtime) = reached {
            value["first_reached"] = json!({
                "run_seconds": 1.5,
                "moment": {"input_hash": "-7", "vtime": vtime},
            });
        }
        Location::parse(value).unwrap()
    }

    fn plain(text: &str) -> String {
        console::strip_ansi_codes(text).into_owned()
    }

    fn sample() -> Vec<Location> {
        vec![
            location("m", "a.java", Some("com.x.Store"), "get", 20, None),
            location(
                "m",
                "a.java",
                Some("com.x.Cache$Entry"),
                "get",
                10,
                Some("12.846102431882170"),
            ),
            location("m", "a.java", None, "helper", 5, Some("1.0")),
            location("m", "b.go", None, "main", 1, None),
        ]
    }

    fn render(rollup: CoverageRollup, locations: &[Location]) -> String {
        let info = ModuleInfo {
            name: Some("app".into()),
            language: Some("Java".into()),
        };
        let tree = Node::module("m".into(), info, locations);
        plain(
            &Render {
                rollup,
                detail: false,
                width: usize::MAX,
            }
            .module(&tree),
        )
    }

    #[test]
    fn file_rollup_lists_files_under_the_module() {
        assert_eq!(
            render(CoverageRollup::File, &sample()),
            " 50.0%        2/4  module m  app  Java\n \
             66.7%        2/3  ┣━━ a.java\n  \
             0.0%        0/1  ┗━━ b.go"
        );
    }

    #[test]
    fn function_rollup_groups_methods_by_class_in_source_order() {
        let out = render(CoverageRollup::Function, &sample());
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[2], "100.0%        1/1  ┃   ┣━━ (no class)");
        assert_eq!(lines[3], "100.0%        1/1  ┃   ┃   ┗━━ helper");
        assert_eq!(lines[4], "100.0%        1/1  ┃   ┣━━ Cache$Entry");
        assert_eq!(lines[6], "  0.0%        0/1  ┃   ┗━━ Store");
        // A file of free functions only has no class level.
        assert_eq!(lines[9], "  0.0%        0/1      ┗━━ main");
    }

    #[test]
    fn location_rollup_hangs_a_table_under_each_function() {
        let out = render(CoverageRollup::None, &sample());
        assert!(
            out.contains("┃   ┃       START  END   INPUT_HASH  VTIME"),
            "{out}"
        );
        assert!(
            out.contains("┃   ┃       10:5   10:9  -7          12.84610"),
            "{out}"
        );
        assert!(
            out.contains("┃           20:5   20:9  -           -"),
            "{out}"
        );
    }

    #[test]
    fn json_nests_the_tree_down_to_the_rollup() {
        let locations = sample();
        let tree = Node::module("m".into(), ModuleInfo::default(), &locations);
        let file = tree.to_json(CoverageRollup::File);
        assert_eq!(file["files"][0]["file"], "a.java");
        assert!(file["files"][0].get("classes").is_none());
        let functions = tree.to_json(CoverageRollup::Function);
        let class = &functions["files"][0]["classes"][1];
        assert_eq!(class["class"], "com.x.Cache$Entry");
        assert_eq!(
            class["functions"][0]["first_reached"]["moment"]["vtime"],
            json!(12.84610243188217)
        );
        assert!(class["functions"][0].get("locations").is_none());
        let all = tree.to_json(CoverageRollup::None);
        assert_eq!(
            all["files"][0]["classes"][1]["functions"][0]["locations"][0]["address"],
            "10"
        );
    }

    #[test]
    fn native_names_give_the_type_and_trait() {
        assert_eq!(
            native_impl("<krill::disk::Disk>::budget_spend"),
            Some(("krill::disk::Disk".into(), "budget_spend".into()))
        );
        assert_eq!(
            native_impl("<a::Harness as a::disk::Component>::deliver"),
            Some(("a::Harness".into(), "Component::deliver".into()))
        );
        assert_eq!(
            last_segment("alloc::Box<dyn core::Fn<(), Output = a::B>>"),
            "Box<dyn core::Fn<(), Output = a::B>>"
        );
        assert_eq!(native_impl("alloc::boxed::box_new"), None);
    }

    #[test]
    fn counts_are_si_gists() {
        assert_eq!(si(7), "7");
        assert_eq!(si(1148), "1.1k");
        assert_eq!(si(4000), "4k");
        assert_eq!(si(13_003), "13k");
        assert_eq!(si(428_460), "428k");
        assert_eq!(si(1_250_000), "1.2M");
    }

    #[test]
    fn vtime_is_cut_without_rounding() {
        let of = |text: &str| serde_json::from_value::<VTime>(Value::from(text)).unwrap();
        assert_eq!(vtime(of("12.846109"), false), "12.84610");
        assert_eq!(vtime(of("12.846109"), true), "12.846109");
    }
}
