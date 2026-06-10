use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use color_eyre::Section;
use color_eyre::eyre::{OptionExt, Result, bail};
use ptree::print_config::UTF_CHARS_BOLD;
use ptree::{PrintConfig, write_tree_with};
use rusqlite::{Connection, OpenFlags};
use tempfile::NamedTempFile;

mod snippet;

use crate::cli::DocsCommands;
use crate::error::user_error;
use crate::snouty_config::{self, SnoutyConfig};

const DEFAULT_DOCS_URL: &str = "https://antithesis.com/docs";
const SEARCH_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "does", "how", "in", "is", "of", "or", "the", "to", "what",
];

fn docs_url(config: &impl SnoutyConfig) -> String {
    let mut base = config.docs_url().unwrap_or(DEFAULT_DOCS_URL).to_string();
    while base.ends_with('/') {
        base.pop();
    }
    base
}

fn cache_dir(config: &impl SnoutyConfig) -> Result<&Path> {
    let dir = config
        .cache_dir()
        .ok_or_eyre("could not determine cache directory")?;
    fs::create_dir_all(dir)?;
    Ok(dir)
}

fn db_path(config: &impl SnoutyConfig) -> Result<PathBuf> {
    if let Some(p) = config.docs_db_path() {
        return Ok(PathBuf::from(p));
    }
    Ok(cache_dir(config)?.join("docs.db"))
}

fn etag_path(config: &impl SnoutyConfig) -> Result<PathBuf> {
    Ok(cache_dir(config)?.join("docs.db.etag"))
}

pub async fn cmd_docs(command: DocsCommands, offline: bool, json: bool) -> Result<()> {
    let config = snouty_config::default_config(None);

    if !(offline || config.docs_db_path().is_some()) {
        update_with_fallback(&config).await?;
    }

    ensure_docs_db_available(&config, offline)?;

    match command {
        DocsCommands::Search { query, list, limit } => {
            if query.is_empty() {
                return Err(user_error("search query required"));
            }
            search(&config, &query.join(" "), json, list, limit)
        }
        DocsCommands::Sqlite => sqlite_path(&config),
        DocsCommands::Tree { depth, filter } => {
            tree(&config, depth.map(|d| d.get()), filter.as_deref())
        }
        DocsCommands::Show { path } => show(&config, &path),
    }
}

async fn update_with_fallback(config: &impl SnoutyConfig) -> Result<()> {
    if let Err(e) = download_and_cache_db(config).await {
        if db_path(config)?.exists() {
            eprintln!("Warning: failed to update docs, falling back to cached docs\n    {e}\n");
        } else {
            return Err(e);
        }
    }

    Ok(())
}

fn ensure_docs_db_available(config: &impl SnoutyConfig, offline: bool) -> Result<()> {
    let db = db_path(config)?;
    if db.exists() {
        return Ok(());
    }

    if config.docs_db_path().is_some() {
        return Err(user_error(format!(
            "Documentation database not found at {}",
            db.display()
        ))
        .suggestion("point ANTITHESIS_DOCS_DB_PATH at an existing file"));
    }

    if offline {
        return Err(user_error("Documentation database not found")
            .suggestion("remove --offline to download it"));
    }

    Err(user_error(format!(
        "Documentation database not found at {}",
        db.display()
    )))
}

async fn download_and_cache_db(config: &impl SnoutyConfig) -> Result<()> {
    if let Some((bytes, etag)) = fetch_db_if_changed(config).await? {
        atomic_write_db(config, &bytes)?;
        fs::write(etag_path(config)?, etag)?;
    }
    Ok(())
}

/// fetch_db_if_changed returns Ok(None) if the server indicates the database
/// has not changed (304 Not Modified).
async fn fetch_db_if_changed(config: &impl SnoutyConfig) -> Result<Option<(Vec<u8>, String)>> {
    let client = reqwest::Client::builder()
        .user_agent(crate::user_agent())
        .build()?;
    let mut request = client.get(format!("{}/sqlite.db", docs_url(config)));

    if let Ok(etag) = fs::read_to_string(etag_path(config)?) {
        request = request.header("If-None-Match", etag.trim());
    }

    let response = request.send().await?;
    let status = response.status();

    if status == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }

    if !status.is_success() {
        bail!(
            "API error: {} - failed to download documentation database",
            status.as_u16()
        );
    }

    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_eyre("server did not include an ETag header in the response")?;

    let bytes = response.bytes().await?.to_vec();

    Ok(Some((bytes, etag)))
}

fn atomic_write_db(config: &impl SnoutyConfig, bytes: &[u8]) -> Result<()> {
    let cache = cache_dir(config)?;
    let mut tmp = NamedTempFile::new_in(cache)?;
    std::io::Write::write_all(&mut tmp, bytes)?;

    let db_path = db_path(config)?;

    tmp.persist(&db_path).map_err(|e| e.error)?;

    let metadata = fs::metadata(&db_path)?;
    let mut perms = metadata.permissions();
    perms.set_readonly(true);
    fs::set_permissions(&db_path, perms)?;
    Ok(())
}

use snippet::{MATCH_END, MATCH_START};

/// Split a query into simple terms if it uses plain alphanumeric tokens only.
/// Returns None for anything containing FTS5 operators or special syntax.
fn simple_query_terms(query: &str) -> Option<Vec<&str>> {
    let terms: Vec<&str> = query.split_whitespace().collect();
    let is_simple = terms.iter().all(|t| {
        t.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && !matches!(*t, "AND" | "OR" | "NOT" | "NEAR")
    });
    if !is_simple {
        return None;
    }

    Some(terms)
}

/// Normalize simple natural-language queries by dropping filler words so
/// ranking and title boosts focus on the content-bearing terms.
fn normalized_query(query: &str) -> String {
    let Some(terms) = simple_query_terms(query) else {
        return query.to_string();
    };

    let filtered: Vec<&str> = terms
        .iter()
        .copied()
        .filter(|term| !SEARCH_STOPWORDS.contains(&term.to_ascii_lowercase().as_str()))
        .collect();
    let selected = if filtered.is_empty() { terms } else { filtered };

    selected.join(" ")
}

/// Build an FTS5 query that matches all terms against the title column.
/// Only operates on simple queries (alphanumeric terms and spaces).
/// Returns None for anything containing FTS5 operators or special syntax.
fn title_match_query(query: &str) -> Option<String> {
    let terms = simple_query_terms(query)?;

    Some(
        terms
            .iter()
            .map(|t| format!("title:{t}"))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn search(
    config: &impl SnoutyConfig,
    query: &str,
    json: bool,
    list: bool,
    limit: usize,
) -> Result<()> {
    let conn = open_db(config)?;
    let normalized_query = normalized_query(query);

    let title_query = title_match_query(&normalized_query);

    let order_by = if title_query.is_some() {
        "rank * CASE WHEN pages_fts.rowid IN (
             SELECT rowid FROM pages_fts WHERE pages_fts MATCH ?2
         ) THEN 2.0 ELSE 1.0 END"
    } else {
        "rank"
    };

    let sql = format!(
        "SELECT p.path,
                highlight(pages_fts, 0, '{MATCH_START}', '{MATCH_END}'),
                p.content,
                CASE WHEN ?2 != '' AND pages_fts.rowid IN (
                    SELECT rowid FROM pages_fts WHERE pages_fts MATCH ?2
                ) THEN 1 ELSE 0 END as title_boosted
         FROM pages_fts
         JOIN pages p ON p.rowid = pages_fts.rowid
         WHERE pages_fts MATCH ?1 AND rank MATCH 'bm25(5.0, 1.0)'
         ORDER BY {}
         LIMIT ?3",
        order_by,
    );

    let mut stmt = conn.prepare(&sql)?;

    // When there's no title query, ?2 is unused but still must be bound
    let title_param = title_query.as_deref().unwrap_or("");

    let fetch_limit = limit.saturating_mul(5).max(limit);

    let results: Vec<(String, String, String, bool)> = stmt
        .query_map(
            rusqlite::params![normalized_query, title_param, fetch_limit],
            |row| {
                Ok((
                    row.get::<_, String>(0)?, // path
                    row.get::<_, String>(1)?, // title
                    row.get::<_, String>(2)?, // content
                    row.get::<_, bool>(3)?,   // title_boosted
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let results: Vec<_> = results
        .into_iter()
        .take(limit)
        .map(|(path, title, content, title_boosted)| {
            (
                path,
                title,
                snippet::extract_snippet(&content, &normalized_query, 300, title_boosted),
            )
        })
        .collect();

    if json {
        if results.is_empty() {
            print_empty_json_array()?;
        } else if list {
            print_path_json(&results)?;
        } else {
            print_json(&results)?;
        }
    } else if results.is_empty() {
        eprintln!("No results found for '{}'", query);
    } else if list {
        print_paths(&results);
    } else {
        print_results(&results);
    }

    Ok(())
}

fn print_paths(results: &[(String, String, String)]) {
    for (path, _, _) in results {
        println!("{path}");
    }
}

fn print_empty_json_array() -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&Vec::<serde_json::Value>::new())?
    );
    Ok(())
}

fn print_path_json(results: &[(String, String, String)]) -> Result<()> {
    let items: Vec<&str> = results.iter().map(|(path, _, _)| path.as_str()).collect();
    println!("{}", serde_json::to_string_pretty(&items)?);
    Ok(())
}

fn print_json(results: &[(String, String, String)]) -> Result<()> {
    let items: Vec<serde_json::Value> = results
        .iter()
        .map(|(path, title, snippet)| {
            serde_json::json!({
                "path": path,
                "title": snippet::strip_markers(title),
                "snippet": snippet::strip_markers(snippet),
            })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&items)?);
    Ok(())
}

fn for_each_marked_segment(mut text: &str, mut f: impl FnMut(&str, bool)) {
    while let Some(start) = text.find(MATCH_START) {
        let (plain, rest) = text.split_at(start);
        if !plain.is_empty() {
            f(plain, false);
        }

        let rest = &rest[MATCH_START.len()..];
        if let Some(end) = rest.find(MATCH_END) {
            let (matched, remainder) = rest.split_at(end);
            if !matched.is_empty() {
                f(matched, true);
            }
            text = &remainder[MATCH_END.len()..];
        } else {
            if !rest.is_empty() {
                f(rest, false);
            }
            return;
        }
    }

    if !text.is_empty() {
        f(text, false);
    }
}

fn visible_len(text: &str) -> usize {
    let mut len = 0;
    for_each_marked_segment(text, |segment, _| len += segment.chars().count());
    len
}

fn render_marked(text: &str, bold_plain: bool) -> String {
    let mut rendered = String::new();

    for_each_marked_segment(text, |segment, highlighted| {
        if highlighted {
            rendered.push_str(&console::style(segment).yellow().bold().to_string());
        } else if bold_plain {
            rendered.push_str(&console::style(segment).bold().to_string());
        } else {
            rendered.push_str(segment);
        }
    });

    rendered
}

/// Word-wrap text that may contain MATCH_START/MATCH_END markers,
/// counting only visible characters toward the width.
fn wrap_snippet(snippet: &str, width: usize) -> String {
    let mut result = String::new();
    let mut col = 0;

    for word in snippet.split_whitespace() {
        let word_len = visible_len(word);
        if col > 0 && col + 1 + word_len > width {
            result.push('\n');
            col = 0;
        }
        if col > 0 {
            result.push(' ');
            col += 1;
        }
        result.push_str(word);
        col += word_len;
    }

    result
}

fn style_line(line: &str) -> String {
    render_marked(line, false)
}

fn style_title(title: &str) -> String {
    render_marked(title, true)
}

fn print_results(results: &[(String, String, String)]) {
    let width = console::Term::stdout().size().1.min(80) as usize;

    for (i, (path, title, snippet)) in results.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{}  {}", console::style(path).dim(), style_title(title),);
        let wrapped = wrap_snippet(snippet, width);
        for line in wrapped.lines() {
            println!("  {}", style_line(line));
        }
    }
}

fn open_db(config: &impl SnoutyConfig) -> Result<Connection> {
    let db = db_path(config)?;
    Ok(Connection::open_with_flags(
        &db,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?)
}

/// Resolve a language segment from a `generated/sdk/{lang}` request to a
/// canonical generated SDK and its entry-point index page (relative to
/// `docs_url()`). Generated SDK reference docs are produced by language-specific
/// tooling (godoc, rustdoc, pdoc, javadoc, docfx) rather than Eleventy, so they
/// are not in the offline Markdown set; we point callers at the live HTML to
/// crawl directly. Aliases such as `go` (the name used in the human-facing SDK
/// docs) are mapped to their generated-docs spelling (`golang`). Returns `None`
/// for languages without generated reference docs. Index paths were verified to
/// resolve.
fn generated_sdk_index(lang: &str) -> Option<(&'static str, &'static str)> {
    let resolved = match lang.to_ascii_lowercase().as_str() {
        "golang" | "go" => ("golang", "generated/sdk/golang/"),
        "rust" | "rs" => ("rust", "generated/sdk/rust/antithesis_sdk/"),
        // python's `python/` index is only a meta-refresh stub (not followed by
        // curl/wget), so point straight at the real module index page.
        "python" | "py" => ("python", "generated/sdk/python/antithesis.html"),
        "java" => ("java", "generated/sdk/java/"),
        // dotnet's `dotnet/` index is a meta-refresh stub (and points at a
        // broken absolute path), so link the namespace page directly.
        "dotnet" | "csharp" | "cs" | "c#" => {
            ("dotnet", "generated/sdk/dotnet/api/Antithesis.SDK.html")
        }
        _ => return None,
    };
    Some(resolved)
}

fn show(config: &impl SnoutyConfig, path: &str) -> Result<()> {
    let conn = open_db(config)?;

    let path = normalized_path(path);
    let db_path = format!("docs/{}", path);

    // Try exact match (normalize DB paths the same way)
    let result: Option<(String, String)> = conn
        .query_row(
            "SELECT title, content FROM pages WHERE trim(path, '/') = ?1",
            [&db_path],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();

    if let Some((title, content)) = result {
        println!("# {title}\n{content}");
        return Ok(());
    }

    // No exact match — find similar paths to suggest
    let mut stmt =
        conn.prepare("SELECT path FROM pages WHERE path LIKE '%' || ?1 || '%' LIMIT 10")?;
    let suggestions: Vec<String> = stmt
        .query_map([&path], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // The message states the error; the live-docs pointer and the close-match
    // candidates are guidance, so they ride along as notes.
    let mut report = user_error(format!("page not found: {db_path}"));
    let sdk_lang = path
        .strip_prefix("generated/sdk/")
        .and_then(|rest| rest.split('/').next())
        .and_then(generated_sdk_index);
    if let Some((lang, rel)) = sdk_lang {
        // Recognized generated SDK language: point at its live HTML index to
        // crawl directly. An unrecognized language falls through to the normal
        // not-found suggestions below.
        report = report.note(format!(
            "the {lang} SDK reference docs are not part of the offline docs; they are \
             published as HTML and can be crawled directly over the network, starting from {}/{}",
            docs_url(config),
            rel,
        ));
    } else if path == "generated" || path.starts_with("generated/") {
        // Other generated pages, including SDK languages we don't recognize
        // (our alias table may simply be out of date): point at the live docs.
        report = report
            .note("generated pages (e.g. SDK references) are not included in the offline docs")
            .note(format!(
                "if this is a valid page, try {}/{}/",
                docs_url(config),
                path
            ));
    }
    if !suggestions.is_empty() {
        report = report.suggestion(format!(
            "did you mean one of these?\n  {}",
            suggestions.join("\n  ")
        ));
    }
    Err(report)
}

fn sqlite_path(config: &impl SnoutyConfig) -> Result<()> {
    println!("{}", db_path(config)?.display());
    Ok(())
}

#[derive(Default)]
struct TreeNode {
    page_title: Option<String>,
    children: BTreeMap<String, TreeNode>,
}

impl TreeNode {
    /// Insert a documentation page into the path-derived tree, creating any
    /// missing intermediate grouping nodes along the way.
    fn insert_page(&mut self, path: &str, title: String) {
        let mut node = self;
        for segment in normalized_path(path)
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            node = node.children.entry(segment.to_string()).or_default();
        }
        node.page_title = Some(title);
    }
}

/// Load documentation pages from SQLite, optionally filter the tree, and print
/// a Unicode-rendered view of the remaining paths.
fn tree(config: &impl SnoutyConfig, depth: Option<usize>, filter: Option<&str>) -> Result<()> {
    let conn = open_db(config)?;
    let mut stmt = conn.prepare("SELECT path, title FROM pages ORDER BY path")?;
    let mut root = TreeNode::default();
    for page in stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })? {
        let (path, title) = page?;
        root.insert_page(&path, title);
    }

    let filter = filter.map(str::to_ascii_lowercase);
    if let Some(filter) = &filter {
        root = filter_tree(root, "", filter).unwrap_or_default();
    }

    if root.children.is_empty() {
        if let Some(label) = filter.as_deref() {
            eprintln!("No results found for '{label}'");
        } else {
            eprintln!("No documentation pages found");
        }
        return Ok(());
    }

    print!("{}", render_forest(&root, depth)?);
    Ok(())
}

/// Render each top-level node as its own tree so the synthetic `docs` root is
/// omitted from the user-facing output.
fn render_forest(root: &TreeNode, max_depth: Option<usize>) -> Result<String> {
    if root.children.is_empty() {
        return Ok(String::new());
    }

    let config = PrintConfig {
        indent: 4,
        characters: UTF_CHARS_BOLD.into(),
        ..PrintConfig::default()
    };
    let mut rendered = Vec::new();
    let child_count = root.children.len();

    for (index, (name, child)) in root.children.iter().enumerate() {
        let tree = render_tree(name, child, 1, max_depth);
        write_tree_with(&tree, &mut rendered, &config)?;
        if index + 1 != child_count {
            rendered.write_all(b"\n")?;
        }
    }

    Ok(String::from_utf8(rendered)?)
}

/// Convert a `TreeNode` into a printable tree item, stopping recursion once
/// the requested depth limit is reached.
fn render_tree(
    name: &str,
    node: &TreeNode,
    current_depth: usize,
    max_depth: Option<usize>,
) -> ptree::item::StringItem {
    let mut children = Vec::new();
    if max_depth.is_none_or(|limit| current_depth < limit) {
        for (child_name, child) in &node.children {
            children.push(render_tree(child_name, child, current_depth + 1, max_depth));
        }
    }

    ptree::item::StringItem {
        text: node_label(name, node),
        children,
    }
}

/// Render a node label as `segment - title` when the segment is a real page;
/// otherwise keep the plain grouping name.
fn node_label(name: &str, node: &TreeNode) -> String {
    node.page_title
        .as_deref()
        .map_or_else(|| name.to_string(), |title| page_label(name, title))
}

/// Format the display text for a page node.
fn page_label(name: &str, title: &str) -> String {
    format!("{name} - {title}")
}

/// Prune the tree to pages whose normalized path or title contains the filter,
/// preserving ancestor nodes needed to show matching descendants.
fn filter_tree(node: TreeNode, path_prefix: &str, filter: &str) -> Option<TreeNode> {
    let mut kept_children = BTreeMap::new();
    for (name, child) in node.children {
        let child_path = if path_prefix.is_empty() {
            name.clone()
        } else {
            format!("{path_prefix}/{name}")
        };
        if let Some(filtered_child) = filter_tree(child, &child_path, filter) {
            kept_children.insert(name, filtered_child);
        }
    }

    let page_matches = node.page_title.as_ref().is_some_and(|title| {
        path_prefix.to_ascii_lowercase().contains(filter)
            || title.to_ascii_lowercase().contains(filter)
    });

    if page_matches || !kept_children.is_empty() {
        Some(TreeNode {
            page_title: node.page_title,
            children: kept_children,
        })
    } else {
        None
    }
}

/// Normalize documentation paths so tree construction consistently works with
/// stored `docs/...` paths, public Antithesis docs URLs, and user-facing
/// relative paths.
fn normalized_path(path: &str) -> String {
    let trimmed = path
        .split(['?', '#'])
        .next()
        .unwrap_or(path)
        .trim_matches('/');
    let trimmed = trimmed
        .strip_prefix("https://antithesis.com/")
        .unwrap_or(trimmed);
    let trimmed = trimmed.strip_prefix("docs/").unwrap_or(trimmed);
    trimmed.strip_suffix(".md").unwrap_or(trimmed).to_string()
}

#[cfg(test)]
mod tests {
    use super::{generated_sdk_index, normalized_path};

    #[test]
    fn generated_sdk_index_resolves_go_alias() {
        assert_eq!(
            generated_sdk_index("go"),
            Some(("golang", "generated/sdk/golang/"))
        );
        assert_eq!(
            generated_sdk_index("golang"),
            Some(("golang", "generated/sdk/golang/"))
        );
    }

    #[test]
    fn generated_sdk_index_is_case_insensitive() {
        assert_eq!(
            generated_sdk_index("Rust").map(|(lang, _)| lang),
            Some("rust")
        );
    }

    #[test]
    fn generated_sdk_index_unknown_language_is_none() {
        assert_eq!(generated_sdk_index("cpp"), None);
        assert_eq!(generated_sdk_index(""), None);
    }

    #[test]
    fn normalized_path_strips_docs_prefix() {
        assert_eq!(normalized_path("docs/getting_started"), "getting_started");
    }

    #[test]
    fn normalized_path_accepts_full_antithesis_docs_url() {
        assert_eq!(
            normalized_path("https://antithesis.com/docs/getting_started/"),
            "getting_started"
        );
    }

    #[test]
    fn normalized_path_strips_markdown_suffix() {
        assert_eq!(
            normalized_path("https://antithesis.com/docs/getting_started.md"),
            "getting_started"
        );
    }

    #[test]
    fn normalized_path_discards_query_and_fragment() {
        assert_eq!(
            normalized_path("https://antithesis.com/docs/getting_started/?utm=1#overview"),
            "getting_started"
        );
    }
}
