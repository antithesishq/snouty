use std::fs;
use std::path::PathBuf;

use color_eyre::eyre::{OptionExt, Result, bail};
use rusqlite::{Connection, OpenFlags};
use tempfile::NamedTempFile;

mod snippet;

use crate::cli::{DocsCommands, OutputFormat};

const DEFAULT_DOCS_URL: &str = "https://antithesis.com/docs";

fn docs_url() -> String {
    let mut base =
        std::env::var("ANTITHESIS_DOCS_URL").unwrap_or_else(|_| DEFAULT_DOCS_URL.to_string());
    while base.ends_with('/') {
        base.pop();
    }
    base
}

fn docs_user_agent() -> String {
    format!(
        "snouty/{} ({}; {}; rust{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("SNOUTY_RUSTC_VERSION")
    )
}

fn cache_dir() -> Result<PathBuf> {
    let dir = dirs::cache_dir()
        .ok_or_eyre("could not determine cache directory")?
        .join("snouty");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn db_path() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("ANTITHESIS_DOCS_DB_PATH") {
        return Ok(PathBuf::from(p));
    }
    Ok(cache_dir()?.join("docs.db"))
}

fn env_db_path_is_set() -> bool {
    std::env::var_os("ANTITHESIS_DOCS_DB_PATH").is_some()
}

fn etag_path() -> Result<PathBuf> {
    Ok(cache_dir()?.join("docs.db.etag"))
}

pub async fn cmd_docs(command: DocsCommands, offline: bool) -> Result<()> {
    if !(offline || env_db_path_is_set()) {
        update_with_fallback().await?;
    }

    ensure_docs_db_available(offline)?;

    match command {
        DocsCommands::Search {
            query,
            format,
            limit,
        } => {
            if query.is_empty() {
                bail!("search query required");
            }
            search(&query.join(" "), format, limit)
        }
        DocsCommands::Sqlite => sqlite_path(),
        DocsCommands::Show { path } => show(&path),
    }
}

async fn update_with_fallback() -> Result<()> {
    if let Err(e) = download_and_cache_db().await {
        if db_path()?.exists() {
            eprintln!("Warning: failed to update docs, falling back to cached docs\n    {e}\n");
        } else {
            return Err(e);
        }
    }

    Ok(())
}

fn ensure_docs_db_available(offline: bool) -> Result<()> {
    let db = db_path()?;
    if db.exists() {
        return Ok(());
    }

    if env_db_path_is_set() {
        bail!(
            "Documentation database not found at {}. Point ANTITHESIS_DOCS_DB_PATH at an existing file.",
            db.display()
        );
    }

    if offline {
        bail!("Documentation database not found. Remove --offline to download it.");
    }

    bail!("Documentation database not found at {}.", db.display());
}

async fn download_and_cache_db() -> Result<()> {
    if let Some((bytes, etag)) = fetch_db_if_changed().await? {
        atomic_write_db(&bytes)?;
        fs::write(etag_path()?, etag)?;
    }
    Ok(())
}

/// fetch_db_if_changed returns Ok(None) if the server indicates the database
/// has not changed (304 Not Modified).
async fn fetch_db_if_changed() -> Result<Option<(Vec<u8>, String)>> {
    let client = reqwest::Client::builder()
        .user_agent(docs_user_agent())
        .build()?;
    let mut request = client.get(format!("{}/sqlite.db", docs_url()));

    if let Ok(etag) = fs::read_to_string(etag_path()?) {
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

fn atomic_write_db(bytes: &[u8]) -> Result<()> {
    let cache = cache_dir()?;
    let mut tmp = NamedTempFile::new_in(&cache)?;
    std::io::Write::write_all(&mut tmp, bytes)?;

    // Make the file read-only to prevent accidental modifications
    let metadata = tmp.as_file().metadata()?;
    let mut perms = metadata.permissions();
    perms.set_readonly(true);
    tmp.as_file().set_permissions(perms)?;

    tmp.persist(db_path()?).map_err(|e| e.error)?;
    Ok(())
}

use snippet::{MATCH_END, MATCH_START};

/// Build an FTS5 query that matches all terms against the title column.
/// Only operates on simple queries (alphanumeric terms and spaces).
/// Returns None for anything containing FTS5 operators or special syntax.
fn title_match_query(query: &str) -> Option<String> {
    let terms: Vec<&str> = query.split_whitespace().collect();
    let is_simple = terms.iter().all(|t| {
        t.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            && !matches!(*t, "AND" | "OR" | "NOT" | "NEAR")
    });
    if !is_simple {
        return None;
    }

    Some(
        terms
            .iter()
            .map(|t| format!("title:{t}"))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn search(query: &str, format: OutputFormat, limit: usize) -> Result<()> {
    let conn = open_db()?;

    let title_query = title_match_query(query);

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

    let results: Vec<(String, String, String, bool)> = stmt
        .query_map(rusqlite::params![query, title_param, limit], |row| {
            Ok((
                row.get::<_, String>(0)?, // path
                row.get::<_, String>(1)?, // title
                row.get::<_, String>(2)?, // content
                row.get::<_, bool>(3)?,   // title_boosted
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let results: Vec<_> = results
        .into_iter()
        .map(|(path, title, content, title_boosted)| {
            (
                path,
                title,
                snippet::extract_snippet(&content, query, 300, title_boosted),
            )
        })
        .collect();

    if results.is_empty() {
        eprintln!("No results found for '{}'", query);
        return Ok(());
    }

    match format {
        OutputFormat::Json => print_json(&results)?,
        OutputFormat::Plain => print_results(&results),
    }

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

fn open_db() -> Result<Connection> {
    let db = db_path()?;
    Ok(Connection::open_with_flags(
        &db,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?)
}

fn show(path: &str) -> Result<()> {
    let conn = open_db()?;

    // Normalize: strip leading/trailing slashes and optional "docs/" prefix
    let path = path.trim_matches('/');
    let path = path.strip_prefix("docs/").unwrap_or(path);
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
        .query_map([path], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut msg = format!("page not found: {}", db_path);
    if !suggestions.is_empty() {
        msg.push_str("\n\nDid you mean one of these?");
        for s in &suggestions {
            msg.push_str(&format!("\n  {}", s));
        }
    }
    bail!("{}", msg)
}

fn sqlite_path() -> Result<()> {
    println!("{}", db_path()?.display());
    Ok(())
}
