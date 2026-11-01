use std::io::Write;

use color_eyre::eyre::Result;
use serde_json::{Map, Value, json};

use super::{Settings, exact_byte_size};
use crate::features::{self, Feature};
use crate::render::{render_kv, sanitize};
use crate::stdio;
use crate::table::{Column, Table};

const NOT_SET: &str = "not set";

/// Each setting's name in a settings file, its resolved value (null when
/// unset), and the text the table prints for it.
/// A `features` row follows when an unstable feature is on; an empty row would
/// be noise on every ordinary run.
fn rows(settings: &Settings, features: &[Feature]) -> Vec<(&'static str, Value, String)> {
    fn text_row(
        name: &'static str,
        value: Option<&str>,
        unset: &str,
    ) -> (&'static str, Value, String) {
        (name, json!(value), value.unwrap_or(unset).to_owned())
    }

    let max_file_size = settings.api_cache_max_file_size();
    let mut rows = vec![
        text_row("profile", settings.profile(), "default"),
        text_row("tenant", settings.tenant(), NOT_SET),
        text_row("repository", settings.repository(), NOT_SET),
        text_row("base_url", settings.base_url(), NOT_SET),
        text_row("https_proxy", settings.https_proxy(), NOT_SET),
        text_row(
            "container_engine",
            settings.container_engine(),
            "auto-detect",
        ),
        text_row(
            "update_channel",
            Some(settings.update_channel().as_str()),
            NOT_SET,
        ),
        (
            "api_cache_max_file_size",
            json!(max_file_size),
            exact_byte_size(max_file_size),
        ),
    ];
    if !features.is_empty() {
        let ids: Vec<String> = features.iter().map(Feature::to_string).collect();
        rows.push(text_row("features", Some(&ids.join(", ")), NOT_SET));
    }
    rows
}

pub fn cmd_settings_show(settings: &Settings, json: bool) -> Result<()> {
    let rows = rows(settings, &features::enabled());
    let mut stdout = stdio::stdout();
    if json {
        let object: Map<String, Value> = rows
            .into_iter()
            .map(|(name, value, _)| (name.to_owned(), value))
            .collect();
        let report = json!({ "settings": object, "files": settings.files() });
        writeln!(stdout, "{}", serde_json::to_string_pretty(&report)?)?;
    } else {
        let text: Vec<(&str, String)> = rows
            .into_iter()
            .map(|(name, _, text)| (name, text))
            .collect();
        write!(stdout, "{}", render_kv(&text, 0))?;
        let files = settings.files();
        if files.is_empty() {
            writeln!(stdout, "\nsettings files: none")?;
        } else {
            let rows = files
                .iter()
                .map(|file| {
                    vec![
                        file.file_type.to_string(),
                        sanitize(&file.path.display().to_string()),
                    ]
                })
                .collect();
            // Both columns are fixed, so the table never needs the terminal
            // width.
            let table = Table::builder()
                .column(Column::new("TYPE"))
                .column(Column::new("PATH"))
                .render(rows, usize::MAX);
            writeln!(stdout, "\n{table}")?;
        }
    }
    Ok(())
}
