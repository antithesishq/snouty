use std::io::Write;

use color_eyre::eyre::Result;
use serde_json::{Map, Value, json};

use super::{Settings, SettingsFiles};
use crate::features::{self, Feature};
use crate::render::render_kv;
use crate::stdio;

const NOT_SET: &str = "not set";

/// Each setting's name in a settings file, its resolved value (null when
/// unset), and the text the table prints when it is unset.
/// A `features` row follows when an unstable feature is on; an empty row would
/// be noise on every ordinary run.
fn rows(settings: &Settings, features: &[Feature]) -> Vec<(&'static str, Value, &'static str)> {
    let mut rows = vec![
        ("profile", json!(settings.profile()), "default"),
        ("tenant", json!(settings.tenant()), NOT_SET),
        ("repository", json!(settings.repository()), NOT_SET),
        ("base_url", json!(settings.base_url()), NOT_SET),
        ("https_proxy", json!(settings.https_proxy()), NOT_SET),
        (
            "container_engine",
            json!(settings.container_engine()),
            "auto-detect",
        ),
        (
            "update_channel",
            json!(settings.update_channel().as_str()),
            NOT_SET,
        ),
        (
            "api_cache_max_file_size",
            json!(settings.api_cache_max_file_size()),
            NOT_SET,
        ),
        (
            "api_cache_respect_headers",
            json!(settings.api_cache_respect_headers()),
            NOT_SET,
        ),
    ];
    if !features.is_empty() {
        let ids: Vec<String> = features.iter().map(Feature::to_string).collect();
        rows.push(("features", json!(ids.join(", ")), NOT_SET));
    }
    rows
}

/// The human table's rows for the settings files: each file's path, or
/// `none` when it does not exist.
fn file_rows(files: &SettingsFiles) -> [(&'static str, String); 2] {
    [
        (
            "project file",
            files.project.as_ref().map_or_else(
                || "none".to_owned(),
                |file| format!("{} ({})", file.path.display(), file.source),
            ),
        ),
        (
            "global file",
            files
                .global
                .as_ref()
                .map_or_else(|| "none".to_owned(), |path| path.display().to_string()),
        ),
    ]
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
            .map(|(name, value, unset)| {
                let text = match value {
                    Value::Null => unset.to_owned(),
                    Value::String(text) => text,
                    other => other.to_string(),
                };
                (name, text)
            })
            .collect();
        let files = file_rows(settings.files());
        // One label width for both blocks, so that their values line up.
        let width = text
            .iter()
            .map(|(name, _)| name.len())
            .chain(files.iter().map(|(name, _)| name.len()))
            .max()
            .unwrap_or(0);
        write!(
            stdout,
            "{}\n{}",
            render_kv(&text, width),
            render_kv(&files, width)
        )?;
    }
    Ok(())
}
