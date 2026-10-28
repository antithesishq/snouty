//! `snouty config show`.

use std::io::Write;

use color_eyre::eyre::Result;
use serde_json::{Map, Value, json};

use super::Settings;
use crate::render::render_kv;
use crate::stdio;

/// Each setting, keyed by its name in a settings file, with its resolved
/// value; null when it is not set.
fn rows(settings: &Settings) -> [(&'static str, Value); 9] {
    [
        ("profile", json!(settings.profile())),
        ("tenant", json!(settings.tenant())),
        ("repository", json!(settings.repository())),
        ("base_url", json!(settings.base_url())),
        ("https_proxy", json!(settings.https_proxy())),
        ("container_engine", json!(settings.container_engine())),
        ("update_channel", json!(settings.update_channel().as_str())),
        (
            "api_cache_max_file_size",
            json!(settings.api_cache_max_file_size()),
        ),
        (
            "api_cache_respect_headers",
            json!(settings.api_cache_respect_headers()),
        ),
    ]
}

/// The human table's word for a setting that is not set: what snouty does
/// without it.
fn unset_text(name: &str) -> &'static str {
    match name {
        "profile" => "default",
        "container_engine" => "auto-detect",
        _ => "not set",
    }
}

pub fn cmd_config_show(settings: &Settings, json: bool) -> Result<()> {
    let rows = rows(settings);
    let mut stdout = stdio::stdout();
    if json {
        let object: Map<String, Value> = rows
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        writeln!(stdout, "{}", serde_json::to_string_pretty(&object)?)?;
    } else {
        let text: Vec<(&str, String)> = rows
            .into_iter()
            .map(|(name, value)| {
                let text = match value {
                    Value::Null => unset_text(name).to_owned(),
                    Value::String(text) => text,
                    other => other.to_string(),
                };
                (name, text)
            })
            .collect();
        write!(stdout, "{}", render_kv(&text, 0))?;
    }
    Ok(())
}
