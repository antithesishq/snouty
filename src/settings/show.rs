use std::io::Write;

use color_eyre::eyre::Result;
use serde_json::{Map, Value, json};

use super::Settings;
use crate::render::render_kv;
use crate::stdio;

const NOT_SET: &str = "not set";

/// Each setting's name in a settings file, its resolved value (null when
/// unset), and the text the table prints when it is unset.
fn rows(settings: &Settings) -> [(&'static str, Value, &'static str); 9] {
    [
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
    ]
}

pub fn cmd_config_show(settings: &Settings, json: bool) -> Result<()> {
    let rows = rows(settings);
    let mut stdout = stdio::stdout();
    if json {
        let object: Map<String, Value> = rows
            .into_iter()
            .map(|(name, value, _)| (name.to_owned(), value))
            .collect();
        writeln!(stdout, "{}", serde_json::to_string_pretty(&object)?)?;
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
        write!(stdout, "{}", render_kv(&text, 0))?;
    }
    Ok(())
}
