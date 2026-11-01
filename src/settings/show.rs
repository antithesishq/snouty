use std::io::Write;

use color_eyre::eyre::Result;
use serde_json::{Map, Value, json};

use super::Settings;
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

/// `bytes` in the largest unit that holds it exactly, such as `10 MiB` or
/// `10 MB`, so that a size prints as the user wrote it. The `Display` of
/// `ByteSize` rounds: it prints a size set as `10 MB` as `9.5 MiB`.
fn exact_byte_size(bytes: u64) -> String {
    const UNITS: [(u64, &str); 10] = [
        (1 << 50, "PiB"),
        (1_000_000_000_000_000, "PB"),
        (1 << 40, "TiB"),
        (1_000_000_000_000, "TB"),
        (1 << 30, "GiB"),
        (1_000_000_000, "GB"),
        (1 << 20, "MiB"),
        (1_000_000, "MB"),
        (1 << 10, "KiB"),
        (1_000, "KB"),
    ];
    UNITS
        .iter()
        .find(|(size, _)| bytes != 0 && bytes.is_multiple_of(*size))
        .map_or_else(
            || format!("{bytes} B"),
            |(size, unit)| format!("{} {unit}", bytes / size),
        )
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

#[cfg(test)]
mod tests {
    use hegel::generators;

    use super::exact_byte_size;
    use crate::settings::parse_byte_size;

    #[test]
    fn byte_sizes_print_in_the_largest_exact_unit() {
        assert_eq!(exact_byte_size(10 * 1024 * 1024), "10 MiB");
        assert_eq!(exact_byte_size(10_000_000), "10 MB");
        assert_eq!(exact_byte_size(1_500), "1500 B");
        assert_eq!(exact_byte_size(0), "0 B");
    }

    #[hegel::test]
    fn a_printed_byte_size_parses_back_to_the_same_size(tc: hegel::TestCase) {
        let bytes = tc.draw(generators::integers::<u64>());
        assert_eq!(
            parse_byte_size("setting", &exact_byte_size(bytes)).unwrap(),
            bytes
        );
    }
}
