//! The settings that name one of a fixed set of values.

use std::{fmt, str::FromStr};

use clap::ValueEnum;

/// Which releases `snouty update` considers when no explicit version is given.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, ValueEnum)]
pub enum UpdateChannel {
    /// Install the latest release
    #[default]
    Stable,
    /// Also consider pre-releases, but prefer the latest release when it is newer
    Unstable,
}

/// A container engine that snouty can use, in the order that auto-detection
/// tries them.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum ContainerEngine {
    Podman,
    Docker,
}

impl fmt::Display for UpdateChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_name(self, f)
    }
}

impl FromStr for UpdateChannel {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        parse_name(text)
    }
}

impl fmt::Display for ContainerEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_name(self, f)
    }
}

impl FromStr for ContainerEngine {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        parse_name(text)
    }
}

/// Write the name of `value`.
fn write_name<E: ValueEnum>(value: &E, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(
        value
            .to_possible_value()
            .expect("no variant is skipped")
            .get_name(),
    )
}

/// The variant named `text`; an error that lists every name.
fn parse_name<E: ValueEnum + fmt::Display>(text: &str) -> Result<E, String> {
    E::from_str(text, false).map_err(|_| {
        let names: Vec<String> = E::value_variants()
            .iter()
            .map(|value| format!("`{value}`"))
            .collect();
        let names = match names.split_last() {
            Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
            _ => names.concat(),
        };
        format!("expected {names}, got `{text}`")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trips<E: ValueEnum + fmt::Display + FromStr<Err = String> + PartialEq + fmt::Debug>() {
        for value in E::value_variants() {
            assert_eq!(value.to_string().parse::<E>().as_ref(), Ok(value));
        }
    }

    #[test]
    fn each_name_parses_to_its_value() {
        round_trips::<UpdateChannel>();
        round_trips::<ContainerEngine>();
    }

    #[test]
    fn an_unknown_name_lists_the_names() {
        assert_eq!(
            "nightly".parse::<UpdateChannel>().unwrap_err(),
            "expected `stable` or `unstable`, got `nightly`"
        );
        assert_eq!(
            "none".parse::<ContainerEngine>().unwrap_err(),
            "expected `podman` or `docker`, got `none`"
        );
    }
}
