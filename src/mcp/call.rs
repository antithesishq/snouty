//! The command line of a tool call.

use std::ffi::OsString;

use clap::Arg;
use color_eyre::eyre::{Result, bail, eyre};
use rmcp::model::JsonObject;
use serde_json::Value;

use super::Globals;
use super::tools::{ParamKind, ToolName};
use crate::cli::tree;

/// The command line of a tool call, after `snouty`.
///
/// The child runs a command from the fixed tool table, never one that the
/// request names. Each param must be an exposed arg of that command. An option
/// goes in as one `--long=value` token and the positionals go after `--`, so a
/// value never becomes a flag or a subcommand.
///
/// A call without `json` passes `--detail` when the command has it and the
/// command line still parses with it: a param can conflict with `--detail`,
/// and `--detail` can require a param.
pub(crate) fn command_args(
    tool: ToolName,
    globals: &Globals,
    params: JsonObject,
) -> Result<Vec<OsString>> {
    let spec = tool.spec();
    for param in &spec.params {
        if param.required && params.get(param.id()).is_none_or(Value::is_null) {
            bail!("missing param: {}", param.id());
        }
    }
    let json = params.get("json") == Some(&Value::Bool(true));
    let mut options: Vec<OsString> = Vec::new();
    let mut positionals: Vec<(usize, Vec<String>)> = Vec::new();
    for (key, value) in params {
        let param = spec
            .param(&key)
            .ok_or_else(|| eyre!("unknown param: {key}"))?;
        let values = match (param.kind, value) {
            (ParamKind::Flag, Value::Bool(true)) => {
                options.push(format!("--{}", long(param.arg)).into());
                continue;
            }
            (ParamKind::Flag, Value::Bool(false) | Value::Null) => continue,
            (ParamKind::Flag, _) => bail!("{key}: must be true or false"),
            (_, Value::Null) => continue,
            (ParamKind::Many(_), Value::Array(items)) => items
                .into_iter()
                .map(|item| scalar(&key, item))
                .collect::<Result<_>>()?,
            (_, value) => vec![scalar(&key, value)?],
        };
        if let Some(index) = param.arg.get_index() {
            positionals.push((index, values));
        } else {
            let long = long(param.arg);
            options.extend(
                values
                    .iter()
                    .map(|value| format!("--{long}={value}").into()),
            );
        }
    }
    positionals.sort_by_key(|(index, _)| *index);
    // A missing optional positional before a given one would shift the given
    // values into the wrong args.
    if let Some((last, _)) = positionals.last() {
        for param in &spec.params {
            if param.arg.get_index().is_some_and(|index| index < *last)
                && !positionals
                    .iter()
                    .any(|(index, _)| Some(*index) == param.arg.get_index())
            {
                bail!("missing param: {}", param.id());
            }
        }
    }

    let mut argv: Vec<OsString> = Vec::new();
    if globals.verbose {
        argv.push("--verbose".into());
    }
    if let Some(settings) = &globals.settings {
        let mut flag = OsString::from("--settings=");
        flag.push(settings);
        argv.push(flag);
    }
    if let Some(profile) = &globals.profile {
        argv.push(format!("--profile={profile}").into());
    }
    argv.extend(tool.path().iter().map(OsString::from));
    argv.extend(options);
    let separator = argv.len();
    argv.push("--".into());
    argv.extend(
        positionals
            .into_iter()
            .flat_map(|(_, values)| values)
            .map(OsString::from),
    );
    if spec.supports_detail && !json {
        let mut with_detail = argv.clone();
        with_detail.insert(separator, "--detail".into());
        if parses(&with_detail) {
            argv = with_detail;
        }
    }
    Ok(argv)
}

/// Whether snouty's clap definition accepts `snouty <argv>`.
fn parses(argv: &[OsString]) -> bool {
    tree::root()
        .clone()
        .try_get_matches_from(std::iter::once(OsString::from("snouty")).chain(argv.iter().cloned()))
        .is_ok()
}

fn long(arg: &Arg) -> &str {
    arg.get_long()
        .unwrap_or_else(|| panic!("{}: an option has a long name", arg.get_id()))
}

/// The largest magnitude at which every integer has an exact f64.
const MAX_EXACT_FLOAT: f64 = 9_007_199_254_740_992.0;

fn scalar(key: &str, value: Value) -> Result<String> {
    let text = match value {
        Value::String(text) => text,
        Value::Number(number) if number.is_i64() || number.is_u64() => number.to_string(),
        // serde_json reads an integer too large for u64 as an f64, which has
        // lost digits by now.
        Value::Number(number)
            if number
                .as_f64()
                .is_some_and(|float| float.abs() < MAX_EXACT_FLOAT) =>
        {
            number.to_string()
        }
        Value::Number(_) => bail!("{key}: the number is too large, pass it as a string"),
        _ => bail!("{key}: must be a string or a number"),
    };
    if text.contains('\0') {
        bail!("{key}: must not contain NUL");
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use clap::Parser;
    use hegel::generators;
    use serde_json::json;

    use super::*;
    use crate::cli::{Cli, Commands, RunsCommands};
    use crate::mcp::object;

    fn globals() -> Globals {
        Globals {
            settings: None,
            profile: None,
            verbose: false,
            max_concurrent_calls: NonZeroUsize::new(8).unwrap(),
        }
    }

    fn args(name: ToolName, globals: &Globals, params: Value) -> Result<Vec<OsString>> {
        command_args(name, globals, object(params))
    }

    /// Parses a tool call's command line as snouty would.
    fn parse(argv: Vec<OsString>) -> Cli {
        Cli::try_parse_from(std::iter::once(OsString::from("snouty")).chain(argv))
            .expect("the command line parses")
    }

    /// An option value reaches its arg unchanged, whatever its text, and
    /// cannot set another flag.
    #[hegel::test]
    fn an_option_value_stays_one_value(tc: hegel::TestCase) {
        // A NUL is refused, as `bad_params_are_refused_before_any_command`
        // checks.
        let value = tc.draw(generators::text().exclude_characters("\0"));
        let argv = args(ToolName::RunsList, &globals(), json!({ "launcher": value }));
        let cli = parse(argv.unwrap());
        assert!(!cli.json && cli.settings.is_none() && cli.profile.is_none());
        let Commands::Runs {
            command: Some(RunsCommands::List(args)),
        } = cli.command
        else {
            panic!("not runs list");
        };
        assert_eq!(args.launcher.as_deref(), Some(value.as_str()));
    }

    /// A positional value reaches its arg unchanged, whatever its text, and
    /// cannot set another flag or name another command.
    #[hegel::test]
    fn a_positional_value_stays_one_value(tc: hegel::TestCase) {
        let script = tc.draw(generators::text().min_size(1).exclude_characters("\0"));
        let call = json!({
            "run_id": "r", "input_hash": "-1", "vtime": 1.5, "script": script,
        });
        let cli = parse(args(ToolName::RunsExec, &globals(), call).unwrap());
        assert!(!cli.json && cli.settings.is_none() && cli.profile.is_none());
        let Commands::Runs {
            command:
                Some(RunsCommands::Exec {
                    script: parsed,
                    input_hash,
                    ..
                }),
        } = cli.command
        else {
            panic!("not runs exec");
        };
        assert_eq!(parsed.as_deref(), Some(script.as_str()));
        assert_eq!(input_hash, "-1");
    }

    #[test]
    fn the_server_globals_reach_the_child() {
        let globals = Globals {
            settings: Some("s.toml".into()),
            profile: Some("p".to_owned()),
            verbose: true,
            max_concurrent_calls: NonZeroUsize::new(1).unwrap(),
        };
        let cli = parse(args(ToolName::Doctor, &globals, json!({})).unwrap());
        assert_eq!(
            cli.settings.as_deref(),
            Some(std::path::Path::new("s.toml"))
        );
        assert_eq!(cli.profile.as_deref(), Some("p"));
        assert!(cli.verbose);
    }

    #[test]
    fn bad_params_are_refused_before_any_command() {
        let cases = [
            (
                ToolName::RunsShow,
                json!({ "run_id": "r", "web": true }),
                "unknown param: web",
            ),
            (
                ToolName::RunsList,
                json!({ "settings": "x" }),
                "unknown param: settings",
            ),
            (
                ToolName::RunsList,
                json!({ "detail": true }),
                "unknown param: detail",
            ),
            (
                ToolName::DocsShow,
                json!({ "path": "p", "json": true }),
                "unknown param: json",
            ),
            (
                ToolName::RunsSearch,
                json!({ "run_id": "r", "query": "q", "count": "yes" }),
                "count: must be true or false",
            ),
            (
                ToolName::RunsList,
                json!({ "launcher": ["a"] }),
                "launcher: must be a string or a number",
            ),
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "-1", "vtime": 1 }),
                "missing param: script",
            ),
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "-1", "script": "ls" }),
                "missing param: vtime",
            ),
            (ToolName::RunsShow, json!({}), "missing param: run_id"),
            (
                ToolName::RunsLogs,
                json!({ "run_id": "r", "vtime": 1 }),
                "missing param: input_hash",
            ),
            (
                ToolName::RunsList,
                json!({ "launcher": "x\u{0}y" }),
                "launcher: must not contain NUL",
            ),
            (
                ToolName::RunsShow,
                json!({ "run_id": 12345678901234567890123.0 }),
                "run_id: the number is too large, pass it as a string",
            ),
            (
                ToolName::RunsList,
                json!({ "limit": 1e300 }),
                "limit: the number is too large, pass it as a string",
            ),
        ];
        for (name, call, error) in cases {
            let result = args(name, &globals(), call.clone());
            let error_text = result.err().map(|e| e.to_string());
            assert_eq!(error_text.as_deref(), Some(error), "{name} {call}");
        }
    }

    /// A call prints detailed text output, or JSON with `json`.
    #[test]
    fn json_picks_the_output() {
        let cases = [
            (
                ToolName::RunsProperties,
                json!({ "run_id": "r" }),
                "runs properties --detail",
            ),
            (
                ToolName::RunsProperties,
                json!({ "run_id": "r", "json": false }),
                "runs properties --detail",
            ),
            (
                ToolName::RunsProperties,
                json!({ "run_id": "r", "json": true }),
                "runs properties --json",
            ),
            // `count` conflicts with `--detail`.
            (
                ToolName::RunsSearch,
                json!({ "run_id": "r", "query": "q", "count": true }),
                "runs search --count",
            ),
            (
                ToolName::RunsSearch,
                json!({ "run_id": "r", "query": "q", "count": false }),
                "runs search --detail",
            ),
            (ToolName::RunsShow, json!({ "run_id": "r" }), "runs show"),
            // `--detail` requires `--events`.
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "h", "vtime": 1, "script": "s" }),
                "runs exec",
            ),
            (
                ToolName::RunsExec,
                json!({ "run_id": "r", "input_hash": "h", "vtime": 1, "script": "s", "events": true }),
                "runs exec --events --detail",
            ),
        ];
        for (name, call, line) in cases {
            let argv = args(name, &globals(), call.clone()).unwrap();
            let options: Vec<&str> = argv
                .iter()
                .map(|arg| arg.to_str().unwrap())
                .take_while(|arg| *arg != "--")
                .collect();
            assert_eq!(options.join(" "), line, "{name} {call}");
            parse(argv);
        }
    }

    #[test]
    fn debug_params_reach_the_command() {
        let call = json!({
            "run_id": "r", "input_hash": "-5", "vtime": 2.5, "param": ["a=b", "c=d"],
        });
        let cli = parse(args(ToolName::Debug, &globals(), call).unwrap());
        let Commands::Debug(debug) = cli.command else {
            panic!("not debug");
        };
        assert_eq!(debug.run_id.as_deref(), Some("r"));
        assert_eq!(debug.input_hash, "-5");
        assert_eq!(debug.params.len(), 2);
    }
}
