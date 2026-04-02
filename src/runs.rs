use std::io::Write;

use color_eyre::eyre::{Result, eyre};
use futures_util::TryStreamExt;
use log::info;

use crate::api::{AntithesisApi, RunDetail, RunStatus, RunSummary, RunsFilterOptions};
use crate::cli::{RunsCommands, RunsListArgs};

pub async fn cmd_runs(command: Option<RunsCommands>) -> Result<()> {
    match command {
        None => cmd_runs_list(RunsListArgs::default()).await,
        Some(RunsCommands::List(args)) => cmd_runs_list(args).await,
        Some(RunsCommands::Show { run_id, json }) => cmd_runs_show(&run_id, json).await,
        Some(RunsCommands::BuildLogs { run_id, json }) => cmd_runs_build_logs(&run_id, json).await,
        Some(RunsCommands::Logs {
            run_id,
            input_hash,
            vtime,
            begin_vtime,
            begin_input_hash,
            disable_default_log_filter,
            json,
        }) => {
            cmd_runs_logs(
                &run_id,
                &input_hash,
                &vtime,
                begin_input_hash.as_deref(),
                begin_vtime.as_deref(),
                disable_default_log_filter,
                json,
            )
            .await
        }
    }
}

async fn cmd_runs_list(args: RunsListArgs) -> Result<()> {
    info!("listing runs");

    let api = AntithesisApi::from_env()?;

    let status = args
        .status
        .as_deref()
        .map(|s| s.parse::<RunStatus>())
        .transpose()
        .map_err(|_| {
            eyre!(
                "invalid status: '{}'\nvalid values: starting, in_progress, debugger_ready, completed, cancelled, failed",
                args.status.as_deref().unwrap_or_default()
            )
        })?;

    let opts = RunsFilterOptions {
        status,
        launcher: args.launcher,
        created_after: args
            .created_after
            .as_deref()
            .map(|s| s.parse())
            .transpose()
            .map_err(|e| eyre!("invalid --created-after timestamp: {e}"))?,
        created_before: args
            .created_before
            .as_deref()
            .map(|s| s.parse())
            .transpose()
            .map_err(|e| eyre!("invalid --created-before timestamp: {e}"))?,
    };

    let has_filters = opts.status.is_some()
        || opts.launcher.is_some()
        || opts.created_after.is_some()
        || opts.created_before.is_some();

    let mut runs: Vec<RunSummary> = if has_filters {
        api.stream_runs_filtered(&opts)
            .try_collect::<Vec<_>>()
            .await?
    } else {
        api.stream_runs().try_collect::<Vec<_>>().await?
    };

    // Apply client-side limit
    runs.truncate(args.limit);

    if runs.is_empty() {
        println!("No runs found.");
        return Ok(());
    }

    runs.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then(a.status.cmp(&b.status))
    });

    println!("{}", render_runs_table(&runs));
    Ok(())
}

async fn cmd_runs_show(run_id: &str, json: bool) -> Result<()> {
    info!("showing run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let run = api.get_run(run_id).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&run)?);
    } else {
        print_run_detail(&run);
    }

    Ok(())
}

fn print_run_detail(run: &RunDetail) {
    let mut rows: Vec<(&str, String)> = vec![
        ("Run ID", run.run_id.clone()),
        ("Status", render_enum(&run.status)),
    ];

    if let Some(ref t) = run.type_ {
        rows.push(("Type", render_enum(t)));
    }

    rows.push(("Created", run.created_at.to_rfc3339()));

    if let Some(ref t) = run.started_at {
        rows.push(("Started", t.to_rfc3339()));
    }
    if let Some(ref t) = run.completed_at {
        rows.push(("Completed", t.to_rfc3339()));
    }

    rows.push(("Launcher", run.launcher.clone()));

    if let Some(ref links) = run.links {
        if let Some(ref url) = links.triage_report {
            rows.push(("Report", url.clone()));
        }
    }

    if let Some(ref creator) = run.creator {
        if let Some(ref name) = creator.name {
            rows.push(("Creator", name.clone()));
        }
    }

    let label_width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    for (label, value) in &rows {
        println!("{label:label_width$}  {value}");
    }
}

async fn cmd_runs_build_logs(run_id: &str, json: bool) -> Result<()> {
    info!("streaming build logs for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let response = api.get_run_build_logs(run_id).await?;
    let mut stdout = std::io::stdout().lock();

    if json {
        stream_ndjson_lines(response, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        stream_ndjson_lines(response, |line| {
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                let ts = entry["timestamp"].as_str().unwrap_or("");
                let stream = entry["stream"].as_str().unwrap_or("out");
                let text = entry["text"].as_str().unwrap_or("");
                writeln!(stdout, "{ts} [{stream}] {text}")?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    }
}

async fn cmd_runs_logs(
    run_id: &str,
    input_hash: &str,
    vtime: &str,
    begin_input_hash: Option<&str>,
    begin_vtime: Option<&str>,
    disable_default_log_filter: bool,
    json: bool,
) -> Result<()> {
    info!("streaming logs for run: {}", run_id);

    let api = AntithesisApi::from_env()?;
    let response = api
        .get_run_logs(
            run_id,
            input_hash,
            vtime,
            begin_input_hash,
            begin_vtime,
            disable_default_log_filter,
        )
        .await?;

    let mut stdout = std::io::stdout().lock();
    if json {
        stream_ndjson_lines(response, |line| {
            writeln!(stdout, "{line}")?;
            Ok(())
        })
        .await
    } else {
        writeln!(stdout, "{:<22}  {:<20}  {}", "VTIME", "SOURCE", "OUTPUT")?;
        stream_ndjson_lines(response, |line| {
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                let vtime = entry["moment"]["vtime"].as_str().unwrap_or("");
                let container = entry["source"]["container"].as_str().unwrap_or("");
                let stream = entry["source"]["stream"].as_str().unwrap_or("");
                let source = format!("[{container}:{stream}]");
                let output = entry["output_text"].as_str().unwrap_or("");
                writeln!(stdout, "{vtime:<22}  {source:<20}  {output}")?;
            } else {
                writeln!(stdout, "{line}")?;
            }
            Ok(())
        })
        .await
    }
}

async fn stream_ndjson_lines(
    response: reqwest::Response,
    mut process_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    use futures_util::StreamExt;

    let mut stream = response.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let text = std::str::from_utf8(&chunk)
            .map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
        buf.push_str(text);

        while let Some(pos) = buf.find('\n') {
            let line = &buf[..pos];
            if !line.is_empty() {
                process_line(line)?;
            }
            buf = buf[pos + 1..].to_string();
        }
    }

    // Process any remaining data without a trailing newline
    if !buf.is_empty() {
        process_line(&buf)?;
    }

    Ok(())
}

fn render_runs_table(runs: &[RunSummary]) -> String {
    let headers = vec![
        "RUN ID".to_string(),
        "STATUS".to_string(),
        "TYPE".to_string(),
        "CREATED AT".to_string(),
        "LAUNCHER".to_string(),
    ];
    let rows = runs
        .iter()
        .map(|run| {
            vec![
                run.run_id.clone(),
                render_enum(&run.status),
                run.type_.as_ref().map(render_enum).unwrap_or_default(),
                run.created_at.to_rfc3339(),
                run.launcher.clone(),
            ]
        })
        .collect::<Vec<_>>();

    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let mut output = String::new();
    push_table_row(&mut output, &headers, &widths);
    for row in rows {
        push_table_row(&mut output, &row, &widths);
    }

    output.trim_end().to_string()
}

fn push_table_row(output: &mut String, row: &[String], widths: &[usize]) {
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            output.push_str("  ");
        }
        output.push_str(&format!("{cell:<width$}", width = widths[index]));
    }
    output.push('\n');
}

fn render_enum(value: &impl serde::Serialize) -> String {
    let json = serde_json::to_string(value).unwrap_or_default();
    json.trim_matches('"').to_string()
}
