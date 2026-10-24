//! `snouty runs usage`: the usage hours of recent runs, one row per run, or
//! one row per period with `--rollup`.
//!
//! TODO: when the server adds the usage of a run to the `runs list` response,
//! read it from there, add the run metadata, and drop the usage endpoint.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::io::Write;
use std::num::NonZeroU64;

use chrono::{DateTime, Datelike, Days, Local, Months, NaiveDate};
use clap::ValueEnum;
use color_eyre::eyre::Result;
use futures_util::{Stream, TryStreamExt};
use log::debug;
use serde::Serialize;

use super::{limit_note, terminal_width};
use crate::api::{AntithesisApi, UsageEntry};
use crate::cli::RunsUsageArgs;
use crate::render::{OutputOptions, sanitize};
use crate::settings::Settings;
use crate::table::{Align, Column, Table};
use crate::time::format_local;

/// The period that `runs usage --rollup` totals runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Rollup {
    Day,
    Week,
    Month,
}

/// The `--json` row of a run: the usage entry, with its times in the local
/// timezone.
#[derive(Serialize)]
struct LocalUsageEntry {
    run_id: String,
    hours: f64,
    start_time: DateTime<Local>,
    end_time: DateTime<Local>,
}

impl From<UsageEntry> for LocalUsageEntry {
    fn from(entry: UsageEntry) -> Self {
        Self {
            run_id: entry.run_id,
            hours: entry.hours,
            start_time: entry.start_time.with_timezone(&Local),
            end_time: entry.end_time.with_timezone(&Local),
        }
    }
}

/// The total usage of the runs that started in one period.
#[derive(Serialize)]
struct PeriodUsage {
    /// The last day of the period.
    date: NaiveDate,
    runs: u64,
    hours: f64,
}

impl Rollup {
    /// The last day of the period that holds `date`. A period that ends after
    /// the last representable date ends on that date.
    fn period_end(self, date: NaiveDate) -> NaiveDate {
        match self {
            Rollup::Day => Some(date),
            Rollup::Week => date.checked_add_days(Days::new(
                6 - u64::from(date.weekday().num_days_from_monday()),
            )),
            Rollup::Month => date
                .with_day(1)
                .and_then(|first| first.checked_add_months(Months::new(1)))
                .and_then(|next| next.pred_opt()),
        }
        .unwrap_or(NaiveDate::MAX)
    }
}

pub(super) async fn cmd_runs_usage(
    args: RunsUsageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("listing usage");
    let api = AntithesisApi::new(settings, verbose)?;
    let RunsUsageArgs {
        rollup,
        started_after,
        started_before,
        limit,
    } = args;

    match rollup {
        None => {
            let entries: Vec<LocalUsageEntry> = api
                .stream_usage(started_after, started_before, Some(limit))
                .map_ok(LocalUsageEntry::from)
                .try_collect()
                .await?;
            print_rows(&entries, json, limit, render_usage_table)
        }
        Some(rollup) => {
            // A period is complete only when an older period starts, so the
            // stream has no limit, and the limit applies to periods here.
            let entries = api.stream_usage(started_after, started_before, None);
            let periods = total_by_period(entries, rollup, limit).await?;
            print_rows(&periods, json, limit, render_periods_table)
        }
    }
}

/// Print `rows` as JSON lines, or as the table that `render` makes.
fn print_rows<T: Serialize>(
    rows: &[T],
    json: bool,
    limit: NonZeroU64,
    render: fn(&[T], usize) -> String,
) -> Result<()> {
    if json {
        for row in rows {
            outln!("{}", serde_json::to_string(row)?)?;
        }
        return Ok(());
    }
    if rows.is_empty() {
        outln!("No usage found.")?;
        return Ok(());
    }
    outln!("{}", render(rows, terminal_width()))?;
    limit_note(rows.len(), limit, false);
    Ok(())
}

/// Totals `entries` by the period, in the local timezone, in which each one
/// started, newest period first. Reading stops at the first entry of a period
/// past the `limit` newest, so that no page past it is fetched. That needs the
/// entries newest first, as the usage endpoint sends them.
async fn total_by_period(
    entries: impl Stream<Item = Result<UsageEntry>>,
    rollup: Rollup,
    limit: NonZeroU64,
) -> Result<Vec<PeriodUsage>> {
    let mut entries = std::pin::pin!(entries);
    // The early stop needs the entries in start-time order, which only the
    // cursor of the endpoint shows. The map keeps a small disorder inside the
    // periods it reads from printing one date twice.
    let mut totals: BTreeMap<NaiveDate, (u64, f64)> = BTreeMap::new();
    while let Some(entry) = entries.try_next().await? {
        let date = rollup.period_end(entry.start_time.with_timezone(&Local).date_naive());
        let full = totals.len() as u64 == limit.get();
        match totals.entry(date) {
            Entry::Occupied(mut total) => {
                let (runs, hours) = total.get_mut();
                *runs += 1;
                *hours += entry.hours;
            }
            Entry::Vacant(_) if full => break,
            Entry::Vacant(total) => {
                total.insert((1, entry.hours));
            }
        }
    }
    Ok(totals
        .into_iter()
        .rev()
        .map(|(date, (runs, hours))| PeriodUsage { date, runs, hours })
        .collect())
}

fn format_hours(hours: f64) -> String {
    format!("{hours:.2}")
}

fn render_usage_table(entries: &[LocalUsageEntry], width: usize) -> String {
    let rows = entries
        .iter()
        .map(|entry| {
            vec![
                sanitize(&entry.run_id),
                format_hours(entry.hours),
                format_local(entry.start_time),
                format_local(entry.end_time),
            ]
        })
        .collect();
    Table::builder()
        .column(Column::new("RUN ID"))
        .column(Column::new("HOURS").align(Align::Right))
        .column(Column::new("STARTED"))
        .column(Column::new("ENDED"))
        .render(rows, width)
}

fn render_periods_table(periods: &[PeriodUsage], width: usize) -> String {
    let rows = periods
        .iter()
        .map(|period| {
            vec![
                period.date.to_string(),
                period.runs.to_string(),
                format_hours(period.hours),
            ]
        })
        .collect();
    Table::builder()
        .column(Column::new("DATE"))
        .column(Column::new("RUNS").align(Align::Right))
        .column(Column::new("HOURS").align(Align::Right))
        .render(rows, width)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators::{self, Generator};

    fn date(text: &str) -> NaiveDate {
        text.parse().unwrap()
    }

    #[test]
    fn period_end_is_the_last_day_of_the_period() {
        // 2026-10-21 is a Wednesday.
        let wednesday = date("2026-10-21");
        assert_eq!(Rollup::Day.period_end(wednesday), wednesday);
        assert_eq!(Rollup::Week.period_end(wednesday), date("2026-10-25"));
        assert_eq!(Rollup::Month.period_end(wednesday), date("2026-10-31"));
        assert_eq!(
            Rollup::Month.period_end(date("2028-02-03")),
            date("2028-02-29")
        );
        assert_eq!(
            Rollup::Week.period_end(date("2026-10-25")),
            date("2026-10-25")
        );
    }

    /// The period that ends on `end` holds `date` and `end`, and the day after
    /// `end` starts a new period.
    #[hegel::test]
    fn period_end_closes_the_period_of_date(tc: hegel::TestCase) {
        let rollup =
            tc.draw(generators::sampled_from(Rollup::value_variants().to_vec()).print_as_debug());
        let days = tc.draw(generators::integers::<u64>().max_value(200_000));
        let date = date("1900-01-01") + Days::new(days);
        let end = rollup.period_end(date);
        assert!(end >= date);
        assert_eq!(rollup.period_end(end), end);
        let next = end.succ_opt().unwrap();
        assert!(rollup.period_end(next) > end);
        match rollup {
            Rollup::Day => assert_eq!(end, date),
            Rollup::Week => assert_eq!(next.weekday(), chrono::Weekday::Mon),
            Rollup::Month => assert_eq!(next.day(), 1),
        }
        assert!((end - date).num_days() < 31);
    }
}
