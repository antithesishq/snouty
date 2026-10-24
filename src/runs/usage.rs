//! TODO: when the server adds the usage of a run to the `runs list` response,
//! read it from there, add the run metadata, and drop the usage endpoint.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroU64;

use chrono::{DateTime, Datelike, Days, Local, Months, NaiveDate, NaiveTime};
use clap::ValueEnum;
use color_eyre::eyre::Result;
use futures_util::TryStreamExt;
use log::debug;
use serde::Serialize;

use super::{limit_note, terminal_width};
use crate::api::{AntithesisApi, UsageEntry};
use crate::cli::RunsUsageArgs;
use crate::render::{OutputOptions, sanitize};
use crate::settings::Settings;
use crate::table::{Align, Column, Table};
use crate::time::format_local;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Rollup {
    Day,
    Week,
    Month,
}

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
    /// Rounded to hundredths, so that a sum shows no floating-point noise.
    hours: f64,
}

/// No usage predates the Unix epoch, so no window reaches past it. This also
/// bounds the window for a very large `--limit`.
const FIRST_DAY: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

impl Rollup {
    /// The first day of the period that holds `date`.
    fn period_start(self, date: NaiveDate) -> NaiveDate {
        match self {
            Rollup::Day => date,
            Rollup::Week => date - Days::new(u64::from(date.weekday().num_days_from_monday())),
            Rollup::Month => date.with_day(1).expect("every month has a first day"),
        }
    }

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
            print_rows(&entries, json, Some(limit), render_usage_table)
        }
        Some(rollup) => {
            let anchor = started_before.map_or_else(Local::now, |t| t.with_timezone(&Local));
            let floor = started_after.map_or(FIRST_DAY, |t| {
                t.with_timezone(&Local).date_naive().max(FIRST_DAY)
            });
            let ends = window(rollup, anchor.date_naive(), floor, limit);
            // The endpoint documents its order as creation time, not start
            // time, so every page of the window is read and the order does not
            // matter. The bound is a day early, because the start of the oldest
            // period is local midnight; entries outside the window are dropped.
            let oldest = ends.last().map_or(floor, |end| rollup.period_start(*end));
            let bound = (oldest - Days::new(1)).and_time(NaiveTime::MIN).and_utc();
            let after = started_after.map_or(bound, |t| t.max(bound));
            let mut totals: BTreeMap<NaiveDate, (u64, f64)> =
                ends.iter().map(|end| (*end, (0, 0.0))).collect();
            let entries = api.stream_usage(Some(after), started_before, None);
            let mut entries = std::pin::pin!(entries);
            while let Some(entry) = entries.try_next().await? {
                let date = entry.start_time.with_timezone(&Local).date_naive();
                if let Some((runs, hours)) = totals.get_mut(&rollup.period_end(date)) {
                    *runs += 1;
                    *hours += entry.hours;
                }
            }
            let periods: Vec<PeriodUsage> = totals
                .into_iter()
                .rev()
                .map(|(date, (runs, hours))| PeriodUsage {
                    date,
                    runs,
                    hours: (hours * 100.0).round() / 100.0,
                })
                .collect();
            // The window always has `limit` periods, or reaches the floor, so
            // there is no limit note.
            print_rows(&periods, json, None, render_periods_table)
        }
    }
}

/// The last days of the `limit` periods that end with the period that holds
/// `anchor`, newest first. The window stops early at the period that holds
/// `floor`.
fn window(
    rollup: Rollup,
    anchor: NaiveDate,
    floor: NaiveDate,
    limit: NonZeroU64,
) -> Vec<NaiveDate> {
    let mut ends = Vec::new();
    let mut end = rollup.period_end(anchor);
    loop {
        ends.push(end);
        let start = rollup.period_start(end);
        if start <= floor || ends.len() as u64 == limit.get() {
            return ends;
        }
        end = start - Days::new(1);
    }
}

fn print_rows<T: Serialize>(
    rows: &[T],
    json: bool,
    limit: Option<NonZeroU64>,
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
    if let Some(limit) = limit {
        limit_note(rows.len(), limit, false);
    }
    Ok(())
}

/// `hours` to the nearest minute, as `Xh Ym`.
fn format_hours(hours: f64) -> String {
    let minutes = (hours * 60.0).round() as u64;
    format!("{}h {}m", minutes / 60, minutes % 60)
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

    #[test]
    fn window_counts_back_from_the_anchor_to_the_floor() {
        let limit = NonZeroU64::new(3).unwrap();
        let ends = |rollup, floor| window(rollup, date("2026-03-03"), date(floor), limit);
        let dates = |texts: &[&str]| texts.iter().map(|t| date(t)).collect::<Vec<_>>();
        assert_eq!(
            ends(Rollup::Day, "1970-01-01"),
            dates(&["2026-03-03", "2026-03-02", "2026-03-01"])
        );
        assert_eq!(
            ends(Rollup::Week, "1970-01-01"),
            dates(&["2026-03-08", "2026-03-01", "2026-02-22"])
        );
        assert_eq!(
            ends(Rollup::Month, "1970-01-01"),
            dates(&["2026-03-31", "2026-02-28", "2026-01-31"])
        );
        // The period that holds the floor is the last one.
        assert_eq!(
            ends(Rollup::Month, "2026-02-14"),
            dates(&["2026-03-31", "2026-02-28"])
        );
        assert_eq!(ends(Rollup::Day, "2026-03-03"), dates(&["2026-03-03"]));
    }

    /// The period from `start` to `end` holds `date`, and the days before
    /// `start` and after `end` are in other periods.
    #[hegel::test]
    fn period_end_closes_the_period_of_date(tc: hegel::TestCase) {
        let rollup =
            tc.draw(generators::sampled_from(Rollup::value_variants().to_vec()).print_as_debug());
        let days = tc.draw(generators::integers::<u64>().max_value(200_000));
        let date = date("1900-01-01") + Days::new(days);
        let end = rollup.period_end(date);
        let start = rollup.period_start(date);
        assert!(start <= date && date <= end);
        assert_eq!(rollup.period_start(end), start);
        assert!(rollup.period_end(start - Days::new(1)) < start);
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
