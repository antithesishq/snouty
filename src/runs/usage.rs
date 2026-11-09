//! TODO: when the server adds the usage of a run to the `runs list` response,
//! read it from there, add the run metadata, and drop the usage endpoint.

use std::collections::BTreeMap;
use std::io::Write;

use chrono::{DateTime, Datelike, Days, Months, NaiveDate, TimeDelta, Utc};
use clap::ValueEnum;
use color_eyre::Section;
use color_eyre::eyre::Result;
use futures_util::{StreamExt, TryStreamExt, stream};
use log::debug;
use serde::Serialize;

use super::terminal_width;
use crate::api::{AntithesisApi, UsageEntry};
use crate::cli::RunsUsageArgs;
use crate::error::user_error;
use crate::render::{OutputOptions, sanitize};
use crate::settings::Settings;
use crate::table::{Align, Column, Table};
use crate::time::{format_timestamp_for_display, local_calendar_date, start_of_local_calendar_day};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Rollup {
    Day,
    Week,
    Month,
}

/// The usage entry of a run, as a row of the output. The JSON keeps the
/// fields in the order of the API schema.
#[derive(Serialize)]
struct UsageRow {
    run_id: String,
    hours: f64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

impl From<UsageEntry> for UsageRow {
    fn from(entry: UsageEntry) -> Self {
        Self {
            run_id: entry.run_id,
            hours: entry.hours,
            start_time: entry.start_time,
            end_time: entry.end_time,
        }
    }
}

/// The total usage of the runs that started in one period.
#[derive(Serialize)]
struct PeriodUsage {
    /// The first day of the period. The table shows it; the JSON leaves it
    /// out, because `date` with the rollup names the period.
    #[serde(skip)]
    start: NaiveDate,
    /// The last day of the period.
    date: NaiveDate,
    runs: u64,
    /// The exact sum. The JSON rounds it to hundredths, so that it shows no
    /// floating-point noise; the table rounds it once, to one decimal.
    #[serde(serialize_with = "serialize_hundredths")]
    hours: f64,
}

// `serialize_with` passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_hundredths<S: serde::Serializer>(
    hours: &f64,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64((hours * 100.0).round() / 100.0)
}

/// No usage predates the Unix epoch, so no period does. This also bounds the
/// number of periods for a very old `--after`.
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

/// Where the output starts when `--after` is not set: a time before `end`
/// that suits the size of the rows.
fn default_start(rollup: Option<Rollup>, end: DateTime<Utc>) -> DateTime<Utc> {
    match rollup {
        None | Some(Rollup::Day) => end - TimeDelta::weeks(1),
        Some(Rollup::Week) => end - TimeDelta::weeks(4),
        // Months are counted on the local calendar, as the periods are.
        Some(Rollup::Month) => start_of_local_calendar_day(
            local_calendar_date(end)
                .checked_sub_months(Months::new(6))
                .unwrap_or(FIRST_DAY),
        ),
    }
}

/// The number of usage requests in flight at one time for a list of runs.
const RUN_REQUESTS: usize = 8;

pub(super) async fn cmd_runs_usage(
    args: RunsUsageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("listing usage");
    let RunsUsageArgs {
        run_ids,
        rollup,
        after,
        before,
    } = args;
    let api = AntithesisApi::new(settings, verbose)?;

    if !run_ids.is_empty() {
        let entries: Vec<Option<UsageEntry>> = stream::iter(&run_ids)
            .map(|run_id| api.get_usage(run_id))
            .buffered(RUN_REQUESTS)
            .try_collect()
            .await?;
        let mut missing = Vec::new();
        let mut rows = Vec::new();
        for (run_id, entry) in run_ids.iter().zip(entries) {
            match entry {
                Some(entry) => rows.push(UsageRow::from(entry)),
                None => missing.push(sanitize(run_id)),
            }
        }
        if !rows.is_empty() {
            print_rows(&rows, json, render_usage_table)?;
        }
        if missing.is_empty() {
            return Ok(());
        }
        return Err(
            user_error(format!("no usage found for {}", missing.join(", ")))
                .note("a run has no usage until it ends")
                .suggestion(
                    "check each run ID and the status of its run with `snouty runs show <run_id>`",
                ),
        );
    }

    // No run ends after the present, so a later `--before` selects the same
    // runs.
    let now = Utc::now();
    let end = before.map_or(now, |time| time.min(now));
    let start = after.unwrap_or_else(|| default_start(rollup, end));

    let Some(rollup) = rollup else {
        let mut entries: Vec<UsageEntry> = api.stream_usage(start, before).try_collect().await?;
        // The endpoint sends the newest runs first. The start time and the
        // run ID order the runs that ended at the same time.
        entries.sort_by(|a, b| {
            (a.end_time, a.start_time, &a.run_id).cmp(&(b.end_time, b.start_time, &b.run_id))
        });
        let rows: Vec<UsageRow> = entries.into_iter().map(UsageRow::from).collect();
        return print_rows(&rows, json, render_usage_table);
    };

    // A run counts in the period in which it ended, as the usage summary and
    // the usage cap count it. `end` is exclusive, so the newest period is the
    // one that holds the instant before it.
    let ends = period_ends(
        rollup,
        local_calendar_date(start),
        local_calendar_date(end - TimeDelta::nanoseconds(1)),
    );
    let mut totals: BTreeMap<NaiveDate, (u64, f64)> =
        ends.iter().map(|end| (*end, (0, 0.0))).collect();
    if let (Some(oldest_end), Some(newest_end)) = (ends.first(), ends.last()) {
        // The default start shows the whole of its period. A filter cuts its
        // period.
        let first = start_of_local_calendar_day(rollup.period_start(*oldest_end));
        let last = start_of_local_calendar_day(*newest_end + Days::new(1));
        let after = after.map_or(first, |time| time.max(first));
        let before = before.map_or(last, |time| time.min(last));
        let entries = api.stream_usage(after, Some(before));
        let mut entries = std::pin::pin!(entries);
        while let Some(entry) = entries.try_next().await? {
            let date = rollup.period_end(local_calendar_date(entry.end_time));
            if let Some((runs, hours)) = totals.get_mut(&date) {
                *runs += 1;
                *hours += entry.hours;
            }
        }
    }
    let periods: Vec<PeriodUsage> = totals
        .into_iter()
        .map(|(date, (runs, hours))| PeriodUsage {
            start: rollup.period_start(date),
            date,
            runs,
            hours,
        })
        .collect();
    print_rows(&periods, json, render_periods_table)
}

/// The last days of the periods from the one that holds `oldest` to the one
/// that holds `newest`, oldest first.
fn period_ends(rollup: Rollup, oldest: NaiveDate, newest: NaiveDate) -> Vec<NaiveDate> {
    let last = rollup.period_end(newest);
    let mut ends = Vec::new();
    let mut end = rollup.period_end(oldest.max(FIRST_DAY));
    while end <= last {
        ends.push(end);
        let Some(next) = end.succ_opt() else { break };
        end = rollup.period_end(next);
    }
    ends
}

fn print_rows<T: Serialize>(
    rows: &[T],
    json: bool,
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
    Ok(())
}

/// Compute hours to one decimal place. A compute hour is a billing unit, not a
/// duration, so it does not print as hours and minutes.
fn format_hours(hours: f64) -> String {
    format!("{hours:.1}")
}

fn render_usage_table(entries: &[UsageRow], width: usize) -> String {
    let rows = entries
        .iter()
        .map(|entry| {
            vec![
                sanitize(&entry.run_id),
                format_timestamp_for_display(entry.start_time),
                format_timestamp_for_display(entry.end_time),
                format_hours(entry.hours),
            ]
        })
        .collect();
    Table::builder()
        .column(Column::new("RUN ID"))
        .column(Column::new("STARTED"))
        .column(Column::new("ENDED"))
        .column(Column::new("COMPUTE HOURS").align(Align::Right))
        .render(rows, width)
}

fn render_periods_table(periods: &[PeriodUsage], width: usize) -> String {
    let rows = periods
        .iter()
        .map(|period| {
            vec![
                period.start.to_string(),
                period.date.to_string(),
                period.runs.to_string(),
                format_hours(period.hours),
            ]
        })
        .collect();
    Table::builder()
        .column(Column::new("START"))
        .column(Column::new("END"))
        .column(Column::new("RUNS").align(Align::Right))
        .column(Column::new("COMPUTE HOURS").align(Align::Right))
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
    fn a_period_total_rounds_once() {
        let period = PeriodUsage {
            start: date("2026-10-19"),
            date: date("2026-10-25"),
            runs: 2,
            hours: 0.446,
        };
        // Rounded to hundredths first, 0.446 would become 0.45 and print 0.5.
        assert_eq!(format_hours(period.hours), "0.4");
        assert_eq!(
            serde_json::to_value(&period).unwrap()["hours"],
            serde_json::json!(0.45)
        );
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

    fn dates(texts: &[&str]) -> Vec<NaiveDate> {
        texts.iter().map(|text| date(text)).collect()
    }

    #[test]
    fn period_ends_count_forward_from_oldest_to_newest() {
        let ends = |rollup, oldest, newest| period_ends(rollup, date(oldest), date(newest));
        assert_eq!(
            ends(Rollup::Day, "2026-03-01", "2026-03-03"),
            dates(&["2026-03-01", "2026-03-02", "2026-03-03"])
        );
        assert_eq!(
            ends(Rollup::Week, "2026-02-18", "2026-03-03"),
            dates(&["2026-02-22", "2026-03-01", "2026-03-08"])
        );
        assert_eq!(
            ends(Rollup::Month, "2026-01-31", "2026-03-01"),
            dates(&["2026-01-31", "2026-02-28", "2026-03-31"])
        );
        assert_eq!(
            ends(Rollup::Day, "2026-03-03", "2026-03-03"),
            dates(&["2026-03-03"])
        );
        assert_eq!(ends(Rollup::Day, "2026-03-04", "2026-03-03"), dates(&[]));
        // No period starts before 1970.
        assert_eq!(
            ends(Rollup::Month, "0001-01-01", "1970-02-01"),
            dates(&["1970-01-31", "1970-02-28"])
        );
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
