//! TODO: when the server adds the usage of a run to the `runs list` response,
//! read it from there, add the run metadata, and drop the usage endpoint.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroU64;

use chrono::{
    DateTime, Datelike, Days, FixedOffset, Local, Months, NaiveDate, NaiveTime, TimeDelta,
    TimeZone, Utc,
};
use clap::ValueEnum;
use color_eyre::eyre::Result;
use futures_util::TryStreamExt;
use log::debug;
use serde::Serialize;

use super::{limit_note, terminal_width};
use crate::api::{AntithesisApi, UsageEntry};
use crate::cli::{DEFAULT_RUNS_LIMIT, RunsUsageArgs};
use crate::error::user_error;
use crate::render::{OutputOptions, sanitize};
use crate::settings::Settings;
use crate::table::{Align, Column, Table};
use crate::time::format_dense;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Rollup {
    Day,
    Week,
    Month,
}

/// The time zone of the output: of the times, and of the periods that runs
/// are totaled in.
#[derive(Clone, Copy)]
enum Zone {
    Local,
    Utc,
}

impl Zone {
    fn time(self, time: DateTime<Utc>) -> DateTime<FixedOffset> {
        match self {
            Zone::Local => time.with_timezone(&Local).fixed_offset(),
            Zone::Utc => time.fixed_offset(),
        }
    }

    /// The start of `date`. Where a clock change skips local midnight, this
    /// is UTC midnight, which is off by at most the size of the change.
    fn midnight(self, date: NaiveDate) -> DateTime<Utc> {
        let midnight = date.and_time(NaiveTime::MIN);
        match self {
            Zone::Local => Local.from_local_datetime(&midnight).earliest(),
            Zone::Utc => None,
        }
        .map_or(midnight.and_utc(), |time| time.to_utc())
    }
}

/// A `--after` or `--before` value: a date, which means the start of that
/// day in the output time zone, or an RFC 3339 time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TimeBound {
    Date(NaiveDate),
    Time(DateTime<Utc>),
}

impl std::str::FromStr for TimeBound {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            return Ok(TimeBound::Date(date));
        }
        text.parse().map(TimeBound::Time).map_err(|_| {
            "expected a date such as 2026-10-01, or an RFC 3339 time such as \
             2026-10-01T12:00:00Z"
                .to_string()
        })
    }
}

impl TimeBound {
    fn resolve(self, zone: Zone) -> DateTime<Utc> {
        match self {
            TimeBound::Date(date) => zone.midnight(date),
            TimeBound::Time(time) => time,
        }
    }
}

/// The usage entry of a run, with its times in the output time zone.
#[derive(Serialize)]
struct ZonedUsageEntry {
    run_id: String,
    hours: f64,
    start_time: DateTime<FixedOffset>,
    end_time: DateTime<FixedOffset>,
}

impl ZonedUsageEntry {
    fn new(entry: UsageEntry, zone: Zone) -> Self {
        Self {
            run_id: entry.run_id,
            hours: entry.hours,
            start_time: zone.time(entry.start_time),
            end_time: zone.time(entry.end_time),
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

fn serialize_hundredths<S: serde::Serializer>(
    hours: &f64,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_f64((hours * 100.0).round() / 100.0)
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

/// Which rows the output holds. The usage endpoint sends the newest runs
/// first, so only `Newest` can stop at the first `limit` runs that it reads.
// TODO: when the endpoint can sort the oldest runs first, read `Oldest` from
// one request at `--after` instead of from windows of end times.
#[derive(Clone, Copy)]
enum Selection {
    /// The `limit` newest rows, before `--before` when it is set.
    Newest(NonZeroU64),
    /// The `limit` oldest rows from `--after`.
    Oldest(DateTime<Utc>, NonZeroU64),
    /// Every row between `--after` and `--before`.
    All(DateTime<Utc>),
}

impl Selection {
    fn limit(self) -> Option<NonZeroU64> {
        match self {
            Selection::Newest(limit) | Selection::Oldest(_, limit) => Some(limit),
            Selection::All(_) => None,
        }
    }
}

pub(super) async fn cmd_runs_usage(
    args: RunsUsageArgs,
    settings: &Settings,
    OutputOptions { json, verbose }: OutputOptions,
) -> Result<()> {
    debug!("listing usage");
    let RunsUsageArgs {
        rollup,
        after,
        before,
        utc,
        limit,
    } = args;
    let zone = if utc { Zone::Utc } else { Zone::Local };
    let after = after.map(|bound| bound.resolve(zone));
    let before = before.map(|bound| bound.resolve(zone));
    let selection = match (after, before, limit) {
        (Some(_), Some(_), Some(_)) => {
            return Err(user_error(
                "--limit cannot be used with both --after and --before, which select every \
                 row between them",
            ));
        }
        (Some(after), Some(_), None) => Selection::All(after),
        (Some(after), None, limit) => Selection::Oldest(after, limit.unwrap_or(DEFAULT_RUNS_LIMIT)),
        (None, _, limit) => Selection::Newest(limit.unwrap_or(DEFAULT_RUNS_LIMIT)),
    };
    let api = AntithesisApi::new(settings, verbose)?;

    let Some(rollup) = rollup else {
        let entries = match selection {
            Selection::Newest(limit) => {
                api.stream_usage(None, before, Some(limit))
                    .try_collect()
                    .await?
            }
            Selection::Oldest(after, limit) => oldest_entries(&api, after, limit).await?,
            Selection::All(after) => {
                api.stream_usage(Some(after), before, None)
                    .try_collect()
                    .await?
            }
        };
        let entries: Vec<ZonedUsageEntry> = entries
            .into_iter()
            .map(|entry| ZonedUsageEntry::new(entry, zone))
            .collect();
        return print_rows(
            &entries,
            json,
            selection.limit(),
            "results",
            render_usage_table,
        );
    };

    // A run counts in the period in which it ended, as the usage summary and
    // the usage cap count it. `before` is exclusive, so the newest period is
    // the one that holds the instant before it.
    let date = |time: DateTime<Utc>| zone.time(time).date_naive();
    let newest = date(before.map_or_else(Utc::now, |time| time - TimeDelta::nanoseconds(1)));
    let ends = match selection {
        Selection::Newest(limit) => periods_back(rollup, newest, FIRST_DAY, Some(limit)),
        Selection::Oldest(after, limit) => periods_forward(rollup, date(after), newest, limit),
        Selection::All(after) => periods_back(rollup, newest, date(after), None),
    };
    let mut totals: BTreeMap<NaiveDate, (u64, f64)> =
        ends.iter().map(|end| (*end, (0, 0.0))).collect();
    // One window of end times holds every period, so the page order does not
    // matter.
    if let (Some(newest_end), Some(oldest_end)) = (ends.first(), ends.last()) {
        let start = zone.midnight(rollup.period_start(*oldest_end));
        let end = zone.midnight(*newest_end + Days::new(1));
        let after = after.map_or(start, |time| time.max(start));
        let before = before.map_or(end, |time| time.min(end));
        let entries = api.stream_usage(Some(after), Some(before), None);
        let mut entries = std::pin::pin!(entries);
        while let Some(entry) = entries.try_next().await? {
            let date = rollup.period_end(date(entry.end_time));
            // The midnight of a skipped clock change can let in a run from
            // just outside the window, which has no period here.
            if let Some((runs, hours)) = totals.get_mut(&date) {
                *runs += 1;
                *hours += entry.hours;
            }
        }
    }
    let periods: Vec<PeriodUsage> = totals
        .into_iter()
        .rev()
        .map(|(date, (runs, hours))| PeriodUsage {
            start: rollup.period_start(date),
            date,
            runs,
            hours,
        })
        .collect();
    print_rows(
        &periods,
        json,
        selection.limit(),
        "periods",
        render_periods_table,
    )
}

/// The `limit` runs that ended first at or after `after`, newest first. The
/// endpoint sends the newest runs first, so this reads windows of end times
/// forward from `after`, each twice as long as the one before, until they hold
/// `limit` runs or pass the present.
async fn oldest_entries(
    api: &AntithesisApi,
    after: DateTime<Utc>,
    limit: NonZeroU64,
) -> Result<Vec<UsageEntry>> {
    let now = Utc::now();
    let mut entries: Vec<UsageEntry> = Vec::new();
    let (mut start, mut length) = (after, TimeDelta::days(1));
    while (entries.len() as u64) < limit.get() && start <= now {
        let end = start + length;
        let window: Vec<UsageEntry> = api
            .stream_usage(Some(start), Some(end), None)
            .try_collect()
            .await?;
        entries.extend(window);
        (start, length) = (end, length * 2);
    }
    entries.sort_by_key(|entry| entry.end_time);
    entries.truncate(usize::try_from(limit.get()).unwrap_or(usize::MAX));
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.start_time));
    Ok(entries)
}

/// The last days of the periods from the one that holds `newest` back to the
/// one that holds `oldest`, newest first, and at most `limit` of them.
fn periods_back(
    rollup: Rollup,
    newest: NaiveDate,
    oldest: NaiveDate,
    limit: Option<NonZeroU64>,
) -> Vec<NaiveDate> {
    let last = rollup.period_end(oldest.max(FIRST_DAY));
    let mut ends = Vec::new();
    let mut end = rollup.period_end(newest);
    while end >= last && limit.is_none_or(|limit| (ends.len() as u64) < limit.get()) {
        ends.push(end);
        end = rollup.period_start(end) - Days::new(1);
    }
    ends
}

/// The last days of the `limit` periods from the one that holds `oldest`
/// forward, but not past the one that holds `newest`, newest first.
fn periods_forward(
    rollup: Rollup,
    oldest: NaiveDate,
    newest: NaiveDate,
    limit: NonZeroU64,
) -> Vec<NaiveDate> {
    let last = rollup.period_end(newest);
    let mut ends = Vec::new();
    let mut end = rollup.period_end(oldest.max(FIRST_DAY));
    while end <= last && (ends.len() as u64) < limit.get() {
        ends.push(end);
        end = rollup.period_end(end + Days::new(1));
    }
    ends.reverse();
    ends
}

/// `noun` names the rows in the limit note.
fn print_rows<T: Serialize>(
    rows: &[T],
    json: bool,
    limit: Option<NonZeroU64>,
    noun: &str,
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
        limit_note(rows.len(), limit, noun, false);
    }
    Ok(())
}

/// Core-hours to one decimal place. A core-hour is a billing unit, not a
/// duration, so it does not print as hours and minutes.
fn format_hours(hours: f64) -> String {
    format!("{hours:.1}")
}

fn render_usage_table(entries: &[ZonedUsageEntry], width: usize) -> String {
    let rows = entries
        .iter()
        .map(|entry| {
            vec![
                sanitize(&entry.run_id),
                format_dense(entry.start_time),
                format_dense(entry.end_time),
                format_hours(entry.hours),
            ]
        })
        .collect();
    Table::builder()
        .column(Column::new("RUN ID"))
        .column(Column::new("STARTED"))
        .column(Column::new("ENDED"))
        .column(Column::new("CORE-HOURS").align(Align::Right))
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
        .column(Column::new("CORE-HOURS").align(Align::Right))
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

    /// A date parses as a date, and an RFC 3339 time as the same instant.
    #[hegel::test]
    fn time_bound_parses_dates_and_times(tc: hegel::TestCase) {
        let days = tc.draw(generators::integers::<u64>().max_value(3_000_000));
        let date = date("0001-01-01") + Days::new(days);
        let text = date.format("%Y-%m-%d").to_string();
        assert_eq!(text.parse::<TimeBound>(), Ok(TimeBound::Date(date)));
        let seconds = tc.draw(
            generators::integers::<i64>()
                .min_value(0)
                .max_value(250_000_000_000),
        );
        let time = DateTime::from_timestamp(seconds, 0).unwrap();
        assert_eq!(
            time.to_rfc3339().parse::<TimeBound>(),
            Ok(TimeBound::Time(time))
        );
        assert!("2026-10-01 12:00".parse::<TimeBound>().is_err());
    }

    fn dates(texts: &[&str]) -> Vec<NaiveDate> {
        texts.iter().map(|text| date(text)).collect()
    }

    #[test]
    fn periods_back_counts_back_from_newest_to_oldest() {
        let three = NonZeroU64::new(3);
        let back =
            |rollup, oldest, limit| periods_back(rollup, date("2026-03-03"), date(oldest), limit);
        assert_eq!(
            back(Rollup::Day, "1970-01-01", three),
            dates(&["2026-03-03", "2026-03-02", "2026-03-01"])
        );
        assert_eq!(
            back(Rollup::Week, "1970-01-01", three),
            dates(&["2026-03-08", "2026-03-01", "2026-02-22"])
        );
        assert_eq!(
            back(Rollup::Month, "1970-01-01", three),
            dates(&["2026-03-31", "2026-02-28", "2026-01-31"])
        );
        // The period that holds `oldest` is the last one.
        assert_eq!(
            back(Rollup::Month, "2026-02-14", None),
            dates(&["2026-03-31", "2026-02-28"])
        );
        assert_eq!(
            back(Rollup::Day, "2026-03-03", None),
            dates(&["2026-03-03"])
        );
        assert_eq!(back(Rollup::Day, "2026-03-04", None), dates(&[]));
    }

    #[test]
    fn periods_forward_counts_forward_from_oldest_to_newest() {
        let two = NonZeroU64::new(2).unwrap();
        let forward =
            |rollup, newest| periods_forward(rollup, date("2026-02-14"), date(newest), two);
        assert_eq!(
            forward(Rollup::Day, "2026-03-03"),
            dates(&["2026-02-15", "2026-02-14"])
        );
        assert_eq!(
            forward(Rollup::Month, "2026-03-03"),
            dates(&["2026-03-31", "2026-02-28"])
        );
        // No period after the one that holds `newest`.
        assert_eq!(forward(Rollup::Month, "2026-02-20"), dates(&["2026-02-28"]));
        assert_eq!(forward(Rollup::Day, "2026-02-13"), dates(&[]));
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
