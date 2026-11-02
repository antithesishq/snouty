//! Human-typed durations.
//!
//! Antithesis takes a run's duration in minutes, and has historically accepted
//! a fractional value (e.g. `0.05` for three seconds). [`HumanDuration`]
//! preserves that, while also accepting the unit forms people reach for
//! (`1h30m`, `2h`, `30s`).
//!
//! A bare number is read as a (possibly fractional) count of minutes and
//! rounded to whole seconds — the finest resolution we keep. A flag that has
//! always taken seconds reads a bare number as whole seconds instead, and an
//! MCP param refuses a bare number; see [`BareUnit`]. A bare number is
//! deprecated on the command line, and the docs give only the unit forms
//! ([`DURATION_FORMAT`]). Unit forms use whole-number components (`h`/`m`/`s`,
//! in that order); fractional components like `1.5h` are rejected, since the
//! bare-minutes form already covers that.

use chrono::{
    DateTime, Local, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeDelta,
    TimeZone, Utc,
};

use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

const SECS_PER_MINUTE: u64 = 60;
const SECS_PER_HOUR: u64 = 60 * SECS_PER_MINUTE;

/// The duration format, as every duration flag and param documents it. A
/// macro, so that a clap `help` attribute can `concat!` it.
macro_rules! duration_format {
    () => {
        "A duration is a whole number with an h, m or s unit, or several in that \
         order, such as 90s, 5m or 1h30m; a bare number is not permitted"
    };
}
pub(crate) use duration_format;

/// [`duration_format!`] as a value.
pub const DURATION_FORMAT: &str = duration_format!();

/// A duration in the forms people type (`90`, `1h30m`, `30s`), held as a
/// [`Duration`] rounded to whole seconds. Carried in the `antithesis.duration`
/// report parameter, among other uses.
///
/// Parse one from the strings people type:
/// ```
/// # use snouty::time::HumanDuration;
/// assert_eq!("1h30m".parse::<HumanDuration>().unwrap().minutes(), 90.0);
/// assert_eq!("0.05".parse::<HumanDuration>().unwrap().minutes(), 0.05); // fractional minutes
/// assert_eq!("7s".parse::<HumanDuration>().unwrap().minutes(), 0.12);   // rounded to 2 dp
/// assert!("1.5h".parse::<HumanDuration>().is_err());                    // units stay whole
/// // Display uses the minimized h/m/s form:
/// assert_eq!("0.05".parse::<HumanDuration>().unwrap().to_string(), "3s");
/// assert_eq!("90".parse::<HumanDuration>().unwrap().to_string(), "1h30m");
/// ```
///
/// [`Display`](fmt::Display) renders the minimized `h`/`m`/`s` form. For the
/// `antithesis.duration` parameter use [`minutes()`](Self::minutes)`.to_string()`
/// instead — that's the (possibly fractional) minute count the API expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanDuration(Duration);

/// How to read a bare number (no `h`/`m`/`s` suffix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BareUnit {
    /// A possibly fractional count of minutes: the [`FromStr`] form.
    Minutes,
    /// A whole count of seconds, for flags that have always taken seconds.
    Seconds,
    /// A bare number is an error. MCP params use this, so that a bare number
    /// cannot mean minutes in one tool and seconds in another.
    Refused,
}

impl HumanDuration {
    /// Parse `s`, reading a bare number as `bare` says. Unit forms parse the
    /// same with every [`BareUnit`].
    pub fn parse(s: &str, bare: BareUnit) -> Result<Self, ParseDurationError> {
        let s = s.trim().to_ascii_lowercase();

        // `bare` decides a bare number; other input needs whole-number
        // `h`/`m`/`s` components.
        let parsed = if is_decimal(&s) {
            match bare {
                BareUnit::Minutes => s.parse().ok().and_then(Self::from_minutes),
                BareUnit::Seconds => s.parse().ok().map(Self::from_seconds),
                BareUnit::Refused => None,
            }
        } else {
            parse_units(&s).map(Self::from_seconds)
        };
        parsed.ok_or(ParseDurationError)
    }

    /// Parse `s` as a duration flag does: a bare number still counts `bare`
    /// units, but is deprecated, so the result also says whether `s` was one.
    pub fn parse_flag(s: &str, bare: BareUnit) -> Result<(Self, bool), ParseDurationError> {
        match Self::parse(s, BareUnit::Refused) {
            Ok(duration) => Ok((duration, false)),
            Err(_) => Self::parse(s, bare).map(|duration| (duration, true)),
        }
    }

    /// Build from a whole number of seconds.
    pub fn from_seconds(seconds: u64) -> Self {
        Self(Duration::from_secs(seconds))
    }

    /// Build from a (possibly fractional) number of minutes, rounded to whole
    /// seconds. Returns `None` for a non-finite or negative value.
    pub fn from_minutes(minutes: f64) -> Option<Self> {
        if !minutes.is_finite() || minutes < 0.0 {
            return None;
        }
        // `as u64` saturates rather than wraps, so an absurd value clamps
        // instead of panicking.
        let seconds = (minutes * SECS_PER_MINUTE as f64).round() as u64;
        Some(Self::from_seconds(seconds))
    }

    /// The duration as a number of minutes, rounded to 2 decimal places — the
    /// form the API wants: e.g. 7s is `0.12`, not `0.11666…`.
    pub fn minutes(&self) -> f64 {
        self.in_units_of(SECS_PER_MINUTE)
    }

    /// The duration as a number of hours, rounded to 2 decimal places as
    /// [`minutes()`](Self::minutes) is: e.g. 50m is `0.83`.
    pub fn hours(&self) -> f64 {
        self.in_units_of(SECS_PER_HOUR)
    }

    fn in_units_of(&self, secs_per_unit: u64) -> f64 {
        // Rounded in integer space, not by dividing floats, so a second-granular
        // value does not become a long repeating decimal.
        let per_unit = u128::from(secs_per_unit);
        let hundredths = (u128::from(self.seconds()) * 100 + per_unit / 2) / per_unit;
        hundredths as f64 / 100.0
    }

    /// The duration as a whole number of seconds.
    pub fn seconds(&self) -> u64 {
        self.0.as_secs()
    }
}

impl fmt::Display for HumanDuration {
    /// Minimized `h`/`m`/`s` form: each nonzero component in order (e.g.
    /// `1h30m`, `1m30s`, `1h1m1s`), or `0s` for zero.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.seconds();
        let (hours, mins, secs) = (
            total / SECS_PER_HOUR,
            (total % SECS_PER_HOUR) / SECS_PER_MINUTE,
            total % SECS_PER_MINUTE,
        );
        if total == 0 {
            return f.write_str("0s");
        }
        if hours != 0 {
            write!(f, "{hours}h")?;
        }
        if mins != 0 {
            write!(f, "{mins}m")?;
        }
        if secs != 0 {
            write!(f, "{secs}s")?;
        }
        Ok(())
    }
}

/// The underlying [`Duration`], for use with timers and the like.
impl From<HumanDuration> for Duration {
    fn from(value: HumanDuration) -> Self {
        value.0
    }
}

impl FromStr for HumanDuration {
    type Err = ParseDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s, BareUnit::Minutes)
    }
}

/// Error from parsing a [`HumanDuration`]. Its message gives the documented
/// format, which leaves out the deprecated bare number; clap prefixes it with
/// the offending value and flag name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDurationError;

impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "must be a whole number with an h, m or s unit, or several in that order \
             (e.g. `90s`, `5m`, `1h30m`); a bare number is not permitted",
        )
    }
}

impl Error for ParseDurationError {}

/// Whether `s` matches `^[0-9]+(\.[0-9]+)?$` — a bare integer or decimal.
fn is_decimal(s: &str) -> bool {
    let mut parts = s.splitn(2, '.');
    let int = parts.next().unwrap_or_default();
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    digits(int) && parts.next().is_none_or(digits)
}

/// Parse a unit-suffixed duration — whole-number `h`, `m`, and `s` components
/// in that order — into total seconds. Returns `None` for a missing component,
/// the wrong order, an unknown unit, or trailing junk.
fn parse_units(s: &str) -> Option<u64> {
    let mut rest = s;
    let mut seconds = 0u64;
    let mut matched = false;

    for (unit, secs_per_unit) in [('h', SECS_PER_HOUR), ('m', SECS_PER_MINUTE), ('s', 1)] {
        if let Some((value, after)) = split_unit(rest, unit) {
            seconds = seconds.checked_add(value.checked_mul(secs_per_unit)?)?;
            rest = after;
            matched = true;
        }
    }

    (matched && rest.is_empty()).then_some(seconds)
}

/// If `s` starts with `<digits><unit>`, return the parsed number and the
/// remainder after the unit; otherwise `None`.
fn split_unit(s: &str, unit: char) -> Option<(u64, &str)> {
    let idx = s.find(unit)?;
    let (number, rest) = s.split_at(idx);
    if number.is_empty() || !number.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((number.parse().ok()?, &rest[unit.len_utf8()..]))
}

/// The form of a timestamp that snouty prints, and that
/// [`parse_timestamp_arg`] reads back.
const DISPLAY_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// Writes `time` as the local wall time, without the zone, such as
/// `2026-05-27 08:25:13`.
pub(crate) fn format_timestamp_for_display(time: DateTime<Utc>) -> String {
    time.with_timezone(&Local)
        .format(DISPLAY_FORMAT)
        .to_string()
}

/// [`format_timestamp_for_display`] for an RFC 3339 string from the server.
/// Returns the string as it is when it does not parse.
pub(crate) fn format_rfc3339_for_display(raw: &str) -> String {
    DateTime::parse_from_rfc3339(raw).map_or_else(
        |_| raw.to_string(),
        |time| format_timestamp_for_display(time.to_utc()),
    )
}

/// Writes `time` as RFC 3339 with the local UTC offset, for JSON output. It
/// keeps the fraction of a second that `time` has.
fn format_rfc3339_with_local_offset(time: DateTime<Utc>) -> String {
    time.with_timezone(&Local)
        .to_rfc3339_opts(SecondsFormat::AutoSi, false)
}

/// Serializes `time` as [`format_rfc3339_with_local_offset`] does.
pub(crate) fn serialize_with_local_offset<S: serde::Serializer>(
    time: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format_rfc3339_with_local_offset(*time))
}

/// Rewrites each of `fields` of the JSON object `object` that holds an RFC
/// 3339 time, such as a time from the server, as
/// [`format_rfc3339_with_local_offset`] does. Other values stay as they are.
pub(crate) fn localize_rfc3339_fields(object: &mut serde_json::Value, fields: &[&str]) {
    for field in fields {
        if let Some(serde_json::Value::String(text)) = object.get_mut(field)
            && let Ok(time) = DateTime::parse_from_rfc3339(text)
        {
            *text = format_rfc3339_with_local_offset(time.to_utc());
        }
    }
}

/// Reads a timestamp argument: a date (the start of that day), a time as
/// snouty prints it or with a `T`, or an RFC 3339 time. A value without an
/// offset is a local wall time. A wall time that a clock change skips is an
/// error.
pub(crate) fn parse_timestamp_arg(text: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(time) = text.parse() {
        return Ok(time);
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(start_of_local_calendar_day(date));
    }
    let wall_time = NaiveDateTime::parse_from_str(text, DISPLAY_FORMAT)
        // `NaiveDateTime`'s own parser reads the `T` form, and a fraction.
        .or_else(|_| text.parse())
        .map_err(|_| {
            "expected a date (2026-10-01), a time as snouty prints it \
             (2026-10-01 12:00:00), or an RFC 3339 time (2026-10-01T12:00:00Z)"
                .to_string()
        })?;
    instant_of_wall_time(&Local, wall_time).ok_or_else(|| {
        format!("{text} does not exist in the local timezone (a clock change skips it)")
    })
}

/// The date of the local calendar day that holds `instant`.
pub(crate) fn local_calendar_date(instant: DateTime<Utc>) -> NaiveDate {
    instant.with_timezone(&Local).date_naive()
}

/// The first instant of the local calendar day `date`.
pub(crate) fn start_of_local_calendar_day(date: NaiveDate) -> DateTime<Utc> {
    start_of_calendar_day(&Local, date)
}

/// The first instant of the calendar day `date` of a clock in `tz`.
fn start_of_calendar_day<Tz: TimeZone>(tz: &Tz, date: NaiveDate) -> DateTime<Utc> {
    // A few zones skip midnight: the day then starts at its first minute that
    // exists.
    (0..24 * 60)
        .find_map(|minute| {
            instant_of_wall_time(
                tz,
                date.and_time(NaiveTime::MIN) + TimeDelta::minutes(minute),
            )
        })
        .expect("a calendar day has a minute that exists")
}

/// The instant at which a clock in `tz` shows `wall_time`, or `None` when a
/// clock change skips it.
fn instant_of_wall_time<Tz: TimeZone>(tz: &Tz, wall_time: NaiveDateTime) -> Option<DateTime<Utc>> {
    match tz.from_local_datetime(&wall_time) {
        LocalResult::Single(time) => Some(time.to_utc()),
        // A clock change shows the time twice: take the earlier instant.
        LocalResult::Ambiguous(a, b) => Some(a.min(b).to_utc()),
        LocalResult::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, Offset};
    use chrono_tz::America::{New_York, Santiago};
    use chrono_tz::Asia::Tokyo;
    use chrono_tz::Australia::Lord_Howe;
    use chrono_tz::Tz;
    use std::sync::OnceLock;

    fn minutes(s: &str) -> f64 {
        s.parse::<HumanDuration>().unwrap().minutes()
    }

    fn seconds(s: &str) -> u64 {
        s.parse::<HumanDuration>().unwrap().seconds()
    }

    fn hours(s: &str) -> f64 {
        s.parse::<HumanDuration>().unwrap().hours()
    }

    #[test]
    fn parses_bare_number_as_minutes() {
        assert_eq!(minutes("30"), 30.0);
        assert_eq!(minutes("0"), 0.0);
        assert_eq!(minutes("  15  "), 15.0); // surrounding whitespace tolerated
        // Fractional minutes are kept (rounded to whole seconds).
        assert_eq!(seconds("0.05"), 3);
        assert_eq!(minutes("0.05"), 0.05);
        assert_eq!(seconds("1.5"), 90);
    }

    #[test]
    fn minutes_rounds_to_two_decimals() {
        // Rounded in integer space, so no float drift and no long decimals.
        assert_eq!(HumanDuration::from_seconds(3).minutes(), 0.05);
        assert_eq!(HumanDuration::from_seconds(7).minutes(), 0.12); // 0.11666… rounded up
        assert_eq!(HumanDuration::from_seconds(5).minutes(), 0.08); // 0.08333… rounded down
        assert_eq!(HumanDuration::from_seconds(5442).minutes(), 90.7); // float-divide would give 90.69
        assert_eq!(HumanDuration::from_seconds(5445).minutes(), 90.75);
        // ...and the string sent to the API stays clean.
        assert_eq!(HumanDuration::from_seconds(7).minutes().to_string(), "0.12");
    }

    #[test]
    fn hours_rounds_to_two_decimals() {
        assert_eq!(hours("30m"), 0.5);
        assert_eq!(hours("1h30m"), 1.5);
        assert_eq!(hours("6h"), 6.0);
        assert_eq!(hours("50m").to_string(), "0.83");
    }

    #[test]
    fn rounds_fractional_minutes_to_whole_seconds() {
        assert_eq!(seconds("0.05"), 3); // 3.0s
        assert_eq!(seconds("0.051"), 3); // 3.06s -> 3
        assert_eq!(seconds("0.06"), 4); // 3.6s -> 4
    }

    #[test]
    fn parses_unit_suffixes() {
        assert_eq!(seconds("15m"), 15 * 60);
        assert_eq!(seconds("1h"), 3600);
        assert_eq!(seconds("2h"), 7200);
        assert_eq!(seconds("1h30m"), 5400);
        assert_eq!(seconds("30s"), 30);
        assert_eq!(seconds("90s"), 90);
        assert_eq!(seconds("1m30s"), 90);
        assert_eq!(seconds("1h30m45s"), 5445);
        assert_eq!(seconds("1H30M"), 5400); // case-insensitive
    }

    #[test]
    fn rejects_unparsable_values() {
        // Unknown units, fractional unit components, a trailing unitless number,
        // wrong unit order, and malformed numbers all fail.
        for bad in [
            "abc", "15x", "1.5h", "0.5m", "1h30", "1h2h", "30m15h", "1.2.3", "1.2.3h", "h", "",
            "  ",
        ] {
            assert_eq!(
                bad.parse::<HumanDuration>(),
                Err(ParseDurationError),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn bare_seconds_reads_a_whole_number_as_seconds() {
        let seconds = |s: &str| HumanDuration::parse(s, BareUnit::Seconds).map(|d| d.seconds());
        assert_eq!(seconds("120"), Ok(120));
        assert_eq!(seconds(" 0 "), Ok(0));
        // Unit forms parse as they do with bare minutes.
        assert_eq!(seconds("2m"), Ok(120));
        assert_eq!(seconds("90s"), Ok(90));
        assert_eq!(seconds("1h"), Ok(3600));
        // Seconds stay whole, so a fractional bare number is rejected.
        for bad in ["1.5", "abc", "1.5h", "1h30", ""] {
            assert_eq!(
                seconds(bad),
                Err(ParseDurationError),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn refused_bare_unit_takes_only_unit_forms() {
        let seconds = |s: &str| HumanDuration::parse(s, BareUnit::Refused).map(|d| d.seconds());
        assert_eq!(seconds("90s"), Ok(90));
        assert_eq!(seconds(" 5M "), Ok(300));
        assert_eq!(seconds("1h30m"), Ok(5400));
        for bad in ["30", "0", "1.5", "1.5h", ""] {
            assert_eq!(
                seconds(bad),
                Err(ParseDurationError),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn parse_flag_marks_a_bare_number() {
        let flag =
            |s: &str, bare| HumanDuration::parse_flag(s, bare).map(|(d, b)| (d.seconds(), b));
        assert_eq!(flag("30", BareUnit::Minutes), Ok((1800, true)));
        assert_eq!(flag("1.5", BareUnit::Minutes), Ok((90, true)));
        assert_eq!(flag(" 45 ", BareUnit::Seconds), Ok((45, true)));
        assert_eq!(flag("30m", BareUnit::Minutes), Ok((1800, false)));
        assert_eq!(flag("45s", BareUnit::Seconds), Ok((45, false)));
        assert_eq!(flag("1.5", BareUnit::Seconds), Err(ParseDurationError));
        assert_eq!(flag("1.5h", BareUnit::Minutes), Err(ParseDurationError));
    }

    #[test]
    fn from_minutes_rejects_non_finite_and_negative() {
        assert_eq!(HumanDuration::from_minutes(f64::NAN), None);
        assert_eq!(HumanDuration::from_minutes(f64::INFINITY), None);
        assert_eq!(HumanDuration::from_minutes(-1.0), None);
    }

    #[test]
    fn display_renders_minimized_hms() {
        let display = |s: &str| s.parse::<HumanDuration>().unwrap().to_string();
        assert_eq!(display("0"), "0s"); // zero
        assert_eq!(display("0.05"), "3s"); // sub-minute
        assert_eq!(display("90s"), "1m30s");
        assert_eq!(display("45"), "45m"); // sub-hour
        assert_eq!(display("60"), "1h"); // whole hours
        assert_eq!(display("120"), "2h");
        assert_eq!(display("90"), "1h30m"); // hours + minutes
        assert_eq!(display("1h30m45s"), "1h30m45s"); // all three
    }

    #[test]
    fn display_round_trips_by_value() {
        for s in [
            "0", "0.05", "45", "60", "90", "120", "1h30m", "30s", "1h30m45s", "90m",
        ] {
            let parsed = s.parse::<HumanDuration>().unwrap();
            let reparsed = parsed.to_string().parse::<HumanDuration>().unwrap();
            assert_eq!(parsed, reparsed, "{s:?} did not round-trip through Display");
        }
    }

    use hegel::generators;

    /// The minimized `h`/`m`/`s` `Display` form parses back to the same value,
    /// for *every* whole-second duration the type can hold. (The hand-written
    /// `display_round_trips_by_value` checks a handful of literals; this checks
    /// the whole `u64`-seconds domain, including `0` and `u64::MAX`, where the
    /// component arithmetic in `parse_units` is most likely to misbehave.)
    #[hegel::test]
    fn display_round_trips_for_all_durations(tc: hegel::TestCase) {
        let seconds = tc.draw(generators::integers::<u64>());
        let d = HumanDuration::from_seconds(seconds);
        let reparsed = d
            .to_string()
            .parse::<HumanDuration>()
            .expect("Display output must re-parse");
        assert_eq!(d, reparsed);
    }

    /// With bare seconds, a whole number parses to that many seconds, and the
    /// `Display` form parses back to the same value.
    #[hegel::test]
    fn bare_seconds_round_trips(tc: hegel::TestCase) {
        let seconds = tc.draw(generators::integers::<u64>());
        let d = HumanDuration::from_seconds(seconds);
        assert_eq!(
            HumanDuration::parse(&seconds.to_string(), BareUnit::Seconds),
            Ok(d)
        );
        assert_eq!(
            HumanDuration::parse(&d.to_string(), BareUnit::Seconds),
            Ok(d)
        );
    }

    #[test]
    fn an_unparsable_rfc3339_string_prints_as_it_is() {
        assert_eq!(format_rfc3339_for_display("not a time"), "not a time");
    }

    /// A timestamp that snouty prints reads back as the same instant, in the
    /// space form, in the `T` form, and as RFC 3339, in the timezone of the
    /// test process. A date reads as the start of its day.
    #[hegel::test]
    fn parse_timestamp_arg_reads_back_what_snouty_prints(tc: hegel::TestCase) {
        let seconds = tc.draw(
            generators::integers::<i64>()
                .min_value(0)
                .max_value(250_000_000_000),
        );
        let time = DateTime::from_timestamp(seconds, 0).unwrap();
        let printed = format_timestamp_for_display(time);
        // A wall time that a clock change shows twice reads as the earlier
        // instant, so `expected` is `time` or an instant before it.
        let expected = parse_timestamp_arg(&printed).unwrap();
        assert!(expected <= time, "{printed}");
        assert_eq!(format_timestamp_for_display(expected), printed);
        let t_form = printed.replace(' ', "T");
        assert_eq!(parse_timestamp_arg(&t_form), Ok(expected), "{printed}");
        assert_eq!(parse_timestamp_arg(&time.to_rfc3339()), Ok(time));
        let date = local_calendar_date(time);
        assert_eq!(
            parse_timestamp_arg(&date.format("%Y-%m-%d").to_string()),
            Ok(start_of_local_calendar_day(date))
        );
    }

    #[test]
    fn parse_timestamp_arg_names_the_forms_it_accepts() {
        let err = parse_timestamp_arg("2026-10-01 12:00").unwrap_err();
        assert!(err.contains("2026-10-01 12:00:00"), "{err}");
    }

    /// A time field keeps its instant, with its fraction of a second, under
    /// any offset that the server writes. Other fields stay as they are.
    #[hegel::test]
    fn localize_rfc3339_fields_keeps_the_instant(tc: hegel::TestCase) {
        let seconds = tc.draw(
            generators::integers::<i64>()
                .min_value(0)
                .max_value(250_000_000_000),
        );
        let nanos = tc.draw(generators::integers::<u32>().max_value(999_999_999));
        let minutes = tc.draw(
            generators::integers::<i32>()
                .min_value(-12 * 60)
                .max_value(14 * 60),
        );
        let time = DateTime::from_timestamp(seconds, nanos).unwrap();
        let offset = FixedOffset::east_opt(minutes * 60).unwrap();
        let server = time.with_timezone(&offset).to_rfc3339();
        let mut object = serde_json::json!({ "at": server, "text": "2025-03-20T02:00:00Z" });
        localize_rfc3339_fields(&mut object, &["at", "missing"]);
        let local = object["at"].as_str().unwrap();
        assert_eq!(
            DateTime::parse_from_rfc3339(local).unwrap(),
            time,
            "{local}"
        );
        assert_eq!(object["text"], "2025-03-20T02:00:00Z");
    }

    fn wall_time(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, DISPLAY_FORMAT).unwrap()
    }

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    /// New York springs forward an hour at 02:00, and Lord Howe half an hour.
    #[test]
    fn a_skipped_wall_time_does_not_exist() {
        assert_eq!(
            instant_of_wall_time(&New_York, wall_time("2026-03-08 02:30:00")),
            None
        );
        assert_eq!(
            instant_of_wall_time(&Lord_Howe, wall_time("2026-10-04 02:15:00")),
            None
        );
    }

    /// New York falls back an hour at 02:00, and Lord Howe half an hour.
    #[test]
    fn a_repeated_wall_time_is_the_earlier_instant() {
        let new_york = instant_of_wall_time(&New_York, wall_time("2026-11-01 01:30:00"));
        assert_eq!(new_york, Some(utc("2026-11-01T05:30:00Z")));
        let lord_howe = instant_of_wall_time(&Lord_Howe, wall_time("2026-04-05 01:45:00"));
        assert_eq!(lord_howe, Some(utc("2026-04-04T14:45:00Z")));
    }

    /// Santiago springs forward at midnight, so 2026-09-06 starts at 01:00.
    #[test]
    fn a_day_whose_midnight_is_skipped_starts_at_its_first_instant() {
        let date = NaiveDate::from_ymd_opt(2026, 9, 6).unwrap();
        let start = start_of_calendar_day(&Santiago, date);
        assert_eq!(
            start.with_timezone(&Santiago).naive_local(),
            wall_time("2026-09-06 01:00:00")
        );
        assert!(start_of_calendar_day(&Santiago, date.pred_opt().unwrap()) < start);
    }

    /// The wall times in 2026 at which the clock of `tz` changes, to the
    /// minute, as the clock shows them just before the change.
    fn clock_changes(tz: Tz) -> Vec<NaiveDateTime> {
        let offset = |time: DateTime<Utc>| tz.offset_from_utc_datetime(&time.naive_utc()).fix();
        let start = utc("2026-01-01T00:00:00Z");
        (0..365 * 24 * 60)
            .map(|minute| start + TimeDelta::minutes(minute))
            .filter(|time| offset(*time - TimeDelta::minutes(1)) != offset(*time))
            .map(|time| time.naive_utc() + offset(time - TimeDelta::minutes(1)))
            .collect()
    }

    /// Near the clock changes of a zone, a wall time that exists converts to
    /// an instant that shows that wall time, a repeated one to the earlier
    /// instant, and a skipped one to `None`. A later wall time never gives an
    /// earlier instant.
    #[hegel::test]
    fn instant_of_wall_time_handles_clock_changes(tc: hegel::TestCase) {
        static CLOCK_CHANGES: OnceLock<Vec<Vec<NaiveDateTime>>> = OnceLock::new();
        let zones = [New_York, Lord_Howe, Tokyo];
        let changes =
            CLOCK_CHANGES.get_or_init(|| zones.iter().map(|tz| clock_changes(*tz)).collect());
        let zone = tc.draw(generators::integers::<usize>().max_value(zones.len() - 1));
        let tz = zones[zone];
        let center = match changes[zone].as_slice() {
            [] => wall_time("2026-06-01 00:00:00"),
            all => all[tc.draw(generators::integers::<usize>().max_value(all.len() - 1))],
        };
        let minutes = || generators::integers::<i64>().min_value(-180).max_value(180);
        let (a, b) = (tc.draw(minutes()), tc.draw(minutes()));
        let (early, late) = (
            center + TimeDelta::minutes(a.min(b)),
            center + TimeDelta::minutes(a.max(b)),
        );
        for wall in [early, late] {
            let instant = instant_of_wall_time(&tz, wall);
            match tz.from_local_datetime(&wall) {
                LocalResult::Single(_) => assert_eq!(
                    instant.map(|time| time.with_timezone(&tz).naive_local()),
                    Some(wall)
                ),
                LocalResult::Ambiguous(x, y) => assert_eq!(instant, Some(x.min(y).to_utc())),
                LocalResult::None => assert_eq!(instant, None, "{wall}"),
            }
        }
        if let (Some(early_instant), Some(late_instant)) = (
            instant_of_wall_time(&tz, early),
            instant_of_wall_time(&tz, late),
        ) {
            assert!(early_instant <= late_instant, "{early} {late}");
        }
    }

    /// Parsing arbitrary text must never panic — it returns `Ok` or
    /// `ParseDurationError`, but the `from_str`/`from_minutes`/`parse_units`
    /// path (with its `as u64` casts and `checked_*` arithmetic) is the kind of
    /// code that overflows or indexes out of bounds on hostile input.
    #[hegel::test]
    fn parse_never_panics(tc: hegel::TestCase) {
        let s = tc.draw(generators::text());
        let _ = s.parse::<HumanDuration>();
    }

    /// `from_minutes` is `Some` exactly for finite, non-negative inputs, and
    /// when it succeeds the stored seconds are the minutes scaled by 60 and
    /// rounded (saturating, never panicking, on absurd values). Drawn from the
    /// full `f64` domain so NaN, infinities, negatives, and huge magnitudes are
    /// all exercised.
    #[hegel::test]
    fn from_minutes_matches_contract(tc: hegel::TestCase) {
        let minutes = tc.draw(generators::floats::<f64>());
        match HumanDuration::from_minutes(minutes) {
            Some(d) => {
                assert!(minutes.is_finite() && minutes >= 0.0);
                assert_eq!(d.seconds(), (minutes * 60.0).round() as u64);
            }
            None => assert!(!minutes.is_finite() || minutes < 0.0),
        }
    }

    /// `minutes()` is the documented integer-space rounding of the second
    /// count to hundredths of a minute — no float drift, no long decimals.
    /// Bounded to `u32`-range seconds (~136 years) so the `* 100.0` round-trip
    /// stays exactly representable in `f64`; the integer arithmetic the test
    /// mirrors is what actually runs in production.
    #[hegel::test]
    fn minutes_is_integer_space_rounding(tc: hegel::TestCase) {
        let seconds = u64::from(tc.draw(generators::integers::<u32>()));
        let d = HumanDuration::from_seconds(seconds);
        let expected_hundredths = (u128::from(seconds) * 100 + 30) / 60;
        assert_eq!((d.minutes() * 100.0).round() as u128, expected_hundredths);
    }

    /// Over the `debug --duration` range, `hours()` is the second count rounded
    /// to the nearest hundredth of an hour, and prints as a plain decimal.
    #[hegel::test]
    fn hours_is_close_and_short(tc: hegel::TestCase) {
        let seconds = tc.draw(
            generators::integers::<u64>()
                .min_value(1800)
                .max_value(21600),
        );
        let hours = HumanDuration::from_seconds(seconds).hours();
        assert_eq!(
            (hours * 100.0).round() as u64,
            (seconds * 100 + 1800) / 3600
        );
        let text = hours.to_string();
        assert!(
            text.split_once('.').is_none_or(|(_, frac)| frac.len() <= 2),
            "{text}"
        );
    }
}
