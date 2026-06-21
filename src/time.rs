//! Run durations.
//!
//! Antithesis takes a run's duration in minutes, and has historically accepted
//! a fractional value (e.g. `0.05` for three seconds). [`ReportDuration`]
//! preserves that, while also accepting the unit forms people reach for
//! (`1h30m`, `2h`, `30s`).
//!
//! A bare number is read as a (possibly fractional) count of minutes and
//! rounded to whole seconds — the finest resolution we keep. Unit forms use
//! whole-number components (`h`/`m`/`s`, in that order); fractional components
//! like `1.5h` are rejected, since the bare-minutes form already covers that.

use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

const SECS_PER_MINUTE: u64 = 60;
const SECS_PER_HOUR: u64 = 60 * SECS_PER_MINUTE;

/// A run's configured duration, carried in the `antithesis.duration` report
/// parameter. Held internally as a [`Duration`] rounded to whole seconds.
///
/// Parse one from the strings people type:
/// ```
/// # use snouty::time::ReportDuration;
/// assert_eq!("1h30m".parse::<ReportDuration>().unwrap().minutes(), 90.0);
/// assert_eq!("0.05".parse::<ReportDuration>().unwrap().minutes(), 0.05); // fractional minutes
/// assert_eq!("7s".parse::<ReportDuration>().unwrap().minutes(), 0.12);   // rounded to 2 dp
/// assert!("1.5h".parse::<ReportDuration>().is_err());                    // units stay whole
/// // Display uses the minimized h/m/s form:
/// assert_eq!("0.05".parse::<ReportDuration>().unwrap().to_string(), "3s");
/// assert_eq!("90".parse::<ReportDuration>().unwrap().to_string(), "1h30m");
/// ```
///
/// [`Display`](fmt::Display) renders the minimized `h`/`m`/`s` form. For the
/// `antithesis.duration` parameter use [`minutes()`](Self::minutes)`.to_string()`
/// instead — that's the (possibly fractional) minute count the API expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReportDuration(Duration);

impl ReportDuration {
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
    /// form the API wants. The hundredths are computed (and rounded) in integer
    /// space rather than dividing as floats, so a second-granular value stays
    /// clean instead of turning into a long repeating decimal: e.g. 7s is
    /// `0.12`, not `0.11666…`.
    pub fn minutes(&self) -> f64 {
        let per_min = SECS_PER_MINUTE as u128;
        let hundredths = (self.seconds() as u128 * 100 + per_min / 2) / per_min;
        hundredths as f64 / 100.0
    }

    /// The duration as a whole number of seconds.
    pub fn seconds(&self) -> u64 {
        self.0.as_secs()
    }

    /// The underlying [`Duration`], for use with timers and the like.
    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

impl fmt::Display for ReportDuration {
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

impl AsRef<Duration> for ReportDuration {
    fn as_ref(&self) -> &Duration {
        &self.0
    }
}

impl From<ReportDuration> for Duration {
    fn from(value: ReportDuration) -> Self {
        value.0
    }
}

impl FromStr for ReportDuration {
    type Err = ParseDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();

        // A bare number is (fractional) minutes, kept for backwards
        // compatibility; otherwise expect whole-number `h`/`m`/`s` components.
        if is_decimal(&s) {
            let minutes: f64 = s.parse().map_err(|_| ParseDurationError)?;
            return Self::from_minutes(minutes).ok_or(ParseDurationError);
        }

        parse_units(&s)
            .map(Self::from_seconds)
            .ok_or(ParseDurationError)
    }
}

/// Error from parsing a [`ReportDuration`]. Its message lists the accepted
/// forms; clap prefixes it with the offending value and flag name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDurationError;

impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "must be a number of minutes (e.g. `30`) or use h/m units \
             (e.g. `90m`, `2h`, `1h30m`)",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn minutes(s: &str) -> f64 {
        s.parse::<ReportDuration>().unwrap().minutes()
    }

    fn seconds(s: &str) -> u64 {
        s.parse::<ReportDuration>().unwrap().seconds()
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
        assert_eq!(ReportDuration::from_seconds(3).minutes(), 0.05);
        assert_eq!(ReportDuration::from_seconds(7).minutes(), 0.12); // 0.11666… rounded up
        assert_eq!(ReportDuration::from_seconds(5).minutes(), 0.08); // 0.08333… rounded down
        assert_eq!(ReportDuration::from_seconds(5442).minutes(), 90.7); // float-divide would give 90.69
        assert_eq!(ReportDuration::from_seconds(5445).minutes(), 90.75);
        // ...and the string sent to the API stays clean.
        assert_eq!(
            ReportDuration::from_seconds(7).minutes().to_string(),
            "0.12"
        );
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
                bad.parse::<ReportDuration>(),
                Err(ParseDurationError),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn from_minutes_rejects_non_finite_and_negative() {
        assert_eq!(ReportDuration::from_minutes(f64::NAN), None);
        assert_eq!(ReportDuration::from_minutes(f64::INFINITY), None);
        assert_eq!(ReportDuration::from_minutes(-1.0), None);
    }

    #[test]
    fn display_renders_minimized_hms() {
        let display = |s: &str| s.parse::<ReportDuration>().unwrap().to_string();
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
            let parsed = s.parse::<ReportDuration>().unwrap();
            let reparsed = parsed.to_string().parse::<ReportDuration>().unwrap();
            assert_eq!(parsed, reparsed, "{s:?} did not round-trip through Display");
        }
    }

    #[test]
    fn converts_to_duration() {
        let d = "1h30m".parse::<ReportDuration>().unwrap();
        assert_eq!(d.as_duration(), Duration::from_secs(5400));
        assert_eq!(Duration::from(d), Duration::from_secs(5400));
        assert_eq!(d.as_ref(), &Duration::from_secs(5400));
    }
}
