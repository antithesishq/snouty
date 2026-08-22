//! The `--limit` types for the endpoints that take a row limit.
//!
//! A limited listing asks the server for one row more than it prints. That
//! row's arrival is the only proof that more rows exist, and it drives the
//! truncation note; it is never displayed. Reserving it is the whole reason
//! this type exists: the reservation is easy to write on one path and forget
//! on the next, and the bug it causes is silent — the output looks complete.
//!
//! So the arithmetic lives here and nowhere else. [`PROBE_ROW`] is subtracted
//! once, to derive the largest value a flag accepts, and added once, in
//! [`Limit::for_request`]. Everything outside this file — the clap parser, the
//! commands, the render pipeline — deals in the number of rows the user asked
//! for.

use std::fmt;
use std::num::NonZeroU64;
use std::str::FromStr;

/// The row a limited listing asks for beyond the rows it prints.
const PROBE_ROW: u64 = 1;

/// A row limit for an endpoint whose spec caps `limit` at `SERVER_MAX`.
///
/// The value inside is what the user asked to see. It stops one row below
/// `SERVER_MAX` so that the probe row stays inside the server's range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Limit<const SERVER_MAX: u64>(NonZeroU64);

impl<const SERVER_MAX: u64> Limit<SERVER_MAX> {
    /// The largest number of rows the flag accepts.
    pub const MAX: u64 = SERVER_MAX - PROBE_ROW;

    /// A limit checked at compile time — for the defaults the flags declare.
    pub const fn new(rows: u64) -> Self {
        assert!(rows >= 1, "a limit of 0 rows asks for nothing");
        assert!(rows <= Self::MAX, "limit above the endpoint's ceiling");
        match NonZeroU64::new(rows) {
            Some(rows) => Self(rows),
            None => unreachable!(),
        }
    }

    /// The number of rows to print.
    pub fn get(self) -> u64 {
        self.0.get()
    }

    /// The `limit` a request must name: one row past what gets printed — the
    /// reservation the flag's ceiling already leaves room for, so the sum
    /// stays within `SERVER_MAX`.
    ///
    /// Reserving the row costs nothing on its own. Both endpoints stream,
    /// and a caller that prints no truncation note simply stops pulling at
    /// the limit, so the reserved row is never fetched.
    pub fn for_request(self) -> NonZeroU64 {
        self.0.saturating_add(PROBE_ROW)
    }
}

impl<const SERVER_MAX: u64> FromStr for Limit<SERVER_MAX> {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // `NonZeroU64` rejects 0 with a plain message, and it is the type the
        // generated request builders carry.
        let rows: NonZeroU64 = value
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        if rows.get() > Self::MAX {
            return Err(format!("must be at most {}", Self::MAX));
        }
        Ok(Self(rows))
    }
}

impl<const SERVER_MAX: u64> fmt::Display for Limit<SERVER_MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// `runs events --limit` and `runs search --limit`. Both events endpoints —
/// the GET events endpoint and the events-search endpoint — document `limit`
/// as 1..=999 (`openapi.json`).
pub type EventsLimit = Limit<999>;

/// `runs list --limit`. The runs endpoint is paged and sets no ceiling on the
/// total, so only the probe row bounds this one.
pub type RunsLimit = Limit<{ u64::MAX }>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_stops_one_row_below_the_server_ceiling() {
        assert_eq!(EventsLimit::MAX, 998);
        assert_eq!("998".parse::<EventsLimit>().unwrap().get(), 998);
        assert_eq!(
            "999".parse::<EventsLimit>().unwrap_err(),
            "must be at most 998"
        );
        assert!("0".parse::<EventsLimit>().is_err());
        assert!("-1".parse::<EventsLimit>().is_err());
    }

    #[test]
    fn a_request_asks_for_one_row_more_and_stays_in_range() {
        assert_eq!(EventsLimit::new(998).for_request().get(), 999);
        assert_eq!(EventsLimit::new(1).for_request().get(), 2);
    }

    #[test]
    fn an_unbounded_endpoint_still_reserves_the_probe_row() {
        assert_eq!(RunsLimit::MAX, u64::MAX - 1);
        assert_eq!(RunsLimit::new(RunsLimit::MAX).for_request().get(), u64::MAX);
    }
}
