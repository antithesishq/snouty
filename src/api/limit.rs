//! The `--limit` types for the endpoints that take a row limit.
//!
//! A limited listing asks the server for one row more than it prints. That
//! row's arrival is the only proof that more rows exist, and it drives the
//! truncation note; it is never displayed.
//!
//! The arithmetic lives here and nowhere else. [`PROBE_ROW`] is subtracted
//! once, to derive the largest value a flag accepts, and added once, in
//! [`Limit::for_request`]. Everything else — the clap parser, the commands,
//! the render pipeline — deals in the number of rows the user asked for.

use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;

/// The row a limited listing asks for beyond the rows it prints.
const PROBE_ROW: usize = 1;

/// A row limit for an endpoint whose spec caps `limit` at `SERVER_MAX`.
///
/// The value inside is what the user asked to see. It stops one row below
/// `SERVER_MAX` so that the probe row stays inside the server's range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Limit<const SERVER_MAX: usize>(NonZeroUsize);

impl<const SERVER_MAX: usize> Limit<SERVER_MAX> {
    /// The largest number of rows the flag accepts.
    pub const MAX: usize = SERVER_MAX - PROBE_ROW;

    /// A limit checked at compile time — for the defaults the flags declare.
    pub const fn new(rows: usize) -> Self {
        assert!(rows >= 1, "a limit of 0 rows asks for nothing");
        assert!(rows <= Self::MAX, "limit above the endpoint's ceiling");
        match NonZeroUsize::new(rows) {
            Some(rows) => Self(rows),
            None => unreachable!(),
        }
    }

    /// The requested limit.
    pub fn get(self) -> usize {
        self.0.get()
    }

    /// The `limit` a request must name: one row past what gets printed.
    /// [`Limit::MAX`] leaves room for it, so the sum stays within
    /// `SERVER_MAX`.
    pub fn for_request(self) -> NonZeroUsize {
        self.0.saturating_add(PROBE_ROW)
    }
}

impl<const SERVER_MAX: usize> FromStr for Limit<SERVER_MAX> {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // `NonZeroUsize` rejects 0 with a plain message of its own.
        let rows: NonZeroUsize = value
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        if rows.get() > Self::MAX {
            return Err(format!("must be at most {}", Self::MAX));
        }
        Ok(Self(rows))
    }
}

impl<const SERVER_MAX: usize> fmt::Display for Limit<SERVER_MAX> {
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
pub type RunsLimit = Limit<{ usize::MAX }>;

/// The server's default `limit` on both events endpoints, applied when the
/// request names none. A caller that enforces the limit client-side caps at
/// this value when no explicit limit was given.
pub const SEARCH_DEFAULT_LIMIT: EventsLimit = EventsLimit::new(50);

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
        assert_eq!(RunsLimit::MAX, usize::MAX - 1);
        assert_eq!(
            RunsLimit::new(RunsLimit::MAX).for_request().get(),
            usize::MAX
        );
    }

    #[test]
    fn an_optional_limit_costs_no_extra_word() {
        // The inner `NonZeroUsize` gives `Option<Limit>` a niche, so an
        // absent limit costs no discriminant. A single-field struct inherits
        // that without `repr(transparent)`.
        assert_eq!(
            std::mem::size_of::<Option<EventsLimit>>(),
            std::mem::size_of::<EventsLimit>()
        );
    }
}
