//! `VTime`: a moment's virtual time, in seconds.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

/// Ticks per second of virtual time: the hypervisor counts 2^32 ticks per
/// second, and the API reports a vtime as `ticks / 2^32` seconds.
const TICKS_PER_SECOND: f64 = 4294967296.0;

/// A moment's virtual time in seconds.
///
/// A vtime is precision-sensitive: a moment sent back to the API must carry
/// exactly the value the API named, or it identifies the wrong moment. An
/// `f64` gives that guarantee by construction: `ticks / 2^32` has a
/// power-of-two denominator, so the quotient is exact in an `f64` for any
/// tick count below 2^53 (a vtime of ~24.3 days); `str::parse::<f64>` is
/// correctly-rounded, so it recovers the exact value the API printed; and
/// both [`fmt::Display`] and the JSON output print the shortest text that
/// parses back to the same `f64`. Every path through this type is therefore
/// value-exact.
///
/// Serialization writes a JSON *number* (not the API's string form): a number
/// can't be compared alphanumerically by accident, where `"1000.0" < "9.0"`
/// is true but wrong.
#[derive(Clone, Copy, Debug)]
pub struct VTime(f64);

impl VTime {
    /// The placeholder vtime (`"0"`) the API reports when a run has no
    /// moment-pinned failure.
    pub const ZERO: VTime = VTime(0.0);

    /// A vtime from raw seconds, or `None` when the value is not finite —
    /// the invariant every constructor enforces (see the `Ord` impl).
    pub fn from_seconds(seconds: f64) -> Option<VTime> {
        if !seconds.is_finite() {
            return None;
        }
        let vtime = VTime(seconds);
        if !vtime.is_tick_aligned() {
            // Every vtime the API prints today sits on a hypervisor tick. A
            // value off the tick grid means the API changed representation —
            // surface that under --verbose instead of absorbing it silently.
            log::debug!("vtime {seconds} is not tick-aligned (ticks / 2^32)");
        }
        Some(vtime)
    }

    /// The raw seconds value, for arithmetic that leaves the vtime domain.
    pub fn as_seconds(self) -> f64 {
        self.0
    }

    /// This vtime plus a duration in seconds — e.g. a fault window's end
    /// projected from its start and `max_duration`. Adding two finite values
    /// can overflow to infinity but can never produce a NaN, so the `Ord`
    /// invariant holds.
    pub fn plus_seconds(self, seconds: f64) -> VTime {
        VTime(self.0 + seconds)
    }

    /// Whether this vtime sits exactly on a hypervisor tick (an integer
    /// count of 1/2^32 seconds). Every vtime the API reports is tick-aligned.
    pub fn is_tick_aligned(self) -> bool {
        (self.0 * TICKS_PER_SECOND).fract() == 0.0
    }
}

/// `total_cmp` is a total order over `f64`, which is what lets `VTime` be
/// `Eq`/`Ord` at all. Using it is sound only because a NaN can never enter
/// the type: `FromStr` and `Deserialize` reject non-finite values, and
/// arithmetic on finite values can't produce one. Equality is defined through
/// the same comparison so `Eq` and `Ord` always agree.
impl Ord for VTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for VTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for VTime {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for VTime {}

/// Print with ryu — the same shortest-round-trip formatter `serde_json` uses
/// for numbers — so the human-visible text always agrees byte-for-byte with
/// the JSON output. (std's `{}` float formatting differs: it prints `402`
/// where JSON needs `402.0`.)
impl fmt::Display for VTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(ryu::Buffer::new().format(self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseVTimeError;

impl fmt::Display for ParseVTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("vtime is not a finite decimal number")
    }
}

impl std::error::Error for ParseVTimeError {}

impl FromStr for VTime {
    type Err = ParseVTimeError;

    /// `str::parse::<f64>` is correctly-rounded, recovering the exact `f64`
    /// the API printed. It also accepts `inf`/`NaN`, which the finiteness
    /// check rejects.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let seconds = s.parse::<f64>().map_err(|_| ParseVTimeError)?;
        VTime::from_seconds(seconds).ok_or(ParseVTimeError)
    }
}

impl serde::Serialize for VTime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_f64(self.0)
    }
}

/// An explicit visitor rather than `#[serde(untagged)]`: untagged buffers the
/// value through serde's private `Content` type, which makes the string path
/// less predictable; the visitor takes each input shape directly.
struct VTimeVisitor;

impl serde::de::Visitor<'_> for VTimeVisitor {
    type Value = VTime;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a vtime: a finite number of seconds, or its decimal-string form")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<VTime, E> {
        v.parse().map_err(E::custom)
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<VTime, E> {
        VTime::from_seconds(v).ok_or_else(|| E::custom(ParseVTimeError))
    }

    // serde_json hands a whole-number JSON value (e.g. `402`) to the integer
    // visitors, not `visit_f64`. The `as f64` conversion is exact below 2^53.
    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<VTime, E> {
        self.visit_f64(v as f64)
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<VTime, E> {
        self.visit_f64(v as f64)
    }
}

impl<'de> serde::Deserialize<'de> for VTime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(VTimeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every full-precision vtime that appears in the repository's fixtures
    /// and specs. Each one is the API's own print of `ticks / 2^32`.
    const REAL_VTIMES: &[&str] = &[
        "398.4898056755774",
        "313.15126806590706",
        "22.475549569819123",
        "45.334635781589895",
        "329.8037810830865",
        "73.94233945617452",
        "311.8487535319291",
        "90.17225814750418",
        "22.66615231265314",
        "402.0",
    ];

    #[test]
    fn real_vtimes_round_trip_byte_identical() {
        for s in REAL_VTIMES {
            let vtime: VTime = s.parse().unwrap();
            assert_eq!(vtime.to_string(), *s, "Display of {s}");
            assert_eq!(
                serde_json::to_string(&vtime).unwrap(),
                *s,
                "JSON number of {s}"
            );
            assert!(vtime.is_tick_aligned(), "{s} should be tick-aligned");
        }
    }

    #[test]
    fn tick_counts_recover_exactly_across_the_range() {
        for ticks in [1u64, 1 << 20, 1 << 32, 1 << 42, 1 << 52, (1 << 53) - 1] {
            let seconds = ticks as f64 / TICKS_PER_SECOND;
            // Round-trip through the JSON number text, like a client would.
            let text = serde_json::to_string(&VTime::from_seconds(seconds).unwrap()).unwrap();
            let back: VTime = serde_json::from_str(&text).unwrap();
            assert_eq!(
                (back.as_seconds() * TICKS_PER_SECOND) as u64,
                ticks,
                "tick count {ticks} did not survive the round trip"
            );
        }
    }

    #[test]
    fn from_str_rejects_non_finite_and_non_numbers() {
        for s in ["NaN", "nan", "inf", "-inf", "infinity", "1e999", "", "abc"] {
            assert!(s.parse::<VTime>().is_err(), "{s:?} should be rejected");
        }
    }

    #[test]
    fn deserializes_from_string_and_number() {
        let from_string: VTime = serde_json::from_str("\"398.4898056755774\"").unwrap();
        let from_number: VTime = serde_json::from_str("398.4898056755774").unwrap();
        assert_eq!(from_string, from_number);
        // A whole-number JSON value arrives via the integer visitors.
        let from_integer: VTime = serde_json::from_str("402").unwrap();
        assert_eq!(from_integer.to_string(), "402.0");
        assert!(serde_json::from_str::<VTime>("\"NaN\"").is_err());
        assert!(serde_json::from_str::<VTime>("\"not a number\"").is_err());
        assert!(serde_json::from_str::<VTime>("true").is_err());
    }

    #[test]
    fn serializes_as_json_number() {
        let vtime: VTime = "45.334635781589895".parse().unwrap();
        assert!(serde_json::to_value(vtime).unwrap().is_number());
    }

    #[test]
    fn ord_is_numeric_not_alphanumeric() {
        let nine: VTime = "9.0".parse().unwrap();
        let thousand: VTime = "1000.0".parse().unwrap();
        assert!(nine < thousand);
        assert!(
            "1000.0" < "9.0",
            "the string order this type guards against"
        );
    }

    #[test]
    fn zero_equals_the_placeholder() {
        assert_eq!("0".parse::<VTime>().unwrap(), VTime::ZERO);
        assert_ne!("0.5".parse::<VTime>().unwrap(), VTime::ZERO);
    }

    #[test]
    fn plus_seconds_matches_fault_window_arithmetic() {
        // The fault-window boundary tests pin `end = max_duration + start`
        // exactly; plus_seconds must reproduce that sum bit-for-bit.
        let start: VTime = "5".parse().unwrap();
        assert_eq!(start.plus_seconds(5.0), "10".parse().unwrap());
        let clog: VTime = "401.5".parse().unwrap();
        assert_eq!(clog.plus_seconds(0.267), VTime(0.267 + 401.5));
    }

    #[test]
    fn tick_alignment_detects_off_grid_values() {
        assert!(VTime::ZERO.is_tick_aligned());
        assert!(!VTime::from_seconds(0.1).unwrap().is_tick_aligned());
    }
}
