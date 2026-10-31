//! [`CachePolicy`] — an admission verdict on a fetched API response.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use headers::{CacheControl, HeaderMapExt};

/// Whether, and for how long, the cache may store a fetched value. Each `#[cached]` handler
/// tags its value with the verdict from its response's headers (see
/// [`crate::api_cache::ApiCache::headers_policy`]).
#[derive(Clone, Debug)]
pub enum CachePolicy {
    /// Store the value and serve it for this long.
    Cacheable(Duration),
    /// A stream that never changes once complete. It is stored for good only
    /// if the flag is set when it ends. A value with this tag is never stored.
    CacheableOnceComplete(Arc<AtomicBool>),
    Uncacheable,
}

impl CachePolicy {
    /// For a value that never changes.
    pub const IMMUTABLE: Self = Self::Cacheable(Duration::MAX);

    /// The verdict `Cache-Control` and `Age` give a private client cache:
    /// cacheable for the response's remaining freshness lifetime. Entries are
    /// never revalidated, so any directive that demands revalidation means
    /// [`CachePolicy::Uncacheable`]; an absent or malformed header grants
    /// nothing. `private` is fine — the cache directory is per-user.
    pub fn from_headers(headers: &http::HeaderMap) -> Self {
        freshness_lifetime(headers).map_or(Self::Uncacheable, Self::Cacheable)
    }
}

/// The response's remaining freshness lifetime: `max-age` less `Age`, or
/// `None` when that is not positive or a directive forbids caching (see
/// [`CachePolicy::from_headers`]).
fn freshness_lifetime(headers: &http::HeaderMap) -> Option<Duration> {
    // Directive names are case-insensitive (RFC 9111 §5.2) but the parser
    // matches lowercase only — a `No-Cache` veto would pass unrecognized.
    // Lowercase each value first; none of the inspected directives carries a
    // case-sensitive argument.
    let mut lowered = http::HeaderMap::new();
    for value in headers.get_all(http::header::CACHE_CONTROL) {
        let normalized = value
            .to_str()
            .ok()
            .and_then(|text| text.to_ascii_lowercase().parse().ok());
        lowered.append(http::header::CACHE_CONTROL, normalized?);
    }
    let cache_control = lowered.typed_get::<CacheControl>()?;
    if cache_control.no_store() || cache_control.no_cache() || cache_control.must_revalidate() {
        return None;
    }
    // Of a list-valued `Age`, only the first member counts. A malformed
    // `Age` is ignored, and one too large to represent is treated as the
    // largest age (RFC 9111 §1.2.2, §5.1).
    let age = headers
        .get(http::header::AGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.split(',').next())
        .map(str::trim)
        .filter(|text| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
        .map_or(Duration::ZERO, |text| {
            Duration::from_secs(text.parse().unwrap_or(u64::MAX))
        });
    cache_control
        .max_age()?
        .checked_sub(age)
        .filter(|lifetime| !lifetime.is_zero())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        for (name, value) in pairs {
            headers.append(*name, value.parse().unwrap());
        }
        headers
    }

    fn lifetime(cache_control: &str) -> Option<Duration> {
        freshness_lifetime(&headers(&[("cache-control", cache_control)]))
    }

    #[test]
    fn the_lifetime_is_max_age() {
        // The exact header the live API sends on cacheable reads.
        assert_eq!(
            lifetime(crate::testutils::CACHEABLE_CACHE_CONTROL),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(lifetime("max-age=1"), Some(Duration::from_secs(1)));
        assert_eq!(
            lifetime("max-age=60, immutable"),
            Some(Duration::from_secs(60))
        );
        // Mixed case grants too, not only vetoes.
        assert_eq!(
            lifetime("Private, Max-Age=3600"),
            Some(Duration::from_secs(3600))
        );
    }

    #[test]
    fn age_shortens_the_lifetime() {
        let lifetime = |age: &str| {
            freshness_lifetime(&headers(&[("cache-control", "max-age=3600"), ("age", age)]))
        };
        assert_eq!(lifetime("600"), Some(Duration::from_secs(3000)));
        assert_eq!(lifetime("3600"), None);
        assert_eq!(lifetime("7200"), None);
        assert_eq!(lifetime("soon"), Some(Duration::from_secs(3600)));
        assert_eq!(lifetime("99999999999999999999"), None);
        assert_eq!(lifetime("600, 30"), Some(Duration::from_secs(3000)));
        assert_eq!(lifetime(" 600 "), Some(Duration::from_secs(3000)));
    }

    // The lifetime is max-age less Age while that is positive, and a
    // malformed Age leaves max-age as it is.
    #[hegel::test]
    fn the_lifetime_is_max_age_less_age(tc: hegel::TestCase) {
        use hegel::generators;

        let max_age = tc.draw(generators::integers::<u32>());
        let age = tc.draw(generators::integers::<u64>());
        let cache_control = format!("max-age={max_age}");
        let lifetime = |age: &str| {
            freshness_lifetime(&headers(&[
                ("cache-control", cache_control.as_str()),
                ("age", age),
            ]))
        };
        let expected = u64::from(max_age)
            .checked_sub(age)
            .filter(|left| *left > 0)
            .map(Duration::from_secs);
        assert_eq!(lifetime(&age.to_string()), expected);

        let malformed = tc.draw(generators::text().alphabet("ab -.+"));
        let expected = (max_age > 0).then(|| Duration::from_secs(max_age.into()));
        assert_eq!(lifetime(&malformed), expected);
    }

    #[test]
    fn absent_or_non_positive_headers_deny_caching() {
        assert_eq!(freshness_lifetime(&http::HeaderMap::new()), None);
        assert_eq!(lifetime("max-age=0"), None);
        assert_eq!(lifetime("private"), None);
        // `immutable` describes a lifetime; it does not grant one.
        assert_eq!(lifetime("immutable"), None);
        assert_eq!(lifetime("not a directive ==="), None);
    }

    #[test]
    fn revalidation_directives_deny_caching() {
        assert_eq!(lifetime("no-cache"), None);
        assert_eq!(lifetime("no-store"), None);
        assert_eq!(lifetime("no-cache, max-age=3600"), None);
        assert_eq!(lifetime("no-store, max-age=3600, immutable"), None);
        assert_eq!(lifetime("max-age=3600, must-revalidate"), None);
        // Directive names are case-insensitive (RFC 9111 §5.2): a mixed-case
        // veto must still veto.
        assert_eq!(lifetime("No-Cache, max-age=3600"), None);
    }
}
