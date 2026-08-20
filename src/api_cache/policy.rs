//! [`CachePolicy`] — an admission verdict on a fetched API response.

use headers::{CacheControl, HeaderMapExt};

/// A handler's admission verdict on the value it fetched. The cache stores
/// nothing untagged: `#[cached]` requires the handler body to return a
/// [`crate::tag::Tagged`] value, so admission lives in the handler — next to
/// the response, whose headers carry the other half of the verdict (see
/// [`crate::api_cache::ApiCache::headers_policy`]).
#[derive(Clone, Copy, Debug)]
pub enum CachePolicy {
    Cacheable,
    Uncacheable,
}

impl CachePolicy {
    /// [`CachePolicy::Cacheable`] when `cacheable` is true.
    pub fn cache_if(cacheable: bool) -> Self {
        if cacheable {
            Self::Cacheable
        } else {
            Self::Uncacheable
        }
    }

    /// The verdict `Cache-Control` gives a private client cache. Entries are
    /// never revalidated and never expire, so any directive that demands
    /// revalidation means [`CachePolicy::Uncacheable`]; an absent or
    /// malformed header grants nothing. `private` is fine — the cache
    /// directory is per-user.
    pub fn from_headers(headers: &http::HeaderMap) -> Self {
        Self::cache_if(cache_headers_allow(headers))
    }

    /// [`CachePolicy::Cacheable`] only when both verdicts are.
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Cacheable, Self::Cacheable) => Self::Cacheable,
            _ => Self::Uncacheable,
        }
    }
}

/// Whether `Cache-Control` lets the cache hold the response: a positive
/// freshness lifetime and no revalidation demand (see
/// [`CachePolicy::from_headers`]).
fn cache_headers_allow(headers: &http::HeaderMap) -> bool {
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
        match normalized {
            Some(value) => lowered.append(http::header::CACHE_CONTROL, value),
            None => return false,
        };
    }
    let Some(cache_control) = lowered.typed_get::<CacheControl>() else {
        return false;
    };
    if cache_control.no_store() || cache_control.no_cache() || cache_control.must_revalidate() {
        return false;
    }
    cache_control.immutable() || cache_control.max_age().is_some_and(|age| !age.is_zero())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_control(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        headers.insert("cache-control", value.parse().unwrap());
        headers
    }

    #[test]
    fn a_positive_freshness_lifetime_allows_caching() {
        // The exact header the live API sends on cacheable reads.
        assert!(cache_headers_allow(&cache_control(
            crate::testutils::CACHEABLE_CACHE_CONTROL
        )));
        assert!(cache_headers_allow(&cache_control("max-age=1")));
        assert!(cache_headers_allow(&cache_control("immutable")));
        // Mixed case grants too, not only vetoes.
        assert!(cache_headers_allow(&cache_control("Private, Max-Age=3600")));
    }

    #[test]
    fn absent_or_non_positive_headers_deny_caching() {
        assert!(!cache_headers_allow(&http::HeaderMap::new()));
        assert!(!cache_headers_allow(&cache_control("max-age=0")));
        assert!(!cache_headers_allow(&cache_control("private")));
        assert!(!cache_headers_allow(&cache_control("not a directive ===")));
    }

    #[test]
    fn revalidation_directives_deny_caching() {
        assert!(!cache_headers_allow(&cache_control("no-cache")));
        assert!(!cache_headers_allow(&cache_control("no-store")));
        assert!(!cache_headers_allow(&cache_control(
            "no-cache, max-age=3600"
        )));
        assert!(!cache_headers_allow(&cache_control("no-store, immutable")));
        assert!(!cache_headers_allow(&cache_control(
            "max-age=3600, must-revalidate"
        )));
        // Directive names are case-insensitive (RFC 9111 §5.2): a mixed-case
        // veto must still veto.
        assert!(!cache_headers_allow(&cache_control(
            "No-Cache, max-age=3600"
        )));
    }
}
