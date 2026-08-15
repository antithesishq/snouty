//! Opening URLs in the user's default browser.

use color_eyre::eyre::{Result, eyre};

/// Normalize `url` and require an `http`/`https` scheme. Anything else
/// (`file:`, `javascript:`, garbage) is rejected rather than handed to an OS
/// launcher.
fn validate_browser_launch_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .filter(|parsed| matches!(parsed.scheme(), "http" | "https"))
        .map(|parsed| parsed.as_str().to_owned())
}

/// Best-effort, non-blocking open of `url` in the user's default browser.
///
/// Returns `Ok(true)` when an opener was launched, `Ok(false)` when launching
/// one failed, and `Err` when `url` is not a valid HTTP(S) URL (such a URL is
/// never handed to an OS launcher). Callers always also print the URL, so a
/// headless or opener-less environment can still open it by hand.
pub fn open_in_browser(url: &str) -> Result<bool> {
    let url =
        validate_browser_launch_url(url).ok_or_else(|| eyre!("not a valid HTTP(S) URL: {url}"))?;
    Ok(open::that_detached(url).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_launch_url_accepts_only_http_and_https() {
        // Real authorization URLs (http/https) pass through.
        assert!(validate_browser_launch_url("https://idp.example.com/authorize?a=1&b=2").is_some());
        assert!(validate_browser_launch_url("http://localhost:12345/callback").is_some());

        // Anything else is dropped rather than handed to an OS launcher.
        assert_eq!(validate_browser_launch_url("file:///etc/passwd"), None);
        assert_eq!(validate_browser_launch_url("javascript:alert(1)"), None);
        assert_eq!(validate_browser_launch_url("ftp://example.com/x"), None);
        assert_eq!(validate_browser_launch_url("not a url"), None);
        assert_eq!(validate_browser_launch_url(""), None);
    }

    #[test]
    fn browser_launch_url_preserves_ampersand_query_params() {
        // The `&`-separated params (what an unquoted Windows shell would
        // mangle) survive normalization intact.
        let normalized = validate_browser_launch_url(
            "https://idp/authorize?response_type=code&client_id=x&state=y",
        )
        .expect("valid https URL");
        assert!(
            normalized.contains("response_type=code"),
            "got: {normalized}"
        );
        assert!(normalized.contains("&client_id=x"), "got: {normalized}");
        assert!(normalized.contains("&state=y"), "got: {normalized}");
    }
}
