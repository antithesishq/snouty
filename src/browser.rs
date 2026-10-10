//! Opening URLs in the user's default browser.

use color_eyre::eyre::{Result, eyre};

/// Parse `url` and require an `http`/`https` scheme. Anything else
/// (`file:`, `javascript:`, garbage) is rejected rather than handed to an OS
/// launcher or put in a link. The `Err` carries the reason (the parse error,
/// or the offending scheme) so callers can surface a hint to the user.
pub(crate) fn parse_http_url(url: &str) -> Result<reqwest::Url> {
    let parsed = reqwest::Url::parse(url).map_err(|err| eyre!("invalid URL: {err}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(eyre!(
            "refusing to open a {:?} URL; only http/https URLs are opened",
            parsed.scheme()
        ));
    }
    Ok(parsed)
}

/// Best-effort, non-blocking open of `url` in the user's default browser.
///
/// `Err` says why the URL was not opened: it failed to parse, it is not an
/// HTTP(S) URL (such a URL is never handed to an OS launcher), or the opener
/// failed to launch. Callers always also print the URL, so a headless or
/// opener-less environment can still open it by hand.
pub fn open_in_browser(url: &str) -> Result<()> {
    open::that_detached(parse_http_url(url)?.as_str())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_launch_url_accepts_only_http_and_https() {
        // Real authorization URLs (http/https) pass through.
        assert!(parse_http_url("https://idp.example.com/authorize?a=1&b=2").is_ok());
        assert!(parse_http_url("http://localhost:12345/callback").is_ok());

        // Anything else is dropped rather than handed to an OS launcher.
        assert!(parse_http_url("file:///etc/passwd").is_err());
        assert!(parse_http_url("javascript:alert(1)").is_err());
        assert!(parse_http_url("ftp://example.com/x").is_err());
        assert!(parse_http_url("not a url").is_err());
        assert!(parse_http_url("").is_err());
    }

    #[test]
    fn browser_launch_url_errors_carry_a_hint() {
        // A parse failure surfaces the parser's own message…
        let err = parse_http_url("not a url").unwrap_err();
        assert!(err.to_string().starts_with("invalid URL: "), "got: {err}");
        // …and a wrong scheme names the scheme that was rejected.
        let err = parse_http_url("file:///etc/passwd").unwrap_err();
        assert!(err.to_string().contains("\"file\""), "got: {err}");
    }

    #[test]
    fn browser_launch_url_preserves_ampersand_query_params() {
        // The `&`-separated params (what an unquoted Windows shell would
        // mangle) survive normalization intact.
        let normalized =
            parse_http_url("https://idp/authorize?response_type=code&client_id=x&state=y")
                .expect("valid https URL")
                .to_string();
        assert!(
            normalized.contains("response_type=code"),
            "got: {normalized}"
        );
        assert!(normalized.contains("&client_id=x"), "got: {normalized}");
        assert!(normalized.contains("&state=y"), "got: {normalized}");
    }
}
