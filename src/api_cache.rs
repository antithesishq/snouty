//! A logical, domain-aware cache for Antithesis API responses.
//!
//! Unlike an HTTP cache, this cache ignores the server's cache headers and
//! decides admission from what snouty knows about the domain: immutable
//! resources (logs addressed by moment, properties, terminal run details) are
//! cached forever; everything that can still change is never cached. There is
//! no per-entry expiration — the cache lives in a transient per-user directory
//! (`XDG_RUNTIME_DIR`; the user temp dir on macOS) that the OS clears
//! periodically.
//!
//! The cache is infallible by construction: every cache error — unreadable
//! entry, unwritable directory, corrupt index — degrades to a cache miss and
//! the request goes to the server. Storage is [`cacache`], which is safe for
//! concurrent readers and writers across processes. The cache key is the full
//! request URL (including query parameters); the value is the server's raw
//! response body.

use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, Stream, StreamExt};
use log::{debug, warn};
use progenitor_client::OperationInfo;
use reqwest::ResponseBuilderExt;
use tokio::io::AsyncWrite;

use crate::api::RunStatus;
use crate::env;

/// Cache directory name under the runtime dir. The `v2` names the on-disk
/// format (bare response bodies keyed by URL); bump it when the format
/// changes so stale layouts are simply abandoned.
const CACHE_DIR_NAME: &str = "api-cache-v2";

/// Bodies larger than this are not cached (see
/// [`crate::settings::Settings::api_cache_max_file_size`] to override).
pub const DEFAULT_MAX_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// A whole-body admission check (see [`Admission::IfBody`]).
type BodyCheck = fn(&[u8]) -> bool;

/// Whether and when a response may enter the cache.
#[derive(Clone, Copy, Debug)]
enum Admission {
    /// Never cached: the resource can still change.
    Never,
    /// Immutable: cache the body forever.
    Forever,
    /// Cache only when the response body passes this check (used for run
    /// detail, which is immutable only once the run reaches a terminal
    /// status).
    IfBody(BodyCheck),
}

/// The admission policy, by generated `operation_id`.
///
/// POST operations (`launch_test`, `launch_mvd`, `execute_command`, `search`)
/// are deliberately absent: the cache ignores non-GET requests for now.
/// `search_run_events` stays uncached until there is a reliable way to tell
/// that no more events will show up. `list_runs` and `get_version` are live
/// values. Build logs rely on the tee's general rule: the entry commits only
/// when the stream reaches its end without error.
fn admission(operation_id: &str) -> Admission {
    match operation_id {
        "get_run" => Admission::IfBody(run_detail_is_terminal),
        "get_run_logs" => Admission::Forever,
        "get_run_build_logs" => Admission::Forever,
        "list_run_properties" => Admission::Forever,
        _ => Admission::Never,
    }
}

/// Whether a run detail body reports a terminal status. Malformed bodies are
/// not terminal (and therefore not cached).
fn run_detail_is_terminal(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|detail| serde_json::from_value::<RunStatus>(detail.get("status")?.clone()).ok())
        .is_some_and(RunStatus::is_terminal)
}

/// Env var that overrides the cache directory outright (used as-is, no
/// `snouty/api-cache-v2` suffix). This exists for test harnesses, which need
/// an isolated cache but cannot repoint `XDG_RUNTIME_DIR`: rootless podman
/// resolves its API socket under that variable, so overriding it breaks any
/// container command snouty spawns.
const API_CACHE_DIR_VAR_NAME: &str = "SNOUTY_API_CACHE_DIR";

/// The default cache directory: `$SNOUTY_API_CACHE_DIR` if set, else
/// `$XDG_RUNTIME_DIR/snouty/api-cache-v2` — except on macOS, where
/// `XDG_RUNTIME_DIR` is normally unset and the per-user temp dir
/// (`std::env::temp_dir`) takes its place. `None` disables caching.
pub fn default_dir() -> Option<PathBuf> {
    if let Some(dir) = env::var(API_CACHE_DIR_VAR_NAME).ok().flatten() {
        return Some(PathBuf::from(dir));
    }
    #[cfg(target_os = "macos")]
    let base = Some(std::env::temp_dir());
    #[cfg(not(target_os = "macos"))]
    let base = env::var("XDG_RUNTIME_DIR")
        .ok()
        .flatten()
        .map(PathBuf::from);
    Some(base?.join("snouty").join(CACHE_DIR_NAME))
}

/// The logical API response cache. Cheap to clone; all state is on disk.
#[derive(Clone, Debug)]
pub struct ApiCache {
    dir: PathBuf,
    max_file_size: u64,
}

impl ApiCache {
    pub fn new(dir: PathBuf, max_file_size: u64) -> Self {
        Self { dir, max_file_size }
    }

    /// Serve `request` from the cache if it has the response. `None` means
    /// "send the request": a non-cacheable operation, a miss, or any cache
    /// error. The returned response streams the cached body directly from
    /// disk with a synthetic `200 OK` status (only 200 responses are ever
    /// admitted).
    pub async fn lookup(
        &self,
        request: &reqwest::Request,
        info: &OperationInfo,
    ) -> Option<reqwest::Response> {
        if matches!(admission(info.operation_id), Admission::Never) {
            return None;
        }
        let url = request.url();
        let reader = match cacache::Reader::open(&self.dir, url.as_str()).await {
            Ok(reader) => reader,
            Err(cacache::Error::EntryNotFound(..)) => return None,
            Err(err) => {
                // A broken entry would otherwise stay broken forever; evict it
                // so the next response can replace it.
                warn!("API cache read failed for {url}, bypassing cache: {err}");
                let _ = cacache::remove(&self.dir, url.as_str()).await;
                return None;
            }
        };
        debug!("API cache hit for {url}");
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(reader));
        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .url(url.clone())
            .body(body)
            .expect("a bare 200 response builds");
        Some(response.into())
    }

    /// Pass `response` through the admission policy: if it is cacheable, the
    /// returned response tees its body into the cache as the caller reads it,
    /// committing the entry only once the body is read to the end (a body the
    /// caller abandons partway is never committed). Otherwise the response is
    /// returned untouched. Never fails: any cache error abandons the entry
    /// and the caller keeps streaming from the server.
    pub async fn store(
        &self,
        info: &OperationInfo,
        response: reqwest::Response,
    ) -> reqwest::Response {
        if response.status() != reqwest::StatusCode::OK {
            return response;
        }
        let verify = match admission(info.operation_id) {
            Admission::Never => return response,
            Admission::Forever => None,
            Admission::IfBody(verify) => Some(verify),
        };

        let key = response.url().as_str();
        let writer = match cacache::Writer::create(&self.dir, key).await {
            Ok(writer) => writer,
            Err(err) => {
                warn!("API cache write failed for {key}, bypassing cache: {err}");
                return response;
            }
        };

        let status = response.status();
        let headers = response.headers().clone();
        let url = response.url().clone();
        let tee = TeeStream {
            inner: response.bytes_stream().boxed(),
            tee: Some(Tee {
                writer,
                remaining: self.max_file_size,
                buffered: verify.map(|verify| (verify, Vec::new())),
            }),
            pending: None,
            committing: None,
            done: false,
        };
        let mut builder = http::Response::builder().status(status).url(url);
        *builder
            .headers_mut()
            .expect("a fresh response builder has no error") = headers;
        builder
            .body(reqwest::Body::wrap_stream(tee))
            .expect("a response rebuilt from valid parts builds")
            .into()
    }
}

/// The active half of a [`TeeStream`]: dropped (abandoning the entry) on any
/// write error, on exceeding the size budget, or when the body verify check
/// fails.
struct Tee {
    writer: cacache::Writer,
    /// Size budget left before the entry is abandoned as too large.
    remaining: u64,
    /// For [`Admission::IfBody`]: the check plus a copy of the body so far.
    /// The copy is bounded by `remaining`'s starting budget.
    buffered: Option<(BodyCheck, Vec<u8>)>,
}

/// A body stream that forwards chunks unchanged while writing them into a
/// [`cacache::Writer`], committing the cache entry once the source body ends.
struct TeeStream {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    tee: Option<Tee>,
    /// A chunk not yet fully written to the cache: `(chunk, bytes written)`.
    pending: Option<(Bytes, usize)>,
    committing: Option<BoxFuture<'static, cacache::Result<cacache::Integrity>>>,
    done: bool,
}

impl Stream for TeeStream {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }

            // The source body has ended: finish the commit before ending the
            // tee'd body, so a fully-read response is durably cached by the
            // time the caller sees the end.
            if let Some(commit) = &mut this.committing {
                match commit.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        if let Err(err) = result {
                            warn!("API cache commit failed: {err}");
                        }
                        this.committing = None;
                        this.done = true;
                        return Poll::Ready(None);
                    }
                }
            }

            // Finish writing the current chunk into the cache before handing
            // it downstream. A write error abandons the entry (the chunk
            // still goes downstream — the cache never breaks the response).
            if let Some((chunk, written)) = &mut this.pending {
                if let Some(tee) = &mut this.tee {
                    while *written < chunk.len() {
                        match Pin::new(&mut tee.writer).poll_write(cx, &chunk[*written..]) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Ok(n)) => *written += n,
                            Poll::Ready(Err(err)) => {
                                warn!("API cache write failed, bypassing cache: {err}");
                                this.tee = None;
                                break;
                            }
                        }
                    }
                }
                let (chunk, _) = this.pending.take().expect("pending was just matched");
                return Poll::Ready(Some(Ok(chunk)));
            }

            match this.inner.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    match &mut this.tee {
                        Some(tee) if (chunk.len() as u64) <= tee.remaining => {
                            tee.remaining -= chunk.len() as u64;
                            if let Some((_, copy)) = &mut tee.buffered {
                                copy.extend_from_slice(&chunk);
                            }
                            this.pending = Some((chunk, 0));
                        }
                        Some(_) => {
                            // Over the size limit: abandon the entry, keep
                            // streaming to the caller.
                            debug!("API cache entry over the size limit, not caching");
                            this.tee = None;
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        None => return Poll::Ready(Some(Ok(chunk))),
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    // A transport error means the body is incomplete; never
                    // commit it.
                    this.tee = None;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => match this.tee.take() {
                    Some(tee)
                        if tee
                            .buffered
                            .as_ref()
                            .is_none_or(|(verify, body)| verify(body)) =>
                    {
                        this.committing = Some(tee.writer.commit().boxed());
                    }
                    _ => {
                        this.done = true;
                        return Poll::Ready(None);
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_matches_the_domain_policy() {
        assert!(matches!(admission("get_run"), Admission::IfBody(_)));
        assert!(matches!(admission("get_run_logs"), Admission::Forever));
        assert!(matches!(
            admission("get_run_build_logs"),
            Admission::Forever
        ));
        assert!(matches!(
            admission("list_run_properties"),
            Admission::Forever
        ));
        for uncacheable in [
            "list_runs",
            "search_run_events",
            "get_version",
            "launch_test",
            "launch_mvd",
            "execute_command",
            "search",
        ] {
            assert!(
                matches!(admission(uncacheable), Admission::Never),
                "{uncacheable} must not be cached"
            );
        }
    }

    #[test]
    fn run_detail_terminality_gates_on_the_status_field() {
        let body = |status: &str| format!(r#"{{"run_id":"r","status":"{status}"}}"#);
        for terminal in ["completed", "cancelled", "incomplete"] {
            assert!(run_detail_is_terminal(body(terminal).as_bytes()));
        }
        for live in ["starting", "in_progress", "unknown"] {
            assert!(!run_detail_is_terminal(body(live).as_bytes()));
        }
        // Malformed bodies are not cached.
        assert!(!run_detail_is_terminal(b"not json"));
        assert!(!run_detail_is_terminal(br#"{"run_id":"r"}"#));
        assert!(!run_detail_is_terminal(br#"{"status":"bogus"}"#));
    }
}
