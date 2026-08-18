//! A logical, domain-aware cache for Antithesis API responses.
//!
//! Unlike an HTTP cache, this cache ignores the server's cache headers:
//! each `AntithesisApi` handler decides whether its resource is immutable
//! (logs addressed by moment, properties, terminal run details) and only
//! those enter the cache. There is no per-entry expiration — the cache lives
//! in a transient per-user directory (`XDG_RUNTIME_DIR`; the user temp dir
//! on macOS) that the OS clears periodically.
//!
//! The cache is infallible by construction: every cache error — unreadable
//! entry, unwritable directory, corrupt index — degrades to a cache miss and
//! the request goes to the server. Storage is [`cacache`], which is safe for
//! concurrent readers and writers across processes.
//!
//! Keys are built from the handler's own typed parameters (see [`CacheKey`]).
//! Values are snouty's serialization of the parsed response — a JSON object,
//! or one JSON value per line for a stream — not the server's bytes, which is
//! why every key carries the generated-client hash: an entry written by one
//! build may not match the next build's types.

use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use futures_util::{FutureExt, Stream, StreamExt};
use log::{debug, warn};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::env;
use crate::jsonl::JsonStream;

/// Cache directory name under the runtime dir. The `v3` names the on-disk
/// format (re-serialized values keyed by handler parameters); bump it when
/// the format changes so stale layouts are simply abandoned.
const CACHE_DIR_NAME: &str = "api-cache-v3";

/// Bodies larger than this are not cached (see
/// [`crate::settings::Settings::api_cache_max_file_size`] to override).
pub const DEFAULT_MAX_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// Env var that overrides the cache directory outright (used as-is, no
/// `snouty/api-cache-v3` suffix). This exists for test harnesses, which need
/// an isolated cache but cannot repoint `XDG_RUNTIME_DIR`: rootless podman
/// resolves its API socket under that variable, so overriding it breaks any
/// container command snouty spawns.
const API_CACHE_DIR_VAR_NAME: &str = "SNOUTY_API_CACHE_DIR";

/// The default cache directory: `$SNOUTY_API_CACHE_DIR` if set, else
/// `$XDG_RUNTIME_DIR/snouty/api-cache-v3` — except on macOS, where
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

/// A cache entry's identity: the operation, the handler's parameters, the
/// base URL (two tenants must never share an entry), and the generated-client
/// hash (entries never outlive the client that wrote them).
///
/// Handlers destructure their parameters exhaustively when building the key,
/// so adding a parameter fails the build instead of silently aliasing entries
/// that differ in it.
pub struct CacheKey {
    key: String,
    /// The operation name, kept out of `key`'s opaque text for log messages.
    operation: &'static str,
}

impl CacheKey {
    pub fn new(base_url: &str, operation: &'static str, params: &impl Serialize) -> Self {
        // JSON escapes any newline inside a string and a URL cannot carry a
        // raw one, so '\n' cannot occur inside a segment.
        let params =
            serde_json::to_string(params).expect("handler cache-key parameters serialize to JSON");
        let key = format!(
            "{}\n{base_url}\n{operation}\n{params}",
            env!("SNOUTY_GENERATED_API_HASH")
        );
        Self { key, operation }
    }
}

/// The logical API response cache. Cheap to clone; all state is on disk.
#[derive(Clone, Debug)]
pub struct ApiCache {
    dir: PathBuf,
    max_file_size: u64,
    /// Announce each hit on stderr (the `--verbose` request log has no
    /// request to show for a hit, so this line takes its place).
    verbose: bool,
}

impl ApiCache {
    pub fn new(dir: PathBuf, max_file_size: u64, verbose: bool) -> Self {
        Self {
            dir,
            max_file_size,
            verbose,
        }
    }

    /// Read `key`'s entry, evicting on any read error so a broken entry does
    /// not stay broken forever. `cacache` verifies content integrity on read,
    /// so bytes that come back are the bytes that were committed.
    async fn read(&self, key: &CacheKey) -> Option<Vec<u8>> {
        match cacache::read(&self.dir, &key.key).await {
            Ok(bytes) => {
                debug!("API cache hit for {}", key.operation);
                if self.verbose {
                    eprintln!("* {} response served from the local cache", key.operation);
                }
                Some(bytes)
            }
            Err(cacache::Error::EntryNotFound(..)) => None,
            Err(err) => {
                warn!(
                    "API cache read failed for {}, bypassing cache: {err}",
                    key.operation
                );
                let _ = cacache::remove(&self.dir, &key.key).await;
                None
            }
        }
    }

    async fn write(&self, key: &CacheKey, bytes: Vec<u8>) {
        if bytes.len() as u64 > self.max_file_size {
            debug!("API cache entry over the size limit, not caching");
            return;
        }
        if let Err(err) = cacache::write(&self.dir, &key.key, bytes).await {
            warn!(
                "API cache write failed for {}, bypassing cache: {err}",
                key.operation
            );
        }
    }

    /// The cached value under `key`, deserialized. `None` means "send the
    /// request": a miss, any cache error, or an entry the current type no
    /// longer parses (evicted, so the fresh response can replace it).
    pub async fn lookup_value<T: DeserializeOwned>(&self, key: &CacheKey) -> Option<T> {
        let bytes = self.read(key).await?;
        match serde_json::from_slice(&bytes) {
            Ok(value) => Some(value),
            Err(err) => {
                warn!(
                    "API cache entry for {} does not parse, evicting: {err}",
                    key.operation
                );
                let _ = cacache::remove(&self.dir, &key.key).await;
                None
            }
        }
    }

    /// Store `value` under `key`. Never fails: an oversized or unwritable
    /// entry is dropped and the caller keeps the value it already has.
    pub async fn store_value<T: Serialize>(&self, key: &CacheKey, value: &T) {
        match serde_json::to_vec(value) {
            Ok(bytes) => self.write(key, bytes).await,
            Err(err) => warn!(
                "API cache entry for {} does not serialize, not caching: {err}",
                key.operation
            ),
        }
    }

    /// Replay the stream cached under `key`, or `None` on a miss or any
    /// cache error.
    pub async fn lookup_stream(&self, key: &CacheKey) -> Option<JsonStream> {
        let bytes = self.read(key).await?;
        Some(JsonStream::from_stream(futures_util::stream::once(
            async move { reqwest::Result::Ok(bytes.into()) },
        )))
    }

    /// Tee `stream` into the cache as the caller reads it, committing the
    /// entry only once the stream ends without an error (a stream the caller
    /// abandons partway, or that fails mid-way, is never committed). Never
    /// fails: an oversized or unwritable entry is dropped and the caller
    /// keeps streaming.
    pub fn store_stream(&self, key: CacheKey, stream: JsonStream) -> JsonStream {
        JsonStream::from_values(StoreStream {
            inner: stream,
            cache: self.clone(),
            entry: Some((key, Vec::new())),
            committing: None,
            done: false,
        })
    }
}

/// A value stream that forwards items unchanged while buffering their
/// serialized lines, writing the entry once the source ends cleanly.
struct StoreStream {
    inner: JsonStream,
    cache: ApiCache,
    /// The key and the entry so far, one serialized value per line; `None`
    /// once the entry is abandoned (an error item, or the size budget
    /// exceeded).
    entry: Option<(CacheKey, Vec<u8>)>,
    committing: Option<BoxFuture<'static, ()>>,
    done: bool,
}

impl Stream for StoreStream {
    type Item = color_eyre::Result<Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }

            // The source has ended: finish the write before ending the tee'd
            // stream, so a fully-read response is durably cached by the time
            // the caller sees the end.
            if let Some(commit) = &mut this.committing {
                match commit.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.committing = None;
                        this.done = true;
                        return Poll::Ready(None);
                    }
                }
            }

            match this.inner.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(value))) => {
                    if let Some((_, buf)) = &mut this.entry {
                        let line =
                            serde_json::to_vec(&value).expect("a serde_json::Value serializes");
                        if (buf.len() + line.len() + 1) as u64 > this.cache.max_file_size {
                            debug!("API cache entry over the size limit, not caching");
                            this.entry = None;
                        } else {
                            buf.extend_from_slice(&line);
                            buf.push(b'\n');
                        }
                    }
                    return Poll::Ready(Some(Ok(value)));
                }
                Poll::Ready(Some(Err(err))) => {
                    // An error means the stream is incomplete; never commit it.
                    this.entry = None;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => match this.entry.take() {
                    Some((key, buf)) => {
                        let cache = this.cache.clone();
                        this.committing = Some(async move { cache.write(&key, buf).await }.boxed());
                    }
                    None => {
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
    use futures_util::TryStreamExt;
    use tempfile::TempDir;

    fn test_cache(dir: &TempDir) -> ApiCache {
        ApiCache::new(dir.path().to_path_buf(), DEFAULT_MAX_FILE_SIZE, false)
    }

    fn values_stream(values: &[Value]) -> JsonStream {
        let items: Vec<color_eyre::Result<Value>> = values.iter().cloned().map(Ok).collect();
        JsonStream::from_values(futures_util::stream::iter(items))
    }

    // The key separates tenants, operations, and every parameter; only a
    // full match replays another call's entry.
    #[test]
    fn cache_keys_separate_tenants_operations_and_params() {
        let key =
            |base: &str, op: &'static str, params: &str| CacheKey::new(base, op, &(params)).key;
        let reference = key("https://a.example.com", "get_run", "run-1");
        assert_eq!(reference, key("https://a.example.com", "get_run", "run-1"));
        assert_ne!(reference, key("https://b.example.com", "get_run", "run-1"));
        assert_ne!(
            reference,
            key("https://a.example.com", "get_run_logs", "run-1")
        );
        assert_ne!(reference, key("https://a.example.com", "get_run", "run-2"));
    }

    #[tokio::test]
    async fn a_stream_round_trips_through_the_cache() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = || CacheKey::new("http://t", "get_run_logs", &("run-1"));
        let values = [
            serde_json::json!({"text": "one"}),
            serde_json::json!({"text": "two"}),
        ];

        let teed = cache.store_stream(key(), values_stream(&values));
        assert_eq!(teed.try_collect::<Vec<_>>().await.unwrap(), values);

        let replay = cache.lookup_stream(&key()).await.expect("a cache hit");
        assert_eq!(replay.try_collect::<Vec<_>>().await.unwrap(), values);
    }

    #[tokio::test]
    async fn a_failed_stream_is_never_committed() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = || CacheKey::new("http://t", "get_run_logs", &("run-1"));

        let items: Vec<color_eyre::Result<Value>> = vec![
            Ok(serde_json::json!({"text": "one"})),
            Err(color_eyre::eyre::eyre!("mid-stream failure")),
        ];
        let teed = cache.store_stream(
            key(),
            JsonStream::from_values(futures_util::stream::iter(items)),
        );
        assert!(teed.try_collect::<Vec<_>>().await.is_err());

        assert!(cache.lookup_stream(&key()).await.is_none());
    }

    #[tokio::test]
    async fn values_round_trip_through_the_cache() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = CacheKey::new("http://t", "get_run", &("run-1"));

        assert_eq!(cache.lookup_value::<Value>(&key).await, None);
        cache
            .store_value(&key, &serde_json::json!({"run_id": "run-1"}))
            .await;
        assert_eq!(
            cache.lookup_value::<Value>(&key).await,
            Some(serde_json::json!({"run_id": "run-1"}))
        );
    }

    // An entry the current type no longer parses is a miss, and it is
    // evicted so the fresh response can replace it.
    #[tokio::test]
    async fn an_unparseable_entry_degrades_to_a_miss() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = CacheKey::new("http://t", "get_run", &("run-1"));

        cache
            .store_value(&key, &serde_json::json!("a string"))
            .await;
        #[derive(serde::Deserialize)]
        struct Object {
            #[allow(dead_code)]
            run_id: String,
        }
        assert!(cache.lookup_value::<Object>(&key).await.is_none());
        assert!(cache.lookup_value::<Value>(&key).await.is_none());
    }
}
