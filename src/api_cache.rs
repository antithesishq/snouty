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

use futures_util::StreamExt;
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
/// Handlers key on their parameter values themselves (serde-serialized), so
/// a field added to a parameter type enters the key automatically instead of
/// silently aliasing entries that differ in it.
pub struct CacheKey {
    key: String,
    /// The operation name and its parameters, kept out of `key`'s opaque
    /// text so log messages can name the request a hit or failure is about.
    operation: &'static str,
    params: String,
}

impl CacheKey {
    fn new(base_url: &str, operation: &'static str, params: &impl Serialize) -> Self {
        // JSON escapes any newline inside a string and a URL cannot carry a
        // raw one, so '\n' cannot occur inside a segment.
        let params =
            serde_json::to_string(params).expect("handler cache-key parameters serialize to JSON");
        let key = format!(
            "{}\n{base_url}\n{operation}\n{params}",
            env!("SNOUTY_GENERATED_API_HASH")
        );
        Self {
            key,
            operation,
            params,
        }
    }
}

impl std::fmt::Display for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.operation, self.params)
    }
}

/// The logical API response cache. Cheap to clone; all state is on disk.
#[derive(Clone, Debug)]
pub struct ApiCache {
    /// `None` disables the cache: every lookup misses and every store is a
    /// no-op.
    dir: Option<PathBuf>,
    max_file_size: u64,
    /// The tenant every key is bound to.
    base_url: String,
    /// Announce each hit on stderr (the `--verbose` request log has no
    /// request to show for a hit, so that line takes its place).
    verbose: bool,
}

impl ApiCache {
    pub fn new(dir: Option<PathBuf>, max_file_size: u64, base_url: String, verbose: bool) -> Self {
        Self {
            dir,
            max_file_size,
            base_url,
            verbose,
        }
    }

    /// The cache key for `operation` with `params`, bound to this cache's
    /// tenant.
    pub fn key(&self, operation: &'static str, params: &impl Serialize) -> CacheKey {
        CacheKey::new(&self.base_url, operation, params)
    }

    fn note_hit(&self, key: &CacheKey) {
        debug!("API cache hit for {key}");
        if self.verbose {
            eprintln!("* response served from the local cache: {key}");
        }
    }

    /// Read `key`'s entry, evicting on any read error. `cacache` verifies
    /// content integrity on read, so bytes that come back are the bytes that
    /// were committed.
    async fn read(&self, key: &CacheKey) -> Option<Vec<u8>> {
        match cacache::read(self.dir.as_ref()?, &key.key).await {
            Ok(bytes) => Some(bytes),
            Err(cacache::Error::EntryNotFound(..)) => None,
            Err(err) => {
                warn!("API cache read failed for {key}, bypassing cache: {err}");
                self.evict(key).await;
                None
            }
        }
    }

    /// Evict `key` so a broken entry does not stay broken forever.
    async fn evict(&self, key: &CacheKey) {
        let Some(dir) = &self.dir else { return };
        let _ = cacache::remove(dir, &key.key).await;
    }

    async fn write(&self, key: &CacheKey, bytes: Vec<u8>) {
        let Some(dir) = &self.dir else { return };
        if bytes.len() as u64 > self.max_file_size {
            debug!("API cache entry over the size limit, not caching");
            return;
        }
        if let Err(err) = cacache::write(dir, &key.key, bytes).await {
            warn!("API cache write failed for {key}, bypassing cache: {err}");
        }
    }

    /// Read and parse `key`'s entry, evicting an entry that no longer parses
    /// so the fresh response can replace it.
    async fn read_parsed<T>(
        &self,
        key: &CacheKey,
        parse: impl FnOnce(&[u8]) -> serde_json::Result<T>,
    ) -> Option<T> {
        let bytes = self.read(key).await?;
        match parse(&bytes) {
            Ok(value) => Some(value),
            Err(err) => {
                warn!("API cache entry for {key} does not parse, evicting: {err}");
                self.evict(key).await;
                None
            }
        }
    }

    /// The cached value under `key`, deserialized. `None` means "send the
    /// request": a miss, any cache error, or an entry the current type no
    /// longer parses.
    pub async fn lookup_value<T: DeserializeOwned>(&self, key: &CacheKey) -> Option<T> {
        let value = self
            .read_parsed(key, |bytes| serde_json::from_slice(bytes))
            .await?;
        self.note_hit(key);
        Some(value)
    }

    /// Store `value` under `key`. Never fails: an oversized or unwritable
    /// entry is dropped and the caller keeps the value it already has.
    pub async fn store_value<T: Serialize>(&self, key: &CacheKey, value: &T) {
        if self.dir.is_none() {
            return;
        }
        match serde_json::to_vec(value) {
            Ok(bytes) => self.write(key, bytes).await,
            Err(err) => warn!("API cache entry for {key} does not serialize, not caching: {err}"),
        }
    }

    /// Replay the stream cached under `key`, or `None` on a miss or any
    /// cache error. The entry is parsed up front — it is already fully in
    /// memory, and an unparsable entry becomes a miss instead of failing
    /// the stream partway.
    pub async fn lookup_stream(&self, key: &CacheKey) -> Option<JsonStream> {
        let values: Vec<Value> = self
            .read_parsed(key, |bytes| {
                serde_json::Deserializer::from_slice(bytes)
                    .into_iter()
                    .collect()
            })
            .await?;
        self.note_hit(key);
        Some(JsonStream::from_values(futures_util::stream::iter(
            values.into_iter().map(Ok),
        )))
    }

    /// Tee `stream` into the cache as the caller reads it, committing the
    /// entry only once the stream ends without an error (a stream the caller
    /// abandons partway, or that fails mid-way, is never committed). Never
    /// fails: an oversized or unwritable entry is dropped and the caller
    /// keeps streaming.
    pub fn store_stream(&self, key: CacheKey, stream: JsonStream) -> JsonStream {
        if self.dir.is_none() {
            return stream;
        }
        // The state carries the entry so far, one serialized value per line;
        // the entry is dropped on an error item or over the size budget,
        // which bounds the buffer while the caller streams.
        let state = (stream, self.clone(), Some((key, Vec::new())));
        JsonStream::from_values(
            futures_util::stream::unfold(state, |(mut stream, cache, mut entry)| async move {
                let Some(item) = stream.next().await else {
                    // The source has ended: finish the write before ending
                    // the tee'd stream, so a fully-read response is durably
                    // cached by the time the caller sees the end.
                    if let Some((key, buf)) = entry {
                        cache.write(&key, buf).await;
                    }
                    return None;
                };
                match &item {
                    Ok(value) => {
                        if let Some((_, buf)) = &mut entry {
                            serde_json::to_writer(&mut *buf, value)
                                .expect("a serde_json::Value serializes");
                            buf.push(b'\n');
                            if buf.len() as u64 > cache.max_file_size {
                                debug!("API cache entry over the size limit, not caching");
                                entry = None;
                            }
                        }
                    }
                    // An error means the stream is incomplete; never commit it.
                    Err(_) => entry = None,
                }
                Some((item, (stream, cache, entry)))
            })
            .fuse(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use tempfile::TempDir;

    fn test_cache(dir: &TempDir) -> ApiCache {
        ApiCache::new(
            Some(dir.path().to_path_buf()),
            DEFAULT_MAX_FILE_SIZE,
            "http://t".to_owned(),
            false,
        )
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
            Ok(serde_json::json!({"text": "after the failure"})),
        ];
        let mut teed = cache.store_stream(
            key(),
            JsonStream::from_values(futures_util::stream::iter(items)),
        );
        // Drain past the error to the stream's end: even a caller that keeps
        // reading must not commit the broken body.
        let mut saw_error = false;
        while let Some(item) = teed.next().await {
            saw_error |= item.is_err();
        }
        assert!(saw_error);

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
    async fn an_unparsable_entry_degrades_to_a_miss() {
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
