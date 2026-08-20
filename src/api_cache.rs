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

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use log::{debug, warn};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::env;
use crate::jsonl::{JsonStream, json_lines};
use crate::util::Tagged;

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

/// A handler's admission verdict on the value it fetched. The cache stores
/// nothing untagged: `#[cached]` requires the handler body to return a
/// [`Tagged`] value, so admission lives in the handler — next to the
/// response, where a future policy can also read its headers.
#[derive(Clone, Copy, Debug)]
pub enum CachePolicy {
    Cacheable,
    Uncacheable,
}

/// `true` is [`CachePolicy::Cacheable`].
impl From<bool> for CachePolicy {
    fn from(cacheable: bool) -> Self {
        if cacheable {
            Self::Cacheable
        } else {
            Self::Uncacheable
        }
    }
}

/// A cache entry's identity: the operation, the handler's parameters, the
/// base URL (two tenants must never share an entry), and the generated-client
/// hash (entries never outlive the client that wrote them).
///
/// Handlers get their key from the `#[cached]` attribute (see
/// [`snouty_macros::cached`]), which serializes every handler parameter into
/// it — a parameter or field added to a handler enters the key automatically
/// instead of silently aliasing entries that differ in it.
pub struct CacheKey {
    key: String,
    /// The operation and its parameters, duplicated out of `key`'s opaque
    /// text so log messages can name the request.
    operation: &'static str,
    params: String,
}

impl CacheKey {
    fn new(base_url: &str, operation: &'static str, params: &impl Serialize) -> Self {
        // JSON escapes any newline inside a string and a URL cannot carry a
        // raw one, so '\n' cannot occur inside a segment.
        let params =
            serde_json::to_string(params).expect("handler cache-key parameters serialize to JSON");
        let identity = format!(
            "{}\n{base_url}\n{operation}\n{params}",
            env!("SNOUTY_GENERATED_API_HASH")
        );
        // cacache stores each key verbatim in its index; hashing keeps the
        // stored key fixed-length however large the parameters get.
        let key = BASE64_URL_SAFE_NO_PAD.encode(Sha256::digest(identity));
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

    /// Evict `key` so a broken entry does not stay broken forever.
    async fn evict(&self, key: &CacheKey) {
        let Some(dir) = &self.dir else { return };
        let _ = cacache::remove(dir, &key.key).await;
    }

    /// The cached value under `key`, deserialized. Returns `None` if the key
    /// doesn't exist or is unusable.
    pub async fn lookup_value<T: DeserializeOwned>(
        &self,
        key: &CacheKey,
    ) -> Option<Tagged<T, CachePolicy>> {
        let bytes = match cacache::read(self.dir.as_ref()?, &key.key).await {
            Ok(bytes) => bytes,
            Err(cacache::Error::EntryNotFound(..)) => return None,
            Err(err) => {
                warn!("API cache read failed for {key}, bypassing cache: {err}");
                self.evict(key).await;
                return None;
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(value) => {
                self.note_hit(key);
                Some(Tagged::new(value, CachePolicy::Cacheable))
            }
            Err(err) => {
                warn!("API cache entry for {key} does not parse, evicting: {err}");
                self.evict(key).await;
                None
            }
        }
    }

    /// Store a [`CachePolicy::Cacheable`]-tagged value under `key`; anything
    /// else is a no-op. Never fails: an oversized or unwritable entry is
    /// dropped and the caller keeps the value it already has.
    pub async fn store_value<T: Serialize>(&self, key: &CacheKey, tagged: &Tagged<T, CachePolicy>) {
        let Some(dir) = &self.dir else { return };
        if !matches!(tagged.tag(), CachePolicy::Cacheable) {
            return;
        }
        match serde_json::to_vec(tagged.value()) {
            Ok(bytes) if bytes.len() as u64 > self.max_file_size => {
                debug!("API cache entry over the size limit, not caching");
            }
            Ok(bytes) => {
                if let Err(err) = cacache::write(dir, &key.key, bytes).await {
                    warn!("API cache write failed for {key}, bypassing cache: {err}");
                }
            }
            Err(err) => warn!("API cache entry for {key} does not serialize, not caching: {err}"),
        }
    }

    /// Replay the stream cached under `key`, or `None` if the key doesn't
    /// exist or is unusable at open. The entry streams from disk; a read
    /// failure after open fails the replayed stream.
    pub async fn lookup_stream(&self, key: &CacheKey) -> Option<Tagged<JsonStream, CachePolicy>> {
        let reader = match cacache::Reader::open(self.dir.as_ref()?, &key.key).await {
            Ok(reader) => reader,
            Err(cacache::Error::EntryNotFound(..)) => return None,
            Err(err) => {
                warn!("API cache read failed for {key}, bypassing cache: {err}");
                self.evict(key).await;
                return None;
            }
        };
        self.note_hit(key);
        Some(Tagged::new(
            json_lines(tokio_util::io::ReaderStream::new(reader)),
            CachePolicy::Cacheable,
        ))
    }

    /// Tee a [`CachePolicy::Cacheable`]-tagged stream into the cache as the
    /// caller reads it, committing the entry only once the stream ends
    /// without an error (a stream the caller abandons partway, or that fails
    /// mid-way, is never committed). Anything else passes through untouched.
    /// Never fails: an oversized or unwritable entry is dropped and the
    /// caller keeps streaming.
    pub fn store_stream(
        &self,
        key: CacheKey,
        tagged: Tagged<JsonStream, CachePolicy>,
    ) -> Tagged<JsonStream, CachePolicy> {
        let Some(dir) = self.dir.clone() else {
            return tagged;
        };
        if !matches!(tagged.tag(), CachePolicy::Cacheable) {
            return tagged;
        }
        let tee = Some(Tee {
            dir,
            key,
            writer: None,
            remaining: self.max_file_size,
        });
        let teed = futures_util::stream::unfold(
            (tagged.unwrap(), tee),
            |(mut stream, mut tee)| async move {
                let Some(item) = stream.next().await else {
                    // The source has ended: finish the commit before ending
                    // the tee'd stream, so a fully-read response is durably
                    // cached by the time the caller sees the end.
                    if let Some(Tee {
                        key,
                        writer: Some(writer),
                        ..
                    }) = tee
                        && let Err(err) = writer.commit().await
                    {
                        warn!("API cache commit failed for {key}: {err}");
                    }
                    return None;
                };
                match &item {
                    Ok(value) => {
                        if let Some(active) = tee.take() {
                            tee = active.append(value).await;
                        }
                    }
                    // An error means the stream is incomplete; never commit it.
                    Err(_) => tee = None,
                }
                Some((item, (stream, tee)))
            },
        )
        .fuse()
        .boxed();
        Tagged::new(teed, CachePolicy::Cacheable)
    }
}

/// The active half of a stream tee: the writer opens on the first value,
/// and dropping the tee (over the size budget, an error item, or any cache
/// error) abandons the entry uncommitted.
struct Tee {
    dir: PathBuf,
    key: CacheKey,
    writer: Option<cacache::Writer>,
    /// Size budget left before the entry is abandoned as too large.
    remaining: u64,
}

impl Tee {
    /// Write one serialized value into the entry. Returns `None` when the
    /// entry is abandoned.
    async fn append(mut self, value: &Value) -> Option<Self> {
        let mut line = serde_json::to_vec(value).expect("a serde_json::Value serializes");
        line.push(b'\n');
        if line.len() as u64 > self.remaining {
            debug!("API cache entry over the size limit, not caching");
            return None;
        }
        self.remaining -= line.len() as u64;
        if self.writer.is_none() {
            match cacache::Writer::create(&self.dir, &self.key.key).await {
                Ok(writer) => self.writer = Some(writer),
                Err(err) => {
                    warn!(
                        "API cache write failed for {}, bypassing cache: {err}",
                        self.key
                    );
                    return None;
                }
            }
        }
        let writer = self.writer.as_mut().expect("the writer was just opened");
        if let Err(err) = writer.write_all(&line).await {
            warn!(
                "API cache write failed for {}, bypassing cache: {err}",
                self.key
            );
            return None;
        }
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use serde_json::Value;
    use tempfile::TempDir;

    fn test_cache(dir: &TempDir) -> ApiCache {
        ApiCache::new(
            Some(dir.path().to_path_buf()),
            DEFAULT_MAX_FILE_SIZE,
            "http://t".to_owned(),
            false,
        )
    }

    fn values_stream(values: &[Value]) -> Tagged<JsonStream, CachePolicy> {
        let items: Vec<color_eyre::Result<Value>> = values.iter().cloned().map(Ok).collect();
        Tagged::new(
            futures_util::stream::iter(items).boxed(),
            CachePolicy::Cacheable,
        )
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

        let teed = cache.store_stream(key(), values_stream(&values)).unwrap();
        assert_eq!(teed.try_collect::<Vec<_>>().await.unwrap(), values);

        let replay = cache.lookup_stream(&key()).await.expect("a cache hit");
        assert_eq!(
            replay.unwrap().try_collect::<Vec<_>>().await.unwrap(),
            values
        );
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
        let mut teed = cache
            .store_stream(
                key(),
                Tagged::new(
                    futures_util::stream::iter(items).boxed(),
                    CachePolicy::Cacheable,
                ),
            )
            .unwrap();
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

        assert!(cache.lookup_value::<Value>(&key).await.is_none());
        let tagged = Tagged::new(
            serde_json::json!({"run_id": "run-1"}),
            CachePolicy::Cacheable,
        );
        cache.store_value(&key, &tagged).await;
        assert_eq!(
            cache.lookup_value::<Value>(&key).await.map(Tagged::unwrap),
            Some(serde_json::json!({"run_id": "run-1"}))
        );
    }

    // An Uncacheable tag makes the store a no-op: the cache itself honors
    // the handler's verdict.
    #[tokio::test]
    async fn an_uncacheable_value_is_not_stored() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = CacheKey::new("http://t", "get_run", &("run-1"));

        let tagged = Tagged::new(
            serde_json::json!({"run_id": "run-1"}),
            CachePolicy::Uncacheable,
        );
        cache.store_value(&key, &tagged).await;
        assert!(cache.lookup_value::<Value>(&key).await.is_none());
    }

    // An entry the current type no longer parses is a miss, and it is
    // evicted so the fresh response can replace it.
    #[tokio::test]
    async fn an_unparsable_entry_degrades_to_a_miss() {
        let dir = TempDir::new().unwrap();
        let cache = test_cache(&dir);
        let key = CacheKey::new("http://t", "get_run", &("run-1"));

        cache
            .store_value(
                &key,
                &Tagged::new(serde_json::json!("a string"), CachePolicy::Cacheable),
            )
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
