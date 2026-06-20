use std::path::PathBuf;

use http_cache_reqwest::{
    CACacheManager, Cache, CacheMode, CacheOptions, HttpCache, HttpCacheOptions,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

use crate::env;

pub(crate) fn build_cached_client(
    client: reqwest::Client,
    explicit_root: Option<PathBuf>,
) -> Option<ClientWithMiddleware> {
    // The cache is best-effort: an unusable XDG_RUNTIME_DIR (unset, empty, or
    // non-Unicode — all collapsed by `env::var`/`.ok().flatten()`) just disables
    // caching rather than failing the command.
    let root = explicit_root
        .or_else(|| cache_dir_from_runtime_dir(env::var("XDG_RUNTIME_DIR").ok().flatten()))?;
    Some(build_cached_client_at(client, root))
}

pub(crate) fn build_cached_client_at(
    client: reqwest::Client,
    root: PathBuf,
) -> ClientWithMiddleware {
    let cache = Cache(HttpCache {
        mode: CacheMode::Default,
        manager: CACacheManager::new(root, false),
        options: HttpCacheOptions {
            cache_options: Some(CacheOptions {
                shared: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    });
    ClientBuilder::new(client).with(cache).build()
}

fn cache_dir_from_runtime_dir(runtime_dir: Option<String>) -> Option<PathBuf> {
    // `runtime_dir` is already empty-collapsed by the caller (see `env::var`), so
    // this just appends snouty's cache subpath.
    runtime_dir.map(|dir| PathBuf::from(dir).join("snouty").join("api-cache-v1"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_dir_uses_xdg_runtime_dir() {
        assert_eq!(
            cache_dir_from_runtime_dir(Some("/run/user/1000".to_string())).unwrap(),
            PathBuf::from("/run/user/1000/snouty/api-cache-v1")
        );
    }

    #[test]
    fn cache_dir_is_absent_without_xdg_runtime_dir() {
        // Empty/non-Unicode are collapsed to `None` upstream by `env::var`, so
        // this builder only ever sees `Some(non-empty)` or `None`.
        assert_eq!(cache_dir_from_runtime_dir(None), None);
    }
}
