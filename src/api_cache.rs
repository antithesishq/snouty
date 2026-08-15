use std::path::PathBuf;

use http_cache_reqwest::{
    CACacheManager, Cache, CacheMode, CacheOptions, HttpCache, HttpCacheOptions,
};

use crate::env;

/// Where the default response cache lives:
/// `$XDG_RUNTIME_DIR/snouty/api-cache-v1`. The cache is best-effort: an
/// unusable XDG_RUNTIME_DIR (unset, empty, or non-Unicode — all collapsed by
/// `env::var`/`.ok().flatten()`) just disables caching rather than failing the
/// command.
pub(crate) fn default_cache_dir() -> Option<PathBuf> {
    cache_dir_from_runtime_dir(env::var("XDG_RUNTIME_DIR").ok().flatten())
}

/// The response-cache middleware, storing under `root`.
pub(crate) fn cache_middleware(root: PathBuf) -> Cache<CACacheManager> {
    Cache(HttpCache {
        mode: CacheMode::Default,
        manager: CACacheManager::new(root, false),
        options: HttpCacheOptions {
            cache_options: Some(CacheOptions {
                shared: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    })
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
}
