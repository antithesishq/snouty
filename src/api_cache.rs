use std::path::{Path, PathBuf};

use http_cache_reqwest::{
    CACacheManager, Cache, CacheMode, CacheOptions, HttpCache, HttpCacheOptions,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

pub(crate) fn build_cached_client(
    client: reqwest::Client,
    cache_dir: Option<&Path>,
) -> Option<ClientWithMiddleware> {
    cache_dir_from_runtime_dir(cache_dir).map(|root| build_cached_client_at(client, root))
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

fn cache_dir_from_runtime_dir(runtime_dir: Option<&Path>) -> Option<PathBuf> {
    runtime_dir.map(|dir| dir.join("api-cache-v1"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "linux")]
    fn cache_dir_uses_xdg_runtime_dir() {
        assert_eq!(
            cache_dir_from_runtime_dir(Some(&PathBuf::from("/run/user/1000/snouty"))).unwrap(),
            PathBuf::from("/run/user/1000/snouty/api-cache-v1")
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn cache_dir_is_absent_without_xdg_runtime_dir() {
        assert_eq!(cache_dir_from_runtime_dir(None), None);
    }
}
