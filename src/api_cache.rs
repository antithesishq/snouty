use std::env;
use std::path::PathBuf;

use http_cache_reqwest::{
    CACacheManager, Cache, CacheMode, CacheOptions, HttpCache, HttpCacheOptions,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

pub(crate) fn build_cached_client(client: reqwest::Client) -> Option<ClientWithMiddleware> {
    let root = cache_dir_from_runtime_dir(env::var_os("XDG_RUNTIME_DIR"))?;
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

fn cache_dir_from_runtime_dir(runtime_dir: Option<std::ffi::OsString>) -> Option<PathBuf> {
    runtime_dir.map(|dir| PathBuf::from(dir).join("snouty").join("api-cache-v1"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_dir_uses_xdg_runtime_dir() {
        assert_eq!(
            cache_dir_from_runtime_dir(Some("/run/user/1000".into())).unwrap(),
            PathBuf::from("/run/user/1000/snouty/api-cache-v1")
        );
    }

    #[test]
    fn cache_dir_is_absent_without_xdg_runtime_dir() {
        assert_eq!(cache_dir_from_runtime_dir(None), None);
    }
}
