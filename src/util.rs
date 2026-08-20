//! Small cross-cutting utilities with no better home.

use std::path::Path;

use color_eyre::eyre::{Context, Result};

/// A value carrying a tag.
#[derive(Debug)]
pub struct Tagged<V, T> {
    value: V,
    tag: T,
}

impl<V, T> Tagged<V, T> {
    pub fn new(value: V, tag: T) -> Self {
        Self { value, tag }
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn tag(&self) -> &T {
        &self.tag
    }

    /// The inner value, dropping the tag.
    pub fn unwrap(self) -> V {
        self.value
    }
}

/// Wrap any value in a [`Tagged`].
pub trait Tag: Sized {
    fn with_tag<T>(self, tag: T) -> Tagged<Self, T> {
        Tagged::new(self, tag)
    }
}

impl<V> Tag for V {}

/// The first error of type `T` in `err`'s source chain, if any.
pub fn source_error<'a, T: std::error::Error + 'static>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    let mut source = err.source();
    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<T>() {
            return Some(err);
        }
        source = err.source();
    }
    None
}

/// Recursively copy the contents of `src` into `dst` (which must already exist).
///
/// Symlinks are recreated as-is (not dereferenced), so the copied tree is
/// byte-for-byte what a `docker build` context would tar from `src`. The stdlib
/// has no recursive copy, and the common crates (`fs_extra`, `walkdir`-based
/// copies) dereference symlinks, which would change that content — hence this
/// small local implementation.
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    for entry in
        std::fs::read_dir(src).wrap_err_with(|| format!("failed to read {}", src.display()))?
    {
        let entry = entry.wrap_err_with(|| format!("failed to read entry in {}", src.display()))?;
        let file_type = entry.file_type().wrap_err("failed to read file type")?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            std::fs::create_dir_all(&to)
                .wrap_err_with(|| format!("failed to create {}", to.display()))?;
            copy_dir_recursive(&from, &to)?;
        } else if file_type.is_symlink() {
            let target = std::fs::read_link(&from)
                .wrap_err_with(|| format!("failed to read symlink {}", from.display()))?;
            std::os::unix::fs::symlink(&target, &to)
                .wrap_err_with(|| format!("failed to create symlink {}", to.display()))?;
        } else {
            std::fs::copy(&from, &to).wrap_err_with(|| {
                format!("failed to copy {} to {}", from.display(), to.display())
            })?;
        }
    }
    Ok(())
}
