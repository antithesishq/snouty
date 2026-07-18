//! Small cross-cutting utilities with no better home.

use std::path::Path;

use color_eyre::eyre::{Context, Result};

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
