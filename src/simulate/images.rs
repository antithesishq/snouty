use std::path::{Path, PathBuf};
use std::time::Duration;

use color_eyre::eyre::{Context, Result, bail};

use crate::container::{Architecture, ContainerRuntime, ImageArchive, SaveExit};
use crate::digest::Sha256Digest;
use crate::runs::ReadyDisplay;
use crate::simulate::SimulatePlatform;

pub(super) const IMAGE_TIMEOUT: Duration = Duration::from_mins(30);

pub(super) struct Image {
    pub id: Sha256Digest,
}

pub(super) struct Images {
    runtime: Box<dyn ContainerRuntime>,
}

impl Images {
    pub fn from_runtime(runtime: &dyn ContainerRuntime) -> Self {
        Self {
            runtime: runtime.clone_box(),
        }
    }

    /// Inspect a local image and check that it runs on `platform`. This
    /// blocks briefly, as other local image checks do.
    pub fn inspect(&self, reference: &str, platform: SimulatePlatform) -> Result<Image> {
        let platform = Architecture::from(platform);
        let details = self
            .runtime
            .image_inspect(reference, Some(&platform))
            .wrap_err_with(|| format!("cannot inspect image {reference}"))?;
        if details.architecture != platform {
            bail!(
                "image {reference} uses {}; simulate requires {platform}",
                details.architecture
            );
        }
        Ok(Image { id: details.id })
    }

    /// Extract the ISO of the environment image `reference`. A local image
    /// permits testing a release before it is published; a missing one is
    /// pulled.
    pub async fn environment_iso(
        &self,
        reference: &str,
        run_dir: &Path,
        platform: SimulatePlatform,
    ) -> Result<PathBuf> {
        if !self
            .runtime
            .image_exists(reference, Some(&Architecture::from(platform)))?
        {
            self.pull(reference, platform).await?;
        }
        let image = self.inspect(reference, platform)?;
        let cache = crate::settings::cache_dir()
            .unwrap_or_else(|| run_dir.to_owned())
            .join("simulate")
            .join(image.id.to_string());
        self.extract_iso(reference, &cache, platform).await
    }

    /// Pull `reference` with the engine's own registry credentials. For the
    /// Antithesis registry, those come from `snouty docker-credential`.
    async fn pull(&self, reference: &str, platform: SimulatePlatform) -> Result<()> {
        let mut display = ReadyDisplay::new("pulling the simulation environment image");
        let platform = Architecture::from(platform);
        let pull = crate::container::pull_image(
            self.runtime.as_ref(),
            reference,
            Some(&platform),
            IMAGE_TIMEOUT,
        );
        tokio::pin!(pull);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let pulled = loop {
            tokio::select! {
                pulled = &mut pull => break pulled,
                _ = tick.tick() => display.show(),
            }
        };
        display.clear();
        pulled.wrap_err_with(|| format!("cannot pull simulation environment image {reference}"))
    }

    async fn extract_iso(
        &self,
        reference: &str,
        cache: &Path,
        platform: SimulatePlatform,
    ) -> Result<PathBuf> {
        std::fs::create_dir_all(cache)?;
        let iso = cache.join("guest.iso");
        if std::fs::symlink_metadata(&iso)
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            return Ok(iso);
        }
        let staging = tempfile::tempdir_in(cache)?;
        let staged_iso = staging.path().join("guest.iso");
        crate::container::copy_from_image(
            self.runtime.as_ref(),
            reference,
            Some(&Architecture::from(platform)),
            "/guest.iso",
            &staged_iso,
            IMAGE_TIMEOUT,
        )
        .await?;
        let metadata = std::fs::symlink_metadata(&staged_iso)?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!("simulation environment image: /guest.iso must be a nonempty regular file");
        }
        std::fs::rename(&staged_iso, &iso)?;
        Ok(iso)
    }

    /// Start `save` of the workload images `references` as one archive.
    pub fn save(
        &self,
        references: &[String],
        platform: SimulatePlatform,
    ) -> Result<(ImageArchive, SaveExit)> {
        if references.is_empty() {
            bail!("no workload images to export");
        }
        let references: Vec<&str> = references.iter().map(String::as_str).collect();
        crate::container::save_images(
            self.runtime.as_ref(),
            &references,
            Some(&Architecture::from(platform)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn engine(directory: &Path) -> Images {
        let path = directory.join("engine");
        std::fs::write(
            &path,
            r#"#!/bin/sh
root=${0%/*}
case "$1" in
create)
  touch "$root/started"
  if [ -f "$root/slow" ]; then
    while [ ! -f "$root/release" ]; do sleep 0.01; done
  fi
  touch "$root/container"
  ;;
cp)
  if [ -f "$root/fail-copy" ]; then exit 3; fi
  printf 'environment iso' > "$3"
  ;;
rm)
  rm -f "$root/container"
  touch "$root/removed"
  ;;
save)
  printf '%s\n' "$@" > "$root/save-args"
  ;;
esac
"#,
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Images {
            runtime: Box::new(crate::container::PodmanRuntime::new(path.to_string_lossy())),
        }
    }

    async fn wait_for(path: &Path) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn extraction_is_cached_only_after_copy_and_cleanup_succeed() {
        let directory = tempfile::tempdir().unwrap();
        let images = engine(directory.path());
        let cache = directory.path().join("cache");
        std::fs::write(directory.path().join("fail-copy"), "").unwrap();
        assert!(
            images
                .extract_iso("environment:test", &cache, SimulatePlatform::Amd64)
                .await
                .is_err()
        );
        assert!(!cache.join("guest.iso").exists());
        assert!(!directory.path().join("container").exists());
        assert!(directory.path().join("removed").exists());
        std::fs::remove_file(directory.path().join("fail-copy")).unwrap();
        let iso = images
            .extract_iso("environment:test", &cache, SimulatePlatform::Amd64)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&iso).unwrap(), b"environment iso");
        std::fs::remove_file(directory.path().join("started")).unwrap();
        assert_eq!(
            images
                .extract_iso("environment:test", &cache, SimulatePlatform::Amd64)
                .await
                .unwrap(),
            iso
        );
        assert!(!directory.path().join("started").exists());
    }

    #[tokio::test]
    async fn cancelling_create_removes_container_after_creation_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let images = engine(directory.path());
        std::fs::write(directory.path().join("slow"), "").unwrap();
        let cache = directory.path().join("cache");
        let task = tokio::spawn(async move {
            images
                .extract_iso("environment:test", &cache, SimulatePlatform::Amd64)
                .await
        });
        wait_for(&directory.path().join("started")).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        std::fs::write(directory.path().join("release"), "").unwrap();
        wait_for(&directory.path().join("removed")).await;
        assert!(!directory.path().join("container").exists());
    }

    #[tokio::test]
    async fn podman_exports_unique_image_references_as_a_multi_image_archive() {
        let directory = tempfile::tempdir().unwrap();
        let images = engine(directory.path());
        let (_, exit) = images
            .save(
                &[
                    "example.test/app:one".into(),
                    "example.test/app:one".into(),
                    "example.test/app:two".into(),
                ],
                SimulatePlatform::Amd64,
            )
            .unwrap();
        exit.wait().await.unwrap();
        let args = std::fs::read_to_string(directory.path().join("save-args")).unwrap();
        assert!(args.lines().any(|arg| arg == "--multi-image-archive"));
        assert!(!args.lines().any(|arg| arg == "--output"));
        assert_eq!(
            args.lines()
                .filter(|arg| *arg == "example.test/app:one")
                .count(),
            1
        );
        assert!(args.lines().any(|arg| arg == "example.test/app:two"));
        assert!(images.save(&[], SimulatePlatform::Amd64).is_err());
    }
}
