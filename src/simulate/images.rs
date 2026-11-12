use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;

use color_eyre::eyre::{Context, Result, bail};

use crate::container::{Architecture, ContainerRuntime, Engine};
use crate::process::{output_async, output_with_timeout};
use crate::runs::ReadyDisplay;
use crate::simulate::SimulatePlatform;

const IMAGE_TIMEOUT: Duration = Duration::from_mins(30);
const CREATE_TIMEOUT: Duration = Duration::from_secs(60);
const SHA256_PREFIX: &str = "sha256:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ImageId(String);

impl FromStr for ImageId {
    type Err = color_eyre::eyre::Report;

    fn from_str(value: &str) -> Result<Self> {
        let digest = value.strip_prefix(SHA256_PREFIX).unwrap_or(value);
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("image has an invalid SHA-256 content ID");
        }
        Ok(Self(digest.to_ascii_lowercase()))
    }
}

impl fmt::Display for ImageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SHA256_PREFIX}{}", self.0)
    }
}

pub(super) struct Image {
    pub id: ImageId,
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
        Ok(Image {
            id: details.id.parse()?,
        })
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
        let mut pull = self.runtime.tokio_command(&[
            "pull",
            "--platform",
            &format!("linux/{platform}"),
            reference,
        ]);
        pull.stdin(Stdio::null());
        let mut display = ReadyDisplay::new("pulling the simulation environment image");
        let output = output_async(pull, IMAGE_TIMEOUT);
        tokio::pin!(output);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let output = loop {
            tokio::select! {
                output = &mut output => break output?,
                _ = tick.tick() => display.show(),
            }
        };
        display.clear();
        if !output.status.success() {
            bail!(
                "cannot pull simulation environment image {reference}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
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
        let name = format!(
            "snouty-iso-{}",
            staging
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace('.', "")
        );
        let runtime = self.runtime.clone_box();
        let reference = reference.to_owned();
        // The worker retains cleanup ownership when its caller is cancelled.
        let container = tokio::task::spawn_blocking(move || {
            let container = ExtractionContainer {
                runtime,
                name: Some(name),
            };
            let create = container.runtime.command(&[
                "create",
                "--platform",
                &format!("linux/{platform}"),
                "--name",
                container.name.as_deref().expect("container name assigned"),
                &reference,
                "/bin/true",
            ]);
            let output = output_with_timeout(create, CREATE_TIMEOUT)?;
            if !output.status.success() {
                bail!(
                    "cannot create ISO extraction container: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Ok::<_, color_eyre::eyre::Report>(container)
        })
        .await??;
        let staged_iso = staging.path().join("guest.iso");
        let mut copy = self.runtime.tokio_command(&[
            "cp",
            &format!(
                "{}:/guest.iso",
                container.name.as_deref().expect("container name assigned")
            ),
        ]);
        copy.arg(&staged_iso).stdin(Stdio::null());
        let output = output_async(copy, IMAGE_TIMEOUT).await?;
        if !output.status.success() {
            bail!(
                "cannot extract /guest.iso: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let metadata = std::fs::symlink_metadata(&staged_iso)?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!("simulation environment image: /guest.iso must be a nonempty regular file");
        }
        container.remove().await?;
        std::fs::rename(&staged_iso, &iso)?;
        Ok(iso)
    }

    pub async fn save(
        &self,
        references: &[String],
        archive: &Path,
        platform: SimulatePlatform,
    ) -> Result<()> {
        let references: BTreeSet<_> = references.iter().cloned().collect();
        if references.is_empty() {
            bail!("no workload images to export");
        }
        let mut command = self.runtime.tokio_command(&["save"]);
        command.args(
            self.runtime
                .image_platform_args(Some(&Architecture::from(platform))),
        );
        if self.runtime.engine_kind() == Engine::Podman {
            // Without this flag Podman interprets extra arguments as tags of one image.
            command.args(["--format", "docker-archive", "--multi-image-archive"]);
        }
        command
            .arg("--output")
            .arg(archive)
            .args(references)
            .stdin(Stdio::null());
        let output = output_async(command, IMAGE_TIMEOUT).await?;
        if !output.status.success() {
            bail!(
                "cannot export workload images: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }
}

struct ExtractionContainer {
    runtime: Box<dyn ContainerRuntime>,
    name: Option<String>,
}

impl ExtractionContainer {
    async fn remove(mut self) -> Result<()> {
        let mut command = self.runtime.tokio_command(&[
            "rm",
            "--force",
            self.name.as_deref().expect("container name assigned"),
        ]);
        command.stdin(Stdio::null());
        let output = output_async(command, Duration::from_secs(10)).await?;
        if !output.status.success() {
            bail!(
                "cannot remove ISO extraction container: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        self.name = None;
        Ok(())
    }
}

impl Drop for ExtractionContainer {
    fn drop(&mut self) {
        let Some(name) = self.name.take() else { return };
        let command = self.runtime.command(&["rm", "--force", &name]);
        match output_with_timeout(command, Duration::from_secs(10)) {
            Ok(output) if !output.status.success() => log::warn!(
                "cannot remove ISO extraction container {name}: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => log::warn!("cannot remove ISO extraction container {name}: {error}"),
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;
    use std::os::unix::fs::PermissionsExt;

    const ID: &str = "afddb59f8e828f0306d47b7084673f16112c6af52cce6b6e1d3f8c737e1a984b";

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
        images
            .save(
                &[
                    "example.test/app:one".into(),
                    "example.test/app:one".into(),
                    "example.test/app:two".into(),
                ],
                &directory.path().join("images.tar"),
                SimulatePlatform::Amd64,
            )
            .await
            .unwrap();
        let args = std::fs::read_to_string(directory.path().join("save-args")).unwrap();
        assert!(args.lines().any(|arg| arg == "--multi-image-archive"));
        assert_eq!(
            args.lines()
                .filter(|arg| *arg == "example.test/app:one")
                .count(),
            1
        );
        assert!(args.lines().any(|arg| arg == "example.test/app:two"));
        assert!(
            images
                .save(
                    &[],
                    &directory.path().join("bad.tar"),
                    SimulatePlatform::Amd64
                )
                .await
                .is_err()
        );
    }

    #[hegel::test]
    fn digest_normalization_preserves_identity(tc: hegel::TestCase) {
        use std::fmt::Write as _;
        let bytes = tc.draw(generators::binary().min_size(32).max_size(32));
        let digest = bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").unwrap();
            output
        });
        let id: ImageId = digest.parse().unwrap();
        assert_eq!(
            format!("sha256:{}", digest.to_ascii_uppercase())
                .parse::<ImageId>()
                .unwrap(),
            id
        );
        assert_eq!(id.to_string(), format!("sha256:{digest}"));
        assert_eq!(id.to_string().parse::<ImageId>().unwrap(), id);
        let path = Path::new("cache").join(id.to_string());
        assert_eq!(path.parent(), Some(Path::new("cache")));
    }

    #[hegel::test]
    fn arbitrary_ids_cannot_escape_the_cache(tc: hegel::TestCase) {
        let input = tc.draw(generators::text());
        assert!(format!("../{input}").parse::<ImageId>().is_err());
        assert!(format!("{ID}/{input}").parse::<ImageId>().is_err());
    }
}
