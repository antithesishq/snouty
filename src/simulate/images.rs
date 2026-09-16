use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;

use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::Deserialize;
use tokio::process::Command;

use crate::container::{AMD64_PLATFORM, Architecture, ContainerRuntime};
use crate::process::{output_async, output_with_timeout};

const IMAGE_TIMEOUT: Duration = Duration::from_secs(1800);
const CREATE_TIMEOUT: Duration = Duration::from_secs(60);
const SHA256_PREFIX: &str = "sha256:";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String")]
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

impl TryFrom<String> for ImageId {
    type Error = color_eyre::eyre::Report;

    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

impl fmt::Display for ImageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub(super) struct Image {
    pub id: ImageId,
}

#[derive(Clone, Copy)]
enum Engine {
    Docker,
    Podman,
}

pub(super) struct Images {
    engine: String,
    kind: Engine,
}

impl Images {
    pub fn from_runtime(runtime: &dyn ContainerRuntime) -> Self {
        Self {
            engine: runtime.name().to_owned(),
            kind: match runtime.engine_kind() {
                "docker" => Engine::Docker,
                "podman" => Engine::Podman,
                _ => unreachable!("container runtime has an unsupported engine kind"),
            },
        }
    }

    pub async fn inspect(&self, reference: &str) -> Result<Image> {
        #[derive(Deserialize)]
        struct Inspect {
            // Podman v5.8.2: local antithesis-guest:v61.2 inspect uses Id and Architecture.
            #[serde(rename = "Id")]
            id: ImageId,
            #[serde(rename = "Architecture")]
            architecture: String,
        }
        let mut cmd = Command::new(&self.engine);
        cmd.args(["image", "inspect", reference])
            .stdin(Stdio::null());
        let output = output_async(cmd, Duration::from_secs(60)).await?;
        if !output.status.success() {
            bail!(
                "cannot inspect image {reference}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let images: Vec<Inspect> =
            serde_json::from_slice(&output.stdout).wrap_err("invalid image inspect response")?;
        let image = images
            .into_iter()
            .next()
            .ok_or_else(|| eyre!("image inspect returned no image"))?;
        let architecture = Architecture::from(image.architecture.as_str());
        if architecture != Architecture::Amd64 {
            bail!("image {reference} uses {architecture}; simulate requires amd64");
        }
        Ok(Image { id: image.id })
    }

    pub async fn guest_iso(&self, reference: &str, run_dir: &Path) -> Result<PathBuf> {
        // Local guest images permit testing a guest release before it is published.
        let mut exists = Command::new(&self.engine);
        exists
            .args(["image", "inspect", reference])
            .stdin(Stdio::null());
        if !output_async(exists, Duration::from_secs(60))
            .await?
            .status
            .success()
        {
            let mut pull = Command::new(&self.engine);
            pull.args(["pull", "--platform", AMD64_PLATFORM, reference])
                .stdin(Stdio::null());
            let output = output_async(pull, IMAGE_TIMEOUT).await?;
            if !output.status.success() {
                bail!(
                    "cannot pull guest image {reference}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        let image = self.inspect(reference).await?;
        let cache = crate::settings::cache_dir()
            .unwrap_or_else(|| run_dir.to_owned())
            .join("simulate")
            .join(image.id.to_string());
        self.extract_iso(image.id, &cache).await
    }

    async fn extract_iso(&self, id: ImageId, cache: &Path) -> Result<PathBuf> {
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
        let engine = self.engine.clone();
        // Keep ownership in the worker until create finishes. Cancelling its caller
        // must not remove the named container before the daemon has created it.
        let container = tokio::task::spawn_blocking(move || {
            let container = ExtractionContainer {
                engine,
                name: Some(name),
            };
            let mut create = std::process::Command::new(&container.engine);
            create.args([
                "create",
                "--name",
                container.name.as_deref().expect("container name assigned"),
                &id.to_string(),
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
        let mut copy = Command::new(&self.engine);
        copy.arg("cp")
            .arg(format!(
                "{}:/guest.iso",
                container.name.as_deref().expect("container name assigned")
            ))
            .arg(&staged_iso)
            .stdin(Stdio::null());
        let output = output_async(copy, IMAGE_TIMEOUT).await?;
        if !output.status.success() {
            bail!(
                "cannot extract /guest.iso: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let metadata = std::fs::symlink_metadata(&staged_iso)?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!("guest image /guest.iso must be a nonempty regular file");
        }
        container.remove().await?;
        std::fs::rename(&staged_iso, &iso)?;
        Ok(iso)
    }

    pub async fn save(&self, references: &[String], archive: &Path) -> Result<()> {
        let ids = references
            .iter()
            .map(|reference| reference.parse::<ImageId>().map(|id| id.to_string()))
            .collect::<Result<BTreeSet<_>>>()?;
        if ids.is_empty() {
            bail!("no workload images to export");
        }
        let mut command = Command::new(&self.engine);
        command.arg("save");
        if matches!(self.kind, Engine::Podman) {
            // Without this flag Podman interprets extra arguments as tags of one image.
            command.args(["--format", "docker-archive", "--multi-image-archive"]);
        }
        command
            .arg("--output")
            .arg(archive)
            .args(ids)
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
    engine: String,
    name: Option<String>,
}

impl ExtractionContainer {
    async fn remove(mut self) -> Result<()> {
        let mut command = Command::new(&self.engine);
        command
            .args([
                "rm",
                "--force",
                self.name.as_deref().expect("container name assigned"),
            ])
            .stdin(Stdio::null());
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
        let mut command = std::process::Command::new(&self.engine);
        command.args(["rm", "--force", &name]);
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
  printf 'guest iso' > "$3"
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
            engine: path.to_string_lossy().into_owned(),
            kind: Engine::Podman,
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
                .extract_iso(ID.parse().unwrap(), &cache)
                .await
                .is_err()
        );
        assert!(!cache.join("guest.iso").exists());
        assert!(!directory.path().join("container").exists());
        assert!(directory.path().join("removed").exists());
        std::fs::remove_file(directory.path().join("fail-copy")).unwrap();
        let iso = images
            .extract_iso(ID.parse().unwrap(), &cache)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&iso).unwrap(), b"guest iso");
        std::fs::remove_file(directory.path().join("started")).unwrap();
        assert_eq!(
            images
                .extract_iso(ID.parse().unwrap(), &cache)
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
        let task =
            tokio::spawn(async move { images.extract_iso(ID.parse().unwrap(), &cache).await });
        wait_for(&directory.path().join("started")).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        std::fs::write(directory.path().join("release"), "").unwrap();
        wait_for(&directory.path().join("removed")).await;
        assert!(!directory.path().join("container").exists());
    }

    #[tokio::test]
    async fn podman_exports_unique_immutable_images_as_a_multi_image_archive() {
        let directory = tempfile::tempdir().unwrap();
        let images = engine(directory.path());
        let other = "b".repeat(64);
        images
            .save(
                &[ID.into(), format!("sha256:{ID}"), other.clone()],
                &directory.path().join("images.tar"),
            )
            .await
            .unwrap();
        let args = std::fs::read_to_string(directory.path().join("save-args")).unwrap();
        assert!(args.lines().any(|arg| arg == "--multi-image-archive"));
        assert_eq!(args.lines().filter(|arg| *arg == ID).count(), 1);
        assert!(args.lines().any(|arg| arg == other));
        assert!(
            images
                .save(&["latest".into()], &directory.path().join("bad.tar"))
                .await
                .is_err()
        );
    }

    #[hegel::test]
    fn digest_normalization_preserves_identity(tc: hegel::TestCase) {
        let bytes = tc.draw(generators::binary().min_size(32).max_size(32));
        let digest = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let id: ImageId = digest.parse().unwrap();
        assert_eq!(
            format!("sha256:{}", digest.to_ascii_uppercase())
                .parse::<ImageId>()
                .unwrap(),
            id
        );
        assert_eq!(id.to_string(), digest);
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
