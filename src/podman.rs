use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

use chrono::Utc;
use color_eyre::{
    Section, SectionExt,
    eyre::{Context, Result, bail, eyre},
};

/// Build and push a config image from a local directory using a pre-generated image reference.
///
/// The directory must contain a `docker-compose.yaml` file.
pub fn build_and_push_config_image(config_dir: &Path, image_ref: &str) -> Result<()> {
    let runtime = find_container_runtime()?;
    validate_compose_file(runtime, config_dir)?;

    eprintln!("Building config image: {}", image_ref);
    container_build(runtime, config_dir, image_ref)?;

    eprintln!("Pushing config image: {}", image_ref);
    image_push(runtime, image_ref)?;
    eprintln!("Config image pushed successfully: {image_ref}");
    Ok(())
}

/// Check that the directory exists and contains a docker-compose file.
pub fn validate_config_dir(config_dir: &Path) -> Result<()> {
    if !config_dir.is_dir() {
        bail!(
            "config directory error: '{}' is not a directory",
            config_dir.display()
        );
    }

    if config_dir.join("docker-compose.yml").is_file() {
        bail!(
            "config directory error: directory '{}' contains docker-compose.yml, but Antithesis requires docker-compose.yaml (rename the file)",
            config_dir.display()
        );
    }

    if !config_dir.join("docker-compose.yaml").is_file() {
        bail!(
            "config directory error: directory '{}' does not contain a docker-compose.yaml file",
            config_dir.display()
        );
    }

    Ok(())
}

/// Run `{runtime} compose config` to validate the compose file.
fn validate_compose_file(runtime: &str, config_dir: &Path) -> Result<()> {
    let output = Command::new(runtime)
        .args(["compose", "config", "--quiet"])
        .current_dir(config_dir)
        .output()
        .wrap_err(format!("failed to run '{runtime} compose config'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("docker-compose file validation failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    Ok(())
}

/// Generate a unique image reference with a timestamp + random suffix tag.
pub fn generate_image_ref(registry: &str) -> String {
    let ts = Utc::now().format("%Y%m%d-%H%M%S");
    let mut buf = [0u8; 2];
    getrandom::fill(&mut buf).expect("failed to get random bytes");
    let suffix = format!("{:02x}{:02x}", buf[0], buf[1]);
    format!(
        "{}/snouty-config:{}-{}",
        registry.trim_end_matches('/'),
        ts,
        suffix
    )
}

static CONTAINER_RUNTIME: OnceLock<Result<String, String>> = OnceLock::new();

/// Find a container runtime, preferring podman over docker.
/// The result is cached so detection only runs once.
fn find_container_runtime() -> Result<&'static str> {
    CONTAINER_RUNTIME
        .get_or_init(|| {
            // Try podman first
            match Command::new("podman").arg("--version").output() {
                Ok(output) if output.status.success() => return Ok("podman".to_string()),
                Ok(_) => {} // podman found but failed, try docker
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // not installed
                Err(e) => return Err(format!("failed to check podman: {e}")),
            }

            // Fall back to docker
            match Command::new("docker").arg("--version").output() {
                Ok(output) if output.status.success() => {
                    log::error!("podman not found, falling back to docker");
                    Ok("docker".to_string())
                }
                Ok(_) => Err(
                    "'docker --version' failed; unable to find working container runtime"
                        .to_string(),
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err("neither podman nor docker is installed".to_string())
                }
                Err(e) => Err(format!("failed to check docker: {e}")),
            }
        })
        .as_ref()
        .map(|s| s.as_str())
        .map_err(|e| eyre!("{e}"))
}

/// Build a scratch image containing the config directory contents.
fn container_build(runtime: &str, config_dir: &Path, image_ref: &str) -> Result<()> {
    let mut child = Command::new(runtime)
        .args(["build", "-t", image_ref, "-f", "-", "."])
        .current_dir(config_dir)
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .wrap_err(format!("failed to start '{runtime} build'"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(b"FROM scratch\nCOPY . /\n")
            .wrap_err("failed to write Dockerfile to stdin")?;
    }

    let output = child
        .wait_with_output()
        .wrap_err(format!("failed to wait for '{runtime} build'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("'{runtime} build' failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    Ok(())
}

/// Push the image to the registry.
fn image_push(runtime: &str, image_ref: &str) -> Result<()> {
    let mut args = vec!["push"];

    // Podman requires --tls-verify=false for plain HTTP registries.
    // Docker treats localhost as insecure automatically.
    if runtime == "podman"
        && (image_ref.starts_with("localhost") || image_ref.starts_with("127.0.0.1"))
    {
        args.push("--tls-verify=false");
    }

    args.push(image_ref);

    let output = Command::new(runtime)
        .args(&args)
        .output()
        .wrap_err(format!("failed to run '{runtime} push'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("'{runtime} push' failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    Ok(())
}

/// Extract image references from the docker-compose.yaml in the given config directory.
/// Uses `{runtime} compose config` to resolve env variable substitutions.
fn extract_image_refs(runtime: &str, config_dir: &Path) -> Result<Vec<String>> {
    let output = Command::new(runtime)
        .args(["compose", "config"])
        .current_dir(config_dir)
        .output()
        .wrap_err(format!("failed to run '{runtime} compose config'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(eyre!("'{runtime} compose config' failed"))
            .with_section(move || stdout.trim().to_string().header("Stdout:"))
            .with_section(move || stderr.trim().to_string().header("Stderr:"));
    }

    let contents = String::from_utf8_lossy(&output.stdout);
    parse_compose_images(&contents)
}

/// Parse image references from resolved compose config YAML.
fn parse_compose_images(yaml: &str) -> Result<Vec<String>> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).wrap_err("failed to parse docker-compose.yaml")?;

    let mut images = Vec::new();
    if let Some(services) = doc.get("services").and_then(|s| s.as_mapping()) {
        for (_name, service) in services {
            if let Some(image) = service.get("image").and_then(|i| i.as_str()) {
                let image = image.to_string();
                if !images.contains(&image) {
                    images.push(image);
                }
            }
        }
    }

    Ok(images)
}

/// Filter images to only those that should be pushed: images whose name
/// starts with the given registry prefix. Bare images (no `/`) are skipped.
fn filter_pushable_images<'a>(images: &'a [String], registry: &str) -> Vec<&'a str> {
    let registry = registry.trim_end_matches('/');
    let prefix = format!("{registry}/");
    images
        .iter()
        .filter(|img| img.starts_with(&prefix))
        .map(|s| s.as_str())
        .collect()
}

/// Push compose images that match the registry before building the config image.
pub fn push_compose_images(config_dir: &Path, registry: &str) -> Result<()> {
    let runtime = find_container_runtime()?;
    let images = extract_image_refs(runtime, config_dir)?;
    let pushable = filter_pushable_images(&images, registry);

    for image in pushable {
        eprintln!("Pushing image: {image}");
        image_push(runtime, image)?;
        eprintln!("Image pushed: {image}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Start a minimal mock OCI registry that accepts image pushes.
    ///
    /// Handles the OCI Distribution Spec endpoints needed for `podman push`:
    /// - `GET  /v2/`                        → 200 (API version check)
    /// - `HEAD /v2/<name>/blobs/<digest>`    → 404 (blob not found)
    /// - `HEAD /v2/<name>/manifests/<ref>`   → 404 (manifest not found)
    /// - `POST /v2/<name>/blobs/uploads/`    → 202 (initiate upload)
    /// - `PATCH  /v2/_uploads/<uuid>`        → 202 (chunked upload)
    /// - `PUT    /v2/_uploads/<uuid>?digest=…` → 201 (complete upload)
    /// - `PUT  /v2/<name>/manifests/<ref>`   → 201 (push manifest)
    ///
    /// Use `registry.uri().replace("http://", "")` as the registry
    /// host:port in image references.
    async fn mock_oci_registry() -> MockServer {
        let server = MockServer::start().await;
        let upload_url = format!("{}/v2/_uploads/test-uuid", server.uri());

        // V2 API version check
        Mock::given(method("GET"))
            .and(path_regex("^/v2/?$"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Docker-Distribution-API-Version", "registry/2.0"),
            )
            .mount(&server)
            .await;

        // Blob existence check → not found
        Mock::given(method("HEAD"))
            .and(path_regex("^/v2/.+/blobs/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        // Manifest existence check → not found
        Mock::given(method("HEAD"))
            .and(path_regex("^/v2/.+/manifests/"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        // Initiate blob upload
        Mock::given(method("POST"))
            .and(path_regex("^/v2/.+/blobs/uploads"))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", &upload_url)
                    .insert_header("Docker-Upload-UUID", "test-uuid")
                    .insert_header("Range", "0-0"),
            )
            .mount(&server)
            .await;

        // Chunked blob upload
        Mock::given(method("PATCH"))
            .and(path_regex("^/v2/_uploads/"))
            .respond_with(
                ResponseTemplate::new(202)
                    .insert_header("Location", &upload_url)
                    .insert_header("Range", "0-999999"),
            )
            .mount(&server)
            .await;

        // Complete blob upload
        Mock::given(method("PUT"))
            .and(path_regex("^/v2/_uploads/"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        // Push manifest
        Mock::given(method("PUT"))
            .and(path_regex("^/v2/.+/manifests/"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        server
    }

    #[tokio::test]
    async fn build_and_push_to_mock_registry() {
        if find_container_runtime().is_err() {
            eprintln!("skipping: no container runtime available");
            return;
        }

        let registry = mock_oci_registry().await;
        let addr = registry.uri().replace("http://", "");

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("docker-compose.yaml"),
            "services:\n  app:\n    image: test:latest\n",
        )
        .unwrap();

        let image_ref = format!("{addr}/test/snouty-config:test");
        build_and_push_config_image(dir.path(), &image_ref).unwrap_or_else(|e| panic!("{e:?}"));

        // Clean up the local image.
        let runtime = find_container_runtime().unwrap();
        let _ = Command::new(runtime).args(["rmi", &image_ref]).output();
    }

    #[test]
    fn validate_config_dir_nonexistent() {
        let result = validate_config_dir(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not a directory"), "got: {err}");
    }

    #[test]
    fn generate_image_ref_format() {
        let image_ref = generate_image_ref("us-central1-docker.pkg.dev/proj/repo");
        assert!(
            image_ref.starts_with("us-central1-docker.pkg.dev/proj/repo/snouty-config:"),
            "got: {image_ref}"
        );
        // Tag should be YYYYMMDD-HHMMSS-XXXX format (20 chars)
        let tag = image_ref.rsplit(':').next().unwrap();
        assert!(
            tag.len() == 20 && tag.chars().nth(8) == Some('-') && tag.chars().nth(15) == Some('-'),
            "tag format should be YYYYMMDD-HHMMSS-XXXX, got: {tag}"
        );
    }

    #[test]
    fn generate_image_ref_strips_trailing_slash() {
        let image_ref = generate_image_ref("registry.example.com/repo/");
        assert!(
            image_ref.starts_with("registry.example.com/repo/snouty-config:"),
            "got: {image_ref}"
        );
    }

    #[test]
    fn parse_compose_images_basic() {
        let yaml = "\
services:
  app:
    image: us-central1-docker.pkg.dev/proj/repo/app:v1
  sidecar:
    image: us-central1-docker.pkg.dev/proj/repo/sidecar:latest
  builder:
    build:
      context: ./builder
";
        let refs = parse_compose_images(yaml).unwrap();
        assert_eq!(
            refs,
            vec![
                "us-central1-docker.pkg.dev/proj/repo/app:v1",
                "us-central1-docker.pkg.dev/proj/repo/sidecar:latest",
            ]
        );
    }

    #[test]
    fn parse_compose_images_deduplicates() {
        let yaml = "\
services:
  a:
    image: myimage:latest
  b:
    image: myimage:latest
";
        let refs = parse_compose_images(yaml).unwrap();
        assert_eq!(refs, vec!["myimage:latest"]);
    }

    #[test]
    fn parse_compose_images_no_services() {
        let yaml = "version: '3'\n";
        let refs = parse_compose_images(yaml).unwrap();
        assert!(refs.is_empty());
    }

    #[test]
    fn extract_image_refs_resolves_env() {
        // Skip if no container runtime is available.
        if find_container_runtime().is_err() {
            eprintln!("skipping: no container runtime available");
            return;
        }
        let runtime = find_container_runtime().unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".env"),
            "REPOSITORY=us-central1-docker.pkg.dev/proj/repo\nIMAGES_TAG=v2\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("docker-compose.yaml"),
            "\
services:
  app:
    image: ${REPOSITORY}/app:${IMAGES_TAG}
  sidecar:
    image: docker.io/library/nginx:latest
",
        )
        .unwrap();

        let refs = extract_image_refs(runtime, dir.path()).unwrap();
        assert_eq!(
            refs,
            vec![
                "us-central1-docker.pkg.dev/proj/repo/app:v2",
                "docker.io/library/nginx:latest",
            ]
        );
    }

    #[test]
    fn filter_pushable_images_matching_registry() {
        let images = vec![
            "us-central1-docker.pkg.dev/proj/repo/app:v1".to_string(),
            "ghcr.io/other/image:latest".to_string(),
            "myorg/foo:bar".to_string(),
            "app:latest".to_string(),
        ];
        let result = filter_pushable_images(&images, "us-central1-docker.pkg.dev/proj/repo");
        assert_eq!(result, vec!["us-central1-docker.pkg.dev/proj/repo/app:v1"]);
    }

    #[test]
    fn filter_pushable_images_trailing_slash() {
        let images = vec!["us-central1-docker.pkg.dev/proj/repo/app:v1".to_string()];
        let result = filter_pushable_images(&images, "us-central1-docker.pkg.dev/proj/repo/");
        assert_eq!(result, vec!["us-central1-docker.pkg.dev/proj/repo/app:v1"]);
    }

    #[test]
    fn filter_pushable_images_empty() {
        let images: Vec<String> = vec![];
        let result = filter_pushable_images(&images, "registry.example.com/repo");
        assert!(result.is_empty());
    }
}
