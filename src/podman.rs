use std::io::Write;
use std::path::Path;
use std::process::Command;

use chrono::Utc;
use color_eyre::eyre::{Context, Result, bail};

/// Build and push a config image from a local directory.
///
/// The directory must contain a `docker-compose.yaml` file.
/// Returns the full image reference that was pushed.
pub fn build_and_push_config_image(config_dir: &Path, registry: &str) -> Result<String> {
    validate_config_dir(config_dir)?;

    let runtime = find_container_runtime()?;
    validate_compose_file(&runtime, config_dir)?;

    let image_ref = generate_image_ref(registry);

    eprintln!("Building config image: {}", image_ref);
    container_build(&runtime, config_dir, &image_ref)?;

    eprintln!("Pushing config image: {}", image_ref);
    container_push(&runtime, &image_ref)?;

    eprintln!("Config image pushed successfully");
    Ok(image_ref)
}

/// Check that the directory exists and contains a docker-compose file.
fn validate_config_dir(config_dir: &Path) -> Result<()> {
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
        bail!("docker-compose file validation failed:\n{}", stderr.trim());
    }

    Ok(())
}

/// Generate a unique image reference with a timestamp + random suffix tag.
fn generate_image_ref(registry: &str) -> String {
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

/// Find a container runtime, preferring podman over docker.
fn find_container_runtime() -> Result<String> {
    // Try podman first
    match Command::new("podman").arg("--version").output() {
        Ok(output) if output.status.success() => return Ok("podman".to_string()),
        Ok(_) => {} // podman found but failed, try docker
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // not installed
        Err(e) => bail!("failed to check podman: {e}"),
    }

    // Fall back to docker
    match Command::new("docker").arg("--version").output() {
        Ok(output) if output.status.success() => {
            eprintln!("podman not found, falling back to docker");
            Ok("docker".to_string())
        }
        Ok(_) => bail!("neither podman nor docker is available"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            bail!("neither podman nor docker is installed")
        }
        Err(e) => bail!("failed to check docker: {e}"),
    }
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
        bail!("'{runtime} build' failed:\n{}", stderr.trim());
    }

    Ok(())
}

/// Push the image to the registry.
fn container_push(runtime: &str, image_ref: &str) -> Result<()> {
    let output = Command::new(runtime)
        .args(["push", image_ref])
        .output()
        .wrap_err(format!("failed to run '{runtime} push'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("'{runtime} push' failed:\n{}", stderr.trim());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
