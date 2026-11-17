//! The EROFS store from each container engine's own images and `save`
//! output, piped into the converter as `snouty simulate` pipes it.
//!
//! A separate test binary: unit tests in the library change `HOME`, which
//! Podman reads.

use std::io::Write;
use std::process::Stdio;

use std::path::Path;

use snouty::container::{ContainerRuntime, save_images};
use snouty::containers_storage::{Converted, convert};
use snouty::erofs_builder::Reader;

fn new_header(mode: u32) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_mode(mode);
    header.set_uid(1000);
    header.set_gid(1000);
    header.set_mtime(0);
    header
}

fn layer() -> (Vec<u8>, Vec<u8>) {
    let big: Vec<u8> = (0..20_000u32).map(|i| (i * 13 % 251) as u8).collect();
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = new_header(0o755);
    header.set_entry_type(tar::EntryType::Directory);
    header.set_size(0);
    builder
        .append_data(&mut header, "app/", std::io::empty())
        .unwrap();
    let mut header = new_header(0o644);
    header.set_size(big.len() as u64);
    builder
        .append_data(&mut header, "app/big", big.as_slice())
        .unwrap();
    for (kind, path, target) in [
        (tar::EntryType::Link, "app/again", "app/big"),
        (tar::EntryType::Symlink, "app/link", "big"),
    ] {
        let mut header = new_header(0o644);
        header.set_entry_type(kind);
        header.set_size(0);
        builder.append_link(&mut header, path, target).unwrap();
    }
    (builder.into_inner().unwrap(), big)
}

async fn nid(reader: &mut Reader, path: &str) -> u64 {
    let mut nid = reader.root_nid();
    for name in path.split('/') {
        nid = reader
            .dirents(nid)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.name == name)
            .unwrap_or_else(|| panic!("image has no {path}"))
            .nid;
    }
    nid
}

async fn read(reader: &mut Reader, path: &str) -> Vec<u8> {
    let nid = nid(reader, path).await;
    let mut data = Vec::new();
    reader
        .read_content(nid, &mut |chunk| data.extend_from_slice(chunk))
        .await
        .unwrap();
    data
}

fn run(command: &mut std::process::Command, what: &str) -> String {
    let output = command.stdin(Stdio::null()).output().unwrap();
    assert!(
        output.status.success(),
        "{what}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Make a one-layer image `tag` from the tar `layer`.
fn import(runtime: &dyn ContainerRuntime, layer: &[u8], tag: &str) {
    let mut import = runtime
        .command(&["import", "-", tag])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    import.stdin.take().unwrap().write_all(layer).unwrap();
    assert!(
        import.wait().unwrap().success(),
        "{} import",
        runtime.name()
    );
}

/// Convert `save` of `tags` as it streams, as simulate does.
async fn save_and_convert(
    runtime: &dyn ContainerRuntime,
    tags: &[&str],
    path: &Path,
) -> color_eyre::Result<Converted> {
    let (archive, exit) = save_images(runtime, tags, None).unwrap();
    let output = std::fs::File::create(path).unwrap();
    let converted = tokio::task::spawn_blocking(move || convert(vec![archive], output, &[]));
    let (converted, saved) = tokio::join!(converted, exit.wait());
    let converted = converted.unwrap();
    // A failed conversion stops reading, which can fail save in turn, so
    // report both.
    assert!(
        saved.is_ok(),
        "{saved:?}\nconversion: {:?}",
        converted.as_ref().err()
    );
    converted
}

#[tokio::test]
async fn engine_save_streams_into_the_store() {
    let (layer, big) = layer();
    for runtime in snouty::testutils::require_runtimes() {
        let name = runtime.name().to_owned();
        let tag = format!("localhost/snouty-stream-test:{name}-{}", std::process::id());
        import(runtime.as_ref(), &layer, &tag);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("images.erofs");
        let converted = save_and_convert(runtime.as_ref(), &[&tag], &path).await;
        snouty::testutils::remove_test_images(&name, &[&tag]);
        assert_eq!(
            converted.unwrap(),
            Converted {
                images: 1,
                layers: 1
            },
            "{name}"
        );

        let mut reader = Reader::open(&path).await.unwrap();
        let images: serde_json::Value = serde_json::from_slice(
            &read(&mut reader, "containers/overlay-images/images.json").await,
        )
        .unwrap();
        assert_eq!(images[0]["names"], serde_json::json!([tag]), "{name}");
        let diff = format!(
            "containers/overlay/{}/diff/app",
            images[0]["layer"].as_str().unwrap()
        );
        assert_eq!(
            read(&mut reader, &format!("{diff}/big")).await,
            big,
            "{name}"
        );
        assert_eq!(
            nid(&mut reader, &format!("{diff}/again")).await,
            nid(&mut reader, &format!("{diff}/big")).await,
            "{name}: a hard link shares its inode"
        );
        assert_eq!(
            read(&mut reader, &format!("{diff}/link")).await,
            b"big",
            "{name}"
        );
    }
}

/// Two images whose engine built the same top layer on different parents.
/// `save` writes that layer once, and the store needs it under each parent.
#[tokio::test]
async fn engine_layer_on_two_parents_is_placed_twice() {
    let shared: Vec<u8> = (0..20_000u32).map(|i| (i * 17 % 251) as u8).collect();
    for runtime in snouty::testutils::require_runtimes() {
        let name = runtime.name().to_owned();
        let tag = |kind: &str, parent: &str| {
            format!(
                "localhost/snouty-two-parents-{kind}:{name}-{parent}-{}",
                std::process::id()
            )
        };
        let context = tempfile::tempdir().unwrap();
        let file = context.path().join("shared.bin");
        std::fs::write(&file, &shared).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH)
            .unwrap();
        let mut tags = Vec::new();
        for parent in ["a", "b"] {
            let mut builder = tar::Builder::new(Vec::new());
            let mut header = new_header(0o644);
            header.set_size(parent.len() as u64);
            builder
                .append_data(&mut header, parent, parent.as_bytes())
                .unwrap();
            let base = tag("base", parent);
            import(runtime.as_ref(), &builder.into_inner().unwrap(), &base);
            let dockerfile = context.path().join(format!("Dockerfile.{parent}"));
            std::fs::write(
                &dockerfile,
                format!("FROM {base}\nCOPY shared.bin /shared.bin\n"),
            )
            .unwrap();
            let top = tag("top", parent);
            run(
                runtime
                    .command(&["build", "-q", "-t", &top, "-f"])
                    .arg(&dockerfile)
                    .arg(context.path()),
                &format!("{name} build"),
            );
            tags.push(base);
            tags.push(top);
        }
        let tops = [tags[1].as_str(), tags[3].as_str()];
        let layers: Vec<Vec<String>> = tops
            .iter()
            .map(|top| {
                let layers = run(
                    &mut runtime.command(&[
                        "image",
                        "inspect",
                        "--format",
                        "{{json .RootFS.Layers}}",
                        top,
                    ]),
                    &format!("{name} inspect"),
                );
                serde_json::from_str(layers.trim()).unwrap()
            })
            .collect();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("images.erofs");
        let converted = save_and_convert(runtime.as_ref(), &tops, &path).await;
        let all: Vec<&str> = tags.iter().map(String::as_str).collect();
        snouty::testutils::remove_test_images(&name, &all);
        assert_eq!(
            layers[0][1], layers[1][1],
            "{name}: the engine shares the top layer"
        );
        assert_ne!(layers[0][0], layers[1][0], "{name}");
        assert_eq!(
            converted.unwrap(),
            Converted {
                images: 2,
                layers: 4
            },
            "{name}"
        );

        let mut reader = Reader::open(&path).await.unwrap();
        let records: Vec<serde_json::Value> = serde_json::from_slice(
            &read(&mut reader, "containers/overlay-layers/layers.json").await,
        )
        .unwrap();
        let copies: Vec<_> = records
            .iter()
            .filter(|record| record["diff-digest"] == layers[0][1].as_str())
            .collect();
        assert_eq!(copies.len(), 2, "{name}");
        assert_ne!(copies[0]["parent"], copies[1]["parent"], "{name}");
        let mut files = Vec::new();
        for copy in copies {
            let path = format!(
                "containers/overlay/{}/diff/shared.bin",
                copy["id"].as_str().unwrap()
            );
            assert_eq!(read(&mut reader, &path).await, shared, "{name}");
            files.push(nid(&mut reader, &path).await);
        }
        assert_ne!(files[0], files[1], "{name}: each copy is its own inode");
        let image = std::fs::read(&path).unwrap();
        assert_eq!(
            image
                .windows(shared.len())
                .filter(|window| *window == shared.as_slice())
                .count(),
            1,
            "{name}: the contents are stored once"
        );
    }
}
