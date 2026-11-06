//! Converts `docker save` and `podman save` archives into one EROFS image
//! that holds a containers-storage tree for the overlay driver.
//!
//! The production pipeline writes this tree to disk on a Linux host with
//! `transfer-containers.py`. This module reads tar streams and writes the
//! image file directly, so it runs on any host OS: no Linux file system
//! semantics (symlinks, device nodes, xattrs, ownership) reach the host file
//! system. A Linux guest mounts the image read-only and lists it in
//! `additionalimagestores`, and then Podman and CRI-O run its images as if
//! they had pulled them.
//!
//! Several archives can go into one image. Images and layers that two of
//! them share are written once.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context as TaskContext, Poll, Waker};

use base64::Engine as _;
use color_eyre::eyre::{Context, Result, bail, ensure, eyre};
use erofs_builder::layout::mode;
use erofs_builder::{CreateOptions, InodeMeta};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const WHITEOUT_PREFIX: &[u8] = b".wh.";
const OPAQUE_MARKER: &[u8] = b".wh..wh..opq";
const OPAQUE_XATTR: &str = "trusted.overlay.opaque";
const PAX_XATTR_PREFIX: &str = "SCHILY.xattr.";

/// What [`convert`] wrote.
#[derive(Debug, PartialEq, Eq)]
pub struct Converted {
    pub images: usize,
    pub layers: usize,
}

/// Convert the image archives at `archives` into one EROFS image on
/// `output`.
pub fn convert(archives: &[&Path], output: File) -> Result<Converted> {
    let mut store = StoreWriter::new(output)?;
    for path in archives {
        let archive = SavedArchive::open(path)?;
        let saved: Vec<SavedImage> = serde_json::from_slice(&archive.read("manifest.json")?)
            .wrap_err("invalid manifest.json in image archive")?;
        let manifests = if archive.contains("index.json") {
            oci_manifests(&archive)?
        } else {
            HashMap::new()
        };
        for image in saved {
            store.add_image(&archive, image, &manifests)?;
        }
    }
    store.finish()
}

/// A `sha256:` digest, stored as lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Sha256Digest(String);

impl Sha256Digest {
    fn of(bytes: &[u8]) -> Self {
        Self::from_hasher(Sha256::new_with_prefix(bytes))
    }

    fn from_hasher(hasher: Sha256) -> Self {
        Self(hex(&hasher.finalize()))
    }

    fn hex(&self) -> &str {
        &self.0
    }
}

impl FromStr for Sha256Digest {
    type Err = color_eyre::Report;

    fn from_str(value: &str) -> Result<Self> {
        let hex = value
            .strip_prefix("sha256:")
            .ok_or_else(|| eyre!("unsupported digest {value}"))?;
        ensure!(
            hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid digest {value}"
        );
        Ok(Self(hex.to_ascii_lowercase()))
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sha256:{}", self.0)
    }
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}

/// An image archive from `docker save` or `podman save`, indexed for random
/// access to its members.
struct SavedArchive {
    path: PathBuf,
    /// Member name → (data offset, size).
    members: HashMap<String, (u64, u64)>,
}

impl SavedArchive {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .wrap_err_with(|| format!("cannot open image archive {}", path.display()))?;
        let mut archive = tar::Archive::new(BufReader::new(file));
        let mut members = HashMap::new();
        // Classic `docker save` links a repeated layer to its first copy.
        let mut links = Vec::new();
        for entry in archive.entries()? {
            let entry = entry?;
            let name = member_name(&entry.path_bytes());
            match entry.header().entry_type() {
                tar::EntryType::Regular | tar::EntryType::Continuous => {
                    members.insert(name, (entry.raw_file_position(), entry.size()));
                }
                tar::EntryType::Symlink | tar::EntryType::Link => {
                    let target = entry.link_name_bytes().unwrap_or_default();
                    let target = if entry.header().entry_type() == tar::EntryType::Symlink {
                        let parent = name.rsplit_once('/').map_or("", |(dir, _)| dir);
                        resolve(parent, &String::from_utf8_lossy(&target))
                    } else {
                        member_name(&target)
                    };
                    links.push((name, target));
                }
                _ => {}
            }
        }
        for (name, target) in links {
            if let Some(&member) = members.get(&target) {
                members.insert(name, member);
            }
        }
        Ok(Self {
            path: path.to_owned(),
            members,
        })
    }

    fn contains(&self, name: &str) -> bool {
        self.members.contains_key(name)
    }

    fn open_member(&self, name: &str) -> Result<io::Take<BufReader<File>>> {
        let &(offset, size) = self
            .members
            .get(name)
            .ok_or_else(|| eyre!("image archive has no {name}"))?;
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(BufReader::new(file).take(size))
    }

    fn read(&self, name: &str) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.open_member(name)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

fn member_name(path: &[u8]) -> String {
    resolve("", &String::from_utf8_lossy(path))
}

/// Join `path` onto the archive directory `base` and normalize the result.
fn resolve(base: &str, path: &str) -> String {
    let mut parts: Vec<&str> = if path.starts_with('/') {
        Vec::new()
    } else {
        base.split('/').filter(|part| !part.is_empty()).collect()
    };
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    parts.join("/")
}

/// One entry of `manifest.json`, the Docker archive format that both
/// `docker save` and `podman save --format docker-archive` write.
#[derive(Deserialize)]
struct SavedImage {
    #[serde(rename = "Config")]
    config: String,
    #[serde(rename = "RepoTags", default)]
    repo_tags: Option<Vec<String>>,
    #[serde(rename = "Layers")]
    layers: Vec<String>,
}

#[derive(Deserialize)]
struct Descriptor {
    digest: Sha256Digest,
}

/// An OCI index or image manifest, or a Docker manifest list or manifest.
#[derive(Deserialize)]
struct ManifestDocument {
    #[serde(default)]
    manifests: Vec<Descriptor>,
    config: Option<Descriptor>,
    #[serde(default)]
    layers: Vec<Descriptor>,
}

/// A manifest or index exactly as the registry served it. Its digest is what
/// a `name@sha256:…` reference names, so the bytes must not change.
#[derive(Clone)]
struct Document {
    digest: Sha256Digest,
    bytes: Vec<u8>,
}

/// The image manifests that `index.json` reaches, keyed by config digest.
/// Each value lists the indexes on the path to the manifest, outermost
/// first, and ends with the manifest itself.
fn oci_manifests(archive: &SavedArchive) -> Result<HashMap<Sha256Digest, Vec<Document>>> {
    fn walk(
        archive: &SavedArchive,
        descriptors: &[Descriptor],
        path: &[Document],
        found: &mut HashMap<Sha256Digest, Vec<Document>>,
    ) -> Result<()> {
        for descriptor in descriptors {
            let name = format!("blobs/sha256/{}", descriptor.digest.hex());
            // An archive keeps only the platform it saved; skip the others.
            if !archive.contains(&name) {
                continue;
            }
            let bytes = archive.read(&name)?;
            ensure!(
                Sha256Digest::of(&bytes) == descriptor.digest,
                "image archive member {name} does not match its digest"
            );
            let document: ManifestDocument = serde_json::from_slice(&bytes)
                .wrap_err_with(|| format!("invalid manifest {}", descriptor.digest))?;
            let mut path = path.to_vec();
            path.push(Document {
                digest: descriptor.digest.clone(),
                bytes,
            });
            if let Some(config) = document.config {
                found.entry(config.digest).or_insert(path);
            } else {
                walk(archive, &document.manifests, &path, found)?;
            }
        }
        Ok(())
    }
    let index: ManifestDocument =
        serde_json::from_slice(&archive.read("index.json")?).wrap_err("invalid index.json")?;
    let mut found = HashMap::new();
    walk(archive, &index.manifests, &[], &mut found)?;
    Ok(found)
}

#[derive(Deserialize)]
struct ImageConfig {
    created: Option<String>,
    rootfs: RootFs,
}

#[derive(Deserialize)]
struct RootFs {
    diff_ids: Vec<Sha256Digest>,
}

/// How a layer blob is compressed, numbered as containers-storage numbers it
/// in `layers.json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Compression {
    Uncompressed = 0,
    Gzip = 2,
}

impl Compression {
    fn detect(magic: &[u8]) -> Result<Self> {
        match magic {
            [0x1f, 0x8b, ..] => Ok(Self::Gzip),
            [0x28, 0xb5, 0x2f, 0xfd, ..] => bail!("zstd layers are not supported yet"),
            _ => Ok(Self::Uncompressed),
        }
    }

    fn media_type(self) -> &'static str {
        match self {
            Self::Uncompressed => "application/vnd.docker.image.rootfs.diff.tar",
            Self::Gzip => "application/vnd.docker.image.rootfs.diff.tar.gzip",
        }
    }
}

impl Serialize for Compression {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(*self as u8)
    }
}

/// A reader that hashes and counts the bytes it passes on.
struct Hashing<R> {
    inner: R,
    hasher: Sha256,
    size: u64,
}

impl<R: Read> Hashing<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            size: 0,
        }
    }

    /// Read to the end and return the digest and size of everything read.
    fn finish(mut self) -> io::Result<(Sha256Digest, u64, R)> {
        io::copy(&mut self, &mut io::sink())?;
        Ok((
            Sha256Digest::from_hasher(self.hasher),
            self.size,
            self.inner,
        ))
    }
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        self.size += read as u64;
        Ok(read)
    }
}

enum Decoded<R: BufRead> {
    Uncompressed(R),
    Gzip(MultiGzDecoder<R>),
}

impl<R: BufRead> Decoded<R> {
    fn into_inner(self) -> R {
        match self {
            Self::Uncompressed(inner) => inner,
            Self::Gzip(decoder) => decoder.into_inner(),
        }
    }
}

impl<R: BufRead> Read for Decoded<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Uncompressed(inner) => inner.read(buf),
            Self::Gzip(decoder) => decoder.read(buf),
        }
    }
}

/// An entry of `overlay-layers/layers.json`.
#[derive(Serialize)]
struct LayerRecord {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    created: String,
    #[serde(rename = "compressed-diff-digest")]
    compressed_digest: Sha256Digest,
    #[serde(rename = "compressed-size")]
    compressed_size: u64,
    #[serde(rename = "diff-digest")]
    diff_digest: Sha256Digest,
    #[serde(rename = "diff-size")]
    diff_size: u64,
    compression: Compression,
    uidset: BTreeSet<u64>,
    gidset: BTreeSet<u64>,
}

struct Layer {
    record: LayerRecord,
    /// The overlay `lower` value of a child: this layer's link, then its
    /// own lower layers, top first.
    lower_for_child: String,
}

/// An entry of `overlay-images/images.json`.
#[derive(Serialize)]
struct ImageRecord {
    id: String,
    digest: Sha256Digest,
    names: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    layer: Option<String>,
    metadata: &'static str,
    #[serde(rename = "big-data-names")]
    big_data_names: Vec<String>,
    #[serde(rename = "big-data-sizes")]
    big_data_sizes: BTreeMap<String, u64>,
    #[serde(rename = "big-data-digests")]
    big_data_digests: BTreeMap<String, Sha256Digest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
}

/// The ownership and times of an entry.
#[derive(Clone, Copy)]
struct Meta {
    mode: u32,
    uid: u64,
    gid: u64,
    mtime: u64,
}

impl Meta {
    fn root(mode: u32, mtime: u64) -> Self {
        Self {
            mode,
            uid: 0,
            gid: 0,
            mtime,
        }
    }

    /// The EROFS inode metadata for a file of type `kind`, an `S_IF*` value.
    fn inode(self, kind: u16, xattrs: BTreeMap<String, Vec<u8>>) -> Result<InodeMeta> {
        Ok(InodeMeta {
            mode: kind | (self.mode & 0o7777) as u16,
            uid: u32::try_from(self.uid).wrap_err("uid does not fit in 32 bits")?,
            gid: u32::try_from(self.gid).wrap_err("gid does not fit in 32 bits")?,
            mtime: self.mtime,
            xattrs,
            ..InodeMeta::default()
        })
    }
}

type Xattrs = BTreeMap<String, Vec<u8>>;
type Image = erofs_builder::Writer<Blocking<io::BufWriter<File>>>;

struct StoreWriter {
    image: Image,
    layers: Vec<Layer>,
    layer_index: HashMap<String, usize>,
    images: Vec<ImageRecord>,
    created: String,
    mtime: u64,
}

impl StoreWriter {
    fn new(output: File) -> Result<Self> {
        let now = chrono::Utc::now();
        let mtime = u64::try_from(now.timestamp()).unwrap_or_default();
        let image = run(erofs_builder::Writer::new(
            Blocking::new(io::BufWriter::new(output)),
            CreateOptions {
                build_time: mtime,
                volume_name: "snouty-images".to_owned(),
                ..CreateOptions::default()
            },
        ))?;
        let mut writer = Self {
            image,
            layers: Vec::new(),
            layer_index: HashMap::new(),
            images: Vec::new(),
            created: now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            mtime,
        };
        for (path, mode) in [
            ("overlay", 0o700),
            ("overlay/l", 0o755),
            ("overlay-layers", 0o700),
            ("overlay-images", 0o700),
        ] {
            writer.directory(path, mode)?;
        }
        Ok(writer)
    }

    fn add_image(
        &mut self,
        archive: &SavedArchive,
        image: SavedImage,
        manifests: &HashMap<Sha256Digest, Vec<Document>>,
    ) -> Result<()> {
        let config_bytes = archive.read(&image.config)?;
        let config_digest = Sha256Digest::of(&config_bytes);
        let names = image_names(image.repo_tags.unwrap_or_default());
        if let Some(existing) = self
            .images
            .iter_mut()
            .find(|existing| existing.id == config_digest.hex())
        {
            existing.names.extend(names);
            existing.names.sort();
            existing.names.dedup();
            return Ok(());
        }
        let config: ImageConfig = serde_json::from_slice(&config_bytes)
            .wrap_err_with(|| format!("invalid image config {config_digest}"))?;
        ensure!(
            config.rootfs.diff_ids.len() == image.layers.len(),
            "image {config_digest} lists {} layers but its config lists {}",
            image.layers.len(),
            config.rootfs.diff_ids.len()
        );
        let documents = manifests.get(&config_digest);
        let manifest_layers = documents
            .and_then(|documents| documents.last())
            .map(|manifest| -> Result<_> {
                let document: ManifestDocument = serde_json::from_slice(&manifest.bytes)?;
                ensure!(
                    document.layers.len() == image.layers.len(),
                    "manifest {} does not match image {config_digest}",
                    manifest.digest
                );
                Ok(document.layers)
            })
            .transpose()?;

        let mut chain = Vec::with_capacity(image.layers.len());
        for (position, (blob, diff_id)) in
            image.layers.iter().zip(config.rootfs.diff_ids).enumerate()
        {
            let parent = chain.last().copied();
            let id = match parent {
                None => diff_id.hex().to_owned(),
                Some(parent) => {
                    let parent: &Layer = &self.layers[parent];
                    Sha256Digest::of(format!("{}+{diff_id}", parent.record.id).as_bytes())
                        .hex()
                        .to_owned()
                }
            };
            let index = match self.layer_index.get(&id) {
                Some(&index) => index,
                None => self.write_layer(archive, blob, id, parent, &diff_id)?,
            };
            if let Some(layers) = &manifest_layers {
                let compressed = &self.layers[index].record.compressed_digest;
                ensure!(
                    &layers[position].digest == compressed,
                    "layer {blob} does not match manifest digest {}",
                    layers[position].digest
                );
            }
            chain.push(index);
        }

        let documents = match documents {
            Some(documents) => documents.clone(),
            None => vec![self.synthesize_manifest(&config_bytes, &config_digest, &chain)?],
        };
        let manifest = documents
            .last()
            .expect("a manifest path ends at a manifest");
        let mut big_data = vec![
            (config_digest.to_string(), config_bytes.clone()),
            ("manifest".to_owned(), manifest.bytes.clone()),
        ];
        big_data.extend(documents.iter().map(|document| {
            (
                format!("manifest-{}", document.digest),
                document.bytes.clone(),
            )
        }));
        let directory = format!("overlay-images/{}", config_digest.hex());
        self.directory(&directory, 0o700)?;
        let mut record = ImageRecord {
            id: config_digest.hex().to_owned(),
            digest: documents[0].digest.clone(),
            names,
            layer: chain
                .last()
                .map(|&index| self.layers[index].record.id.clone()),
            metadata: "{}",
            big_data_names: Vec::new(),
            big_data_sizes: BTreeMap::new(),
            big_data_digests: BTreeMap::new(),
            created: config.created,
        };
        for (key, bytes) in big_data {
            self.file(
                &format!("{directory}/{}", big_data_file_name(&key)),
                0o600,
                &bytes,
            )?;
            record
                .big_data_sizes
                .insert(key.clone(), bytes.len() as u64);
            record
                .big_data_digests
                .insert(key.clone(), Sha256Digest::of(&bytes));
            record.big_data_names.push(key);
        }
        self.images.push(record);
        Ok(())
    }

    /// A Docker v2 schema 2 manifest for an archive without the original,
    /// such as one from `podman save` or the classic Docker image store.
    fn synthesize_manifest(
        &self,
        config_bytes: &[u8],
        config_digest: &Sha256Digest,
        chain: &[usize],
    ) -> Result<Document> {
        let layers: Vec<_> = chain
            .iter()
            .map(|&index| {
                let record = &self.layers[index].record;
                serde_json::json!({
                    "mediaType": record.compression.media_type(),
                    "size": record.compressed_size,
                    "digest": record.compressed_digest,
                })
            })
            .collect();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "size": config_bytes.len(),
                "digest": config_digest,
            },
            "layers": layers,
        }))?;
        Ok(Document {
            digest: Sha256Digest::of(&bytes),
            bytes,
        })
    }

    fn write_layer(
        &mut self,
        archive: &SavedArchive,
        blob: &str,
        id: String,
        parent: Option<usize>,
        diff_id: &Sha256Digest,
    ) -> Result<usize> {
        let link = short_link(&id);
        let base = format!("overlay/{id}");
        let diff = format!("{base}/diff");
        self.directory(&base, 0o700)?;
        self.directory(&diff, 0o755)?;

        let mut raw = BufReader::new(Hashing::new(archive.open_member(blob)?));
        let compression = Compression::detect(raw.fill_buf()?)?;
        let decoded = match compression {
            Compression::Uncompressed => Decoded::Uncompressed(raw),
            Compression::Gzip => Decoded::Gzip(MultiGzDecoder::new(raw)),
        };
        let mut layer = tar::Archive::new(Hashing::new(decoded));
        let (uidset, gidset) = self
            .copy_layer_entries(&mut layer, &diff)
            .wrap_err_with(|| format!("cannot convert layer {blob}"))?;
        let (diff_digest, diff_size, decoded) = layer.into_inner().finish()?;
        ensure!(
            &diff_digest == diff_id,
            "layer {blob} has digest {diff_digest}, but its image config says {diff_id}"
        );
        let (compressed_digest, compressed_size, _) = decoded.into_inner().into_inner().finish()?;

        self.file(&format!("{base}/link"), 0o644, link.as_bytes())?;
        let lower = parent.map(|parent| self.layers[parent].lower_for_child.clone());
        if let Some(lower) = &lower {
            self.file(&format!("{base}/lower"), 0o644, lower.as_bytes())?;
        } else {
            self.directory(&format!("{base}/empty"), 0o700)?;
        }
        self.directory(&format!("{base}/work"), 0o700)?;
        self.directory(&format!("{base}/merged"), 0o700)?;
        self.symlink(&format!("overlay/l/{link}"), &format!("../{id}/diff"))?;

        let lower_for_child = match lower {
            Some(lower) => format!("l/{link}:{lower}"),
            None => format!("l/{link}"),
        };
        let index = self.layers.len();
        self.layer_index.insert(id.clone(), index);
        self.layers.push(Layer {
            record: LayerRecord {
                id,
                parent: parent.map(|parent| self.layers[parent].record.id.clone()),
                created: self.created.clone(),
                compressed_digest,
                compressed_size,
                diff_digest,
                diff_size,
                compression,
                uidset,
                gidset,
            },
            lower_for_child,
        });
        Ok(index)
    }

    /// Copy the entries of a layer under `diff`, converting AUFS whiteouts to
    /// the overlay form: `.wh.NAME` becomes a 0/0 character device NAME, and
    /// `.wh..wh..opq` sets `trusted.overlay.opaque` on its directory.
    fn copy_layer_entries<R: Read>(
        &mut self,
        layer: &mut tar::Archive<R>,
        diff: &str,
    ) -> Result<(BTreeSet<u64>, BTreeSet<u64>)> {
        let mut uids = BTreeSet::new();
        let mut gids = BTreeSet::new();
        let mut directories: HashMap<Vec<u8>, (Meta, Xattrs)> = HashMap::new();
        let mut opaque: HashSet<Vec<u8>> = HashSet::new();
        for entry in layer.entries()? {
            let mut entry = entry?;
            let path = clean(&entry.path_bytes())?;
            let link = entry
                .link_name_bytes()
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_default();
            let header = entry.header();
            let kind = header.entry_type();
            let mut meta = Meta {
                mode: header.mode()?,
                uid: header.uid()?,
                gid: header.gid()?,
                mtime: header.mtime()?,
            };
            let rdev = if matches!(kind, tar::EntryType::Char | tar::EntryType::Block) {
                encode_device(
                    header.device_major()?.unwrap_or(0),
                    header.device_minor()?.unwrap_or(0),
                )
            } else {
                0
            };
            let mut xattrs = Xattrs::new();
            if let Some(extensions) = entry.pax_extensions()? {
                for extension in extensions {
                    let extension = extension?;
                    let key = extension.key()?;
                    let value = extension.value_bytes();
                    match key {
                        "uid" => meta.uid = parse_pax_number(value)?,
                        "gid" => meta.gid = parse_pax_number(value)?,
                        key => {
                            if let Some(name) = key.strip_prefix(PAX_XATTR_PREFIX) {
                                xattrs.insert(name.to_owned(), value.to_vec());
                            }
                        }
                    }
                }
            }
            uids.insert(meta.uid);
            gids.insert(meta.gid);
            let (parent, name) = match path.iter().rposition(|&b| b == b'/') {
                Some(slash) => (&path[..slash], &path[slash + 1..]),
                None => (&path[..0], &path[..]),
            };
            if name == OPAQUE_MARKER {
                let (meta, mut xattrs) = directories
                    .get(parent)
                    .cloned()
                    .unwrap_or_else(|| (Meta::root(0o755, meta.mtime), Xattrs::new()));
                xattrs.insert(OPAQUE_XATTR.to_owned(), b"y".to_vec());
                opaque.insert(parent.to_vec());
                self.mkdir(&join(diff, parent)?, meta, xattrs)?;
                continue;
            }
            if let Some(hidden) = name.strip_prefix(WHITEOUT_PREFIX) {
                // AUFS metadata such as `.wh..wh.plnk` has no overlay form.
                if hidden.starts_with(WHITEOUT_PREFIX) {
                    continue;
                }
                let mut target = parent.to_vec();
                if !target.is_empty() {
                    target.push(b'/');
                }
                target.extend_from_slice(hidden);
                let inode = Meta { mode: 0, ..meta }.inode(mode::S_IFCHR, Xattrs::new())?;
                run(self.image.mknod(&join(diff, &target)?, inode))?;
                continue;
            }
            let target = join(diff, &path)?;
            match kind {
                tar::EntryType::Directory => {
                    directories.insert(path.clone(), (meta, xattrs.clone()));
                    // An opaque marker can precede its directory's entry.
                    if opaque.contains(&path) {
                        xattrs.insert(OPAQUE_XATTR.to_owned(), b"y".to_vec());
                    }
                    self.mkdir(&target, meta, xattrs)?;
                }
                tar::EntryType::Regular | tar::EntryType::Continuous => {
                    let size = entry.size();
                    self.add_file(&target, meta, xattrs, size, &mut entry)?;
                }
                tar::EntryType::Symlink => {
                    let inode = meta.inode(mode::S_IFLNK, xattrs)?;
                    run(self.image.symlink(&target, &link, inode))?;
                }
                tar::EntryType::Link => {
                    run(self.image.link(&target, &join(diff, &clean(&link)?)?))?;
                }
                tar::EntryType::Char | tar::EntryType::Block | tar::EntryType::Fifo => {
                    let kind = match kind {
                        tar::EntryType::Char => mode::S_IFCHR,
                        tar::EntryType::Block => mode::S_IFBLK,
                        _ => mode::S_IFIFO,
                    };
                    let inode = InodeMeta {
                        rdev,
                        ..meta.inode(kind, xattrs)?
                    };
                    run(self.image.mknod(&target, inode))?;
                }
                tar::EntryType::XGlobalHeader => {}
                other => bail!(
                    "unsupported tar entry type {other:?} at {}",
                    String::from_utf8_lossy(&path)
                ),
            }
        }
        Ok((uids, gids))
    }

    fn finish(mut self) -> Result<Converted> {
        let layers: Vec<&LayerRecord> = self.layers.iter().map(|layer| &layer.record).collect();
        let layers_json = serde_json::to_vec(&layers)?;
        let images_json = serde_json::to_vec(&self.images)?;
        self.file("overlay-layers/layers.json", 0o600, &layers_json)?;
        self.file("overlay-images/images.json", 0o600, &images_json)?;
        // A read-only store must already have its lock files: containers-
        // storage opens them before it reads the catalogs.
        self.file("overlay-layers/layers.lock", 0o644, b"")?;
        self.file("overlay-images/images.lock", 0o644, b"")?;
        let converted = Converted {
            images: self.images.len(),
            layers: self.layers.len(),
        };
        run(self.image.finish())?.into_inner().flush()?;
        Ok(converted)
    }

    fn directory(&mut self, path: &str, mode: u32) -> Result<()> {
        self.mkdir(path, Meta::root(mode, self.mtime), Xattrs::new())
    }

    fn mkdir(&mut self, path: &str, meta: Meta, xattrs: Xattrs) -> Result<()> {
        let inode = meta.inode(mode::S_IFDIR, xattrs)?;
        Ok(run(self.image.mkdir(path, inode))?)
    }

    fn file(&mut self, path: &str, mode: u32, bytes: &[u8]) -> Result<()> {
        let meta = Meta::root(mode, self.mtime);
        self.add_file(path, meta, Xattrs::new(), bytes.len() as u64, bytes)
    }

    fn add_file(
        &mut self,
        path: &str,
        meta: Meta,
        xattrs: Xattrs,
        size: u64,
        data: impl Read + Unpin,
    ) -> Result<()> {
        let inode = meta.inode(mode::S_IFREG, xattrs)?;
        let mut data = Blocking::new(data);
        Ok(run(self.image.add_file(path, inode, size, &mut data))?)
    }

    fn symlink(&mut self, path: &str, target: &str) -> Result<()> {
        let inode = Meta::root(0o777, self.mtime).inode(mode::S_IFLNK, Xattrs::new())?;
        Ok(run(self.image.symlink(path, target.as_bytes(), inode))?)
    }
}

fn parse_pax_number(value: &[u8]) -> Result<u64> {
    Ok(std::str::from_utf8(value)?.parse()?)
}

/// Normalize a layer path: drop empty, `.` and leading `/` components.
/// Reject `..`, which could escape the layer directory.
fn clean(path: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(path.len());
    for part in path.split(|&b| b == b'/') {
        match part {
            b"" | b"." => {}
            b".." => bail!(
                "layer path {} escapes the layer",
                String::from_utf8_lossy(path)
            ),
            part => {
                if !out.is_empty() {
                    out.push(b'/');
                }
                out.extend_from_slice(part);
            }
        }
    }
    Ok(out)
}

/// Join a layer path onto the layer's `diff` directory. EROFS names here
/// must be UTF-8.
fn join(base: &str, path: &[u8]) -> Result<String> {
    let path = std::str::from_utf8(path).map_err(|_| {
        eyre!(
            "layer path {} is not UTF-8, which is not supported yet",
            String::from_utf8_lossy(path)
        )
    })?;
    Ok(if path.is_empty() {
        base.to_owned()
    } else {
        format!("{base}/{path}")
    })
}

/// A device number in the encoding that Linux `new_encode_dev` uses, which
/// EROFS stores.
fn encode_device(major: u32, minor: u32) -> u32 {
    (minor & 0xff) | (major << 8) | ((minor & !0xff) << 12)
}

/// Blocking I/O behind the async traits that erofs-builder takes. Every call
/// completes before it returns, so [`run`] drives the writer without a
/// runtime.
struct Blocking<T> {
    inner: T,
    seek: Option<SeekFrom>,
}

impl<T> Blocking<T> {
    fn new(inner: T) -> Self {
        Self { inner, seek: None }
    }

    fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: Read + Unpin> tokio::io::AsyncRead for Blocking<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let read = self.inner.read(buf.initialize_unfilled());
        Poll::Ready(read.map(|read| buf.advance(read)))
    }
}

impl<T: Write + Unpin> tokio::io::AsyncWrite for Blocking<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.inner.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.flush())
    }
}

impl<T: Seek + Unpin> tokio::io::AsyncSeek for Blocking<T> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        self.seek = Some(position);
        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<u64>> {
        let position = match self.seek.take() {
            Some(position) => self.inner.seek(position),
            None => self.inner.stream_position(),
        };
        Poll::Ready(position)
    }
}

/// Drive a future whose I/O is all [`Blocking`].
fn run<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => unreachable!("blocking I/O completes before it returns"),
    }
}

/// The names containers-storage records: Docker Hub shorthand expanded, so
/// `nginx:1.25` becomes `docker.io/library/nginx:1.25`, which is how both
/// Podman and Kubernetes resolve the short name.
fn image_names(repo_tags: Vec<String>) -> Vec<String> {
    let mut names: Vec<String> = repo_tags
        .into_iter()
        .map(|reference| {
            let slash = reference.rfind('/').map_or(0, |slash| slash + 1);
            match reference[slash..].rfind(':') {
                Some(colon) => {
                    let (repo, tag) = reference.split_at(slash + colon);
                    format!("{}{tag}", crate::container::normalize_repo(repo))
                }
                None => format!("{}:latest", crate::container::normalize_repo(&reference)),
            }
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The file name containers-storage gives a big-data item: the key itself
/// when it has only lowercase letters, digits and dots, otherwise `=` and
/// the key in base64 (`makeBigDataBaseName`).
fn big_data_file_name(key: &str) -> String {
    if key
        .bytes()
        .all(|b| b == b'.' || b.is_ascii_digit() || b.is_ascii_lowercase())
    {
        key.to_owned()
    } else {
        format!("={}", base64::engine::general_purpose::STANDARD.encode(key))
    }
}

/// The 26-character name of a layer's `overlay/l` symlink. Podman draws it
/// at random; deriving it from the layer ID gives the same result shape.
fn short_link(id: &str) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let digest = Sha256::digest(id.as_bytes());
    let mut bits: u32 = 0;
    let mut count = 0;
    let mut out = String::with_capacity(26);
    for &byte in &digest[..16] {
        bits = (bits << 8) | u32::from(byte);
        count += 8;
        while count >= 5 {
            count -= 5;
            out.push(char::from(ALPHABET[((bits >> count) & 31) as usize]));
        }
    }
    out.push(char::from(ALPHABET[((bits << (5 - count)) & 31) as usize]));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression as Level, write::GzEncoder};

    /// Reads the subset of EROFS that the writer emits: flat inodes with
    /// inline xattrs, no compression.
    struct ErofsImage {
        bytes: Vec<u8>,
        block_size: usize,
        meta: usize,
        root: u64,
    }

    #[derive(Debug)]
    struct Inode {
        nid: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        nlink: u32,
        rdev: u32,
        xattrs: Xattrs,
        /// File contents, symlink target, or directory entry blocks.
        data: Vec<u8>,
    }

    impl Inode {
        fn kind(&self) -> u16 {
            self.mode & mode::S_IFMT
        }
    }

    fn le16(bytes: &[u8], at: usize) -> u16 {
        u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap())
    }

    fn le32(bytes: &[u8], at: usize) -> u32 {
        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
    }

    fn le64(bytes: &[u8], at: usize) -> u64 {
        u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
    }

    impl ErofsImage {
        fn new(bytes: Vec<u8>) -> Self {
            let superblock = &bytes[1024..1024 + 128];
            assert_eq!(le32(superblock, 0), 0xE0F5_E1E2, "EROFS magic");
            let block_size = 1 << superblock[12];
            let root = u64::from(le16(superblock, 14));
            let meta = le32(superblock, 40) as usize * block_size;
            Self {
                bytes,
                block_size,
                meta,
                root,
            }
        }

        fn inode(&self, nid: u64) -> Inode {
            let base = self.meta + nid as usize * 32;
            let b = &self.bytes;
            let format = le16(b, base);
            let extended = format & 1 != 0;
            let inline = (format >> 1) & 7 == 2;
            let xattr_count = le16(b, base + 2) as usize;
            let mode = le16(b, base + 4);
            let (size, uid, gid, nlink, inode_size) = if extended {
                (
                    le64(b, base + 8),
                    le32(b, base + 24),
                    le32(b, base + 28),
                    le32(b, base + 44),
                    64,
                )
            } else {
                (
                    u64::from(le32(b, base + 8)),
                    u32::from(le16(b, base + 24)),
                    u32::from(le16(b, base + 26)),
                    u32::from(le16(b, base + 6)),
                    32,
                )
            };
            let i_u = le32(b, base + 16);
            let xattr_size = if xattr_count == 0 {
                0
            } else {
                12 + (xattr_count - 1) * 4
            };
            let mut xattrs = Xattrs::new();
            let mut at = base + inode_size + 12;
            while at < base + inode_size + xattr_size {
                let name_len = b[at] as usize;
                let prefix = match b[at + 1] {
                    1 => "user.",
                    4 => "trusted.",
                    6 => "security.",
                    other => panic!("unexpected xattr prefix index {other}"),
                };
                let value_len = le16(b, at + 2) as usize;
                let name = std::str::from_utf8(&b[at + 4..at + 4 + name_len]).unwrap();
                let value = b[at + 4 + name_len..at + 4 + name_len + value_len].to_vec();
                xattrs.insert(format!("{prefix}{name}"), value);
                at += (4 + name_len + value_len).div_ceil(4) * 4;
            }
            let has_data = matches!(
                mode & mode::S_IFMT,
                mode::S_IFREG | mode::S_IFDIR | mode::S_IFLNK
            );
            let data = if has_data && size > 0 {
                let start = if inline {
                    base + inode_size + xattr_size
                } else {
                    i_u as usize * self.block_size
                };
                b[start..start + size as usize].to_vec()
            } else {
                Vec::new()
            };
            Inode {
                nid,
                mode,
                uid,
                gid,
                nlink,
                rdev: if has_data { 0 } else { i_u },
                xattrs,
                data,
            }
        }

        fn entries(&self, directory: &Inode) -> Vec<(Vec<u8>, u64)> {
            let mut entries = Vec::new();
            for block in directory.data.chunks(self.block_size) {
                let count = le16(block, 8) as usize / 12;
                for k in 0..count {
                    let start = le16(block, k * 12 + 8) as usize;
                    let end = if k + 1 < count {
                        le16(block, (k + 1) * 12 + 8) as usize
                    } else {
                        block.len()
                    };
                    let mut name = &block[start..end];
                    while name.last() == Some(&0) {
                        name = &name[..name.len() - 1];
                    }
                    entries.push((name.to_vec(), le64(block, k * 12)));
                }
            }
            entries
        }

        fn get(&self, path: &str) -> Option<Inode> {
            let mut inode = self.inode(self.root);
            for name in path.split('/').filter(|name| !name.is_empty()) {
                let nid = self
                    .entries(&inode)
                    .into_iter()
                    .find(|(entry, _)| entry == name.as_bytes())?
                    .1;
                inode = self.inode(nid);
            }
            Some(inode)
        }

        fn at(&self, path: &str) -> Inode {
            self.get(path)
                .unwrap_or_else(|| panic!("image has no {path}"))
        }

        fn json(&self, path: &str) -> serde_json::Value {
            serde_json::from_slice(&self.at(path).data).unwrap()
        }
    }

    enum Item<'a> {
        Dir(&'a str),
        File(&'a str, &'a [u8]),
        Hardlink(&'a str, &'a str),
        Symlink(&'a str, &'a str),
    }

    fn layer_tar(items: &[Item<'_>]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for item in items {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o4755);
            header.set_uid(1000);
            header.set_gid(1001);
            match *item {
                Item::Dir(path) => {
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_size(0);
                    builder.append_data(&mut header, path, io::empty()).unwrap();
                }
                Item::File(path, data) => {
                    header.set_size(data.len() as u64);
                    builder.append_data(&mut header, path, data).unwrap();
                }
                Item::Hardlink(path, target) => {
                    header.set_entry_type(tar::EntryType::Link);
                    header.set_size(0);
                    builder.append_link(&mut header, path, target).unwrap();
                }
                Item::Symlink(path, target) => {
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_size(0);
                    builder.append_link(&mut header, path, target).unwrap();
                }
            }
        }
        builder.into_inner().unwrap()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Level::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn add(builder: &mut tar::Builder<Vec<u8>>, path: &str, bytes: &[u8]) {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(bytes.len() as u64);
        builder.append_data(&mut header, path, bytes).unwrap();
    }

    fn blob_path(bytes: &[u8]) -> String {
        format!("blobs/sha256/{}", Sha256Digest::of(bytes).hex())
    }

    struct Fixture {
        layers: Vec<(Vec<u8>, Vec<u8>)>,
        config: Vec<u8>,
    }

    fn fixture() -> Fixture {
        let long = format!("srv/{}/file", "d".repeat(120));
        let big = vec![7u8; 10_000];
        let base = layer_tar(&[
            Item::Dir("./"),
            Item::Dir("etc/"),
            Item::File("etc/passwd", b"root"),
            Item::Dir("var/"),
            Item::Dir("var/cache/"),
            Item::File("var/cache/old", b"old"),
            Item::File(&long, b"long"),
            Item::File("usr/lib/big", &big),
            // Implicitly created by usr/lib/big, then given its metadata.
            Item::Dir("usr/"),
        ]);
        let top = layer_tar(&[
            Item::Dir("etc/"),
            Item::File("etc/.wh.passwd", b""),
            Item::File("var/cache/.wh..wh..opq", b""),
            Item::File("var/cache/new", b"new"),
            Item::Hardlink("var/cache/again", "var/cache/new"),
            Item::Symlink("bin", "/usr/bin"),
        ]);
        let layers: Vec<_> = [base, top]
            .into_iter()
            .map(|uncompressed| (gzip(&uncompressed), uncompressed))
            .collect();
        let diff_ids: Vec<_> = layers
            .iter()
            .map(|(_, uncompressed)| Sha256Digest::of(uncompressed).to_string())
            .collect();
        let config = serde_json::to_vec(&serde_json::json!({
            "created": "2026-11-06T00:00:00Z",
            "rootfs": {"type": "layers", "diff_ids": diff_ids},
        }))
        .unwrap();
        Fixture { layers, config }
    }

    /// A `docker save` archive from the containerd image store: the
    /// original manifest under a nested index, and manifest.json.
    fn oci_archive(fixture: &Fixture, directory: &Path) -> (PathBuf, Sha256Digest, Sha256Digest) {
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": Sha256Digest::of(&fixture.config), "size": fixture.config.len()},
            "layers": fixture.layers.iter().map(|(compressed, _)| serde_json::json!({
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": Sha256Digest::of(compressed),
                "size": compressed.len(),
            })).collect::<Vec<_>>(),
        }))
        .unwrap();
        let other_platform = Sha256Digest::of(b"absent");
        let nested = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "manifests": [
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": other_platform, "size": 6},
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": Sha256Digest::of(&manifest), "size": manifest.len()},
            ],
        }))
        .unwrap();
        let index = serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 2,
            "manifests": [{"mediaType": "application/vnd.oci.image.index.v1+json", "digest": Sha256Digest::of(&nested), "size": nested.len()}],
        }))
        .unwrap();
        let saved = serde_json::to_vec(&serde_json::json!([{
            "Config": blob_path(&fixture.config),
            "RepoTags": ["app:1", "registry.example.com:5000/team/app:2"],
            "Layers": fixture.layers.iter().map(|(compressed, _)| blob_path(compressed)).collect::<Vec<_>>(),
        }]))
        .unwrap();
        let mut builder = tar::Builder::new(Vec::new());
        for blob in [&manifest, &nested, &fixture.config]
            .into_iter()
            .chain(fixture.layers.iter().map(|(compressed, _)| compressed))
        {
            add(&mut builder, &blob_path(blob), blob);
        }
        add(&mut builder, "index.json", &index);
        add(&mut builder, "manifest.json", &saved);
        let path = directory.join("oci.tar");
        std::fs::write(&path, builder.into_inner().unwrap()).unwrap();
        (path, Sha256Digest::of(&nested), Sha256Digest::of(&manifest))
    }

    /// A `podman save --format docker-archive` archive: uncompressed layers
    /// and no original manifest. The second layer is a symlink to a copy.
    fn docker_archive(fixture: &Fixture, directory: &Path, tag: &str) -> PathBuf {
        let saved = serde_json::to_vec(&serde_json::json!([{
            "Config": "config.json",
            "RepoTags": [tag],
            "Layers": ["base/layer.tar", "top/layer.tar"],
        }]))
        .unwrap();
        let mut builder = tar::Builder::new(Vec::new());
        add(&mut builder, "config.json", &fixture.config);
        add(&mut builder, "base/layer.tar", &fixture.layers[0].1);
        add(&mut builder, "copy/layer.tar", &fixture.layers[1].1);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        builder
            .append_link(&mut header, "top/layer.tar", "../copy/layer.tar")
            .unwrap();
        add(&mut builder, "manifest.json", &saved);
        let path = directory.join(format!("docker-{}.tar", tag.replace([':', '/'], "-")));
        std::fs::write(&path, builder.into_inner().unwrap()).unwrap();
        path
    }

    fn convert_to_image(archives: &[&Path]) -> (Converted, ErofsImage) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("images.erofs");
        let converted = convert(archives, File::create(&path).unwrap()).unwrap();
        (converted, ErofsImage::new(std::fs::read(&path).unwrap()))
    }

    #[test]
    fn oci_archive_keeps_original_manifests_and_converts_whiteouts() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = fixture();
        let (archive, index_digest, manifest_digest) = oci_archive(&fixture, directory.path());
        let (converted, image) = convert_to_image(&[&archive]);
        assert_eq!(
            converted,
            Converted {
                images: 1,
                layers: 2
            }
        );

        let layers = image.json("overlay-layers/layers.json");
        let base_id = layers[0]["id"].as_str().unwrap().to_owned();
        let top_id = layers[1]["id"].as_str().unwrap().to_owned();
        assert_eq!(
            format!("sha256:{base_id}"),
            Sha256Digest::of(&fixture.layers[0].1).to_string()
        );
        assert_eq!(layers[1]["parent"], base_id.as_str());
        assert_eq!(layers[1]["compression"], 2);
        assert_eq!(
            layers[1]["compressed-diff-digest"],
            Sha256Digest::of(&fixture.layers[1].0).to_string()
        );
        assert_eq!(layers[0]["uidset"], serde_json::json!([1000]));

        let base = format!("overlay/{base_id}/diff");
        let top = format!("overlay/{top_id}/diff");
        let passwd = image.at(&format!("{base}/etc/passwd"));
        assert_eq!(passwd.data, b"root");
        assert_eq!(passwd.mode & 0o7777, 0o4755);
        assert_eq!((passwd.uid, passwd.gid), (1000, 1001));
        assert_eq!(
            image.at(&format!("{base}/usr/lib/big")).data,
            vec![7u8; 10_000]
        );
        assert_eq!(image.at(&format!("{base}/usr")).uid, 1000);
        let long = format!("{base}/srv/{}/file", "d".repeat(120));
        assert_eq!(image.at(&long).data, b"long");

        let whiteout = image.at(&format!("{top}/etc/passwd"));
        assert_eq!(whiteout.kind(), mode::S_IFCHR);
        assert_eq!(whiteout.rdev, 0);
        assert!(image.get(&format!("{top}/etc/.wh.passwd")).is_none());
        let cache = image.at(&format!("{top}/var/cache"));
        assert_eq!(cache.xattrs[OPAQUE_XATTR], b"y");
        assert!(
            image
                .get(&format!("{top}/var/cache/.wh..wh..opq"))
                .is_none()
        );
        let new = image.at(&format!("{top}/var/cache/new"));
        let again = image.at(&format!("{top}/var/cache/again"));
        assert_eq!(new.nid, again.nid);
        assert_eq!(new.nlink, 2);
        assert_eq!(again.data, b"new");
        let bin = image.at(&format!("{top}/bin"));
        assert_eq!(bin.kind(), mode::S_IFLNK);
        assert_eq!(bin.data, b"/usr/bin");

        let base_link =
            String::from_utf8(image.at(&format!("overlay/{base_id}/link")).data).unwrap();
        let top_link = String::from_utf8(image.at(&format!("overlay/{top_id}/link")).data).unwrap();
        assert_eq!(base_link.len(), 26);
        assert_eq!(
            image.at(&format!("overlay/{top_id}/lower")).data,
            format!("l/{base_link}").into_bytes()
        );
        assert_eq!(
            image.at(&format!("overlay/l/{top_link}")).data,
            format!("../{top_id}/diff").into_bytes()
        );
        assert!(image.get(&format!("overlay/{base_id}/empty")).is_some());
        assert!(image.get(&format!("overlay/{top_id}/empty")).is_none());
        assert!(image.get("overlay-layers/layers.lock").is_some());
        assert!(image.get("overlay-images/images.lock").is_some());

        let images = image.json("overlay-images/images.json");
        let record = &images[0];
        let config_id = Sha256Digest::of(&fixture.config);
        assert_eq!(record["id"], config_id.hex());
        assert_eq!(record["layer"], top_id.as_str());
        assert_eq!(record["digest"], index_digest.to_string());
        assert_eq!(
            record["names"],
            serde_json::json!([
                "docker.io/library/app:1",
                "registry.example.com:5000/team/app:2"
            ])
        );
        let manifest_key = format!("manifest-{manifest_digest}");
        assert_eq!(
            record["big-data-digests"][&manifest_key],
            manifest_digest.to_string()
        );
        assert!(record["big-data-sizes"][format!("manifest-{index_digest}")].is_u64());
        let images_dir = format!("overlay-images/{}", config_id.hex());
        let manifest = image.at(&format!(
            "{images_dir}/{}",
            big_data_file_name(&manifest_key)
        ));
        assert_eq!(Sha256Digest::of(&manifest.data), manifest_digest);
        assert_eq!(
            image.at(&format!("{images_dir}/manifest")).data,
            manifest.data
        );
    }

    #[test]
    fn docker_archive_gets_a_synthesized_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = fixture();
        let archive = docker_archive(&fixture, directory.path(), "localhost/app:dev");
        let (_, image) = convert_to_image(&[&archive]);
        let layers = image.json("overlay-layers/layers.json");
        assert_eq!(layers[1]["compression"], 0);
        assert_eq!(
            layers[1]["compressed-diff-digest"],
            layers[1]["diff-digest"]
        );
        let images = image.json("overlay-images/images.json");
        assert_eq!(images[0]["names"], serde_json::json!(["localhost/app:dev"]));
        let id = images[0]["id"].as_str().unwrap();
        let manifest = image.at(&format!("overlay-images/{id}/manifest")).data;
        let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(parsed["layers"][1]["digest"], layers[1]["diff-digest"]);
        assert_eq!(images[0]["digest"], Sha256Digest::of(&manifest).to_string());
    }

    #[test]
    fn several_archives_share_one_image() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = fixture();
        let (oci, _, _) = oci_archive(&fixture, directory.path());
        let tagged = docker_archive(&fixture, directory.path(), "localhost/app:dev");
        let mut other = fixture;
        other.config = serde_json::to_vec(&serde_json::json!({
            "created": "2026-11-07T00:00:00Z",
            "rootfs": {"type": "layers", "diff_ids": [Sha256Digest::of(&other.layers[0].1)]},
        }))
        .unwrap();
        other.layers.truncate(1);
        let saved = serde_json::to_vec(&serde_json::json!([{
            "Config": "config.json",
            "RepoTags": ["base:1"],
            "Layers": ["base/layer.tar"],
        }]))
        .unwrap();
        let mut builder = tar::Builder::new(Vec::new());
        add(&mut builder, "config.json", &other.config);
        add(&mut builder, "base/layer.tar", &other.layers[0].1);
        add(&mut builder, "manifest.json", &saved);
        let base = directory.path().join("base.tar");
        std::fs::write(&base, builder.into_inner().unwrap()).unwrap();

        let (converted, image) = convert_to_image(&[&oci, &tagged, &base]);
        // The same image twice, and a second image on the shared base layer.
        assert_eq!(
            converted,
            Converted {
                images: 2,
                layers: 2
            }
        );
        let images = image.json("overlay-images/images.json");
        assert_eq!(
            images[0]["names"],
            serde_json::json!([
                "docker.io/library/app:1",
                "localhost/app:dev",
                "registry.example.com:5000/team/app:2"
            ])
        );
        assert_eq!(
            images[1]["names"],
            serde_json::json!(["docker.io/library/base:1"])
        );
        assert_eq!(
            images[1]["layer"],
            image.json("overlay-layers/layers.json")[0]["id"]
        );
    }

    #[test]
    fn corrupt_layer_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut fixture = fixture();
        fixture.config = String::from_utf8(fixture.config)
            .unwrap()
            .replace(
                Sha256Digest::of(&fixture.layers[1].1).hex(),
                &"0".repeat(64),
            )
            .into_bytes();
        let archive = docker_archive(&fixture, directory.path(), "app:bad");
        let output = File::create(directory.path().join("images.erofs")).unwrap();
        let error = convert(&[&archive], output).unwrap_err();
        assert!(format!("{error:#}").contains("but its image config says"));
    }

    #[test]
    fn escaping_paths_are_rejected() {
        assert!(clean(b"../etc/passwd").is_err());
        assert!(clean(b"a/../../b").is_err());
        assert_eq!(clean(b"./a//b/").unwrap(), b"a/b");
        assert_eq!(clean(b"/").unwrap(), b"");
    }

    #[test]
    fn devices_use_the_linux_encoding() {
        assert_eq!(encode_device(0, 0), 0);
        // /dev/null is 1:3; a minor above 255 moves to the high bits.
        assert_eq!(encode_device(1, 3), 0x103);
        assert_eq!(encode_device(8, 0x123), 0x0010_0823);
    }

    #[test]
    fn big_data_names_match_containers_storage() {
        assert_eq!(big_data_file_name("manifest"), "manifest");
        // Observed in a store that Podman 5.8.2 wrote.
        assert_eq!(
            big_data_file_name(
                "sha256:38e546db1e72f1b34dc8dabf92d772983012dbcd8046cc41e2a445a4e1ec412f"
            ),
            "=c2hhMjU2OjM4ZTU0NmRiMWU3MmYxYjM0ZGM4ZGFiZjkyZDc3Mjk4MzAxMmRiY2Q4MDQ2Y2M0MWUyYTQ0NWE0ZTFlYzQxMmY="
        );
    }

    #[test]
    fn short_links_are_base32_like_podman() {
        let link = short_link("abc");
        assert_eq!(link.len(), 26);
        assert!(
            link.bytes()
                .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
        );
    }
}
