//! End-to-end: build an image with Writer into an in-memory sink, then
//! walk the on-disk structures to verify layout and content.

use std::collections::HashMap;
use std::io::Cursor;

use erofs_builder::layout::mode;
use erofs_builder::layout::{EROFS_SUPER_MAGIC_V1, EROFS_SUPER_OFFSET};
use erofs_builder::{CreateOptions, InodeMeta, Writer};

const BS: usize = 4096;

fn le16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn le32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn le64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes([
        b[o],
        b[o + 1],
        b[o + 2],
        b[o + 3],
        b[o + 4],
        b[o + 5],
        b[o + 6],
        b[o + 7],
    ])
}

struct Inode {
    layout: u8,
    mode: u16,
    size: u64,
    i_u: u32,
    /// Offset of inline trailing data (after inode + xattrs).
    trailing_off: usize,
}

fn read_inode(img: &[u8], meta_off: usize, nid: u64) -> Inode {
    let base = meta_off + nid as usize * 32;
    assert!(base + 64 <= img.len(), "inode beyond image end");
    let format = le16(img, base);
    let compact = format & 1 == 0;
    let inode_size = if compact { 32 } else { 64 };
    assert_eq!(le16(img, base + 2), 0, "no xattrs expected in tests");
    Inode {
        layout: ((format >> 1) & 0x7) as u8,
        mode: le16(img, base + 4),
        size: if compact {
            le32(img, base + 8) as u64
        } else {
            le64(img, base + 8)
        },
        i_u: le32(img, base + 16),
        trailing_off: base + inode_size,
    }
}

/// Parse a dirent blob; names are contiguous, each bounded by the next
/// entry's nameoff (the last by a NUL within `limit`).
fn parse_dirents(img: &[u8], off: usize, limit: usize) -> HashMap<String, u64> {
    let count = le16(img, off + 8) as usize / 12;
    let mut out = HashMap::new();
    for i in 0..count {
        let e = off + i * 12;
        let nid = le64(img, e);
        let nameoff = le16(img, e + 8) as usize;
        let start = off + nameoff;
        let end = if i + 1 < count {
            off + le16(img, off + (i + 1) * 12 + 8) as usize
        } else {
            let mut end = start;
            while end < limit && img[end] != 0 {
                end += 1;
            }
            end
        };
        let name = String::from_utf8(img[start..end].to_vec()).unwrap();
        out.insert(name, nid);
    }
    out
}

fn dirent_region(img: &[u8], ino: &Inode) -> (usize, usize) {
    if ino.layout == 2 {
        (ino.trailing_off, img.len())
    } else {
        let off = ino.i_u as usize * BS;
        (off, off + BS)
    }
}

fn content_region<'a>(img: &'a [u8], ino: &Inode) -> &'a [u8] {
    let off = if ino.layout == 0 {
        ino.i_u as usize * BS
    } else {
        ino.trailing_off
    };
    &img[off..off + ino.size as usize]
}

#[tokio::test]
async fn builds_valid_image() {
    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let block = vec![7u8; BS];
    let almost = vec![9u8; BS - 2]; // can never be inlined -> fallback pass
    let small = b"hello".to_vec();

    let mut w = Writer::new(Cursor::new(Vec::new()), CreateOptions::default())
        .await
        .unwrap();
    let mut c = Cursor::new(big.clone());
    w.add_file("/big.bin", InodeMeta::reg(0o644), big.len() as u64, &mut c)
        .await
        .unwrap();
    let mut c = Cursor::new(block.clone());
    w.add_file("/block.bin", InodeMeta::reg(0o644), BS as u64, &mut c)
        .await
        .unwrap();
    let mut c = Cursor::new(almost.clone());
    w.add_file(
        "/almost.bin",
        InodeMeta::reg(0o644),
        almost.len() as u64,
        &mut c,
    )
    .await
    .unwrap();
    let mut c = Cursor::new(small.clone());
    w.add_file(
        "/etc/motd",
        InodeMeta::reg(0o644),
        small.len() as u64,
        &mut c,
    )
    .await
    .unwrap();
    w.symlink("/link", b"etc/motd", InodeMeta::symlink())
        .await
        .unwrap();
    w.mkdir("/d", InodeMeta::dir(0o755)).await.unwrap();
    let mut e = tokio::io::empty();
    w.add_file("/empty", InodeMeta::reg(0o644), 0, &mut e)
        .await
        .unwrap();
    let img = w.finish().await.unwrap().into_inner();

    // Superblock.
    let sb = EROFS_SUPER_OFFSET;
    assert_eq!(&img[sb..sb + 4], &EROFS_SUPER_MAGIC_V1.to_le_bytes());
    assert_eq!(img[sb + 12], 12, "blkszbits");
    assert_eq!(le16(&img, sb + 14), 0, "root nid");
    let blocks = le32(&img, sb + 36) as usize;
    let meta_blkaddr = le32(&img, sb + 40) as usize;
    assert_eq!(img.len(), blocks * BS, "image size matches block count");
    let meta_off = meta_blkaddr * BS;
    assert!(meta_off >= BS, "metadata starts after the sb block");

    // Root directory.
    let root = read_inode(&img, meta_off, 0);
    assert_eq!(root.mode & mode::S_IFMT, mode::S_IFDIR);
    let (off, limit) = dirent_region(&img, &root);
    let entries = parse_dirents(&img, off, limit);
    assert_eq!(entries.len(), 9, "., .. and seven children");
    assert!(entries.contains_key(".") && entries.contains_key(".."));

    let check_file = |name: &str, expected: &[u8]| {
        let ino = read_inode(&img, meta_off, entries[name]);
        assert_eq!(ino.mode & mode::S_IFMT, mode::S_IFREG);
        assert_eq!(ino.size, expected.len() as u64);
        assert_eq!(content_region(&img, &ino), expected, "{name} content");
    };
    check_file("big.bin", &big);
    check_file("block.bin", &block);
    check_file("almost.bin", &almost);
    check_file("empty", b"");

    // Nested dir /etc with motd.
    let etc = read_inode(&img, meta_off, entries["etc"]);
    assert_eq!(etc.mode & mode::S_IFMT, mode::S_IFDIR);
    let (off, limit) = dirent_region(&img, &etc);
    let etc_entries = parse_dirents(&img, off, limit);
    let motd = read_inode(&img, meta_off, etc_entries["motd"]);
    assert_eq!(content_region(&img, &motd), &small[..]);

    // Symlink.
    let link = read_inode(&img, meta_off, entries["link"]);
    assert_eq!(link.mode & mode::S_IFMT, mode::S_IFLNK);
    assert_eq!(link.size, 8);
    assert_eq!(content_region(&img, &link), b"etc/motd");

    // Empty dir.
    let d = read_inode(&img, meta_off, entries["d"]);
    assert_eq!(d.mode & mode::S_IFMT, mode::S_IFDIR);
    let (off, limit) = dirent_region(&img, &d);
    let d_entries = parse_dirents(&img, off, limit);
    assert_eq!(d_entries.len(), 2, "only . and ..");
}

#[tokio::test]
async fn short_read_is_rejected() {
    let mut w = Writer::new(Cursor::new(Vec::new()), CreateOptions::default())
        .await
        .unwrap();
    let short = vec![1u8; 100];
    let mut c = Cursor::new(short);
    let err = w
        .add_file("/file.bin", InodeMeta::reg(0o644), 200, &mut c)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn checksum_feature_bit() {
    let mut w = Writer::new(
        Cursor::new(Vec::new()),
        CreateOptions {
            checksum: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut c = Cursor::new(vec![42u8; 8192]);
    w.add_file("/f", InodeMeta::reg(0o644), 8192, &mut c)
        .await
        .unwrap();
    let img = w.finish().await.unwrap().into_inner();

    let sb = EROFS_SUPER_OFFSET;
    assert_eq!(
        le32(&img, sb + 8),
        erofs_builder::layout::EROFS_FEATURE_COMPAT_SB_CHKSUM
    );
    assert_ne!(le32(&img, sb + 4), 0, "crc32c field is set");
}

#[tokio::test]
async fn empty_image_is_valid() {
    let w = Writer::new(Cursor::new(Vec::new()), CreateOptions::default())
        .await
        .unwrap();
    let img = w.finish().await.unwrap().into_inner();
    let sb = EROFS_SUPER_OFFSET;
    assert_eq!(&img[sb..sb + 4], &EROFS_SUPER_MAGIC_V1.to_le_bytes());
    let blocks = le32(&img, sb + 36) as usize;
    let meta_blkaddr = le32(&img, sb + 40) as usize;
    assert_eq!(img.len(), blocks * BS);
    let root = read_inode(&img, meta_blkaddr * BS, 0);
    let (off, limit) = dirent_region(&img, &root);
    assert_eq!(parse_dirents(&img, off, limit).len(), 2);
}
