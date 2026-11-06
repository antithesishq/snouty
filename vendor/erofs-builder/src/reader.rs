//! Async reader for images produced by this crate's [`Writer`](crate::Writer).
//!
//! Only the feature subset the writer emits is supported: uncompressed
//! flat/inline data layouts, no xattrs, no chunked or compressed files.

use std::io;
use std::path::Path;

use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, SeekFrom};

use crate::layout::{
    mode, DataLayout, FileType, EROFS_I_DATALAYOUT_BIT, EROFS_I_VERSION_BIT, EROFS_SUPER_MAGIC_V1,
    EROFS_SUPER_OFFSET, SIZE_DIRENT, SIZE_INODE_COMPACT, SIZE_INODE_EXTENDED,
};

#[derive(Debug)]
pub struct Stat {
    pub kind: FileType,
    /// Full mode, including the type bits.
    pub mode: u16,
    pub size: u64,
}

#[derive(Debug)]
pub struct Dirent {
    pub name: String,
    pub nid: u64,
    pub kind: FileType,
}

struct InodeView {
    kind: FileType,
    mode: u16,
    size: u64,
    layout: DataLayout,
    /// FlatPlain: first block of the out-of-line data.
    blkaddr: u32,
    /// Absolute image offset of the inline trailing data.
    trailing_off: u64,
}

pub struct Reader {
    file: tokio::fs::File,
    bs: usize,
    meta_off: u64,
    root_nid: u64,
}

fn le16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}

fn le32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn le64(b: &[u8], o: usize) -> u64 {
    let mut v = [0u8; 8];
    v.copy_from_slice(&b[o..o + 8]);
    u64::from_le_bytes(v)
}

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

impl Reader {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Reader> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut sb = vec![0u8; 128];
        file.seek(SeekFrom::Start(EROFS_SUPER_OFFSET as u64))
            .await?;
        file.read_exact(&mut sb).await?;
        if le32(&sb, 0) != EROFS_SUPER_MAGIC_V1 {
            return Err(bad("not an erofs image: bad magic"));
        }
        let bits = sb[12] as usize;
        if !(9..=16).contains(&bits) {
            return Err(bad(format!("invalid block bits {bits}")));
        }
        let root_nid = le16(&sb, 14) as u64;
        let meta_blkaddr = le32(&sb, 40);
        Ok(Reader {
            file,
            bs: 1usize << bits,
            meta_off: meta_blkaddr as u64 * (1usize << bits) as u64,
            root_nid,
        })
    }

    pub fn root_nid(&self) -> u64 {
        self.root_nid
    }

    async fn read_exact_at(&mut self, off: u64, buf: &mut [u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(off)).await?;
        self.file.read_exact(buf).await?;
        Ok(())
    }

    async fn inode(&mut self, nid: u64) -> io::Result<InodeView> {
        let base = self.meta_off + nid * SIZE_INODE_COMPACT as u64;
        let mut head = [0u8; SIZE_INODE_COMPACT];
        self.read_exact_at(base, &mut head).await?;
        let format = le16(&head, 0);
        let extended = format & (1 << EROFS_I_VERSION_BIT) != 0;
        let inode_size = if extended {
            SIZE_INODE_EXTENDED
        } else {
            SIZE_INODE_COMPACT
        };
        let mut buf = head.to_vec();
        if extended {
            buf.resize(SIZE_INODE_EXTENDED, 0);
            self.file.read_exact(&mut buf[SIZE_INODE_COMPACT..]).await?;
        }
        if le16(&buf, 2) != 0 {
            return Err(bad(format!("inode {nid}: xattrs unsupported")));
        }
        let layout = DataLayout::from_u8(((format >> EROFS_I_DATALAYOUT_BIT) & 0x7) as u8)
            .ok_or_else(|| bad(format!("inode {nid}: unknown data layout")))?;
        if !matches!(layout, DataLayout::FlatPlain | DataLayout::FlatInline) {
            return Err(bad(format!(
                "inode {nid}: unsupported data layout {layout:?}"
            )));
        }
        let mode_ = le16(&buf, 4);
        let kind = match mode::file_type(mode_) {
            mode::S_IFREG => FileType::RegFile,
            mode::S_IFDIR => FileType::Dir,
            mode::S_IFLNK => FileType::Symlink,
            _ => return Err(bad(format!("inode {nid}: unsupported file type"))),
        };
        let size = if extended {
            le64(&buf, 8)
        } else {
            le32(&buf, 8) as u64
        };
        Ok(InodeView {
            kind,
            mode: mode_,
            size,
            layout,
            blkaddr: le32(&buf, 16),
            trailing_off: base + inode_size as u64,
        })
    }

    pub async fn stat(&mut self, nid: u64) -> io::Result<Stat> {
        let v = self.inode(nid).await?;
        Ok(Stat {
            kind: v.kind,
            mode: v.mode,
            size: v.size,
        })
    }

    /// Lists a directory's entries as stored, `.`/`..` omitted. The writer
    /// emits them name-sorted; callers needing a specific order must sort.
    pub async fn dirents(&mut self, nid: u64) -> io::Result<Vec<Dirent>> {
        let v = self.inode(nid).await?;
        if v.kind != FileType::Dir {
            return Err(bad(format!("inode {nid}: not a directory")));
        }
        let start = if v.layout == DataLayout::FlatInline {
            v.trailing_off
        } else {
            v.blkaddr as u64 * self.bs as u64
        };
        let mut blob = vec![0u8; v.size as usize];
        self.read_exact_at(start, &mut blob).await?;
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < blob.len() {
            let end = (off + self.bs).min(blob.len());
            parse_dirent_chunk(&blob[off..end], &mut out)?;
            off = end;
        }
        Ok(out)
    }

    /// Streams the content of a regular file or the target of a symlink,
    /// invoking `f` per chunk. Returns the number of bytes passed.
    pub async fn read_content(
        &mut self,
        nid: u64,
        f: &mut (dyn FnMut(&[u8]) + Send),
    ) -> io::Result<u64> {
        let v = self.inode(nid).await?;
        if !matches!(v.kind, FileType::RegFile | FileType::Symlink) {
            return Err(bad(format!("inode {nid}: not a regular file or symlink")));
        }
        if v.size == 0 {
            return Ok(0);
        }
        let start = if v.layout == DataLayout::FlatInline {
            v.trailing_off
        } else {
            v.blkaddr as u64 * self.bs as u64
        };
        let mut remaining = v.size;
        self.file.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0u8; 64 << 10];
        while remaining > 0 {
            let n = buf.len().min(remaining as usize);
            self.file.read_exact(&mut buf[..n]).await?;
            f(&buf[..n]);
            remaining -= n as u64;
        }
        Ok(v.size)
    }
}

/// Parses one block-sized chunk of a dirent blob. Each chunk holds a run
/// of 12-byte records (nid, nameoff, file type, pad) followed by the names
/// packed back to back; the first record's nameoff gives the run length.
fn parse_dirent_chunk(chunk: &[u8], out: &mut Vec<Dirent>) -> io::Result<()> {
    if chunk.len() < SIZE_DIRENT {
        return Err(bad("dirent chunk too small"));
    }
    let count = le16(chunk, 8) as usize / SIZE_DIRENT;
    if count == 0 || count * SIZE_DIRENT > chunk.len() {
        return Err(bad("dirent chunk: bad nameoff"));
    }
    for k in 0..count {
        let rec = k * SIZE_DIRENT;
        let nameoff = le16(chunk, rec + 8) as usize;
        let start = nameoff;
        let end = if k + 1 < count {
            le16(chunk, rec + SIZE_DIRENT + 8) as usize
        } else {
            chunk.len()
        };
        if start < count * SIZE_DIRENT || end > chunk.len() || start > end {
            return Err(bad("dirent chunk: bad name offsets"));
        }
        let mut name = &chunk[start..end];
        if k + 1 == count {
            while name.last() == Some(&0) {
                name = &name[..name.len() - 1];
            }
        }
        let name = std::str::from_utf8(name)
            .map_err(|_| bad("dirent chunk: non-utf8 name"))?
            .to_string();
        if name == "." || name == ".." {
            continue;
        }
        let kind = FileType::from_u8(chunk[rec + 10])
            .ok_or_else(|| bad("dirent chunk: unknown file type"))?;
        out.push(Dirent {
            name,
            nid: le64(chunk, rec),
            kind,
        });
    }
    Ok(())
}
