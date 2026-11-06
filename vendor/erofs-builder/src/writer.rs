//! The EROFS image writer: incremental single-pass streaming layout.

use std::collections::{HashMap, HashSet};
use std::io;

use crate::layout::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

pub const DEFAULT_BLOCK_SIZE: usize = 4096;
pub const MIN_BLOCK_SIZE: usize = 512;
pub const MAX_BLOCK_SIZE: usize = 1 << 16;

const ZEROS: [u8; MAX_BLOCK_SIZE] = [0u8; MAX_BLOCK_SIZE];

/// Metadata for a filesystem entry, mirroring `stat` plus EROFS extras.
#[derive(Clone, Debug)]
pub struct InodeMeta {
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    /// Seconds since the epoch. If this equals the image build time and
    /// other constraints hold, a compact 32-byte inode is emitted.
    pub mtime: u64,
    pub mtime_nsec: u32,
    /// Explicit nlink; when `None` it is computed (dirs: 2 + subdirs).
    pub nlink: Option<u32>,
    pub rdev: u32,
    /// Extended attributes as full "prefix.name" → value.
    pub xattrs: std::collections::BTreeMap<String, Vec<u8>>,
}

impl Default for InodeMeta {
    fn default() -> Self {
        Self {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime: 0,
            mtime_nsec: 0,
            nlink: None,
            rdev: 0,
            xattrs: Default::default(),
        }
    }
}

impl InodeMeta {
    pub fn dir(mode: u16) -> Self {
        Self {
            mode: mode::S_IFDIR | (mode & !mode::S_IFMT),
            ..Default::default()
        }
    }
    pub fn reg(mode: u16) -> Self {
        Self {
            mode: mode::S_IFREG | (mode & !mode::S_IFMT),
            ..Default::default()
        }
    }
    pub fn symlink() -> Self {
        Self {
            mode: mode::S_IFLNK | 0o777,
            ..Default::default()
        }
    }
}

enum Content {
    Empty,
    /// Held in memory because it may still qualify for inline storage.
    Inline(Vec<u8>),
    /// Already written to the sink as block-aligned data; the start block
    /// is kept in `Node::data_blkaddr`.
    Streamed {
        len: u64,
    },
}

struct Node {
    name: String,
    meta: InodeMeta,
    children: Vec<Node>,
    content: Content,
    link_target: Vec<u8>,
    nid: u64,
    parent_nid: u64,
    layout: DataLayout,
    compact: bool,
    xattr_size: usize,
    trailing_size: usize,
    data_blkaddr: u32,
    file_type: FileType,
}

impl Node {
    fn new(name: String, meta: InodeMeta) -> Self {
        let file_type = match mode::file_type(meta.mode) {
            mode::S_IFREG => FileType::RegFile,
            mode::S_IFDIR => FileType::Dir,
            mode::S_IFCHR => FileType::Chrdev,
            mode::S_IFBLK => FileType::Blkdev,
            mode::S_IFIFO => FileType::Fifo,
            mode::S_IFSOCK => FileType::Sock,
            mode::S_IFLNK => FileType::Symlink,
            _ => FileType::Unknown,
        };
        Self {
            name,
            meta,
            children: Vec::new(),
            content: Content::Empty,
            link_target: Vec::new(),
            nid: 0,
            parent_nid: 0,
            layout: DataLayout::FlatPlain,
            compact: false,
            xattr_size: 0,
            trailing_size: 0,
            data_blkaddr: 0,
            file_type,
        }
    }

    fn is_dir(&self) -> bool {
        self.file_type == FileType::Dir
    }

    fn effective_nlink(&self) -> u32 {
        if let Some(n) = self.meta.nlink {
            return n;
        }
        if self.is_dir() {
            2 + self.children.iter().filter(|c| c.is_dir()).count() as u32
        } else {
            1
        }
    }

    fn on_disk_size(&self) -> u64 {
        match self.file_type {
            FileType::RegFile | FileType::Unknown => match self.content {
                Content::Empty => 0,
                Content::Inline(ref d) => d.len() as u64,
                Content::Streamed { len } => len,
            },
            FileType::Symlink => self.link_target.len() as u64,
            _ => 0,
        }
    }
}

fn split_xattr_name(name: &str) -> (u8, &str) {
    const PREFIXES: [(&str, u8); 6] = [
        ("user.", xattr_index::USER),
        ("system.posix_acl_access.", xattr_index::POSIX_ACL_ACCESS),
        ("system.posix_acl_default.", xattr_index::POSIX_ACL_DEFAULT),
        ("trusted.", xattr_index::TRUSTED),
        ("lustre.", xattr_index::LUSTRE),
        ("security.", xattr_index::SECURITY),
    ];
    for (p, idx) in PREFIXES {
        if let Some(rest) = name.strip_prefix(p) {
            return (idx, rest);
        }
    }
    (0, name)
}

fn calc_xattr_size(xattrs: &std::collections::BTreeMap<String, Vec<u8>>) -> usize {
    if xattrs.is_empty() {
        return 0;
    }
    let mut entries = SIZE_XATTR_ENTRY;
    for (name, value) in xattrs {
        let (_, suffix) = split_xattr_name(name);
        entries += SIZE_XATTR_ENTRY + suffix.len() + value.len();
        entries = round_up(entries, 4);
    }
    SIZE_XATTR_IBODY_HEADER + entries - SIZE_XATTR_ENTRY
}

fn xattr_count(xattr_size: usize) -> u16 {
    if xattr_size == 0 {
        0
    } else {
        ((xattr_size - SIZE_XATTR_IBODY_HEADER) / SIZE_XATTR_ENTRY + 1) as u16
    }
}

fn round_up(v: usize, align: usize) -> usize {
    v.div_ceil(align) * align
}

fn blk_bits(block_size: usize) -> u8 {
    block_size.trailing_zeros() as u8
}

/// Options for image creation.
#[derive(Clone, Debug)]
pub struct CreateOptions {
    pub block_size: usize,
    pub build_time: u64,
    pub build_time_nsec: u32,
    pub uuid: [u8; 16],
    pub volume_name: String,
    /// Emit a crc32c superblock checksum (SB_CHKSUM compat feature).
    pub checksum: bool,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            build_time: 0,
            build_time_nsec: 0,
            uuid: [0; 16],
            volume_name: String::new(),
            checksum: false,
        }
    }
}

/// Single-pass streaming EROFS image writer.
///
/// File data is written to the sink as it is added; [`Writer::finish`]
/// appends the metadata area and patches the superblock in place via a
/// seek back to offset 0, so the sink must implement both
/// [`tokio::io::AsyncWrite`] and [`tokio::io::AsyncSeek`] (e.g.
/// [`tokio::fs::File`], `tokio::io::BufWriter<File>`, or
/// `std::io::Cursor<Vec<u8>>` for in-memory images).
///
/// Regular-file content is pushed with a declared size, which keeps memory
/// bounded and allows sources that can only be read once (such as a tar
/// archive being consumed as it streams past) to be packed in a single
/// pass. Files no larger than the block size are buffered in memory since
/// they may end up inlined into their inode.
pub struct Writer<W> {
    sink: W,
    root: Node,
    build_time: u64,
    build_time_nsec: u32,
    block_size: usize,
    uuid: [u8; 16],
    volume_name: String,
    checksum: bool,
    paths: HashSet<String>,
    /// Next free block in the data area (absolute block index).
    data_end: u32,
}

impl<W> Writer<W>
where
    W: AsyncWrite + AsyncSeek + Unpin + Send,
{
    pub async fn new(sink: W, opts: CreateOptions) -> io::Result<Self> {
        if opts.block_size < MIN_BLOCK_SIZE
            || opts.block_size > MAX_BLOCK_SIZE
            || !opts.block_size.is_power_of_two()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid block size {}: must be a power of two between {} and {}",
                    opts.block_size, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE
                ),
            ));
        }
        if opts.volume_name.len() > 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "volume name longer than 16 bytes",
            ));
        }
        let mut sink = sink;
        let sb_area_bytes = round_up(EROFS_SUPER_OFFSET + SIZE_SUPER_BLOCK, opts.block_size);
        sink.write_all(&vec![0u8; sb_area_bytes]).await?;
        Ok(Self {
            sink,
            root: Node::new(String::new(), InodeMeta::dir(0o755)),
            build_time: opts.build_time,
            build_time_nsec: opts.build_time_nsec,
            block_size: opts.block_size,
            uuid: opts.uuid,
            volume_name: opts.volume_name,
            checksum: opts.checksum,
            paths: ["/".to_string()].into_iter().collect(),
            data_end: (sb_area_bytes / opts.block_size) as u32,
        })
    }

    /// Add a regular file, reading exactly `size` bytes from `data` and
    /// writing them to the sink immediately. Errors if `data` runs short.
    pub async fn add_file<R>(
        &mut self,
        path: &str,
        meta: InodeMeta,
        size: u64,
        data: &mut R,
    ) -> io::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let clean = clean(path);
        if clean.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid path"));
        }
        ensure_dir(&mut self.root, &mut self.paths, &clean)?;
        let mut node = Node::new(String::new(), meta);
        node.content = if size == 0 {
            Content::Empty
        } else if size <= self.block_size as u64 {
            let mut buf = vec![0u8; size as usize];
            data.read_exact(&mut buf).await?;
            Content::Inline(buf)
        } else {
            node.data_blkaddr = self.stream_data(size, data).await?;
            Content::Streamed { len: size }
        };
        insert(&mut self.root, &mut self.paths, &clean, node)
    }

    async fn stream_data<R>(&mut self, size: u64, data: &mut R) -> io::Result<u32>
    where
        R: AsyncRead + Unpin,
    {
        let bs = self.block_size as u64;
        let blocks = size.div_ceil(bs);
        if self.data_end as u64 + blocks > u32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "image too large",
            ));
        }
        let start = self.data_end;
        let start_pos = start as u64 * self.block_size as u64;

        let res = async {
            let mut chunk = vec![0u8; 64 * 1024];
            let mut remaining = size;
            while remaining > 0 {
                let n = chunk.len().min(remaining as usize);
                data.read_exact(&mut chunk[..n]).await?;
                self.sink.write_all(&chunk[..n]).await?;
                remaining -= n as u64;
            }
            Ok::<(), io::Error>(())
        }
        .await;

        match res {
            Ok(()) => {
                let pad = (blocks * bs - size) as usize;
                if pad > 0 {
                    self.sink.write_all(&ZEROS[..pad]).await?;
                }
                self.data_end = start + blocks as u32;
                Ok(start)
            }
            Err(e) => {
                // Rewind so a subsequent add_file overwrites the partial data.
                let _ = self.sink.seek(io::SeekFrom::Start(start_pos)).await;
                Err(e)
            }
        }
    }

    /// Add a directory. Intermediate directories are created implicitly.
    pub async fn mkdir(&mut self, path: &str, meta: InodeMeta) -> io::Result<()> {
        let clean = clean(path);
        if clean.is_empty() {
            self.root.meta = meta;
            return Ok(());
        }
        ensure_dir(&mut self.root, &mut self.paths, &clean)?;
        insert(
            &mut self.root,
            &mut self.paths,
            &clean,
            Node::new(String::new(), meta),
        )
    }

    /// Add a symlink.
    pub async fn symlink(&mut self, path: &str, target: &[u8], meta: InodeMeta) -> io::Result<()> {
        let clean = clean(path);
        if clean.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid path"));
        }
        ensure_dir(&mut self.root, &mut self.paths, &clean)?;
        let mut node = Node::new(String::new(), meta);
        node.link_target = target.to_vec();
        insert(&mut self.root, &mut self.paths, &clean, node)
    }

    /// Add a device node, FIFO or socket (`meta.mode` carries the type).
    pub async fn mknod(&mut self, path: &str, meta: InodeMeta) -> io::Result<()> {
        let clean = clean(path);
        if clean.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid path"));
        }
        ensure_dir(&mut self.root, &mut self.paths, &clean)?;
        insert(
            &mut self.root,
            &mut self.paths,
            &clean,
            Node::new(String::new(), meta),
        )
    }

    /// Lay out metadata, append outstanding data (dirent blocks, fallback
    /// file blocks, non-inline symlink targets) and the metadata area, then
    /// patch the superblock and return the sink.
    pub async fn finish(mut self) -> io::Result<W> {
        let bs = self.block_size;
        let bits = blk_bits(bs);

        plan_layout(&mut self.root, bs, self.build_time, self.build_time_nsec);

        let entries = collect_entries(&self.root);
        let total_inodes = entries.len() as u64;

        // Nids are final now; fold the streamed blocks into the addr map.
        let mut data_addrs: HashMap<u64, u32> = HashMap::new();
        for e in &entries {
            if let Content::Streamed { .. } = e.content {
                data_addrs.insert(e.nid, e.data_blkaddr);
            }
        }

        // Fallback data pass: everything that could not be inlined.
        let mut addr = self.data_end;
        let mut stack: Vec<&Node> = vec![&self.root];
        while let Some(n) = stack.pop() {
            let needs_data = match n.file_type {
                FileType::RegFile => {
                    n.layout == DataLayout::FlatPlain
                        && matches!(&n.content, Content::Inline(d) if !d.is_empty())
                }
                FileType::Dir => n.layout == DataLayout::FlatPlain,
                FileType::Symlink => n.layout == DataLayout::FlatPlain && !n.link_target.is_empty(),
                _ => false,
            };
            if needs_data {
                let mut buf = match n.file_type {
                    FileType::Dir => build_dirents(n, bs),
                    FileType::Symlink => n.link_target.clone(),
                    FileType::RegFile => match &n.content {
                        Content::Inline(d) => d.clone(),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                pad_to_block(&mut buf, bs);
                data_addrs.insert(n.nid, addr);
                addr += (buf.len() / bs) as u32;
                self.sink.write_all(&buf).await?;
            }
            for c in n.children.iter().rev() {
                stack.push(c);
            }
        }
        let meta_blkaddr = addr;

        // Compute metadata size.
        let mut meta_bytes = 0usize;
        for e in &entries {
            let inode_size = if e.compact {
                SIZE_INODE_COMPACT
            } else {
                SIZE_INODE_EXTENDED
            };
            let sz = round_up(inode_size + e.xattr_size + e.trailing_size, 32);
            let expected = (e.nid as usize) * 32;
            if expected > meta_bytes {
                meta_bytes = expected;
            }
            meta_bytes += sz;
        }
        let meta_blocks = meta_bytes.div_ceil(bs);
        let total_blocks = meta_blkaddr as usize + meta_blocks;

        // Metadata area.
        let mut meta_buf: Vec<u8> = Vec::with_capacity(meta_bytes);
        write_metadata(&self.root, &mut meta_buf, &data_addrs, bs)?;
        debug_assert_eq!(meta_buf.len(), meta_bytes, "metadata size mismatch");
        meta_buf.resize(meta_blocks * bs, 0);
        self.sink.write_all(&meta_buf).await?;

        // Patch the superblock over its placeholder.
        let sb_area_bytes = round_up(EROFS_SUPER_OFFSET + SIZE_SUPER_BLOCK, bs);
        let mut sb_area = vec![0u8; sb_area_bytes];
        write_superblock(
            &mut sb_area,
            SuperblockParams {
                root_nid: self.root.nid,
                inodes: total_inodes,
                epoch: self.build_time,
                fixed_nsec: self.build_time_nsec,
                blocks: total_blocks as u32,
                meta_blkaddr,
                bits,
                uuid: &self.uuid,
                volume_name: &self.volume_name,
                checksum: self.checksum,
            },
        );
        self.sink.seek(io::SeekFrom::Start(0)).await?;
        self.sink.write_all(&sb_area).await?;
        self.sink.flush().await?;
        Ok(self.sink)
    }
}

fn clean(path: &str) -> String {
    let p = path.trim_start_matches('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            _ => parts.push(seg),
        }
    }
    parts.join("/")
}

fn ensure_dir(root: &mut Node, paths: &mut HashSet<String>, clean: &str) -> io::Result<()> {
    if clean.is_empty() {
        return Ok(());
    }
    // Only materialize parent directories; the final segment is created
    // by `insert` with the caller-supplied metadata.
    let parent = match clean.rfind('/') {
        Some(i) => &clean[..i],
        None => return Ok(()),
    };
    let mut cur = String::new();
    for seg in parent.split('/').filter(|s| !s.is_empty()) {
        if !cur.is_empty() {
            cur.push('/');
        }
        cur.push_str(seg);
        if paths.contains(&format!("/{}", cur)) {
            continue;
        }
        let node = Node::new(seg.to_string(), InodeMeta::dir(0o755));
        insert(root, paths, &cur, node)?;
    }
    Ok(())
}

fn insert(
    root: &mut Node,
    paths: &mut HashSet<String>,
    clean: &str,
    mut node: Node,
) -> io::Result<()> {
    let (parent, name) = match clean.rfind('/') {
        Some(i) => (&clean[..i], &clean[i + 1..]),
        None => ("", clean),
    };
    let mut dir = root;
    for seg in parent.split('/').filter(|s| !s.is_empty()) {
        dir = dir
            .children
            .iter_mut()
            .find(|c| c.is_dir() && c.name == seg)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing parent directory"))?;
    }
    if dir.children.iter().any(|c| c.name == name) {
        if paths.contains(&format!("/{}", clean)) {
            return Ok(());
        }
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("duplicate path /{}", clean),
        ));
    }
    node.name = name.to_string();
    dir.children.push(node);
    paths.insert(format!("/{}", clean));
    Ok(())
}

fn plan_layout(root: &mut Node, bs: usize, bt: u64, btn: u32) {
    fn plan_node(n: &mut Node, off: &mut usize, bs: usize, bt: u64, btn: u32) {
        n.xattr_size = calc_xattr_size(&n.meta.xattrs);
        n.compact = n.meta.uid <= 0xFFFF
            && n.meta.gid <= 0xFFFF
            && n.effective_nlink() <= 0xFFFF
            && n.on_disk_size() <= u32::MAX as u64
            && n.meta.mtime == bt
            && n.meta.mtime_nsec == btn;

        let inode_size = if n.compact {
            SIZE_INODE_COMPACT
        } else {
            SIZE_INODE_EXTENDED
        };
        let header_size = inode_size + n.xattr_size;

        match n.file_type {
            FileType::RegFile => {
                n.layout = match &n.content {
                    Content::Empty => DataLayout::FlatPlain,
                    Content::Streamed { .. } => DataLayout::FlatPlain,
                    Content::Inline(ref d) => {
                        if d.is_empty() {
                            DataLayout::FlatPlain
                        } else {
                            let in_block_off = (*off + header_size) % bs;
                            if in_block_off + d.len() <= bs {
                                DataLayout::FlatInline
                            } else {
                                DataLayout::FlatPlain
                            }
                        }
                    }
                };
            }
            FileType::Symlink => {
                let in_block_off = (*off + header_size) % bs;
                n.layout = if !n.link_target.is_empty() && in_block_off + n.link_target.len() <= bs
                {
                    DataLayout::FlatInline
                } else {
                    DataLayout::FlatPlain
                };
            }
            FileType::Dir => {
                let ds = dirent_data_size(n, bs);
                let in_block_off = (*off + header_size) % bs;
                n.layout = if ds > 0 && in_block_off + ds <= bs {
                    DataLayout::FlatInline
                } else {
                    DataLayout::FlatPlain
                };
            }
            _ => n.layout = DataLayout::FlatPlain,
        }

        n.trailing_size = calc_trailing_size(n, bs);

        // Inode core must not cross a block boundary.
        if *off % bs + inode_size > bs {
            *off = round_up(*off, bs);
        }
        n.nid = (*off / 32) as u64;

        // Inline data must not cross a block boundary either.
        if n.layout == DataLayout::FlatInline {
            let block_off = *off % bs;
            if block_off + header_size + n.trailing_size > bs {
                n.layout = DataLayout::FlatPlain;
                n.trailing_size = calc_trailing_size(n, bs);
            }
        }

        let total = round_up(header_size + n.trailing_size, 32);
        *off += total;

        for c in n.children.iter_mut() {
            plan_node(c, off, bs, bt, btn);
        }
    }

    // Root inode lives at offset 0.
    let mut off;
    {
        let n = &mut *root;
        n.nid = 0;
        n.xattr_size = calc_xattr_size(&n.meta.xattrs);
        n.compact = n.meta.uid <= 0xFFFF
            && n.meta.gid <= 0xFFFF
            && n.effective_nlink() <= 0xFFFF
            && n.on_disk_size() <= u32::MAX as u64
            && n.meta.mtime == bt
            && n.meta.mtime_nsec == btn;
        let inode_size = if n.compact {
            SIZE_INODE_COMPACT
        } else {
            SIZE_INODE_EXTENDED
        };
        let header_size = inode_size + n.xattr_size;
        let ds = dirent_data_size(n, bs);
        let in_block_off = header_size % bs;
        n.layout = if ds > 0 && in_block_off + ds <= bs {
            DataLayout::FlatInline
        } else {
            DataLayout::FlatPlain
        };
        n.trailing_size = calc_trailing_size(n, bs);
        off = round_up(header_size + n.trailing_size, 32);
    }
    for i in 0..root.children.len() {
        plan_node(&mut root.children[i], &mut off, bs, bt, btn);
    }

    assign_parent_nids(root, 0);
}

fn collect_entries<'a>(root: &'a Node) -> Vec<&'a Node> {
    let mut out = Vec::new();
    fn rec<'a>(n: &'a Node, out: &mut Vec<&'a Node>) {
        out.push(n);
        for c in &n.children {
            rec(c, out);
        }
    }
    rec(root, &mut out);
    out
}

struct SuperblockParams<'a> {
    root_nid: u64,
    inodes: u64,
    epoch: u64,
    fixed_nsec: u32,
    blocks: u32,
    meta_blkaddr: u32,
    bits: u8,
    uuid: &'a [u8; 16],
    volume_name: &'a str,
    checksum: bool,
}

fn write_superblock(area: &mut [u8], p: SuperblockParams) {
    let mut sb = [0u8; SIZE_SUPER_BLOCK];
    sb[0..4].copy_from_slice(&EROFS_SUPER_MAGIC_V1.to_le_bytes());
    // checksum at 4..8 patched below when enabled
    sb[12] = p.bits; // blkszbits
    sb[13] = 0; // sb_extslots
    sb[14..16].copy_from_slice(&(p.root_nid as u16).to_le_bytes()); // rootnid_2b
    sb[16..24].copy_from_slice(&p.inodes.to_le_bytes());
    sb[24..32].copy_from_slice(&p.epoch.to_le_bytes());
    sb[32..36].copy_from_slice(&p.fixed_nsec.to_le_bytes());
    sb[36..40].copy_from_slice(&p.blocks.to_le_bytes());
    sb[40..44].copy_from_slice(&p.meta_blkaddr.to_le_bytes());
    sb[44..48].copy_from_slice(&0u32.to_le_bytes()); // xattr_blkaddr
    sb[48..64].copy_from_slice(p.uuid);
    let vn = p.volume_name.as_bytes();
    sb[64..64 + vn.len()].copy_from_slice(vn);
    sb[80..84].copy_from_slice(&0u32.to_le_bytes()); // feature_incompat
    sb[84..86].copy_from_slice(&0u16.to_le_bytes()); // available_compr_algs
    sb[86..88].copy_from_slice(&0u16.to_le_bytes()); // extra_devices
    sb[88..90].copy_from_slice(&0u16.to_le_bytes()); // devt_slotoff
                                                     // dirblkbits stays 0: kernels <= 6.18 reject any nonzero value, and
                                                     // block-sized directory blocks are the default anyway

    area[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + SIZE_SUPER_BLOCK].copy_from_slice(&sb);

    if p.checksum {
        // erofs-utils computes crc32c over blksz-1024 bytes starting at the
        // superblock offset, with the checksum field zeroed and the
        // SB_CHKSUM feature bit already set, and no final xor.
        let len = p.bits as usize;
        let len = (1usize << len) - EROFS_SUPER_OFFSET;
        let feat = EROFS_FEATURE_COMPAT_SB_CHKSUM.to_le_bytes();
        area[EROFS_SUPER_OFFSET + 8..EROFS_SUPER_OFFSET + 12].copy_from_slice(&feat);
        area[EROFS_SUPER_OFFSET + 4..EROFS_SUPER_OFFSET + 8].fill(0);
        let mut crc: u32 = 0xFFFFFFFF;
        for &b in &area[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + len] {
            crc ^= b as u32;
            for _ in 0..8 {
                crc = (crc >> 1) ^ (if crc & 1 != 0 { 0x82F63B78 } else { 0 });
            }
        }
        area[EROFS_SUPER_OFFSET + 4..EROFS_SUPER_OFFSET + 8].copy_from_slice(&crc.to_le_bytes());
        let feat = EROFS_FEATURE_COMPAT_SB_CHKSUM.to_le_bytes();
        area[EROFS_SUPER_OFFSET + 8..EROFS_SUPER_OFFSET + 12].copy_from_slice(&feat);
    }
}

fn calc_trailing_size(n: &Node, bs: usize) -> usize {
    match n.file_type {
        FileType::RegFile => match n.layout {
            DataLayout::FlatInline => n.on_disk_size() as usize,
            _ => 0,
        },
        FileType::Dir => {
            if n.layout == DataLayout::FlatInline {
                dirent_data_size(n, bs)
            } else {
                0
            }
        }
        FileType::Symlink => {
            if n.layout == DataLayout::FlatInline {
                n.link_target.len()
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn dirent_data_size(n: &Node, block_size: usize) -> usize {
    let mut names: Vec<&str> = vec![".", ".."];
    names.extend(n.children.iter().map(|c| c.name.as_str()));
    names.sort_unstable();

    let n_entries = names.len();
    let mut total = 0;
    let mut i = 0;
    while i < n_entries {
        let start = i;
        let mut used = 0;
        let mut name_size = 0;
        for j in i..n_entries {
            let headers = (j - start + 1) * SIZE_DIRENT;
            name_size += names[j].len();
            let needed = headers + name_size;
            if needed > block_size {
                break;
            }
            used = needed;
            i = j + 1;
        }
        if i == start {
            used = SIZE_DIRENT + names[i].len();
            i += 1;
        }
        if i < n_entries {
            used = round_up(used, block_size);
        }
        total += used;
    }
    total
}

fn assign_parent_nids(n: &mut Node, parent_nid: u64) {
    n.parent_nid = parent_nid;
    let my = n.nid;
    for c in n.children.iter_mut() {
        assign_parent_nids(c, my);
    }
}

fn build_dirents(n: &Node, block_size: usize) -> Vec<u8> {
    struct De {
        name: String,
        nid: u64,
        ft: FileType,
    }
    let mut all: Vec<De> = vec![
        De {
            name: ".".into(),
            nid: n.nid,
            ft: FileType::Dir,
        },
        De {
            name: "..".into(),
            nid: n.parent_nid,
            ft: FileType::Dir,
        },
    ];
    for c in &n.children {
        all.push(De {
            name: c.name.clone(),
            nid: c.nid,
            ft: c.file_type,
        });
    }
    all.sort_by(|a, b| a.name.cmp(&b.name));

    let mut out = Vec::new();
    let mut i = 0;
    while i < all.len() {
        let start = i;
        let mut name_size = 0;
        for j in i..all.len() {
            let headers = (j - start + 1) * SIZE_DIRENT;
            name_size += all[j].name.len();
            let needed = headers + name_size;
            if needed > block_size {
                break;
            }
            i = j + 1;
        }
        if i == start {
            i += 1;
        }
        let group = &all[start..i];
        let headers_len = group.len() * SIZE_DIRENT;
        let mut nameoff = headers_len as u16;
        for (k, de) in group.iter().enumerate() {
            if k > 0 {
                nameoff += group[k - 1].name.len() as u16;
            }
            out.extend_from_slice(&de.nid.to_le_bytes());
            out.extend_from_slice(&nameoff.to_le_bytes());
            out.push(de.ft as u8);
            out.push(0);
        }
        for de in group {
            out.extend_from_slice(de.name.as_bytes());
        }
        if i < all.len() {
            while out.len() % block_size != 0 {
                out.push(0);
            }
        }
    }
    out
}

fn pad_to_block(buf: &mut Vec<u8>, bs: usize) {
    while buf.len() % bs != 0 {
        buf.push(0);
    }
}

fn write_metadata(
    root: &Node,
    buf: &mut Vec<u8>,
    data_addrs: &HashMap<u64, u32>,
    bs: usize,
) -> io::Result<()> {
    fn rec(
        n: &Node,
        buf: &mut Vec<u8>,
        data_addrs: &HashMap<u64, u32>,
        bs: usize,
    ) -> io::Result<()> {
        let expected = (n.nid as usize) * 32;
        if expected > buf.len() {
            buf.resize(expected, 0);
        }
        write_inode(n, buf, data_addrs)?;

        if n.xattr_size > 0 {
            write_xattrs(n, buf);
        }

        if n.layout == DataLayout::FlatInline {
            match n.file_type {
                FileType::RegFile => {
                    if let Content::Inline(ref d) = n.content {
                        buf.extend_from_slice(d);
                    }
                }
                FileType::Symlink => buf.extend_from_slice(&n.link_target),
                FileType::Dir => {
                    let d = build_dirents(n, bs);
                    buf.extend_from_slice(&d);
                }
                _ => {}
            }
        }

        let inode_size = if n.compact {
            SIZE_INODE_COMPACT
        } else {
            SIZE_INODE_EXTENDED
        };
        let written = inode_size + n.xattr_size + n.trailing_size;
        let pad = round_up(written, 32) - written;
        buf.resize(buf.len() + pad, 0);

        for c in &n.children {
            rec(c, buf, data_addrs, bs)?;
        }
        Ok(())
    }
    rec(root, buf, data_addrs, bs)
}

fn write_inode(n: &Node, buf: &mut Vec<u8>, data_addrs: &HashMap<u64, u32>) -> io::Result<()> {
    let mut b = [0u8; SIZE_INODE_EXTENDED];

    let layout_bits = (n.layout as u16) << EROFS_I_DATALAYOUT_BIT;
    let version_bit: u16 = if n.compact {
        0
    } else {
        1 << EROFS_I_VERSION_BIT
    };

    let mut i_u: u32 = 0;
    let on_disk_size = match n.file_type {
        FileType::Dir => dirent_data_size(n, DEFAULT_BLOCK_SIZE) as u64,
        FileType::Symlink => n.link_target.len() as u64,
        _ => n.on_disk_size(),
    };

    match n.file_type {
        FileType::RegFile | FileType::Dir | FileType::Symlink => {
            if n.layout == DataLayout::FlatPlain {
                i_u = *data_addrs.get(&n.nid).unwrap_or(&0);
            }
        }
        FileType::Chrdev | FileType::Blkdev | FileType::Fifo | FileType::Sock => {
            i_u = n.meta.rdev;
        }
        _ => {}
    }

    let nlink = n.effective_nlink();

    if n.compact {
        b[0..2].copy_from_slice(&(layout_bits | version_bit).to_le_bytes());
        b[2..4].copy_from_slice(&xattr_count(n.xattr_size).to_le_bytes());
        b[4..6].copy_from_slice(&n.meta.mode.to_le_bytes());
        b[6..8].copy_from_slice(&(nlink as u16).to_le_bytes());
        b[8..12].copy_from_slice(&(on_disk_size as u32).to_le_bytes());
        b[16..20].copy_from_slice(&i_u.to_le_bytes());
        b[24..26].copy_from_slice(&(n.meta.uid as u16).to_le_bytes());
        b[26..28].copy_from_slice(&(n.meta.gid as u16).to_le_bytes());
        buf.extend_from_slice(&b[..SIZE_INODE_COMPACT]);
    } else {
        b[0..2].copy_from_slice(&(layout_bits | version_bit).to_le_bytes());
        b[2..4].copy_from_slice(&xattr_count(n.xattr_size).to_le_bytes());
        b[4..6].copy_from_slice(&n.meta.mode.to_le_bytes());
        b[8..16].copy_from_slice(&on_disk_size.to_le_bytes());
        b[16..20].copy_from_slice(&i_u.to_le_bytes());
        b[24..28].copy_from_slice(&n.meta.uid.to_le_bytes());
        b[28..32].copy_from_slice(&n.meta.gid.to_le_bytes());
        b[32..40].copy_from_slice(&n.meta.mtime.to_le_bytes());
        b[40..44].copy_from_slice(&n.meta.mtime_nsec.to_le_bytes());
        b[44..48].copy_from_slice(&nlink.to_le_bytes());
        buf.extend_from_slice(&b[..SIZE_INODE_EXTENDED]);
    }
    Ok(())
}

fn write_xattrs(n: &Node, buf: &mut Vec<u8>) {
    let mut hdr = [0u8; SIZE_XATTR_IBODY_HEADER];
    hdr[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    buf.extend_from_slice(&hdr);

    for (name, value) in &n.meta.xattrs {
        let (idx, suffix) = split_xattr_name(name);
        buf.push(suffix.len() as u8);
        buf.push(idx);
        buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        buf.extend_from_slice(suffix.as_bytes());
        buf.extend_from_slice(value);
        let entry_len = SIZE_XATTR_ENTRY + suffix.len() + value.len();
        for _ in entry_len..round_up(entry_len, 4) {
            buf.push(0);
        }
    }
}
