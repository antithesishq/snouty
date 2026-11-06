//! On-disk layout constants and structures (see fs/erofs/erofs_fs.h).

pub const EROFS_SUPER_MAGIC_V1: u32 = 0xE0F5E1E2;
/// To allow for x86 boot sectors and other oddities.
pub const EROFS_SUPER_OFFSET: usize = 1024;

pub const EROFS_FEATURE_COMPAT_SB_CHKSUM: u32 = 0x00000001;
pub const EROFS_FEATURE_INCOMPAT_CHUNKED_FILE: u32 = 0x00000004;
pub const EROFS_FEATURE_INCOMPAT_DEVICE_TABLE: u32 = 0x00000008;
pub const EROFS_ALL_FEATURE_INCOMPAT: u32 = 0x000001FF;

pub const EROFS_I_VERSION_BIT: u16 = 0;
pub const EROFS_I_DATALAYOUT_BIT: u16 = 1;
pub const EROFS_I_NLINK_1_BIT: u16 = 4;
pub const EROFS_I_DOT_OMITTED_BIT: u16 = 4;

pub const SIZE_INODE_COMPACT: usize = 32;
pub const SIZE_INODE_EXTENDED: usize = 64;
pub const SIZE_DIRENT: usize = 12;
pub const SIZE_XATTR_IBODY_HEADER: usize = 12;
pub const SIZE_XATTR_ENTRY: usize = 4;
pub const SIZE_DEVICE_SLOT: usize = 128;
pub const SIZE_CHUNK_INDEX: usize = 8;
pub const SIZE_SUPER_BLOCK: usize = 128;

pub const EROFS_NAME_LEN: usize = 255;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum DataLayout {
    FlatPlain = 0,
    CompressedFull = 1,
    FlatInline = 2,
    CompressedCompact = 3,
    ChunkBased = 4,
}

impl DataLayout {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => DataLayout::FlatPlain,
            1 => DataLayout::CompressedFull,
            2 => DataLayout::FlatInline,
            3 => DataLayout::CompressedCompact,
            4 => DataLayout::ChunkBased,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub enum FileType {
    Unknown = 0,
    RegFile = 1,
    Dir = 2,
    Chrdev = 3,
    Blkdev = 4,
    Fifo = 5,
    Sock = 6,
    Symlink = 7,
}

impl FileType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => FileType::Unknown,
            1 => FileType::RegFile,
            2 => FileType::Dir,
            3 => FileType::Chrdev,
            4 => FileType::Blkdev,
            5 => FileType::Fifo,
            6 => FileType::Sock,
            7 => FileType::Symlink,
            _ => return None,
        })
    }
}

pub mod mode {
    pub const S_IFMT: u16 = 0o170000;
    pub const S_IFREG: u16 = 0o100000;
    pub const S_IFDIR: u16 = 0o040000;
    pub const S_IFCHR: u16 = 0o020000;
    pub const S_IFBLK: u16 = 0o060000;
    pub const S_IFIFO: u16 = 0o010000;
    pub const S_IFSOCK: u16 = 0o140000;
    pub const S_IFLNK: u16 = 0o120000;

    pub fn file_type(mode: u16) -> u16 {
        mode & S_IFMT
    }
}

/// Name indexes for well-known xattr prefixes.
pub mod xattr_index {
    pub const USER: u8 = 1;
    pub const POSIX_ACL_ACCESS: u8 = 2;
    pub const POSIX_ACL_DEFAULT: u8 = 3;
    pub const TRUSTED: u8 = 4;
    pub const LUSTRE: u8 = 5;
    pub const SECURITY: u8 = 6;
}

/// Chunk format bits (i_u for chunk-based inodes).
pub const EROFS_CHUNK_FORMAT_BLKBITS_MASK: u32 = 0x001F;
pub const EROFS_CHUNK_FORMAT_INDEXES: u32 = 0x0020;
pub const EROFS_CHUNK_FORMAT_48BIT: u32 = 0x0040;

/// Sentinel physical block address marking a hole (sparse chunk).
pub const NULL_ADDR: u64 = u64::MAX;
