//! Async EROFS image writer on tokio.
//!
//! Images are written incrementally by [`Writer`], which streams file data
//! to any [`tokio::io::AsyncWrite`] + [`tokio::io::AsyncSeek`] sink (e.g. a
//! [`tokio::fs::File`], or a `std::io::Cursor<Vec<u8>>` for in-memory
//! images) as entries are added, patching the superblock in place on
//! finish. This allows sources that can only be read once, such as a
//! streaming tar archive, to be packed in a single pass with bounded
//! memory use.

pub mod layout;
pub mod reader;
pub mod writer;

pub use layout::{DataLayout, FileType};
pub use reader::{Dirent, Reader, Stat};
pub use writer::{CreateOptions, InodeMeta, Writer, DEFAULT_BLOCK_SIZE};
