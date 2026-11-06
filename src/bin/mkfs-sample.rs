use clap::{Parser, ValueEnum};
use erofs_builder::layout::mode;
use erofs_builder::{CreateOptions, InodeMeta, Writer};
use tokio::io::{AsyncReadExt, BufWriter};

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
enum Mode {
    /// Deterministic fixed fixture (used by the test script).
    Fixture,
    /// Pack a directory tree into an image.
    Dir,
    /// Pack a ustar tar archive into an image in a single pass.
    Tar,
}

#[derive(Parser, Debug)]
#[command(name = "mkfs-sample", about = "Create an EROFS image with erofs-rs")]
struct Args {
    output: String,
    #[arg(value_enum, default_value_t = Mode::Fixture)]
    mode: Mode,
    #[arg(long)]
    dir: Option<String>,
    #[arg(long)]
    tar: Option<String>,
    #[arg(long, default_value_t = 4096)]
    block_size: usize,
    #[arg(long, default_value_t = 0)]
    build_time: u64,
    #[arg(long)]
    checksum: bool,
}

type Sink = BufWriter<tokio::fs::File>;
type ImageWriter = Writer<Sink>;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let opts = CreateOptions {
        block_size: args.block_size,
        build_time: args.build_time,
        build_time_nsec: 0,
        uuid: *uuid_from_seed(args.build_time),
        volume_name: "erofs-builder".into(),
        checksum: args.checksum,
    };
    let file = tokio::fs::File::create(&args.output).await?;
    let mut w = Writer::new(BufWriter::new(file), opts).await?;

    match args.mode {
        Mode::Fixture => build_fixture(&mut w).await?,
        Mode::Dir => {
            let dir = args.dir.as_deref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--dir required for dir mode",
                )
            })?;
            pack_dir(&mut w, dir).await?;
        }
        Mode::Tar => {
            let tar = args.tar.as_deref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--tar required for tar mode",
                )
            })?;
            pack_tar(&mut w, tar).await?;
        }
    }

    let file = w.finish().await?.into_inner();
    file.sync_all().await?;
    Ok(())
}

fn uuid_from_seed(seed: u64) -> &'static [u8; 16] {
    use rand::{RngCore, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    Box::leak(Box::new({
        let mut u = [0u8; 16];
        rng.fill_bytes(&mut u);
        u
    }))
}

async fn build_fixture(w: &mut ImageWriter) -> std::io::Result<()> {
    const T: u64 = 1_700_000_000;
    let meta_dir = InodeMeta {
        mtime: T,
        ..InodeMeta::dir(0o755)
    };

    w.mkdir("/etc", meta_dir.clone()).await?;

    let mut motd_meta = InodeMeta {
        mtime: T,
        ..InodeMeta::reg(0o644)
    };
    motd_meta
        .xattrs
        .insert("trusted.origin".to_string(), b"erofs-builder".to_vec());
    let motd = b"hello from erofs-builder\n";
    let mut motd_cur = std::io::Cursor::new(motd);
    w.add_file("/etc/motd", motd_meta, motd.len() as u64, &mut motd_cur)
        .await?;

    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let mut big_cur = std::io::Cursor::new(big);
    w.add_file(
        "/big.bin",
        InodeMeta {
            mtime: T,
            ..InodeMeta::reg(0o600)
        },
        100_000,
        &mut big_cur,
    )
    .await?;

    w.symlink(
        "/link",
        b"etc/motd",
        InodeMeta {
            mtime: T,
            ..InodeMeta::symlink()
        },
    )
    .await?;

    let mut zero_meta = InodeMeta {
        mtime: T,
        ..Default::default()
    };
    zero_meta.mode = 0o020000 | 0o666; // chrdev
    zero_meta.rdev = (0x01 << 8) | 0x05; // 1:5
    w.mknod("/zero", zero_meta).await?;

    w.mkdir("/empty", meta_dir).await?;
    let mut nothing = tokio::io::empty();
    w.add_file(
        "/empty-file",
        InodeMeta {
            mtime: T,
            ..InodeMeta::reg(0o644)
        },
        0,
        &mut nothing,
    )
    .await?;
    Ok(())
}

async fn pack_dir(w: &mut ImageWriter, root: &str) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    fn meta_from(md: &std::fs::Metadata) -> InodeMeta {
        InodeMeta {
            mode: (md.mode() & 0xFFFF) as u16,
            uid: md.uid(),
            gid: md.gid(),
            mtime: md.mtime().max(0) as u64,
            mtime_nsec: md.mtime_nsec().max(0) as u32,
            nlink: Some(md.nlink() as u32),
            rdev: md.rdev() as u32,
            xattrs: Default::default(),
        }
    }

    async fn walk(w: &mut ImageWriter, fs_path: &str, img_path: &str) -> std::io::Result<()> {
        let mut rd = tokio::fs::read_dir(fs_path).await?;
        let mut entries = Vec::new();
        while let Some(e) = rd.next_entry().await? {
            entries.push(e);
        }
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() > 255 {
                continue;
            }
            let child_fs = format!("{}/{}", fs_path, name);
            let child_img = format!("{}/{}", img_path.trim_end_matches('/'), name);
            let md = entry.metadata().await?;
            let ft = md.file_type();
            if ft.is_dir() {
                w.mkdir(&child_img, meta_from(&md)).await?;
                Box::pin(walk(w, &child_fs, &child_img)).await?;
            } else if ft.is_symlink() {
                let target = tokio::fs::read_link(&child_fs).await?;
                w.symlink(
                    &child_img,
                    target.as_os_str().as_encoded_bytes(),
                    meta_from(&md),
                )
                .await?;
            } else if ft.is_file() {
                let md = tokio::fs::metadata(&child_fs).await?;
                let mut f = tokio::fs::File::open(&child_fs).await?;
                w.add_file(&child_img, meta_from(&md), md.len(), &mut f)
                    .await?;
            } else {
                w.mknod(&child_img, meta_from(&md)).await?;
            }
        }
        Ok(())
    }

    let md = tokio::fs::metadata(root).await?;
    w.mkdir("/", meta_from(&md)).await?;
    walk(w, root, "/").await
}

fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).trim().to_string()
}

fn octal(field: &[u8]) -> std::io::Result<u64> {
    if !field.is_empty() && field[0] & 0x80 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "base-256 tar fields are not supported",
        ));
    }
    let s = cstr(field);
    if s.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(&s, 8).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid octal field {:?}", s),
        )
    })
}

async fn skip_bytes<R: AsyncReadExt + Unpin>(r: &mut R, mut n: u64) -> std::io::Result<()> {
    let mut buf = vec![0u8; 8192];
    while n > 0 {
        let want = buf.len().min(n as usize);
        r.read_exact(&mut buf[..want]).await?;
        n -= want as u64;
    }
    Ok(())
}

/// Read a ustar archive sequentially, feeding each member to the writer as
/// its header streams past. Member data is consumed directly from the same
/// file handle by `add_file`.
async fn pack_tar(w: &mut ImageWriter, tar_path: &str) -> std::io::Result<()> {
    let mut f = tokio::fs::File::open(tar_path).await?;
    loop {
        let mut hdr = [0u8; 512];
        match f.read_exact(&mut hdr).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        if hdr.iter().all(|&b| b == 0) {
            break;
        }

        let mut name = cstr(&hdr[0..100]);
        let is_ustar = &hdr[257..262] == b"ustar";
        if is_ustar && &hdr[263..265] == b"00" {
            let prefix = cstr(&hdr[345..500]);
            if !prefix.is_empty() {
                name = format!("{}/{}", prefix, name);
            }
        }

        let perms = octal(&hdr[100..108])? as u16 & 0o7777;
        let uid = octal(&hdr[108..116])? as u32;
        let gid = octal(&hdr[116..124])? as u32;
        let size = octal(&hdr[124..136])?;
        let mtime = octal(&hdr[136..148])?;
        let typeflag = hdr[156];

        match typeflag {
            b'5' => {
                w.mkdir(
                    &name,
                    InodeMeta {
                        mode: mode::S_IFDIR | perms,
                        uid,
                        gid,
                        mtime,
                        ..Default::default()
                    },
                )
                .await?;
            }
            b'0' | 0 | b'7' => {
                w.add_file(
                    &name,
                    InodeMeta {
                        mode: mode::S_IFREG | perms,
                        uid,
                        gid,
                        mtime,
                        ..Default::default()
                    },
                    size,
                    &mut f,
                )
                .await?;
            }
            b'2' => {
                let target = cstr(&hdr[157..257]);
                w.symlink(
                    &name,
                    target.as_bytes(),
                    InodeMeta {
                        mode: mode::S_IFLNK | perms,
                        uid,
                        gid,
                        mtime,
                        ..Default::default()
                    },
                )
                .await?;
            }
            b'3' | b'4' | b'6' => {
                let major = octal(&hdr[329..337])?;
                let minor = octal(&hdr[337..345])?;
                let ifmt = match typeflag {
                    b'3' => mode::S_IFCHR,
                    b'4' => mode::S_IFBLK,
                    _ => mode::S_IFIFO,
                };
                w.mknod(
                    &name,
                    InodeMeta {
                        mode: ifmt | perms,
                        uid,
                        gid,
                        mtime,
                        rdev: ((major as u32) << 8) | minor as u32,
                        ..Default::default()
                    },
                )
                .await?;
            }
            b'x' | b'g' => {}
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported tar entry type {:?}", other as char),
                ));
            }
        };

        // Only regular-file members had their data consumed by add_file.
        if !matches!(typeflag, b'0' | 0 | b'7') {
            skip_bytes(&mut f, size).await?;
        }
        let pad = size.div_ceil(512) * 512 - size;
        if pad > 0 {
            skip_bytes(&mut f, pad).await?;
        }
    }
    Ok(())
}
