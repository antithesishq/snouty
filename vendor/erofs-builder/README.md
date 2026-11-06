# erofs-builder

A simple library to build [EROFS](https://docs.kernel.org/filesystems/erofs.html) images.

## Usage

```rust
use erofs_builder::{CreateOptions, InodeMeta, Writer};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let out = tokio::fs::File::create("image.erofs").await?;
    let mut w = Writer::new(out, CreateOptions::default()).await?;

    let mut f = tokio::fs::File::open("big.bin").await?;
    let size = f.metadata().await?.len();
    w.add_file("/big.bin", InodeMeta::reg(0o644), size, &mut f).await?;

    let out = w.finish().await?;
    out.sync_all().await
}
```
