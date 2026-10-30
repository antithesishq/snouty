use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use color_eyre::eyre::{Result, bail};
use futures_util::Stream;

pub(super) type LogStream = Pin<Box<dyn Stream<Item = Result<(u64, String)>> + Send>>;
pub(super) const MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn serial_lines(path: &Path, start: u64) -> Result<LogStream> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    Ok(Box::pin(futures_util::stream::try_unfold(
        (file, Vec::<u8>::new(), start),
        |(mut file, mut pending, mut offset)| async move {
            loop {
                if let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                    let bytes: Vec<_> = pending.drain(..=end).collect();
                    let line = String::from_utf8_lossy(&bytes[..end]).into_owned();
                    offset += bytes.len() as u64;
                    return Ok(Some(((offset, line), (file, pending, offset))));
                }
                let mut buffer = [0; 8192];
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    pending.extend_from_slice(&buffer[..count]);
                    if pending.len() > MAX_RECORD_BYTES {
                        bail!("instrumentation line exceeds 4 MiB");
                    }
                }
            }
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulate::events::{self, EventKind};
    use futures_util::{FutureExt, StreamExt};
    use hegel::generators;
    use std::io::Write;

    #[tokio::test]
    async fn tail_preserves_partial_lines_across_cancelled_reads() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "old boot event\n").unwrap();
        let mut stream =
            serial_lines(file.path(), std::fs::metadata(file.path()).unwrap().len()).unwrap();
        let mut writer = std::fs::OpenOptions::new()
            .append(true)
            .open(file.path())
            .unwrap();
        write!(writer, "10 [workload] [INFO] 'part").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(80), stream.next())
                .await
                .is_err()
        );
        writeln!(writer, "ial'").unwrap();
        let line = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(line.1, "10 [workload] [INFO] 'partial'");
        assert!(events::parse_line(&line.1).unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_ends_after_an_oversized_record() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut stream =
            serial_lines(file.path(), std::fs::metadata(file.path()).unwrap().len()).unwrap();
        std::fs::write(file.path(), vec![b'x'; 4 * 1024 * 1024 + 1]).unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }

    #[hegel::test]
    fn readiness_record_survives_any_serial_split(tc: hegel::TestCase) {
        let record = b"2 [antithesis/ssh-ready.json] [JSON] '{\"antithesis_host\":{\"status\":\"ssh_ready\"}}'\n";
        let split = tc.draw(generators::integers::<usize>().max_value(record.len() - 1));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut stream = serial_lines(file.path(), 0).unwrap();
            let mut writer = file.reopen().unwrap();
            writer.write_all(&record[..split]).unwrap();
            assert!(stream.next().now_or_never().is_none());
            writer.write_all(&record[split..]).unwrap();
            let (offset, line) = stream.next().await.unwrap().unwrap();
            assert_eq!(offset, record.len() as u64);
            assert!(matches!(
                events::parse_line(&line).unwrap().unwrap().kind,
                EventKind::SshReady
            ));
        });
    }
}
