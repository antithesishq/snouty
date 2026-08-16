//! The standard shape of every Antithesis streaming response: JSONL, one
//! JSON object per line.
//!
//! Every streaming endpoint — logs, build logs, events, event search,
//! execute-command, and whatever comes next — answers with this shape, so
//! every one of them comes out of [`crate::api`] as a [`JsonlStream`]: the
//! HTTP byte stream split into lines and parsed with serde as early as
//! possible. Consumers see [`JsonlLine`]s, never raw bytes.
//!
//! Parsing this early costs nothing in fidelity: serde_json is built with
//! `preserve_order` and `float_roundtrip`, so a [`Value`] serialized back out
//! carries the server's key order and float values. That makes the parsed
//! Value the single currency for both raw round-trip output and rendered
//! output.

use bytes::Bytes;
use color_eyre::eyre::{Result, eyre};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use serde_json::Value;

/// One line of a JSONL stream, parsed as early as possible.
#[derive(Debug)]
pub enum JsonlLine {
    /// The happy case: the line parsed as a JSON *object* — the shape every
    /// record has.
    Value(Value),
    /// Everything else: a line that isn't valid JSON (a truncated final
    /// chunk, a proxy-injected error blob, …) or valid JSON that isn't an
    /// object. Carries the original text so callers surface it verbatim
    /// rather than dropping it silently.
    Raw(String),
}

/// A JSONL response stream: the HTTP byte stream, split into lines and
/// parsed one [`JsonlLine`] at a time. Pull lines with [`next_line`]
/// (`JsonlStream::next_line`); it buffers partial lines across chunks and
/// yields each complete line as it arrives, so a live stream renders line by
/// line rather than waiting for an EOF that may never come.
pub struct JsonlStream {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    buf: Vec<u8>,
    done: bool,
}

impl JsonlStream {
    pub fn new(stream: progenitor_client::ByteStream) -> Self {
        Self::from_stream(stream.into_inner())
    }

    /// Wrap any chunk stream; the constructor tests and non-HTTP callers use.
    pub fn from_stream(
        stream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
    ) -> Self {
        Self {
            inner: stream.boxed(),
            buf: Vec::new(),
            done: false,
        }
    }

    /// The next line of the stream, or `None` when the stream is over.
    /// Empty lines are skipped; a final line without a trailing newline
    /// still counts.
    pub async fn next_line(&mut self) -> Result<Option<JsonlLine>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = self.buf.drain(..=pos).take(pos).collect();
                if line_bytes.is_empty() {
                    continue;
                }
                return Ok(Some(parse_line(&line_bytes)?));
            }
            if self.done {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                let line_bytes = std::mem::take(&mut self.buf);
                return Ok(Some(parse_line(&line_bytes)?));
            }
            match self.inner.next().await {
                Some(chunk) => self.buf.extend_from_slice(&chunk?),
                None => self.done = true,
            }
        }
    }
}

fn parse_line(bytes: &[u8]) -> Result<JsonlLine> {
    let line =
        std::str::from_utf8(bytes).map_err(|e| eyre!("invalid UTF-8 in response stream: {e}"))?;
    if let Ok(value) = serde_json::from_str::<Value>(line)
        && value.is_object()
    {
        return Ok(JsonlLine::Value(value));
    }
    Ok(JsonlLine::Raw(line.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    fn jsonl(chunks: &[&'static str]) -> JsonlStream {
        let chunks: Vec<reqwest::Result<Bytes>> = chunks
            .iter()
            .map(|c| Ok(Bytes::from_static(c.as_bytes())))
            .collect();
        JsonlStream::from_stream(stream::iter(chunks))
    }

    async fn drain(mut stream: JsonlStream) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(line) = stream.next_line().await.unwrap() {
            out.push(match line {
                JsonlLine::Value(value) => format!("V {value}"),
                JsonlLine::Raw(raw) => format!("R {raw}"),
            });
        }
        out
    }

    #[tokio::test]
    async fn splits_lines_across_chunk_boundaries() {
        let stream = jsonl(&["{\"a\":1}\n{\"b\"", ":2}\n", "{\"c\":3}"]);
        assert_eq!(
            drain(stream).await,
            vec![r#"V {"a":1}"#, r#"V {"b":2}"#, r#"V {"c":3}"#]
        );
    }

    #[tokio::test]
    async fn skips_empty_lines_and_keeps_non_objects_raw() {
        let stream = jsonl(&["{\"a\":1}\n\n\nnot json\n[1,2]\n42\n"]);
        assert_eq!(
            drain(stream).await,
            vec![r#"V {"a":1}"#, "R not json", "R [1,2]", "R 42"]
        );
    }

    #[tokio::test]
    async fn round_trips_key_order_and_floats() {
        // preserve_order + float_roundtrip make the parsed Value a faithful
        // currency for raw output: key order and float values survive.
        let stream = jsonl(&["{\"z\":1,\"a\":{\"vtime\":398.4898056755774}}\n"]);
        assert_eq!(
            drain(stream).await,
            vec![r#"V {"z":1,"a":{"vtime":398.4898056755774}}"#]
        );
    }

    #[tokio::test]
    async fn invalid_utf8_is_an_error() {
        let chunks: Vec<reqwest::Result<Bytes>> =
            vec![Ok(Bytes::from_static(b"{\"a\":1}\n\xff\xfe\n"))];
        let mut stream = JsonlStream::from_stream(stream::iter(chunks));
        assert!(matches!(
            stream.next_line().await.unwrap(),
            Some(JsonlLine::Value(_))
        ));
        assert!(stream.next_line().await.is_err());
    }
}
