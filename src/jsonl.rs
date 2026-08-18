//! The standard shape of every Antithesis streaming response: JSONL, one
//! JSON value per line.
//!
//! Every streaming endpoint — logs, build logs, events, event search,
//! execute-command, and whatever comes next — answers with this shape, so
//! every one of them comes out of [`crate::api`] as a [`JsonStream`]: a
//! [`futures_util::Stream`] of parsed [`Value`]s, split from the HTTP byte
//! stream and parsed with serde as early as possible. A line that is not
//! valid JSON fails the stream with the corrupted line in the error.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use color_eyre::Section;
use color_eyre::eyre::{Result, eyre};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use serde_json::Value;

/// A JSONL response stream: each item is one line, parsed. Lines are yielded
/// as they arrive — a partial line buffers across chunks, so a live stream
/// renders line by line rather than waiting for an EOF that may never come.
/// Empty lines are skipped; a final line without a trailing newline still
/// counts.
pub struct JsonStream {
    inner: BoxStream<'static, Result<Value>>,
}

impl JsonStream {
    pub fn new(stream: progenitor_client::ByteStream) -> Self {
        Self::from_stream(stream.into_inner())
    }

    /// Wrap any chunk stream; the constructor tests and non-HTTP callers use.
    pub fn from_stream(
        stream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
    ) -> Self {
        Self::from_values(LineParser {
            inner: stream.boxed(),
            buf: Vec::new(),
            done: false,
        })
    }

    /// Wrap a stream whose items are already parsed values (the API cache
    /// replays and tees entries at this level).
    pub fn from_values(stream: impl Stream<Item = Result<Value>> + Send + 'static) -> Self {
        Self {
            inner: stream.boxed(),
        }
    }
}

impl Stream for JsonStream {
    type Item = Result<Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

/// The byte-to-line half of [`JsonStream::from_stream`]: splits the HTTP
/// chunk stream on newlines and parses each line.
struct LineParser {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    buf: Vec<u8>,
    done: bool,
}

impl Stream for LineParser {
    type Item = Result<Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(pos) = this.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = this.buf.drain(..=pos).take(pos).collect();
                if line.is_empty() {
                    continue;
                }
                return Poll::Ready(Some(parse_line(&line)));
            }
            if this.done {
                if this.buf.is_empty() {
                    return Poll::Ready(None);
                }
                let line = std::mem::take(&mut this.buf);
                return Poll::Ready(Some(parse_line(&line)));
            }
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(chunk))) => this.buf.extend_from_slice(&chunk),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => this.done = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn parse_line(bytes: &[u8]) -> Result<Value> {
    let line = std::str::from_utf8(bytes)
        .map_err(|e| eyre!("invalid UTF-8 in the response stream: {e}"))?;
    serde_json::from_str(line).map_err(|e| {
        eyre!("invalid JSON in the response stream: {e}").note(format!(
            "the corrupted line: {}",
            crate::render::sanitize(line)
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{TryStreamExt, stream};

    fn jsonl(chunks: &[&'static str]) -> JsonStream {
        let chunks: Vec<reqwest::Result<Bytes>> = chunks
            .iter()
            .map(|c| Ok(Bytes::from_static(c.as_bytes())))
            .collect();
        JsonStream::from_stream(stream::iter(chunks))
    }

    async fn drain(mut stream: JsonStream) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(value) = stream.try_next().await.unwrap() {
            out.push(value.to_string());
        }
        out
    }

    #[tokio::test]
    async fn splits_lines_across_chunk_boundaries() {
        let stream = jsonl(&["{\"a\":1}\n{\"b\"", ":2}\n", "{\"c\":3}"]);
        assert_eq!(
            drain(stream).await,
            vec![r#"{"a":1}"#, r#"{"b":2}"#, r#"{"c":3}"#]
        );
    }

    #[tokio::test]
    async fn skips_empty_lines_and_parses_any_json_value() {
        // Reshaped rows (fold/map pipelines) can be any JSON value, not just
        // objects.
        let stream = jsonl(&["{\"a\":1}\n\n\n[1,2]\n42\n"]);
        assert_eq!(drain(stream).await, vec![r#"{"a":1}"#, "[1,2]", "42"]);
    }

    #[tokio::test]
    async fn round_trips_floats() {
        // float_roundtrip makes the parsed Value a faithful currency for raw
        // output: float values survive.
        let stream = jsonl(&["{\"a\":{\"vtime\":398.4898056755774}}\n"]);
        assert_eq!(
            drain(stream).await,
            vec![r#"{"a":{"vtime":398.4898056755774}}"#]
        );
    }

    /// The whole-pipeline guarantee: any series of JSON values, serialized
    /// to one JSONL buffer and split into arbitrary chunks, round-trips
    /// through the stream back to the same series.
    #[hegel::test]
    fn round_trips_any_series_across_any_chunking(tc: hegel::TestCase) {
        use hegel::generators;

        /// A random JSON value, at most `2 - depth` levels of nesting deep.
        fn json_value(tc: &hegel::TestCase, depth: u8) -> Value {
            let max_tag = if depth < 2 { 6 } else { 4 };
            match tc.draw(generators::integers::<u8>().max_value(max_tag)) {
                0 => Value::Null,
                1 => Value::Bool(tc.draw(generators::booleans())),
                2 => Value::from(tc.draw(generators::integers::<i64>())),
                3 => tc
                    .draw(
                        generators::floats::<f64>()
                            .allow_nan(false)
                            .allow_infinity(false),
                    )
                    .into(),
                4 => Value::String(tc.draw(generators::text())),
                5 => Value::Array(
                    (0..tc.draw(generators::integers::<usize>().max_value(3)))
                        .map(|_| json_value(tc, depth + 1))
                        .collect(),
                ),
                _ => {
                    let mut map = serde_json::Map::new();
                    for _ in 0..tc.draw(generators::integers::<usize>().max_value(3)) {
                        map.insert(tc.draw(generators::text()), json_value(tc, depth + 1));
                    }
                    Value::Object(map)
                }
            }
        }

        let count = tc.draw(generators::integers::<usize>().max_value(8));
        let values: Vec<Value> = (0..count).map(|_| json_value(&tc, 0)).collect();

        // One JSONL buffer (serialization escapes any newline inside a
        // value, so the line structure is exactly one value per line) ...
        let mut buf: Vec<u8> = Vec::new();
        for value in &values {
            buf.extend_from_slice(value.to_string().as_bytes());
            buf.push(b'\n');
        }

        // ... split into arbitrary chunks, so line boundaries land anywhere.
        // Empty chunks mix in too: HTTP/2 allows an empty DATA frame and
        // hyper does not promise to filter one out, so the stream must
        // treat an empty chunk as a no-op. Each iteration still consumes
        // at least one byte, so the loop terminates.
        let mut chunks: Vec<reqwest::Result<Bytes>> = Vec::new();
        let mut rest = buf.as_slice();
        while !rest.is_empty() {
            if tc.draw(generators::booleans()) {
                chunks.push(Ok(Bytes::new()));
            }
            let n = tc.draw(
                generators::integers::<usize>()
                    .min_value(1)
                    .max_value(rest.len()),
            );
            let (head, tail) = rest.split_at(n);
            chunks.push(Ok(Bytes::copy_from_slice(head)));
            rest = tail;
        }

        let out = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let mut stream = JsonStream::from_stream(stream::iter(chunks));
                let mut out = Vec::new();
                while let Some(value) = stream.try_next().await.unwrap() {
                    out.push(value);
                }
                out
            });
        assert_eq!(out, values);
    }

    #[tokio::test]
    async fn a_corrupted_line_fails_the_stream_and_names_it() {
        let mut stream = jsonl(&["{\"a\":1}\nnot json\n"]);
        assert!(stream.try_next().await.unwrap().is_some());
        let err = format!("{:?}", stream.try_next().await.unwrap_err());
        assert!(err.contains("invalid JSON in the response stream"), "{err}");
        assert!(err.contains("the corrupted line: not json"), "{err}");
    }

    #[tokio::test]
    async fn invalid_utf8_is_an_error() {
        let chunks: Vec<reqwest::Result<Bytes>> =
            vec![Ok(Bytes::from_static(b"{\"a\":1}\n\xff\xfe\n"))];
        let mut stream = JsonStream::from_stream(stream::iter(chunks));
        assert!(stream.try_next().await.unwrap().is_some());
        assert!(stream.try_next().await.is_err());
    }
}
