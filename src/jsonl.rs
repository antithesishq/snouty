//! The standard shape of every Antithesis streaming response: JSONL, one
//! JSON value per line.
//!
//! Every streaming endpoint — logs, build logs, events, event search,
//! execute-command, and whatever comes next — answers with this shape, so
//! every one of them comes out of [`crate::api`] as a [`JsonStream`]: a
//! boxed [`futures_util::Stream`] of parsed [`Value`]s, split from the HTTP
//! byte stream and parsed with serde as early as possible. A line that is
//! not valid JSON fails the stream with the corrupted line in the error.

use bytes::Bytes;
use color_eyre::Section;
use color_eyre::eyre::{Report, Result, eyre};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt, stream};
use serde_json::Value;

/// A stream of parsed JSON values: what every streaming endpoint returns,
/// and what the API cache replays and tees.
pub type JsonStream = BoxStream<'static, Result<Value>>;

/// Parse a JSONL chunk stream into a [`JsonStream`]: each item is one line,
/// parsed. Lines are yielded as they arrive — a partial line buffers across
/// chunks, so a live stream renders line by line rather than waiting for an
/// EOF that may never come. Empty lines are skipped; a final line without a
/// trailing newline still counts.
pub fn json_lines<E>(chunks: impl Stream<Item = Result<Bytes, E>> + Send + 'static) -> JsonStream
where
    E: std::error::Error + Send + Sync + 'static,
{
    let chunks = chunks.map(|item| item.map_err(Report::from)).boxed();
    stream::unfold(
        (chunks, Vec::new(), false),
        |(mut chunks, mut buf, mut done)| async move {
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).take(pos).collect();
                    if line.is_empty() {
                        continue;
                    }
                    return Some((parse_line(&line), (chunks, buf, done)));
                }
                if done {
                    if buf.is_empty() {
                        return None;
                    }
                    let line = std::mem::take(&mut buf);
                    return Some((parse_line(&line), (chunks, buf, done)));
                }
                match chunks.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(err)) => return Some((Err(err), (chunks, buf, done))),
                    None => done = true,
                }
            }
        },
    )
    .boxed()
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
    use futures_util::TryStreamExt;

    fn jsonl(chunks: &[&'static str]) -> JsonStream {
        let chunks: Vec<reqwest::Result<Bytes>> = chunks
            .iter()
            .map(|c| Ok(Bytes::from_static(c.as_bytes())))
            .collect();
        json_lines(stream::iter(chunks))
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
                let mut stream = json_lines(stream::iter(chunks));
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
        let mut stream = json_lines(stream::iter(chunks));
        assert!(stream.try_next().await.unwrap().is_some());
        assert!(stream.try_next().await.is_err());
    }
}
