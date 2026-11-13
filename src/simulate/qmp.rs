//! A minimal client for the QEMU Machine Protocol (QMP).

use color_eyre::eyre::{Context, Result, bail, eyre};
use serde::Deserialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};

/// QEMU closed the QMP connection, which it does only when it exits.
#[derive(Debug)]
pub struct Disconnected;

impl std::fmt::Display for Disconnected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QEMU closed the QMP connection")
    }
}

impl std::error::Error for Disconnected {}

pub struct Qmp<R, W> {
    lines: Lines<BufReader<R>>,
    writer: W,
}

#[derive(Deserialize)]
struct Greeting {
    #[serde(rename = "QMP")]
    _qmp: IgnoredAny,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Message<T> {
    Reply(Reply<T>),
    Event {
        #[serde(rename = "event")]
        _event: IgnoredAny,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Reply<T> {
    Return(T),
    Error { desc: String },
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> Qmp<R, W> {
    /// Reads the greeting and leaves capability negotiation mode.
    pub async fn connect(reader: R, writer: W) -> Result<Self> {
        let mut qmp = Self {
            lines: BufReader::new(reader).lines(),
            writer,
        };
        let greeting = qmp.read_line().await?;
        serde_json::from_str::<Greeting>(&greeting)
            .wrap_err_with(|| format!("unexpected QMP greeting: {greeting}"))?;
        qmp.execute::<IgnoredAny>(r#"{"execute":"qmp_capabilities"}"#)
            .await?;
        Ok(qmp)
    }

    /// The host port of the user network's TCP forward to `guest_port`.
    ///
    /// QMP has no structured query for this, so read the table that
    /// `info usernet` prints. QEMU's own tests parse it the same way.
    pub async fn host_forward_port(&mut self, guest_port: u16) -> Result<u16> {
        let table: String = self
            .execute(
                r#"{"execute":"human-monitor-command","arguments":{"command-line":"info usernet"}}"#,
            )
            .await?;
        // Protocol[State] FD SourceAddress Port DestAddress Port RecvQ SendQ
        table
            .lines()
            .find_map(
                |line| match line.split_whitespace().collect::<Vec<_>>().as_slice() {
                    ["TCP[HOST_FORWARD]", _, _, host, _, guest, ..]
                        if guest.parse() == Ok(guest_port) =>
                    {
                        Some(host.parse())
                    }
                    _ => None,
                },
            )
            .ok_or_else(|| eyre!("QEMU reported no forward to guest port {guest_port}:\n{table}"))?
            .wrap_err_with(|| format!("QEMU reported an invalid host port:\n{table}"))
    }

    async fn execute<T: DeserializeOwned>(&mut self, command: &str) -> Result<T> {
        self.writer.write_all(command.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        loop {
            let line = self.read_line().await?;
            let message = serde_json::from_str(&line)
                .wrap_err_with(|| format!("unexpected QMP message: {line}"))?;
            match message {
                Message::Reply(Reply::Return(value)) => return Ok(value),
                Message::Reply(Reply::Error { desc }) => bail!("QMP command failed: {desc}"),
                Message::Event { .. } => {}
            }
        }
    }

    async fn read_line(&mut self) -> Result<String> {
        Ok(self.lines.next_line().await?.ok_or(Disconnected)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// QEMU 8.2's replies, with an event before the `info usernet` reply.
    const SESSION: &str = concat!(
        r#"{"QMP": {"version": {"qemu": {"micro": 2, "minor": 2, "major": 8}, "package": ""}, "capabilities": ["oob"]}}"#,
        "\n",
        r#"{"return": {}}"#,
        "\n",
        r#"{"timestamp": {"seconds": 1, "microseconds": 2}, "event": "NIC_RX_FILTER_CHANGED", "data": {"path": "/machine/peripheral-anon/device[1]/virtio-backend"}}"#,
        "\n",
        r#"{"return": "Hub -1 (net0):\r\n  Protocol[State]    FD  Source Address  Port   Dest. Address  Port RecvQ SendQ\r\n  TCP[HOST_FORWARD]   3       127.0.0.1 34825       10.0.2.15    22     0     0\r\n"}"#,
        "\n",
    );

    #[tokio::test]
    async fn reads_the_forwarded_port_after_negotiation() {
        let mut requests = Vec::new();
        let mut qmp = Qmp::connect(SESSION.as_bytes(), &mut requests)
            .await
            .unwrap();
        assert_eq!(qmp.host_forward_port(22).await.unwrap(), 34825);
        assert_eq!(
            String::from_utf8(requests).unwrap(),
            concat!(
                r#"{"execute":"qmp_capabilities"}"#,
                "\n",
                r#"{"execute":"human-monitor-command","arguments":{"command-line":"info usernet"}}"#,
                "\n",
            )
        );
    }

    #[tokio::test]
    async fn reports_a_missing_forward() {
        let mut qmp = Qmp::connect(SESSION.as_bytes(), Vec::new()).await.unwrap();
        let error = qmp.host_forward_port(2222).await.unwrap_err();
        assert!(
            error.to_string().contains("no forward to guest port 2222"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn reports_a_command_error() {
        let session = concat!(
            r#"{"QMP": {}}"#,
            "\n",
            r#"{"error": {"class": "CommandNotFound", "desc": "The command qmp_capabilities has not been found"}}"#,
            "\n",
        );
        let error = Qmp::connect(session.as_bytes(), Vec::new())
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("has not been found"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn end_of_stream_is_a_disconnect() {
        let greeting_only = SESSION.lines().next().unwrap();
        let error = Qmp::connect(greeting_only.as_bytes(), Vec::new())
            .await
            .err()
            .unwrap();
        assert!(error.is::<Disconnected>(), "{error:?}");
    }
}
