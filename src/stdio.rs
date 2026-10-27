//! Stdout and stderr writers that mark a closed stream.
//!
//! An `io::Error` does not record which file descriptor failed. A
//! `BrokenPipe` from stdout (`snouty runs list | head`) is a normal end of a
//! pipeline, but a `BrokenPipe` from a child's stdin or a socket is a real
//! error. These writers put a [`StreamClosed`] marker inside each
//! `BrokenPipe` error from their stream, so that the two can be told apart.

use std::fmt;
use std::io::{self, ErrorKind, IsTerminal, Write};

/// One of the process's two output streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        })
    }
}

/// The inner error of a `BrokenPipe` `io::Error` that came from writing to
/// the process's own stdout or stderr.
#[derive(Debug)]
pub struct StreamClosed(pub Stream);

impl fmt::Display for StreamClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} was closed", self.0)
    }
}

impl std::error::Error for StreamClosed {}

impl StreamClosed {
    /// The stream that `err` marks as closed, if `err` came from a
    /// [`StdWriter`].
    pub fn of(err: &io::Error) -> Option<Stream> {
        err.get_ref()?.downcast_ref::<Self>().map(|closed| closed.0)
    }
}

/// A writer to stdout or stderr. Each `BrokenPipe` error it returns holds a
/// [`StreamClosed`]; it returns every other error unchanged.
pub struct StdWriter<W> {
    inner: W,
    stream: Stream,
}

/// The process's stdout.
pub fn stdout() -> StdWriter<io::Stdout> {
    // The one place that may open stdout: all other code goes through here.
    #[allow(clippy::disallowed_methods)]
    let inner = io::stdout();
    StdWriter {
        inner,
        stream: Stream::Stdout,
    }
}

/// The process's stderr.
pub fn stderr() -> StdWriter<io::Stderr> {
    // The one place that may open stderr: all other code goes through here.
    #[allow(clippy::disallowed_methods)]
    let inner = io::stderr();
    StdWriter {
        inner,
        stream: Stream::Stderr,
    }
}

impl StdWriter<io::Stdout> {
    pub fn lock(&self) -> StdWriter<io::StdoutLock<'static>> {
        StdWriter {
            inner: self.inner.lock(),
            stream: self.stream,
        }
    }
}

impl StdWriter<io::Stderr> {
    pub fn lock(&self) -> StdWriter<io::StderrLock<'static>> {
        StdWriter {
            inner: self.inner.lock(),
            stream: self.stream,
        }
    }
}

// `IsTerminal` is sealed, so this is an inherent method.
impl<W: IsTerminal> StdWriter<W> {
    pub fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }
}

impl<W> StdWriter<W> {
    fn mark(&self, err: io::Error) -> io::Error {
        if err.kind() == ErrorKind::BrokenPipe {
            io::Error::new(ErrorKind::BrokenPipe, StreamClosed(self.stream))
        } else {
            err
        }
    }
}

impl<W: Write> Write for StdWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf).map_err(|e| self.mark(e))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush().map_err(|e| self.mark(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Failing(ErrorKind);

    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(self.0.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(self.0.into())
        }
    }

    fn writer(kind: ErrorKind) -> StdWriter<Failing> {
        StdWriter {
            inner: Failing(kind),
            stream: Stream::Stderr,
        }
    }

    #[test]
    fn broken_pipe_gets_the_marker() {
        let mut w = writer(ErrorKind::BrokenPipe);
        for err in [writeln!(w, "x").unwrap_err(), w.flush().unwrap_err()] {
            assert_eq!(err.kind(), ErrorKind::BrokenPipe);
            assert_eq!(StreamClosed::of(&err), Some(Stream::Stderr));
        }
    }

    #[test]
    fn other_errors_pass_through() {
        let err = writeln!(writer(ErrorKind::PermissionDenied), "x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(StreamClosed::of(&err), None);
    }
}
