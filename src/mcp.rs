//! `snouty mcp`: an MCP server over stdio, or over streamable HTTP in
//! stateless JSON mode. rmcp handles the protocol, and plain hyper serves the
//! HTTP transport with no axum or tower.

mod tools;

pub use tools::ToolName;

use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::io::{self, Write};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use color_eyre::eyre::{Result, WrapErr};
use http::{Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use rmcp::ErrorData;
use rmcp::model::{
    CallToolRequestMethod, ClientNotification, ClientRequest, ConstString, ProtocolVersion,
    ServerResult,
};
use rmcp::service::{
    NotificationContext, RequestContext, RoleServer, ServerInitializeError, Service, serve_server,
};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::cli::McpArgs;
use crate::features;
use crate::settings::Settings;

pub const MCP_PATH: &str = "/mcp";

/// The worker threads of the MCP server runtime. A tool call waits mostly on
/// the network, so a few threads serve many calls.
pub const WORKER_THREADS: usize = 4;

/// The largest request body the server reads. rmcp gets the same limit, so
/// the two checks agree.
const MAX_BODY_BYTES: usize = 4 << 20;

/// The wait after a failed `accept`. Without it, an error that repeats (for
/// example, no free file descriptors) makes the loop spin.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

type Mcp = StreamableHttpService<tools::Snouty, NeverSessionManager>;
type Body = BoxBody<Bytes, Infallible>;

pub async fn cmd_mcp(args: McpArgs, settings: Settings, verbose: bool) -> Result<()> {
    let server = tools::Snouty::new(settings, verbose, &features::enabled());
    if args.stdio {
        serve_stdio(server, verbose).await
    } else {
        serve_http(args, server, verbose).await
    }
}

/// Serves one client over stdin and stdout until stdin closes or a signal
/// arrives. stdout carries only MCP messages, so the request log goes to
/// stderr and nothing announces the start.
async fn serve_stdio(server: tools::Snouty, verbose: bool) -> Result<()> {
    let mut stop = StopSignals::install()?;
    let service = LoggedService {
        inner: server,
        verbose,
    };
    let running = tokio::select! {
        running = serve_server(service, rmcp::transport::stdio()) => running,
        () = stop.recv() => return Ok(()),
    };
    let running = match running {
        Ok(running) => running,
        // A client that closes stdin before `initialize` sent no requests.
        Err(ServerInitializeError::ConnectionClosed(_)) => return Ok(()),
        Err(e) => return Err(e).wrap_err("MCP initialization failed"),
    };
    // rmcp stops the service when stdin closes. A signal cancels it, which
    // also cancels the `ctx.ct` of every tool call in progress.
    let cancel = running.cancellation_token();
    let waiting = running.waiting();
    tokio::pin!(waiting);
    let quit = tokio::select! {
        quit = &mut waiting => quit,
        () = stop.recv() => {
            cancel.cancel();
            waiting.await
        }
    };
    quit.wrap_err("MCP server failed")?;
    Ok(())
}

/// Ctrl-C, SIGTERM and SIGHUP, which all stop the server. Install them
/// before the server can get requests: without a handler, a signal kills the
/// process.
struct StopSignals {
    interrupt: Signal,
    terminate: Signal,
    hangup: Signal,
}

impl StopSignals {
    fn install() -> io::Result<Self> {
        Ok(StopSignals {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
            hangup: signal(SignalKind::hangup())?,
        })
    }

    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
            _ = self.hangup.recv() => {}
        }
    }
}

async fn serve_http(args: McpArgs, server: tools::Snouty, verbose: bool) -> Result<()> {
    let listener = TcpListener::bind((args.host, args.port))
        .await
        .wrap_err_with(|| format!("failed to listen on {}:{}", args.host, args.port))?;
    let addr = listener.local_addr()?;

    // A client can send a signal as soon as it reads `Listening on`.
    let mut stop = StopSignals::install()?;

    let mut allowed_hosts = vec![addr.to_string()];
    // A Host header with no port means port 80, and clients leave that port
    // out. rmcp lets an entry with no port match every port.
    if addr.port() == 80 {
        allowed_hosts.push(match addr.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => format!("[{ip}]"),
        });
    }
    allowed_hosts.extend(args.allowed_hosts.iter().map(ToString::to_string));

    let shutdown = CancellationToken::new();
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_allowed_hosts(allowed_hosts)
        // With no allowed origins, rmcp refuses every request that has an
        // Origin header. A request with no Origin header is not a browser
        // request, and it passes.
        .enforce_origin_validation()
        .with_max_request_body_bytes(MAX_BODY_BYTES)
        .with_cancellation_token(shutdown.child_token());
    let mcp: Mcp = StreamableHttpService::new(
        move || Ok(server.clone()),
        Arc::new(NeverSessionManager::default()),
        config,
    );

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "Listening on {addr}")
        .and_then(|()| stdout.flush())
        .wrap_err("failed to write to stdout")?;
    drop(stdout);

    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            () = stop.recv() => break,
            // Reap finished connections, so the set does not grow without
            // bound.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let mcp = mcp.clone();
                    let service = service_fn(move |req| route(mcp.clone(), verbose, req));
                    conns.spawn(async move {
                        // Without a timer, hyper does not enforce its header
                        // read timeout, and a client that sends part of a
                        // request keeps the connection open.
                        if let Err(e) = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .serve_connection(TokioIo::new(stream), service)
                            .await
                        {
                            log::debug!("connection error: {e}");
                        }
                    });
                }
                Err(e) => {
                    // The default log filter shows only errors, and this
                    // error can repeat with no other sign.
                    let _ = writeln!(io::stderr().lock(), "Warning: accept failed: {e}");
                    tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                }
            },
        }
    }

    // Abort the connections first, so that each request in progress logs
    // `disconnected`. A cancel first lets a call answer with an error on
    // another worker thread before its connection is aborted. rmcp runs each
    // tool call in its own task, and the abort cancels the call's `ctx.ct`.
    // Then `main` exits, which ends any task that is left.
    conns.shutdown().await;
    shutdown.cancel();
    Ok(())
}

/// Serves one HTTP request: only `MCP_PATH` goes to rmcp. Every request
/// writes one log line.
async fn route(
    mcp: Mcp,
    verbose: bool,
    req: Request<Incoming>,
) -> Result<Response<Body>, Infallible> {
    let (parts, body) = req.into_parts();
    let mut log = PendingLog {
        entry: RequestLog {
            method: LoggedMethod::Http(parts.method.clone()),
            tool: None,
            params: None,
            outcome: Outcome::Disconnected,
        },
        verbose,
    };

    if parts.uri.path() != MCP_PATH {
        return Ok(log.respond(plain_response(StatusCode::NOT_FOUND, "Not Found")));
    }

    let bytes = match Limited::new(body, MAX_BODY_BYTES).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) if e.is::<LengthLimitError>() => {
            return Ok(log.respond(plain_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Payload Too Large",
            )));
        }
        Err(e) => {
            log::debug!("failed to read the request body: {e}");
            return Ok(log.respond(plain_response(StatusCode::BAD_REQUEST, "Bad Request")));
        }
    };

    // A body that is not one JSON-RPC message keeps the HTTP method in the
    // log; rmcp gives the error response.
    if parts.method == Method::POST
        && let Ok(request) = serde_json::from_slice::<RpcRequest<NamedParams>>(&bytes)
    {
        if request.method == CallToolRequestMethod::VALUE {
            log.entry.tool = request.params.and_then(|params| params.name);
        }
        log.entry.method = LoggedMethod::Rpc(request.method);
        // Only a verbose line shows the params, so only then parse them whole.
        if verbose {
            log.entry.params = serde_json::from_slice::<RpcRequest<Value>>(&bytes)
                .ok()
                .and_then(|request| request.params);
        }
    }

    let response = mcp
        .handle(Request::from_parts(parts, Full::new(bytes)))
        .await;
    // In JSON response mode, the body is one complete JSON value, so read it
    // whole to find the outcome of the call.
    let (parts, body) = response.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(never) => match never {},
    };
    let failure = Failure::of(&bytes);
    log.entry.outcome = Outcome::Sent {
        status: parts.status,
        failure,
    };
    Ok(Response::from_parts(parts, Full::new(bytes).boxed()))
}

fn plain_response(status: StatusCode, text: &'static str) -> Response<Body> {
    let mut response = Response::new(Full::new(Bytes::from_static(text.as_bytes())).boxed());
    *response.status_mut() = status;
    response
}

/// A request log line that is written when it drops. If the request future
/// drops before a response (the client went away, or the server stopped),
/// the line says `disconnected`.
struct PendingLog {
    entry: RequestLog,
    verbose: bool,
}

impl PendingLog {
    fn respond(&mut self, response: Response<Body>) -> Response<Body> {
        self.entry.outcome = Outcome::Sent {
            status: response.status(),
            failure: None,
        };
        response
    }
}

impl Drop for PendingLog {
    fn drop(&mut self) {
        let line = self.entry.line(Utc::now(), self.verbose);
        // The log must not stop the server: a closed stderr loses the line.
        let _ = writeln!(io::stderr().lock(), "{line}");
    }
}

/// The parts of a JSON-RPC request that the log reads. serde skips the
/// fields that `P` does not name.
#[derive(Deserialize)]
struct RpcRequest<P> {
    method: String,
    params: Option<P>,
}

/// The tool name in the params of a `tools/call`.
#[derive(Deserialize)]
struct NamedParams {
    name: Option<String>,
}

/// The parts of a JSON-RPC response that the log reads. serde skips the
/// other fields, so the tool output is not copied again.
#[derive(Deserialize)]
struct RpcResponse {
    error: Option<RpcError>,
    result: Option<ToolResultFlag>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
}

#[derive(Deserialize)]
struct ToolResultFlag {
    #[serde(default, rename = "isError")]
    is_error: bool,
}

/// The method that a request log line names.
enum LoggedMethod {
    Rpc(String),
    /// The HTTP method, for a body that is not a JSON-RPC request.
    Http(Method),
}

impl fmt::Display for LoggedMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoggedMethod::Rpc(method) => f.write_str(method),
            LoggedMethod::Http(method) => method.fmt(f),
        }
    }
}

struct RequestLog {
    method: LoggedMethod,
    /// The tool name of a `tools/call`.
    tool: Option<String>,
    /// The JSON-RPC params. The line shows them only with `--verbose`.
    params: Option<Value>,
    outcome: Outcome,
}

enum Outcome {
    /// The server sent a response with this status.
    Sent {
        status: StatusCode,
        failure: Option<Failure>,
    },
    /// The request stopped before a response.
    Disconnected,
    /// The stdio transport handled the message.
    Handled { failure: Option<Failure> },
    /// The stdio transport dropped the request before a response: the
    /// client cancelled it, or the server stopped.
    Cancelled,
}

/// How a request that got an HTTP response failed. Both travel as HTTP 200.
#[derive(Debug, PartialEq)]
enum Failure {
    /// A tool result with `isError`.
    ToolError,
    /// A JSON-RPC error response, with its code.
    Rpc(i64),
}

impl From<&ErrorData> for Failure {
    fn from(error: &ErrorData) -> Self {
        Failure::Rpc(error.code.0.into())
    }
}

impl Failure {
    /// Reads the failure from a response body. A body that is not a
    /// JSON-RPC response has none.
    fn of(body: &[u8]) -> Option<Failure> {
        let response: RpcResponse = serde_json::from_slice(body).ok()?;
        match response {
            RpcResponse {
                error: Some(RpcError { code }),
                ..
            } => Some(Failure::Rpc(code)),
            RpcResponse {
                result: Some(ToolResultFlag { is_error: true }),
                ..
            } => Some(Failure::ToolError),
            _ => None,
        }
    }
}

impl RequestLog {
    /// `<time> <status> <method>[ <tool>][ isError | error <code>][ <params>]`,
    /// with `-` for the status and a `disconnected` marker when no response
    /// was sent. The stdio transport has no status, so its lines leave it out.
    fn line(&self, now: DateTime<Utc>, verbose: bool) -> String {
        let time = now.to_rfc3339_opts(SecondsFormat::Millis, true);
        let mut line = match &self.outcome {
            Outcome::Sent { status, .. } => format!("{time} {} {}", status.as_u16(), self.method),
            Outcome::Disconnected => format!("{time} - {}", self.method),
            Outcome::Handled { .. } | Outcome::Cancelled => format!("{time} {}", self.method),
        };
        if let Some(tool) = &self.tool {
            line.push(' ');
            line.push_str(tool);
        }
        match &self.outcome {
            Outcome::Sent { failure, .. } | Outcome::Handled { failure } => match failure {
                Some(Failure::ToolError) => line.push_str(" isError"),
                Some(Failure::Rpc(code)) => line.push_str(&format!(" error {code}")),
                None => {}
            },
            Outcome::Disconnected => line.push_str(" disconnected"),
            Outcome::Cancelled => line.push_str(" cancelled"),
        }
        if verbose && let Some(params) = &self.params {
            line.push(' ');
            line.push_str(&params.to_string());
        }
        line
    }
}

/// Writes one request log line on stderr for each message that the stdio
/// transport receives. The HTTP transport logs in `route` instead, where the
/// HTTP status is known.
struct LoggedService {
    inner: tools::Snouty,
    verbose: bool,
}

impl LoggedService {
    /// `message` serializes as `{"method": …, "params": …}`. It is read only
    /// for the params of a verbose line, or for the method of a
    /// notification, which has no typed accessor.
    fn pending(
        &self,
        method: Option<&str>,
        tool: Option<String>,
        message: impl Serialize,
    ) -> PendingLog {
        let json = (self.verbose || method.is_none())
            .then(|| serde_json::to_value(message).ok())
            .flatten()
            .and_then(|json| RpcRequest::<Value>::deserialize(json).ok());
        let (json_method, params) = json.map_or((None, None), |r| (Some(r.method), r.params));
        PendingLog {
            entry: RequestLog {
                method: LoggedMethod::Rpc(
                    method
                        .map(str::to_owned)
                        .or(json_method)
                        .unwrap_or_default(),
                ),
                tool,
                params: params.filter(|_| self.verbose),
                outcome: Outcome::Cancelled,
            },
            verbose: self.verbose,
        }
    }
}

impl Service<RoleServer> for LoggedService {
    async fn handle_request(
        &self,
        request: ClientRequest,
        context: RequestContext<RoleServer>,
    ) -> Result<ServerResult, ErrorData> {
        let tool = match &request {
            ClientRequest::CallToolRequest(r) => Some(r.params.name.to_string()),
            _ => None,
        };
        let mut log = self.pending(Some(request.method()), tool, &request);
        let result = self.inner.handle_request(request, context).await;
        let failure = match &result {
            Err(e) => Some(e.into()),
            Ok(ServerResult::CallToolResult(r)) if r.is_error == Some(true) => {
                Some(Failure::ToolError)
            }
            Ok(_) => None,
        };
        log.entry.outcome = Outcome::Handled { failure };
        result
    }

    async fn handle_notification(
        &self,
        notification: ClientNotification,
        context: NotificationContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let mut log = self.pending(None, None, &notification);
        let result = self.inner.handle_notification(notification, context).await;
        log.entry.outcome = Outcome::Handled {
            failure: result.as_ref().err().map(Failure::from),
        };
        result
    }

    fn get_info(&self) -> rmcp::model::ServerConfig {
        self.inner.get_info()
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Service::supported_protocol_versions(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators;
    use serde_json::json;

    fn time() -> DateTime<Utc> {
        "2026-09-30T22:52:39.438Z".parse().unwrap()
    }

    fn call(outcome: Outcome) -> RequestLog {
        RequestLog {
            method: LoggedMethod::Rpc(CallToolRequestMethod::VALUE.to_owned()),
            tool: Some("runs_show".to_owned()),
            params: Some(json!({"name": "runs_show", "arguments": {"run_id": "abc"}})),
            outcome,
        }
    }

    #[test]
    fn log_line_formats() {
        let ok = call(Outcome::Sent {
            status: StatusCode::OK,
            failure: None,
        });
        assert_eq!(
            ok.line(time(), false),
            "2026-09-30T22:52:39.438Z 200 tools/call runs_show"
        );
        assert_eq!(
            ok.line(time(), true),
            r#"2026-09-30T22:52:39.438Z 200 tools/call runs_show {"name":"runs_show","arguments":{"run_id":"abc"}}"#
        );
        let tool_error = call(Outcome::Sent {
            status: StatusCode::OK,
            failure: Some(Failure::ToolError),
        });
        assert_eq!(
            tool_error.line(time(), false),
            "2026-09-30T22:52:39.438Z 200 tools/call runs_show isError"
        );
        let rpc_error = call(Outcome::Sent {
            status: StatusCode::OK,
            failure: Some(Failure::Rpc(-32602)),
        });
        assert_eq!(
            rpc_error.line(time(), false),
            "2026-09-30T22:52:39.438Z 200 tools/call runs_show error -32602"
        );
        assert_eq!(
            call(Outcome::Disconnected).line(time(), false),
            "2026-09-30T22:52:39.438Z - tools/call runs_show disconnected"
        );
        // The stdio transport has no HTTP status.
        let stdio_error = call(Outcome::Handled {
            failure: Some(Failure::ToolError),
        });
        assert_eq!(
            stdio_error.line(time(), false),
            "2026-09-30T22:52:39.438Z tools/call runs_show isError"
        );
        assert_eq!(
            call(Outcome::Cancelled).line(time(), false),
            "2026-09-30T22:52:39.438Z tools/call runs_show cancelled"
        );
        let refused = RequestLog {
            method: LoggedMethod::Http(Method::GET),
            tool: None,
            params: None,
            outcome: Outcome::Sent {
                status: StatusCode::FORBIDDEN,
                failure: None,
            },
        };
        assert_eq!(
            refused.line(time(), true),
            "2026-09-30T22:52:39.438Z 403 GET"
        );
    }

    #[test]
    fn failure_reads_the_response_message() {
        let failure = |message: Value| Failure::of(message.to_string().as_bytes());
        assert_eq!(
            failure(json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "x"}})),
            Some(Failure::Rpc(-32602))
        );
        assert_eq!(
            failure(json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [], "isError": true}})),
            Some(Failure::ToolError)
        );
        assert_eq!(
            failure(
                json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [], "isError": false}})
            ),
            None
        );
        assert_eq!(
            failure(json!({"jsonrpc": "2.0", "id": 1, "result": {}})),
            None
        );
        assert_eq!(Failure::of(b""), None);
    }

    #[hegel::test]
    fn log_line_shows_params_only_when_verbose(tc: hegel::TestCase) {
        let value = tc.draw(generators::text());
        let verbose = tc.draw(generators::booleans());
        let params = json!({"arguments": {"run_id": value}});
        let shown = params.to_string();
        let entry = RequestLog {
            method: LoggedMethod::Rpc(CallToolRequestMethod::VALUE.to_owned()),
            tool: Some("runs_show".to_owned()),
            params: Some(params),
            outcome: Outcome::Sent {
                status: StatusCode::OK,
                failure: None,
            },
        };
        assert_eq!(entry.line(time(), verbose).contains(&shown), verbose);
    }
}
