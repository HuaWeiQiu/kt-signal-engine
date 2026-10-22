use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use std::future::Future;

use bytes::Bytes;
use futures::channel::oneshot::Canceled;
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use reqwest::Method;
use reqwest_websocket::{CloseCode, Message as WsMessage, WebSocket};
use tokio::sync::Notify;
use tokio::time::{timeout, Instant};
use tracing::debug;

use crate::configuration::SignalServers;
use crate::prelude::PushService;
use crate::proto::{
    web_socket_message, WebSocketMessage, WebSocketRequestMessage,
    WebSocketResponseMessage,
};
use crate::push_service::{ServiceError, SignalServiceResponse};

pub mod account;
#[cfg(feature = "cdsi")]
pub mod directory;
pub mod keys;
pub mod linking;
pub mod profile;
pub mod registration;
mod request;
mod sender;
pub mod slot;
pub mod stickers;
mod usernames;

/// Close code the Signal server sends when it drops a device websocket
/// because another connection for the same device was established.
/// KT (single-connection): the 4409 branch decision consumes this literal.
pub const CLOSE_CODE_CONNECTED_ELSEWHERE: u16 = 4409;

/// Synthetic application close code recorded when the local single-connection
/// slot replaces this connection (kill-before-create handover). It is never
/// sent on the wire; the receive owner treats it like 1000/4409 (do not
/// reconnect), otherwise the replaced pipeline would fight the new one.
pub const CLOSE_CODE_LOCAL_HANDOVER: u16 = 4000;

/// Close the websocket when the last successful keepalive response is at
/// least this old, instead of sending on a dead link (machine
/// suspend/wake). Official Desktop
/// `WebsocketResources.preload.ts:508` `STALE_THRESHOLD_MS`.
pub const STALE_KEEPALIVE_THRESHOLD: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

/// Application keepalive cadence (official Desktop 30s).
const KEEPALIVE_INTERVAL: Duration =
    crate::push_service::KEEPALIVE_TIMEOUT_SECONDS;

/// A keepalive request unanswered for this long closes the link instead of
/// waiting for the next tick. Official `WebsocketResources.preload.ts`
/// gives every request a 30s response deadline and closes with 3001.
const KEEPALIVE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Any application request unanswered for this long closes the link. This is
/// what wakes a receive task suspended on a request after the transport
/// silently died (the 2026-10-21 silent-receive incident).
const REQUEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A single websocket frame write that cannot complete within this budget is
/// a dead transport. Without it a half-open socket can block the I/O pump
/// (and every timer in it) forever.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on the close handshake: never let a dead socket keep the task alive.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Watchdog granularity. Keeping it independent of the keepalive cadence means
/// a due deadline is acted on promptly instead of on the next 30s tick.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);

/// Close code the official client uses when a request/keepalive deadline
/// expires (`WebsocketResources.preload.ts`; Signal-Android 3001).
const CLOSE_CODE_TIMEOUT: u16 = 3001;

/// Official `isOlderThan(lastAliveAt, STALE_THRESHOLD_MS)` — an elapsed
/// time at or beyond the threshold is stale.
fn keepalive_is_stale(last_alive_at: Instant, now: Instant) -> bool {
    now.duration_since(last_alive_at) >= STALE_KEEPALIVE_THRESHOLD
}

/// Official per-request response deadline (30s). Separate helper so the
/// watchdog and the tests share one definition.
fn request_is_overdue(sent_at: Instant, now: Instant) -> bool {
    now.duration_since(sent_at) >= REQUEST_RESPONSE_TIMEOUT
}

/// What the keepalive watchdog wants to do on this tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeepAliveStep {
    /// No deadline is due; carry on.
    Wait,
    /// It is time to send the next keepalive.
    Send,
    /// No successful keepalive for five minutes (suspend/wake).
    CloseStale,
    /// The in-flight keepalive has been unanswered for 30s.
    CloseUnanswered,
    /// An application request has been unanswered for 30s.
    CloseRequestTimeout,
    /// Writing the keepalive to the socket failed or timed out.
    CloseSendFailed,
}

impl KeepAliveStep {
    /// Every close step must tear the connection down so the receive stream
    /// ends and the owner reconnects. Send/Wait never close.
    fn is_close(self) -> bool {
        matches!(
            self,
            KeepAliveStep::CloseStale
                | KeepAliveStep::CloseUnanswered
                | KeepAliveStep::CloseRequestTimeout
                | KeepAliveStep::CloseSendFailed
        )
    }

    fn close_reason(self) -> &'static str {
        match self {
            KeepAliveStep::CloseStale => "stale keepalive state",
            KeepAliveStep::CloseUnanswered => "keepalive response timeout",
            KeepAliveStep::CloseRequestTimeout => "request response timeout",
            KeepAliveStep::CloseSendFailed => "keepalive send failure",
            KeepAliveStep::Wait | KeepAliveStep::Send => "not a close step",
        }
    }
}

/// Outcome of one keepalive frame write, classified so the close decision is
/// unit-testable without a live socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeepAliveSendOutcome {
    Sent,
    Failed,
    TimedOut,
}

/// Why a frame write did not complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WsWriteError {
    Failed,
    TimedOut,
}

/// Keepalive timing state. All transitions are pure so tests can pin the
/// official cadence: 30s tick, 30s response deadline, 5min stale threshold,
/// and "a successful response restarts the 30s clock".
#[derive(Debug)]
struct KeepAliveState {
    last_alive_at: Instant,
    next_send_at: Instant,
    in_flight: Option<(u64, Instant)>,
}

impl KeepAliveState {
    fn new(now: Instant) -> Self {
        Self {
            last_alive_at: now,
            next_send_at: now,
            in_flight: None,
        }
    }

    fn on_sent(&mut self, now: Instant, id: u64) {
        self.in_flight = Some((id, now));
        self.next_send_at = now + KEEPALIVE_INTERVAL;
    }

    fn on_response(&mut self, now: Instant) {
        self.last_alive_at = now;
        self.in_flight = None;
        self.next_send_at = now + KEEPALIVE_INTERVAL;
    }

    /// Classify the write result. A failed or timed-out write is link death,
    /// exactly like an unanswered request; the caller closes with 3001.
    fn on_send_result(
        &mut self,
        outcome: KeepAliveSendOutcome,
    ) -> KeepAliveStep {
        match outcome {
            KeepAliveSendOutcome::Sent => KeepAliveStep::Wait,
            KeepAliveSendOutcome::Failed | KeepAliveSendOutcome::TimedOut => {
                KeepAliveStep::CloseSendFailed
            },
        }
    }

    fn in_flight_id(&self) -> Option<u64> {
        self.in_flight.map(|(id, _)| id)
    }

    fn step(&self, now: Instant) -> KeepAliveStep {
        if keepalive_is_stale(self.last_alive_at, now) {
            return KeepAliveStep::CloseStale;
        }
        if let Some((_, sent_at)) = self.in_flight {
            if now.duration_since(sent_at) >= KEEPALIVE_RESPONSE_TIMEOUT {
                return KeepAliveStep::CloseUnanswered;
            }
            return KeepAliveStep::Wait;
        }
        if now >= self.next_send_at {
            return KeepAliveStep::Send;
        }
        KeepAliveStep::Wait
    }
}

pub use request::WebSocketRequestMessageBuilder;

type RequestStreamItem = (
    WebSocketRequestMessage,
    oneshot::Sender<WebSocketResponseMessage>,
);

pub struct SignalRequestStream {
    inner: mpsc::UnboundedReceiver<RequestStreamItem>,
}

impl Stream for SignalRequestStream {
    type Item = RequestStreamItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = &mut self.inner;
        futures::pin_mut!(inner);
        Stream::poll_next(inner, cx)
    }
}

#[derive(Debug, Clone)]
pub struct Identified;

#[derive(Debug, Clone)]
pub struct Unidentified;

pub trait WebSocketType: 'static {}

impl WebSocketType for Identified {}

impl WebSocketType for Unidentified {}

/// A dispatching web socket client for the Signal web socket API.
///
/// This structure can be freely cloned, since this acts as a *facade* for multiple entry and exit
/// points.
pub struct SignalWebSocket<C: WebSocketType> {
    _type: PhantomData<C>,
    // XXX: at the end of the migration, this should be CDN operations only
    pub(crate) unidentified_push_service: PushService,
    inner: Arc<Mutex<SignalWebSocketInner>>,
    request_sink: mpsc::Sender<(
        WebSocketRequestMessage,
        oneshot::Sender<Result<WebSocketResponseMessage, ServiceError>>,
    )>,
    kill: Arc<Notify>,
}

// Manual impl (no `C: Clone` bound): every field is cloneable for any `C`.
// KT (single-connection): the generic slot registry clones through `C`.
impl<C: WebSocketType> Clone for SignalWebSocket<C> {
    fn clone(&self) -> Self {
        Self {
            _type: PhantomData,
            unidentified_push_service: self.unidentified_push_service.clone(),
            inner: Arc::clone(&self.inner),
            request_sink: self.request_sink.clone(),
            kill: Arc::clone(&self.kill),
        }
    }
}

struct SignalWebSocketInner {
    stream: Option<SignalRequestStream>,
    // KT (single-connection): close code of the last server close frame, for
    // the application-side 4409 branch decision.
    last_close_code: Option<u16>,
}

struct SignalWebSocketProcess {
    /// Whether to enable keep-alive or not (and send a request to this path)
    keep_alive_path: String,

    /// Receives requests from the application, which we forward to Signal.
    requests: mpsc::Receiver<(
        WebSocketRequestMessage,
        oneshot::Sender<Result<WebSocketResponseMessage, ServiceError>>,
    )>,
    /// Signal's requests should go in here, to be delivered to the application.
    request_sink: mpsc::UnboundedSender<RequestStreamItem>,

    /// Application requests awaiting a server response, keyed by request id,
    /// with the instant they were written. The watchdog closes the link when
    /// one exceeds `REQUEST_RESPONSE_TIMEOUT` (official per-request 30s).
    outgoing_requests: HashMap<
        u64,
        (
            oneshot::Sender<Result<WebSocketResponseMessage, ServiceError>>,
            Instant,
        ),
    >,

    /// Keepalive cadence/response deadlines and the last successful response.
    keepalive: KeepAliveState,

    outgoing_responses: FuturesUnordered<
        BoxFuture<'static, Result<WebSocketResponseMessage, Canceled>>,
    >,

    ws: WebSocket,

    // KT (single-connection): shared facade state (close-code bookkeeping)
    // and the termination signal for the kill-before-create handover.
    shared: Arc<Mutex<SignalWebSocketInner>>,
    kill: Arc<Notify>,
}

impl SignalWebSocketProcess {
    async fn process_frame(
        &mut self,
        frame: Bytes,
    ) -> Result<(), ServiceError> {
        use prost::Message;
        let msg = WebSocketMessage::decode(frame)?;
        if let Some(request) = &msg.request {
            tracing::trace!(
                msg_type =? msg.r#type(),
                request.id,
                request.verb,
                request.path,
                request_body_size_bytes = request.body.as_ref().map(|x| x.len()).unwrap_or(0),
                ?request.headers,
                "decoded WebSocketMessage request"
            );
        } else if let Some(response) = &msg.response {
            tracing::trace!(
                msg_type =? msg.r#type(),
                response.status,
                response.message,
                response_body_size_bytes = response.body.as_ref().map(|x| x.len()).unwrap_or(0),
                ?response.headers,
                response.id,
                "decoded WebSocketMessage response"
            );
        } else {
            tracing::debug!("decoded {msg:?}");
        }

        use web_socket_message::Type;
        match (msg.r#type(), msg.request, msg.response) {
            (Type::Unknown, _, _) => Err(ServiceError::InvalidFrame {
                reason: "unknown frame type",
            }),
            (Type::Request, Some(request), _) => {
                let (sink, recv) = oneshot::channel();
                tracing::trace!("sending request with body");
                self.request_sink.send((request, sink)).await.map_err(
                    |_| ServiceError::WsClosing {
                        reason: "request handler failed",
                    },
                )?;
                self.outgoing_responses.push(Box::pin(recv));

                Ok(())
            },
            (Type::Request, None, _) => Err(ServiceError::InvalidFrame {
                reason: "type was request, but does not contain request",
            }),
            (Type::Response, _, Some(response)) => {
                if let Some(id) = response.id {
                    if let Some((responder, _sent_at)) =
                        self.outgoing_requests.remove(&id)
                    {
                        if let Err(e) = responder.send(Ok(response)) {
                            tracing::warn!(
                                "Could not deliver response for id {}: {:?}",
                                id,
                                e
                            );
                        }
                    } else if self.keepalive.in_flight_id() == Some(id) {
                        let status = reqwest::StatusCode::from_u16(
                            response.status() as _,
                        )
                        .map_err(|e| {
                            ServiceError::IO(std::io::Error::other(format!(
                                "invalid http status code {} - {e}",
                                response.status()
                            )))
                        })?;
                        if !status.is_success() {
                            tracing::warn!(
                                %status,
                                "response code for keep-alive not successful"
                            );
                            return Err(ServiceError::UnhandledResponseCode {
                                status,
                                body: String::from_utf8_lossy(response.body())
                                    .into_owned(),
                            });
                        }
                        // Official: a successful response means the link is
                        // alive *now* and restarts the 30s keepalive clock.
                        self.keepalive.on_response(Instant::now());
                    } else {
                        tracing::warn!(
                            ?response,
                            "response for non existing request"
                        );
                    }
                }

                Ok(())
            },
            (Type::Response, _, None) => Err(ServiceError::InvalidFrame {
                reason: "type was response, but does not contain response",
            }),
        }
    }

    fn next_request_id(&self) -> u64 {
        use rand::Rng;
        let mut rng = rand::rng();
        loop {
            let id = rng.random();
            if !self.outgoing_requests.contains_key(&id)
                && self.keepalive.in_flight_id() != Some(id)
            {
                return id;
            }
        }
    }

    /// Write one frame with a hard deadline. A half-open transport can make
    /// `Sink::send` wait forever; without this the pump (and the keepalive
    /// watchdog living in it) would starve and the receive stream would go
    /// silent with no log line.
    async fn send_frame(
        &mut self,
        message: WebSocketMessage,
        stage: &'static str,
    ) -> Result<(), WsWriteError> {
        use prost::Message;
        let buffer = message.encode_to_vec();
        match timeout(
            SEND_TIMEOUT,
            self.ws.send(WsMessage::Binary(buffer.into())),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                tracing::warn!(stage, %error, "websocket frame write failed");
                Err(WsWriteError::Failed)
            },
            Err(_) => {
                tracing::warn!(stage, "websocket frame write timed out");
                Err(WsWriteError::TimedOut)
            },
        }
    }

    /// Tear the link down after a detected death. The close handshake is
    /// bounded so a dead socket cannot keep this task (and therefore the
    /// receive stream) alive; dropping the request responders wakes every
    /// application request suspended on the link.
    async fn close_link(&mut self, reason: &'static str) {
        tracing::warn!(
            cause = reason,
            code = CLOSE_CODE_TIMEOUT,
            "websocket link is dead; closing so the receive stream reconnects"
        );
        // `WebSocket::close` consumes the socket, so send the official 3001
        // close frame through the sink and then close it; both steps are
        // bounded so a dead transport cannot wedge this task.
        let _ = timeout(CLOSE_TIMEOUT, async {
            let _ = self
                .ws
                .send(WsMessage::Close {
                    code: CloseCode::Library(CLOSE_CODE_TIMEOUT),
                    reason: reason.to_owned(),
                })
                .await;
            let _ = futures::SinkExt::close(&mut self.ws).await;
        })
        .await;
        self.keepalive.in_flight = None;
        self.outgoing_requests.clear();
    }

    async fn run(mut self) -> Result<(), ServiceError> {
        let mut watchdog = tokio::time::interval(WATCHDOG_INTERVAL);
        watchdog
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        watchdog.tick().await;

        loop {
            futures::select! {
                _ = watchdog.tick().fuse() => {
                    let now = Instant::now();
                    // Per-request deadline first: a receive task abandoned on
                    // a request must be woken even while keepalives still
                    // succeed. Official per-request timeout is 30s.
                    let step = if self
                        .outgoing_requests
                        .values()
                        .any(|(_, sent_at)| request_is_overdue(*sent_at, now))
                    {
                        KeepAliveStep::CloseRequestTimeout
                    } else {
                        self.keepalive.step(now)
                    };
                    match step {
                        KeepAliveStep::Wait => {}
                        KeepAliveStep::Send => {
                            let id = self.next_request_id();
                            self.keepalive.on_sent(now, id);
                            let request = WebSocketRequestMessage::new(Method::GET)
                                .id(id)
                                .path(&self.keep_alive_path)
                                .build();
                            let message = WebSocketMessage {
                                r#type: Some(web_socket_message::Type::Request.into()),
                                request: Some(request),
                                ..Default::default()
                            };
                            let outcome = match self.send_frame(message, "keepalive").await {
                                Ok(()) => KeepAliveSendOutcome::Sent,
                                Err(WsWriteError::TimedOut) => KeepAliveSendOutcome::TimedOut,
                                Err(WsWriteError::Failed) => KeepAliveSendOutcome::Failed,
                            };
                            let after_write = self.keepalive.on_send_result(outcome);
                            if after_write.is_close() {
                                self.close_link(after_write.close_reason()).await;
                                break;
                            }
                        }
                        close => {
                            debug_assert!(close.is_close());
                            self.close_link(close.close_reason()).await;
                            break;
                        }
                    }
                },
                // Process requests from the application, forward them to Signal
                x = self.requests.next() => {
                    match x {
                        Some((mut request, responder)) => {
                            // Regenerate ID if already in the table
                            request.id = Some(
                                request
                                    .id
                                    .filter(|x| {
                                        !self.outgoing_requests.contains_key(x)
                                            && self.keepalive.in_flight_id() != Some(*x)
                                    })
                                    .unwrap_or_else(|| self.next_request_id()),
                            );
                            tracing::trace!(
                                request.id,
                                request.verb,
                                request.path,
                                request_body_size_bytes = request.body.as_ref().map(|x| x.len()),
                                ?request.headers,
                                "sending WebSocketRequestMessage",
                            );

                            let id = request.id.unwrap();
                            self.outgoing_requests
                                .insert(id, (responder, Instant::now()));
                            let message = WebSocketMessage {
                                r#type: Some(web_socket_message::Type::Request.into()),
                                request: Some(request),
                                ..Default::default()
                            };
                            if let Err(error) = self.send_frame(message, "request").await {
                                // The responder is still in the map and is
                                // dropped by close_link, waking the caller with
                                // WsClosing instead of leaving it suspended.
                                tracing::warn!(error = ?error, "application websocket write failed");
                                self.close_link("application request send failure").await;
                                break;
                            }
                        }
                        None => {
                            debug!("end of application request stream; websocket closing");
                            return Ok(());
                        }
                    }
                }
                // KT (single-connection): discipline handover — a dial for the
                // same account replaced this connection; close frame then exit.
                _ = self.kill.notified().fuse() => {
                    tracing::warn!("websocket terminating: close requested by local single-connection discipline");
                    // Record why this connection ended: the receive owner must
                    // not reconnect a link that a newer local dial replaced.
                    self.shared.lock().unwrap().last_close_code =
                        Some(CLOSE_CODE_LOCAL_HANDOVER);
                    let _ = timeout(
                        CLOSE_TIMEOUT,
                        self.ws.send(WsMessage::Close {
                            code: CloseCode::Away,
                            reason: String::new(),
                        }),
                    )
                    .await;
                    self.outgoing_requests.clear();
                    break;
                }
                // Incoming websocket message
                web_socket_item = self.ws.next().fuse() => {
                    use reqwest_websocket::Message;
                    match web_socket_item {
                        Some(Ok(Message::Close { code, reason })) => {
                            let code = u16::from(code);
                            tracing::warn!(%code, reason, "websocket closed");
                            if code == CLOSE_CODE_CONNECTED_ELSEWHERE {
                                tracing::warn!("websocket closed with 4409 (connected elsewhere): another client took over this device connection"); // KT (single-connection)
                            }
                            self.shared.lock().unwrap().last_close_code = Some(code);
                            break;
                        },
                        Some(Ok(Message::Binary(frame))) => {
                            self.process_frame(frame).await?;
                        }
                        Some(Ok(Message::Ping(_))) => {
                            tracing::trace!("received ping");
                        }
                        Some(Ok(Message::Pong(_))) => {
                            tracing::trace!("received pong");
                        }
                        Some(Ok(Message::Text(_))) => {
                            tracing::trace!("received text (unsupported, skipping)");
                        }
                        Some(Err(e)) => {
                            tracing::warn!(%e, "websocket read failed; link is dead");
                            return Err(e.into());
                        }
                        None => {
                            tracing::warn!("websocket stream ended; link is dead");
                            return Err(ServiceError::WsClosing {
                                reason: "end of web request stream; socket closing"
                            });
                        }
                    }
                }
                response = self.outgoing_responses.next() => {
                    match response {
                        Some(Ok(response)) => {
                            tracing::trace!("sending response {:?}", response);

                            let message = WebSocketMessage {
                                r#type: Some(web_socket_message::Type::Response.into()),
                                response: Some(response),
                                ..Default::default()
                            };
                            if let Err(error) = self.send_frame(message, "response").await {
                                tracing::warn!(error = ?error, "application response write failed");
                                self.close_link("application response send failure").await;
                                break;
                            }
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, "could not generate response to a Signal request; responder was canceled. continuing.");
                        }
                        None => {
                            unreachable!("outgoing responses should never fuse")
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<C: WebSocketType> SignalWebSocket<C> {
    fn inner_locked(&self) -> MutexGuard<'_, SignalWebSocketInner> {
        self.inner.lock().unwrap()
    }

    pub fn new(
        ws: WebSocket,
        keep_alive_path: String,
        unidentified_push_service: PushService,
    ) -> (Self, impl Future<Output = ()>) {
        // Create process
        let (incoming_request_sink, incoming_request_stream) =
            mpsc::unbounded();
        let (outgoing_request_sink, outgoing_requests) = mpsc::channel(1);

        let inner = Arc::new(Mutex::new(SignalWebSocketInner {
            stream: Some(SignalRequestStream {
                inner: incoming_request_stream,
            }),
            last_close_code: None,
        }));
        let kill = Arc::new(Notify::new());

        let process = SignalWebSocketProcess {
            keep_alive_path,
            requests: outgoing_requests,
            request_sink: incoming_request_sink,
            outgoing_requests: HashMap::default(),
            keepalive: KeepAliveState::new(Instant::now()),
            // Initializing the FuturesUnordered with a `pending` future means it will never fuse
            // itself, so an "empty" FuturesUnordered will still allow new futures to be added.
            outgoing_responses: vec![
                Box::pin(futures::future::pending()) as BoxFuture<_>
            ]
            .into_iter()
            .collect(),
            ws,
            shared: Arc::clone(&inner),
            kill: Arc::clone(&kill),
        };
        let process = process.run().map(|x| match x {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("SignalWebSocket: {}", e);
            },
        });

        (
            Self {
                _type: PhantomData,
                request_sink: outgoing_request_sink,
                unidentified_push_service,
                inner,
                kill,
            },
            process,
        )
    }

    pub fn servers(&self) -> SignalServers {
        self.unidentified_push_service.servers
    }

    pub fn is_closed(&self) -> bool {
        self.request_sink.is_closed()
    }

    pub fn is_used(&self) -> bool {
        self.inner_locked().stream.is_none()
    }

    /// Close code of the last server close frame observed by this
    /// connection's background process, if any.
    /// KT (single-connection): feeds the application-side 4409 branch.
    pub fn close_code(&self) -> Option<u16> {
        self.inner_locked().last_close_code
    }

    /// Ask the background process to send a close frame and exit. The next
    /// dial for the same account replaces this connection (kill-before-create
    /// handover, see [`slot`]).
    /// KT (single-connection).
    pub fn terminate(&self) {
        self.kill.notify_one();
    }

    pub(crate) fn take_request_stream(
        &mut self,
    ) -> Option<SignalRequestStream> {
        self.inner_locked().stream.take()
    }

    pub(crate) fn return_request_stream(&mut self, r: SignalRequestStream) {
        self.inner_locked().stream.replace(r);
    }

    // XXX Ideally, this should take an *async* closure, then we could get rid of the
    // `take_request_stream` and `return_request_stream`.
    pub async fn with_request_stream<
        R: 'static,
        F: FnOnce(&mut SignalRequestStream) -> R,
    >(
        &mut self,
        f: F,
    ) -> R {
        let mut s = self
            .inner_locked()
            .stream
            .take()
            .expect("request stream invariant");
        let r = f(&mut s);
        self.inner_locked().stream.replace(s);
        r
    }

    pub fn request(
        &mut self,
        r: WebSocketRequestMessage,
    ) -> impl Future<Output = Result<WebSocketResponseMessage, ServiceError>>
    {
        let (sink, recv): (
            oneshot::Sender<Result<WebSocketResponseMessage, ServiceError>>,
            _,
        ) = oneshot::channel();

        let mut request_sink = self.request_sink.clone();
        async move {
            if let Err(_e) = request_sink.send((r, sink)).await {
                return Err(ServiceError::WsClosing {
                    reason: "WebSocket closing while sending request",
                });
            }
            // Handle the oneshot sender error for dropped senders.
            match recv.await {
                Ok(x) => x,
                Err(_) => Err(ServiceError::WsClosing {
                    reason: "WebSocket closing while waiting for a response",
                }),
            }
        }
    }

    pub(crate) async fn request_json_with<T, F, Fut>(
        &mut self,
        r: WebSocketRequestMessage,
        decode: F,
    ) -> Result<T, ServiceError>
    where
        for<'de> T: serde::Deserialize<'de>,
        F: FnOnce(WebSocketResponseMessage) -> Fut + Send,
        Fut: Future<Output = Result<WebSocketResponseMessage, ServiceError>>
            + Send,
    {
        self.request(r)
            .await?
            .service_error_for_status_with(decode)
            .await?
            .json()
            .await
    }
}

impl WebSocketResponseMessage {
    pub async fn json<T: for<'a> serde::Deserialize<'a>>(
        &self,
    ) -> Result<T, ServiceError> {
        self.body
            .as_ref()
            .ok_or(ServiceError::UnsupportedContent)
            .and_then(|b| serde_json::from_slice(b).map_err(Into::into))
    }
}

#[cfg(test)]
mod keepalive_tests {
    use super::{
        keepalive_is_stale, request_is_overdue, KeepAliveSendOutcome,
        KeepAliveState, KeepAliveStep, KEEPALIVE_INTERVAL,
        KEEPALIVE_RESPONSE_TIMEOUT, REQUEST_RESPONSE_TIMEOUT,
        STALE_KEEPALIVE_THRESHOLD,
    };
    use std::time::Duration;
    use tokio::time::Instant;

    // KT: the suspend/wake sentinel must match the official 5-minute
    // threshold exactly (WebsocketResources.preload.ts:508).
    #[test]
    fn stale_threshold_is_five_minutes() {
        assert_eq!(STALE_KEEPALIVE_THRESHOLD, Duration::from_secs(5 * 60));
    }

    #[test]
    fn last_alive_at_or_beyond_threshold_is_stale() {
        // Official isOlderThan: elapsed >= threshold, boundary included.
        let last = Instant::now();
        assert!(keepalive_is_stale(last, last + Duration::from_secs(5 * 60)));
        // Machine slept for half an hour: first tick after wake must trip.
        assert!(keepalive_is_stale(
            last,
            last + Duration::from_secs(30 * 60)
        ));
    }

    #[test]
    fn fresh_keepalive_response_is_not_stale() {
        let last = Instant::now();
        assert!(!keepalive_is_stale(last, last));
        assert!(!keepalive_is_stale(last, last + Duration::from_secs(30)));
        assert!(!keepalive_is_stale(
            last,
            last + Duration::from_secs(4 * 60)
        ));
    }

    /// Official cadence: keepalive every 30s; a successful response restarts
    /// the 30s clock instead of inheriting the old tick boundary.
    #[test]
    fn keepalive_is_due_every_30s_and_success_resets_the_clock() {
        let t0 = Instant::now();
        let mut keepalive = KeepAliveState::new(t0);

        assert_eq!(keepalive.step(t0), KeepAliveStep::Send);
        keepalive.on_sent(t0, 7);
        assert_eq!(
            keepalive.step(t0 + Duration::from_secs(29)),
            KeepAliveStep::Wait
        );

        keepalive.on_response(t0 + Duration::from_secs(29));
        assert_eq!(
            keepalive.step(t0 + Duration::from_secs(30)),
            KeepAliveStep::Wait,
            "response at +29s must reset the 30s clock"
        );
        assert_eq!(
            keepalive.step(t0 + Duration::from_secs(59)),
            KeepAliveStep::Send
        );
    }

    /// B: a single keepalive with no response for 30s closes immediately
    /// instead of waiting for the next 30s tick (official close 3001).
    #[test]
    fn unanswered_keepalive_closes_at_30s_without_waiting_for_next_tick() {
        let t0 = Instant::now();
        let mut keepalive = KeepAliveState::new(t0);
        keepalive.on_sent(t0, 1);

        assert_eq!(
            keepalive.step(t0 + Duration::from_secs(29)),
            KeepAliveStep::Wait
        );
        assert_eq!(
            keepalive.step(t0 + KEEPALIVE_RESPONSE_TIMEOUT),
            KeepAliveStep::CloseUnanswered
        );
        assert!(KeepAliveStep::CloseUnanswered.is_close());
    }

    /// 5min without any successful keepalive closes even if a keepalive is
    /// currently in flight (suspend/wake path).
    #[test]
    fn stale_link_closes_even_with_a_keepalive_in_flight() {
        let t0 = Instant::now();
        let mut keepalive = KeepAliveState::new(t0);
        keepalive.on_sent(t0 + Duration::from_secs(4 * 60 + 50), 9);

        assert_eq!(
            keepalive.step(t0 + STALE_KEEPALIVE_THRESHOLD),
            KeepAliveStep::CloseStale
        );
    }

    /// A: a keepalive frame write that fails or times out is link death, not
    /// an info-level no-op: the caller closes so the receive stream reconnects
    /// (C: a successful new stream resets the backoff).
    #[test]
    fn keepalive_send_failure_or_timeout_is_link_death() {
        let t0 = Instant::now();
        let mut keepalive = KeepAliveState::new(t0);

        assert_eq!(
            keepalive.on_send_result(KeepAliveSendOutcome::Failed),
            KeepAliveStep::CloseSendFailed
        );
        assert_eq!(
            keepalive.on_send_result(KeepAliveSendOutcome::TimedOut),
            KeepAliveStep::CloseSendFailed
        );
        assert_eq!(
            keepalive.on_send_result(KeepAliveSendOutcome::Sent),
            KeepAliveStep::Wait
        );
        assert!(KeepAliveStep::CloseSendFailed.is_close());
        assert_eq!(
            KeepAliveStep::CloseSendFailed.close_reason(),
            "keepalive send failure"
        );
    }

    /// An application request with no response for 30s closes the link; this
    /// is what wakes a receive task suspended on a request over a dead link.
    #[test]
    fn request_without_response_closes_after_30s() {
        let sent = Instant::now();
        assert!(!request_is_overdue(sent, sent + Duration::from_secs(29)));
        assert!(request_is_overdue(sent, sent + REQUEST_RESPONSE_TIMEOUT));
        assert_eq!(REQUEST_RESPONSE_TIMEOUT, Duration::from_secs(30));
    }

    /// Watchdog granularity is decoupled from the keepalive cadence so a due
    /// deadline is acted on within ~1s, not on the next 30s tick.
    #[test]
    fn watchdog_is_finer_than_the_keepalive_interval() {
        assert!(super::WATCHDOG_INTERVAL < KEEPALIVE_INTERVAL);
        assert_eq!(KEEPALIVE_INTERVAL, Duration::from_secs(30));
    }

    /// Every close decision is fatal and has a loggable reason; Send/Wait are
    /// never treated as closure.
    #[test]
    fn close_steps_are_exhaustive_and_never_silent() {
        for step in [
            KeepAliveStep::CloseStale,
            KeepAliveStep::CloseUnanswered,
            KeepAliveStep::CloseRequestTimeout,
            KeepAliveStep::CloseSendFailed,
        ] {
            assert!(step.is_close());
            assert!(!step.close_reason().is_empty());
        }
        assert!(!KeepAliveStep::Send.is_close());
        assert!(!KeepAliveStep::Wait.is_close());
    }
}
