use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use std::future::Future;

use bytes::Bytes;
use futures::channel::oneshot::Canceled;
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use reqwest::Method;
use reqwest_websocket::WebSocket;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::debug;

use crate::configuration::SignalServers;
use crate::prelude::PushService;
use crate::proto::{
    web_socket_message, WebSocketMessage, WebSocketRequestMessage,
    WebSocketResponseMessage,
};
use crate::push_service::{self, ServiceError, SignalServiceResponse};

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

/// Close the websocket when the last successful keepalive response is at
/// least this old, instead of sending on a dead link (machine
/// suspend/wake). Official Desktop
/// `WebsocketResources.preload.ts:508` `STALE_THRESHOLD_MS`.
pub const STALE_KEEPALIVE_THRESHOLD: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

/// Official `isOlderThan(lastAliveAt, STALE_THRESHOLD_MS)` — an elapsed
/// time at or beyond the threshold is stale.
fn keepalive_is_stale(last_alive_at: Instant, now: Instant) -> bool {
    now.duration_since(last_alive_at) >= STALE_KEEPALIVE_THRESHOLD
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

    outgoing_requests: HashMap<
        u64,
        oneshot::Sender<Result<WebSocketResponseMessage, ServiceError>>,
    >,

    outgoing_keep_alive_set: HashSet<u64>,

    // KT: moment of the last successful keepalive response; the tick arm
    // closes the link instead of sending once this goes stale (suspend/wake).
    last_alive_at: Instant,

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
                    if let Some(responder) = self.outgoing_requests.remove(&id)
                    {
                        if let Err(e) = responder.send(Ok(response)) {
                            tracing::warn!(
                                "Could not deliver response for id {}: {:?}",
                                id,
                                e
                            );
                        }
                    } else if let Some(_x) =
                        self.outgoing_keep_alive_set.take(&id)
                    {
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
                            tracing::warn!(%status, "ka_diag keepalive response non-success, task exiting"); // KA-DIAG
                            return Err(ServiceError::UnhandledResponseCode {
                                status,
                                body: String::from_utf8_lossy(response.body())
                                    .into_owned(),
                            });
                        }
                        self.last_alive_at = Instant::now();
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
            if !self.outgoing_requests.contains_key(&id) {
                return id;
            }
        }
    }

    async fn run(mut self) -> Result<(), ServiceError> {
        let mut ka_interval = tokio::time::interval_at(
            Instant::now(),
            push_service::KEEPALIVE_TIMEOUT_SECONDS,
        );

        loop {
            futures::select! {
                _ = ka_interval.tick().fuse() => {
                    tracing::warn!(pending = self.outgoing_keep_alive_set.len(), "ka_diag tick fired"); // KA-DIAG
                    use prost::Message;
                    if keepalive_is_stale(self.last_alive_at, Instant::now()) {
                        tracing::warn!("Websocket will be closed due to stale keepalive state: last successful keepalive response is at least 5 minutes in the past (stale).");
                        if let Err(e) = self.ws.close(reqwest_websocket::CloseCode::Away, Some("stale")).await {
                            tracing::debug!("Could not close WebSocket: {:?}", e);
                        }
                        self.outgoing_keep_alive_set.clear();
                        tracing::warn!("ka_diag task exiting: stale keepalive state"); // KA-DIAG
                        break;
                    }
                    if !self.outgoing_keep_alive_set.is_empty() {
                        tracing::warn!("Websocket will be closed due to failed keepalives.");
                        if let Err(e) = self.ws.close(reqwest_websocket::CloseCode::Away, None).await {
                            tracing::debug!("Could not close WebSocket: {:?}", e);
                        }
                        self.outgoing_keep_alive_set.clear();
                        tracing::warn!("ka_diag task exiting: failed keepalives"); // KA-DIAG
                        break;
                    }
                    tracing::debug!("sending keep-alive");
                    let request = WebSocketRequestMessage::new(Method::GET)
                        .id(self.next_request_id())
                        .path(&self.keep_alive_path)
                        .build();
                    self.outgoing_keep_alive_set.insert(request.id.unwrap());
                    let msg = WebSocketMessage {
                        r#type: Some(web_socket_message::Type::Request.into()),
                        request: Some(request),
                        ..Default::default()
                    };
                    let buffer = msg.encode_to_vec();
                    if let Err(e) = self.ws.send(reqwest_websocket::Message::Binary(buffer.into())).await {
                        tracing::warn!("Websocket sink has closed: {:?}.", e); // KA-DIAG: info→warn
                        tracing::warn!("ka_diag task exiting: keepalive send failed"); // KA-DIAG
                        break;
                    };
                },
                // Process requests from the application, forward them to Signal
                x = self.requests.next() => {
                    match x {
                        Some((mut request, responder)) => {
                            use prost::Message;

                            // Regenerate ID if already in the table
                            request.id = Some(
                                request
                                    .id
                                    .filter(|x| !self.outgoing_requests.contains_key(x))
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

                            self.outgoing_requests.insert(request.id.unwrap(), responder);
                            let msg = WebSocketMessage {
                                r#type: Some(web_socket_message::Type::Request.into()),
                                request: Some(request),
                                ..Default::default()
                            };
                            let buffer = msg.encode_to_vec();
                            self.ws.send(reqwest_websocket::Message::Binary(buffer.into())).await?
                        }
                        None => {
                            debug!("end of application request stream; websocket closing");
                            tracing::warn!("ka_diag task exiting: application request stream ended"); // KA-DIAG
                            return Ok(());
                        }
                    }
                }
                // KT (single-connection): discipline handover — a dial for the
                // same account replaced this connection; close frame then exit.
                _ = self.kill.notified().fuse() => {
                    tracing::warn!("websocket terminating: close requested by local single-connection discipline");
                    if let Err(e) = self.ws.close(reqwest_websocket::CloseCode::Away, None).await {
                        tracing::debug!("Could not close WebSocket: {:?}", e);
                    }
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
                            tracing::warn!("ka_diag task exiting: websocket close frame"); // KA-DIAG
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
                            tracing::warn!("ka_diag task exiting: inbound websocket error"); // KA-DIAG
                            return Err(e.into());
                        }
                        None => {
                            tracing::warn!("ka_diag task exiting: end of web request stream"); // KA-DIAG
                            return Err(ServiceError::WsClosing {
                                reason: "end of web request stream; socket closing"
                            });
                        }
                    }
                }
                response = self.outgoing_responses.next() => {
                    use prost::Message;
                    match response {
                        Some(Ok(response)) => {
                            tracing::trace!("sending response {:?}", response);

                            let msg = WebSocketMessage {
                                r#type: Some(web_socket_message::Type::Response.into()),
                                response: Some(response),
                                ..Default::default()
                            };
                            let buffer = msg.encode_to_vec();
                            self.ws.send(buffer.into()).await?;
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
            outgoing_keep_alive_set: HashSet::new(),
            last_alive_at: Instant::now(),
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
mod keepalive_stale_tests {
    use super::{keepalive_is_stale, STALE_KEEPALIVE_THRESHOLD};
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
}
