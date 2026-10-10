use futures::{
    channel::{
        mpsc::{self, Sender},
        oneshot,
    },
    prelude::*,
};
use prost::Message;

pub use crate::{
    configuration::ServiceCredentials,
    proto::{
        web_socket_message, Envelope, WebSocketMessage,
        WebSocketRequestMessage, WebSocketResponseMessage,
    },
};

use crate::{
    push_service::ServiceError,
    websocket::{self, SignalWebSocket},
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Incoming {
    /// The ack is deferred: the [`EnvelopeAck`] handle must be completed by the
    /// consumer after processing (Java: ack after "you handled it", by the caller).
    Envelope(Envelope, EnvelopeAck),
    QueueEmpty,
}

/// Deferred ack handle for one envelope. The 200 response is only written
/// when [`EnvelopeAck::ack`] is called; dropping the handle without acking
/// leaves the request unanswered so the server redelivers it later.
#[derive(Debug)]
pub struct EnvelopeAck {
    response: WebSocketResponseMessage,
    responder: oneshot::Sender<WebSocketResponseMessage>,
}

impl EnvelopeAck {
    /// Send the 200 ack. Returns false when the socket is already gone.
    pub fn ack(self) -> bool {
        self.responder.send(self.response).is_ok()
    }
}

pub struct MessagePipe {
    ws: SignalWebSocket<websocket::Identified>,
}

impl MessagePipe {
    pub fn from_socket(ws: SignalWebSocket<websocket::Identified>) -> Self {
        MessagePipe { ws }
    }

    /// Return a SignalWebSocket for sending messages and other purposes beyond receiving messages.
    pub fn ws(&self) -> SignalWebSocket<websocket::Identified> {
        self.ws.clone()
    }

    /// Worker task that processes the websocket into Envelopes
    async fn run(
        mut self,
        mut sink: Sender<Result<Incoming, ServiceError>>,
    ) -> Result<(), mpsc::SendError> {
        let mut ws = self.ws.clone();
        let mut stream = ws
            .take_request_stream()
            .expect("web socket request handler not in use");

        while let Some((request, responder)) = stream.next().await {
            // WebsocketConnection::onMessage(ByteString)
            if let Some(env) =
                self.process_request(request, responder).await.transpose()
            {
                sink.send(env).await?;
            } else {
                tracing::trace!("got empty message in websocket");
            }
        }

        ws.return_request_stream(stream);

        Ok(())
    }

    async fn process_request(
        &mut self,
        request: WebSocketRequestMessage,
        responder: oneshot::Sender<WebSocketResponseMessage>,
    ) -> Result<Option<Incoming>, ServiceError> {
        // Java: MessagePipe::read
        let response = WebSocketResponseMessage::from_request(&request);

        // XXX Change the signature of this method to yield an enum of Envelope and EndOfQueue
        if request.is_signal_service_envelope() {
            let body = if let Some(body) = request.body.as_ref() {
                body
            } else {
                return Err(ServiceError::InvalidFrame {
                    reason: "request without body.",
                });
            };
            // Envelopes yield unacked; the consumer acks via the handle once the
            // envelope is decrypted, stored and projected (Java: MessagePipe::read
            // acks after "you handled it").
            let envelope = Envelope::decode(body.as_slice())?;
            return Ok(Some(Incoming::Envelope(
                envelope,
                EnvelopeAck {
                    response,
                    responder,
                },
            )));
        }

        let result = if request.is_queue_empty() {
            Some(Incoming::QueueEmpty)
        } else {
            None
        };

        responder
            .send(response)
            .map_err(|_| ServiceError::WsClosing {
                reason: "could not respond to message pipe request",
            })?;

        Ok(result)
    }

    /// Returns the stream of `Envelope`s
    ///
    /// Envelopes yield an [`EnvelopeAck`] handle; the consumer acks after
    /// processing. Queue markers ack immediately.
    pub fn stream(self) -> impl Stream<Item = Result<Incoming, ServiceError>> {
        let (sink, stream) = mpsc::channel(1);

        let stream = stream.map(Some);
        let runner = self.run(sink).map(|e| {
            tracing::info!("sink was closed: {:?}", e);
            None
        });

        let combined = futures::stream::select(stream, runner.into_stream());
        combined.filter_map(|x| async { x })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_ack_writes_the_response_exactly_once() {
        let (responder, receiver) = oneshot::channel();
        let ack = EnvelopeAck {
            response: WebSocketResponseMessage::default(),
            responder,
        };

        assert!(ack.ack());
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn envelope_ack_reports_false_once_the_socket_is_gone() {
        let (responder, receiver) = oneshot::channel();
        let ack = EnvelopeAck {
            response: WebSocketResponseMessage::default(),
            responder,
        };
        drop(receiver);

        assert!(!ack.ack());
    }
}
