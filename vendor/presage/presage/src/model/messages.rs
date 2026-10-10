use libsignal_service::messagepipe::EnvelopeAck;
use libsignal_service::{prelude::Content, protocol::ServiceId};

#[derive(Debug)]
pub enum Received {
    /// when the receive loop is empty, happens when opening the websocket for the first time
    /// once you're done synchronizing all pending messages for this registered client.
    QueueEmpty,

    /// Got contacts (only applies if linked to a primary device
    /// Contacts can be later queried in the store.
    /// They are stored before yielding; the consumer acks the source envelope.
    Contacts(EnvelopeAck),

    // Failed to receive a message from the specified service ID.
    // A session reset with the contact may fix this.
    // Carries the deferred ack (decrypt failures are acked too, like upstream
    // clients) and the sender-assigned client timestamp when known.
    DecryptionError(ServiceId, Option<u64>, EnvelopeAck),

    /// Redelivered envelope: a `(sender, client_timestamp)` row already exists
    /// in the store. The message was stored (and is expected to have been
    /// projected) once already; consumers ack and must not re-project.
    Duplicate(EnvelopeAck),

    /// Incoming decrypted message with metadata and content, stored before
    /// yielding. The consumer acks after projecting the message.
    Content(Box<Content>, EnvelopeAck),
}
