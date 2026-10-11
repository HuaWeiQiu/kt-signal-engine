use libsignal_service::messagepipe::EnvelopeAck;
use libsignal_service::{prelude::Content, protocol::ServiceId};
use std::fmt;

use crate::model::contacts::Contact;

/// KT (P15a): contacts sync payload — the source envelope plus the parsed
/// contact records from the sync blob (Desktop ContactSyncEvent face:
/// services/contactSync.preload.ts). Only successfully saved records are
/// carried; the store save loop keeps its upstream break-on-error semantics.
pub struct ContactSync {
    pub content: Box<Content>,
    pub complete: bool,
    pub contacts: Vec<Contact>,
}

impl fmt::Debug for ContactSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContactSync")
            .field("complete", &self.complete)
            .field("contacts", &self.contacts.len())
            .finish_non_exhaustive()
    }
}

/// KT (DERM): retry material for a decryption failure, mirroring the two
/// official faces (Android MessageDecryptor buildSendRetryReceiptJob,
/// Desktop handleRetry onRetryRequest / requestResend).
#[derive(Debug)]
pub enum DecryptionFailure {
    /// Our own decrypt failed: a serialized `DecryptionErrorMessage` proto
    /// the consumer can send back to the sender so they re-send the original.
    RetryReceipt(Vec<u8>),
    /// The peer asked us to retry: the timestamp of their failed message,
    /// embedded in the received `DecryptionErrorMessage`.
    RetryRequest(u64),
}

#[derive(Debug)]
pub enum Received {
    /// when the receive loop is empty, happens when opening the websocket for the first time
    /// once you're done synchronizing all pending messages for this registered client.
    QueueEmpty,

    /// Got contacts (only applies if linked to a primary device
    /// Contacts can be later queried in the store.
    /// They are stored before yielding; the consumer acks the source envelope.
    /// KT (P15a): carries the parsed records and source envelope for the
    /// engine projection.
    Contacts(ContactSync, EnvelopeAck),

    // Failed to receive a message from the specified service ID.
    // A session reset with the contact may fix this.
    // Carries the deferred ack (decrypt failures are acked too, like upstream
    // clients), the sender-assigned client timestamp when known, and the DERM
    // retry material when one can be derived (KT).
    DecryptionError(
        ServiceId,
        Option<u64>,
        Option<DecryptionFailure>,
        EnvelopeAck,
    ),

    /// Redelivered envelope: a `(sender, client_timestamp)` row already exists
    /// in the store. The message was stored (and is expected to have been
    /// projected) once already; consumers ack and must not re-project.
    Duplicate(EnvelopeAck),

    /// Incoming decrypted message with metadata and content, stored before
    /// yielding. The consumer acks after projecting the message.
    Content(Box<Content>, EnvelopeAck),

    /// KT: the stored identity for the sender was replaced while decrypting
    /// this envelope (session re-established on a changed key). The message
    /// itself decrypted and is carried through unchanged: consumers project
    /// it exactly like `Content`, ack once, and may surface the change.
    IdentityChange(ServiceId, Box<Content>, EnvelopeAck),
}
