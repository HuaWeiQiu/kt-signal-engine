// SPDX-License-Identifier: AGPL-3.0-only

//! Process-global websocket slots, keyed by account credentials login.
//! KT (single-connection) fork-only module.
//!
//! One account (one device) holds **at most one live websocket of each kind**
//! (identified / unidentified), no matter how many presage `Manager`
//! instances exist: the slot is shared, a dial for the same account first
//! terminates the previous connection (kill-before-create handover), and dials
//! for the same account are serialized on a per-account lock (single-flight;
//! no concurrent double-dial after a 4409 or a dropped stream).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock};

use tokio::sync::Mutex;

use super::{Identified, SignalWebSocket, Unidentified};
use crate::push_service::ServiceError;

struct AccountSlot<C: super::WebSocketType> {
    // std Mutex: no await inside the critical section; the async dial_lock
    // above provides the cross-await serialization.
    current: std::sync::Mutex<Option<SignalWebSocket<C>>>,
    dial_lock: Mutex<()>,
}

impl<C: super::WebSocketType> Default for AccountSlot<C> {
    fn default() -> Self {
        Self {
            current: std::sync::Mutex::new(None),
            dial_lock: Mutex::new(()),
        }
    }
}

type SlotMap<C> = LazyLock<Mutex<HashMap<String, Arc<AccountSlot<C>>>>>;

static IDENTIFIED_SLOTS: SlotMap<Identified> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static UNIDENTIFIED_SLOTS: SlotMap<Unidentified> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

async fn slot_for<C: super::WebSocketType>(
    slots: &SlotMap<C>,
    key: &str,
) -> Arc<AccountSlot<C>> {
    let mut map = slots.lock().await;
    map.entry(key.to_owned())
        .or_insert_with(|| Arc::new(AccountSlot::default()))
        .clone()
}

/// Handover decision inside a held dial lock: reuse the slot connection when
/// it is live (and its request stream is untaken if `require_unused`), else
/// terminate the stale entry before dialing the replacement.
async fn acquire<C: super::WebSocketType>(
    slot: &AccountSlot<C>,
    require_unused: bool,
    dial: impl Future<Output = Result<SignalWebSocket<C>, ServiceError>>,
    kind: &'static str,
) -> Result<SignalWebSocket<C>, ServiceError> {
    let _dial_permit = slot.dial_lock.lock().await;
    {
        let mut current = slot.current.lock().unwrap();
        if let Some(ws) = current.as_ref() {
            if !ws.is_closed() && !(require_unused && ws.is_used()) {
                return Ok(ws.clone());
            }
        }
        if let Some(previous) = current.take() {
            tracing::warn!(
                "websocket handover: terminating previous {kind} connection before dialing"
            );
            previous.terminate();
        }
    }
    let ws = dial.await?;
    slot.current.lock().unwrap().replace(ws.clone());
    Ok(ws)
}

/// Reuse or (after terminating the previous) dial the account's identified
/// websocket. `require_unused` mirrors upstream semantics: the caller wants a
/// websocket whose server-request stream is untaken (message pipe).
pub async fn acquire_identified(
    key: &str,
    require_unused: bool,
    dial: impl Future<Output = Result<SignalWebSocket<Identified>, ServiceError>>,
) -> Result<SignalWebSocket<Identified>, ServiceError> {
    let slot = slot_for(&IDENTIFIED_SLOTS, key).await;
    acquire(&slot, require_unused, dial, "identified").await
}

/// Reuse or (after terminating the previous) dial the account's unidentified
/// websocket.
pub async fn acquire_unidentified(
    key: &str,
    dial: impl Future<Output = Result<SignalWebSocket<Unidentified>, ServiceError>>,
) -> Result<SignalWebSocket<Unidentified>, ServiceError> {
    let slot = slot_for(&UNIDENTIFIED_SLOTS, key).await;
    acquire(&slot, false, dial, "unidentified").await
}

/// Close code observed on the account's slot identified websocket, if any.
pub async fn identified_close_code(key: &str) -> Option<u16> {
    let ws = IDENTIFIED_SLOTS.lock().await.get(key).and_then(|slot| {
        slot.current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(SignalWebSocket::close_code)
    });
    ws
}

/// Whether the account's slot identified websocket is currently live. After
/// the receive pipe ended with 4409, a live websocket means another
/// engine-internal connection took over (self conflict); none means an
/// external client displaced us.
pub async fn identified_open_count(key: &str) -> usize {
    let open = IDENTIFIED_SLOTS.lock().await.get(key).is_some_and(|slot| {
        slot.current
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|ws| !ws.is_closed())
    });
    usize::from(open)
}
