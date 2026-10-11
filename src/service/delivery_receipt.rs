// SPDX-License-Identifier: AGPL-3.0-only

//! 自动 delivery receipt（D 批 D1，官方桌面行为对齐）：
//!
//! - 排队闸（官方 `ts/messages/handleDataMessage.preload.ts:374-398` 与
//!   `ts/util/handleEditMessage.preload.ts:267-286`）：入站（非 sync/sent）
//!   DataMessage / EditMessage、sealed sender 信封
//!   （`unidentifiedDeliveryReceived` ← Metadata.unidentified_sender）、
//!   解密成功（引擎侧 = `Received::Content` 路径，对位官方 `!hasErrors`）、
//!   非 story、发送方为 ACI（官方 `isAciString` strictAssert）。回执时间戳
//!   = 消息 upstream timestamp（dataMessage.timestamp；edit 用内层
//!   dataMessage.timestamp，官方 editAttributes.message.timestamp 同位）。
//! - 批量（官方 deliveryReceiptBatcher，`ts/util/deliveryReceipt.preload.ts`；
//!   分帧常量见 `ts/util/sendReceipts.preload.ts` CHUNK_SIZE）：wait 500ms /
//!   max 100，按发送方归组，逐作者直发 `ReceiptMessage{DELIVERY,
//!   timestamps[]}`（原会话是群组也直发原作者，官方从不整组广播回执）。
//! - 两条官方闸引擎侧不具备同等状态，作为偏差如实登记（compat §5）：
//!   `conversation.getAccepted()`（会话接受态在宿主/connector，引擎无会话
//!   模型）与拉黑会话闸同理——引擎对满足上述条件的入站行一律排队。
//! - 失败静默：flush 发送失败仅弃批量（引擎零 stderr 纪律，compat §1），
//!   绝不影响主链路 ack；重复投递的投影去重在上游完成（A 批 store 行查重），
//!   批内 (sender, ts) 二次排队折叠。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use presage::libsignal_service::content::{Content, ContentBody};
use presage::libsignal_service::protocol::ServiceId;

/// 官方 deliveryReceiptBatcher `wait: 500`（ts/util/deliveryReceipt.preload.ts:28）。
pub(crate) const BATCH_WAIT: Duration = Duration::from_millis(500);
/// 官方 deliveryReceiptBatcher `maxSize: 100`（同 :30）；批满不等窗口立即 flush。
pub(crate) const BATCH_MAX_ENTRIES: usize = 100;

/// 单账号接收流上的待发 delivery receipt 批量缓冲。
pub(crate) struct AutoDeliveryReceipts {
    pending: HashMap<ServiceId, Vec<u64>>,
    due_at: Option<tokio::time::Instant>,
}

impl Default for AutoDeliveryReceipts {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoDeliveryReceipts {
    pub(crate) fn new() -> Self {
        Self {
            pending: HashMap::new(),
            due_at: None,
        }
    }

    /// 排队一条（批内同 (sender, timestamp) 折叠）；首个条目开启 500ms 批量窗。
    pub(crate) fn queue(&mut self, sender: ServiceId, timestamp: u64) {
        let entries = self.pending.entry(sender).or_default();
        if entries.contains(&timestamp) {
            return;
        }
        entries.push(timestamp);
        if self.due_at.is_none() {
            self.due_at = Some(tokio::time::Instant::now() + BATCH_WAIT);
        }
    }

    /// select! 计时臂的唤醒点：批量窗到期时刻（无排队则 None 不唤醒）。
    pub(crate) fn due_at(&self) -> Option<tokio::time::Instant> {
        self.due_at
    }

    /// 批量窗到期判据（now 注入便于测试）：到期或批满。
    fn ready_at(&self, now: tokio::time::Instant) -> bool {
        self.total() >= BATCH_MAX_ENTRIES || self.due_at.is_some_and(|at| now >= at)
    }

    fn total(&self) -> usize {
        self.pending.values().map(|v| v.len()).sum()
    }

    /// 到期/批满则取走整批（按发送方归组，条目保排队序），否则 None。
    pub(crate) fn take_ready(&mut self) -> Option<Vec<(ServiceId, Vec<u64>)>> {
        if !self.ready_at(tokio::time::Instant::now()) {
            return None;
        }
        self.take()
    }

    /// 无条件取走整批（流排空时的收尾 flush）。
    pub(crate) fn take(&mut self) -> Option<Vec<(ServiceId, Vec<u64>)>> {
        if self.pending.is_empty() {
            return None;
        }
        let batch = self.pending.drain().collect();
        self.due_at = None;
        Some(batch)
    }
}

/// 单批 flush：spawn_local 承载网络发送（接收循环 select 臂内即刻返回，
/// 不阻塞收流），复用 sendReceipt 公共发送面（open_registered → 常驻管理器
/// → 逐作者直发，CHUNK_SIZE 分帧内置）。失败静默（官方 sendReceipt 存储
/// 后发送失败不重试同位；引擎零 stderr 纪律，compat §1）；open_registered
/// 失败（账号已摘除/未注册）整批弃。
pub(crate) fn spawn_delivery_receipt_flush(
    service: &Arc<super::Service>,
    account: &str,
    batch: Vec<(ServiceId, Vec<u64>)>,
) {
    if batch.is_empty() {
        return;
    }
    let service = Arc::clone(service);
    let account = account.to_owned();
    tokio::task::spawn_local(async move {
        for (sender, timestamps) in batch {
            let _ = service
                .send_receipt(
                    &account,
                    &sender.service_id_string(),
                    &timestamps,
                    presage::proto::receipt_message::Type::Delivery,
                )
                .await;
        }
    });
}

/// 排队闸（见模块注释的官方出处）：命中返回 (发送方 ACI, 回执时间戳)。
pub(crate) fn candidate_of(content: &Content) -> Option<(ServiceId, u64)> {
    // 官方「non-story sealed sender messages」：story 信封不回执（story
    // 时间线无会话回执面）。
    if content.metadata.story == Some(true) {
        return None;
    }
    // 官方 `unidentifiedDeliveryReceived`（sealed sender 信封）：明信封
    // （SMS 遗留/明文 prekey 形态）不回执。
    if !content.metadata.unidentified_sender {
        return None;
    }
    // 官方 `isAciString(sourceServiceId)` strictAssert：发送方必须是 ACI。
    if !matches!(content.metadata.sender, ServiceId::Aci(_)) {
        return None;
    }
    let timestamp = match &content.body {
        ContentBody::DataMessage(data_message) => Some(
            data_message
                .timestamp
                .unwrap_or(content.metadata.client_timestamp.timestamp_millis().max(0) as u64),
        ),
        // 官方对入站编辑同样回执（handleEditMessage.preload.ts:267-286），
        // 时间戳 = 内层 dataMessage.timestamp（编辑行自身的 upstream ts）；
        // 内层体/时间戳缺席 = 官方 unprocessable（hasErrors 同位）不回执。
        ContentBody::EditMessage(edit) => edit.data_message.as_ref().and_then(|dm| dm.timestamp),
        _ => return None,
    };
    Some((content.metadata.sender, timestamp?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::content::Metadata;
    use presage::libsignal_service::prelude::Uuid;
    use presage::libsignal_service::protocol::DeviceId;
    use presage::proto::{receipt_message, typing_message};
    use presage::proto::{sync_message, DataMessage, EditMessage, ReceiptMessage, SyncMessage};

    fn aci(uuid: &str) -> ServiceId {
        ServiceId::Aci(Uuid::parse_str(uuid).unwrap().into())
    }

    fn metadata_of(client_ts_ms: i64) -> Metadata {
        Metadata {
            sender: aci("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e11"),
            destination: aci("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"),
            sender_device: DeviceId::new(1).expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(client_ts_ms)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(client_ts_ms)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: true,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        }
    }

    fn content_of(body: ContentBody) -> Content {
        Content::from_body(body, metadata_of(1_700_000_000_000))
    }

    fn data_message(timestamp: u64) -> ContentBody {
        ContentBody::DataMessage(DataMessage {
            timestamp: Some(timestamp),
            ..DataMessage::default()
        })
    }

    /// 排队闸命中：sealed 非 story ACI 的 dataMessage/edit。
    #[test]
    fn candidate_accepts_sealed_incoming_data_and_edit() {
        let mut content = content_of(data_message(1234));
        assert_eq!(
            candidate_of(&content).map(|(_, ts)| ts),
            Some(1234),
            "dataMessage 用自身 timestamp"
        );
        content.body = ContentBody::EditMessage(EditMessage {
            target_sent_timestamp: Some(1100),
            data_message: Some(DataMessage {
                timestamp: Some(1300),
                ..DataMessage::default()
            }),
        });
        assert_eq!(
            candidate_of(&content).map(|(_, ts)| ts),
            Some(1300),
            "edit 用内层 dataMessage timestamp"
        );
        // timestamp 缺席回退信封 client timestamp（不发明 0）。
        content.metadata.client_timestamp = chrono::DateTime::from_timestamp_millis(7777).unwrap();
        content.body = ContentBody::DataMessage(DataMessage::default());
        assert_eq!(candidate_of(&content).map(|(_, ts)| ts), Some(7777));
    }

    /// 排队闸拒绝面：story/明信封/PNI 发送方/非消息体。
    #[test]
    fn candidate_rejects_story_plain_pni_and_non_message_bodies() {
        let mut content = content_of(data_message(1));
        content.metadata.story = Some(true);
        assert!(candidate_of(&content).is_none(), "story 信封不回执");

        let mut content = content_of(data_message(2));
        content.metadata.unidentified_sender = false;
        assert!(candidate_of(&content).is_none(), "明信封不回执");

        let mut content = content_of(data_message(3));
        content.metadata.sender = ServiceId::Pni(
            Uuid::parse_str("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e22")
                .unwrap()
                .into(),
        );
        assert!(candidate_of(&content).is_none(), "PNI 发送方不回执");

        let own = aci("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e11");
        let typing = ContentBody::TypingMessage(presage::proto::TypingMessage {
            timestamp: Some(4),
            action: Some(typing_message::Action::Started as i32),
            group_id: None,
        });
        assert!(candidate_of(&content_of(typing)).is_none(), "typing 不回执");
        let receipt = ContentBody::ReceiptMessage(ReceiptMessage {
            r#type: Some(receipt_message::Type::Read as i32),
            timestamp: vec![5],
        });
        assert!(candidate_of(&content_of(receipt)).is_none(), "回执体不回执");
        let sync = ContentBody::SynchronizeMessage(SyncMessage {
            read: vec![sync_message::Read {
                sender_aci: Some(own.service_id_string()),
                timestamp: Some(6),
                sender_aci_binary: None,
            }],
            ..SyncMessage::default()
        });
        assert!(candidate_of(&content_of(sync)).is_none(), "sync 体不回执");
        let empty_edit = ContentBody::EditMessage(EditMessage {
            target_sent_timestamp: Some(7),
            data_message: None,
        });
        assert!(
            candidate_of(&content_of(empty_edit)).is_none(),
            "无内层体的编辑不可回执"
        );
    }

    /// 批量缓冲：批内 (sender, ts) 折叠、按发送方归组、保排队序。
    #[test]
    fn queue_dedups_and_groups_by_sender() {
        let a = aci("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e11");
        let b = aci("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e33");
        let mut batcher = AutoDeliveryReceipts::new();
        batcher.queue(a, 1);
        batcher.queue(a, 1);
        batcher.queue(b, 2);
        batcher.queue(a, 3);
        assert_eq!(batcher.total(), 3, "同 (sender, ts) 折叠");
        let batch = batcher.take().expect("take");
        assert_eq!(batch.len(), 2, "按发送方归组");
        for (sender, timestamps) in batch {
            match sender {
                s if s.service_id_string() == a.service_id_string() => {
                    assert_eq!(timestamps, vec![1, 3], "组内保排队序")
                }
                s if s.service_id_string() == b.service_id_string() => {
                    assert_eq!(timestamps, vec![2])
                }
                other => panic!("unexpected sender {other:?}"),
            }
        }
        assert!(batcher.due_at().is_none(), "取走后批量窗关闭");
    }

    /// 未到期且未批满不 flush；批满立即 flush（不等窗口）；窗口到期判据
    /// 由 ready_at 注入时刻覆盖，收尾经无条件 take（真实时钟不等待）。
    #[test]
    fn take_ready_gates_on_window_and_cap() {
        let a = aci("d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e11");
        let mut batcher = AutoDeliveryReceipts::new();
        batcher.queue(a, 1);
        // 未到期：不取（take_ready 以真实时钟判定）。
        assert!(!batcher.ready_at(tokio::time::Instant::now()));
        assert!(batcher.take_ready().is_none());
        // 批满：立即取（不等窗口）。
        for ts in 2..=BATCH_MAX_ENTRIES as u64 {
            batcher.queue(a, ts);
        }
        let batch = batcher.take_ready().expect("cap flush");
        assert_eq!(batch[0].1.len(), BATCH_MAX_ENTRIES);
        // 窗口到期判据（ready_at 注入远期时刻）+ 无条件 take 收尾路径。
        batcher.queue(a, 999);
        let due = batcher.due_at().expect("window open");
        assert!(batcher.ready_at(due + Duration::from_millis(1)));
        let batch = batcher.take().expect("final take");
        assert_eq!(batch[0].1, vec![999]);
        assert!(batcher.take_ready().is_none(), "空批不取");
    }
}
