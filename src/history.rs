// SPDX-License-Identifier: AGPL-3.0-only

//! ADR 0005 S2：transfer archive 解密 + 分帧 + NDJSON 导出。
//!
//! 解密面全官方（`FramesReader`：HMAC-SHA256 尾验证 + AES-256-CBC + gzip +
//! padding 剥离；`VarintDelimitedReader` varint 分帧），帧 proto 为自持生成代码
//! （`crate::proto`，官方开源 schema）。逐帧流式转 receive-compatible 中间表示
//! （NDJSON），O(1) 帧内存。v1 范围与跳过策略见 ADR 0005 §S2 设计定案；
//! recipient 会话归属身份投影（aci/e164/masterKey，E5）按消费方 connector
//! 契约 1.39（implementation-plan §4.38）的解析面逐字对齐。

use std::io::Write;
use std::path::Path;

use libsignal_account_keys::BackupKey;
use libsignal_message_backup::frame::{FileReaderFactory, FramesReader, VerifyHmac};
use libsignal_message_backup::key::MessageBackupKey;
use libsignal_message_backup::parse::VarintDelimitedReader;
use prost::Message as _;

use crate::proto::group::GroupAttributeBlob;
use crate::proto::{
    chat_item, chat_update_message, frame, group_change_chat_update, quote, recipient, Chat,
    ChatItem, ChatUpdateMessage, FilePointer, Frame, Recipient,
};

/// 导出计数：结果与跳过都要可见，禁静默丢帧。
/// `frames` 为档案总帧数（含 BackupInfo 元帧）。
#[derive(Debug, Default, PartialEq)]
pub struct ExportStats {
    pub backup_version: u64,
    pub frames: u64,
    pub recipients: u64,
    pub chats: u64,
    pub messages: u64,
    pub remote_deleted: u64,
    pub skipped_items: u64,
    pub skipped_frames: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("backup frames: {0}")]
    Frames(#[from] libsignal_message_backup::frame::ValidationError),
    #[error("hmac: {0}")]
    Hmac(#[from] libsignal_message_backup::frame::VerifyHmacError),
    #[error("json encode: {0}")]
    Json(#[from] serde_json::Error),
    #[error("no frames in archive")]
    NoFrames,
}

/// 解密 `archive` 并把逐帧中间表示写入 `out`（NDJSON，一行一记录）。
///
/// 注意：上游 FramesReader 内部持 Rc/非 Send 状态（sync-IO 适配），本函数
/// 的 future 非 Send——调用方须在 LocalSet 上运行（spawn_local），与引擎
/// 其他 presage 任务同策略。
pub async fn export_archive(
    archive: &Path,
    aci: presage::libsignal_service::prelude::Uuid,
    ephemeral_backup_key: [u8; 32],
    out: &mut dyn Write,
) -> Result<ExportStats, HistoryError> {
    let backup_key: BackupKey = BackupKey(ephemeral_backup_key);
    let backup_id =
        backup_key.derive_backup_id(&presage::libsignal_service::protocol::Aci::from(aci));
    let key = MessageBackupKey::derive(&backup_key, &backup_id, None);

    let factory = FileReaderFactory { path: archive };
    let frames = FramesReader::new(&key, factory).await?;
    let mut delimited = VarintDelimitedReader::new(frames);

    let first = delimited.read_next().await?.ok_or(HistoryError::NoFrames)?;
    let info = crate::proto::BackupInfo::decode(&first[..])?;
    let mut stats = ExportStats {
        backup_version: info.version,
        frames: 1,
        ..ExportStats::default()
    };

    while let Some(bytes) = delimited.read_next().await? {
        stats.frames += 1;
        let frame = Frame::decode(&bytes[..])?;
        // Frame.item unset：官方语义为「跳过该帧，不报错」。
        match frame.item {
            Some(frame::Item::Recipient(recipient)) => {
                stats.recipients += 1;
                write_json(out, recipient_record(&recipient))?;
            }
            Some(frame::Item::Chat(chat)) => {
                stats.chats += 1;
                write_json(out, chat_record(&chat))?;
            }
            Some(frame::Item::ChatItem(item)) => match message_record(&item) {
                Some(record) => {
                    stats.messages += 1;
                    write_json(out, record)?;
                }
                None => {
                    if matches!(item.item, Some(chat_item::Item::RemoteDeletedMessage(_))) {
                        stats.remote_deleted += 1;
                    } else {
                        stats.skipped_items += 1;
                    }
                }
            },
            Some(_) | None => stats.skipped_frames += 1,
        }
    }

    // 流读完即到档案尾：消费 reader 校验 HMAC 尾部（不校验 = 不可信导入）。
    delimited
        .into_inner()
        .verify_hmac()
        .await
        .map_err(HistoryError::Hmac)?;
    Ok(stats)
}

fn write_json(out: &mut dyn Write, value: serde_json::Value) -> Result<(), HistoryError> {
    serde_json::to_writer(&mut *out, &value)?;
    out.write_all(b"\n")?;
    Ok(())
}

fn recipient_record(r: &Recipient) -> serde_json::Value {
    let (kind, name) = match &r.destination {
        Some(recipient::Destination::Contact(c)) => {
            let mut name = c.system_given_name.clone();
            if !c.system_family_name.is_empty() {
                if !name.is_empty() {
                    name.push(' ');
                }
                name.push_str(&c.system_family_name);
            }
            ("contact", name)
        }
        Some(recipient::Destination::Group(g)) => (
            "group",
            g.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.title.as_ref())
                .map(blob_string)
                .unwrap_or_default(),
        ),
        Some(recipient::Destination::Self_(_)) => ("self", String::new()),
        _ => ("other", String::new()),
    };
    let mut record = serde_json::json!({
        "type": "recipient",
        "id": r.id,
        "kind": kind,
        "name": name,
    });
    project_recipient_identity(&mut record, r);
    record
}

/// E5 会话归属身份投影（消费方 connector 契约 1.39 §4.38，形状逐字对齐其
/// `recipient {id, kind, name, aci?, e164?, masterKey?}` 解析面）。proto 只
/// 投影不发明：缺/畸形的身份一律省略键——connector 视为缺席，会话归属
/// 失败由其如实计入 skippedChats，绝不投占位值。
fn project_recipient_identity(record: &mut serde_json::Value, r: &Recipient) {
    match &r.destination {
        Some(recipient::Destination::Contact(c)) => {
            if let Some(aci) = c.aci.as_deref().and_then(aci_string) {
                record["aci"] = serde_json::Value::String(aci);
            }
            if let Some(e164) = c.e164.filter(|e164| *e164 > 0) {
                record["e164"] = serde_json::json!(e164);
            }
        }
        Some(recipient::Destination::Group(g)) => {
            if let Some(master_key) = master_key_string(&g.master_key) {
                record["masterKey"] = serde_json::Value::String(master_key);
            }
        }
        // self：proto Self_ 只携带 avatar_color，无身份可投——connector 以
        // 账号注册号码（engine-state.json registry 映射）作 self 会话归属。
        _ => {}
    }
}

/// proto Contact.aci（16 字节）→ 小写连字 UUID 串（connector is_uuid_shape
/// 消费形状；引擎直接出规范小写形）。长度畸形的档案字节不可信 → 省略。
fn aci_string(bytes: &[u8]) -> Option<String> {
    Some(
        presage::libsignal_service::prelude::Uuid::from_slice(bytes)
            .ok()?
            .to_string(),
    )
}

/// 群 master key → 标准 base64：live 群会话投影 `groupInfo.groupId` /
/// `listGroups` id 同形同值，connector 以该形作群 peer_key 与 live 行去重。
/// 空 key 无身份可投影 → 省略。
fn master_key_string(key: &[u8]) -> Option<String> {
    use base64::Engine as _;
    (!key.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(key))
}

/// GroupAttributeBlob 的 oneof：title 场景取 `Title` 变体。
fn blob_string(blob: &GroupAttributeBlob) -> String {
    match &blob.content {
        Some(crate::proto::group::group_attribute_blob::Content::Title(s)) => s.clone(),
        _ => String::new(),
    }
}

fn chat_record(c: &Chat) -> serde_json::Value {
    let mut record = serde_json::json!({
        "type": "chat",
        "id": c.id,
        "recipientId": c.recipient_id,
        "archived": c.archived,
        "pinnedOrder": c.pinned_order,
    });
    if let Some(timer_ms) = c.expiration_timer_ms {
        record["expirationTimerMs"] = serde_json::json!(timer_ms);
    }
    if c.expire_timer_version > 0 {
        record["expireTimerVersion"] = serde_json::json!(c.expire_timer_version);
    }
    record
}

/// v1 中间表示：text + 附件 metadata（不下载字节）+ quote + 远端删除。
/// 返回 `None` = 跳过（计数可见）。
fn message_record(item: &ChatItem) -> Option<serde_json::Value> {
    let direction = match item.directional_details.as_ref()? {
        chat_item::DirectionalDetails::Incoming(_) => "incoming",
        chat_item::DirectionalDetails::Outgoing(_) => "outgoing",
        chat_item::DirectionalDetails::Directionless(_) => "directionless",
    };

    let mut record = serde_json::json!({
        "type": "message",
        "chatId": item.chat_id,
        "authorId": item.author_id,
        "dateSent": item.date_sent,
        "direction": direction,
    });
    if item.sms {
        record["sms"] = serde_json::json!(true);
    }
    if let Some(pin) = &item.pin_details {
        record["pinnedAtTimestamp"] = serde_json::json!(pin.pinned_at_timestamp);
    }
    if let Some(start) = item.expire_start_date.filter(|start| *start > 0) {
        record["expireStartDate"] = serde_json::json!(start);
    }
    if let Some(expires_in) = item.expires_in_ms.filter(|expires_in| *expires_in > 0) {
        record["expiresInMs"] = serde_json::json!(expires_in);
    }

    match item.item.as_ref()? {
        chat_item::Item::StandardMessage(standard) => {
            if let Some(text) = &standard.text {
                record["text"] = serde_json::json!(text.body);
            }
            let attachments: Vec<_> = standard
                .attachments
                .iter()
                .filter_map(attachment_record)
                .collect();
            if !attachments.is_empty() {
                record["attachments"] = serde_json::json!(attachments);
            }
            if let Some(quote) = &standard.quote {
                record["quote"] = serde_json::json!({
                    "targetSentTimestamp": quote.target_sent_timestamp,
                    "authorId": quote.author_id,
                    "text": quote.text.as_ref().map(|t| t.body.clone()),
                    "type": quote_type_name(quote.r#type),
                });
            }
            if let Some(long_text) = &standard.long_text {
                record["longText"] = serde_json::json!(file_pointer_record(long_text));
            }
        }
        chat_item::Item::RemoteDeletedMessage(_) => {
            record["remoteDeleted"] = serde_json::json!(true);
        }
        chat_item::Item::UpdateMessage(update) => {
            record["expirationTimerChange"] = timer_change_record(update)?;
        }
        // 其余 item 类型 v1 跳过（调用方计数）；方向可用的 update/call 类未来再扩。
        _ => return None,
    };
    Some(record)
}

/// E 批备份导出计时器变更行（connector 拿去做 system 行：会话归属 + 新
/// timer 值 + 0=关闭语义）。1:1 取 ExpirationTimerChatUpdate；群取
/// GroupChange 批内第一个 GroupExpirationTimerUpdate。其余 update 类型不投
/// （返回 None 计入 skippedItems）。proto 只投影不发明：expiresInMs 为
/// proto3 标量原样透出（0 原样保留）；updaterAci 为 16 字节 ACI 的小写
/// UUID 形，畸形省略键。
fn timer_change_record(update: &ChatUpdateMessage) -> Option<serde_json::Value> {
    match update.update.as_ref()? {
        chat_update_message::Update::ExpirationTimerChange(change) => {
            Some(serde_json::json!({ "expiresInMs": change.expires_in_ms }))
        }
        chat_update_message::Update::GroupChange(group) => {
            group
                .updates
                .iter()
                .find_map(|entry| match entry.update.as_ref()? {
                    group_change_chat_update::update::Update::GroupExpirationTimerUpdate(timer) => {
                        let mut change = serde_json::json!({ "expiresInMs": timer.expires_in_ms });
                        if let Some(aci) = timer.updater_aci.as_deref().and_then(aci_string) {
                            change["updaterAci"] = serde_json::Value::String(aci);
                        }
                        Some(change)
                    }
                    _ => None,
                })
        }
        _ => None,
    }
}

fn attachment_record(attachment: &crate::proto::MessageAttachment) -> Option<serde_json::Value> {
    let pointer: &FilePointer = attachment.pointer.as_ref()?;
    let mut record = serde_json::json!({
        "contentType": pointer.content_type,
        "fileName": pointer.file_name,
        "flag": attachment.flag,
    });
    if let Some(locator) = &pointer.locator_info {
        record["size"] = serde_json::json!(locator.size);
    }
    Some(record)
}

/// metadata-only：只取文件名/类型/大小等描述字段，绝不取密钥与 locator。
fn file_pointer_record(pointer: &FilePointer) -> serde_json::Value {
    let mut record = serde_json::json!({
        "contentType": pointer.content_type,
        "fileName": pointer.file_name,
    });
    if let Some(locator) = &pointer.locator_info {
        record["size"] = serde_json::json!(locator.size);
    }
    record
}

fn quote_type_name(value: i32) -> &'static str {
    quote::Type::try_from(value)
        .map(|t| match t {
            quote::Type::GiftBadge => "giftBadge",
            quote::Type::ViewOnce => "viewOnce",
            quote::Type::Poll => "poll",
            _ => "normal",
        })
        .unwrap_or("normal")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        chat_item::{DirectionlessMessageDetails, IncomingMessageDetails, Item, PinDetails},
        chat_update_message, file_pointer, frame, group_change_chat_update,
        recipient::Destination,
        BackupInfo, ChatUpdateMessage, Contact, ExpirationTimerChatUpdate, Group,
        GroupChangeChatUpdate, GroupExpirationTimerUpdate, StandardMessage, Text,
    };
    use base64::Engine as _;
    use libsignal_message_backup::export::{
        aes_cbc_encrypt, gzip_compress, hmac_checksum, pad_gzipped_bucketed,
    };

    /// 16 字节档案 aci → 小写连字 UUID「6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b」。
    const CONTACT_ACI_BYTES: [u8; 16] = [
        0x6e, 0x08, 0xf0, 0xb6, 0x1c, 0x2d, 0x4e, 0x5f, 0x8a, 0x9b, 0x0c, 0x1d, 0x2e, 0x3f, 0x4a,
        0x5b,
    ];
    const CONTACT_E164: u64 = 15550000001;
    const GROUP_MASTER_KEY_BYTES: [u8; 32] = [9u8; 32];

    fn varint(mut value: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            out.push(if value == 0 { byte } else { byte | 0x80 });
            if value == 0 {
                return out;
            }
        }
    }

    fn frame_bytes(msg: &impl prost::Message) -> Vec<u8> {
        let body = msg.encode_to_vec();
        let mut out = varint(body.len());
        out.extend_from_slice(&body);
        out
    }

    fn encrypt_archive(frames: Vec<u8>, key: &MessageBackupKey) -> Vec<u8> {
        let mut gzipped = gzip_compress(futures::io::Cursor::new(frames));
        pad_gzipped_bucketed(&mut gzipped);
        let iv = [0x42u8; 16];
        aes_cbc_encrypt(&key.aes_key, &iv, &mut gzipped);
        let mac = hmac_checksum(&key.hmac_key, &iv, &gzipped);
        let mut out = Vec::with_capacity(16 + gzipped.len() + 32);
        out.extend_from_slice(&iv);
        out.extend_from_slice(&gzipped);
        out.extend_from_slice(&mac);
        out
    }

    #[tokio::test]
    async fn roundtrip_decrypts_and_exports_frames() {
        const TEST_KEY: [u8; 32] = [7u8; 32];
        let aci = presage::libsignal_service::prelude::Uuid::from_bytes([9u8; 16]);
        let backup_key: BackupKey = BackupKey(TEST_KEY);
        let backup_id =
            backup_key.derive_backup_id(&presage::libsignal_service::protocol::Aci::from(aci));
        let key = MessageBackupKey::derive(&backup_key, &backup_id, None);

        let info = frame_bytes(&BackupInfo {
            version: 1,
            backup_time_ms: 42,
            ..Default::default()
        });
        let recipient = frame_bytes(&Frame {
            item: Some(frame::Item::Recipient(Recipient {
                id: 7,
                destination: Some(Destination::Contact(Contact {
                    system_given_name: "A".into(),
                    system_family_name: "B".into(),
                    aci: Some(CONTACT_ACI_BYTES.to_vec()),
                    e164: Some(CONTACT_E164),
                    ..Default::default()
                })),
            })),
        });
        let group_recipient = frame_bytes(&Frame {
            item: Some(frame::Item::Recipient(Recipient {
                id: 8,
                destination: Some(Destination::Group(Group {
                    master_key: GROUP_MASTER_KEY_BYTES.to_vec(),
                    ..Default::default()
                })),
            })),
        });
        let chat = frame_bytes(&Frame {
            item: Some(frame::Item::Chat(Chat {
                id: 3,
                recipient_id: 7,
                archived: false,
                expiration_timer_ms: Some(86_400_000),
                expire_timer_version: 2,
                ..Default::default()
            })),
        });
        let item = frame_bytes(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 7,
                date_sent: 123,
                directional_details: Some(chat_item::DirectionalDetails::Incoming(
                    IncomingMessageDetails::default(),
                )),
                item: Some(Item::StandardMessage(StandardMessage {
                    text: Some(Text {
                        body: "hello".into(),
                        ..Default::default()
                    }),
                    attachments: vec![crate::proto::MessageAttachment {
                        pointer: Some(FilePointer {
                            content_type: Some("text/plain".into()),
                            file_name: Some("f.txt".into()),
                            locator_info: Some(file_pointer::LocatorInfo {
                                size: 5,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    quote: Some(crate::proto::Quote {
                        target_sent_timestamp: Some(100),
                        author_id: 8,
                        text: Some(Text {
                            body: "quoted".into(),
                            ..Default::default()
                        }),
                        r#type: quote::Type::Normal as i32,
                        attachments: Vec::new(),
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        });
        let pinned = frame_bytes(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 8,
                date_sent: 200,
                directional_details: Some(chat_item::DirectionalDetails::Outgoing(
                    chat_item::OutgoingMessageDetails::default(),
                )),
                item: Some(Item::StandardMessage(StandardMessage {
                    text: Some(Text {
                        body: "pinned".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                pin_details: Some(PinDetails {
                    pinned_at_timestamp: 300,
                    pin_expiry: None,
                }),
                ..Default::default()
            })),
        });
        // 未知帧 item（官方语义：跳过不报错）。
        let skipped = frame_bytes(&Frame { item: None });
        // E 批：1:1 计时器变更 update（官方导出 directionless 形态）成行。
        let timer_update = frame_bytes(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 7,
                date_sent: 400,
                directional_details: Some(chat_item::DirectionalDetails::Directionless(
                    DirectionlessMessageDetails::default(),
                )),
                item: Some(Item::UpdateMessage(ChatUpdateMessage {
                    update: Some(chat_update_message::Update::ExpirationTimerChange(
                        ExpirationTimerChatUpdate {
                            expires_in_ms: 86_400_000,
                        },
                    )),
                })),
                ..Default::default()
            })),
        });

        let payload: Vec<u8> = [
            info,
            recipient,
            group_recipient,
            chat,
            item,
            pinned,
            timer_update,
            skipped,
        ]
        .into_iter()
        .flatten()
        .collect();
        let archive = encrypt_archive(payload, &key);
        let dir = std::env::temp_dir().join("kt-engine-history-test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("fixture.bin");
        std::fs::write(&path, &archive).unwrap();

        let mut out: Vec<u8> = Vec::new();
        let stats = export_archive(&path, aci, TEST_KEY, &mut out)
            .await
            .expect("export");
        assert_eq!(
            stats,
            ExportStats {
                backup_version: 1,
                frames: 8,
                recipients: 2,
                chats: 1,
                messages: 3,
                remote_deleted: 0,
                skipped_items: 0,
                skipped_frames: 1,
            }
        );

        let lines: Vec<serde_json::Value> = std::str::from_utf8(&out)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 6);
        // E5：会话归属身份随帧投影（connector §4.38 消费形状）。
        assert_eq!(lines[0]["kind"], "contact");
        assert_eq!(lines[0]["name"], "A B");
        assert_eq!(lines[0]["aci"], "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b");
        assert_eq!(lines[0]["e164"], CONTACT_E164);
        assert!(lines[0]["e164"].is_u64(), "e164 以 JSON number 投影");
        assert_eq!(lines[1]["kind"], "group",);
        assert_eq!(
            lines[1]["masterKey"],
            base64::engine::general_purpose::STANDARD.encode(GROUP_MASTER_KEY_BYTES)
        );
        // E 批：chat 级计时器随帧投影。
        assert_eq!(lines[2]["expirationTimerMs"], 86_400_000);
        assert_eq!(lines[2]["expireTimerVersion"], 2);
        assert_eq!(lines[3]["text"], "hello");
        assert_eq!(lines[3]["direction"], "incoming");
        assert_eq!(lines[3]["attachments"][0]["size"], 5);
        assert_eq!(lines[3]["quote"]["text"], "quoted");
        assert_eq!(lines[4]["pinnedAtTimestamp"], 300);
        assert_eq!(lines[4]["direction"], "outgoing");
        // E 批：计时器变更 update 成行（计入 messages），connector 拿去做 system 行。
        assert_eq!(lines[5]["expirationTimerChange"]["expiresInMs"], 86_400_000);
        assert_eq!(lines[5]["direction"], "directionless");

        std::fs::remove_file(&path).ok();
    }

    /// E5 三面钉投影：有身份（contact aci+e164 / group masterKey）/ 缺身份
    /// （全键缺席）/ 畸形身份（aci 非 16B、e164=0 → 省略键不发明占位值）。
    #[test]
    fn recipient_identity_projection_faces() {
        let with_identity = recipient_record(&Recipient {
            id: 1,
            destination: Some(Destination::Contact(Contact {
                aci: Some(CONTACT_ACI_BYTES.to_vec()),
                e164: Some(CONTACT_E164),
                ..Default::default()
            })),
        });
        assert_eq!(with_identity["aci"], "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b");
        assert_eq!(with_identity["e164"], CONTACT_E164);
        assert!(
            with_identity["e164"].is_u64(),
            "JSON number，connector normalize_e164 双形态兼容"
        );
        let group = recipient_record(&Recipient {
            id: 2,
            destination: Some(Destination::Group(Group {
                master_key: GROUP_MASTER_KEY_BYTES.to_vec(),
                ..Default::default()
            })),
        });
        assert_eq!(
            group["masterKey"],
            base64::engine::general_purpose::STANDARD.encode(GROUP_MASTER_KEY_BYTES)
        );

        // 缺身份：contact 无 aci/e164、group 空 master key、self（proto 只带
        // avatar_color）、destination 未设（官方语义跳过不报错）——全部无身份键。
        let missing: Vec<(&str, serde_json::Value)> = vec![
            (
                "contact-no-identity",
                recipient_record(&Recipient {
                    id: 3,
                    destination: Some(Destination::Contact(Contact::default())),
                }),
            ),
            (
                "empty-master-key",
                recipient_record(&Recipient {
                    id: 4,
                    destination: Some(Destination::Group(Group::default())),
                }),
            ),
            (
                "self",
                recipient_record(&Recipient {
                    id: 5,
                    destination: Some(Destination::Self_(crate::proto::Self_::default())),
                }),
            ),
            (
                "unset-destination",
                recipient_record(&Recipient {
                    id: 6,
                    destination: None,
                }),
            ),
        ];
        for (name, record) in missing {
            assert!(record.get("aci").is_none(), "{name}: aci 不得投影");
            assert!(record.get("e164").is_none(), "{name}: e164 不得投影");
            assert!(
                record.get("masterKey").is_none(),
                "{name}: masterKey 不得投影"
            );
            assert!(record.get("kind").is_some(), "{name}: 基础面仍在");
        }

        // 畸形身份：aci 非 16 字节、e164=0（E.164 不存在 0 号）→ 省略键，
        // 不投占位值（connector 视作缺席，会话归属失败如实 skippedChats）。
        let malformed = recipient_record(&Recipient {
            id: 7,
            destination: Some(Destination::Contact(Contact {
                aci: Some(vec![0x11; 15]),
                e164: Some(0),
                ..Default::default()
            })),
        });
        assert!(malformed.get("aci").is_none());
        assert!(malformed.get("e164").is_none());
    }

    /// E 批计时器变更行三面：1:1 变更（含 0=关闭语义）、群变更（批内扫描 +
    /// updaterAci 有/无/畸形）、非计时器 update 与方向缺失仍整行跳过。
    #[test]
    fn timer_update_projection_faces() {
        // 1:1：expiresInMs 原样透出；directionless（官方导出形态）成行。
        let one_to_one = message_record(&ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 400,
            directional_details: Some(chat_item::DirectionalDetails::Directionless(
                DirectionlessMessageDetails::default(),
            )),
            item: Some(Item::UpdateMessage(ChatUpdateMessage {
                update: Some(chat_update_message::Update::ExpirationTimerChange(
                    ExpirationTimerChatUpdate {
                        expires_in_ms: 86_400_000,
                    },
                )),
            })),
            ..Default::default()
        })
        .expect("1:1 计时器变更是行");
        assert_eq!(one_to_one["direction"], "directionless");
        assert_eq!(
            one_to_one["expirationTimerChange"]["expiresInMs"],
            86_400_000
        );

        // 0=关闭语义原样保留，不吞成「无键」。
        let disabled = timer_change_record(&ChatUpdateMessage {
            update: Some(chat_update_message::Update::ExpirationTimerChange(
                ExpirationTimerChatUpdate { expires_in_ms: 0 },
            )),
        })
        .expect("0=关闭仍是行");
        assert_eq!(disabled["expiresInMs"], 0);

        // 群：批内第一个 GroupExpirationTimerUpdate 命中，updaterAci 小写 UUID。
        let group_update = |timer: GroupExpirationTimerUpdate| {
            ChatUpdateMessage {
            update: Some(chat_update_message::Update::GroupChange(
                GroupChangeChatUpdate {
                    updates: vec![
                        group_change_chat_update::Update {
                            update: Some(
                                group_change_chat_update::update::Update::GroupNameUpdate(
                                    Default::default(),
                                ),
                            ),
                        },
                        group_change_chat_update::Update {
                            update: Some(
                                group_change_chat_update::update::Update::GroupExpirationTimerUpdate(
                                    timer,
                                ),
                            ),
                        },
                    ],
                },
            )),
        }
        };
        let group = timer_change_record(&group_update(GroupExpirationTimerUpdate {
            expires_in_ms: 604_800_000,
            updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
        }))
        .expect("群计时器变更是行");
        assert_eq!(group["expiresInMs"], 604_800_000);
        assert_eq!(group["updaterAci"], "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b");

        // updaterAci 缺席/畸形（非 16B）→ 键省略，其余面照投。
        for (name, timer) in [
            (
                "no-updater",
                GroupExpirationTimerUpdate {
                    expires_in_ms: 604_800_000,
                    updater_aci: None,
                },
            ),
            (
                "malformed-updater",
                GroupExpirationTimerUpdate {
                    expires_in_ms: 604_800_000,
                    updater_aci: Some(vec![0x11; 15]),
                },
            ),
        ] {
            let record = timer_change_record(&group_update(timer)).expect(name);
            assert!(record.get("updaterAci").is_none(), "{name}: 键必须省略");
            assert_eq!(record["expiresInMs"], 604_800_000);
        }

        // 非计时器 update（群改名 / SimpleUpdate）与 update 未设 → 整行跳过。
        for (name, update) in [
            (
                "group-name-only",
                chat_update_message::Update::GroupChange(GroupChangeChatUpdate {
                    updates: vec![group_change_chat_update::Update {
                        update: Some(group_change_chat_update::update::Update::GroupNameUpdate(
                            Default::default(),
                        )),
                    }],
                }),
            ),
            (
                "simple-update",
                chat_update_message::Update::SimpleUpdate(Default::default()),
            ),
        ] {
            assert!(
                timer_change_record(&ChatUpdateMessage {
                    update: Some(update)
                })
                .is_none(),
                "{name}: 非计时器 update 不投行"
            );
        }
        assert!(
            timer_change_record(&ChatUpdateMessage { update: None }).is_none(),
            "update 未设不投行"
        );

        // 方向缺失（官方导入器跳过的形态）→ 计入 skippedItems 不投行。
        assert!(message_record(&ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 401,
            item: Some(Item::UpdateMessage(ChatUpdateMessage {
                update: Some(chat_update_message::Update::ExpirationTimerChange(
                    ExpirationTimerChatUpdate {
                        expires_in_ms: 86_400_000,
                    },
                )),
            })),
            ..Default::default()
        })
        .is_none());
    }

    /// E 批 chat/item 计时器字段两面：有（原样透出）/ 缺（键不在；proto3
    /// 标量 0 视同未设；item 级 Some(0) 不投）。
    #[test]
    fn chat_and_item_timer_field_faces() {
        let chat = chat_record(&Chat {
            id: 3,
            recipient_id: 7,
            expiration_timer_ms: Some(86_400_000),
            expire_timer_version: 2,
            ..Default::default()
        });
        assert_eq!(chat["expirationTimerMs"], 86_400_000);
        assert_eq!(chat["expireTimerVersion"], 2);

        let bare = chat_record(&Chat {
            id: 4,
            recipient_id: 8,
            ..Default::default()
        });
        assert!(bare.get("expirationTimerMs").is_none());
        assert!(bare.get("expireTimerVersion").is_none());

        let item = |expire_start_date: Option<u64>, expires_in_ms: Option<u64>| ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 1,
            expire_start_date,
            expires_in_ms,
            directional_details: Some(chat_item::DirectionalDetails::Incoming(
                IncomingMessageDetails::default(),
            )),
            item: Some(Item::StandardMessage(StandardMessage {
                text: Some(Text {
                    body: "hi".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        let with = message_record(&item(Some(1_000), Some(86_400_000))).expect("行");
        assert_eq!(with["expireStartDate"], 1_000);
        assert_eq!(with["expiresInMs"], 86_400_000);
        let without = message_record(&item(Some(0), None)).expect("行");
        assert!(without.get("expireStartDate").is_none());
        assert!(without.get("expiresInMs").is_none());
    }

    #[tokio::test]
    async fn hmac_mismatch_is_rejected() {
        const TEST_KEY: [u8; 32] = [7u8; 32];
        let aci = presage::libsignal_service::prelude::Uuid::from_bytes([9u8; 16]);
        let backup_key: BackupKey = BackupKey(TEST_KEY);
        let backup_id =
            backup_key.derive_backup_id(&presage::libsignal_service::protocol::Aci::from(aci));
        let key = MessageBackupKey::derive(&backup_key, &backup_id, None);

        let info = frame_bytes(&BackupInfo {
            version: 1,
            ..Default::default()
        });
        let mut archive = encrypt_archive(info, &key);
        let last = archive.len() - 1;
        archive[last] ^= 0xff; // 篡改 MAC 尾字节

        let dir = std::env::temp_dir().join("kt-engine-history-test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("badmac.bin");
        std::fs::write(&path, &archive).unwrap();

        let mut out: Vec<u8> = Vec::new();
        let error = export_archive(&path, aci, TEST_KEY, &mut out)
            .await
            .expect_err("must fail");
        // MAC 校验在读流读到档案尾时即由 FramesReader 触发
        // （ValidationError::InvalidHmac），显式 verify_hmac() 是兜底校验点。
        assert!(
            matches!(
                error,
                HistoryError::Frames(
                    libsignal_message_backup::frame::ValidationError::InvalidHmac(_)
                )
            ),
            "got {error:?}"
        );
        std::fs::remove_file(&path).ok();
    }
}
