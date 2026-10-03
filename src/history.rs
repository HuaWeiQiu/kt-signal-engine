// SPDX-License-Identifier: AGPL-3.0-only

//! ADR 0005 S2：transfer archive 解密 + 分帧 + NDJSON 导出。
//!
//! 解密面全官方（`FramesReader`：HMAC-SHA256 尾验证 + AES-256-CBC + gzip +
//! padding 剥离；`VarintDelimitedReader` varint 分帧），帧 proto 为自持生成代码
//! （`crate::proto`，官方开源 schema）。逐帧流式转 receive-compatible 中间表示
//! （NDJSON），O(1) 帧内存。v1 范围与跳过策略见 ADR 0005 §S2 设计定案。

use std::io::Write;
use std::path::Path;

use libsignal_account_keys::BackupKey;
use libsignal_message_backup::frame::{FileReaderFactory, FramesReader, VerifyHmac};
use libsignal_message_backup::key::MessageBackupKey;
use libsignal_message_backup::parse::VarintDelimitedReader;
use prost::Message as _;

use crate::proto::group::GroupAttributeBlob;
use crate::proto::{
    chat_item, frame, quote, recipient, Chat, ChatItem, FilePointer, Frame, Recipient,
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
    serde_json::json!({
        "type": "recipient",
        "id": r.id,
        "kind": kind,
        "name": name,
    })
}

/// GroupAttributeBlob 的 oneof：title 场景取 `Title` 变体。
fn blob_string(blob: &GroupAttributeBlob) -> String {
    match &blob.content {
        Some(crate::proto::group::group_attribute_blob::Content::Title(s)) => s.clone(),
        _ => String::new(),
    }
}

fn chat_record(c: &Chat) -> serde_json::Value {
    serde_json::json!({
        "type": "chat",
        "id": c.id,
        "recipientId": c.recipient_id,
        "archived": c.archived,
        "pinnedOrder": c.pinned_order,
    })
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
        // 其余 item 类型 v1 跳过（调用方计数）；方向可用的 update/call 类未来再扩。
        _ => return None,
    };
    Some(record)
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
        chat_item::{IncomingMessageDetails, Item, PinDetails},
        file_pointer, frame,
        recipient::Destination,
        BackupInfo, Contact, StandardMessage, Text,
    };
    use libsignal_message_backup::export::{
        aes_cbc_encrypt, gzip_compress, hmac_checksum, pad_gzipped_bucketed,
    };

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
                    ..Default::default()
                })),
            })),
        });
        let chat = frame_bytes(&Frame {
            item: Some(frame::Item::Chat(Chat {
                id: 3,
                recipient_id: 7,
                archived: false,
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

        let payload: Vec<u8> = [info, recipient, chat, item, pinned, skipped]
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
                frames: 6,
                recipients: 1,
                chats: 1,
                messages: 2,
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
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0]["kind"], "contact");
        assert_eq!(lines[0]["name"], "A B");
        assert_eq!(lines[2]["text"], "hello");
        assert_eq!(lines[2]["direction"], "incoming");
        assert_eq!(lines[2]["attachments"][0]["size"], 5);
        assert_eq!(lines[2]["quote"]["text"], "quoted");
        assert_eq!(lines[3]["pinnedAtTimestamp"], 300);
        assert_eq!(lines[3]["direction"], "outgoing");

        std::fs::remove_file(&path).ok();
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
