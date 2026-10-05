// SPDX-License-Identifier: AGPL-3.0-only

//! 帧构造 + 帧序状态机（`docs/backup-export-contract.md` §6/§7）。
//!
//! 官方 Backup.proto 帧序规则（proto 头注释 + validator 硬校验双保险）：
//!
//! 1. 流面：varint 分帧，首条目是 `BackupInfo`，其后每条目是一个 `Frame`。
//! 2. 恰一个 `AccountData` 且为第一个 Frame。
//! 3. 被引用帧（Recipient）先于引用帧（Chat/ChatItem）。
//! 4. 恰一个 Self recipient；恰一个 ALL ChatFolder（有 folder 时）。
//! 5. contact 的 aci/e164/pni/username 与 group 的 masterKey 全局去重。
//!
//! 本模块是引用完整性的引擎侧闸：违规构造一律 fail-closed 返回
//! [`FrameSequenceError`]，保证经本状态机产出的帧流可被官方
//! `BackupReader::validate_all` 语义校验通过（id 分配、去重、引用前置全在
//! 这里闭环）。ChatItem 全局渲染序（官方规则 3，validator 不查）由调用方按
//! 接收序调用保证。

use std::collections::HashMap;

use crate::proto;

/// 聊天消息方向（v2 帧面 `directionalDetails` 的 MVP 两个方向）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatItemDirection {
    /// 收信：`Incoming{dateReceived, read, sealedSender=false}`。
    Incoming { date_received_ms: u64, read: bool },
    /// 发信：`Outgoing{dateReceived, sendStatus=[]}`。
    Outgoing { date_received_ms: u64 },
}

/// 帧序违规（全部 fail-closed：构造即拒绝，不产半成品帧流）。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameSequenceError {
    #[error("BackupInfo frame must be emitted exactly once, first")]
    BackupInfoNotFirst,
    #[error("BackupInfo.mediaRootBackupKey must be 32 bytes, got {0}")]
    InvalidMediaRootBackupKey(usize),
    #[error("AccountData must be the first Frame, exactly once")]
    AccountDataPlacement,
    #[error("AccountData.profileKey must be 32 bytes, got {0}")]
    InvalidProfileKey(usize),
    #[error("AccountData.phone number sharing mode must be Everybody or Nobody (validator rejects Unknown)")]
    InvalidPhoneNumberSharingMode,
    #[error("svrPin is account credentials: never exported (contract §8.3)")]
    SvrPinExportForbidden,
    #[error("contact aci/pni must be 16 bytes, got {0}")]
    InvalidContactServiceId(usize),
    #[error("contact e164 must be non-zero")]
    InvalidContactE164,
    #[error("contact must carry at least one of aci/e164/pni")]
    ContactWithoutIdentity,
    #[error("contact identity already registered as recipient {existing}")]
    DuplicateContactIdentity { existing: u64 },
    #[error("group master key already registered as recipient {0}")]
    DuplicateGroupMasterKey(u64),
    #[error("self recipient must be registered exactly once")]
    DuplicateSelfRecipient,
    #[error("recipient {0} not registered (reference-before-registration)")]
    UnknownRecipient(u64),
    #[error("recipient {0} cannot author a chat item (only self/contact with aci or e164)")]
    InvalidChatAuthor(u64),
    #[error("chat {0} not registered (reference-before-registration)")]
    UnknownChat(u64),
    #[error("message body must not be empty (validator rejects empty text)")]
    EmptyMessageBody,
    #[error("message body was {0} bytes (validator cap 128 KiB)")]
    MessageBodyTooLong(usize),
    #[error("ALL chat folder must be emitted exactly once")]
    DuplicateAllChatFolder,
}

/// recipient 登记种类（作者校验与 chat 资格判定用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecipientKind {
    Self_,
    Contact { has_aci_or_e164: bool },
    Group,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecipientRecord {
    kind: RecipientKind,
}

/// ALL chat folder 的 folder id：官方 validator 只校验 16B 长度，内容不查
/// （Android 端生成随机 UUID）。固定常量保证同输入导出可复现。
const ALL_FOLDER_ID: [u8; 16] = *b"KT-all-chat-fold";

/// Self recipient 固定 id（Android 导出惯例位，validator 无硬性要求）。
const SELF_RECIPIENT_ID: u64 = 1;

/// 帧序状态机：BackupInfo → AccountData → Recipient* → Chat*/ChatItem* →
/// ChatFolder/ChatItem*，违规 fail-closed（契约 §7）。
///
/// id 分配口径：self recipient 恒为 1（官方 validator 对 Self id 无硬性要求，
/// 但 1 是 Android 导出的惯例位），contact/group 自 2 起自动分配；chat id 自 1
/// 起自动分配。ALL folder id 用固定 16B 常量（validator 只查长度）。
pub struct FrameSequencer {
    backup_info: Option<proto::BackupInfo>,
    account_data_emitted: bool,
    self_recipient_id: Option<u64>,
    recipients: HashMap<u64, RecipientRecord>,
    by_aci: HashMap<[u8; 16], u64>,
    by_pni: HashMap<[u8; 16], u64>,
    by_e164: HashMap<u64, u64>,
    by_username: HashMap<String, u64>,
    by_group_master_key: HashMap<[u8; 32], u64>,
    next_recipient_id: u64,
    chats: HashMap<u64, u64>,
    next_chat_id: u64,
    all_chat_folder_emitted: bool,
}

impl FrameSequencer {
    pub fn new(backup_info: proto::BackupInfo) -> Self {
        Self {
            backup_info: Some(backup_info),
            account_data_emitted: false,
            self_recipient_id: None,
            recipients: HashMap::new(),
            by_aci: HashMap::new(),
            by_pni: HashMap::new(),
            by_e164: HashMap::new(),
            by_username: HashMap::new(),
            by_group_master_key: HashMap::new(),
            next_recipient_id: 2,
            chats: HashMap::new(),
            next_chat_id: 1,
            all_chat_folder_emitted: false,
        }
    }

    /// 首条目：BackupInfo（恰一次；mediaRootBackupKey 32B 硬校验，validator
    /// `InvalidMediaRootBackupKey` 同款口径前置）。
    pub fn backup_info(&mut self) -> Result<proto::BackupInfo, FrameSequenceError> {
        let info = self
            .backup_info
            .take()
            .ok_or(FrameSequenceError::BackupInfoNotFirst)?;
        if info.media_root_backup_key.len() != 32 {
            return Err(FrameSequenceError::InvalidMediaRootBackupKey(
                info.media_root_backup_key.len(),
            ));
        }
        Ok(info)
    }

    /// 第一个 Frame：AccountData（恰一次）。svrPin 恒不导出（fail-closed）；
    /// account settings 缺省时补最小合法面（validator 对 phone sharing、sent
    /// media quality、app theme、calls use less data 四枚举均为 UNKNOWN 硬错，
    /// 缺省取 Signal-Android 对应默认值：NOBODY / HIGH / SYSTEM /
    /// WIFI_AND_MOBILE_DATA）；已带 settings 时未设置（UNKNOWN）的上述枚举
    /// 同款补默认；profileKey 32B 前置校验。
    pub fn account_data(
        &mut self,
        mut account: proto::AccountData,
    ) -> Result<proto::Frame, FrameSequenceError> {
        if self.backup_info.is_some() || self.account_data_emitted {
            return Err(FrameSequenceError::AccountDataPlacement);
        }
        self.account_data_emitted = true;

        if !account.svr_pin.is_empty() {
            return Err(FrameSequenceError::SvrPinExportForbidden);
        }
        if account.profile_key.len() != 32 {
            return Err(FrameSequenceError::InvalidProfileKey(
                account.profile_key.len(),
            ));
        }
        match account.account_settings.as_mut() {
            None => {
                account.account_settings = Some(proto::account_data::AccountSettings {
                    phone_number_sharing_mode: proto::account_data::PhoneNumberSharingMode::Nobody
                        as i32,
                    default_sent_media_quality: proto::account_data::SentMediaQuality::High as i32,
                    app_theme: proto::account_data::AppTheme::System as i32,
                    calls_use_less_data_setting:
                        proto::account_data::CallsUseLessDataSetting::WifiAndMobileData as i32,
                    ..Default::default()
                });
            }
            Some(settings) => {
                if !matches!(
                    settings.phone_number_sharing_mode,
                    x if x == proto::account_data::PhoneNumberSharingMode::Everybody as i32
                        || x == proto::account_data::PhoneNumberSharingMode::Nobody as i32
                ) {
                    return Err(FrameSequenceError::InvalidPhoneNumberSharingMode);
                }
                let enums = [
                    (
                        &mut settings.default_sent_media_quality,
                        proto::account_data::SentMediaQuality::UnknownQuality as i32,
                        proto::account_data::SentMediaQuality::High as i32,
                    ),
                    (
                        &mut settings.app_theme,
                        proto::account_data::AppTheme::UnknownAppTheme as i32,
                        proto::account_data::AppTheme::System as i32,
                    ),
                    (
                        &mut settings.calls_use_less_data_setting,
                        proto::account_data::CallsUseLessDataSetting::UnknownCallDataSetting as i32,
                        proto::account_data::CallsUseLessDataSetting::WifiAndMobileData as i32,
                    ),
                ];
                for (field, unknown, default) in enums {
                    if *field == unknown {
                        *field = default;
                    }
                }
            }
        }

        Ok(proto::Frame {
            item: Some(proto::frame::Item::Account(account)),
        })
    }

    /// Self recipient（恰一次，id=1）。
    pub fn self_recipient(&mut self) -> Result<(u64, proto::Frame), FrameSequenceError> {
        if self.self_recipient_id.is_some() {
            return Err(FrameSequenceError::DuplicateSelfRecipient);
        }
        self.self_recipient_id = Some(SELF_RECIPIENT_ID);
        self.recipients.insert(
            SELF_RECIPIENT_ID,
            RecipientRecord {
                kind: RecipientKind::Self_,
            },
        );
        Ok((
            SELF_RECIPIENT_ID,
            proto::Frame {
                item: Some(proto::frame::Item::Recipient(proto::Recipient {
                    id: SELF_RECIPIENT_ID,
                    destination: Some(proto::recipient::Destination::Self_(proto::Self_ {
                        avatar_color: None,
                    })),
                })),
            },
        ))
    }

    /// Contact recipient：aci/e164/pni/username 全局去重（validator
    /// DuplicateContact* 同款口径，username 按 ASCII 小写归一），自动分配 id。
    pub fn contact_recipient(
        &mut self,
        contact: proto::Contact,
    ) -> Result<(u64, proto::Frame), FrameSequenceError> {
        let aci: Option<[u8; 16]> = match contact.aci.as_deref() {
            None => None,
            Some(bytes) => match <[u8; 16]>::try_from(bytes) {
                Ok(array) => Some(array),
                Err(_) => return Err(FrameSequenceError::InvalidContactServiceId(bytes.len())),
            },
        };
        let pni: Option<[u8; 16]> = match contact.pni.as_deref() {
            None => None,
            Some(bytes) => match <[u8; 16]>::try_from(bytes) {
                Ok(array) => Some(array),
                Err(_) => return Err(FrameSequenceError::InvalidContactServiceId(bytes.len())),
            },
        };
        let e164 = contact.e164;
        if e164.is_some_and(|v| v == 0) {
            return Err(FrameSequenceError::InvalidContactE164);
        }
        if aci.is_none() && pni.is_none() && e164.is_none() {
            return Err(FrameSequenceError::ContactWithoutIdentity);
        }

        let conflicts = [
            aci.and_then(|k| self.by_aci.get(&k).copied()),
            pni.and_then(|k| self.by_pni.get(&k).copied()),
            e164.and_then(|k| self.by_e164.get(&k).copied()),
            contact
                .username
                .as_deref()
                .and_then(|u| self.by_username.get(&u.to_ascii_lowercase()).copied()),
        ];
        if let Some(existing) = conflicts.into_iter().flatten().next() {
            return Err(FrameSequenceError::DuplicateContactIdentity { existing });
        }

        let id = self.next_recipient_id;
        self.next_recipient_id += 1;
        if let Some(aci) = aci {
            self.by_aci.insert(aci, id);
        }
        if let Some(pni) = pni {
            self.by_pni.insert(pni, id);
        }
        if let Some(e164) = e164 {
            self.by_e164.insert(e164, id);
        }
        if let Some(username) = contact.username.as_deref() {
            self.by_username.insert(username.to_ascii_lowercase(), id);
        }
        self.recipients.insert(
            id,
            RecipientRecord {
                kind: RecipientKind::Contact {
                    has_aci_or_e164: aci.is_some() || e164.is_some(),
                },
            },
        );

        Ok((
            id,
            proto::Frame {
                item: Some(proto::frame::Item::Recipient(proto::Recipient {
                    id,
                    destination: Some(proto::recipient::Destination::Contact(contact)),
                })),
            },
        ))
    }

    /// Group recipient（masterKey 去重，自动分配 id）。
    pub fn group_recipient(
        &mut self,
        master_key: [u8; 32],
    ) -> Result<(u64, proto::Frame), FrameSequenceError> {
        if let Some(&existing) = self.by_group_master_key.get(&master_key) {
            return Err(FrameSequenceError::DuplicateGroupMasterKey(existing));
        }
        let id = self.next_recipient_id;
        self.next_recipient_id += 1;
        self.by_group_master_key.insert(master_key, id);
        self.recipients.insert(
            id,
            RecipientRecord {
                kind: RecipientKind::Group,
            },
        );
        Ok((
            id,
            proto::Frame {
                item: Some(proto::frame::Item::Recipient(proto::Recipient {
                    id,
                    destination: Some(proto::recipient::Destination::Group(proto::Group {
                        master_key: master_key.to_vec(),
                        ..Default::default()
                    })),
                })),
            },
        ))
    }

    /// Chat 帧（recipient 必须已登记；自动分配 chat id）。
    pub fn chat(&mut self, recipient_id: u64) -> Result<(u64, proto::Frame), FrameSequenceError> {
        if !self.recipients.contains_key(&recipient_id) {
            return Err(FrameSequenceError::UnknownRecipient(recipient_id));
        }
        let id = self.next_chat_id;
        self.next_chat_id += 1;
        self.chats.insert(id, recipient_id);
        Ok((
            id,
            proto::Frame {
                item: Some(proto::frame::Item::Chat(proto::Chat {
                    id,
                    recipient_id,
                    ..Default::default()
                })),
            },
        ))
    }

    /// 文本消息帧（chat/author 必须已登记；author 限 self 或带 aci/e164 的
    /// contact；body 非空且 ≤128KiB——validator EmptyBody/TooLongBody 前置）。
    pub fn chat_item_text(
        &mut self,
        chat_id: u64,
        author_id: u64,
        date_sent_ms: u64,
        direction: ChatItemDirection,
        body: &str,
    ) -> Result<proto::Frame, FrameSequenceError> {
        if !self.chats.contains_key(&chat_id) {
            return Err(FrameSequenceError::UnknownChat(chat_id));
        }
        match self.recipients.get(&author_id).map(|r| r.kind) {
            None => return Err(FrameSequenceError::UnknownRecipient(author_id)),
            Some(RecipientKind::Self_) => {}
            Some(RecipientKind::Contact { has_aci_or_e164 }) => {
                if !has_aci_or_e164 {
                    return Err(FrameSequenceError::InvalidChatAuthor(author_id));
                }
            }
            Some(RecipientKind::Group) => {
                return Err(FrameSequenceError::InvalidChatAuthor(author_id))
            }
        }
        if body.is_empty() {
            return Err(FrameSequenceError::EmptyMessageBody);
        }
        const MAX_BODY: usize = 128 * 1024;
        if body.len() > MAX_BODY {
            return Err(FrameSequenceError::MessageBodyTooLong(body.len()));
        }

        let directional_details = match direction {
            ChatItemDirection::Incoming {
                date_received_ms,
                read,
            } => proto::chat_item::DirectionalDetails::Incoming(
                proto::chat_item::IncomingMessageDetails {
                    date_received: date_received_ms,
                    date_server_sent: None,
                    read,
                    sealed_sender: false,
                },
            ),
            ChatItemDirection::Outgoing { date_received_ms } => {
                proto::chat_item::DirectionalDetails::Outgoing(
                    proto::chat_item::OutgoingMessageDetails {
                        send_status: Vec::new(),
                        date_received: date_received_ms,
                    },
                )
            }
        };

        Ok(proto::Frame {
            item: Some(proto::frame::Item::ChatItem(proto::ChatItem {
                chat_id,
                author_id,
                date_sent: date_sent_ms,
                directional_details: Some(directional_details),
                item: Some(proto::chat_item::Item::StandardMessage(
                    proto::StandardMessage {
                        text: Some(proto::Text {
                            body: body.to_owned(),
                            body_ranges: Vec::new(),
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
        })
    }

    /// ALL chat folder（恰一次；validator `validate_all_chat_folder` 要求的
    /// 规范字段面：空名、显示静默、含全部单聊群聊、无包含/排除集、16B id）。
    pub fn all_chat_folder(&mut self) -> Result<proto::Frame, FrameSequenceError> {
        if self.all_chat_folder_emitted {
            return Err(FrameSequenceError::DuplicateAllChatFolder);
        }
        self.all_chat_folder_emitted = true;
        Ok(proto::Frame {
            item: Some(proto::frame::Item::ChatFolder(proto::ChatFolder {
                name: String::new(),
                show_only_unread: false,
                show_muted_chats: true,
                include_all_individual_chats: true,
                include_all_group_chats: true,
                folder_type: proto::chat_folder::FolderType::All as i32,
                included_recipient_ids: Vec::new(),
                excluded_recipient_ids: Vec::new(),
                id: ALL_FOLDER_ID.to_vec(),
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::backup::container::BackupStreamWriter;
    use crate::backup::keys::tests_official_keys;

    fn backup_info() -> proto::BackupInfo {
        proto::BackupInfo {
            version: 1,
            backup_time_ms: 1_760_000_000_000,
            media_root_backup_key: vec![0x42; 32],
            current_app_version: "kt-signal-engine 0.1.0".into(),
            first_app_version: "kt-signal-engine 0.1.0".into(),
            debug_info: b"kt-signal-engine backup export mvp".to_vec(),
        }
    }

    fn contact(aci_bytes: [u8; 16], e164: u64) -> proto::Contact {
        proto::Contact {
            aci: Some(aci_bytes.to_vec()),
            e164: Some(e164),
            profile_key: Some(vec![7u8; 32]),
            profile_given_name: Some("Ada".into()),
            registration: Some(proto::contact::Registration::Registered(
                proto::contact::Registered {},
            )),
            ..Default::default()
        }
    }

    fn account() -> proto::AccountData {
        proto::AccountData {
            profile_key: vec![0x11; 32],
            given_name: "Kit".into(),
            ..Default::default()
        }
    }

    fn encode(msg: &impl Message) -> Vec<u8> {
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("encode into Vec");
        buf
    }

    fn decode_frame(bytes: &[u8]) -> proto::Frame {
        proto::Frame::decode(bytes).expect("frame decodes")
    }

    /// 官方 androidTest 黄金样本（varint 分帧流：BackupInfo + Frames）。
    /// 既验证生成代码与官方字节兼容，也验证帧序状态机的官方输入认知。
    #[test]
    fn official_binproto_fixtures_decode_with_expected_shapes() {
        fn read_varint(buf: &[u8], mut i: usize) -> (u64, usize) {
            let mut value = 0u64;
            let mut shift = 0;
            loop {
                let byte = buf[i];
                i += 1;
                value |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return (value, i);
                }
                shift += 7;
            }
        }

        type FrameAsserter = fn(&[proto::Frame]);

        let cases: &[(&str, FrameAsserter)] = &[
            ("account_data_00", |frames: &[proto::Frame]| {
                assert!(matches!(
                    frames[0].item,
                    Some(proto::frame::Item::Account(_))
                ));
            }),
            ("recipient_self_00", |frames: &[proto::Frame]| {
                assert!(frames.iter().any(|f| matches!(
                    f.item,
                    Some(proto::frame::Item::Recipient(proto::Recipient {
                        destination: Some(proto::recipient::Destination::Self_(_)),
                        ..
                    }))
                )));
            }),
            ("recipient_contacts_00", |frames: &[proto::Frame]| {
                assert!(frames.iter().any(|f| matches!(
                    f.item,
                    Some(proto::frame::Item::Recipient(proto::Recipient {
                        destination: Some(proto::recipient::Destination::Contact(_)),
                        ..
                    }))
                )));
            }),
            ("chat_00", |frames: &[proto::Frame]| {
                assert!(frames
                    .iter()
                    .any(|f| matches!(f.item, Some(proto::frame::Item::Chat(_)))));
            }),
            (
                "chat_item_standard_message_text_only_00",
                |frames: &[proto::Frame]| {
                    assert!(frames.iter().any(|f| matches!(
                        f.item,
                        Some(proto::frame::Item::ChatItem(proto::ChatItem {
                            item: Some(proto::chat_item::Item::StandardMessage(_)),
                            ..
                        }))
                    )));
                },
            ),
            (
                "chat_item_standard_message_with_quote_00",
                |frames: &[proto::Frame]| {
                    assert!(frames.iter().any(|f| matches!(
                        f.item,
                        Some(proto::frame::Item::ChatItem(proto::ChatItem {
                            item: Some(proto::chat_item::Item::StandardMessage(
                                proto::StandardMessage { quote: Some(_), .. }
                            )),
                            ..
                        }))
                    )));
                },
            ),
            ("chat_folder_00", |frames: &[proto::Frame]| {
                assert!(frames.iter().any(|f| matches!(
                    f.item,
                    Some(proto::frame::Item::ChatFolder(proto::ChatFolder {
                        folder_type,
                        ..
                    })) if folder_type == proto::chat_folder::FolderType::All as i32
                )));
            }),
        ];

        for (name, check) in cases {
            let path = format!(
                "{}/tests/fixtures/signal-backup-tests/{name}.binproto",
                env!("CARGO_MANIFEST_DIR")
            );
            let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {name}: {e}"));
            let mut cursor = 0usize;
            let mut messages: Vec<&[u8]> = Vec::new();
            while cursor < bytes.len() {
                let (len, next) = read_varint(&bytes, cursor);
                let len = len as usize;
                messages.push(&bytes[next..next + len]);
                cursor = next + len;
            }

            let info = proto::BackupInfo::decode(messages[0]).expect("first item is BackupInfo");
            assert_eq!(info.version, 1, "{name}");

            let frames: Vec<proto::Frame> = messages[1..]
                .iter()
                .map(|m| proto::Frame::decode(*m).expect("frame decodes"))
                .collect();
            assert!(!frames.is_empty(), "{name}");
            check(&frames);
        }
    }

    /// 全链：状态机产出的 MVP 帧集 → 官方容器 → 官方 reader 逐帧读回，与
    /// 状态机产出的帧逐一等价。
    #[tokio::test]
    async fn full_mvp_sequence_round_trips_through_official_reader() {
        use libsignal_message_backup::frame::{CursorFactory, FramesReader};
        use libsignal_message_backup::parse::VarintDelimitedReader;

        let mut seq = FrameSequencer::new(backup_info());
        let info = seq.backup_info().expect("backup info");
        let account = seq.account_data(account()).expect("account data");
        let (self_id, self_frame) = seq.self_recipient().expect("self");
        let (contact_id, contact_frame) = seq
            .contact_recipient(contact([0xaa; 16], 15555550100))
            .expect("contact");
        let (group_id, group_frame) = seq.group_recipient([0xbb; 32]).expect("group");
        let (chat_id, chat_frame) = seq.chat(contact_id).expect("chat");
        let (note_chat_id, note_chat_frame) = seq.chat(self_id).expect("note-to-self chat");
        let incoming = seq
            .chat_item_text(
                chat_id,
                contact_id,
                1_760_000_001_000,
                ChatItemDirection::Incoming {
                    date_received_ms: 1_760_000_001_005,
                    read: true,
                },
                "hello from contact",
            )
            .expect("incoming");
        let outgoing = seq
            .chat_item_text(
                chat_id,
                self_id,
                1_760_000_002_000,
                ChatItemDirection::Outgoing {
                    date_received_ms: 1_760_000_002_010,
                },
                "hello back",
            )
            .expect("outgoing");
        let note_item = seq
            .chat_item_text(
                note_chat_id,
                self_id,
                1_760_000_003_000,
                ChatItemDirection::Outgoing {
                    date_received_ms: 1_760_000_003_000,
                },
                "note to self",
            )
            .expect("note item");
        let folder = seq.all_chat_folder().expect("folder");

        assert_eq!(self_id, 1);
        assert_eq!(contact_id, 2);
        assert_eq!(group_id, 3);
        assert_eq!(chat_id, 1);
        assert_eq!(note_chat_id, 2);

        let frames: Vec<proto::Frame> = vec![
            account,
            self_frame,
            contact_frame,
            group_frame,
            chat_frame,
            note_chat_frame,
            incoming,
            outgoing,
            note_item,
            folder,
        ];

        let key = tests_official_keys().message_key;
        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        writer.write_frame(&encode(&info)).expect("write info");
        for frame in &frames {
            writer.write_frame(&encode(frame)).expect("write frame");
        }
        let archive = writer.finish().expect("finish");

        let factory = CursorFactory::new(&archive);
        let raw = FramesReader::new(&key, factory)
            .await
            .expect("container valid");
        let mut delimited = VarintDelimitedReader::new(raw);
        let mut read_back: Vec<proto::Frame> = Vec::new();
        let mut info_checked = false;
        while let Some(bytes) = delimited.read_next().await.expect("readable") {
            if !info_checked {
                info_checked = true;
                let info_back =
                    proto::BackupInfo::decode(&bytes[..]).expect("first item is BackupInfo");
                assert_eq!(info_back, info);
                continue;
            }
            read_back.push(proto::Frame::decode(&bytes[..]).expect("frame decodes"));
        }

        assert_eq!(read_back, frames);
    }

    #[test]
    fn backup_info_must_come_first_and_only_once() {
        let mut seq = FrameSequencer::new(backup_info());
        assert_eq!(
            seq.account_data(account()).err(),
            Some(FrameSequenceError::AccountDataPlacement)
        );
        seq.backup_info().expect("first emission ok");
        assert_eq!(
            seq.backup_info().err(),
            Some(FrameSequenceError::BackupInfoNotFirst)
        );
    }

    #[test]
    fn backup_info_rejects_non_32b_media_root_key() {
        let mut bad = backup_info();
        bad.media_root_backup_key = vec![0x42; 16];
        let mut seq = FrameSequencer::new(bad);
        assert_eq!(
            seq.backup_info().err(),
            Some(FrameSequenceError::InvalidMediaRootBackupKey(16))
        );
    }

    #[test]
    fn account_data_must_be_first_frame_and_only_once() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");

        let other = FrameSequencer::new(backup_info());
        let _ = other; // 占位：下一行才是被测序列

        seq.account_data(account()).expect("first account ok");
        assert_eq!(
            seq.account_data(account()).err(),
            Some(FrameSequenceError::AccountDataPlacement)
        );

        // BackupInfo 之后再发 BackupInfo 已被上一测试覆盖；这里补：
        // 未发 BackupInfo 时 AccountData 拒绝（另一实例）。
        let mut seq2 = FrameSequencer::new(backup_info());
        assert_eq!(
            seq2.account_data(account()).err(),
            Some(FrameSequenceError::AccountDataPlacement)
        );
    }

    #[test]
    fn self_recipient_is_id_1_and_exactly_once() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        let (id, frame) = seq.self_recipient().expect("self");
        assert_eq!(id, 1);
        assert!(matches!(
            frame.item,
            Some(proto::frame::Item::Recipient(proto::Recipient {
                destination: Some(proto::recipient::Destination::Self_(_)),
                ..
            }))
        ));
        assert_eq!(
            seq.self_recipient().err(),
            Some(FrameSequenceError::DuplicateSelfRecipient)
        );
    }

    #[test]
    fn contact_recipient_dedupes_all_identities_fail_closed() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        let (first_id, _) = seq
            .contact_recipient(contact([0xaa; 16], 15555550100))
            .expect("first");

        // 同 aci
        let dup_aci = seq
            .contact_recipient(contact([0xaa; 16], 15555550199))
            .err();
        assert!(matches!(
            dup_aci,
            Some(FrameSequenceError::DuplicateContactIdentity { .. })
        ));
        // 同 e164
        let dup_e164 = seq
            .contact_recipient(contact([0xab; 16], 15555550100))
            .err();
        assert!(matches!(
            dup_e164,
            Some(FrameSequenceError::DuplicateContactIdentity { .. })
        ));

        // 同 pni
        let mut pni_contact = contact([0xac; 16], 15555550200);
        pni_contact.pni = Some([0xee; 16].to_vec());
        let (pni_id, _) = seq.contact_recipient(pni_contact).expect("pni contact");
        let mut dup_pni = contact([0xad; 16], 15555550201);
        dup_pni.pni = Some([0xee; 16].to_vec());
        assert!(matches!(
            seq.contact_recipient(dup_pni).err(),
            Some(FrameSequenceError::DuplicateContactIdentity { .. })
        ));

        // 无任何标识
        assert_eq!(
            seq.contact_recipient(proto::Contact::default()).err(),
            Some(FrameSequenceError::ContactWithoutIdentity)
        );

        // 合法新 contact 分配递增 id
        let (next_id, _) = seq
            .contact_recipient(contact([0xaf; 16], 15555550300))
            .expect("new");
        assert_eq!(next_id, first_id + 2);
        assert_eq!(next_id, pni_id + 1);
    }

    #[test]
    fn group_recipient_dedupes_master_key() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        let (id1, frame1) = seq.group_recipient([0x22; 32]).expect("group 1");
        assert!(matches!(
            frame1.item,
            Some(proto::frame::Item::Recipient(proto::Recipient {
                destination: Some(proto::recipient::Destination::Group(_)),
                ..
            }))
        ));
        assert_eq!(
            seq.group_recipient([0x22; 32]).err(),
            Some(FrameSequenceError::DuplicateGroupMasterKey(id1))
        );
        let (id2, _) = seq.group_recipient([0x23; 32]).expect("group 2");
        assert_ne!(id1, id2);
    }

    #[test]
    fn chat_requires_registered_recipient() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        assert_eq!(
            seq.chat(99).err(),
            Some(FrameSequenceError::UnknownRecipient(99))
        );
    }

    #[test]
    fn chat_item_requires_registered_chat_and_eligible_author() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        let (_, self_frame) = seq.self_recipient().expect("self");
        let _ = self_frame;

        // chat 未登记
        assert_eq!(
            seq.chat_item_text(
                42,
                1,
                0,
                ChatItemDirection::Outgoing {
                    date_received_ms: 0
                },
                "x"
            )
            .err(),
            Some(FrameSequenceError::UnknownChat(42))
        );

        let (chat_id, _) = seq.chat(1).expect("chat");
        // author 未登记
        assert_eq!(
            seq.chat_item_text(
                chat_id,
                77,
                0,
                ChatItemDirection::Outgoing {
                    date_received_ms: 0
                },
                "x"
            )
            .err(),
            Some(FrameSequenceError::UnknownRecipient(77))
        );

        // author 是 group → 不合格
        let (group_id, _) = seq.group_recipient([0x31; 32]).expect("group");
        assert_eq!(
            seq.chat_item_text(
                chat_id,
                group_id,
                0,
                ChatItemDirection::Outgoing {
                    date_received_ms: 0
                },
                "x"
            )
            .err(),
            Some(FrameSequenceError::InvalidChatAuthor(group_id))
        );
    }

    #[test]
    fn all_chat_folder_is_once_with_validator_canonical_fields() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        let folder = seq.all_chat_folder().expect("folder");
        let Some(proto::frame::Item::ChatFolder(ref f)) = folder.item else {
            panic!("expected chat folder frame");
        };
        assert_eq!(f.name, "");
        assert!(!f.show_only_unread);
        assert!(f.show_muted_chats);
        assert!(f.include_all_individual_chats);
        assert!(f.include_all_group_chats);
        assert!(f.included_recipient_ids.is_empty());
        assert!(f.excluded_recipient_ids.is_empty());
        assert_eq!(f.folder_type, proto::chat_folder::FolderType::All as i32);
        assert_eq!(f.id.len(), 16);

        assert_eq!(
            seq.all_chat_folder().err(),
            Some(FrameSequenceError::DuplicateAllChatFolder)
        );
    }

    #[test]
    fn account_data_fills_minimal_settings_rejects_svrpin_and_bad_profile_key() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");

        let frame = seq.account_data(account()).expect("account");
        let Some(proto::frame::Item::Account(ref a)) = frame.item else {
            panic!("expected account frame");
        };
        // 缺省 settings 自动补最小合法面
        let settings = a.account_settings.as_ref().expect("settings filled");
        assert_eq!(
            settings.phone_number_sharing_mode,
            proto::account_data::PhoneNumberSharingMode::Nobody as i32
        );
        assert!(a.svr_pin.is_empty());
        assert_eq!(a.profile_key.len(), 32);

        // 调用方显式带 svrPin → fail-closed（新实例，避开恰一次闸先挡）
        let with_pin_seq = || {
            let mut seq = FrameSequencer::new(backup_info());
            seq.backup_info().expect("info");
            seq
        };

        let mut with_pin = account();
        with_pin.svr_pin = "somesvrpin".into();
        assert_eq!(
            with_pin_seq().account_data(with_pin).err(),
            Some(FrameSequenceError::SvrPinExportForbidden)
        );

        // profileKey 长度非法
        let mut bad_key = account();
        bad_key.profile_key = vec![1; 8];
        assert_eq!(
            with_pin_seq().account_data(bad_key).err(),
            Some(FrameSequenceError::InvalidProfileKey(8))
        );

        // settings 带非法 sharing mode
        let mut bad_mode = account();
        bad_mode.account_settings = Some(proto::account_data::AccountSettings {
            phone_number_sharing_mode: 0,
            ..Default::default()
        });
        assert_eq!(
            with_pin_seq().account_data(bad_mode).err(),
            Some(FrameSequenceError::InvalidPhoneNumberSharingMode)
        );
    }

    #[test]
    fn chat_item_text_maps_directions_to_official_details() {
        let mut seq = FrameSequencer::new(backup_info());
        seq.backup_info().expect("info");
        seq.self_recipient().expect("self");
        let (contact_id, _) = seq
            .contact_recipient(contact([0xca; 16], 15555550400))
            .expect("c");
        let (chat_id, _) = seq.chat(contact_id).expect("chat");

        let incoming = seq
            .chat_item_text(
                chat_id,
                contact_id,
                1_000,
                ChatItemDirection::Incoming {
                    date_received_ms: 1_005,
                    read: true,
                },
                "in",
            )
            .expect("incoming");
        let Some(proto::frame::Item::ChatItem(ref item)) = incoming.item else {
            panic!("expected chat item");
        };
        assert_eq!(item.chat_id, chat_id);
        assert_eq!(item.author_id, contact_id);
        assert_eq!(item.date_sent, 1_000);
        assert!(matches!(
            item.directional_details,
            Some(proto::chat_item::DirectionalDetails::Incoming(
                proto::chat_item::IncomingMessageDetails {
                    date_received: 1_005,
                    read: true,
                    ..
                }
            ))
        ));
        assert!(matches!(
            item.item,
            Some(proto::chat_item::Item::StandardMessage(proto::StandardMessage {
                text: Some(proto::Text { ref body, .. }),
                ..
            })) if body == "in"
        ));

        let outgoing = seq
            .chat_item_text(
                chat_id,
                1,
                2_000,
                ChatItemDirection::Outgoing {
                    date_received_ms: 2_010,
                },
                "out",
            )
            .expect("outgoing");
        let Some(proto::frame::Item::ChatItem(ref item)) = outgoing.item else {
            panic!("expected chat item");
        };
        assert_eq!(item.author_id, 1);
        assert!(matches!(
            item.directional_details,
            Some(proto::chat_item::DirectionalDetails::Outgoing(
                proto::chat_item::OutgoingMessageDetails {
                    date_received: 2_010,
                    ..
                }
            ))
        ));

        // encode→decode 等价（每个产出帧都可无损往返）
        for frame in [incoming, outgoing] {
            let bytes = encode(&frame);
            assert_eq!(decode_frame(&bytes), frame);
        }
    }
}
