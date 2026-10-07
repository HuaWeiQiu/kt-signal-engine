// SPDX-License-Identifier: AGPL-3.0-only

//! ADR 0005 S2：transfer archive 解密 + 分帧 + NDJSON 导出。
//!
//! 解密面全官方（`FramesReader`：HMAC-SHA256 尾验证 + AES-256-CBC + gzip +
//! padding 剥离；`VarintDelimitedReader` varint 分帧），帧 proto 为自持生成代码
//! （`crate::proto`，官方开源 schema）。逐帧流式转 receive-compatible 中间表示
//! （NDJSON），O(1) 帧内存。v1 范围与跳过策略见 ADR 0005 §S2 设计定案；
//! recipient 会话归属身份投影（aci/e164/masterKey，E5）按消费方 connector
//! 契约 1.39（implementation-plan §4.38）的解析面逐字对齐。
//!
//! 附件指针面（H3 附件媒体字节 v2，方案 C ①；消费方 connector 契约 1.55
//! §4.38）：v1 把 `FilePointer.LocatorInfo` 三件套全丢（metadata-only），
//! 官方导入后能看媒体靠的恰是这个指针（transfer archive 容器里没有媒体
//! 字节，见 docs/2026-10-06-attachment-media-bytes-v2-research.md §0）。
//! v2 = 投影扩面：locator（key/cdnKey/cdnNumber/uploadTimestamp/
//! integrity(digest|plaintextHash)）+ clientUuid/宽高/caption/blurHash/
//! incrementalMac+chunkSize 随行导出；`key` 是附件解密钥，属只经内存/
//! 工件中转的传输材料，引擎任何持久层不留存。wire 面契约记录在
//! docs/jsonrpc-compat.md §5「附件指针投影面」。

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
/// `frames` 为档案总帧数（含 BackupInfo 元帧）。`contacts`/`group_updates`
/// 为 `messages` 内两非文本类型的细分计数（B1 扩面可见性）。
#[derive(Debug, Default, PartialEq)]
pub struct ExportStats {
    pub backup_version: u64,
    pub frames: u64,
    pub recipients: u64,
    pub chats: u64,
    pub messages: u64,
    pub contacts: u64,
    pub group_updates: u64,
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

    let stats = export_frames_loop(&mut delimited, out, &aci).await?;

    // 流读完即到档案尾：消费 reader 校验 HMAC 尾部（不校验 = 不可信导入）。
    delimited
        .into_inner()
        .verify_hmac()
        .await
        .map_err(HistoryError::Hmac)?;
    Ok(stats)
}

/// 帧循环公共面（transfer archive 导出与官方快照导入两条入口共用，
/// `backup::receive::export_snapshot_frames`）：首帧必须是 BackupInfo（档案
/// 版本来源），后续帧按 recipient/chat/chatItem 投影 NDJSON，item unset/
/// 未知帧型官方语义跳过并计数。`self_aci` 为本机账号 ACI（群变更 self-invite/
/// pending-remove 投影的 serviceId 面需要）。尾部 HMAC 验证归入口层（调用方
/// 持有具体 reader 类型）。future 非 Send，调用方须在 LocalSet 上运行。
pub(crate) async fn export_frames_loop<R>(
    delimited: &mut VarintDelimitedReader<R>,
    out: &mut dyn Write,
    self_aci: &presage::libsignal_service::prelude::Uuid,
) -> Result<ExportStats, HistoryError>
where
    R: futures::io::AsyncRead + Unpin,
{
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
            Some(frame::Item::ChatItem(item)) => {
                let is_contact = matches!(item.item, Some(chat_item::Item::ContactMessage(_)));
                let is_group_update = matches!(
                    &item.item,
                    Some(chat_item::Item::UpdateMessage(update)) if matches!(
                        update.update,
                        Some(chat_update_message::Update::GroupChange(_))
                    )
                );
                match message_record(&item, self_aci) {
                    Some(record) => {
                        stats.messages += 1;
                        if is_contact {
                            stats.contacts += 1;
                        }
                        if is_group_update {
                            stats.group_updates += 1;
                        }
                        write_json(out, record)?;
                    }
                    None => {
                        if matches!(item.item, Some(chat_item::Item::RemoteDeletedMessage(_))) {
                            stats.remote_deleted += 1;
                        } else {
                            stats.skipped_items += 1;
                        }
                    }
                }
            }
            Some(_) | None => stats.skipped_frames += 1,
        }
    }
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
    uuid_string(bytes)
}

/// 16 字节 → 小写连字 UUID 串；长度畸形的档案字节不可信 → None。
fn uuid_string(bytes: &[u8]) -> Option<String> {
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

/// v2 中间表示：text + 附件 pointer-complete 投影（locator 面 +
/// clientUuid/宽高/caption/blurHash/incrementalMac，见
/// `project_pointer_common`；字节本身不在档案内，下载归 connector）+
/// quote + sticker（`sticker_record`）+ contact（`contact_record`）+
/// group update（`group_update_record`）+ 远端删除。返回 `None` = 跳过
/// （计数可见）。
fn message_record(
    item: &ChatItem,
    self_aci: &presage::libsignal_service::prelude::Uuid,
) -> Option<serde_json::Value> {
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
        chat_item::Item::StickerMessage(sticker_message) => {
            record["sticker"] = sticker_record(sticker_message)?;
        }
        chat_item::Item::ContactMessage(contact_message) => {
            record["contact"] = contact_record(contact_message)?;
        }
        chat_item::Item::UpdateMessage(update) => {
            let timer = timer_change_record(update);
            let group = group_update_record(update, self_aci);
            if timer.is_none() && group.is_none() {
                return None;
            }
            if let Some(face) = timer {
                record["expirationTimerChange"] = face;
            }
            if let Some(face) = group {
                record["groupUpdate"] = face;
            }
        }
        // 其余 item 类型 v1 跳过（调用方计数）；方向可用的 call 类未来再扩。
        _ => return None,
    };
    Some(record)
}

/// E 批备份导出计时器变更行（connector 拿去做 system 行：会话归属 + 新
/// timer 值 + 0=关闭语义）。1:1 取 ExpirationTimerChatUpdate；群取
/// GroupChange 批内第一个 GroupExpirationTimerUpdate。其余 update 类型不投
/// 计时器面（群元数据变更另走 `group_update_record` 的 `groupUpdate` 键）。
/// proto 只投影不发明：expiresInMs 为 proto3 标量原样透出（0 原样保留）；
/// updaterAci 为 16 字节 ACI 的小写 UUID 形，畸形省略键。
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

/// B1 sticker item 完整投影（官方 Transfer/backup 导入对位：Signal-Desktop
/// `import.preload.ts:2749-2789` —— `stickerMessage` 要求 sticker 在场且
/// packId/packKey 字节长恰为 16/32（`Stickers.preload.ts:108-109`
/// STICKERPACK_ID_BYTE_LEN/KEY），序列化 = packId `Bytes.toHex` /
/// packKey `Bytes.toBase64`，emoji 可选、data 经附件指针转换可选）。投影
/// 口径：身份畸形（pack 字节长不合规）= 整 item 跳过（官方 strictAssert
/// 的 KT 落法——绝不投半个贴纸，调用方计数可见，导入永不致命）；emoji 与
/// data 沿官方可缺席语义，缺省键不在（data 缺席官方投 undefined 同形）。
/// `data` 复用附件指针公共面（locator 三件套同款面，字节不下载）；emoji
/// 空串零信息量不投（caption/blurHash 同款纪律）。行形状与 live 接收投影
/// 同构（`service::project_data_message` 的 `sticker` 面：packId
/// hex/packKey base64/stickerId/emoji?/data?），消费方 connector 契约
/// 1.35 的 NormalizedSticker 身份面 + §4.38 import_attachments 描述符面
/// 直接可吃。
fn sticker_record(sticker_message: &crate::proto::StickerMessage) -> Option<serde_json::Value> {
    let sticker = sticker_message.sticker.as_ref()?;
    if sticker.pack_id.len() != 16 || sticker.pack_key.len() != 32 {
        return None;
    }
    use base64::Engine as _;
    let mut face = serde_json::json!({
        "packId": hex_string(&sticker.pack_id),
        "packKey": base64::engine::general_purpose::STANDARD.encode(&sticker.pack_key),
        "stickerId": sticker.sticker_id,
    });
    if let Some(emoji) = sticker.emoji.as_deref().filter(|emoji| !emoji.is_empty()) {
        face["emoji"] = serde_json::Value::String(emoji.to_string());
    }
    if let Some(data) = &sticker.data {
        face["data"] = serde_json::json!(file_pointer_record(data));
    }
    Some(face)
}

/// B1 group update item 完整投影（官方 Transfer/backup 导入对位：
/// Signal-Desktop `import.preload.ts:3190-3764` `#fromGroupUpdateMessage` ——
/// GroupChangeChatUpdate.updates 逐条映射为 GroupV2Change detail 词表：
/// summary/create/title/avatar/description/access-*/announcements-only/
/// member-privilege/member-remove/member-add(-from-invite/-from-link/
/// -from-admin-approval)/pending-add-one/pending-add-many/pending-remove-one/
/// pending-remove-many/admin-approval-*/group-link-*，migration 族归并为单条
/// v1-migration detail（官方 migrationMessage getDefaultMigrationMessage 同面：
/// areWeInvited 缺省 false、计数缺省 0），GroupExpirationTimerUpdate 不入
/// detail（骑既有 `expirationTimerChange` 面）。投影口径：
/// - detail 词表逐条与官方对位；updaterAci 有则投（16B 小写 UUID 形，畸形
///   省略键）——官方以 `from` 变量逐条覆盖，KT 按条随身投出，行级 actor 归
///   消费方裁定。
/// - 身份必需的 detail（member-privilege/member-remove/member-add 族/
///   admin-approval 族等官方 strictAssert/throw 面）身份残缺 = 该条 detail
///   丢弃（KT 永不致命——官方 throw 的 KT 落法），其余 detail 照投；一行
///   零 detail（且零计时器面）= 整 item 跳过计入 skippedItems。
/// - update 未设的条目：官方 proto 注释「importers should consider it to be
///   a GenericGroupUpdate with unset updaterAci」→ 投 summary 无 updaterAci。
/// - 官方 openApproval/bounce 同名合并（isApprovalPending=true）是渲染侧
///   refine，KT 不做（消费方不消费该位），如实记档 §5。
/// - details 上限 32 条（真实变更批 1-5 条，超限属敌意行域，截断不致命）。
///
/// 消费方 connector 契约 1.79：presence + title detail 的 newTitle 作
/// group_name 镜像，落 live 同形 system 通知行（「群聊信息已更新」）。
const MAX_GROUP_UPDATE_DETAILS: usize = 32;

fn group_update_record(
    update: &ChatUpdateMessage,
    self_aci: &presage::libsignal_service::prelude::Uuid,
) -> Option<serde_json::Value> {
    let chat_update_message::Update::GroupChange(change) = update.update.as_ref()? else {
        return None;
    };

    let mut details: Vec<serde_json::Value> = Vec::new();
    let mut migration: Option<serde_json::Value> = None;
    for entry in &change.updates {
        if details.len() >= MAX_GROUP_UPDATE_DETAILS {
            break;
        }
        if let Some(detail) = group_change_detail(entry, self_aci, &mut migration) {
            details.push(detail);
        }
    }
    // migration 族官方不落 detail（独立 group-v1-migration 消息）；KT 归并为
    // 单条 v1-migration detail 追加在批尾（消费方单 face 承载同语义）。
    if details.len() < MAX_GROUP_UPDATE_DETAILS {
        if let Some(face) = migration.take() {
            details.push(face);
        }
    }

    (!details.is_empty()).then(|| serde_json::json!({ "details": details }))
}

/// updaterAci 公共投法：官方逐条读 `updaterAci` 覆盖行级 `from` 变量，KT 按
/// 条随身投出（行级 actor 归消费方裁定）；畸形（非 16B）省略键不发明。
fn project_updater(detail: &mut serde_json::Value, updater_aci: &Option<Vec<u8>>) {
    if let Some(aci) = updater_aci.as_deref().and_then(aci_string) {
        detail["updaterAci"] = serde_json::Value::String(aci);
    }
}

/// 官方 `parseGroupV2AccessLevel`：UNKNOWN/词表外/越界一律归 UNKNOWN。
fn group_v2_access_level_name(access_level: i32) -> &'static str {
    crate::proto::GroupV2AccessLevel::try_from(access_level)
        .map(|level| level.as_str_name())
        .unwrap_or("UNKNOWN")
}

/// migration 族归并面：首见即建缺省 face（areWeInvited=false、两计数=0，
/// 官方 `getDefaultMigrationMessage` 同面），后续变体逐位叠加。
fn fold_migration(migration: &mut Option<serde_json::Value>) -> &mut serde_json::Value {
    migration.get_or_insert_with(|| {
        serde_json::json!({
            "type": "v1-migration",
            "areWeInvited": false,
            "invitedMemberCount": 0,
            "droppedMemberCount": 0,
        })
    })
}

/// GroupChange 批内单条 detail（官方 `#fromGroupUpdateMessage` 词表逐臂）。
/// 身份必需的 detail（官方 strictAssert/throw 面）身份残缺 = 条目丢弃
/// （`None`），永不致命；migration 族不入此处（归并见 `fold_migration`）；
/// GroupExpirationTimerUpdate 不入 details（骑既有 `expirationTimerChange` 面）。
fn group_change_detail(
    entry: &group_change_chat_update::Update,
    self_aci: &presage::libsignal_service::prelude::Uuid,
    migration: &mut Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    use group_change_chat_update::update::Update as EntryUpdate;

    let Some(update) = entry.update.as_ref() else {
        // 官方 proto 注释：update 未设视为 GenericGroupUpdate 且 updaterAci
        // 未设 → summary 无 updaterAci。
        return Some(serde_json::json!({ "type": "summary" }));
    };
    match update {
        EntryUpdate::GenericGroupUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "summary" });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupCreationUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "create" });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupNameUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "title" });
            // 官方 dropNull 语义之上收紧：空串零信息量同缺席（caption/blurHash
            // 同款纪律），键不在。
            if let Some(title) = change.new_group_name.as_deref().filter(|t| !t.is_empty()) {
                detail["newTitle"] = serde_json::Value::String(title.to_string());
            }
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupAvatarUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "avatar", "removed": change.was_removed });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupDescriptionUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "description" });
            match change.new_description.as_deref().filter(|d| !d.is_empty()) {
                Some(description) => {
                    detail["description"] = serde_json::Value::String(description.to_string());
                }
                // 官方：description 缺席/空串 → removed:true。
                None => {
                    detail["removed"] = serde_json::json!(true);
                }
            }
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupMembershipAccessLevelChangeUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "access-members",
                "newPrivilege": group_v2_access_level_name(change.access_level),
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupMemberLabelAccessLevelChangeUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "access-member-label",
                "newPrivilege": group_v2_access_level_name(change.access_level),
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupAttributesAccessLevelChangeUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "access-attributes",
                "newPrivilege": group_v2_access_level_name(change.access_level),
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupAnnouncementOnlyChangeUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "announcements-only",
                "announcementsOnly": change.is_announcement_only,
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupAdminStatusUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "member-privilege",
                "aci": aci_string(&change.member_aci)?,
                "newPrivilege": if change.was_admin_status_granted {
                    "ADMINISTRATOR"
                } else {
                    "DEFAULT"
                },
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupMemberLeftUpdate(change) => Some(serde_json::json!({
            "type": "member-remove",
            "aci": aci_string(&change.aci)?,
        })),
        EntryUpdate::GroupMemberRemovedUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "member-remove",
                "aci": aci_string(&change.removed_aci)?,
            });
            project_updater(&mut detail, &change.remover_aci);
            Some(detail)
        }
        EntryUpdate::SelfInvitedToGroupUpdate(change) => {
            // 官方 serviceId = aboutMe.aci（受邀的是本机账号）。
            let mut detail = serde_json::json!({
                "type": "pending-add-one",
                "serviceId": self_aci.to_string(),
            });
            project_updater(&mut detail, &change.inviter_aci);
            Some(detail)
        }
        EntryUpdate::SelfInvitedOtherUserToGroupUpdate(change) => Some(serde_json::json!({
            "type": "pending-add-one",
            "serviceId": aci_string(&change.invitee_service_id)?,
        })),
        EntryUpdate::GroupUnknownInviteeUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "pending-add-many",
                "count": change.invitee_count,
            });
            project_updater(&mut detail, &change.inviter_aci);
            Some(detail)
        }
        EntryUpdate::GroupInvitationAcceptedUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "member-add-from-invite",
                "aci": aci_string(&change.new_member_aci)?,
            });
            if let Some(inviter) = change.inviter_aci.as_deref().and_then(aci_string) {
                detail["inviter"] = serde_json::Value::String(inviter);
            }
            Some(detail)
        }
        EntryUpdate::GroupInvitationDeclinedUpdate(change) => {
            // 官方恒投此条：serviceId/inviter 各自畸形省键，条目不丢。
            let mut detail = serde_json::json!({ "type": "pending-remove-one" });
            if let Some(service_id) = change.invitee_aci.as_deref().and_then(aci_string) {
                detail["serviceId"] = serde_json::Value::String(service_id);
            }
            if let Some(inviter) = change.inviter_aci.as_deref().and_then(aci_string) {
                detail["inviter"] = serde_json::Value::String(inviter);
            }
            Some(detail)
        }
        EntryUpdate::GroupMemberJoinedUpdate(change) => Some(serde_json::json!({
            "type": "member-add",
            "aci": aci_string(&change.new_member_aci)?,
        })),
        EntryUpdate::GroupMemberAddedUpdate(change) => {
            let aci = aci_string(&change.new_member_aci)?;
            let invited = change.had_open_invitation
                || change
                    .inviter_aci
                    .as_deref()
                    .is_some_and(|inviter| !inviter.is_empty());
            let mut detail = if invited {
                let mut detail = serde_json::json!({
                    "type": "member-add-from-invite",
                    "aci": aci,
                });
                if let Some(inviter) = change.inviter_aci.as_deref().and_then(aci_string) {
                    detail["inviter"] = serde_json::Value::String(inviter);
                }
                detail
            } else {
                serde_json::json!({
                    "type": "member-add",
                    "aci": aci,
                })
            };
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupSelfInvitationRevokedUpdate(change) => {
            // 官方 serviceId = aboutMe.aci（被撤邀请的是本机账号）。
            let mut detail = serde_json::json!({
                "type": "pending-remove-one",
                "serviceId": self_aci.to_string(),
            });
            project_updater(&mut detail, &change.revoker_aci);
            Some(detail)
        }
        EntryUpdate::GroupInvitationRevokedUpdate(change) => {
            // 官方 throw「missing invitees list」的 KT 落法：名单空 = 条目丢弃。
            let first = change.invitees.first()?;
            // 全体 inviter 一致才投 inviter（官方 firstInviter+every 同语义），
            // 畸形省键；不一致/缺省则不投。
            let inviter = first
                .inviter_aci
                .as_deref()
                .filter(|first_inviter| {
                    change
                        .invitees
                        .iter()
                        .all(|invitee| invitee.inviter_aci.as_deref() == Some(*first_inviter))
                })
                .and_then(aci_string);
            let mut detail = if change.invitees.len() == 1 {
                // 单条：aci 优先 pni（官方同序），两者皆缺 → count:1 的 many。
                let service_id = first
                    .invitee_aci
                    .as_deref()
                    .and_then(aci_string)
                    .or_else(|| first.invitee_pni.as_deref().and_then(aci_string));
                match service_id {
                    Some(service_id) => serde_json::json!({
                        "type": "pending-remove-one",
                        "serviceId": service_id,
                    }),
                    None => serde_json::json!({
                        "type": "pending-remove-many",
                        "count": 1,
                    }),
                }
            } else {
                serde_json::json!({
                    "type": "pending-remove-many",
                    "count": change.invitees.len(),
                })
            };
            if let Some(inviter) = inviter {
                detail["inviter"] = serde_json::Value::String(inviter);
            }
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupJoinRequestUpdate(change) => Some(serde_json::json!({
            "type": "admin-approval-add-one",
            "aci": aci_string(&change.requestor_aci)?,
        })),
        EntryUpdate::GroupJoinRequestApprovalUpdate(change) => {
            let mut detail = if change.was_approved {
                serde_json::json!({
                    "type": "member-add-from-admin-approval",
                    "aci": aci_string(&change.requestor_aci)?,
                })
            } else {
                serde_json::json!({
                    "type": "admin-approval-remove-one",
                    "aci": aci_string(&change.requestor_aci)?,
                })
            };
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupJoinRequestCanceledUpdate(change) => Some(serde_json::json!({
            "type": "admin-approval-remove-one",
            "aci": aci_string(&change.requestor_aci)?,
        })),
        EntryUpdate::GroupInviteLinkResetUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "group-link-reset" });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupInviteLinkEnabledUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "group-link-add",
                "privilege": if change.link_requires_admin_approval {
                    "ADMINISTRATOR"
                } else {
                    "ANY"
                },
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupInviteLinkAdminApprovalUpdate(change) => {
            let mut detail = serde_json::json!({
                "type": "access-invite-link",
                "newPrivilege": if change.link_requires_admin_approval {
                    "ADMINISTRATOR"
                } else {
                    "ANY"
                },
            });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupInviteLinkDisabledUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "group-link-remove" });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        EntryUpdate::GroupMemberJoinedByLinkUpdate(change) => Some(serde_json::json!({
            "type": "member-add-from-link",
            "aci": aci_string(&change.new_member_aci)?,
        })),
        EntryUpdate::GroupV2MigrationUpdate(_) => {
            fold_migration(migration);
            None
        }
        EntryUpdate::GroupV2MigrationSelfInvitedUpdate(_) => {
            fold_migration(migration)["areWeInvited"] = serde_json::json!(true);
            None
        }
        EntryUpdate::GroupV2MigrationInvitedMembersUpdate(change) => {
            fold_migration(migration)["invitedMemberCount"] =
                serde_json::json!(change.invited_members_count);
            None
        }
        EntryUpdate::GroupV2MigrationDroppedMembersUpdate(change) => {
            fold_migration(migration)["droppedMemberCount"] =
                serde_json::json!(change.dropped_members_count);
            None
        }
        EntryUpdate::GroupSequenceOfRequestsAndCancelsUpdate(change) => {
            // 官方 isApprovalPending 渲染侧 refine 合并 KT 不做（记档 §5）。
            Some(serde_json::json!({
                "type": "admin-approval-bounce",
                "aci": aci_string(&change.requestor_aci)?,
                "times": change.count,
            }))
        }
        EntryUpdate::GroupTerminateChangeUpdate(change) => {
            let mut detail = serde_json::json!({ "type": "terminated" });
            project_updater(&mut detail, &change.updater_aci);
            Some(detail)
        }
        // 群计时器不入 details：骑既有 `expirationTimerChange` 面（同批合行）。
        EntryUpdate::GroupExpirationTimerUpdate(_) => None,
    }
}

/// B1 contact item 完整投影（官方 Transfer/backup 导入对位：Signal-Desktop
/// `import.preload.ts:2620-2705` —— `contactMessage` strictAssert 要求
/// contact 在场，随后 name/number/email/organization/avatar 逐面映射为
/// ContactForm 形状：name 子字段空串归缺席（官方 `givenName: name.given ||
/// undefined` 同口径）、number/email 条目 value 空的整条丢弃（官方
/// `!value → undefined` + `filter(isNotNil)`）、type 词表
/// HOME/MOBILE/WORK/CUSTOM（`phoneToContactFormType`:191-214 /
/// `emailToContactFormType`:216-240——UNKNOWN/词表外一律归 HOME）、organization
/// 空串归缺席、avatar 经 `convertFilePointerToAttachment` 附件指针转换）。
/// 投影口径：contact 未设 = 整 item 跳过（官方 strictAssert 的 KT 落法，
/// 计数可见不致命）；全零信息量 face（无任何可渲染字段）= 整 item 跳过
/// （空卡片即占位，KT 永不投占位）；地址面（PostalAddress）本批不投（任务
/// 面=姓名/电话/邮箱字段指针面，如实记档 §5 gap）。avatar 复用附件指针
/// 公共面（locator 同款面，字节不下载），消费方 connector 并入描述符列表
/// 与 live 附件归一同构。
fn contact_record(contact_message: &crate::proto::ContactMessage) -> Option<serde_json::Value> {
    let contact = contact_message.contact.as_ref()?;
    let mut face = serde_json::Map::new();
    if let Some(name) = &contact.name {
        let mut projected = serde_json::Map::new();
        for (key, value) in [
            ("givenName", &name.given_name),
            ("familyName", &name.family_name),
            ("prefix", &name.prefix),
            ("suffix", &name.suffix),
            ("middleName", &name.middle_name),
            ("nickname", &name.nickname),
        ] {
            if !value.is_empty() {
                projected.insert(key.into(), serde_json::Value::String(value.clone()));
            }
        }
        if !projected.is_empty() {
            face.insert("name".into(), serde_json::Value::Object(projected));
        }
    }
    let numbers: Vec<_> = contact
        .number
        .iter()
        .filter_map(|phone| contact_field_record(&phone.value, phone.r#type, &phone.label))
        .collect();
    if !numbers.is_empty() {
        face.insert("number".into(), serde_json::json!(numbers));
    }
    let emails: Vec<_> = contact
        .email
        .iter()
        .filter_map(|email| contact_field_record(&email.value, email.r#type, &email.label))
        .collect();
    if !emails.is_empty() {
        face.insert("email".into(), serde_json::json!(emails));
    }
    if !contact.organization.is_empty() {
        face.insert(
            "organization".into(),
            serde_json::Value::String(contact.organization.clone()),
        );
    }
    if let Some(avatar) = &contact.avatar {
        face.insert(
            "avatar".into(),
            serde_json::json!(file_pointer_record(avatar)),
        );
    }
    (!face.is_empty()).then_some(serde_json::Value::Object(face))
}

/// number/email 条目投影：value 空的整条丢弃（官方 `!value → undefined` +
/// `filter(isNotNil)` 同口径）；type 词表经 `contact_field_type_name`；label
/// 非空才投（官方 `label || undefined` 同口径）。
fn contact_field_record(value: &str, field_type: i32, label: &str) -> Option<serde_json::Value> {
    if value.is_empty() {
        return None;
    }
    let mut record = serde_json::json!({
        "value": value,
        "type": contact_field_type_name(field_type),
    });
    if !label.is_empty() {
        record["label"] = serde_json::Value::String(label.to_string());
    }
    Some(record)
}

/// proto Phone/Email.Type → 官方 ContactFormType 词表（HOME/MOBILE/WORK/
/// CUSTOM）。UNKNOWN/缺省/词表外一律归 HOME（官方 `phoneToContactFormType`
/// `import.preload.ts:191-214` 同口径：`isKnownProtoEnumMember` 失败与
/// UNKNOWN 分支都归 HOME）。两枚举词表同形，共用一个映射函数。
fn contact_field_type_name(value: i32) -> &'static str {
    match crate::proto::contact_attachment::phone::Type::try_from(value) {
        Ok(crate::proto::contact_attachment::phone::Type::Mobile) => "MOBILE",
        Ok(crate::proto::contact_attachment::phone::Type::Work) => "WORK",
        Ok(crate::proto::contact_attachment::phone::Type::Custom) => "CUSTOM",
        Ok(crate::proto::contact_attachment::phone::Type::Home)
        | Ok(crate::proto::contact_attachment::phone::Type::Unknown)
        | Err(_) => "HOME",
    }
}

/// 附件/文件指针投影公共面（H3 附件媒体字节 v2，方案 C ①）：v1 只有
/// contentType/fileName/flag/size（metadata-only，locator 全丢——connector
/// 无从下载）；v2 扩成 pointer-complete。官方消费面锚点：Signal-Desktop
/// `convertFilePointerToAttachment`（ts/services/backups/util/
/// filePointers.preload.ts:51-168，AGPL 只读参照零拷贝）——把 locator 三件套
/// 映射为常规附件下载器输入 key/digest|plaintextHash/cdnKey/cdnNumber/
/// uploadTimestamp；`isDownloadableFromTransitTier`（ts/util/Attachment.std.ts
/// :806-825）四件齐即可走 transit CDN。
///
/// 投影口径（proto 只投影不发明，缺/空一律省略键）：
/// - `width`/`height`：Some 原样透出（Some(0) 也是档案陈述）；`caption`/
///   `blurHash`：非空才投（空串零信息量）；`incrementalMac`+`chunkSize`
///   同进同退（官方 :84-87 两者齐备才保留；mac = 标准 base64）。
/// - `locator` 对象（官方属性名同形）：`key` ← LocatorInfo.key 标准 base64
///   （附件解密钥——NDJSON 行是引擎↔connector 工件传输材料，只经内存/工件
///   中转，引擎任何持久层不留存）；`cdnKey`/`cdnNumber` ← transit_cdn_key/
///   transit_cdn_number（Some(0) 是合法 CDN0，原样投）；`uploadTimestamp` ←
///   transit_tier_upload_timestamp（0 无意义，官方 falsy 同款省略）；
///   `plaintextHash` ← integrity_check PlaintextHash（小写 hex，官方
///   Bytes.toHex 同形）/ `digest` ← EncryptedDigest（标准 base64，官方同形），
///   oneof 二选一。mediaTierCdnNumber/localKey 实有但本批不投：不在 H3
///   字段清单、connector 无媒体 tier 消费面（付费备份长期兜底属后续批），
///   见 docs/jsonrpc-compat.md §5 附件指针面块的登记。
/// - locator 三件套全空（官方 Locator::Invalid = 附件已不可取）→ 整个
///   `locator` 键缺席；`size` 维持 v1 顶层位（connector v1 消费形状不变）。
fn project_pointer_common(record: &mut serde_json::Value, pointer: &FilePointer) {
    use base64::Engine as _;
    if let Some(locator) = &pointer.locator_info {
        record["size"] = serde_json::json!(locator.size);
    }
    if let Some(width) = pointer.width {
        record["width"] = serde_json::json!(width);
    }
    if let Some(height) = pointer.height {
        record["height"] = serde_json::json!(height);
    }
    if let Some(caption) = pointer.caption.as_deref().filter(|text| !text.is_empty()) {
        record["caption"] = serde_json::Value::String(caption.to_string());
    }
    if let Some(blur_hash) = pointer.blur_hash.as_deref().filter(|text| !text.is_empty()) {
        record["blurHash"] = serde_json::Value::String(blur_hash.to_string());
    }
    if let (Some(mac), Some(chunk_size)) =
        (&pointer.incremental_mac, pointer.incremental_mac_chunk_size)
    {
        if !mac.is_empty() && chunk_size > 0 {
            record["incrementalMac"] =
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(mac));
            record["chunkSize"] = serde_json::json!(chunk_size);
        }
    }
    let Some(locator) = &pointer.locator_info else {
        return;
    };
    let mut face = serde_json::Map::new();
    if !locator.key.is_empty() {
        face.insert(
            "key".into(),
            serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(&locator.key),
            ),
        );
    }
    if let Some(cdn_key) = locator
        .transit_cdn_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        face.insert(
            "cdnKey".into(),
            serde_json::Value::String(cdn_key.to_string()),
        );
    }
    if let Some(cdn_number) = locator.transit_cdn_number {
        face.insert("cdnNumber".into(), serde_json::json!(cdn_number));
    }
    if let Some(timestamp) = locator.transit_tier_upload_timestamp.filter(|ts| *ts > 0) {
        face.insert("uploadTimestamp".into(), serde_json::json!(timestamp));
    }
    match &locator.integrity_check {
        Some(crate::proto::file_pointer::locator_info::IntegrityCheck::PlaintextHash(hash))
            if !hash.is_empty() =>
        {
            face.insert(
                "plaintextHash".into(),
                serde_json::Value::String(hex_string(hash)),
            );
        }
        Some(crate::proto::file_pointer::locator_info::IntegrityCheck::EncryptedDigest(digest))
            if !digest.is_empty() =>
        {
            face.insert(
                "digest".into(),
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(digest)),
            );
        }
        _ => {}
    }
    if !face.is_empty() {
        record["locator"] = serde_json::Value::Object(face);
    }
}

/// 字节串小写 hex（官方 Bytes.toHex 同形，plaintextHash 投影用）。
fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn attachment_record(attachment: &crate::proto::MessageAttachment) -> Option<serde_json::Value> {
    let pointer: &FilePointer = attachment.pointer.as_ref()?;
    let mut record = serde_json::json!({
        "contentType": pointer.content_type,
        "fileName": pointer.file_name,
        "flag": attachment.flag,
    });
    project_pointer_common(&mut record, pointer);
    // clientUuid ← MessageAttachment.client_uuid（官方 bytesToUuid 同形：
    // 小写连字 UUID）；畸形长度省略键。
    if let Some(uuid) = attachment.client_uuid.as_deref().and_then(uuid_string) {
        record["clientUuid"] = serde_json::Value::String(uuid);
    }
    Some(record)
}

/// longText 等裸 FilePointer 行：同名公共面（clientUuid 只在
/// MessageAttachment 上，此处无此键）。
fn file_pointer_record(pointer: &FilePointer) -> serde_json::Value {
    let mut record = serde_json::json!({
        "contentType": pointer.content_type,
        "fileName": pointer.file_name,
    });
    project_pointer_common(&mut record, pointer);
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

    /// 本机账号 aci（self-invite/pending-remove-one 面 serviceId 期望值
    /// 「11111111-1111-1111-1111-111111111111」）。
    fn test_self_aci() -> presage::libsignal_service::prelude::Uuid {
        presage::libsignal_service::prelude::Uuid::from_bytes([0x11u8; 16])
    }

    /// 单参便捷封装：非 roundtrip 的逐投影测试统一走本机 aci。
    fn test_record(item: &ChatItem) -> Option<serde_json::Value> {
        message_record(item, &test_self_aci())
    }

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
        // B1 批：contact item（官方 contactMessage 导出形态）成行。
        let contact_item = frame_bytes(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 7,
                date_sent: 450,
                directional_details: Some(chat_item::DirectionalDetails::Incoming(
                    IncomingMessageDetails::default(),
                )),
                item: Some(Item::ContactMessage(crate::proto::ContactMessage {
                    contact: Some(crate::proto::ContactAttachment {
                        name: Some(crate::proto::contact_attachment::Name {
                            given_name: "Grace".into(),
                            family_name: "Hopper".into(),
                            ..Default::default()
                        }),
                        number: vec![crate::proto::contact_attachment::Phone {
                            value: "+15550000002".into(),
                            r#type: crate::proto::contact_attachment::phone::Type::Mobile as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    reactions: Vec::new(),
                })),
                ..Default::default()
            })),
        });
        // B1 批：群变更 item（改名 + 批内群计时器合行 → 双 face 单行）成行。
        let group_item = frame_bytes(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 7,
                date_sent: 500,
                directional_details: Some(chat_item::DirectionalDetails::Incoming(
                    IncomingMessageDetails::default(),
                )),
                item: Some(Item::UpdateMessage(ChatUpdateMessage {
                    update: Some(chat_update_message::Update::GroupChange(
                        GroupChangeChatUpdate {
                            updates: vec![
                                group_change_chat_update::Update {
                                    update: Some(
                                        group_change_chat_update::update::Update::GroupNameUpdate(
                                            crate::proto::GroupNameUpdate {
                                                updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
                                                new_group_name: Some("KT 群".into()),
                                            },
                                        ),
                                    ),
                                },
                                group_change_chat_update::Update {
                                    update: Some(
                                        group_change_chat_update::update::Update::GroupExpirationTimerUpdate(
                                            GroupExpirationTimerUpdate {
                                                expires_in_ms: 604_800_000,
                                                updater_aci: None,
                                            },
                                        ),
                                    ),
                                },
                            ],
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
            contact_item,
            group_item,
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
                frames: 10,
                recipients: 2,
                chats: 1,
                messages: 5,
                contacts: 1,
                group_updates: 1,
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
        assert_eq!(lines.len(), 8);
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
        // B1 批：contact 行成行（计入 messages 与 contacts 细分）。
        assert_eq!(lines[6]["contact"]["name"]["givenName"], "Grace");
        assert_eq!(lines[6]["contact"]["name"]["familyName"], "Hopper");
        assert_eq!(lines[6]["contact"]["number"][0]["value"], "+15550000002");
        assert_eq!(lines[6]["contact"]["number"][0]["type"], "MOBILE");
        assert_eq!(lines[6]["direction"], "incoming");
        // B1 批：群变更行双 face 合行（改名 detail + 批内群计时器），group_updates 细分计数。
        assert_eq!(lines[7]["groupUpdate"]["details"][0]["type"], "title");
        assert_eq!(lines[7]["groupUpdate"]["details"][0]["newTitle"], "KT 群");
        assert_eq!(
            lines[7]["groupUpdate"]["details"][0]["updaterAci"],
            "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b"
        );
        assert_eq!(
            lines[7]["expirationTimerChange"]["expiresInMs"],
            604_800_000
        );
        assert_eq!(lines[7]["direction"], "incoming");

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
        let one_to_one = test_record(&ChatItem {
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
        assert!(test_record(&ChatItem {
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
        let with = test_record(&item(Some(1_000), Some(86_400_000))).expect("行");
        assert_eq!(with["expireStartDate"], 1_000);
        assert_eq!(with["expiresInMs"], 86_400_000);
        let without = test_record(&item(Some(0), None)).expect("行");
        assert!(without.get("expireStartDate").is_none());
        assert!(without.get("expiresInMs").is_none());
    }

    /// H3 v2（方案 C ①）附件指针投影三形态：完整 transit locator（key/
    /// cdnKey/cdnNumber(0 合法)/uploadTimestamp/digest|plaintextHash/
    /// incrementalMac+chunkSize/宽高/caption/blurHash/clientUuid）/ 部分
    /// locator（缺 transit 指针只有 integrity → 只有对应键）/ Invalid（无
    /// locator_info 或 key 空、integrity 空 → locator 键缺席，v1 基础面
    /// 仍在）。mediaTierCdnNumber/localKey 实有不投（本批裁定，见
    /// `project_pointer_common` 文档）。
    #[test]
    fn attachment_pointer_projection_faces() {
        use crate::proto::{file_pointer::locator_info::IntegrityCheck, MessageAttachment};

        let full = attachment_record(&MessageAttachment {
            pointer: Some(FilePointer {
                content_type: Some("image/png".into()),
                file_name: Some("pic.png".into()),
                width: Some(640),
                height: Some(480),
                caption: Some("a caption".into()),
                blur_hash: Some("LEHV6nWB2yk8pyo0adR*.7kCMdnj".into()),
                incremental_mac: Some(vec![1, 2, 3, 4]),
                incremental_mac_chunk_size: Some(8192),
                locator_info: Some(file_pointer::LocatorInfo {
                    key: vec![0xABu8; 32],
                    size: 1234,
                    transit_cdn_key: Some("abcd-1234".into()),
                    transit_cdn_number: Some(0),
                    transit_tier_upload_timestamp: Some(1_700_000_000_000),
                    integrity_check: Some(IntegrityCheck::PlaintextHash(vec![0x0f; 32])),
                    ..Default::default()
                }),
            }),
            flag: 1,
            was_downloaded: true,
            client_uuid: Some(vec![
                0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x6a, 0x7b, 0x8c, 0x9d, 0xae, 0xbf, 0xc0, 0xd1,
                0xe2, 0xf3,
            ]),
        })
        .expect("完整形态是行");
        assert_eq!(full["size"], 1234, "size 维持 v1 顶层位");
        assert_eq!(full["width"], 640);
        assert_eq!(full["height"], 480);
        assert_eq!(full["caption"], "a caption");
        assert_eq!(full["blurHash"], "LEHV6nWB2yk8pyo0adR*.7kCMdnj");
        assert_eq!(full["flag"], 1);
        assert_eq!(
            full["clientUuid"], "0a1b2c3d-4e5f-6a7b-8c9d-aebfc0d1e2f3",
            "官方 bytesToUuid 同形小写连字"
        );
        // locator 面：key/cdnKey base64，cdnNumber=0 是合法 CDN0，plaintext
        // hash 走 hex（官方 Bytes.toHex 同形）。
        assert_eq!(
            full["locator"]["key"],
            base64::engine::general_purpose::STANDARD.encode([0xABu8; 32])
        );
        assert_eq!(full["locator"]["cdnKey"], "abcd-1234");
        assert_eq!(full["locator"]["cdnNumber"], 0);
        assert_eq!(full["locator"]["uploadTimestamp"], 1_700_000_000_000_u64);
        assert_eq!(full["locator"]["plaintextHash"], "0f".repeat(32));
        assert!(
            full["locator"].get("digest").is_none(),
            "oneof 只投命中的 arm"
        );
        // incrementalMac + chunkSize 同进同退（官方 :84-87 两者齐备才保留）。
        assert_eq!(
            full["incrementalMac"],
            base64::engine::general_purpose::STANDARD.encode([1, 2, 3, 4])
        );
        assert_eq!(full["chunkSize"], 8192);
        // 实有不投字段（mediaTierCdnNumber/localKey）不得以任何形态出现。
        assert!(full["locator"].get("mediaTierCdnNumber").is_none());
        assert!(full["locator"].get("localKey").is_none());

        // digest arm（EncryptedDigest：发送方原始附件、未下载过的形态）。
        let encrypted = attachment_record(&MessageAttachment {
            pointer: Some(FilePointer {
                content_type: Some("video/mp4".into()),
                locator_info: Some(file_pointer::LocatorInfo {
                    key: vec![7u8; 32],
                    size: 42,
                    transit_cdn_key: Some("k".into()),
                    transit_cdn_number: Some(3),
                    integrity_check: Some(IntegrityCheck::EncryptedDigest(vec![0xEE; 16])),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("digest 形态是行");
        assert_eq!(
            encrypted["locator"]["digest"],
            base64::engine::general_purpose::STANDARD.encode([0xEE; 16]),
            "digest 标准 base64（官方 Bytes.toBase64 同形）"
        );
        assert!(encrypted["locator"].get("plaintextHash").is_none());
        assert!(
            encrypted.get("uploadTimestamp").is_none()
                && encrypted["locator"].get("uploadTimestamp").is_none(),
            "uploadTimestamp 缺席省略键"
        );

        // 部分 locator：有 key + plaintextHash 但 transit 指针缺席（本机
        // 下载过的官方形态）——只有存在的键，不发明占位值。
        let partial = file_pointer_record(&FilePointer {
            content_type: Some("application/pdf".into()),
            file_name: Some("doc.pdf".into()),
            locator_info: Some(file_pointer::LocatorInfo {
                key: vec![1u8; 32],
                size: 99,
                integrity_check: Some(IntegrityCheck::PlaintextHash(vec![0x7f; 32])),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(partial["size"], 99);
        assert_eq!(
            partial["locator"]["key"],
            base64::engine::general_purpose::STANDARD.encode([1u8; 32])
        );
        assert_eq!(partial["locator"]["plaintextHash"], "7f".repeat(32));
        for absent in ["cdnKey", "cdnNumber", "uploadTimestamp", "digest"] {
            assert!(
                partial["locator"].get(absent).is_none(),
                "{absent} 不得占位"
            );
        }

        // 官方 Locator::Invalid 两形态：locator_info 整体缺席；key 空 +
        // integrity 空（proto3 默认形态）——locator 键缺席，v1 基础面仍在。
        let invalid_pointer = file_pointer_record(&FilePointer {
            content_type: Some("application/octet-stream".into()),
            file_name: Some("gone.bin".into()),
            ..Default::default()
        });
        assert!(invalid_pointer.get("locator").is_none());
        assert!(invalid_pointer.get("size").is_none());
        assert_eq!(invalid_pointer["contentType"], "application/octet-stream");
        let empty_locator = attachment_record(&MessageAttachment {
            pointer: Some(FilePointer {
                content_type: Some("image/webp".into()),
                file_name: Some("sticker.webp".into()),
                locator_info: Some(file_pointer::LocatorInfo::default()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("空 locator 形态是行");
        assert!(
            empty_locator.get("locator").is_none(),
            "三件套全空 = Invalid"
        );
        // size 维持 v1 行为：locator_info 在场即投（0 原样透出，官方
        // `size ?? 0` 同口径的诚实零——空附件合法，proto 注释明示）。
        assert_eq!(empty_locator["size"], 0);
        assert_eq!(empty_locator["fileName"], "sticker.webp");

        // 畸形面：clientUuid 非 16B → 省略键；incrementalMac 只有一半
        // （缺 chunkSize）→ 两个键都不投（同进同退）。
        let malformed = attachment_record(&MessageAttachment {
            pointer: Some(FilePointer {
                content_type: Some("text/plain".into()),
                incremental_mac: Some(vec![9; 8]),
                ..Default::default()
            }),
            client_uuid: Some(vec![0x11; 15]),
            ..Default::default()
        })
        .expect("畸形形态是行");
        assert!(malformed.get("clientUuid").is_none());
        assert!(malformed.get("incrementalMac").is_none());
        assert!(malformed.get("chunkSize").is_none());
        assert!(malformed["locator"].is_null());
    }

    /// B1 groupUpdate 投影词表钉样（官方 `#fromGroupUpdateMessage` 逐臂对位）：
    /// title（空串收紧=缺席）/ summary 双源（update 未设条目 + 显式
    /// GenericGroupUpdate）/ create/avatar removed 恒投/description 两态/
    /// access 三面 + 越界 i32 归 UNKNOWN（官方 parseGroupV2AccessLevel）/
    /// announcements-only 与 group-link 两 privilege 键形/member-privilege
    /// 身份必需（残缺=条目丢弃不致命）/self-invite serviceId=本机 aci/
    /// member-add 族分叉（有无 open invitation）/invitation-revoked 多人
    /// inviter 归并/migration 族四变体归并单条 v1-migration/bounce times/
    /// 零 detail（且零计时器面）整 item 跳过。updaterAci 一律 16B 小写 UUID
    /// 随身投出，畸形省键。
    #[test]
    fn group_update_projection_vocabulary() {
        use crate::proto::{
            group_invitation_revoked_update, GroupAnnouncementOnlyChangeUpdate,
            GroupAttributesAccessLevelChangeUpdate, GroupAvatarUpdate, GroupCreationUpdate,
            GroupDescriptionUpdate, GroupInvitationRevokedUpdate,
            GroupInviteLinkAdminApprovalUpdate, GroupInviteLinkEnabledUpdate,
            GroupMemberAddedUpdate, GroupMemberLabelAccessLevelChangeUpdate,
            GroupMembershipAccessLevelChangeUpdate, GroupNameUpdate,
            GroupV2MigrationDroppedMembersUpdate, GroupV2MigrationInvitedMembersUpdate,
            GroupV2MigrationSelfInvitedUpdate, GroupV2MigrationUpdate,
            SelfInvitedOtherUserToGroupUpdate, SelfInvitedToGroupUpdate,
        };
        use group_change_chat_update::update::Update as Entry;

        // 与 CONTACT_ACI 不同的第二身份（invitation-revoked 的 inviter 面）。
        const INVITER_ACI_BYTES: [u8; 16] = [0x22u8; 16];
        let inviter_uuid = "22222222-2222-2222-2222-222222222222";
        let contact_uuid = "6e08f0b6-1c2d-4e5f-8a9b-0c1d2e3f4a5b";
        let self_uuid = "11111111-1111-1111-1111-111111111111";

        let group_item = |updates: Vec<group_change_chat_update::Update>| ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 700,
            directional_details: Some(chat_item::DirectionalDetails::Incoming(
                IncomingMessageDetails::default(),
            )),
            item: Some(Item::UpdateMessage(ChatUpdateMessage {
                update: Some(chat_update_message::Update::GroupChange(
                    GroupChangeChatUpdate { updates },
                )),
            })),
            ..Default::default()
        };
        let entry = |update: Option<Entry>| group_change_chat_update::Update { update };
        let details = |updates: Vec<group_change_chat_update::Update>| {
            test_record(&group_item(updates)).expect("有 detail 即成行")["groupUpdate"]["details"]
                .clone()
        };

        // title：newTitle 非空才投；updaterAci 随身（官方 from 变量的按条投法）。
        let d = details(vec![entry(Some(Entry::GroupNameUpdate(GroupNameUpdate {
            updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
            new_group_name: Some("新名".into()),
        })))]);
        assert_eq!(d[0]["type"], "title");
        assert_eq!(d[0]["newTitle"], "新名");
        assert_eq!(d[0]["updaterAci"], contact_uuid);
        // 空串标题零信息量：detail 仍在，newTitle 键不在（caption 同款纪律）。
        let d = details(vec![entry(Some(Entry::GroupNameUpdate(GroupNameUpdate {
            updater_aci: None,
            new_group_name: Some(String::new()),
        })))]);
        assert!(d[0].get("newTitle").is_none());

        // summary 双源：update 未设条目（官方视为 GenericGroupUpdate 无
        // updaterAci）与显式 GenericGroupUpdate（updaterAci 随身）。
        let d = details(vec![
            entry(None),
            entry(Some(Entry::GenericGroupUpdate(
                crate::proto::GenericGroupUpdate {
                    updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
                },
            ))),
        ]);
        assert_eq!(d[0], serde_json::json!({ "type": "summary" }));
        assert_eq!(d[1]["type"], "summary");
        assert_eq!(d[1]["updaterAci"], contact_uuid);

        // create / avatar（removed 布尔恒投）/ description 两态（缺席或空串 →
        // removed:true，非空 → description 键）。
        let d = details(vec![
            entry(Some(Entry::GroupCreationUpdate(GroupCreationUpdate {
                updater_aci: None,
            }))),
            entry(Some(Entry::GroupAvatarUpdate(GroupAvatarUpdate {
                updater_aci: None,
                was_removed: true,
            }))),
            entry(Some(Entry::GroupDescriptionUpdate(
                GroupDescriptionUpdate {
                    updater_aci: None,
                    new_description: Some("说明".into()),
                },
            ))),
            entry(Some(Entry::GroupDescriptionUpdate(
                GroupDescriptionUpdate {
                    updater_aci: None,
                    new_description: None,
                },
            ))),
        ]);
        assert_eq!(d[0], serde_json::json!({ "type": "create" }));
        assert_eq!(
            d[1],
            serde_json::json!({ "type": "avatar", "removed": true })
        );
        assert_eq!(d[2]["type"], "description");
        assert_eq!(d[2]["description"], "说明");
        assert!(d[2].get("removed").is_none());
        assert_eq!(
            d[3],
            serde_json::json!({ "type": "description", "removed": true })
        );

        // access 三面：词表值原样（as_str_name），越界 i32 归 UNKNOWN。
        let d = details(vec![
            entry(Some(Entry::GroupMembershipAccessLevelChangeUpdate(
                GroupMembershipAccessLevelChangeUpdate {
                    updater_aci: None,
                    access_level: crate::proto::GroupV2AccessLevel::Administrator as i32,
                },
            ))),
            entry(Some(Entry::GroupMemberLabelAccessLevelChangeUpdate(
                GroupMemberLabelAccessLevelChangeUpdate {
                    updater_aci: None,
                    access_level: crate::proto::GroupV2AccessLevel::Any as i32,
                },
            ))),
            entry(Some(Entry::GroupAttributesAccessLevelChangeUpdate(
                GroupAttributesAccessLevelChangeUpdate {
                    updater_aci: None,
                    access_level: 99,
                },
            ))),
        ]);
        assert_eq!(
            d[0],
            serde_json::json!({ "type": "access-members", "newPrivilege": "ADMINISTRATOR" })
        );
        assert_eq!(
            d[1],
            serde_json::json!({ "type": "access-member-label", "newPrivilege": "ANY" })
        );
        assert_eq!(
            d[2],
            serde_json::json!({ "type": "access-attributes", "newPrivilege": "UNKNOWN" })
        );

        // announcements-only 布尔恒投；group-link-add 的 privilege 与
        // access-invite-link 的 newPrivilege 两键形（官方同形异键）。
        let d = details(vec![
            entry(Some(Entry::GroupAnnouncementOnlyChangeUpdate(
                GroupAnnouncementOnlyChangeUpdate {
                    updater_aci: None,
                    is_announcement_only: true,
                },
            ))),
            entry(Some(Entry::GroupInviteLinkEnabledUpdate(
                GroupInviteLinkEnabledUpdate {
                    updater_aci: None,
                    link_requires_admin_approval: true,
                },
            ))),
            entry(Some(Entry::GroupInviteLinkAdminApprovalUpdate(
                GroupInviteLinkAdminApprovalUpdate {
                    updater_aci: None,
                    link_requires_admin_approval: false,
                },
            ))),
        ]);
        assert_eq!(
            d[0],
            serde_json::json!({ "type": "announcements-only", "announcementsOnly": true })
        );
        assert_eq!(
            d[1],
            serde_json::json!({ "type": "group-link-add", "privilege": "ADMINISTRATOR" })
        );
        assert_eq!(
            d[2],
            serde_json::json!({ "type": "access-invite-link", "newPrivilege": "ANY" })
        );

        // 身份必需（官方 throw 面）：member-privilege 残缺 member aci = 条目
        // 丢弃，批内其余照投不致命；健全条目 newPrivilege 两态 + updaterAci。
        let d = details(vec![
            entry(Some(Entry::GroupAdminStatusUpdate(
                crate::proto::GroupAdminStatusUpdate {
                    updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
                    member_aci: vec![0x11; 15],
                    was_admin_status_granted: true,
                },
            ))),
            entry(Some(Entry::GroupAdminStatusUpdate(
                crate::proto::GroupAdminStatusUpdate {
                    updater_aci: Some(CONTACT_ACI_BYTES.to_vec()),
                    member_aci: CONTACT_ACI_BYTES.to_vec(),
                    was_admin_status_granted: false,
                },
            ))),
        ]);
        assert_eq!(d.as_array().unwrap().len(), 1, "残缺条目丢弃不致命");
        assert_eq!(
            d[0],
            serde_json::json!({
                "type": "member-privilege",
                "aci": contact_uuid,
                "newPrivilege": "DEFAULT",
                "updaterAci": contact_uuid,
            })
        );

        // self-invite：serviceId = 本机 aci（官方 aboutMe.aci），inviter 随身。
        let d = details(vec![entry(Some(Entry::SelfInvitedToGroupUpdate(
            SelfInvitedToGroupUpdate {
                inviter_aci: Some(CONTACT_ACI_BYTES.to_vec()),
            },
        )))]);
        assert_eq!(
            d[0],
            serde_json::json!({
                "type": "pending-add-one",
                "serviceId": self_uuid,
                "updaterAci": contact_uuid,
            })
        );
        // self-invited-other-user：invitee 身份残缺 = 条目丢弃 → 零 detail 且
        // 零计时器面 = 整 item 跳过（官方 throw 的 KT 落法）。
        assert!(
            test_record(&group_item(vec![entry(Some(
                Entry::SelfInvitedOtherUserToGroupUpdate(SelfInvitedOtherUserToGroupUpdate {
                    invitee_service_id: Vec::new(),
                })
            ))]))
            .is_none(),
            "零 detail 整 item 跳过"
        );

        // member-add 族分叉：open invitation/inviter 在场 → from-invite 带
        // inviter；两者皆无 → 朴素 member-add。
        let d = details(vec![
            entry(Some(Entry::GroupMemberAddedUpdate(
                GroupMemberAddedUpdate {
                    updater_aci: None,
                    new_member_aci: CONTACT_ACI_BYTES.to_vec(),
                    had_open_invitation: true,
                    inviter_aci: Some(CONTACT_ACI_BYTES.to_vec()),
                },
            ))),
            entry(Some(Entry::GroupMemberAddedUpdate(
                GroupMemberAddedUpdate {
                    updater_aci: None,
                    new_member_aci: CONTACT_ACI_BYTES.to_vec(),
                    had_open_invitation: false,
                    inviter_aci: None,
                },
            ))),
        ]);
        assert_eq!(d[0]["type"], "member-add-from-invite");
        assert_eq!(d[0]["inviter"], contact_uuid);
        assert_eq!(
            d[1],
            serde_json::json!({ "type": "member-add", "aci": contact_uuid })
        );

        // invitation-revoked 多人：全体 inviter 一致才投 inviter；count=N。
        let invitee = |inviter: Option<Vec<u8>>| group_invitation_revoked_update::Invitee {
            inviter_aci: inviter,
            invitee_aci: None,
            invitee_pni: None,
        };
        let d = details(vec![entry(Some(Entry::GroupInvitationRevokedUpdate(
            GroupInvitationRevokedUpdate {
                updater_aci: None,
                invitees: vec![
                    invitee(Some(INVITER_ACI_BYTES.to_vec())),
                    invitee(Some(INVITER_ACI_BYTES.to_vec())),
                ],
            },
        )))]);
        assert_eq!(
            d[0],
            serde_json::json!({
                "type": "pending-remove-many",
                "count": 2,
                "inviter": inviter_uuid,
            })
        );

        // migration 族（self-invited/invited/dropped/plain 四变体）归并单条
        // v1-migration，逐位叠加（官方 getDefaultMigrationMessage 同面）。
        let d = details(vec![
            entry(Some(Entry::GroupV2MigrationSelfInvitedUpdate(
                GroupV2MigrationSelfInvitedUpdate {},
            ))),
            entry(Some(Entry::GroupV2MigrationInvitedMembersUpdate(
                GroupV2MigrationInvitedMembersUpdate {
                    invited_members_count: 3,
                },
            ))),
            entry(Some(Entry::GroupV2MigrationDroppedMembersUpdate(
                GroupV2MigrationDroppedMembersUpdate {
                    dropped_members_count: 2,
                },
            ))),
            entry(Some(Entry::GroupV2MigrationUpdate(
                GroupV2MigrationUpdate {},
            ))),
        ]);
        assert_eq!(d.as_array().unwrap().len(), 1, "migration 归并单条");
        assert_eq!(
            d[0],
            serde_json::json!({
                "type": "v1-migration",
                "areWeInvited": true,
                "invitedMemberCount": 3,
                "droppedMemberCount": 2,
            })
        );

        // admin-approval-bounce：times=count 原样（isApprovalPending 渲染侧
        // refine 不投，记档 §5）。
        let d = details(vec![entry(Some(
            Entry::GroupSequenceOfRequestsAndCancelsUpdate(
                crate::proto::GroupSequenceOfRequestsAndCancelsUpdate {
                    requestor_aci: CONTACT_ACI_BYTES.to_vec(),
                    count: 4,
                },
            ),
        ))]);
        assert_eq!(
            d[0],
            serde_json::json!({
                "type": "admin-approval-bounce",
                "aci": contact_uuid,
                "times": 4,
            })
        );
    }

    /// B1 sticker item 投影五面：完整（packId hex/packKey base64/stickerId/
    /// emoji/data 指针 locator 同款面）/ 缺 emoji / 缺 data（官方可缺席语义，
    /// 键不在）/ 身份畸形（packId 15B、packKey 31B → 整 item 跳过，官方
    /// strictAssert 的 KT 落法）/ sticker 未设 → 整 item 跳过。proto 只投影
    /// 不发明：stickerId=0 原样透出（proto3 标量）。
    #[test]
    fn sticker_message_projection_faces() {
        use crate::proto::{Sticker, StickerMessage};

        let sticker_message = |sticker: Option<Sticker>| ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 500,
            directional_details: Some(chat_item::DirectionalDetails::Incoming(
                IncomingMessageDetails::default(),
            )),
            item: Some(Item::StickerMessage(StickerMessage {
                sticker,
                reactions: Vec::new(),
            })),
            ..Default::default()
        };
        let full_sticker = Sticker {
            pack_id: vec![0xABu8; 16],
            pack_key: vec![0xCDu8; 32],
            sticker_id: 9,
            emoji: Some("🎉".into()),
            data: Some(FilePointer {
                content_type: Some("image/webp".into()),
                file_name: Some("sticker.webp".into()),
                locator_info: Some(file_pointer::LocatorInfo {
                    key: vec![0x11u8; 32],
                    size: 77,
                    transit_cdn_key: Some("pack-cdn".into()),
                    transit_cdn_number: Some(2),
                    integrity_check: Some(
                        file_pointer::locator_info::IntegrityCheck::PlaintextHash(vec![0x2a; 32]),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let full = test_record(&sticker_message(Some(full_sticker.clone()))).expect("完整是行");
        assert_eq!(
            full["sticker"]["packId"],
            "ab".repeat(16),
            "官方 Bytes.toHex 同形"
        );
        assert_eq!(
            full["sticker"]["packKey"],
            base64::engine::general_purpose::STANDARD.encode([0xCDu8; 32]),
            "官方 Bytes.toBase64 同形"
        );
        assert_eq!(full["sticker"]["stickerId"], 9);
        assert_eq!(full["sticker"]["emoji"], "🎉");
        assert_eq!(full["sticker"]["data"]["contentType"], "image/webp");
        assert_eq!(full["sticker"]["data"]["size"], 77, "locator 公共面同款");
        assert_eq!(full["sticker"]["data"]["locator"]["cdnKey"], "pack-cdn");
        assert_eq!(full["sticker"]["data"]["locator"]["cdnNumber"], 2);
        assert_eq!(
            full["sticker"]["data"]["locator"]["plaintextHash"],
            "2a".repeat(32)
        );
        assert!(full.get("text").is_none(), "贴纸行无文本键");
        assert!(
            full.get("attachments").is_none(),
            "贴纸指针只在 sticker.data，不进顶层 attachments"
        );

        // 缺 emoji、缺 data：官方可缺席语义（emoji→undefined/data→undefined），
        // 键不在；身份面仍在。
        let bare = test_record(&sticker_message(Some(Sticker {
            emoji: None,
            data: None,
            ..full_sticker
        })))
        .expect("缺 emoji/data 仍是行");
        assert_eq!(bare["sticker"]["stickerId"], 9);
        assert!(bare["sticker"].get("emoji").is_none());
        assert!(bare["sticker"].get("data").is_none());

        // emoji 空串零信息量不投（caption/blurHash 同款纪律）。
        let empty_emoji = test_record(&sticker_message(Some(Sticker {
            emoji: Some(String::new()),
            data: None,
            pack_id: vec![0xABu8; 16],
            pack_key: vec![0xCDu8; 32],
            sticker_id: 0,
        })))
        .expect("空 emoji 仍是行");
        assert_eq!(
            empty_emoji["sticker"]["stickerId"], 0,
            "proto3 标量 0 原样透出"
        );
        assert!(empty_emoji["sticker"].get("emoji").is_none());

        // 身份畸形 = 整 item 跳过（官方 strictAssert 的 KT 落法：不投半个
        // 贴纸，调用方计入 skipped_items，导入永不致命）。
        for (name, pack_id, pack_key) in [
            ("pack-id-15b", vec![0xABu8; 15], vec![0xCDu8; 32]),
            ("pack-key-31b", vec![0xABu8; 16], vec![0xCDu8; 31]),
            ("pack-id-empty", Vec::new(), vec![0xCDu8; 32]),
        ] {
            assert!(
                test_record(&sticker_message(Some(Sticker {
                    pack_id,
                    pack_key,
                    sticker_id: 1,
                    ..Default::default()
                })))
                .is_none(),
                "{name}: 身份畸形整 item 跳过"
            );
        }
        // sticker 未设：官方「stickerMessage must have a sticker」同形跳过。
        assert!(test_record(&sticker_message(None)).is_none());
    }

    /// B1 contact item 投影多面：完整（name 子字段/number/email/organization/
    /// avatar 指针 locator 同款面）/ 空串子字段与空 value 条目、label 缺席
    /// （官方 `|| undefined`/`filter(isNotNil)` 同口径，键不在）/ UNKNOWN 与
    /// 词表外 type 归 HOME（官方 phoneToContactFormType 同口径）/ 全零信息量
    /// face 与 contact 未设 → 整 item 跳过（官方 strictAssert 的 KT 落法）。
    #[test]
    fn contact_message_projection_faces() {
        use crate::proto::{contact_attachment, ContactAttachment, ContactMessage};

        let contact_message = |contact: Option<ContactAttachment>| ChatItem {
            chat_id: 3,
            author_id: 7,
            date_sent: 600,
            directional_details: Some(chat_item::DirectionalDetails::Incoming(
                IncomingMessageDetails::default(),
            )),
            item: Some(Item::ContactMessage(ContactMessage {
                contact,
                reactions: Vec::new(),
            })),
            ..Default::default()
        };
        let full = test_record(&contact_message(Some(ContactAttachment {
            name: Some(contact_attachment::Name {
                given_name: "Ada".into(),
                family_name: "Lovelace".into(),
                prefix: "Dr.".into(),
                nickname: "ada".into(),
                ..Default::default()
            }),
            number: vec![
                contact_attachment::Phone {
                    value: "+15550000003".into(),
                    r#type: contact_attachment::phone::Type::Home as i32,
                    ..Default::default()
                },
                contact_attachment::Phone {
                    value: "+15550000004".into(),
                    r#type: contact_attachment::phone::Type::Mobile as i32,
                    label: "cell".into(),
                },
            ],
            email: vec![contact_attachment::Email {
                value: "ada@example.org".into(),
                r#type: contact_attachment::email::Type::Work as i32,
                ..Default::default()
            }],
            organization: "Analytical Engines Ltd".into(),
            avatar: Some(FilePointer {
                content_type: Some("image/jpeg".into()),
                file_name: Some("avatar.jpg".into()),
                locator_info: Some(file_pointer::LocatorInfo {
                    key: vec![0x21u8; 32],
                    size: 55,
                    transit_cdn_key: Some("avatar-cdn".into()),
                    transit_cdn_number: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })))
        .expect("完整 contact 是行");
        assert_eq!(full["contact"]["name"]["givenName"], "Ada");
        assert_eq!(full["contact"]["name"]["familyName"], "Lovelace");
        assert_eq!(full["contact"]["name"]["prefix"], "Dr.");
        assert_eq!(full["contact"]["name"]["nickname"], "ada");
        assert!(
            full["contact"]["name"].get("middleName").is_none(),
            "空串子字段键不在（官方 || undefined 同口径）"
        );
        assert_eq!(full["contact"]["number"][0]["type"], "HOME");
        assert!(full["contact"]["number"][0].get("label").is_none());
        assert_eq!(full["contact"]["number"][1]["type"], "MOBILE");
        assert_eq!(full["contact"]["number"][1]["label"], "cell");
        assert_eq!(full["contact"]["email"][0]["value"], "ada@example.org");
        assert_eq!(full["contact"]["email"][0]["type"], "WORK");
        assert_eq!(full["contact"]["organization"], "Analytical Engines Ltd");
        assert_eq!(full["contact"]["avatar"]["contentType"], "image/jpeg");
        assert_eq!(full["contact"]["avatar"]["size"], 55);
        assert_eq!(full["contact"]["avatar"]["locator"]["cdnKey"], "avatar-cdn");
        assert!(
            full.get("text").is_none() && full.get("attachments").is_none(),
            "contact 身份即行本体，指针只在 contact.avatar"
        );

        // UNKNOWN 与词表外 type 归 HOME（官方 phoneToContactFormType 同口径）；
        // value 空条目整条丢弃；空 label 键不在。
        let coerced = test_record(&contact_message(Some(ContactAttachment {
            number: vec![
                contact_attachment::Phone {
                    value: "+15550000005".into(),
                    r#type: contact_attachment::phone::Type::Unknown as i32,
                    ..Default::default()
                },
                contact_attachment::Phone {
                    value: "+15550000006".into(),
                    r#type: 99,
                    ..Default::default()
                },
                contact_attachment::Phone {
                    value: String::new(),
                    r#type: contact_attachment::phone::Type::Work as i32,
                    ..Default::default()
                },
            ],
            ..Default::default()
        })))
        .expect("type 归一 face 是行");
        assert_eq!(coerced["contact"]["number"][0]["type"], "HOME");
        assert_eq!(coerced["contact"]["number"][1]["type"], "HOME");
        assert_eq!(
            coerced["contact"]["number"].as_array().unwrap().len(),
            2,
            "空 value 条目整条丢弃"
        );

        // 全零信息量 face（proto3 默认 ContactAttachment）= 整 item 跳过：
        // 空卡片即占位，KT 永不投占位。
        assert!(test_record(&contact_message(Some(ContactAttachment::default()))).is_none());
        // contact 未设：官方「contactMessage must have a contact」同形跳过。
        assert!(test_record(&contact_message(None)).is_none());
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
