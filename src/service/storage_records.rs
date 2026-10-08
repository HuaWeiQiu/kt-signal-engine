// SPDX-License-Identifier: AGPL-3.0-only

//! Storage Service 跨设备同步记录面：ChatFolder（聊天文件夹）与
//! NotificationProfile（通知档案）——compat §3 `chatFolders.list`/
//! `chatFolders.setRecords`/`notificationProfiles.list`/
//! `notificationProfiles.setRecords` 四方法的 storage 侧。
//!
//! 记录 proto = 官方 vendored（`crate::proto::storage`，字段保真口径同
//! Account/Contact 面）；每实体独立 record + 独立 manifest identifier
//! （官方 upload 语义：ChatFolder=8 / NotificationProfile=9 各一条）。
//! 写面按 record 内实体 id（ChatFolderRecord.identifier /
//! NotificationProfile.id，16B）定位云上同实体旧 identifier → 新 16B
//! item id 换写（旧 key 进 deleteKey）；与云上现存结构全等 → 幂等跳过
//! 不轮换 key，整批全跳过 = 无 PUT 不 bump version。`deletedAtTimestampMs`
//! 非 0 = 墓碑写（24h 过期清理归调用方）；`removeIds` = 官方 deleteKey
//! 语义（identifier 出 manifest + deleteKey，无新 item）。409 采纳重放
//! 循环与贴纸面同构（conflictBackOff 3 次）。
//!
//! 读写直通纪律：record 内容不落引擎本地（读面每次 manifest+items 现读，
//! 写面只写 storage service；唯一本地文件是既有 storage-manifest-state.json
//! 排查面，本面不扩字段不登记）。recipients 与引擎会话引用双射
//! （`{conversationId, kind}`；e164-only 条目读面跳过宁缺勿错、写面整拒）。

use base64::Engine as _;
use presage::libsignal_service::master_key::StorageServiceKey;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::proto::ManifestRecord;
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::store::StateStore as _;
use prost::Message as _;

use crate::proto::storage::storage_record::Record;
use crate::proto::storage::{self as storage_proto};
use crate::service::storage_sync::{
    conflict_decision, decrypt_raw_item, new_item_key, open_storage_service, plan_record_writes,
    put_write_operation, read_raw_items, storage_auth, ConflictDecision, PutOutcome, RecordPayload,
    RecordUpsert, StorageRecordKind, StorageSyncError,
};
use crate::service::StorageContext;

/// 云上单条记录（存储 key → 解码后的记录值；读面坏条目不入列表）。
pub(crate) struct CloudEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: RecordValue,
}

/// 记录值（两类别共用判等面；PartialEq 供幂等跳过判定——与云上现存
/// decode→re-encode 结构全等即跳过，不轮换 item id）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RecordValue {
    Folder(storage_proto::ChatFolderRecord),
    Profile(storage_proto::NotificationProfile),
}

impl RecordValue {
    pub(crate) fn kind(&self) -> StorageRecordKind {
        match self {
            Self::Folder(_) => StorageRecordKind::ChatFolder,
            Self::Profile(_) => StorageRecordKind::NotificationProfile,
        }
    }

    /// 实体 id（16B；云上同实体定位键——非 manifest identifier raw）。
    pub(crate) fn entity_id(&self) -> &[u8] {
        match self {
            Self::Folder(record) => &record.identifier,
            Self::Profile(record) => &record.id,
        }
    }

    pub(crate) fn project(&self) -> Option<serde_json::Value> {
        match self {
            Self::Folder(record) => project_folder(record),
            Self::Profile(record) => project_profile(record),
        }
    }
}

// ---------------------------------------------------------------------------
// JSON → proto（setRecords 入参解析，严格形状闸 fail-closed）
// ---------------------------------------------------------------------------

/// JSON 字段收布尔：必须在场且为布尔（形状错 → None → 调用方 -32603）。
fn bool_field(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<bool> {
    map.get(key)?.as_bool()
}

/// JSON 字段收 u64：必须在场且为非负整数（浮点/负数/字符串拒绝）。
fn u64_field(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<u64> {
    map.get(key)?.as_u64()
}

/// JSON 字段收可选 u64（缺省/显式 null → 0；在场必须非负整数）。
fn u64_optional(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<u64> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Some(0),
        Some(value) => value.as_u64(),
    }
}

/// JSON 字段收可选 u32（缺省/null → 0）。
fn u32_optional(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<u32> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Some(0),
        Some(value) => u32::try_from(value.as_u64()?).ok(),
    }
}

/// uuid 字符串 → 16B（record 实体 id 与 removeIds 的统一形态；
/// `Uuid::parse_str` 认连字符/无连字符/braced/urn 各排版，同实体判等以
/// 16B 字节为准）。
pub(crate) fn uuid_bytes(value: &serde_json::Value) -> Option<Vec<u8>> {
    use presage::libsignal_service::prelude::Uuid;
    Some(Uuid::parse_str(value.as_str()?).ok()?.as_bytes().to_vec())
}

/// 32 位小写 hex 串 → 16B（notificationProfile id / removeIds 形态；
/// `hex_decode` 恒小写口径）。
pub(crate) fn hex_id_bytes(value: &serde_json::Value) -> Option<Vec<u8>> {
    crate::service::hex_decode(value.as_str()?)
}

/// recipients 条目 `{conversationId, kind}` → Recipient（三形态双射写面；
/// 解析不出对应形态 → None → 整批整拒 -32603，契约口径）。
pub(crate) fn recipient_of_json(item: &serde_json::Value) -> Option<storage_proto::Recipient> {
    use presage::libsignal_service::protocol::ServiceId;
    let map = item.as_object()?;
    let conversation_id = map.get("conversationId")?.as_str()?;
    let recipient = match map.get("kind")?.as_str()? {
        // contact：serviceId 字符串形态（ACI uuid / PNI:uuid）→ Recipient
        // 带 binary（16/17B 优先，官方 isProtoBinaryEncodingEnabled 现行面）。
        "contact" => {
            let service_id = ServiceId::parse_from_service_id_string(conversation_id.trim())?;
            storage_proto::Recipient {
                identifier: Some(storage_proto::recipient::Identifier::Contact(
                    storage_proto::recipient::Contact {
                        service_id: service_id.service_id_string(),
                        e164: String::new(),
                        service_id_binary: service_id.service_id_binary(),
                    },
                )),
            }
        }
        // group：32B master key 标准 base64（decode_group_id 同口径）。
        "group" => storage_proto::Recipient {
            identifier: Some(storage_proto::recipient::Identifier::GroupMasterKey(
                crate::service::decode_group_id(conversation_id)?,
            )),
        },
        // legacyGroup：legacy groupId 原字节文本（utf8 直通）。
        "legacyGroup" => storage_proto::Recipient {
            identifier: Some(storage_proto::recipient::Identifier::LegacyGroupId(
                conversation_id.as_bytes().to_vec(),
            )),
        },
        _ => return None,
    };
    Some(recipient)
}

/// recipients 数组字段（可选；在场必须为数组且条目全过——任一条目解析
/// 失败整拒）。
fn recipients_field(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<Vec<storage_proto::Recipient>> {
    match map.get(key) {
        None | Some(serde_json::Value::Null) => Some(Vec::new()),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(recipient_of_json)
            .collect::<Option<Vec<_>>>(),
        Some(_) => None,
    }
}

/// `chatFolders.setRecords` records 条目 → ChatFolderRecord（严格形状闸；
/// 详见 compat §3 行与 §5 记录面块）。
pub(crate) fn parse_folder(value: &serde_json::Value) -> Option<RecordValue> {
    let map = value.as_object()?;
    let folder_type = match map.get("folderType")?.as_str()? {
        "UNKNOWN" => storage_proto::chat_folder_record::FolderType::Unknown,
        "ALL" => storage_proto::chat_folder_record::FolderType::All,
        "CUSTOM" => storage_proto::chat_folder_record::FolderType::Custom,
        _ => return None,
    };
    Some(RecordValue::Folder(storage_proto::ChatFolderRecord {
        identifier: uuid_bytes(map.get("id")?)?,
        name: map.get("name")?.as_str()?.to_string(),
        position: u32::try_from(u64_field(map, "position")?).ok()?,
        show_only_unread: bool_field(map, "showOnlyUnread")?,
        show_muted_chats: bool_field(map, "showMutedChats")?,
        include_all_individual_chats: bool_field(map, "includeAllIndividualChats")?,
        include_all_group_chats: bool_field(map, "includeAllGroupChats")?,
        folder_type: folder_type as i32,
        included_recipients: recipients_field(map, "includedConversationIds")?,
        excluded_recipients: recipients_field(map, "excludedConversationIds")?,
        deleted_at_timestamp_ms: u64_optional(map, "deletedAtTimestampMs")?,
    }))
}

/// `notificationProfiles.setRecords` records 条目 → NotificationProfile
/// （id = 32 位小写 hex；name 非空必填；scheduleDaysEnabled 仅 1..=7）。
pub(crate) fn parse_profile(value: &serde_json::Value) -> Option<RecordValue> {
    let map = value.as_object()?;
    let name = map.get("name")?.as_str()?;
    if name.is_empty() {
        return None;
    }
    let emoji = match map.get("emoji") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(value.as_str()?.to_string()),
    };
    let schedule_days_enabled = match map.get("scheduleDaysEnabled") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                let day = item.as_u64()?;
                if !(1..=7).contains(&day) {
                    return None;
                }
                Some(day as i32)
            })
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    Some(RecordValue::Profile(storage_proto::NotificationProfile {
        id: crate::service::hex_decode(map.get("id")?.as_str()?)?,
        name: name.to_string(),
        emoji,
        color: u32::try_from(u64_field(map, "color")?).ok()?,
        created_at_ms: u64_field(map, "createdAtMs")?,
        allow_all_calls: bool_field(map, "allowAllCalls")?,
        allow_all_mentions: bool_field(map, "allowAllMentions")?,
        allowed_members: recipients_field(map, "allowedMembers")?,
        schedule_enabled: bool_field(map, "scheduleEnabled")?,
        schedule_start_time: u32_optional(map, "scheduleStartTime")?,
        schedule_end_time: u32_optional(map, "scheduleEndTime")?,
        schedule_days_enabled,
        deleted_at_timestamp_ms: u64_optional(map, "deletedAtTimestampMs")?,
    }))
}

// ---------------------------------------------------------------------------
// proto → JSON（读投影；键名对齐官方 fromChatFolderRecord /
// mergeNotificationProfileRecord 输出面）
// ---------------------------------------------------------------------------

/// Recipient → `{conversationId, kind}`（contact 二进制优先、其次字符串；
/// e164-only 条目 None → 调用方跳过该条目，宁缺勿错）。
pub(crate) fn json_of_recipient(recipient: &storage_proto::Recipient) -> Option<serde_json::Value> {
    use presage::libsignal_service::protocol::ServiceId;
    match recipient.identifier.as_ref()? {
        storage_proto::recipient::Identifier::Contact(contact) => {
            let service_id = if !contact.service_id_binary.is_empty() {
                ServiceId::parse_from_service_id_binary(&contact.service_id_binary)?
            } else if !contact.service_id.is_empty() {
                ServiceId::parse_from_service_id_string(&contact.service_id)?
            } else {
                return None;
            };
            Some(serde_json::json!({
                "conversationId": service_id.service_id_string(),
                "kind": "contact",
            }))
        }
        storage_proto::recipient::Identifier::GroupMasterKey(key) => Some(serde_json::json!({
            "conversationId": base64::engine::general_purpose::STANDARD.encode(key),
            "kind": "group",
        })),
        storage_proto::recipient::Identifier::LegacyGroupId(id) => {
            let text = String::from_utf8_lossy(id);
            Some(serde_json::json!({
                "conversationId": text,
                "kind": "legacyGroup",
            }))
        }
    }
}

fn recipients_json(recipients: &[storage_proto::Recipient]) -> serde_json::Value {
    serde_json::Value::Array(recipients.iter().filter_map(json_of_recipient).collect())
}

/// ChatFolderRecord → JSON（identifier 非 16B / folder_type 词表外 →
/// None，调用方跳过该条目不炸面；deletedAtTimestampMs 恒在，0 = 未删）。
pub(crate) fn project_folder(
    record: &storage_proto::ChatFolderRecord,
) -> Option<serde_json::Value> {
    use presage::libsignal_service::prelude::Uuid;
    if record.identifier.len() != 16 {
        return None;
    }
    let id = Uuid::from_slice(&record.identifier).ok()?;
    let word =
        match storage_proto::chat_folder_record::FolderType::try_from(record.folder_type).ok()? {
            storage_proto::chat_folder_record::FolderType::Unknown => "UNKNOWN",
            storage_proto::chat_folder_record::FolderType::All => "ALL",
            storage_proto::chat_folder_record::FolderType::Custom => "CUSTOM",
        };
    Some(serde_json::json!({
        "id": id.to_string(),
        "name": record.name,
        "position": record.position,
        "showOnlyUnread": record.show_only_unread,
        "showMutedChats": record.show_muted_chats,
        "includeAllIndividualChats": record.include_all_individual_chats,
        "includeAllGroupChats": record.include_all_group_chats,
        "folderType": word,
        "includedConversationIds": recipients_json(&record.included_recipients),
        "excludedConversationIds": recipients_json(&record.excluded_recipients),
        "deletedAtTimestampMs": record.deleted_at_timestamp_ms,
    }))
}

/// NotificationProfile → JSON（id = 32 位小写 hex；scheduleStartTime/
/// scheduleEndTime/deletedAtTimestampMs 0 → 键不在；scheduleDaysEnabled
/// 恒在数组（1..=7 整数，词表外条目跳过）；emoji Some 才投）。
pub(crate) fn project_profile(
    record: &storage_proto::NotificationProfile,
) -> Option<serde_json::Value> {
    let mut object = serde_json::Map::new();
    object.insert(
        "id".into(),
        serde_json::Value::String(crate::service::hex_encode(&record.id)),
    );
    object.insert(
        "name".into(),
        serde_json::Value::String(record.name.clone()),
    );
    if let Some(emoji) = record.emoji.as_ref() {
        object.insert("emoji".into(), serde_json::Value::String(emoji.clone()));
    }
    object.insert("color".into(), serde_json::json!(record.color));
    object.insert(
        "createdAtMs".into(),
        serde_json::json!(record.created_at_ms),
    );
    object.insert(
        "allowAllCalls".into(),
        serde_json::json!(record.allow_all_calls),
    );
    object.insert(
        "allowAllMentions".into(),
        serde_json::json!(record.allow_all_mentions),
    );
    object.insert(
        "allowedMembers".into(),
        recipients_json(&record.allowed_members),
    );
    object.insert(
        "scheduleEnabled".into(),
        serde_json::json!(record.schedule_enabled),
    );
    if record.schedule_start_time != 0 {
        object.insert(
            "scheduleStartTime".into(),
            serde_json::json!(record.schedule_start_time),
        );
    }
    if record.schedule_end_time != 0 {
        object.insert(
            "scheduleEndTime".into(),
            serde_json::json!(record.schedule_end_time),
        );
    }
    object.insert(
        "scheduleDaysEnabled".into(),
        serde_json::Value::Array(
            record
                .schedule_days_enabled
                .iter()
                .filter(|day| (1..=7).contains(*day))
                .map(|day| serde_json::json!(day))
                .collect(),
        ),
    );
    if record.deleted_at_timestamp_ms != 0 {
        object.insert(
            "deletedAtTimestampMs".into(),
            serde_json::json!(record.deleted_at_timestamp_ms),
        );
    }
    Some(serde_json::Value::Object(object))
}

// ---------------------------------------------------------------------------
// 批计划纯函数（幂等/替换/删除闸）
// ---------------------------------------------------------------------------

/// 批写计划：云态 + 调用方 records/removeIds → upsert 列表。同实体同值
/// 跳过（幂等不轮换 key）；同实体异值换写（旧 key 进 deleteKey）；新实体
/// 插入；removeIds 命中 → 纯删除 upsert（payload None，identifier 出
/// manifest + deleteKey）；未命中 removeId 忽略。返回空 = 整批幂等，
/// 调用方无 PUT。
pub(crate) fn plan_record_batch(
    cloud: &[CloudEntry],
    records: &[RecordValue],
    remove_ids: &[Vec<u8>],
) -> Vec<RecordUpsert> {
    let mut upserts = Vec::new();
    for record in records {
        match cloud
            .iter()
            .find(|entry| entry.value.entity_id() == record.entity_id())
        {
            Some(entry) if entry.value == *record => {}
            Some(entry) => upserts.push(RecordUpsert {
                kind: record.kind(),
                new_key: new_item_key(),
                replaced_key: Some(entry.key.clone()),
                payload: Some(record_payload(record)),
            }),
            None => upserts.push(RecordUpsert {
                kind: record.kind(),
                new_key: new_item_key(),
                replaced_key: None,
                payload: Some(record_payload(record)),
            }),
        }
    }
    for remove_id in remove_ids {
        if let Some(entry) = cloud
            .iter()
            .find(|entry| entry.value.entity_id() == remove_id.as_slice())
        {
            upserts.push(RecordUpsert {
                kind: entry.value.kind(),
                new_key: Vec::new(),
                replaced_key: Some(entry.key.clone()),
                payload: None,
            });
        }
    }
    upserts
}

fn record_payload(value: &RecordValue) -> RecordPayload {
    match value {
        RecordValue::Folder(record) => RecordPayload::ChatFolder(record.clone()),
        RecordValue::Profile(record) => RecordPayload::NotificationProfile(record.clone()),
    }
}

// ---------------------------------------------------------------------------
// 云态读取（list 面与写循环共用）
// ---------------------------------------------------------------------------

/// manifest（Option；None = 无 storage 状态）→ 本类别全部记录（存储序 =
/// manifest identifiers 首现序；单条解密/解码失败跳过不炸面）。
async fn read_cloud_entries(
    push: &PushService,
    storage_key: &StorageServiceKey,
    manifest: Option<&ManifestRecord>,
    selector: StorageRecordKind,
) -> Result<Vec<CloudEntry>, StorageSyncError> {
    let Some(manifest) = manifest else {
        return Ok(Vec::new());
    };
    let wanted = selector.identifier_type();
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| identifier.r#type == wanted)
        .map(|identifier| identifier.raw.clone())
        .collect();
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let record_ikm = if manifest.record_ikm.is_empty() {
        None
    } else {
        Some(manifest.record_ikm.as_slice())
    };
    let auth = storage_auth(push).await?;
    let items = read_raw_items(push, auth, keys.clone()).await?;
    let mut out = Vec::new();
    for key in keys {
        let Some(item) = items.iter().find(|item| item.key == key) else {
            continue;
        };
        let Ok(plaintext) = decrypt_raw_item(storage_key, item, record_ikm) else {
            continue;
        };
        let Ok(record) = storage_proto::StorageRecord::decode(plaintext.as_slice()) else {
            continue;
        };
        let value = match (selector, record.record) {
            (StorageRecordKind::ChatFolder, Some(Record::ChatFolder(inner))) => {
                RecordValue::Folder(inner)
            }
            (StorageRecordKind::NotificationProfile, Some(Record::NotificationProfile(inner))) => {
                RecordValue::Profile(inner)
            }
            _ => continue,
        };
        out.push(CloudEntry { key, value });
    }
    Ok(out)
}

/// 当前 manifest（Some / NotFound → None / 其余错误 → 调用方指定的失败
/// 形态：读面 ReadFailed、写面 WriteFailed——写意图未落地贴纸先例）。
async fn fetch_manifest(
    storage: &StorageService,
    failure: StorageSyncError,
) -> Result<Option<ManifestRecord>, StorageSyncError> {
    match storage.manifest().await {
        Ok(manifest) => Ok(Some(manifest)),
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => Ok(None),
        Err(_) => Err(failure),
    }
}

// ---------------------------------------------------------------------------
// 编排（Service 调用入口；网络面，不做网络测试）
// ---------------------------------------------------------------------------

/// 读面：`chatFolders.list` / `notificationProfiles.list` 存储序投影。
/// master key 缺失或 manifest 404 → 空表（账号从未有过 storage 状态）。
pub(crate) async fn read_records(
    context: &StorageContext,
    selector: StorageRecordKind,
) -> Result<Vec<serde_json::Value>, StorageSyncError> {
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok(Vec::new());
    };
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let manifest = fetch_manifest(&storage, StorageSyncError::ReadFailed).await?;
    Ok(read_cloud_entries(
        &context.push,
        &StorageServiceKey::from_master_key(&master_key),
        manifest.as_ref(),
        selector,
    )
    .await?
    .iter()
    .filter_map(|entry| entry.value.project())
    .collect())
}

/// 写面：`chatFolders.setRecords` / `notificationProfiles.setRecords`
/// （官方 upload 语义 + 409 采纳重放；首写分支播种 Account——服务器要求
/// manifest 恒含一条 Account 记录）。返回写后全量投影（存储序 = 写后
/// manifest 本类别 identifiers 首现序）；整批幂等 = 无 PUT 直接返回当前
/// 云态投影。不登记 manifest-state（记录面不扩排查字段，compat §5）。
pub(crate) async fn write_records(
    context: &StorageContext,
    selector: StorageRecordKind,
    records: Vec<RecordValue>,
    remove_ids: Vec<Vec<u8>>,
) -> Result<Vec<serde_json::Value>, StorageSyncError> {
    // master key 播种分支（贴纸面同构）：账号从未有过 storage 状态时以
    // 新 key 建链，Account 记录随本批首写入 manifest。
    let master_key = match context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?
    {
        Some(key) => key,
        None => {
            let generated =
                presage::libsignal_service::master_key::MasterKey::generate(&mut rand::rng());
            context
                .store
                .store_master_key(Some(&generated))
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?;
            generated
        }
    };
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let mut manifest = {
        let candidate = fetch_manifest(&storage, StorageSyncError::WriteFailed).await?;
        // 有 manifest 无 Account 标识 = 非法状态（置顶/贴纸面同口径）。
        if candidate.as_ref().is_some_and(|record| {
            crate::service::storage_sync::account_item_key_of(record).is_none()
        }) {
            return Err(StorageSyncError::WriteFailed);
        }
        candidate
    };

    // 冲突重放循环：每轮从「当前服务器状态 + 本地意图」重建 WriteOperation。
    let mut retries: u32 = 0;
    loop {
        // 重放循环内读云态失败归 WriteFailed（写意图未落地；贴纸先例——
        // read_cloud_entries 内部错误按本面语义折叠）。
        let current = read_cloud_entries(
            &context.push,
            &StorageServiceKey::from_master_key(&master_key),
            manifest.as_ref(),
            selector,
        )
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?;
        let upserts = plan_record_batch(&current, &records, &remove_ids);
        if upserts.is_empty() {
            // 整批幂等：与云上现存结构全等 → 无 PUT 不 bump version，
            // 重放安全。
            return Ok(current
                .iter()
                .filter_map(|entry| entry.value.project())
                .collect());
        }

        // 首写分支播种 Account（仅 manifest 404）；其余写不动 Account item。
        let mut batch: Vec<RecordUpsert> = Vec::with_capacity(upserts.len() + 1);
        let previous = match &manifest {
            Some(record) => Some(record),
            None => {
                batch.push(RecordUpsert {
                    kind: StorageRecordKind::Account,
                    new_key: new_item_key(),
                    replaced_key: None,
                    payload: Some(RecordPayload::Account(
                        crate::service::storage_sync::minimal_account_record(Vec::new()),
                    )),
                });
                None
            }
        };
        batch.extend(upserts);
        let planned = plan_record_writes(
            previous,
            &batch,
            context.device_id,
            &StorageServiceKey::from_master_key(&master_key),
        );

        let auth = storage_auth(&context.push).await?;
        match put_write_operation(&context.push, auth, &planned.write_operation).await? {
            PutOutcome {
                conflict_manifest: None,
            } => {
                // 写后全量投影（存储序）：新 item → 本批写入值，其余 →
                // 本轮读取态（写成功即我方 manifest 整体生效）。
                let mut out = Vec::new();
                for identifier in planned
                    .manifest_record
                    .identifiers
                    .iter()
                    .filter(|identifier| identifier.r#type == selector.identifier_type())
                {
                    let written = batch
                        .iter()
                        .find(|upsert| upsert.payload.is_some() && upsert.new_key == identifier.raw)
                        .and_then(|upsert| match upsert.payload.as_ref() {
                            Some(RecordPayload::ChatFolder(record)) => {
                                Some(RecordValue::Folder(record.clone()))
                            }
                            Some(RecordPayload::NotificationProfile(record)) => {
                                Some(RecordValue::Profile(record.clone()))
                            }
                            _ => None,
                        });
                    let value = match written {
                        Some(value) => Some(value),
                        None => current
                            .iter()
                            .find(|entry| entry.key == identifier.raw)
                            .map(|entry| &entry.value)
                            .cloned(),
                    };
                    if let Some(json) = value.as_ref().and_then(RecordValue::project) {
                        out.push(json);
                    }
                }
                return Ok(out);
            }
            PutOutcome {
                conflict_manifest: Some(server_manifest),
            } => match conflict_decision(retries) {
                ConflictDecision::GiveUp => return Err(StorageSyncError::WriteFailed),
                ConflictDecision::Retry { delay_ms } => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    retries += 1;
                    // 采纳服务器 manifest（官方 409 语义）；Account 标识
                    // 校验同首读（置顶/贴纸面同口径）；下轮整体重读重放。
                    let adopted = StorageService::decrypt_manifest(
                        &StorageServiceKey::from_master_key(&master_key),
                        &server_manifest,
                    )
                    .map_err(|_| StorageSyncError::WriteFailed)?;
                    if crate::service::storage_sync::account_item_key_of(&adopted).is_none() {
                        return Err(StorageSyncError::WriteFailed);
                    }
                    manifest = Some(adopted);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FOLDER_ID: &str = "11111111-2222-4333-8444-555555555555";
    const CONTACT_ID: &str = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
    const CONTACT_PNI: &str = "PNI:7c2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
    const PROFILE_ID_HEX: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn folder_roundtrip_full_face_and_three_recipient_forms() {
        let input = json!({
            "id": FOLDER_ID,
            "name": "工作",
            "position": 3,
            "showOnlyUnread": true,
            "showMutedChats": false,
            "includeAllIndividualChats": true,
            "includeAllGroupChats": false,
            "folderType": "CUSTOM",
            "includedConversationIds": [
                { "conversationId": CONTACT_ID, "kind": "contact" },
                { "conversationId": base64::engine::general_purpose::STANDARD.encode([0u8; 32]), "kind": "group" },
                { "conversationId": "legacy-原始", "kind": "legacyGroup" },
            ],
            "excludedConversationIds": [
                { "conversationId": CONTACT_PNI, "kind": "contact" },
            ],
            "deletedAtTimestampMs": 0,
        });
        let record = parse_folder(&input).expect("parse");
        // proto 中介：StorageRecord 包裹 encode → decode（官方线格式位）。
        use prost::Message as _;
        let wire = crate::proto::storage::StorageRecord {
            record: Some(crate::proto::storage::storage_record::Record::ChatFolder(
                match &record {
                    RecordValue::Folder(inner) => inner.clone(),
                    _ => panic!("folder"),
                },
            )),
        }
        .encode_to_vec();
        let decoded =
            crate::proto::storage::StorageRecord::decode(wire.as_slice()).expect("decode");
        let Some(crate::proto::storage::storage_record::Record::ChatFolder(inner)) = decoded.record
        else {
            panic!("chat folder arm");
        };
        let projected = project_folder(&inner).expect("project");
        assert_eq!(projected, input);
        // recipients 双射逐形态核对（contact 双形态：ACI 字符串 + PNI 前缀）。
        let included = projected["includedConversationIds"]
            .as_array()
            .expect("arr");
        assert_eq!(included[0]["conversationId"], CONTACT_ID);
        assert_eq!(included[0]["kind"], "contact");
        use base64::Engine as _;
        assert_eq!(
            included[1]["conversationId"],
            base64::engine::general_purpose::STANDARD.encode([0u8; 32])
        );
        assert_eq!(included[1]["kind"], "group");
        assert_eq!(included[2]["conversationId"], "legacy-原始");
        assert_eq!(included[2]["kind"], "legacyGroup");
        assert_eq!(
            projected["excludedConversationIds"][0]["conversationId"],
            CONTACT_PNI
        );
    }

    #[test]
    fn folder_optional_fields_default_and_tombstone_presence() {
        // deletedAtTimestampMs 缺省 = 0，投影恒在键（0 = 未删）。
        let input = json!({
            "id": FOLDER_ID,
            "name": "",
            "position": 0,
            "showOnlyUnread": false,
            "showMutedChats": false,
            "includeAllIndividualChats": false,
            "includeAllGroupChats": false,
            "folderType": "ALL",
        });
        let record = parse_folder(&input).expect("parse");
        let RecordValue::Folder(inner) = &record else {
            panic!("folder");
        };
        assert_eq!(inner.deleted_at_timestamp_ms, 0);
        assert_eq!(inner.included_recipients.len(), 0);
        let projected = project_folder(inner).expect("project");
        assert_eq!(projected["deletedAtTimestampMs"], 0);
        assert_eq!(projected["folderType"], "ALL");
        // 墓碑形态：非 0 时间戳原样往返。
        let tombstone = json!({
            "id": FOLDER_ID,
            "name": "gone",
            "position": 1,
            "showOnlyUnread": false,
            "showMutedChats": false,
            "includeAllIndividualChats": false,
            "includeAllGroupChats": false,
            "folderType": "CUSTOM",
            "deletedAtTimestampMs": 1700000000000u64,
        });
        let RecordValue::Folder(inner) = &parse_folder(&tombstone).expect("parse") else {
            panic!("folder");
        };
        assert_eq!(inner.deleted_at_timestamp_ms, 1700000000000);
    }

    #[test]
    fn folder_parse_gates_reject_malformed() {
        let valid = json!({
            "id": FOLDER_ID,
            "name": "x",
            "position": 1,
            "showOnlyUnread": false,
            "showMutedChats": false,
            "includeAllIndividualChats": false,
            "includeAllGroupChats": false,
            "folderType": "CUSTOM",
        });
        // 词表外 folderType。
        let mut bad = valid.clone();
        bad["folderType"] = json!("SOMETING");
        assert!(parse_folder(&bad).is_none());
        // id 非 UUID。
        let mut bad = valid.clone();
        bad["id"] = json!("not-a-uuid");
        assert!(parse_folder(&bad).is_none());
        // 缺 name。
        let mut bad = valid.clone();
        bad.as_object_mut().expect("obj").remove("name");
        assert!(parse_folder(&bad).is_none());
        // position 负数（浮点/负数不满足非负整数闸）。
        let mut bad = valid.clone();
        bad["position"] = json!(-1);
        assert!(parse_folder(&bad).is_none());
        // position 超 u32。
        let mut bad = valid.clone();
        bad["position"] = json!(u64::from(u32::MAX) + 1);
        assert!(parse_folder(&bad).is_none());
        // 布尔字段非布尔。
        let mut bad = valid.clone();
        bad["showOnlyUnread"] = json!("yes");
        assert!(parse_folder(&bad).is_none());
        // recipient kind 词表外（写面整拒）。
        let mut bad = valid.clone();
        bad["includedConversationIds"] = json!([{ "conversationId": "x", "kind": "other" }]);
        assert!(parse_folder(&bad).is_none());
        // recipients 非数组。
        let mut bad = valid.clone();
        bad["includedConversationIds"] = json!("nope");
        assert!(parse_folder(&bad).is_none());
        // 条目非对象整体拒。
        assert!(parse_folder(&json!("root")).is_none());
    }

    #[test]
    fn profile_roundtrip_full_face_schedule_and_omissions() {
        let input = json!({
            "id": PROFILE_ID_HEX,
            "name": "睡眠",
            "emoji": "😴",
            "color": 4278190335u64,
            "createdAtMs": 1700000000000u64,
            "allowAllCalls": true,
            "allowAllMentions": false,
            "allowedMembers": [
                { "conversationId": CONTACT_ID, "kind": "contact" },
            ],
            "scheduleEnabled": true,
            "scheduleStartTime": 2300,
            "scheduleEndTime": 700,
            "scheduleDaysEnabled": [1, 2, 3, 4, 5],
        });
        let record = parse_profile(&input).expect("parse");
        let RecordValue::Profile(inner) = &record else {
            panic!("profile");
        };
        assert_eq!(inner.id.len(), 16);
        let projected = project_profile(inner).expect("project");
        assert_eq!(projected, input);
        // 0 值缺省形态：schedule 族/emoji/墓碑键不在。
        let minimal = json!({
            "id": PROFILE_ID_HEX,
            "name": "工作",
            "color": 0,
            "createdAtMs": 0,
            "allowAllCalls": false,
            "allowAllMentions": false,
            "scheduleEnabled": false,
        });
        let RecordValue::Profile(inner) = &parse_profile(&minimal).expect("parse") else {
            panic!("profile");
        };
        assert!(inner.emoji.is_none());
        let projected = project_profile(inner).expect("project");
        assert!(projected.get("emoji").is_none());
        assert!(projected.get("scheduleStartTime").is_none());
        assert!(projected.get("scheduleEndTime").is_none());
        assert!(projected.get("deletedAtTimestampMs").is_none());
        assert_eq!(projected["scheduleDaysEnabled"], json!([]), "恒在数组");
    }

    #[test]
    fn profile_parse_gates_reject_malformed() {
        let valid = json!({
            "id": PROFILE_ID_HEX,
            "name": "x",
            "color": 0,
            "createdAtMs": 0,
            "allowAllCalls": false,
            "allowAllMentions": false,
            "scheduleEnabled": false,
        });
        // id 非 32 位 hex（大写显式拒绝同 hex_decode 口径）。
        let mut bad = valid.clone();
        bad["id"] = json!("0123456789ABCDEF0123456789ABCDEF");
        assert!(parse_profile(&bad).is_none());
        // name 空串拒绝（非空必填）。
        let mut bad = valid.clone();
        bad["name"] = json!("");
        assert!(parse_profile(&bad).is_none());
        // scheduleDaysEnabled 条目超 1..=7。
        let mut bad = valid.clone();
        bad["scheduleDaysEnabled"] = json!([1, 8]);
        assert!(parse_profile(&bad).is_none());
        let mut bad = valid.clone();
        bad["scheduleDaysEnabled"] = json!([0]);
        assert!(parse_profile(&bad).is_none());
        // color 超 u32。
        let mut bad = valid.clone();
        bad["color"] = json!(u64::from(u32::MAX) + 1);
        assert!(parse_profile(&bad).is_none());
        // emoji 非字符串。
        let mut bad = valid.clone();
        bad["emoji"] = json!(3);
        assert!(parse_profile(&bad).is_none());
    }

    fn cloud_of(records: &[RecordValue]) -> Vec<CloudEntry> {
        records
            .iter()
            .enumerate()
            .map(|(index, value)| CloudEntry {
                key: vec![0xAA, index as u8],
                value: value.clone(),
            })
            .collect()
    }

    fn folder_value(id: &str, name: &str, position: u32) -> RecordValue {
        let mut json_value = json!({
            "id": id,
            "name": name,
            "position": position,
            "showOnlyUnread": false,
            "showMutedChats": false,
            "includeAllIndividualChats": false,
            "includeAllGroupChats": false,
            "folderType": "CUSTOM",
        });
        if position > 0 {
            json_value["showOnlyUnread"] = json!(true);
        }
        parse_folder(&json_value).expect("parse")
    }

    #[test]
    fn plan_record_batch_idempotent_replace_insert_remove() {
        let existing = folder_value(FOLDER_ID, "原有", 1);
        let cloud = cloud_of(std::slice::from_ref(&existing));

        // 同值幂等：无 upsert（不轮换 item id）。
        let same = folder_value(FOLDER_ID, "原有", 1);
        assert!(plan_record_batch(&cloud, &[same], &[]).is_empty());

        // 变更：换写 upsert 带 replaced_key（旧 identifier 进 deleteKey）。
        let changed = folder_value(FOLDER_ID, "改名", 1);
        let upserts = plan_record_batch(&cloud, &[changed], &[]);
        assert_eq!(upserts.len(), 1);
        assert_eq!(
            upserts[0].replaced_key.as_deref(),
            Some(cloud[0].key.as_slice())
        );
        assert!(upserts[0].payload.is_some());

        // 新实体：无 replaced_key。
        let fresh = folder_value("99999999-9999-4999-8999-999999999999", "新", 2);
        let upserts = plan_record_batch(&cloud, &[fresh], &[]);
        assert_eq!(upserts.len(), 1);
        assert!(upserts[0].replaced_key.is_none());

        // removeId 命中 → 纯删除 upsert（payload None、new_key 空）。
        let remove_id = uuid_bytes(&json!(FOLDER_ID)).expect("id bytes");
        let upserts = plan_record_batch(&cloud, &[], &[remove_id]);
        assert_eq!(upserts.len(), 1);
        assert_eq!(
            upserts[0].replaced_key.as_deref(),
            Some(cloud[0].key.as_slice())
        );
        assert!(upserts[0].payload.is_none());

        // removeId 未命中 → 忽略（空批）。
        let unknown = uuid_bytes(&json!("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")).expect("id");
        assert!(plan_record_batch(&cloud, &[], &[unknown]).is_empty());
    }

    #[test]
    fn plan_record_batch_mixed_batch_and_kind_isolation() {
        let folder = folder_value(FOLDER_ID, "夹", 1);
        let profile_input = json!({
            "id": PROFILE_ID_HEX,
            "name": "档",
            "color": 0,
            "createdAtMs": 0,
            "allowAllCalls": false,
            "allowAllMentions": false,
            "scheduleEnabled": false,
        });
        let profile = parse_profile(&profile_input).expect("parse");
        let cloud = vec![
            CloudEntry {
                key: vec![1],
                value: folder,
            },
            CloudEntry {
                key: vec![2],
                value: profile.clone(),
            },
        ];
        // 混合批：folder 变更 + profile 幂等 + 未知新 profile 插入。
        let renamed = folder_value(FOLDER_ID, "改名", 1);
        let fresh_input = json!({
            "id": "ffffffffffffffffffffffffffffffff",
            "name": "新档",
            "color": 1,
            "createdAtMs": 0,
            "allowAllCalls": true,
            "allowAllMentions": true,
            "scheduleEnabled": false,
        });
        let fresh = parse_profile(&fresh_input).expect("parse");
        let upserts = plan_record_batch(&cloud, &[renamed, profile, fresh], &[]);
        assert_eq!(upserts.len(), 2, "幂等条目不出批");
        assert_eq!(
            upserts[0].kind,
            crate::service::storage_sync::StorageRecordKind::ChatFolder
        );
        assert_eq!(
            upserts[1].kind,
            crate::service::storage_sync::StorageRecordKind::NotificationProfile
        );
    }

    #[test]
    fn entity_id_matching_is_byte_level() {
        // 同实体不同排版 uuid（braced/urn/hyphenless）判等同条。
        let braced = json!({
            "id": format!("{{{FOLDER_ID}}}"),
            "name": "x",
            "position": 1,
            "showOnlyUnread": false,
            "showMutedChats": false,
            "includeAllIndividualChats": false,
            "includeAllGroupChats": false,
            "folderType": "CUSTOM",
        });
        let plain = folder_value(FOLDER_ID, "x", 1);
        assert_eq!(braced_entity(&braced), plain.entity_id());
    }

    fn braced_entity(value: &serde_json::Value) -> Vec<u8> {
        match parse_folder(value).expect("parse") {
            RecordValue::Folder(inner) => inner.identifier,
            _ => panic!("folder"),
        }
    }
}
