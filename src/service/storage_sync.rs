// SPDX-License-Identifier: AGPL-3.0-only

//! Storage Service 同步：会话级置顶（P4 扩面）+ 拉黑只读面（P5，
//! `getBlockedConversations` 批量原样读）+ 写面泛化（P5，支撑贴纸包
//! 记录面 `storage_sticker` 与账号级设置写面 `setAccountConfiguration`/
//! `setUniversalExpireTimer`，共用 `update_account_record` 编排骨架）。
//!
//! 真相源 = Storage Service 的 `AccountRecord.pinned_conversations`（数组序
//! 即置顶顺序，无时间戳）。读面走 presage 上游 pub API（`StorageService::
//! manifest`）；写面是上游缺口（libsignal-service-rs 只有读），自建薄封装
//! 收敛在本模块 `put_write_operation` 单点，上游补齐后整段替换。
//!
//! **记录载荷 proto = 官方 vendored**（`crate::proto::storage`，字段保真
//! 加固）：钉定 presage rev 的 StorageService.proto 只定义到 AccountRecord
//! field 51，官方现行已用到 field 60——Account/Contact 等记录的 decode→
//! mutate→encode 一律走 vendored 完整字段面 + 本模块 item 加解密 shim
//! （`read_account_record`/`account_record_of_wire`/`encrypt_raw_item`），
//! 否则每次置顶/贴纸/账号设置写都会把手机侧 52-60 的未知字段静默清零
//! （`tests::account_record_fields_52_to_60_survive_engine_write_path`
//! 字段保真铁面）。manifest/WriteOperation 面形状完整，仍走 presage 类型。
//!
//! 官方写语义（Signal-Desktop `ts/services/storage.preload.ts` 行为对齐，
//! 不抄码）：读 manifest → 读 Account item → **只在服务器返回的
//! AccountRecord 上原地改 pinned 字段**（decode→mutate→encode round-trip
//! 保真其余字段；绝禁止从零构造 AccountRecord，会清空用户其他设备账号
//! 设置）→ `encrypt_item`（新 16B 随机 id）+ `encrypt_manifest`（version+1，
//! source_device=本设备 device_id，record_ikm 原样透传）→
//! `WriteOperation{manifest, insertItem, deleteKey}` → `PUT v1/storage/`
//! （WebAPI.preload.ts:831 storageModify 同路径）。409 响应体含服务器当前
//! manifest → 采纳重读，按 conflictBackOff [1s,5s,30s]
//! （storage.preload.ts:150）重放，连满 3 次放弃本轮。
//!
//! 写面泛化（P5）：`plan_record_writes` 支持多记录类别 upsert（Account=
//! identifier type 4、StickerPack=type 6，换入新 key / 旧 key 进
//! deleteKey / version+1 / source_device·record_ikm 透传），置顶面
//! `plan_write` 收敛为其 Account 单记录薄壳，行为逐字节不变。
//!
//! 贴纸记录 proto 收敛点：钉定 presage rev 的 StorageService.proto 落后
//! （无 STICKER_PACK=6/StickerPackRecord），该记录的 item 加解密无法走
//! `StorageService::encrypt_item/decrypt_item`——本模块提供同构 shim
//! （`encrypt_raw_item`/`decrypt_raw_item`/`read_raw_items`，派生与
//! AES-GCM 形状按上游 storage_service.rs 文档化实现，双向交叉验证单测钉
//! 定），上游 proto 补齐后整段替换。Account 面不受影响，仍走上游 pub API。
//!
//! 账号从未有过 storage 状态（manifest 404）：读 → 空列表；写 → 以
//! version 1 + 最小 AccountRecord（只含 pinned）建初始 manifest——该分支
//! 语义见 compat 文档 §5。网络层薄壳不做网络测试。

use std::path::Path;

use base64::Engine as _;
use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::master_key::{MasterKey, StorageServiceKey};
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::proto::manifest_record;
use presage::libsignal_service::proto::{ManifestRecord, StorageManifest, WriteOperation};
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::store::{ContentsStore, StateStore};
use prost::Message as _;
use rand::RngCore;
use serde_json::json;

use crate::proto::storage::{self as storage_proto, AccountRecord, StorageRecord};
// 记录载荷面（AccountRecord/PinnedConversation/StorageRecord）一律走官方
// vendored proto（crate::proto/storage.rs）——钉定 presage rev 的同源 proto
// 只定义到 AccountRecord field 51，直接用会把官方现行 52-60 字段在每次
// storage 写时静默清零（字段保真铁面测试钉定）。manifest/WriteOperation 面
// 仍走 presage 类型（形状完整）。
use crate::proto::storage::account_record::{self, PinnedConversation};
use crate::service::StorageContext;

/// 409 冲突重试上限：官方 conflictBackOff 序列长度（[1s,5s,30s]，storage
/// preload.ts:150），连满放弃本轮返回错误。
pub(crate) const CONFLICT_BACKOFF_STEPS_MS: &[u64] = &[1_000, 5_000, 30_000];

/// 置顶面错误（methods.rs 一一折叠为 compat §4.33 结构化错误码）：
/// UnknownAccount → 引擎 -32603（账号参数不指向已链接账号，非 storage 面），
/// ConversationNotResolved → `CONVERSATION_NOT_RESOLVED`（云引用解析失败），
/// Unavailable → `STORAGE_UNAVAILABLE`（storage 服务/auth 不可达），
/// ReadFailed → `STORAGE_READ_FAILED`（读解密/item 读取失败），
/// WriteFailed → `STORAGE_WRITE_FAILED`（PUT 失败 / 409 连满 3 次放弃 /
/// 写面冲突重放中的任何失败——写意图未落地）。
#[derive(Debug)]
pub(crate) enum StorageSyncError {
    /// 账号不在注册表 / store 不可开 / 注册数据不可读。
    UnknownAccount,
    /// 会话标识解析不回存储标识（写面反投影失败）。
    ConversationNotResolved,
    /// storage 服务/auth 不可达（manifest 拉取网络失败等）。
    Unavailable,
    /// 读面失败（item 读取/解密）。
    ReadFailed,
    /// 写面失败（PUT、冲突重放放弃、写面本地状态准备失败）。
    WriteFailed,
    /// storage.write 通用写面：调用方意图与现存记录冲突（INSERT 目标 key
    /// 已存在且内容不同 / UPDATE 换记录类型）——fail-closed -32603，引擎
    /// 不静默替换外部记录。
    IntentConflict,
    /// storage.write 通用写面：服务器 409 = base version 失配。写意图未
    /// 落地且**不自动重试**（mutating 语义，引擎不可重推调用方 record）；
    /// server_version = 409 响应体 StorageManifest.version（明文可读，取
    /// 不到缺席）。
    VersionConflict { server_version: Option<u64> },
}

/// 置顶条目投影（引擎会话标识口径）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PinnedEntry {
    /// 引擎会话标识（存储条目映射不出本引擎口径时为 None，投影层跳过）。
    pub(crate) id: Option<EngineConversationId>,
    /// 存储侧原始条目（写面原样保留其他形态的条目，如 ReleaseNotes）。
    pub(crate) raw: PinnedConversation,
}

/// 引擎会话标识口径（compat §3 pinned 行）：contact=ACI service id 字符串
/// （listContacts uuid 同源）、group=master key 标准 base64（send 的 groupId
/// 同口径）、legacyGroup=legacy groupId bytes。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EngineConversationId {
    Contact(String),
    Group(String),
    LegacyGroup(String),
}

impl EngineConversationId {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Contact(_) => "contact",
            Self::Group(_) => "group",
            Self::LegacyGroup(_) => "legacyGroup",
        }
    }

    pub(crate) fn id_string(&self) -> &str {
        match self {
            Self::Contact(id) | Self::Group(id) | Self::LegacyGroup(id) => id,
        }
    }
}

/// 存储侧 PinnedConversation → 引擎会话标识（映射不出的条目 None，调用方
/// 跳过并 warn，不炸整面）。
pub(crate) fn storage_to_engine(pinned: &PinnedConversation) -> Option<EngineConversationId> {
    use base64::Engine as _;
    match pinned.identifier.as_ref()? {
        account_record::pinned_conversation::Identifier::Contact(contact) => {
            // ACI 优先：serviceIdBinary 16 字节（ACI 无前缀）→ UUID 字符串；
            // 字符串形态直通；e164 形态非本引擎 contact 标识口径 → 跳过。
            if !contact.service_id_binary.is_empty() {
                let uuid = presage::libsignal_service::prelude::Uuid::from_slice(
                    &contact.service_id_binary,
                )
                .ok()?;
                return Some(EngineConversationId::Contact(uuid.to_string()));
            }
            if contact.service_id.is_empty() {
                return None;
            }
            presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(
                &contact.service_id,
            )
            .map(|sid| EngineConversationId::Contact(sid.service_id_string()))
        }
        account_record::pinned_conversation::Identifier::GroupMasterKey(key) => Some(
            EngineConversationId::Group(base64::engine::general_purpose::STANDARD.encode(key)),
        ),
        account_record::pinned_conversation::Identifier::LegacyGroupId(id) => Some(
            EngineConversationId::LegacyGroup(String::from_utf8_lossy(id).into_owned()),
        ),
        // ReleaseNotes 是官方 Signal 账号内置会话，非引擎会话口径 → 跳过。
        account_record::pinned_conversation::Identifier::ReleaseNotes(_) => None,
    }
}

/// 引擎会话标识 → 存储侧 PinnedConversation（写面正向投影）。contact 需
/// 引擎侧解析（ACI 字符串直通），group 传 master key bytes。
pub(crate) fn engine_to_storage(
    id: &EngineConversationId,
    contact_service_id: Option<&str>,
) -> Result<PinnedConversation, StorageSyncError> {
    let identifier = match (id, contact_service_id) {
        (EngineConversationId::Contact(_), Some(service_id)) => {
            let sid =
                presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(
                    service_id,
                )
                .ok_or(StorageSyncError::ConversationNotResolved)?;
            account_record::pinned_conversation::Identifier::Contact(
                account_record::pinned_conversation::Contact {
                    service_id: sid.service_id_string(),
                    e164: String::new(),
                    service_id_binary: sid.service_id_binary(),
                },
            )
        }
        (EngineConversationId::Contact(_), None) => {
            return Err(StorageSyncError::ConversationNotResolved)
        }
        (EngineConversationId::Group(_), _) => {
            return Err(StorageSyncError::ConversationNotResolved)
        }
        (EngineConversationId::LegacyGroup(_), _) => {
            return Err(StorageSyncError::ConversationNotResolved)
        }
    };
    Ok(PinnedConversation {
        identifier: Some(identifier),
    })
}

/// group 标识反查存储 master key bytes（send 的 groupId = 标准 base64）。
pub(crate) fn engine_group_to_master_key(
    id: &EngineConversationId,
) -> Result<Vec<u8>, StorageSyncError> {
    match id {
        EngineConversationId::Group(group_id) => crate::service::decode_group_id(group_id)
            .ok_or(StorageSyncError::ConversationNotResolved),
        _ => Err(StorageSyncError::ConversationNotResolved),
    }
}

/// legacy 标识反查 bytes。
pub(crate) fn engine_legacy_to_bytes(
    id: &EngineConversationId,
) -> Result<Vec<u8>, StorageSyncError> {
    match id {
        EngineConversationId::LegacyGroup(text) => Ok(text.clone().into_bytes()),
        _ => Err(StorageSyncError::ConversationNotResolved),
    }
}

/// 409 冲突重试决策纯函数：输入本轮已重试次数，输出动作。
/// - `Retry{delay_ms}`：sleep 后基于服务器 manifest 重放本地意图；
/// - `GiveUp`：连满 3 次放弃本轮（官方 conflictBackOff.isFull 语义）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConflictDecision {
    Retry { delay_ms: u64 },
    GiveUp,
}

pub(crate) fn conflict_decision(retries_done: u32) -> ConflictDecision {
    match CONFLICT_BACKOFF_STEPS_MS.get(retries_done as usize) {
        Some(delay_ms) => ConflictDecision::Retry {
            delay_ms: *delay_ms,
        },
        None => ConflictDecision::GiveUp,
    }
}

/// 账号目录（data_dir/accounts/lk-…）下的本地 manifest 状态文件。
///
/// P5 泛化：`sticker_item_keys` 记最近一次写成功 manifest 里全部
/// STICKER_PACK=6 identifier（b64）。`None` = 本次写不感知该面（置顶面），
/// 落盘沿用文件旧值；`Some` = 覆盖。旧格式文件（无 stickerItemKeys 键）
/// 兼容读取为 `None`。服务器 manifest 为真相源，本文件仅测试/排查。
pub(crate) struct ManifestState {
    pub(crate) version: u64,
    /// 上次写成功的 Account item id（16B raw，base64 存）。
    pub(crate) account_item_key: Option<String>,
    /// STICKER_PACK item key 集（b64）；None = 沿用旧值。
    pub(crate) sticker_item_keys: Option<Vec<String>>,
}

const MANIFEST_STATE_FILE: &str = "storage-manifest-state.json";

/// 容错读本地状态（缺文件/坏 JSON/缺键 → 对应字段缺省；旧格式兼容）。
pub(crate) fn load_manifest_state(account_dir: &Path) -> Option<ManifestState> {
    let bytes = std::fs::read(account_dir.join(MANIFEST_STATE_FILE)).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    Some(ManifestState {
        version: value.get("version").and_then(serde_json::Value::as_u64)?,
        account_item_key: value
            .get("accountItemKey")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        sticker_item_keys: value
            .get("stickerItemKeys")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect()
            }),
    })
}

/// 写本地状态（best-effort：半写不可见，临时文件 + rename；格式为
/// `{version, accountItemKey, stickerItemKeys}`，读取方仅测试/排查，
/// 服务器 manifest 为真相源）。sticker_item_keys=None 时沿用文件旧值。
pub(crate) fn save_manifest_state(account_dir: &Path, state: &ManifestState) {
    let sticker_item_keys = state
        .sticker_item_keys
        .clone()
        .or_else(|| load_manifest_state(account_dir).and_then(|loaded| loaded.sticker_item_keys))
        .unwrap_or_default();
    let Ok(bytes) = serde_json::to_vec_pretty(&json!({
        "version": state.version,
        "accountItemKey": state.account_item_key,
        "stickerItemKeys": sticker_item_keys,
    })) else {
        return;
    };
    let path = account_dir.join(MANIFEST_STATE_FILE);
    let tmp = account_dir.join(format!("{MANIFEST_STATE_FILE}.tmp"));
    if std::fs::write(&tmp, bytes).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// 新 Account item id（官方 generateStorageID 同构：16 随机字节）。
pub(crate) fn new_item_key() -> Vec<u8> {
    let mut key = [0u8; 16];
    rand::rng().fill_bytes(&mut key);
    key.to_vec()
}

/// 最小初始 AccountRecord（仅当账号从未有过 storage 状态时使用；注释与
/// compat §5 登记该分支语义）：只含 pinned，其余字段默认——官方首次启用
/// Storage Service 时同样由写方播种 Account 记录。
pub(crate) fn minimal_account_record(pinned: Vec<PinnedConversation>) -> AccountRecord {
    AccountRecord {
        pinned_conversations: pinned,
        ..AccountRecord::default()
    }
}

/// AccountRecord 读-改-写保真核心：decode 服务器返回的 record → 只改
/// pinned 字段 → encode。其余字段 prost round-trip 字节级原样保留。
pub(crate) fn account_record_with_pinned(
    record: &AccountRecord,
    pinned: Vec<PinnedConversation>,
) -> AccountRecord {
    let mut updated = record.clone();
    updated.pinned_conversations = pinned;
    updated
}

/// 从（官方 vendored）StorageRecord 提取 AccountRecord（非 Account 记录 →
/// None）。
pub(crate) fn account_record_of(record: &StorageRecord) -> Option<&AccountRecord> {
    match record.record.as_ref()? {
        storage_proto::storage_record::Record::Account(account) => Some(account),
        _ => None,
    }
}

/// Account item 明文 bytes → AccountRecord（解码失败/非 Account 记录 →
/// None；读面统一入口，字段面 = 官方 vendored proto）。
pub(crate) fn account_record_of_wire(plaintext: &[u8]) -> Option<AccountRecord> {
    account_record_of(&StorageRecord::decode(plaintext).ok()?).cloned()
}

/// manifest identifiers 中 Account 记录的 raw key。
pub(crate) fn account_item_key_of(manifest: &ManifestRecord) -> Option<Vec<u8>> {
    manifest
        .identifiers
        .iter()
        .find(|identifier| identifier.r#type() == manifest_record::identifier::Type::Account)
        .map(|identifier| identifier.raw.clone())
}

/// 写面记录类别（manifest identifier type：Account=4、StickerPack=6、
/// Contact=1、GroupV1=2、GroupV2=3、ChatFolder=8、NotificationProfile=9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageRecordKind {
    Account,
    StickerPack,
    Contact,
    GroupV1,
    GroupV2,
    ChatFolder,
    NotificationProfile,
}

impl StorageRecordKind {
    /// ManifestRecord.Identifier.Type wire 值。StickerPack=官方
    /// SignalStorage.proto :54 STICKER_PACK=6（钉定 presage rev 的枚举缺
    /// 该值，全程以 raw i32 读写，读面同口径识别）。
    pub(crate) fn identifier_type(self) -> i32 {
        match self {
            Self::Account => manifest_record::identifier::Type::Account as i32,
            Self::StickerPack => crate::service::storage_sticker::STICKER_PACK_IDENTIFIER_TYPE,
            Self::Contact => manifest_record::identifier::Type::Contact as i32,
            Self::GroupV1 => manifest_record::identifier::Type::Groupv1 as i32,
            Self::GroupV2 => manifest_record::identifier::Type::Groupv2 as i32,
            Self::ChatFolder => manifest_record::identifier::Type::ChatFolder as i32,
            Self::NotificationProfile => {
                manifest_record::identifier::Type::NotificationProfile as i32
            }
        }
    }
}

/// 一次写面的单条记录意图（官方 toXxxRecord + generateManifest 语义的
/// 输入形状）：新 16B item id 换入 identifiers，`replaced_key`（同类型同
/// 实体的旧 identifier raw）进 deleteKey。Account 恒单条：有旧 manifest
/// 时调用方传其 Account key；StickerPack 按 packId 定位旧 key。
/// `payload: None` = 纯删除（官方 deleteKey 语义：identifier 出 manifest +
/// deleteKey，无新 item——expired-tombstone 清理面；new_key 恒空向量）。
pub(crate) struct RecordUpsert {
    pub(crate) kind: StorageRecordKind,
    pub(crate) new_key: Vec<u8>,
    pub(crate) replaced_key: Option<Vec<u8>>,
    pub(crate) payload: Option<RecordPayload>,
}

// 瞬态写计划值：构造即编码、不批量持有，变体尺寸差不值得 Box 化波及全部构造点。
#[allow(clippy::large_enum_variant)]
pub(crate) enum RecordPayload {
    Account(AccountRecord),
    StickerPack(crate::service::storage_sticker::StickerPackRecord),
    /// 拉黑写面三类记录（官方 vendored proto 完整字段面，decode→mutate→
    /// encode 保真；与 Account 同一加解密 shim 通道）。
    Contact(storage_proto::ContactRecord),
    GroupV1(storage_proto::GroupV1Record),
    GroupV2(storage_proto::GroupV2Record),
    /// 聊天文件夹/通知档案记录面（官方 vendored proto 完整字段面；
    /// 每实体独立 record + identifier，Storage 跨设备同步批）。
    ChatFolder(storage_proto::ChatFolderRecord),
    NotificationProfile(storage_proto::NotificationProfile),
}

/// plan_record_writes 的产出：WriteOperation + 明文 manifest（调用方投影
/// 写后 identifiers / 登记本地状态用）。
pub(crate) struct RecordWritePlan {
    pub(crate) write_operation: WriteOperation,
    pub(crate) new_version: u64,
    pub(crate) manifest_record: ManifestRecord,
}

/// WriteOperation 组装泛化纯函数（官方 generateManifest+encryptManifest
/// 语义收敛点）：新 manifest（version+1、source_device、record_ikm 透传、
/// identifiers=旧集合按 upsert 换入换出）+ 全部新 item + 换出旧 key 删除。
/// Account 换写清除全部 Account 标识（恒单条不变量）；StickerPack 仅清除
/// replaced_key 对应标识（多包并存）。
pub(crate) fn plan_record_writes(
    previous: Option<&ManifestRecord>,
    upserts: &[RecordUpsert],
    source_device: u32,
    storage_key: &StorageServiceKey,
) -> RecordWritePlan {
    let (next_version, mut identifiers, mut delete_keys, record_ikm) = match previous {
        Some(manifest) => (
            manifest.version + 1,
            manifest.identifiers.clone(),
            Vec::new(),
            manifest.record_ikm.clone(),
        ),
        None => (1, Vec::new(), Vec::new(), Vec::new()),
    };
    let record_ikm_opt = if record_ikm.is_empty() {
        None
    } else {
        Some(record_ikm.as_slice())
    };
    let mut insert_item = Vec::with_capacity(upserts.len());
    for upsert in upserts {
        match upsert.kind {
            StorageRecordKind::Account => {
                identifiers.retain(|identifier| {
                    identifier.r#type() != manifest_record::identifier::Type::Account
                });
            }
            // 多条并存类别（贴纸包/联系人/GV1/GV2/聊天文件夹/通知档案）：
            // 仅清除 replaced_key 对应标识（同类型同实体的旧 identifier 进
            // deleteKey）。
            StorageRecordKind::StickerPack
            | StorageRecordKind::Contact
            | StorageRecordKind::GroupV1
            | StorageRecordKind::GroupV2
            | StorageRecordKind::ChatFolder
            | StorageRecordKind::NotificationProfile => {
                if let Some(old_key) = &upsert.replaced_key {
                    identifiers.retain(|identifier| {
                        !(identifier.r#type == upsert.kind.identifier_type()
                            && &identifier.raw == old_key)
                    });
                }
            }
        }
        if let Some(old_key) = &upsert.replaced_key {
            delete_keys.push(old_key.clone());
        }
        // payload=None = 纯删除（官方 deleteKey 语义）：identifier 出
        // manifest + 旧 key 进 deleteKey，无新 identifier 无新 item。
        let Some(payload) = upsert.payload.as_ref() else {
            continue;
        };
        identifiers.push(manifest_record::Identifier {
            raw: upsert.new_key.clone(),
            r#type: upsert.kind.identifier_type(),
        });
        let item = match payload {
            // Account 载荷用官方 vendored proto 编码（完整字段面），经本地
            // item 加解密 shim 上行（与上游 encrypt_item 派生一致，交叉
            // 验证单测钉定）；StickerPack 同 shim（上游 proto 缺该记录）；
            // Contact/GV1/GV2 同 shim（拉黑写面，vendored 完整字段面保真）；
            // ChatFolder/NotificationProfile 同 shim（记录面批，vendored
            // 完整字段面保真）。
            RecordPayload::Account(account) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::Account(
                        account.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
            RecordPayload::StickerPack(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &crate::service::storage_sticker::sticker_record_wire(record),
                record_ikm_opt,
            ),
            RecordPayload::Contact(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::Contact(
                        record.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
            RecordPayload::GroupV1(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::GroupV1(
                        record.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
            RecordPayload::GroupV2(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::GroupV2(
                        record.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
            RecordPayload::ChatFolder(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::ChatFolder(
                        record.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
            RecordPayload::NotificationProfile(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::NotificationProfile(
                        record.clone(),
                    )),
                }
                .encode_to_vec(),
                record_ikm_opt,
            ),
        };
        insert_item.push(item);
    }

    let manifest_record = ManifestRecord {
        version: next_version,
        source_device,
        identifiers,
        record_ikm,
    };
    let manifest = StorageService::encrypt_manifest(storage_key, &manifest_record);
    RecordWritePlan {
        write_operation: WriteOperation {
            manifest: Some(manifest),
            insert_item,
            delete_key: delete_keys,
            clear_all: false,
        },
        new_version: next_version,
        manifest_record,
    }
}

/// WriteOperation 组装（Account 单记录，既有置顶面入口；`plan_record_
/// writes` 的薄壳，行为与泛化前逐字节一致）。
pub(crate) struct PlannedWrite {
    pub(crate) write_operation: WriteOperation,
    pub(crate) new_version: u64,
    pub(crate) new_account_key: Vec<u8>,
}

pub(crate) fn plan_write(
    previous: Option<(&ManifestRecord, Vec<u8>)>,
    new_account_record: &AccountRecord,
    source_device: u32,
    new_key: Vec<u8>,
    storage_key: &StorageServiceKey,
) -> PlannedWrite {
    let (manifest, replaced_key) = match previous {
        Some((manifest, old_key)) => (Some(manifest), Some(old_key)),
        None => (None, None),
    };
    let plan = plan_record_writes(
        manifest,
        &[RecordUpsert {
            kind: StorageRecordKind::Account,
            new_key: new_key.clone(),
            replaced_key,
            payload: Some(RecordPayload::Account(new_account_record.clone())),
        }],
        source_device,
        storage_key,
    );
    PlannedWrite {
        write_operation: plan.write_operation,
        new_version: plan.new_version,
        new_account_key: new_key,
    }
}

/// 写请求薄封装（上游缺口单点）：`PUT v1/storage/`，body = WriteOperation
/// protobuf；200/204 成功，409 由调用方读响应体重试。上游 libsignal-service
/// 补齐写面后整段替换。
pub(crate) struct PutOutcome {
    pub(crate) conflict_manifest: Option<StorageManifest>,
}

pub(crate) async fn put_write_operation(
    push: &PushService,
    auth: presage::libsignal_service::push_service::HttpAuth,
    write_operation: &WriteOperation,
) -> Result<PutOutcome, StorageSyncError> {
    use presage::libsignal_service::push_service::HttpAuthOverride;
    use prost::Message;
    let body = write_operation.encode_to_vec();
    let response = push
        .request(
            reqwest::Method::PUT,
            Endpoint::storage("v1/storage/"),
            HttpAuthOverride::Identified(auth),
        )
        .map_err(|_| StorageSyncError::WriteFailed)?
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?;
    match response.status().as_u16() {
        200 | 204 => Ok(PutOutcome {
            conflict_manifest: None,
        }),
        409 => {
            let bytes = response
                .bytes()
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?;
            let manifest = StorageManifest::decode(bytes.as_ref())
                .map_err(|_| StorageSyncError::WriteFailed)?;
            Ok(PutOutcome {
                conflict_manifest: Some(manifest),
            })
        }
        _ => Err(StorageSyncError::WriteFailed),
    }
}

// ---------------------------------------------------------------------------
// 贴纸记录面的 item 加解密 shim 与批量原样读取（钉定 presage rev 的
// StorageService.proto 无 StickerPack=6，无法经 StorageService pub API 走；
// 派生/AES-GCM 形状与上游 storage_service.rs 私有实现同构，双向交叉验证
// 见 tests::raw_item_crypto_matches_upstream_api。上游 proto 补齐后整段替换）
// ---------------------------------------------------------------------------

/// item 密钥派生（上游 storage_service.rs item_key 同构）：modern =
/// HKDF-SHA256(ikm=record_ikm, info="20240801_SIGNAL_STORAGE_SERVICE_ITEM_"
/// || raw_id)；legacy（record_ikm 空）= HMAC-SHA256(storage_key,
/// "Item_" + base64(raw_id))。
fn storage_item_key(
    storage_key: &StorageServiceKey,
    raw_id: &[u8],
    record_ikm: Option<&[u8]>,
) -> [u8; 32] {
    use hmac::{KeyInit as _, Mac as _};
    const ITEM_KEY_INFO_PREFIX: &[u8] = b"20240801_SIGNAL_STORAGE_SERVICE_ITEM_";
    match record_ikm {
        Some(ikm) if !ikm.is_empty() => {
            let hk = hkdf::Hkdf::<sha2::Sha256>::new(None, ikm);
            let mut okm = [0u8; 32];
            hk.expand_multi_info(&[ITEM_KEY_INFO_PREFIX, raw_id], &mut okm)
                .expect("32-byte HKDF output is valid");
            okm
        }
        _ => {
            let b64 = base64::engine::general_purpose::STANDARD.encode(raw_id);
            let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&storage_key.inner)
                .expect("HMAC accepts any key length");
            mac.update(b"Item_");
            mac.update(b64.as_bytes());
            mac.finalize().into_bytes().into()
        }
    }
}

/// 任意明文 → StorageItem（blob = iv(12B) || ciphertext || tag(16B)，
/// AES-256-GCM 无 AAD，上游 encrypt 同构）。
pub(crate) fn encrypt_raw_item(
    storage_key: &StorageServiceKey,
    raw_id: Vec<u8>,
    plaintext: &[u8],
    record_ikm: Option<&[u8]>,
) -> presage::libsignal_service::proto::StorageItem {
    use aes_gcm::aead::Aead as _;
    use aes_gcm::KeyInit as _;
    use presage::libsignal_service::proto::StorageItem;
    let key = storage_item_key(storage_key, &raw_id, record_ikm);
    let cipher = aes_gcm::Aes256Gcm::new((&key).into());
    // 随机 12B IV（上游 encrypt 同构：OsRng 填充）。
    let mut iv = [0u8; 12];
    rand::rng().fill_bytes(&mut iv);
    let nonce = aes_gcm::Nonce::try_from(&iv[..]).expect("12-byte IV");
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .expect("AES-256-GCM encrypt infallible for valid key");
    let mut value = iv.to_vec();
    value.extend_from_slice(&ciphertext);
    StorageItem { key: raw_id, value }
}

/// StorageItem → 任意明文（MAC 失败/截断 → Err，调用方按面语义处理）。
pub(crate) fn decrypt_raw_item(
    storage_key: &StorageServiceKey,
    item: &presage::libsignal_service::proto::StorageItem,
    record_ikm: Option<&[u8]>,
) -> Result<Vec<u8>, ()> {
    use aes_gcm::aead::Aead as _;
    use aes_gcm::KeyInit as _;
    if item.value.len() <= 12 {
        return Err(());
    }
    let key = storage_item_key(storage_key, &item.key, record_ikm);
    let (iv, ciphertext) = item.value.split_at(12);
    let nonce = aes_gcm::Nonce::try_from(iv).map_err(|_| ())?;
    let cipher = aes_gcm::Aes256Gcm::new((&key).into());
    cipher.decrypt(&nonce, ciphertext).map_err(|_| ())
}

/// 批量原样读（上游 read_items 同端点同请求体，但不解密不投影——钉定
/// presage rev 的 StorageRecord 缺 sticker arm，解出即丢内容；`PUT
/// /v1/storage/read`，items 按服务器返回序，调用方按 key 定位）。
pub(crate) async fn read_raw_items(
    push: &PushService,
    auth: presage::libsignal_service::push_service::HttpAuth,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<presage::libsignal_service::proto::StorageItem>, StorageSyncError> {
    use presage::libsignal_service::proto::{ReadOperation, StorageItems};
    use presage::libsignal_service::push_service::HttpAuthOverride;
    use prost::Message as _;
    let body = ReadOperation { read_key: keys };
    let mut buf = Vec::with_capacity(body.encoded_len());
    body.encode(&mut buf).expect("infallible encode into Vec");
    let response = push
        .request(
            reqwest::Method::PUT,
            Endpoint::storage("/v1/storage/read"),
            HttpAuthOverride::Identified(auth),
        )
        .map_err(|_| StorageSyncError::ReadFailed)?
        .header("Content-Type", "application/x-protobuf")
        .body(buf)
        .send()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?;
    if !response.status().is_success() {
        return Err(StorageSyncError::ReadFailed);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?;
    let items = StorageItems::decode(bytes.as_ref()).map_err(|_| StorageSyncError::ReadFailed)?;
    Ok(items.items)
}

// ---------------------------------------------------------------------------
// 读/写面编排（Service 调用入口；网络面，不做网络测试）
// ---------------------------------------------------------------------------

/// Storage Service 句柄（上游 pub API；auth 换取在 `StorageService::new`
/// 内部完成，与上游 storage_service.rs:103-128 同一模式）。
pub(crate) async fn open_storage_service(
    push: PushService,
    storage_key: StorageServiceKey,
) -> Result<StorageService, StorageSyncError> {
    StorageService::new(push, storage_key)
        .await
        .map_err(|_| StorageSyncError::Unavailable)
}

/// Account item 原样读取（`PUT /v1/storage/read`，storage_auth 显式换取）
/// → 本地 shim 解密 → 官方 vendored proto 解码（字段保真读面；不经 presage
/// `read_items`——其解出类型缺 AccountRecord 52-60 字段面，读即丢失）。
/// manifest 无 Account 标识或服务器缺 item → None；解密/解码失败 →
/// ReadFailed。
pub(crate) async fn read_account_record(
    push: &PushService,
    storage_key: &StorageServiceKey,
    manifest: &ManifestRecord,
) -> Result<Option<AccountRecord>, StorageSyncError> {
    let Some(key) = account_item_key_of(manifest) else {
        return Ok(None);
    };
    let ikm = manifest.record_ikm.as_slice();
    let record_ikm = if ikm.is_empty() { None } else { Some(ikm) };
    let auth = storage_auth(push).await?;
    let items = read_raw_items(push, auth, vec![key]).await?;
    let Some(item) = items.into_iter().next() else {
        return Ok(None);
    };
    let plaintext = decrypt_raw_item(storage_key, &item, record_ikm)
        .map_err(|_| StorageSyncError::ReadFailed)?;
    Ok(account_record_of_wire(&plaintext))
}

/// 本账号 AccountRecord 只读面（getUsername 等账号字段读面的真相源入口）：
/// master key → manifest → Account item → vendored proto 解码。
/// 从未有 storage 状态（无 master key / manifest 404 / manifest 无
/// Account key / item 缺席）→ None（调用方按「未设」投影，不报错）。
pub(crate) async fn read_own_account_record(
    context: &StorageContext,
) -> Result<Option<AccountRecord>, StorageSyncError> {
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok(None);
    };
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => {
            return Ok(None);
        }
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    read_account_record(
        &context.push,
        &StorageServiceKey::from_master_key(&master_key),
        &manifest,
    )
    .await
}

/// 读面编排：master key → manifest → Account item → 引擎会话标识投影
/// （存储序）。master key 缺失或 manifest 404（账号从未有过 storage 状态）
/// → 空列表。
pub(crate) async fn read_pinned(
    context: &StorageContext,
) -> Result<Vec<PinnedEntry>, StorageSyncError> {
    // master key 缺失 = 从未有过 storage 状态（读面语义：无远程置顶）。
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok(Vec::new());
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => {
            return Ok(Vec::new());
        }
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    let account = match read_account_record(&context.push, &storage_key, &manifest).await? {
        Some(account) => account,
        None => return Ok(Vec::new()),
    };
    Ok(project_pinned(&account))
}

fn project_pinned(account: &AccountRecord) -> Vec<PinnedEntry> {
    account
        .pinned_conversations
        .iter()
        .map(|raw| PinnedEntry {
            id: storage_to_engine(raw),
            raw: raw.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 拉黑只读面（P5 扩面）
// ---------------------------------------------------------------------------

/// ContactRecord → 引擎会话标识（口径与 `storage_to_engine` 的 contact 分支
/// 对齐：aciBinary 16 字节 → UUID 字符串优先，字符串形态直通，e164 形态非
/// 本引擎 contact 标识口径 → 跳过）。
fn contact_engine_id(contact: &storage_proto::ContactRecord) -> Option<EngineConversationId> {
    if !contact.aci_binary.is_empty() {
        let uuid =
            presage::libsignal_service::prelude::Uuid::from_slice(&contact.aci_binary).ok()?;
        return Some(EngineConversationId::Contact(uuid.to_string()));
    }
    if contact.aci.is_empty() {
        return None;
    }
    presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(&contact.aci)
        .map(|sid| EngineConversationId::Contact(sid.service_id_string()))
}

/// 单条明文 StorageRecord → 引擎会话标识（blocked 面投影）：仅
/// Contact/GroupV1/GroupV2 三类参与，`blocked==false`、非三类记录或标识
/// 映射不出 → None（调用方跳过，零 stderr——daemon 静默纪律，compat §1）。
pub(crate) fn blocked_conversation_of(plaintext: &[u8]) -> Option<EngineConversationId> {
    let record = storage_proto::StorageRecord::decode(plaintext)
        .ok()?
        .record?;
    match record {
        storage_proto::storage_record::Record::Contact(contact) if contact.blocked => {
            contact_engine_id(&contact)
        }
        storage_proto::storage_record::Record::GroupV1(group)
            if group.blocked && !group.id.is_empty() =>
        {
            Some(EngineConversationId::LegacyGroup(
                String::from_utf8_lossy(&group.id).into_owned(),
            ))
        }
        storage_proto::storage_record::Record::GroupV2(group)
            if group.blocked && !group.master_key.is_empty() =>
        {
            Some(EngineConversationId::Group(
                base64::engine::general_purpose::STANDARD.encode(&group.master_key),
            ))
        }
        _ => None,
    }
}

/// manifest 全部 Contact/GroupV1/GroupV2 identifier → 批量原样读 → 解密 →
/// vendored proto 解码 → blocked 位过滤（存储序 = manifest identifiers 首
/// 现序；单条坏 item 跳过不炸面，与贴纸批量读同纪律）。
async fn read_blocked_ids(
    push: &PushService,
    storage_key: &StorageServiceKey,
    manifest: &ManifestRecord,
) -> Result<Vec<EngineConversationId>, StorageSyncError> {
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| {
            matches!(
                identifier.r#type(),
                manifest_record::identifier::Type::Contact
                    | manifest_record::identifier::Type::Groupv1
                    | manifest_record::identifier::Type::Groupv2
            )
        })
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
        if let Some(id) = blocked_conversation_of(&plaintext) {
            out.push(id);
        }
    }
    Ok(out)
}

/// 读面编排：master key → manifest → 全部 blocked 会话（存储序）。master
/// key 缺失或 manifest 404（账号从未有过 storage 状态）→ 空列表。v1 投影
/// 不含 blockedAtTimestamp（官方 Contact/GroupV2 记录有该字段，投影面 v1
/// 裁剪——compat §5 登记）。
pub(crate) async fn read_blocked_conversations(
    context: &StorageContext,
) -> Result<Vec<EngineConversationId>, StorageSyncError> {
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok(Vec::new());
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => {
            return Ok(Vec::new());
        }
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    read_blocked_ids(&context.push, &storage_key, &manifest).await
}

// ---------------------------------------------------------------------------
// 拉黑写面（setConversationBlocked，官方 storageRecordOps 行为对齐不抄码：
// 逐会话记录 blocked 位 + blockedAt；解除 = blocked=false；纯拉黑不删会话；
// 多设备/手机经 storage sync 收敛，无 SyncMessage.Blocked 通道——compat §5）
// ---------------------------------------------------------------------------

/// 会话明文记录 → 引擎会话标识（不看 blocked 位；Contact/GV1/GV2 三类参
/// 与，写面定位目标用；标识口径与 `blocked_conversation_of` 读投影同构）。
fn conversation_record_of(plaintext: &[u8]) -> Option<EngineConversationId> {
    let record = storage_proto::StorageRecord::decode(plaintext)
        .ok()?
        .record?;
    match record {
        storage_proto::storage_record::Record::Contact(contact) => contact_engine_id(&contact),
        storage_proto::storage_record::Record::GroupV1(group) if !group.id.is_empty() => Some(
            EngineConversationId::LegacyGroup(String::from_utf8_lossy(&group.id).into_owned()),
        ),
        storage_proto::storage_record::Record::GroupV2(group) if !group.master_key.is_empty() => {
            Some(EngineConversationId::Group(
                base64::engine::general_purpose::STANDARD.encode(&group.master_key),
            ))
        }
        _ => None,
    }
}

/// 单条明文记录拉黑位翻面（decode→mutate 只动 blocked 面，encode 保真其余
/// 字段）：置 blocked=true 时写 blockedAtTimestamp=now（首次拉黑时刻；已拉
/// 黑不重写=保住他端时刻，含 0=未知原样保留），解除清零（blocked=false +
/// blockedAt=0）。GV1 无时间戳字段（官方 proto 同构）。意图已满足 →
/// None（幂等无写）。解码失败 → ReadFailed（目标在云上但读不出）。
fn flip_blocked(
    plaintext: &[u8],
    blocked: bool,
    now_ms: u64,
) -> Result<Option<(StorageRecordKind, RecordPayload)>, StorageSyncError> {
    use storage_proto::storage_record::Record as WireRecord;
    let record = storage_proto::StorageRecord::decode(plaintext)
        .ok()
        .and_then(|decoded| decoded.record)
        .ok_or(StorageSyncError::ReadFailed)?;
    Ok(match record {
        WireRecord::Contact(contact) => contact_with_blocked(&contact, blocked, now_ms)
            .map(|updated| (StorageRecordKind::Contact, RecordPayload::Contact(updated))),
        WireRecord::GroupV1(group) => group_v1_with_blocked(&group, blocked)
            .map(|updated| (StorageRecordKind::GroupV1, RecordPayload::GroupV1(updated))),
        WireRecord::GroupV2(group) => group_v2_with_blocked(&group, blocked, now_ms)
            .map(|updated| (StorageRecordKind::GroupV2, RecordPayload::GroupV2(updated))),
        _ => return Err(StorageSyncError::ReadFailed),
    })
}

/// ContactRecord 拉黑位翻面：返回 None = 意图已满足（幂等无写）。
fn contact_with_blocked(
    record: &storage_proto::ContactRecord,
    blocked: bool,
    now_ms: u64,
) -> Option<storage_proto::ContactRecord> {
    if blocked && record.blocked {
        return None;
    }
    if !blocked && !record.blocked && record.blocked_at_timestamp == 0 {
        return None;
    }
    let mut updated = record.clone();
    updated.blocked = blocked;
    updated.blocked_at_timestamp = match blocked {
        // 已拉黑保留原时刻（不重写他端时钟）；首次拉黑 = 引擎时钟。
        true if record.blocked => record.blocked_at_timestamp,
        true => now_ms,
        false => 0,
    };
    Some(updated)
}

/// GroupV1Record 拉黑位翻面（无 blockedAtTimestamp 字段）。
fn group_v1_with_blocked(
    record: &storage_proto::GroupV1Record,
    blocked: bool,
) -> Option<storage_proto::GroupV1Record> {
    (record.blocked != blocked).then(|| {
        let mut updated = record.clone();
        updated.blocked = blocked;
        updated
    })
}

/// GroupV2Record 拉黑位翻面（blockedAtTimestamp 语义同 ContactRecord）。
fn group_v2_with_blocked(
    record: &storage_proto::GroupV2Record,
    blocked: bool,
    now_ms: u64,
) -> Option<storage_proto::GroupV2Record> {
    if blocked && record.blocked {
        return None;
    }
    if !blocked && !record.blocked && record.blocked_at_timestamp == 0 {
        return None;
    }
    let mut updated = record.clone();
    updated.blocked = blocked;
    updated.blocked_at_timestamp = match blocked {
        true if record.blocked => record.blocked_at_timestamp,
        true => now_ms,
        false => 0,
    };
    Some(updated)
}

/// 云上无目标记录时的新建面（官方 toContactRecord/toGroupVxRecord 最小同
/// 构：标识 + blocked 面，e164 号码命中面可得则写）：blocked=false = 无可
/// 解除 → None（幂等无写）；contact 解析失败 → ConversationNotResolved。
async fn fresh_blocked_record(
    context: &StorageContext,
    target: &EngineConversationId,
    canonical: &EngineConversationId,
    blocked: bool,
    now_ms: u64,
) -> Result<Option<(StorageRecordKind, RecordPayload)>, StorageSyncError> {
    if !blocked {
        return Ok(None);
    }
    Ok(Some(match target {
        EngineConversationId::Contact(_) => {
            let (service_id, e164) = resolve_contact_identity(context, target)
                .await
                .ok_or(StorageSyncError::ConversationNotResolved)?;
            (
                StorageRecordKind::Contact,
                RecordPayload::Contact(storage_proto::ContactRecord {
                    aci: service_id.service_id_string(),
                    aci_binary: service_id.service_id_binary(),
                    e164: e164.unwrap_or_default(),
                    blocked: true,
                    blocked_at_timestamp: now_ms,
                    ..storage_proto::ContactRecord::default()
                }),
            )
        }
        EngineConversationId::Group(_) => (
            StorageRecordKind::GroupV2,
            RecordPayload::GroupV2(storage_proto::GroupV2Record {
                master_key: engine_group_to_master_key(canonical)?,
                blocked: true,
                blocked_at_timestamp: now_ms,
                ..storage_proto::GroupV2Record::default()
            }),
        ),
        EngineConversationId::LegacyGroup(_) => (
            StorageRecordKind::GroupV1,
            RecordPayload::GroupV1(storage_proto::GroupV1Record {
                id: engine_legacy_to_bytes(canonical)?,
                blocked: true,
                ..storage_proto::GroupV1Record::default()
            }),
        ),
    }))
}

/// 写面目标解析：引擎会话标识 → 云上记录口径的规范标识（contact → ACI
/// uuid 串；group → master key 标准 base64；legacy → id bytes 文本）。解析
/// 失败 fail-closed（不发请求）。
async fn blocked_target_canonical(
    context: &StorageContext,
    target: &EngineConversationId,
) -> Result<EngineConversationId, StorageSyncError> {
    match target {
        EngineConversationId::Contact(_) => {
            let (service_id, _) = resolve_contact_identity(context, target)
                .await
                .ok_or(StorageSyncError::ConversationNotResolved)?;
            Ok(EngineConversationId::Contact(
                service_id.service_id_string(),
            ))
        }
        EngineConversationId::Group(_) => Ok(EngineConversationId::Group(
            base64::engine::general_purpose::STANDARD.encode(engine_group_to_master_key(target)?),
        )),
        EngineConversationId::LegacyGroup(_) => Ok(EngineConversationId::LegacyGroup(
            String::from_utf8_lossy(&engine_legacy_to_bytes(target)?).into_owned(),
        )),
    }
}

/// 批量原样读全部 Contact/GV1/GV2 item（写面定位/重放用；manifest 404 →
/// 空；坏条目跳过不炸面，读面同纪律）。
struct ConversationItem {
    key: Vec<u8>,
    plaintext: Vec<u8>,
}

async fn read_conversation_items(
    push: &PushService,
    storage_key: &StorageServiceKey,
    manifest: Option<&ManifestRecord>,
) -> Result<Vec<ConversationItem>, StorageSyncError> {
    let Some(manifest) = manifest else {
        return Ok(Vec::new());
    };
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| {
            matches!(
                identifier.r#type(),
                manifest_record::identifier::Type::Contact
                    | manifest_record::identifier::Type::Groupv1
                    | manifest_record::identifier::Type::Groupv2
            )
        })
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
        out.push(ConversationItem { key, plaintext });
    }
    Ok(out)
}

/// blocked 名单投影（明文批次 → 读面同构投影）。
fn project_blocked(items: &[ConversationItem]) -> Vec<EngineConversationId> {
    items
        .iter()
        .filter_map(|item| blocked_conversation_of(&item.plaintext))
        .collect()
}

/// 写后 blocked 名单投影（官方 upload 成功语义 = 本地写后面为准）：被换写
/// 的目标条目按写后状态计入，其余条目沿用本轮读取态（存储序）。
fn project_blocked_after(
    items: &[ConversationItem],
    replaced_key: Option<&[u8]>,
    canonical: &EngineConversationId,
    target_blocked: bool,
) -> Vec<EngineConversationId> {
    let mut out: Vec<EngineConversationId> = items
        .iter()
        .filter(|item| replaced_key.is_none_or(|key| item.key != key))
        .filter_map(|item| blocked_conversation_of(&item.plaintext))
        .collect();
    if target_blocked && !out.contains(canonical) {
        out.push(canonical.clone());
    }
    out
}

/// 拉黑写面编排（官方 upload 语义）：本地意图解析（fail-closed，不发请求）
/// → 逐 item 读改写（decode→mutate→encode 保真，仅动 blocked 面）→ 新 16B
/// item id 重加密 → WriteOperation version+1 → `PUT v1/storage/` → 409 采纳
/// 重放（conflictBackOff 3 次）。manifest 404 + blocked=true → master key
/// 播种 + version 1 最小 AccountRecord 与目标记录同批首写（compat §5 分支）；
/// manifest 404 + blocked=false = 无可解除，幂等成功不播种。同值幂等无写。
pub(crate) async fn write_blocked(
    context: &StorageContext,
    target: &EngineConversationId,
    blocked: bool,
    now_ms: u64,
) -> Result<Vec<EngineConversationId>, StorageSyncError> {
    let canonical = blocked_target_canonical(context, target).await?;
    let master_key = match context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?
    {
        Some(key) => key,
        None => {
            if !blocked {
                // 无 storage 状态且要解除：云上无任何记录，无可解除（诚实
                // 幂等，不播种不建 manifest）。
                return Ok(Vec::new());
            }
            let generated = MasterKey::generate(&mut rand::rng());
            context
                .store
                .store_master_key(Some(&generated))
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?;
            generated
        }
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let mut manifest: Option<ManifestRecord> = match storage.manifest().await {
        Ok(record) => {
            // 有 manifest 无 Account 标识 = 非法状态（置顶/贴纸面同口径）。
            if account_item_key_of(&record).is_none() {
                return Err(StorageSyncError::WriteFailed);
            }
            Some(record)
        }
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => None,
        Err(_) => return Err(StorageSyncError::Unavailable),
    };

    // 冲突重放循环：每轮从「当前服务器状态 + 本地意图」重建 WriteOperation。
    let mut retries: u32 = 0;
    loop {
        let items = read_conversation_items(&context.push, &storage_key, manifest.as_ref()).await?;
        let found = items
            .iter()
            .find(|item| conversation_record_of(&item.plaintext).as_ref() == Some(&canonical));
        let mutation = match found {
            Some(item) => flip_blocked(&item.plaintext, blocked, now_ms)?,
            None => fresh_blocked_record(context, target, &canonical, blocked, now_ms).await?,
        };
        let Some((kind, payload)) = mutation else {
            // 幂等（同值）/ 无可解除：返回当前 blocked 名单（存储序）。
            return Ok(project_blocked(&items));
        };
        let replaced_key = found.map(|item| item.key.clone());

        // 首写分支播种 Account（仅 manifest 404）；其余写不动 Account item。
        let mut upserts: Vec<RecordUpsert> = Vec::with_capacity(2);
        let mut seeded_account_key: Option<Vec<u8>> = None;
        let previous = match &manifest {
            Some(record) => Some(record),
            None => {
                let key = new_item_key();
                seeded_account_key = Some(key.clone());
                upserts.push(RecordUpsert {
                    kind: StorageRecordKind::Account,
                    new_key: key,
                    replaced_key: None,
                    payload: Some(RecordPayload::Account(minimal_account_record(Vec::new()))),
                });
                None
            }
        };
        let new_key = new_item_key();
        upserts.push(RecordUpsert {
            kind,
            new_key: new_key.clone(),
            replaced_key: replaced_key.clone(),
            payload: Some(payload),
        });
        let planned = plan_record_writes(previous, &upserts, context.device_id, &storage_key);

        let auth = storage_auth(&context.push).await?;
        match put_write_operation(&context.push, auth, &planned.write_operation).await? {
            PutOutcome {
                conflict_manifest: None,
            } => {
                let account_key =
                    seeded_account_key.or_else(|| manifest.as_ref().and_then(account_item_key_of));
                save_manifest_state(
                    &context.account_dir,
                    &ManifestState {
                        version: planned.new_version,
                        account_item_key: account_key
                            .map(|key| base64::engine::general_purpose::STANDARD.encode(key)),
                        // 拉黑面不感知贴纸面：沿用状态文件旧值。
                        sticker_item_keys: None,
                    },
                );
                return Ok(project_blocked_after(
                    &items,
                    replaced_key.as_deref(),
                    &canonical,
                    blocked,
                ));
            }
            PutOutcome {
                conflict_manifest: Some(server_manifest),
            } => match conflict_decision(retries) {
                ConflictDecision::GiveUp => return Err(StorageSyncError::WriteFailed),
                ConflictDecision::Retry { delay_ms } => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    retries += 1;
                    // 采纳服务器 manifest（官方 409 语义）；下轮整体重读重放
                    //（读失败归 WriteFailed：写意图未落地）。
                    let decrypted =
                        StorageService::decrypt_manifest(&storage_key, &server_manifest)
                            .map_err(|_| StorageSyncError::WriteFailed)?;
                    if account_item_key_of(&decrypted).is_none() {
                        return Err(StorageSyncError::WriteFailed);
                    }
                    manifest = Some(decrypted);
                }
            },
        }
    }
}

/// 账号记录写编排通用骨架（write_pinned 收敛出的共用面，账号级设置批
/// setAccountConfiguration / setUniversalExpireTimer 复用）：seed master
/// key → manifest 读取（404 → 最小 AccountRecord 首写分支）→ mutate 回调
/// （None = 意图已满足，幂等无写直接返回现状）→ plan_write → PUT → 409
/// 采纳重放（conflictBackOff 3 次）。返回写后（或未变的）AccountRecord。
pub(crate) async fn update_account_record(
    context: &StorageContext,
    mutate: impl Fn(&AccountRecord) -> Option<AccountRecord>,
) -> Result<AccountRecord, StorageSyncError> {
    // master key 缺失 = 从未有过 storage 状态（compat §5 首写分支前半）：
    // 生成并落库，随后以 version 1 + 最小 AccountRecord 建初始 manifest。
    // 手机侧并发首建的竞争由 409 采纳重放兜底（服务器 manifest 为准）。
    let master_key = match context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?
    {
        Some(key) => key,
        None => {
            let generated = MasterKey::generate(&mut rand::rng());
            context
                .store
                .store_master_key(Some(&generated))
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?;
            generated
        }
    };
    // StorageService::new 按值收 key：句柄持有一份，plan/decrypt 复用另一份
    // （from_master_key 确定性派生，同 master key 两次派生等价）。
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;

    // manifest 404（从未有过 storage 状态）：以 version 1 + 最小
    // AccountRecord 建初始 manifest（compat §5 登记分支）。Account item
    // 读面走官方 vendored proto（read_account_record，字段保真）；读失败
    // 归 WriteFailed（写意图未落地）。
    let (manifest, account) = match storage.manifest().await {
        Ok(manifest) => {
            if account_item_key_of(&manifest).is_none() {
                return Err(StorageSyncError::WriteFailed);
            }
            let account = read_account_record(&context.push, &storage_key, &manifest)
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?
                .ok_or(StorageSyncError::WriteFailed)?;
            Ok((Some(manifest), account))
        }
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => {
            Ok((None, minimal_account_record(Vec::new())))
        }
        Err(_) => Err(StorageSyncError::Unavailable),
    }?;

    // 冲突重放循环：每轮从「当前服务器状态 + 本地意图」重建 WriteOperation；
    // adopted = 409 采纳的服务器 (manifest, Account item)。
    let mut retries: u32 = 0;
    let mut current = (manifest, account);
    loop {
        let (current_manifest, current_account) = (&current.0, &current.1);
        let Some(updated) = mutate(current_account) else {
            // 意图已满足（幂等）：无写面，返回现状。
            return Ok(current_account.clone());
        };

        let planned = plan_write(
            current_manifest
                .as_ref()
                .map(|manifest| (manifest, account_item_key_of(manifest).unwrap_or_default())),
            &updated,
            context.device_id,
            new_item_key(),
            &storage_key,
        );

        let auth = storage_auth(&context.push).await?;
        match put_write_operation(&context.push, auth, &planned.write_operation).await? {
            PutOutcome {
                conflict_manifest: None,
            } => {
                save_manifest_state(
                    &context.account_dir,
                    &ManifestState {
                        version: planned.new_version,
                        account_item_key: Some(
                            base64::engine::general_purpose::STANDARD
                                .encode(&planned.new_account_key),
                        ),
                        // 置顶/设置面不感知贴纸面：沿用状态文件旧值。
                        sticker_item_keys: None,
                    },
                );
                // 写成功后以本地写后面为准返回（官方 upload 成功语义）。
                return Ok(updated);
            }
            PutOutcome {
                conflict_manifest: Some(server_manifest),
            } => match conflict_decision(retries) {
                ConflictDecision::GiveUp => return Err(StorageSyncError::WriteFailed),
                ConflictDecision::Retry { delay_ms } => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    retries += 1;
                    // 采纳服务器 manifest：重读 Account item（官方 409 语义
                    // ——响应体 manifest 为准，item 内容重拉后重放本地意图；
                    // 任何读取失败归 WriteFailed：写意图未落地）。
                    let decrypted =
                        StorageService::decrypt_manifest(&storage_key, &server_manifest)
                            .map_err(|_| StorageSyncError::WriteFailed)?;
                    if account_item_key_of(&decrypted).is_none() {
                        return Err(StorageSyncError::WriteFailed);
                    }
                    let server_account =
                        read_account_record(&context.push, &storage_key, &decrypted)
                            .await
                            .map_err(|_| StorageSyncError::WriteFailed)?
                            .ok_or(StorageSyncError::WriteFailed)?;
                    current = (Some(decrypted), server_account);
                }
            },
        }
    }
}

/// 写面编排（官方 upload 语义）：本地意图解析（fail-closed，不发请求）→
/// update_account_record 通用骨架（原地改 pinned → encrypt 新 id → PUT →
/// 409 采纳重放）→ 引擎会话标识投影。
pub(crate) async fn write_pinned(
    context: &StorageContext,
    target: EngineConversationId,
    pinned: bool,
) -> Result<Vec<PinnedEntry>, StorageSyncError> {
    // 本地意图先解析（解析失败 fail-closed，不发请求）：
    // contact → serviceId（ACI 优先，本机号走注册数据）；group → master key。
    let new_pinned_proto: PinnedConversation = match &target {
        EngineConversationId::Contact(_) => {
            let service_id = resolve_contact_service_id(context, &target)
                .await
                .ok_or(StorageSyncError::ConversationNotResolved)?;
            engine_to_storage(&target, Some(&service_id))?
        }
        EngineConversationId::Group(_) => PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::GroupMasterKey(
                    engine_group_to_master_key(&target)?,
                ),
            ),
        },
        EngineConversationId::LegacyGroup(_) => PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::LegacyGroupId(
                    engine_legacy_to_bytes(&target)?,
                ),
            ),
        },
    };
    let updated = update_account_record(context, |account| {
        let updated_list = if pinned {
            pinned_add_stored(&account.pinned_conversations, &new_pinned_proto)
        } else {
            pinned_remove_stored(&account.pinned_conversations, &new_pinned_proto)
        };
        let unchanged = updated_list.len() == account.pinned_conversations.len()
            && updated_list
                .iter()
                .zip(account.pinned_conversations.iter())
                .all(|(left, right)| same_storage_identity(left, right));
        (!unchanged).then(|| account_record_with_pinned(account, updated_list))
    })
    .await?;
    Ok(project_pinned(&updated))
}

/// API 字符串 → 官方 PhoneNumberSharingMode 数值（setAccountConfiguration
/// 参数面）：everybody=1 / nobody=2（官方 Android/桌面现行枚举一致，桌面
/// v8.31 同值；旧 CONTACTS_ONLY 已从官方 proto 移除，不收）。
pub(crate) fn phone_number_sharing_mode_of(text: &str) -> Option<i32> {
    match text {
        "everybody" => Some(account_record::PhoneNumberSharingMode::Everybody as i32),
        "nobody" => Some(account_record::PhoneNumberSharingMode::Nobody as i32),
        _ => None,
    }
}

/// 账号级设置写意图（setAccountConfiguration）：只写提供的字段。
#[derive(Debug, Clone, Default)]
pub(crate) struct AccountConfigurationUpdate {
    pub(crate) read_receipts: Option<bool>,
    pub(crate) typing_indicators: Option<bool>,
    /// PhoneNumberSharingMode 数值（`phone_number_sharing_mode_of` 出）。
    pub(crate) phone_number_sharing_mode: Option<i32>,
    pub(crate) unlisted_phone_number: Option<bool>,
}

/// AccountRecord 读-改-写保真核心（设置面）：只覆写提供的字段，其余字段
/// 原样（字段保真铁面同款 decode→mutate→encode）；全部提供字段已满足 →
/// None（幂等无写）。
pub(crate) fn account_record_with_configuration(
    record: &AccountRecord,
    update: &AccountConfigurationUpdate,
) -> Option<AccountRecord> {
    let AccountConfigurationUpdate {
        read_receipts,
        typing_indicators,
        phone_number_sharing_mode,
        unlisted_phone_number,
    } = update;
    let mut updated = record.clone();
    let mut changed = false;
    if let Some(value) = read_receipts {
        changed |= updated.read_receipts != *value;
        updated.read_receipts = *value;
    }
    if let Some(value) = typing_indicators {
        changed |= updated.typing_indicators != *value;
        updated.typing_indicators = *value;
    }
    if let Some(value) = phone_number_sharing_mode {
        changed |= updated.phone_number_sharing_mode != *value;
        updated.phone_number_sharing_mode = *value;
    }
    if let Some(value) = unlisted_phone_number {
        changed |= updated.unlisted_phone_number != *value;
        updated.unlisted_phone_number = *value;
    }
    changed.then_some(updated)
}

/// AccountRecord 读-改-写保真核心（个人资料面，setProfile）：只覆写名字
/// 与头像 path（avatar_url_path=None = 保留现状），其余字段原样。
pub(crate) fn account_record_with_profile(
    record: &AccountRecord,
    given_name: &str,
    family_name: Option<&str>,
    avatar_url_path: Option<&str>,
) -> AccountRecord {
    let mut updated = record.clone();
    updated.given_name = given_name.to_owned();
    updated.family_name = family_name.unwrap_or_default().to_owned();
    if let Some(path) = avatar_url_path {
        updated.avatar_url_path = path.to_owned();
    }
    updated
}

/// AccountRecord 读-改-写保真核心（universal timer 面，setUniversalExpireTimer）：
/// 只覆写 universalExpireTimer(17)；同值 → None（幂等无写）。
pub(crate) fn account_record_with_universal_expire_timer(
    record: &AccountRecord,
    expiration_seconds: u32,
) -> Option<AccountRecord> {
    (record.universal_expire_timer != expiration_seconds).then(|| {
        let mut updated = record.clone();
        updated.universal_expire_timer = expiration_seconds;
        updated
    })
}

/// AccountRecord 读-改-写保真核心（用户名写面）：username(33) +
/// usernameLink(35) 写入。`username=Some("")` = 清除（usernameDelete）；
/// link material = (entropy 32B, server_id 16B)，`None` = 清链接。
/// 意图已满足（33/35 均无变化）→ None（幂等无写）。
pub(crate) fn account_record_with_username(
    record: &AccountRecord,
    username: Option<&str>,
    link: Option<([u8; 32], [u8; 16])>,
) -> Option<AccountRecord> {
    let mut updated = record.clone();
    if let Some(username) = username {
        updated.username = username.to_owned();
    }
    updated.username_link = link.map(|(entropy, server_id)| account_record::UsernameLink {
        entropy: entropy.to_vec(),
        server_id: server_id.to_vec(),
        color: account_record::username_link::Color::Unknown as i32,
    });
    let changed =
        updated.username != record.username || updated.username_link != record.username_link;
    changed.then_some(updated)
}

/// universal timer 写面（setUniversalExpireTimer，官方 universalTimer 等价）：
/// 账户级默认消失消息计时器 → AccountRecord.universalExpireTimer(17)（
/// 0=关闭），update_account_record 骨架（409 采纳重放；同值幂等无写）。
/// 账户级默认值无广播（会话级计时器才广播 expiration-update），其他设备
/// 经 storage sync 收敛。
pub(crate) async fn write_universal_expire_timer(
    context: &StorageContext,
    expiration_seconds: u32,
) -> Result<(), StorageSyncError> {
    update_account_record(context, |account| {
        account_record_with_universal_expire_timer(account, expiration_seconds)
    })
    .await
    .map(|_| ())
}

/// 账号级设置写面（setAccountConfiguration）：update_account_record 通用
/// 骨架 + 设置面 mutate。官方 updateConfiguration 等价——真相源 =
/// AccountRecord 的 readReceipts(6)/typingIndicators(8)/
/// phoneNumberSharingMode(12)/unlistedPhoneNumber(13)，读改写仅动提供的
/// 字段，其余账号设置原样（字段保真铁面同款链路）。
pub(crate) async fn write_account_configuration(
    context: &StorageContext,
    update: AccountConfigurationUpdate,
) -> Result<(), StorageSyncError> {
    update_account_record(context, |account| {
        account_record_with_configuration(account, &update)
    })
    .await
    .map(|_| ())
}

/// contact 会话 → (ServiceId, e164?)（ACI 优先）：UUID 形状直通 → 本机号
/// 走注册数据 → contacts 数字位匹配（listContacts 同源数据）。e164 在号码
/// 命中面可得（拉黑写面新建 ContactRecord 的 e164 字段用；服务 id 命中面
/// 引擎不反查号码，None 诚实缺席）。
pub(crate) async fn resolve_contact_identity(
    context: &StorageContext,
    target: &EngineConversationId,
) -> Option<(
    presage::libsignal_service::protocol::ServiceId,
    Option<String>,
)> {
    let EngineConversationId::Contact(text) = target else {
        return None;
    };
    if let Some(service_id) =
        presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(text)
    {
        return Some((service_id, None));
    }
    let want: String = text.chars().filter(char::is_ascii_digit).collect();
    if want.is_empty() {
        return None;
    }
    // 本机号：置顶/拉黑自己（Note to Self）。
    if let Ok(Some(data)) = context.store.load_registration_data().await {
        let own_e164 = crate::service::e164_string(&data.phone_number);
        let own_digits: String = own_e164.chars().filter(char::is_ascii_digit).collect();
        if own_digits == want {
            return Some((
                presage::libsignal_service::protocol::ServiceId::Aci(data.service_ids.aci.into()),
                Some(own_e164),
            ));
        }
    }
    let contacts = context.store.contacts().await.ok()?;
    let digits_of = |phone: String| {
        phone
            .chars()
            .filter(char::is_ascii_digit)
            .collect::<String>()
    };
    contacts.filter_map(Result::ok).find_map(|contact| {
        let e164 = contact
            .phone_number
            .as_ref()
            .map(crate::service::e164_string);
        let matches = e164
            .as_ref()
            .map(|phone| digits_of(phone.clone()))
            .is_some_and(|digits| digits == want);
        matches.then_some((
            presage::libsignal_service::protocol::ServiceId::Aci(contact.uuid.into()),
            e164,
        ))
    })
}

/// contact 会话 → serviceId 字符串（ACI 优先）：置顶面入口（
/// `resolve_contact_identity` 薄壳，行为不变）。
async fn resolve_contact_service_id(
    context: &StorageContext,
    target: &EngineConversationId,
) -> Option<String> {
    let (service_id, _) = resolve_contact_identity(context, target).await?;
    Some(service_id.service_id_string())
}

/// stored 面增删（保序）：与引擎面纯函数同语义，但保留非引擎口径条目
/// （ReleaseNotes 等）原样。
fn pinned_add_stored(
    list: &[PinnedConversation],
    target: &PinnedConversation,
) -> Vec<PinnedConversation> {
    let mut out = list.to_vec();
    if !list.iter().any(|item| same_storage_identity(item, target)) {
        out.push(target.clone());
    }
    out
}

fn pinned_remove_stored(
    list: &[PinnedConversation],
    target: &PinnedConversation,
) -> Vec<PinnedConversation> {
    list.iter()
        .filter(|item| !same_storage_identity(item, target))
        .cloned()
        .collect()
}

/// 存储身份等价判定：同类 oneof 且标识等价（contact 按 serviceIdBinary/
/// serviceId 任一命中；group/legacy 按 bytes）。
fn same_storage_identity(a: &PinnedConversation, b: &PinnedConversation) -> bool {
    use account_record::pinned_conversation::Identifier;
    match (a.identifier.as_ref(), b.identifier.as_ref()) {
        (Some(Identifier::Contact(left)), Some(Identifier::Contact(right))) => {
            let same_binary = !left.service_id_binary.is_empty()
                && left.service_id_binary == right.service_id_binary;
            let same_string = !left.service_id.is_empty() && left.service_id == right.service_id;
            same_binary || same_string
        }
        (Some(Identifier::GroupMasterKey(left)), Some(Identifier::GroupMasterKey(right))) => {
            left == right
        }
        (Some(Identifier::LegacyGroupId(left)), Some(Identifier::LegacyGroupId(right))) => {
            left == right
        }
        (Some(Identifier::ReleaseNotes(_)), Some(Identifier::ReleaseNotes(_))) => true,
        _ => false,
    }
}

/// `GET /v1/storage/auth` 换 short-lived 基本认证（上游
/// StorageService::new 同款模式；写面 PUT 与贴纸面批量读需显式 auth 透传）。
pub(crate) async fn storage_auth(
    push: &PushService,
) -> Result<presage::libsignal_service::push_service::HttpAuth, StorageSyncError> {
    use presage::libsignal_service::push_service::HttpAuthOverride;
    #[derive(serde::Deserialize)]
    struct StorageAuthResponse {
        username: String,
        password: String,
    }
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service("/v1/storage/auth"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| StorageSyncError::Unavailable)?
        .send()
        .await
        .map_err(|_| StorageSyncError::Unavailable)?;
    if !response.status().is_success() {
        return Err(StorageSyncError::Unavailable);
    }
    let auth: StorageAuthResponse = response
        .json()
        .await
        .map_err(|_| StorageSyncError::Unavailable)?;
    Ok(presage::libsignal_service::push_service::HttpAuth {
        username: auth.username,
        password: auth.password,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn contact_pinned(uuid: &str) -> PinnedConversation {
        let sid = presage::libsignal_service::protocol::ServiceId::Aci(
            presage::libsignal_service::prelude::Uuid::parse_str(uuid)
                .expect("uuid")
                .into(),
        );
        PinnedConversation {
            identifier: Some(account_record::pinned_conversation::Identifier::Contact(
                account_record::pinned_conversation::Contact {
                    service_id: sid.service_id_string(),
                    e164: String::new(),
                    service_id_binary: sid.service_id_binary(),
                },
            )),
        }
    }

    fn group_pinned(key: &[u8]) -> PinnedConversation {
        PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::GroupMasterKey(key.to_vec()),
            ),
        }
    }

    fn legacy_pinned(text: &str) -> PinnedConversation {
        PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::LegacyGroupId(
                    text.as_bytes().to_vec(),
                ),
            ),
        }
    }

    /// 存储标识 → 引擎会话标识投影：contact（binary 优先，字符串形态回退）、
    /// group（master key 标准 base64 = send groupId 口径）、legacyGroup。
    #[test]
    fn storage_to_engine_projects_all_kinds() {
        let aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let contact = storage_to_engine(&contact_pinned(aci)).expect("contact projects");
        assert_eq!(contact, EngineConversationId::Contact(aci.to_owned()));

        let master_key = [9u8; 32];
        let group = storage_to_engine(&group_pinned(&master_key)).expect("group projects");
        assert_eq!(
            group,
            EngineConversationId::Group(
                base64::engine::general_purpose::STANDARD.encode(master_key)
            )
        );

        let legacy = storage_to_engine(&legacy_pinned("old-group-id")).expect("legacy projects");
        assert_eq!(
            legacy,
            EngineConversationId::LegacyGroup("old-group-id".to_owned())
        );

        // ReleaseNotes：官方内置会话，非引擎口径 → 跳过。
        let release = PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::ReleaseNotes(
                    account_record::pinned_conversation::ReleaseNotes {},
                ),
            ),
        };
        assert!(storage_to_engine(&release).is_none());
        // 空标识（无 oneof）→ 跳过。
        assert!(storage_to_engine(&PinnedConversation::default()).is_none());
    }

    /// 拉黑只读面投影：blocked 三类记录 wire → decode → 过滤 → 投影；
    /// 未拉黑/非三类记录/垃圾 bytes/空标识 → None（调用方跳过）。
    #[test]
    fn blocked_conversation_projection_faces() {
        use storage_proto::{ContactRecord, GroupV1Record, GroupV2Record};

        fn wire(record: storage_proto::storage_record::Record) -> Vec<u8> {
            storage_proto::StorageRecord {
                record: Some(record),
            }
            .encode_to_vec()
        }

        // contact blocked：aciBinary 16 字节优先 → 小写连字 UUID（置顶面
        // contact 投影同口径）。
        let contact = ContactRecord {
            blocked: true,
            aci_binary: [
                0x6a, 0x2c, 0xb1, 0x7f, 0xef, 0x7c, 0x4a, 0x5d, 0x9c, 0x69, 0x6f, 0x8b, 0x2b, 0x1f,
                0x88, 0xae,
            ]
            .to_vec(),
            ..Default::default()
        };
        let expected_aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::Contact(
                contact.clone()
            ))),
            Some(EngineConversationId::Contact(expected_aci.to_owned()))
        );

        // 字符串形态回退（无 binary）：ACI 字符串直通。
        let mut by_string = contact.clone();
        by_string.aci_binary = Vec::new();
        by_string.aci = expected_aci.to_owned();
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::Contact(
                by_string
            ))),
            Some(EngineConversationId::Contact(expected_aci.to_owned()))
        );

        // e164 形态非引擎 contact 口径 → 跳过。
        let mut e164_only = contact.clone();
        e164_only.aci_binary = Vec::new();
        e164_only.e164 = "+15551234567".to_owned();
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::Contact(
                e164_only
            ))),
            None
        );

        // 未拉黑 → None。
        let mut unblocked = contact;
        unblocked.blocked = false;
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::Contact(
                unblocked
            ))),
            None
        );

        // group（v2）blocked：master key → 标准 base64（send groupId 口径）。
        let mut group = GroupV2Record {
            blocked: true,
            master_key: vec![9u8; 32],
            ..Default::default()
        };
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::GroupV2(
                group.clone()
            ))),
            Some(EngineConversationId::Group(
                base64::engine::general_purpose::STANDARD.encode([9u8; 32])
            ))
        );
        // 空 master key → 跳过。
        let mut keyless = group.clone();
        keyless.master_key = Vec::new();
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::GroupV2(
                keyless
            ))),
            None
        );
        // 未拉黑 → None。
        group.blocked = false;
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::GroupV2(group))),
            None
        );

        // legacyGroup（v1）blocked：id bytes → 引擎字符串口径。
        let mut legacy = GroupV1Record {
            blocked: true,
            id: b"old-group-id".to_vec(),
            ..Default::default()
        };
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::GroupV1(
                legacy.clone()
            ))),
            Some(EngineConversationId::LegacyGroup("old-group-id".to_owned()))
        );
        // 未拉黑 → None。
        legacy.blocked = false;
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::GroupV1(
                legacy
            ))),
            None
        );

        // 非三类记录（Account）/垃圾 bytes/空明文 → None（跳过不炸面）。
        assert_eq!(
            blocked_conversation_of(&wire(storage_proto::storage_record::Record::Account(
                AccountRecord::default()
            ))),
            None
        );
        assert_eq!(blocked_conversation_of(&[0xff, 0xff, 0xff]), None);
        assert_eq!(blocked_conversation_of(&[]), None);
    }

    /// 拉黑位翻面纯函数（flip_blocked / contact_with_blocked /
    /// group_v1_with_blocked / group_v2_with_blocked）：置拉黑写
    /// blockedAt=now、已拉黑幂等不重写他端时刻、解除清零、GV1 无时间戳
    /// 字段；非三类记录/垃圾 bytes → ReadFailed。
    #[test]
    fn flip_blocked_faces_idempotent_and_timestamp_semantics() {
        use prost::Message as _;
        use storage_proto::{ContactRecord, GroupV1Record, GroupV2Record};

        const NOW: u64 = 1_700_000_000_000;
        let contact_wire = |blocked: bool, blocked_at: u64| {
            storage_proto::StorageRecord {
                record: Some(storage_proto::storage_record::Record::Contact(
                    ContactRecord {
                        blocked,
                        blocked_at_timestamp: blocked_at,
                        given_name: "Given".to_owned(),
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec()
        };

        // 未拉黑 → 拉黑：blocked=true + blockedAt=now，given_name 保真。
        let Some((kind, RecordPayload::Contact(updated))) =
            flip_blocked(&contact_wire(false, 0), true, NOW).expect("flips")
        else {
            panic!("contact arm");
        };
        assert_eq!(kind, StorageRecordKind::Contact);
        assert!(updated.blocked);
        assert_eq!(updated.blocked_at_timestamp, NOW);
        assert_eq!(updated.given_name, "Given");

        // 已拉黑（含历史时刻）再拉黑 → None（幂等，不重写他端时刻）。
        assert!(flip_blocked(&contact_wire(true, 123), true, NOW)
            .expect("decodes")
            .is_none());

        // 拉黑 → 解除：blocked=false + blockedAt 清零。
        let Some((_, RecordPayload::Contact(lifted))) =
            flip_blocked(&contact_wire(true, 123), false, NOW).expect("flips")
        else {
            panic!("contact arm");
        };
        assert!(!lifted.blocked);
        assert_eq!(lifted.blocked_at_timestamp, 0);

        // 未拉黑再解除 → None（幂等）。
        assert!(flip_blocked(&contact_wire(false, 0), false, NOW)
            .expect("decodes")
            .is_none());

        // GV2 同 contact 时间戳语义；GV1 无时间戳字段，仅翻位。
        let group_v2_wire = |blocked: bool, blocked_at: u64| {
            storage_proto::StorageRecord {
                record: Some(storage_proto::storage_record::Record::GroupV2(
                    GroupV2Record {
                        blocked,
                        blocked_at_timestamp: blocked_at,
                        master_key: vec![9u8; 32],
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec()
        };
        let Some((StorageRecordKind::GroupV2, RecordPayload::GroupV2(group))) =
            flip_blocked(&group_v2_wire(false, 0), true, NOW).expect("flips")
        else {
            panic!("group arm");
        };
        assert!(group.blocked);
        assert_eq!(group.blocked_at_timestamp, NOW);
        assert!(flip_blocked(&group_v2_wire(true, 5), true, NOW)
            .expect("decodes")
            .is_none());

        let legacy_wire = storage_proto::StorageRecord {
            record: Some(storage_proto::storage_record::Record::GroupV1(
                GroupV1Record {
                    blocked: false,
                    id: b"old-group-id".to_vec(),
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec();
        let Some((StorageRecordKind::GroupV1, RecordPayload::GroupV1(legacy))) =
            flip_blocked(&legacy_wire, true, NOW).expect("flips")
        else {
            panic!("legacy arm");
        };
        assert!(legacy.blocked);
        assert_eq!(legacy.id, b"old-group-id".to_vec());

        // 非三类记录（Account）/垃圾 bytes → ReadFailed（目标在云上但读不出）。
        let account_wire = storage_proto::StorageRecord {
            record: Some(storage_proto::storage_record::Record::Account(
                AccountRecord::default(),
            )),
        }
        .encode_to_vec();
        assert!(matches!(
            flip_blocked(&account_wire, true, NOW),
            Err(StorageSyncError::ReadFailed)
        ));
        assert!(matches!(
            flip_blocked(&[0xff, 0xff, 0xff], true, NOW),
            Err(StorageSyncError::ReadFailed)
        ));
    }

    /// 写后 blocked 名单投影（project_blocked_after）：被换写条目按写后
    /// 状态计入（replaced_key 剔除旧态）、目标置拉黑缺席则补尾（存储序）、
    /// 解除（false）不追加。
    #[test]
    fn project_blocked_after_swaps_target_and_appends() {
        let aci_a = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let aci_b = "11111111-2222-4333-8444-555555555555";
        let contact_item = |aci: &str| ConversationItem {
            key: vec![aci.as_bytes().first().copied().unwrap_or(b'x')],
            plaintext: storage_proto::StorageRecord {
                record: Some(storage_proto::storage_record::Record::Contact(
                    storage_proto::ContactRecord {
                        blocked: true,
                        aci: aci.to_owned(),
                        ..Default::default()
                    },
                )),
            }
            .encode_to_vec(),
        };
        let items = vec![contact_item(aci_a), contact_item(aci_b)];
        let target =
            EngineConversationId::Contact("99999999-8888-4777-8666-555555555555".to_owned());

        // 目标不在名单：置拉黑写后补尾（存储序 = 先既有后目标）。
        assert_eq!(
            project_blocked_after(&items, Some(b"9"), &target, true),
            vec![
                EngineConversationId::Contact(aci_a.to_owned()),
                EngineConversationId::Contact(aci_b.to_owned()),
                target.clone(),
            ]
        );

        // 解除（false）：不追加，名单只剩既有条目。
        assert_eq!(
            project_blocked_after(&items, None, &target, false),
            vec![
                EngineConversationId::Contact(aci_a.to_owned()),
                EngineConversationId::Contact(aci_b.to_owned()),
            ]
        );

        // replaced_key 命中既有条目：旧态剔除（目标换写态由上方补尾位）。
        let replaced = items[0].key.clone();
        assert_eq!(
            project_blocked_after(&items, Some(&replaced), &target, true),
            vec![EngineConversationId::Contact(aci_b.to_owned()), target,]
        );
    }

    /// contact 反投影：ACI 字符串 → serviceIdBinary 往返；无 service_id
    /// 参数 → CONVERSATION_NOT_RESOLVED。
    #[test]
    fn engine_to_storage_roundtrip_contact() {
        let aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let engine = EngineConversationId::Contact(aci.to_owned());
        let pinned = engine_to_storage(&engine, Some(aci)).expect("resolves");
        let back = storage_to_engine(&pinned).expect("parses back");
        assert_eq!(back, engine);

        assert!(matches!(
            engine_to_storage(&engine, None),
            Err(StorageSyncError::ConversationNotResolved)
        ));
        assert!(matches!(
            engine_to_storage(&engine, Some("not-a-uuid")),
            Err(StorageSyncError::ConversationNotResolved)
        ));
    }

    /// group 反投影：master key bytes → base64 → bytes 往返。
    #[test]
    fn engine_group_roundtrip() {
        let master_key = [7u8; 32];
        let engine = EngineConversationId::Group(
            base64::engine::general_purpose::STANDARD.encode(master_key),
        );
        assert_eq!(
            engine_group_to_master_key(&engine).expect("decodes"),
            master_key.to_vec()
        );
        assert!(matches!(
            engine_group_to_master_key(&EngineConversationId::Contact("x".to_owned())),
            Err(StorageSyncError::ConversationNotResolved)
        ));
    }

    /// legacy 反投影往返。
    #[test]
    fn engine_legacy_roundtrip() {
        let engine = EngineConversationId::LegacyGroup("grp-1".to_owned());
        assert_eq!(engine_legacy_to_bytes(&engine).expect("bytes"), b"grp-1");
    }

    /// pinned 增删纯函数（stored 面）：数组序语义、幂等、重复添加去重、
    /// 移除不存在条目、非目标条目（ReleaseNotes）原样保留。
    #[test]
    fn pinned_add_remove_array_order_semantics() {
        let a = contact_pinned("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae");
        let b = group_pinned(&[1u8; 32]);
        let c = group_pinned(&[2u8; 32]);
        let release = PinnedConversation {
            identifier: Some(
                account_record::pinned_conversation::Identifier::ReleaseNotes(
                    account_record::pinned_conversation::ReleaseNotes {},
                ),
            ),
        };

        // 追加到数组尾。
        let list = pinned_add_stored(&[], &a);
        assert_eq!(list, vec![a.clone()]);
        let list = pinned_add_stored(&list, &b);
        assert_eq!(list, vec![a.clone(), b.clone()]);

        // 重复添加去重（保持原序，官方 addPinFromState 缺失才追加）。
        let list = pinned_add_stored(&list, &a);
        assert_eq!(list, vec![a.clone(), b.clone()]);

        // 移除（数组序保持），ReleaseNotes 不受影响。
        let with_release = pinned_add_stored(&list, &release);
        assert_eq!(with_release, vec![a.clone(), b.clone(), release.clone()]);
        let removed = pinned_remove_stored(&with_release, &a);
        assert_eq!(removed, vec![b.clone(), release.clone()]);

        // 移除不存在条目 = 幂等成功。
        assert_eq!(pinned_remove_stored(&removed, &c), vec![b.clone(), release]);
        assert!(pinned_remove_stored(&[], &a).is_empty());
    }

    /// 409 重试决策纯函数：1s/5s/30s 递增、第 4 次放弃。
    #[test]
    fn conflict_retry_increments_then_gives_up() {
        assert_eq!(
            conflict_decision(0),
            ConflictDecision::Retry { delay_ms: 1_000 }
        );
        assert_eq!(
            conflict_decision(1),
            ConflictDecision::Retry { delay_ms: 5_000 }
        );
        assert_eq!(
            conflict_decision(2),
            ConflictDecision::Retry { delay_ms: 30_000 }
        );
        assert_eq!(conflict_decision(3), ConflictDecision::GiveUp);
        assert_eq!(conflict_decision(99), ConflictDecision::GiveUp);
    }

    /// 本地状态文件写入：格式 `{version, accountItemKey}`、无 `.tmp` 残留、
    /// 原子覆盖（读取方仅排查用，格式稳定性由本测试钉住）。
    #[test]
    fn manifest_state_file_write_format_is_stable() {
        let root = std::env::temp_dir().join(format!(
            "kt-engine-storage-state-{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        ));
        std::fs::create_dir_all(&root).unwrap();

        save_manifest_state(
            &root,
            &ManifestState {
                version: 7,
                account_item_key: Some("QUJD".to_owned()),
                sticker_item_keys: None,
            },
        );
        let bytes = std::fs::read(root.join(MANIFEST_STATE_FILE)).expect("written");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(value["version"], 7);
        assert_eq!(value["accountItemKey"], "QUJD");
        assert!(!root.join(format!("{MANIFEST_STATE_FILE}.tmp")).exists());

        // 二次写入覆盖（无追加残留）。
        save_manifest_state(
            &root,
            &ManifestState {
                version: 8,
                account_item_key: None,
                sticker_item_keys: None,
            },
        );
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(value["version"], 8);
        assert_eq!(value["accountItemKey"], serde_json::Value::Null);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 状态文件 P5 泛化：stickerItemKeys 写入/沿用/旧格式兼容读取。
    #[test]
    fn manifest_state_sticker_keys_and_legacy_format_compat() {
        let root = std::env::temp_dir().join(format!(
            "kt-engine-storage-state5-{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        ));
        std::fs::create_dir_all(&root).unwrap();

        // 旧格式（P4：无 stickerItemKeys 键）兼容读取。
        std::fs::write(
            root.join(MANIFEST_STATE_FILE),
            br#"{"version": 3, "accountItemKey": "QUJD"}"#,
        )
        .unwrap();
        let loaded = load_manifest_state(&root).expect("loads legacy format");
        assert_eq!(loaded.version, 3);
        assert_eq!(loaded.account_item_key.as_deref(), Some("QUJD"));
        assert_eq!(loaded.sticker_item_keys, None);

        // 置顶面写（sticker_item_keys=None）→ 沿用旧值（旧文件无值 → 空表）。
        save_manifest_state(
            &root,
            &ManifestState {
                version: 4,
                account_item_key: Some("REVGSg==".to_owned()),
                sticker_item_keys: None,
            },
        );
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(value["stickerItemKeys"], json!([]));

        // 贴纸面写（Some）→ 覆盖；再置顶面写（None）→ 保留贴纸面列表。
        save_manifest_state(
            &root,
            &ManifestState {
                version: 5,
                account_item_key: Some("REVGSg==".to_owned()),
                sticker_item_keys: Some(vec!["AA==".to_owned(), "AQ==".to_owned()]),
            },
        );
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(value["stickerItemKeys"], json!(["AA==", "AQ=="]));
        save_manifest_state(
            &root,
            &ManifestState {
                version: 6,
                account_item_key: Some("REVGSg==".to_owned()),
                sticker_item_keys: None,
            },
        );
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(value["stickerItemKeys"], json!(["AA==", "AQ=="]));

        // 坏 JSON / 缺文件 → None（容错读取）。
        std::fs::write(root.join(MANIFEST_STATE_FILE), b"not json").unwrap();
        assert!(load_manifest_state(&root).is_none());
        let _ = std::fs::remove_dir_all(&root);
        assert!(load_manifest_state(&root).is_none());
    }

    /// AccountRecord 读-改-写保真：带多字段的 record bytes → decode → 只改
    /// pinned → encode → 其他字段字节不变（prost round-trip 保真铁面）。
    #[test]
    fn account_record_read_modify_write_preserves_other_fields() {
        let original = AccountRecord {
            profile_key: vec![1u8; 32],
            given_name: "Given".to_owned(),
            family_name: "Family".to_owned(),
            avatar_url_path: "avatars/xyz".to_owned(),
            note_to_self_archived: true,
            read_receipts: true,
            sealed_sender_indicators: true,
            typing_indicators: true,
            link_previews: true,
            phone_number_sharing_mode: 2,
            universal_expire_timer: 86_400,
            preferred_reaction_emoji: vec!["👍".to_owned(), "❤️".to_owned()],
            subscriber_id: vec![9u8; 16],
            subscriber_currency_code: "USD".to_owned(),
            username: "kt.01".to_owned(),
            story_view_receipts_enabled: 2,
            pinned_conversations: vec![contact_pinned("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")],
            ..AccountRecord::default()
        };
        let encoded = original.encode_to_vec();
        let decoded = AccountRecord::decode(encoded.as_slice()).expect("decodes");

        // 保真铁面：把 pinned 清空后重编码，应与「原文去掉 field 14」逐字节
        // 等价——即除 pinned 外无任何字段被触碰。做法：原文与改写文都剥出
        // 重新 decode 的全字段比对（pinned 之外逐一相等）。
        let target = group_pinned([5u8; 32].as_slice());
        let updated = account_record_with_pinned(&decoded, vec![target]);
        let re_encoded = updated.encode_to_vec();
        let reparsed = AccountRecord::decode(re_encoded.as_slice()).expect("decodes");
        assert_eq!(reparsed.profile_key, original.profile_key);
        assert_eq!(reparsed.given_name, original.given_name);
        assert_eq!(reparsed.family_name, original.family_name);
        assert_eq!(reparsed.avatar_url_path, original.avatar_url_path);
        assert!(reparsed.note_to_self_archived);
        assert!(reparsed.read_receipts);
        assert!(reparsed.sealed_sender_indicators);
        assert!(reparsed.typing_indicators);
        assert!(reparsed.link_previews);
        assert_eq!(reparsed.phone_number_sharing_mode, 2);
        assert_eq!(reparsed.universal_expire_timer, 86_400);
        assert_eq!(
            reparsed.preferred_reaction_emoji,
            original.preferred_reaction_emoji
        );
        assert_eq!(reparsed.subscriber_id, original.subscriber_id);
        assert_eq!(
            reparsed.subscriber_currency_code,
            original.subscriber_currency_code
        );
        assert_eq!(reparsed.username, original.username);
        assert_eq!(reparsed.story_view_receipts_enabled, 2);
        // pinned 恰好换成了目标条目。
        assert_eq!(reparsed.pinned_conversations.len(), 1);
        assert!(matches!(
            reparsed.pinned_conversations[0].identifier,
            Some(account_record::pinned_conversation::Identifier::GroupMasterKey(_))
        ));

        // 字节级差分面：把改写文 pinned 置回原文形状后重编码，与原文编码
        // 逐字节一致（decode→mutate→encode 无信息损失）。
        let restored = account_record_with_pinned(&reparsed, original.pinned_conversations.clone());
        assert_eq!(restored.encode_to_vec(), encoded);
    }

    /// 字段保真铁面（官方 vendored proto，src/proto/storage.rs）：官方现行
    /// AccountRecord 已用到 field 60，钉定 presage rev 的同源 proto 只定义到
    /// field 51——引擎写路径（Account item 明文 decode→只改 pinned→encode）
    /// 必须保活 52-60 的全部官方字段，否则每次置顶/贴纸/账号设置写都会把
    /// 手机侧这些账号设置静默清零。
    #[test]
    fn account_record_fields_52_to_60_survive_engine_write_path() {
        use crate::proto::storage::{account_record as off_account, AccountRecord as Official};
        use crate::proto::storage::{storage_record as off_storage, StorageRecord as OfficialWrap};
        use presage::libsignal_service::master_key::MasterKey;

        use prost::Message as _;
        let original = Official {
            // 既有字段面（1-51 抽样）一并钉定，防修复把旧字段改丢。
            profile_key: vec![1u8; 32],
            given_name: "Given".to_owned(),
            read_receipts: true,
            typing_indicators: true,
            phone_number_sharing_mode: 2,
            universal_expire_timer: 86_400,
            username: "kt.01".to_owned(),
            pinned_conversations: vec![crate::proto::storage::account_record::PinnedConversation {
                identifier: Some(
                    crate::proto::storage::account_record::pinned_conversation::Identifier::GroupMasterKey(
                        vec![1u8; 32],
                    ),
                ),
            }],
            // 官方现行 52-60 全字段（field 51 之后新增的账号设置面）。
            release_notes_chat_blocked_at: Some(1_720_000_000_000),
            unread_badge_type: off_account::UnreadBadgeType::UnreadChats as i32,
            include_muted_chats_in_badge: crate::proto::storage::OptionalBool::Enabled as i32,
            reaction_notifications: crate::proto::storage::OptionalBool::Disabled as i32,
            notify_for_calls_if_muted: crate::proto::storage::OptionalBool::Enabled as i32,
            notify_for_mentions_if_muted: crate::proto::storage::OptionalBool::Disabled as i32,
            notify_for_replies_if_muted: crate::proto::storage::OptionalBool::Enabled as i32,
            show_unread_reminders: crate::proto::storage::OptionalBool::Disabled as i32,
            notify_when_contact_joins: crate::proto::storage::OptionalBool::Enabled as i32,
            ..Official::default()
        };
        let wire = OfficialWrap {
            record: Some(off_storage::Record::Account(original.clone())),
        }
        .encode_to_vec();

        // 引擎生产链（read→mutate→plan_write→decrypt 回读，与 write_pinned
        // 同构）：只改 pinned（Group 换 Group），其余字段经 vendored proto
        // 全链保活——修复前走 presage 类型面，52-60 在 decode 即被剥除。
        let read_back = account_record_of_wire(&wire).expect("account item decodes");
        let updated =
            account_record_with_pinned(&read_back, vec![group_pinned([2u8; 32].as_slice())]);
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[3u8; 32]).expect("key"));
        let planned = plan_write(None, &updated, 2, vec![0xaau8; 16], &storage_key);
        let rewritten =
            decrypt_raw_item(&storage_key, &planned.write_operation.insert_item[0], None)
                .expect("decrypts");

        // 用官方 vendored proto 回读，逐字段断言 52-60 保活。
        let back = match OfficialWrap::decode(rewritten.as_slice())
            .expect("rewritten decodes")
            .record
            .expect("record arm")
        {
            off_storage::Record::Account(account) => account,
            _ => panic!("account arm"),
        };
        assert_eq!(back.profile_key, original.profile_key);
        assert_eq!(back.given_name, original.given_name);
        assert!(back.read_receipts);
        assert!(back.typing_indicators);
        assert_eq!(back.phone_number_sharing_mode, 2);
        assert_eq!(back.universal_expire_timer, 86_400);
        assert_eq!(back.username, original.username);
        assert_eq!(back.pinned_conversations.len(), 1);
        match back.pinned_conversations[0].identifier.as_ref() {
            Some(off_account::pinned_conversation::Identifier::GroupMasterKey(key)) => {
                assert_eq!(key, &vec![2u8; 32]);
            }
            _ => panic!("group master key arm"),
        }
        assert_eq!(
            back.release_notes_chat_blocked_at,
            original.release_notes_chat_blocked_at
        );
        assert_eq!(back.unread_badge_type, original.unread_badge_type);
        assert_eq!(
            back.include_muted_chats_in_badge,
            original.include_muted_chats_in_badge
        );
        assert_eq!(back.reaction_notifications, original.reaction_notifications);
        assert_eq!(
            back.notify_for_calls_if_muted,
            original.notify_for_calls_if_muted
        );
        assert_eq!(
            back.notify_for_mentions_if_muted,
            original.notify_for_mentions_if_muted
        );
        assert_eq!(
            back.notify_for_replies_if_muted,
            original.notify_for_replies_if_muted
        );
        assert_eq!(back.show_unread_reminders, original.show_unread_reminders);
        assert_eq!(
            back.notify_when_contact_joins,
            original.notify_when_contact_joins
        );
    }

    /// 设置面 mutate（setAccountConfiguration）：只写提供字段，其余字段
    /// （含 52-60 与 pinned）原样；提供字段已全部满足 → None（幂等无写）；
    /// PhoneNumberSharingMode 参数映射（everybody=1/nobody=2，桌面/Android
    /// 现行枚举一致，旧 CONTACTS_ONLY 不收）。
    #[test]
    fn account_record_configuration_writes_only_provided_fields() {
        use crate::proto::storage::account_record as off_account;
        use crate::proto::storage::AccountRecord as Official;
        use crate::proto::storage::OptionalBool as OffOptionalBool;

        let everybody = off_account::PhoneNumberSharingMode::Everybody as i32;
        let nobody = off_account::PhoneNumberSharingMode::Nobody as i32;
        let base = Official {
            given_name: "Given".to_owned(),
            read_receipts: false,
            typing_indicators: false,
            phone_number_sharing_mode: everybody,
            unlisted_phone_number: false,
            universal_expire_timer: 86_400,
            username: "kt.01".to_owned(),
            story_view_receipts_enabled: OffOptionalBool::Enabled as i32,
            release_notes_chat_blocked_at: Some(1_720_000_000_000),
            pinned_conversations: vec![crate::proto::storage::account_record::PinnedConversation {
                identifier: Some(
                    crate::proto::storage::account_record::pinned_conversation::Identifier::GroupMasterKey(
                        vec![1u8; 32],
                    ),
                ),
            }],
            ..Official::default()
        };

        // 只写两个开关：手机号可见性/名字/universal timer/52-60/pinned 原样。
        let update = AccountConfigurationUpdate {
            read_receipts: Some(true),
            typing_indicators: Some(true),
            ..AccountConfigurationUpdate::default()
        };
        let updated = account_record_with_configuration(&base, &update).expect("changes");
        assert!(updated.read_receipts);
        assert!(updated.typing_indicators);
        assert_eq!(updated.phone_number_sharing_mode, everybody);
        assert!(!updated.unlisted_phone_number);
        assert_eq!(updated.given_name, "Given");
        assert_eq!(updated.universal_expire_timer, 86_400);
        assert_eq!(updated.username, "kt.01");
        assert_eq!(
            updated.release_notes_chat_blocked_at,
            Some(1_720_000_000_000)
        );
        assert_eq!(
            updated.story_view_receipts_enabled,
            OffOptionalBool::Enabled as i32
        );
        assert_eq!(updated.pinned_conversations.len(), 1);

        // 手机号可见性：sharing mode → nobody + unlisted=true；未提供的
        // 开关字段不动。
        let update = AccountConfigurationUpdate {
            phone_number_sharing_mode: Some(nobody),
            unlisted_phone_number: Some(true),
            ..AccountConfigurationUpdate::default()
        };
        let updated = account_record_with_configuration(&base, &update).expect("changes");
        assert_eq!(updated.phone_number_sharing_mode, nobody);
        assert!(updated.unlisted_phone_number);
        assert!(!updated.read_receipts);
        assert!(!updated.typing_indicators);

        // 幂等：提供字段与现状全等 → None（无写面）。
        let same = AccountConfigurationUpdate {
            read_receipts: Some(false),
            typing_indicators: Some(false),
            phone_number_sharing_mode: Some(everybody),
            unlisted_phone_number: Some(false),
        };
        assert!(account_record_with_configuration(&base, &same).is_none());

        // 参数面映射：官方枚举数值 + 未知字符串不收。
        assert_eq!(phone_number_sharing_mode_of("everybody"), Some(everybody));
        assert_eq!(phone_number_sharing_mode_of("nobody"), Some(nobody));
        assert_eq!(phone_number_sharing_mode_of("contactsOnly"), None);
        assert_eq!(phone_number_sharing_mode_of(""), None);
    }

    /// universal timer mutate（setUniversalExpireTimer）：只覆写
    /// universalExpireTimer(17)，其余字段（含 52-60/pinned）原样；同值 →
    /// None（幂等无写）。
    #[test]
    fn account_record_universal_expire_timer_writes_only_the_field() {
        use crate::proto::storage::account_record as off_account;
        use crate::proto::storage::AccountRecord as Official;
        use crate::proto::storage::OptionalBool as OffOptionalBool;

        let base = Official {
            universal_expire_timer: 0,
            read_receipts: true,
            release_notes_chat_blocked_at: Some(1_720_000_000_000),
            story_view_receipts_enabled: OffOptionalBool::Enabled as i32,
            pinned_conversations: vec![crate::proto::storage::account_record::PinnedConversation {
                identifier: Some(
                    off_account::pinned_conversation::Identifier::GroupMasterKey(vec![1u8; 32]),
                ),
            }],
            ..Official::default()
        };

        let updated = account_record_with_universal_expire_timer(&base, 86_400).expect("changes");
        assert_eq!(updated.universal_expire_timer, 86_400);
        assert!(updated.read_receipts);
        assert_eq!(
            updated.release_notes_chat_blocked_at,
            Some(1_720_000_000_000)
        );
        assert_eq!(
            updated.story_view_receipts_enabled,
            OffOptionalBool::Enabled as i32
        );
        assert_eq!(updated.pinned_conversations.len(), 1);

        // 0=关闭（写 0 也是合法变更面）。
        let updated = account_record_with_universal_expire_timer(&updated, 0)
            .expect("0 is a valid change (off)");
        assert_eq!(updated.universal_expire_timer, 0);

        // 幂等：同值 → None。
        assert!(account_record_with_universal_expire_timer(&base, 0).is_none());
    }

    /// plan_write：首写（无旧 manifest）→ version 1、无 deleteKey；次写 →
    /// version 递增、旧 Account key 进 deleteKey、record_ikm 透传。
    #[test]
    fn plan_write_versions_and_deletes() {
        use presage::libsignal_service::master_key::MasterKey;
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[3u8; 32]).expect("key"));
        let record = minimal_account_record(vec![group_pinned([1u8; 32].as_slice())]);

        // 首写分支：从未有过 storage 状态。
        let planned = plan_write(None, &record, 2, vec![0xaau8; 16], &storage_key);
        assert_eq!(planned.new_version, 1);
        assert_eq!(planned.new_account_key, vec![0xaau8; 16]);
        assert!(planned.write_operation.delete_key.is_empty());
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            planned.write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        assert_eq!(manifest_record.version, 1);
        assert_eq!(manifest_record.source_device, 2);
        assert_eq!(manifest_record.identifiers.len(), 1);
        assert_eq!(manifest_record.identifiers[0].raw, vec![0xaau8; 16]);
        // 新 item 能解回（legacy 路径，无 record_ikm）：本地 shim 解出官方
        // vendored proto 记录；上游类型也解同一条 item（写面与上游读兼容）。
        let item = &planned.write_operation.insert_item[0];
        let plaintext = decrypt_raw_item(&storage_key, item, None).expect("decrypts");
        assert!(account_record_of_wire(&plaintext).is_some());
        let upstream = StorageService::decrypt_item(&storage_key, item, None).expect("decrypts");
        assert!(matches!(
            upstream.record,
            Some(presage::libsignal_service::proto::storage_record::Record::Account(_))
        ));

        // 次写分支：旧 manifest 换 Account key，record_ikm 透传。
        let old_manifest = ManifestRecord {
            version: 4,
            source_device: 2,
            identifiers: vec![
                manifest_record::Identifier {
                    raw: vec![0x11u8; 16],
                    r#type: manifest_record::identifier::Type::Contact as i32,
                },
                manifest_record::Identifier {
                    raw: vec![0x22u8; 16],
                    r#type: manifest_record::identifier::Type::Account as i32,
                },
            ],
            record_ikm: vec![0x33u8; 32],
        };
        let planned = plan_write(
            Some((&old_manifest, vec![0x22u8; 16])),
            &record,
            2,
            vec![0xbbu8; 16],
            &storage_key,
        );
        assert_eq!(planned.new_version, 5);
        assert_eq!(planned.write_operation.delete_key, vec![vec![0x22u8; 16]]);
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            planned.write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        assert_eq!(manifest_record.record_ikm, vec![0x33u8; 32]);
        assert_eq!(manifest_record.identifiers.len(), 2);
        assert!(manifest_record
            .identifiers
            .iter()
            .all(|identifier| identifier.raw != vec![0x22u8; 16]));
        // modern 路径：新 item 以 record_ikm 解回（本地 shim + 上游双验）。
        let item = &planned.write_operation.insert_item[0];
        let plaintext = decrypt_raw_item(&storage_key, item, Some(&manifest_record.record_ikm))
            .expect("decrypts");
        assert!(account_record_of_wire(&plaintext).is_some());
        let upstream =
            StorageService::decrypt_item(&storage_key, item, Some(&manifest_record.record_ikm))
                .expect("decrypts");
        assert!(matches!(
            upstream.record,
            Some(presage::libsignal_service::proto::storage_record::Record::Account(_))
        ));
    }

    /// minimal_account_record：仅 pinned，其余默认。
    #[test]
    fn minimal_account_record_has_only_pinned() {
        let record = minimal_account_record(vec![group_pinned([2u8; 32].as_slice())]);
        assert_eq!(record.pinned_conversations.len(), 1);
        assert!(record.given_name.is_empty());
        assert!(record.profile_key.is_empty());
        assert!(!record.read_receipts);
    }

    /// plan_record_writes 泛化：Account + StickerPack 混合 identifiers 的
    /// 换入换出（type=6 raw i32）、deleteKey 按实体换出、record_ikm 透传、
    /// 各 item 以对应路径解回。plan_write（Account 薄壳）行为由既有
    /// plan_write_versions_and_deletes 钉定不变。
    #[test]
    fn plan_record_writes_mixed_account_and_sticker() {
        use crate::service::storage_sticker::{sticker_record_of, StickerPackRecord};
        use presage::libsignal_service::master_key::MasterKey;
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[3u8; 32]).expect("key"));
        let old_manifest = ManifestRecord {
            version: 4,
            source_device: 2,
            identifiers: vec![
                manifest_record::Identifier {
                    raw: vec![0x11u8; 16],
                    r#type: manifest_record::identifier::Type::Contact as i32,
                },
                manifest_record::Identifier {
                    raw: vec![0x22u8; 16],
                    r#type: manifest_record::identifier::Type::Account as i32,
                },
                manifest_record::Identifier {
                    raw: vec![0x44u8; 16],
                    r#type: crate::service::storage_sticker::STICKER_PACK_IDENTIFIER_TYPE,
                },
            ],
            record_ikm: vec![0x33u8; 32],
        };
        let sticker = StickerPackRecord {
            pack_id: vec![7u8; 16],
            pack_key: vec![8u8; 32],
            position: 2,
            deleted_at_timestamp: 0,
        };
        let planned = plan_record_writes(
            Some(&old_manifest),
            &[
                RecordUpsert {
                    kind: StorageRecordKind::Account,
                    new_key: vec![0xbbu8; 16],
                    replaced_key: Some(vec![0x22u8; 16]),
                    payload: Some(RecordPayload::Account(minimal_account_record(Vec::new()))),
                },
                RecordUpsert {
                    kind: StorageRecordKind::StickerPack,
                    new_key: vec![0xccu8; 16],
                    replaced_key: Some(vec![0x44u8; 16]),
                    payload: Some(RecordPayload::StickerPack(sticker.clone())),
                },
            ],
            2,
            &storage_key,
        );
        assert_eq!(planned.new_version, 5);
        assert_eq!(
            planned.write_operation.delete_key,
            vec![vec![0x22u8; 16], vec![0x44u8; 16]]
        );
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            planned.write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        // Contact 保留、Account/Sticker 各换新、旧 key 消失、type=6 raw 透传。
        assert_eq!(manifest_record.record_ikm, vec![0x33u8; 32]);
        assert_eq!(manifest_record.identifiers.len(), 3);
        let by_type = |raw: &[u8]| {
            manifest_record
                .identifiers
                .iter()
                .find(|identifier| identifier.raw == raw)
                .map(|identifier| identifier.r#type)
        };
        assert_eq!(
            by_type(&[0x11u8; 16]),
            Some(manifest_record::identifier::Type::Contact as i32)
        );
        assert_eq!(
            by_type(&[0xbbu8; 16]),
            Some(manifest_record::identifier::Type::Account as i32)
        );
        assert_eq!(by_type(&[0xccu8; 16]), Some(6));
        assert!(by_type(&[0x22u8; 16]).is_none());
        assert!(by_type(&[0x44u8; 16]).is_none());

        // insert items：[0] Account（本地 shim 解密路径，官方 vendored proto
        // 字段面；上游类型解同一条 item 验证写面兼容——tag 4 Account arm
        // 命中）、[1] Sticker（本地 shim 解密路径 → tag=6 包裹解析还原）。
        let account_item = &planned.write_operation.insert_item[0];
        let account_plaintext = decrypt_raw_item(
            &storage_key,
            account_item,
            Some(&manifest_record.record_ikm),
        )
        .expect("decrypts");
        assert!(account_record_of_wire(&account_plaintext).is_some());
        let upstream_account = StorageService::decrypt_item(
            &storage_key,
            account_item,
            Some(&manifest_record.record_ikm),
        )
        .expect("decrypts");
        assert!(matches!(
            upstream_account.record,
            Some(presage::libsignal_service::proto::storage_record::Record::Account(_))
        ));
        let sticker_item = &planned.write_operation.insert_item[1];
        let plaintext = decrypt_raw_item(
            &storage_key,
            sticker_item,
            Some(&manifest_record.record_ikm),
        )
        .expect("decrypts");
        assert_eq!(sticker_record_of(&plaintext), Some(sticker.clone()));
        // 上游类型读同一条 item：tag=6 未知字段被跳过（旧 proto 读面行为）。
        let upstream_view = StorageService::decrypt_item(
            &storage_key,
            sticker_item,
            Some(&manifest_record.record_ikm),
        )
        .expect("decrypts");
        assert!(upstream_view.record.is_none());

        // 贴纸包首次写入（无 replaced key）→ 无 deleteKey 条目；无旧
        // manifest → version 1。
        let planned = plan_record_writes(
            None,
            &[RecordUpsert {
                kind: StorageRecordKind::StickerPack,
                new_key: vec![0xddu8; 16],
                replaced_key: None,
                payload: Some(RecordPayload::StickerPack(sticker.clone())),
            }],
            2,
            &storage_key,
        );
        assert_eq!(planned.new_version, 1);
        assert!(planned.write_operation.delete_key.is_empty());
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            planned.write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        assert_eq!(manifest_record.identifiers.len(), 1);
        assert_eq!(manifest_record.identifiers[0].r#type, 6);
        // legacy 路径（无 record_ikm）解回。
        let plaintext =
            decrypt_raw_item(&storage_key, &planned.write_operation.insert_item[0], None)
                .expect("decrypts");
        assert_eq!(sticker_record_of(&plaintext), Some(sticker));
    }

    /// 本地 item 加解密 shim 与上游 StorageService pub API 的派生一致性
    /// （双向交叉钉定）：上游 encrypt_item 产出的 item 本地能解；本地加密
    /// 的 Account 记录 item 上游能解回同记录。modern/legacy 两路径都钉。
    /// 上游侧显式用 presage 类型（形状面完整）；vendored 编码字节与上游
    /// 读面的兼容性由 plan_write 系测试另钉。
    #[test]
    fn raw_item_crypto_matches_upstream_api() {
        use presage::libsignal_service::master_key::MasterKey;
        use presage::libsignal_service::proto::storage_record as upstream_storage_record;
        use presage::libsignal_service::proto::StorageRecord as UpstreamStorageRecord;
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[4u8; 32]).expect("key"));
        let raw_id = vec![0xaau8; 16];
        let record = UpstreamStorageRecord {
            record: Some(upstream_storage_record::Record::Account(
                presage::libsignal_service::proto::AccountRecord::default(),
            )),
        };
        for record_ikm in [None, Some([0x55u8; 32].as_slice())] {
            // 上游加密 → 本地解密：明文逐字节等于 record 编码。
            let item =
                StorageService::encrypt_item(&storage_key, raw_id.clone(), &record, record_ikm);
            let plaintext = decrypt_raw_item(&storage_key, &item, record_ikm)
                .expect("local decrypts upstream item");
            assert_eq!(plaintext, record.encode_to_vec());
            // 截断/MAC 失败 → Err。
            let mut tampered = item.clone();
            tampered.value.pop();
            assert!(decrypt_raw_item(&storage_key, &tampered, record_ikm).is_err());

            // 本地加密（明文 = record 编码）→ 上游解密：同记录还原。
            let item = encrypt_raw_item(
                &storage_key,
                raw_id.clone(),
                &record.encode_to_vec(),
                record_ikm,
            );
            let back = StorageService::decrypt_item(&storage_key, &item, record_ikm)
                .expect("upstream decrypts local item");
            assert_eq!(back, record);
        }
    }
}
