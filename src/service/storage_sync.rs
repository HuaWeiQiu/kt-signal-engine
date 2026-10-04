// SPDX-License-Identifier: AGPL-3.0-only

//! Storage Service 同步：会话级置顶（P4 扩面）+ 写面泛化（P5，支撑贴纸包
//! 记录面 `storage_sticker`）。
//!
//! 真相源 = Storage Service 的 `AccountRecord.pinned_conversations`（数组序
//! 即置顶顺序，无时间戳）。读面走 presage 上游 pub API（`StorageService::
//! manifest`/`read_items`）；写面是上游缺口（libsignal-service-rs 只有读），
//! 自建薄封装收敛在本模块 `put_write_operation` 单点，上游补齐后整段替换。
//!
//! 官方写语义（Signal-Desktop `ts/services/storage.preload.ts` 行为对齐，
//! 不抄码）：读 manifest → 读 Account item → **只在服务器返回的
//! AccountRecord 上原地改 pinned 字段**（prost decode→mutate→encode
//! round-trip 天然保真其余 30+ 字段；绝禁止从零构造 AccountRecord，会清空
//! 用户其他设备账号设置）→ `encrypt_item`（新 16B 随机 id）+
//! `encrypt_manifest`（version+1，source_device=本设备 device_id，
//! record_ikm 原样透传）→ `WriteOperation{manifest, insertItem, deleteKey}`
//! → `PUT v1/storage/`（WebAPI.preload.ts:831 storageModify 同路径）。
//! 409 响应体含服务器当前 manifest → 采纳重读，按 conflictBackOff
//! [1s,5s,30s]（storage.preload.ts:150）重放，连满 3 次放弃本轮。
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
use presage::libsignal_service::proto::{
    account_record, storage_record, AccountRecord, ManifestRecord, StorageManifest, StorageRecord,
    WriteOperation,
};
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::proto::account_record::PinnedConversation;
use presage::store::{ContentsStore, StateStore};
use rand::RngCore;
use serde_json::json;

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

/// 从 StorageRecord 提取 AccountRecord（非 Account 记录 → None）。
pub(crate) fn account_record_of(record: &StorageRecord) -> Option<&AccountRecord> {
    match record.record.as_ref()? {
        storage_record::Record::Account(account) => Some(account),
        _ => None,
    }
}

/// manifest identifiers 中 Account 记录的 raw key。
pub(crate) fn account_item_key_of(manifest: &ManifestRecord) -> Option<Vec<u8>> {
    manifest
        .identifiers
        .iter()
        .find(|identifier| identifier.r#type() == manifest_record::identifier::Type::Account)
        .map(|identifier| identifier.raw.clone())
}

/// 写面记录类别（manifest identifier type：Account=4、StickerPack=6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageRecordKind {
    Account,
    StickerPack,
}

impl StorageRecordKind {
    /// ManifestRecord.Identifier.Type wire 值。StickerPack=官方
    /// SignalStorage.proto :54 STICKER_PACK=6（钉定 presage rev 的枚举缺
    /// 该值，全程以 raw i32 读写，读面同口径识别）。
    pub(crate) fn identifier_type(self) -> i32 {
        match self {
            Self::Account => manifest_record::identifier::Type::Account as i32,
            Self::StickerPack => crate::service::storage_sticker::STICKER_PACK_IDENTIFIER_TYPE,
        }
    }
}

/// 一次写面的单条记录意图（官方 toXxxRecord + generateManifest 语义的
/// 输入形状）：新 16B item id 换入 identifiers，`replaced_key`（同类型同
/// 实体的旧 identifier raw）进 deleteKey。Account 恒单条：有旧 manifest
/// 时调用方传其 Account key；StickerPack 按 packId 定位旧 key。
pub(crate) struct RecordUpsert {
    pub(crate) kind: StorageRecordKind,
    pub(crate) new_key: Vec<u8>,
    pub(crate) replaced_key: Option<Vec<u8>>,
    pub(crate) payload: RecordPayload,
}

// 瞬态写计划值：构造即编码、不批量持有，变体尺寸差不值得 Box 化波及全部构造点。
#[allow(clippy::large_enum_variant)]
pub(crate) enum RecordPayload {
    Account(AccountRecord),
    StickerPack(crate::service::storage_sticker::StickerPackRecord),
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
            StorageRecordKind::StickerPack => {
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
        identifiers.push(manifest_record::Identifier {
            raw: upsert.new_key.clone(),
            r#type: upsert.kind.identifier_type(),
        });
        let item = match &upsert.payload {
            RecordPayload::Account(account) => StorageService::encrypt_item(
                storage_key,
                upsert.new_key.clone(),
                &StorageRecord {
                    record: Some(storage_record::Record::Account(account.clone())),
                },
                record_ikm_opt,
            ),
            RecordPayload::StickerPack(record) => encrypt_raw_item(
                storage_key,
                upsert.new_key.clone(),
                &crate::service::storage_sticker::sticker_record_wire(record),
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
            payload: RecordPayload::Account(new_account_record.clone()),
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

use crate::service::StorageContext;

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
    let Some(account_key) = account_item_key_of(&manifest) else {
        return Ok(Vec::new());
    };
    let records = storage
        .read_items(vec![account_key], Some(&manifest.record_ikm))
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?;
    let Some(account) = records.iter().find_map(account_record_of) else {
        return Ok(Vec::new());
    };
    Ok(project_pinned(account))
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

/// 写面编排（官方 upload 语义）：读 manifest + Account item → 原地改
/// pinned → encrypt（新 id）→ PUT → 409 采纳重放（conflictBackOff 3 次）。
pub(crate) async fn write_pinned(
    context: &StorageContext,
    target: EngineConversationId,
    pinned: bool,
) -> Result<Vec<PinnedEntry>, StorageSyncError> {
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

    // manifest 404（从未有过 storage 状态）：以 version 1 + 最小
    // AccountRecord（只含 pinned）建初始 manifest（compat §5 登记分支）。
    let (manifest, account) = match storage.manifest().await {
        Ok(manifest) => {
            let Some(account_key) = account_item_key_of(&manifest) else {
                return Err(StorageSyncError::WriteFailed);
            };
            let records = storage
                .read_items(vec![account_key.clone()], Some(&manifest.record_ikm))
                .await
                .map_err(|_| StorageSyncError::WriteFailed)?;
            let account = records
                .iter()
                .find_map(account_record_of)
                .cloned()
                .ok_or(StorageSyncError::WriteFailed)?;
            Ok((Some((manifest, account_key)), account))
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
        let updated_list = if pinned {
            pinned_add_stored(&current_account.pinned_conversations, &new_pinned_proto)
        } else {
            pinned_remove_stored(&current_account.pinned_conversations, &new_pinned_proto)
        };
        if updated_list.len() == current_account.pinned_conversations.len()
            && updated_list
                .iter()
                .zip(current_account.pinned_conversations.iter())
                .all(|(left, right)| same_storage_identity(left, right))
        {
            // 意图已满足（pinned=false 幂等 / 已在列表）：无写面，返回现状。
            return Ok(project_pinned(current_account));
        }
        let updated = account_record_with_pinned(current_account, updated_list);

        let planned = plan_write(
            current_manifest
                .as_ref()
                .map(|(manifest, _)| (manifest, account_item_key_of(manifest).unwrap_or_default())),
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
                        // 置顶面不感知贴纸面：沿用状态文件旧值。
                        sticker_item_keys: None,
                    },
                );
                // 写成功后以本地写后面为准返回（官方 upload 成功语义）。
                return Ok(project_pinned(&updated));
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
                    let account_key = account_item_key_of(&decrypted)
                        .ok_or(StorageSyncError::WriteFailed)?
                        .clone();
                    let records = storage
                        .read_items(vec![account_key.clone()], Some(&decrypted.record_ikm))
                        .await
                        .map_err(|_| StorageSyncError::WriteFailed)?;
                    let server_account = records
                        .iter()
                        .find_map(account_record_of)
                        .cloned()
                        .ok_or(StorageSyncError::WriteFailed)?;
                    current = (Some((decrypted, account_key)), server_account);
                }
            },
        }
    }
}

/// contact 会话 → serviceId 字符串（ACI 优先）：UUID 形状直通 → 本机号走
/// 注册数据 → contacts 数字位匹配（listContacts 同源数据）。
async fn resolve_contact_service_id(
    context: &StorageContext,
    target: &EngineConversationId,
) -> Option<String> {
    let EngineConversationId::Contact(text) = target else {
        return None;
    };
    if presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(text).is_some()
    {
        return Some(text.clone());
    }
    let want: String = text.chars().filter(char::is_ascii_digit).collect();
    if want.is_empty() {
        return None;
    }
    // 本机号：置顶自己（Note to Self）。
    if let Ok(Some(data)) = context.store.load_registration_data().await {
        let own_digits: String = crate::service::e164_string(&data.phone_number)
            .chars()
            .filter(char::is_ascii_digit)
            .collect();
        if own_digits == want {
            return Some(
                presage::libsignal_service::protocol::ServiceId::Aci(data.service_ids.aci.into())
                    .service_id_string(),
            );
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
        let matches = contact
            .phone_number
            .as_ref()
            .map(crate::service::e164_string)
            .map(digits_of)
            .is_some_and(|digits| digits == want);
        matches.then_some(contact.uuid.to_string())
    })
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
        // 新 item 能用同 storage key 解回（legacy 路径，无 record_ikm）。
        let item = &planned.write_operation.insert_item[0];
        let back = StorageService::decrypt_item(&storage_key, item, None).expect("decrypts");
        assert!(account_record_of(&back).is_some());

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
        // modern 路径：新 item 以 record_ikm 解回。
        let item = &planned.write_operation.insert_item[0];
        let back =
            StorageService::decrypt_item(&storage_key, item, Some(&manifest_record.record_ikm))
                .expect("decrypts");
        assert!(account_record_of(&back).is_some());
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
                    payload: RecordPayload::Account(minimal_account_record(Vec::new())),
                },
                RecordUpsert {
                    kind: StorageRecordKind::StickerPack,
                    new_key: vec![0xccu8; 16],
                    replaced_key: Some(vec![0x44u8; 16]),
                    payload: RecordPayload::StickerPack(sticker.clone()),
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

        // insert items：[0] Account（上游解密路径）、[1] Sticker（本地 shim
        // 解密路径 → tag=6 包裹解析还原）。
        let account_item = StorageService::decrypt_item(
            &storage_key,
            &planned.write_operation.insert_item[0],
            Some(&manifest_record.record_ikm),
        )
        .expect("decrypts");
        assert!(account_record_of(&account_item).is_some());
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
                payload: RecordPayload::StickerPack(sticker.clone()),
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
    #[test]
    fn raw_item_crypto_matches_upstream_api() {
        use presage::libsignal_service::master_key::MasterKey;
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[4u8; 32]).expect("key"));
        let raw_id = vec![0xaau8; 16];
        let record = StorageRecord {
            record: Some(storage_record::Record::Account(minimal_account_record(
                Vec::new(),
            ))),
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
