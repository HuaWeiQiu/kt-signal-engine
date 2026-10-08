// SPDX-License-Identifier: AGPL-3.0-only

//! `storage.write` 通用批量 manifest item 写面（P0，KT 扩展方法，compat
//! §3 storage.write 行 + §5「storage 写面 P0」块）。
//!
//! 与既有语义级写面（置顶/拉黑/账号设置/贴纸包，各自封装官方
//! toXxxRecord 语义）并存：本面调用方自带 StorageRecord 明文与 item key，
//! 引擎只做协议面翻译——arm 白名单闸（Contact/GroupV1/GroupV2/
//! StoryDistributionList/StickerPack，Account 归专用写面）→ 幂等比较（读
//! 现存 item 明文）→ WriteOperation
//! 组装（version+1、UPDATE 轮换新 16B item id、旧 key 进 deleteKey）→
//! 单次 PUT。409 版本冲突答 `STORAGE_VERSION_CONFLICT`（`retryable:
//! false`）且**不自动重试**——mutating 语义且引擎不可重推调用方 record，
//! 调用方重读 manifest 后自行重放（语义级写面保留官方 conflictBackOff
//! 自动重放，差异登记见 compat §5）。
//!
//! item 加密/manifest 加密/读通道全部复用 `storage_sync` 既有面
//! （`encrypt_raw_item`/`StorageService::encrypt_manifest`/
//! `read_raw_items` shim + `storage_auth`），manifest version 解析复用
//! `StorageService::manifest`（外层 version 明文可读，409 响应体直接取）。

use std::collections::{HashMap, HashSet};

use base64::Engine as _;
use presage::libsignal_service::master_key::{MasterKey, StorageServiceKey};
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::proto::manifest_record;
use presage::libsignal_service::proto::{ManifestRecord, WriteOperation};
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::store::StateStore;
use prost::Message as _;
use serde_json::{json, Value};

use crate::jsonrpc::RpcError;
use crate::proto::storage::{self as storage_proto, StorageRecord};
use crate::service::storage_sync::{
    account_item_key_of, decrypt_raw_item, encrypt_raw_item, minimal_account_record, new_item_key,
    open_storage_service, put_write_operation, read_raw_items, save_manifest_state, storage_auth,
    ManifestState, PutOutcome, StorageRecordKind, StorageSyncError,
};
use crate::service::StorageContext;

/// 批内单条操作类别（`type` 参数三枚举）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ItemOp {
    Insert,
    Update,
    Delete,
}

impl ItemOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// 批内单条写意图（parse_request 产出，已过形状闸 + arm 白名单闸；
/// record = 调用方 StorageRecord wire bytes，引擎 verbatim 上行不重编码
/// ——vendored proto decode→encode 会丢未知字段，保真只能直传）。
#[derive(Debug, Clone)]
pub(crate) enum StorageItemUpdate {
    Insert {
        key: Vec<u8>,
        record: Vec<u8>,
        kind: StorageRecordKind,
    },
    Update {
        key: Vec<u8>,
        record: Vec<u8>,
        kind: StorageRecordKind,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl StorageItemUpdate {
    pub(crate) fn op(&self) -> ItemOp {
        match self {
            Self::Insert { .. } => ItemOp::Insert,
            Self::Update { .. } => ItemOp::Update,
            Self::Delete { .. } => ItemOp::Delete,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// 单条结果：Written（writtenKey：INSERT/UPDATE = 新 item key——INSERT 为
/// 原 key 回显、UPDATE 为轮换新 key；DELETE 无 writtenKey）/ Noop。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ItemOutcome {
    Written { written_key: Option<Vec<u8>> },
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemResult {
    pub(crate) op: ItemOp,
    pub(crate) key: Vec<u8>,
    pub(crate) outcome: ItemOutcome,
}

/// 写面产出（真实或 fake）：version = 写后 manifest version（全 no-op =
/// 当前 version，从未有 storage 状态 = 0）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteOutcome {
    pub(crate) version: u64,
    pub(crate) results: Vec<ItemResult>,
}

/// plan_item_writes 产出：write_operation = None 表示全 no-op（不 PUT 不
/// bump version）；account_key = 写后 Account item key（播种时为新生成
/// key，供本地 manifest 状态文件登记）。
#[derive(Debug)]
pub(crate) struct ItemWritePlan {
    pub(crate) write_operation: Option<WriteOperation>,
    pub(crate) new_version: u64,
    pub(crate) account_key: Option<Vec<u8>>,
    pub(crate) results: Vec<ItemResult>,
}

/// 参数闸（compat §3 storage.write 行错误列全量；fake 模式同样生效）：
/// accountId 非空字符串、updates 非空数组、条目形状、key 标准 base64 且
/// 解出恰好 16B、INSERT/UPDATE record 必带（base64 → arm 白名单）、
/// DELETE 带 record 拒、批量 key 重复拒。任何不合规 → Err(())。
pub(crate) fn parse_request(
    params: &serde_json::Map<String, Value>,
) -> Result<(String, Vec<StorageItemUpdate>), ()> {
    let account = params
        .get("accountId")
        .and_then(Value::as_str)
        .filter(|account| !account.is_empty())
        .ok_or(())?;
    let entries = params
        .get("updates")
        .and_then(Value::as_array)
        .filter(|entries| !entries.is_empty())
        .ok_or(())?;
    let mut updates = Vec::with_capacity(entries.len());
    let mut seen = HashSet::new();
    for entry in entries {
        let object = entry.as_object().ok_or(())?;
        let op = match object.get("type").and_then(Value::as_str) {
            Some("INSERT") => ItemOp::Insert,
            Some("UPDATE") => ItemOp::Update,
            Some("DELETE") => ItemOp::Delete,
            _ => return Err(()),
        };
        let key = decode_item_key(object.get("key").and_then(Value::as_str).ok_or(())?)?;
        if !seen.insert(key.clone()) {
            return Err(());
        }
        let update = match op {
            ItemOp::Insert | ItemOp::Update => {
                let record = object
                    .get("record")
                    .and_then(Value::as_str)
                    .ok_or(())?
                    .trim();
                let record = base64::engine::general_purpose::STANDARD
                    .decode(record)
                    .map_err(|_| ())?;
                let kind = record_kind_of(&record).ok_or(())?;
                if op == ItemOp::Insert {
                    StorageItemUpdate::Insert { key, record, kind }
                } else {
                    StorageItemUpdate::Update { key, record, kind }
                }
            }
            ItemOp::Delete => {
                match object.get("record") {
                    None | Some(Value::Null) => {}
                    Some(_) => return Err(()),
                }
                StorageItemUpdate::Delete { key }
            }
        };
        updates.push(update);
    }
    Ok((account.to_owned(), updates))
}

/// storage item key：标准 base64（容忍首尾空白）→ 恰好 16 字节。
fn decode_item_key(text: &str) -> Result<Vec<u8>, ()> {
    let key = base64::engine::general_purpose::STANDARD
        .decode(text.trim())
        .map_err(|_| ())?;
    if key.len() == 16 {
        Ok(key)
    } else {
        Err(())
    }
}

/// record 白名单闸（官方 vendored proto 解码 + arm 白名单）：Contact=1 /
/// GroupV1=2 / GroupV2=3 / StoryDistributionList=5 / StickerPack=6 → 类别；
/// Account=4（归专用写面）与其余 arm（CallLink/ChatFolder/
/// NotificationProfile）、解码失败、无 arm → None。
pub(crate) fn record_kind_of(record: &[u8]) -> Option<StorageRecordKind> {
    match StorageRecord::decode(record).ok()?.record? {
        storage_proto::storage_record::Record::Contact(_) => Some(StorageRecordKind::Contact),
        storage_proto::storage_record::Record::GroupV1(_) => Some(StorageRecordKind::GroupV1),
        storage_proto::storage_record::Record::GroupV2(_) => Some(StorageRecordKind::GroupV2),
        storage_proto::storage_record::Record::StoryDistributionList(_) => {
            Some(StorageRecordKind::StoryDistributionList)
        }
        storage_proto::storage_record::Record::StickerPack(_) => {
            Some(StorageRecordKind::StickerPack)
        }
        _ => None,
    }
}

/// 批量写计划纯函数（网络面收在 write_items）：先判定逐条 disposition
///（写入/轮换/删除/幂等 no-op/意图冲突），再一次性组装 WriteOperation。
/// plaintexts = 批内已在 manifest 的 INSERT/UPDATE 目标 key 的现存明文
///（write_items 批量预读；key 在 manifest 但比较面缺明文 → fail-closed
/// 冲突，不静默放行）。
pub(crate) fn plan_item_writes(
    previous: Option<&ManifestRecord>,
    updates: &[StorageItemUpdate],
    plaintexts: &HashMap<Vec<u8>, Vec<u8>>,
    source_device: u32,
    storage_key: &StorageServiceKey,
) -> Result<ItemWritePlan, StorageSyncError> {
    // 从未有过 storage 状态且批内无 INSERT：幂等成功不播种（compat §5
    // 首写分支前半；version 0 = 从未有状态口径）。
    let seeding = previous.is_none();
    if seeding && !updates.iter().any(|update| update.op() == ItemOp::Insert) {
        return Ok(ItemWritePlan {
            write_operation: None,
            new_version: 0,
            account_key: None,
            results: updates
                .iter()
                .map(|update| ItemResult {
                    op: update.op(),
                    key: update.key().to_vec(),
                    outcome: ItemOutcome::Noop,
                })
                .collect(),
        });
    }
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
    let mut insert_item = Vec::new();
    let mut results = Vec::with_capacity(updates.len());
    let mut seeded_account_key: Option<Vec<u8>> = None;
    if seeding {
        // 首写播种：version 1 + 最小 AccountRecord（官方首次启用 Storage
        // Service 同构；Account 记录用官方 vendored proto 编码走 item 加密
        // shim，legacy 路径 record_ikm 空）。Account identifier 排首（与
        // 置顶/拉黑面播种分支同序）。
        let account_key = new_item_key();
        insert_item.push(encrypt_raw_item(
            storage_key,
            account_key.clone(),
            &StorageRecord {
                record: Some(storage_proto::storage_record::Record::Account(
                    minimal_account_record(Vec::new()),
                )),
            }
            .encode_to_vec(),
            record_ikm_opt,
        ));
        identifiers.push(manifest_record::Identifier {
            raw: account_key.clone(),
            r#type: StorageRecordKind::Account.identifier_type(),
        });
        seeded_account_key = Some(account_key);
    }
    for update in updates {
        match update {
            StorageItemUpdate::Insert { key, record, kind } => {
                let existing_type = identifiers
                    .iter()
                    .find(|identifier| identifier.raw == *key)
                    .map(|identifier| identifier.r#type);
                if let Some(existing_type) = existing_type {
                    // 目标 key 已在 manifest：类型一致且明文逐字节相同 →
                    // no-op（整批重放）；类型漂移（含撞 Account 标识——
                    // Account 归专用写面）或内容不同 → 意图冲突 fail-closed
                    //（引擎不静默替换外部记录）。
                    let same_content = match plaintexts.get(key) {
                        Some(stored) => stored.as_slice() == record.as_slice(),
                        // manifest 有标识但比较面缺明文：fail-closed。
                        None => false,
                    };
                    if existing_type != kind.identifier_type() || !same_content {
                        return Err(StorageSyncError::IntentConflict);
                    }
                    results.push(ItemResult {
                        op: ItemOp::Insert,
                        key: key.clone(),
                        outcome: ItemOutcome::Noop,
                    });
                    continue;
                }
                identifiers.push(manifest_record::Identifier {
                    raw: key.clone(),
                    r#type: kind.identifier_type(),
                });
                // record verbatim 加密上行（不 decode→encode，vendored proto
                // 未知的调用方新字段不得静默清零）。
                insert_item.push(encrypt_raw_item(
                    storage_key,
                    key.clone(),
                    record,
                    record_ikm_opt,
                ));
                results.push(ItemResult {
                    op: ItemOp::Insert,
                    key: key.clone(),
                    outcome: ItemOutcome::Written {
                        written_key: Some(key.clone()),
                    },
                });
            }
            StorageItemUpdate::Update { key, record, kind } => {
                let existing_type = identifiers
                    .iter()
                    .find(|identifier| identifier.raw == *key)
                    .map(|identifier| identifier.r#type);
                let Some(existing_type) = existing_type else {
                    // key 不在 manifest：上次写已生效或从未存在 → 重放安全
                    // no-op。
                    results.push(ItemResult {
                        op: ItemOp::Update,
                        key: key.clone(),
                        outcome: ItemOutcome::Noop,
                    });
                    continue;
                };
                if existing_type != kind.identifier_type() {
                    // 同 key 换记录类型：意图冲突 fail-closed（key 是随机
                    // 16B id，类型漂移 = 调用方意图与存储真相失配）。
                    return Err(StorageSyncError::IntentConflict);
                }
                if plaintexts.get(key).is_some_and(|stored| stored == record) {
                    // 同内容：幂等 no-op（不轮换不 bump）。
                    results.push(ItemResult {
                        op: ItemOp::Update,
                        key: key.clone(),
                        outcome: ItemOutcome::Noop,
                    });
                    continue;
                }
                // 内容变化：轮换新 16B item id（官方换记录必轮换
                // generateStorageID——对端以 identifier diff 判变更，同 key
                // 原地改写会被静默跳过），旧 key 进 deleteKey。
                let new_key = new_item_key();
                identifiers.retain(|identifier| identifier.raw != *key);
                delete_keys.push(key.clone());
                identifiers.push(manifest_record::Identifier {
                    raw: new_key.clone(),
                    r#type: kind.identifier_type(),
                });
                insert_item.push(encrypt_raw_item(
                    storage_key,
                    new_key.clone(),
                    record,
                    record_ikm_opt,
                ));
                results.push(ItemResult {
                    op: ItemOp::Update,
                    key: key.clone(),
                    outcome: ItemOutcome::Written {
                        written_key: Some(new_key),
                    },
                });
            }
            StorageItemUpdate::Delete { key } => {
                let existing_type = identifiers
                    .iter()
                    .find(|identifier| identifier.raw == *key)
                    .map(|identifier| identifier.r#type);
                let Some(existing_type) = existing_type else {
                    // key 不在 manifest：无可删除 → 重放安全 no-op。
                    results.push(ItemResult {
                        op: ItemOp::Delete,
                        key: key.clone(),
                        outcome: ItemOutcome::Noop,
                    });
                    continue;
                };
                if existing_type == StorageRecordKind::Account.identifier_type() {
                    // Account item 归专用写面：通用写面不得动账号标识
                    //（manifest 恒有 Account 不变量）。
                    return Err(StorageSyncError::IntentConflict);
                }
                identifiers.retain(|identifier| identifier.raw != *key);
                delete_keys.push(key.clone());
                results.push(ItemResult {
                    op: ItemOp::Delete,
                    key: key.clone(),
                    outcome: ItemOutcome::Written { written_key: None },
                });
            }
        }
    }
    if insert_item.is_empty() && delete_keys.is_empty() {
        // 全 no-op：不 PUT 不 bump version。
        return Ok(ItemWritePlan {
            write_operation: None,
            new_version: previous.map(|manifest| manifest.version).unwrap_or(0),
            account_key: previous.and_then(account_item_key_of),
            results,
        });
    }
    let manifest_record = ManifestRecord {
        version: next_version,
        source_device,
        identifiers,
        record_ikm,
    };
    let manifest = StorageService::encrypt_manifest(storage_key, &manifest_record);
    Ok(ItemWritePlan {
        write_operation: Some(WriteOperation {
            manifest: Some(manifest),
            insert_item,
            delete_key: delete_keys,
            clear_all: false,
        }),
        new_version: next_version,
        account_key: seeded_account_key.or_else(|| previous.and_then(account_item_key_of)),
        results,
    })
}

/// 写编排（storage.write 服务入口）：master key 分支（缺失 + 无 INSERT =
/// 幂等成功不播种；缺失 + 含 INSERT = 播种 master key）→ manifest 读取
///（复用读侧解析）→ 幂等明文批量预读 → plan → 全 no-op 早退 → 单次 PUT
///（无冲突 → 落本地状态返回；409 → VersionConflict 不自动重试）。
pub(crate) async fn write_items(
    context: &StorageContext,
    updates: Vec<StorageItemUpdate>,
) -> Result<WriteOutcome, StorageSyncError> {
    let has_insert = updates.iter().any(|update| update.op() == ItemOp::Insert);
    let master_key = match context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::WriteFailed)?
    {
        Some(key) => key,
        None => {
            if !has_insert {
                return Ok(all_noop_outcome(0, &updates));
            }
            // 首写播种 master key（置顶/账号设置写面同款）。
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
    //（from_master_key 确定性派生，同 master key 两次派生等价）。
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let previous = match storage.manifest().await {
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
    let Some(previous) = previous else {
        // 服务器无 manifest = 从未有过 storage 状态：无 INSERT → 幂等成功
        // 不播种（version 0）；含 INSERT → version 1 首写播种（无现存
        // item 可比较，空 plaintexts）。
        if !has_insert {
            return Ok(all_noop_outcome(0, &updates));
        }
        let plan = plan_item_writes(
            None,
            &updates,
            &HashMap::new(),
            context.device_id,
            &storage_key,
        )?;
        return finish_write(context, plan).await;
    };
    // 幂等比较预读：批内 INSERT/UPDATE 目标 key ∩ manifest 现存标识 → 批量
    // 读现存明文（与读面同一 storage_auth + read_raw_items + 解密 shim）。
    let ikm = previous.record_ikm.as_slice();
    let record_ikm = if ikm.is_empty() { None } else { Some(ikm) };
    let mut plaintexts: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let wanted: Vec<Vec<u8>> = updates
        .iter()
        .filter(|update| update.op() != ItemOp::Delete)
        .map(StorageItemUpdate::key)
        .filter(|key| {
            previous
                .identifiers
                .iter()
                .any(|identifier| &identifier.raw == key)
        })
        .map(|key| key.to_vec())
        .collect();
    if !wanted.is_empty() {
        let auth = storage_auth(&context.push).await?;
        let items = read_raw_items(&context.push, auth, wanted.clone()).await?;
        for item in items {
            let plaintext = decrypt_raw_item(&storage_key, &item, record_ikm)
                .map_err(|_| StorageSyncError::ReadFailed)?;
            plaintexts.insert(item.key, plaintext);
        }
        // manifest 有标识但服务器缺 item = 非法状态，诚实失败（不静默当
        // 「不存在」放行写）。
        if wanted.iter().any(|key| !plaintexts.contains_key(key)) {
            return Err(StorageSyncError::ReadFailed);
        }
    }
    let plan = plan_item_writes(
        Some(&previous),
        &updates,
        &plaintexts,
        context.device_id,
        &storage_key,
    )?;
    let Some(write_operation) = plan.write_operation else {
        return Ok(WriteOutcome {
            version: plan.new_version,
            results: plan.results,
        });
    };
    let auth = storage_auth(&context.push).await?;
    match put_write_operation(&context.push, auth, &write_operation).await? {
        PutOutcome {
            conflict_manifest: None,
        } => {
            save_manifest_state(
                &context.account_dir,
                &ManifestState {
                    version: plan.new_version,
                    account_item_key: plan
                        .account_key
                        .map(|key| base64::engine::general_purpose::STANDARD.encode(key)),
                    // 通用写面不感知贴纸面：沿用状态文件旧值（读侧以服务器
                    // manifest 为真相源）。
                    sticker_item_keys: None,
                },
            );
            Ok(WriteOutcome {
                version: plan.new_version,
                results: plan.results,
            })
        }
        PutOutcome {
            conflict_manifest: Some(server_manifest),
        } => Err(StorageSyncError::VersionConflict {
            // 409 响应体 StorageManifest 外层 version 明文可读。
            server_version: Some(server_manifest.version),
        }),
    }
}

/// PUT 成功收尾（无 manifest 首写分支专用；manifest 加密在 plan 已完成，
/// 这里只做单次 PUT + 本地状态登记）。
async fn finish_write(
    context: &StorageContext,
    plan: ItemWritePlan,
) -> Result<WriteOutcome, StorageSyncError> {
    let write_operation = plan
        .write_operation
        .expect("first-write plan always carries a write operation");
    let auth = storage_auth(&context.push).await?;
    match put_write_operation(&context.push, auth, &write_operation).await? {
        PutOutcome {
            conflict_manifest: None,
        } => {
            save_manifest_state(
                &context.account_dir,
                &ManifestState {
                    version: plan.new_version,
                    account_item_key: plan
                        .account_key
                        .map(|key| base64::engine::general_purpose::STANDARD.encode(key)),
                    sticker_item_keys: None,
                },
            );
            Ok(WriteOutcome {
                version: plan.new_version,
                results: plan.results,
            })
        }
        PutOutcome {
            conflict_manifest: Some(server_manifest),
        } => Err(StorageSyncError::VersionConflict {
            server_version: Some(server_manifest.version),
        }),
    }
}

fn all_noop_outcome(version: u64, updates: &[StorageItemUpdate]) -> WriteOutcome {
    WriteOutcome {
        version,
        results: updates
            .iter()
            .map(|update| ItemResult {
                op: update.op(),
                key: update.key().to_vec(),
                outcome: ItemOutcome::Noop,
            })
            .collect(),
    }
}

/// fake 模式形状桩（参数闸已在 parse_request 全量生效后到达）：version 0、
/// 逐条回显（INSERT/UPDATE writtenKey = 原 key，DELETE 无 writtenKey，无
/// no-op 标注——桩不模拟幂等面）。
pub(crate) fn fake_outcome(updates: &[StorageItemUpdate]) -> WriteOutcome {
    WriteOutcome {
        version: 0,
        results: updates
            .iter()
            .map(|update| {
                let outcome = match update {
                    StorageItemUpdate::Insert { key, .. }
                    | StorageItemUpdate::Update { key, .. } => ItemOutcome::Written {
                        written_key: Some(key.clone()),
                    },
                    StorageItemUpdate::Delete { .. } => ItemOutcome::Written { written_key: None },
                };
                ItemResult {
                    op: update.op(),
                    key: update.key().to_vec(),
                    outcome,
                }
            })
            .collect(),
    }
}

/// STORAGE_VERSION_CONFLICT 结构化错误（compat §3 storage.write 行）：
/// `data.retryable = false`（不自动重试，调用方重读 manifest 后重放），
/// `data.serverVersion` = 409 响应体版本（取不到缺席）。
pub(crate) fn version_conflict_rpc(server_version: Option<u64>) -> RpcError {
    let mut data = serde_json::Map::new();
    data.insert("retryable".to_owned(), Value::Bool(false));
    if let Some(version) = server_version {
        data.insert("serverVersion".to_owned(), json!(version));
    }
    RpcError::with_data(-1, "STORAGE_VERSION_CONFLICT", Value::Object(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage_sync::account_record_of_wire;

    fn storage_key() -> StorageServiceKey {
        use presage::libsignal_service::master_key::MasterKey;
        StorageServiceKey::from_master_key(&MasterKey::from_slice(&[3u8; 32]).expect("key"))
    }

    fn contact_wire(aci: &str) -> Vec<u8> {
        StorageRecord {
            record: Some(storage_proto::storage_record::Record::Contact(
                storage_proto::ContactRecord {
                    aci: aci.to_owned(),
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec()
    }

    fn sticker_wire(pack_id: &[u8]) -> Vec<u8> {
        StorageRecord {
            record: Some(storage_proto::storage_record::Record::StickerPack(
                storage_proto::StickerPackRecord {
                    pack_id: pack_id.to_vec(),
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec()
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn key16(byte: u8) -> Vec<u8> {
        vec![byte; 16]
    }

    fn account_identifier() -> manifest_record::Identifier {
        manifest_record::Identifier {
            raw: key16(0x22),
            r#type: manifest_record::identifier::Type::Account as i32,
        }
    }

    /// parse_request：三操作 happy path + fail-closed 矩阵（缺 accountId/
    /// 空串/非串、缺 updates/空批/非数组、条目非对象、type 未知、key 缺失/
    /// 非 base64/15B、INSERT 缺 record、UPDATE record null、DELETE 带
    /// record、record 垃圾/Account arm、批量重复 key）。
    #[test]
    fn parse_request_shape_gates() {
        let delete = json!({ "type": "DELETE", "key": b64(&key16(0x03)) });
        let insert = json!({
            "type": "INSERT",
            "key": b64(&key16(0x01)),
            "record": b64(&contact_wire("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")),
        });
        let update = json!({
            "type": "UPDATE",
            "key": b64(&key16(0x02)),
            "record": b64(&contact_wire("other")),
        });
        let (account, updates) = parse_request(
            json!({
                "accountId": "acct-1",
                "updates": [insert.clone(), update.clone(), delete.clone()],
            })
            .as_object()
            .expect("object"),
        )
        .expect("parses");
        assert_eq!(account, "acct-1");
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[0].op(), ItemOp::Insert);
        assert_eq!(updates[0].key(), key16(0x01).as_slice());
        assert!(matches!(
            &updates[0],
            StorageItemUpdate::Insert {
                kind: StorageRecordKind::Contact,
                ..
            }
        ));
        assert_eq!(updates[1].op(), ItemOp::Update);
        assert_eq!(updates[2].op(), ItemOp::Delete);
        assert_eq!(updates[2].key(), key16(0x03).as_slice());

        let bad = [
            json!({ "accountId": "acct-1" }),
            json!({ "accountId": "acct-1", "updates": [] }),
            json!({ "accountId": "acct-1", "updates": "nope" }),
            json!({ "updates": [delete.clone()] }),
            json!({ "accountId": "", "updates": [delete.clone()] }),
            json!({ "accountId": 7, "updates": [delete.clone()] }),
            json!({ "accountId": "acct-1", "updates": [7] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "UPSERT", "key": b64(&key16(0x01)) }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "DELETE" }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "DELETE", "key": "!!!not-base64!!!" }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "DELETE", "key": b64(&[0x01; 15]) }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "INSERT", "key": b64(&key16(0x01)) }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "UPDATE", "key": b64(&key16(0x02)), "record": Value::Null }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "DELETE", "key": b64(&key16(0x03)), "record": b64(&contact_wire("x")) }] }),
            json!({ "accountId": "acct-1", "updates": [{ "type": "INSERT", "key": b64(&key16(0x01)), "record": "AAAA" }] }),
            json!({
                "accountId": "acct-1",
                "updates": [{
                    "type": "INSERT",
                    "key": b64(&key16(0x01)),
                    "record": b64(&StorageRecord {
                        record: Some(storage_proto::storage_record::Record::Account(
                            minimal_account_record(Vec::new()),
                        )),
                    }
                    .encode_to_vec()),
                }],
            }),
            json!({ "accountId": "acct-1", "updates": [delete.clone(), { "type": "DELETE", "key": b64(&key16(0x03)) }] }),
        ];
        for params in bad {
            assert!(
                parse_request(params.as_object().expect("object")).is_err(),
                "must fail closed: {params}",
            );
        }
    }

    /// record 白名单闸：五类白名单 arm 通过；Account/CallLink/垃圾 bytes/
    /// 空 bytes 拒（StoryDistributionList=5 随名单面批放开，其余 arm
    /// fail-closed 不变）。
    #[test]
    fn record_kind_of_whitelist() {
        assert_eq!(
            record_kind_of(&contact_wire("aci")),
            Some(StorageRecordKind::Contact)
        );
        assert_eq!(
            record_kind_of(
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::GroupV1(
                        storage_proto::GroupV1Record::default(),
                    )),
                }
                .encode_to_vec()
            ),
            Some(StorageRecordKind::GroupV1)
        );
        assert_eq!(
            record_kind_of(
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::GroupV2(
                        storage_proto::GroupV2Record::default(),
                    )),
                }
                .encode_to_vec()
            ),
            Some(StorageRecordKind::GroupV2)
        );
        assert_eq!(
            record_kind_of(
                &StorageRecord {
                    record: Some(
                        storage_proto::storage_record::Record::StoryDistributionList(
                            storage_proto::StoryDistributionListRecord::default(),
                        )
                    ),
                }
                .encode_to_vec()
            ),
            Some(StorageRecordKind::StoryDistributionList)
        );
        assert_eq!(
            record_kind_of(&sticker_wire(&[7u8; 16])),
            Some(StorageRecordKind::StickerPack)
        );
        // Account 归专用写面；CallLink 白名单外。
        assert_eq!(
            record_kind_of(
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::Account(
                        minimal_account_record(Vec::new()),
                    )),
                }
                .encode_to_vec()
            ),
            None
        );
        assert_eq!(
            record_kind_of(
                &StorageRecord {
                    record: Some(storage_proto::storage_record::Record::CallLink(
                        storage_proto::CallLinkRecord::default(),
                    )),
                }
                .encode_to_vec()
            ),
            None
        );
        assert_eq!(record_kind_of(b"garbage"), None);
        assert_eq!(record_kind_of(b""), None);
    }

    /// 首写播种：无 manifest + 含 INSERT → version 1、Account identifier
    /// 排首、最小 AccountRecord item 入批、无 deleteKey；批内 DELETE 未中
    /// → no-op；item 明文逐字节 = 调用方 record（verbatim 保真）。
    #[test]
    fn plan_first_write_seeds_account_and_noop_delete() {
        let storage_key = storage_key();
        let contact_key = key16(0x01);
        let record = contact_wire("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae");
        let plan = plan_item_writes(
            None,
            &[
                StorageItemUpdate::Insert {
                    key: contact_key.clone(),
                    record: record.clone(),
                    kind: StorageRecordKind::Contact,
                },
                StorageItemUpdate::Delete { key: key16(0x09) },
            ],
            &HashMap::new(),
            2,
            &storage_key,
        )
        .expect("plans");
        assert_eq!(plan.new_version, 1);
        let write_operation = plan.write_operation.expect("first write carries operation");
        assert_eq!(write_operation.insert_item.len(), 2);
        assert!(write_operation.delete_key.is_empty());
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        assert_eq!(manifest_record.version, 1);
        assert_eq!(manifest_record.source_device, 2);
        assert_eq!(manifest_record.identifiers.len(), 2);
        assert_eq!(
            manifest_record.identifiers[0].r#type,
            StorageRecordKind::Account.identifier_type()
        );
        assert_eq!(
            plan.account_key,
            Some(manifest_record.identifiers[0].raw.clone())
        );
        assert_eq!(manifest_record.identifiers[1].raw, contact_key);
        let contact_item = write_operation
            .insert_item
            .iter()
            .find(|item| item.key == contact_key)
            .expect("contact item");
        assert_eq!(
            decrypt_raw_item(&storage_key, contact_item, None).expect("decrypts"),
            record
        );
        let account_item = write_operation
            .insert_item
            .iter()
            .find(|item| item.key != contact_key)
            .expect("account item");
        assert!(account_record_of_wire(
            &decrypt_raw_item(&storage_key, account_item, None).expect("decrypts")
        )
        .is_some());
        assert_eq!(plan.results.len(), 2);
        assert_eq!(
            plan.results[0],
            ItemResult {
                op: ItemOp::Insert,
                key: contact_key,
                outcome: ItemOutcome::Written {
                    written_key: Some(key16(0x01)),
                },
            }
        );
        assert_eq!(plan.results[1].outcome, ItemOutcome::Noop);
    }

    /// 既有 manifest 语义：UPDATE 轮换（旧 key 进 deleteKey、新随机 key 入
    /// identifiers、明文 verbatim）、INSERT 新类别入批、DELETE 摘除、
    /// record_ikm 透传且新 item 以 ikm 解回、Account 标识不动。
    #[test]
    fn plan_existing_manifest_semantics() {
        let storage_key = storage_key();
        let old = ManifestRecord {
            version: 4,
            source_device: 2,
            identifiers: vec![
                manifest_record::Identifier {
                    raw: key16(0x11),
                    r#type: manifest_record::identifier::Type::Contact as i32,
                },
                account_identifier(),
                manifest_record::Identifier {
                    raw: key16(0x44),
                    r#type: crate::service::storage_sticker::STICKER_PACK_IDENTIFIER_TYPE,
                },
            ],
            record_ikm: vec![0x33u8; 32],
        };
        let new_record = contact_wire("other-aci");
        let sticker_record = sticker_wire(&[7u8; 16]);
        let plan = plan_item_writes(
            Some(&old),
            &[
                StorageItemUpdate::Update {
                    key: key16(0x11),
                    record: new_record.clone(),
                    kind: StorageRecordKind::Contact,
                },
                StorageItemUpdate::Insert {
                    key: key16(0x55),
                    record: sticker_record.clone(),
                    kind: StorageRecordKind::StickerPack,
                },
                StorageItemUpdate::Delete { key: key16(0x44) },
            ],
            &HashMap::new(),
            2,
            &storage_key,
        )
        .expect("plans");
        assert_eq!(plan.new_version, 5);
        let write_operation = plan.write_operation.expect("write");
        assert_eq!(write_operation.delete_key, vec![key16(0x11), key16(0x44)]);
        assert_eq!(write_operation.insert_item.len(), 2);
        let manifest_record = StorageService::decrypt_manifest(
            &storage_key,
            write_operation.manifest.as_ref().expect("manifest"),
        )
        .expect("decrypts");
        assert_eq!(manifest_record.record_ikm, vec![0x33u8; 32]);
        let identifiers = &manifest_record.identifiers;
        assert_eq!(identifiers.len(), 3);
        assert!(!identifiers
            .iter()
            .any(|identifier| identifier.raw == key16(0x11)));
        assert!(!identifiers
            .iter()
            .any(|identifier| identifier.raw == key16(0x44)));
        assert!(identifiers.contains(&account_identifier()));
        assert!(identifiers
            .iter()
            .any(|identifier| identifier.raw == key16(0x55)
                && identifier.r#type
                    == crate::service::storage_sticker::STICKER_PACK_IDENTIFIER_TYPE));
        let rotated = &plan.results[0];
        let rotated_key = match &rotated.outcome {
            ItemOutcome::Written {
                written_key: Some(key),
            } => key.clone(),
            other => panic!("update rotates a fresh item id, got {other:?}"),
        };
        assert_ne!(rotated_key, key16(0x11));
        assert_eq!(rotated_key.len(), 16);
        assert!(identifiers
            .iter()
            .any(|identifier| identifier.raw == rotated_key));
        let rotated_item = write_operation
            .insert_item
            .iter()
            .find(|item| item.key == rotated_key)
            .expect("rotated item");
        assert_eq!(
            decrypt_raw_item(
                &storage_key,
                rotated_item,
                Some(&manifest_record.record_ikm)
            )
            .expect("decrypts"),
            new_record
        );
        let sticker_item = write_operation
            .insert_item
            .iter()
            .find(|item| item.key == key16(0x55))
            .expect("sticker item");
        assert_eq!(
            decrypt_raw_item(
                &storage_key,
                sticker_item,
                Some(&manifest_record.record_ikm)
            )
            .expect("decrypts"),
            sticker_record
        );
        assert_eq!(
            plan.results[1],
            ItemResult {
                op: ItemOp::Insert,
                key: key16(0x55),
                outcome: ItemOutcome::Written {
                    written_key: Some(key16(0x55)),
                },
            }
        );
        assert_eq!(
            plan.results[2],
            ItemResult {
                op: ItemOp::Delete,
                key: key16(0x44),
                outcome: ItemOutcome::Written { written_key: None },
            }
        );
        assert_eq!(plan.account_key.as_deref(), Some(key16(0x22).as_slice()));
    }

    /// 全 no-op：UPDATE 同内容 + DELETE 未中 → 无 WriteOperation、version
    /// 保持、逐条 no-op。
    #[test]
    fn plan_all_noop_makes_no_write() {
        let storage_key = storage_key();
        let record = contact_wire("same");
        let manifest = ManifestRecord {
            version: 4,
            source_device: 2,
            identifiers: vec![
                manifest_record::Identifier {
                    raw: key16(0x11),
                    r#type: manifest_record::identifier::Type::Contact as i32,
                },
                account_identifier(),
            ],
            record_ikm: Vec::new(),
        };
        let mut plaintexts = HashMap::new();
        plaintexts.insert(key16(0x11), record.clone());
        let plan = plan_item_writes(
            Some(&manifest),
            &[
                StorageItemUpdate::Update {
                    key: key16(0x11),
                    record: record.clone(),
                    kind: StorageRecordKind::Contact,
                },
                StorageItemUpdate::Delete { key: key16(0x99) },
            ],
            &plaintexts,
            2,
            &storage_key,
        )
        .expect("plans");
        assert!(plan.write_operation.is_none());
        assert_eq!(plan.new_version, 4);
        assert_eq!(plan.results.len(), 2);
        assert!(plan
            .results
            .iter()
            .all(|result| result.outcome == ItemOutcome::Noop));
    }

    /// 意图冲突 fail-closed：INSERT 撞已存在 key 内容不同、UPDATE 换记录
    /// 类型、DELETE Account 标识；INSERT 同 key 同内容仍幂等 no-op。
    #[test]
    fn plan_intent_conflicts_fail_closed() {
        let storage_key = storage_key();
        let manifest = ManifestRecord {
            version: 4,
            source_device: 2,
            identifiers: vec![
                manifest_record::Identifier {
                    raw: key16(0x11),
                    r#type: manifest_record::identifier::Type::Contact as i32,
                },
                account_identifier(),
            ],
            record_ikm: Vec::new(),
        };
        let stored = contact_wire("stored");
        let mut plaintexts = HashMap::new();
        plaintexts.insert(key16(0x11), stored.clone());
        let conflict = plan_item_writes(
            Some(&manifest),
            &[StorageItemUpdate::Insert {
                key: key16(0x11),
                record: contact_wire("different"),
                kind: StorageRecordKind::Contact,
            }],
            &plaintexts,
            2,
            &storage_key,
        )
        .expect_err("conflicting insert rejected");
        assert!(matches!(conflict, StorageSyncError::IntentConflict));
        let drift = plan_item_writes(
            Some(&manifest),
            &[StorageItemUpdate::Update {
                key: key16(0x11),
                record: sticker_wire(&[7u8; 16]),
                kind: StorageRecordKind::StickerPack,
            }],
            &plaintexts,
            2,
            &storage_key,
        )
        .expect_err("type drift rejected");
        assert!(matches!(drift, StorageSyncError::IntentConflict));
        let account = plan_item_writes(
            Some(&manifest),
            &[StorageItemUpdate::Delete { key: key16(0x22) }],
            &plaintexts,
            2,
            &storage_key,
        )
        .expect_err("account identifier protected");
        assert!(matches!(account, StorageSyncError::IntentConflict));
        let idempotent = plan_item_writes(
            Some(&manifest),
            &[StorageItemUpdate::Insert {
                key: key16(0x11),
                record: stored,
                kind: StorageRecordKind::Contact,
            }],
            &plaintexts,
            2,
            &storage_key,
        )
        .expect("same-content insert replays as no-op");
        assert!(idempotent.write_operation.is_none());
        assert_eq!(idempotent.results.len(), 1);
        assert_eq!(idempotent.results[0].outcome, ItemOutcome::Noop);
    }

    /// STORAGE_VERSION_CONFLICT 形状：retryable=false 恒在，serverVersion
    /// 取到才在。
    #[test]
    fn version_conflict_rpc_shape() {
        let error = version_conflict_rpc(Some(7));
        assert_eq!(error.code, -1);
        assert_eq!(error.message, "STORAGE_VERSION_CONFLICT");
        assert_eq!(
            error.data,
            Some(json!({ "retryable": false, "serverVersion": 7 }))
        );
        let bare = version_conflict_rpc(None);
        assert_eq!(bare.data, Some(json!({ "retryable": false })));
    }

    /// fake 桩：version 0、逐条回显（INSERT/UPDATE writtenKey = 原 key，
    /// DELETE 无 writtenKey，无 no-op 标注）。
    #[test]
    fn fake_outcome_echoes_written_keys() {
        let outcome = fake_outcome(&[
            StorageItemUpdate::Insert {
                key: key16(0x01),
                record: contact_wire("x"),
                kind: StorageRecordKind::Contact,
            },
            StorageItemUpdate::Update {
                key: key16(0x02),
                record: sticker_wire(&[7u8; 16]),
                kind: StorageRecordKind::StickerPack,
            },
            StorageItemUpdate::Delete { key: key16(0x03) },
        ]);
        assert_eq!(outcome.version, 0);
        assert_eq!(outcome.results.len(), 3);
        assert_eq!(
            outcome.results[0],
            ItemResult {
                op: ItemOp::Insert,
                key: key16(0x01),
                outcome: ItemOutcome::Written {
                    written_key: Some(key16(0x01)),
                },
            }
        );
        assert_eq!(
            outcome.results[1],
            ItemResult {
                op: ItemOp::Update,
                key: key16(0x02),
                outcome: ItemOutcome::Written {
                    written_key: Some(key16(0x02)),
                },
            }
        );
        assert_eq!(
            outcome.results[2],
            ItemResult {
                op: ItemOp::Delete,
                key: key16(0x03),
                outcome: ItemOutcome::Written { written_key: None },
            }
        );
    }
}
