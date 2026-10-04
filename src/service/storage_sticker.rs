// SPDX-License-Identifier: AGPL-3.0-only

//! 贴纸包安装状态 Storage Service 同步（P5 扩面）。
//!
//! 真相源 = 独立 StorageRecord（`Record::StickerPack`）：官方
//! SignalStorage.proto StickerPackRecord :343-362（packId 16B/packKey 32B/
//! position/deletedAtTimestamp）与 ManifestRecord.Identifier.Type.
//! STICKER_PACK = 6（:54）；行为参照 Signal-Desktop
//! `ts/services/storageRecordOps.preload.ts`（toStickerPackRecord :822-844、
//! mergeStickerPackRecord :2460-2611），不抄码。
//!
//! 官方写语义：安装 → 新 16B item key 写 `{packId, packKey, position}`
//! （position 未传时本地按当前已安装 MAX+1 分配，服务器记录之）；卸载 →
//! 新 16B key 写墓碑 `{packId, deletedAtTimestamp}`（packKey/position 不
//! 写）+ 旧 key 进 deleteKey——「新 key 插入 + 旧 key 删除」与官方更新语义
//! 同构，不物理删旧 record。409 采纳重放同置顶面（conflictBackOff
//! [1s,5s,30s]，`storage_sync::conflict_decision` 复用不重写）。
//!
//! v1 单通道边界：官方另有 legacy sync message（SyncMessage
//! stickerPackOperation）双保险通道，本引擎不做——贴纸安装状态以 Storage
//! Service 为唯一真相源（compat §5 登记）。
//!
//! proto 收敛点：钉定 presage rev 的 StorageService.proto 落后（无
//! STICKER_PACK=6/StickerPackRecord），本模块自带该消息的 prost 定义与
//! StorageRecord tag=6 线格式包裹，item 加解密走 `storage_sync` 本地 shim；
//! 上游 proto 补齐后整段替换。读面坏条目跳过不炸面（引擎 daemon 零 stderr
//! 日志纪律，compat §1——「warn」落地为跳过 + compat §5 登记）。

use base64::Engine as _;
use presage::libsignal_service::master_key::{MasterKey, StorageServiceKey};
use presage::libsignal_service::prelude::{PushService, ServiceError};
use presage::libsignal_service::proto::{ManifestRecord, StorageItem};
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::store::StateStore;

use crate::service::storage_sync::{
    account_item_key_of, conflict_decision, decrypt_raw_item, minimal_account_record, new_item_key,
    open_storage_service, plan_record_writes, put_write_operation, read_raw_items,
    save_manifest_state, storage_auth, ConflictDecision, ManifestState, PutOutcome, RecordPayload,
    RecordUpsert, StorageRecordKind, StorageSyncError,
};
use crate::service::StorageContext;

/// ManifestRecord.Identifier.Type.STICKER_PACK wire 值（官方
/// SignalStorage.proto :54；钉定 presage rev 的枚举无该值，全程以 raw i32
/// 读写，读面同口径识别）。
pub(crate) const STICKER_PACK_IDENTIFIER_TYPE: i32 = 6;

/// StickerPackRecord（官方 SignalStorage.proto :343-362 字段面；钉定
/// presage rev 无此消息，prost 定义同构本地持有，上游补齐后替换）。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(crate) struct StickerPackRecord {
    /// 16 字节 packId。
    #[prost(bytes = "vec", tag = "1")]
    pub(crate) pack_id: Vec<u8>,
    /// 32 字节 packKey（安装态；墓碑不写）。
    #[prost(bytes = "vec", tag = "2")]
    pub(crate) pack_key: Vec<u8>,
    /// 安装顺序（官方排序 = position 升序 + packId hex 字典序）。
    #[prost(uint32, tag = "3")]
    pub(crate) position: u32,
    /// 墓碑毫秒时间戳（非零 = 已卸载，packKey/position 语义作废）。
    #[prost(uint64, tag = "4")]
    pub(crate) deleted_at_timestamp: u64,
}

/// StorageRecord tag=6 线格式镜像（只声明 sticker arm；prost 解码天然跳过
/// 其余/未知字段，与官方 StorageRecord 单层 oneof 兼容）。
#[derive(Clone, PartialEq, prost::Message)]
struct StickerRecordEnvelope {
    #[prost(message, optional, tag = "6")]
    sticker_pack: Option<StickerPackRecord>,
}

/// StickerPackRecord → StorageRecord 线格式 bytes（tag 6 LEN 包裹）。
pub(crate) fn sticker_record_wire(record: &StickerPackRecord) -> Vec<u8> {
    use prost::Message as _;
    StickerRecordEnvelope {
        sticker_pack: Some(record.clone()),
    }
    .encode_to_vec()
}

/// StorageRecord 明文 bytes → StickerPackRecord（非贴纸记录/缺 tag 6/内层
/// 解码失败 → None，调用方跳过）。
pub(crate) fn sticker_record_of(plaintext: &[u8]) -> Option<StickerPackRecord> {
    use prost::Message as _;
    StickerRecordEnvelope::decode(plaintext).ok()?.sticker_pack
}

/// 贴纸包安装状态投影（引擎口径，getStickerPackSyncs/setStickerPackSync
/// 返回形状的数据面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StickerPackSync {
    /// 16 字节 packId（JSON 投影 32 位小写 hex）。
    pub(crate) pack_id: Vec<u8>,
    /// 32 字节 packKey（JSON 投影标准 base64；墓碑 → None）。
    pub(crate) pack_key: Option<Vec<u8>>,
    pub(crate) position: Option<u32>,
    pub(crate) deleted_at_timestamp_ms: Option<u64>,
}

/// StickerPackRecord → 投影（官方 toStickerPackRecord 同构）：墓碑 →
/// packKey/position 置空；packId 空或安装态缺 packKey = malformed → None
/// （调用方跳过，不炸整面）。
pub(crate) fn project_sticker_pack(record: &StickerPackRecord) -> Option<StickerPackSync> {
    if record.pack_id.is_empty() {
        return None;
    }
    if record.deleted_at_timestamp > 0 {
        return Some(StickerPackSync {
            pack_id: record.pack_id.clone(),
            pack_key: None,
            position: None,
            deleted_at_timestamp_ms: Some(record.deleted_at_timestamp),
        });
    }
    if record.pack_key.is_empty() {
        return None;
    }
    Some(StickerPackSync {
        pack_id: record.pack_id.clone(),
        pack_key: Some(record.pack_key.clone()),
        position: Some(record.position),
        deleted_at_timestamp_ms: None,
    })
}

/// 写面意图（类型化安装/卸载；方法层已 fail-closed：install 必带 32B
/// packKey，uninstall 的墓碑时间戳由编排层以引擎时钟注入）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StickerPackWrite {
    Install {
        pack_id: Vec<u8>,
        pack_key: [u8; 32],
        /// None = 本地 MAX+1 分配（官方安装动作同源）。
        position: Option<u32>,
    },
    Uninstall {
        pack_id: Vec<u8>,
    },
}

impl StickerPackWrite {
    pub(crate) fn pack_id(&self) -> &[u8] {
        match self {
            Self::Install { pack_id, .. } | Self::Uninstall { pack_id } => pack_id,
        }
    }
}

/// 云上贴纸包一条（manifest key + 投影；写面定位 replaced key 用）。
struct CloudPack {
    key: Vec<u8>,
    sync: StickerPackSync,
}

/// 本地意图 → 目标 StickerPackRecord（官方 toStickerPackRecord 同构）；
/// 云态已满足意图（幂等）→ None = 无写。
fn sticker_record_for(
    current: Option<&StickerPackSync>,
    intent: &StickerPackWrite,
    positions: &[u32],
    now_ms: u64,
) -> Option<StickerPackRecord> {
    match intent {
        StickerPackWrite::Install {
            pack_id,
            pack_key,
            position,
        } => {
            // 同 key 已安装（非墓碑）。
            let same_installed = current.filter(|current| {
                current.deleted_at_timestamp_ms.is_none()
                    && current.pack_key.as_deref() == Some(pack_key.as_slice())
            });
            // 幂等：已装同 key 且未要求挪位 → 无写（官方 storageNeedsSync=
            // false 同语义）。
            if let Some(current) = same_installed {
                if position.is_none() || current.position == *position {
                    return None;
                }
            }
            // 未显式给位：已在装 → 保持现位；否则本地 MAX+1（官方安装动作
            // 同源：max_position + 1）。
            let position = position.unwrap_or_else(|| {
                same_installed
                    .and_then(|current| current.position)
                    .unwrap_or_else(|| {
                        positions
                            .iter()
                            .max()
                            .map_or(0, |max| max.saturating_add(1))
                    })
            });
            Some(StickerPackRecord {
                pack_id: pack_id.clone(),
                pack_key: pack_key.to_vec(),
                position,
                deleted_at_timestamp: 0,
            })
        }
        StickerPackWrite::Uninstall { pack_id } => match current {
            // 云上无此包 / 已墓碑 → 幂等无写。
            None => None,
            Some(current) if current.deleted_at_timestamp_ms.is_some() => None,
            Some(_) => Some(StickerPackRecord {
                pack_id: pack_id.clone(),
                pack_key: Vec::new(),
                position: 0,
                deleted_at_timestamp: now_ms,
            }),
        },
    }
}

/// 单条 item → CloudPack（解密/解析/投影任一失败 → None，调用方跳过）。
fn cloud_pack_of(
    storage_key: &StorageServiceKey,
    item: &StorageItem,
    record_ikm: Option<&[u8]>,
) -> Option<CloudPack> {
    let plaintext = decrypt_raw_item(storage_key, item, record_ikm).ok()?;
    let sync = project_sticker_pack(&sticker_record_of(&plaintext)?)?;
    Some(CloudPack {
        key: item.key.clone(),
        sync,
    })
}

/// manifest 全部 STICKER_PACK=6 identifier → 批量原样读 → 解密/解析/投影
/// （存储序 = manifest identifiers 首现序；单条坏 item 跳过不炸面）。
async fn read_cloud_packs(
    push: &PushService,
    storage_key: &StorageServiceKey,
    manifest: Option<&ManifestRecord>,
) -> Result<Vec<CloudPack>, StorageSyncError> {
    let Some(manifest) = manifest else {
        return Ok(Vec::new());
    };
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| identifier.r#type == STICKER_PACK_IDENTIFIER_TYPE)
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
        if let Some(pack) = cloud_pack_of(storage_key, item, record_ikm) {
            out.push(pack);
        }
    }
    Ok(out)
}

/// 读面编排：master key → manifest → 全部贴纸包安装状态（存储序）。
/// master key 缺失或 manifest 404（账号从未有过 storage 状态）→ 空列表。
pub(crate) async fn read_sticker_packs(
    context: &StorageContext,
) -> Result<Vec<StickerPackSync>, StorageSyncError> {
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
    Ok(
        read_cloud_packs(&context.push, &storage_key, Some(&manifest))
            .await?
            .into_iter()
            .map(|pack| pack.sync)
            .collect(),
    )
}

/// 写面编排（官方 upload 语义）：读 manifest + 全部贴纸 item → 定位
/// replaced key → 换入新 key/换出旧 key → PUT → 409 采纳重放
/// （conflictBackOff 3 次）。manifest 404 → 同置顶面首写分支（compat §5）：
/// master key 播种 + version 1 最小 AccountRecord（服务器要求 manifest 恒
/// 含一条 Account 记录）与贴纸记录同批写入。
pub(crate) async fn write_sticker_pack(
    context: &StorageContext,
    intent: &StickerPackWrite,
    now_ms: u64,
) -> Result<Vec<StickerPackSync>, StorageSyncError> {
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
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let storage = open_storage_service(
        context.push.clone(),
        StorageServiceKey::from_master_key(&master_key),
    )
    .await?;
    let mut manifest: Option<ManifestRecord> = match storage.manifest().await {
        Ok(manifest) => {
            // 有 manifest 无 Account 标识 = 非法状态（同置顶面口径）。
            if account_item_key_of(&manifest).is_none() {
                return Err(StorageSyncError::WriteFailed);
            }
            Some(manifest)
        }
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => None,
        Err(_) => return Err(StorageSyncError::Unavailable),
    };

    // 冲突重放循环：每轮从「当前服务器状态 + 本地意图」重建 WriteOperation。
    let mut retries: u32 = 0;
    loop {
        let current = read_cloud_packs(&context.push, &storage_key, manifest.as_ref()).await?;
        let target = current
            .iter()
            .find(|pack| pack.sync.pack_id == intent.pack_id());
        let positions: Vec<u32> = current
            .iter()
            .filter_map(|pack| pack.sync.position)
            .collect();
        let Some(new_record) =
            sticker_record_for(target.map(|pack| &pack.sync), intent, &positions, now_ms)
        else {
            // 意图已满足（云态幂等）：返回当前云态投影（存储序）。
            return Ok(current.into_iter().map(|pack| pack.sync).collect());
        };
        let replaced_key = target.map(|pack| pack.key.clone());

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
                    payload: RecordPayload::Account(minimal_account_record(Vec::new())),
                });
                None
            }
        };
        let new_key = new_item_key();
        upserts.push(RecordUpsert {
            kind: StorageRecordKind::StickerPack,
            new_key: new_key.clone(),
            replaced_key,
            payload: RecordPayload::StickerPack(new_record.clone()),
        });
        let planned = plan_record_writes(previous, &upserts, context.device_id, &storage_key);

        let auth = storage_auth(&context.push).await?;
        match put_write_operation(&context.push, auth, &planned.write_operation).await? {
            PutOutcome {
                conflict_manifest: None,
            } => {
                // 本地状态登记（best-effort 排查面）：version / Account key /
                // 全部 STICKER identifier。
                let account_key =
                    seeded_account_key.or_else(|| manifest.as_ref().and_then(account_item_key_of));
                save_manifest_state(
                    &context.account_dir,
                    &ManifestState {
                        version: planned.new_version,
                        account_item_key: account_key
                            .map(|key| base64::engine::general_purpose::STANDARD.encode(key)),
                        sticker_item_keys: Some(
                            planned
                                .manifest_record
                                .identifiers
                                .iter()
                                .filter(|identifier| {
                                    identifier.r#type == STICKER_PACK_IDENTIFIER_TYPE
                                })
                                .map(|identifier| {
                                    base64::engine::general_purpose::STANDARD
                                        .encode(&identifier.raw)
                                })
                                .collect(),
                        ),
                    },
                );
                // 写成功后以本地写后面为准返回（官方 upload 成功语义）：
                // 存储序 = 新 manifest STICKER identifiers 首现序；其余包
                // 沿用本轮读取态（写成功即我方 manifest 整体生效）。
                let mut out = Vec::new();
                for identifier in planned
                    .manifest_record
                    .identifiers
                    .iter()
                    .filter(|identifier| identifier.r#type == STICKER_PACK_IDENTIFIER_TYPE)
                {
                    if identifier.raw == new_key {
                        if let Some(sync) = project_sticker_pack(&new_record) {
                            out.push(sync);
                        }
                    } else if let Some(pack) =
                        current.iter().find(|pack| pack.key == identifier.raw)
                    {
                        out.push(pack.sync.clone());
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
                    // 采纳服务器 manifest（官方 409 语义）；贴纸面下轮整体
                    // 重读重放（读失败归 WriteFailed：写意图未落地）。
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::storage_sync::encrypt_raw_item;
    use presage::libsignal_service::proto::{storage_record, AccountRecord, StorageRecord};
    use prost::Message as _;

    fn installed_record(pack_id: &[u8], pack_key: &[u8], position: u32) -> StickerPackRecord {
        StickerPackRecord {
            pack_id: pack_id.to_vec(),
            pack_key: pack_key.to_vec(),
            position,
            deleted_at_timestamp: 0,
        }
    }

    fn tombstone_record(pack_id: &[u8], deleted_at: u64) -> StickerPackRecord {
        StickerPackRecord {
            pack_id: pack_id.to_vec(),
            pack_key: Vec::new(),
            position: 0,
            deleted_at_timestamp: deleted_at,
        }
    }

    /// 记录线格式往返：安装态/墓碑经 tag=6 包裹 → 解析还原；非贴纸记录
    /// （Account）、垃圾 bytes、空明文 → None（读面跳过口径）；本地线格式
    /// bytes 与上游 StorageRecord 类型互读（旧 proto 未知字段跳过 = 上游
    /// 读面行为，故读 shim 必须本地持有）。
    #[test]
    fn sticker_record_wire_roundtrip_and_foreign_records_rejected() {
        let pack_id = [0x11u8; 16];
        let pack_key = [0x22u8; 32];
        let installed = installed_record(&pack_id, &pack_key, 3);
        let wire = sticker_record_wire(&installed);
        assert_eq!(sticker_record_of(&wire), Some(installed));

        let tombstone = tombstone_record(&pack_id, 1_720_000_000_000);
        assert_eq!(
            sticker_record_of(&sticker_record_wire(&tombstone)),
            Some(tombstone)
        );

        // 非 sticker arm 的 StorageRecord（Account，上游类型编码）→ None。
        let account_wire = StorageRecord {
            record: Some(storage_record::Record::Account(AccountRecord::default())),
        }
        .encode_to_vec();
        assert_eq!(sticker_record_of(&account_wire), None);
        // 垃圾 bytes / 空明文 → None。
        assert_eq!(sticker_record_of(&[0xff, 0xff, 0xff]), None);
        assert_eq!(sticker_record_of(&[]), None);

        // 上游类型能解本地线格式 bytes（无 tag 6 arm → record 为空）：
        // 证明钉定 rev 读不出贴纸记录，本地镜像必要。
        let reparsed = StorageRecord::decode(wire.as_slice()).expect("decodes");
        assert!(reparsed.record.is_none());
    }

    /// 投影形状（官方 toStickerPackRecord 同构）：安装态/墓碑/malformed。
    #[test]
    fn project_sticker_pack_shapes() {
        let sync =
            project_sticker_pack(&installed_record(&[1u8; 16], &[2u8; 32], 7)).expect("projects");
        assert_eq!(sync.pack_id, vec![1u8; 16]);
        assert_eq!(sync.pack_key, Some(vec![2u8; 32]));
        assert_eq!(sync.position, Some(7));
        assert_eq!(sync.deleted_at_timestamp_ms, None);

        // 墓碑：packKey/position 置空。
        let sync = project_sticker_pack(&tombstone_record(&[3u8; 16], 1_720_000_000_000))
            .expect("projects");
        assert_eq!(sync.pack_key, None);
        assert_eq!(sync.position, None);
        assert_eq!(sync.deleted_at_timestamp_ms, Some(1_720_000_000_000));

        // malformed：packId 空 / 安装态缺 packKey → None（调用方跳过）。
        assert!(project_sticker_pack(&installed_record(&[], &[2u8; 32], 0)).is_none());
        assert!(project_sticker_pack(&StickerPackRecord {
            pack_id: vec![4u8; 16],
            pack_key: Vec::new(),
            position: 0,
            deleted_at_timestamp: 0,
        })
        .is_none());
    }

    /// 意图 → 目标记录：官方 toStickerPackRecord 同构（安装 = packId+
    /// packKey+position；卸载 = packId+deletedAtTimestamp，packKey/position
    /// 不写）、幂等无写、position 未传本地 MAX+1 分配。
    #[test]
    fn sticker_record_for_semantics() {
        let pack_id = [9u8; 16];
        let key_a = [0xaau8; 32];
        let key_b = [0xbbu8; 32];

        // 全新安装：position 未传 → 本地 MAX+1；无已安装包 → 0。
        let record = sticker_record_for(
            None,
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_a,
                position: None,
            },
            &[2, 5, 3],
            0,
        )
        .expect("writes");
        assert_eq!(record, installed_record(&pack_id, &key_a, 6));
        let record = sticker_record_for(
            None,
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_a,
                position: None,
            },
            &[],
            0,
        )
        .expect("writes");
        assert_eq!(record.position, 0);

        // 显式 position 透传。
        let record = sticker_record_for(
            None,
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_a,
                position: Some(1),
            },
            &[2, 5],
            0,
        )
        .expect("writes");
        assert_eq!(record.position, 1);

        // 幂等：已安装且 packKey/position 全同 → 无写。
        let current =
            project_sticker_pack(&installed_record(&pack_id, &key_a, 6)).expect("projects");
        assert!(sticker_record_for(
            Some(&current),
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_a,
                position: None,
            },
            &[6],
            0,
        )
        .is_none());

        // 已安装但 position 不同 / 换 key → 换写。
        assert!(sticker_record_for(
            Some(&current),
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_a,
                position: Some(0),
            },
            &[6],
            0,
        )
        .is_some());
        assert!(sticker_record_for(
            Some(&current),
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_b,
                position: Some(6),
            },
            &[6],
            0,
        )
        .is_some());

        // 覆盖墓碑重装 → 换写（非幂等）。
        let tombstoned =
            project_sticker_pack(&tombstone_record(&pack_id, 1_000)).expect("projects");
        assert!(sticker_record_for(
            Some(&tombstoned),
            &StickerPackWrite::Install {
                pack_id: pack_id.to_vec(),
                pack_key: key_b,
                position: Some(2),
            },
            &[],
            0,
        )
        .is_some());

        // 卸载：已安装 → 墓碑（packKey/position 不写，时间戳 = 引擎时钟）。
        let record = sticker_record_for(
            Some(&current),
            &StickerPackWrite::Uninstall {
                pack_id: pack_id.to_vec(),
            },
            &[],
            1_720_000_000_000,
        )
        .expect("writes");
        assert_eq!(record, tombstone_record(&pack_id, 1_720_000_000_000));

        // 卸载幂等：云上无此包 / 已墓碑 → 无写。
        assert!(sticker_record_for(
            None,
            &StickerPackWrite::Uninstall {
                pack_id: pack_id.to_vec(),
            },
            &[],
            1,
        )
        .is_none());
        assert!(sticker_record_for(
            Some(&tombstoned),
            &StickerPackWrite::Uninstall {
                pack_id: pack_id.to_vec(),
            },
            &[],
            1,
        )
        .is_none());
    }

    /// item 经本地 shim 的加解密往返（modern/legacy 两路径 + 换 key MAC
    /// 失败面）。与上游 API 的派生一致性由 storage_sync tests 交叉钉定。
    #[test]
    fn sticker_item_roundtrip_through_local_shim() {
        let storage_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[5u8; 32]).expect("key"));
        let other_key =
            StorageServiceKey::from_master_key(&MasterKey::from_slice(&[6u8; 32]).expect("key"));
        let installed = installed_record(&[7u8; 16], &[8u8; 32], 1);
        let wire = sticker_record_wire(&installed);
        for record_ikm in [None, Some([0x33u8; 32].as_slice())] {
            let item = encrypt_raw_item(&storage_key, vec![0xccu8; 16], &wire, record_ikm);
            let plaintext = decrypt_raw_item(&storage_key, &item, record_ikm).expect("decrypts");
            assert_eq!(sticker_record_of(&plaintext), Some(installed.clone()));
            // 解密面失败（MAC）：legacy 路径换 storage key；modern 路径
            // item key 只派生自 record_ikm（storage key 不参与，上游同构）
            // → 换 record_ikm。
            match record_ikm {
                None => assert!(decrypt_raw_item(&other_key, &item, None).is_err()),
                Some(_) => {
                    assert!(decrypt_raw_item(&storage_key, &item, Some(&[0x34u8; 32])).is_err())
                }
            }
        }
    }
}
