// SPDX-License-Identifier: AGPL-3.0-only

//! CallLinkRecord Storage Service 同步（窗D A6，缺口总登记）。
//!
//! 真相源 = 独立 StorageRecord（`Record::CallLink`，tag 7）：官方
//! SignalStorage.proto CallLinkRecord（rootKey 1/adminPasskey 2/
//! deletedAtTimestampMs 3，reserved 4 = 弃用 epoch）与
//! ManifestRecord.Identifier.Type.CALL_LINK = 7。行为参照
//! Signal-Desktop `ts/services/storageRecordOps.preload.ts`
//! （toCallLinkRecord :847/mergeCallLinkRecord :2626）与
//! `ts/services/storage.preload.ts` upload callLinks 段（:685-730），
//! 不抄码。
//!
//! 引擎此前 call link 面只有凭据材料（calls.linkPresentation/
//! createLinkAuth/groupProof），无 storage 同步腿：新链接设备后其他
//! 设备创建/删除的 call link 状态不可见。
//!
//! 官方写语义：active → `{rootKey, adminPasskey}`（deletedAtTimestampMs
//! 不写）；deleted → `{rootKey, deletedAtTimestampMs}`（adminKey 恒缺，
//! toCallLinkRecord deleted 分支逐字）；更新 = 新 16B item key 换入 +
//! 旧 key 进 deleteKey（贴纸包面同构）。409 采纳重放同置顶面。
//!
//! 官方以 roomId 定位同链接（roomId = RingRTC derive_room_id(rootKey)
//! 纯函数）；引擎以 rootKey 字节等价定位（同 rootKey ⇒ 同 roomId），
//! 不复制 RingRTC HKDF 派生面。

use presage::libsignal_service::master_key::{MasterKey, StorageServiceKey};
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::proto::{ManifestRecord, StorageItem};
use presage::libsignal_service::{StorageService, StorageServiceError};
use presage::store::StateStore as _;
use prost::Message as _;

use super::storage_sync::{
    account_item_key_of, conflict_decision, decrypt_raw_item, minimal_account_record, new_item_key,
    open_storage_service, plan_record_writes, put_write_operation, read_raw_items,
    save_manifest_state, storage_auth, ConflictDecision, ManifestState, PutOutcome, RecordPayload,
    RecordUpsert, StorageRecordKind, StorageSyncError,
};
use super::StorageContext;
use crate::proto::storage as storage_proto;

/// adminPasskey 参数闸（fail-closed）：标准 base64 非空。官方
/// toAdminKeyBytes 无长度闸同域（RingRTC adminKey 不定长透传）。
pub(crate) fn decode_admin_passkey(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()?;
    (!bytes.is_empty()).then_some(bytes)
}

/// CallLinkRecord 写面意图（方法层已 fail-closed：rootKey 16/21B、
/// adminPasskey/deletedAtTimestampMs 两键互斥）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallLinkWrite {
    pub(crate) root_key: Vec<u8>,
    /// active 态 admin passkey（deleted 态恒 None）。
    pub(crate) admin_passkey: Option<Vec<u8>>,
    /// deleted 态毫秒时间戳（active 态恒 None）。
    pub(crate) deleted_at_timestamp_ms: Option<u64>,
}

/// 投影（wire additive，getCallLinks/setCallLink 返回形状的数据面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallLinkSync {
    /// 标准 base64 出（16/21B，RingRTC CallLinkRootKey 两版本定长）。
    pub(crate) root_key: Vec<u8>,
    pub(crate) admin_passkey: Option<Vec<u8>>,
    pub(crate) deleted_at_timestamp_ms: Option<u64>,
}

/// CallLinkRecord → 投影（官方 mergeCallLinkRecord isBadRemoteData 同域：
/// deletedAt 在场即视为删除态，adminPasskey 不透出；toCallLinkRecord
/// deleted 分支 adminKey 恒缺同形）。rootKey 空 = 官方 shouldDrop
/// 'no rootKey' 同位 → None（调用方跳过）。
pub(crate) fn project_call_link(record: &storage_proto::CallLinkRecord) -> Option<CallLinkSync> {
    if record.root_key.is_empty() {
        return None;
    }
    if record.deleted_at_timestamp_ms > 0 {
        return Some(CallLinkSync {
            root_key: record.root_key.clone(),
            admin_passkey: None,
            deleted_at_timestamp_ms: Some(record.deleted_at_timestamp_ms),
        });
    }
    Some(CallLinkSync {
        root_key: record.root_key.clone(),
        admin_passkey: if record.admin_passkey.is_empty() {
            None
        } else {
            Some(record.admin_passkey.clone())
        },
        deleted_at_timestamp_ms: None,
    })
}

/// 本地意图 → 目标 record（官方 toCallLinkRecord 同构：deleted 态
/// adminPasskey 恒不写）。云态投影已满足意图（幂等）→ None = 无写
/// （官方 storageNeedsSync=false 同语义）。
fn call_link_record_for(
    current: Option<&CallLinkSync>,
    intent: &CallLinkWrite,
) -> Option<storage_proto::CallLinkRecord> {
    let target = CallLinkSync {
        root_key: intent.root_key.clone(),
        admin_passkey: intent.admin_passkey.clone(),
        deleted_at_timestamp_ms: intent.deleted_at_timestamp_ms,
    };
    if current == Some(&target) {
        return None;
    }
    Some(storage_proto::CallLinkRecord {
        root_key: intent.root_key.clone(),
        admin_passkey: intent.admin_passkey.clone().unwrap_or_default(),
        deleted_at_timestamp_ms: intent.deleted_at_timestamp_ms.unwrap_or(0),
    })
}

/// 云上 call link 一条（manifest key + 投影；写面定位 replaced key 用）。
struct CloudLink {
    key: Vec<u8>,
    sync: CallLinkSync,
}

/// 单条 item → CloudLink（解密/解析/投影任一失败 → None，调用方跳过）。
fn cloud_link_of(
    storage_key: &StorageServiceKey,
    item: &StorageItem,
    record_ikm: Option<&[u8]>,
) -> Option<CloudLink> {
    let plaintext = decrypt_raw_item(storage_key, item, record_ikm).ok()?;
    let record = storage_proto::StorageRecord::decode(plaintext.as_slice()).ok()?;
    let Some(storage_proto::storage_record::Record::CallLink(call_link)) = record.record else {
        return None;
    };
    let sync = project_call_link(&call_link)?;
    Some(CloudLink {
        key: item.key.clone(),
        sync,
    })
}

/// manifest 全部 CALL_LINK identifier → 批量原样读 → 解密/解析/投影
/// （存储序 = manifest identifiers 首现序；单条坏 item 跳过不炸面）。
async fn read_cloud_links(
    push: &presage::libsignal_service::prelude::PushService,
    storage_key: &StorageServiceKey,
    manifest: Option<&ManifestRecord>,
) -> Result<Vec<CloudLink>, StorageSyncError> {
    let Some(manifest) = manifest else {
        return Ok(Vec::new());
    };
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| {
            identifier.r#type()
                == presage::libsignal_service::proto::manifest_record::identifier::Type::CallLink
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
        if let Some(link) = cloud_link_of(storage_key, item, record_ikm) {
            out.push(link);
        }
    }
    Ok(out)
}

/// 读面编排：master key → manifest → 全部 call link 投影（存储序）。
/// master key 缺失或 manifest 404（账号从未有过 storage 状态）→ 空列表
/// （置顶/贴纸读面同口径）。
pub(crate) async fn read_call_links(
    context: &StorageContext,
) -> Result<Vec<CallLinkSync>, StorageSyncError> {
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
        Ok(manifest) => Some(manifest),
        Err(StorageServiceError::Service(ServiceError::NotFoundError)) => None,
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    Ok(
        read_cloud_links(&context.push, &storage_key, manifest.as_ref())
            .await?
            .into_iter()
            .map(|link| link.sync)
            .collect(),
    )
}

/// 写面编排（官方 upload 语义 + 贴纸包面同构）：读 manifest + 全部
/// call link item → 定位 replaced key（rootKey 字节等价）→ 换入新 key/
/// 换出旧 key → PUT → 409 采纳重放（conflictBackOff 3 次）。manifest
/// 404 → 置顶/贴纸面首写分支：master key 播种 + version 1 最小
/// AccountRecord 与 call link 记录同批写入。
pub(crate) async fn write_call_link(
    context: &StorageContext,
    intent: &CallLinkWrite,
) -> Result<Vec<CallLinkSync>, StorageSyncError> {
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
            // 有 manifest 无 Account 标识 = 非法状态（置顶面口径）。
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
        let current = read_cloud_links(&context.push, &storage_key, manifest.as_ref()).await?;
        let target = current
            .iter()
            .find(|link| link.sync.root_key == intent.root_key);
        let Some(new_record) = call_link_record_for(target.map(|link| &link.sync), intent) else {
            // 意图已满足（云态幂等）：返回当前云态投影（存储序）。
            return Ok(current.into_iter().map(|link| link.sync).collect());
        };
        let replaced_key = target.map(|link| link.key.clone());

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
            kind: StorageRecordKind::CallLink,
            new_key: new_key.clone(),
            replaced_key,
            payload: Some(RecordPayload::CallLink(new_record.clone())),
        });
        let planned = plan_record_writes(previous, &upserts, context.device_id, &storage_key);

        let auth = storage_auth(&context.push).await?;
        match put_write_operation(&context.push, auth, &planned.write_operation).await? {
            PutOutcome {
                conflict_manifest: None,
            } => {
                // 本地状态登记（best-effort 排查面）：version / Account key。
                let account_key =
                    seeded_account_key.or_else(|| manifest.as_ref().and_then(account_item_key_of));
                save_manifest_state(
                    &context.account_dir,
                    &ManifestState {
                        version: planned.new_version,
                        account_item_key: account_key.map(|key| {
                            use base64::Engine as _;
                            base64::engine::general_purpose::STANDARD.encode(key)
                        }),
                        sticker_item_keys: None,
                    },
                );
                // 写成功后以本地写后面为准返回（官方 upload 成功语义）：
                // 存储序 = 新 manifest CALL_LINK identifiers 首现序。
                let mut out = Vec::new();
                for identifier in planned.manifest_record.identifiers.iter().filter(|identifier| {
                    identifier.r#type()
                        == presage::libsignal_service::proto::manifest_record::identifier::Type::CallLink
                }) {
                    if identifier.raw == new_key {
                        if let Some(sync) = project_call_link(&new_record) {
                            out.push(sync);
                        }
                    } else if let Some(link) =
                        current.iter().find(|link| link.key == identifier.raw)
                    {
                        out.push(link.sync.clone());
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
                    // 采纳服务器 manifest（官方 409 语义）；下轮整体重读重放
                    // （读失败归 WriteFailed：写意图未落地）。
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

    fn active_record(root: &[u8], passkey: &[u8]) -> storage_proto::CallLinkRecord {
        storage_proto::CallLinkRecord {
            root_key: root.to_vec(),
            admin_passkey: passkey.to_vec(),
            deleted_at_timestamp_ms: 0,
        }
    }

    /// 记录线格式往返（引擎 proto StorageRecord tag=7 包裹）：active/deleted
    /// 编码 → 解析还原；非 call link arm（Account）→ None（读面跳过口径）。
    #[test]
    fn call_link_record_wire_roundtrip_and_foreign_records_rejected() {
        use prost::Message as _;

        let active = active_record(&[0x11u8; 16], &[0x22u8; 32]);
        let wire = storage_proto::StorageRecord {
            record: Some(storage_proto::storage_record::Record::CallLink(
                active.clone(),
            )),
        }
        .encode_to_vec();
        let reparsed = storage_proto::StorageRecord::decode(wire.as_slice()).expect("decodes");
        assert_eq!(
            reparsed.record,
            Some(storage_proto::storage_record::Record::CallLink(active))
        );

        // 非 call link arm → None。
        let account_wire = storage_proto::StorageRecord {
            record: Some(storage_proto::storage_record::Record::Account(
                storage_proto::AccountRecord::default(),
            )),
        }
        .encode_to_vec();
        let reparsed = storage_proto::StorageRecord::decode(account_wire.as_slice()).expect("ok");
        assert!(!matches!(
            reparsed.record,
            Some(storage_proto::storage_record::Record::CallLink(_))
        ));
    }

    /// 投影形状（官方 mergeCallLinkRecord isBadRemoteData 同域）：active /
    /// deleted（adminPasskey 不透出）/ 空 rootKey → None。
    #[test]
    fn project_call_link_shapes() {
        let sync = project_call_link(&active_record(&[1u8; 16], &[2u8; 32])).expect("projects");
        assert_eq!(sync.root_key, vec![1u8; 16]);
        assert_eq!(sync.admin_passkey, Some(vec![2u8; 32]));
        assert_eq!(sync.deleted_at_timestamp_ms, None);

        // deleted：adminPasskey 恒不透出（toCallLinkRecord deleted 分支
        // adminKey 恒缺同形；deletedAt+adminPasskey 并存的坏远端数据同位
        // 归删除态）。
        let deleted = storage_proto::CallLinkRecord {
            root_key: vec![3u8; 21],
            admin_passkey: vec![4u8; 32],
            deleted_at_timestamp_ms: 1_720_000_000_000,
        };
        let sync = project_call_link(&deleted).expect("projects");
        assert_eq!(sync.admin_passkey, None);
        assert_eq!(sync.deleted_at_timestamp_ms, Some(1_720_000_000_000));

        // active 态无 adminPasskey（只读投影）。
        let sync = project_call_link(&active_record(&[5u8; 16], &[])).expect("projects");
        assert_eq!(sync.admin_passkey, None);

        // rootKey 空 = 官方 shouldDrop 'no rootKey' 同位 → None。
        assert!(project_call_link(&active_record(&[], &[2u8; 32])).is_none());
    }

    /// 意图 → 目标记录（官方 toCallLinkRecord 同构）：active = rootKey+
    /// adminPasskey（deletedAt 不写）、deleted = rootKey+deletedAtTimestampMs
    /// （adminPasskey 不写）、云态已满足意图 → None 无写。
    #[test]
    fn call_link_record_for_semantics() {
        let root = vec![0x11u8; 16];
        let passkey = vec![0x22u8; 32];

        // active 意图。
        let intent = CallLinkWrite {
            root_key: root.clone(),
            admin_passkey: Some(passkey.clone()),
            deleted_at_timestamp_ms: None,
        };
        assert_eq!(
            call_link_record_for(None, &intent),
            Some(active_record(&root, &passkey))
        );
        // 幂等：云态已同 → 无写。
        let current = project_call_link(&active_record(&root, &passkey));
        assert_eq!(call_link_record_for(current.as_ref(), &intent), None);
        // 换 passkey → 重写。
        let other = CallLinkWrite {
            root_key: root.clone(),
            admin_passkey: Some(vec![0x33u8; 32]),
            deleted_at_timestamp_ms: None,
        };
        assert!(call_link_record_for(current.as_ref(), &other).is_some());

        // deleted 意图：adminPasskey 恒不写。
        let intent = CallLinkWrite {
            root_key: root.clone(),
            admin_passkey: None,
            deleted_at_timestamp_ms: Some(1_720_000_000_000),
        };
        assert_eq!(
            call_link_record_for(None, &intent),
            Some(storage_proto::CallLinkRecord {
                root_key: root.clone(),
                admin_passkey: Vec::new(),
                deleted_at_timestamp_ms: 1_720_000_000_000,
            })
        );
        // 幂等：已墓碑同刻 → 无写。
        let current = project_call_link(&storage_proto::CallLinkRecord {
            root_key: root.clone(),
            admin_passkey: Vec::new(),
            deleted_at_timestamp_ms: 1_720_000_000_000,
        });
        assert_eq!(call_link_record_for(current.as_ref(), &intent), None);
        // 坏远端数据（deletedAt+adminPasskey 并存）投影为删除态 → 同刻
        // deleted 意图幂等无写（官方 assume deleted 同位）。
        let bad_remote = project_call_link(&storage_proto::CallLinkRecord {
            root_key: root.clone(),
            admin_passkey: passkey.clone(),
            deleted_at_timestamp_ms: 1_720_000_000_000,
        });
        assert_eq!(call_link_record_for(bad_remote.as_ref(), &intent), None);
    }

    /// adminPasskey 参数闸：标准 base64 非空；垃圾/空串 → None。
    #[test]
    fn decode_admin_passkey_shapes() {
        assert_eq!(decode_admin_passkey("KQ=="), Some(vec![0x29]));
        // 非法 base64 / 空解码 → None。
        assert_eq!(decode_admin_passkey("not-base64!!"), None);
        assert_eq!(decode_admin_passkey(""), None);
        // 前后空白容忍（rootKey/roomId 闸同域 trim）。
        assert_eq!(decode_admin_passkey("  KQ==  "), Some(vec![0x29]));
    }
}
