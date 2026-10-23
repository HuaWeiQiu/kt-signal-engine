// SPDX-License-Identifier: AGPL-3.0-only

//! Storage Service ContactRecord merge/发现面（窗D A4，缺口总登记）。
//!
//! 官方行为参照（AGPL 只读锚，非源码复制）：`services/storageRecordOps.
//! preload.ts` mergeContactRecord——Storage 同步通道是可信同步源（服务器
//! 认证 + 端到端加密 item），ContactRecord 携带对端 identityKey/profileKey/
//! 姓名面，桌面侧 merge 进本地库。引擎侧缺该面时：新链接设备 / 换机后
//! presage 身份库为空，对端身份钥只能靠发送时服务器 prekey 面旁路回填，
//! 安全号/信任闸/头像发现面无从谈起。
//!
//! 引擎腿语义（官方全量 merge 的协议面子集）：
//! - identityKey 非空 → `updateIdentityAfterSync` 同构（SignalProtocolStore.
//!   preload.ts:2378）：absent→save、同钥→no-op、异钥→replace（vendored
//!   save_identity 承担换钥 archive sessions）；identityState 随行落
//!   sidecar 信任态（apply_synced_trust，verified 同步收口同通道语义）。
//! - profileKey 非空且本地缺失 → upsert_profile_key（官方 isFirstSync ||
//!   !localProfileKey 同闸；已有不覆盖）。
//! - 联系人缺失 → save_contact 建档（e164/name/profileKey 随建）；已有
//!   档 → 存储名（given+family）非空且与本地不同 → 覆盖本地 contacts.name
//!   （官方 Storage 通道可信同步源语义，姓名升级批 2026-10-16；存储名空
//!   保留本地，同名跳过；引擎仍不发明 system/nickname 面）。
//! - 无消费模型字段（unregisteredAtTimestamp/hideStory/hidden/avatarColor/
//!   note/nickname 族）记档跳过——archived/markedUnread/muted/blocked 已有
//!   专用读写面（A2/拉黑批），此处不重复。

use presage::libsignal_service::master_key::StorageServiceKey;
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::prelude::Uuid;
use presage::libsignal_service::protocol::{
    IdentityChange, IdentityKey, IdentityKeyStore as _, ProtocolAddress, ServiceId,
};
use presage::libsignal_service::zkgroup::profiles::ProfileKey;
use presage::model::contacts::Contact;
use presage::store::{ContentsStore as _, StateStore as _, Store as _};
use prost::Message as _;

use super::storage_sync::{self, decrypt_raw_item, read_raw_items, storage_auth, StorageSyncError};
use super::{identity, StorageContext};
use crate::proto::storage as storage_proto;

/// merge 汇总（wire additive，connector/desktop 消费）。
#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct MergeOutcome {
    /// 解码成功的 ContactRecord 数。
    pub(crate) records_read: u32,
    /// 官方 shouldDrop 同位：无 serviceId/身份钥坏形/本机 uuid——逐条跳过
    /// 不炸批。
    pub(crate) skipped: u32,
    /// 身份库新增（此前无该对端身份钥）。
    pub(crate) identities_new: u32,
    /// 身份钥替换（官方 keychange 同位；vendored save_identity 换钥 archive
    /// sessions 语义随行）。
    pub(crate) identities_replaced: u32,
    /// 本地缺失而补入的 profile key 数。
    pub(crate) profile_keys_set: u32,
    /// contacts 表新建档数。
    pub(crate) contacts_created: u32,
    /// contacts 表既有档名字升级数（存储名非空且与本地不同 → 覆盖本地
    /// name；姓名升级批 2026-10-16，wire additive）。
    pub(crate) contacts_name_updated: u32,
    /// sidecar 信任态随 identityState 收敛次数。
    pub(crate) trust_states_synced: u32,
    /// 本腿读到的 manifest version（批 R 重连版本闸拉后落账基线；
    /// manifest 404/无 master key 早退 = 0 =「无 storage 态」哨兵）。
    /// 引擎内部簿记面，RPC 应答序列化冻结（serde skip——wire 零变化）。
    #[serde(skip)]
    pub(crate) version: u64,
}

/// identityState（Storage ContactRecord 枚举）→ sidecar 信任态（官方
/// identityState null → DEFAULT 同域；无 sidecar 语义的坏值归 default）。
fn trust_state_of(identity_state: i32) -> identity::TrustState {
    use crate::proto::storage::contact_record::IdentityState;
    match IdentityState::try_from(identity_state) {
        Ok(IdentityState::Verified) => identity::TrustState::Verified,
        Ok(IdentityState::Unverified) => identity::TrustState::Unverified,
        _ => identity::TrustState::Default,
    }
}

/// 存储名复合（given+family 空格拼接，family 空则单段）——建档与姓名
/// 升级两臂共用单点（复合规则漂移即断）。
fn stored_name_of(contact: &storage_proto::ContactRecord) -> String {
    let mut name = contact.given_name.clone();
    if !contact.family_name.is_empty() {
        if !name.is_empty() {
            name.push(' ');
        }
        name.push_str(&contact.family_name);
    }
    name
}

/// 姓名升级判定（官方 Storage 通道姓名权威批 2026-10-16，纯函数零 IO
/// ——三臂单测钉定）：存储名非空且 != 本地名 → Some(新名)；存储名空
/// （无权威材料，保留本地）或同名（幂等 no-op）→ None。
fn name_upgrade_of(stored_name: &str, local_name: &str) -> Option<String> {
    (!stored_name.is_empty() && stored_name != local_name).then(|| stored_name.to_owned())
}

/// ContactRecord → service id（官方 fromAciUuidBytesOrString/
/// fromPniUuidBytesOrUntaggedString 同构：binary 16B 优先、字符串兜底；
/// serviceId = aci || pni）。
fn service_id_of(contact: &storage_proto::ContactRecord) -> Option<ServiceId> {
    if !contact.aci_binary.is_empty() {
        if let Ok(uuid) = Uuid::from_slice(&contact.aci_binary) {
            return Some(ServiceId::Aci(uuid.into()));
        }
    }
    if !contact.aci.is_empty() {
        if let Some(sid) = ServiceId::parse_from_service_id_string(&contact.aci) {
            if matches!(sid, ServiceId::Aci(_)) {
                return Some(sid);
            }
        }
    }
    if !contact.pni_binary.is_empty() {
        if let Ok(uuid) = Uuid::from_slice(&contact.pni_binary) {
            return Some(ServiceId::Pni(uuid.into()));
        }
    }
    if !contact.pni.is_empty() {
        // pni 字段是官方语义的 untagged PNI 串（fromPniUuidBytesOrUntagged
        // String：字段本身声明种类，裸 UUID 即 PNI）；"PNI:" 标签形宽容解析。
        if let Ok(uuid) = Uuid::parse_str(&contact.pni) {
            return Some(ServiceId::Pni(uuid.into()));
        }
        if let Some(sid) = ServiceId::parse_from_service_id_string(&contact.pni) {
            if matches!(sid, ServiceId::Pni(_)) {
                return Some(sid);
            }
        }
    }
    None
}

/// merge 编排：manifest → Contact identifiers 批读 → 逐条 merge（汇总面）。
/// master key 缺失或 manifest 404（从未有 storage 状态）→ 零汇总幂等成功
/// （blocked 只读面同口径）。
/// 返回 (汇总, 观测批)：观测批 = 本轮成功解密的记录逐条过
/// `conversation_observation_of`（F 链路入站观测——三类会话记录的三 flag
/// 随行观测，含 merge 语义跳过的条目；观测是内容驱动，不随 merge 的
/// shouldDrop 闸）。调用方（service 层）负责把观测批过簿发通知。
pub(crate) async fn merge_contacts(
    mut context: StorageContext,
    now: u64,
) -> Result<
    (
        MergeOutcome,
        Vec<(
            storage_sync::EngineConversationId,
            storage_sync::ConversationState,
        )>,
    ),
    StorageSyncError,
> {
    let mut outcome = MergeOutcome::default();
    let mut observed = Vec::new();
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok((outcome, observed));
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let read_key = StorageServiceKey::from_master_key(&master_key);
    let storage = storage_sync::open_storage_service(context.push.clone(), read_key).await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(presage::libsignal_service::StorageServiceError::Service(
            ServiceError::NotFoundError,
        )) => return Ok((outcome, observed)),
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    // 批 R：本腿 manifest version 随行汇总（空 keys 早退前落值——version
    // 是 manifest 面事实，与会话条目有无无关）。
    outcome.version = manifest.version;
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| {
            matches!(
                identifier.r#type(),
                presage::libsignal_service::proto::manifest_record::identifier::Type::Contact
            )
        })
        .map(|identifier| identifier.raw.clone())
        .collect();
    if keys.is_empty() {
        return Ok((outcome, observed));
    }
    let record_ikm = if manifest.record_ikm.is_empty() {
        None
    } else {
        Some(manifest.record_ikm.as_slice())
    };
    let auth = storage_auth(&context.push).await?;
    let items = read_raw_items(&context.push, auth, keys.clone()).await?;

    let mut aci_store = context.store.aci_protocol_store();
    let mut pni_store = context.store.pni_protocol_store();

    for key in &keys {
        let Some(item) = items.iter().find(|item| &item.key == key) else {
            continue;
        };
        let Ok(plaintext) = decrypt_raw_item(&storage_key, item, record_ikm) else {
            outcome.skipped += 1;
            continue;
        };
        let Ok(record) = storage_proto::StorageRecord::decode(plaintext.as_slice()) else {
            outcome.skipped += 1;
            continue;
        };
        // F 链路入站观测：记录内容驱动（merge 的 shouldDrop/本机 uuid 闸
        // 不影响观测——三 flag 观测不消费身份材料）。
        let Some(record) = record.record else {
            outcome.skipped += 1;
            continue;
        };
        if let Some(observation) = storage_sync::conversation_observation_of(&record) {
            observed.push(observation);
        }
        let storage_proto::storage_record::Record::Contact(contact) = record else {
            outcome.skipped += 1;
            continue;
        };
        let Some(service_id) = service_id_of(&contact) else {
            outcome.skipped += 1;
            continue;
        };
        // 本机 uuid 不 merge（官方 'our own uuid' shouldDrop 同位）。
        let uuid = service_id.raw_uuid();
        if uuid == context.aci_uuid || uuid == context.pni_uuid {
            outcome.skipped += 1;
            continue;
        }
        outcome.records_read += 1;
        let sid = service_id.service_id_string();
        let address = ProtocolAddress::new(
            sid.clone(),
            presage::libsignal_service::protocol::DeviceId::new(1).expect("primary device"),
        );

        // 官方 mergeContactRecord identity 面闸：identityKey.length 才动
        // 身份库与信任态（updateIdentityAfterSync 同位）。
        if !contact.identity_key.is_empty() {
            let Ok(peer_key) = IdentityKey::decode(&contact.identity_key) else {
                outcome.skipped += 1;
                continue;
            };
            let change = if matches!(service_id, ServiceId::Aci(_)) {
                aci_store.save_identity(&address, &peer_key).await
            } else {
                pni_store.save_identity(&address, &peer_key).await
            };
            match change {
                Ok(IdentityChange::NewOrUnchanged) => {}
                Ok(IdentityChange::ReplacedExisting) => {
                    outcome.identities_replaced += 1;
                }
                Err(_) => {
                    outcome.skipped += 1;
                    continue;
                }
            }
            if identity::apply_synced_trust(
                &context.account_dir,
                &sid,
                trust_state_of(contact.identity_state),
                now,
            ) {
                outcome.trust_states_synced += 1;
            }
        }

        // profileKey：本地缺失才写（官方 isFirstSync || !localProfileKey
        // 同闸，已有不覆盖）；坏长度（≠32B）诚实跳过。
        if !contact.profile_key.is_empty() {
            let local = context.store.profile_key(&service_id).await.unwrap_or(None);
            if local.is_none() {
                if let Ok(key_bytes) = <[u8; 32]>::try_from(contact.profile_key.as_slice()) {
                    if context
                        .store
                        .upsert_profile_key(&uuid, ProfileKey::create(key_bytes))
                        .await
                        .is_ok()
                    {
                        outcome.profile_keys_set += 1;
                    }
                }
            }
        }

        // 联系人面（官方 mergeContactRecord 姓名升级批 2026-10-16）：缺失
        // → 建档（e164/name/profileKey 随建）；已有 → 存储名非空且与本地
        // 不同 → save_contact 覆盖本地 name（其余本地字段原样保留，
        // contacts_name_updated 计数）；存储名空保留本地；同名跳过。读
        // 失败维持原状静默跳过（与此前 Err 臂同口径）。
        match context.store.contact_by_id(&service_id).await {
            Ok(None) => {
                let contact_model = Contact {
                    uuid,
                    phone_number: if contact.e164.is_empty() {
                        None
                    } else {
                        presage::libsignal_service::prelude::phonenumber::parse(None, &contact.e164)
                            .ok()
                    },
                    name: stored_name_of(&contact),
                    verified: Default::default(),
                    profile_key: contact.profile_key.clone(),
                    expire_timer: 0,
                    expire_timer_version: 0,
                    inbox_position: 0,
                    avatar: None,
                };
                if context.store.save_contact(&contact_model).await.is_ok() {
                    outcome.contacts_created += 1;
                }
            }
            Ok(Some(local)) => {
                if let Some(name) = name_upgrade_of(&stored_name_of(&contact), &local.name) {
                    let mut updated = local;
                    updated.name = name;
                    if context.store.save_contact(&updated).await.is_ok() {
                        outcome.contacts_name_updated += 1;
                    }
                }
            }
            Err(_) => {}
        }
    }
    Ok((outcome, observed))
}

/// 群导入汇总（wire additive）。
#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct GroupImportOutcome {
    /// 解码成功的 GroupV2Record 数。
    pub(crate) records_read: u32,
    /// 本地未知 → 群态拉取 + 落库成功（导入完成）。
    pub(crate) imported: u32,
    /// 本地已有（store group 命中）→ 跳过不重复拉。
    pub(crate) already_known: u32,
    /// master key 坏形（≠32B）/群态拉取失败/落库失败。
    pub(crate) failed: u32,
    /// 本腿读到的 manifest version（语义与 MergeOutcome::version 同构，
    /// serde skip 同款冻结）。
    #[serde(skip)]
    pub(crate) version: u64,
}

/// Storage GroupV2Record masterKey → 群导入（窗D A5）：manifest 全部
/// GROUPV2 identifier 批读 → 本地未知 master key 逐群拉群态（groups
/// 服务器 GET v1/groups/，既有 fetch_group_state 面）→ save_group 落
/// presage store（listGroups/open_group_sender 发现与发送面即刻可用）。
/// 官方行为参照（AGPL 只读锚）：桌面 mergeGroupV2Record 后经
/// getOrCreateGroupSnapshot 按需拉群态；引擎作为 sidecar 无群快照惰性层，
/// 导入面直接拉取落库（增量：已有群零网络）。逐群独立 best-effort：单群
/// 拉取失败记 failed 不炸批（服务器对该群无凭据/已退出等场景诚实计数）。
/// 返回 (汇总, 观测批)：观测批口径与 merge_contacts 同（F 链路入站观测，
/// 成功解密记录逐条过 conversation_observation_of；GV2 族观测由此补
/// ——Contact 族在 mergeContacts）。
pub(crate) async fn import_groups(
    context: StorageContext,
) -> Result<
    (
        GroupImportOutcome,
        Vec<(
            storage_sync::EngineConversationId,
            storage_sync::ConversationState,
        )>,
    ),
    StorageSyncError,
> {
    let mut outcome = GroupImportOutcome::default();
    let mut observed = Vec::new();
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok((outcome, observed));
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let read_key = StorageServiceKey::from_master_key(&master_key);
    let storage = storage_sync::open_storage_service(context.push.clone(), read_key).await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(presage::libsignal_service::StorageServiceError::Service(
            ServiceError::NotFoundError,
        )) => return Ok((outcome, observed)),
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
    // 批 R：本腿 manifest version 随行汇总（merge_contacts 同位）。
    outcome.version = manifest.version;
    let keys: Vec<Vec<u8>> = manifest
        .identifiers
        .iter()
        .filter(|identifier| {
            matches!(
                identifier.r#type(),
                presage::libsignal_service::proto::manifest_record::identifier::Type::Groupv2
            )
        })
        .map(|identifier| identifier.raw.clone())
        .collect();
    if keys.is_empty() {
        return Ok((outcome, observed));
    }
    let record_ikm = if manifest.record_ikm.is_empty() {
        None
    } else {
        Some(manifest.record_ikm.as_slice())
    };
    let auth = storage_auth(&context.push).await?;
    let items = read_raw_items(&context.push, auth, keys.clone()).await?;

    for key in &keys {
        let Some(item) = items.iter().find(|item| &item.key == key) else {
            continue;
        };
        let Ok(plaintext) = decrypt_raw_item(&storage_key, item, record_ikm) else {
            outcome.failed += 1;
            continue;
        };
        let Ok(record) = storage_proto::StorageRecord::decode(plaintext.as_slice()) else {
            outcome.failed += 1;
            continue;
        };
        // F 链路入站观测（merge_contacts 同位，内容驱动）。
        let Some(record) = record.record else {
            outcome.failed += 1;
            continue;
        };
        if let Some(observation) = storage_sync::conversation_observation_of(&record) {
            observed.push(observation);
        }
        let storage_proto::storage_record::Record::GroupV2(group_record) = record else {
            outcome.failed += 1;
            continue;
        };
        if group_record.master_key.is_empty() {
            outcome.failed += 1;
            continue;
        }
        outcome.records_read += 1;
        let Ok(group_master_key) = <[u8; 32]>::try_from(group_record.master_key.as_slice()) else {
            outcome.failed += 1;
            continue;
        };
        // 本地已有 → 零网络跳过（增量导入纪律）。
        if matches!(context.store.group(group_master_key).await, Ok(Some(_))) {
            outcome.already_known += 1;
            continue;
        }
        match super::groups::fetch_group_state(&context, &group_record.master_key).await {
            Ok(group) => {
                if context
                    .store
                    .save_group(group_master_key, presage::model::groups::Group::from(group))
                    .await
                    .is_ok()
                {
                    outcome.imported += 1;
                } else {
                    outcome.failed += 1;
                }
            }
            Err(_) => outcome.failed += 1,
        }
    }
    Ok((outcome, observed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::storage::contact_record::IdentityState;

    fn contact_with(aci_binary: &[u8], aci: &str, pni: &str) -> storage_proto::ContactRecord {
        storage_proto::ContactRecord {
            aci_binary: aci_binary.to_vec(),
            aci: aci.to_owned(),
            pni: pni.to_owned(),
            ..storage_proto::ContactRecord::default()
        }
    }

    /// service id 解析优先序（官方 fromAciUuidBytesOrString 同构）：aciBinary
    /// 16B 优先 → aci 字符串兜底 → pniBinary → pni 字符串 → 全缺 None。
    #[test]
    fn service_id_resolution_precedence() {
        let uuid_bytes = [
            0x11u8, 0x11, 0x11, 0x11, 0x22, 0x22, 0x43, 0x33, 0x84, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55,
        ];
        let aci_uuid = Uuid::from_slice(&uuid_bytes).expect("uuid");

        // binary 优先：binary 与字符串并存时取 binary。
        let record = contact_with(&uuid_bytes, "wrong", "");
        assert_eq!(
            service_id_of(&record),
            Some(ServiceId::Aci(aci_uuid.into()))
        );

        // 字符串兜底（binary 缺）。
        let record = contact_with(&[], "11111111-2222-4333-8444-555555555555", "");
        assert!(service_id_of(&record).is_some());

        // aci 缺 → pniBinary；pni 字段裸 UUID（untagged PNI 官方语义）→ PNI；
        // pni 字段 "PNI:" 标签形宽容解析；全缺 → None（官方 'no uuid'
        // shouldDrop 同位）；aci 字段 PNI 形态不当 ACI 收（官方 'invalid aci'
        // shouldDrop 同域，pni 面缺失整条跳过）。
        let record = contact_with(&[], "", "22222222-2222-4333-8444-555555555555");
        assert!(matches!(service_id_of(&record), Some(ServiceId::Pni(_))));
        let record = contact_with(&[], "", "PNI:22222222-2222-4333-8444-555555555555");
        assert!(matches!(service_id_of(&record), Some(ServiceId::Pni(_))));
        let record = contact_with(&[], "", "");
        assert_eq!(service_id_of(&record), None);
        let record = contact_with(&[], "PNI:22222222-2222-4333-8444-555555555555", "");
        assert_eq!(service_id_of(&record), None);
    }

    /// identityState → sidecar 信任态映射：Verified/Unverified 直映、
    /// Default 与坏值归 default（官方 identityState null → DEFAULT 同域）。
    #[test]
    fn trust_state_mapping() {
        assert_eq!(
            trust_state_of(IdentityState::Verified as i32),
            identity::TrustState::Verified
        );
        assert_eq!(
            trust_state_of(IdentityState::Unverified as i32),
            identity::TrustState::Unverified
        );
        assert_eq!(
            trust_state_of(IdentityState::Default as i32),
            identity::TrustState::Default
        );
        assert_eq!(trust_state_of(99), identity::TrustState::Default);
    }

    /// 姓名升级三臂（姓名升级批 2026-10-16，compat §3/§5 窗D A4）+ 复合
    /// 单点：空→非空升级、非空不同覆盖、空存储名保留本地、同名跳过；
    /// given+family 空格拼接规则建档/升级两臂同源。
    #[test]
    fn contact_name_upgrade_arms() {
        fn record(given: &str, family: &str) -> storage_proto::ContactRecord {
            storage_proto::ContactRecord {
                given_name: given.to_owned(),
                family_name: family.to_owned(),
                ..storage_proto::ContactRecord::default()
            }
        }

        // 复合单点：两段 → "given family"；单段 → 原样；两段全空 → 空串。
        assert_eq!(stored_name_of(&record("Ada", "Lovelace")), "Ada Lovelace");
        assert_eq!(stored_name_of(&record("Ada", "")), "Ada");
        assert_eq!(stored_name_of(&record("", "Lovelace")), "Lovelace");
        assert_eq!(stored_name_of(&record("", "")), "");

        // 臂一：空→非空升级（本地旧名被存储名覆盖）。
        assert_eq!(
            name_upgrade_of(&stored_name_of(&record("Ada", "Lovelace")), "stale local"),
            Some("Ada Lovelace".to_owned())
        );
        // 臂二：非空不同 → 覆盖（手机端改名收敛进引擎）。
        assert_eq!(
            name_upgrade_of(&stored_name_of(&record("Ada", "King")), "Ada Lovelace"),
            Some("Ada King".to_owned())
        );
        // 臂三：空存储名 → 保留本地（None）。
        assert_eq!(
            name_upgrade_of(&stored_name_of(&record("", "")), "Ada Lovelace"),
            None
        );
        // 同名 → 跳过（幂等 no-op）。
        assert_eq!(
            name_upgrade_of(&stored_name_of(&record("Ada", "Lovelace")), "Ada Lovelace"),
            None
        );
    }
}
