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
//!   不改（姓名权威 = 手机 contacts sync，引擎不发明 system/nickname 面）。
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
    /// contacts 表新建档数（已有档不动）。
    pub(crate) contacts_created: u32,
    /// sidecar 信任态随 identityState 收敛次数。
    pub(crate) trust_states_synced: u32,
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
pub(crate) async fn merge_contacts(
    mut context: StorageContext,
    now: u64,
) -> Result<MergeOutcome, StorageSyncError> {
    let mut outcome = MergeOutcome::default();
    let Some(master_key) = context
        .store
        .fetch_master_key()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?
    else {
        return Ok(outcome);
    };
    let storage_key = StorageServiceKey::from_master_key(&master_key);
    let read_key = StorageServiceKey::from_master_key(&master_key);
    let storage = storage_sync::open_storage_service(context.push.clone(), read_key).await?;
    let manifest = match storage.manifest().await {
        Ok(manifest) => manifest,
        Err(presage::libsignal_service::StorageServiceError::Service(
            ServiceError::NotFoundError,
        )) => return Ok(outcome),
        Err(_) => return Err(StorageSyncError::Unavailable),
    };
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
        return Ok(outcome);
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
        let Some(storage_proto::storage_record::Record::Contact(contact)) = record.record else {
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

        // 联系人建档：缺失才建（姓名/e164/profileKey 随建）；已有档不动（姓名权威 =
        // 手机 contacts sync，engine 不发明 system/nickname 面——官方
        // systemGivenName 族 set 位记档跳过）。
        if matches!(context.store.contact_by_id(&service_id).await, Ok(None)) {
            let mut name = contact.given_name.clone();
            if !contact.family_name.is_empty() {
                if !name.is_empty() {
                    name.push(' ');
                }
                name.push_str(&contact.family_name);
            }
            let contact_model = Contact {
                uuid,
                phone_number: if contact.e164.is_empty() {
                    None
                } else {
                    presage::libsignal_service::prelude::phonenumber::parse(None, &contact.e164)
                        .ok()
                },
                name,
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
    }
    Ok(outcome)
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
}
