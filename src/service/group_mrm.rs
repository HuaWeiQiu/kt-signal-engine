// SPDX-License-Identifier: AGPL-3.0-only

//! C 批：群发单请求 MRM 发送链（sender-key + GroupSendEndorsement）。
//!
//! 官方 sendToGroup.preload.ts 行为对齐（行为参照，非源码复制）：
//! - SenderKeyInfo（distributionId + 已分发设备面）落 `kv` 表，90 天或
//!   成员会话面变化复位；SKDM 只发增量成员（逐成员 1:1，官方同形）。
//! - 群载荷单请求 `PUT /v1/messages/multi_recipient`：padMessage →
//!   group_encrypt → USMC（uuid-only 群证书 + Implicit + groupId=master
//!   key）→ MRM 加密 → GSE token 举权。
//! - 目标 < 2、endorsements 缺席、MRM 链路不可用等场景 failover 回
//!   vendored 逐收件人路径（官方合法 fallback）。
//! - 409/410 修复循环（missing 建会话、stale/extra 删会话）≤ 2 轮。

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::TimeZone as _;
use presage::libsignal_service::content::{Content, ContentBody, Metadata};
use presage::libsignal_service::prelude::{ServiceError, Uuid};
use presage::libsignal_service::protocol::{
    create_sender_key_distribution_message, group_encrypt, sealed_sender_multi_recipient_encrypt,
    Aci, CiphertextMessageType, ContentHint, DeviceId, ProtocolAddress, ServiceId, SessionRecord,
    SessionStore as _, Timestamp as ProtocolTimestamp, UnidentifiedSenderMessageContent,
};
use presage::libsignal_service::push_service::DEFAULT_DEVICE_ID;
use presage::libsignal_service::sender::MessageSenderError;
use presage::libsignal_service::session_store::SessionStoreExt as _;
use presage::libsignal_service::zkgroup::api::groups::GroupSendEndorsementsResponse;
use presage::libsignal_service::zkgroup::groups::{GroupMasterKey, GroupSecretParams};
use presage::manager::{group_member_is_admin, Manager, Registered};
use presage::proto::{sync_message, DataMessage, GroupContextV2, NullMessage, SyncMessage};
use presage::store::{ContentsStore as _, Store, Thread};
use presage_store_sqlite::SqliteStore;
use prost::Message as _;
use rand::rng;

use libsignal_net::chat::ChatConnection;
use libsignal_net_chat::api::messages::{
    MultiRecipientSendAuthorization, MultiRecipientSendFailure, UnauthenticatedChatApi as _,
};
use libsignal_net_chat::api::{RequestError, Unauth};

use super::cloud_backup::connect_production_chat;
use super::{GroupSendOutcomes, SendError};

/// SenderKeyInfo 落库键前缀（`kv` 表；值 = JSON，键 = 群 id 标准 base64）。
const KV_KEY_PREFIX: &str = "kt_sender_key_info_";
/// SenderKeyInfo 有效期（官方 SenderKeyInfo.createdAt 90 天过期复位）。
const SENDER_KEY_INFO_TTL_MS: u64 = 90 * 24 * 3600 * 1000;
/// 409/410 修复循环上限（官方 MAX_RECURSION=10 的保守收敛版：两轮内未收敛
/// 说明群设备面在抖动，failover 逐收件人路径兜住正确性）。
const MRM_REPAIR_ROUNDS: usize = 2;

/// 群 sender key 分发台账（kv 落库 JSON，官方 SenderKeyInfo 对位件）。
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct SenderKeyInfo {
    distribution_id: Uuid,
    created_at_ms: u64,
    /// 成员 ACI uuid 串 → SKDM 已覆盖的设备 id（升序去重）。
    covered: BTreeMap<String, Vec<u32>>,
}

impl SenderKeyInfo {
    fn new(now_ms: u64) -> Self {
        Self {
            distribution_id: Uuid::new_v4(),
            created_at_ms: now_ms,
            covered: BTreeMap::new(),
        }
    }

    fn expired(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.created_at_ms) >= SENDER_KEY_INFO_TTL_MS
    }
}

async fn load_sender_key_info(store: &SqliteStore, group_id: &str) -> Option<SenderKeyInfo> {
    let raw = store
        .kt_kv_get(&format!("{KV_KEY_PREFIX}{group_id}"))
        .await
        .ok()??;
    serde_json::from_str(&raw).ok()
}

async fn save_sender_key_info(store: &SqliteStore, group_id: &str, info: &SenderKeyInfo) {
    if let Ok(raw) = serde_json::to_string(info) {
        let _ = store
            .kt_kv_set(&format!("{KV_KEY_PREFIX}{group_id}"), &raw)
            .await;
    }
}

/// 成员当前会话设备面：主设备 + 有会话的子设备（升序去重）。
async fn session_device_ids<S: Store>(aci_store: &S::AciStore, aci: Uuid) -> Vec<u32> {
    let service_id = ServiceId::from(Aci::from(aci));
    let mut ids: Vec<u32> = aci_store
        .get_sub_device_sessions(&service_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(u32::from)
        .collect();
    ids.push(u32::from(*DEFAULT_DEVICE_ID));
    ids.sort_unstable();
    ids.dedup();
    let mut with_sessions = Vec::with_capacity(ids.len());
    for id in ids {
        let Ok(device) = DeviceId::try_from(id) else {
            continue;
        };
        if matches!(
            aci_store
                .load_session(&service_id.to_protocol_address(device))
                .await,
            Ok(Some(_))
        ) {
            with_sessions.push(id);
        }
    }
    with_sessions
}

/// expire_timer 线程态回填（vendored restore_thread_timer 同构，MRM 路径
/// 自行回填；failover 路径由 vendored send_message_to_group 内部处理）。
fn restore_expire_timer(body: &mut DataMessage, stored: Option<(u32, u32)>) {
    if body.expire_timer.is_none() {
        body.expire_timer = stored.and_then(|(t, _)| if t == 0 { None } else { Some(t) });
        body.expire_timer_version = Some(stored.map(|(_, v)| v).unwrap_or_default());
    } else {
        body.expire_timer_version = Some(stored.map(|(_, v)| v).unwrap_or_default() + 1);
    }
}

/// padMessage（官方 16 字节对齐：长度 +1 向上取整到 16 倍数，末位 0x80）。
fn pad_message(plaintext: &[u8]) -> Vec<u8> {
    let padded_len = (plaintext.len() + 1).div_ceil(16) * 16;
    let mut padded = vec![0u8; padded_len];
    padded[..plaintext.len()].copy_from_slice(plaintext);
    padded[plaintext.len()] = 0x80;
    padded
}

/// presage::Error → per-recipient 失败面（outcomes 投影用；与 vendored
/// failover 路径的 SendMessageResult.err() 同型）。
fn outcome_failure_of(error: presage::Error<<SqliteStore as Store>::Error>) -> MessageSenderError {
    match error {
        presage::Error::MessageSenderError(inner) => *inner,
        presage::Error::ServiceError(inner) => MessageSenderError::ServiceError(inner),
        presage::Error::ProtocolError(inner) => MessageSenderError::ProtocolError(inner),
        other => MessageSenderError::ServiceError(ServiceError::SendError {
            reason: other.to_string(),
        }),
    }
}

/// MRM 单请求发送的失败分型（群级失败不投影成员级；uuids404 未注册面在
/// Ok 响应的 unregistered_ids 里，不是失败分型）。
enum MrmSendFailure {
    /// per-recipient 设备面失配（409/410，进修复循环）。
    Mismatched(Vec<ServiceId>),
    /// 群级：链路/授权不可用，failover 逐收件人路径。
    Fatal,
}

/// 群发入口：MRM 主路径 + vendored failover（设计冻结：不做并行 fan-out
/// 半吊子中间态——要么单请求 MRM，要么逐收件人，两条路都是官方合法形态）。
pub(crate) async fn dispatch_group_message(
    service: &Arc<super::Service>,
    manager: &mut Manager<SqliteStore, Registered>,
    group_id: &str,
    body: DataMessage,
    timestamp: u64,
) -> Result<GroupSendOutcomes, SendError> {
    let Some(master_key) = super::decode_group_id(group_id) else {
        return Err(SendError::Internal);
    };
    let thread = Thread::Group(
        <[u8; 32]>::try_from(master_key.as_slice()).map_err(|_| SendError::Internal)?,
    );

    // 群态 + GSE 一次取回（fetch 失败回 store 缓存；群不存在 → failover
    // 走 vendored 的 UnknownGroup 错误面）。
    let fetched = manager
        .fetch_group_with_endorsements(&master_key)
        .await
        .map_err(super::send_error_of)?;

    // DataMessage 注入群上下文（接收方进群线程的唯一依据；B 批审计缺
    // group_v2 的修复，failover 路径同样受益）。
    let mut body = body;
    body.group_v2 = Some(GroupContextV2 {
        master_key: Some(master_key.clone()),
        revision: fetched.as_ref().map(|(group, _)| group.revision),
        group_change: None,
    });

    let Some((group, endorsements)) = fetched else {
        return failover(manager, &master_key, body, timestamp).await;
    };

    let own_aci = manager.registration_data().service_ids.aci();
    let own_uuid: Uuid = own_aci.into();
    let own_aci = Aci::from(own_uuid);
    if group.announcements_only && !group_member_is_admin(&group, &own_aci) {
        return Err(SendError::AnnouncementsOnly);
    }

    let mut targets: Vec<Uuid> = group
        .members
        .iter()
        .map(|member| member.aci.into())
        .filter(|aci| *aci != own_uuid)
        .collect();
    targets.sort();
    targets.dedup();

    // 官方 fallback：目标 < 2 走逐收件人（Desktop sendToGroup 同语义）。
    if targets.len() < 2 {
        return failover(manager, &master_key, body, timestamp).await;
    }

    // —— SKDM 阶段：增量分发 sender key（失败成员进 outcomes 并剔除）——
    let now = super::now_ms();
    let own_address = ServiceId::from(own_aci).to_protocol_address(manager.device_id());
    let mut info = match load_sender_key_info(manager.store(), group_id).await {
        Some(info) if !info.expired(now) => info,
        _ => SenderKeyInfo::new(now),
    };

    let mut aci_store = manager.store().aci_protocol_store();
    let mut outcomes: GroupSendOutcomes = Vec::with_capacity(targets.len());
    let mut mrm_targets: Vec<Uuid> = Vec::with_capacity(targets.len());
    for target in &targets {
        let key = target.to_string();
        let devices = session_device_ids::<SqliteStore>(&aci_store, *target).await;
        if info.covered.get(&key).map(Vec::as_slice) == Some(devices.as_slice()) {
            mrm_targets.push(*target);
            continue;
        }
        let skdm = match create_sender_key_distribution_message(
            &own_address,
            info.distribution_id,
            &mut aci_store,
            &mut rng(),
        )
        .await
        {
            Ok(skdm) => skdm,
            Err(error) => {
                outcomes.push((
                    ServiceId::from(Aci::from(*target)),
                    Some(MessageSenderError::ProtocolError(error)),
                ));
                continue;
            }
        };
        match manager
            .send_message(
                ServiceId::from(Aci::from(*target)),
                ContentBody::SenderKeyDistributionMessage(skdm),
                timestamp,
            )
            .await
        {
            Ok(()) => {
                info.covered.insert(key, devices);
                mrm_targets.push(*target);
            }
            Err(error) => {
                outcomes.push((
                    ServiceId::from(Aci::from(*target)),
                    Some(outcome_failure_of(error)),
                ));
            }
        }
    }
    save_sender_key_info(manager.store(), group_id, &info).await;

    if mrm_targets.len() < 2 {
        // SKDM 增量后目标收敛到 < 2（全部失败/仅剩单成员的边角）：官方
        // fallback 面兜住（vendored 路径幂等重发，正确性不受影响）。
        return failover(manager, &master_key, body, timestamp).await;
    }

    // —— GSE token（无 endorsements / 验签失败 → failover）——
    let Some(endorsement_bytes) = endorsements else {
        return failover(manager, &master_key, body, timestamp).await;
    };
    let token = match build_group_send_token(
        &endorsement_bytes,
        &master_key,
        &mrm_targets,
        own_aci,
        super::now_ms(),
    ) {
        Ok(token) => token,
        Err(_) => return failover(manager, &master_key, body, timestamp).await,
    };

    // —— 载荷明文：expire 回填 → Content 编码 → pad（循环外一次；修复轮
    // 只变目标会话面，明文不变）——
    restore_expire_timer(
        &mut body,
        manager.store().expire_timer(&thread).await.unwrap_or(None),
    );
    let plaintext = presage::proto::Content {
        content: Some(presage::proto::content::Content::DataMessage(body.clone())),
        sender_key_distribution_message: None,
        pni_signature_message: None,
    }
    .encode_to_vec();

    // —— 单请求发送 + 409/410 修复循环（每轮重枚举会话设备面并重加密：
    // 修复轮删除/重建会话后，载荷的 per-device 目标集必须跟着变）——
    let mut rounds = 0usize;
    let unregistered: Vec<ServiceId> = loop {
        let mut destinations: Vec<(ProtocolAddress, SessionRecord)> = Vec::new();
        for target in &mrm_targets {
            let service_id = ServiceId::from(Aci::from(*target));
            for device in session_device_ids::<SqliteStore>(&aci_store, *target).await {
                let Ok(device_id) = DeviceId::try_from(device) else {
                    continue;
                };
                if let Ok(Some(record)) = aci_store
                    .load_session(&service_id.to_protocol_address(device_id))
                    .await
                {
                    destinations.push((service_id.to_protocol_address(device_id), record));
                }
            }
        }
        destinations.sort_by(|a, b| a.0.name().cmp(b.0.name()));

        let skm = match group_encrypt(
            &mut aci_store,
            &own_address,
            info.distribution_id,
            &pad_message(&plaintext),
            &mut rng(),
        )
        .await
        {
            Ok(skm) => skm,
            Err(_) => return failover(manager, &master_key, body, timestamp).await,
        };

        // 群证书 uuid-only（vendored sender_certificate 即 uuid-only 面），
        // contentHint 群发恒 Implicit（重发无意义），groupId = master key。
        let usmc = match manager.sender_certificate().await {
            Ok(cert) => UnidentifiedSenderMessageContent::new(
                CiphertextMessageType::SenderKey,
                cert,
                skm.serialized().to_vec(),
                ContentHint::Implicit,
                Some(master_key.clone()),
            ),
            Err(_) => return failover(manager, &master_key, body, timestamp).await,
        };
        let Ok(usmc) = usmc else {
            return failover(manager, &master_key, body, timestamp).await;
        };

        let address_refs: Vec<&ProtocolAddress> = destinations.iter().map(|(a, _)| a).collect();
        let record_refs: Vec<&SessionRecord> = destinations.iter().map(|(_, r)| r).collect();
        let payload = match sealed_sender_multi_recipient_encrypt(
            &address_refs,
            &record_refs,
            [ServiceId::from(own_aci)],
            &usmc,
            &aci_store,
            &mut rng(),
        )
        .await
        {
            Ok(payload) => payload,
            Err(_) => return failover(manager, &master_key, body, timestamp).await,
        };

        match send_mrm(service, &payload, timestamp, token.clone()).await {
            Ok(response) => break response.unregistered_ids,
            Err(MrmSendFailure::Mismatched(accounts)) => {
                rounds += 1;
                if rounds > MRM_REPAIR_ROUNDS {
                    return failover(manager, &master_key, body, timestamp).await;
                }
                repair_device_mismatch::<SqliteStore>(
                    manager,
                    &mut aci_store,
                    &accounts,
                    &own_address,
                    info.distribution_id,
                )
                .await;
            }
            Err(MrmSendFailure::Fatal) => {
                return failover(manager, &master_key, body, timestamp).await;
            }
        }
    };

    let unregistered: std::collections::HashSet<ServiceId> = unregistered.into_iter().collect();
    for target in mrm_targets {
        let service_id = ServiceId::from(Aci::from(target));
        if unregistered.contains(&service_id) {
            outcomes.push((
                service_id,
                Some(MessageSenderError::NotFound { service_id }),
            ));
        } else {
            outcomes.push((service_id, None));
        }
    }

    // —— 发后落库 + 自同步 transcript（官方 sendToGroup 收尾同构）——
    save_outgoing_group_message(manager, &thread, &body, timestamp).await?;
    let sent = sync_message::Sent {
        destination_service_id: None,
        timestamp: Some(timestamp),
        message: Some(body),
        ..sync_message::Sent::default()
    };
    let _ = manager
        .send_message(
            own_aci,
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::Sent(sent)),
                ..Default::default()
            }),
            timestamp,
        )
        .await;

    Ok(outcomes)
}

/// vendored 逐收件人 failover（官方 fallback；body 已带 group_v2，接收方
/// 群线程正确；expire timer 由 vendored 内部 restore）。
async fn failover(
    manager: &mut Manager<SqliteStore, Registered>,
    master_key: &[u8],
    body: DataMessage,
    timestamp: u64,
) -> Result<GroupSendOutcomes, SendError> {
    manager
        .send_message_to_group(master_key, ContentBody::DataMessage(body), timestamp)
        .await
        .map_err(super::send_error_of)
}

/// GSE 验签出 token：endorsements 含 self（服务器签发口径），combine 时
/// 剔除 self（官方 set 语义：combine(非self) == remove(self, combine(全))）。
fn build_group_send_token(
    endorsement_bytes: &[u8],
    master_key: &[u8],
    targets: &[Uuid],
    own_aci: Aci,
    now_ms: u64,
) -> Result<zkgroup::groups::GroupSendFullToken, zkgroup::ZkGroupVerificationFailure> {
    let response: GroupSendEndorsementsResponse =
        zkgroup::deserialize(endorsement_bytes).map_err(|_| zkgroup::ZkGroupVerificationFailure)?;
    let expiration = response.expiration();
    let group_secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(
        <[u8; 32]>::try_from(master_key).map_err(|_| zkgroup::ZkGroupVerificationFailure)?,
    ));
    let own_service_id = ServiceId::from(own_aci);
    let mut user_ids: Vec<ServiceId> = targets
        .iter()
        .map(|aci| ServiceId::from(Aci::from(*aci)))
        .collect();
    user_ids.push(own_service_id);
    let received = response.receive_with_service_ids_single_threaded(
        user_ids.clone(),
        zkgroup::Timestamp::from_epoch_seconds(now_ms / 1000),
        &group_secret_params,
        super::zkgroup_consts::legacy_server_public_params(),
    )?;
    let combined = zkgroup::api::groups::GroupSendEndorsement::combine(
        received
            .into_iter()
            .zip(user_ids)
            .filter(|(_, id)| *id != own_service_id)
            .map(|(endorsement, _)| endorsement.decompressed),
    );
    Ok(combined
        .to_token(group_secret_params)
        .into_full_token(expiration))
}

/// 单请求 MRM 发送（Service 级 unauth 连接缓存，传输失败摘除重连一次）。
async fn send_mrm(
    service: &Arc<super::Service>,
    payload: &[u8],
    timestamp: u64,
    token: zkgroup::groups::GroupSendFullToken,
) -> Result<libsignal_net_chat::api::messages::MultiRecipientMessageResponse, MrmSendFailure> {
    for attempt in 0..2 {
        let mut guard = service.mrm_chat.lock().await;
        if guard.is_none() {
            *guard = Some(
                connect_production_chat()
                    .await
                    .map_err(|_| MrmSendFailure::Fatal)?,
            );
        }
        let api: &Unauth<ChatConnection> = guard.as_ref().unwrap().into();
        let result = api
            .send_multi_recipient_message(
                Bytes::from(payload.to_vec()),
                ProtocolTimestamp::from_epoch_millis(timestamp),
                MultiRecipientSendAuthorization::Group(token.clone()),
                false,
                true,
            )
            .await;
        match result {
            Ok(response) => return Ok(response),
            Err(RequestError::Other(MultiRecipientSendFailure::MismatchedDevices(list))) => {
                return Err(MrmSendFailure::Mismatched(
                    list.into_iter().map(|mismatch| mismatch.account).collect(),
                ));
            }
            Err(RequestError::Other(MultiRecipientSendFailure::Unauthorized)) => {
                return Err(MrmSendFailure::Fatal);
            }
            Err(_) => {
                // 传输层失败：摘除缓存连接重连一次；仍失败 → failover。
                *guard = None;
                if attempt == 1 {
                    return Err(MrmSendFailure::Fatal);
                }
            }
        }
    }
    Err(MrmSendFailure::Fatal)
}

/// 409/410 修复：失配设备面整体删会话重建（409 missing 无会话、410
/// stale/extra 已失效——统一重建收敛更快，vendored 1:1 路径同型拆删）；
/// 同 distribution_id 重发 SKDM（成员要的正是本次群发的 sender key），
/// send 内部拉 prekey 建链，失败静默（下一轮 MRM 仍报失配 → 轮次耗尽 →
/// failover）。
async fn repair_device_mismatch<S: Store>(
    manager: &mut Manager<SqliteStore, Registered>,
    aci_store: &mut S::AciStore,
    accounts: &[ServiceId],
    own_address: &ProtocolAddress,
    distribution_id: Uuid,
) {
    for account in accounts {
        let ServiceId::Aci(aci) = account else {
            continue;
        };
        let service_id = ServiceId::Aci(*aci);
        let devices = session_device_ids::<S>(aci_store, (*aci).into()).await;
        for device in devices {
            let Ok(device_id) = DeviceId::try_from(device) else {
                continue;
            };
            let _ = aci_store
                .delete_service_addr_device_session(&service_id.to_protocol_address(device_id))
                .await;
        }
    }
    for account in accounts {
        let ServiceId::Aci(aci) = account else {
            continue;
        };
        let Ok(skdm) = create_sender_key_distribution_message(
            own_address,
            distribution_id,
            aci_store,
            &mut rng(),
        )
        .await
        else {
            continue;
        };
        let _ = manager
            .send_message(
                ServiceId::Aci(*aci),
                ContentBody::SenderKeyDistributionMessage(skdm),
                super::now_ms(),
            )
            .await;
    }
}

/// 发后落库（vendored save_message 群线程语义同构：remote delete 命中既有
/// 行 → 置空替换；否则追加新行）。
async fn save_outgoing_group_message(
    manager: &mut Manager<SqliteStore, Registered>,
    thread: &Thread,
    body: &DataMessage,
    timestamp: u64,
) -> Result<(), SendError> {
    let store = manager.store().clone();
    if let Some(delete) = &body.delete {
        if let Some(target) = delete.target_sent_timestamp {
            if let Ok(Some(mut existing)) = store.message(thread, target).await {
                existing.metadata.sender = Aci::from(Uuid::nil()).into();
                existing.body = ContentBody::NullMessage(NullMessage::default());
                let _ = store.save_message(thread, existing).await;
                return Ok(());
            }
        }
    }
    let content = Content {
        metadata: Metadata {
            sender: manager.registration_data().service_ids.aci().into(),
            destination: manager.registration_data().service_ids.aci().into(),
            sender_device: manager.device_id(),
            server_guid: None,
            client_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
            server_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            pni_verified: None,
            story: None,
        },
        body: ContentBody::DataMessage(body.clone()),
    };
    store
        .save_message(thread, content)
        .await
        .map_err(|_| SendError::Internal)
}

// —— 纯函数单测（wire 契约钉面）——
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pad_message_aligns_to_16_with_terminal_marker() {
        // 空 明文：1 pad 字节 → 16。
        assert_eq!(pad_message(b"").len(), 16);
        assert_eq!(pad_message(b""), {
            let mut v = vec![0u8; 16];
            v[0] = 0x80;
            v
        });
        // 15 字节：+1 pad → 16。
        assert_eq!(pad_message(&[7u8; 15]).len(), 16);
        // 16 字节：+1 pad → 32（至少一个 pad 字节）。
        let padded = pad_message(&[7u8; 16]);
        assert_eq!(padded.len(), 32);
        assert_eq!(padded[16], 0x80);
        assert!(padded[..16] == [7u8; 16]);
    }

    #[test]
    fn sender_key_info_roundtrips_and_expires() {
        let info = SenderKeyInfo::new(1_000);
        assert!(!info.expired(1_000 + SENDER_KEY_INFO_TTL_MS - 1));
        assert!(info.expired(1_000 + SENDER_KEY_INFO_TTL_MS));
        let raw = serde_json::to_string(&info).unwrap();
        let back: SenderKeyInfo = serde_json::from_str(&raw).unwrap();
        assert_eq!(back.distribution_id, info.distribution_id);
        assert_eq!(back.created_at_ms, info.created_at_ms);
    }

    /// GSE 全链往返（本地造证）：issue → 字节 deserialize → receive（含
    /// self）→ zip 对位剔 self → combine(decompressed) → token → 服务端
    /// 视角 verify。生产 `build_group_send_token` 的公钥钉死 Signal 生产
    /// 参数（配对密钥在 Signal 服务端，本地无法签发），故以本地
    /// `ServerSecretParams` 镜像同一调用链，钉死 zkgroup API 组合语义：
    /// receive 结果与 user_ids 顺序对位、剔 self 后 combine、过期即拒。
    #[test]
    fn gse_roundtrip_issue_receive_combine_verify() {
        use zkgroup::api::groups::{
            GroupSendDerivedKeyPair, GroupSendEndorsement, GroupSendEndorsementsResponse,
        };
        use zkgroup::ServerSecretParams;

        let server_secret = ServerSecretParams::generate([3u8; 32]);
        let server_public = server_secret.get_public_params();

        let group_params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new([9u8; 32]));

        let member_ids: Vec<ServiceId> = [
            Uuid::from_u128(0xA1),
            Uuid::from_u128(0xB2),
            Uuid::from_u128(0xC3),
        ]
        .iter()
        .map(|aci| ServiceId::from(Aci::from(*aci)))
        .collect();

        let now = zkgroup::Timestamp::from_epoch_seconds(1_760_000_000);
        let expiration = GroupSendEndorsementsResponse::default_expiration(now);
        let derived = GroupSendDerivedKeyPair::for_expiration(expiration, &server_secret);
        let response = GroupSendEndorsementsResponse::issue(
            member_ids
                .iter()
                .map(|id| group_params.encrypt_service_id(*id)),
            &derived,
            [7u8; 32],
        );

        // 生产入口同参：字节化后 deserialize（build_group_send_token 首步）。
        let response: GroupSendEndorsementsResponse =
            zkgroup::deserialize(&zkgroup::serialize(&response)).unwrap();

        let received = response
            .receive_with_service_ids_single_threaded(
                member_ids.clone(),
                now,
                &group_params,
                &server_public,
            )
            .unwrap();

        // build_group_send_token 镜像：zip 对位 → 剔 self → combine。
        let combined = GroupSendEndorsement::combine(
            received
                .into_iter()
                .zip(member_ids.clone())
                .filter(|(_, id)| *id != member_ids[0])
                .map(|(endorsement, _)| endorsement.decompressed),
        );
        let token = combined.to_token(group_params).into_full_token(expiration);

        // 服务端视角：非 self 成员集合验签通过；过期一秒必拒。
        let verify_key = GroupSendDerivedKeyPair::for_expiration(expiration, &server_secret);
        token
            .verify(member_ids[1..].iter().copied(), now, &verify_key)
            .expect("非 self 成员集合验签通过");
        assert!(
            token
                .verify(
                    member_ids[1..].iter().copied(),
                    expiration.add_seconds(1),
                    &verify_key
                )
                .is_err(),
            "过期 token 验签必拒"
        );
    }
}
