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
//!
//! R1 性能对齐（群发稳态 0 次额外 HTTPS，MRM 主请求除外）：群态+GSE 本地
//! 优先（group_cache 落 kv，官方 2h-2d 窗 + 成员面失效判定），miss 才
//! fetch；服务器 403（token 过期/成员面漂移）→ 缓存废弃 → 群态+GSE 重取
//! 一次 → 重试（官方 didRefreshGroupState 同型），再失败 failover。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

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
    /// GSE token 层拒发（403）：进「刷群态一次→重试」臂（官方
    /// didRefreshGroupState 同型）。
    Unauthorized,
    /// 群级：链路/授权不可用，failover 逐收件人路径。
    Fatal,
}

/// MRM unauth 连接池上限（R2 资源公平：进程级连接上限 2）。
///
/// 审计 §9.6 R4：`mrm_chat` 原为进程级单槽未鉴权共享连接，一个账号的失败
/// 会摘除所有账号的连接；R4 修法 = 按账号隔离。上限 2 是 R2 的引擎连接
/// 预算（审计「engine 连接池上限 2」）：超出时**fail closed**（该次群发走
/// vendored 逐收件人 failover，正确性不变），不排队阻塞、不驱逐使用中的
/// 连接。
///
/// 官方对应面：Desktop 单账号 `SocketManager` 只有一个 unauth 槽
/// （`ts/textsecure/SocketManager.preload.ts:733` 起），无多账号池；per-account
/// 池是 `[KT 适配]`。
pub(crate) const MRM_CONNECTION_LIMIT: usize = 2;

/// 单个账号的 MRM 连接槽（按账号隔离；连接惰性建、失败只摘本槽）。
pub(crate) struct MrmSlot {
    generation: u64,
    connection: tokio::sync::Mutex<Option<ChatConnection>>,
}

/// MRM 连接池：账号 → 槽；最多 [`MRM_CONNECTION_LIMIT`] 个活槽。
pub(crate) struct MrmPool {
    limit: usize,
    slots: StdMutex<HashMap<String, Arc<MrmSlot>>>,
    next_generation: AtomicU64,
}

impl MrmPool {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            slots: StdMutex::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
        }
    }

    /// 取账号槽：命中即复用；未满则新建；已满且存在**空闲**槽（仅池持有
    /// 引用、互斥锁可立即取到）则淘汰该槽让位；否则 None（fail-closed）。
    ///
    /// 空闲判定只看 `Arc::strong_count == 1`：有在途发送（持槽 Arc 或等锁）
    /// 的槽永不被淘汰，避免打断其他账号的 MRM 请求。
    pub(crate) fn slot_for(&self, account: &str) -> Option<Arc<MrmSlot>> {
        let mut slots = self.slots.lock().expect("mrm pool lock");
        if let Some(slot) = slots.get(account) {
            return Some(Arc::clone(slot));
        }
        if slots.len() >= self.limit {
            // 空闲槽按代次最旧优先淘汰（代次 = 创建序，确定性可测）。
            let evicted = slots
                .iter()
                .filter(|(_, slot)| Arc::strong_count(slot) == 1)
                .filter(|(_, slot)| slot.connection.try_lock().is_ok())
                .min_by_key(|(_, slot)| slot.generation)
                .map(|(key, _)| key.clone());
            slots.remove(&evicted?);
        }
        let slot = Arc::new(MrmSlot {
            generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            connection: tokio::sync::Mutex::new(None),
        });
        slots.insert(account.to_owned(), Arc::clone(&slot));
        Some(slot)
    }

    /// 账号删除/重链：摘除该账号槽（在途发送持的 Arc 自然走完并落弃）。
    pub(crate) fn remove(&self, account: &str) {
        self.slots.lock().expect("mrm pool lock").remove(account);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots.lock().expect("mrm pool lock").len()
    }
}

impl MrmSlot {
    /// 槽代次（观测/测试用：淘汰后新槽代次不同）。
    #[cfg(test)]
    fn generation(&self) -> u64 {
        self.generation
    }
}

/// 群发入口：MRM 主路径 + vendored failover（设计冻结：不做并行 fan-out
/// 半吊子中间态——要么单请求 MRM，要么逐收件人，两条路都是官方合法形态）。
pub(crate) async fn dispatch_group_message(
    service: &Arc<super::Service>,
    account: &str,
    manager: &mut Manager<SqliteStore, Registered>,
    group_id: &str,
    body: DataMessage,
    timestamp: u64,
    self_sync: super::GroupSelfSync<'_>,
) -> Result<GroupSendOutcomes, SendError> {
    let Some(master_key) = super::decode_group_id(group_id) else {
        return Err(SendError::Internal);
    };
    let thread = Thread::Group(
        <[u8; 32]>::try_from(master_key.as_slice()).map_err(|_| SendError::Internal)?,
    );

    // 群态 + GSE 本地优先（R1）：store 群态 + kv GSE 双校验（官方发送前
    // 本地读面同位，groupSendEndorsements.preload.ts:363-397），命中即零
    // 网络；miss/失效才走 fetch（官方失效→刷群态一次→重试同语义，此处刷
    // 取在发送前完成）。fetch 失败回 store 缓存；群不存在 → failover 走
    // vendored 的 UnknownGroup 错误面。
    // KT (send latency probe): 获取段打点（群发送延迟批 2026-10-18）——
    // deliver-group 细分第一段，cache=hit|network 固定词；纯日志零行为变化。
    let group_refresh_t0 = std::time::Instant::now();
    let (fetched, group_refresh_hit) =
        match super::group_cache::cached_group_with_endorsements(manager, &master_key).await {
            Some(hit) => (Some(hit), true),
            None => {
                let fresh = manager
                    .fetch_group_with_endorsements(&master_key)
                    .await
                    .map_err(super::send_error_of)?;
                super::group_cache::store_endorsements(manager.store(), &master_key, &fresh).await;
                (fresh, false)
            }
        };
    tracing::warn!(
        target: "send_latency_probe",
        stage = "engine.group-refresh",
        cache = if group_refresh_hit { "hit" } else { "network" },
        dur_ms = group_refresh_t0.elapsed().as_millis() as u64,
        ts_id = timestamp,
        "send-latency-probe",
    );

    // DataMessage 注入群上下文（接收方进群线程的唯一依据；B 批审计缺
    // group_v2 的修复，failover 路径同样受益）。
    let mut body = body;
    body.group_v2 = Some(GroupContextV2 {
        master_key: Some(master_key.clone()),
        revision: fetched.as_ref().map(|(group, _)| group.revision),
        group_change: None,
    });

    let Some((group, endorsements)) = fetched else {
        return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
    };

    // KT (sender key interop probe): GSE 取回结局打点（send-latency-probe
    // 词表 additive，纯日志零行为）。官方语义注记：GSE 由服务端签发随群
    // 拉取下发（客户端无上传面），absent 词用于真机区分「服务端未签发
    // 我们成员的 endorsement」与「取回面本身故障」。
    tracing::warn!(
        target: "send_latency_probe",
        stage = "engine.gse",
        outcome = if endorsements.is_some() { "present" } else { "absent" },
        cache = if group_refresh_hit { "hit" } else { "network" },
        ts_id = timestamp,
        "send-latency-probe",
    );

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

    // 群腿零收件人防御（2026-10-16 批，compat §3/§5）：成员过滤自身后
    // 目标集为空 → 确定性失败。此前掉进下方 fallback → vendored 空循环
    // → Ok 空 outcomes → send 应答 Ok((ts, [])) 整体成功（静默真空成功，
    // 三层无防御）。NoRecipients = 确定未送达（请求从未构造收件人面）。
    if targets.is_empty() {
        return Err(SendError::NoRecipients);
    }

    // 官方 fallback：目标 < 2 走逐收件人（Desktop sendToGroup 同语义）。
    if targets.len() < 2 {
        return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
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
        return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
    }

    // —— GSE token 循环内建（R1 失效→刷一次→重试，官方
    // profiles.preload.ts:422-425「didRefreshGroupState → 重取再试」同型）：
    // 服务器 403（GSE token 过期/成员面漂移）→ 废弃 kv 缓存 → 群态+GSE
    // 重取一次 → 修订 body revision → 重建 token 重试；再 403 → failover。
    let mut endorsement_bytes = endorsements;
    let mut gse_refreshed = false;

    // —— 载荷明文基准：expire 回填（循环外一次：expire_timer_version 不因
    // 修复/刷新轮重复自增；各轮按当前 body 现场编码）——
    restore_expire_timer(
        &mut body,
        manager.store().expire_timer(&thread).await.unwrap_or(None),
    );

    // —— 单请求发送 + 409/410 修复循环（每轮重枚举会话设备面并重加密：
    // 修复轮删除/重建会话后，载荷的 per-device 目标集必须跟着变）——
    let mut rounds = 0usize;
    // S2a 打点：MRM 主路径计数（修复环前即 MRM 链路已锁定）。
    super::perf_metrics::add(super::perf_metrics::keys::SEND_GROUP_MRM, 1);
    let unregistered: Vec<ServiceId> = loop {
        let Some(endorsement) = endorsement_bytes.clone() else {
            return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
        };
        let token = match build_group_send_token(
            &endorsement,
            &master_key,
            &mrm_targets,
            own_aci,
            super::now_ms(),
        ) {
            Ok(token) => token,
            Err(_) => {
                return failover(service, manager, &self_sync, &master_key, body, timestamp).await
            }
        };
        let plaintext = presage::proto::Content {
            content: Some(presage::proto::content::Content::DataMessage(body.clone())),
            sender_key_distribution_message: None,
            pni_signature_message: None,
        }
        .encode_to_vec();

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

        // KT (send latency probe): 加密段打点（群发送延迟批 2026-10-18）——
        // deliver-group 细分第二段 = sender key 加密 + 证书装配 + 逐设备
        // sealed 封装（修复轮每轮一条，round 固定词）；纯日志零行为变化。
        let encrypt_t0 = std::time::Instant::now();
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
            Err(_) => {
                return failover(service, manager, &self_sync, &master_key, body, timestamp).await
            }
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
            Err(_) => {
                return failover(service, manager, &self_sync, &master_key, body, timestamp).await
            }
        };
        let Ok(usmc) = usmc else {
            return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
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
            Err(_) => {
                return failover(service, manager, &self_sync, &master_key, body, timestamp).await
            }
        };
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.group-encrypt",
            round = rounds,
            dur_ms = encrypt_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );

        let wire_t0 = std::time::Instant::now();
        let wire_result = send_mrm(service, account, &payload, timestamp, token.clone()).await;
        // KT (send latency probe): wire 段打点——deliver-group 细分第三段 =
        // MRM 单请求 WS/HTTPS 往返（失败轮 outcome=err，修复/刷新后重试照付）。
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.group-wire",
            round = rounds,
            outcome = if wire_result.is_ok() { "ok" } else { "err" },
            dur_ms = wire_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
        match wire_result {
            Ok(response) => break response.unregistered_ids,
            Err(MrmSendFailure::Mismatched(accounts)) => {
                rounds += 1;
                // S2a 打点：409/410 修复轮计数（每轮一次）。
                super::perf_metrics::add(super::perf_metrics::keys::SEND_GROUP_REPAIR_ROUND, 1);
                if rounds > MRM_REPAIR_ROUNDS {
                    return failover(service, manager, &self_sync, &master_key, body, timestamp)
                        .await;
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
            Err(MrmSendFailure::Unauthorized) => {
                // 官方 refresh-once-retry（profiles.preload.ts:422-425）：
                // 403 只刷一次；缓存废弃 → 群态+GSE 重取 → 修订 revision →
                // 下轮重建 token。取回失败/仍 403 → failover 兜底。
                if gse_refreshed {
                    return failover(service, manager, &self_sync, &master_key, body, timestamp)
                        .await;
                }
                gse_refreshed = true;
                super::group_cache::invalidate_endorsements(manager.store(), &master_key).await;
                let refreshed = match manager.fetch_group_with_endorsements(&master_key).await {
                    Ok(fresh) => fresh,
                    Err(_) => {
                        return failover(service, manager, &self_sync, &master_key, body, timestamp)
                            .await
                    }
                };
                super::group_cache::store_endorsements(manager.store(), &master_key, &refreshed)
                    .await;
                let Some((fresh_group, fresh_endorsements)) = refreshed else {
                    return failover(service, manager, &self_sync, &master_key, body, timestamp)
                        .await;
                };
                endorsement_bytes = fresh_endorsements;
                body.group_v2 = Some(GroupContextV2 {
                    master_key: Some(master_key.clone()),
                    revision: Some(fresh_group.revision),
                    group_change: None,
                });
            }
            Err(MrmSendFailure::Fatal) => {
                return failover(service, manager, &self_sync, &master_key, body, timestamp).await;
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
    let succeeded: Vec<ServiceId> = outcomes
        .iter()
        .filter(|(_, error)| error.is_none())
        .map(|(service_id, _)| *service_id)
        .collect();
    let sent = build_group_sent_transcript(body, timestamp, &succeeded);
    match self_sync {
        super::GroupSelfSync::Blocking => {
            if let Err(error) = manager
                .send_message(
                    own_aci,
                    ContentBody::SynchronizeMessage(SyncMessage {
                        content: Some(sync_message::Content::Sent(sent)),
                        ..Default::default()
                    }),
                    timestamp,
                )
                .await
            {
                // best-effort：群成员已达，副本失败不连坐主发送结局（官方
                // wrapWithSyncMessageSend 同语义），记 stderr（connector 转发通道）。
                eprintln!("kt-signal-engine: group sent transcript failed: {error}");
            }
        }
        // 修2 send-done 语义批：send 文本群面转录腿后台化——行为零漂移
        //（同 manager.send_message 载荷），仅时序移出主应答。
        super::GroupSelfSync::Deferred(account) => {
            super::spawn_self_sync_leg(
                service,
                account,
                super::BackgroundSelfLeg::GroupManaged(SyncMessage {
                    content: Some(sync_message::Content::Sent(sent)),
                    ..Default::default()
                }),
                timestamp,
            );
        }
    }

    Ok(outcomes)
}

/// 群发 Sent 转录构造（官方 sendToGroup → wrapWithSyncMessageSend 收尾同构，
/// wire additive 批次 T）：destination 双形态恒无——群线程身份由 message
/// groupV2 携带，官方群转录同位；expirationStartTimestamp 按 expire_timer
/// 在场钉发送时刻（直聊面 vendored create_multi_device_sent_transcript_content
/// 同一规则；timer 关闭/无 timer 群 = 缺席）；UDS 逐成功收件人 string+binary
/// 双写、unidentified 恒 true（MRM 全程 sealed sender）、PNI identity key
/// 恒无（官方 SignalService.proto「Only set for PNI destinations」）。
fn build_group_sent_transcript(
    body: DataMessage,
    timestamp: u64,
    succeeded: &[ServiceId],
) -> sync_message::Sent {
    let expiration_start_timestamp = body.expire_timer.map(|_| timestamp);
    sync_message::Sent {
        destination_service_id: None,
        destination_service_id_binary: None,
        destination_e164: None,
        timestamp: Some(timestamp),
        message: Some(body),
        expiration_start_timestamp,
        unidentified_status: succeeded
            .iter()
            .map(
                |service_id| sync_message::sent::UnidentifiedDeliveryStatus {
                    destination_service_id: Some(service_id.service_id_string()),
                    destination_service_id_binary: Some(service_id.service_id_binary()),
                    unidentified: Some(true),
                    destination_pni_identity_key: None,
                },
            )
            .collect(),
        ..sync_message::Sent::default()
    }
}

/// vendored 逐收件人 failover（官方 fallback；body 已带 group_v2，接收方
/// 群线程正确；expire timer 由 vendored 内部 restore）。Deferred 面（修2
/// send-done 语义批，send 文本群面）= split_self 变体：逐收件人腿 ack 即
/// 返回，self 同步转录腿 spawn_local 后台执行；Blocking 面行为零变化。
async fn failover(
    service: &Arc<super::Service>,
    manager: &mut Manager<SqliteStore, Registered>,
    self_sync: &super::GroupSelfSync<'_>,
    master_key: &[u8],
    body: DataMessage,
    timestamp: u64,
) -> Result<GroupSendOutcomes, SendError> {
    // S2a 打点：vendored 逐收件人 failover 路径计数（官方合法 fallback）。
    super::perf_metrics::add(super::perf_metrics::keys::SEND_GROUP_FAILOVER, 1);
    let outcomes = match self_sync {
        super::GroupSelfSync::Blocking => manager
            .send_message_to_group(master_key, ContentBody::DataMessage(body), timestamp)
            .await
            .map_err(super::send_error_of)?,
        super::GroupSelfSync::Deferred(account) => {
            let (outcomes, sync_body) = manager
                .send_message_to_group_split_self(
                    master_key,
                    ContentBody::DataMessage(body),
                    timestamp,
                )
                .await
                .map_err(super::send_error_of)?;
            if let Some(body) = sync_body {
                super::spawn_self_sync_leg(
                    service,
                    account,
                    super::BackgroundSelfLeg::Prepared(body),
                    timestamp,
                );
            }
            outcomes
        }
    };
    // 群腿零收件人防御第二点（2026-10-16 批，compat §5）：vendored 路径
    // 空 outcomes ⟺ 群成员过滤自身后为空（群态取不到回退路径与逐收件人
    // 路径的全部零收件人面）——同转确定性失败，真空成功关闭。vendored
    // 内部已落的本地 transcript 副本为无 wire 影响的本地副作用（vendored
    // 行为不回改），引擎对 connector 的结局从 Ok([]) 翻转为失败面。
    if outcomes.is_empty() {
        return Err(SendError::NoRecipients);
    }
    Ok(outcomes)
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

/// 单请求 MRM 发送（per-account 连接槽：传输失败只摘除本账号连接重连一次，
/// 不影响其他账号；池满且无空闲槽 = fail-closed 走 failover）。
async fn send_mrm(
    service: &Arc<super::Service>,
    account: &str,
    payload: &[u8],
    timestamp: u64,
    token: zkgroup::groups::GroupSendFullToken,
) -> Result<libsignal_net_chat::api::messages::MultiRecipientMessageResponse, MrmSendFailure> {
    let Some(slot) = service.mrm_pool.slot_for(account) else {
        return Err(MrmSendFailure::Fatal);
    };
    for attempt in 0..2 {
        let mut guard = slot.connection.lock().await;
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
                // GSE token 层拒发（403）：单独分型交发送循环走「刷群态
                // 一次→重试」臂；直落 Fatal 会白炸 N 个逐收件人请求且不修因。
                return Err(MrmSendFailure::Unauthorized);
            }
            Err(_) => {
                // 传输层失败：摘除本账号缓存连接重连一次；仍失败 → failover。
                // 只摘本槽——其他账号的连接与在途请求不受影响（R4）。
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
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
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

    /// R4：MRM 连接按账号隔离——同账号复用同一槽，异账号各自独立槽。
    #[test]
    fn mrm_pool_isolates_accounts() {
        let pool = MrmPool::new(MRM_CONNECTION_LIMIT);
        let a = pool.slot_for("+15550000001").expect("slot a");
        let a_again = pool.slot_for("+15550000001").expect("slot a reuse");
        let b = pool.slot_for("+15550000002").expect("slot b");
        assert!(Arc::ptr_eq(&a, &a_again), "同账号复用连接槽");
        assert!(!Arc::ptr_eq(&a, &b), "异账号互不共享");
        assert_eq!(pool.len(), 2);
        let a_generation = a.generation();
        drop((a, a_again, b));
        pool.remove("+15550000001");
        assert_eq!(pool.len(), 1);
        // 摘除后重建：新槽代次（不复活旧连接）。
        let rebuilt = pool.slot_for("+15550000001").expect("rebuilt slot");
        assert_ne!(rebuilt.generation(), a_generation);
    }

    /// R4/R2：池达上限后，使用中的槽不被驱逐；全忙 = fail-closed（None，
    /// 调用方走 failover）。空闲槽可被淘汰让位。
    #[test]
    fn mrm_pool_caps_connections_and_fails_closed_when_all_busy() {
        let pool = MrmPool::new(2);
        let a = pool.slot_for("a").expect("slot a");
        let b = pool.slot_for("b").expect("slot b");
        assert_eq!(pool.len(), 2);

        // 两槽都被调用方持有（模拟在途发送）→ 第三个账号 fail-closed。
        assert!(pool.slot_for("c").is_none());
        assert_eq!(pool.len(), 2);

        // 释放 a 的引用 → a 成为空闲槽，可被 c 淘汰。
        let a_generation = a.generation();
        drop(a);
        let c = pool.slot_for("c").expect("idle slot evicted");
        assert_eq!(pool.len(), 2);
        assert!(c.generation() != a_generation);
        // b 仍在池中（未被误淘汰）。
        assert!(Arc::ptr_eq(&pool.slot_for("b").expect("b survives"), &b));
    }

    /// 群 Sent 转录字段钉面（批次 T，官方 sendToGroup → wrapWithSyncMessageSend
    /// 同构）：destination 双形态恒无、timestamp/message 原样、
    /// expirationStartTimestamp 按 expire_timer 在场、UDS 逐成功收件人
    /// string+binary 双写 + unidentified=true + 无 PNI key。
    #[test]
    fn group_sent_transcript_pins_official_fields() {
        let body = DataMessage {
            timestamp: Some(1_000),
            body: Some("hello".into()),
            expire_timer: Some(60),
            expire_timer_version: Some(1),
            ..DataMessage::default()
        };
        let succeeded = vec![
            ServiceId::from(Aci::from(Uuid::from_u128(0xA1))),
            ServiceId::from(Aci::from(Uuid::from_u128(0xB2))),
        ];
        let sent = build_group_sent_transcript(body.clone(), 1_700_000_000_000, &succeeded);
        assert_eq!(sent.destination_service_id, None);
        assert_eq!(sent.destination_service_id_binary, None);
        assert_eq!(sent.destination_e164, None);
        assert_eq!(sent.timestamp, Some(1_700_000_000_000));
        assert_eq!(sent.message, Some(body));
        assert_eq!(sent.expiration_start_timestamp, Some(1_700_000_000_000));
        assert_eq!(sent.unidentified_status.len(), succeeded.len());
        for (status, id) in sent.unidentified_status.iter().zip(&succeeded) {
            assert_eq!(
                status.destination_service_id.as_deref(),
                Some(id.service_id_string().as_str())
            );
            assert_eq!(
                status.destination_service_id_binary.as_deref(),
                Some(id.service_id_binary().as_slice())
            );
            assert_eq!(status.unidentified, Some(true));
            assert_eq!(status.destination_pni_identity_key, None);
        }
    }

    /// 无 timer 群 = expirationStartTimestamp 缺席；无成功收件人 = UDS 空表。
    #[test]
    fn group_sent_transcript_gates_expiration_start_and_uds() {
        let body = DataMessage {
            timestamp: Some(5),
            ..DataMessage::default()
        };
        let sent = build_group_sent_transcript(body, 42, &[]);
        assert_eq!(sent.expiration_start_timestamp, None);
        assert!(sent.unidentified_status.is_empty());
        assert_eq!(sent.timestamp, Some(42));
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

    /// SKDM 接收落库 + MRM 加解密往返（sender key 互操作批复核钉面）。
    /// 发送侧 create_sender_key_distribution_message（send_mrm SKDM 阶段
    /// 同款调用）→ 接收侧 process_sender_key_distribution_message（vendored
    /// cipher::open_envelope 收包面同款调用，SKDM-only 信封在投影层之前
    /// 消费，projection 层不投影臂为不可达防御臂）→ 接收侧 sender key
    /// store 出现 (sender, distribution_id) 条目（修1 验收口径）→
    /// group_encrypt/group_decrypt 往返成功（能解官方 MRM 群密文）。
    #[tokio::test]
    async fn skdm_receive_stores_key_and_group_decrypt_roundtrip() {
        use presage::libsignal_service::protocol::{
            group_decrypt, group_encrypt, process_sender_key_distribution_message, IdentityKeyPair,
            InMemSignalProtocolStore, ProtocolAddress, SenderKeyStore as _,
        };

        let distribution_id = Uuid::from_u128(0xD15);
        let sender = ProtocolAddress::new(
            Uuid::from_u128(0xA11).to_string(),
            DeviceId::try_from(1).expect("device id"),
        );

        let alice_pair = IdentityKeyPair::generate(&mut rng());
        let mut alice = InMemSignalProtocolStore::new(alice_pair, 1).expect("alice protocol store");
        let bob_pair = IdentityKeyPair::generate(&mut rng());
        let mut bob = InMemSignalProtocolStore::new(bob_pair, 2).expect("bob protocol store");

        let skdm = create_sender_key_distribution_message(
            &sender,
            distribution_id,
            &mut alice,
            &mut rng(),
        )
        .await
        .expect("create SKDM");

        // 处理前：接收侧 store 无该条目。
        assert!(bob
            .load_sender_key(&sender, distribution_id)
            .await
            .expect("load before process")
            .is_none());

        process_sender_key_distribution_message(&sender, &skdm, &mut bob)
            .await
            .expect("process SKDM");

        // 修1 验收口径：处理后条目落库可取回。
        assert!(bob
            .load_sender_key(&sender, distribution_id)
            .await
            .expect("load after process")
            .is_some());

        // 互操作不变量：发送侧 MRM 群加密 → 接收侧可解密。
        let padded = pad_message(b"hello group");
        let ciphertext = group_encrypt(&mut alice, &sender, distribution_id, &padded, &mut rng())
            .await
            .expect("group encrypt");
        let decrypted = group_decrypt(ciphertext.serialized(), &mut bob, &sender)
            .await
            .expect("group decrypt");
        assert_eq!(decrypted, padded);
    }
}
