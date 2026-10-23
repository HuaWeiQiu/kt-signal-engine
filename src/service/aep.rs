// SPDX-License-Identifier: AGPL-3.0-only

//! L1 AEP 获取（设备转移备份协议第一层，台账 §29.10 / §29.10.1）：链接设备
//! （非 primary）缺 AccountEntropyPool（账户主密钥）时的索取与落库，官方
//! ensureAEP 链路行为对齐（Signal-Desktop，零抄码，协议字面量同名同值）。
//!
//! 官方协议面（实测源，file:line 锚）：
//! - 请求臂：afterEveryLinkedStartup → ensureAEP（background.preload.ts:2028/
//!   :2066）——AEP 缺席且非 primary 时按日级节律（accountEntropyPoolLast
//!   RequestTime 书签；isOlderThan(lastSent, DAY) || lastSent > now，时钟
//!   回拨视同到期）经 singleProtoJobQueue 发 getRequestKeySyncMessage
//!   （SendMessage.preload.ts:1809）＝ `SyncMessage.Request{type: KEYS}`
//!   （SignalService.proto Request.Type.KEYS=5，padded sync message，发自有
//!   ACI），发送入队成功才落书签。
//! - 响应臂：#handleSyncMessage 双闸（MessageReceiver.preload.ts:2979——
//!   非 own ACI 整封丢、sourceDevice==own 自设备 echo 整封丢）→ if 链
//!  分发 `content.keys`（:3114，stickerPackOperation 直挂词条先于 keys）
//!   → #handleKeys（:3338，`accountEntropyPool: sync.accountEntropyPool ||
//!   undefined`——缺席与空串同为无 AEP）→ onKeysSync（background.preload
//!   .ts:3707——primary 不处理；AEP 在场则落、缺席则删本地；并以 AEP 派生
//!   masterKey 同步落/删）。keys 信封零渲染零投影。
//! - 引擎存储位：vendored presage `StateStore::fetch/store_account_entropy
//!   _pool`（kv 'account_entropy_pool'，链接期 provisioning 同位——manager/
//!   linking.rs:166）+ `store_master_key`（kv 'master_key'；AEP→derive_svr
//!   _key 派生，官方 deriveMasterKey→put masterKey 同语义，L2 备份面既有
//!   消费方）。mediaRootBackupKey：官方 onKeysSync 另落 backupMediaRootKey
//!   item——presage 无该存储面，L1 不发明存储，记档台账随媒体备份层补。
//!
//! 纪律：零 stderr（tracing only，账号经 probe_account8 脱敏）；AEP/
//! masterKey 是账户级密钥材料，只进 store，零投影外传 connector——闸失败
//! 整封吞；通过后放行既有 keys 投影臂（P14 组 6 mediaRootBackupKey +
//! accountEntropyPoolPresent 布尔形状零变化，AEP 材料键照旧不在）；AEP 形状
//! 坏（parse 失败）fail-closed 不落库（坏 AEP 会毒化 L2 BackupKey 派生）；
//! 请求臂 best-effort，失败静默下轮重补（书签只在发送成功后落，失败下轮
//! 即重试）。

use presage::libsignal_service::content::ContentBody;
use presage::libsignal_service::master_key::MasterKey;
use presage::libsignal_service::protocol::ServiceId;
use presage::manager::{Manager, Registered, RegistrationType};
use presage::store::StateStore as _;
use presage_store_sqlite::SqliteStore;

/// 请求节律书签（kt_kv，per-account store.db，跨重启生效；官方
/// accountEntropyPoolLastRequestTime 同位）。
const KV_LAST_REQUEST_MS: &str = "aep_last_request_ms";

/// 日级重试窗（官方 DAY = 1 天；isOlderThan 为严格大于，恰 24h 边界未到期）。
const REQUEST_WINDOW_MS: u64 = 24 * 60 * 60 * 1000;

/// 请求到期判定（纯函数，官方 ensureAEP 判别同构）：无书签 = 立即请求
/// （首启即索）；书签超窗（严格 > 24h）= 到期；书签在未来（时钟回拨/
/// 穿越，官方 `lastSent > now` 臂）= 到期重发。
fn aep_request_due(last_request_ms: Option<u64>, now_ms: u64) -> bool {
    match last_request_ms {
        None => true,
        Some(last) => now_ms.saturating_sub(last) > REQUEST_WINDOW_MS || last > now_ms,
    }
}

/// ensureAEP 三闸合成（纯函数）：有 AEP / primary / 未到期 任一即免发。
fn aep_request_needed(has_pool: bool, primary: bool, due: bool) -> bool {
    !has_pool && !primary && due
}

/// SyncMessage.Keys.accountEntropyPool 判读（官方 `|| undefined` 同构：
/// 缺席与空串同为无 AEP）。
fn keys_account_entropy_pool(keys: &presage::proto::sync_message::Keys) -> Option<&str> {
    keys.account_entropy_pool
        .as_deref()
        .filter(|raw| !raw.is_empty())
}

/// 请求臂（官方 ensureAEP 同位；挂点 = 收流臂 initial sync 沉降时机，即引擎
/// 的「链接启动」位）：三闸（有 AEP / primary / 未到期）任一即静默返回；
/// 到期发 `SyncMessage.Request{KEYS}` 至自有 ACI（官方 padded sync 同族，
/// 构帧复用 service::build_sync_request 单点），发送成功才落书签（官方
/// enqueue 成功后 put 同序）。
pub(crate) async fn ensure_aep(manager: &mut Manager<SqliteStore, Registered>, account: &str) {
    let has_pool = matches!(
        manager.store().fetch_account_entropy_pool().await,
        Ok(Some(_))
    );
    let primary = manager.registration_type() != RegistrationType::Secondary;
    let now = super::now_ms();
    let last = manager
        .store()
        .kt_kv_get(KV_LAST_REQUEST_MS)
        .await
        .ok()
        .flatten()
        .and_then(|raw| raw.parse::<u64>().ok());
    if !aep_request_needed(has_pool, primary, aep_request_due(last, now)) {
        return;
    }
    let own_aci = ServiceId::Aci(manager.registration_data().service_ids.aci.into());
    let body = ContentBody::SynchronizeMessage(super::build_sync_request(
        presage::proto::sync_message::request::Type::Keys,
    ));
    // 发送成功才落书签：失败下轮收流臂即重试（官方 job 队列入队失败不落
    // lastSent 同语义）。
    if manager.send_message(own_aci, body, now).await.is_ok() {
        let _ = manager
            .store()
            .kt_kv_set(KV_LAST_REQUEST_MS, &now.to_string())
            .await;
    } else {
        tracing::warn!(
            target: "engine.receive",
            stage = "aep_request_send_failed",
            account = %crate::methods::probe_account8(Some(account)),
            "aep key sync request send failed; will retry on next arm"
        );
    }
}

/// 响应臂（官方 #handleSyncMessage 双闸 + onKeysSync 落库同位；挂点 =
/// project_content_inner 的 SynchronizeMessage 臂）：闸序 own-ACI → 自设备
/// echo → primary，任一失败整封吞（官方 throw 丢封同语义）返回 false；
/// 通过后 AEP 落/删 + masterKey 派生落/删并返回 true——调用方随后放行既有
/// keys 投影臂（mediaRootBackupKey/accountEntropyPoolPresent 布尔形状零
/// 变化，AEP 材料照旧不出引擎边界；官方桌面 keys 零渲染，引擎侧信号面
/// 事件系既有消费契约，本批不动）。
pub(crate) async fn on_keys_envelope(
    store: &SqliteStore,
    keys: &presage::proto::sync_message::Keys,
    sender: &ServiceId,
    sender_device: u32,
    account: &str,
) -> bool {
    let registration = match store.load_registration_data().await {
        Ok(Some(registration)) => registration,
        // Err = store 读面坏；Ok(None) = 未注册（理论不可达：收流已注册）。
        // 两态同 fail-closed 整封吞。
        _ => {
            tracing::warn!(
                target: "engine.receive",
                stage = "aep_keys_registration_unreadable",
                account = %crate::methods::probe_account8(Some(account)),
                "keys sync dropped: registration data unreadable"
            );
            return false;
        }
    };
    // 闸 1：sync 只认 own ACI（官方 envelope.sourceServiceId !== ourAci 同款）。
    let own_aci = ServiceId::Aci(registration.service_ids.aci.into());
    if sender != &own_aci {
        tracing::warn!(
            target: "engine.receive",
            stage = "aep_keys_foreign_sender",
            account = %crate::methods::probe_account8(Some(account)),
            "keys sync dropped: sender is not our own aci"
        );
        return false;
    }
    // 闸 2：自设备 echo 不处理（官方 sourceDevice == ourDeviceId 同款）。
    if registration.device_id == Some(sender_device) {
        tracing::warn!(
            target: "engine.receive",
            stage = "aep_keys_self_echo",
            account = %crate::methods::probe_account8(Some(account)),
            "keys sync dropped: echoed from our own device"
        );
        return false;
    }
    // 闸 3：primary 不处理（官方 onKeysSync areWePrimaryDevice 同款；presage
    // 以 device_name 缺席判 primary，与 registration_type 同判据）。
    if registration.device_name.is_none() {
        return false;
    }
    match keys_account_entropy_pool(keys) {
        Some(raw) => match raw.parse::<libsignal_account_keys::AccountEntropyPool>() {
            Ok(aep) => {
                // AEP 落库 + SVR master key 派生落库（链接期 provisioning 同位
                // linking.rs:158-171；官方 onKeysSync deriveMasterKey→put
                // masterKey 同语义）。
                let master = MasterKey::from_slice(aep.derive_svr_key().as_slice()).ok();
                let _ = store.store_account_entropy_pool(Some(&aep)).await;
                let _ = store.store_master_key(master.as_ref()).await;
            }
            Err(_) => {
                // 形状坏 fail-closed：不落库不删旧值（坏 AEP 落库会毒化 L2
                // BackupKey 派生面；官方此处无形状闸，引擎侧宁缺勿错收紧）。
                tracing::warn!(
                    target: "engine.receive",
                    stage = "aep_keys_invalid_shape",
                    account = %crate::methods::probe_account8(Some(account)),
                    "keys sync carried unparseable accountEntropyPool; ignored"
                );
            }
        },
        // 缺席/空串：官方 onKeysSync 删本地 AEP + masterKey 同构（信主设备）。
        None => {
            let _ = store.store_account_entropy_pool(None).await;
            let _ = store.store_master_key(None).await;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use libsignal_account_keys::AccountEntropyPool;
    use presage::libsignal_service::content::{Content, ContentBody, Metadata};
    use presage::libsignal_service::protocol::DeviceId;
    use presage::libsignal_service::utils::{phonenumber_from_signal, TryIntoE164};
    use presage::manager::RegistrationData;
    use serde_json::json;

    use super::*;

    /// 到期判定（官方 isOlderThan(lastSent, DAY) || lastSent > now 同构）：
    /// 无书签立即请求；恰 24h 边界未到期（严格大于）；超窗到期；未来书签
    /// （时钟回拨）到期重发。
    #[test]
    fn request_due_follows_official_day_rhythm() {
        let now: u64 = 1_700_000_000_000;
        // 无书签（首启即索）。
        assert!(aep_request_due(None, now));
        // 1h 前请求过：未到期。
        assert!(!aep_request_due(Some(now - 3_600_000), now));
        // 恰 24h 边界：未到期（官方 isOlderThan 严格大于同款）。
        assert!(!aep_request_due(Some(now - REQUEST_WINDOW_MS), now));
        // 24h + 1ms：到期。
        assert!(aep_request_due(Some(now - REQUEST_WINDOW_MS - 1), now));
        // 书签在未来（时钟回拨/穿越）：官方 lastSent > now 臂，到期重发。
        assert!(aep_request_due(Some(now + 1), now));
    }

    /// 三闸合成：任一闸（有 AEP / primary / 未到期）即免发，全清才发。
    #[test]
    fn request_needed_requires_all_gates_clear() {
        assert!(!aep_request_needed(true, false, true));
        assert!(!aep_request_needed(false, true, true));
        assert!(!aep_request_needed(false, false, false));
        assert!(aep_request_needed(false, false, true));
    }

    /// AEP 判读：缺席与空串同为无 AEP（官方 `|| undefined` 同构）。
    #[test]
    fn keys_reader_treats_empty_string_as_absent() {
        let keys = |raw: Option<&str>| presage::proto::sync_message::Keys {
            account_entropy_pool: raw.map(str::to_string),
            media_root_backup_key: None,
        };
        assert_eq!(keys_account_entropy_pool(&keys(None)), None);
        assert_eq!(keys_account_entropy_pool(&keys(Some(""))), None);
        assert_eq!(
            keys_account_entropy_pool(&keys(Some("dtjs858a"))),
            Some("dtjs858a")
        );
    }

    /// 请求构帧 wire 字面量：`SyncMessage.Request{type: KEYS}`，KEYS=5
    /// （SignalService.proto:614 官方同值，四纪律 #1 字面量钉定）。
    #[test]
    fn request_frame_carries_official_keys_literal() {
        let frame =
            super::super::build_sync_request(presage::proto::sync_message::request::Type::Keys);
        assert_eq!(
            presage::proto::sync_message::request::Type::Keys as i32,
            5,
            "官方 KEYS=5 wire 字面量"
        );
        match frame.content.as_ref() {
            Some(presage::proto::sync_message::Content::Request(request)) => {
                assert_eq!(
                    request.r#type,
                    Some(presage::proto::sync_message::request::Type::Keys as i32)
                );
            }
            other => panic!("keys request frame shape: {other:?}"),
        }
    }

    fn test_store_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "kt-engine-aep-{tag}-{}-{:016x}",
            std::process::id(),
            super::super::mix_seed_step(super::super::mix_seed())
        ))
    }

    fn linked_registration(aci: &str) -> RegistrationData {
        let e164 = "+15551234567".try_into_e164().expect("e164");
        let phone_number = phonenumber_from_signal(&e164);
        serde_json::from_str(
            &json!({
                "signal_servers": "Production",
                "device_name": "KT Signal Engine",
                "phone_number": serde_json::to_value(&phone_number).expect("phone serializes"),
                "uuid": aci,
                "pni": "7b3dc19e-11f2-4c5a-9d0e-2f6a8b4c1d3e",
                "password": "abcdefghijklmnopqrstuvwx",
                "device_id": Some(2u32),
                "registration_id": 1234u32,
                "pni_registration_id": Some(5678u32),
                "profile_key": base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
            })
            .to_string(),
        )
        .expect("registration data parses")
    }

    fn keys_metadata(sender: ServiceId, sender_device: u8) -> Metadata {
        Metadata {
            sender,
            destination: sender,
            sender_device: DeviceId::new(sender_device).expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        }
    }

    fn keys_content(sender: ServiceId, sender_device: u8, aep: Option<String>) -> Content {
        Content::from_body(
            ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                content: Some(presage::proto::sync_message::Content::Keys(
                    presage::proto::sync_message::Keys {
                        account_entropy_pool: aep,
                        media_root_backup_key: None,
                    },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rand::rng())
            }),
            keys_metadata(sender, sender_device),
        )
    }

    const TEST_ACI: &str = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
    const FOREIGN_ACI: &str = "11111111-2222-4333-8444-555555555555";

    /// 投影面恰一条事件断言（service::tests 同名辅助跨模块不可见，本地同构）。
    fn pop_unwrap(notifications: Vec<serde_json::Value>, message: &str) -> serde_json::Value {
        assert_eq!(notifications.len(), 1, "{message}: {notifications:?}");
        notifications.into_iter().next().expect("non-empty")
    }

    /// 响应臂落库：own ACI（非自设备 echo）回传 AEP → store AEP + 派生
    /// masterKey 落库；keys 信封零投影（整封吞）；随后无 AEP 字段的 keys
    /// 信封 → 本地 AEP + masterKey 删除（官方 onKeysSync put/remove 同构）。
    #[tokio::test]
    async fn keys_envelope_stores_then_removes_aep() {
        let store_dir = test_store_dir("store-remove");
        let mut store = super::super::open_store(&store_dir).await.unwrap();
        store
            .save_registration_data(&linked_registration(TEST_ACI))
            .await
            .expect("registration stored");
        let own = ServiceId::parse_from_service_id_string(TEST_ACI).expect("own aci");
        let registry = super::super::groups::TerminatedGroups::new();

        // ① AEP 回传 → 落库 + 派生 masterKey 落库；投影走既有 keys 臂
        // （形状零变化：Present 布尔在场、AEP 材料键永不在）。
        let aep = AccountEntropyPool::generate(&mut rand::rng());
        let content = keys_content(own, 1_u8, Some(aep.to_string()));
        let envelope = pop_unwrap(
            super::super::project_content("acct", &store, &content, &registry, None).await,
            "keys projects after store",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["keys"]["accountEntropyPoolPresent"],
            json!(true),
            "既有投影面形状零变化（Present 布尔）"
        );
        assert!(
            envelope["syncMessage"]["keys"]
                .get("accountEntropyPool")
                .is_none(),
            "AEP 密钥材料零外传 connector（P14 组 6 纪律保持）"
        );
        let stored = store.fetch_account_entropy_pool().await.expect("aep fetch");
        // AccountEntropyPool 无 PartialEq：经官方字符串形（to_string）比对。
        assert_eq!(
            stored.as_ref().map(|pool| pool.to_string()),
            Some(aep.to_string()),
            "AEP 落库原值保真"
        );
        let master = store.fetch_master_key().await.expect("master fetch");
        let derived = MasterKey::from_slice(aep.derive_svr_key().as_slice()).ok();
        assert_eq!(
            master, derived,
            "masterKey = AEP SVR 派生（官方 deriveMasterKey 同语义）"
        );

        // ② 无 AEP 字段的 keys 信封 → 本地 AEP + masterKey 删除（官方
        // onKeysSync 同构）；投影面照走既有臂（空对象信号面事件不变）。
        let content = keys_content(own, 1_u8, None);
        let envelope = pop_unwrap(
            super::super::project_content("acct", &store, &content, &registry, None).await,
            "keys projects after remove",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["keys"],
            json!({}),
            "无 AEP 信封投影空对象（既有信号面契约不变）"
        );
        assert_eq!(
            store
                .fetch_account_entropy_pool()
                .await
                .expect("aep fetch")
                .map(|pool| pool.to_string()),
            None,
            "官方 onKeysSync 删本地 AEP 同构"
        );
        assert_eq!(
            store.fetch_master_key().await.expect("master fetch"),
            None,
            "masterKey 随 AEP 缺席同步删除"
        );
    }

    /// 双闸 fail-closed：外来「own」ACI 伪造 keys 信封与自设备 echo 都整封
    /// 吞、零落库（官方 #handleSyncMessage throw 丢封同语义）。
    #[tokio::test]
    async fn keys_envelope_gates_drop_foreign_and_self_echo() {
        let store_dir = test_store_dir("gates");
        let mut store = super::super::open_store(&store_dir).await.unwrap();
        store
            .save_registration_data(&linked_registration(TEST_ACI))
            .await
            .expect("registration stored");
        let registry = super::super::groups::TerminatedGroups::new();
        let existing = AccountEntropyPool::generate(&mut rand::rng());
        store
            .store_account_entropy_pool(Some(&existing))
            .await
            .expect("preseed aep");

        // 外来 sender 伪 keys 信封（闸 1）。
        let foreign = ServiceId::parse_from_service_id_string(FOREIGN_ACI).expect("foreign aci");
        let content = keys_content(
            foreign,
            1,
            Some("ZmFrZWFlcGZha2VhZXAwMDAwMDAwMDAwMDAwMDAwMDA".into()),
        );
        assert!(
            super::super::project_content("acct", &store, &content, &registry, None)
                .await
                .is_empty(),
            "外来 keys 信封整封吞"
        );
        // 自设备 echo（闸 2，own device_id=2）。
        let own = ServiceId::parse_from_service_id_string(TEST_ACI).expect("own aci");
        let content = keys_content(
            own,
            2,
            Some("ZmFrZWFlcGZha2VhZXAwMDAwMDAwMDAwMDAwMDAwMDA".into()),
        );
        assert!(
            super::super::project_content("acct", &store, &content, &registry, None)
                .await
                .is_empty(),
            "自设备 echo keys 信封整封吞"
        );
        // 本地 AEP 未被两封污染。
        assert_eq!(
            store
                .fetch_account_entropy_pool()
                .await
                .expect("aep fetch")
                .map(|pool| pool.to_string()),
            Some(existing.to_string()),
            "闸失败零落库（fail-closed）"
        );
    }
}
