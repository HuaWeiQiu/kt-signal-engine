// SPDX-License-Identifier: AGPL-3.0-only

//! 云备份（Signal Backups）restore 凭据面——纯函数核（docs/cloud-backup-
//! contract.md §1 凭据链的解析/日窗/形状闸部分；REST/gRPC/CDN 传输面随批）。
//!
//! 官方对位（upstream WebAPI.preload.ts / credentials.preload.ts，AGPL 只读
//! 零拷贝）：
//! - 响应形状 `GET v1/archives/auth` = `{credentials:{messages:[…],media:[…]}}`，
//!   每条 `{credential: b64, redemptionTime: 秒}`（WebAPI:1330-1337）。
//! - 日窗 = `startDayInMs = toDayMillis(now)`、`endDayInMs = toDayMillis(now+6d)`
//!   （credentials.preload.ts:225-232）——UTC 日界（zkgroup redemption_time
//!   `is_day_aligned` 同口径），7 天窗口给服务器签发余量。
//! - 404 = backupId 未设置（官方 credentials.preload.ts:242-263 setBackupId
//!   引导自愈后重试）——非错误形态，调用方据此走引导臂。

use serde_json::Value;

/// 官方凭据端点（WebAPI:798 `getBackupCredentials`）。
pub const BACKUP_CREDENTIALS_PATH: &str = "v1/archives/auth";
/// backupId 引导端点（WebAPI:813 `setBackupId`）。
pub const BACKUP_BACKUP_ID_PATH: &str = "v1/archives/backupid";

/// 单日窗口秒数（redemptionTime 以秒计，is_day_aligned）。
const DAY_SECONDS: u64 = 86_400;

/// 官方 `#doFetch` 日窗（credentials.preload.ts:225-232）：[startDay, endDay]
/// 均为 UTC 日界秒（redemptionTime 口径），跨度 7 天（start..now+6d）。
pub fn backup_credential_day_window(now_ms: u64) -> (u64, u64) {
    let start_day = now_ms / (DAY_SECONDS * 1000) * DAY_SECONDS;
    let end_day = (now_ms + 6 * DAY_SECONDS * 1000) / (DAY_SECONDS * 1000) * DAY_SECONDS;
    (start_day, end_day)
}

/// 单条 messages 档凭据（media 档订阅加购二期，本期不消费）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupCredentialEntry {
    /// 服务器签发的 zkgroup 凭据（base64 标准形）。
    pub credential_b64: String,
    /// 兑换时刻（秒，UTC 日界对齐；zkgroup receive 的 expected_redemption_time）。
    pub redemption_time_seconds: u64,
}

/// 解析 `v1/archives/auth` 响应的 messages 档凭据列表。
///
/// 形状闸（fail-closed）：缺 credentials/messages、messages 非数组、条目缺
/// credential/redemptionTime、b64 非法、时间非正整数——逐条丢弃，全非法 =
/// 空表（诚实缺席，绝不抛）。官方 desktop 同形（zod schema WebAPI:1339-1348）。
pub fn parse_backup_credentials_response(body: &Value) -> Vec<BackupCredentialEntry> {
    let Some(entries) = body
        .get("credentials")
        .and_then(|c| c.get("messages"))
        .and_then(|m| m.as_array())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries {
        let Some(credential) = entry.get("credential").and_then(Value::as_str) else {
            continue;
        };
        let Ok(decoded) = use_base64_decode(credential) else {
            continue;
        };
        if decoded.is_empty() {
            continue;
        }
        let Some(redemption) = entry.get("redemptionTime").and_then(Value::as_u64) else {
            continue;
        };
        if redemption == 0 || redemption % DAY_SECONDS != 0 {
            // 日界未对齐 = 非官方签发形态，丢弃（zkgroup receive 同闸）。
            continue;
        }
        out.push(BackupCredentialEntry {
            credential_b64: credential.to_string(),
            redemption_time_seconds: redemption,
        });
    }
    out
}

/// 官方 404 引导判定：HTTP 404 = backupId 未设置（credentials.preload.ts:242
/// 注释原文语义）——调用方走 `PUT v1/archives/backupid` 引导后重试一次。
pub fn is_backup_id_missing_status(status: u16) -> bool {
    status == 404
}

/// base64 标准形解码（缺字符集校验由解码器承担；与 official Bytes.fromBase64
/// 同标准形口径）。
fn use_base64_decode(value: &str) -> Result<Vec<u8>, ()> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn day_window_is_utc_day_aligned_seven_day_span() {
        // 2026-10-08T15:00:00Z = 1780943_6000…ms（取整日界验证）。
        let now_ms = 1_760_000_000_000u64; // 任意时刻
        let (start, end) = backup_credential_day_window(now_ms);
        assert_eq!(start % DAY_SECONDS, 0);
        assert_eq!(end % DAY_SECONDS, 0);
        assert!(start <= now_ms);
        // 6 天窗：end - start ∈ [6d, 7d)。
        assert!(end - start >= 6 * DAY_SECONDS);
        assert!(end - start < 7 * DAY_SECONDS);
    }

    #[test]
    fn parses_messages_credentials_and_drops_bad_entries() {
        let body = json!({
            "credentials": {
                "messages": [
                    {"credential": "AAEC", "redemptionTime": 86400},
                    {"credential": "not-b64!!", "redemptionTime": 86400},
                    {"credential": "AAEC", "redemptionTime": 0},
                    {"credential": "AAEC"},
                    {"wrong": true},
                ],
                "media": [{"credential": "AAEC", "redemptionTime": 86400}],
            }
        });
        let parsed = parse_backup_credentials_response(&body);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].credential_b64, "AAEC");
        assert_eq!(parsed[0].redemption_time_seconds, 86_400);
    }

    #[test]
    fn missing_sections_are_honest_absence() {
        assert!(parse_backup_credentials_response(&json!({})).is_empty());
        assert!(parse_backup_credentials_response(&json!({"credentials": {}})).is_empty());
        assert!(parse_backup_credentials_response(&Value::Null).is_empty());
    }

    #[test]
    fn media_only_responses_yield_no_messages_credentials() {
        let body =
            json!({"credentials": {"media": [{"credential": "AAEC", "redemptionTime": 86400}]}});
        assert!(parse_backup_credentials_response(&body).is_empty());
    }

    #[test]
    fn backup_id_missing_is_404_only() {
        assert!(is_backup_id_missing_status(404));
        assert!(!is_backup_id_missing_status(401));
        assert!(!is_backup_id_missing_status(500));
    }

    // —— 契约 §4 单测三链 ——

    fn entry(day: u64) -> BackupCredentialEntry {
        BackupCredentialEntry {
            credential_b64: "AAEC".into(),
            redemption_time_seconds: day,
        }
    }

    /// ①zkgroup 兑换 roundtrip：客户端材料 →（模拟服务器签发）→ 包装函数
    /// 兑换成功；日界不符/垃圾 b64/错服务器公钥族三面 fail-closed。
    #[test]
    fn zkgroup_credential_roundtrip_and_fail_closed() {
        use zkgroup::api::generic_server_params::GenericServerSecretParams;
        use zkgroup::backups::{BackupAuthCredentialRequest, BackupCredentialType, BackupLevel};

        // 服务器侧材料（测试内模拟 Signal 服务器）：secret→public 参数对。
        let server_secret = GenericServerSecretParams::generate([0xA0; 32]);
        let server_public = server_secret.get_public_params();
        let other_public = GenericServerSecretParams::generate([0xB0; 32]).get_public_params();

        // 客户端材料（官方 AEP 测试向量 → BackupKey/ACI，确定性派生）。
        let keys = crate::backup::keys::tests_official_keys();
        let day_seconds = 86_400 * 20_400;
        let materials = create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());

        // 服务器视角：解请求 → 签发（messages 档/Free 级）→ b64 回传。
        let request_bytes = use_base64_decode(&materials.request_b64).expect("request b64");
        let request: BackupAuthCredentialRequest =
            zkgroup::deserialize(&request_bytes).expect("request deser");
        let response = request.issue(
            Timestamp::from_epoch_seconds(day_seconds),
            BackupLevel::Free,
            BackupCredentialType::Messages,
            &server_secret,
            [0xA1; 32],
        );
        let response_b64 = use_base64_encode(&zkgroup::serialize(&response));

        // roundtrip：包装函数全路径兑换成功，等级/类型如实。
        let credential = receive_backup_auth_credential(
            materials.request_context,
            &response_b64,
            &server_public,
            day_seconds,
        )
        .expect("roundtrip receive");
        assert_eq!(credential.backup_level(), BackupLevel::Free);
        assert_eq!(credential.credential_type(), BackupCredentialType::Messages);

        // fail-closed①：兑换日不符（expected ≠ response 内嵌日界）。
        let m = create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());
        assert!(receive_backup_auth_credential(
            m.request_context,
            &response_b64,
            &server_public,
            day_seconds + 86_400,
        )
        .is_err());
        // fail-closed②：垃圾 b64。
        let m = create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());
        assert!(receive_backup_auth_credential(
            m.request_context,
            "not-b64!!",
            &server_public,
            day_seconds,
        )
        .is_err());
        // fail-closed③：错服务器公钥族（IssuanceProof 验证不过）。
        let m = create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());
        assert!(receive_backup_auth_credential(
            m.request_context,
            &response_b64,
            &other_public,
            day_seconds,
        )
        .is_err());
    }

    /// ②404→引导→重试状态机：注入 fetch/bootstrap 重演四场景（命中不引导/
    /// 引导自愈/引导后仍缺席诚实上交/引导失败如实上抛）。
    #[tokio::test]
    async fn bootstrap_retry_state_machine() {
        // A：首次即命中——不触发引导。
        let bootstrapped = std::cell::Cell::new(false);
        let (list, missing) = fetch_with_bootstrap_retry(
            || async { Ok((vec![entry(86_400)], false)) },
            || async {
                bootstrapped.set(true);
                Ok(())
            },
        )
        .await
        .expect("first-hit");
        assert_eq!(list.len(), 1);
        assert!(!missing);
        assert!(!bootstrapped.get());

        // B：404→引导→重试命中（fetch 恰好两次）。
        let fetches = std::cell::Cell::new(0u8);
        let (list, missing) = fetch_with_bootstrap_retry(
            || async {
                fetches.set(fetches.get() + 1);
                if fetches.get() == 1 {
                    Ok((Vec::new(), true))
                } else {
                    Ok((vec![entry(86_400)], false))
                }
            },
            || async { Ok(()) },
        )
        .await
        .expect("bootstrap-healed");
        assert_eq!(fetches.get(), 2);
        assert_eq!(list.len(), 1);
        assert!(!missing);

        // C：引导后仍 404 = 诚实缺席（missing=true → 调用方映射
        // NotProvisioned，fetch_today_credentials 同口径）。
        let (_, missing) =
            fetch_with_bootstrap_retry(|| async { Ok((Vec::new(), true)) }, || async { Ok(()) })
                .await
                .expect("still-missing is ok-value");
        assert!(missing);

        // D：引导失败如实上抛（不吞不伪装缺席）。
        let result = fetch_with_bootstrap_retry(
            || async { Ok((Vec::new(), true)) },
            || async { Err(CloudRestoreError::Transport("bootstrap".into())) },
        )
        .await;
        assert!(matches!(result, Err(CloudRestoreError::Transport(_))));
    }

    /// ③失败形态结构化错误面：七族 → 稳定码表（JSON-RPC 同步面与
    /// cloudStatus failed 相位共用），detail 仅带消息族有。
    #[test]
    fn error_families_map_to_stable_rpc_codes() {
        assert_eq!(
            CloudRestoreError::NotProvisioned.rpc_code(),
            "BACKUP_NOT_PROVISIONED"
        );
        assert_eq!(
            CloudRestoreError::AuthRejected.rpc_code(),
            "BACKUP_AUTH_FAILED"
        );
        assert_eq!(
            CloudRestoreError::AlreadyRunning.rpc_code(),
            "BACKUP_RUNNING"
        );
        assert_eq!(
            CloudRestoreError::Transport("x".into()).rpc_code(),
            "BACKUP_TRANSPORT_FAILED"
        );
        assert_eq!(
            CloudRestoreError::Download("x".into()).rpc_code(),
            "BACKUP_DOWNLOAD_FAILED"
        );
        assert_eq!(
            CloudRestoreError::Decrypt("x".into()).rpc_code(),
            "BACKUP_DECRYPT_FAILED"
        );
        assert_eq!(
            CloudRestoreError::Export("x".into()).rpc_code(),
            "BACKUP_EXPORT_FAILED"
        );
        assert_eq!(
            CloudRestoreError::Download("cdn 502".into()).detail(),
            Some("cdn 502")
        );
        assert_eq!(CloudRestoreError::NotProvisioned.detail(), None);
        assert_eq!(CloudRestoreError::AlreadyRunning.detail(), None);
    }

    /// 进度面生命周期：begin/add/fail/clear + 在途前失败立 failed 相位；
    /// 运行闸互斥（守卫单抢到，Drop 释放后可再抢）+ 会话归属（取消面判定）。
    #[test]
    fn progress_lifecycle_and_running_gate() {
        let progress = CloudRestoreProgress::new();
        assert!(progress.snapshot().is_none());
        assert!(!progress.owned_by("lk-a"), "未开会话无归属");
        progress.start_session("lk-a");
        assert!(progress.owned_by("lk-a"));
        assert!(!progress.owned_by("lk-b"), "归属单槽，不串账号");
        progress.begin(Some(100));
        progress.add(40);
        let s = progress.snapshot().expect("in-flight");
        assert_eq!(s.phase, "downloading");
        assert_eq!(s.bytes_read, 40);
        assert_eq!(s.bytes_total, Some(100));
        assert!(s.error_code.is_none());
        // 失败终态：已计字节保留 + 相位翻面 + code 透出。
        progress.fail("BACKUP_DOWNLOAD_FAILED");
        let s = progress.snapshot().expect("failed terminal");
        assert_eq!(s.phase, "failed");
        assert_eq!(s.bytes_read, 40);
        assert_eq!(s.error_code, Some("BACKUP_DOWNLOAD_FAILED"));
        // 成功终态：清缺（transfer_archive 同纪律）；归属一并清。
        progress.clear();
        assert!(progress.snapshot().is_none());
        assert!(!progress.owned_by("lk-a"), "clear 连归属一起清");
        // 在途前失败（凭据段）：无快照也立 failed。
        progress.fail("BACKUP_AUTH_FAILED");
        let s = progress.snapshot().expect("early failure");
        assert_eq!(s.phase, "failed");
        assert_eq!(s.bytes_read, 0);
        progress.clear();
        assert!(progress.snapshot().is_none());

        // 运行闸互斥（守卫持闸，Drop 释放）。
        {
            let guard = CloudRestoreProgress::try_begin_running();
            assert!(guard.is_some(), "首抢命中");
            assert!(
                CloudRestoreProgress::try_begin_running().is_none(),
                "在闸期间重入如实拒"
            );
            // 守卫 Drop 释放（含 abort 路径——闭包收尾不执行的场景）。
        }
        assert!(
            CloudRestoreProgress::try_begin_running().is_some(),
            "Drop 后闸已释放"
        );
        // 显式释放同路（测试直用关联函数钉释放语义）。
        CloudRestoreProgress::finish_running();
    }

    /// 取消侧半成品清理：密文工作目录与半途 NDJSON 双删，其余文件不动，
    /// 幂等不因缺席炸。
    #[test]
    fn restore_artifact_cleanup_removes_workdir_and_ndjson() {
        let dir =
            std::env::temp_dir().join(format!("kt-engine-restore-cancel-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("cloud-backup")).expect("workdir");
        std::fs::write(dir.join("cloud-backup/main"), b"ciphertext").expect("main");
        std::fs::write(dir.join("history-import-cloud.ndjson"), b"{\"partial\"").expect("ndjson");
        std::fs::write(dir.join("keep-me"), b"untouched").expect("other file");
        remove_restore_artifacts(&dir);
        assert!(!dir.join("cloud-backup").exists());
        assert!(!dir.join("history-import-cloud.ndjson").exists());
        assert!(
            dir.join("keep-me").exists(),
            "只清 restore 产物，不动其他文件"
        );
        remove_restore_artifacts(&dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// —— zkgroup 兑换/呈现（契约 §1.3；官方 credentials.preload.ts:316 与
//    zkgroup auth_credential.rs 对位，批 2）——

use libsignal_account_keys::BackupKey;
use presage::libsignal_service::protocol::Aci;
use zkgroup::api::generic_server_params::GenericServerPublicParams;
use zkgroup::backups::{BackupAuthCredentialRequestContext, BackupAuthCredentialResponse};
use zkgroup::Timestamp;

/// 生产 GenericServerPublicParams 常量面已提升为共享模块（P4c 切一：
/// call-link 凭据链同源消费，不重复嵌）——此处重导出保持既有引用不漂。
/// **行为修正（随批记档 compat §5）**：本模块前批嵌的是轮换后 V1 值
/// （首字节 0x01，v0.99.0 `VersionByte<0>` 严格校验下反序列化必失败 =
/// BackupAuth 兑换死链），共享模块改钉轮换前 V0 生产值（legacy 族，与
/// v0.99.0 请求族匹配）；真机回归按 V0 执行（cloud-backup-contract §6.5
/// 后续批）。
pub use super::zkgroup_consts::generic_server_public_params;

/// 引导材料：请求上下文 + 官方 bootstrap PUT 体（b64(request)，WebAPI:813
/// messagesBackupAuthCredentialRequest 同键）。
pub struct BackupAuthRequestMaterials<const VERSION: u8> {
    pub request_b64: String,
    pub request_context: BackupAuthCredentialRequestContext,
}

/// AEP 派生 BackupKey + 本机 ACI → 凭据请求上下文（官方
/// createBackupAuthCredentialRequestContext 同构）。
pub fn create_backup_auth_request_materials<const VERSION: u8>(
    backup_key: &BackupKey<VERSION>,
    aci: Aci,
) -> BackupAuthRequestMaterials<VERSION> {
    let request_context = BackupAuthCredentialRequestContext::new(backup_key, aci);
    let request_b64 = use_base64_encode(&zkgroup::serialize(&request_context.get_request()));
    BackupAuthRequestMaterials {
        request_b64,
        request_context,
    }
}

/// 服务器凭据（b64）+ 兑换日（秒）→ 兑换后的凭据（zkgroup receive：日界
/// 未对齐/签名验证失败 = ZkGroupVerificationFailure，收敛形状闸 fail-closed）。
pub fn receive_backup_auth_credential(
    request_context: BackupAuthCredentialRequestContext,
    credential_b64: &str,
    params: &GenericServerPublicParams,
    redemption_time_seconds: u64,
) -> Result<zkgroup::backups::BackupAuthCredential, ()> {
    let response_bytes = use_base64_decode(credential_b64)?;
    let response: BackupAuthCredentialResponse =
        zkgroup::deserialize(&response_bytes).map_err(|_| ())?;
    request_context
        .receive(
            response,
            params,
            Timestamp::from_epoch_seconds(redemption_time_seconds),
        )
        .map_err(|_| ())
}

/// 凭据呈现+签名走 `BackupAuth::new` 内部（官方 net/chat api 同构，gRPC 面
/// get_message_backup_info/get_backup_cdn_credentials 各自呈现），不另设
/// 手工 present 包装——单一路径，不漂。
fn use_base64_encode(value: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(value)
}

// —— 第三片：chat gRPC 传输面 + CDN 下载 + 解密导入（契约 §0.2/§1.4/§1.5/§1.6）——

use std::time::Duration;

use libsignal_net::chat::ws;
use libsignal_net::chat::{ChatConnection, ConnectError};
use libsignal_net::connect_state::{
    ConnectState, DefaultConnectorFactory, PreconnectingFactory, SUGGESTED_CONNECT_CONFIG,
};
use libsignal_net::env::constants::CHAT_WEBSOCKET_PATH;
use libsignal_net::env::{StaticIpOrder, UserAgent};
use libsignal_net::infra::dns::DnsResolver;
use libsignal_net::infra::route::{DirectOrProxyMode, DirectOrProxyProvider};
use libsignal_net::infra::utils::no_network_change_events;
use libsignal_net::infra::{EnableDomainFronting, OverrideNagleAlgorithm};

/// 生产 chat WS 连接（官方 simple_chat_connection 逐行复刻，chat.rs:510-573：
/// env::PROD 直连、无域前端、无代理、headers=None=未鉴权）。
///
/// # Errors
/// 连接失败（DNS/TLS/WS 握手）原样映射，调用方按 CONNECT 族呈现。
pub async fn connect_production_chat() -> Result<ChatConnection, ConnectError> {
    let env = &libsignal_net::env::PROD;
    let dns_resolver = DnsResolver::new_with_static_fallback(
        env.static_fallback(StaticIpOrder::HARDCODED),
        &no_network_change_events(),
    );
    let route_provider = DirectOrProxyProvider {
        inner: env.chat_domain_config.connect.route_provider(
            EnableDomainFronting::No,
            OverrideNagleAlgorithm::UseSystemDefault,
        ),
        mode: DirectOrProxyMode::DirectOnly,
    };
    let connect = ConnectState::new_with_transport_connector(
        SUGGESTED_CONNECT_CONFIG,
        PreconnectingFactory::new(DefaultConnectorFactory, Duration::ZERO),
    );
    let user_agent = UserAgent::with_libsignal_version(concat!(
        env!("CARGO_PKG_NAME"),
        " ",
        env!("CARGO_PKG_VERSION")
    ));
    let ws_config = ws::Config {
        initial_request_id: 0,
        local_idle_timeout: Duration::from_secs(60),
        post_request_interface_check_timeout: Duration::MAX,
        remote_idle_timeout: Duration::from_secs(60),
    };
    let connection_resources = libsignal_net::connect_state::ConnectionResources {
        connect_state: &connect,
        dns_resolver: &dns_resolver,
        network_change_event: &no_network_change_events(),
        confirmation_header_name: env
            .chat_domain_config
            .connect
            .confirmation_header_name
            .map(http::HeaderName::from_static),
    };
    let pending = ChatConnection::start_connect_with(
        connection_resources,
        env.chat_domain_config.connect.service,
        route_provider,
        CHAT_WEBSOCKET_PATH,
        &user_agent,
        ws_config,
        None,
        "cloud-backup",
    )
    .await?;
    let listener: ws::EventListener = Box::new(|_event| {});
    let tokio_runtime = tokio::runtime::Handle::try_current()
        .expect("cloud restore runs on the engine LocalSet tokio runtime");
    Ok(ChatConnection::finish_connect(
        tokio_runtime,
        pending,
        Default::default(),
        listener,
    ))
}

// —— 3B：云备份 restore 全流程编排（契约 §1.4-§1.6；官方 credentials.preload.ts
//    + calling 前例 download_transfer_archive 传输形态同构）——
//
// I/O 面参数化注入（PushService 沿用 service 层装配）；帧导出复用
// backup/receive.rs export_snapshot_frames（密文落 work_dir/main + AEP 派生
// message_key + 尾部 HMAC）。future 非 Send——调用方 LocalSet 承载
// （link_flow/download_transfer_archive 同策略）。

use std::path::Path;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::protocol::PrivateKey;
use presage::libsignal_service::push_service::HttpAuthOverride;

use libsignal_net_chat::api::backups::BackupAuth;
use libsignal_net_chat::api::Unauth;

use crate::backup::keys::BackupKeys;

/// restore 流程错误族（methods 层映射 BACKUP_* 结构化码）。
#[derive(Debug)]
pub enum CloudRestoreError {
    /// 引导自愈后仍无 backupId（账号从未开启云备份）——retryable=false。
    NotProvisioned,
    /// 凭据兑换被拒（zkgroup 验证失败）——retryable=false。
    AuthRejected,
    /// 重入（同引擎已有在途 restore）——retryable=false。
    AlreadyRunning,
    /// chat/REST 传输失败——retryable=true。
    Transport(String),
    /// CDN 下载失败——retryable=true。
    Download(String),
    /// 解密/HMAC 校验失败——retryable=false。
    Decrypt(String),
    /// 帧导出失败——retryable=false。
    Export(String),
}

impl Clone for CloudRestoreError {
    fn clone(&self) -> Self {
        match self {
            Self::NotProvisioned => Self::NotProvisioned,
            Self::AuthRejected => Self::AuthRejected,
            Self::AlreadyRunning => Self::AlreadyRunning,
            Self::Transport(m) => Self::Transport(m.clone()),
            Self::Download(m) => Self::Download(m.clone()),
            Self::Decrypt(m) => Self::Decrypt(m.clone()),
            Self::Export(m) => Self::Export(m.clone()),
        }
    }
}

impl CloudRestoreError {
    /// 结构化错误码（cloud-backup-contract §2）：JSON-RPC 错误面与
    /// cloudStatus failed 相位共用同一映射，两端不漂。
    pub(crate) fn rpc_code(&self) -> &'static str {
        match self {
            Self::NotProvisioned => "BACKUP_NOT_PROVISIONED",
            Self::AuthRejected => "BACKUP_AUTH_FAILED",
            Self::AlreadyRunning => "BACKUP_RUNNING",
            Self::Transport(_) => "BACKUP_TRANSPORT_FAILED",
            Self::Download(_) => "BACKUP_DOWNLOAD_FAILED",
            Self::Decrypt(_) => "BACKUP_DECRYPT_FAILED",
            Self::Export(_) => "BACKUP_EXPORT_FAILED",
        }
    }

    /// 错误细节（仅带消息的族有；观测面可选 detail 字段）。
    pub(crate) fn detail(&self) -> Option<&str> {
        match self {
            Self::NotProvisioned | Self::AuthRejected | Self::AlreadyRunning => None,
            Self::Transport(m) | Self::Download(m) | Self::Decrypt(m) | Self::Export(m) => Some(m),
        }
    }
}

async fn fetch_credentials_once(
    push: &PushService,
    now_ms: u64,
) -> Result<(Vec<BackupCredentialEntry>, bool), CloudRestoreError> {
    let (start_day, end_day) = backup_credential_day_window(now_ms);
    let path = format!(
        "{}?redemptionStartSeconds={}&redemptionEndSeconds={}",
        super::cloud_backup::BACKUP_CREDENTIALS_PATH,
        start_day / 1000,
        end_day / 1000
    );
    let response = push
        .request(
            http::Method::GET,
            Endpoint::service(&path),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| CloudRestoreError::Transport("credentials build".into()))?
        .send()
        .await
        .map_err(|e| CloudRestoreError::Transport(format!("credentials: {e}")))?;
    let status = response.status().as_u16();
    if is_backup_id_missing_status(status) {
        return Ok((Vec::new(), true));
    }
    if !(200..300).contains(&status) {
        return Err(CloudRestoreError::Transport(format!(
            "credentials status {status}"
        )));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| CloudRestoreError::Transport(format!("credentials json: {e}")))?;
    Ok((parse_backup_credentials_response(&body), false))
}

async fn bootstrap_backup_id(
    push: &PushService,
    request_b64: &str,
) -> Result<(), CloudRestoreError> {
    let body = serde_json::json!({
        "messagesBackupAuthCredentialRequest": request_b64,
        "mediaBackupAuthCredentialRequest": request_b64,
    });
    let response = push
        .request(
            http::Method::PUT,
            Endpoint::service(super::cloud_backup::BACKUP_BACKUP_ID_PATH),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| CloudRestoreError::Transport("backupid build".into()))?
        .json(&body)
        .send()
        .await
        .map_err(|e| CloudRestoreError::Transport(format!("backupid: {e}")))?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(CloudRestoreError::Transport(format!(
            "backupid status {}",
            response.status().as_u16()
        )));
    }
    Ok(())
}

/// 当日凭据拉取（404→引导→重试一次，官方 credentials.preload.ts:242-258 同序）。
///
/// # Errors
/// 引导/传输失败 = `Transport`；引导后仍空 = `NotProvisioned`。
/// 404→引导→重试状态机（官方 credentials.preload.ts:242-263 setBackupId 自愈
/// 语义）：fetch 报 backupId 缺席 → bootstrap PUT → fetch 重试一次；仍缺席 =
/// 诚实 missing=true（调用方映射 NotProvisioned）。fetch/bootstrap 注入形，
/// 单测脱离 PushService 重演全状态机。
async fn fetch_with_bootstrap_retry<F, B, FutF, FutB>(
    fetch: F,
    bootstrap: B,
) -> Result<(Vec<BackupCredentialEntry>, bool), CloudRestoreError>
where
    F: Fn() -> FutF,
    B: Fn() -> FutB,
    FutF:
        std::future::Future<Output = Result<(Vec<BackupCredentialEntry>, bool), CloudRestoreError>>,
    FutB: std::future::Future<Output = Result<(), CloudRestoreError>>,
{
    let (list, was_missing) = fetch().await?;
    if !was_missing {
        return Ok((list, false));
    }
    bootstrap().await?;
    fetch().await
}

pub async fn fetch_today_credentials(
    push: &PushService,
    backup_request_b64: &str,
    now_ms: u64,
) -> Result<BackupCredentialEntry, CloudRestoreError> {
    let (mut entries, missing) = fetch_with_bootstrap_retry(
        || fetch_credentials_once(push, now_ms),
        || bootstrap_backup_id(push, backup_request_b64),
    )
    .await?;
    if missing {
        return Err(CloudRestoreError::NotProvisioned);
    }
    let today_seconds = now_ms / 1000 / 86_400 * 86_400;
    entries.sort_by_key(|entry| entry.redemption_time_seconds);
    entries
        .into_iter()
        .find(|entry| entry.redemption_time_seconds == today_seconds)
        .ok_or(CloudRestoreError::NotProvisioned)
}

/// 云备份 restore 全编排：凭据拉取（404 引导自愈）→ zkgroup 兑换 → BackupAuth
/// → 备份定位（未鉴权 gRPC）→ CDN 读凭据头 → CDN 下载 → 密文落 work_dir/main
/// → export_snapshot_frames（AEP 派生 message_key 解密 + 尾部 HMAC）→ NDJSON。
///
/// # Errors
/// 各段错误如实映射（凭据空/兑换拒/传输/下载/解密导出）。
///
/// # Panics
/// 无 tokio runtime 时 `Handle::try_current` panic——引擎 LocalSet 装配保证
/// 存在（connect_production_chat 同前提）。
pub async fn restore_from_cloud(
    push: &PushService,
    store_dir: &Path,
    keys: &BackupKeys,
    signing_key: &PrivateKey,
    rng: &mut (dyn rand::CryptoRng + Send),
    progress: &CloudRestoreProgress,
    out: &mut dyn std::io::Write,
) -> Result<crate::history::ExportStats, CloudRestoreError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let materials = create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());
    let entry = fetch_today_credentials(push, &materials.request_b64, now_ms).await?;

    let params = generic_server_public_params()
        .map_err(|_| CloudRestoreError::Decrypt("generic params".into()))?;
    let credential = receive_backup_auth_credential(
        materials.request_context,
        &entry.credential_b64,
        &params,
        entry.redemption_time_seconds,
    )
    .map_err(|_| CloudRestoreError::AuthRejected)?;
    let backup_auth = BackupAuth::new(&credential, &params, signing_key);

    // gRPC：未鉴权连接 + BackupAuth 呈现（官方 scaffold Unauth(shared_h2) 同构）。
    let chat = connect_production_chat()
        .await
        .map_err(|e| CloudRestoreError::Transport(format!("chat connect: {e}")))?;
    let grpc = Unauth(
        chat.shared_h2_connection()
            .expect("H2 connection available"),
    );
    let info = grpc
        .get_message_backup_info(&backup_auth, rng)
        .await
        .map_err(|_| CloudRestoreError::Transport("backup info".into()))?;
    let cdn_creds = grpc
        .get_backup_cdn_credentials(&backup_auth, info.cdn, rng)
        .await
        .map_err(|_| CloudRestoreError::Transport("cdn credentials".into()))?;

    // CDN 下载（presage cdn 主机映射 + 凭据头直附）。
    let cdn_path = format!("backups/{}/{}", info.backup_dir, info.backup_name);
    let mut request = push
        .request(
            http::Method::GET,
            Endpoint::cdn(info.cdn, &cdn_path),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| CloudRestoreError::Download("cdn build".into()))?;
    for (name, value) in cdn_creds.headers.iter() {
        request = request.header(name.as_str(), value.as_bytes());
    }
    let response = request
        .send()
        .await
        .map_err(|_| CloudRestoreError::Download("cdn send".into()))?;
    if !response.status().is_success() {
        return Err(CloudRestoreError::Download(format!(
            "cdn status {}",
            response.status().as_u16()
        )));
    }
    // 流式下载 + 进度快照（bytes_total 缺席 = 官方 WaitForBackup 同款不确定
    // 形态，消费方不给假 0；transfer_archive 下载段同纪律）。
    let bytes_total = response.content_length().filter(|total| *total > 0);
    progress.begin(bytes_total);
    let mut ciphertext = Vec::new();
    {
        use futures::StreamExt;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    progress.add(chunk.len() as u64);
                    ciphertext.extend_from_slice(&chunk);
                }
                Err(e) => {
                    progress.clear();
                    return Err(CloudRestoreError::Download(format!("cdn read: {e}")));
                }
            }
        }
    }

    // 密文落 work_dir/main（export_snapshot_frames 契约面），解密导出 NDJSON。
    let work_dir = store_dir.join("cloud-backup");
    std::fs::create_dir_all(&work_dir)
        .map_err(|e| CloudRestoreError::Export(format!("workdir: {e}")))?;
    let main_path = work_dir.join("main");
    std::fs::write(&main_path, ciphertext.as_slice())
        .map_err(|e| CloudRestoreError::Export(format!("write: {e}")))?;
    crate::backup::receive::export_snapshot_frames(&work_dir, keys, out)
        .await
        .map_err(|e| CloudRestoreError::Export(format!("{e:?}")))
}

// —— 第四片：进度注册表 + methods 挂载包装（cloud-backup-contract §2）——

/// 进度快照（`backups.cloudStatus` 只读数据源；bytes_total 缺席 = 官方
/// WaitForBackup 同款不确定形态，消费方不给假 0）。phase：downloading =
/// 在途（字节计数有效）；failed = 终态失败（error_code 结构化透出，快照
/// 保留至下次 start 覆盖，不装死）。成功终态 = 快照清缺（transfer_archive
/// 「无论成败收尾清快照」同纪律——成功产物走 NDJSON 消费链，不在观测面
/// 留痕）。
#[derive(Debug, Clone, Copy)]
pub struct CloudRestoreSnapshot {
    pub phase: &'static str,
    pub bytes_read: u64,
    pub bytes_total: Option<u64>,
    pub error_code: Option<&'static str>,
}

/// 进度注册表（Clone 供 Service 与 spawn 任务双持；进程内存不落盘）。
/// owner = 当前/最近一次 restore 会话的账号（不投影 wire，只供按账号取消
/// 与终态清除判定——快照面进程级单槽，cloudStatus 的 account 参数不参与
/// 路由，但取消面必须区分「这是谁的 restore」）。
#[derive(Clone, Default)]
pub struct CloudRestoreProgress {
    inner: std::sync::Arc<std::sync::Mutex<RestoreSession>>,
}

#[derive(Default)]
struct RestoreSession {
    snapshot: Option<CloudRestoreSnapshot>,
    owner: Option<String>,
}

static CLOUD_RESTORE_RUNNING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// 运行闸守卫（Drop 释放）：`history.importCancel` 经 abort 中止 restore
/// 任务时闭包收尾代码不再执行——闸的释放挂守卫 Drop，取消路径不泄漏
/// BACKUP_RUNNING（否则取消后的 restore 重入被永久误拒）。
pub(crate) struct RunningGuard;

impl Drop for RunningGuard {
    fn drop(&mut self) {
        CloudRestoreProgress::finish_running();
    }
}

impl CloudRestoreProgress {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 重入闸抢占（`backups.restoreFromCloud` start 前置检查全过后调用）：
    /// 抢到 = 持闸（守卫 Drop 释放）；已 true = 并发/在途，如实拒。
    pub(crate) fn try_begin_running() -> Option<RunningGuard> {
        CLOUD_RESTORE_RUNNING
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
            .then_some(RunningGuard)
    }

    /// 重入闸释放（守卫 Drop 单点；保留关联函数供测试直用）。
    pub(crate) fn finish_running() {
        CLOUD_RESTORE_RUNNING.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// 登记当前会话归属账号（start 闭包首行；取消/终态清除按账号判定）。
    pub(crate) fn start_session(&self, owner: &str) {
        self.inner.lock().expect("cloud progress lock").owner = Some(owner.to_owned());
    }

    /// 当前/最近一次会话是否属于该账号（取消面判定：终态快照清不清）。
    pub(crate) fn owned_by(&self, account: &str) -> bool {
        self.inner
            .lock()
            .expect("cloud progress lock")
            .owner
            .as_deref()
            == Some(account)
    }

    pub(crate) fn begin(&self, bytes_total: Option<u64>) {
        self.inner.lock().expect("cloud progress lock").snapshot = Some(CloudRestoreSnapshot {
            phase: "downloading",
            bytes_read: 0,
            bytes_total,
            error_code: None,
        });
    }

    pub(crate) fn add(&self, bytes: u64) {
        let mut session = self.inner.lock().expect("cloud progress lock");
        if let Some(snapshot) = session.snapshot.as_mut() {
            snapshot.bytes_read += bytes;
        }
    }

    /// 终态失败：保留已计字节（部分下载如实），翻相位 + 结构化 code。
    pub(crate) fn fail(&self, code: &'static str) {
        let mut session = self.inner.lock().expect("cloud progress lock");
        let snapshot = session.snapshot.get_or_insert(CloudRestoreSnapshot {
            phase: "failed",
            bytes_read: 0,
            bytes_total: None,
            error_code: None,
        });
        snapshot.phase = "failed";
        snapshot.error_code = Some(code);
    }

    pub(crate) fn snapshot(&self) -> Option<CloudRestoreSnapshot> {
        self.inner.lock().expect("cloud progress lock").snapshot
    }

    /// 会话收尾清快照（成功终态不留痕；取消/重登清除同路）——归属一并
    /// 清除，旧账号名不残留到下一次会话。
    pub(crate) fn clear(&self) {
        *self.inner.lock().expect("cloud progress lock") = RestoreSession::default();
    }
}

/// 取消侧半成品清理（云备份 restore 腿）：密文暂存工作目录（main）与
/// 半途 NDJSON 产物。best-effort，不产生 stderr（compat §1）；账号目录
/// 不存在（fake/已删除）同样安静。
pub(crate) fn remove_restore_artifacts(account_dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(account_dir.join("cloud-backup"));
    let _ = std::fs::remove_file(account_dir.join("history-import-cloud.ndjson"));
}

/// methods 挂载包装（mod.rs start 调用同签名）：AEP/ACI 从 store 取（
/// snapshot_import 先例）→ 凭据拉取（404 引导自愈）→ 兑换呈现 → 全编排 →
/// NDJSON 落 `history-import-cloud.ndjson`（connector 消费链同位）。
///
/// # Errors
/// 全链错误如实映射（NotProvisioned/AuthRejected/Transport/Download/Decrypt/
/// Export）。
#[allow(clippy::too_many_lines)]
pub async fn run_cloud_restore(
    push: &PushService,
    account_dir: std::path::PathBuf,
    store: presage_store_sqlite::SqliteStore,
    signing_key: presage::libsignal_service::protocol::PrivateKey,
    rng: &mut (dyn rand::CryptoRng + Send),
    progress: &CloudRestoreProgress,
) -> Result<crate::history::ExportStats, CloudRestoreError> {
    use presage::store::StateStore as _;
    // ① AEP/ACI（presage store kv + registration data，snapshot_import 先例）。
    let account_entropy_pool = store
        .fetch_account_entropy_pool()
        .await
        .map_err(|_| CloudRestoreError::AuthRejected)?
        .ok_or(CloudRestoreError::NotProvisioned)?;
    let registration = store
        .load_registration_data()
        .await
        .map_err(|_| CloudRestoreError::AuthRejected)?
        .ok_or(CloudRestoreError::NotProvisioned)?;
    let aci = presage::libsignal_service::protocol::Aci::from(registration.service_ids.aci);
    let keys = crate::backup::keys::derive_keys(&account_entropy_pool, &aci);

    // ② 全编排（restore_from_cloud 内含凭据拉取/404 引导自愈/zkgroup 兑现/
    //    BackupAuth/gRPC 定位/CDN 下载；进度经 progress 面透出）。
    let mut ndjson = Vec::new();
    let stats = restore_from_cloud(
        push,
        &account_dir,
        &keys,
        &signing_key,
        rng,
        progress,
        &mut ndjson,
    )
    .await?;

    // ③ NDJSON 落盘（connector 消费链同位：transfer_archive 的
    //    history-import.ndjson 消费面，云备份独立文件名免混淆）。
    let ndjson_path = account_dir.join("history-import-cloud.ndjson");
    std::fs::write(&ndjson_path, &ndjson)
        .map_err(|e| CloudRestoreError::Export(format!("write: {e}")))?;
    Ok(stats)
}
