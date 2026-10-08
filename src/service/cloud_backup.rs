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
}

// —— zkgroup 兑换/呈现（契约 §1.3；官方 credentials.preload.ts:316 与
//    zkgroup auth_credential.rs 对位，批 2）——

use libsignal_account_keys::BackupKey;
use presage::libsignal_service::protocol::Aci;
use rand::Rng as _;
use zkgroup::api::generic_server_params::GenericServerPublicParams;
use zkgroup::backups::{BackupAuthCredentialRequestContext, BackupAuthCredentialResponse};
use zkgroup::Timestamp;

/// 生产环境 GenericServerPublicParams（官方 config/production.json
/// `genericServerPublicParams` 逐字节；zkgroup 公钥材料可入库，libsignal
/// 桥同源）。default.json 为 staging 值，KT 钉生产。
pub const GENERIC_SERVER_PUBLIC_PARAMS_B64: &str = "AeCO67P9mIv1yUHkdeZ9JF789GDbox61GvTqq3S4kYc1ADUWxWHQygU390tv1oRWt9WjkdZlU7mKkifF59ftjE+2ZlMmxns6I+ySiLpR8FEmfu+TGpVp3zYTjNV93obJJTyBCCsSHVETCyQRbKdCyb5TMa6LGrvcZaX0Q/VAavhuNA/m4kSiRMgSnYrUjGhVekdDnF+7xioo4wvFnxjIDh7uJQrYOWD6MloNGX7St5gbysTuQQ7i/HI38b9V8x8mKazuDSXxB//BKGZx/XHkK8cHX+QK1MPxYUVM1/CBI5oW";

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

/// 凭据 → 呈现字节（序列化后喂 gRPC 面与 SFU HTTP；rand 32B 由调用方注入）。
/// 凭据 → 呈现字节 + 签名（gRPC 头两件套；官方 net/chat api BackupAuth::present
/// 同构：zkgroup present 无签名，ACI 私钥 calculate_signature 签呈现字节）。
/// rand/CryptoRng 与 presage 协议族同版本（rand re-export）。
pub fn present_backup_auth_credential(
    credential: &zkgroup::backups::BackupAuthCredential,
    params: &GenericServerPublicParams,
    signing_key: &presage::libsignal_service::protocol::PrivateKey,
    rng: &mut dyn rand::CryptoRng,
) -> Result<(Vec<u8>, Vec<u8>), ()> {
    let presentation = credential.present(params, rng.random());
    let serialized = zkgroup::serialize(&presentation);
    let signature = signing_key
        .calculate_signature(&serialized, rng)
        .map_err(|_| ())?;
    Ok((serialized, signature.into()))
}

fn use_base64_encode(value: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(value)
}
