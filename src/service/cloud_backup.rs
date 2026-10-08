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
