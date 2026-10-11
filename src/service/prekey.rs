// SPDX-License-Identifier: AGPL-3.0-only

//! prekey 定时维护（B 批，官方对齐 P8）：2 天水位巡检 + signed prekey 按
//! 龄轮换 + 发送前 14 天超龄闸。
//!
//! 官方语义锚点：
//! - 一次性 prekey 水位补发 = libsignal-service `update_pre_key_bundle`
//!   （服务器计数 < PRE_KEY_MINIMUM=10 → 批量 100 补发；presage receive
//!   流每次开启同路径已跑，本模块补的是长连接下的 2 天定时巡检）。
//! - signed prekey 按龄轮换 = 官方客户端按时间轮换（14 天档）；水位闸在
//!   计数健康时永不轮换，本模块按龄强转（vendor
//!   `rotate_signed_pre_key_bundle` 直发补发路径）。
//! - 发送前 14 天超龄闸 = 发送开管理器时本地判龄，超龄 best-effort 轮换
//!   （fail-open：轮换失败绝不阻断发送——陈旧 signed prekey 仍可用，轮换
//!   是卫生面不是正确性面）。

use std::time::{SystemTime, UNIX_EPOCH};

use presage::libsignal_service::pre_keys::PreKeysStore;
use presage::libsignal_service::protocol::{GenericSignedPreKey as _, SignedPreKeyStore};
use presage::manager::Registered;
use presage::store::Store;
use presage::Manager;

use crate::service::SqliteStore;

/// 水位巡检节律（2 天）：书签超期即补水位 + 判龄轮换。
pub(crate) const MAINTENANCE_INTERVAL_MS: u64 = 2 * 24 * 60 * 60 * 1_000;
/// 快速巡检时间触发（30min）：长连接内的低水位快速路径。`refresh_pre_keys`
/// 自带服务器计数闸（≥`PRE_KEY_MINIMUM`=10 零上传），触发只多一次
/// GET /v2/keys，30min 一拍无成本压力。
pub(crate) const PREKEY_QUICK_CHECK_INTERVAL_MS: u64 = 30 * 60 * 1_000;
/// 快速巡检量触发（信封数）：一次大流量窗内也可能把水位打穿，按量兜底。
/// 计数含解密失败/重复投递臂——服务器侧取信封同样消耗一次性 prekey。
pub(crate) const PREKEY_QUICK_CHECK_MESSAGES: u64 = 200;
/// signed prekey 超龄闸（14 天，官方客户端按龄轮换同档）。
pub(crate) const SIGNED_PRE_KEY_MAX_AGE_MS: u64 = 14 * 24 * 60 * 60 * 1_000;
/// 巡检书签 kv 键（per-account store.db，跨重启生效）。
const KV_LAST_RUN_MS: &str = "kt_prekey_maintenance_last_run_ms";

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 巡检到期判定（纯函数）：无书签 = 立即巡检（首启即补水位）；书签超
/// `MAINTENANCE_INTERVAL_MS` = 到期。书签落在未来（时钟回拨）按未到期
/// 处理，不追赶。
pub(crate) fn maintenance_due(last_run_ms: Option<u64>, now: u64) -> bool {
    match last_run_ms {
        None => true,
        Some(last) => now.saturating_sub(last) >= MAINTENANCE_INTERVAL_MS,
    }
}

/// signed prekey 超龄判定（纯函数）：`None` 时间戳/键缺失 = 超龄（无证据
/// 视为陈旧，宁可多轮换）；超 `SIGNED_PRE_KEY_MAX_AGE_MS` = 超龄。
pub(crate) fn signed_pre_key_over_age(created_at_ms: Option<u64>, now: u64) -> bool {
    match created_at_ms {
        None => true,
        Some(created) => now.saturating_sub(created) >= SIGNED_PRE_KEY_MAX_AGE_MS,
    }
}

/// 双身份族 signed prekey 龄值读取（本地 store，零网络面）：取 ACI/PNI 两
/// 族当前 signed prekey 的 created-at；任一族读不到（键缺失/记录缺失） =
/// `None`（闸按超龄处理）。记录时间戳来自 libsignal 记录本体（生成时刻），
/// 无需额外落账。
async fn oldest_signed_pre_key_created_at(store: &SqliteStore) -> Option<u64> {
    let aci = store.aci_protocol_store();
    let pni = store.pni_protocol_store();
    let mut oldest = None;
    for protocol_store in [&aci, &pni] {
        let id = protocol_store.signed_prekey_id().await.ok().flatten()?;
        let record = protocol_store.get_signed_pre_key(id).await.ok()?;
        let millis = record.timestamp().ok()?.epoch_millis();
        oldest = Some(oldest.map_or(millis, |prev: u64| prev.min(millis)));
    }
    oldest
}

/// 发送前超龄闸（open_registered 单一漏斗挂点）：signed prekey 超 14 天 →
/// best-effort 按龄轮换（两族一次跑齐）。任何失败静默（fail-open——轮换
/// 失败不阻断发送，下一轮发送再试；官方陈旧 signed prekey 仍可建立会话）。
pub(crate) async fn send_gate(manager: &mut Manager<SqliteStore, Registered>) {
    let now = now_ms();
    let Some(created_at) = oldest_signed_pre_key_created_at(manager.store()).await else {
        return;
    };
    if !signed_pre_key_over_age(Some(created_at), now) {
        return;
    }
    let _ = manager.rotate_signed_pre_keys().await;
}

/// 快速巡检时间到期判定（纯函数）：`None` = 未到期（首拍水位面归
/// 2 天 maintenance_tick 的立即巡检，快速路径不重复打服务器）；书签超
/// 30min = 到期；未来书签（时钟回拨）不追赶。
pub(crate) fn quick_check_due(last_check_ms: Option<u64>, now: u64) -> bool {
    match last_check_ms {
        None => false,
        Some(last) => now.saturating_sub(last) >= PREKEY_QUICK_CHECK_INTERVAL_MS,
    }
}

/// 快速巡检量触发判定（纯函数）：窗内信封计数达阈值即到期。
pub(crate) fn quick_check_volume_due(seen: u64) -> bool {
    seen >= PREKEY_QUICK_CHECK_MESSAGES
}

/// 快速巡检执行面（receive 管线时间/量触发挂点）：只做水位补发（双身份
/// 族，服务器计数闸决定是否真发），signed prekey 按龄轮换留给 2 天
/// maintenance_tick。全链路 best-effort：失败静默，只影响本轮，下一拍
/// 时间/量触发再补。
pub(crate) async fn quick_check(manager: &mut Manager<SqliteStore, Registered>) {
    let _ = manager.refresh_pre_keys().await;
}

/// 定时巡检执行面（receive 管线节律臂挂点）：书签到期 → 水位补发（双身份
/// 族，服务器计数闸决定是否真发）+ signed prekey 按龄轮换（超龄才转）→
/// 书签落库。全链路 best-effort：任一步失败静默，只影响本轮，书签照写
/// （下轮节律再补——水位是巡检语义不是告警语义，不因单次失败连轰）。
pub(crate) async fn maintenance_tick(manager: &mut Manager<SqliteStore, Registered>) {
    let now = now_ms();
    let last_run = manager
        .store()
        .kt_kv_get(KV_LAST_RUN_MS)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok());
    if !maintenance_due(last_run, now) {
        return;
    }
    let _ = manager.refresh_pre_keys().await;
    if let Some(created_at) = oldest_signed_pre_key_created_at(manager.store()).await {
        if signed_pre_key_over_age(Some(created_at), now) {
            let _ = manager.rotate_signed_pre_keys().await;
        }
    }
    let _ = manager
        .store()
        .kt_kv_set(KV_LAST_RUN_MS, &now.to_string())
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 巡检节律（2 天）：无书签立即巡检；书签新鲜未到期；恰 2 天边界到期；
    /// 未来书签（时钟回拨）不追赶。
    #[test]
    fn maintenance_cadence_follows_two_day_bookmark() {
        let day: u64 = 24 * 60 * 60 * 1_000;
        assert!(maintenance_due(None, 1_000));
        assert!(!maintenance_due(Some(1_000), 1_000 + day));
        assert!(maintenance_due(Some(1_000), 1_000 + 2 * day));
        assert!(maintenance_due(Some(1_000), 1_000 + 3 * day));
        // 时钟回拨：书签在未来，saturating_sub=0 < 2 天，不追赶。
        assert!(!maintenance_due(Some(5_000), 1_000));
    }

    /// signed prekey 超龄闸（14 天）：缺证据（None）= 超龄；恰 14 天边界
    /// 超龄；13 天内不轮换。
    #[test]
    fn signed_pre_key_age_gate_matches_official_14d() {
        let day: u64 = 24 * 60 * 60 * 1_000;
        assert!(signed_pre_key_over_age(None, 1_000));
        assert!(!signed_pre_key_over_age(Some(1_000), 1_000 + 13 * day));
        assert!(signed_pre_key_over_age(Some(1_000), 1_000 + 14 * day));
        assert!(signed_pre_key_over_age(Some(1_000), 1_000 + 15 * day));
    }

    /// 快速巡检时间触发（30min）：无书签未到期（首拍归 2 天巡检面）；窗口
    /// 内未到期；恰 30min 边界到期；未来书签（时钟回拨）不追赶。
    #[test]
    fn quick_check_time_trigger_is_thirty_minutes() {
        let half: u64 = PREKEY_QUICK_CHECK_INTERVAL_MS;
        assert!(!quick_check_due(None, 1_000));
        assert!(!quick_check_due(Some(1_000), 1_000 + half - 1));
        assert!(quick_check_due(Some(1_000), 1_000 + half));
        assert!(!quick_check_due(Some(5_000), 1_000));
    }

    /// 快速巡检量触发（200 信封）：199 不触发；恰 200 触发；超量触发。
    #[test]
    fn quick_check_volume_trigger_at_two_hundred() {
        assert!(!quick_check_volume_due(0));
        assert!(!quick_check_volume_due(199));
        assert!(quick_check_volume_due(200));
        assert!(quick_check_volume_due(500));
    }
}
