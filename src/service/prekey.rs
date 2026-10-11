// SPDX-License-Identifier: AGPL-3.0-only

//! prekey 定时维护（B 批，官方对齐 P8；时序审计组四 E1 收紧）：2 天水位
//! 巡检 + signed prekey 与 last-resort kyber prekey 按 1.5 天龄轮换 +
//! 发送前超龄闸。
//!
//! 官方语义锚点：
//! - 一次性 prekey 水位补发 = libsignal-service `update_pre_key_bundle`
//!   （服务器计数 < PRE_KEY_MINIMUM=10 → 批量 100 补发；presage receive
//!   流每次开启同路径已跑，本模块补的是长连接下的 2 天定时巡检）。
//! - signed prekey 按龄轮换 = 官方 1.5 天档（AccountManager.preload.ts
//!   :141）；last-resort kyber 同 1.5 天按龄轮换（:115）且与 signed 同一
//!   PUT 携带（#doUpdateKeys / #maybeUpdateLastResortKyberKey）。
//! - 发送前超龄闸 = 发送开管理器时本地判龄，超龄 best-effort 轮换
//!   （fail-open：轮换失败绝不阻断发送——陈旧 signed prekey 仍可用，轮换
//!   是卫生面不是正确性面）。
//! - 偏差登记：官方轮换判据 = 最近 confirmed 键的 lastUpdate；我方 store
//!   先于上传、无 confirmed 位，视 stored = confirmed（记录内嵌时间戳即
//!   生成时刻）。

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
/// signed prekey 超龄闸（1.5 天，官方 AccountManager.preload.ts:141
/// SIGNED_PRE_KEY_ROTATION_AGE = DAY*1.5 同值）。
pub(crate) const SIGNED_PRE_KEY_MAX_AGE_MS: u64 = 36 * 60 * 60 * 1_000;
/// last-resort kyber prekey 超龄闸（同 1.5 天，官方 :115
/// LAST_RESORT_KEY_ROTATION_AGE = DAY*1.5——官方按龄轮换 last-resort，
/// 与 signed 同 PUT 携带上传）。
pub(crate) const LAST_RESORT_KEY_MAX_AGE_MS: u64 = 36 * 60 * 60 * 1_000;

// 本地保留窗（官方 _cleanPreKeys/_cleanKyberPreKeys/_cleanSignedPreKeys
// 同构：保留 newest-N，窗外的还要够老才删）。
/// 一次性族保留 newest（官方 PRE_KEY_MAX_COUNT = 200，AccountManager
/// .preload.ts:128）。
pub(crate) const PRE_KEY_KEEP_NEWEST: usize = 200;
/// 一次性族窗龄（官方 PRE_KEY_ARCHIVE_AGE = 90 天，:120）。
pub(crate) const PRE_KEY_ARCHIVE_AGE_MS: u64 = 90 * 24 * 60 * 60 * 1_000;
/// signed/last-resort 族保留 newest（官方 SIGNED_PRE_KEY_MINIMUM = 5 :143、
/// LAST_RESORT_KEY_MINIMUM = 5 :116）。
pub(crate) const SIGNED_FAMILY_KEEP_NEWEST: usize = 5;
/// signed/last-resort 族窗龄 floor（官方 getMessageQueueTime
/// `max(remoteConfig, MONTH/SECOND)` 的 MONTH 下限，30 天；remote config
/// 读面见 `fetch_message_queue_time_seconds`，失败落本值）。
pub(crate) const MESSAGE_QUEUE_TIME_FLOOR_SECONDS: u64 = 30 * 24 * 60 * 60;
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

/// last-resort kyber prekey 超龄判定（纯函数）：`None` = 该族无
/// last-resort 键——补装归水位路径（update_pre_key_bundle 生成语义），
/// 轮换路径不越权，返回 false；有键超 `LAST_RESORT_KEY_MAX_AGE_MS` =
/// 超龄（官方 #maybeUpdateLastResortKyberKey `!isMoreRecentThan` 同判据）。
pub(crate) fn last_resort_key_over_age(newest_created_at_ms: Option<u64>, now: u64) -> bool {
    match newest_created_at_ms {
        None => false,
        Some(created) => now.saturating_sub(created) >= LAST_RESORT_KEY_MAX_AGE_MS,
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

/// 双身份族 last-resort kyber prekey 最新龄值读取（本地 store，零网络面）：
/// 每族取 newest（服务器在册 = 最后 PUT 的那把，判龄以它为准），两族取
/// 更旧者（与 signed 读面同一保守口径）。两族都无 last-resort 键 =
/// `None`（水位补装语义，不进轮换判断）。
async fn oldest_newest_last_resort_created_at(store: &SqliteStore) -> Option<u64> {
    use presage::libsignal_service::pre_keys::KyberPreKeyStoreExt as _;

    let aci = store.aci_protocol_store();
    let pni = store.pni_protocol_store();
    let mut oldest_newest = None;
    for protocol_store in [&aci, &pni] {
        let keys = protocol_store
            .load_last_resort_kyber_pre_keys()
            .await
            .ok()?;
        let newest = keys
            .into_iter()
            .filter_map(|key| key.timestamp().ok().map(|ts| ts.epoch_millis()))
            .max()?;
        oldest_newest = Some(oldest_newest.map_or(newest, |prev: u64| prev.min(newest)));
    }
    oldest_newest
}

/// 发送前超龄闸（open_registered 单一漏斗挂点）：signed prekey 超 1.5 天 →
/// best-effort 按龄轮换（两族一次跑齐；last-resort 同 PUT 齐转）。
/// 任何失败静默（fail-open——轮换失败不阻断发送，下一轮发送再试；官方
/// 陈旧 signed prekey 仍可建立会话）。
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

/// 按龄轮换判定（纯函数）：signed 超龄（缺证据=超龄，宁多转）或
/// last-resort 超龄（缺键=水位补装语义、轮换不越权）任一成立即轮换；
/// 轮换 PUT 本身 signed + last-resort 同携（官方 #doUpdateKeys 同形），
/// 单侧超龄即两侧齐转。
pub(crate) fn rotation_due(
    signed_created_at: Option<u64>,
    last_resort_newest: Option<u64>,
    now: u64,
) -> bool {
    signed_pre_key_over_age(signed_created_at, now)
        || last_resort_key_over_age(last_resort_newest, now)
}

/// 快速巡检执行面（receive 管线时间/量触发挂点）：水位补发（双身份族，
/// 服务器计数闸决定是否真发）+ 按龄轮换判（本地读零成本；1.5 天龄档细于
/// 2 天 maintenance 节律，判龄挂 30min 快拍保证实际龄不漂过 2 天）。
/// 全链路 best-effort：失败静默，只影响本轮，下一拍时间/量触发再补。
pub(crate) async fn quick_check(manager: &mut Manager<SqliteStore, Registered>) {
    let _ = manager.refresh_pre_keys().await;
    let now = now_ms();
    let signed = oldest_signed_pre_key_created_at(manager.store()).await;
    let last_resort = oldest_newest_last_resort_created_at(manager.store()).await;
    if rotation_due(signed, last_resort, now) {
        let _ = manager.rotate_signed_pre_keys().await;
    }
}

/// 保留窗判删（纯函数）：按 created-at 升序取最旧 `keep_newest` 窗外的
/// 条目，其中早于 `older_than_ms` 的才进删除名单——窗外的年轻条目保留
/// （官方 _clean* 家族「count 超了还要够老」双条件同构）。`keep_newest`
/// 恒存活（当前在役 signed/last-resort 键按构造必在最新一批内）。
pub(crate) fn sweep_candidates(
    entries: Vec<(u32, u64)>,
    keep_newest: usize,
    older_than_ms: u64,
    now: u64,
) -> Vec<u32> {
    let mut entries = entries;
    entries.sort_by_key(|&(_, ts)| ts);
    let removable = entries.len().saturating_sub(keep_newest);
    entries
        .into_iter()
        .take(removable)
        .filter(|&(_, ts)| now.saturating_sub(ts) >= older_than_ms)
        .map(|(id, _)| id)
        .collect()
}

/// signed/last-resort 族窗龄（官方 getMessageQueueTime 同式，纯函数）：
/// `max(remoteConfig 秒值, 30 天 floor)`；config 缺席 = floor（官方
/// parseIntWithFallback 缺省 MONTH/SECOND 同 fail-open 语义）。
pub(crate) fn signed_family_delete_age_ms(message_queue_time_seconds: Option<u64>) -> u64 {
    let seconds = message_queue_time_seconds
        .unwrap_or(MESSAGE_QUEUE_TIME_FLOOR_SECONDS)
        .max(MESSAGE_QUEUE_TIME_FLOOR_SECONDS);
    seconds.saturating_mul(1_000)
}

/// remote config 读面（官方 WebAPI getConfig 行为对齐：chatService host
/// `GET v2/config`、账号 basic auth、body = `{config: {name: value}}`
/// JSON map；`HttpAuthOverride::NoOverride` = 注册凭据 basic auth）。
/// 取 `config["global.messageQueueTimeInSeconds"]`（字符串秒，官方
/// RemoteConfig 值形态）。任何失败（非 2xx/坏 JSON/键缺席/非数）= None，
/// 调用方落 floor——不缓存不重试（maintenance 2 天一拍，下拍再取；官方
/// desktop 有本地 config 缓存 + ETag 增量拉，headless 引擎拍频低无需
/// 同构）。
async fn fetch_message_queue_time_seconds(
    push: &presage::libsignal_service::prelude::PushService,
) -> Option<u64> {
    use presage::libsignal_service::configuration::Endpoint;
    use presage::libsignal_service::push_service::HttpAuthOverride;
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service("v2/config"),
            HttpAuthOverride::NoOverride,
        )
        .ok()?
        .send()
        .await
        .ok()?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return None;
    }
    let body: serde_json::Value = response.json().await.ok()?;
    body.get("config")?
        .get("global.messageQueueTimeInSeconds")?
        .as_str()?
        .parse::<u64>()
        .ok()
}

/// 本地清理执行面（maintenance_tick 挂点）：四族按官方保留窗扫删——
/// 一次性 200/90d、signed 5/{messageQueueTime 窗}、常规 kyber 200/90d、
/// last-resort 5/{messageQueueTime 窗}。signed/last-resort 窗龄取 remote
/// config `global.messageQueueTimeInSeconds`（官方 getMessageQueueTime
/// 同源；读面失败落 30 天 floor，2 天一拍只多一次 GET）。删除名单之外
/// 的键（含当前在役 signed/last-resort）恒存活。全链路 best-effort：
/// 读/删失败静默，下轮节律再扫。
pub(crate) async fn cleanup_tick(manager: &mut Manager<SqliteStore, Registered>) {
    let message_queue_time =
        match super::push_service_from_registration(manager.registration_data()) {
            Some(push) => fetch_message_queue_time_seconds(&push).await,
            None => None,
        };
    let signed_family_delete_age_ms = signed_family_delete_age_ms(message_queue_time);
    let now = now_ms();
    let store = manager.store();
    for protocol_store in [store.aci_protocol_store(), store.pni_protocol_store()] {
        let one_time = match protocol_store.kt_list_one_time_pre_keys().await {
            Ok(entries) => sweep_candidates(
                entries
                    .into_iter()
                    .map(|(id, ts)| (id, u64::try_from(ts).unwrap_or(0)))
                    .collect(),
                PRE_KEY_KEEP_NEWEST,
                PRE_KEY_ARCHIVE_AGE_MS,
                now,
            ),
            Err(_) => continue,
        };
        let _ = protocol_store.kt_remove_pre_keys(&one_time).await;

        let signed = match protocol_store.kt_list_signed_pre_keys().await {
            Ok(entries) => sweep_candidates(
                entries,
                SIGNED_FAMILY_KEEP_NEWEST,
                signed_family_delete_age_ms,
                now,
            ),
            Err(_) => continue,
        };
        let _ = protocol_store.kt_remove_signed_pre_keys(&signed).await;

        let kyber = match protocol_store.kt_list_kyber_pre_keys().await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        let regular = kyber
            .iter()
            .filter(|(_, _, is_last_resort)| !is_last_resort)
            .map(|&(id, ts, _)| (id, ts))
            .collect();
        let regular = sweep_candidates(regular, PRE_KEY_KEEP_NEWEST, PRE_KEY_ARCHIVE_AGE_MS, now);
        let _ = protocol_store.kt_remove_kyber_pre_keys(&regular).await;

        let last_resort = kyber
            .iter()
            .filter(|(_, _, is_last_resort)| *is_last_resort)
            .map(|&(id, ts, _)| (id, ts))
            .collect();
        let last_resort = sweep_candidates(
            last_resort,
            SIGNED_FAMILY_KEEP_NEWEST,
            signed_family_delete_age_ms,
            now,
        );
        let _ = protocol_store.kt_remove_kyber_pre_keys(&last_resort).await;
    }
}

/// 定时巡检执行面（receive 管线节律臂挂点）：书签到期 → 水位补发（双身份
/// 族，服务器计数闸决定是否真发）+ 按龄轮换（signed/last-resort 任一超龄
/// 才转；quick_check 30min 快拍已常态兜底，本拍保留幂等）+ 本地四族保留
/// 窗扫删（cleanup_tick）→ 书签落库。
/// 全链路 best-effort：任一步失败静默，只影响本轮，书签照写（下轮节律再
/// 补——水位是巡检语义不是告警语义，不因单次失败连轰）。
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
    let signed = oldest_signed_pre_key_created_at(manager.store()).await;
    let last_resort = oldest_newest_last_resort_created_at(manager.store()).await;
    if rotation_due(signed, last_resort, now) {
        let _ = manager.rotate_signed_pre_keys().await;
    }
    cleanup_tick(manager).await;
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

    /// signed prekey 超龄闸（1.5 天，官方 SIGNED_PRE_KEY_ROTATION_AGE）：
    /// 缺证据（None）= 超龄；恰 1.5 天边界超龄；1 天内不轮换。
    #[test]
    fn signed_pre_key_age_gate_matches_official_15d() {
        let hour: u64 = 60 * 60 * 1_000;
        let age = 36 * hour;
        assert!(signed_pre_key_over_age(None, 1_000));
        assert!(!signed_pre_key_over_age(Some(1_000), 1_000 + 24 * hour));
        assert!(signed_pre_key_over_age(Some(1_000), 1_000 + age));
        assert!(signed_pre_key_over_age(Some(1_000), 1_000 + age + hour));
    }

    /// last-resort kyber 超龄闸（1.5 天，官方 LAST_RESORT_KEY_ROTATION_AGE）：
    /// 缺键（None）= 不归轮换管（水位补装语义，返回 false）；恰 1.5 天边界
    /// 超龄；1 天内不轮换。
    #[test]
    fn last_resort_age_gate_matches_official_15d() {
        let hour: u64 = 60 * 60 * 1_000;
        let age = 36 * hour;
        assert!(!last_resort_key_over_age(None, 1_000));
        assert!(!last_resort_key_over_age(Some(1_000), 1_000 + 24 * hour));
        assert!(last_resort_key_over_age(Some(1_000), 1_000 + age));
    }

    /// 按龄轮换合成判定：signed 超龄即转（含缺证据）；last-resort 超龄即转
    /// （signed 新鲜也转——同 PUT 齐转）；两侧都新鲜不转；last-resort 缺键
    /// 且 signed 新鲜不转。
    #[test]
    fn rotation_due_or_gate() {
        let hour: u64 = 60 * 60 * 1_000;
        let fresh = 1_000;
        let stale = 1_000 - 1;
        let now_fresh_window = fresh + 10 * hour; // < 36h：都算新鲜
        let now_stale_window = fresh + 40 * hour; // > 36h：都算超龄
                                                  // signed 超龄（缺证据同语义，此处给旧值）→ 转；last-resort 缺键不越权。
        assert!(rotation_due(Some(stale), None, now_stale_window));
        // signed 新鲜 + last-resort 缺键 → 不转（缺键归水位补装）。
        assert!(!rotation_due(Some(fresh), None, now_fresh_window));
        // last-resort 超龄 → 转（signed 新鲜也转——同 PUT 齐转）。
        assert!(rotation_due(Some(fresh), Some(stale), now_stale_window));
        // 两侧新鲜 → 不转。
        assert!(!rotation_due(Some(fresh), Some(fresh), now_fresh_window));
    }

    /// 保留窗判删（官方 _clean* 双条件钉形）：窗内恒活；窗外够老才删；
    /// 窗外年轻保留；全量小于窗全活。
    #[test]
    fn sweep_candidates_keep_newest_and_age_gate() {
        let day: u64 = 24 * 60 * 60 * 1_000;
        let now = 1_000 * day;
        // 5 把：ids 递增但时间戳故意乱序（store 读面无序，函数内排序）。
        let entries = vec![
            (1u32, now - 40 * day),
            (2, now - 10 * day),
            (3, now - 100 * day),
            (4, now - 200 * day),
            (5, now - day),
        ];
        // keep 2：窗外 = {1, 3, 4}（最旧三把），其中超 90 天 = {3, 4}；
        // 1 号窗外但只 40 天 → 保留。
        let mut ids = sweep_candidates(entries, 2, 90 * day, now);
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 4]);

        // 窗 ≥ 总量：一个不删（含超老键）。
        let entries = vec![(1u32, now - 400 * day), (2, now - 300 * day)];
        assert!(sweep_candidates(entries, 5, 90 * day, now).is_empty());

        // created_at=0（migration 前旧行）= 最旧：窗外即删（90 天必然超过）。
        let entries = vec![(7u32, 0), (8, now - day)];
        assert_eq!(sweep_candidates(entries, 1, 90 * day, now), vec![7]);
    }

    /// signed/last-resort 族窗龄（官方 getMessageQueueTime 同式回归锚）：
    /// config 缺席/低于 floor = 30 天 floor；等于 floor 原值；高于 floor
    /// 取 remote 值（官方 max(parsed, MONTH/SECOND) 同式）。
    #[test]
    fn signed_family_delete_age_ms_matches_official_max_floor() {
        let day_ms: u64 = 24 * 60 * 60 * 1_000;
        let floor = 30 * 24 * 60 * 60;
        // 缺席 = floor。
        assert_eq!(signed_family_delete_age_ms(None), 30 * day_ms);
        // 低于 floor（服务器调小也压不过下限）= floor。
        assert_eq!(signed_family_delete_age_ms(Some(3600)), 30 * day_ms);
        assert_eq!(signed_family_delete_age_ms(Some(floor - 1)), 30 * day_ms);
        // 恰 floor。
        assert_eq!(signed_family_delete_age_ms(Some(floor)), 30 * day_ms);
        // 高于 floor = remote 值。
        assert_eq!(
            signed_family_delete_age_ms(Some(floor + 86_400)),
            31 * day_ms
        );
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
