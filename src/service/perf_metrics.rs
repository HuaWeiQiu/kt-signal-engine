// SPDX-License-Identifier: AGPL-3.0-only

//! S2a 性能基线打点层（只测不改，立项纪律见
//! `kt-desktop-wt-signal/docs/plan/signal-runtime-followup-projects.md` §附四②）：
//! 进程内有界指标注册表 + `engine.perfMetrics` 只读投影（methods.rs）。
//!
//! 纪律对位：
//! - 零 stderr（compat §1：connector watchdog 以 stderr ERROR 行判死活）；
//! - 零行为语义变化——只包 `Instant` 计时与原子计数，调用序/返回值/控制流
//!   不动；测量点全部走单一咽喉（`Service::flush_barrier`、
//!   `lock_registered_for_send`、`dispatch_group_message` 等），不在热路径
//!   分散采样；
//! - 词表封闭（[`keys`]）：新测量点必须先登记常量，未知键计
//!   `unknown_key_dropped` 丢弃，内存有界不随运行增长；
//! - 计时单位微秒（`totalUs/maxUs/lastUs`），快照键序 BTreeMap 确定性输出。
//!
//! 消费面：`engine.perfMetrics` JSON-RPC 只读方法（wire additive，
//! 【engine-compat】；connector 白名单透传属 connector 切片遗留，S2b 实测
//! 前由直连 harness/契约测试消费）。

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde_json::json;

/// 测量点词表（封闭集；键名 snake_case，快照逐字透出）。
pub(crate) mod keys {
    // —— 计时序列（微秒）——
    /// 收流信封到达 → ack 完成（Contacts/DecryptionError/Duplicate/
    /// IdentityChange/Content 五臂；QueueEmpty 非信封不计时；StdioDead
    /// 停管线路径不采样）。
    pub const RECEIVE_ENVELOPE: &str = "receive_envelope";
    /// `project_content` 投影（Content/IdentityChange 共用咽喉）。
    pub const RECEIVE_PROJECT: &str = "receive_project";
    /// G1 flush 背压栅栏（`Service::flush_barrier` 咽喉；O3 行为候选的
    /// 测量面——批量化与否凭本序列基线申请）。
    pub const FLUSH_BARRIER: &str = "flush_barrier";
    /// 常驻发送管理器锁等待（`lock_registered_for_send` 咽喉，含 send_gate
    /// 按龄轮换；P12 #1 常驻链的观测面）。
    pub const SEND_LOCK_WAIT: &str = "send_lock_wait";
    /// 文本族发送整体（methods.rs send 调用点 → 返回）。
    pub const SEND_TEXT_TOTAL: &str = "send_text_total";
    /// 群发送整体（Service 群发送咽喉 → 返回，MRM/failover 汇总）。
    pub const SEND_GROUP_TOTAL: &str = "send_group_total";
    /// JSON-RPC 请求分派整体（handle_inbound 单点包裹）。
    pub const RPC_DISPATCH: &str = "rpc_dispatch";
    /// prekey 快速巡检（P12 #8；30min 时间拍 + 200 信封量拍共用执行面）。
    pub const PREKEY_QUICK_CHECK: &str = "prekey_quick_check";
    /// prekey 2 天维护拍。
    pub const PREKEY_MAINTENANCE: &str = "prekey_maintenance_tick";
    /// prekey 保留窗扫删。
    pub const PREKEY_CLEANUP: &str = "prekey_cleanup_tick";
    /// 重连退避实际等待（P12 2c'；Fibonacci 1→55s + jitter 的落地值）。
    pub const RECONNECT_BACKOFF: &str = "reconnect_backoff";
    /// 链接建立后存量补同步三请求（P15c 项4；2s 沉降后一次性）。
    pub const INITIAL_SYNC_REQUESTS: &str = "initial_sync_requests";
    /// send 链分段打点（send 性能批 2026-10-16，docs/jsonrpc-compat.md §5
    /// 「send 链分段打点」块）：阶段边界 resolve → sender_setup →
    /// cert_fetch → session → encrypt → ws_rtt；total 段沿用
    /// [`SEND_TEXT_TOTAL`]/[`SEND_GROUP_TOTAL`]，锁等待沿用
    /// [`SEND_LOCK_WAIT`]。vendor 侧四键字面量与
    /// `libsignal_service::perf_hook::keys` 跨 crate 钉等（单测）。
    /// 收件人解析整体（resolve_recipient 单一咽喉自计时）。
    pub const SEND_RESOLVE: &str = "send_resolve";
    /// 发送管理器装配：双 WS 复用或拨号（vendor presage
    /// new_message_sender；预热同序列——冷拨代价即归因要的 max 值）。
    pub const SEND_SENDER_SETUP: &str = "send_sender_setup";
    /// 发送证书续期网络臂（缓存命中零网络不计时；vendor presage）。
    pub const SEND_CERT_FETCH: &str = "send_cert_fetch";
    /// 冷会话建立：prekey 一次拉全设备 + 本地建会话（vendor
    /// libsignal-service create_encrypted_message 会话臂，预热共用单点）。
    pub const SEND_SESSION_ESTABLISH: &str = "send_session_establish";
    /// 会话内加密（cipher.encrypt；vendor libsignal-service）。
    pub const SEND_ENCRYPT: &str = "send_encrypt";
    /// WS 消息上行 → 服务器应答（修复环每轮各计一条；vendor
    /// libsignal-service try_send_message）。
    pub const SEND_WS_RTT: &str = "send_ws_rtt";

    // —— 计数器 ——
    /// 启动预热批（send 性能批 2026-10-16，compat §5「启动预热」块；
    /// send_warmup 模块）：RUN 每次预热批尝试 +1（fake 账号不计）；
    /// OK = established + existing（变热收件人数）；ERR = failed +
    /// 超时未确认数（诚实语义，超时 = 整批目标数）。
    pub const SEND_WARMUP_RUN: &str = "send_warmup_run";
    pub const SEND_WARMUP_OK: &str = "send_warmup_ok";
    pub const SEND_WARMUP_ERR: &str = "send_warmup_err";
    /// T-9 manifest 自动拉（消费臂触发源：FetchLatest STORAGE_MANIFEST 防抖
    /// 窗沿到期一次一计）。
    pub const STORAGE_MANIFEST_PULL: &str = "storage_manifest_pull";
    /// T-9 manifest 自动拉（登录期触发源：建流 +2s 沉降时机带，60s 闸内
    /// 恰一次）。
    pub const STORAGE_MANIFEST_PULL_LOGIN: &str = "storage_manifest_pull_login";
    /// T-9 manifest 自动拉逐腿失败计（mergeContacts/importGroups 各自计；
    /// 读面幂等，失败靠下次触发/下次链路建立重补）。
    pub const STORAGE_MANIFEST_PULL_ERR: &str = "storage_manifest_pull_err";
    pub const RECEIVE_KIND_CONTENT: &str = "receive_kind_content";
    pub const RECEIVE_KIND_IDENTITY_CHANGE: &str = "receive_kind_identity_change";
    pub const RECEIVE_KIND_DECRYPTION_ERROR: &str = "receive_kind_decryption_error";
    pub const RECEIVE_KIND_DUPLICATE: &str = "receive_kind_duplicate";
    pub const RECEIVE_KIND_CONTACTS: &str = "receive_kind_contacts";
    pub const RECEIVE_KIND_QUEUE_EMPTY: &str = "receive_kind_queue_empty";
    /// 群取回失败信封计（接收完整性批②：groupSyncFailed/groupTerminated
    /// 事件腿，decode+gone 合并计数）。
    pub const RECEIVE_KIND_GROUP_SYNC_FAILED: &str = "receive_kind_group_sync_failed";
    pub const FLUSH_BARRIER_OK: &str = "flush_barrier_ok";
    pub const FLUSH_BARRIER_DEAD: &str = "flush_barrier_dead";
    pub const SEND_TEXT_OK: &str = "send_text_ok";
    pub const SEND_TEXT_ERR: &str = "send_text_err";
    pub const SEND_GROUP_MRM: &str = "send_group_mrm";
    pub const SEND_GROUP_FAILOVER: &str = "send_group_failover";
    pub const SEND_GROUP_REPAIR_ROUND: &str = "send_group_repair_round";
    pub const PREKEY_QUICK_TIME_TRIGGER: &str = "prekey_quick_check_time_trigger";
    pub const PREKEY_QUICK_VOLUME_TRIGGER: &str = "prekey_quick_check_volume_trigger";
    pub const STREAM_RECOVERABLE: &str = "stream_outcome_recoverable";
    pub const STREAM_CONNECTED: &str = "stream_outcome_connected";
    pub const STREAM_UNBOUND: &str = "stream_outcome_unbound";
    pub const STREAM_STDIO_DEAD: &str = "stream_outcome_stdio_dead";
    /// 接收流以官方「不重连」close code（1000/4409）结束：停环等显式重链。
    pub const STREAM_CLOSED_NO_RECONNECT: &str = "stream_outcome_closed_no_reconnect";
    /// 遗留键（单连接纪律批）：2026-10-21 对齐官方后 4409 统一走
    /// `STREAM_CLOSED_NO_RECONNECT` 停环，本键保留仅为词表/wire 兼容，不再
    /// 递增。
    pub const STREAM_CLOSE_4409_EXTERNAL: &str = "stream_close_4409_external";
    /// 遗留键（同上）：4409 自家互搏残影臂已并入统一停环语义，不再递增。
    pub const STREAM_CLOSE_4409_SELF: &str = "stream_close_4409_self";
}

/// 词表全集（计时 + 计数同表校验；快照只透出登记过的键）。
const KNOWN_KEYS: &[&str] = &[
    keys::RECEIVE_ENVELOPE,
    keys::RECEIVE_PROJECT,
    keys::FLUSH_BARRIER,
    keys::SEND_LOCK_WAIT,
    keys::SEND_TEXT_TOTAL,
    keys::SEND_GROUP_TOTAL,
    keys::RPC_DISPATCH,
    keys::PREKEY_QUICK_CHECK,
    keys::PREKEY_MAINTENANCE,
    keys::PREKEY_CLEANUP,
    keys::RECONNECT_BACKOFF,
    keys::INITIAL_SYNC_REQUESTS,
    keys::SEND_RESOLVE,
    keys::SEND_SENDER_SETUP,
    keys::SEND_CERT_FETCH,
    keys::SEND_SESSION_ESTABLISH,
    keys::SEND_ENCRYPT,
    keys::SEND_WS_RTT,
    keys::SEND_WARMUP_RUN,
    keys::SEND_WARMUP_OK,
    keys::SEND_WARMUP_ERR,
    keys::STORAGE_MANIFEST_PULL,
    keys::STORAGE_MANIFEST_PULL_LOGIN,
    keys::STORAGE_MANIFEST_PULL_ERR,
    keys::RECEIVE_KIND_CONTENT,
    keys::RECEIVE_KIND_IDENTITY_CHANGE,
    keys::RECEIVE_KIND_DECRYPTION_ERROR,
    keys::RECEIVE_KIND_DUPLICATE,
    keys::RECEIVE_KIND_CONTACTS,
    keys::RECEIVE_KIND_QUEUE_EMPTY,
    keys::RECEIVE_KIND_GROUP_SYNC_FAILED,
    keys::FLUSH_BARRIER_OK,
    keys::FLUSH_BARRIER_DEAD,
    keys::SEND_TEXT_OK,
    keys::SEND_TEXT_ERR,
    keys::SEND_GROUP_MRM,
    keys::SEND_GROUP_FAILOVER,
    keys::SEND_GROUP_REPAIR_ROUND,
    keys::PREKEY_QUICK_TIME_TRIGGER,
    keys::PREKEY_QUICK_VOLUME_TRIGGER,
    keys::STREAM_RECOVERABLE,
    keys::STREAM_CONNECTED,
    keys::STREAM_UNBOUND,
    keys::STREAM_STDIO_DEAD,
    keys::STREAM_CLOSED_NO_RECONNECT,
    keys::STREAM_CLOSE_4409_EXTERNAL,
    keys::STREAM_CLOSE_4409_SELF,
];

#[derive(Default)]
struct Series {
    count: u64,
    total_us: u64,
    max_us: u64,
    last_us: u64,
}

#[derive(Default)]
struct Registry {
    timings: BTreeMap<&'static str, Series>,
    counters: BTreeMap<&'static str, u64>,
    unknown_key_dropped: u64,
    started: Option<Instant>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

fn known(key: &str) -> bool {
    KNOWN_KEYS.contains(&key)
}

/// 计一次时长（微秒）。键不在词表 → 计丢弃计数，不落表（有界面）。
pub(crate) fn record_us(key: &'static str, us: u64) {
    let mut registry = registry().lock().expect("perf registry poisoned");
    if registry.started.is_none() {
        registry.started = Some(Instant::now());
    }
    if !known(key) {
        registry.unknown_key_dropped += 1;
        return;
    }
    let series = registry.timings.entry(key).or_default();
    series.count += 1;
    series.total_us = series.total_us.saturating_add(us);
    series.max_us = series.max_us.max(us);
    series.last_us = us;
}

/// 计数器累加。键不在词表 → 计丢弃计数，不落表。
pub(crate) fn add(key: &'static str, n: u64) {
    let mut registry = registry().lock().expect("perf registry poisoned");
    if registry.started.is_none() {
        registry.started = Some(Instant::now());
    }
    if !known(key) {
        registry.unknown_key_dropped += 1;
        return;
    }
    *registry.counters.entry(key).or_insert(0) += n;
}

/// `engine.perfMetrics` 快照：`{uptimeMs, timings, counters, unknownKeyDropped}`。
/// 键序 BTreeMap 确定性；未命中的计时/计数键不出现（诚实缺席，不发明 0）。
pub(crate) fn snapshot_json() -> serde_json::Value {
    let registry = registry().lock().expect("perf registry poisoned");
    let uptime_ms = registry
        .started
        .map(|started| started.elapsed().as_millis() as u64)
        .unwrap_or(0);
    let timings = serde_json::Map::from_iter(registry.timings.iter().map(|(key, series)| {
        (
            (*key).to_string(),
            json!({
                "count": series.count,
                "totalUs": series.total_us,
                "maxUs": series.max_us,
                "lastUs": series.last_us,
            }),
        )
    }));
    let counters = serde_json::Map::from_iter(
        registry
            .counters
            .iter()
            .map(|(key, value)| ((*key).to_string(), json!(value))),
    );
    json!({
        "uptimeMs": uptime_ms,
        "timings": timings,
        "counters": counters,
        "unknownKeyDropped": registry.unknown_key_dropped,
    })
}

#[cfg(test)]
fn reset_for_tests() {
    let mut registry = registry().lock().expect("perf registry poisoned");
    *registry = Registry {
        started: Some(Instant::now()),
        ..Registry::default()
    };
}

#[cfg(test)]
mod tests {
    use super::{add, keys, record_us, reset_for_tests, snapshot_json};

    #[test]
    fn record_then_snapshot_has_series_and_counter_absent_until_used() {
        reset_for_tests();
        record_us(keys::FLUSH_BARRIER, 1_500);
        add(keys::FLUSH_BARRIER_OK, 2);
        let snapshot = snapshot_json();
        let timings = snapshot["timings"].as_object().expect("timings object");
        let series = &timings[keys::FLUSH_BARRIER];
        assert_eq!(series["count"], 1);
        assert_eq!(series["totalUs"], 1_500);
        assert_eq!(series["maxUs"], 1_500);
        assert_eq!(series["lastUs"], 1_500);
        let counters = snapshot["counters"].as_object().expect("counters object");
        assert_eq!(counters[keys::FLUSH_BARRIER_OK], 2);
        // 未用过的词表键诚实缺席（不发明 0）。
        assert!(!timings.contains_key(keys::SEND_LOCK_WAIT));
        assert!(!counters.contains_key(keys::SEND_TEXT_OK));
        assert_eq!(snapshot["unknownKeyDropped"], 0);
    }

    #[test]
    fn series_aggregates_count_total_max_last() {
        reset_for_tests();
        record_us(keys::RECEIVE_ENVELOPE, 100);
        record_us(keys::RECEIVE_ENVELOPE, 900);
        record_us(keys::RECEIVE_ENVELOPE, 300);
        let snapshot = snapshot_json();
        let series = &snapshot["timings"][keys::RECEIVE_ENVELOPE];
        assert_eq!(series["count"], 3);
        assert_eq!(series["totalUs"], 1_300);
        assert_eq!(series["maxUs"], 900);
        assert_eq!(series["lastUs"], 300);
    }

    #[test]
    fn unknown_key_is_dropped_and_counted_not_stored() {
        reset_for_tests();
        record_us("rogue_metric", 1);
        add("rogue_metric", 1);
        let snapshot = snapshot_json();
        assert_eq!(snapshot["unknownKeyDropped"], 2);
        assert!(snapshot["timings"]
            .as_object()
            .expect("timings object")
            .is_empty());
    }

    #[test]
    fn snapshot_keys_are_deterministic_and_word_list_is_respected() {
        reset_for_tests();
        add(keys::STREAM_CONNECTED, 1);
        add(keys::STREAM_RECOVERABLE, 3);
        let snapshot = snapshot_json();
        let counters = snapshot["counters"].as_object().expect("counters object");
        let collected: Vec<&String> = counters.keys().collect();
        let mut sorted = collected.clone();
        sorted.sort();
        assert_eq!(collected, sorted, "BTreeMap 键序确定性");
        assert_eq!(counters.len(), 2);
    }

    /// send 分段打点批：六计时键全部在册（记录不落 unknownKeyDropped），
    /// 且 vendor 钩子键字面量与引擎词表跨 crate 钉等（单一真相在引擎，
    /// vendor 字面量漂移即此测试红）。
    #[test]
    fn send_segment_keys_are_registered_and_pinned_to_vendor_literals() {
        reset_for_tests();
        for key in [
            keys::SEND_RESOLVE,
            keys::SEND_SENDER_SETUP,
            keys::SEND_CERT_FETCH,
            keys::SEND_SESSION_ESTABLISH,
            keys::SEND_ENCRYPT,
            keys::SEND_WS_RTT,
        ] {
            record_us(key, 7);
        }
        let snapshot = snapshot_json();
        let timings = snapshot["timings"].as_object().expect("timings object");
        assert_eq!(timings.len(), 6, "六键全部落表");
        assert_eq!(snapshot["unknownKeyDropped"], 0, "在册键零丢弃");

        use presage::libsignal_service::perf_hook;
        assert_eq!(keys::SEND_SENDER_SETUP, perf_hook::keys::SENDER_SETUP);
        assert_eq!(keys::SEND_CERT_FETCH, perf_hook::keys::CERT_FETCH);
        assert_eq!(
            keys::SEND_SESSION_ESTABLISH,
            perf_hook::keys::SESSION_ESTABLISH
        );
        assert_eq!(keys::SEND_ENCRYPT, perf_hook::keys::ENCRYPT);
        assert_eq!(keys::SEND_WS_RTT, perf_hook::keys::WS_RTT);
    }

    /// 启动预热批：三计数键全部在册（累加不落 unknownKeyDropped，快照
    /// 可见——观测面唯一出口，失败静默纪律的计数承载）。
    #[test]
    fn send_warmup_counter_keys_are_registered_not_dropped() {
        reset_for_tests();
        add(keys::SEND_WARMUP_RUN, 1);
        add(keys::SEND_WARMUP_OK, 4);
        add(keys::SEND_WARMUP_ERR, 1);
        let snapshot = snapshot_json();
        let counters = snapshot["counters"].as_object().expect("counters object");
        assert_eq!(counters[keys::SEND_WARMUP_RUN], 1);
        assert_eq!(counters[keys::SEND_WARMUP_OK], 4);
        assert_eq!(counters[keys::SEND_WARMUP_ERR], 1);
        assert_eq!(snapshot["unknownKeyDropped"], 0);
    }
}
