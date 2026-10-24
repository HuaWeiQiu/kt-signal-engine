// SPDX-License-Identifier: AGPL-3.0-only

//! per-account 观测面（稳定性批 2026-10-20，审计 §9.13.2 A1）。
//!
//! 目标词表（审计原文）：mailbox 深度 / 在飞 / p50·p95 / 接收延迟 / 重连
//! 次数。S1 actor-per-account 尚未落地，引擎没有「每账号 mailbox」这一实体
//! （全部账号共享 stdin 单通道与 LocalSet），因此本模块只投影**真实存在**的
//! 量，不发明队列：
//!
//! - `inflight`：该账号接收信封处理中（0/1；单任务顺序处理，同账号不会 >1）。
//! - `receiveP50Us` / `receiveP95Us` / `receiveMaxUs` / `receiveLastUs`：
//!   信封到达 → ack 完成（与既有 perf_metrics `receive_envelope` 同一起止，
//!   `[KT 适配]` 单位微秒；最近 [`RING_CAPACITY`] 个样本的最近秩分位）。
//! - `receiveLagP50Ms` / `receiveLagP95Ms`：信封 `server_timestamp` → 本地
//!   处理时刻（仅 Content / IdentityChange 臂有服务器时间戳；负偏移按 0 归，
//!   时钟偏移本身由既有 ClockSkewGate 独立告警）。
//! - `streamConnected` / `streamRecoverable` / `streamUnbound`：建流结局
//!   计数（接收链既有 `StreamOutcome` 四出口的逐账号投影）。
//! - `crashes`：supervisor 捕获的 panic 重启次数（[`super::supervision`]）。
//! - 出站 mailbox：`outbound.globalDepth`（全局 writer 队列当前深度）+
//!   `outbound.accounts.<account>`（该账号在途帧数，outbound 信号量已占
//!   许可数）——见 [`super::outbound::OutboundHub::depth_json`]。
//!
//! 暴露面：扩展现有 `engine.perfMetrics` 只读快照的 `accounts` / `outbound`
//! 两键（wire additive，不新增 JSON-RPC 方法）。快照键为账号明文（与
//! `listAccounts` 同级，wire 面既有形状）；日志面仍只允许不可逆短哈希。
//! 内存有界：账号数 × 两条定长环形缓冲，不随运行增长；进程内存不跨重启。
//!
//! 官方对应面：Desktop 单账号进程无 per-account 观测；本模块整体
//! `[KT 适配]`，指标语义与既有 perf_metrics 词表一致（微秒、诚实缺席）。

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Map, Value};

use super::outbound::OutboundHub;

/// 分位样本窗（每账号）。256 个样本足以覆盖最近数分钟的信封到达分布，
/// 内存上界 = 账号数 × 2 × 256 × 8B。
pub(crate) const RING_CAPACITY: usize = 256;

#[derive(Default)]
struct Series {
    count: u64,
    total: u64,
    max: u64,
    last: u64,
    ring: VecDeque<u64>,
}

impl Series {
    fn record(&mut self, value: u64) {
        self.count = self.count.saturating_add(1);
        self.total = self.total.saturating_add(value);
        self.max = self.max.max(value);
        self.last = value;
        if self.ring.len() == RING_CAPACITY {
            self.ring.pop_front();
        }
        self.ring.push_back(value);
    }

    /// 最近秩分位（nearest-rank）；空窗返回 None（诚实缺席）。
    fn percentile(&self, percent: u32) -> Option<u64> {
        if self.ring.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = self.ring.iter().copied().collect();
        sorted.sort_unstable();
        let rank = (u64::from(percent) * sorted.len() as u64)
            .div_ceil(100)
            .max(1)
            .min(sorted.len() as u64);
        sorted.get(rank as usize - 1).copied()
    }
}

#[derive(Default)]
struct AccountStat {
    receive_us: Series,
    receive_lag_ms: Series,
    inflight: u32,
    stream_connected: u64,
    stream_recoverable: u64,
    stream_unbound: u64,
    crashes: u64,
}

/// 建流结局分类（与 [`super::StreamOutcome`] 的观测三臂对应；StdioDead 是
/// 出口死亡不是账号健康量，不入本表）。
pub(crate) enum StreamEvent {
    Connected,
    Recoverable,
    Unbound,
}

fn registry() -> &'static Mutex<HashMap<String, AccountStat>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, AccountStat>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn with_account(account: &str, update: impl FnOnce(&mut AccountStat)) {
    let mut registry = registry().lock().expect("account metrics lock");
    update(registry.entry(account.to_owned()).or_default());
}

/// 信封到达 → ack 时长（微秒）。
pub(crate) fn record_receive(account: &str, us: u64) {
    with_account(account, |stat| stat.receive_us.record(us));
}

/// 服务器时间戳 → 本地处理时刻（毫秒；负偏移归 0）。
pub(crate) fn record_receive_lag(account: &str, lag_ms: u64) {
    with_account(account, |stat| stat.receive_lag_ms.record(lag_ms));
}

/// 建流结局计数（每账号）。
pub(crate) fn record_stream(account: &str, event: StreamEvent) {
    with_account(account, |stat| match event {
        StreamEvent::Connected => {
            stat.stream_connected = stat.stream_connected.saturating_add(1);
        }
        StreamEvent::Recoverable => {
            stat.stream_recoverable = stat.stream_recoverable.saturating_add(1);
        }
        StreamEvent::Unbound => {
            stat.stream_unbound = stat.stream_unbound.saturating_add(1);
        }
    });
}

/// supervisor 崩溃重启计数。
pub(crate) fn record_crash(account: &str) {
    with_account(account, |stat| {
        stat.crashes = stat.crashes.saturating_add(1);
    });
}

/// 接收处理中守卫：进入信封臂即置 1，离开（含提前 return）降回 0。
pub(crate) struct ReceiveInflight {
    account: String,
}

impl ReceiveInflight {
    pub(crate) fn enter(account: &str) -> Self {
        with_account(account, |stat| {
            stat.inflight = stat.inflight.saturating_add(1);
        });
        Self {
            account: account.to_owned(),
        }
    }
}

impl Drop for ReceiveInflight {
    fn drop(&mut self) {
        with_account(&self.account, |stat| {
            stat.inflight = stat.inflight.saturating_sub(1);
        });
    }
}

/// 账号从注册表摘除时清理观测行（delete/relink）。
pub(crate) fn remove_account(account: &str) {
    registry()
        .lock()
        .expect("account metrics lock")
        .remove(account);
}

fn series_json(series: &Series, p50_key: &str, p95_key: &str, unit: &str) -> Value {
    let mut row = Map::with_capacity(8);
    row.insert("count".to_owned(), json!(series.count));
    row.insert(format!("total{unit}"), json!(series.total));
    row.insert(format!("max{unit}"), json!(series.max));
    row.insert(format!("last{unit}"), json!(series.last));
    if let Some(p50) = series.percentile(50) {
        row.insert(p50_key.to_owned(), json!(p50));
    }
    if let Some(p95) = series.percentile(95) {
        row.insert(p95_key.to_owned(), json!(p95));
    }
    Value::Object(row)
}

fn account_json(stat: &AccountStat) -> Value {
    let mut row = Map::with_capacity(6);
    row.insert("inflight".to_owned(), json!(stat.inflight));
    row.insert(
        "receive".to_owned(),
        series_json(&stat.receive_us, "p50Us", "p95Us", "Us"),
    );
    row.insert(
        "receiveLag".to_owned(),
        series_json(&stat.receive_lag_ms, "p50Ms", "p95Ms", "Ms"),
    );
    row.insert("streamConnected".to_owned(), json!(stat.stream_connected));
    row.insert(
        "streamRecoverable".to_owned(),
        json!(stat.stream_recoverable),
    );
    row.insert("streamUnbound".to_owned(), json!(stat.stream_unbound));
    row.insert("crashes".to_owned(), json!(stat.crashes));
    Value::Object(row)
}

/// 把 per-account 观测并入既有 `engine.perfMetrics` 快照（两键 additive）：
/// `accounts`（账号 → 接收/重连/崩溃面）+ `outbound`（全局深度 + 逐账号在途）。
pub(crate) fn extend_snapshot(snapshot: &mut Value, outbound: &OutboundHub) {
    let accounts = {
        let registry = registry().lock().expect("account metrics lock");
        let mut rows = Map::with_capacity(registry.len());
        for (account, stat) in registry.iter() {
            rows.insert(account.clone(), account_json(stat));
        }
        Value::Object(rows)
    };
    if let Some(object) = snapshot.as_object_mut() {
        object.insert("accounts".to_owned(), accounts);
        object.insert("outbound".to_owned(), outbound.depth_json());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extend_snapshot, record_crash, record_receive, record_receive_lag, record_stream,
        remove_account, ReceiveInflight, StreamEvent, RING_CAPACITY,
    };
    use crate::service::outbound;
    use serde_json::json;

    #[test]
    fn percentiles_are_nearest_rank_and_series_totals_are_self_consistent() {
        for value in 1..=100u64 {
            record_receive("pct-acct", value);
        }
        let (hub, _rx) = outbound::test_hub();
        let mut snapshot = json!({});
        extend_snapshot(&mut snapshot, &hub);
        let receive = &snapshot["accounts"]["pct-acct"]["receive"];
        assert_eq!(receive["count"], 100);
        assert_eq!(receive["p50Us"], 50);
        assert_eq!(receive["p95Us"], 95);
        assert_eq!(receive["maxUs"], 100);
        assert_eq!(receive["lastUs"], 100);
        assert_eq!(receive["totalUs"], 5050);
    }

    #[test]
    fn ring_is_bounded_but_count_keeps_growing() {
        for value in 1..=(RING_CAPACITY as u64 * 4) {
            record_receive("ring-acct", value);
        }
        let (hub, _rx) = outbound::test_hub();
        let mut snapshot = json!({});
        extend_snapshot(&mut snapshot, &hub);
        let receive = &snapshot["accounts"]["ring-acct"]["receive"];
        assert_eq!(receive["count"], RING_CAPACITY as u64 * 4);
        // 最近 256 个样本 = 769..=1024 → p50 = 896、p95 = 1012。
        assert_eq!(receive["p50Us"], 896);
        assert_eq!(receive["p95Us"], 1012);
        assert_eq!(receive["maxUs"], RING_CAPACITY as u64 * 4);
    }

    #[test]
    fn accounts_are_isolated_and_absent_series_do_not_invent_values() {
        record_receive("iso-a", 10);
        record_receive("iso-b", 20);
        record_receive_lag("iso-a", 7);
        record_stream("iso-a", StreamEvent::Connected);
        record_stream("iso-b", StreamEvent::Recoverable);
        record_stream("iso-b", StreamEvent::Unbound);
        record_crash("iso-b");
        let (hub, _rx) = outbound::test_hub();
        let mut snapshot = json!({});
        extend_snapshot(&mut snapshot, &hub);

        assert_eq!(snapshot["accounts"]["iso-a"]["receive"]["lastUs"], 10);
        assert_eq!(snapshot["accounts"]["iso-b"]["receive"]["lastUs"], 20);
        assert_eq!(snapshot["accounts"]["iso-a"]["receiveLag"]["lastMs"], 7);
        assert_eq!(snapshot["accounts"]["iso-a"]["streamConnected"], 1);
        assert_eq!(snapshot["accounts"]["iso-b"]["streamRecoverable"], 1);
        assert_eq!(snapshot["accounts"]["iso-b"]["streamUnbound"], 1);
        assert_eq!(snapshot["accounts"]["iso-b"]["crashes"], 1);
        // 未采样的分位键诚实缺席（不投 0）。
        assert!(snapshot["accounts"]["iso-b"]["receiveLag"]
            .get("p50Ms")
            .is_none());

        remove_account("iso-a");
        let mut snapshot = json!({});
        extend_snapshot(&mut snapshot, &hub);
        assert!(snapshot["accounts"].get("iso-a").is_none());
        assert!(snapshot["accounts"].get("iso-b").is_some());
    }

    #[test]
    fn inflight_guard_returns_to_zero_on_drop() {
        let (hub, _rx) = outbound::test_hub();
        {
            let _guard = ReceiveInflight::enter("inflight-acct");
            let mut snapshot = json!({});
            extend_snapshot(&mut snapshot, &hub);
            assert_eq!(snapshot["accounts"]["inflight-acct"]["inflight"], 1);
        }
        let mut snapshot = json!({});
        extend_snapshot(&mut snapshot, &hub);
        assert_eq!(snapshot["accounts"]["inflight-acct"]["inflight"], 0);
    }
}
