// SPDX-License-Identifier: AGPL-3.0-only

//! 启动预热批（send 性能批 2026-10-16，docs/jsonrpc-compat.md §5「启动
//! 预热」块）：通道 ready 后（profile 名字刷新批之后同一沉降带）对近活跃
//! 会话做一次发送路径预热——双 WS 装配 + 发送证书缓存 + 近活跃收件人冷
//! 会话预建立，让进程内首批真实发送踩热路径而不是付冷启动税。
//!
//! 纪律（docs 契约面）：
//! - 零真实消息：预热只做 `new_message_sender`（WS 复用/拨号）、
//!   `sender_certificate`（缓存）与 `ensure_session`（prekey 拉取 + 本地
//!   建会话），不上行任何 wire 消息信封。
//! - 不阻塞真实发送：跑在既有 sync 臂 spawn_local 沉降任务内，独立
//!   Manager（store 连接隔离，websocket 槽进程级归一共享），不取
//!   `lock_registered_for_send` 常驻发送句柄。
//! - 有界：近活跃 thread ≤[`WARMUP_RECENT_THREADS`]、扇出去重帽
//!   [`WARMUP_MAX_TARGETS`]、并发 ≤[`WARMUP_CONCURRENCY`]（独立 manager
//!   数）、整批 ≤[`WARMUP_BATCH_DEADLINE_MS`]（超时整批目标计 err——
//!   诚实语义）。
//! - 失败静默：零 stderr，perf 计数面（SEND_WARMUP_RUN/OK/ERR）承载
//!   观测；单收件人失败不中断批次。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use presage::libsignal_service::protocol::ServiceId;
use presage::manager::{Manager, Registered};
use presage::store::{ContentsStore as _, StateStore as _, Thread};
use presage_store_sqlite::SqliteStore;

use super::{open_store, perf_metrics, profile_refresh, storage_sync, Service};

/// 近活跃 thread 数（按最后一条消息 ts 新序取前 N；直聊 + 群混排）。
pub(crate) const WARMUP_RECENT_THREADS: usize = 5;
/// 并发上界（独立 manager 数 = store 连接数；websocket 槽进程级归一，
/// 同 profile_refresh 纪律注）。
pub(crate) const WARMUP_CONCURRENCY: usize = 2;
/// 整批时限（毫秒）：到期未确认的目标整体计 err（诚实语义：预热批未使
/// 该收件人变热；在途块允许被丢弃——超时即放弃，不悬挂）。
pub(crate) const WARMUP_BATCH_DEADLINE_MS: u64 = 10_000;
/// 扇出去重后的目标帽（群成员扇出的爆炸半径上界）。
pub(crate) const WARMUP_MAX_TARGETS: usize = 32;

/// 目标收集（纯函数，测试钉形）：近活跃 thread → 收件人扇出——直聊取
/// 对端 ACI、群取全体成员 ACI；跳自身；Vec contains 去重保序（首现序 =
/// thread 新序 × 成员序，去重不用 HashSet——ServiceId 的 Hash 界不进
/// 引擎假设面）；截帽。
pub(crate) fn warm_targets_from_threads(
    threads: &[(Thread, u64)],
    mut members_of_group: impl FnMut(&[u8; 32]) -> Vec<ServiceId>,
    own: ServiceId,
    cap: usize,
) -> Vec<ServiceId> {
    let mut targets: Vec<ServiceId> = Vec::new();
    for (thread, _ts) in threads {
        match thread {
            Thread::Contact(service_id) => {
                if *service_id != own && !targets.contains(service_id) {
                    targets.push(*service_id);
                }
            }
            Thread::Group(key) => {
                for member in members_of_group(key) {
                    if member != own && !targets.contains(&member) {
                        targets.push(member);
                    }
                }
            }
        }
    }
    targets.truncate(cap);
    targets
}

/// 预热单点（sync 臂沉降带调用，60s 一次性纪律继承）：目标收集（本地读
/// 零网络）→ 有界并发预热（每块独立 Manager + `warm_send_path`）→ 计数
/// 聚合（RUN 恒 +1；OK = established + existing；ERR = failed；超时 =
/// 整批目标数）。best-effort：store/注册数据不可读即静默返回（RUN 已
/// 记尝试，OK/ERR 同缺席诚实）。
pub(crate) async fn warm_after_profile_refresh(service: &Arc<Service>, account: &str) {
    if !service.real {
        return;
    }
    perf_metrics::add(perf_metrics::keys::SEND_WARMUP_RUN, 1);
    let dir = {
        let state = service.state.lock().await;
        let Some(rel) = state.account_dir(account) else {
            return;
        };
        service.data_dir.join(rel)
    };
    let Some(targets) = collect_warm_targets(&dir).await else {
        return;
    };
    if targets.is_empty() {
        return;
    }
    let total = targets.len() as u64;
    let chunks = profile_refresh::chunk_targets(targets, WARMUP_CONCURRENCY);
    let batch = futures::future::join_all(chunks.iter().map(|chunk| {
        let dir = &dir;
        async move { warm_chunk(dir, chunk).await }
    }));
    match tokio::time::timeout(Duration::from_millis(WARMUP_BATCH_DEADLINE_MS), batch).await {
        Ok(partials) => {
            let mut ok = 0;
            let mut err = 0;
            for (chunk_ok, chunk_err) in partials {
                ok += chunk_ok;
                err += chunk_err;
            }
            perf_metrics::add(perf_metrics::keys::SEND_WARMUP_OK, ok);
            perf_metrics::add(perf_metrics::keys::SEND_WARMUP_ERR, err);
        }
        Err(_elapsed) => {
            perf_metrics::add(perf_metrics::keys::SEND_WARMUP_ERR, total);
        }
    }
}

/// 目标收集：注册数据取自身 ACI + 近活跃 thread 读（ts 新序）+ 群成员
/// 扇出（本地缓存读，失败该群空扇出——预热 best-effort 不炸目标面）。
async fn collect_warm_targets(dir: &Path) -> Option<Vec<ServiceId>> {
    let store = open_store(dir).await.ok()?;
    let own = ServiceId::Aci(
        store
            .load_registration_data()
            .await
            .ok()
            .flatten()?
            .service_ids
            .aci
            .into(),
    );
    let recent = store.recent_threads(WARMUP_RECENT_THREADS).await.ok()?;
    // 纯函数收扇出闭包（同步面），群成员材料先 await 读齐（同 store
    // 连接，本地读零网络；key → 成员表保序对齐）。
    let mut members_by_key: Vec<([u8; 32], Vec<ServiceId>)> = Vec::new();
    for (thread, _ts) in &recent {
        if let Thread::Group(key) = thread {
            let members = store
                .group(*key)
                .await
                .ok()
                .flatten()
                .map(|group| {
                    group
                        .members
                        .into_iter()
                        .map(|member| ServiceId::from(member.aci))
                        .collect()
                })
                .unwrap_or_default();
            members_by_key.push((*key, members));
        }
    }
    let targets = warm_targets_from_threads(
        &recent,
        |key| {
            members_by_key
                .iter()
                .find(|(stored, _)| stored == key)
                .map(|(_, members)| members.clone())
                .unwrap_or_default()
        },
        own,
        WARMUP_MAX_TARGETS,
    );
    drop(store);
    Some(targets)
}

/// 单块预热：独立 store 连接 + Manager（websocket 槽进程级归一共享），
/// `warm_send_path` 一次吃整块。块级基础设施失败（store 打不开/注册数据
/// 不可读）= 该块全部目标计 err（诚实：未确认变热）。
async fn warm_chunk(dir: &Path, targets: &[ServiceId]) -> (u64, u64) {
    if targets.is_empty() {
        return (0, 0);
    }
    let failed = (0, targets.len() as u64);
    let store = match open_store(dir).await {
        Ok(store) => store,
        Err(()) => return failed,
    };
    let Ok(mut manager) = Manager::<SqliteStore, Registered>::load_registered(store).await else {
        return failed;
    };
    match manager.warm_send_path(targets).await {
        Ok(stats) => (
            (stats.established + stats.existing) as u64,
            stats.failed as u64,
        ),
        Err(_) => failed,
    }
}

/// whitelist 冷启动预热（发送长尾修3，2026-10-17）：账号 sync 臂沉降带
/// （60s 一次性纪律继承）后台 best-effort 回填 profileSharing 闸位 map
/// 缓存。探针实证（restart26-28，2026-10-17）：TTL 过期/进程冷启动后的
/// 首条 send 付 ~1.8s Storage Service 全量拉取（engine.whitelist
/// cache=miss），首批真实发送踩冷缓存；本预热在真实发送前提前回填，让
/// 首条 send 的 `engine.whitelist` 段踩 hit（dur_ms≈0）。
///
/// 语义面（与发送路径读面 [`super::profile_sharing_whitelist_probed`]
/// 逐字同源，仅多出失败不毒缓存一位）：
/// - 已有缓存（窗内真实发送先行回填）→ 零网络零动作，cache=hit；
/// - storage 上下文可得 → `storage_sync::contact_whitelist_map` 全量读
///   经 [`super::whitelist_cache_put`] 单点回填（TTL/写面失效四处挂载
///   与发送路径完全同一语义），cache=filled；
/// - storage 上下文不可得 → **不回填**（保持冷态，下条真实发送按既有
///   路径重试；发送路径的「错误空 map 同缓存」语义在此不复制——预热
///   是锦上添花，失败不得毒缓存压后续 600s），cache=err。
///
/// 全失败面静默不上抛（零 stderr 纪律），观测只走本探针行；fake 模式
/// 早退（与 [`warm_after_profile_refresh`] 同姿态）。正文/号码不入日志，
/// 账号仅前 8 字符前缀（probe_account8 单点）。
pub(crate) async fn warm_whitelist_cache(service: &Arc<Service>, account: &str) {
    if !service.real {
        return;
    }
    let probe_t0 = std::time::Instant::now();
    let cache_word = if super::whitelist_cache_get(&service.whitelist_cache, account).is_some() {
        "hit"
    } else {
        match super::storage_context(service, account).await {
            Ok(context) => {
                let map = storage_sync::contact_whitelist_map(&context).await;
                super::whitelist_cache_put(&service.whitelist_cache, account, map);
                "filled"
            }
            Err(_) => "err",
        }
    };
    tracing::warn!(
        target: "send_latency_probe",
        stage = "engine.whitelist-warmup",
        dur_ms = probe_t0.elapsed().as_millis() as u64,
        account = %crate::methods::probe_account8(Some(account)),
        cache = cache_word,
        "send-latency-probe",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::prelude::Uuid;
    use presage::libsignal_service::protocol::Aci;

    fn aci(n: u32) -> ServiceId {
        ServiceId::Aci(Aci::from(Uuid::from_u128(n as u128)))
    }

    fn key(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    /// 空表 → 零目标（无近活跃会话时预热批空转零开销）。
    #[test]
    fn empty_threads_yield_no_targets() {
        let targets = warm_targets_from_threads(&[], |_| Vec::new(), aci(1), 32);
        assert!(targets.is_empty());
    }

    /// 直聊 → 对端 ACI 单目标；自身 thread（给自己发的消息）跳过。
    #[test]
    fn direct_threads_take_peer_and_skip_self() {
        let threads = vec![
            (Thread::Contact(aci(2)), 100),
            (Thread::Contact(aci(1)), 200),
        ];
        let targets = warm_targets_from_threads(&threads, |_| Vec::new(), aci(1), 32);
        assert_eq!(targets, vec![aci(2)]);
    }

    /// 群扇出：全体成员 ACI（跳自身）；群读失败（空扇出）不炸目标面。
    #[test]
    fn group_threads_fan_out_members() {
        let threads = vec![(Thread::Group(key(7)), 100)];
        let targets = warm_targets_from_threads(
            &threads,
            |k| {
                assert_eq!(k, &key(7));
                vec![aci(3), aci(1), aci(4)]
            },
            aci(1),
            32,
        );
        assert_eq!(targets, vec![aci(3), aci(4)]);
        let targets = warm_targets_from_threads(&threads, |_| Vec::new(), aci(1), 32);
        assert!(targets.is_empty(), "群读失败 = 空扇出");
    }

    /// 去重保序：跨 thread 重复收件人（直聊×直聊、直聊×群成员）只出一
    /// 次，首现序 = thread 新序 × 成员序。
    #[test]
    fn targets_are_deduped_preserving_first_seen_order() {
        let threads = vec![
            (Thread::Contact(aci(2)), 300),
            (Thread::Group(key(7)), 200),
            (Thread::Contact(aci(3)), 100),
        ];
        let targets = warm_targets_from_threads(
            &threads,
            |k| {
                assert_eq!(k, &key(7));
                vec![aci(3), aci(5)]
            },
            aci(1),
            32,
        );
        assert_eq!(
            targets,
            vec![aci(2), aci(3), aci(5)],
            "首现序：直聊2 → 群成员3、5 → 直聊3 与群成员重复被跳过"
        );
    }

    /// 截帽：目标数超帽截首现序前 cap 个（扇出爆炸半径上界）。
    #[test]
    fn targets_are_capped() {
        let threads = vec![(Thread::Contact(aci(2)), 300), (Thread::Group(key(7)), 200)];
        let targets =
            warm_targets_from_threads(&threads, |_| vec![aci(3), aci(4), aci(5)], aci(1), 3);
        assert_eq!(targets, vec![aci(2), aci(3), aci(4)]);
    }
}
