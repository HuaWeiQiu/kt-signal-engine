// SPDX-License-Identifier: AGPL-3.0-only

//! stdout 出站通道有界化（稳定性批，2026-10-20）。
//!
//! 此前 receive 通知与 JSON-RPC 响应共用一条无界 mpsc：单账号收流洪峰
//! （历史队列排空 / 群同步风暴）会把投影逐条堆进内存，连接器读得慢时
//! 无界增长；单账号也能把全局队列塞满，饿死其他账号的通知。
//!
//! 本模块两层限流：
//!
//! - **全局有界队列**（[`OUTBOUND_GLOBAL_CAPACITY`]）：writer 的唯一
//!   输入，响应与各账号通知共享；满了 `send().await` 等待 writer 排空
//!   （真背压，不丢帧、不改行序）。
//! - **每账号信号量**（[`OUTBOUND_ACCOUNT_CAPACITY`]）：每个账号最多
//!   N 帧在途（已进全局队列或正被 writer 写出）。许可挂在帧上，writer
//!   写完并 flush 后才释放——单账号洪峰在该账号的 `notify` 处背压，
//!   不会无限占全局队列；其他账号有独立许可，照常入队（不饿死）。
//!
//! `notify` 在帧进入全局队列后才返回，保持「通知先于后续响应入队」的
//! 既有行序语义。Flush 栅栏（时序审计 G1）与通知走同一全局 FIFO：此前
//! 已入队的帧必先于栅栏被 writer 写出并 flush，oneshot 解决后才算 ack
//! 安全。出口死亡（writer 退出 / 接收端丢弃）时 send 立即落错，等待中
//! 的 send/flush 不悬挂。

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::OutboundFrame;

/// 全局 writer 队列容量（响应 + 全部账号通知的在途合计上限）。
pub(crate) const OUTBOUND_GLOBAL_CAPACITY: usize = 256;

/// 单账号在途帧上限（信号量许可数；满即对该账号背压）。
pub(crate) const OUTBOUND_ACCOUNT_CAPACITY: usize = 64;

/// 出站 hub：全局 writer 队列 + 每账号在途许可注册表。
pub(crate) struct OutboundHub {
    global: mpsc::Sender<OutboundFrame>,
    accounts: StdMutex<HashMap<String, Arc<Semaphore>>>,
}

impl OutboundHub {
    pub(crate) fn new(global: mpsc::Sender<OutboundFrame>) -> Arc<Self> {
        Arc::new(Self {
            global,
            accounts: StdMutex::new(HashMap::new()),
        })
    }

    /// 全局帧（JSON-RPC 响应）：直接进全局有界队列；队列满时等待 writer
    /// 排空（背压），writer 已死返回 false。
    pub(crate) async fn send_global(&self, frame: OutboundFrame) -> bool {
        self.global.send(frame).await.is_ok()
    }

    /// 账号通知：先取该账号的在途许可（满则等待其他账号/本账号帧被
    /// writer 写出），再入全局有界队列。返回时帧已在全局队列中（行序
    /// 与既有实现一致）；出口已死返回 false。
    pub(crate) async fn notify(&self, account: &str, value: Value) -> bool {
        let Ok(permit) = self.account_semaphore(account).acquire_owned().await else {
            return false;
        };
        self.global
            .send(OutboundFrame::Notice {
                value,
                permit: Some(permit),
            })
            .await
            .is_ok()
    }

    /// 账号级 flush 栅栏：排在全局 FIFO 中此前通知之后，writer flush 完
    /// 全部前序帧才解决。出口已死 / 帧被弃置返回 false。
    pub(crate) async fn flush(&self, account: &str) -> bool {
        // 栅栏也走该账号许可：洪峰期间不额外挤占全局容量；许可与通知
        // 同一等待队列，栅栏不会被同账号后续帧插队。
        let Ok(permit) = self.account_semaphore(account).acquire_owned().await else {
            return false;
        };
        let (done, wait) = oneshot::channel();
        if self
            .global
            .send(OutboundFrame::Flush {
                done,
                permit: Some(permit),
            })
            .await
            .is_err()
        {
            return false;
        }
        wait.await.is_ok()
    }

    /// 出站 mailbox 观测快照（A1 per-account 观测，2026-10-20 稳定性批）：
    /// `globalDepth` = 全局 writer 队列当前占用（容量 - 可用容量）；
    /// `accounts` = 逐账号在途帧数（已占用的账号许可；writer 写完并 flush
    /// 才释放）。纯读数，不建信号量、不改队列状态。
    pub(crate) fn depth_json(&self) -> serde_json::Value {
        let global_capacity = self.global.max_capacity();
        let global_depth = global_capacity.saturating_sub(self.global.capacity());
        let accounts = {
            let accounts = self.accounts.lock().expect("outbound accounts lock");
            let mut rows = serde_json::Map::with_capacity(accounts.len());
            for (account, semaphore) in accounts.iter() {
                rows.insert(
                    account.clone(),
                    serde_json::json!(
                        OUTBOUND_ACCOUNT_CAPACITY.saturating_sub(semaphore.available_permits())
                    ),
                );
            }
            serde_json::Value::Object(rows)
        };
        serde_json::json!({
            "globalDepth": global_depth,
            "globalCapacity": global_capacity,
            "accounts": accounts,
        })
    }

    /// 取账号信号量（惰性建；账号数由账号表界定，条目随账号数有界）。
    fn account_semaphore(&self, account: &str) -> Arc<Semaphore> {
        let mut accounts = self.accounts.lock().expect("outbound accounts lock");
        Arc::clone(
            accounts
                .entry(account.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(OUTBOUND_ACCOUNT_CAPACITY))),
        )
    }
}

/// 单测装配：返回 hub + 全局接收端（测试按需断言 Notice 帧；不排空的
/// 用例受全局/账号容量约束，少量通知不会阻塞）。
#[cfg(test)]
pub(crate) fn test_hub() -> (Arc<OutboundHub>, mpsc::Receiver<OutboundFrame>) {
    let (tx, rx) = mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
    (OutboundHub::new(tx), rx)
}

#[cfg(test)]
mod tests {
    use super::{OutboundHub, OUTBOUND_ACCOUNT_CAPACITY, OUTBOUND_GLOBAL_CAPACITY};
    use crate::OutboundFrame;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    /// 洪峰下有界 + 另一账号仍能推进：A 账号持续灌入、消费者正常排空时，
    /// A 的 `notify` 受账号许可背压，B 的 `notify` 不被饿死且全部帧按序
    /// 送达。
    #[tokio::test]
    async fn flood_is_bounded_and_other_accounts_progress() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
        let hub = OutboundHub::new(tx);
        let drained = tokio::spawn(async move {
            let mut count = 0usize;
            while let Some(frame) = rx.recv().await {
                if matches!(frame, OutboundFrame::Notice { .. }) {
                    count += 1;
                }
            }
            count
        });

        let flood_hub = Arc::clone(&hub);
        let flood = tokio::spawn(async move {
            for i in 0..(OUTBOUND_ACCOUNT_CAPACITY * 8) {
                assert!(flood_hub.notify("a", json!({ "i": i })).await);
            }
        });

        // B 在 A 洪峰期间仍能入队/推进。
        let other = tokio::time::timeout(
            Duration::from_secs(2),
            hub.notify("b", json!({ "ok": true })),
        )
        .await
        .expect("other account must progress under flood");
        assert!(other);

        tokio::time::timeout(Duration::from_secs(5), flood)
            .await
            .expect("flood completes")
            .expect("flood task");
        drop(hub);
        let count = tokio::time::timeout(Duration::from_secs(5), drained)
            .await
            .expect("drain finishes")
            .expect("drain task");
        assert_eq!(count, OUTBOUND_ACCOUNT_CAPACITY * 8 + 1);
    }

    /// 无消费者时洪峰内存有界：单账号在途帧不超过账号许可数，灌入任务
    /// 被背压挂起而不是无限堆内存。
    #[tokio::test]
    async fn flood_memory_is_bounded_without_consumer() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
        let hub = OutboundHub::new(tx);
        let flood_hub = Arc::clone(&hub);
        let flood = tokio::spawn(async move {
            for i in 0..(OUTBOUND_GLOBAL_CAPACITY * 4) {
                let _ = flood_hub.notify("a", json!({ "i": i })).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!flood.is_finished(), "flood must be back-pressured");

        let mut retained = 0usize;
        while rx.try_recv().is_ok() {
            retained += 1;
        }
        assert!(retained > 0, "some frames are in flight");
        assert!(
            retained <= OUTBOUND_ACCOUNT_CAPACITY,
            "single-account retained {retained} exceeds account permit bound"
        );
        assert!(!flood.is_finished(), "flood still waiting after drain");

        // 弃置出口：洪峰任务立即落错退出，不悬挂。
        drop(rx);
        tokio::time::timeout(Duration::from_secs(2), flood)
            .await
            .expect("flood unblocks after outbound death")
            .expect("flood task");
    }

    /// flush 栅栏按全局 FIFO：先投递的通知必须先于 Flush 到 writer，
    /// writer 解决 oneshot 后 `flush` 返回 true。
    #[tokio::test]
    async fn flush_is_ordered_behind_prior_notices() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
        let hub = OutboundHub::new(tx);
        assert!(hub.notify("a", json!({ "n": 1 })).await);

        let flush_hub = Arc::clone(&hub);
        let flush = tokio::spawn(async move { flush_hub.flush("a").await });

        let first = rx.recv().await.expect("notice frame");
        let OutboundFrame::Notice { value, .. } = first else {
            panic!("notice must precede flush");
        };
        assert_eq!(value, json!({ "n": 1 }));
        let second = rx.recv().await.expect("flush frame");
        let OutboundFrame::Flush { done, .. } = second else {
            panic!("flush frame follows notices");
        };
        let _ = done.send(());
        assert!(tokio::time::timeout(Duration::from_secs(2), flush)
            .await
            .expect("flush resolves")
            .expect("flush task"));
    }

    /// A1 观测面：出站深度读数——账号在途 = 已占许可数，全局深度 = 队列
    /// 当前占用；接收端取走帧并析构后归零。纯读数不改队列。
    #[tokio::test]
    async fn depth_snapshot_reports_global_and_per_account_inflight() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
        let hub = OutboundHub::new(tx);

        let empty = hub.depth_json();
        assert_eq!(empty["globalDepth"], 0);
        assert_eq!(empty["globalCapacity"], OUTBOUND_GLOBAL_CAPACITY);
        assert!(empty["accounts"]
            .as_object()
            .expect("accounts object")
            .is_empty());

        assert!(hub.notify("a", json!({ "n": 1 })).await);
        assert!(hub.notify("a", json!({ "n": 2 })).await);
        assert!(hub.notify("b", json!({ "n": 3 })).await);
        let depth = hub.depth_json();
        assert_eq!(depth["globalDepth"], 3);
        assert_eq!(depth["accounts"]["a"], 2);
        assert_eq!(depth["accounts"]["b"], 1);

        // 取走并析构帧 → 许可释放。a 两帧 + b 一帧。
        for _ in 0..3 {
            drop(rx.recv().await.expect("frame"));
        }
        let depth = hub.depth_json();
        assert_eq!(depth["globalDepth"], 0);
        assert_eq!(depth["accounts"]["a"], 0);
        assert_eq!(depth["accounts"]["b"], 0);
    }

    /// 出口死亡（接收端丢弃）后 notify/flush 立即落错，不悬挂；重复
    /// notify 也不堆积任务。
    #[tokio::test]
    async fn dead_outbound_never_hangs() {
        let (tx, rx) = tokio::sync::mpsc::channel(OUTBOUND_GLOBAL_CAPACITY);
        drop(rx);
        let hub = OutboundHub::new(tx);

        for _ in 0..100 {
            tokio::time::timeout(Duration::from_secs(1), hub.notify("a", json!({ "x": 1 })))
                .await
                .expect("notify must not hang after outbound death");
        }
        let flushed = tokio::time::timeout(Duration::from_secs(1), hub.flush("a"))
            .await
            .expect("flush must not hang after outbound death");
        assert!(!flushed, "dead outbound flush must fail");
    }
}
