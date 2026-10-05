// SPDX-License-Identifier: AGPL-3.0-only

//! transfer_archive 下载进度快照（契约 1.53 §4.38 扩展：`progress` 观测面）。
//!
//! 进程内存注册表（per-account），下载任务边读 CDN 响应边累计 bytes；引擎
//! 进程崩掉即清零——**不落盘不走文件**（§4.38 v1 同一诚实口径：快照缺席 =
//! 引擎拿不出进度，消费方维持无 progress 的状态投影，不猜）。
//!
//! 官方对齐（语义，AGPL 零拷贝）：Download 段有 currentBytes/totalBytes
//! 「X of Y」数字进度（Signal-Desktop `ts/state/ducks/installer.preload.ts`
//! L143-152 payload 形状 + reducer L642-675），content-length 拿不到时官方
//! 的 totalBytes 形状仍带数字——我们更保守：拿不到 total 只报 stage，不给
//! 假 0。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 单账号下载快照：累计读数字节 + 可选总字节。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransferSnapshot {
    pub bytes_read: u64,
    pub bytes_total: Option<u64>,
}

/// per-account 进程内注册表（引擎进程内存，非 Send 任务也可安全写——纯
/// std Mutex + Arc）。
#[derive(Clone, Default)]
pub(crate) struct TransferProgress {
    rows: Arc<Mutex<HashMap<String, TransferSnapshot>>>,
}

impl TransferProgress {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 注册/覆盖一次下载会话的总量（content-length 有则 Some，无则 None：
    /// 已在跑的会话 total 不被后写抹掉——只允许从 None 升级到 Some）。
    pub(crate) fn begin(&self, account: &str, bytes_total: Option<u64>) {
        let mut rows = self.rows.lock().expect("transfer progress lock");
        rows.entry(account.to_owned())
            .and_modify(|row| {
                if row.bytes_total.is_none() {
                    row.bytes_total = bytes_total;
                }
            })
            .or_insert_with(|| TransferSnapshot {
                bytes_read: 0,
                bytes_total,
            });
    }

    /// 累计一次块读（饱和加：进度观测面不因溢出炸任务）。
    pub(crate) fn add(&self, account: &str, bytes: u64) {
        let mut rows = self.rows.lock().expect("transfer progress lock");
        if let Some(row) = rows.get_mut(account) {
            row.bytes_read = row.bytes_read.saturating_add(bytes);
        }
    }

    /// 读当前快照；无会话（下载未开始/已结束）为 None。
    pub(crate) fn snapshot(&self, account: &str) -> Option<TransferSnapshot> {
        self.rows
            .lock()
            .expect("transfer progress lock")
            .get(account)
            .copied()
    }

    /// 下载会话收尾清快照（成功落盘或失败静默都走这，无泄漏）。
    pub(crate) fn clear(&self, account: &str) {
        self.rows
            .lock()
            .expect("transfer progress lock")
            .remove(account);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_and_reports_per_account() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", Some(100));
        progress.add("lk-a", 30);
        progress.add("lk-a", 20);
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                bytes_read: 50,
                bytes_total: Some(100),
            })
        );
        // 其他账号互不可见。
        assert_eq!(progress.snapshot("lk-b"), None);
    }

    #[test]
    fn missing_content_length_reports_none_total() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", None);
        progress.add("lk-a", 7);
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                bytes_read: 7,
                bytes_total: None,
            })
        );
    }

    #[test]
    fn clear_removes_the_session() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", Some(10));
        progress.clear("lk-a");
        assert_eq!(progress.snapshot("lk-a"), None);
        // 清后再 add 是无目标 no-op，不复活行。
        progress.add("lk-a", 5);
        assert_eq!(progress.snapshot("lk-a"), None);
    }

    #[test]
    fn total_only_upgrades_from_none() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", None);
        // 长轮询后的 CDN 响应晚于注册到达：None → Some 允许。
        progress.begin("lk-a", Some(64));
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                bytes_read: 0,
                bytes_total: Some(64),
            })
        );
        // 已有 total 不被覆盖。
        progress.begin("lk-a", Some(128));
        assert_eq!(progress.snapshot("lk-a").unwrap().bytes_total, Some(64));
    }

    #[test]
    fn saturates_on_overflow() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", None);
        progress.add("lk-a", u64::MAX);
        progress.add("lk-a", 1);
        assert_eq!(progress.snapshot("lk-a").unwrap().bytes_read, u64::MAX);
    }
}
