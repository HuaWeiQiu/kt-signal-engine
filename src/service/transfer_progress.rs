// SPDX-License-Identifier: AGPL-3.0-only

//! transfer_archive 下载进度快照 + 下载任务取消句柄注册表（契约 1.53 §4.38
//! 扩展：`progress` 观测面；`history.importCancel` 取消面）。
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
//!
//! 失败显形（官方行为基准：失败带错误与重试，不静默傻等）：下载任务任何
//! 一步失败在快照上保留 `failed` 终态（短错误码，`transfer.downloadStatus`
//! 观测），「手机端没传历史」（204 重试耗尽）= `no_archive` 独立信息态；
//! 200 体官方契约还内嵌手机端主动通知两态（`{error:"RELINK_REQUESTED"}`
//! 要求重新扫码 / `{error:"CONTINUE_WITHOUT_UPLOAD"}` 放弃迁移继续使用），
//! 分别 = `relink_requested`/`continue_without_upload` 独立信息态，同型
//! 终态保留。终态清除触发器恰三：下次 begin（新会话）、
//! `history.importCancel` 取消、账号重链（finish_link）。快照缺席语义
//! 不变 = 无在途任务/无终态。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// 快照相位：在途（字节计数有效）。
pub(crate) const PHASE_DOWNLOADING: &str = "downloading";
/// 快照相位：终态失败（error_code 结构化透出，快照保留至清除触发器）。
pub(crate) const PHASE_FAILED: &str = "failed";

/// 下载失败短码词表（wire `errorCode`；短码、不含消息内容/路径/敏感信息
/// ——引擎仓日志纪律同款红线）。
pub(crate) mod failure {
    /// 204 重试耗尽 = 手机端没有可传档案（独立信息态，非故障）。
    pub(crate) const NO_ARCHIVE: &str = "no_archive";
    /// transfer_archive 长轮询面传输失败（请求构建/发送错误、非 200/204 状态）。
    pub(crate) const NETWORK: &str = "network";
    /// CDN 下载段失败（请求构建/发送错误、非 2xx 状态、流中断）。
    pub(crate) const CDN_UNREACHABLE: &str = "cdn_unreachable";
    /// 档案解不开：manifest 非法 JSON/形状不符（无 cdn+key 且 error 非官方
    /// 已知码）、解密/分帧导出失败。
    pub(crate) const DECODE_FAILED: &str = "decode_failed";
    /// 本地文件面失败（档案落盘/NDJSON 建文件）。
    pub(crate) const IO: &str = "io";
    /// 手机端主动要求重新扫码迁移（200 体 `{error:"RELINK_REQUESTED"}`，原
    /// 链接作废）：独立信息态非故障，终态保留供桌面诚实呈现（同 no_archive
    /// 型）。官方契约：Signal-Desktop WebAPI.preload.ts TransferArchiveSchema
    /// union，消费见 backups/index.preload.ts（RelinkRequestedError）。
    pub(crate) const RELINK_REQUESTED: &str = "relink_requested";
    /// 手机端主动放弃迁移继续使用（200 体
    /// `{error:"CONTINUE_WITHOUT_UPLOAD"}`，无档案可拉）：独立信息态非故障，
    /// 终态保留供桌面诚实呈现（同 no_archive 型）。官方契约同上。
    pub(crate) const CONTINUE_WITHOUT_UPLOAD: &str = "continue_without_upload";
}

/// 单账号下载快照：相位 + 累计读数字节 + 可选总字节 + 终态失败短码。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TransferSnapshot {
    pub phase: &'static str,
    pub bytes_read: u64,
    pub bytes_total: Option<u64>,
    pub error_code: Option<&'static str>,
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

    /// 注册/覆盖一次下载会话的总量（content-length 有则 Some，无则 None）。
    /// 会话语义：在途行（phase=downloading）只允许 total 从 None 升级到
    /// Some（长轮询后的 CDN 响应晚于注册到达），字节计数不被后写抹掉；
    /// 终态失败行被新会话整体重置（失败终态清除触发器之一：begin）。
    pub(crate) fn begin(&self, account: &str, bytes_total: Option<u64>) {
        let mut rows = self.rows.lock().expect("transfer progress lock");
        match rows.get_mut(account) {
            Some(row) if row.phase == PHASE_DOWNLOADING => {
                if row.bytes_total.is_none() {
                    row.bytes_total = bytes_total;
                }
            }
            _ => {
                rows.insert(
                    account.to_owned(),
                    TransferSnapshot {
                        phase: PHASE_DOWNLOADING,
                        bytes_read: 0,
                        bytes_total,
                        error_code: None,
                    },
                );
            }
        }
    }

    /// 累计一次块读（饱和加：进度观测面不因溢出炸任务）。只在在途行上
    /// 生效——终态行与缺席行都是无目标 no-op（取消后的迟到块读不复活行、
    /// 不污染终态字节计数）。
    pub(crate) fn add(&self, account: &str, bytes: u64) {
        let mut rows = self.rows.lock().expect("transfer progress lock");
        if let Some(row) = rows.get_mut(account) {
            if row.phase == PHASE_DOWNLOADING {
                row.bytes_read = row.bytes_read.saturating_add(bytes);
            }
        }
    }

    /// 终态失败：保留已计字节（部分进度如实），翻相位 + 短错误码。无快照
    /// （失败发生在 begin 前/清快照后，如导出段失败）也立 failed 终态——
    /// 失败显形不依赖字节段曾开始。
    pub(crate) fn fail(&self, account: &str, code: &'static str) {
        let mut rows = self.rows.lock().expect("transfer progress lock");
        let row = rows.entry(account.to_owned()).or_insert(TransferSnapshot {
            phase: PHASE_FAILED,
            bytes_read: 0,
            bytes_total: None,
            error_code: None,
        });
        row.phase = PHASE_FAILED;
        row.error_code = Some(code);
    }

    /// 读当前快照；无会话（未开始/已成功收尾/终态已被清除）为 None。
    pub(crate) fn snapshot(&self, account: &str) -> Option<TransferSnapshot> {
        self.rows
            .lock()
            .expect("transfer progress lock")
            .get(account)
            .copied()
    }

    /// 会话收尾清快照（成功落盘或终态被取消/重链/begin 覆盖，无泄漏）。
    pub(crate) fn clear(&self, account: &str) {
        self.rows
            .lock()
            .expect("transfer progress lock")
            .remove(account);
    }
}

/// transfer_archive 下载任务句柄注册表（per-account 单槽；`history.importCancel`
/// 取消面的引擎侧机制）。任务经 `spawn_local` 上引擎单线程 LocalSet，取消 =
/// `JoinHandle::abort`：任务在下一个 await 点（长轮询挂起/CDN 块读/帧导出）
/// 即弃，在途 HTTP 流随 future 一起断；abort 返回后任务不会再执行任何指令
/// （单线程运行时，dispatch 与任务互斥调度）——调用侧随后的半成品清理与
/// 任务内写盘天然无竞争。
#[derive(Clone, Default)]
pub(crate) struct TransferDownloadRegistry {
    rows: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
}

impl TransferDownloadRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 登记在途任务句柄（同账号重链覆盖旧句柄——旧任务收尾/被取消后槽位
    /// 归新会话所有）。
    pub(crate) fn register(&self, account: &str, handle: tokio::task::JoinHandle<()>) {
        self.rows
            .lock()
            .expect("transfer download registry lock")
            .insert(account.to_owned(), handle);
    }

    /// 取消在途任务；`true` = 确实中止了在途任务。句柄缺席或已收尾
    /// （自然完成/此前已被取消）= `false`——幂等，不假装取消了什么。
    pub(crate) fn cancel(&self, account: &str) -> bool {
        let handle = self
            .rows
            .lock()
            .expect("transfer download registry lock")
            .remove(account);
        match handle {
            Some(handle) if !handle.is_finished() => {
                handle.abort();
                true
            }
            _ => false,
        }
    }
}

/// 取消/失败侧半成品清理（transfer 腿）：下载档案与半途 NDJSON（导出段
/// 被 abort 打断时 BufWriter 已冲刷的部分帧）。best-effort，不产生 stderr
/// （compat §1）；账号目录不存在（fake/已删除）同样安静。
pub(crate) fn remove_transfer_artifacts(account_dir: &std::path::Path) {
    for name in ["transfer-archive.bin", "history-import.ndjson"] {
        let _ = std::fs::remove_file(account_dir.join(name));
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
                phase: PHASE_DOWNLOADING,
                bytes_read: 50,
                bytes_total: Some(100),
                error_code: None,
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
                phase: PHASE_DOWNLOADING,
                bytes_read: 7,
                bytes_total: None,
                error_code: None,
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
    fn total_only_upgrades_from_none_while_in_flight() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", None);
        // 长轮询后的 CDN 响应晚于注册到达：None → Some 允许。
        progress.begin("lk-a", Some(64));
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                phase: PHASE_DOWNLOADING,
                bytes_read: 0,
                bytes_total: Some(64),
                error_code: None,
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

    /// 失败显形：终态保留已计字节 + 短码；字节段未开始的失败（如长轮询
    /// 面失败）也立 failed 终态。
    #[test]
    fn fail_keeps_partial_bytes_and_surfaces_code() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", Some(100));
        progress.add("lk-a", 40);
        progress.fail("lk-a", failure::CDN_UNREACHABLE);
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                phase: PHASE_FAILED,
                bytes_read: 40,
                bytes_total: Some(100),
                error_code: Some(failure::CDN_UNREACHABLE),
            })
        );
        // 无快照的失败（凭据/长轮询早期）同样立终态。
        progress.clear("lk-a");
        progress.fail("lk-b", failure::NETWORK);
        assert_eq!(
            progress.snapshot("lk-b"),
            Some(TransferSnapshot {
                phase: PHASE_FAILED,
                bytes_read: 0,
                bytes_total: None,
                error_code: Some(failure::NETWORK),
            })
        );
    }

    /// 终态清除触发器①：新会话 begin 重置失败终态（重链/重跑同账号）。
    #[test]
    fn begin_resets_failed_terminal() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", Some(100));
        progress.add("lk-a", 40);
        progress.fail("lk-a", failure::NO_ARCHIVE);
        // 新会话：整体重置（字节归零、相位翻回在途、短码清除）。
        progress.begin("lk-a", None);
        assert_eq!(
            progress.snapshot("lk-a"),
            Some(TransferSnapshot {
                phase: PHASE_DOWNLOADING,
                bytes_read: 0,
                bytes_total: None,
                error_code: None,
            })
        );
    }

    /// 终态行冻结：add 不污染终态字节计数，begin 不覆盖终态 total——
    /// 只有整体重置（begin 新会话）或 clear（取消/重链）能动终态。
    #[test]
    fn terminal_row_is_frozen_against_add() {
        let progress = TransferProgress::new();
        progress.begin("lk-a", None);
        progress.add("lk-a", 40);
        progress.fail("lk-a", failure::IO);
        progress.add("lk-a", 60);
        assert_eq!(progress.snapshot("lk-a").unwrap().bytes_read, 40);
    }

    /// 下载任务取消句柄注册表：在途任务 abort 命中（true）、幂等再取消
    /// （false）、已收尾任务不假装取消（false）、无登记账号（false）。
    #[test]
    fn registry_cancel_semantics() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
            .block_on(tokio::task::LocalSet::new().run_until(async {
                let registry = TransferDownloadRegistry::new();
                // 无登记：幂等 false。
                assert!(!registry.cancel("lk-a"));

                // 在途任务（parked 在 await 上，模拟长轮询挂起/CDN 块读）。
                let (tx, rx) = tokio::sync::watch::channel(false);
                let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let flag = Arc::clone(&aborted);
                let handle = tokio::task::spawn_local(async move {
                    let mut rx = rx;
                    loop {
                        tokio::select! {
                            _ = tx.closed() => break,
                            _ = rx.changed() => break,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                        }
                    }
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                });
                registry.register("lk-a", handle);
                assert!(registry.cancel("lk-a"), "在途任务取消命中");
                // abort 后任务在下一个 await 点即弃：任务体收尾代码不再执行
                // （单线程 LocalSet 语义）——这正是取消侧清理不与任务竞争的
                // 依据；句柄收尾为 cancelled。
                assert!(!registry.cancel("lk-a"), "二次取消幂等 = false");
                assert!(
                    !aborted.load(std::sync::atomic::Ordering::SeqCst),
                    "abort 即弃：任务体不跑到收尾"
                );

                // 已收尾任务：cancel 不假装取消了什么。
                let handle = tokio::task::spawn_local(async {});
                registry.register("lk-b", handle);
                tokio::task::yield_now().await;
                assert!(!registry.cancel("lk-b"), "已收尾任务取消 = false");
            }));
    }

    /// 半成品清理：档案与半途 NDJSON 双删，best-effort 不因缺席炸。
    #[test]
    fn artifact_cleanup_removes_archive_and_ndjson() {
        let dir =
            std::env::temp_dir().join(format!("kt-engine-transfer-cancel-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("transfer-archive.bin"), b"cipher").expect("archive");
        std::fs::write(dir.join("history-import.ndjson"), b"{\"partial\"").expect("ndjson");
        std::fs::write(dir.join("keep-me"), b"untouched").expect("other file");
        remove_transfer_artifacts(&dir);
        assert!(!dir.join("transfer-archive.bin").exists());
        assert!(!dir.join("history-import.ndjson").exists());
        assert!(dir.join("keep-me").exists(), "只清导入产物，不动其他文件");
        // 幂等：缺席再清不炸。
        remove_transfer_artifacts(&dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
