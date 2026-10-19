// SPDX-License-Identifier: AGPL-3.0-only

//! 每账号接收管线 supervisor（稳定性批 2026-10-20，审计 §9.13.2 S4/A2）。
//!
//! 现状前提：S1 actor-per-account 未落地，全部账号共享同一 LocalSet 线程；
//! 单账号接收任务 panic 时 tokio 只收口该任务，但原实现丢弃 JoinHandle
//! （`spawn_pipeline`），该账号从此静默不再接收，无重启、无计数。
//!
//! 本模块把「单账号任务」包成可重启的监督环：
//!
//! - **panic 隔离**：worker future 以 `catch_unwind` 包裹，panic 只收口本次
//!   运行，其余账号任务与进程继续（进程级 abort 语义不在本层，`panic=abort`
//!   时由部署层兜底）。
//! - **崩溃退避**：每次 panic 计一次账号级崩溃，复用接收链既有 Fibonacci
//!   序列 + jitter（[`super::reconnect_backoff_delay`] /
//!   [`super::reconnect_jittered_delay`]，官方 Desktop `ts/util/BackOff.std.ts`
//!   `FIBONACCI`/`get()` 同语义）：1,1,2,3,5,8,13,21,34,55s 封顶。
//! - **恢复清零**：一次运行存活 ≥ [`CRASH_RESET_UPTIME`] 视为已恢复，清零
//!   退避步进（官方 `BackOff.reset()` 在链路恢复时调用）。
//!
//! 官方多账号对应面：无（Desktop 单进程单账号，`SocketManager` 单连接槽）。
//! per-account supervisor 是 `[KT 适配]`；退避序列与 jitter 语义严格复用官方
//! BackOff 行为，不发明新节奏。
//!
//! 日志纪律（compat §1）：本模块不落 panic payload、账号号码、正文或密钥；
//! 账号只以不可逆短哈希出现在调用方日志行（`methods::probe_account8`）。

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt as _;
use tokio::sync::watch;

#[cfg(test)]
use super::reconnect_backoff_delay;
use super::reconnect_jittered_delay;

/// 一次运行存活达到该时长即视为恢复，崩溃退避清零。取值参考官方
/// `BackOff.reset()`「链路成功建立即清零」语义：健康接收流不会在 30s 内
/// 反复 panic；短命重启仍按崩溃循环逐级退避。
pub(crate) const CRASH_RESET_UPTIME: Duration = Duration::from_secs(30);

/// 账号级崩溃退避（进程内、随接收管线任务生命周期；不落盘不跨重启）。
#[derive(Debug, Default)]
pub(crate) struct AccountCrashBackoff {
    crashes: u32,
    step: u32,
}

impl AccountCrashBackoff {
    /// 记一次崩溃并返回本次重启等待（Fibonacci 基值 + 既有 jitter）。
    #[cfg(test)]
    pub(crate) fn on_crash(&mut self) -> Duration {
        self.on_crash_with(reconnect_backoff_delay)
    }

    /// 同 `on_crash`，退避基值由调用方注入（监督环测试用短拍）。
    pub(crate) fn on_crash_with(&mut self, delay_of: impl Fn(u32) -> Duration) -> Duration {
        self.crashes = self.crashes.saturating_add(1);
        let delay = reconnect_jittered_delay(delay_of(self.step));
        self.step = self.step.saturating_add(1);
        delay
    }

    /// 链路恢复 / 运行足够久：退避从首拍重来（崩溃累计计数保留，作观测面）。
    pub(crate) fn on_recovered(&mut self) {
        self.step = 0;
    }

    /// 累计崩溃次数（饱和计数，只增不清——观测面用）。
    pub(crate) fn crashes(&self) -> u32 {
        self.crashes
    }

    /// 当前退避步进（0 = 下一次崩溃用首拍）。
    pub(crate) fn step(&self) -> u32 {
        self.step
    }
}

/// 监督一个账号任务：正常返回即停；panic 按账号级退避重启。
///
/// - `worker` 每次运行重新构造 future（panic 后运行态状态机重置；流内重连
///   仍由 worker 自身处理，不触发重启）。
/// - `delay_of(step)` 便于测试注入短退避；生产传 `reconnect_backoff_delay`。
/// - `on_crash(crashes, step, delay)` 供调用方落结构化日志/指标，入参不含
///   账号明文与 panic payload。
///
/// 返回最终退避状态（观测/测试用；生产调用方忽略）。
pub(crate) async fn run_account_supervisor<W, Fut, D, C>(
    mut shutdown: watch::Receiver<bool>,
    mut backoff: AccountCrashBackoff,
    mut worker: W,
    delay_of: D,
    mut on_crash: C,
) -> AccountCrashBackoff
where
    W: FnMut(watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = ()>,
    D: Fn(u32) -> Duration,
    C: FnMut(u32, u32, Duration),
{
    loop {
        if *shutdown.borrow() {
            return backoff;
        }
        let run_started = Instant::now();
        let run_shutdown = shutdown.clone();
        // AssertUnwindSafe：worker 持有 presage 非 Send 状态与跨 await 的可变
        // 引用；panic 后这些状态整体丢弃，重启从干净状态机开始。
        let caught = AssertUnwindSafe(worker(run_shutdown)).catch_unwind().await;
        if caught.is_ok() {
            // 正常收口（Unbound / StdioDead / 停机旗）：保持既有停环语义。
            return backoff;
        }
        if *shutdown.borrow() {
            return backoff;
        }
        if run_started.elapsed() >= CRASH_RESET_UPTIME {
            backoff.on_recovered();
        }
        let delay = backoff.on_crash_with(&delay_of);
        let (crashes, step) = (backoff.crashes(), backoff.step().saturating_sub(1));
        on_crash(crashes, step, delay);
        tokio::select! {
            _ = shutdown.changed() => return backoff,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AccountCrashBackoff;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;
    use tokio::sync::watch;

    /// 崩溃退避复用既有 Fibonacci + jitter：1,1,2,3,5…55s 封顶（>5s 档
    /// 叠加 0..5s 单向上浮）；恢复后从首拍清零，累计计数保留。
    #[test]
    fn crash_backoff_reuses_fibonacci_and_resets_after_recovery() {
        let mut backoff = AccountCrashBackoff::default();
        assert_eq!(backoff.on_crash(), Duration::from_millis(1_000));
        assert_eq!(backoff.on_crash(), Duration::from_millis(1_000));
        assert_eq!(backoff.on_crash(), Duration::from_millis(2_000));
        assert_eq!(backoff.on_crash(), Duration::from_millis(3_000));
        assert_eq!(backoff.crashes(), 4);

        // 长拍叠加 jitter：基值 5s 档（step=4）→ [5s, 10s)。
        let delay = backoff.on_crash();
        assert!(delay >= Duration::from_millis(5_000));
        assert!(delay < Duration::from_millis(10_000));

        // 封顶：推进到 55s 档后继续崩溃，基值保持 55s，jitter 上界 +5s。
        for _ in 0..6 {
            backoff.on_crash();
        }
        for _ in 0..5 {
            let delay = backoff.on_crash();
            assert!(delay >= Duration::from_millis(55_000));
            assert!(delay < Duration::from_millis(60_000));
        }

        // 恢复清零：下一次崩溃回到首拍；累计计数不回退。
        backoff.on_recovered();
        assert_eq!(backoff.step(), 0);
        assert_eq!(backoff.on_crash(), Duration::from_millis(1_000));
        assert_eq!(backoff.crashes(), 17);
    }

    /// 单账号 panic 被收口：连续两次 panic 后本次运行正常返回，监督环
    /// 共运行 3 次且进程/调用方继续；panic 不向外传播。
    #[tokio::test]
    async fn panic_is_contained_and_account_restarts() {
        let runs = Rc::new(Cell::new(0u32));
        let runs_in = Rc::clone(&runs);
        let (_tx, rx) = watch::channel(false);
        let mut crashes = Vec::new();
        let backoff = super::run_account_supervisor(
            rx,
            AccountCrashBackoff::default(),
            move |_shutdown| {
                let runs = Rc::clone(&runs_in);
                async move {
                    let run = runs.get() + 1;
                    runs.set(run);
                    if run <= 2 {
                        panic!("injected account panic");
                    }
                }
            },
            |_step| Duration::from_millis(1),
            |count, step, delay| crashes.push((count, step, delay)),
        )
        .await;

        assert_eq!(runs.get(), 3, "两次 panic 后第三次运行并正常返回");
        assert_eq!(backoff.crashes(), 2);
        assert_eq!(crashes.len(), 2);
        assert_eq!(crashes[0].0, 1);
        assert_eq!(crashes[1].0, 2);
    }

    /// 一个账号的 panic 不拖垮另一个账号：两个监督环并行推进，A 崩溃两次
    /// 后恢复，B 全程只跑一次并正常返回。
    #[tokio::test]
    async fn panicking_account_does_not_stop_other_accounts() {
        let a_runs = Rc::new(Cell::new(0u32));
        let b_runs = Rc::new(Cell::new(0u32));
        let a_in = Rc::clone(&a_runs);
        let b_in = Rc::clone(&b_runs);
        let (_tx_a, rx_a) = watch::channel(false);
        let (_tx_b, rx_b) = watch::channel(false);

        let account_a = super::run_account_supervisor(
            rx_a,
            AccountCrashBackoff::default(),
            move |_shutdown| {
                let runs = Rc::clone(&a_in);
                async move {
                    let run = runs.get() + 1;
                    runs.set(run);
                    if run < 3 {
                        panic!("account a panic");
                    }
                }
            },
            |_step| Duration::from_millis(1),
            |_, _, _| {},
        );
        let account_b = super::run_account_supervisor(
            rx_b,
            AccountCrashBackoff::default(),
            move |_shutdown| {
                let runs = Rc::clone(&b_in);
                async move {
                    runs.set(runs.get() + 1);
                }
            },
            |_step| Duration::from_millis(1),
            |_, _, _| {},
        );

        let (_a, _b) = tokio::join!(account_a, account_b);
        assert_eq!(a_runs.get(), 3);
        assert_eq!(b_runs.get(), 1, "另一账号不受影响");
    }

    /// 停机旗在退避等待中生效：panic 后等待期间收到停旗，监督环立即返回，
    /// 不再重启 worker。
    #[tokio::test]
    async fn shutdown_during_backoff_stops_restarts() {
        let runs = Rc::new(Cell::new(0u32));
        let runs_in = Rc::clone(&runs);
        let (tx, rx) = watch::channel(false);

        let supervisor = super::run_account_supervisor(
            rx,
            AccountCrashBackoff::default(),
            move |_shutdown| {
                let runs = Rc::clone(&runs_in);
                async move {
                    runs.set(runs.get() + 1);
                    panic!("account panic");
                }
            },
            |_step| Duration::from_secs(30),
            |_, _, _| {},
        );
        let stopper = async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(true);
        };
        let (backoff, ()) = tokio::join!(supervisor, stopper);
        assert_eq!(runs.get(), 1, "停旗后不再重启");
        assert_eq!(backoff.crashes(), 1);
    }
}
