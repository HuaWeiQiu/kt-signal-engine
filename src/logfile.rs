// SPDX-License-Identifier: AGPL-3.0-only

//! 引擎自身滚动文件日志（send 链可观测批 2026-10-16，compat §1/§5）。
//!
//! 背景：引擎 stderr 被 connector 收进内存环形缓冲（connector
//! `src/engine.rs` 一带），历史失败无第一手证据；本模块把进程内 tracing
//! 事件（vendor 传输栈 warn/error + 引擎自有失败证据行）镜像写入
//! `<data-dir>/logs/engine-stderr.log`——单文件 ≤5 MiB 大小轮转
//! （`.log.1`/`.log.2` 位移保留 2 份）、权限 0o600、路径从 `--data-dir`
//! 派生。stderr 纪律不变：本模块不写任何 stderr/stdout，该文件是 stderr
//! 的替代落点而非第二条 stderr 通道。
//!
//! 实现为零依赖自实现 `tracing::Subscriber`：不引 tracing-subscriber 及其
//! 依赖树（tracing 本体已经 presage/libsignal-service 在依赖图）。span 面
//! 最小化（不跟踪 span 树，事件只落 level/target/message/字段）——订阅
//! 目标是失败证据而非链路追踪。级别钉 warn（trace 恒关：vendor 在 trace
//! 级才输出 request headers/URI，纪律红线；debug/info 常规流不落盘，
//! 日志体积有界）。行格式：
//!
//! ```text
//! 2026-10-16T08:15:42.123Z WARN libsignal_service::websocket: websocket closed code=1006
//! ```
//!
//! 日志纪律（compat §1 同源）：引擎自有行只落固定字面量 cause 类（如
//! `cause=WsRejected`），无收件人/号码/消息内容/密钥；vendor 传输诊断
//! 文本为只读透传。初始化失败（目录创建/文件打开）= 无日志静默降级——
//! 不 panic、不写 stderr、IPC 服务不受影响。

// WIP（临终现场保全批，非本批工作）：`dump_last_moments` 与三个计数探针
// 尚未接线，故意保留在本文件等待该批完成。为避免阻断其他批次的
// clippy `-D warnings` 门禁，这里临时允许 dead_code；接线批次完成后删除
// 本属性并补上调用点（`record_pipeline_*` / `record_rpc_*` / panic hook）。
#![allow(dead_code)]

use std::collections::VecDeque;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{Attributes, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Id, Level, Metadata, Subscriber};
use tracing_core::span::Current;

/// 单文件上限：5 MiB（契约 ≤5MB，compat §1）。
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;
/// 轮转保留代数：`.log.1` + `.log.2`（契约保留 2 个轮转）。
const ROTATED_KEEP: usize = 2;
/// 文件级别上限：warn（vendor error!/warn! 失败证据 + 引擎自有 warn 行；
/// trace 恒关——headers/URI 仅在 trace 级，纪律红线）。唯一窄口例外见
/// [`OBSERVABILITY_INFO_TARGETS`]。
///
/// 注：`tracing::Level` 的 Ord 是 `ERROR > WARN > INFO > DEBUG > TRACE`，
/// 因此「warn 上限」不能用 `level <= Level::WARN` 表达（那样连 info 也放
/// 行）。逐事件判定收在 [`event_observable`]，这里只给订阅器 hint。
const HINT_LEVEL: LevelFilter = LevelFilter::INFO;

/// 观测白名单（info 级允许落盘的 target）：D0 转移归档链全程打点
/// （`engine.transfer`，D0 真机重验前置，台账 §29.10.8/§29.18）必须在
/// `engine-stderr.log` 可复盘——否则这些 info 打点等于不存在（hint 曾在
/// warn，info 调用点注册期即被判 `Interest::never`，一条也出不来）。只有
/// 白名单 target 的 info 落盘，其余 info 恒弃，日志体量与既有 warn+ 语义
/// 不变（2026-10-23 合流批，D0 观测准备）。
const OBSERVABILITY_INFO_TARGETS: &[&str] = &["engine.transfer"];

/// 事件是否落盘：error/warn 恒落（原契约）；info 仅白名单 target 落；
/// debug/trace 恒弃。hint 上界是 info，逐事件在此收紧。
fn event_observable(level: Level, target: &str) -> bool {
    match level {
        Level::ERROR | Level::WARN => true,
        Level::INFO => OBSERVABILITY_INFO_TARGETS.contains(&target),
        Level::DEBUG | Level::TRACE => false,
    }
}

/// 日志文件名（data 目录下 logs/ 子目录内）。
const LOG_FILE_NAME: &str = "engine-stderr.log";

// —— 临终现场保全（现场保全批 2026-10-17）——
// 背景：2026-10-17 20:16 退化窗（读调用 2.5s 失败/15s 超时、群发 26.5s、
// 信封错误风暴）后进程重启，engine-stderr.log 被整体截断——根因现场丢失。
// 修法 = 订阅器内嵌「最近 warn+ 环形缓冲」+ 进程临终三路径（panic hook /
// supervisor SIGTERM/SIGINT / ——）把缓冲 + 连接状态快照落 sidecar 文件
// `engine-last-moments-<ts>.log`：截断主日志不动，sidecar 不丢。全部静态
// 同步态——panic hook 内可安全读（无 await、无 async 锁）；任何失败静默
// （不 panic、不写 stderr，compat §1 同源纪律）。SIGKILL 不可钩（内核语义），
// 该面记档为已知界。

/// 环形缓冲容量（warn+ 行数）：退化窗（信封错误风暴）下 warn 行密、15 分钟
/// 数千行量级——1024 行足以覆盖「最近 N 分钟」现场且内存有界（单行远小于
/// 1 KiB）。
const LAST_MOMENTS_RING_LINES: usize = 1024;

static LAST_MOMENTS_RING: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());
/// sidecar 落点目录（`init` 时置位；未置位 = dump 静默 no-op）。
static LAST_MOMENTS_DIR: Mutex<Option<PathBuf>> = Mutex::new(None);
/// 每进程至多 dump 一次（panic 与 SIGTERM 竞态时先到的赢——最接近临终的
/// 现场；重复 dump 只会用更晚但更少上下文的缓冲覆盖更早的快照）。
static LAST_MOMENTS_DUMPED: AtomicBool = AtomicBool::new(false);
/// 活跃接收管线数（receive_pipeline spawn/退出计数；panic 快照可读）。
static PIPELINES_ACTIVE: AtomicUsize = AtomicUsize::new(0);
/// 在飞 JSON-RPC 请求数（main dispatch spawn_local 计数；panic 快照可读）。
static RPCS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// 最近 ka_diag（vendor websocket keepalive 诊断，websocket/mod.rs KA-DIAG
/// 批）事件时间戳（毫秒）与整行——连接退化第一证据，订阅器顺手沉淀。
static LAST_KA_DIAG_AT_MS: AtomicU64 = AtomicU64::new(0);
static LAST_KA_DIAG_LINE: Mutex<String> = Mutex::new(String::new());
/// 进程起始时刻（uptime 快照用）。
static STARTED_AT: OnceLock<Instant> = OnceLock::new();

/// 接收管线 spawn 计数（service::spawn_pipeline 挂载）。
pub(crate) fn record_pipeline_spawned() {
    PIPELINES_ACTIVE.fetch_add(1, Ordering::Relaxed);
}

/// 接收管线退出计数（receive_pipeline future 收口挂载）。
pub(crate) fn record_pipeline_exited() {
    PIPELINES_ACTIVE.fetch_sub(1, Ordering::Relaxed);
}

/// 在飞 RPC 进入计数（main dispatch spawn_local 挂载）。
pub(crate) fn record_rpc_entered() {
    RPCS_IN_FLIGHT.fetch_add(1, Ordering::Relaxed);
}

/// 在飞 RPC 收口计数。
pub(crate) fn record_rpc_exited() {
    RPCS_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
}

/// 临终现场落盘：sidecar 文件 `engine-last-moments-<ts>.log`（0o600，目录
/// 即主日志同 `logs/`）。头部 = 原因 + 时间戳 + uptime + 连接状态快照
/// （活跃管线数/在飞 RPC/最近 ka_diag），正文 = 环形缓冲内全部 warn+ 行
/// （旧→新）。每进程至多一次；任何失败静默（compat §1 纪律）。
pub(crate) fn dump_last_moments(reason: &str) {
    if LAST_MOMENTS_DUMPED.swap(true, Ordering::SeqCst) {
        return;
    }
    let Ok(dir_guard) = LAST_MOMENTS_DIR.lock() else {
        return;
    };
    let Some(dir) = dir_guard.clone() else {
        return;
    };
    drop(dir_guard);
    // 文件名时间戳：RFC3339 的 `:` 在 Windows 文件名非法，统一换 `-`。
    let stamp = timestamp().replace(':', "-");
    let path = dir.join(format!("engine-last-moments-{stamp}.log"));
    let Some(mut file) = open_log_file(&path) else {
        return;
    };
    let mut out = String::with_capacity(64 * 1024);
    out.push_str(&format!("last-moments reason={reason}\n"));
    out.push_str(&format!(
        "dumped_at={} uptime_secs={}\n",
        timestamp(),
        STARTED_AT.get().map(|t| t.elapsed().as_secs()).unwrap_or(0)
    ));
    out.push_str(&format!(
        "pipelines_active={} rpcs_in_flight={}\n",
        PIPELINES_ACTIVE.load(Ordering::Relaxed),
        RPCS_IN_FLIGHT.load(Ordering::Relaxed)
    ));
    let (ka_at, ka_line) = (
        LAST_KA_DIAG_AT_MS.load(Ordering::Relaxed),
        LAST_KA_DIAG_LINE
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default(),
    );
    out.push_str(&format!("last_ka_diag_at_ms={ka_at} line={ka_line}\n"));
    out.push_str("--- ring (warn+ events, oldest first) ---\n");
    if let Ok(ring) = LAST_MOMENTS_RING.lock() {
        for line in ring.iter() {
            out.push_str(line);
            out.push('\n');
        }
    }
    let _ = file.write_all(out.as_bytes());
    let _ = file.flush();
}

/// 订阅器内部状态：`file: None` = 打开失败静默降级（永不重试——data 目录
/// 启动期不可写通常进程级不可写，逐行重试只会放大失败）。
struct LogState {
    dir: PathBuf,
    current: PathBuf,
    file: Option<File>,
    size: u64,
}

impl LogState {
    /// 第 N 代轮转文件路径（`.log.1`/`.log.2`）。
    fn rotated_path(&self, generation: usize) -> PathBuf {
        self.dir.join(format!("{LOG_FILE_NAME}.{generation}"))
    }

    /// 轮转：老 `.log.2` 删除、`.log.1 → .log.2`、当前 → `.log.1`、重开
    /// 当前从零计长（Windows 语义 rename 不覆盖目标，故先删后移）。
    /// 任何一步失败静默——保留尚可写的旧文件优于失败上抛。
    fn rotate(&mut self) {
        for generation in (1..ROTATED_KEEP).rev() {
            let _ = fs::remove_file(self.rotated_path(generation + 1));
            let _ = fs::rename(
                self.rotated_path(generation),
                self.rotated_path(generation + 1),
            );
        }
        let _ = fs::remove_file(self.rotated_path(1));
        let _ = fs::rename(&self.current, self.rotated_path(1));
        self.file = open_log_file(&self.current);
        self.size = 0;
    }

    /// 追加一行；写前判满触发轮转（行级粒度，单行远小于帽值，文件体积
    /// 有界于 `MAX_FILE_BYTES + 单行`）。写后即 flush（崩溃面最小化）。
    fn write_line(&mut self, line: &str) {
        if self.file.is_some() && self.size >= MAX_FILE_BYTES {
            self.rotate();
        }
        let Some(file) = self.file.as_mut() else {
            return;
        };
        let mut bytes = line.as_bytes().to_vec();
        bytes.push(b'\n');
        let written = file.write_all(&bytes).is_ok();
        let _ = file.flush();
        if written {
            self.size += bytes.len() as u64;
        }
    }
}

/// 打开（或创建）日志文件，unix 权限钉 0o600（契约）。
fn open_log_file(path: &Path) -> Option<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
    }
    Some(file)
}

/// 滚动文件订阅器。`set_global_default` 要求 Send + Sync：状态在 `Mutex`
/// 内，事件写入是短临界区（无 await、无锁嵌套）。
pub(crate) struct RollingFileSubscriber {
    state: Mutex<LogState>,
}

impl Subscriber for RollingFileSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        event_observable(*metadata.level(), metadata.target())
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        // span 树不跟踪（事件面订阅）；恒一占位 id。
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = LineVisitor::default();
        event.record(&mut visitor);
        let Some(message) = visitor.message else {
            return;
        };
        let mut line = format!(
            "{} {} {}: {}",
            timestamp(),
            event.metadata().level(),
            event.metadata().target(),
            message
        );
        for (key, value) in visitor.fields {
            line.push(' ');
            line.push_str(key);
            line.push('=');
            line.push_str(&value);
        }
        // 临终现场环形缓冲 + ka_diag 沉淀（现场保全批）：warn+ 每行顺手
        // 进 ring（克隆代价可忽略——warn 级稀疏）；ka_diag 行另记时间戳与
        // 整行作连接退化第一证据。锁顺序：ring/ka 锁与 state 锁互不嵌套。
        // 环形缓冲维持 warn+ 语义（白名单 info 打点不得挤占临终现场：
        // poll_204 单次重链可至 60 行）。
        if matches!(event.metadata().level(), &Level::ERROR | &Level::WARN) {
            if let Ok(mut ring) = LAST_MOMENTS_RING.lock() {
                if ring.len() >= LAST_MOMENTS_RING_LINES {
                    ring.pop_front();
                }
                ring.push_back(line.clone());
            }
        }
        if line.contains("ka_diag") {
            LAST_KA_DIAG_AT_MS.store(
                chrono::Utc::now().timestamp_millis() as u64,
                Ordering::Relaxed,
            );
            if let Ok(mut last) = LAST_KA_DIAG_LINE.lock() {
                *last = line.clone();
            }
        }
        if let Ok(mut state) = self.state.lock() {
            state.write_line(&line);
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn current_span(&self) -> Current {
        Current::none()
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(HINT_LEVEL)
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        // 白名单 target 的 info 调用点必须注册（否则 hint 之外的静态闸会
        // 把它判成 never）；其余 target 仍按 warn+ 注册。
        if event_observable(*metadata.level(), metadata.target()) {
            Interest::always()
        } else {
            Interest::never()
        }
    }
}

/// 事件字段访问器：`message` 字段单列，其余字段 `key=Debug(value)` 尾随
/// （vendor 行如 `code=1006 reason="…"`；引擎自有行如 `cause=WsRejected`）。
#[derive(Default)]
struct LineVisitor {
    message: Option<String>,
    fields: Vec<(&'static str, String)>,
}

impl Visit for LineVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        } else {
            self.fields.push((field.name(), format!("{value:?}")));
        }
    }
}

/// 行时间戳：UTC RFC3339 毫秒（chrono 已在依赖图，零新增）。
fn timestamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// 启动期安装（main.rs 在 `--data-dir` 解析后调用，先于任何服务活动）：
/// 建 `<data-dir>/logs/`、打开/续写 `engine-stderr.log`（0o600，起点大小
/// 取现文件实长），设为全局默认订阅器。任何一步失败 = 无日志静默降级
/// （不 panic、不写 stderr——compat §1 stderr 纪律）。
pub(crate) fn init(data_dir: &Path) {
    let dir = data_dir.join("logs");
    let current = dir.join(LOG_FILE_NAME);
    let Some(state) = (|| -> Option<LogState> {
        fs::create_dir_all(&dir).ok()?;
        let size = fs::metadata(&current).map(|meta| meta.len()).unwrap_or(0);
        let file = open_log_file(&current)?;
        Some(LogState {
            dir: dir.clone(),
            current,
            file: Some(file),
            size,
        })
    })() else {
        return;
    };
    let _ = tracing::subscriber::set_global_default(RollingFileSubscriber {
        state: Mutex::new(state),
    });
    // 临终现场保全：sidecar 目录与起始时刻置位（dump 失败静默的既有纪律）。
    let _ = STARTED_AT.set(Instant::now());
    if let Ok(mut dir_slot) = LAST_MOMENTS_DIR.lock() {
        *dir_slot = Some(dir);
    }
}

/// 测试隔离辅助：重置 dump 一次旗 + 环形缓冲 + 快照静态（进程内静态态，
/// 并行测试间必须显式复位保证确定性）。
#[cfg(test)]
fn reset_last_moments_for_test() {
    LAST_MOMENTS_DUMPED.store(false, Ordering::SeqCst);
    LAST_KA_DIAG_AT_MS.store(0, Ordering::SeqCst);
    PIPELINES_ACTIVE.store(0, Ordering::SeqCst);
    RPCS_IN_FLIGHT.store(0, Ordering::SeqCst);
    if let Ok(mut ring) = LAST_MOMENTS_RING.lock() {
        ring.clear();
    }
    if let Ok(mut last) = LAST_KA_DIAG_LINE.lock() {
        *last = String::new();
    }
    if let Ok(mut dir_slot) = LAST_MOMENTS_DIR.lock() {
        *dir_slot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试沙盒目录（进程 id + 测试名隔离——并行测试不得共享目录，用后
    /// 即删）。
    fn sandbox(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kt-engine-logtest-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("tmp dir");
        dir
    }

    /// 轮转位移面：写满触发「老 .2 删、.1→.2、当前→.1、当前重开从零
    /// 计长」，恰保留两代；权限 0o600。
    #[test]
    fn rotation_shifts_and_keeps_two_generations() {
        let dir = sandbox("rotate");
        let current = dir.join(LOG_FILE_NAME);

        let mut state = LogState {
            dir: dir.clone(),
            current: current.clone(),
            file: open_log_file(&current),
            size: MAX_FILE_BYTES,
        };

        // 第一轮：current 为空即触轮转（空 .1），新行落新文件。
        state.write_line("first-after-rotation");
        assert_eq!(
            fs::read_to_string(&current).expect("current content"),
            "first-after-rotation\n"
        );
        assert_eq!(
            fs::read_to_string(dir.join(format!("{LOG_FILE_NAME}.1"))).expect("empty gen-1 file"),
            ""
        );

        // 第二轮：.1 = 前一文件内容（first-after-rotation），gen1 落当前。
        state.size = MAX_FILE_BYTES;
        state.write_line("gen1");
        assert_eq!(
            fs::read_to_string(dir.join(format!("{LOG_FILE_NAME}.1"))).expect("gen-1 rotated"),
            "first-after-rotation\n"
        );

        // 第三轮：.2 = gen1 前的 .1 内容，.1 = gen1，恰两代、更早内容被移出。
        state.size = MAX_FILE_BYTES;
        state.write_line("gen2");
        assert_eq!(
            fs::read_to_string(dir.join(format!("{LOG_FILE_NAME}.2"))).expect("gen-2 rotated"),
            "first-after-rotation\n"
        );
        assert_eq!(
            fs::read_to_string(dir.join(format!("{LOG_FILE_NAME}.1")))
                .expect("gen-1 after third rotation"),
            "gen1\n"
        );
        assert_eq!(fs::read_to_string(&current).expect("current"), "gen2\n");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&current).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "日志文件权限 0o600");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// 体积界：未满不轮转（同文件续写，行数随写随增）。
    #[test]
    fn under_budget_keeps_appending() {
        let dir = sandbox("append");
        let current = dir.join(LOG_FILE_NAME);
        let mut state = LogState {
            dir: dir.clone(),
            current: current.clone(),
            file: open_log_file(&current),
            size: 0,
        };
        state.write_line("a");
        state.write_line("b");
        assert_eq!(
            fs::read_to_string(&current).expect("content"),
            "a\nb\n",
            "未满不轮转"
        );
        assert!(state.size > 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// 事件端到端：Subscriber 经真实 `warn!` 事件落盘——行格式
    /// `{ts} {LEVEL} {target}: {message} {fields}`、cause 字段尾随、级别
    /// 闸（info 恒不落盘）。
    #[test]
    fn subscriber_writes_warn_events_with_fields() {
        let dir = sandbox("event");
        let current = dir.join(LOG_FILE_NAME);
        fs::create_dir_all(&dir).expect("dir");
        let size = 0;
        let state = LogState {
            dir: dir.clone(),
            current: current.clone(),
            file: open_log_file(&current),
            size,
        };
        let subscriber = RollingFileSubscriber {
            state: Mutex::new(state),
        };
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                cause = "WsRejected",
                "send fast retry after definite failure"
            );
            // 级别闸：warn 上限之下（info/debug/trace）恒不落盘。
            tracing::info!("info-must-not-land");
        });
        let content = fs::read_to_string(&current).expect("log content");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1, "恰一行（info 被级别闸挡下）: {content:?}");
        let line = lines[0];
        assert!(line.contains("WARN"), "级别段: {line}");
        assert!(
            line.contains("send fast retry after definite failure"),
            "message 段: {line}"
        );
        assert!(line.contains("cause=\"WsRejected\""), "字段尾随: {line}");
        // 时间戳形状：RFC3339 毫秒（`YYYY-MM-DDTHH:MM:SS.mmmZ` 前缀）。
        let (ts, _rest) = line.split_once(' ').expect("ts split");
        assert_eq!(ts.len(), 24, "RFC3339 毫秒长度: {ts}");
        assert_eq!(&ts[4..5], "-", "ts 分隔: {ts}");
        assert!(ts.ends_with('Z'), "ts UTC 尾: {ts}");
        let _ = fs::remove_dir_all(&dir);
    }

    /// 观测白名单闸（D0 观测准备批）：warn/error 恒落盘；info 仅在白名单
    /// target（`engine.transfer`）落盘；debug/trace 恒弃。
    #[test]
    fn info_observability_whitelist_arms() {
        assert!(event_observable(Level::ERROR, "anything"));
        assert!(event_observable(Level::WARN, "kt_signal_engine::service"));
        assert!(event_observable(Level::INFO, "engine.transfer"));
        assert!(!event_observable(Level::INFO, "kt_signal_engine::service"));
        assert!(!event_observable(Level::DEBUG, "engine.transfer"));
        assert!(!event_observable(Level::TRACE, "engine.transfer"));
    }

    /// 端到端：白名单 target 的 info 事件真落盘（D0 真机复盘面），且不进
    /// 临终现场环形缓冲；非白名单 info 仍被挡下。
    #[test]
    fn subscriber_writes_whitelisted_info_target_only() {
        let dir = sandbox("info-whitelist");
        let current = dir.join(LOG_FILE_NAME);
        fs::create_dir_all(&dir).expect("dir");
        let state = LogState {
            dir: dir.clone(),
            current: current.clone(),
            file: open_log_file(&current),
            size: 0,
        };
        let subscriber = RollingFileSubscriber {
            state: Mutex::new(state),
        };
        reset_last_moments_for_test();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                target: "engine.transfer",
                stage = "poll_204",
                round = 1u32,
                "transfer_archive poll 204; phone side not ready yet"
            );
            tracing::info!(target: "kt_signal_engine::service", "plain-info-must-not-land");
            tracing::warn!(target: "engine.transfer", "transfer_archive poll failed");
        });
        let content = fs::read_to_string(&current).expect("log content");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "info(白名单)+warn 各一行: {content:?}");
        assert!(
            lines[0].contains(" INFO engine.transfer: transfer_archive poll 204"),
            "白名单 info 行: {}",
            lines[0]
        );
        assert!(
            lines[0].contains("stage=\"poll_204\""),
            "字段: {}",
            lines[0]
        );
        assert!(
            !content.contains("plain-info-must-not-land"),
            "非白名单 info 不得落盘"
        );
        // 环形缓冲（临终现场）仍只收 warn+：白名单 info 行不占位（ring 是
        // 进程级静态，并行测试别的 warn 行可能已入队，故按内容判定）。
        let ring = LAST_MOMENTS_RING.lock().expect("ring lock");
        assert!(
            !ring.iter().any(|line| line.contains("poll 204")),
            "info 不得进 ring: {:?}",
            ring.iter().collect::<Vec<_>>()
        );
        drop(ring);
        let _ = fs::remove_dir_all(&dir);
    }
}
