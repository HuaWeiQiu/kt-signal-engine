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

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
/// trace 恒关——headers/URI 仅在 trace 级，纪律红线）。
const MAX_LEVEL: LevelFilter = LevelFilter::WARN;

/// 日志文件名（data 目录下 logs/ 子目录内）。
const LOG_FILE_NAME: &str = "engine-stderr.log";

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
        *metadata.level() <= Level::WARN
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
        Some(MAX_LEVEL)
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if *metadata.level() <= Level::WARN {
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
            dir,
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
}
