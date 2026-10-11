// SPDX-License-Identifier: AGPL-3.0-only

//! 备份导出任务面（`docs/backup-export-contract.md` §10.3/§10.4）：每账号
//! 后台导出任务注册表 + `backup.export`/`backup.exportStatus` 语义。
//!
//! 执行机制（契约 §10.3 裁定）：密钥派生在 LocalSet（presage 非 Send 面），
//! 装配链（读工件 → `MainStreamWriter` → `export_local_backup`）是纯同步
//! `Send` 代码，移交专用 OS 线程执行——导出不得阻塞同一线程上的接收管线
//! 与请求 dispatch；完成通知走注册表轮询（exportStatus），无 async 唤醒面。
//! 注册表纯进程内存（不落盘，引擎重启归 idle，快照仍在盘上）。
//!
//! 纪律：备份密钥绝不离开引擎进程（§10.5）——本层不落日志不落盘，线程
//! 即用即弃；工件在任务离开 running 态时必删（§10.2 过渡性硬边界）。

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use presage::store::StateStore as _;
use rand::RngCore as _;
use serde_json::{json, Value};

use crate::backup::{
    assemble, derive_keys, export_local_backup, validate_first_line, BackupError,
    LocalBackupParams, ARTIFACT_FILE_NAME,
};

/// 快照根目录名（`export.rs` 目录编排约定）。
const SNAPSHOT_ROOT: &str = "SignalBackups";

/// exportStatus 快照清单帽（契约 §10.2，新→旧）。
const MAX_SNAPSHOT_ENTRIES: usize = 32;

/// 任务注册表（引擎进程内存；键 = 账号号码）。
pub(crate) type BackupTaskRegistry = StdMutex<HashMap<String, BackupTaskRecord>>;

/// 任务状态机（契约 §10.3）：`running → completed | failed`；idle = 无记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupTaskState {
    Running,
    Completed,
    Failed,
}

impl BackupTaskState {
    fn as_str(self) -> &'static str {
        match self {
            BackupTaskState::Running => "running",
            BackupTaskState::Completed => "completed",
            BackupTaskState::Failed => "failed",
        }
    }
}

/// 本进程任务产出的快照登记（exportStatus `snapshot`/`snapshots` 数据源）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupSnapshotInfo {
    /// `signal-backup-<UTC ts>` 目录名。
    pub(crate) name: String,
    /// main 文件字节数（密文）。
    pub(crate) main_bytes: u64,
    /// 帧数仅本进程任务已知（历史快照不重读密文，诚实缺席）。
    pub(crate) frames: Option<u64>,
}

/// 单账号任务记录（exportStatus 的全部状态来源）。
#[derive(Debug, Clone)]
pub(crate) struct BackupTaskRecord {
    pub(crate) state: BackupTaskState,
    pub(crate) started_at_ms: u64,
    pub(crate) finished_at_ms: Option<u64>,
    /// 工件行数（成功面诚实计数；失败面未知缺席）。
    pub(crate) lines: Option<u64>,
    pub(crate) frames: Option<u64>,
    pub(crate) snapshot: Option<BackupSnapshotInfo>,
    /// 结构化错误码（契约 §10.7；仅 failed 态）。
    pub(crate) error_code: Option<&'static str>,
}

/// `backup.export` 拒绝面（契约 §10.7：参数/账号/AEP 面一律 `-32603`，
/// 结构化码走 `-1` + message）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupExportRejection {
    /// `-1` + 码位（connector 白名单计划逐字，契约 §10.7）。
    Structured(&'static str),
    /// `-32603` 参数面（缺 account/未注册/fake 账号/AEP 缺失）。
    Internal,
}

/// launch 阶段失败（start 校验后段：在跑闸与线程启动）。
#[derive(Debug)]
enum LaunchError {
    AlreadyRunning,
    SpawnFailed,
}

/// start 校验前段（契约 §10.4 顺序①②）：工件存在（缺
/// `BACKUP_ARTIFACT_MISSING`）→ 首行可解析为合法 account 行（非法当场删
/// 工件 + `BACKUP_ARTIFACT_INVALID`，§10.2）。
fn precheck_artifact(account_dir: &Path) -> Result<(), BackupExportRejection> {
    let artifact = account_dir.join(ARTIFACT_FILE_NAME);
    if !artifact.is_file() {
        return Err(BackupExportRejection::Structured("BACKUP_ARTIFACT_MISSING"));
    }
    if validate_first_line(&artifact).is_err() {
        // 非法工件当场删除：引擎不持久化明文聊天内容（§10.2 硬边界）。
        let _ = fs::remove_file(&artifact);
        return Err(BackupExportRejection::Structured("BACKUP_ARTIFACT_INVALID"));
    }
    Ok(())
}

/// launch（契约 §10.4 顺序④）：在跑闸（`BACKUP_EXPORT_ALREADY_RUNNING`）
/// 与注册+线程启动在同一注册表锁段内完成（并发 start 只有一个赢家）。
fn launch_export_task(
    registry: &Arc<BackupTaskRegistry>,
    account: String,
    account_dir: PathBuf,
    params: LocalBackupParams,
) -> Result<(), LaunchError> {
    {
        let mut guard = registry.lock().expect("backup task registry lock");
        if guard
            .get(&account)
            .is_some_and(|record| record.state == BackupTaskState::Running)
        {
            return Err(LaunchError::AlreadyRunning);
        }
        guard.insert(
            account.clone(),
            BackupTaskRecord {
                state: BackupTaskState::Running,
                started_at_ms: now_ms(),
                finished_at_ms: None,
                lines: None,
                frames: None,
                snapshot: None,
                error_code: None,
            },
        );
    }

    let artifact = account_dir.join(ARTIFACT_FILE_NAME);
    let registry_for_thread = Arc::clone(registry);
    // 线程启动失败（资源耗尽级罕见面）：任务记录留在 Running——工件与终态
    // 删除留给下一次导出尝试（§10.2）；调用方按 -32603 收敛。
    std::thread::Builder::new()
        .name("backup-export".into())
        .spawn(move || run_export_task(registry_for_thread, account, account_dir, params, artifact))
        .map(|_| ())
        .map_err(|_| LaunchError::SpawnFailed)
}

/// 专用线程体：装配链全同步（读工件 → 状态机 → 落快照），任何终态后删
/// 工件（§10.2），回写注册表（exportStatus 轮询面）。
fn run_export_task(
    registry: Arc<BackupTaskRegistry>,
    account: String,
    account_dir: PathBuf,
    params: LocalBackupParams,
    artifact: PathBuf,
) {
    let mut lines: u64 = 0;
    let outcome = export_local_backup(&account_dir, &params, |out| {
        let reader = BufReader::new(File::open(&artifact)?);
        let stats = assemble(out, reader, params.backup_time_ms)?;
        lines = stats.lines;
        Ok(())
    });
    // 终态即删：成功或任何失败都不留明文工件（契约 §10.2 硬边界）。
    let _ = fs::remove_file(&artifact);

    let mut guard = registry.lock().expect("backup task registry lock");
    let record = guard.entry(account).or_insert_with(|| BackupTaskRecord {
        state: BackupTaskState::Running,
        started_at_ms: now_ms(),
        finished_at_ms: None,
        lines: None,
        frames: None,
        snapshot: None,
        error_code: None,
    });
    match outcome {
        Ok(report) => {
            record.state = BackupTaskState::Completed;
            record.finished_at_ms = Some(now_ms());
            record.lines = Some(lines);
            record.frames = Some(report.frame_count);
            record.snapshot = Some(BackupSnapshotInfo {
                name: report
                    .snapshot_dir
                    .file_name()
                    .expect("snapshot dir has file name")
                    .to_string_lossy()
                    .into_owned(),
                main_bytes: report.main_bytes,
                frames: Some(report.frame_count),
            });
            record.error_code = None;
        }
        Err(error) => {
            // 行级/帧序违规与 IO 失败都可能发生在任意中途位置：行数/帧数/
            // 快照未知，诚实缺席（契约 §10.4「不发明 0」）。
            record.state = BackupTaskState::Failed;
            record.finished_at_ms = Some(now_ms());
            record.lines = None;
            record.frames = None;
            record.snapshot = None;
            record.error_code = Some(task_error_code(&error));
        }
    }
}

/// 导出面错误 → 结构化码（契约 §10.7 五码位）。
fn task_error_code(error: &BackupError) -> &'static str {
    match error {
        BackupError::SnapshotExists(_) => "BACKUP_SNAPSHOT_EXISTS",
        BackupError::Io(_) | BackupError::Proto(_) => "BACKUP_IO_FAILED",
        // 帧序状态机违规与工件行级违规同码（§10.7：永不静默丢行）。
        BackupError::FrameSequence(_) | BackupError::Artifact(_) => "BACKUP_ARTIFACT_INVALID",
    }
}

// ---------------------------------------------------------------------------
// Service RPC 语义（实现挂在 Service 上；本模块可见其私有字段）
// ---------------------------------------------------------------------------

impl super::Service {
    /// `backup.export`（契约 §10.4 校验序）：账号已注册 → 工件存在 → 首行
    /// 合法 → AEP 可读（密钥派生在 LocalSet）→ 在跑闸 + 线程启动。成功即
    /// 后台执行，调用方回 `{"status": "started"}`。
    pub(crate) async fn backup_export(&self, account: &str) -> Result<(), BackupExportRejection> {
        // ① 账号已注册（注册表有 store 目录；fake/未注册 -32603）。
        let account_dir = {
            let state = self.state.lock().await;
            let rel = state
                .account_dir(account)
                .ok_or(BackupExportRejection::Internal)?;
            self.data_dir.join(rel)
        };
        // ② 工件存在 → ③ 首行合法（非法当场删）。
        precheck_artifact(&account_dir)?;
        // ④ AEP/ACI（presage store kv + registration data）→ 派生（LocalSet
        //    内完成，§10.3/§10.5）。缺 AEP 导出不可为，-32603。
        let store = super::open_store(&account_dir)
            .await
            .map_err(|_| BackupExportRejection::Internal)?;
        let account_entropy_pool = store
            .fetch_account_entropy_pool()
            .await
            .map_err(|_| BackupExportRejection::Internal)?
            .ok_or(BackupExportRejection::Internal)?;
        let registration = store
            .load_registration_data()
            .await
            .map_err(|_| BackupExportRejection::Internal)?
            .ok_or(BackupExportRejection::Internal)?;
        let aci = presage::libsignal_service::protocol::Aci::from(registration.service_ids.aci);
        let keys = derive_keys(&account_entropy_pool, &aci);

        // mediaRootBackupKey：引擎无既定持久来源，每导出随机 32B（契约
        // §10.5 登记；未来附件批必须重审）。
        let mut media_root_backup_key = [0u8; 32];
        rand::rng().fill_bytes(&mut media_root_backup_key);
        let params = LocalBackupParams {
            keys,
            backup_time_ms: now_ms(),
            media_root_backup_key,
            app_version: format!("kt-signal-engine {}", env!("CARGO_PKG_VERSION")),
        };

        // ⑤ 在跑闸 + 注册 + 专用线程启动（原子段在注册表锁内）。
        launch_export_task(&self.backup_tasks, account.to_owned(), account_dir, params).map_err(
            |error| match error {
                LaunchError::AlreadyRunning => {
                    BackupExportRejection::Structured("BACKUP_EXPORT_ALREADY_RUNNING")
                }
                LaunchError::SpawnFailed => BackupExportRejection::Internal,
            },
        )
    }

    /// `backup.exportStatus`（契约 §10.4）：注册表状态 + 工件在场 +
    /// 快照清单（新→旧 ≤32）。未注册账号 -32603（`Err(())`）。
    pub(crate) async fn backup_export_status(&self, account: &str) -> Result<Value, ()> {
        let account_dir = {
            let state = self.state.lock().await;
            let rel = state.account_dir(account).ok_or(())?;
            self.data_dir.join(rel)
        };
        let record = self
            .backup_tasks
            .lock()
            .expect("backup task registry lock")
            .get(account)
            .cloned();
        let artifact_present = account_dir.join(ARTIFACT_FILE_NAME).exists();
        let snapshots = list_snapshots(&account_dir);

        let mut payload = json!({
            "account": account,
            "state": record
                .as_ref()
                .map(|record| record.state.as_str())
                .unwrap_or("idle"),
            "artifactPresent": artifact_present,
            "snapshots": snapshots
                .iter()
                .map(|snapshot| {
                    // frames 仅本进程任务产出已知；历史快照只报 name/mainBytes。
                    let known_frames = record
                        .as_ref()
                        .and_then(|record| record.snapshot.as_ref())
                        .filter(|known| known.name == snapshot.name)
                        .and_then(|known| known.frames);
                    snapshot_value(&BackupSnapshotInfo {
                        frames: known_frames,
                        ..snapshot.clone()
                    })
                })
                .collect::<Vec<_>>(),
        });
        if let Some(record) = record {
            payload["startedAtMs"] = json!(record.started_at_ms);
            if let Some(finished_at_ms) = record.finished_at_ms {
                payload["finishedAtMs"] = json!(finished_at_ms);
            }
            if let Some(lines) = record.lines {
                payload["lines"] = json!(lines);
            }
            if let Some(frames) = record.frames {
                payload["frames"] = json!(frames);
            }
            if let Some(snapshot) = record.snapshot {
                payload["snapshot"] = snapshot_value(&snapshot);
            }
            if let Some(error_code) = record.error_code {
                payload["error"] = json!(error_code);
            }
        }
        Ok(payload)
    }
}

/// 快照清单（契约 §10.4）：`signal-backup-*` 非 `-tmp`，新→旧，≤32 条；
/// main 缺失记 0（损坏面如实）。引擎只报告不清理（§10.6）。
fn list_snapshots(account_dir: &Path) -> Vec<BackupSnapshotInfo> {
    let root = account_dir.join(SNAPSHOT_ROOT);
    let Ok(entries) = fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut snapshots: Vec<BackupSnapshotInfo> = entries
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("signal-backup-") && !name.ends_with("-tmp"))
        .map(|name| BackupSnapshotInfo {
            main_bytes: fs::metadata(root.join(&name).join("main"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            name,
            frames: None,
        })
        .collect();
    // 目录名后缀是 UTC `yyyy-MM-dd-HH-mm-ss`：字典序 = 时间序。
    snapshots.sort_by(|a, b| b.name.cmp(&a.name));
    snapshots.truncate(MAX_SNAPSHOT_ENTRIES);
    snapshots
}

fn snapshot_value(snapshot: &BackupSnapshotInfo) -> Value {
    let mut value = json!({
        "name": snapshot.name,
        "mainBytes": snapshot.main_bytes,
    });
    if let Some(frames) = snapshot.frames {
        value["frames"] = json!(frames);
    }
    value
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis()
        .try_into()
        .expect("millis fit u64")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use base64::Engine as _;
    use presage::libsignal_service::utils::{phonenumber_from_signal, TryIntoE164};
    use presage::manager::RegistrationData;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::backup::LocalBackupParams;

    const BACKUP_TIME_MS: u64 = 1_760_000_000_000;

    fn temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-engine-backup-task-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    fn params_at(backup_time_ms: u64) -> LocalBackupParams {
        LocalBackupParams {
            keys: crate::backup::tests_official_keys(),
            backup_time_ms,
            media_root_backup_key: [0x42; 32],
            app_version: "kt-signal-engine 0.1.0".into(),
        }
    }

    fn account_line() -> String {
        format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\",\"givenName\":\"Kit\"}}",
            "11".repeat(32)
        )
    }

    fn contact_line(id: u64) -> String {
        format!(
            "{{\"type\":\"contact\",\"id\":{id},\"e164\":15555550100,\"registration\":\"registered\"}}"
        )
    }

    fn chat_line(id: u64, recipient: u64) -> String {
        format!("{{\"type\":\"chat\",\"id\":{id},\"recipient\":{recipient}}}")
    }

    fn item_line(chat: u64, author: u64, body: &str) -> String {
        format!(
            "{{\"type\":\"chat_item\",\"chat\":{chat},\"author\":{author},\"dateSent\":1760000001000,\
             \"direction\":\"incoming\",\"dateReceived\":1760000001005,\"body\":\"{body}\"}}"
        )
    }

    fn write_artifact(account_dir: &Path, lines: &[String]) -> PathBuf {
        std::fs::create_dir_all(account_dir).expect("account dir");
        let path = account_dir.join(ARTIFACT_FILE_NAME);
        std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("artifact written");
        path
    }

    /// 小工件：account + contact + chat + 1 条消息（4 行）。
    fn small_lines() -> Vec<String> {
        vec![
            account_line(),
            contact_line(10),
            chat_line(1, 10),
            item_line(1, 10, "hello"),
        ]
    }

    fn wait_terminal(registry: &Arc<BackupTaskRegistry>, account: &str) -> (BackupTaskRecord, u64) {
        let mut polls = 0u64;
        for _ in 0..6000 {
            polls += 1;
            if let Some(record) = registry.lock().expect("registry lock").get(account) {
                if record.state != BackupTaskState::Running {
                    return (record.clone(), polls);
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("export task did not reach a terminal state");
    }

    fn expect_structured(rejection: Result<(), BackupExportRejection>, code: &'static str) {
        assert_eq!(rejection, Err(BackupExportRejection::Structured(code)));
    }

    /// start 校验前段：缺失 / 首行非法（当场删工件）/ 合法放行。
    #[test]
    fn precheck_gates_missing_and_invalid_artifacts() {
        let dir = temp_dir("precheck");
        std::fs::create_dir_all(&dir).expect("dir");

        expect_structured(precheck_artifact(&dir), "BACKUP_ARTIFACT_MISSING");

        // 首行非 account：拒绝 + 工件当场删除（§10.2 硬边界）。
        let artifact = write_artifact(&dir, &[contact_line(10)]);
        expect_structured(precheck_artifact(&dir), "BACKUP_ARTIFACT_INVALID");
        assert!(!artifact.exists(), "invalid artifact deleted synchronously");

        write_artifact(&dir, &[account_line()]);
        precheck_artifact(&dir).expect("valid artifact");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 生命周期：running → completed；工件已删；快照在盘且字段诚实。
    #[test]
    fn export_task_reaches_completed_and_deletes_artifact() {
        let dir = temp_dir("lifecycle");
        let artifact = write_artifact(&dir, &small_lines());
        let registry: Arc<BackupTaskRegistry> = Arc::new(StdMutex::new(HashMap::new()));

        launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("task starts");
        assert_eq!(
            registry
                .lock()
                .expect("registry lock")
                .get("acct")
                .map(|record| record.state),
            Some(BackupTaskState::Running)
        );

        let (record, _) = wait_terminal(&registry, "acct");
        assert_eq!(record.state, BackupTaskState::Completed);
        assert_eq!(record.error_code, None);
        assert_eq!(record.lines, Some(4));
        // BackupInfo + Account + self + contact + chat + item + folder(自动补)。
        assert_eq!(record.frames, Some(7));
        assert!(record.started_at_ms > 0);
        assert!(record.finished_at_ms.is_some());
        let snapshot = record.snapshot.expect("snapshot recorded");
        assert!(snapshot.name.starts_with("signal-backup-"));
        assert!(snapshot.main_bytes > 0);
        assert_eq!(snapshot.frames, record.frames);

        assert!(!artifact.exists(), "artifact deleted at terminal state");
        let snapshot_dir = dir.join(SNAPSHOT_ROOT).join(&snapshot.name);
        assert!(snapshot_dir.join("metadata").is_file());
        assert!(snapshot_dir.join("main").is_file());
        assert!(snapshot_dir.join("files").is_dir());

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 在跑闸 + 活性：导出运行中第二个 launch 被拒（AlreadyRunning）；
    /// 轮询面在整个导出窗口持续推进（导出跑在专用线程，不占调用线程，
    /// 契约 §10.3 活性证据）。
    #[test]
    fn already_running_rejected_and_registry_pollable_during_export() {
        let dir = temp_dir("liveness");
        // 大工件拉长导出窗口，给轮询面留出可观测的 running 区间。
        let mut lines = vec![account_line(), contact_line(10), chat_line(1, 10)];
        for index in 0..40_000 {
            lines.push(item_line(1, 10, &format!("message {index}")));
        }
        write_artifact(&dir, &lines);
        let registry: Arc<BackupTaskRegistry> = Arc::new(StdMutex::new(HashMap::new()));

        launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("first task starts");

        // 在跑闸：Running 记录在注册表，第二次 launch 立即被拒（工件仍在盘，
        // 冲突只可能在任务闸上）。
        let second = launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        );
        assert!(matches!(second, Err(LaunchError::AlreadyRunning)));
        // 被拒的 start 不得留下第二条记录或改写首条。
        assert_eq!(
            registry.lock().expect("registry lock").len(),
            1,
            "rejected start does not duplicate the record"
        );

        // 活性：首观测态 running；轮询循环在导出窗口内持续推进。
        let mut polls = 0u64;
        let mut first_observed_running = false;
        loop {
            let state = registry
                .lock()
                .expect("registry lock")
                .get("acct")
                .map(|record| record.state);
            if polls == 0 {
                first_observed_running = state == Some(BackupTaskState::Running);
            }
            polls += 1;
            if matches!(
                state,
                Some(BackupTaskState::Completed) | Some(BackupTaskState::Failed)
            ) {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(first_observed_running, "first observed state is running");
        assert!(
            polls > 10,
            "poller kept advancing during export ({polls} polls)"
        );

        let (record, _) = wait_terminal(&registry, "acct");
        assert_eq!(record.state, BackupTaskState::Completed);
        assert!(
            !dir.join(ARTIFACT_FILE_NAME).exists(),
            "artifact deleted at terminal state"
        );

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 运行中行级违规：任务 failed（BACKUP_ARTIFACT_INVALID），工件已删，
    /// 无半成品快照（§10.2/§10.7）。
    #[test]
    fn mid_file_violation_fails_task_and_cleans_up() {
        let dir = temp_dir("mid-violation");
        let artifact = write_artifact(&dir, &[account_line(), "{\"type\":\"bogus\"}".to_owned()]);
        let registry: Arc<BackupTaskRegistry> = Arc::new(StdMutex::new(HashMap::new()));

        launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("task starts");
        let (record, _) = wait_terminal(&registry, "acct");
        assert_eq!(record.state, BackupTaskState::Failed);
        assert_eq!(record.error_code, Some("BACKUP_ARTIFACT_INVALID"));
        assert_eq!(record.lines, None, "failed record reports honest absence");
        assert_eq!(record.snapshot, None);
        assert!(!artifact.exists(), "artifact deleted on failure");
        let backups = fs::read_dir(dir.join(SNAPSHOT_ROOT)).expect("backups root");
        assert_eq!(backups.count(), 0, "no tmp or partial snapshot residue");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 同秒快照冲突（BACKUP_SNAPSHOT_EXISTS）：同参数二次导出撞同名快照。
    #[test]
    fn same_second_collision_maps_snapshot_exists() {
        let dir = temp_dir("collision");
        let registry: Arc<BackupTaskRegistry> = Arc::new(StdMutex::new(HashMap::new()));

        write_artifact(&dir, &small_lines());
        launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("first export");
        let (record, _) = wait_terminal(&registry, "acct");
        assert_eq!(record.state, BackupTaskState::Completed);

        // 同 backup_time_ms 重导：快照目录已存在 → 任务失败 + 可重试码。
        write_artifact(&dir, &small_lines());
        launch_export_task(
            &registry,
            "acct".into(),
            dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("second export starts (no running record)");
        let (record, _) = wait_terminal(&registry, "acct");
        assert_eq!(record.state, BackupTaskState::Failed);
        assert_eq!(record.error_code, Some("BACKUP_SNAPSHOT_EXISTS"));
        assert!(
            !dir.join(ARTIFACT_FILE_NAME).exists(),
            "artifact deleted on failure too"
        );

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 快照清单：新→旧、`-tmp`/杂目录剔除、main 字节、32 条帽。
    #[test]
    fn snapshot_listing_is_sorted_capped_and_honest() {
        let dir = temp_dir("snapshots");
        let root = dir.join(SNAPSHOT_ROOT);
        fs::create_dir_all(root.join("signal-backup-2020-01-01-00-00-00")).expect("old");
        fs::write(
            root.join("signal-backup-2020-01-01-00-00-00").join("main"),
            b"1234567",
        )
        .expect("main");
        fs::create_dir_all(root.join("signal-backup-2021-01-01-00-00-00-tmp")).expect("tmp");
        fs::create_dir_all(root.join("not-a-snapshot")).expect("stray");
        // 33 个新快照（2022-…-00..32）+ 1 个 2020 快照 → 只留最新 32 条。
        for suffix in 0..33u32 {
            let name = format!("signal-backup-2022-01-01-00-00-{suffix:02}");
            fs::create_dir_all(root.join(&name)).expect("snapshot dir");
        }
        fs::write(
            root.join("signal-backup-2022-01-01-00-00-32").join("main"),
            b"123456789",
        )
        .expect("main of newest");

        let snapshots = list_snapshots(&dir);
        assert_eq!(snapshots.len(), MAX_SNAPSHOT_ENTRIES, "capped at 32");
        assert_eq!(
            snapshots[0].name, "signal-backup-2022-01-01-00-00-32",
            "newest first"
        );
        assert_eq!(snapshots[0].main_bytes, 9, "main bytes reported");
        assert_eq!(snapshots[1].name, "signal-backup-2022-01-01-00-00-31");
        assert_eq!(snapshots[1].main_bytes, 0, "main missing reported as 0");
        assert_eq!(
            snapshots[0].frames, None,
            "historical snapshots report no frames (honest absence)"
        );
        assert!(
            snapshots
                .iter()
                .all(|snapshot| !snapshot.name.ends_with("-tmp")),
            "tmp residue never listed"
        );
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.name != "not-a-snapshot"),
            "stray dirs never listed"
        );
        // 2020 快照被 32 条帽裁掉（34 个合法条目 → 最旧出局）。
        assert!(
            snapshots
                .iter()
                .all(|snapshot| !snapshot.name.starts_with("signal-backup-2020")),
            "oldest snapshot evicted by cap"
        );

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// exportStatus 状态面：idle → 任务经注册表启动 → completed（快照字段
    /// 与 artifacts 在场面诚实）。
    #[tokio::test]
    async fn export_status_surface_tracks_lifecycle() {
        let data_dir = temp_dir("status");
        let account_dir = data_dir.join("accounts").join("lk-test");
        let mut state = crate::state::EngineState::load(&data_dir).expect("state");
        state
            .add_account(
                "+15551234567".to_owned(),
                Some("accounts/lk-test".to_owned()),
            )
            .expect("account registered");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = mpsc::unbounded_channel::<crate::OutboundFrame>();
        let service = super::super::Service::new(data_dir.clone(), state, outbound);

        // idle 面：无任务、无工件、无快照。
        let idle = service
            .backup_export_status("+15551234567")
            .await
            .expect("status for registered account");
        assert_eq!(idle["state"], json!("idle"));
        assert_eq!(idle["account"], json!("+15551234567"));
        assert_eq!(idle["artifactPresent"], json!(false));
        assert_eq!(idle["snapshots"], json!([]));
        assert!(idle.get("startedAtMs").is_none(), "honest absence on idle");
        assert!(idle.get("error").is_none());

        // 未注册账号 -32603 面。
        assert!(service.backup_export_status("+15559999999").await.is_err());

        // 经注册表直接启动（presage 面由独立测试覆盖），轮询状态面到终态。
        write_artifact(&account_dir, &small_lines());
        launch_export_task(
            &service.backup_tasks,
            "+15551234567".to_owned(),
            account_dir.clone(),
            params_at(BACKUP_TIME_MS),
        )
        .expect("task starts");
        let mut payload = None;
        for _ in 0..600 {
            let status = service
                .backup_export_status("+15551234567")
                .await
                .expect("status");
            if status["state"] == json!("completed") || status["state"] == json!("failed") {
                payload = Some(status);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let payload = payload.expect("reaches terminal state via status polling");
        assert_eq!(payload["state"], json!("completed"));
        assert_eq!(payload["artifactPresent"], json!(false));
        assert_eq!(payload["lines"], json!(4));
        assert_eq!(payload["frames"], json!(7));
        assert!(payload["startedAtMs"].is_u64());
        assert!(payload["finishedAtMs"].is_u64());
        assert_eq!(payload["snapshot"]["frames"], payload["frames"]);
        assert!(payload["snapshot"]["name"]
            .as_str()
            .expect("snapshot name")
            .starts_with("signal-backup-"));
        // snapshots[0] = 本进程快照，frames 已知。
        assert_eq!(payload["snapshots"][0]["frames"], payload["frames"]);

        std::fs::remove_dir_all(&data_dir).expect("cleanup");
    }

    /// presage AEP 全链（契约 §10.5 主路径）：store 里落 AEP + registration
    /// data → `backup.export` 读出并派生 → 快照可被同材料派生钥的官方
    /// reader 校验。密钥全程不出进程（本测试只经盘上密文间接验证）。
    #[tokio::test]
    async fn backup_export_reads_aep_from_presage_store() {
        use libsignal_account_keys::AccountEntropyPool;
        use libsignal_message_backup::backup::Purpose;
        use libsignal_message_backup::frame::CursorFactory;
        use libsignal_message_backup::BackupReader;
        use presage::libsignal_service::prelude::Uuid;
        use presage::libsignal_service::protocol::Aci;

        // AEP = 官方 key.rs 已知向量（keys.rs 同源）；ACI 用测试注册身份，
        // 与 keys.rs 金样 ACI 不同——证明派生链对任意 (AEP, ACI) 对成立。
        const TEST_AEP: &str = "dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l";
        const TEST_ACI: &str = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

        let data_dir = temp_dir("presage");
        let account_dir = data_dir.join("accounts").join("lk-test");
        let mut state = crate::state::EngineState::load(&data_dir).expect("state");
        state
            .add_account(
                "+15551234567".to_owned(),
                Some("accounts/lk-test".to_owned()),
            )
            .expect("account registered");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = mpsc::unbounded_channel::<crate::OutboundFrame>();
        let service = super::super::Service::new(data_dir.clone(), state, outbound);

        // 注册数据 + AEP 落 store（serde 数据面 = 官方 store 落库格式，
        // service/mod.rs §S2 roundtrip 锁定的形状）。
        let e164 = "+15551234567".try_into_e164().expect("e164");
        let phone_number = phonenumber_from_signal(&e164);
        let registration: RegistrationData = serde_json::from_str(
            &json!({
                "signal_servers": "Production",
                "device_name": "KT Signal Engine",
                "phone_number": serde_json::to_value(&phone_number).expect("phone serializes"),
                "uuid": TEST_ACI,
                "pni": "7b3dc19e-11f2-4c5a-9d0e-2f6a8b4c1d3e",
                "password": "abcdefghijklmnopqrstuvwx",
                "device_id": Some(2u32),
                "registration_id": 1234u32,
                "pni_registration_id": Some(5678u32),
                "profile_key": base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
            })
            .to_string(),
        )
        .expect("registration data parses");
        let aep: AccountEntropyPool = TEST_AEP.parse().expect("valid AEP");

        {
            let mut store = super::super::open_store(&account_dir)
                .await
                .expect("store opens");
            store
                .store_account_entropy_pool(Some(&aep))
                .await
                .expect("aep stored");
            store
                .save_registration_data(&registration)
                .await
                .expect("registration stored");
        }
        // 旧 store 连接完全释放后再让导出面开新连接（sqlite 文件锁让路）。
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 无工件先证 MISSING 面（AEP 在场也拦在工件闸）。
        assert!(matches!(
            service.backup_export("+15551234567").await,
            Err(BackupExportRejection::Structured("BACKUP_ARTIFACT_MISSING"))
        ));

        write_artifact(&account_dir, &small_lines());
        service
            .backup_export("+15551234567")
            .await
            .expect("export starts");

        let payload = {
            let mut payload = None;
            for _ in 0..600 {
                let status = service
                    .backup_export_status("+15551234567")
                    .await
                    .expect("status");
                if status["state"] == json!("completed") || status["state"] == json!("failed") {
                    payload = Some(status);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            payload.expect("reaches terminal state via status polling")
        };
        assert_eq!(payload["state"], json!("completed"), "payload: {payload}");
        let snapshot_name = payload["snapshot"]["name"]
            .as_str()
            .expect("snapshot name recorded")
            .to_owned();

        // 恢复面同构验证：同 store 材料（AEP + 注册 ACI）派生钥必须解开
        // 快照——AEP 若没有真的从 presage store 读出，派生钥对不上密文。
        let keys = derive_keys(&aep, &Aci::from(Uuid::parse_str(TEST_ACI).expect("aci")));
        let archive = std::fs::read(
            account_dir
                .join(SNAPSHOT_ROOT)
                .join(&snapshot_name)
                .join("main"),
        )
        .expect("snapshot main readable");
        let reader = BackupReader::new_encrypted_compressed(
            &keys.message_key,
            CursorFactory::new(&archive),
            Purpose::RemoteBackup,
        )
        .await
        .expect("container valid");
        let result = reader.validate_all().await;
        assert!(
            result.result.is_ok(),
            "same-material keys validate the snapshot: {:?}",
            result.result.err()
        );

        std::fs::remove_dir_all(&data_dir).expect("cleanup");
    }
}
