// SPDX-License-Identifier: AGPL-3.0-only

//! 官方快照导入任务面（`docs/backup-export-contract.md` §11）：每账号后台
//! 导入任务注册表 + `backup.importSnapshot`/`backup.importSnapshotStatus`
//! 语义（镜像 `backup_task.rs` 导出任务范式）。
//!
//! 执行机制：metadata 校验（CTR 16B，微秒级）在 start 校验序内同步完成——
//! 目录/metadata 缺、形状不合规、错钥在启动当场答结构化码，不空转；main 的
//! 解密+分帧+投影是 async 非 Send 面（`FramesReader` 内部 Rc），`spawn_local`
//! 承载（与接收管线/transfer 下载同策略；dispatch 本就跑在 LocalSet 内）。
//! 注册表纯进程内存（引擎重启归 idle，NDJSON 在盘上）。
//!
//! NDJSON 产物与存量历史迁移下载（`download_transfer_archive`）同一落点：
//! `<account_dir>/history-import.ndjson`（connector `run_import` 消费面）。
//! 落盘用临时名 + rename 原子替换（POSIX rename 不打断并发读）；任务失败删
//! 半成品、既有 NDJSON 不动——失败形态即 NDJSON 缺席或旧内容，消费方按
//! 文件存在性判定导入结果。快照目录路径由调用方（connector）传入，引擎
//! 不持久化该路径。
//!
//! 纪律：备份密钥绝不离开引擎进程（§10.5）——密钥材料只活在任务 future 内，
//! 任务结束即 drop，不落日志不落盘。

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use presage::store::StateStore as _;
use serde_json::{json, Value};

use crate::backup::BackupKeys;
use crate::backup::{
    derive_keys, export_snapshot_frames, verify_snapshot_metadata, SnapshotImportError,
};

/// NDJSON 落盘文件名（connector `ARCHIVE_FILE_NAME` 消费面同字面量）。
const HISTORY_IMPORT_FILE_NAME: &str = "history-import.ndjson";
/// 临时名后缀（写全 → rename 原子替换）。
const HISTORY_IMPORT_TMP_SUFFIX: &str = "-tmp";

/// 任务注册表（引擎进程内存；键 = 账号号码）。
pub(crate) type SnapshotImportRegistry = StdMutex<HashMap<String, SnapshotImportRecord>>;

/// 任务状态机：`running → completed | failed`；idle = 无记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotImportState {
    Running,
    Completed,
    Failed,
}

impl SnapshotImportState {
    fn as_str(self) -> &'static str {
        match self {
            SnapshotImportState::Running => "running",
            SnapshotImportState::Completed => "completed",
            SnapshotImportState::Failed => "failed",
        }
    }
}

/// 单账号任务记录（importSnapshotStatus 的全部状态来源）。
#[derive(Debug, Clone)]
pub(crate) struct SnapshotImportRecord {
    pub(crate) state: SnapshotImportState,
    pub(crate) started_at_ms: u64,
    pub(crate) finished_at_ms: Option<u64>,
    /// NDJSON 投影行数（recipient+chat+message；成功面诚实计数）。
    pub(crate) lines: Option<u64>,
    /// 档案总帧数（含 BackupInfo 元帧）。
    pub(crate) frames: Option<u64>,
    /// 结构化错误码（仅 failed 态）。
    pub(crate) error_code: Option<&'static str>,
}

/// `backup.importSnapshot` 拒绝面：参数/账号/AEP 面一律 `-32603`，结构化码
/// 走 `-1` + message（与 backup.export 同款双轨）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotImportRejection {
    Structured(&'static str),
    Internal,
}

/// 快照导入错误 → 结构化码（§11 五码位；start 校验序与任务体两处共用）。
fn snapshot_import_error_code(error: &SnapshotImportError) -> &'static str {
    match error {
        SnapshotImportError::SnapshotMissing(_) => "BACKUP_SNAPSHOT_MISSING",
        SnapshotImportError::MetadataInvalid(_) => "BACKUP_SNAPSHOT_METADATA_INVALID",
        SnapshotImportError::KeyMismatch => "BACKUP_SNAPSHOT_KEY_MISMATCH",
        SnapshotImportError::Io(_) | SnapshotImportError::Frames(_) => "BACKUP_SNAPSHOT_IO_FAILED",
    }
}

/// launch：在跑闸（`BACKUP_IMPORT_ALREADY_RUNNING`）与注册+任务启动在同一
/// 注册表锁段内完成（并发 start 只有一个赢家）。任务体 spawn_local（帧导出
/// future 非 Send），只会在 LocalSet 上下文外 panic（装配保证不发生）。
fn launch_import_task(
    registry: &Arc<SnapshotImportRegistry>,
    account: String,
    account_dir: PathBuf,
    snapshot_dir: PathBuf,
    keys: BackupKeys,
) -> Result<(), SnapshotImportRejection> {
    {
        let mut guard = registry.lock().expect("snapshot import registry lock");
        if guard
            .get(&account)
            .map(|record| record.state == SnapshotImportState::Running)
            .unwrap_or(false)
        {
            return Err(SnapshotImportRejection::Structured(
                "BACKUP_IMPORT_ALREADY_RUNNING",
            ));
        }
        guard.insert(
            account.clone(),
            SnapshotImportRecord {
                state: SnapshotImportState::Running,
                started_at_ms: now_ms(),
                finished_at_ms: None,
                lines: None,
                frames: None,
                error_code: None,
            },
        );
    }
    tokio::task::spawn_local(run_import_task(
        Arc::clone(registry),
        account,
        account_dir,
        snapshot_dir,
        keys,
    ));
    Ok(())
}

/// 任务体：metadata 已在 start 校验序过闸，此处只做 main 全链（解密 → 分帧
/// → 投影 NDJSON → 原子落盘）→ 回写注册表终态。
async fn run_import_task(
    registry: Arc<SnapshotImportRegistry>,
    account: String,
    account_dir: PathBuf,
    snapshot_dir: PathBuf,
    keys: BackupKeys,
) {
    let tmp_path = account_dir.join(format!(
        "{HISTORY_IMPORT_FILE_NAME}{HISTORY_IMPORT_TMP_SUFFIX}"
    ));
    let final_path = account_dir.join(HISTORY_IMPORT_FILE_NAME);

    let outcome = async {
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::new(file);
        let stats = export_snapshot_frames(&snapshot_dir, &keys, &mut writer).await?;
        writer.flush()?;
        // 全部成功才原子替换（失败面既有 NDJSON 不动，半成品当场删）。
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(stats)
    }
    .await;

    let stats = match outcome {
        Ok(stats) => stats,
        Err(error) => {
            let _ = std::fs::remove_file(&tmp_path);
            let mut guard = registry.lock().expect("snapshot import registry lock");
            if let Some(record) = guard.get_mut(&account) {
                record.state = SnapshotImportState::Failed;
                record.finished_at_ms = Some(now_ms());
                record.lines = None;
                record.frames = None;
                record.error_code = Some(snapshot_import_error_code(&error));
            }
            return;
        }
    };

    let mut guard = registry.lock().expect("snapshot import registry lock");
    if let Some(record) = guard.get_mut(&account) {
        record.state = SnapshotImportState::Completed;
        record.finished_at_ms = Some(now_ms());
        record.lines = Some(stats.recipients + stats.chats + stats.messages);
        record.frames = Some(stats.frames);
        record.error_code = None;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis()
        .try_into()
        .expect("millis fit u64")
}

// ---------------------------------------------------------------------------
// Service RPC 语义（实现挂在 Service 上；本模块可见其私有字段）
// ---------------------------------------------------------------------------

impl super::Service {
    /// `backup.importSnapshot`（§11 校验序）：账号已注册 → AEP/ACI 可读
    /// （密钥派生在 LocalSet）→ metadata 快校验（同步，错误当场答结构化码）
    /// → 在跑闸 + spawn_local 任务。成功即后台执行，调用方回
    /// `{"status": "started"}`。
    pub(crate) async fn backup_import_snapshot(
        &self,
        account: &str,
        snapshot_dir: &Path,
    ) -> Result<(), SnapshotImportRejection> {
        // ① 账号已注册（注册表有 store 目录；fake/未注册 -32603）。
        let account_dir = {
            let state = self.state.lock().await;
            let rel = state
                .account_dir(account)
                .ok_or(SnapshotImportRejection::Internal)?;
            self.data_dir.join(rel)
        };
        // ② AEP/ACI（presage store kv + registration data）→ 派生（LocalSet
        //    内完成）。缺 AEP 导入不可为，-32603。
        let store = super::open_store(&account_dir)
            .await
            .map_err(|_| SnapshotImportRejection::Internal)?;
        let account_entropy_pool = store
            .fetch_account_entropy_pool()
            .await
            .map_err(|_| SnapshotImportRejection::Internal)?
            .ok_or(SnapshotImportRejection::Internal)?;
        let registration = store
            .load_registration_data()
            .await
            .map_err(|_| SnapshotImportRejection::Internal)?
            .ok_or(SnapshotImportRejection::Internal)?;
        let aci = presage::libsignal_service::protocol::Aci::from(registration.service_ids.aci);
        let keys = derive_keys(&account_entropy_pool, &aci);

        // ③ metadata 快校验（官方 verifyLocalBackupMetadata 落法）：目录/
        //    metadata/main 缺、形状不合规、错钥（非本账号备份）在此当场短路。
        verify_snapshot_metadata(snapshot_dir, &keys).map_err(|error| {
            SnapshotImportRejection::Structured(snapshot_import_error_code(&error))
        })?;

        // ④ 在跑闸 + 注册 + spawn_local 任务（原子段在注册表锁内）。
        launch_import_task(
            &self.snapshot_imports,
            account.to_owned(),
            account_dir,
            snapshot_dir.to_path_buf(),
            keys,
        )
    }

    /// `backup.importSnapshotStatus`（§11）：注册表状态 + NDJSON 在场面。
    /// 未注册账号 -32603（`Err(())`）。
    pub(crate) async fn backup_import_snapshot_status(&self, account: &str) -> Result<Value, ()> {
        let account_dir = {
            let state = self.state.lock().await;
            let rel = state.account_dir(account).ok_or(())?;
            self.data_dir.join(rel)
        };
        let record = self
            .snapshot_imports
            .lock()
            .expect("snapshot import registry lock")
            .get(account)
            .cloned();
        let ndjson_present = account_dir.join(HISTORY_IMPORT_FILE_NAME).is_file();

        let mut payload = json!({
            "account": account,
            "state": record
                .as_ref()
                .map(|record| record.state.as_str())
                .unwrap_or("idle"),
            "ndjsonPresent": ndjson_present,
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
            if let Some(error_code) = record.error_code {
                payload["error"] = json!(error_code);
            }
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use base64::Engine as _;
    use presage::libsignal_service::prelude::Uuid;
    use presage::libsignal_service::protocol::Aci;
    use presage::libsignal_service::utils::{phonenumber_from_signal, TryIntoE164};
    use presage::manager::RegistrationData;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::backup::{export_local_backup, LocalBackupParams};

    const TEST_AEP: &str = "dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l";
    const TEST_ACI: &str = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

    fn temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-engine-snapshot-import-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    fn test_keys() -> BackupKeys {
        let aep: libsignal_account_keys::AccountEntropyPool = TEST_AEP.parse().expect("AEP");
        let aci = Aci::from(Uuid::parse_str(TEST_ACI).expect("aci"));
        derive_keys(&aep, &aci)
    }

    /// 官方布局真快照（export 通路产出；keys 按值注入，最小内容集）。
    fn export_official_snapshot(root: &Path, keys: BackupKeys) -> PathBuf {
        let params = LocalBackupParams {
            keys,
            backup_time_ms: 1_760_000_000_000,
            media_root_backup_key: [0x42; 32],
            app_version: "kt-signal-engine 0.1.0".into(),
        };
        let report = export_local_backup(root, &params, |out| {
            out.account_data(crate::proto::AccountData {
                profile_key: vec![0x11; 32],
                given_name: "Kit".into(),
                ..Default::default()
            })?;
            let contact_id = out.contact_recipient(crate::proto::Contact {
                aci: Some([0xaa; 16].to_vec()),
                e164: Some(15_555_550_100),
                profile_key: Some(vec![7; 32]),
                profile_given_name: Some("Ada".into()),
                registration: Some(crate::proto::contact::Registration::Registered(
                    crate::proto::contact::Registered {},
                )),
                ..Default::default()
            })?;
            let chat_id = out.chat(contact_id)?;
            out.chat_item_text(
                chat_id,
                contact_id,
                1_760_000_001_000,
                crate::backup::ChatItemDirection::Incoming {
                    date_received_ms: 1_760_000_001_005,
                    read: true,
                },
                "hello from contact",
            )?;
            Ok(())
        })
        .expect("official export");
        report.snapshot_dir
    }

    fn spawn_registry() -> Arc<SnapshotImportRegistry> {
        Arc::new(StdMutex::new(HashMap::new()))
    }

    fn wait_terminal(
        registry: &Arc<SnapshotImportRegistry>,
        account: &str,
    ) -> SnapshotImportRecord {
        let mut polls = 0u64;
        for _ in 0..6000 {
            polls += 1;
            if let Some(record) = registry.lock().expect("registry lock").get(account) {
                if record.state != SnapshotImportState::Running {
                    return record.clone();
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("import task did not reach a terminal state ({polls} polls)");
    }

    /// 任务体直跑：官方快照 → NDJSON 落盘 + completed（lines/frames 诚实，
    /// 无 -tmp 残留，既有同位 NDJSON 被原子替换）。
    #[tokio::test]
    async fn import_task_completes_and_atomically_replaces_ndjson() {
        let root = temp_dir("task-ok");
        let account_dir = root.join("account");
        std::fs::create_dir_all(&account_dir).expect("account dir");
        // 既有 NDJSON（上一次导入遗留）：本次必须原子替换而非追加。
        std::fs::write(account_dir.join(HISTORY_IMPORT_FILE_NAME), "stale\n")
            .expect("stale ndjson");

        let snapshot_dir = export_official_snapshot(&root, test_keys());
        let registry = spawn_registry();
        registry.lock().expect("lock").insert(
            "acct".into(),
            SnapshotImportRecord {
                state: SnapshotImportState::Running,
                started_at_ms: now_ms(),
                finished_at_ms: None,
                lines: None,
                frames: None,
                error_code: None,
            },
        );

        run_import_task(
            Arc::clone(&registry),
            "acct".into(),
            account_dir.clone(),
            snapshot_dir,
            test_keys(),
        )
        .await;

        let record = registry.lock().expect("lock").get("acct").cloned().unwrap();
        assert_eq!(record.state, SnapshotImportState::Completed);
        assert_eq!(record.lines, Some(3), "recipient+chat+message rows");
        assert_eq!(record.frames, Some(5), "BackupInfo+AccountData+R+C+Item");
        assert_eq!(record.error_code, None);
        assert!(record.finished_at_ms.is_some());

        let ndjson = std::fs::read_to_string(account_dir.join(HISTORY_IMPORT_FILE_NAME))
            .expect("ndjson readable");
        assert!(!ndjson.contains("stale"), "old content replaced");
        assert_eq!(ndjson.lines().count(), 3);
        assert!(
            !account_dir
                .join(format!(
                    "{HISTORY_IMPORT_FILE_NAME}{HISTORY_IMPORT_TMP_SUFFIX}"
                ))
                .exists(),
            "no tmp residue"
        );

        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    /// 篡改 main → failed（BACKUP_SNAPSHOT_IO_FAILED），半成品删、既有
    /// NDJSON 原样（失败面旧导入物不被破坏）。
    #[tokio::test]
    async fn task_failure_cleans_tmp_and_keeps_existing_ndjson() {
        let root = temp_dir("task-fail");
        let account_dir = root.join("account");
        std::fs::create_dir_all(&account_dir).expect("account dir");
        let existing = account_dir.join(HISTORY_IMPORT_FILE_NAME);
        std::fs::write(&existing, "previous-good\n").expect("existing ndjson");

        let snapshot_dir = export_official_snapshot(&root, test_keys());
        let mut main = std::fs::read(snapshot_dir.join("main")).expect("read main");
        let mid = main.len() / 2;
        main[mid] ^= 0xff;
        std::fs::write(snapshot_dir.join("main"), &main).expect("tamper main");

        let registry = spawn_registry();
        registry.lock().expect("lock").insert(
            "acct".into(),
            SnapshotImportRecord {
                state: SnapshotImportState::Running,
                started_at_ms: now_ms(),
                finished_at_ms: None,
                lines: None,
                frames: None,
                error_code: None,
            },
        );

        run_import_task(
            Arc::clone(&registry),
            "acct".into(),
            account_dir.clone(),
            snapshot_dir,
            test_keys(),
        )
        .await;

        let record = registry.lock().expect("lock").get("acct").cloned().unwrap();
        assert_eq!(record.state, SnapshotImportState::Failed);
        assert_eq!(record.error_code, Some("BACKUP_SNAPSHOT_IO_FAILED"));
        assert_eq!(record.lines, None, "honest absence on failure");
        assert_eq!(
            std::fs::read_to_string(&existing).expect("existing intact"),
            "previous-good\n",
            "existing ndjson untouched"
        );
        assert!(
            !account_dir
                .join(format!(
                    "{HISTORY_IMPORT_FILE_NAME}{HISTORY_IMPORT_TMP_SUFFIX}"
                ))
                .exists(),
            "tmp removed"
        );

        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    /// launch 闸：LocalSet 内同步序列（launch1 注册 Running → launch2），
    /// 第二次必 AlreadyRunning 且不产生第二条记录。
    #[tokio::test]
    async fn launch_gate_rejects_concurrent_start() {
        let root = temp_dir("gate");
        let account_dir = root.join("account");
        std::fs::create_dir_all(&account_dir).expect("account dir");
        let snapshot_dir = export_official_snapshot(&root, test_keys());
        let registry = spawn_registry();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                launch_import_task(
                    &registry,
                    "acct".into(),
                    account_dir.clone(),
                    snapshot_dir.clone(),
                    test_keys(),
                )
                .expect("first launch");
                // 同步段（未让出调度）：spawn 的任务还没跑，闸必然在。
                let second = launch_import_task(
                    &registry,
                    "acct".into(),
                    account_dir,
                    snapshot_dir,
                    test_keys(),
                );
                assert_eq!(
                    second,
                    Err(SnapshotImportRejection::Structured(
                        "BACKUP_IMPORT_ALREADY_RUNNING"
                    ))
                );
                assert_eq!(
                    registry.lock().expect("lock").len(),
                    1,
                    "rejected launch does not duplicate the record"
                );
                // run_until 内让出调度，spawn_local 的任务在 sleep await 点
                // 得以推进到终态（run_until 返回后挂起任务不再被 poll）。
                loop {
                    let state = registry
                        .lock()
                        .expect("lock")
                        .get("acct")
                        .map(|record| record.state);
                    if matches!(
                        state,
                        Some(SnapshotImportState::Completed) | Some(SnapshotImportState::Failed)
                    ) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;

        let record = wait_terminal(&registry, "acct");
        assert_eq!(record.state, SnapshotImportState::Completed);

        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    /// Service 全链（store AEP → 派生 → metadata 闸 → 后台任务）：真快照
    /// 导入完成 + status 面轮询到 completed；未注册账号 -32603。
    #[tokio::test]
    async fn service_import_snapshot_end_to_end() {
        let data_dir = temp_dir("e2e");
        let account_dir = data_dir.join("accounts").join("lk-test");
        let mut state = crate::state::EngineState::load(&data_dir).expect("state");
        state
            .add_account(
                "+15551234567".to_owned(),
                Some("accounts/lk-test".to_owned()),
            )
            .expect("account registered");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = mpsc::unbounded_channel::<Value>();
        let service = super::super::Service::new(data_dir.clone(), state, outbound);

        // 注册数据 + AEP 落 store（backup_task.rs 同款 serde 形状）。
        {
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
            let aep: libsignal_account_keys::AccountEntropyPool =
                TEST_AEP.parse().expect("valid AEP");
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 未注册账号 -32603。
        assert_eq!(
            service
                .backup_import_snapshot("+15559999999", Path::new("/nonexistent"))
                .await,
            Err(SnapshotImportRejection::Internal)
        );

        // 官方快照（store 同材料派生）→ 导入 started → status 轮询 completed。
        let snapshot_dir = export_official_snapshot(&data_dir, test_keys());
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                service
                    .backup_import_snapshot("+15551234567", &snapshot_dir)
                    .await
                    .expect("import starts");

                let mut payload = None;
                for _ in 0..600 {
                    let status = service
                        .backup_import_snapshot_status("+15551234567")
                        .await
                        .expect("status");
                    if status["state"] == json!("completed") || status["state"] == json!("failed") {
                        payload = Some(status);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                payload.expect("reaches terminal state via status polling")
            })
            .await;

        let payload_ndjson = std::fs::read_to_string(account_dir.join(HISTORY_IMPORT_FILE_NAME))
            .expect("ndjson landed");
        assert_eq!(payload_ndjson.lines().count(), 3);

        std::fs::remove_dir_all(&data_dir).expect("cleanup");
    }

    /// metadata 闸结构化码面（经 Service，AEP 在场）：目录缺 → MISSING；
    /// 形状不合规 → METADATA_INVALID；他号快照 → KEY_MISMATCH。
    #[tokio::test]
    async fn service_metadata_gate_maps_structured_codes() {
        let data_dir = temp_dir("gate-codes");
        let account_dir = data_dir.join("accounts").join("lk-test");
        let mut state = crate::state::EngineState::load(&data_dir).expect("state");
        state
            .add_account(
                "+15551234567".to_owned(),
                Some("accounts/lk-test".to_owned()),
            )
            .expect("account registered");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = mpsc::unbounded_channel::<Value>();
        let service = super::super::Service::new(data_dir.clone(), state, outbound);

        {
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
            let aep: libsignal_account_keys::AccountEntropyPool =
                TEST_AEP.parse().expect("valid AEP");
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
        tokio::time::sleep(Duration::from_millis(100)).await;

        // ① 目录缺。
        assert_eq!(
            service
                .backup_import_snapshot("+15551234567", Path::new("/nonexistent/snapshot"))
                .await,
            Err(SnapshotImportRejection::Structured(
                "BACKUP_SNAPSHOT_MISSING"
            ))
        );

        // ② 形状不合规（version=2）。
        let bad_dir = data_dir.join("bad-snapshot");
        std::fs::create_dir_all(&bad_dir).expect("dir");
        std::fs::write(bad_dir.join("metadata"), b"\x08\x02").expect("version=2 metadata");
        assert_eq!(
            service
                .backup_import_snapshot("+15551234567", &bad_dir)
                .await,
            Err(SnapshotImportRejection::Structured(
                "BACKUP_SNAPSHOT_METADATA_INVALID"
            ))
        );

        // ③ 他号快照（同 AEP 异 ACI 派生 → backupId 不匹配）。
        let foreign_keys = {
            let aep: libsignal_account_keys::AccountEntropyPool = TEST_AEP.parse().expect("AEP");
            let other_aci = Aci::from(Uuid::from_bytes([0x77; 16]));
            derive_keys(&aep, &other_aci)
        };
        let foreign_dir = export_official_snapshot(&data_dir, foreign_keys);
        assert_eq!(
            service
                .backup_import_snapshot("+15551234567", &foreign_dir)
                .await,
            Err(SnapshotImportRejection::Structured(
                "BACKUP_SNAPSHOT_KEY_MISMATCH"
            ))
        );

        std::fs::remove_dir_all(&data_dir).expect("cleanup");
    }

    /// importSnapshotStatus idle/未注册面。
    #[tokio::test]
    async fn import_status_idle_surface() {
        let data_dir = temp_dir("status-idle");
        let account_dir = data_dir.join("accounts").join("lk-test");
        let mut state = crate::state::EngineState::load(&data_dir).expect("state");
        state
            .add_account(
                "+15551234567".to_owned(),
                Some("accounts/lk-test".to_owned()),
            )
            .expect("account registered");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = mpsc::unbounded_channel::<Value>();
        let service = super::super::Service::new(data_dir.clone(), state, outbound);

        let idle = service
            .backup_import_snapshot_status("+15551234567")
            .await
            .expect("status for registered account");
        assert_eq!(idle["state"], json!("idle"));
        assert_eq!(idle["account"], json!("+15551234567"));
        assert_eq!(idle["ndjsonPresent"], json!(false));
        assert!(idle.get("startedAtMs").is_none(), "honest absence on idle");
        assert!(idle.get("error").is_none());

        assert!(service
            .backup_import_snapshot_status("+15559999999")
            .await
            .is_err());

        // NDJSON 在场但无任务记录（引擎重启过）：state=idle、ndjsonPresent
        // 诚实为 true（盘面真值，消费方自行判定重导）。
        std::fs::create_dir_all(&account_dir).expect("account dir");
        std::fs::write(account_dir.join(HISTORY_IMPORT_FILE_NAME), "x\n").expect("ndjson");
        let idle = service
            .backup_import_snapshot_status("+15551234567")
            .await
            .expect("status");
        assert_eq!(idle["state"], json!("idle"));
        assert_eq!(idle["ndjsonPresent"], json!(true));

        std::fs::remove_dir_all(&data_dir).expect("cleanup");
    }
}
