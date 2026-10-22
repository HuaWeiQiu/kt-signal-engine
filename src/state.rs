// SPDX-License-Identifier: AGPL-3.0-only

//! 数据目录内引擎状态：`engine-` 命名空间，不与 signal-cli 布局混用
//! （compat §1）。
//!
//! P1b（v2）：账号注册表 `{number, dir}`——dir 指向 presage store 目录
//! （`accounts/lk-<hex>`，ADR 0004）；fake 模式账号 dir=None（无 store）。
//! v1（P1a 桩时代的合成号码）仅在确认 `accounts/` 下没有任何带
//! `store.db` 的目录时按旧语义重置为空；一旦存在 store 就 fail closed
//! （空表 + 清扫会删真实 store），恢复/迁移须人工处理。未知版本/坏 JSON
//! 同样 fail closed。发送调度日志纪律不变。
//!
//! 并发契约（D5，稳定性批 2026-10-20）：`EngineState` 只经
//! `Arc<AsyncMutex<EngineState>>` 访问（main/service 单实例），同一时刻只有
//! 一个 link/delete 变更在锁内执行——跨账号写天然串行。落盘固定
//! 「同目录临时文件 + rename」原子替换，临时文件名带进程内序号，即便未来
//! 出现绕过锁的调用也不会互相覆盖半写文件（最后 rename 赢，读者永远看到
//! 完整 JSON）。官方 Desktop 为单账号进程，无多账号并发对应面（`[KT 适配]`）。
//!
//! P0-B（2026-10-21，存储对账）：注册表是清扫的唯一权威，但「读不到」绝不
//! 等于「空表」。文件存在但损坏/版本未知、文件缺失但 accounts/ 下仍有带
//! `store.db` 的 `lk-*`（注册表被删/恢复不完整）、以及隔离区非空，一律
//! fail closed：`load` 返回错误、进程退出、清扫不运行。只有「文件缺失且
//! 没有任何带 store 的账号目录」才按首次运行给空表。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Map;

const STATE_FILE: &str = "engine-state.json";
const DISPATCH_LOG: &str = "engine-send-log.jsonl";
/// 注册表版本（v2 = P1b `{number, dir}`；ADR 0004）。
const STATE_VERSION: u32 = 2;

/// 账号目录 / presage store 文件名（state 加载与启动清扫共用同一判定源）。
pub(crate) const ACCOUNTS_DIR: &str = "accounts";
pub(crate) const STORE_DB_FILE: &str = "store.db";
/// 未登记凭证目录隔离区（P0-B）。位于 `accounts/_quarantine/`，不匹配
/// 清扫的 `lk-*` 直接子目录规则，二次启动不会被再次清扫。
pub(crate) const QUARANTINE_DIR: &str = "_quarantine";
/// 隔离审计日志（JSONL，一行一条；恢复指引见 type `QuarantineRecord`）。
pub(crate) const QUARANTINE_LOG: &str = "quarantine-log.jsonl";

/// 进程内临时文件序号（D5：并发 persist 各写各的 tmp，rename 原子替换）。
static TMP_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccountEntry {
    pub number: String,
    /// presage store 目录（相对 --data-dir）；fake 账号为 None。
    pub dir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Persisted {
    version: u32,
    accounts: Vec<AccountEntry>,
}

pub(crate) struct EngineState {
    data_dir: PathBuf,
    accounts: Vec<AccountEntry>,
}

impl EngineState {
    /// 载入注册表（P0-B fail closed）。
    ///
    /// - 文件存在但读失败/坏 JSON/未知版本：返回 `InvalidData`，**不**给
    ///   空表（调用方无法构造 `EngineState`，启动清扫自然不运行）；
    /// - v1 旧桩注册表：只有在 `accounts/lk-*/store.db` 全部不存在时才按
    ///   ADR 0004 重置为空；存在任一 store 同样返回错误；
    /// - 文件缺失：先看 `accounts/lk-*/store.db`。存在任一 store 即视为
    ///   「注册表被删/恢复不完整」，同样返回错误；不存在才是首次运行（空表）；
    /// - 隔离区非空：返回错误，等用户显式恢复（见 sweep 的恢复指引）。
    pub(crate) fn load(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(STATE_FILE);
        let accounts = match fs::read(&path) {
            Ok(bytes) => match parse_registry(&bytes)? {
                RegistryContents::V2(accounts) => accounts,
                RegistryContents::V1 => {
                    ensure_no_stores(data_dir, "RegistryV1WithStores", "engine-state.json is v1")?;
                    tracing::warn!(
                        cause = "RegistryV1ResetNoStores",
                        "engine-state.json is v1 with no account stores; resetting legacy stub registry"
                    );
                    Vec::new()
                }
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                ensure_no_stores(
                    data_dir,
                    "RegistryMissingWithStores",
                    "engine-state.json is missing",
                )?;
                tracing::warn!(
                    cause = "RegistryAbsentFirstRun",
                    "engine-state.json absent with no account stores; treating as first run"
                );
                Vec::new()
            }
            Err(err) => {
                tracing::error!(
                    cause = "RegistryUnreadable",
                    error = %err,
                    "engine-state.json exists but is unreadable; refusing to start (fail closed, sweep disabled)"
                );
                return Err(io::Error::new(
                    err.kind(),
                    format!("engine-state.json unreadable: {err}"),
                ));
            }
        };
        reject_quarantined(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            accounts,
        })
    }

    fn persist(&self) -> io::Result<()> {
        let path = self.data_dir.join(STATE_FILE);
        let persisted = Persisted {
            version: STATE_VERSION,
            accounts: self.accounts.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&persisted)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "serialize"))?;
        // 同目录临时文件 + rename：半写状态不可见。临时文件名带进程内序号
        // （D5）：并发调用各写各的临时文件，rename 原子替换，读者永远读到
        // 完整 JSON（不会读到交错半写）。
        let tmp = self.data_dir.join(format!(
            "{STATE_FILE}.{}.{}.tmp",
            std::process::id(),
            TMP_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        if let Err(error) = fs::write(&tmp, bytes) {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        if let Err(error) = fs::rename(&tmp, &path) {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn add_account(&mut self, number: String, dir: Option<String>) -> io::Result<()> {
        self.accounts.retain(|existing| existing.number != number);
        self.accounts.push(AccountEntry { number, dir });
        self.persist()
    }

    /// 摘除并返回被删条目（fake 账号 dir=None；真实账号由调用方清目录）。
    pub(crate) fn remove_account(&mut self, account: &str) -> io::Result<Option<AccountEntry>> {
        let removed = self
            .accounts
            .iter()
            .position(|existing| existing.number == account)
            .map(|index| self.accounts.remove(index));
        if removed.is_some() {
            self.persist()?;
        }
        Ok(removed)
    }

    pub(crate) fn accounts(&self) -> &[AccountEntry] {
        &self.accounts
    }

    /// 账号的 store 目录（相对路径）；fake 账号或未登记返回 None。
    pub(crate) fn account_dir(&self, account: &str) -> Option<String> {
        self.accounts
            .iter()
            .find(|existing| existing.number == account)
            .and_then(|entry| entry.dir.clone())
    }

    /// 发送调度日志（夹具 SEND_LOG 同纪律：逐行 JSON，供契约测试断言
    /// connector 依赖的参数形状——recipient 数组/单字符串/显式 stop 布尔）。
    /// real/fake 两模式同写：日志记录的是「引擎收到了什么调度」。
    pub(crate) fn append_dispatch(&self, method: &str, params: &Map<String, serde_json::Value>) {
        let entry = serde_json::json!({ "method": method, "params": params });
        let line = entry.to_string();
        use std::io::Write;
        let path = self.data_dir.join(DISPATCH_LOG);
        if let Ok(mut log) = fs::OpenOptions::new().create(true).append(true).open(&path) {
            let _ = writeln!(log, "{line}");
        }
    }
}

/// 注册表解析结果：v2 = 正常账号表；v1 = 旧桩注册表（是否允许重置由调用方
/// 按 `accounts/lk-*/store.db` 是否存在决定）。
enum RegistryContents {
    V2(Vec<AccountEntry>),
    V1,
}

/// 注册表解析：坏 JSON 返回 `InvalidData`（P0-B：绝不降级成空表）；v1 返回
/// legacy 标记（调用方确认无 store 后才可重置为空）；其他版本 fail closed。
fn parse_registry(bytes: &[u8]) -> io::Result<RegistryContents> {
    let persisted = serde_json::from_slice::<Persisted>(bytes).map_err(|error| {
        tracing::error!(
            cause = "RegistryCorrupt",
            error = %error,
            "engine-state.json is corrupt; refusing to start (fail closed, sweep disabled)"
        );
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("engine-state.json is corrupt: {error}"),
        )
    })?;
    match persisted.version {
        STATE_VERSION => Ok(RegistryContents::V2(persisted.accounts)),
        1 => Ok(RegistryContents::V1),
        version => {
            tracing::error!(
                cause = "RegistryVersionUnsupported",
                version,
                "engine-state.json version is unsupported; refusing to start (fail closed, sweep disabled)"
            );
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "engine-state.json version {version} is unsupported (expected {STATE_VERSION} or legacy 1)"
                ),
            ))
        }
    }
}

/// 注册表缺位/仅 v1 时：只要 `accounts/` 下仍有带 `store.db` 的 `lk-*`，
/// 就 fail closed（空表 + 清扫会删凭证）。`cause` 为结构化日志分类。
fn ensure_no_stores(data_dir: &Path, cause: &'static str, context: &str) -> io::Result<()> {
    let stores = account_store_dirs(data_dir)?;
    if stores.is_empty() {
        return Ok(());
    }
    tracing::error!(
        cause = cause,
        stores = stores.len(),
        "account stores exist while the registry cannot be trusted; refusing to start (fail closed, sweep disabled)"
    );
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{context} but {} account store {}; refusing to start without sweeping",
            stores.len(),
            if stores.len() == 1 {
                "directory exists"
            } else {
                "directories exist"
            }
        ),
    ))
}

/// `accounts/lk-*` 中带 `store.db` 的目录名（排序后返回，日志/测试确定性）。
/// 只用于「注册表缺失/仅 v1」时的恢复完整性判定；accounts/ 缺失 = 无 store；
/// 其他读取错误向上抛（无法证明安全就必须 fail closed）。
fn account_store_dirs(data_dir: &Path) -> io::Result<Vec<String>> {
    let accounts = data_dir.join(ACCOUNTS_DIR);
    let entries = match fs::read_dir(&accounts) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            tracing::error!(
                cause = "AccountsDirUnreadable",
                error = %err,
                "cannot inspect accounts dir; refusing to start (fail closed)"
            );
            return Err(err);
        }
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        // 非 UTF-8 名同样按 lossy 名参与判定（与清扫同口径）：宁可误判
        // 「可能有 store」而 fail closed，也不把它当首次运行放行清扫。
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("lk-") || !entry.file_type()?.is_dir() {
            continue;
        }
        if store_db_present(&entry.path())? {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

/// `store.db` 是否存在。探测错误向上抛（调用方必须按「可能有 store」处理）。
pub(crate) fn store_db_present(dir: &Path) -> io::Result<bool> {
    dir.join(STORE_DB_FILE).try_exists()
}

/// 隔离区里真正的 `lk-*` 目录数（不把审计日志/空时间戳桶当凭证）。
fn quarantined_store_count(data_dir: &Path) -> io::Result<usize> {
    let root = data_dir.join(ACCOUNTS_DIR).join(QUARANTINE_DIR);
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            tracing::error!(
                cause = "QuarantineUnreadable",
                error = %err,
                "cannot inspect quarantine dir; refusing to start (fail closed)"
            );
            return Err(err);
        }
    };
    let mut count = 0;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == QUARANTINE_LOG || !entry.file_type()?.is_dir() {
            continue;
        }
        if name.starts_with("lk-") {
            count += 1;
            continue;
        }
        // 时间戳桶：只数桶内真正的 lk-* 目录；空桶（rename 失败残留）不拦启动。
        for nested in fs::read_dir(entry.path())? {
            let nested = nested?;
            if nested.file_name().to_string_lossy().starts_with("lk-")
                && nested.file_type()?.is_dir()
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// 隔离区非空 = 有待用户显式恢复的凭证目录，启动 fail closed。返回错误信息
/// 指向审计日志（恢复入口），不打印账号号码/密钥。
fn reject_quarantined(data_dir: &Path) -> io::Result<()> {
    let count = quarantined_store_count(data_dir)?;
    if count == 0 {
        return Ok(());
    }
    tracing::error!(
        cause = "QuarantinePresent",
        quarantined = count,
        "quarantined account stores require explicit recovery; refusing to start (fail closed)"
    );
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "account store quarantine is not empty ({count} dir(s)); inspect {ACCOUNTS_DIR}/{QUARANTINE_DIR}/{QUARANTINE_LOG} and recover explicitly before restarting"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        EngineState, ACCOUNTS_DIR, QUARANTINE_DIR, QUARANTINE_LOG, STATE_FILE, STORE_DB_FILE,
    };
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;

    fn temp_dir(tag: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "kt-engine-state-{tag}-{}-{stamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn temp_leftovers(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .expect("read state dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect()
    }

    /// D5 落盘面：add/remove 后 engine-state.json 可重载一致，临时文件不残留。
    #[test]
    fn persist_roundtrips_and_leaves_no_temp_file() {
        let dir = temp_dir("roundtrip");
        let mut state = EngineState::load(&dir).expect("load");
        state
            .add_account("+15550000001".to_owned(), Some("accounts/a".to_owned()))
            .expect("add");
        state
            .add_account("+15550000002".to_owned(), None)
            .expect("add fake");
        assert!(dir.join(STATE_FILE).is_file());
        assert!(temp_leftovers(&dir).is_empty());

        state.remove_account("+15550000001").expect("remove");
        let reloaded = EngineState::load(&dir).expect("reload");
        assert_eq!(reloaded.accounts(), state.accounts());
        assert_eq!(reloaded.accounts().len(), 1);
        assert!(temp_leftovers(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// D5 跨账号串行：并发 link（add）与 delete（remove）都经同一把
    /// AsyncMutex 串行化；结束态与磁盘逐条一致、无半写临时文件。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_link_and_delete_are_serialized() {
        let dir = temp_dir("concurrent");
        let state = Arc::new(AsyncMutex::new(EngineState::load(&dir).expect("load")));
        let mut tasks = Vec::new();
        const WORKERS: u32 = 4;
        const ROUNDS: u32 = 25;
        for worker in 0..WORKERS {
            let state = Arc::clone(&state);
            tasks.push(tokio::spawn(async move {
                for round in 0..ROUNDS {
                    let number = format!("+1555{worker:03}{round:04}");
                    state
                        .lock()
                        .await
                        .add_account(number.clone(), None)
                        .expect("add");
                    if round % 3 == 0 {
                        state.lock().await.remove_account(&number).expect("remove");
                    }
                }
            }));
        }
        for task in tasks {
            task.await.expect("worker task");
        }

        let memory = {
            let state = state.lock().await;
            state.accounts().to_vec()
        };
        let reloaded = EngineState::load(&dir).expect("reload");
        assert_eq!(memory, reloaded.accounts(), "内存态与磁盘态逐条一致");
        // 每 worker 25 次 add、9 次 remove（round 0,3,…,24）→ 16 条/worker。
        assert_eq!(reloaded.accounts().len(), (WORKERS * 16) as usize);
        assert!(
            temp_leftovers(&dir).is_empty(),
            "并发 persist 后无半写临时文件残留"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn write_store(dir: &Path, name: &str) -> PathBuf {
        let store_dir = dir.join(ACCOUNTS_DIR).join(name);
        std::fs::create_dir_all(&store_dir).expect("store dir");
        std::fs::write(store_dir.join(STORE_DB_FILE), b"credential-bytes").expect("store db");
        store_dir
    }

    fn load_err(dir: &Path) -> std::io::Error {
        match EngineState::load(dir) {
            Ok(_) => panic!("expected EngineState::load to fail closed"),
            Err(error) => error,
        }
    }

    /// P0-B：坏 JSON 绝不能被当作空表（旧语义），必须 fail closed。
    #[test]
    fn load_fails_closed_on_corrupt_registry() {
        let dir = temp_dir("corrupt-registry");
        std::fs::write(dir.join(STATE_FILE), b"{not-json").expect("write corrupt registry");
        let err = load_err(&dir);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "cause: {err}");
        assert!(err.to_string().contains("corrupt"), "cause: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 未知版本 fail closed（绝不降级为空表）。
    #[test]
    fn load_fails_closed_on_unknown_registry_version() {
        let dir = temp_dir("version-registry");
        std::fs::write(dir.join(STATE_FILE), br#"{"version":3,"accounts":[]}"#)
            .expect("write v3 registry");
        let err = load_err(&dir);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "cause: {err}");
        assert!(err.to_string().contains("version 3"), "cause: {err}");
        assert!(err.to_string().contains("unsupported"), "cause: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// v1 旧桩注册表 + 无 store：保留 ADR 0004 的「重置为空」迁移语义。
    #[test]
    fn load_resets_v1_registry_when_no_stores_present() {
        let dir = temp_dir("v1-no-stores");
        std::fs::write(dir.join(STATE_FILE), br#"{"version":1,"accounts":[]}"#)
            .expect("write v1 registry");
        let state = EngineState::load(&dir).expect("v1 without stores can reset");
        assert!(state.accounts().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// v1 旧桩注册表 + 真实 store：空表 + 清扫会删凭证，必须 fail closed。
    #[test]
    fn load_fails_closed_when_v1_registry_has_store() {
        let dir = temp_dir("v1-with-store");
        std::fs::write(dir.join(STATE_FILE), br#"{"version":1,"accounts":[]}"#)
            .expect("write v1 registry");
        let store_dir = write_store(&dir, "lk-old-stub");
        let err = load_err(&dir);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "cause: {err}");
        assert!(err.to_string().contains("v1"), "cause: {err}");
        assert!(
            store_dir.join(STORE_DB_FILE).is_file(),
            "store 必须原样保留"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 文件存在但读失败（目录当文件 / 权限拒绝）与坏 JSON 同属 fail closed。
    #[test]
    fn load_fails_closed_when_registry_path_is_unreadable() {
        let dir = temp_dir("unreadable-registry");
        std::fs::create_dir(dir.join(STATE_FILE)).expect("registry path as dir");
        let err = load_err(&dir);
        assert!(err.to_string().contains("unreadable"), "cause: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P0-B 核心：注册表不存在但仍有带 store.db 的 lk-* = 注册表被删/恢复
    /// 不完整，fail closed；不得返回空表（否则启动清扫会删凭证）。
    #[test]
    fn load_fails_closed_when_registry_missing_but_store_present() {
        let dir = temp_dir("missing-with-store");
        let store_dir = write_store(&dir, "lk-restore-me");
        let err = load_err(&dir);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "cause: {err}");
        assert!(err.to_string().contains("missing"), "cause: {err}");
        assert!(
            store_dir.join(STORE_DB_FILE).is_file(),
            "store 必须原样保留"
        );
        assert!(
            !dir.join(ACCOUNTS_DIR).join(QUARANTINE_DIR).exists(),
            "加载阶段不得移动/隔离任何目录"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 首次运行：无注册表、无账号目录 → 正常空表启动。
    #[test]
    fn load_treats_absent_registry_without_stores_as_first_run() {
        let dir = temp_dir("first-run");
        let state = EngineState::load(&dir).expect("first run must load");
        assert!(state.accounts().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 无注册表但只有空 lk-* 目录（无 store.db）→ 仍属首次运行/可清扫面，
    /// 不因「目录存在」就 fail closed。
    #[test]
    fn load_ignores_empty_lk_dirs_when_registry_absent() {
        let dir = temp_dir("missing-empty-lk");
        std::fs::create_dir_all(dir.join(ACCOUNTS_DIR).join("lk-empty")).expect("empty lk dir");
        let state = EngineState::load(&dir).expect("empty lk dir is not a credential");
        assert!(state.accounts().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 隔离区非空 = 有待显式恢复的凭证：启动 fail closed，且不移动/删除。
    #[test]
    fn load_fails_closed_when_quarantine_has_entries() {
        let dir = temp_dir("quarantine-present");
        let quarantined = dir
            .join(ACCOUNTS_DIR)
            .join(QUARANTINE_DIR)
            .join("1700000000000")
            .join("lk-old");
        std::fs::create_dir_all(&quarantined).expect("quarantine dir");
        std::fs::write(quarantined.join(STORE_DB_FILE), b"credential-bytes").expect("store db");
        let err = load_err(&dir);
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "cause: {err}");
        assert!(err.to_string().contains("quarantine"), "cause: {err}");
        assert!(
            quarantined.join(STORE_DB_FILE).is_file(),
            "隔离内容必须保留"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 空时间戳桶与审计日志不算「有隔离凭证」：用户恢复/放弃后可正常启动。
    #[test]
    fn load_ignores_empty_quarantine_bucket_and_manifest() {
        let dir = temp_dir("quarantine-empty");
        let root = dir.join(ACCOUNTS_DIR).join(QUARANTINE_DIR);
        std::fs::create_dir_all(root.join("1700000000000")).expect("empty bucket");
        std::fs::write(root.join(QUARANTINE_LOG), b"").expect("manifest");
        let state = EngineState::load(&dir).expect("empty bucket/manifest must not block startup");
        assert!(state.accounts().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
