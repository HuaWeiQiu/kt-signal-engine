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
/// 重链接取代目录的归档区（P0 重链孤儿批，2026-10-23）。位于
/// `accounts/_archive/`：既不匹配启动清扫的 `lk-*` 直接子目录规则，也不在
/// `_quarantine` 内——归档是**正常生命周期动作**（重链换新 store 目录，旧
/// 目录被取代），不该触发 fail-closed 闸门。隔离区只留给真异常残留。
pub(crate) const ARCHIVE_DIR: &str = "_archive";
/// 取代目录归档审计日志（JSONL，一行一条；凭证永不自动删除）。
pub(crate) const ARCHIVE_LOG: &str = "link-superseded-log.jsonl";

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
        self.add_account_superseding(number, dir).map(|_| ())
    }

    /// 同上，但把被同号新链接**取代**的旧条目交回调用方（P0 重链孤儿批）。
    ///
    /// 注册表按号码唯一：同号重链写入新 `{number, dir}` 时旧 `dir` 从表里
    /// 消失，旧 store 目录随即成为「未登记目录」——启动清扫按 fail-closed
    /// 把它移进 `_quarantine`，隔离区非空又让引擎拒绝启动（2026-10-23 真机：
    /// `accounts/lk-7c63e095135726e1` → quarantine-log 记
    /// `reason=unregistered_store_dir` → 桌面「通道正在自恢复」死循环）。
    /// 收尾方据此归档旧目录（`archive_superseded_account_dir`），把「正常
    /// 生命周期动作」与「真异常残留」分开。
    ///
    /// 落盘在**同一次** persist 内完成：调用方拿到的旧条目与新条目来自同一
    /// 次原子替换，不存在「新条目已落盘、旧条目还没交回」的中间态。
    pub(crate) fn add_account_superseding(
        &mut self,
        number: String,
        dir: Option<String>,
    ) -> io::Result<Option<AccountEntry>> {
        let superseded = self
            .accounts
            .iter()
            .position(|existing| existing.number == number)
            .map(|index| self.accounts.remove(index));
        self.accounts.push(AccountEntry { number, dir });
        self.persist()?;
        Ok(superseded)
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

/// 被取代账号目录的归档记录（JSONL）。只含目录名/相对路径/时间戳，
/// 不含号码或密钥——与隔离审计记录同纪律。
#[derive(serde::Serialize)]
struct SupersededRecord<'a> {
    observed_at_ms: u64,
    dir: &'a str,
    from: String,
    to: String,
    reason: &'static str,
    /// 归档区不是隔离区：引擎照常启动，本记录只是「旧凭证去哪了」的线索。
    restore: &'static str,
}

/// 归档被重链接取代的账号目录（P0 重链孤儿批）：同文件系统 rename 到
/// `accounts/_archive/<observedAtMs>/lk-<hex>`（目标名冲突加 `.1`/`.2`
/// 后缀，不覆盖已有归档），并追加一条 `link-superseded-log.jsonl`。
///
/// 纪律（与清扫同源）：
/// - **绝不删除凭证**：只 rename；任何一步失败原地保留并交回 `None`
///   （调用方 warn 落日志，下次启动清扫按既有隔离语义处理——闸门不放宽）；
/// - 只接受注册表形态的 store 目录（`accounts/<lk-*>`，恰好两级、无
///   `.`/`..`、非符号链接），其他形状一律不动；
/// - 目标目录已存在同名归档时加后缀，不覆盖既有归档内容。
///
/// 返回归档后的相对路径（`accounts/_archive/<ts>/lk-<hex>`）。
pub(crate) fn archive_superseded_account_dir(
    data_dir: &Path,
    entry: &AccountEntry,
    observed_at_ms: u64,
) -> Option<String> {
    let name = store_dir_name(entry.dir.as_deref()?)?;
    let source = data_dir.join(ACCOUNTS_DIR).join(&name);
    // 符号链接不跟随：归档会把链接目标搬走，越过数据目录的内容不归本层处置。
    match fs::symlink_metadata(&source) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        _ => return None,
    }
    let bucket = data_dir
        .join(ACCOUNTS_DIR)
        .join(ARCHIVE_DIR)
        .join(observed_at_ms.to_string());
    if fs::create_dir_all(&bucket).is_err() {
        tracing::warn!(
            cause = "SupersededArchiveFailed",
            dir = %name,
            stage = "bucket_create",
            "cannot create archive bucket; leaving superseded account directory untouched"
        );
        return None;
    }
    let destination = unique_destination_in(&bucket, &name)?;
    if let Err(error) = fs::rename(&source, &destination) {
        tracing::warn!(
            cause = "SupersededArchiveFailed",
            dir = %name,
            stage = "rename",
            error = %error,
            "archive rename failed; leaving superseded account directory untouched"
        );
        return None;
    }
    let destination_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(name.as_str());
    let relative_destination =
        format!("{ACCOUNTS_DIR}/{ARCHIVE_DIR}/{observed_at_ms}/{destination_name}");
    tracing::warn!(
        cause = "SupersededStoreArchived",
        dir = %name,
        from = %format!("{ACCOUNTS_DIR}/{name}"),
        to = %relative_destination,
        archive_log = %format!("{ACCOUNTS_DIR}/{ARCHIVE_DIR}/{ARCHIVE_LOG}"),
        "superseded account store archived by relink (not deleted, not quarantined)"
    );
    append_superseded_record(data_dir, &name, observed_at_ms, &relative_destination);
    Some(relative_destination)
}

/// 注册表 `dir` 字段 → store 目录名，只接受 `accounts/<lk-*>` 形态
/// （与清扫/隔离同一判定域；其他形状返回 None，本层不动）。
fn store_dir_name(rel: &str) -> Option<String> {
    let mut components = Path::new(rel).components();
    let accounts = components.next()?;
    let name = components.next()?;
    if components.next().is_some() {
        return None;
    }
    if accounts.as_os_str() != ACCOUNTS_DIR {
        return None;
    }
    let name = name.as_os_str().to_str()?;
    if !name.starts_with("lk-") || name.contains(['/', '\\']) || name == ".." || name == "." {
        return None;
    }
    Some(name.to_owned())
}

/// 归档目标名：原目录名优先；已存在则 `.1`/`.2`…（不覆盖既有归档）。
fn unique_destination_in(bucket: &Path, name: &str) -> Option<PathBuf> {
    const MAX_SUFFIX: u32 = 1024;
    for ordinal in 0..=MAX_SUFFIX {
        let candidate = if ordinal == 0 {
            bucket.join(name)
        } else {
            bucket.join(format!("{name}.{ordinal}"))
        };
        match candidate.try_exists() {
            Ok(false) => return Some(candidate),
            Ok(true) => continue,
            Err(_) => return None,
        }
    }
    None
}

fn append_superseded_record(data_dir: &Path, name: &str, observed_at_ms: u64, destination: &str) {
    use std::io::Write as _;
    let root = data_dir.join(ACCOUNTS_DIR).join(ARCHIVE_DIR);
    let path = root.join(ARCHIVE_LOG);
    let record = SupersededRecord {
        observed_at_ms,
        dir: name,
        from: format!("{ACCOUNTS_DIR}/{name}"),
        to: destination.to_owned(),
        reason: "superseded_by_relink",
        restore: "this directory is an archived copy of a store replaced by a relink; it is never auto-deleted and never blocks startup — move it back under accounts/ and re-add its {number,dir} entry only if you need the old credentials",
    };
    let Ok(line) = serde_json::to_string(&record) else {
        tracing::warn!(
            cause = "SupersededArchiveLogFailed",
            dir = %name,
            "archive manifest not written; archived directory remains recoverable"
        );
        return;
    };
    match fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut file) => {
            if writeln!(file, "{line}").is_err() {
                tracing::warn!(
                    cause = "SupersededArchiveLogFailed",
                    dir = %name,
                    "archive manifest not written; archived directory remains recoverable"
                );
            }
        }
        Err(_) => tracing::warn!(
            cause = "SupersededArchiveLogFailed",
            dir = %name,
            "archive manifest not written; archived directory remains recoverable"
        ),
    }
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
        archive_superseded_account_dir, AccountEntry, EngineState, ACCOUNTS_DIR, ARCHIVE_DIR,
        ARCHIVE_LOG, QUARANTINE_DIR, QUARANTINE_LOG, STATE_FILE, STORE_DB_FILE,
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

    /// P0 重链孤儿批（state 面）：同号重链交回被取代条目 + 归档后旧目录
    /// 不再是未登记目录（隔离区空、注册表可重载、凭证字节原样）。
    #[test]
    fn add_account_superseding_returns_old_entry_and_archive_clears_orphan() {
        let dir = temp_dir("relink-supersede");
        // 先 load（空目录 = 首次运行）再建 store：注册表缺失而 store 在场是
        // P0-B 的 fail-closed 面，测试造数据顺序必须避开它。
        let mut state = EngineState::load(&dir).expect("load");
        let old_store = write_store(&dir, "lk-old");
        state
            .add_account(
                "+15550000001".to_owned(),
                Some("accounts/lk-old".to_owned()),
            )
            .expect("register old");
        write_store(&dir, "lk-new");

        let superseded = state
            .add_account_superseding(
                "+15550000001".to_owned(),
                Some("accounts/lk-new".to_owned()),
            )
            .expect("relink")
            .expect("old entry handed back");
        assert_eq!(superseded.dir.as_deref(), Some("accounts/lk-old"));
        assert_eq!(state.accounts().len(), 1, "同号唯一：注册表不留旧条目");
        assert!(old_store.exists(), "交回条目时目录尚未被动过");

        let to =
            archive_superseded_account_dir(&dir, &superseded, 1_700_000_000_000).expect("archived");
        assert_eq!(
            to,
            format!("{ACCOUNTS_DIR}/{ARCHIVE_DIR}/1700000000000/lk-old")
        );
        assert!(!old_store.exists(), "归档后 accounts/lk-old 不再存在");
        assert_eq!(
            std::fs::read(dir.join(&to).join(STORE_DB_FILE)).unwrap(),
            b"credential-bytes".as_slice(),
            "归档保留凭证字节"
        );
        assert!(
            !dir.join(ACCOUNTS_DIR).join(QUARANTINE_DIR).exists(),
            "归档不进隔离区（不触发 fail-closed）"
        );
        let manifest =
            std::fs::read_to_string(dir.join(ACCOUNTS_DIR).join(ARCHIVE_DIR).join(ARCHIVE_LOG))
                .expect("archive manifest");
        let record: serde_json::Value =
            serde_json::from_str(manifest.lines().next().expect("one line")).expect("json");
        assert_eq!(record["reason"], "superseded_by_relink");
        assert_eq!(record["to"], serde_json::json!(to));

        let reloaded = EngineState::load(&dir).expect("reload after archive");
        assert_eq!(reloaded.accounts().len(), 1);
        assert_eq!(
            reloaded.account_dir("+15550000001").as_deref(),
            Some("accounts/lk-new")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 归档形状闸：非 accounts/<lk-*> 形态、目录不存在、符号链接一律不动；
    /// 同名归档目标已存在时加后缀不覆盖。
    #[test]
    fn archive_superseded_refuses_foreign_shapes_and_never_overwrites() {
        let dir = temp_dir("relink-archive-guards");
        // 非 accounts/lk-* 形态：原样不动。
        for rel in [
            "elsewhere/lk-x".to_owned(),
            "accounts/nested/lk-x".to_owned(),
            "accounts/not-lk".to_owned(),
            "accounts/../lk-escape".to_owned(),
        ] {
            let entry = AccountEntry {
                number: "+15550000002".to_owned(),
                dir: Some(rel.clone()),
            };
            assert!(
                archive_superseded_account_dir(&dir, &entry, 1).is_none(),
                "形状外的 dir 不得归档: {rel}"
            );
        }
        // 注册表形态但目录不存在：零动作（下一启动清扫自然处理）。
        let missing = AccountEntry {
            number: "+15550000003".to_owned(),
            dir: Some("accounts/lk-missing".to_owned()),
        };
        assert!(archive_superseded_account_dir(&dir, &missing, 1).is_none());
        // fake 账号（dir=None）零动作。
        let fake = AccountEntry {
            number: "+15550000004".to_owned(),
            dir: None,
        };
        assert!(archive_superseded_account_dir(&dir, &fake, 1).is_none());

        // 同名归档目标冲突：第二次归档加 .1，既有归档字节不被覆盖。
        let entry = AccountEntry {
            number: "+15550000005".to_owned(),
            dir: Some("accounts/lk-dup".to_owned()),
        };
        write_store(&dir, "lk-dup");
        let first =
            archive_superseded_account_dir(&dir, &entry, 1_700_000_000_001).expect("first archive");
        assert_eq!(
            first,
            format!("{ACCOUNTS_DIR}/{ARCHIVE_DIR}/1700000000001/lk-dup")
        );
        // 同名目录再现（第二轮重链）：目标名冲突 → 后缀，不覆盖既有归档。
        let store = write_store(&dir, "lk-dup");
        std::fs::write(store.join(STORE_DB_FILE), b"second-generation").expect("recreate store");
        let second = archive_superseded_account_dir(&dir, &entry, 1_700_000_000_001)
            .expect("second archive");
        assert_eq!(
            second,
            format!("{ACCOUNTS_DIR}/{ARCHIVE_DIR}/1700000000001/lk-dup.1")
        );
        assert_eq!(
            std::fs::read(dir.join(&first).join(STORE_DB_FILE)).unwrap(),
            b"credential-bytes".as_slice(),
            "既有归档不被覆盖"
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
