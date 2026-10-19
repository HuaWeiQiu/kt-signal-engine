// SPDX-License-Identifier: AGPL-3.0-only

//! 数据目录内引擎状态：`engine-` 命名空间，不与 signal-cli 布局混用
//! （compat §1）。
//!
//! P1b（v2）：账号注册表 `{number, dir}`——dir 指向 presage store 目录
//! （`accounts/lk-<hex>`，ADR 0004）；fake 模式账号 dir=None（无 store）。
//! v1（P1a 桩时代的合成号码）一律重置为空，不迁移。发送调度日志纪律不变。
//!
//! 并发契约（D5，稳定性批 2026-10-20）：`EngineState` 只经
//! `Arc<AsyncMutex<EngineState>>` 访问（main/service 单实例），同一时刻只有
//! 一个 link/delete 变更在锁内执行——跨账号写天然串行。落盘固定
//! 「同目录临时文件 + rename」原子替换，临时文件名带进程内序号，即便未来
//! 出现绕过锁的调用也不会互相覆盖半写文件（最后 rename 赢，读者永远看到
//! 完整 JSON）。官方 Desktop 为单账号进程，无多账号并发对应面（`[KT 适配]`）。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Map;

const STATE_FILE: &str = "engine-state.json";
const DISPATCH_LOG: &str = "engine-send-log.jsonl";

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
    pub(crate) fn load(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(STATE_FILE);
        let accounts = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Persisted>(&bytes) {
                Ok(persisted) if persisted.version == 2 => persisted.accounts,
                // v1（P1a 桩合成号码）/未知版本：重置为空（ADR 0004）。
                _ => Vec::new(),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            accounts,
        })
    }

    fn persist(&self) -> io::Result<()> {
        let path = self.data_dir.join(STATE_FILE);
        let persisted = Persisted {
            version: 2,
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

#[cfg(test)]
mod tests {
    use super::{EngineState, STATE_FILE};
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
}
