// SPDX-License-Identifier: AGPL-3.0-only

//! 数据目录内引擎状态：`engine-` 命名空间，不与 signal-cli 布局混用
//! （compat §1）。
//!
//! P1b（v2）：账号注册表 `{number, dir}`——dir 指向 presage store 目录
//! （`accounts/lk-<hex>`，ADR 0004）；fake 模式账号 dir=None（无 store）。
//! v1（P1a 桩时代的合成号码）一律重置为空，不迁移。发送调度日志纪律不变。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Map;

const STATE_FILE: &str = "engine-state.json";
const DISPATCH_LOG: &str = "engine-send-log.jsonl";

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        // 同目录临时文件 + rename：半写状态不可见。
        let tmp = self.data_dir.join(format!("{STATE_FILE}.tmp"));
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &path)
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
