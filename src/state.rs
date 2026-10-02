// SPDX-License-Identifier: AGPL-3.0-only

//! 数据目录内引擎状态：`engine-` 命名空间，不与 signal-cli 布局混用
//! （compat §1）。P1a 承载 link/delete 生命周期与发送调度日志；P1b 接
//! presage store 后本模块收敛为生命周期包装。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Map;

const STATE_FILE: &str = "engine-state.json";
const DISPATCH_LOG: &str = "engine-send-log.jsonl";

#[derive(Debug, Serialize, Deserialize)]
struct Persisted {
    version: u32,
    accounts: Vec<String>,
}

pub struct EngineState {
    data_dir: PathBuf,
    accounts: Vec<String>,
    /// `startLink` 签发、`finishLink` 校验的进行中链接 URI（仅进程内存，
    /// 不持久化——上游链接会话同样是易失的）。
    pub pending_link_uri: Option<String>,
}

impl EngineState {
    pub fn load(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(STATE_FILE);
        let accounts = match fs::read(&path) {
            Ok(bytes) => {
                let persisted: Persisted = serde_json::from_slice(&bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "state file"))?;
                persisted.accounts
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            accounts,
            pending_link_uri: None,
        })
    }

    fn persist(&self) -> io::Result<()> {
        let path = self.data_dir.join(STATE_FILE);
        let persisted = Persisted {
            version: 1,
            accounts: self.accounts.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&persisted)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "serialize"))?;
        // 同目录临时文件 + rename：半写状态不可见。
        let tmp = self.data_dir.join(format!("{STATE_FILE}.tmp"));
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &path)
    }

    /// P1a-stub：链接号码由本模块按序生成（形状合法的 E.164 占位），
    /// P1b 由 presage provisioning 返回真实账号。
    pub fn link_new_account(&mut self) -> io::Result<String> {
        let number = format!("+1555000{:04}", self.accounts.len() + 1);
        self.accounts.push(number.clone());
        self.persist()?;
        Ok(number)
    }

    pub fn delete_account(&mut self, account: &str) -> io::Result<bool> {
        let before = self.accounts.len();
        self.accounts.retain(|existing| existing != account);
        let removed = self.accounts.len() != before;
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    pub fn accounts(&self) -> &[String] {
        &self.accounts
    }

    /// 发送调度日志（夹具 SEND_LOG 同纪律：逐行 JSON，供契约测试断言
    /// connector 依赖的参数形状——recipient 数组/单字符串/显式 stop 布尔）。
    pub fn append_dispatch(&self, method: &str, params: &Map<String, serde_json::Value>) {
        let entry = serde_json::json!({ "method": method, "params": params });
        let line = entry.to_string();
        use std::io::Write;
        let path = self.data_dir.join(DISPATCH_LOG);
        if let Ok(mut log) = fs::OpenOptions::new().create(true).append(true).open(&path) {
            let _ = writeln!(log, "{line}");
        }
    }
}
