// SPDX-License-Identifier: AGPL-3.0-only

//! Android 本地备份（v2 归档）导出（`docs/backup-export-contract.md`）。
//!
//! 分层：`keys`（官方派生链）→ `frames`（帧构造 + 帧序状态机）→
//! `container`（main 加密容器流式写面）→ `export`（目录编排）。全部内部
//! Rust API（契约 §7），无 store/state 依赖：调用方注入账号材料与消息内容，
//! 引擎即用即弃、不落任何存储。

mod artifact;
mod container;
mod export;
mod frames;
pub(crate) mod keys;
pub(crate) mod receive;
pub(crate) mod transfer;

/// connector 消费批任务层（`service::backup_task`）的输入面。
pub(crate) use artifact::{assemble, validate_first_line, ARTIFACT_FILE_NAME};
pub(crate) use export::{export_local_backup, write_main_stream, LocalBackupParams};
pub(crate) use keys::{derive_keys, BackupKeys};
/// 快照导入收向面（`service::snapshot_import` 的输入面）。
pub(crate) use receive::{export_snapshot_frames, verify_snapshot_metadata, SnapshotImportError};
// 转移归档容器收向面（`transfer`，台账 §29.10.2 L4 节）：D0 真机接线批的
// 既成对接面——解析/双臂往返/拒绝语义以测试钉形在案，生产流暂走既有
// `history::export_archive`（路径入口），字节入口与双臂选择随 D0 接线
// 消费（模块级 allow 理由见 transfer.rs 头；L3 backup_api 同先例）。

#[cfg(test)]
pub(crate) use frames::ChatItemDirection;
#[cfg(test)]
pub(crate) use keys::tests_official_keys;

/// 导出面错误（目录编排层；容器/帧序/工件错误原样透传）。
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error(transparent)]
    FrameSequence(#[from] frames::FrameSequenceError),
    #[error(transparent)]
    Artifact(#[from] artifact::ArtifactError),
    #[error("snapshot directory already exists: {0}")]
    SnapshotExists(std::path::PathBuf),
    #[error("backup container io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("backup proto encode/decode error: {0}")]
    Proto(#[from] prost::EncodeError),
}
