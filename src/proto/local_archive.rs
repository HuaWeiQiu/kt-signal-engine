// SPDX-License-Identifier: AGPL-3.0-only
//
//! Android 本地备份目录 metadata 面（Signal-Android `lib/archive` 的
//! `LocalArchive.proto`，vendored 于 `protos/LocalArchive.proto`，来源钉定
//! main@b377bd213ad370dea96c46f75491b598983ebf7f，2026-10-05 取证）。
//!
//! 字段面过小（Metadata 两层四字段），以 prost-build 等价手写生成代码替代
//! 构建期生成链；字段号与 vendored proto 逐字一致，由往返单测锁定
//! （`crate::backup`）。契约见 `docs/backup-export-contract.md` §5。

#![allow(clippy::all)]
#![allow(dead_code)]

/// `Metadata`：本地备份快照目录内 `metadata` 文件的 proto 面（明文帧，非
/// 加密容器）。
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    /// 快照格式版本（官方当前 VERSION=1；恢复端要求 `<= 1`）。
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// 加密的 BackupId（仅凭 AEP 即可解出，用于密钥匹配校验）。
    #[prost(message, optional, tag = "2")]
    pub backup_id: ::core::option::Option<metadata::EncryptedBackupId>,
}

/// `Metadata.FilesFrame`：`files/` 附件索引（MVP 附件面不做，仅定义保真）。
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilesFrame {
    #[prost(oneof = "files_frame::Item", tags = "1")]
    pub item: ::core::option::Option<files_frame::Item>,
}

/// Nested message and enum types in `Metadata`.
pub mod metadata {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EncryptedBackupId {
        /// 12 bytes, randomly generated
        #[prost(bytes = "vec", tag = "1")]
        pub iv: ::prost::alloc::vec::Vec<u8>,
        /// AES-256-CTR, key = local backup metadata key, message = backup ID bytes
        #[prost(bytes = "vec", tag = "2")]
        pub encrypted_id: ::prost::alloc::vec::Vec<u8>,
    }
}

/// Nested message and enum types in `FilesFrame`.
pub mod files_frame {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Item {
        #[prost(string, tag = "1")]
        MediaName(::prost::alloc::string::String),
    }
}
