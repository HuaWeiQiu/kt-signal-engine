// SPDX-License-Identifier: AGPL-3.0-only

//! 本地备份密钥派生（`docs/backup-export-contract.md` §3）。
//!
//! 全部走官方 pub API（libsignal v0.99.0，本仓钉定），不自实现密码学：
//! `BackupKey::derive_from_account_entropy_pool` → `derive_backup_id(aci)` →
//! `MessageBackupKey::derive(.., None)`（local/linking 变体，DST
//! `20241007_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:`，无 forward-secrecy
//! token）+ `derive_local_backup_metadata_key()`（metadata 文件 backupId
//! 加密钥，info `20241011_SIGNAL_LOCAL_BACKUP_METADATA_KEY`）。
//!
//! AEP 是账号材料：链接时经 provisioning 落 presage store kv，导出过程内存
//! 即用即弃；调用方亦可显式注入（connector 消费批可不经 presage 供给）。

use libsignal_account_keys::{AccountEntropyPool, BackupId, BackupKey};
use libsignal_message_backup::key::MessageBackupKey;
use presage::libsignal_service::protocol::Aci;

/// metadata 文件 `backupId` 加密钥长度（官方 `LOCAL_BACKUP_METADATA_KEY_LEN`）。
pub const METADATA_KEY_LEN: usize = 32;

/// 一套完整的本地备份密钥材料（内存持有，导出完成即弃，不落任何存储）。
#[derive(Debug)]
pub struct BackupKeys {
    /// 根密钥（恢复批的 backupId 重派生/校验面需要）；导出链只用派生产物。
    // 恢复端对齐面，恢复批接线后摘除（当前仅测试使用）。
    #[allow(dead_code)]
    pub backup_key: BackupKey,
    pub backup_id: BackupId,
    pub message_key: MessageBackupKey,
    pub metadata_key: [u8; METADATA_KEY_LEN],
    /// 本机账号 ACI（derive_keys 原始输入随件保存）：帧投影群变更 self-invite/
    /// pending-remove 的 serviceId 面需要，快照导入入口不另带身份参数。
    pub aci: presage::libsignal_service::prelude::Uuid,
}

/// AEP + ACI → 全套备份密钥（官方派生链单点封装）。
pub fn derive_keys(aep: &AccountEntropyPool, aci: &Aci) -> BackupKeys {
    let backup_key = BackupKey::derive_from_account_entropy_pool(aep);
    let backup_id = backup_key.derive_backup_id(aci);
    let message_key = MessageBackupKey::derive(&backup_key, &backup_id, None);
    let metadata_key = backup_key.derive_local_backup_metadata_key();
    let aci_uuid: presage::libsignal_service::prelude::Uuid = (*aci).into();
    BackupKeys {
        backup_key,
        backup_id,
        message_key,
        metadata_key,
        aci: aci_uuid,
    }
}

/// 官方 key.rs 已知向量（v0.99.0 `FAKE_MESSAGE_BACKUP_KEY_LEGACY` 路径）派生的
/// 全套密钥，供 container/frames/export 测试共用（单点维护向量）。
#[cfg(test)]
pub(crate) fn tests_official_keys() -> BackupKeys {
    const OFFICIAL_AEP: &str = "dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l";
    const OFFICIAL_ACI_BYTES: [u8; 16] = [
        0x65, 0x9a, 0xa5, 0xf4, 0xa2, 0x8d, 0xfc, 0xc1, 0x1e, 0xa1, 0xb9, 0x97, 0x53, 0x7a, 0x3d,
        0x95,
    ];
    let aep: AccountEntropyPool = OFFICIAL_AEP.parse().expect("valid AEP");
    let aci = Aci::from_uuid_bytes(OFFICIAL_ACI_BYTES);
    derive_keys(&aep, &aci)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 官方 key.rs 已知向量（libsignal v0.99.0 message-backup key.rs，
    /// `FAKE_ACCOUNT_ENTROPY_POOL`/`FAKE_ACI`/`FAKE_MESSAGE_BACKUP_KEY_LEGACY`，
    /// Generated from AccountEntropyPool::generate，verified in
    /// message_backup_key_legacy）。
    const OFFICIAL_AEP: &str = "dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l";
    const OFFICIAL_ACI_BYTES: [u8; 16] = [
        0x65, 0x9a, 0xa5, 0xf4, 0xa2, 0x8d, 0xfc, 0xc1, 0x1e, 0xa1, 0xb9, 0x97, 0x53, 0x7a, 0x3d,
        0x95,
    ];
    const OFFICIAL_HMAC_KEY_HEX: &str =
        "f425e22a607c529717e1e1b29f9fe139f9d1c7e7d01e371c7753c544a3026649";
    const OFFICIAL_AES_KEY_HEX: &str =
        "e143f4ad5668d8bfed2f88562f0693f53bda2c0e55c9d71730f30e24695fd6df";

    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
            .collect()
    }

    fn official_keys() -> BackupKeys {
        let aep: AccountEntropyPool = OFFICIAL_AEP.parse().expect("valid AEP");
        let aci = Aci::from_uuid_bytes(OFFICIAL_ACI_BYTES);
        derive_keys(&aep, &aci)
    }

    #[test]
    fn derives_official_known_message_backup_key_legacy_vector() {
        let keys = official_keys();
        assert_eq!(hex_decode(OFFICIAL_HMAC_KEY_HEX), keys.message_key.hmac_key);
        assert_eq!(hex_decode(OFFICIAL_AES_KEY_HEX), keys.message_key.aes_key);
    }

    #[test]
    fn metadata_key_is_deterministic_and_distinct() {
        let a = official_keys();
        let b = official_keys();
        assert_eq!(a.metadata_key, b.metadata_key);
        assert_eq!(a.backup_id.0, b.backup_id.0);
        assert_ne!(a.metadata_key, a.message_key.hmac_key);
        assert_ne!(a.metadata_key, a.message_key.aes_key);
    }
}
