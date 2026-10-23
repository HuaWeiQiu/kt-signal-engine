// SPDX-License-Identifier: AGPL-3.0-only

//! 本地备份密钥派生（`docs/backup-export-contract.md` §3；台账 §29.10.2
//! L2 节 / §29.10.3 交付）。
//!
//! 全部走官方 pub API（libsignal v0.99.0，本仓钉定），不自实现密码学：
//! `BackupKey::derive_from_account_entropy_pool` → `derive_backup_id(aci)` →
//! `MessageBackupKey::derive(.., None)`（local/linking 变体，DST
//! `20241007_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:`，无 forward-secrecy
//! token）+ `derive_local_backup_metadata_key()`（metadata 文件 backupId
//! 加密钥，info `20241011_SIGNAL_LOCAL_BACKUP_METADATA_KEY`）。
//!
//! L2 按需派生面（台账 §29.10.2 L2 表七 info 常量，全走 vendored
//! account-keys backup.rs 现成 API，零自实现）：
//! - 容器钥双 DST：`BackupKeys::message_key`（legacy 臂）+
//!   `BackupKeys::message_key_with_token`（remote 臂 20250708 + FS token）。
//! - 转移归档根钥臂：`BackupKeys::from_raw_key`（32B 裸根——手机转移归档
//!   加密根 ephemeralKey；官方 index.preload.ts:840 `getKeyMaterial(new
//!   BackupKey(ephemeralKey))` 同一派生面只换根钥来源）。
//! - media id 族：`media_root_key`（MediaRootKey 非派生——provisioning
//!   mediaRootBackupKey field 16 / BackupInfo.mediaRootBackupKey 的 32B
//!   随机值，`new BackupKey(bytes)` 同位）→ `media_id`（15B）→
//!   `media_encryption_key_data`（64B：前 32B HMAC / 后 32B AES，
//!   crypto.preload.ts:92-120 同位）与缩略图 transit 专用 DST（20241030）。
//!
//! AEP 是账号材料：链接时经 provisioning 落 presage store kv（L1 keys sync
//! 臂 `service::aep` 同位落库），读取面 = `StateStore::fetch_account_entropy
//! _pool`（cloud_backup/snapshot_import 既有消费先例），派生过程内存即用
//! 即弃；调用方亦可显式注入（connector 消费批可不经 presage 供给）。

use libsignal_account_keys::{
    AccountEntropyPool, BackupForwardSecrecyToken, BackupId, BackupKey, MEDIA_ID_LEN,
};
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
    BackupKeys::from_raw_key(&BackupKey::derive_from_account_entropy_pool(aep).0, aci)
}

/// 媒体内容加密钥对（官方 64B 派生切分：前 32B HMAC / 后 32B AES，
/// crypto.preload.ts:92-120 同位；MessageBackupKey 同构切分）。
/// L3/L4 媒体下载/解密批接线预置面（当前仅测试消费，接线批摘除 allow）。
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct MediaEncryptionKeyData {
    pub hmac_key: [u8; 32],
    pub aes_key: [u8; 32],
}

/// 媒体根钥还原（MediaRootKey **非派生**：provisioning
/// `mediaRootBackupKey`(field 16) / 转移归档 BackupInfo.mediaRootBackupKey
/// 分发的 32B 随机值；官方 `new BackupKey(bytes)` 同位——BackupKey 类型
/// 兼任根钥容器）。L3/L4 接线预置面（当前仅测试消费，接线批摘除 allow）。
#[allow(dead_code)]
pub fn media_root_key(bytes: &[u8; 32]) -> BackupKey {
    BackupKey(*bytes)
}

/// mediaId 派生（MediaRootKey‖mediaName → 15B，info
/// `20241007_SIGNAL_BACKUP_MEDIA_ID:`；mediaName = hex(plaintextHash‖key)
/// 由调用方构形）。L3/L4 接线预置面（同上摘除纪律）。
#[allow(dead_code)]
pub fn media_id(root: &BackupKey, media_name: &str) -> [u8; MEDIA_ID_LEN] {
    root.derive_media_id(media_name)
}

/// 媒体内容钥（MediaRootKey‖mediaId → 64B 切分，info
/// `20241007_SIGNAL_BACKUP_ENCRYPT_MEDIA:`）。L3/L4 接线预置面（同上摘
/// 除纪律）。
#[allow(dead_code)]
pub fn media_encryption_key_data(
    root: &BackupKey,
    id: &[u8; MEDIA_ID_LEN],
) -> MediaEncryptionKeyData {
    split_media_key_data(root.derive_media_encryption_key_data(id))
}

/// 缩略图 transit 钥（同切分、专用 DST
/// `20241030_SIGNAL_BACKUP_ENCRYPT_THUMBNAIL:`）。L3/L4 接线预置面（同
/// 上摘除纪律）。
#[allow(dead_code)]
pub fn thumbnail_transit_encryption_key_data(
    root: &BackupKey,
    id: &[u8; MEDIA_ID_LEN],
) -> MediaEncryptionKeyData {
    split_media_key_data(root.derive_thumbnail_transit_encryption_key_data(id))
}

#[allow(dead_code)]
fn split_media_key_data(bytes: [u8; 64]) -> MediaEncryptionKeyData {
    // 官方 64B 切分位：[..32] = HMAC / [32..] = AES（derive_arrays 序 =
    // crypto.preload.ts:92-120 切面）。
    let mut hmac_key = [0u8; 32];
    let mut aes_key = [0u8; 32];
    hmac_key.copy_from_slice(&bytes[..32]);
    aes_key.copy_from_slice(&bytes[32..]);
    MediaEncryptionKeyData { hmac_key, aes_key }
}

impl BackupKeys {
    /// 转移归档根钥臂（32B 裸根 → 全套密钥；官方 index.preload.ts:840
    /// `getKeyMaterial(new BackupKey(ephemeralKey))` 同一派生面只换根钥
    /// 来源——下游 backup_id/message_key/metadata_key 派生与 AEP 臂
    /// [`derive_keys`] 完全共享）。
    pub fn from_raw_key(raw_key: &[u8; 32], aci: &Aci) -> BackupKeys {
        let backup_key = BackupKey(*raw_key);
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

    /// 容器钥 remote 臂按需派生（Some(token) → DST
    /// `20250708_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:` + 32B salt；
    /// `self.message_key` 即 legacy 臂 None → `20241007…`——双 DST 完整面，
    /// 供 SVR forward-secrecy 消费方按需取用，不落库）。L3 接线预置面
    /// （当前仅测试消费，接线批摘除 allow）。
    #[allow(dead_code)]
    pub fn message_key_with_token(&self, token: &BackupForwardSecrecyToken) -> MessageBackupKey {
        MessageBackupKey::derive(&self.backup_key, &self.backup_id, Some(token))
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

    fn test_aci() -> Aci {
        Aci::from_uuid_bytes(OFFICIAL_ACI_BYTES)
    }

    fn official_keys() -> BackupKeys {
        let aep: AccountEntropyPool = OFFICIAL_AEP.parse().expect("valid AEP");
        derive_keys(&aep, &test_aci())
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

    /// remote 臂（Some(token) → NEW_DST）与 legacy 臂（None → OLD_DST）双向量
    /// 钉定：token 即官方 key.rs 测试向量（"i pass the knife to the
    /// axolotl!"）。云备份 upload 产物与桌面端 remote 型逐字节同构、派生钥
    /// 相同的契约承诺（docs/cloud-backup-contract.md §6.1）以此测试固化。
    #[test]
    fn remote_arm_matches_official_some_and_none_vectors() {
        use libsignal_account_keys::BackupForwardSecrecyToken;

        const OFFICIAL_TOKEN_HEX: &str =
            "69207061737320746865206b6e69666520746f207468652061786f6c6f746c21";
        const OFFICIAL_REMOTE_HMAC_KEY_HEX: &str =
            "20e6ab57b87e051f3e695e953cf8a261dd307e4f92ae2921673f1d397e07887b";
        const OFFICIAL_REMOTE_AES_KEY_HEX: &str =
            "602af6ecfc09d695a8d58da9f18225e967979c5e03543ca0224a03cca3d9735e";

        let keys = official_keys();
        let token_bytes: [u8; 32] = hex_decode(OFFICIAL_TOKEN_HEX)
            .try_into()
            .expect("token is 32 bytes");
        let token = BackupForwardSecrecyToken(token_bytes);

        let remote_key = MessageBackupKey::derive(&keys.backup_key, &keys.backup_id, Some(&token));
        assert_eq!(
            hex_decode(OFFICIAL_REMOTE_HMAC_KEY_HEX),
            remote_key.hmac_key
        );
        assert_eq!(hex_decode(OFFICIAL_REMOTE_AES_KEY_HEX), remote_key.aes_key);

        let legacy_key = MessageBackupKey::derive(&keys.backup_key, &keys.backup_id, None);
        assert_eq!(hex_decode(OFFICIAL_HMAC_KEY_HEX), legacy_key.hmac_key);
        assert_eq!(hex_decode(OFFICIAL_AES_KEY_HEX), legacy_key.aes_key);
        assert_ne!(remote_key.hmac_key, legacy_key.hmac_key);
        assert_ne!(remote_key.aes_key, legacy_key.aes_key);

        // L2 按需接缝：message_key_with_token（remote 臂）与直接
        // MessageBackupKey::derive(Some(token)) 同面；legacy 臂即
        // self.message_key 原样。
        let keys = official_keys();
        let via_seam = keys.message_key_with_token(&token);
        assert_eq!(
            via_seam.hmac_key.to_vec(),
            hex_decode(OFFICIAL_REMOTE_HMAC_KEY_HEX)
        );
        assert_eq!(
            via_seam.aes_key.to_vec(),
            hex_decode(OFFICIAL_REMOTE_AES_KEY_HEX)
        );
        assert_eq!(
            keys.message_key.hmac_key.to_vec(),
            hex_decode(OFFICIAL_HMAC_KEY_HEX)
        );
    }

    /// 官方 account-keys backup.rs 已知向量（v0.99.0
    /// `backup_key_known_from_account_entropy`/`backup_id_known_from_account
    /// _entropy`）：AEP→BackupKey 与 BackupKey‖ACI→BackupId 逐字节钉定。
    #[test]
    fn derives_official_known_backup_key_and_id_vectors() {
        let keys = official_keys();
        assert_eq!(
            keys.backup_key.0.to_vec(),
            hex_decode("ea26a2ddb5dba5ef9e34e1b8dea1f5ae7f255306a6d2d883e542306eaa9fe985"),
            "BackupKey = AEP expand-only HKDF(info 20240801_SIGNAL_BACKUP_KEY)"
        );
        assert_eq!(
            keys.backup_id.0.to_vec(),
            hex_decode("8a624fbc45379043f39f1391cddc5fe8"),
            "BackupId = BackupKey‖ACI expand(info 20241024_SIGNAL_BACKUP_ID:)"
        );
    }

    /// 转移归档根钥臂等价钉形：from_raw_key(同一 32B 根) 与 AEP 臂全套
    /// 逐面相等——「同一派生面只换根钥来源」（官方 index.preload.ts:840
    /// 同语义）；异根（AEP 派生根 vs 随机根）全套必然互异。
    #[test]
    fn raw_key_arm_equals_aep_arm_on_same_root() {
        let aep_keys = official_keys();
        let raw_keys = BackupKeys::from_raw_key(&aep_keys.backup_key.0, &test_aci());
        assert_eq!(aep_keys.backup_key.0, raw_keys.backup_key.0);
        assert_eq!(aep_keys.backup_id.0, raw_keys.backup_id.0);
        assert_eq!(aep_keys.message_key.hmac_key, raw_keys.message_key.hmac_key);
        assert_eq!(aep_keys.message_key.aes_key, raw_keys.message_key.aes_key);
        assert_eq!(aep_keys.metadata_key, raw_keys.metadata_key);

        // 异根互异（转移归档 ephemeralKey ≠ 账号 BackupKey 的世界分离面）。
        let other = BackupKeys::from_raw_key(&[0x5A_u8; 32], &test_aci());
        assert_ne!(aep_keys.backup_key.0, other.backup_key.0);
        assert_ne!(aep_keys.backup_id.0, other.backup_id.0);
        assert_ne!(aep_keys.message_key.hmac_key, other.message_key.hmac_key);
    }

    /// media id 族钉形（L3/L4 按需消费面）：id 15B 确定性；内容钥 64B 切分
    /// 位（前 32B HMAC / 后 32B AES）与 vendored 原始派生逐字节对齐；缩略图
    /// transit 专用 DST 与正片钥互异；根分离（不同 MediaRootKey 同名不同
    /// id；AEP 账号根 ≠ 媒体根）。
    #[test]
    fn media_family_derivation_pins() {
        // mediaName = hex(plaintextHash‖key)（mediaId.preload.ts:44-51 官方
        // 构形；此处以形钉派生面）。
        let media_root = media_root_key(&[0x11_u8; 32]);
        let media_name = "aabbccddeeff";

        let id_a = media_id(&media_root, media_name);
        let id_again = media_id(&media_root, media_name);
        assert_eq!(id_a, id_again, "同根同名 id 确定性");
        assert_eq!(id_a.len(), 15, "官方 MEDIA_ID_LEN");

        let content = media_encryption_key_data(&media_root, &id_a);
        let raw = media_root.derive_media_encryption_key_data(&id_a);
        assert_eq!(content.hmac_key[..], raw[..32], "切分位：前 32B HMAC");
        assert_eq!(content.aes_key[..], raw[32..], "切分位：后 32B AES");

        let thumb = thumbnail_transit_encryption_key_data(&media_root, &id_a);
        assert_ne!(
            thumb.hmac_key, content.hmac_key,
            "专用 DST 20241030 ≠ 正片 20241007"
        );
        assert_ne!(thumb.aes_key, content.aes_key);

        let other_root = media_root_key(&[0x22_u8; 32]);
        assert_ne!(
            media_id(&media_root, media_name),
            media_id(&other_root, media_name),
            "根分离"
        );
        assert_ne!(
            media_id(&media_root, media_name),
            official_keys().backup_key.derive_media_id(media_name),
            "媒体根 ≠ AEP 账号根（BackupKey 类型兼任容器，世界分离）"
        );
    }
}
