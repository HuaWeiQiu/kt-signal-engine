// SPDX-License-Identifier: AGPL-3.0-only

//! 官方桌面本地备份快照导入（收向面，`docs/backup-export-contract.md` §11）。
//!
//! 对齐官方 `ts/services/backups/index.preload.ts` `importLocalBackup` 与
//! `ts/services/backups/util/localBackup.node.ts` `verifyLocalBackupMetadata`
//! （AGPL 只读参照零拷贝）：读 `<snapshotDir>/metadata` → decode
//! `Signal.backup.local.Metadata` → strict `version === 1` → backupId/iv/
//! encryptedId 在场且 iv 为 12B → AES-CTR 解密 backupId（计数器块 = iv(12B)
//! ‖ 4B 零，`Ctr32BE` 初始计数 0，与 `export.rs` 写面同款原语）→ 逐字节
//! 比对派生 `backupId`，不等即快照不属于本账号。
//!
//! KT 裁定：metadata 校验前置于 main 解密（官方是 importFromDisk 内并行），
//! 错目录/错账号在微秒级 metadata 面当场短路，不空转解密整个 main。main 的
//! 解密+分帧+尾部 HMAC 验证全官方 reader（`FramesReader`），逐帧投影复用
//! `history::export_frames_loop`（与 transfer archive 导出同一 NDJSON 通路，
//! connector `run_import` 直接消费）。快照目录由调用方（connector）传入，
//! 引擎不持久化该路径。

use std::path::Path;

use libsignal_message_backup::frame::{FileReaderFactory, FramesReader, VerifyHmac as _};
use libsignal_message_backup::parse::VarintDelimitedReader;
use prost::Message as _;

use crate::backup::export::decrypt_backup_id;
use crate::backup::keys::BackupKeys;
use crate::history::{export_frames_loop, ExportStats, HistoryError};
use crate::proto::local_archive;

/// metadata `backupId.encryptedId` 明文长度（= BackupId 16B，AES-CTR 同长）。
const BACKUP_ID_LEN: usize = 16;
/// metadata `backupId.iv` 长度（官方 `LOCAL_BACKUP_BACKUP_ID_IV_LENGTH`）。
const BACKUP_ID_IV_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotImportError {
    /// 目录缺席，或 `metadata`/`main` 任一必需文件缺席/不可读。
    #[error("snapshot directory or required file missing: {0}")]
    SnapshotMissing(std::path::PathBuf),
    /// metadata 解码失败，或 decode 后 version≠1/形状不合规（官方 strict
    /// assert 面）。
    #[error("snapshot metadata invalid: {0}")]
    MetadataInvalid(&'static str),
    /// 解密出的 backupId 与本账号派生值不等（官方 `Bytes.areEqual` 失败）：
    /// 快照属于别的账号（或密钥材料不一致）。
    #[error("snapshot backupId mismatch: not this account's backup")]
    KeyMismatch,
    /// metadata 读取 io（文件在场但读失败，权限级）。
    #[error("snapshot metadata io: {0}")]
    Io(#[from] std::io::Error),
    /// main 解密/分帧/HMAC/投影失败（官方 canDecryptMainArchive 面）。
    #[error("snapshot frames: {0}")]
    Frames(#[from] HistoryError),
}

/// `FramesReader::new` 的容器级校验错误（HistoryError 已有该变体，收向错误
/// 直通同一个 Frames 面）。
impl From<libsignal_message_backup::frame::ValidationError> for SnapshotImportError {
    fn from(error: libsignal_message_backup::frame::ValidationError) -> Self {
        Self::Frames(HistoryError::Frames(error))
    }
}

/// metadata 校验（官方 `verifyLocalBackupMetadata` 的 KT 落法，纯同步面）：
/// 读 → decode → version==1 → 形状（iv 12B、encryptedId 16B）→ 解密 →
/// 逐字节比对派生 backupId。任一步失败即导入拒绝（错误码归 service 层映射）。
/// `files/` 官方可缺席，不在本仓导入面消费范围。
pub(crate) fn verify_snapshot_metadata(
    snapshot_dir: &Path,
    keys: &BackupKeys,
) -> Result<(), SnapshotImportError> {
    let metadata_path = snapshot_dir.join("metadata");
    let bytes = match std::fs::read(&metadata_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SnapshotImportError::SnapshotMissing(metadata_path));
        }
        Err(error) => return Err(error.into()),
    };

    let metadata = local_archive::Metadata::decode(bytes.as_slice())
        .map_err(|_| SnapshotImportError::MetadataInvalid("protobuf decode"))?;
    if metadata.version != 1 {
        return Err(SnapshotImportError::MetadataInvalid("version"));
    }
    let Some(encrypted) = &metadata.backup_id else {
        return Err(SnapshotImportError::MetadataInvalid("backupId missing"));
    };
    if encrypted.iv.len() != BACKUP_ID_IV_LEN {
        return Err(SnapshotImportError::MetadataInvalid("iv length"));
    }
    if encrypted.encrypted_id.len() != BACKUP_ID_LEN {
        return Err(SnapshotImportError::MetadataInvalid("encryptedId length"));
    }

    let decrypted = decrypt_backup_id(&encrypted.iv, &encrypted.encrypted_id, keys);
    if decrypted != keys.backup_id.0 {
        return Err(SnapshotImportError::KeyMismatch);
    }
    Ok(())
}

/// main 全链导入导出面：官方 reader（HMAC-SHA256 尾验证 + AES-256-CBC +
/// gzip + padding 剥离）解密快照 main，逐帧复用 history 投影写 NDJSON。
/// O(1) 帧内存；future 非 Send，调用方须在 LocalSet 上运行（spawn_local）。
pub(crate) async fn export_snapshot_frames(
    snapshot_dir: &Path,
    keys: &BackupKeys,
    out: &mut dyn std::io::Write,
) -> Result<ExportStats, SnapshotImportError> {
    let main_path = snapshot_dir.join("main");
    if !main_path.is_file() {
        return Err(SnapshotImportError::SnapshotMissing(main_path));
    }
    let factory = FileReaderFactory { path: &main_path };
    let frames = FramesReader::new(&keys.message_key, factory).await?;
    let mut delimited = VarintDelimitedReader::new(frames);

    let stats = export_frames_loop(&mut delimited, out).await?;

    delimited
        .into_inner()
        .verify_hmac()
        .await
        .map_err(HistoryError::Hmac)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use cbc::cipher::{KeyIvInit as _, StreamCipher as _};
    use ctr::Ctr32BE;

    use super::*;
    use crate::backup::export::{export_local_backup, write_metadata, LocalBackupParams};
    use crate::backup::keys::tests_official_keys;
    use crate::proto::local_archive::metadata::EncryptedBackupId;

    fn unique_test_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-signal-engine-receive-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    /// 官方布局真快照（export 通路产出，seed 走 export.rs 测试同款最小集）。
    fn export_official_snapshot(root: &Path) -> PathBuf {
        let params = LocalBackupParams {
            keys: tests_official_keys(),
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
                crate::backup::frames::ChatItemDirection::Incoming {
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

    fn parse_aep() -> libsignal_account_keys::AccountEntropyPool {
        "dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l"
            .parse()
            .expect("valid AEP")
    }

    #[tokio::test]
    async fn exported_official_snapshot_passes_metadata_verify_and_frame_export() {
        let root = unique_test_dir("happy");
        let snapshot_dir = export_official_snapshot(&root);
        let keys = tests_official_keys();

        verify_snapshot_metadata(&snapshot_dir, &keys).expect("metadata verifies");

        let mut ndjson = Vec::new();
        let stats = export_snapshot_frames(&snapshot_dir, &keys, &mut ndjson)
            .await
            .expect("frames");
        // 1 BackupInfo + 1 AccountData + 1 Recipient + 1 Chat + 1 ChatItem。
        assert_eq!(stats.frames, 5);
        assert_eq!(stats.recipients, 1);
        assert_eq!(stats.chats, 1);
        assert_eq!(stats.messages, 1);
        assert_eq!(stats.backup_version, 1);
        // NDJSON 行形状与 history 通路同构（recipient 行带 aci 投影）。
        let text = String::from_utf8(ndjson).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "recipient+chat+message rows");
        let recipient: serde_json::Value = serde_json::from_str(lines[0]).expect("json");
        assert_eq!(recipient["type"], "recipient");
        assert_eq!(recipient["aci"], "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        let chat: serde_json::Value = serde_json::from_str(lines[1]).expect("json");
        assert_eq!(chat["type"], "chat");
        let message: serde_json::Value = serde_json::from_str(lines[2]).expect("json");
        assert_eq!(message["type"], "message");
        assert_eq!(message["text"], "hello from contact");

        fs::remove_dir_all(&root).expect("cleanup");
    }

    /// 官方布局缺件面：目录缺 / metadata 缺 → metadata 校验即 MISSING；
    /// metadata-only 目录（main 缺）过 metadata 校验，帧导出面 MISSING。
    #[tokio::test]
    async fn missing_directory_and_files_map_to_snapshot_missing() {
        let root = unique_test_dir("missing");
        let keys = tests_official_keys();

        let err = verify_snapshot_metadata(&root.join("nope"), &keys).unwrap_err();
        assert!(matches!(err, SnapshotImportError::SnapshotMissing(_)));

        // 目录在、metadata 缺。
        fs::create_dir_all(root.join("snapshot")).expect("dir");
        let err = verify_snapshot_metadata(&root.join("snapshot"), &keys).unwrap_err();
        assert!(matches!(err, SnapshotImportError::SnapshotMissing(_)));

        // metadata 在、main 缺（metadata 校验不查 main；帧导出面查）。
        let snapshot = root.join("snapshot");
        let mut buf = Vec::new();
        write_metadata(&mut buf, &keys).expect("metadata");
        fs::write(snapshot.join("metadata"), &buf).expect("write metadata");
        verify_snapshot_metadata(&snapshot, &keys).expect("metadata-only dir verifies");
        let err = export_snapshot_frames(&snapshot, &keys, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(err, SnapshotImportError::SnapshotMissing(_)));

        fs::remove_dir_all(&root).ok();
    }

    /// 官方 strict assert 面：version≠1 / iv≠12B / encryptedId≠16B / 缺
    /// backupId 一律 MetadataInvalid（逐字段手造违规 metadata）。
    #[test]
    fn malformed_metadata_fields_are_invalid_not_mismatch() {
        let root = unique_test_dir("malformed");
        let snapshot = root.join("snapshot");
        fs::create_dir_all(&snapshot).expect("dir");
        let keys = tests_official_keys();

        let good = {
            let mut buf = Vec::new();
            write_metadata(&mut buf, &keys).expect("metadata");
            local_archive::Metadata::decode(buf.as_slice()).expect("decode")
        };
        let encode = |metadata: &local_archive::Metadata| {
            let mut buf = Vec::new();
            metadata.encode(&mut buf).expect("encode");
            buf
        };
        let write_and_verify = |bytes: &[u8]| {
            fs::write(snapshot.join("metadata"), bytes).expect("write");
            verify_snapshot_metadata(&snapshot, &keys)
        };

        let mut wrong_version = good.clone();
        wrong_version.version = 2;
        assert!(matches!(
            write_and_verify(&encode(&wrong_version)),
            Err(SnapshotImportError::MetadataInvalid("version"))
        ));

        let mut missing_id = good.clone();
        missing_id.backup_id = None;
        assert!(matches!(
            write_and_verify(&encode(&missing_id)),
            Err(SnapshotImportError::MetadataInvalid("backupId missing"))
        ));

        let mut short_iv = good.clone();
        if let Some(encrypted) = &mut short_iv.backup_id {
            encrypted.iv.truncate(8);
        }
        assert!(matches!(
            write_and_verify(&encode(&short_iv)),
            Err(SnapshotImportError::MetadataInvalid("iv length"))
        ));

        let mut long_id = good;
        if let Some(encrypted) = &mut long_id.backup_id {
            encrypted.encrypted_id.push(0u8);
        }
        assert!(matches!(
            write_and_verify(&encode(&long_id)),
            Err(SnapshotImportError::MetadataInvalid("encryptedId length"))
        ));

        // 非 protobuf 字节 → decode 失败同码。
        assert!(matches!(
            write_and_verify(b"not protobuf at all"),
            Err(SnapshotImportError::MetadataInvalid("protobuf decode"))
        ));

        fs::remove_dir_all(&root).ok();
    }

    /// 官方 Bytes.areEqual 失败面：同 AEP 不同账号（ACI 不同 → backupId 不同）
    /// 的快照对本账号 = KeyMismatch；随机密文 = 不匹配（CTR 下随机数解出随机
    /// 明文，比对即败，绝不误判 INVALID）。
    #[test]
    fn foreign_or_corrupted_snapshot_fails_as_key_mismatch() {
        let root = unique_test_dir("mismatch");
        let snapshot = root.join("snapshot");
        fs::create_dir_all(&snapshot).expect("dir");
        let keys = tests_official_keys();

        // 同 AEP、不同 ACI → 不同 backupId 的密钥材料，解密必不匹配。
        let other_aci = presage::libsignal_service::protocol::Aci::from(
            presage::libsignal_service::prelude::Uuid::from_bytes([0x77; 16]),
        );
        let other_keys = crate::backup::keys::derive_keys(&parse_aep(), &other_aci);
        let mut buf = Vec::new();
        write_metadata(&mut buf, &other_keys).expect("metadata");
        fs::write(snapshot.join("metadata"), &buf).expect("write");
        assert!(matches!(
            verify_snapshot_metadata(&snapshot, &keys),
            Err(SnapshotImportError::KeyMismatch)
        ));

        // 合法形状、随机密文：仍是 KeyMismatch（不是 INVALID）。
        let random = local_archive::Metadata {
            version: 1,
            backup_id: Some(EncryptedBackupId {
                iv: vec![0x01; 12],
                encrypted_id: vec![0x02; 16],
            }),
        };
        let mut buf = Vec::new();
        random.encode(&mut buf).expect("encode");
        fs::write(snapshot.join("metadata"), &buf).expect("write");
        assert!(matches!(
            verify_snapshot_metadata(&snapshot, &keys),
            Err(SnapshotImportError::KeyMismatch)
        ));

        fs::remove_dir_all(&root).ok();
    }

    /// 官方 canDecryptMainArchive 面：合法 metadata + 被篡改的 main → 帧导出
    /// 失败（HMAC 或解密面），绝不产出半可信 NDJSON。
    #[tokio::test]
    async fn tampered_main_fails_frame_export() {
        let root = unique_test_dir("tamper");
        let snapshot_dir = export_official_snapshot(&root);
        let keys = tests_official_keys();

        let mut main = fs::read(snapshot_dir.join("main")).expect("read main");
        let mid = main.len() / 2;
        main[mid] ^= 0xff;
        fs::write(snapshot_dir.join("main"), &main).expect("write tampered main");

        let err = export_snapshot_frames(&snapshot_dir, &keys, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, SnapshotImportError::Frames(_)),
            "tampered main must fail in frame layer, got {err:?}"
        );

        fs::remove_dir_all(&root).ok();
    }

    /// decrypt_backup_id 语义单测（export.rs 写面 + receive.rs 读面回环）：
    /// Ctr32BE(iv‖0u32BE) 对称，写出的 encryptedId 原样解回 backupId。
    #[test]
    fn decrypt_backup_id_round_trips_written_metadata() {
        let keys = tests_official_keys();
        let mut buf = Vec::new();
        write_metadata(&mut buf, &keys).expect("metadata");
        let metadata = local_archive::Metadata::decode(buf.as_slice()).expect("decode");
        let encrypted = metadata.backup_id.expect("present");
        let decrypted = decrypt_backup_id(&encrypted.iv, &encrypted.encrypted_id, &keys);
        assert_eq!(decrypted, keys.backup_id.0);

        // 错钥（异 metadata_key）解不出同值。注意官方派生链里 metadata_key
        // 只依赖 AEP（BackupKey.derive_local_backup_metadata_key 不掺 ACI），
        // 所以「错钥」必须真的换 metadata_key 字节（同 AEP 异 ACI 的派生件
        // metadata_key 相同，keystream 相同，解密恒等——那正是 foreign 快照
        // 走 KeyMismatch 而非解密路径分歧的官方语义）。
        let other = BackupKeys {
            backup_key: libsignal_account_keys::BackupKey([0x5au8; 32]),
            backup_id: keys.backup_id,
            message_key: keys.message_key,
            metadata_key: [0x99u8; 32],
        };
        let wrong = decrypt_backup_id(&encrypted.iv, &encrypted.encrypted_id, &other);
        assert_ne!(wrong, keys.backup_id.0);
    }

    /// CTR 原语自证：手动构造与 write_metadata 相同的密文（同 iv）逐字节一致，
    /// 证明读面用的 Ctr32BE 构造与写面无漂移。
    #[test]
    fn ctr_primitive_matches_export_side_construction() {
        let keys = tests_official_keys();
        let iv = [0x5au8; 12];
        let mut iv_block = [0u8; 16];
        iv_block[..12].copy_from_slice(&iv);
        iv_block[12..].copy_from_slice(&0u32.to_be_bytes());
        let mut cipher =
            Ctr32BE::<aes::Aes256>::new_from_slices(&keys.metadata_key, &iv_block).expect("ctr");
        let mut encrypted = keys.backup_id.0.to_vec();
        cipher.apply_keystream(&mut encrypted);

        let decrypted = decrypt_backup_id(&iv, &encrypted, &keys);
        assert_eq!(decrypted, keys.backup_id.0);
    }
}
