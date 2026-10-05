// SPDX-License-Identifier: AGPL-3.0-only

//! 目录编排（`docs/backup-export-contract.md` §5/§7）：metadata + main 两文件
//! + `files/` 空目录 + 临时名 `-tmp` → rename 原子化。
//!
//! metadata 形态对齐官方 `LocalArchiver.getEncryptedBackupId`：
//! `Metadata{version=1, backupId{iv(12B 随机), encryptedId=AES-256-CTR32BE(
//! metadata_key, iv, backupId 16B)}}`。官方 Android 的 `Aes256Ctr32` 即
//! libsignal `aes_ctr.rs` 里 `ctr::Ctr32BE` 的 12B nonce + 初始计数器封装
//! （nonce 块 = iv ‖ 4B 计数，BE）——本仓用同款原语直接构造，密钥流逐字节等价。

use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use cbc::cipher::{KeyIvInit as _, StreamCipher as _};
use ctr::Ctr32BE;
use prost::Message;

use crate::backup::container::BackupStreamWriter;
use crate::backup::frames::{ChatItemDirection, FrameSequencer};
use crate::backup::keys::BackupKeys;
use crate::backup::BackupError;
use crate::proto::{self, local_archive};
use rand::RngCore as _;

/// metadata `backupId` 加密用 IV 长度（官方 `Util.getSecretBytes(12)`）。
const METADATA_IV_LEN: usize = 12;

/// 导出参数（契约 §7 `LocalBackupParams`）。AEP→密钥派生在 `keys::derive_keys`，
/// 调用方（connector 消费批）可从 presage store 取 AEP 后注入，亦可直接注入
/// 已派生的 `BackupKeys`。
pub struct LocalBackupParams {
    pub keys: BackupKeys,
    pub backup_time_ms: u64,
    /// 32B 随机，备份首次创建时生成后恒定（官方 mediaRootBackupKey）。
    pub media_root_backup_key: [u8; 32],
    /// 写入 currentAppVersion/firstAppVersion。
    pub app_version: String,
}

/// main 流写出统计（BackupInfo 计 1 个条目）。
pub struct MainStreamStats {
    pub frame_count: u64,
}

/// 一次完整目录导出的结果。
#[derive(Debug)]
pub struct ExportReport {
    /// 最终快照目录 `<dir>/SignalBackups/signal-backup-<ts UTC>/`。
    pub snapshot_dir: PathBuf,
    /// main 条目数（BackupInfo + Frames）。
    pub frame_count: u64,
    /// main 文件字节数（密文，IV+CBC+HMAC）。
    pub main_bytes: u64,
}

/// metadata 文件写面（明文 proto 帧，非加密容器；唯一敏感面是 backupId 的
/// CTR 密文，仅凭 AEP 可解——官方同款边界）。
pub fn write_metadata<W: Write>(mut w: W, keys: &BackupKeys) -> Result<(), BackupError> {
    let mut iv = [0u8; METADATA_IV_LEN];
    rand::rng().fill_bytes(&mut iv);

    let mut iv_block = [0u8; 16];
    iv_block[..METADATA_IV_LEN].copy_from_slice(&iv);
    // 初始计数器 0（官方 applyCipher: Aes256Ctr32(metadataKey, iv, 0)）。
    iv_block[METADATA_IV_LEN..].copy_from_slice(&0u32.to_be_bytes());
    let mut cipher = Ctr32BE::<aes::Aes256>::new_from_slices(&keys.metadata_key, &iv_block)
        .expect("CTR32 accepts 32B key + 16B counter block");

    let mut encrypted_id = keys.backup_id.0.to_vec();
    cipher.apply_keystream(&mut encrypted_id);

    let metadata = local_archive::Metadata {
        version: 1,
        backup_id: Some(local_archive::metadata::EncryptedBackupId {
            iv: iv.to_vec(),
            encrypted_id,
        }),
    };
    let mut buf = Vec::new();
    metadata.encode(&mut buf)?;
    w.write_all(&buf)?;
    w.flush()?;
    Ok(())
}

/// metadata `encryptedId` 解密回 `BackupId`（CTR 对称，恢复端同款操作；
/// 官方 `LocalArchiver.decryptBackupId` 对齐面）。
// 恢复端对齐面，恢复批接线后摘除（当前仅测试使用）。
#[allow(dead_code)]
pub fn decrypt_backup_id(iv: &[u8], encrypted_id: &[u8], keys: &BackupKeys) -> [u8; 16] {
    let mut iv_block = [0u8; 16];
    let n = iv.len().min(METADATA_IV_LEN);
    iv_block[..n].copy_from_slice(&iv[..n]);
    iv_block[METADATA_IV_LEN..].copy_from_slice(&0u32.to_be_bytes());
    let mut cipher = Ctr32BE::<aes::Aes256>::new_from_slices(&keys.metadata_key, &iv_block)
        .expect("CTR32 accepts 32B key + 16B counter block");
    let mut out = [0u8; 16];
    let n = encrypted_id.len().min(16);
    out[..n].copy_from_slice(&encrypted_id[..n]);
    cipher.apply_keystream(&mut out[..n]);
    out
}

/// main 容器写面 + 帧序状态机合体：状态机的每个产出帧即时编码写出，O(1)
/// 帧内存。BackupInfo 首条目由本层直接写出（构造即完成）。
pub struct MainStreamWriter<W: Write> {
    writer: BackupStreamWriter<W>,
    sequencer: FrameSequencer,
    frame_count: u64,
}

impl<W: Write> MainStreamWriter<W> {
    fn new(w: W, params: &LocalBackupParams) -> Result<Self, BackupError> {
        let writer = BackupStreamWriter::new(w, &params.keys.message_key)?;
        let backup_info = proto::BackupInfo {
            version: 1,
            backup_time_ms: params.backup_time_ms,
            media_root_backup_key: params.media_root_backup_key.to_vec(),
            current_app_version: params.app_version.clone(),
            first_app_version: params.app_version.clone(),
            debug_info: b"kt-signal-engine local backup export".to_vec(),
        };
        let mut sequencer = FrameSequencer::new(backup_info);
        let info = sequencer.backup_info()?;
        let mut slf = Self {
            writer,
            sequencer,
            frame_count: 0,
        };
        slf.write_encoded(&encode(&info))?;
        Ok(slf)
    }

    fn write_encoded(&mut self, encoded: &[u8]) -> Result<(), BackupError> {
        self.writer.write_frame(encoded)?;
        self.frame_count += 1;
        Ok(())
    }

    /// 帧序状态机直通面（本层未封装的帧型走这里）。
    // 恢复端对齐面，恢复批接线后摘除（当前仅测试使用）。
    #[allow(dead_code)]
    pub fn sequencer(&mut self) -> &mut FrameSequencer {
        &mut self.sequencer
    }

    /// 原始帧直写面（绕过状态机的逃生口，仅限测试注入违规帧证明 fail-closed）。
    // 恢复端对齐面，恢复批接线后摘除（当前仅测试使用）。
    #[allow(dead_code)]
    pub fn emit(&mut self, frame: proto::Frame) -> Result<(), BackupError> {
        self.write_encoded(&encode(&frame))
    }

    pub fn account_data(&mut self, account: proto::AccountData) -> Result<(), BackupError> {
        let frame = self.sequencer.account_data(account)?;
        self.emit(frame)
    }

    pub fn self_recipient(&mut self) -> Result<u64, BackupError> {
        let (id, frame) = self.sequencer.self_recipient()?;
        self.emit(frame)?;
        Ok(id)
    }

    pub fn contact_recipient(&mut self, contact: proto::Contact) -> Result<u64, BackupError> {
        let (id, frame) = self.sequencer.contact_recipient(contact)?;
        self.emit(frame)?;
        Ok(id)
    }

    pub fn group_recipient(&mut self, master_key: [u8; 32]) -> Result<u64, BackupError> {
        let (id, frame) = self.sequencer.group_recipient(master_key)?;
        self.emit(frame)?;
        Ok(id)
    }

    pub fn chat(&mut self, recipient_id: u64) -> Result<u64, BackupError> {
        let (id, frame) = self.sequencer.chat(recipient_id)?;
        self.emit(frame)?;
        Ok(id)
    }

    pub fn chat_item_text(
        &mut self,
        chat_id: u64,
        author_id: u64,
        date_sent_ms: u64,
        direction: ChatItemDirection,
        body: &str,
    ) -> Result<(), BackupError> {
        let frame =
            self.sequencer
                .chat_item_text(chat_id, author_id, date_sent_ms, direction, body)?;
        self.emit(frame)
    }

    pub fn all_chat_folder(&mut self) -> Result<(), BackupError> {
        let frame = self.sequencer.all_chat_folder()?;
        self.emit(frame)
    }

    /// 收尾容器（gzip 终结 → 补零 → PKCS7 → HMAC 尾），返回底层写面。
    pub fn finish(self) -> Result<(W, u64), BackupError> {
        let w = self.writer.finish()?;
        Ok((w, self.frame_count))
    }
}

fn encode(msg: &impl Message) -> Vec<u8> {
    let mut buf = Vec::new();
    msg.encode(&mut buf).expect("encode into Vec");
    buf
}

/// main 全链写面入口：`seed` 闭包向 writer 注入账号与会话内容（契约 §7
/// 「调用方给什么导什么」）。返回底层写面（调用方负责 flush/落盘语义）与
/// 条目统计。
pub fn write_main_stream<W: Write, F>(
    w: W,
    params: &LocalBackupParams,
    seed: F,
) -> Result<(W, MainStreamStats), BackupError>
where
    F: FnOnce(&mut MainStreamWriter<W>) -> Result<(), BackupError>,
{
    let mut out = MainStreamWriter::new(w, params)?;
    seed(&mut out)?;
    let (w, frame_count) = out.finish()?;
    Ok((w, MainStreamStats { frame_count }))
}

/// 完整目录导出（契约 §5 形态）：
///
/// ```text
/// <dir>/SignalBackups/signal-backup-<yyyy-MM-dd-HH-mm-ss>(UTC)/
///   metadata  main  files/
/// ```
///
/// 写入临时名 `-tmp` 后 rename 去尾（官方原子化纪律）；任一步失败清理临时
/// 目录，不留半成品。目标快照目录已存在时 fail-closed（不覆盖旧快照，升级
/// 清理策略归 connector 消费批）。
pub fn export_local_backup<F>(
    dir: &Path,
    params: &LocalBackupParams,
    seed: F,
) -> Result<ExportReport, BackupError>
where
    F: FnOnce(&mut MainStreamWriter<BufWriter<fs::File>>) -> Result<(), BackupError>,
{
    let backups_root = dir.join("SignalBackups");
    let snapshot_name = format!(
        "signal-backup-{}",
        format_utc_timestamp(params.backup_time_ms)
    );
    let tmp_dir = backups_root.join(format!("{snapshot_name}-tmp"));
    let final_dir = backups_root.join(&snapshot_name);
    if final_dir.exists() {
        return Err(BackupError::SnapshotExists(final_dir));
    }

    fs::create_dir_all(&tmp_dir)?;
    let result = (|| -> Result<ExportReport, BackupError> {
        write_metadata(
            BufWriter::new(fs::File::create(tmp_dir.join("metadata"))?),
            &params.keys,
        )?;
        fs::create_dir(tmp_dir.join("files"))?;

        let main_file = BufWriter::new(fs::File::create(tmp_dir.join("main"))?);
        let (mut main, stats) = write_main_stream(main_file, params, seed)?;
        main.flush()?;
        let main_bytes = fs::metadata(tmp_dir.join("main"))?.len();

        Ok(ExportReport {
            snapshot_dir: final_dir.clone(),
            frame_count: stats.frame_count,
            main_bytes,
        })
    })();

    match result {
        Ok(report) => {
            fs::rename(&tmp_dir, &final_dir)?;
            Ok(report)
        }
        Err(err) => {
            let _ = fs::remove_dir_all(&tmp_dir);
            Err(err)
        }
    }
}

/// `yyyy-MM-dd-HH-mm-ss`（UTC，官方快照目录名形态）。无 chrono 依赖，天数→
/// 民用历日用 Hinnant civil_from_days 算法（纯整数、无溢出域）。
pub fn format_utc_timestamp(ms: u64) -> String {
    const SECS_PER_DAY: u64 = 86_400;
    let secs = ms / 1000;
    let days = (secs / SECS_PER_DAY) as i64;
    let secs_of_day = secs % SECS_PER_DAY;

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };

    let (h, m, s) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    format!("{year:04}-{month:02}-{day:02}-{h:02}-{m:02}-{s:02}")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use libsignal_message_backup::backup::Purpose;
    use libsignal_message_backup::frame::{CursorFactory, FramesReader};
    use libsignal_message_backup::parse::VarintDelimitedReader;
    use libsignal_message_backup::BackupReader;

    use super::*;
    use crate::backup::frames::FrameSequenceError;
    use crate::backup::keys::tests_official_keys;

    fn params() -> LocalBackupParams {
        LocalBackupParams {
            keys: tests_official_keys(),
            backup_time_ms: 1_760_000_000_000,
            media_root_backup_key: [0x42; 32],
            app_version: "kt-signal-engine 0.1.0".into(),
        }
    }

    fn unique_test_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-signal-engine-export-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    fn seed_mvp<W: Write>(out: &mut MainStreamWriter<W>) -> Result<(), BackupError> {
        out.account_data(proto::AccountData {
            profile_key: vec![0x11; 32],
            given_name: "Kit".into(),
            ..Default::default()
        })?;
        let self_id = out.self_recipient()?;
        let contact_id = out.contact_recipient(proto::Contact {
            aci: Some([0xaa; 16].to_vec()),
            e164: Some(15_555_550_100),
            profile_key: Some(vec![7; 32]),
            profile_given_name: Some("Ada".into()),
            registration: Some(proto::contact::Registration::Registered(
                proto::contact::Registered {},
            )),
            ..Default::default()
        })?;
        let chat_id = out.chat(contact_id)?;
        out.chat_item_text(
            chat_id,
            contact_id,
            1_760_000_001_000,
            ChatItemDirection::Incoming {
                date_received_ms: 1_760_000_001_005,
                read: true,
            },
            "hello from contact",
        )?;
        out.chat_item_text(
            chat_id,
            self_id,
            1_760_000_002_000,
            ChatItemDirection::Outgoing {
                date_received_ms: 1_760_000_002_010,
            },
            "hello back",
        )?;
        out.all_chat_folder()?;
        Ok(())
    }

    #[test]
    fn metadata_round_trips_backup_id() {
        let keys = tests_official_keys();
        let mut buf = Vec::new();
        write_metadata(&mut buf, &keys).expect("metadata");

        let metadata = local_archive::Metadata::decode(&buf[..]).expect("decodes");
        assert_eq!(metadata.version, 1);
        let encrypted = metadata.backup_id.expect("backup id present");
        assert_eq!(encrypted.iv.len(), METADATA_IV_LEN);
        assert_eq!(encrypted.encrypted_id.len(), 16);
        assert_ne!(encrypted.encrypted_id, keys.backup_id.0);

        let decrypted = decrypt_backup_id(&encrypted.iv, &encrypted.encrypted_id, &keys);
        assert_eq!(decrypted, keys.backup_id.0);
    }

    /// 官方 `LocalArchiver` 对齐面：metadata 解码→解 backupId→`canDecrypt-
    /// MainArchive` 等价物（main 首条目可解密且 HMAC 通过）。
    #[tokio::test]
    async fn exported_main_reads_back_with_official_reader() {
        let p = params();
        let (archive, _) = write_main_stream(Vec::new(), &p, seed_mvp).expect("export");

        let message_key = &p.keys.message_key;
        let factory = CursorFactory::new(&archive);
        let frames = FramesReader::new(message_key, factory)
            .await
            .expect("container valid");
        let mut delimited = VarintDelimitedReader::new(frames);
        let first = delimited
            .read_next()
            .await
            .expect("readable")
            .expect("first");
        let info = proto::BackupInfo::decode(&first[..]).expect("first item is BackupInfo");
        assert_eq!(info.version, 1);
        assert_eq!(info.media_root_backup_key, vec![0x42; 32]);
        assert!(delimited.read_next().await.expect("readable").is_some());
    }

    /// 验收②：官方 `BackupReader::validate_all` 对 MVP 样例档案双 Purpose
    /// 全绿（ArchiveValidator.kt 同款口径：恰一 AccountData、恰一 Self、
    /// 恰一 ALL folder、引用与去重全过）。
    #[tokio::test]
    async fn validator_accepts_mvp_archive_for_both_purposes() {
        let p = params();
        let (sink, stats) = write_main_stream(Vec::new(), &p, seed_mvp).expect("export");
        let _ = sink;

        for purpose in [Purpose::DeviceTransfer, Purpose::RemoteBackup] {
            let factory = CursorFactory::new(&sink);
            let reader =
                BackupReader::new_encrypted_compressed(&p.keys.message_key, factory, purpose)
                    .await
                    .expect("container valid");
            let result = reader.validate_all().await;
            assert!(
                result.result.is_ok(),
                "{purpose:?} validation failed: {:?}",
                result.result.err()
            );
            assert!(
                result.found_unknown_fields.is_empty(),
                "{purpose:?} unknown fields: {:?}",
                result.found_unknown_fields
            );
        }
        // 1 BackupInfo + 1 AccountData + 2 Recipient + 1 Chat + 2 ChatItem
        // + 1 ChatFolder
        assert_eq!(stats.frame_count, 8);
    }

    /// validator 有效性证明：绕过状态机直写缺 AccountData 的档案，官方
    /// validator 必须拒绝——证明上一测试不是空转绿。
    #[tokio::test]
    async fn validator_rejects_archive_without_account_data() {
        let p = params();
        let mut writer = BackupStreamWriter::new(Vec::new(), &p.keys.message_key).expect("writer");
        let info = proto::BackupInfo {
            version: 1,
            backup_time_ms: p.backup_time_ms,
            media_root_backup_key: p.media_root_backup_key.to_vec(),
            current_app_version: p.app_version.clone(),
            first_app_version: p.app_version.clone(),
            debug_info: vec![],
        };
        let mut encoded = Vec::new();
        info.encode(&mut encoded).expect("encode");
        writer.write_frame(&encoded).expect("write");
        let mut self_frame = Vec::new();
        proto::Frame {
            item: Some(proto::frame::Item::Recipient(proto::Recipient {
                id: 1,
                destination: Some(proto::recipient::Destination::Self_(proto::Self_ {
                    avatar_color: None,
                })),
            })),
        }
        .encode(&mut self_frame)
        .expect("encode");
        writer.write_frame(&self_frame).expect("write");
        let archive = writer.finish().expect("finish");

        let factory = CursorFactory::new(&archive);
        let reader = BackupReader::new_encrypted_compressed(
            &p.keys.message_key,
            factory,
            Purpose::DeviceTransfer,
        )
        .await
        .expect("container valid");
        assert!(reader.validate_all().await.result.is_err());
    }

    #[tokio::test]
    async fn export_local_backup_produces_official_directory_layout() {
        let root = unique_test_dir("layout");
        let p = params();

        let report = export_local_backup(&root, &p, seed_mvp).expect("export");
        assert_eq!(
            report.snapshot_dir,
            root.join("SignalBackups/signal-backup-2025-10-09-08-53-20")
        );
        assert!(report.snapshot_dir.join("metadata").is_file());
        assert!(report.snapshot_dir.join("main").is_file());
        assert!(report.snapshot_dir.join("files").is_dir());
        assert_eq!(report.frame_count, 8);
        assert!(report.main_bytes > 541 + 16 + 32, "padded ciphertext floor");
        // 无 -tmp 残留
        let backups = root.join("SignalBackups");
        let entries: Vec<_> = fs::read_dir(&backups).unwrap().collect();
        assert_eq!(entries.len(), 1, "only the final snapshot dir remains");

        // 目录里的 main 可被官方 reader 打开（与内存链路同物）
        let archive = fs::read(report.snapshot_dir.join("main")).expect("read main");
        let factory = CursorFactory::new(&archive);
        let reader = BackupReader::new_encrypted_compressed(
            &p.keys.message_key,
            factory,
            Purpose::RemoteBackup,
        )
        .await
        .expect("container valid");
        assert!(reader.validate_all().await.result.is_ok());

        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[tokio::test]
    async fn failed_export_cleans_tmp_dir_and_keeps_no_snapshot() {
        let root = unique_test_dir("failure");
        let p = params();

        let err = export_local_backup(&root, &p, |out| {
            out.account_data(proto::AccountData {
                profile_key: vec![0x11; 32],
                ..Default::default()
            })?;
            // 违规帧序：AccountData 之后再来一次 → 状态机 fail-closed
            let frame = out
                .sequencer()
                .account_data(proto::AccountData::default())
                .map_err(BackupError::from)?;
            out.emit(frame)
        });
        assert!(matches!(err, Err(BackupError::FrameSequence(_))));
        // SignalBackups 父目录残留合法；内部不得有 -tmp 残留或半成品快照
        let leftovers: Vec<_> = fs::read_dir(root.join("SignalBackups"))
            .expect("backups root exists")
            .collect();
        assert!(leftovers.is_empty(), "tmp dir cleaned: {leftovers:?}");

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn existing_snapshot_dir_fails_closed() {
        let root = unique_test_dir("exists");
        let p = params();
        export_local_backup(&root, &p, seed_mvp).expect("first export");
        let second = export_local_backup(&root, &p, seed_mvp);
        assert!(matches!(second, Err(BackupError::SnapshotExists(_))));
        fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn timestamp_format_matches_official_snapshot_naming() {
        // 1_760_000_000_000 ms = 2025-10-09T08:53:20Z
        assert_eq!(
            format_utc_timestamp(1_760_000_000_000),
            "2025-10-09-08-53-20"
        );
        // 纪元起点
        assert_eq!(format_utc_timestamp(0), "1970-01-01-00-00-00");
        // 闰年 2 月 29：2024-02-29T23:59:59Z = 1709251199 s
        assert_eq!(
            format_utc_timestamp(1_709_251_199_000),
            "2024-02-29-23-59-59"
        );
    }

    #[test]
    fn frame_sequence_error_transparent_in_report() {
        // BackupError 对 FrameSequenceError 的 From 透传（目录层不吞错）
        fn wrap(e: FrameSequenceError) -> BackupError {
            e.into()
        }
        assert!(matches!(
            wrap(FrameSequenceError::UnknownChat(7)),
            BackupError::FrameSequence(FrameSequenceError::UnknownChat(7))
        ));
    }
}
