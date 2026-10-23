// SPDX-License-Identifier: AGPL-3.0-only

//! 设备转移备份归档容器收向面（台账 §29.10.2 L4 节实施规格；L4 交付）。

//! 容器分层（官方 index.preload.ts:857-899 解密序，导出序为其逆）：
//!
//! ~~~text
//! bytes = IV(16B,随机) || AES-256-CBC(aesKey, IV, PKCS7) over gzip(帧流) ||
//!         HMAC-SHA256(macKey, IV||密文全量)(32B,尾附)
//! 帧流 = varint 长度前缀(LB128) || protobuf 载荷；首帧恒 BackupInfo
//! ~~~

//! 导出侧 gzip 后还过 padding（logPadding 尺寸模糊化）；导入端 gzip 解压
//! 自动吃掉 padding，本面无需感知。

//! 解密/分帧/HMAC 全走 vendored libsignal-message-backup 现成 reader
//! （FramesReader 一体面 + VarintDelimitedReader），本模块只做两件事：
//! ①容器钥臂选择（见下）；②入口形状 = 内存字节（CDN
//! /attachments/{key} 下载体直连，D0 真机批免落盘）。帧循环与 NDJSON
//! 投影复用 history::export_frames_loop（与快照导入/云恢复/既有转移
//! 下载同一 NDJSON 通路）；首帧 BackupInfo version=1 硬校验在帧循环
//! 单点（官方 import.preload.ts:342-344 同语义）；尾部 HMAC 验证在流读
//! 完时执行（官方两遍验证的消费臂：篡改/截断档案在此拒绝）。

//! 容器钥双臂（L2 BackupKeys::from_raw_key 同根，只差容器钥 DST；
//! D0 真机实测定夺生产走哪臂，两臂都实现）：
//!
//! - legacy 臂（默认）：MessageBackupKey::derive(.., None)，DST
//!   20241007_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:——桌面转移归档
//!   消费臂（crypto.preload.ts getKeyMaterial 无 FS token）。
//! - remote 臂：DST 20250708_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:
//!   加 FS token 32B salt——SVR forward-secrecy 产出臂（手机云端备份
//!   若启用 SVR token 则走此臂；`message_key_with_token` L2 接缝）。
//!
//! AGPL：未复制上游源码（行为对齐 + 行号参照 + 现成 API 直用）。
//
// 模块级 allow(dead_code)（L3 backup_api 同先例，附注理由）：本面为 D0
// 真机接线批的既成对接面——解析/双臂往返/拒绝语义以测试钉形在案；生产
// 流暂走既有 history::export_archive（路径入口），字节入口与双臂选择随
// D0 接线消费时摘除本 allow。
#![allow(dead_code)]

use libsignal_account_keys::BackupForwardSecrecyToken;
use libsignal_message_backup::frame::{CursorFactory, FramesReader, VerifyHmac as _};
use libsignal_message_backup::parse::VarintDelimitedReader;

use crate::backup::keys::BackupKeys;
use crate::history::{export_frames_loop, ExportStats, HistoryError};

/// 转移归档容器钥臂（两臂同根 32B 裸根 ephemeralKey，只差容器钥派生
/// DST；D0 实测定夺生产臂，构造面同时保留）。
#[derive(Debug, Clone)]
pub enum TransferKeyArm {
    /// legacy 臂：无 FS token（DST 20241007）——桌面转移归档消费臂。
    Legacy,
    /// remote 臂：SVR forward-secrecy token（DST 20250708 + 32B salt）。
    Remote(BackupForwardSecrecyToken),
}

/// 转移归档收向错误（容器级与帧级两层；词表归 HistoryError/容器校验）。
#[derive(Debug, thiserror::Error)]
pub enum TransferArchiveError {
    /// 容器级校验拒收（IV/流形/长度不符——官方 FramesReader 面同语义）。
    #[error("transfer archive container: {0}")]
    Container(#[from] libsignal_message_backup::frame::ValidationError),
    /// 帧级失败（首帧 decode/版本硬校验/尾部 HMAC/投影）。
    #[error("transfer archive frames: {0}")]
    History(#[from] HistoryError),
}

/// 转移归档解容器 → 逐帧 NDJSON 导出（内存字节入口）。
///
/// ephemeral_backup_key 为 32B 裸根（链接期 provisioning
/// ephemeralBackupKey；官方 index.preload.ts:840
/// getKeyMaterial(new BackupKey(ephemeralKey)) 同一派生面，经 L2
/// BackupKeys::from_raw_key 单点落地）。尾部 HMAC 在流读完时验证：
/// 验证失败 = 档案被篡改/截断，整体拒绝（错误归
/// TransferArchiveError::History(HistoryError::Hmac)）。future 非 Send
/// （上游 reader 持非 Send 状态），调用方须在 LocalSet 上运行。
pub async fn export_transfer_archive(
    archive: &[u8],
    ephemeral_backup_key: &[u8; 32],
    aci: presage::libsignal_service::prelude::Uuid,
    arm: &TransferKeyArm,
    out: &mut dyn std::io::Write,
) -> Result<ExportStats, TransferArchiveError> {
    let keys = BackupKeys::from_raw_key(
        ephemeral_backup_key,
        &presage::libsignal_service::protocol::Aci::from(aci),
    );
    let message_key = match arm {
        TransferKeyArm::Legacy => keys.message_key,
        TransferKeyArm::Remote(token) => keys.message_key_with_token(token),
    };
    let frames = FramesReader::new(&message_key, CursorFactory::new(archive)).await?;
    let mut delimited = VarintDelimitedReader::new(frames);
    let stats = export_frames_loop(&mut delimited, out, &aci).await?;
    delimited
        .into_inner()
        .verify_hmac()
        .await
        .map_err(HistoryError::Hmac)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::backup::container::BackupStreamWriter;
    use crate::proto::{chat_item, frame, BackupInfo, ChatItem, Frame, StandardMessage, Text};

    const RAW_KEY: [u8; 32] = [0x5Au8; 32];
    /// 官方 key.rs 测试向量 token（"i pass the knife to the axolotl!"，
    /// keys.rs remote 臂同款）；remote 臂往复测试只论自洽，官方向量钉形
    /// 在 keys.rs 单点维护。
    const TOKEN_BYTES: [u8; 32] = [
        0x69, 0x20, 0x70, 0x61, 0x73, 0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6b, 0x6e, 0x69, 0x66,
        0x65, 0x20, 0x74, 0x6f, 0x20, 0x74, 0x68, 0x65, 0x20, 0x61, 0x78, 0x6f, 0x6c, 0x6f, 0x74,
        0x6c, 0x21,
    ];

    fn test_aci() -> presage::libsignal_service::prelude::Uuid {
        presage::libsignal_service::prelude::Uuid::from_bytes([0x77u8; 16])
    }

    fn encode(msg: &impl prost::Message) -> Vec<u8> {
        msg.encode_to_vec()
    }

    /// 最小帧集：BackupInfo(version) + 一条 incoming 文本 ChatItem。
    fn frame_payload(version: u64) -> Vec<Vec<u8>> {
        let info = encode(&BackupInfo {
            version,
            backup_time_ms: 1_760_000_000_000,
            ..Default::default()
        });
        let item = encode(&Frame {
            item: Some(frame::Item::ChatItem(ChatItem {
                chat_id: 3,
                author_id: 7,
                date_sent: 1_760_000_001_000,
                directional_details: Some(chat_item::DirectionalDetails::Incoming(
                    chat_item::IncomingMessageDetails {
                        date_received: 1_760_000_001_005,
                        ..Default::default()
                    },
                )),
                item: Some(chat_item::Item::StandardMessage(StandardMessage {
                    text: Some(Text {
                        body: "hello transfer".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        });
        vec![info, item]
    }

    /// 官方 writer（BackupStreamWriter：IV/AES-CBC/gzip/padding/HMAC 尾）
    /// 造容器——与转移归档生产者分层同构，密钥由调用方注入。
    fn sealed_archive(
        message_key: &libsignal_message_backup::key::MessageBackupKey,
        frames: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut writer = BackupStreamWriter::new(Vec::new(), message_key).expect("writer");
        for frame in frames {
            writer.write_frame(frame).expect("write frame");
        }
        writer.finish().expect("finish")
    }

    fn entry_keys() -> crate::backup::keys::BackupKeys {
        BackupKeys::from_raw_key(
            &RAW_KEY,
            &presage::libsignal_service::protocol::Aci::from(test_aci()),
        )
    }

    #[tokio::test]
    async fn legacy_arm_roundtrips_container_to_ndjson() {
        let aci = test_aci();
        let archive = sealed_archive(&entry_keys().message_key, &frame_payload(1));

        let mut out: Vec<u8> = Vec::new();
        let stats =
            export_transfer_archive(&archive, &RAW_KEY, aci, &TransferKeyArm::Legacy, &mut out)
                .await
                .expect("legacy roundtrip");
        assert_eq!(stats.backup_version, 1);
        assert_eq!(stats.frames, 2);
        assert_eq!(stats.messages, 1);
        let text = String::from_utf8(out).expect("utf8");
        assert!(
            text.contains(r#""text":"hello transfer""#),
            "ndjson: {text}"
        );
    }

    #[tokio::test]
    async fn remote_arm_roundtrips_with_forward_secrecy_token() {
        let aci = test_aci();
        let token = BackupForwardSecrecyToken(TOKEN_BYTES);
        let remote_key = entry_keys().message_key_with_token(&token);
        let archive = sealed_archive(&remote_key, &frame_payload(1));

        let mut out: Vec<u8> = Vec::new();
        let stats = export_transfer_archive(
            &archive,
            &RAW_KEY,
            aci,
            &TransferKeyArm::Remote(token),
            &mut out,
        )
        .await
        .expect("remote roundtrip");
        assert_eq!(stats.backup_version, 1);
        assert_eq!(stats.messages, 1);
        // 双臂异容器钥：legacy 臂读 remote 档案必须在容器/帧级被拒。
        let mut mismatch: Vec<u8> = Vec::new();
        let result = export_transfer_archive(
            &archive,
            &RAW_KEY,
            aci,
            &TransferKeyArm::Legacy,
            &mut mismatch,
        )
        .await;
        assert!(result.is_err(), "legacy arm must not open remote archive");
    }

    #[tokio::test]
    async fn rejects_first_frame_version_other_than_one() {
        let aci = test_aci();
        let archive = sealed_archive(&entry_keys().message_key, &frame_payload(2));

        let mut out: Vec<u8> = Vec::new();
        let error =
            export_transfer_archive(&archive, &RAW_KEY, aci, &TransferKeyArm::Legacy, &mut out)
                .await
                .expect_err("version 2 must be rejected");
        assert!(
            matches!(
                error,
                TransferArchiveError::History(HistoryError::UnsupportedBackupVersion(2))
            ),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn rejects_archive_without_frames() {
        let aci = test_aci();
        let archive = sealed_archive(&entry_keys().message_key, &[]);

        let mut out: Vec<u8> = Vec::new();
        let error =
            export_transfer_archive(&archive, &RAW_KEY, aci, &TransferKeyArm::Legacy, &mut out)
                .await
                .expect_err("empty archive must be rejected");
        assert!(
            matches!(error, TransferArchiveError::History(HistoryError::NoFrames)),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn rejects_tampered_hmac_tail() {
        let aci = test_aci();
        let mut archive = sealed_archive(&entry_keys().message_key, &frame_payload(1));
        // 只翻尾 32B（HMAC 尾附段）最后一字节：密文/明文不动，流照读，
        // 终验必炸——钉官方两遍验证的消费臂语义（篡改档案整体拒绝）。
        let last = archive.len() - 1;
        archive[last] ^= 0xff;

        let mut out: Vec<u8> = Vec::new();
        let error =
            export_transfer_archive(&archive, &RAW_KEY, aci, &TransferKeyArm::Legacy, &mut out)
                .await
                .expect_err("tampered hmac must be rejected");
        // 官方两遍验证的第一遍在容器级：FramesReader::new 即校验 HMAC
        // （InvalidHmac 面拒收，流一个字节都不交给投影）；帧循环读完后的
        // verify_hmac() 为第二遍防 TOCTOU 复验（HISTORY Hmac 面）。
        assert!(
            matches!(error, TransferArchiveError::Container(_)),
            "unexpected error: {error:?}"
        );
    }
}
