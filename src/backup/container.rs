// SPDX-License-Identifier: AGPL-3.0-only

//! main 文件加密容器写面（`docs/backup-export-contract.md` §4）。
//!
//! 官方形态（Signal-Android `EncryptedBackupWriter::createForLocalOrLinking`
//! + libsignal v0.99.0 `export.rs` 导出原语，行为对齐、代码自实现）：
//!
//! ```text
//! main = IV(16B 随机) ‖ AES-256-CBC(aes_key, IV, PKCS7, payload) ‖ HMAC-SHA256(mac_key, IV ‖ CBC密文)
//! payload = gzip(varint32(len) ‖ Frame ‖ …) ‖ 0x00 * padded_length(gzip长度) 差值
//! ```
//!
//! 流式实现：帧进 gzip、gzip 进 CBC（块级缓冲）、HMAC 随密文滚动——O(1) 帧
//! 内存，无全量缓冲。分块 flush（官方 main 分支 PaddedGzipOutputStream 的
//! FULL_FLUSH 策略）不做，见契约 §8 差异 1。

use std::io::{self, Write};

use aes::Aes256;
use cbc::cipher::{BlockModeEncrypt as _, KeyIvInit as _};
use cbc::Encryptor;
use flate2::write::GzEncoder;
use flate2::Compression;
use hmac::{Hmac, KeyInit as _, Mac as _};
use libsignal_message_backup::key::MessageBackupKey;
use rand::RngCore as _;
use sha2::Sha256;

const BLOCK_LEN: usize = 16;
const IV_LEN: usize = 16;
const HMAC_LEN: usize = 32;
/// 官方 Android `GZIPOutputStream` 缺省 Deflater 压缩级别
/// （DEFAULT_COMPRESSION → zlib 级别 6）。
const GZIP_LEVEL: u32 = 6;
/// 补零分块大小（bucket 差值最大可达压缩量 ~5%，分块写避免整块分配）。
const PAD_CHUNK: usize = 64 * 1024;

/// varint32（protobuf/LEB128，官方 `writeVarInt32` 同形）。
pub fn write_varint32(buf: &mut Vec<u8>, value: u32) {
    let mut v = value;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// 计数器写面：gzip 收尾后据此取压缩输出字节数（flate2 的 write::GzEncoder
/// 不暴露 total_out）。
struct CountingWriter<W: Write> {
    inner: W,
    count: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// CBC 加密 + HMAC 滚动的 `Write` 适配层：进来的明文按 16B 块加密写出，
/// 密文进 HMAC；`finalize` 做 PKCS7 终块并追加 HMAC 尾。
struct CbcMacWriter<W: Write> {
    out: W,
    enc: Encryptor<Aes256>,
    block: cbc::cipher::Block<Encryptor<Aes256>>,
    block_len: usize,
    mac: Hmac<Sha256>,
}

impl<W: Write> CbcMacWriter<W> {
    fn new(out: W, aes_key: &[u8; 32], iv: &[u8; IV_LEN], mac: Hmac<Sha256>) -> Self {
        Self {
            out,
            enc: Encryptor::new_from_slices(aes_key, iv)
                .expect("AES-256-CBC accepts 32B key + 16B iv"),
            block: Default::default(),
            block_len: 0,
            mac,
        }
    }

    fn encrypt_and_emit(&mut self) -> io::Result<()> {
        let mut block = self.block;
        self.enc.encrypt_block(&mut block);
        self.mac.update(&block);
        self.out.write_all(&block)?;
        self.block_len = 0;
        Ok(())
    }

    /// PKCS7 终块 + HMAC 尾（HMAC 覆盖 IV ‖ 全部密文）。
    fn finalize(mut self) -> io::Result<W> {
        let pad = (BLOCK_LEN - self.block_len) as u8;
        while self.block_len < BLOCK_LEN {
            self.block[self.block_len] = pad;
            self.block_len += 1;
        }
        self.encrypt_and_emit()?;
        let tag = self.mac.finalize().into_bytes();
        debug_assert_eq!(tag.len(), HMAC_LEN, "SHA-256 tag is 32 bytes");
        self.out.write_all(&tag[..])?;
        Ok(self.out)
    }
}

impl<W: Write> Write for CbcMacWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut consumed = 0;
        while consumed < buf.len() {
            let take = (BLOCK_LEN - self.block_len).min(buf.len() - consumed);
            self.block[self.block_len..self.block_len + take]
                .copy_from_slice(&buf[consumed..consumed + take]);
            self.block_len += take;
            consumed += take;
            if self.block_len == BLOCK_LEN {
                self.encrypt_and_emit()?;
            }
        }
        Ok(consumed)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

/// main 容器流式写面。`finish` 之前帧一直可写；`finish` 消费自身。
pub struct BackupStreamWriter<W: Write> {
    gzip: GzEncoder<CountingWriter<CbcMacWriter<W>>>,
    iv: [u8; IV_LEN],
}

impl<W: Write> BackupStreamWriter<W> {
    pub fn new(mut out: W, message_key: &MessageBackupKey) -> io::Result<Self> {
        let mut iv = [0u8; IV_LEN];
        rand::rng().fill_bytes(&mut iv);
        let mut mac = Hmac::<Sha256>::new_from_slice(&message_key.hmac_key)
            .expect("HMAC-SHA256 accepts any key size");
        mac.update(&iv);
        out.write_all(&iv)?;
        let cbc = CbcMacWriter::new(out, &message_key.aes_key, &iv, mac);
        Ok(Self {
            gzip: GzEncoder::new(
                CountingWriter {
                    inner: cbc,
                    count: 0,
                },
                Compression::new(GZIP_LEVEL),
            ),
            iv,
        })
    }

    /// 写一帧（已 protobuf 编码的 `Frame`/`BackupInfo`），自动加 varint32
    /// 长度前缀。
    pub fn write_frame(&mut self, encoded: &[u8]) -> io::Result<()> {
        let mut prefix = Vec::with_capacity(5);
        write_varint32(&mut prefix, encoded.len() as u32);
        self.gzip.write_all(&prefix)?;
        self.gzip.write_all(encoded)
    }

    /// 收尾：gzip 流结束 → 按 `padded_length` 补零 → PKCS7 → HMAC 尾。
    pub fn finish(self) -> io::Result<W> {
        let BackupStreamWriter { gzip, iv } = self;
        let _ = &iv;
        let counting = gzip.finish()?;
        let compressed_len = counting.count;
        let mut cbc = counting.inner;

        // 官方 v0.99.0 padded_length：min 541，1.05 bucket。>4GB fail-closed。
        let bucket = libsignal_message_backup::padded_length(
            u32::try_from(compressed_len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup over 4GiB"))?,
        );
        let mut remaining = bucket as u64 - compressed_len;
        let zeros = [0u8; PAD_CHUNK];
        while remaining > 0 {
            let take = remaining.min(PAD_CHUNK as u64) as usize;
            cbc.write_all(&zeros[..take])?;
            remaining -= take as u64;
        }

        cbc.finalize()
    }

    /// IV（测试交叉验证面；写出流里 IV 在文件头）。
    #[cfg(test)]
    pub(crate) fn iv(&self) -> &[u8; IV_LEN] {
        &self.iv
    }
}

#[cfg(test)]
mod tests {
    use libsignal_message_backup::frame::{CursorFactory, FramesReader};
    use libsignal_message_backup::parse::VarintDelimitedReader;

    use super::*;
    use crate::backup::keys::tests_official_keys;

    fn message_key() -> MessageBackupKey {
        tests_official_keys().message_key
    }

    async fn read_back(archive: &[u8], key: &MessageBackupKey) -> Vec<Vec<u8>> {
        let factory = CursorFactory::new(archive);
        let frames = FramesReader::new(key, factory)
            .await
            .expect("container valid");
        let mut delimited = VarintDelimitedReader::new(frames);
        let mut out = Vec::new();
        while let Some(frame) = delimited.read_next().await.expect("frame readable") {
            out.push(frame.into_vec());
        }
        out
    }

    #[tokio::test]
    async fn official_reader_reads_back_frames_and_hmac_verifies() {
        let key = message_key();
        let expected: Vec<Vec<u8>> =
            vec![b"frame-one".to_vec(), b"frame-two".to_vec(), vec![0u8; 300]];

        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        for frame in &expected {
            writer.write_frame(frame).expect("write");
        }
        let archive = writer.finish().expect("finish");

        assert_eq!(read_back(&archive, &key).await, expected);
    }

    #[tokio::test]
    async fn small_payload_is_padded_to_official_bucket_floor() {
        let key = message_key();
        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        writer.write_frame(b"tiny").expect("write");
        let archive = writer.finish().expect("finish");

        // 文件 = IV + CBC(PKCS7(gzip ‖ pad)) + HMAC。bucket 下限 541 保证
        // 密文覆盖的明文至少 541 字节（PKCS7 整块向上取整）。
        let padded_plaintext = archive.len() - IV_LEN - HMAC_LEN;
        assert!(
            padded_plaintext >= 541,
            "padded plaintext {padded_plaintext} < 541"
        );

        let expected: Vec<Vec<u8>> = vec![b"tiny".to_vec()];
        assert_eq!(read_back(&archive, &key).await, expected);
    }

    #[test]
    fn tail_hmac_matches_official_export_primitive() {
        // 官方 export.rs hmac_checksum(hmac_key, iv, ciphertext) 必须等于
        // 我们写出流的尾部 32 字节——HMAC 覆盖域逐字节一致。
        use libsignal_message_backup::export::hmac_checksum;

        let hmac_key = tests_official_keys().message_key.hmac_key;
        let key = MessageBackupKey {
            hmac_key,
            aes_key: tests_official_keys().message_key.aes_key,
        };
        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        writer.write_frame(b"cross-check").expect("write");
        let iv = *writer.iv();
        let archive = writer.finish().expect("finish");

        let (ciphertext, tail) = archive.split_at(archive.len() - HMAC_LEN);
        let expected = hmac_checksum(&hmac_key, &iv, &ciphertext[IV_LEN..]);
        assert_eq!(tail, &expected[..]);
    }

    #[tokio::test]
    async fn corrupted_payload_fails_hmac() {
        let key = message_key();
        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        writer.write_frame(b"intact").expect("write");
        let mut archive = writer.finish().expect("finish");
        let last = archive.len() - 1;
        archive[last] ^= 0xff;

        let factory = CursorFactory::new(&archive);
        assert!(FramesReader::new(&key, factory).await.is_err());
    }

    #[tokio::test]
    async fn hmac_key_mismatch_fails_container_open() {
        let key = message_key();
        let mut writer = BackupStreamWriter::new(Vec::new(), &key).expect("writer");
        writer.write_frame(b"secret").expect("write");
        let archive = writer.finish().expect("finish");

        let mut hmac_key = key.hmac_key;
        hmac_key[0] ^= 0xff;
        let wrong = MessageBackupKey {
            hmac_key,
            aes_key: key.aes_key,
        };
        let factory = CursorFactory::new(&archive);
        assert!(FramesReader::new(&wrong, factory).await.is_err());
    }
}
