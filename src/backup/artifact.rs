// SPDX-License-Identifier: AGPL-3.0-only

//! 输入工件（`docs/backup-export-contract.md` §10.2）：connector 写入的
//! ndjson 行流 → 帧序状态机装配。引擎只做「全部成帧或整体失败」——任何
//! 行级/形状/引用/帽值违规一律 [`ArtifactError`] fail-closed（跳行决策只在
//! connector 建工件时做，契约 §10.2）。
//!
//! 过渡性边界（engine AGENTS.md：不持久化明文聊天内容）：本模块只读工件，
//! 删除时机归任务层（`service::backup_task`，任务离开 running 态即删）。

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;
use serde_json::{Map, Value};

use super::export::MainStreamWriter;
use super::frames::ChatItemDirection;
use super::BackupError;

/// 工件文件名（与 backup5 出方向的 `history-import.ndjson` 同目录对称，
/// 契约 §10.2）。
pub(crate) const ARTIFACT_FILE_NAME: &str = "backup-export-input.ndjson";

/// 契约 §10.2 帽值（两端文档与实现一致）。
pub(crate) const MAX_LINE_BYTES: usize = 1_048_576;
pub(crate) const MAX_TOTAL_LINES: u64 = 4_000_000;
pub(crate) const MAX_TOTAL_BYTES: u64 = 2_147_483_648;

/// 工件行错误（任务层统一折叠为 `BACKUP_ARTIFACT_INVALID`，契约 §10.7）。
#[derive(Debug, thiserror::Error)]
pub(crate) enum ArtifactError {
    #[error("artifact file is empty")]
    EmptyArtifact,
    #[error("first artifact line must be an account line")]
    FirstLineNotAccount,
    #[error("artifact line has a missing or unknown \"type\" discriminator")]
    UnknownType,
    #[error("artifact line field {0} has an invalid value")]
    InvalidField(&'static str),
    #[error("artifact line exceeds the 1 MiB per-line cap")]
    LineTooLong,
    #[error("artifact exceeds the 4,000,000 line cap")]
    TooManyLines,
    #[error("artifact exceeds the 2 GiB total cap")]
    ArtifactTooLarge,
    #[error("artifact line is not valid JSON or has a field type mismatch")]
    Json(#[from] serde_json::Error),
    #[error("artifact hex field is not lowercase hex of the required length")]
    BadHex,
    #[error("artifact reference id {0} declared more than once")]
    DuplicateRef(u64),
    #[error("artifact reference id 0 is reserved for the engine self recipient")]
    SelfRefReserved,
    #[error("artifact reference id {0} used before declaration")]
    DanglingReference(u64),
    #[error(transparent)]
    FrameSequence(#[from] super::frames::FrameSequenceError),
    #[error("artifact io error: {0}")]
    Io(#[from] std::io::Error),
}

/// 装配统计（exportStatus 的 `lines`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AssemblyStats {
    pub lines: u64,
}

/// 帽值参数化（生产走 [`CONTRACT_LIMITS`]；测试用小帽验证 fail-closed 路径）。
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_line_bytes: usize,
    max_total_lines: u64,
    max_total_bytes: u64,
}

const CONTRACT_LIMITS: Limits = Limits {
    max_line_bytes: MAX_LINE_BYTES,
    max_total_lines: MAX_TOTAL_LINES,
    max_total_bytes: MAX_TOTAL_BYTES,
};

// ---------------------------------------------------------------------------
// 行解析（`type` 判别 + 未知键拒收；serde 内部 tag 与 deny_unknown_fields
// 不兼容，先取 Map 摘除 "type" 再按形状结构体反序列化）
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AccountLine {
    profile_key: String,
    given_name: Option<String>,
    family_name: Option<String>,
    bio_text: Option<String>,
    bio_emoji: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ContactLine {
    id: u64,
    aci: Option<String>,
    pni: Option<String>,
    e164: Option<u64>,
    username: Option<String>,
    profile_key: Option<String>,
    given_name: Option<String>,
    family_name: Option<String>,
    registration: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct GroupLine {
    id: u64,
    master_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ChatLine {
    id: u64,
    recipient: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ChatItemLine {
    chat: u64,
    author: u64,
    date_sent: u64,
    direction: String,
    date_received: u64,
    read: Option<bool>,
    body: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AllChatFolderLine {}

enum ParsedLine {
    Account(AccountLine),
    Contact(ContactLine),
    Group(GroupLine),
    Chat(ChatLine),
    ChatItem(ChatItemLine),
    AllChatFolder(AllChatFolderLine),
}

fn parse_line(buf: &[u8]) -> Result<ParsedLine, ArtifactError> {
    let mut map: Map<String, Value> = serde_json::from_slice(buf)?;
    let tag = map.remove("type").ok_or(ArtifactError::UnknownType)?;
    let tag = tag.as_str().ok_or(ArtifactError::UnknownType)?;
    let body = Value::Object(map);
    Ok(match tag {
        "account" => ParsedLine::Account(serde_json::from_value(body)?),
        "contact" => ParsedLine::Contact(serde_json::from_value(body)?),
        "group" => ParsedLine::Group(serde_json::from_value(body)?),
        "chat" => ParsedLine::Chat(serde_json::from_value(body)?),
        "chat_item" => ParsedLine::ChatItem(serde_json::from_value(body)?),
        "all_chat_folder" => ParsedLine::AllChatFolder(serde_json::from_value(body)?),
        _ => return Err(ArtifactError::UnknownType),
    })
}

/// N 字节 = 2N 位小写 hex（契约 §10.2；大写/越界/长度不符一律 [`ArtifactError::BadHex`]）。
fn hex_bytes<const N: usize>(value: &str) -> Result<[u8; N], ArtifactError> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ArtifactError::BadHex);
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ArtifactError::BadHex)?;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 有界按行读（单行超帽即刻失败，不预分配无界缓冲）
// ---------------------------------------------------------------------------

/// 读一行（含换行剥离）。`Ok(true)` = 拿到一行（存 `buf`）；`Ok(false)` =
/// EOF 且无残留行。行超帽即刻 [`ArtifactError::LineTooLong`]（任务注定失败，
/// 不再吞到行尾——删除工件是调用方的收尾动作）。
fn read_line_bounded(
    reader: &mut impl BufRead,
    buf: &mut Vec<u8>,
    max_line_bytes: usize,
) -> Result<bool, ArtifactError> {
    buf.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            // EOF：无换行结尾的最后一行也作一行交出。
            return Ok(!buf.is_empty());
        }
        match available.iter().position(|&byte| byte == b'\n') {
            Some(position) => {
                buf.extend_from_slice(&available[..position]);
                reader.consume(position + 1);
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
                if buf.len() > max_line_bytes {
                    return Err(ArtifactError::LineTooLong);
                }
                return Ok(true);
            }
            None => {
                let filled = available.len();
                buf.extend_from_slice(available);
                reader.consume(filled);
                if buf.len() > max_line_bytes {
                    return Err(ArtifactError::LineTooLong);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 装配
// ---------------------------------------------------------------------------

/// start 阶段首行同步校验（契约 §10.4）：首行可解析为合法 account 行返回
/// `Ok(())`；否则返回错误，由调用方当场删工件 + `BACKUP_ARTIFACT_INVALID`。
pub(crate) fn validate_first_line(path: &Path) -> Result<(), ArtifactError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = Vec::new();
    if !read_line_bounded(&mut reader, &mut buf, MAX_LINE_BYTES)? {
        return Err(ArtifactError::EmptyArtifact);
    }
    match parse_line(&buf)? {
        ParsedLine::Account(_) => Ok(()),
        _ => Err(ArtifactError::FirstLineNotAccount),
    }
}

/// 装配整份工件到 `out`（契约帽值）。account 行后自动发 self recipient
/// （id=1）；`all_chat_folder` 缺省时收尾自动补一帧（契约 §10.2）。错误面
/// 是 [`BackupError`]：行级违规经 `Artifact` 变体透传，帧序/容器错误原样。
pub(crate) fn assemble<W: std::io::Write>(
    out: &mut MainStreamWriter<W>,
    reader: impl BufRead,
    backup_time_ms: u64,
) -> Result<AssemblyStats, BackupError> {
    assemble_with_limits(out, reader, backup_time_ms, CONTRACT_LIMITS)
}

fn assemble_with_limits<W: std::io::Write>(
    out: &mut MainStreamWriter<W>,
    mut reader: impl BufRead,
    backup_time_ms: u64,
    limits: Limits,
) -> Result<AssemblyStats, BackupError> {
    let mut stats = AssemblyStats { lines: 0 };
    let mut total_bytes: u64 = 0;
    let mut buf: Vec<u8> = Vec::new();

    // 引用登记：connector 行 id → 引擎 recipient/chat id（被引用者必须先于
    // 引用者，契约 §10.2；帧序状态机再做二次兜底）。
    enum RefKind {
        Contact,
        Group,
    }
    let mut refs: HashMap<u64, (u64, RefKind)> = HashMap::new();
    let mut chats: HashMap<u64, u64> = HashMap::new();
    let mut self_engine_id: Option<u64> = None;
    let mut folder_emitted = false;

    // 引用解析：0 = self recipient；其余必须已声明（contact/group 命名空间）。
    let resolve = |id: u64,
                   refs: &HashMap<u64, (u64, RefKind)>,
                   self_engine_id: Option<u64>|
     -> Result<u64, ArtifactError> {
        if id == 0 {
            return self_engine_id.ok_or(ArtifactError::FirstLineNotAccount);
        }
        refs.get(&id)
            .map(|(engine_id, _)| *engine_id)
            .ok_or(ArtifactError::DanglingReference(id))
    };

    while read_line_bounded(&mut reader, &mut buf, limits.max_line_bytes)? {
        stats.lines += 1;
        // 换行符计入总字节（末行无换行的差一计入忽略：相对 2 GiB 帽无意义，
        // 只做 runaway 熔断）。
        total_bytes += u64::try_from(buf.len()).expect("line fits u64") + 1;
        if stats.lines > limits.max_total_lines {
            return Err(ArtifactError::TooManyLines.into());
        }
        if total_bytes > limits.max_total_bytes {
            return Err(ArtifactError::ArtifactTooLarge.into());
        }
        match parse_line(&buf)? {
            ParsedLine::Account(line) => {
                // 首行必须且仅一行 account（契约 §10.2）：工件层显式锁位
                // （状态机的 recipient/chat 面不查 account 位置）。
                if stats.lines > 1 {
                    return Err(ArtifactError::FirstLineNotAccount.into());
                }
                out.account_data(crate::proto::AccountData {
                    profile_key: hex_bytes::<32>(&line.profile_key)?.to_vec(),
                    given_name: line.given_name.unwrap_or_default(),
                    family_name: line.family_name.unwrap_or_default(),
                    bio_text: line.bio_text.unwrap_or_default(),
                    bio_emoji: line.bio_emoji.unwrap_or_default(),
                    ..Default::default()
                })?;
                self_engine_id = Some(out.self_recipient()?);
            }
            ParsedLine::Contact(line) => {
                if line.id == 0 {
                    return Err(ArtifactError::SelfRefReserved.into());
                }
                if refs.contains_key(&line.id) {
                    return Err(ArtifactError::DuplicateRef(line.id).into());
                }
                if line.aci.is_none() && line.pni.is_none() && line.e164.is_none() {
                    // 状态机 ContactWithoutIdentity 前置：行级先拒，错误面一致。
                    return Err(ArtifactError::InvalidField("aci/pni/e164").into());
                }
                // 官方 validator 对缺席 registration 硬错
                //（ContactRegistrationUnknown）：缺省补 Registered（保守合法
                // 投影；connector 未跟踪注册态的联系人按可注册导出）。
                let registration = match line.registration.as_deref() {
                    None => Some(crate::proto::contact::Registration::Registered(
                        crate::proto::contact::Registered {},
                    )),
                    Some("registered") => Some(crate::proto::contact::Registration::Registered(
                        crate::proto::contact::Registered {},
                    )),
                    // validator 对 NotRegistered.unregisteredTimestamp 的非零
                    // 口径：引擎没有解绑时刻，盖 backup_time_ms（「导出时刻
                    // 已知未注册」，确定性且落在合法区间）。
                    Some("unregistered") => {
                        Some(crate::proto::contact::Registration::NotRegistered(
                            crate::proto::contact::NotRegistered {
                                unregistered_timestamp: backup_time_ms,
                            },
                        ))
                    }
                    Some(_) => return Err(ArtifactError::InvalidField("registration").into()),
                };
                let engine_id = out.contact_recipient(crate::proto::Contact {
                    aci: line
                        .aci
                        .as_deref()
                        .map(hex_bytes::<16>)
                        .transpose()?
                        .map(|bytes| bytes.to_vec()),
                    pni: line
                        .pni
                        .as_deref()
                        .map(hex_bytes::<16>)
                        .transpose()?
                        .map(|bytes| bytes.to_vec()),
                    e164: line.e164,
                    username: line.username,
                    profile_key: line
                        .profile_key
                        .as_deref()
                        .map(hex_bytes::<32>)
                        .transpose()?
                        .map(|bytes| bytes.to_vec()),
                    profile_given_name: line.given_name,
                    profile_family_name: line.family_name,
                    registration,
                    ..Default::default()
                })?;
                refs.insert(line.id, (engine_id, RefKind::Contact));
            }
            ParsedLine::Group(line) => {
                if line.id == 0 {
                    return Err(ArtifactError::SelfRefReserved.into());
                }
                if refs.contains_key(&line.id) {
                    return Err(ArtifactError::DuplicateRef(line.id).into());
                }
                let engine_id = out.group_recipient(hex_bytes::<32>(&line.master_key)?)?;
                refs.insert(line.id, (engine_id, RefKind::Group));
            }
            ParsedLine::Chat(line) => {
                // chat id 是独立命名空间（proto 语义同构），同款 0 保留 + 查重。
                if line.id == 0 {
                    return Err(ArtifactError::SelfRefReserved.into());
                }
                if chats.contains_key(&line.id) {
                    return Err(ArtifactError::DuplicateRef(line.id).into());
                }
                let recipient_engine_id = resolve(line.recipient, &refs, self_engine_id)?;
                let engine_id = out.chat(recipient_engine_id)?;
                chats.insert(line.id, engine_id);
            }
            ParsedLine::ChatItem(line) => {
                let chat_engine_id = chats
                    .get(&line.chat)
                    .copied()
                    .ok_or(ArtifactError::DanglingReference(line.chat))?;
                let author_engine_id = resolve(line.author, &refs, self_engine_id)?;
                let direction = match line.direction.as_str() {
                    "incoming" => ChatItemDirection::Incoming {
                        date_received_ms: line.date_received,
                        read: line.read.unwrap_or(false),
                    },
                    "outgoing" => ChatItemDirection::Outgoing {
                        date_received_ms: line.date_received,
                    },
                    _ => return Err(ArtifactError::InvalidField("direction").into()),
                };
                // body 非空且 ≤128 KiB 由状态机强制（EmptyMessageBody/
                // MessageBodyTooLong → BACKUP_ARTIFACT_INVALID）。
                out.chat_item_text(
                    chat_engine_id,
                    author_engine_id,
                    line.date_sent,
                    direction,
                    &line.body,
                )?;
            }
            ParsedLine::AllChatFolder(_) => {
                out.all_chat_folder()?;
                folder_emitted = true;
            }
        }
    }

    if stats.lines == 0 {
        return Err(ArtifactError::EmptyArtifact.into());
    }
    if self_engine_id.is_none() {
        // 全文件没有任何 account 行（首行是其他类型或直到 EOF 都没出现）。
        return Err(ArtifactError::FirstLineNotAccount.into());
    }
    if !folder_emitted {
        out.all_chat_folder()?;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use libsignal_message_backup::backup::Purpose;
    use libsignal_message_backup::frame::CursorFactory;
    use libsignal_message_backup::BackupReader;

    use super::*;
    use crate::backup::export::{write_main_stream, LocalBackupParams};
    use crate::backup::frames::FrameSequenceError;
    use crate::backup::keys::tests_official_keys;

    const ACI_HEX: &str = "659aa5f4a28dfcc11ea1b997537a3d95";
    const MASTER_KEY_HEX: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    const BACKUP_TIME_MS: u64 = 1_760_000_000_000;

    fn params() -> LocalBackupParams {
        LocalBackupParams {
            keys: tests_official_keys(),
            backup_time_ms: BACKUP_TIME_MS,
            media_root_backup_key: [0x42; 32],
            app_version: "kt-signal-engine 0.1.0".into(),
        }
    }

    fn account_line() -> String {
        format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\",\"givenName\":\"Kit\",\"bioText\":\"hi\"}}",
            "11".repeat(32)
        )
    }

    fn contact_line(id: u64, with_aci: bool, with_e164: bool) -> String {
        let mut fields = vec!["\"type\":\"contact\"".to_owned(), format!("\"id\":{id}")];
        if with_aci {
            fields.push(format!("\"aci\":\"{ACI_HEX}\""));
        }
        if with_e164 {
            fields.push("\"e164\":15555550100".to_owned());
        }
        fields.push("\"givenName\":\"Ada\"".to_owned());
        format!("{{{}}}", fields.join(","))
    }

    fn unregistered_contact_line(id: u64) -> String {
        format!(
            "{{\"type\":\"contact\",\"id\":{id},\"e164\":15555550199,\"registration\":\"unregistered\"}}"
        )
    }

    fn group_line(id: u64) -> String {
        format!("{{\"type\":\"group\",\"id\":{id},\"masterKey\":\"{MASTER_KEY_HEX}\"}}")
    }

    fn chat_line(id: u64, recipient: u64) -> String {
        format!("{{\"type\":\"chat\",\"id\":{id},\"recipient\":{recipient}}}")
    }

    fn incoming_item_line(chat: u64, author: u64, body: &str) -> String {
        format!(
            "{{\"type\":\"chat_item\",\"chat\":{chat},\"author\":{author},\"dateSent\":1760000001000,\
             \"direction\":\"incoming\",\"dateReceived\":1760000001005,\"read\":true,\"body\":\"{body}\"}}"
        )
    }

    fn outgoing_item_line(chat: u64, author: u64) -> String {
        format!(
            "{{\"type\":\"chat_item\",\"chat\":{chat},\"author\":{author},\"dateSent\":1760000002000,\
             \"direction\":\"outgoing\",\"dateReceived\":1760000002010,\"body\":\"back\"}}"
        )
    }

    fn artifact_bytes(lines: &[String]) -> Vec<u8> {
        let mut bytes = lines.join("\n").into_bytes();
        bytes.push(b'\n');
        bytes
    }

    /// MVP 金样：account + contact（registered）+ contact（unregistered）+
    /// group + 三 chat（self/contact/group）+ 三 chat_item；无 folder 行
    ///（收尾自动补）。
    fn golden_lines() -> Vec<String> {
        vec![
            account_line(),
            contact_line(10, true, true),
            unregistered_contact_line(11),
            group_line(12),
            chat_line(100, 0),
            chat_line(101, 10),
            chat_line(102, 12),
            incoming_item_line(101, 10, "hello from contact"),
            outgoing_item_line(100, 0),
            incoming_item_line(102, 10, "group text"),
        ]
    }

    /// 装配到内存档案，返回（档案字节、行数、帧数）。
    fn assemble_archive(lines: &[String]) -> (Vec<u8>, u64, u64) {
        let artifact = artifact_bytes(lines);
        let p = params();
        let mut seen_lines = 0u64;
        let (sink, stats) = write_main_stream(Vec::new(), &p, |out| {
            let assembled = assemble(out, Cursor::new(&artifact), p.backup_time_ms)?;
            seen_lines = assembled.lines;
            Ok(())
        })
        .expect("golden export");
        (sink, seen_lines, stats.frame_count)
    }

    fn assemble_error(lines: &[String]) -> BackupError {
        let artifact = artifact_bytes(lines);
        let p = params();
        match write_main_stream(Vec::new(), &p, |out| {
            assemble(out, Cursor::new(&artifact), p.backup_time_ms)?;
            Ok(())
        }) {
            Err(error) => error,
            Ok(_) => panic!("expected artifact failure"),
        }
    }

    /// 小帽装配（帽值路径专用；写面丢弃输出）。
    fn assemble_limited(lines: &[String], caps: Limits) -> BackupError {
        let artifact = artifact_bytes(lines);
        let p = params();
        match write_main_stream(NullWriter, &p, |out| {
            assemble_with_limits(out, Cursor::new(&artifact), p.backup_time_ms, caps)?;
            Ok(())
        }) {
            Err(error) => error,
            Ok(_) => panic!("expected artifact failure"),
        }
    }

    fn artifact_error_of(result: BackupError) -> ArtifactError {
        match result {
            BackupError::Artifact(error) => error,
            other => panic!("expected artifact error, got {other:?}"),
        }
    }

    fn frame_error_of(result: BackupError) -> FrameSequenceError {
        match result {
            BackupError::FrameSequence(error) => error,
            other => panic!("expected frame sequence error, got {other:?}"),
        }
    }

    /// 抽象验收：官方 `BackupReader::validate_all` 双 Purpose 全绿。
    async fn assert_official_validation(sink: &[u8]) {
        let p = params();
        for purpose in [Purpose::DeviceTransfer, Purpose::RemoteBackup] {
            let factory = CursorFactory::new(sink);
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
        }
    }

    /// 端到端金样：工件 → 档案 → 官方 validator 双 Purpose 全绿（含
    /// unregistered contact 的引擎盖时间戳、Note to Self、群 chat）。
    #[tokio::test]
    async fn golden_artifact_assembles_into_officially_valid_archive() {
        let (sink, lines, frames) = assemble_archive(&golden_lines());
        // 1 BackupInfo + 1 AccountData + 4 Recipient(self+2contact+group)
        // + 3 Chat + 3 ChatItem + 1 ChatFolder(自动补) = 13。
        assert_eq!(frames, 13);
        assert_eq!(lines, 10);
        assert_official_validation(&sink).await;
    }

    /// 显式 all_chat_folder 行：不再自动补（validator 恰一 folder 通过）。
    #[tokio::test]
    async fn explicit_all_chat_folder_is_emitted_once() {
        let mut lines = golden_lines();
        lines.push("{\"type\":\"all_chat_folder\"}".to_owned());
        let (sink, _lines, frames) = assemble_archive(&lines);
        assert_eq!(frames, 13);
        assert_official_validation(&sink).await;
    }

    /// 帽值 fail-closed（参数化小帽验证三条帽路径）。
    #[test]
    fn caps_fail_closed() {
        // 单行超帽：读取阶段即拒（不解析、不预分配无界缓冲）。
        let long_line = format!("\"{}\"", "x".repeat(64));
        let err = assemble_limited(&[long_line], limits(16, 1_000, 1_000_000));
        assert!(matches!(artifact_error_of(err), ArtifactError::LineTooLong));

        // 总行数超帽：三行互异 e164-only contact（避免身份重复判定先触发）。
        let distinct = |id: u64, e164: u64| {
            format!(
                "{{\"type\":\"contact\",\"id\":{id},\"e164\":{e164},\"registration\":\"registered\"}}"
            )
        };
        let err = assemble_limited(
            &[
                distinct(1, 15555550101),
                distinct(2, 15555550102),
                distinct(3, 15555550103),
            ],
            limits(1_048_576, 2, 1_000_000_000),
        );
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::TooManyLines
        ));

        // 总字节超帽。
        let err = assemble_limited(
            &[contact_line(1, true, false)],
            limits(1_048_576, 1_000, 64),
        );
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::ArtifactTooLarge
        ));
    }

    /// 行级形状面 fail-closed（逐错误变体）。
    #[test]
    fn line_shapes_fail_closed() {
        // 首行非 account。
        let err = assemble_error(&[contact_line(1, true, false)]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::FirstLineNotAccount
        ));
        // 二行 account（首行锁位）。
        let err = assemble_error(&[account_line(), account_line()]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::FirstLineNotAccount
        ));
        // 未知 type / 缺 type / 未知键。
        let err = assemble_error(&[account_line(), "{\"type\":\"sticker_pack\"}".to_owned()]);
        assert!(matches!(artifact_error_of(err), ArtifactError::UnknownType));
        let err = assemble_error(&["{\"profileKey\":\"x\"}".to_owned()]);
        assert!(matches!(artifact_error_of(err), ArtifactError::UnknownType));
        let err = assemble_error(&[format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\",\"extra\":1}}",
            "11".repeat(32)
        )]);
        assert!(matches!(artifact_error_of(err), ArtifactError::Json(_)));
        // 坏 hex（大写 / 长度错）。
        let err = assemble_error(&[format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\"}}",
            "A".repeat(64)
        )]);
        assert!(matches!(artifact_error_of(err), ArtifactError::BadHex));
        let err = assemble_error(&[format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\"}}",
            "11".repeat(31)
        )]);
        assert!(matches!(artifact_error_of(err), ArtifactError::BadHex));
        // id=0 保留 / 重复 id。
        let err = assemble_error(&[account_line(), contact_line(0, true, false)]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::SelfRefReserved
        ));
        let err = assemble_error(&[
            account_line(),
            contact_line(7, true, false),
            contact_line(7, true, false),
        ]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::DuplicateRef(7)
        ));
        // 引用先于声明 / chat 帽下同款。
        let err = assemble_error(&[
            account_line(),
            chat_line(1, 10),
            contact_line(10, true, false),
        ]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::DanglingReference(10)
        ));
        // registration/direction 非法枚举。
        let err = assemble_error(&[
            "{\"type\":\"contact\",\"id\":1,\"e164\":15555550100,\"registration\":\"bogus\"}"
                .to_owned(),
        ]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::InvalidField("registration")
        ));
        let err = assemble_error(&[
            account_line(),
            contact_line(10, true, false),
            chat_line(1, 10),
            "{\"type\":\"chat_item\",\"chat\":1,\"author\":10,\"dateSent\":5,\
             \"direction\":\"sideways\",\"dateReceived\":6,\"body\":\"x\"}"
                .to_owned(),
        ]);
        assert!(matches!(
            artifact_error_of(err),
            ArtifactError::InvalidField("direction")
        ));
    }

    /// 帧序状态机的 fail-closed 穿透（同码 BACKUP_ARTIFACT_INVALID 的来源面）。
    #[test]
    fn frame_sequence_violations_fail_closed() {
        // 空 body / 超 128 KiB body。
        let err = assemble_error(&[
            account_line(),
            contact_line(10, true, false),
            chat_line(1, 10),
            "{\"type\":\"chat_item\",\"chat\":1,\"author\":10,\"dateSent\":5,\
             \"direction\":\"incoming\",\"dateReceived\":6,\"body\":\"\"}"
                .to_owned(),
        ]);
        assert!(matches!(
            frame_error_of(err),
            FrameSequenceError::EmptyMessageBody
        ));
        let err = assemble_error(&[
            account_line(),
            contact_line(10, true, false),
            chat_line(1, 10),
            format!(
                "{{\"type\":\"chat_item\",\"chat\":1,\"author\":10,\"dateSent\":5,\
                 \"direction\":\"incoming\",\"dateReceived\":6,\"body\":\"{}\"}}",
                "x".repeat(128 * 1024 + 1)
            ),
        ]);
        assert!(matches!(
            frame_error_of(err),
            FrameSequenceError::MessageBodyTooLong(_)
        ));
        // group 作 author（官方 v2 归档群消息署名到个人）；断言的是引擎侧
        // 分配的 recipient id（group 12 → 引擎 id 2）。
        let err = assemble_error(&[
            account_line(),
            group_line(12),
            chat_line(1, 12),
            "{\"type\":\"chat_item\",\"chat\":1,\"author\":12,\"dateSent\":5,\
             \"direction\":\"incoming\",\"dateReceived\":6,\"body\":\"x\"}"
                .to_owned(),
        ]);
        assert!(matches!(
            frame_error_of(err),
            FrameSequenceError::InvalidChatAuthor(2)
        ));
        // 空白行 = 非法 JSON 行（永不静默丢行）。
        let err = assemble_error(&["".to_owned()]);
        assert!(matches!(artifact_error_of(err), ArtifactError::Json(_)));
    }

    /// 零字节工件：EmptyArtifact（start 首行校验同款口径）。
    #[test]
    fn empty_artifact_fails_closed() {
        let p = params();
        let result = write_main_stream(Vec::new(), &p, |out| {
            assemble(out, Cursor::new(Vec::new()), p.backup_time_ms)?;
            Ok(())
        });
        assert!(matches!(
            match result {
                Err(error) => error,
                Ok(_) => panic!("expected artifact failure"),
            },
            BackupError::Artifact(ArtifactError::EmptyArtifact)
        ));
    }

    /// start 阶段首行同步校验：合法 account 行放行，其余报错。
    #[test]
    fn validate_first_line_gates_account_only() {
        let dir = unique_test_dir("first-line");
        std::fs::create_dir_all(&dir).expect("dir");

        let path = dir.join(ARTIFACT_FILE_NAME);
        std::fs::write(&path, artifact_bytes(&[account_line()])).expect("artifact");
        validate_first_line(&path).expect("valid first line");

        std::fs::write(&path, artifact_bytes(&[contact_line(1, true, false)])).expect("artifact");
        assert!(matches!(
            validate_first_line(&path),
            Err(ArtifactError::FirstLineNotAccount)
        ));

        std::fs::write(&path, b"").expect("empty artifact");
        assert!(matches!(
            validate_first_line(&path),
            Err(ArtifactError::EmptyArtifact)
        ));

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    fn limits(max_line_bytes: usize, max_total_lines: u64, max_total_bytes: u64) -> Limits {
        Limits {
            max_line_bytes,
            max_total_lines,
            max_total_bytes,
        }
    }

    fn unique_test_dir(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-signal-engine-artifact-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    /// 无操作写面（帽值路径不需要真档案）。
    struct NullWriter;

    impl std::io::Write for NullWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
