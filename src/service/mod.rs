// SPDX-License-Identifier: AGPL-3.0-only

//! 服务层：P1b 双实现（ADR 0003/0004）。
//!
//! - `Real`：presage 真实链接（`Manager::link_secondary_device` 公开入口 +
//!   finishLink 后经 store/attributes 改名）、接收投影与发送管线。
//! - `Fake`：P1a 形状桩的原样保留，**仅**由 `KT_ENGINE_FAKE_SERVICE=1`
//!   显式开启，供契约测试在无网络环境下钉定 IPC 形状（compat §6）。
//!   生产路径永远走 Real。
//!
//! 纪律（compat §1）：本层不产生任何 stderr 输出；错误统一折叠为枚举，
//! 响应面错误码由 methods.rs 固定；不落正文/号码/URI/密钥。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures::StreamExt;
use presage::libsignal_service::configuration::{Endpoint, ServiceCredentials, SignalServers};
use presage::libsignal_service::content::ContentBody;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::protocol::{IdentityKeyPair, ServiceId};
use presage::libsignal_service::provisioning::{
    link_device, NewDeviceRegistration, SecondaryDeviceProvisioning,
};
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::libsignal_service::utils::phonenumber_from_signal;
use presage::manager::{Manager, Registered, RegistrationData};
use presage::model::identity::OnNewIdentity;
use presage::model::messages::Received;
use presage::proto::DataMessage;
use presage::store::{ContentsStore, StateStore, Store};
use presage_store_sqlite::SqliteStore;
use rand::distr::{Alphanumeric, SampleString};
use rand::rng;
use rand::Rng;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify};

use crate::state::EngineState;

pub(crate) mod avatar;
pub(crate) mod backup_task;
pub(crate) mod call_links;
pub(crate) mod capabilities;
pub(crate) mod cdn_upload;
pub(crate) mod delivery_receipt;
/// R1 群发本地缓存面：GSE 按群落 kv（官方 2h-2d 窗 + 成员面失效）+ 发送
/// 面 terminated 探针缓存。稳态群发 0 次额外 HTTPS。
pub(crate) mod group_cache;
pub(crate) mod group_link;
pub(crate) mod groups;
pub(crate) mod identity;
pub(crate) mod prekey;
pub(crate) mod profile;
pub(crate) mod report;
pub(crate) mod snapshot_import;
pub(crate) mod stickers;
pub(crate) mod storage_records;
pub(crate) mod storage_sticker;
pub(crate) mod storage_sync;
pub(crate) mod storage_write;
pub(crate) mod stories;
pub(crate) mod username;
pub(crate) mod zkgroup_consts;

/// C 批群发提速：sender-key + 单请求 MRM 发送链（vendored 逐收件人路径
/// 作官方合法 failover 兜底）。
pub(crate) mod group_mrm;

/// startLink 时刻的占位设备名（ADR 0004）：finishLink 拿到用户输入的名字后
/// 改写 store 注册数据，接收管线首推 attributes 时覆盖为真名。
const PLACEHOLDER_DEVICE_NAME: &str = "KT Signal Engine";

/// 出网 UA：官方审计面统一标识（引擎所有直连 REST/WS 共用）。
pub(crate) const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"));

/// 回执 timestamps 每帧上限：官方 sendReceipts.preload.ts CHUNK_SIZE=100，
/// 超出按官方分帧继续发（非 fail-closed）。
const RECEIPT_CHUNK_SIZE: usize = 100;

/// send 扩面 payload（compat §3 send 行，键名与 connector 上游同名直投）。
/// quoteType/quoteBodyRanges 两键（2026-10-11 六问批，官方 Quote.type 恒写
/// `isGiftBadge ? GIFT_BADGE : NORMAL`（SendMessage.preload.ts:506-509）、
/// bodyRanges 经 toBodyRange 同构）：词表缺省 normal（proto NORMAL=0 缺省
/// 同位），bodyRanges 条目见 SendQuoteBodyRange。
pub(crate) struct SendQuote {
    pub(crate) timestamp: u64,
    pub(crate) author: String,
    pub(crate) gift_badge: bool,
    pub(crate) body_ranges: Vec<SendQuoteBodyRange>,
}

/// quoteBodyRanges 条目（官方 toBodyRange 同构）：mention（number→ACI 解析）
/// 与 style（五值词表）恰一（官方 BodyRange 无关联值条目不产）；start/length
/// 为 UTF-16 code units（官方 BodyRange 同语义），透传不校验正文对齐
/// （mentions/textStyle 同款调用方纪律）。
pub(crate) struct SendQuoteBodyRange {
    pub(crate) mention: Option<String>,
    pub(crate) style: Option<String>,
    pub(crate) start: u32,
    pub(crate) length: u32,
}

pub(crate) struct SendPreview {
    pub(crate) url: String,
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    /// previewImage 数据 URI（data URI 同附件形状）；随同批 CDN 上行。
    pub(crate) image: Option<String>,
    /// P14 Preview.date 两端批：链接预览文章发布时刻（毫秒，>0）；缺席 =
    /// proto 字段省略（官方 desktop preview.date 同位）。
    pub(crate) date: Option<u64>,
}

/// mentions 条目：number 经 resolve_author_aci 转 ACI；start/length 为
/// UTF-16 code units（官方 BodyRange 同语义），透传不校验正文对齐。
pub(crate) struct SendMention {
    pub(crate) number: String,
    pub(crate) start: u32,
    pub(crate) length: u32,
}

/// send textStyle 条目（compat §3 send 行 textStyle 扩面，connector 契约
/// 1.69 §4.62）：键名与 connector 上游同名直投；style 为五值白名单词表
/// （methods.rs 解析闸钉定，此处透传），start/length 为 UTF-16 code units
/// （官方 BodyRange 同语义），透传不校验正文对齐。
pub(crate) struct SendTextStyle {
    pub(crate) style: String,
    pub(crate) start: u32,
    pub(crate) length: u32,
}

/// send textStyle 条目上限：connector `MAX_SEND_TEXT_STYLES` 同值（其 §4.20
/// receive cap 的发送侧镜像，契约 1.69 §4.62——超出整拒，不静默截断）。
pub(crate) const MAX_SEND_TEXT_STYLES: usize = 64;

/// P14 组 4：story 回复发送面（compat §3 send 行 storyReplyAuthor/
/// storyReplyTimestamp 扩面，官方 Desktop sendMessage storyContext 同位
/// SendMessage.preload.ts:565-577）。author 为被回复 story 作者 ACI
/// （resolve_author_aci 解析），sent_timestamp 为被回复 story 的发送
/// 时间戳毫秒；双参同进退（methods.rs 解析闸）。
pub(crate) struct SendStoryContext {
    pub(crate) author: String,
    pub(crate) sent_timestamp: u64,
}

/// send 贴纸参数（compat §3 send 行 sticker 扩面，键名与官方桌面
/// sendStickerMessage 序列化同构：packId hex（Bytes.fromHex 逆运算）、
/// packKey base64（Bytes.fromBase64 逆运算）、emoji、image data URI、
/// width/height（官方随 sticker.data AttachmentPointer 出站的展示元数据）。
/// 与 message/attachments/preview/mentions 互斥（官方 body=undefined、
/// attachments=[]、无 preview/bodyRanges），quote 可共存（官方 quote 选项）。
#[derive(Clone)]
pub(crate) struct SendSticker {
    pub(crate) pack_id: String,
    pub(crate) pack_key: String,
    pub(crate) sticker_id: u32,
    pub(crate) emoji: Option<String>,
    pub(crate) image: String,
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
}

/// sendCallMessage 参数（通话消息面，KT 扩展方法）：modern 面走 opaque 字节
/// （已 base64 解码；引擎不解析 opaque 内容，Signal-RTC 层信令职责，透传），
/// urgency 缺省 Droppable（proto 同缺省语义）；legacy 面走 busy/hangup 帧族
///（RingRTC 2.72.1 实证出向 busy/hangup 无 opaque 材料，官方 proto
/// Busy{id}/Hangup{id,type,deviceId} 裸帧族，id 由调用方给正 u32）。两面互斥
///（同帧同置 fail-closed）；destinationDeviceId 官方多设备定向投递位，缺省
/// 不写。
pub(crate) struct SendCallMessage {
    pub(crate) face: CallMessageFace,
    pub(crate) destination_device_id: Option<u32>,
}

/// sendCallMessage 的两面负载：modern = CallMessage{opaque{data, urgency}}；
/// legacy = 官方 proto 裸帧（busy 位或 hangup 帧，无 opaque 字段）。
pub(crate) enum CallMessageFace {
    Opaque {
        data: Vec<u8>,
        urgency: presage::proto::call_message::opaque::Urgency,
    },
    LegacyBusy {
        id: u64,
    },
    LegacyHangup {
        id: u64,
        kind: presage::proto::call_message::hangup::Type,
        device_id: Option<u32>,
    },
}

/// legacy call 帧面参数解析（sendCallMessage `legacy` 键，1.53 批）：官方
/// proto Busy{id}/Hangup{id,type,deviceId} 裸帧族（无 opaque 字段，桌面
/// callingMessageToProto busy={id}/hangup={id,type} 同款）。形状：
/// `{kind:"busy", id:正数}` | `{kind:"hangup", id:正数, type?, deviceId?}`。
/// hangup type 词汇投影收投影面（normal/accepted/declined/busy/
/// needPermission），缺省 normal（proto HANGUP_NORMAL=0 缺省同语义）。
/// 未知 kind / 形状错 / id≤0 / deviceId 超 u32 → None（fail-closed -32603）；
/// 键缺席 → None 不报错（modern 面路径）。
pub(crate) fn parse_legacy_call_face(value: Option<&Value>) -> Option<CallMessageFace> {
    use presage::proto::call_message::hangup::Type as HangupType;
    let value = value?;
    let kind = value.get("kind").and_then(Value::as_str)?;
    let id = value
        .get("id")
        .and_then(Value::as_u64)
        .filter(|id| *id > 0)?;
    match kind {
        "busy" => Some(CallMessageFace::LegacyBusy { id }),
        "hangup" => {
            let hangup_kind = match value.get("type") {
                None | Some(Value::Null) => HangupType::HangupNormal,
                Some(Value::String(text)) => match text.as_str() {
                    "normal" => HangupType::HangupNormal,
                    "accepted" => HangupType::HangupAccepted,
                    "declined" => HangupType::HangupDeclined,
                    "busy" => HangupType::HangupBusy,
                    "needPermission" => HangupType::HangupNeedPermission,
                    _ => return None,
                },
                Some(_) => return None,
            };
            let device_id = match value.get("deviceId") {
                None | Some(Value::Null) => None,
                Some(item) => item
                    .as_u64()
                    .and_then(|item| u32::try_from(item).ok())
                    .map(Some)?,
            };
            Some(CallMessageFace::LegacyHangup {
                id,
                kind: hangup_kind,
                device_id,
            })
        }
        _ => None,
    }
}

/// callMessage.opaque.data 解码：标准 base64、非空（官方 Opaque.data 必有
/// 信令负载；空负载无语义 fail-closed）。
pub(crate) fn decode_call_data(data: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data.as_bytes())
        .ok()?;
    (!bytes.is_empty()).then_some(bytes)
}

/// sendCallMessage 信封构造（1:1/群两臂共用单点）：face 逐面映射官方
/// CallMessage proto——opaque 面 {data, urgency}（urgency 写 Opaque.urgency，
/// 桌面 callingMessageToProto 同构）、legacy 面官方裸帧族无 opaque 字段；
/// destinationDeviceId 为 1:1 臂透传位（群臂 sender-key 群发无设备定向，
/// 参数层 fail-closed，群构面恒 None）。
fn build_call_message(payload: &SendCallMessage) -> presage::proto::CallMessage {
    match &payload.face {
        CallMessageFace::Opaque { data, urgency } => presage::proto::CallMessage {
            opaque: Some(presage::proto::call_message::Opaque {
                data: Some(data.clone()),
                urgency: Some(*urgency as i32),
            }),
            destination_device_id: payload.destination_device_id,
            ..presage::proto::CallMessage::default()
        },
        CallMessageFace::LegacyBusy { id } => presage::proto::CallMessage {
            busy: Some(presage::proto::call_message::Busy { id: Some(*id) }),
            destination_device_id: payload.destination_device_id,
            ..presage::proto::CallMessage::default()
        },
        CallMessageFace::LegacyHangup {
            id,
            kind,
            device_id,
        } => presage::proto::CallMessage {
            hangup: Some(presage::proto::call_message::Hangup {
                id: Some(*id),
                r#type: Some(*kind as i32),
                device_id: *device_id,
            }),
            destination_device_id: payload.destination_device_id,
            ..presage::proto::CallMessage::default()
        },
    }
}

/// 附件条数上限：与接收投影 §4.13/compat receive 32 条同口径，超出
/// fail-closed。
const ATTACHMENT_MAX_COUNT: usize = 32;

/// 单附件字节上限：官方桌面 100MB CDN 上限同源，超出 fail-closed。
const ATTACHMENT_MAX_BYTES: usize = 100_000_000;

struct ParsedAttachment {
    spec: presage::libsignal_service::sender::AttachmentSpec,
    bytes: Vec<u8>,
}

/// send 附件条目解析：data URI 唯一形状 = connector 上游形状
/// `data:<mediatype>[;filename=<percent-encoded>][;voice=true];base64,<payload>`
/// （RFC 2397 扩展 filename 参数同款；voice 参数映射官方 VOICE_MESSAGE
/// flags，presage voice_note→AttachmentPointer.flags 官方同构）。mediatype
/// 缺省 application/octet-stream；非 base64 / 坏 percent / 坏 base64 /
/// 坏 voice 值 / 超上限一律 None（fail-closed）。
fn parse_attachment_data_uri(uri: &str) -> Option<ParsedAttachment> {
    let rest = uri.strip_prefix("data:")?;
    let (meta, payload) = rest.split_once(',')?;
    let mut parts = meta.split(';');
    let declared = parts.next().unwrap_or_default();
    let content_type = if declared.is_empty() {
        "application/octet-stream"
    } else {
        declared
    };
    let mut filename = None;
    let mut is_base64 = false;
    let mut is_voice = false;
    for param in parts {
        if param.eq_ignore_ascii_case("base64") {
            is_base64 = true;
        } else if let Some(value) = param.strip_prefix("filename=") {
            filename = Some(percent_decode(value)?);
        } else if param.eq_ignore_ascii_case("voice") {
            is_voice = true;
        } else if let Some(value) = param.strip_prefix("voice=") {
            match value {
                "true" => is_voice = true,
                "false" => {}
                _ => return None,
            }
        }
    }
    if !is_base64 {
        return None;
    }
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .ok()?;
    if bytes.len() > ATTACHMENT_MAX_BYTES {
        return None;
    }
    Some(ParsedAttachment {
        spec: presage::libsignal_service::sender::AttachmentSpec {
            content_type: content_type.to_owned(),
            length: bytes.len(),
            file_name: filename,
            voice_note: if is_voice { Some(true) } else { None },
            ..presage::libsignal_service::sender::AttachmentSpec::default()
        },
        bytes,
    })
}

/// percent-decode（connector build_attachment_data_uri 的 encodeURIComponent
/// 语义逆运算）；坏序列/非 UTF-8 一律 None。
fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes.get(index + 1..index + 3)?;
            let byte = u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// packId hex（官方桌面 Bytes.fromHex 逆运算）；奇数长/非 hex 字符 None。
/// 官方仅认小写（`[0-9a-f]{32}`），大写显式拒绝。
pub(crate) fn hex_decode(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    (0..value.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&value[index..index + 2], 16).ok())
        .collect()
}

/// packId hex 编码（接收投影官方序列化同构；32 位小写）。
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// fake 服务开关（compat §6）：仅显式环境变量，生产缺省为真实服务。
pub(crate) fn mode_from_env() -> bool {
    std::env::var("KT_ENGINE_FAKE_SERVICE").ok().as_deref() == Some("1")
}

#[derive(Debug)]
pub(crate) enum LinkError {
    /// finishLink 校验失败的契约错误原文（-1，compat §3）。
    UnknownUri,
    /// 内部失败（-32603，细节不外泄）。
    Internal,
}

#[derive(Debug)]
pub(crate) enum SendError {
    /// 账号不在本引擎注册表中（连接器只对 listAccounts 结果发送，正常不到达）。
    UnknownAccount,
    /// 目标群已终止（官方 shouldSendToConversation terminated 位硬拒；错误
    /// 面 `-1` + `SEND_TARGET_TERMINATED`，compat §3）。
    TargetTerminated,
    /// 目标群为 announcements-only 且发送方无 ADMINISTRATOR 角色（官方客户端
    /// 侧闸，服务器不拦；错误面 `-1` + `SEND_TARGET_ANNOUNCEMENTS_ONLY`，
    /// compat §3 群发送语义批）。群态完全取不到时不走此臂（UnknownGroup
    /// 既有行为），fail-open 不新增失败面。
    AnnouncementsOnly,
    /// 服务端凭据拒绝（HTTP 401/403 → ServiceError::Unauthorized，vendored
    /// push_service/response.rs 归型）：账号在 Signal 侧已解绑。确定性失败，
    /// 标失败不重试（unknown-send 纪律同向）；错误面 -32603 + 解绑原文
    /// （getUserStatus 同款，compat §3）。
    Unbound,
    /// 服务端限流（HTTP 429/413 → ServiceError::RateLimitExceeded，vendored
    /// push_service/response.rs 归型，retry-after 不上 wire）。引擎不自动
    /// 重试；错误面 -32603 + 限流分类原文（compat §3）。
    RateLimited,
    /// 服务端要求人机验证（HTTP 428 → ServiceError::ProofRequiredError；
    /// send 链 sender 同因再包层 MessageSenderError::ProofRequired，
    /// token/options 不出归型面）。不自动重试；错误面 -32603（compat §3）。
    ProofRequired,
    /// Signal 服务端不可用（HTTP 5xx → ServiceError::UnhandledResponseCode
    /// 兜底臂 status≥500；vendored 无独立 ServerError 变体）。建议可重试，
    /// 重试决策在 connector/desktop；错误面 -32603（compat §3）。
    ServerUnavailable,
    /// 对端身份钥已变更且未经确认（P9 身份拒发闸，compat §3/§5）：协议层
    /// 旧钥会话/换钥 bundle 拒加密（vendored `is_trusted_identity`
    /// Direction::Sending + 不匹配 → UntrustedIdentity），或服务层 sidecar
    /// `pending` 闸命中（接收侧换钥后未获 trust 确认）。确定性失败不
    /// 重试；错误面 -32603 + 固定原文（锚词 UntrustedIdentityException）。
    IdentityUnverified,
    Internal,
}

/// presage 发送/上行面错误归型：服务端 401/403（凭据吊销/解绑）同归
/// `SendError::Unbound`——直接 ServiceError 面（whoami 等推送服务调用）
/// 与 MessageSenderError 面（send_message 内 sender 上行同源再包一层）
/// 两处归型点都覆盖；限流（429/413 → RateLimited）、人机验证（428 →
/// ProofRequired）、服务端 5xx 不可用（→ ServerUnavailable）同以
/// ServiceError 面单点归型（compat §3 结构化三分类）；其余（网络/协议/
/// 存储）折叠 `SendError::Internal`。
/// send 链各触网点单点共用（getUserStatus 的 whoami 面同归型先行）。
pub(crate) fn send_error_of(error: presage::Error<<SqliteStore as Store>::Error>) -> SendError {
    use presage::libsignal_service::prelude::ServiceError;
    use presage::libsignal_service::sender::MessageSenderError;

    /// ServiceError 面单点归型（直接推送服务调用与 sender 再包层两触点
    /// 共用；vendored push_service/response.rs 归型路径）。
    fn service_error_face(error: ServiceError) -> SendError {
        match error {
            ServiceError::Unauthorized => SendError::Unbound,
            ServiceError::RateLimitExceeded { .. } => SendError::RateLimited,
            ServiceError::ProofRequiredError(..) => SendError::ProofRequired,
            ServiceError::UnhandledResponseCode { status, .. } if status.as_u16() >= 500 => {
                SendError::ServerUnavailable
            }
            _ => SendError::Internal,
        }
    }

    match error {
        presage::Error::GroupAnnouncementsOnly => SendError::AnnouncementsOnly,
        presage::Error::MessageSenderError(sender_error) => match *sender_error {
            MessageSenderError::ServiceError(service_error) => service_error_face(service_error),
            MessageSenderError::ProofRequired { .. } => SendError::ProofRequired,
            // P9 身份拒发闸：sender 对 process_prekey_bundle 失败统一再包
            // UntrustedIdentity（vendor sender.rs），协议层拒发（旧钥会话/
            // 换钥 bundle）落到此臂；ProtocolError 面的裸
            // SignalProtocolError::UntrustedIdentity 同因同归型。
            MessageSenderError::UntrustedIdentity { .. } => SendError::IdentityUnverified,
            MessageSenderError::ProtocolError(
                presage::libsignal_service::protocol::SignalProtocolError::UntrustedIdentity(_),
            ) => SendError::IdentityUnverified,
            _ => SendError::Internal,
        },
        presage::Error::ServiceError(service_error) => service_error_face(service_error),
        presage::Error::ProtocolError(
            presage::libsignal_service::protocol::SignalProtocolError::UntrustedIdentity(_),
        ) => SendError::IdentityUnverified,
        _ => SendError::Internal,
    }
}

/// P9 身份拒发闸（服务层）：对端 sidecar 条目挂起（接收侧换钥未获确认）
/// → 拒发。直聊内容发送单点判定（闸面与确认钩子见 identity 模块注与
/// compat §5）；首联系人（无条目）恒放行。sidecar 读失败 = 未挂起
/// （fail-open，主干保护）。
fn identity_send_gate(account_dir: &Path, service_id: &ServiceId) -> Result<(), SendError> {
    if identity::send_gate_blocked(account_dir, &service_id.service_id_string()) {
        Err(SendError::IdentityUnverified)
    } else {
        Ok(())
    }
}

/// P9 profileSharing 闸读面（S3）：Storage Service 联系人记录 whitelist 位
/// map。任一读失败 → 空 map = 照常注入（fail-open，官方读本地库无失败面，
/// 偏差登记 compat §5）。
async fn profile_sharing_whitelist(service: &Arc<Service>, account: &str) -> HashMap<String, bool> {
    match storage_context(service, account).await {
        Ok(context) => storage_sync::contact_whitelist_map(&context).await,
        Err(_) => HashMap::new(),
    }
}

/// whitelist 位 → 是否注入本机 profile_key：仅记录显式 `whitelisted == false`
/// 才不注入（缺记录/位 true → 注入，fail-open 同语义）。
fn include_profile_key_for(whitelist: &HashMap<String, bool>, service_id: &ServiceId) -> bool {
    !matches!(
        whitelist
            .get(&service_id.service_id_string().to_lowercase())
            .copied(),
        Some(false)
    )
}

/// sendMessageRequestResponse 的线程腿（connector §4.56 同款两腿恰一）：
/// Thread = 直聊对端（recipient，号码/UUID 双形态，service 层解析为 ACI）；
/// Group = 群 master key（groupId，32B 标准 base64，decode_group_id 闸后
/// 传入）。
#[derive(Debug, Clone)]
pub(crate) enum MessageRequestTarget {
    Thread(String),
    Group(Vec<u8>),
}

/// sendMessageRequestResponse type 参数词汇 → proto 枚举（方法参数单点
/// 映射）：词表镜像官方 `Proto.SyncMessage.MessageRequestResponse.Type` 六值
/// 驼峰词（桌面 conversations.preload.ts syncMessageRequestResponse 直传枚举
/// 序数；UNKNOWN=0 不在词表——官方发送侧无「发未知」语义，缺席/越界同拒）。
pub(crate) fn message_request_type_of(
    text: &str,
) -> Option<presage::proto::sync_message::message_request_response::Type> {
    use presage::proto::sync_message::message_request_response::Type;
    match text {
        "accept" => Some(Type::Accept),
        "delete" => Some(Type::Delete),
        "block" => Some(Type::Block),
        "blockAndDelete" => Some(Type::BlockAndDelete),
        "spam" => Some(Type::Spam),
        "blockAndSpam" => Some(Type::BlockAndSpam),
        _ => None,
    }
}

/// 消息请求同步构造（桌面 getMessageRequestResponseSync 行为同构）：
/// SyncMessage{content: MessageRequestResponse{type, threadAciBinary|
/// threadAci?, groupId?}}，padded sync message（官方 padSyncMessage 同位，
/// presage send 面注入同源 padding）。thread 腿 ACI 可解析 16B 时直写
/// threadAciBinary（官方 isProtoBinaryEncodingEnabled 现行面），不可解析的
/// 已闸字符串降写 threadAci string 形态；group 腿 = 32B master key 原样。
fn build_message_request_response_sync(
    target: &MessageRequestTarget,
    r#type: presage::proto::sync_message::message_request_response::Type,
) -> presage::proto::SyncMessage {
    let message_request_response = presage::proto::sync_message::MessageRequestResponse {
        r#type: Some(r#type as i32),
        thread_aci: match target {
            MessageRequestTarget::Thread(aci) if aci_to_bytes(aci).is_none() => Some(aci.clone()),
            _ => None,
        },
        thread_aci_binary: match target {
            MessageRequestTarget::Thread(aci) => aci_to_bytes(aci),
            MessageRequestTarget::Group(_) => None,
        },
        group_id: match target {
            MessageRequestTarget::Group(master_key) => Some(master_key.clone()),
            MessageRequestTarget::Thread(_) => None,
        },
    };
    presage::proto::SyncMessage {
        content: Some(
            presage::proto::sync_message::Content::MessageRequestResponse(message_request_response),
        ),
        ..presage::proto::SyncMessage::with_padding(&mut rng())
    }
}

#[derive(Debug)]
pub(crate) enum UserStatusError {
    /// 账号在服务器侧已解绑（401）：connector 1.21 靠错误原文识别解绑态。
    Unbound,
    Internal,
}

/// engine.deviceCapabilities（D5 设备面注册）错误归型：401 解绑判据与
/// UserStatusError 同源，其余折叠 Internal（参数面无独立错误）。
#[derive(Debug)]
pub(crate) enum DeviceCapabilitiesError {
    Unbound,
    Internal,
}

/// 链接结果共享槽：finishLink 可被连接器超时重试（connector service.rs 语义），
/// 多个等待者共享同一次结果。
struct LinkResultSlot {
    outcome: StdMutex<Option<Result<String, ()>>>,
    resolved: Notify,
}

impl LinkResultSlot {
    fn new() -> Self {
        Self {
            outcome: StdMutex::new(None),
            resolved: Notify::new(),
        }
    }

    fn complete(&self, outcome: Result<String, ()>) {
        *self.outcome.lock().expect("outcome lock") = Some(outcome);
        self.resolved.notify_waiters();
    }

    async fn wait(&self) -> Result<String, ()> {
        loop {
            if let Some(outcome) = self.outcome.lock().expect("outcome lock").clone() {
                return outcome;
            }
            self.resolved.notified().await;
        }
    }
}

struct PendingLink {
    uri: String,
    store_dir: PathBuf,
    result: Arc<LinkResultSlot>,
}

/// transfer_archive 下载进度快照注册表（契约 1.53 §4.38 扩展）。
mod transfer_progress;

/// 云备份（Signal Backups）restore 凭据面（docs/cloud-backup-contract.md；
/// 纯函数核先行，REST/gRPC/CDN 传输面随批）。
pub mod cloud_backup;

/// 云备份 upload 面（docs/cloud-backup-contract.md §6，二期：uploadToCloud/
/// uploadStatus；装配段专用线程 + 上行段 LocalSet watcher）。
pub(crate) mod cloud_upload;

pub(crate) struct Service {
    data_dir: PathBuf,
    real: bool,
    state: Arc<AsyncMutex<EngineState>>,
    /// receive 通知出口（与 JSON-RPC 响应同一 stdout writer 队列；帧协议
    /// 见 main::OutboundFrame——Notice 直通，Flush 背压栅栏）。
    outbound: mpsc::UnboundedSender<crate::OutboundFrame>,
    inner: AsyncMutex<Inner>,
    /// 运行中的接收管线（账号 → 停机旗），delete 时通知退出。
    pipelines: AsyncMutex<HashMap<String, watch::Sender<bool>>>,
    /// 备份导出任务注册表（进程内存，backup_task 模块契约 §10.3）。
    backup_tasks: Arc<backup_task::BackupTaskRegistry>,
    /// 官方快照导入任务注册表（进程内存，snapshot_import 模块 §11）。
    snapshot_imports: Arc<snapshot_import::SnapshotImportRegistry>,
    /// 已终止群登记表（进程内存，groups 模块：receive 投影 → send 面拦截）。
    terminated_groups: groups::TerminatedGroups,
    /// transfer_archive 下载进度快照（进程内存，不落盘；§4.38 progress 面）。
    transfer_progress: transfer_progress::TransferProgress,
    /// 云备份 restore 进度/终态快照（进程内存单槽，不落盘；cloud-backup-contract
    /// §2 cloudStatus 面）。
    cloud_restore_progress: cloud_backup::CloudRestoreProgress,
    /// 云备份 upload 进度/终态快照（进程内存单槽，不落盘；cloud-backup-contract
    /// §6.2 uploadStatus 面）。
    cloud_upload_progress: cloud_upload::CloudUploadProgress,
    /// 群凭据双族缓存（进程内存 per-account ACI 键控，不落盘不跨重启；
    /// P4c 切一 call_links 模块契约，官方 groupCredentialFetcher 对位件）。
    group_credentials: Arc<call_links::GroupCredentialCache>,
    /// 账号级常驻发送管理器（C 批提速）：号码 → 共享互斥管理器。发送取用
    /// 命中即复用（presage Manager 内部 identified/unidentified websocket
    /// 同生命周期常驻），死亡自愈重建在 vendor 侧（closed → 重开）；惰性
    /// 建连 + 空闲保留，不逐消息冷启动。同账号取用互斥串行（Manager 方法
    /// 需 &mut 且跨 await 持有），跨账号互不阻塞；delete/relink 摘除。
    send_managers: AsyncMutex<SendManagerCache>,
    /// 群发 MRM 单请求面（C 批提速）：进程级未鉴权 chat WS 连接单槽缓存
    /// （PUT /v1/messages/multi_recipient 不带账号凭据，跨账号共用）；
    /// 传输失败摘除重连一次（group_mrm::send_mrm），惰性建连。
    mrm_chat: AsyncMutex<Option<libsignal_net::chat::ChatConnection>>,
}

/// 常驻发送管理器缓存：号码 → 共享互斥管理器（Service 同型，单测可断言）。
type SendManagerCache = HashMap<String, SharedSendManager>;

/// 常驻发送管理器共享句柄（取用方持句柄 + `lock_registered_for_send` 锁定）。
type SharedSendManager = Arc<AsyncMutex<Manager<SqliteStore, Registered>>>;

struct Inner {
    pending: Option<PendingLink>,
    fake_pending_uri: Option<String>,
}

impl Service {
    pub(crate) fn new(
        data_dir: PathBuf,
        state: Arc<AsyncMutex<EngineState>>,
        outbound: mpsc::UnboundedSender<crate::OutboundFrame>,
    ) -> Arc<Self> {
        let real = !mode_from_env();
        Arc::new(Self {
            data_dir,
            real,
            state,
            outbound,
            inner: AsyncMutex::new(Inner {
                pending: None,
                fake_pending_uri: None,
            }),
            pipelines: AsyncMutex::new(HashMap::new()),
            backup_tasks: Arc::new(StdMutex::new(HashMap::new())),
            snapshot_imports: Arc::new(StdMutex::new(HashMap::new())),
            terminated_groups: groups::TerminatedGroups::new(),
            transfer_progress: transfer_progress::TransferProgress::new(),
            cloud_restore_progress: cloud_backup::CloudRestoreProgress::new(),
            cloud_upload_progress: cloud_upload::CloudUploadProgress::new(),
            group_credentials: Arc::new(call_links::GroupCredentialCache::new()),
            send_managers: AsyncMutex::new(HashMap::new()),
            mrm_chat: AsyncMutex::new(None),
        })
    }

    /// stdout 通知入队（fire-and-forget；行序 = 队列序，writer 死亡静默）。
    pub(crate) fn notify(&self, value: Value) {
        let _ = self.outbound.send(crate::OutboundFrame::Notice(value));
    }

    /// flush 背压栅栏（时序审计 G1）：resolve = 此前全部已入队通知已写进
    /// stdout 并 flush（连接器至少收到字节）；`false` = 出口已死（writer
    /// 退出/通道关闭）——调用方据此放弃 ack，让服务器队列保留信封待重启
    /// 重放，连接器侧永不缺投影。
    pub(crate) async fn flush_barrier(&self) -> bool {
        crate::outbound_flush_barrier(&self.outbound).await
    }

    /// 启动时为每个真实账号拉起接收管线（`--receive-mode on-start` 语义）。
    pub(crate) async fn spawn_receive_pipelines(self: &Arc<Self>) {
        if !self.real {
            return;
        }
        let referenced: HashSet<String> = {
            let state = self.state.lock().await;
            state
                .accounts()
                .iter()
                .filter_map(|entry| entry.dir.clone())
                .collect()
        };
        sweep_orphan_store_dirs(&self.data_dir, &referenced);
        let accounts: Vec<(String, PathBuf)> = {
            let state = self.state.lock().await;
            state
                .accounts()
                .iter()
                .filter_map(|entry| {
                    entry
                        .dir
                        .as_ref()
                        .map(|rel| (entry.number.clone(), self.data_dir.join(rel)))
                })
                .collect()
        };
        for (number, dir) in accounts {
            self.spawn_pipeline(number, dir).await;
        }
    }

    async fn spawn_pipeline(self: &Arc<Self>, number: String, dir: PathBuf) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = Arc::clone(self);
        let task_number = number.clone();
        // presage 的接收流持有非 Send 状态（ThreadRng/dyn ProtocolStore），
        // 进不了 tokio::spawn；统一走 LocalSet（main 装配提供上下文）。
        tokio::task::spawn_local(async move {
            receive_pipeline(&service, task_number, dir, shutdown_rx).await;
        });
        self.pipelines.lock().await.insert(number, shutdown_tx);
    }

    pub(crate) async fn start_link(self: &Arc<Self>) -> Result<String, LinkError> {
        if !self.real {
            let mut inner = self.inner.lock().await;
            let uri = fake_link_uri();
            inner.fake_pending_uri = Some(uri.clone());
            return Ok(uri);
        }

        let mut inner = self.inner.lock().await;
        // 新会话作废旧会话：丢掉 url_rx（presage 内部 send 失败 → 流程按
        // Err 收口，任务自行清理目录）。
        inner.pending = None;

        let store_dir = self.data_dir.join("accounts").join(random_dir_name());
        let store = open_store(&store_dir)
            .await
            .map_err(|_| LinkError::Internal)?;

        let (url_tx, url_rx) = futures::channel::oneshot::channel::<url::Url>();
        let slot = Arc::new(LinkResultSlot::new());

        // 链接流程 future 同样非 Send：spawn_local 承载（见 spawn_pipeline）。
        // start_link 只会从 LocalSet 内的 dispatch 被调用，上下文必然在。
        let task_dir = store_dir.clone();
        let task_slot = Arc::clone(&slot);
        let task_progress = self.transfer_progress.clone();
        tokio::task::spawn_local(async move {
            match link_flow(store, url_tx, task_dir, task_progress).await {
                Ok(number) => task_slot.complete(Ok(number)),
                Err(()) => task_slot.complete(Err(())),
            }
        });

        // URL 产出有界等待：provisioning WS 正常亚秒级出 URL。超时/失败时
        // url_rx 随返回值丢弃，链接任务自行收口。
        let uri = match tokio::time::timeout(Duration::from_secs(30), url_rx).await {
            Ok(Ok(uri)) => uri.to_string(),
            _ => return Err(LinkError::Internal),
        };

        inner.pending = Some(PendingLink {
            uri,
            store_dir,
            result: slot,
        });
        Ok(inner.pending.as_ref().expect("just set").uri.clone())
    }

    pub(crate) async fn finish_link(
        self: &Arc<Self>,
        uri: &str,
        device_name: &str,
    ) -> Result<String, LinkError> {
        if !self.real {
            let mut inner = self.inner.lock().await;
            let matches = inner
                .fake_pending_uri
                .as_deref()
                .is_some_and(|pending| pending == uri);
            if !matches {
                return Err(LinkError::UnknownUri);
            }
            inner.fake_pending_uri = None;
            let mut state = self.state.lock().await;
            let number = format!("+1555000{:04}", state.accounts().len() + 1);
            state
                .add_account(number.clone(), None)
                .map_err(|_| LinkError::Internal)?;
            return Ok(number);
        }

        // 锁内只做会话查找；等待手机确认必须放锁（finishLink 可能阻塞数分钟，
        // 其他方法必须继续可用）。
        let slot = {
            let inner = self.inner.lock().await;
            let pending = inner.pending.as_ref().ok_or(LinkError::UnknownUri)?;
            if pending.uri != uri {
                return Err(LinkError::UnknownUri);
            }
            Arc::clone(&pending.result)
        };
        let number = slot.wait().await.map_err(|_| LinkError::Internal)?;

        // 成功：会话收口。设备名（ADR 0004）：改写 store 注册数据后启动接收
        // 管线；名字只落本地注册数据，管线不回写服务器（B4 起停发
        // AccountAttributes PUT——官方链接设备从不 PUT attributes，compat §5）。
        let mut inner = self.inner.lock().await;
        if inner
            .pending
            .as_ref()
            .is_none_or(|pending| pending.uri != uri)
        {
            // 并发重试已完成收口：幂等返回同一号码。
            return Ok(number);
        }
        let pending = inner.pending.take().expect("checked");
        if !device_name.is_empty() {
            let mut store = open_store(&pending.store_dir)
                .await
                .map_err(|_| LinkError::Internal)?;
            if let Some(mut data) = store
                .load_registration_data()
                .await
                .map_err(|_| LinkError::Internal)?
            {
                data.device_name = Some(device_name.to_owned());
                store
                    .save_registration_data(&data)
                    .await
                    .map_err(|_| LinkError::Internal)?;
            }
        }
        let rel = pending
            .store_dir
            .strip_prefix(&self.data_dir)
            .unwrap_or(&pending.store_dir)
            .to_string_lossy()
            .to_string();
        self.state
            .lock()
            .await
            .add_account(number.clone(), Some(rel))
            .map_err(|_| LinkError::Internal)?;
        // 同号重链换新 store 目录：摘除旧常驻管理器（下次发送按新目录重建）。
        self.send_managers.lock().await.remove(&number);
        self.spawn_pipeline(number.clone(), pending.store_dir).await;
        Ok(number)
    }

    /// transfer_archive 下载进度查询（契约 1.53 §4.38 扩展，connector 的
    /// `history.importStatus` progress 组合面消费）：进程内快照只读，无
    /// 网络、无 store。快照缺席（未开始/已结束/引擎重启过）= None，消费方
    /// 维持无 progress 的诚实投影。
    pub(crate) fn transfer_archive_snapshot(
        &self,
        account: &str,
    ) -> Option<transfer_progress::TransferSnapshot> {
        self.transfer_progress.snapshot(account)
    }

    pub(crate) async fn delete_account(&self, account: &str) -> std::io::Result<bool> {
        // 注册表摘除并取回目录（fake 账号 dir=None）。
        let removed_dir: Option<PathBuf> = {
            let mut state = self.state.lock().await;
            state
                .remove_account(account)?
                .and_then(|entry| entry.dir.map(|rel| self.data_dir.join(rel)))
        };
        let had_dir = removed_dir.is_some();
        if let Some(dir) = removed_dir {
            let _ = std::fs::remove_dir_all(&dir);
        }
        // 停掉接收管线（fake/无管线时无操作）。锁序 inner < state < pipelines，
        // 此处 state → pipelines 与 spawn 路径同向，无环。
        if let Some(shutdown) = self.pipelines.lock().await.remove(account) {
            let _ = shutdown.send(true);
        }
        // 摘除常驻发送管理器（若有在途发送，guard 释放后管理器随 Arc 落弃）。
        self.send_managers.lock().await.remove(account);
        Ok(had_dir || !self.real)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_text(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        message: &str,
        quote: Option<&SendQuote>,
        preview: Option<&SendPreview>,
        mentions: &[SendMention],
        text_styles: &[SendTextStyle],
        attachments: &[String],
        sticker: Option<&SendSticker>,
        story_context: Option<&SendStoryContext>,
        view_once: bool,
    ) -> Result<(u64, Vec<Value>, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new(), Vec::new()));
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        let whitelist = profile_sharing_whitelist(self, account).await;
        // 附件/预览图一次上行（多收件人共享同一批指针）；贴纸图单条上行，
        // 同样多收件人共享指针。
        let (pointers, preview_image) = upload_attachments_for_send(
            &manager,
            attachments,
            preview.and_then(|item| item.image.as_deref()),
        )
        .await?;
        let sticker_pointer = match sticker {
            Some(sticker) => Some(upload_sticker_image(&manager, sticker).await?),
            None => None,
        };
        // 时序审计组二#9：per-recipient 成败收集（不再 fail-fast 丢弃部分
        // 成功——重试全批会对已达收件人重复投递）。results = 成功条目（旧
        // 形状原样）；failures = 失败条目（wire additive，connector 只补
        // 未达方）。全败 = 整批错误面维持旧状（首错分类上抛，此时零已达
        // 收件人，整批重试无重复投递风险）。
        let mut results = Vec::new();
        let mut failures = Vec::new();
        let mut first_error: Option<SendError> = None;
        for number in recipients {
            let send_one = async {
                let service_id = resolve_recipient(&manager, number)
                    .await
                    .ok_or(SendError::Internal)?;
                // P9 身份拒发闸：对端换钥未确认 → 拒发（直聊内容发送面统一
                // 判定；整批失败与既有逐收件人失败同语义，compat §3）。
                identity_send_gate(&account_dir, &service_id)?;
                // P9 profileSharing 闸（S3）：会话未同意 profileSharing → 不注入
                // 本机 profile_key（官方 sendNormalMessage.preload.ts:183-185）。
                let include_profile_key = include_profile_key_for(&whitelist, &service_id);
                let body = match sticker {
                    Some(sticker) => {
                        build_sticker_data_message(
                            manager.store(),
                            sticker,
                            quote,
                            sticker_pointer.clone().ok_or(SendError::Internal)?,
                            timestamp,
                        )
                        .await?
                    }
                    None if view_once => build_view_once_data_message(pointers.clone(), timestamp)?,
                    None => {
                        build_text_data_message(
                            manager.store(),
                            message,
                            quote,
                            preview,
                            mentions,
                            text_styles,
                            pointers.clone(),
                            preview_image.clone(),
                            story_context,
                            timestamp,
                        )
                        .await?
                    }
                };
                manager
                    .send_message_with_profile_sharing(
                        service_id,
                        body,
                        timestamp,
                        include_profile_key,
                    )
                    .await
                    .map_err(send_error_of)?;
                Ok(())
            };
            match send_one.await {
                Ok(()) => results.push(json!({
                    "recipientNumber": number,
                    "success": { "timestamp": timestamp },
                })),
                Err(error) => {
                    let (code, message) = crate::methods::send_error_face(&error);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    failures.push(json!({
                        "recipientNumber": number,
                        "error": { "code": code, "message": message },
                    }));
                }
            }
        }
        if results.is_empty() && !failures.is_empty() {
            return Err(first_error.unwrap_or(SendError::Internal));
        }
        Ok((timestamp, results, failures))
    }

    pub(crate) async fn send_remote_delete(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        let whitelist = profile_sharing_whitelist(self, account).await;
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            // P9 身份拒发闸（撤回同为内容面）。
            identity_send_gate(&account_dir, &service_id)?;
            // P9 profileSharing 闸（S3）：未同意 → 不注入本机 profile_key。
            let include_profile_key = include_profile_key_for(&whitelist, &service_id);
            let body = DataMessage {
                timestamp: Some(timestamp),
                delete: Some(presage::proto::data_message::Delete {
                    target_sent_timestamp: Some(target_timestamp),
                }),
                ..DataMessage::default()
            };
            manager
                .send_message_with_profile_sharing(service_id, body, timestamp, include_profile_key)
                .await
                .map_err(send_error_of)?;
        }
        Ok(())
    }

    /// sendStory（Stories 发送批，KT 扩展方法）：text story 构造 + 逐收件人
    /// 分发 + best-effort 自同步，编排全量在 stories 模块（compat §3
    /// sendStory 行）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_story(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        distribution_list_id: &str,
        allows_replies: bool,
        text: &str,
        text_style: Option<i32>,
        body_ranges: &[SendTextStyle],
        preview_image: Option<&str>,
        preview_url: Option<String>,
        preview_title: Option<String>,
        preview_date: Option<u64>,
    ) -> Result<(u64, Vec<Value>), SendError> {
        stories::send_story(
            self,
            account,
            recipients,
            distribution_list_id,
            allows_replies,
            text,
            text_style,
            body_ranges,
            preview_image,
            preview_url,
            preview_title,
            preview_date,
        )
        .await
    }

    pub(crate) async fn send_reaction(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_author: &str,
        target_timestamp: u64,
        emoji: &str,
        remove: bool,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        let whitelist = profile_sharing_whitelist(self, account).await;
        // 协议字段是 targetAuthorAci（UUID）：本机号走注册数据（回应自己发的
        // 消息），其余经 contacts 映射解析（ADR 0004）；都未命中按内部失败，
        // 不透传号码。
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .ok_or(SendError::Internal)?;
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            // P9 身份拒发闸（回应面同为直聊内容发送）。
            identity_send_gate(&account_dir, &service_id)?;
            // P9 profileSharing 闸（S3）：未同意 → 不注入本机 profile_key。
            let include_profile_key = include_profile_key_for(&whitelist, &service_id);
            let body = DataMessage {
                timestamp: Some(timestamp),
                reaction: Some(presage::proto::data_message::Reaction {
                    emoji: Some(emoji.to_owned()),
                    target_author_aci: Some(target_aci.clone()),
                    target_sent_timestamp: Some(target_timestamp),
                    remove: Some(remove),
                    ..presage::proto::data_message::Reaction::default()
                }),
                ..DataMessage::default()
            };
            manager
                .send_message_with_profile_sharing(service_id, body, timestamp, include_profile_key)
                .await
                .map_err(send_error_of)?;
        }
        Ok(())
    }

    /// sendPinMessage：官方 DataMessage.pinMessage=27（SignalService.proto §27，
    /// 官方桌面 SendMessage.preload.ts 行为对齐）。`pin_duration_seconds=None`
    /// = pinDurationForever；`Some(s)` = pinDurationSeconds（u32）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_pin_message(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_author: &str,
        target_timestamp: u64,
        pin_duration_seconds: Option<u32>,
    ) -> Result<(), SendError> {
        self.send_targeted_data_message(
            account,
            recipients,
            target_author,
            |aci_bytes, timestamp| DataMessage {
                timestamp: Some(timestamp),
                pin_message: Some(pin_message_proto(
                    aci_bytes,
                    target_timestamp,
                    pin_duration_seconds,
                )),
                ..DataMessage::default()
            },
        )
        .await
    }

    /// sendUnpinMessage：官方 DataMessage.unpinMessage=28。
    pub(crate) async fn send_unpin_message(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_author: &str,
        target_timestamp: u64,
    ) -> Result<(), SendError> {
        self.send_targeted_data_message(
            account,
            recipients,
            target_author,
            |aci_bytes, timestamp| DataMessage {
                timestamp: Some(timestamp),
                unpin_message: Some(presage::proto::data_message::UnpinMessage {
                    target_author_aci_binary: Some(aci_bytes),
                    target_sent_timestamp: Some(target_timestamp),
                }),
                ..DataMessage::default()
            },
        )
        .await
    }

    /// sendAdminDelete（群组）：官方 DataMessage.adminDelete=29，语义 = 群管理
    /// 员删除他人消息；直聊无 admin 概念，仅 groupId 形态。
    pub(crate) async fn send_admin_delete_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_author: &str,
        target_timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        // 官方语义 targetAuthor 一律 ACI；群内兼容号码/UUID 解析，未命中
        // 原样透传（协议侧报错，不在此编造）。
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .unwrap_or_else(|| target_author.trim().to_owned());
        let aci_bytes = aci_to_bytes(&target_aci).ok_or(SendError::Internal)?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            admin_delete: Some(presage::proto::data_message::AdminDelete {
                target_author_aci_binary: Some(aci_bytes),
                target_sent_timestamp: Some(target_timestamp),
            }),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    /// sendPinMessage（群组）：目标消息置顶跨群发（官方群内置顶同字段）。
    pub(crate) async fn send_pin_message_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_author: &str,
        target_timestamp: u64,
        pin_duration_seconds: Option<u32>,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .unwrap_or_else(|| target_author.trim().to_owned());
        let aci_bytes = aci_to_bytes(&target_aci).ok_or(SendError::Internal)?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            pin_message: Some(pin_message_proto(
                aci_bytes,
                target_timestamp,
                pin_duration_seconds,
            )),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    /// sendUnpinMessage（群组）。
    pub(crate) async fn send_unpin_message_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_author: &str,
        target_timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .unwrap_or_else(|| target_author.trim().to_owned());
        let aci_bytes = aci_to_bytes(&target_aci).ok_or(SendError::Internal)?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            unpin_message: Some(presage::proto::data_message::UnpinMessage {
                target_author_aci_binary: Some(aci_bytes),
                target_sent_timestamp: Some(target_timestamp),
            }),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    /// 直聊 targeted dataMessage 公共头（pin/unpin 共用）：解析作者 ACI、
    /// 逐 recipient 发送。`body` 闭包收 (16B ACI bytes, timestamp)。
    async fn send_targeted_data_message<F>(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_author: &str,
        body: F,
    ) -> Result<(), SendError>
    where
        F: Fn(Vec<u8>, u64) -> DataMessage,
    {
        if !self.real {
            return Ok(());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        let whitelist = profile_sharing_whitelist(self, account).await;
        // 直聊回应用自己发的消息时 targetAuthor 是本机号（不在 contacts），
        // resolve_author_aci 已含注册数据回退。
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .ok_or(SendError::Internal)?;
        let aci_bytes = aci_to_bytes(&target_aci).ok_or(SendError::Internal)?;
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            // P9 身份拒发闸（pin/unpin 同为直聊内容发送）。
            identity_send_gate(&account_dir, &service_id)?;
            // P9 profileSharing 闸（S3）：未同意 → 不注入本机 profile_key。
            let include_profile_key = include_profile_key_for(&whitelist, &service_id);
            manager
                .send_message_with_profile_sharing(
                    service_id,
                    body(aci_bytes.clone(), timestamp),
                    timestamp,
                    include_profile_key,
                )
                .await
                .map_err(send_error_of)?;
        }
        Ok(())
    }

    pub(crate) async fn send_typing(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        stop: bool,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            let body = ContentBody::TypingMessage(presage::proto::TypingMessage {
                timestamp: Some(timestamp),
                action: Some(if stop {
                    presage::proto::typing_message::Action::Stopped as i32
                } else {
                    presage::proto::typing_message::Action::Started as i32
                }),
                group_id: None,
            });
            manager
                .send_message(service_id, body, timestamp)
                .await
                .map_err(send_error_of)?;
        }
        Ok(())
    }

    /// sendDeliveryReceipt/sendReadReceipt/sendViewedReceipt 公共面：官方
    /// sendReceipts.preload.ts + SendMessage.preload.ts #sendReceiptMessage
    /// 行为对齐——ReceiptMessage{type, timestamp[]} 一律逐作者直发
    /// （sendIndividualProto 同语义；原会话是群组也直发原作者，官方从不
    /// 整组广播回执）；timestamps 按官方 CHUNK_SIZE=100 分帧，每帧独立
    /// envelope 时间戳；空数组 fail-closed（协议面无意义的空回执）。
    pub(crate) async fn send_receipt(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        timestamps: &[u64],
        receipt_type: presage::proto::receipt_message::Type,
    ) -> Result<(), SendError> {
        if timestamps.is_empty() {
            return Err(SendError::Internal);
        }
        if !self.real {
            return Ok(());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let service_id = resolve_recipient(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        // P9 身份拒发闸（官方 conversationJobQueue 对 untrusted 目标取消
        // Receipts 任务同位——conversationJobQueue.preload.ts:1141-1146；
        // 自动 delivery receipt flush 复用本面，失败静默弃批）。
        identity_send_gate(&account_dir, &service_id)?;
        for chunk in timestamps.chunks(RECEIPT_CHUNK_SIZE) {
            let timestamp = now_ms();
            let body = ContentBody::ReceiptMessage(presage::proto::ReceiptMessage {
                r#type: Some(receipt_type as i32),
                timestamp: chunk.to_vec(),
            });
            manager
                .send_message(service_id, body, timestamp)
                .await
                .map_err(send_error_of)?;
        }
        Ok(())
    }

    /// sendViewOnceOpen（E4，官方 syncViewOnceOpen 同构）：向自己 ACI 发
    /// SyncMessage.ViewOnceOpen{senderAci, timestamp}（content oneof
    /// viewOnceOpen=11，padded sync message），多设备同步「该阅后即焚已被
    /// 本机打开」；与 sendViewedReceipt（作者面 VIEWED 回执）双通道独立。
    /// presage self-send 通路已验证可行：`Manager::send_message` 对自 ACI
    /// 发 SyncMessage 即官方 sendIndividualProto(ownAci) 同型线面（presage
    /// 上游 request_contacts 同路径），无需降级；钉定 rev 的同内容二次转录
    /// 重发怪癖登记 compat §5。senderAci 号码/UUID 双形态（reaction
    /// targetAuthor 同解析口径），不可解析 fail-closed。
    pub(crate) async fn send_view_once_open(
        self: &Arc<Self>,
        account: &str,
        sender: &str,
        timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let sender_aci = resolve_author_aci(manager.store(), sender)
            .await
            .ok_or(SendError::Internal)?;
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        let body = ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
            content: Some(presage::proto::sync_message::Content::ViewOnceOpen(
                presage::proto::sync_message::ViewOnceOpen {
                    sender_aci: Some(sender_aci.clone()),
                    timestamp: Some(timestamp),
                    sender_aci_binary: aci_to_bytes(&sender_aci),
                },
            )),
            ..presage::proto::SyncMessage::with_padding(&mut rng())
        });
        let envelope_ts = now_ms();
        manager
            .send_message(own_aci, body, envelope_ts)
            .await
            .map_err(send_error_of)?;
        Ok(())
    }

    /// markRead/markViewed（P14 组 6，官方 syncReadMessages/syncView 同构，
    /// SendMessage.preload.ts:2097/:2145）：向自己 ACI 发 SyncMessage.read=5/
    /// viewed=16（直接 repeated 字段，非 content oneof），多设备同步「这些
    /// 消息已读/已看」。条目 (sender, timestamp)：sender 号码/UUID 双解析
    /// （resolve_author_aci，viewOnceOpen 同口径，不可解析 fail-closed 整拒
    /// ——发送面参数闸纪律）；ACI binary+string 双写（官方
    /// isProtoBinaryEncodingEnabled 现行面/旧字符串面双兼容，E4 双写先例
    /// 同构）；100 条/chunk 分信封（官方 syncHelpers.preload.ts:25
    /// CHUNK_SIZE=100 同构）；padded sync message。presage self-send 通路与
    /// sendViewOnceOpen 同款（compat §5 E4 已验证）；同内容二次转录重发
    /// 怪癖同面（接收端幂等）。与 sendReadReceipt/sendViewedReceipt（作者
    /// 面 ReceiptMessage 回执）双通道独立（官方 markConversationRead
    /// .preload.ts:252 注同位）。
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn mark_read_state(
        self: &Arc<Self>,
        account: &str,
        viewed: bool,
        entries: Vec<(String, u64)>,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        // 条目解析先行（fail-closed：任一 sender 不可解析整拒，不发半批）。
        let mut resolved = Vec::with_capacity(entries.len());
        for (sender, timestamp) in entries {
            let aci = resolve_author_aci(manager.store(), &sender)
                .await
                .ok_or(SendError::Internal)?;
            resolved.push((aci, timestamp));
        }
        let envelope_ts = now_ms();
        if viewed {
            for chunk in resolved.chunks(100) {
                let viewed_entries = chunk
                    .iter()
                    .map(|(aci, timestamp)| presage::proto::sync_message::Viewed {
                        sender_aci: Some(aci.clone()),
                        timestamp: Some(*timestamp),
                        sender_aci_binary: aci_to_bytes(aci),
                    })
                    .collect();
                let body = ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                    viewed: viewed_entries,
                    ..presage::proto::SyncMessage::with_padding(&mut rng())
                });
                manager
                    .send_message(own_aci, body, envelope_ts)
                    .await
                    .map_err(send_error_of)?;
            }
        } else {
            for chunk in resolved.chunks(100) {
                let read_entries = chunk
                    .iter()
                    .map(|(aci, timestamp)| presage::proto::sync_message::Read {
                        sender_aci: Some(aci.clone()),
                        timestamp: Some(*timestamp),
                        sender_aci_binary: aci_to_bytes(aci),
                    })
                    .collect();
                let body = ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                    read: read_entries,
                    ..presage::proto::SyncMessage::with_padding(&mut rng())
                });
                manager
                    .send_message(own_aci, body, envelope_ts)
                    .await
                    .map_err(send_error_of)?;
            }
        }
        Ok(())
    }

    /// sendMessageRequestResponse（消息请求写面，connector 契约 1.63 §4.56
    /// 债务批，KT 扩展方法）：官方桌面 getMessageRequestResponseSync 同构——
    /// 向自己 ACI 发 SyncMessage.messageRequestResponse{threadAciBinary/
    /// threadAci?, groupId?, type}（content oneof messageRequestResponse=14，
    /// 多设备同步「该消息请求已接受/拒绝/拉黑…」终态）。thread 腿 =
    /// resolve_recipient 双形态解析 → ACI（binary 16B 直写，官方
    /// isProtoBinaryEncodingEnabled 现行面同构）；group 腿 = decode_group_id
    /// 32B master key 原样（官方 groupId Uint8Array 同位）。presage
    /// self-send 通路与 sendViewOnceOpen 同款（compat §5 E4 已验证）；钉定
    /// rev 的同内容二次转录重发怪癖同面（接收端幂等）。写结局即返回值：
    /// 管理器打开/腿解析/上行任一步失败按 SendError 上抛，未知结局不重试
    /// 纪律不变（connector §4.56 SEND_OUTCOME_UNKNOWN 终态语义的引擎侧
    /// 对应位）。
    pub(crate) async fn send_message_request_response(
        self: &Arc<Self>,
        account: &str,
        target: MessageRequestTarget,
        r#type: presage::proto::sync_message::message_request_response::Type,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        let body =
            ContentBody::SynchronizeMessage(build_message_request_response_sync(&target, r#type));
        let envelope_ts = now_ms();
        manager
            .send_message(own_aci, body, envelope_ts)
            .await
            .map_err(send_error_of)?;
        Ok(())
    }

    /// sendCallMessage 1:1 臂（通话消息面，KT 扩展方法）：官方
    /// Content.callMessage=3 信封。构造面 = build_call_message 单点（opaque
    /// 为 Signal-RTC 层信令负载，引擎不解析只透传，桌面 callingMessage 面
    /// 同构：offer/answer/ice/hangup/busy 都封装在 opaque 内；legacy 面按
    /// 官方 proto 裸帧族构造——`CallMessage{busy{id}}` /
    /// `CallMessage{hangup{id, type, deviceId?}}`，无 opaque 字段（官方桌面
    /// callingMessageToProto 同款：busy={id}、hangup={id,type}；RingRTC
    /// 2.72.1 出向 hangup/busy 实证无 opaque 材料；signal-cli 0.14.7/0.14.8
    /// CallManager.sendBusyViaSignal/sendHangupViaSignal 同构裸帧族）。收件人
    /// 号码/UUID 双形态解析（send 系同口径）；群臂见 send_call_message_group。
    /// 发送通路注记（compat §5 通话消息块，2026-10-11 勘误）：早前版本记
    /// 「官方 call 消息 online=true」为偏差——官方源码复核（abe80d3）不成立：
    /// 官方仅 typing 显式 `online: true`（conversations.preload.ts:1530/:1551），
    /// call 发送链（sendCallingMessage → sendMessageProtoAndWait）不设 online，
    /// 线面 online_only=false 与引擎/presage 现状一致（presage send_message
    /// 硬编码 online_only=false，上游 TODO issue #252——事实保留，当前无行为
    /// 差；上游若开放 online 选项再跟进）。CallMessage 无 sent transcript
    /// （上游 create_multi_device_sent_transcript_content 对 CallMessage 返回
    /// None），本机其他设备不同步自发 call 记录。
    pub(crate) async fn send_call_message(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        payload: SendCallMessage,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let service_id = resolve_recipient(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        // P9 身份拒发闸（直聊通话信令臂；群臂走群发送通路不在此闸）。
        identity_send_gate(&account_dir, &service_id)?;
        let timestamp = now_ms();
        let body = build_call_message(&payload);
        manager
            .send_message(service_id, body, timestamp)
            .await
            .map_err(send_error_of)?;
        Ok(timestamp)
    }

    /// sendCallMessage 群臂（P4b 群信令出网批，connector 契约 1.82 wire 面
    /// 对位）：官方桌面 #handleSendCallMessageToGroup（calling.preload.ts:
    /// 3338-3407）同构——信令字节包成 `CallMessage{opaque{data, urgency}}`
    /// sender-key 加密群发（官方群发落网 sendCallingMessage.preload.ts:
    /// 99-118 isGroup 臂 sendContentMessageToGroup；官方群信令恒 opaque，
    /// legacy 裸帧族群向无先例、sender-key 群发无设备定向位，参数层
    /// fail-closed）。群头 = open_group_sender（群可解 + terminated 硬拒
    /// `SEND_TARGET_TERMINATED`，send 系群组形态同纪律）+ 群向
    /// `send_message_to_group`（sendTyping 群臂同原语；服务器群态不在库
    /// UnknownGroup → Internal 折叠 -32603）。fake 模式成功应答 {timestamp}
    /// 与 1:1 同口径（伪群 id 在参数层拒绝，不到此处）。
    pub(crate) async fn send_call_message_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        payload: SendCallMessage,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
        manager
            .send_message_to_group(&master_key, build_call_message(&payload), timestamp)
            .await
            .map_err(send_error_of)
            .map(|_| ())?;
        Ok(timestamp)
    }

    /// editMessage 直聊腿（2026-10-11 六问批，官方编辑发送面对位）：官方编辑
    /// = Content.editMessage{targetSentTimestamp, dataMessage}（SendMessage
    /// .preload.ts:1075-1092 getContentMessage 同构，内层 dataMessage 只携
    /// 新 body + timestamp）。24h 窗口/10 次上限是官方 UI 层闸
    /// （canEditMessage.dom.ts），引擎 wire 面不设（API 调用方自担，§5 登记）；
    /// 内层不带 attachments/preview/sticker 等扩展位（官方编辑内层同界）。
    /// 身份拒发闸 + profileSharing 白名单与 send_text 同纪律；返回
    /// （信封 timestamp, 逐收件人 results），形状同 send。
    pub(crate) async fn send_edit_message(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        target_timestamp: u64,
        message: &str,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        let whitelist = profile_sharing_whitelist(self, account).await;
        let mut results = Vec::new();
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            // P9 身份拒发闸（编辑同为内容面）。
            identity_send_gate(&account_dir, &service_id)?;
            let include_profile_key = include_profile_key_for(&whitelist, &service_id);
            let body = build_edit_message_body(target_timestamp, timestamp, message, None);
            manager
                .send_message_with_profile_sharing(service_id, body, timestamp, include_profile_key)
                .await
                .map_err(send_error_of)?;
            results.push(json!({
                "recipientNumber": number,
                "success": { "timestamp": timestamp },
            }));
        }
        Ok((timestamp, results))
    }

    /// editMessage 群腿（同批）：MRM 主路径只支持 DataMessage（group_mrm
    /// 硬编码 Content::DataMessage），走 vendored `send_message_to_group`
    /// 原语（自带刷群态/announcements 闸/per-recipient outcomes/多设备
    /// 转录——vendored create_multi_device_sent_transcript_content 已把
    /// EditMessage 转成 Sent.editMessage）。vendored 不为 EditMessage 注入
    /// 群上下文，内层 DataMessage 的 group_v2 由本面手注（接收方进群线程
    /// 唯一依据，group_mrm DataMessage 同判例）；revision 经
    /// fetch_group_with_endorsements best-effort，失败 None（proto 缺席合法，
    /// 接收端按 0 归线程）。
    pub(crate) async fn send_edit_message_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_timestamp: u64,
        message: &str,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
        let revision = match manager.fetch_group_with_endorsements(&master_key).await {
            Ok(Some((group, _))) => Some(group.revision),
            _ => None,
        };
        let body = build_edit_message_body(
            target_timestamp,
            timestamp,
            message,
            Some((master_key.clone(), revision)),
        );
        let outcomes = manager
            .send_message_to_group(&master_key, body, timestamp)
            .await
            .map_err(send_error_of)?;
        let numbers = contacts_number_map(manager.store()).await;
        Ok((
            timestamp,
            group_send_result_entries(outcomes, timestamp, &numbers),
        ))
    }

    /// sendGroupCallUpdate（2026-10-11 六问批，官方群通话开始/结束信令面对
    /// 位）：DataMessage.groupCallUpdate{eraId}（field 19）走 dispatch_group
    /// （MRM 主路径 DataMessage 同族，group_v2 上下文 + announcements 闸 +
    /// terminated 硬拒全部沿用群发送系口径）。官方发送端
    /// `sendToGroup {groupCallUpdate:{eraId}, groupV2, timestamp}`
    /// （jobs/helpers/sendGroupCallUpdate.preload.ts:23-100）同构；官方对
    /// groupCallUpdate 不设 announcements 闸（KT 群发送闸族统一收口，§5
    /// 登记偏差）。fake 成功 `{}`（remoteDelete 同风格）。
    pub(crate) async fn send_group_call_update(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        era_id: &str,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let body = DataMessage {
            timestamp: Some(timestamp),
            group_call_update: Some(presage::proto::data_message::GroupCallUpdate {
                era_id: Some(era_id.to_owned()),
            }),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    /// listContacts：presage store 已同步的联系人投影（手机端链接后自动推送）。
    /// profile 拆名面（givenName/familyName）presage Contact 未建模，P2+ 补。
    pub(crate) async fn list_contacts(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<Value>, SendError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let manager = lock_registered_for_send(&shared).await;
        let contacts = manager
            .store()
            .contacts()
            .await
            .map_err(|_| SendError::Internal)?;
        let mut out = Vec::new();
        for contact in contacts.filter_map(Result::ok) {
            let number = contact
                .phone_number
                .as_ref()
                .map(e164_string)
                .unwrap_or_else(|| contact.uuid.to_string());
            let mut entry = json!({ "number": number });
            if !contact.name.is_empty() {
                entry["name"] = json!(contact.name);
            }
            out.push(entry);
        }
        Ok(out)
    }

    /// listGroups：store 内已同步群组。群组 id = master key 标准 base64
    ///（signal-cli 同源，connector send 回传同 id）。投影族细节见
    /// groups::project_group_list_entry（群写面 P5 批：description/
    /// announcementsOnly/expireTimerSeconds/revision/members[].role/pending
    /// Members/requestingMembers/bannedMembers/accessControl 四族；isMember
    /// 改计算值——own ACI ∈ members，早期「入库即成员恒 true」口径废止）。
    /// `terminated` 位 = 进程内终止登记（groups 模块；presage Group 模型无
    /// 该字段，读面以登记表为准，compat §5 终止批登记）。
    /// `filter` = 可选 groupId 过滤（P15b 审计项5 单群读投影）：Some 只投
    /// 命中的群，None 全表（wire additive，缺席 = 旧行为）。
    pub(crate) async fn list_groups(
        self: &Arc<Self>,
        account: &str,
        filter: Option<&[Vec<u8>]>,
    ) -> Result<Vec<Value>, SendError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let manager = lock_registered_for_send(&shared).await;
        let numbers = contacts_number_map(manager.store()).await;
        let own_aci = presage::libsignal_service::protocol::Aci::from(
            manager.registration_data().service_ids.aci,
        );
        let groups = manager
            .store()
            .groups()
            .await
            .map_err(|_| SendError::Internal)?;
        let mut out = Vec::new();
        for (master_key, group) in groups.filter_map(Result::ok) {
            if !group_key_passes_filter(&master_key, filter) {
                continue;
            }
            let terminated = {
                use base64::Engine as _;
                let id = base64::engine::general_purpose::STANDARD.encode(master_key);
                self.terminated_groups.contains(&id)
            };
            out.push(crate::service::groups::project_group_list_entry(
                &master_key,
                &group,
                &numbers,
                own_aci,
                terminated,
            ));
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_text_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        message: &str,
        quote: Option<&SendQuote>,
        preview: Option<&SendPreview>,
        mentions: &[SendMention],
        text_styles: &[SendTextStyle],
        attachments: &[String],
        sticker: Option<&SendSticker>,
        story_context: Option<&SendStoryContext>,
        view_once: bool,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let (pointers, preview_image) = upload_attachments_for_send(
            &manager,
            attachments,
            preview.and_then(|item| item.image.as_deref()),
        )
        .await?;
        let body = match sticker {
            Some(sticker) => {
                let pointer = upload_sticker_image(&manager, sticker).await?;
                build_sticker_data_message(manager.store(), sticker, quote, pointer, timestamp)
                    .await?
            }
            None if view_once => build_view_once_data_message(pointers, timestamp)?,
            None => {
                build_text_data_message(
                    manager.store(),
                    message,
                    quote,
                    preview,
                    mentions,
                    text_styles,
                    pointers,
                    preview_image,
                    story_context,
                    timestamp,
                )
                .await?
            }
        };
        let outcomes = dispatch_group(self, &mut manager, group_id, body, timestamp).await?;
        let numbers = contacts_number_map(manager.store()).await;
        Ok((
            timestamp,
            group_send_result_entries(outcomes, timestamp, &numbers),
        ))
    }

    pub(crate) async fn send_remote_delete_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let body = DataMessage {
            timestamp: Some(timestamp),
            delete: Some(presage::proto::data_message::Delete {
                target_sent_timestamp: Some(target_timestamp),
            }),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    pub(crate) async fn send_reaction_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_author: &str,
        target_timestamp: u64,
        emoji: &str,
        remove: bool,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        // 群内 targetAuthor 兼容号码与 ACI：本机号/contacts 命中转 UUID，
        // UUID 形状直通；都未命中原样透传，协议侧报错，不在此编造。
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .unwrap_or_else(|| target_author.trim().to_owned());
        let body = DataMessage {
            timestamp: Some(timestamp),
            reaction: Some(presage::proto::data_message::Reaction {
                emoji: Some(emoji.to_owned()),
                target_author_aci: Some(target_aci),
                target_sent_timestamp: Some(target_timestamp),
                remove: Some(remove),
                ..presage::proto::data_message::Reaction::default()
            }),
            ..DataMessage::default()
        };
        dispatch_group(self, &mut manager, group_id, body, timestamp)
            .await
            .map(|_| ())
    }

    pub(crate) async fn send_typing_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        stop: bool,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
        let body = ContentBody::TypingMessage(typing_group_body(&master_key, timestamp, stop));
        manager
            .send_message_to_group(&master_key, body, timestamp)
            .await
            .map_err(send_error_of)
            .map(|_| ())
    }

    /// 群组发送公共头：账号/群可解校验 + 终止面硬拒（官方
    /// shouldSendToConversation terminated 位同语义——已终止群一律拒发）+
    /// 时间戳统一此处取（fake 模式各方法自行早退，不走此处）。
    async fn open_group_sender(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
    ) -> Result<(SharedSendManager, u64), SendError> {
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        decode_group_id(group_id).ok_or(SendError::Internal)?;
        if self.send_target_terminated(account, group_id).await {
            return Err(SendError::TargetTerminated);
        }
        Ok((shared, now_ms()))
    }

    /// 发送面终止判定（R1 缓存化）：进程内登记表（本进程见过 terminated
    /// change/写面成功）先行；kv 探针缓存（24h TTL）次之；缓存 miss 才发
    /// 服务器群态读面并落缓存（读面失败不缓存不阻塞——网络故障不误伤既有
    /// 群会话，终止真值以登记表 + 服务器裁决为准）。官方发送面只看本地
    /// conversation.terminated 标志从不逐发送探测（shouldSendToConversation
    /// 同位），本缓存即该本地标志面；稳态群发 0 次探针 HTTPS。
    async fn send_target_terminated(&self, account: &str, group_id: &str) -> bool {
        if self.terminated_groups.contains(group_id) {
            return true;
        }
        if !self.real {
            return false;
        }
        let Some(master_key) = decode_group_id(group_id) else {
            return false;
        };
        let Ok(context) = storage_context(self, account).await else {
            return false;
        };
        group_cache::cached_terminated_probe(&context, &master_key).await
    }

    /// terminateGroup（群终止，KT 扩展方法）：幂等判定 → 群凭据 →
    /// `PATCH v2/groups` 上传 terminate change → 本地登记。fake 模式成功
    /// 形状桩（send 系同款纪律）。
    pub(crate) async fn terminate_group(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        groups::terminate_group(&context, &self.terminated_groups, &master_key).await
    }

    /// updateGroup（群更新写面，KT 扩展方法）：expiration/title/avatar/
    /// addMembers/removeMembers 合成单 change（同 version 多 action）单次
    /// `PATCH v2/groups`。avatar Replace 臂先走群头像上行链（form 拉取 →
    /// blob 加密 → CDN POST，§5 群头像上行面批）；add/remove 成员臂成员解
    /// 析与群态分类（不可解析整拒，§5 P3/P4 块）。fake 两臂：缺省/null 臂
    /// 成功形状桩（PATCH 面，expiration 批纪律；成员臂随桩成功——fake 无解
    /// 析链）；data URI 臂失败 AvatarUpload——fake 无 CDN/服务器上行面，上
    /// 行链不可达即错误面（stickers fake 错误面先例）。
    pub(crate) async fn update_group_change(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        write: groups::GroupUpdateWrite,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return match write.avatar {
                Some(groups::AvatarWrite::Replace(_)) => Err(groups::TerminateError::AvatarUpload),
                _ => Ok(()),
            };
        }
        let context = storage_context(self, account).await?;
        groups::update_group_change(&context, &master_key, write).await
    }

    /// leaveGroup（群退出，KT 扩展方法，官方桌面 leaveGroupV2 行为对齐）：
    /// 群态读面 self 分类（ACI pending → ACI member → PNI pending，官方同
    /// 序）→ `PATCH v2/groups` delete change → 本地群态刷新。fake 模式成功
    /// 形状桩（terminateGroup 同纪律；fake 无群态读面，不触网）。
    pub(crate) async fn leave_group(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        groups::leave_group(&context, &master_key).await
    }

    /// previewGroupViaLink（链接预览读面，KT 扩展方法，官方
    /// getPreJoinGroupInfo 行为对齐）：链接已由 handler 解析（形状面
    /// -32603 收口在方法层），此处读面 + 投影。读面非协议上行不落调度
    /// 日志（terminateGroup mutating 面纪律的读面同位）。fake 模式错误面
    /// 形状桩：Failed{None} → GROUP_INVITE_LINK_PREVIEW_FAILED（无
    /// status）——fake 无链接读面，材料不可得即错误面（stickers fake 错误
    /// 面先例）。
    pub(crate) async fn preview_group_via_link(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        password: Vec<u8>,
    ) -> Result<groups::GroupJoinPreview, groups::TerminateError> {
        if !self.real {
            return Err(groups::TerminateError::Failed { status: None });
        }
        let context = storage_context(self, account).await?;
        groups::preview_group_via_link(&context, &master_key, &password).await
    }

    /// joinGroupViaLink（经链接入群，KT 扩展方法，官方 joinGroupV2ViaLink
    /// 行为对齐，双臂同构）：返回 requested = true 为审批臂
    ///（{status:"requested"}）、false 为 direct 臂（{status:"joined"}）。
    /// 写面协议上行落调度日志（handler 面）。fake 模式形状桩 {status:
    /// "joined"}（direct 臂形状——fake 无 join info 读面不裁定审批臂，§5
    /// 登记偏差）。
    pub(crate) async fn join_group_via_link(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        password: Vec<u8>,
    ) -> Result<bool, groups::TerminateError> {
        if !self.real {
            return Ok(false);
        }
        let context = storage_context(self, account).await?;
        groups::join_group_via_link(&context, &master_key, &password).await
    }

    /// cancelJoinRequest（撤回入群申请，KT 扩展方法，官方桌面
    /// cancelJoinRequest 行为对齐）：重拉 join info 取 version +
    /// delete_members_pending_admin_approval，链接授权写面单次 PATCH。
    /// fake 模式成功形状桩 {status:"cancelled"}（leaveGroup/terminateGroup
    /// 同纪律）。
    pub(crate) async fn cancel_join_request_via_link(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        password: Vec<u8>,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        groups::cancel_join_request_via_link(&context, &master_key, &password).await
    }

    /// approveJoinRequest（管理员批准入群申请，KT 扩展方法，官方
    /// approvePendingMembershipFromGroupV2 行为对齐）：群态读面本地闸
    ///（requesting_members 在场）+ promote_members_pending_admin_approval
    /// 普通 PATCH。fake 模式成功形状桩 {status:"approved"}。
    pub(crate) async fn approve_join_request(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        recipient: &str,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        groups::approve_join_request(&context, &master_key, recipient).await
    }

    /// createGroup（自建群，KT 扩展方法，官方 createGroupV2 行为对齐）：
    /// 随机 master key + zkgroup 派生链 + 初始 Group proto `PUT v1/groups`
    /// 与本地群态落库；成员入参（service-id 串/号码）在 real 模式解析。
    /// fake 模式形状桩（随机 master key 标准 base64 + revision 0，不触网；
    /// avatar data URI 臂同 updateGroup fake 纪律 = AvatarUpload 错误面）。
    pub(crate) async fn create_group(
        self: &Arc<Self>,
        account: &str,
        title: &str,
        expiration_secs: Option<u32>,
        avatar: Option<groups::AvatarWrite>,
        members: &[String],
    ) -> Result<(String, u32), groups::TerminateError> {
        if !self.real {
            return match avatar {
                Some(groups::AvatarWrite::Replace(_)) => Err(groups::TerminateError::AvatarUpload),
                _ => {
                    use base64::Engine as _;
                    let master_key = groups::random_32_bytes_pub();
                    Ok((
                        base64::engine::general_purpose::STANDARD.encode(master_key),
                        0,
                    ))
                }
            };
        }
        let context = storage_context(self, account).await?;
        groups::create_group(&context, title, expiration_secs, avatar, members).await
    }

    /// generateInviteLinkPassword（群邀请链接族，KT 扩展批 P13/任务1）：16B
    /// CSPRNG（官方 GROUP_INVITE_LINK_PASSWORD_LENGTH 同长）。纯本地面，
    /// fake/real 同码路，零网络。
    pub(crate) fn generate_invite_link_password(&self) -> [u8; group_link::LINK_PASSWORD_BYTES] {
        group_link::generate_invite_link_password()
    }

    /// buildInviteLink（同批）：password 显式臂 = 纯构造（fake/real 同码路，
    /// 官方 buildGroupLink 同形状）；password 缺省臂 = 服务器群态读面取
    /// invite_link_password（官方 conversation.groupInviteLinkPassword 同
    /// 位；fake 无群态面 → InviteLinkUnset）。
    pub(crate) async fn build_invite_link(
        self: &Arc<Self>,
        account: &str,
        master_key: &[u8],
        password: Option<Vec<u8>>,
    ) -> Result<String, groups::TerminateError> {
        if let Some(password) = password {
            return Ok(group_link::build_invite_link(master_key, &password));
        }
        if !self.real {
            return Err(groups::TerminateError::InviteLinkUnset);
        }
        let context = storage_context(self, account).await?;
        let group = groups::fetch_group_state(&context, master_key).await?;
        if group.invite_link_password.is_empty() {
            return Err(groups::TerminateError::InviteLinkUnset);
        }
        Ok(group_link::build_invite_link(
            master_key,
            &group.invite_link_password,
        ))
    }

    /// calls.linkPresentation（KT 扩展，P4c 切一）：当天 CallLinkAuthCredential
    /// 呈现字节（compat §3 calls 族行 + §5 块）。材料面非协议上行，不落调度
    /// 日志；凭据/呈现字节零日志零落盘。fake 模式形状桩（非有效 zk 材料）。
    pub(crate) async fn call_link_presentation(
        self: &Arc<Self>,
        account: &str,
        root_key: &[u8],
    ) -> Result<Vec<u8>, call_links::CallCredentialError> {
        if !self.real {
            return Ok(call_links::FAKE_LINK_PRESENTATION.to_vec());
        }
        let context = storage_context(self, account).await?;
        call_links::link_presentation(&context, root_key).await
    }

    /// calls.createLinkAuth（同批）：两段式出 CreateCallLinkCredential
    /// Presentation + CallLinkPublicParams 双字节。fake 模式形状桩。
    pub(crate) async fn create_link_auth(
        self: &Arc<Self>,
        account: &str,
        root_key: &[u8],
        room_id: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), call_links::CallCredentialError> {
        if !self.real {
            return Ok((
                call_links::FAKE_CREATE_PRESENTATION.to_vec(),
                call_links::FAKE_CREATE_PUBLIC_PARAMS.to_vec(),
            ));
        }
        let context = storage_context(self, account).await?;
        call_links::create_link_auth(&context, root_key, room_id).await
    }

    /// calls.groupProof（同批）：ExternalGroupCredential.token 透出。fake
    /// 模式形状桩（固定串）。
    pub(crate) async fn call_group_proof(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
    ) -> Result<String, call_links::CallCredentialError> {
        if !self.real {
            return Ok(call_links::FAKE_GROUP_PROOF.to_owned());
        }
        let context = storage_context(self, account).await?;
        call_links::group_proof(&context, &master_key).await
    }

    /// calls.groupMembers（同批）：群成员 UuidCiphertext 材料清单。fake
    /// 模式空清单形状桩（读面纪律，getPinnedConversations 同款）。
    pub(crate) async fn call_group_members(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
    ) -> Result<Vec<call_links::MemberMaterial>, call_links::CallCredentialError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        call_links::group_members(&context, &master_key).await
    }

    /// calls.endorsementsPublicKey（同批）：legacy ServerPublicParams 派生
    /// EndorsementPublicKey 序列化字节；常量派生不触网，fake/real 同码路。
    pub(crate) fn endorsements_public_key(&self) -> Vec<u8> {
        call_links::endorsements_public_key_bytes()
    }

    /// getUserStatus：whoami 轻量鉴权探测（real 模式触网；fake 恒已注册）。
    /// 仅 401 判解绑——网络等其他失败按内部失败，不误报解绑态。
    pub(crate) async fn get_user_status(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<(), UserStatusError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(UserStatusError::Internal)?;
        let manager = lock_registered_for_send(&shared).await;
        match manager.whoami().await {
            Ok(_) => Ok(()),
            Err(presage::Error::ServiceError(
                presage::libsignal_service::prelude::ServiceError::Unauthorized,
            )) => Err(UserStatusError::Unbound),
            Err(_) => Err(UserStatusError::Internal),
        }
    }

    /// engine.deviceCapabilities（D5 设备面能力注册）：PUT
    /// /v1/devices/capabilities（官方 registerCapabilities 同端点，位值 =
    /// capabilities::DEVICE_CAPABILITIES_UPLOAD 钉定镜像）。fake 恒已注册
    /// （getUserStatus 同款）；仅 401 判解绑，其余折叠 Internal。宿主手动
    /// 触发 + 上传面回读；接收管线建流另有自动注册（同一常量）。
    pub(crate) async fn register_device_capabilities(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<(), DeviceCapabilitiesError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(DeviceCapabilitiesError::Internal)?;
        let manager = lock_registered_for_send(&shared).await;
        match manager
            .register_device_capabilities(capabilities::DEVICE_CAPABILITIES_UPLOAD)
            .await
        {
            Ok(()) => Ok(()),
            Err(presage::Error::ServiceError(
                presage::libsignal_service::prelude::ServiceError::Unauthorized,
            )) => Err(DeviceCapabilitiesError::Unbound),
            Err(_) => Err(DeviceCapabilitiesError::Internal),
        }
    }

    /// updateContact：P1b 仅在本地 store 命中该号码时改名（uuid 归属需要
    /// contacts 同步，P2 补服务器面）；未命中为幂等 no-op（compat §3）。
    pub(crate) async fn update_contact(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        name: &str,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let manager = lock_registered_for_send(&shared).await;
        let existing = find_contact_by_number(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        let mut contact = existing;
        contact.name = name.to_owned();
        let mut store = manager.store().clone();
        store
            .save_contact(&contact)
            .await
            .map_err(|_| SendError::Internal)?;
        Ok(())
    }

    /// setExpirationTimer（消失消息 E 批，KT 扩展方法）：1:1 会话计时器
    /// 设置，语义对位 signal-cli `Manager.setExpirationTimer`
    /// （ManagerImpl.java:1419 = ContactHelper 持久化 + asExpirationUpdate
    /// 广播）：持久化联系人计时器（presage store Contact 面真相源，
    /// version 单调 +1，接收方据此做乱序丢弃）后向对端广播 expiration-
    /// update（DataMessage.Flags.EXPIRATION_TIMER_UPDATE=2 位，官方桌面
    /// v8.31 收发两侧同 flag 机制；信封 = flag + expireTimer +
    /// expireTimerVersion，经 presage send 面 restore_thread_timer 注入
    /// 刚持久化的 (timer, version) 对——构造面不设 expire_timer，显式设置
    /// 会被 presage 对已设值再 +1 造成 store/线面脱节）。timer=0（关闭）
    /// 经 presage 0→None 归一后线上 = 官方桌面关闭形态（无 expireTimer
    /// 字段的 expiration-update，官方 updateExpirationTimer(undefined) 同
    /// 形状）。fail-closed：未注册联系人（store contacts 无行——presage
    /// update_expire_timer 对缺行静默 no-op，必须前置拒绝）与 timer 非法
    /// 值（methods.rs 参数面）。同值 no-op 不广播不 bump（signal-cli
    /// ContactHelper 同款）。返回广播信封时间戳（同值 no-op = 处理时刻）。
    pub(crate) async fn set_expiration_timer(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        timer_seconds: u32,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let service_id = resolve_recipient(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        let mut store = manager.store().clone();
        // 未注册联系人拒绝：contacts 表必须已有该收件人行（UUID 形态直通
        // 也不例外——无行 = presage 持久化面静默 no-op，宁拒不哑改）。
        store
            .contact_by_id(&service_id)
            .await
            .map_err(|_| SendError::Internal)?
            .ok_or(SendError::Internal)?;
        let thread = presage::store::Thread::Contact(service_id);
        let current = store
            .expire_timer(&thread)
            .await
            .map_err(|_| SendError::Internal)?;
        if current.map(|(timer, _)| timer) == Some(timer_seconds) {
            return Ok(now_ms());
        }
        let version = current
            .map(|(_, version)| version)
            .unwrap_or(0)
            .saturating_add(1);
        store
            .update_expire_timer(&thread, timer_seconds, version)
            .await
            .map_err(|_| SendError::Internal)?;
        let timestamp = now_ms();
        let body = build_expiration_timer_update_data_message(timestamp);
        manager
            .send_message(service_id, body, timestamp)
            .await
            .map_err(send_error_of)?;
        Ok(timestamp)
    }

    /// getStickerPackManifest（P4 扩面，不依赖账号）：匿名 CDN 下载 +
    /// 解密 + 投影。参数校验 fail-closed。fake 模式走错误面（该开关仅
    /// IPC 形状测试用，生产永远 real，compat §6）。
    pub(crate) async fn sticker_pack_manifest(
        &self,
        pack_id: &str,
        pack_key: &str,
    ) -> Result<stickers::ManifestProjection, stickers::StickerError> {
        let key =
            stickers::decode_pack_key(pack_key).ok_or(stickers::StickerError::InvalidParams)?;
        stickers::decode_pack_id(pack_id).ok_or(stickers::StickerError::InvalidParams)?;
        if !self.real {
            return Err(stickers::StickerError::Fetch);
        }
        stickers::fetch_manifest(pack_id, &key).await
    }

    /// getStickerImage（P4 扩面，不依赖账号）：匿名 CDN 单图下载 + 解密 +
    /// MIME 嗅探。≤300KB fail-closed。
    pub(crate) async fn sticker_image(
        &self,
        pack_id: &str,
        pack_key: &str,
        sticker_id: u32,
    ) -> Result<stickers::ImageProjection, stickers::StickerError> {
        let key =
            stickers::decode_pack_key(pack_key).ok_or(stickers::StickerError::InvalidParams)?;
        stickers::decode_pack_id(pack_id).ok_or(stickers::StickerError::InvalidParams)?;
        if !self.real {
            return Err(stickers::StickerError::Fetch);
        }
        stickers::fetch_image(pack_id, &key, sticker_id).await
    }

    /// getPinnedConversations（P4 扩面）：读 Storage Service
    /// AccountRecord.pinnedConversations → 引擎会话标识投影（存储序）。
    pub(crate) async fn pinned_conversations(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<storage_sync::PinnedEntry>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sync::read_pinned(&context).await
    }

    /// getBlockedConversations（P5 扩面）：读 Storage Service 全部
    /// Contact/GroupV1/GroupV2 记录的 blocked 位 → 引擎会话标识投影（存储
    /// 序；形状与置顶条目同构，只读面不落 store）。
    pub(crate) async fn blocked_conversations(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<storage_sync::EngineConversationId>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sync::read_blocked_conversations(&context).await
    }

    /// setConversationPinned（P4 扩面）：官方写语义（读改写 AccountRecord，
    /// 仅动 pinned 字段）+ 409 冲突重放（conflictBackOff 3 次）。
    pub(crate) async fn set_conversation_pinned(
        self: &Arc<Self>,
        account: &str,
        conversation: storage_sync::EngineConversationId,
        pinned: bool,
    ) -> Result<Vec<storage_sync::PinnedEntry>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sync::write_pinned(&context, conversation, pinned).await
    }

    /// setConversationBlocked（拉黑写面）：官方写语义（读改写目标会话
    /// 记录，仅动 blocked 位 + blockedAtTimestamp）+ 409 冲突重放；返回
    /// 写后全量 blocked 名单（存储序，与置顶面同构）。fake 模式空表桩。
    pub(crate) async fn set_conversation_blocked(
        self: &Arc<Self>,
        account: &str,
        conversation: storage_sync::EngineConversationId,
        blocked: bool,
    ) -> Result<Vec<storage_sync::EngineConversationId>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sync::write_blocked(&context, &conversation, blocked, now_ms()).await
    }

    /// getConversationState（P15b 项2）：读 Storage Service 目标会话记录
    /// 三 flag（mutedUntilTimestamp/archived/markedUnread）→ 读投影；目标
    /// 记录缺席 / 无 storage 状态 = 全缺省（诚实幂等）。fake 模式缺省桩。
    pub(crate) async fn conversation_state(
        self: &Arc<Self>,
        account: &str,
        conversation: storage_sync::EngineConversationId,
    ) -> Result<storage_sync::ConversationState, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_sync::ConversationState::default());
        }
        let context = storage_context(self, account).await?;
        storage_sync::read_conversation_state(&context, &conversation).await
    }

    /// setConversationState（P15b 项2）：三 flag 部分写意图（缺席位不动）
    /// → Storage Service 目标会话记录读改写 + 409 冲突重放；返回写后三
    /// flag（官方 upload 成功语义 = 本地写后面为准）。fake 模式缺省态叠
    /// 意图回显。
    pub(crate) async fn set_conversation_state(
        self: &Arc<Self>,
        account: &str,
        conversation: storage_sync::EngineConversationId,
        intent: storage_sync::ConversationStateWrite,
    ) -> Result<storage_sync::ConversationState, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_sync::ConversationState {
                muted_until_timestamp: intent.muted_until_timestamp.unwrap_or(0),
                archived: intent.archived.unwrap_or(false),
                marked_unread: intent.marked_unread.unwrap_or(false),
            });
        }
        let context = storage_context(self, account).await?;
        storage_sync::write_conversation_state(&context, &conversation, intent).await
    }

    /// getStickerPackSyncs（P5 扩面）：读全部贴纸包安装状态（存储序 =
    /// manifest STICKER identifiers 首现序；坏条目跳过不炸面）。
    pub(crate) async fn sticker_pack_syncs(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<storage_sticker::StickerPackSync>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sticker::read_sticker_packs(&context).await
    }

    /// setStickerPackSync（P5 扩面）：安装/卸载写面（官方 toStickerPackRecord
    /// 语义 + 409 采纳重放）；卸载墓碑时间戳 = 引擎时钟；返回写后云态投影。
    pub(crate) async fn set_sticker_pack_sync(
        self: &Arc<Self>,
        account: &str,
        intent: storage_sticker::StickerPackWrite,
    ) -> Result<Vec<storage_sticker::StickerPackSync>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sticker::write_sticker_pack(&context, &intent, now_ms()).await
    }

    /// listIdentities（安全码引擎面批，KT 扩展方法）：单引用窄读——号码/
    /// ACI/PNI 形态直通 resolve 阶梯 → 对端 identity + 本机 identity 双方
    /// 指纹（Fingerprint v2/5200）→ 数字串（12 组排版）+ scannable
    /// （CombinedFingerprints base64）投影；trust 态读 identity-trust.json
    /// sidecar（首读建档，compat §5）。对端 identity 未知 → 空表
    /// （SafetyNumberNotReady 态非错误）；窄读不命中 → 空表。Err = 账号
    /// 不指向已链接账号（-32603）。fake 模式空表桩。
    pub(crate) async fn list_identities(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        number: Option<String>,
    ) -> Result<Vec<serde_json::Value>, ()> {
        if !self.real {
            return Ok(Vec::new());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account).await.ok_or(())?;
        let manager = lock_registered_for_send(&shared).await;
        let Some(peer) = resolve_recipient(&manager, recipient).await else {
            return Ok(Vec::new());
        };
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        Ok(identity::identity_records(
            &manager.store().aci_protocol_store(),
            &own_aci,
            peer,
            number.as_deref(),
            &account_dir,
            now_ms(),
        )
        .await)
    }

    /// trust（安全码引擎面批，KT 扩展方法）：verified 臂三形态比对闸
    /// （60 位数字/66 位 hex/base64 scannable）+ clear 臂（无数字闸）；
    /// 落 identity-trust.json sidecar 后 best-effort 自发
    /// SyncMessage.verified（Content oneof verified=7，signal-cli
    /// sendVerifiedMessage 同构）——同步失败不回滚不报错，sidecar 写为
    /// 权威突变（compat §5）。fake 模式成功桩。
    pub(crate) async fn trust_identity(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        intent: identity::TrustIntent,
    ) -> Result<(), identity::TrustError> {
        if !self.real {
            return Ok(());
        }
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(identity::TrustError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let Some(peer) = resolve_recipient(&manager, recipient).await else {
            return Err(identity::TrustError::Failed);
        };
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        let outcome = identity::write_trust(
            &manager.store().aci_protocol_store(),
            &own_aci,
            peer,
            &intent,
            &account_dir,
            now_ms(),
        )
        .await?;
        let verified = presage::proto::Verified {
            destination_aci_binary: Some(peer.service_id_binary()),
            identity_key: Some(outcome.peer_key),
            state: Some(outcome.state.sync_state() as i32),
            ..Default::default()
        };
        let body = ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
            content: Some(presage::proto::sync_message::Content::Verified(verified)),
            ..Default::default()
        });
        let _ = manager.send_message(own_aci, body, now_ms()).await;
        Ok(())
    }

    /// chatFolders.list（Storage 跨设备同步批）：ChatFolder 记录读投影
    /// （存储序；坏条目跳过；读写直通不落引擎本地，compat §5）。
    pub(crate) async fn chat_folders(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::read_records(&context, storage_sync::StorageRecordKind::ChatFolder).await
    }

    /// chatFolders.setRecords（Storage 跨设备同步批）：官方 upload 语义
    /// 写面（每实体独立 record + 16B item id 换写 + 409 采纳重放 + 首写
    /// 播种 Account）；返回写后全量投影（存储序）。
    pub(crate) async fn set_chat_folders(
        self: &Arc<Self>,
        account: &str,
        records: Vec<storage_records::RecordValue>,
        remove_ids: Vec<Vec<u8>>,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::write_records(
            &context,
            storage_sync::StorageRecordKind::ChatFolder,
            records,
            remove_ids,
        )
        .await
    }

    /// notificationProfiles.list（Storage 跨设备同步批）：同 chatFolders
    /// 读面，type=9。
    pub(crate) async fn notification_profiles(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::read_records(
            &context,
            storage_sync::StorageRecordKind::NotificationProfile,
        )
        .await
    }

    /// notificationProfiles.setRecords（Storage 跨设备同步批）：同
    /// chatFolders 写面，type=9。
    pub(crate) async fn set_notification_profiles(
        self: &Arc<Self>,
        account: &str,
        records: Vec<storage_records::RecordValue>,
        remove_ids: Vec<Vec<u8>>,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::write_records(
            &context,
            storage_sync::StorageRecordKind::NotificationProfile,
            records,
            remove_ids,
        )
        .await
    }

    /// storyDistributionLists.list（Stories 跨设备名单同步批）：同
    /// chatFolders 读面，type=5（STORY_DISTRIBUTION_LIST）。
    pub(crate) async fn story_distribution_lists(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::read_records(
            &context,
            storage_sync::StorageRecordKind::StoryDistributionList,
        )
        .await
    }

    /// storyDistributionLists.setRecords（Stories 跨设备名单同步批）：同
    /// chatFolders 写面（upsert/墓碑/幂等/409 采纳重放），type=5。
    pub(crate) async fn set_story_distribution_lists(
        self: &Arc<Self>,
        account: &str,
        records: Vec<storage_records::RecordValue>,
        remove_ids: Vec<Vec<u8>>,
    ) -> Result<Vec<serde_json::Value>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_records::write_records(
            &context,
            storage_sync::StorageRecordKind::StoryDistributionList,
            records,
            remove_ids,
        )
        .await
    }

    /// setAccountConfiguration（P5 扩面，官方 updateConfiguration 等价）：
    /// 账号级 readReceipts/typingIndicators/手机号可见性 → Storage Service
    /// AccountRecord 写面（读改写仅动提供字段）+ 409 采纳重放；全部提供
    /// 字段已满足 → 幂等无写直接成功。
    pub(crate) async fn set_account_configuration(
        self: &Arc<Self>,
        account: &str,
        update: storage_sync::AccountConfigurationUpdate,
    ) -> Result<(), storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        storage_sync::write_account_configuration(&context, update).await
    }

    /// setProfile（P5 扩面，官方 updateProfile 等价）：名字/关于/emoji +
    /// 头像（缺省保留/data URI 换新/null 移除）server 面写 + storage 面
    /// AccountRecord 名字与头像 path 同步。
    pub(crate) async fn set_profile(
        self: &Arc<Self>,
        account: &str,
        update: profile::ProfileUpdate,
    ) -> Result<(), profile::ProfileError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        profile::set_profile(&context, update).await
    }

    /// setUniversalExpireTimer（P5 扩面，官方 universal timer 写面）：账户级
    /// 默认消失消息计时器 → Storage Service AccountRecord.universalExpireTimer
    /// (17) 写面；同值幂等无写。
    pub(crate) async fn set_universal_expire_timer(
        self: &Arc<Self>,
        account: &str,
        expiration_seconds: u32,
    ) -> Result<(), storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        storage_sync::write_universal_expire_timer(&context, expiration_seconds).await
    }

    /// getAvatar（头像下行，KT 扩展方法）：1:1 与群统一入口，presage 官方
    /// 头像下行 API（缓存/CDN/解密/入库），fake 模式可达「无头像」错误面。
    pub(crate) async fn get_avatar(
        self: &Arc<Self>,
        account: &str,
        conversation_id: &str,
    ) -> Result<avatar::AvatarPayload, avatar::AvatarError> {
        avatar::get_avatar(self, account, conversation_id).await
    }

    /// reportSpam（举报，KT 扩展方法）：官方 v1/messages/report 逐条 POST
    /// （本机凭据 basic auth），部分失败收敛（全部失败才报错）；fake 模式
    /// 全量成功形状桩（send 系同款纪律）。
    pub(crate) async fn report_spam(
        self: &Arc<Self>,
        account: &str,
        sender_aci: ServiceId,
        server_guids: Vec<String>,
        token: Option<String>,
    ) -> Result<report::ReportOutcome, report::ReportSpamError> {
        if !self.real {
            return Ok(report::ReportOutcome {
                reported: server_guids.len(),
                failures: Vec::new(),
            });
        }
        let context = storage_context(self, account).await?;
        report::report_spam(&context, &sender_aci, &server_guids, token.as_deref()).await
    }

    /// getUsername（用户名读面，KT 扩展方法）：AccountRecord.username(33)/
    /// usernameLink(35) 投影——真相源 = Storage Service（多设备经 storage
    /// sync 收敛，官方「仅 server confirm/delete 时更新」同语义）；从未有
    /// storage 状态 = `{}`（未设，不报错）。fake 模式形状桩 `{}`。
    pub(crate) async fn get_username(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<serde_json::Value, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(json!({}));
        }
        let context = storage_context(self, account).await?;
        username::get_own_username(&context).await
    }

    /// usernameLookup（用户名解析读面，KT 扩展方法）：条目（全名或
    /// signal.me 链接）→ 服务端匿名 lookup 通路 → 逐条投影（输入序，
    /// 失败条目诚实降级 isRegistered:false，不炸整批）。账号参数照常
    /// 走 storage_context 解析梯（未注册 -32603）。fake 模式形状桩。
    pub(crate) async fn username_lookup(
        self: &Arc<Self>,
        account: &str,
        entries: Vec<String>,
    ) -> Result<serde_json::Value, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(json!(entries
                .iter()
                .map(|entry| json!({ "recipient": entry, "isRegistered": false }))
                .collect::<Vec<_>>()));
        }
        // 账号解析梯（未注册账号 -32603，与一切 account 参数面同口径）。
        let _context = storage_context(self, account).await?;
        let push = username::anonymous_push();
        let mut rows = Vec::with_capacity(entries.len());
        for entry in &entries {
            let (username, uuid, registered) = username::resolve_entry(&push, entry).await;
            let mut row = serde_json::Map::new();
            row.insert("recipient".to_owned(), json!(entry));
            if let Some(username) = username {
                row.insert("username".to_owned(), json!(username));
            }
            if let Some(uuid) = uuid {
                row.insert("uuid".to_owned(), json!(uuid));
            }
            row.insert("isRegistered".to_owned(), json!(registered));
            rows.push(serde_json::Value::Object(row));
        }
        Ok(json!(rows))
    }

    /// usernameReserve（用户名写面一，KT 扩展方法）：候选生成 + 批 hash
    /// → 服务端 reserve → 采纳候选投影。fake 模式纯本地形状桩（语法/hash
    /// 走同一 crate 面，无网络）。
    pub(crate) async fn username_reserve(
        self: &Arc<Self>,
        account: &str,
        username: &str,
    ) -> Result<username::UsernameReservation, username::UsernameWriteError> {
        if !self.real {
            // reserve_candidates 只产 BadParams（形状闸）——原样上浮，让
            // 参数面 fail-closed 走 -32603 而非服务器四码。
            let candidates = username::reserve_candidates(username)?;
            let candidate = candidates
                .first()
                .ok_or(username::UsernameWriteError::ServerFailed)?;
            let hash = usernames::Username::new(candidate)
                .map_err(|_| username::UsernameWriteError::ServerFailed)?
                .hash();
            use base64::Engine as _;
            return Ok(username::UsernameReservation {
                username: candidate.clone(),
                username_hash: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash),
            });
        }
        let context = storage_context(self, account).await?;
        username::reserve_username(&context, username).await
    }

    /// usernameConfirm（用户名写面二，KT 扩展方法）：proof + link 加密 →
    /// 服务端 confirm → 完整链接重建 → AccountRecord 33/35 storage 落盘。
    /// fake 模式纯本地形状桩（随机 handle，不落盘）。
    pub(crate) async fn username_confirm(
        self: &Arc<Self>,
        account: &str,
        username: &str,
        username_hash: &str,
    ) -> Result<serde_json::Value, username::UsernameWriteError> {
        if !self.real {
            let parsed = usernames::Username::new(username)
                .map_err(|_| username::UsernameWriteError::BadParams)?;
            let (entropy, _ciphertext) =
                usernames::create_for_username(&mut rand::rng(), parsed.to_string(), None)
                    .map_err(|_| username::UsernameWriteError::ServerFailed)?;
            let mut handle_bytes = [0u8; 16];
            rand::Rng::fill(&mut rand::rng(), &mut handle_bytes[..]);
            let handle = presage::libsignal_service::prelude::Uuid::from_slice(&handle_bytes)
                .map_err(|_| username::UsernameWriteError::ServerFailed)?;
            return Ok(json!({
                "username": parsed.to_string(),
                "usernameLink": username::build_username_link(&entropy, &handle),
            }));
        }
        let context = storage_context(self, account).await?;
        username::confirm_username(&context, username, username_hash).await
    }

    /// usernameDelete（用户名写面三，KT 扩展方法）：服务端 DELETE →
    /// AccountRecord 33/35 清除（本就未设 = 幂等无写）。fake 模式形状桩。
    pub(crate) async fn username_delete(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<serde_json::Value, username::UsernameWriteError> {
        if !self.real {
            return Ok(json!({}));
        }
        let context = storage_context(self, account).await?;
        username::delete_username(&context).await
    }

    /// storage.write（P0，KT 扩展方法）：通用批量 manifest item 写（compat
    /// §5「storage 写面 P0」块）。参数闸已在 dispatch 层 parse_request 全量
    /// 生效；fake 模式参数闸后形状桩；409 → VersionConflict（不自动重试，
    /// 调用方重读 manifest 后重放）。
    pub(crate) async fn storage_write(
        self: &Arc<Self>,
        account: &str,
        updates: Vec<storage_write::StorageItemUpdate>,
    ) -> Result<storage_write::WriteOutcome, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_write::fake_outcome(&updates));
        }
        let context = storage_context(self, account).await?;
        storage_write::write_items(&context, updates).await
    }

    /// 云备份 restore 启动（cloud-backup-contract §2：`backups.restoreFromCloud`；
    /// send lane mutating；同引擎至多一个，重入 = Transport already running）。
    /// AEP/ACI/signing key 从账号 store/registration 取（snapshot_import 先例）；
    /// 非 Send future spawn_local 承载（link_flow/download_transfer_archive 同
    /// 策略）。NDJSON 产物落 `history-import-cloud.ndjson`（connector 消费链
    /// 与 transfer_archive 同位）。
    pub(crate) async fn cloud_backup_restore_start(
        &self,
        account: &str,
    ) -> Result<(), cloud_backup::CloudRestoreError> {
        use presage::libsignal_service::protocol::IdentityKeyStore as _;
        use presage::store::Store as _;
        let context = storage_context(self, account)
            .await
            .map_err(|_| cloud_backup::CloudRestoreError::AuthRejected)?;
        // fail-fast 预检：AEP/registration 缺席 = 同步 AuthRejected（契约 §3
        // 诚实上报口径）；派生 keys 由 run_cloud_restore 内部同路径自取，
        // 此处不重复派生。
        context
            .store
            .fetch_account_entropy_pool()
            .await
            .map_err(|_| cloud_backup::CloudRestoreError::AuthRejected)?
            .ok_or(cloud_backup::CloudRestoreError::AuthRejected)?;
        context
            .store
            .load_registration_data()
            .await
            .map_err(|_| cloud_backup::CloudRestoreError::AuthRejected)?
            .ok_or(cloud_backup::CloudRestoreError::AuthRejected)?;
        // 呈现签名密钥 = 本机 ACI 身份私钥（presage protocol store 对称读出；
        // link_flow 落库对位）。
        let signing_key = *context
            .store
            .aci_protocol_store()
            .get_identity_key_pair()
            .await
            .map_err(|_| cloud_backup::CloudRestoreError::AuthRejected)?
            .private_key();
        let account_dir = context.account_dir.clone();
        let push = context.push.clone();
        let store = context.store.clone();
        let progress = self.cloud_restore_progress.clone();
        // 重入闸置位（原子 compare-and-set）：前置检查全过后、spawn 前抢占；
        // 闭包收尾释放（finish_running），并发第二请求如实吃 AlreadyRunning。
        if !cloud_backup::CloudRestoreProgress::try_begin_running() {
            return Err(cloud_backup::CloudRestoreError::AlreadyRunning);
        }
        tokio::task::spawn_local(async move {
            use rand::SeedableRng as _;
            let mut rng = rand::rngs::StdRng::from_seed(rand::random());
            let rng: &mut (dyn rand::CryptoRng + Send) = &mut rng;
            let result = cloud_backup::run_cloud_restore(
                &push,
                account_dir,
                store,
                signing_key,
                rng,
                &progress,
            )
            .await;
            // 终态纪律（transfer_archive 同）：成功清快照（缺席=无在途）；
            // 失败留 failed 相位快照（结构化 code 诚实透出，下次 start 覆盖）。
            match result {
                Ok(_) => progress.clear(),
                Err(e) => progress.fail(e.rpc_code()),
            }
            cloud_backup::CloudRestoreProgress::finish_running();
        });
        Ok(())
    }

    /// 云备份 restore 进度/状态（`backups.cloudStatus` 只读数据源；快照缺席 =
    /// `{present:false}`）。
    pub(crate) fn cloud_backup_status(&self) -> Option<cloud_backup::CloudRestoreSnapshot> {
        self.cloud_restore_progress.snapshot()
    }

    /// 云备份 upload 启动（cloud-backup-contract §6.2：`backups.uploadToCloud`；
    /// send lane mutating；同引擎至多一个，重入 = BACKUP_UPLOAD_ALREADY_RUNNING）。
    /// 前置检查序与 restore 同构：storage_context → AEP/registration → signing
    /// key → 工件闸 → 单槽重入闸；装配段专用 OS 线程（§10.3 同款）+ 上行段
    /// LocalSet watcher（watch_and_upload 统一 finally：临时产物必删 + 闸释放）。
    pub(crate) async fn cloud_backup_upload_start(
        &self,
        account: &str,
    ) -> Result<(), cloud_upload::CloudUploadError> {
        use presage::libsignal_service::protocol::IdentityKeyStore as _;
        use presage::store::Store as _;
        let context = storage_context(self, account)
            .await
            .map_err(|_| cloud_upload::CloudUploadError::AuthRejected)?;
        // fail-fast 预检（restore 同口径）：AEP/registration 缺席 = 同步
        // AuthRejected（契约 §6.2 诚实上报）。
        let account_entropy_pool = context
            .store
            .fetch_account_entropy_pool()
            .await
            .map_err(|_| cloud_upload::CloudUploadError::AuthRejected)?
            .ok_or(cloud_upload::CloudUploadError::AuthRejected)?;
        let registration = context
            .store
            .load_registration_data()
            .await
            .map_err(|_| cloud_upload::CloudUploadError::AuthRejected)?
            .ok_or(cloud_upload::CloudUploadError::AuthRejected)?;
        // 呈现签名密钥 = 本机 ACI 身份私钥（restore 同源）。
        let signing_key = *context
            .store
            .aci_protocol_store()
            .get_identity_key_pair()
            .await
            .map_err(|_| cloud_upload::CloudUploadError::AuthRejected)?
            .private_key();
        // 工件闸（§6.2）：缺失/首行非法同步终态，不起线程（非法当场删工件）。
        let account_dir = context.account_dir.clone();
        cloud_upload::precheck_artifact(&account_dir)?;
        // 单槽重入闸：前置检查全过后、spawn 前抢占；watcher 收尾释放。
        if !cloud_upload::CloudUploadProgress::try_begin_running() {
            return Err(cloud_upload::CloudUploadError::AlreadyRunning);
        }
        self.cloud_upload_progress.begin_exporting();
        // 密钥派生与 zkgroup 材料（spawn 前构造：keys 随 params 移入装配线程，
        // materials 以值移交 watcher）。
        let aci = presage::libsignal_service::protocol::Aci::from(registration.service_ids.aci);
        let keys = crate::backup::derive_keys(&account_entropy_pool, &aci);
        let materials =
            cloud_backup::create_backup_auth_request_materials(&keys.backup_key, keys.aci.into());
        // mediaRootBackupKey：同 backup.export 口径——引擎无既定持久来源，每
        // 次随机 32B（契约差异登记，§10.5 同款）。
        use rand::RngCore as _;
        let mut media_root_backup_key = [0u8; 32];
        rand::rng().fill_bytes(&mut media_root_backup_key);
        let params = crate::backup::LocalBackupParams {
            keys,
            backup_time_ms: cloud_upload::now_ms(),
            media_root_backup_key,
            app_version: format!("kt-signal-engine {}", env!("CARGO_PKG_VERSION")),
        };
        let push = context.push.clone();
        let progress = self.cloud_upload_progress.clone();
        let temp = account_dir.join(cloud_upload::UPLOAD_TEMP_NAME);
        let artifact = account_dir.join(crate::backup::ARTIFACT_FILE_NAME);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let temp_for_assembly = temp.clone();
        let spawn = std::thread::Builder::new()
            .name("cloud-upload-assembly".into())
            .spawn(move || {
                let _ = tx.send(cloud_upload::run_assembly(
                    &artifact,
                    &temp_for_assembly,
                    &params,
                ));
            });
        if let Err(error) = spawn {
            progress.clear();
            cloud_upload::CloudUploadProgress::finish_running();
            return Err(cloud_upload::CloudUploadError::Io(format!(
                "assembly thread spawn: {error}"
            )));
        }
        tokio::task::spawn_local(cloud_upload::watch_and_upload(
            rx,
            push,
            signing_key,
            materials,
            temp,
            progress,
        ));
        Ok(())
    }

    /// 云备份 upload 进度/状态（`backups.uploadStatus` 只读数据源；快照缺席 =
    /// `{present:false}`）。
    pub(crate) fn cloud_upload_status(&self) -> Option<cloud_upload::CloudUploadSnapshot> {
        self.cloud_upload_progress.snapshot()
    }
}

/// storage 面共用上下文：已注册账号的 store + 手工构造凭据的 PushService
/// + 账号 store 目录（service/mod.rs link_flow 凭据构造先例同构）。
pub(crate) struct StorageContext {
    pub(crate) store: SqliteStore,
    pub(crate) push: PushService,
    pub(crate) device_id: u32,
    pub(crate) account_dir: PathBuf,
    /// 本机 ACI/PNI（UUID；群凭据 present 面，groups 模块消费，不入日志）。
    pub(crate) aci_uuid: presage::libsignal_service::prelude::Uuid,
    pub(crate) pni_uuid: presage::libsignal_service::prelude::Uuid,
    /// 群凭据双族缓存句柄（Service 共享，进程内存不落盘；P4c 切一）。
    pub(crate) group_credentials: Arc<call_links::GroupCredentialCache>,
}

/// 组装 storage 上下文：账号 store + 凭据（照抄 link_flow 先例）。
async fn storage_context(
    service: &Service,
    account: &str,
) -> Result<StorageContext, storage_sync::StorageSyncError> {
    let rel = {
        let state = service.state.lock().await;
        state
            .account_dir(account)
            .ok_or(storage_sync::StorageSyncError::UnknownAccount)?
    };
    let account_dir = service.data_dir.join(&rel);
    let store = open_store(&account_dir)
        .await
        .map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?;
    let data = store
        .load_registration_data()
        .await
        .map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?
        .ok_or(storage_sync::StorageSyncError::UnknownAccount)?;
    // password 为 pub(crate)：经 serde 数据面读回（ADR 0005 §S2 唯一外部
    // 构造路径，JSON 形态 = presage 官方 store 落库格式）。
    let raw =
        serde_json::to_value(&data).map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?;
    let password = raw["password"]
        .as_str()
        .ok_or(storage_sync::StorageSyncError::UnknownAccount)?
        .to_string();
    // phonenumber 字段要 E164（与 link_flow / presage credentials() 同构）。
    use presage::libsignal_service::utils::TryIntoE164;
    let push = PushService::new(
        SignalServers::Production,
        Some(ServiceCredentials {
            aci: Some(data.service_ids.aci),
            pni: Some(data.service_ids.pni),
            phonenumber: data
                .phone_number
                .try_into_e164()
                .map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?,
            password: Some(password),
            device_id: data.device_id.and_then(|id| id.try_into().ok()),
        }),
        USER_AGENT,
    );
    Ok(StorageContext {
        store,
        push,
        device_id: data.device_id.unwrap_or(1),
        account_dir,
        aci_uuid: data.service_ids.aci,
        pni_uuid: data.service_ids.pni,
        group_credentials: service.group_credentials.clone(),
    })
}

// ---------------------------------------------------------------------------
// 接收管线与投影
// ---------------------------------------------------------------------------

/// DecryptionError 结构化事件（止血批①：解密失败不再静默丢弃）。加面不改
/// 既有投影字段；形状 = receive 事件族同构（{account, envelope:{...}}），
/// 信封面带 sourceUuid 与（已知时）发送方 client timestamp。官方语义：
/// 解密失败也 ack + 记录（AndroidIncomingMessageObserver 落库后逐条 ack），
/// DERM 回发属 B 批不做。
fn decryption_error_notification(
    account: &str,
    sender: &ServiceId,
    timestamp: Option<u64>,
) -> Value {
    let envelope = match timestamp {
        Some(timestamp) => json!({
            "timestamp": timestamp,
            "sourceUuid": sender.service_id_string(),
            "decryptionError": {},
        }),
        None => json!({
            "sourceUuid": sender.service_id_string(),
            "decryptionError": {},
        }),
    };
    json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": { "account": account, "envelope": envelope },
    })
}

/// 身份钥变更结构化事件（B4 身份事件面，compat §5）：形状 = receive 事件
/// 族同构，envelope 级 `identityChanged` 标记（解密失败 decryptionError
/// 同款加面纪律）。信任判定不动（OnNewIdentity::Trust），事件仅为投影；
/// 触发该事件的本体消息照常随信封投影。
/// 排空通知（时序审计 G3，compat §3）：服务器历史队列排空（官方
/// ApiEmptyQueue→EmptyEvent 同位），`receive.emptied` 语义 per-account
/// additive 上抛；引擎只报态，消费方（连接器/desktop）自定后续。
fn queue_emptied_notification(account: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "receive.emptied",
        "params": { "account": account },
    })
}

fn identity_changed_notification(account: &str, sender: &ServiceId, timestamp: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": account,
            "envelope": {
                "sourceUuid": sender.service_id_string(),
                "timestamp": timestamp,
                "identityChanged": {},
            },
        },
    })
}

/// Unbound 停环事件（B4，compat §5）：服务端 401 凭据拒绝（vendored 归型
/// `ServiceError::Unauthorized`，401/403 同型）后接收循环停止，上抛一次
/// 本通知（wire additive，connector 白名单不消费零影响）——官方链接设备
/// 语义：凭据失效即停，等用户重链，不无限退避重连。
fn account_unbound_notification(account: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "accountUnbound",
        "params": { "account": account },
    })
}

/// receive 流建流错误归型（B4 Unbound 停环）：`ServiceError::Unauthorized`
/// → true（停环）；send_error_of 同款双层归型面（直接 ServiceError 与
/// sender 再包层），其余（网络/协议/存储）维持退避重连。
fn receive_stream_unbound(error: &presage::Error<<SqliteStore as Store>::Error>) -> bool {
    use presage::libsignal_service::prelude::ServiceError;
    use presage::libsignal_service::sender::MessageSenderError;
    match error {
        presage::Error::ServiceError(ServiceError::Unauthorized) => true,
        presage::Error::MessageSenderError(inner) => matches!(
            **inner,
            MessageSenderError::ServiceError(ServiceError::Unauthorized)
        ),
        _ => false,
    }
}

/// serve 流结局（B4 Unbound 停环）：Connected = 建流成功后结束（对端断/
/// 关停/流排空）→ 退避 reset；Recoverable = 建流失败（store/manager/
/// websocket 初始化）→ 退避步进；Unbound = 服务端凭据拒绝 → 停环等重链。
#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamOutcome {
    Connected,
    Recoverable,
    Unbound,
    /// 时序审计 G1：stdout 出口死亡（flush 栅栏不可解）。停环等连接器
    /// 重启引擎，不退避重连（重连 = 重投信封被 Duplicate 就地 ack 吞）。
    StdioDead,
}

// ---------------------------------------------------------------------------
// DERM 收发自愈（B 批，官方 Android MessageDecryptor / Desktop handleRetry
// 行为对齐）：解密失败回发 DecryptionErrorMessage；收到重试请求按原始
// 时间戳重发已发消息，找不到/超龄/超限 → clear_sessions 会话重建。
// ---------------------------------------------------------------------------

/// 同一 (来源, 原始时间戳) 重试请求放行上限（官方 Desktop handleRetry
/// RETRY_LIMIT = 5 同值）。
const DERM_RETRY_LIMIT: u32 = 5;
/// 重试请求新鲜度窗口（官方 Desktop retryRespondMaxAge 缺省 14 天同值）：
/// 原始消息超龄不再重发，转会话重建档。
const DERM_RESPOND_MAX_AGE_MS: u64 = 14 * 24 * 60 * 60 * 1_000;

/// 防环计数器（进程内存，接收管线生命周期；键 = 来源 ACI + 原始时间戳，
/// 与官方 getRetryKey 同形）。
#[derive(Default)]
struct DermRetryGuard {
    attempts: HashMap<(String, u64), u32>,
}

impl DermRetryGuard {
    /// 计入一次并判定是否放行（超限 false，官方 retryCount > RETRY_LIMIT
    /// early-return 同语义）。
    fn admit(&mut self, sender: &str, original_ts: u64) -> bool {
        let count = self
            .attempts
            .entry((sender.to_owned(), original_ts))
            .or_insert(0);
        *count = count.saturating_add(1);
        *count <= DERM_RETRY_LIMIT
    }
}

/// 新鲜度判定（纯函数）：原始消息不晚于当下且在窗口内。
fn derm_request_fresh(now_ms: u64, original_ts: u64) -> bool {
    now_ms >= original_ts && now_ms - original_ts <= DERM_RESPOND_MAX_AGE_MS
}

/// 重发资格判定（纯函数面）：直聊线程须恰指向请求者；群线程须请求者在
/// 成员名单内（官方 getSentProtoByRecipient 按 recipient 查同语义——
/// 非收件人请求重发一律转会话重建档，不向他方泄露群消息）。
fn derm_resend_allowed(
    thread: &presage::store::Thread,
    requester: &ServiceId,
    group_members: Option<&[presage::model::groups::Member]>,
) -> bool {
    match thread {
        presage::store::Thread::Contact(service_id) => service_id == requester,
        presage::store::Thread::Group(_) => group_members.is_some_and(|members| {
            members
                .iter()
                .any(|member| ServiceId::from(member.aci) == *requester)
        }),
    }
}

/// DERM 分发：两档失败面（我方解密失败 → 回发重试回执；对端重试请求 →
/// 重发/会话重建）。全链路 best-effort：任何失败静默，绝不影响原信封 ack。
async fn derm_handle(
    manager: &mut Manager<SqliteStore, Registered>,
    sender: ServiceId,
    failure: presage::model::messages::DecryptionFailure,
    guard: &mut DermRetryGuard,
) {
    use presage::model::messages::DecryptionFailure as Failure;
    match failure {
        Failure::RetryReceipt(bytes) => {
            derm_send_receipt(manager, sender, &bytes).await;
        }
        Failure::RetryRequest(original_ts) => {
            derm_on_retry_request(manager, sender, original_ts, guard).await;
        }
    }
}

/// 档一：把重试回执（DecryptionErrorMessage 序列化 proto）经发送通道回原
/// 发送者。回执发送失败静默（事件面已上抛，官方 requestResend 队列失败
/// 同样落会话重置兜底）。
async fn derm_send_receipt(
    manager: &mut Manager<SqliteStore, Registered>,
    sender: ServiceId,
    receipt: &[u8],
) {
    use prost::Message as _;
    let Ok(body) = presage::proto::DecryptionErrorMessage::decode(receipt) else {
        return;
    };
    let _ = manager
        .send_message(sender, ContentBody::DecryptionErrorMessage(body), now_ms())
        .await;
}

/// 档二：对端重试请求（DERM.RetryRequest）。闸序：防环计数 → 新鲜度 →
/// 已发消息找回（含群成员资格）；可重发则重发原始内容，否则
/// clear_sessions（会话重建在下一轮收发自然发生）。
async fn derm_on_retry_request(
    manager: &mut Manager<SqliteStore, Registered>,
    requester: ServiceId,
    original_ts: u64,
    guard: &mut DermRetryGuard,
) {
    if !guard.admit(&requester.service_id_string(), original_ts)
        || !derm_request_fresh(now_ms(), original_ts)
    {
        let _ = manager.clear_sessions(&requester).await;
        return;
    }
    let lookup = manager
        .store()
        .sent_message_by_timestamp(&requester, original_ts)
        .await
        .ok()
        .flatten();
    let Some((thread, content)) = lookup else {
        let _ = manager.clear_sessions(&requester).await;
        return;
    };
    let group_members = match &thread {
        presage::store::Thread::Group(master_key) => manager
            .store()
            .group(*master_key)
            .await
            .ok()
            .flatten()
            .map(|group| group.members),
        presage::store::Thread::Contact(_) => None,
    };
    if !derm_resend_allowed(&thread, &requester, group_members.as_deref()) {
        let _ = manager.clear_sessions(&requester).await;
        return;
    }
    // 重发原始内容（原始时间戳保序；本地 store INSERT OR REPLACE 同行
    // 幂等）。resend 失败静默（请求方下轮重试或会话重建档兜底）。
    let _ = manager
        .send_message(requester, content.body, original_ts)
        .await;
}

async fn receive_pipeline(
    service: &Arc<Service>,
    number: String,
    dir: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) {
    // 存量联系人补同步的一次性纪律：60s 内断连重开不重复轰主设备。
    let mut last_contacts_sync: Option<std::time::Instant> = None;
    // DERM 重试防环计数（同管线生命周期；断连重开不清——防环窗口以
    // 原始消息时间戳为键，跨重连持续有效）。
    let mut derm_guard = DermRetryGuard::default();
    // D1 自动 delivery receipt 批量缓冲（管线生命周期，跨流重启存活：
    // 未发批次等下一条流的批量窗续发，不随重连丢失）。
    let mut receipts = delivery_receipt::AutoDeliveryReceipts::default();
    // D5 设备面能力注册：每管线运行成功一次即止（官方 afterEveryLinked
    // Startup 每次链接启动同位，不随重连重发）；失败随建流重试。
    let mut capabilities_registered = false;
    // 重连退避（官方 Desktop ts/backoff/BackOff.std 同语义）：Fibonacci
    // 1→55s + ±5s jitter，连接成功 reset 从 1s 重来。流失败路径
    // （open store/manager/websocket 失败）按步进退避，连接成功清零。
    let mut backoff_step: u32 = 0;
    // 发送证书后台预轮换任务（止血批③）：与接收循环同生命周期（shutdown
    // 即止），周期检查 store 持久证书临期并刷新，发送路径只读缓存。
    let mut cert_shutdown = shutdown.clone();
    let cert_dir = dir.clone();
    tokio::spawn(async move {
        let mut maintenance = CertificateMaintenance::new();
        loop {
            tokio::select! {
                _ = cert_shutdown.changed() => return,
                _ = tokio::time::sleep(maintenance.delay()) => {
                    certificate_maintenance_tick(&cert_dir, &mut maintenance).await;
                }
            }
        }
    });
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut serve_rx = shutdown.clone();
        let outcome = tokio::select! {
            _ = shutdown.changed() => return,
            outcome = serve_account_stream(
                service,
                &number,
                &dir,
                &mut serve_rx,
                &mut last_contacts_sync,
                &mut derm_guard,
                &mut receipts,
                &mut capabilities_registered,
            ) => outcome,
        };
        // Unbound（服务端 401 凭据拒绝）：官方链接设备语义——停环等重链，
        // 不退避重连；结构化上抛一次（wire additive，compat §5）。
        if outcome == StreamOutcome::Unbound {
            service.notify(account_unbound_notification(&number));
            return;
        }
        // StdioDead（时序审计 G1）：stdout 已死（连接器侧断开/半开管道），
        // 投影永不可达。停管线不退避——重连只会把重投信封喂进 Duplicate
        // 就地 ack 吞掉；信封留服务器队列，连接器重启引擎后全量重放补投影。
        if outcome == StreamOutcome::StdioDead {
            return;
        }
        // 流断开：退避后重开（重连治理 P2 精化：官方 BackOff.std 同款）。
        backoff_step = if outcome == StreamOutcome::Connected {
            0
        } else {
            backoff_step.saturating_add(1)
        };
        let delay = reconnect_jittered_delay(reconnect_backoff_delay(backoff_step));
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Fibonacci 退避基值（官方 Desktop BackOff.std.ts 同序列）：1,1,2,3,5,8,
/// 13,21,34,55，封顶 55s。step=0 → 1s。
fn reconnect_backoff_delay(step: u32) -> Duration {
    const STEP_MS: u64 = 1_000;
    const CAP_MS: u64 = 55_000;
    let (mut a, mut b) = (0u64, STEP_MS);
    for _ in 0..step {
        let next = (a + b).min(CAP_MS);
        a = b;
        b = next;
    }
    Duration::from_millis(b.min(CAP_MS))
}

/// 退避 jitter（官方 BackOff.get 同语义）：`jitter < result` 才叠加
/// `random()*jitter` 且单向正值——短拍（≤5s 档，含首拍 1s 与 5s 档）
/// 恒精确不被 jitter 拖慢，长拍单向上浮消重连峰。
fn reconnect_jittered_delay(base: Duration) -> Duration {
    const JITTER_MS: u64 = 5_000;
    if base.as_millis() <= JITTER_MS as u128 {
        return base;
    }
    let jitter: u64 = rng().random_range(0..JITTER_MS);
    base + Duration::from_millis(jitter)
}

/// 发送证书后台预轮换（止血批③，官方 Desktop 1h 轮换缓冲同语义）：周期
/// （10min）检查 store 持久证书临期情况，1h 内到期/缺失/不可解析才触发
/// 刷新（presage 内部按 needs_renewal 续期并落库）；失败退避重试
/// （1→16min，封顶轮询间隔）。发送路径只读缓存：刷新失败由发送侧
/// identified 保险丝兜底（vendor presage unidentified_access_or_identified）。
/// 刷新走 presage 既有发送路径同款短命 identified websocket（随取随弃，
/// 与现有每发送续期同形态，不新增常驻第二连接）。
const CERT_CHECK_INTERVAL: Duration = Duration::from_secs(600);
const CERT_FAILURE_BACKOFF: Duration = Duration::from_secs(60);
/// 提前量 1h（官方 Desktop 证书轮换缓冲同值）。
const CERT_REFRESH_BUFFER_MS: u64 = 60 * 60 * 1_000;

struct CertificateMaintenance {
    failures: u32,
}

impl CertificateMaintenance {
    fn new() -> Self {
        Self { failures: 0 }
    }

    /// 正常 = 轮询间隔；失败后 1,2,4,8,16min 指数退避，封顶轮询间隔。
    fn delay(&self) -> Duration {
        if self.failures == 0 {
            return CERT_CHECK_INTERVAL;
        }
        CERT_FAILURE_BACKOFF
            .saturating_mul(1u32 << (self.failures - 1).min(4))
            .min(CERT_CHECK_INTERVAL)
    }
}

/// 单轮检查：证书健康即不动（清退避）；不健康 → 刷新。零 stderr（接收
/// 面纪律），失败只记退避态，下一轮自愈。
async fn certificate_maintenance_tick(dir: &Path, maintenance: &mut CertificateMaintenance) {
    let Ok(store) = open_store(dir).await else {
        maintenance.failures += 1;
        return;
    };
    let needs_refresh = match store.sender_certificate().await {
        Ok(Some(cert)) => match cert.expiration() {
            Ok(expiration) => expiration.epoch_millis() <= now_ms() + CERT_REFRESH_BUFFER_MS,
            Err(_) => true,
        },
        Ok(None) => true,
        Err(_) => true,
    };
    if !needs_refresh {
        maintenance.failures = 0;
        return;
    }
    if let Ok(mut manager) = Manager::load_registered(store).await {
        if manager.refresh_sender_certificate().await.is_ok() {
            maintenance.failures = 0;
            return;
        }
    }
    maintenance.failures += 1;
}

/// 建流失败返回 Recoverable（退避步进）；流建立后结束（对端断/关停/流
/// 排空）返回 Connected（连接成功 → 退避 reset，官方 BackOff.reset 同
/// 语义）；建流遇服务端 401 凭据拒绝（vendored
/// `ServiceError::Unauthorized`）返回 Unbound（停环等重链，B4）。
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn serve_account_stream(
    service: &Arc<Service>,
    number: &str,
    dir: &Path,
    shutdown: &mut watch::Receiver<bool>,
    last_contacts_sync: &mut Option<std::time::Instant>,
    derm_guard: &mut DermRetryGuard,
    receipts: &mut delivery_receipt::AutoDeliveryReceipts,
    capabilities_registered: &mut bool,
) -> StreamOutcome {
    let Ok(store) = open_store(dir).await else {
        return StreamOutcome::Recoverable;
    };
    let Ok(mut manager) = Manager::load_registered(store).await else {
        return StreamOutcome::Recoverable;
    };
    // D5 设备面能力注册（官方 afterEveryLinkedStartup → registerCapabilities
    // 同位）：best-effort，失败静默（官方 catch+log 对位，引擎零 stderr 纪律）
    // 下一轮建流重试；成功置位，重连不再重发。
    if !*capabilities_registered
        && manager
            .register_device_capabilities(capabilities::DEVICE_CAPABILITIES_UPLOAD)
            .await
            .is_ok()
    {
        *capabilities_registered = true;
    }
    let mut stream = match manager.receive_messages().await {
        Ok(stream) => stream,
        Err(error) if receive_stream_unbound(&error) => return StreamOutcome::Unbound,
        Err(_) => return StreamOutcome::Recoverable,
    };
    // 存量联系人/会话补同步（欠账 §10-1，官方行为对齐：signal-cli/桌面
    // 链接后同样向主设备要一轮 contacts sync）：链路建立 2s 沉降后
    // request_contacts（presage 官方 API，SyncMessage.request=contacts），
    // 主设备回传经接收流落 contacts 表 → connector listContacts 出骨架。
    // warn-only（本层零 stderr，失败静默）；每账号 60s 一次性，watchdog
    // 断连重开不重复。群组不在本面（presage 栈无 groups sync 消费路径，
    // 首条群消息渐进落库=官方链接设备同款）。
    let mut sync_pending = match *last_contacts_sync {
        Some(at) if at.elapsed() < Duration::from_secs(60) => None,
        _ => Some(tokio::time::Instant::now() + Duration::from_secs(2)),
    };
    // prekey 定时维护节律（B 批，2 天水位巡检 + 按龄轮换）：下一拍 = 书签
    // +2 天（书签在 store kv，跨重连/重启有效；首拍即到期立即巡检）。
    // tick 内部自读书签判到期，重连重开不重复轰服务器。
    let mut maint_at = Some(tokio::time::Instant::now() + Duration::from_millis(0));
    // prekey 快速巡检（P12/R4）：30min 时间触发 + 200 信封量触发的低水位
    // 快速路径——长连接内 prekey 耗尽的发现窗从 2 天（maintenance_tick）
    // 收敛到 30min/200 信封。refresh_pre_keys 自带服务器计数闸（≥10 零上
    // 传），触发频繁也只多一次 GET /v2/keys；本地面重连即重置（水位权威
    // 在服务器计数，短重连风暴靠闸挡）。
    let mut quick_at = Some(
        tokio::time::Instant::now() + Duration::from_millis(prekey::PREKEY_QUICK_CHECK_INTERVAL_MS),
    );
    // 毫秒书签与 Instant 计时器并行：拍到时 quick_check_due 二次裁定
    // （maintenance_tick 书签判到期同纪律），量触发刷新书签让时间窗重开。
    let mut quick_last_ms: Option<u64> = None;
    let mut quick_seen: u64 = 0;
    loop {
        tokio::select! {
            _ = shutdown.changed() => return StreamOutcome::Connected,
            _ = async {
                if let Some(at) = sync_pending {
                    tokio::time::sleep_until(at).await;
                    // 官方 request_contacts：成功/失败都不重试（一次性纪律，
                    // 下次链路建立再补）。
                    let _ = manager.request_contacts().await;
                    *last_contacts_sync = Some(std::time::Instant::now());
                }
            }, if sync_pending.is_some() => {
                sync_pending = None;
            }
            _ = async {
                if let Some(at) = maint_at {
                    tokio::time::sleep_until(at).await;
                }
            }, if maint_at.is_some() => {
                // 下一拍固定 +2 天（tick 内书签判到期，短拍只做本地判定）。
                // manager 调用放臂体：select 其他 future 已弃，借用不冲突。
                prekey::maintenance_tick(&mut manager).await;
                maint_at = Some(
                    tokio::time::Instant::now()
                        + Duration::from_millis(prekey::MAINTENANCE_INTERVAL_MS),
                );
            }
            // prekey 快速巡检时间臂（P12/R4）：30min 一拍，与量触发共用
            // quick_check 面；拍后计时器重置。
            _ = async {
                if let Some(at) = quick_at {
                    tokio::time::sleep_until(at).await;
                }
            }, if quick_at.is_some() => {
                let now = prekey::now_ms();
                if prekey::quick_check_due(quick_last_ms, now) {
                    prekey::quick_check(&mut manager).await;
                }
                quick_last_ms = Some(now);
                quick_seen = 0;
                quick_at = Some(
                    tokio::time::Instant::now()
                        + Duration::from_millis(prekey::PREKEY_QUICK_CHECK_INTERVAL_MS),
                );
            }
            // D1 自动 delivery receipt 批量窗：到期即取批直发（官方
            // deliveryReceiptBatcher wait=500ms 语义）。发送 spawn_local
            // 承载，臂内即刻返回不阻塞收流。
            _ = async {
                if let Some(at) = receipts.due_at() {
                    tokio::time::sleep_until(at).await;
                }
            }, if receipts.due_at().is_some() => {
                if let Some(batch) = receipts.take_ready() {
                    delivery_receipt::spawn_delivery_receipt_flush(service, number, batch);
                }
            }
            item = stream.next() => {
                match item {
                // QueueEmpty：历史队列排空，流继续保持（queue 标记在
                // messagepipe 层已就地 ack）。G3：排空态 additive 上抛
                // （官方 ApiEmptyQueue→EmptyEvent 同位；引擎只报态，连接
                // 器/desktop 消费自定）。
                Some(Received::QueueEmpty) => {
                    service.notify(queue_emptied_notification(number));
                }
                // contacts 由 presage 自动入库（号码/名字面随之可用）。P15a
                // 项3：解析后的联系人记录随 Received::Contacts 上抛（vendored
                // ContactSync 载体，fork 登记 vendor/README.md），投影 receive
                // 事件供 connector upsert 自有库（官方 contactSync 消费同位）。
                // G1：ack 前等 flush 栅栏（投影可达连接器才耗服务器队列）。
                Some(Received::Contacts(sync, ack)) => {
                    quick_seen += 1;
                    for notification in project_contacts_sync(number, &sync) {
                        service.notify(notification);
                    }
                    if !service.flush_barrier().await {
                        return StreamOutcome::StdioDead;
                    }
                    ack.ack();
                }
                // 解密失败：官方语义同样是 ack（Android IncomingMessageObserver
                // 落库事务后逐条 ack，解密失败也 ack），否则服务器队列堆积；
                // 同时结构化上抛 connector（不再静默丢弃）。DERM 自愈两档
                // （官方 Android MessageDecryptor / Desktop handleRetry 同型）：
                // 我方解密失败 → 回发 DecryptionErrorMessage；收到对端重试
                // 请求 → 按原始时间戳找回已发消息重发，找不到/超龄/超限 →
                // clear_sessions 触发会话重建。全链路失败静默，绝不影响 ack。
                Some(Received::DecryptionError(sender, timestamp, failure, ack)) => {
                    service.notify(decryption_error_notification(number, &sender, timestamp));
                    if !service.flush_barrier().await {
                        return StreamOutcome::StdioDead;
                    }
                    if let Some(failure) = failure {
                        derm_handle(&mut manager, sender, failure, derm_guard).await;
                    }
                    quick_seen += 1;
                    ack.ack();
                }
                // 重复投递（store 已有同源行）：已投影过，仅补 ack 不再通知。
                Some(Received::Duplicate(ack)) => {
                    quick_seen += 1;
                    ack.ack();
                }
                // 身份钥变更（B4 身份事件面，compat §5）：解密时 store 侧
                // identity 被替换（会话按新钥重建，OnNewIdentity::Trust
                // Receiving 纪律不动——收向照常换钥上事件）。结构化上抛
                // connector；同信封本体解密成功，照常走投影面，单次 ack。
                // P9 身份拒发批：sidecar `pending` 落标（换钥未确认 → 发送
                // 链拒发）+ verified 态随换钥降级（官方 saveIdentity 换钥
                // VERIFIED/UNVERIFIED → UNVERIFIED 同位）；写失败静默
                // fail-open（闸缺位不阻断主收发，compat §5 如实登记）。
                // 确认钩子 = trust 写面（verified / state:"default" 清标）。
                Some(Received::IdentityChange(sender, content, ack)) => {
                    let sid_string = sender.service_id_string();
                    let _ = identity::mark_identity_replaced(dir, &sid_string, now_ms());
                    service.notify(identity_changed_notification(
                        number,
                        &sender,
                        content.metadata.client_timestamp.timestamp_millis().max(0) as u64,
                    ));
                    for notification in
                        project_content(
                            number,
                            manager.store(),
                            &content,
                            &service.terminated_groups,
                        )
                        .await
                    {
                        service.notify(notification);
                    }
                    // G1：ack 前等 flush 栅栏（同 Content 臂）。
                    if !service.flush_barrier().await {
                        return StreamOutcome::StdioDead;
                    }
                    quick_seen += 1;
                    ack.ack();
                }
                Some(Received::Content(content, ack)) => {
                    // B4 身份事件面：verified 同步下行（多设备信任态收敛）
                    // ——sidecar 权威突变 + receive 事件投影，先于普通投影
                    // 面（该体在 project_content 恒空投影，零冲突）。
                    if let Some(notification) =
                        project_verified_sync(number, manager.store(), &content, dir).await
                    {
                        service.notify(notification);
                    }
                    // 官方契约：解密+落库+投影完成后才 ack（Android
                    // IncomingMessageObserver.kt 事务提交后逐条 ack；
                    // SignalWebSocket.kt「处理成功后由你 ack」）。两条路
                    // （投影成功/失败）都必达 ack，防服务器队列堆积。
                    // G1 时序收口：ack 再晚于「投影已写进 stdout 并 flush」
                    // ——此前投影 fire-and-forget 入无界 mpsc 即 ack，引擎在
                    // presage 已落库、连接器未收到的窗口内死亡（watchdog/
                    // 链式重启/OOM）时，服务器队列已耗、连接器永不收到，且
                    // 重启重放被 (sender, client_timestamp) 重放闸吞成
                    // Duplicate——消息永久丢失。栅栏把 ack 顶到「连接器至少
                    // 收到字节」之后（engine 内闭环，天然背压上收）；栅栏
                    // 不可解（出口死）→ 不 ack + 停管线，信封留服务器队列
                    // 待重启重放。
                    for notification in
                        project_content(
                            number,
                            manager.store(),
                            &content,
                            &service.terminated_groups,
                        )
                        .await
                    {
                        service.notify(notification);
                    }
                    if !service.flush_barrier().await {
                        return StreamOutcome::StdioDead;
                    }
                    // D1 自动 delivery receipt 排队闸（官方 handleDataMessage
                    // / handleEditMessage 条件面，见 delivery_receipt 模块注）。
                    // IdentityChange 臂不排队（官方 hasErrors 同位）；批满
                    // 100 即发不等窗口。
                    if let Some((sender, timestamp)) = delivery_receipt::candidate_of(&content) {
                        receipts.queue(sender, timestamp);
                        if let Some(batch) = receipts.take_ready() {
                            delivery_receipt::spawn_delivery_receipt_flush(
                                service, number, batch,
                            );
                        }
                    }
                    quick_seen += 1;
                    ack.ack();
                }
                    None => return StreamOutcome::Connected,
                }
                // prekey 快速巡检量臂（P12/R4）：窗内 ≥200 信封即快速巡检
                // 一次，计数与时间臂计时器同复位（含解密失败/重复投递——
                // 服务器侧取信封同样消耗一次性 prekey）。
                if prekey::quick_check_volume_due(std::mem::replace(&mut quick_seen, 0)) {
                    prekey::quick_check(&mut manager).await;
                    quick_last_ms = Some(prekey::now_ms());
                    quick_at = Some(
                        tokio::time::Instant::now()
                            + Duration::from_millis(prekey::PREKEY_QUICK_CHECK_INTERVAL_MS),
                    );
                }
            }
        }
    }
}

/// verified 同步下行投影（B4 身份事件面，compat §5）：多设备信任态收敛——
/// 主设备 verify/clear 后其他链接设备同步收口。sidecar 写为权威突变
/// （identity::apply_synced_trust，比对闸已由发起设备完成，收侧直接落态）+
/// receive 事件投影（`syncMessage.verified`，形状镜像自发面
/// {destinationUuid, destinationNumber?, identityKey?, state}，state = trust
/// 词表小写）。非 verified 体返回 None 零打扰；destination/state 缺席或
/// 归型不出（UNKNOWN/TRANSLATED）整信封跳过不落不投（宁缺勿错）。
async fn project_verified_sync(
    account: &str,
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
    account_dir: &Path,
) -> Option<Value> {
    let presage::libsignal_service::content::ContentBody::SynchronizeMessage(sync) = &content.body
    else {
        return None;
    };
    let Some(presage::proto::sync_message::Content::Verified(verified)) = sync.content.as_ref()
    else {
        return None;
    };
    let state = identity::TrustState::from_sync_state(verified.state)?;
    let destination = verified.parse_destination_aci()?;
    let sid = presage::libsignal_service::protocol::ServiceId::Aci(destination);
    let sid_string = sid.service_id_string();
    let timestamp = content.metadata.client_timestamp.timestamp_millis().max(0) as u64;
    identity::apply_synced_trust(account_dir, &sid_string, state, timestamp);
    let mut verified_json = json!({
        "destinationUuid": sid_string,
        "state": state.word(),
    });
    if let Some(peer) = describe_peer(store, &sid).await {
        if let Some(number) = peer.number {
            verified_json["destinationNumber"] = json!(number);
        }
    }
    if let Some(key) = verified.identity_key.as_ref() {
        verified_json["identityKey"] = json!(crate::service::hex_encode(key));
    }
    Some(json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": account,
            "envelope": {
                "sourceUuid": content.metadata.sender.service_id_string(),
                "sourceDevice": u32::from(content.metadata.sender_device),
                "timestamp": timestamp,
                "syncMessage": { "verified": verified_json },
            },
        },
    }))
}

/// P2-A 投影边界（compat §3 receive 段）：直聊 dataMessage 全消费面
/// （正文/引用/reaction/remoteDelete/附件元数据/链接预览/mentions/viewOnce）、
/// editMessage、typingMessage、receiptMessage、本机多设备 sent-sync。
/// 群组接收投影：dataMessage.groupV2 / sent 内群组投影 signal-cli 同款
/// `groupInfo{groupId}`（connector 群会话管线既有形状）；群更新类消息
/// （groupContext/groupChange 非空）v1 仍整体跳过（成员变更/权限面后续阶段）。
/// 通话消息面（2026-10-06 扩展批）：CallMessage 1:1 信令透投影 +
/// SyncMessage.CallEvent 通话记录同步投影；story/null 体跳过。
/// messageRequestResponse 批（2026-10-06）：SyncMessage.messageRequest
/// Response 收包终态投影（`syncMessage.messageRequestResponse`，多设备
/// echo 的接受/拒绝位，connector §4.58 推导的引擎侧真数据面，详见函数内
/// 与 compat §5）。
/// profileKeyUpdate 批（2026-10-06）：DataMessage.profileKey 按发送方 ACI
/// 落库 + contactProfileUpdated 事件（详见函数内与 compat §5）。
/// 形状钉定 signal-cli 0.14.8 JSON（connector implementation-plan §4.13/
/// §4.19/§4.20），作者号字段在 contacts 命中时给 E.164，未命中给 ACI 字符串。
/// 返回通知向量（可空：群终止/群更新事件可能替换/追加普通投影；调用方
/// 逐条外发）。
async fn project_content(
    account: &str,
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
    terminated_groups: &groups::TerminatedGroups,
) -> Vec<Value> {
    use presage::libsignal_service::content::ContentBody;

    let envelope_ts = content.metadata.client_timestamp.timestamp_millis().max(0) as u64;
    let mut envelope = json!({
        "sourceUuid": content.metadata.sender.service_id_string(),
        "sourceDevice": u32::from(content.metadata.sender_device),
        "timestamp": envelope_ts,
    });
    // 举报面（reportSpam 批）：server_guid = 信封公共位（服务器为该条消息
    // 分配的 UUID，举报 API 的路径参数；proto 只投影不发明，缺席键不在）。
    if let Some(server_guid) = content.metadata.server_guid.as_ref() {
        envelope["serverGuid"] = json!(server_guid.to_string());
    }
    // P15a 项2（compat §5）：Envelope.ephemeral(12)/urgent(14)/
    // report_spam_token(17) 经 vendored Metadata 扩位捕获（story bit 同型
    // fork），官方事件面同位（MessageReceiver.preload.ts:1177-1180 urgent
    // 原样透传、reportingToken Bytes.isNotEmpty 闸、键名沿官方事件面——
    // proto 字段 reportSpamToken，组5 adminKey 先例同款）。两标志 raw 位
    // 投影（在场即投真值）：urgent wire 缺席 = proto 缺省 true、ephemeral
    // 缺席 = false，缺席键不在（旧宿主零变化，§3 记档）。官方桌面对
    // ephemeral 零消费（P14 组7 核对结论），按任务口径如实投位。
    if let Some(urgent) = content.metadata.urgent {
        envelope["urgent"] = json!(urgent);
    }
    if let Some(ephemeral) = content.metadata.ephemeral {
        envelope["ephemeral"] = json!(ephemeral);
    }
    if let Some(token) = content
        .metadata
        .report_spam_token
        .as_deref()
        .filter(|token| !token.is_empty())
    {
        use base64::Engine as _;
        envelope["reportingToken"] = json!(base64::engine::general_purpose::STANDARD.encode(token));
    }
    if let Some(peer) = describe_peer(store, &content.metadata.sender).await {
        if let Some(number) = peer.number {
            envelope["sourceNumber"] = json!(number);
        }
        if let Some(name) = peer.name {
            envelope["sourceName"] = json!(name);
        }
    }

    // Stories 接收批（E2）官方闸（MessageReceiver.preload.ts:1524-1544 行为
    // 对齐，引擎零 stderr 同款只留判面）：信封 story 位在场，但体既不是
    // story（StoryMessage）、也不是群 story 回复（dataMessage 带
    // storyContext ∧ groupV2）、也不是 remote delete → 整信封丢弃。反向
    // 违例（体是 story/群回复但信封无位）官方仅 warn 照续——引擎零 stderr
    // 纪律下照投，isStory 位如实反映信封位（不发明）。
    if content.metadata.story == Some(true) {
        let (is_story, is_group_story_reply, is_delete_for_everyone) = match &content.body {
            ContentBody::StoryMessage(_) => (true, false, false),
            ContentBody::DataMessage(data_message) => (
                false,
                data_message.story_context.is_some() && data_message.group_v2.is_some(),
                data_message.delete.is_some(),
            ),
            _ => (false, false, false),
        };
        if !is_story && !is_group_story_reply && !is_delete_for_everyone {
            return Vec::new();
        }
        envelope["isStory"] = json!(true);
    }

    // profileKeyUpdate 批（compat §5）：DataMessage.profileKey = 发送方随行
    // 的当前 profile key（改资料/首聊采集两形态）。官方行为：按 sender ACI
    // 存储后再决定消息面走向；key 与已存相同 = 无事件（幂等闸，官方
    // hasChanged 门同构）。
    let profile_key_update = receive_profile_key(store, content).await;

    match &content.body {
        ContentBody::DataMessage(data_message) => {
            // PROFILE_KEY_UPDATE=4 在场 = 该行是资料键更新行（官方桌面收侧
            // 对该行清 body/attachments、只走 profile key 事件面）→ 整信封
            // 替换为 contactProfileUpdated 事件（key 已在上面落库）。
            if is_profile_key_update_row(data_message) {
                if let Some(aci) = profile_key_update {
                    return vec![contact_profile_updated_notification(
                        account,
                        envelope_ts,
                        &content.metadata.sender,
                        &aci,
                    )];
                }
                // key 面（缺席/非法长度/存储失败）没落地 → 不发明事件也不
                // 投影更新行（宁缺勿错，群终止投影同口径）。
                return Vec::new();
            }
            let numbers = resolve_author_numbers(store, data_message).await;
            envelope["dataMessage"] = project_data_message(data_message, &numbers);
        }
        ContentBody::EditMessage(edit) => {
            let Some(data_message) = edit.data_message.as_ref() else {
                return Vec::new();
            };
            let numbers = resolve_author_numbers(store, data_message).await;
            let dm = project_data_message(data_message, &numbers);
            envelope["editMessage"] = json!({
                "targetSentTimestamp": edit.target_sent_timestamp.unwrap_or(0),
                "dataMessage": dm,
            });
        }
        ContentBody::TypingMessage(typing) => {
            let action = match typing.action {
                Some(value) if value == presage::proto::typing_message::Action::Stopped as i32 => {
                    "STOPPED"
                }
                _ => "STARTED",
            };
            let mut typing_json = json!({
                "action": action,
                "timestamp": typing.timestamp.unwrap_or(envelope_ts),
            });
            // P14 收投影缺：TypingMessage.groupId=3（群 typing 面位——官方
            // desktop TypingMessage 与 1:1 同信封分发，connector 群会话
            // typing 归路靠它；1:1 缺席键不在，旧宿主零变化）。
            if let Some(group_id) = typing.group_id.as_deref().filter(|id| !id.is_empty()) {
                use base64::Engine as _;
                typing_json["groupId"] =
                    json!(base64::engine::general_purpose::STANDARD.encode(group_id));
            }
            envelope["typingMessage"] = typing_json;
        }
        ContentBody::ReceiptMessage(receipt) => {
            let kind = receipt.r#type.unwrap_or_default();
            let is_delivery = kind == presage::proto::receipt_message::Type::Delivery as i32;
            let is_read = kind == presage::proto::receipt_message::Type::Read as i32;
            let is_viewed = kind == presage::proto::receipt_message::Type::Viewed as i32;
            envelope["receiptMessage"] = json!({
                // signal-cli JsonReceiptMessage 同款：timestamp 为命中的消息
                // upstream 时间戳数组（connector 据此升级对应行为已送达/已读）。
                "timestamp": receipt.timestamp,
                "when": envelope_ts,
                "isDelivery": is_delivery,
                "isRead": is_read,
                "isViewed": is_viewed,
            });
        }
        ContentBody::SynchronizeMessage(sync) => {
            // sent/viewOnceOpen 在 SyncMessage.content oneof 里（read/viewed
            // 是直接 repeated 字段，兜底臂承接，D2）。
            match sync.content.as_ref() {
                Some(presage::proto::sync_message::Content::Sent(sent)) => {
                    // 公共位（timestamp/expirationStart/destination）三臂共用；
                    // 面臂按 message → editMessage → storyMessage 序取第一个
                    // 在场位（官方 sent sync 单面发送，多面并存在线面上不出现）；
                    // 全缺 = 空转录整信封丢弃（宁缺勿错）。
                    let mut sent_json = serde_json::Map::new();
                    sent_json.insert(
                        "timestamp".into(),
                        json!(sent.timestamp.unwrap_or(envelope_ts)),
                    );
                    // 消失消息 E 批：Sent.expirationStartTimestamp=4（官方多
                    // 设备的精确起算同步位，官方 1:1 已读起算语义的线面对应）。
                    // 0 无起算语义不投（同 history 面 0 过滤口径）；收侧
                    // DataMessage 在钉定 rev 无同名字段（proto 缺口，§5）。
                    if let Some(start) = sent_expiration_start(sent) {
                        sent_json.insert("expirationStartTimestamp".into(), json!(start));
                    }
                    match (
                        sent.destination_e164.as_deref(),
                        sent.parse_destination_service_id(),
                    ) {
                        (Some(e164), _) => {
                            sent_json.insert("destinationNumber".into(), json!(e164));
                        }
                        (None, Some(service_id)) => {
                            sent_json.insert(
                                "destinationUuid".into(),
                                json!(service_id.service_id_string()),
                            );
                        }
                        (None, None) => {}
                    }
                    // P14 收投影缺：Sent 三位可选面（isRecipientUpdate=6/
                    // unidentifiedStatus=5/storyMessageRecipients=9，官方桌面
                    // SentEvent 同名直投：MessageReceiver.preload.ts:2097-2101
                    // 解构同位）。proto 只投影不发明，缺席/空批键不在。
                    if sent.is_recipient_update == Some(true) {
                        sent_json.insert("isRecipientUpdate".into(), json!(true));
                    }
                    if !sent.unidentified_status.is_empty() {
                        sent_json.insert(
                            "unidentifiedStatus".into(),
                            Value::Array(
                                sent.unidentified_status
                                    .iter()
                                    .map(sent_unidentified_status_json)
                                    .collect(),
                            ),
                        );
                    }
                    if !sent.story_message_recipients.is_empty() {
                        sent_json.insert(
                            "storyMessageRecipients".into(),
                            Value::Array(
                                sent.story_message_recipients
                                    .iter()
                                    .map(sent_story_message_recipient_json)
                                    .collect(),
                            ),
                        );
                    }
                    if let Some(data_message) = sent.message.as_ref() {
                        let numbers = resolve_author_numbers(store, data_message).await;
                        match project_data_message(data_message, &numbers) {
                            Value::Object(map) => sent_json.extend(map),
                            _ => unreachable!("dataMessage projection is an object"),
                        }
                        envelope["syncMessage"] =
                            json!({ "sentMessage": Value::Object(sent_json) });
                    } else if let Some(edit) = sent.edit_message.as_ref() {
                        // Q1：自发 echo 的 editMessage 投影（Sent.editMessage=10，
                        // 多设备编辑同步——此前被整臂丢弃）。形状镜像 proto
                        // Sent：sentMessage.editMessage{targetSentTimestamp,
                        // dataMessage}，公共位照旧。
                        let Some(data_message) = edit.data_message.as_ref() else {
                            return Vec::new();
                        };
                        let numbers = resolve_author_numbers(store, data_message).await;
                        sent_json.insert(
                            "editMessage".into(),
                            json!({
                                "targetSentTimestamp": edit.target_sent_timestamp.unwrap_or(0),
                                "dataMessage": project_data_message(data_message, &numbers),
                            }),
                        );
                        envelope["syncMessage"] =
                            json!({ "sentMessage": Value::Object(sent_json) });
                    } else if let Some(story) = sent.story_message.as_ref() {
                        // Q6③：sent.storyMessage 投影（Sent.storyMessage=8，
                        // story 发送的多设备转录面——此前被整臂丢弃）。复用
                        // story 投影单点；无 attachment 位 = malformed 整信封
                        // 丢弃纪律照旧。
                        let numbers = resolve_story_mention_numbers(store, story).await;
                        let Some(projection) = project_story_message(story, &numbers) else {
                            return Vec::new();
                        };
                        sent_json.insert("storyMessage".into(), projection);
                        envelope["syncMessage"] =
                            json!({ "sentMessage": Value::Object(sent_json) });
                    } else {
                        return Vec::new();
                    }
                }
                // E3：viewOnceOpen 投影（官方 content oneof viewOnceOpen=11）
                // ——本机主设备/其他设备已打开某阅后即焚消息，上层据此把对应
                // 行置 Viewed 并执行字节焚毁。
                Some(presage::proto::sync_message::Content::ViewOnceOpen(open)) => {
                    let Some(projection) = project_view_once_open(open) else {
                        return Vec::new();
                    };
                    envelope["syncMessage"] = json!({ "viewOnceOpen": projection });
                }
                // 通话消息面（KT 扩展）：CallEvent 通话记录同步投影——本机
                // 其他设备/主设备的 1:1 通话历史写入（官方
                // callEventSync 同位，桌面 #handleCallEvent 行为对齐）。
                Some(presage::proto::sync_message::Content::CallEvent(event)) => {
                    let Some(projection) = project_call_event(event) else {
                        return Vec::new();
                    };
                    envelope["syncMessage"] = json!({ "callEvent": projection });
                }
                // 消息请求收包投影（messageRequestResponse 批，KT 扩展，
                // compat §5）：多设备 echo 的接受/拒绝终态——官方 content
                // oneof messageRequestResponse=14，connector §4.58 推导第一
                // 因子权重（messageRequestResponseType === ACCEPT）的引擎侧
                // 真数据面。
                Some(presage::proto::sync_message::Content::MessageRequestResponse(mrr)) => {
                    let Some(projection) = project_message_request_response(mrr) else {
                        return Vec::new();
                    };
                    envelope["syncMessage"] = json!({ "messageRequestResponse": projection });
                }
                // Q6② + P15a 项1：deleteForMe 收包投影（SyncContent.
                // deleteForMe=22，本机「为我删除」多设备同步）：vendored 接收
                // 面已对 presage store 做 messageDeletes 消费（registered.rs
                // save_message 臂）且 Received::Content 无条件上抛，引擎投影
                // 给 connector 供其删自有库行。P15a 项1 补整会话清除族两臂
                // （官方 handleDeleteForMeSync conversationDeletes/
                // localOnlyConversationDeletes 同位，桌面会话清除同步面）；
                // attachmentDeletes 臂仍不投（§5 gap 登记）。作者不可解析条
                // 目丢弃（vendored store 面 warn-and-continue 同口径）；无有
                // 效条目整信封丢弃。
                Some(presage::proto::sync_message::Content::DeleteForMe(delete)) => {
                    let Some(projection) = project_delete_for_me(delete) else {
                        return Vec::new();
                    };
                    envelope["syncMessage"] = json!({ "deleteForMe": projection });
                }
                // P14 组 5：sync 七整臂收投影（官方 handleConfiguration/
                // handleFetchLatest/handleCallLinkUpdate/handleCallLogEvent/
                // handleDeviceNameChangeSync/handleUsernameChangeSync/
                // handlePNIChangeNumber 同位消费面——此前全部落兜底臂整信封
                // 丢弃）。缺席键不在，旧宿主零变化。
                Some(presage::proto::sync_message::Content::Configuration(configuration)) => {
                    let mut face = serde_json::Map::new();
                    if let Some(flag) = configuration.read_receipts {
                        face.insert("readReceipts".into(), json!(flag));
                    }
                    if let Some(flag) = configuration.unidentified_delivery_indicators {
                        face.insert("unidentifiedDeliveryIndicators".into(), json!(flag));
                    }
                    if let Some(flag) = configuration.typing_indicators {
                        face.insert("typingIndicators".into(), json!(flag));
                    }
                    if let Some(flag) = configuration.link_previews {
                        face.insert("linkPreviews".into(), json!(flag));
                    }
                    envelope["syncMessage"] = json!({ "configuration": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::FetchLatest(fetch)) => {
                    // type 词 SCREAMING（as_str_name 惯例）；缺席/未知 i32 照投
                    // 空对象、UNKNOWN 词如实投位（官方 FetchLatestEvent 原样
                    // 透传 type、UNKNOWN 下游 no-op 同位——信号面即事件）。
                    let kind = fetch
                        .r#type
                        .and_then(|raw| {
                            presage::proto::sync_message::fetch_latest::Type::try_from(raw).ok()
                        })
                        .map(|kind| kind.as_str_name().to_owned());
                    let mut face = serde_json::Map::new();
                    if let Some(kind) = kind {
                        face.insert("type".into(), json!(kind));
                    }
                    envelope["syncMessage"] = json!({ "fetchLatest": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::CallLinkUpdate(update)) => {
                    // 官方 handleCallLinkUpdate 事件形状 {type: 'update',
                    // rootKey, adminKey}：type 缺省 UPDATE（proto 注释同位）；
                    // 未知 type 官方 throw（整信封丢弃）——引擎宁收勿丢只投
                    // 已知词（§5）；rootKey/adminPasskey 空字节官方 undefined
                    // 同口径省键。adminKey 键名沿官方事件面（proto 字段
                    // adminPasskey）。
                    let mut face = serde_json::Map::new();
                    if let Some(word) = update
                        .r#type
                        .and_then(|raw| {
                            presage::proto::sync_message::call_link_update::Type::try_from(raw).ok()
                        })
                        .map(|kind| kind.as_str_name().to_owned())
                    {
                        face.insert("type".into(), json!(word));
                    }
                    use base64::Engine as _;
                    if let Some(root_key) = update.root_key.as_deref().filter(|key| !key.is_empty())
                    {
                        face.insert(
                            "rootKey".into(),
                            json!(base64::engine::general_purpose::STANDARD.encode(root_key)),
                        );
                    }
                    if let Some(admin_key) = update
                        .admin_passkey
                        .as_deref()
                        .filter(|key| !key.is_empty())
                    {
                        face.insert(
                            "adminKey".into(),
                            json!(base64::engine::general_purpose::STANDARD.encode(admin_key)),
                        );
                    }
                    envelope["syncMessage"] = json!({ "callLinkUpdate": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::CallLogEvent(event)) => {
                    // 官方 getCallLogEventForProto 透传面 {type, targetTimestamp,
                    // conversationId(bytes), callId}：type 词 SCREAMING；proto
                    // timestamp 键名沿 proto（官方 targetTimestamp 是 zod 归一
                    // 名，wire 材料同源）；conversationId 原字节标准 base64
                    // （1:1 service-id/群 id/ad-hoc roomId 三形由消费方自判，
                    // 官方 peerId 双路解释同构）。
                    let mut face = serde_json::Map::new();
                    if let Some(word) = event
                        .r#type
                        .and_then(|raw| {
                            presage::proto::sync_message::call_log_event::Type::try_from(raw).ok()
                        })
                        .map(|kind| kind.as_str_name().to_owned())
                    {
                        face.insert("type".into(), json!(word));
                    }
                    if let Some(timestamp) = event.timestamp {
                        face.insert("timestamp".into(), json!(timestamp));
                    }
                    use base64::Engine as _;
                    if let Some(conversation_id) =
                        event.conversation_id.as_deref().filter(|id| !id.is_empty())
                    {
                        face.insert(
                            "conversationId".into(),
                            json!(base64::engine::general_purpose::STANDARD
                                .encode(conversation_id)),
                        );
                    }
                    if let Some(call_id) = event.call_id {
                        face.insert("callId".into(), json!(call_id));
                    }
                    envelope["syncMessage"] = json!({ "callLogEvent": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::DeviceNameChange(change)) => {
                    let mut face = serde_json::Map::new();
                    if let Some(device_id) = change.device_id {
                        face.insert("deviceId".into(), json!(device_id));
                    }
                    envelope["syncMessage"] = json!({ "deviceNameChange": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::UsernameChange(_)) => {
                    // 空消息纯信号面（官方 handleUsernameChangeSync 只触发
                    // profile 重取，零字段消费）。
                    envelope["syncMessage"] = json!({ "usernameChange": {} });
                }
                Some(presage::proto::sync_message::Content::PniChangeNumber(change)) => {
                    // 只投 {registrationId?, newE164?}——identityKeyPair/
                    // signedPreKey/lastResortKyberPreKey 密钥材料不出引擎边界
                    // （先例：backup key/凭据面同纪律）；引擎不做 PNI re-key
                    // 消费（官方全键消费+换钥，§5 gap 登记）。
                    let mut face = serde_json::Map::new();
                    if let Some(registration_id) = change.registration_id {
                        face.insert("registrationId".into(), json!(registration_id));
                    }
                    if let Some(new_e164) = change.new_e164.as_deref() {
                        face.insert("newE164".into(), json!(new_e164));
                    }
                    envelope["syncMessage"] = json!({ "pniChangeNumber": Value::Object(face) });
                }
                Some(presage::proto::sync_message::Content::Keys(keys)) => {
                    // P14 组 6：mediaRootBackupKey 云媒体备份根钥投影（官方
                    // handleKeys 事件面 MessageReceiver.preload.ts:3338-3358，
                    // 空字节省键 Bytes.isNotEmpty 同口径）。官方三键面
                    // {masterKey, accountEntropyPool, mediaRootBackupKey}，
                    // 引擎只投 mediaRootBackupKey：accountEntropyPool 密钥
                    // 材料不出引擎边界（PNI 密钥同纪律，§5）；masterKey 在
                    // 钉定 rev proto 已 reserved 删字段（prost 跳未知，无法
                    // 投）——两处偏差落 §5 组 6 块。Request{Keys} 应答停答
                    // 面（P9 S2）与此接收投影面互不相干。
                    let mut face = serde_json::Map::new();
                    if let Some(key) = keys
                        .media_root_backup_key
                        .as_deref()
                        .filter(|key| !key.is_empty())
                    {
                        use base64::Engine as _;
                        face.insert(
                            "mediaRootBackupKey".into(),
                            json!(base64::engine::general_purpose::STANDARD.encode(key)),
                        );
                    }
                    envelope["syncMessage"] = json!({ "keys": Value::Object(face) });
                }
                // P15a 项4：Blocked 同步收投影（官方 _handleBlocked 同位
                // MessageReceiver.preload.ts:3988——此前落兜底臂整信封丢）。
                // modern/legacy 家族选择按官方 areModernFieldsUsed/
                // areLegacyFieldsUsed 判定同构：任一 modern 三型非空 → modern
                // 家族；否则（legacy 在场或全空）→ legacy 家族（官方
                // `areModernFieldsUsed || !areLegacyFieldsUsed` 同构——全空
                // 归 modern 空对象，信号面即事件）。投影所选家族的 proto
                // 字段：modern = blockedE164s[{e164?, timestamp?}]/
                // blockedAcis[{aci, timestamp?}]/blockedGroups[{groupId,
                // timestamp?}]（aci 二进制→UUID 串、groupId 原字节标准
                // base64——引擎 groupId 口径；timestamp 缺席键不在）；
                // legacy = numbers/acis/groupIds/acisBinary 四位（acisBinary
                // 逐项 UUID 串、groupIds 逐项 base64）。上游 abe80d3 新增
                // modern 三型（vendored proto 同步登记 §5 proto 批）。
                Some(presage::proto::sync_message::Content::Blocked(blocked)) => {
                    use base64::Engine as _;
                    let modern_used = !blocked.blocked_e164s.is_empty()
                        || !blocked.blocked_acis.is_empty()
                        || !blocked.blocked_groups.is_empty();
                    let mut face = serde_json::Map::new();
                    if modern_used {
                        let e164s: Vec<Value> = blocked
                            .blocked_e164s
                            .iter()
                            .filter_map(|item| {
                                let e164 = item.e164.as_deref().filter(|e| !e.is_empty())?;
                                let mut entry = serde_json::Map::new();
                                entry.insert("e164".into(), json!(e164));
                                if let Some(ts) = item.timestamp {
                                    entry.insert("timestamp".into(), json!(ts));
                                }
                                Some(Value::Object(entry))
                            })
                            .collect();
                        if !e164s.is_empty() {
                            face.insert("blockedE164s".into(), Value::Array(e164s));
                        }
                        let acis: Vec<Value> = blocked
                            .blocked_acis
                            .iter()
                            .filter_map(|item| {
                                let aci = ServiceId::parse_from_service_id_binary(
                                    item.aci_binary.as_deref()?,
                                )
                                .map(|sid| sid.service_id_string())?;
                                let mut entry = serde_json::Map::new();
                                entry.insert("aci".into(), json!(aci));
                                if let Some(ts) = item.timestamp {
                                    entry.insert("timestamp".into(), json!(ts));
                                }
                                Some(Value::Object(entry))
                            })
                            .collect();
                        if !acis.is_empty() {
                            face.insert("blockedAcis".into(), Value::Array(acis));
                        }
                        let groups: Vec<Value> = blocked
                            .blocked_groups
                            .iter()
                            .filter_map(|item| {
                                let group_id =
                                    item.group_id.as_deref().filter(|g| !g.is_empty())?;
                                let mut entry = serde_json::Map::new();
                                entry.insert(
                                    "groupId".into(),
                                    json!(
                                        base64::engine::general_purpose::STANDARD.encode(group_id)
                                    ),
                                );
                                if let Some(ts) = item.timestamp {
                                    entry.insert("timestamp".into(), json!(ts));
                                }
                                Some(Value::Object(entry))
                            })
                            .collect();
                        if !groups.is_empty() {
                            face.insert("blockedGroups".into(), Value::Array(groups));
                        }
                    } else {
                        let numbers: Vec<Value> = blocked
                            .numbers
                            .iter()
                            .filter(|n| !n.is_empty())
                            .map(|n| json!(n))
                            .collect();
                        if !numbers.is_empty() {
                            face.insert("numbers".into(), Value::Array(numbers));
                        }
                        let acis: Vec<Value> = blocked
                            .acis
                            .iter()
                            .filter(|a| !a.is_empty())
                            .map(|a| json!(a))
                            .collect();
                        if !acis.is_empty() {
                            face.insert("acis".into(), Value::Array(acis));
                        }
                        let group_ids: Vec<Value> = blocked
                            .group_ids
                            .iter()
                            .filter(|g| !g.is_empty())
                            .map(|g| json!(base64::engine::general_purpose::STANDARD.encode(g)))
                            .collect();
                        if !group_ids.is_empty() {
                            face.insert("groupIds".into(), Value::Array(group_ids));
                        }
                        let aci_binaries: Vec<Value> = blocked
                            .acis_binary
                            .iter()
                            .filter_map(|bytes| {
                                ServiceId::parse_from_service_id_binary(bytes)
                                    .map(|sid| json!(sid.service_id_string()))
                            })
                            .collect();
                        if !aci_binaries.is_empty() {
                            face.insert("acisBinary".into(), Value::Array(aci_binaries));
                        }
                    }
                    envelope["syncMessage"] = json!({ "blocked": Value::Object(face) });
                }
                // D2：syncMessage.read / viewed 收包投影（官方 #handleRead/
                // #handleViewed「read 专信封」调度序同位——content 面分支
                // 不动，兜底臂承接直接 repeated 字段 read=5/viewed=16）。
                // 条目对 (senderAci, timestamp) 全保留（比聚合
                // read{timestamps[]} 忠实：多发送方各自独立，官方语义同）。
                _ => {
                    let read = project_sync_read_entries(store, &sync.read, |entry| {
                        entry
                            .parse_sender_aci()
                            .map(ServiceId::from)
                            .zip(entry.timestamp)
                    })
                    .await;
                    let viewed = project_sync_read_entries(store, &sync.viewed, |entry| {
                        entry
                            .parse_sender_aci()
                            .map(ServiceId::from)
                            .zip(entry.timestamp)
                    })
                    .await;
                    if read.is_empty() && viewed.is_empty() {
                        return Vec::new();
                    }
                    let mut sync_json = serde_json::Map::new();
                    if !read.is_empty() {
                        sync_json.insert("read".to_owned(), Value::Array(read));
                    }
                    if !viewed.is_empty() {
                        sync_json.insert("viewed".to_owned(), Value::Array(viewed));
                    }
                    envelope["syncMessage"] = Value::Object(sync_json);
                }
            }
        }
        ContentBody::CallMessage(call) => {
            let Some(projection) = project_call_message(call) else {
                return Vec::new();
            };
            envelope["callMessage"] = projection;
        }
        ContentBody::StoryMessage(story) => {
            // P14 组 4：mentions 面补 E.164 解析（dataMessage 同口径）。
            let numbers = resolve_story_mention_numbers(store, story).await;
            match project_story_message(story, &numbers) {
                Some(projection) => {
                    envelope["storyMessage"] = projection;
                }
                // 无 attachment 位 = 官方发送面不可能产出的 malformed 行，整
                // 信封丢弃（宁缺勿错）。
                None => return Vec::new(),
            }
        }
        ContentBody::NullMessage(_) | ContentBody::DecryptionErrorMessage(_) => return Vec::new(),
        // SKDM（群发 sender-key 分发，C 批）：协议内部握手行，永不投影到
        // connector（接收侧已在 cipher 层消费）。
        ContentBody::SenderKeyDistributionMessage(_) => return Vec::new(),
    }

    // 群变更投影放开（最小面）：群更新消息携带 terminated change →
    // `groupTerminated` 事件（{groupId, terminator?, timestamp}）；携带
    // expiration_timer change → `groupUpdated` 事件（{groupId, revision,
    // expireTimerSeconds, editor?}，expireTimerSeconds=0 = 关闭）；其余群
    // 更新（成员变更/权限面等）本批继续跳过（compat §5 终止批 + expiration
    // 批边界登记，全投影另行立项）。投影独立于 dataMessage 投影：既覆盖纯
    // change 信封（无正文）也覆盖携带 change 的普通群消息；其他设备自己发
    // 起的变更（sent sync）同样落事件。终止登记表先行（写面事件后到达的
    // 乱序窗口由该表兜住发送拦截）。
    if let Some((master_key, change)) = group_change_of(&content.body) {
        if let Some(terminate) = groups::project_terminate_from_context(&master_key, change) {
            terminated_groups.register(&terminate.group_id);
            return vec![json!({
                "jsonrpc": "2.0",
                "method": "receive",
                "params": {
                    "account": account,
                    "envelope": {
                        "timestamp": envelope_ts,
                        "groupTerminated": {
                            "groupId": terminate.group_id,
                            "terminator": terminate.terminator,
                            "timestamp": envelope_ts,
                        },
                    },
                },
            })];
        }
        if let Some(expiration) = groups::project_expiration_from_context(&master_key, change) {
            return vec![json!({
                "jsonrpc": "2.0",
                "method": "receive",
                "params": {
                    "account": account,
                    "envelope": {
                        "timestamp": envelope_ts,
                        "groupUpdated": {
                            "groupId": expiration.group_id,
                            "revision": expiration.revision,
                            "expireTimerSeconds": expiration.expire_timer_seconds,
                            "editor": expiration.editor,
                        },
                    },
                },
            })];
        }
        // D3 群态 change 全投影：批内含非 timer/terminate change →
        // `groupChanged` 事件（官方 GroupV2ChangeDetailType 词表，compat
        // §5）；timer 夹带走既有 expireTimerSeconds 键形；纯 timer/
        // terminate 批由上面两既有面承接（None 不投影）。
        if let Some(changes) = groups::project_changes_from_context(&master_key, change) {
            let mut event = json!({
                "groupId": changes.group_id,
                "revision": changes.revision,
                "changes": changes.changes,
            });
            if let Some(editor) = changes.editor {
                event["editor"] = json!(editor);
            }
            if let Some(seconds) = changes.expire_timer_seconds {
                event["expireTimerSeconds"] = json!(seconds);
            }
            return vec![json!({
                "jsonrpc": "2.0",
                "method": "receive",
                "params": {
                    "account": account,
                    "envelope": {
                        "timestamp": envelope_ts,
                        "groupChanged": event,
                    },
                },
            })];
        }
    }

    let mut notifications = Vec::new();
    if let Some(aci) = profile_key_update {
        // 无 flag 采集形态：普通消息照常投影（正文不吞），key 落库与事件
        // 静默并行——事件挂载零新增投影键（官方桌面 harvest 态同款）。
        notifications.push(contact_profile_updated_notification(
            account,
            envelope_ts,
            &content.metadata.sender,
            &aci,
        ));
    }
    notifications.push(json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": { "account": account, "envelope": envelope },
    }));
    notifications
}

/// DataMessage 资料键更新行判定：官方 flag 位 PROFILE_KEY_UPDATE=4（钉定
/// rev proto 实名）。官方桌面 flagCount 断言（≤1 个 flag 位）在此不复制
/// ——引擎只读不 enforce（expiration flag 同口径）。
fn is_profile_key_update_row(data_message: &presage::proto::DataMessage) -> bool {
    use presage::proto::data_message::Flags;
    data_message
        .flags
        .is_some_and(|flags| flags & Flags::ProfileKeyUpdate as u32 != 0)
}

/// receive 链 profile key 落库（profileKeyUpdate 批主体）：DataMessage 内
/// 携带 profile_key（bytes）时按发送方 ACI 落 presage store 既有
/// profile_keys 位（contacts 同步随行落库的同一位，不发明存储），返回
/// 「需要通知的 ACI」——32 字节合法且与已存值不同（首次/换 key）才 Some，
/// 幂等闸（同 key 重复收 = no-op，官方 hasChanged 门同构）；空 bytes/
/// 长度非法（协议定长 32B）fail-closed 丢弃不崩；store 错误静默（receive
/// 面零 stderr 纪律，下条消息重试自然收敛）。群内/sent sync 不处理
/// （profile key 是 1:1 作者面语义，见 compat §5 本批边界）。
async fn receive_profile_key(
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
) -> Option<presage::libsignal_service::prelude::Uuid> {
    let ContentBody::DataMessage(data_message) = &content.body else {
        return None;
    };
    let bytes = data_message.profile_key.as_deref()?;
    // 协议定长 32 字节：非法长度 fail-closed 丢弃（不崩不发明）。
    let Ok(key) = <[u8; 32]>::try_from(bytes) else {
        return None;
    };
    let key = presage::libsignal_service::zkgroup::profiles::ProfileKey::create(key);
    let sender = content.metadata.sender;
    let ServiceId::Aci(_) = sender else {
        return None;
    };
    let existing = store
        .profile_key(&sender)
        .await
        .ok()
        .flatten()
        .map(|existing| existing.bytes);
    if existing == Some(key.bytes) {
        return None;
    }
    // upsert_profile_key 是 &mut 面（trait 纪律）：SqliteStore 是连接池句柄
    // （Clone 语义，proj 开头 open 的同库同池），本函数就近克隆写入。
    let mut writable = store.clone();
    writable
        .upsert_profile_key(&sender.raw_uuid(), key)
        .await
        .ok()?;
    Some(sender.raw_uuid())
}

/// contactProfileUpdated 事件（profileKeyUpdate 批消费面）：profile key
/// 首次落库/换 key 后通知连接器——实际 profile/头像重取由消费方经既有
/// getAvatar 面按需拉取（引擎不持 profile 缓存）。密钥材料不出引擎：事件
/// 只带 aci（= 发送方 ACI，与 sourceUuid 同值）。
fn contact_profile_updated_notification(
    account: &str,
    timestamp: u64,
    sender: &ServiceId,
    aci: &presage::libsignal_service::prelude::Uuid,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": account,
            "envelope": {
                "timestamp": timestamp,
                "sourceUuid": sender.service_id_string(),
                "contactProfileUpdated": { "aci": aci.to_string() },
            },
        },
    })
}

/// 群更新消息的 change 密文提取（终止投影配套）：dataMessage / sent sync 内
/// 群上下文（master key + group_change 密文）。缺 change / 缺 master key →
/// None。
fn group_change_of(body: &ContentBody) -> Option<(Vec<u8>, Option<&[u8]>)> {
    match body {
        ContentBody::DataMessage(DataMessage {
            group_v2:
                Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key),
                    group_change,
                    ..
                }),
            ..
        }) => Some((master_key.clone(), group_change.as_deref())),
        ContentBody::SynchronizeMessage(sync) => match sync.content.as_ref() {
            Some(presage::proto::sync_message::Content::Sent(sent)) => {
                match sent.message.as_ref() {
                    Some(DataMessage {
                        group_v2:
                            Some(presage::proto::GroupContextV2 {
                                master_key: Some(master_key),
                                group_change,
                                ..
                            }),
                        ..
                    }) => Some((master_key.clone(), group_change.as_deref())),
                    _ => None,
                }
            }
            _ => None,
        },
        _ => None,
    }
}

/// E3：SyncMessage.ViewOnceOpen 投影 `{senderAci, timestamp}`。senderAci
/// 解析与官方 parse_sender_aci 同构（senderAciBinary 16B 优先、senderAci
/// 字符串兜底，双写形态兼容）。不可解析（binary 坏 + string 缺）= 无法定位
/// 目标行，warn-only 丢弃（None，零 stderr；乱序兜底口径与官方
/// modifyTargetMessage 对齐——引擎侧只登记不暂存，compat §5）；timestamp
/// 缺席投影 0 不丢弃（投影面 unwrap_or(0) 同风格）。
fn project_view_once_open(open: &presage::proto::sync_message::ViewOnceOpen) -> Option<Value> {
    let sender_aci = open.parse_sender_aci()?;
    Some(json!({
        "senderAci": presage::libsignal_service::protocol::ServiceId::from(sender_aci)
            .service_id_string(),
        "timestamp": open.timestamp.unwrap_or(0),
    }))
}

/// D2：syncMessage.read / viewed 收包投影条目（官方桌面 #handleRead /
/// #handleViewed 行为面，MessageReceiver.preload.ts:3437/:3472）。
/// SyncMessage 直接 repeated 字段 read=5 / viewed=16（不在 content
/// oneof），Read/Viewed 双形字段同构 → 闭包取 (senderAci, timestamp)
/// 对。senderAci 解析与官方 fromAciUuidBytesOrString 同构
/// （senderAciBinary 16B 优先、senderAci 字符串兜底，vendored
/// parse_sender_aci）；senderNumber contacts 命中补面（receive 信封作者
/// 号同口径）。timestamp 缺席或发送方不可解析丢条目（宁缺勿错；官方
/// toNumber ?? 0 / senderAci null 由上层过滤同位），全丢/空 = 键缺席。
/// connector 现今两者都不消费，纯 additive（compat §5）。
async fn project_sync_read_entries<T>(
    store: &SqliteStore,
    entries: &[T],
    parse: impl Fn(&T) -> Option<(presage::libsignal_service::protocol::ServiceId, u64)>,
) -> Vec<Value> {
    let mut out = Vec::new();
    for entry in entries {
        let Some((service_id, timestamp)) = parse(entry) else {
            continue;
        };
        let mut item = json!({
            "senderUuid": service_id.service_id_string(),
            "timestamp": timestamp,
        });
        if let Some(peer) = describe_peer(store, &service_id).await {
            if let Some(number) = peer.number {
                item["senderNumber"] = json!(number);
            }
        }
        out.push(item);
    }
    out
}

/// 消息请求收包投影（messageRequestResponse 批，compat §5）：SyncMessage.
/// MessageRequestResponse 投影 `{threadAci?, groupId?,
/// messageRequestResponseType}`。官方桌面 #handleMessageRequestResponse
/// 行为同构：thread 双形 binary 优先 string 兜底（parse_thread_aci 钉定
/// helper）；groupId 必须 32 字节（官方 GROUPV2_ID_LENGTH 校验同构）→
/// 标准 base64（引擎 send/listGroups groupId 口径），长度非法整事件丢弃
/// （官方 removeFromCache 同果）；线程双缺（无可路由会话）同丢（宁缺勿
/// 错，viewOnceOpen senderAci 不可解析同纪律）；type 缺席/枚举外值强制
/// UNKNOWN=0（官方 isKnownProtoEnumMember 同构），数字 = 官方因子词汇原值
/// （桌面 conversationAttrs.messageRequestResponseType 同位，ACCEPT=1）。
fn project_message_request_response(
    mrr: &presage::proto::sync_message::MessageRequestResponse,
) -> Option<Value> {
    use base64::Engine as _;
    use presage::proto::sync_message::message_request_response::Type;
    const GROUP_V2_ID_LENGTH: usize = 32;
    if mrr
        .group_id
        .as_deref()
        .is_some_and(|group_id| group_id.len() != GROUP_V2_ID_LENGTH)
    {
        return None;
    }
    let thread_aci = mrr.parse_thread_aci();
    if thread_aci.is_none() && mrr.group_id.is_none() {
        return None;
    }
    let mut out = json!({
        "messageRequestResponseType": mrr
            .r#type
            .and_then(|value| Type::try_from(value).ok())
            .unwrap_or(Type::Unknown) as i32,
    });
    if let Some(aci) = thread_aci {
        out["threadAci"] =
            json!(presage::libsignal_service::protocol::ServiceId::from(aci).service_id_string());
    }
    if let Some(group_id) = mrr.group_id.as_deref() {
        out["groupId"] = json!(base64::engine::general_purpose::STANDARD.encode(group_id));
    }
    Some(out)
}

/// CallMessage 1:1 信令投影（通话消息面，proto 只投影不发明）：opaque.data
/// base64 + urgency（缺席 = Droppable 原样透出，官方 proto 缺省同语义）；
/// offer/answer/iceUpdate/busy/hangup/destinationDeviceId 在场字段逐面
/// 投影（旧线面信令）。空白 CallMessage（无可投影字段）= None 整事件丢弃
/// （与群组 master key 缺席同款宁缺勿错）。
fn project_call_message(call: &presage::proto::CallMessage) -> Option<Value> {
    let mut out = match call.opaque.as_ref() {
        Some(opaque) => {
            use base64::Engine as _;
            let mut value = json!({
                "opaque": base64::engine::general_purpose::STANDARD
                    .encode(opaque.data.as_deref().unwrap_or_default()),
                "urgency": opaque_urgency_json(opaque.urgency),
            });
            if let Some(device_id) = call.destination_device_id {
                value["destinationDeviceId"] = json!(device_id);
            }
            value
        }
        None => json!({}),
    };
    if let Some(offer) = call.offer.as_ref() {
        let mut value = json!({ "id": offer.id.unwrap_or(0) });
        if let Some(kind) = offer
            .r#type
            .and_then(|kind| presage::proto::call_message::offer::Type::try_from(kind).ok())
        {
            value["type"] = json!(offer_type_json(kind));
        }
        if let Some(bytes) = offer.opaque.as_deref() {
            use base64::Engine as _;
            value["opaque"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
        }
        out["offer"] = value;
    }
    if let Some(answer) = call.answer.as_ref() {
        let mut value = json!({ "id": answer.id.unwrap_or(0) });
        if let Some(bytes) = answer.opaque.as_deref() {
            use base64::Engine as _;
            value["opaque"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
        }
        out["answer"] = value;
    }
    if !call.ice_update.is_empty() {
        out["iceUpdate"] = json!(call
            .ice_update
            .iter()
            .map(|update| {
                let mut value = json!({ "id": update.id.unwrap_or(0) });
                if let Some(bytes) = update.opaque.as_deref() {
                    use base64::Engine as _;
                    value["opaque"] =
                        json!(base64::engine::general_purpose::STANDARD.encode(bytes));
                }
                value
            })
            .collect::<Vec<_>>());
    }
    if let Some(busy) = call.busy.as_ref() {
        out["busy"] = json!({ "id": busy.id.unwrap_or(0) });
    }
    if let Some(hangup) = call.hangup.as_ref() {
        let mut value = json!({ "id": hangup.id.unwrap_or(0) });
        if let Some(kind) = hangup
            .r#type
            .and_then(|kind| presage::proto::call_message::hangup::Type::try_from(kind).ok())
        {
            value["type"] = json!(hangup_type_json(kind));
        }
        if let Some(device_id) = hangup.device_id {
            value["deviceId"] = json!(device_id);
        }
        out["hangup"] = value;
    }
    // offer/answer/opaque 都缺席且无其他信令面：无语义空信封丢弃。
    if out.as_object().is_some_and(|map| map.is_empty()) {
        return None;
    }
    Some(out)
}

/// Opaque.urgency → 投影串。缺席 = "droppable"（proto「缺省按 DROPPABLE
/// 处理」注释同语义；投影显式化，不发明 null）；枚举外值原样缺省折叠。
fn opaque_urgency_json(urgency: Option<i32>) -> &'static str {
    match urgency
        .and_then(|value| presage::proto::call_message::opaque::Urgency::try_from(value).ok())
    {
        Some(presage::proto::call_message::opaque::Urgency::HandleImmediately) => {
            "handleImmediately"
        }
        _ => "droppable",
    }
}

fn offer_type_json(kind: presage::proto::call_message::offer::Type) -> &'static str {
    match kind {
        presage::proto::call_message::offer::Type::OfferVideoCall => "video",
        _ => "audio",
    }
}

fn hangup_type_json(kind: presage::proto::call_message::hangup::Type) -> &'static str {
    match kind {
        presage::proto::call_message::hangup::Type::HangupAccepted => "accepted",
        presage::proto::call_message::hangup::Type::HangupDeclined => "declined",
        presage::proto::call_message::hangup::Type::HangupBusy => "busy",
        presage::proto::call_message::hangup::Type::HangupNeedPermission => "needPermission",
        _ => "normal",
    }
}

/// CallEvent 通话记录同步投影（官方 getCallEventForProto 同构）：类型/
/// 方向/事件枚举投影小写字符串；conversationId 按官方桌面
/// conversationPeerIdInBytesSchema 口径（16B = UUID 串，否则标准 base64；
/// ad-hoc 房间 id hex）；callId 官方投影字符串形态（longToString 同构，
/// JS u64 安全面）。callId 缺席 = 无记录主键丢弃（warn-only，与
/// viewOnceOpen senderAci 不可解析同纪律，零 stderr）。
fn project_call_event(event: &presage::proto::sync_message::CallEvent) -> Option<Value> {
    let call_id = event.call_id?;
    use presage::proto::sync_message::call_event as event_enum;
    let kind = event
        .r#type
        .and_then(|value| event_enum::Type::try_from(value).ok());
    let direction = event
        .direction
        .and_then(|value| event_enum::Direction::try_from(value).ok());
    let outcome = event
        .event
        .and_then(|value| event_enum::Event::try_from(value).ok());
    let mut out = json!({
        "callId": call_id.to_string(),
        "timestamp": event.timestamp.unwrap_or(0),
    });
    if let Some(kind) = kind {
        out["type"] = json!(call_event_type_json(kind));
    }
    if let Some(direction) = direction {
        out["direction"] = json!(call_event_direction_json(direction));
    }
    if let Some(outcome) = outcome {
        out["event"] = json!(call_event_outcome_json(outcome));
    }
    if let Some(bytes) = event.conversation_id.as_deref() {
        if let Some(conversation_id) = call_event_conversation_id_json(bytes, kind) {
            out["conversationId"] = json!(conversation_id);
        }
    }
    Some(out)
}

fn call_event_type_json(kind: presage::proto::sync_message::call_event::Type) -> &'static str {
    use presage::proto::sync_message::call_event::Type;
    match kind {
        Type::AudioCall => "audio",
        Type::VideoCall => "video",
        Type::GroupCall => "group",
        Type::AdHocCall => "ad-hoc",
        Type::UnknownType => "unknown",
    }
}

fn call_event_direction_json(
    direction: presage::proto::sync_message::call_event::Direction,
) -> &'static str {
    use presage::proto::sync_message::call_event::Direction;
    match direction {
        Direction::Outgoing => "outgoing",
        Direction::Incoming => "incoming",
        Direction::UnknownDirection => "unknown",
    }
}

fn call_event_outcome_json(
    outcome: presage::proto::sync_message::call_event::Event,
) -> &'static str {
    use presage::proto::sync_message::call_event::Event;
    match outcome {
        Event::Accepted => "accepted",
        Event::NotAccepted => "not-accepted",
        Event::Delete => "delete",
        Event::Observed => "observed",
        Event::UnknownEvent => "unknown",
    }
}

/// CallEvent.conversationId → 会话标识（官方桌面同构）：ad-hoc = 房间 id
/// hex（roomIdInBytesSchema 同构）；16B = 直聊 ACI UUID 串；否则群 = 标准
/// base64。空字节无归属语义 → None（键省略，不发明空串）。
fn call_event_conversation_id_json(
    bytes: &[u8],
    kind: Option<presage::proto::sync_message::call_event::Type>,
) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    use presage::proto::sync_message::call_event::Type;
    if kind == Some(Type::AdHocCall) {
        return Some(hex_encode(bytes));
    }
    if bytes.len() == 16 {
        if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
            return Some(uuid.to_string());
        }
    }
    use base64::Engine as _;
    Some(base64::engine::general_purpose::STANDARD.encode(bytes))
}

struct PeerDescription {
    number: Option<String>,
    name: Option<String>,
}

/// pin/unpin/adminDelete 的 targetAuthor 投影：16B UUID → contacts 命中转
/// E.164，未命中 ACI 字符串（与 quote/reaction 面同口径）。
fn targeted_author_json(
    aci_binary: Option<&[u8]>,
    author_numbers: &HashMap<String, String>,
) -> Value {
    let aci = aci_binary
        .and_then(|bytes| presage::libsignal_service::prelude::Uuid::from_slice(bytes).ok())
        .map(|uuid| uuid.to_string())
        .unwrap_or_default();
    json!(author_numbers.get(&aci).cloned().unwrap_or(aci))
}

async fn describe_peer(store: &SqliteStore, id: &ServiceId) -> Option<PeerDescription> {
    let contact = store.contact_by_id(id).await.ok()??;
    Some(PeerDescription {
        number: contact.phone_number.as_ref().map(e164_string),
        name: Some(contact.name).filter(|name| !name.is_empty()),
    })
}

/// 引用/反应作者号解析：quote.author 与 reaction.targetAuthor 在 contacts 命中
/// 时投影为 E.164（signal-cli 行为），未命中保留 ACI 字符串。
async fn resolve_author_numbers(
    store: &SqliteStore,
    data_message: &presage::proto::DataMessage,
) -> HashMap<String, String> {
    let mut candidates: Vec<String> = Vec::new();
    if let Some(quote) = data_message.quote.as_ref() {
        candidates.extend(quote.author_aci.clone());
        if let Some(bytes) = quote.author_aci_binary.as_deref() {
            if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
                candidates.push(uuid.to_string());
            }
        }
        // Q5：quote.bodyRanges mention 作者与 quote author 同解析口径
        // （E.164 优先，收投影 bodyRanges 映射共用）。
        for range in &quote.body_ranges {
            match range.associated_value.as_ref() {
                Some(presage::proto::body_range::AssociatedValue::MentionAci(aci)) => {
                    candidates.push(aci.clone());
                }
                Some(presage::proto::body_range::AssociatedValue::MentionAciBinary(bytes)) => {
                    if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
                        candidates.push(uuid.to_string());
                    }
                }
                _ => {}
            }
        }
    }
    // Q3：pollVote targetAuthor 与 pin/unpin/adminDelete 同解析口径。
    if let Some(bytes) = data_message
        .poll_vote
        .as_ref()
        .and_then(|vote| vote.target_author_aci_binary.as_deref())
    {
        if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
            candidates.push(uuid.to_string());
        }
    }
    if let Some(reaction) = data_message.reaction.as_ref() {
        candidates.extend(reaction.target_author_aci.clone());
        if let Some(bytes) = reaction.target_author_aci_binary.as_deref() {
            if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
                candidates.push(uuid.to_string());
            }
        }
    }
    // pin/unpin/adminDelete 的 targetAuthor 与 reaction 同解析口径（E.164
    // 优先，contacts 未命中保留 ACI）。
    for targeted in [
        data_message
            .pin_message
            .as_ref()
            .map(|p| &p.target_author_aci_binary),
        data_message
            .unpin_message
            .as_ref()
            .map(|p| &p.target_author_aci_binary),
        data_message
            .admin_delete
            .as_ref()
            .map(|p| &p.target_author_aci_binary),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(bytes) = targeted.as_deref() {
            if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
                candidates.push(uuid.to_string());
            }
        }
    }
    let mut numbers = HashMap::new();
    for aci in candidates {
        if numbers.contains_key(&aci) {
            continue;
        }
        if let Some(service_id) = ServiceId::parse_from_service_id_string(&aci) {
            if let Some(peer) = describe_peer(store, &service_id).await {
                if let Some(number) = peer.number {
                    numbers.insert(aci, number);
                }
            }
        }
    }
    numbers
}

/// story mentions 作者号解析（P14 组 4：project_story_message mentions 面
/// E.164 映射，resolve_author_numbers 同口径——story.body_ranges mention
/// 臂候选 → contacts 命中投影 E.164，未命中保留 ACI）。
async fn resolve_story_mention_numbers(
    store: &SqliteStore,
    story: &presage::proto::StoryMessage,
) -> HashMap<String, String> {
    let mut candidates: Vec<String> = Vec::new();
    for range in &story.body_ranges {
        match range.associated_value.as_ref() {
            Some(presage::proto::body_range::AssociatedValue::MentionAci(aci)) => {
                candidates.push(aci.clone());
            }
            Some(presage::proto::body_range::AssociatedValue::MentionAciBinary(bytes)) => {
                if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
                    candidates.push(uuid.to_string());
                }
            }
            _ => {}
        }
    }
    let mut numbers = HashMap::new();
    for aci in candidates {
        if numbers.contains_key(&aci) {
            continue;
        }
        if let Some(service_id) = ServiceId::parse_from_service_id_string(&aci) {
            if let Some(peer) = describe_peer(store, &service_id).await {
                if let Some(number) = peer.number {
                    numbers.insert(aci, number);
                }
            }
        }
    }
    numbers
}

/// 群组接收投影 id：dataMessage.groupV2 master key 标准 base64（32 字节，
/// signal-cli 同源形状，与 connector send 回传/群会话管线同 id 口径）。
fn group_id_of(data_message: &presage::proto::DataMessage) -> Option<String> {
    use base64::Engine as _;
    let key = data_message.group_v2.as_ref()?.master_key.as_deref()?;
    if key.is_empty() {
        return None;
    }
    Some(base64::engine::general_purpose::STANDARD.encode(key))
}

/// 1:1 计时器变更行判定（收投影 isExpirationUpdate 对位）：官方 flag 位
/// EXPIRATION_TIMER_UPDATE=2（钉定 rev proto 实名，见 project_data_message
/// 注）。官方桌面 isValidTapToView 同代的 flagCount 断言（0 或 1 个 flag
/// 位）在此不复制——引擎只读不 enforce，非法组合由消费方裁量。
fn is_expiration_update_row(data_message: &presage::proto::DataMessage) -> bool {
    use presage::proto::data_message::Flags;
    data_message
        .flags
        .is_some_and(|flags| flags & Flags::ExpirationTimerUpdate as u32 != 0)
}

/// Sent 面起算时间（消失消息 E 批）：Some 且 >0 才投，0/缺席无起算语义
/// 键不在（同 history 面 0 过滤口径）。
fn sent_expiration_start(sent: &presage::proto::sync_message::Sent) -> Option<u64> {
    sent.expiration_start_timestamp.filter(|start| *start > 0)
}

/// Sent.unidentifiedStatus 条目投影（P14 收投影缺）：destinationUuid =
/// binary 16/17B 优先、字符串兜底（Sent.destination 同口径），两者皆缺/
/// 不可解析键省略；unidentified 布尔 Some 才投（线上缺省 false 如实缺席）；
/// destinationPniIdentityKey = 目的地 PNI 身份公钥字节（公开材料，标准
/// base64，非空才投——官方 PNI 目的地专用位）。
fn sent_unidentified_status_json(
    status: &presage::proto::sync_message::sent::UnidentifiedDeliveryStatus,
) -> Value {
    use base64::Engine as _;
    let mut out = serde_json::Map::new();
    let service_id = status
        .destination_service_id_binary
        .as_deref()
        .and_then(ServiceId::parse_from_service_id_binary)
        .or_else(|| {
            status
                .destination_service_id
                .as_deref()
                .and_then(ServiceId::parse_from_service_id_string)
        });
    if let Some(service_id) = service_id {
        out.insert(
            "destinationUuid".into(),
            json!(service_id.service_id_string()),
        );
    }
    if let Some(unidentified) = status.unidentified {
        out.insert("unidentified".into(), json!(unidentified));
    }
    if let Some(key) = status
        .destination_pni_identity_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        out.insert(
            "destinationPniIdentityKey".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(key)),
        );
    }
    Value::Object(out)
}

/// Sent.storyMessageRecipients 条目投影（P14 收投影缺，官方桌面
/// handleSentMessage storyMessageRecipients 同位）：destinationUuid =
/// binary 优先字符串兜底（键省略同上）；distributionListIds 原样数组
/// （空批照投空数组——名单收窄语义）；isAllowedToReply Some 才投。
fn sent_story_message_recipient_json(
    recipient: &presage::proto::sync_message::sent::StoryMessageRecipient,
) -> Value {
    let mut out = serde_json::Map::new();
    let service_id = recipient
        .destination_service_id_binary
        .as_deref()
        .and_then(ServiceId::parse_from_service_id_binary)
        .or_else(|| {
            recipient
                .destination_service_id
                .as_deref()
                .and_then(ServiceId::parse_from_service_id_string)
        });
    if let Some(service_id) = service_id {
        out.insert(
            "destinationUuid".into(),
            json!(service_id.service_id_string()),
        );
    }
    out.insert(
        "distributionListIds".into(),
        json!(recipient.distribution_list_ids),
    );
    if let Some(allowed) = recipient.is_allowed_to_reply {
        out.insert("isAllowedToReply".into(), json!(allowed));
    }
    Value::Object(out)
}

/// dataMessage 面（正文/引用/reaction/remoteDelete/pinMessage/unpinMessage/
/// adminDelete/附件元数据/预览/mentions/viewOnce）。边界纪律：附件 ≤32、
/// 预览 ≤4、mentions ≤64（connector §4.13/§4.20 上限同源）；条目内字段协议
/// 缺省即省略键。
fn project_data_message(
    data_message: &presage::proto::DataMessage,
    author_numbers: &HashMap<String, String>,
) -> Value {
    let timestamp = data_message.timestamp.unwrap_or(0);
    let mut out = json!({ "timestamp": timestamp });
    // 消失消息 E 批（接收投影，缺省键不在）：expireTimer=5（秒）为官方
    // 「每条消息携带会话计时器」语义位，收投影 faithful 透出（signal-cli
    // JsonDataMessage.expiresInSeconds 同名）；发送构造面不设该字段（立项
    // 红线，出站普通消息不加 expireTimer）。
    if let Some(seconds) = data_message.expire_timer {
        out["expiresInSeconds"] = json!(seconds);
    }
    // isExpirationUpdate 对位（proto 实名已核）：钉定 rev 的
    // SignalService.proto 无 isExpirationUpdate bool（新版 field 38），官方
    // 机制是 DataMessage.Flags.EXPIRATION_TIMER_UPDATE=2 位——官方桌面
    // v8.31 收（processDataMessage）/发（conversations.updateExpirationTimer）
    // 两侧都用该 flag 位，signal-cli 0.14.8 面的 isExpirationUpdate 即其
    // 同义投影。
    if is_expiration_update_row(data_message) {
        out["isExpirationUpdate"] = json!(true);
    }
    // P14 收投影缺：expireTimerVersion=23（计时器版本单调位，官方桌面
    // updateExpirationTimer 乱序丢弃判据；E 批起我们发送线面由 presage
    // 注入，收投影此前缺位）。>0 才投（0 = INITIAL 初值零信息量——presage
    // restore_thread_timer 对无计时器线程注 Some(0)，全量透出会给每条
    // 无计时器消息加零信息键；1 起才是官方版本化语义）。
    if let Some(version) = data_message
        .expire_timer_version
        .filter(|version| *version > 0)
    {
        out["expireTimerVersion"] = json!(version);
    }
    // P14 收投影缺（弱面）：requiredProtocolVersion=12。官方强校验是
    // required > 本端支持即整消息拒收（宁收勿丢红线，不收紧）；引擎只
    // 投影给消费方自判，缺席/>0 才投（0 = INITIAL 无要求语义，零信息量
    // 不投）。宽容面记 compat §5。
    if let Some(version) = data_message
        .required_protocol_version
        .filter(|version| *version > 0)
    {
        out["requiredProtocolVersion"] = json!(version);
    }
    if let Some(group_id) = group_id_of(data_message) {
        // signal-cli 同款形状（dataMessage.groupV2 / sent 内群组同形状）：
        // connector 既有群会话管线（data_message_group_id）直接消费。
        out["groupInfo"] = json!({ "groupId": group_id });
    }
    if let Some(body) = data_message.body.as_deref() {
        out["message"] = json!(body);
    }
    if data_message.is_view_once == Some(true) {
        out["viewOnce"] = json!(true);
        // E2：官方 isValidTapToView 同构校验——非法行补 viewOnceInvalid
        // （官方 isTapToViewInvalid 同位）。引擎 metadata-only 无字节可擦，
        // 只标记不擦除（compat §5）。
        if !is_valid_view_once_row(data_message) {
            out["viewOnceInvalid"] = json!(true);
        }
    }
    if let Some(quote) = data_message.quote.as_ref() {
        let author = quote
            .author_aci
            .as_deref()
            .map(|aci| {
                author_numbers
                    .get(aci)
                    .cloned()
                    .unwrap_or_else(|| aci.to_owned())
            })
            .unwrap_or_default();
        let mut quote_json = json!({
            "id": quote.id.unwrap_or(0),
            "author": author,
        });
        if let Some(text) = quote.text.as_deref() {
            quote_json["text"] = json!(text);
        }
        // D4：quote.attachments[] 附件元数据面（官方 QuotedAttachment wire
        // 三键形；wire 键 filename 同引擎附件面）。thumbnail =
        // AttachmentPointer 全投影（project_attachment_pointer 同源；引擎
        // metadata-only 透传指针，可定位不可渲染）。空数组键缺席。
        let attachments: Vec<Value> = quote
            .attachments
            .iter()
            .map(|attachment| {
                let mut item = serde_json::Map::new();
                if let Some(content_type) = attachment.content_type.as_deref() {
                    item.insert("contentType".into(), json!(content_type));
                }
                if let Some(file_name) = attachment.file_name.as_deref() {
                    item.insert("filename".into(), json!(file_name));
                }
                if let Some(thumbnail) = attachment.thumbnail.as_ref() {
                    item.insert("thumbnail".into(), project_attachment_pointer(thumbnail));
                }
                Value::Object(item)
            })
            .collect();
        if !attachments.is_empty() {
            quote_json["attachments"] = Value::Array(attachments);
        }
        // Q5：quote.type 收投影（Quote.type=7；官方 processQuote 恒产 type——
        // isKnownProtoEnumMember coerce，未知/缺席值归 NORMAL，
        // processDataMessage.preload.ts:227-229）。
        let quote_type =
            presage::proto::data_message::quote::Type::try_from(quote.r#type.unwrap_or_default())
                .map(|value| value.as_str_name())
                .unwrap_or("NORMAL");
        quote_json["type"] = json!(quote_type);
        // Q5：quote.bodyRanges 收投影（Quote.bodyRanges=6；官方 processQuote
        // bodyRanges 面 filterAndClean 同构）。mention 臂二进制优先 +
        // author_numbers 映射（quote author 同口径），Style 臂归同条目 style
        // 键（消息面 textStyles 词表同源；NONE composer 永不出、投影丢弃
        // 对称）；未知枚举整条丢弃；条目 ≤64（mentions 收投影 cap 同源）。
        let quote_body_ranges: Vec<Value> = quote
            .body_ranges
            .iter()
            .filter_map(|range| {
                let start = range.start.unwrap_or(0);
                let length = range.length.unwrap_or(0);
                match range.associated_value.as_ref()? {
                    presage::proto::body_range::AssociatedValue::MentionAci(aci) => Some(json!({
                        "number": author_numbers.get(aci).cloned().unwrap_or_else(|| aci.clone()),
                        "start": start,
                        "length": length,
                    })),
                    presage::proto::body_range::AssociatedValue::MentionAciBinary(bytes) => {
                        let aci = presage::libsignal_service::prelude::Uuid::from_slice(bytes)
                            .ok()?
                            .to_string();
                        Some(json!({
                            "number": author_numbers.get(&aci).cloned().unwrap_or(aci),
                            "start": start,
                            "length": length,
                        }))
                    }
                    presage::proto::body_range::AssociatedValue::Style(value) => {
                        let word = presage::proto::body_range::Style::try_from(*value)
                            .ok()?
                            .as_str_name();
                        if word == "NONE" {
                            return None;
                        }
                        Some(json!({ "style": word, "start": start, "length": length }))
                    }
                }
            })
            .take(64)
            .collect();
        if !quote_body_ranges.is_empty() {
            quote_json["bodyRanges"] = Value::Array(quote_body_ranges);
        }
        out["quote"] = quote_json;
    }
    if let Some(reaction) = data_message.reaction.as_ref() {
        let target_author = reaction
            .target_author_aci
            .as_deref()
            .map(|aci| {
                author_numbers
                    .get(aci)
                    .cloned()
                    .unwrap_or_else(|| aci.to_owned())
            })
            .unwrap_or_default();
        out["reaction"] = json!({
            "emoji": reaction.emoji.clone().unwrap_or_default(),
            "targetAuthor": target_author,
            "targetSentTimestamp": reaction.target_sent_timestamp.unwrap_or(0),
            "isRemove": reaction.remove.unwrap_or(false),
        });
    }
    if let Some(delete) = data_message.delete.as_ref() {
        out["remoteDelete"] = json!({ "timestamp": delete.target_sent_timestamp.unwrap_or(0) });
    }
    // 官方 pinMessage=27/unpinMessage=28/adminDelete=29（SignalService.proto，
    // processDataMessage.preload.ts processPinMessage 行为对齐）。targetAuthor
    // 与 reaction 同口径（E.164 命中转号码，未命中 ACI）；pinDurationSeconds
    // 缺席 = forever（官方 pinDuration null 语义）。
    if let Some(pin) = data_message.pin_message.as_ref() {
        let mut value = json!({
            "targetAuthor": targeted_author_json(pin.target_author_aci_binary.as_deref(), author_numbers),
            "targetSentTimestamp": pin.target_sent_timestamp.unwrap_or(0),
        });
        if let Some(presage::proto::data_message::pin_message::PinDuration::PinDurationSeconds(
            seconds,
        )) = pin.pin_duration.as_ref()
        {
            value["pinDurationSeconds"] = json!(seconds);
        }
        out["pinMessage"] = value;
    }
    if let Some(unpin) = data_message.unpin_message.as_ref() {
        out["unpinMessage"] = json!({
            "targetAuthor": targeted_author_json(unpin.target_author_aci_binary.as_deref(), author_numbers),
            "targetSentTimestamp": unpin.target_sent_timestamp.unwrap_or(0),
        });
    }
    if let Some(admin_delete) = data_message.admin_delete.as_ref() {
        out["adminDelete"] = json!({
            "targetAuthor": targeted_author_json(admin_delete.target_author_aci_binary.as_deref(), author_numbers),
            "targetSentTimestamp": admin_delete.target_sent_timestamp.unwrap_or(0),
        });
    }
    let attachments: Vec<Value> = data_message
        .attachments
        .iter()
        .take(32)
        .map(project_attachment_pointer)
        .collect();
    if !attachments.is_empty() {
        out["attachments"] = json!(attachments);
    }
    let previews: Vec<Value> = data_message
        .preview
        .iter()
        .take(4)
        .map(|preview| {
            let mut value = json!({ "url": preview.url.clone().unwrap_or_default() });
            if let Some(title) = preview.title.as_deref() {
                value["title"] = json!(title);
            }
            if let Some(description) = preview.description.as_deref() {
                value["description"] = json!(description);
            }
            // P14 收投影缺：Preview.date=5（链接预览文章发布时刻，毫秒；
            // 官方 cleanLinkPreviewDate 同语义 >0 才有效——0/缺席无时刻
            // 信息不投）。两端批：发送面 previewDate 对位。
            if let Some(date) = preview.date.filter(|date| *date > 0) {
                value["date"] = json!(date);
            }
            if let Some(image) = preview.image.as_ref() {
                value["image"] = project_attachment_pointer(image);
            }
            value
        })
        .collect();
    if !previews.is_empty() {
        out["previews"] = json!(previews);
    }
    // Q2：contact 卡片收投影（官方 processDataMessage processContact 行为
    // 对齐 processDataMessage.preload.ts:263-281：slice(0,1) + avatar
    // {avatar, isProfile:Boolean} 展开位）。slice(0,1) = 首条；键形与 backup
    // 导出面 contact_record（history.rs）同族；address 面与展开的其余位不投
    // （backup 导出面同边界，§5 gap 登记）；空卡片整键缺席（backup 面同
    // 口径：空卡片即占位，永不投占位）。
    if let Some(contact) = data_message.contact.first() {
        if let Some(card) = project_contact_card(contact) {
            out["contact"] = card;
        }
    }
    let mentions: Vec<Value> = data_message
        .body_ranges
        .iter()
        .filter_map(|range| {
            let uuid = match range.associated_value.as_ref()? {
                presage::proto::body_range::AssociatedValue::MentionAci(aci) => aci.clone(),
                presage::proto::body_range::AssociatedValue::MentionAciBinary(bytes) => {
                    presage::libsignal_service::prelude::Uuid::from_slice(bytes)
                        .ok()?
                        .to_string()
                }
                // Style arm 归 textStyles 投影（同函数下方），mentions 不取。
                presage::proto::body_range::AssociatedValue::Style(_) => return None,
            };
            Some(json!({
                "number": uuid,
                "start": range.start.unwrap_or(0),
                "length": range.length.unwrap_or(0),
            }))
        })
        .take(64)
        .collect();
    if !mentions.is_empty() {
        out["mentions"] = json!(mentions);
    }
    // 文字格式化样式投影（connector 契约 1.69 §4.62 收侧对称面，wire 三仓
    // 并行批钉定）：body_ranges 的 Style arm → `textStyles[] {style, start,
    // length}`。NONE 不投（发送面拒收对称——官方 composer 永不发出的 no-op
    // 标记）；未知枚举值整条丢弃（proto 只投影不发明，不 fatal）；重复/
    // 重叠条目不做重叠闸（官方渲染侧直接按序应用，投影侧只丢形状坏的：
    // start+length 超 body UTF-16 长度的条目丢弃——u32 字段经 unwrap_or(0)
    // 缺省不会溢出；无 Style 条目不携键，缺席=纯文本，旧宿主零变化；条目
    // 保持 wire 顺序，官方渲染侧按序应用）。proto 证据：钉定
    // libsignal-service-rs（9e6c08b）SignalService.proto:983-1001。
    let body_utf16_len = data_message
        .body
        .as_deref()
        .map(|body| body.encode_utf16().count())
        .unwrap_or(0);
    let text_styles: Vec<Value> = data_message
        .body_ranges
        .iter()
        .filter_map(|range| {
            let value = match range.associated_value.as_ref()? {
                presage::proto::body_range::AssociatedValue::Style(value) => *value,
                _ => return None,
            };
            let word = presage::proto::body_range::Style::try_from(value)
                .ok()?
                .as_str_name();
            // NONE=0 是 composer 永不发出的 no-op 标记：投影丢弃（发送面
            // 词表拒收对称）。
            if word == "NONE" {
                return None;
            }
            let start = range.start.unwrap_or(0);
            let length = range.length.unwrap_or(0);
            if start as u64 + length as u64 > body_utf16_len as u64 {
                return None;
            }
            Some(json!({
                "style": word,
                "start": start,
                "length": length,
            }))
        })
        .collect();
    if !text_styles.is_empty() {
        out["textStyles"] = json!(text_styles);
    }
    // 贴纸投影：官方接收面 = DataMessage.Sticker（packId hex/packKey base64
    // 官方桌面序列化同构），data 经既有附件指针投影（connector 媒体边界
    // 同口径：元数据 only，字节永不下载）。
    if let Some(sticker) = data_message.sticker.as_ref() {
        use base64::Engine as _;
        let pack_key = base64::engine::general_purpose::STANDARD
            .encode(sticker.pack_key.as_deref().unwrap_or_default());
        let mut value = json!({
            "packId": hex_encode(sticker.pack_id.as_deref().unwrap_or_default()),
            "packKey": pack_key,
            "stickerId": sticker.sticker_id.unwrap_or(0),
        });
        if let Some(emoji) = sticker.emoji.as_deref() {
            value["emoji"] = json!(emoji);
        }
        if let Some(data) = sticker.data.as_ref() {
            value["data"] = project_attachment_pointer(data);
        }
        out["sticker"] = value;
    }
    // Q3：poll 三臂收投影（PollCreate=24/PollTerminate=25/PollVote=26；官方
    // processDataMessage.preload.ts:395-441 行为对齐，backup 导出面 poll_record
    // 键形同族）。pollVote.targetAuthor 与 pin/unpin 同口径（二进制解析 +
    // author_numbers 命中转 E.164）；optionIndexes Set 去重保首现序（官方
    // new Set 同构）；voteCount 0 透出（`||0` 同语义）；空 optionIndexes =
    // 清票动作，照投空数组。
    if let Some(poll) = data_message.poll_create.as_ref() {
        out["poll"] = json!({
            "question": poll.question.clone().unwrap_or_default(),
            "options": poll.options,
            "allowMultiple": poll.allow_multiple.unwrap_or(false),
        });
    }
    if let Some(vote) = data_message.poll_vote.as_ref() {
        let mut option_indexes = Vec::with_capacity(vote.option_indexes.len());
        let mut seen = std::collections::HashSet::new();
        for index in &vote.option_indexes {
            if seen.insert(*index) {
                option_indexes.push(*index);
            }
        }
        out["pollVote"] = json!({
            "targetAuthor": targeted_author_json(
                vote.target_author_aci_binary.as_deref(),
                author_numbers,
            ),
            "targetSentTimestamp": vote.target_sent_timestamp.unwrap_or(0),
            "optionIndexes": option_indexes,
            "voteCount": vote.vote_count.unwrap_or(0),
        });
    }
    if let Some(terminate) = data_message.poll_terminate.as_ref() {
        out["pollTerminate"] =
            json!({ "targetSentTimestamp": terminate.target_sent_timestamp.unwrap_or(0) });
    }
    // Q4：groupCallUpdate 收投影（field 19，官方群通话开始/结束信令；收侧
    // background.preload.ts:3535-3543 只在群上下文消费 eraId）。eraId 缺席/
    // 空串整键缺席（宁缺勿错——官方 `eraId != null` 判面同域）。
    if let Some(update) = data_message.group_call_update.as_ref() {
        if let Some(era_id) = update.era_id.as_deref().filter(|era| !era.is_empty()) {
            out["groupCallUpdate"] = json!({ "eraId": era_id });
        }
    }
    // story 回复投影（Stories 接收批，E2）：storyContext → `storyReply
    // {authorAci, sentTimestamp}`（author binary 形态优先、string 兜底——
    // mentions 同款二进制优先纪律；两者皆缺/不可解析 = 整键缺席宁缺勿错，
    // 官方 storyReply 面无 author 不可路由）。
    if let Some(context) = data_message.story_context.as_ref() {
        if let Some(author) = story_reply_author(context) {
            out["storyReply"] = json!({
                "authorAci": author,
                "sentTimestamp": context.sent_timestamp.unwrap_or(0),
            });
        }
    }
    // P14 组 2 收投影缺：payment=20（官方 processDataMessage processPayment
    // 行为对齐——payment 缺席/item 缺席整键不在；notification 臂 =
    // {kind:"NOTIFICATION", note}（note 官方 `note ?? null` null 填位镜像；
    // receipt 字节官方事件面同样不投）；activation 臂官方折叠 kind 词——
    // type REQUEST→"ACTIVATION_REQUEST"、ACTIVATED→"ACTIVATION"（SCREAMING
    // 词表同 quote/contact 惯例），type 缺省/词表外 = 官方 fall-through
    // 整键不在。引擎 metadata-only 无 MobileCoin 节点/余额面，Amount 位
    // 官方通知事件同样不投）。
    if let Some(payment) = data_message.payment.as_ref() {
        let event = match payment.item.as_ref() {
            Some(presage::proto::data_message::payment::Item::Notification(notification)) => {
                let mut event = json!({ "kind": "NOTIFICATION" });
                event["note"] = match notification.note.as_deref() {
                    Some(note) => json!(note),
                    None => Value::Null,
                };
                Some(event)
            }
            Some(presage::proto::data_message::payment::Item::Activation(activation)) => {
                let kind = activation.r#type.and_then(|kind| {
                    presage::proto::data_message::payment::activation::Type::try_from(kind).ok()
                });
                match kind {
                    Some(presage::proto::data_message::payment::activation::Type::Request) => {
                        Some(json!({ "kind": "ACTIVATION_REQUEST" }))
                    }
                    Some(presage::proto::data_message::payment::activation::Type::Activated) => {
                        Some(json!({ "kind": "ACTIVATION" }))
                    }
                    None => None,
                }
            }
            None => None,
        };
        if let Some(event) = event {
            out["payment"] = event;
        }
    }
    // P14 组 2 收投影缺：giftBadge=22（官方 processGiftBadge 行为对齐——
    // presentation 缺席/空 = 官方 undefined 整键不在；在场 zkgroup 解析取
    // expiration（秒→毫秒，官方 `getReceiptExpirationTime() * SECOND` 同
    // 算）/level，连同 base64 原件与 receive 时点初态 state 词；解析失败
    // 官方是构造 throw 炸整条消息处理，引擎宁收勿丢红线不镜像——降级只投
    // 材料两键可补算，宽容面记 compat §5）。
    if let Some(gift_badge) = data_message.gift_badge.as_ref() {
        if let Some(value) = project_gift_badge(gift_badge) {
            out["giftBadge"] = value;
        }
    }
    out
}

/// 阅后即焚合法性校验（E2，官方 isValidTapToView 同构逐项）：body 空（官方
/// JS `if (body)` 同口径：空串为 falsy 即合法）∧ 恰 1 附件 ∧ 媒体族
/// image|video ∧ 无 quote/sticker/contact/preview。官方不查 mentions，接收
/// 面同构不查（发送面 E1 更严：mentions 禁发，compat §5）。
fn is_valid_view_once_row(data_message: &presage::proto::DataMessage) -> bool {
    if data_message
        .body
        .as_deref()
        .is_some_and(|body| !body.is_empty())
    {
        return false;
    }
    if data_message.attachments.len() != 1 {
        return false;
    }
    let Some(content_type) = data_message.attachments[0].content_type.as_deref() else {
        return false;
    };
    if !is_view_once_media(content_type) {
        return false;
    }
    if data_message.quote.is_some() || data_message.sticker.is_some() {
        return false;
    }
    data_message.contact.is_empty() && data_message.preview.is_empty()
}

/// story 回复作者解析（Stories 接收批，E2）：author_aci_binary 16B UUID
/// 优先、author_aci 字符串兜底；两者皆缺/不可解析 = None（调用方整键
/// 缺席，宁缺勿错）。
fn story_reply_author(context: &presage::proto::data_message::StoryContext) -> Option<String> {
    if let Some(bytes) = context.author_aci_binary.as_deref() {
        if let Ok(uuid) = presage::libsignal_service::prelude::Uuid::from_slice(bytes) {
            return Some(uuid.to_string());
        }
    }
    context.author_aci.clone().filter(|aci| !aci.is_empty())
}

/// P14 组 2 giftBadge=22 投影（官方 processGiftBadge 同形四键
/// {expiration(毫秒), level, receiptCredentialPresentation(标准 base64),
/// state}；官方 `id: undefined` JSON 面无键不投）。state 词 =
/// "UNOPENED"（桌面 GiftBadgeStates.Unopened 的 SCREAMING 投影形，词表
/// 大小写惯例同 quote/contact）——receive 时点恒初态，开启/兑换状态机归
/// 消费方。
fn project_gift_badge(gift_badge: &presage::proto::data_message::GiftBadge) -> Option<Value> {
    use base64::Engine as _;
    let presentation = gift_badge.receipt_credential_presentation.as_deref()?;
    if presentation.is_empty() {
        return None;
    }
    let mut out = serde_json::Map::new();
    if let Ok(receipt) =
        zkgroup::deserialize::<zkgroup::api::receipts::ReceiptCredentialPresentation>(presentation)
    {
        out.insert(
            "expiration".into(),
            json!(receipt.get_receipt_expiration_time().epoch_seconds() * 1000),
        );
        out.insert("level".into(), json!(receipt.get_receipt_level()));
    }
    out.insert(
        "receiptCredentialPresentation".into(),
        json!(base64::engine::general_purpose::STANDARD.encode(presentation)),
    );
    out.insert("state".into(), json!("UNOPENED"));
    Some(Value::Object(out))
}

/// Q2 contact 卡片投影（官方 processContact 展开位 + backup 导出面
/// contact_record 键形同族）：name 六子字段（空串省略）、number[]/email[]
/// （条目 {value, type, label?}——value 空整条丢弃官方 `!value → undefined`
/// 同口径、type 词表见 `contact_field_type_word`、label 非空才投）、
/// organization（非空）、avatar（指针走附件指针元数据面 + isProfile 恒投
/// Boolean；avatar 位在场但指针缺席 → 整键缺席）。全空 → None（整键缺席）。
fn project_contact_card(
    contact: &presage::proto::data_message::Contact,
) -> Option<serde_json::Value> {
    let mut face = serde_json::Map::new();
    if let Some(name) = contact.name.as_ref() {
        let mut projected = serde_json::Map::new();
        for (key, value) in [
            ("givenName", name.given_name.as_deref()),
            ("familyName", name.family_name.as_deref()),
            ("prefix", name.prefix.as_deref()),
            ("suffix", name.suffix.as_deref()),
            ("middleName", name.middle_name.as_deref()),
            ("nickname", name.nickname.as_deref()),
        ] {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                projected.insert(key.into(), json!(value));
            }
        }
        if !projected.is_empty() {
            face.insert("name".into(), serde_json::Value::Object(projected));
        }
    }
    let numbers: Vec<serde_json::Value> = contact
        .number
        .iter()
        .filter_map(|phone| {
            contact_field_entry(phone.value.as_deref(), phone.r#type, phone.label.as_deref())
        })
        .collect();
    if !numbers.is_empty() {
        face.insert("number".into(), json!(numbers));
    }
    let emails: Vec<serde_json::Value> = contact
        .email
        .iter()
        .filter_map(|email| {
            contact_field_entry(email.value.as_deref(), email.r#type, email.label.as_deref())
        })
        .collect();
    if !emails.is_empty() {
        face.insert("email".into(), json!(emails));
    }
    if let Some(organization) = contact
        .organization
        .as_deref()
        .filter(|organization| !organization.is_empty())
    {
        face.insert("organization".into(), json!(organization));
    }
    if let Some(avatar) = contact.avatar.as_ref() {
        if let Some(pointer) = avatar.avatar.as_ref() {
            face.insert(
                "avatar".into(),
                json!({
                    "avatar": project_attachment_pointer(pointer),
                    // 官方 Boolean(item.avatar.isProfile) 同构：缺席 = false。
                    "isProfile": avatar.is_profile.unwrap_or(false),
                }),
            );
        }
    }
    (!face.is_empty()).then_some(serde_json::Value::Object(face))
}

/// contact number/email 条目（backup 导出面 contact_field_record 同构；data
/// 面 value/label 均为 optional，缺席=value 空）。
fn contact_field_entry(
    value: Option<&str>,
    field_type: Option<i32>,
    label: Option<&str>,
) -> Option<serde_json::Value> {
    let value = value?;
    if value.is_empty() {
        return None;
    }
    let mut record = json!({
        "value": value,
        "type": contact_field_type_word(field_type),
    });
    if let Some(label) = label.filter(|label| !label.is_empty()) {
        record["label"] = serde_json::Value::String(label.to_string());
    }
    Some(record)
}

/// contact 条目 type 词（官方 contactFieldTypeName 同构，backup 导出面同
/// 词表）：MOBILE/WORK/CUSTOM 识别；HOME/缺席/未知归 HOME。phone/email 两
/// 枚举同编号（HOME=1/MOBILE=2/WORK=3/CUSTOM=4），词源统一取 phone 枚举。
fn contact_field_type_word(field_type: Option<i32>) -> &'static str {
    use presage::proto::data_message::contact::phone::Type;
    match field_type.and_then(|value| Type::try_from(value).ok()) {
        Some(Type::Mobile) => "MOBILE",
        Some(Type::Work) => "WORK",
        Some(Type::Custom) => "CUSTOM",
        _ => "HOME",
    }
}

/// DeleteForMe 投影（Q6② + P15a 项1，compat 批块）：messageDeletes 臂逐组
/// 投影——`[{conversationId?, messages: [{author, timestamp}]}]`（组无
/// conversation 照投、conversationId 键省略——Q6② 既有口径不动）；
/// conversationDeletes 臂（官方 handleDeleteForMeSync 同位
/// MessageReceiver.preload.ts:3658-3697）——`[{conversationId,
/// isFullDelete, mostRecentMessages, mostRecentNonExpiringMessages}]`，
/// conversation 缺席或 mostRecentMessages 清空整组丢（官方 warn-drop 同
/// 口径），isFullDelete 官方 Boolean() 恒布尔；localOnlyConversationDeletes
/// 臂（:3699-3722）——`[{conversationId}]`，conversation 缺席丢。
/// attachmentDeletes 臂不投（§5 gap 登记）。author 二进制（ACI/PNI 16/17B）
/// 优先、字符串/e164 直通；author 不可解析条目丢弃（vendored store 面
/// warn-and-continue 同口径）。全部臂皆无有效条目 → None（调用方整信封
/// 丢弃，官方「Nothing found in sync message」throw 同口径）。
fn project_delete_for_me(delete: &presage::proto::sync_message::DeleteForMe) -> Option<Value> {
    let mut face = serde_json::Map::new();

    let groups: Vec<Value> = delete
        .message_deletes
        .iter()
        .filter_map(|group| {
            let messages: Vec<Value> = group
                .messages
                .iter()
                .filter_map(project_delete_for_me_message)
                .collect();
            if messages.is_empty() {
                return None;
            }
            let mut item = serde_json::Map::new();
            if let Some(conversation) = delete_for_me_conversation_id(&group.conversation) {
                item.insert("conversationId".into(), json!(conversation));
            }
            item.insert("messages".into(), Value::Array(messages));
            Some(Value::Object(item))
        })
        .collect();
    if !groups.is_empty() {
        face.insert("messageDeletes".into(), Value::Array(groups));
    }

    let conversation_deletes: Vec<Value> = delete
        .conversation_deletes
        .iter()
        .filter_map(|group| {
            let conversation = delete_for_me_conversation_id(&group.conversation)?;
            let most_recent: Vec<Value> = group
                .most_recent_messages
                .iter()
                .filter_map(project_delete_for_me_message)
                .collect();
            if most_recent.is_empty() {
                return None;
            }
            let non_expiring: Vec<Value> = group
                .most_recent_non_expiring_messages
                .iter()
                .filter_map(project_delete_for_me_message)
                .collect();
            let mut item = serde_json::Map::new();
            item.insert("conversationId".into(), json!(conversation));
            item.insert(
                "isFullDelete".into(),
                json!(group.is_full_delete.unwrap_or(false)),
            );
            item.insert("mostRecentMessages".into(), Value::Array(most_recent));
            item.insert(
                "mostRecentNonExpiringMessages".into(),
                Value::Array(non_expiring),
            );
            Some(Value::Object(item))
        })
        .collect();
    if !conversation_deletes.is_empty() {
        face.insert(
            "conversationDeletes".into(),
            Value::Array(conversation_deletes),
        );
    }

    let local_only: Vec<Value> = delete
        .local_only_conversation_deletes
        .iter()
        .filter_map(|group| {
            let conversation = delete_for_me_conversation_id(&group.conversation)?;
            let mut item = serde_json::Map::new();
            item.insert("conversationId".into(), json!(conversation));
            Some(Value::Object(item))
        })
        .collect();
    if !local_only.is_empty() {
        face.insert(
            "localOnlyConversationDeletes".into(),
            Value::Array(local_only),
        );
    }

    (!face.is_empty()).then_some(Value::Object(face))
}

/// DeleteForMe AddressableMessage 投影单点：{author, timestamp}（author
/// 二进制优先、字符串/e164 直通、不可解析 None；timestamp 缺省 0——Q6②
/// 既有口径）。
fn project_delete_for_me_message(message: &presage::proto::AddressableMessage) -> Option<Value> {
    let author = match message.author.as_ref()? {
        presage::proto::addressable_message::Author::AuthorServiceId(id) => {
            Some(id.clone()).filter(|id| !id.is_empty())
        }
        presage::proto::addressable_message::Author::AuthorServiceIdBinary(bytes) => {
            ServiceId::parse_from_service_id_binary(bytes).map(|sid| sid.service_id_string())
        }
        presage::proto::addressable_message::Author::AuthorE164(number) => {
            Some(number.clone()).filter(|number| !number.is_empty())
        }
    }?;
    Some(json!({
        "author": author,
        "timestamp": message.sent_timestamp.unwrap_or(0),
    }))
}

/// P15a 项3 contacts sync 投影（compat §5）：解析后的联系人记录上抛事件
/// ——官方 handleContacts（MessageReceiver.preload.ts:3934 blob 消费面）
/// → ContactSyncEvent → doContactSync（services/contactSync.preload.ts
/// upsert aci/number/name/inboxPosition/expireTimer 族）同位；blob 本体
/// 不投（二进制），投解析产物。记录 = vendored presage 解析+落库成功条目
/// （parse 失败上游 filter_map 丢、store save 失败 break 后不再投影——
/// 如实登记）；avatar 字节不投影（gap 登记后续批）。expireTimer/
/// expireTimerVersion/inboxPosition 0 与缺席不可分（presage u32 模型），
/// 0 不投（§5 记档）。空记录照投事件（complete 旗标即信号，官方
/// contactSync:complete 事件空 blob 照发同构）。
fn project_contacts_sync(
    account: &str,
    sync: &presage::model::messages::ContactSync,
) -> Vec<Value> {
    let metadata = &sync.content.metadata;
    let mut records = Vec::new();
    for contact in &sync.contacts {
        let mut entry = serde_json::Map::new();
        entry.insert("aci".into(), json!(contact.uuid.to_string()));
        if let Some(number) = contact.phone_number.as_ref() {
            entry.insert("number".into(), json!(e164_string(number)));
        }
        if !contact.name.is_empty() {
            entry.insert("name".into(), json!(contact.name));
        }
        if contact.expire_timer != 0 {
            entry.insert("expireTimer".into(), json!(contact.expire_timer));
        }
        if contact.expire_timer_version != 0 {
            entry.insert(
                "expireTimerVersion".into(),
                json!(contact.expire_timer_version),
            );
        }
        if contact.inbox_position != 0 {
            entry.insert("inboxPosition".into(), json!(contact.inbox_position));
        }
        records.push(Value::Object(entry));
    }
    let envelope = json!({
        "sourceUuid": metadata.sender.service_id_string(),
        "sourceDevice": u32::from(metadata.sender_device),
        "timestamp": metadata.client_timestamp.timestamp_millis().max(0) as u64,
        "syncMessage": {
            "contacts": {
                "complete": sync.complete,
                "records": records,
            }
        }
    });
    vec![json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": { "account": account, "envelope": envelope },
    })]
}

/// DeleteForMe 会话标识（ConversationIdentifier oneof）：serviceId/binary →
/// UUID 串（PNI 17B 同解析）、groupId → master key 标准 base64（引擎 groupId
/// 口径）、e164 → 号码原样；缺席/空/不可解析 → None（键省略）。
fn delete_for_me_conversation_id(
    conversation: &Option<presage::proto::ConversationIdentifier>,
) -> Option<String> {
    use base64::Engine as _;
    match conversation.as_ref()?.identifier.as_ref()? {
        presage::proto::conversation_identifier::Identifier::ThreadServiceId(id) => {
            Some(id.clone()).filter(|id| !id.is_empty())
        }
        presage::proto::conversation_identifier::Identifier::ThreadGroupId(bytes) => {
            (!bytes.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        presage::proto::conversation_identifier::Identifier::ThreadE164(number) => {
            Some(number.clone()).filter(|number| !number.is_empty())
        }
        presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes) => {
            ServiceId::parse_from_service_id_binary(bytes).map(|sid| sid.service_id_string())
        }
    }
}

/// story 投影（Stories 接收批，E2，compat §5 Stories 接收批块）：proto 只
/// 投影不发明，缺席键不在。textAttachment{text, textStyle?, preview{image
/// 指针, url?, title?}}/fileAttachment → 附件指针投影（connector 媒体边界
/// 同口径：元数据 only，字节永不下载）；allowsReplies 恒投（proto bool 缺
/// 席 = 线上缺省 false 如实）；profileKey/group 群 story 母钥 base64（有值
/// 才投）。textStyle 缺席/未知枚举值不投键（词表纪律同 textStyles 收投
/// 影）；DEFAULT=0 线上在场照投词——与发送面缺省省略的不对称是线上形状的
/// 如实两面。preview.url/preview.title 引擎扩展键往返面（2026-10-09 批，
/// 发送面 previewUrl/previewTitle 对端回声）：Some 且非空才投，同一
/// preview 对象内与 image 并列（description 官方 story 预览面未消
/// 费，仍不投；date 已于 P14 Preview.date 两端批补投）。P14 组 4：
/// bodyRanges=6 分流 `mentions[]`/`textStyles[]`（dataMessage 同构——
/// mention 二进制优先 + author_numbers E.164 映射；style 词表 NONE 丢/
/// 未知丢，start+length 超 story 正文 UTF-16 长度的形状坏条目丢弃）与
/// 色位投影（textForegroundColor/textBackgroundColor u32 原样、background
/// oneof → `background {gradient{startColor?, endColor?, angle?, colors[],
/// positions[]}}` 或 `background {color}`，官方 SendMessage TextAttachment
/// Params 同形键）。无 attachment 位
/// = 官方发送面不可能产出的 malformed 行，
/// 返回 None（调用方整信封丢弃）。
fn project_story_message(
    story: &presage::proto::StoryMessage,
    author_numbers: &HashMap<String, String>,
) -> Option<Value> {
    use base64::Engine as _;
    let attachment = story.attachment.as_ref()?;
    let mut out = serde_json::Map::new();
    match attachment {
        presage::proto::story_message::Attachment::TextAttachment(text) => {
            let mut value = serde_json::Map::new();
            if let Some(body) = text.text.as_deref() {
                value.insert("text".into(), json!(body));
            }
            if let Some(style) = text.text_style {
                if let Ok(word) = presage::proto::text_attachment::Style::try_from(style) {
                    value.insert("textStyle".into(), json!(word.as_str_name()));
                }
            }
            // P14 组 4：story 正文样式分流（dataMessage bodyRanges 同构——
            // mentions 臂 E.164 映射 + textStyles 臂五值词表；形状坏条目
            // 丢弃纪律同源）。正文长度闸 = textAttachment.text UTF-16。
            let mentions: Vec<Value> = story
                .body_ranges
                .iter()
                .filter_map(|range| {
                    let uuid = match range.associated_value.as_ref()? {
                        presage::proto::body_range::AssociatedValue::MentionAci(aci) => aci.clone(),
                        presage::proto::body_range::AssociatedValue::MentionAciBinary(bytes) => {
                            presage::libsignal_service::prelude::Uuid::from_slice(bytes)
                                .ok()?
                                .to_string()
                        }
                        presage::proto::body_range::AssociatedValue::Style(_) => return None,
                    };
                    Some(json!({
                        "number": author_numbers.get(&uuid).cloned().unwrap_or(uuid),
                        "start": range.start.unwrap_or(0),
                        "length": range.length.unwrap_or(0),
                    }))
                })
                .collect();
            if !mentions.is_empty() {
                value.insert("mentions".into(), json!(mentions));
            }
            let text_utf16_len = text
                .text
                .as_deref()
                .map(|body| body.encode_utf16().count())
                .unwrap_or(0);
            let text_styles: Vec<Value> = story
                .body_ranges
                .iter()
                .filter_map(|range| {
                    let style = match range.associated_value.as_ref()? {
                        presage::proto::body_range::AssociatedValue::Style(value) => *value,
                        _ => return None,
                    };
                    let word = presage::proto::body_range::Style::try_from(style)
                        .ok()?
                        .as_str_name();
                    if word == "NONE" {
                        return None;
                    }
                    let start = range.start.unwrap_or(0);
                    let length = range.length.unwrap_or(0);
                    if start as u64 + length as u64 > text_utf16_len as u64 {
                        return None;
                    }
                    Some(json!({
                        "style": word,
                        "start": start,
                        "length": length,
                    }))
                })
                .collect();
            if !text_styles.is_empty() {
                value.insert("textStyles".into(), json!(text_styles));
            }
            // P14 组 4：色位/背景面（官方 TextAttachment.Params 同形键——
            // u32 颜色数值原样透出；background oneof gradient|color 恰一，
            // gradient 弃用位 startColor/endColor 在场照投官方同款）。
            if let Some(color) = text.text_foreground_color {
                value.insert("textForegroundColor".into(), json!(color));
            }
            if let Some(color) = text.text_background_color {
                value.insert("textBackgroundColor".into(), json!(color));
            }
            match text.background.as_ref() {
                Some(presage::proto::text_attachment::Background::Gradient(gradient)) => {
                    let mut face = serde_json::Map::new();
                    if let Some(start) = gradient.start_color {
                        face.insert("startColor".into(), json!(start));
                    }
                    if let Some(end) = gradient.end_color {
                        face.insert("endColor".into(), json!(end));
                    }
                    if let Some(angle) = gradient.angle {
                        face.insert("angle".into(), json!(angle));
                    }
                    face.insert("colors".into(), json!(gradient.colors));
                    face.insert("positions".into(), json!(gradient.positions));
                    value.insert(
                        "background".into(),
                        json!({ "gradient": Value::Object(face) }),
                    );
                }
                Some(presage::proto::text_attachment::Background::Color(color)) => {
                    value.insert("background".into(), json!({ "color": color }));
                }
                None => {}
            }
            if let Some(preview) = text.preview.as_ref() {
                let mut face = serde_json::Map::new();
                if let Some(url) = preview.url.as_deref().filter(|url| !url.is_empty()) {
                    face.insert("url".into(), json!(url));
                }
                if let Some(title) = preview.title.as_deref().filter(|title| !title.is_empty()) {
                    face.insert("title".into(), json!(title));
                }
                // P14 Preview.date 两端批：官方 story 预览同样走 processPreview
                // 消费 date（MessageReceiver.preload.ts:2183 → processDataMessage
                // .preload.ts:297-310），与 dataMessage preview.date 同语义
                //（>0 才投）。
                if let Some(date) = preview.date.filter(|date| *date > 0) {
                    face.insert("date".into(), json!(date));
                }
                if let Some(image) = preview.image.as_ref() {
                    face.insert("image".into(), project_attachment_pointer(image));
                }
                if !face.is_empty() {
                    value.insert("preview".into(), Value::Object(face));
                }
            }
            out.insert("textAttachment".into(), Value::Object(value));
        }
        presage::proto::story_message::Attachment::FileAttachment(pointer) => {
            out.insert("fileAttachment".into(), project_attachment_pointer(pointer));
        }
    }
    out.insert(
        "allowsReplies".into(),
        json!(story.allows_replies.unwrap_or(false)),
    );
    if let Some(key) = story.profile_key.as_deref().filter(|key| !key.is_empty()) {
        out.insert(
            "profileKey".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(key)),
        );
    }
    if let Some(key) = story
        .group
        .as_ref()
        .and_then(|group| group.master_key.as_deref())
        .filter(|key| !key.is_empty())
    {
        out.insert(
            "group".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(key)),
        );
    }
    Some(Value::Object(out))
}

/// 附件投影：v1 基础面（connector §4.13/§6.7 媒体边界：字节永不下载）+
/// v2 locator 指针面（2026-10-07，connector 契约 1.61 §4.54 引擎侧缺口闭合，
/// live receive 信封数据面）。`id` = AttachmentPointer.client_uuid（hyphenated
/// UUID，signal-cli `id` 位）。
///
/// locator 面（proto 只投影不发明，缺/空一律省略键；导出侧
/// `history.rs::project_pointer_common` 同款、engine-compat 1.56 消费形状同
/// 形——connector `normalized_attachment` ingest 落 schema-19 live 列，
/// `attachments.download` 据此走其 §4.38 step-3 transit CDN 链）：
/// - `key` ← AttachmentPointer.key 标准 base64（附件 64B 加密材料，
///   只经信封内存中转，引擎不持久化）；
/// - `cdnKey` ← attachment_identifier CdnKey 字符串（legacy CdnId(fixed64)
///   指针无字符串形态——该键缺席，下载资格闸由消费方判，宁缺勿错不发明）；
/// - `cdnNumber` ← AttachmentPointer.cdn_number（Some(0) 是合法 CDN0 原样投）；
/// - `uploadTimestamp` ← AttachmentPointer.upload_timestamp（0 无意义省略，
///   官方 falsy 同款）；
/// - `digest` ← AttachmentPointer.digest 标准 base64（whole-body SHA-256，
///   transit 终闸输入）。钉定 rev 线上指针无 plaintextHash 面（backup transit
///   指针专属字段），诚实缺席不发明——仅 digest 腿即 connector TransitFace
///   可下载形态。
/// - 指针全无定位材料（上游钉定面不含定位字段的形态）→ 整个
///   `locator` 键缺席，wire 与旧版逐字节同形。
fn project_attachment_pointer(pointer: &presage::proto::AttachmentPointer) -> Value {
    use base64::Engine as _;
    use presage::proto::attachment_pointer::Flags;
    let id = pointer
        .client_uuid
        .as_deref()
        .and_then(|bytes| presage::libsignal_service::prelude::Uuid::from_slice(bytes).ok())
        .map(|uuid| uuid.to_string())
        .unwrap_or_default();
    let mut value = json!({
        "id": id,
        "contentType": pointer.content_type.clone().unwrap_or_default(),
    });
    if let Some(filename) = pointer.file_name.as_deref() {
        value["filename"] = json!(filename);
    }
    if let Some(size) = pointer.size {
        value["size"] = json!(size);
    }
    if let Some(width) = pointer.width {
        value["width"] = json!(width);
    }
    if let Some(height) = pointer.height {
        value["height"] = json!(height);
    }
    if let Some(caption) = pointer.caption.as_deref() {
        value["caption"] = json!(caption);
    }
    // P14 live 投影缺（导出面 project_pointer_common 已有，live 接收信封
    // 补齐同形）：blurHash 非空才投（空串零信息量）；incrementalMac+
    // chunkSize 同进同退（官方 filePointers.preload.ts:84-87 两者齐备才
    // 保留同构——流式完整性校验材料，mac 标准 base64，chunkSize >0）。
    if let Some(blur_hash) = pointer.blur_hash.as_deref().filter(|hash| !hash.is_empty()) {
        value["blurHash"] = json!(blur_hash);
    }
    if let (Some(mac), Some(chunk_size)) = (pointer.incremental_mac.as_deref(), pointer.chunk_size)
    {
        if !mac.is_empty() && chunk_size > 0 {
            use base64::Engine as _;
            value["incrementalMac"] = json!(base64::engine::general_purpose::STANDARD.encode(mac));
            value["chunkSize"] = json!(chunk_size);
        }
    }
    let voice_note = pointer
        .flags
        .is_some_and(|flags| flags & Flags::VoiceMessage as u32 != 0);
    value["isVoiceNote"] = json!(voice_note);
    // P14 组 3 收投影缺（附件 flags）：BORDERLESS=2/GIF=8 两 flag 位恒投
    // 布尔（镜像 isVoiceNote 先例——无 flag 位 = false，三布尔同形消费面
    // 无需判键）。官方 isGIF（Attachment.std.ts:321）另有 contentType
    // video/* 附加闸，引擎 flag-only 如实投位：媒体类自判归消费方（宁多
    // 投不误判；GIF 边界官方语义登记 §5）。
    let is_borderless = pointer
        .flags
        .is_some_and(|flags| flags & Flags::Borderless as u32 != 0);
    let is_gif = pointer
        .flags
        .is_some_and(|flags| flags & Flags::Gif as u32 != 0);
    value["isBorderless"] = json!(is_borderless);
    value["isGif"] = json!(is_gif);
    let mut locator = serde_json::Map::new();
    if let Some(key) = pointer.key.as_deref().filter(|key| !key.is_empty()) {
        locator.insert(
            "key".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(key)),
        );
    }
    if let Some(presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey(cdn_key)) =
        pointer.attachment_identifier.as_ref()
    {
        if !cdn_key.is_empty() {
            locator.insert("cdnKey".into(), Value::String(cdn_key.clone()));
        }
    }
    if let Some(cdn_number) = pointer.cdn_number {
        locator.insert("cdnNumber".into(), json!(cdn_number));
    }
    if let Some(timestamp) = pointer.upload_timestamp.filter(|ts| *ts > 0) {
        locator.insert("uploadTimestamp".into(), json!(timestamp));
    }
    if let Some(digest) = pointer
        .digest
        .as_deref()
        .filter(|digest| !digest.is_empty())
    {
        locator.insert(
            "digest".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(digest)),
        );
    }
    if !locator.is_empty() {
        value["locator"] = Value::Object(locator);
    }
    value
}

// ---------------------------------------------------------------------------
// ADR 0005 S2：backup5 链接流程与存量历史迁移下载
// ---------------------------------------------------------------------------

/// 链接主流程：绕过 `Manager::link_secondary_device`（其内部丢弃
/// `ephemeral_backup_key`，且 `RegistrationData` 的 password/profile_key 为
/// pub(crate) 无法外部构造），直调 provisioning 层 `link_device`；QR URL 追加
/// `capabilities=backup5`（官方 Provisioner 语义：手机读到声明才发起存量历史
/// 迁移上传；URL 文本层追加，connector 展示面零改动）。拿到
/// `NewDeviceRegistration` 后按官方 store 落库格式经 serde 数据面构造
/// `RegistrationData`，回归既有 `load_registered` 收发管线。key 命中时后台
/// best-effort 下载迁移档案，不阻塞链接成功（ADR 0005 §S2 定案）。
///
/// 纪律：失败即整会话作废（删目录 + Err）——provisioning WS 约 90s 服务端
/// 空闲超时，旧 QR 不可复用，重试由 connector 重新 startLink。
async fn link_flow(
    mut store: SqliteStore,
    url_tx: futures::channel::oneshot::Sender<url::Url>,
    store_dir: PathBuf,
    transfer_progress: transfer_progress::TransferProgress,
) -> Result<String, ()> {
    let mut rng = rng();
    let password = Alphanumeric.sample_string(&mut rng, 24);
    let push_service = PushService::new(SignalServers::Production, None, USER_AGENT);
    let (event_tx, mut event_rx) = futures::channel::mpsc::channel(1);

    let (link_outcome, registration) = futures::future::join(
        link_device(
            &mut store.aci_protocol_store(),
            &mut store.pni_protocol_store(),
            &mut rng,
            push_service,
            &password,
            PLACEHOLDER_DEVICE_NAME,
            event_tx,
        ),
        async move {
            let url = match event_rx.next().await {
                Some(SecondaryDeviceProvisioning::Url(url)) => url,
                _ => return Err(()),
            };
            let mut qr_url = url;
            qr_url
                .query_pairs_mut()
                .append_pair("capabilities", "backup5");
            let _ = url_tx.send(qr_url);
            match event_rx.next().await {
                Some(SecondaryDeviceProvisioning::NewDeviceRegistration(data)) => Ok(data),
                _ => Err(()),
            }
        },
    )
    .await;

    link_outcome.map_err(|_| ())?;
    let NewDeviceRegistration {
        phone_number,
        device_id,
        registration_id,
        pni_registration_id,
        service_ids,
        aci_private_key,
        aci_public_key,
        pni_private_key,
        pni_public_key,
        profile_key,
        account_entropy_pool,
        ephemeral_backup_key,
    } = registration.map_err(|_| ())?;

    // serde 数据面构造（官方 store 落库格式）：pub(crate) 字段的唯一合法
    // 构造路径；键面与序列化形态由往返单测锁死。
    let number = e164_string(&phonenumber_from_signal(&phone_number));
    let servers = serde_json::to_value(SignalServers::Production).map_err(|_| ())?;
    let phone = serde_json::to_value(phonenumber_from_signal(&phone_number)).map_err(|_| ())?;
    use base64::Engine as _;
    let profile_key_b64 = base64::engine::general_purpose::STANDARD.encode(profile_key.bytes);
    // 注意 from_str 而非 from_value：上游 serde_profile_key 用借用反序列化
    // （<&str>::deserialize），owned Value 路径直接报 "expected a borrowed
    // string"；字符串 deserializer 才满足借用生命周期。
    let registration_data: RegistrationData = serde_json::from_str(
        &serde_json::to_string(&json!({
            "signal_servers": servers,
            "device_name": PLACEHOLDER_DEVICE_NAME,
            "phone_number": phone,
            // ServiceIds flatten 后 aci 的 JSON 键名是 "uuid"（上游 serde rename）。
            "uuid": service_ids.aci,
            "pni": service_ids.pni,
            "password": password,
            "device_id": Some(u32::from(device_id)),
            "registration_id": registration_id,
            "pni_registration_id": Some(pni_registration_id),
            "profile_key": profile_key_b64,
        }))
        .map_err(|_| ())?,
    )
    .map_err(|_| ())?;

    // 落库顺序与官方 link_secondary_device 一致。
    store
        .set_aci_identity_key_pair(IdentityKeyPair::new(aci_public_key, aci_private_key))
        .await
        .map_err(|_| ())?;
    store
        .set_pni_identity_key_pair(IdentityKeyPair::new(pni_public_key, pni_private_key))
        .await
        .map_err(|_| ())?;
    let master_key = account_entropy_pool.as_ref().and_then(|aep| {
        presage::libsignal_service::prelude::MasterKey::from_slice(aep.derive_svr_key().as_slice())
            .ok()
    });
    store
        .store_master_key(master_key.as_ref())
        .await
        .map_err(|_| ())?;
    store
        .store_account_entropy_pool(account_entropy_pool.as_ref())
        .await
        .map_err(|_| ())?;
    store
        .save_registration_data(&registration_data)
        .await
        .map_err(|_| ())?;

    // 回归既有收发管线的门禁：标准 load 路径能读回（本任务内 Manager 即弃，
    // 接收管线在 finish_link 后由 spawn_pipeline 自行 load）。
    Manager::load_registered(store).await.map_err(|_| ())?;

    if let Some(backup_key) = ephemeral_backup_key {
        let credentials = ServiceCredentials {
            aci: Some(service_ids.aci),
            pni: Some(service_ids.pni),
            phonenumber: phone_number,
            password: Some(password),
            device_id: Some(device_id),
        };
        // 导出面 future 非 Send（上游 FramesReader 持 Rc 状态）：与链接/
        // 接收任务同策略，LocalSet 承载（main 装配提供上下文）。
        tokio::task::spawn_local(download_transfer_archive(
            store_dir,
            service_ids.aci,
            backup_key,
            credentials,
            number.clone(),
            transfer_progress,
        ));
    }
    Ok(number)
}

/// 存量历史迁移下载（best-effort）：`transfer_archive` 长轮询等手机上传
/// 就绪（204 重试上限 10 次、间隔 3s、服务端单次挂起 60s），200 后从 CDN
/// 拉档案，`history::export_archive` 解密逐帧导出 NDJSON 到账号 store 目录
/// （`history-import.ndjson`，S3 connector 消费）。任何一步失败静默放弃并
/// 清理半成品——失败形态即 NDJSON 缺席，消费方以文件存在性判定导入结果；
/// 本任务不产生 stderr（compat §1），不影响链接成功。
///
/// 下载段边读边累计进程内进度快照（`account` 名下的 `progress` 观测面，
/// 契约 1.53 §4.38 扩展）；无论成败，任务收尾清快照——快照缺席即引擎
/// 无在途下载，消费方不猜。
async fn download_transfer_archive(
    store_dir: PathBuf,
    aci: presage::libsignal_service::prelude::Uuid,
    backup_key: [u8; 32],
    credentials: ServiceCredentials,
    account: String,
    transfer_progress: transfer_progress::TransferProgress,
) {
    transfer_progress.begin(&account, None);
    let push = PushService::new(SignalServers::Production, Some(credentials), USER_AGENT);

    let manifest: Value = {
        let mut polls = 0u32;
        loop {
            let response = match push.request(
                Method::GET,
                Endpoint::service("v1/devices/transfer_archive?timeout=60"),
                HttpAuthOverride::NoOverride,
            ) {
                Ok(builder) => builder,
                Err(_) => {
                    transfer_progress.clear(&account);
                    return;
                }
            };
            let response = match response.send().await {
                Ok(response) => response,
                Err(_) => {
                    transfer_progress.clear(&account);
                    return;
                }
            };
            match response.status().as_u16() {
                200 => {
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            transfer_progress.clear(&account);
                            return;
                        }
                    };
                    match serde_json::from_slice(&bytes) {
                        Ok(value) => break value,
                        Err(_) => {
                            transfer_progress.clear(&account);
                            return;
                        }
                    }
                }
                204 if polls >= 9 => {
                    transfer_progress.clear(&account);
                    return;
                }
                204 => {
                    polls += 1;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                _ => {
                    transfer_progress.clear(&account);
                    return;
                }
            }
        }
    };
    let (Some(cdn), Some(key)) = (
        manifest["cdn"].as_u64().map(|value| value as u32),
        manifest["key"].as_str().map(str::to_owned),
    ) else {
        transfer_progress.clear(&account);
        return;
    };

    let response = match push.request(
        Method::GET,
        Endpoint::cdn(cdn, format!("attachments/{key}")),
        HttpAuthOverride::NoOverride,
    ) {
        Ok(builder) => builder,
        Err(_) => {
            transfer_progress.clear(&account);
            return;
        }
    };
    let response = match response.send().await {
        Ok(response) => response,
        Err(_) => {
            transfer_progress.clear(&account);
            return;
        }
    };
    // content-length 缺席（chunked/代理剥头）是诚实形态：total=None，只报
    // stage（官方 Process 段同理只有文案没有数字）。
    let bytes_total = response.content_length().filter(|total| *total > 0);
    transfer_progress.begin(&account, bytes_total);

    // 块读（reqwest 流式 chunk）：边读边累计快照，不整包驻内存。
    let mut archive_bytes = Vec::new();
    {
        use futures::StreamExt;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    transfer_progress.add(&account, chunk.len() as u64);
                    archive_bytes.extend_from_slice(&chunk);
                }
                Err(_) => {
                    transfer_progress.clear(&account);
                    return;
                }
            }
        }
    }

    let archive_path = store_dir.join("transfer-archive.bin");
    if std::fs::write(&archive_path, &archive_bytes).is_err() {
        transfer_progress.clear(&account);
        return;
    }
    // 导出（解密+分帧）是本地计算不再有下载字节：快照在导出前清掉——
    // connector 观测的 progress 只覆盖官方 Download 段，Process 段由
    // connector 自己的导入 pass 投影（§4.38 progress 语义）。
    transfer_progress.clear(&account);
    let ndjson_path = store_dir.join("history-import.ndjson");
    let outcome = std::fs::File::create(&ndjson_path)
        .map_err(|_| ())
        .map(std::io::BufWriter::new);
    let mut writer = match outcome {
        Ok(writer) => writer,
        Err(_) => {
            let _ = std::fs::remove_file(&archive_path);
            return;
        }
    };
    let exported =
        crate::history::export_archive(&archive_path, aci, backup_key, &mut writer).await;
    let _ = std::io::Write::flush(&mut writer);
    drop(writer);
    if exported.is_err() {
        let _ = std::fs::remove_file(&ndjson_path);
    }
    let _ = std::fs::remove_file(&archive_path);
}

// ---------------------------------------------------------------------------
// real 模式共用件
// ---------------------------------------------------------------------------

/// 官方 pinMessage=27 proto 组装（SignalService.proto §27；对齐官方桌面
/// SendMessage.preload.ts：`pinDurationSeconds != null ? {seconds} : {forever}`）。
fn pin_message_proto(
    target_author_aci_bytes: Vec<u8>,
    target_sent_timestamp: u64,
    pin_duration_seconds: Option<u32>,
) -> presage::proto::data_message::PinMessage {
    use presage::proto::data_message::pin_message::PinDuration;
    presage::proto::data_message::PinMessage {
        target_author_aci_binary: Some(target_author_aci_bytes),
        target_sent_timestamp: Some(target_sent_timestamp),
        pin_duration: Some(match pin_duration_seconds {
            Some(seconds) => PinDuration::PinDurationSeconds(seconds),
            None => PinDuration::PinDurationForever(true),
        }),
    }
}

/// ACI 字符串 → 16 字节原始 UUID（官方 targetAuthorAciBinary 形态）。
fn aci_to_bytes(aci: &str) -> Option<Vec<u8>> {
    let uuid = presage::libsignal_service::prelude::Uuid::parse_str(aci.trim()).ok()?;
    Some(uuid.into_bytes().to_vec())
}

/// 群组 id（signal-cli 同源：master key 标准 base64）→ 32 字节 master key。
pub(crate) fn decode_group_id(group_id: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let key = base64::engine::general_purpose::STANDARD
        .decode(group_id.trim())
        .ok()?;
    (key.len() == 32).then_some(key)
}

/// listGroups 单群读过滤判定（P15b 审计项5：可选 groupId 过滤参数，
/// connector §4.53/§4.57/§4.62 fold 读已派发 `groupId: [id]` 数组形）：
/// filter 缺席 = 全放行（全表旧行为）；在场 = master key 逐字节命中任一
/// id 才放行——well-formed 但 store 不在的 id 掉出结果（结果可空，调用方
/// fold 侧 `first()` → 空集诚实降级），不做错误面（读面只投影 store 实有）。
fn group_key_passes_filter(master_key: &[u8], filter: Option<&[Vec<u8>]>) -> bool {
    match filter {
        None => true,
        Some(ids) => ids.iter().any(|id| id.as_slice() == master_key),
    }
}

/// 群 typing 体（止血批④）：groupId 携带 32 字节群 master key（proto tag
/// 3；官方 Desktop conversations.preload.ts:1510-1516 群 typing 同语义——
/// 群体缺 groupId 会被接收方当直聊 typing 识别失败）。
fn typing_group_body(
    master_key: &[u8],
    timestamp: u64,
    stop: bool,
) -> presage::proto::TypingMessage {
    presage::proto::TypingMessage {
        timestamp: Some(timestamp),
        action: Some(if stop {
            presage::proto::typing_message::Action::Stopped as i32
        } else {
            presage::proto::typing_message::Action::Started as i32
        }),
        group_id: Some(master_key.to_vec()),
    }
}

/// 群发送 per-recipient 结果向量（B 批群发送语义，compat §3 wire additive）：
/// `(收件人 ServiceId, 该收件人失败)`——`None` = 已送达。单个成员失败
/// （含未注册）不再整体炸批（官方 per-recipient 结果同语义），transcript
/// 照落库。
pub(crate) type GroupSendOutcomes = Vec<(
    ServiceId,
    Option<presage::libsignal_service::sender::MessageSenderError>,
)>;

/// 群发送 per-recipient 失败词表（connector additive 消费面，compat §3）：
/// NotFound=未注册（signal-cli UnregisteredFailure 同语义）、
/// UntrustedIdentity=身份钥不匹配、Unauthorized=凭据失效（SendError::Unbound
/// 同因）、其余（网络/协议）折叠 NetworkFailure。
fn group_send_failure_word(
    error: &presage::libsignal_service::sender::MessageSenderError,
) -> &'static str {
    use presage::libsignal_service::prelude::ServiceError;
    use presage::libsignal_service::sender::MessageSenderError as Error;
    match error {
        Error::NotFound { .. } => "UnregisteredFailure",
        Error::UntrustedIdentity { .. } => "UntrustedIdentityFailure",
        Error::ServiceError(ServiceError::Unauthorized) => "AuthorizationFailedFailure",
        _ => "NetworkFailure",
    }
}

/// 群发送结果条目组装（send 群形态 `results` 数组 additive 面，条目形状
/// 与直聊同构：成功 `{recipientNumber, success:{timestamp}}`，失败
/// `{recipientNumber, failure:{type}}`；号码经 contacts 映射，未命中回退
/// ACI uuid 串——listGroups 成员面同纪律）。
fn group_send_result_entries(
    outcomes: GroupSendOutcomes,
    timestamp: u64,
    numbers: &HashMap<String, String>,
) -> Vec<Value> {
    outcomes
        .into_iter()
        .map(|(service_id, outcome)| {
            let key = service_id.service_id_string().to_owned();
            let number = numbers.get(&key).cloned().unwrap_or(key);
            match outcome {
                None => json!({
                    "recipientNumber": number,
                    "success": { "timestamp": timestamp },
                }),
                Some(error) => json!({
                    "recipientNumber": number,
                    "failure": { "type": group_send_failure_word(&error) },
                }),
            }
        })
        .collect()
}

async fn dispatch_group(
    service: &Arc<Service>,
    manager: &mut Manager<SqliteStore, Registered>,
    group_id: &str,
    body: DataMessage,
    timestamp: u64,
) -> Result<GroupSendOutcomes, SendError> {
    // C 批提速：sender-key + 单请求 MRM 主路径；目标过少/MRM 链路不可用
    // 时内部 failover 回 vendored 逐收件人路径（官方合法 fallback）。
    group_mrm::dispatch_group_message(service, manager, group_id, body, timestamp).await
}

/// contacts 的 uuid 字符串 → E.164 映射（群组成员/作者号批量解析共用）。
async fn contacts_number_map(store: &SqliteStore) -> HashMap<String, String> {
    let Ok(contacts) = store.contacts().await else {
        return HashMap::new();
    };
    contacts
        .filter_map(Result::ok)
        .filter_map(|contact| {
            let number = contact.phone_number.as_ref().map(e164_string)?;
            Some((contact.uuid.to_string(), number))
        })
        .collect()
}

/// 回应/群内作者号 → ACI 字符串。解析顺序：UUID 形状直通 → 本机注册数据
/// （回应自己发的消息时 targetAuthor 是本机号，不在 contacts）→ contacts
/// 数字位匹配；都不中返回 None，回退语义由调用方决定。
/// send 附件上行：官方 CDN v4 面零 fork（presage upload_attachments，加密/
/// padding/digest/上传全官方原语）。任何一条失败整体 fail-closed（半发的
/// 引用消息比拒绝更糟）。预览图（若有）与附件同批上传，按序取回。
async fn upload_attachments_for_send(
    manager: &Manager<SqliteStore, Registered>,
    entries: &[String],
    preview_image: Option<&str>,
) -> Result<
    (
        Vec<presage::proto::AttachmentPointer>,
        Option<presage::proto::AttachmentPointer>,
    ),
    SendError,
> {
    let mut parsed = entries
        .iter()
        .map(|entry| parse_attachment_data_uri(entry))
        .collect::<Option<Vec<_>>>()
        .ok_or(SendError::Internal)?;
    let image_index = match preview_image {
        Some(uri) => {
            parsed.push(parse_attachment_data_uri(uri).ok_or(SendError::Internal)?);
            Some(parsed.len() - 1)
        }
        None => None,
    };
    if parsed.len() > ATTACHMENT_MAX_COUNT {
        return Err(SendError::Internal);
    }
    if parsed.is_empty() {
        return Ok((Vec::new(), None));
    }
    let uploads = manager
        .upload_attachments(
            parsed
                .into_iter()
                .map(|item| (item.spec, item.bytes))
                .collect(),
        )
        .await
        .map_err(send_error_of)?;
    let mut pointers = uploads
        .into_iter()
        .map(|result| result.ok())
        .collect::<Option<Vec<_>>>()
        .ok_or(SendError::Internal)?;
    let image = image_index.map(|index| pointers.remove(index));
    Ok((pointers, image))
}

/// send 文本 dataMessage 公共构造（直聊/群组同形状，compat §3 send 行
/// quoteTimestamp/quoteAuthor/previewUrl/mentions/attachments/textStyle 扩面）：
/// - quote：assemble_quote 单点组装（与贴纸分支同源）。
/// - preview：url/title/description 直投 proto Preview；previewImage 数据
///   URI 随同批 CDN 上行后挂 Preview.image（官方同构）。
/// - mentions：number→ACI 后 BodyRange.MentionAci（≤条目数即上限，与接收
///   投影 ≤64 同口径的调用方纪律）；offsets 透传。
/// - textStyles：五值白名单词表（解析闸钉定）→ BodyRange.Style（enum
///   as_str_name 同构）单点映射；offsets UTF-16 code units 透传（与
///   mentions 同款调用方纪律）。
/// - attachments：caller 已上行完毕的 AttachmentPointer 原样入消息。
#[allow(clippy::too_many_arguments)]
async fn build_text_data_message(
    store: &SqliteStore,
    message: &str,
    quote: Option<&SendQuote>,
    preview: Option<&SendPreview>,
    mentions: &[SendMention],
    text_styles: &[SendTextStyle],
    attachment_pointers: Vec<presage::proto::AttachmentPointer>,
    preview_image: Option<presage::proto::AttachmentPointer>,
    story_context: Option<&SendStoryContext>,
    timestamp: u64,
) -> Result<DataMessage, SendError> {
    let mut dm = DataMessage {
        timestamp: Some(timestamp),
        body: Some(message.to_owned()),
        attachments: attachment_pointers,
        ..DataMessage::default()
    };
    if let Some(quote) = quote {
        dm.quote = Some(assemble_quote(store, quote).await?);
    }
    // P14 组 4：storyContext 发送面（官方 storyContext = {sentTimestamp,
    // authorAci|authorAciBinary} 二选一按 binary 模式——引擎沿 mentions/
    // quote 先例写 string 形 authorAci，收端两侧都读互通无差，§5）。
    if let Some(story_context) = story_context {
        let author_aci = resolve_author_aci(store, &story_context.author)
            .await
            .ok_or(SendError::Internal)?;
        dm.story_context = Some(presage::proto::data_message::StoryContext {
            author_aci: Some(author_aci),
            sent_timestamp: Some(story_context.sent_timestamp),
            author_aci_binary: None,
        });
    }
    if let Some(preview) = preview {
        dm.preview = vec![presage::proto::Preview {
            url: Some(preview.url.clone()),
            title: preview.title.clone(),
            description: preview.description.clone(),
            image: preview_image,
            // P14 Preview.date 两端批：链接预览发布时刻（毫秒）如实透传，
            // 官方 desktop preview.date 同位（SendMessage.preload.ts:687）。
            date: preview.date,
        }];
    }
    if !mentions.is_empty() {
        let mut ranges = Vec::with_capacity(mentions.len());
        for mention in mentions {
            let aci = resolve_author_aci(store, &mention.number)
                .await
                .ok_or(SendError::Internal)?;
            ranges.push(presage::proto::BodyRange {
                start: Some(mention.start),
                length: Some(mention.length),
                associated_value: Some(presage::proto::body_range::AssociatedValue::MentionAci(
                    aci,
                )),
            });
        }
        dm.body_ranges = ranges;
    }
    if !text_styles.is_empty() {
        // 上游词表同构映射：prost 生成的 `Style` 枚举（repr i32）即官方
        // BodyRange.Style 的线上枚举形态（BOLD=1/ITALIC=2/SPOILER=3/
        // STRIKETHROUGH=4/MONOSPACE=5，proto 枚举名同词表）；NONE 是协议
        // 内部「无样式」标记（官方 composer 永不出、发出即静默无样式），
        // 与词表外值一并 fail-closed（解析闸之后服务层防御，不静默丢样式）。
        let mut ranges = Vec::with_capacity(text_styles.len());
        for style in text_styles {
            let value = match presage::proto::body_range::Style::from_str_name(&style.style) {
                Some(
                    value @ (presage::proto::body_range::Style::Bold
                    | presage::proto::body_range::Style::Italic
                    | presage::proto::body_range::Style::Spoiler
                    | presage::proto::body_range::Style::Strikethrough
                    | presage::proto::body_range::Style::Monospace),
                ) => value,
                _ => return Err(SendError::Internal),
            };
            ranges.push(presage::proto::BodyRange {
                start: Some(style.start),
                length: Some(style.length),
                associated_value: Some(presage::proto::body_range::AssociatedValue::Style(
                    value as i32,
                )),
            });
        }
        dm.body_ranges.extend(ranges);
    }
    Ok(dm)
}

/// 被引消息找回：author ACI + upstream 时间戳定位 presage 线程消息，
/// 直聊/群组 thread 都命中；任何环节失败 = None（官方降级同款）。
async fn quoted_data_message(
    store: &SqliteStore,
    author_aci: &str,
    timestamp: u64,
) -> Option<DataMessage> {
    let sender = ServiceId::parse_from_service_id_string(author_aci)?;
    let thread = store
        .thread_for_sender_and_timestamp(&sender, timestamp)
        .await
        .ok()??;
    let content = store.message(&thread, timestamp).await.ok()??;
    match content.body {
        ContentBody::DataMessage(dm) => Some(dm),
        _ => None,
    }
}

/// D4：quote 附件元数据找回（官方 makeQuote → getQuoteAttachment 行为对
/// 齐，`ts/util/makeQuote.preload.ts:69-138`）：被引消息首附件 > preview
/// 首图 > sticker data（官方优先级同序，各 take 1）。view-once 被引行
/// 同官方 isTapToView 短路固定 image/jpeg 占位（无 filename）。仅
/// metadata-only：thumbnail 不出——引擎无图像管线（无上传/无缩略图生成，
/// compat §5 gap 登记）；contentType/fileName 取被引指针原值不发明。
fn quoted_attachment_of(
    data_message: &DataMessage,
) -> Option<presage::proto::data_message::quote::QuotedAttachment> {
    use presage::proto::data_message::quote::QuotedAttachment;
    if data_message.is_view_once == Some(true) {
        return Some(QuotedAttachment {
            content_type: Some("image/jpeg".to_owned()),
            ..QuotedAttachment::default()
        });
    }
    data_message
        .attachments
        .first()
        .map(|attachment| QuotedAttachment {
            content_type: attachment.content_type.clone(),
            file_name: attachment.file_name.clone(),
            thumbnail: None,
        })
        .or_else(|| {
            data_message.preview.iter().find_map(|preview| {
                preview.image.as_ref().map(|image| QuotedAttachment {
                    content_type: image.content_type.clone(),
                    file_name: None,
                    thumbnail: None,
                })
            })
        })
        .or_else(|| {
            data_message.sticker.as_ref().and_then(|sticker| {
                sticker.data.as_ref().map(|data| QuotedAttachment {
                    content_type: data.content_type.clone(),
                    file_name: None,
                    thumbnail: None,
                })
            })
        })
}

/// quote 组装（文本/贴纸分支共用单点，compat §3 send 行 quote 语义）：
/// id=被引消息 upstream 时间戳、author=原作者 ACI（号码经
/// resolve_author_aci，不可解析 fail-closed = SendError::Internal，与
/// mentions 同口径）；text 从本机 store 找回——presage 官方接收路径已落库
/// 的消息（引擎无新增落库面），找不到按官方/上游同款降级缺省 text（收端
/// 仅少预览文案，协议面合法）。附件元数据同 store 找回（D4，官方
/// makeQuote 优先级面见 quoted_attachment_of）。
async fn assemble_quote(
    store: &SqliteStore,
    quote: &SendQuote,
) -> Result<presage::proto::data_message::Quote, SendError> {
    let author_aci = resolve_author_aci(store, &quote.author)
        .await
        .ok_or(SendError::Internal)?;
    let mut q = presage::proto::data_message::Quote {
        id: Some(quote.timestamp),
        author_aci: Some(author_aci.clone()),
        // 官方发送恒写 type（isGiftBadge ? GIFT_BADGE : NORMAL，
        // SendMessage.preload.ts:506-509）——词表缺省 normal 同语义。
        r#type: Some(if quote.gift_badge {
            presage::proto::data_message::quote::Type::GiftBadge
        } else {
            presage::proto::data_message::quote::Type::Normal
        } as i32),
        ..presage::proto::data_message::Quote::default()
    };
    // Q5：quote.bodyRanges 发送面（官方 quote.bodyRanges?.map(toBodyRange)
    // 同构）：mention number→ACI（与 quote author 同解析，不可解析
    // fail-closed）、style 五值词表（解析闸之后服务层防御，与 textStyle 同
    // 口径）。官方「bodyRanges 在场抬 requiredProtocolVersion 至 MENTIONS」
    // 是桌面进程内断言面，wire 由加密层协商，引擎不设（§5 登记）。
    for range in &quote.body_ranges {
        let associated_value = match (&range.mention, &range.style) {
            (Some(number), None) => presage::proto::body_range::AssociatedValue::MentionAci(
                resolve_author_aci(store, number)
                    .await
                    .ok_or(SendError::Internal)?,
            ),
            (None, Some(style)) => presage::proto::body_range::AssociatedValue::Style(
                match presage::proto::body_range::Style::from_str_name(style) {
                    Some(
                        value @ (presage::proto::body_range::Style::Bold
                        | presage::proto::body_range::Style::Italic
                        | presage::proto::body_range::Style::Spoiler
                        | presage::proto::body_range::Style::Strikethrough
                        | presage::proto::body_range::Style::Monospace),
                    ) => value as i32,
                    _ => return Err(SendError::Internal),
                },
            ),
            _ => return Err(SendError::Internal),
        };
        q.body_ranges.push(presage::proto::BodyRange {
            start: Some(range.start),
            length: Some(range.length),
            associated_value: Some(associated_value),
        });
    }
    if let Some(quoted) = quoted_data_message(store, &author_aci, quote.timestamp).await {
        if let Some(text) = quoted.body.clone() {
            q.text = Some(text);
        }
        if let Some(attachment) = quoted_attachment_of(&quoted) {
            q.attachments.push(attachment);
        }
    }
    Ok(q)
}

/// 贴纸图单条上行（官方 sendStickerMessage 同构：贴纸字节原样 upload，无
/// 常规附件的 voice 语义）。mediatype 必须 image/*（官方 MIME 嗅探只接受
/// image 且禁 video/text，回退 image/webp 的语义由上游声明等价承担）；
/// width/height 官方随 AttachmentPointer 出站，经 AttachmentSpec 透传。
async fn upload_sticker_image(
    manager: &Manager<SqliteStore, Registered>,
    sticker: &SendSticker,
) -> Result<presage::proto::AttachmentPointer, SendError> {
    let parsed = parse_attachment_data_uri(&sticker.image).ok_or(SendError::Internal)?;
    if !parsed.spec.content_type.starts_with("image/") {
        return Err(SendError::Internal);
    }
    let spec = presage::libsignal_service::sender::AttachmentSpec {
        width: sticker.width,
        height: sticker.height,
        ..parsed.spec
    };
    let mut uploads = manager
        .upload_attachments(vec![(spec, parsed.bytes)])
        .await
        .map_err(send_error_of)?;
    uploads
        .pop()
        .and_then(|result| result.ok())
        .ok_or(SendError::Internal)
}

/// 贴纸 dataMessage 构造（proto 面 = 线上 DataMessage.Sticker：packId/packKey
/// 字节、stickerId、emoji、data=已上行指针）。packId hex / packKey base64
/// 解码失败或空字节 fail-closed（官方桌面本地贴纸包必带两者，上游同契约）。
/// quote 可共存（官方「贴纸回应」StagedStickerReply = 同一 DataMessage 带
/// sticker+quote，非 reaction——Reaction proto 无贴纸位）：经 assemble_quote
/// 与文本分支同源组装，author 不可解析 fail-closed、正文找回失败降级缺
/// text；body 维持 undefined。
async fn build_sticker_data_message(
    store: &SqliteStore,
    sticker: &SendSticker,
    quote: Option<&SendQuote>,
    data: presage::proto::AttachmentPointer,
    timestamp: u64,
) -> Result<DataMessage, SendError> {
    let pack_id = hex_decode(&sticker.pack_id).ok_or(SendError::Internal)?;
    let pack_key = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(sticker.pack_key.as_bytes())
            .ok()
            .filter(|bytes| !bytes.is_empty())
            .ok_or(SendError::Internal)?
    };
    if pack_id.is_empty() {
        return Err(SendError::Internal);
    }
    let mut dm = DataMessage {
        timestamp: Some(timestamp),
        sticker: Some(presage::proto::data_message::Sticker {
            pack_id: Some(pack_id),
            pack_key: Some(pack_key),
            sticker_id: Some(sticker.sticker_id),
            emoji: sticker.emoji.clone(),
            data: Some(data),
        }),
        ..DataMessage::default()
    };
    if let Some(quote) = quote {
        dm.quote = Some(assemble_quote(store, quote).await?);
    }
    Ok(dm)
}

/// expiration-update DataMessage 构造（官方 asExpirationUpdate 同构，消失
/// 消息 E 批）：只设 Flags.EXPIRATION_TIMER_UPDATE=2 位 + timestamp。不设
/// expire_timer/expire_timer_version——presage send 面
/// restore_thread_timer 注入刚持久化的 (timer, version) 对（官方信封 =
/// flag 位 + expireTimer + expireTimerVersion，signal-cli SendHelper
/// sendMessage 的 withExpiration/withExpireTimerVersion 同型）；显式设置
/// 会被 presage 对已设 timer 再 bump version（持久化态/线面脱节），构造面
/// 保持 None 即官方语义单点。
fn build_expiration_timer_update_data_message(timestamp: u64) -> DataMessage {
    use presage::proto::data_message::Flags;
    DataMessage {
        timestamp: Some(timestamp),
        flags: Some(Flags::ExpirationTimerUpdate as u32),
        ..DataMessage::default()
    }
}

/// editMessage 体构造（官方 getContentMessage 同构：Content.editMessage
/// {targetSentTimestamp, dataMessage{timestamp, body}}，SendMessage
/// .preload.ts:1075-1092）。群腿由调用方携 (master_key, revision) 手注
/// group_v2（vendored send 面不为 EditMessage 注入群上下文）；直聊腿传 None。
fn build_edit_message_body(
    target_timestamp: u64,
    timestamp: u64,
    message: &str,
    group: Option<(Vec<u8>, Option<u32>)>,
) -> ContentBody {
    ContentBody::EditMessage(presage::proto::EditMessage {
        target_sent_timestamp: Some(target_timestamp),
        data_message: Some(DataMessage {
            timestamp: Some(timestamp),
            body: Some(message.to_owned()),
            group_v2: group.map(|(master_key, revision)| presage::proto::GroupContextV2 {
                master_key: Some(master_key),
                revision,
                group_change: None,
            }),
            ..DataMessage::default()
        }),
    })
}

/// 阅后即焚媒体族判定（E1/E2 共用）：`image/*|video/*` 前缀族。官方桌面
/// isValidTapToView 用 Chrome 支持集枚举表（GoogleChrome.std.ts：avif/bmp/
/// gif/jpeg/webp/png/apng/ico 系 + mp4/ogg/webm），引擎 metadata-only 无渲染
/// 面按族宽判——官方集外但手机端合法的 video/quicktime 等不被误标 invalid
/// （差异登记 compat §5）。导出面 view-once 投影（history.rs）共用本函数
/// 作单一真相源。
pub(crate) fn is_view_once_media(content_type: &str) -> bool {
    let lowered = content_type.to_ascii_lowercase();
    lowered.starts_with("image/") || lowered.starts_with("video/")
}

/// 阅后即焚 dataMessage 构造（E1，官方发送面同构）：body 无、单附件、
/// is_view_once=Some(true)（线上 DataMessage.isViewOnce=14 bool；官方发送侧
/// 不 bump requiredProtocolVersion——构造面不设该字段，presage send 面注入
/// 的恒为默认 0 = 官方初值）。fail-closed：恰 1 附件 ∧ 媒体族 image|video；
/// quote/preview/mentions/sticker/body 由参数面禁入（methods.rs），构造面
/// 结构上不可能携带，违规 -32603（折叠 SendError::Internal）。
fn build_view_once_data_message(
    attachment_pointers: Vec<presage::proto::AttachmentPointer>,
    timestamp: u64,
) -> Result<DataMessage, SendError> {
    if attachment_pointers.len() != 1 {
        return Err(SendError::Internal);
    }
    let content_type = attachment_pointers[0]
        .content_type
        .as_deref()
        .ok_or(SendError::Internal)?;
    if !is_view_once_media(content_type) {
        return Err(SendError::Internal);
    }
    Ok(DataMessage {
        timestamp: Some(timestamp),
        attachments: attachment_pointers,
        is_view_once: Some(true),
        ..DataMessage::default()
    })
}

async fn resolve_author_aci(store: &SqliteStore, author: &str) -> Option<String> {
    let trimmed = author.trim();
    if let Some(service_id) = ServiceId::parse_from_service_id_string(trimmed) {
        return Some(service_id.service_id_string().to_owned());
    }
    let want: String = trimmed.chars().filter(char::is_ascii_digit).collect();
    if want.is_empty() {
        return None;
    }
    if let Some(data) = store.load_registration_data().await.ok().flatten() {
        let own = e164_string(&data.phone_number);
        let own_digits: String = own.chars().filter(char::is_ascii_digit).collect();
        if own_digits == want {
            return Some(data.service_ids.aci.to_string());
        }
    }
    let contacts = contacts_number_map(store).await;
    contacts
        .into_iter()
        .find(|(_, number)| {
            let digits: String = number.chars().filter(char::is_ascii_digit).collect();
            digits.ends_with(&want)
        })
        .map(|(uuid, _)| uuid)
}

async fn open_store(dir: &Path) -> Result<SqliteStore, ()> {
    std::fs::create_dir_all(dir).map_err(|_| ())?;
    // SqliteStore 要的是数据库文件路径，不是目录；sqlx 的连接串解析只剥
    // `sqlite://`/`sqlite:` 前缀，传 `file:///…` 会把整串当文件名。这里固定
    // 在账号目录下落 store.db、直接传裸路径。路径来自受信启动器，可入日志。
    let path = dir.join("store.db");
    let path = path.to_str().ok_or(())?;
    SqliteStore::open(path, OnNewIdentity::Trust)
        .await
        .map_err(|error| {
            eprintln!("kt-signal-engine: sqlite store open failed: {error}");
        })
}

/// 发送路径取用账号常驻管理器共享句柄（C 批提速单一漏斗）：缓存命中即复用
/// （presage Manager 内部 websocket 常驻，closed 自愈重建）；未命中惰性建
/// 连（open store + load_registered）入缓存。同账号取用互斥串行（`lock_
/// registered_for_send`），跨账号并行；delete/relink 摘除句柄。缓存 map 锁
/// 只包查找/插入，绝不跨 guard 锁持有。
async fn open_registered(service: &Arc<Service>, account: &str) -> Option<SharedSendManager> {
    let cached = {
        let cache = service.send_managers.lock().await;
        cache.get(account).cloned()
    };
    if let Some(shared) = cached {
        return Some(shared);
    }
    let dir = {
        let state = service.state.lock().await;
        let rel = state.account_dir(account)?;
        service.data_dir.join(rel)
    };
    let store = open_store(&dir).await.ok()?;
    let manager = Manager::load_registered(store).await.ok()?;
    let mut cache = service.send_managers.lock().await;
    let shared = cache
        .entry(account.to_owned())
        .or_insert_with(|| Arc::new(AsyncMutex::new(manager)));
    Some(Arc::clone(shared))
}

/// 常驻管理器取用锁定：互斥 guard + 每次取用 prekey 14 天超龄闸（B 批语义
/// 按次保住，本地零网络读，fail-open）。guard 借用调用方持有的句柄。
async fn lock_registered_for_send(
    shared: &SharedSendManager,
) -> AsyncMutexGuard<'_, Manager<SqliteStore, Registered>> {
    let mut manager = shared.lock().await;
    prekey::send_gate(&mut manager).await;
    manager
}

/// 取用常驻管理器共享句柄 + 账号目录（安全码 trust sidecar 等账号目录伴生
/// 文件所需）。取用纪律与 [`open_registered`] 同一漏斗同闸。
pub(crate) async fn open_registered_with_dir(
    service: &Arc<Service>,
    account: &str,
) -> Option<(SharedSendManager, std::path::PathBuf)> {
    let dir = {
        let state = service.state.lock().await;
        let rel = state.account_dir(account)?;
        service.data_dir.join(rel)
    };
    let shared = open_registered(service, account).await?;
    Some((shared, dir))
}

/// 收件人 → ServiceId：号码与 UUID 双形态都收（对齐 signal-cli 公开 JSON-RPC
/// 的 recipient 语义）——UUID 直通为 ACI；号码走 contacts 数字位匹配（链接后
/// 手机端立即推送同步）。冷启动窗口内未命中返回 None（发送按内部失败处理，
/// compat §5）。
async fn resolve_recipient(
    manager: &Manager<SqliteStore, Registered>,
    recipient: &str,
) -> Option<presage::libsignal_service::protocol::ServiceId> {
    if let Some(service_id) = ServiceId::parse_from_service_id_string(recipient.trim()) {
        return Some(service_id);
    }
    let contact = find_contact_by_number(manager, recipient).await?;
    Some(ServiceId::Aci(contact.uuid.into()))
}

pub(crate) async fn find_contact_by_number(
    manager: &Manager<SqliteStore, Registered>,
    number: &str,
) -> Option<presage::model::contacts::Contact> {
    // 号码匹配只比数字位：store 里是 international 排版，入参是 E.164。
    let want: String = number.chars().filter(char::is_ascii_digit).collect();
    if want.is_empty() {
        return None;
    }
    let contacts = manager.store().contacts().await.ok()?;
    contacts.filter_map(Result::ok).find(|contact| {
        contact.phone_number.as_ref().is_some_and(|phone| {
            let digits: String = phone
                .to_string()
                .chars()
                .filter(char::is_ascii_digit)
                .collect();
            digits.ends_with(&want)
        })
    })
}

/// PhoneNumber → E.164 文本（`+15551234567`）：Display 默认是 international
/// 排版（带空格/连字符），不能直接当账号标识。
pub(crate) fn e164_string(
    phone: &presage::libsignal_service::prelude::phonenumber::PhoneNumber,
) -> String {
    use presage::libsignal_service::prelude::phonenumber::Mode;
    phone.format().mode(Mode::E164).to_string()
}

// ---------------------------------------------------------------------------
// fake 模式共用件（P1a 形状桩语义原样保留）
// ---------------------------------------------------------------------------

fn fake_link_uri() -> String {
    let mut seed = mix_seed();
    let mut next = || {
        seed = mix_seed_step(seed);
        seed
    };
    format!("sgnl://link?uuid={:016x}&pub_key={:016x}", next(), next())
}

/// 时间+pid 熵的 splitmix64（无 rand 依赖：P1a stub 同纪律）。
fn mix_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ ((std::process::id() as u64) << 32)
}

fn mix_seed_step(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// 账号 store 目录名：`lk-<hex16>`（ADR 0004：链接开始即定名，免重命名竞态）。
fn random_dir_name() -> String {
    format!("lk-{:016x}", mix_seed_step(mix_seed()))
}

/// 启动清扫：孤儿 store 目录。链接目录「开始即定名」，正常失败路径由链接
/// 任务自清，但进程被杀/崩溃时残留。启动时机无在途链接，凡 engine-state.json
/// 未登记的 `accounts/lk-*` 目录一律删除：其 store 至多持有半程注册数据，
/// 用户消息只在链接成功后落 store，清扫不触及任何聊天内容。链接成功与登记
/// 之间被杀的毫秒级窗口会连已注册 store 一起删——状态文件是权威，恢复只能
/// 重新扫码，窗口代价可接受。本层纪律：无 stderr 输出，失败按尽力而为跳过
/// （下次启动重试）。
fn sweep_orphan_store_dirs(data_dir: &Path, referenced: &HashSet<String>) -> usize {
    let Ok(entries) = std::fs::read_dir(data_dir.join("accounts")) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !name.starts_with("lk-") || !entry.path().is_dir() {
            continue;
        }
        if referenced.contains(&format!("accounts/{name}")) {
            continue;
        }
        removed += usize::from(std::fs::remove_dir_all(entry.path()).is_ok());
    }
    removed
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use presage::proto::attachment_pointer::Flags;
    use presage::proto::body_range::AssociatedValue;
    use presage::proto::{data_message, AttachmentPointer, BodyRange, DataMessage, Preview};
    use prost::Message as _;

    /// 发送链凭据失效归型：presage 两层归型点（直接 ServiceError 面 /
    /// MessageSenderError 内 sender 同源再包一层）的 401/403 同归
    /// SendError::Unbound；其余错误（404 目标态、sender 其他失败、非服务
    /// 错误）不误报解绑，维持 Internal 折叠。
    #[test]
    fn send_errors_classify_unauthorized_as_unbound() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::Unauthorized)),
            SendError::Unbound
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::ServiceError(ServiceError::Unauthorized)
            ))),
            SendError::Unbound
        ));

        // 404 = 收件人面（官方 response.rs 注：recipient not registered），
        // 与本账号凭据失效不同族，不得误报。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::NotFoundError)),
            SendError::Internal
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::NoMessagesToSend
            ))),
            SendError::Internal
        ));
        assert!(matches!(
            send_error_of(presage::Error::UnknownGroup),
            SendError::Internal
        ));
    }

    /// 发送链结构化三分类（compat §3）：限流 429/413（两触点面）、人机
    /// 验证 428（直接 ServiceError 臂 + sender 再包层臂，token/options
    /// 归型即弃不上 wire）、服务端 5xx 不可用（UnhandledResponseCode 兜底
    /// 臂，vendored 无独立 ServerError 变体）；<500 的 UnhandledResponseCode
    /// 不算不可用，维持 Internal 折叠。
    #[test]
    fn send_errors_classify_structured_server_faces() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        // 限流：429/413 同归型 RateLimitExceeded（vendored response.rs）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(
                ServiceError::RateLimitExceeded { retry_after: None }
            )),
            SendError::RateLimited
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::ServiceError(ServiceError::RateLimitExceeded {
                    retry_after: None
                })
            ))),
            SendError::RateLimited
        ));

        // 人机验证 428：直接臂 + sender 再包层臂（token/options 不出归型面）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(
                ServiceError::ProofRequiredError(
                    presage::libsignal_service::push_service::ProofRequired {
                        token: String::new(),
                        options: Vec::new(),
                    }
                )
            )),
            SendError::ProofRequired
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::ProofRequired {
                    token: "proof-token".to_owned(),
                    options: vec!["captcha".to_owned()],
                }
            ))),
            SendError::ProofRequired
        ));

        // 服务端 5xx → ServerUnavailable；<500（如 400）不算不可用。
        let unhandled = |status: u16| {
            send_error_of(presage::Error::ServiceError(
                ServiceError::UnhandledResponseCode {
                    status: reqwest::StatusCode::from_u16(status).unwrap(),
                    body: String::new(),
                },
            ))
        };
        assert!(matches!(unhandled(500), SendError::ServerUnavailable));
        assert!(matches!(unhandled(503), SendError::ServerUnavailable));
        assert!(matches!(unhandled(400), SendError::Internal));
    }

    /// project_content 现返通知向量：契约测试形状 = 恰一条投影，取首条
    /// （多通知形状由本批 profileKeyUpdate 面单测覆盖）。
    #[track_caller]
    fn pop_unwrap(notifications: Vec<Value>, message: &str) -> Value {
        assert_eq!(notifications.len(), 1, "{message}: expected single");
        notifications.into_iter().next().expect("single")
    }

    #[test]
    fn orphan_store_sweep_removes_only_unreferenced_lk_dirs() {
        let root = std::env::temp_dir().join(format!(
            "kt-engine-sweep-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let accounts = root.join("accounts");
        std::fs::create_dir_all(accounts.join("lk-live/nested")).unwrap();
        std::fs::write(accounts.join("lk-live/store.db"), b"x").unwrap();
        std::fs::create_dir_all(accounts.join("lk-orphan-empty")).unwrap();
        std::fs::create_dir_all(accounts.join("lk-orphan-db")).unwrap();
        std::fs::write(accounts.join("lk-orphan-db/store.db"), b"x").unwrap();
        std::fs::create_dir_all(accounts.join("not-lk")).unwrap();
        std::fs::write(accounts.join("lk-loose-file"), b"x").unwrap();

        let referenced: HashSet<String> = ["accounts/lk-live".to_owned()].into_iter().collect();
        let removed = sweep_orphan_store_dirs(&root, &referenced);

        assert_eq!(removed, 2);
        assert!(accounts.join("lk-live/store.db").exists());
        assert!(accounts.join("not-lk").is_dir());
        assert!(accounts.join("lk-loose-file").exists());
        assert!(!accounts.join("lk-orphan-empty").try_exists().unwrap());
        assert!(!accounts.join("lk-orphan-db").try_exists().unwrap());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn attachment_data_uri_parses_filename_and_voice_params() {
        let parsed = |uri| parse_attachment_data_uri(uri).expect("parses");
        let none = |uri| assert!(parse_attachment_data_uri(uri).is_none());

        // 基线形状（829e09f）：mediatype + filename。
        let plain = parsed("data:text/plain;filename=hello%20world.txt;base64,aGVsbG8=");
        assert_eq!(plain.spec.content_type, "text/plain");
        assert_eq!(plain.spec.file_name.as_deref(), Some("hello world.txt"));
        assert_eq!(plain.spec.voice_note, None);
        assert_eq!(plain.bytes, b"hello");

        // voice 裸参数与 voice=true 等价（官方 VOICE_MESSAGE 同一 flags 面）。
        for uri in [
            "data:audio/mpeg;voice;base64,aGVsbG8=",
            "data:audio/mpeg;voice=true;base64,aGVsbG8=",
            "data:audio/mpeg;voice=true;filename=voice%20note.mp3;base64,aGVsbG8=",
        ] {
            let voice = parsed(uri);
            assert_eq!(voice.spec.content_type, "audio/mpeg");
            assert_eq!(voice.spec.voice_note, Some(true));
        }
        assert_eq!(
            parsed("data:audio/mpeg;voice=true;filename=voice%20note.mp3;base64,aGVsbG8=")
                .spec
                .file_name
                .as_deref(),
            Some("voice note.mp3")
        );

        // voice=false 显式不带 flags；非 bool 值 fail-closed。
        assert_eq!(
            parsed("data:audio/mpeg;voice=false;base64,aGVsbG8=")
                .spec
                .voice_note,
            None
        );
        none("data:audio/mpeg;voice=1;base64,aGVsbG8=");
        none("data:audio/mpeg;voice=yes;base64,aGVsbG8=");

        // 既有 fail-closed 面不回归：非 base64 / 坏 percent / 超 base64 字符集。
        none("data:text/plain,aGVsbG8=");
        none("data:text/plain;filename=%zz;base64,aGVsbG8=");
        none("data:text/plain;base64,not@base64!");
    }

    #[test]
    fn data_message_projection_carries_all_consumed_surfaces() {
        let author = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let mut numbers = HashMap::new();
        numbers.insert(author.to_owned(), "+15551230000".to_owned());

        let dm = DataMessage {
            timestamp: Some(1_700_000_000_000),
            body: Some("hi @bob".to_owned()),
            is_view_once: Some(true),
            quote: Some(data_message::Quote {
                id: Some(1_699_999_000_000),
                author_aci: Some(author.to_owned()),
                text: Some("earlier".to_owned()),
                ..Default::default()
            }),
            reaction: Some(data_message::Reaction {
                emoji: Some("👍".to_owned()),
                target_author_aci: Some(author.to_owned()),
                target_sent_timestamp: Some(1_699_999_000_000),
                remove: Some(false),
                ..Default::default()
            }),
            delete: Some(data_message::Delete {
                target_sent_timestamp: Some(1_699_998_000_000),
            }),
            attachments: vec![AttachmentPointer {
                client_uuid: Some(uuid_bytes("11111111-2222-4333-8444-555555555555")),
                content_type: Some("image/png".to_owned()),
                file_name: Some("shot.png".to_owned()),
                size: Some(2048),
                width: Some(64),
                height: Some(32),
                caption: Some("cap".to_owned()),
                flags: Some(Flags::VoiceMessage as u32),
                ..Default::default()
            }],
            body_ranges: vec![BodyRange {
                start: Some(3),
                length: Some(4),
                associated_value: Some(AssociatedValue::MentionAci(author.to_owned())),
            }],
            ..Default::default()
        };

        let out = project_data_message(&dm, &numbers);
        assert_eq!(out["timestamp"], json!(1_700_000_000_000_u64));
        assert_eq!(out["message"], json!("hi @bob"));
        assert_eq!(out["viewOnce"], json!(true));
        assert_eq!(out["quote"]["author"], json!("+15551230000"));
        assert_eq!(out["quote"]["id"], json!(1_699_999_000_000_u64));
        assert_eq!(out["reaction"]["targetAuthor"], json!("+15551230000"));
        assert_eq!(out["reaction"]["isRemove"], json!(false));
        assert_eq!(
            out["remoteDelete"]["timestamp"],
            json!(1_699_998_000_000_u64)
        );
        assert_eq!(
            out["attachments"][0]["id"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(out["attachments"][0]["isVoiceNote"], json!(true));
        assert_eq!(out["attachments"][0]["size"], json!(2048));
        assert_eq!(out["mentions"][0]["number"], json!(author));
        assert_eq!(out["mentions"][0]["start"], json!(3));
    }

    /// D4 quote 附件元数据双面：接收投影（quote.attachments[] 条目
    /// contentType/filename/thumbnail 三键形，空条目空对象，无附件键缺席）
    /// 与发送找回 quoted_attachment_of 官方 makeQuote 优先级（首附件 >
    /// preview 首图 > sticker data，各 take 1；view-once 短路固定
    /// image/jpeg 无 filename；thumbnail 不出 = metadata-only）。
    #[test]
    fn quote_attachment_metadata_faces() {
        use presage::proto::data_message::quote::QuotedAttachment;

        // —— 接收投影面 ——
        let author = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let mut numbers = HashMap::new();
        numbers.insert(author.to_owned(), "+15551230000".to_owned());

        let dm = DataMessage {
            quote: Some(data_message::Quote {
                id: Some(1_699_999_000_000),
                author_aci: Some(author.to_owned()),
                attachments: vec![
                    QuotedAttachment {
                        content_type: Some("image/png".to_owned()),
                        file_name: Some("shot.png".to_owned()),
                        thumbnail: Some(AttachmentPointer {
                            width: Some(16),
                            ..Default::default()
                        }),
                    },
                    QuotedAttachment::default(),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let out = project_data_message(&dm, &numbers);
        let first = &out["quote"]["attachments"][0];
        assert_eq!(first["contentType"], json!("image/png"));
        assert_eq!(first["filename"], json!("shot.png"));
        assert_eq!(first["thumbnail"]["width"], json!(16));
        assert_eq!(out["quote"]["attachments"][1], json!({}));

        let bare = DataMessage {
            quote: Some(data_message::Quote {
                id: Some(1_699_999_000_000),
                author_aci: Some(author.to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let out_bare = project_data_message(&bare, &numbers);
        assert!(out_bare["quote"].get("attachments").is_none());

        // —— 发送找回优先级面 ——
        // attachments[0] take 1：contentType/fileName 取被引指针原值。
        let with_attachments = DataMessage {
            attachments: vec![
                AttachmentPointer {
                    content_type: Some("video/mp4".to_owned()),
                    file_name: Some("clip.mp4".to_owned()),
                    ..Default::default()
                },
                AttachmentPointer {
                    content_type: Some("image/png".to_owned()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let picked = quoted_attachment_of(&with_attachments).expect("first attachment");
        assert_eq!(picked.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(picked.file_name.as_deref(), Some("clip.mp4"));
        assert!(picked.thumbnail.is_none(), "metadata-only: thumbnail 不出");

        // preview 首图：无 filename（官方 preview 图无文件名语义）。
        let with_preview = DataMessage {
            preview: vec![Preview {
                url: Some("https://example.com/a".to_owned()),
                image: Some(AttachmentPointer {
                    content_type: Some("image/jpeg".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let picked = quoted_attachment_of(&with_preview).expect("preview image");
        assert_eq!(picked.content_type.as_deref(), Some("image/jpeg"));
        assert!(picked.file_name.is_none());
        assert!(picked.thumbnail.is_none());

        // sticker data：image/webp。
        let with_sticker = DataMessage {
            sticker: Some(data_message::Sticker {
                data: Some(AttachmentPointer {
                    content_type: Some("image/webp".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let picked = quoted_attachment_of(&with_sticker).expect("sticker data");
        assert_eq!(picked.content_type.as_deref(), Some("image/webp"));
        assert!(picked.file_name.is_none());

        // view-once 短路：固定 image/jpeg 占位，覆盖 attachments 命中。
        let view_once = DataMessage {
            is_view_once: Some(true),
            attachments: vec![AttachmentPointer {
                content_type: Some("video/mp4".to_owned()),
                file_name: Some("clip.mp4".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let picked = quoted_attachment_of(&view_once).expect("view-once placeholder");
        assert_eq!(picked.content_type.as_deref(), Some("image/jpeg"));
        assert!(picked.file_name.is_none());
        assert!(picked.thumbnail.is_none());

        // 全缺 → None（quote 不出 attachments 条目，收端仅少缩略占位）。
        assert!(quoted_attachment_of(&DataMessage::default()).is_none());
    }

    #[test]
    fn data_message_projection_resolves_unmapped_author_to_aci() {
        let author = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let dm = DataMessage {
            reaction: Some(data_message::Reaction {
                target_author_aci: Some(author.to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let out = project_data_message(&dm, &HashMap::new());
        assert_eq!(out["reaction"]["targetAuthor"], json!(author));
        assert!(out.get("message").is_none(), "body absent stays absent");
        assert!(out.get("attachments").is_none());
    }

    /// 附件 locator 指针面投影（connector 契约 1.61 §4.54 引擎侧缺口闭合，
    /// live receive 信封面；消费方 normalized_attachment ingest 落 schema-19
    /// live 列 → attachments.download 走 §4.38 step-3 transit 链）。投影口径
    /// = proto 只投影不发明（导出侧 project_pointer_common 同款，engine-compat
    /// 1.56 消费形状同形）：key/cdnKey/uploadTimestamp/digest，全缺 → locator
    /// 键缺席（wire 与旧版逐字节同形）。钉定 rev AttachmentPointer 无
    /// plaintextHash 面（backup transit 指针专属），诚实缺席不发明。
    #[test]
    fn data_message_projection_projects_attachment_locator_faces() {
        use base64::engine::general_purpose::STANDARD;
        let key_set = [0x33_u8; 64];
        let digest = [0x44_u8; 32];

        // 完整 transit 面：key + cdnKey + cdnNumber + digest（官方
        // isDownloadableFromTransitTier 四件齐形态，connector 侧可下载）。
        let full = project_attachment_pointer(&AttachmentPointer {
            client_uuid: Some(uuid_bytes("11111111-2222-4333-8444-555555555555")),
            content_type: Some("image/jpeg".to_owned()),
            size: Some(4096),
            key: Some(key_set.to_vec()),
            digest: Some(digest.to_vec()),
            cdn_number: Some(2),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey(
                    "live-cdn-key".to_owned(),
                ),
            ),
            upload_timestamp: Some(1_700_000_000_000),
            ..Default::default()
        });
        let locator = full["locator"].as_object().expect("locator face present");
        assert_eq!(
            locator["key"].as_str(),
            Some(STANDARD.encode(key_set).as_str())
        );
        assert_eq!(locator["cdnKey"].as_str(), Some("live-cdn-key"));
        assert_eq!(locator["cdnNumber"], json!(2));
        assert_eq!(
            locator["digest"].as_str(),
            Some(STANDARD.encode(digest).as_str())
        );
        assert_eq!(locator["uploadTimestamp"], json!(1_700_000_000_000_u64));
        // 钉定 rev 无 plaintextHash 字段：键不得发明。
        assert!(locator.get("plaintextHash").is_none());

        // CDN0 原样透出（cdnNumber=0 是合法 CDN0，导出侧同口径）；
        // upload_timestamp 缺席/0 → 键省略（官方 falsy 同款）。
        let cdn0 = project_attachment_pointer(&AttachmentPointer {
            key: Some(key_set.to_vec()),
            digest: Some(digest.to_vec()),
            cdn_number: Some(0),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey(
                    "cdn-zero".to_owned(),
                ),
            ),
            upload_timestamp: Some(0),
            ..Default::default()
        });
        let zero_locator = cdn0["locator"].as_object().expect("locator present");
        assert_eq!(zero_locator["cdnNumber"], json!(0));
        assert!(zero_locator.get("uploadTimestamp").is_none());

        // 部分 locator 只投命中字段：legacy CdnId 指针无 cdnKey 字符串 → 该
        // 键缺席（transit 资格闸在消费方判，宁缺勿错不发明字符串形态）。
        let cdn_id = project_attachment_pointer(&AttachmentPointer {
            key: Some(key_set.to_vec()),
            digest: Some(digest.to_vec()),
            cdn_number: Some(0),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnId(1234),
            ),
            ..Default::default()
        });
        let id_locator = cdn_id["locator"].as_object().expect("locator present");
        assert!(id_locator.get("cdnKey").is_none());
        assert!(id_locator.get("key").is_some());

        // 空字节 key / 空 cdnKey / digest 缺 → 各自键省略；全缺 → 整个
        // locator 键缺席（wire 与旧版逐字节同形）。
        let empty_key = project_attachment_pointer(&AttachmentPointer {
            key: Some(Vec::new()),
            digest: Some(digest.to_vec()),
            cdn_number: Some(3),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey("k".to_owned()),
            ),
            ..Default::default()
        });
        assert!(empty_key["locator"]
            .as_object()
            .unwrap()
            .get("key")
            .is_none());

        let metadata_only = project_attachment_pointer(&AttachmentPointer {
            client_uuid: Some(uuid_bytes("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")),
            content_type: Some("image/png".to_owned()),
            size: Some(8),
            ..Default::default()
        });
        assert!(
            metadata_only.get("locator").is_none(),
            "metadata-only pointer stays byte-identical: no locator key"
        );
        assert_eq!(
            metadata_only["id"],
            json!("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        );
        assert_eq!(metadata_only["contentType"], json!("image/png"));
        assert_eq!(metadata_only["size"], json!(8));
    }

    /// preview.image 与 sticker.data 同走附件指针投影：locator 面随行透出
    /// （消费方 ingest 各按其归属落位；locator 全缺形状不变）。
    #[test]
    fn preview_and_sticker_pointer_projection_carries_locator() {
        let pointer = || AttachmentPointer {
            key: Some([0x55_u8; 64].to_vec()),
            digest: Some([0x66_u8; 32].to_vec()),
            cdn_number: Some(2),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey(
                    "preview-key".to_owned(),
                ),
            ),
            ..Default::default()
        };
        let dm = DataMessage {
            preview: vec![Preview {
                url: Some("https://example.com/a".to_owned()),
                image: Some(pointer()),
                ..Default::default()
            }],
            sticker: Some(data_message::Sticker {
                pack_id: Some(vec![0x01; 16]),
                pack_key: Some(vec![0x02; 32]),
                sticker_id: Some(7),
                data: Some(pointer()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let out = project_data_message(&dm, &HashMap::new());
        let preview_locator = out["previews"][0]["image"]["locator"]
            .as_object()
            .expect("preview image locator");
        assert_eq!(preview_locator["cdnKey"].as_str(), Some("preview-key"));
        let sticker_locator = out["sticker"]["data"]["locator"]
            .as_object()
            .expect("sticker data locator");
        assert_eq!(sticker_locator["cdnNumber"], json!(2));
    }

    /// P14 收投影缺批（组 1）：dataMessage 面四键——expireTimerVersion=23
    /// （>0 才投——0 = INITIAL 初值零信息量，presage 对无计时器线程注
    /// Some(0)）、requiredProtocolVersion=12（弱面：>0 才投、不 enforce——
    /// 宽容面记 compat §5）、preview.date（>0 才投，官方 cleanLinkPreviewDate
    /// 同语义）。
    #[test]
    fn data_message_projection_carries_p14_receive_faces() {
        let numbers = HashMap::new();

        // 有：version/requiredVersion/date 全投。
        let out = project_data_message(
            &DataMessage {
                expire_timer_version: Some(3),
                required_protocol_version: Some(2),
                timestamp: Some(1_700_000_000_000),
                preview: vec![Preview {
                    url: Some("https://example.com/a".to_owned()),
                    date: Some(1_600_000_000_000),
                    ..Default::default()
                }],
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["expireTimerVersion"], json!(3));
        assert_eq!(out["requiredProtocolVersion"], json!(2));
        assert_eq!(out["previews"][0]["date"], json!(1_600_000_000_000_u64));

        // 缺省键不在；required=0/date=0 零信息量不投（0 = INITIAL 无要求
        // 语义 / 官方 falsy 同口径）。
        let out = project_data_message(
            &DataMessage {
                expire_timer_version: Some(0),
                required_protocol_version: Some(0),
                preview: vec![Preview {
                    url: Some("https://example.com/a".to_owned()),
                    date: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("expireTimerVersion").is_none());
        assert!(out.get("requiredProtocolVersion").is_none());
        assert!(out["previews"][0].get("date").is_none());

        // 普通行零 P14 键。
        let out = project_data_message(
            &DataMessage {
                body: Some("plain".to_owned()),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("expireTimerVersion").is_none());
        assert!(out.get("requiredProtocolVersion").is_none());
    }

    /// P14 收投影缺批（组 1）：附件指针 live 投影 blurHash /
    /// incrementalMac+chunkSize（导出面 project_pointer_common 同形：
    /// blurHash 非空才投；mac+chunkSize 同进同退、齐备才保留——官方
    /// filePointers.preload.ts:84-87 同构）。
    #[test]
    fn attachment_pointer_projection_carries_blur_hash_and_incremental_mac() {
        use base64::Engine as _;

        let out = project_attachment_pointer(&AttachmentPointer {
            blur_hash: Some("L6H2:R-;4nWB?b-;~qof4nWBj[%M".to_owned()),
            incremental_mac: Some(vec![0x77_u8; 32]),
            chunk_size: Some(1024),
            ..Default::default()
        });
        assert_eq!(
            out["blurHash"].as_str(),
            Some("L6H2:R-;4nWB?b-;~qof4nWBj[%M")
        );
        assert_eq!(
            out["incrementalMac"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0x77_u8; 32])
                    .as_str()
            )
        );
        assert_eq!(out["chunkSize"], json!(1024));

        // 同进同退：只带 mac 无 chunkSize → 两键都不投；空串 blurHash 不投。
        let out = project_attachment_pointer(&AttachmentPointer {
            blur_hash: Some(String::new()),
            incremental_mac: Some(vec![0x77_u8; 32]),
            ..Default::default()
        });
        assert!(out.get("blurHash").is_none());
        assert!(out.get("incrementalMac").is_none());
        assert!(out.get("chunkSize").is_none());
    }

    /// P14 收投影缺批（组 3）：附件 flags BORDERLESS=2/GIF=8 恒投布尔
    /// （镜像 isVoiceNote 先例）；与 VOICE_MESSAGE 位共存按位掩码各自命中；
    /// 无 flags = 三布尔全 false。
    #[test]
    fn attachment_pointer_projection_carries_borderless_and_gif_flags() {
        use presage::proto::attachment_pointer::Flags;

        let out = project_attachment_pointer(&AttachmentPointer {
            flags: Some(Flags::Borderless as u32),
            ..Default::default()
        });
        assert_eq!(out["isBorderless"], json!(true));
        assert_eq!(out["isGif"], json!(false));
        assert_eq!(out["isVoiceNote"], json!(false));

        let out = project_attachment_pointer(&AttachmentPointer {
            flags: Some(Flags::Gif as u32),
            ..Default::default()
        });
        assert_eq!(out["isBorderless"], json!(false));
        assert_eq!(out["isGif"], json!(true));

        // 多 flag 共存按位掩码各自命中（未知 flag 位宽容面 §5 同口径）。
        let out = project_attachment_pointer(&AttachmentPointer {
            flags: Some(Flags::VoiceMessage as u32 | Flags::Gif as u32),
            ..Default::default()
        });
        assert_eq!(out["isVoiceNote"], json!(true));
        assert_eq!(out["isBorderless"], json!(false));
        assert_eq!(out["isGif"], json!(true));

        let out = project_attachment_pointer(&AttachmentPointer::default());
        assert_eq!(out["isBorderless"], json!(false));
        assert_eq!(out["isGif"], json!(false));
        assert_eq!(out["isVoiceNote"], json!(false));
    }

    /// P14 收投影缺批（组 2）：payment=20（notification 臂 note 有/无两面
    /// + activation 双 kind 词 + type 缺省整键不在）与 giftBadge=22（测试
    ///   内模拟服务器签发 zkgroup roundtrip → 全四键；空 presentation 整键
    ///   不在；坏字节降级材料两键——官方 throw 面不镜像）。
    #[test]
    fn data_message_projection_carries_payment_and_gift_badge_faces() {
        use base64::Engine as _;
        use presage::proto::data_message::payment::{self, Item};

        let numbers = HashMap::new();

        // notification 臂：note 有值如实、无值 null 填位（官方 `note ?? null`）。
        let out = project_data_message(
            &DataMessage {
                payment: Some(presage::proto::data_message::Payment {
                    item: Some(Item::Notification(payment::Notification {
                        note: Some("thx".to_owned()),
                        transaction: None,
                    })),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["payment"]["kind"], json!("NOTIFICATION"));
        assert_eq!(out["payment"]["note"], json!("thx"));

        let out = project_data_message(
            &DataMessage {
                payment: Some(presage::proto::data_message::Payment {
                    item: Some(Item::Notification(payment::Notification {
                        note: None,
                        transaction: None,
                    })),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["payment"]["note"], serde_json::Value::Null);

        // activation 臂：REQUEST/ACTIVATED 折叠双 kind 词；type 缺省 =
        // 官方 fall-through 整键不在。
        for (kind, expected) in [
            (
                Some(payment::activation::Type::Request),
                "ACTIVATION_REQUEST",
            ),
            (Some(payment::activation::Type::Activated), "ACTIVATION"),
        ] {
            let out = project_data_message(
                &DataMessage {
                    payment: Some(presage::proto::data_message::Payment {
                        item: Some(Item::Activation(payment::Activation {
                            r#type: kind.map(|kind| kind as i32),
                        })),
                    }),
                    ..Default::default()
                },
                &numbers,
            );
            assert_eq!(out["payment"]["kind"], json!(expected));
        }
        let out = project_data_message(
            &DataMessage {
                payment: Some(presage::proto::data_message::Payment {
                    item: Some(Item::Activation(payment::Activation { r#type: None })),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("payment").is_none());

        // gift badge：测试内模拟 Signal 服务器签发（deterministic
        // randomness），投影全四键（expiration 秒→毫秒同官方换算）。
        use zkgroup::api::server_params::ServerSecretParams;
        let server_secret = ServerSecretParams::generate([0xC0; 32]);
        let server_public = server_secret.get_public_params();
        let context =
            server_public.create_receipt_credential_request_context([0xC1; 32], [0x42_u8; 16]);
        let response = server_secret.issue_receipt_credential(
            [0xC2; 32],
            &context.get_request(),
            zkgroup::Timestamp::from_epoch_seconds(86_400 * 20_500),
            2,
        );
        let credential = server_public
            .receive_receipt_credential(&context, &response)
            .expect("credential");
        let presentation =
            server_public.create_receipt_credential_presentation([0xC3; 32], &credential);
        let presentation_bytes = zkgroup::serialize(&presentation);
        let out = project_data_message(
            &DataMessage {
                gift_badge: Some(presage::proto::data_message::GiftBadge {
                    receipt_credential_presentation: Some(presentation_bytes.clone()),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(
            out["giftBadge"]["expiration"],
            json!(86_400_u64 * 20_500 * 1000)
        );
        assert_eq!(out["giftBadge"]["level"], json!(2));
        assert_eq!(
            out["giftBadge"]["receiptCredentialPresentation"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&presentation_bytes)
                    .as_str()
            )
        );
        assert_eq!(out["giftBadge"]["state"], json!("UNOPENED"));

        // presentation 空 = 官方 undefined 整键不在；坏字节 = 降级材料两键
        // （官方构造 throw 炸整条消息，引擎宁收勿丢不镜像）。
        let out = project_data_message(
            &DataMessage {
                gift_badge: Some(presage::proto::data_message::GiftBadge {
                    receipt_credential_presentation: Some(Vec::new()),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("giftBadge").is_none());

        let garbage = vec![0x00_u8; 32];
        let out = project_data_message(
            &DataMessage {
                gift_badge: Some(presage::proto::data_message::GiftBadge {
                    receipt_credential_presentation: Some(garbage.clone()),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(
            out["giftBadge"]["receiptCredentialPresentation"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&garbage)
                    .as_str()
            )
        );
        assert_eq!(out["giftBadge"]["state"], json!("UNOPENED"));
        assert!(out["giftBadge"].get("expiration").is_none());
        assert!(out["giftBadge"].get("level").is_none());
    }

    /// P14 收投影缺批（组 1）：TypingMessage.groupId=3 收投影——群 typing
    /// 面位（标准 base64，非空才投）；1:1 缺席键不在（旧宿主零变化）。
    #[tokio::test]
    async fn typing_projection_carries_group_id() {
        use base64::Engine as _;
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-typing-group-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };

        // 群 typing：groupId 标准 base64 投出。
        let content = Content::from_body(
            ContentBody::TypingMessage(presage::proto::TypingMessage {
                action: Some(presage::proto::typing_message::Action::Started as i32),
                timestamp: Some(1_700_000_000_000),
                group_id: Some(vec![0xAB_u8; 32]),
            }),
            metadata.clone(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "typing projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["typingMessage"]["groupId"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0xAB_u8; 32])
                    .as_str()
            )
        );

        // 1:1 typing：groupId 键不在。
        let content = Content::from_body(
            ContentBody::TypingMessage(presage::proto::TypingMessage {
                action: Some(presage::proto::typing_message::Action::Started as i32),
                timestamp: Some(1_700_000_000_000),
                group_id: None,
            }),
            metadata,
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "typing projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope["typingMessage"].get("groupId").is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P14 收投影缺批（组 1）：Sent 三位可选面——isRecipientUpdate=true 才
    /// 投、unidentifiedStatus[]（destinationUuid binary 优先/字符串兜底、
    /// unidentified、destinationPniIdentityKey base64 非空才投）、
    /// storyMessageRecipients[]（destinationUuid、distributionListIds 原样、
    /// isAllowedToReply Some 才投）。空批/缺省键不在（旧宿主零变化）。
    #[tokio::test]
    async fn sent_sync_projection_carries_p14_three_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-sent-faces-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };

        let base_sent = || sync_message::Sent {
            timestamp: Some(1_699_999_999_000),
            message: Some(DataMessage {
                body: Some("echo".to_owned()),
                timestamp: Some(1_699_999_999_000),
                ..Default::default()
            }),
            ..Default::default()
        };

        // 有：三位全投（PNI 17B binary 优先解析；uuid string 形态兜底在
        // 二进制缺席臂单测钉）。
        let pni_bytes =
            ServiceId::parse_from_service_id_string("PNI:00000000-0000-0000-0000-000000000001")
                .expect("pni")
                .raw_uuid()
                .as_bytes()
                .to_vec();
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::Sent(sync_message::Sent {
                    is_recipient_update: Some(true),
                    unidentified_status: vec![sync_message::sent::UnidentifiedDeliveryStatus {
                        destination_service_id_binary: Some(
                            [[0x01_u8].as_slice(), pni_bytes.as_slice()].concat(),
                        ),
                        unidentified: Some(true),
                        destination_pni_identity_key: Some(vec![0x88_u8; 32]),
                        ..Default::default()
                    }],
                    story_message_recipients: vec![sync_message::sent::StoryMessageRecipient {
                        destination_service_id: Some(
                            "11111111-2222-4333-8444-555555555555".to_owned(),
                        ),
                        distribution_list_ids: vec![
                            "00000000-0000-0000-0000-000000000000".to_owned()
                        ],
                        is_allowed_to_reply: Some(false),
                        ..Default::default()
                    }],
                    ..base_sent()
                })),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let sent = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "sent projects",
        )["params"]["envelope"]["syncMessage"]["sentMessage"]
            .clone();
        assert_eq!(sent["isRecipientUpdate"], json!(true));
        assert_eq!(
            sent["unidentifiedStatus"][0]["destinationUuid"].as_str(),
            Some("PNI:00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(sent["unidentifiedStatus"][0]["unidentified"], json!(true));
        assert!(sent["unidentifiedStatus"][0]["destinationPniIdentityKey"]
            .as_str()
            .is_some());
        // 16B ACI binary（无前缀）同面解析；字符串形态兜底臂在
        // storyMessageRecipients 条目钉（destination_service_id 字符串）。
        assert_eq!(
            sent["storyMessageRecipients"][0]["destinationUuid"].as_str(),
            Some("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(
            sent["storyMessageRecipients"][0]["distributionListIds"],
            json!(["00000000-0000-0000-0000-000000000000"])
        );
        assert_eq!(
            sent["storyMessageRecipients"][0]["isAllowedToReply"],
            json!(false)
        );

        // 缺：isRecipientUpdate=false/缺席、两数组空批 → 三键全不在。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::Sent(base_sent())),
                ..Default::default()
            }),
            metadata,
        );
        let sent = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "sent projects",
        )["params"]["envelope"]["syncMessage"]["sentMessage"]
            .clone();
        assert!(sent.get("isRecipientUpdate").is_none());
        assert!(sent.get("unidentifiedStatus").is_none());
        assert!(sent.get("storyMessageRecipients").is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P14 组 5：sync 七整臂收投影——Configuration 四旗/FetchLatest type 词/
    /// CallLinkUpdate{type,rootKey,adminKey}/CallLogEvent 四键/
    /// DeviceNameChange{deviceId}/UsernameChange{} 纯信号面/PniChangeNumber
    /// 仅 {registrationId,newE164}（密钥材料不出引擎边界）。缺席键不在
    /// （旧宿主零变化）；官方 handleXxx 同位（MessageReceiver.preload.ts
    /// :3084/:3123/:3129/:3141/:3153/:3362）。
    #[tokio::test]
    async fn sync_seven_arm_projection_carries_p14_faces() {
        use base64::Engine as _;
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-sync-seven-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let make = |sync_content| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    content: Some(sync_content),
                    ..Default::default()
                }),
                metadata.clone(),
            )
        };
        async fn sync_arm_face(
            store: &SqliteStore,
            registry: &groups::TerminatedGroups,
            content: presage::libsignal_service::content::Content,
        ) -> serde_json::Value {
            pop_unwrap(
                project_content("acct", store, &content, registry).await,
                "sync arm projects",
            )["params"]["envelope"]["syncMessage"]
                .clone()
        }

        // Configuration：Some 才投（四旗两在两缺）。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::Configuration(
                sync_message::Configuration {
                    read_receipts: Some(true),
                    unidentified_delivery_indicators: None,
                    typing_indicators: Some(false),
                    link_previews: None,
                },
            )),
        )
        .await;
        assert_eq!(face["configuration"]["readReceipts"], json!(true));
        assert_eq!(face["configuration"]["typingIndicators"], json!(false));
        assert!(face["configuration"]
            .get("unidentifiedDeliveryIndicators")
            .is_none());
        assert!(face["configuration"].get("linkPreviews").is_none());

        // FetchLatest：type 词 SCREAMING；缺席照投空对象（信号面即事件）。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::FetchLatest(
                sync_message::FetchLatest {
                    r#type: Some(sync_message::fetch_latest::Type::LocalProfile as i32),
                },
            )),
        )
        .await;
        assert_eq!(face["fetchLatest"]["type"], json!("LOCAL_PROFILE"));
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::FetchLatest(
                sync_message::FetchLatest { r#type: None },
            )),
        )
        .await;
        assert_eq!(face["fetchLatest"], json!({}));

        // CallLinkUpdate：{type,rootKey,adminKey}（adminKey 键名沿官方事件面，
        // proto 字段 adminPasskey）；type 缺席官方 throw 不镜像（宁收勿丢）。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::CallLinkUpdate(
                sync_message::CallLinkUpdate {
                    root_key: Some(vec![0xCD_u8; 32]),
                    admin_passkey: Some(vec![0xEF_u8; 32]),
                    r#type: Some(sync_message::call_link_update::Type::Update as i32),
                },
            )),
        )
        .await;
        assert_eq!(face["callLinkUpdate"]["type"], json!("UPDATE"));
        assert_eq!(
            face["callLinkUpdate"]["rootKey"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0xCD_u8; 32])
                    .as_str()
            )
        );
        assert_eq!(
            face["callLinkUpdate"]["adminKey"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0xEF_u8; 32])
                    .as_str()
            )
        );
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::CallLinkUpdate(
                sync_message::CallLinkUpdate {
                    root_key: None,
                    admin_passkey: None,
                    r#type: None,
                },
            )),
        )
        .await;
        assert_eq!(face["callLinkUpdate"], json!({}));

        // CallLogEvent：四键透传（type 词 SCREAMING、conversationId 原字节
        // 标准 base64）。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::CallLogEvent(
                sync_message::CallLogEvent {
                    r#type: Some(sync_message::call_log_event::Type::MarkedAsRead as i32),
                    timestamp: Some(1_700_000_000_000),
                    conversation_id: Some(vec![0x11_u8; 16]),
                    call_id: Some(424_242),
                },
            )),
        )
        .await;
        assert_eq!(face["callLogEvent"]["type"], json!("MARKED_AS_READ"));
        assert_eq!(
            face["callLogEvent"]["timestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert_eq!(
            face["callLogEvent"]["conversationId"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0x11_u8; 16])
                    .as_str()
            )
        );
        assert_eq!(face["callLogEvent"]["callId"], json!(424_242_u64));

        // DeviceNameChange：{deviceId}；缺席键不在。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::DeviceNameChange(
                sync_message::DeviceNameChange { device_id: Some(2) },
            )),
        )
        .await;
        assert_eq!(face["deviceNameChange"]["deviceId"], json!(2));
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::DeviceNameChange(
                sync_message::DeviceNameChange { device_id: None },
            )),
        )
        .await;
        assert_eq!(face["deviceNameChange"], json!({}));

        // UsernameChange：空消息纯信号面。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::UsernameChange(
                sync_message::UsernameChange {},
            )),
        )
        .await;
        assert_eq!(face["usernameChange"], json!({}));

        // PniChangeNumber：只投 {registrationId,newE164}，三个密钥材料字段
        // 不出引擎边界（键都不在）。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::PniChangeNumber(
                sync_message::PniChangeNumber {
                    identity_key_pair: Some(vec![0x77_u8; 32]),
                    signed_pre_key: Some(vec![0x88_u8; 32]),
                    last_resort_kyber_pre_key: Some(vec![0x99_u8; 32]),
                    registration_id: Some(4242),
                    new_e164: Some("+15550001111".to_owned()),
                },
            )),
        )
        .await;
        assert_eq!(face["pniChangeNumber"]["registrationId"], json!(4242));
        assert_eq!(face["pniChangeNumber"]["newE164"], json!("+15550001111"));
        assert!(face["pniChangeNumber"].get("identityKeyPair").is_none());
        assert!(face["pniChangeNumber"].get("signedPreKey").is_none());
        assert!(face["pniChangeNumber"]
            .get("lastResortKyberPreKey")
            .is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P14 组 6：Keys 整臂收投影——mediaRootBackupKey 标准 base64（非空才
    /// 投，官方 Bytes.isNotEmpty 同口径）；accountEntropyPool 密钥材料不投
    /// （键不在，§5 组 6 块）；键全缺照投空对象（信号面即事件）。
    #[tokio::test]
    async fn keys_projection_carries_media_root_backup_key() {
        use base64::Engine as _;
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-keys-face-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let make = |keys| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    content: Some(sync_message::Content::Keys(keys)),
                    ..Default::default()
                }),
                metadata.clone(),
            )
        };

        // 有：mediaRootBackupKey 标准 base64 投出；AEP 不投（键不在）。
        let envelope = pop_unwrap(
            project_content(
                "acct",
                &store,
                &make(sync_message::Keys {
                    account_entropy_pool: Some("pool-secret".to_owned()),
                    media_root_backup_key: Some(vec![0xAB_u8; 32]),
                }),
                &registry,
            )
            .await,
            "keys projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["keys"]["mediaRootBackupKey"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0xAB_u8; 32])
                    .as_str()
            )
        );
        assert!(envelope["syncMessage"]["keys"]
            .get("accountEntropyPool")
            .is_none());

        // 缺：全缺照投空对象。
        let envelope = pop_unwrap(
            project_content(
                "acct",
                &store,
                &make(sync_message::Keys::default()),
                &registry,
            )
            .await,
            "keys projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["syncMessage"]["keys"], json!({}));

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn data_message_projection_carries_pin_unpin_admin_delete() {
        use presage::proto::data_message::{pin_message, AdminDelete, PinMessage, UnpinMessage};
        let author = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let author_bytes = uuid_bytes(author);
        let mut numbers = HashMap::new();
        numbers.insert(author.to_owned(), "+15551230000".to_owned());

        // 定时置顶：pinDurationSeconds 存在。
        let out = project_data_message(
            &DataMessage {
                pin_message: Some(PinMessage {
                    target_author_aci_binary: Some(author_bytes.clone()),
                    target_sent_timestamp: Some(1_700_000_000_000),
                    pin_duration: Some(pin_message::PinDuration::PinDurationSeconds(86_400)),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["pinMessage"]["targetAuthor"], json!("+15551230000"));
        assert_eq!(
            out["pinMessage"]["targetSentTimestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert_eq!(out["pinMessage"]["pinDurationSeconds"], json!(86_400));

        // 永久置顶：pinDurationSeconds 缺席（官方 pinDuration null 语义）。
        let out = project_data_message(
            &DataMessage {
                pin_message: Some(PinMessage {
                    target_author_aci_binary: Some(author_bytes.clone()),
                    target_sent_timestamp: Some(1),
                    pin_duration: Some(pin_message::PinDuration::PinDurationForever(true)),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out["pinMessage"].get("pinDurationSeconds").is_none());

        // 取消置顶与管理员删除：targetAuthor 同 E.164 口径。
        let out = project_data_message(
            &DataMessage {
                unpin_message: Some(UnpinMessage {
                    target_author_aci_binary: Some(author_bytes.clone()),
                    target_sent_timestamp: Some(2),
                }),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["unpinMessage"]["targetAuthor"], json!("+15551230000"));
        assert_eq!(out["unpinMessage"]["targetSentTimestamp"], json!(2));

        let out = project_data_message(
            &DataMessage {
                admin_delete: Some(AdminDelete {
                    target_author_aci_binary: Some(author_bytes),
                    target_sent_timestamp: Some(3),
                }),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(out["adminDelete"]["targetAuthor"], json!(author));
        assert_eq!(out["adminDelete"]["targetSentTimestamp"], json!(3));
    }

    /// bodyRanges 收包投影 `textStyles` 键（connector 契约 1.69 §4.62 收侧
    /// 对称面，wire 三仓并行批钉定）：DataMessage.body_ranges 的 Style arm
    /// 逐条投影 `textStyles[] {style, start, length}`——五值枚举词原样大写
    /// （proto 枚举词表）、NONE 不投（与发送面拒收对称）、未知枚举值整条
    /// 丢弃（proto 只投影不发明，未知值不 fatal）、start+length 超 body
    /// UTF-16 长度的形状坏条目丢弃（官方渲染侧不发明闸，只丢形状坏的）、
    /// 重复/重叠条目按官方容忍口径照投（官方渲染侧直接按序应用，投影侧
    /// 不发明重叠闸）、无 Style 条目不携键（缺席=纯文本，旧宿主零变化）、
    /// 条目保持 wire 顺序（官方渲染侧按序应用）。
    #[test]
    fn data_message_projection_projects_text_styles_wire_contract() {
        let style_range = |name: i32, start: u32, length: u32| BodyRange {
            start: Some(start),
            length: Some(length),
            associated_value: Some(AssociatedValue::Style(name)),
        };

        // 五值各一：词表原样大写、数值位 = proto 字段值（u32）。
        let five = [
            (presage::proto::body_range::Style::Bold, "BOLD"),
            (presage::proto::body_range::Style::Italic, "ITALIC"),
            (presage::proto::body_range::Style::Spoiler, "SPOILER"),
            (
                presage::proto::body_range::Style::Strikethrough,
                "STRIKETHROUGH",
            ),
            (presage::proto::body_range::Style::Monospace, "MONOSPACE"),
        ];
        for (value, word) in five {
            let out = project_data_message(
                &DataMessage {
                    body: Some("hello world".to_owned()),
                    body_ranges: vec![style_range(value as i32, 0, 5)],
                    ..Default::default()
                },
                &HashMap::new(),
            );
            assert_eq!(
                out["textStyles"],
                json!([{ "style": word, "start": 0, "length": 5 }]),
                "{word} projects with proto enum word"
            );
        }

        // NONE 不投：条目被滤掉后无剩余 → 键整体缺席（非空数组）。
        let out = project_data_message(
            &DataMessage {
                body: Some("plain".to_owned()),
                body_ranges: vec![style_range(
                    presage::proto::body_range::Style::None as i32,
                    0,
                    5,
                )],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(
            out.get("textStyles").is_none(),
            "NONE entry is not projected"
        );

        // 未知枚举值整条丢弃，不 fatal；同行 mention 条目不受牵连。
        let out = project_data_message(
            &DataMessage {
                body: Some("mixed".to_owned()),
                body_ranges: vec![
                    style_range(99, 0, 5),
                    BodyRange {
                        start: Some(0),
                        length: Some(5),
                        associated_value: Some(AssociatedValue::MentionAci(
                            "11111111-2222-4333-8444-555555555555".to_owned(),
                        )),
                    },
                ],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("textStyles").is_none(), "unknown enum dropped");
        assert_eq!(
            out["mentions"][0]["number"],
            json!("11111111-2222-4333-8444-555555555555")
        );

        // start+length 超 body UTF-16 长度：形状坏条目丢弃（含跨 BMP 代理
        // 对的 UTF-16 口径：7 个 UTF-16 code units ≠ 7 个 char）。
        let out = project_data_message(
            &DataMessage {
                // "a😀b" = 4 UTF-16 code units（😀 占 2）。
                body: Some("a😀b".to_owned()),
                body_ranges: vec![
                    style_range(presage::proto::body_range::Style::Bold as i32, 0, 4), // 恰好贴边
                    style_range(presage::proto::body_range::Style::Italic as i32, 3, 2), // 3+2>4 越界
                ],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(
            out["textStyles"],
            json!([{ "style": "BOLD", "start": 0, "length": 4 }]),
            "out-of-bounds entry dropped, boundary entry kept"
        );

        // 无 Style 条目 = 不携键（缺席=纯文本，旧宿主零变化）；
        // 仅 mention 条目时同样不携键。
        let out = project_data_message(
            &DataMessage {
                body: Some("no styles".to_owned()),
                body_ranges: vec![BodyRange {
                    start: Some(0),
                    length: Some(2),
                    associated_value: Some(AssociatedValue::MentionAci(
                        "11111111-2222-4333-8444-555555555555".to_owned(),
                    )),
                }],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("textStyles").is_none(), "mention-only: no key");
        let out = project_data_message(
            &DataMessage {
                body: Some("plain body".to_owned()),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("textStyles").is_none(), "no body_ranges: no key");

        // 多条目保序：wire 顺序 = 投影顺序（官方渲染侧按序应用）；重复/
        // 重叠条目不做重叠闸（官方容忍口径），照投。
        let out = project_data_message(
            &DataMessage {
                body: Some("ordered body".to_owned()),
                body_ranges: vec![
                    style_range(presage::proto::body_range::Style::Monospace as i32, 0, 7),
                    style_range(presage::proto::body_range::Style::Bold as i32, 0, 7),
                    style_range(presage::proto::body_range::Style::Italic as i32, 8, 4),
                ],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(
            out["textStyles"],
            json!([
                { "style": "MONOSPACE", "start": 0, "length": 7 },
                { "style": "BOLD", "start": 0, "length": 7 },
                { "style": "ITALIC", "start": 8, "length": 4 },
            ]),
            "entries keep wire order, overlaps tolerated"
        );

        // start/length 缺席 = 0 缺省（proto 只投影不发明，mentions 同款
        // unwrap_or(0) 口径）：0+0 ≤ body 长度恒成立，条目保留。
        let out = project_data_message(
            &DataMessage {
                body: Some("x".to_owned()),
                body_ranges: vec![BodyRange {
                    start: None,
                    length: None,
                    associated_value: Some(AssociatedValue::Style(
                        presage::proto::body_range::Style::Bold as i32,
                    )),
                }],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(
            out["textStyles"],
            json!([{ "style": "BOLD", "start": 0, "length": 0 }]),
            "missing start/length default to 0"
        );
    }

    /// 贴纸收发面（官方 sendStickerMessage / DataMessage.Sticker 同构）：
    /// 接收投影 packId hex + packKey base64 官方序列化同构、data 经附件指针
    /// 投影；send 构造 packId/packKey 解码 fail-closed；quote 可共存（官方
    /// 贴纸回应 StagedStickerReply 同一 DataMessage sticker+quote，组装与
    /// 文本分支同源：author 不可解析 fail-closed、正文找回降级缺 text）。
    #[tokio::test]
    async fn sticker_send_build_and_receive_projection() {
        use presage::proto::data_message::Sticker;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-sticker-quote-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();

        // send 构造：合法 hex packId + base64 packKey → DM 贴纸面齐备。
        let sticker = SendSticker {
            pack_id: "0123456789abcdef".to_owned(),
            pack_key: "QUJDREVG".to_owned(),
            sticker_id: 7,
            emoji: Some("😅".to_owned()),
            image: "data:image/webp;base64,aGVsbG8=".to_owned(),
            width: Some(512),
            height: Some(512),
        };
        let pointer = presage::proto::AttachmentPointer {
            content_type: Some("image/webp".to_owned()),
            ..Default::default()
        };
        let dm =
            build_sticker_data_message(&store, &sticker, None, pointer.clone(), 1_700_000_000_000)
                .await
                .unwrap();
        assert!(dm.body.is_none(), "官方贴纸消息 body=undefined");
        assert!(dm.quote.is_none(), "缺省 quote 不带键");
        let sent = dm.sticker.as_ref().unwrap();
        assert_eq!(
            sent.pack_id.as_deref(),
            Some(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef][..])
        );
        assert_eq!(sent.pack_key.as_deref(), Some(b"ABCDEF".as_slice()));
        assert_eq!(sent.sticker_id, Some(7));
        assert_eq!(sent.emoji.as_deref(), Some("😅"));
        assert_eq!(
            sent.data.as_ref().unwrap().content_type.as_deref(),
            Some("image/webp")
        );
        assert_eq!(dm.timestamp, Some(1_700_000_000_000));

        // fail-closed：奇数长 hex / 非 hex 字符 / 非 base64 / 空字节。
        async fn bad(
            store: &SqliteStore,
            base: &SendSticker,
            pack_id: &str,
            pack_key: &str,
        ) -> bool {
            build_sticker_data_message(
                store,
                &SendSticker {
                    pack_id: pack_id.to_owned(),
                    pack_key: pack_key.to_owned(),
                    ..base.clone()
                },
                None,
                presage::proto::AttachmentPointer::default(),
                1,
            )
            .await
            .is_err()
        }
        assert!(
            bad(&store, &sticker, "012", "QUJDREVG").await,
            "odd-length hex"
        );
        assert!(
            bad(&store, &sticker, "0123456789abcdeg", "QUJDREVG").await,
            "non-hex char"
        );
        assert!(bad(&store, &sticker, "0123456789abcdef", "not@base64!").await);
        assert!(
            bad(&store, &sticker, "0123456789abcdef", "").await,
            "empty packKey"
        );
        assert!(bad(&store, &sticker, "", "QUJDREVG").await, "empty packId");

        // quote 透传：ACI 直通 author，正文 store 无命中降级缺 text（与文本
        // 分支同口径）；author 不可解析 fail-closed。
        let aci = "11111111-2222-4333-8444-555555555555";
        let quoted = build_sticker_data_message(
            &store,
            &sticker,
            Some(&SendQuote {
                timestamp: 1_699_999_000_000,
                author: aci.to_owned(),
                gift_badge: false,
                body_ranges: Vec::new(),
            }),
            pointer.clone(),
            1_700_000_000_000,
        )
        .await
        .unwrap();
        assert!(quoted.body.is_none());
        let quote = quoted.quote.as_ref().expect("quote coexists with sticker");
        assert_eq!(quote.id, Some(1_699_999_000_000));
        assert_eq!(quote.author_aci.as_deref(), Some(aci));
        assert!(quote.text.is_none(), "store 无命中降级缺 text");
        assert!(
            build_sticker_data_message(
                &store,
                &sticker,
                Some(&SendQuote {
                    timestamp: 1,
                    author: "+15550009999".to_owned(),
                    gift_badge: false,
                    body_ranges: Vec::new(),
                }),
                pointer,
                1,
            )
            .await
            .is_err(),
            "quote author unresolvable fail-closed（文本分支同口径）"
        );

        // 接收投影：packId hex + packKey base64（官方序列化逆运算），emoji
        // 缺席不带键，data 走附件指针投影（id=client_uuid）；quote+sticker
        // 同一信封双投影（贴纸回应形态防回归）。
        let out = project_data_message(
            &DataMessage {
                quote: Some(data_message::Quote {
                    id: Some(1_699_999_000_000),
                    author_aci: Some("11111111-2222-4333-8444-555555555555".to_owned()),
                    ..Default::default()
                }),
                sticker: Some(Sticker {
                    pack_id: Some(vec![0xde, 0xad, 0xbe, 0xef]),
                    pack_key: Some(b"KEY".to_vec()),
                    sticker_id: Some(3),
                    emoji: Some("🎉".to_owned()),
                    data: Some(presage::proto::AttachmentPointer {
                        client_uuid: Some(uuid_bytes("11111111-2222-4333-8444-555555555555")),
                        content_type: Some("image/webp".to_owned()),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(
            out["quote"]["author"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(out["quote"]["id"], json!(1_699_999_000_000_u64));
        assert_eq!(out["sticker"]["packId"], json!("deadbeef"));
        assert_eq!(out["sticker"]["packKey"], json!("S0VZ"));
        assert_eq!(out["sticker"]["stickerId"], json!(3));
        assert_eq!(out["sticker"]["emoji"], json!("🎉"));
        assert_eq!(
            out["sticker"]["data"]["id"],
            json!("11111111-2222-4333-8444-555555555555")
        );

        // hex 助手 round-trip 与坏输入。
        assert_eq!(
            hex_decode(&hex_encode(&[0x00, 0xff, 0x10])),
            Some(vec![0x00, 0xff, 0x10])
        );
        assert_eq!(hex_decode("0 1"), None);

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 群组接收投影：groupV2 master key → signal-cli 同款
    /// `groupInfo{groupId}`（标准 base64，connector 群会话管线同 id 口径）；
    /// 直聊（无 groupV2 / master key 缺席）不带该键。
    #[test]
    fn data_message_projection_carries_group_info_only_for_groups() {
        use base64::Engine as _;

        let master_key = [9u8; 32];
        let out = project_data_message(
            &DataMessage {
                body: Some("group hello".to_owned()),
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(
            out["groupInfo"]["groupId"],
            json!(base64::engine::general_purpose::STANDARD.encode(master_key))
        );
        assert_eq!(out["message"], json!("group hello"));

        // 直聊：不带 groupInfo 键。
        let out = project_data_message(
            &DataMessage {
                body: Some("dm hello".to_owned()),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("groupInfo").is_none());

        // groupV2 存在但 master key 缺席/为空：同样不带键（宁缺勿错）。
        let out = project_data_message(
            &DataMessage {
                group_v2: Some(presage::proto::GroupContextV2::default()),
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("groupInfo").is_none());
    }

    #[test]
    fn group_id_decodes_standard_base64_master_key() {
        let key = [7u8; 32];
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(key);
        assert_eq!(decode_group_id(&encoded).as_deref(), Some(key.as_slice()));
        // 非 32 字节（GV1 残留/截断）一律拒绝。
        assert_eq!(
            decode_group_id(&base64::engine::general_purpose::STANDARD.encode([7u8; 16])),
            None
        );
        assert_eq!(decode_group_id("not base64!!"), None);
    }

    /// 阅后即焚 E1：构造面 body 无/单附件/is_view_once=true，不碰
    /// requiredProtocolVersion（官方发送侧不 bump；presage send 面注入默认
    /// 0 = 官方初值）；媒体族 image|video 与附件数 fail-closed（参数面互斥
    /// 在 methods.rs，构造面兜底服务层不可绕过的形状）。
    #[test]
    fn view_once_send_build_and_fail_closed() {
        let pointer = |content_type: &str| presage::proto::AttachmentPointer {
            content_type: Some(content_type.to_owned()),
            ..Default::default()
        };
        for content_type in ["image/jpeg", "video/mp4"] {
            let dm = build_view_once_data_message(vec![pointer(content_type)], 1_700_000_000_000)
                .expect("valid view-once builds");
            assert_eq!(dm.is_view_once, Some(true));
            assert_eq!(dm.body, None);
            assert_eq!(dm.attachments.len(), 1);
            assert_eq!(
                dm.attachments[0].content_type.as_deref(),
                Some(content_type)
            );
            assert_eq!(dm.timestamp, Some(1_700_000_000_000));
            assert_eq!(dm.required_protocol_version, None, "官方不 bump");
            assert!(dm.quote.is_none());
            assert!(dm.preview.is_empty());
            assert!(dm.body_ranges.is_empty());
            assert!(dm.sticker.is_none());
        }
        // fail-closed：0 附件 / 2 附件 / audio / mediatype 缺席。
        assert!(build_view_once_data_message(Vec::new(), 1).is_err());
        assert!(
            build_view_once_data_message(vec![pointer("image/png"), pointer("image/png")], 1)
                .is_err()
        );
        assert!(build_view_once_data_message(vec![pointer("audio/mp4")], 1).is_err());
        assert!(build_view_once_data_message(
            vec![presage::proto::AttachmentPointer::default()],
            1
        )
        .is_err());
    }

    /// textStyle 发送构造面（connector 契约 1.69 §4.62 透传批）：
    /// - 五值词表逐值构造出 proto BodyRange.Style（线上枚举字节级断言，
    ///   tag=4 enumeration；NONE 词表外不进引擎）、offsets UTF-16 透传；
    /// - 与 mentions 同发 = MentionAci(tag=3)/Style(tag=4) 两 arm 共存于
    ///   body_ranges（官方 bodyRanges 语义）；
    /// - 无参数 = body_ranges 零差异（零回归锚：字节级 old==new）；
    /// - 词表外值构造面兜底 fail-closed（解析闸在 methods.rs，此处防御
    ///   服务层不可绕过的形状）。
    #[tokio::test]
    async fn text_style_send_build_faces() {
        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-text-style-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();

        // 合法面：五值白名单逐值构造 + 字节级断言（prost 线上编码，Style
        // enumeration = BodyRange 字段 4，值 = proto 枚举序数）。
        let styles = [
            ("BOLD", presage::proto::body_range::Style::Bold),
            ("ITALIC", presage::proto::body_range::Style::Italic),
            ("SPOILER", presage::proto::body_range::Style::Spoiler),
            (
                "STRIKETHROUGH",
                presage::proto::body_range::Style::Strikethrough,
            ),
            ("MONOSPACE", presage::proto::body_range::Style::Monospace),
        ];
        let entries: Vec<SendTextStyle> = styles
            .iter()
            .enumerate()
            .map(|(index, (name, _))| SendTextStyle {
                style: name.to_string(),
                start: index as u32 * 2,
                length: 1,
            })
            .collect();
        let dm = build_text_data_message(
            &store,
            "styled body",
            None,
            None,
            &[],
            &entries,
            Vec::new(),
            None,
            None,
            1_700_000_000_000,
        )
        .await
        .expect("styled text builds");
        assert_eq!(dm.body.as_deref(), Some("styled body"));
        assert_eq!(dm.body_ranges.len(), 5, "one BodyRange per entry");
        for (index, (name, value)) in styles.iter().enumerate() {
            let range = &dm.body_ranges[index];
            assert_eq!(range.start, Some(index as u32 * 2));
            assert_eq!(range.length, Some(1));
            match &range.associated_value {
                Some(AssociatedValue::Style(actual)) => {
                    assert_eq!(*actual, *value as i32, "{name} wire enum value");
                }
                other => panic!("{name}: expected Style arm, got {other:?}"),
            }
        }
        // 字节级：字段 4（tag byte 0x20 = field 4 wire type 0 varint）+
        // 枚举值逐字（含 tag=5 MentionAciBinary 不出现的负样本面）。
        for (index, (_, value)) in styles.iter().enumerate() {
            let range = dm.body_ranges[index].encode_to_vec();
            assert_eq!(
                range,
                vec![0x08, index as u8 * 2, 0x10, 0x01, 0x20, *value as i32 as u8],
                "{index}: BodyRange wire bytes"
            );
        }

        // 与 mentions 同发：MentionAci 与 Style 共存 body_ranges（arm 有序）。
        let mention_aci = "11111111-2222-4333-8444-555555555555";
        let dm = build_text_data_message(
            &store,
            "mixed",
            None,
            None,
            &[SendMention {
                number: mention_aci.to_owned(),
                start: 0,
                length: 4,
            }],
            &[SendTextStyle {
                style: "BOLD".to_owned(),
                start: 6,
                length: 4,
            }],
            Vec::new(),
            None,
            None,
            1,
        )
        .await
        .expect("mixed ranges build");
        assert_eq!(dm.body_ranges.len(), 2);
        assert!(matches!(
            dm.body_ranges[0].associated_value,
            Some(AssociatedValue::MentionAci(_))
        ));
        assert!(matches!(
            dm.body_ranges[1].associated_value,
            Some(AssociatedValue::Style(1)) // BOLD
        ));
        assert_eq!(dm.body_ranges[1].start, Some(6));
        assert_eq!(dm.body_ranges[1].length, Some(4));

        // 零回归锚：无参数（textStyle 键缺席）构造字节 = 旧版形状逐字节
        // 一致（无 body_ranges 字段产生）。
        let legacy = build_text_data_message(
            &store,
            "plain",
            None,
            None,
            &[],
            &[],
            Vec::new(),
            None,
            None,
            1,
        )
        .await
        .expect("plain text builds");
        assert!(legacy.body_ranges.is_empty());
        let expected_legacy = DataMessage {
            timestamp: Some(1),
            body: Some("plain".to_owned()),
            ..DataMessage::default()
        }
        .encode_to_vec();
        assert_eq!(
            legacy.encode_to_vec(),
            expected_legacy,
            "no-param wire bytes unchanged"
        );

        // 构造面兜底 fail-closed：词表外 style（解析闸之后服务层防御）。
        assert!(
            build_text_data_message(
                &store,
                "x",
                None,
                None,
                &[],
                &[SendTextStyle {
                    style: "NONE".to_owned(),
                    start: 0,
                    length: 1,
                }],
                Vec::new(),
                None,
                None,
                1,
            )
            .await
            .is_err(),
            "NONE rejected at build layer too"
        );
    }

    /// P14 组 4：story 投影面——bodyRanges=6 分流 `mentions[]`/`textStyles[]`
    /// （mention binary 16B→UUID、形状坏条目丢弃）与色位/背景投影
    /// （textForegroundColor/textBackgroundColor u32 原样、background oneof
    /// gradient|color，官方 TextAttachment.Params 同形键）。
    #[test]
    fn story_projection_carries_body_ranges_and_color_faces() {
        use presage::proto::text_attachment::{Background, Gradient};

        let mention_uuid_bytes = [
            0x11_u8, 0x11, 0x11, 0x11, 0x22, 0x22, 0x43, 0x33, 0x84, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55,
        ];
        let story = presage::proto::StoryMessage {
            attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                presage::proto::TextAttachment {
                    text: Some("story body".to_owned()),
                    text_style: Some(2),
                    text_foreground_color: Some(0xFF00_FF00),
                    text_background_color: Some(0),
                    background: Some(Background::Gradient(Gradient {
                        start_color: Some(0x1122_3344),
                        end_color: None,
                        angle: Some(90),
                        colors: vec![0xA, 0xB],
                        positions: vec![0.0, 1.0],
                    })),
                    preview: None,
                },
            )),
            body_ranges: vec![
                BodyRange {
                    start: Some(0),
                    length: Some(5),
                    associated_value: Some(AssociatedValue::MentionAciBinary(
                        mention_uuid_bytes.to_vec(),
                    )),
                },
                BodyRange {
                    start: Some(6),
                    length: Some(4),
                    associated_value: Some(AssociatedValue::Style(
                        presage::proto::body_range::Style::Bold as i32,
                    )),
                },
                // 形状坏条目：start+length 超 story 正文 UTF-16 长度 → 丢。
                BodyRange {
                    start: Some(50),
                    length: Some(100),
                    associated_value: Some(AssociatedValue::Style(
                        presage::proto::body_range::Style::Italic as i32,
                    )),
                },
            ],
            ..Default::default()
        };
        let out = project_story_message(&story, &HashMap::new()).expect("story projection");
        let text = &out["textAttachment"];
        assert_eq!(text["textForegroundColor"], json!(0xFF00_FF00_u32));
        // u32 原样：Some(0) 也是线上陈述（显式 0 = 合法色值）。
        assert_eq!(text["textBackgroundColor"], json!(0_u32));
        assert_eq!(
            text["background"]["gradient"]["startColor"],
            json!(0x1122_3344_u32)
        );
        assert!(text["background"]["gradient"].get("endColor").is_none());
        assert_eq!(text["background"]["gradient"]["angle"], json!(90));
        assert_eq!(text["background"]["gradient"]["colors"], json!([10, 11]));
        assert_eq!(
            text["background"]["gradient"]["positions"],
            json!([0.0, 1.0])
        );
        assert_eq!(text["mentions"][0]["start"], json!(0));
        assert_eq!(text["mentions"][0]["length"], json!(5));
        assert_eq!(
            text["mentions"][0]["number"].as_str().map(str::len),
            Some(36)
        );
        assert_eq!(text["textStyles"][0]["style"], json!("BOLD"));
        assert_eq!(text["textStyles"].as_array().map(Vec::len), Some(1));

        // color 臂：background {color} 单键。
        let story = presage::proto::StoryMessage {
            attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                presage::proto::TextAttachment {
                    text: Some("hi".to_owned()),
                    background: Some(Background::Color(0xABCDEF)),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let out = project_story_message(&story, &HashMap::new()).expect("story projection");
        assert_eq!(
            out["textAttachment"]["background"]["color"],
            json!(0xABCDEF_u32)
        );
    }

    /// P14 组 4：story 回复发送面——story_context 构造（author ACI 解析 +
    /// sent_timestamp 直投，string 形 authorAci 沿 mentions/quote 先例，
    /// binary 位不写）；无参数 = story_context 缺席（旧字节零变化）；作者
    /// 不可解析 fail-closed。
    #[tokio::test]
    async fn story_context_send_build_faces() {
        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-ctx-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();

        let author = "11111111-2222-4333-8444-555555555555";
        let dm = build_text_data_message(
            &store,
            "reply",
            None,
            None,
            &[],
            &[],
            Vec::new(),
            None,
            Some(&SendStoryContext {
                author: author.to_owned(),
                sent_timestamp: 1_700_000_000_000,
            }),
            7,
        )
        .await
        .expect("story reply builds");
        let ctx = dm.story_context.expect("storyContext set");
        assert_eq!(ctx.author_aci.as_deref(), Some(author));
        assert_eq!(ctx.sent_timestamp, Some(1_700_000_000_000));
        assert!(ctx.author_aci_binary.is_none());

        // 无参数 = story_context 缺席。
        let dm = build_text_data_message(
            &store,
            "plain",
            None,
            None,
            &[],
            &[],
            Vec::new(),
            None,
            None,
            7,
        )
        .await
        .expect("plain builds");
        assert!(dm.story_context.is_none());

        // 作者不可解析 fail-closed。
        assert!(build_text_data_message(
            &store,
            "x",
            None,
            None,
            &[],
            &[],
            Vec::new(),
            None,
            Some(&SendStoryContext {
                author: "not-a-recipient".to_owned(),
                sent_timestamp: 1,
            }),
            7,
        )
        .await
        .is_err());
    }

    /// 阅后即焚 E2：收投影对 viewOnce:true 行跑官方 isValidTapToView 同构
    /// 校验，非法行补 viewOnceInvalid（metadata-only 只标记不擦除）；合法行
    /// 不带该键；非 view-once 行完全不受影响。
    #[test]
    fn view_once_receive_projection_marks_invalid_rows() {
        let image_pointer = presage::proto::AttachmentPointer {
            content_type: Some("image/jpeg".to_owned()),
            ..Default::default()
        };
        let audio_pointer = presage::proto::AttachmentPointer {
            content_type: Some("audio/mp4".to_owned()),
            ..Default::default()
        };

        // 合法行：viewOnce:true，无 viewOnceInvalid。
        let out = project_data_message(
            &DataMessage {
                is_view_once: Some(true),
                attachments: vec![image_pointer.clone()],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert_eq!(out["viewOnce"], json!(true));
        assert!(out.get("viewOnceInvalid").is_none());

        // 官方 body 空串 = falsy = 合法（isValidTapToView `if (body)` 同构）。
        let out = project_data_message(
            &DataMessage {
                is_view_once: Some(true),
                body: Some(String::new()),
                attachments: vec![image_pointer.clone()],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("viewOnceInvalid").is_none());

        // 非法逐面：body 非空 / 0 附件 / 2 附件 / audio / mediatype 缺席 /
        // quote / sticker / contact / preview。
        let invalid = |dm: DataMessage| {
            let out = project_data_message(&dm, &HashMap::new());
            assert_eq!(out["viewOnce"], json!(true));
            assert_eq!(out["viewOnceInvalid"], json!(true));
        };
        invalid(DataMessage {
            is_view_once: Some(true),
            body: Some("nope".to_owned()),
            attachments: vec![image_pointer.clone()],
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![image_pointer.clone(), image_pointer.clone()],
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![audio_pointer],
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![presage::proto::AttachmentPointer::default()],
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![image_pointer.clone()],
            quote: Some(data_message::Quote::default()),
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![image_pointer.clone()],
            sticker: Some(data_message::Sticker::default()),
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![image_pointer.clone()],
            contact: vec![presage::proto::data_message::Contact::default()],
            ..Default::default()
        });
        invalid(DataMessage {
            is_view_once: Some(true),
            attachments: vec![image_pointer],
            preview: vec![presage::proto::Preview::default()],
            ..Default::default()
        });

        // 非 view-once 行：多附件/带正文不触发任何 viewOnce 键。
        let out = project_data_message(
            &DataMessage {
                body: Some("plain".to_owned()),
                attachments: vec![
                    presage::proto::AttachmentPointer::default(),
                    presage::proto::AttachmentPointer::default(),
                ],
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(out.get("viewOnce").is_none());
        assert!(out.get("viewOnceInvalid").is_none());
    }

    /// 消失消息 E 批收投影三面：expiresInSeconds（含 0=关闭）/ isExpirationUpdate
    /// （flag 位）/ expirationStartTimestamp（Sent 面 >0）有/缺/组合——缺省键
    /// 不在；非过期行零新键。
    #[test]
    fn expiration_receive_projection_faces() {
        use presage::proto::data_message::Flags;
        let numbers = HashMap::new();

        // 有：timer + flag 位组合同投；expiresInSeconds 含 0（关闭语义透出）。
        let out = project_data_message(
            &DataMessage {
                expire_timer: Some(0),
                flags: Some(Flags::ExpirationTimerUpdate as u32),
                timestamp: Some(1_700_000_000_000),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["expiresInSeconds"], json!(0));
        assert_eq!(out["isExpirationUpdate"], json!(true));
        assert_eq!(out["timestamp"], json!(1_700_000_000_000_u64));

        let out = project_data_message(
            &DataMessage {
                expire_timer: Some(86_400),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["expiresInSeconds"], json!(86_400));
        assert!(out.get("isExpirationUpdate").is_none(), "无 flag 位不投");

        let out = project_data_message(
            &DataMessage {
                flags: Some(Flags::ExpirationTimerUpdate as u32),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("expiresInSeconds").is_none(), "timer 缺席键不在");
        assert_eq!(out["isExpirationUpdate"], json!(true));

        // flag 位组合性：ExpirationTimerUpdate 与其他位（ProfileKeyUpdate=4）
        // 共存的位掩码仍命中。
        let out = project_data_message(
            &DataMessage {
                flags: Some(Flags::ExpirationTimerUpdate as u32 | Flags::ProfileKeyUpdate as u32),
                ..Default::default()
            },
            &numbers,
        );
        assert_eq!(out["isExpirationUpdate"], json!(true));

        // 缺：普通行（正文/反应等）零过期键。
        let out = project_data_message(
            &DataMessage {
                body: Some("plain".to_owned()),
                ..Default::default()
            },
            &numbers,
        );
        assert!(out.get("expiresInSeconds").is_none());
        assert!(out.get("isExpirationUpdate").is_none());
        assert!(out.get("expirationStartTimestamp").is_none());

        // Sent 面起算时间：Some>0 投，Some(0)/None 键不在。
        let sent = |start: Option<u64>| presage::proto::sync_message::Sent {
            expiration_start_timestamp: start,
            ..Default::default()
        };
        assert_eq!(
            sent_expiration_start(&sent(Some(1_700_000_000_000))),
            Some(1_700_000_000_000)
        );
        assert_eq!(sent_expiration_start(&sent(Some(0))), None);
        assert_eq!(sent_expiration_start(&sent(None)), None);
    }

    /// 消失消息 E 批构造面：expiration-update 信封只设 flag 位 + timestamp，
    /// 不设 expire_timer/expire_timer_version（presage send 面
    /// restore_thread_timer 注入刚持久化的 (timer, version) 对；构造面显式
    /// 设置会再 bump 造成 store/线面脱节）；亦不碰 requiredProtocolVersion。
    #[test]
    fn expiration_timer_update_build_sets_flag_only() {
        use presage::proto::data_message::Flags;
        let dm = build_expiration_timer_update_data_message(1_700_000_000_000);
        assert_eq!(dm.flags, Some(Flags::ExpirationTimerUpdate as u32));
        assert_eq!(dm.timestamp, Some(1_700_000_000_000));
        assert_eq!(dm.expire_timer, None, "构造面不设 timer，注入归 presage");
        assert_eq!(dm.expire_timer_version, None);
        assert_eq!(dm.required_protocol_version, None, "官方不 bump");
        assert_eq!(dm.body, None);
        assert!(dm.attachments.is_empty());
        assert!(dm.body_ranges.is_empty());
    }

    /// 阅后即焚 E3：viewOnceOpen sync 投影 {senderAci, timestamp}（binary
    /// 优先 string 兜底，官方 parse_sender_aci 同构）；senderAci 不可解析 =
    /// warn-only 丢弃（None，零 stderr，compat §5 乱序兜底口径）；timestamp
    /// 缺席投影 0 不丢弃。
    #[test]
    fn view_once_open_sync_projection_and_warn_only_drop() {
        use presage::proto::sync_message::ViewOnceOpen;
        let aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

        // binary+string 双写：binary 优先（官方 parse_sender_aci 同构）。
        let out = project_view_once_open(&ViewOnceOpen {
            sender_aci: Some(aci.to_owned()),
            timestamp: Some(1_700_000_000_000),
            sender_aci_binary: Some(uuid_bytes(aci)),
        })
        .expect("dual form parses");
        assert_eq!(out["senderAci"], json!(aci));
        assert_eq!(out["timestamp"], json!(1_700_000_000_000_u64));

        // string-only / binary-only 两种线上形态都投影。
        let out = project_view_once_open(&ViewOnceOpen {
            sender_aci: Some(aci.to_owned()),
            timestamp: Some(7),
            sender_aci_binary: None,
        })
        .expect("string form parses");
        assert_eq!(out["senderAci"], json!(aci));
        let out = project_view_once_open(&ViewOnceOpen {
            sender_aci: None,
            timestamp: Some(8),
            sender_aci_binary: Some(uuid_bytes(aci)),
        })
        .expect("binary form parses");
        assert_eq!(out["senderAci"], json!(aci));

        // 不可解析（binary 坏 + string 缺）：warn-only 丢弃。
        assert!(project_view_once_open(&ViewOnceOpen {
            sender_aci: None,
            timestamp: Some(9),
            sender_aci_binary: Some(vec![0xde, 0xad]),
        })
        .is_none());

        // timestamp 缺席：投影 0，不丢弃。
        let out = project_view_once_open(&ViewOnceOpen {
            sender_aci: Some(aci.to_owned()),
            timestamp: None,
            sender_aci_binary: None,
        })
        .expect("missing timestamp still projects");
        assert_eq!(out["timestamp"], json!(0));
    }

    /// ADR 0005 §S2：serde 数据面是 `RegistrationData` pub(crate) 字段（
    /// password/profile_key）的唯一外部构造路径；JSON 形态 = presage 官方
    /// store 落库格式，往返锁死 = 格式稳定性等价 store 格式稳定性。
    #[test]
    fn registration_data_serde_official_store_shape_roundtrip() {
        use base64::Engine as _;
        use presage::libsignal_service::utils::TryIntoE164;

        let e164 = "+15551234567".try_into_e164().expect("e164");
        let phone_number = presage::libsignal_service::utils::phonenumber_from_signal(&e164);
        let value = json!({
            "signal_servers": "Production",
            "device_name": "KT Signal Engine",
            "phone_number": serde_json::to_value(&phone_number).expect("phone serializes"),
            "uuid": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
            "pni": "7b3dc19e-11f2-4c5a-9d0e-2f6a8b4c1d3e",
            "password": "abcdefghijklmnopqrstuvwx",
            "device_id": Some(2u32),
            "registration_id": 1234u32,
            "pni_registration_id": Some(5678u32),
            "profile_key": base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
        });

        let data: RegistrationData =
            serde_json::from_str(&value.to_string()).expect("deserializes");
        let round: Value = serde_json::to_value(&data).expect("serializes");
        assert_eq!(round, value);
        let data2: RegistrationData =
            serde_json::from_str(&round.to_string()).expect("re-deserializes");
        let round2: Value = serde_json::to_value(&data2).expect("re-serializes");
        assert_eq!(round2, value);
    }

    /// receive 信封投影 serverGuid 位（reportSpam 面）：metadata.server_guid
    /// 在场 → 信封带 serverGuid（UUID 串形态）；缺席 → 键不在（proto 只投影
    /// 不发明）。基础信封位（sourceUuid/sourceDevice/timestamp）同批防回归。
    #[tokio::test]
    async fn receive_envelope_projects_server_guid_when_present() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-server-guid-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = |server_guid| Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let server_guid = presage::libsignal_service::prelude::Uuid::parse_str(
            "f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        )
        .expect("server guid");

        // 在场：信封带 serverGuid（字符串形态）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("hello".to_owned()),
                ..Default::default()
            }),
            metadata(Some(server_guid)),
        );
        let registry = groups::TerminatedGroups::new();
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "dataMessage projects",
        );
        let envelope = &envelope["params"]["envelope"];
        assert_eq!(
            envelope["serverGuid"],
            json!("f81d4fae-7dec-11d0-a765-00a0c91e6bf6")
        );
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(envelope["sourceDevice"], json!(1));
        assert_eq!(envelope["timestamp"], json!(1_700_000_000_000_u64));

        // 缺席：键不在（不发明 null/空串）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage::default()),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "dataMessage projects",
        );
        assert!(envelope.get("serverGuid").is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P15a 项2 信封三标志投影（vendored Metadata 扩位捕获面）：urgent/
    /// ephemeral 在场即投真值（raw 位）、reportSpamToken 非空 base64（键名
    /// 沿官方事件面 reportingToken）、全缺席键不在（urgent wire 缺席 = proto
    /// 缺省 true、ephemeral 缺席 = false，§3 记档）。
    #[tokio::test]
    async fn receive_envelope_projects_urgent_ephemeral_reporting_token() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};

        use base64::Engine as _;
        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-envelope-flags-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let metadata = |ephemeral: Option<bool>,
                        urgent: Option<bool>,
                        report_spam_token: Option<Vec<u8>>| Metadata {
            sender,
            destination: sender,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral,
            urgent,
            report_spam_token,
        };
        // 三位在场：urgent/ephemeral raw 真值 + token 标准 base64。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage::default()),
            metadata(Some(true), Some(false), Some(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "dataMessage projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["ephemeral"], json!(true));
        assert_eq!(envelope["urgent"], json!(false));
        assert_eq!(
            envelope["reportingToken"],
            json!(base64::engine::general_purpose::STANDARD.encode([0xDE, 0xAD, 0xBE, 0xEF]))
        );

        // 空字节 token：官方 Bytes.isNotEmpty 闸同口径省键。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage::default()),
            metadata(Some(false), Some(true), Some(Vec::new())),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "dataMessage projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["ephemeral"], json!(false));
        assert_eq!(envelope["urgent"], json!(true));
        assert!(envelope.get("reportingToken").is_none());

        // 全缺席：三键都不在（旧宿主零变化）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage::default()),
            metadata(None, None, None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "dataMessage projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope.get("ephemeral").is_none());
        assert!(envelope.get("urgent").is_none());
        assert!(envelope.get("reportingToken").is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P15a 项3 contacts sync 投影（vendored ContactSync 载体）：records 逐
    /// 条 aci/number/name/expireTimer 族（0 值不投、空名不投）、complete
    /// 旗标、信封公共位（sourceUuid/sourceDevice/timestamp）、空记录照投
    /// 事件。
    #[test]
    fn contacts_sync_projects_parsed_records() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::libsignal_service::utils::{phonenumber_from_signal, TryIntoE164};
        use presage::model::contacts::Contact;
        use presage::model::messages::ContactSync;

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let metadata = Metadata {
            sender,
            destination: sender,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let e164 = "+15551234567".try_into_e164().expect("e164");
        let contacts = vec![
            Contact {
                uuid: presage::libsignal_service::prelude::Uuid::parse_str(
                    "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
                )
                .expect("uuid"),
                phone_number: Some(phonenumber_from_signal(&e164)),
                name: "Alice".to_owned(),
                verified: Default::default(),
                profile_key: Vec::new(),
                expire_timer: 0,
                expire_timer_version: 2,
                inbox_position: 3,
                avatar: None,
            },
            // 全零/空名条目：0 值与空名键不投，aci 照投。
            Contact {
                uuid: presage::libsignal_service::prelude::Uuid::parse_str(
                    "0b1c2d3e-4f50-4a1b-8c2d-3e4f50617283",
                )
                .expect("uuid"),
                phone_number: None,
                name: String::new(),
                verified: Default::default(),
                profile_key: Vec::new(),
                expire_timer: 0,
                expire_timer_version: 0,
                inbox_position: 0,
                avatar: None,
            },
        ];
        let sync = ContactSync {
            content: Box::new(Content::from_body(
                ContentBody::DataMessage(DataMessage::default()),
                metadata,
            )),
            complete: true,
            contacts,
        };
        let notification = pop_unwrap(
            project_contacts_sync("acct", &sync),
            "contacts sync projects",
        );
        assert_eq!(notification["method"], json!("receive"));
        let envelope = &notification["params"]["envelope"];
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(envelope["sourceDevice"], json!(1));
        assert_eq!(envelope["timestamp"], json!(1_700_000_000_000_u64));
        let face = &envelope["syncMessage"]["contacts"];
        assert_eq!(face["complete"], json!(true));
        let records = face["records"].as_array().expect("records");
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            json!({
                "aci": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
                "number": "+15551234567",
                "name": "Alice",
                "expireTimerVersion": 2,
                "inboxPosition": 3,
            })
        );
        assert_eq!(
            records[1],
            json!({ "aci": "0b1c2d3e-4f50-4a1b-8c2d-3e4f50617283" })
        );

        // 空记录照投事件（complete 旗标即信号）。
        let sync = ContactSync {
            content: sync.content,
            complete: false,
            contacts: Vec::new(),
        };
        let face = &pop_unwrap(project_contacts_sync("acct", &sync), "empty projects")["params"]
            ["envelope"]["syncMessage"]["contacts"];
        assert_eq!(face["complete"], json!(false));
        assert_eq!(face["records"], json!([]));
    }

    /// P15a 项4 Blocked 同步收投影：modern/legacy 家族选择（官方
    /// areModernFieldsUsed 判定同构——modern 任一非空走 modern、否则
    /// legacy；全空归 modern 空对象）、modern 三型 shape（aci 二进制→UUID
    /// 串/groupId base64/timestamp 缺席键不在）、legacy 四位 shape、混合
    /// 在场 modern 胜。
    #[tokio::test]
    async fn sync_projects_blocked_modern_and_legacy_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::sync_message::blocked::{BlockedAci, BlockedE164, BlockedGroup};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-blocked-sync-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let metadata = Metadata {
            sender,
            destination: sender,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let aci_bytes = |text: &str| {
            ServiceId::parse_from_service_id_string(text)
                .unwrap()
                .raw_uuid()
                .as_bytes()
                .to_vec()
        };
        let blocked_body = |blocked: sync_message::Blocked| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    content: Some(sync_message::Content::Blocked(blocked)),
                    ..SyncMessage::default()
                }),
                metadata.clone(),
            )
        };

        // modern 家族：三型齐投（e164 缺席条目丢），legacy 同场在场——
        // modern 非空胜（官方判定同构）。
        let content = blocked_body(sync_message::Blocked {
            blocked_e164s: vec![
                BlockedE164 {
                    e164: Some("+15557654321".to_owned()),
                    timestamp: Some(1_699_998_000_000),
                },
                // e164 缺席条目丢。
                BlockedE164 {
                    e164: None,
                    timestamp: Some(1_699_998_000_100),
                },
            ],
            blocked_acis: vec![BlockedAci {
                aci_binary: Some(aci_bytes("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")),
                timestamp: None,
            }],
            blocked_groups: vec![BlockedGroup {
                group_id: Some(vec![9u8; 32]),
                timestamp: Some(1_699_998_000_200),
            }],
            numbers: vec!["+15550000000".to_owned()],
            ..sync_message::Blocked::default()
        });
        let face = &pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "blocked projects",
        )["params"]["envelope"]["syncMessage"]["blocked"];
        assert!(face.get("numbers").is_none(), "modern 在场时 legacy 不投");
        assert_eq!(
            face["blockedE164s"],
            json!([{ "e164": "+15557654321", "timestamp": 1_699_998_000_000_u64 }])
        );
        assert_eq!(
            face["blockedAcis"],
            json!([{ "aci": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae" }])
        );
        assert_eq!(
            face["blockedGroups"],
            json!([{
                "groupId": base64::engine::general_purpose::STANDARD.encode([9u8; 32]),
                "timestamp": 1_699_998_000_200_u64,
            }])
        );

        // legacy 家族：modern 全空才走。
        let content = blocked_body(sync_message::Blocked {
            numbers: vec!["+15550000000".to_owned()],
            acis: vec!["6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae".to_owned()],
            group_ids: vec![vec![7u8; 32]],
            acis_binary: vec![aci_bytes("0b1c2d3e-4f50-4a1b-8c2d-3e4f50617283")],
            ..sync_message::Blocked::default()
        });
        let face = &pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "blocked legacy projects",
        )["params"]["envelope"]["syncMessage"]["blocked"];
        assert_eq!(face["numbers"], json!(["+15550000000"]));
        assert_eq!(
            face["acis"],
            json!(["6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"])
        );
        assert_eq!(
            face["groupIds"],
            json!([base64::engine::general_purpose::STANDARD.encode([7u8; 32])])
        );
        assert_eq!(
            face["acisBinary"],
            json!(["0b1c2d3e-4f50-4a1b-8c2d-3e4f50617283"])
        );

        // 全空：官方 `areModernFieldsUsed || !areLegacyFieldsUsed` 归 modern
        // 空对象（信号面即事件）。
        let content = blocked_body(sync_message::Blocked::default());
        let face = &pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "blocked empty projects",
        )["params"]["envelope"]["syncMessage"]["blocked"];
        assert_eq!(face, &json!({}));

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Stories 接收批（E2）：StoryMessage text 面投影——textAttachment
    /// {text, textStyle, preview.image 指针投影}、allowsReplies、profileKey
    /// base64（有值才投）、group 缺席键不在；isStory 位随信封 story 位；
    /// textStyle 线上在场（含 DEFAULT=0）照投词。
    #[tokio::test]
    async fn receive_story_message_projects_text_face() {
        use base64::Engine as _;
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{Preview, TextAttachment};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-text-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = |story| Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let story = |text_style| presage::proto::StoryMessage {
            profile_key: Some(vec![9u8; 32]),
            group: None,
            allows_replies: Some(true),
            body_ranges: Vec::new(),
            attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                TextAttachment {
                    text: Some("hello story".to_owned()),
                    text_style,
                    text_foreground_color: None,
                    text_background_color: None,
                    preview: Some(Preview {
                        url: None,
                        title: None,
                        image: Some(presage::proto::AttachmentPointer {
                            content_type: Some("image/webp".to_owned()),
                            cdn_number: Some(3),
                            key: Some(vec![1u8; 64]),
                            ..Default::default()
                        }),
                        description: None,
                        date: None,
                    }),
                    background: None,
                },
            )),
        };

        let content = Content::from_body(
            ContentBody::StoryMessage(story(Some(
                presage::proto::text_attachment::Style::Bold as i32,
            ))),
            metadata(Some(true)),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["isStory"], json!(true), "story 位在场");
        let face = &envelope["storyMessage"];
        assert_eq!(face["textAttachment"]["text"], json!("hello story"));
        assert_eq!(face["textAttachment"]["textStyle"], json!("BOLD"));
        let preview = &face["textAttachment"]["preview"];
        assert_eq!(preview["image"]["contentType"], json!("image/webp"));
        assert!(
            preview["image"]["locator"]["key"].is_string(),
            "preview.image 走附件指针投影（locator.key base64）"
        );
        assert!(
            preview.get("url").is_none() && preview.get("title").is_none(),
            "url/title proto 缺席不投键（缺席键不在）"
        );
        assert_eq!(face["allowsReplies"], json!(true));
        assert_eq!(
            face["profileKey"],
            json!(base64::engine::general_purpose::STANDARD.encode([9u8; 32]))
        );
        assert!(face.get("group").is_none(), "群母钥缺席键不在");
        assert!(
            envelope.get("dataMessage").is_none(),
            "story 不落普通会话流"
        );

        // textStyle DEFAULT=0 线上在场照投词（与发送面缺省省略的不对称是
        // 线上形状的如实两面）。
        let content = Content::from_body(
            ContentBody::StoryMessage(story(Some(
                presage::proto::text_attachment::Style::Default as i32,
            ))),
            metadata(Some(true)),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["storyMessage"]["textAttachment"]["textStyle"],
            json!("DEFAULT")
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Stories 接收批（E2）preview.url/preview.title 扩面（2026-10-09 批，
    /// 发送面 previewUrl/previewTitle 对端回声）：Some 且非空投字符串键（与
    /// image 同一 preview 对象并列；无 image 的 preview 面照投）；空串省键
    /// （缺席键不在），preview 面全空 = preview 键不在。
    #[tokio::test]
    async fn receive_story_message_projects_preview_url_and_title() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{Preview, TextAttachment};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-preview-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = || Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: Some(true),
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let story = |url: Option<&str>, title: Option<&str>| presage::proto::StoryMessage {
            profile_key: None,
            group: None,
            allows_replies: Some(true),
            body_ranges: Vec::new(),
            attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                TextAttachment {
                    text: Some("preview face".to_owned()),
                    text_style: None,
                    text_foreground_color: None,
                    text_background_color: None,
                    preview: Some(Preview {
                        url: url.map(str::to_owned),
                        title: title.map(str::to_owned),
                        image: None,
                        description: None,
                        date: None,
                    }),
                    background: None,
                },
            )),
        };

        // 双在场：url/title 投字符串键，同一 preview 对象；image 缺席键不在
        //（url-only story 的对端回声形状）。
        let content = Content::from_body(
            ContentBody::StoryMessage(story(Some("https://example.com/a"), Some("Example"))),
            metadata(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story projects",
        )["params"]["envelope"]
            .clone();
        let preview = &envelope["storyMessage"]["textAttachment"]["preview"];
        assert_eq!(preview["url"], json!("https://example.com/a"));
        assert_eq!(preview["title"], json!("Example"));
        assert!(
            preview.get("image").is_none(),
            "image 缺席键不在（preview 对象不发明 image）"
        );

        // 空串：省键；preview 面全空 = preview 键不在（缺席键不在纪律）。
        let content = Content::from_body(
            ContentBody::StoryMessage(story(Some(""), Some(""))),
            metadata(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story projects",
        )["params"]["envelope"]
            .clone();
        assert!(
            envelope["storyMessage"]["textAttachment"]
                .get("preview")
                .is_none(),
            "preview 面全空不投键"
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Stories 接收批（E2）官方闸三腿：①story 位 + 普通 dataMessage = 整
    /// 信封丢弃（空向量）；②story 位 + remote delete = 豁免照投（isStory
    /// 在场）；③story 位 + 群 story 回复（storyContext ∧ groupV2）= 豁免
    /// 照投（dataMessage.storyReply + groupInfo + isStory）。
    #[tokio::test]
    async fn receive_story_gate_drop_and_exemptions() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{data_message, GroupContextV2};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-gate-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = |story| Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let story_context = data_message::StoryContext {
            author_aci: None,
            sent_timestamp: Some(42),
            author_aci_binary: Some(
                presage::libsignal_service::prelude::Uuid::parse_str(
                    "11111111-2222-4333-8444-555555555555",
                )
                .expect("author uuid")
                .as_bytes()
                .to_vec(),
            ),
        };

        // ①普通正文 + story 位：官方同款整信封丢弃。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("plain".to_owned()),
                ..Default::default()
            }),
            metadata(Some(true)),
        );
        assert!(
            project_content("acct", &store, &content, &registry)
                .await
                .is_empty(),
            "非 story/群回复/remote delete 行丢弃"
        );

        // ②remote delete 豁免：照投 remoteDelete 面 + isStory。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                delete: Some(presage::proto::data_message::Delete {
                    target_sent_timestamp: Some(7),
                }),
                ..Default::default()
            }),
            metadata(Some(true)),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "delete projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["isStory"], json!(true));
        assert_eq!(
            envelope["dataMessage"]["remoteDelete"]["timestamp"],
            json!(7)
        );

        // ③群 story 回复豁免：storyReply + groupInfo + isStory。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                story_context: Some(story_context.clone()),
                group_v2: Some(GroupContextV2 {
                    master_key: Some(vec![0x33u8; 32]),
                    revision: None,
                    group_change: None,
                }),
                ..Default::default()
            }),
            metadata(Some(true)),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "group story reply projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["isStory"], json!(true));
        assert_eq!(
            envelope["dataMessage"]["storyReply"]["sentTimestamp"],
            json!(42)
        );
        assert_eq!(
            envelope["dataMessage"]["storyReply"]["authorAci"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert!(
            envelope["dataMessage"]["groupInfo"]["groupId"].is_string(),
            "群 story 回复另带既有 groupInfo 投影"
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Stories 接收批（E2）边界腿：①1:1 story 回复（信封无 story 位）→
    /// dataMessage.storyReply 照投、isStory 键不在；author_aci 字符串形态
    /// 兜底；author 不可解析整键缺席（宁缺勿错）；②反向违例（story 体但
    /// 信封无位）官方 warn-only 放行——引擎照投 storyMessage、isStory 键
    /// 不在（如实反映信封位，不发明）；空 profileKey 不投键。
    #[tokio::test]
    async fn receive_story_reply_and_malformed_story_body() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::data_message;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-edge-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = |story| Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };

        // ①1:1 story 回复：string 兜底形态，isStory 键不在。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                story_context: Some(data_message::StoryContext {
                    author_aci: Some("22222222-3333-4444-8555-666666666666".to_owned()),
                    sent_timestamp: Some(43),
                    author_aci_binary: None,
                }),
                ..Default::default()
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story reply projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope.get("isStory").is_none(), "信封无位不发明标记");
        assert_eq!(
            envelope["dataMessage"]["storyReply"],
            json!({
                "authorAci": "22222222-3333-4444-8555-666666666666",
                "sentTimestamp": 43
            })
        );

        // author 不可解析（两形态皆缺）：storyReply 整键缺席。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                story_context: Some(data_message::StoryContext {
                    author_aci: None,
                    sent_timestamp: Some(44),
                    author_aci_binary: None,
                }),
                ..Default::default()
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story reply projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope["dataMessage"].get("storyReply").is_none());

        // ②反向违例（官方 warn-only 放行面）：story 体但信封无位——
        // storyMessage 照投，isStory 键不在；空 profileKey 不投键。
        let content = Content::from_body(
            ContentBody::StoryMessage(presage::proto::StoryMessage {
                profile_key: Some(Vec::new()),
                group: None,
                allows_replies: Some(false),
                body_ranges: Vec::new(),
                attachment: Some(presage::proto::story_message::Attachment::FileAttachment(
                    presage::proto::AttachmentPointer {
                        content_type: Some("image/jpeg".to_owned()),
                        ..Default::default()
                    },
                )),
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story body projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope.get("isStory").is_none());
        let face = &envelope["storyMessage"];
        assert_eq!(
            face["fileAttachment"]["contentType"],
            json!("image/jpeg"),
            "文件 story 走附件指针投影"
        );
        assert_eq!(face["allowsReplies"], json!(false), "bool 缺省如实投影");
        assert!(face.get("profileKey").is_none(), "空 key 不投");

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 群终止投影放开（最小面）：dataMessage.groupV2 携带 terminated change
    /// → 整信封替换为 `groupTerminated` 事件（{groupId, terminator,
    /// timestamp}）并落登记表；非 terminated change（改标题等）→ 群更新消息
    /// 照旧走普通投影（本批继续跳过面的现状不变：普通群消息带 groupInfo）；
    /// sent sync 内终止 → 同事件（其他设备发起面）。
    #[tokio::test]
    async fn receive_projects_group_terminated_change_only() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::libsignal_service::zkgroup;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-term-proj-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1).expect("device"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };

        let master_key = [0x33u8; 32];
        use base64::Engine as _;
        let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
        let secret_params =
            presage::libsignal_service::zkgroup::groups::GroupSecretParams::derive_from_master_key(
                presage::libsignal_service::zkgroup::groups::GroupMasterKey::new(master_key),
            );
        let editor = presage::libsignal_service::protocol::Aci::parse_from_service_id_string(
            "22222222-3333-4444-8555-666666666666",
        )
        .expect("editor aci");
        let terminate_change = presage::proto::GroupChange {
            actions: presage::proto::group_change::Actions {
                source_user_id: zkgroup::serialize(
                    &secret_params.encrypt_service_id(editor.into()),
                ),
                group_id: secret_params.get_group_identifier().to_vec(),
                version: 9,
                terminate_group: Some(
                    presage::proto::group_change::actions::TerminateGroupAction {},
                ),
                ..presage::proto::group_change::Actions::default()
            }
            .encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: groups::SUPPORTED_CHANGE_EPOCH,
        };

        // terminated change → 整信封替换为 groupTerminated 事件 + 登记。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(9),
                    group_change: Some(terminate_change.encode_to_vec()),
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "terminate event projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["groupTerminated"]["groupId"], json!(group_id));
        assert_eq!(
            envelope["groupTerminated"]["terminator"],
            json!("22222222-3333-4444-8555-666666666666")
        );
        assert_eq!(
            envelope["groupTerminated"]["timestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert!(envelope.get("dataMessage").is_none(), "整信封替换");
        assert!(registry.contains(&group_id), "send 面拦截登记");

        // 非 terminated change（改标题）→ 群更新消息照旧走普通投影（groupInfo
        // 面；终止投影不误伤）。
        let mut randomness = [0u8; 32];
        use rand::RngCore as _;
        rand::rng().fill_bytes(&mut randomness);
        let title_blob = presage::proto::GroupAttributeBlob {
            content: Some(presage::proto::group_attribute_blob::Content::Title(
                "renamed".to_owned(),
            )),
        }
        .encode_to_vec();
        let rename_change = presage::proto::GroupChange {
            actions: presage::proto::group_change::Actions {
                source_user_id: zkgroup::serialize(
                    &secret_params.encrypt_service_id(editor.into()),
                ),
                group_id: secret_params.get_group_identifier().to_vec(),
                version: 10,
                modify_title: Some(presage::proto::group_change::actions::ModifyTitleAction {
                    title: secret_params.encrypt_blob_with_padding(randomness, &title_blob, 0),
                }),
                ..presage::proto::group_change::Actions::default()
            }
            .encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: groups::SUPPORTED_CHANGE_EPOCH,
        };
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(10),
                    group_change: Some(rename_change.encode_to_vec()),
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "group dataMessage projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert!(envelope.get("groupTerminated").is_none());
        // D3 起：非 terminate 的群 change 批 → groupChanged 事件（整信封
        // 替换同 terminate/expiration 面；groupInfo 面只留无 change 群消息）。
        assert_eq!(envelope["groupChanged"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupChanged"]["revision"], json!(10));
        assert_eq!(
            envelope["groupChanged"]["changes"],
            json!([{ "type": "title", "newTitle": "renamed" }])
        );

        // sent sync 内终止（其他设备发起）→ 同事件面。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                content: Some(presage::proto::sync_message::Content::Sent(
                    presage::proto::sync_message::Sent {
                        timestamp: Some(1_700_000_000_002),
                        message: Some(DataMessage {
                            group_v2: Some(presage::proto::GroupContextV2 {
                                master_key: Some(master_key.to_vec()),
                                revision: Some(11),
                                group_change: Some(terminate_change.encode_to_vec()),
                            }),
                            ..Default::default()
                        }),
                        ..presage::proto::sync_message::Sent::default()
                    },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rand::rng())
            }),
            metadata,
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "sent terminate projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["groupTerminated"]["groupId"], json!(group_id));

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 群 expiration 投影放开（updateGroup 批最小面）：dataMessage.groupV2
    /// 携带 expiration_timer change → 整信封替换为 `groupUpdated` 事件
    ///（{groupId, revision, expireTimerSeconds, editor}，0 = 关闭）且不登记
    /// 终止表；非 expiration change（改标题等）→ 群更新消息照旧走普通投影
    ///（本批继续跳过面的现状不变）；terminated 优先（同信封两 change 不
    /// 可能——proto3 Actions 单消息类型各一，两投影互斥由 change 内容定）。
    #[tokio::test]
    async fn receive_projects_group_expiration_change_only() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::libsignal_service::zkgroup;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-exp-proj-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1).expect("device"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };

        let master_key = [0x55u8; 32];
        use base64::Engine as _;
        let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
        let secret_params =
            presage::libsignal_service::zkgroup::groups::GroupSecretParams::derive_from_master_key(
                presage::libsignal_service::zkgroup::groups::GroupMasterKey::new(master_key),
            );
        let editor = presage::libsignal_service::protocol::Aci::parse_from_service_id_string(
            "33333333-4444-4555-9666-777777777777",
        )
        .expect("editor aci");
        let expiration_change = |version: u32, duration: u32| {
            let mut randomness = [0u8; 32];
            use rand::RngCore as _;
            rand::rng().fill_bytes(&mut randomness);
            let blob = presage::proto::GroupAttributeBlob {
                content: Some(
                    presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(
                        duration,
                    ),
                ),
            }
            .encode_to_vec();
            presage::proto::GroupChange {
                actions: presage::proto::group_change::Actions {
                    source_user_id: zkgroup::serialize(
                        &secret_params.encrypt_service_id(editor.into()),
                    ),
                    group_id: secret_params.get_group_identifier().to_vec(),
                    version,
                    modify_disappearing_message_timer: Some(
                        presage::proto::group_change::actions::ModifyDisappearingMessageTimerAction {
                            timer: secret_params.encrypt_blob_with_padding(randomness, &blob, 0),
                        },
                    ),
                    ..presage::proto::group_change::Actions::default()
                }
                .encode_to_vec(),
                server_signature: Vec::new(),
                change_epoch: groups::SUPPORTED_CHANGE_EPOCH,
            }
        };

        // expiration change → 整信封替换为 groupUpdated 事件（revision =
        // change version、明文秒数、编辑者 ACI），终止表不动。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(5),
                    group_change: Some(expiration_change(5, 86_400).encode_to_vec()),
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "expiration event projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["groupUpdated"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupUpdated"]["revision"], json!(5));
        assert_eq!(
            envelope["groupUpdated"]["expireTimerSeconds"],
            json!(86_400)
        );
        assert_eq!(
            envelope["groupUpdated"]["editor"],
            json!("33333333-4444-4555-9666-777777777777")
        );
        assert!(envelope.get("dataMessage").is_none(), "整信封替换");
        assert!(!registry.contains(&group_id), "expiration 不登记终止表");

        // timer = 0（关闭）→ expireTimerSeconds 投影 0（关闭语义不吞）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(6),
                    group_change: Some(expiration_change(6, 0).encode_to_vec()),
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "timer-off event projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["groupUpdated"]["revision"], json!(6));
        assert_eq!(envelope["groupUpdated"]["expireTimerSeconds"], json!(0));

        // 非 expiration change（改标题）→ 群更新消息照旧走普通投影（groupInfo
        // 面；expiration 投影不误伤）。
        let mut randomness = [0u8; 32];
        use rand::RngCore as _;
        rand::rng().fill_bytes(&mut randomness);
        let title_blob = presage::proto::GroupAttributeBlob {
            content: Some(presage::proto::group_attribute_blob::Content::Title(
                "renamed".to_owned(),
            )),
        }
        .encode_to_vec();
        let rename_change = presage::proto::GroupChange {
            actions: presage::proto::group_change::Actions {
                source_user_id: zkgroup::serialize(
                    &secret_params.encrypt_service_id(editor.into()),
                ),
                group_id: secret_params.get_group_identifier().to_vec(),
                version: 10,
                modify_title: Some(presage::proto::group_change::actions::ModifyTitleAction {
                    title: secret_params.encrypt_blob_with_padding(randomness, &title_blob, 0),
                }),
                ..presage::proto::group_change::Actions::default()
            }
            .encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: groups::SUPPORTED_CHANGE_EPOCH,
        };
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                group_v2: Some(presage::proto::GroupContextV2 {
                    master_key: Some(master_key.to_vec()),
                    revision: Some(10),
                    group_change: Some(rename_change.encode_to_vec()),
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "group dataMessage projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert!(envelope.get("groupUpdated").is_none());
        // D3 起：非 timer 的群 change 批 → groupChanged 事件（整信封替换；
        // groupInfo 面只留无 change 群消息）。
        assert_eq!(envelope["groupChanged"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupChanged"]["revision"], json!(10));
        assert_eq!(
            envelope["groupChanged"]["changes"],
            json!([{ "type": "title", "newTitle": "renamed" }])
        );

        // sent sync 内 expiration（本机其他设备设置）→ 同事件面。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                content: Some(presage::proto::sync_message::Content::Sent(
                    presage::proto::sync_message::Sent {
                        timestamp: Some(1_700_000_000_002),
                        message: Some(DataMessage {
                            group_v2: Some(presage::proto::GroupContextV2 {
                                master_key: Some(master_key.to_vec()),
                                revision: Some(11),
                                group_change: Some(expiration_change(11, 3600).encode_to_vec()),
                            }),
                            ..Default::default()
                        }),
                        ..presage::proto::sync_message::Sent::default()
                    },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rand::rng())
            }),
            metadata,
        );
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "sent expiration projects",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(envelope["groupUpdated"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupUpdated"]["revision"], json!(11));
        assert_eq!(envelope["groupUpdated"]["expireTimerSeconds"], json!(3600));

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    fn uuid_bytes(text: &str) -> Vec<u8> {
        presage::libsignal_service::prelude::Uuid::parse_str(text)
            .expect("uuid parses")
            .into_bytes()
            .to_vec()
    }

    /// 通话消息面（KT 扩展批）：CallMessage 收投影——opaque base64+urgency
    /// （缺席=droppable）+ 旧线面 offer/answer/ice/hangup/busy/destinationDeviceId
    /// 逐面投影；空 CallMessage 丢弃（宁缺勿错）。
    #[test]
    fn call_message_projection_faces() {
        use base64::Engine as _;
        use presage::proto::call_message as call;

        // opaque 形态（现行线面）：data base64 + urgency 显式 handleImmediately
        // + destinationDeviceId。
        let out = project_call_message(&presage::proto::CallMessage {
            opaque: Some(call::Opaque {
                data: Some(b"signal-rtc".to_vec()),
                urgency: Some(call::opaque::Urgency::HandleImmediately as i32),
            }),
            destination_device_id: Some(2),
            ..Default::default()
        })
        .expect("opaque call projects");
        assert_eq!(
            out["opaque"],
            json!(base64::engine::general_purpose::STANDARD.encode(b"signal-rtc"))
        );
        assert_eq!(out["urgency"], json!("handleImmediately"));
        assert_eq!(out["destinationDeviceId"], json!(2));
        assert!(out.get("offer").is_none());

        // urgency 缺席 = droppable（proto 缺省语义显式化）；destinationDeviceId
        // 缺席键不在。
        let out = project_call_message(&presage::proto::CallMessage {
            opaque: Some(call::Opaque {
                data: Some(vec![1]),
                urgency: None,
            }),
            ..Default::default()
        })
        .expect("default urgency projects");
        assert_eq!(out["urgency"], json!("droppable"));
        assert!(out.get("destinationDeviceId").is_none());
        // 枚举外值折叠缺省（不炸面）。
        let out = project_call_message(&presage::proto::CallMessage {
            opaque: Some(call::Opaque {
                data: Some(vec![1]),
                urgency: Some(42),
            }),
            ..Default::default()
        })
        .expect("unknown urgency folds to droppable");
        assert_eq!(out["urgency"], json!("droppable"));

        // 旧线面：offer(audio)/answer/ice/hangup(declined)/busy 同一信封。
        let out = project_call_message(&presage::proto::CallMessage {
            offer: Some(call::Offer {
                id: Some(11),
                r#type: Some(call::offer::Type::OfferAudioCall as i32),
                opaque: Some(vec![0xaa]),
            }),
            answer: Some(call::Answer {
                id: Some(12),
                opaque: Some(vec![0xbb]),
            }),
            ice_update: vec![call::IceUpdate {
                id: Some(13),
                opaque: Some(vec![0xcc]),
            }],
            busy: Some(call::Busy { id: Some(14) }),
            hangup: Some(call::Hangup {
                id: Some(15),
                r#type: Some(call::hangup::Type::HangupDeclined as i32),
                device_id: Some(3),
            }),
            ..Default::default()
        })
        .expect("legacy call projects");
        assert_eq!(out["offer"]["id"], json!(11));
        assert_eq!(out["offer"]["type"], json!("audio"));
        assert_eq!(
            out["offer"]["opaque"],
            json!(base64::engine::general_purpose::STANDARD.encode([0xaa]))
        );
        assert_eq!(out["answer"]["id"], json!(12));
        assert_eq!(out["iceUpdate"][0]["id"], json!(13));
        assert_eq!(out["busy"]["id"], json!(14));
        assert_eq!(out["hangup"]["id"], json!(15));
        assert_eq!(out["hangup"]["type"], json!("declined"));
        assert_eq!(out["hangup"]["deviceId"], json!(3));

        // offer video 枚举与 hangup normal 投影。
        let out = project_call_message(&presage::proto::CallMessage {
            offer: Some(call::Offer {
                id: Some(1),
                r#type: Some(call::offer::Type::OfferVideoCall as i32),
                opaque: None,
            }),
            ..Default::default()
        })
        .expect("video offer projects");
        assert_eq!(out["offer"]["type"], json!("video"));
        assert!(out["offer"].get("opaque").is_none());
        let out = project_call_message(&presage::proto::CallMessage {
            hangup: Some(call::Hangup {
                id: Some(1),
                r#type: Some(call::hangup::Type::HangupNormal as i32),
                device_id: None,
            }),
            ..Default::default()
        })
        .expect("hangup projects");
        assert_eq!(out["hangup"]["type"], json!("normal"));

        // 空 CallMessage：无可投影字段整体丢弃。
        assert!(project_call_message(&presage::proto::CallMessage::default()).is_none());
    }

    /// CallEvent sync 投影：枚举串（audio/incoming/accepted 等）+ callId
    /// 字符串形态（官方 longToString 同构）+ conversationId 三口径（16B
    /// UUID / 群 base64 / ad-hoc hex）；callId 缺席 = warn-only 丢弃；
    /// conversationId 空字节键省略。
    #[test]
    fn call_event_sync_projection_faces() {
        use base64::Engine as _;
        use presage::proto::sync_message::{call_event, CallEvent};
        let aci_bytes = uuid_bytes("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae");

        // 直聊全字段：16B conversationId → UUID 串。
        let out = project_call_event(&CallEvent {
            conversation_id: Some(aci_bytes.clone()),
            call_id: Some(1_700_000_000_123),
            timestamp: Some(1_700_000_000_000),
            r#type: Some(call_event::Type::AudioCall as i32),
            direction: Some(call_event::Direction::Incoming as i32),
            event: Some(call_event::Event::Accepted as i32),
        })
        .expect("call event projects");
        assert_eq!(
            out["conversationId"],
            json!("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
        );
        assert_eq!(out["callId"], json!("1700000000123"));
        assert_eq!(out["timestamp"], json!(1_700_000_000_000_u64));
        assert_eq!(out["type"], json!("audio"));
        assert_eq!(out["direction"], json!("incoming"));
        assert_eq!(out["event"], json!("accepted"));

        // 群（非 16B 字节）→ 标准 base64；枚举外值按 proto3 未知值语义
        // 归缺省 UNKNOWN → 键省略（不发明串）。
        let out = project_call_event(&CallEvent {
            conversation_id: Some(vec![0xde, 0xad, 0xbe, 0xef]),
            call_id: Some(7),
            timestamp: None,
            r#type: Some(call_event::Type::GroupCall as i32),
            direction: Some(42),
            event: Some(43),
        })
        .expect("group call event projects");
        assert_eq!(
            out["conversationId"],
            json!(base64::engine::general_purpose::STANDARD.encode([0xde, 0xad, 0xbe, 0xef]))
        );
        assert_eq!(out["type"], json!("group"));
        assert!(out.get("direction").is_none(), "枚举外值键省略");
        assert!(out.get("event").is_none(), "枚举外值键省略");
        assert_eq!(out["timestamp"], json!(0));

        // ad-hoc：conversationId hex（官方 roomIdInBytesSchema 同构）。
        let out = project_call_event(&CallEvent {
            conversation_id: Some(vec![0x01, 0x02, 0x03]),
            call_id: Some(8),
            timestamp: Some(9),
            r#type: Some(call_event::Type::AdHocCall as i32),
            direction: Some(call_event::Direction::Outgoing as i32),
            event: Some(call_event::Event::Observed as i32),
        })
        .expect("ad-hoc call event projects");
        assert_eq!(out["conversationId"], json!("010203"));
        assert_eq!(out["type"], json!("ad-hoc"));
        assert_eq!(out["direction"], json!("outgoing"));
        assert_eq!(out["event"], json!("observed"));

        // 枚举缺席：键省略（不发明 unknown 串）。
        let out = project_call_event(&CallEvent {
            conversation_id: Some(aci_bytes.clone()),
            call_id: Some(1),
            timestamp: Some(2),
            ..Default::default()
        })
        .expect("bare call event projects");
        assert!(out.get("type").is_none());
        assert!(out.get("direction").is_none());
        assert!(out.get("event").is_none());

        // conversationId 空字节：键省略（不发明空串/空 base64）。
        let out = project_call_event(&CallEvent {
            conversation_id: Some(Vec::new()),
            call_id: Some(1),
            ..Default::default()
        })
        .expect("empty conversation id omits key");
        assert!(out.get("conversationId").is_none());

        // callId 缺席：无记录主键，warn-only 整事件丢弃（零 stderr）。
        assert!(project_call_event(&CallEvent {
            conversation_id: Some(aci_bytes),
            call_id: None,
            ..Default::default()
        })
        .is_none());
    }

    /// send 构造面：opaque data 非空 + urgency/destinationDeviceId 透传；
    /// legacy 裸帧族按官方 proto 编码（busy 位 / hangup{id,type,deviceId}，
    /// 无 opaque 字段）；解码面 fail-closed（非 base64 / 空负载）。
    #[test]
    fn call_message_send_payload_and_decode_faces() {
        use base64::Engine as _;
        use presage::proto::call_message::opaque::Urgency;

        let data = decode_call_data(&base64::engine::general_purpose::STANDARD.encode(b"payload"))
            .expect("valid base64 decodes");
        assert_eq!(data, b"payload".to_vec());
        assert!(decode_call_data("not@base64!").is_none(), "非 base64 拒绝");
        assert!(decode_call_data("").is_none(), "空负载拒绝");
        assert!(
            decode_call_data(&base64::engine::general_purpose::STANDARD.encode([])).is_none(),
            "空串 base64 同为空负载拒绝"
        );

        let opaque = CallMessageFace::Opaque {
            data: b"payload".to_vec(),
            urgency: Urgency::HandleImmediately,
        };
        let payload = SendCallMessage {
            face: opaque,
            destination_device_id: Some(4),
        };
        match &payload.face {
            CallMessageFace::Opaque { data, urgency } => {
                assert_eq!(data, &b"payload".to_vec());
                assert_eq!(*urgency, Urgency::HandleImmediately);
            }
            _ => panic!("opaque face expected"),
        }
        assert_eq!(payload.destination_device_id, Some(4));
    }

    /// legacy 裸帧族字节面（1.53 批）：busy/hangup 构造的 CallMessage 经
    /// prost 标准编码，逐字节对照官方 proto 手编形状——busy = 字段5长度分隔
    /// 内 id varint；hangup = 字段7长度分隔内 id/type/deviceId；两面都不含
    /// opaque（字段10 缺席 = 无 opaque 材料，官方裸帧族 wire 语义）。
    #[test]
    fn legacy_call_faces_encode_official_proto_bytes() {
        fn encode(body: presage::proto::CallMessage) -> Vec<u8> {
            use prost::Message as _;
            let mut buf = Vec::new();
            body.encode(&mut buf).expect("proto encodes");
            buf
        }
        fn varint(value: u64) -> Vec<u8> {
            let mut out = Vec::new();
            let mut v = value;
            loop {
                let byte = (v & 0x7f) as u8;
                v >>= 7;
                if v == 0 {
                    out.push(byte);
                    return out;
                }
                out.push(byte | 0x80);
            }
        }

        // busy{id: 0x0102030405060708}：字段5(tag 0x2a) + len 8 + id varint。
        let busy_id = 0x0102_0304_0506_0708u64;
        let busy = encode(presage::proto::CallMessage {
            busy: Some(presage::proto::call_message::Busy { id: Some(busy_id) }),
            ..Default::default()
        });
        let busy_expected = {
            let mut inner = vec![0x08];
            inner.extend(varint(busy_id));
            let mut out = vec![0x2a, inner.len() as u8];
            out.extend(inner);
            out
        };
        assert_eq!(busy, busy_expected, "busy 裸帧 = 字段5{{id}}");

        // hangup{id: 7, type: ACCEPTED(1), deviceId: 2}：字段7(tag 0x3a) +
        // len + id(0x08) + type(0x10) + deviceId(0x18)。
        let hangup = encode(presage::proto::CallMessage {
            hangup: Some(presage::proto::call_message::Hangup {
                id: Some(7),
                r#type: Some(presage::proto::call_message::hangup::Type::HangupAccepted as i32),
                device_id: Some(2),
            }),
            ..Default::default()
        });
        let hangup_expected = [0x3a, 0x06, 0x08, 0x07, 0x10, 0x01, 0x18, 0x02];
        assert_eq!(
            hangup, hangup_expected,
            "hangup 裸帧 = 字段7{{id,type,deviceId}}"
        );

        // hangup 缺省面：type/type/deviceId 都缺席时不写内字段（proto3 缺省
        // 不编码语义）——tag + len 0。
        let bare_hangup = encode(presage::proto::CallMessage {
            hangup: Some(presage::proto::call_message::Hangup {
                id: None,
                r#type: None,
                device_id: None,
            }),
            ..Default::default()
        });
        assert_eq!(bare_hangup, vec![0x3a, 0x00], "空 hangup 帧无内字段");

        // 两面 wire 互斥：legacy 裸帧族不含 opaque 字段（字段10 tag 0x52
        // 不在列），modern 面不含 busy/hangup 位。
        let modern = encode(presage::proto::CallMessage {
            opaque: Some(presage::proto::call_message::Opaque {
                data: Some(b"x".to_vec()),
                urgency: None,
            }),
            ..Default::default()
        });
        assert!(!busy.contains(&0x52u8), "busy 无 opaque 字段");
        assert!(!hangup.contains(&0x52u8), "hangup 无 opaque 字段");
        assert!(
            !modern.contains(&0x2au8) && !modern.contains(&0x3au8),
            "modern 无 busy/hangup 位"
        );
    }

    /// 群臂构造面（P4b 群信令出网批）：群向信封 = `CallMessage{opaque{data,
    /// urgency}}` 单点（官方桌面 #handleSendCallMessageToGroup
    /// calling.preload.ts:3338-3407 构 CallingMessage{opaque} 群发，恒
    /// opaque 无裸帧、无设备定向位）；prost wire 逐字节钉 opaque 字段10
    ///（tag 0x52 长度分隔）内 data 字段1 + urgency 字段2，无 busy/hangup
    /// 位、无 destinationDeviceId（字段9 缺席）。
    #[test]
    fn group_call_arm_builds_opaque_envelope_only() {
        use base64::Engine as _;
        use presage::proto::call_message::opaque::Urgency;

        let payload = SendCallMessage {
            face: CallMessageFace::Opaque {
                data: decode_call_data(
                    &base64::engine::general_purpose::STANDARD.encode(b"signal-rtc"),
                )
                .expect("valid opaque data"),
                urgency: Urgency::HandleImmediately,
            },
            destination_device_id: None,
        };
        let message = build_call_message(&payload);
        assert!(message.opaque.is_some(), "群臂信封含 opaque 材料");
        assert!(message.busy.is_none(), "群臂无 busy 位");
        assert!(message.hangup.is_none(), "群臂无 hangup 位");
        assert_eq!(message.destination_device_id, None, "群臂无设备定向位");

        // wire 逐字节：字段10(tag 0x52) + len 14 内 data(0x0a,len 10,
        // "signal-rtc") + urgency(0x10, varint 1)；无 busy(0x2a)/hangup
        // (0x3a)/destinationDeviceId(字段9 tag 0x48) 位。
        use prost::Message as _;
        let mut bytes = Vec::new();
        message.encode(&mut bytes).expect("proto encodes");
        assert_eq!(
            bytes,
            [
                0x52, 0x0e, 0x0a, 0x0a, b's', b'i', b'g', b'n', b'a', b'l', b'-', b'r', b't', b'c',
                0x10, 0x01
            ],
            "群臂 wire = 字段10{{data,urgency}}"
        );
        assert!(
            !bytes.contains(&0x2au8) && !bytes.contains(&0x3au8) && !bytes.contains(&0x48u8),
            "群臂 wire 无裸帧/设备定向位"
        );

        // 字段投影面：opaque 材料 + urgency=HANDLE_IMMEDIATELY(1) 原样写入
        //（桌面 callingMessageToProto 值透传同构）。
        let opaque = message.opaque.expect("opaque present");
        assert_eq!(opaque.data.as_deref(), Some(b"signal-rtc".as_slice()));
        assert_eq!(opaque.urgency, Some(Urgency::HandleImmediately as i32));
    }

    /// legacy 参数面（parse_legacy_call_face）：官方形状接受、投影词汇收齐、
    /// 形状错 fail-closed（未知 kind / id≤0 / id 缺席 / 非法 type / deviceId
    /// 超 u32）；legacy 键缺席 = modern 面不受影响。
    #[test]
    fn legacy_call_face_params_gate() {
        use crate::service::CallMessageFace;
        use presage::proto::call_message::hangup::Type as HangupType;

        let json = |v: Value| parse_legacy_call_face(Some(&v));

        let busy = json(json!({"kind": "busy", "id": 7}));
        assert!(matches!(busy, Some(CallMessageFace::LegacyBusy { id: 7 })));

        let hangup = json(json!({"kind": "hangup", "id": 7, "type": "declined", "deviceId": 2}));
        match hangup {
            Some(CallMessageFace::LegacyHangup {
                id,
                kind,
                device_id,
            }) => {
                assert_eq!(id, 7);
                assert_eq!(kind, HangupType::HangupDeclined);
                assert_eq!(device_id, Some(2));
            }
            _ => panic!("hangup face expected"),
        }

        // 缺省面：type 缺席 = normal，deviceId 缺席 = 不写。
        let default_hangup = json(json!({"kind": "hangup", "id": 9}));
        match default_hangup {
            Some(CallMessageFace::LegacyHangup {
                id,
                kind,
                device_id,
            }) => {
                assert_eq!(id, 9);
                assert_eq!(kind, HangupType::HangupNormal);
                assert_eq!(device_id, None);
            }
            _ => panic!("default hangup face expected"),
        }

        // fail-closed 面。
        assert!(
            json(json!({"kind": "offer", "id": 7})).is_none(),
            "非 busy/hangup kind 拒绝"
        );
        assert!(json(json!({"kind": "busy"})).is_none(), "id 缺席拒绝");
        assert!(
            json(json!({"kind": "busy", "id": 0})).is_none(),
            "id=0 拒绝"
        );
        assert!(
            json(json!({"kind": "hangup", "id": 7, "type": "urgent"})).is_none(),
            "非法 type 拒绝"
        );
        assert!(
            json(json!({"kind": "hangup", "id": 7, "deviceId": u32::MAX as u64 + 1})).is_none(),
            "deviceId 超 u32 拒绝"
        );
        assert!(
            json(Value::String("busy".into())).is_none(),
            "非对象形状拒绝"
        );

        // modern 面共存键：legacy 键缺席 = None（不报错、不误伤 modern 路径）。
        assert!(parse_legacy_call_face(None).is_none());
    }

    /// 收投影全信封面：CallMessage 体 → receive 通知信封 callMessage 键；
    /// CallEvent sync 体 → syncMessage.callEvent 键（投影函数经
    /// project_content 接线防回归）。
    #[tokio::test]
    async fn call_faces_wire_into_receive_envelope() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::call_message as call;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-call-envelope-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let metadata = envelope_metadata();

        // CallMessage：信封带 callMessage{opaque, urgency}。
        let content = Content::from_body(
            ContentBody::CallMessage(presage::proto::CallMessage {
                opaque: Some(call::Opaque {
                    data: Some(b"rtc".to_vec()),
                    urgency: None,
                }),
                ..Default::default()
            }),
            metadata.clone(),
        );
        let registry = groups::TerminatedGroups::new();
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "call message projects to receive",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(
            envelope["callMessage"]["opaque"],
            json!(base64::engine::general_purpose::STANDARD.encode(b"rtc"))
        );
        assert_eq!(envelope["callMessage"]["urgency"], json!("droppable"));
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );

        // CallEvent sync：信封带 syncMessage.callEvent。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                content: Some(presage::proto::sync_message::Content::CallEvent(
                    presage::proto::sync_message::CallEvent {
                        conversation_id: Some(uuid_bytes("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")),
                        call_id: Some(42),
                        timestamp: Some(43),
                        r#type: Some(
                            presage::proto::sync_message::call_event::Type::VideoCall as i32,
                        ),
                        direction: Some(
                            presage::proto::sync_message::call_event::Direction::Outgoing as i32,
                        ),
                        event: Some(
                            presage::proto::sync_message::call_event::Event::NotAccepted as i32,
                        ),
                    },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rng())
            }),
            metadata,
        );
        let registry = groups::TerminatedGroups::new();
        let notification = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "call event sync projects to receive",
        );
        let envelope = &notification["params"]["envelope"];
        assert_eq!(
            envelope["syncMessage"]["callEvent"]["conversationId"],
            json!("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
        );
        assert_eq!(envelope["syncMessage"]["callEvent"]["callId"], json!("42"));
        assert_eq!(envelope["syncMessage"]["callEvent"]["type"], json!("video"));
        assert_eq!(
            envelope["syncMessage"]["callEvent"]["direction"],
            json!("outgoing")
        );
        assert_eq!(
            envelope["syncMessage"]["callEvent"]["event"],
            json!("not-accepted")
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    fn envelope_metadata() -> presage::libsignal_service::content::Metadata {
        use presage::libsignal_service::content::Metadata;
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        }
    }

    /// profileKeyUpdate 批（compat §5）：合法 32 字节 key 按发送方 ACI 落
    /// presage store profile_keys 位 + contactProfileUpdated 事件；幂等闸
    /// （同 key 重复收 = 无事件无写）；换 key（48 字节错值、合法 32B 新值）
    /// 再落库再事件；非法长度 fail-closed 丢弃（普通投影照常、无事件、
    /// store 无写）；无 profileKey 消息零影响；PROFILE_KEY_UPDATE flag 行
    /// = 事件整信封替换（不进普通投影）；sent sync 不处理。
    #[tokio::test]
    async fn receive_profile_key_update_faces() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::libsignal_service::zkgroup::profiles::ProfileKey;

        let store_dir =
            std::env::temp_dir().join(format!("kt-engine-pku-{:016x}", mix_seed_step(mix_seed())));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();
        let sender_aci = "11111111-2222-4333-8444-555555555555";

        // 合法 32 字节：落库 + 事件（采集形态：普通投影并行不吞）。
        let key_a = ProfileKey::create([0x11u8; 32]);
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("hello".to_owned()),
                profile_key: Some(key_a.bytes.to_vec()),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 2, "harvest: event + normal投影");
        let event = &notifications[0]["params"]["envelope"];
        assert_eq!(event["contactProfileUpdated"]["aci"], json!(sender_aci));
        assert_eq!(event["sourceUuid"], json!(sender_aci));
        let normal = &notifications[1]["params"]["envelope"];
        assert_eq!(normal["dataMessage"]["message"], json!("hello"));
        assert!(normal.get("contactProfileUpdated").is_none());
        assert_eq!(
            store
                .profile_key(&ServiceId::parse_from_service_id_string(sender_aci).expect("aci"))
                .await
                .expect("store read")
                .expect("key persisted")
                .bytes,
            key_a.bytes
        );

        // 幂等：同 key 重复收 → 只有普通投影，无事件无重写。
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "idempotent: normal only");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());

        // 换 key：合法 32B 新值 → 再落库再事件。
        let key_b = ProfileKey::create([0x22u8; 32]);
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                profile_key: Some(key_b.bytes.to_vec()),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 2, "rotate: event + normal投影");
        assert_eq!(
            notifications[0]["params"]["envelope"]["contactProfileUpdated"]["aci"],
            json!(sender_aci)
        );
        assert_eq!(
            store
                .profile_key(&ServiceId::parse_from_service_id_string(sender_aci).expect("aci"))
                .await
                .expect("store read")
                .expect("key persisted")
                .bytes,
            key_b.bytes
        );

        // 非法长度（31B）fail-closed：无事件、store 维持旧值、普通投影照常。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("still normal".to_owned()),
                profile_key: Some(vec![0x33u8; 31]),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "invalid length: normal only");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());
        assert_eq!(
            notifications[0]["params"]["envelope"]["dataMessage"]["message"],
            json!("still normal")
        );
        assert_eq!(
            store
                .profile_key(&ServiceId::parse_from_service_id_string(sender_aci).expect("aci"))
                .await
                .expect("store read")
                .expect("key unchanged")
                .bytes,
            key_b.bytes
        );

        // 空 bytes：同 fail-closed 面（官方 hasProfileKey 分支同口径）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                profile_key: Some(Vec::new()),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "empty key: normal only");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());

        // PROFILE_KEY_UPDATE flag 行：事件整信封替换，不进普通投影。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("flagged".to_owned()),
                profile_key: Some(key_a.bytes.to_vec()),
                flags: Some(data_message::Flags::ProfileKeyUpdate as u32),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "flagged: single event");
        let envelope = &notifications[0]["params"]["envelope"];
        assert_eq!(envelope["contactProfileUpdated"]["aci"], json!(sender_aci));
        assert!(envelope.get("dataMessage").is_none());

        // 同 key 的 flag 行（换回旧值触发事件；再收一次 = 幂等：无事件且
        // 更新行本身无可投影正文 → 零通知，官方 hasChanged 门 no-op 同果）。
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert!(notifications.is_empty(), "flagged idempotent: no event");

        // sent sync（本机其他设备镜像）不处理：无事件、store 不动。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                content: Some(presage::proto::sync_message::Content::Sent(
                    presage::proto::sync_message::Sent {
                        message: Some(DataMessage {
                            profile_key: Some([0x44u8; 32].to_vec()),
                            ..Default::default()
                        }),
                        ..presage::proto::sync_message::Sent::default()
                    },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rng())
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "sent sync: normal only");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());
        assert_ne!(
            store
                .profile_key(&ServiceId::parse_from_service_id_string(sender_aci).expect("aci"))
                .await
                .expect("store read")
                .expect("key unchanged")
                .bytes,
            [0x44u8; 32]
        );

        // 无 profileKey 消息零影响：单投影、无事件（既有面防回归）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("plain".to_owned()),
                ..Default::default()
            }),
            envelope_metadata(),
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "plain: single投影");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// PNI 发送方不处理（profile key 是 1:1 ACI 作者面语义，compat §5 本批
    /// 边界④）：非法发送面不落库不事件，普通投影照常。
    #[tokio::test]
    async fn receive_profile_key_skips_pni_sender() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::libsignal_service::zkgroup::profiles::ProfileKey;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-pku-pni-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let mut metadata = envelope_metadata();
        metadata.sender =
            ServiceId::parse_from_service_id_string("PNI:11111111-2222-4333-8444-555555555555")
                .expect("pni");
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                profile_key: Some(ProfileKey::create([0x55u8; 32]).bytes.to_vec()),
                ..Default::default()
            }),
            metadata,
        );
        let notifications = project_content("acct", &store, &content, &registry).await;
        assert_eq!(notifications.len(), 1, "pni sender: normal only");
        assert!(notifications[0]["params"]["envelope"]
            .get("contactProfileUpdated")
            .is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 消息请求收包投影（messageRequestResponse 批，compat §3/§5）：sync
    /// oneof messageRequestResponse=14 投影 `syncMessage.messageRequestResponse
    /// {threadAci?, groupId?, messageRequestResponseType}`——多设备 echo 的
    /// ACCEPT 终态（官方 isConversationAccepted 第一因子权重
    /// messageRequestResponseType === ACCEPT，connector §4.58 推导的引擎侧
    /// 真数据面）；拒绝（DELETE）对称投影同形状不同枚举值；信封公共位只增
    /// 不改（旧宿主无该键不炸，键面纯增量）。
    #[tokio::test]
    async fn message_request_response_sync_projection_terminal_state() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir =
            std::env::temp_dir().join(format!("kt-engine-mrr-{:016x}", mix_seed_step(mix_seed())));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let mrr = |r#type: Option<i32>,
                   thread_aci: Option<String>,
                   thread_aci_binary: Option<Vec<u8>>,
                   group_id: Option<Vec<u8>>| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    content: Some(sync_message::Content::MessageRequestResponse(
                        sync_message::MessageRequestResponse {
                            r#type,
                            thread_aci,
                            thread_aci_binary,
                            group_id,
                        },
                    )),
                    ..SyncMessage::with_padding(&mut rng())
                }),
                envelope_metadata(),
            )
        };

        // ACCEPT（1）直聊线程：单通知，syncMessage.messageRequestResponse
        // 含 threadAci（binary 优先解析）与 messageRequestResponseType=1；
        // 信封公共位保持（键面纯增量）。
        let thread = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let notifications = project_content(
            "acct",
            &store,
            &mrr(
                Some(sync_message::message_request_response::Type::Accept as i32),
                None,
                Some(uuid_bytes(thread)),
                None,
            ),
            &registry,
        )
        .await;
        assert_eq!(notifications.len(), 1, "accept: single投影");
        let envelope = &notifications[0]["params"]["envelope"];
        assert_eq!(envelope["timestamp"], json!(1_700_000_000_000_u64));
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        let projection = &envelope["syncMessage"]["messageRequestResponse"];
        assert_eq!(projection["threadAci"], json!(thread));
        assert_eq!(projection["messageRequestResponseType"], json!(1));

        // 拒绝（DELETE=2）对称：同形状，枚举值 2（不做任何 ACCEPT 化变换）。
        let notifications = project_content(
            "acct",
            &store,
            &mrr(
                Some(sync_message::message_request_response::Type::Delete as i32),
                Some(thread.to_owned()),
                None,
                None,
            ),
            &registry,
        )
        .await;
        assert_eq!(notifications.len(), 1, "delete: single投影");
        let projection =
            &notifications[0]["params"]["envelope"]["syncMessage"]["messageRequestResponse"];
        assert_eq!(projection["threadAci"], json!(thread));
        assert_eq!(projection["messageRequestResponseType"], json!(2));

        // 群线程（32B master key）：groupId = 标准 base64（桌面
        // GROUPV2_ID_LENGTH=32 校验同构），type = BLOCK_AND_SPAM=6。
        let group_key = [0x77u8; 32];
        let notifications = project_content(
            "acct",
            &store,
            &mrr(
                Some(sync_message::message_request_response::Type::BlockAndSpam as i32),
                None,
                None,
                Some(group_key.to_vec()),
            ),
            &registry,
        )
        .await;
        assert_eq!(notifications.len(), 1, "group: single投影");
        let projection =
            &notifications[0]["params"]["envelope"]["syncMessage"]["messageRequestResponse"];
        assert_eq!(
            projection["groupId"],
            json!(base64::engine::general_purpose::STANDARD.encode(group_key))
        );
        assert_eq!(projection["messageRequestResponseType"], json!(6));
        assert!(
            projection.get("threadAci").is_none(),
            "群线程不投 threadAci"
        );

        // 无可路由线程（binary 坏 + string 缺 + 无 groupId）：整事件丢弃
        // （宁缺勿错，viewOnceOpen senderAci 不可解析同纪律）。
        let notifications = project_content(
            "acct",
            &store,
            &mrr(Some(1), None, Some(vec![0xde, 0xad]), None),
            &registry,
        )
        .await;
        assert!(notifications.is_empty(), "unroutable: dropped");

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// D2 syncMessage.read/viewed 收包投影：条目对 (senderUuid, timestamp)
    /// 逐条保留 + senderAci 双形解析（binary 优先 string 兜底）；坏条目
    /// （发送方不可解析/时间戳缺席）丢；空 viewed/read 键缺席；全空整信封
    /// 零投影（官方「read 专信封」兜底臂承接）。
    #[tokio::test]
    async fn sync_read_viewed_projection_entries() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-readsync-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let sync_of = |read: Vec<sync_message::Read>, viewed: Vec<sync_message::Viewed>| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    read,
                    viewed,
                    ..SyncMessage::with_padding(&mut rng())
                }),
                envelope_metadata(),
            )
        };

        // read：binary 16B 优先 + string 兜底，条目对逐条保留。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(
                vec![
                    sync_message::Read {
                        sender_aci: None,
                        timestamp: Some(111),
                        sender_aci_binary: Some(uuid_bytes(sender)),
                    },
                    sync_message::Read {
                        sender_aci: Some(sender.to_owned()),
                        timestamp: Some(222),
                        sender_aci_binary: None,
                    },
                ],
                Vec::new(),
            ),
            &registry,
        )
        .await;
        assert_eq!(notifications.len(), 1, "read: single 投影");
        let envelope = &notifications[0]["params"]["envelope"];
        let read = envelope["syncMessage"]["read"]
            .as_array()
            .expect("read array");
        assert_eq!(read.len(), 2, "条目对逐条保留");
        assert_eq!(read[0]["senderUuid"], json!(sender));
        assert_eq!(read[0]["timestamp"], json!(111));
        assert_eq!(read[1]["senderUuid"], json!(sender), "string 兜底同解析");
        assert_eq!(read[1]["timestamp"], json!(222));
        assert!(
            read[0].get("senderNumber").is_none(),
            "contacts 未命中无号码面"
        );
        assert!(
            envelope["syncMessage"].get("viewed").is_none(),
            "空 viewed 键缺席"
        );

        // viewed 同构 + 坏条目丢（发送方双缺 + 时间戳缺席）。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(
                Vec::new(),
                vec![
                    sync_message::Viewed {
                        sender_aci: None,
                        timestamp: Some(333),
                        sender_aci_binary: Some(uuid_bytes(sender)),
                    },
                    sync_message::Viewed {
                        sender_aci: None,
                        timestamp: None,
                        sender_aci_binary: None,
                    },
                ],
            ),
            &registry,
        )
        .await;
        assert_eq!(notifications.len(), 1, "viewed: single 投影");
        let face = &notifications[0]["params"]["envelope"]["syncMessage"];
        assert!(face.get("read").is_none(), "空 read 键缺席");
        let viewed = face["viewed"].as_array().expect("viewed array");
        assert_eq!(viewed.len(), 1, "坏条目丢");
        assert_eq!(viewed[0]["timestamp"], json!(333));

        // 全空：整信封零投影。
        let notifications =
            project_content("acct", &store, &sync_of(Vec::new(), Vec::new()), &registry).await;
        assert!(notifications.is_empty(), "空 read/viewed 不投影");

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 消息请求收包投影 helper 全口径：六枚举值原值往返、thread 双形、
    /// 群 32B 闸、type 缺席/枚举外强制 UNKNOWN=0（官方
    /// isKnownProtoEnumMember 同构，不丢弃）、丢弃面（线程双缺/群长度非法）。
    #[test]
    fn message_request_response_projection_helper_faces() {
        use presage::proto::sync_message::{message_request_response, MessageRequestResponse};
        let thread = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

        // 六枚举值全部原值往返（数字 = 官方因子词汇，无字符串映射发明）。
        for (value, wire) in [
            (message_request_response::Type::Unknown, 0),
            (message_request_response::Type::Accept, 1),
            (message_request_response::Type::Delete, 2),
            (message_request_response::Type::Block, 3),
            (message_request_response::Type::BlockAndDelete, 4),
            (message_request_response::Type::Spam, 5),
            (message_request_response::Type::BlockAndSpam, 6),
        ] {
            let out = project_message_request_response(&MessageRequestResponse {
                r#type: Some(value as i32),
                thread_aci: Some(thread.to_owned()),
                thread_aci_binary: None,
                group_id: None,
            })
            .expect("known type projects");
            assert_eq!(out["messageRequestResponseType"], json!(wire));
            assert_eq!(out["threadAci"], json!(thread));
            assert!(out.get("groupId").is_none(), "无群不投 groupId");
        }

        // thread 双形：string-only / binary-only 都投影（binary 优先）。
        let out = project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: Some(thread.to_owned()),
            thread_aci_binary: Some(uuid_bytes(thread)),
            group_id: None,
        })
        .expect("dual form parses");
        assert_eq!(out["threadAci"], json!(thread));
        let out = project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: None,
            thread_aci_binary: Some(uuid_bytes(thread)),
            group_id: None,
        })
        .expect("binary form parses");
        assert_eq!(out["threadAci"], json!(thread));

        // 线程双缺但有合法群：只投 groupId + type（官方两形态各自路由同构）。
        let group_key = [0x88u8; 32];
        let out = project_message_request_response(&MessageRequestResponse {
            r#type: None,
            thread_aci: None,
            thread_aci_binary: None,
            group_id: Some(group_key.to_vec()),
        })
        .expect("group-only projects");
        assert_eq!(
            out["groupId"],
            json!(base64::engine::general_purpose::STANDARD.encode(group_key))
        );
        // type 缺席 → UNKNOWN=0，事件照投（官方 coercion 同构）。
        assert_eq!(out["messageRequestResponseType"], json!(0));

        // 枚举外数值（未来枚举 99）→ UNKNOWN=0 不丢弃。
        let out = project_message_request_response(&MessageRequestResponse {
            r#type: Some(99),
            thread_aci: Some(thread.to_owned()),
            thread_aci_binary: None,
            group_id: None,
        })
        .expect("unknown type still projects");
        assert_eq!(out["messageRequestResponseType"], json!(0));

        // 丢弃面：群长度非法（31B/33B）整事件丢弃（官方 removeFromCache 同果
        // ——即使线程可解析也不路由）。
        assert!(project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: Some(thread.to_owned()),
            thread_aci_binary: None,
            group_id: Some(vec![0x99u8; 31]),
        })
        .is_none());
        assert!(project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: None,
            thread_aci_binary: Some(uuid_bytes(thread)),
            group_id: Some(vec![0x99u8; 33]),
        })
        .is_none());

        // 丢弃面：线程不可解析（binary 坏 + string 缺/坏）且无群。
        assert!(project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: Some("not-a-uuid".to_owned()),
            thread_aci_binary: Some(vec![0xde, 0xad]),
            group_id: None,
        })
        .is_none());
        assert!(project_message_request_response(&MessageRequestResponse {
            r#type: Some(1),
            thread_aci: None,
            thread_aci_binary: None,
            group_id: None,
        })
        .is_none());
    }

    /// 消息请求写面 type 词汇表全口径：六值驼峰词逐一映射 proto 枚举序数
    ///（桌面 syncMessageRequestResponse 直传枚举同词汇）；UNKNOWN/词表外/
    /// 大小写漂移/空串一律 None（methods.rs 参数闸 fail-closed 的词表面）。
    #[test]
    fn message_request_type_glossary_maps_six_official_words() {
        use presage::proto::sync_message::message_request_response::Type;
        for (word, value) in [
            ("accept", Type::Accept),
            ("delete", Type::Delete),
            ("block", Type::Block),
            ("blockAndDelete", Type::BlockAndDelete),
            ("spam", Type::Spam),
            ("blockAndSpam", Type::BlockAndSpam),
        ] {
            assert_eq!(message_request_type_of(word), Some(value));
            assert_eq!(
                message_request_type_of(word).expect("mapped") as i32,
                value as i32
            );
        }
        for rejected in [
            "unknown",
            "UNKNOWN",
            "Accept",
            "ACCEPT",
            "block_and_delete",
            "blockanddelete",
            "unblock",
            "",
            "0",
        ] {
            assert!(
                message_request_type_of(rejected).is_none(),
                "词表外 {rejected:?} 必须拒"
            );
        }
    }

    /// 消息请求同步构造字节面（官方 proto 字段钉）：thread 腿 binary 直写
    ///（字段5 threadAciBinary = 16B UUID、字段2 缺席）；群腿（字段3 groupId
    /// = 32B master key、thread 双字段全缺席）；type 序数落字段4；oneof 变体
    /// = messageRequestResponse（SyncMessage.content 字段14）。两腿互斥同构
    ///（官方 Params groupId/threadAci/threadAciBinary 三位按腿赋值）。
    #[test]
    fn message_request_response_sync_encodes_official_proto_bytes() {
        use presage::proto::sync_message;
        let thread = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

        // thread 腿：prost 字段序升序（4 < 5）= 字段4{1}（tag 0x20）在前、
        // 长度分隔的 threadAciBinary（字段5，tag 0x2a + len 16）在后；
        // 字段2（0x12）/字段3（0x1a）不在列。
        let sync = build_message_request_response_sync(
            &MessageRequestTarget::Thread(thread.to_owned()),
            sync_message::message_request_response::Type::Accept,
        );
        let Some(sync_message::Content::MessageRequestResponse(mrr)) = sync.content else {
            panic!("oneof 变体必须是 messageRequestResponse");
        };
        assert_eq!(mrr.r#type, Some(1));
        assert_eq!(
            mrr.thread_aci_binary.as_deref(),
            Some(uuid_bytes(thread).as_slice())
        );
        assert!(mrr.thread_aci.is_none(), "binary 形态不写 string 字段");
        assert!(mrr.group_id.is_none());
        let bytes = {
            use prost::Message as _;
            let mut buf = Vec::new();
            mrr.encode(&mut buf).expect("proto encodes");
            buf
        };
        // prost 按字段序升序编码：type（字段4，tag 0x20）在前，长度分隔的
        // threadAciBinary（字段5，tag 0x2a + len 16）在后。
        assert_eq!(
            bytes,
            [vec![0x20, 0x01], vec![0x2a, 16], uuid_bytes(thread)].concat(),
            "thread 腿 = 字段4{{1}} + 字段5{{16B UUID}}"
        );
        assert!(!bytes.starts_with(&[0x12]), "字段2（threadAci string）缺席");
        assert!(!bytes.starts_with(&[0x1a]), "字段3（groupId）缺席");

        // 群腿：字段3 = tag 0x1a + len 32 + master key；thread 双字段缺席；
        // 字段4 = type 序数（BLOCK_AND_SPAM=6 → 0x20 0x06）。
        let master_key = [0x33u8; 32];
        let sync = build_message_request_response_sync(
            &MessageRequestTarget::Group(master_key.to_vec()),
            sync_message::message_request_response::Type::BlockAndSpam,
        );
        let Some(sync_message::Content::MessageRequestResponse(mrr)) = sync.content else {
            panic!("oneof 变体必须是 messageRequestResponse");
        };
        assert_eq!(mrr.r#type, Some(6));
        assert_eq!(mrr.group_id.as_deref(), Some(master_key.as_slice()));
        assert!(mrr.thread_aci_binary.is_none());
        assert!(mrr.thread_aci.is_none());
        let bytes = {
            use prost::Message as _;
            let mut buf = Vec::new();
            mrr.encode(&mut buf).expect("proto encodes");
            buf
        };
        // prost 字段序升序（3 < 4）：群腿 = 字段3{32B master key} +
        // 字段4{6}，thread 双字段（0x12/0x2a 头）缺席。
        assert_eq!(
            bytes,
            [vec![0x1a, 32], master_key.to_vec(), vec![0x20, 0x06]].concat(),
            "群腿 = 字段3{{32B master key}} + 字段4{{6}}"
        );
        assert!(!bytes.starts_with(&[0x12]), "字段2 缺席");
        assert!(!bytes.windows(2).any(|w| w == [0x2a, 16]), "字段5 缺席");

        // string 形态兜底（已闸的不可 16B 解析串，service 层防御分支）：
        // 字段2 = tag 0x12 + len + utf8；字段5 缺席。
        let odd = "u:88889999";
        let sync = build_message_request_response_sync(
            &MessageRequestTarget::Thread(odd.to_owned()),
            sync_message::message_request_response::Type::Delete,
        );
        let Some(sync_message::Content::MessageRequestResponse(mrr)) = sync.content else {
            panic!("oneof 变体必须是 messageRequestResponse");
        };
        assert_eq!(mrr.thread_aci.as_deref(), Some(odd));
        assert!(mrr.thread_aci_binary.is_none());
        assert_eq!(mrr.r#type, Some(2));
        let bytes = {
            use prost::Message as _;
            let mut buf = Vec::new();
            mrr.encode(&mut buf).expect("proto encodes");
            buf
        };
        // prost 字段序升序（2 < 4）：string 形态 = 字段2{{utf8}} + 字段4{{2}}，
        // 字段5 缺席。
        assert_eq!(
            bytes,
            [
                vec![0x12, odd.len() as u8],
                odd.as_bytes().to_vec(),
                vec![0x20, 0x02],
            ]
            .concat(),
            "string 形态 = 字段2{{utf8}} + 字段4{{2}}"
        );
        assert!(
            !bytes.windows(2).any(|w| w == [0x2a, odd.len() as u8]),
            "字段5 缺席"
        );

        // padded sync message：其他字段为 padding 面（presage
        // with_padding 同源），oneof 恰一个变体由上方模式匹配钉定。
    }

    /// 链接 URI 的 numberless capability 门（取证 docs/upstream-capability-nopni2.md）：
    /// Android 8.30+/8.31 扫码侧对无 PNI 账号强制 `nopni2`、旧版强制 `nopni`，
    /// 缺失即 OUTDATED_DEVICE 拒连；双报对齐官方桌面，`backup5` 不报
    /// （引擎未实现 link-and-sync，报了手机会走 archive 同步链接流）。
    #[test]
    fn link_uri_advertises_numberless_capabilities() {
        use presage::libsignal_service::provisioning::link_uri;

        let url = link_uri("uuid-1234", "PUBKEY=");
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        assert!(pairs.contains(&("uuid".to_owned(), "uuid-1234".to_owned())));
        assert!(pairs.contains(&("pub_key".to_owned(), "PUBKEY=".to_owned())));
        let capabilities = pairs
            .iter()
            .find(|(key, _)| key == "capabilities")
            .map(|(_, value)| value.as_str())
            .expect("link URI carries capabilities");
        let names: Vec<&str> = capabilities.split(',').collect();
        assert!(names.contains(&"nopni"), "{capabilities}");
        assert!(names.contains(&"nopni2"), "{capabilities}");
        assert!(!names.contains(&"backup5"), "{capabilities}");
    }

    /// DecryptionError 结构化事件形状（止血批①，加面）：receive 事件族
    /// 同构，sourceUuid + decryptionError 恒在；timestamp 仅在已知时出现
    /// （sealed sender 失败路径无时间戳）。不改任何既有投影字段。
    #[test]
    fn decryption_error_event_shape_is_additive_receive_face() {
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");

        let with_ts = decryption_error_notification("acct", &sender, Some(1_700_000_000_000));
        assert_eq!(with_ts["jsonrpc"], json!("2.0"));
        assert_eq!(with_ts["method"], json!("receive"));
        assert_eq!(with_ts["params"]["account"], json!("acct"));
        let envelope = &with_ts["params"]["envelope"];
        assert_eq!(envelope["timestamp"], json!(1_700_000_000_000_u64));
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(envelope["decryptionError"], json!({}));

        let without_ts = decryption_error_notification("acct", &sender, None);
        let envelope = &without_ts["params"]["envelope"];
        assert!(envelope.get("timestamp").is_none());
        assert_eq!(
            envelope["sourceUuid"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(envelope["decryptionError"], json!({}));
    }

    /// G3 排空通知形状面：`receive.emptied` 独立方法（非 envelope 载体——
    /// 排空是流态事件，无信封可挂），per-account 单键 params。
    #[test]
    fn queue_emptied_notification_shape_is_additive_stream_face() {
        let notification = queue_emptied_notification("acct");
        assert_eq!(notification["jsonrpc"], json!("2.0"));
        assert_eq!(notification["method"], json!("receive.emptied"));
        assert_eq!(notification["params"]["account"], json!("acct"));
        assert!(notification["params"].get("envelope").is_none());
    }

    /// DERM 新鲜度窗口（官方 retryRespondMaxAge=14 天同语义）：窗口内
    /// 放行，边界外（超龄/未来时间戳）转会话重建档。
    #[test]
    fn derm_request_freshness_window_matches_official_14d() {
        let now = 1_700_000_000_000_u64;
        let day: u64 = 24 * 60 * 60 * 1_000;
        assert!(derm_request_fresh(now, now));
        assert!(derm_request_fresh(now, now - 13 * day));
        assert!(derm_request_fresh(now, now - 14 * day));
        assert!(!derm_request_fresh(now, now - 14 * day - 1));
        // 未来时间戳（时钟偏差）不放行。
        assert!(!derm_request_fresh(now, now + 1));
    }

    /// DERM 防环计数（官方 RETRY_LIMIT=5 同值）：同 (来源, 原始时间戳)
    /// 第 6 次起不放行；不同键互不挤占。
    #[test]
    fn derm_retry_guard_caps_repeats_per_key() {
        let mut guard = DermRetryGuard::default();
        for _ in 0..DERM_RETRY_LIMIT {
            assert!(guard.admit("sender-a", 1000));
        }
        assert!(!guard.admit("sender-a", 1000));
        // 另一来源/另一时间戳不受同键计数牵连。
        assert!(guard.admit("sender-b", 1000));
        assert!(guard.admit("sender-a", 2000));
    }

    /// DERM 重发资格矩阵：直聊线程须恰指向请求者；群线程须请求者在成员
    /// 名单内（非收件人请求不重发，防群内容外泄）。
    #[test]
    fn derm_resend_allowed_matrix() {
        use presage::libsignal_service::groups_v2::Role;
        use presage::libsignal_service::prelude::Uuid;
        use presage::libsignal_service::protocol::Aci;
        use presage::model::groups::Member;
        use presage::store::Thread;

        let requester_aci = Aci::from(
            Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("requester aci"),
        );
        let stranger_aci = Aci::from(
            Uuid::parse_str("99999999-2222-4333-8444-555555555555").expect("stranger aci"),
        );
        let requester = ServiceId::from(requester_aci);
        let stranger = ServiceId::from(stranger_aci);
        let member = |aci: Aci| Member {
            aci,
            role: Role::Default,
            profile_key: presage::libsignal_service::prelude::ProfileKey::create([7u8; 32]),
            joined_at_revision: 1,
            label: None,
            label_emoji: None,
        };

        // 直聊：请求者本人 → 重发；他人借道同线程键 → 拒。
        assert!(derm_resend_allowed(
            &Thread::Contact(requester),
            &requester,
            None
        ));
        assert!(!derm_resend_allowed(
            &Thread::Contact(requester),
            &stranger,
            None
        ));
        // 群：成员 → 重发；非成员（仅他人/请求者在名单时第三方借道同拒）/
        // 名单不可得 → 会话重建档。
        let members = vec![member(requester_aci)];
        assert!(derm_resend_allowed(
            &Thread::Group([0u8; 32]),
            &requester,
            Some(&members)
        ));
        assert!(!derm_resend_allowed(
            &Thread::Group([0u8; 32]),
            &stranger,
            Some(&members)
        ));
        assert!(!derm_resend_allowed(
            &Thread::Group([0u8; 32]),
            &requester,
            None
        ));
    }

    /// 群发送失败词表（per-recipient failure.type additive 面）：NotFound=
    /// UnregisteredFailure、UntrustedIdentity=UntrustedIdentityFailure、
    /// Unauthorized=AuthorizationFailedFailure、其余折叠 NetworkFailure。
    #[test]
    fn group_send_failure_word_matrix() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError as Error;

        let not_found = Error::NotFound {
            service_id: ServiceId::from(presage::libsignal_service::protocol::Aci::from(
                presage::libsignal_service::prelude::Uuid::nil(),
            )),
        };
        assert_eq!(group_send_failure_word(&not_found), "UnregisteredFailure");
        let untrusted = Error::UntrustedIdentity {
            address: ServiceId::from(presage::libsignal_service::protocol::Aci::from(
                presage::libsignal_service::prelude::Uuid::nil(),
            )),
        };
        assert_eq!(
            group_send_failure_word(&untrusted),
            "UntrustedIdentityFailure"
        );
        assert_eq!(
            group_send_failure_word(&Error::ServiceError(ServiceError::Unauthorized)),
            "AuthorizationFailedFailure"
        );
        assert_eq!(
            group_send_failure_word(&Error::ServiceError(ServiceError::RateLimitExceeded {
                retry_after: None
            })),
            "NetworkFailure"
        );
        assert_eq!(
            group_send_failure_word(&Error::NoMessagesToSend),
            "NetworkFailure"
        );
    }

    /// 群发送结果条目组装形状（send 群形态 `results` additive 面）：成功
    /// `{recipientNumber, success:{timestamp}}`、失败 `{recipientNumber,
    /// failure:{type}}`；contacts 未命中回退 ACI uuid 串。
    #[test]
    fn group_send_result_entries_shapes() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError as Error;

        let aci = ServiceId::from(presage::libsignal_service::protocol::Aci::from(
            presage::libsignal_service::prelude::Uuid::parse_str(
                "11111111-2222-4333-8444-555555555555",
            )
            .expect("aci"),
        ));
        let outcomes: GroupSendOutcomes = vec![
            (aci, None),
            (aci, Some(Error::NotFound { service_id: aci })),
        ];
        let mut numbers = HashMap::new();
        numbers.insert(
            aci.service_id_string().to_owned(),
            "+15550001111".to_owned(),
        );
        let entries = group_send_result_entries(outcomes, 1_700_000_000_000_u64, &numbers);
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0],
            json!({
                "recipientNumber": "+15550001111",
                "success": { "timestamp": 1_700_000_000_000_u64 },
            })
        );
        assert_eq!(
            entries[1],
            json!({
                "recipientNumber": "+15550001111",
                "failure": { "type": "UnregisteredFailure" },
            })
        );
        // contacts 未命中：recipientNumber 回退 ACI uuid 串（listGroups 同纪律）。
        let fallback = group_send_result_entries(
            vec![(aci, Some(Error::ServiceError(ServiceError::Unauthorized)))],
            7,
            &HashMap::new(),
        );
        assert_eq!(
            fallback[0]["recipientNumber"],
            json!(aci.service_id_string())
        );
        assert_eq!(
            fallback[0]["failure"]["type"],
            json!("AuthorizationFailedFailure")
        );
    }

    /// 群发送语义三件的两个 vendor 纯函数契约：typing 不触发群态刷新
    /// （官方 typing 不刷群态），其余发送强刷；announcements-only 闸判
    /// ADMINISTRATOR 角色（Default/缺席皆非 admin）。
    #[test]
    fn group_refresh_and_announcements_gate_contract() {
        use presage::libsignal_service::groups_v2::Role;
        use presage::libsignal_service::prelude::{ProfileKey, Uuid};
        use presage::libsignal_service::protocol::Aci;
        use presage::manager::{group_member_is_admin, group_refresh_needed};
        use presage::model::groups::{Group, Member};
        use presage::proto::DataMessage;

        // 刷新决策：typing 免刷，DataMessage 强刷。
        assert!(!group_refresh_needed(&ContentBody::TypingMessage(
            presage::proto::TypingMessage::default()
        )));
        assert!(group_refresh_needed(&ContentBody::DataMessage(
            DataMessage::default()
        )));

        let own = Aci::from(Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("own"));
        let other =
            Aci::from(Uuid::parse_str("99999999-2222-4333-8444-555555555555").expect("other"));
        let member = |aci: Aci, role: Role| Member {
            aci,
            role,
            profile_key: ProfileKey::create([7u8; 32]),
            joined_at_revision: 1,
            label: None,
            label_emoji: None,
        };
        let group = |members: Vec<Member>| Group {
            title: "g".to_owned(),
            avatar: String::new(),
            disappearing_messages_timer: None,
            access_control: None,
            revision: 1,
            members,
            pending_members: Vec::new(),
            requesting_members: Vec::new(),
            invite_link_password: Vec::new(),
            description: None,
            announcements_only: true,
            banned_members: Vec::new(),
        };

        // 本机为 ADMINISTRATOR → 闸放行；Default 角色/缺席 → 闸拦下。
        assert!(group_member_is_admin(
            &group(vec![
                member(other, Role::Default),
                member(own, Role::Administrator)
            ]),
            &own
        ));
        assert!(!group_member_is_admin(
            &group(vec![member(own, Role::Default)]),
            &own
        ));
        assert!(!group_member_is_admin(
            &group(vec![member(other, Role::Administrator)]),
            &own
        ));
    }

    /// send_error_of announcements 归型：vendor GroupAnnouncementsOnly →
    /// SendError::AnnouncementsOnly（不经 Internal 折叠）。
    #[test]
    fn send_errors_classify_announcements_only() {
        assert!(matches!(
            send_error_of(presage::Error::GroupAnnouncementsOnly),
            SendError::AnnouncementsOnly
        ));
    }

    /// 重试回执解码形状（derm_send_receipt 入参面）：序列化
    /// DecryptionErrorMessage proto 往返后 timestamp/device_id 保真——
    /// 请求方按这两个位找回原消息。
    #[test]
    fn derm_receipt_roundtrip_keeps_timestamp_and_device() {
        use presage::proto::DecryptionErrorMessage;
        use prost::Message as _;

        let receipt = DecryptionErrorMessage {
            timestamp: Some(1_700_000_000_000),
            ratchet_key: Some(vec![1, 2, 3]),
            device_id: Some(2),
        };
        let bytes = receipt.encode_to_vec();
        let decoded = DecryptionErrorMessage::decode(bytes.as_slice()).expect("roundtrip");
        assert_eq!(decoded.timestamp, Some(1_700_000_000_000));
        assert_eq!(decoded.device_id, Some(2));
        assert_eq!(decoded.ratchet_key, Some(vec![1, 2, 3]));
    }

    /// 投影去重键（止血批①③）：presage unfold 以
    /// `thread_for_sender_and_timestamp(sender, client_ts)` 判重复投递，
    /// 本测试钉定该判重键与 store 落库行的对齐（同 sender+ts 命中，
    /// 换 ts/换 sender 不命中）——命中即 Duplicate（不再投影/通知）。
    #[tokio::test]
    async fn dedupe_key_hits_store_row_for_redelivered_envelope() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::libsignal_service::protocol::DeviceId;
        use presage::proto::NullMessage;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-dedupe-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let other = ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
            .expect("other aci");
        let metadata = |sender| Metadata {
            sender,
            destination: sender,
            sender_device: DeviceId::new(1).expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        // NullMessage 走 save_message 落库面（receipt/call 同族，typing 不落库
        // ——瞬态面不去重，与 unfold 注释一致）。
        let content = Content::from_body(
            ContentBody::NullMessage(NullMessage::default()),
            metadata(sender),
        );
        let thread = presage::store::Thread::Contact(sender);
        store.save_message(&thread, content).await.unwrap();

        // 同 (sender, client_ts)：重复投递 → 命中 → Duplicate。
        let ts = 1_700_000_000_000_u64;
        assert!(store
            .thread_for_sender_and_timestamp(&sender, ts)
            .await
            .unwrap()
            .is_some());
        // 换 sender / 换 ts：真新消息 → 不命中 → 照常投影。
        assert!(store
            .thread_for_sender_and_timestamp(&other, ts)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .thread_for_sender_and_timestamp(&sender, ts + 1)
            .await
            .unwrap()
            .is_none());
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 重连退避序列（止血批②，官方 Desktop ts/backoff/BackOff.std.ts 同
    /// 序列钉定）：Fibonacci 1,1,2,3,5,8,13,21,34,55 封顶 55s，之后不再
    /// 增长；step=0（连接成功 reset 后）从 1s 重来。
    #[test]
    fn reconnect_backoff_follows_official_fibonacci_sequence() {
        let ms = |step| reconnect_backoff_delay(step).as_millis();
        assert_eq!(ms(0), 1_000);
        assert_eq!(ms(1), 1_000);
        assert_eq!(ms(2), 2_000);
        assert_eq!(ms(3), 3_000);
        assert_eq!(ms(4), 5_000);
        assert_eq!(ms(5), 8_000);
        assert_eq!(ms(6), 13_000);
        assert_eq!(ms(7), 21_000);
        assert_eq!(ms(8), 34_000);
        assert_eq!(ms(9), 55_000);
        assert_eq!(ms(10), 55_000, "官方 MAX_BACKOFF 封顶");
        assert_eq!(ms(50), 55_000, "封顶后不增长");
    }

    /// 退避 jitter（P12/R5，官方 BackOff.get 同语义）：≤5s 档（含首拍
    /// 1s）恒精确；>5s 单向上浮 [base, base+5s)，不下行。
    #[test]
    fn reconnect_jitter_matches_official_short_exact_long_upward() {
        for base_ms in [0u64, 500, 1_000, 5_000] {
            for _ in 0..50 {
                let delay = reconnect_jittered_delay(Duration::from_millis(base_ms));
                assert_eq!(
                    delay,
                    Duration::from_millis(base_ms),
                    "短拍必须精确：base={base_ms} delay={delay:?}"
                );
            }
        }
        for base_ms in [8_000u64, 55_000] {
            for _ in 0..50 {
                let delay = reconnect_jittered_delay(Duration::from_millis(base_ms)).as_millis();
                assert!(delay >= base_ms as u128, "base={base_ms} delay={delay}");
                assert!(
                    delay < base_ms as u128 + 5_000,
                    "base={base_ms} delay={delay}"
                );
            }
        }
    }

    /// 证书预轮换退避（止血批③）：健康 = 10min 轮询；失败 1→16min 指数，
    /// 封顶轮询间隔。
    #[test]
    fn certificate_maintenance_backoff_interval_and_cap() {
        let delay = |failures| CertificateMaintenance { failures }.delay();
        assert_eq!(delay(0).as_secs(), 600);
        assert_eq!(delay(1).as_secs(), 60);
        assert_eq!(delay(2).as_secs(), 120);
        assert_eq!(delay(3).as_secs(), 240);
        assert_eq!(delay(4).as_secs(), 480);
        assert_eq!(delay(5).as_secs(), 600, "封顶轮询间隔");
        assert_eq!(delay(20).as_secs(), 600);
    }

    /// identified 保险丝（止血批③，官方 Desktop identified fallback 同
    /// 语义）：证书获取失败（调用方映射 None）→ 无 unidentified access →
    /// MessageSender 走 identified 分支（sender.rs "sending identified"），
    /// 发送不再被证书临期/续期失败打死；key 缺席维持既有 None 语义。
    #[test]
    fn sender_certificate_failure_falls_back_to_identified_access() {
        use presage::libsignal_service::zkgroup::profiles::ProfileKey;
        use presage::manager::unidentified_access_or_identified;

        assert!(
            unidentified_access_or_identified(Some(ProfileKey::create([7u8; 32])), None).is_none()
        );
        assert!(unidentified_access_or_identified(None, None).is_none());
    }

    /// 群 typing wire 形状（止血批④，官方 conversations.preload.ts
    /// :1510-1516 对齐）：groupId（proto tag 3, bytes）= 32 字节群 master
    /// key，编码级钉定；action Started/Stopped 两态同体携带。
    #[test]
    fn group_typing_wire_carries_group_master_key() {
        use presage::proto::typing_message::Action;
        use prost::Message as _;

        let master_key = [0xABu8; 32];
        let body = typing_group_body(&master_key, 1_700_000_000_000, false);
        assert_eq!(body.timestamp, Some(1_700_000_000_000));
        assert_eq!(body.action, Some(Action::Started as i32));
        assert_eq!(body.group_id.as_deref(), Some(master_key.as_slice()));

        // wire 级：encode → groupId 字段（tag 3, wire type 2 → 0x1A）在位
        // 且载荷恰为 master key 字节；反解码回同值。
        let bytes = typing_group_body(&master_key, 5, true).encode_to_vec();
        let pos = bytes
            .windows(2)
            .position(|w| w == [0x1A, 0x20])
            .expect("groupId tag3 length-delimited in wire");
        assert_eq!(&bytes[pos + 2..pos + 34], &master_key);
        let decoded = presage::proto::TypingMessage::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(decoded.group_id.as_deref(), Some(master_key.as_slice()));
        assert_eq!(decoded.action, Some(Action::Stopped as i32));
        assert_eq!(decoded.timestamp, Some(5));
    }

    /// B4 Unbound 停环：建流错误归型——Unauthorized 两层归型面（直接
    /// ServiceError 与 sender 再包层）判 Unbound；404 NotFound / 其他
    /// presage 错误不误报（维持退避重连，send_error_of 同纪律）。
    #[test]
    fn receive_stream_unbound_classifies_unauthorized_only() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        let unbound = |error| assert!(receive_stream_unbound(&error));
        let bound = |error| assert!(!receive_stream_unbound(&error));

        unbound(presage::Error::ServiceError(ServiceError::Unauthorized));
        unbound(presage::Error::MessageSenderError(Box::new(
            MessageSenderError::ServiceError(ServiceError::Unauthorized),
        )));
        bound(presage::Error::ServiceError(ServiceError::NotFoundError));
        bound(presage::Error::MessageSenderError(Box::new(
            MessageSenderError::ServiceError(ServiceError::NotFoundError),
        )));
        bound(presage::Error::UnknownGroup);
    }

    /// B4 事件面形状钉定：身份钥变更 = receive 族 + envelope.identityChanged
    /// 标记（decryptionError 同款加面纪律）；Unbound = accountUnbound 独立
    /// method + account 参数。
    #[test]
    fn identity_change_and_unbound_notification_shapes() {
        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");

        let identity = identity_changed_notification("acct", &sender, 1_700_000_000_000);
        assert_eq!(identity["jsonrpc"], "2.0");
        assert_eq!(identity["method"], "receive");
        assert_eq!(identity["params"]["account"], "acct");
        let envelope = &identity["params"]["envelope"];
        assert_eq!(
            envelope["sourceUuid"],
            "11111111-2222-4333-8444-555555555555"
        );
        assert_eq!(envelope["timestamp"], 1_700_000_000_000i64);
        assert_eq!(envelope["identityChanged"], json!({}));

        let unbound = account_unbound_notification("acct");
        assert_eq!(unbound["method"], "accountUnbound");
        assert_eq!(unbound["params"]["account"], "acct");
    }

    /// B4 身份事件面：verified 同步 state 归型——vendored proto 三值
    /// （DEFAULT=0/VERIFIED=1/UNVERIFIED=2）全出；缺字段/越界值归 None
    /// （收侧跳过不落，宁缺勿错）。
    #[test]
    fn trust_state_from_sync_covers_pinned_proto_values() {
        use presage::proto::verified::State;

        assert_eq!(
            identity::TrustState::from_sync_state(Some(State::Default as i32)),
            Some(identity::TrustState::Default)
        );
        assert_eq!(
            identity::TrustState::from_sync_state(Some(State::Verified as i32)),
            Some(identity::TrustState::Verified)
        );
        assert_eq!(
            identity::TrustState::from_sync_state(Some(State::Unverified as i32)),
            Some(identity::TrustState::Unverified)
        );
        assert_eq!(identity::TrustState::from_sync_state(None), None);
        assert_eq!(identity::TrustState::from_sync_state(Some(99)), None);
    }

    /// B4 verified 同步下行全链：sidecar 权威突变（首建档 + 跨态迁移保留
    /// addedAtMs）+ receive 事件投影（destinationUuid/destinationNumber?/
    /// identityKey/state trust 词表）；普通投影面对该体恒空投影（零冲突）；
    /// destination 缺席整信封跳过不落不投。
    #[tokio::test]
    async fn verified_sync_writes_sidecar_and_projects() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::sync_message::Content as SyncContent;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-verified-sync-{:016x}",
            mix_seed_step(mix_seed())
        ));
        std::fs::create_dir_all(&store_dir).unwrap();
        let store = open_store(&store_dir).await.unwrap();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let verified_body = |state: presage::proto::verified::State, with_dest: bool| {
            Content::from_body(
                ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                    content: Some(SyncContent::Verified(presage::proto::Verified {
                        destination_aci_binary: with_dest.then(|| {
                            presage::libsignal_service::prelude::Uuid::parse_str(
                                "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
                            )
                            .expect("uuid")
                            .into_bytes()
                            .to_vec()
                        }),
                        identity_key: Some(vec![7u8; 33]),
                        state: Some(state as i32),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                metadata.clone(),
            )
        };
        let sidecar_state = |dir: &std::path::Path| {
            serde_json::from_str::<serde_json::Value>(
                &std::fs::read_to_string(dir.join("identity-trust.json")).expect("sidecar"),
            )
            .expect("json")["identities"]["6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"]
                .clone()
        };

        // verified：sidecar 首建档 + 投影全形状。
        let first = project_verified_sync(
            "acct",
            &store,
            &verified_body(presage::proto::verified::State::Verified, true),
            &store_dir,
        )
        .await
        .expect("verified projects");
        assert_eq!(first["method"], "receive");
        assert_eq!(first["params"]["account"], "acct");
        let envelope = &first["params"]["envelope"];
        assert_eq!(
            envelope["sourceUuid"],
            "11111111-2222-4333-8444-555555555555"
        );
        assert_eq!(envelope["timestamp"], 1_700_000_000_000i64);
        let face = &envelope["syncMessage"]["verified"];
        assert_eq!(
            face["destinationUuid"],
            "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"
        );
        assert_eq!(face["state"], "verified");
        assert_eq!(face["identityKey"], hex_encode(&[7u8; 33]));
        let entry = sidecar_state(&store_dir);
        assert_eq!(entry["state"], "verified");
        assert_eq!(entry["addedAtMs"], 1_700_000_000_000u64);

        // unverified：跨态迁移保留 addedAtMs；普通投影面恒空（零冲突）。
        let added_before = entry["addedAtMs"].as_u64();
        let second = project_verified_sync(
            "acct",
            &store,
            &verified_body(presage::proto::verified::State::Unverified, true),
            &store_dir,
        )
        .await
        .expect("unverified projects");
        assert_eq!(
            second["params"]["envelope"]["syncMessage"]["verified"]["state"],
            "unverified"
        );
        let entry = sidecar_state(&store_dir);
        assert_eq!(entry["state"], "unverified");
        assert_eq!(entry["addedAtMs"].as_u64(), added_before);

        assert!(project_content(
            "acct",
            &store,
            &verified_body(presage::proto::verified::State::Verified, true),
            &groups::TerminatedGroups::new(),
        )
        .await
        .is_empty());

        // destination 缺席：整信封跳过不落不投。
        assert!(project_verified_sync(
            "acct",
            &store,
            &verified_body(presage::proto::verified::State::Verified, false,),
            &store_dir
        )
        .await
        .is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Q2/Q3/Q4 收投影：contact 卡片（键形与 backup contact_record 同族 +
    /// type 词表 + 空 value 条目丢弃 + 空卡片整键缺席）、poll 三臂（去重保序/
    /// voteCount 0 透出/targetAuthor E.164 映射/清票空数组）、groupCallUpdate
    /// （eraId 在场/空串整键缺席）。
    #[test]
    fn data_message_projects_contact_poll_and_group_call_faces() {
        use presage::proto::data_message::contact::{self, Avatar, Name, Phone};
        use presage::proto::data_message::Contact;
        use std::collections::HashMap;

        let aci = "11111111-2222-4333-8444-555555555555";
        let mut author_numbers = HashMap::new();
        author_numbers.insert(aci.to_owned(), "+15550001111".to_owned());

        // 全卡片：name 非空子字段 + number（MOBILE）+ email（HOME 缺省词）+
        // organization + avatar（isProfile 恒投）。
        let out = project_data_message(
            &DataMessage {
                timestamp: Some(1),
                contact: vec![Contact {
                    name: Some(Name {
                        given_name: Some("Ada".to_owned()),
                        family_name: Some("Lovelace".to_owned()),
                        prefix: Some("".to_owned()),
                        ..Name::default()
                    }),
                    number: vec![
                        Phone {
                            value: Some("+15551234567".to_owned()),
                            r#type: Some(contact::phone::Type::Mobile as i32),
                            label: Some("cell".to_owned()),
                        },
                        // value 空整条丢弃。
                        Phone {
                            value: Some(String::new()),
                            r#type: Some(contact::phone::Type::Work as i32),
                            label: None,
                        },
                    ],
                    email: vec![contact::Email {
                        value: Some("ada@example.test".to_owned()),
                        // 缺席 type 归 HOME 词。
                        r#type: None,
                        label: None,
                    }],
                    organization: Some("KT".to_owned()),
                    avatar: Some(Avatar {
                        avatar: Some(AttachmentPointer {
                            content_type: Some("image/png".to_owned()),
                            ..AttachmentPointer::default()
                        }),
                        is_profile: Some(true),
                    }),
                    ..Contact::default()
                }],
                ..DataMessage::default()
            },
            &author_numbers,
        );
        let card = &out["contact"];
        assert_eq!(
            card["name"],
            json!({ "givenName": "Ada", "familyName": "Lovelace" })
        );
        assert_eq!(
            card["number"],
            json!([{ "value": "+15551234567", "type": "MOBILE", "label": "cell" }])
        );
        assert_eq!(
            card["email"],
            json!([{ "value": "ada@example.test", "type": "HOME" }])
        );
        assert_eq!(card["organization"], json!("KT"));
        assert_eq!(card["avatar"]["isProfile"], json!(true));
        assert!(card["avatar"]["avatar"].is_object(), "avatar pointer face");

        // 空卡片（全部字段缺席）→ contact 键不在（永不投占位）。
        let out = project_data_message(
            &DataMessage {
                contact: vec![Contact::default()],
                ..DataMessage::default()
            },
            &author_numbers,
        );
        assert!(out.get("contact").is_none(), "空卡片整键缺席");

        // poll 三臂：optionIndexes 去重保首现序、voteCount 0 透出、
        // targetAuthor E.164 命中转号码。
        let out = project_data_message(
            &DataMessage {
                timestamp: Some(1),
                poll_create: Some(data_message::PollCreate {
                    question: Some("lunch?".to_owned()),
                    allow_multiple: Some(true),
                    options: vec!["a".to_owned(), "b".to_owned()],
                }),
                poll_vote: Some(data_message::PollVote {
                    target_author_aci_binary: Some(
                        ServiceId::parse_from_service_id_string(aci)
                            .unwrap()
                            .raw_uuid()
                            .as_bytes()
                            .to_vec(),
                    ),
                    target_sent_timestamp: Some(1_699_999_000_000),
                    option_indexes: vec![2, 1, 2, 3],
                    vote_count: Some(0),
                }),
                poll_terminate: Some(data_message::PollTerminate {
                    target_sent_timestamp: Some(1_699_999_000_500),
                }),
                ..DataMessage::default()
            },
            &author_numbers,
        );
        assert_eq!(
            out["poll"],
            json!({ "question": "lunch?", "options": ["a", "b"], "allowMultiple": true })
        );
        assert_eq!(
            out["pollVote"],
            json!({
                "targetAuthor": "+15550001111",
                "targetSentTimestamp": 1_699_999_000_000_u64,
                "optionIndexes": [2, 1, 3],
                "voteCount": 0,
            })
        );
        assert_eq!(
            out["pollTerminate"],
            json!({ "targetSentTimestamp": 1_699_999_000_500_u64 })
        );

        // groupCallUpdate：eraId 在场投影；空串整键缺席。
        let out = project_data_message(
            &DataMessage {
                group_call_update: Some(data_message::GroupCallUpdate {
                    era_id: Some("era-1".to_owned()),
                }),
                ..DataMessage::default()
            },
            &author_numbers,
        );
        assert_eq!(out["groupCallUpdate"], json!({ "eraId": "era-1" }));
        let out = project_data_message(
            &DataMessage {
                group_call_update: Some(data_message::GroupCallUpdate {
                    era_id: Some(String::new()),
                }),
                ..DataMessage::default()
            },
            &author_numbers,
        );
        assert!(out.get("groupCallUpdate").is_none(), "空 eraId 整键缺席");
    }

    /// Q5 quote 收投影：type 恒产（GIFT_BADGE 词 / 未知值 coerce NORMAL /
    /// 缺席 NORMAL——官方 isKnownProtoEnumMember 同构）+ bodyRanges（mention
    /// 二进制优先 + author_numbers 映射、Style 归同条目 style 键、NONE 丢弃）。
    #[test]
    fn data_message_projects_quote_type_and_body_ranges() {
        use presage::proto::data_message::quote::Type;
        use std::collections::HashMap;

        let aci = "11111111-2222-4333-8444-555555555555";
        let mut author_numbers = HashMap::new();
        author_numbers.insert(aci.to_owned(), "+15550001111".to_owned());
        let aci_bytes = ServiceId::parse_from_service_id_string(aci)
            .unwrap()
            .raw_uuid()
            .as_bytes()
            .to_vec();

        let out = project_data_message(
            &DataMessage {
                quote: Some(data_message::Quote {
                    id: Some(42),
                    author_aci: Some(aci.to_owned()),
                    r#type: Some(Type::GiftBadge as i32),
                    body_ranges: vec![
                        BodyRange {
                            start: Some(0),
                            length: Some(3),
                            associated_value: Some(AssociatedValue::MentionAciBinary(
                                aci_bytes.clone(),
                            )),
                        },
                        BodyRange {
                            start: Some(4),
                            length: Some(2),
                            associated_value: Some(AssociatedValue::Style(
                                presage::proto::body_range::Style::Spoiler as i32,
                            )),
                        },
                        // NONE：composer 永不出、投影丢弃对称。
                        BodyRange {
                            start: Some(6),
                            length: Some(1),
                            associated_value: Some(AssociatedValue::Style(
                                presage::proto::body_range::Style::None as i32,
                            )),
                        },
                    ],
                    ..Default::default()
                }),
                ..DataMessage::default()
            },
            &author_numbers,
        );
        assert_eq!(out["quote"]["type"], json!("GIFT_BADGE"));
        assert_eq!(
            out["quote"]["bodyRanges"],
            json!([
                { "number": "+15550001111", "start": 0, "length": 3 },
                { "style": "SPOILER", "start": 4, "length": 2 },
            ])
        );

        // 未知 type 值 coerce NORMAL；缺席同（官方 processQuote 同构）。
        for type_value in [Some(99), None] {
            let out = project_data_message(
                &DataMessage {
                    quote: Some(data_message::Quote {
                        id: Some(1),
                        r#type: type_value,
                        ..Default::default()
                    }),
                    ..DataMessage::default()
                },
                &author_numbers,
            );
            assert_eq!(
                out["quote"]["type"],
                json!("NORMAL"),
                "coerce {type_value:?}"
            );
        }
    }

    /// Q1 editMessage 体构造：直聊腿无 group_v2；群腿手注 (master_key,
    /// revision) 上下文（vendored 不为 EditMessage 注入群上下文的引擎侧补位）。
    #[test]
    fn edit_message_body_injects_group_context_when_provided() {
        use presage::libsignal_service::content::ContentBody;
        for group in [None, Some((vec![7u8; 32], Some(5u32)))] {
            let body = build_edit_message_body(42, 100, "edited", group.clone());
            let ContentBody::EditMessage(edit) = body else {
                panic!("edit body shape");
            };
            assert_eq!(edit.target_sent_timestamp, Some(42));
            let inner = edit.data_message.expect("inner dataMessage");
            assert_eq!(inner.body.as_deref(), Some("edited"));
            assert_eq!(inner.timestamp, Some(100));
            match group {
                None => assert!(inner.group_v2.is_none(), "直聊腿无群上下文"),
                Some((master_key, revision)) => {
                    let context = inner.group_v2.expect("群腿注入 group_v2");
                    assert_eq!(context.master_key.as_deref(), Some(master_key.as_slice()));
                    assert_eq!(context.revision, revision);
                }
            }
        }
    }

    /// Q1/Q6③ Sent sync echo 投影：editMessage 臂（sentMessage.editMessage
    /// {targetSentTimestamp, dataMessage} + 公共位 timestamp/destination）与
    /// storyMessage 臂（sentMessage.storyMessage 复用 story 投影单点）；
    /// 全缺位整信封丢弃。
    #[tokio::test]
    async fn sent_sync_projects_edit_and_story_echoes() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{sync_message, EditMessage, StoryMessage};
        use presage::proto::{SyncMessage, TextAttachment};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-sent-echo-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let destination =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .expect("destination aci");
        let metadata = Metadata {
            sender,
            destination,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let sent_body = |sent: sync_message::Sent| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    content: Some(sync_message::Content::Sent(sent)),
                    ..SyncMessage::default()
                }),
                metadata.clone(),
            )
        };

        // editMessage echo：公共位 + editMessage 双键形。
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            destination_service_id: Some("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae".to_owned()),
            edit_message: Some(EditMessage {
                target_sent_timestamp: Some(1_699_999_000_000),
                data_message: Some(DataMessage {
                    timestamp: Some(1_700_000_000_123),
                    body: Some("edited body".to_owned()),
                    ..DataMessage::default()
                }),
            }),
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "edit echo projects",
        )["params"]["envelope"]
            .clone();
        let sent_json = &envelope["syncMessage"]["sentMessage"];
        assert_eq!(sent_json["timestamp"], json!(1_700_000_000_123_u64));
        assert_eq!(
            sent_json["destinationUuid"],
            json!("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
        );
        assert_eq!(
            sent_json["editMessage"]["targetSentTimestamp"],
            json!(1_699_999_000_000_u64)
        );
        assert_eq!(
            sent_json["editMessage"]["dataMessage"]["message"],
            json!("edited body")
        );
        assert!(sent_json.get("message").is_none(), "面臂单选");

        // storyMessage echo：复用 story 投影（textAttachment.text 面）。
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            story_message: Some(StoryMessage {
                attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                    TextAttachment {
                        text: Some("hello story".to_owned()),
                        ..TextAttachment::default()
                    },
                )),
                ..StoryMessage::default()
            }),
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "story echo projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyMessage"]["textAttachment"]["text"],
            json!("hello story")
        );

        // 全缺位：整信封丢弃（宁缺勿错）。
        let content = sent_body(sync_message::Sent::default());
        assert!(project_content("acct", &store, &content, &registry)
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Q6② deleteForMe 收包投影：messageDeletes 组投影（author 二进制解析/
    /// e164 直通、timestamp 缺省 0、conversationId groupId→base64）、坏作者
    /// 条目丢弃、全坏整信封丢弃。
    #[tokio::test]
    async fn sync_projects_delete_for_me_face() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::sync_message::delete_for_me::MessageDeletes;
        use presage::proto::{
            sync_message, AddressableMessage, ConversationIdentifier, SyncMessage,
        };

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-delete-for-me-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let metadata = Metadata {
            sender,
            destination: sender,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let author_bytes =
            ServiceId::parse_from_service_id_string("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
                .unwrap()
                .raw_uuid()
                .as_bytes()
                .to_vec();
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe {
                        message_deletes: vec![MessageDeletes {
                            conversation: Some(ConversationIdentifier {
                                identifier: Some(
                                    presage::proto::conversation_identifier::Identifier::
                                        ThreadGroupId(vec![7u8; 32]),
                                ),
                            }),
                            messages: vec![
                                AddressableMessage {
                                    author: Some(
                                        presage::proto::addressable_message::Author::
                                            AuthorServiceIdBinary(author_bytes),
                                    ),
                                    sent_timestamp: Some(1_699_999_000_000),
                                },
                                // 坏作者条目（无 author 位）丢弃。
                                AddressableMessage {
                                    author: None,
                                    sent_timestamp: Some(1_699_999_000_100),
                                },
                            ],
                        }],
                        ..sync_message::DeleteForMe::default()
                    },
                )),
                ..SyncMessage::default()
            }),
            metadata.clone(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "deleteForMe projects",
        )["params"]["envelope"]
            .clone();
        let face = &envelope["syncMessage"]["deleteForMe"];
        let groups = face["messageDeletes"].as_array().expect("groups");
        assert_eq!(groups.len(), 1, "坏作者条目所在组仍有有效成员");
        assert_eq!(
            groups[0]["conversationId"],
            json!(base64::engine::general_purpose::STANDARD.encode([7u8; 32]))
        );
        assert_eq!(
            groups[0]["messages"],
            json!([{ "author": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "timestamp": 1_699_999_000_000_u64 }])
        );

        // 全坏（无 messageDeletes）→ 整信封丢弃。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe::default(),
                )),
                ..SyncMessage::default()
            }),
            Metadata {
                sender: ServiceId::parse_from_service_id_string(
                    "11111111-2222-4333-8444-555555555555",
                )
                .unwrap(),
                ..metadata
            },
        );
        assert!(project_content("acct", &store, &content, &registry)
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P15a 项1 deleteForMe 整会话清除族收包投影：conversationDeletes（
    /// isFullDelete 布尔/mostRecentMessages 清空整组丢/mostRecentNonExpiring
    /// Messages 空数组照投）、localOnlyConversationDeletes（conversation
    /// 必需）、多臂同投、全坏整信封丢。
    #[tokio::test]
    async fn sync_projects_delete_for_me_conversation_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::sync_message::delete_for_me::{
            ConversationDelete, LocalOnlyConversationDelete,
        };
        use presage::proto::{
            sync_message, AddressableMessage, ConversationIdentifier, SyncMessage,
        };

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-dfm-conversations-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sender =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("sender aci");
        let metadata = Metadata {
            sender,
            destination: sender,
            sender_device: presage::libsignal_service::protocol::DeviceId::new(1)
                .expect("device id"),
            pni_verified: None,
            client_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_000)
                .expect("timestamp"),
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_000_001)
                .expect("timestamp"),
            needs_receipt: false,
            unidentified_sender: false,
            was_plaintext: false,
            server_guid: None,
            story: None,
            ephemeral: None,
            urgent: None,
            report_spam_token: None,
        };
        let aci = |text: &str| {
            ServiceId::parse_from_service_id_string(text)
                .unwrap()
                .raw_uuid()
                .as_bytes()
                .to_vec()
        };
        let author = |bytes: Vec<u8>, ts: u64| AddressableMessage {
            author: Some(presage::proto::addressable_message::Author::AuthorServiceIdBinary(bytes)),
            sent_timestamp: Some(ts),
        };
        let conversation = |bytes: Vec<u8>| ConversationIdentifier {
            identifier: Some(
                presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes),
            ),
        };
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe {
                        conversation_deletes: vec![
                            ConversationDelete {
                                conversation: Some(conversation(aci(
                                    "9d0e5f1a-2b3c-4d5e-8f60-7a8b9c0d1e2f",
                                ))),
                                most_recent_messages: vec![
                                    author(
                                        aci("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"),
                                        1_699_999_000_000,
                                    ),
                                    // 坏作者条目丢弃，组内仍有有效成员。
                                    AddressableMessage {
                                        author: None,
                                        sent_timestamp: Some(1_699_999_000_100),
                                    },
                                ],
                                is_full_delete: Some(true),
                                most_recent_non_expiring_messages: vec![],
                            },
                            // mostRecentMessages 清空 → 整组丢（官方同口径）。
                            ConversationDelete {
                                conversation: Some(conversation(aci(
                                    "0b1c2d3e-4f50-4a1b-8c2d-3e4f50617283",
                                ))),
                                most_recent_messages: vec![],
                                is_full_delete: Some(false),
                                most_recent_non_expiring_messages: vec![],
                            },
                        ],
                        local_only_conversation_deletes: vec![
                            LocalOnlyConversationDelete {
                                conversation: Some(conversation(aci(
                                    "7c8d9e0f-1a2b-4c3d-9e4f-5a6b7c8d9e0f",
                                ))),
                            },
                            // conversation 缺席 → 丢（官方 No target
                            // conversation 同口径）。
                            LocalOnlyConversationDelete { conversation: None },
                        ],
                        ..sync_message::DeleteForMe::default()
                    },
                )),
                ..SyncMessage::default()
            }),
            metadata.clone(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry).await,
            "deleteForMe conversations project",
        )["params"]["envelope"]
            .clone();
        let face = &envelope["syncMessage"]["deleteForMe"];
        let deletes = face["conversationDeletes"].as_array().expect("deletes");
        assert_eq!(deletes.len(), 1, "mostRecentMessages 清空组丢弃");
        assert_eq!(
            deletes[0]["conversationId"],
            json!("9d0e5f1a-2b3c-4d5e-8f60-7a8b9c0d1e2f")
        );
        assert_eq!(deletes[0]["isFullDelete"], json!(true));
        assert_eq!(
            deletes[0]["mostRecentMessages"],
            json!([{ "author": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "timestamp": 1_699_999_000_000_u64 }])
        );
        assert_eq!(deletes[0]["mostRecentNonExpiringMessages"], json!([]));
        let local_only = face["localOnlyConversationDeletes"]
            .as_array()
            .expect("local only");
        assert_eq!(local_only.len(), 1, "conversation 缺席条目丢弃");
        assert_eq!(
            local_only[0]["conversationId"],
            json!("7c8d9e0f-1a2b-4c3d-9e4f-5a6b7c8d9e0f")
        );
        assert!(face.get("messageDeletes").is_none(), "缺席臂键不在");

        // 全坏（清除族条目全部无效）→ 整信封丢弃。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe {
                        local_only_conversation_deletes: vec![LocalOnlyConversationDelete {
                            conversation: None,
                        }],
                        ..sync_message::DeleteForMe::default()
                    },
                )),
                ..SyncMessage::default()
            }),
            metadata,
        );
        assert!(project_content("acct", &store, &content, &registry)
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// listGroups 单群读过滤判定（P15b 审计项5）：filter 缺席全放行（全表
    /// 旧行为）；在场 = 逐字节命中任一 id 才放行，未命中掉出（结果可空）。
    #[test]
    fn group_key_filter_semantics() {
        let a = vec![0x51u8; 32];
        let b = vec![0x52u8; 32];
        let both = vec![a.clone(), b.clone()];
        assert!(group_key_passes_filter(&a, None), "缺席 = 全放行");
        assert!(group_key_passes_filter(&b, None), "缺席 = 全放行");
        assert!(
            group_key_passes_filter(&b, Some(&both)),
            "命中任一 id 即放行"
        );
        assert!(
            !group_key_passes_filter(&b, Some(std::slice::from_ref(&a))),
            "未命中掉出结果"
        );
        assert!(
            !group_key_passes_filter(&[0x53u8; 32], Some(&both)),
            "store 实有但不在过滤集 → 掉出"
        );
    }
}
