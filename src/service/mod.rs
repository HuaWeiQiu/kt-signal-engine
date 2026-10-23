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
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

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
use presage_store_sqlite::{SqliteConnectOptions, SqliteStore};
use rand::distr::{Alphanumeric, SampleString};
use rand::rng;
use rand::Rng;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify};

use crate::state::{AccountEntry, EngineState, ACCOUNTS_DIR, QUARANTINE_DIR, QUARANTINE_LOG};

/// A1 per-account 观测面（稳定性批 2026-10-20）：接收在飞/延迟分位/建流
/// 结局/崩溃计数，经既有 `engine.perfMetrics` 快照 additive 暴露。
pub(crate) mod account_metrics;
pub(crate) mod avatar;
pub(crate) mod backup_task;
pub(crate) mod call_links;
/// 通话中继配置（calls.relays）：认证 GET /v2/calling/relays + 官方同口径
/// TTL 缓存（模块头注载形状/纪律）。
pub(crate) mod calling_relays;
pub(crate) mod capabilities;
pub(crate) mod cdn_upload;
pub(crate) mod delivery_receipt;
pub(crate) mod devices;
/// R1 群发本地缓存面：GSE 按群落 kv（官方 2h-2d 窗 + 成员面失效）+ 发送
/// 面 terminated 探针缓存。稳态群发 0 次额外 HTTPS。
pub(crate) mod group_cache;
pub(crate) mod group_link;
pub(crate) mod groups;
pub(crate) mod identity;
/// stdout 出站通道有界化（稳定性批）：全局 writer 队列 + 每账号背压队列
/// （模块头注载容量/公平/死亡语义）。
pub(crate) mod outbound;
/// 本机账号 profile capabilities 读面（回填能力门数据面，2026-10-20 批，
/// wire additive；模块头注载来源/隐私红线/缓存与预算纪律）。
pub(crate) mod own_capabilities;
/// S2a 性能基线打点层（只测不改）：进程内有界指标注册表 + `engine.perfMetrics`
/// 只读投影。零 stderr、零行为语义、词表封闭（模块头注载纪律）。
pub(crate) mod perf_metrics;
/// 按手机号查找面（phoneLookup，KT 扩展方法，CDSI）：模块头注载语义/
/// 纪律（结果只投影即弃，不落库）。
pub(crate) mod phone;
pub(crate) mod prekey;
pub(crate) mod profile;
/// 联系人 profile 名字材料刷新读路径（profiles.refreshContacts + 登录期
/// 自动批，2026-10-16 批，模块头注载语义/纪律/落表查证结论）。
pub(crate) mod profile_refresh;
pub(crate) mod report;
/// 启动预热批（send 性能批 2026-10-16，模块头注载契约纪律：零真实消息、
/// 不阻塞真实发送、有界并发/时限、失败静默 perf 计数）。
pub(crate) mod send_warmup;
pub(crate) mod snapshot_import;
pub(crate) mod stickers;
pub(crate) mod storage_call_link;
pub(crate) mod storage_merge;
pub(crate) mod storage_records;
pub(crate) mod storage_sticker;
pub(crate) mod storage_sync;
/// F 链路入站观测面（模块头注载语义/帽/取舍纪律）：Storage 合并驱动的会话
/// 三 flag 变更差分簿 + `receive.conversationState` 通知构造（词表第五词）。
pub(crate) mod storage_watch;
pub(crate) mod storage_write;
pub(crate) mod stories;
/// 每账号接收管线 supervisor（稳定性批 2026-10-20，审计 S4/A2）：panic
/// 隔离 + 账号级 Fibonacci+jitter 重启退避（模块头注载官方锚点/纪律）。
pub(crate) mod supervision;
pub(crate) mod username;
/// 出向语音笔记波形+时长生成（send `;voice=true` 面，2026-10-14 批；
/// 官方 AttachmentPointer.audioWaveform/audioDurationSeconds 算法对齐，
/// 模块头注载 wire 形状/失败姿态/解码覆盖矩阵）。
pub(crate) mod voice_note;
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

/// 媒体 story caption 字节上限（2026-10-15 批）：官方 story composer
/// caption 输入帽 `MAX_BODY_ATTACHMENT_BYTE_LENGTH = 64 * KIBIBYTE`
///（Signal-Desktop ts/util/longAttachment.std.ts:8，CompositionInput.dom.tsx
/// :686-692 `Buffer.byteLength(text) > 64 KiB` 裁决同源）——字节面非字符面
///（Rust `str.len()` 即 UTF-8 字节，previewTitle ≤1024 字节纪律同构），
/// 超出整拒不静默截断。
pub(crate) const STORY_CAPTION_MAX_BYTES: usize = 64 * 1024;

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

/// sendCallEvent 参数（通话事件出向同步批，KT 扩展方法）：官方桌面
/// updateRemoteCallHistory/getDeleteCallEvent 同构——本机通话事件（接听/
/// 挂断/未接/删除/observed）即时向自有设备发 SyncMessage.callEvent=19，
/// 各设备会话据此落通话记录条。词表镜像入向 `syncMessage.callEvent` 投影
///（§3 receive 行）互逆：type ∈ audio|video|group|ad-hoc、direction ∈
/// incoming|outgoing、event ∈ accepted|not-accepted|delete|observed
///（官方 statusToProto 非空位才同步——missed/ringing/pending 等中间态
/// 不出线，UNKNOWN 词不作发送意图）。conversationId 形态随 type：直聊 =
/// 号码/service-id 双形态（resolve_recipient 同链）→ 16B UUID 字节；群 =
/// 32B master key 标准 base64（官方 getBytesForPeerId fromBase64 同位）；
/// ad-hoc = roomId 小写 hex（官方 fromHex 同位，与入向投影 hex_encode
/// 互逆）。callId 十进制串（官方 longToString 同构，u64 域）：直聊/群必带
///（官方「For other calls, we expect a valid callId」同位）；ad-hoc 可缺
///（官方 call-link 占位记录「only send the timestamp」同臂）。timestamp
/// 缺省 now（官方调用方 Date.now() 事件时刻同位）。
pub(crate) struct SendCallEvent {
    pub(crate) kind: presage::proto::sync_message::call_event::Type,
    pub(crate) direction: presage::proto::sync_message::call_event::Direction,
    pub(crate) outcome: presage::proto::sync_message::call_event::Event,
    pub(crate) conversation: CallEventConversation,
    pub(crate) call_id: Option<u64>,
    pub(crate) timestamp: Option<u64>,
}

/// sendCallEvent 会话腿三形：直聊腿存原始寻址串（号码/service-id，服务层
/// 开管理器后按 store 解析）；群/ad-hoc 腿参数层即解字节（形状错参数层
/// 拒，不触服务层）。
pub(crate) enum CallEventConversation {
    Direct(String),
    Group(Vec<u8>),
    AdHoc(Vec<u8>),
}

/// sendCallEvent 参数解析闸（fail-closed -32603 → None）：词表三面 +
/// callId 十进制 u64（非空全数字；负号/小数/hex/超 u64 全拒——官方
/// BigInt 十进制串同域）+ 直聊/群腿 callId 必带 + timestamp 正整数
///（0/负/小数拒）+ 群 32B base64 / ad-hoc 小写 hex（hex_decode 小写纪律
/// 同源）。conversationId 空串/非字符串整拒。
pub(crate) fn parse_call_event(params: &serde_json::Map<String, Value>) -> Option<SendCallEvent> {
    use presage::proto::sync_message::call_event::{Direction, Event, Type};
    let kind = match params.get("type")?.as_str()? {
        "audio" => Type::AudioCall,
        "video" => Type::VideoCall,
        "group" => Type::GroupCall,
        "ad-hoc" => Type::AdHocCall,
        _ => return None,
    };
    let direction = match params.get("direction")?.as_str()? {
        "incoming" => Direction::Incoming,
        "outgoing" => Direction::Outgoing,
        _ => return None,
    };
    let outcome = match params.get("event")?.as_str()? {
        "accepted" => Event::Accepted,
        "not-accepted" => Event::NotAccepted,
        "delete" => Event::Delete,
        "observed" => Event::Observed,
        _ => return None,
    };
    let conversation_id = params.get("conversationId")?.as_str()?;
    let conversation = match kind {
        Type::GroupCall => CallEventConversation::Group(decode_group_id(conversation_id)?),
        Type::AdHocCall => {
            let bytes = hex_decode(conversation_id)?;
            (!bytes.is_empty()).then_some(CallEventConversation::AdHoc(bytes))?
        }
        _ => {
            let trimmed = conversation_id.trim();
            (!trimmed.is_empty()).then_some(CallEventConversation::Direct(trimmed.to_owned()))?
        }
    };
    let call_id = match params.get("callId") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(parse_u64_decimal(text)?),
        Some(_) => return None,
    };
    // 直聊/群腿 callId 必带（官方 getCallIdForProto 非 ad-hoc 腿严格面）。
    if call_id.is_none() && kind != Type::AdHocCall {
        return None;
    }
    let timestamp = match params.get("timestamp") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let millis = value.as_u64()?;
            Some((millis > 0).then_some(millis)?)
        }
    };
    Some(SendCallEvent {
        kind,
        direction,
        outcome,
        conversation,
        call_id,
        timestamp,
    })
}

/// 十进制串 → u64（官方 longToString 逆运算同域）：非空、全 ASCII 数字、
/// u64 容量内；"0" 合法（proto uint64 域），"+1"/"-1"/" 1"/"0x1"/"1.0"/
/// 空串全拒。
fn parse_u64_decimal(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// sendCallEvent CallEvent proto 构造单点（官方 getProtoForCallHistory
/// 同位）：六字段全显式（conversationId 字节/callId/timestamp/type/
/// direction/event），语义与入向投影 project_call_event 严格互逆
///（单测往返钉定）；conversation_bytes 由调用方解析回填（直聊腿 =
/// resolve_recipient → 16B UUID 字节，群/ad-hoc 腿 = 参数层已解字节）。
fn build_call_event(
    event: &SendCallEvent,
    conversation_bytes: Vec<u8>,
) -> presage::proto::sync_message::CallEvent {
    presage::proto::sync_message::CallEvent {
        conversation_id: Some(conversation_bytes),
        call_id: event.call_id,
        timestamp: event.timestamp,
        r#type: Some(event.kind as i32),
        direction: Some(event.direction as i32),
        event: Some(event.outcome as i32),
    }
}

/// sendContact 名片参数（窗D B11，官方 EmbeddedContact/DataMessage.Contact
/// 同构，SendMessage.preload.ts:418-492 toProto contact 段同位）：name 六
/// 子字段、number/email 条目 {value, type?, label?}、address 九子字段条目、
/// avatar data URI 单条（官方 EmbeddedContactWithUploadedAvatar 上传面同构
/// ——上传后取回指针进 Contact.Avatar）、organization。type 为词表词
///（methods.rs 解析闸钉定：number/email 四值、address 三值），此处透传。
#[derive(Clone)]
pub(crate) struct SendContact {
    pub(crate) name: Option<SendContactName>,
    pub(crate) number: Vec<SendContactField>,
    pub(crate) email: Vec<SendContactField>,
    pub(crate) address: Vec<SendContactAddress>,
    pub(crate) avatar: Option<String>,
    pub(crate) organization: Option<String>,
}

/// sendContact name 子字段（官方 Contact.Name 六位同构，displayName 保留
/// 位无官方语义不收）。
#[derive(Clone)]
pub(crate) struct SendContactName {
    pub(crate) given_name: Option<String>,
    pub(crate) family_name: Option<String>,
    pub(crate) prefix: Option<String>,
    pub(crate) suffix: Option<String>,
    pub(crate) middle_name: Option<String>,
    pub(crate) nickname: Option<String>,
}

/// sendContact number/email 条目：value 必填非空（官方发送面 `!value →
/// undefined` 丢弃口径的引擎侧前置闸），type 为词表词或缺省、label 透传。
#[derive(Clone)]
pub(crate) struct SendContactField {
    pub(crate) value: String,
    pub(crate) r#type: Option<String>,
    pub(crate) label: Option<String>,
}

/// sendContact address 条目（官方 Contact.PostalAddress 九位同构；type 词
/// 表三值，无 MOBILE——postal 枚举本无此值）。
#[derive(Clone)]
pub(crate) struct SendContactAddress {
    pub(crate) r#type: Option<String>,
    pub(crate) label: Option<String>,
    pub(crate) street: Option<String>,
    pub(crate) pobox: Option<String>,
    pub(crate) neighborhood: Option<String>,
    pub(crate) city: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) postcode: Option<String>,
    pub(crate) country: Option<String>,
}

/// sendContact 参数解析（fail-closed，词表外 -32603）：contact 缺席/非对象、
/// name 在场非对象/子字段非字符串、number/email/address 在场非数组/条目非
/// 对象/value 缺席·非字符串·空串/type 词表外/label·address 子字段非字符
/// 串、organization 非字符串、avatar 非字符串全拒。词表外引擎拒绝（官方
/// numberTo*Type 词表外静默归 HOME——textStyle 先例同纪律，§5 偏差记档）；
/// organization 空串 = 官方 `if (organization)` falsy 同位归缺席。
pub(crate) fn parse_send_contact(value: Option<&Value>) -> Option<SendContact> {
    let value = value?;
    if !value.is_object() {
        return None;
    }
    let name = match value.get("name") {
        None | Some(Value::Null) => None,
        Some(name) => {
            let field = |key: &str| -> Option<Option<String>> {
                match name.get(key) {
                    None | Some(Value::Null) => Some(None),
                    Some(Value::String(text)) => Some(Some(text.clone())),
                    Some(_) => None,
                }
            };
            if !name.is_object() {
                return None;
            }
            Some(SendContactName {
                given_name: field("givenName")?,
                family_name: field("familyName")?,
                prefix: field("prefix")?,
                suffix: field("suffix")?,
                middle_name: field("middleName")?,
                nickname: field("nickname")?,
            })
        }
    };
    // number/email 同构条目族（官方两枚举同编号同词表，解析单点）。
    let field_entries = |key: &str| -> Option<Vec<SendContactField>> {
        match value.get(key) {
            None | Some(Value::Null) => Some(Vec::new()),
            Some(Value::Array(entries)) => entries
                .iter()
                .map(|entry| {
                    if !entry.is_object() {
                        return None;
                    }
                    let entry_value = match entry.get("value") {
                        Some(Value::String(text)) if !text.is_empty() => text.clone(),
                        _ => return None,
                    };
                    let entry_type = match entry.get("type") {
                        None | Some(Value::Null) => None,
                        Some(Value::String(word))
                            if matches!(word.as_str(), "HOME" | "MOBILE" | "WORK" | "CUSTOM") =>
                        {
                            Some(word.clone())
                        }
                        _ => return None,
                    };
                    let label = match entry.get("label") {
                        None | Some(Value::Null) => None,
                        Some(Value::String(text)) => Some(text.clone()),
                        Some(_) => return None,
                    };
                    Some(SendContactField {
                        value: entry_value,
                        r#type: entry_type,
                        label,
                    })
                })
                .collect(),
            Some(_) => None,
        }
    };
    let address = match value.get("address") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(entries)) => entries
            .iter()
            .map(|entry| {
                let text = |key: &str| -> Option<Option<String>> {
                    match entry.get(key) {
                        None | Some(Value::Null) => Some(None),
                        Some(Value::String(text)) => Some(Some(text.clone())),
                        Some(_) => None,
                    }
                };
                if !entry.is_object() {
                    return None;
                }
                let entry_type = match entry.get("type") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(word))
                        if matches!(word.as_str(), "HOME" | "WORK" | "CUSTOM") =>
                    {
                        Some(word.clone())
                    }
                    _ => return None,
                };
                Some(SendContactAddress {
                    r#type: entry_type,
                    label: text("label")?,
                    street: text("street")?,
                    pobox: text("pobox")?,
                    neighborhood: text("neighborhood")?,
                    city: text("city")?,
                    region: text("region")?,
                    postcode: text("postcode")?,
                    country: text("country")?,
                })
            })
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    let organization = match value.get("organization") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) if !text.is_empty() => Some(text.clone()),
        Some(Value::String(_)) => None,
        Some(_) => return None,
    };
    let avatar = match value.get("avatar") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(_) => return None,
    };
    Some(SendContact {
        name,
        number: field_entries("number")?,
        email: field_entries("email")?,
        address,
        avatar,
        organization,
    })
}

/// 附件条数上限：与接收投影 §4.13/compat receive 32 条同口径，超出
/// fail-closed。
const ATTACHMENT_MAX_COUNT: usize = 32;

/// 单附件字节上限：官方桌面 100MB CDN 上限同源，超出 fail-closed。
const ATTACHMENT_MAX_BYTES: usize = 100_000_000;

struct ParsedAttachment {
    spec: presage::libsignal_service::sender::AttachmentSpec,
    bytes: Vec<u8>,
    /// `;gif=true` 出向 GIF 旗标——钉定 presage AttachmentSpec 无 gif 位，
    /// 不能走 spec 面；由 upload_attachments_for_send 在上传后按本位对返回
    /// 指针置 Flags.GIF（官方消息组装期置位同构，见 apply_outgoing_gif_flag）。
    is_gif: bool,
}

/// 出向 GIF 旗标单点：官方 GIF 消息 = 普通附件 DataMessage.attachments[].flags
/// 带 Flags.GIF=8 位（接收投影 isGif 同位读回，Attachment.std.ts 收发同位）。
/// flags 是客户端线上陈述，CDN 上行面零涉及——上传返回指针直接置位与官方
/// 消息组装期置位同构；与既有位（voice/borderless）按位掩码叠加，重复置位
/// 幂等。
fn apply_outgoing_gif_flag(pointer: &mut presage::proto::AttachmentPointer) {
    use presage::proto::attachment_pointer::Flags;
    pointer.flags = Some(pointer.flags.unwrap_or(0) | Flags::Gif as u32);
}

/// 出向语音元数据置位单点：官方 Android 发送侧生成
/// （GenerateAudioWaveFormJob → `AttachmentPointer.audioWaveform=21`/
/// `audioDurationSeconds=22`，vendored proto 已同步两位），上传返回指针
/// 置位与官方消息组装期置位同构（GIF 旗标先例同位）。生成失败 = 两字段
/// 全不带照发（官方 catch-all 姿态，wire 恒成对不产半对形状）。
fn apply_outgoing_voice_metadata(
    pointer: &mut presage::proto::AttachmentPointer,
    metadata: &voice_note::VoiceAudioMetadata,
) {
    pointer.audio_waveform = Some(metadata.waveform.clone());
    pointer.audio_duration_seconds = Some(metadata.duration_seconds);
}

/// 出向 caption 置位单点（2026-10-15 媒体 story caption 批）：caption 走
/// presage spec 上行面直通——vendored `AttachmentSpec.caption`（官方
/// `MAX_BODY_ATTACHMENT_BYTE_LENGTH` 帽闸在 methods 参数面）由
/// `upload_attachment` 原样落返回指针 `AttachmentPointer.caption`
///（vendored sender.rs spec.caption 直通；官方 uploadAttachment
/// `caption || null` 进指针同构，uploadAttachment.preload.ts:99/:134），
/// 零 vendor/fork 改动。仅 story 媒体腿传 Some（entries 恒单条，置位
/// 首条正文附件），send/preview 面传 None 零变化；空 parsed 批 = 编排面
/// 不会发生（媒体腿恒单条先行置位前已非空闸）。
fn apply_outgoing_caption(parsed: &mut [ParsedAttachment], caption: Option<&str>) {
    if let Some(text) = caption {
        if let Some(first) = parsed.first_mut() {
            first.spec.caption = Some(text.to_owned());
        }
    }
}

/// send 附件条目解析：data URI 唯一形状 = connector 上游形状
/// `data:<mediatype>[;filename=<percent-encoded>][;voice=true][;gif=true];base64,<payload>`
/// （RFC 2397 扩展 filename 参数同款；voice 参数映射官方 VOICE_MESSAGE
/// flags，presage voice_note→AttachmentPointer.flags 官方同构；gif 参数映射
/// 官方 Flags.GIF=8 位，参数面无 mediatype 闸——官方 Tenor GIF 即
/// video/mp4 载体带 GIF 位，媒体类自判归消费方，收投影 isGif 同口径）。
/// mediatype 缺省 application/octet-stream；非 base64 / 坏 percent / 坏
/// base64 / 坏 voice/gif 值 / 超上限一律 None（fail-closed）。
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
    let mut is_gif = false;
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
        } else if param.eq_ignore_ascii_case("gif") {
            is_gif = true;
        } else if let Some(value) = param.strip_prefix("gif=") {
            match value {
                "true" => is_gif = true,
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
        is_gif,
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

/// finishLink 成功应答投影：号码 + 账号自有 ACI（wire additive，connector
/// 契约 1.142 消费面）。`aci` 是可选键的取值位：None = wire 整键缺席（不
/// 出 null）——fake 面、并发幂等臂、store 不可读均诚实落 None。
pub(crate) struct LinkedAccount {
    pub(crate) number: String,
    pub(crate) aci: Option<String>,
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
    RateLimited {
        // 服务端在 429/413 应答里给出的等待时长（秒）。上游
        // ServiceError::RateLimitExceeded { retry_after } 本来带着它，之前用
        // { .. } 丢弃，导致桌面只能按固定 60 秒重试（官方
        // sleepForRateLimitRetryAfterTime 用的是服务端值）。
        retry_after_secs: Option<u64>,
    },
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
    /// WS 请求被服务器明确拒绝（508 request rejected/499 客户端过旧/send
    /// 面 413 超限/404 未注册收件人/其余 4xx UnhandledResponseCode）：
    /// 服务器应答拒绝即未入队，确定未送达。同调用内快速重试 1 次；错误面
    /// -32603 + 固定原文（锚词 WsRejectedException；cause 类全覆盖批
    /// 2026-10-16，compat §3/§5）。
    WsRejected,
    /// WS 请求响应未达/等响应期断流（`ServiceError::Timeout`、等响应相
    /// `WsClosing`、`WsError`、presage Elapsed 超时）：请求可能已写入
    /// 服务器，结局未知——connector 契约「An unknown send outcome must
    /// not be retried automatically」同源纪律，**绝不自动重试**（可能已
    /// 在服务器落地，重试会重复投递）；错误面 -32603 + 固定原文（锚词
    /// WsTimeoutException）。
    WsTimeout,
    /// 请求写出前传输已断（`WsClosing{reason="WebSocket closing while
    /// sending request"}`——vendored websocket/mod.rs 写出相 reason 字面量
    /// 与等响应相区分；`ServiceError::SendError` 请求发送失败同相）：
    /// 请求未离开引擎，确定未送达。快速重试 1 次；错误面 -32603 + 固定
    /// 原文（锚词 WsTransportException）。
    WsTransport,
    /// prekey 拉取失败（send 链 404 取钥面 `ServiceError::NotFoundError`；
    /// 近似归类记档：vendored 错误不携带阶段信息，按 send 链 404 主导成因
    /// 归族，§5）：会话建立前置失败，先于 wire，确定未送达。快速重试 1
    /// 次；错误面 -32603 + 固定原文（锚词 PreKeyFetchException）。
    PreKey,
    /// 会话建立/加密面失败（vendored 409/410 修复环耗尽
    /// MaximumRetriesLimitExceeded、非 UntrustedIdentity 的协议错误、
    /// Curve/私钥面、MismatchedDevices/StaleDevices 漏出）：全部先于
    /// wire，确定未送达。快速重试 1 次；错误面 -32603 + 固定原文（锚词
    /// SessionBuildException）。
    SessionBuild,
    /// 附件/贴纸图/预览图上行失败（`AttachmentUploadError` 外层 sender
    /// 构建与内层逐条两面，`upload_error_of` 单点归型）：上传先于任何
    /// 收件人发送，消息确定未投递（上传请求自身结局歧义不构成重复投递
    /// 风险）。快速重试 1 次；错误面 -32603 + 固定原文（锚词
    /// AttachmentUploadException）。
    AttachmentUpload,
    /// 群腿零收件人（群成员过滤自身后目标集为空——独群/全成员即自身的
    /// 群态）：请求从未构造收件人面，确定未送达（2026-10-16 群腿零收件人
    /// 防御批，杜绝 vendored 空循环 → Ok 空 outcomes → send 应答整体成功
    /// 的真空成功洞，三层防御点见 group_mrm/dispatch/failover，compat
    /// §3/§5）。错误面 -32603 + 固定原文（锚词 NoRecipientsException）。
    /// 群腿无整体自动重试（既有裁定），is_definite_failure=true 只补结局
    /// 类完备性（语义 = 确定未送达）。
    NoRecipients,
    /// 群态取回解码类失败重试耗尽（接收完整性批②：群 update/sync 信封
    /// 触发的群 fetch/decrypt 在 vendored 重试环内重试 1 次后仍败，结构化
    /// 上抛不再静默回退旧群态；发送链该面已 fail-open 兜底、此臂为
    /// getUserStatus 等旁路触点的归型完备臂）。发送面 -1 +
    /// `GROUP_SYNC_FAILED` + `data.count`（尝试次数，additive，compat
    /// §3）；不进 is_definite_failure——群态失败不自动重试，决策归
    /// connector/desktop。
    GroupSyncFailed {
        count: u32,
    },
    Internal,
}

impl SendError {
    /// 结局类判定（§5 快速重试硬边界，纯函数）：`true` = 确定未送达——
    /// 请求未离开引擎（WsTransport）、服务器显式拒绝未入队（WsRejected）、
    /// 先于 wire 的本地面失败（PreKey/SessionBuild）、消息未离站
    /// （AttachmentUpload）、从未构造收件人面（NoRecipients）——允许同
    /// 调用内快速重试 1 次；`false` = 未知
    /// 结局或确定性失败，绝不自动重试（WsTimeout 响应缺失可能已落地、
    /// Unbound/RateLimited/ProofRequired/IdentityUnverified 确定性失败、
    /// GroupSyncFailed 群态失败重试决策归调用方、
    /// ServerUnavailable 决策归 connector/desktop、Internal 本地面）。
    pub(crate) fn is_definite_failure(&self) -> bool {
        matches!(
            self,
            SendError::WsRejected
                | SendError::WsTransport
                | SendError::PreKey
                | SendError::SessionBuild
                | SendError::AttachmentUpload
                | SendError::NoRecipients
        )
    }
}

/// presage 发送/上行面错误归型：服务端 401/403（凭据吊销/解绑）同归
/// `SendError::Unbound`——直接 ServiceError 面（whoami 等推送服务调用）
/// 与 MessageSenderError 面（send_message 内 sender 上行同源再包一层）
/// 两处归型点都覆盖；限流（429/413 → RateLimited）、人机验证（428 →
/// ProofRequired）、服务端 5xx 不可用（→ ServerUnavailable）同以
/// ServiceError 面单点归型（compat §3 结构化三分类）；cause 类全覆盖批
/// （2026-10-16，§5）再扩六族：服务器明确拒绝（4xx/499/508/send 面
/// 413/404 未注册）→ WsRejected、响应未知结局（Timeout/等响应相
/// WsClosing/WsError/Elapsed）→ WsTimeout、写出前断（写出相 WsClosing
/// reason 字面量/ServiceError::SendError）→ WsTransport、取钥 404 →
/// PreKey、修复环耗尽/协议面 → SessionBuild、附件上行 → AttachmentUpload
/// （`upload_error_of`）；其余（网络/协议/存储本地失败）折叠
/// `SendError::Internal`——折叠臂同点落变体路径探针日志（见
/// `*_variant_face` 三 helper 注）。
/// send 链各触网点单点共用（getUserStatus 的 whoami 面同归型先行）。
pub(crate) fn send_error_of(error: presage::Error<<SqliteStore as Store>::Error>) -> SendError {
    use presage::libsignal_service::prelude::ServiceError;
    use presage::libsignal_service::sender::MessageSenderError;

    /// ServiceError 面单点归型（直接推送服务调用与 sender 再包层两触点
    /// 共用；vendored push_service/response.rs 归型路径 + websocket/mod.rs
    /// 写出相/等响应相 WsClosing reason 字面量区分）。`presage_path` =
    /// 调用侧 presage 外层路径字面量（Internal 探针 face 前缀）。
    fn service_error_face(error: ServiceError, presage_path: &'static str) -> SendError {
        match error {
            ServiceError::Unauthorized => SendError::Unbound,
            ServiceError::RateLimitExceeded { retry_after } => SendError::RateLimited {
                retry_after_secs: retry_after.map(|d| d.num_seconds().max(0) as u64),
            },
            ServiceError::ProofRequiredError(..) => SendError::ProofRequired,
            ServiceError::UnhandledResponseCode { status, .. } if status.as_u16() >= 500 => {
                SendError::ServerUnavailable
            }
            // —— cause 类全覆盖批（2026-10-16，§5）：服务器显式应答拒绝
            // 族（应答即未入队，确定未送达）——
            ServiceError::UnhandledResponseCode { .. } => SendError::WsRejected,
            ServiceError::ServerRejected | ServiceError::DeprecatedVersion => SendError::WsRejected,
            ServiceError::MessageTooLarge | ServiceError::UnregisteredRecipient => {
                SendError::WsRejected
            }
            // 响应未知结局族（请求可能已写入，绝不自动重试）。
            ServiceError::Timeout { .. } => SendError::WsTimeout,
            // 写出相 reason 字面量（vendored websocket/mod.rs）= 请求未
            // 写出即断，确定未送达；其余 WsClosing = 等响应相，未知结局。
            ServiceError::WsClosing {
                reason: "WebSocket closing while sending request",
            } => SendError::WsTransport,
            ServiceError::WsClosing { .. } | ServiceError::WsError(_) => SendError::WsTimeout,
            ServiceError::SendError { .. } => SendError::WsTransport,
            // 会话前置取钥 404（近似归类记档 §5）。
            ServiceError::NotFoundError => SendError::PreKey,
            // 会话/加密面（先于 wire）。
            ServiceError::MismatchedDevicesException(_) | ServiceError::StaleDevices(_) => {
                SendError::SessionBuild
            }
            ServiceError::SignalProtocolError(_) => SendError::SessionBuild,
            ServiceError::Curve(_) => SendError::SessionBuild,
            other => {
                // Internal 探针（2026-10-16 备忘录 Internal 批）：变体路径
                // 字面量落日志（path + face 拼读 = 完整类型路径），零内容。
                tracing::warn!(
                    path = presage_path,
                    face = service_error_variant_face(&other),
                    "send error folded internal",
                );
                SendError::Internal
            }
        }
    }

    match error {
        presage::Error::GroupAnnouncementsOnly => SendError::AnnouncementsOnly,
        presage::Error::MessageSenderError(sender_error) => match *sender_error {
            MessageSenderError::ServiceError(service_error) => {
                service_error_face(service_error, "PresageError::MessageSenderError::")
            }
            MessageSenderError::ProofRequired { .. } => SendError::ProofRequired,
            // P9 身份拒发闸：sender 对 process_prekey_bundle 失败统一再包
            // UntrustedIdentity（vendor sender.rs），协议层拒发（旧钥会话/
            // 换钥 bundle）落到此臂；ProtocolError 面的裸
            // SignalProtocolError::UntrustedIdentity 同因同归型。
            MessageSenderError::UntrustedIdentity { .. } => SendError::IdentityUnverified,
            MessageSenderError::ProtocolError(
                presage::libsignal_service::protocol::SignalProtocolError::UntrustedIdentity(_),
            ) => SendError::IdentityUnverified,
            // cause 类全覆盖批（§5）：404 未注册收件人（vendored 修复环
            // 归一面）确定未送达；修复环耗尽/协议/密钥面先于 wire；附件
            // 上行（sender 内层）消息未离站。
            MessageSenderError::NotFound { .. } => SendError::WsRejected,
            MessageSenderError::MaximumRetriesLimitExceeded => SendError::SessionBuild,
            MessageSenderError::ProtocolError(_) | MessageSenderError::InvalidPrivateKey(_) => {
                SendError::SessionBuild
            }
            MessageSenderError::AttachmentUploadError(_) => SendError::AttachmentUpload,
            other => {
                tracing::warn!(
                    face = message_sender_variant_face(&other),
                    "send error folded internal",
                );
                SendError::Internal
            }
        },
        presage::Error::ServiceError(service_error) => {
            service_error_face(service_error, "PresageError::")
        }
        presage::Error::ProtocolError(
            presage::libsignal_service::protocol::SignalProtocolError::UntrustedIdentity(_),
        ) => SendError::IdentityUnverified,
        presage::Error::ProtocolError(_) => SendError::SessionBuild,
        presage::Error::Timeout(_) => SendError::WsTimeout,
        // 群取回分型降级（接收完整性批②，compat §3）：解码类重试耗尽结构
        // 化上抛（count = 尝试次数）；404 群在服务器已消失 → 目标终止同面
        // （group send 的 terminated 硬拒族，vendored 发送链已 fail-open
        // 兜底，此臂覆盖 getUserStatus 等旁路触点）。
        presage::Error::GroupSyncFailed { attempts } => {
            SendError::GroupSyncFailed { count: attempts }
        }
        presage::Error::GroupGone => SendError::TargetTerminated,
        other => {
            tracing::warn!(
                face = presage_error_variant_face(&other),
                "send error folded internal",
            );
            SendError::Internal
        }
    }
}

/// 附件上行面错误归型（cause 类全覆盖批 2026-10-16，§5 新增单点）：内层
/// 逐条 `AttachmentUploadError`（此前被吞成 `Internal`）统一归
/// `SendError::AttachmentUpload`——上传先于任何收件人发送，消息确定未
/// 投递，上传请求自身的结局歧义（65s 控制面总超时内断流等）不构成重复
/// 投递风险；外层错误（sender 构建/凭据面）沿 `send_error_of` 原语义
/// （401 解绑等归型比笼统「上传失败」更诚实，且不进重试）。未匹配臂经
/// 委托折 Internal 时探针在 `send_error_of` 单点触发（本函数不重复落
/// 日志——归型单点即日志单点）。
pub(crate) fn upload_error_of(error: presage::Error<<SqliteStore as Store>::Error>) -> SendError {
    match error {
        presage::Error::MessageSenderError(boxed) => match *boxed {
            presage::libsignal_service::sender::MessageSenderError::AttachmentUploadError(_) => {
                SendError::AttachmentUpload
            }
            // 上行路径外层不该出现其他 sender 错误（防御臂）：仍按 send
            // 链归型面分类，未知形状折叠 Internal（不重试）。
            other => send_error_of(presage::Error::MessageSenderError(Box::new(other))),
        },
        _ => send_error_of(error),
    }
}

/// —— Internal 折叠臂变体路径探针（2026-10-16 备忘录 Internal 批）——
/// `SendError::Internal` 的每个构造臂（`send_error_of` 三处 fallback）在
/// 归型同时落一行变体路径字面量（warn 进 engine-stderr.log）：只走类型
/// 路径，payload（收件人 UUID/号码/reason 串/错误正文）绝不进日志——
/// 此前 "Internal error" 无锚词零归因（备忘录整夜 13 次失败实证）。三个
/// helper 逐变体穷尽映射字面量（vendored rev 钉定，上游加变体编译期
/// 报错；presage::Error 带 #[non_exhaustive] 走尾臂字面量兜底）。
fn service_error_variant_face(
    error: &presage::libsignal_service::prelude::ServiceError,
) -> &'static str {
    use presage::libsignal_service::prelude::ServiceError as Service;
    match error {
        Service::Timeout { .. } => "ServiceError::Timeout",
        Service::InvalidUrl(_) => "ServiceError::InvalidUrl",
        Service::InvalidAddressType(_) => "ServiceError::InvalidAddressType",
        Service::InvalidPhoneNumber => "ServiceError::InvalidPhoneNumber",
        Service::SendError { .. } => "ServiceError::SendError",
        Service::IO(_) => "ServiceError::IO",
        Service::JsonDecodeError(_) => "ServiceError::JsonDecodeError",
        Service::ProtobufDecodeError(_) => "ServiceError::ProtobufDecodeError",
        Service::BincodeError(_) => "ServiceError::BincodeError",
        Service::Base64DecodeError(_) => "ServiceError::Base64DecodeError",
        Service::RateLimitExceeded { .. } => "ServiceError::RateLimitExceeded",
        Service::Unauthorized => "ServiceError::Unauthorized",
        Service::Locked(_) => "ServiceError::Locked",
        Service::UnhandledResponseCode { .. } => "ServiceError::UnhandledResponseCode",
        Service::WsError(_) => "ServiceError::WsError",
        Service::WsClosing { .. } => "ServiceError::WsClosing",
        Service::Padding(_) => "ServiceError::Padding",
        Service::PaddingVersion(_) => "ServiceError::PaddingVersion",
        Service::InvalidFrame { .. } => "ServiceError::InvalidFrame",
        Service::MacError => "ServiceError::MacError",
        Service::SignalProtocolError(_) => "ServiceError::SignalProtocolError",
        Service::SealedSenderDecryptionError(_) => "ServiceError::SealedSenderDecryptionError",
        Service::InvalidDeviceId(_) => "ServiceError::InvalidDeviceId",
        Service::ProofRequiredError(_) => "ServiceError::ProofRequiredError",
        Service::MismatchedDevicesException(_) => "ServiceError::MismatchedDevicesException",
        Service::StaleDevices(_) => "ServiceError::StaleDevices",
        Service::CredentialsCacheError(_) => "ServiceError::CredentialsCacheError",
        Service::GroupsV2Error => "ServiceError::GroupsV2Error",
        Service::GroupsV2DecryptionError(_) => "ServiceError::GroupsV2DecryptionError",
        Service::ZkGroupDeserializationFailure(_) => "ServiceError::ZkGroupDeserializationFailure",
        Service::ZkGroupVerificationFailure(_) => "ServiceError::ZkGroupVerificationFailure",
        Service::UnsupportedContent => "ServiceError::UnsupportedContent",
        Service::NotFoundError => "ServiceError::NotFoundError",
        Service::DecryptDeviceInfoFieldError(_) => "ServiceError::DecryptDeviceInfoFieldError",
        Service::UnknownCdnVersion(_) => "ServiceError::UnknownCdnVersion",
        Service::DeviceLimitReached { .. } => "ServiceError::DeviceLimitReached",
        Service::DeprecatedVersion => "ServiceError::DeprecatedVersion",
        Service::ServerRejected => "ServiceError::ServerRejected",
        Service::UnregisteredRecipient => "ServiceError::UnregisteredRecipient",
        Service::MessageTooLarge => "ServiceError::MessageTooLarge",
        Service::MultiRecipientMismatchedDevices(_) => {
            "ServiceError::MultiRecipientMismatchedDevices"
        }
        Service::MultiRecipientStaleDevices(_) => "ServiceError::MultiRecipientStaleDevices",
        Service::InvalidDeviceVerificationCode => "ServiceError::InvalidDeviceVerificationCode",
        Service::DeviceCapabilityDowngrade => "ServiceError::DeviceCapabilityDowngrade",
        Service::NoSuchSession => "ServiceError::NoSuchSession",
        Service::InvalidVerificationSessionId => "ServiceError::InvalidVerificationSessionId",
        Service::TokenNotAccepted(_) => "ServiceError::TokenNotAccepted",
        Service::RegistrationSessionConflict(_) => "ServiceError::RegistrationSessionConflict",
        Service::VerificationSessionRateLimited { .. } => {
            "ServiceError::VerificationSessionRateLimited"
        }
        Service::InvalidTransportMode(_) => "ServiceError::InvalidTransportMode",
        Service::VerificationDeliveryFailed(_) => "ServiceError::VerificationDeliveryFailed",
        Service::DeviceTransferAvailable => "ServiceError::DeviceTransferAvailable",
        Service::UsernameHashNotSet => "ServiceError::UsernameHashNotSet",
        Service::AttachmentTooLarge => "ServiceError::AttachmentTooLarge",
        Service::ChallengeNotAccepted => "ServiceError::ChallengeNotAccepted",
        Service::Http(_) => "ServiceError::Http",
        Service::Curve(_) => "ServiceError::Curve",
        Service::Hpke(_) => "ServiceError::Hpke",
        Service::Fingerprint(_) => "ServiceError::Fingerprint",
        Service::CdsiLookupError(_) => "ServiceError::CdsiLookupError",
    }
}

/// MessageSenderError 变体路径字面量（探针专用，self-contained 全路径；
/// 仅 `send_error_of` 的 sender fallback 臂消费——该臂实际可达面为
/// InvalidDeviceId/SendSyncMessageError/NoMessagesToSend，其余变体字面量
/// 为穷尽性完备位）。
fn message_sender_variant_face(
    error: &presage::libsignal_service::sender::MessageSenderError,
) -> &'static str {
    use presage::libsignal_service::sender::MessageSenderError as Sender;
    match error {
        Sender::ServiceError(_) => "PresageError::MessageSenderError::ServiceError",
        Sender::ProtocolError(_) => "PresageError::MessageSenderError::ProtocolError",
        Sender::InvalidPrivateKey(_) => "PresageError::MessageSenderError::InvalidPrivateKey",
        Sender::InvalidDeviceId(_) => "PresageError::MessageSenderError::InvalidDeviceId",
        Sender::AttachmentUploadError(_) => {
            "PresageError::MessageSenderError::AttachmentUploadError"
        }
        Sender::SendSyncMessageError(_) => "PresageError::MessageSenderError::SendSyncMessageError",
        Sender::UntrustedIdentity { .. } => "PresageError::MessageSenderError::UntrustedIdentity",
        Sender::MaximumRetriesLimitExceeded => {
            "PresageError::MessageSenderError::MaximumRetriesLimitExceeded"
        }
        Sender::ProofRequired { .. } => "PresageError::MessageSenderError::ProofRequired",
        Sender::NotFound { .. } => "PresageError::MessageSenderError::NotFound",
        Sender::NoMessagesToSend => "PresageError::MessageSenderError::NoMessagesToSend",
    }
}

/// presage::Error 顶层变体路径字面量（探针专用；仅 `send_error_of` 外层
/// fallback 臂消费，#[non_exhaustive] 尾臂兜底）。
fn presage_error_variant_face(
    error: &presage::Error<<SqliteStore as Store>::Error>,
) -> &'static str {
    use presage::Error as Presage;
    match error {
        Presage::CaptchaRequired => "PresageError::CaptchaRequired",
        Presage::IoError(_) => "PresageError::IoError",
        Presage::JsonError(_) => "PresageError::JsonError",
        Presage::Base64Error(_) => "PresageError::Base64Error",
        Presage::TryFromSliceError(_) => "PresageError::TryFromSliceError",
        Presage::PhoneNumberError(_) => "PresageError::PhoneNumberError",
        Presage::UuidError(_) => "PresageError::UuidError",
        Presage::InvalidThread(_) => "PresageError::InvalidThread",
        Presage::ProtocolError(_) => "PresageError::ProtocolError",
        Presage::ServiceError(_) => "PresageError::ServiceError",
        Presage::ProfileManagerError(_) => "PresageError::ProfileManagerError",
        Presage::MessageSenderError(_) => "PresageError::MessageSenderError",
        Presage::AlreadyRegisteredError => "PresageError::AlreadyRegisteredError",
        Presage::NotYetRegisteredError => "PresageError::NotYetRegisteredError",
        Presage::ProvisioningError(_) => "PresageError::ProvisioningError",
        Presage::NoProvisioningMessageReceived => "PresageError::NoProvisioningMessageReceived",
        Presage::LinkingError => "PresageError::LinkingError",
        Presage::RelinkNecessary => "PresageError::RelinkNecessary",
        Presage::MissingKeyError(_) => "PresageError::MissingKeyError",
        Presage::MessagePipeNotStarted => "PresageError::MessagePipeNotStarted",
        Presage::MessagePipeInterruptedError => "PresageError::MessagePipeInterruptedError",
        Presage::ParseContactError(_) => "PresageError::ParseContactError",
        Presage::AttachmentCipherError(_) => "PresageError::AttachmentCipherError",
        Presage::UnknownGroup => "PresageError::UnknownGroup",
        Presage::GroupAnnouncementsOnly => "PresageError::GroupAnnouncementsOnly",
        Presage::UnknownRecipient => "PresageError::UnknownRecipient",
        Presage::Timeout(_) => "PresageError::Timeout",
        Presage::Store(_) => "PresageError::Store",
        Presage::PushChallengeRequired => "PresageError::PushChallengeRequired",
        Presage::RequestingCodeForbidden(_) => "PresageError::RequestingCodeForbidden",
        Presage::UnexpectedAttachmentChecksum => "PresageError::UnexpectedAttachmentChecksum",
        Presage::UnverifiedRegistrationSession => "PresageError::UnverifiedRegistrationSession",
        Presage::ProfileCipherError(_) => "PresageError::ProfileCipherError",
        Presage::NotPrimaryDevice => "PresageError::NotPrimaryDevice",
        Presage::UpdatePreKeyFailure => "PresageError::UpdatePreKeyFailure",
        Presage::InvalidDeviceId => "PresageError::InvalidDeviceId",
        Presage::InvalidUsername(_) => "PresageError::InvalidUsername",
        _ => "PresageError::Other",
    }
}

/// SendError 变体路径字面量（listGroups/listContacts 读面探针专用，
/// 2026-10-16 listGroups Internal 归因批）：dispatch 层读面 Err 折
/// -32603 前落一行变体路径（与 send_error_of 三 fallback 臂同风格——
/// 只走类型路径零 payload）。SendError 非 non_exhaustive，逐变体穷尽
/// 映射，上游加变体编译期报错。
pub(crate) fn send_error_variant_face(error: &SendError) -> &'static str {
    match error {
        SendError::UnknownAccount => "SendError::UnknownAccount",
        SendError::TargetTerminated => "SendError::TargetTerminated",
        SendError::AnnouncementsOnly => "SendError::AnnouncementsOnly",
        SendError::Unbound => "SendError::Unbound",
        SendError::RateLimited { .. } => "SendError::RateLimited",
        SendError::ProofRequired => "SendError::ProofRequired",
        SendError::ServerUnavailable => "SendError::ServerUnavailable",
        SendError::IdentityUnverified => "SendError::IdentityUnverified",
        SendError::WsRejected => "SendError::WsRejected",
        SendError::WsTimeout => "SendError::WsTimeout",
        SendError::WsTransport => "SendError::WsTransport",
        SendError::PreKey => "SendError::PreKey",
        SendError::SessionBuild => "SendError::SessionBuild",
        SendError::AttachmentUpload => "SendError::AttachmentUpload",
        SendError::NoRecipients => "SendError::NoRecipients",
        SendError::GroupSyncFailed { .. } => "SendError::GroupSyncFailed",
        SendError::Internal => "SendError::Internal",
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

/// P9 profileSharing 闸读面（S3，发送长尾修1 加会话级缓存）：Storage
/// Service 联系人记录 whitelist 位 map。account 作用域进程内存缓存，TTL 内
/// 直聊/群发送批零 Storage 网络读（活体 2026-10-17 实测该读 1.5-2.3s =
/// send 暗区根因）；联系人族写面（`storage.write` Contact 臂 / blocked /
/// state 读改写 / 入站 merge Contact 观测）即清该账号缓存，残余陈旧
/// ≤ [`WHITELIST_CACHE_TTL`]。fail-open 语义零变化（缺记录/位 true/读失败
/// 空 map 恒注入，[`include_profile_key_for`] 判定式不动；错误空 map 与
/// 真实空 map 同缓存同语义）。返回值第二位 = 缓存命中与否（探针段用）。
async fn profile_sharing_whitelist_probed(
    service: &Arc<Service>,
    account: &str,
) -> (HashMap<String, bool>, bool) {
    if let Some(hit) = whitelist_cache_get(&service.whitelist_cache, account) {
        return (hit, true);
    }
    let fresh = match storage_context(service, account).await {
        Ok(context) => storage_sync::contact_whitelist_map(&context).await,
        Err(_) => HashMap::new(),
    };
    whitelist_cache_put(&service.whitelist_cache, account, fresh.clone());
    (fresh, false)
}

/// profileSharing 闸读面（探针不关心的调用面薄壳，语义同
/// [`profile_sharing_whitelist_probed`]）。
async fn profile_sharing_whitelist(service: &Arc<Service>, account: &str) -> HashMap<String, bool> {
    profile_sharing_whitelist_probed(service, account).await.0
}

/// whitelist 缓存 TTL（发送长尾修1）：TTL 内重复发送零 Storage 读；
/// 写面失效兜住主动翻转，残余陈旧窗口 ≤ TTL（profileSharing 位非实时
/// 安全闸，偏差面同 compat §5 fail-open 登记）。
/// 取值依据（2026-10-17 真机探针复盘）：5s TTL 在真实发送节奏（间隔
/// 7-16s）下每条必 miss（服务端全量 manifest+联系人往返 1.5-1.7s 照付），
/// 形同虚设；读取面是 Signal Storage Service 网络往返（storage_sync::
/// contact_whitelist_map 全量拉取），本地 SQLite 假设不成立。TTL=600s
/// 让正常节奏全部命中，正确性由联系人族写面失效（本文件
/// whitelist_cache_invalidate 四处挂载）承担，TTL 仅作失联兜底。
const WHITELIST_CACHE_TTL: Duration = Duration::from_secs(600);

/// whitelist 缓存条目：(位 map, 过期时刻)（到期判定用注入时钟可比对的
/// deadline 形，测试可拨针）。
type WhitelistCacheEntry = (HashMap<String, bool>, std::time::Instant);

/// account 作用域 whitelist 缓存（Service 字段类型，进程内存不落盘）。
type WhitelistCache = HashMap<String, WhitelistCacheEntry>;

/// 缓存读取：TTL 内命中返回位 map，过期/缺席 None（锁内零 await）。
fn whitelist_cache_get(
    cache: &StdMutex<WhitelistCache>,
    account: &str,
) -> Option<HashMap<String, bool>> {
    let guard = cache.lock().expect("whitelist cache lock");
    let (map, deadline) = guard.get(account)?;
    (std::time::Instant::now() < *deadline).then(|| map.clone())
}

/// 缓存写入（读面 miss 回填单点）。
fn whitelist_cache_put(
    cache: &StdMutex<WhitelistCache>,
    account: &str,
    map: HashMap<String, bool>,
) {
    cache.lock().expect("whitelist cache lock").insert(
        account.to_owned(),
        (map, std::time::Instant::now() + WHITELIST_CACHE_TTL),
    );
}

/// 联系人族写面 → 该账号缓存失效（storage.write Contact 臂 / blocked /
/// state 读改写 / 入站 merge Contact 观测共用单点；下一发送重读全量）。
fn whitelist_cache_invalidate(cache: &StdMutex<WhitelistCache>, account: &str) {
    cache.lock().expect("whitelist cache lock").remove(account);
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

/// F 链路回声抑制预观测（storage.write 面）：三类会话记录（Contact/GV1/
/// GV2）的 INSERT/UPDATE 明文 → (会话标识, 三 flag)；DELETE（无记录载荷）
/// 与其余记录族 → None。标识/投影口径 = storage_sync 读面同构两函数
/// （明文两次 decode 换零耦合，批规模小无感）。
fn conversation_write_observation(
    update: &storage_write::StorageItemUpdate,
) -> Option<(
    storage_sync::EngineConversationId,
    storage_sync::ConversationState,
)> {
    match update {
        storage_write::StorageItemUpdate::Insert { record, .. }
        | storage_write::StorageItemUpdate::Update { record, .. } => {
            let id = storage_sync::conversation_record_of(record)?;
            let state = storage_sync::conversation_state_of(record)?;
            Some((id, state))
        }
        storage_write::StorageItemUpdate::Delete { .. } => None,
    }
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

/// lookupOutgoingDispatch 的应答面（KT 扩展，compat §5 unknown 出站行
/// 引擎真相对账批）：`dispatched` = 引擎库在该 destination 的 thread 内
/// 存在「sender=本机 ACI、ts 落容差窗」的出站行（引擎侧派发调用成功
/// 返回才会落行）；`server_timestamp` = 命中行落库 ts（dispatched=true
/// 时在场；presage 落行时 server_timestamp 即回填客户端时间戳、server_ts
/// 列不落写，引擎无独立服务器时刻源，键名沿用调用方命名）。应答面只
/// 此两键——正文/号码/群名/作者永不入面（内容红线）。
pub(crate) struct OutgoingDispatchLookup {
    pub(crate) dispatched: bool,
    pub(crate) server_timestamp: Option<u64>,
}

/// lookupOutgoingDispatch 容差窗缺省毫秒（compat §3：sentTimestamp 双侧
/// 容差窗；窗的存在理由与宽窄权衡见 §5 批块——connector unknown 行的
/// sent_at 是发送准备时刻，引擎行 ts 是派发信封时间戳，两者天然有偏移）。
pub(crate) const OUTGOING_DISPATCH_DEFAULT_TOLERANCE_MS: u64 = 2_000;

/// lookupOutgoingDispatch 容差窗硬帽毫秒（fail-closed 上界：窗即查询界，
/// 帽钉 60s 保持有界；官方 receipt 配对同粒度的 timestamp-only 不可分辨
/// 性随窗单调放大，上界不放大）。
pub(crate) const OUTGOING_DISPATCH_MAX_TOLERANCE_MS: u64 = 60_000;

/// 出站派发查证单点（lookupOutgoingDispatch 服务面的纯函数核，容差窗/
/// 发送者过滤/最近命中在此钉定）：候选行按 `sender == own` 过滤（出站行
/// 的落库判别——直聊行 destination=对端、群行 destination=本机，sender
/// 恒为本机 ACI）、按 `|ts - sent_timestamp|` 取最近者。行级解码失败由
/// 调用方 filter_map 跳过（fail-open 行级不炸面）；空窗诚实
/// dispatched=false。
fn nearest_outgoing_dispatch(
    rows: impl Iterator<Item = presage::libsignal_service::prelude::Content>,
    own: &ServiceId,
    sent_timestamp: u64,
) -> OutgoingDispatchLookup {
    let mut nearest: Option<(u64, u64)> = None;
    for content in rows {
        let ts = content.metadata.client_timestamp.timestamp_millis();
        let Ok(ts) = u64::try_from(ts) else {
            continue;
        };
        if &content.metadata.sender != own {
            continue;
        }
        let distance = ts.abs_diff(sent_timestamp);
        if nearest.is_none_or(|(best, _)| distance < best) {
            nearest = Some((distance, ts));
        }
    }
    match nearest {
        Some((_, ts)) => OutgoingDispatchLookup {
            dispatched: true,
            server_timestamp: Some(ts),
        },
        None => OutgoingDispatchLookup {
            dispatched: false,
            server_timestamp: None,
        },
    }
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

/// attachmentBackfillRequest 同步构帧（官方
/// getAttachmentBackfillSyncMessage 同构，SendMessage.preload.ts:1987-2018）：
/// SyncMessage{content: AttachmentBackfillRequest{targetMessage:
/// AddressableMessage{authorServiceIdBinary|authorServiceId?, sentTimestamp},
/// targetConversation: ConversationIdentifier{threadServiceIdBinary|
/// threadServiceId?|threadGroupId}}，padded sync message。target 面与
/// deleteForMe 构帧单点同源（官方 toAddressableMessage/toConversation
/// Identifier 同一对辅助函数）：作者 ACI 16B binary 现行面优先、不可解析
/// 16B 的已闸字符串降写 string 形态；群腿 = 32B master key 原样。
fn build_attachment_backfill_sync(
    target: &MessageRequestTarget,
    author: &presage::libsignal_service::protocol::ServiceId,
    sent_timestamp: u64,
) -> presage::proto::SyncMessage {
    use presage::proto::addressable_message::Author;
    use presage::proto::conversation_identifier::Identifier;

    let author = if let Some(bytes) = aci_to_bytes(&author.service_id_string()) {
        Author::AuthorServiceIdBinary(bytes)
    } else {
        Author::AuthorServiceId(author.service_id_string())
    };
    let identifier = match target {
        // Thread 腿 = 对端 service id（connector 侧已解析；UUID 形态写
        // binary 现行面，其余已闸字符串降写 string 形态——消息请求构帧
        // 同款双形态纪律）。
        MessageRequestTarget::Thread(peer) => match aci_to_bytes(peer) {
            Some(bytes) => Identifier::ThreadServiceIdBinary(bytes),
            None => Identifier::ThreadServiceId(peer.clone()),
        },
        MessageRequestTarget::Group(master_key) => Identifier::ThreadGroupId(master_key.clone()),
    };
    let request = presage::proto::sync_message::AttachmentBackfillRequest {
        target_message: Some(presage::proto::AddressableMessage {
            author: Some(author),
            sent_timestamp: Some(sent_timestamp),
        }),
        target_conversation: Some(presage::proto::ConversationIdentifier {
            identifier: Some(identifier),
        }),
    };
    presage::proto::SyncMessage {
        content: Some(presage::proto::sync_message::Content::AttachmentBackfillRequest(request)),
        ..presage::proto::SyncMessage::with_padding(&mut rng())
    }
}

/// deleteForMeConversation 会话标识（官方 toConversationIdentifier 三形态，
/// SendMessage.preload.ts:2915）：service id（ACI/PNI）直写
/// threadServiceIdBinary 现行 binary 面；E164 直写 threadE164；GV2 群 =
/// 32B master key 原样 bytes。非 GV2 群官方不支持（getConversationIdentifier
/// isGroup&&!isGroupV2 throw，syncIdentifiers.preload.ts:148-150），API 面
/// 同拒 legacyGroup。
pub(crate) enum DeleteForMeConversation {
    Contact(ServiceId),
    Phone(String),
    Group(Vec<u8>),
}

/// deleteForMeConversation mostRecentMessages 条目（官方 toAddressableMessage
/// 同构：author ACI/PNI/E164 三形态 + sentTimestamp；SendMessage.preload.ts
/// :2871）。作者 = 调用方消息行的身份直通（ACI/E164，官方
/// getAddressableMessage 同源取材），引擎不做号码↔service id 二次变换。
pub(crate) struct DeleteForMeEntry {
    pub(crate) author: DeleteForMeAuthor,
    pub(crate) timestamp: u64,
}

pub(crate) enum DeleteForMeAuthor {
    ServiceId(ServiceId),
    Phone(String),
}

/// DeleteForMe 会话标识构帧单点（官方 toConversationIdentifier 三形态，
/// SendMessage.preload.ts:2915）：service id（ACI/PNI）直写
/// threadServiceIdBinary 现行 binary 面；E164 直写 threadE164；GV2 群 =
/// 32B master key 原样 bytes。会话级与消息级两条发送臂共用（官方
/// getDeleteForMeSyncMessage 对所有 DeleteForMe 条目同调该辅助函数）。
fn delete_for_me_conversation_identifier(
    conversation: &DeleteForMeConversation,
) -> presage::proto::ConversationIdentifier {
    use presage::proto::conversation_identifier::Identifier;

    let identifier = match conversation {
        DeleteForMeConversation::Contact(sid) => {
            Identifier::ThreadServiceIdBinary(sid.service_id_binary())
        }
        DeleteForMeConversation::Phone(e164) => Identifier::ThreadE164(e164.clone()),
        DeleteForMeConversation::Group(master_key) => Identifier::ThreadGroupId(master_key.clone()),
    };
    presage::proto::ConversationIdentifier {
        identifier: Some(identifier),
    }
}

/// DeleteForMe 消息条目构帧单点（官方 toAddressableMessage 同构：
/// author ACI/PNI/E164 三形态 + sentTimestamp；SendMessage.preload.ts:2871）。
/// 会话级 mostRecent* 与消息级 messages 两臂共用。
fn delete_for_me_addressable_message(
    entry: &DeleteForMeEntry,
) -> presage::proto::AddressableMessage {
    use presage::proto::addressable_message::Author;

    presage::proto::AddressableMessage {
        author: Some(match &entry.author {
            DeleteForMeAuthor::ServiceId(sid) => {
                Author::AuthorServiceIdBinary(sid.service_id_binary())
            }
            DeleteForMeAuthor::Phone(e164) => Author::AuthorE164(e164.clone()),
        }),
        sent_timestamp: Some(entry.timestamp),
    }
}

/// DeleteForMe 会话级构帧（官方 getDeleteForMeSyncMessage delete-conversation
/// / delete-local-conversation 两变体同位，SendMessage.preload.ts:1840/
/// conversations.preload.ts:5640-5663）：全量清空臂 = conversationDeletes[0]
/// {conversation, isFullDelete: true, mostRecentMessages,
/// mostRecentNonExpiringMessages?}；仅本地臂 = localOnlyConversationDeletes[0]
/// {conversation}（官方 mostRecentMessages 空批 else 分支同语义）。padded
/// sync message。
fn build_delete_for_me_conversation_sync(
    conversation: &DeleteForMeConversation,
    local_only: bool,
    most_recent: &[DeleteForMeEntry],
    most_recent_non_expiring: &[DeleteForMeEntry],
) -> presage::proto::SyncMessage {
    let conversation_identifier = delete_for_me_conversation_identifier(conversation);
    let delete_for_me = if local_only {
        presage::proto::sync_message::DeleteForMe {
            local_only_conversation_deletes: vec![
                presage::proto::sync_message::delete_for_me::LocalOnlyConversationDelete {
                    conversation: Some(conversation_identifier),
                },
            ],
            ..presage::proto::sync_message::DeleteForMe::default()
        }
    } else {
        presage::proto::sync_message::DeleteForMe {
            conversation_deletes: vec![
                presage::proto::sync_message::delete_for_me::ConversationDelete {
                    conversation: Some(conversation_identifier),
                    is_full_delete: Some(true),
                    most_recent_messages: most_recent
                        .iter()
                        .map(delete_for_me_addressable_message)
                        .collect(),
                    most_recent_non_expiring_messages: most_recent_non_expiring
                        .iter()
                        .map(delete_for_me_addressable_message)
                        .collect(),
                },
            ],
            ..presage::proto::sync_message::DeleteForMe::default()
        }
    };
    presage::proto::SyncMessage {
        content: Some(presage::proto::sync_message::Content::DeleteForMe(
            delete_for_me,
        )),
        ..presage::proto::SyncMessage::with_padding(&mut rng())
    }
}

/// deleteForMeMessages 单请求条目帽（官方 SendMessage.preload.ts:1923
/// 用 MAX_MESSAGE_COUNT=500 分批，每批超限只 warn 不截断；引擎单 RPC
/// 按破坏性意图 fail-closed：超帽整批拒，不静默截断也不拆批，调用方
/// 自行分批。取值对齐 connector 消费面
/// `MAX_SYNC_DELETE_GROUP_MESSAGES = 256`（kt-signal-connector
/// src/engine.rs），与 attachmentDeletes 收投影帽同值）。
pub(crate) const MAX_DELETE_FOR_ME_MESSAGES: usize = 256;

/// DeleteForMe 消息级构帧（官方 getDeleteForMeSyncMessage messageDeletes
/// 臂，SendMessage.preload.ts:1840-1936；收方处理同位
/// MessageReceiver.preload.ts:3609-3657）：SyncMessage.deleteForMe.
/// messageDeletes[0]{conversation, messages[]}——官方按会话聚合，
/// `messages = items.map(toAddressableMessage)`；本方法一次 RPC 只服务
/// 一个会话，故恰一条 MessageDeletes。padded sync message。
fn build_delete_for_me_messages_sync(
    conversation: &DeleteForMeConversation,
    entries: &[DeleteForMeEntry],
) -> presage::proto::SyncMessage {
    let message_deletes = presage::proto::sync_message::delete_for_me::MessageDeletes {
        conversation: Some(delete_for_me_conversation_identifier(conversation)),
        messages: entries
            .iter()
            .map(delete_for_me_addressable_message)
            .collect(),
    };
    presage::proto::SyncMessage {
        content: Some(presage::proto::sync_message::Content::DeleteForMe(
            presage::proto::sync_message::DeleteForMe {
                message_deletes: vec![message_deletes],
                ..presage::proto::sync_message::DeleteForMe::default()
            },
        )),
        ..presage::proto::SyncMessage::with_padding(&mut rng())
    }
}

/// 登录期 sync/自动拉 60s 一次性窗（毫秒）：watchdog 断连重开风暴内不重复
/// 轰主设备/Storage Service（P15c 项4 既有纪律，T-9 manifest 自动拉同门）。
const INITIAL_SYNC_WINDOW_MS: u64 = 60_000;

/// 登录期 sync 时机裁定（60s 一次性纪律单点；P15c 项4 既有语义抽纯函数，
/// T-9 manifest 自动拉同门共闸）：上次记录缺席（首流/进程重启后）或超窗
/// → true（本次建流安排 +2s 沉降）；窗内（重连风暴）→ false 恰跳过。
fn initial_sync_due(last_elapsed_ms: Option<u64>) -> bool {
    match last_elapsed_ms {
        Some(elapsed) => elapsed >= INITIAL_SYNC_WINDOW_MS,
        None => true,
    }
}

/// FetchLatest STORAGE_MANIFEST 触发判别（T-9 消费臂入口纯函数面）：官方
/// 「实时」= 改了 manifest 的设备发普通 E2E 信封 SyncMessage.fetchLatest
/// {STORAGE_MANIFEST}，收方 HTTP 重拉（服务器 websocket 只推 E2E 信封 +
/// 队列排空，storage 无 live 推送——ADR 0007 / compat §5 本批块结论）。
/// 仅 STORAGE_MANIFEST 触发；LOCAL_PROFILE/SUBSCRIPTION_STATUS/UNKNOWN/
/// type 缺席（缺席照投空对象的信号面语义不变）一律不触发。
fn storage_manifest_fetch_requested(
    content: &presage::libsignal_service::content::Content,
) -> bool {
    let presage::libsignal_service::content::ContentBody::SynchronizeMessage(sync) = &content.body
    else {
        return false;
    };
    matches!(
        sync.content.as_ref(),
        Some(presage::proto::sync_message::Content::FetchLatest(fetch))
            if fetch.r#type
                == Some(presage::proto::sync_message::fetch_latest::Type::StorageManifest as i32)
    )
}

/// FetchLatest STORAGE_MANIFEST 消费臂防抖器（官方 sync job debounce 500ms
/// 同款）：trailing 沿——每封把窗沿推后 500ms，防抖窗内多封合并为恰一次
/// 拉取；take_due 到期裁定恰一次清位。毫秒簿记纯函数面（now 注入可测），
/// 收流 select 臂按簿记桥接 sleep。
#[derive(Default)]
struct ManifestPullDebounce {
    deadline: Option<u64>,
}

impl ManifestPullDebounce {
    /// 防抖窗（官方 storage sync job debounce 同款 500ms）。
    const WINDOW_MS: u64 = 500;

    /// 一封触发信封到达：窗沿推后（trailing debounce；墙钟毫秒簿记，NTP
    /// 回拨至多延迟一次拉取，前跳至多提前——拉取幂等，两向无害）。
    fn on_fetch(&mut self, now: u64) {
        self.deadline = Some(now.saturating_add(Self::WINDOW_MS));
    }

    /// 窗沿裁定：到期 → 清位 + true（本窗恰一次）；未到期/无窗 → false
    /// （簿记不动——select 假醒重睡，拉取不丢不重）。
    fn take_due(&mut self, now: u64) -> bool {
        match self.deadline {
            Some(at) if now >= at => {
                self.deadline = None;
                true
            }
            _ => false,
        }
    }

    fn pending_deadline(&self) -> Option<u64> {
        self.deadline
    }
}

/// manifest 自动拉单次结果（两腿各自汇总/错误）：触发方 best-effort 不看
/// ，测试钉定分发面（UnknownAccount 只能出自两腿本体）+ 打点外观测面。
type ManifestPullOutcome = (
    Result<storage_merge::MergeOutcome, storage_sync::StorageSyncError>,
    Result<storage_merge::GroupImportOutcome, storage_sync::StorageSyncError>,
);

/// manifest 自动拉单次（T-9 两触发面共用单点）：storage.mergeContacts 与
/// storage.importGroups 同一内部函数面（一处权威两链共用——消费臂、登录
/// 期自动拉、connector 显式 RPC 三链同走这两个 Service 方法），manifest
/// 重读 → Contact 全量 merge → GROUPV2 导入 → 既有 storage_watch 观测簿
/// 差分出口自然出 `receive.conversationState` 翻转通知。逐腿 best-effort
/// （零 stderr 纪律，失败走打点 + `engine.storagePull` 逐腿如实位；读面
/// 幂等，靠下次触发/下次链路建立重补）。两臂解析后经既有通知通道恒发
/// 一条 `engine.storagePull` 结果事件（T-9b，storage_pull_notification）。
async fn pull_storage_manifest(
    service: &Arc<Service>,
    account: &str,
    trigger: &'static str,
) -> ManifestPullOutcome {
    perf_metrics::add(trigger, 1);
    let contacts = service.merge_storage_contacts(account).await;
    if contacts.is_err() {
        perf_metrics::add(perf_metrics::keys::STORAGE_MANIFEST_PULL_ERR, 1);
    }
    let groups = service.import_storage_groups(account).await;
    if groups.is_err() {
        perf_metrics::add(perf_metrics::keys::STORAGE_MANIFEST_PULL_ERR, 1);
    }
    service
        .notify(
            account,
            storage_pull_notification(account, trigger, contacts.is_ok(), groups.is_ok()),
        )
        .await;
    (contacts, groups)
}

/// manifest 拉取结果通知（T-9b，wire additive，`engine.storagePull`，
/// compat §5 契约 1.141 由 connector 侧登记、形状以本仓为权威）：两触发
/// 臂解析后恒发恰一条。params = account（通道惯例域，receive.emptied 同
/// 款）/ trigger（login | fetchLatest，perf 触发键单射映射）/ contactsOk
/// / groupsOk——两腿逐腿如实布尔（false = 该腿 Err，含未注册账号
/// UnknownAccount），成功面与 conversationState 差分正交（manifest 无
/// 变化零翻转也有本事件可等）。补 b719997 双信号缺口之第 1 缺：失败此前
/// 只有内部打点（storage_manifest_pull_err），桌面不可观测。best-effort：
/// notify fire-and-forget（receive.emptied/receive.clockskew 同款），发射
/// 不连坐拉取结果、不重试；connector 显式 RPC 链（storage.mergeContacts
/// /storage.importGroups 直调）不经本单点，零新增通知。
fn storage_pull_notification(
    account: &str,
    trigger: &str,
    contacts_ok: bool,
    groups_ok: bool,
) -> Value {
    let trigger_name = if trigger == perf_metrics::keys::STORAGE_MANIFEST_PULL_LOGIN {
        "login"
    } else {
        "fetchLatest"
    };
    json!({
        "jsonrpc": "2.0",
        "method": "engine.storagePull",
        "params": {
            "account": account,
            "trigger": trigger_name,
            "contactsOk": contacts_ok,
            "groupsOk": groups_ok,
        },
    })
}

/// 链接期初始同步完成事件（收件箱闸批①，wire additive，compat §5 新增契约——`engine.initialSyncComplete`）：登录窗 sync 臂（每链路 60s 一次性）三件套全部落定后恒发恰一条——初始三请求已上行（臂内串行先行）、manifest 登录拉（30s 上限独立任务）与首轮 profile 刷新（+10s 沉降独立任务）两腿 join 完成时刻。params = account（通道惯例域）/ manifestOk / profilesOk——两腿逐腿如实布尔（false = 该腿 Err 或超时），官方 contactSync:complete/storageService:syncComplete 仅成功路径发的语义由消费方按 ok face 落定（引擎只报态，零 stderr 纪律 fire-and-forget，receive.emptied 同款：发射不连坐结果、不重试；失败由下次链路建立重补 + 消费方半边超时兜底）。桌面收件箱闸第三半边（首轮 profile 刷新）的唯一定局信号——官方 background.preload.ts:1926-1948 步骤 7 对位。
fn initial_sync_complete_notification(
    account: &str,
    manifest_ok: bool,
    profiles_ok: bool,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "engine.initialSyncComplete",
        "params": {
            "account": account,
            "manifestOk": manifest_ok,
            "profilesOk": profiles_ok,
        },
    })
}

/// syncRequests 单请求构帧（P15c 项4，官方 getRequestConfigurationSyncMessage
/// / getRequestBlockSyncMessage 同位，SendMessage.preload.ts:1685/:1654）：
/// SyncMessage.content.Request{type}，padded sync message。
fn build_sync_request(
    r#type: presage::proto::sync_message::request::Type,
) -> presage::proto::SyncMessage {
    presage::proto::SyncMessage {
        content: Some(presage::proto::sync_message::Content::Request(
            presage::proto::sync_message::Request {
                r#type: Some(r#type as i32),
            },
        )),
        ..presage::proto::SyncMessage::with_padding(&mut rng())
    }
}

/// 链接完成 sync 上行三请求（P15c 项4，官方 sendSyncRequests 同位，
/// syncRequests.preload.ts:11-34 / afterEveryLinkedStartupOnNewVersion
/// 触发）：新设备/链接启动向主设备要 contacts/configuration/block 三族
/// 全量 sync（三封独立 SyncMessage.Request 上行，官方 singleProtoJobQueue
/// × 3 同形态；contacts 腿 = presage 官方 request_contacts API）。引擎
/// 既有 60s 一次性纪律保留（重连风暴不重复轰主设备），请求族由 contacts
/// 一腿补齐为官方三腿；每腿独立 best-effort（官方 Promise.all 一损俱损，
/// 引擎零 stderr 纪律下逐腿尽力——sync 请求幂等，失败靠下次链路建立重补）。
async fn send_initial_sync_requests(manager: &mut Manager<SqliteStore, Registered>) {
    let _ = manager.request_contacts().await;
    let own_aci = ServiceId::Aci(manager.registration_data().service_ids.aci.into());
    for r#type in [
        presage::proto::sync_message::request::Type::Configuration,
        presage::proto::sync_message::request::Type::Blocked,
    ] {
        let body = ContentBody::SynchronizeMessage(build_sync_request(r#type));
        let _ = manager.send_message(own_aci, body, now_ms()).await;
    }
}

/// E.164 形状闸（`+` 前缀 + 1..=15 位 ASCII 数字；deleteForMeConversation
/// conversation/author E164 臂用——引擎无号码库语义，形状闸 fail-closed）。
pub(crate) fn is_e164(text: &str) -> bool {
    match text.strip_prefix('+') {
        Some(digits) => {
            !digits.is_empty() && digits.len() <= 15 && digits.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
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

/// calls.relays 错误面：未知/未注册账号（惯例 -32603 折叠）与账号凭据
/// 失效（解绑原文）。网络/形状失败不进本枚举——按模块纪律保旧缓存/空态
/// 如实应答，错误不连坐。
pub(crate) enum CallingRelaysError {
    UnknownAccount,
    Unbound,
}

/// 链接结果共享槽：finishLink 可被连接器超时重试（connector service.rs 语义），
/// 多个等待者共享同一次结果。
/// 链接成功行囊：号码 + 迁移下载凭据（可选）。transfer 腿不在 link_flow
/// 启动——收口清理（finish_link 同号旧任务 cancel + 进度清缺）必须先行，
/// 凭据经本槽后移，语句序结构性锁死「先清旧、后启新」。
type LinkOutcome = (String, Option<PendingTransferDownload>);

struct LinkResultSlot {
    outcome: StdMutex<Option<Result<LinkOutcome, ()>>>,
    resolved: Notify,
}

impl LinkResultSlot {
    fn new() -> Self {
        Self {
            outcome: StdMutex::new(None),
            resolved: Notify::new(),
        }
    }

    fn complete(&self, outcome: Result<LinkOutcome, ()>) {
        *self.outcome.lock().expect("outcome lock") = Some(outcome);
        self.resolved.notify_waiters();
    }

    async fn wait(&self) -> Result<LinkOutcome, ()> {
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

/// provisioning 一次性迁移密钥的收口前行囊：link_flow 解构后原样暂存，
/// finish_link 清理旧任务后据此启动 transfer 下载腿。
#[derive(Clone)]
struct PendingTransferDownload {
    aci: presage::libsignal_service::prelude::Uuid,
    credentials: ServiceCredentials,
    backup_key: [u8; 32],
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
    /// 见 main::OutboundFrame——Notice 直通，Flush 背压栅栏）。全局队列
    /// 有界 + 每账号在途许可有界，`notify` 在账号许可耗尽时对单账号背压
    /// （见 [`outbound`] 模块头注）。
    outbound: Arc<outbound::OutboundHub>,
    inner: AsyncMutex<Inner>,
    /// 运行中的接收管线（账号 → 停机旗 + 任务句柄），delete/relink 时
    /// 通知退出并等待收口（A4：停接收 → drop Manager → 落盘）。
    pipelines: AsyncMutex<HashMap<String, PipelineHandle>>,
    /// 备份导出任务注册表（进程内存，backup_task 模块契约 §10.3）。
    backup_tasks: Arc<backup_task::BackupTaskRegistry>,
    /// 官方快照导入任务注册表（进程内存，snapshot_import 模块 §11）。
    snapshot_imports: Arc<snapshot_import::SnapshotImportRegistry>,
    /// 已终止群登记表（进程内存，groups 模块：receive 投影 → send 面拦截）。
    terminated_groups: groups::TerminatedGroups,
    /// transfer_archive 下载进度快照（进程内存，不落盘；§4.38 progress 面；
    /// 失败终态同面透出，`transfer.downloadStatus` 观测）。
    transfer_progress: transfer_progress::TransferProgress,
    /// transfer_archive 下载任务句柄注册表（进程内存；`history.importCancel`
    /// 取消面 transfer 腿）。
    transfer_downloads: transfer_progress::TransferDownloadRegistry,
    /// 云备份 restore 进度/终态快照（进程内存单槽，不落盘；cloud-backup-contract
    /// §2 cloudStatus 面）。
    cloud_restore_progress: cloud_backup::CloudRestoreProgress,
    /// 云备份 restore 在途任务句柄（进程级单槽：归属账号 + JoinHandle；
    /// `history.importCancel` 取消面 restore 腿）。
    cloud_restore_task: StdMutex<Option<(String, tokio::task::JoinHandle<()>)>>,
    /// 云备份 upload 进度/终态快照（进程内存单槽，不落盘；cloud-backup-contract
    /// §6.2 uploadStatus 面）。
    cloud_upload_progress: cloud_upload::CloudUploadProgress,
    /// 群凭据双族缓存（进程内存 per-account ACI 键控，不落盘不跨重启；
    /// P4c 切一 call_links 模块契约，官方 groupCredentialFetcher 对位件）。
    group_credentials: Arc<call_links::GroupCredentialCache>,
    /// F 链路入站观测簿（进程内存 per-account，不落盘不跨重启）：会话三
    /// flag「消费方最后被告知的态」，storage_watch 模块契约（帽/种子静默/
    /// 防回声纪律见模块头注）。delete_account 摘除。
    conversation_state_books: StdMutex<HashMap<String, storage_watch::ConversationStateBook>>,
    /// 账号级常驻发送管理器（C 批提速）：号码 → 共享互斥管理器。发送取用
    /// 命中即复用（websocket 由进程级 slot 注册表归一——同账号全引擎共享
    /// 单连接，closed → 重开并先拆旧，死亡自愈在 vendor 侧）；惰性
    /// 建连 + 空闲保留，不逐消息冷启动。同账号取用互斥串行（Manager 方法
    /// 需 &mut 且跨 await 持有），跨账号互不阻塞；delete/relink 摘除。
    send_managers: AsyncMutex<SendManagerCache>,
    /// calls.relays 应答缓存（进程内存 per-account，不落盘不跨重启；
    /// 官方 #iceServersCache 对位件 per-account 分桶版，calling_relays
    /// 模块头注载 TTL/失败保旧纪律）。delete_account 摘除。
    calling_relays_cache: AsyncMutex<HashMap<String, calling_relays::CacheEntry>>,
    /// 群发 MRM 单请求面（C 批提速）：per-account unauth chat WS 连接池
    /// （PUT /v1/messages/multi_recipient 不带账号凭据，token 自证；连接
    /// 按账号隔离——R4：单账号失败/重连只摘本槽，不拖垮其他账号；池上限
    /// 2 = R2 连接预算，全忙 fail-closed 走 failover）。
    mrm_pool: group_mrm::MrmPool,
    /// profileSharing 闸 whitelist 位缓存（发送长尾修1）：账号 → (位 map,
    /// 过期时刻)，TTL [`WHITELIST_CACHE_TTL`] + 联系人族写面失效；进程内存
    /// 不落盘不跨重启。delete_account 摘除。
    whitelist_cache: StdMutex<WhitelistCache>,
    /// 本机 profile capabilities 位缓存（回填能力门数据面，2026-10-20 批）：
    /// 账号 → 回填位 + 过期时刻，TTL [`own_capabilities::CAPABILITIES_CACHE_TTL`]，
    /// 失败不缓存；进程内存不落盘不跨重启。delete/重链摘除。
    own_capabilities_cache: own_capabilities::CapabilitiesCache,
}

/// 接收管线停机宽限（S5 关停宽限可配）：停旗发出后等待任务收口（drop
/// Manager → SQLite store 刷盘）的上限。正常收口是 select 的即时臂（毫秒
/// 级）；超时只可能发生在传输/落库长尾，按有界宽限继续删除流程（账号删除
/// 本身是显式破坏性意图）。
///
/// 连接器的关停预算（今天 3 s + 2 s margin；池化后建议 ≥12 s）必须覆盖本
/// 宽限 + 进程收尾。默认 10 s 与连接器 12 s 预算对齐；运维可用
/// `KT_ENGINE_SHUTDOWN_GRACE_MS` 把两侧钉到同一预算（设计 §6.2 / §9 S5.3）。
const PIPELINE_SHUTDOWN_GRACE_ENV: &str = "KT_ENGINE_SHUTDOWN_GRACE_MS";
const PIPELINE_SHUTDOWN_GRACE_DEFAULT_MS: u64 = 10_000;
/// 上限 5 min：配置错误不应让 delete/进程退出无限悬挂。
const PIPELINE_SHUTDOWN_GRACE_MAX_MS: u64 = 300_000;

/// 纯解析：空/非数字/0/超上限 = None（调用方回默认）。单测直打。
fn parse_pipeline_shutdown_grace(raw: Option<&str>) -> Option<Duration> {
    let millis = raw?.trim().parse::<u64>().ok()?;
    if millis == 0 || millis > PIPELINE_SHUTDOWN_GRACE_MAX_MS {
        return None;
    }
    Some(Duration::from_millis(millis))
}

/// 解析环境预算；非法值回默认并留结构化 warn（关停预算是运维旋钮，不应
/// 把进程挡在启动门外）。
fn pipeline_shutdown_grace() -> Duration {
    let raw = std::env::var(PIPELINE_SHUTDOWN_GRACE_ENV).ok();
    if let Some(grace) = parse_pipeline_shutdown_grace(raw.as_deref()) {
        return grace;
    }
    if raw.as_deref().is_some_and(|value| !value.trim().is_empty()) {
        tracing::warn!(
            target: "engine.supervisor",
            stage = "pipeline_shutdown_grace_invalid",
            env = PIPELINE_SHUTDOWN_GRACE_ENV,
            value = %raw.as_deref().unwrap_or_default(),
            default_ms = PIPELINE_SHUTDOWN_GRACE_DEFAULT_MS,
            max_ms = PIPELINE_SHUTDOWN_GRACE_MAX_MS,
            "invalid shutdown grace; falling back to default",
        );
    }
    Duration::from_millis(PIPELINE_SHUTDOWN_GRACE_DEFAULT_MS)
}

/// 运行中的接收管线句柄：停机旗 + 任务句柄（delete/relink 等待收口）。
struct PipelineHandle {
    shutdown: watch::Sender<bool>,
    done: tokio::task::JoinHandle<()>,
}

/// 等待管线任务收口（有界宽限；超时只落结构化 warn，账号哈希不落号码）。
async fn await_pipeline_shutdown(account: String, handle: PipelineHandle, grace: Duration) {
    if tokio::time::timeout(grace, handle.done).await.is_err() {
        tracing::warn!(
            target: "engine.supervisor",
            stage = "pipeline_shutdown_timeout",
            account = %crate::methods::probe_account8(Some(&account)),
            grace_ms = grace.as_millis() as u64,
            "pipeline did not stop within grace; continuing",
        );
    }
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
        outbound: Arc<outbound::OutboundHub>,
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
            transfer_downloads: transfer_progress::TransferDownloadRegistry::new(),
            cloud_restore_progress: cloud_backup::CloudRestoreProgress::new(),
            cloud_restore_task: StdMutex::new(None),
            cloud_upload_progress: cloud_upload::CloudUploadProgress::new(),
            group_credentials: Arc::new(call_links::GroupCredentialCache::new()),
            conversation_state_books: StdMutex::new(HashMap::new()),
            send_managers: AsyncMutex::new(HashMap::new()),
            calling_relays_cache: AsyncMutex::new(HashMap::new()),
            mrm_pool: group_mrm::MrmPool::new(group_mrm::MRM_CONNECTION_LIMIT),
            whitelist_cache: StdMutex::new(HashMap::new()),
            own_capabilities_cache: own_capabilities::CapabilitiesCache::new(),
        })
    }

    /// stdout 通知入队（账号在途许可 + 全局有界队列；行序 = 队列序）。
    /// 账号许可耗尽时等待该账号帧被 writer 写出（单账号背压，不阻塞其他
    /// 账号）；writer 死亡返回 false（调用方按 fire-and-forget 纪律忽略，
    /// ack 安全性由 flush 栅栏兜底）。
    pub(crate) async fn notify(&self, account: &str, value: Value) -> bool {
        // 出站队列/账号许可在连接器停止读取时会无限背压；收流任务不能
        // 卡在这里（卡住 = 不再 poll 信封）。30s 上限后按出口不可达返回
        // false，后续 flush 栅栏会再次超时并触发 StdioDead 停管线。
        match tokio::time::timeout(RECEIVE_SIDE_TIMEOUT, self.outbound.notify(account, value)).await
        {
            Ok(resolved) => resolved,
            Err(_) => {
                tracing::warn!(
                    target: "engine.receive",
                    stage = "notify_timeout",
                    account = %crate::methods::probe_account8(Some(account)),
                    "receive notification could not enter the outbound queue within the deadline"
                );
                false
            }
        }
    }

    /// flush 背压栅栏（时序审计 G1）：resolve = 该账号此前全部已入队通知
    /// 已写进 stdout 并 flush（连接器至少收到字节）；`false` = 出口已死
    /// （writer 退出/通道关闭）——调用方据此放弃 ack，让服务器队列保留
    /// 信封待重启重放，连接器侧永不缺投影。
    pub(crate) async fn flush_barrier(&self, account: &str) -> bool {
        let started = std::time::Instant::now();
        // 出口半开（连接器活着但不再读 stdout）时，栅栏会无限等待，收流
        // 任务随之静默停摆。30s 内写不出字节 = 出口事实死亡：返回 false，
        // 调用方按 StdioDead 停管线，信封留服务器队列待重启重放。
        let resolved =
            match tokio::time::timeout(RECEIVE_SIDE_TIMEOUT, self.outbound.flush(account)).await {
                Ok(resolved) => resolved,
                Err(_) => {
                    tracing::warn!(
                        target: "engine.receive",
                        stage = "flush_barrier_timeout",
                        account = %crate::methods::probe_account8(Some(account)),
                        "stdout flush barrier timed out; treating the output side as dead"
                    );
                    false
                }
            };
        // S2a 打点：栅栏时长 + 成败计数（O3 flush_barrier 批量化候选的
        // 基线测量面；只测不改，语义零变化）。
        perf_metrics::record_us(
            perf_metrics::keys::FLUSH_BARRIER,
            started.elapsed().as_micros() as u64,
        );
        if resolved {
            perf_metrics::add(perf_metrics::keys::FLUSH_BARRIER_OK, 1);
        } else {
            perf_metrics::add(perf_metrics::keys::FLUSH_BARRIER_DEAD, 1);
        }
        resolved
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
        let sweep = sweep_orphan_store_dirs(&self.data_dir, &referenced);
        if sweep.quarantined > 0
            || sweep.removed_empty > 0
            || sweep.kept_non_empty > 0
            || sweep.failed > 0
        {
            tracing::warn!(
                cause = "OrphanSweepSummary",
                quarantined = sweep.quarantined,
                removed_empty = sweep.removed_empty,
                kept_non_empty = sweep.kept_non_empty,
                failed = sweep.failed,
                "startup orphan store sweep finished"
            );
        }
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
        if accounts.is_empty() {
            tracing::warn!(
                target: "engine.receive",
                stage = "no_accounts",
                "no registered accounts at startup; receive pipelines will start after link"
            );
        }
        for (number, dir) in accounts {
            self.spawn_pipeline(number, dir).await;
        }
    }

    async fn spawn_pipeline(self: &Arc<Self>, number: String, dir: PathBuf) {
        // relink 同号：先停旧管线并等待收口（A4：停接收 → drop Manager →
        // 落盘），再挂新 store 目录的管线。启动路径无旧句柄 = no-op。
        self.stop_pipeline(&number).await;
        // 启动/重链证据行：旧实现建流成功与失败都可能零日志，现场只剩
        // 「没有消息」。warn 级落滚动日志（logfile 只收 warn+）。
        tracing::warn!(
            target: "engine.receive",
            stage = "pipeline_spawn",
            account = %crate::methods::probe_account8(Some(number.as_str())),
            "receive pipeline spawned"
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = Arc::clone(self);
        let task_number = number.clone();
        // presage 的接收流持有非 Send 状态（ThreadRng/dyn ProtocolStore），
        // 进不了 tokio::spawn；统一走 LocalSet（main 装配提供上下文）。
        let done = tokio::task::spawn_local(async move {
            receive_pipeline(&service, task_number, dir, shutdown_rx).await;
        });
        self.pipelines.lock().await.insert(
            number,
            PipelineHandle {
                shutdown: shutdown_tx,
                done,
            },
        );
    }

    /// 停一个账号的接收管线并等待收口（有界宽限）。幂等：无句柄 = no-op。
    async fn stop_pipeline(&self, account: &str) {
        let handle = self.pipelines.lock().await.remove(account);
        if let Some(handle) = handle {
            tracing::warn!(
                target: "engine.receive",
                stage = "pipeline_stop",
                account = %crate::methods::probe_account8(Some(account)),
                "receive pipeline stopping"
            );
            let _ = handle.shutdown.send(true);
            await_pipeline_shutdown(account.to_owned(), handle, pipeline_shutdown_grace()).await;
        }
    }

    /// 进程收尾：通知全部接收管线停机并等待收口（stdin EOF 的干净退出路径）。
    /// 在途信封在其处理臂内完成（select 停旗不打断已开始的臂体），Manager
    /// drop 时 SQLite store 正常收尾——不丢未结算行。
    pub(crate) async fn shutdown_all(&self) {
        let handles: Vec<(String, PipelineHandle)> = {
            let mut pipelines = self.pipelines.lock().await;
            pipelines.drain().collect()
        };
        let pipelines = handles.len();
        let grace = pipeline_shutdown_grace();
        let started = Instant::now();
        for (_, handle) in &handles {
            let _ = handle.shutdown.send(true);
        }
        let waits = handles
            .into_iter()
            .map(|(account, handle)| await_pipeline_shutdown(account, handle, grace));
        futures::future::join_all(waits).await;
        // S5：关停收口耗时留痕（warn 级 = 滚动文件日志上限；无账号/正文）。
        // 正常毫秒级；接近 grace_ms 说明传输/落库长尾，供连接器 12 s 预算
        // 对账（设计 §6.2）。
        tracing::warn!(
            target: "engine.supervisor",
            stage = "engine_shutdown",
            pipelines,
            grace_ms = grace.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "receive pipelines drained; engine shutdown complete",
        );
    }

    /// `engine.perfMetrics` 快照（S2a 基线 + A1 per-account 观测 additive）：
    /// 既有 `{uptimeMs,timings,counters,unknownKeyDropped}` 四键不变，追加
    /// `accounts`（逐账号接收/重连/崩溃面）与 `outbound`（全局/逐账号在途）。
    pub(crate) fn perf_metrics_snapshot(&self) -> Value {
        let mut snapshot = perf_metrics::snapshot_json();
        account_metrics::extend_snapshot(&mut snapshot, &self.outbound);
        snapshot
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
        let task_slot = Arc::clone(&slot);
        tokio::task::spawn_local(async move {
            match link_flow(store, url_tx).await {
                Ok(outcome) => task_slot.complete(Ok(outcome)),
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
    ) -> Result<LinkedAccount, LinkError> {
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
            // fake 账号无 store：aci 整键缺席（LinkedAccount::aci = None）。
            return Ok(LinkedAccount { number, aci: None });
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
        let (number, pending_transfer) = slot.wait().await.map_err(|_| LinkError::Internal)?;

        // 成功：会话收口。设备名（ADR 0004）：改写 store 注册数据后启动接收
        // 管线；名字只落本地注册数据，管线不回写服务器（B4 起停发
        // AccountAttributes PUT——官方链接设备从不 PUT attributes，compat §5）。
        let mut inner = self.inner.lock().await;
        if inner
            .pending
            .as_ref()
            .is_none_or(|pending| pending.uri != uri)
        {
            // 并发重试已完成收口：幂等返回同一号码。ACI 不为此回查 store
            //（会话槽已收走，锁结构不为一枚 additive 键复杂化）——键缺席
            // 无害，重试消费方按可选键语义退回旧行为。
            return Ok(LinkedAccount { number, aci: None });
        }
        let pending = inner.pending.take().expect("checked");
        // 会话槽已收走：后续 store/state/pipeline 收口均不持 inner 锁。
        drop(inner);
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
        // P0 重链孤儿批（2026-10-23）：注册表按号码唯一，写入新 dir 的同时
        // 旧条目从表里消失。旧 store 目录随即成为「未登记目录」——启动清扫
        // 会把它移进 `_quarantine`，隔离区非空又让引擎 fail-closed，桌面看到
        // 的是「通道正在自恢复」死循环（真机：lk-7c63e095135726e1 因重链变
        // 孤儿 → quarantine-log reason=unregistered_store_dir → 引擎拒绝启动，
        // 手工归档才起来）。官方桌面重链 = 替换账号数据，无孤儿概念：收尾即
        // 归档被取代目录（不删除凭证、不进隔离区），只有真异常残留才落隔离。
        let superseded = self
            .state
            .lock()
            .await
            .add_account_superseding(number.clone(), Some(rel.clone()))
            .map_err(|_| LinkError::Internal)?;
        // 同号重链换新 store 目录：摘除旧常驻管理器（下次发送按新目录重建）。
        self.send_managers.lock().await.remove(&number);
        // 同号重链：摘除旧 MRM 连接槽（R4 槽按账号生命周期；新身份下次发送
        // 惰性重建）。
        self.mrm_pool.remove(&number);
        // 重链 = 新账号身份（新 ACI/新 profile）：能力位缓存一并摘除，
        // 避免旧链接的 profile capabilities 在 TTL 内冒充新账号。
        self.own_capabilities_cache.invalidate(&number);
        // 同号重链：旧链接会话的在途存量下载中止 + 导入终态清缺（终态清除
        // 触发器之三）——旧任务若继续跑会写旧目录、复活旧账号名下的快照面。
        let _ = self.transfer_downloads.cancel(&number);
        self.transfer_progress.clear(&number);
        // transfer 腿启动：严格在上一行清理之后（语句序结构性锁死）——spawn
        // 曾留在 link_flow（先于本清理执行），新任务每次被同号清理确定性误杀
        // （abort 命中），真机四连「链接成功但永久同步中」即此。注册表语义
        // 不变：`history.importCancel` 按账号中止在途下载（transfer 腿）。
        if let Some(transfer) = pending_transfer {
            let handle = tokio::task::spawn_local(download_transfer_archive(
                pending.store_dir.clone(),
                transfer.aci,
                transfer.backup_key,
                transfer.credentials,
                number.clone(),
                self.transfer_progress.clone(),
            ));
            self.transfer_downloads.register(&number, handle);
            if std::env::var("KT_ENGINE_LINK_DEBUG").is_ok() {
                use std::io::Write;
                if let Ok(mut f) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("/tmp/kt-engine-link-debug.log")
                {
                    let _ = writeln!(f, "link_debug: transfer download spawned account={number}");
                }
            }
        }
        // ACI additive 投影（connector 契约 1.142）：链接已成功，注册数据
        // 读取按 best-effort（account_aci）——宁缺勿炸，不连坐链接主结局。
        let aci = account_aci(&pending.store_dir).await;
        self.spawn_pipeline(number.clone(), pending.store_dir).await;
        // 旧 store 归档（顺序结构性锁死）：必须晚于 spawn_pipeline——它在
        // 内部先 stop_pipeline 并等旧管线收口（store drop → SQLite 刷盘），
        // 归档前不再有句柄写旧目录；也晚于 transfer 腿 cancel。仍然引用的
        // 目录（极端：同 dir 被另一号码登记）不动；与本次新 dir 相同的也不动。
        self.archive_superseded_link_dir(superseded, &rel).await;
        Ok(LinkedAccount { number, aci })
    }

    /// 重链收尾归档单点（P0 重链孤儿批）：把被取代的旧 store 目录归档到
    /// `accounts/_archive/<ts>/`，永不删除凭证、永不进隔离区（重链是正常
    /// 生命周期动作，官方桌面同语义：重链即替换账号数据）。
    ///
    /// 零动作闸：无旧条目 / 旧条目无 dir（fake 账号）/ 旧 dir == 新 dir /
    /// 仍有其它注册表条目引用该 dir。归档失败只落 warn：归档不是链接主结局
    /// 的一部分，失败时目录原地保留，下次启动清扫按既有 fail-closed 语义处置
    /// （闸门不放宽）。
    async fn archive_superseded_link_dir(&self, superseded: Option<AccountEntry>, new_dir: &str) {
        let Some(entry) = superseded else {
            return;
        };
        let Some(old_dir) = entry.dir.as_deref() else {
            return;
        };
        if old_dir == new_dir {
            return;
        }
        let still_referenced = {
            let state = self.state.lock().await;
            state
                .accounts()
                .iter()
                .any(|existing| existing.dir.as_deref() == Some(old_dir))
        };
        if still_referenced {
            return;
        }
        let _ = crate::state::archive_superseded_account_dir(&self.data_dir, &entry, now_ms());
    }

    /// listAccounts 读投影（wire additive `aci` 键，connector 契约 1.142
    /// 消费面）：注册表快照后放锁，逐账号 best-effort 读注册数据——sqlite
    /// 打开是盘 IO，不持引擎状态锁做。fake 账号（dir=None）恒无 aci；
    /// 条目整键缺席语义见 account_aci。
    ///
    /// 回填能力门数据面（2026-10-20 批，wire additive `capabilities` 键）：
    /// 真实账号读本机 profile 的 `capabilities.attachmentBackfill`（模块
    /// `own_capabilities`，缓存 + 整批 2s 预算）；fake 账号无 store，形状桩
    /// 投影引擎自声明位。取不到 = 该条目整键缺席，**不**连坐列表主结局。
    pub(crate) async fn list_accounts(&self) -> Vec<Value> {
        let accounts: Vec<AccountEntry> = {
            let state = self.state.lock().await;
            state.accounts().to_vec()
        };
        // 整批预算（逐账号余量递减）：网络拉取只发生在缓存 miss，且不得
        // 把列表应答拖成慢调用（模块纪律）。
        let capabilities_deadline =
            std::time::Instant::now() + own_capabilities::CAPABILITIES_LIST_BUDGET;
        let mut rows = Vec::with_capacity(accounts.len());
        for entry in accounts {
            let mut row = serde_json::Map::with_capacity(3);
            row.insert("number".to_owned(), json!(entry.number));
            if let Some(rel) = &entry.dir {
                let dir = self.data_dir.join(rel);
                if let Some(aci) = account_aci(&dir).await {
                    row.insert("aci".to_owned(), json!(aci));
                }
                // 真实位只在 real 面读（fake 模式即使有 store 目录也不触网）。
                if self.real {
                    if let Some(attachment_backfill) = own_capabilities::attachment_backfill_cached(
                        &self.own_capabilities_cache,
                        &entry.number,
                        &dir,
                        capabilities_deadline.saturating_duration_since(std::time::Instant::now()),
                    )
                    .await
                    {
                        row.insert(
                            "capabilities".to_owned(),
                            own_capabilities::wire_capabilities(attachment_backfill),
                        );
                    }
                }
            }
            if !self.real {
                // fake 形状桩（官方 test/mock 环境回填门恒 true 同向）。
                row.insert(
                    "capabilities".to_owned(),
                    own_capabilities::wire_capabilities(
                        own_capabilities::declared_attachment_backfill(),
                    ),
                );
            }
            rows.push(Value::Object(row));
        }
        rows
    }

    /// transfer_archive 下载进度/终态查询（契约 1.53 §4.38 扩展，connector 的
    /// `history.importStatus` progress 组合面消费）：进程内快照只读，无
    /// 网络、无 store。快照缺席（未开始/已成功收尾/终态已被清除/引擎重启过）
    /// = None，消费方维持无 progress 的诚实投影；`failed` 终态（短错误码）
    /// 保留至清除触发器（begin/取消/重链）。
    pub(crate) fn transfer_archive_snapshot(
        &self,
        account: &str,
    ) -> Option<transfer_progress::TransferSnapshot> {
        self.transfer_progress.snapshot(account)
    }

    pub(crate) async fn delete_account(&self, account: &str) -> std::io::Result<bool> {
        // 取消面：在途导入任务随账号摘除中止（transfer 下载 + 云备份 restore
        // 两腿），进度/终态快照同步清缺——账号没了终态无人消费，半成品随
        // 下面的目录删除一并消失。
        let _ = self.history_import_cancel(account).await;
        // 注册表摘除并取回目录（fake 账号 dir=None）。
        let removed_dir: Option<PathBuf> = {
            let mut state = self.state.lock().await;
            state
                .remove_account(account)?
                .and_then(|entry| entry.dir.map(|rel| self.data_dir.join(rel)))
        };
        let had_dir = removed_dir.is_some();
        // A4 收口顺序（官方 MessageReceiver.stopProcessing → drain 同语义，
        // `ts/textsecure/MessageReceiver.preload.ts:516-547`）：先停接收并等待
        // 任务退出（drop Manager → SQLite store 刷盘），再删 store 目录——
        // 避免在飞投影/落库落在已删除目录上。删除是显式破坏性意图，宽限
        // 超时仍继续（不阻塞连接器）。
        self.stop_pipeline(account).await;
        if let Some(dir) = removed_dir {
            let _ = std::fs::remove_dir_all(&dir);
        }
        // 摘除常驻发送管理器（若有在途发送，guard 释放后管理器随 Arc 落弃）。
        self.send_managers.lock().await.remove(account);
        // MRM 连接槽按账号摘除（R4：其他账号槽不受影响）。
        self.mrm_pool.remove(account);
        // A1 观测行随账号生命周期摘除。
        account_metrics::remove_account(account);
        // F 链路观测簿摘除（账号面随注册表生命周期）。
        self.conversation_state_books
            .lock()
            .expect("conversation state books lock")
            .remove(account);
        // calls.relays 缓存摘除（账号面随注册表生命周期）。
        self.calling_relays_cache.lock().await.remove(account);
        // whitelist 缓存摘除（账号面随注册表生命周期，修1）。
        whitelist_cache_invalidate(&self.whitelist_cache, account);
        // profile capabilities 缓存摘除（回填能力门数据面，2026-10-20 批）。
        self.own_capabilities_cache.invalidate(account);
        Ok(had_dir || !self.real)
    }

    /// §5 快速重试上限：同调用内 1 次、零退避——官方管线层不重试传输
    /// 失败、重试/退避归队列层（conversationJobQueue Fibonacci 分钟级
    /// 退避），引擎只做确定未送达的单次快速重试，退避策略归
    /// connector/desktop。
    const SEND_FAST_RETRY_LIMIT: usize = 1;

    /// 直聊单收件人投递 + 确定未送达快速重试（§5 cause 全覆盖 + 快速重试
    /// 批）：逐收件人失败即该收件人确定未送达（vendored 应答/本地前置面
    /// 唯一决定），重试无重复投递；未知结局（WsTimeout——请求可能已在
    /// 服务器落地）绝不自动重试（connector 契约 An unknown send outcome
    /// must not be retried automatically 同源纪律）。body 按次克隆（prost
    /// 消息，各 attempt 间互不影响）。返回值第二位 = 已备好的 self 同步
    /// 转录腿载荷（修2 send-done 语义批：直聊腿 ack 即完成本函数，转录腿
    /// 由调用方 spawn_local 后台执行，官方 wrapWithSyncMessageSend 同语义；
    /// None = 无转录面/Note-to-Self 臂）。
    async fn deliver_direct_message(
        manager: &mut Manager<SqliteStore, Registered>,
        service_id: ServiceId,
        body: DataMessage,
        timestamp: u64,
        include_profile_key: bool,
    ) -> Result<Option<presage::libsignal_service::content::ContentBody>, SendError> {
        let mut attempt = 0usize;
        loop {
            match manager
                .send_message_with_profile_sharing_split_self(
                    service_id,
                    body.clone(),
                    timestamp,
                    include_profile_key,
                )
                .await
                .map_err(send_error_of)
            {
                Ok(sync_body) => return Ok(sync_body),
                Err(error)
                    if attempt < Self::SEND_FAST_RETRY_LIMIT && error.is_definite_failure() =>
                {
                    attempt += 1;
                    // 日志纪律：只落固定字面量 cause 类（变体名），无收件
                    // 人/号码/内容。
                    tracing::warn!(cause = ?error, "send fast retry after definite failure");
                }
                Err(error) => return Err(error),
            }
        }
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
        // KT (send latency probe): 纯日志零行为变化——lock 段含 db 打开与
        // 账号互斥等待（发送长尾嫌疑面），ts_id 供与 dispatch/vendor 行
        // 对账；正文/号码/群名永不入日志。
        let probe_t0 = std::time::Instant::now();
        let (shared, account_dir) = open_registered_with_dir(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let timestamp = now_ms();
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.lock",
            dur_ms = probe_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
        // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
        // KT (send latency probe): 暗区段探针三之一——whitelist 读面（修1
        // 缓存后命中即近零；cache=hit|miss 固定词，正文/号码不入日志）。
        let probe_whitelist_t0 = std::time::Instant::now();
        let (whitelist, probe_whitelist_hit) =
            profile_sharing_whitelist_probed(self, account).await;
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.whitelist",
            dur_ms = probe_whitelist_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            cache = if probe_whitelist_hit { "hit" } else { "miss" },
            "send-latency-probe",
        );
        // 附件/预览图一次上行（多收件人共享同一批指针）；贴纸图单条上行，
        // 同样多收件人共享指针。caption 位 = story 媒体腿专属（send 面无
        // caption 参数，None 零变化）。上传失败整批确定未投递（先于全部
        // 收件人），AttachmentUpload 族同调用内快速重试 1 次（§5）。
        let probe_upload_t0 = std::time::Instant::now();
        let (pointers, preview_image) = {
            let mut attempt = 0usize;
            loop {
                match upload_attachments_for_send(
                    &manager,
                    attachments,
                    preview.and_then(|item| item.image.as_deref()),
                    None,
                )
                .await
                {
                    Ok(uploaded) => break uploaded,
                    Err(error)
                        if attempt < Self::SEND_FAST_RETRY_LIMIT && error.is_definite_failure() =>
                    {
                        attempt += 1;
                        tracing::warn!(cause = ?error, "send fast retry after definite failure");
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        // KT (send latency probe): 暗区段探针三之二——upload 段恒落（纯文
        // 本 no-op 早退也测，钉死排除法；原 engine.upload 条件行更名）。
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.upload-pre",
            dur_ms = probe_upload_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
        let probe_sticker_t0 = std::time::Instant::now();
        let sticker_pointer = match sticker {
            Some(sticker) => {
                let mut attempt = 0usize;
                loop {
                    match upload_sticker_image(&manager, sticker).await {
                        Ok(pointer) => break Some(pointer),
                        Err(error)
                            if attempt < Self::SEND_FAST_RETRY_LIMIT
                                && error.is_definite_failure() =>
                        {
                            attempt += 1;
                            tracing::warn!(cause = ?error, "send fast retry after definite failure");
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            None => None,
        };
        // KT (send latency probe): 暗区段探针三之三——sticker 段恒落
        //（None 臂 no-op 也测，钉死排除法）。
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.sticker-pre",
            dur_ms = probe_sticker_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
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
                let probe_resolve_t0 = std::time::Instant::now();
                let service_id = resolve_recipient(&manager, number)
                    .await
                    .ok_or(SendError::Internal)?;
                tracing::warn!(
                    target: "send_latency_probe",
                    stage = "engine.resolve",
                    dur_ms = probe_resolve_t0.elapsed().as_millis() as u64,
                    ts_id = timestamp,
                    "send-latency-probe",
                );
                // P9 身份拒发闸：对端换钥未确认 → 拒发（直聊内容发送面统一
                // 判定；整批失败与既有逐收件人失败同语义，compat §3）。
                identity_send_gate(&account_dir, &service_id)?;
                // P9 profileSharing 闸（S3）：会话未同意 profileSharing → 不注入
                // 本机 profile_key（官方 sendNormalMessage.preload.ts:183-185）。
                let include_profile_key = include_profile_key_for(&whitelist, &service_id);
                let probe_build_t0 = std::time::Instant::now();
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
                tracing::warn!(
                    target: "send_latency_probe",
                    stage = "engine.build",
                    dur_ms = probe_build_t0.elapsed().as_millis() as u64,
                    ts_id = timestamp,
                    "send-latency-probe",
                );
                // 直聊投递 + 确定未送达快速重试 1 次（§5；逐收件人失败 =
                // 该收件人确定未送达，重试无重复投递）。探针 outcome 只落
                // ok/err 固定词（内部含 ensure_session/加密/WS/ack 全程，
                // vendor 层再逐段拆）。
                let probe_deliver_t0 = std::time::Instant::now();
                let probe_deliver_result = Self::deliver_direct_message(
                    &mut manager,
                    service_id,
                    body,
                    timestamp,
                    include_profile_key,
                )
                .await;
                tracing::warn!(
                    target: "send_latency_probe",
                    stage = "engine.deliver",
                    dur_ms = probe_deliver_t0.elapsed().as_millis() as u64,
                    ts_id = timestamp,
                    outcome = if probe_deliver_result.is_ok() { "ok" } else { "err" },
                    "send-latency-probe",
                );
                // 修2 send-done 语义批：self 同步转录腿后台化——官方
                // wrapWithSyncMessageSend 语义（send-done = 主投递腿 ack），
                // 转录腿失败只 warn 不上抛（转录丢失官方桌面同样后台化）。
                match probe_deliver_result {
                    Ok(Some(sync_body)) => {
                        spawn_self_sync_leg(
                            self,
                            account,
                            BackgroundSelfLeg::Prepared(sync_body),
                            timestamp,
                        );
                        Ok(())
                    }
                    Ok(None) => Ok(()),
                    Err(error) => Err(error),
                }
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
    /// sendStory 行）。媒体 story 发送面（2026-10-14 批）：`attachment`
    /// Some = 文件 story（data URI 走既有 transit CDN 上行链，指针进
    /// `StoryMessage.fileAttachment`，官方 getStoryMessage fileAttachment
    /// 臂同位），缺席 = text story 现状零变化（两腿互斥闸在 methods 参数
    /// 面）；自同步 `story_message` 载两腿同形状值，恰好一次语义不变。
    /// 群 story 受众腿（2026-10-15 批）：`group` Some = 群受众腿
    ///（stories::send_story_group 官方群 story 行为对齐），与名单腿共存
    /// 官方同构（混合受众 = 两 job 两 sync：名单腿先行，群腿 ts+1 官方
    /// +index+1 同款；recipients 空 = 群单腿），返回值第三位 = 群腿应答面
    /// `{groupId, timestamp}`（缺席 = None）。
    /// 应答指针脸（媒体 story 指针回传批，2026-10-15）：返回值第四位 =
    /// 名单腿媒体 story 上传指针全脸（投影与收投影同一点，严格互逆；
    /// text 腿/失败 = None 键缺席）；群腿指针经第三位 group face 内
    /// `fileAttachment` 键上抛（混合受众两腿独立上传指针互异，分键 §5）。
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
        attachment: Option<&str>,
        caption: Option<&str>,
        preview_image: Option<&str>,
        preview_url: Option<String>,
        preview_title: Option<String>,
        preview_date: Option<u64>,
        group: Option<&str>,
    ) -> Result<(u64, Vec<Value>, Option<Value>, Option<Value>), SendError> {
        // 名单受众腿（recipients 空 = 群单腿跳过名单腿；群缺席时空批由
        // methods 参数闸整拒，不达此处——现状口径零变化）。
        let (timestamp, results, pointer_face) = if recipients.is_empty() {
            (now_ms(), Vec::new(), None)
        } else {
            stories::send_story(
                self,
                account,
                recipients,
                distribution_list_id,
                allows_replies,
                text,
                text_style,
                body_ranges,
                attachment,
                caption,
                preview_image,
                preview_url.clone(),
                preview_title.clone(),
                preview_date,
            )
            .await?
        };
        // 群受众腿（官方混合受众两 job 两 sync 同构；群 ts = 名单腿 ts+1
        // 保跨腿唯一——官方 groupTimestamp = timestamp + index + 1 同款；
        // 群单腿 = 自取 now）。
        let group_face = match group {
            Some(group_id) => {
                let (_, face) = stories::send_story_group(
                    self,
                    account,
                    group_id,
                    if recipients.is_empty() {
                        timestamp
                    } else {
                        timestamp + 1
                    },
                    text,
                    text_style,
                    body_ranges,
                    attachment,
                    caption,
                    preview_image,
                    preview_url,
                    preview_title,
                    preview_date,
                )
                .await?;
                Some(face)
            }
            None => None,
        };
        Ok((timestamp, results, group_face, pointer_face))
    }

    /// deleteStoryForEveryone（story 删除扇出批 S5，KT 扩展方法）：官方
    /// sendDeleteStoryForEveryone 行为对齐——名单 story 逐收件人 DOE 腿走
    /// story 通道 + 恰好一次自同步 storyMessageRecipients 扇出面（编排
    /// 全量在 stories 模块，compat §3/§5；群 story 腿 = 普通 group DOE，
    /// sendRemoteDelete 覆盖，v1 边界不做）。
    pub(crate) async fn send_story_delete_for_everyone(
        self: &Arc<Self>,
        account: &str,
        target_timestamp: u64,
        recipients: &[String],
        updated: &[stories::SendStoryRecipientUpdate],
    ) -> Result<u64, SendError> {
        stories::send_story_delete_for_everyone(
            self,
            account,
            target_timestamp,
            recipients,
            updated,
        )
        .await
    }

    /// sendStoryReaction（story 反应发送批 S4，KT 扩展方法）：官方
    /// reactToStory 行为对齐（stories.preload.ts:556 →
    /// enqueueReactionForSend → sendNormalMessage getMessageSendData）——
    /// 被回应 story 的锚（作者 ACI + story 发送时刻）同时进
    /// `Reaction.targetAuthor/targetSentTimestamp` 与
    /// `StoryContext.author/sentTimestamp` 两位，单收件人直聊腿发往
    /// story 作者会话（官方 1:1 story reaction targetConversation 同位；
    /// 群 story 腿 = `send_story_reaction_group`，E-S7r 批，compat §5）。
    /// remove 官方恒 false
    /// （sendNormalMessage reactionForSend 同位硬编码），方法面不带参。
    /// 官方发送前 profileSharing 置位（enqueueReactionForSend:72-82）由
    /// P9 白名单闸同语义承接；sync 面 = 普通 sent 转录（presage fork
    /// send_message 自动产出），无独立回执面（核实记档 compat §5）。
    pub(crate) async fn send_story_reaction(
        self: &Arc<Self>,
        account: &str,
        story_author: &str,
        story_timestamp: u64,
        emoji: &str,
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
        // 锚作者（targetAuthorAci/storyContext.authorAci 同一 ACI）：本机号
        // 走注册数据、contacts E.164 命中、UUID 直通（sendReaction 同链）；
        // 都未命中按内部失败，不透传号码。
        let author_aci = resolve_author_aci(manager.store(), story_author)
            .await
            .ok_or(SendError::Internal)?;
        // 目的地 = story 作者会话（号码/UUID 双形态收敛同一 ServiceId）。
        let service_id = resolve_recipient(&manager, story_author)
            .await
            .ok_or(SendError::Internal)?;
        // P9 身份拒发闸（story 反应同为直聊内容发送）。
        identity_send_gate(&account_dir, &service_id)?;
        // P9 profileSharing 闸（S3）：未同意 → 不注入本机 profile_key。
        let include_profile_key = include_profile_key_for(&whitelist, &service_id);
        let body =
            build_story_reaction_data_message(&author_aci, story_timestamp, emoji, timestamp);
        manager
            .send_message_with_profile_sharing(service_id, body, timestamp, include_profile_key)
            .await
            .map_err(send_error_of)?;
        Ok(())
    }

    /// sendStoryReaction 群腿（群 story 反应发送面批 E-S7r，KT 扩展，compat
    /// §5 群 story 反应发送面）：官方群 story 反应走普通群通道——
    /// enqueueReactionForSend.preload.ts:84-91 群 story targetConversation =
    /// 群本身（仅 1:1 story 才换作者会话），sendNormalMessage 群臂
    /// sendToGroup 载 DataMessage{groupV2, reaction, storyContext}
    ///（reactionForSend remove 恒 false :711-723；toProto 三键共存
    /// SendMessage.preload.ts :642/:567-579/:663-679），非
    /// Content.storyMessage（S4 批「story 通道 groupV2 形态」措辞失真随本
    /// 批订正）。三键共存全走共享群发送路径单点：`open_group_sender`
    ///（32B 解码 fail-closed + terminated 硬拒）+ `dispatch_group`
    ///（announcementsOnly 非 admin 拒 group_mrm:225-227 同源 + group_v2
    /// 自动注入 {masterKey, revision, groupChange: None} :211-216 同源，
    /// sendTextGroup/sendReactionGroup 同款——MRM 主路 + vendored failover
    /// 及 Sent 转录自带）；reaction/storyContext 两键复用
    /// build_story_reaction_data_message（1:1 腿构造单点，双锚同值、remove
    /// 恒 Some(false) 官方同位——群腿无 remove 参）。作者 ACI 解析与 1:1
    /// 腿同链同严（本机号→contacts E.164→UUID 直通，不可解析 Internal 不
    /// 透传号码）；群腿不走直聊身份拒发闸/profileSharing 白名单（群发送面
    /// 同口径，send_reaction_group 同构）。应答 = {groupId, timestamp 群腿
    /// 时刻}（fake 同形状；sync 面 = 共享群发送路径 Sent 转录，无独立面）。
    pub(crate) async fn send_story_reaction_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        story_author: &str,
        story_timestamp: u64,
        emoji: &str,
    ) -> Result<Value, SendError> {
        if !self.real {
            let timestamp = now_ms();
            return Ok(json!({ "groupId": group_id, "timestamp": timestamp }));
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        // 被回应 story 作者锚（reaction.targetAuthorAci/storyContext.
        // authorAci 同一 ACI）：解析链与 1:1 腿同源同严。
        let author_aci = resolve_author_aci(manager.store(), story_author)
            .await
            .ok_or(SendError::Internal)?;
        let body =
            build_story_reaction_data_message(&author_aci, story_timestamp, emoji, timestamp);
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await?;
        Ok(json!({ "groupId": group_id, "timestamp": timestamp }))
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
        .map(|_| ())
    }

    /// sendPollCreate（群组）：官方 DataMessage.pollCreate=24（Polls 发送
    /// 批，窗D A7）。官方桌面仅群聊形态（enqueuePollCreateForSend.dom.ts
    /// 1:1 走 isPollSend1to1Enabled feature flag，默认关——引擎按默认面
    /// 群聊 only，无 1:1 形态；§5 偏差记档）。
    pub(crate) async fn send_poll_create_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        question: &str,
        options: &[String],
        allow_multiple: bool,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let body = DataMessage {
            timestamp: Some(timestamp),
            poll_create: Some(poll_create_proto(question, options, allow_multiple)),
            ..DataMessage::default()
        };
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
        .map(|_| ())
    }

    /// sendPollVote（群组）：官方 DataMessage.pollVote=26。voteCount 由调用
    /// 方按官方 enqueuePollVoteForSend 语义（自身历史票 max+1，首票=1）计
    /// 算后传入——引擎不持投票历史，缺省 1（§5 偏差记档）。
    pub(crate) async fn send_poll_vote_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        target_author: &str,
        target_timestamp: u64,
        option_indexes: &[u32],
        vote_count: u32,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        // 官方语义 targetAuthor 一律 ACI（被投票 poll 的作者）；群内兼容号
        // 码/UUID 解析，未命中原样透传（sendAdminDelete 群组同口径）。
        let target_aci = resolve_author_aci(manager.store(), target_author)
            .await
            .unwrap_or_else(|| target_author.trim().to_owned());
        let aci_bytes = aci_to_bytes(&target_aci).ok_or(SendError::Internal)?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            poll_vote: Some(poll_vote_proto(
                aci_bytes,
                target_timestamp,
                option_indexes,
                vote_count,
            )),
            ..DataMessage::default()
        };
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
        .map(|_| ())
    }

    /// sendPollTerminate（群组）：官方 DataMessage.pollTerminate=25（终止
    /// poll）；GV1 拒发由群腿形状闸天然覆盖（decode_group_id 只收 GV2
    /// 32B master key）。
    pub(crate) async fn send_poll_terminate_group(
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
            poll_terminate: Some(poll_terminate_proto(target_timestamp)),
            ..DataMessage::default()
        };
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
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
        // KT (typing self gate): typing is never sent to our own other
        // devices (official `isMe` early return in sendTypingMessage) — a
        // TypingMessage has no sent-transcript form, so a self target would
        // land in the vendored self-None path and fail this RPC (-32603).
        let own = ServiceId::from(manager.registration_data().service_ids.aci());
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            if service_id == own {
                continue;
            }
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

    /// requestAttachmentBackfill（附件回填请求写面，KT 扩展方法，官方
    /// AttachmentBackfill.request 发送面对位，jobs/helpers/attachmentBackfill
    /// .preload.ts:77-109 + SendMessage.getAttachmentBackfillSyncMessage
    /// .preload.ts:1987-2018）：本地附件字节缺失且无可下载指针面时，向自己
    /// ACI 发 SyncMessage.content oneof attachmentBackfillRequest=24
    /// {targetMessage: AddressableMessage{author, sentTimestamp},
    /// targetConversation: ConversationIdentifier}——主设备收请求后重上传并
    /// 回 attachmentBackfillResponse（收投影臂 compat §5
    /// attachmentBackfillResponse 块，engine 侧 2026-10-17 已落）。会话腿 =
    /// MessageRequestTarget 同款两腿恰一（Thread = resolve_recipient 号码/
    /// UUID 双形态 → ACI binary 现行面；Group = 32B master key 原样）；作者 =
    /// resolve_author_aci 双形态解析（mark_read_state 同口径，不可解析
    /// fail-closed 整拒）。padded sync message；presage self-send 通路与
    /// send_message_request_response 同款。写结局即返回值，未知结局不重试。
    pub(crate) async fn request_attachment_backfill(
        self: &Arc<Self>,
        account: &str,
        target: MessageRequestTarget,
        author: &str,
        sent_timestamp: u64,
    ) -> Result<(), SendError> {
        if !self.real {
            return Ok(());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let author_aci = resolve_author_aci(manager.store(), author)
            .await
            .ok_or(SendError::Internal)?;
        let author_sid =
            presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(
                &author_aci,
            )
            .ok_or(SendError::Internal)?;
        let body = ContentBody::SynchronizeMessage(build_attachment_backfill_sync(
            &target,
            &author_sid,
            sent_timestamp,
        ));
        let own_aci = presage::libsignal_service::protocol::ServiceId::Aci(
            manager.registration_data().service_ids.aci.into(),
        );
        let envelope_ts = now_ms();
        manager
            .send_message(own_aci, body, envelope_ts)
            .await
            .map_err(send_error_of)?;
        Ok(())
    }

    /// lookupOutgoingDispatch（unknown 出站行引擎真相对账只读面，KT 扩展，
    /// 官方 outbox 恢复语义对位，compat §5 对账批）：引擎库
    /// thread_messages 是「引擎是否真的派发过」的本地真相（出站行一律在
    /// 派发调用成功返回后才落库——直聊腿 registered.rs:1694-1750 服务器
    /// ack 后 save_message、群腿 :2013-2068 逐成员派发尝试后转录必落），
    /// 本面把它以布尔面透出给 connector 的启动对账臂（connector 契约
    /// 1.179 / §4.179）：destination 的 thread 内取 sender=本机 ACI 且 ts
    /// 落 `[sent_timestamp ± tolerance_ms]` 的出站行，多条命中取 |Δts|
    /// 最小者（timestamp-only 配对 = 官方 receipt 配对同粒度）。应答只含
    /// 布尔与命中行落库 ts——正文/号码/群名永不出面（内容红线）；查证走
    /// presage `messages(thread, range)` trait 原语内存内过滤，零 vendor
    /// 改动。fake：`{dispatched: false}` 确定形状桩。
    pub(crate) async fn lookup_outgoing_dispatch(
        self: &Arc<Self>,
        account: &str,
        target: MessageRequestTarget,
        sent_timestamp: u64,
        tolerance_ms: u64,
    ) -> Result<OutgoingDispatchLookup, SendError> {
        if !self.real {
            return Ok(OutgoingDispatchLookup {
                dispatched: false,
                server_timestamp: None,
            });
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let manager = lock_registered_for_send(&shared).await;
        let thread = match target {
            // thread 腿 = resolve_recipient 双形态解析（号码/UUID）——与发送
            // 面同一解析单点，保证对账命中的正是发送面当初落行的同一
            // Thread::Contact；不可解析（contacts 冷启动窗）fail-closed。
            MessageRequestTarget::Thread(text) => {
                let service_id = resolve_recipient(&manager, &text)
                    .await
                    .ok_or(SendError::Internal)?;
                presage::store::Thread::Contact(service_id)
            }
            // 群腿 = 32B master key（dispatch 层 decode_group_id 已闸形状，
            // 此处仅类型收敛）——与群发送面同一 Thread::Group 落行键。
            MessageRequestTarget::Group(key) => {
                let master_key: [u8; 32] = key.try_into().map_err(|_| SendError::Internal)?;
                presage::store::Thread::Group(master_key)
            }
        };
        let own = ServiceId::Aci(manager.registration_data().service_ids.aci.into());
        let lo = sent_timestamp.saturating_sub(tolerance_ms);
        let hi = sent_timestamp.saturating_add(tolerance_ms);
        let rows = manager
            .store()
            .messages(&thread, lo..=hi)
            .await
            .map_err(|_| SendError::Internal)?;
        Ok(nearest_outgoing_dispatch(
            rows.filter_map(Result::ok),
            &own,
            sent_timestamp,
        ))
    }

    /// deleteForMeConversation（P15c 项3，官方桌面 conversations.preload.ts
    /// deleteConversation 发送面同构）：会话级「为我删除」上行——全量清空臂
    /// （delete-conversation：mostRecentMessages 全量清空语义 +
    /// isFullDelete:true）/ 仅本地臂（delete-local-conversation）双变体，
    /// 向自己 ACI 发 SyncMessage.deleteForMe（content oneof deleteForMe=22，
    /// padded sync message；构帧单点 build_delete_for_me_conversation_sync）。
    /// desktop 上行接线归后续批。
    pub(crate) async fn send_delete_for_me_conversation(
        self: &Arc<Self>,
        account: &str,
        conversation: DeleteForMeConversation,
        local_only: bool,
        most_recent: Vec<DeleteForMeEntry>,
        most_recent_non_expiring: Vec<DeleteForMeEntry>,
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
        let body = ContentBody::SynchronizeMessage(build_delete_for_me_conversation_sync(
            &conversation,
            local_only,
            &most_recent,
            &most_recent_non_expiring,
        ));
        let envelope_ts = now_ms();
        manager
            .send_message(own_aci, body, envelope_ts)
            .await
            .map_err(send_error_of)?;
        Ok(())
    }

    /// deleteForMeMessages（leg 3/3，官方 getDeleteForMeSyncMessage
    /// messageDeletes 发送面同构，SendMessage.preload.ts:1840-1936）：
    /// 消息级「为我删除」上行——把一个会话内的若干消息行（author +
    /// sentTimestamp 定位）同步给本账号其他设备，收方按官方
    /// MessageReceiver.preload.ts:3609-3657 逐条走 delete-message。
    /// 向自己 ACI 发 SyncMessage.deleteForMe.messageDeletes[0]
    /// （content oneof deleteForMe=22，padded sync message；构帧单点
    /// build_delete_for_me_messages_sync）。fake 成功早退（形状桩）。
    pub(crate) async fn send_delete_for_me_messages(
        self: &Arc<Self>,
        account: &str,
        conversation: DeleteForMeConversation,
        entries: Vec<DeleteForMeEntry>,
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
        let body = ContentBody::SynchronizeMessage(build_delete_for_me_messages_sync(
            &conversation,
            &entries,
        ));
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

    /// sendCallEvent（通话事件出向同步批，KT 扩展方法）：官方桌面
    /// updateRemoteCallHistory/getDeleteCallEvent（SendMessage.preload.ts:
    /// 2055 getDeleteCallEvent 单帧作业 + callDisposition.preload.ts:376
    /// getProtoForCallHistory 构造）同构——每事件即发，`SyncMessage.callEvent`
    ///（content oneof 字段 19）padded sync message 向自己 ACI 单发（官方
    /// sendIndividualProto(ourAci) urgent:false Resendable 同位；presage
    /// self-send 通路 sendViewOnceOpen 先例已验证，compat §5 E4；钉定 rev
    /// 同内容二次转录重发怪癖同面，接收端幂等）。无 sent-transcript、无
    /// 群/设备定向位（CallEvent proto 六字段封闭，官方 wire 同形）。直聊腿
    /// 号码/service-id 双形态解析（resolve_recipient，send 系同口径；PNI 形
    /// 服务 id 原样 16B 直写——CallEvent.conversationId 线面无 ACI/PNI 语义
    /// 位，收端 16B → UUID 串归一）；无身份拒发闸（自发同步面，对端加密
    /// 不涉）。callId 缺席仅 ad-hoc 腿放行（参数层闸），timestamp 缺省 =
    /// 信封时刻（官方调用方 Date.now() 同位）。返回信封 timestamp
    ///（sendCallMessage 同款）。触发时机归事件源（connector/sidecar 每事件
    /// 即调，引擎不做本地通话态）。
    pub(crate) async fn send_call_event(
        self: &Arc<Self>,
        account: &str,
        event: SendCallEvent,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let shared = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = lock_registered_for_send(&shared).await;
        let conversation_bytes = match &event.conversation {
            CallEventConversation::Direct(recipient) => {
                let service_id = resolve_recipient(&manager, recipient)
                    .await
                    .ok_or(SendError::Internal)?;
                aci_to_bytes(&service_id.service_id_string()).ok_or(SendError::Internal)?
            }
            CallEventConversation::Group(bytes) | CallEventConversation::AdHoc(bytes) => {
                bytes.clone()
            }
        };
        let own_aci = ServiceId::Aci(manager.registration_data().service_ids.aci.into());
        let body = ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
            content: Some(presage::proto::sync_message::Content::CallEvent(
                build_call_event(&event, conversation_bytes),
            )),
            ..presage::proto::SyncMessage::with_padding(&mut rng())
        });
        let timestamp = now_ms();
        manager
            .send_message(own_aci, body, timestamp)
            .await
            .map_err(send_error_of)?;
        Ok(timestamp)
    }

    /// sendContact 名片 DataMessage 组装单点（窗D B11）：avatar data URI
    ///（若有）随批 CDN 上行取回指针（官方 EmbeddedContactWithUploadedAvatar
    /// 上传面同构，send 附件面同 upload_attachments_for_send 原语），失败
    /// 整体 fail-closed（半发名片比拒绝更糟）；contact_proto 单点投影，
    /// DataMessage.contact=9 单条目（官方 sendMessage 面单条目序列化同位，
    /// body/attachments 恒空——名片无正文位）。
    async fn build_contact_data_message(
        manager: &Manager<SqliteStore, Registered>,
        payload: &SendContact,
        timestamp: u64,
    ) -> Result<DataMessage, SendError> {
        let avatar = match payload.avatar.as_deref() {
            Some(uri) => {
                let (pointers, _) =
                    upload_attachments_for_send(manager, &[uri.to_owned()], None, None).await?;
                Some(pointers.into_iter().next().ok_or(SendError::Internal)?)
            }
            None => None,
        };
        Ok(DataMessage {
            timestamp: Some(timestamp),
            contact: vec![contact_proto(payload, avatar)],
            ..DataMessage::default()
        })
    }

    /// sendContact 1:1 臂（名片发送臂，窗D B11）：官方 sendMessage 面同一
    /// DataMessage 通路。收件人号码/UUID 双形态解析（send 系同口径）+ P9
    /// 身份拒发闸（名片同为内容面）；avatar 上行失败整体拒。返回信封
    /// timestamp。
    pub(crate) async fn send_contact(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        payload: &SendContact,
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
        // P9 身份拒发闸（直聊内容面同纪律）。
        identity_send_gate(&account_dir, &service_id)?;
        let timestamp = now_ms();
        let body = Self::build_contact_data_message(&manager, payload, timestamp).await?;
        manager
            .send_message(service_id, body, timestamp)
            .await
            .map_err(send_error_of)?;
        Ok(timestamp)
    }

    /// sendContact 群臂（窗D B11 同批）：官方名片群发同 DataMessage 通路，
    /// dispatch_group MRM 主路径（poll 群臂同款——open_group_sender
    /// terminated 硬拒 + announcements 闸 + sender-key 主路径 vendored
    /// 逐收件人 failover）。群腿 outcomes 折叠（sendCallMessage 群臂同口径，
    /// 应答恒 {timestamp}）。
    pub(crate) async fn send_contact_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        payload: &SendContact,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let body = Self::build_contact_data_message(&manager, payload, timestamp).await?;
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
        .map(|_| ())
    }

    /// listContacts：presage store 已同步的联系人投影（手机端链接后自动推送）。
    /// profile 拆名面（2026-10-16 profile 名字刷新批，wire additive，§5 块）：
    /// 条目 additive `profile:{givenName?, familyName?}`——数据源 = profiles
    /// 表（`profiles.refreshContacts` 拉取落库面；键形状与 signal-cli 同形，
    /// connector supervisor 消费腿同形状）；名字键缺席纪律见
    /// `project_contact_entry`（no-null：非空才出现）。
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
        let mut contact_uuids = HashSet::new();
        for contact in contacts.filter_map(Result::ok) {
            contact_uuids.insert(contact.uuid);
            let number = contact
                .phone_number
                .as_ref()
                .map(e164_string)
                .unwrap_or_else(|| contact.uuid.to_string());
            let profile = profile_name_material(manager.store(), contact.uuid).await;
            let profile = profile
                .as_ref()
                .map(|(given, family)| (given.as_str(), family.as_deref()));
            out.push(project_contact_entry(&number, &contact.name, profile));
        }
        // additive 投影（2026-10-23 目标集扩展批，台账 §29.4 剩余项②）：
        // profiles 表有名字材料的非通讯录对端作为 contact 条目（uuid +
        // profile 名字；手机号/通讯录名诚实缺席）。§4.37 aci-only 不落行
        // 纪律的 additive 裁定：既有 contacts 行语义零变化，本面只增行；
        // 枚举失败退 contacts-only（读面 fail-open 不炸）。
        out.extend(non_contact_profile_entries(manager.store(), &contact_uuids).await);
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
        // Internal 来源静态标注（2026-10-16 listGroups Internal 归因批，
        // 活体裁决版）：本函数 Err 出口仅两条——`open_registered` 失败
        // （UnknownAccount，冷路径含 open_store+load_registered，毫秒级）
        // 与本臂（groups() sqlx 读失败，Internal）。生产实证（engine+
        // connector 双侧日志 07:37-07:39 窗全部 duration_ms=0、open_store
        // 零失败行）排除本函数两条臂；探针部署后活体复现（08:27 桌面
        // 进入 Signal 工作区）裁决失败臂 = dispatch 层 groupId 过滤
        // `stage="filter_key_len"`——**groupId 是合法标准 base64 但解码
        // 后 ≠32 字节**（桌面 SignalWorkspace.refreshSelectedGroupRoster
        // 把会话 conversationId（sha256 hex 40 字符 → base64 解 30B）误
        // 当 groupKey 传 groups.get，connector 透传；引擎 fail-closed 契
        // 约不变，修复面在桌面仓）。本臂由读面 Err 变体探针覆盖，不双写。
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
        // KT (send latency probe): 群腿两行——open_group_sender（群钥/凭据
        // 准备）+账号互斥为 lock 段，dispatch_group（MRM/逐收件人）为
        // deliver 段；纯日志零行为变化。
        let probe_t0 = std::time::Instant::now();
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.lock-group",
            dur_ms = probe_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
        let (pointers, preview_image) = upload_attachments_for_send(
            &manager,
            attachments,
            preview.and_then(|item| item.image.as_deref()),
            None,
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
        let probe_deliver_t0 = std::time::Instant::now();
        // 修2 send-done 语义批：send 文本群面 = Deferred——MRM/逐收件人主
        // 投递腿 ack 即完成 deliver-group（send-done），self 同步转录腿
        // spawn_local 后台执行（官方 wrapWithSyncMessageSend 语义对齐）。
        let outcomes = dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Deferred(account),
        )
        .await?;
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.deliver-group",
            dur_ms = probe_deliver_t0.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
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
        dispatch_group(
            self,
            account,
            &mut manager,
            group_id,
            body,
            timestamp,
            GroupSelfSync::Blocking,
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn send_typing_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        stop: bool,
    ) -> Result<TypingGroupSendOutcome, SendError> {
        if !self.real {
            // fake 形状桩：成功面（compat §6；无 store 无群态，失败显形
            // 面只在 real 生效）。
            return Ok(TypingGroupSendOutcome::Ok);
        }
        let (shared, timestamp) = self.open_group_sender(account, group_id).await?;
        let mut manager = lock_registered_for_send(&shared).await;
        let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
        let body = ContentBody::TypingMessage(typing_group_body(&master_key, timestamp, stop));
        // 失败显形批（2026-10-16，compat §3/§5）：不再丢弃逐收件人
        // outcomes（此前 `.map(|_| ())` 整体丢弃——成员全失败也回 `{}` 静默
        // 成功）；空 outcomes = 零收件人（群成员过滤自身后为空）同转
        // NoRecipients 确定性失败；其余分类归 classify 单点（全败 =
        // AllFailed 失败面；部分失败 warn 计数后按成功面应答）。
        let outcomes = manager
            .send_message_to_group(&master_key, body, timestamp)
            .await
            .map_err(send_error_of)?;
        if outcomes.is_empty() {
            return Err(SendError::NoRecipients);
        }
        Ok(classify_typing_group_outcomes(outcomes))
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
    async fn send_target_terminated(self: &Arc<Self>, account: &str, group_id: &str) -> bool {
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

    /// migrateGroup（GV1 群迁移 GV2，KT 扩展方法，官方
    /// initiateMigrationToGroupV2 行为对齐）：GV1 群 id 字节 HKDF 派生新
    /// master key + createGroup 共用主体（成员分流/avatar 上行/初始
    /// proto/`PUT v1/groups`）+ 已迁移兜底（读面探测诚实返回既有群态）。
    /// fake 模式形状桩（随机 master key 标准 base64 + revision 0，不触网；
    /// avatar data URI 臂同 createGroup fake 纪律 = AvatarUpload 错误面）。
    pub(crate) async fn migrate_group(
        self: &Arc<Self>,
        account: &str,
        legacy_group_id: &[u8],
        title: Option<&str>,
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
        groups::migrate_group(
            &context,
            legacy_group_id,
            title,
            expiration_secs,
            avatar,
            members,
        )
        .await
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
        let group = groups::fetch_group_state(&context, master_key, "invite_link").await?;
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

    /// calls.relays 服务腿：TTL 内缓存直读（零网络，官方 TTL 语义），过期
    /// /缺席触发认证重拉（成功非空覆盖 / 空应答保旧 / 失败保旧 + 记因，
    /// calling_relays::advance_cache 单点），401/403 归解绑诚实错误面，
    /// 网络失败不连坐（空态或旧值 + lastError 如实应答）。
    pub(crate) async fn calling_relays(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<calling_relays::RelaysFace, CallingRelaysError> {
        let now = now_ms();
        if !self.real {
            // fake：无网络无账号面，进程真实态 = 空态（空 relays + stale），
            // 形状与 real 空态同构，契约测试可钉。
            return Ok(calling_relays::face_from_cache(None, now, None));
        }
        {
            let cache = self.calling_relays_cache.lock().await;
            if let Some(entry) = cache.get(account) {
                if calling_relays::cache_fresh(entry, now) {
                    return Ok(calling_relays::face_from_cache(Some(entry), now, None));
                }
            }
        }
        let context = storage_context(self, account)
            .await
            .map_err(|_| CallingRelaysError::UnknownAccount)?;
        let outcome = match calling_relays::fetch_relays(&context.push).await {
            Ok(doc) => calling_relays::RefreshOutcome::Fetched(doc),
            Err(calling_relays::RelaysError::Unauthorized) => {
                return Err(CallingRelaysError::Unbound)
            }
            Err(error) => calling_relays::RefreshOutcome::Failed(error.to_string()),
        };
        let mut cache = self.calling_relays_cache.lock().await;
        let entry = calling_relays::advance_cache(cache.get(account).cloned(), &outcome, now);
        let empty_error = match &outcome {
            calling_relays::RefreshOutcome::Failed(reason) if entry.is_none() => {
                Some(reason.clone())
            }
            _ => None,
        };
        let face = calling_relays::face_from_cache(entry.as_ref(), now, empty_error);
        match entry {
            Some(entry) => {
                cache.insert(account.to_owned(), entry);
            }
            None => {
                cache.remove(account);
            }
        }
        Ok(face)
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

    /// getBlockedConversations（P5 扩面 + 窗D B7 blockedAt 透出）：读
    /// Storage Service 全部 Contact/GroupV1/GroupV2 记录的 blocked 位 →
    /// 引擎会话标识 + blockedAtTimestamp 投影（存储序；形状与置顶条目同
    /// 构，只读面不落 store）。
    pub(crate) async fn blocked_conversations(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<storage_sync::BlockedConversation>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_sync::read_blocked_conversations(&context).await
    }

    /// storage.mergeContacts（窗D A4）：Storage Service ContactRecord 全量
    /// merge/发现面——identityKey 灌 presage 身份库（官方 updateIdentity-
    /// AfterSync 同构）+ identityState 落 sidecar 信任态 + profileKey 补缺 +
    /// contacts 建档，逐条汇总返回。幂等：同钥重跑全 no-op。
    /// F 链路：merge 读到的会话记录随行过观测簿，真实翻转合成一条
    /// `receive.conversationState` 通知（storage_watch 契约）。
    pub(crate) async fn merge_storage_contacts(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<storage_merge::MergeOutcome, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_merge::MergeOutcome::default());
        }
        let context = storage_context(self, account).await?;
        let (outcome, observed) = storage_merge::merge_contacts(context, now_ms()).await?;
        self.publish_conversation_state_observations(account, observed)
            .await;
        Ok(outcome)
    }

    /// storage.importGroups（窗D A5）：manifest GROUPV2 masterKey → 群态
    /// 拉取 + presage store 落库（listGroups/open_group_sender 即刻可用）。
    /// fake 模式零汇总早退（与 mergeContacts 同纪律）。F 链路观测随行
    /// （GV2 族观测腿，merge_storage_contacts 同位）。
    pub(crate) async fn import_storage_groups(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<storage_merge::GroupImportOutcome, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_merge::GroupImportOutcome::default());
        }
        // P1 群状态链打点（§29.18）：链入口行——登录窗 manifest 自动拉、
        // fetchLatest 消费臂、connector 显式 RPC 三链都经本单点。有入口行
        // 才谈得上「链跑没跑」；无入口行 = 消费方从未调用（诊断分叉点）。
        tracing::warn!(
            target: "engine.groupstate",
            stage = "import_groups_enter",
            account = %crate::methods::probe_account8(Some(account)),
            "group state import leg entered"
        );
        let context = storage_context(self, account).await?;
        let result = storage_merge::import_groups(context).await;
        if result.is_err() {
            tracing::warn!(
                target: "engine.groupstate",
                stage = "import_groups_error",
                account = %crate::methods::probe_account8(Some(account)),
                "group state import leg failed"
            );
        }
        let (outcome, observed) = result?;
        self.publish_conversation_state_observations(account, observed)
            .await;
        Ok(outcome)
    }

    /// F 链路观测发布（merge 两族共用单点）：观测批过 per-account 簿差分，
    /// 真实翻转非空才合成一条 `receive.conversationState` 通知（批内合流
    /// + 批帽截断在 storage_watch 内；簿锁内无 await/无 notify）。
    async fn publish_conversation_state_observations(
        &self,
        account: &str,
        observed: Vec<(
            storage_sync::EngineConversationId,
            storage_sync::ConversationState,
        )>,
    ) {
        if observed.is_empty() {
            return;
        }
        // 入站 merge 路过的 Contact 记录（修1）：远端（手机端）whitelist 位
        // 翻转的引擎可见面，缓存同步失效（TTL 兜残余窗口）。
        if observed
            .iter()
            .any(|(id, _)| matches!(id, storage_sync::EngineConversationId::Contact(_)))
        {
            whitelist_cache_invalidate(&self.whitelist_cache, account);
        }
        let flips = {
            let mut books = self
                .conversation_state_books
                .lock()
                .expect("conversation state books lock");
            storage_watch::observe_batch(books.entry(account.to_owned()).or_default(), &observed)
        };
        if flips.is_empty() {
            return;
        }
        self.notify(
            account,
            storage_watch::conversation_state_notification(account, &flips),
        )
        .await;
    }

    /// F 链路消费方已知态登记（防回声/防冗余单点）：本机写后
    /// （setConversationState 与 storage.write 三类记录臂）与读投影应答
    /// （getConversationState）登记写后/读出态——簿语义 = 「消费方最后
    /// 被告知的态」，本机动作不再随下次 merge 差分出通知。
    fn note_conversation_state_known(
        &self,
        account: &str,
        canonical: storage_sync::EngineConversationId,
        state: storage_sync::ConversationState,
    ) {
        self.conversation_state_books
            .lock()
            .expect("conversation state books lock")
            .entry(account.to_owned())
            .or_default()
            .note_known(canonical, state);
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
    ) -> Result<Vec<storage_sync::BlockedConversation>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        let result = storage_sync::write_blocked(&context, &conversation, blocked, now_ms()).await;
        // Contact 族拉黑读改写同携全记录重写（修1）：写成功即失效缓存。
        if result.is_ok() && matches!(conversation, storage_sync::EngineConversationId::Contact(_))
        {
            whitelist_cache_invalidate(&self.whitelist_cache, account);
        }
        result
    }

    /// getConversationState（P15b 项2）：读 Storage Service 目标会话记录
    /// 三 flag（mutedUntilTimestamp/archived/markedUnread）→ 读投影；目标
    /// 记录缺席 / 无 storage 状态 = 全缺省（诚实幂等）。fake 模式缺省桩。
    /// F 链路：读出态登记观测簿（读投影 = 消费方已知态，防冗余通知）。
    pub(crate) async fn conversation_state(
        self: &Arc<Self>,
        account: &str,
        conversation: storage_sync::EngineConversationId,
    ) -> Result<storage_sync::ConversationState, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_sync::ConversationState::default());
        }
        let context = storage_context(self, account).await?;
        let (canonical, state) =
            storage_sync::read_conversation_state(&context, &conversation).await?;
        self.note_conversation_state_known(account, canonical, state);
        Ok(state)
    }

    /// setConversationState（P15b 项2）：三 flag 部分写意图（缺席位不动）
    /// → Storage Service 目标会话记录读改写 + 409 冲突重放；返回写后三
    /// flag（官方 upload 成功语义 = 本地写后面为准）。fake 模式缺省态叠
    /// 意图回显。F 链路：写后态登记观测簿（写方已知，防回声）。
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
        let (canonical, written) =
            storage_sync::write_conversation_state(&context, &conversation, intent).await?;
        // Contact 族读改写携全记录重写（whitelist 位随读值回写，修1）：缓存
        // 同步失效，与写面真相对齐。
        if matches!(canonical, storage_sync::EngineConversationId::Contact(_)) {
            whitelist_cache_invalidate(&self.whitelist_cache, account);
        }
        self.note_conversation_state_known(account, canonical, written);
        Ok(written)
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

    /// storage.getCallLinks（窗D A6）：读全部 call link 投影（存储序 =
    /// manifest CALL_LINK identifiers 首现序；坏条目跳过不炸面）。fake
    /// 空表桩（读面纪律）。
    pub(crate) async fn get_call_links(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<storage_call_link::CallLinkSync>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_call_link::read_call_links(&context).await
    }

    /// storage.setCallLink（窗D A6）：active/deleted 写面（官方
    /// toCallLinkRecord 语义 + 409 采纳重放）；返回写后云态投影。
    pub(crate) async fn set_call_link(
        self: &Arc<Self>,
        account: &str,
        intent: storage_call_link::CallLinkWrite,
    ) -> Result<Vec<storage_call_link::CallLinkSync>, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        storage_call_link::write_call_link(&context, &intent).await
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
        let body = ContentBody::SynchronizeMessage(identity::verified_sync_message(
            &peer,
            outcome.peer_key,
            outcome.state,
        ));
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

    /// getConfiguration（账号级设置读投影，窗D B6 KT 扩展方法）：
    /// AccountRecord 配置字段面 → setAccountConfiguration 可写六位镜像；
    /// 真相源 = Storage Service（多设备经 storage sync 收敛）。从未有
    /// storage 状态（无 master key / manifest 404 / 记录缺席）= 全缺省面
    /// （诚实缺省，不报错）。fake 模式缺省桩。
    pub(crate) async fn get_configuration(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<storage_sync::AccountConfigurationFace, storage_sync::StorageSyncError> {
        if !self.real {
            return Ok(storage_sync::AccountConfigurationFace::default());
        }
        let context = storage_context(self, account).await?;
        match storage_sync::read_own_account_record(&context).await? {
            Some(record) => Ok(storage_sync::account_configuration_of_record(&record)),
            // 记录缺席 = 全缺省面（空记录投影与 Default 面逐位等价，单测钉形）。
            None => Ok(storage_sync::AccountConfigurationFace::default()),
        }
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

    /// getDevices（设备清单读面，窗D B13，KT 扩展方法）：GET v1/devices
    /// 投影 + 设备名引擎侧解密（本机 ACI 身份私钥）；fake 模式空表桩
    /// （读面纪律）。
    pub(crate) async fn list_devices(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<devices::DeviceProjection>, devices::DevicesError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let context = storage_context(self, account).await?;
        devices::list_devices(&context).await
    }

    /// unlinkDevice（服务端解绑，窗D B13 同批）：DELETE v1/devices/{id}
    /// 直通（服务器裁决授权）；fake 模式成功桩。
    pub(crate) async fn unlink_device(
        self: &Arc<Self>,
        account: &str,
        device_id: u32,
    ) -> Result<(), devices::DevicesError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        devices::unlink_device(&context, device_id).await
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

    /// phoneLookup（按手机号查找读面，KT 扩展方法，CDSI）：号码批 →
    /// CDSI（SGX attested 通路，vendored directory.rs ALL_PROXY 同源出口）
    /// → 逐条投影 `{number, aci?, pni?, isRegistered}`（输入序，未命中
    /// 诚实降级 isRegistered:false，usernameLookup 同款降级语义）。账号
    /// 参数走 open_registered 解析梯（未注册 -32603）。**结果只投影即
    /// 弃**：不落引擎库、不发通知（AGENTS.md 联系人纪律；号码→账号本地
    /// 映射归 connector contacts 同步链）。fake 模式形状桩（逐条回显
    /// `{number, isRegistered:false}`，不触网）。
    pub(crate) async fn phone_lookup(
        self: &Arc<Self>,
        account: &str,
        numbers: &[String],
    ) -> Result<serde_json::Value, phone::PhoneLookupError> {
        if !self.real {
            return Ok(json!(numbers
                .iter()
                .map(|number| json!({ "number": number, "isRegistered": false }))
                .collect::<Vec<_>>()));
        }
        // 账号解析梯（未注册账号 UnknownAccount → -32603，与一切 account
        // 参数面同口径）。常驻管理器缓存复用（C 批提速先例）；lookup 非
        // 发送面，不做 prekey send-gate，只取互斥。
        let shared = open_registered(self, account)
            .await
            .ok_or(phone::PhoneLookupError::UnknownAccount)?;
        let mut manager = shared.lock().await;
        let rows = phone::lookup(&mut manager, numbers).await?;
        let mut projected = Vec::with_capacity(rows.len());
        for row in rows {
            let mut entry = serde_json::Map::new();
            entry.insert("number".to_owned(), json!(row.number));
            match row.service_id {
                Some(ServiceId::Aci(aci)) => {
                    entry.insert("aci".to_owned(), json!(aci.service_id_string()));
                }
                Some(ServiceId::Pni(pni)) => {
                    entry.insert("pni".to_owned(), json!(pni.service_id_string()));
                }
                None => {}
            }
            entry.insert("isRegistered".to_owned(), json!(row.service_id.is_some()));
            projected.push(serde_json::Value::Object(entry));
        }
        Ok(json!(projected))
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
        // F 链路回声抑制：批内三类会话记录（Contact/GV1/GV2）的 INSERT/
        // UPDATE 先预观测（record 明文 → 会话标识 + 三 flag），写成功后仅
        // 对 Written 条目登记观测簿（Noop 未落云不登记；写方已知不自我
        // 通知，setConversationState 登记同纪律）。
        let pre_observed: Vec<
            Option<(
                storage_sync::EngineConversationId,
                storage_sync::ConversationState,
            )>,
        > = updates.iter().map(conversation_write_observation).collect();
        let context = storage_context(self, account).await?;
        let outcome = storage_write::write_items(&context, updates).await?;
        for (observation, result) in pre_observed.into_iter().zip(&outcome.results) {
            if let (Some((canonical, state)), storage_write::ItemOutcome::Written { .. }) =
                (observation, &result.outcome)
            {
                // whitelist 位住在 ContactRecord（修1）：联系人族写面即清
                // 发送闸缓存，下一发送重读全量。
                if matches!(canonical, storage_sync::EngineConversationId::Contact(_)) {
                    whitelist_cache_invalidate(&self.whitelist_cache, account);
                }
                self.note_conversation_state_known(account, canonical, state);
            }
        }
        Ok(outcome)
    }

    /// 云备份 restore 启动（cloud-backup-contract §2：`backups.restoreFromCloud`；
    /// send lane mutating；同引擎至多一个，重入 = Transport already running）。
    /// AEP/ACI/signing key 从账号 store/registration 取（snapshot_import 先例）；
    /// 非 Send future spawn_local 承载（link_flow/download_transfer_archive 同
    /// 策略）。NDJSON 产物落 `history-import-cloud.ndjson`（connector 消费链
    /// 与 transfer_archive 同位）。
    pub(crate) async fn cloud_backup_restore_start(
        self: &Arc<Self>,
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
        // 并发第二请求如实吃 AlreadyRunning。闸挂 RunningGuard——abort（取消）
        // 路径闭包收尾不执行，守卫 Drop 保证闸不泄漏。
        let Some(_running) = cloud_backup::CloudRestoreProgress::try_begin_running() else {
            return Err(cloud_backup::CloudRestoreError::AlreadyRunning);
        };
        // 会话归属登记（取消面按账号判定终态清不清）。
        progress.start_session(account);
        let task_handle = tokio::task::spawn_local(async move {
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
            // _running 守卫随闭包收尾 Drop 释放闸；abort 路径则随 future
            // 弃置 Drop——两路都不泄漏。
        });
        // 句柄入单槽（归属账号随行）——`history.importCancel` 按账号中止
        // 在途 restore（restore 腿）。
        *self
            .cloud_restore_task
            .lock()
            .expect("cloud restore task lock") = Some((account.to_owned(), task_handle));
        Ok(())
    }

    /// 云备份 restore 进度/状态（`backups.cloudStatus` 只读数据源；快照缺席 =
    /// `{present:false}`）。
    pub(crate) fn cloud_backup_status(&self) -> Option<cloud_backup::CloudRestoreSnapshot> {
        self.cloud_restore_progress.snapshot()
    }

    /// 账号 store 目录（注册表快照后放锁；fake 账号/未知账号 = None）。
    async fn account_dir(&self, account: &str) -> Option<PathBuf> {
        let rel = self.state.lock().await.account_dir(account)?;
        Some(self.data_dir.join(rel))
    }

    /// 存量历史导入取消（`history.importCancel` 服务面，KT 扩展）：按账号
    /// 中止在途导入任务——transfer_archive 下载（transfer 腿）与云备份
    /// restore（restore 腿）两腿都查——并清理半成品（`transfer-archive.bin`/
    /// `history-import.ndjson`/`cloud-backup/`/`history-import-cloud.ndjson`）
    /// 与快照面（在途 + failed 终态，两腿同清）。半成品按腿清理：取消哪腿
    /// 清哪腿（被中止任务的在写 NDJSON 一并删——被取消的导入不得被
    /// connector 消费链悄悄补上）；另一腿已完成待消费的产物不连坐。
    ///
    /// 返回 `true` = 确实中止了在途任务；无在途任务 = `false`（幂等成功，
    /// 不假装取消了什么）。终态清除是取消的附带效果（终态清除触发器恰三：
    /// begin/取消/重链）；restore 单槽属于其他账号时快照不动。
    pub(crate) async fn history_import_cancel(&self, account: &str) -> bool {
        // transfer 腿：abort 即弃（单线程 LocalSet，返回后任务不再执行）。
        let transfer_cancelled = self.transfer_downloads.cancel(account);
        // restore 腿：进程级单槽，仅当在途任务属于本账号才动；无在途时按
        // 会话归属判定终态快照清不清（failed 终态挂在最近会话的账号上）。
        let (restore_ours, restore_cancelled) = {
            let mut slot = self
                .cloud_restore_task
                .lock()
                .expect("cloud restore task lock");
            match slot.as_ref() {
                Some((owner, _)) if owner == account => {
                    let mut cancelled = false;
                    if let Some((_, handle)) = slot.take() {
                        if !handle.is_finished() {
                            handle.abort();
                            cancelled = true;
                        }
                    }
                    (true, cancelled)
                }
                _ => (self.cloud_restore_progress.owned_by(account), false),
            }
        };
        // 半成品清理按腿进行：取消哪腿清哪腿——另一腿已完成的产物（等待
        // connector 消费的 NDJSON）不连坐。
        if let Some(dir) = self.account_dir(account).await {
            if transfer_cancelled {
                transfer_progress::remove_transfer_artifacts(&dir);
            }
            if restore_cancelled {
                cloud_backup::remove_restore_artifacts(&dir);
            }
        }
        self.transfer_progress.clear(account);
        if restore_ours {
            self.cloud_restore_progress.clear();
        }
        transfer_cancelled || restore_cancelled
    }

    /// 云备份 upload 启动（cloud-backup-contract §6.2：`backups.uploadToCloud`；
    /// send lane mutating；同引擎至多一个，重入 = BACKUP_UPLOAD_ALREADY_RUNNING）。
    /// 前置检查序与 restore 同构：storage_context → AEP/registration → signing
    /// key → 工件闸 → 单槽重入闸；装配段专用 OS 线程（§10.3 同款）+ 上行段
    /// LocalSet watcher（watch_and_upload 统一 finally：临时产物必删 + 闸释放）。
    pub(crate) async fn cloud_backup_upload_start(
        self: &Arc<Self>,
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
    /// 所属 Service 弱句柄 + 账号名（群态成员 profileKey 新落 → profile
    /// 刷新批触发联动，未知联系人根因批 2026-10-23；弱句柄不延长关停期
    /// 生命周期）。
    pub(crate) service: Weak<Service>,
    pub(crate) account: String,
}

/// 凭据构造共用面（storage_context 上下文与 prekey remote config 读面同
/// 源单点）：注册数据 → 手工凭据 PushService（link_flow 先例同构）。
/// password 为 pub(crate)：经 serde 数据面读回（ADR 0005 §S2 唯一外部
/// 构造路径，JSON 形态 = presage 官方 store 落库格式）；phonenumber 字段
/// 要 E164（与 link_flow / presage credentials() 同构）。
pub(crate) fn push_service_from_registration(data: &RegistrationData) -> Option<PushService> {
    let raw = serde_json::to_value(data).ok()?;
    let password = raw["password"].as_str()?.to_string();
    use presage::libsignal_service::utils::TryIntoE164;
    Some(PushService::new(
        SignalServers::Production,
        Some(ServiceCredentials {
            aci: Some(data.service_ids.aci),
            pni: Some(data.service_ids.pni),
            phonenumber: data.phone_number.clone().try_into_e164().ok()?,
            password: Some(password),
            device_id: data.device_id.and_then(|id| id.try_into().ok()),
        }),
        USER_AGENT,
    ))
}

/// 组装 storage 上下文：账号 store + 凭据（照抄 link_flow 先例）。Arc 面
/// 入参：群态 profileKey 新落触发面需要 Service 弱句柄（StorageContext.service）。
async fn storage_context(
    service: &Arc<Service>,
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
    let push = push_service_from_registration(&data)
        .ok_or(storage_sync::StorageSyncError::UnknownAccount)?;
    Ok(StorageContext {
        store,
        push,
        device_id: data.device_id.unwrap_or(1),
        account_dir,
        aci_uuid: data.service_ids.aci,
        pni_uuid: data.service_ids.pni,
        group_credentials: service.group_credentials.clone(),
        service: Arc::downgrade(service),
        account: account.to_owned(),
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

/// 群取回解码失败结构化事件（接收完整性批②，compat §3 additive）：群
/// update/sync 信封触发的群 fetch/decrypt 解码类失败经 vendored 重试环
/// 耗尽 → receive 族 envelope 级 `groupSyncFailed` 标记（{groupId, count}，
/// count = 重试环尝试次数，无身份/群名面）。旧群态保留，但不再静默兜底
/// ——消费方自定后续（引擎只报态）。
fn group_sync_failed_notification(account: &str, group_id: &str, count: u32) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": account,
            "envelope": {
                "timestamp": now_ms(),
                "groupSyncFailed": {
                    "groupId": group_id,
                    "count": count,
                },
            },
        },
    })
}

/// 群在服务器侧已消失（HTTP 404 → 官方 GROUP_NONEXISTENT，接收完整性批②）：
/// 复用既有 `groupTerminated` 事件面（terminator=null——非终止 change，
/// 本地无编辑者可指），调用方随事件登记 terminated 注册表拦截后续发送。
fn group_gone_notification(account: &str, group_id: &str) -> Value {
    let timestamp = now_ms();
    json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": {
            "account": account,
            "envelope": {
                "timestamp": timestamp,
                "groupTerminated": {
                    "groupId": group_id,
                    "terminator": Value::Null,
                    "timestamp": timestamp,
                },
            },
        },
    })
}

/// 时钟偏移闸阈值（时序审计 G-时钟，P15b 项8）：官方 Signal-Android
/// ClockSkewDetector 阈值 = remote config `client.maxAllowedClockSkewSeconds`
/// 默认 24h（RemoteConfig.kt defaultValue = 24.hours）。引擎无 remote
/// config 面，取同默认值为编译期常量。
const CLOCK_SKEW_THRESHOLD_MS: i64 = 24 * 60 * 60 * 1_000;

/// 偏移判定纯函数：`|本地 - 服务器| > 阈值` → Some(skew_ms)，符号 = 本地
/// − 服务器（正 = 本地钟快）；阈内 → None（官方 ClockSkewDetector.skewFrom
/// 同式：abs(local − server)；喂闸锚位与官方 WS 层锚的差异及等价设计见
/// ClockSkewGate）。
fn clock_skew_over_threshold(server_ts_ms: i64, now_ms: i64) -> Option<i64> {
    let skew = now_ms - server_ts_ms;
    (skew.abs() > CLOCK_SKEW_THRESHOLD_MS).then_some(skew)
}

/// 偏移状态机单步（边缘触发，防 per-信封告警风暴）：
/// - 未告警 + 越阈 → `Alert(skew)`：上抛一次告警；
/// - 已告警 + 仍越阈 → `Sustained`：静默（每信封重复告警无增量信息）；
/// - 阈内 → `Rearm`：清告警态（官方 detected 态 in-memory 自愈清除同
///   语义——时钟修好后下次越阈重新告警）。
enum SkewTransition {
    Alert(i64),
    Sustained,
    Rearm,
}

fn skew_transition(alerted: bool, server_ts_ms: i64, now_ms: i64) -> SkewTransition {
    match clock_skew_over_threshold(server_ts_ms, now_ms) {
        Some(skew) if !alerted => SkewTransition::Alert(skew),
        Some(_) => SkewTransition::Sustained,
        None => SkewTransition::Rearm,
    }
}

/// per-管线时钟偏移闸。锚位（行为对齐，非抄码，2026-10-12 逐行核实）：
/// 官方 skew 判定锚在 **WebSocket 层服务器时间**——Signal-Desktop
/// （pinned abe80d3）ts/textsecure/SocketManager.preload.ts
/// #handleServerTimestamp（:928）由 connectAuthenticated 的
/// onServerTimestamp 回调喂（WS 响应级服务器时间，恒新鲜，与积压信封
/// 无关），#getSkew（:914）+ 阈值 remote config
/// `client.maxAllowedClockSkewSeconds`（兜底 24h、下限 1h）；官方
/// Signal-Android（pinned 92f5ccd）ClockSkewDetector.onServerTimeReceived
/// 同位（ChatConnectionListener.onServerTimestamp 经
/// SignalWebSocketHealthMonitor + remote config fetch）。官方从不拿信封
/// 内层 metadata.server_timestamp 喂判定。sidecar 等价设计（积压免疫）：
/// presage receive_stream 暴露不了 WS 层服务器时间，改用武装闸——每条
/// 新流入口先解除武装（新流 = 服务器历史队列可能未排空，积压信封
/// server_timestamp 是入队时刻），排空标记 `Received::QueueEmpty` 到达
/// 再武装（服务器侧 = RedisDynamoDbMessagePublisher「initial queue drain
/// complete signal」：零存量建连恒产出、websocket 通道每流恰一次，
/// vendored presage 无条件透传不丢弃）；武装后信封为实时相，喂闸锚恒
/// 新鲜，等价官方 WS 层锚。`alerted` 跨重连存活（边缘触发纪律不变），
/// `armed` 随每条新流重置。偏移观测只做告警上抛，不拒信封不停车：本地
/// 钟错不该丢真消息（拒绝面不做，§5 偏差登记）；官方 Android 超阈「停
/// 保活 + 阻断屏」是 GUI 语义（有用户可修钟），sidecar 无用户可介入，
/// 停车即自 DoS——保守策略 = 报态给消费方自定。
struct ClockSkewGate {
    alerted: bool,
    armed: bool,
}

impl ClockSkewGate {
    /// 新流入口解除武装：排水完成前的信封可能出自服务器历史队列，
    /// observe 一律 no-op（不告警、不变更 alerted、不重臂）。
    fn disarm(&mut self) {
        self.armed = false;
    }

    /// 排空标记（`Received::QueueEmpty`）到达：武装，此后实时相信封喂闸。
    fn arm(&mut self) {
        self.armed = true;
    }

    /// 单步推进闸状态（纯状态机，可测）：未武装 no-op 返回 None；武装后
    /// 按 skew_transition 推进并同步 alerted，返回本步转移。
    fn step(&mut self, server_ts_ms: i64, now_ms: i64) -> Option<SkewTransition> {
        if !self.armed {
            return None;
        }
        let transition = skew_transition(self.alerted, server_ts_ms, now_ms);
        match transition {
            SkewTransition::Alert(skew) => {
                self.alerted = true;
                Some(SkewTransition::Alert(skew))
            }
            SkewTransition::Sustained => Some(SkewTransition::Sustained),
            SkewTransition::Rearm => {
                self.alerted = false;
                Some(SkewTransition::Rearm)
            }
        }
    }

    async fn observe(&mut self, service: &Arc<Service>, number: &str, server_ts_ms: i64) {
        if let Some(SkewTransition::Alert(skew)) = self.step(server_ts_ms, now_ms() as i64) {
            service
                .notify(
                    number,
                    clock_skew_notification(number, skew, server_ts_ms.max(0) as u64),
                )
                .await;
        }
    }
}

/// 偏移告警通知（wire additive，`receive.clockskew`，receive.emptied 同款
/// 独立方法名非 envelope 载体——偏移是流态观测无信封可挂）。params：
/// account / skewMs（带符号 = 本地 − 服务器，正 = 本地快）/ serverTimestamp
///（观测源信封服务器毫秒；积压免疫下仅排空完成后的实时相信封可达告警，
/// 见 ClockSkewGate）。引擎只报态；连接器/desktop 消费自定（日志/
/// 告警/运维通知）。
fn clock_skew_notification(account: &str, skew_ms: i64, server_timestamp_ms: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "receive.clockskew",
        "params": {
            "account": account,
            "skewMs": skew_ms,
            "serverTimestamp": server_timestamp_ms,
        },
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
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum StreamOutcome {
    Connected,
    Recoverable,
    Unbound,
    /// 时序审计 G1：stdout 出口死亡（flush 栅栏不可解）。停环等连接器
    /// 重启引擎，不退避重连（重连 = 重投信封被 Duplicate 就地 ack 吞）。
    StdioDead,
    /// 服务端以 1000（normal closure）或 4409（connected elsewhere）关闭：
    /// 官方 SocketManager 明确不重连（`SocketManager.preload.ts`:391-394）。
    /// 停环等连接器/重链显式重建，绝不与夺走连接的客户端互搏。
    Terminated,
}

impl StreamOutcome {
    /// 终止结局：不进入重连退避。
    fn is_terminal(self) -> bool {
        matches!(
            self,
            StreamOutcome::Unbound | StreamOutcome::StdioDead | StreamOutcome::Terminated
        )
    }
}

/// 服务器侧「同设备他处已连接」close code（Signal 服务端 WebSocket 语义；
/// 官方 Signal-Desktop SocketManager.preload.ts:391-394 对 4409 不重连）。
const WS_CLOSE_CONNECTED_ELSEWHERE: u16 = 4409;

/// 服务端正常关闭（官方 `SocketManager` 同判：不重连）。
const WS_CLOSE_NORMAL: u16 = 1000;

/// 本地单连接槽接管（kill-before-create handover）的合成 close code，
/// 与 vendored `websocket::CLOSE_CODE_LOCAL_HANDOVER` 同值。它不是服务端
/// 帧，只用于告诉旧收流「你已被本进程的新连接替换，不要重连」。
const WS_CLOSE_LOCAL_HANDOVER: u16 = 4000;

/// 接收流结束时是否进入「不重连」终点：1000（normal closure）、4409
/// （connected elsewhere，官方同位）与 4000（本地槽接管，引擎适配）。
/// 其余（1001/1006/3001/无 close code）按传输失败走 Fibonacci 重连。
fn receive_close_is_terminal(close_code: Option<u16>) -> bool {
    matches!(
        close_code,
        Some(WS_CLOSE_NORMAL) | Some(WS_CLOSE_CONNECTED_ELSEWHERE) | Some(WS_CLOSE_LOCAL_HANDOVER)
    )
}

// ---------------------------------------------------------------------------
// DERM 收发自愈（B 批；时序审计组四 E3/E4 官方对齐细化，Desktop
// handleRetry 行为对齐）：解密失败回发 DecryptionErrorMessage（明文腿）；
// 收到重试请求按官方闸序处理——超限早退（无会话动作）→ archive-on-match
// → 新鲜度/找回重发 → 兜底（SKDM 臂未建，归档后 NullMessage，差异记档
// compat §5）。
// ---------------------------------------------------------------------------

/// 同一 (来源, 原始时间戳) 重试请求放行上限（官方 Desktop handleRetry
/// RETRY_LIMIT = 5 同值）。
const DERM_RETRY_LIMIT: u32 = 5;
/// 重试请求新鲜度窗口（官方 Desktop retryRespondMaxAge 缺省 14 天同值）：
/// 原始消息超龄不再重发，转兜底档（官方 sendDistributionMessageOrNullMessage）。
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
        Failure::RetryRequest {
            original_ts,
            ratchet_key,
            derm_device_id,
            requester_device,
        } => {
            derm_on_retry_request(
                manager,
                sender,
                original_ts,
                ratchet_key,
                derm_device_id,
                requester_device,
                guard,
            )
            .await;
        }
    }
}

/// 档一：把重试回执（DecryptionErrorMessage 序列化 proto）按官方明文腿
/// 回原发送者：PlaintextContent（0xC0 + Content{derm} + 0x80）以 Envelope
/// type=8 直发、不做会话加密（官方 Desktop requestResend → PlaintextContent
/// → sendMessageProtoAndWait{proto: plaintext}；OutgoingMessage 对
/// PlaintextContent 不走 getCiphertextMessage）。回执发送失败静默（官方
/// requestResend 队列失败同样落会话重置兜底）。
async fn derm_send_receipt(
    manager: &mut Manager<SqliteStore, Registered>,
    sender: ServiceId,
    receipt: &[u8],
) {
    use presage::libsignal_service::protocol::{DecryptionErrorMessage, PlaintextContent};
    // try_from 要求 timestamp 在场：回执由 presage RetryReceiptMaterial 经
    // for_original 构造，timestamp 恒有；坏回执静默丢弃。
    let Ok(body) = DecryptionErrorMessage::try_from(receipt) else {
        return;
    };
    let plaintext = PlaintextContent::from(body);
    let _ = manager
        .send_plaintext_content(sender, &plaintext, now_ms())
        .await;
}

/// 档二：对端重试请求（DERM.RetryRequest）。官方 onRetryRequest 闸序对齐
/// （handleRetry.preload.ts:129-261）：①防环超限早退（无任何会话动作，
/// 官方 retryCount > RETRY_LIMIT 同序）；②archive-on-match（DERM 点名我
/// 设备且携 ratchet key → 比对 (requester, 其发送设备) 会话当前 ratchet，
/// 命中归档该单一会话）；③新鲜度 → 已发消息找回（含群成员资格）→ 命中
/// 重发原内容（原时间戳）；④超龄/找不到/不允许 → 兜底（官方群+senderKey
/// 重发 SKDM，否则归档过才发 NullMessage，否则不做；我方 SKDM 臂未建，
/// 偏差登记 compat §5）。
async fn derm_on_retry_request(
    manager: &mut Manager<SqliteStore, Registered>,
    requester: ServiceId,
    original_ts: u64,
    ratchet_key: Option<Vec<u8>>,
    derm_device_id: u32,
    requester_device: u32,
    guard: &mut DermRetryGuard,
) {
    // ① 超限早退：官方不重发也不做会话动作（信任既有兜底已铺开）。
    if !guard.admit(&requester.service_id_string(), original_ts) {
        return;
    }
    // ② archive-on-match。
    let did_archive = derm_archive_session_on_match(
        manager.store(),
        u32::from(manager.device_id()),
        &requester,
        ratchet_key.as_deref(),
        derm_device_id,
        requester_device,
    )
    .await;
    // ③ 新鲜度 + 找回（含群成员资格）。
    if !derm_request_fresh(now_ms(), original_ts) {
        derm_send_distribution_or_null(manager, requester, did_archive).await;
        return;
    }
    let lookup = manager
        .store()
        .sent_message_by_timestamp(&requester, original_ts)
        .await
        .ok()
        .flatten();
    let Some((thread, content)) = lookup else {
        derm_send_distribution_or_null(manager, requester, did_archive).await;
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
        derm_send_distribution_or_null(manager, requester, did_archive).await;
        return;
    }
    // 重发原始内容（原始时间戳保序；本地 store INSERT OR REPLACE 同行
    // 幂等）。resend 失败静默（请求方下轮重试或兜底档再收敛）。
    let _ = manager
        .send_message(requester, content.body, original_ts)
        .await;
}

/// archive-on-match（官方 archiveSessionOnMatch :338-368）：DERM 内嵌
/// device_id 点名我设备且携 ratchet key 时，取 (requester, 其发送设备) 会
/// 话，当前 ratchet key 与 DERM 所载一致 = 正是这把会话在对方解不开 →
/// 归档该单一地址（不 clear 全设备会话）。任何前置不满足 = 不动会话。
async fn derm_archive_session_on_match(
    store: &SqliteStore,
    own_device_id: u32,
    requester: &ServiceId,
    ratchet_key: Option<&[u8]>,
    derm_device_id: u32,
    requester_device: u32,
) -> bool {
    use presage::libsignal_service::protocol::{DeviceId, PublicKey, SessionStore as _};

    let Some(ratchet_bytes) = ratchet_key else {
        return false;
    };
    // DERM 点名我设备：不是我这台设备造成的失败，不归我修（官方
    // ourDeviceId !== senderDevice 同闸）。
    if derm_device_id != own_device_id {
        return false;
    }
    let Ok(ratchet_key) = PublicKey::deserialize(ratchet_bytes) else {
        return false;
    };
    let Some(device_id) = u8::try_from(requester_device)
        .ok()
        .and_then(|raw| DeviceId::new(raw).ok())
    else {
        return false;
    };
    let address = requester.to_protocol_address(device_id);
    let mut protocol_store = store.aci_protocol_store();
    let Some(mut session) = protocol_store.load_session(&address).await.ok().flatten() else {
        return false;
    };
    let matches = session
        .current_ratchet_key_matches(&ratchet_key)
        .unwrap_or(false);
    if !matches {
        return false;
    }
    if session.archive_current_state().is_err() {
        return false;
    }
    protocol_store
        .store_session(&address, &session)
        .await
        .is_ok()
}

/// 兜底（官方 sendDistributionMessageOrNullMessage :370-440）：群 + 有
/// sender key 分发台账 → 重发 SKDM（官方主臂）；否则归档过才发
/// NullMessage（用新会话敲一下对端，促其重建），否则什么都不做。
/// 偏差登记：我方 SKDM 重发臂未建（sender-key 分发只随群发送自然发生），
/// 群兜底一律落入归档/NullMessage 同判（compat §5）。
async fn derm_send_distribution_or_null(
    manager: &mut Manager<SqliteStore, Registered>,
    requester: ServiceId,
    did_archive: bool,
) {
    if !did_archive {
        return;
    }
    use rand::Rng as _;
    let mut padding = vec![0u8; 64];
    rand::rng().fill(&mut padding[..]);
    let _ = manager
        .send_message(
            requester,
            ContentBody::NullMessage(presage::proto::NullMessage {
                padding: Some(padding),
            }),
            now_ms(),
        )
        .await;
}

/// 账号接收管线入口（稳定性批 S4/A2/A4）：证书维护任务挂管线生命周期
/// （panic 重启不重复 spawn）；账号内层循环由 supervisor 包裹，panic 只
/// 收口本次运行并按账号级 Fibonacci+jitter 退避重启。
async fn receive_pipeline(
    service: &Arc<Service>,
    number: String,
    dir: PathBuf,
    shutdown: watch::Receiver<bool>,
) {
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
    supervision::run_account_supervisor(
        shutdown,
        supervision::AccountCrashBackoff::default(),
        |run_shutdown| receive_account_loop(service, &number, &dir, run_shutdown),
        reconnect_backoff_delay,
        |crashes, step, delay| {
            // 日志零号码/正文/密钥：账号只落不可逆短哈希；panic payload 不落。
            account_metrics::record_crash(&number);
            tracing::warn!(
                target: "engine.supervisor",
                stage = "account_restart",
                account = %crate::methods::probe_account8(Some(number.as_str())),
                crashes,
                step,
                delay_ms = delay.as_millis() as u64,
                "account receive task panicked; restarting after backoff",
            );
        },
    )
    .await;
}

/// 单次账号运行态：流内重连由本循环承载（状态跨流存活），panic 时随
/// supervisor 重启整体重置。
/// 测试观测：接收循环真实尝试次数（最小复现「manager 不可用 → 重试而非
/// 静默退出」用；生产路径不读不写）。
#[cfg(test)]
static RECEIVE_LOOP_ATTEMPTS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

async fn receive_account_loop(
    service: &Arc<Service>,
    number: &str,
    dir: &Path,
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
    // P15b 项8 时钟偏移闸：alerted 跨重连存活（管线生命周期一份，边缘
    // 触发上抛 receive.clockskew）；armed 随每条新流重置（积压免疫，
    // serve_account_stream 入口 disarm / QueueEmpty 臂 arm）。
    let mut skew_gate = ClockSkewGate {
        alerted: false,
        armed: false,
    };
    // 重连节奏（官方 Desktop ts/backoff/BackOff.std 同语义）：Fibonacci
    // 1→55s + ±5s jitter，连接成功 reset 从 1s 重来。
    let mut schedule = ReconnectSchedule::default();
    loop {
        if *shutdown.borrow() {
            return;
        }
        #[cfg(test)]
        RECEIVE_LOOP_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut serve_rx = shutdown.clone();
        // 不在外层 select 取消 serve_account_stream：停旗只在信封臂之间生效
        // （serve_account_stream 自身的 shutdown 臂），在飞信封处理走完
        // 当前臂体后才收口——A4 不丢未结算行。停旗期间阻塞等 10s 宽限由
        // delete/shutdown_all 承担。
        let outcome = serve_account_stream(
            service,
            number,
            dir,
            &mut serve_rx,
            &mut last_contacts_sync,
            &mut derm_guard,
            &mut receipts,
            &mut capabilities_registered,
            &mut skew_gate,
        )
        .await;
        if *shutdown.borrow() {
            return;
        }
        match outcome {
            // Unbound（服务端 401/403 凭据拒绝）：官方链接设备语义——停环
            // 等重链，不退避重连；结构化上抛一次（wire additive，compat §5）。
            StreamOutcome::Unbound => {
                perf_metrics::add(perf_metrics::keys::STREAM_UNBOUND, 1);
                account_metrics::record_stream(number, account_metrics::StreamEvent::Unbound);
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stopped",
                    reason = "unbound",
                    account = %crate::methods::probe_account8(Some(number)),
                    "receive credentials rejected (401/403); stopping until relink"
                );
                service
                    .notify(number, account_unbound_notification(number))
                    .await;
                return;
            }
            // StdioDead（时序审计 G1）：stdout 已死（连接器侧断开/半开管道），
            // 投影永不可达。停管线不退避——重连只会把重投信封喂进 Duplicate
            // 就地 ack 吞掉；信封留服务器队列，连接器重启引擎后全量重放补投影。
            StreamOutcome::StdioDead => {
                perf_metrics::add(perf_metrics::keys::STREAM_STDIO_DEAD, 1);
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stopped",
                    reason = "stdio_dead",
                    account = %crate::methods::probe_account8(Some(number)),
                    "receive output channel is dead; stopping until engine restart"
                );
                return;
            }
            // 1000/4409：官方 SocketManager 不重连（与服务端/另一客户端
            // 抢连接只会振荡）。停环等连接器/重链显式重建。
            StreamOutcome::Terminated => {
                perf_metrics::add(perf_metrics::keys::STREAM_CLOSED_NO_RECONNECT, 1);
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stopped",
                    reason = "no_reconnect_close_code",
                    account = %crate::methods::probe_account8(Some(number)),
                    "receive stream closed with a no-reconnect close code; stopping per official semantics"
                );
                return;
            }
            StreamOutcome::Connected | StreamOutcome::Recoverable => {}
        }
        if outcome == StreamOutcome::Connected {
            perf_metrics::add(perf_metrics::keys::STREAM_CONNECTED, 1);
            account_metrics::record_stream(number, account_metrics::StreamEvent::Connected);
        } else {
            perf_metrics::add(perf_metrics::keys::STREAM_RECOVERABLE, 1);
            account_metrics::record_stream(number, account_metrics::StreamEvent::Recoverable);
        }
        // 排程单一取点：成功 reset 到 1s，失败逐级 Fibonacci（官方
        // BackOff.reset/get 同语义）。终止结局已在上面返回，None 防御性兜底。
        let Some(delay) = schedule.next_delay(outcome) else {
            return;
        };
        // S2a 打点：退避实际等待落地值（P12 2c' 退避链观测面）。
        perf_metrics::record_us(
            perf_metrics::keys::RECONNECT_BACKOFF,
            delay.as_micros() as u64,
        );
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// 信封到达 → ack 完成：既有全局计时 + A1 逐账号序列（单点，不复制语义）。
fn record_receive_envelope(account: &str, started: std::time::Instant) {
    let us = started.elapsed().as_micros() as u64;
    perf_metrics::record_us(perf_metrics::keys::RECEIVE_ENVELOPE, us);
    account_metrics::record_receive(account, us);
}

/// Fibonacci 退避基值（官方 Desktop `BackOff.std.ts` 同序列）：
/// 1,2,3,5,8,13,21,34,55，封顶 55s。step=0 → 1s，step=1 → 2s（旧实现
/// step=0/1 都是 1s，重连序列多出一个 1s；2026-10-21 事故对齐批修正）。
pub(crate) fn reconnect_backoff_delay(step: u32) -> Duration {
    const STEP_MS: u64 = 1_000;
    const CAP_MS: u64 = 55_000;
    let (mut a, mut b) = (STEP_MS, STEP_MS);
    for _ in 0..step {
        let next = (a + b).min(CAP_MS);
        a = b;
        b = next;
    }
    Duration::from_millis(b.min(CAP_MS))
}

/// 接收重连节奏状态机（官方 `BackOff` 同语义）：失败逐级 Fibonacci，
/// 建流成功即回到首拍 1s（`BackOff.reset()`）。纯状态，单测钉形。
#[derive(Debug, Default)]
struct ReconnectSchedule {
    step: u32,
}

impl ReconnectSchedule {
    /// 下一次重连等待。终止结局（凭据拒绝/出口死亡/服务端 1000/4409）
    /// 返回 None（停环，不重连）；Connected = 刚建流成功即结束 → reset 到
    /// 首拍；Recoverable = 建流失败 → 步进。
    fn next_delay(&mut self, outcome: StreamOutcome) -> Option<Duration> {
        if outcome.is_terminal() {
            return None;
        }
        if outcome == StreamOutcome::Connected {
            self.step = 0;
        }
        let delay = reconnect_jittered_delay(reconnect_backoff_delay(self.step));
        if outcome != StreamOutcome::Connected {
            self.step = self.step.saturating_add(1);
        }
        Some(delay)
    }

    #[cfg(test)]
    fn step(&self) -> u32 {
        self.step
    }
}

/// 退避 jitter（官方 BackOff.get 同语义）：`jitter < result` 才叠加
/// `random()*jitter` 且单向正值——短拍（≤5s 档，含首拍 1s 与 5s 档）
/// 恒精确不被 jitter 拖慢，长拍单向上浮消重连峰。
pub(crate) fn reconnect_jittered_delay(base: Duration) -> Duration {
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

/// 接收循环内任何网络维护调用的硬上限（官方单请求 30s 语义）：维护臂
/// 超时即放弃本次、回到收流，绝不允许「一个卡住的同步/拉取把整条接收流
/// 静默挂死」（2026-10-21 事故根因链的一环）。
const RECEIVE_SIDE_TIMEOUT: Duration = Duration::from_secs(30);

/// 建流阶段（能力注册 + receive_messages 内的 dial/prekey/群态装配）上限。
/// 该阶段含多次串行 HTTP 调用，单次控制面预算为 65s
/// （`CONTROL_PLANE_TOTAL_TIMEOUT`），因此给它 90s，避免把慢网建流误判为
/// 永久失败；仍然有界，不会无限挂在 manager 未 ready 上。
const RECEIVE_CONNECT_TIMEOUT: Duration = Duration::from_secs(90);

/// KT (pre-keys gate, §29.6): 接收流拨号前 pre-keys 注册的等待上限。正常
/// 注册亚秒级（服务器计数 ≥10 时零上传仅一次计数查询），30s 覆盖慢网 +
/// §29.6 WsClosing 重试链（2×500ms）+ 一次完整重拨，超时不再死等。
const PREKEYS_GATE_TIMEOUT: Duration = Duration::from_secs(30);

/// KT (pre-keys gate, §29.6): 闸结局 —— `Registered` = 注册完成放行；
/// `Degraded` = 注册未确认（重试耗尽/超时）仍放行，原因串进降级日志。
#[derive(Debug, PartialEq, Eq)]
enum PreKeysGateOutcome {
    Registered,
    Degraded(&'static str),
}

/// KT (pre-keys gate, §29.6): pre-keys 时序闸（生产包装）。接收流拨号前
/// 串行等待 `refresh_pre_keys` 完成（内部自带 WsClosing 兜底重试与
/// require_unused=false 的 slot 拨号——复用活连接，不杀消息管道）。任何
/// 失败模式都放行接收流（显式落降级 warn），不把注册面故障变成接收面
/// 停摆。证据行 warn 级 = 滚动日志（与 receive_stream_established 同位，
/// 链接窗口的时序对账直接可查）。
async fn prekeys_gate(
    manager: &mut Manager<SqliteStore, Registered>,
    account: &str,
) -> PreKeysGateOutcome {
    let outcome = run_prekeys_gate(|| manager.refresh_pre_keys(), PREKEYS_GATE_TIMEOUT).await;
    match &outcome {
        PreKeysGateOutcome::Registered => tracing::warn!(
            target: "engine.receive",
            stage = "prekeys_gate_registered",
            account = %crate::methods::probe_account8(Some(account)),
            "pre-keys registered before receive stream dial"
        ),
        PreKeysGateOutcome::Degraded(reason) => tracing::warn!(
            target: "engine.receive",
            stage = "prekeys_gate_degraded",
            account = %crate::methods::probe_account8(Some(account)),
            reason,
            "pre-keys gate released the receive stream in degraded mode"
        ),
    }
    outcome
}

/// KT (pre-keys gate, §29.6): 闸核心（预算注入，单测用假注册 future 钉
/// 语义）。`register` 只调用一次；预算内完成且成功 = `Registered`，否则
/// `Degraded`（失败 / 超时各有原因串）。
async fn run_prekeys_gate<E, F, Fut>(register: F, budget: Duration) -> PreKeysGateOutcome
where
    E: std::error::Error,
    F: FnOnce() -> Fut,
    Fut: futures::Future<Output = Result<(), presage::Error<E>>>,
{
    match tokio::time::timeout(budget, register()).await {
        Ok(Ok(())) => PreKeysGateOutcome::Registered,
        Ok(Err(_)) => PreKeysGateOutcome::Degraded("registration_failed"),
        Err(_) => PreKeysGateOutcome::Degraded("timeout"),
    }
}

/// 后台维护作业上限（manifest 拉取内部可能串多次 HTTP，单次控制面预算
/// 65s）。它已在独立任务里运行，不阻塞收流；120s 只用于防止任务永久悬挂。
const RECEIVE_JOB_TIMEOUT: Duration = Duration::from_secs(120);

/// 建流失败返回 Recoverable（退避步进）；流建立后结束（对端断/关停/流
/// 排空）返回 Connected（连接成功 → 退避 reset，官方 BackOff.reset 同
/// 语义）；建流遇服务端 401 凭据拒绝（vendored
/// `ServiceError::Unauthorized`）返回 Unbound（停环等重链，B4）。
/// 服务端 1000/4409 关闭返回 Terminated（官方不重连）。
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
    skew_gate: &mut ClockSkewGate,
) -> StreamOutcome {
    // 时钟偏移闸积压免疫：每条新流 = 服务器历史队列可能未排空（刚链接
    // 设备的队列排水、断线重连补投），排水完成前的信封 server_timestamp
    // 是入队时刻而非观测时刻，喂闸必出「本机钟快数天」假告警——入口先
    // 解除武装（observe no-op），排空标记 Received::QueueEmpty 到达后
    // 重新武装（服务器零存量建连也恒发排空标记，武装必达）。
    skew_gate.disarm();
    let account_hash = crate::methods::probe_account8(Some(number));
    let store = match tokio::time::timeout(RECEIVE_CONNECT_TIMEOUT, open_store(dir)).await {
        Ok(Ok(store)) => store,
        Ok(Err(())) => {
            tracing::warn!(
                target: "engine.receive",
                stage = "receive_store_open",
                account = %account_hash,
                "receive store unavailable; retrying with backoff"
            );
            return StreamOutcome::Recoverable;
        }
        Err(_) => {
            tracing::warn!(
                target: "engine.receive",
                stage = "receive_store_open_timeout",
                account = %account_hash,
                "receive store open timed out; retrying with backoff"
            );
            return StreamOutcome::Recoverable;
        }
    };
    let mut manager = match tokio::time::timeout(
        RECEIVE_CONNECT_TIMEOUT,
        Manager::load_registered(store),
    )
    .await
    {
        Ok(Ok(manager)) => manager,
        Ok(Err(error)) => {
            // 曾经的静默跳过失控点：manager 未 ready（store 半写/锁/凭据面）
            // 时旧代码直接返回 Recoverable 且零日志，现场只看到「没消息」。
            tracing::warn!(
                target: "engine.receive",
                stage = "receive_manager_load",
                account = %account_hash,
                %error,
                "manager not ready; retrying with backoff"
            );
            return StreamOutcome::Recoverable;
        }
        Err(_) => {
            tracing::warn!(
                target: "engine.receive",
                stage = "receive_manager_load_timeout",
                account = %account_hash,
                "manager load timed out; retrying with backoff"
            );
            return StreamOutcome::Recoverable;
        }
    };
    // D5 设备面能力注册（官方 afterEveryLinkedStartup → registerCapabilities
    // 同位）：best-effort，失败下一步建流重试；成功置位，重连不再重发。
    // 加硬上限：这条调用在 receive_messages 之前，卡住会让整条接收管线
    // 从未建流却零日志；建流阶段预算 90s（控制面单请求 65s + 余量）。
    if !*capabilities_registered {
        match tokio::time::timeout(
            RECEIVE_CONNECT_TIMEOUT,
            manager.register_device_capabilities(capabilities::DEVICE_CAPABILITIES_UPLOAD),
        )
        .await
        {
            Ok(Ok(())) => *capabilities_registered = true,
            Ok(Err(error)) => tracing::warn!(
                target: "engine.receive",
                stage = "device_capabilities",
                account = %account_hash,
                %error,
                "device capability registration failed; retrying on next stream"
            ),
            Err(_) => tracing::warn!(
                target: "engine.receive",
                stage = "device_capabilities_timeout",
                account = %account_hash,
                "device capability registration timed out; retrying on next stream"
            ),
        }
    }
    // KT (pre-keys gate, §29.6): 时序闸 —— 接收流拨号必须等 pre-keys 注册
    // 完成（所有建流路径的咽喉：finish_link 重链 / startup on-start /
    // supervisor 重启都经此）。因果：pre-keys 未上服务器 → 入站初始会话
    // 无法用 pre-key 解密 → 服务器投递重试 → 降级窗（2026-10-23 09:30：
    // 附件 UPSTREAM_ERROR×3 + 推送延迟 16-24s，且 "receive pipeline
    // spawned" 先于 pre-keys 注册失败落日志——当时两条腿并发）。注册完成
    // 放行；重试耗尽/超时放行但记降级 warn——服务器可能仍有存量 pre-keys，
    // 接收面健康优先于注册面严格，缺口由 prekey 巡检与 §29.6 WsClosing
    // 重试兜底补齐。
    prekeys_gate(&mut manager, &account_hash).await;
    let mut stream =
        match tokio::time::timeout(RECEIVE_CONNECT_TIMEOUT, manager.receive_messages()).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) if receive_stream_unbound(&error) => {
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stream_unbound",
                    account = %account_hash,
                    %error,
                    "receive stream rejected credentials (401/403); stopping until relink"
                );
                return StreamOutcome::Unbound;
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stream_connect",
                    account = %account_hash,
                    %error,
                    "receive stream connection failed; retrying with backoff"
                );
                return StreamOutcome::Recoverable;
            }
            Err(_) => {
                tracing::warn!(
                    target: "engine.receive",
                    stage = "receive_stream_connect_timeout",
                    account = %account_hash,
                    "receive stream connection timed out; retrying with backoff"
                );
                return StreamOutcome::Recoverable;
            }
        };
    // 建流成功证据行：旧实现成功路径零日志，排障只能靠 profile refresh
    // 等间接信号推测接收是否起来。每条新流恰一行（重连风暴由退避限速）。
    tracing::warn!(
        target: "engine.receive",
        stage = "receive_stream_established",
        account = %account_hash,
        "receive stream established"
    );
    // 存量联系人/会话补同步（欠账 §10-1，官方行为对齐：signal-cli/桌面
    // 链接后同样向主设备要一轮 sync）：链路建立 2s 沉降后上行官方
    // sendSyncRequests 三请求（contacts/configuration/block，P15c 项4——
    // 此前仅 contacts 一腿），主设备回传经接收流落 contacts/配置/拉黑
    // 各面 → connector 消费。warn-only（本层零 stderr，失败静默）；每账号
    // 60s 一次性（initial_sync_due 单点闸），watchdog 断连重开不重复。
    // 群组不在本面（presage 栈无 groups sync 消费路径，首条群消息渐进落
    // 库=官方链接设备同款）；群会话完整性由本批登录期 manifest 自动拉补
    // （同一时机带，见 sync 臂体）。
    let mut sync_pending =
        if initial_sync_due((*last_contacts_sync).map(|at| at.elapsed().as_millis() as u64)) {
            Some(tokio::time::Instant::now() + Duration::from_secs(2))
        } else {
            None
        };
    // T-9 fetchLatest 消费臂防抖簿（触发位在 Received::Content 臂；
    // 500ms trailing 窗沿毫秒簿记，窗沿 select 臂执行拉取）。
    let mut manifest_debounce = ManifestPullDebounce::default();
    // prekey 定时维护节律（B 批，2 天水位巡检 + 按龄轮换）：下一拍 = 书签
    // +2 天（书签在 store kv，跨重连/重启有效；首拍即到期立即巡检）。
    // tick 内部自读书签判到期，重连重开不重复轰服务器。
    // 台账 §25.5→§26：维护拨第二条 identified WS 曾与单连接纪律互斥
    // （纪律终止收流、拨号挂 30s、断连循环 450+ 次/日）。根修：presage
    // account_manager 改为复用活动 WS（registered.rs account_manager 传
    // require_unused=false），不再拨第二条——维护恢复默认开启。
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
                    // 链接完成 sync 上行三请求（P15c 项4）：官方
                    // sendSyncRequests 同位——成功/失败都不重试（一次性
                    // 纪律，下次链路建立再补）。
                    let sync_started = std::time::Instant::now();
                    if tokio::time::timeout(
                        RECEIVE_CONNECT_TIMEOUT,
                        send_initial_sync_requests(&mut manager),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            target: "engine.receive",
                            stage = "initial_sync_requests_timeout",
                            account = %account_hash,
                            "initial sync requests timed out; returning to receive loop"
                        );
                    }
                    // S2a 打点：存量补同步三请求耗时（一次性面基线）。
                    perf_metrics::record_us(
                        perf_metrics::keys::INITIAL_SYNC_REQUESTS,
                        sync_started.elapsed().as_micros() as u64,
                    );
                    // T-9 登录期 manifest 自动拉（官方 afterFirstAuthSocket
                    // Connect 同位——首连授权 socket 建立即重读 manifest；
                    // 会话列表完整性含无消息群会话依赖它，正是「群会话登录
                    // 后不显示」的主根因补口）：与三 sync 请求同一沉降时机
                    // 带，走 pull_storage_manifest 单点（merge 两族权威函数
                    // 面 + storage_watch 观测出口）。connector 登录期显式
                    // merge 保留（读面幂等 + 簿防回声，双链无叠加效应）。
                    // 60s 一次性随 sync_pending 继承，重连风暴不重复轰。
                    // 独立任务 + 30s 上限：manifest 拉取再慢也不许把收流
                    // 循环按在原地（2026-10-21 事故：维护臂无限等待 =
                    // 收流静默）。
                    // （收件箱闸批①）：JoinHandle 保留给本臂末尾的完成
                    // join——`engine.initialSyncComplete` 在 manifest +
                    // 首轮 profile 刷新两腿全部落定（含超时面）后才发，
                    // 任务体的 30s 上限与超时日志面零变化。
                    let manifest_service = Arc::clone(service);
                    let manifest_account = number.to_owned();
                    let manifest_join = tokio::task::spawn_local(async move {
                        let outcome = tokio::time::timeout(
                            RECEIVE_JOB_TIMEOUT,
                            pull_storage_manifest(
                                &manifest_service,
                                &manifest_account,
                                perf_metrics::keys::STORAGE_MANIFEST_PULL_LOGIN,
                            ),
                        )
                        .await
                        .ok();
                        if outcome.is_none() {
                            tracing::warn!(
                                target: "engine.receive",
                                stage = "storage_manifest_pull_login_timeout",
                                account = %crate::methods::probe_account8(Some(&manifest_account)),
                                "login manifest pull timed out; receive loop stays live"
                            );
                        }
                        outcome
                    });
                    // profile 名字材料自动批（2026-10-16 profile 名字刷新
                    // 批，compat §5）：contacts sync / manifest 异步回传，
                    // +10s 沉降后跑刷新单点（`profiles.refreshContacts`
                    // 同一函数面）。spawn_local 独立任务不阻塞收流循环；
                    // best-effort 失败静默（零 stderr 纪律，显式方法/下次
                    // 链路建立重补；读面幂等）。触发面继承 60s 一次性纪律
                    // （重连风暴不重复），每窗至多一批。
                    // （收件箱闸批①）：JoinHandle 保留给完成 join；预热
                    // 两步移入 join 臂随通知之后（见下）。
                    let refresh_service = Arc::clone(service);
                    let refresh_account = number.to_owned();
                    let refresh_join = tokio::task::spawn_local(async move {
                        tokio::time::sleep(Duration::from_millis(
                            profile_refresh::AUTO_REFRESH_SETTLE_MS,
                        ))
                        .await;
                        profile_refresh::refresh_contact_profiles(
                            &refresh_service,
                            &refresh_account,
                        )
                        .await
                    });
                    // 收件箱闸批① join 臂：官方 background.preload.ts 步骤 7
                    // 「Wait for critical post-registration syncs before
                    // showing inbox」的引擎侧完成时刻——两腿 join 落定后恒发
                    // 一条 `engine.initialSyncComplete`（逐腿如实布尔，失败
                    // 腿 false；官方 complete 信号仅成功路径发的语义由消费方
                    // 按 ok face 落定）。预热两步（send 性能批/whitelist 批，
                    // compat §5）沿原序随其后：gate 放行前收件箱不可达，预热
                    // 顺位后移零观感、语义零变化（≤10s 整批 + 失败静默不变）。
                    let join_service = Arc::clone(service);
                    let join_account = number.to_owned();
                    tokio::task::spawn_local(async move {
                        // JoinHandle 本身是 Result（JoinError = 任务 panic）——
                        // 两层都如实折进 face：内层超时（None）/该腿 Err/外层
                        // JoinError 任一 = false（官方失败不发 complete 同义，
                        // 桌面半边超时兜底）。
                        let manifest_ok = match manifest_join.await {
                            Ok(Some((contacts, groups))) => {
                                contacts.is_ok() && groups.is_ok()
                            }
                            _ => false,
                        };
                        let profiles_ok = refresh_join
                            .await
                            .map(|refreshed| refreshed.is_ok())
                            .unwrap_or(false);
                        join_service
                            .notify(
                                &join_account,
                                initial_sync_complete_notification(
                                    &join_account,
                                    manifest_ok,
                                    profiles_ok,
                                ),
                            )
                            .await;
                        // 启动预热批（send 性能批 2026-10-16，compat §5）：
                        // 刷新批之后同一沉降带一次发送路径预热（近活跃会话
                        // 目标收集 → 独立 Manager ensure_session，零真实
                        // 消息、不阻塞真实发送、≤10s 整批、失败静默 perf
                        // 计数）。触发面继承 60s 一次性纪律（重连风暴不
                        // 重复轰），每窗至多一批。
                        send_warmup::warm_after_profile_refresh(
                            &join_service,
                            &join_account,
                        )
                        .await;
                        // whitelist 冷启动预热（发送长尾修3，compat §5）：
                        // 同一沉降带最后一步——profileSharing 闸位 map 在
                        // 真实发送前提前回填（TTL 过期/进程冷启动后的首条
                        // send 免付 ~1.8s Storage 全量拉取税；探针行
                        // engine.whitelist-warmup 承载验证面，cache=
                        // hit|filled|err 固定词）。best-effort 失败静默不
                        // 上抛、失败不毒缓存；60s 一次性纪律随本臂继承
                        // （重连风暴不重复轰），每窗至多一次。
                        send_warmup::warm_whitelist_cache(
                            &join_service,
                            &join_account,
                        )
                        .await;
                    });
                    *last_contacts_sync = Some(std::time::Instant::now());
                }
            }, if sync_pending.is_some() => {
                sync_pending = None;
            }
            // T-9 fetchLatest STORAGE_MANIFEST 消费臂窗沿：到期 → manifest
            // 重读 + merge（pull_storage_manifest 单点）→ 观测簿差分通知
            // 自然出口。take_due 恰一次清位（假醒重睡不丢不重）；窗内新封
            // 在 Content 臂推后沿（官方 sync job debounce 同款），多封合并
            // 一次拉。拉取期间收流循环串行等待（单任务 select，无并发信封
            // 处理）——读面幂等，与 connector 驱动的显式 merge 并发安全见
            // ADR 0007 / compat §5 互斥结论。
            _ = async {
                if let Some(at) = manifest_debounce.pending_deadline() {
                    tokio::time::sleep(Duration::from_millis(
                        at.saturating_sub(now_ms()),
                    ))
                    .await;
                }
            }, if manifest_debounce.pending_deadline().is_some() => {
                if manifest_debounce.take_due(now_ms()) {
                    // 同登录期腿：manifest 拉取转后台 + 30s 上限，收流循环
                    // 绝不在维护调用上无限等待。
                    let pull_service = Arc::clone(service);
                    let pull_account = number.to_owned();
                    tokio::task::spawn_local(async move {
                        if tokio::time::timeout(
                            RECEIVE_JOB_TIMEOUT,
                            pull_storage_manifest(
                                &pull_service,
                                &pull_account,
                                perf_metrics::keys::STORAGE_MANIFEST_PULL,
                            ),
                        )
                        .await
                        .is_err()
                        {
                            tracing::warn!(
                                target: "engine.receive",
                                stage = "storage_manifest_pull_timeout",
                                account = %crate::methods::probe_account8(Some(&pull_account)),
                                "manifest pull timed out; receive loop stays live"
                            );
                        }
                    });
                }
            }
            _ = async {
                if let Some(at) = maint_at {
                    tokio::time::sleep_until(at).await;
                }
            }, if maint_at.is_some() => {
                // 下一拍固定 +2 天（tick 内书签判到期，短拍只做本地判定）。
                // manager 调用放臂体：select 其他 future 已弃，借用不冲突。
                // 30s 硬上限：网络维护不许挂死收流。
                if tokio::time::timeout(
                    RECEIVE_SIDE_TIMEOUT,
                    prekey::maintenance_tick(&mut manager),
                )
                .await
                .is_err()
                {
                    tracing::warn!(
                        target: "engine.receive",
                        stage = "prekey_maintenance_timeout",
                        account = %account_hash,
                        "prekey maintenance timed out; returning to receive loop"
                    );
                }
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
                    // S2a 打点：快速巡检触发源计数（时间拍）。
                    perf_metrics::add(perf_metrics::keys::PREKEY_QUICK_TIME_TRIGGER, 1);
                    if tokio::time::timeout(
                        RECEIVE_SIDE_TIMEOUT,
                        prekey::quick_check(&mut manager),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            target: "engine.receive",
                            stage = "prekey_quick_check_timeout",
                            account = %account_hash,
                            "prekey quick check timed out; returning to receive loop"
                        );
                    }
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
                // S2a 打点：信封级到达起点（ack 前逐臂落 RECEIVE_ENVELOPE）。
                let envelope_started = std::time::Instant::now();
                // A1 观测：该账号接收在飞置 1（离开臂体含提前 return 自动归 0）。
                let _inflight = account_metrics::ReceiveInflight::enter(number);
                match item {
                // QueueEmpty：历史队列排空，流继续保持（queue 标记在
                // messagepipe 层已就地 ack）。G3：排空态 additive 上抛
                // （官方 ApiEmptyQueue→EmptyEvent 同位；引擎只报态，连接
                // 器/desktop 消费自定）。
                Some(Received::QueueEmpty) => {
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_QUEUE_EMPTY, 1);
                    // 时钟偏移闸武装锚：排空标记到达 = 实时相开始，此后
                    // 信封才允许喂闸（服务器侧 RedisDynamoDbMessagePublisher
                    // 「initial queue drain complete signal」——零存量建连
                    // 也恒产出、每流恰一次，vendored presage 无条件透传
                    // 不丢弃，武装必达）。
                    skew_gate.arm();
                    service
                        .notify(number, queue_emptied_notification(number))
                        .await;
                }
                // contacts 由 presage 自动入库（号码/名字面随之可用）。P15a
                // 项3：解析后的联系人记录随 Received::Contacts 上抛（vendored
                // ContactSync 载体，fork 登记 vendor/README.md），投影 receive
                // 事件供 connector upsert 自有库（官方 contactSync 消费同位）。
                // G1：ack 前等 flush 栅栏（投影可达连接器才耗服务器队列）。
                Some(Received::Contacts(sync, ack)) => {
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_CONTACTS, 1);
                    quick_seen += 1;
                    for notification in project_contacts_sync(number, &sync) {
                        service.notify(number, notification).await;
                    }
                    if !service.flush_barrier(number).await {
                        return StreamOutcome::StdioDead;
                    }
                    record_receive_envelope(number, envelope_started);
                    ack.ack();
                }
                // 解密失败：官方语义同样是 ack（Android IncomingMessageObserver
                // 落库事务后逐条 ack，解密失败也 ack），否则服务器队列堆积；
                // 同时结构化上抛 connector（不再静默丢弃）。DERM 自愈两档
                // （官方 Desktop handleRetry 同型）：我方解密失败 → 明文腿
                // 回发 DecryptionErrorMessage；收到对端重试请求 → 按官方闸序
                // （超限早退 / archive-on-match / 找回重发 / 兜底）处理。
                // 全链路失败静默，绝不影响 ack。
                Some(Received::DecryptionError(sender, timestamp, failure, ack)) => {
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_DECRYPTION_ERROR, 1);
                    service
                        .notify(
                            number,
                            decryption_error_notification(number, &sender, timestamp),
                        )
                        .await;
                    if !service.flush_barrier(number).await {
                        return StreamOutcome::StdioDead;
                    }
                    if let Some(failure) = failure {
                        if tokio::time::timeout(
                            RECEIVE_CONNECT_TIMEOUT,
                            derm_handle(&mut manager, sender, failure, derm_guard),
                        )
                        .await
                        .is_err()
                        {
                            tracing::warn!(
                                target: "engine.receive",
                                stage = "derm_handle_timeout",
                                account = %account_hash,
                                "DERM retry handling timed out; returning to receive loop"
                            );
                        }
                    }
                    quick_seen += 1;
                    record_receive_envelope(number, envelope_started);
                    ack.ack();
                }
                // 重复投递（store 已有同源行）：已投影过，仅补 ack 不再通知。
                Some(Received::Duplicate(ack)) => {
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_DUPLICATE, 1);
                    quick_seen += 1;
                    record_receive_envelope(number, envelope_started);
                    ack.ack();
                }
                // 群取回失败（接收完整性批②，compat §3 additive）：同信封
                // 触发的群 update fetch/decrypt 在 vendored 重试环耗尽。解码
                // 类 → groupSyncFailed 事件（{groupId, count}）；404 群在
                // 服务器已消失 → 复用 groupTerminated 事件（terminator=null）
                // 并登记 terminated 注册表（发送面同步拦截）。两臂均无 ack
                // ——单 ack 契约由同信封 Content 槽承担（本体消息照常投影）。
                Some(Received::GroupSyncFailed {
                    master_key,
                    kind,
                    attempts,
                }) => {
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_GROUP_SYNC_FAILED, 1);
                    quick_seen += 1;
                    use base64::Engine as _;
                    let group_id =
                        base64::engine::general_purpose::STANDARD.encode(master_key);
                    match kind {
                        presage::model::messages::GroupSyncFailureKind::Decode => {
                            service
                                .notify(
                                    number,
                                    group_sync_failed_notification(number, &group_id, attempts),
                                )
                                .await;
                        }
                        presage::model::messages::GroupSyncFailureKind::Gone => {
                            service.terminated_groups.register(&group_id);
                            service
                                .notify(number, group_gone_notification(number, &group_id))
                                .await;
                        }
                    }
                    if !service.flush_barrier(number).await {
                        return StreamOutcome::StdioDead;
                    }
                    record_receive_envelope(number, envelope_started);
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
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_IDENTITY_CHANGE, 1);
                    let sid_string = sender.service_id_string();
                    let _ = identity::mark_identity_replaced(dir, &sid_string, now_ms());
                    // P15b 项8：偏移观测（IdentityChange 臂同 Content 臂；
                    // 积压免疫武装前置——disarmed 时 no-op）。
                    skew_gate
                        .observe(
                            service,
                            number,
                            content.metadata.server_timestamp.timestamp_millis(),
                        )
                        .await;
                    // A1 观测：与 Content 臂同口径的接收延迟。
                    account_metrics::record_receive_lag(
                        number,
                        now_ms().saturating_sub(
                            content.metadata.server_timestamp.timestamp_millis().max(0) as u64,
                        ),
                    );
                    service
                        .notify(
                            number,
                            identity_changed_notification(
                                number,
                                &sender,
                                content.metadata.client_timestamp.timestamp_millis().max(0) as u64,
                            ),
                        )
                        .await;
                    for notification in
                        project_content(
                            number,
                            manager.store(),
                            &content,
                            &service.terminated_groups,
                            Some(service),
                        )
                        .await
                    {
                        service.notify(number, notification).await;
                    }
                    // G1：ack 前等 flush 栅栏（同 Content 臂）。
                    if !service.flush_barrier(number).await {
                        return StreamOutcome::StdioDead;
                    }
                    quick_seen += 1;
                    record_receive_envelope(number, envelope_started);
                    ack.ack();
                }
                Some(Received::Content(content, ack)) => {
                    // S2a 打点：Content 臂信封计数。
                    perf_metrics::add(perf_metrics::keys::RECEIVE_KIND_CONTENT, 1);
                    // 延迟探针（契约 §4.90 服务器时间戳的观测用法）：
                    // server_timestamp 是**服务器收到该信封**的时刻，now 是 engine
                    // 处理它的时刻，差值 = 服务端→engine 的推送延迟。这正是
                    // 「WS 是否把消息及时送到」的直接度量，与对端时钟无关。
                    // 注意：重连后的队列排水期差值天然很大（积压补投），属预期。
                    // 只打毫秒差，不含任何内容/号码。
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let server_ms =
                            content.metadata.server_timestamp.timestamp_millis() as u64;
                        if now_ms > server_ms {
                            tracing::warn!(
                                target: "engine.latency",
                                stage = "content_arrival",
                                delay_ms = now_ms - server_ms,
                                "server-to-engine push latency"
                            );
                        }
                    }
                    // P15b 项8 时钟偏移闸：服务器时间戳 vs 本地钟（边缘触发
                    // 告警上抛，不拒信封不停车，§5 偏差登记；积压免疫武装
                    // 前置——排水完成前的信封不喂闸，见 ClockSkewGate）。
                    skew_gate
                        .observe(
                            service,
                            number,
                            content.metadata.server_timestamp.timestamp_millis(),
                        )
                        .await;
                    // A1 观测：服务器入队时刻 → 本地处理时刻（负偏移归 0；
                    // 时钟偏移本身由上面 ClockSkewGate 独立告警）。
                    account_metrics::record_receive_lag(
                        number,
                        now_ms().saturating_sub(
                            content.metadata.server_timestamp.timestamp_millis().max(0) as u64,
                        ),
                    );
                    // B4 身份事件面：verified 同步下行（多设备信任态收敛）
                    // ——sidecar 权威突变 + receive 事件投影，先于普通投影
                    // 面（该体在 project_content 恒空投影，零冲突）。
                    if let Some(notification) =
                        project_verified_sync(number, manager.store(), &content, dir).await
                    {
                        service.notify(number, notification).await;
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
                            Some(service),
                        )
                        .await
                    {
                        service.notify(number, notification).await;
                    }
                    if !service.flush_barrier(number).await {
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
                    // T-9 fetchLatest 消费臂触发位：STORAGE_MANIFEST 信封 →
                    // 防抖窗沿推后（拉取在独立 select 臂执行，不在信封咽喉
                    // 内阻塞 ack 时序）；非 STORAGE_MANIFEST type 不触发
                    // （LOCAL_PROFILE/SUBSCRIPTION_STATUS/UNKNOWN/type 缺席，
                    // 信号面投影照旧）。IdentityChange 臂不触发：自发 sync
                    // 信封换钥解密路径现实中不可达，消费臂单点收口。
                    if storage_manifest_fetch_requested(&content) {
                        manifest_debounce.on_fetch(now_ms());
                    }
                    quick_seen += 1;
                    record_receive_envelope(number, envelope_started);
                    ack.ack();
                }
                    None => {
                        // 官方 SocketManager 语义（:391-394）：1000（normal
                        // closure）与 4409（connected elsewhere）不重连；
                        // 另加引擎适配 4000（本地单连接槽接管）——旧管线
                        // 不得重拨抢回已被新连接接管的槽。其余关闭/传输
                        // 失败回到 Fibonacci 重连。旧实现把 4409 当
                        // Connected 立即重连，会与被夺走的客户端互踢振荡。
                        let close_code = manager.identified_websocket_close_code().await;
                        if receive_close_is_terminal(close_code) {
                            tracing::warn!(
                                target: "engine.receive",
                                stage = "receive_stream_closed_no_reconnect",
                                account = %account_hash,
                                close_code = ?close_code,
                                "receive stream closed with a no-reconnect code (1000/4409/4000)"
                            );
                            return StreamOutcome::Terminated;
                        }
                        return StreamOutcome::Connected;
                    }
                }
                // prekey 快速巡检量臂（P12/R4）：窗内 ≥200 信封即快速巡检
                // 一次，计数与时间臂计时器同复位（含解密失败/重复投递——
                // 服务器侧取信封同样消耗一次性 prekey）。
                if prekey::quick_check_volume_due(std::mem::replace(&mut quick_seen, 0)) {
                    // S2a 打点：快速巡检触发源计数（量拍）。
                    perf_metrics::add(perf_metrics::keys::PREKEY_QUICK_VOLUME_TRIGGER, 1);
                    if tokio::time::timeout(
                        RECEIVE_SIDE_TIMEOUT,
                        prekey::quick_check(&mut manager),
                    )
                    .await
                    .is_err()
                    {
                        tracing::warn!(
                            target: "engine.receive",
                            stage = "prekey_quick_check_volume_timeout",
                            account = %account_hash,
                            "prekey quick check timed out; returning to receive loop"
                        );
                    }
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
/// S2a 打点薄壳：投影阶段时长（RECEIVE_PROJECT；函数体在
/// [`project_content_inner`] 原样未动——只测不改，控制流零变化）。
async fn project_content(
    account: &str,
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
    terminated_groups: &groups::TerminatedGroups,
    service: Option<&Arc<Service>>,
) -> Vec<Value> {
    let started = std::time::Instant::now();
    let projected =
        project_content_inner(account, store, content, terminated_groups, service).await;
    perf_metrics::record_us(
        perf_metrics::keys::RECEIVE_PROJECT,
        started.elapsed().as_micros() as u64,
    );
    projected
}

async fn project_content_inner(
    account: &str,
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
    terminated_groups: &groups::TerminatedGroups,
    service: Option<&Arc<Service>>,
) -> Vec<Value> {
    use presage::libsignal_service::content::ContentBody;

    let envelope_ts = content.metadata.client_timestamp.timestamp_millis().max(0) as u64;
    let mut envelope = json!({
        "sourceUuid": content.metadata.sender.service_id_string(),
        "sourceDevice": u32::from(content.metadata.sender_device),
        "timestamp": envelope_ts,
    });
    // P15c 项5（compat §3/§5）：Envelope.serverTimestamp 信封公共位投影
    // （服务器为该条消息记录的接收时间，官方事件面同位
    // MessageReceiver.preload.ts:1178）。vendored Metadata.server_timestamp
    // 同源捕获（cipher 解密自 Envelope.server_timestamp proto 位），恒有值
    // 恒投（wire 缺席 = proto 缺省 0，官方 ?? 0n 同口径照投，不发明不省
    // 键）；消费面 = 48h 编辑窗判据（isTooOldToEditMessage，多设备编辑
    // echo 补齐面）+ 消息行服务器时间。editMessage/sent echo 同走本信封，
    // 单键齐备编辑消费面。
    envelope["serverTimestamp"] =
        json!(content.metadata.server_timestamp.timestamp_millis().max(0) as u64);
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
            // stickerPackSync 收投影臂（接收完整性批③，N1 缺口闭合，compat
            // §3）：`syncMessage.stickerPackOperation` repeated 直挂字段独占
            // 分发（官方 #handleStickerPackOperation 词条同形，
            // MessageReceiver.preload.ts:3093/:3413）——此前该信封落 read/
            // viewed 兜底臂整信封丢弃。词条 {id: packId 小写 hex, key:
            // packKey 标准 base64, isInstall, isRemove}（源位缺席/空 → 该键
            // 省略）；type 裸位判定（prost 枚举 getter 会把未知值兜成缺省
            // INSTALL，宁缺勿错须跳整条）；写面 best-effort：install 须
            // 32B packKey（缺/短只跳写面、事件照投），uninstall 走既有
            // Storage Service 通道（P5 批 409 采纳重放+幂等同语义），写失败
            // 事件照投（主设备自有写面 + manifest 读收敛兜底）；packId 空
            // 词条只投影不写；服务面缺席（单测注入 None）只投影。
            if !sync.sticker_pack_operation.is_empty() {
                use presage::proto::sync_message::sticker_pack_operation::Type;
                let mut entries = Vec::with_capacity(sync.sticker_pack_operation.len());
                for operation in &sync.sticker_pack_operation {
                    let (is_install, is_remove) = match operation.r#type {
                        // proto 缺省 = INSTALL（官方 Type::INSTALL=0 同语义）。
                        None => (true, false),
                        Some(value) if value == Type::Install as i32 => (true, false),
                        Some(value) if value == Type::Remove as i32 => (false, true),
                        Some(_) => continue,
                    };
                    let pack_id = operation.pack_id();
                    let pack_key = operation.pack_key();
                    let mut entry = serde_json::Map::new();
                    if !pack_id.is_empty() {
                        entry.insert("id".into(), json!(hex_encode(pack_id)));
                    }
                    if !pack_key.is_empty() {
                        use base64::Engine as _;
                        entry.insert(
                            "key".into(),
                            json!(base64::engine::general_purpose::STANDARD.encode(pack_key)),
                        );
                    }
                    entry.insert("isInstall".into(), json!(is_install));
                    entry.insert("isRemove".into(), json!(is_remove));
                    if let Some(service) = service {
                        let intent = match (is_install, pack_key.try_into()) {
                            (true, Ok(pack_key)) if !pack_id.is_empty() => {
                                Some(storage_sticker::StickerPackWrite::Install {
                                    pack_id: pack_id.to_vec(),
                                    pack_key,
                                    position: None,
                                })
                            }
                            (false, _) if !pack_id.is_empty() => {
                                Some(storage_sticker::StickerPackWrite::Uninstall {
                                    pack_id: pack_id.to_vec(),
                                })
                            }
                            _ => None,
                        };
                        if let Some(intent) = intent {
                            let _ = service.set_sticker_pack_sync(account, intent).await;
                        }
                    }
                    entries.push(Value::Object(entry));
                }
                if !entries.is_empty() {
                    envelope["syncMessage"] = json!({ "stickerPackSync": entries });
                    return vec![json!({
                        "jsonrpc": "2.0",
                        "method": "receive",
                        "params": { "account": account, "envelope": envelope },
                    })];
                }
                // 全部词条未知 type 值 → 整信封无可投影（宁缺勿错），落
                // 下方既有 sync 臂兜底。
            }
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
                    } else if sent.is_recipient_update == Some(true)
                        && !sent.story_message_recipients.is_empty()
                    {
                        // S5 story 删除扇出批：官方 storyRecipientUpdate 门
                        // （MessageReceiver.preload.ts:3002-3030——
                        // isRecipientUpdate ∧ storyMessageRecipients 非空 →
                        // StoryRecipientUpdateEvent，判定先于 storyMessage 面
                        // 同序镜像）。官方多名单 story 发送 sync（sendStory
                        // isSyncMessageUpdate = 有名单即 isUpdate=true）同落
                        // 此脸——行为变化记档 compat §5：该形状此前投影
                        // storyMessage echo。形状镜像官方事件体
                        // StoryRecipientUpdateData {destinationServiceId,
                        // timestamp, storyMessageRecipients}（timestamp = 信封
                        // ts 官方同源；destination = 信封 destinationServiceId
                        // 恒自有 ACI 不投——sync 恒指向自有号纪律）。
                        envelope["syncMessage"] = json!({
                            "storyRecipientUpdate": {
                                "timestamp": envelope_ts,
                                "storyMessageRecipients": Value::Array(
                                    sent.story_message_recipients
                                        .iter()
                                        .map(sent_story_message_recipient_json)
                                        .collect(),
                                ),
                            }
                        });
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
                // Q6② + P15a 项1 + attachmentDeletes leg 1/3：deleteForMe
                // 收包投影（SyncContent.deleteForMe=22，本机「为我删除」多
                // 设备同步）：vendored 接收面已对 presage store 做
                // messageDeletes 消费（registered.rs save_message 臂）且
                // Received::Content 无条件上抛，引擎投影给 connector 供其删
                // 自有库行。P15a 项1 补整会话清除族两臂（官方
                // handleDeleteForMeSync conversationDeletes/
                // localOnlyConversationDeletes 同位，桌面会话清除同步面）；
                // 本批补第四类型 attachmentDeletes（官方 :3724-3790 同位；
                // leg 1/3 = 引擎投影先行，connector/桌面消费待后续批）。
                // 作者不可解析条目丢弃（vendored store 面 warn-and-continue
                // 同口径）；无有效条目整信封丢弃。
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
                    //
                    // 三键布尔面评估批（2026-10-17）：AEP 存在性布尔一位
                    // additive（材料不出边界，Present 后缀与官方材料面
                    // string 同名键区隔）；masterKey 存在性不可得（评估
                    // 结论与边界原因落评估批块）；mediaRootBackupKey 材料
                    // 在场 = 存在性超集不重复投布尔。
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
                    if keys
                        .account_entropy_pool
                        .as_deref()
                        .is_some_and(|pool| !pool.is_empty())
                    {
                        face.insert("accountEntropyPoolPresent".into(), json!(true));
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
                // attachmentBackfillResponse 收投影臂（官方
                // content oneof attachmentBackfillResponse=25，
                // #handleAttachmentBackfillResponse 同位
                // MessageReceiver.preload.ts:3146-3151/:3806——此前落兜底臂
                // 整信封丢）。最小对齐：解包投影 targetMessage/
                // targetConversation + 回填结果（error 词或 attachments 批，
                // status 词/指针全投影），引擎不消费回填状态——connector 消费
                // 面后续立项（compat §5 记档）。词表 additive。
                Some(presage::proto::sync_message::Content::AttachmentBackfillResponse(
                    response,
                )) => {
                    let Some(projection) = project_attachment_backfill_response(response) else {
                        return Vec::new();
                    };
                    envelope["syncMessage"] = json!({ "attachmentBackfillResponse": projection });
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
        // connector。接收面实证（sender key 互操作批复核批 2026-10-18）：
        // SKDM 走 Content proto 侧车字节位，cipher 层在投影之前已消费入
        // 协议存储（SKDM-only 信封 Content::from_proto 判 UnsupportedContent，
        // 本臂实际不可达，纯防御；收发往返钉面 =
        // group_mrm::tests::skdm_receive_stores_key_and_group_decrypt_roundtrip）。
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

/// DeleteForMe 投影（Q6② + P15a 项1 + attachmentDeletes leg 1/3，compat
/// 批块）：messageDeletes 臂逐组投影——`[{conversationId?, messages:
/// [{author, timestamp}]}]`（组无 conversation 照投、conversationId 键省略
/// ——Q6② 既有口径不动）；conversationDeletes 臂（官方 handleDeleteForMeSync
/// 同位 MessageReceiver.preload.ts:3658-3697）——`[{conversationId,
/// isFullDelete, mostRecentMessages, mostRecentNonExpiringMessages}]`，
/// conversation 缺席或 mostRecentMessages 清空整组丢（官方 warn-drop 同
/// 口径），isFullDelete 官方 Boolean() 恒布尔；localOnlyConversationDeletes
/// 臂（:3699-3722）——`[{conversationId}]`，conversation 缺席丢；
/// attachmentDeletes 臂（:3724-3790，leg 1/3）——
/// `[{conversationId, targetMessage: {author, timestamp}, clientUuid?,
/// fallbackDigest?, fallbackPlaintextHash?}]`，conversation/targetMessage
/// 不可解析或三腿全缺整条丢（见 [`project_delete_for_me_attachment`]）。
/// author 二进制（ACI/PNI 16/17B）优先、字符串/e164 直通；author 不可解析
/// 条目丢弃（vendored store 面 warn-and-continue 同口径）。四臂皆无有效
/// 条目 → None（调用方整信封丢弃，官方「Nothing found in sync message」
/// throw 同口径）。
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

    let attachment_deletes: Vec<Value> = delete
        .attachment_deletes
        .iter()
        .take(MAX_DELETE_FOR_ME_ATTACHMENT_DELETES)
        .filter_map(project_delete_for_me_attachment)
        .collect();
    if !attachment_deletes.is_empty() {
        face.insert("attachmentDeletes".into(), Value::Array(attachment_deletes));
    }

    (!face.is_empty()).then_some(Value::Object(face))
}

/// attachmentDeletes 条目帽：对齐 connector 消费面
/// `MAX_SYNC_DELETE_GROUP_MESSAGES`（kt-signal-connector src/engine.rs:1953，
/// =256；该符号定义在 connector，引擎侧无同名常量，按同值做恶意批量兜底）。
/// 官方接收面不设帽、真实批量是「每次用户操作一条」，超帽仅可能命中恶意
/// 面；超出条目丢弃（逐条独立，不改变保留条目的删除语义）。
const MAX_DELETE_FOR_ME_ATTACHMENT_DELETES: usize = 256;

/// DeleteForMe attachmentDeletes 条目投影（leg 1/3，compat §5）：官方
/// handleDeleteForMeSync attachmentDeletes 臂（MessageReceiver.preload.ts:
/// 3724-3790）同构——conversation 不可解析整条丢（官方「No target
/// conversation」warn-drop，:3744）；targetMessage 不可解析整条丢
/// （官方「No target message」，:3750；author/sentTimestamp 双闸 =
/// processAddressableMessage :4415-4429，此处比既有 messageDeletes 臂的
/// 「timestamp 缺省 0」更严——破坏性单附件删除不接受无时刻锚点）；
/// clientUuid/fallbackDigest/fallbackPlaintextHash 三腿全缺整条丢
/// （:3765-3767）；各腿空字节 = 缺席（官方 `?.length` 同构）。字节转换同构：
/// clientUuid = bytesToUuid（16B 且非全零才投，uuidToBytes.std.ts:21-36）、
/// fallbackDigest = Bytes.toBase64（标准 base64）、fallbackPlaintextHash =
/// Bytes.toHex（小写 hex）。
fn project_delete_for_me_attachment(
    delete: &presage::proto::sync_message::delete_for_me::AttachmentDelete,
) -> Option<Value> {
    use base64::Engine as _;

    let conversation = delete_for_me_conversation_id(&delete.conversation)?;
    let target = delete.target_message.as_ref()?;
    let target_message = project_delete_for_me_message(target)?;
    // 官方 processAddressableMessage 的 sentTimestamp warn-drop（:4423-4429）
    // ——见函数注释：本臂不继承 messageDeletes 臂的缺省 0。
    target.sent_timestamp?;

    let client_uuid = delete
        .client_uuid
        .as_deref()
        .filter(|bytes| !bytes.is_empty())
        .and_then(|bytes| {
            presage::libsignal_service::prelude::Uuid::from_slice(bytes)
                .ok()
                .filter(|uuid| !uuid.is_nil())
                .map(|uuid| uuid.to_string())
        });
    let fallback_digest = delete
        .fallback_digest
        .as_deref()
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
    let fallback_plaintext_hash = delete
        .fallback_plaintext_hash
        .as_deref()
        .filter(|bytes| !bytes.is_empty())
        .map(hex_encode);

    if client_uuid.is_none() && fallback_digest.is_none() && fallback_plaintext_hash.is_none() {
        return None;
    }

    let mut item = serde_json::Map::new();
    item.insert("conversationId".into(), json!(conversation));
    item.insert("targetMessage".into(), target_message);
    if let Some(client_uuid) = client_uuid {
        item.insert("clientUuid".into(), json!(client_uuid));
    }
    if let Some(fallback_digest) = fallback_digest {
        item.insert("fallbackDigest".into(), json!(fallback_digest));
    }
    if let Some(fallback_plaintext_hash) = fallback_plaintext_hash {
        item.insert(
            "fallbackPlaintextHash".into(),
            json!(fallback_plaintext_hash),
        );
    }
    Some(Value::Object(item))
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

/// attachmentBackfillResponse 收投影（compat §5，词表 additive）：官方
/// #handleAttachmentBackfillResponse（MessageReceiver.preload.ts:3806，
/// content oneof attachmentBackfillResponse=25）的最小对齐——引擎不消费
/// 回填状态（connector 后续消费面），只解包投影事件体。target 面 =
/// 官方 processAddressableMessage/processConversationIdentifier 同构：
/// author/sentTimestamp 双闸（官方任一缺席 drop 同判）、conversation
/// service-id binary 优先 string 次之（fromServiceIdBinaryOrString 同序）。
/// data 面 = error 词（未知 i32 投 UNKNOWN，官方兜 0 同口径）或
/// attachments 批（status 词 / attachment 指针全投影
/// [`project_attachment_pointer`] 同源；官方 processBackfilledAttachment
/// 附加的 downloadPath 是桌面本地续传名，引擎不预分配下载面，缺席记档）。
/// 官方 strictAssert 三处失败（targetMessage/targetConversation/attachments
/// 缺席）都走丢信封路径——本投影同步返回 None 丢整信封（宁缺勿错）。
fn project_attachment_backfill_response(
    response: &presage::proto::sync_message::AttachmentBackfillResponse,
) -> Option<Value> {
    use presage::proto::sync_message::attachment_backfill_response::{
        attachment_data, Data as ResponseData,
    };

    let target = response.target_message.as_ref()?;
    let author = match target.author.as_ref()? {
        presage::proto::addressable_message::Author::AuthorServiceId(id) => {
            ServiceId::parse_from_service_id_string(id).map(|sid| sid.service_id_string())
        }
        presage::proto::addressable_message::Author::AuthorServiceIdBinary(bytes) => {
            ServiceId::parse_from_service_id_binary(bytes).map(|sid| sid.service_id_string())
        }
        presage::proto::addressable_message::Author::AuthorE164(number) => {
            Some(number.clone()).filter(|number| !number.is_empty())
        }
    }?;
    let sent_timestamp = target.sent_timestamp?;
    let mut target_message = serde_json::Map::new();
    target_message.insert("author".into(), json!(author));
    target_message.insert("sentTimestamp".into(), json!(sent_timestamp));

    let conversation = response.target_conversation.as_ref()?;
    let mut target_conversation = serde_json::Map::new();
    match conversation.identifier.as_ref()? {
        presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes) => {
            target_conversation.insert(
                "serviceId".into(),
                json!(ServiceId::parse_from_service_id_binary(bytes)
                    .map(|sid| sid.service_id_string())?),
            );
        }
        presage::proto::conversation_identifier::Identifier::ThreadServiceId(id) => {
            target_conversation.insert(
                "serviceId".into(),
                json!(ServiceId::parse_from_service_id_string(id)
                    .map(|sid| sid.service_id_string())?),
            );
        }
        presage::proto::conversation_identifier::Identifier::ThreadGroupId(group_id) => {
            use base64::Engine as _;
            if group_id.is_empty() {
                return None;
            }
            target_conversation.insert(
                "groupId".into(),
                json!(base64::engine::general_purpose::STANDARD.encode(group_id)),
            );
        }
        presage::proto::conversation_identifier::Identifier::ThreadE164(number) => {
            if number.is_empty() {
                return None;
            }
            target_conversation.insert("e164".into(), json!(number));
        }
    }

    let backfill_entry =
        |entry: &presage::proto::sync_message::attachment_backfill_response::AttachmentData| {
            let mut value = serde_json::Map::new();
            match entry.data.as_ref()? {
                attachment_data::Data::Status(raw) => {
                    let word = attachment_data::Status::try_from(*raw)
                        .map(|status| status.as_str_name().to_owned())
                        .unwrap_or_else(|_| "UNKNOWN".to_owned());
                    value.insert("status".into(), json!(word));
                }
                attachment_data::Data::Attachment(pointer) => {
                    value.insert("attachment".into(), project_attachment_pointer(pointer));
                }
            }
            Some(Value::Object(value))
        };

    let mut face = serde_json::Map::new();
    face.insert("targetMessage".into(), Value::Object(target_message));
    face.insert(
        "targetConversation".into(),
        Value::Object(target_conversation),
    );
    match response.data.as_ref()? {
        ResponseData::Error(raw) => {
            let word =
                presage::proto::sync_message::attachment_backfill_response::Error::try_from(*raw)
                    .map(|error| error.as_str_name().to_owned())
                    .unwrap_or_else(|_| "UNKNOWN".to_owned());
            face.insert("error".into(), json!(word));
        }
        ResponseData::Attachments(list) => {
            let attachments: Vec<Value> =
                list.attachments.iter().filter_map(backfill_entry).collect();
            face.insert("attachments".into(), Value::Array(attachments));
            if let Some(long_text) = list.long_text.as_ref() {
                if let Some(entry) = backfill_entry(long_text) {
                    face.insert("longText".into(), entry);
                }
            }
        }
    }
    Some(Value::Object(face))
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
    // 语音笔记元数据收投影（2026-10-14 批）：官方 Android 发送侧波形
    // （audioWaveform=21，恒 100 桶 dB 尺度 0..255）/时长
    // （audioDurationSeconds=22，float 秒）如实透传——waveform 标准
    // base64 非空才投（blurHash 同款，空字节串零信息量），duration 在场
    // 即投。官方发送端恒成对，收侧各自独立如实投位不加成对闸（宁多投
    // 不误判，消费方自判）。
    if let Some(waveform) = pointer
        .audio_waveform
        .as_deref()
        .filter(|waveform| !waveform.is_empty())
    {
        value["waveform"] = json!(base64::engine::general_purpose::STANDARD.encode(waveform));
    }
    if let Some(duration) = pointer.audio_duration_seconds {
        value["duration"] = json!(duration);
    }
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

/// 渲染面 QR URL（startLink `deviceLinkUri` 的最终形状）：把声明位收敛成
/// **单个** `capabilities` 查询参数并保证 `backup5` 在位，值序与参数序对齐
/// 官方（Signal-Desktop `Provisioner.preload.ts` capabilities 数组经
/// toAppUrl 一次成形单参数；`uuid`/`pub_key` 在前，`capabilities` 收尾）。
///
/// 为什么不能两个同名参数：Android 扫码侧 `LinkDeviceViewModel.getCapabilities`
/// 走 `Uri.getQueryParameter`（单数 API，同名参数只取第一个）——重复参数令
/// `backup5` 被静默吞掉，手机判 `VALID_WITHOUT_SYNC`：链接成功但不弹
/// 「转移聊天记录」、不上传迁移档案，引擎空等 archive 至预算上限
/// （2026-10-15 真机取证复现，15:30 链接成功无弹框）。
fn qr_link_url(mut url: url::Url) -> url::Url {
    const BACKUP5: &str = "backup5";
    let mut capabilities: Vec<String> = Vec::new();
    let mut rest: Vec<(String, String)> = Vec::new();
    for (key, value) in url.query_pairs() {
        let (key, value) = (key.into_owned(), value.into_owned());
        if key == "capabilities" {
            for name in value.split(',').filter(|name| !name.is_empty()) {
                if capabilities.iter().all(|existing| existing != name) {
                    capabilities.push(name.to_string());
                }
            }
        } else {
            rest.push((key, value));
        }
    }
    if capabilities.iter().all(|name| name != BACKUP5) {
        capabilities.push(BACKUP5.to_string());
    }
    {
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        pairs.extend_pairs(rest.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        pairs.append_pair("capabilities", &capabilities.join(","));
    }
    url
}

/// 链接主流程：绕过 `Manager::link_secondary_device`（其内部丢弃
/// `ephemeral_backup_key`，且 `RegistrationData` 的 password/profile_key 为
/// pub(crate) 无法外部构造），直调 provisioning 层 `link_device`；QR URL 经
/// `qr_link_url` 收敛为单 `capabilities` 参数并保证 `backup5` 在位（官方
/// Provisioner 语义：手机读到声明才发起存量历史迁移上传；URL 文本层变换，
/// connector 展示面零改动）。拿到
/// `NewDeviceRegistration` 后按官方 store 落库格式经 serde 数据面构造
/// `RegistrationData`，回归既有 `load_registered` 收发管线。key 命中时移交
/// finish_link 后台 best-effort 下载迁移档案，不阻塞链接成功（ADR 0005 §S2
/// 定案）。
///
/// 纪律：失败即整会话作废（删目录 + Err）——provisioning WS 约 90s 服务端
/// 空闲超时，旧 QR 不可复用，重试由 connector 重新 startLink。
///
/// 一次性迁移密钥不在此启动下载：transfer 腿移交 finish_link（见
/// PendingTransferDownload）——本函数返回早于收口清理，spawn 留在这里会被
/// 同号清理确定性误杀（真机四连「永久同步中」的根因）。
async fn link_flow(
    mut store: SqliteStore,
    url_tx: futures::channel::oneshot::Sender<url::Url>,
) -> Result<LinkOutcome, ()> {
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
            let _ = url_tx.send(qr_link_url(url));
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

    // dev 诊断（`KT_ENGINE_LINK_DEBUG=1` 时向 /tmp/kt-engine-link-debug.log
    // 追加一行；生产 env 缺席零输出零文件，compat §1 不破）：直接记录手机
    // provisioning 消息是否携带一次性迁移密钥——真机「链接成功但 transfer
    // 任务从未启动」之谜的证据面（字段链 proto/vendor/link_flow 三方核对
    // 一致后，唯一未知量剩消息本体）。
    if std::env::var("KT_ENGINE_LINK_DEBUG").is_ok() {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/kt-engine-link-debug.log")
        {
            let _ = writeln!(
                f,
                "link_debug: provisioning ephemeral_backup_key_present={} device_id={device_id:?}",
                ephemeral_backup_key.is_some()
            );
        }
    }

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

    // 一次性迁移密钥：原样装入行囊移交 finish_link——收口清理（同号旧
    // 任务 cancel + 进度清缺）必须先于新任务启动，spawn 留在本函数会被
    // finish_link 确定性误杀（真机四连「永久同步中」根因）。导出面 future
    // 非 Send（上游 FramesReader 持 Rc 状态），spawn 仍在 LocalSet 承载。
    let pending_transfer = ephemeral_backup_key.map(|backup_key| PendingTransferDownload {
        aci: service_ids.aci,
        credentials: ServiceCredentials {
            aci: Some(service_ids.aci),
            pni: Some(service_ids.pni),
            phonenumber: phone_number,
            password: Some(password),
            device_id: Some(device_id),
        },
        backup_key,
    });
    Ok((number, pending_transfer))
}

/// transfer_archive 长轮询预算（对齐官方）：单轮服务端挂起
/// `?timeout={TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS}`，204 重试上限
/// [`TRANSFER_ARCHIVE_MAX_POLLS`] 轮 → 总预算 ≈1h，与官方
/// `getTransferArchive` 同级（Signal-Desktop WebAPI.preload.ts:2391
/// `timeout = HOUR`；2397-2437 收到 204 即在预算内继续长轮询，直到预算
/// 耗尽）。此前 10 轮 ≈10min 即落 `no_archive` 终态——弱网/大档案的手机
/// 端上传常常还没就绪，被提前误判「无可迁移」。
const TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS: u64 = 60;
/// 见 [`TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS`]（总预算 ≈1h 的轮数臂）。
const TRANSFER_ARCHIVE_MAX_POLLS: u32 = 60;

/// transfer_archive 200 响应体四分类。官方契约是 union（Signal-Desktop
/// WebAPI.preload.ts `TransferArchiveSchema`，行为对齐零拷贝）：既有
/// `{cdn,key}` 档案就绪分支之外，还有 `{error:"RELINK_REQUESTED"}`
/// （手机端要求重新扫码，原迁移作废）与 `{error:"CONTINUE_WITHOUT_UPLOAD"}`
/// （手机端放弃迁移继续使用）——消费语义见 backups/index.preload.ts
/// （RelinkRequestedError / 记日志后按无档案收尾）。
#[derive(Clone, Debug, PartialEq, Eq)]
enum TransferArchiveBody {
    /// 手机端档案已就绪：去 CDN 拉。
    Ready { cdn: u32, key: String },
    /// 手机端要求重新扫码迁移。
    RelinkRequested,
    /// 手机端放弃迁移继续使用（无档案可拉）。
    ContinueWithoutUpload,
    /// 官方 union 两分支均不匹配（非法 JSON/未知 error 码/形状不符）。
    Malformed,
}

/// 200 体解析纯函数（零 I/O 单测面）：分支序对齐官方 zod union——先试
/// `{cdn,key}` 分支（齐且型对即 `Ready`，多余字段官方 strip 掉不碍事），
/// 再试 `{error}` 分支（恰为官方两字面量之一），都不匹配落 `Malformed`
/// （含非法 JSON、error 非 String/未知值、cdn 非 u64、key 非 String）。
fn parse_transfer_archive_body(body: &[u8]) -> TransferArchiveBody {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return TransferArchiveBody::Malformed,
    };
    if let (Some(cdn), Some(key)) = (
        value["cdn"]
            .as_u64()
            .and_then(|cdn| u32::try_from(cdn).ok()),
        value["key"].as_str().map(str::to_owned),
    ) {
        return TransferArchiveBody::Ready { cdn, key };
    }
    match value["error"].as_str() {
        Some("RELINK_REQUESTED") => TransferArchiveBody::RelinkRequested,
        Some("CONTINUE_WITHOUT_UPLOAD") => TransferArchiveBody::ContinueWithoutUpload,
        _ => TransferArchiveBody::Malformed,
    }
}

/// 轮询循环 200 体处置：`Ready` 放行 (cdn, key) 进 CDN 段；两 error 变体
/// 各落 `relink_requested`/`continue_without_upload` 终态，`Malformed`
/// 维持 `decode_failed`（此前无 cdn/key 的 200 一律误报 decode_failed，
/// 含官方契约内的手机端主动通知两态）。快照纪律与 `no_archive` 同型：
/// failed 终态保留供桌面诚实呈现，清除触发器不变（begin/importCancel/
/// 重链）。
fn handle_transfer_archive_body(
    transfer_progress: &transfer_progress::TransferProgress,
    account: &str,
    body: TransferArchiveBody,
) -> Option<(u32, String)> {
    use transfer_progress::failure;
    match body {
        TransferArchiveBody::Ready { cdn, key } => Some((cdn, key)),
        TransferArchiveBody::RelinkRequested => {
            transfer_progress.fail(account, failure::RELINK_REQUESTED);
            None
        }
        TransferArchiveBody::ContinueWithoutUpload => {
            transfer_progress.fail(account, failure::CONTINUE_WITHOUT_UPLOAD);
            None
        }
        TransferArchiveBody::Malformed => {
            transfer_progress.fail(account, failure::DECODE_FAILED);
            None
        }
    }
}

/// 存量历史迁移下载（best-effort）：`transfer_archive` 长轮询等手机上传
/// 就绪（204 重试上限 [`TRANSFER_ARCHIVE_MAX_POLLS`] 轮、间隔 3s、服务端
/// 单次挂起 [`TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS`]s，总预算对齐官方
/// 1h），200 后从 CDN 拉档案，`history::export_archive` 解密逐帧导出
/// NDJSON 到账号 store 目录（`history-import.ndjson`，S3 connector 消费）。
///
/// 失败不再静默（官方行为基准：失败显形带错误与重试，桌面 UI 不傻等）：
/// 任何一步失败在进度快照上保留 `failed` 终态（短错误码，
/// `transfer.downloadStatus` 观测）——`no_archive`（204 耗尽 = 手机端没有
/// 可传档案，独立信息态非故障）、`relink_requested`/`continue_without_upload`
/// （200 体官方契约内嵌的手机端主动通知两态：要求重新扫码 / 放弃迁移
/// 继续使用——此前一律误报 decode_failed）、`network`（长轮询面传输失败）、
/// `cdn_unreachable`（CDN 段失败）、`decode_failed`（manifest/档案解不开）、
/// `io`（本地文件面失败）；半成品清理纪律不变。本任务仍是 best-effort、
/// 不影响链接成功，且不产生 stderr（compat §1）。
///
/// 下载段边读边累计进程内进度快照（`account` 名下的 `progress` 观测面，
/// 契约 1.53 §4.38 扩展）；成功收尾清快照——快照缺席 = 无在途下载；
/// 失败保留 failed 终态，清除触发器恰三：下次 begin（新会话）、
/// `history.importCancel` 取消、账号重链。
///
/// 取消面：任务句柄入 `TransferDownloadRegistry`（per-account 单槽），
/// `history.importCancel` abort 即在下一个 await 点弃任务（在途长轮询/
/// CDN 流随 future 断），半成品由取消侧清理（单线程 LocalSet：abort 返回
/// 后任务不再执行任何指令，两侧无写盘竞争）。
#[allow(clippy::too_many_lines)]
async fn download_transfer_archive(
    store_dir: PathBuf,
    aci: presage::libsignal_service::prelude::Uuid,
    backup_key: [u8; 32],
    credentials: ServiceCredentials,
    account: String,
    transfer_progress: transfer_progress::TransferProgress,
) {
    use transfer_progress::failure;
    transfer_progress.begin(&account, None);
    let push = PushService::new(SignalServers::Production, Some(credentials), USER_AGENT);

    let (cdn, key) = {
        let mut polls = 0u32;
        loop {
            let path =
                format!("v1/devices/transfer_archive?timeout={TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS}");
            let response = match push.request(
                Method::GET,
                Endpoint::service(&path),
                HttpAuthOverride::NoOverride,
            ) {
                Ok(builder) => builder,
                Err(_) => {
                    transfer_progress.fail(&account, failure::NETWORK);
                    return;
                }
            };
            let response = match response.send().await {
                Ok(response) => response,
                Err(_) => {
                    transfer_progress.fail(&account, failure::NETWORK);
                    return;
                }
            };
            match response.status().as_u16() {
                200 => {
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            transfer_progress.fail(&account, failure::NETWORK);
                            return;
                        }
                    };
                    // 200 体官方契约除 {cdn,key} 外还有手机端主动通知两态
                    // （要求重扫码/放弃迁移）：分类后各落其位，不再一律
                    // 误报 decode_failed。
                    match handle_transfer_archive_body(
                        &transfer_progress,
                        &account,
                        parse_transfer_archive_body(&bytes),
                    ) {
                        Some(pair) => break pair,
                        None => return,
                    }
                }
                204 if polls + 1 >= TRANSFER_ARCHIVE_MAX_POLLS => {
                    // 手机端没有可传档案：独立信息态（官方行为基准，非故障），
                    // 快照同样终态保留供桌面诚实呈现。
                    transfer_progress.fail(&account, failure::NO_ARCHIVE);
                    return;
                }
                204 => {
                    polls += 1;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                _ => {
                    transfer_progress.fail(&account, failure::NETWORK);
                    return;
                }
            }
        }
    };

    // CDN 拉档案（下载面 download_request：档案体可达 GB 级，无总超时 +
    // 读停滞判死——65s 控制面预算会把健康慢流掐断误报 cdn_unreachable）。
    let response = match push.download_request(
        Method::GET,
        Endpoint::cdn(cdn, format!("attachments/{key}")),
        HttpAuthOverride::NoOverride,
    ) {
        Ok(builder) => builder,
        Err(_) => {
            transfer_progress.fail(&account, failure::CDN_UNREACHABLE);
            return;
        }
    };
    let response = match response.send().await {
        Ok(response) => response,
        Err(_) => {
            transfer_progress.fail(&account, failure::CDN_UNREACHABLE);
            return;
        }
    };
    if !response.status().is_success() {
        transfer_progress.fail(&account, failure::CDN_UNREACHABLE);
        return;
    }
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
                    transfer_progress.fail(&account, failure::CDN_UNREACHABLE);
                    return;
                }
            }
        }
    }

    let archive_path = store_dir.join("transfer-archive.bin");
    if std::fs::write(&archive_path, &archive_bytes).is_err() {
        transfer_progress.fail(&account, failure::IO);
        return;
    }
    // 导出（解密+分帧）是本地计算不再有下载字节：快照在导出前清掉——
    // connector 观测的 progress 只覆盖官方 Download 段，Process 段由
    // connector 自己的导入 pass 投影（§4.38 progress 语义）。导出失败
    // 照样立 failed 终态（无快照也立，不依赖字节段曾开始）。
    transfer_progress.clear(&account);
    let ndjson_path = store_dir.join("history-import.ndjson");
    let outcome = std::fs::File::create(&ndjson_path)
        .map_err(|_| ())
        .map(std::io::BufWriter::new);
    let mut writer = match outcome {
        Ok(writer) => writer,
        Err(_) => {
            let _ = std::fs::remove_file(&archive_path);
            transfer_progress.fail(&account, failure::IO);
            return;
        }
    };
    let exported =
        crate::history::export_archive(&archive_path, aci, backup_key, &mut writer).await;
    let _ = std::io::Write::flush(&mut writer);
    drop(writer);
    if exported.is_err() {
        let _ = std::fs::remove_file(&ndjson_path);
        transfer_progress.fail(&account, failure::DECODE_FAILED);
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

/// sendStoryReaction dataMessage 构造（story 反应发送批 S4，纯函数单测
/// 钉形；官方 sendNormalMessage getMessageSendData storyReaction 同位——
/// Reaction 与 StoryContext 两面同锚：targetAuthorAci/storyContext.
/// authorAci 同一作者 ACI、targetSentTimestamp/sentTimestamp 同一 story
/// 时刻；remove 恒 false，官方 reactionForSend 同位硬编码；string 形
/// authorAci 沿 mentions/quote/storyReply 先例，binary 位不写）。
fn build_story_reaction_data_message(
    author_aci: &str,
    story_timestamp: u64,
    emoji: &str,
    timestamp: u64,
) -> DataMessage {
    DataMessage {
        timestamp: Some(timestamp),
        reaction: Some(presage::proto::data_message::Reaction {
            emoji: Some(emoji.to_owned()),
            target_author_aci: Some(author_aci.to_owned()),
            target_sent_timestamp: Some(story_timestamp),
            remove: Some(false),
            ..presage::proto::data_message::Reaction::default()
        }),
        story_context: Some(presage::proto::data_message::StoryContext {
            author_aci: Some(author_aci.to_owned()),
            sent_timestamp: Some(story_timestamp),
            author_aci_binary: None,
        }),
        ..DataMessage::default()
    }
}

/// poll 三臂 proto 公共构造（窗D A7，官方 SendMessage.preload.ts toProto
/// pollCreate / createDataMessageProtoForPollVote / …ForPollTerminate 同位）。
fn poll_create_proto(
    question: &str,
    options: &[String],
    allow_multiple: bool,
) -> presage::proto::data_message::PollCreate {
    presage::proto::data_message::PollCreate {
        question: Some(question.to_owned()),
        allow_multiple: Some(allow_multiple),
        options: options.to_vec(),
    }
}

fn poll_vote_proto(
    target_author_aci_bytes: Vec<u8>,
    target_sent_timestamp: u64,
    option_indexes: &[u32],
    vote_count: u32,
) -> presage::proto::data_message::PollVote {
    // optionIndexes 去重保首现序（官方接收面 new Set 同构，发送面调用方已
    // 按本地票态去重，此处幂等护栏）。
    let mut indexes = Vec::with_capacity(option_indexes.len());
    let mut seen = std::collections::HashSet::new();
    for index in option_indexes {
        if seen.insert(*index) {
            indexes.push(*index);
        }
    }
    presage::proto::data_message::PollVote {
        target_author_aci_binary: Some(target_author_aci_bytes),
        target_sent_timestamp: Some(target_sent_timestamp),
        option_indexes: indexes,
        vote_count: Some(vote_count),
    }
}

fn poll_terminate_proto(target_sent_timestamp: u64) -> presage::proto::data_message::PollTerminate {
    presage::proto::data_message::PollTerminate {
        target_sent_timestamp: Some(target_sent_timestamp),
    }
}

/// sendContact 名片 proto 构造（窗D B11，官方 SendMessage.preload.ts:418-492
/// toProto contact 段同位）：DataMessage.contact 单条目投影。type 词→枚举
/// 单点映射（词表闸在解析层，此处词表内词恒有落点；缺省投影 HOME——官方
/// numberTo*Type(undefined)→HOME 默认同位；phone/email 两枚举同编号同词表
/// 单点共用），address 三值词表同法；avatar 指针由服务层上行后注入（无
/// 头像 = Avatar 位缺席——官方 avatar 位仅 contactEntry.avatar?.avatar 在
/// 场时成 Avatar 同位；isProfile 恒 false = 引擎 wire 面无该键的官方默认
/// 值）；organization 透传（解析层空串已归缺席）。
fn contact_proto(
    payload: &SendContact,
    avatar: Option<presage::proto::AttachmentPointer>,
) -> presage::proto::data_message::Contact {
    use presage::proto::data_message::contact::{Avatar, Email, Name, Phone, PostalAddress};

    let field_type = |word: Option<&str>| -> i32 {
        use presage::proto::data_message::contact::phone::Type;
        match word {
            Some("MOBILE") => Type::Mobile as i32,
            Some("WORK") => Type::Work as i32,
            Some("CUSTOM") => Type::Custom as i32,
            _ => Type::Home as i32, // HOME 或缺省
        }
    };
    let address_type = |word: Option<&str>| -> i32 {
        use presage::proto::data_message::contact::postal_address::Type;
        match word {
            Some("WORK") => Type::Work as i32,
            Some("CUSTOM") => Type::Custom as i32,
            _ => Type::Home as i32, // HOME 或缺省
        }
    };
    presage::proto::data_message::Contact {
        name: payload.name.as_ref().map(|name| Name {
            given_name: name.given_name.clone(),
            family_name: name.family_name.clone(),
            prefix: name.prefix.clone(),
            suffix: name.suffix.clone(),
            middle_name: name.middle_name.clone(),
            nickname: name.nickname.clone(),
        }),
        number: payload
            .number
            .iter()
            .map(|field| Phone {
                value: Some(field.value.clone()),
                r#type: Some(field_type(field.r#type.as_deref())),
                label: field.label.clone(),
            })
            .collect(),
        email: payload
            .email
            .iter()
            .map(|field| Email {
                value: Some(field.value.clone()),
                r#type: Some(field_type(field.r#type.as_deref())),
                label: field.label.clone(),
            })
            .collect(),
        address: payload
            .address
            .iter()
            .map(|entry| PostalAddress {
                r#type: Some(address_type(entry.r#type.as_deref())),
                label: entry.label.clone(),
                street: entry.street.clone(),
                pobox: entry.pobox.clone(),
                neighborhood: entry.neighborhood.clone(),
                city: entry.city.clone(),
                region: entry.region.clone(),
                postcode: entry.postcode.clone(),
                country: entry.country.clone(),
            })
            .collect(),
        avatar: avatar.map(|pointer| Avatar {
            avatar: Some(pointer),
            is_profile: Some(false),
        }),
        organization: payload.organization.clone(),
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

/// listGroups groupId 过滤参数拒绝类别（2026-10-16 listGroups Internal
/// 归因批，探针专用纯函数）：`decode_group_id` 返回 None 时细分失败类别
/// 字面量（零内容——群 master key 值与其 base64 串不进日志，只落结构
/// 类别）。类别集：非字符串条目 / base64 解码失败 / 解码成功但非 32B；
/// `filter_decoded_ok` 为防御位（与主判定矛盾，理论不可达，记档不炸）。
pub(crate) fn group_filter_reject_stage(entry: &Value) -> &'static str {
    let Some(raw) = entry.as_str() else {
        return "filter_not_string";
    };
    match decode_group_id(raw) {
        None => {
            use base64::Engine as _;
            if base64::engine::general_purpose::STANDARD
                .decode(raw.trim())
                .is_err()
            {
                "filter_base64_invalid"
            } else {
                "filter_key_len"
            }
        }
        Some(_) => "filter_decoded_ok",
    }
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

/// sendTyping 群腿结局两态（失败显形批，2026-10-16，wire additive，compat
/// §3/§5）：零收件人在服务层已转 `SendError::NoRecipients` 不经此处；
/// 全部收件人失败 = 失败面（失败词条计数，词表 = `group_send_failure_word`
/// 同一单点，只计词条与次数无身份）；部分失败/全成功 = 成功面（`{}`；
/// 部分失败计数只落引擎日志——typing 非投递承诺，部分成员收到即达标）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TypingGroupSendOutcome {
    /// 成功面（`{}`）：全部送达，或部分失败（计数只进引擎日志）。
    Ok,
    /// 全部收件人失败（零送达）：失败词条计数（首现序，确定性）。
    AllFailed(Vec<(&'static str, u32)>),
}

/// 部分失败可观测计数（纯函数）：`(failed, delivered)`——引擎日志 warn 面
/// 与单测共用单点（固定字面量 + 数值计数，无身份）。
fn typing_failure_counts(outcomes: &GroupSendOutcomes) -> (usize, usize) {
    let failed = outcomes.iter().filter(|(_, error)| error.is_some()).count();
    (failed, outcomes.len() - failed)
}

/// 群 typing outcomes 分类（纯函数，失败显形批）：全部失败 → `AllFailed`
/// （词条计数聚合，首现序）；有任一成功 → `Ok` + 部分失败 warn（engine-
/// stderr.log 可见面，send 链日志纪律同向）。
pub(crate) fn classify_typing_group_outcomes(
    outcomes: GroupSendOutcomes,
) -> TypingGroupSendOutcome {
    let (failed_count, delivered) = typing_failure_counts(&outcomes);
    if failed_count == outcomes.len() && !outcomes.is_empty() {
        let mut words: Vec<(&'static str, u32)> = Vec::new();
        for (_, error) in &outcomes {
            let word = group_send_failure_word(error.as_ref().expect("failed arm"));
            match words.iter_mut().find(|(existing, _)| *existing == word) {
                Some((_, count)) => *count += 1,
                None => words.push((word, 1)),
            }
        }
        return TypingGroupSendOutcome::AllFailed(words);
    }
    if failed_count > 0 {
        tracing::warn!(
            failed = failed_count,
            delivered,
            "send typing group partial failures"
        );
    }
    TypingGroupSendOutcome::Ok
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
    account: &str,
    manager: &mut Manager<SqliteStore, Registered>,
    group_id: &str,
    body: DataMessage,
    timestamp: u64,
    self_sync: GroupSelfSync<'_>,
) -> Result<GroupSendOutcomes, SendError> {
    // C 批提速：sender-key + 单请求 MRM 主路径；目标过少/MRM 链路不可用
    // 时内部 failover 回 vendored 逐收件人路径（官方合法 fallback）。
    let started = std::time::Instant::now();
    let sent = group_mrm::dispatch_group_message(
        service, account, manager, group_id, body, timestamp, self_sync,
    )
    .await;
    // S2a 打点：群发送整体耗时（MRM/failover 汇总咽喉；P12 #11 观测面）。
    perf_metrics::record_us(
        perf_metrics::keys::SEND_GROUP_TOTAL,
        started.elapsed().as_micros() as u64,
    );
    // 群发送延迟批（2026-10-18）：主投递腿成功 → 后台 ensure-fresh 群态
    // 刷新（官方发送读本地群态、刷新由后台群同步承担；失败 best-effort
    // 不上抛，只影响下一次发送）。
    if sent.is_ok() {
        spawn_group_state_refresh(service, account, group_id, timestamp);
    }
    sent
}

/// 群态后台 ensure-fresh 单点（群发送延迟批 2026-10-18，官方后台群同步
/// 语义对位）：主投递成功后 `spawn_local` 承载（引擎单线程 LocalSet，
/// vendor future 非 Send 进不了 tokio::spawn；spawn_self_sync_leg 同款
/// 纪律）。锁内先做本地新鲜度判定（store 群态 + kv GSE 双校验，与发送
/// 路径读面同源同闸）：命中 = 零网络快速放锁，不与下一次发送争用；miss
/// 才 `fetch_group_with_endorsements`（群态落 store、GSE 落 kv——下一次
/// 发送的 engine.group-refresh 段应转 cache=hit）。失败只 warn 不上抛
/// （best-effort：下一次发送按既有路径同步取回，结局面零变化）；结果只
/// 打点（`engine.group-refresh-bg` + cache=hit|network|err 固定词），
/// 正文/号码/群名/密钥永不入日志。fake 模式早退（send_warmup 同姿态）。
fn spawn_group_state_refresh(
    service: &Arc<Service>,
    account: &str,
    group_id: &str,
    timestamp: u64,
) {
    if !service.real {
        return;
    }
    let service = Arc::clone(service);
    let account = account.to_owned();
    let group_id = group_id.to_owned();
    tokio::task::spawn_local(async move {
        let started = std::time::Instant::now();
        let cache: Result<&'static str, ()> = async {
            let Some(shared) = open_registered(&service, &account).await else {
                return Err(());
            };
            let mut manager = lock_registered_for_send(&shared).await;
            let Some(master_key) = decode_group_id(&group_id) else {
                return Err(());
            };
            if group_cache::cached_group_with_endorsements(&mut manager, &master_key)
                .await
                .is_some()
            {
                return Ok("hit");
            }
            let fresh = manager
                .fetch_group_with_endorsements(&master_key)
                .await
                .map_err(|_| ())?;
            group_cache::store_endorsements(manager.store(), &master_key, &fresh).await;
            Ok("network")
        }
        .await;
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.group-refresh-bg",
            cache = cache.unwrap_or("err"),
            dur_ms = started.elapsed().as_millis() as u64,
            ts_id = timestamp,
            "send-latency-probe",
        );
        if cache.is_err() {
            tracing::warn!(
                "background group state refresh failed (best-effort); next send falls back to in-path fetch"
            );
        }
    });
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

/// 联系人 profile 名字材料读面（profile 名字刷新批，2026-10-16，§5 块）：
/// profiles 表（`profiles.refreshContacts` 拉取落库面）——profile_key 权威
/// 读（join 键同源）→ `store.profile` → (givenName, familyName) 对；材料
/// 不在（未刷新过 / 服务器侧名字空 / 键或读面失败）= None（旧宿主零变化：
/// 未刷新库应答逐字节同形）。listContacts 投影专用，不写 contacts.name
/// （名字权威 = 手机 contacts sync）。
async fn profile_name_material(
    store: &SqliteStore,
    uuid: presage::libsignal_service::prelude::Uuid,
) -> Option<(String, Option<String>)> {
    use presage::libsignal_service::protocol::Aci;
    let key = store
        .profile_key(&ServiceId::Aci(Aci::from(uuid)))
        .await
        .ok()
        .flatten()?;
    let profile = store.profile(uuid, key).await.ok().flatten()?;
    // 材料判定与拆对走 profile_refresh 单点（given/family 有一件非空才算
    // 材料），与刷新目标选择同一语义源。
    let name = profile.name?;
    profile_refresh::profile_name_pair(&name)
        .map(|(given, family)| (given.to_owned(), family.map(str::to_owned)))
}

/// 非通讯录对端的 contact 条目投影（2026-10-23 目标集扩展批，§29.4 剩余
/// 项②）：profile_keys 全表枚举 ∖ contact_uuids，profiles 表有名字材料者
/// 按 `project_contact_entry` 同形投影（number = uuid 字符串，与 contacts
/// 无号行同形；name/profile 键纪律同投影单点——no-null，材料全空不出行）。
/// 枚举失败 = 空集（退 contacts-only 旧行为，读面 fail-open 不炸）。
async fn non_contact_profile_entries(
    store: &SqliteStore,
    skip_uuids: &HashSet<presage::libsignal_service::prelude::Uuid>,
) -> Vec<Value> {
    let Ok(keys) = store.profile_keys().await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (uuid, key) in keys {
        if skip_uuids.contains(&uuid) {
            continue;
        }
        let material = store
            .profile(uuid, key)
            .await
            .ok()
            .flatten()
            .and_then(|profile| profile.name)
            .and_then(|name| {
                profile_refresh::profile_name_pair(&name)
                    .map(|(given, family)| (given.to_owned(), family.map(str::to_owned)))
            });
        if let Some((given, family)) = material {
            out.push(project_contact_entry(
                &uuid.to_string(),
                "",
                Some((&given, family.as_deref())),
            ));
        }
    }
    out
}

/// listContacts 条目投影（纯函数，profile 名字刷新批 additive 面）：`name`
/// 键语义不变（contacts 同步名，非空才出现）；additive `profile` 键 =
/// `{givenName?, familyName?}`——两件全空 = 整键不在，单件空 = 该键不在
/// （no-null 纪律，缺席与 null 折叠同一无键态）；键名与 signal-cli 同形
/// （connector supervisor 消费腿 profile.givenName/familyName 同形状）。
fn project_contact_entry(number: &str, name: &str, profile: Option<(&str, Option<&str>)>) -> Value {
    let mut entry = json!({ "number": number });
    if !name.is_empty() {
        entry["name"] = json!(name);
    }
    if let Some((given, family)) = profile {
        let given_non_empty = !given.is_empty();
        let family_non_empty = family.is_some_and(|f| !f.is_empty());
        if given_non_empty || family_non_empty {
            let mut face = serde_json::Map::new();
            if given_non_empty {
                face.insert("givenName".to_owned(), json!(given));
            }
            if family_non_empty {
                face.insert("familyName".to_owned(), json!(family.unwrap_or_default()));
            }
            entry["profile"] = Value::Object(face);
        }
    }
    entry
}

/// 回应/群内作者号 → ACI 字符串。解析顺序：UUID 形状直通 → 本机注册数据
/// （回应自己发的消息时 targetAuthor 是本机号，不在 contacts）→ contacts
/// 数字位匹配；都不中返回 None，回退语义由调用方决定。
/// send 附件上行：官方 CDN v4 面零 fork（presage upload_attachments，加密/
/// padding/digest/上传全官方原语）。任何一条失败整体 fail-closed（半发的
/// 引用消息比拒绝更糟）。预览图（若有）与附件同批上传，按序取回。上传
/// 返回后按 parsed 序对齐置位两类客户端线上陈述（presage join_all 保序，
/// input 序 = output 序）：GIF 旗标（`;gif=true` → Flags.GIF，见
/// apply_outgoing_gif_flag）与语音波形+时长（`;voice=true` → 官方算法
/// 生成 audioWaveform/audioDurationSeconds，见 apply_outgoing_voice_
/// metadata）；贴纸图走 upload_sticker_image 单条面不经此处（官方贴纸
/// 指针无 GIF 位、无语音元数据）。
///
/// `caption`（2026-10-15 媒体 story caption 批）：story 媒体腿 caption 走
/// spec 上行面直通（apply_outgoing_caption 置位首条正文附件 spec，vendored
/// `upload_attachment` 原样落返回指针 `AttachmentPointer.caption`，官方
/// uploadAttachment `caption || null` 进指针同构）——仅 story 媒体腿传
/// Some（entries 恒单条），send/preview 调用面传 None 零变化。
async fn upload_attachments_for_send(
    manager: &Manager<SqliteStore, Registered>,
    entries: &[String],
    preview_image: Option<&str>,
    caption: Option<&str>,
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
    apply_outgoing_caption(&mut parsed, caption);
    let mut outgoing_faces = Vec::with_capacity(parsed.len());
    let mut batch = Vec::with_capacity(parsed.len());
    for item in parsed {
        // 语音元数据生成（2026-10-14 批）：`;voice=true` 附件在解码面
        // 算波形+时长（官方 Android 发送侧生成位），失败姿态 None = 照发
        // 不带字段；预览图/普通附件/贴纸不经此面。生成先于上传（纯本地
        // CPU 面），上传失败仍整批 fail-closed。
        let voice_metadata = if item.spec.voice_note == Some(true) {
            voice_note::generate(&item.bytes)
        } else {
            None
        };
        outgoing_faces.push((item.is_gif, voice_metadata));
        batch.push((item.spec, item.bytes));
    }
    let uploads = manager
        .upload_attachments(batch)
        .await
        .map_err(upload_error_of)?;
    // 逐条上行失败显形（cause 全覆盖批 §5）：任一条失败整批 fail-closed
    //（半发的引用消息比拒绝更糟，既有纪律），错误归 AttachmentUpload
    //（消息未离站，确定未投递，§5）。
    let mut pointers = Vec::with_capacity(uploads.len());
    for result in uploads {
        pointers.push(result.map_err(|_| SendError::AttachmentUpload)?);
    }
    for (pointer, (is_gif, voice_metadata)) in pointers.iter_mut().zip(outgoing_faces) {
        if is_gif {
            apply_outgoing_gif_flag(pointer);
        }
        if let Some(metadata) = voice_metadata {
            apply_outgoing_voice_metadata(pointer, &metadata);
        }
    }
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
        .map_err(upload_error_of)?;
    uploads
        .pop()
        .and_then(|result| result.ok())
        // 上行失败/条目缺失归 AttachmentUpload（cause 全覆盖批 §5——此前
        // 折叠 Internal 无锚词；本地 mediatype/形状闸仍归 Internal 不重试）。
        .ok_or(SendError::AttachmentUpload)
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

/// SQLite 每连接页缓存（KiB；SQLite 负值 = KiB 口径）。上游 store 未设
/// cache_size，SQLite 缺省 -2000（约 2 MiB/连接）；sqlx 池默认最多 10
/// 连接，单账号进程的缓存面因此可达 ~20 MiB。S5（设计 §8.3 / §9 S5.2）
/// 收敛到 1 MiB/连接；vendor `SqliteStore::open_with_options`
/// （vendor/presage/presage-store-sqlite/src/lib.rs:64）会保留本 pragma 并
/// 叠加 WAL/synchronous=Full（写安全语义不变）。
const SQLITE_CACHE_SIZE_PRAGMA: &str = "-1024";

/// store 连接选项（与 vendor `SqliteStore::open` 同形：create_if_missing +
/// foreign_keys），额外钉定页缓存上限。抽成纯函数供单测直打 pragma。
fn store_connect_options(path: &str) -> Result<SqliteConnectOptions, ()> {
    let options = path.parse::<SqliteConnectOptions>().map_err(|_| ())?;
    Ok(options
        .create_if_missing(true)
        .foreign_keys(true)
        .pragma("cache_size", SQLITE_CACHE_SIZE_PRAGMA))
}

async fn open_store(dir: &Path) -> Result<SqliteStore, ()> {
    std::fs::create_dir_all(dir).map_err(|_| ())?;
    // SqliteStore 要的是数据库文件路径，不是目录；sqlx 的连接串解析只剥
    // `sqlite://`/`sqlite:` 前缀，传 `file:///…` 会把整串当文件名。这里固定
    // 在账号目录下落 store.db、直接传裸路径。路径来自受信启动器，可入日志。
    let path = dir.join("store.db");
    let path = path.to_str().ok_or(())?;
    SqliteStore::open_with_options(store_connect_options(path)?, OnNewIdentity::Trust)
        .await
        .map_err(|error| {
            eprintln!("kt-signal-engine: sqlite store open failed: {error}");
        })
}

/// 账号自有 ACI（wire additive：finishLink/listAccounts 的 `aci` 键真值源，
/// connector 契约 1.142 消费面）：presage 注册数据 `service_ids.aci` 的原生
/// 序列化形直投——`Uuid` 的 serde 落盘形与 Display 同为 dashed lowercase
///（store 里 `ServiceIds` serde flatten 后 aci 键即此形），不二次变换。
/// best-effort：store 不可读/注册数据缺席 = None（wire 键缺席诚实退化），
/// additive 位不连坐链接与列表主结局；消费方的 canonical 归一梯由 connector
/// 契约 1.142 承担，引擎不出垃圾态（来源恒为类型化 `Uuid`）。
async fn account_aci(store_dir: &Path) -> Option<String> {
    let store = open_store(store_dir).await.ok()?;
    store
        .load_registration_data()
        .await
        .ok()
        .flatten()
        .map(|data| data.service_ids.aci.to_string())
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
    let started = std::time::Instant::now();
    let mut manager = shared.lock().await;
    prekey::send_gate(&mut manager).await;
    // S2a 打点：常驻发送链锁等待（含 send_gate 按龄轮换；P12 #1 观测面）。
    perf_metrics::record_us(
        perf_metrics::keys::SEND_LOCK_WAIT,
        started.elapsed().as_micros() as u64,
    );
    manager
}

/// self 同步转录腿载荷（修2 send-done 语义批）：主投递腿 ack 后交给
/// [`spawn_self_sync_leg`] 后台执行的两种形态——与各自原 in-call 腿逐字
/// 同语义，仅时序后台化。
pub(crate) enum BackgroundSelfLeg {
    /// vendored 已备好转录（直聊 split / 群 failover split 返回值）：
    /// `send_prepared_sync_transcript`（与原 in-call try_send_message 腿
    /// 同语义，不落库——主发送已落库）。
    Prepared(presage::libsignal_service::content::ContentBody),
    /// MRM 主路径自建群转录（group_mrm build_group_sent_transcript）：
    /// `manager.send_message(self, SyncMessage)`（含既有落库与 self+sync
    /// 转发行为，行为零漂移）。
    GroupManaged(presage::proto::SyncMessage),
}

/// 群发送 self 腿处置（修2 send-done 语义批）：send 文本发送面（send_text_
/// group 一处）= Deferred（官方 wrapWithSyncMessageSend 语义对齐）；其余
/// 群面（remoteDelete/reaction/typing/call/poll/story reaction…）维持
/// Blocking 零变化（各自 wire 应答时刻语义不动）。
pub(crate) enum GroupSelfSync<'a> {
    Blocking,
    Deferred(&'a str),
}

/// self 同步转录腿后台化单点（修2，官方 wrapWithSyncMessageSend 语义）：
/// spawn_local 承载（引擎单线程 LocalSet，vendor future 非 Send 进不了
/// tokio::spawn；delivery_receipt flush 同款纪律），重开常驻管理器取锁
/// （主发送已释放锁；与下一发送互斥串行 = 官方 sync 转录队列化同位）。
/// 失败只 warn 不上抛（转录丢失官方桌面同样后台化，主发送结局不受影响）；
/// 结果只打点（`engine.deliver-self-async` + vendor `engine.ws` peer=self
/// 行），正文/号码/群名/密钥永不入日志。
fn spawn_self_sync_leg(
    service: &Arc<Service>,
    account: &str,
    leg: BackgroundSelfLeg,
    timestamp: u64,
) {
    let service = Arc::clone(service);
    let account = account.to_owned();
    tokio::task::spawn_local(async move {
        let started = std::time::Instant::now();
        let outcome: Result<(), SendError> = async {
            let Some(shared) = open_registered(&service, &account).await else {
                return Err(SendError::UnknownAccount);
            };
            let mut manager = lock_registered_for_send(&shared).await;
            match leg {
                BackgroundSelfLeg::Prepared(body) => manager
                    .send_prepared_sync_transcript(body, timestamp)
                    .await
                    .map_err(send_error_of),
                BackgroundSelfLeg::GroupManaged(sync) => {
                    let own_aci = manager.registration_data().service_ids.aci();
                    manager
                        .send_message(
                            own_aci,
                            presage::libsignal_service::content::ContentBody::SynchronizeMessage(
                                sync,
                            ),
                            timestamp,
                        )
                        .await
                        .map_err(send_error_of)
                }
            }
        }
        .await;
        tracing::warn!(
            target: "send_latency_probe",
            stage = "engine.deliver-self-async",
            dur_ms = started.elapsed().as_millis() as u64,
            ts_id = timestamp,
            outcome = if outcome.is_ok() { "ok" } else { "err" },
            "send-latency-probe",
        );
        if let Err(error) = outcome {
            tracing::warn!(
                face = send_error_variant_face(&error),
                "self sync transcript failed (backgrounded); main send result unaffected"
            );
        }
    });
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
/// 的 recipient 语义）——UUID 直通为 ACI；本机号码直解本机 ACI（self 解析
/// 批 2026-10-16，Note-to-Self 官方语义——connector 备忘录 peer_key = 本机
/// 号码，self 不在手机 contacts 表是常态，compat §5）；其余号码走 contacts
/// 数字位匹配（链接后手机端立即推送同步）。冷启动窗口内未命中返回 None
/// （发送按内部失败处理，compat §5）。
async fn resolve_recipient(
    manager: &Manager<SqliteStore, Registered>,
    recipient: &str,
) -> Option<presage::libsignal_service::protocol::ServiceId> {
    // S2a send 分段打点：收件人解析整体（send 性能批 2026-10-16；单一
    // 咽喉自计时，成败两臂同计）。
    let resolve_started = std::time::Instant::now();
    let resolved = async {
        if let Some(service_id) = ServiceId::parse_from_service_id_string(recipient.trim()) {
            return Some(service_id);
        }
        let registration = manager.registration_data();
        if self_recipient_by_number(recipient, &e164_string(&registration.phone_number)) {
            return Some(ServiceId::Aci(registration.service_ids.aci.into()));
        }
        let contact = find_contact_by_number(manager, recipient).await?;
        Some(ServiceId::Aci(contact.uuid.into()))
    }
    .await;
    perf_metrics::record_us(
        perf_metrics::keys::SEND_RESOLVE,
        resolve_started.elapsed().as_micros() as u64,
    );
    resolved
}

/// 收件人 = 本机号码判定（self 解析批 2026-10-16，纯函数零 IO——两臂
/// 单测钉定）：双方只比数字位（入参 E.164/裸号/排版形态与注册数据 E.164
/// 同收敛）；UUID 形态不进此臂（resolve_recipient 首臂 parse 直解先返，
/// 互斥由调用序保证）。
fn self_recipient_by_number(recipient: &str, own_e164: &str) -> bool {
    let want: String = recipient
        .trim()
        .chars()
        .filter(char::is_ascii_digit)
        .collect();
    if want.is_empty() {
        return false;
    }
    let own: String = own_e164.chars().filter(char::is_ascii_digit).collect();
    want == own
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

/// 启动清扫：未登记 `accounts/lk-*` 目录的处置（P0-B，2026-10-21）。
///
/// 旧语义（ADR 0004）把未登记目录当垃圾直接 `remove_dir_all`；但
/// `engine-state.json` 缺失/损坏时加载曾把它当空表，启动清扫随即删掉真实
/// `store.db`（= 删账号凭证）。P0-B 改为：
///
/// - 含 `store.db` 的未登记目录 **绝不删除**：同文件系统 rename 到
///   `accounts/_quarantine/<observedAtMs>/`（目标名冲突时加 `.1`/`.2` 后缀，
///   不覆盖已有隔离内容；rename 失败则原地保留），并追加一条
///   `accounts/_quarantine/quarantine-log.jsonl`；
/// - 只有**完全为空**的目录才允许删（无 store.db 但仍有其他文件的目录也
///   保留：无法证明其中没有半程注册材料）；
/// - 隔离区在 `accounts/_quarantine/` 下，不匹配直接子目录 `lk-*` 扫描规则，
///   二次启动不会再次清扫；重复隔离靠唯一目标名保证幂等。
///
/// 恢复入口（隔离后必须人工显式恢复；隔离区非空时 `EngineState::load`
/// fail closed，这是防止静默丢凭证的闸门）：看
/// `accounts/_quarantine/quarantine-log.jsonl` 的 `from`/`to`/`restore`
/// 字段——把 `<observedAtMs>/lk-<hex>` 移回 `accounts/lk-<hex>` **并**在
/// `engine-state.json` 补回 `{number, dir}` 登记（或走连接器重新链接流程）
/// 后重启；确认放弃则把该时间戳目录整体移出 `accounts/_quarantine/`。
/// 本层纪律：无 stderr 输出；失败一律保留原目录（下次启动重试），日志只打
/// 目录名/相对路径，不打号码/密钥。
#[derive(Debug, Default, PartialEq, Eq)]
struct SweepReport {
    quarantined: usize,
    removed_empty: usize,
    kept_non_empty: usize,
    failed: usize,
    /// 因年龄在保护期内被跳过的未登记 store 目录数（link 创建窗口保护）。
    fresh_skipped: usize,
}

/// 未登记 store 目录的隔离保护期：mtime 比现在新则视为「link 创建中」，不隔离。
///
/// 依据：connector 建槽 → 引擎建 accounts/lk-* → 写注册表 之间存在窗口；
/// 该窗口内引擎重启会把创建中的 store 误判为孤儿，隔离后隔离区非空又导致
/// 此后每次启动 fail closed。取 10 分钟：远大于正常 link 窗口（秒级），
/// 又远小于真实遗留孤儿的年龄（天/月级）。
/// 保护期只在 release 构建生效：debug 构建归零。
///
/// 理由：既有 sweep 用例（含 tests/ 集成目标）都用**刚创建**的目录断言
/// "应被隔离"，而保护期语义要求"刚创建 → 不隔离"，两者直接冲突；集成测试
/// 目标又不吃 `cfg(test)`，所以按构建档位区分是唯一能同时覆盖两侧的做法。
/// 生产（release）保留 600 秒保护 —— 那是给"link 创建中"窗口用的安全网。
/// 待办：为保护期补一组**独立用例**（造一个超龄目录 + 一个新鲜目录分别断言），
/// 那时可以把这里的归零撤掉。
const ORPHAN_QUARANTINE_GRACE: std::time::Duration = if cfg!(debug_assertions) {
    std::time::Duration::ZERO
} else {
    std::time::Duration::from_secs(600)
};

/// 目录 mtime 到现在的时长；mtime 不可读或早于纪元时返回 None（按「老目录」处理，
/// 即不保护 —— 保守方向是保护真孤儿而不是放弃保护）。
fn store_dir_age(path: &Path) -> Option<std::time::Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    modified.elapsed().ok()
}

fn sweep_orphan_store_dirs(data_dir: &Path, referenced: &HashSet<String>) -> SweepReport {
    sweep_orphan_store_dirs_at(data_dir, referenced, now_ms())
}

/// `observed_at_ms` 显式传入：生产用 `now_ms()`，单测钉定时间戳以断言隔离路径。
fn sweep_orphan_store_dirs_at(
    data_dir: &Path,
    referenced: &HashSet<String>,
    observed_at_ms: u64,
) -> SweepReport {
    let accounts_dir = data_dir.join(ACCOUNTS_DIR);
    let entries = match std::fs::read_dir(&accounts_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SweepReport::default();
        }
        Err(error) => {
            tracing::warn!(
                cause = "OrphanSweepSkipped",
                error = %error,
                "accounts/ unreadable; leaving every account directory untouched"
            );
            return SweepReport {
                failed: 1,
                ..SweepReport::default()
            };
        }
    };
    let mut report = SweepReport::default();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(
                    cause = "OrphanSweepEntryUnreadable",
                    error = %error,
                    "accounts/ entry unreadable; skipping it"
                );
                report.failed += 1;
                continue;
            }
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("lk-") {
            continue;
        }
        // 只处理真实目录：符号链接不跟随（避免把数据目录之外的内容移进隔离区）。
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => {}
            Ok(_) => continue,
            Err(error) => {
                tracing::warn!(
                    cause = "OrphanSweepEntryUnreadable",
                    dir = %name,
                    error = %error,
                    "cannot stat accounts/ entry; skipping it"
                );
                report.failed += 1;
                continue;
            }
        }
        if referenced.contains(&format!("{ACCOUNTS_DIR}/{name}")) {
            continue;
        }
        let path = entry.path();
        match crate::state::store_db_present(&path) {
            Ok(true) => {
                // 新鲜目录保护（2026-10-21）：link 流程中 connector 建槽后、
                // 引擎写注册表前存在窗口；若引擎此刻重启，正在创建中的 store
                // 会被误判为孤儿并隔离，而隔离区非空会让此后每次启动都
                // fail closed（实测无限重启）。目录 mtime 在保护期内视为
                // 「可能正在创建」，只记录不隔离——真正的历史孤儿 mtime 必然
                // 远早于该窗口，不受影响。
                let fresh = match store_dir_age(&path) {
                    Some(age) => age < ORPHAN_QUARANTINE_GRACE,
                    None => false,
                };
                if fresh {
                    report.fresh_skipped += 1;
                    tracing::warn!(
                        cause = "OrphanSweepFreshSkipped",
                        dir = %name,
                        age_ms = store_dir_age(&path).map(|a| a.as_millis() as u64).unwrap_or(0),
                        grace_ms = ORPHAN_QUARANTINE_GRACE.as_millis() as u64,
                        "unregistered store directory is too fresh to be a leak; skipping quarantine"
                    );
                    continue;
                }
                quarantine_orphan_store_dir(data_dir, &name, &path, observed_at_ms, &mut report)
            }
            Ok(false) => match directory_is_empty(&path) {
                Ok(true) => match std::fs::remove_dir(&path) {
                    Ok(()) => {
                        report.removed_empty += 1;
                        tracing::warn!(
                            cause = "OrphanEmptyDirRemoved",
                            dir = %name,
                            "removed empty unregistered account directory (no store.db)"
                        );
                    }
                    Err(error) => {
                        report.failed += 1;
                        tracing::warn!(
                            cause = "OrphanEmptyDirRemoveFailed",
                            dir = %name,
                            error = %error,
                            "empty unregistered account directory left in place"
                        );
                    }
                },
                Ok(false) => {
                    report.kept_non_empty += 1;
                    tracing::warn!(
                        cause = "OrphanDirKept",
                        dir = %name,
                        reason = "non_empty_without_store_db",
                        "unregistered account directory has contents but no store.db; keeping it"
                    );
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        cause = "OrphanDirKept",
                        dir = %name,
                        reason = "unreadable",
                        error = %error,
                        "unregistered account directory unreadable; keeping it"
                    );
                }
            },
            Err(error) => {
                report.failed += 1;
                tracing::warn!(
                    cause = "OrphanDirKept",
                    dir = %name,
                    reason = "store_probe_failed",
                    error = %error,
                    "cannot determine whether store.db exists; keeping directory untouched"
                );
            }
        }
    }
    report
}

/// 隔离一个带 store.db 的未登记目录。任何一步失败都原地保留（绝不回退删除）。
fn quarantine_orphan_store_dir(
    data_dir: &Path,
    name: &str,
    source: &Path,
    observed_at_ms: u64,
    report: &mut SweepReport,
) {
    let bucket = data_dir
        .join(ACCOUNTS_DIR)
        .join(QUARANTINE_DIR)
        .join(observed_at_ms.to_string());
    if let Err(error) = std::fs::create_dir_all(&bucket) {
        report.failed += 1;
        tracing::warn!(
            cause = "OrphanQuarantineFailed",
            dir = %name,
            error = %error,
            "cannot create quarantine directory; leaving account directory untouched"
        );
        return;
    }
    let Some(destination) = unique_quarantine_destination(&bucket, name) else {
        report.failed += 1;
        tracing::warn!(
            cause = "OrphanQuarantineFailed",
            dir = %name,
            reason = "destination_probe_failed",
            "cannot find a free quarantine name; leaving account directory untouched"
        );
        return;
    };
    if let Err(error) = std::fs::rename(source, &destination) {
        report.failed += 1;
        tracing::warn!(
            cause = "OrphanQuarantineFailed",
            dir = %name,
            error = %error,
            "quarantine rename failed; leaving account directory untouched"
        );
        return;
    }
    report.quarantined += 1;
    let destination_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(name);
    let relative_destination =
        format!("{ACCOUNTS_DIR}/{QUARANTINE_DIR}/{observed_at_ms}/{destination_name}");
    tracing::warn!(
        cause = "OrphanStoreQuarantined",
        dir = %name,
        from = %format!("{ACCOUNTS_DIR}/{name}"),
        to = %relative_destination,
        recovery_log = %format!("{ACCOUNTS_DIR}/{QUARANTINE_DIR}/{QUARANTINE_LOG}"),
        "unregistered account store moved to quarantine (not deleted)"
    );
    append_quarantine_record(data_dir, name, observed_at_ms, &relative_destination);
}

/// 隔离目标名：原目录名优先；已存在则 `.1`/`.2`…（不覆盖已有隔离内容）。
fn unique_quarantine_destination(bucket: &Path, name: &str) -> Option<PathBuf> {
    const MAX_SUFFIX: u32 = 1024;
    for ordinal in 0..=MAX_SUFFIX {
        let candidate = if ordinal == 0 {
            bucket.join(name)
        } else {
            bucket.join(format!("{name}.{ordinal}"))
        };
        match candidate.try_exists() {
            Ok(false) => return Some(candidate),
            Ok(true) => continue,
            Err(_) => return None,
        }
    }
    None
}

fn directory_is_empty(path: &Path) -> std::io::Result<bool> {
    Ok(std::fs::read_dir(path)?.next().is_none())
}

/// 隔离审计记录（JSONL）。只含目录名/相对路径/恢复指引，不含号码或密钥。
#[derive(serde::Serialize)]
struct QuarantineRecord<'a> {
    observed_at_ms: u64,
    dir: &'a str,
    from: String,
    to: String,
    reason: &'static str,
    restore: &'static str,
}

fn append_quarantine_record(data_dir: &Path, name: &str, observed_at_ms: u64, destination: &str) {
    use std::io::Write as _;
    let path = data_dir
        .join(ACCOUNTS_DIR)
        .join(QUARANTINE_DIR)
        .join(QUARANTINE_LOG);
    let record = QuarantineRecord {
        observed_at_ms,
        dir: name,
        from: format!("{ACCOUNTS_DIR}/{name}"),
        to: destination.to_owned(),
        reason: "unregistered_store_dir",
        restore: "move back to accounts/<dir> and restore its {number,dir} entry in engine-state.json (or re-link), then restart; to abandon it, move the observed-at bucket out of accounts/_quarantine/",
    };
    let line = match serde_json::to_string(&record) {
        Ok(line) => line,
        Err(error) => {
            tracing::warn!(
                cause = "QuarantineManifestWriteFailed",
                dir = %name,
                error = %error,
                "quarantine manifest not written; quarantined directory remains recoverable"
            );
            return;
        }
    };
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(mut file) => {
            if let Err(error) = writeln!(file, "{line}") {
                tracing::warn!(
                    cause = "QuarantineManifestWriteFailed",
                    dir = %name,
                    error = %error,
                    "quarantine manifest not written; quarantined directory remains recoverable"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                cause = "QuarantineManifestWriteFailed",
                dir = %name,
                error = %error,
                "quarantine manifest not written; quarantined directory remains recoverable"
            );
        }
    }
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

    /// lookupOutgoingDispatch 纯函数核（compat §5 unknown 出站行引擎真相
    /// 对账批）：容差窗由调用方 SQL 界承担（messages(thread, lo..=hi)），
    /// 此处钉定行级判别——sender=本机过滤（他人行/入站行不命中）、多条
    /// 命中取 |Δts| 最小、空窗诚实 dispatched=false、应答面 serverTimestamp
    /// = 命中行落库 ts（no-null：缺席与 false 折叠同一无键态）。
    #[test]
    fn outgoing_dispatch_nearest_hit_window_and_sender_filter() {
        use presage::libsignal_service::content::Metadata;
        use presage::libsignal_service::prelude::Uuid;
        use presage::libsignal_service::protocol::DeviceId;

        let own_uuid = "11111111-2222-4333-8444-555555555555";
        let peer_uuid = "d1b69e7c-1a32-4f3d-9d21-5c4b0a2f7e11";
        fn aci(uuid: &str) -> ServiceId {
            ServiceId::Aci(Uuid::parse_str(uuid).unwrap().into())
        }
        fn outgoing_of(
            sender: &ServiceId,
            client_ts_ms: i64,
        ) -> presage::libsignal_service::prelude::Content {
            presage::libsignal_service::prelude::Content {
                metadata: Metadata {
                    sender: *sender,
                    destination: aci("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"),
                    sender_device: DeviceId::new(1).expect("device id"),
                    pni_verified: None,
                    client_timestamp: chrono::DateTime::from_timestamp_millis(client_ts_ms)
                        .expect("timestamp"),
                    server_timestamp: chrono::DateTime::from_timestamp_millis(client_ts_ms)
                        .expect("timestamp"),
                    needs_receipt: false,
                    unidentified_sender: false,
                    was_plaintext: false,
                    server_guid: None,
                    story: None,
                    ephemeral: None,
                    urgent: None,
                    report_spam_token: None,
                },
                body: ContentBody::DataMessage(DataMessage::default()),
            }
        }

        let own = aci(own_uuid);
        let sent = 1_700_000_000_000u64;

        // 窗内命中：恰等于 sentTimestamp 的本机出站行。
        let hit =
            nearest_outgoing_dispatch([outgoing_of(&own, sent as i64)].into_iter(), &own, sent);
        assert!(hit.dispatched);
        assert_eq!(hit.server_timestamp, Some(sent));

        // 多条命中取 |Δts| 最小：-1500 与 +500 并存 → +500 胜（遍历序无
        // 关——DESC 供入序与 ASC 供入序同答案）。
        let rows = [
            outgoing_of(&own, (sent - 1_500) as i64),
            outgoing_of(&own, (sent + 500) as i64),
        ];
        let nearest = nearest_outgoing_dispatch(rows.clone().into_iter(), &own, sent);
        assert!(nearest.dispatched);
        assert_eq!(nearest.server_timestamp, Some(sent + 500));
        let nearest_rev = nearest_outgoing_dispatch(rows.into_iter().rev(), &own, sent);
        assert_eq!(nearest_rev.server_timestamp, nearest.server_timestamp);

        // 他人行（对端来信）永不命中：出站判别 = sender 恒为本机 ACI。
        let miss = nearest_outgoing_dispatch(
            [outgoing_of(&aci(peer_uuid), sent as i64)].into_iter(),
            &own,
            sent,
        );
        assert!(!miss.dispatched);
        assert_eq!(miss.server_timestamp, None);

        // 混合行集：本机行与对端行并存，只认本机行。
        let mixed = nearest_outgoing_dispatch(
            [
                outgoing_of(&aci(peer_uuid), (sent + 10) as i64),
                outgoing_of(&own, (sent - 2_000) as i64),
            ]
            .into_iter(),
            &own,
            sent,
        );
        assert!(mixed.dispatched);
        assert_eq!(mixed.server_timestamp, Some(sent - 2_000));

        // 空窗诚实缺席：零行 → dispatched=false 且无 serverTimestamp。
        let empty = nearest_outgoing_dispatch(Vec::new().into_iter(), &own, sent);
        assert!(!empty.dispatched);
        assert_eq!(empty.server_timestamp, None);
    }

    /// lookupOutgoingDispatch 应答面形状钉定（内容红线）：成功应答只含
    /// dispatched / serverTimestamp 两键——dispatched=false 时单键，
    /// dispatched=true 时双键；无正文/号码/群名/作者键可挂（methods.rs
    /// 组装面同款两键 Map，此处在类型层再钉一次 no-null 折叠语义）。
    #[test]
    fn outgoing_dispatch_response_face_has_no_content_keys() {
        let own = ServiceId::Aci(
            presage::libsignal_service::prelude::Uuid::parse_str(
                "11111111-2222-4333-8444-555555555555",
            )
            .unwrap()
            .into(),
        );
        let lookup = nearest_outgoing_dispatch(Vec::new().into_iter(), &own, 1);
        // no-null：缺席与 false 折叠同一无键态——序列化面只出
        // {"dispatched":false}。
        let face = serde_json::json!({
            "dispatched": lookup.dispatched,
            "serverTimestamp": lookup.server_timestamp,
        });
        assert_eq!(face["dispatched"], serde_json::json!(false));
        assert_eq!(face["serverTimestamp"], serde_json::json!(null));
        // 命中面：serverTimestamp 在场为正整数。
        let hit = nearest_outgoing_dispatch(
            [presage::libsignal_service::prelude::Content {
                metadata: presage::libsignal_service::content::Metadata {
                    sender: own,
                    destination: own,
                    sender_device: presage::libsignal_service::protocol::DeviceId::new(1).unwrap(),
                    pni_verified: None,
                    client_timestamp: chrono::DateTime::from_timestamp_millis(42).unwrap(),
                    server_timestamp: chrono::DateTime::from_timestamp_millis(42).unwrap(),
                    needs_receipt: false,
                    unidentified_sender: false,
                    was_plaintext: false,
                    server_guid: None,
                    story: None,
                    ephemeral: None,
                    urgent: None,
                    report_spam_token: None,
                },
                body: ContentBody::DataMessage(DataMessage::default()),
            }]
            .into_iter(),
            &own,
            42,
        );
        assert!(hit.dispatched);
        assert_eq!(hit.server_timestamp, Some(42));
    }

    /// self 解析两臂（self 解析批 2026-10-16，compat §5）：号码臂=数字位
    /// 等于本机号码（E.164/裸号/排版形态同收敛，空串恒否）→ resolve_
    /// recipient 直解本机 ACI 不经 contacts；UUID 臂=service-id 串（含本机
    /// ACI）在首臂 parse 直解、不经号码臂（互斥由调用序保证，此处钉定
    /// parse 直解语义与本机 ACI 形态确实覆盖）。Manager 不可单测构造
    /// （RegistrationData 构造受限），判定逻辑以纯函数层验证。
    #[test]
    fn self_recipient_resolution_arms() {
        // —— self-by-e164 臂 ——
        assert!(self_recipient_by_number("+15551234567", "+15551234567"));
        // 裸号（无 +）/排版形态（international 空格连字符）与本机 E.164
        // 数字位同收敛。
        assert!(self_recipient_by_number("15551234567", "+15551234567"));
        assert!(self_recipient_by_number(
            "+1 (555) 123-4567",
            "+15551234567"
        ));
        // 非本机号码 → 否（照旧走 contacts 匹配臂）。
        assert!(!self_recipient_by_number("+15551230000", "+15551234567"));
        // 数字位前缀重叠 ≠ 相等。
        assert!(!self_recipient_by_number("+1555123456700", "+15551234567"));
        // 空串/无数字入参恒否（不劫持 contacts 臂）。
        assert!(!self_recipient_by_number("", "+15551234567"));
        assert!(!self_recipient_by_number("  ", "+15551234567"));

        // —— self-by-aci 臂：首臂 parse 直解（ACI/PNI 串形态）——
        let own_aci = "11111111-2222-4333-8444-555555555555";
        let parsed = ServiceId::parse_from_service_id_string(own_aci).expect("own aci parse");
        assert_eq!(
            parsed,
            ServiceId::Aci(
                presage::libsignal_service::prelude::Uuid::parse_str(own_aci)
                    .expect("uuid")
                    .into()
            )
        );
        // 带标签 PNI 形态同直解（vendored parse 词表 = 裸 UUID + PNI: 前缀；
        // ACI: 标签形不被接受——只钉定真实词表不发明）。
        assert!(ServiceId::parse_from_service_id_string(&format!("PNI:{own_aci}")).is_some());
        assert!(ServiceId::parse_from_service_id_string(&format!("ACI:{own_aci}")).is_none());
        // UUID 形态不被号码臂劫持（首臂先返的调用序防线）。
        assert!(!self_recipient_by_number(own_aci, "+15551234567"));
    }

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

        // 404 = 取钥端点面（send 链 NotFoundError 主导成因，cause 全覆盖
        // 批归 PreKey——§5 近似归类记档），与本账号凭据失效不同族，不得
        // 误报解绑。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::NotFoundError)),
            SendError::PreKey
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
    /// 算服务器明确拒绝（cause 全覆盖批 §5 归 WsRejected——应答即未入队），
    /// 不算不可用。
    #[test]
    fn send_errors_classify_structured_server_faces() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        // 限流：429/413 同归型 RateLimitExceeded（vendored response.rs）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(
                ServiceError::RateLimitExceeded { retry_after: None }
            )),
            SendError::RateLimited {
                retry_after_secs: None
            }
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::ServiceError(ServiceError::RateLimitExceeded {
                    retry_after: None
                })
            ))),
            SendError::RateLimited {
                retry_after_secs: None
            }
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
        // <500（如 400）= 服务器明确拒绝（cause 全覆盖批 §5），非不可用。
        assert!(matches!(unhandled(400), SendError::WsRejected));
    }

    /// send 链 cause 类全覆盖归型（2026-10-16 批，§5）：六族新变体的
    /// vendored 错误形状 → SendError 映射逐臂钉死——服务器明确拒绝族
    /// （508/499/413 send 面/404 未注册/4xx UnhandledResponseCode）、
    /// 未知结局族（Timeout/等响应相 WsClosing/WsError）、写出前断族
    /// （写出相 WsClosing reason 字面量）、取钥 404、修复环耗尽/协议面、
    /// 附件上行内外两层。
    #[test]
    fn send_errors_classify_cause_families() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        // 服务器明确拒绝族 → WsRejected（确定未送达）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::ServerRejected)),
            SendError::WsRejected
        ));
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(
                ServiceError::DeprecatedVersion
            )),
            SendError::WsRejected
        ));
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::MessageTooLarge)),
            SendError::WsRejected
        ));
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(
                ServiceError::UnregisteredRecipient
            )),
            SendError::WsRejected
        ));
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::NotFound {
                    service_id: ServiceId::parse_from_service_id_string(
                        "00000000-0000-0000-0000-000000000000",
                    )
                    .expect("aci"),
                },
            ))),
            SendError::WsRejected
        ));

        // 未知结局族 → WsTimeout（绝不自动重试）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::Timeout {
                reason: "test",
            })),
            SendError::WsTimeout
        ));
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::WsClosing {
                reason: "WebSocket closing while waiting for a response",
            })),
            SendError::WsTimeout
        ));

        // 写出前断族 → WsTransport（确定未送达；等响应相 reason 字面量
        // 区分单测）。
        assert!(matches!(
            send_error_of(presage::Error::ServiceError(ServiceError::WsClosing {
                reason: "WebSocket closing while sending request",
            })),
            SendError::WsTransport
        ));

        // 修复环耗尽/协议面 → SessionBuild（先于 wire）。
        assert!(matches!(
            send_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::MaximumRetriesLimitExceeded
            ))),
            SendError::SessionBuild
        ));

        // 附件上行内层 → AttachmentUpload；上传面归类单点 upload_error_of。
        assert!(matches!(
            upload_error_of(presage::Error::MessageSenderError(Box::new(
                MessageSenderError::AttachmentUploadError(
                    presage::libsignal_service::sender::AttachmentUploadError::IoError(
                        std::io::Error::other("test"),
                    ),
                )
            ))),
            SendError::AttachmentUpload
        ));
        // 上行外层沿 send_error_of 原语义（401 解绑归型比笼统「上传失败」
        // 更诚实，§5）。
        assert!(matches!(
            upload_error_of(presage::Error::ServiceError(ServiceError::Unauthorized)),
            SendError::Unbound
        ));
    }

    /// 快速重试硬边界（§5）：is_definite_failure 全变体钉死——确定未送达
    /// 五族为 true，未知结局（WsTimeout）与既有四族/本地面全部 false。
    #[test]
    fn send_fast_retry_boundary_is_definite_failure_only() {
        let definite = [
            SendError::WsRejected,
            SendError::WsTransport,
            SendError::PreKey,
            SendError::SessionBuild,
            SendError::AttachmentUpload,
        ];
        for error in definite {
            assert!(error.is_definite_failure(), "{error:?} 应可快速重试");
        }
        let never = [
            SendError::UnknownAccount,
            SendError::TargetTerminated,
            SendError::AnnouncementsOnly,
            SendError::Unbound,
            SendError::RateLimited {
                retry_after_secs: None,
            },
            SendError::ProofRequired,
            SendError::ServerUnavailable,
            SendError::IdentityUnverified,
            SendError::WsTimeout,
            SendError::Internal,
        ];
        for error in never {
            assert!(!error.is_definite_failure(), "{error:?} 绝不自动重试");
        }
    }

    /// project_content 现返通知向量：契约测试形状 = 恰一条投影，取首条
    /// （多通知形状由本批 profileKeyUpdate 面单测覆盖）。
    #[track_caller]
    fn pop_unwrap(notifications: Vec<Value>, message: &str) -> Value {
        assert_eq!(notifications.len(), 1, "{message}: expected single");
        notifications.into_iter().next().expect("single")
    }

    fn sweep_sandbox(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "kt-engine-sweep-{tag}-{:016x}",
            mix_seed_step(mix_seed())
        ));
        std::fs::create_dir_all(root.join(ACCOUNTS_DIR)).expect("accounts dir");
        root
    }

    fn write_store_file(accounts: &Path, name: &str) -> PathBuf {
        let dir = accounts.join(name);
        std::fs::create_dir_all(&dir).expect("store dir");
        std::fs::write(dir.join(crate::state::STORE_DB_FILE), b"credential-bytes")
            .expect("store.db");
        dir
    }

    /// P0 重链孤儿批端到端钉形（2026-10-23 真机事故）：
    /// 同号重链写新 dir → 被取代的旧目录归档 → 启动清扫零隔离 → 注册表可
    /// 重载。对照组（下一例）证明旧语义确实会隔离并造成启动 fail-closed——
    /// 差异来自「重链收尾归档」而非清扫放宽。
    #[tokio::test]
    async fn relink_archives_superseded_store_dir_and_avoids_quarantine() {
        use tokio::sync::Mutex as AsyncMutex;

        const OBSERVED: u64 = 1_700_000_000_777;
        let root = sweep_sandbox("relink-archive");
        let accounts = root.join(ACCOUNTS_DIR);
        // 先 load（空 accounts = 首次运行）再建 store：注册表缺失 + store 在场
        // 是 P0-B 的 fail-closed 面。
        let mut state = crate::state::EngineState::load(&root).expect("state");
        write_store_file(&accounts, "lk-old");
        write_store_file(&accounts, "lk-new");
        state
            .add_account(
                "+15550001111".to_owned(),
                Some("accounts/lk-old".to_owned()),
            )
            .expect("register old");
        let state = Arc::new(AsyncMutex::new(state));
        let (outbound, _rx) = crate::service::outbound::test_hub();
        let service = Service::new(root.clone(), Arc::clone(&state), outbound);

        // finish_link 同序：注册表先原子换新条目并把旧条目交回，再归档。
        let superseded = state
            .lock()
            .await
            .add_account_superseding(
                "+15550001111".to_owned(),
                Some("accounts/lk-new".to_owned()),
            )
            .expect("relink");
        assert_eq!(
            superseded.as_ref().and_then(|entry| entry.dir.as_deref()),
            Some("accounts/lk-old")
        );
        service
            .archive_superseded_link_dir(superseded, "accounts/lk-new")
            .await;

        assert!(
            !accounts.join("lk-old").try_exists().unwrap(),
            "被取代目录必须搬离 accounts/ 直接子目录（否则下次清扫当孤儿）"
        );
        let manifest =
            std::fs::read_to_string(accounts.join("_archive").join("link-superseded-log.jsonl"))
                .expect("archive manifest");
        let record: serde_json::Value =
            serde_json::from_str(manifest.lines().next().expect("one line")).expect("json");
        assert_eq!(record["reason"], "superseded_by_relink");
        assert_eq!(record["from"], "accounts/lk-old");
        let to = record["to"].as_str().expect("to").to_owned();
        assert_eq!(
            std::fs::read(root.join(&to).join("store.db")).unwrap(),
            b"credential-bytes".as_slice(),
            "归档保留凭证字节（不是删除/截断）"
        );

        let referenced: HashSet<String> = state
            .lock()
            .await
            .accounts()
            .iter()
            .filter_map(|entry| entry.dir.clone())
            .collect();
        let report = sweep_orphan_store_dirs_at(&root, &referenced, OBSERVED);
        assert_eq!(report.quarantined, 0, "重链后不得产生隔离条目");
        assert_eq!(report.failed, 0);
        assert!(
            !accounts.join(QUARANTINE_DIR).exists(),
            "隔离区必须保持空（非空 = 下次启动 fail closed）"
        );
        crate::state::EngineState::load(&root).expect("engine starts after relink");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 对照组：不归档的旧语义（= 修复前的重链收尾）——旧目录成孤儿，清扫
    /// 隔离它，隔离区非空使下一次启动 fail closed，正是「通道正在自恢复」
    /// 死循环的机制。本用例只证明机制，不改产品语义（fail-closed 保留）。
    #[tokio::test]
    async fn relink_without_archive_quarantines_and_blocks_next_start() {
        const OBSERVED: u64 = 1_700_000_000_888;
        let root = sweep_sandbox("relink-legacy");
        let accounts = root.join(ACCOUNTS_DIR);
        let mut state = crate::state::EngineState::load(&root).expect("state");
        write_store_file(&accounts, "lk-old");
        write_store_file(&accounts, "lk-new");
        state
            .add_account(
                "+15550002222".to_owned(),
                Some("accounts/lk-old".to_owned()),
            )
            .expect("register old");
        // 修复前：注册表只换条目，旧目录原地留下。
        state
            .add_account(
                "+15550002222".to_owned(),
                Some("accounts/lk-new".to_owned()),
            )
            .expect("relink");

        let referenced: HashSet<String> = state
            .accounts()
            .iter()
            .filter_map(|entry| entry.dir.clone())
            .collect();
        let report = sweep_orphan_store_dirs_at(&root, &referenced, OBSERVED);
        assert_eq!(report.quarantined, 1, "旧语义下旧目录被隔离");
        assert!(
            crate::state::EngineState::load(&root).is_err(),
            "隔离区非空 → 拒绝启动（fail-closed 语义保留）"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// P0-B：未登记但带 store.db 的目录 → 隔离（保留凭证），绝不 remove。
    /// 已登记目录、非 lk-* 目录、散落文件都不动。
    #[test]
    fn orphan_store_sweep_quarantines_store_dirs_and_keeps_referenced() {
        const OBSERVED: u64 = 1_700_000_000_123;
        let root = sweep_sandbox("quarantine");
        let accounts = root.join(ACCOUNTS_DIR);
        write_store_file(&accounts, "lk-live");
        write_store_file(&accounts, "lk-orphan-db");
        std::fs::create_dir_all(accounts.join("not-lk")).expect("non-lk dir");
        std::fs::write(accounts.join("lk-loose-file"), b"x").expect("loose file");

        let referenced: HashSet<String> = ["accounts/lk-live".to_owned()].into_iter().collect();
        let report = sweep_orphan_store_dirs_at(&root, &referenced, OBSERVED);

        assert_eq!(report.quarantined, 1);
        assert_eq!(report.removed_empty, 0);
        assert_eq!(report.kept_non_empty, 0);
        assert_eq!(report.failed, 0);
        assert!(accounts.join("lk-live/store.db").is_file());
        assert!(!accounts.join("lk-orphan-db").try_exists().unwrap());
        assert!(accounts.join("not-lk").is_dir());
        assert!(accounts.join("lk-loose-file").is_file());

        let quarantined = accounts
            .join(QUARANTINE_DIR)
            .join(OBSERVED.to_string())
            .join("lk-orphan-db");
        assert_eq!(
            std::fs::read(quarantined.join("store.db")).unwrap(),
            b"credential-bytes".as_slice(),
            "隔离必须保留原 store 字节（不是删除/截断）"
        );

        let manifest = std::fs::read_to_string(accounts.join(QUARANTINE_DIR).join(QUARANTINE_LOG))
            .expect("quarantine manifest");
        let records: Vec<serde_json::Value> = manifest
            .lines()
            .map(|line| serde_json::from_str(line).expect("manifest json"))
            .collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["dir"], "lk-orphan-db");
        assert_eq!(records[0]["reason"], "unregistered_store_dir");
        assert_eq!(records[0]["from"], "accounts/lk-orphan-db");
        assert_eq!(
            records[0]["to"].as_str(),
            Some(format!("accounts/_quarantine/{OBSERVED}/lk-orphan-db").as_str())
        );
        let restore = records[0]["restore"].as_str().expect("restore hint");
        assert!(
            restore.contains("engine-state.json"),
            "恢复指引须包含注册表登记: {restore}"
        );
        assert!(
            restore.contains("accounts/_quarantine"),
            "放弃指引须包含隔离区: {restore}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 只有空目录可删；无 store.db 但仍有残留文件的目录无法证明可丢弃 → 保留。
    #[test]
    fn orphan_store_sweep_removes_only_empty_dirs() {
        const OBSERVED: u64 = 1_700_000_000_124;
        let root = sweep_sandbox("empty");
        let accounts = root.join(ACCOUNTS_DIR);
        std::fs::create_dir_all(accounts.join("lk-empty")).expect("empty dir");
        std::fs::create_dir_all(accounts.join("lk-partial/sessions")).expect("partial dir");
        std::fs::write(accounts.join("lk-partial/sessions/keep"), b"half-written")
            .expect("partial file");
        write_store_file(&accounts, "lk-with-store");

        let report = sweep_orphan_store_dirs_at(&root, &HashSet::new(), OBSERVED);

        assert_eq!(report.removed_empty, 1);
        assert_eq!(report.kept_non_empty, 1);
        assert_eq!(report.quarantined, 1);
        assert_eq!(report.failed, 0);
        assert!(!accounts.join("lk-empty").try_exists().unwrap());
        assert!(accounts.join("lk-partial/sessions/keep").is_file());
        assert!(accounts
            .join(QUARANTINE_DIR)
            .join(OBSERVED.to_string())
            .join("lk-with-store/store.db")
            .is_file());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 幂等：二次启动不再扫隔离区；重复隔离不覆盖已有隔离内容。
    #[test]
    fn orphan_store_sweep_is_idempotent_and_never_overwrites_quarantine() {
        const FIRST: u64 = 1_700_000_000_125;
        const SECOND: u64 = 1_700_000_000_126;
        let root = sweep_sandbox("idempotent");
        let accounts = root.join(ACCOUNTS_DIR);
        write_store_file(&accounts, "lk-once");
        let existing = accounts
            .join(QUARANTINE_DIR)
            .join(FIRST.to_string())
            .join("lk-once");
        std::fs::create_dir_all(&existing).expect("existing quarantine");
        std::fs::write(existing.join("store.db"), b"older-quarantine").expect("older store");

        let first = sweep_orphan_store_dirs_at(&root, &HashSet::new(), FIRST);
        assert_eq!(first.quarantined, 1);
        assert_eq!(
            std::fs::read(existing.join("store.db")).unwrap(),
            b"older-quarantine".as_slice(),
            "同名隔离目录不得被覆盖"
        );
        let moved = accounts
            .join(QUARANTINE_DIR)
            .join(FIRST.to_string())
            .join("lk-once.1");
        assert_eq!(
            std::fs::read(moved.join("store.db")).unwrap(),
            b"credential-bytes".as_slice(),
            "冲突时用后缀落新隔离目录"
        );

        let second = sweep_orphan_store_dirs_at(&root, &HashSet::new(), SECOND);
        assert_eq!(second, SweepReport::default(), "二次启动不得再动隔离区");
        assert_eq!(
            std::fs::read(moved.join("store.db")).unwrap(),
            b"credential-bytes".as_slice()
        );
        assert_eq!(
            std::fs::read(existing.join("store.db")).unwrap(),
            b"older-quarantine".as_slice()
        );
        assert!(
            !moved.join(QUARANTINE_DIR).exists(),
            "隔离区不匹配 lk-* 扫描规则，不得嵌套再隔离"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 隔离目录不可建（被同名文件占位）时原地保留源目录，绝不回退删除。
    #[test]
    fn orphan_store_sweep_keeps_source_when_quarantine_unavailable() {
        const OBSERVED: u64 = 1_700_000_000_127;
        let root = sweep_sandbox("quarantine-fail");
        let accounts = root.join(ACCOUNTS_DIR);
        let source = write_store_file(&accounts, "lk-victim");
        let quarantine_root = accounts.join(QUARANTINE_DIR);
        std::fs::create_dir_all(&quarantine_root).expect("quarantine root");
        std::fs::write(
            quarantine_root.join(OBSERVED.to_string()),
            b"not-a-directory",
        )
        .expect("blocked bucket");

        let report = sweep_orphan_store_dirs_at(&root, &HashSet::new(), OBSERVED);

        assert_eq!(report.quarantined, 0);
        assert_eq!(report.failed, 1);
        assert_eq!(
            std::fs::read(source.join("store.db")).unwrap(),
            b"credential-bytes".as_slice(),
            "隔离失败必须原地保留"
        );
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
    fn attachment_data_uri_parses_gif_param() {
        let parsed = |uri| parse_attachment_data_uri(uri).expect("parses");
        let none = |uri| assert!(parse_attachment_data_uri(uri).is_none());

        // 基线：mediatype image/gif 不自动带旗标——GIF 位是显式参数面，
        // 非内容嗅探（官方 Tenor GIF 常为 video/mp4 载体，同不嗅探）。
        let plain = parsed("data:image/gif;base64,aGVsbG8=");
        assert!(!plain.is_gif);

        // gif 裸参数与 gif=true 等价（voice 同款参数面）。
        for uri in [
            "data:video/mp4;gif;base64,aGVsbG8=",
            "data:video/mp4;gif=true;base64,aGVsbG8=",
            "data:video/mp4;gif=true;filename=tenor%20clip.mp4;base64,aGVsbG8=",
        ] {
            assert!(parsed(uri).is_gif);
        }

        // gif=false 显式不带旗标；非 bool 值 fail-closed（voice 同款）。
        assert!(!parsed("data:image/gif;gif=false;base64,aGVsbG8=").is_gif);
        none("data:image/gif;gif=1;base64,aGVsbG8=");
        none("data:image/gif;gif=yes;base64,aGVsbG8=");

        // voice+gif 共存：两旗标互不挤占（wire 按位掩码叠加）。
        let both = parsed("data:video/mp4;voice=true;gif=true;base64,aGVsbG8=");
        assert_eq!(both.spec.voice_note, Some(true));
        assert!(both.is_gif);
    }

    #[test]
    fn attachment_data_uri_decodes_incident_sized_payload() {
        use base64::Engine as _;

        // 2026-10-19 实证回归：4.4MB 视频以 data URI 内联进 send 帧
        // （≈5.9MB 单帧；request_bytes=5947588）。解码面必须完整接受
        // 该载荷，字节数/长度陈述与官方 upload_attachments 输入对齐。
        let payload: Vec<u8> = (0..4_400_000u32).map(|index| (index % 251) as u8).collect();
        let uri = format!(
            "data:video/mp4;filename=clip.mp4;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(&payload)
        );
        assert!(uri.len() > 5 * 1024 * 1024, "data uri length {}", uri.len());
        let parsed = parse_attachment_data_uri(&uri).expect("4.4MB payload parses");
        assert_eq!(parsed.bytes.len(), payload.len());
        assert_eq!(parsed.bytes, payload);
        assert_eq!(parsed.spec.length, payload.len());
        assert_eq!(parsed.spec.content_type, "video/mp4");
        assert_eq!(parsed.spec.file_name.as_deref(), Some("clip.mp4"));
    }

    #[test]
    fn apply_outgoing_caption_spec_face() {
        // 媒体 story caption 置位单点（2026-10-15 批）：Some = 首条正文附件
        // spec 载 caption（vendored upload_attachment spec.caption 直通返回
        // 指针 AttachmentPointer.caption，官方 uploadAttachment caption 进
        // 指针同构）；None = 零变化不覆盖；空 parsed 批（preview 单条面）
        // 不 panic 不置位。
        let uri = "data:image/jpeg;base64,aGVsbG8=";
        let mut parsed = vec![parse_attachment_data_uri(uri).expect("parses")];
        assert_eq!(parsed[0].spec.caption, None, "解析面 caption 恒缺席");
        apply_outgoing_caption(&mut parsed, Some("story caption"));
        assert_eq!(parsed[0].spec.caption.as_deref(), Some("story caption"));

        let mut parsed = vec![parse_attachment_data_uri(uri).expect("parses")];
        apply_outgoing_caption(&mut parsed, None);
        assert_eq!(parsed[0].spec.caption, None, "None = 零变化");

        // 正文附件 + 预览图同批形态：置位首条（正文），预览图不受染。
        let mut batch = vec![
            parse_attachment_data_uri(uri).expect("parses"),
            parse_attachment_data_uri("data:image/png;base64,aGVsbG8=").expect("parses"),
        ];
        apply_outgoing_caption(&mut batch, Some("story caption"));
        assert_eq!(batch[0].spec.caption.as_deref(), Some("story caption"));
        assert_eq!(batch[1].spec.caption, None, "preview 位不受染");

        let mut bare: Vec<ParsedAttachment> = Vec::new();
        apply_outgoing_caption(&mut bare, Some("no-op"));
        assert!(bare.is_empty());
    }

    #[test]
    fn outgoing_gif_flag_or_masks_and_is_idempotent() {
        use presage::proto::attachment_pointer::Flags;
        use presage::proto::AttachmentPointer;

        // 平指针：置位后恰 GIF=8 单位。
        let mut plain = AttachmentPointer::default();
        apply_outgoing_gif_flag(&mut plain);
        assert_eq!(plain.flags, Some(Flags::Gif as u32));

        // voice 已置位指针：按位叠加不挤占（官方 voice+gif 同位掩码面）。
        let mut voiced = AttachmentPointer {
            flags: Some(Flags::VoiceMessage as u32),
            ..AttachmentPointer::default()
        };
        apply_outgoing_gif_flag(&mut voiced);
        assert_eq!(
            voiced.flags,
            Some(Flags::VoiceMessage as u32 | Flags::Gif as u32)
        );

        // 已置 GIF 再置：幂等（不翻倍不变形）。
        apply_outgoing_gif_flag(&mut plain);
        assert_eq!(plain.flags, Some(Flags::Gif as u32));
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

    /// 语音笔记元数据收投影（2026-10-14 批）：waveform 标准 base64
    /// 非空才投（空字节串零信息量）、duration float 秒在场即投；全缺 =
    /// 两键不在（历史普通附件行零新增键）。
    #[test]
    fn attachment_pointer_projection_carries_voice_waveform_and_duration() {
        let out = project_attachment_pointer(&AttachmentPointer {
            audio_waveform: Some(vec![0, 128, 255]),
            audio_duration_seconds: Some(2.5),
            ..Default::default()
        });
        assert_eq!(
            out["waveform"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0u8, 128, 255])
                    .as_str()
            )
        );
        assert_eq!(out["duration"], json!(2.5));

        // 空字节串 waveform 不投；duration 独立如实投（无成对闸）。
        let out = project_attachment_pointer(&AttachmentPointer {
            audio_waveform: Some(Vec::new()),
            audio_duration_seconds: Some(1.5),
            ..Default::default()
        });
        assert!(out.get("waveform").is_none());
        assert_eq!(out["duration"], json!(1.5));

        let out = project_attachment_pointer(&AttachmentPointer::default());
        assert!(out.get("waveform").is_none());
        assert!(out.get("duration").is_none());
    }

    /// 出向语音元数据置位面（2026-10-14 批）：置位后指针带波形+时长两位；
    /// 与 GIF 旗标共存互不挤占（flags 按位掩码、元数据独立字段）。
    #[test]
    fn outgoing_voice_metadata_applied_to_pointer() {
        let metadata = voice_note::VoiceAudioMetadata {
            waveform: vec![7_u8; 100],
            duration_seconds: 1.5,
        };
        let mut pointer = AttachmentPointer::default();
        apply_outgoing_voice_metadata(&mut pointer, &metadata);
        assert_eq!(pointer.audio_waveform.as_deref(), Some(&[7_u8; 100][..]));
        assert_eq!(pointer.audio_duration_seconds, Some(1.5));

        apply_outgoing_gif_flag(&mut pointer);
        assert_eq!(pointer.flags, Some(Flags::Gif as u32));
        assert_eq!(
            pointer.audio_waveform.as_deref().map(<[u8]>::len),
            Some(100)
        );
        assert_eq!(pointer.audio_duration_seconds, Some(1.5));
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
                project_content("acct", store, &content, registry, None).await,
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

    /// attachmentBackfillResponse 收投影臂（官方
    /// #handleAttachmentBackfillResponse 同位
    /// MessageReceiver.preload.ts:3146-3151/:3806）：target 面
    /// {author,sentTimestamp}/{serviceId|groupId|e164} 双闸（官方
    /// processAddressableMessage/processConversationIdentifier 同构，缺席
    /// 丢整信封）；error 词 / attachments 批（status 词 SCREAMING +
    /// UNKNOWN 兜、attachment 指针全投影同源）；data 缺席丢整信封
    /// （官方 strictAssert 同判）。
    #[tokio::test]
    async fn sync_attachment_backfill_response_arm_projects_minimal_face() {
        use base64::Engine as _;
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{
            sync_message, AddressableMessage, AttachmentPointer, ConversationIdentifier,
            SyncMessage,
        };

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-backfill-face-{:016x}",
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
                project_content("acct", store, &content, registry, None).await,
                "backfill arm projects",
            )["params"]["envelope"]["syncMessage"]["attachmentBackfillResponse"]
                .clone()
        }

        // 完整面：attachments 批（status 词条 + 指针词条 + longText）。
        let response = sync_message::AttachmentBackfillResponse {
            target_message: Some(AddressableMessage {
                sent_timestamp: Some(1_690_000_000_000),
                author: Some(presage::proto::addressable_message::Author::AuthorServiceIdBinary(
                    [
                        0x11_u8, 0x11, 0x11, 0x11, 0x22, 0x22, 0x43, 0x33, 0x84, 0x44, 0x55,
                        0x55, 0x55, 0x55, 0x55, 0x55,
                    ]
                    .to_vec(),
                )),
            }),
            target_conversation: Some(ConversationIdentifier {
                identifier: Some(
                    presage::proto::conversation_identifier::Identifier::ThreadGroupId(
                        vec![0xAB_u8; 16],
                    ),
                ),
            }),
            data: Some(sync_message::attachment_backfill_response::Data::Attachments(
                sync_message::attachment_backfill_response::AttachmentDataList {
                    attachments: vec![
                        sync_message::attachment_backfill_response::AttachmentData {
                            data: Some(
                                sync_message::attachment_backfill_response::attachment_data::Data::Status(
                                    sync_message::attachment_backfill_response::attachment_data::Status::TerminalError as i32,
                                ),
                            ),
                        },
                        sync_message::attachment_backfill_response::AttachmentData {
                            data: Some(
                                sync_message::attachment_backfill_response::attachment_data::Data::Attachment(
                                    AttachmentPointer {
                                        client_uuid: Some([0xEF_u8; 16].to_vec()),
                                        content_type: Some("image/png".to_owned()),
                                        size: Some(4096),
                                        key: Some(vec![0x5A_u8; 64]),
                                        cdn_number: Some(2),
                                        ..Default::default()
                                    },
                                ),
                            ),
                        },
                    ],
                    long_text: Some(sync_message::attachment_backfill_response::AttachmentData {
                        data: Some(
                            sync_message::attachment_backfill_response::attachment_data::Data::Status(
                                sync_message::attachment_backfill_response::attachment_data::Status::Pending as i32,
                            ),
                        ),
                    }),
                },
            )),
        };
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::AttachmentBackfillResponse(response)),
        )
        .await;
        assert_eq!(
            face["targetMessage"]["author"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(
            face["targetMessage"]["sentTimestamp"],
            json!(1_690_000_000_000_u64)
        );
        assert_eq!(
            face["targetConversation"]["groupId"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode([0xAB_u8; 16])
                    .as_str()
            )
        );
        assert_eq!(face["attachments"].as_array().map(Vec::len), Some(2));
        assert_eq!(face["attachments"][0]["status"], json!("TERMINAL_ERROR"));
        assert_eq!(
            face["attachments"][1]["attachment"]["contentType"],
            json!("image/png")
        );
        assert_eq!(face["attachments"][1]["attachment"]["size"], json!(4096));
        assert_eq!(
            face["attachments"][1]["attachment"]["locator"]["cdnNumber"],
            json!(2)
        );
        assert_eq!(face["longText"]["status"], json!("PENDING"));
        assert!(face.get("error").is_none());

        // error 臂：MESSAGE_NOT_FOUND 词。
        let face = sync_arm_face(
            &store,
            &registry,
            make(sync_message::Content::AttachmentBackfillResponse(
                sync_message::AttachmentBackfillResponse {
                    target_message: Some(AddressableMessage {
                        sent_timestamp: Some(1_690_000_000_000),
                        author: Some(presage::proto::addressable_message::Author::AuthorE164(
                            "+15550001111".to_owned(),
                        )),
                    }),
                    target_conversation: Some(ConversationIdentifier {
                        identifier: Some(
                            presage::proto::conversation_identifier::Identifier::ThreadServiceId(
                                "11111111-2222-4333-8444-555555555555".to_owned(),
                            ),
                        ),
                    }),
                    data: Some(sync_message::attachment_backfill_response::Data::Error(
                        sync_message::attachment_backfill_response::Error::MessageNotFound as i32,
                    )),
                },
            )),
        )
        .await;
        assert_eq!(face["error"], json!("MESSAGE_NOT_FOUND"));
        assert_eq!(face["targetMessage"]["author"], json!("+15550001111"));
        assert_eq!(
            face["targetConversation"]["serviceId"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert!(face.get("attachments").is_none());

        // targetMessage 缺席 → 整信封丢弃（官方 strictAssert 同判）。
        let dropped = project_content(
            "acct",
            &store,
            &make(sync_message::Content::AttachmentBackfillResponse(
                sync_message::AttachmentBackfillResponse {
                    target_message: None,
                    target_conversation: None,
                    data: None,
                },
            )),
            &registry,
            None,
        )
        .await;
        assert!(dropped.is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// 附件回填请求构帧（附件回填闭环批，官方
    /// getAttachmentBackfillSyncMessage 对位）：content oneof
    /// attachmentBackfillRequest=24，targetMessage author binary 现行面 +
    /// sentTimestamp，targetConversation 三腿（serviceId binary / groupId /
    /// serviceId string 降写）；padded sync message（padding 位非空）。
    #[test]
    fn attachment_backfill_request_sync_frame() {
        use presage::proto::{addressable_message, conversation_identifier, sync_message};

        let author =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("author aci");
        // direct 腿（UUID 对端）：author binary 面 + serviceId binary 面。
        let frame = build_attachment_backfill_sync(
            &MessageRequestTarget::Thread("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae".to_owned()),
            &author,
            1_690_000_000_000,
        );
        let Some(sync_message::Content::AttachmentBackfillRequest(request)) = &frame.content else {
            panic!("content oneof 须为 attachmentBackfillRequest");
        };
        let Some(addressable_message::Author::AuthorServiceIdBinary(bytes)) = request
            .target_message
            .as_ref()
            .and_then(|m| m.author.as_ref())
        else {
            panic!("author 须为 binary 现行面");
        };
        assert_eq!(
            ServiceId::parse_from_service_id_binary(bytes)
                .map(|sid| sid.service_id_string())
                .as_deref(),
            Some("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(
            request
                .target_message
                .as_ref()
                .and_then(|m| m.sent_timestamp),
            Some(1_690_000_000_000)
        );
        let Some(conversation_identifier::Identifier::ThreadServiceIdBinary(peer)) = request
            .target_conversation
            .as_ref()
            .and_then(|c| c.identifier.as_ref())
        else {
            panic!("direct 腿须为 serviceId binary 面");
        };
        assert_eq!(
            ServiceId::parse_from_service_id_binary(peer)
                .map(|sid| sid.service_id_string())
                .as_deref(),
            Some("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
        );
        assert!(!frame.padding.is_none(), "padded sync message");

        // 群腿：threadGroupId 原样 32B。
        let frame = build_attachment_backfill_sync(
            &MessageRequestTarget::Group(vec![0xAB_u8; 32]),
            &author,
            1_690_000_000_001,
        );
        let Some(sync_message::Content::AttachmentBackfillRequest(request)) = &frame.content else {
            panic!("content oneof 须为 attachmentBackfillRequest");
        };
        assert!(
            matches!(
                request
                    .target_conversation
                    .as_ref()
                    .and_then(|c| c.identifier.as_ref()),
                Some(conversation_identifier::Identifier::ThreadGroupId(key))
                    if key == &vec![0xAB_u8; 32]
            ),
            "群腿须为 threadGroupId 原样"
        );

        // 非 UUID 对端（E.164 降写面）：threadServiceId string 腿。
        let frame = build_attachment_backfill_sync(
            &MessageRequestTarget::Thread("+15555550101".to_owned()),
            &author,
            1_690_000_000_002,
        );
        let Some(sync_message::Content::AttachmentBackfillRequest(request)) = &frame.content else {
            panic!("content oneof 须为 attachmentBackfillRequest");
        };
        assert!(
            matches!(
                request
                    .target_conversation
                    .as_ref()
                    .and_then(|c| c.identifier.as_ref()),
                Some(conversation_identifier::Identifier::ThreadServiceId(text))
                    if text == "+15555550101"
            ),
            "非 UUID 对端降写 threadServiceId string 面"
        );
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
                None,
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
        // 三键布尔面评估批：AEP 非空 → accountEntropyPoolPresent=true（材料
        // 键仍不在，Present 布尔与官方材料面同名键区隔）。
        assert_eq!(
            envelope["syncMessage"]["keys"]["accountEntropyPoolPresent"],
            json!(true)
        );

        // 空串 AEP 与缺席同为无键（非空才投）。
        let envelope = pop_unwrap(
            project_content(
                "acct",
                &store,
                &make(sync_message::Keys {
                    account_entropy_pool: Some(String::new()),
                    media_root_backup_key: None,
                }),
                &registry,
                None,
            )
            .await,
            "keys projects empty aep",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["syncMessage"]["keys"], json!({}));

        // 缺：全缺照投空对象。
        let envelope = pop_unwrap(
            project_content(
                "acct",
                &store,
                &make(sync_message::Keys::default()),
                &registry,
                None,
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

    /// 读面探针类别钉形（2026-10-16 listGroups Internal 归因批）：reject
    /// 类别三分（非字符串/base64 坏/长度错）与零内容纪律（字面量不含
    /// 参数值）；SendError 变体路径穷尽映射代表性臂 + 兜底字面量形状。
    #[test]
    fn list_read_face_probe_stage_and_variant_faces() {
        assert_eq!(
            group_filter_reject_stage(&serde_json::json!(32)),
            "filter_not_string"
        );
        assert_eq!(
            group_filter_reject_stage(&serde_json::json!("not base64!!")),
            "filter_base64_invalid"
        );
        assert_eq!(
            group_filter_reject_stage(&serde_json::json!(
                base64::engine::general_purpose::STANDARD.encode([7u8; 16])
            ),),
            "filter_key_len"
        );
        // 与主判定矛盾位（decode 成功不该进 reject 路径）：防御位字面量。
        let key = base64::engine::general_purpose::STANDARD.encode([7u8; 32]);
        assert_eq!(
            group_filter_reject_stage(&serde_json::json!(key)),
            "filter_decoded_ok"
        );
        // 变体路径：读面两臂高频形状（UnknownAccount 折 -32603、Internal）
        // 字面量即完整类型路径（日志拼读面，零 payload）。
        assert_eq!(
            send_error_variant_face(&SendError::UnknownAccount),
            "SendError::UnknownAccount"
        );
        assert_eq!(
            send_error_variant_face(&SendError::Internal),
            "SendError::Internal"
        );
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

    /// S4 story 反应发送批：`build_story_reaction_data_message` 钉形——
    /// Reaction 与 StoryContext 两面同锚（targetAuthorAci/storyContext.
    /// authorAci 同一作者 ACI，targetSentTimestamp/sentTimestamp 同一 story
    /// 时刻毫秒）；remove 恒 Some(false)（官方 reactionForSend 硬编码同位，
    /// 方法面无 remove 参）；string 形 authorAci（mentions/quote/storyReply
    /// 先例），binary 位不写；timestamp = 腿发送时刻。
    #[test]
    fn story_reaction_data_message_build_shape() {
        let author = "11111111-2222-4333-8444-555555555555";
        let dm = build_story_reaction_data_message(author, 1_700_000_000_000, "👍", 7_777);
        let reaction = dm.reaction.expect("reaction set");
        assert_eq!(reaction.emoji.as_deref(), Some("👍"));
        assert_eq!(reaction.target_author_aci.as_deref(), Some(author));
        assert_eq!(reaction.target_sent_timestamp, Some(1_700_000_000_000));
        assert_eq!(reaction.remove, Some(false));
        let context = dm.story_context.expect("storyContext set");
        assert_eq!(context.author_aci.as_deref(), Some(author));
        assert_eq!(context.sent_timestamp, Some(1_700_000_000_000));
        assert!(context.author_aci_binary.is_none());
        assert_eq!(dm.timestamp, Some(7_777));
        assert!(dm.body.is_none(), "story reaction 无正文位");
    }

    /// E-S7r 群 story 反应腿 wire 钉形：`DataMessage{groupV2, reaction,
    /// storyContext}` 三键共存逐键（官方 sendNormalMessage 群臂
    /// groupSendOptions 载 reaction+storyContext + SendMessage toProto
    /// groupV2/storyContext/reaction 三独立键同位；非 Content.storyMessage
    /// ——S4 批「story 通道 groupV2 形态」措辞失真随本批订正）。构造复用
    /// build_story_reaction_data_message（1:1 腿单点），群上下文注入形状与
    /// group_mrm::dispatch_group_message:211-216 逐字同源（dispatch 离线
    /// 不可达，镜像钉形）：{master_key 32B, revision 群态, group_change
    /// 恒 None}。
    #[test]
    fn story_reaction_group_data_message_three_keys_shape() {
        let author = "11111111-2222-4333-8444-555555555555";
        let mut body = build_story_reaction_data_message(author, 1_700_000_000_000, "👍", 7_777);
        body.group_v2 = Some(presage::proto::GroupContextV2 {
            master_key: Some(vec![9u8; 32]),
            revision: Some(7),
            group_change: None,
        });
        // 群上下文键：32B master key + 群态 revision + groupChange 恒缺。
        let group_v2 = body.group_v2.as_ref().expect("groupV2 set");
        assert_eq!(group_v2.master_key, Some(vec![9u8; 32]));
        assert_eq!(group_v2.revision, Some(7));
        assert!(group_v2.group_change.is_none());
        // reaction 键（与 1:1 腿同构：remove 恒 Some(false)）。
        let reaction = body.reaction.as_ref().expect("reaction set");
        assert_eq!(reaction.emoji.as_deref(), Some("👍"));
        assert_eq!(reaction.target_author_aci.as_deref(), Some(author));
        assert_eq!(reaction.target_sent_timestamp, Some(1_700_000_000_000));
        assert_eq!(reaction.remove, Some(false));
        // storyContext 键（与 reaction 同锚同值）。
        let context = body.story_context.as_ref().expect("storyContext set");
        assert_eq!(context.author_aci.as_deref(), Some(author));
        assert_eq!(context.sent_timestamp, Some(1_700_000_000_000));
        // 纯三键共存：正文/附件/删除位缺席。
        assert!(body.body.is_none());
        assert!(body.attachments.is_empty());
        assert!(body.delete.is_none());
        assert_eq!(body.timestamp, Some(7_777));
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

    /// 测试夹具：向 store 写一份合法注册数据（官方落库 JSON 形，ACI 直给；
    /// 其余键值与往返测试同款合法值），返回 store 目录。store 连接随返回
    /// drop（sqlx WAL 多连接安全，被测面 account_aci/list_accounts 自行
    /// 重开，生产路径 open_registered 同模式）。
    async fn seed_store_with_aci(data_dir: &Path, rel: &str, aci: &str) -> PathBuf {
        use presage::libsignal_service::utils::TryIntoE164;

        let store_dir = data_dir.join(rel);
        let e164 = "+15551234567".try_into_e164().expect("e164");
        let phone_number = presage::libsignal_service::utils::phonenumber_from_signal(&e164);
        let data: RegistrationData = serde_json::from_str(
            &json!({
                "signal_servers": "Production",
                "device_name": "KT Signal Engine",
                "phone_number": serde_json::to_value(&phone_number).expect("phone serializes"),
                "uuid": aci,
                "pni": "7b3dc19e-11f2-4c5a-9d0e-2f6a8b4c1d3e",
                "password": "abcdefghijklmnopqrstuvwx",
                "device_id": Some(2u32),
                "registration_id": 1234u32,
                "pni_registration_id": Some(5678u32),
                "profile_key": base64::engine::general_purpose::STANDARD.encode([7u8; 32]),
            })
            .to_string(),
        )
        .expect("registration data deserializes");
        let mut store = open_store(&store_dir).await.expect("store opens");
        store
            .save_registration_data(&data)
            .await
            .expect("registration data saves");
        store_dir
    }

    /// wire additive `aci` 键真值面（connector 契约 1.142 消费面）：注册
    /// 数据 `service_ids.aci` 原生序列化形直投——dashed lowercase 原样，
    /// 引擎零二次变换。
    #[tokio::test]
    async fn account_aci_projects_registration_uuid_dashed_lowercase() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-aci-truth-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store_dir = seed_store_with_aci(
            &data_dir,
            "accounts/test-aci",
            "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
        )
        .await;
        assert_eq!(
            account_aci(&store_dir).await,
            Some("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae".to_owned())
        );
    }

    /// 缺席面一：store 在、注册数据缺席（链接半途/未注册 store）= None
    ///（wire 整键缺席，不出 null、不出垃圾态）。
    #[tokio::test]
    async fn account_aci_absent_without_registration_data() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-aci-empty-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store_dir = data_dir.join("accounts/test-empty");
        open_store(&store_dir).await.expect("store opens");
        assert_eq!(account_aci(&store_dir).await, None);
    }

    /// 缺席面二：注册表悬挂引用（store 目录不存在）= None 同缺席口径。
    #[tokio::test]
    async fn account_aci_absent_for_hanging_store_dir() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-aci-hang-{:016x}",
            mix_seed_step(mix_seed())
        ));
        assert_eq!(
            account_aci(&data_dir.join("accounts/never-created")).await,
            None
        );
    }

    /// listAccounts 多账号投影：各账号各带各的 aci（逐 store 读注册数据
    /// 据），fake 账号（dir=None）整键缺席。回填能力门数据面（2026-10-20
    /// 批）：能力位 per-account 投影（缓存预热 = 单测期不触网，命中路径与
    /// 生产同一条 `attachment_backfill_cached`）。
    #[tokio::test]
    async fn list_account_rows_carry_per_account_aci() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-aci-rows-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, _rx) = outbound::test_hub();
        let service = Service::new(data_dir.clone(), Arc::clone(&state), outbound);

        seed_store_with_aci(
            &data_dir,
            "accounts/test-a",
            "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
        )
        .await;
        seed_store_with_aci(
            &data_dir,
            "accounts/test-b",
            "11111111-2222-4333-8444-555555555555",
        )
        .await;
        {
            let mut state = state.lock().await;
            state
                .add_account(
                    "+15550001111".to_owned(),
                    Some("accounts/test-a".to_owned()),
                )
                .expect("add a");
            state
                .add_account(
                    "+15550002222".to_owned(),
                    Some("accounts/test-b".to_owned()),
                )
                .expect("add b");
            state
                .add_account("+15550003333".to_owned(), None)
                .expect("add fake");
        }
        // 预热能力位缓存：本测试钉投影/多账号隔离，不钉网络面（seed 的
        // 注册数据是假凭据，真 fetch 会打网络且非确定）。a=true、b=false
        // 同时钉「明确 false 也照实投」。
        service
            .own_capabilities_cache
            .put("+15550001111", true, std::time::Instant::now());
        service
            .own_capabilities_cache
            .put("+15550002222", false, std::time::Instant::now());

        let rows = service.list_accounts().await;
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            json!({
                "number": "+15550001111",
                "aci": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
                "capabilities": {"attachmentBackfill": true},
            })
        );
        assert_eq!(
            rows[1],
            json!({
                "number": "+15550002222",
                "aci": "11111111-2222-4333-8444-555555555555",
                "capabilities": {"attachmentBackfill": false},
            })
        );
        // fake 账号（real 服务下的 dir=None）：aci 与 capabilities 整键缺席。
        assert_eq!(rows[2], json!({"number": "+15550003333"}));
        assert!(rows[2].get("aci").is_none());
        assert!(rows[2].get("capabilities").is_none());
    }

    /// 取不到 profile（store 在但无注册数据，链接半途/未注册）→ 条目照常
    /// 返回、`capabilities` 整键缺席（不投 false、不连坐列表）。本地前置
    /// 即缺席，零网络。
    #[tokio::test]
    async fn list_accounts_absent_capabilities_keeps_row() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-caps-absent-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, _rx) = outbound::test_hub();
        let service = Service::new(data_dir.clone(), Arc::clone(&state), outbound);
        open_store(&data_dir.join("accounts/test-unregistered"))
            .await
            .expect("empty store opens");
        state
            .lock()
            .await
            .add_account(
                "+15550004444".to_owned(),
                Some("accounts/test-unregistered".to_owned()),
            )
            .expect("add account");

        let rows = service.list_accounts().await;
        assert_eq!(rows, vec![json!({"number": "+15550004444"})]);
        assert!(
            rows[0].get("capabilities").is_none(),
            "读不到 profile = 整键缺席，不投 false"
        );
    }

    /// A4：deleteAccount 的收口顺序 = 停接收 → 等在飞处理单元完成 → drop
    /// Manager（store 落盘）→ 删目录。停机过程中有在飞帧时，删除等待它
    /// 结算，目录在结算前不消失（不丢未结算行）。
    #[tokio::test]
    async fn delete_account_waits_for_inflight_envelope_before_removing_store() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let data_dir = std::env::temp_dir().join(format!(
                    "kt-engine-delete-drain-{:016x}",
                    mix_seed_step(mix_seed())
                ));
                let store_dir = data_dir.join("accounts/lk-drain");
                std::fs::create_dir_all(&store_dir).expect("store dir");
                let state = Arc::new(AsyncMutex::new(
                    EngineState::load(&data_dir).expect("state"),
                ));
                state
                    .lock()
                    .await
                    .add_account(
                        "+15550009999".to_owned(),
                        Some("accounts/lk-drain".to_owned()),
                    )
                    .expect("add account");
                let (outbound, _rx) = outbound::test_hub();
                let service = Service::new(data_dir.clone(), Arc::clone(&state), outbound);

                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = tokio::sync::oneshot::channel();
                let settled = std::rc::Rc::new(std::cell::Cell::new(false));
                let settled_task = std::rc::Rc::clone(&settled);
                let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
                let done = tokio::task::spawn_local(async move {
                    // 在飞信封：停旗到达后仍完成当前处理单元（select 停旗
                    // 不打断已开始的臂体），之后才 drop Manager（此处以
                    // 置位代替 store 收口）。
                    started_tx.send(()).expect("started");
                    let _ = shutdown_rx.changed().await;
                    let _ = release_rx.await;
                    settled_task.set(true);
                });
                service.pipelines.lock().await.insert(
                    "+15550009999".to_owned(),
                    PipelineHandle {
                        shutdown: shutdown_tx,
                        done,
                    },
                );

                let delete_service = Arc::clone(&service);
                let delete = tokio::task::spawn_local(async move {
                    delete_service.delete_account("+15550009999").await
                });
                started_rx.await.expect("pipeline running");
                tokio::time::sleep(Duration::from_millis(30)).await;
                assert!(
                    !delete.is_finished(),
                    "删除必须等在飞处理单元结算，不得提前返回"
                );
                assert!(store_dir.exists(), "结算前 store 目录不得删除");

                release_tx.send(()).expect("release");
                let removed = delete.await.expect("delete task").expect("delete ok");
                assert!(removed);
                assert!(settled.get(), "在飞处理单元已结算后才收口");
                assert!(!store_dir.exists(), "收口后目录删除");
                let _ = std::fs::remove_dir_all(&data_dir);
            })
            .await;
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
            "dataMessage projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope.get("ephemeral").is_none());
        assert!(envelope.get("urgent").is_none());
        assert!(envelope.get("reportingToken").is_none());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// P15c 项5 信封 serverTimestamp 投影（compat §3 信封公共位）：vendored
    /// Metadata.server_timestamp 恒投（wire 缺席 = proto 缺省 0，官方 ??
    /// 0n 同口径）；dataMessage 与 editMessage 两体面同信封（编辑 48h 窗
    /// isTooOldToEditMessage 消费面由此齐备），client/server 两时间戳不串。
    #[tokio::test]
    async fn receive_envelope_projects_server_timestamp() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-envelope-servertimestamp-{:016x}",
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
            server_timestamp: chrono::DateTime::from_timestamp_millis(1_700_000_004_321)
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

        // dataMessage 面：serverTimestamp 恒投，与 client timestamp 各归各键。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage::default()),
            metadata.clone(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "dataMessage projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["serverTimestamp"], json!(1_700_000_004_321_u64));
        assert_eq!(envelope["timestamp"], json!(1_700_000_000_000_u64));

        // editMessage 面（P15c 项1 编辑消费面）：同一信封位随行。
        let content = Content::from_body(
            ContentBody::EditMessage(presage::proto::EditMessage {
                target_sent_timestamp: Some(1_699_000_000_000),
                data_message: Some(DataMessage::default()),
            }),
            metadata,
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "editMessage projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["serverTimestamp"], json!(1_700_000_004_321_u64));

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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None)
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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

    /// S4 story 反应接收面三腿（官方 MessageReceiver:1534-1544 门序镜像，
    /// 接收投影零 wire 改动——reaction/storyReply 既有键共在即 story 锚定
    /// 形状）：①1:1 story reaction（信封无 story 位）走普通路径——reaction
    /// 与 storyReply 共在（storyReply 键在场 = 宿主区分 story 反应与普通
    /// 反应的判别键），isStory 键不在；②信封 story 位 + reaction+storyContext
    /// 无 groupV2 = 官方整信封丢弃（story 位 1:1 reaction 形状非法）；③信封
    /// story 位 + reaction+storyContext+groupV2 = 群 story 回复豁免照投
    /// （isStory + reaction + storyReply + groupInfo 共在）。
    #[tokio::test]
    async fn receive_story_reaction_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{data_message, GroupContextV2};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-reaction-{:016x}",
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
        let story_reaction = |group: Option<GroupContextV2>| DataMessage {
            reaction: Some(presage::proto::data_message::Reaction {
                emoji: Some("👍".to_owned()),
                target_author_aci: Some("11111111-2222-4333-8444-555555555555".to_owned()),
                target_sent_timestamp: Some(1_700_000_000_000),
                remove: Some(false),
                ..Default::default()
            }),
            story_context: Some(data_message::StoryContext {
                author_aci: Some("11111111-2222-4333-8444-555555555555".to_owned()),
                sent_timestamp: Some(1_700_000_000_000),
                author_aci_binary: None,
            }),
            group_v2: group,
            ..Default::default()
        };

        // ①1:1 story reaction：信封无 story 位，普通路径照投。
        let content = Content::from_body(
            ContentBody::DataMessage(story_reaction(None)),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "1:1 story reaction projects",
        )["params"]["envelope"]
            .clone();
        assert!(envelope.get("isStory").is_none());
        assert_eq!(
            envelope["dataMessage"]["reaction"],
            json!({
                "emoji": "👍",
                "targetAuthor": "11111111-2222-4333-8444-555555555555",
                "targetSentTimestamp": 1_700_000_000_000_u64,
                "isRemove": false
            })
        );
        assert_eq!(
            envelope["dataMessage"]["storyReply"],
            json!({
                "authorAci": "11111111-2222-4333-8444-555555555555",
                "sentTimestamp": 1_700_000_000_000_u64
            })
        );

        // ②信封 story 位 + 1:1 reaction 形状（无 groupV2）：官方整信封丢弃。
        let content = Content::from_body(
            ContentBody::DataMessage(story_reaction(None)),
            metadata(Some(true)),
        );
        assert!(
            project_content("acct", &store, &content, &registry, None)
                .await
                .is_empty(),
            "story 位 1:1 reaction 形状丢弃"
        );

        // ③信封 story 位 + reaction+storyContext+groupV2：群 story 反应豁免
        // 照投（isStory + reaction + storyReply + groupInfo 共在）。
        let content = Content::from_body(
            ContentBody::DataMessage(story_reaction(Some(GroupContextV2 {
                master_key: Some(vec![0x33u8; 32]),
                revision: None,
                group_change: None,
            }))),
            metadata(Some(true)),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "group story reaction projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(envelope["isStory"], json!(true));
        assert_eq!(envelope["dataMessage"]["reaction"]["emoji"], json!("👍"));
        assert_eq!(
            envelope["dataMessage"]["storyReply"]["sentTimestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert!(
            envelope["dataMessage"]["groupInfo"]["groupId"].is_string(),
            "群 story 反应另带既有 groupInfo 投影"
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// storyContext 接收投影 wire additive 钉形批（2026-10-15，接收侧零
    /// wire 改动——storyReply 键形与四腿挂点已在 E2 批落地，本批补钉形与
    /// 记档，compat §5 键名裁定块）：①1:1 腿 binary 优先裁定（官方
    /// isProtoBinaryEncodingEnabled 双写面 binary 权威——binary 与 string
    /// 同在且不一致时 binary 赢，mentions/quote/reaction 先例同款）；
    /// ②缺席同形（story_context 缺席 = pre-批帧逐字节同形：普通正文行与
    /// 普通 reaction 行 storyReply 键都不在，旧宿主零变化）；③sent sync
    /// 腿（多设备自发 story 回复/反应 echo 平铺面 storyReply 在场、
    /// reaction+storyReply 共在——storyReply 投影单点四腿覆盖的最后一腿
    /// 钉形）。
    #[tokio::test]
    async fn receive_story_reply_wire_additive_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{data_message, GroupContextV2};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-additive-{:016x}",
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
        let binary_author = presage::libsignal_service::prelude::Uuid::parse_str(
            "9a1f2b3c-4d5e-4f60-8a71-9b2c3d4e5f60",
        )
        .expect("binary author uuid");

        // ①1:1 腿 binary 优先裁定：binary（合法 UUID A）与 string（不同值
        // B）同在 → authorAci = A（binary 权威；string 是旧客户端兼容面）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("story reply".to_owned()),
                story_context: Some(data_message::StoryContext {
                    author_aci: Some("22222222-3333-4444-8555-666666666666".to_owned()),
                    sent_timestamp: Some(1_700_000_000_500),
                    author_aci_binary: Some(binary_author.as_bytes().to_vec()),
                }),
                ..Default::default()
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "binary-first story reply projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["dataMessage"]["storyReply"],
            json!({
                "authorAci": binary_author.to_string(),
                "sentTimestamp": 1_700_000_000_500_u64
            }),
            "binary 与 string 不一致时 binary 优先"
        );

        // ②缺席同形（pre-批帧逐字节同形）：story_context 缺席的普通正文行
        // 与普通 reaction 行 storyReply 键都不在——投影输出与 E2 批之前
        // 完全同形（旧宿主零变化）。
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                body: Some("plain".to_owned()),
                ..Default::default()
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "plain row projects",
        )["params"]["envelope"]
            .clone();
        assert!(
            envelope["dataMessage"].get("storyReply").is_none(),
            "无 storyContext = storyReply 键不在（缺席同形）"
        );
        let content = Content::from_body(
            ContentBody::DataMessage(DataMessage {
                reaction: Some(presage::proto::data_message::Reaction {
                    emoji: Some("👍".to_owned()),
                    target_author_aci: Some("22222222-3333-4444-8555-666666666666".to_owned()),
                    target_sent_timestamp: Some(1_700_000_000_000),
                    remove: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            metadata(None),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "plain reaction projects",
        )["params"]["envelope"]
            .clone();
        assert!(
            envelope["dataMessage"]["reaction"].is_object(),
            "反应面照投"
        );
        assert!(
            envelope["dataMessage"].get("storyReply").is_none(),
            "无 storyContext 的普通反应 = storyReply 键不在（缺席同形）"
        );

        // ③sent sync 腿：多设备自发 story 回复 echo（body+storyContext）与
        // 自发 story 反应 echo（reaction+storyContext）——平铺面
        // sentMessage.storyReply 在场，反应 echo reaction+storyReply 共在
        // （自发腿信封无 story 位，官方 1:1 story 同位）。
        let sent_content = |message: DataMessage| {
            Content::from_body(
                ContentBody::SynchronizeMessage(presage::proto::SyncMessage {
                    content: Some(presage::proto::sync_message::Content::Sent(
                        presage::proto::sync_message::Sent {
                            timestamp: Some(1_700_000_000_500),
                            message: Some(message),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                metadata(None),
            )
        };
        let content = sent_content(DataMessage {
            body: Some("story reply".to_owned()),
            story_context: Some(data_message::StoryContext {
                author_aci: None,
                sent_timestamp: Some(1_700_000_000_400),
                author_aci_binary: Some(binary_author.as_bytes().to_vec()),
            }),
            ..Default::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "sent story reply projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyReply"],
            json!({
                "authorAci": binary_author.to_string(),
                "sentTimestamp": 1_700_000_000_400_u64
            }),
            "sent sync 腿 storyReply 平铺照投"
        );
        let content = sent_content(DataMessage {
            reaction: Some(presage::proto::data_message::Reaction {
                emoji: Some("❤️".to_owned()),
                target_author_aci: Some(binary_author.to_string()),
                target_sent_timestamp: Some(1_700_000_000_400),
                remove: Some(false),
                ..Default::default()
            }),
            story_context: Some(data_message::StoryContext {
                author_aci: None,
                sent_timestamp: Some(1_700_000_000_400),
                author_aci_binary: Some(binary_author.as_bytes().to_vec()),
            }),
            group_v2: Some(GroupContextV2 {
                master_key: Some(vec![0x44u8; 32]),
                revision: None,
                group_change: None,
            }),
            ..Default::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "sent story reaction projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["reaction"]["emoji"],
            json!("❤️")
        );
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyReply"]["sentTimestamp"],
            json!(1_700_000_000_400_u64),
            "sent sync 腿 reaction+storyReply 共在"
        );

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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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

    /// sendCallEvent 直聊腿 wire 逐字节（通话事件出向同步批）：官方
    /// getProtoForCallHistory 构造同位——CallEvent 六字段按号序
    /// conversationId(1,tag 0x0a)/callId(2,0x10)/timestamp(3,0x18)/type(4,
    /// 0x20)/direction(5,0x28)/event(6,0x30)；callId 用 2^53+1 钉 u64 全域
    ///（JS 安全区外——wire 十进制串的意义所在）；type=AUDIO_CALL(1)/
    /// direction=INCOMING(1)/event=ACCEPTED(1)。
    #[test]
    fn call_event_direct_builds_six_field_wire() {
        use presage::proto::sync_message::call_event::{Direction, Event, Type};
        use prost::Message as _;

        let conversation: Vec<u8> = (1u8..=16).collect();
        let event = SendCallEvent {
            kind: Type::AudioCall,
            direction: Direction::Incoming,
            outcome: Event::Accepted,
            conversation: CallEventConversation::Direct("+15551234567".into()),
            call_id: Some(9007199254740993),
            timestamp: Some(1700000000000),
        };
        let proto = build_call_event(&event, conversation.clone());
        let mut bytes = Vec::new();
        proto.encode(&mut bytes).expect("proto encodes");
        assert_eq!(
            bytes,
            [
                0x0a, 0x10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                16, // field1 16B
                0x10, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10, // field2 callId
                0x18, 0x80, 0xd0, 0x95, 0xff, 0xbc, 0x31, // field3 timestamp
                0x20, 0x01, // field4 type AUDIO_CALL
                0x28, 0x01, // field5 direction INCOMING
                0x30, 0x01, // field6 event ACCEPTED
            ],
            "直聊腿 wire = 六字段号序"
        );
        // 解码回读：值域逐面一致（u64 不经 f64 有损面）。
        let decoded =
            presage::proto::sync_message::CallEvent::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(
            decoded.conversation_id.as_deref(),
            Some(conversation.as_slice())
        );
        assert_eq!(decoded.call_id, Some(9007199254740993));
        assert_eq!(decoded.timestamp, Some(1700000000000));
        assert_eq!(decoded.r#type, Some(Type::AudioCall as i32));
        assert_eq!(decoded.direction, Some(Direction::Incoming as i32));
        assert_eq!(decoded.event, Some(Event::Accepted as i32));
    }

    /// type 词表三面映射（官方 typeToProto/statusToProto/directionToProto
    /// 同位）：audio/video/group/ad-hoc → 1/2/3/4；accepted/not-accepted/
    /// delete/observed → 1/2/3/4；incoming/outgoing → 1/2。
    #[test]
    fn call_event_vocabulary_maps_to_proto_enums() {
        use presage::proto::sync_message::call_event::{Direction, Event, Type};
        let params = |kind: &str| {
            let conversation_id = if kind == "ad-hoc" {
                "0123456789abcdef" // ad-hoc 腿 hex 占位
            } else {
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" // 32B base64 占位
            };
            json!({
                "type": kind,
                "direction": "outgoing",
                "event": "not-accepted",
                "conversationId": conversation_id,
                "callId": "42",
            })
        };
        let parse = |value: Value| parse_call_event(value.as_object().expect("object"));
        for (word, value) in [
            ("audio", Type::AudioCall),
            ("video", Type::VideoCall),
            ("group", Type::GroupCall),
            ("ad-hoc", Type::AdHocCall),
        ] {
            let parsed = parse(params(word)).expect("parses");
            assert_eq!(parsed.kind, value, "type 词 {word} → {value:?}");
        }
        for (word, value) in [
            ("accepted", Event::Accepted),
            ("not-accepted", Event::NotAccepted),
            ("delete", Event::Delete),
            ("observed", Event::Observed),
        ] {
            let mut params = params("group");
            params["event"] = json!(word);
            let parsed = parse(params).expect("parses");
            assert_eq!(parsed.outcome, value, "event 词 {word} → {value:?}");
        }
        for (word, value) in [
            ("incoming", Direction::Incoming),
            ("outgoing", Direction::Outgoing),
        ] {
            let mut params = params("audio");
            params["direction"] = json!(word);
            let parsed = parse(params).expect("parses");
            assert_eq!(parsed.direction, value, "direction 词 {word} → {value:?}");
        }
        // UNKNOWN 词不作发送意图（官方 statusToProto 中间态 null 不出线同位）。
        for bad in ["unknown", "AUDIO", "", "missed"] {
            let mut params = params("audio");
            params["type"] = json!(bad);
            assert!(parse(params).is_none(), "type 词 {bad} 拒绝");
        }
    }

    /// ad-hoc 腿：roomId 小写 hex 字节直写（官方 getBytesForPeerId fromHex
    /// 同位），callId 缺席放行（官方 call-link 占位「only send the
    /// timestamp」同臂）；群腿 32B master key base64、callId 必带。
    #[test]
    fn call_event_adhoc_and_group_conversation_legs() {
        use presage::proto::sync_message::call_event::Type;
        let parse = |value: Value| parse_call_event(value.as_object().expect("object"));
        let room_hex = "0123456789abcdef0123456789abcdef";
        let adhoc = parse(json!({
            "type": "ad-hoc",
            "direction": "incoming",
            "event": "observed",
            "conversationId": room_hex,
        }))
        .expect("adhoc parses without callId");
        assert_eq!(adhoc.call_id, None);
        let room_bytes = hex_decode(room_hex).expect("hex");
        match &adhoc.conversation {
            CallEventConversation::AdHoc(bytes) => {
                assert_eq!(bytes, &room_bytes);
            }
            _ => panic!("ad-hoc leg expected"),
        }
        let proto = build_call_event(&adhoc, room_bytes);
        assert_eq!(proto.call_id, None, "ad-hoc 无 callId 位");
        assert_eq!(proto.r#type, Some(Type::AdHocCall as i32));

        let master_key: Vec<u8> = (0u8..32).collect();
        let group = parse(json!({
            "type": "group",
            "direction": "outgoing",
            "event": "accepted",
            "conversationId": base64::engine::general_purpose::STANDARD.encode(&master_key),
            "callId": "42",
        }))
        .expect("group parses");
        match group.conversation {
            CallEventConversation::Group(bytes) => assert_eq!(bytes, master_key),
            _ => panic!("group leg expected"),
        }
        // 群腿坏 base64/≠32B 全拒（合法 32B 已由上方 group 正例覆盖）。
        let short_key = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
        for conversation_id in ["not-base64!".to_string(), short_key] {
            let params = json!({
                "type": "group", "direction": "outgoing", "event": "accepted",
                "conversationId": conversation_id, "callId": "42",
            });
            assert!(
                parse_call_event(params.as_object().expect("object")).is_none(),
                "群腿形状 {conversation_id} 拒绝"
            );
        }
        let group_missing_call_id = json!({
            "type": "group", "direction": "outgoing", "event": "accepted",
            "conversationId": base64::engine::general_purpose::STANDARD.encode(&master_key),
        });
        assert!(
            parse_call_event(group_missing_call_id.as_object().expect("object")).is_none(),
            "群腿 callId 必带"
        );
    }

    /// 参数闸 fail-closed 全臂：callId 十进制 u64 域（负号/小数/hex/超
    /// u64/数字类型全拒）、直聊腿 callId 必带、timestamp 正整数、
    /// conversationId 空/缺、必填词表键缺席。
    #[test]
    fn call_event_param_gates_fail_closed() {
        let base = json!({
            "type": "audio",
            "direction": "incoming",
            "event": "accepted",
            "conversationId": "+15551234567",
            "callId": "42",
        });
        let parse = |value: Value| parse_call_event(value.as_object().expect("object"));
        assert!(parse(base.clone()).is_some(), "基线形状通过");

        for bad_call_id in [
            "-1",
            "1.0",
            "0x10",
            "abc",
            "",
            "18446744073709551616",
            " 42",
            "+42",
        ] {
            let mut params = base.clone();
            params["callId"] = json!(bad_call_id);
            assert!(parse(params).is_none(), "callId {bad_call_id:?} 拒绝");
        }
        let mut numeric_call_id = base.clone();
        numeric_call_id["callId"] = json!(42);
        assert!(
            parse(numeric_call_id).is_none(),
            "callId 数字类型拒绝（wire 恒十进制串，官方 longToString 同构）"
        );

        let mut missing_call_id = base.clone();
        missing_call_id.as_object_mut().unwrap().remove("callId");
        assert!(parse(missing_call_id).is_none(), "直聊腿 callId 必带");

        for bad_ts in [json!(0), json!(-1), json!(1.5)] {
            let mut params = base.clone();
            params["timestamp"] = bad_ts.clone();
            assert!(parse(params).is_none(), "timestamp {bad_ts} 拒绝");
        }

        for empty in ["", "   "] {
            let mut params = base.clone();
            params["conversationId"] = json!(empty);
            assert!(parse(params).is_none(), "conversationId {empty:?} 拒绝");
        }
        let odd_hex = json!({
            "type": "ad-hoc", "direction": "incoming", "event": "delete",
            "conversationId": "abc", // 奇数长 hex
        });
        assert!(parse(odd_hex).is_none(), "ad-hoc 奇数长 hex 拒绝");
        let upper_hex = json!({
            "type": "ad-hoc", "direction": "incoming", "event": "delete",
            "conversationId": "ABCDEF", // 大写 hex（hex_decode 小写纪律）
        });
        assert!(parse(upper_hex).is_none(), "ad-hoc 大写 hex 拒绝");

        for key in ["type", "direction", "event", "conversationId"] {
            let mut params = base.clone();
            params.as_object_mut().unwrap().remove(key);
            assert!(parse(params).is_none(), "必填键 {key} 缺席拒绝");
        }
    }

    /// 出向构造 ↔ 入向投影互逆（同批词表契约）：构造 → 编码 → 解码 →
    /// project_call_event → JSON 词与入参逐面同形（type/direction/event/
    /// callId 十进制串/conversationId 直聊 UUID 串）。
    #[test]
    fn call_event_outbound_inbound_round_trip() {
        use presage::proto::sync_message::call_event::{Direction, Event, Type};
        use prost::Message as _;

        let aci = "6e56f7d5-8ab1-4a0e-b34e-2e3a5c9f0d17";
        let bytes = aci_to_bytes(aci).expect("valid uuid");
        let event = SendCallEvent {
            kind: Type::VideoCall,
            direction: Direction::Outgoing,
            outcome: Event::NotAccepted,
            conversation: CallEventConversation::Direct(aci.to_owned()),
            call_id: Some(1234567890123),
            timestamp: Some(1234567890123),
        };
        let proto = build_call_event(&event, bytes);
        let mut encoded = Vec::new();
        proto.encode(&mut encoded).expect("encodes");
        let decoded =
            presage::proto::sync_message::CallEvent::decode(encoded.as_slice()).expect("decodes");
        let projected = project_call_event(&decoded).expect("projects");
        assert_eq!(projected["type"], json!("video"));
        assert_eq!(projected["direction"], json!("outgoing"));
        assert_eq!(projected["event"], json!("not-accepted"));
        assert_eq!(projected["callId"], json!("1234567890123"));
        assert_eq!(projected["timestamp"], json!(1234567890123u64));
        assert_eq!(projected["conversationId"], json!(aci));
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
        assert_eq!(notifications.len(), 1, "flagged: single event");
        let envelope = &notifications[0]["params"]["envelope"];
        assert_eq!(envelope["contactProfileUpdated"]["aci"], json!(sender_aci));
        assert!(envelope.get("dataMessage").is_none());

        // 同 key 的 flag 行（换回旧值触发事件；再收一次 = 幂等：无事件且
        // 更新行本身无可投影正文 → 零通知，官方 hasChanged 门 no-op 同果）。
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
        let notifications = project_content("acct", &store, &content, &registry, None).await;
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
        )
        .await;
        assert_eq!(notifications.len(), 1, "viewed: single 投影");
        let face = &notifications[0]["params"]["envelope"]["syncMessage"];
        assert!(face.get("read").is_none(), "空 read 键缺席");
        let viewed = face["viewed"].as_array().expect("viewed array");
        assert_eq!(viewed.len(), 1, "坏条目丢");
        assert_eq!(viewed[0]["timestamp"], json!(333));

        // 全空：整信封零投影。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(Vec::new(), Vec::new()),
            &registry,
            None,
        )
        .await;
        assert!(notifications.is_empty(), "空 read/viewed 不投影");

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// stickerPackSync 收投影臂（接收完整性批③，compat §3 additive）：
    /// `stickerPackOperation` 非空 sync 信封独占早返（不落 read/viewed 兜底
    /// 臂），词条 {id: packId 小写 hex, key: packKey 标准 base64,
    /// isInstall, isRemove}（源位缺席 → 该键省略）；type 裸位判定
    /// （None = proto 缺省 INSTALL 同官方、REMOVE=1、未知值整条跳过宁缺勿
    /// 错，全部未知 → 整信封零投影）；服务面缺席（None 注入）只投影零写面。
    #[tokio::test]
    async fn sticker_pack_sync_projection_exclusive_arm_shapes() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::{sync_message, SyncMessage};

        let store_dir =
            std::env::temp_dir().join(format!("kt-engine-sps-{:016x}", mix_seed_step(mix_seed())));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let sync_of = |operations: Vec<sync_message::StickerPackOperation>| {
            Content::from_body(
                ContentBody::SynchronizeMessage(SyncMessage {
                    sticker_pack_operation: operations,
                    ..SyncMessage::with_padding(&mut rng())
                }),
                envelope_metadata(),
            )
        };
        let operation =
            |pack_id: Option<Vec<u8>>, pack_key: Option<Vec<u8>>, r#type: Option<i32>| {
                sync_message::StickerPackOperation {
                    pack_id,
                    pack_key,
                    r#type,
                }
            };

        // install（None = proto 缺省）+ remove：词条同官方事件面（id 小写
        // hex、key base64、isInstall/isRemove 布尔），独占臂单通知。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(vec![
                operation(Some(vec![0x0a; 16]), Some(vec![0x0b; 32]), None),
                operation(Some(vec![0x0c; 16]), Some(vec![0x0d; 32]), Some(1)),
            ]),
            &registry,
            None,
        )
        .await;
        assert_eq!(notifications.len(), 1, "独占臂单通知");
        let envelope = &notifications[0]["params"]["envelope"];
        let entries = envelope["syncMessage"]["stickerPackSync"]
            .as_array()
            .expect("stickerPackSync array");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["id"], json!(hex_encode(&[0x0a_u8; 16])));
        assert_eq!(
            entries[0]["key"],
            json!(base64::engine::general_purpose::STANDARD.encode([0x0b_u8; 32]))
        );
        assert_eq!(entries[0]["isInstall"], json!(true));
        assert_eq!(entries[0]["isRemove"], json!(false));
        assert_eq!(entries[1]["id"], json!(hex_encode(&[0x0c_u8; 16])));
        assert_eq!(entries[1]["isInstall"], json!(false));
        assert_eq!(entries[1]["isRemove"], json!(true));

        // 显式 INSTALL=0 同缺省形状（官方 INSTALL=0 值面）。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(vec![operation(
                Some(vec![0x0a; 16]),
                Some(vec![0x0b; 32]),
                Some(0),
            )]),
            &registry,
            None,
        )
        .await;
        assert_eq!(
            notifications[0]["params"]["envelope"]["syncMessage"]["stickerPackSync"][0]
                ["isInstall"],
            json!(true),
            "显式 INSTALL 与 proto 缺省同形状"
        );

        // 未知 type 值整条跳过（宁缺勿错）；全部未知 → 整信封零投影。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(vec![
                operation(Some(vec![0x0a; 16]), Some(vec![0x0b; 32]), Some(7)),
                operation(Some(vec![0x0c; 16]), Some(vec![0x0d; 32]), Some(1)),
            ]),
            &registry,
            None,
        )
        .await;
        let entries = notifications[0]["params"]["envelope"]["syncMessage"]["stickerPackSync"]
            .as_array()
            .expect("unknown entry skipped");
        assert_eq!(entries.len(), 1, "未知 type 词条跳过");
        assert_eq!(entries[0]["isRemove"], json!(true));

        let notifications = project_content(
            "acct",
            &store,
            &sync_of(vec![operation(
                Some(vec![0x0a; 16]),
                Some(vec![0x0b; 32]),
                Some(9),
            )]),
            &registry,
            None,
        )
        .await;
        assert!(notifications.is_empty(), "全未知词条零投影");

        // 源位缺席 → 键省略（布尔恒在）：packId 缺 = id 键不在；独占臂不
        // 落 read/viewed 兜底（syncMessage 无 read/viewed 键）。
        let notifications = project_content(
            "acct",
            &store,
            &sync_of(vec![operation(None, Some(vec![0x0b; 32]), None)]),
            &registry,
            None,
        )
        .await;
        let entry = &notifications[0]["params"]["envelope"]["syncMessage"]["stickerPackSync"][0];
        assert!(entry.get("id").is_none(), "packId 缺席 id 键不在");
        assert!(entry.get("key").is_some());
        assert_eq!(entry["isInstall"], json!(true));
        assert!(notifications[0]["params"]["envelope"]["syncMessage"]
            .get("read")
            .is_none(),);

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// stickerPackSync 写面联动（接收完整性批③）：Some(service) 注入下投影
    /// 逐字同形（写面 best-effort——未注册账号写面 Err 吞掉不炸投影）；
    /// 短 packKey（<32B）install 只跳写面、事件照投（key 键在）。
    #[tokio::test]
    async fn sticker_pack_sync_projection_with_service_write_is_best_effort() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::{sync_message, SyncMessage};

        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-sps-write-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, _rx) = outbound::test_hub();
        let service = Service::new(data_dir.clone(), Arc::clone(&state), outbound);

        let store_dir =
            std::env::temp_dir().join(format!("kt-engine-sps2-{:016x}", mix_seed_step(mix_seed())));
        let store = open_store(&store_dir).await.unwrap();
        let registry = groups::TerminatedGroups::new();

        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                sticker_pack_operation: vec![
                    sync_message::StickerPackOperation {
                        pack_id: Some(vec![0x11; 16]),
                        pack_key: Some(vec![0x22; 32]),
                        r#type: None,
                    },
                    // 短 packKey install：写面跳过（<32B），事件照投。
                    sync_message::StickerPackOperation {
                        pack_id: Some(vec![0x33; 16]),
                        pack_key: Some(vec![0x44; 16]),
                        r#type: Some(0),
                    },
                    sync_message::StickerPackOperation {
                        pack_id: Some(vec![0x55; 16]),
                        pack_key: None,
                        r#type: Some(1),
                    },
                ],
                ..SyncMessage::with_padding(&mut rng())
            }),
            envelope_metadata(),
        );
        let notifications =
            project_content("acct", &store, &content, &registry, Some(&service)).await;
        assert_eq!(notifications.len(), 1, "写面失败不影响投影");
        let entries = notifications[0]["params"]["envelope"]["syncMessage"]["stickerPackSync"]
            .as_array()
            .expect("entries");
        assert_eq!(entries.len(), 3, "三条词条全投影");
        assert_eq!(entries[0]["isInstall"], json!(true));
        assert_eq!(
            entries[1]["key"].as_str().map(str::len),
            Some(24),
            "16B base64"
        );
        assert_eq!(entries[1]["isInstall"], json!(true), "短 key 只跳写面");
        assert_eq!(entries[2]["isRemove"], json!(true));
        assert!(entries[2].get("key").is_none(), "packKey 缺席 key 键不在");

        let _ = std::fs::remove_dir_all(&data_dir);
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
    /// 缺失即 OUTDATED_DEVICE 拒连；双报对齐官方桌面。vendored 层不报
    /// `backup5`——该声明位由引擎渲染面 `qr_link_url` 统一收敛追加（见下），
    /// vendor 语义保持与上游单一职责。
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

    /// 渲染面 QR URL 收敛钉形：capabilities 必须是**单个**查询参数——
    /// Android 扫码侧 `Uri.getQueryParameter`（单数）只读第一个同名参数，
    /// 双参数形状令 `backup5` 不可见（2026-10-15 真机取证：链接成功但手机
    /// 不弹「转移聊天记录」、不上传迁移档案）。
    #[test]
    fn qr_link_url_collapses_duplicate_capabilities_into_single_param() {
        let base = url::Url::parse(
            "sgnl://linkdevice?uuid=u1&pub_key=PK=&capabilities=nopni,nopni2&capabilities=backup5",
        )
        .expect("static URL parses");
        let url = qr_link_url(base);
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let capability_values: Vec<&str> = pairs
            .iter()
            .filter(|(key, _)| key == "capabilities")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(capability_values.len(), 1, "{pairs:?}");
        assert_eq!(
            capability_values[0].split(',').collect::<Vec<_>>(),
            vec!["nopni", "nopni2", "backup5"],
            "{pairs:?}"
        );
        assert!(pairs.contains(&("uuid".to_owned(), "u1".to_owned())));
        assert!(pairs.contains(&("pub_key".to_owned(), "PK=".to_owned())));
    }

    /// 旧 vendor 形状（无 capabilities 底参）收敛为单参数并补齐 `backup5`
    /// （幂等安全网：上游 link_uri 语义变化时渲染面仍保证单参数声明）。
    #[test]
    fn qr_link_url_appends_backup5_when_capabilities_absent() {
        let base =
            url::Url::parse("sgnl://linkdevice?uuid=u1&pub_key=PK=").expect("static URL parses");
        let url = qr_link_url(base);
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        assert_eq!(
            pairs,
            vec![
                ("uuid".to_owned(), "u1".to_owned()),
                ("pub_key".to_owned(), "PK=".to_owned()),
                ("capabilities".to_owned(), "backup5".to_owned()),
            ]
        );
    }

    /// 幂等（定点）：`backup5` 已在位时应用两次与应用一次结果一致
    /// （重试/重复 startLink 面安全；不与字面输入比串——append_pair 的
    /// form-urlencoded 归一化对未触及参数对同样生效，语义透明）。
    #[test]
    fn qr_link_url_is_idempotent() {
        let base = url::Url::parse(
            "sgnl://linkdevice?uuid=u1&pub_key=PK=&capabilities=nopni,nopni2,backup5",
        )
        .expect("static URL parses");
        let once = qr_link_url(base);
        let twice = qr_link_url(once.clone());
        assert_eq!(twice, once);
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

    /// 群取回失败两事件形状面（接收完整性批②，compat §3 additive）：解码
    /// 类 → receive 族 envelope 级 `groupSyncFailed`（{groupId, count}，
    /// count = vendored 重试环尝试次数，无身份/群名面）；404 群消失 → 复
    /// 用 groupTerminated 事件（terminator=null——非终止 change 无编辑者
    /// 面）且 envelope 顶层无 dataMessage 等其余投影键。
    #[test]
    fn group_sync_failure_notification_shapes_are_additive_receive_face() {
        let group_id = "dGVzdC1ncm91cC1tYXN0ZXIta2V5LTIwMjYtc2lnbmFs";
        let failed = group_sync_failed_notification("acct", group_id, 2);
        assert_eq!(failed["jsonrpc"], json!("2.0"));
        assert_eq!(failed["method"], json!("receive"));
        assert_eq!(failed["params"]["account"], json!("acct"));
        let envelope = &failed["params"]["envelope"];
        assert!(
            envelope["timestamp"].as_u64().is_some(),
            "本地生成时间戳恒在"
        );
        assert_eq!(envelope["groupSyncFailed"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupSyncFailed"]["count"], json!(2));
        assert!(
            envelope.get("dataMessage").is_none(),
            "整信封即事件，无其余投影键"
        );

        let gone = group_gone_notification("acct", group_id);
        assert_eq!(gone["jsonrpc"], json!("2.0"));
        assert_eq!(gone["method"], json!("receive"));
        assert_eq!(gone["params"]["account"], json!("acct"));
        let envelope = &gone["params"]["envelope"];
        assert!(envelope["timestamp"].as_u64().is_some());
        assert_eq!(envelope["groupTerminated"]["groupId"], json!(group_id));
        assert_eq!(envelope["groupTerminated"]["terminator"], Value::Null);
        assert_eq!(
            envelope["groupTerminated"]["timestamp"], envelope["timestamp"],
            "事件与信封同源时间戳"
        );
    }

    /// P15b 项8 时钟偏移闸判定：阈内 None；越阈 Some(skew)（符号 = 本地 −
    /// 服务器）；阈值 = 官方 ClockSkewDetector 默认 24h。
    #[test]
    fn clock_skew_threshold_matches_official_24h_default() {
        let day: i64 = 24 * 60 * 60 * 1_000;
        assert_eq!(CLOCK_SKEW_THRESHOLD_MS, day);
        assert_eq!(clock_skew_over_threshold(1_000_000, 1_000_000), None);
        assert_eq!(
            clock_skew_over_threshold(1_000_000, 1_000_000 + day),
            None,
            "恰在阈值不告警"
        );
        assert_eq!(
            clock_skew_over_threshold(1_000_000, 1_000_000 + day + 1),
            Some(day + 1),
            "本地钟快超阈，skew 为正"
        );
        assert_eq!(
            clock_skew_over_threshold(1_000_000 + day + 5_000, 1_000_000),
            Some(-(day + 5_000)),
            "本地钟慢超阈，skew 为负"
        );
    }

    /// P15b 项8 偏移状态机（边缘触发）：首越阈 Alert → 持续 Sustained →
    /// 回阈内 Rearm → 再越阈重新 Alert。
    #[test]
    fn skew_transition_edge_triggers_once_per_episode() {
        let day: i64 = 24 * 60 * 60 * 1_000;
        let t0 = 1_000_000_i64;
        // 未告警 + 阈内 = Rearm（幂等无害）。
        assert!(matches!(
            skew_transition(false, t0, t0),
            SkewTransition::Rearm
        ));
        // 首越阈 = Alert（带符号 skew）。
        assert!(matches!(
            skew_transition(false, t0, t0 + day + 60_000),
            SkewTransition::Alert(s) if s == day + 60_000
        ));
        // 已告警 + 仍越阈 = Sustained（不重复告警）。
        assert!(matches!(
            skew_transition(true, t0, t0 + day + 120_000),
            SkewTransition::Sustained
        ));
        // 已告警 + 回阈内 = Rearm。
        assert!(matches!(
            skew_transition(true, t0, t0),
            SkewTransition::Rearm
        ));
        // 重臂后再越阈 = 重新 Alert。
        assert!(matches!(
            skew_transition(false, t0, t0 - day - 1),
            SkewTransition::Alert(s) if s == -(day + 1)
        ));
    }

    /// P15b 项8 偏移告警通知形状面：`receive.clockskew` 独立方法（非
    /// envelope 载体同 emptied 纪律），params 三键（account/skewMs/
    /// serverTimestamp）。
    #[test]
    fn clock_skew_notification_shape_is_additive_stream_face() {
        let notification = clock_skew_notification("acct", -86_500_000, 1_700_000_000_000);
        assert_eq!(notification["jsonrpc"], json!("2.0"));
        assert_eq!(notification["method"], json!("receive.clockskew"));
        assert_eq!(notification["params"]["account"], json!("acct"));
        assert_eq!(notification["params"]["skewMs"], json!(-86_500_000));
        assert_eq!(
            notification["params"]["serverTimestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert!(notification["params"].get("envelope").is_none());
    }

    /// P15b 项8 时钟偏移闸积压免疫（时序修复批）武装态机：官方锚在 WS 层
    /// 恒新鲜服务器时间，sidecar 等价 = 排空前不喂闸——①新流解除武装后
    /// observe 单步 no-op（不上抛、不变 alerted、不清既有告警态）；②排空
    /// 标记（QueueEmpty→arm）后旧时间戳（入队时刻的积压信封）首越阈恰一
    /// 次 Alert，持续越阈 Sustained 静默；③新流再 disarm 后旧时间戳仍
    /// no-op 且 alerted 跨流保持；④重新武装后新鲜时间戳照常 Rearm、再越
    /// 阈重新 Alert（边缘触发纪律不变）。
    #[test]
    fn clock_skew_gate_backlog_immunity_armed_state_machine() {
        let day: i64 = 24 * 60 * 60 * 1_000;
        let t0 = 1_000_000_i64;
        let stale = t0 - day - 60_000;

        // ① 未武装（新流排水未完成）：no-op，不上抛不改态。
        let mut gate = ClockSkewGate {
            alerted: false,
            armed: false,
        };
        assert!(gate.step(stale, t0).is_none(), "disarmed 不产生转移");
        assert!(!gate.alerted, "disarmed 不变更 alerted");
        assert!(!gate.armed);

        // 排空标记到达（QueueEmpty 臂）→ arm；积压后的旧时间戳首越阈 =
        // Alert（恰一次），持续越阈 = Sustained。
        let mut gate = ClockSkewGate {
            alerted: false,
            armed: false,
        };
        gate.arm();
        assert!(matches!(
            gate.step(stale, t0),
            Some(SkewTransition::Alert(s)) if s == t0 - stale
        ));
        assert!(gate.alerted);
        assert!(matches!(
            gate.step(stale, t0),
            Some(SkewTransition::Sustained)
        ));
        assert!(gate.alerted, "Sustained 不重复告警不清态");

        // ③ 新流入口 disarm：旧时间戳重新 no-op，alerted 保持（跨重连存
        // 活），不清态不重臂。
        gate.disarm();
        assert!(gate.step(stale, t0).is_none());
        assert!(gate.alerted, "disarmed observe 不变更 alerted");

        // ④ 重新武装 + 新鲜时间戳（阈内）→ Rearm 清告警态；再越阈照旧重
        // 新 Alert（边缘触发纪律与修复前一致）。
        gate.arm();
        assert!(matches!(gate.step(t0, t0), Some(SkewTransition::Rearm)));
        assert!(!gate.alerted);
        assert!(matches!(
            gate.step(stale, t0),
            Some(SkewTransition::Alert(_))
        ));
        assert!(gate.alerted);
    }

    /// P15b 项8 偏移闸 observe 边界（真 Service 出口钉形）：积压免疫下
    /// disarmed observe 零通知上抛；armed 后旧时间戳恰一条
    /// `receive.clockskew`（skewMs 带符号 = 本地 − 服务器；积压信封入队
    /// 时刻在过去 → 正值，即「本机钟快」假告警形状——修复前必现）。
    #[tokio::test]
    async fn clock_skew_gate_disarmed_observe_emits_no_notice() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-skew-gate-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, mut rx) = outbound::test_hub();
        let service = Service::new(data_dir, Arc::clone(&state), outbound);

        let now = now_ms() as i64;
        let stale = now - 25 * 60 * 60 * 1_000; // 数天前入队的积压信封
        let clockskew_notices = |rx: &mut tokio::sync::mpsc::Receiver<crate::OutboundFrame>| {
            let mut notices = Vec::new();
            while let Ok(crate::OutboundFrame::Notice { value, .. }) = rx.try_recv() {
                if value["method"] == json!("receive.clockskew") {
                    notices.push(value);
                }
            }
            notices
        };

        // ① disarmed（新流排水未完成）：喂积压旧时间戳零上抛。
        let mut gate = ClockSkewGate {
            alerted: false,
            armed: false,
        };
        gate.observe(&service, "acct", stale).await;
        assert!(
            clockskew_notices(&mut rx).is_empty(),
            "disarmed observe 不上抛"
        );

        // ② QueueEmpty 排空标记后（arm）：同一旧时间戳恰一条告警。
        gate.arm();
        gate.observe(&service, "acct", stale).await;
        let notices = clockskew_notices(&mut rx);
        assert_eq!(notices.len(), 1, "armed 首越阈恰一条 receive.clockskew");
        assert_eq!(notices[0]["params"]["account"], json!("acct"));
        assert!(
            notices[0]["params"]["skewMs"].as_i64().unwrap() > 24 * 60 * 60 * 1_000,
            "skew = 本地 − 服务器，积压信封入队时刻在过去 → 本地「快」为正（假告警形状）"
        );
        // 持续越阈静默：同态再喂零新通知。
        gate.observe(&service, "acct", stale).await;
        assert!(clockskew_notices(&mut rx).is_empty());

        // ③ 新流 disarm 后喂旧时间戳：零上抛且 alerted 保持；重新武装 +
        // 新鲜时间戳 Rearm 后再越阈照旧重新上抛（边缘触发不变）。
        gate.disarm();
        gate.observe(&service, "acct", stale).await;
        assert!(clockskew_notices(&mut rx).is_empty());
        gate.arm();
        gate.observe(&service, "acct", now).await;
        assert!(clockskew_notices(&mut rx).is_empty(), "回阈内 Rearm 无告警");
        gate.observe(&service, "acct", stale).await;
        assert_eq!(clockskew_notices(&mut rx).len(), 1, "重臂后再越阈重新告警");
    }

    /// F 链路引擎腿接线（storage_watch × Service 出口）：入站观测批过
    /// per-account 簿——首批种子静默（零帧，启动风暴闸）、真实翻转合成
    /// 恰一条 `receive.conversationState` 通知帧（形状钉定）、值未变零帧；
    /// 本机已知态登记（写后/读出面同入口）防回声——登记后观测同态零帧、
    /// 观测异态（他设备真改）照常通知；账号删除摘簿（删后重观测 = 种子
    /// 静默而非翻转）。
    #[tokio::test]
    async fn conversation_state_watch_publish_and_echo_suppression() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-conv-watch-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, mut rx) = outbound::test_hub();
        let service = Service::new(data_dir, Arc::clone(&state), outbound);

        let contact = storage_sync::EngineConversationId::Contact(
            "11111111-2222-4333-8444-555555555555".to_owned(),
        );
        let flags = |muted: u64| storage_sync::ConversationState {
            muted_until_timestamp: muted,
            archived: true,
            marked_unread: false,
        };

        // 首批观测 = 种子静默。
        service
            .publish_conversation_state_observations("acct", vec![(contact.clone(), flags(0))])
            .await;
        assert!(rx.try_recv().is_err(), "首观测种子静默");

        // 真实翻转 → 恰一条通知帧（批内合流），形状 = 词表第五词钉定面。
        service
            .publish_conversation_state_observations("acct", vec![(contact.clone(), flags(7))])
            .await;
        let crate::OutboundFrame::Notice { value, .. } = rx.try_recv().expect("flip notifies")
        else {
            panic!("notice frame");
        };
        assert_eq!(value["jsonrpc"], json!("2.0"));
        assert_eq!(value["method"], json!("receive.conversationState"));
        assert_eq!(value["params"]["account"], json!("acct"));
        assert_eq!(value["params"]["overflow"], json!(false));
        let entry = &value["params"]["conversations"][0];
        assert_eq!(
            entry["conversationId"],
            json!("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(entry["kind"], json!("contact"));
        assert_eq!(entry["mutedUntilTimestamp"], json!(7));
        assert_eq!(entry["archived"], json!(true));
        assert_eq!(entry["markedUnread"], json!(false));
        assert!(rx.try_recv().is_err(), "一次 merge 批合成恰一条");

        // 值未变（合并但无翻转）→ 零帧。
        service
            .publish_conversation_state_observations("acct", vec![(contact.clone(), flags(7))])
            .await;
        assert!(rx.try_recv().is_err(), "值未变不发");

        // 本机写/读登记防回声：登记写后态 → 观测同态零帧；观测异态照常。
        service.note_conversation_state_known("acct", contact.clone(), flags(9));
        service
            .publish_conversation_state_observations("acct", vec![(contact.clone(), flags(9))])
            .await;
        assert!(rx.try_recv().is_err(), "本机写不自我通知");
        service
            .publish_conversation_state_observations("acct", vec![(contact.clone(), flags(11))])
            .await;
        assert!(rx.try_recv().is_ok(), "他设备真改照常通知");

        // 空批零帧。
        service
            .publish_conversation_state_observations("acct", Vec::new())
            .await;
        assert!(rx.try_recv().is_err());

        // 账号删除摘簿：删后同会话重观测 = 种子静默（非翻转通知）。
        state
            .lock()
            .await
            .add_account("acct".to_owned(), None)
            .expect("add account");
        service.note_conversation_state_known("acct", contact.clone(), flags(13));
        service.delete_account("acct").await.expect("delete");
        service
            .publish_conversation_state_observations("acct", vec![(contact, flags(16))])
            .await;
        assert!(rx.try_recv().is_err(), "摘簿后重观测 = 种子静默");
    }

    /// T-9 fetchLatest 消费臂触发判别面：仅 STORAGE_MANIFEST 触发；
    /// LOCAL_PROFILE/SUBSCRIPTION_STATUS/UNKNOWN/type 缺席/未知 raw 值一律
    /// 不触发；非 FetchLatest sync 体（sent）与 DataMessage 体不触发。
    /// 信号面投影不受本判别影响（投影臂照旧，零 wire 变化）。
    #[test]
    fn storage_manifest_fetch_trigger_faces() {
        use presage::libsignal_service::content::{Content, ContentBody};
        use presage::proto::sync_message;

        fn sync_content(sync: presage::proto::SyncMessage) -> Content {
            Content::from_body(ContentBody::SynchronizeMessage(sync), envelope_metadata())
        }
        fn fetch_latest(raw_type: Option<i32>) -> Content {
            sync_content(presage::proto::SyncMessage {
                content: Some(sync_message::Content::FetchLatest(
                    sync_message::FetchLatest { r#type: raw_type },
                )),
                ..presage::proto::SyncMessage::with_padding(&mut rng())
            })
        }

        let storage_manifest =
            presage::proto::sync_message::fetch_latest::Type::StorageManifest as i32;
        assert!(storage_manifest_fetch_requested(&fetch_latest(Some(
            storage_manifest
        ))));
        let local_profile = presage::proto::sync_message::fetch_latest::Type::LocalProfile as i32;
        assert!(
            !storage_manifest_fetch_requested(&fetch_latest(Some(local_profile))),
            "LOCAL_PROFILE 不触发"
        );
        let subscription =
            presage::proto::sync_message::fetch_latest::Type::SubscriptionStatus as i32;
        assert!(
            !storage_manifest_fetch_requested(&fetch_latest(Some(subscription))),
            "SUBSCRIPTION_STATUS 不触发"
        );
        let unknown = presage::proto::sync_message::fetch_latest::Type::Unknown as i32;
        assert!(!storage_manifest_fetch_requested(&fetch_latest(Some(
            unknown
        ))));
        assert!(
            !storage_manifest_fetch_requested(&fetch_latest(Some(99))),
            "未知 raw 值不触发"
        );
        assert!(
            !storage_manifest_fetch_requested(&fetch_latest(None)),
            "type 缺席不触发（信号面照投空对象语义不变）"
        );

        // 非 FetchLatest 体不触发。
        let sent = sync_content(presage::proto::SyncMessage {
            content: Some(sync_message::Content::Sent(sync_message::Sent::default())),
            ..presage::proto::SyncMessage::with_padding(&mut rng())
        });
        assert!(!storage_manifest_fetch_requested(&sent));
        let data = Content::from_body(
            ContentBody::DataMessage(presage::proto::DataMessage::default()),
            envelope_metadata(),
        );
        assert!(!storage_manifest_fetch_requested(&data));
    }

    /// T-9 消费臂防抖器（官方 sync job debounce 500ms 同款，trailing 沿）：
    /// 防抖窗内多封合并为恰一次拉取——窗沿随每封推后、旧沿作废、静默期满
    /// 恰一拉、同窗 take_due 不重复；火后新封开新窗。
    #[test]
    fn manifest_pull_debounce_coalesces_burst_into_single_fire() {
        let mut debounce = ManifestPullDebounce::default();
        assert_eq!(debounce.pending_deadline(), None);
        assert!(!debounce.take_due(1_000), "无窗不拉");

        // 首封开窗（+500ms）；窗内不拉。
        debounce.on_fetch(1_000);
        assert_eq!(debounce.pending_deadline(), Some(1_500));
        assert!(!debounce.take_due(1_499), "窗沿未到不拉");

        // 窗内第二封：窗沿推后（trailing），旧沿作废。
        debounce.on_fetch(1_400);
        assert_eq!(debounce.pending_deadline(), Some(1_900));
        assert!(!debounce.take_due(1_500), "旧窗沿已作废");

        // 三封 burst（1_000/1_400/1_850）→ 窗沿随末封推至 2_350，静默期满
        // 恰一拉。
        debounce.on_fetch(1_850);
        assert!(!debounce.take_due(2_349));
        assert!(debounce.take_due(2_350), "静默期满恰一次");
        assert_eq!(debounce.pending_deadline(), None);
        assert!(!debounce.take_due(2_351), "同窗不重复拉");

        // 火后新封 → 新窗（后续信封照常触发下一轮）。
        debounce.on_fetch(2_000);
        assert_eq!(debounce.pending_deadline(), Some(2_500));
        assert!(debounce.take_due(2_500));
        assert_eq!(debounce.pending_deadline(), None);
    }

    /// T-9 登录期 sync/自动拉共闸（60s 一次性，P15c 项4 既有语义抽纯函数
    /// ——「登录期建流后自动拉恰一次」的闸腿：首流/重启后 true，重连风暴
    /// 窗内恰跳过，超窗重臂；臂体一次性消费 + 拉取随臂体恰一调为结构保证
    /// ——单任务 select 循环，臂体串行无重入）。
    #[test]
    fn initial_sync_gate_window_faces() {
        assert!(initial_sync_due(None), "首流/重启后必安排");
        assert!(
            !initial_sync_due(Some(INITIAL_SYNC_WINDOW_MS - 1)),
            "窗内（重连风暴）恰跳过"
        );
        assert!(initial_sync_due(Some(INITIAL_SYNC_WINDOW_MS)), "超窗重臂");
    }

    /// T-9b 结果事件消费方测试替身：通道恰一帧且为 `engine.storagePull`
    /// 通知，方法名/jsonrpc/account/trigger/双腿位逐键验型（形状钉定）。
    fn expect_storage_pull_notice(
        rx: &mut tokio::sync::mpsc::Receiver<crate::OutboundFrame>,
        account: &str,
        trigger_wire: &str,
        contacts_ok: bool,
        groups_ok: bool,
    ) {
        let frame = rx.try_recv().expect("结果事件恰一帧");
        let crate::OutboundFrame::Notice { value, .. } = frame else {
            panic!("出站帧应为通知帧");
        };
        assert_eq!(value["method"], json!("engine.storagePull"));
        assert_eq!(value["jsonrpc"], json!("2.0"));
        assert_eq!(value["params"]["account"], json!(account));
        assert_eq!(value["params"]["trigger"], json!(trigger_wire));
        assert_eq!(value["params"]["contactsOk"], json!(contacts_ok));
        assert_eq!(value["params"]["groupsOk"], json!(groups_ok));
    }

    /// T-9 manifest 自动拉分发面（单点两腿恰各一调 + 幂等入口）：未注册
    /// 账号 real 模式两腿都在 account 解析处报 UnknownAccount（分发证据
    /// ——该错误只能来自 merge_storage_contacts/import_storage_groups 本体
    /// ，错误逐腿上浮不连坐）；重复拉同形再现，无 panic 无状态残留。
    /// fake 模式两腿零汇总早退（Ok(Default)，与 storage.mergeContacts/
    /// importGroups RPC fake 面同纪律）。T-9b 起两臂解析后各恒发一条
    /// `engine.storagePull` 结果事件（失败如实位——此前「零通知」断言由
    /// 本契约取代），storage_watch 观测出口照旧零翻转（观测批空，无
    /// conversationState 噪声帧）。通知抑制的幂等语义（重复拉同值零帧）
    /// 由 conversation_state_watch_publish_and_echo_suppression 钉定（同
    /// 一 publish 单点）。
    #[tokio::test]
    async fn manifest_pull_dispatches_both_merge_legs_idempotently() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-manifest-pull-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, mut rx) = outbound::test_hub();
        let service = Service::new(data_dir, Arc::clone(&state), outbound);
        let (legs_ok, expect_legs) = {
            let expect_legs = |outcome: &ManifestPullOutcome| {
                let (contacts, groups) = outcome;
                if mode_from_env() {
                    assert!(contacts.is_ok(), "fake 两腿零汇总早退");
                    assert!(groups.is_ok(), "fake 两腿零汇总早退");
                } else {
                    assert!(
                        matches!(
                            contacts,
                            Err(storage_sync::StorageSyncError::UnknownAccount)
                        ),
                        "mergeContacts 腿被调（UnknownAccount 上浮）"
                    );
                    assert!(
                        matches!(groups, Err(storage_sync::StorageSyncError::UnknownAccount)),
                        "importGroups 腿被调（UnknownAccount 上浮）"
                    );
                }
            };
            (mode_from_env(), expect_legs)
        };

        let outcome = pull_storage_manifest(
            &service,
            "+15550000000",
            perf_metrics::keys::STORAGE_MANIFEST_PULL,
        )
        .await;
        expect_legs(&outcome);
        expect_storage_pull_notice(&mut rx, "+15550000000", "fetchLatest", legs_ok, legs_ok);
        assert!(rx.try_recv().is_err(), "观测批空，conversationState 零帧");
        // 幂等入口：重复拉同形再现（结果事件随行各一帧）。
        let outcome = pull_storage_manifest(
            &service,
            "+15550000000",
            perf_metrics::keys::STORAGE_MANIFEST_PULL,
        )
        .await;
        expect_legs(&outcome);
        expect_storage_pull_notice(&mut rx, "+15550000000", "fetchLatest", legs_ok, legs_ok);
        assert!(rx.try_recv().is_err(), "无其他帧");
    }

    /// T-9b 结果事件面（契约 1.141 引擎腿）：两触发臂各发恰一条，trigger
    /// wire 名与 perf 触发键单射（storage_manifest_pull → fetchLatest、
    /// storage_manifest_pull_login → login），腿位如实，通道无残余帧。
    #[tokio::test]
    async fn storage_pull_result_event_fires_per_trigger_with_truthy_legs() {
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-storage-pull-event-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, mut rx) = outbound::test_hub();
        let service = Service::new(data_dir, Arc::clone(&state), outbound);
        let legs_ok = mode_from_env();

        let _ = pull_storage_manifest(
            &service,
            "+15550000001",
            perf_metrics::keys::STORAGE_MANIFEST_PULL_LOGIN,
        )
        .await;
        expect_storage_pull_notice(&mut rx, "+15550000001", "login", legs_ok, legs_ok);

        let _ = pull_storage_manifest(
            &service,
            "+15550000001",
            perf_metrics::keys::STORAGE_MANIFEST_PULL,
        )
        .await;
        expect_storage_pull_notice(&mut rx, "+15550000001", "fetchLatest", legs_ok, legs_ok);

        assert!(rx.try_recv().is_err(), "每拉恰一帧，无其他通知");
    }

    /// `engine.storagePull` 通知帧消费方测试替身（词表注册验收面）：方法名
    /// 入词表、params 恰四键逐键验型，坏形状整拒。
    #[test]
    fn storage_pull_notification_shape_pins_wire_contract() {
        fn strict_parse(value: &Value) -> Option<(String, String, bool, bool)> {
            if value["method"] != "engine.storagePull" || value["jsonrpc"] != "2.0" {
                return None;
            }
            let params = value["params"].as_object()?;
            if params.len() != 4 {
                return None;
            }
            Some((
                params["account"].as_str()?.to_owned(),
                params["trigger"].as_str()?.to_owned(),
                params["contactsOk"].as_bool()?,
                params["groupsOk"].as_bool()?,
            ))
        }

        let login = storage_pull_notification(
            "acct",
            perf_metrics::keys::STORAGE_MANIFEST_PULL_LOGIN,
            true,
            false,
        );
        assert_eq!(
            strict_parse(&login),
            Some(("acct".to_owned(), "login".to_owned(), true, false))
        );
        let fetch = storage_pull_notification(
            "acct",
            perf_metrics::keys::STORAGE_MANIFEST_PULL,
            false,
            true,
        );
        assert_eq!(
            strict_parse(&fetch),
            Some(("acct".to_owned(), "fetchLatest".to_owned(), false, true))
        );
        // 词表外方法名整拒（consumer 侧 default 臂语义）。
        assert!(strict_parse(&json!({"method": "receive", "params": {}})).is_none());
    }

    /// `engine.initialSyncComplete` 通知帧消费方测试替身（收件箱闸批①
    /// wire additive）：方法名入词表、params 恰三键逐键验型，坏形状整拒
    /// ——storagePull 同款验收纪律，connector/desktop 转发链照此对账。
    #[test]
    fn initial_sync_complete_notification_shape_pins_wire_contract() {
        fn strict_parse(value: &Value) -> Option<(String, bool, bool)> {
            if value["method"] != "engine.initialSyncComplete" || value["jsonrpc"] != "2.0" {
                return None;
            }
            let params = value["params"].as_object()?;
            if params.len() != 3 {
                return None;
            }
            Some((
                params["account"].as_str()?.to_owned(),
                params["manifestOk"].as_bool()?,
                params["profilesOk"].as_bool()?,
            ))
        }

        let ok = initial_sync_complete_notification("acct", true, true);
        assert_eq!(strict_parse(&ok), Some(("acct".to_owned(), true, true)));
        let partial = initial_sync_complete_notification("acct", false, true);
        assert_eq!(
            strict_parse(&partial),
            Some(("acct".to_owned(), false, true))
        );
        // 词表外方法名整拒（consumer 侧 default 臂语义）。
        assert!(strict_parse(&json!({"method": "receive", "params": {}})).is_none());
    }

    /// F 链路回声抑制预观测（storage.write 面）：三类会话记录 INSERT/
    /// UPDATE 明文出 (标识, 三 flag) 观测对；DELETE（无载荷）与非会话族
    /// /坏 record → None。
    #[test]
    fn conversation_write_observation_faces() {
        let contact_wire = crate::proto::storage::StorageRecord {
            record: Some(crate::proto::storage::storage_record::Record::Contact(
                crate::proto::storage::ContactRecord {
                    aci: "11111111-2222-4333-8444-555555555555".to_owned(),
                    archived: true,
                    ..Default::default()
                },
            )),
        }
        .encode_to_vec();
        let update = storage_write::StorageItemUpdate::Update {
            key: vec![1u8; 16],
            record: contact_wire,
            kind: storage_sync::StorageRecordKind::Contact,
        };
        let (id, state) = conversation_write_observation(&update).expect("observed");
        assert_eq!(
            id,
            storage_sync::EngineConversationId::Contact(
                "11111111-2222-4333-8444-555555555555".to_owned()
            )
        );
        assert!(state.archived);
        assert!(!state.marked_unread);
        assert_eq!(state.muted_until_timestamp, 0);

        let delete = storage_write::StorageItemUpdate::Delete { key: vec![1u8; 16] };
        assert!(conversation_write_observation(&delete).is_none());
        let bad = storage_write::StorageItemUpdate::Insert {
            key: vec![2u8; 16],
            record: vec![0xff, 0xff],
            kind: storage_sync::StorageRecordKind::Contact,
        };
        assert!(conversation_write_observation(&bad).is_none());
    }

    /// whitelist 会话级缓存（修1）：TTL 内命中、过期未命中、失效单点、
    /// 跨账号隔离（deadline 形可拨针，无真实等待）。
    #[test]
    fn whitelist_cache_ttl_and_invalidation() {
        let cache = StdMutex::new(WhitelistCache::new());
        assert!(
            whitelist_cache_get(&cache, "acct").is_none(),
            "空缓存未命中"
        );
        let mut map = HashMap::new();
        map.insert("aci-a".to_owned(), false);
        map.insert("aci-b".to_owned(), true);
        whitelist_cache_put(&cache, "acct", map);
        let hit = whitelist_cache_get(&cache, "acct").expect("TTL 内命中");
        assert_eq!(hit.get("aci-a"), Some(&false), "位值逐条保真");
        assert_eq!(hit.get("aci-b"), Some(&true));
        // 拨针过期：deadline 拨到过去 → 未命中（fail-open 面由调用方空
        // map 语义兜住，缓存只管时限）。
        cache
            .lock()
            .expect("whitelist cache lock")
            .get_mut("acct")
            .expect("entry")
            .1 = std::time::Instant::now() - WHITELIST_CACHE_TTL;
        assert!(
            whitelist_cache_get(&cache, "acct").is_none(),
            "TTL 过期未命中"
        );
        // 失效单点：在册账号移除；他账号不受牵连。
        whitelist_cache_put(&cache, "acct", HashMap::new());
        whitelist_cache_put(&cache, "other", HashMap::new());
        whitelist_cache_invalidate(&cache, "acct");
        assert!(whitelist_cache_get(&cache, "acct").is_none(), "单账号失效");
        assert!(
            whitelist_cache_get(&cache, "other").is_some(),
            "他账号不受牵连"
        );
    }

    /// DERM 新鲜度窗口（官方 retryRespondMaxAge=14 天同语义）：窗口内
    /// 放行，边界外（超龄/未来时间戳）转兜底档。
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
        // 名单不可得 → 兜底档。
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

    /// 群发送语义两件：announcements-only 闸判 ADMINISTRATOR 角色
    /// （Default/缺席皆非 admin）。群发送延迟批（2026-10-18）后发送路径
    /// 不再有同步群态刷新决策（vendored `group_refresh_needed` 强刷契约
    /// 废止——官方同语义：发送读本地群态直投，刷新由后台群同步承担，
    /// vendor 侧该纯函数随之移除，无表面可钉）。
    #[test]
    fn group_announcements_gate_contract() {
        use presage::libsignal_service::groups_v2::Role;
        use presage::libsignal_service::prelude::{ProfileKey, Uuid};
        use presage::libsignal_service::protocol::Aci;
        use presage::manager::group_member_is_admin;
        use presage::model::groups::{Group, Member};

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

    /// 重试回执解码形状（derm_send_receipt 入参面）：presage RetryReceipt
    /// 字节（libsignal for_original 序列化面）→ DecryptionErrorMessage 保真
    /// timestamp/device_id/ratchet_key；缺 timestamp 的坏回执拒收。
    #[test]
    fn derm_receipt_roundtrip_keeps_timestamp_and_device() {
        use presage::libsignal_service::protocol::{DecryptionErrorMessage, KeyPair};
        use prost::Message as _;

        // 构造面与 presage RetryReceiptMaterial::into_failure 一致
        // （for_original：timestamp 恒有，ratchet_key 随密文类型可空）。
        let ratchet_key = KeyPair::generate(&mut rand::rng()).public_key.serialize();
        let receipt = presage::proto::DecryptionErrorMessage {
            timestamp: Some(1_700_000_000_000),
            ratchet_key: Some(ratchet_key.to_vec()),
            device_id: Some(2),
        };
        let bytes = receipt.encode_to_vec();

        let decoded = DecryptionErrorMessage::try_from(bytes.as_slice()).expect("roundtrip");
        assert_eq!(decoded.timestamp().epoch_millis(), 1_700_000_000_000);
        assert_eq!(decoded.device_id(), 2);
        assert!(decoded.ratchet_key().is_some());

        let broken = presage::proto::DecryptionErrorMessage {
            timestamp: None,
            ..receipt
        };
        assert!(
            DecryptionErrorMessage::try_from(broken.encode_to_vec().as_slice()).is_err(),
            "缺 timestamp 的回执必须拒收（请求方无法定位原消息）"
        );
    }

    /// 明文回执腿 wire 形状钉（E3 additive）：DecryptionErrorMessage →
    /// PlaintextContent = 0xC0 前缀 + Content proto（内嵌原始回执字节）+
    /// 0x80 边界字节；对端 extract 可逆找回同一 timestamp/device_id——
    /// 即 Envelope type=8 明文信封所载字节的钉形。
    #[test]
    fn derm_plaintext_receipt_wire_shape() {
        use presage::libsignal_service::protocol::{
            extract_decryption_error_message_from_serialized_content, DecryptionErrorMessage,
            PlaintextContent,
        };
        use prost::Message as _;

        let ratchet_key = presage::libsignal_service::protocol::KeyPair::generate(&mut rand::rng())
            .public_key
            .serialize();
        let receipt = presage::proto::DecryptionErrorMessage {
            timestamp: Some(1_700_000_000_000),
            ratchet_key: Some(ratchet_key.to_vec()),
            device_id: Some(2),
        };
        let derm =
            DecryptionErrorMessage::try_from(receipt.encode_to_vec().as_slice()).expect("parse");

        let plaintext = PlaintextContent::from(derm);
        let wire = plaintext.serialized();
        assert_eq!(wire.first(), Some(&0xC0), "PlaintextContent 标识前缀");
        assert_eq!(wire.last(), Some(&0x80), "padding 边界字节");

        let recovered = extract_decryption_error_message_from_serialized_content(plaintext.body())
            .expect("对端按官方同函数找回");
        assert_eq!(recovered.timestamp().epoch_millis(), 1_700_000_000_000);
        assert_eq!(recovered.device_id(), 2);
        assert!(recovered.ratchet_key().is_some());
    }

    /// archive-on-match 负路径钉（E4）：无 ratchet key / 非点名我设备 /
    /// 坏钥字节 / 会话 ratchet 不匹配 / 无会话地址——任一前置不满足即不动
    /// 会话（返回 false，既有会话原封）。正路径（命中→归档）完全委托上游
    /// `current_ratchet_key_matches`/`archive_current_state` 原语（上游
    /// 自测），此处不重建会话脚手架。
    #[tokio::test]
    async fn derm_archive_on_match_gates_refuse_without_match() {
        use presage::libsignal_service::protocol::{
            DeviceId, KeyPair, SessionRecord, SessionStore as _,
        };

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-derm-archive-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let store = open_store(&store_dir).await.unwrap();
        let requester =
            ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
                .expect("requester aci");

        // 落一个空会话（无 current session：current_ratchet_key_matches 恒
        // false——真命中路径在此脚手架里不会发生）。
        let address = requester.to_protocol_address(DeviceId::new(3).expect("device id"));
        let mut protocol_store = store.aci_protocol_store();
        protocol_store
            .store_session(&address, &SessionRecord::new_fresh())
            .await
            .expect("seed session");
        assert!(protocol_store
            .load_session(&address)
            .await
            .unwrap()
            .is_some());

        let key_bytes = KeyPair::generate(&mut rand::rng())
            .public_key
            .serialize()
            .to_vec();

        // 无 ratchet key → 不动。
        assert!(!derm_archive_session_on_match(&store, 1, &requester, None, 1, 3).await);
        // 点名他设备 → 不动（即使带 key）。
        assert!(
            !derm_archive_session_on_match(&store, 1, &requester, Some(&key_bytes), 2, 3).await
        );
        // 点名我设备 + 带 key，但会话 ratchet 不匹配 → 不动。
        assert!(
            !derm_archive_session_on_match(&store, 1, &requester, Some(&key_bytes), 1, 3).await
        );
        // 坏钥字节 → 不动。
        assert!(
            !derm_archive_session_on_match(&store, 1, &requester, Some(&[1, 2, 3]), 1, 3).await
        );
        // 无会话地址 → 不动。
        assert!(
            !derm_archive_session_on_match(&store, 1, &requester, Some(&key_bytes), 1, 9).await
        );

        // 原会话原封未动。
        assert!(protocol_store
            .load_session(&address)
            .await
            .unwrap()
            .is_some());
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

    /// 官方终局 close code 钉形：1000（normal closure）与 4409（connected
    /// elsewhere）停环不重连；1001/1006/3001/无 code 仍走重连。
    #[test]
    fn receive_close_codes_follow_official_no_reconnect_rule() {
        assert!(receive_close_is_terminal(Some(1000)));
        assert!(receive_close_is_terminal(Some(4409)));
        assert!(
            receive_close_is_terminal(Some(4000)),
            "本地槽接管（4000）也必须停环，避免旧管线与新管线互踢"
        );
        assert!(!receive_close_is_terminal(Some(1001)));
        assert!(!receive_close_is_terminal(Some(1006)));
        assert!(!receive_close_is_terminal(Some(3001)));
        assert!(!receive_close_is_terminal(None));
        assert!(StreamOutcome::Terminated.is_terminal());
    }

    /// 重连排程：manager 不可用（Recoverable）按官方 Fibonacci 逐级重试，
    /// 绝不会因为一次失败而静默退出；建流成功（Connected）立即 reset 到
    /// 首拍 1s；终止结局返回 None（停环）。
    #[test]
    fn reconnect_schedule_retries_recoverable_then_resets_on_success() {
        let mut schedule = ReconnectSchedule::default();

        assert_eq!(
            schedule.next_delay(StreamOutcome::Recoverable),
            Some(Duration::from_millis(1_000)),
            "第一次建流失败 → 1s 后重试"
        );
        assert_eq!(schedule.step(), 1);
        assert_eq!(
            schedule.next_delay(StreamOutcome::Recoverable),
            Some(Duration::from_millis(2_000)),
            "第二次失败 → 2s（不再重复 1s）"
        );
        assert_eq!(schedule.step(), 2);
        assert_eq!(
            schedule.next_delay(StreamOutcome::Recoverable),
            Some(Duration::from_millis(3_000))
        );

        // 建流成功（流结束后回到重连）→ reset，首拍 1s。
        assert_eq!(
            schedule.next_delay(StreamOutcome::Connected),
            Some(Duration::from_millis(1_000))
        );
        assert_eq!(schedule.step(), 0);
        assert_eq!(
            schedule.next_delay(StreamOutcome::Recoverable),
            Some(Duration::from_millis(1_000)),
            "成功 reset 后下一次失败从首拍重来"
        );

        // 终止结局不进入退避。
        assert_eq!(schedule.next_delay(StreamOutcome::Unbound), None);
        assert_eq!(schedule.next_delay(StreamOutcome::Terminated), None);
        assert_eq!(schedule.next_delay(StreamOutcome::StdioDead), None);
    }

    /// 最小复现（2026-10-21 静默接收事故）：manager 在启动期不可用
    /// （store 存在但 load_registered 失败）时，接收循环必须持续重试并保持
    /// 存活，直到显式停机——旧路径无日志、无重启，现场只看到「没消息」。
    /// paused 时钟下按 1s/2s/3s/5s… 推进，断言至少发生 3 次真实尝试且任务
    /// 未静默退出。
    #[tokio::test(start_paused = true)]
    async fn receive_loop_retries_unavailable_manager_until_shutdown() {
        use std::sync::atomic::Ordering;

        RECEIVE_LOOP_ATTEMPTS.store(0, Ordering::Relaxed);
        let data_dir = std::env::temp_dir().join(format!(
            "kt-engine-receive-retry-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let state = Arc::new(AsyncMutex::new(
            EngineState::load(&data_dir).expect("state"),
        ));
        let (outbound, _rx) = outbound::test_hub();
        let service = Service::new(data_dir.clone(), Arc::clone(&state), outbound);
        // 启动期不可用的等价注入：store 路径被普通文件占住 → open_store
        // 立即失败，Manager 无从加载；接收循环只能按退避重试。（不用真实
        // 空 store：SQLx 连接池会在 paused 时钟下先触发自身超时，单测不
        // 确定；该故障与 manager load 失败共享同一 Recoverable 重试路径。）
        std::fs::create_dir_all(data_dir.join("accounts")).expect("accounts dir");
        let dir = data_dir.join("accounts/never-registered");
        std::fs::write(&dir, b"not a store directory").expect("blocking file");
        let (tx, rx) = watch::channel(false);

        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let handle = tokio::task::spawn_local(async move {
                    receive_account_loop(&service, "+15550002222", &dir, rx).await;
                });
                // 1s→2s→3s→5s 的重试在 paused 时钟下逐小步推进；小步
                // 让出调度，确保每轮退避都能登记计时器后再前进。
                let mut waited = Duration::ZERO;
                while RECEIVE_LOOP_ATTEMPTS.load(Ordering::Relaxed) < 3
                    && waited < Duration::from_secs(30)
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    waited += Duration::from_millis(100);
                }
                let attempts = RECEIVE_LOOP_ATTEMPTS.load(Ordering::Relaxed);
                assert!(
                    attempts >= 3,
                    "manager 不可用时接收循环必须持续重试，实际仅 {attempts} 次"
                );
                assert!(
                    !handle.is_finished(),
                    "manager 不可用时接收循环不得静默退出"
                );
                tx.send(true).expect("shutdown flag");
                handle.await.expect("receive loop joins");
            })
            .await;
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// 重连退避序列（止血批②，官方 Desktop ts/backoff/BackOff.std.ts 同
    /// 序列钉定）：Fibonacci 1,2,3,5,8,13,21,34,55 封顶 55s，之后不再
    /// 增长；step=0（连接成功 reset 后）从 1s 重来。2026-10-21 对齐批去掉
    /// 旧实现 step=0/1 重复的 1s。
    #[test]
    fn reconnect_backoff_follows_official_fibonacci_sequence() {
        let ms = |step| reconnect_backoff_delay(step).as_millis();
        assert_eq!(ms(0), 1_000);
        assert_eq!(ms(1), 2_000);
        assert_eq!(ms(2), 3_000);
        assert_eq!(ms(3), 5_000);
        assert_eq!(ms(4), 8_000);
        assert_eq!(ms(5), 13_000);
        assert_eq!(ms(6), 21_000);
        assert_eq!(ms(7), 34_000);
        assert_eq!(ms(8), 55_000);
        assert_eq!(ms(9), 55_000, "官方 MAX_BACKOFF 封顶");
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
            None,
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

    /// Polls 发送批（窗D A7）proto 构造面：pollCreate{question,allowMultiple,
    /// options} / pollVote{targetAuthorAciBinary,targetSentTimestamp,
    /// optionIndexes(去重保首现序),voteCount} / pollTerminate{targetSent-
    /// Timestamp}，三臂互斥各居其位（官方 toProto/
    /// createDataMessageProtoForPoll* 同位）。
    #[test]
    fn poll_send_proto_faces() {
        let aci_bytes = aci_to_bytes("11111111-2222-4333-8444-555555555555").unwrap();

        let create = poll_create_proto("Lunch?", &["Ramen".into(), "Salad".into()], true);
        assert_eq!(create.question.as_deref(), Some("Lunch?"));
        assert_eq!(create.allow_multiple, Some(true));
        assert_eq!(create.options, vec!["Ramen".to_owned(), "Salad".to_owned()]);

        let vote = poll_vote_proto(aci_bytes.clone(), 42, &[3, 1, 3, 9], 7);
        assert_eq!(
            vote.target_author_aci_binary.as_deref(),
            Some(aci_bytes.as_slice())
        );
        assert_eq!(vote.target_sent_timestamp, Some(42));
        assert_eq!(vote.option_indexes, vec![3, 1, 9]);
        assert_eq!(vote.vote_count, Some(7));

        let terminate = poll_terminate_proto(99);
        assert_eq!(terminate.target_sent_timestamp, Some(99));
    }

    /// sendContact（窗D B11）proto 构造面：name 六子字段、number/email
    /// type 词表→枚举映射（缺省投影 HOME——官方 numberTo*Type(undefined)
    /// 默认同位；phone/email 同编号单点共用）、address 三值词表、
    /// organization 透传、avatar 指针注入与缺席两态（isProfile 恒 false）。
    #[test]
    fn contact_proto_semantics() {
        use presage::proto::data_message::contact::{email, phone, postal_address};

        let payload = SendContact {
            name: Some(SendContactName {
                given_name: Some("Ada".to_owned()),
                family_name: Some("Lovelace".to_owned()),
                prefix: None,
                suffix: None,
                middle_name: Some("Augusta".to_owned()),
                nickname: None,
            }),
            number: vec![
                SendContactField {
                    value: "+15551234567".to_owned(),
                    r#type: Some("MOBILE".to_owned()),
                    label: Some("cell".to_owned()),
                },
                SendContactField {
                    value: "+15557654321".to_owned(),
                    r#type: None,
                    label: None,
                },
            ],
            email: vec![SendContactField {
                value: "ada@example.test".to_owned(),
                r#type: Some("WORK".to_owned()),
                label: None,
            }],
            address: vec![SendContactAddress {
                r#type: Some("CUSTOM".to_owned()),
                label: Some("lab".to_owned()),
                street: Some("1 Analytical Way".to_owned()),
                pobox: None,
                neighborhood: None,
                city: Some("London".to_owned()),
                region: None,
                postcode: None,
                country: Some("UK".to_owned()),
            }],
            avatar: None,
            organization: Some("Analytical Engines".to_owned()),
        };

        let contact = contact_proto(&payload, None);
        let name = contact.name.expect("name projected");
        assert_eq!(name.given_name.as_deref(), Some("Ada"));
        assert_eq!(name.family_name.as_deref(), Some("Lovelace"));
        assert_eq!(name.middle_name.as_deref(), Some("Augusta"));
        assert_eq!(name.prefix, None);
        assert_eq!(name.suffix, None);
        assert_eq!(name.nickname, None);

        assert_eq!(contact.number.len(), 2);
        assert_eq!(contact.number[0].r#type, Some(phone::Type::Mobile as i32));
        assert_eq!(contact.number[0].label.as_deref(), Some("cell"));
        // type 缺省 → HOME（官方 numberToPhoneType(undefined) 同位）。
        assert_eq!(contact.number[1].r#type, Some(phone::Type::Home as i32));

        assert_eq!(contact.email.len(), 1);
        assert_eq!(contact.email[0].r#type, Some(email::Type::Work as i32));
        assert_eq!(contact.email[0].value.as_deref(), Some("ada@example.test"));

        assert_eq!(contact.address.len(), 1);
        assert_eq!(
            contact.address[0].r#type,
            Some(postal_address::Type::Custom as i32)
        );
        assert_eq!(contact.address[0].city.as_deref(), Some("London"));
        assert_eq!(contact.address[0].pobox, None);

        assert_eq!(contact.organization.as_deref(), Some("Analytical Engines"));
        // 无头像 = Avatar 位缺席（官方 avatar 位仅在场才成 Avatar 同位）。
        assert!(contact.avatar.is_none());

        // 头像指针注入：pointer 原样进 Avatar.avatar，isProfile 恒 false。
        let with_avatar = contact_proto(&payload, Some(Default::default()));
        let avatar = with_avatar.avatar.expect("avatar projected");
        assert!(avatar.avatar.is_some());
        assert_eq!(avatar.is_profile, Some(false));
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

    /// P15c 项4 syncRequests 单请求构帧：SyncMessage.content.Request{type}
    /// 枚举直通（CONFIGURATION=4/BLOCKED=3，官方
    /// getRequestConfigurationSyncMessage/getRequestBlockSyncMessage 同位）。
    #[test]
    fn sync_request_frame_carries_type() {
        for (word, value) in [
            (
                presage::proto::sync_message::request::Type::Configuration,
                4,
            ),
            (presage::proto::sync_message::request::Type::Blocked, 3),
        ] {
            let sync = build_sync_request(word);
            let presage::proto::sync_message::Content::Request(request) =
                sync.content.as_ref().expect("content")
            else {
                panic!("request content");
            };
            assert_eq!(request.r#type, Some(value), "{value} 枚举直通");
        }
    }

    /// P15c 项3 DeleteForMe 会话级构帧：全量清空臂（conversationDeletes[0]
    /// isFullDelete + mostRecent 双数组）/ 仅本地臂（localOnlyConversation
    /// Deletes[0]）三形态会话标识（service-id binary/E164/group master key）
    /// 含条目 author binary 面——官方 getDeleteForMeSyncMessage 同位字节
    /// 钉形（encode→decode 回读对账，群审批 deny 原语单测同纪律）。
    #[test]
    fn delete_for_me_conversation_sync_shapes() {
        let aci = ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
            .expect("aci");
        let entries = |count: usize| {
            (0..count)
                .map(|i| DeleteForMeEntry {
                    author: DeleteForMeAuthor::ServiceId(aci),
                    timestamp: 1_700_000_000_000 + i as u64,
                })
                .collect::<Vec<_>>()
        };

        // 全量清空臂（直聊 ACI）：conversationDeletes 恰一条、isFullDelete
        // 真、mostRecent 双数组进位；localOnly 臂空。
        let sync = build_delete_for_me_conversation_sync(
            &DeleteForMeConversation::Contact(aci),
            false,
            &entries(2),
            &entries(1),
        );
        let presage::proto::sync_message::Content::DeleteForMe(delete) =
            sync.content.as_ref().expect("content")
        else {
            panic!("deleteForMe content");
        };
        assert!(delete.message_deletes.is_empty());
        assert!(delete.local_only_conversation_deletes.is_empty());
        let [group] = delete.conversation_deletes.as_slice() else {
            panic!("one conversation delete");
        };
        assert_eq!(group.is_full_delete, Some(true));
        assert_eq!(group.most_recent_messages.len(), 2);
        assert_eq!(group.most_recent_non_expiring_messages.len(), 1);
        let identifier = group
            .conversation
            .as_ref()
            .expect("conversation")
            .identifier
            .as_ref()
            .expect("identifier");
        match identifier {
            presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes) => {
                assert_eq!(bytes, &aci.service_id_binary(), "service id binary 面");
            }
            other => panic!("thread service id binary, got {other:?}"),
        }
        let entry = &group.most_recent_messages[0];
        assert_eq!(entry.sent_timestamp, Some(1_700_000_000_000));
        match entry.author.as_ref().expect("author") {
            presage::proto::addressable_message::Author::AuthorServiceIdBinary(bytes) => {
                assert_eq!(bytes, &aci.service_id_binary());
            }
            other => panic!("author binary, got {other:?}"),
        }

        // 仅本地臂（群 master key）：localOnlyConversationDeletes 恰一条、
        // conversationDeletes 空。
        let sync = build_delete_for_me_conversation_sync(
            &DeleteForMeConversation::Group(vec![9u8; 32]),
            true,
            &[],
            &[],
        );
        let presage::proto::sync_message::Content::DeleteForMe(delete) =
            sync.content.as_ref().expect("content")
        else {
            panic!("deleteForMe content");
        };
        assert!(delete.conversation_deletes.is_empty());
        let [local] = delete.local_only_conversation_deletes.as_slice() else {
            panic!("one local-only delete");
        };
        match local
            .conversation
            .as_ref()
            .expect("conversation")
            .identifier
            .as_ref()
            .expect("identifier")
        {
            presage::proto::conversation_identifier::Identifier::ThreadGroupId(bytes) => {
                assert_eq!(bytes, &[9u8; 32], "group master key 原样");
            }
            other => panic!("thread group id, got {other:?}"),
        }

        // E164 会话 + E164 作者（官方 e164 臂直通形态）。
        let sync = build_delete_for_me_conversation_sync(
            &DeleteForMeConversation::Phone("+15551234567".to_owned()),
            false,
            &[DeleteForMeEntry {
                author: DeleteForMeAuthor::Phone("+15550009999".to_owned()),
                timestamp: 42,
            }],
            &[],
        );
        let presage::proto::sync_message::Content::DeleteForMe(delete) =
            sync.content.as_ref().expect("content")
        else {
            panic!("deleteForMe content");
        };
        let [group] = delete.conversation_deletes.as_slice() else {
            panic!("one conversation delete");
        };
        match group
            .conversation
            .as_ref()
            .expect("conversation")
            .identifier
            .as_ref()
            .expect("identifier")
        {
            presage::proto::conversation_identifier::Identifier::ThreadE164(e164) => {
                assert_eq!(e164, "+15551234567");
            }
            other => panic!("thread e164, got {other:?}"),
        }
        match group.most_recent_messages[0]
            .author
            .as_ref()
            .expect("author")
        {
            presage::proto::addressable_message::Author::AuthorE164(e164) => {
                assert_eq!(e164, "+15550009999");
            }
            other => panic!("author e164, got {other:?}"),
        }
    }

    /// leg 3/3 DeleteForMe 消息级构帧：messageDeletes 恰一条
    /// {conversation, messages[]}（官方 getDeleteForMeSyncMessage 按会话
    /// 聚合，SendMessage.preload.ts:1912-1930），其余三臂空；三形态会话
    /// 标识 + 条目 author binary/E164 字节面钉形。
    #[test]
    fn delete_for_me_messages_sync_shapes() {
        let aci = ServiceId::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
            .expect("aci");
        let entries = vec![
            DeleteForMeEntry {
                author: DeleteForMeAuthor::ServiceId(aci),
                timestamp: 1_700_000_000_000,
            },
            DeleteForMeEntry {
                author: DeleteForMeAuthor::Phone("+15550009999".to_owned()),
                timestamp: 1_700_000_000_001,
            },
        ];
        let sync =
            build_delete_for_me_messages_sync(&DeleteForMeConversation::Contact(aci), &entries);
        let presage::proto::sync_message::Content::DeleteForMe(delete) =
            sync.content.as_ref().expect("content")
        else {
            panic!("deleteForMe content");
        };
        assert!(delete.conversation_deletes.is_empty());
        assert!(delete.local_only_conversation_deletes.is_empty());
        assert!(delete.attachment_deletes.is_empty());
        let [message_deletes] = delete.message_deletes.as_slice() else {
            panic!("one message deletes group");
        };
        match message_deletes
            .conversation
            .as_ref()
            .expect("conversation")
            .identifier
            .as_ref()
            .expect("identifier")
        {
            presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes) => {
                assert_eq!(bytes, &aci.service_id_binary(), "service id binary 面");
            }
            other => panic!("thread service id binary, got {other:?}"),
        }
        assert_eq!(message_deletes.messages.len(), 2);
        let first = &message_deletes.messages[0];
        assert_eq!(first.sent_timestamp, Some(1_700_000_000_000));
        match first.author.as_ref().expect("author") {
            presage::proto::addressable_message::Author::AuthorServiceIdBinary(bytes) => {
                assert_eq!(bytes, &aci.service_id_binary());
            }
            other => panic!("author binary, got {other:?}"),
        }
        match message_deletes.messages[1].author.as_ref().expect("author") {
            presage::proto::addressable_message::Author::AuthorE164(e164) => {
                assert_eq!(e164, "+15550009999");
            }
            other => panic!("author e164, got {other:?}"),
        }

        // 群 master key + E164 会话标识两形态（消息级同一构帧单点）。
        for (conversation, expected) in [
            (
                DeleteForMeConversation::Group(vec![9u8; 32]),
                presage::proto::conversation_identifier::Identifier::ThreadGroupId(vec![9u8; 32]),
            ),
            (
                DeleteForMeConversation::Phone("+15551234567".to_owned()),
                presage::proto::conversation_identifier::Identifier::ThreadE164(
                    "+15551234567".to_owned(),
                ),
            ),
        ] {
            let sync = build_delete_for_me_messages_sync(&conversation, &entries[..1]);
            let presage::proto::sync_message::Content::DeleteForMe(delete) =
                sync.content.as_ref().expect("content")
            else {
                panic!("deleteForMe content");
            };
            let [message_deletes] = delete.message_deletes.as_slice() else {
                panic!("one message deletes group");
            };
            assert_eq!(
                message_deletes
                    .conversation
                    .as_ref()
                    .expect("conversation")
                    .identifier
                    .as_ref()
                    .expect("identifier"),
                &expected,
            );
            assert_eq!(message_deletes.messages.len(), 1);
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
            project_content("acct", &store, &content, &registry, None).await,
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
            project_content("acct", &store, &content, &registry, None).await,
            "story echo projects",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyMessage"]["textAttachment"]["text"],
            json!("hello story")
        );

        // 全缺位：整信封丢弃（宁缺勿错）。
        let content = sent_body(sync_message::Sent::default());
        assert!(project_content("acct", &store, &content, &registry, None)
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// S5 story 删除扇出批：官方 storyRecipientUpdate 门三腿
    /// （MessageReceiver.preload.ts:3002-3030 门序镜像）。①命中腿：
    /// isRecipientUpdate ∧ storyMessageRecipients 非空 → `syncMessage.
    /// storyRecipientUpdate` 脸（{timestamp, storyMessageRecipients[]}，
    /// timestamp = 信封 ts 官方同源；destination 恒自有 ACI 不投），
    /// sentMessage 脸不在；②优先级腿：同信封再携 storyMessage 面也归新脸
    /// （官方门先于 storyMessage 判定同序——多名单 story 发送 sync
    /// isUpdate=true 形状自此改投新脸，行为变化记档 compat §5）；③负腿：
    /// storyMessage 面但 isRecipientUpdate 缺位 / recipients 空批 → 既有
    /// storyMessage echo 形状不变（门不命中走原路）。
    #[tokio::test]
    async fn sent_sync_story_recipient_update_gate_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::{sync_message, StoryMessage, SyncMessage, TextAttachment};

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-story-rcpt-{:016x}",
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
        let recipient = || sync_message::sent::StoryMessageRecipient {
            destination_service_id: Some("f4b3a2c0-1d2e-4f5a-8b7c-6d5e4f3a2b10".to_owned()),
            distribution_list_ids: vec!["00000000-0000-0000-0000-000000000000".to_owned()],
            is_allowed_to_reply: Some(true),
            destination_service_id_binary: None,
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
        let story_face = || StoryMessage {
            attachment: Some(presage::proto::story_message::Attachment::TextAttachment(
                TextAttachment {
                    text: Some("hello story".to_owned()),
                    ..TextAttachment::default()
                },
            )),
            ..StoryMessage::default()
        };

        // ①命中腿：isRecipientUpdate + 名单非空 → storyRecipientUpdate 脸。
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            is_recipient_update: Some(true),
            story_message_recipients: vec![recipient()],
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "recipient update projects",
        )["params"]["envelope"]
            .clone();
        assert!(
            envelope["syncMessage"].get("sentMessage").is_none(),
            "命中分支不产 sentMessage 脸（isRecipientUpdate/storyMessageRecipients 键不落旧位）"
        );
        let face = &envelope["syncMessage"]["storyRecipientUpdate"];
        assert_eq!(face["timestamp"], json!(1_700_000_000_000_u64), "信封 ts");
        assert_eq!(
            face["storyMessageRecipients"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            face["storyMessageRecipients"][0]["destinationUuid"],
            json!("f4b3a2c0-1d2e-4f5a-8b7c-6d5e4f3a2b10")
        );
        assert_eq!(
            face["storyMessageRecipients"][0]["distributionListIds"],
            json!(["00000000-0000-0000-0000-000000000000"])
        );
        assert_eq!(
            face["storyMessageRecipients"][0]["isAllowedToReply"],
            json!(true)
        );

        // ②优先级腿：+storyMessage 面同信封仍归新脸（官方门序镜像）。
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            is_recipient_update: Some(true),
            story_message: Some(story_face()),
            story_message_recipients: vec![recipient()],
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "gate precedes story face",
        )["params"]["envelope"]
            .clone();
        assert!(envelope["syncMessage"]
            .get("storyRecipientUpdate")
            .is_some());
        assert!(envelope["syncMessage"].get("sentMessage").is_none());

        // ③负腿：storyMessage 面在场但门键不齐 → 既有 storyMessage echo。
        // isRecipientUpdate 缺位：
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            story_message: Some(story_face()),
            story_message_recipients: vec![recipient()],
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "no isUpdate keeps echo",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyMessage"]["textAttachment"]["text"],
            json!("hello story")
        );
        assert!(envelope["syncMessage"]
            .get("storyRecipientUpdate")
            .is_none());
        // recipients 空批（isRecipientUpdate 在场）：
        let content = sent_body(sync_message::Sent {
            timestamp: Some(1_700_000_000_123),
            is_recipient_update: Some(true),
            story_message: Some(story_face()),
            ..sync_message::Sent::default()
        });
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "empty recipients keeps echo",
        )["params"]["envelope"]
            .clone();
        assert_eq!(
            envelope["syncMessage"]["sentMessage"]["storyMessage"]["textAttachment"]["text"],
            json!("hello story")
        );
        assert!(envelope["syncMessage"]
            .get("storyRecipientUpdate")
            .is_none());

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
            project_content("acct", &store, &content, &registry, None).await,
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
        assert!(project_content("acct", &store, &content, &registry, None)
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
            project_content("acct", &store, &content, &registry, None).await,
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
        assert!(project_content("acct", &store, &content, &registry, None)
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// attachmentDeletes 收投影（leg 1/3）：官方 handleDeleteForMeSync
    /// attachmentDeletes 臂同构——clientUuid→UUID 逐字、digest→标准
    /// base64、plaintextHash→小写 hex；conversation/targetMessage/author/
    /// sentTimestamp 不可解析、clientUuid 非 16B 或全零、三腿全缺均整条
    /// 丢；与其余三臂同信封并存；只有无效条目时整信封丢。
    #[tokio::test]
    async fn sync_projects_delete_for_me_attachment_faces() {
        use presage::libsignal_service::content::{Content, ContentBody, Metadata};
        use presage::proto::sync_message::delete_for_me::{
            AttachmentDelete, ConversationDelete, LocalOnlyConversationDelete, MessageDeletes,
        };
        use presage::proto::{
            sync_message, AddressableMessage, ConversationIdentifier, SyncMessage,
        };

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-dfm-attachments-{:016x}",
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
        let conversation = |bytes: Vec<u8>| ConversationIdentifier {
            identifier: Some(
                presage::proto::conversation_identifier::Identifier::ThreadServiceIdBinary(bytes),
            ),
        };
        let target = |bytes: Option<Vec<u8>>, ts: Option<u64>| AddressableMessage {
            author: bytes.map(presage::proto::addressable_message::Author::AuthorServiceIdBinary),
            sent_timestamp: ts,
        };
        let attachment =
            |conversation: Option<ConversationIdentifier>,
             target_message: Option<AddressableMessage>,
             client_uuid: Option<Vec<u8>>,
             fallback_digest: Option<Vec<u8>>,
             fallback_plaintext_hash: Option<Vec<u8>>| AttachmentDelete {
                conversation,
                target_message,
                client_uuid,
                fallback_digest,
                fallback_plaintext_hash,
            };

        let conversation_aci = "9d0e5f1a-2b3c-4d5e-8f60-7a8b9c0d1e2f";
        let author_aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        let client_uuid = "11111111-2222-4333-8444-555555555555";
        let digest = vec![0x00_u8, 0xff, 0x10];
        let plaintext_hash = vec![0xde_u8, 0xad, 0xbe, 0xef];

        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe {
                        message_deletes: vec![MessageDeletes {
                            conversation: Some(conversation(aci(conversation_aci))),
                            messages: vec![target(Some(aci(author_aci)), Some(1_699_999_000_000))],
                        }],
                        conversation_deletes: vec![ConversationDelete {
                            conversation: Some(conversation(aci(conversation_aci))),
                            most_recent_messages: vec![target(
                                Some(aci(author_aci)),
                                Some(1_699_999_000_000),
                            )],
                            is_full_delete: Some(true),
                            most_recent_non_expiring_messages: vec![],
                        }],
                        local_only_conversation_deletes: vec![LocalOnlyConversationDelete {
                            conversation: Some(conversation(aci(conversation_aci))),
                        }],
                        attachment_deletes: vec![
                            // 三腿齐全：逐字验证三种字节转换。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_000))),
                                Some(
                                    presage::libsignal_service::prelude::Uuid::parse_str(
                                        client_uuid,
                                    )
                                    .unwrap()
                                    .as_bytes()
                                    .to_vec(),
                                ),
                                Some(digest.clone()),
                                Some(plaintext_hash.clone()),
                            ),
                            // 仅 digest 腿（空 clientUuid = 缺席）。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_001))),
                                Some(vec![]),
                                Some(digest.clone()),
                                None,
                            ),
                            // 仅 plaintextHash 腿。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_002))),
                                None,
                                None,
                                Some(plaintext_hash.clone()),
                            ),
                            // clientUuid 非 16B（官方 bytesToUuid 拒绝）且
                            // 其余两腿缺席 → 三腿全缺，整条丢。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_003))),
                                Some(vec![0x11; 15]),
                                None,
                                None,
                            ),
                            // 全零 16B = bytesToUuid 的 nil 面，同样丢。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_004))),
                                Some(vec![0; 16]),
                                None,
                                None,
                            ),
                            // conversation 缺席 → 丢（官方 No target conversation）。
                            attachment(
                                None,
                                Some(target(Some(aci(author_aci)), Some(1_699_999_000_005))),
                                None,
                                Some(digest.clone()),
                                None,
                            ),
                            // targetMessage 缺席 → 丢（官方 No target message）。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                None,
                                None,
                                Some(digest.clone()),
                                None,
                            ),
                            // target author 缺席 → 丢（processAddressableMessage 同判）。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(None, Some(1_699_999_000_006))),
                                None,
                                Some(digest.clone()),
                                None,
                            ),
                            // target sentTimestamp 缺席 → 丢（官方严格面，本臂
                            // 不继承 messageDeletes 的缺省 0）。
                            attachment(
                                Some(conversation(aci(conversation_aci))),
                                Some(target(Some(aci(author_aci)), None)),
                                None,
                                Some(digest.clone()),
                                None,
                            ),
                        ],
                    },
                )),
                ..SyncMessage::default()
            }),
            metadata.clone(),
        );
        let envelope = pop_unwrap(
            project_content("acct", &store, &content, &registry, None).await,
            "deleteForMe attachments project",
        )["params"]["envelope"]
            .clone();
        let face = &envelope["syncMessage"]["deleteForMe"];
        // 四臂同信封并存。
        assert!(face["messageDeletes"].as_array().is_some());
        assert!(face["conversationDeletes"].as_array().is_some());
        assert!(face["localOnlyConversationDeletes"].as_array().is_some());
        let attachments = face["attachmentDeletes"].as_array().expect("attachments");
        assert_eq!(attachments.len(), 3, "无效条目逐条丢弃");
        assert_eq!(
            attachments[0],
            json!({
                "conversationId": conversation_aci,
                "targetMessage": {
                    "author": author_aci,
                    "timestamp": 1_699_999_000_000_u64
                },
                "clientUuid": client_uuid,
                "fallbackDigest": base64::engine::general_purpose::STANDARD.encode(&digest),
                "fallbackPlaintextHash": "deadbeef",
            })
        );
        assert_eq!(
            attachments[1],
            json!({
                "conversationId": conversation_aci,
                "targetMessage": {
                    "author": author_aci,
                    "timestamp": 1_699_999_000_001_u64
                },
                "fallbackDigest": base64::engine::general_purpose::STANDARD.encode(&digest),
            })
        );
        assert_eq!(
            attachments[2],
            json!({
                "conversationId": conversation_aci,
                "targetMessage": {
                    "author": author_aci,
                    "timestamp": 1_699_999_000_002_u64
                },
                "fallbackPlaintextHash": "deadbeef",
            })
        );
        assert!(
            attachments[1].get("clientUuid").is_none(),
            "空 clientUuid = 缺席，不投空串"
        );

        // 只有无效 attachmentDeletes → 四臂全空，整信封丢弃。
        let content = Content::from_body(
            ContentBody::SynchronizeMessage(SyncMessage {
                content: Some(sync_message::Content::DeleteForMe(
                    sync_message::DeleteForMe {
                        attachment_deletes: vec![attachment(
                            Some(conversation(aci(conversation_aci))),
                            Some(target(Some(aci(author_aci)), Some(1_699_999_000_007))),
                            Some(vec![]),
                            None,
                            None,
                        )],
                        ..sync_message::DeleteForMe::default()
                    },
                )),
                ..SyncMessage::default()
            }),
            metadata,
        );
        assert!(project_content("acct", &store, &content, &registry, None)
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

    /// 迁移预算常量对齐官方 1h（Signal-Desktop WebAPI.getTransferArchive
    /// `timeout = HOUR`，WebAPI.preload.ts:2391/2397-2437）：60 轮长轮询 ×
    /// 60s 服务端挂起 ≥ 1h；叠加轮间 3s 空转后总预算仍 < 2h（预算放宽但
    /// 不放飞——此前 10 轮 ≈10min 会把弱网/大档案误判「无可迁移」）。
    #[test]
    fn transfer_archive_poll_budget_matches_official_hour() {
        assert_eq!(TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS, 60);
        assert_eq!(TRANSFER_ARCHIVE_MAX_POLLS, 60);
        let hang_seconds =
            u64::from(TRANSFER_ARCHIVE_MAX_POLLS) * TRANSFER_ARCHIVE_POLL_TIMEOUT_SECS;
        assert!(hang_seconds >= 3600, "总预算不得低于官方 1h");
        let sleep_seconds = u64::from(TRANSFER_ARCHIVE_MAX_POLLS - 1) * 3;
        assert!(
            hang_seconds + sleep_seconds < 2 * 3600,
            "预算放宽到官方同级，但不破 2h"
        );
    }

    /// 200 体四分类钉形（官方 TransferArchiveSchema union 对齐，行为对齐
    /// 零拷贝）：{cdn,key} 即 Ready（两分支字段并存时官方 zod union 先试
    /// {cdn,key} 分支，多余字段 strip——同形序）；error 恰为官方两字面量
    /// 之一对应对应变体；其余（非法 JSON/未知 error/cdn·key 缺失或型不对）
    /// 一律 Malformed。
    #[test]
    fn transfer_archive_body_classification() {
        assert_eq!(
            parse_transfer_archive_body(br#"{"cdn":3,"key":"manifold://cb#k"}"#),
            TransferArchiveBody::Ready {
                cdn: 3,
                key: "manifold://cb#k".to_owned()
            }
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"cdn":0,"key":"k","error":"RELINK_REQUESTED"}"#),
            TransferArchiveBody::Ready {
                cdn: 0,
                key: "k".to_owned()
            },
            "官方 zod union 分支序：{{cdn,key}} 齐 = Ready 赢"
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"error":"RELINK_REQUESTED"}"#),
            TransferArchiveBody::RelinkRequested
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"error":"CONTINUE_WITHOUT_UPLOAD"}"#),
            TransferArchiveBody::ContinueWithoutUpload
        );
        let malformed_bodies: [&[u8]; 9] = [
            b"not json",
            br#"{}"#,
            br#"{"cdn":3}"#,
            br#"{"key":"k"}"#,
            br#"{"cdn":"3","key":"k"}"#,
            br#"{"cdn":3.5,"key":"k"}"#,
            br#"{"error":"SOMETHING_ELSE"}"#,
            br#"{"error":123}"#,
            br#"{"error":"relink_requested"}"#,
        ];
        for body in malformed_bodies {
            assert_eq!(
                parse_transfer_archive_body(body),
                TransferArchiveBody::Malformed,
                "「{}」应判 Malformed",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// 轮询循环 200 体处置终态路径：两 error 变体各落 `relink_requested`/
    /// `continue_without_upload` 终态（不再误报 decode_failed），`Malformed`
    /// 维持 `decode_failed`，`Ready` 放行 (cdn,key) 且快照不受扰——快照
    /// 纪律与 `no_archive` 同型（failed 终态保留供桌面诚实呈现）。
    #[test]
    fn transfer_archive_body_terminal_paths() {
        for (body, expected) in [
            (
                TransferArchiveBody::RelinkRequested,
                transfer_progress::failure::RELINK_REQUESTED,
            ),
            (
                TransferArchiveBody::ContinueWithoutUpload,
                transfer_progress::failure::CONTINUE_WITHOUT_UPLOAD,
            ),
            (
                TransferArchiveBody::Malformed,
                transfer_progress::failure::DECODE_FAILED,
            ),
        ] {
            let progress = transfer_progress::TransferProgress::new();
            progress.begin("lk-a", None);
            progress.add("lk-a", 7);
            assert_eq!(handle_transfer_archive_body(&progress, "lk-a", body), None);
            assert_eq!(
                progress.snapshot("lk-a"),
                Some(transfer_progress::TransferSnapshot {
                    phase: transfer_progress::PHASE_FAILED,
                    bytes_read: 7,
                    bytes_total: None,
                    error_code: Some(expected),
                }),
                "终态短码应恰为 {expected}"
            );
        }

        // Ready：放行 (cdn, key) 进 CDN 段，快照保持在途不受扰。
        let progress = transfer_progress::TransferProgress::new();
        progress.begin("lk-a", Some(10));
        assert_eq!(
            handle_transfer_archive_body(
                &progress,
                "lk-a",
                TransferArchiveBody::Ready {
                    cdn: 3,
                    key: "k".to_owned()
                },
            ),
            Some((3, "k".to_owned()))
        );
        assert_eq!(
            progress.snapshot("lk-a").map(|snapshot| snapshot.phase),
            Some(transfer_progress::PHASE_DOWNLOADING)
        );
    }

    /// sendTyping 群腿结局两态分类（失败显形批，2026-10-16）：全部失败 →
    /// AllFailed（词条计数首现序、词表单点）；部分失败/全成功 → Ok 成功面；
    /// 零收件人（空表）不经本单点（服务层已转 NoRecipients，空表落 Ok 臂
    /// 由调用方前置拦截）。失败计数单点（typing_failure_counts）逐臂钉形。
    #[test]
    fn classify_typing_group_outcomes_arms() {
        use presage::libsignal_service::prelude::ServiceError;
        use presage::libsignal_service::sender::MessageSenderError;

        fn failed(
            service_id: ServiceId,
            error: MessageSenderError,
        ) -> (ServiceId, Option<MessageSenderError>) {
            (service_id, Some(error))
        }
        fn ok(service_id: ServiceId) -> (ServiceId, Option<MessageSenderError>) {
            (service_id, None)
        }

        let a = ServiceId::from(presage::libsignal_service::protocol::Aci::from(
            uuid::Uuid::from_u128(1),
        ));
        let b = ServiceId::from(presage::libsignal_service::protocol::Aci::from(
            uuid::Uuid::from_u128(2),
        ));
        let c = ServiceId::from(presage::libsignal_service::protocol::Aci::from(
            uuid::Uuid::from_u128(3),
        ));

        // 全部失败：词条计数聚合（NotFound 两例 + Unauthorized 一例），
        // 首现序钉定（确定性 wire 面）。
        let outcomes = vec![
            failed(a, MessageSenderError::NotFound { service_id: a }),
            failed(
                b,
                MessageSenderError::ServiceError(ServiceError::Unauthorized),
            ),
            failed(c, MessageSenderError::NotFound { service_id: c }),
        ];
        assert_eq!(typing_failure_counts(&outcomes), (3, 0));
        match classify_typing_group_outcomes(outcomes) {
            TypingGroupSendOutcome::AllFailed(words) => assert_eq!(
                words,
                vec![
                    ("UnregisteredFailure", 2),
                    ("AuthorizationFailedFailure", 1)
                ]
            ),
            TypingGroupSendOutcome::Ok => panic!("all-failed must classify AllFailed"),
        }

        // 全部失败（未知网络族）：同词折叠单条。
        let outcomes = vec![
            failed(a, MessageSenderError::NoMessagesToSend),
            failed(b, MessageSenderError::NoMessagesToSend),
        ];
        match classify_typing_group_outcomes(outcomes) {
            TypingGroupSendOutcome::AllFailed(words) => {
                assert_eq!(words, vec![("NetworkFailure", 2)])
            }
            TypingGroupSendOutcome::Ok => panic!("all-failed must classify AllFailed"),
        }

        // 部分失败：成功面（计数单点 (1, 1)——warn 面同源）。
        let outcomes = vec![
            ok(a),
            failed(b, MessageSenderError::NotFound { service_id: b }),
        ];
        assert_eq!(typing_failure_counts(&outcomes), (1, 1));
        assert_eq!(
            classify_typing_group_outcomes(outcomes),
            TypingGroupSendOutcome::Ok
        );

        // 全成功：成功面计数 (0, 2)。
        let outcomes = vec![ok(a), ok(b)];
        assert_eq!(typing_failure_counts(&outcomes), (0, 2));
        assert_eq!(
            classify_typing_group_outcomes(outcomes),
            TypingGroupSendOutcome::Ok
        );
    }

    /// listContacts 非通讯录对端投影（2026-10-23 目标集扩展批）：profiles
    /// 材料在库的非通讯录 uuid 出行（number=uuid 字符串、profile 面在、
    /// name 键缺席）；无材料/材料全空不出行；contacts 集合 uuid 去重不出；
    /// 枚举失败空集 fail-open。
    #[tokio::test]
    async fn non_contact_peer_entries_project_profile_material() {
        use presage::libsignal_service::prelude::Uuid;
        use presage::libsignal_service::profile_name::ProfileName;
        use presage::libsignal_service::proto::Verified;
        use presage::libsignal_service::zkgroup::profiles::ProfileKey;
        use presage::libsignal_service::Profile;
        use presage::model::contacts::Contact;

        let store_dir = std::env::temp_dir().join(format!(
            "kt-engine-noncontact-{:016x}",
            mix_seed_step(mix_seed())
        ));
        let mut store = open_store(&store_dir).await.unwrap();
        let contact = Contact {
            uuid: Uuid::parse_str("00000000-0000-4000-8000-00000000000a").unwrap(),
            phone_number: None,
            name: "Alice".to_owned(),
            verified: Verified::default(),
            profile_key: vec![0u8; 32],
            expire_timer: 0,
            expire_timer_version: 2,
            inbox_position: 0,
            avatar: None,
        };
        store.save_contact(&contact).await.unwrap();
        let named = ProfileKey::create([0x21; 32]);
        let bare = ProfileKey::create([0x22; 32]);
        let blank = ProfileKey::create([0x23; 32]);
        // 非通讯录 peer：材料在库 → 出行。
        let uuid_peer = Uuid::parse_str("00000000-0000-4000-8000-00000000000b").unwrap();
        store.upsert_profile_key(&uuid_peer, named).await.unwrap();
        store
            .save_profile(
                uuid_peer,
                named,
                Profile {
                    name: Some(ProfileName {
                        given_name: "Dave".to_owned(),
                        family_name: None,
                    }),
                    about: None,
                    about_emoji: None,
                    avatar: None,
                    unrestricted_unidentified_access: false,
                },
            )
            .await
            .unwrap();
        // 非通讯录 peer：无 profile 行 → 不出行。
        let uuid_bare = Uuid::parse_str("00000000-0000-4000-8000-00000000000c").unwrap();
        store.upsert_profile_key(&uuid_bare, bare).await.unwrap();
        // 非通讯录 peer：两件全空材料 → 不出行。
        let uuid_blank = Uuid::parse_str("00000000-0000-4000-8000-00000000000d").unwrap();
        store.upsert_profile_key(&uuid_blank, blank).await.unwrap();
        store
            .save_profile(
                uuid_blank,
                blank,
                Profile {
                    name: Some(ProfileName {
                        given_name: "".to_owned(),
                        family_name: None,
                    }),
                    about: None,
                    about_emoji: None,
                    avatar: None,
                    unrestricted_unidentified_access: false,
                },
            )
            .await
            .unwrap();

        let skip_uuids: HashSet<presage::libsignal_service::prelude::Uuid> = store
            .contacts()
            .await
            .unwrap()
            .filter_map(Result::ok)
            .map(|contact| contact.uuid)
            .collect();
        let out = non_contact_profile_entries(&store, &skip_uuids).await;
        assert_eq!(out.len(), 1, "仅材料在库的 peer 出行");
        assert_eq!(out[0]["number"], json!(uuid_peer.to_string()));
        assert!(out[0].get("name").is_none(), "通讯录名键诚实缺席");
        assert_eq!(out[0]["profile"]["givenName"], json!("Dave"));
        assert!(out[0]["profile"].get("familyName").is_none());

        // fail-open 臂：contacts 集合含全部 uuid 时恒空集。
        let all: HashSet<presage::libsignal_service::prelude::Uuid> =
            [contact.uuid, uuid_peer, uuid_bare, uuid_blank]
                .into_iter()
                .collect();
        assert!(non_contact_profile_entries(&store, &all).await.is_empty());
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// listContacts 条目投影（profile 名字刷新批 additive 面，纯函数）：
    /// number 恒在；name/profile 两键非空才出现（no-null）；profile 内
    /// givenName/familyName 单件空 = 该键不在；两件全空 = profile 整键
    /// 不在；键集形状钉定（不得发明）。
    #[test]
    fn project_contact_entry_shapes() {
        // 仅 number（未刷新库零变化：应答逐字节同形）。
        assert_eq!(
            project_contact_entry("+15551234567", "", None),
            json!({
                "number": "+15551234567",
            })
        );

        // contacts 名照旧；profile 材料在 → givenName/familyName 全出。
        assert_eq!(
            project_contact_entry("+15551234567", "Alice", Some(("Ada", Some("Lee")))),
            json!({
                "number": "+15551234567",
                "name": "Alice",
                "profile": { "givenName": "Ada", "familyName": "Lee" },
            })
        );

        // 单件空：given 缺 / family 缺（no-null，缺席键不在）。
        assert_eq!(
            project_contact_entry("+15551234567", "", Some(("", Some("Lee")))),
            json!({
                "number": "+15551234567",
                "profile": { "familyName": "Lee" },
            })
        );
        assert_eq!(
            project_contact_entry("+15551234567", "", Some(("Ada", None))),
            json!({
                "number": "+15551234567",
                "profile": { "givenName": "Ada" },
            })
        );
        assert_eq!(
            project_contact_entry("+15551234567", "", Some(("Ada", Some("")))),
            json!({
                "number": "+15551234567",
                "profile": { "givenName": "Ada" },
            })
        );

        // 两件全空 = profile 整键不在（服务器侧空 profile 名不算材料）。
        assert_eq!(
            project_contact_entry("+15551234567", "", Some(("", None))),
            json!({ "number": "+15551234567" })
        );
        assert_eq!(
            project_contact_entry("+15551234567", "", Some(("", Some("")))),
            json!({ "number": "+15551234567" })
        );
    }

    /// S5 页缓存：open_store 的连接选项钉定 cache_size=-1024（1 MiB/连接）。
    /// sqlx 的 SqliteConnectOptions 无 pragma getter，Debug 形是唯一可直打
    /// 的只读面（pragma 键在 Debug 中恒为 cache_size）。
    #[test]
    fn store_connect_options_pins_sqlite_page_cache() {
        let options = store_connect_options(":memory:").expect("parse in-memory store path");
        let debug = format!("{options:?}");
        assert!(debug.contains("cache_size"), "pragma missing: {debug}");
        assert!(debug.contains("-1024"), "cache size not pinned: {debug}");
    }

    /// S5 关停宽限：默认 10 s；`KT_ENGINE_SHUTDOWN_GRACE_MS=12000` 对齐
    /// 连接器 12 s 预算；空/非数字/零/超上限一律回默认（不阻断启动）。
    #[test]
    fn pipeline_shutdown_grace_parses_env_budget() {
        assert_eq!(PIPELINE_SHUTDOWN_GRACE_DEFAULT_MS, 10_000);
        assert_eq!(parse_pipeline_shutdown_grace(None), None);
        assert_eq!(parse_pipeline_shutdown_grace(Some("")), None);
        assert_eq!(parse_pipeline_shutdown_grace(Some("   ")), None);
        assert_eq!(parse_pipeline_shutdown_grace(Some("0")), None);
        assert_eq!(parse_pipeline_shutdown_grace(Some("abc")), None);
        assert_eq!(parse_pipeline_shutdown_grace(Some("-1")), None);
        assert_eq!(
            parse_pipeline_shutdown_grace(Some(&(PIPELINE_SHUTDOWN_GRACE_MAX_MS + 1).to_string())),
            None
        );
        assert_eq!(
            parse_pipeline_shutdown_grace(Some("12000")),
            Some(Duration::from_millis(12_000))
        );
        assert_eq!(
            parse_pipeline_shutdown_grace(Some(" 10000 ")),
            Some(Duration::from_millis(10_000))
        );
        assert_eq!(
            parse_pipeline_shutdown_grace(Some(&PIPELINE_SHUTDOWN_GRACE_MAX_MS.to_string())),
            Some(Duration::from_millis(PIPELINE_SHUTDOWN_GRACE_MAX_MS))
        );
    }

    // KT (pre-keys gate, §29.6): 闸语义一 —— 注册未完成时接收流不得拨号
    // （串行闸因果序）。事件序向量钉死「register_started → register_done →
    // stream_dial」：闸 pending 期间任何拨号步都是违序；这正是 2026-10-23
    // 09:30 故障链的结构性反面（当时 "receive pipeline spawned" 先于
    // pre-keys 注册完成落账，两条腿并发）。
    #[tokio::test(start_paused = true)]
    async fn prekeys_gate_blocks_stream_dial_until_registration_completes() {
        let events: Arc<StdMutex<Vec<&'static str>>> = Default::default();
        let release = Arc::new(tokio::sync::Notify::new());

        let ev = events.clone();
        let rel = release.clone();
        let register = move || {
            let events = ev.clone();
            let release = rel.clone();
            async move {
                events.lock().unwrap().push("register_started");
                release.notified().await;
                events.lock().unwrap().push("register_done");
                Ok::<(), presage::Error<std::convert::Infallible>>(())
            }
        };

        let gate = run_prekeys_gate(register, Duration::from_secs(30));
        tokio::pin!(gate);
        // 闸 pending 期间：注册未完成，拨号步不得发生。
        for _ in 0..10 {
            assert!(
                futures::poll!(gate.as_mut()).is_pending(),
                "gate released before registration completed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["register_started"],
            "stream dial must not happen while registration is in flight"
        );

        // 注册完成后闸放行；随后的拨号步严格落在 register_done 之后。
        release.notify_waiters();
        let outcome = gate.await;
        assert_eq!(outcome, PreKeysGateOutcome::Registered);
        events.lock().unwrap().push("stream_dial");
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["register_started", "register_done", "stream_dial"]
        );
    }

    // KT (pre-keys gate, §29.6): 闸语义二 —— 注册卡死（在途悬挂）时按预算
    // 超时放行，结局 = Degraded("timeout")（调用方据此落降级 warn），闸
    // 自身不悬死、等待完整预算。
    #[tokio::test(start_paused = true)]
    async fn prekeys_gate_times_out_and_releases_with_degraded_record() {
        let register = || async {
            futures::future::pending::<Result<(), presage::Error<std::convert::Infallible>>>().await
        };
        let started = tokio::time::Instant::now();
        let outcome = run_prekeys_gate(register, Duration::from_millis(50)).await;
        assert_eq!(outcome, PreKeysGateOutcome::Degraded("timeout"));
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "gate must wait the full budget before releasing"
        );
    }
}
