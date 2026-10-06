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
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, Notify};

use crate::state::EngineState;

pub(crate) mod avatar;
pub(crate) mod backup_task;
pub(crate) mod groups;
pub(crate) mod profile;
pub(crate) mod report;
pub(crate) mod stickers;
pub(crate) mod storage_sticker;
pub(crate) mod storage_sync;
pub(crate) mod storage_write;

/// startLink 时刻的占位设备名（ADR 0004）：finishLink 拿到用户输入的名字后
/// 改写 store 注册数据，接收管线首推 attributes 时覆盖为真名。
const PLACEHOLDER_DEVICE_NAME: &str = "KT Signal Engine";

/// 出网 UA：官方审计面统一标识（引擎所有直连 REST/WS 共用）。
pub(crate) const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"));

/// 回执 timestamps 每帧上限：官方 sendReceipts.preload.ts CHUNK_SIZE=100，
/// 超出按官方分帧继续发（非 fail-closed）。
const RECEIPT_CHUNK_SIZE: usize = 100;

/// send 扩面 payload（compat §3 send 行，键名与 connector 上游同名直投）。
pub(crate) struct SendQuote {
    pub(crate) timestamp: u64,
    pub(crate) author: String,
}

pub(crate) struct SendPreview {
    pub(crate) url: String,
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    /// previewImage 数据 URI（data URI 同附件形状）；随同批 CDN 上行。
    pub(crate) image: Option<String>,
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
    Internal,
}

#[derive(Debug)]
pub(crate) enum UserStatusError {
    /// 账号在服务器侧已解绑（401）：connector 1.21 靠错误原文识别解绑态。
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

pub(crate) struct Service {
    data_dir: PathBuf,
    real: bool,
    state: Arc<AsyncMutex<EngineState>>,
    /// receive 通知出口（与 JSON-RPC 响应同一 stdout writer 队列）。
    outbound: mpsc::UnboundedSender<Value>,
    inner: AsyncMutex<Inner>,
    /// 运行中的接收管线（账号 → 停机旗），delete 时通知退出。
    pipelines: AsyncMutex<HashMap<String, watch::Sender<bool>>>,
    /// 备份导出任务注册表（进程内存，backup_task 模块契约 §10.3）。
    backup_tasks: Arc<backup_task::BackupTaskRegistry>,
    /// 已终止群登记表（进程内存，groups 模块：receive 投影 → send 面拦截）。
    terminated_groups: groups::TerminatedGroups,
    /// transfer_archive 下载进度快照（进程内存，不落盘；§4.38 progress 面）。
    transfer_progress: transfer_progress::TransferProgress,
}

struct Inner {
    pending: Option<PendingLink>,
    fake_pending_uri: Option<String>,
}

impl Service {
    pub(crate) fn new(
        data_dir: PathBuf,
        state: Arc<AsyncMutex<EngineState>>,
        outbound: mpsc::UnboundedSender<Value>,
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
            terminated_groups: groups::TerminatedGroups::new(),
            transfer_progress: transfer_progress::TransferProgress::new(),
        })
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
        // 管线，管线首步 set_account_attributes 把用户输入的名字推上服务器。
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
        view_once: bool,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let timestamp = now_ms();
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
        let mut results = Vec::new();
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
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
                        timestamp,
                    )
                    .await?
                }
            };
            manager
                .send_message(service_id, body, timestamp)
                .await
                .map_err(|_| SendError::Internal)?;
            results.push(json!({
                "recipientNumber": number,
                "success": { "timestamp": timestamp },
            }));
        }
        Ok((timestamp, results))
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let timestamp = now_ms();
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            let body = DataMessage {
                timestamp: Some(timestamp),
                delete: Some(presage::proto::data_message::Delete {
                    target_sent_timestamp: Some(target_timestamp),
                }),
                ..DataMessage::default()
            };
            manager
                .send_message(service_id, body, timestamp)
                .await
                .map_err(|_| SendError::Internal)?;
        }
        Ok(())
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let timestamp = now_ms();
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
                .send_message(service_id, body, timestamp)
                .await
                .map_err(|_| SendError::Internal)?;
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
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
        dispatch_group(&mut manager, group_id, body, timestamp).await
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
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
        dispatch_group(&mut manager, group_id, body, timestamp).await
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
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
        dispatch_group(&mut manager, group_id, body, timestamp).await
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let timestamp = now_ms();
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
            manager
                .send_message(service_id, body(aci_bytes.clone(), timestamp), timestamp)
                .await
                .map_err(|_| SendError::Internal)?;
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
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
                .map_err(|_| SendError::Internal)?;
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let service_id = resolve_recipient(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        for chunk in timestamps.chunks(RECEIPT_CHUNK_SIZE) {
            let timestamp = now_ms();
            let body = ContentBody::ReceiptMessage(presage::proto::ReceiptMessage {
                r#type: Some(receipt_type as i32),
                timestamp: chunk.to_vec(),
            });
            manager
                .send_message(service_id, body, timestamp)
                .await
                .map_err(|_| SendError::Internal)?;
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
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
            .map_err(|_| SendError::Internal)?;
        Ok(())
    }

    /// sendCallMessage（通话消息面，KT 扩展方法）：官方 Content.callMessage=3
    /// 信封。modern 面构造 CallMessage{opaque{data, urgency}}（destinationDeviceId
    /// 在场才写）——opaque 为 Signal-RTC 层信令负载，引擎不解析只透传（桌面
    /// callingMessage 面同构：offer/answer/ice/hangup/busy 都封装在 opaque
    /// 内）；legacy 面按官方 proto 裸帧族构造——`CallMessage{busy{id}}` /
    /// `CallMessage{hangup{id, type, deviceId?}}`，无 opaque 字段（官方桌面
    /// callingMessageToProto 同款：busy={id}、hangup={id,type}；RingRTC
    /// 2.72.1 出向 hangup/busy 实证无 opaque 材料；signal-cli 0.14.7/0.14.8
    /// CallManager.sendBusyViaSignal/sendHangupViaSignal 同构裸帧族）。收件人
    /// 号码/UUID 双形态解析（send 系同口径）。发送面偏差（compat §5 通话消息
    /// 块登记）：官方 call 消息 online=true 不排队，presage send_message 硬编码
    /// online_only=false（上游 TODO issue #252）——引擎发出的 call 信封离线可
    /// 排队，属可接受偏差；CallMessage 无 sent transcript（上游
    /// create_multi_device_sent_transcript_content 对 CallMessage 返回 None），
    /// 本机其他设备不同步自发 call 记录。
    pub(crate) async fn send_call_message(
        self: &Arc<Self>,
        account: &str,
        recipient: &str,
        payload: SendCallMessage,
    ) -> Result<u64, SendError> {
        if !self.real {
            return Ok(now_ms());
        }
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let service_id = resolve_recipient(&manager, recipient)
            .await
            .ok_or(SendError::Internal)?;
        let timestamp = now_ms();
        let body = match payload.face {
            CallMessageFace::Opaque { data, urgency } => presage::proto::CallMessage {
                opaque: Some(presage::proto::call_message::Opaque {
                    data: Some(data),
                    urgency: Some(urgency as i32),
                }),
                destination_device_id: payload.destination_device_id,
                ..presage::proto::CallMessage::default()
            },
            CallMessageFace::LegacyBusy { id } => presage::proto::CallMessage {
                busy: Some(presage::proto::call_message::Busy { id: Some(id) }),
                destination_device_id: payload.destination_device_id,
                ..presage::proto::CallMessage::default()
            },
            CallMessageFace::LegacyHangup {
                id,
                kind,
                device_id,
            } => presage::proto::CallMessage {
                hangup: Some(presage::proto::call_message::Hangup {
                    id: Some(id),
                    r#type: Some(kind as i32),
                    device_id,
                }),
                destination_device_id: payload.destination_device_id,
                ..presage::proto::CallMessage::default()
            },
        };
        manager
            .send_message(service_id, body, timestamp)
            .await
            .map_err(|_| SendError::Internal)?;
        Ok(timestamp)
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
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

    /// listGroups：store 内已同步群组（本机所在群才会在库）。`isMember` 恒
    /// true（入库即成员；被移出群的本地残留清理 P3）；群组 id = master key
    /// 标准 base64（signal-cli 同源，connector send 回传同 id）。`terminated`
    /// 位 = 进程内终止登记（groups 模块；presage Group 模型无该字段，读面
    /// 以登记表为准，compat §5 终止批登记）。
    pub(crate) async fn list_groups(
        self: &Arc<Self>,
        account: &str,
    ) -> Result<Vec<Value>, SendError> {
        if !self.real {
            return Ok(Vec::new());
        }
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let numbers = contacts_number_map(manager.store()).await;
        let groups = manager
            .store()
            .groups()
            .await
            .map_err(|_| SendError::Internal)?;
        let mut out = Vec::new();
        for (master_key, group) in groups.filter_map(Result::ok) {
            use base64::Engine as _;
            let id = base64::engine::general_purpose::STANDARD.encode(master_key);
            let members: Vec<Value> = group
                .members
                .iter()
                .map(|member| {
                    let sid = ServiceId::from(member.aci);
                    let number = numbers
                        .get(&sid.service_id_string().to_owned())
                        .cloned()
                        .unwrap_or_else(|| sid.service_id_string().to_owned());
                    json!({ "number": number })
                })
                .collect();
            out.push(json!({
                "id": id,
                "name": group.title,
                "isMember": true,
                "members": members,
                "terminated": self.terminated_groups.contains(&id),
            }));
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
        view_once: bool,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
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
                    timestamp,
                )
                .await?
            }
        };
        dispatch_group(&mut manager, group_id, body, timestamp).await?;
        Ok((timestamp, Vec::new()))
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            delete: Some(presage::proto::data_message::Delete {
                target_sent_timestamp: Some(target_timestamp),
            }),
            ..DataMessage::default()
        };
        dispatch_group(&mut manager, group_id, body, timestamp).await
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
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
        dispatch_group(&mut manager, group_id, body, timestamp).await
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
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
        let body = ContentBody::TypingMessage(presage::proto::TypingMessage {
            timestamp: Some(timestamp),
            action: Some(if stop {
                presage::proto::typing_message::Action::Stopped as i32
            } else {
                presage::proto::typing_message::Action::Started as i32
            }),
            group_id: None,
        });
        let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
        manager
            .send_message_to_group(&master_key, body, timestamp)
            .await
            .map_err(|_| SendError::Internal)
    }

    /// 群组发送公共头：账号/群可解校验 + 终止面硬拒（官方
    /// shouldSendToConversation terminated 位同语义——已终止群一律拒发）+
    /// 时间戳统一此处取（fake 模式各方法自行早退，不走此处）。
    async fn open_group_sender(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
    ) -> Result<(Manager<SqliteStore, Registered>, u64), SendError> {
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        decode_group_id(group_id).ok_or(SendError::Internal)?;
        if self.send_target_terminated(account, group_id).await {
            return Err(SendError::TargetTerminated);
        }
        Ok((manager, now_ms()))
    }

    /// 发送面终止判定：进程内登记表（本进程见过 terminated change/写面
    /// 成功）先行；real 模式下服务器群态真值兜底（读面失败不阻塞发送面
    /// ——网络故障不误伤既有群会话，终止真值以登记表 + 服务器裁决为准）。
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
        matches!(
            groups::fetch_group_state_for_send(&context, &master_key).await,
            Some(true)
        )
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

    /// updateGroup {expiration}（群消失计时器，KT 扩展方法）：群态读面
    /// （revision 面失败退化 0）→ 群凭据 → `PATCH v2/groups` 上传 expiration
    /// change（modify_disappearing_message_timer action，0 = 关闭）。fake
    /// 模式成功形状桩（terminateGroup 同款纪律）。
    pub(crate) async fn update_group_expiration(
        self: &Arc<Self>,
        account: &str,
        master_key: Vec<u8>,
        expiration_secs: u32,
    ) -> Result<(), groups::TerminateError> {
        if !self.real {
            return Ok(());
        }
        let context = storage_context(self, account).await?;
        groups::update_group_expiration(&context, &master_key, expiration_secs).await
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
        let manager = open_registered(self, account)
            .await
            .ok_or(UserStatusError::Internal)?;
        match manager.whoami().await {
            Ok(_) => Ok(()),
            Err(presage::Error::ServiceError(
                presage::libsignal_service::prelude::ServiceError::Unauthorized,
            )) => Err(UserStatusError::Unbound),
            Err(_) => Err(UserStatusError::Internal),
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
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
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
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
            .map_err(|_| SendError::Internal)?;
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
    })
}

// ---------------------------------------------------------------------------
// 接收管线与投影
// ---------------------------------------------------------------------------

async fn receive_pipeline(
    service: &Service,
    number: String,
    dir: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) {
    // 存量联系人补同步的一次性纪律：60s 内断连重开不重复轰主设备。
    let mut last_contacts_sync: Option<std::time::Instant> = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut serve_rx = shutdown.clone();
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = serve_account_stream(
                service,
                &number,
                &dir,
                &mut serve_rx,
                &mut last_contacts_sync,
            ) => {}
        }
        // 流断开：短暂退避后重开（重连治理 P2 精化）。
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(Duration::from_secs(3)) => {}
        }
    }
}

async fn serve_account_stream(
    service: &Service,
    number: &str,
    dir: &Path,
    shutdown: &mut watch::Receiver<bool>,
    last_contacts_sync: &mut Option<std::time::Instant>,
) {
    let Ok(store) = open_store(dir).await else {
        return;
    };
    let Ok(mut manager) = Manager::load_registered(store).await else {
        return;
    };
    let Ok(mut stream) = manager.receive_messages().await else {
        return;
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
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
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
            item = stream.next() => match item {
                // QueueEmpty：历史队列排空，流继续保持。
                Some(Received::QueueEmpty) => {}
                // contacts 由 presage 自动入库（号码/名字面随之可用）。
                Some(Received::Contacts) => {}
                // 解密失败按事件丢弃（零日志；会话重置治理 P2）。
                Some(Received::DecryptionError(_)) => {}
                Some(Received::Content(content)) => {
                    for notification in
                        project_content(
                            number,
                            manager.store(),
                            &content,
                            &service.terminated_groups,
                        )
                        .await
                    {
                        let _ = service.outbound.send(notification);
                    }
                }
                None => return,
            }
        }
    }
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
    // envelope 级 reportSpamToken 经 presage Content 转换丢弃（Metadata 不
    // 携带），token 不在本投影——compat §5 reportSpam 块登记。
    if let Some(server_guid) = content.metadata.server_guid.as_ref() {
        envelope["serverGuid"] = json!(server_guid.to_string());
    }
    if let Some(peer) = describe_peer(store, &content.metadata.sender).await {
        if let Some(number) = peer.number {
            envelope["sourceNumber"] = json!(number);
        }
        if let Some(name) = peer.name {
            envelope["sourceName"] = json!(name);
        }
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
            envelope["typingMessage"] = json!({
                "action": action,
                "timestamp": typing.timestamp.unwrap_or(envelope_ts),
            });
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
            // 等其他面 P2+）。
            match sync.content.as_ref() {
                Some(presage::proto::sync_message::Content::Sent(sent)) => {
                    let Some(data_message) = sent.message.as_ref() else {
                        return Vec::new();
                    };
                    let numbers = resolve_author_numbers(store, data_message).await;
                    let mut sent_json = project_data_message(data_message, &numbers);
                    sent_json["timestamp"] = json!(sent.timestamp.unwrap_or(envelope_ts));
                    // 消失消息 E 批：Sent.expirationStartTimestamp=4（官方多
                    // 设备的精确起算同步位，官方 1:1 已读起算语义的线面对应）。
                    // 0 无起算语义不投（同 history 面 0 过滤口径）；收侧
                    // DataMessage 在钉定 rev 无同名字段（proto 缺口，§5）。
                    if let Some(start) = sent_expiration_start(sent) {
                        sent_json["expirationStartTimestamp"] = json!(start);
                    }
                    match (
                        sent.destination_e164.as_deref(),
                        sent.parse_destination_service_id(),
                    ) {
                        (Some(e164), _) => sent_json["destinationNumber"] = json!(e164),
                        (None, Some(service_id)) => {
                            sent_json["destinationUuid"] = json!(service_id.service_id_string());
                        }
                        (None, None) => {}
                    }
                    envelope["syncMessage"] = json!({ "sentMessage": sent_json });
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
                _ => return Vec::new(),
            }
        }
        ContentBody::CallMessage(call) => {
            let Some(projection) = project_call_message(call) else {
                return Vec::new();
            };
            envelope["callMessage"] = projection;
        }
        ContentBody::NullMessage(_)
        | ContentBody::StoryMessage(_)
        | ContentBody::DecryptionErrorMessage(_) => return Vec::new(),
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
            if let Some(image) = preview.image.as_ref() {
                value["image"] = project_attachment_pointer(image);
            }
            value
        })
        .collect();
    if !previews.is_empty() {
        out["previews"] = json!(previews);
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

/// 附件只投影元数据（connector §4.13/§6.7 媒体边界：字节永不下载）。
/// `id` = AttachmentPointer.client_uuid（hyphenated UUID，signal-cli `id` 位）。
fn project_attachment_pointer(pointer: &presage::proto::AttachmentPointer) -> Value {
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
    let voice_note = pointer
        .flags
        .is_some_and(|flags| flags & Flags::VoiceMessage as u32 != 0);
    value["isVoiceNote"] = json!(voice_note);
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

async fn dispatch_group(
    manager: &mut Manager<SqliteStore, Registered>,
    group_id: &str,
    body: DataMessage,
    timestamp: u64,
) -> Result<(), SendError> {
    let master_key = decode_group_id(group_id).ok_or(SendError::Internal)?;
    manager
        .send_message_to_group(&master_key, body, timestamp)
        .await
        .map_err(|_| SendError::Internal)
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
        .map_err(|_| SendError::Internal)?;
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
    if let Some(preview) = preview {
        dm.preview = vec![presage::proto::Preview {
            url: Some(preview.url.clone()),
            title: preview.title.clone(),
            description: preview.description.clone(),
            image: preview_image,
            ..presage::proto::Preview::default()
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

/// 被引消息正文找回：author ACI + upstream 时间戳定位 presage 线程消息，
/// 直聊/群组 thread 都命中；任何环节失败 = 引用缺 text（官方降级同款）。
async fn quoted_text(store: &SqliteStore, author_aci: &str, timestamp: u64) -> Option<String> {
    let sender = ServiceId::parse_from_service_id_string(author_aci)?;
    let thread = store
        .thread_for_sender_and_timestamp(&sender, timestamp)
        .await
        .ok()??;
    let content = store.message(&thread, timestamp).await.ok()??;
    match content.body {
        ContentBody::DataMessage(dm) => dm.body,
        _ => None,
    }
}

/// quote 组装（文本/贴纸分支共用单点，compat §3 send 行 quote 语义）：
/// id=被引消息 upstream 时间戳、author=原作者 ACI（号码经
/// resolve_author_aci，不可解析 fail-closed = SendError::Internal，与
/// mentions 同口径）；text 从本机 store 找回——presage 官方接收路径已落库
/// 的消息（引擎无新增落库面），找不到按官方/上游同款降级缺省 text（收端
/// 仅少预览文案，协议面合法）。
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
        ..presage::proto::data_message::Quote::default()
    };
    if let Some(text) = quoted_text(store, &author_aci, quote.timestamp).await {
        q.text = Some(text);
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
        .map_err(|_| SendError::Internal)?;
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

/// 阅后即焚媒体族判定（E1/E2 共用）：`image/*|video/*` 前缀族。官方桌面
/// isValidTapToView 用 Chrome 支持集枚举表（GoogleChrome.std.ts：avif/bmp/
/// gif/jpeg/webp/png/apng/ico 系 + mp4/ogg/webm），引擎 metadata-only 无渲染
/// 面按族宽判——官方集外但手机端合法的 video/quicktime 等不被误标 invalid
/// （差异登记 compat §5）。
fn is_view_once_media(content_type: &str) -> bool {
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

async fn open_registered(
    service: &Arc<Service>,
    account: &str,
) -> Option<Manager<SqliteStore, Registered>> {
    let rel = {
        let state = service.state.lock().await;
        state
            .account_dir(account)
            .map(|rel| service.data_dir.join(rel))?
    };
    let store = open_store(&rel).await.ok()?;
    Manager::load_registered(store).await.ok()
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
    use presage::proto::{data_message, AttachmentPointer, BodyRange, DataMessage};
    use prost::Message as _;

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

    /// pinMessage=27/unpinMessage=28/adminDelete=29 投影（官方
    /// processDataMessage.preload.ts processPinMessage 行为对齐）。
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
        let legacy =
            build_text_data_message(&store, "plain", None, None, &[], &[], Vec::new(), None, 1)
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
                1,
            )
            .await
            .is_err(),
            "NONE rejected at build layer too"
        );
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
    /// 不发明）；token 不透投影（presage Content 转换丢弃 envelope 级
    /// reportSpamToken，compat §5 reportSpam 块登记的已知边界）。基础信封位
    /// （sourceUuid/sourceDevice/timestamp）同批防回归。
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
        assert_eq!(
            envelope["dataMessage"]["groupInfo"]["groupId"],
            json!(group_id)
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
        assert_eq!(
            envelope["dataMessage"]["groupInfo"]["groupId"],
            json!(group_id)
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
}
