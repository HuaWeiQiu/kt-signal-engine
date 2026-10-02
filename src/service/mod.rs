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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures::StreamExt;
use presage::libsignal_service::configuration::SignalServers;
use presage::libsignal_service::content::ContentBody;
use presage::manager::Manager;
use presage::manager::Registered;
use presage::model::identity::OnNewIdentity;
use presage::model::messages::Received;
use presage::proto::DataMessage;
use presage::store::{ContentsStore, StateStore};
use presage_store_sqlite::SqliteStore;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, Notify};

use crate::state::EngineState;

/// startLink 时刻的占位设备名（ADR 0004）：finishLink 拿到用户输入的名字后
/// 改写 store 注册数据，接收管线首推 attributes 时覆盖为真名。
const PLACEHOLDER_DEVICE_NAME: &str = "KT Signal Engine";

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

pub(crate) struct Service {
    data_dir: PathBuf,
    real: bool,
    state: Arc<AsyncMutex<EngineState>>,
    /// receive 通知出口（与 JSON-RPC 响应同一 stdout writer 队列）。
    outbound: mpsc::UnboundedSender<Value>,
    inner: AsyncMutex<Inner>,
    /// 运行中的接收管线（账号 → 停机旗），delete 时通知退出。
    pipelines: AsyncMutex<HashMap<String, watch::Sender<bool>>>,
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
        })
    }

    /// 启动时为每个真实账号拉起接收管线（`--receive-mode on-start` 语义）。
    pub(crate) async fn spawn_receive_pipelines(self: &Arc<Self>) {
        if !self.real {
            return;
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
        tokio::task::spawn_local(async move {
            let linked = Manager::link_secondary_device(
                store,
                SignalServers::Production,
                PLACEHOLDER_DEVICE_NAME.to_owned(),
                url_tx,
            )
            .await;
            match linked {
                Ok(manager) => {
                    let number = manager
                        .store()
                        .load_registration_data()
                        .await
                        .ok()
                        .flatten()
                        .map(|data| e164_string(&data.phone_number));
                    match number {
                        Some(number) => task_slot.complete(Ok(number)),
                        None => {
                            let _ = std::fs::remove_dir_all(&task_dir);
                            task_slot.complete(Err(()));
                        }
                    }
                }
                Err(_) => {
                    let _ = std::fs::remove_dir_all(&task_dir);
                    task_slot.complete(Err(()));
                }
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

    pub(crate) async fn send_text(
        self: &Arc<Self>,
        account: &str,
        recipients: &[String],
        message: &str,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        let mut manager = manager;
        let timestamp = now_ms();
        let mut results = Vec::new();
        for number in recipients {
            let service_id = resolve_recipient(&manager, number)
                .await
                .ok_or(SendError::Internal)?;
            let body = DataMessage {
                timestamp: Some(timestamp),
                body: Some(message.to_owned()),
                ..DataMessage::default()
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
        // 协议字段是 targetAuthorAci（UUID）：经 contacts 映射解析号码
        // （ADR 0004）；冷启动未命中按内部失败，不透传号码。
        let target_aci = find_contact_by_number(&manager, target_author)
            .await
            .map(|contact| contact.uuid.to_string())
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
    loop {
        if *shutdown.borrow() {
            return;
        }
        let mut serve_rx = shutdown.clone();
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = serve_account_stream(service, &number, &dir, &mut serve_rx) => {}
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
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            item = stream.next() => match item {
                // QueueEmpty：历史队列排空，流继续保持。
                Some(Received::QueueEmpty) => {}
                // contacts 由 presage 自动入库；联系人/群组表面 P2。
                Some(Received::Contacts) => {}
                // 解密失败按事件丢弃（零日志；会话重置治理 P2）。
                Some(Received::DecryptionError(_)) => {}
                Some(Received::Content(content)) => {
                    if let Some(notification) = project_content(number, &content) {
                        let _ = service.outbound.send(notification);
                    }
                }
                None => return,
            }
        }
    }
}

/// P1b 投影边界（compat §3 receive 行）：仅直聊 dataMessage 文本面
/// （sourceUuid/sourceDevice/timestamp + dataMessage{timestamp,message}）。
/// 群组消息整体跳过（无 groupInfo 投影会误路由为直聊，P2/P3 补齐）；
/// typing/receipt/sent-sync/edit 等非 DataMessage 体跳过（P2/P4）。
fn project_content(
    account: &str,
    content: &presage::libsignal_service::content::Content,
) -> Option<Value> {
    let presage::libsignal_service::content::ContentBody::DataMessage(data_message) = &content.body
    else {
        return None;
    };
    if data_message.group_v2.is_some() {
        return None;
    }
    let fallback_ts = content.metadata.client_timestamp.timestamp_millis().max(0) as u64;
    let envelope = json!({
        "sourceUuid": content.metadata.sender.service_id_string(),
        "sourceDevice": u32::from(content.metadata.sender_device),
        "timestamp": fallback_ts,
        "dataMessage": {
            "timestamp": data_message.timestamp.unwrap_or(fallback_ts),
            "message": data_message.body,
        },
    });
    Some(json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": { "account": account, "envelope": envelope },
    }))
}

// ---------------------------------------------------------------------------
// real 模式共用件
// ---------------------------------------------------------------------------

async fn open_store(dir: &Path) -> Result<SqliteStore, ()> {
    std::fs::create_dir_all(dir).map_err(|_| ())?;
    let url = url::Url::from_file_path(dir).map_err(|_| ())?;
    SqliteStore::open(url.as_str(), OnNewIdentity::Trust)
        .await
        .map_err(|_| ())
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

/// 号码 → ServiceId：contacts 同步入库的 number→uuid 映射（链接后手机端
/// 立即推送）。冷启动窗口内未命中返回 None（发送按内部失败处理，compat §5）。
async fn resolve_recipient(
    manager: &Manager<SqliteStore, Registered>,
    number: &str,
) -> Option<presage::libsignal_service::protocol::ServiceId> {
    let contact = find_contact_by_number(manager, number).await?;
    Some(presage::libsignal_service::protocol::ServiceId::Aci(
        contact.uuid.into(),
    ))
}

async fn find_contact_by_number(
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
fn e164_string(phone: &presage::libsignal_service::prelude::phonenumber::PhoneNumber) -> String {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
