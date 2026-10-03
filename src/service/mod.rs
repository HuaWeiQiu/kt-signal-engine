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

/// startLink 时刻的占位设备名（ADR 0004）：finishLink 拿到用户输入的名字后
/// 改写 store 注册数据，接收管线首推 attributes 时覆盖为真名。
const PLACEHOLDER_DEVICE_NAME: &str = "KT Signal Engine";

/// 出网 UA：官方审计面统一标识（引擎所有直连 REST/WS 共用）。
const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"));

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
        tokio::task::spawn_local(async move {
            match link_flow(store, url_tx, task_dir).await {
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
    /// 标准 base64（signal-cli 同源，connector send 回传同 id）。
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
            }));
        }
        Ok(out)
    }

    pub(crate) async fn send_text_group(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
        message: &str,
    ) -> Result<(u64, Vec<Value>), SendError> {
        if !self.real {
            return Ok((now_ms(), Vec::new()));
        }
        let (mut manager, timestamp) = self.open_group_sender(account, group_id).await?;
        let body = DataMessage {
            timestamp: Some(timestamp),
            body: Some(message.to_owned()),
            ..DataMessage::default()
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

    /// 群组发送公共头：账号/群可解校验 + 时间戳统一此处取（fake 模式各方法
    /// 自行早退，不走此处）。
    async fn open_group_sender(
        self: &Arc<Self>,
        account: &str,
        group_id: &str,
    ) -> Result<(Manager<SqliteStore, Registered>, u64), SendError> {
        let manager = open_registered(self, account)
            .await
            .ok_or(SendError::UnknownAccount)?;
        decode_group_id(group_id).ok_or(SendError::Internal)?;
        Ok((manager, now_ms()))
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
                // contacts 由 presage 自动入库（号码/名字面随之可用）。
                Some(Received::Contacts) => {}
                // 解密失败按事件丢弃（零日志；会话重置治理 P2）。
                Some(Received::DecryptionError(_)) => {}
                Some(Received::Content(content)) => {
                    if let Some(notification) =
                        project_content(number, manager.store(), &content).await
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
/// call/story/null 体跳过。
/// 形状钉定 signal-cli 0.14.8 JSON（connector implementation-plan §4.13/
/// §4.19/§4.20），作者号字段在 contacts 命中时给 E.164，未命中给 ACI 字符串。
async fn project_content(
    account: &str,
    store: &SqliteStore,
    content: &presage::libsignal_service::content::Content,
) -> Option<Value> {
    use presage::libsignal_service::content::ContentBody;

    let envelope_ts = content.metadata.client_timestamp.timestamp_millis().max(0) as u64;
    let mut envelope = json!({
        "sourceUuid": content.metadata.sender.service_id_string(),
        "sourceDevice": u32::from(content.metadata.sender_device),
        "timestamp": envelope_ts,
    });
    if let Some(peer) = describe_peer(store, &content.metadata.sender).await {
        if let Some(number) = peer.number {
            envelope["sourceNumber"] = json!(number);
        }
        if let Some(name) = peer.name {
            envelope["sourceName"] = json!(name);
        }
    }

    match &content.body {
        ContentBody::DataMessage(data_message) => {
            let numbers = resolve_author_numbers(store, data_message).await;
            envelope["dataMessage"] = project_data_message(data_message, &numbers);
        }
        ContentBody::EditMessage(edit) => {
            let data_message = edit.data_message.as_ref()?;
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
            // sent 在 SyncMessage.content oneof 里（read/viewed 等其他面 P2+）。
            let sent = match sync.content.as_ref() {
                Some(presage::proto::sync_message::Content::Sent(sent)) => sent,
                _ => return None,
            };
            let data_message = sent.message.as_ref()?;
            let numbers = resolve_author_numbers(store, data_message).await;
            let mut sent_json = project_data_message(data_message, &numbers);
            sent_json["timestamp"] = json!(sent.timestamp.unwrap_or(envelope_ts));
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
        ContentBody::NullMessage(_)
        | ContentBody::CallMessage(_)
        | ContentBody::StoryMessage(_)
        | ContentBody::DecryptionErrorMessage(_) => return None,
    }

    Some(json!({
        "jsonrpc": "2.0",
        "method": "receive",
        "params": { "account": account, "envelope": envelope },
    }))
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
    out
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
async fn download_transfer_archive(
    store_dir: PathBuf,
    aci: presage::libsignal_service::prelude::Uuid,
    backup_key: [u8; 32],
    credentials: ServiceCredentials,
) {
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
                Err(_) => return,
            };
            let response = match response.send().await {
                Ok(response) => response,
                Err(_) => return,
            };
            match response.status().as_u16() {
                200 => {
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes,
                        Err(_) => return,
                    };
                    match serde_json::from_slice(&bytes) {
                        Ok(value) => break value,
                        Err(_) => return,
                    }
                }
                204 if polls >= 9 => return,
                204 => {
                    polls += 1;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                _ => return,
            }
        }
    };
    let (Some(cdn), Some(key)) = (
        manifest["cdn"].as_u64().map(|value| value as u32),
        manifest["key"].as_str().map(str::to_owned),
    ) else {
        return;
    };

    let response = match push.request(
        Method::GET,
        Endpoint::cdn(cdn, format!("attachments/{key}")),
        HttpAuthOverride::NoOverride,
    ) {
        Ok(builder) => builder,
        Err(_) => return,
    };
    let bytes = match response.send().await {
        Ok(response) => match response.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => return,
        },
        Err(_) => return,
    };

    let archive_path = store_dir.join("transfer-archive.bin");
    if std::fs::write(&archive_path, &bytes).is_err() {
        return;
    }
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
fn decode_group_id(group_id: &str) -> Option<Vec<u8>> {
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
    use presage::proto::attachment_pointer::Flags;
    use presage::proto::body_range::AssociatedValue;
    use presage::proto::{data_message, AttachmentPointer, BodyRange, DataMessage};

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

    fn uuid_bytes(text: &str) -> Vec<u8> {
        presage::libsignal_service::prelude::Uuid::parse_str(text)
            .expect("uuid parses")
            .into_bytes()
            .to_vec()
    }
}
