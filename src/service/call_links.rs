// SPDX-License-Identifier: AGPL-3.0-only

//! call-link 凭据材料面（KT 扩展，P4c 切一）：双族群凭据缓存 + 五种通话
//! 材料链。契约与官方锚点逐条记档 = `docs/jsonrpc-compat.md` §3 calls 族
//! 五行 + §5「call-link 凭据材料面」块（2026-10-09）。
//!
//! 官方行为锚（Signal-Desktop 实读，AGPL 只读参照零拷贝；钉 libsignal
//! v0.99.0 对位的 legacy 凭据族形状 = 28a0430c59「Upgrade libsignal to
//! 0.101.2」2026-09-10 轮换前形状——轮换后 URL 带 `v101=true` 触发服务器
//! 签发 V1 Standard 族凭据，v0.99.0 zkgroup 不可解，引擎一律不带）：
//! - 凭据拉取 = `GET v1/certificate/auth/group?redemptionStartSeconds={秒}&
//!   redemptionEndSeconds={秒}&zkcCredential=true`（WebAPI.preload.ts 轮换前
//!   :4542-4564；`pniAsServiceId=true` 为引擎既有参数保留，legacy 族
//!   receive 面同源），响应 `{pni?, credentials[], callLinkAuthCredentials[]}`
//!   （解构位 groupCredentialFetcher.preload.ts:205-209；字段在轮换提交
//!   background.preload.ts 清缓存 hunk（isBeforeVersion('v8.29.0-beta.1')）
//!   证实轮换前已存在，现树型别 = WebAPI.preload.ts:1035-1039）。
//! - 窗口/缓存节奏 = getDatesForRequest（:336-362）逐字对位：today→+6d、
//!   增量 top-up（last+1d）、last>=sixDaysOut 不拉；双族各算窗取
//!   min(start)/max(end) 合并（maybeFetchNewCredentials :175-196）；合并清
//!   <today（:285-301）。官方 4h 主动定时器（:77）不移植——引擎无后台调度面，
//!   每次材料调用先跑同一 maybeFetch 逻辑（使用时点新鲜度与官方等价：官方
//!   fetchMembershipProof 前亦调 maybeFetchNewCredentials，
//!   groups.preload.ts:1789）。
//! - 材料链：linkPresentation = util/callLinks/zkgroup.preload.ts:16-49；
//!   createLinkAuth = calling.preload.ts:860-940 凭据段 + WebAPI
//!   callLinkCreateAuth :3439-3453；groupProof = groups.preload.ts:1782-1806
//!   → getExternalGroupCredential :4566-4586；groupMembers = getMembershipList
//!   :7611-7632；endorsementsPublicKey = server_params.rs:348 派生。
//!
//! 纪律：凭据与 presentation 字节只经内存转发，零日志、零落盘（relay-api-v2
//! §5 :95-97）；缓存进程内存 per-account（ACI 键控）不跨重启；rootKey 生成/
//! roomId 派生/adminPasskey 皆 RingRTC 侧职责，引擎只收成品字节。

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::protocol::{Aci, Pni};
use presage::libsignal_service::push_service::{HttpAuth, HttpAuthOverride};
use prost::Message as _;
use zkgroup::api::call_links::{
    CallLinkAuthCredential, CallLinkSecretParams, CreateCallLinkCredentialRequestContext,
    CreateCallLinkCredentialResponse,
};
use zkgroup::api::generic_server_params::GenericServerPublicParams;
use zkgroup::auth::{AuthCredentialWithPni, AuthCredentialWithPniResponse};
use zkgroup::groups::{GroupMasterKey, GroupSecretParams};
use zkgroup::{ServerPublicParams, Timestamp};

use super::groups::TerminateError;
use super::storage_sync::StorageSyncError;
use super::{zkgroup_consts, StorageContext};

/// 材料面错误族（TerminateError 同枚举同口径，compat §3 calls 族行）：参数面
/// 坏形状在 dispatch 臂已拦（-32603），UnknownAccount/UnknownGroup 折叠
/// -32603；Failed 折叠 `-1` + `CALL_CREDENTIALS_FAILED` + data.status 诚实
/// 透传；Unavailable 折叠 `-1` + `CALL_CREDENTIALS_UNAVAILABLE`（retryable
/// 语义）。
#[derive(Debug)]
pub(crate) enum CallCredentialError {
    UnknownAccount,
    UnknownGroup,
    Failed { status: Option<u16> },
    Unavailable,
}

impl From<StorageSyncError> for CallCredentialError {
    fn from(value: StorageSyncError) -> Self {
        match value {
            StorageSyncError::UnknownAccount => CallCredentialError::UnknownAccount,
            _ => CallCredentialError::Unavailable,
        }
    }
}

impl From<TerminateError> for CallCredentialError {
    fn from(value: TerminateError) -> Self {
        match value {
            TerminateError::UnknownAccount => CallCredentialError::UnknownAccount,
            TerminateError::UnknownGroup => CallCredentialError::UnknownGroup,
            TerminateError::Failed { status } => CallCredentialError::Failed { status },
            // 群头像上行链/链接族错误不经 call 链（变体穷尽性）；同按
            // retryable 网络面折叠。
            TerminateError::AvatarUpload => CallCredentialError::Unavailable,
            TerminateError::Unavailable => CallCredentialError::Unavailable,
            TerminateError::InviteLinkUnset => CallCredentialError::Unavailable,
        }
    }
}

impl From<CallCredentialError> for TerminateError {
    fn from(value: CallCredentialError) -> Self {
        match value {
            CallCredentialError::UnknownAccount => TerminateError::UnknownAccount,
            CallCredentialError::UnknownGroup => TerminateError::UnknownGroup,
            CallCredentialError::Failed { status } => TerminateError::Failed { status },
            CallCredentialError::Unavailable => TerminateError::Unavailable,
        }
    }
}

/// fake 模式形状桩（固定占位字节/串，**非有效 zk 材料**，仅钉契约形状；
/// compat §5 块纪律）。
pub(crate) const FAKE_LINK_PRESENTATION: &[u8] = b"kt-fake-call-link-presentation";
pub(crate) const FAKE_CREATE_PRESENTATION: &[u8] = b"kt-fake-create-link-presentation";
pub(crate) const FAKE_CREATE_PUBLIC_PARAMS: &[u8] = b"kt-fake-call-link-public-params";
pub(crate) const FAKE_GROUP_PROOF: &str = "kt-fake-group-proof";

/// RingRTC v2.72.1 CallLinkRootKey 两版本定长（lite/call_links/root_key.rs：
/// V0::LEN = CRN_LEN = 16；V1::LEN = 16 + 1 version + 4 epoch = 21）。zkgroup
/// `derive_from_root_key` 只取前 16B CRN（ROOT_KEY_MAX_BYTES_FOR_SHO=16）。
pub(crate) const ROOT_KEY_LENGTHS: [usize; 2] = [16, 21];

/// roomId 定长 32B（root_key.rs `derive_room_id` = HKDF-SHA256 固定 32B 输出，
/// info "20230501-Signal-CallLinkRootKey-RoomId"；zkgroup 侧无长度闸，引擎按
/// 官方派生定长收口）。
pub(crate) const ROOM_ID_BYTES: usize = 32;

/// rootKey 参数闸（fail-closed）：标准 base64 + 解码后恰 16/21B。
pub(crate) fn decode_root_key(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()?;
    ROOT_KEY_LENGTHS.contains(&bytes.len()).then_some(bytes)
}

/// roomId 参数闸（fail-closed）：标准 base64 + 解码后恰 32B。
pub(crate) fn decode_room_id(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()?;
    (bytes.len() == ROOM_ID_BYTES).then_some(bytes)
}

const DAY_SECONDS: u64 = 86_400;

/// 今天 UTC 零点起的 epoch 秒（官方 toDayMillis 同语义，秒面）。
pub(crate) fn today_seconds(now_epoch_secs: u64) -> u64 {
    now_epoch_secs / DAY_SECONDS * DAY_SECONDS
}

pub(crate) fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 官方 getDatesForRequest 逐字对位（groupCredentialFetcher.preload.ts:
/// 336-362）。返回 None = 窗口已覆盖不需拉取；Some((start, end)) = 拉取窗
///（含两端，秒，日界对齐）。
fn dates_for_request(today: u64, credential_days_asc: &[u64]) -> Option<(u64, u64)> {
    let six_days_out = today + 6 * DAY_SECONDS;
    let last = credential_days_asc.last().copied();
    if !credential_days_asc.contains(&today) || last.is_none() || last < Some(today) {
        return Some((today, six_days_out));
    }
    let last = last.expect("contains(today) 蕴含非空");
    if last >= six_days_out {
        return None;
    }
    Some((last + DAY_SECONDS, six_days_out))
}

/// 官方双族窗口合并逐字对位（maybeFetchNewCredentials :175-196）：legacy 族
/// 与 callLink 族各算一次 dates_for_request，两窗都需拉取时取
/// min(start)/max(end) 合并；两窗都覆盖 = None（零网络）。
fn merge_windows(auth: Option<(u64, u64)>, call_link: Option<(u64, u64)>) -> Option<(u64, u64)> {
    match (auth, call_link) {
        (Some((a_start, a_end)), Some((c_start, c_end))) => {
            Some((a_start.min(c_start), a_end.max(c_end)))
        }
        (Some(window), None) | (None, Some(window)) => Some(window),
        (None, None) => None,
    }
}

/// 兑换后凭据条目（序列化字节按兑换日秒键控；官方 itemStorage 同语义，
/// ms/秒单位差在引擎侧统一为秒）。
#[derive(Clone, Debug, PartialEq, Eq)]
struct CredentialEntry {
    redemption_day_seconds: u64,
    credential: Vec<u8>,
}

/// 官方合并清理逐字对位（:285-301）：prev 过滤 >= today + new，按日升序。
/// 窗口设计保证 prev/new 日键不相交（top-up 起点 = last+1d），不去重。
fn merge_entries(
    prev: &[CredentialEntry],
    new: Vec<CredentialEntry>,
    today: u64,
) -> Vec<CredentialEntry> {
    let mut merged: Vec<CredentialEntry> = prev
        .iter()
        .filter(|entry| entry.redemption_day_seconds >= today)
        .cloned()
        .collect();
    merged.extend(new);
    merged.sort_by_key(|entry| entry.redemption_day_seconds);
    merged
}

/// 双族选择（legacy AuthCredentialWithPni / callLink CallLinkAuthCredential）。
#[derive(Clone, Copy)]
enum Family {
    Auth,
    CallLink,
}

/// per-account 双族缓存（兑换后序列化字节，按兑换日升序）。
#[derive(Default)]
struct AccountCredentials {
    auth: Vec<CredentialEntry>,
    call_link: Vec<CredentialEntry>,
}

impl AccountCredentials {
    fn family(&self, family: Family) -> &Vec<CredentialEntry> {
        match family {
            Family::Auth => &self.auth,
            Family::CallLink => &self.call_link,
        }
    }
}

/// 群凭据双族进程内存缓存（per-account ACI 键控；不落盘不跨重启，relay
/// §5 纪律）。官方 groupCredentialFetcher 的引擎侧对位件。
pub(crate) struct GroupCredentialCache {
    inner: StdMutex<HashMap<String, AccountCredentials>>,
}

impl GroupCredentialCache {
    pub(crate) fn new() -> Self {
        Self {
            inner: StdMutex::new(HashMap::new()),
        }
    }

    /// 窗口键控面：指定族当前兑换日清单（升序）。
    fn credential_days(&self, account_key: &str, family: Family) -> Vec<u64> {
        self.inner
            .lock()
            .expect("group credential cache lock")
            .get(account_key)
            .map(|entry| {
                entry
                    .family(family)
                    .iter()
                    .map(|item| item.redemption_day_seconds)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 当日条目字节（缺席 = None；凭据面异常由调用方收敛 Failed{None}）。
    fn today_credential(
        &self,
        account_key: &str,
        family: Family,
        today: u64,
    ) -> Option<CredentialEntry> {
        self.inner
            .lock()
            .expect("group credential cache lock")
            .get(account_key)
            .and_then(|entry| {
                entry
                    .family(family)
                    .iter()
                    .find(|item| item.redemption_day_seconds == today)
                    .cloned()
            })
    }

    /// 合并入缓存（双族各自过滤 <today 后并入，升序）。
    fn merge(
        &self,
        account_key: &str,
        today: u64,
        auth: Vec<CredentialEntry>,
        call_link: Vec<CredentialEntry>,
    ) {
        let mut guard = self.inner.lock().expect("group credential cache lock");
        let entry = guard.entry(account_key.to_owned()).or_default();
        entry.auth = merge_entries(&entry.auth, auth, today);
        entry.call_link = merge_entries(&entry.call_link, call_link, today);
    }
}

impl Default for GroupCredentialCache {
    fn default() -> Self {
        Self::new()
    }
}

/// 凭据响应形状（官方 CredentialResponse camelCase 同形状；`pni` 字段不消费
/// ——官方仅对账日志，兑换恒用本机 PNI；`callLinkAuthCredentials` 旧服务器
/// 可缺席，缺省空族）。
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupCredentialsResponse {
    credentials: Vec<TemporalCredential>,
    #[serde(default)]
    call_link_auth_credentials: Vec<TemporalCredential>,
}

#[derive(Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TemporalCredential {
    credential: String,
    redemption_time: u64,
}

/// legacy 族单条兑换（官方 formatCredential 同链，groupCredentialFetcher
/// :225-257）：b64 → AuthCredentialWithPniResponse → receive(legacy 公钥,
/// ACI, PNI, 兑换日秒) → 序列化缓存。任一步失败 = fail-closed。
fn exchange_auth_credential(
    entry: &TemporalCredential,
    server_public_params: &ServerPublicParams,
    aci: Aci,
    pni: Pni,
) -> Result<CredentialEntry, CallCredentialError> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&entry.credential)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let response = AuthCredentialWithPniResponse::new(&bytes)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let credential = response
        .receive(
            server_public_params,
            aci,
            pni,
            Timestamp::from_epoch_seconds(entry.redemption_time),
        )
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    Ok(CredentialEntry {
        redemption_day_seconds: entry.redemption_time,
        credential: zkgroup::serialize(&credential),
    })
}

/// callLink 族单条兑换（官方 formatCallingCredential 同链，:259-276）：b64 →
/// CallLinkAuthCredentialResponse（bincode）→ receive(ACI, 兑换日秒,
/// GenericServerPublicParams)（内含日界对齐闸）→ 序列化缓存。
fn exchange_call_link_credential(
    entry: &TemporalCredential,
    generic_public_params: &GenericServerPublicParams,
    aci: Aci,
) -> Result<CredentialEntry, CallCredentialError> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&entry.credential)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let response: zkgroup::api::call_links::CallLinkAuthCredentialResponse =
        zkgroup::deserialize(&bytes).map_err(|_| CallCredentialError::Failed { status: None })?;
    let credential = response
        .receive(
            aci,
            Timestamp::from_epoch_seconds(entry.redemption_time),
            generic_public_params,
        )
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    Ok(CredentialEntry {
        redemption_day_seconds: entry.redemption_time,
        credential: zkgroup::serialize(&credential),
    })
}

/// 拉取 + 双族兑换（官方 maybeFetchNewCredentials 的取数段）：任一条目兑换
/// 失败 = 整批 fail-closed 不入缓存（官方 maybeFetch 抛错重试同语义）。
async fn fetch_credentials(
    context: &StorageContext,
    start: u64,
    end: u64,
) -> Result<(Vec<CredentialEntry>, Vec<CredentialEntry>), CallCredentialError> {
    let path = format!(
        "/v1/certificate/auth/group?redemptionStartSeconds={start}&redemptionEndSeconds={end}&pniAsServiceId=true&zkcCredential=true"
    );
    let response = context
        .push
        .request(
            reqwest::Method::GET,
            Endpoint::service(path),
            // 本机凭据 basic auth（官方 identified 通道）。
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| CallCredentialError::Unavailable)?
        .send()
        .await
        .map_err(|_| CallCredentialError::Unavailable)?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(CallCredentialError::Failed {
            status: Some(response.status().as_u16()),
        });
    }
    let body: GroupCredentialsResponse = response
        .json()
        .await
        .map_err(|_| CallCredentialError::Failed { status: None })?;

    let server_public_params = context.zkgroup_server_public_params();
    let generic_public_params = zkgroup_consts::generic_server_public_params()
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let service_ids = context.service_ids();
    let aci = Aci::from_uuid_bytes(service_ids.aci.into_bytes());
    let pni = Pni::from_uuid_bytes(service_ids.pni.into_bytes());

    let mut auth = Vec::with_capacity(body.credentials.len());
    for entry in &body.credentials {
        auth.push(exchange_auth_credential(
            entry,
            &server_public_params,
            aci,
            pni,
        )?);
    }
    let mut call_link = Vec::with_capacity(body.call_link_auth_credentials.len());
    for entry in &body.call_link_auth_credentials {
        call_link.push(exchange_call_link_credential(
            entry,
            &generic_public_params,
            aci,
        )?);
    }
    Ok((auth, call_link))
}

/// 官方 maybeFetchNewCredentials 同语义（引擎按需版）：双族各算窗口
///（:175-176），缺料才拉取，双族一次请求并入缓存；窗口已覆盖 = 零网络。
/// 并发重复拉取幂等合并（last-write-wins，官方 retry 同语义由调用方 -1
/// UNAVAILABLE 重试承载）。
pub(crate) async fn ensure_credentials(
    context: &StorageContext,
) -> Result<(), CallCredentialError> {
    let account_key = context.aci_uuid.to_string();
    let today = today_seconds(now_epoch_secs());
    let auth_days = context
        .group_credentials
        .credential_days(&account_key, Family::Auth);
    let call_link_days = context
        .group_credentials
        .credential_days(&account_key, Family::CallLink);
    let Some((start, end)) = merge_windows(
        dates_for_request(today, &auth_days),
        dates_for_request(today, &call_link_days),
    ) else {
        return Ok(());
    };
    let (auth, call_link) = fetch_credentials(context, start, end).await?;
    context
        .group_credentials
        .merge(&account_key, today, auth, call_link);
    Ok(())
}

/// 当日 legacy 凭据 → 群 basic auth 材料（官方 generateGroupAuth 同构，
/// WebAPI:4533-4540：username = hex(bincode(GroupPublicParams))，password =
/// hex(bincode(AnyAuthCredentialPresentation))）。凭据字节零日志零落盘。
fn group_http_auth(
    context: &StorageContext,
    group_secret_params: &GroupSecretParams,
) -> Result<HttpAuth, CallCredentialError> {
    use rand::RngCore as _;
    let account_key = context.aci_uuid.to_string();
    let today = today_seconds(now_epoch_secs());
    let entry = context
        .group_credentials
        .today_credential(&account_key, Family::Auth, today)
        .ok_or(CallCredentialError::Failed { status: None })?;
    let credential = AuthCredentialWithPni::new(&entry.credential)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let mut randomness = [0u8; 32];
    rand::rng().fill_bytes(&mut randomness);
    let presentation = zkgroup::serialize(&credential.present(
        &context.zkgroup_server_public_params(),
        group_secret_params,
        randomness,
    ));
    Ok(HttpAuth {
        // zkgroup bincode 序列化（simpleapi.rs GroupSecretParams_
        // getPublicParams 同构，官方凭据面同款）。
        username: super::hex_encode(
            &bincode::serialize(&group_secret_params.get_public_params())
                .map_err(|_| CallCredentialError::UnknownGroup)?,
        ),
        password: super::hex_encode(&presentation),
    })
}

/// 群 basic auth 材料供给面（groups.rs 群写面/读面与 calls.groupProof 共用）：
/// 先按需补凭据再出 auth。
pub(crate) async fn group_authorization(
    context: &StorageContext,
    group_secret_params: &GroupSecretParams,
) -> Result<HttpAuth, CallCredentialError> {
    ensure_credentials(context).await?;
    group_http_auth(context, group_secret_params)
}

/// calls.linkPresentation（官方 getCallLinkAuthCredentialPresentation 同链，
/// util/callLinks/zkgroup.preload.ts:16-49）：当日 CallLinkAuthCredential →
/// present(ACI, 兑换日秒, GenericServerPublicParams, derive_from_root_key(
/// rootKey 前 16B)) → 序列化字节（官方返未序列化对象给 RingRTC 绑定层，引擎
/// 字节化出 RPC）。
pub(crate) async fn link_presentation(
    context: &StorageContext,
    root_key: &[u8],
) -> Result<Vec<u8>, CallCredentialError> {
    use rand::RngCore as _;
    ensure_credentials(context).await?;
    let account_key = context.aci_uuid.to_string();
    let today = today_seconds(now_epoch_secs());
    let entry = context
        .group_credentials
        .today_credential(&account_key, Family::CallLink, today)
        .ok_or(CallCredentialError::Failed { status: None })?;
    let credential: CallLinkAuthCredential = zkgroup::deserialize(&entry.credential)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let generic_public_params = zkgroup_consts::generic_server_public_params()
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let call_link_params = CallLinkSecretParams::derive_from_root_key(root_key);
    let mut randomness = [0u8; 32];
    rand::rng().fill_bytes(&mut randomness);
    let presentation = credential.present(
        Aci::from_uuid_bytes(context.aci_uuid.into_bytes()),
        Timestamp::from_epoch_seconds(entry.redemption_day_seconds),
        &generic_public_params,
        &call_link_params,
        randomness,
    );
    Ok(zkgroup::serialize(&presentation))
}

/// create-auth 响应形状（官方 callLinkCreateAuthResponseSchema，
/// WebAPI.preload.ts:1471-1473：`{credential: string}`）。
#[derive(serde::Deserialize)]
struct CreateAuthResponse {
    credential: String,
}

/// calls.createLinkAuth（官方 createCallLink 凭据段同链，calling.preload.ts:
/// 860-940）：context.new(roomId) → `POST v1/call-link/create-auth`
/// （chatService host 本机凭据，json `{createCallLinkCredentialRequest: b64}`
/// → `{credential: b64}`；官方轮换前无 urlParameters，v101 形不移植——钉
/// v0.99.0 只能消费 legacy 族）→ context.receive(ACI, generic) →
/// present(roomId, ACI, generic, derive_from_root_key(rootKey)) +
/// get_public_params() 双序列化。
/// 单次调用内两段式（无跨调用状态）；rootKey 生成/roomId 派生/adminPasskey
/// 皆 RingRTC 侧职责。
pub(crate) async fn create_link_auth(
    context: &StorageContext,
    root_key: &[u8],
    room_id: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), CallCredentialError> {
    use base64::Engine as _;
    use rand::RngCore as _;

    let mut randomness = [0u8; 32];
    rand::rng().fill_bytes(&mut randomness);
    let request_context = CreateCallLinkCredentialRequestContext::new(room_id, randomness);
    let request_b64 = base64::engine::general_purpose::STANDARD
        .encode(zkgroup::serialize(&request_context.get_request()));

    let response = context
        .push
        .request(
            reqwest::Method::POST,
            Endpoint::service("/v1/call-link/create-auth"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| CallCredentialError::Unavailable)?
        .json(&serde_json::json!({
            "createCallLinkCredentialRequest": request_b64,
        }))
        .send()
        .await
        .map_err(|_| CallCredentialError::Unavailable)?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(CallCredentialError::Failed {
            status: Some(response.status().as_u16()),
        });
    }
    let body: CreateAuthResponse = response
        .json()
        .await
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let response_bytes = base64::engine::general_purpose::STANDARD
        .decode(&body.credential)
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let credential_response: CreateCallLinkCredentialResponse =
        zkgroup::deserialize(&response_bytes)
            .map_err(|_| CallCredentialError::Failed { status: None })?;

    let generic_public_params = zkgroup_consts::generic_server_public_params()
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    let aci = Aci::from_uuid_bytes(context.aci_uuid.into_bytes());
    let credential = request_context
        .receive(credential_response, aci, &generic_public_params)
        .map_err(|_| CallCredentialError::Failed { status: None })?;

    let call_link_params = CallLinkSecretParams::derive_from_root_key(root_key);
    let mut present_randomness = [0u8; 32];
    rand::rng().fill_bytes(&mut present_randomness);
    let presentation = credential.present(
        room_id,
        aci,
        &generic_public_params,
        &call_link_params,
        present_randomness,
    );
    Ok((
        zkgroup::serialize(&presentation),
        zkgroup::serialize(&call_link_params.get_public_params()),
    ))
}

/// calls.groupProof（官方 fetchMembershipProof 同链，groups.preload.ts:
/// 1782-1806 → getExternalGroupCredential WebAPI:4567-4586）：当日 legacy 凭据
/// present(group) → `GET v2/groups/token`（storageService host，群 basic auth）
/// → ExternalGroupCredential{token=1}（Groups.proto:284-286，presage::proto
/// 生成面复用）→ token 字符串原样透出（RingRTC setMembershipProof 消费其
/// UTF-8 字节）。
pub(crate) async fn group_proof(
    context: &StorageContext,
    master_key: &[u8],
) -> Result<String, CallCredentialError> {
    let key: [u8; 32] = master_key
        .try_into()
        .map_err(|_| CallCredentialError::UnknownGroup)?;
    let group_secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));
    let auth = group_authorization(context, &group_secret_params).await?;
    let response = context
        .push
        .request(
            reqwest::Method::GET,
            Endpoint::storage("v2/groups/token"),
            HttpAuthOverride::Identified(auth),
        )
        .map_err(|_| CallCredentialError::Unavailable)?
        .send()
        .await
        .map_err(|_| CallCredentialError::Unavailable)?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(CallCredentialError::Failed {
            status: Some(status),
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| CallCredentialError::Unavailable)?;
    let credential = presage::proto::ExternalGroupCredential::decode(bytes.as_ref())
        .map_err(|_| CallCredentialError::Failed { status: None })?;
    Ok(credential.token)
}

/// groupMembers 材料条目（relay-api-v2 §2 帧面对位）：uuid = 成员 ACI 字符
/// 串，user_id_cipher_text = UuidCiphertext bincode 序列化字节。
pub(crate) struct MemberMaterial {
    pub(crate) uuid: String,
    pub(crate) user_id_cipher_text: Vec<u8>,
}

/// calls.groupMembers（官方 getMembershipList 同语义，groups.preload.ts:
/// 7611-7632）：服务器群态读面（fetch_group_state 既有面）→ 每成员
/// `GroupSecretParams::encrypt_service_id(ACI)` → UuidCiphertext 序列化。
pub(crate) async fn group_members(
    context: &StorageContext,
    master_key: &[u8],
) -> Result<Vec<MemberMaterial>, CallCredentialError> {
    let group = super::groups::fetch_group_state(context, master_key).await?;
    let key: [u8; 32] = master_key
        .try_into()
        .map_err(|_| CallCredentialError::UnknownGroup)?;
    let secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));
    Ok(group
        .members
        .iter()
        .map(|member| MemberMaterial {
            uuid: member.aci.service_id_string(),
            user_id_cipher_text: zkgroup::serialize(
                &secret_params.encrypt_service_id(member.aci.into()),
            ),
        })
        .collect())
}

/// endorsementsPublicKey 材料面（常量派生不触网；zkgroup_consts 单一真相源）。
pub(crate) fn endorsements_public_key_bytes() -> Vec<u8> {
    zkgroup_consts::endorsement_public_key_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zkgroup::api::generic_server_params::GenericServerSecretParams;

    const DAY: u64 = 86_400;

    fn entry(day: u64, tag: u8) -> CredentialEntry {
        CredentialEntry {
            redemption_day_seconds: day * DAY,
            credential: vec![tag],
        }
    }

    /// 官方 getDatesForRequest 逐字钉定（:336-362 全分支）。
    #[test]
    fn dates_for_request_matches_official_window() {
        let today = 20_000 * DAY;
        let six_days_out = today + 6 * DAY;
        // 空缓存 / 缺今日 → 全窗 {today, +6d}。
        assert_eq!(dates_for_request(today, &[]), Some((today, six_days_out)));
        assert_eq!(
            dates_for_request(today, &[(today + 2 * DAY)]),
            Some((today, six_days_out)),
            "有未来日但缺今日 → 全窗重拉（haveToday=false 臂）"
        );
        // 末条 < 今日（陈旧）→ 全窗。
        assert_eq!(
            dates_for_request(today, &[today - 2 * DAY, today - DAY]),
            Some((today, six_days_out))
        );
        // 有今日、末条 < sixDaysOut → 增量 top-up {last+1d, +6d}。
        assert_eq!(
            dates_for_request(today, &[today]),
            Some((today + DAY, six_days_out))
        );
        assert_eq!(
            dates_for_request(today, &[today, today + 5 * DAY]),
            Some((today + 6 * DAY, six_days_out))
        );
        // 末条 >= sixDaysOut → 不拉。
        assert_eq!(dates_for_request(today, &[today, six_days_out]), None);
        assert_eq!(dates_for_request(today, &[today, today + 7 * DAY]), None);
    }

    /// 官方双族窗口合并逐字钉定（maybeFetchNewCredentials :175-196）：
    /// min(start)/max(end)、单侧覆盖取单侧、双侧覆盖零网络。
    #[test]
    fn merge_windows_matches_official_dual_family() {
        let today = 20_000 * DAY;
        let six_days_out = today + 6 * DAY;
        // 双侧都需全窗 → 合并仍全窗。
        assert_eq!(
            merge_windows(Some((today, six_days_out)), Some((today, six_days_out))),
            Some((today, six_days_out))
        );
        // auth 增量窗 + callLink 缺今日全窗 → min/max 合并（官方同形）。
        assert_eq!(
            merge_windows(
                Some((today + DAY, six_days_out)),
                Some((today, six_days_out))
            ),
            Some((today, six_days_out))
        );
        // auth 不需拉 + callLink 增量 → 取 callLink 窗。
        assert_eq!(
            merge_windows(None, Some((today + DAY, six_days_out))),
            Some((today + DAY, six_days_out))
        );
        // callLink 不需拉 + auth 增量 → 取 auth 窗。
        assert_eq!(
            merge_windows(Some((today + 2 * DAY, six_days_out)), None),
            Some((today + 2 * DAY, six_days_out))
        );
        // 双侧都覆盖 → 零网络。
        assert_eq!(merge_windows(None, None), None);
    }

    /// 官方合并清理逐字钉定（:285-301）：prev 过滤 <today + new 升序。
    #[test]
    fn merge_drops_expired_and_sorts() {
        let today_day = 20_000u64;
        let prev = vec![
            entry(today_day - 2, 1),
            entry(today_day - 1, 2),
            entry(today_day, 3),
        ];
        let merged = merge_entries(
            &prev,
            vec![entry(today_day + 2, 4), entry(today_day + 1, 5)],
            today_day * DAY,
        );
        assert_eq!(
            merged,
            vec![
                entry(today_day, 3),
                entry(today_day + 1, 5),
                entry(today_day + 2, 4),
            ]
        );
    }

    /// 参数闸：rootKey 恰 16/21B、roomId 恰 32B，其余 fail-closed。
    #[test]
    fn parameter_gates_are_fail_closed() {
        use base64::Engine as _;
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        assert_eq!(
            decode_root_key(&b64(&[0x11; 16])).map(|v| v.len()),
            Some(16)
        );
        assert_eq!(
            decode_root_key(&b64(&[0x11; 21])).map(|v| v.len()),
            Some(21)
        );
        for len in [0usize, 15, 17, 20, 22, 32] {
            assert!(
                decode_root_key(&b64(&vec![0x11; len])).is_none(),
                "len={len}"
            );
        }
        assert!(decode_root_key("not-b64!!").is_none());
        assert_eq!(decode_room_id(&b64(&[0x22; 32])).map(|v| v.len()), Some(32));
        for len in [0usize, 16, 31, 33] {
            assert!(
                decode_room_id(&b64(&vec![0x22; len])).is_none(),
                "len={len}"
            );
        }
    }

    /// callLink 族兑换+呈现 roundtrip：模拟服务器签发 → 引擎兑换函数 →
    /// 缓存字节 → 呈现 → 服务器公钥族验证通过；错公钥族/垃圾字节/日界不符
    /// 三面 fail-closed。
    #[test]
    fn call_link_credential_roundtrip_and_fail_closed() {
        use zkgroup::api::call_links::CallLinkAuthCredentialResponse;

        let server_secret = GenericServerSecretParams::generate([0xA0; 32]);
        let server_public = server_secret.get_public_params();
        let other_public = GenericServerSecretParams::generate([0xB0; 32]).get_public_params();
        let aci = Aci::from_uuid_bytes([0x11; 16]);
        let redemption_day = 20_000 * DAY;

        // 服务器视角：签发当日凭据 → 序列化 → b64（线面形状）。
        let issued = CallLinkAuthCredentialResponse::issue_credential(
            aci,
            Timestamp::from_epoch_seconds(redemption_day),
            &server_secret,
            [0xA1; 32],
        );
        let wire_b64 = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(zkgroup::serialize(&issued))
        };
        let wire = TemporalCredential {
            credential: wire_b64,
            redemption_time: redemption_day,
        };

        // 兑换 → 缓存字节 → 呈现 → 服务器侧 verify 通过。
        let exchanged =
            exchange_call_link_credential(&wire, &server_public, aci).expect("兑换成功");
        assert_eq!(exchanged.redemption_day_seconds, redemption_day);
        let credential: CallLinkAuthCredential =
            zkgroup::deserialize(&exchanged.credential).expect("缓存字节可解");
        let root_key = [0x42; 16];
        let call_link_params = CallLinkSecretParams::derive_from_root_key(&root_key);
        let presentation = credential.present(
            aci,
            Timestamp::from_epoch_seconds(exchanged.redemption_day_seconds),
            &server_public,
            &call_link_params,
            [0xA2; 32],
        );
        presentation
            .verify(
                Timestamp::from_epoch_seconds(redemption_day + 3600),
                &server_secret,
                &call_link_params.get_public_params(),
            )
            .expect("呈现经服务器密钥族验证");

        // fail-closed①：错服务器公钥族。
        assert!(exchange_call_link_credential(&wire, &other_public, aci).is_err());
        // fail-closed②：垃圾 b64 / 垃圾字节。
        let mut bad = wire.clone();
        bad.credential = "not-b64!!".to_owned();
        assert!(exchange_call_link_credential(&bad, &server_public, aci).is_err());
        bad.credential = "AAEC".to_owned();
        assert!(exchange_call_link_credential(&bad, &server_public, aci).is_err());
        // fail-closed③：兑换日非日界对齐（receive 内日界闸）。
        let mut misaligned = wire.clone();
        misaligned.redemption_time = redemption_day + 1;
        assert!(exchange_call_link_credential(&misaligned, &server_public, aci).is_err());
    }

    /// createLinkAuth 两段式 roundtrip：context.new → get_request 线面字节 →
    /// 模拟服务器 issue → 引擎 receive → present + publicParams → 服务器侧
    /// verify 通过。
    #[test]
    fn create_link_auth_two_stage_roundtrip() {
        let server_secret = GenericServerSecretParams::generate([0xC0; 32]);
        let server_public = server_secret.get_public_params();
        let aci = Aci::from_uuid_bytes([0x33; 16]);
        let room_id = [0x44; 32];
        let root_key = [0x55; 16];

        let context = CreateCallLinkCredentialRequestContext::new(&room_id, [0xC1; 32]);
        let request_bytes = zkgroup::serialize(&context.get_request());
        // 服务器视角：解请求 → 签发（日界时刻）→ 序列化回传。
        let request: zkgroup::api::call_links::CreateCallLinkCredentialRequest =
            zkgroup::deserialize(&request_bytes).expect("请求字节可解");
        let issued_at = 20_000 * DAY;
        let response = request.issue(
            aci,
            Timestamp::from_epoch_seconds(issued_at),
            &server_secret,
            [0xC2; 32],
        );
        let response: CreateCallLinkCredentialResponse =
            zkgroup::deserialize(&zkgroup::serialize(&response)).expect("响应线面 round-trip");

        let credential = context
            .receive(response, aci, &server_public)
            .expect("两段式兑换成功");
        let call_link_params = CallLinkSecretParams::derive_from_root_key(&root_key);
        let presentation =
            credential.present(&room_id, aci, &server_public, &call_link_params, [0xC3; 32]);
        presentation
            .verify(
                &room_id,
                Timestamp::from_epoch_seconds(issued_at + 3600),
                &server_secret,
                &call_link_params.get_public_params(),
            )
            .expect("呈现经服务器密钥族验证");
        // publicParams 字节与 secretParams 派生一致（双材料同源）。
        assert_eq!(
            zkgroup::serialize(&call_link_params.get_public_params()),
            zkgroup::serialize(
                &CallLinkSecretParams::derive_from_root_key(&root_key).get_public_params()
            )
        );
    }

    /// ExternalGroupCredential proto 面（presage::proto 生成面复用）round-trip
    /// 钉定：token=1 字符串字段编码/解码互逆。
    #[test]
    fn external_group_credential_proto_roundtrip() {
        let credential = presage::proto::ExternalGroupCredential {
            token: "kt-test-token".to_owned(),
        };
        let bytes = credential.encode_to_vec();
        // field 1 length-delimited 前缀钉定（tag 0x0A）。
        assert_eq!(bytes[0], 0x0A);
        let decoded =
            presage::proto::ExternalGroupCredential::decode(bytes.as_slice()).expect("解码");
        assert_eq!(decoded.token, "kt-test-token");
    }
}
