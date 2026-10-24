// SPDX-License-Identifier: AGPL-3.0-only

//! 用户名面（KT 扩展方法）：读面 getUsername / usernameLookup（本批）+
//! 写面 usernameReserve / usernameConfirm / usernameDelete（后续批）。
//!
//! 官方行为锚（AGPL 仓行为对照，零抄码，引用只到文件:行）：
//! - 服务端 REST 面（Signal-Server AccountController，行为观察面）：
//!   `GET v1/accounts/username_hash/{hash}`（强制匿名，200 `{uuid}` / 404）、
//!   `GET v1/accounts/username_link/{uuid}`（强制匿名，200
//!   `{usernameLinkEncryptedValue}` / 404）、`PUT v1/accounts/username_hash/
//!   reserve`（`{usernameHashes:[b64url]}` → 200 `{usernameHash}` / 409 全占）、
//!   `PUT v1/accounts/username_hash/confirm`（`{usernameHash, zkProof,
//!   encryptedUsername}` → 200 `{usernameLinkHandle}` / 409 无预约 / 410 已占）、
//!   `DELETE v1/accounts/username_hash`（204）。base64 一律 URL-safe 无 padding。
//!   libsignal-service 钉定 rev 的 `src/websocket/usernames.rs`（模块私有，
//!   行为对照物）走同两条 lookup 路径与同一解码面。
//! - 客户端编排骨架（Signal-Desktop ts/services/username.preload.ts）：
//!   reserve = 候选全名逐个 hash 成批上行 → 服务器返回采纳的那个 hash →
//!   按下标对回候选全名；confirm = `Username::proof`（ZK proof）+
//!   username link 加密负载（`create_for_username`）成批上行 → 返回
//!   usernameLinkHandle → 完整链接 = `https://signal.me/#eu/` +
//!   base64url(entropy(32B) || handle(16B))；delete = 上行 DELETE 后清本地。
//! - 语法面（ts/util/Username.dom.ts + libsignal usernames crate）：
//!   nickname `[a-zA-Z_][a-zA-Z0-9_]*` 3..32 字符（软限，crate 硬限 48）、
//!   discriminator 全数字无前导零（0 打头仅允许恰两位）、crate `Username::new`
//!   为权威校验器。
//! - 持久化真相源：Storage Service AccountRecord——`username`(33)、
//!   `usernameLink`(35，entropy+serverId，官方注释「仅 server confirm/delete
//!   时更新」）。桌面 confirm 后写模型 + storage 同步（updateProfile
//!   usernameChangeSyncMessage 面），本引擎以 AccountRecord 读改写同语义
//!   收敛（storage_sync 通路），无第二真相源。
//!
//! 读面语义（本批）：
//! - `getUsername`：AccountRecord(33/35) 投影 `{username?, usernameLink?}`——
//!   键缺席 = 未设。多设备经 storage sync 收敛（桌面同源真相）。
//! - `usernameLookup`：条目（全名或 signal.me 链接）→ 服务端匿名 lookup 通路
//!   → `{recipient, username?, uuid?, isRegistered}` 逐条投影（输入序）。
//!   链接条目先解密出用户名再走 hash lookup；任一环失败（语法不符/链接
//!   打不开/解不开/未命中）诚实降级 `{recipient, isRegistered: false}`，
//!   不炸整批。

use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL_SAFE_NO_PAD;
use base64::Engine as _;
use presage::libsignal_service::configuration::{Endpoint, SignalServers};
use presage::libsignal_service::prelude::{PushService, Uuid};
use presage::libsignal_service::protocol::Aci;
use presage::libsignal_service::push_service::HttpAuthOverride;

use super::storage_sync::{self, StorageSyncError};
use super::StorageContext;

/// username link 负载的熵段长度（官方 USERNAME_LINK_ENTROPY_SIZE 同值 32B，
/// 用户名面协议常量，引擎侧独立钉定）。
pub(crate) const USERNAME_LINK_ENTROPY_SIZE: usize = 32;

/// username link handle 的字节数（服务器 UUID 原样 16B）。
const USERNAME_LINK_HANDLE_SIZE: usize = 16;

/// link payload 总长 = entropy(32) || handle(16)。
const USERNAME_LINK_PAYLOAD_SIZE: usize = USERNAME_LINK_ENTROPY_SIZE + USERNAME_LINK_HANDLE_SIZE;

/// usernameLookup 单批条目上限（connector 契约 1.59 §4.55 的 KT 帧界
/// 同源镜像：20 条/批，单条 1..64 字符——帧界而非语法界）。
pub(crate) const USERNAME_LOOKUP_MAX_ENTRIES: usize = 20;
pub(crate) const USERNAME_LOOKUP_ENTRY_MAX_CHARS: usize = 64;

/// 匿名 PushService（lookup 端点强制无鉴权——带凭据反而 400）：
/// avatar CDN 先例同构（profile.rs post_avatar_to_cdn）。
pub(crate) fn anonymous_push() -> PushService {
    PushService::new(SignalServers::Production, None, super::USER_AGENT)
}

/// username link → (entropy, handle)：`https://signal.me/#eu/<payload>`，
/// payload = base64url(entropy(32B) || handle(16B))。语法不符 → None
/// （调用方逐条诚实降级，不炸整批）。
pub(crate) fn parse_username_link(link: &str) -> Option<([u8; USERNAME_LINK_ENTROPY_SIZE], Uuid)> {
    let url = url::Url::parse(link).ok()?;
    if url.scheme() != "https" || url.host_str() != Some("signal.me") {
        return None;
    }
    let fragment = url.fragment()?;
    let mut segments = fragment.split('/');
    if segments.next() != Some("eu") {
        return None;
    }
    let payload = segments.next()?;
    if segments.next().is_some() {
        return None;
    }
    let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
    if bytes.len() != USERNAME_LINK_PAYLOAD_SIZE {
        return None;
    }
    let mut entropy = [0u8; USERNAME_LINK_ENTROPY_SIZE];
    entropy.copy_from_slice(&bytes[..USERNAME_LINK_ENTROPY_SIZE]);
    let handle = Uuid::from_slice(&bytes[USERNAME_LINK_ENTROPY_SIZE..]).ok()?;
    Some((entropy, handle))
}

/// (entropy, handle) → 完整分享链接（官方 generate_username_link 同构）。
pub(crate) fn build_username_link(
    entropy: &[u8; USERNAME_LINK_ENTROPY_SIZE],
    handle: &Uuid,
) -> String {
    let mut payload = Vec::with_capacity(USERNAME_LINK_PAYLOAD_SIZE);
    payload.extend_from_slice(entropy);
    payload.extend_from_slice(handle.as_bytes());
    format!(
        "https://signal.me/#eu/{}",
        BASE64_URL_SAFE_NO_PAD.encode(payload)
    )
}

/// 服务端匿名 hash lookup：`GET v1/accounts/username_hash/{hash}`。
/// 200 → Some(ACI uuid 串)；404 → None（未注册/未命中）；其余/网络 → Err。
async fn lookup_username_hash(
    push: &PushService,
    hash: &[u8; 32],
) -> Result<Option<String>, StorageSyncError> {
    #[derive(serde::Deserialize)]
    struct UsernameHashResponse {
        uuid: String,
    }
    let path = format!(
        "v1/accounts/username_hash/{}",
        BASE64_URL_SAFE_NO_PAD.encode(hash)
    );
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service(&path),
            HttpAuthOverride::Unidentified,
        )
        .map_err(|_| StorageSyncError::Unavailable)?
        .send()
        .await
        .map_err(|_| StorageSyncError::Unavailable)?;
    if response.status().as_u16() == 404 {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(StorageSyncError::Unavailable);
    }
    let parsed: UsernameHashResponse = response
        .json()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?;
    // 服务端回传 ACI UUID；解不出 ServiceId 形状 = 上游面损坏，逐条降级。
    let aci = Aci::parse_from_service_id_string(&parsed.uuid);
    Ok(aci.map(|aci| aci.service_id_string()))
}

/// 服务端匿名链接取负载：`GET v1/accounts/username_link/{uuid}` →
/// `usernameLinkEncryptedValue`（base64url）。404 → None；其余/网络 → Err。
async fn fetch_link_ciphertext(
    push: &PushService,
    handle: &Uuid,
) -> Result<Option<Vec<u8>>, StorageSyncError> {
    #[derive(serde::Deserialize)]
    struct UsernameLinkResponse {
        #[serde(rename = "usernameLinkEncryptedValue")]
        encrypted_username: String,
    }
    let path = format!("v1/accounts/username_link/{handle}");
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service(&path),
            HttpAuthOverride::Unidentified,
        )
        .map_err(|_| StorageSyncError::Unavailable)?
        .send()
        .await
        .map_err(|_| StorageSyncError::Unavailable)?;
    if response.status().as_u16() == 404 {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(StorageSyncError::Unavailable);
    }
    let parsed: UsernameLinkResponse = response
        .json()
        .await
        .map_err(|_| StorageSyncError::ReadFailed)?;
    BASE64_URL_SAFE_NO_PAD
        .decode(&parsed.encrypted_username)
        .map(Some)
        .map_err(|_| StorageSyncError::ReadFailed)
}

/// 单条 usernameLookup 解析：全名 → hash lookup；signal.me 链接 → 链接
/// 解密 → hash lookup。返回 `(username?, uuid?, registered)`。任一环失败
/// （语法不符/链接打不开/解不开/未命中/网络）诚实降级，不炸整批。
pub(crate) async fn resolve_entry(
    push: &PushService,
    entry: &str,
) -> (Option<String>, Option<String>, bool) {
    // 全名分支：crate 权威语法校验（硬语法面；软限由上游 reserve 面管）。
    if let Ok(username) = usernames::Username::new(entry) {
        return match lookup_username_hash(push, &username.hash()).await {
            Ok(Some(uuid)) => (Some(username.to_string()), Some(uuid), true),
            _ => (None, None, false),
        };
    }
    // 链接分支：解密出用户名再走 hash lookup（官方 resolveUsernameLink
    // 同序；解密出的用户名过权威语法面，坏负载 = 降级不炸面）。
    let Some((entropy, handle)) = parse_username_link(entry) else {
        return (None, None, false);
    };
    let Ok(Some(ciphertext)) = fetch_link_ciphertext(push, &handle).await else {
        return (None, None, false);
    };
    let Ok(plaintext) = usernames::decrypt_username(&entropy, &ciphertext) else {
        return (None, None, false);
    };
    let Ok(username) = usernames::Username::new(&plaintext) else {
        return (None, None, false);
    };
    match lookup_username_hash(push, &username.hash()).await {
        Ok(Some(uuid)) => (Some(username.to_string()), Some(uuid), true),
        _ => (None, None, false),
    }
}

// ---------------------------------------------------------------------------
// 读面编排（getUsername）
// ---------------------------------------------------------------------------

/// 本账号用户名投影（AccountRecord 33/35 → wire 形状）。
pub(crate) fn project_own_username(
    record: &crate::proto::storage::AccountRecord,
) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    if !record.username.is_empty() {
        out.insert("username".to_owned(), serde_json::json!(record.username));
    }
    if let Some(link) = &record.username_link {
        let lengths_ok = link.entropy.len() == USERNAME_LINK_ENTROPY_SIZE
            && link.server_id.len() == USERNAME_LINK_HANDLE_SIZE;
        let handle = if lengths_ok {
            Uuid::from_slice(&link.server_id).ok()
        } else {
            None
        };
        if let Some(handle) = handle {
            let mut entropy = [0u8; USERNAME_LINK_ENTROPY_SIZE];
            entropy.copy_from_slice(&link.entropy);
            out.insert(
                "usernameLink".to_owned(),
                serde_json::json!(build_username_link(&entropy, &handle)),
            );
        }
    }
    serde_json::Value::Object(out)
}

/// getUsername 读面编排：storage 读 → 33/35 投影。从未有 storage 状态
/// （无 master key / manifest 404 / 无 Account item）→ `{}`。
pub(crate) async fn get_own_username(
    context: &StorageContext,
) -> Result<serde_json::Value, StorageSyncError> {
    match storage_sync::read_own_account_record(context).await? {
        Some(record) => Ok(project_own_username(&record)),
        None => Ok(serde_json::json!({})),
    }
}

// ---------------------------------------------------------------------------
// 写面（usernameReserve / usernameConfirm / usernameDelete）
// ---------------------------------------------------------------------------

/// 写面错误：参数面（BadParams）折叠 -32603 fail-closed；服务器态四码
/// 结构化 `-1` + message 码位（reserve 409 全占=TAKEN / confirm 409 无
/// 预约=RESERVATION_GONE / confirm 410 已占=TAKEN / 413·429=RATE_LIMITED /
/// 其余 HTTP 与网络=SERVER_FAILED）；storage 链路同四码族透传。
#[derive(Debug)]
pub(crate) enum UsernameWriteError {
    BadParams,
    Taken,
    ReservationGone,
    RateLimited,
    ServerFailed,
    Storage(StorageSyncError),
}

impl From<StorageSyncError> for UsernameWriteError {
    fn from(value: StorageSyncError) -> Self {
        UsernameWriteError::Storage(value)
    }
}

/// reserve 采纳结果：服务器采纳的候选全名 + 其 hash（base64url 无
/// padding，confirm 参数面原样回传）。
pub(crate) struct UsernameReservation {
    pub(crate) username: String,
    pub(crate) username_hash: String,
}

/// nickname 语法闸（官方桌面 Username.dom.ts 镜像，connector §4.55 同源）：
/// `[a-zA-Z_][a-zA-Z0-9_]*` 3..32 字符。crate 硬语法为权威校验器，本闸
/// 只是把软限与字符面提前到参数面 fail-closed。
fn nickname_shape_ok(nickname: &str) -> bool {
    let length = nickname.chars().count();
    (3..=32).contains(&length)
        && nickname
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && nickname
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// discriminator 语法闸（connector §4.55 同源）：全数字、非空、无 `00`
/// 前缀、0 打头仅允许恰两位。数值域（crate u64 面）由 from_parts 权威校验。
fn discriminator_shape_ok(discriminator: &str) -> bool {
    !discriminator.is_empty()
        && discriminator.bytes().all(|byte| byte.is_ascii_digit())
        && !discriminator.starts_with("00")
        && (!discriminator.starts_with('0') || discriminator.len() == 2)
}

/// usernameReserve 编排（官方 reserveUsername 同序）：候选生成 → 逐个
/// hash 成批 → `PUT v1/accounts/username_hash/reserve` → 服务器采纳的
/// hash 按下标对回候选全名。全名条目（带 `.`）= 单候选；裸昵称 =
/// crate `candidates_from` 候选拒绝器（服务器各 range 取一个）。fake 面
/// 由 Service 层短路（纯本地同函数，无网络）。
pub(crate) async fn reserve_username(
    context: &StorageContext,
    username: &str,
) -> Result<UsernameReservation, UsernameWriteError> {
    let candidates = reserve_candidates(username)?;
    let hashes: Vec<[u8; 32]> = candidates
        .iter()
        .map(|candidate| {
            usernames::Username::new(candidate)
                .map(|parsed| parsed.hash())
                .map_err(|_| UsernameWriteError::BadParams)
        })
        .collect::<Result<_, _>>()?;
    let accepted = reserve_hash_on_server(&context.push, &hashes).await?;
    let index = hashes
        .iter()
        .position(|hash| *hash == accepted)
        .ok_or(UsernameWriteError::ServerFailed)?;
    Ok(UsernameReservation {
        username: candidates[index].clone(),
        username_hash: BASE64_URL_SAFE_NO_PAD.encode(accepted),
    })
}

/// 候选生成：全名（带 `.`）= 单候选（语法闸 + crate from_parts 软限权威
/// 校验）；裸昵称 = crate candidates_from（软限 3..32 同源默认）。
pub(crate) fn reserve_candidates(username: &str) -> Result<Vec<String>, UsernameWriteError> {
    match username.split_once('.') {
        Some((nickname, discriminator)) => {
            if !nickname_shape_ok(nickname) || !discriminator_shape_ok(discriminator) {
                return Err(UsernameWriteError::BadParams);
            }
            let parsed = usernames::Username::from_parts(
                nickname,
                discriminator,
                usernames::NicknameLimits::default(),
            )
            .map_err(|_| UsernameWriteError::BadParams)?;
            Ok(vec![parsed.to_string()])
        }
        None => {
            if !nickname_shape_ok(username) {
                return Err(UsernameWriteError::BadParams);
            }
            let candidates = usernames::Username::candidates_from(
                &mut rand::rng(),
                username,
                usernames::NicknameLimits::default(),
            )
            .map_err(|_| UsernameWriteError::BadParams)?;
            Ok(candidates)
        }
    }
}

/// `PUT v1/accounts/username_hash/reserve`（本机凭据 basic auth）：批
/// hashes → 200 `{usernameHash}`（b64url，32B）；409 → Taken（批内全占，
/// 官方同码位）；413/429 → RateLimited；其余 → ServerFailed。
async fn reserve_hash_on_server(
    push: &PushService,
    hashes: &[[u8; 32]],
) -> Result<[u8; 32], UsernameWriteError> {
    #[derive(serde::Serialize)]
    struct ReserveRequest {
        #[serde(rename = "usernameHashes")]
        username_hashes: Vec<String>,
    }
    #[derive(serde::Deserialize)]
    struct ReserveResponse {
        #[serde(rename = "usernameHash")]
        username_hash: String,
    }
    let body = ReserveRequest {
        username_hashes: hashes
            .iter()
            .map(|hash| BASE64_URL_SAFE_NO_PAD.encode(hash))
            .collect(),
    };
    let response = push
        .request(
            reqwest::Method::PUT,
            Endpoint::service("v1/accounts/username_hash/reserve"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| UsernameWriteError::ServerFailed)?
        .json(&body)
        .send()
        .await
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let status = response.status().as_u16();
    if status == 409 {
        return Err(UsernameWriteError::Taken);
    }
    if status == 413 || status == 429 {
        return Err(UsernameWriteError::RateLimited);
    }
    if !response.status().is_success() {
        return Err(UsernameWriteError::ServerFailed);
    }
    let parsed: ReserveResponse = response
        .json()
        .await
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let bytes = BASE64_URL_SAFE_NO_PAD
        .decode(&parsed.username_hash)
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    bytes
        .try_into()
        .map_err(|_| UsernameWriteError::ServerFailed)
}

/// usernameConfirm 编排（官方 confirmUsername 同序）：语法面重验（硬语法）
/// 与 hash 失配闸 → `Username::proof`（ZK，随机 32B）→ username link 加密
/// 负载（`create_for_username`，新 entropy）→ `PUT
/// v1/accounts/username_hash/confirm` → `usernameLinkHandle` 加本地 entropy
/// 重建完整链接 → AccountRecord 33/35 storage 写（官方 confirm 落盘同位）。
/// 服务器已确认而 storage 写失败 = 诚实部分成功面（STORAGE_* 浮出，多设备
/// 经 storage sync 收敛；§5 用户名写面块）。
pub(crate) async fn confirm_username(
    context: &StorageContext,
    username: &str,
    username_hash: &str,
) -> Result<serde_json::Value, UsernameWriteError> {
    let parsed = usernames::Username::new(username).map_err(|_| UsernameWriteError::BadParams)?;
    let computed = BASE64_URL_SAFE_NO_PAD.encode(parsed.hash());
    if computed != username_hash {
        // hash 失配 = 预约与本名不一致（两相调用面被撕裂），fail-closed。
        return Err(UsernameWriteError::BadParams);
    }

    let mut randomness = [0u8; 32];
    rand::Rng::fill(&mut rand::rng(), &mut randomness[..]);
    let proof = parsed
        .proof(&randomness)
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let (entropy, ciphertext) =
        usernames::create_for_username(&mut rand::rng(), parsed.to_string(), None)
            .map_err(|_| UsernameWriteError::ServerFailed)?;

    #[derive(serde::Serialize)]
    struct ConfirmRequest {
        #[serde(rename = "usernameHash")]
        username_hash: String,
        #[serde(rename = "zkProof")]
        zk_proof: String,
        #[serde(rename = "encryptedUsername")]
        encrypted_username: String,
    }
    #[derive(serde::Deserialize)]
    struct ConfirmResponse {
        #[serde(rename = "usernameLinkHandle")]
        username_link_handle: String,
    }
    let body = ConfirmRequest {
        username_hash: username_hash.to_owned(),
        zk_proof: BASE64_URL_SAFE_NO_PAD.encode(&proof),
        encrypted_username: BASE64_URL_SAFE_NO_PAD.encode(&ciphertext),
    };
    let response = context
        .push
        .request(
            reqwest::Method::PUT,
            Endpoint::service("v1/accounts/username_hash/confirm"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| UsernameWriteError::ServerFailed)?
        .json(&body)
        .send()
        .await
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let status = response.status().as_u16();
    if status == 409 {
        return Err(UsernameWriteError::ReservationGone);
    }
    if status == 410 {
        return Err(UsernameWriteError::Taken);
    }
    if status == 413 || status == 429 {
        return Err(UsernameWriteError::RateLimited);
    }
    if !response.status().is_success() {
        return Err(UsernameWriteError::ServerFailed);
    }
    let parsed_response: ConfirmResponse = response
        .json()
        .await
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let handle = Uuid::parse_str(&parsed_response.username_link_handle)
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let link = build_username_link(&entropy, &handle);

    // storage 面同步（AccountRecord 33/35 读改写；409 采纳重放与幂等无写
    // 由 update_account_record 骨架承担）。
    storage_sync::update_account_record(context, |record| {
        storage_sync::account_record_with_username(
            record,
            Some(&parsed.to_string()),
            Some((entropy, *handle.as_bytes())),
        )
    })
    .await?;

    let mut out = serde_json::Map::new();
    out.insert("username".to_owned(), serde_json::json!(parsed.to_string()));
    out.insert("usernameLink".to_owned(), serde_json::json!(link));
    Ok(serde_json::Value::Object(out))
}

/// usernameDelete 编排（官方 deleteUsername 同序）：`DELETE
/// v1/accounts/username_hash`（服务器清 hash；官方 AccountsManager 同语义
/// 连带清 link 负载）→ AccountRecord 33/35 清除（读改写；本就未设 =
/// 幂等无写）。413/429 → RateLimited；其余失败 → ServerFailed。
pub(crate) async fn delete_username(
    context: &StorageContext,
) -> Result<serde_json::Value, UsernameWriteError> {
    let response = context
        .push
        .request(
            reqwest::Method::DELETE,
            Endpoint::service("v1/accounts/username_hash"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| UsernameWriteError::ServerFailed)?
        .send()
        .await
        .map_err(|_| UsernameWriteError::ServerFailed)?;
    let status = response.status().as_u16();
    if status == 413 || status == 429 {
        return Err(UsernameWriteError::RateLimited);
    }
    if !response.status().is_success() {
        return Err(UsernameWriteError::ServerFailed);
    }

    storage_sync::update_account_record(context, |record| {
        storage_sync::account_record_with_username(record, Some(""), None)
    })
    .await?;
    Ok(serde_json::json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// username link 解析/重建 round-trip：payload = base64url(entropy ||
    /// handle)；链接前缀 `https://signal.me/#eu/`；entropy/handle 原样往返。
    #[test]
    fn username_link_round_trip() {
        let entropy = [0x42u8; USERNAME_LINK_ENTROPY_SIZE];
        let handle = Uuid::from_bytes([
            0x9d, 0x06, 0x52, 0xa3, 0xdc, 0xc3, 0x4d, 0x11, 0x97, 0x5f, 0x74, 0xd6, 0x15, 0x98,
            0x73, 0x3f,
        ]);
        let link = build_username_link(&entropy, &handle);
        assert!(link.starts_with("https://signal.me/#eu/"));
        let (parsed_entropy, parsed_handle) =
            parse_username_link(&link).expect("well-shaped link parses");
        assert_eq!(parsed_entropy, entropy);
        assert_eq!(parsed_handle, handle);

        // 重建再解析（二次往返稳定）。
        let rebuilt = build_username_link(&parsed_entropy, &parsed_handle);
        assert_eq!(rebuilt, link);
    }

    /// 链接语法面 fail-closed：非 https / 非 signal.me / 缺 fragment /
    /// 非 eu 段 / 多段 / payload 长度错 / 坏 base64 一律 None。
    #[test]
    fn username_link_parse_rejects_malformed() {
        let good = build_username_link(&[1u8; 32], &Uuid::nil());
        let swap_host = good.replace("signal.me", "example.com");
        assert!(parse_username_link(&swap_host).is_none());
        let http = good.replace("https://", "http://");
        assert!(parse_username_link(&http).is_none());
        assert!(parse_username_link("https://signal.me/").is_none());
        assert!(parse_username_link("https://signal.me/#foo/AAA").is_none());
        assert!(parse_username_link("https://signal.me/#eu/AAA/extra").is_none());
        // payload 长度错（缺 handle 16B）。
        assert!(parse_username_link("https://signal.me/#eu/AAAA").is_none());
        // 坏 base64。
        assert!(parse_username_link("https://signal.me/#eu/###").is_none());
        // 非 URL 输入。
        assert!(parse_username_link("ktpay.01").is_none());
    }

    /// 全名语法面走 crate 权威校验器（引擎 lookup 分支同款调用）：合法
    /// 全名接受（大小写保留、hash 按小写）、非法形态拒绝——单段/数字开头/
    /// 非法字符/单数字 discriminator/前导零/零 discriminator。
    #[test]
    fn entry_grammar_edges_follow_the_crate() {
        // 合法：字母/下划线开头 + 两位 discriminator；大小写保留在 Display。
        let mixed = usernames::Username::new("KtPay.01").expect("uppercase nickname allowed");
        assert_eq!(mixed.to_string(), "KtPay.01");
        // 非法：缺分隔符 / 空昵称 / 数字开头 / 非法字符 / 单数字 disc /
        // 前导零三位 / 零 disc / 越界 disc。
        assert!(usernames::Username::new("barenickname").is_err());
        assert!(usernames::Username::new(".01").is_err());
        assert!(usernames::Username::new("9kt.01").is_err());
        assert!(usernames::Username::new("kt-pay.01").is_err());
        assert!(usernames::Username::new("ktpay.1").is_err());
        assert!(usernames::Username::new("ktpay.001").is_err());
        assert!(usernames::Username::new("ktpay.00").is_err());
        // 大数字面越 u64（crate 硬语法面的 PosOverflow 臂）。
        assert!(usernames::Username::new("ktpay.99999999999999999999999").is_err());
    }

    /// hash/proof 原语可达性钉定（lookup/reserve/confirm 三面共用的
    /// crate 面）：hash 32B 确定性；proof 可被 crate verify_proof 验过、
    /// 篡改即拒——零自研密码学。
    #[test]
    fn hash_and_proof_primitives_round_trip() {
        let username = usernames::Username::new("kt_engine.42").expect("valid");
        let hash = username.hash();
        assert_eq!(hash.len(), 32);
        assert_eq!(username.hash(), hash, "deterministic");

        let mut randomness = [0u8; 32];
        rand::Rng::fill(&mut rand::rng(), &mut randomness[..]);
        let proof = username.proof(&randomness).expect("proof builds");
        usernames::Username::verify_proof(&proof, hash).expect("proof verifies");
        let mut corrupted = proof.clone();
        corrupted[0] ^= 0x01;
        assert!(usernames::Username::verify_proof(&corrupted, hash).is_err());
    }

    /// AccountRecord 33/35 → wire 投影：设齐出双键、只设名出单键、
    /// 全空出 `{}`；链接材料形状错（长度不符）该键缺席不发明。
    #[test]
    fn own_username_projection_keys() {
        use crate::proto::storage::account_record::UsernameLink;
        let full = crate::proto::storage::AccountRecord {
            username: "kt_pay.01".to_owned(),
            username_link: Some(UsernameLink {
                entropy: vec![7u8; 32],
                server_id: Uuid::nil().as_bytes().to_vec(),
                color: 0,
            }),
            ..Default::default()
        };
        let projection = project_own_username(&full);
        assert_eq!(projection["username"], json!("kt_pay.01"));
        let expected_link = build_username_link(&[7u8; 32], &Uuid::nil());
        assert_eq!(projection["usernameLink"], json!(expected_link));

        let name_only = crate::proto::storage::AccountRecord {
            username: "kt_pay.01".to_owned(),
            ..Default::default()
        };
        let projection = project_own_username(&name_only);
        assert_eq!(projection["username"], json!("kt_pay.01"));
        assert!(projection.get("usernameLink").is_none());

        let empty = crate::proto::storage::AccountRecord::default();
        assert_eq!(project_own_username(&empty), json!({}));

        let malformed = crate::proto::storage::AccountRecord {
            username: "kt_pay.01".to_owned(),
            username_link: Some(UsernameLink {
                entropy: vec![7u8; 8],
                server_id: Uuid::nil().as_bytes().to_vec(),
                color: 0,
            }),
            ..Default::default()
        };
        let projection = project_own_username(&malformed);
        assert!(projection.get("usernameLink").is_none());
        assert_eq!(projection["username"], json!("kt_pay.01"));
    }

    /// 候选生成面（reserve 参数闸 + crate 权威校验双层）：全名 = 单候选
    /// （大小写保留）；裸昵称 = 候选拒绝器（crate 8 range 共 20 候选，
    /// 全部硬语法合法、同昵称前缀、两位起 disc）；形状闸逐项 fail-closed。
    #[test]
    fn reserve_candidates_faces() {
        use super::reserve_candidates;
        use super::UsernameWriteError;

        // 全名：单候选，原样大小写保留。
        let single = reserve_candidates("KtPay.07").expect("valid full username");
        assert_eq!(single, vec!["KtPay.07".to_owned()]);

        // 裸昵称：crate 候选器（[4,3,3,2,2,2,2,2] × 8 range = 20），全部
        // `{nick}.{disc:0>2}` 形态且通过 crate 权威语法。
        let bare = reserve_candidates("kt_pay").expect("valid nickname");
        assert_eq!(bare.len(), 20);
        assert!(bare.iter().all(|candidate| {
            usernames::Username::new(candidate)
                .map(|parsed| parsed.to_string() == *candidate)
                .unwrap_or(false)
        }));
        assert!(bare
            .iter()
            .all(|candidate| candidate.starts_with("kt_pay.")));

        // 形状闸 fail-closed：短昵称/坏字符/数字开头/空 disc/单 0/前导零
        // 三位/00 前缀。
        let rejects = [
            "ab.01",
            "9ktpay.01",
            "kt-pay.01",
            "kt_pay.01x.02",
            "kt_pay.",
            "kt_pay.0",
            "kt_pay.007",
            "kt_pay.00",
            "名.01",
            "",
        ];
        for reject in rejects {
            assert!(
                matches!(
                    reserve_candidates(reject),
                    Err(UsernameWriteError::BadParams)
                ),
                "shape gate must reject {reject}"
            );
        }
    }

    /// 裸昵称候选可全量走完 reserve 本地面：每候选 hash 32B 确定性
    /// （crate 面，无网络）。
    #[test]
    fn bare_candidates_all_hashable() {
        let bare = reserve_candidates("kt_pay").expect("valid nickname");
        for candidate in &bare {
            let parsed = usernames::Username::new(candidate).expect("crate-valid candidate");
            assert_eq!(parsed.hash().len(), 32);
        }
    }

    /// username link 加解密 round-trip（写面 confirm 的 link 材料与读面
    /// 链接解析共用 crate 面）：自造密文可解回原文；密文位翻转发 MAC 拒。
    #[test]
    fn link_encryption_round_trip() {
        let (entropy, ciphertext) =
            usernames::create_for_username(&mut rand::rng(), "kt_pay.01".to_owned(), None)
                .expect("encrypts");
        let plaintext =
            usernames::decrypt_username(&entropy, &ciphertext).expect("decrypts own ciphertext");
        assert_eq!(plaintext, "kt_pay.01");

        let mut corrupted = ciphertext.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert!(usernames::decrypt_username(&entropy, &corrupted).is_err());
    }

    /// AccountRecord 33/35 写入 helper：写新=Some(变更)、同值幂等=None、
    /// 清除（Some("")+None）落空、本就未设的清除=None。
    #[test]
    fn account_record_with_username_faces() {
        use crate::proto::storage::account_record::UsernameLink;
        let material: ([u8; 32], [u8; 16]) = ([9u8; 32], [8u8; 16]);

        // 未设 → 写入：Some，双字段落位。
        let fresh = crate::proto::storage::AccountRecord::default();
        let updated =
            storage_sync::account_record_with_username(&fresh, Some("kt_pay.01"), Some(material))
                .expect("write lands");
        assert_eq!(updated.username, "kt_pay.01");
        let link = updated.username_link.clone().expect("link set");
        assert_eq!(link.entropy, vec![9u8; 32]);
        assert_eq!(link.server_id, vec![8u8; 16]);

        // 同值幂等：无变化 → None（不 PUT 不 bump version）。
        assert!(storage_sync::account_record_with_username(
            &updated,
            Some("kt_pay.01"),
            Some(material)
        )
        .is_none());

        // 清除：置空 + 链接清除。
        let cleared = storage_sync::account_record_with_username(&updated, Some(""), None)
            .expect("clear lands");
        assert_eq!(cleared.username, "");
        assert!(cleared.username_link.is_none());
        // 本就未设的清除 = 幂等无写。
        assert!(storage_sync::account_record_with_username(&fresh, Some(""), None).is_none());

        // 只换名不换链接（大小写刷新场景）：33 变、35 保真原样。
        let existing = crate::proto::storage::AccountRecord {
            username: "old_name.01".to_owned(),
            username_link: Some(UsernameLink {
                entropy: vec![3u8; 32],
                server_id: vec![4u8; 16],
                color: 0,
            }),
            ..Default::default()
        };
        let renamed = storage_sync::account_record_with_username(
            &existing,
            Some("New_Name.01"),
            Some(material),
        )
        .expect("rename lands");
        assert_eq!(renamed.username, "New_Name.01");
        // 新链接材料覆盖（confirm 面语义： entropy 每次新生成）。
        assert_eq!(renamed.username_link.expect("link").entropy, vec![9u8; 32]);
    }
}
