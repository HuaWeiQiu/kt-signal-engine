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
}
