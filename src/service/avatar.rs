// SPDX-License-Identifier: AGPL-3.0-only

//! 头像下行（getAvatar，KT 扩展方法）：1:1 与群统一入口。
//!
//! 官方行为锚（Signal-Desktop 行为对齐，不抄码）：chat 服务 GET v1/profile
//! 带 profileKey → avatar path → CDN 密文下载 + profileKey 解密（未 accept
//! 会话不下载）；403/404 = 无头像。1:1 面走 presage
//! `Manager::retrieve_profile_avatar_by_uuid`（查缓存 → 查/拉 profile → CDN
//! 下载 → `ProfileCipher::decrypt_avatar` → save_profile_avatar 入库），群
//! 面走 `Manager::retrieve_group_avatar`（群 master key 派生
//! GroupSecretParams 解密；master key 与 listGroups 同源，群 id = master key
//! 标准 base64）。profileKey 来源 = presage store `profile_key(service_id)`
//! （contacts 同步随行落库）；未同步过 profile_key 的联系人诚实报「无头像」。
//!
//! 出参 = data URI + 字节数；尺寸帽照 presage 注释口径 10MB（Signal
//! Android 分配量级），超出 fail-closed。

use presage::manager::{Manager, Registered};
use presage::proto::GroupContextV2;
use presage::store::{ContentsStore as _, Store};
use presage_store_sqlite::SqliteStore;
use std::sync::Arc;

use super::profile::AVATAR_MAX_BYTES;
use super::stickers::sniff_image_mime_type;
use super::{decode_group_id, find_contact_by_number, open_registered, Service};
use presage::libsignal_service::prelude::ServiceError;
use presage::libsignal_service::protocol::ServiceId;

/// 头像单次下行上限（与上行同帽，官方 AvatarHelper 10MB 同量级）。
pub(crate) const AVATAR_DOWNLOAD_MAX_BYTES: usize = AVATAR_MAX_BYTES;

/// getAvatar 会话参数分派结果（形状判定，纯函数）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AvatarTarget {
    /// ACI/PNI service id（UUID 直通）。
    ServiceId(ServiceId),
    /// 手机号文本（E.164 或裸数字位），contacts 反查。
    Number(String),
    /// 群 master key（32B，listGroups id 同源）。
    Group(Vec<u8>),
}

/// getAvatar 错误面：UnknownAccount/UnknownConversation 由 methods.rs 折叠
/// -32603（账号/会话参数不指向可达目标，非头像面，细节不外泄）；Unavailable
/// 折叠 `AVATAR_UNAVAILABLE`（无头像：未同步 profile_key / 对端或群未设头
/// 像 / 服务器 403/404）；FetchFailed 折叠 `AVATAR_FETCH_FAILED`（网络/下载/
/// 解密失败）；TooLarge 折叠 `AVATAR_TOO_LARGE`（超 10MB 帽）。结构化码均
/// `-1` + message 码位（置顶/贴纸面白名单风格）。
#[derive(Debug)]
pub(crate) enum AvatarError {
    UnknownAccount,
    UnknownConversation,
    Unavailable,
    FetchFailed,
    TooLarge,
}

/// getAvatar 成功载荷：data URI（`data:<mime>;base64,<payload>`）+ 字节数。
pub(crate) struct AvatarPayload {
    pub(crate) data_uri: String,
    pub(crate) size: usize,
}

/// getAvatar 会话参数分派（形状判定，纯函数）：ServiceId（ACI/PNI，UUID
/// 双形态）→ 1:1；32B 标准 base64 → 群；纯数字（允许前导 +）→ 1:1 号码
/// （contacts 反查）。三者皆不中 → None（fail-closed）。判定序有意避开
/// 形状交叠：号码（≤15 位数字）不可能命中 32B base64（44 字符）。
pub(crate) fn classify_conversation(conversation_id: &str) -> Option<AvatarTarget> {
    let text = conversation_id.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(service_id) = ServiceId::parse_from_service_id_string(text) {
        return Some(AvatarTarget::ServiceId(service_id));
    }
    if let Some(master_key) = decode_group_id(text) {
        return Some(AvatarTarget::Group(master_key));
    }
    let numberish = text.chars().any(|c| c.is_ascii_digit())
        && text.chars().all(|c| c.is_ascii_digit() || c == '+');
    numberish.then(|| AvatarTarget::Number(text.to_owned()))
}

/// 头像 bytes → (data URI, 字节数)：魔数嗅探 image MIME（官方
/// sniffImageMimeType 同表），未识别回退 `application/octet-stream`
/// （诚实面：官方桌面头像落盘不标 MIME；引擎出 data URI 必须给类型，
/// 不猜 image/*）。
pub(crate) fn avatar_data_uri(bytes: &[u8]) -> (String, usize) {
    use base64::Engine as _;
    let mime = sniff_image_mime_type(bytes).unwrap_or("application/octet-stream");
    let data_uri = format!(
        "data:{mime};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    );
    (data_uri, bytes.len())
}

/// 10MB 帽（presage 注释口径：Signal Android 分配量级），超出 fail-closed。
fn cap(bytes: Vec<u8>) -> Result<Vec<u8>, AvatarError> {
    if bytes.len() > AVATAR_DOWNLOAD_MAX_BYTES {
        Err(AvatarError::TooLarge)
    } else {
        Ok(bytes)
    }
}

/// getAvatar 编排：会话分派（参数面 fail-closed）→ fake 桩 → manager →
/// presage 头像下行（缓存/CDN/解密/入库由 presage 承担）→ 帽 → data URI
/// 投影。
pub(crate) async fn get_avatar(
    service: &Arc<Service>,
    account: &str,
    conversation_id: &str,
) -> Result<AvatarPayload, AvatarError> {
    let target = classify_conversation(conversation_id).ok_or(AvatarError::UnknownConversation)?;
    // fake 形状桩：无 store 可达「无头像」错误面（IPC 契约测试钉定形状，
    // compat §6；参数面 fail-closed 在本桩之前生效）。
    if !service.real {
        return Err(AvatarError::Unavailable);
    }
    let mut manager = open_registered(service, account)
        .await
        .ok_or(AvatarError::UnknownAccount)?;
    let bytes = match target {
        AvatarTarget::ServiceId(service_id) => profile_avatar(&mut manager, service_id).await?,
        AvatarTarget::Number(number) => {
            let contact = find_contact_by_number(&manager, &number)
                .await
                .ok_or(AvatarError::Unavailable)?;
            profile_avatar(&mut manager, ServiceId::Aci(contact.uuid.into())).await?
        }
        AvatarTarget::Group(master_key) => group_avatar(&mut manager, master_key).await?,
    };
    let bytes = cap(bytes)?;
    let (data_uri, size) = avatar_data_uri(&bytes);
    Ok(AvatarPayload { data_uri, size })
}

/// 1:1 头像下行：store profile_key（未同步 → Unavailable，诚实「无头像」）
/// → presage retrieve_profile_avatar_by_uuid（None = 对端未设头像 →
/// Unavailable）。
async fn profile_avatar(
    manager: &mut Manager<SqliteStore, Registered>,
    service_id: ServiceId,
) -> Result<Vec<u8>, AvatarError> {
    let profile_key = manager
        .store()
        .profile_key(&service_id)
        .await
        .map_err(|_| AvatarError::FetchFailed)?
        .ok_or(AvatarError::Unavailable)?;
    manager
        .retrieve_profile_avatar_by_uuid(service_id.raw_uuid(), profile_key)
        .await
        .map_err(|ref error| avatar_error_of(error))?
        .ok_or(AvatarError::Unavailable)
}

/// 群头像下行：presage retrieve_group_avatar（master key 派生解密；群无头
/// 像/未入库 → None → Unavailable）。
async fn group_avatar(
    manager: &mut Manager<SqliteStore, Registered>,
    master_key: Vec<u8>,
) -> Result<Vec<u8>, AvatarError> {
    let context = GroupContextV2 {
        master_key: Some(master_key),
        revision: None,
        group_change: None,
    };
    manager
        .retrieve_group_avatar(context)
        .await
        .map_err(|ref error| avatar_error_of(error))?
        .ok_or(AvatarError::Unavailable)
}

/// presage 头像错误 → 引擎错误面：服务器 404（NotFoundError）与 403/404
/// 状态码同语义（官方 403/404=无头像行为锚）→ Unavailable；其余（网络/
/// 解密/存储）→ FetchFailed。
fn avatar_error_of(error: &presage::Error<<SqliteStore as Store>::Error>) -> AvatarError {
    match error {
        presage::Error::ServiceError(ServiceError::NotFoundError) => AvatarError::Unavailable,
        presage::Error::ServiceError(ServiceError::UnhandledResponseCode { status, .. })
            if status.as_u16() == 403 || status.as_u16() == 404 =>
        {
            AvatarError::Unavailable
        }
        _ => AvatarError::FetchFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 会话参数分派形状面：ServiceId UUID 直通（ACI/PNI 前缀形态亦收）、
    /// 32B 标准 base64 → 群、纯数字/带 + 号码 → Number；空串/垃圾/任意
    /// 其他形状 fail-closed。
    #[test]
    fn classify_conversation_shapes() {
        let uuid = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
        assert_eq!(
            classify_conversation(uuid),
            Some(AvatarTarget::ServiceId(
                ServiceId::parse_from_service_id_string(uuid).expect("aci")
            ))
        );
        // PNI 前缀形态直通。
        let pni = format!("PNI:{uuid}");
        assert!(matches!(
            classify_conversation(&pni),
            Some(AvatarTarget::ServiceId(_))
        ));

        // 32B base64 → 群。
        use base64::Engine as _;
        let master_key = [0xAB_u8; 32];
        let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
        assert_eq!(
            classify_conversation(&group_id),
            Some(AvatarTarget::Group(master_key.to_vec()))
        );
        // 非 32B base64 不算群。
        assert_ne!(
            classify_conversation(&base64::engine::general_purpose::STANDARD.encode([1u8; 16])),
            Some(AvatarTarget::Group(vec![1u8; 16]))
        );

        // 号码面：E.164 与裸数字位。
        assert_eq!(
            classify_conversation("+15551234567"),
            Some(AvatarTarget::Number("+15551234567".to_owned()))
        );
        assert_eq!(
            classify_conversation("15551234567"),
            Some(AvatarTarget::Number("15551234567".to_owned()))
        );

        // fail-closed：空串、垃圾、非数字文本。
        assert_eq!(classify_conversation(""), None);
        assert_eq!(classify_conversation("   "), None);
        assert_eq!(classify_conversation("not-a-conversation"), None);
        assert_eq!(classify_conversation("contact:xyz"), None);
        // 纯 '+'（无数字）不是号码。
        assert_eq!(classify_conversation("+++"), None);
    }

    /// data URI 投影：PNG 魔数命中 image/png；未识别字节诚实回退
    /// application/octet-stream；字节数 = 原始长度。
    #[test]
    fn avatar_data_uri_sniffs_mime_and_reports_size() {
        let png = b"\x89PNG\r\n\x1a\n-fake-bytes";
        let (uri, size) = avatar_data_uri(png);
        assert!(uri.starts_with("data:image/png;base64,"));
        assert_eq!(size, png.len());
        let payload = uri.split(',').next_back().expect("payload");
        use base64::Engine as _;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .expect("base64"),
            png
        );

        let (uri, size) = avatar_data_uri(b"\x00\x01\x02\x03");
        assert!(uri.starts_with("data:application/octet-stream;base64,"));
        assert_eq!(size, 4);
    }

    /// 10MB 帽：帽内通过，超帽 TooLarge fail-closed。
    #[test]
    fn download_cap_rejects_oversize() {
        assert_eq!(cap(vec![0u8; 3]).expect("within cap").len(), 3);
        let big = vec![0u8; AVATAR_DOWNLOAD_MAX_BYTES + 1];
        assert!(matches!(cap(big), Err(AvatarError::TooLarge)));
        assert!(cap(vec![0u8; AVATAR_DOWNLOAD_MAX_BYTES]).is_ok());
    }

    /// presage 错误面折叠：404（NotFoundError）与 403/404 状态 →
    /// Unavailable（官方 403/404=无头像）；其余错误 → FetchFailed。
    #[test]
    fn presage_error_faces_fold_to_engine_codes() {
        use presage::libsignal_service::configuration::SignalServers;
        let not_found = presage::Error::ServiceError(ServiceError::NotFoundError);
        assert!(matches!(
            avatar_error_of(&not_found),
            AvatarError::Unavailable
        ));
        let forbidden = presage::Error::ServiceError(ServiceError::UnhandledResponseCode {
            status: reqwest::StatusCode::FORBIDDEN,
            body: String::new(),
        });
        assert!(matches!(
            avatar_error_of(&forbidden),
            AvatarError::Unavailable
        ));
        let missing = presage::Error::ServiceError(ServiceError::UnhandledResponseCode {
            status: reqwest::StatusCode::NOT_FOUND,
            body: String::new(),
        });
        assert!(matches!(
            avatar_error_of(&missing),
            AvatarError::Unavailable
        ));
        let other = presage::Error::ServiceError(ServiceError::Timeout { reason: "x" });
        assert!(matches!(avatar_error_of(&other), AvatarError::FetchFailed));
        // 非 403/404 的 HTTP 状态（如 500）→ FetchFailed。
        let boom = presage::Error::ServiceError(ServiceError::UnhandledResponseCode {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: String::new(),
        });
        assert!(matches!(avatar_error_of(&boom), AvatarError::FetchFailed));
        let _ = SignalServers::Production;
    }
}
