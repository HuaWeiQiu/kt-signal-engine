// SPDX-License-Identifier: AGPL-3.0-only

//! 个人资料编辑（setProfile，P5 扩面）：名字/关于/emoji + 头像上行。
//!
//! 文字面（无 avatar 参数）走 presage `Manager::update_profile`（上游
//! `upload_versioned_profile_without_avatar`，retain_avatar=true）。头像面是
//! presage/libsignal-service 双层缺口（`upload_versioned_profile` 的
//! NewAvatar 分支 `unimplemented!()`，`write_profile` 的上传响应也不解析），
//! 自建薄封装对齐官方桌面（encryptProfileData.preload.ts /
//! WebAPI.preload.ts putProfile + uploadAvatar + makePutParams，行为对齐不
//! 抄码）：ProfileCipher 加密 name/about/emoji → `PUT /v1/profile`（JSON
//! `{version, name, about, aboutEmoji, avatar, sameAvatar, commitment}`）→
//! 响应 = 头像上传属性 `{key, credential, acl, algorithm, date, policy,
//! signature}` → multipart/form-data POST 到 CDN0 根（官方 makePutParams
//! 字段序逐一同构，boundary = `----------------<uuid 去连字符>`）→ 头像
//! path = 响应 `key`。头像加密 = `iv(12B) || AES-256-GCM(key=profileKey,
//! 无 AAD, 无 padding)`（桌面 Crypto.node.ts encryptProfile / 上游
//! ProfileCipher 同构；`ProfileCipher::decrypt_avatar` 交叉验证钉定）。
//!
//! 写后经 `storage_sync::update_account_record` 把 givenName/familyName/
//! avatarUrlPath 同步进 AccountRecord（官方 updateProfile 的 storage 写面
//! 同构——多设备经 storage sync 收敛）。

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::Uuid;
use presage::libsignal_service::profile_cipher::ProfileCipher;
use presage::libsignal_service::profile_name::ProfileName;
use presage::libsignal_service::protocol::Aci;
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::libsignal_service::push_service::PushService;
use presage::libsignal_service::zkgroup::profiles::ProfileKey;
use presage::manager::Manager;
use presage::manager::Registered;
use presage::store::StateStore as _;
use presage_store_sqlite::SqliteStore;
use rand::RngCore;

use super::storage_sync;
use super::StorageContext;

/// 头像单次上行上限（官方 AvatarHelper 10MB 同量级；超出 fail-closed）。
pub(crate) const AVATAR_MAX_BYTES: usize = 10 * 1024 * 1024;

/// 头像意图（setProfile avatar 参数面）。
pub(crate) enum AvatarIntent {
    /// 缺省：保留现有头像（文字面走 presage retain_avatar 通路）。
    Retain,
    /// null：移除头像（服务器面 `avatar:false`；storage 面清 avatarUrlPath）。
    Remove,
    /// data URI：换新头像（加密上行 + CDN multipart + storage 面写新 path）。
    Replace { bytes: Vec<u8> },
}

/// setProfile 参数意图（methods.rs 解析面出；文字面 = 全量写，与官方
/// updateProfile 一致：givenName 必填，familyName/about/emoji 缺省 = 置空）。
pub(crate) struct ProfileUpdate {
    pub(crate) given_name: String,
    pub(crate) family_name: Option<String>,
    pub(crate) about: Option<String>,
    pub(crate) emoji: Option<String>,
    pub(crate) avatar: AvatarIntent,
}

/// setProfile 错误面：storage 链路四码复用（compat §4.33，头像上行失败归
/// `STORAGE_WRITE_FAILED`——写意图未完整落地；不新增结构化码）。参数形状
/// 错误在 methods.rs 解析面 fail-closed（-32603），不进本错误面。
#[derive(Debug)]
pub(crate) enum ProfileError {
    Storage(storage_sync::StorageSyncError),
}

impl From<storage_sync::StorageSyncError> for ProfileError {
    fn from(value: storage_sync::StorageSyncError) -> Self {
        ProfileError::Storage(value)
    }
}

/// 头像 data URI 解析（fail-closed）：`data:image/*;base64,<payload>`，
/// 必须 base64、必须 image/* 族、非空且 ≤ AVATAR_MAX_BYTES。
pub(crate) fn parse_avatar_data_uri(uri: &str) -> Option<(String, Vec<u8>)> {
    let rest = uri.strip_prefix("data:")?;
    let (meta, payload) = rest.split_once(',')?;
    let mut parts = meta.split(';');
    let content_type = parts.next().unwrap_or_default();
    if !content_type.starts_with("image/") {
        return None;
    }
    if !parts.any(|param| param.eq_ignore_ascii_case("base64")) {
        return None;
    }
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .ok()?;
    if bytes.is_empty() || bytes.len() > AVATAR_MAX_BYTES {
        return None;
    }
    Some((content_type.to_owned(), bytes))
}

/// 头像加密（官方 encryptProfile 同构）：`iv(12B) || AES-256-GCM(
/// key=profileKey.bytes, 无 AAD, 无 padding)`。与上游
/// `ProfileCipher::decrypt_avatar` 交叉验证（tests）。
pub(crate) fn encrypt_avatar(profile_key: &ProfileKey, plaintext: &[u8]) -> Vec<u8> {
    use aes_gcm::aead::Aead as _;
    use aes_gcm::KeyInit as _;
    let mut key = [0u8; 32];
    key.copy_from_slice(&profile_key.bytes);
    let cipher = aes_gcm::Aes256Gcm::new((&key).into());
    let mut iv = [0u8; 12];
    rand::rng().fill_bytes(&mut iv);
    let nonce = aes_gcm::Nonce::try_from(&iv[..]).expect("12-byte IV");
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .expect("AES-256-GCM encrypt infallible for valid key");
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&iv);
    out.extend_from_slice(&ciphertext);
    out
}

/// PUT /v1/profile 响应（官方 AvatarUploadAttributes；verifyAttributes 同构
/// ——七字段全非空才可用）。
#[derive(serde::Deserialize)]
struct AvatarUploadAttributes {
    key: String,
    credential: String,
    acl: String,
    algorithm: String,
    date: String,
    policy: String,
    signature: String,
}

impl AvatarUploadAttributes {
    fn verified(self) -> Option<Self> {
        let empty = [
            self.key.is_empty(),
            self.credential.is_empty(),
            self.acl.is_empty(),
            self.algorithm.is_empty(),
            self.date.is_empty(),
            self.policy.is_empty(),
            self.signature.is_empty(),
        ];
        (!empty.iter().any(|field| *field)).then_some(self)
    }
}

/// CDN multipart body（官方 WebAPI makePutParams 逐字节同构）：字段序
/// key / x-amz-credential / acl / x-amz-algorithm / x-amz-date / policy /
/// x-amz-signature / Content-Type（表单字段值 application/octet-stream）+
/// file part（Content-Type: application/octet-stream）；段间 CRLF；结尾
/// `\r\n--{boundary}--\r\n`。
fn multipart_body(attrs: &AvatarUploadAttributes, encrypted: &[u8], boundary: &str) -> Vec<u8> {
    let section = |name: &str, value: &str| {
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}")
    };
    let mut body = Vec::new();
    body.extend_from_slice(section("key", &attrs.key).as_bytes());
    let fields = [
        ("x-amz-credential", attrs.credential.as_str()),
        ("acl", attrs.acl.as_str()),
        ("x-amz-algorithm", attrs.algorithm.as_str()),
        ("x-amz-date", attrs.date.as_str()),
        ("policy", attrs.policy.as_str()),
        ("x-amz-signature", attrs.signature.as_str()),
        ("Content-Type", "application/octet-stream"),
    ];
    for (name, value) in fields {
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(section(name, value).as_bytes());
    }
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"\r\nContent-Type: application/octet-stream\r\n\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(encrypted);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// CDN0 根 POST（官方 uploadAvatar 同路径）：成功任意 2xx，返回头像 path
/// （= 上行属性 `key`）。匿名 CDN（无 basic auth）。
async fn post_avatar_to_cdn(
    attrs: AvatarUploadAttributes,
    encrypted: &[u8],
) -> Result<String, ProfileError> {
    let mut seed = [0u8; 16];
    rand::rng().fill_bytes(&mut seed);
    let boundary = format!("----------------{}", Uuid::from_bytes(seed).simple());
    let body = multipart_body(&attrs, encrypted, &boundary);
    let push = PushService::new(
        presage::libsignal_service::configuration::SignalServers::Production,
        None,
        crate::service::USER_AGENT,
    );
    let response = push
        .request(
            reqwest::Method::POST,
            Endpoint::cdn(0, "/"),
            HttpAuthOverride::Unidentified,
        )
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
    if !response.status().is_success() {
        return Err(storage_sync::StorageSyncError::WriteFailed.into());
    }
    Ok(attrs.key)
}

/// `PUT /v1/profile`（官方 putProfile 同端点同 JSON 面；本机凭据经
/// PushService basic auth）。avatar=true 时响应为头像上传属性对象并校验
/// 七字段；avatar=false（移除）响应体不消费。
async fn put_profile(
    push: &PushService,
    profile_key: &ProfileKey,
    aci: Aci,
    update: &ProfileUpdate,
    avatar: bool,
) -> Result<Option<AvatarUploadAttributes>, ProfileError> {
    let cipher = ProfileCipher::new(*profile_key);
    let mut rng = rand::rng();
    let name = ProfileName {
        given_name: update.given_name.as_str(),
        family_name: update.family_name.as_deref(),
    };
    let enc_name = cipher
        .encrypt_name(&name, &mut rng)
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
    let enc_about = cipher
        .encrypt_about(update.about.clone().unwrap_or_default(), &mut rng)
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
    let enc_emoji = cipher
        .encrypt_emoji(update.emoji.clone().unwrap_or_default(), &mut rng)
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
    let commitment = profile_key.get_commitment(aci);

    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ProfileWrite {
        version: String,
        name: String,
        about: String,
        about_emoji: String,
        avatar: bool,
        same_avatar: bool,
        commitment: String,
    }
    use base64::Engine as _;
    let command = ProfileWrite {
        version: profile_key.get_profile_key_version(aci).as_ref().to_owned(),
        name: base64::engine::general_purpose::STANDARD.encode(&enc_name),
        about: base64::engine::general_purpose::STANDARD.encode(&enc_about),
        about_emoji: base64::engine::general_purpose::STANDARD.encode(&enc_emoji),
        avatar,
        same_avatar: false,
        commitment: base64::engine::general_purpose::STANDARD
            .encode(bincode::serialize(&commitment).expect("commitment serializes")),
    };
    let response = push
        .request(
            reqwest::Method::PUT,
            Endpoint::service("v1/profile"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?
        .json(&command)
        .send()
        .await
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
    if !response.status().is_success() {
        return Err(storage_sync::StorageSyncError::WriteFailed.into());
    }
    if !avatar {
        return Ok(None);
    }
    response
        .json::<AvatarUploadAttributes>()
        .await
        .ok()
        .and_then(AvatarUploadAttributes::verified)
        .map(Some)
        .ok_or_else(|| storage_sync::StorageSyncError::WriteFailed.into())
}

/// setProfile 编排（server 面/头像面分流 + storage 面同步，官方
/// updateProfile 同序：profile 写 → storage 写）：
/// - Retain（avatar 缺省）：presage `Manager::update_profile`（
///   retain_avatar=true），storage 面只写 givenName/familyName。
/// - Remove（avatar=null）：自建 `PUT /v1/profile{avatar:false}`，storage
///   面写 givenName/familyName + avatarUrlPath=""。
/// - Replace（avatar=data URI）：自建 `PUT /v1/profile{avatar:true}` →
///   头像加密 + CDN multipart 上行 → storage 面写 givenName/familyName +
///   avatarUrlPath=key。
pub(crate) async fn set_profile(
    context: &StorageContext,
    update: ProfileUpdate,
) -> Result<(), ProfileError> {
    let data = context
        .store
        .load_registration_data()
        .await
        .map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?
        .ok_or(storage_sync::StorageSyncError::UnknownAccount)?;
    let profile_key = data.profile_key();
    let aci = data.service_ids.aci();

    let avatar_url_path: Option<String> = match &update.avatar {
        AvatarIntent::Retain => {
            let mut manager =
                Manager::<SqliteStore, Registered>::load_registered(context.store.clone())
                    .await
                    .map_err(|_| storage_sync::StorageSyncError::UnknownAccount)?;
            let name = ProfileName {
                given_name: update.given_name.clone(),
                family_name: update.family_name.clone(),
            };
            manager
                .update_profile(name, update.about.clone(), update.emoji.clone())
                .await
                .map_err(|_| storage_sync::StorageSyncError::WriteFailed)?;
            None
        }
        AvatarIntent::Remove => {
            put_profile(&context.push, &profile_key, aci, &update, false).await?;
            Some(String::new())
        }
        AvatarIntent::Replace { bytes } => {
            let attrs = put_profile(&context.push, &profile_key, aci, &update, true)
                .await?
                .expect("avatar=true response carries upload attributes");
            let encrypted = encrypt_avatar(&profile_key, bytes);
            Some(post_avatar_to_cdn(attrs, &encrypted).await?)
        }
    };

    // storage 面同步（AccountRecord 读改写，仅动名字/头像 path；409 采纳
    // 重放与幂等无写由 update_account_record 骨架承担）。
    storage_sync::update_account_record(context, |account| {
        Some(storage_sync::account_record_with_profile(
            account,
            &update.given_name,
            update.family_name.as_deref(),
            avatar_url_path.as_deref(),
        ))
    })
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 头像加密/解密交叉验证：encrypt_avatar 产物经上游
    /// `ProfileCipher::decrypt_avatar` 还原（官方 decryptProfile 同构）；
    /// 截断/位翻转密文拒绝（AES-GCM MAC 面）。
    #[test]
    fn avatar_crypto_roundtrip_and_corruption_rejected() {
        let profile_key = ProfileKey::create([7u8; 32]);
        // 明文尾部非零字节（上游 decrypt_avatar 的 unpad 面会剥尾部零）。
        let plaintext = b"\x89PNG-fake-avatar-payload-\xff\xfe\x01";
        let encrypted = encrypt_avatar(&profile_key, plaintext);
        assert_eq!(encrypted.len(), 12 + plaintext.len() + 16);

        let cipher = ProfileCipher::new(profile_key);
        let decrypted = cipher
            .decrypt_avatar(&encrypted)
            .expect("upstream decrypts engine-produced avatar ciphertext");
        assert_eq!(decrypted, plaintext);

        // 截断（< iv+tag）与位翻转 → Err。
        assert!(cipher.decrypt_avatar(&encrypted[..20]).is_err());
        let mut corrupted = encrypted.clone();
        corrupted[20] ^= 0x01;
        assert!(cipher.decrypt_avatar(&corrupted).is_err());
    }

    /// CDN multipart body 官方 WebAPI makePutParams 逐字节同构钉定：字段序
    /// key/x-amz-credential/acl/x-amz-algorithm/x-amz-date/policy/
    /// x-amz-signature/Content-Type + file part（application/octet-stream），
    /// 段间 CRLF、结尾 `--{boundary}--\r\n`。
    #[test]
    fn multipart_body_matches_official_shape() {
        let attrs = AvatarUploadAttributes {
            key: "avatars/ABCDEF".to_owned(),
            credential: "cred/20261005/".to_owned(),
            acl: "public-read".to_owned(),
            algorithm: "AWS4-HMAC-SHA256".to_owned(),
            date: "20261005T000000Z".to_owned(),
            policy: "policy-b64".to_owned(),
            signature: "sig-hex".to_owned(),
        };
        let boundary = "----------------abc123";
        let body = multipart_body(&attrs, b"\x01\x02\x03", boundary);
        let expected = "--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\navatars/ABCDEF\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-credential\"\r\n\r\ncred/20261005/\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"acl\"\r\n\r\npublic-read\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-algorithm\"\r\n\r\nAWS4-HMAC-SHA256\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-date\"\r\n\r\n20261005T000000Z\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"policy\"\r\n\r\npolicy-b64\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"x-amz-signature\"\r\n\r\nsig-hex\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"Content-Type\"\r\n\r\napplication/octet-stream\
\r\n--{b}\r\nContent-Disposition: form-data; name=\"file\"\r\nContent-Type: application/octet-stream\r\n\r\n\x01\x02\x03\
\r\n--{b}--\r\n"
            .replace("{b}", boundary);
        assert_eq!(String::from_utf8(body).expect("utf8"), expected);
    }

    /// 上行属性校验（官方 verifyAttributes 同构）：七字段全非空才可用。
    #[test]
    fn avatar_upload_attributes_require_all_fields() {
        let attrs = AvatarUploadAttributes {
            key: "k".to_owned(),
            credential: "c".to_owned(),
            acl: "a".to_owned(),
            algorithm: "al".to_owned(),
            date: "d".to_owned(),
            policy: "p".to_owned(),
            signature: "s".to_owned(),
        };
        assert!(attrs.verified().is_some());
        let missing = AvatarUploadAttributes {
            key: String::new(),
            credential: "c".to_owned(),
            acl: "a".to_owned(),
            algorithm: "al".to_owned(),
            date: "d".to_owned(),
            policy: "p".to_owned(),
            signature: "s".to_owned(),
        };
        assert!(missing.verified().is_none());
    }

    /// 头像 data URI 参数面：image/* base64 合法；非 image 族/非 base64/
    /// 坏 base64/空载荷/超上限/非 data URI 一律拒绝。
    #[test]
    fn avatar_data_uri_parse_faces() {
        use base64::Engine as _;
        let payload = base64::engine::general_purpose::STANDARD.encode(b"\x89PNG-ok");
        let ok = format!("data:image/png;base64,{payload}");
        let (content_type, bytes) = parse_avatar_data_uri(&ok).expect("image/png ok");
        assert_eq!(content_type, "image/png");
        assert_eq!(bytes, b"\x89PNG-ok");

        // 附加参数（filename）容忍，base64 标志面不区分大小写。
        let with_params = format!("data:image/jpeg;filename=a%20v.jpg;Base64,{payload}");
        assert!(parse_avatar_data_uri(&with_params).is_some());

        // 非 image 族 / 非 base64 / 坏 base64 / 空载荷。
        assert!(parse_avatar_data_uri(&format!("data:text/plain;base64,{payload}")).is_none());
        assert!(parse_avatar_data_uri(&format!("data:image/png,{payload}")).is_none());
        assert!(parse_avatar_data_uri("data:image/png;base64,###").is_none());
        assert!(parse_avatar_data_uri("data:image/png;base64,").is_none());
        // 超上限。
        let big = vec![0x41u8; AVATAR_MAX_BYTES + 1];
        let big_payload = base64::engine::general_purpose::STANDARD.encode(&big);
        assert!(parse_avatar_data_uri(&format!("data:image/png;base64,{big_payload}")).is_none());
        // 非 data URI。
        assert!(parse_avatar_data_uri("https://example.invalid/a.png").is_none());
    }
}
