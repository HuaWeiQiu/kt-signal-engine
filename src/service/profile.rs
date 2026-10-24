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

/// 文字面字节预算（UTF-8 字节数，与 connector 契约 1.46 §4.45 逐字同源：
/// 官方桌面 ProfileEditor.dom.tsx `maxBytes: 128`（两个名字件）/ bio
/// `maxByteCount: 512` / emoji 仓内绑定 32——超预算参数面 fail-closed）。
pub(crate) const GIVEN_NAME_MAX_BYTES: usize = 128;
pub(crate) const FAMILY_NAME_MAX_BYTES: usize = 128;
pub(crate) const ABOUT_MAX_BYTES: usize = 512;
pub(crate) const EMOJI_MAX_BYTES: usize = 32;

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

/// setProfile 错误面：storage 链路四码复用（compat §4.33）；头像上行失败
/// 独立 `PROFILE_UPLOAD_FAILED`（`-1`，data.retryable=true——与 storage 族
/// 同为可重试网络面，但语义可区分：写意图卡在头像链而非 storage 记录面；
/// data.textSaved=true 如实呈现文字面已提交的部分成功）。
#[derive(Debug)]
pub(crate) enum ProfileError {
    Storage(storage_sync::StorageSyncError),
    /// 头像上行链失败（属性校验/CDN POST/网络）。文字面（server profile
    /// 写）已提交成功时 `text_saved=true`——部分成功诚实呈现（storage 面
    /// 同步未执行，多设备以文字面新值、头像旧值收敛，无撕裂）。
    UploadFailed {
        text_saved: bool,
    },
}

impl From<storage_sync::StorageSyncError> for ProfileError {
    fn from(value: storage_sync::StorageSyncError) -> Self {
        ProfileError::Storage(value)
    }
}

/// 文字面字节预算闸（UTF-8 字节数，connector 契约 1.46 同口径）：超预算
/// fail-closed。givenName 非空校验在 methods.rs 解析面（空串同样拒）。
pub(crate) fn text_budget_ok(update: &ProfileUpdate) -> bool {
    update.given_name.len() <= GIVEN_NAME_MAX_BYTES
        && update
            .family_name
            .as_ref()
            .is_none_or(|text| text.len() <= FAMILY_NAME_MAX_BYTES)
        && update
            .about
            .as_ref()
            .is_none_or(|text| text.len() <= ABOUT_MAX_BYTES)
        && update
            .emoji
            .as_ref()
            .is_none_or(|text| text.len() <= EMOJI_MAX_BYTES)
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

/// PUT /v1/profile 响应（官方 AvatarUploadAttributes JSON 形状；校验与
/// CDN 上行经共享件 `cdn_upload`，群面 updateGroup avatar 同一件）。
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
    fn into_fields(self) -> super::cdn_upload::AvatarUploadFields {
        super::cdn_upload::AvatarUploadFields {
            key: self.key,
            credential: self.credential,
            acl: self.acl,
            algorithm: self.algorithm,
            date: self.date,
            policy: self.policy,
            signature: self.signature,
        }
    }
}

/// CDN0 根 POST（共享件 `cdn_upload::post_to_cdn0`；错误面归一头像链
/// 失败）。成功返回头像 path（= 上行属性 `key`）。
async fn post_avatar_to_cdn(
    fields: super::cdn_upload::AvatarUploadFields,
    encrypted: &[u8],
) -> Result<String, ProfileError> {
    super::cdn_upload::post_to_cdn0(fields, encrypted)
        .await
        .map_err(|_| storage_sync::StorageSyncError::WriteFailed.into())
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
) -> Result<Option<super::cdn_upload::AvatarUploadFields>, ProfileError> {
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
        .map(AvatarUploadAttributes::into_fields)
        .and_then(super::cdn_upload::AvatarUploadFields::verified)
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
    debug_assert!(
        text_budget_ok(&update),
        "text budget enforced at the params face"
    );
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
            put_profile(&context.push, &profile_key, aci, &update, false)
                .await
                .map_err(|_| ProfileError::UploadFailed { text_saved: false })?;
            Some(String::new())
        }
        AvatarIntent::Replace { bytes } => {
            // 文字面（server profile 写）先落，头像链失败时如实呈现
            // text_saved=true（官方 writeProfile 同序：putProfile 在
            // uploadAvatar 之前）。
            let attrs = put_profile(&context.push, &profile_key, aci, &update, true)
                .await
                .map_err(|_| ProfileError::UploadFailed { text_saved: true })?
                .expect("avatar=true response carries upload attributes");
            let encrypted = encrypt_avatar(&profile_key, bytes);
            let key = post_avatar_to_cdn(attrs, &encrypted).await?;
            Some(key)
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

    fn update_with(
        given: &str,
        family: Option<&str>,
        about: Option<&str>,
        emoji: Option<&str>,
    ) -> ProfileUpdate {
        ProfileUpdate {
            given_name: given.to_owned(),
            family_name: family.map(str::to_owned),
            about: about.map(str::to_owned),
            emoji: emoji.map(str::to_owned),
            avatar: AvatarIntent::Retain,
        }
    }

    /// 文字面字节预算闸（connector 契约 1.46 同口径）：UTF-8 字节数逐键
    /// 判定——given 128 合法 /129 拒、family 128/129、about 512/513、
    /// emoji 32/33、多字节字符按字节算（43 个 3B 汉字 = 129B 拒）。
    #[test]
    fn text_budget_ladder_per_key() {
        let max_given = "g".repeat(GIVEN_NAME_MAX_BYTES);
        let over_given = "g".repeat(GIVEN_NAME_MAX_BYTES + 1);
        assert!(text_budget_ok(&update_with(&max_given, None, None, None)));
        assert!(!text_budget_ok(&update_with(&over_given, None, None, None)));

        let max_family = "f".repeat(FAMILY_NAME_MAX_BYTES);
        let over_family = "f".repeat(FAMILY_NAME_MAX_BYTES + 1);
        assert!(text_budget_ok(&update_with(
            "g",
            Some(&max_family),
            None,
            None
        )));
        assert!(!text_budget_ok(&update_with(
            "g",
            Some(&over_family),
            None,
            None
        )));

        let max_about = "a".repeat(ABOUT_MAX_BYTES);
        let over_about = "a".repeat(ABOUT_MAX_BYTES + 1);
        assert!(text_budget_ok(&update_with(
            "g",
            None,
            Some(&max_about),
            None
        )));
        assert!(!text_budget_ok(&update_with(
            "g",
            None,
            Some(&over_about),
            None
        )));

        let max_emoji = "e".repeat(EMOJI_MAX_BYTES);
        let over_emoji = "e".repeat(EMOJI_MAX_BYTES + 1);
        assert!(text_budget_ok(&update_with(
            "g",
            None,
            None,
            Some(&max_emoji)
        )));
        assert!(!text_budget_ok(&update_with(
            "g",
            None,
            None,
            Some(&over_emoji)
        )));

        // 字节数非字符数：3 字节 UTF-8（汉字）43 个 = 129 字节，字符数 43
        // 远小于各预算——按字节 fail-closed。
        let multibyte = "名".repeat(43);
        assert_eq!(multibyte.len(), 129);
        assert!(!text_budget_ok(&update_with(&multibyte, None, None, None)));
    }

    /// 部分成功语义的编码面：UploadFailed携带 text_saved 标志（Replace 分支
    /// = true、Remove 分支 = false），错误类型本身不携带头像字节。
    #[test]
    fn upload_failed_carries_partial_success_flag() {
        let error = ProfileError::UploadFailed { text_saved: true };
        let ProfileError::UploadFailed { text_saved } = error else {
            panic!("UploadFailed expected");
        };
        assert!(text_saved);
        let error = ProfileError::UploadFailed { text_saved: false };
        let ProfileError::UploadFailed { text_saved } = error else {
            panic!("UploadFailed expected");
        };
        assert!(!text_saved);
    }
}
