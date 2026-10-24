// SPDX-License-Identifier: AGPL-3.0-only

//! 设备清单读面 + 服务端 unlink（窗D B13，缺口总登记）。
//!
//! 官方行为参照（AGPL 只读锚，非源码复制）：`textsecure/WebAPI.preload.ts`
//! getDevices（GET v1/devices，:3044）/ unlink（DELETE v1/devices/{id}，
//! :3011-3021）；`Crypto.node.ts` decryptDeviceName（:142，ECDH → HMAC
//! 双链 → AES-256-CTR + synthetic IV 验证）；`protos/DeviceName.proto`
//! （ephemeralPublic 1 / syntheticIv 2 / ciphertext 3）。
//!
//! 引擎此前无设备管理面：connector/desktop 无通道看本账号挂了哪些设备、
//! 也无法从引擎侧发起服务端解绑（unlink）。本批把两腿接上：设备清单
//! 读投影（设备名引擎侧解密——本机 ACI 身份私钥在 presage store）+
//! DELETE 端点直通（服务器裁决授权：primary 可解绑任意 linked，linked
//! 可解绑自身，越权服务器拒绝原样透出）。
//!
//! createdAtCiphertext 记档透传：官方 Crypto.node.ts 以 identityPublic.
//! seal（'deviceCreatedAt' + deviceId||registrationId 关联数据）解密，
//! v0.99.0 rust crypto 无该 seal/unseal 暴露位——ciphertext 原样透传，
//! 解密面归消费方或另批。

use base64::Engine as _;
use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::protocol::{IdentityKeyStore as _, PrivateKey};
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::store::Store as _;
use prost::Message as _;

use super::StorageContext;

/// 设备面错误：UnknownAccount = 账号参数不指向已链接账号（-32603）；
/// Unavailable = 传输/网络面；Failed = 服务器拒绝（status 诚实透传）。
#[derive(Debug)]
pub(crate) enum DevicesError {
    UnknownAccount,
    Failed { status: Option<u16> },
    Unavailable,
}

/// StorageSyncError → DevicesError（账号面复用折叠口径）。
impl From<crate::service::storage_sync::StorageSyncError> for DevicesError {
    fn from(value: crate::service::storage_sync::StorageSyncError) -> Self {
        match value {
            crate::service::storage_sync::StorageSyncError::UnknownAccount => {
                DevicesError::UnknownAccount
            }
            _ => DevicesError::Unavailable,
        }
    }
}

/// GET v1/devices 单条（服务器 DeviceInfo 形状）。vendored
/// `DeviceInfoEncrypted` serde 同构镜像——该类型 pub(crate) 不可达，本地
/// 同形自持；未知字段 serde 忽略。
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeviceInfoServer {
    id: u32,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    registration_id: Option<i32>,
    #[serde(default)]
    created_at_ciphertext: Option<String>,
    #[serde(default)]
    last_seen: Option<u64>,
}

/// 设备投影（getDevices 返回形状的数据面）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct DeviceProjection {
    pub(crate) device_id: u32,
    /// 解密后设备名（UTF-8）；加密名缺席/解密失败 → None（诚实缺省，
    /// 官方桌面解密失败 warn 不炸面同域）。
    pub(crate) name: Option<String>,
    pub(crate) registration_id: Option<i32>,
    /// 服务器 lastSeen（毫秒）。
    pub(crate) last_seen_ms: Option<u64>,
    /// 服务器 createdAtCiphertext 原样 base64（解密面记档见模块注释）。
    pub(crate) created_at_ciphertext: Option<String>,
}

/// DeviceName proto（官方 DeviceName.proto 三字段）。
#[derive(Clone, PartialEq, prost::Message)]
struct DeviceNameProto {
    #[prost(bytes = "vec", optional, tag = "1")]
    ephemeral_public: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "2")]
    synthetic_iv: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "3")]
    ciphertext: Option<Vec<u8>>,
}

/// 设备名解密（官方 Crypto.node.ts decryptDeviceName 同构）：masterSecret
/// = X25519(ephemeralPublic, 本机身份私钥) → cipherKey = HMAC(HMAC(ms,
/// "cipher"), syntheticIv) → AES-256-CTR（零计数器）解密 → synthetic IV
/// 验证（HMAC(HMAC(ms, "auth"), plaintext) 前 16B）。任何形状/解密/
/// 验证失败 → None。
pub(crate) fn decrypt_device_name(
    encrypted: &[u8],
    identity_private: &PrivateKey,
) -> Option<String> {
    use hmac::{Hmac, KeyInit as _, Mac as _};
    use sha2::Sha256;

    fn hmac_bytes(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    let proto = DeviceNameProto::decode(encrypted).ok()?;
    let ephemeral_public = proto.ephemeral_public?;
    let synthetic_iv = proto.synthetic_iv?;
    let ciphertext = proto.ciphertext?;
    if synthetic_iv.len() != 16 {
        return None;
    }
    // ephemeralPublic 是官方序列化形（DJB 0x05 类型前缀 + 32B，桌面
    // encryptDeviceName 侧 PublicKey.serialize() 同源）；deserialize 校验
    // 类型字节与长度。
    let ephemeral =
        presage::libsignal_service::protocol::PublicKey::deserialize(ephemeral_public.as_slice())
            .ok()?;
    let master_secret = identity_private.calculate_agreement(&ephemeral).ok()?;
    let key2 = hmac_bytes(&master_secret, b"cipher");
    let cipher_key = hmac_bytes(&key2, &synthetic_iv);

    use cbc::cipher::{KeyIvInit as _, StreamCipher as _};
    use ctr::Ctr128BE;
    type Aes256Ctr = Ctr128BE<aes::Aes256>;
    let mut cipher = Aes256Ctr::new_from_slices(&cipher_key, &[0u8; 16]).ok()?;
    let mut plaintext = ciphertext.clone();
    cipher.apply_keystream(&mut plaintext);

    let key1 = hmac_bytes(&master_secret, b"auth");
    let our_synthetic_iv = hmac_bytes(&key1, &plaintext);
    if our_synthetic_iv.get(..16)? != synthetic_iv.as_slice() {
        return None;
    }
    String::from_utf8(plaintext).ok()
}

/// GET v1/devices → 投影（存储/服务器序，含本机设备）。设备名解密用本机
/// ACI 身份私钥（presage store；取钥失败 → 全部设备名置空不炸面）。
pub(crate) async fn list_devices(
    context: &StorageContext,
) -> Result<Vec<DeviceProjection>, DevicesError> {
    let response = context
        .push
        .request(
            reqwest::Method::GET,
            Endpoint::service("v1/devices"),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| DevicesError::Unavailable)?
        .send()
        .await
        .map_err(|_| DevicesError::Unavailable)?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(DevicesError::Failed {
            status: Some(status),
        });
    }
    #[derive(serde::Deserialize)]
    struct DeviceInfoList {
        #[serde(default)]
        devices: Vec<DeviceInfoServer>,
    }
    let list: DeviceInfoList = response
        .json()
        .await
        .map_err(|_| DevicesError::Unavailable)?;

    let identity_private = context
        .store
        .aci_protocol_store()
        .get_identity_key_pair()
        .await
        .ok()
        .map(|pair| *pair.private_key());
    Ok(list
        .devices
        .into_iter()
        .map(|device| {
            let name = match (&device.name, &identity_private) {
                (Some(name_b64), Some(private_key)) => base64::engine::general_purpose::STANDARD
                    .decode(name_b64)
                    .ok()
                    .and_then(|bytes| decrypt_device_name(&bytes, private_key)),
                _ => None,
            };
            DeviceProjection {
                device_id: device.id,
                name,
                registration_id: device.registration_id,
                last_seen_ms: device.last_seen,
                created_at_ciphertext: device.created_at_ciphertext,
            }
        })
        .collect())
}

/// DELETE v1/devices/{id}（官方 unlink 同端点）：服务器裁决授权（primary
/// 解绑任意 linked、linked 解绑自身），越权/主设备删除等拒绝原样透出
/// （Failed{status}）。
pub(crate) async fn unlink_device(
    context: &StorageContext,
    device_id: u32,
) -> Result<(), DevicesError> {
    let path = format!("v1/devices/{device_id}");
    let response = context
        .push
        .request(
            reqwest::Method::DELETE,
            Endpoint::service(&path),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| DevicesError::Unavailable)?
        .send()
        .await
        .map_err(|_| DevicesError::Unavailable)?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(DevicesError::Failed {
            status: Some(status),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::protocol::KeyPair;

    /// encrypt（官方 encryptDeviceName 同构，测试侧自持）→ decrypt 往返：
    /// 明文还原；篡改 ciphertext / synthetic IV → None（synthetic IV 闸
    /// 生效）；坏形状（ephemeralPublic ≠32B / syntheticIv ≠16B）→ None。
    #[test]
    fn device_name_roundtrip_and_tamper_rejected() {
        use cbc::cipher::{KeyIvInit as _, StreamCipher as _};
        use hmac::{Hmac, KeyInit as _, Mac as _};
        use sha2::Sha256;

        fn hmac_bytes(key: &[u8], data: &[u8]) -> Vec<u8> {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac accepts any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }

        let identity = KeyPair::generate(&mut rand::rng());
        let device_name = "tanye 的 iPhone";

        // 加密侧（官方 encryptDeviceName 序列）。
        let ephemeral = KeyPair::generate(&mut rand::rng());
        let master_secret = identity
            .private_key
            .calculate_agreement(&ephemeral.public_key)
            .expect("agreement")
            .to_vec();
        let key1 = hmac_bytes(&master_secret, b"auth");
        let synthetic_iv = hmac_bytes(&key1, device_name.as_bytes())[..16].to_vec();
        let key2 = hmac_bytes(&master_secret, b"cipher");
        let cipher_key = hmac_bytes(&key2, &synthetic_iv);
        let mut cipher =
            ctr::Ctr128BE::<aes::Aes256>::new_from_slices(&cipher_key, &[0u8; 16]).expect("ctr");
        let mut ciphertext = device_name.as_bytes().to_vec();
        cipher.apply_keystream(&mut ciphertext);

        let proto = DeviceNameProto {
            // 官方 wire 形：PublicKey.serialize() = DJB 0x05 前缀 + 32B。
            ephemeral_public: Some(ephemeral.public_key.serialize().to_vec()),
            synthetic_iv: Some(synthetic_iv.clone()),
            ciphertext: Some(ciphertext.clone()),
        };
        let encrypted = proto.encode_to_vec();

        let decrypted = decrypt_device_name(&encrypted, &identity.private_key).expect("decrypts");
        assert_eq!(decrypted, device_name);

        // 篡改 ciphertext → synthetic IV 验证拒绝。
        let mut tampered = proto.clone();
        let mut bad = tampered.ciphertext.clone().expect("ciphertext");
        bad[0] ^= 0xff;
        tampered.ciphertext = Some(bad);
        assert!(decrypt_device_name(&tampered.encode_to_vec(), &identity.private_key).is_none());

        // 篡改 synthetic IV → 同闸拒绝。
        let mut tampered = proto.clone();
        let mut bad_iv = tampered.synthetic_iv.clone().expect("iv");
        bad_iv[0] ^= 0xff;
        tampered.synthetic_iv = Some(bad_iv);
        assert!(decrypt_device_name(&tampered.encode_to_vec(), &identity.private_key).is_none());

        // 坏形状：ephemeralPublic 缺类型前缀（裸 32B）/截断 → None。
        let mut unprefixed = proto.clone();
        unprefixed.ephemeral_public = Some(ephemeral.public_key.public_key_bytes().to_vec());
        assert!(
            decrypt_device_name(&unprefixed.encode_to_vec(), &identity.private_key).is_none(),
            "raw 32B without DJB prefix must be rejected"
        );
        let mut short = proto;
        short.ephemeral_public = Some(vec![1u8; 8]);
        assert!(decrypt_device_name(&short.encode_to_vec(), &identity.private_key).is_none());
    }
}
