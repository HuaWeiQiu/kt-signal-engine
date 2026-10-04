// SPDX-License-Identifier: AGPL-3.0-only

//! 贴纸包浏览（P4 扩面）：匿名 CDN manifest 下载 + 解密 + 投影，与单图下载。
//!
//! 官方行为参照（Signal-Desktop，行为对齐不抄码）：`ts/textsecure/WebAPI.preload.ts`
//! `getStickerPackManifest`/`getSticker`（`cdnUrlObject['0']` = cdn.signal.org，
//! 匿名 GET，无需认证）、`ts/types/Stickers.preload.ts`
//! `parseStickerPackManifest`（≤1024 截断、cover 兜底 stickers[0]、空包
//! malformed）、`ts/util/sniffImageMimeType.std.ts`（魔数嗅探，失败回退
//! image/webp）。解密面用上游 pub API：`sticker_cipher::derive_key`
//! （HKDF-SHA256, salt=32 零字节, info="Sticker Pack"）+
//! `attachment_cipher::decrypt_in_place`（[16B iv][AES-256-CBC][32B HMAC]）。
//!
//! 纪律（compat §1）：本层不产生任何 stderr 输出；网络层薄壳不做网络测试。

use base64::Engine as _;
use presage::libsignal_service::configuration::{Endpoint, SignalServers};
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::proto::Pack;
use presage::libsignal_service::sticker_cipher;
use presage::libsignal_service::{attachment_cipher, push_service::HttpAuthOverride};
use prost::Message;

/// manifest 贴纸数上限：官方 MAX_STICKERS_PER_PACK=1024，超过截断不报错。
const MAX_STICKERS_PER_PACK: usize = 1024;

/// 单图字节上限：官方桌面注释「服务端强制 ≤300KB」，超限 fail-closed。
const STICKER_MAX_BYTES: usize = 300 * 1024;

/// manifest proto 字节数上限（解密前防御面）：1024 贴纸上限下真实包远小于
/// 此，超限按服务端异常拒绝，不做无界解密。
const MANIFEST_MAX_BYTES: usize = 1024 * 1024;

/// 贴纸包面错误（折叠进 JSON-RPC 错误码由 methods.rs 固定，本层只分类）。
#[derive(Debug)]
pub(crate) enum StickerError {
    /// packId 非 32 位 hex / packKey 非 base64 32 字节。
    InvalidParams,
    /// CDN 下载失败（网络/非 200）。
    Fetch,
    /// HMAC 校验失败（packKey 与包不匹配）。
    KeyInvalid,
    /// proto 解码失败 / cover 与 stickers 都空。
    Malformed,
    /// 单图超 300KB。
    ImageTooLarge,
}

/// manifest 投影（compat §3 扩展方法行同构）。
pub(crate) struct ManifestProjection {
    pub(crate) title: String,
    pub(crate) author: String,
    pub(crate) cover: Option<StickerProjection>,
    pub(crate) stickers: Vec<StickerProjection>,
    pub(crate) sticker_count: u32,
}

pub(crate) struct StickerProjection {
    pub(crate) id: u32,
    pub(crate) emoji: Option<String>,
    pub(crate) content_type: Option<String>,
}

/// 单图投影。
pub(crate) struct ImageProjection {
    pub(crate) data_base64: String,
    pub(crate) content_type: String,
    pub(crate) size: u32,
}

/// packId 校验（官方 isPackIdValid：`/^[0-9a-f]{32}$/i`）并解码 16 字节。
pub(crate) fn decode_pack_id(pack_id: &str) -> Option<Vec<u8>> {
    let bytes = crate::service::hex_decode(pack_id)?;
    (pack_id.len() == 32 && bytes.len() == 16).then_some(bytes)
}

/// packKey 校验：base64 解码后必须恰好 32 字节（官方 STICKERPACK_KEY_BYTE_LEN）。
pub(crate) fn decode_pack_key(pack_key: &str) -> Option<[u8; 32]> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(pack_key.trim().as_bytes())
        .ok()?;
    bytes.try_into().ok()
}

/// 匿名 GET `stickers/{packId}/manifest.proto`（cdn 0 = cdn.signal.org）。
async fn fetch(pack_id: &str, path: &str) -> Result<Vec<u8>, StickerError> {
    let push = PushService::new(SignalServers::Production, None, crate::service::USER_AGENT);
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::cdn(0, format!("stickers/{pack_id}/{path}")),
            HttpAuthOverride::Unidentified,
        )
        .map_err(|_| StickerError::Fetch)?
        .send()
        .await
        .map_err(|_| StickerError::Fetch)?;
    if !response.status().is_success() {
        return Err(StickerError::Fetch);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| StickerError::Fetch)?
        .to_vec();
    if bytes.len() > MANIFEST_MAX_BYTES {
        return Err(StickerError::Fetch);
    }
    Ok(bytes)
}

/// 解密 + proto 解码 manifest（round-trip 与真实夹具共用核心）。
pub(crate) fn decrypt_manifest(
    pack_key: &[u8; 32],
    ciphertext: &mut Vec<u8>,
) -> Result<Pack, StickerError> {
    let key = sticker_cipher::derive_key(pack_key).map_err(|_| StickerError::KeyInvalid)?;
    attachment_cipher::decrypt_in_place(key, ciphertext).map_err(|error| match error {
        presage::libsignal_service::attachment_cipher::AttachmentCipherError::MacError => {
            StickerError::KeyInvalid
        }
        presage::libsignal_service::attachment_cipher::AttachmentCipherError::PaddingError => {
            StickerError::Malformed
        }
    })?;
    Pack::decode(ciphertext.as_slice()).map_err(|_| StickerError::Malformed)
}

/// manifest 投影（官方 parseStickerPackManifest 语义：≤1024 截断、cover
/// 缺省兜底 stickers[0]、都空 malformed；proto contentType 官方 manifest
/// 通常缺省 → 原样透出 Option）。
pub(crate) fn project_manifest(pack: &Pack) -> Result<ManifestProjection, StickerError> {
    let mut stickers: &[presage::proto::pack::Sticker] = &pack.stickers;
    if stickers.len() > MAX_STICKERS_PER_PACK {
        stickers = &stickers[..MAX_STICKERS_PER_PACK];
    }
    if pack.cover.is_none() && stickers.is_empty() {
        return Err(StickerError::Malformed);
    }
    let cover = pack.cover.as_ref().map(project_sticker);
    let projected: Vec<StickerProjection> = stickers.iter().map(project_sticker).collect();
    Ok(ManifestProjection {
        title: pack.title.clone().unwrap_or_default(),
        author: pack.author.clone().unwrap_or_default(),
        cover,
        sticker_count: projected
            .len()
            .try_into()
            .unwrap_or(MAX_STICKERS_PER_PACK as u32),
        stickers: projected,
    })
}

fn project_sticker(sticker: &presage::proto::pack::Sticker) -> StickerProjection {
    StickerProjection {
        id: sticker.id.unwrap_or(0),
        emoji: sticker.emoji.clone().filter(|emoji| !emoji.is_empty()),
        content_type: sticker.content_type.clone().filter(|kind| !kind.is_empty()),
    }
}

/// getStickerPackManifest 网络编排：下载 → 解密 → 投影。
pub(crate) async fn fetch_manifest(
    pack_id: &str,
    pack_key: &[u8; 32],
) -> Result<ManifestProjection, StickerError> {
    let mut ciphertext = fetch(pack_id, "manifest.proto").await?;
    let pack = decrypt_manifest(pack_key, &mut ciphertext)?;
    project_manifest(&pack)
}

/// getStickerImage 网络编排：下载单图（≤300KB fail-closed）→ 解密 → 嗅探。
pub(crate) async fn fetch_image(
    pack_id: &str,
    pack_key: &[u8; 32],
    sticker_id: u32,
) -> Result<ImageProjection, StickerError> {
    let mut ciphertext = fetch(pack_id, &format!("full/{sticker_id}")).await?;
    if ciphertext.len() > STICKER_MAX_BYTES {
        return Err(StickerError::ImageTooLarge);
    }
    let key = sticker_cipher::derive_key(pack_key).map_err(|_| StickerError::KeyInvalid)?;
    attachment_cipher::decrypt_in_place(key, &mut ciphertext)
        .map_err(|_| StickerError::KeyInvalid)?;
    let size = ciphertext
        .len()
        .try_into()
        .map_err(|_| StickerError::Malformed)?;
    Ok(ImageProjection {
        data_base64: base64::engine::general_purpose::STANDARD.encode(&ciphertext),
        content_type: sniff_image_mime_type(&ciphertext)
            .unwrap_or("image/webp")
            .to_owned(),
        size,
    })
}

/// 官方 sniffImageMimeType 语义（WHATWG mimesniff image 节）：ICO/BMP/
/// GIF87a/GIF89a/WEBP(RIFF..WEBP..VP8)/PNG/JPEG 魔数嗅探，失败 None 由调用
/// 方回退 image/webp。
pub(crate) fn sniff_image_mime_type(bytes: &[u8]) -> Option<&'static str> {
    /// (pattern, mask, mime)；mask=None 即逐字节精确匹配。
    type Magic = (&'static [u8], Option<&'static [u8]>, &'static str);
    const TYPES: &[Magic] = &[
        (b"\x00\x00\x01\x00", None, "image/x-icon"),
        (b"\x00\x00\x02\x00", None, "image/x-icon"),
        (b"BM", None, "image/bmp"),
        (b"GIF87a", None, "image/gif"),
        (b"GIF89a", None, "image/gif"),
        (
            b"RIFF\x00\x00\x00\x00WEBPVP",
            Some(b"\xff\xff\xff\xff\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff"),
            "image/webp",
        ),
        (b"\x89PNG\r\n\x1a\n", None, "image/png"),
        (b"\xff\xd8\xff", None, "image/jpeg"),
    ];
    for (pattern, mask, mime) in TYPES {
        if bytes.len() < pattern.len() {
            continue;
        }
        let hit = pattern.iter().enumerate().all(|(index, expected)| {
            let mask_byte = mask.map(|mask| mask[index]).unwrap_or(0xff);
            bytes[index] & mask_byte == *expected
        });
        if hit {
            return Some(mime);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::proto::pack::Sticker;

    /// 造 manifest 密文：官方 encrypt_in_place（[iv][cbc][mac] 布局）。
    fn encrypt(pack_key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
        let key = sticker_cipher::derive_key(pack_key).expect("derive");
        let mut buf = plaintext.to_vec();
        attachment_cipher::encrypt_in_place([7u8; 16], key, &mut buf);
        buf
    }

    fn manifest_proto_bytes() -> Vec<u8> {
        use prost::Message;
        Pack {
            title: Some("Demo".to_owned()),
            author: Some("KT".to_owned()),
            cover: Some(Sticker {
                id: Some(0),
                emoji: Some("😀".to_owned()),
                content_type: None,
            }),
            stickers: vec![
                Sticker {
                    id: Some(0),
                    emoji: Some("😀".to_owned()),
                    content_type: None,
                },
                Sticker {
                    id: Some(1),
                    emoji: None,
                    content_type: Some("image/png".to_owned()),
                },
            ],
        }
        .encode_to_vec()
    }

    fn sample_pack_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (index, byte) in key.iter_mut().enumerate() {
            *byte = index as u8;
        }
        key
    }

    /// 加解密 round-trip（明文 proto 面）。
    #[test]
    fn manifest_roundtrip_decrypt() {
        let key = sample_pack_key();
        let mut ciphertext = encrypt(&key, &manifest_proto_bytes());
        let pack = decrypt_manifest(&key, &mut ciphertext).expect("decrypts");
        assert_eq!(pack.title.as_deref(), Some("Demo"));
        assert_eq!(pack.author.as_deref(), Some("KT"));
        assert_eq!(pack.stickers.len(), 2);
    }

    /// Bad MAC 失败面（packKey 错）→ KeyInvalid。
    #[test]
    fn manifest_bad_key_is_key_invalid() {
        let mut ciphertext = encrypt(&sample_pack_key(), &manifest_proto_bytes());
        let mut wrong = [9u8; 32];
        wrong[0] ^= 0xff;
        assert!(matches!(
            decrypt_manifest(&wrong, &mut ciphertext),
            Err(StickerError::KeyInvalid)
        ));
    }

    /// 截断密文：尾部 MAC 域被截 → MacError → KeyInvalid（MAC 校验在 padding
    /// 之前，密文损坏与错 key 密学不可区分，同码位 fail-closed；PaddingError
    /// 面需伪造合法 MAC 才能触达，不为此引入 hmac 直依赖）。
    #[test]
    fn manifest_truncated_ciphertext_fails_closed() {
        let key = sample_pack_key();
        let mut ciphertext = encrypt(&key, &manifest_proto_bytes());
        ciphertext.truncate(ciphertext.len() - 8);
        assert!(matches!(
            decrypt_manifest(&key, &mut ciphertext),
            Err(StickerError::KeyInvalid)
        ));
    }

    /// 官方夹具真实解密：Signal-Desktop fixtures 的加密贴纸包（密文 +
    /// 配套 key，源 `ts/test-mock/storage/fixtures.node.ts` STICKER_PACKS），
    /// title/author/贴纸面逐项断言。
    #[test]
    fn official_fixture_manifest_decrypts_and_projects() {
        // packId 也过 decode_pack_id 校验（官方仅小写 hex，32 字符 = 16 字节）。
        let pack_id = "ae8fedafda4768fd3384d4b3b9db963d";
        assert_eq!(
            decode_pack_id(pack_id).expect("official pack id"),
            crate::service::hex_decode(pack_id).expect("hex")
        );
        // fixtures.node.ts：Buffer.from('53f4…73ca', 'hex')，base64 形同值。
        let pack_key = decode_pack_key("U/Sqi5Xhwuda+rIyj+Z+ttev+81PUM1NqJ38Ml28c8o=")
            .expect("valid pack key");
        assert_eq!(
            pack_key,
            hex_to_32("53f4aa8b95e1c2e75afab2328fe67eb6d7affbcd4f50cd4da89dfc325dbc73ca")
        );

        let mut ciphertext =
            include_bytes!("../../tests/fixtures/stickerpack-ae8fedafda4768fd3384d4b3b9db963d.bin")
                .to_vec();
        let pack = decrypt_manifest(&pack_key, &mut ciphertext).expect("official fixture decrypts");
        assert_eq!(pack.title.as_deref(), Some("Demo Stickerpack"));
        assert_eq!(pack.author.as_deref(), Some("Test"));
        let projection = project_manifest(&pack).expect("projects");
        assert_eq!(projection.title, "Demo Stickerpack");
        assert_eq!(projection.author, "Test");
        assert_eq!(projection.sticker_count, 1);
        assert_eq!(projection.cover.as_ref().expect("cover").id, 0);
        assert_eq!(projection.stickers[0].emoji.as_deref(), Some("😀"));
        // proto contentType 官方 manifest 缺省 → 投影 null（None）。
        assert_eq!(projection.stickers[0].content_type, None);

        // 单图夹具（cover id=0）：解密 + WEBP 魔数嗅探。
        let mut image = include_bytes!(
            "../../tests/fixtures/stickerpack-ae8fedafda4768fd3384d4b3b9db963d-0.bin"
        )
        .to_vec();
        assert!(image.len() <= STICKER_MAX_BYTES);
        let key = sticker_cipher::derive_key(&pack_key).expect("derive");
        attachment_cipher::decrypt_in_place(key, &mut image).expect("image decrypts");
        assert_eq!(sniff_image_mime_type(&image), Some("image/webp"));
        assert_eq!(&image[..4], b"RIFF");
    }

    fn hex_to_32(hex: &str) -> [u8; 32] {
        let bytes = crate::service::hex_decode(hex).expect("hex");
        bytes.try_into().expect("32 bytes")
    }

    /// 1024 截断语义：1200 贴纸 → stickerCount=1024 不报错。
    #[test]
    fn manifest_truncates_over_1024_stickers() {
        let stickers = (0..1200)
            .map(|index| Sticker {
                id: Some(index as u32),
                emoji: None,
                content_type: None,
            })
            .collect();
        let pack = Pack {
            title: Some("big".to_owned()),
            author: None,
            cover: None,
            stickers,
        };
        let projection = project_manifest(&pack).expect("truncates not errors");
        assert_eq!(projection.sticker_count, 1024);
        assert_eq!(projection.stickers.len(), 1024);
        assert_eq!(projection.stickers[1023].id, 1023);
    }

    /// cover 与 stickers 都空 → malformed。
    #[test]
    fn manifest_empty_pack_is_malformed() {
        assert!(matches!(
            project_manifest(&Pack::default()),
            Err(StickerError::Malformed)
        ));
    }

    /// cover 缺省兜底语义保留：官方投影层保留 null 由桌面兜底 stickers[0]，
    /// proto 原样透出（本投影 cover=None、stickers 照常）。
    #[test]
    fn manifest_without_cover_keeps_none() {
        let pack = Pack {
            title: None,
            author: None,
            cover: None,
            stickers: vec![Sticker {
                id: Some(3),
                emoji: Some("🎉".to_owned()),
                content_type: None,
            }],
        };
        let projection = project_manifest(&pack).expect("projects");
        assert!(projection.cover.is_none());
        assert_eq!(projection.title, "");
        assert_eq!(projection.stickers[0].id, 3);
    }

    /// packId/packKey 校验失败面（fail-closed）。
    #[test]
    fn param_validation_fails_closed() {
        assert!(decode_pack_id("ae8fedafda4768fd3384d4b3b9db963d").is_some());
        assert!(
            decode_pack_id("AE8FEDAFDA4768FD3384D4B3B9DB963D").is_none(),
            "大写非官方 regexp"
        );
        assert!(
            decode_pack_id("ae8fedafda4768fd3384d4b3b9db963").is_none(),
            "31 位"
        );
        assert!(
            decode_pack_id("ae8fedafda4768fd3384d4b3b9db963dd").is_none(),
            "33 位"
        );
        assert!(
            decode_pack_id("ze8fedafda4768fd3384d4b3b9db963d").is_none(),
            "非 hex"
        );

        let key = sample_pack_key();
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(key);
        assert_eq!(decode_pack_key(&encoded), Some(key));
        assert_eq!(decode_pack_key("not base64!"), None);
        assert_eq!(
            decode_pack_key(&base64::engine::general_purpose::STANDARD.encode([7u8; 16])),
            None,
            "16 字节不足"
        );
        assert_eq!(
            decode_pack_key(&base64::engine::general_purpose::STANDARD.encode([7u8; 33])),
            None,
            "33 字节超限"
        );
    }

    /// MIME 嗅探全谱 + 失败回退面。
    #[test]
    fn mime_sniffing_matches_official_table() {
        assert_eq!(
            sniff_image_mime_type(b"\x00\x00\x01\x00rest"),
            Some("image/x-icon")
        );
        assert_eq!(
            sniff_image_mime_type(b"\x00\x00\x02\x00"),
            Some("image/x-icon")
        );
        assert_eq!(sniff_image_mime_type(b"BMxx"), Some("image/bmp"));
        assert_eq!(sniff_image_mime_type(b"GIF87a.."), Some("image/gif"));
        assert_eq!(sniff_image_mime_type(b"GIF89a.."), Some("image/gif"));
        let mut webp = *b"RIFF\x00\x00\x00\x00WEBPVP8 ";
        webp[4] = 0x12;
        assert_eq!(sniff_image_mime_type(&webp), Some("image/webp"));
        assert_eq!(
            sniff_image_mime_type(b"\x89PNG\r\n\x1a\n"),
            Some("image/png")
        );
        assert_eq!(
            sniff_image_mime_type(b"\xff\xd8\xff\xe0"),
            Some("image/jpeg")
        );
        assert_eq!(sniff_image_mime_type(b"plain text"), None);
        assert_eq!(
            sniff_image_mime_type(b"BM"),
            Some("image/bmp"),
            "前缀命中即可"
        );
        assert_eq!(sniff_image_mime_type(b""), None);
    }
}
