// SPDX-License-Identifier: AGPL-3.0-only

//! 群邀请链接纯函数面（KT 扩展批 P13/任务1）：链接密码生成、
//! `GroupInviteLink` proto 构造与解析。零网络、零账号态，fake/real 同码路。
//!
//! 官方行为锚（Signal-Desktop abe80d3 行为对齐，不抄码）：
//! - 密码 = `generateGroupInviteLinkPassword`（groups.preload.ts:266）：
//!   16 字节 CSPRNG（`GROUP_INVITE_LINK_PASSWORD_LENGTH = 16`）。
//! - 构造 = `buildGroupLink`（groups.preload.ts:287）：`GroupInviteLink`
//!   proto（contentsV1{groupMasterKey(32B), inviteLinkPassword}）编码 →
//!   标准 base64 → websafe 化（`+`→`-`、`/`→`_`、去 `=`，webSafeBase64.
//!   std.ts 同规则）→ `https://signal.group/#<inviteCode>`
//!   （signalRoutes.std.ts groupInvitesRoute.toWebUrl 同形）。
//! - 解析 = `parseGroupLink`（groups.preload.ts:316）：入参为 inviteCode
//!   （桌面路由层从 URL hash 段摘出，main.main.ts handleSignalRoute）；
//!   websafe 还原（补 `=` 到 4 倍数）→ proto 解码 → 缺 contentsV1 =
//!   官方 LINK_VERSION_ERROR 族、masterKey 空/非 32B、password 空 = 容错
//!   抛错（官方逐条 throw 面）。
//! - URL 形状：`https://signal.group/#…` / `sgnl://signal.group/#…` /
//!   `sgnl://joingroup#…` 三形（groupInvitesRoute patterns 同集）。

use base64::Engine as _;
use prost::Message;

/// 链接密码长度（官方 GROUP_INVITE_LINK_PASSWORD_LENGTH = 16，字节）。
pub(crate) const LINK_PASSWORD_BYTES: usize = 16;

/// 群 master key 长度（32B；官方 parseGroupLink 以 base64 长 44 判定同约束）。
const MASTER_KEY_BYTES: usize = 32;

/// 16 字节随机链接密码（CSPRNG；官方 generateGroupInviteLinkPassword 同长）。
pub(crate) fn generate_invite_link_password() -> [u8; LINK_PASSWORD_BYTES] {
    use rand::RngCore as _;
    let mut bytes = [0u8; LINK_PASSWORD_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

/// 标准 base64 → websafe（官方 toWebSafeBase64 同规则：`+`→`-`、`/`→`_`、
/// 去 `=`）。
pub(crate) fn to_web_safe_base64(standard: &str) -> String {
    standard
        .replace('/', "_")
        .replace('+', "-")
        .replace('=', "")
}

/// websafe → 标准 base64（官方 fromWebSafeBase64 同规则：`_`→`/`、`-`→`+`、
/// 补 `=` 到 4 倍数）。
pub(crate) fn from_web_safe_base64(web_safe: &str) -> String {
    let base64 = web_safe.replace('_', "/").replace('-', "+");
    match base64.len() % 4 {
        2 => format!("{base64}=="),
        3 => format!("{base64}="),
        // remainder 1 官方落 `===`（非法 base64 长度），解码面统一报错。
        _ => base64,
    }
}

/// 链接构造（官方 buildGroupLink 行为对齐）：`GroupInviteLink` proto
///（contentsV1）→ 标准 base64 → websafe → `https://signal.group/#<code>`。
pub(crate) fn build_invite_link(master_key: &[u8], password: &[u8]) -> String {
    let bytes = presage::proto::GroupInviteLink {
        contents: Some(presage::proto::group_invite_link::Contents::ContentsV1(
            presage::proto::group_invite_link::GroupInviteLinkContentsV1 {
                group_master_key: master_key.to_vec(),
                invite_link_password: password.to_vec(),
            },
        )),
    }
    .encode_to_vec();
    let standard = base64::engine::general_purpose::STANDARD.encode(bytes);
    format!("https://signal.group/#{}", to_web_safe_base64(&standard))
}

/// 链接解析结果（master key + 密码原字节）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedInviteLink {
    pub(crate) master_key: Vec<u8>,
    pub(crate) password: Vec<u8>,
}

/// 链接解析错误（wire 面统一 -32603；变体供单测钉容错分类）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkParseError {
    /// 无 hash/inviteCode 段或空串。
    Empty,
    /// base64/proto 解码失败或 masterKey/password 形状非法。
    Malformed,
    /// proto 在场但缺 contentsV1（官方 LINK_VERSION_ERROR 族——链接版本
    /// 不可识别，未来版本 proto）。
    UnsupportedVersion,
}

/// 链接解析（官方 parseGroupLink + 桌面路由层 hash 摘取行为对齐）：入参
/// 接受三种官方 URL 形（https://signal.group/#…、sgnl://signal.group/#…、
/// sgnl://joingroup#…）或裸 inviteCode（官方 parseGroupLink 直收 inviteCode
/// 同位）；摘 `#` 后段 → websafe 还原 → 标准 base64 解码 → proto 解码 →
/// contentsV1 必在（缺 = UnsupportedVersion）→ masterKey 恰 32B、password
/// 非空（官方逐条 throw 容错同口径）。
pub(crate) fn parse_invite_link(value: &str) -> Result<ParsedInviteLink, LinkParseError> {
    let trimmed = value.trim();
    let code = match trimmed.split_once('#') {
        Some((_, after)) if !after.trim().is_empty() => after.trim(),
        _ if !trimmed.contains('#') && !trimmed.is_empty() => trimmed,
        _ => return Err(LinkParseError::Empty),
    };
    let standard = from_web_safe_base64(code);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(standard.as_bytes())
        .map_err(|_| LinkParseError::Malformed)?;
    let link = presage::proto::GroupInviteLink::decode(bytes.as_ref())
        .map_err(|_| LinkParseError::Malformed)?;
    let Some(presage::proto::group_invite_link::Contents::ContentsV1(contents)) = link.contents
    else {
        return Err(LinkParseError::UnsupportedVersion);
    };
    if contents.group_master_key.len() != MASTER_KEY_BYTES {
        return Err(LinkParseError::Malformed);
    }
    if contents.invite_link_password.is_empty() {
        return Err(LinkParseError::Malformed);
    }
    Ok(ParsedInviteLink {
        master_key: contents.group_master_key,
        password: contents.invite_link_password,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 密码 = 16 字节 CSPRNG（官方 GROUP_INVITE_LINK_PASSWORD_LENGTH 同长）；
    /// 两次生成不重合（CSPRNG 面，256^16 碰撞概率工程零）。
    #[test]
    fn password_is_sixteen_csprng_bytes() {
        let a = generate_invite_link_password();
        let b = generate_invite_link_password();
        assert_eq!(a.len(), 16);
        assert_ne!(a, b);
    }

    /// 构造→解析 roundtrip：链接形 = 官方 `https://signal.group/#<websafe>`
    ///；code 段 websafe 化（无 `+/=`）；proto 往返还原 masterKey+password。
    #[test]
    fn build_then_parse_roundtrip_official_url_shape() {
        let master_key = [0x61u8; 32];
        let password = [0u8, 1, 2, 250, 251, 63, 62, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        let link = build_invite_link(&master_key, &password);
        assert!(link.starts_with("https://signal.group/#"));
        let code = link.rsplit('#').next().expect("code segment");
        assert!(!code.contains('+') && !code.contains('/') && !code.contains('='));

        let parsed = parse_invite_link(&link).expect("roundtrip parses");
        assert_eq!(parsed.master_key, master_key.to_vec());
        assert_eq!(parsed.password, password.to_vec());
    }

    /// 三种官方 URL 形 + 裸 inviteCode 皆可解（桌面路由层三 pattern +
    /// parseGroupLink 直收 code 同口径）。
    #[test]
    fn parse_accepts_official_url_shapes_and_bare_code() {
        let master_key = [0x62u8; 32];
        let password = [7u8; 16];
        let link = build_invite_link(&master_key, &password);
        let code = link.rsplit('#').next().expect("code segment").to_owned();
        for value in [
            link.clone(),
            format!("sgnl://signal.group/#{code}"),
            format!("sgnl://joingroup#{code}"),
            code.clone(),
            format!("  {code}  "),
        ] {
            let parsed = parse_invite_link(&value).expect("official shape parses");
            assert_eq!(parsed.master_key, master_key.to_vec());
            assert_eq!(parsed.password, password.to_vec());
        }
    }

    /// 容错分类（官方 parseGroupLink 逐条 throw 面）：空/缺 code = Empty；
    /// 坏 base64/坏 proto/masterKey 非 32B/password 空 = Malformed；缺
    /// contentsV1 = UnsupportedVersion（官方 LINK_VERSION_ERROR 族）。
    #[test]
    fn parse_error_faces_match_official_tolerance() {
        assert_eq!(parse_invite_link(""), Err(LinkParseError::Empty));
        assert_eq!(
            parse_invite_link("https://signal.group/#"),
            Err(LinkParseError::Empty)
        );
        assert_eq!(
            parse_invite_link("https://signal.group/#@@@"),
            Err(LinkParseError::Malformed)
        );
        // 合法 base64 但非 proto。
        let not_proto = base64::engine::general_purpose::STANDARD.encode(b"not a proto");
        assert_eq!(
            parse_invite_link(&to_web_safe_base64(&not_proto)),
            Err(LinkParseError::Malformed)
        );
        // proto 在场、缺 contentsV1（线面只携未知字段号——prost 容忍解码，
        // contents 缺席 = 官方 LINK_VERSION_ERROR 族；全缺省 proto 线面为
        // 零字节，落 Empty 臂，故用未知字段构造非空编码）。
        let versionless_code =
            to_web_safe_base64(&base64::engine::general_purpose::STANDARD.encode([0x10u8, 0x01]));
        assert_eq!(
            parse_invite_link(&versionless_code),
            Err(LinkParseError::UnsupportedVersion)
        );
        // masterKey 非 32B / password 空。
        let short = presage::proto::GroupInviteLink {
            contents: Some(presage::proto::group_invite_link::Contents::ContentsV1(
                presage::proto::group_invite_link::GroupInviteLinkContentsV1 {
                    group_master_key: vec![1u8; 16],
                    invite_link_password: vec![2u8; 16],
                },
            )),
        }
        .encode_to_vec();
        assert_eq!(
            parse_invite_link(&to_web_safe_base64(
                &base64::engine::general_purpose::STANDARD.encode(short)
            )),
            Err(LinkParseError::Malformed)
        );
        let empty_password = presage::proto::GroupInviteLink {
            contents: Some(presage::proto::group_invite_link::Contents::ContentsV1(
                presage::proto::group_invite_link::GroupInviteLinkContentsV1 {
                    group_master_key: vec![1u8; 32],
                    invite_link_password: Vec::new(),
                },
            )),
        }
        .encode_to_vec();
        assert_eq!(
            parse_invite_link(&to_web_safe_base64(
                &base64::engine::general_purpose::STANDARD.encode(empty_password)
            )),
            Err(LinkParseError::Malformed)
        );
    }

    /// websafe 互逆规则（官方 webSafeBase64.std.ts 同规则钉定）。
    #[test]
    fn web_safe_base64_roundtrip_rules() {
        assert_eq!(to_web_safe_base64("a+b/cd=="), "a-b_cd");
        assert_eq!(from_web_safe_base64("a-b_cd"), "a+b/cd==");
        assert_eq!(from_web_safe_base64("abcd"), "abcd");
        assert_eq!(from_web_safe_base64("abc"), "abc=");
        assert_eq!(from_web_safe_base64("ab"), "ab==");
    }
}
