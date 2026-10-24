// SPDX-License-Identifier: AGPL-3.0-only

//! 通话中继配置（calls.relays，KT 扩展方法）：认证 GET /v2/calling/relays
//! 拉取 ICE/TURN 配置。官方 getIceServers 同端点（Signal-Desktop 只读参照
//! WebAPI.preload.ts:796 路径 + :3064 调用——chatService identified 账号
//! Basic Auth，无 unauthenticated/accessKey 位），本引擎握 presage 链接
//! 账号凭据，同一 API 同类用法；代理随进程 ALL_PROXY（main.rs 启动面覆写）
//! 由 reqwest 自动生效。
//!
//! 应答形状严格按官方 GetIceServersResultType / IceServerGroupType（同文件
//! :1000-1013）：`{relays?: [{username, password, urls?, urlsWithIps?,
//! hostname?, ttl?}]}`。serde 对未知字段缺省丢弃（不发明）；官方 zodSchema
//! 为 z.unknown() 未校验（TODO DESKTOP-8719），引擎侧收紧为类型面校验，
//! 形状纪律由模块单测钉定。
//!
//! 缓存 + TTL 官方同口径（calling.preload.ts #getIceServers）：
//! - TTL = relays[].ttl 最小秒值（任一条目 ttl 缺失/0 → 0 = 不缓存，下次
//!   重拉）；TTL 内直接用缓存不打网络（官方起呼按需拉取同节奏，无定时
//!   轮询）；
//! - 拉取成功但 relays 缺席/空数组 = 不覆盖既有缓存（官方转换后列表非空
//!   才写缓存同口径）；
//! - 拉取失败 = 诚实保留旧缓存（官方姿态是错误上抛、起呼失败；本引擎是
//!   后台缓存服务面，保旧 + 新鲜度/lastError 如实暴露，取舍登记 compat
//!   §5），无缓存则空态——错误永不连坐其他链路；
//! - 401/403 = 账号凭据失效，诚实错误应答（engine.deviceCapabilities 解绑
//!   面同款），缓存不动。
//!
//! 缓存 per-account 进程内存（group_credentials 同款纪律），不落盘不跨
//! 重启。并发过期重拉无防抖（幂等 GET，last-write-wins）。

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::push_service::HttpAuthOverride;
use serde::{Deserialize, Serialize};

/// 官方应答文档形状（GetIceServersResultType）。
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct RelaysDocument {
    #[serde(default)]
    pub(crate) relays: Option<Vec<IceServerGroup>>,
}

/// 官方 IceServerGroupType 逐字段；序列化面 None 字段整键缺席（不出
/// null，finishLink aci 同款仓纪律）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IceServerGroup {
    pub(crate) username: String,
    pub(crate) password: String,
    #[serde(
        rename = "urlsWithIps",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) urls_with_ips: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) urls: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ttl: Option<u64>,
}

/// 解析器（字节面 → 文档）：非 JSON / 形状违约（username/password 缺失或
/// 类型错）= Err，走拉取失败族（保旧缓存路径），不发明缺省。
pub(crate) fn parse_relays_document(bytes: &[u8]) -> Result<RelaysDocument, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// TTL 毫秒 = relays[].ttl 最小秒值 ×1000；relays 空 / 任一条目 ttl 缺失
/// 或 0 → 0（不缓存，官方 `reduce min(ttl ?? 0)` 同口径）。
pub(crate) fn ttl_ms(relays: &[IceServerGroup]) -> u64 {
    relays
        .iter()
        .map(|group| group.ttl.unwrap_or(0))
        .min()
        .unwrap_or(0)
        .saturating_mul(1000)
}

/// per-account 缓存条目（进程内存）。last_error = 最近一次重拉失败原因
///（成功刷新即清空；空态失败的错误不入缓存——无条目可挂，face 直接携带）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CacheEntry {
    pub(crate) relays: Vec<IceServerGroup>,
    pub(crate) fetched_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) last_error: Option<String>,
}

/// 缓存新鲜判定（官方 `currentTime < expirationTimestamp` 同式）：
/// TTL 内不重拉。
pub(crate) fn cache_fresh(entry: &CacheEntry, now_ms: u64) -> bool {
    now_ms < entry.expires_at_ms
}

/// 重拉结果（网络腿与决策腿解耦面——单测全矩阵不触网）。
pub(crate) enum RefreshOutcome {
    /// 成功应答（relays 可能缺席/空数组）。
    Fetched(RelaysDocument),
    /// 拉取失败（网络/形状/非 401 服务器错误），携带诚实原因。
    Failed(String),
}

/// 缓存推进纯函数（官方 #getIceServers 三纪律）：
/// - Fetched 且 relays 非空 → 覆盖（expires = now + ttl_ms；ttl 0 = 立即
///   过期条目照写——官方「列表非空即写缓存」同口径，stale 由 face 暴露）；
/// - Fetched 空/缺席 → 缓存原样（不覆盖）；
/// - Failed → 缓存原样 + last_error 记因（无条目维持 None，face 携带）。
pub(crate) fn advance_cache(
    cache: Option<CacheEntry>,
    outcome: &RefreshOutcome,
    now_ms: u64,
) -> Option<CacheEntry> {
    match outcome {
        RefreshOutcome::Fetched(doc) => match doc.relays.as_deref() {
            Some(groups) if !groups.is_empty() => {
                let expires_at_ms = now_ms.saturating_add(ttl_ms(groups));
                Some(CacheEntry {
                    relays: groups.to_vec(),
                    fetched_at_ms: now_ms,
                    expires_at_ms,
                    last_error: None,
                })
            }
            _ => cache,
        },
        RefreshOutcome::Failed(reason) => cache.map(|mut entry| {
            entry.last_error = Some(reason.clone());
            entry
        }),
    }
}

/// calls.relays 应答面（service 层出语义，dispatch 组 wire）。
pub(crate) struct RelaysFace {
    pub(crate) relays: Vec<IceServerGroup>,
    pub(crate) fetched_at_ms: Option<u64>,
    pub(crate) expires_at_ms: Option<u64>,
    /// 无缓存恒 true；有条目 = now ≥ expires（ttl 0 条目当拍即 stale）。
    pub(crate) stale: bool,
    /// 最近一次重拉失败原因（无缓存空态失败时为本次原因，如实新鲜度）。
    pub(crate) last_error: Option<String>,
}

/// 缓存条目 → 应答面。无缓存 = 空态（空 relays + stale），`empty_error`
/// 只在空态臂生效（fresh 缓存命中时本次未拉取，不存在本次错误）。
pub(crate) fn face_from_cache(
    cache: Option<&CacheEntry>,
    now_ms: u64,
    empty_error: Option<String>,
) -> RelaysFace {
    match cache {
        Some(entry) => RelaysFace {
            relays: entry.relays.clone(),
            fetched_at_ms: Some(entry.fetched_at_ms),
            expires_at_ms: Some(entry.expires_at_ms),
            stale: !cache_fresh(entry, now_ms),
            last_error: entry.last_error.clone(),
        },
        None => RelaysFace {
            relays: Vec::new(),
            fetched_at_ms: None,
            expires_at_ms: None,
            stale: true,
            last_error: empty_error,
        },
    }
}

/// 拉取错误族（401/403 独立归型 = 解绑面，其余失败进 Failed 保旧缓存）。
#[derive(Debug)]
pub(crate) enum RelaysError {
    /// 账号凭据失效（401/403）。
    Unauthorized,
    /// 其余非 2xx / 网络 / 请求构造失败。
    Fetch(String),
    /// 应答形状违约（serde 类型面）。
    Malformed(String),
}

impl std::fmt::Display for RelaysError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelaysError::Unauthorized => write!(f, "credentials rejected (401/403)"),
            RelaysError::Fetch(what) => write!(f, "{what}"),
            RelaysError::Malformed(what) => write!(f, "malformed relays payload: {what}"),
        }
    }
}

/// 认证 GET /v2/calling/relays（identified 账号凭据，官方同端点同谓词）。
pub(crate) async fn fetch_relays(push: &PushService) -> Result<RelaysDocument, RelaysError> {
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service("v2/calling/relays"),
            // 本机账号凭据 basic auth（官方 chatService identified 通道）。
            HttpAuthOverride::NoOverride,
        )
        .map_err(|e| RelaysError::Fetch(e.to_string()))?
        .send()
        .await
        .map_err(|e| RelaysError::Fetch(e.to_string()))?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(match status {
            401 | 403 => RelaysError::Unauthorized,
            _ => RelaysError::Fetch(format!("http {status}")),
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|e| RelaysError::Fetch(e.to_string()))?;
    parse_relays_document(&bytes).map_err(|e| RelaysError::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 解析器钉官方应答形状：全字段命中（官方 IceServerGroupType 六字段
    /// 逐字，urlsWithIps camelCase）。
    #[test]
    fn parses_official_shape_full_fields() {
        let doc = parse_relays_document(
            br#"{"relays":[{"username":"u1","password":"p1","urls":["turn:turn.example.org:80?transport=tcp"],"urlsWithIps":["1.2.3.4:3478"],"hostname":"turn.example.org","ttl":86400}]}"#,
        )
        .expect("official shape parses");
        let relays = doc.relays.expect("relays present");
        assert_eq!(relays.len(), 1);
        assert_eq!(relays[0].username, "u1");
        assert_eq!(relays[0].password, "p1");
        assert_eq!(
            relays[0].urls,
            Some(vec!["turn:turn.example.org:80?transport=tcp".to_owned()])
        );
        assert_eq!(
            relays[0].urls_with_ips,
            Some(vec!["1.2.3.4:3478".to_owned()])
        );
        assert_eq!(relays[0].hostname.as_deref(), Some("turn.example.org"));
        assert_eq!(relays[0].ttl, Some(86400));
    }

    /// relays 键缺席 = None（官方 relays? 可选语义，不是空数组）。
    #[test]
    fn parses_relays_absent_as_none() {
        let doc = parse_relays_document(b"{}").expect("empty object parses");
        assert_eq!(doc.relays, None);
    }

    /// relays 空数组 = Some(空)（缺席与空数组是两个态，官方消费面统一
    /// 空列表，引擎保留区分供 advance 语义）。
    #[test]
    fn parses_empty_relays_array() {
        let doc = parse_relays_document(br#"{"relays":[]}"#).expect("empty relays parses");
        assert_eq!(doc.relays, Some(Vec::new()));
    }

    /// 未知字段丢弃不发明：顶层与条目内多余键不进结构、不炸解析。
    #[test]
    fn drops_unknown_fields() {
        let doc = parse_relays_document(
            br#"{"relays":[{"username":"u","password":"p","sneaky":123,"extra":"x"}],"unknownTop":true}"#,
        )
        .expect("unknown fields dropped");
        let relays = doc.relays.expect("relays present");
        assert_eq!(
            relays[0],
            IceServerGroup {
                username: "u".to_owned(),
                password: "p".to_owned(),
                urls_with_ips: None,
                urls: None,
                hostname: None,
                ttl: None,
            }
        );
    }

    /// 序列化面 None 字段整键缺席（不出 null，仓纪律）。
    #[test]
    fn serializes_present_keys_only() {
        let group = IceServerGroup {
            username: "u".to_owned(),
            password: "p".to_owned(),
            urls_with_ips: None,
            urls: Some(vec!["turn:x:3478".to_owned()]),
            hostname: None,
            ttl: None,
        };
        let value = serde_json::to_value(&group).expect("serializes");
        let obj = value.as_object().expect("object");
        assert_eq!(obj.len(), 3);
        assert!(
            obj.contains_key("username")
                && obj.contains_key("password")
                && obj.contains_key("urls")
        );
        assert!(!obj.contains_key("urlsWithIps"));
        assert!(!obj.contains_key("hostname"));
        assert!(!obj.contains_key("ttl"));
    }

    /// 形状违约 fail-closed：username/password 缺失或类型错 = Err（不发明
    /// 缺省），非 JSON = Err。
    #[test]
    fn rejects_malformed_shapes() {
        assert!(parse_relays_document(br#"{"relays":[{"password":"p"}]}"#).is_err());
        assert!(parse_relays_document(br#"{"relays":[{"username":"u"}]}"#).is_err());
        assert!(parse_relays_document(br#"{"relays":[{"username":1,"password":"p"}]}"#).is_err());
        assert!(parse_relays_document(
            br#"{"relays":[{"username":"u","password":"p","ttl":"soon"}]}"#
        )
        .is_err());
        assert!(parse_relays_document(br#"{"relays":"soon"}"#).is_err());
        assert!(parse_relays_document(b"not json").is_err());
    }

    /// TTL 取最小秒值（官方 reduce min 同口径）。
    #[test]
    fn ttl_ms_takes_minimum() {
        fn group(ttl: Option<u64>) -> IceServerGroup {
            IceServerGroup {
                username: "u".to_owned(),
                password: "p".to_owned(),
                urls_with_ips: None,
                urls: None,
                hostname: None,
                ttl,
            }
        }
        assert_eq!(ttl_ms(&[group(Some(300)), group(Some(86400))]), 300_000);
        // 任一条目 ttl 缺失或 0 → 0（不缓存，下次重拉）。
        assert_eq!(ttl_ms(&[group(Some(300)), group(None)]), 0);
        assert_eq!(ttl_ms(&[group(Some(0))]), 0);
        assert_eq!(ttl_ms(&[]), 0);
    }

    /// 新鲜判定边界（官方 `<` 同式）：now = expires 已过期 → 重拉。
    #[test]
    fn cache_fresh_boundary() {
        let entry = CacheEntry {
            relays: Vec::new(),
            fetched_at_ms: 1_000,
            expires_at_ms: 4_000,
            last_error: None,
        };
        assert!(cache_fresh(&entry, 3_999));
        assert!(!cache_fresh(&entry, 4_000));
    }

    fn relay_with_ttl(ttl: Option<u64>) -> IceServerGroup {
        IceServerGroup {
            username: "u".to_owned(),
            password: "p".to_owned(),
            urls_with_ips: None,
            urls: None,
            hostname: None,
            ttl,
        }
    }

    fn doc_of(relays: Option<Vec<IceServerGroup>>) -> RelaysDocument {
        RelaysDocument { relays }
    }

    /// 推进矩阵：Fetched 非空 → 覆盖（fetched/expires/ttl 语义钉定）。
    #[test]
    fn advance_writes_non_empty_with_ttl() {
        let entry = advance_cache(
            None,
            &RefreshOutcome::Fetched(doc_of(Some(vec![relay_with_ttl(Some(300))]))),
            1_000,
        )
        .expect("cache written");
        assert_eq!(entry.fetched_at_ms, 1_000);
        assert_eq!(entry.expires_at_ms, 301_000);
        assert_eq!(entry.last_error, None);
    }

    /// ttl 0 条目照写（官方「列表非空即写缓存」同口径），expires = now
    /// 即当拍 stale，face 如实暴露。
    #[test]
    fn advance_ttl_zero_entry_is_immediately_stale() {
        let entry = advance_cache(
            None,
            &RefreshOutcome::Fetched(doc_of(Some(vec![relay_with_ttl(Some(0))]))),
            1_000,
        )
        .expect("cache written");
        assert_eq!(entry.expires_at_ms, 1_000);
        assert!(!cache_fresh(&entry, 1_000));
    }

    /// Fetched 空/缺席 → 缓存原样（官方空应答不覆盖同口径）。
    #[test]
    fn advance_empty_answer_keeps_old_cache() {
        let old = CacheEntry {
            relays: vec![relay_with_ttl(Some(300))],
            fetched_at_ms: 1_000,
            expires_at_ms: 301_000,
            last_error: Some("earlier".to_owned()),
        };
        let snapshot = old.clone();
        let kept = advance_cache(Some(old), &RefreshOutcome::Fetched(doc_of(None)), 2_000)
            .expect("old cache kept");
        assert_eq!(kept, snapshot);
        let kept = advance_cache(
            Some(snapshot.clone()),
            &RefreshOutcome::Fetched(doc_of(Some(Vec::new()))),
            2_000,
        )
        .expect("old cache kept");
        assert_eq!(kept, snapshot);
    }

    /// Failed → 旧缓存原样保留（relays/时间戳不动）+ last_error 记因；
    /// 成功刷新清 last_error。
    #[test]
    fn advance_failure_keeps_old_cache_and_success_clears_error() {
        let old = CacheEntry {
            relays: vec![relay_with_ttl(Some(300))],
            fetched_at_ms: 1_000,
            expires_at_ms: 301_000,
            last_error: None,
        };
        let failed = advance_cache(
            Some(old.clone()),
            &RefreshOutcome::Failed("http 500".to_owned()),
            2_000,
        )
        .expect("old cache kept");
        assert_eq!(failed.relays, old.relays);
        assert_eq!(failed.fetched_at_ms, 1_000);
        assert_eq!(failed.expires_at_ms, 301_000);
        assert_eq!(failed.last_error.as_deref(), Some("http 500"));

        let refreshed = advance_cache(
            Some(failed),
            &RefreshOutcome::Fetched(doc_of(Some(vec![relay_with_ttl(Some(60))]))),
            3_000,
        )
        .expect("cache refreshed");
        assert_eq!(refreshed.last_error, None);
        assert_eq!(refreshed.fetched_at_ms, 3_000);
    }

    /// Failed 无缓存维持 None（空态，错误由 face 携带不入缓存）。
    #[test]
    fn advance_failure_without_cache_stays_empty() {
        let advanced = advance_cache(
            None,
            &RefreshOutcome::Failed("connect refused".to_owned()),
            1_000,
        );
        assert_eq!(advanced, None);
    }

    /// 应答面投影：fresh/stale/空态三态与 last_error 穿透。
    #[test]
    fn face_from_cache_three_states() {
        let fresh = CacheEntry {
            relays: vec![relay_with_ttl(Some(300))],
            fetched_at_ms: 1_000,
            expires_at_ms: 301_000,
            last_error: None,
        };
        let face = face_from_cache(Some(&fresh), 2_000, None);
        assert!(!face.stale);
        assert_eq!(face.fetched_at_ms, Some(1_000));
        assert_eq!(face.expires_at_ms, Some(301_000));
        assert_eq!(face.last_error, None);

        let face = face_from_cache(Some(&fresh), 301_000, None);
        assert!(face.stale);
        assert_eq!(face.relays.len(), 1);

        let face = face_from_cache(None, 2_000, Some("connect refused".to_owned()));
        assert!(face.relays.is_empty());
        assert!(face.stale);
        assert_eq!(face.fetched_at_ms, None);
        assert_eq!(face.expires_at_ms, None);
        assert_eq!(face.last_error.as_deref(), Some("connect refused"));
    }
}
