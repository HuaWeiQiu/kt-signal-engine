// SPDX-License-Identifier: AGPL-3.0-only

//! 本机账号 profile capabilities 读面（回填能力门数据面，2026-10-20 批，
//! wire additive【engine-compat】；compat §3 `listAccounts` 行 + §5 块）。
//!
//! 官方锚：回填门 `isBackfillEnabled()`（ts/jobs/helpers/
//! attachmentBackfill.preload.ts:57-72）读
//! `getOurConversation()?.get('capabilities')?.attachmentBackfill === true`；
//! our conversation 的 capabilities 来自自己 profile（ts/services/
//! profiles.preload.ts:713-718：`profile.capabilities != null` 时
//! `c.set({ capabilities: profile.capabilities })`）。引擎此前只写自己
//! profile、不回读，connector/桌面没有据以判门的供给腿。
//!
//! 读取路径（为什么不复用 `Manager::retrieve_profile`）：presage
//! `retrieve_profile_by_uuid`（vendored registered.rs:443-478）把服务器
//! 响应经 `ProfileCipher::decrypt` 收成 `Profile`——五字段结构
//! （libsignal-service account_manager.rs:89-95）**不含 capabilities**，
//! 位在解密面被丢弃。故本模块走同一条服务器读面的 HTTP 腿：认证
//! `GET v1/profile/{aci}/{profileKeyVersion}`（vendored websocket/
//! profile.rs:95-110 同 URL 构造；`ProfileKeyVersion` 的 `AsRef<str>` 即
//! 其 hex ASCII 形，本仓直取不二次 bincode），响应 JSON 的 `capabilities`
//! 对象 = 官方 `ProfileType.capabilities`（Signal-Desktop ts/textsecure/
//! WebAPI.preload.ts:989），字段形状复用 vendor `DeviceCapabilities`
//! （websocket/profile.rs:68，`attachment_backfill` 位同构）。
//!
//! 纪律：
//! - **隐私红线：只投 `attachmentBackfill` 一位**。不整包透出 vendor
//!   `DeviceCapabilities`（其余位与未来新增字段不上 wire，消费方无从
//!   依赖引擎未承诺的位）。
//! - **诚实缺席**：未注册/网络/超时/响应无 capabilities 对象 = 整键缺席，
//!   不投 false。官方 `capabilities === undefined` 与 `attachmentBackfill:
//!   false` 在回填门同义（都关闭），但缺席让上层能区分「未知」与「明确
//!   不支持」；capabilities 对象在场而位缺省时按 vendor serde 缺省 =
//!   false（明确不支持）。
//! - **有界缓存**：per-account TTL [`CAPABILITIES_CACHE_TTL`]，失败不缓存
//!   （下一读面重试）；缓存只存一位布尔，不存 profile 载荷。
//! - **热路径预算**：`listAccounts` 整批 [`CAPABILITIES_LIST_BUDGET`] 预算
//!   （逐账号余量递减），超时即缺席——拉取是网络面，不得把列表应答拖成
//!   慢调用；任何失败不连坐 `listAccounts` 主结局。
//! - **fake 面**：无 store/无网络，形状桩投影引擎自声明位
//!   [`super::capabilities::DEVICE_CAPABILITIES`]（当前 true）；官方
//!   `isBackfillEnabled` 对 test/mock 环境恒 true（同文件 :61-63），
//!   形状桩同向。

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::libsignal_service::websocket::account::DeviceCapabilities;
use presage::store::StateStore as _;
use serde_json::{json, Value};

use super::{open_store, push_service_from_registration};

/// 缓存 TTL（官方桌面按 profile 刷新节奏拉取，无独立轮询；引擎读面是
/// 列表投影的旁路，10 分钟窗口足够覆盖 connector 的账号列表刷新节奏，
/// 又不让位长期陈旧——回填门本身只在长文本/大批附件失败后才消费）。
pub(crate) const CAPABILITIES_CACHE_TTL: Duration = Duration::from_secs(600);

/// `listAccounts` 整批拉取预算：缓存未命中的账号共享这 2s（逐账号余量
/// 递减），到期未拉到的条目诚实缺席。与 `profiles.refreshContacts` 的
/// 单条 10s 不同——列表读面是热路径，宁可缺席也不拖慢列表。
pub(crate) const CAPABILITIES_LIST_BUDGET: Duration = Duration::from_millis(2_000);

/// per-account 进程内缓存（不落盘不跨重启；`delete_account`/重链摘除）。
#[derive(Default)]
pub(crate) struct CapabilitiesCache {
    entries: StdMutex<HashMap<String, CacheEntry>>,
}

#[derive(Clone, Copy)]
struct CacheEntry {
    attachment_backfill: bool,
    expires_at: Instant,
}

impl CapabilitiesCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// TTL 内命中返回位值；过期/缺席 None（锁内零 await）。
    pub(crate) fn get(&self, account: &str, now: Instant) -> Option<bool> {
        let guard = self.entries.lock().expect("capabilities cache lock");
        let entry = guard.get(account)?;
        (now < entry.expires_at).then_some(entry.attachment_backfill)
    }

    /// 成功读面回填单点（失败路径不调用——失败不缓存）。
    pub(crate) fn put(&self, account: &str, attachment_backfill: bool, now: Instant) {
        self.entries
            .lock()
            .expect("capabilities cache lock")
            .insert(
                account.to_owned(),
                CacheEntry {
                    attachment_backfill,
                    expires_at: now + CAPABILITIES_CACHE_TTL,
                },
            );
    }

    /// 账号面摘除（delete/重链；下一读面重拉）。
    pub(crate) fn invalidate(&self, account: &str) {
        self.entries
            .lock()
            .expect("capabilities cache lock")
            .remove(account);
    }
}

/// `listAccounts` 条目 `capabilities` 值形状：只含官方回填门消费的一位
/// （隐私红线，见模块注）。
pub(crate) fn wire_capabilities(attachment_backfill: bool) -> Value {
    json!({ "attachmentBackfill": attachment_backfill })
}

/// fake 形状桩的位：引擎自声明面（`DEVICE_CAPABILITIES.attachment_backfill`，
/// 与 `engine.protocolCapabilities` 同源，不另造值）。
pub(crate) fn declared_attachment_backfill() -> bool {
    super::capabilities::DEVICE_CAPABILITIES.attachment_backfill
}

/// 缓存 + 有界拉取单点：命中零网络；miss 在 `budget` 内拉取，成功才写
/// 缓存（失败/超时 = None，不缓存，下次读面重试）。
pub(crate) async fn attachment_backfill_cached(
    cache: &CapabilitiesCache,
    account: &str,
    store_dir: &Path,
    budget: Duration,
) -> Option<bool> {
    cached_within(cache, account, budget, || {
        fetch_own_attachment_backfill(store_dir)
    })
    .await
}

/// 缓存/超时骨架（拉取函数注入：单测用探针计数钉定「命中不重复走网络」
/// 与「失败不缓存」；真实面由 [`attachment_backfill_cached`] 注入 HTTP 腿）。
async fn cached_within<F, Fut>(
    cache: &CapabilitiesCache,
    account: &str,
    budget: Duration,
    fetch: F,
) -> Option<bool>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Option<bool>>,
{
    if let Some(hit) = cache.get(account, Instant::now()) {
        return Some(hit);
    }
    // 预算耗尽（整批前面账号已用完）：不再发起拉取，诚实缺席。
    if budget.is_zero() {
        return None;
    }
    let fetched = tokio::time::timeout(budget, fetch()).await.ok().flatten();
    if let Some(value) = fetched {
        cache.put(account, value, Instant::now());
    }
    fetched
}

/// 服务器 profile 响应的最小投影：只取 capabilities 对象，其余字段
/// （加密 name/about/avatar 等）serde 按未知字段丢弃，不解析不落库。
#[derive(serde::Deserialize)]
struct ProfileResponse {
    #[serde(default)]
    capabilities: Option<DeviceCapabilities>,
}

/// 响应体 → 回填位（纯函数，单测钉形）：capabilities 对象整体缺席 =
/// 未知 = None；对象在场而位缺省 = vendor serde 缺省 false。
fn attachment_backfill_from_profile_body(body: &[u8]) -> Option<bool> {
    let response: ProfileResponse = serde_json::from_slice(body).ok()?;
    Some(response.capabilities?.attachment_backfill)
}

/// 真实读面：本地注册数据 → 认证 GET 自己的 profile（vendor 同端点同
/// URL 构造）→ 只取 `capabilities.attachmentBackfill`。任一步失败 = None
/// （wire 键缺席），调用方不区分失败原因（列表读面不需要错误面）。
async fn fetch_own_attachment_backfill(store_dir: &Path) -> Option<bool> {
    let store = open_store(store_dir).await.ok()?;
    let data = store.load_registration_data().await.ok().flatten()?;
    let aci = data.service_ids.aci();
    let profile_key_version = data.profile_key().get_profile_key_version(aci);
    let path = format!(
        "v1/profile/{}/{}",
        aci.service_id_string(),
        profile_key_version.as_ref()
    );
    let push = push_service_from_registration(&data)?;
    let response = push
        .request(
            reqwest::Method::GET,
            Endpoint::service(path),
            HttpAuthOverride::NoOverride,
        )
        .ok()?
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.bytes().await.ok()?;
    attachment_backfill_from_profile_body(&body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// 缓存命中不重复走网络（探针计数）：同一账号两次读面只拉一次。
    #[tokio::test]
    async fn cache_hit_skips_repeated_fetch_probe() {
        let cache = CapabilitiesCache::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let first = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "+15550001111",
                CAPABILITIES_LIST_BUDGET,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(true)
                },
            )
            .await
        };
        assert_eq!(first, Some(true));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "+15550001111",
                CAPABILITIES_LIST_BUDGET,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(true)
                },
            )
            .await
        };
        assert_eq!(second, Some(true));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "缓存命中不得再打网络");
    }

    /// 失败不缓存：失败读面 → None 且不入缓存；下一次读面重试成功。
    #[tokio::test]
    async fn failed_fetch_is_not_cached_and_retried() {
        let cache = CapabilitiesCache::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let failed = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "acct",
                CAPABILITIES_LIST_BUDGET,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    None
                },
            )
            .await
        };
        assert_eq!(failed, None);
        assert!(
            cache.get("acct", Instant::now()).is_none(),
            "失败不得写缓存"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let retried = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "acct",
                CAPABILITIES_LIST_BUDGET,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(false)
                },
            )
            .await
        };
        assert_eq!(retried, Some(false), "明确 false 是有效读面结果");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            cache.get("acct", Instant::now()),
            Some(false),
            "成功读面（含 false）才入缓存"
        );
    }

    /// 读取超时 → 缺席（None）且不缓存；超时预算外不悬挂。
    #[tokio::test]
    async fn fetch_timeout_yields_absent_and_is_not_cached() {
        let cache = CapabilitiesCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();

        let timed_out = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "acct",
                Duration::from_millis(20),
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Some(true)
                },
            )
            .await
        };
        assert_eq!(timed_out, None, "超时 = 缺席，不投 false");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "超时必须按预算返回，实际 {:?}",
            started.elapsed()
        );
        assert!(
            cache.get("acct", Instant::now()).is_none(),
            "超时不得写缓存"
        );

        // 第二次按预算内成功拉取：证明超时未污染缓存。
        let recovered = {
            let calls = Arc::clone(&calls);
            cached_within(
                &cache,
                "acct",
                CAPABILITIES_LIST_BUDGET,
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Some(true)
                },
            )
            .await
        };
        assert_eq!(recovered, Some(true));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// 预算耗尽（整批前面账号吃完 2s）：不再发起拉取，直接缺席。
    #[tokio::test]
    async fn exhausted_budget_skips_fetch() {
        let cache = CapabilitiesCache::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let result = {
            let calls = Arc::clone(&calls);
            cached_within(&cache, "acct", Duration::ZERO, move || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Some(true)
            })
            .await
        };
        assert_eq!(result, None);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "零预算不得发起拉取");
    }

    /// TTL 边界与 per-account 隔离：TTL 内命中、到点过期、账号互不串；
    /// 明确 false 照实缓存（false 是有效读面结果，不是缺失）。
    #[test]
    fn cache_ttl_boundary_and_per_account_isolation() {
        let cache = CapabilitiesCache::new();
        let now = Instant::now();
        cache.put("a", true, now);
        assert_eq!(cache.get("a", now), Some(true));
        assert_eq!(
            cache.get("a", now + CAPABILITIES_CACHE_TTL - Duration::from_millis(1)),
            Some(true)
        );
        assert_eq!(
            cache.get("a", now + CAPABILITIES_CACHE_TTL),
            None,
            "TTL 到点即过期"
        );
        assert_eq!(cache.get("b", now), None, "per-account 隔离");
        cache.put("b", false, now);
        assert_eq!(cache.get("b", now), Some(false));
        cache.invalidate("a");
        assert_eq!(cache.get("a", now), None, "摘除即失效");
    }

    /// profile 响应解析臂：capabilities 对象在场按 vendor serde 取位；
    /// 对象整体缺席/坏 JSON = 未知（None，整键缺席，不发明 false）。
    #[test]
    fn profile_body_capability_arms() {
        assert_eq!(
            attachment_backfill_from_profile_body(
                br#"{"capabilities":{"attachmentBackfill":true,"spqr":true}}"#
            ),
            Some(true)
        );
        assert_eq!(
            attachment_backfill_from_profile_body(
                br#"{"capabilities":{"attachmentBackfill":false}}"#
            ),
            Some(false)
        );
        // 对象在场、位缺省：vendor serde 缺省 false（明确不支持）。
        assert_eq!(
            attachment_backfill_from_profile_body(br#"{"capabilities":{"spqr":true}}"#),
            Some(false)
        );
        // 对象整体缺席 = 未知 = 键缺席。
        assert_eq!(
            attachment_backfill_from_profile_body(br#"{"name":"x","about":"y"}"#),
            None
        );
        assert_eq!(attachment_backfill_from_profile_body(b"not json"), None);
        assert_eq!(attachment_backfill_from_profile_body(b""), None);
    }

    /// wire 形状钉死：capabilities 对象只含一个布尔键（隐私红线）。
    #[test]
    fn wire_payload_projects_single_bit_only() {
        let payload = wire_capabilities(true);
        assert_eq!(payload, json!({ "attachmentBackfill": true }));
        assert_eq!(
            payload.as_object().expect("object").len(),
            1,
            "只投 attachmentBackfill 一位，不整包透出 vendor 结构"
        );
        assert_eq!(
            wire_capabilities(false),
            json!({ "attachmentBackfill": false })
        );
        // fake 形状桩位 = 引擎自声明面同源（当前 true，翻位随
        // capabilities::DEVICE_CAPABILITIES 走编译期钉定）。
        assert_eq!(
            wire_capabilities(declared_attachment_backfill()),
            json!({ "attachmentBackfill": true })
        );
    }
}
