// SPDX-License-Identifier: AGPL-3.0-only

//! 群发本地缓存面（R1 性能对齐批，官方行为对齐、非源码复制）：
//!
//! - **GSE（group send endorsements）按群落 kv**：官方把 combined + member
//!   endorsements 存本地 SQLite（ts/sql/server/groupSendEndorsements.std.ts
//!   :23-34,94-111），发送时读本地（groupSendEndorsements.preload.ts
//!   :363-397）。失效条件对齐官方 `validateGroupSendEndorsements`：
//!   剩余有效期须落在 (2h, 2d) 开区间（:136-155，`distance <= 2h` 视为
//!   即将到期、`distance >= 2d` 视为异常超远，均判无效），且缓存时的成员
//!   面必须与本地群态成员面一致（:326-361 missing-endorsements 检查 +
//!   「群成员变化必须刷」的正确性纪律）。
//! - **终止探针缓存**：发送面 terminated 判定的服务器读面结果按群缓存
//!   （官方发送面只看本地 conversation.terminated 标志，从不逐发送探测）。
//!   写面（updateGroup/terminate/leave）后失效；过期重探。
//!
//! 全部 best-effort：kv 读/写/解码失败一律按缓存 miss 处理（宁可多刷，
//! 不用旧数据）。稳态群发路径 0 次额外 HTTPS（MRM 主请求除外），缓存
//! miss 时才走 `fetch_group_with_endorsements`。

use base64::Engine as _;
use presage::libsignal_service::zkgroup::api::groups::GroupSendEndorsementsResponse;
use presage::manager::Manager;
use presage::model::groups::Group;
use presage::store::ContentsStore as _;
use presage_store_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};

use super::StorageContext;

/// GSE 缓存 kv 键前缀（值 = JSON，键 = 群 master key 标准 base64）。
const GSE_KEY_PREFIX: &str = "kt_gse_";
/// 终止探针缓存 kv 键前缀。
const TERMINATED_KEY_PREFIX: &str = "kt_gterm_";
/// 剩余有效期下界：≤ 2h 判失效（官方 TWO_HOURS 同值同向）。
const TWO_HOURS_S: u64 = 2 * 60 * 60;
/// 剩余有效期上界：≥ 2d 判失效（官方 TWO_DAYS 同值同向）。
const TWO_DAYS_S: u64 = 2 * 24 * 60 * 60;
/// 终止探针缓存 TTL：终止态单调（群不解散终止），新鲜窗口内直接采用。
const TERMINATED_PROBE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

fn gse_key(master_key: &[u8]) -> String {
    format!(
        "{GSE_KEY_PREFIX}{}",
        base64::engine::general_purpose::STANDARD.encode(master_key)
    )
}

fn terminated_key(master_key: &[u8]) -> String {
    format!(
        "{TERMINATED_KEY_PREFIX}{}",
        base64::engine::general_purpose::STANDARD.encode(master_key)
    )
}

/// 按群缓存的 GSE 材料（官方 groupSendCombinedEndorsement + 成员覆盖面
/// 的对位压缩：原始 endorsements blob + 失效判定所需元数据）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CachedEndorsements {
    /// GroupSendEndorsementsResponse 原始字节（标准 base64）。
    endorsements_b64: String,
    /// 服务器签发的过期时刻（epoch 秒）。
    expiration: u64,
    /// 拉取时的群全成员 ACI（service-id 串，升序去重）——成员面变化检测
    /// 基准（官方 missing-endorsements 检查同语义）。
    members: Vec<String>,
}

/// GSE 缓存可用性判定（官方 `validateGroupSendEndorsements` 同语义，
/// 纯函数）：过期窗口 (2h, 2d) 开区间 + 成员面逐字一致。可用时返回
/// endorsements 原始字节。
pub(crate) fn endorsements_usable(
    cached: &CachedEndorsements,
    group: &Group,
    now_ms: u64,
) -> Option<Vec<u8>> {
    let members_now = group_member_ids(group);
    if members_now != cached.members {
        return None;
    }
    let now_s = now_ms / 1000;
    let distance = cached.expiration.checked_sub(now_s)?;
    if distance <= TWO_HOURS_S || distance >= TWO_DAYS_S {
        return None;
    }
    base64::engine::general_purpose::STANDARD
        .decode(&cached.endorsements_b64)
        .ok()
        .filter(|bytes| !bytes.is_empty())
}

/// 群全成员 ACI service-id 串（升序去重；判定基准，勿改排序——缓存比较
/// 依赖稳定序）。
fn group_member_ids(group: &Group) -> Vec<String> {
    let mut ids: Vec<String> = group
        .members
        .iter()
        .map(|member| member.aci.service_id_string())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// 本地优先的群态 + GSE 读取：store 群态 + kv GSE 双校验（官方发送前
/// maybeCreateGroupSendEndorsementState 本地读面对位）。任一缺席/失效返回
/// None，调用方走 fetch。零网络。
pub(crate) async fn cached_group_with_endorsements(
    manager: &mut Manager<SqliteStore, presage::manager::Registered>,
    master_key: &[u8],
) -> Option<(Group, Option<Vec<u8>>)> {
    let store = manager.store();
    let key: [u8; 32] = master_key.try_into().ok()?;
    let group = store.group(key).await.ok()??;
    let raw = store.kt_kv_get(&gse_key(master_key)).await.ok()??;
    let cached: CachedEndorsements = serde_json::from_str(&raw).ok()?;
    let bytes = endorsements_usable(&cached, &group, super::now_ms())?;
    Some((group, Some(bytes)))
}

/// fetch 结果落 kv 缓存（best-effort）：endorsements 缺席（含终止群空
/// 响应）不缓存——维持逐发送重取的既有行为。
pub(crate) async fn store_endorsements(
    store: &SqliteStore,
    master_key: &[u8],
    fetched: &Option<(Group, Option<Vec<u8>>)>,
) {
    let Some((group, Some(bytes))) = fetched else {
        return;
    };
    let expiration =
        presage::libsignal_service::zkgroup::deserialize::<GroupSendEndorsementsResponse>(bytes)
            .map(|response| response.expiration().epoch_seconds());
    let Ok(expiration) = expiration else {
        return;
    };
    let cached = CachedEndorsements {
        endorsements_b64: base64::engine::general_purpose::STANDARD.encode(bytes),
        expiration,
        members: group_member_ids(group),
    };
    if let Ok(raw) = serde_json::to_string(&cached) {
        let _ = store.kt_kv_set(&gse_key(master_key), &raw).await;
    }
}

/// GSE 缓存废弃（群写面后/403 刷新前调用； 宁可多刷）。
pub(crate) async fn invalidate_endorsements(store: &SqliteStore, master_key: &[u8]) {
    let _ = store.kt_kv_set(&gse_key(master_key), "").await;
}

/// 终止探针缓存命中面（TTL 内返回 Some）。
async fn load_terminated_probe(store: &SqliteStore, master_key: &[u8]) -> Option<TerminatedProbe> {
    let raw = store.kt_kv_get(&terminated_key(master_key)).await.ok()??;
    let probe: TerminatedProbe = serde_json::from_str(&raw).ok()?;
    if probe_terminated_usable(&probe, super::now_ms()) {
        Some(probe)
    } else {
        None
    }
}

/// 终止探针缓存值。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TerminatedProbe {
    terminated: bool,
    checked_at_ms: u64,
}

/// 探针缓存新鲜度（纯函数）：TTL 内有效。
fn probe_terminated_usable(probe: &TerminatedProbe, now_ms: u64) -> bool {
    now_ms.saturating_sub(probe.checked_at_ms) < TERMINATED_PROBE_TTL_MS
}

/// 发送面 terminated 判定（注册表先行在调用方）：探针缓存 → 服务器单次
/// 读面 → 落缓存。读面失败不缓存（维持既有 fail-open：网络故障不误伤
/// 既有群会话）。
pub(crate) async fn cached_terminated_probe(context: &StorageContext, master_key: &[u8]) -> bool {
    if let Some(probe) = load_terminated_probe(&context.store, master_key).await {
        return probe.terminated;
    }
    let terminated = matches!(
        super::groups::fetch_group_state_for_send(context, master_key).await,
        Some(true)
    );
    if let Ok(raw) = serde_json::to_string(&TerminatedProbe {
        terminated,
        checked_at_ms: super::now_ms(),
    }) {
        let _ = context
            .store
            .kt_kv_set(&terminated_key(master_key), &raw)
            .await;
    }
    terminated
}

/// 群写面后废弃终止探针缓存（写意图已交服务器裁决，本机缓存退位）。
pub(crate) async fn invalidate_terminated_probe(store: &SqliteStore, master_key: &[u8]) {
    let _ = store.kt_kv_set(&terminated_key(master_key), "").await;
}

/// 群写面收尾（updateGroup/terminateGroup/leaveGroup 成功后调用）：GSE 与
/// 终止探针缓存一并废弃（写后本地群态刷新若因网络失败，成员面检测失灵，
/// 显式废弃保住「宁可多刷不可用旧」；写后刷新成功时下轮发送会重建缓存）。
pub(crate) async fn invalidate_after_group_write(store: &SqliteStore, master_key: &[u8]) {
    invalidate_endorsements(store, master_key).await;
    invalidate_terminated_probe(store, master_key).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::groups_v2::Role;
    use presage::libsignal_service::prelude::{ProfileKey, Uuid};
    use presage::libsignal_service::protocol::Aci;
    use presage::model::groups::Member;
    use std::str::FromStr;

    fn member(aci: &str) -> Member {
        Member {
            aci: Aci::from(Uuid::from_str(aci).unwrap()),
            role: Role::Default,
            profile_key: ProfileKey::create([0u8; 32]),
            joined_at_revision: 0,
            label: None,
            label_emoji: None,
        }
    }

    fn group_with_members(acis: &[&str]) -> Group {
        Group {
            title: String::new(),
            avatar: String::new(),
            disappearing_messages_timer: None,
            access_control: None,
            revision: 0,
            members: acis.iter().map(|aci| member(aci)).collect(),
            pending_members: Vec::new(),
            requesting_members: Vec::new(),
            invite_link_password: Vec::new(),
            description: None,
            announcements_only: false,
            banned_members: Vec::new(),
        }
    }

    fn cached(members: &[&str], expiration_in_s: i64, now_ms: u64) -> CachedEndorsements {
        CachedEndorsements {
            endorsements_b64: base64::engine::general_purpose::STANDARD.encode(b"blob"),
            expiration: (now_ms / 1000).wrapping_add_signed(expiration_in_s),
            members: {
                let mut ids: Vec<String> = members.iter().map(|s| s.to_string()).collect();
                ids.sort();
                ids
            },
        }
    }

    const NOW_MS: u64 = 1_700_000_000_000;

    /// 官方过期窗口 (2h, 2d) 开区间：≤2h 失效（恰 2h 边界含）、窗口内
    /// 有效、≥2d 失效（恰 2d 边界含）、已过期失效。
    #[test]
    fn endorsement_window_matches_official_two_hours_two_days() {
        let group = group_with_members(&["11111111-1111-1111-1111-111111111111"]);
        let members = ["11111111-1111-1111-1111-111111111111"];

        // 恰 2h 剩余 → 失效（官方 distance <= TWO_HOURS 同向）。
        assert!(endorsements_usable(
            &cached(&members, TWO_HOURS_S as i64, NOW_MS),
            &group,
            NOW_MS
        )
        .is_none());
        // 2h+1s → 有效。
        assert!(endorsements_usable(
            &cached(&members, TWO_HOURS_S as i64 + 1, NOW_MS),
            &group,
            NOW_MS
        )
        .is_some());
        // 恰 2d 剩余 → 失效（官方 distance >= TWO_DAYS 同向）。
        assert!(
            endorsements_usable(&cached(&members, TWO_DAYS_S as i64, NOW_MS), &group, NOW_MS)
                .is_none()
        );
        // 2d-1s → 有效。
        assert!(endorsements_usable(
            &cached(&members, TWO_DAYS_S as i64 - 1, NOW_MS),
            &group,
            NOW_MS
        )
        .is_some());
        // 已过期 → 失效。
        assert!(endorsements_usable(&cached(&members, -1, NOW_MS), &group, NOW_MS).is_none());
    }

    /// 成员面变化必须刷（官方 missing-endorsements 检查 + 正确性优先）：
    /// 增员/减员/成员序无关比较。
    #[test]
    fn endorsement_member_change_invalidates_cache() {
        let a = "11111111-1111-1111-1111-111111111111";
        let b = "22222222-2222-2222-2222-222222222222";
        let group = group_with_members(&[a, b]);
        let in_window = TWO_HOURS_S as i64 + 600;

        // 成员一致（乱序输入按稳定序归一）→ 有效。
        assert!(endorsements_usable(&cached(&[b, a], in_window, NOW_MS), &group, NOW_MS).is_some());
        // 缓存缺 b（本机群态已见 b 入群）→ 失效。
        assert!(endorsements_usable(&cached(&[a], in_window, NOW_MS), &group, NOW_MS).is_none());
        // 缓存多出 c（c 已退群）→ 失效。
        let c = "33333333-3333-3333-3333-333333333333";
        assert!(
            endorsements_usable(&cached(&[a, b, c], in_window, NOW_MS), &group, NOW_MS).is_none()
        );
    }

    /// 本地群态缺席（群未落库）时本地路径直接 miss（交 fetch 面）。
    #[test]
    fn empty_endorsement_blob_is_unusable() {
        let group = group_with_members(&["11111111-1111-1111-1111-111111111111"]);
        let members = ["11111111-1111-1111-1111-111111111111"];
        let mut entry = cached(&members, TWO_HOURS_S as i64 + 600, NOW_MS);
        entry.endorsements_b64 = String::new();
        assert!(endorsements_usable(&entry, &group, NOW_MS).is_none());
    }

    /// 终止探针缓存新鲜度：TTL 内有效、过 TTL 失效、恰 TTL 边界失效。
    #[test]
    fn terminated_probe_respects_ttl() {
        let probe = TerminatedProbe {
            terminated: true,
            checked_at_ms: NOW_MS,
        };
        assert!(probe_terminated_usable(
            &probe,
            NOW_MS + TERMINATED_PROBE_TTL_MS - 1
        ));
        assert!(!probe_terminated_usable(
            &probe,
            NOW_MS + TERMINATED_PROBE_TTL_MS
        ));
    }

    /// GSE 序列化往返（kv 值形状稳定性——跨升级读取的兼容面）。
    #[test]
    fn cached_endorsements_json_roundtrip() {
        let entry = cached(
            &["11111111-1111-1111-1111-111111111111"],
            TWO_HOURS_S as i64 + 600,
            NOW_MS,
        );
        let raw = serde_json::to_string(&entry).unwrap();
        let back: CachedEndorsements = serde_json::from_str(&raw).unwrap();
        assert_eq!(entry, back);
    }
}
