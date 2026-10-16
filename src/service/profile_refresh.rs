// SPDX-License-Identifier: AGPL-3.0-only

//! 联系人 profile 名字材料刷新读路径（`profiles.refreshContacts` + 登录期
//! 自动批，KT 扩展方法，2026-10-16 profile 名字刷新批，compat §3/§5）。
//!
//! 背景：引擎此前只写自己的 profile、从不读别人的——JSON-RPC 全集无
//! getProfile 类方法，桌面端联系人全是「未知联系人」/掩码名。名字材料在
//! 服务器可得（profile_keys 随 contacts 同步落库），本模块对 store 中
//! 「持 profile_key 且名字材料为空」的联系人批量拉取 profile 并落库。
//!
//! 落表查证结论（本批先查证后动工，compat §5 批块）：presage
//! `Manager::retrieve_profile_by_uuid`（缓存查 → 服务器拉 →
//! `ProfileCipher::decrypt` → `store.save_profile`）把名字落
//! **profiles 表**（uuid, given_name, family_name, …），不是 contacts.name
//! ——contacts 表与 profiles 表无写联动。listContacts 投影从 profiles 表
//! 读（见 super::list_contacts / project_contact_entry）。
//!
//! 纪律：
//! - 并发有界 ≤3（独立 `Manager::load_registered` 仅为 store 连接隔离与
//!   读并发；websocket 槽自 2026-10-16 单连接纪律批起由 libsignal-service
//!   进程级 slot 注册表归一——同账号全引擎共享一条 identified 连接，不再
//!   各持独立槽；单联系人失败不中断批次；整批时限
//!   [`REFRESH_BATCH_DEADLINE_MS`]（到期未尝试计 failed——诚实语义））。
//! - wire 返回 `{refreshed, failed, skipped}` 三计数，**只计数不含任何
//!   身份/号码/名字内容**（引擎零联系人外传纪律同向）。
//! - presage 侧 profile 缓存（查到即回、零网络）使重复刷新幂等。
//! - 名字权威面：contacts.name 非空的联系人不刷新（手机 contacts sync
//!   权威，engine 不发明 system/nickname 面，storage_merge 同纪律）。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use presage::libsignal_service::prelude::Uuid;
use presage::libsignal_service::profile_name::ProfileName;
use presage::libsignal_service::protocol::{Aci, ServiceId};
use presage::libsignal_service::zkgroup::profiles::ProfileKey;
use presage::manager::{Manager, Registered};
use presage::store::ContentsStore as _;
use presage_store_sqlite::SqliteStore;

use super::{open_store, Service};

/// 整批时限（毫秒）：每次拉取尝试前检查，到期未尝试计 failed（诚实语义：
/// 本次刷新尝试未使其刷新成功；在途一条允许完成到尾——单条超时兜底）。
pub(crate) const REFRESH_BATCH_DEADLINE_MS: u64 = 20_000;
/// 单次拉取超时（毫秒，服务器往返面）：超时计 failed，防单点网络悬挂拖垮
/// 整批（worst case 批尾 = 时限 + 单条超时）。
pub(crate) const REFRESH_FETCH_TIMEOUT_MS: u64 = 10_000;
/// 并发上界（独立 manager 数 = store 连接数；websocket 槽进程级归一，
/// 见 [`profile_refresh`] 模块纪律注）。
pub(crate) const REFRESH_CONCURRENCY: usize = 3;
/// 登录期自动批沉降（毫秒）：sync 臂 contacts/manifest 异步落库等待，之后
/// 跑刷新单点（材料没落库就跑只会空转，幂等无害）。
pub(crate) const AUTO_REFRESH_SETTLE_MS: u64 = 10_000;

/// 刷新计数面（wire `{refreshed, failed, skipped}` 三 u32 恒在）。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RefreshCounts {
    pub(crate) refreshed: u32,
    pub(crate) failed: u32,
    pub(crate) skipped: u32,
}

/// 错误面：账号不可达（注册表无此号 / store 打不开）。methods 层折叠
/// `-32603`（细节不外泄，listContacts 未注册账号同折叠）。
#[derive(Debug)]
pub(crate) enum RefreshError {
    UnknownAccount,
}

/// 单联系人刷新决策（纯函数，测试钉形）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefreshDecision {
    /// 需要拉取（profile_key 在库、contacts.name 空、profiles 表无名字材料）。
    Refresh,
    /// 无 profile_key（服务器侧名字材料不可得，getAvatar 同口径诚实跳过）。
    SkipNoProfileKey,
    /// contacts.name 非空（名字权威 = 手机 contacts sync，不覆盖）。
    SkipContactNamed,
    /// profiles 表已有名字材料（已刷新过 / presage 缓存在库）。
    SkipMaterialPresent,
}

/// 决策单点：判定序固定（key → contacts 名 → 材料在库）。
pub(crate) fn decide_refresh(
    contact_name: &str,
    has_profile_key: bool,
    stored_has_material: bool,
) -> RefreshDecision {
    if !has_profile_key {
        return RefreshDecision::SkipNoProfileKey;
    }
    if !contact_name.is_empty() {
        return RefreshDecision::SkipContactNamed;
    }
    if stored_has_material {
        return RefreshDecision::SkipMaterialPresent;
    }
    RefreshDecision::Refresh
}

/// 名字材料判定（纯函数）：`profile.name` 在场且 given/family 有一件非空
/// 才算材料（服务器侧空 profile 名不算——拉了也没有可投影物）。
pub(crate) fn profile_name_has_material(name: &ProfileName<String>) -> bool {
    !name.given_name.is_empty()
        || name
            .family_name
            .as_ref()
            .is_some_and(|family| !family.is_empty())
}

/// 目标分块（纯函数，测试钉形）：round-robin 均衡分发到 ≤n 块（并发上界
/// 即块数；目标数少于块数时空块不出工）。
pub(crate) fn chunk_targets<T>(targets: Vec<T>, max_chunks: usize) -> Vec<Vec<T>> {
    if targets.is_empty() {
        return Vec::new();
    }
    let chunk_count = max_chunks.clamp(1, targets.len());
    let mut chunks: Vec<Vec<T>> = (0..chunk_count).map(|_| Vec::new()).collect();
    for (index, target) in targets.into_iter().enumerate() {
        chunks[index % chunk_count].push(target);
    }
    chunks
}

/// 批量刷新单点（显式方法与登录期自动批共用）：目标收集（本地零网络）→
/// 有界并发拉取（presage 同路径：缓存 → 服务器 → ProfileCipher 解密 →
/// profiles 表落库）→ 计数聚合。
pub(crate) async fn refresh_contact_profiles(
    service: &Arc<Service>,
    account: &str,
) -> Result<RefreshCounts, RefreshError> {
    if !service.real {
        // fake 形状桩（compat §6）：零计数应答，无 store 可达。
        return Ok(RefreshCounts::default());
    }
    let batch_started = std::time::Instant::now();
    let dir = {
        let state = service.state.lock().await;
        let rel = state
            .account_dir(account)
            .ok_or(RefreshError::UnknownAccount)?;
        service.data_dir.join(rel)
    };
    // 归因探针（2026-10-16 listGroups Internal 批）：批开始（零内容——
    // 账号不落，落目标面数量级即可），与 listGroups 0ms 失败窗时序对表。
    tracing::warn!("profile refresh batch start");
    let store_open_started = std::time::Instant::now();
    let store = match open_store(&dir).await {
        Ok(store) => {
            tracing::warn!(
                stage = "collect_store_open",
                ok = true,
                elapsed_ms = store_open_started.elapsed().as_millis() as u64,
                "profile refresh store open",
            );
            store
        }
        Err(()) => {
            tracing::warn!(
                stage = "collect_store_open",
                ok = false,
                elapsed_ms = store_open_started.elapsed().as_millis() as u64,
                "profile refresh store open",
            );
            return Err(RefreshError::UnknownAccount);
        }
    };

    // —— 目标收集：profile_key 权威读（profile_keys 表，join 键同源）+
    // 材料在库跳过检查（本地读零网络）。contacts/profile_keys/profile
    // 读失败按该联系人 skip 处理（读面故障不炸整批，尝试面会暴露真失败）。
    let mut targets: Vec<(Uuid, ProfileKey)> = Vec::new();
    let mut counts = RefreshCounts::default();
    let contacts = store
        .contacts()
        .await
        .map_err(|_| RefreshError::UnknownAccount)?;
    for contact in contacts.filter_map(Result::ok) {
        let uuid = contact.uuid;
        let Some(key) = store
            .profile_key(&ServiceId::Aci(Aci::from(uuid)))
            .await
            .ok()
            .flatten()
        else {
            counts.skipped += 1;
            continue;
        };
        let stored_has_material = store
            .profile(uuid, key)
            .await
            .ok()
            .flatten()
            .and_then(|profile| profile.name)
            .map(|name| profile_name_has_material(&name))
            .unwrap_or(false);
        if !matches!(
            decide_refresh(&contact.name, true, stored_has_material),
            RefreshDecision::Refresh
        ) {
            counts.skipped += 1;
            continue;
        }
        targets.push((uuid, key));
    }
    drop(store);

    // —— 有界并发执行：round-robin ≤3 块，每块独立 manager（store 连接
    // 隔离；websocket 槽由进程级 slot 注册表归一共享，不新增连接）；逐条
    // 尝试前查时限，到期未尝试计 failed。
    let deadline = tokio::time::Instant::now() + Duration::from_millis(REFRESH_BATCH_DEADLINE_MS);
    let chunks = chunk_targets(targets, REFRESH_CONCURRENCY);
    let partials = futures::future::join_all(chunks.iter().map(|chunk| async {
        let mut partial = RefreshCounts::default();
        refresh_chunk(&dir, chunk, deadline, &mut partial).await;
        partial
    }))
    .await;
    for partial in partials {
        counts.refreshed += partial.refreshed;
        counts.failed += partial.failed;
    }
    // 归因探针：批结束（计数 + 全批耗时，与开始探针对表锁窗时序）。
    tracing::warn!(
        refreshed = counts.refreshed,
        failed = counts.failed,
        skipped = counts.skipped,
        elapsed_ms = batch_started.elapsed().as_millis() as u64,
        "profile refresh batch done",
    );
    Ok(counts)
}

/// 单块执行：独立 store 连接 + manager（websocket 已由进程级 slot 注册表
/// 归一共享，见模块纪律注），逐联系人 presage 拉取（超时/失败计 failed，
/// 不中断块内后续目标）。块级基础设施失败（store 打不开/注册数据不可读）
/// = 该块全部目标计 failed（诚实：未刷新成功）。
async fn refresh_chunk(
    dir: &Path,
    targets: &[(Uuid, ProfileKey)],
    deadline: tokio::time::Instant,
    counts: &mut RefreshCounts,
) {
    if targets.is_empty() {
        return;
    }
    // 归因探针（每 Manager store 打开结果，含耗时 ms）：独立连接/manager
    // 与主 manager 的 store 并发面即本批归因重点，打开成败与耗时落日志。
    let store_open_started = std::time::Instant::now();
    let store_open = open_store(dir).await;
    tracing::warn!(
        stage = "chunk_store_open",
        ok = store_open.is_ok(),
        targets = targets.len(),
        elapsed_ms = store_open_started.elapsed().as_millis() as u64,
        "profile refresh store open",
    );
    let Ok(store) = store_open else {
        counts.failed += targets.len() as u32;
        return;
    };
    let manager_open_started = std::time::Instant::now();
    let manager_open = Manager::<SqliteStore, Registered>::load_registered(store).await;
    tracing::warn!(
        stage = "chunk_manager_open",
        ok = manager_open.is_ok(),
        targets = targets.len(),
        elapsed_ms = manager_open_started.elapsed().as_millis() as u64,
        "profile refresh manager open",
    );
    let Ok(mut manager) = manager_open else {
        counts.failed += targets.len() as u32;
        return;
    };
    for (uuid, key) in targets {
        // 整批时限：尝试前检查（在途一条允许完成到尾，单条超时兜底）。
        if tokio::time::Instant::now() >= deadline {
            counts.failed += 1;
            continue;
        }
        let fetch = manager.retrieve_profile_by_uuid(Aci::from(*uuid), *key);
        match tokio::time::timeout(Duration::from_millis(REFRESH_FETCH_TIMEOUT_MS), fetch).await {
            Ok(Ok(_profile)) => counts.refreshed += 1,
            Ok(Err(_)) | Err(_) => counts.failed += 1,
        }
    }
}

/// ProfileName → (givenName, familyName) 材料对（listContacts 投影面共用）：
/// 材料不在（name 缺席或两件全空）= None（整键不在的判定交给投影单点）。
pub(crate) fn profile_name_pair(name: &ProfileName<String>) -> Option<(&str, Option<&str>)> {
    let given = name.given_name.as_str();
    let family = name.family_name.as_deref();
    let has_material = !given.is_empty() || family.is_some_and(|f| !f.is_empty());
    has_material.then_some((given, family))
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::Profile;

    fn name_with(given: &str, family: Option<&str>) -> ProfileName<String> {
        ProfileName {
            given_name: given.to_owned(),
            family_name: family.map(str::to_owned),
        }
    }

    /// 决策单点四臂钉形：无 key 跳过、contacts 名权威跳过、材料在库跳过、
    /// 其余刷新。判定序固定（key 优先于名、名优先于材料）。
    #[test]
    fn decide_refresh_arms() {
        assert_eq!(
            decide_refresh("", false, false),
            RefreshDecision::SkipNoProfileKey
        );
        assert_eq!(
            decide_refresh("", false, true),
            RefreshDecision::SkipNoProfileKey,
            "无 key 无从拉取，材料位不参与"
        );
        assert_eq!(
            decide_refresh("Alice", true, false),
            RefreshDecision::SkipContactNamed
        );
        assert_eq!(
            decide_refresh("Alice", true, true),
            RefreshDecision::SkipContactNamed,
            "判定序：contacts 名优先于材料位"
        );
        assert_eq!(
            decide_refresh("", true, true),
            RefreshDecision::SkipMaterialPresent
        );
        assert_eq!(decide_refresh("", true, false), RefreshDecision::Refresh);
    }

    /// 名字材料判定：given 非空 / family 非空（given 空）算材料；两件全空、
    /// family=None 全空不算。
    #[test]
    fn profile_material_arms() {
        assert!(profile_name_has_material(&name_with("Alice", None)));
        assert!(profile_name_has_material(&name_with("", Some("Lee"))));
        assert!(!profile_name_has_material(&name_with("", None)));
        assert!(!profile_name_has_material(&name_with("", Some(""))));
        let no_name = Profile {
            name: None,
            about: None,
            about_emoji: None,
            avatar: None,
            unrestricted_unidentified_access: false,
        };
        assert!(no_name.name.is_none());
    }

    /// 分块面：round-robin 均衡（7→3/2/2）、目标少于块数不出空块、空目标
    /// 零块、块数上界钳制（并发上界即块数）。
    #[test]
    fn chunk_targets_distribution() {
        let chunks = chunk_targets(vec![0, 1, 2, 3, 4, 5, 6], 3);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [3, 2, 2]);
        let chunks = chunk_targets(vec![0, 1], 3);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
        assert!(chunk_targets(Vec::<u8>::new(), 3).is_empty());
        let chunks = chunk_targets(vec![0, 1, 2], 3);
        assert_eq!(chunks.len(), 3);
        // 相邻目标不落同块（round-robin 语义，单块失败面小）。
        let chunks = chunk_targets(vec![0, 1, 2, 3], 2);
        assert_eq!(chunks[0], vec![0, 2]);
        assert_eq!(chunks[1], vec![1, 3]);
    }

    /// ProfileName → 材料对投影：两件全空 = None（整键不在判定前置）；
    /// 单件空照常出对（family=None / family=Some("") 同为缺席语义）。
    #[test]
    fn profile_name_pair_arms() {
        let name = ProfileName {
            given_name: "Alice".to_owned(),
            family_name: None,
        };
        assert_eq!(profile_name_pair(&name), Some(("Alice", None)));
        let name = ProfileName {
            given_name: "".to_owned(),
            family_name: Some("Lee".to_owned()),
        };
        assert_eq!(profile_name_pair(&name), Some(("", Some("Lee"))));
        let name = ProfileName {
            given_name: "".to_owned(),
            family_name: None,
        };
        assert_eq!(profile_name_pair(&name), None);
    }
}
