// SPDX-License-Identifier: AGPL-3.0-only

//! S16 引擎 hidden 半边（KT 扩展方法 setConversationHidden 存储面）。
//!
//! 台账 §29.77 S16 + §4.242/§29.67 批 W 记档：connector 已立 hidden 独立
//! 位（mrr_factors_json.hidden，消费引擎 listContacts 条目的 isHidden
//! 键）；引擎侧此前无独立 hidden 存储，listContacts 只能按
//! inSystemContacts 反演近似（contacts 表行 = false、非通讯录 profile
//! 行 = true）。本模块 = 两仓契约「connector 落位时写引擎存储」的引擎
//! 半落地：per-account store.db `kv` 表 per-peer 版本化键（kt_ 前缀
//! 惯例，group_cache gse/terminated 先例同款），值 = {"v":1,"hidden":
//! bool}——INSERT OR REPLACE 幂等，重复写同值同形；版本不符/形状坏
//! fail-open（None → 读投影退回反演近似，绝不误伤既有行为）。
//!
//! 官方行为锚（Signal-Desktop 行为对齐，零抄码）：hidden 位是桌面
//! 「消息请求」（message requests）语义的一半——官方收侧
//! isConversationAccepted = !isInSystemContacts && !isHidden（桌面
//! message 状态机）；引擎只存位不裁语义，消费面在 connector/desktop。
//! 无内容泄漏：kv 值只有布尔位与版本号，键只有对端 ACI。

use presage_store_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};

/// per-peer hidden 键前缀（kt_ 惯例，kt_gse_/kt_gterm_ 同族）。
const HIDDEN_KEY_PREFIX: &str = "kt_conv_hidden_";

/// 值格式版本：v1 = `{"v":1,"hidden":bool}`。未来扩列只增版本，读臂按
/// 版本闸 fail-open。
const HIDDEN_RECORD_VERSION: u32 = 1;

fn hidden_key(peer_aci: &str) -> String {
    format!("{HIDDEN_KEY_PREFIX}{peer_aci}")
}

/// 写面错误变体（RPC 面折叠为 -32603；变体仅供归因日志，零内容）。
#[derive(Debug)]
pub(crate) enum SetHiddenError {
    /// 账号未注册。
    UnknownAccount,
    /// peerAci 非法（非连字 UUID / PNI: 前缀）。
    InvalidPeer,
    /// store 层写失败。
    Store,
}

/// v1 值形状（serde 兜形状闸：多余/缺失/坏型字段拒）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HiddenRecord {
    v: u32,
    hidden: bool,
}

/// 写面（幂等）：同 peer 同值重复写 = 同键 INSERT OR REPLACE，逐字节同
/// 形。失败返回 Err（store 层错误，调用方映射 RPC 面）。
pub(crate) async fn set_hidden(
    store: &SqliteStore,
    peer_aci: &str,
    hidden: bool,
) -> Result<(), presage_store_sqlite::SqliteStoreError> {
    let raw = serde_json::to_string(&HiddenRecord {
        v: HIDDEN_RECORD_VERSION,
        hidden,
    })
    .expect("v1 hidden record serializes");
    store.kt_kv_set(&hidden_key(peer_aci), &raw).await
}

/// 读面：有位返回 Some(位)；无键/坏形状/版本不符返回 None（读投影退回
/// inSystemContacts 反演近似，fail-open 不误伤）。
pub(crate) async fn hidden_of(store: &SqliteStore, peer_aci: &str) -> Option<bool> {
    let raw = store.kt_kv_get(&hidden_key(peer_aci)).await.ok()??;
    let record: HiddenRecord = serde_json::from_str(&raw).ok()?;
    (record.v == HIDDEN_RECORD_VERSION).then_some(record.hidden)
}

#[cfg(test)]
mod test {
    use super::*;

    async fn test_store(tag: &str) -> (SqliteStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "kt-engine-conv-hidden-{tag}-{:016x}",
            crate::service::mix_seed_step(crate::service::mix_seed())
        ));
        let store = crate::service::open_store(&dir).await.unwrap();
        (store, dir)
    }

    /// 写读往返：true/false 双值各往返一次，读回原位。
    #[tokio::test]
    async fn hidden_roundtrip_both_values() {
        let (store, dir) = test_store("roundtrip").await;
        let peer = "11111111-2222-4333-8444-555555555555";

        assert_eq!(hidden_of(&store, peer).await, None, "no key = absent");

        set_hidden(&store, peer, true).await.expect("write true");
        assert_eq!(hidden_of(&store, peer).await, Some(true));

        set_hidden(&store, peer, false).await.expect("write false");
        assert_eq!(hidden_of(&store, peer).await, Some(false));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 幂等：同值重复写读回不变（INSERT OR REPLACE 同键同形）。
    #[tokio::test]
    async fn hidden_write_is_idempotent() {
        let (store, dir) = test_store("idempotent").await;
        let peer = "22222222-3333-4444-5555-666666666666";

        set_hidden(&store, peer, true).await.expect("write 1");
        set_hidden(&store, peer, true).await.expect("write 2");
        assert_eq!(hidden_of(&store, peer).await, Some(true));

        // 原值形状逐字节同形（v1 版本号不变）。
        let raw = store
            .kt_kv_get(&hidden_key(peer))
            .await
            .ok()
            .flatten()
            .expect("raw value");
        assert_eq!(raw, r#"{"v":1,"hidden":true}"#);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// fail-open：坏形状/异版本值读面 None（读投影退回反演近似）。
    #[tokio::test]
    async fn hidden_read_fails_open_on_foreign_shapes() {
        let (store, dir) = test_store("failopen").await;
        let peer = "33333333-4444-5555-6666-777777777777";

        store
            .kt_kv_set(&hidden_key(peer), "not json")
            .await
            .expect("write garbage");
        assert_eq!(hidden_of(&store, peer).await, None);

        store
            .kt_kv_set(&hidden_key(peer), r#"{"v":99,"hidden":true}"#)
            .await
            .expect("write future version");
        assert_eq!(hidden_of(&store, peer).await, None);

        // 对端隔离：不同 peer 键互不可见。
        set_hidden(&store, peer, true).await.expect("write");
        assert_eq!(
            hidden_of(&store, "44444444-5555-6666-7777-888888888888").await,
            None
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
