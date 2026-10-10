// SPDX-License-Identifier: AGPL-3.0-only

//! 引擎协议能力自证面（`engine.protocolCapabilities`）的事实源登记。
//!
//! 引擎以 linked device 身份向服务器做两处协议能力声明（vendor 真相源，
//! 本模块不发明任何位值）：
//!
//! 1. 账号属性面 `DeviceCapabilities`——接收管线首步 presage
//!    `set_account_attributes` 推送（vendor/presage/presage/src/manager/
//!    registered.rs，`capabilities: Some(DeviceCapabilities { … })` 构造点；
//!    主注册链 confirmation.rs 同字面值一并钉定）。该构造是 vendor 内联
//!    字面值、无导出常量可调用，故此处持**钉定镜像**（同型 vendor 结构体
//!    const 构造——vendor 增删字段时本模块编译期即断），漂移由模块单测扫
//!    vendor 源码拦下。
//! 2. 链接面 `LinkCapabilities`——`link_device` 链路取
//!    `LinkCapabilities::default()`（vendor/libsignal-service/src/
//!    provisioning/mod.rs 构造点，push_service/linking.rs Default 实现）。
//!    构造器可 pub 调用，运行时 **live 复用**，零镜像。
//!
//! 依赖基线（presage rev / libsignal 版本）由 build.rs 从本仓 Cargo.toml/
//! Cargo.lock 解析注入 `KT_ENGINE_*` env 常量。防漂移闸四层：镜像位值
//! 编译期 const 钉死（位值/字段集漂移直接断编译）、镜像经 vendor 类型
//! 全字段显式构造（vendor 字段集变化同样编译期断）、单测扫 vendor 构造
//! 点源码字面（vendor 升级改位/改构造点即红灯）、基线锚值单测硬编码钉
//! 死（依赖升级必须撞闸重走能力位复核）——不允许无人知晓。

use presage::libsignal_service::push_service::linking::LinkCapabilities;
use presage::libsignal_service::websocket::account::DeviceCapabilities;
use serde_json::{json, Value};

/// 账号属性面声明镜像：registered.rs `set_account_attributes`（与
/// confirmation.rs 主注册链同值）的六位字面值。vendor 升级改位 = 单测
/// `device_capabilities_bits_match_vendor_chain` 红灯，复核后同步此处。
pub(crate) const DEVICE_CAPABILITIES: DeviceCapabilities = DeviceCapabilities {
    storage: true,
    transfer: false,
    attachment_backfill: false,
    spqr: true,
    profiles_v2: false,
    username_change_sync_message: true,
};

/// 镜像位值编译期钉定：位值或 vendor 字段集漂移直接断编译，不进运行时。
const _: () = {
    assert!(DEVICE_CAPABILITIES.storage);
    assert!(!DEVICE_CAPABILITIES.transfer);
    assert!(!DEVICE_CAPABILITIES.attachment_backfill);
    assert!(DEVICE_CAPABILITIES.spqr);
    assert!(!DEVICE_CAPABILITIES.profiles_v2);
    assert!(DEVICE_CAPABILITIES.username_change_sync_message);
};

/// `engine.protocolCapabilities` 应答体：只读、零网络、账号无关，未绑定
/// 账号照常应答。能力位 JSON 形状 = vendor serde 属性输出（camelCase；
/// `profiles_v2` 按上游字面保持 snake——线上声明即此拼写，消费方不得
/// "纠正"），键值即引擎向服务器声明的位。
pub(crate) fn protocol_capabilities_payload() -> Value {
    json!({
        "deviceCapabilities": vendor_json(&DEVICE_CAPABILITIES),
        "linkCapabilities": vendor_json(&LinkCapabilities::default()),
        "baseline": {
            "presageRev": env!("KT_ENGINE_PRESAGE_REV"),
            "libsignalVersion": env!("KT_ENGINE_LIBSIGNAL_VERSION"),
            "libsignalCommit": env!("KT_ENGINE_LIBSIGNAL_COMMIT"),
        },
    })
}

/// vendor 结构体按其 serde 属性出 JSON（布尔位结构体序列化不可失败）。
fn vendor_json<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("capability structs serialize infallibly")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(source: &str) -> String {
        source.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn vendor_source(rel: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("vendor source {} unreadable: {error}", path.display()))
    }

    /// vendor 构造点位在空白归一后的块体（anchor 起，至收口 `})`）。
    fn construction_block(source: &str, anchor: &str) -> String {
        let flat = normalized(source);
        let start = flat
            .find(anchor)
            .unwrap_or_else(|| panic!("anchor `{anchor}` found"));
        let end = start + flat[start..].find("})").expect("construction closes");
        flat[start..end].to_owned()
    }

    #[test]
    fn mirror_agrees_with_live_vendor_constructors() {
        let link = LinkCapabilities::default();
        assert_eq!(link.spqr, DEVICE_CAPABILITIES.spqr);
        assert_eq!(
            link.attachment_backfill,
            DEVICE_CAPABILITIES.attachment_backfill
        );
        assert_eq!(
            link.username_change_sync_message,
            DEVICE_CAPABILITIES.username_change_sync_message
        );
        // 反序列化缺省面（websocket/account.rs Default）同为 spqr 开位；
        // 账号属性面 storage/username 位与其不同（registered.rs 显式置位），
        // 镜像不得混用两个 Default。
        assert_eq!(DeviceCapabilities::default().spqr, DEVICE_CAPABILITIES.spqr);
        assert_ne!(
            DeviceCapabilities::default().storage,
            DEVICE_CAPABILITIES.storage
        );
    }

    #[test]
    fn device_capabilities_bits_match_vendor_chain() {
        // B4 身份事件面批：接收流 refresh task 停发 AccountAttributes PUT
        // （vendored registered.rs 构造点已删除），vendor 唯一剩余构造点 =
        // 主设备注册路径（confirmation.rs，引擎链接设备不走的协议必需腿），
        // 镜像仍以它为能力位真相源。
        for rel in ["vendor/presage/presage/src/manager/confirmation.rs"] {
            let block = construction_block(
                &vendor_source(rel),
                "capabilities: Some(DeviceCapabilities {",
            );
            for field in [
                "storage: true,",
                "transfer: false,",
                "attachment_backfill: false,",
                "spqr: true,",
                "profiles_v2: false,",
                "username_change_sync_message: true,",
            ] {
                assert!(
                    block.contains(field),
                    "{rel} 能力位漂移：构造点缺 `{field}`——vendor 升级请复核并同步 service::capabilities 镜像"
                );
            }
        }
    }

    #[test]
    fn link_capabilities_live_default_matches_vendor_chain() {
        // 构造点确用 Default（provisioning/mod.rs），且 Default 实现三位
        // 字面与 live 值一致（push_service/linking.rs）。
        let flat = normalized(&vendor_source(
            "vendor/libsignal-service/src/provisioning/mod.rs",
        ));
        assert!(flat.contains("capabilities: LinkCapabilities::default(),"));
        let default_impl = normalized(&vendor_source(
            "vendor/libsignal-service/src/push_service/linking.rs",
        ));
        assert!(default_impl.contains(
            "Self { attachment_backfill: false, spqr: true, username_change_sync_message: true, }"
        ));
        let link = LinkCapabilities::default();
        assert!(!link.attachment_backfill);
        assert!(link.spqr);
        assert!(link.username_change_sync_message);
    }

    #[test]
    fn payload_shape_and_values() {
        let payload = protocol_capabilities_payload();
        let obj = payload.as_object().expect("payload object");
        assert_eq!(
            obj.keys().collect::<Vec<_>>().as_slice(),
            ["baseline", "deviceCapabilities", "linkCapabilities"]
        );

        let device = obj["deviceCapabilities"].as_object().expect("object");
        assert_eq!(
            device.keys().collect::<Vec<_>>().as_slice(),
            [
                "attachmentBackfill",
                "profiles_v2",
                "spqr",
                "storage",
                "transfer",
                "usernameChangeSyncMessage"
            ]
        );
        assert_eq!(device["spqr"], json!(true));
        assert_eq!(device["storage"], json!(true));
        assert_eq!(device["transfer"], json!(false));
        assert_eq!(device["attachmentBackfill"], json!(false));
        assert_eq!(device["profiles_v2"], json!(false));
        assert_eq!(device["usernameChangeSyncMessage"], json!(true));

        let link = obj["linkCapabilities"].as_object().expect("object");
        assert_eq!(
            link.keys().collect::<Vec<_>>().as_slice(),
            ["attachmentBackfill", "spqr", "usernameChangeSyncMessage"]
        );
        assert_eq!(link["spqr"], json!(true));

        let baseline = obj["baseline"].as_object().expect("object");
        assert_eq!(
            baseline.keys().collect::<Vec<_>>().as_slice(),
            ["libsignalCommit", "libsignalVersion", "presageRev"]
        );
    }

    #[test]
    fn baseline_env_pins_match_repo_declared_dependency_pins() {
        // 刻意硬编码钉死：任一值变化（依赖升级）即红灯，强制走一遍能力位
        // 复核（vendor 构造点扫描单测同闸）——这正是自证面存在的意义。
        assert_eq!(
            env!("KT_ENGINE_PRESAGE_REV"),
            "33dd1491130793390e356e15a5711f1e6ca14fdb"
        );
        assert_eq!(env!("KT_ENGINE_LIBSIGNAL_VERSION"), "0.99.0");
        assert_eq!(
            env!("KT_ENGINE_LIBSIGNAL_COMMIT"),
            "439f035d9cfd7efe5435cbea0294591c9ed17232"
        );
    }
}
