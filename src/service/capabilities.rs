// SPDX-License-Identifier: AGPL-3.0-only

//! 引擎协议能力自证面（`engine.protocolCapabilities`）的事实源登记。
//!
//! 引擎以 linked device 身份向服务器做两处协议能力声明（vendor 真相源，
//! 本模块不发明任何位值）：
//!
//! 1. 账号属性面 `DeviceCapabilities`——vendor 主注册链
//!    confirmation.rs 的 `capabilities: Some(DeviceCapabilities { … })`
//!    构造点（设备注册协议必需腿；B4 起接收管线停发 AccountAttributes
//!    PUT——官方链接设备从不 PUT attributes，vendored registered.rs 构造
//!    点已删除，管线不再推送）。该构造是 vendor 内联字面值、无导出常量
//!    可调用，故此处持**钉定镜像**（同型 vendor 结构体 const 构造——
//!    vendor 增删字段时本模块编译期即断），漂移由模块单测扫 vendor 源码
//!    拦下。
//! 2. 链接面 `LinkCapabilities`——`link_device` 链路取
//!    `LinkCapabilities::default()`（vendor/libsignal-service/src/
//!    provisioning/mod.rs 构造点，push_service/linking.rs Default 实现）。
//!    构造器可 pub 调用，运行时 **live 复用**，零镜像。
//! 3. 设备面注册 `CapabilitiesUpload`——接收管线建流时 best-effort
//!    `PUT /v1/devices/capabilities`（官方桌面每次链接启动
//!    afterEveryLinkedStartup → registerCapabilities 同位，D5 批）；
//!    vendor 构造器可 pub 调用，运行时 **live 复用**（常量经
//!    `DEVICE_CAPABILITIES_UPLOAD` 钉定位值）。
//!
//! 依赖基线（presage rev / libsignal 版本）由 build.rs 从本仓 Cargo.toml/
//! Cargo.lock 解析注入 `KT_ENGINE_*` env 常量。防漂移闸四层：镜像位值
//! 编译期 const 钉死（位值/字段集漂移直接断编译）、镜像经 vendor 类型
//! 全字段显式构造（vendor 字段集变化同样编译期断）、单测扫 vendor 构造
//! 点源码字面（vendor 升级改位/改构造点即红灯）、基线锚值单测硬编码钉
//! 死（依赖升级必须撞闸重走能力位复核）——不允许无人知晓。

use presage::libsignal_service::push_service::linking::LinkCapabilities;
use presage::libsignal_service::websocket::account::{CapabilitiesUpload, DeviceCapabilities};
use serde_json::{json, Value};

/// 账号属性面声明镜像：vendor 注册链 confirmation.rs 构造点（B4 起唯一
/// 剩余构造点——接收管线停发 AccountAttributes PUT，registered.rs 构造
/// 点已删除）的八位字面值。vendor 升级改位 = 单测
/// `device_capabilities_bits_match_vendor_chain` 红灯，复核后同步此处。
/// D5 起补齐 server 真相（Signal-Server DeviceCapability.java 全集）的
/// 两位：`versioned_expiration_timer`（官方桌面设备面不上传，引擎不声明
/// 特性支持）、`optional_phone_number`（官方上传值 = 账号码共享状态位
/// `getOptionalNumber() == null`；KT 引擎账号号码恒共享态 = false）。
pub(crate) const DEVICE_CAPABILITIES: DeviceCapabilities = DeviceCapabilities {
    storage: true,
    transfer: false,
    // 附件回填闭环批（2026-10-18）：引擎已实现 backfill 请求发送面
    // （requestAttachmentBackfill）+ 响应收投影臂，按官方桌面「已实现
    // backfill 恒 true」口径翻位；compat §5 偏差登记同步翻正。
    attachment_backfill: true,
    spqr: true,
    profiles_v2: false,
    username_change_sync_message: true,
    versioned_expiration_timer: false,
    optional_phone_number: false,
};

/// 设备面注册位镜像（官方 `CapabilitiesUploadType` 四位，
/// `ts/types/Capabilities.std.ts`；`PUT /v1/devices/capabilities` body
/// 逐键同形）：spqr/usernameChangeSyncMessage 与账号属性面同位 true；
/// attachmentBackfill 官方桌面恒 true——附件回填闭环批（2026-10-18）起
/// 引擎同声明 true（请求发送面 + 响应收投影臂均已落库）；link 面
/// （LinkCapabilities::default()，vendored 恒 false）不在本镜像内，偏差
/// 登记 compat §5。optional_phoneNumber 状态位同 DEVICE_CAPABILITIES
/// 口径 false。
pub(crate) const DEVICE_CAPABILITIES_UPLOAD: CapabilitiesUpload = CapabilitiesUpload {
    attachment_backfill: true,
    spqr: true,
    username_change_sync_message: true,
    optional_phone_number: false,
};

/// 镜像位值编译期钉定：位值或 vendor 字段集漂移直接断编译，不进运行时。
const _: () = {
    assert!(DEVICE_CAPABILITIES.storage);
    assert!(!DEVICE_CAPABILITIES.transfer);
    assert!(DEVICE_CAPABILITIES.attachment_backfill);
    assert!(DEVICE_CAPABILITIES.spqr);
    assert!(!DEVICE_CAPABILITIES.profiles_v2);
    assert!(DEVICE_CAPABILITIES.username_change_sync_message);
    assert!(!DEVICE_CAPABILITIES.versioned_expiration_timer);
    assert!(!DEVICE_CAPABILITIES.optional_phone_number);
    assert!(DEVICE_CAPABILITIES_UPLOAD.attachment_backfill);
    assert!(DEVICE_CAPABILITIES_UPLOAD.spqr);
    assert!(DEVICE_CAPABILITIES_UPLOAD.username_change_sync_message);
    assert!(!DEVICE_CAPABILITIES_UPLOAD.optional_phone_number);
};

/// `engine.protocolCapabilities` 应答体：只读、零网络、账号无关，未绑定
/// 账号照常应答。能力位 JSON 形状 = vendor serde 属性输出（camelCase；
/// `profiles_v2` 按上游字面保持 snake——线上声明即此拼写，消费方不得
/// "纠正"），键值即引擎向服务器声明的位。
pub(crate) fn protocol_capabilities_payload() -> Value {
    json!({
        "deviceCapabilities": vendor_json(&DEVICE_CAPABILITIES),
        "deviceCapabilitiesUpload": vendor_json(&DEVICE_CAPABILITIES_UPLOAD),
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

/// 设备面注册上传体（engine.deviceCapabilities 回读面）：wire 逐键同
/// 官方 CapabilitiesUploadType serde 输出，位值 = DEVICE_CAPABILITIES_
/// UPLOAD 钉定镜像（PUT body 与此同形）。
pub(crate) fn device_capabilities_upload_payload() -> Value {
    vendor_json(&DEVICE_CAPABILITIES_UPLOAD)
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
        // 附件回填闭环批（2026-10-18）：账号属性面翻 true 后 link 面
        // （vendored LinkCapabilities::default() 恒 false）与本位刻意分叉——
        // 官方 link 面语义 = 链接时协商位，vendor Default 改动走 vendor 台账，
        // 本镜像只钉「link 面不受账号面翻位牵连」。
        assert_ne!(
            link.attachment_backfill,
            DEVICE_CAPABILITIES.attachment_backfill
        );
        assert!(
            !link.attachment_backfill,
            "link 面翻位须走 vendor README 台账并同步本断言"
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
                "versioned_expiration_timer: false,",
                "optional_phone_number: false,",
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
            [
                "baseline",
                "deviceCapabilities",
                "deviceCapabilitiesUpload",
                "linkCapabilities"
            ]
        );

        let device = obj["deviceCapabilities"].as_object().expect("object");
        assert_eq!(
            device.keys().collect::<Vec<_>>().as_slice(),
            [
                "attachmentBackfill",
                "optionalPhoneNumber",
                "profiles_v2",
                "spqr",
                "storage",
                "transfer",
                "usernameChangeSyncMessage",
                "versionedExpirationTimer"
            ]
        );
        assert_eq!(device["spqr"], json!(true));
        assert_eq!(device["storage"], json!(true));
        assert_eq!(device["transfer"], json!(false));
        // 附件回填闭环批（2026-10-18）：已实现 backfill（请求发送面 +
        // 响应收投影臂）按官方桌面口径恒 true。
        assert_eq!(device["attachmentBackfill"], json!(true));
        assert_eq!(device["profiles_v2"], json!(false));
        assert_eq!(device["usernameChangeSyncMessage"], json!(true));
        assert_eq!(device["versionedExpirationTimer"], json!(false));
        assert_eq!(device["optionalPhoneNumber"], json!(false));

        // 设备面注册位（官方 CapabilitiesUploadType 四键，wire 逐键同形）。
        let upload = obj["deviceCapabilitiesUpload"].as_object().expect("object");
        assert_eq!(
            upload.keys().collect::<Vec<_>>().as_slice(),
            [
                "attachmentBackfill",
                "optionalPhoneNumber",
                "spqr",
                "usernameChangeSyncMessage"
            ]
        );
        assert_eq!(upload["spqr"], json!(true));
        assert_eq!(upload["usernameChangeSyncMessage"], json!(true));
        assert_eq!(upload["attachmentBackfill"], json!(true));
        assert_eq!(upload["optionalPhoneNumber"], json!(false));

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
            "96f5e925c45b2d7a1c09afd0aee44733a2c3e671"
        );
        assert_eq!(env!("KT_ENGINE_LIBSIGNAL_VERSION"), "0.99.0");
        assert_eq!(
            env!("KT_ENGINE_LIBSIGNAL_COMMIT"),
            "439f035d9cfd7efe5435cbea0294591c9ed17232"
        );
    }
}
