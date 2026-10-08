// SPDX-License-Identifier: AGPL-3.0-only

//! 群管理写面（KT 扩展方法）：群终止（terminateGroup）+ 群消失计时器
//!（updateGroup {expiration}，KT 扩展批）。
//!
//! 官方行为锚（Signal-Desktop 行为对齐，不抄码）：
//! - 协议载体 = GroupChange.Actions 特殊 action `terminate_group = 28`
//!   （`TerminateGroupAction {}`，Groups.proto 注释 change epoch = 7 = 官方
//!   SUPPORTED_CHANGE_EPOCH）；version = 当前 revision + 1（官方
//!   buildTerminateChange）。
//! - 群 timer 变更 = `modify_disappearing_message_timer = 12`
//!   （`ModifyDisappearingMessageTimerAction { timer = 1 }`，字段编号以本仓
//!   钉定 libsignal-service-rs rev 9e6c08b 的 protobuf/Groups.proto 实查为
//!   准，与官方 storageservice Groups.proto 同源同号）；timer = 加密的
//!   `GroupAttributeBlob { disappearingMessagesDuration = 3 (uint32 秒,
//!   0 = 关) }`（官方 buildDisappearingMessagesTimerChange 同形状：
//!   encrypt_blob_with_padding(randomness, blob, 0)，桌面 encryptGroupBlob
//!   同构）；admin 权限由服务器裁决；变更后群 revision+1，成员经群更新
//!   消息收到新 timer。
//! - 上传 = `PATCH v2/groups`（storage 服务 host，body =
//!   GroupChange.Actions protobuf，群凭据 basic auth
//!   `groupPublicParamsHex:authCredentialPresentationHex`，官方 modifyGroup
//!   同端点同形状）。
//! - 群凭据链（官方 GroupsManager.get_authorization_for_today 同语义）：
//!   `GET v1/certificate/auth/group`（本机凭据）取群证书 → zkgroup
//!   receive/present 出 presentation。钉定栈 GroupsManager 绑定
//!   SignalWebSocket（本引擎不开常驻群 ws），凭据链以 zkgroup 公共 API
//!   自建等价实现（端点/参数/派生形状一致，上游补独立凭据面后可替换）。
//! - 服务器按 GV2 权限校验（仅群 admin 可终止）；409 = revision 竞争（官方
//!   modifyGroupV2 冲突路径先拉最新群态再重试）。
//! - 成员侧：群态 `terminated=true`（官方 extractDiffs 只许 false→true）；
//!   终止后发送面硬拒（官方 shouldSendToConversation `terminated` 位）。
//!
//! 上游缺口：libsignal-service-rs 无群 change 上传 API（只有群读面形状）——
//! PATCH 上传为引擎自建薄封装（单点 `patch_group`，put_write_operation 的
//! request 复用模式 + 群 HttpAuth），端点/鉴权/body 与官方 WebAPI 同形状，
//! 上游补齐后整段替换。
//!
//! presage `model::groups::Group` 无 terminated 字段（有损投影丢弃该位），
//! 读侧走 libsignal-service `decrypt_group`（投影 terminated）+ 本模块
//! terminated 登记表（进程内存）双通道；receive 投影以群更新消息内携带的
//! terminated / expiration_timer change 为准（最小面，其余群更新消息本批
//! 继续跳过，compat §5「群终止扩面批」「群 expiration 批」登记边界）。

use std::collections::HashSet;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::groups_v2::{decrypt_group, GroupOperations};
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::libsignal_service::zkgroup::groups::{GroupMasterKey, GroupSecretParams};
use prost::Message;

use super::storage_sync::StorageSyncError;
use super::StorageContext;

/// change epoch 7 = 官方 SUPPORTED_CHANGE_EPOCH（groups.preload.ts:260，
/// "support for GroupTerminateChangeUpdate"）；terminate_group=28 的最低
/// 支持纪元。上送 change 不携带 epoch（服务器定），收侧按该纪元起接受。
pub(crate) const SUPPORTED_CHANGE_EPOCH: u32 = 7;

/// 群写面错误族（terminateGroup + updateGroup 共用）：UnknownAccount 折叠
/// -32603（未注册账号，细节不外泄）；UnknownGroup 折叠 -32603（群不存在/
/// 坏 groupId，参数面）；Failed 折叠 `-1` + 结构化码位（terminateGroup =
/// `GROUP_TERMINATE_FAILED`、updateGroup = `GROUP_UPDATE_FAILED`）+
/// data.status = 服务器状态码（官方错误语义诚实透传：403/401 = 非
/// admin/凭据无效，409 = revision 竞争需重拉群态，400 = 请求形状被服务
/// 器拒）；Unavailable 折叠 `-1` + `STORAGE_UNAVAILABLE`（网络面，STORAGE
/// 族同款 retryable 语义）。
#[derive(Debug)]
pub(crate) enum TerminateError {
    UnknownAccount,
    UnknownGroup,
    Failed { status: Option<u16> },
    Unavailable,
}

impl From<StorageSyncError> for TerminateError {
    fn from(value: StorageSyncError) -> Self {
        match value {
            StorageSyncError::UnknownAccount => TerminateError::UnknownAccount,
            _ => TerminateError::Unavailable,
        }
    }
}

/// 群终止后的进程内存登记（receive 投影 → send 面拦截的会话内通道；重启
/// 后由服务器群态读面兜底，见 `group_state`）。
pub(crate) struct TerminatedGroups(std::sync::Mutex<HashSet<String>>);

impl TerminatedGroups {
    pub(crate) fn new() -> Self {
        Self(std::sync::Mutex::new(HashSet::new()))
    }

    pub(crate) fn register(&self, group_id: &str) {
        self.0
            .lock()
            .expect("terminated groups lock")
            .insert(group_id.to_owned());
    }

    pub(crate) fn contains(&self, group_id: &str) -> bool {
        self.0
            .lock()
            .expect("terminated groups lock")
            .contains(group_id)
    }
}

impl Default for TerminatedGroups {
    fn default() -> Self {
        Self::new()
    }
}

/// 群凭据派生（官方 get_authorization_for_today 同语义链）：双族凭据缓存
/// 供给（call_links::group_authorization：ensure_credentials 按需补拉，官方
/// getDatesForRequest 窗口逐字对位 today→+6d——旧实现 today→+7d 请求窗偏差
/// 随 P4c 切一修正并记档 compat §5；端点参数对位官方 legacy 族形状
/// zkcCredential=true 一次拉双族，`v101=true` 为 libsignal ≥0.101 Standard
/// 族开关不移植）→ 当日 legacy 条目 present → HttpAuth{
/// username = GroupPublicParams hex（bincode），password = presentation hex}。
/// 群凭据不入日志（字段不进任何投影/日志）。
async fn group_authorization(
    context: &StorageContext,
    master_key: &[u8],
) -> Result<presage::libsignal_service::push_service::HttpAuth, TerminateError> {
    let key: [u8; 32] = master_key
        .try_into()
        .map_err(|_| TerminateError::UnknownGroup)?;
    let group_secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));
    Ok(super::call_links::group_authorization(context, &group_secret_params).await?)
}

impl StorageContext {
    /// 本机 ACI/PNI（群凭据 present 所需；凭据面不入日志）。
    pub(crate) fn service_ids(&self) -> presage::libsignal_service::push_service::ServiceIds {
        presage::libsignal_service::push_service::ServiceIds {
            aci: self.aci_uuid,
            pni: self.pni_uuid,
        }
    }

    /// zkgroup 服务器公共参数（Production 固定，PushService 同参；官方
    /// configuration.rs 同源常量字节，bincode 反序列化）。实现提升为
    /// `zkgroup_consts::legacy_server_public_params` 单一真相源（P4c 切一）。
    pub(crate) fn zkgroup_server_public_params(
        &self,
    ) -> presage::libsignal_service::zkgroup::ServerPublicParams {
        super::zkgroup_consts::legacy_server_public_params()
    }
}

/// 群终止 change 编码（纯函数，单测钉定形状）：version = revision + 1（官方
/// buildTerminateChange 同构），source_user_id/group_id 置空——sourceUserId
/// 服务器回填（官方 toGroupActionsParams 缺省 null 同语义；groupId 上送置
/// 空由服务器按鉴权群定界），空 TerminateGroupAction 的在场即语义（proto3）。
pub(crate) fn build_terminate_change(revision: u32) -> presage::proto::group_change::Actions {
    presage::proto::group_change::Actions {
        source_user_id: Vec::new(),
        group_id: Vec::new(),
        version: revision.saturating_add(1),
        terminate_group: Some(presage::proto::group_change::actions::TerminateGroupAction {}),
        ..presage::proto::group_change::Actions::default()
    }
}

/// 群消失计时器 change 编码（纯函数，单测钉定形状）：官方
/// buildDisappearingMessagesTimerChange 同构——version = revision + 1
///（饱和不回卷），`modify_disappearing_message_timer.timer` = 加密
/// `GroupAttributeBlob { disappearingMessagesDuration }`（uint32 秒，
/// 0 = 关闭；官方 encryptGroupBlob 同构即
/// `encrypt_blob_with_padding(randomness, blob, 0)`，加密在此函数内完成，
/// 密钥面不外泄），source_user_id/group_id 置空（terminate 面同纪律：
/// sourceUserId 服务器回填、groupId 由服务器按鉴权群定界），其余 action
/// 全部缺席。
pub(crate) fn build_expiration_change(
    secret_params: &GroupSecretParams,
    revision: u32,
    expiration_secs: u32,
    randomness: [u8; 32],
) -> presage::proto::group_change::Actions {
    let blob = presage::proto::GroupAttributeBlob {
        content: Some(
            presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(
                expiration_secs,
            ),
        ),
    }
    .encode_to_vec();
    presage::proto::group_change::Actions {
        source_user_id: Vec::new(),
        group_id: Vec::new(),
        version: revision.saturating_add(1),
        modify_disappearing_message_timer: Some(
            presage::proto::group_change::actions::ModifyDisappearingMessageTimerAction {
                timer: secret_params.encrypt_blob_with_padding(randomness, &blob, 0),
            },
        ),
        ..presage::proto::group_change::Actions::default()
    }
}

/// `PATCH v2/groups` 群 change 上传（上游缺口单点，官方 modifyGroup 同形状）：
/// storage 服务 host、body = Actions protobuf、群凭据 basic auth；2xx = 成功
///（响应 GroupChangeResponse 不消费——终止后本机群态以登记表 + 服务器读面
/// 为准）；4xx/5xx → Failed{status}（官方语义诚实透传）；传输层失败 →
/// Unavailable（retryable 面）。
pub(crate) async fn patch_group(
    push: &PushService,
    auth: presage::libsignal_service::push_service::HttpAuth,
    actions: &presage::proto::group_change::Actions,
) -> Result<(), TerminateError> {
    let response = push
        .request(
            reqwest::Method::PATCH,
            Endpoint::storage("v2/groups"),
            HttpAuthOverride::Identified(auth),
        )
        .map_err(|_| TerminateError::Unavailable)?
        .header("Content-Type", "application/x-protobuf")
        .body(actions.encode_to_vec())
        .send()
        .await
        .map_err(|_| TerminateError::Unavailable)?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(TerminateError::Failed {
            status: Some(status),
        })
    }
}

/// 服务器群态拉取 + 解密（群读面：`GET v1/groups/` + decrypt_group 投影
/// terminated——presage Group 模型丢位由本函数绕行；P4c 切一起 calls.
/// groupMembers 材料面复用本函数）。网络失败 → Unavailable；服务器拒绝 →
/// Failed{status}；解密失败 → Failed{None}。
pub(crate) async fn fetch_group_state(
    context: &StorageContext,
    master_key: &[u8],
) -> Result<presage::libsignal_service::groups_v2::Group, TerminateError> {
    let authorization = group_authorization(context, master_key).await?;
    let response = context
        .push
        .request(
            reqwest::Method::GET,
            Endpoint::storage("v1/groups/"),
            HttpAuthOverride::Identified(authorization),
        )
        .map_err(|_| TerminateError::Unavailable)?
        .send()
        .await
        .map_err(|_| TerminateError::Unavailable)?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(TerminateError::Failed {
            status: Some(status),
        });
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|_| TerminateError::Unavailable)?;
    let encrypted = presage::proto::Group::decode(bytes.as_ref())
        .map_err(|_| TerminateError::Failed { status: None })?;
    decrypt_group(master_key, encrypted).map_err(|_| TerminateError::Failed { status: None })
}

/// 发送面群态探针（真机上 send 前判定）：读面失败一律 None（不阻塞发送，
/// 终止真值以登记表 + 服务器裁决为准），只有成功解密且 terminated=true
/// 才返回 Some(true)。
pub(crate) async fn fetch_group_state_for_send(
    context: &StorageContext,
    master_key: &[u8],
) -> Option<bool> {
    fetch_group_state(context, master_key)
        .await
        .ok()
        .map(|group| group.terminated)
}

/// terminateGroup 编排：幂等判定（登记表 → 服务器群态 terminated 真值，
/// 同值幂等纪律对齐 setConversationBlocked 先例）→ 群凭据 → PATCH 上传 →
/// 本地登记。服务器群态不可读（网络/拒绝）不阻塞写面——写意图交服务器
/// 裁决（revision 退化 0，官方对未知 revision 的 buildTerminateChange
/// (group.revision || 0) + 1 同口径）。
pub(crate) async fn terminate_group(
    context: &StorageContext,
    registry: &TerminatedGroups,
    master_key: &[u8],
) -> Result<(), TerminateError> {
    use base64::Engine as _;
    let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
    let mut terminated = registry.contains(&group_id);
    let mut revision = 0u32;
    if !terminated {
        match fetch_group_state(context, master_key).await {
            Ok(group) => {
                terminated = group.terminated;
                revision = group.version;
            }
            // 群读面网络失败不阻塞写面（交服务器裁决；Unavailable 仅在
            // 凭据/上传面不可达时上抛）。
            Err(TerminateError::Unavailable) => {}
            Err(error) => return Err(error),
        }
    }
    if terminated {
        registry.register(&group_id);
        return Ok(());
    }
    let authorization = group_authorization(context, master_key).await?;
    patch_group(
        &context.push,
        authorization,
        &build_terminate_change(revision),
    )
    .await?;
    registry.register(&group_id);
    Ok(())
}

/// updateGroup {expiration} 编排：群态读面取当前 revision（网络失败退化 0
/// 继续写，terminate 面同口径；其余读面错误上抛——坏群在服务器侧裁决）→
/// 群凭据 → PATCH 上传 expiration change → 成功。幂等不做同值折叠：群
/// revision 每次变更 +1 是官方写面语义（revision 即变化本身），timer 同值
/// 再写同样推进 revision 并广播，交服务器与成员侧收敛。
pub(crate) async fn update_group_expiration(
    context: &StorageContext,
    master_key: &[u8],
    expiration_secs: u32,
) -> Result<(), TerminateError> {
    use rand::RngCore as _;
    let revision = match fetch_group_state(context, master_key).await {
        Ok(group) => group.version,
        Err(TerminateError::Unavailable) => 0,
        Err(error) => return Err(error),
    };
    let mut randomness = [0u8; 32];
    rand::rng().fill_bytes(&mut randomness);
    let secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(
        master_key
            .try_into()
            .map_err(|_| TerminateError::UnknownGroup)?,
    ));
    let authorization = group_authorization(context, master_key).await?;
    patch_group(
        &context.push,
        authorization,
        &build_expiration_change(&secret_params, revision, expiration_secs, randomness),
    )
    .await
}

/// receive 投影面：群更新消息内携带 terminated change → `group_terminated`
/// 事件投影（groupId = master key 标准 base64、terminator = 编辑者 ACI 串）。
pub(crate) struct TerminateProjection {
    pub(crate) group_id: String,
    pub(crate) terminator: Option<String>,
}

/// 解密 GroupContextV2.group_change 密文并提取 terminated 位 + 编辑者 ACI
///（libsignal-service `GroupOperations::decrypt_group_change`，zkgroup 解密
/// 公共 API）。非 terminated change / 缺密文 / 解密失败 → None（其余群更新
/// 消息本批继续跳过，宁缺勿错）。
pub(crate) fn project_terminate_from_context(
    master_key: &[u8],
    group_change: Option<&[u8]>,
) -> Option<TerminateProjection> {
    use base64::Engine as _;
    use presage::libsignal_service::groups_v2::GroupChange;

    let group_change = group_change?;
    let key: [u8; 32] = master_key.try_into().ok()?;
    let secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));
    let encrypted = presage::proto::GroupChange::decode(group_change).ok()?;
    let changes = GroupOperations::new(secret_params)
        .decrypt_group_change(encrypted)
        .ok()?;
    // change epoch 闸（官方 isChangeEpochSupported 同语义：epoch > 客户端
    // 支持面的 change 含未知语义，不处理）——终止事件只认 epoch ≤ 7。
    if changes.change_epoch > SUPPORTED_CHANGE_EPOCH {
        return None;
    }
    if !changes
        .changes
        .iter()
        .any(|change| matches!(change, GroupChange::TerminateGroup))
    {
        return None;
    }
    let terminator = changes.editor.service_id_string();
    Some(TerminateProjection {
        group_id: base64::engine::general_purpose::STANDARD.encode(master_key),
        terminator: (!terminator.is_empty()).then_some(terminator),
    })
}

/// receive 投影面：群更新消息内携带 expiration_timer change → `groupUpdated`
/// 事件投影（groupId = master key 标准 base64、revision = change version、
/// editor = 编辑者 ACI 串、expireTimerSeconds = 新 timer 秒数，0 = 关闭）。
pub(crate) struct ExpirationProjection {
    pub(crate) group_id: String,
    pub(crate) revision: u32,
    pub(crate) expire_timer_seconds: u32,
    pub(crate) editor: Option<String>,
}

/// 解密 GroupContextV2.group_change 密文并提取 expiration_timer 位 + 编辑者
/// ACI（libsignal-service `GroupOperations::decrypt_group_change`，zkgroup
/// 解密公共 API）。非 expiration change / 缺密文 / 解密失败 → None（其余群
/// 更新消息继续跳过，宁缺勿错；全投影另行立项）。`GroupChange::Timer`
/// 的 `Option<Timer>` 由解密侧归一（明文 duration 0 → None = 关闭），两端
/// 同语义：投影键恒在，0 = 关闭。
pub(crate) fn project_expiration_from_context(
    master_key: &[u8],
    group_change: Option<&[u8]>,
) -> Option<ExpirationProjection> {
    use base64::Engine as _;
    use presage::libsignal_service::groups_v2::GroupChange;

    let group_change = group_change?;
    let key: [u8; 32] = master_key.try_into().ok()?;
    let secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));
    let encrypted = presage::proto::GroupChange::decode(group_change).ok()?;
    let changes = GroupOperations::new(secret_params)
        .decrypt_group_change(encrypted)
        .ok()?;
    // change epoch 闸（官方 isChangeEpochSupported 同语义）：epoch > 7 的
    // change 含未知语义，不处理。
    if changes.change_epoch > SUPPORTED_CHANGE_EPOCH {
        return None;
    }
    let expiration = changes.changes.iter().find_map(|change| match change {
        GroupChange::Timer(timer) => Some(timer.as_ref().map(|timer| timer.duration).unwrap_or(0)),
        _ => None,
    })?;
    let editor = changes.editor.service_id_string();
    Some(ExpirationProjection {
        group_id: base64::engine::general_purpose::STANDARD.encode(master_key),
        revision: changes.version,
        expire_timer_seconds: expiration,
        editor: (!editor.is_empty()).then_some(editor),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use presage::libsignal_service::protocol::Aci;

    /// 官方 buildTerminateChange 同构：version = revision + 1、只有
    /// terminate_group 一个 action、source_user_id/group_id 置空（服务器
    /// 回填）。revision 饱和加法不回卷。
    #[test]
    fn terminate_change_version_bumps_once_and_carries_only_terminate() {
        let actions = build_terminate_change(41);
        assert_eq!(actions.version, 42);
        assert!(actions.terminate_group.is_some(), "terminate action 在场");
        assert!(actions.source_user_id.is_empty());
        assert!(actions.group_id.is_empty());
        // 官方 toGroupActionsParams 形状：其余 action 全部缺席。
        assert!(actions.add_members.is_empty());
        assert!(actions.delete_members.is_empty());
        assert!(actions.modify_member_roles.is_empty());
        assert!(actions.modify_member_profile_keys.is_empty());
        assert!(actions.add_members_pending_profile_key.is_empty());
        assert!(actions.delete_members_pending_profile_key.is_empty());
        assert!(actions.promote_members_pending_profile_key.is_empty());
        assert!(actions.modify_title.is_none());
        assert!(actions.modify_avatar.is_none());
        assert!(actions.modify_disappearing_message_timer.is_none());
        assert!(actions.modify_attributes_access.is_none());
        assert!(actions.modify_member_access.is_none());
        assert!(actions.modify_add_from_invite_link_access.is_none());
        assert!(actions.modify_member_label_access.is_none());
        assert!(actions.add_members_pending_admin_approval.is_empty());
        assert!(actions.delete_members_pending_admin_approval.is_empty());
        assert!(actions.promote_members_pending_admin_approval.is_empty());
        assert!(actions.modify_invite_link_password.is_none());
        assert!(actions.modify_description.is_none());
        assert!(actions.modify_announcements_only.is_none());
        assert!(actions.add_members_banned.is_empty());
        assert!(actions.delete_members_banned.is_empty());
        assert!(actions
            .promote_members_pending_pni_aci_profile_key
            .is_empty());
        assert!(actions.modify_member_labels.is_empty());

        // revision 0 → version 1（未知 revision 面也合法）；u32::MAX 饱和不
        // 回卷。
        assert_eq!(build_terminate_change(0).version, 1);
        assert_eq!(build_terminate_change(u32::MAX).version, u32::MAX);
    }

    /// Actions 线格式（官方 modifyGroup body 形状）：prost 编码只含 version
    /// (field 2 varint) 与 terminate_group (field 28 len-delim，空 message)
    /// 的字段头；解码回读保真。
    #[test]
    fn terminate_actions_wire_shape_encodes_version_and_terminate_only() {
        // version = revision + 1：revision 7 → version 8。
        let actions = build_terminate_change(7);
        assert_eq!(actions.version, 8);
        let bytes = actions.encode_to_vec();
        // version=8 varint: tag 0x10 (field 2, varint) + 0x08；terminate_group:
        // tag 0xe2 0x01 (field 28, wire type 2) + len 0x00（空 message）。
        assert_eq!(bytes, vec![0x10, 0x08, 0xe2, 0x01, 0x00]);

        let round =
            presage::proto::group_change::Actions::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(round.version, 8);
        assert!(round.terminate_group.is_some());
    }

    /// epoch 7 依据面：常量与官方 SUPPORTED_CHANGE_EPOCH 对齐（Groups.proto
    /// terminate_group=28 的 change epoch 注释同源）。
    #[test]
    fn supported_change_epoch_matches_official() {
        assert_eq!(SUPPORTED_CHANGE_EPOCH, 7);
    }

    /// 群消失计时器 change（updateGroup expiration 批）：version = revision
    /// +1、只有 modify_disappearing_message_timer 一个 action、blob 解密回
    /// 读 = disappearingMessagesDuration（0 = 关闭语义不吞）、其余 action
    /// 全部缺席（官方 toGroupActionsParams 形状）。
    #[test]
    fn expiration_change_carries_only_timer_action() {
        let master_key = [0x44u8; 32];
        let secret_params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key));
        let mut randomness = [0u8; 32];
        use rand::RngCore as _;
        rand::rng().fill_bytes(&mut randomness);

        let actions = build_expiration_change(&secret_params, 41, 86_400, randomness);
        assert_eq!(actions.version, 42);
        let timer = actions
            .modify_disappearing_message_timer
            .as_ref()
            .expect("timer action 在场")
            .timer
            .clone();
        assert!(!timer.is_empty(), "timer 为加密 blob 非空");
        // blob 解密回读：明文 = GroupAttributeBlob{disappearingMessagesDuration}。
        let plaintext = secret_params
            .decrypt_blob_with_padding(&timer)
            .expect("blob decrypts");
        let blob =
            presage::proto::GroupAttributeBlob::decode(plaintext.as_slice()).expect("blob decodes");
        assert_eq!(
            blob.content,
            Some(
                presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(86_400)
            )
        );
        assert!(actions.source_user_id.is_empty());
        assert!(actions.group_id.is_empty());
        assert!(actions.terminate_group.is_none());
        assert!(actions.add_members.is_empty());
        assert!(actions.delete_members.is_empty());
        assert!(actions.modify_member_roles.is_empty());
        assert!(actions.modify_member_profile_keys.is_empty());
        assert!(actions.add_members_pending_profile_key.is_empty());
        assert!(actions.delete_members_pending_profile_key.is_empty());
        assert!(actions.promote_members_pending_profile_key.is_empty());
        assert!(actions.modify_title.is_none());
        assert!(actions.modify_avatar.is_none());
        assert!(actions.modify_attributes_access.is_none());
        assert!(actions.modify_member_access.is_none());
        assert!(actions.modify_add_from_invite_link_access.is_none());
        assert!(actions.modify_member_label_access.is_none());
        assert!(actions.add_members_pending_admin_approval.is_empty());
        assert!(actions.delete_members_pending_admin_approval.is_empty());
        assert!(actions.promote_members_pending_admin_approval.is_empty());
        assert!(actions.modify_invite_link_password.is_none());
        assert!(actions.modify_description.is_none());
        assert!(actions.modify_announcements_only.is_none());
        assert!(actions.add_members_banned.is_empty());
        assert!(actions.delete_members_banned.is_empty());
        assert!(actions
            .promote_members_pending_pni_aci_profile_key
            .is_empty());
        assert!(actions.modify_member_labels.is_empty());

        // 0 = 关闭：明文 duration 0 原样入 blob（不吞不转 None——线面
        // truth 在 duration 数值，关闭语义由消费方按 0 解读，官方
        // buildDisappearingMessagesTimerChange 对 expireTimer=0 同形状）。
        let off = build_expiration_change(&secret_params, 3, 0, randomness);
        let plaintext = secret_params
            .decrypt_blob_with_padding(
                &off.modify_disappearing_message_timer
                    .as_ref()
                    .expect("timer action 在场")
                    .timer,
            )
            .expect("blob decrypts");
        let blob =
            presage::proto::GroupAttributeBlob::decode(plaintext.as_slice()).expect("blob decodes");
        assert_eq!(
            blob.content,
            Some(presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(0))
        );

        // revision 0 → version 1；u32::MAX 饱和不回卷。
        assert_eq!(
            build_expiration_change(&secret_params, 0, 60, randomness).version,
            1
        );
        assert_eq!(
            build_expiration_change(&secret_params, u32::MAX, 60, randomness).version,
            u32::MAX
        );
    }

    /// Actions 线格式（官方 modifyGroup body 形状，学 terminate 面断言
    /// 风格）：prost 编码只含 version (field 2 varint) 与
    /// modify_disappearing_message_timer (field 12 len-delim) 的字段头 +
    /// timer blob（field 1 len-delim）；解码回读保真。字段编号依据 =
    /// 本仓钉定 libsignal-service-rs rev 9e6c08b protobuf/Groups.proto
    /// 实查（modifyDisappearingMessageTimer = 12，官方 storageservice
    /// Groups.proto 同源同号）。
    #[test]
    fn expiration_actions_wire_shape_encodes_version_and_timer_only() {
        let master_key = [0x45u8; 32];
        let secret_params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key));
        let actions = build_expiration_change(&secret_params, 7, 60, [0x77u8; 32]);
        assert_eq!(actions.version, 8);
        let bytes = actions.encode_to_vec();
        // version=8 varint: tag 0x10 (field 2, varint) + 0x08；
        // modify_disappearing_message_timer: tag 0x62 (field 12, wire type 2)
        // + len + timer 子消息（tag 0x0a (field 1, wire type 2) + len +
        // 加密 blob）。首三字节钉死字段编号，长度按密文实长读回。
        assert_eq!(&bytes[0..2], &[0x10, 0x08], "version 字段头");
        assert_eq!(
            bytes[2], 0x62,
            "modify_disappearing_message_timer tag (field 12, wire type 2)"
        );
        let (timer_len, header) = {
            // varint 长度（blob 密文 < 128 字节恒单字节：AES-GCM-SIV =
            // 4B 填充长 + 3B 明文 + 16B tag + 12B nonce + 1B reserved = 36）。
            (bytes[3] as usize, 4usize)
        };
        let timer = &bytes[header..header + timer_len];
        assert_eq!(timer[0], 0x0a, "timer 子消息 field 1 tag");
        assert_eq!(timer[1] as usize, timer_len - 2, "timer 子消息长度自洽");
        assert_eq!(header + timer_len, bytes.len(), "无尾随字段");

        let round =
            presage::proto::group_change::Actions::decode(bytes.as_slice()).expect("decodes");
        assert_eq!(round.version, 8);
        assert!(round.terminate_group.is_none());
        let round_blob = round
            .modify_disappearing_message_timer
            .expect("timer 在场")
            .timer;
        let plaintext = secret_params
            .decrypt_blob_with_padding(&round_blob)
            .expect("blob decrypts");
        let blob =
            presage::proto::GroupAttributeBlob::decode(plaintext.as_slice()).expect("blob decodes");
        assert_eq!(
            blob.content,
            Some(presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(60))
        );
    }

    /// expiration 收投影（updateGroup expiration 批最小放开）：expiration
    /// change → 事件（groupId 标准 base64 + revision = change version +
    /// expireTimerSeconds 明文 + 编辑者 ACI）；非 expiration change /
    /// 缺密文 / 坏密文 / epoch 超面 → None（继续跳过面）。
    #[test]
    fn expiration_projection_faces() {
        use presage::libsignal_service::zkgroup;

        let master_key = [0x46u8; 32];
        use base64::Engine as _;
        let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
        let secret_params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key));
        let editor = Aci::parse_from_service_id_string("33333333-4444-4555-9666-777777777777")
            .expect("editor aci");

        let build_change = |version: u32, duration: u32| {
            let mut randomness = [0u8; 32];
            use rand::RngCore as _;
            rand::rng().fill_bytes(&mut randomness);
            let blob = presage::proto::GroupAttributeBlob {
                content: Some(
                    presage::proto::group_attribute_blob::Content::DisappearingMessagesDuration(
                        duration,
                    ),
                ),
            }
            .encode_to_vec();
            presage::proto::group_change::Actions {
                source_user_id: zkgroup::serialize(
                    &secret_params.encrypt_service_id(editor.into()),
                ),
                group_id: secret_params.get_group_identifier().to_vec(),
                version,
                modify_disappearing_message_timer: Some(
                    presage::proto::group_change::actions::ModifyDisappearingMessageTimerAction {
                        timer: secret_params.encrypt_blob_with_padding(randomness, &blob, 0),
                    },
                ),
                ..presage::proto::group_change::Actions::default()
            }
        };
        let encrypted = |actions: presage::proto::group_change::Actions, epoch: u32| {
            presage::proto::GroupChange {
                actions: actions.encode_to_vec(),
                server_signature: Vec::new(),
                change_epoch: epoch,
            }
        };

        // expiration change（timer = 86400）→ 事件投影。
        let projection = project_expiration_from_context(
            &master_key,
            Some(
                encrypted(build_change(5, 86_400), SUPPORTED_CHANGE_EPOCH)
                    .encode_to_vec()
                    .as_slice(),
            ),
        )
        .expect("expiration change projects");
        assert_eq!(projection.group_id, group_id);
        assert_eq!(projection.revision, 5);
        assert_eq!(projection.expire_timer_seconds, 86_400);
        assert_eq!(
            projection.editor.as_deref(),
            Some("33333333-4444-4555-9666-777777777777")
        );

        // timer = 0（关闭）→ 投影 0（关闭语义不吞；解密侧 0 duration 归一
        // None 后 unwrap_or(0) 还原，两端同语义）。
        let off = project_expiration_from_context(
            &master_key,
            Some(
                encrypted(build_change(6, 0), SUPPORTED_CHANGE_EPOCH)
                    .encode_to_vec()
                    .as_slice(),
            ),
        )
        .expect("timer-off change projects");
        assert_eq!(off.expire_timer_seconds, 0);
        assert_eq!(off.revision, 6);

        // 非 expiration change（改标题）→ None（继续跳过面）。
        let mut randomness = [0u8; 32];
        use rand::RngCore as _;
        rand::rng().fill_bytes(&mut randomness);
        let title_blob = presage::proto::GroupAttributeBlob {
            content: Some(presage::proto::group_attribute_blob::Content::Title(
                "renamed".to_owned(),
            )),
        }
        .encode_to_vec();
        let other = presage::proto::group_change::Actions {
            source_user_id: zkgroup::serialize(&secret_params.encrypt_service_id(editor.into())),
            group_id: secret_params.get_group_identifier().to_vec(),
            version: 7,
            modify_title: Some(presage::proto::group_change::actions::ModifyTitleAction {
                title: secret_params.encrypt_blob_with_padding(randomness, &title_blob, 0),
            }),
            ..presage::proto::group_change::Actions::default()
        };
        assert!(project_expiration_from_context(
            &master_key,
            Some(
                encrypted(other, SUPPORTED_CHANGE_EPOCH)
                    .encode_to_vec()
                    .as_slice()
            )
        )
        .is_none());

        // 缺 group_change 密文 / 坏密文 / master key 非法长度 → None。
        assert!(project_expiration_from_context(&master_key, None).is_none());
        assert!(project_expiration_from_context(&master_key, Some(&[0u8; 8])).is_none());
        assert!(project_expiration_from_context(&[0u8; 16], None).is_none());

        // epoch 闸：epoch 超支持面（>7）的 expiration change 不处理。
        assert!(project_expiration_from_context(
            &master_key,
            Some(
                encrypted(build_change(8, 3600), SUPPORTED_CHANGE_EPOCH + 1)
                    .encode_to_vec()
                    .as_slice()
            )
        )
        .is_none());
    }

    /// terminated 登记表：登记/判定/未登记 false（send 面拦截的会话内通道）。
    #[test]
    fn terminated_registry_roundtrip() {
        let registry = TerminatedGroups::new();
        assert!(!registry.contains("group-a"));
        registry.register("group-a");
        assert!(registry.contains("group-a"));
        assert!(!registry.contains("group-b"));
    }

    /// receive 投影：terminated change → 事件（groupId 标准 base64 +
    /// 编辑者 ACI）；非 terminated change / 缺密文 / 坏密文 → None（本批
    /// 继续跳过的最小面边界）。
    #[test]
    fn terminate_projection_faces() {
        use presage::libsignal_service::zkgroup;

        let master_key = [0x21u8; 32];
        use base64::Engine as _;
        let group_id = base64::engine::general_purpose::STANDARD.encode(master_key);
        let secret_params =
            GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key));
        let editor = Aci::parse_from_service_id_string("11111111-2222-4333-8444-555555555555")
            .expect("editor aci");

        // terminated change 密文（zkgroup 加密公共 API 构造；group_id = 32B
        // 群标识（decrypt_group_change 对其做 GroupV2Id 形状校验；真实线面
        // 该字段由服务器回填）。
        let actions = presage::proto::group_change::Actions {
            source_user_id: zkgroup::serialize(&secret_params.encrypt_service_id(editor.into())),
            group_id: secret_params.get_group_identifier().to_vec(),
            version: 5,
            terminate_group: Some(presage::proto::group_change::actions::TerminateGroupAction {}),
            ..presage::proto::group_change::Actions::default()
        };
        let encrypted_change = presage::proto::GroupChange {
            actions: actions.encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: SUPPORTED_CHANGE_EPOCH,
        };
        let projection = project_terminate_from_context(
            &master_key,
            Some(encrypted_change.encode_to_vec().as_slice()),
        )
        .expect("terminated change projects");
        assert_eq!(projection.group_id, group_id);
        assert_eq!(
            projection.terminator.as_deref(),
            Some("11111111-2222-4333-8444-555555555555")
        );

        // 非 terminated change（只改标题）→ None（继续跳过面）。
        let mut randomness = [0u8; 32];
        use rand::RngCore as _;
        rand::rng().fill_bytes(&mut randomness);
        let title_blob = presage::proto::GroupAttributeBlob {
            content: Some(presage::proto::group_attribute_blob::Content::Title(
                "renamed".to_owned(),
            )),
        }
        .encode_to_vec();
        let other = presage::proto::group_change::Actions {
            source_user_id: zkgroup::serialize(&secret_params.encrypt_service_id(editor.into())),
            group_id: secret_params.get_group_identifier().to_vec(),
            version: 6,
            modify_title: Some(presage::proto::group_change::actions::ModifyTitleAction {
                title: secret_params.encrypt_blob_with_padding(randomness, &title_blob, 0),
            }),
            ..presage::proto::group_change::Actions::default()
        };
        let encrypted_other = presage::proto::GroupChange {
            actions: other.encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: SUPPORTED_CHANGE_EPOCH,
        };
        assert!(project_terminate_from_context(
            &master_key,
            Some(encrypted_other.encode_to_vec().as_slice())
        )
        .is_none());

        // 缺 group_change 密文 / 坏密文 / master key 非法长度 → None（宁缺
        // 勿错）。
        assert!(project_terminate_from_context(&master_key, None).is_none());
        assert!(project_terminate_from_context(&master_key, Some(&[0u8; 8])).is_none());
        assert!(project_terminate_from_context(&[0u8; 16], None).is_none());

        // epoch 闸（官方 isChangeEpochSupported 同语义）：epoch 超支持面
        // （>7）的 terminated change 不处理（未知纪元语义，宁缺勿错）。
        let future_epoch = presage::proto::GroupChange {
            actions: actions.encode_to_vec(),
            server_signature: Vec::new(),
            change_epoch: SUPPORTED_CHANGE_EPOCH + 1,
        };
        assert!(project_terminate_from_context(
            &master_key,
            Some(future_epoch.encode_to_vec().as_slice())
        )
        .is_none());
        // epoch = 7（支持面边界）照常处理（上面首分支已覆盖）。
    }
}
