// SPDX-License-Identifier: AGPL-3.0-only

//! 群终止（group termination）引擎面（KT 扩展方法 terminateGroup）。
//!
//! 官方行为锚（Signal-Desktop 行为对齐，不抄码）：
//! - 协议载体 = GroupChange.Actions 特殊 action `terminate_group = 28`
//!   （`TerminateGroupAction {}`，Groups.proto 注释 change epoch = 7 = 官方
//!   SUPPORTED_CHANGE_EPOCH）；version = 当前 revision + 1（官方
//!   buildTerminateChange）。
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
//! terminated change 为准登记（最小面，其余群更新消息本批继续跳过，
//! compat §5「群终止扩面批」登记边界）。

use std::collections::HashSet;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::groups_v2::{decrypt_group, GroupOperations};
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::protocol::{Aci, Pni};
use presage::libsignal_service::push_service::HttpAuthOverride;
use presage::libsignal_service::zkgroup::groups::{GroupMasterKey, GroupSecretParams};
use presage::libsignal_service::zkgroup::Timestamp;
use prost::Message;

use super::storage_sync::StorageSyncError;
use super::StorageContext;

/// change epoch 7 = 官方 SUPPORTED_CHANGE_EPOCH（groups.preload.ts:260，
/// "support for GroupTerminateChangeUpdate"）；terminate_group=28 的最低
/// 支持纪元。上送 change 不携带 epoch（服务器定），收侧按该纪元起接受。
pub(crate) const SUPPORTED_CHANGE_EPOCH: u32 = 7;

/// terminateGroup 错误面：UnknownAccount 折叠 -32603（未注册账号，细节不
/// 外泄）；UnknownGroup 折叠 -32603（群不存在/坏 groupId，参数面）；Failed
/// 折叠 `-1` + `GROUP_TERMINATE_FAILED` + data.status = 服务器状态码
/// （官方错误语义诚实透传：403/401 = 非 admin/凭据无效，409 = revision
/// 竞争需重拉群态，400 = 请求形状被服务器拒）；Unavailable 折叠 `-1` +
/// `STORAGE_UNAVAILABLE`（网络面，STORAGE 族同款 retryable 语义）。
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

/// 群证书兑换窗口（官方 current_days_seconds 同语义）：今天 UTC 零点起的
/// epoch 秒与 +7 天；按天整除换算（UTC 日界即 86400 整除，无历法面）。
fn redemption_window(now_epoch_secs: u64) -> (u64, u64) {
    let day = now_epoch_secs / 86_400;
    (day * 86_400, (day + 7) * 86_400)
}

/// 群凭据响应（官方 CredentialResponse camelCase 同形状）。
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupCredentialsResponse {
    credentials: Vec<TemporalCredential>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TemporalCredential {
    credential: String,
    redemption_time: u64,
}

/// 群凭据派生（官方 get_authorization_for_today 同语义链）：本机凭据 GET
/// 群证书 → 当天条目 → zkgroup receive + present → HttpAuth{username =
/// GroupPublicParams hex（bincode），password = presentation hex}。群凭据
/// 不入日志（字段不进任何投影/日志）。
async fn group_authorization(
    context: &StorageContext,
    master_key: &[u8],
) -> Result<presage::libsignal_service::push_service::HttpAuth, TerminateError> {
    use base64::Engine as _;

    let key: [u8; 32] = master_key
        .try_into()
        .map_err(|_| TerminateError::UnknownGroup)?;
    let group_secret_params = GroupSecretParams::derive_from_master_key(GroupMasterKey::new(key));

    let (today, today_plus_7_days) = redemption_window(now_epoch_secs());
    let path = format!(
        "/v1/certificate/auth/group?redemptionStartSeconds={today}&redemptionEndSeconds={today_plus_7_days}&pniAsServiceId=true"
    );
    let response = context
        .push
        .request(
            reqwest::Method::GET,
            Endpoint::service(path),
            // 本机凭据 basic auth（官方 identified 通道）。
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| TerminateError::Unavailable)?
        .send()
        .await
        .map_err(|_| TerminateError::Unavailable)?;
    if !(200..300).contains(&response.status().as_u16()) {
        return Err(TerminateError::Failed {
            status: Some(response.status().as_u16()),
        });
    }
    let credentials: GroupCredentialsResponse = response
        .json()
        .await
        .map_err(|_| TerminateError::Failed { status: None })?;
    // 当天条目（服务器按兑换窗口回填，缺席 = 凭据面异常）。
    let entry = credentials
        .credentials
        .iter()
        .find(|entry| entry.redemption_time == today)
        .ok_or(TerminateError::Failed { status: None })?;
    let response_bytes = base64::engine::general_purpose::STANDARD
        .decode(&entry.credential)
        .map_err(|_| TerminateError::Failed { status: None })?;
    let auth_credential_response =
        presage::libsignal_service::zkgroup::auth::AuthCredentialWithPniResponse::new(
            &response_bytes,
        )
        .map_err(|_| TerminateError::Failed { status: None })?;

    let server_public_params = context.zkgroup_server_public_params();
    let service_ids = context.service_ids();
    let auth_credential = auth_credential_response
        .receive(
            &server_public_params,
            Aci::from_uuid_bytes(service_ids.aci.into_bytes()),
            Pni::from_uuid_bytes(service_ids.pni.into_bytes()),
            Timestamp::from_epoch_seconds(today),
        )
        .map_err(|_| TerminateError::Failed { status: None })?;

    let mut random_bytes = [0u8; 32];
    use rand::RngCore as _;
    rand::rng().fill_bytes(&mut random_bytes);
    let presentation = presage::libsignal_service::zkgroup::serialize(&auth_credential.present(
        &server_public_params,
        &group_secret_params,
        random_bytes,
    ));

    Ok(presage::libsignal_service::push_service::HttpAuth {
        // zkgroup bincode 序列化（simpleapi.rs GroupSecretParams_
        // getPublicParams 同构，官方凭据面同款）。
        username: super::hex_encode(
            &bincode::serialize(&group_secret_params.get_public_params())
                .map_err(|_| TerminateError::UnknownGroup)?,
        ),
        password: super::hex_encode(&presentation),
    })
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
    /// configuration.rs 同源常量字节，bincode 反序列化）。
    pub(crate) fn zkgroup_server_public_params(
        &self,
    ) -> presage::libsignal_service::zkgroup::ServerPublicParams {
        use presage::libsignal_service::configuration::{ServiceConfiguration, SignalServers};
        let config: ServiceConfiguration = SignalServers::Production.into();
        config.zkgroup_server_public_params
    }
}

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
/// terminated——presage Group 模型丢位由本函数绕行）。网络失败 → Unavailable；
/// 服务器拒绝 → Failed{status}；解密失败 → Failed{None}。
async fn fetch_group_state(
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// 群证书兑换窗口（官方 current_days_seconds 同语义）：UTC 天对齐整除，
    /// 窗口 7 天（chrono dev-dep 计算固定日期的日界，不发明魔数）。
    #[test]
    fn redemption_window_is_day_aligned_seven_days() {
        // 2026-10-06T12:00:00Z 当天中段时刻。
        let noon = chrono::DateTime::parse_from_rfc3339("2026-10-06T12:00:00Z")
            .expect("parses")
            .timestamp() as u64;
        let (start, end) = redemption_window(noon);
        assert_eq!(start % 86_400, 0, "日界零点对齐");
        assert_eq!(end - start, 7 * 86_400, "窗口 7 天");
        assert_eq!(noon / 86_400, start / 86_400, "当天中段归当天零点");
        // 整零点入参同样对齐本天。
        let (start2, _) = redemption_window(start);
        assert_eq!(start2, start);
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
