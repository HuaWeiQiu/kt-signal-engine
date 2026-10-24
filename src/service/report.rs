// SPDX-License-Identifier: AGPL-3.0-only

//! 举报垃圾消息（reportSpam，KT 扩展方法）。
//!
//! 官方行为锚（Signal-Desktop 行为对齐，不抄码）：REST `POST
//! v1/messages/report/{senderAci}/{serverGuid}`（WebAPI.preload.ts 路径表
//! reportMessage），JSON body `{token}`——token 可选（envelope
//! reportSpamToken，缺席不阻塞，官方 jsonData={token} 对 undefined 序列化
//! 为 `{}`）；批面 ≤1000 条/批（官方 reportSpamJobQueue zod max(1000)），
//! 逐条独立请求；serverGuid = 信封 metadata 公共位（服务器为每条消息分配
//! 的 UUID）。
//!
//! 部分失败语义（诚实部分成功面）：单条 4xx/5xx/网络失败记入 failures，
//! 全部失败才整体报错 `REPORT_SPAM_FAILED`。鉴权通道 = 本机凭据 basic auth
//!（put_profile / storage_auth 同一 PushService 构造先例）。
//!
//! 已知边界（compat §5 登记）：引擎 receive 投影不透 token——envelope 级
//! reportSpamToken 经 presage Content 转换丢弃（Metadata 仅携带
//! server_guid），token 由调用方可选透传。

use super::storage_sync::StorageSyncError;
use super::StorageContext;
use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::Uuid;
use presage::libsignal_service::push_service::HttpAuthOverride;

/// 官方 reportSpamJobQueue 单批上限（zod serverGuids.max(1000)）。
pub(crate) const REPORT_SPAM_MAX_GUIDS: usize = 1000;

/// reportSpam 错误面：UnknownAccount 折叠 -32603（未注册账号）；AllFailed
/// 折叠 `-1` + `REPORT_SPAM_FAILED`（全部 serverGuid 均失败，写意图未落地
/// 任何一条）。参数形状错在 methods.rs 解析面 fail-closed（-32603）。
#[derive(Debug)]
pub(crate) enum ReportSpamError {
    UnknownAccount,
    AllFailed,
}

/// 单条举报结果（诚实部分成功面）。
pub(crate) struct ReportFailure {
    pub(crate) server_guid: String,
    /// 服务器 HTTP 状态码；传输层失败（请求未达服务器）时 None（投影面
    /// 缺席键不在，不发明 0）。
    pub(crate) status: Option<u16>,
}

/// 举报批结果投影：成功条数 + 失败清单。
pub(crate) struct ReportOutcome {
    pub(crate) reported: usize,
    pub(crate) failures: Vec<ReportFailure>,
}

/// 举报 body（官方 jsonData={token} 同构）：token 缺省序列化为 `{}`。
#[derive(serde::Serialize)]
struct ReportBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
}

/// serverGuid 参数校验（URL 安全边界）：只收连字（36）/无连字（32）hex
/// 形态——`parse_str` 本身还接受 `urn:uuid:`/花括号等宽松形态，先做长度 +
/// 字符白名单预检再解析归一为小写连字形态入路径（引擎投影面 serverGuid
/// 即该形态；防路径注入 fail-closed）。
pub(crate) fn normalize_server_guid(server_guid: &str) -> Option<String> {
    let text = server_guid.trim();
    let shape_ok = (text.len() == 36 || text.len() == 32)
        && text.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    if !shape_ok {
        return None;
    }
    Uuid::parse_str(text).ok().map(|uuid| uuid.to_string())
}

/// 单条举报请求：`POST v1/messages/report/{aci}/{guid}`，body `{token}`。
/// 2xx = 成功；其余 = 失败（带状态码；传输层失败 status=None）。
async fn report_one(
    push: &presage::libsignal_service::prelude::PushService,
    sender_aci: &str,
    server_guid: &str,
    token: Option<&str>,
) -> Result<(), Option<u16>> {
    let path = format!("v1/messages/report/{sender_aci}/{server_guid}");
    let request = push
        .request(
            reqwest::Method::POST,
            Endpoint::service(&path),
            // 本机凭据 basic auth（官方 identified 通道；put_profile 同款）。
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| None)?;
    let response = request
        .json(&ReportBody {
            token: token.map(str::to_owned),
        })
        .send()
        .await
        .map_err(|_| None)?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(Some(response.status().as_u16()))
    }
}

/// reportSpam 编排：本机凭据 PushService → 逐条独立请求 → 部分失败收敛。
/// 全部失败 → AllFailed（调用方报整体错误）；至少一条成功 → 诚实投影
/// （reported + failures）。
pub(crate) async fn report_spam(
    context: &StorageContext,
    sender_aci: &presage::libsignal_service::protocol::ServiceId,
    server_guids: &[String],
    token: Option<&str>,
) -> Result<ReportOutcome, ReportSpamError> {
    let sender_aci = sender_aci.service_id_string();
    let mut reported = 0usize;
    let mut failures = Vec::new();
    for server_guid in server_guids {
        // 参数面已校验 UUID（methods.rs），此处归一形态双保险。
        let Some(server_guid) = normalize_server_guid(server_guid) else {
            failures.push(ReportFailure {
                server_guid: server_guid.clone(),
                status: None,
            });
            continue;
        };
        match report_one(&context.push, &sender_aci, &server_guid, token).await {
            Ok(()) => reported += 1,
            Err(status) => failures.push(ReportFailure {
                server_guid,
                status,
            }),
        }
    }
    if reported == 0 {
        return Err(ReportSpamError::AllFailed);
    }
    Ok(ReportOutcome { reported, failures })
}

/// StorageSyncError → ReportSpamError（账号面复用折叠口径）。
impl From<StorageSyncError> for ReportSpamError {
    fn from(value: StorageSyncError) -> Self {
        match value {
            StorageSyncError::UnknownAccount => ReportSpamError::UnknownAccount,
            _ => ReportSpamError::AllFailed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// serverGuid 校验/归一：标准连字、大小写混合、无连字 32 hex 均归一为
    /// 小写连字形态；垃圾串/空串 fail-closed（URL 注入面同堵）。
    #[test]
    fn server_guid_normalizes_and_fails_closed() {
        let guid = normalize_server_guid("6A2CB17F-EF7C-4A5D-9C69-6F8B2B1F88AE")
            .expect("uppercase parses");
        assert_eq!(guid, "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae");

        let compact = "6a2cb17fef7c4a5d9c696f8b2b1f88ae";
        assert_eq!(
            normalize_server_guid(compact).as_deref(),
            Some("6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae")
        );
        // 带空白容忍（trim）。
        assert!(normalize_server_guid("  6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae\n").is_some());

        // fail-closed：垃圾/空/带斜杠（路径注入）/urn 形态。
        assert_eq!(normalize_server_guid("garbage"), None);
        assert_eq!(normalize_server_guid(""), None);
        assert_eq!(normalize_server_guid("../../admin"), None);
        assert_eq!(
            normalize_server_guid("urn:uuid:6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"),
            None
        );
    }

    /// 举报 body 官方同构：token 缺省 = `{}`（官方 jsonData={token} 对
    /// undefined 的序列化形状）；token 在场 = `{"token":"…"}`。
    #[test]
    fn report_body_matches_official_json_shape() {
        let absent = serde_json::to_value(ReportBody { token: None }).expect("serializes");
        assert_eq!(absent, json!({}));
        let present = serde_json::to_value(ReportBody {
            token: Some("tok".to_owned()),
        })
        .expect("serializes");
        assert_eq!(present, json!({ "token": "tok" }));
    }

    /// 单批上限常量：对齐官方 reportSpamJobQueue zod max(1000)。
    #[test]
    fn batch_cap_matches_official() {
        assert_eq!(REPORT_SPAM_MAX_GUIDS, 1000);
    }
}
