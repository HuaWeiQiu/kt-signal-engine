// SPDX-License-Identifier: AGPL-3.0-only

//! L3 服务器备份 API 客户端
#![allow(dead_code)]
// 客户端面一次成型（规格书 §29.10.2 全端点）：解析/单轮
// 长轮询已由 mod.rs 生产流消费；缓存/整循环/路径构造/gRPC 封装为 D0 真机
// 接线批与媒体批的既成对接面——测试钉形在案，非死码。
//!
//! 台账 SIGNAL-UI-ALIGNMENT-LEDGER §29.10.2 L3 节的实施面：REST `v1/devices/transfer_archive` 长轮询 + CDN 下载（读凭据
//! 12h 缓存 / 转移归档无凭据特例）+ 未鉴权 gRPC 备份方法薄封装。
//!
//! 官方对位（upstream Signal-Desktop，AGPL 只读零拷贝，行号参照）：
//! - 长轮询 `GET v1/devices/transfer_archive?timeout=N`：204=预算内继续
//!   （WebAPI.preload.ts:2390-2437 单轮 ≤5min、总预算 HOUR）；200 体 union
//!   `{cdn,key} | {error:RELINK_REQUESTED|CONTINUE_WITHOUT_UPLOAD}`
//!   （WebAPI.preload.ts:1492-1501）。
//! - CDN 读凭据 12h 缓存（backups/credentials.preload.ts:62
//!   `BACKUP_CDN_READ_CREDENTIALS_VALID_DURATION = 12 * HOUR`，按 cdn 号
//!   分槽、进程内存即失效）。
//! - CDN 下载：备份对象 `/backups/{backup_dir}/{backup_name}` 附读凭据头
//!   （WebAPI.preload.ts:3173-3191）；转移归档 `/attachments/{key}`
//!   **不带**凭据头（getEphemeralBackupStream WebAPI.preload.ts:3214-3231
//!   无 headers 参）。
//! - gRPC 未鉴权方法（libsignal-net-chat grpc/backups.rs v0.99.0，
//!   `BackupAuth::present` 呈现逐请求签名）：set_backup_public_key(:207)/
//!   refresh_backup(:318)/backup_delete_all(:351)/get_media_backup_info(:446)/
//!   get_upload_form + get_media_upload_form（api/backups.rs:96-103 trait 面）/
//!   copy_backup_media(:490)/delete_backup_media(:603)。
//!   **诚实缺席**：`list_backup_media` 在本 pin（v0.99.0）无对应方法
//!   （官方桌面更新 pin 才暴露），引擎侧不实现不猜。
//!
//! 归属边界：BackupKey/签名钥/BackupAuth 材料由调用方注入（L2 派生链在
//! `crate::backup::keys`，本模块零派生零存储）；进度/编排留调用方
//! （mod.rs `download_transfer_archive` / cloud_backup.rs restore 链）。

use std::collections::HashMap;
use std::pin::pin;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::push_service::HttpAuthOverride;
use serde_json::Value;

use libsignal_net_chat::api::backups::{
    BackupAuth, BackupAuthCredentialRejected, GetUploadFormFailure, UnauthenticatedChatApi as _,
};
use libsignal_net_chat::api::{RequestError, Unauth, UploadForm};
use libsignal_net_chat::grpc::backups::{
    CopyBackupMediaItem, CopyBackupMediaOutcome, DeleteBackupMediaItem, MediaBackupInfo,
};
use libsignal_net_chat::grpc::GrpcServiceProvider;

/// CDN 读凭据新鲜窗：官方 12h（backups/credentials.preload.ts:62）。
pub(crate) const CDN_READ_CREDENTIALS_VALID_MS: u64 = 12 * 60 * 60 * 1000;

/// L3 API 错误族（错误码与既有 `BACKUP_*`/transfer failure 词表咬合，
/// 调用方按族呈现；detail 仅带消息的族有，且全部 log-safe）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackupApiError {
    /// backupId 从未设置（引导自愈后仍缺）——retryable=false。
    NotProvisioned,
    /// zkgroup 凭据被拒（呈现/签名验证失败）——retryable=false。
    AuthRejected,
    /// 手机端要求重新扫码（200 体 error 臂）——retryable=false。
    RelinkRequested,
    /// 手机端放弃迁移继续使用（200 体 error 臂）——retryable=false。
    ContinueWithoutUpload,
    /// 204 预算耗尽，手机端无可传档案（信息态非故障）——retryable=true。
    NoArchive,
    /// 传输失败（REST/gRPC/CDN 构建与收发）——retryable=true。
    Transport(String),
    /// CDN 段失败（状态码/读流）——retryable=true。
    Download(String),
    /// 协议形状不符（200 体既非档案亦非官方 error 臂等）——retryable=false。
    Protocol(String),
}

impl BackupApiError {
    /// 结构化错误码（BACKUP_* 族；transfer 词表消费方沿用
    /// transfer_progress::failure 映射，此处供 JSON-RPC/观测面统一呈现）。
    pub(crate) fn rpc_code(&self) -> &'static str {
        match self {
            Self::NotProvisioned => "BACKUP_NOT_PROVISIONED",
            Self::AuthRejected => "BACKUP_AUTH_FAILED",
            Self::RelinkRequested => "BACKUP_RELINK_REQUESTED",
            Self::ContinueWithoutUpload => "BACKUP_CONTINUE_WITHOUT_UPLOAD",
            Self::NoArchive => "BACKUP_NO_ARCHIVE",
            Self::Transport(_) => "BACKUP_TRANSPORT_FAILED",
            Self::Download(_) => "BACKUP_DOWNLOAD_FAILED",
            Self::Protocol(_) => "BACKUP_DECODE_FAILED",
        }
    }

    /// 重试面分类：传输/CDN/无档案可再试，其余终态。
    pub(crate) fn retryable(&self) -> bool {
        matches!(
            self,
            Self::NoArchive | Self::Transport(_) | Self::Download(_)
        )
    }
}

// —— transfer_archive 长轮询（REST）——

/// transfer_archive 200 响应体四分类（自 mod.rs 迁入单点，语义零变化）。
/// 官方契约是 union（Signal-Desktop WebAPI.preload.ts `TransferArchiveSchema`，
/// 行为对齐零拷贝）：既有 `{cdn,key}` 档案就绪分支之外，还有
/// `{error:"RELINK_REQUESTED"}`（手机端要求重新扫码，原迁移作废）与
/// `{error:"CONTINUE_WITHOUT_UPLOAD"}`（手机端放弃迁移继续使用）——消费
/// 语义见 backups/index.preload.ts（RelinkRequestedError / 记日志后按无
/// 档案收尾）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TransferArchiveBody {
    /// 手机端档案已就绪：去 CDN 拉。
    Ready { cdn: u32, key: String },
    /// 手机端要求重新扫码迁移。
    RelinkRequested,
    /// 手机端放弃迁移继续使用（无档案可拉）。
    ContinueWithoutUpload,
    /// 官方 union 两分支均不匹配（非法 JSON/未知 error 码/形状不符）。
    Malformed,
}

/// 200 体解析纯函数（零 I/O 单测面）：分支序对齐官方 zod union——先试
/// `{cdn,key}` 分支（齐且型对即 `Ready`，多余字段官方 strip 掉不碍事），
/// 再试 `{error}` 分支（恰为官方两字面量之一），都不匹配落 `Malformed`
/// （含非法 JSON、error 非 String/未知值、cdn 非 u64、key 非 String）。
pub(crate) fn parse_transfer_archive_body(body: &[u8]) -> TransferArchiveBody {
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return TransferArchiveBody::Malformed,
    };
    if let (Some(cdn), Some(key)) = (
        value["cdn"]
            .as_u64()
            .and_then(|cdn| u32::try_from(cdn).ok()),
        value["key"].as_str().map(str::to_owned),
    ) {
        return TransferArchiveBody::Ready { cdn, key };
    }
    match value["error"].as_str() {
        Some("RELINK_REQUESTED") => TransferArchiveBody::RelinkRequested,
        Some("CONTINUE_WITHOUT_UPLOAD") => TransferArchiveBody::ContinueWithoutUpload,
        _ => TransferArchiveBody::Malformed,
    }
}

/// 单轮长轮询结局：`Body` = 200（体已解析，处置权在调用方——mod.rs 进度
/// 编排按官方契约落终态）；`Retry` = 204（预算内继续）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PollRound {
    Body(TransferArchiveBody),
    Retry,
}

/// 单轮长轮询客户端（语义与既有 mod.rs 内联块逐一对齐，零行为变化）：
/// `GET v1/devices/transfer_archive?timeout={timeout_secs}`，构建/收发失败
/// 落 `Transport`，200 落 `Body`，204 落 `Retry`，其余状态落 `Transport`。
///
/// # Errors
/// 构建/收发/非 200/204 状态 = `Transport`。
pub(crate) async fn poll_transfer_archive_round(
    push: &PushService,
    timeout_secs: u64,
) -> Result<PollRound, BackupApiError> {
    let path = format!("v1/devices/transfer_archive?timeout={timeout_secs}");
    let response = push
        .request(
            http::Method::GET,
            Endpoint::service(&path),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| BackupApiError::Transport("transfer archive build".into()))?
        .send()
        .await
        .map_err(|e| BackupApiError::Transport(format!("transfer archive: {e}")))?;
    let status = response.status().as_u16();
    match status {
        200 => {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| BackupApiError::Transport(format!("transfer archive body: {e}")))?;
            Ok(PollRound::Body(parse_transfer_archive_body(&bytes)))
        }
        204 => Ok(PollRound::Retry),
        other => Err(BackupApiError::Transport(format!(
            "transfer archive status {other}"
        ))),
    }
}

/// 长轮询完整循环（规格书形状的可复用客户端面；mod.rs 生产流因需逐轮接
/// 进度快照保留自有循环，本面供后续编排层/测试消费）：204 计 `max_polls`
/// 轮、轮间 `interval` 空转，耗尽落 `NoArchive`；200 体按官方 union 分派
/// （Ready 放行，两 error 与 Malformed 各落对应终态错误）。
///
/// # Errors
/// 轮结局直透；204 耗尽 = `NoArchive`。
pub(crate) async fn poll_transfer_archive_until_ready<F, Fut>(
    mut round: F,
    max_polls: u32,
    interval: Duration,
) -> Result<(u32, String), BackupApiError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<PollRound, BackupApiError>>,
{
    let mut polls = 0u32;
    loop {
        match round().await? {
            PollRound::Body(TransferArchiveBody::Ready { cdn, key }) => return Ok((cdn, key)),
            PollRound::Body(TransferArchiveBody::RelinkRequested) => {
                return Err(BackupApiError::RelinkRequested);
            }
            PollRound::Body(TransferArchiveBody::ContinueWithoutUpload) => {
                return Err(BackupApiError::ContinueWithoutUpload);
            }
            PollRound::Body(TransferArchiveBody::Malformed) => {
                return Err(BackupApiError::Protocol(
                    "transfer archive body malformed".into(),
                ));
            }
            PollRound::Retry => {
                if polls + 1 >= max_polls {
                    return Err(BackupApiError::NoArchive);
                }
                polls += 1;
                tokio::time::sleep(interval).await;
            }
        }
    }
}

// —— CDN：路径/读凭据缓存/下载面 ——

/// 备份对象 CDN 路径（WebAPI.preload.ts:3183/3204 同构）。
pub(crate) fn backup_object_path(backup_dir: &str, backup_name: &str) -> String {
    format!("backups/{backup_dir}/{backup_name}")
}

/// 转移归档 CDN 路径（getEphemeralBackupStream WebAPI.preload.ts:3229 同构）。
pub(crate) fn transit_object_path(key: &str) -> String {
    format!("attachments/{key}")
}

/// CDN 读凭据缓存（按 cdn 号分槽；TTL [`CDN_READ_CREDENTIALS_VALID_MS`]，
/// 官方 credentials.preload.ts:76-192 内存缓存同构——凭证短命，进程内存
/// 即失效，重取由调用方触发）。时钟注入（毫秒），不持真实时钟。
#[derive(Default)]
pub(crate) struct CdnReadCredentialCache {
    entries: StdMutex<HashMap<u32, CdnCacheEntry>>,
}

struct CdnCacheEntry {
    headers: Vec<(String, String)>,
    retrieved_at_ms: u64,
}

impl CdnReadCredentialCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 新鲜凭据命中（TTL 内）则克隆返回；过期/缺席 = None（调用方重取）。
    pub(crate) fn get(&self, cdn: u32, now_ms: u64) -> Option<Vec<(String, String)>> {
        let entries = self.entries.lock().expect("cdn credential cache lock");
        let entry = entries.get(&cdn)?;
        if now_ms.saturating_sub(entry.retrieved_at_ms) < CDN_READ_CREDENTIALS_VALID_MS {
            return Some(entry.headers.clone());
        }
        None
    }

    /// 写入/覆盖凭据（时间源与 get 同一注入源）。
    pub(crate) fn put(&self, cdn: u32, headers: Vec<(String, String)>, now_ms: u64) {
        let mut entries = self.entries.lock().expect("cdn credential cache lock");
        entries.insert(
            cdn,
            CdnCacheEntry {
                headers,
                retrieved_at_ms: now_ms,
            },
        );
    }

    /// 全清（官方 onCdnCredentialError 清缓存同位：401 后重取前置）。
    pub(crate) fn clear(&self) {
        self.entries
            .lock()
            .expect("cdn credential cache lock")
            .clear();
    }
}

/// 通用 CDN 对象下载（下载面 `download_request`：大对象无总超时，读停滞
/// 判死由 presage 面承担——与 mod.rs/cloud_backup.rs 既有下载同纪律）。
/// 凭据头由调用方决定（备份对象附读凭据，转移归档传空表=官方无头特例）。
/// 返回原始响应（流式消费留调用方，进度面不动）。
///
/// # Errors
/// 构建/收发失败 = `Transport`；非 2xx = `Download`。
pub(crate) async fn download_object(
    push: &PushService,
    cdn: u32,
    path: &str,
    headers: &[(String, String)],
) -> Result<reqwest::Response, BackupApiError> {
    let mut request = push
        .download_request(
            http::Method::GET,
            Endpoint::cdn(cdn, path),
            HttpAuthOverride::NoOverride,
        )
        .map_err(|_| BackupApiError::Transport("cdn request build".into()))?;
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_bytes());
    }
    let response = request
        .send()
        .await
        .map_err(|e| BackupApiError::Transport(format!("cdn send: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(BackupApiError::Download(format!(
            "cdn status {}",
            status.as_u16()
        )));
    }
    Ok(response)
}

// —— 未鉴权 gRPC 方法薄封装（BackupAuth 注入）——

/// `RequestError<BackupAuthCredentialRejected>` → 错误族单点：凭据被拒 =
/// `AuthRejected`，其余（超时/断连/限流/服务端错/形状缺）= `Transport`
/// （LogSafeDisplay，detail 本就脱敏）。
fn map_auth_rejected<T>(
    result: Result<T, RequestError<BackupAuthCredentialRejected>>,
) -> Result<T, BackupApiError> {
    result.map_err(|error| match error {
        RequestError::Other(BackupAuthCredentialRejected) => BackupApiError::AuthRejected,
        other => BackupApiError::Transport(other.to_string()),
    })
}

/// 上传表单失败分类：Unauthorized = `AuthRejected`；UploadTooLarge =
/// `Protocol`（服务端上限，形状级）；其余 = `Transport`。
fn map_upload_form_failure<T>(
    result: Result<T, RequestError<GetUploadFormFailure>>,
) -> Result<T, BackupApiError> {
    result.map_err(|error| match error {
        RequestError::Other(GetUploadFormFailure::Unauthorized) => BackupApiError::AuthRejected,
        RequestError::Other(GetUploadFormFailure::UploadTooLarge) => {
            BackupApiError::Protocol("upload too large".into())
        }
        other => BackupApiError::Transport(other.to_string()),
    })
}

/// 注册备份签名公钥（首次开启云备份/换签名钥时；官方 setBackupSignatureKey
/// 同位）。签名钥/凭据材料由调用方经 `auth` 注入。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn set_backup_public_key<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<(), BackupApiError> {
    map_auth_rejected(grpc.set_backup_public_key(auth, rng).await)
}

/// 刷新备份存活期（官方 refreshBackup 同位：上传/定期刷新前置）。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn refresh_backup<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<(), BackupApiError> {
    map_auth_rejected(grpc.refresh_backup(auth, rng).await)
}

/// 媒体备份定位（官方 getMediaBackupInfo 同位；auth 须 media 档凭据）。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn get_media_backup_info<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<MediaBackupInfo, BackupApiError> {
    map_auth_rejected(grpc.get_media_backup_info(auth, rng).await)
}

/// 备份全删（官方 delete 操作对位；危险面，调用方自行把关确认流）。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn backup_delete_all<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<(), BackupApiError> {
    map_auth_rejected(grpc.backup_delete_all(auth, rng).await)
}

/// 消息备份上传表单（官方 getBackupUploadForm 同位）。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；超上限 = `Protocol`；传输失败 = `Transport`。
pub(crate) async fn get_upload_form<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    upload_size: u64,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<UploadForm, BackupApiError> {
    map_upload_form_failure(grpc.get_upload_form(auth, upload_size, rng).await)
}

/// 媒体备份上传表单（官方 getBackupMediaUploadForm 同位）。
///
/// # Errors
/// 同 [`get_upload_form`]。
pub(crate) async fn get_media_upload_form<T: GrpcServiceProvider>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    upload_size: u64,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<UploadForm, BackupApiError> {
    map_upload_form_failure(grpc.get_media_upload_form(auth, upload_size, rng).await)
}

/// 媒体复制收齐（官方 copyBackupMedia 同位；上游按 ≤1000/批分块流式，
/// 此处收齐为 Vec——outcome 的 `cdn_or_failure` 保留逐条成功/失败）。
///
/// # Errors
/// 任一批凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn copy_backup_media<T>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    items: Vec<CopyBackupMediaItem>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<Vec<CopyBackupMediaOutcome>, BackupApiError>
where
    T: GrpcServiceProvider + Clone + 'static,
{
    use futures::StreamExt;
    // 上游返回的流非 Unpin（try_flatten_stream 包 async block），pin 后消费。
    let mut stream = pin!(grpc.copy_backup_media(auth, items, rng));
    let mut outcomes = Vec::new();
    while let Some(item) = stream.next().await {
        outcomes.push(map_auth_rejected(item)?);
    }
    Ok(outcomes)
}

/// 媒体删除收齐（官方 deleteBackupMedia 同位；成功条目按官方契约回显
/// 删除项）。
///
/// # Errors
/// 凭据被拒 = `AuthRejected`；传输失败 = `Transport`。
pub(crate) async fn delete_backup_media<T>(
    grpc: &Unauth<T>,
    auth: &BackupAuth<'_>,
    items: Vec<DeleteBackupMediaItem>,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<Vec<DeleteBackupMediaItem>, BackupApiError>
where
    T: GrpcServiceProvider + Clone + 'static,
{
    use futures::StreamExt;
    // 同 copy：非 Unpin 流 pin 后消费。
    let mut stream = pin!(grpc.delete_backup_media(auth, items, rng));
    let mut deleted = Vec::new();
    while let Some(item) = stream.next().await {
        deleted.push(map_auth_rejected(item)?);
    }
    Ok(deleted)
}
#[cfg(test)]
mod tests {
    use super::*;

    // —— 200 体四分类钉形（自 mod.rs 迁入，官方 TransferArchiveSchema union
    //    对齐，行为对齐零拷贝）：{cdn,key} 即 Ready（两分支字段并存时官方
    //    zod union 先试 {cdn,key} 分支，多余字段 strip——同形序）；error 恰
    //    为官方两字面量之一对应对应变体；其余一律 Malformed。
    #[test]
    fn transfer_archive_body_classification() {
        assert_eq!(
            parse_transfer_archive_body(br#"{"cdn":3,"key":"manifold://cb#k"}"#),
            TransferArchiveBody::Ready {
                cdn: 3,
                key: "manifold://cb#k".to_owned()
            }
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"cdn":0,"key":"k","error":"RELINK_REQUESTED"}"#),
            TransferArchiveBody::Ready {
                cdn: 0,
                key: "k".to_owned()
            },
            "官方 zod union 分支序：{{cdn,key}} 齐 = Ready 赢"
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"error":"RELINK_REQUESTED"}"#),
            TransferArchiveBody::RelinkRequested
        );
        assert_eq!(
            parse_transfer_archive_body(br#"{"error":"CONTINUE_WITHOUT_UPLOAD"}"#),
            TransferArchiveBody::ContinueWithoutUpload
        );
        let malformed_bodies: [&[u8]; 9] = [
            b"not json",
            br#"{}"#,
            br#"{"cdn":3}"#,
            br#"{"key":"k"}"#,
            br#"{"cdn":"3","key":"k"}"#,
            br#"{"cdn":3.5,"key":"k"}"#,
            br#"{"error":"SOMETHING_ELSE"}"#,
            br#"{"error":123}"#,
            br#"{"error":"relink_requested"}"#,
        ];
        for body in malformed_bodies {
            assert_eq!(
                parse_transfer_archive_body(body),
                TransferArchiveBody::Malformed,
                "「{}」应判 Malformed",
                String::from_utf8_lossy(body)
            );
        }
    }

    // —— 长轮询完整循环（注入形）：204×N 后 Ready 放行。
    #[tokio::test]
    async fn poll_loop_returns_ready_after_retries() {
        let polls = std::rc::Rc::new(std::cell::Cell::new(0u32));
        let polls_for_round = polls.clone();
        let result = poll_transfer_archive_until_ready(
            || {
                polls_for_round.set(polls_for_round.get() + 1);
                let seen = polls_for_round.get();
                async move {
                    if seen < 3 {
                        Ok(PollRound::Retry)
                    } else {
                        Ok(PollRound::Body(TransferArchiveBody::Ready {
                            cdn: 3,
                            key: "k".to_owned(),
                        }))
                    }
                }
            },
            60,
            Duration::ZERO,
        )
        .await
        .expect("ready after retries");
        assert_eq!(result, (3, "k".to_owned()));
        assert_eq!(polls.get(), 3);
    }

    // 204 耗尽 = NoArchive（信息态非故障，可再试）。
    #[tokio::test]
    async fn poll_loop_exhaustion_is_no_archive() {
        let result =
            poll_transfer_archive_until_ready(|| async { Ok(PollRound::Retry) }, 5, Duration::ZERO)
                .await
                .expect_err("exhaustion is NoArchive");
        assert_eq!(result, BackupApiError::NoArchive);
        assert_eq!(result.rpc_code(), "BACKUP_NO_ARCHIVE");
        assert!(result.retryable(), "NoArchive 可再试（信息态非故障）");
    }

    // 200 体终态三臂各落类型化错误（两 error + Malformed），均为终态。
    #[tokio::test]
    async fn poll_loop_terminal_bodies_map_to_typed_errors() {
        let cases: [(TransferArchiveBody, BackupApiError); 3] = [
            (
                TransferArchiveBody::RelinkRequested,
                BackupApiError::RelinkRequested,
            ),
            (
                TransferArchiveBody::ContinueWithoutUpload,
                BackupApiError::ContinueWithoutUpload,
            ),
            (
                TransferArchiveBody::Malformed,
                BackupApiError::Protocol("transfer archive body malformed".into()),
            ),
        ];
        for (body, expected) in cases {
            let result = poll_transfer_archive_until_ready(
                || {
                    let body = body.clone();
                    async move { Ok(PollRound::Body(body)) }
                },
                5,
                Duration::ZERO,
            )
            .await
            .expect_err("terminal body is an error");
            assert_eq!(result, expected, "{body:?} 应落 {}", expected.rpc_code());
            assert!(!result.retryable(), "{body:?} 是终态");
        }
    }

    // 传输错误直透（不吞不裹新语义）。
    #[tokio::test]
    async fn poll_loop_transport_error_passthrough() {
        let result = poll_transfer_archive_until_ready(
            || async { Err(BackupApiError::Transport("boom".into())) },
            5,
            Duration::ZERO,
        )
        .await
        .expect_err("transport error passes through");
        assert_eq!(result, BackupApiError::Transport("boom".into()));
        assert!(result.retryable());
    }

    // —— CDN 读凭据缓存：TTL 内命中、过期失效、分 cdn 隔离、clear 全清。
    #[test]
    fn cdn_credential_cache_ttl_and_isolation() {
        let cache = CdnReadCredentialCache::new();
        let headers = vec![("h".to_owned(), "v".to_owned())];
        cache.put(3, headers.clone(), 1_000);

        assert_eq!(cache.get(3, 1_000).as_ref(), Some(&headers), "写入即命中");
        assert_eq!(
            cache
                .get(3, 1_000 + CDN_READ_CREDENTIALS_VALID_MS - 1)
                .as_ref(),
            Some(&headers),
            "TTL 边界内（12h-1ms）仍命中"
        );
        assert!(
            cache
                .get(3, 1_000 + CDN_READ_CREDENTIALS_VALID_MS)
                .is_none(),
            "TTL 满 12h 即过期（官方同窗）"
        );
        assert!(cache.get(2, 1_000).is_none(), "分 cdn 号隔离");

        cache.clear();
        assert!(cache.get(3, 1_000).is_none(), "clear 全清");
    }

    // —— CDN 路径构造：备份对象 vs 转移归档特例（官方两条 CDN 面同构）。
    #[test]
    fn cdn_object_paths_match_official_shapes() {
        assert_eq!(
            backup_object_path("dir.abc", "name.def"),
            "backups/dir.abc/name.def"
        );
        assert_eq!(
            transit_object_path("manifold-key"),
            "attachments/manifold-key"
        );
    }

    // —— 错误族词表钉形：rpc_code 与既有 BACKUP_* 家族咬合、retryable 分类。
    #[test]
    fn error_family_codes_and_retryability() {
        let cases: [(BackupApiError, &'static str, bool); 8] = [
            (
                BackupApiError::NotProvisioned,
                "BACKUP_NOT_PROVISIONED",
                false,
            ),
            (BackupApiError::AuthRejected, "BACKUP_AUTH_FAILED", false),
            (
                BackupApiError::RelinkRequested,
                "BACKUP_RELINK_REQUESTED",
                false,
            ),
            (
                BackupApiError::ContinueWithoutUpload,
                "BACKUP_CONTINUE_WITHOUT_UPLOAD",
                false,
            ),
            (BackupApiError::NoArchive, "BACKUP_NO_ARCHIVE", true),
            (
                BackupApiError::Transport("x".into()),
                "BACKUP_TRANSPORT_FAILED",
                true,
            ),
            (
                BackupApiError::Download("x".into()),
                "BACKUP_DOWNLOAD_FAILED",
                true,
            ),
            (
                BackupApiError::Protocol("x".into()),
                "BACKUP_DECODE_FAILED",
                false,
            ),
        ];
        for (error, code, retryable) in cases {
            assert_eq!(error.rpc_code(), code, "{error:?} 错误码");
            assert_eq!(error.retryable(), retryable, "{error:?} 重试分类");
        }
    }
}
