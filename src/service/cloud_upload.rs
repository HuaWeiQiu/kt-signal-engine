// SPDX-License-Identifier: AGPL-3.0-only

//! 云备份 upload 面（docs/cloud-backup-contract.md §6，二期）：把账号备份
//! 产物（messages 档密文归档）上传 Signal 云存储——官方 `Backups.upload`
//! 同链（index.preload.ts:310-338），AGPL 只读零拷贝。
//!
//! 全链（§6.3）：①装配段=专用 OS 线程（§10.3 同款，装配链纯同步阻塞，
//! 不得阻塞接收管线）：输入工件 → `MainStreamWriter`（local/remote 同容器，
//! §6.1 钉定）→ 临时产物 `<account_dir>/cloud-upload-main-tmp`；②上行段=
//! LocalSet watcher（oneshot 交接）：凭据（§1 链 404 引导自愈）→ zkgroup
//! 兑换 → form gRPC（`get_upload_form`，cdn===3 强制）→ TUS create-with-
//! upload 单发 POST（128 KiB 块流式读盘）→ `refresh_backup` → 临时产物
//! 即弃。成功终态快照清缺，失败留 failed 相位（restore 同纪律）。
//!
//! 纪律：凭据/header 不落日志不落盘；unknown outcome 永不自动重试；上传
//! POST 请求级超时 900 s（显式有界）；产物 1 GiB 帽 fail-closed。

use std::io::Read as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use presage::libsignal_service::configuration::Endpoint;
use presage::libsignal_service::prelude::PushService;
use presage::libsignal_service::protocol::PrivateKey;
use presage::libsignal_service::push_service::HttpAuthOverride;

use libsignal_net_chat::api::backups::{BackupAuth, UnauthenticatedChatApi as _};
use libsignal_net_chat::api::Unauth;

use crate::backup::{
    assemble, validate_first_line, write_main_stream, LocalBackupParams, ARTIFACT_FILE_NAME,
};
use crate::service::cloud_backup::{self, BackupAuthRequestMaterials};
/// 上传产物帽（§6.3）：装配产物超帽 fail-closed（`BACKUP_UPLOAD_FAILED`）。
pub(crate) const UPLOAD_SIZE_CAP: u64 = 1_073_741_824;

/// 上行读盘块大小（§6.3：内存 O(块)，进度按块计数）。
const UPLOAD_CHUNK: usize = 128 * 1024;

/// 上传 POST 请求级超时（§6.3：PushService 客户端默认 65 s 不够长文上行，
/// per-request 覆盖，显式有界）。
const UPLOAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(900);

/// 官方 TUS create-with-upload 协议常量（tusProtocol.node.ts:122-130）。
const TUS_PROTOCOL_VERSION: &str = "1.0.0";
const TUS_CONTENT_TYPE: &str = "application/offset+octet-stream";

/// 临时产物文件名（§6.3：`<account_dir>/cloud-upload-main-tmp`，任何终态
/// 必删；崩溃残留由下次上传覆盖写收敛）。
pub(crate) const UPLOAD_TEMP_NAME: &str = "cloud-upload-main-tmp";

/// cdn3 = 官方备份上行 CDN（`CDNS_SUPPORTING_TUS`，uploadAttachment
/// preload.ts:47；WebAPI:3261 strictAssert 同款强制）。
const OFFICIAL_UPLOAD_CDN: u32 = 3;

/// upload 面错误族（§6.2 八码位；`rpc_code` 单一映射源，uploadStatus
/// failed 相位同表两端不漂）。
#[derive(Debug)]
pub(crate) enum CloudUploadError {
    /// 未注册/fake 账号、AEP/签名密钥缺席、zkgroup 兑换被拒。
    AuthRejected,
    /// 引导自愈后仍无档（账号从未开启云备份）。
    NotProvisioned,
    /// 同引擎已有在途 upload（进程级单槽）。
    AlreadyRunning,
    /// 工件面：start 缺席/首行非法；后台装配行级/帧序违规。码位随件。
    Artifact(&'static str),
    /// 凭据拉取/form gRPC/refresh_backup 传输段失败。
    Transport(String),
    /// CDN 上行失败（非 2xx/cdn≠3/超帽/上行读盘失败）。
    Upload(String),
    /// 临时归档文件系统写失败。
    Io(String),
}

impl CloudUploadError {
    /// 结构化错误码（§6.2 表）。
    pub(crate) fn rpc_code(&self) -> &'static str {
        match self {
            Self::AuthRejected => "BACKUP_AUTH_FAILED",
            Self::NotProvisioned => "BACKUP_NOT_PROVISIONED",
            Self::AlreadyRunning => "BACKUP_UPLOAD_ALREADY_RUNNING",
            Self::Artifact(code) => code,
            Self::Transport(_) => "BACKUP_TRANSPORT_FAILED",
            Self::Upload(_) => "BACKUP_UPLOAD_FAILED",
            Self::Io(_) => "BACKUP_IO_FAILED",
        }
    }

    /// 错误细节（仅带消息的族有）。
    pub(crate) fn detail(&self) -> Option<&str> {
        match self {
            Self::Transport(m) | Self::Upload(m) | Self::Io(m) => Some(m),
            _ => None,
        }
    }
}

/// restore 凭据链错误 → upload 错误族（§1 链复用单点的适配面）。
impl From<cloud_backup::CloudRestoreError> for CloudUploadError {
    fn from(error: cloud_backup::CloudRestoreError) -> Self {
        match error {
            cloud_backup::CloudRestoreError::NotProvisioned => Self::NotProvisioned,
            cloud_backup::CloudRestoreError::AuthRejected => Self::AuthRejected,
            cloud_backup::CloudRestoreError::AlreadyRunning => Self::AlreadyRunning,
            cloud_backup::CloudRestoreError::Transport(m) => Self::Transport(m),
            cloud_backup::CloudRestoreError::Download(m) => Self::Upload(m),
            cloud_backup::CloudRestoreError::Decrypt(m)
            | cloud_backup::CloudRestoreError::Export(m) => Self::Io(m),
        }
    }
}

/// 装配段产物统计（frames=归档总帧数；lines=工件行数；size=密文产物字节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AssemblyStats {
    pub(crate) frames: u64,
    pub(crate) lines: u64,
    pub(crate) size: u64,
}

/// 产物帽检查（§6.3；独立纯函数供单测钉边界）。
pub(crate) fn check_size_cap(size: u64) -> Result<(), CloudUploadError> {
    if size > UPLOAD_SIZE_CAP {
        Err(CloudUploadError::Upload(format!("size {size} over cap")))
    } else {
        Ok(())
    }
}

/// start 工件闸（§6.2：缺失=`BACKUP_ARTIFACT_MISSING`；首行非法当场删
/// 工件+`BACKUP_ARTIFACT_INVALID`——§10.2 硬边界同款）。
pub(crate) fn precheck_artifact(account_dir: &Path) -> Result<(), CloudUploadError> {
    let artifact = account_dir.join(ARTIFACT_FILE_NAME);
    if !artifact.is_file() {
        return Err(CloudUploadError::Artifact("BACKUP_ARTIFACT_MISSING"));
    }
    if validate_first_line(&artifact).is_err() {
        let _ = std::fs::remove_file(&artifact);
        return Err(CloudUploadError::Artifact("BACKUP_ARTIFACT_INVALID"));
    }
    Ok(())
}

/// 装配段（专用线程体；全同步阻塞）：工件 → 容器 → 临时产物。任何返回
/// 后调用方负责临时产物清理（watcher 统一 finally 删）。
pub(crate) fn run_assembly(
    artifact: &Path,
    temp: &Path,
    params: &LocalBackupParams,
) -> Result<AssemblyStats, CloudUploadError> {
    // 工件缺席自检（precheck 与装配线程之间的窗口也归 MISSING 诊断面；
    // 打开后瞬间的极端竞态仍走下方 IO 臂诚实上报）。
    if !artifact.is_file() {
        return Err(CloudUploadError::Artifact("BACKUP_ARTIFACT_MISSING"));
    }
    let mut lines: u64 = 0;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(temp)
        .map_err(|e| CloudUploadError::Io(format!("temp open: {e}")))?;
    let backup_time_ms = params.backup_time_ms;
    let outcome = write_main_stream(std::io::BufWriter::new(file), params, |writer| {
        let reader = std::io::BufReader::new(std::fs::File::open(artifact)?);
        let stats = assemble(writer, reader, backup_time_ms)?;
        lines = stats.lines;
        Ok(())
    });
    let (mut file, stats) = match outcome {
        Ok(pair) => pair,
        Err(error) => {
            return Err(match error {
                crate::backup::BackupError::FrameSequence(_)
                | crate::backup::BackupError::Artifact(_) => {
                    CloudUploadError::Artifact("BACKUP_ARTIFACT_INVALID")
                }
                crate::backup::BackupError::SnapshotExists(_) => {
                    CloudUploadError::Io("snapshot exists".into())
                }
                crate::backup::BackupError::Io(e) => CloudUploadError::Io(format!("{e}")),
                crate::backup::BackupError::Proto(e) => CloudUploadError::Io(format!("{e}")),
            });
        }
    };
    file.flush()
        .map_err(|e| CloudUploadError::Io(format!("temp flush: {e}")))?;
    let size = file
        .get_ref()
        .metadata()
        .map_err(|e| CloudUploadError::Io(format!("temp stat: {e}")))?
        .len();
    check_size_cap(size)?;
    Ok(AssemblyStats {
        frames: stats.frame_count,
        lines,
        size,
    })
}

/// 进度快照（`backups.uploadStatus` 只读数据源）。phase：exporting=装配段
/// （无字节计数语义）；uploading=上行段（块计数有效，total=产物字节）；
/// failed=终态（errorCode 结构化透出，快照保留至下次 start 覆盖）。成功
/// 终态清缺（restore 同纪律）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CloudUploadSnapshot {
    pub(crate) phase: &'static str,
    pub(crate) bytes_read: u64,
    pub(crate) bytes_total: Option<u64>,
    pub(crate) error_code: Option<&'static str>,
}

/// 进度注册表 + 进程级单槽运行闸（Clone 供 Service 与 watcher 双持；进程
/// 内存不落盘不跨重启）。
#[derive(Clone, Default)]
pub(crate) struct CloudUploadProgress {
    inner: std::sync::Arc<std::sync::Mutex<Option<CloudUploadSnapshot>>>,
}

static CLOUD_UPLOAD_RUNNING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl CloudUploadProgress {
    pub(crate) fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// 重入闸抢占（start 前置检查全过后）：false→true 原子换位成功 = 抢到。
    pub(crate) fn try_begin_running() -> bool {
        CLOUD_UPLOAD_RUNNING
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }

    /// 重入闸释放（watcher 收尾唯一出口）。
    pub(crate) fn finish_running() {
        CLOUD_UPLOAD_RUNNING.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// 装配段在途（无字节计数语义，§6.2）。
    pub(crate) fn begin_exporting(&self) {
        *self.inner.lock().expect("cloud upload lock") = Some(CloudUploadSnapshot {
            phase: "exporting",
            bytes_read: 0,
            bytes_total: None,
            error_code: None,
        });
    }

    /// 上行段（total=装配产物字节，不给假 0）。
    pub(crate) fn begin_uploading(&self, bytes_total: u64) {
        let mut guard = self.inner.lock().expect("cloud upload lock");
        let snapshot = guard.get_or_insert(CloudUploadSnapshot {
            phase: "uploading",
            bytes_read: 0,
            bytes_total: None,
            error_code: None,
        });
        snapshot.phase = "uploading";
        snapshot.bytes_total = Some(bytes_total);
    }

    /// 上行块计数。
    pub(crate) fn add(&self, bytes: u64) {
        let mut guard = self.inner.lock().expect("cloud upload lock");
        if let Some(snapshot) = guard.as_mut() {
            snapshot.bytes_read += bytes;
        }
    }

    /// 终态失败：保留已计字节，翻相位 + 结构化 code。
    pub(crate) fn fail(&self, code: &'static str) {
        let mut guard = self.inner.lock().expect("cloud upload lock");
        let snapshot = guard.get_or_insert(CloudUploadSnapshot {
            phase: "failed",
            bytes_read: 0,
            bytes_total: None,
            error_code: None,
        });
        snapshot.phase = "failed";
        snapshot.error_code = Some(code);
    }

    pub(crate) fn snapshot(&self) -> Option<CloudUploadSnapshot> {
        *self.inner.lock().expect("cloud upload lock")
    }

    pub(crate) fn clear(&self) {
        *self.inner.lock().expect("cloud upload lock") = None;
    }
}

/// watcher（LocalSet 承载，start spawn_local 挂载）：等装配 oneshot →
/// 上行全链 → 终态翻面 + 临时产物必删 + 运行闸释放（唯一出口）。materials
/// 以值移交（zkgroup 上下文 receive 一次性消费）。
pub(crate) async fn watch_and_upload<const VERSION: u8>(
    rx: tokio::sync::oneshot::Receiver<Result<AssemblyStats, CloudUploadError>>,
    push: PushService,
    signing_key: PrivateKey,
    materials: BackupAuthRequestMaterials<VERSION>,
    temp: PathBuf,
    progress: CloudUploadProgress,
) {
    let outcome = match rx.await {
        Ok(outcome) => outcome,
        Err(_) => Err(CloudUploadError::Upload("assembly channel".into())),
    };
    match outcome {
        Ok(stats) => {
            progress.begin_uploading(stats.size);
            use rand::SeedableRng as _;
            let mut rng = rand::rngs::StdRng::from_seed(rand::random());
            let rng: &mut (dyn rand::CryptoRng + Send) = &mut rng;
            match upload_chain(
                &push,
                &signing_key,
                materials,
                &temp,
                stats.size,
                &progress,
                rng,
            )
            .await
            {
                Ok(()) => progress.clear(),
                Err(error) => progress.fail(error.rpc_code()),
            }
        }
        Err(error) => progress.fail(error.rpc_code()),
    }
    // 任何终态必删（§6.3：官方 finally unlink 同款）。
    let _ = std::fs::remove_file(&temp);
    CloudUploadProgress::finish_running();
}

/// 上行全链（§6.3 ②③）：凭据（§1 链，404 引导自愈）→ zkgroup 兑换 →
/// BackupAuth → form gRPC → cdn===3 强制 → TUS 单发 POST（块流式）→
/// refresh_backup。
///
/// # Errors
/// 各段如实映射（§6.2 码表）；unknown outcome 不自动重试。
async fn upload_chain<const VERSION: u8>(
    push: &PushService,
    signing_key: &PrivateKey,
    materials: BackupAuthRequestMaterials<VERSION>,
    temp: &Path,
    size: u64,
    progress: &CloudUploadProgress,
    rng: &mut (dyn rand::CryptoRng + Send),
) -> Result<(), CloudUploadError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let entry = cloud_backup::fetch_today_credentials(push, &materials.request_b64, now_ms)
        .await
        .map_err(CloudUploadError::from)?;
    let generic_params =
        cloud_backup::generic_server_public_params().map_err(|_| CloudUploadError::AuthRejected)?;
    let credential = cloud_backup::receive_backup_auth_credential(
        materials.request_context,
        &entry.credential_b64,
        &generic_params,
        entry.redemption_time_seconds,
    )
    .map_err(|_| CloudUploadError::AuthRejected)?;
    let backup_auth = BackupAuth::new(&credential, &generic_params, signing_key);

    // form gRPC（未鉴权连接 + BackupAuth 呈现；restore 同构）。
    let chat = cloud_backup::connect_production_chat()
        .await
        .map_err(|e| CloudUploadError::Transport(format!("chat connect: {e}")))?;
    let grpc = Unauth(
        chat.shared_h2_connection()
            .expect("H2 connection available"),
    );
    let form = grpc
        .get_upload_form(&backup_auth, size, rng)
        .await
        .map_err(|_| CloudUploadError::Transport("upload form".into()))?;
    if form.cdn != OFFICIAL_UPLOAD_CDN {
        return Err(CloudUploadError::Upload(format!("cdn {}", form.cdn)));
    }

    // TUS create-with-upload 单发 POST（§6.1 钉定；128 KiB 块流式读盘）。
    let mut request = push
        .request(
            http::Method::POST,
            Endpoint::Absolute(
                form.signed_upload_url
                    .parse()
                    .map_err(|_| CloudUploadError::Upload("signed url".into()))?,
            ),
            HttpAuthOverride::Unidentified,
        )
        .map_err(|_| CloudUploadError::Upload("request build".into()))?
        .timeout(UPLOAD_REQUEST_TIMEOUT)
        .header("Tus-Resumable", TUS_PROTOCOL_VERSION)
        .header("Upload-Length", size.to_string())
        .header(
            "Upload-Metadata",
            format!("filename {}", base64_standard(form.key.as_bytes())),
        )
        .header("Content-Type", TUS_CONTENT_TYPE);
    for (name, value) in &form.headers {
        request = request.header(name, value);
    }
    let file = std::fs::File::open(temp)
        .map_err(|e| CloudUploadError::Upload(format!("ciphertext open: {e}")))?;
    request = request.body(reqwest::Body::wrap_stream(upload_chunks(
        file,
        progress.clone(),
    )));
    let response = request
        .send()
        .await
        .map_err(|e| CloudUploadError::Upload(format!("cdn send: {e}")))?;
    if !response.status().is_success() {
        return Err(CloudUploadError::Upload(format!(
            "cdn status {}",
            response.status().as_u16()
        )));
    }

    // refresh_backup（§6.0 裁定 5：上传确认成功后引擎直发；失败=unknown-
    // outcome 窗口如实透码，retry=整链重来）。
    grpc.refresh_backup(&backup_auth, rng)
        .await
        .map_err(|_| CloudUploadError::Transport("refresh".into()))?;
    Ok(())
}

fn base64_standard(value: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(value)
}

/// 上行读盘块流（§6.3：128 KiB 块 + 进度按块计数；本地文件页缓块读为
/// µs 级短阻塞，装配长阻塞面已在专用线程，接收管线不被饿死）。
fn upload_chunks(
    file: std::fs::File,
    progress: CloudUploadProgress,
) -> impl futures::Stream<Item = Result<Vec<u8>, std::io::Error>> {
    futures::stream::unfold((file, progress), |(mut file, progress)| async move {
        let mut chunk = vec![0u8; UPLOAD_CHUNK];
        match file.read(&mut chunk) {
            Ok(0) => None,
            Ok(n) => {
                progress.add(n as u64);
                chunk.truncate(n);
                Some((Ok(chunk), (file, progress)))
            }
            Err(e) => Some((Err(e), (file, progress))),
        }
    })
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_millis()
        .try_into()
        .expect("millis fit u64")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use base64::Engine as _;
    use libsignal_message_backup::backup::Purpose;
    use libsignal_message_backup::frame::CursorFactory;
    use libsignal_message_backup::BackupReader;

    use super::*;
    use crate::backup::{derive_keys, LocalBackupParams};

    fn temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "kt-engine-cloud-upload-{}-{tag}-{n}",
            std::process::id()
        ))
    }

    fn account_line() -> String {
        format!(
            "{{\"type\":\"account\",\"profileKey\":\"{}\",\"givenName\":\"Kit\"}}",
            "11".repeat(32)
        )
    }

    fn contact_line(id: u64) -> String {
        format!(
            "{{\"type\":\"contact\",\"id\":{id},\"e164\":15555550100,\"registration\":\"registered\"}}"
        )
    }

    fn chat_line(id: u64, recipient: u64) -> String {
        format!("{{\"type\":\"chat\",\"id\":{id},\"recipient\":{recipient}}}")
    }

    fn item_line(chat: u64, author: u64, body: &str) -> String {
        format!(
            "{{\"type\":\"chat_item\",\"chat\":{chat},\"author\":{author},\"dateSent\":1760000001000,\
             \"direction\":\"incoming\",\"dateReceived\":1760000001005,\"body\":\"{body}\"}}"
        )
    }

    fn small_artifact(dir: &std::path::Path) -> PathBuf {
        std::fs::create_dir_all(dir).expect("dir");
        let artifact = dir.join(ARTIFACT_FILE_NAME);
        std::fs::write(
            &artifact,
            format!(
                "{}\n",
                [
                    account_line(),
                    contact_line(10),
                    chat_line(1, 10),
                    item_line(1, 10, "hello"),
                ]
                .join("\n")
            ),
        )
        .expect("artifact written");
        artifact
    }

    fn upload_params(backup_time_ms: u64) -> LocalBackupParams {
        LocalBackupParams {
            keys: crate::backup::tests_official_keys(),
            backup_time_ms,
            media_root_backup_key: [0x42; 32],
            app_version: "kt-signal-engine 0.1.0".into(),
        }
    }

    /// ①装配产物官方 reader 校验（`Purpose::RemoteBackup`，§6.5 链一）：
    /// 容器/帧面正确性由官方 validator 钉死；frames/lines/size 统计如实。
    #[test]
    fn assembly_produces_official_valid_remote_archive() {
        let dir = temp_dir("assembly");
        let artifact = small_artifact(&dir);
        let temp = dir.join(UPLOAD_TEMP_NAME);
        let stats = run_assembly(&artifact, &temp, &upload_params(1_760_000_000_000))
            .expect("assembly succeeds");
        // BackupInfo + Account + self + contact + chat + item + folder(自动补)。
        assert_eq!(stats.frames, 7);
        assert_eq!(stats.lines, 4);
        let ciphertext = std::fs::read(&temp).expect("temp readable");
        assert_eq!(stats.size, ciphertext.len() as u64);

        // 官方 reader 双闸：能解密分帧 + RemoteBackup purpose validate 全绿。
        let keys = derive_keys(
            &"dtjs858asj6tv0jzsqrsmj0ubp335pisj98e9ssnss8myoc08drhtcktyawvx45l"
                .parse()
                .expect("aep"),
            &presage::libsignal_service::protocol::Aci::from(
                presage::libsignal_service::prelude::Uuid::from_bytes([
                    0x65, 0x9a, 0xa5, 0xf4, 0xa2, 0x8d, 0xfc, 0xc1, 0x1e, 0xa1, 0xb9, 0x97, 0x53,
                    0x7a, 0x3d, 0x95,
                ]),
            ),
        );
        let archive = ciphertext.clone();
        let validate = tokio::runtime::Runtime::new().expect("rt");
        let result = validate.block_on(async {
            BackupReader::new_encrypted_compressed(
                &keys.message_key,
                CursorFactory::new(&archive),
                Purpose::RemoteBackup,
            )
            .await
            .expect("container opens")
            .validate_all()
            .await
        });
        assert!(
            result.result.is_ok(),
            "official RemoteBackup validator: {:?}",
            result.result.err()
        );

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// ②③失败形态：工件行级违规=Artifact(INVALID)；工件缺席=Artifact(MISSING)；
    /// 上行帽 fail-closed；临时产物覆盖写（崩溃残留收敛）。
    #[test]
    fn assembly_failure_faces_and_size_cap() {
        let dir = temp_dir("failures");
        std::fs::create_dir_all(&dir).expect("dir");
        let artifact = dir.join(ARTIFACT_FILE_NAME);

        // 缺席。
        let missing = run_assembly(&artifact, &dir.join(UPLOAD_TEMP_NAME), &upload_params(0));
        assert_eq!(
            missing.unwrap_err().rpc_code(),
            "BACKUP_ARTIFACT_MISSING",
            "assembly opens the artifact, absence maps to the same code"
        );

        // 行级违规（未知 type）。
        std::fs::write(
            &artifact,
            format!("{}\n{{\"type\":\"bogus\"}}", account_line()),
        )
        .expect("artifact");
        let invalid = run_assembly(&artifact, &dir.join(UPLOAD_TEMP_NAME), &upload_params(0))
            .expect_err("invalid artifact");
        assert_eq!(invalid.rpc_code(), "BACKUP_ARTIFACT_INVALID");

        // 帽值边界（独立纯函数钉 1 GiB）。
        assert!(matches!(check_size_cap(UPLOAD_SIZE_CAP), Ok(())));
        assert!(matches!(
            check_size_cap(UPLOAD_SIZE_CAP + 1),
            Err(CloudUploadError::Upload(_))
        ));

        // 崩溃残留收敛：临时产物已存在时覆盖写不炸。
        let artifact = small_artifact(&dir);
        let temp = dir.join(UPLOAD_TEMP_NAME);
        run_assembly(&artifact, &temp, &upload_params(1)).expect("first assembly");
        run_assembly(&artifact, &temp, &upload_params(2)).expect("overwrite residue");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// 进度快照生命周期（exporting→uploading→成功清缺 / failed 留痕保字节）
    /// + 进程级单槽运行闸互斥（§6.5 链二）。
    #[test]
    fn progress_lifecycle_and_running_gate() {
        let progress = CloudUploadProgress::new();
        assert!(progress.snapshot().is_none());
        progress.begin_exporting();
        let s = progress.snapshot().expect("exporting");
        assert_eq!(s.phase, "exporting");
        assert_eq!(s.bytes_read, 0);
        assert_eq!(s.bytes_total, None);
        progress.begin_uploading(100);
        progress.add(40);
        let s = progress.snapshot().expect("uploading");
        assert_eq!(s.phase, "uploading");
        assert_eq!(s.bytes_read, 40);
        assert_eq!(s.bytes_total, Some(100));
        progress.fail("BACKUP_UPLOAD_FAILED");
        let s = progress.snapshot().expect("failed terminal");
        assert_eq!(s.phase, "failed");
        assert_eq!(s.bytes_read, 40, "uploaded bytes preserved on failure");
        assert_eq!(s.error_code, Some("BACKUP_UPLOAD_FAILED"));
        progress.clear();
        assert!(progress.snapshot().is_none());
        // 无快照在途前失败也立 failed 相位（凭据段失败形态）。
        progress.fail("BACKUP_AUTH_FAILED");
        let s = progress.snapshot().expect("early failure");
        assert_eq!(s.phase, "failed");
        assert_eq!(s.bytes_read, 0);
        progress.clear();

        assert!(CloudUploadProgress::try_begin_running());
        assert!(!CloudUploadProgress::try_begin_running());
        CloudUploadProgress::finish_running();
        assert!(CloudUploadProgress::try_begin_running());
        CloudUploadProgress::finish_running();
    }

    /// 错误族 → 稳定码表（§6.2 八码位；detail 仅带消息族有）。
    #[test]
    fn error_families_map_to_stable_rpc_codes() {
        assert_eq!(
            CloudUploadError::AuthRejected.rpc_code(),
            "BACKUP_AUTH_FAILED"
        );
        assert_eq!(
            CloudUploadError::NotProvisioned.rpc_code(),
            "BACKUP_NOT_PROVISIONED"
        );
        assert_eq!(
            CloudUploadError::AlreadyRunning.rpc_code(),
            "BACKUP_UPLOAD_ALREADY_RUNNING"
        );
        assert_eq!(
            CloudUploadError::Artifact("BACKUP_ARTIFACT_MISSING").rpc_code(),
            "BACKUP_ARTIFACT_MISSING"
        );
        assert_eq!(
            CloudUploadError::Artifact("BACKUP_ARTIFACT_INVALID").rpc_code(),
            "BACKUP_ARTIFACT_INVALID"
        );
        assert_eq!(
            CloudUploadError::Transport("x".into()).rpc_code(),
            "BACKUP_TRANSPORT_FAILED"
        );
        assert_eq!(
            CloudUploadError::Upload("x".into()).rpc_code(),
            "BACKUP_UPLOAD_FAILED"
        );
        assert_eq!(
            CloudUploadError::Io("x".into()).rpc_code(),
            "BACKUP_IO_FAILED"
        );
        assert_eq!(
            CloudUploadError::Upload("cdn 502".into()).detail(),
            Some("cdn 502")
        );
        assert_eq!(CloudUploadError::AuthRejected.detail(), None);
        assert_eq!(CloudUploadError::AlreadyRunning.detail(), None);
        assert_eq!(
            CloudUploadError::Artifact("BACKUP_ARTIFACT_MISSING").detail(),
            None
        );
    }

    /// start 工件闸：缺失 / 首行非法（当场删工件）/ 合法放行（§6.2 序）。
    #[test]
    fn precheck_gates_missing_and_invalid_artifacts() {
        let dir = temp_dir("precheck");
        std::fs::create_dir_all(&dir).expect("dir");

        let missing = precheck_artifact(&dir).expect_err("missing");
        assert_eq!(missing.rpc_code(), "BACKUP_ARTIFACT_MISSING");

        let artifact = dir.join(ARTIFACT_FILE_NAME);
        std::fs::write(&artifact, format!("{}\n", contact_line(10))).expect("artifact");
        let invalid = precheck_artifact(&dir).expect_err("invalid");
        assert_eq!(invalid.rpc_code(), "BACKUP_ARTIFACT_INVALID");
        assert!(!artifact.exists(), "invalid artifact deleted synchronously");

        std::fs::write(&artifact, format!("{}\n", account_line())).expect("artifact");
        precheck_artifact(&dir).expect("valid artifact");

        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// base64 辅助面与官方同标准形（Upload-Metadata filename 值域）。
    #[test]
    fn metadata_filename_uses_standard_base64() {
        assert_eq!(
            base64_standard(b"abc"),
            base64::engine::general_purpose::STANDARD.encode(b"abc")
        );
    }
}
