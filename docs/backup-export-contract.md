# Android 本地备份导出契约（引擎批②，官方 v2 本地归档格式）

状态：MVP 契约基线（先文档后代码）｜ 依据：ADR 0001/0002/0005 ｜ 取证日期 2026-10-05

本文件固化引擎侧「官方 Android 备份格式导出」的格式契约、MVP 帧集裁定、
流式导出面 API 形状与验收分层。**硬验收目标：Android 端可恢复**（真机导入，
本批不可达，见 §9）。

## 1. 体系裁定：走 v2 本地归档，不做 v1

官方 Signal-Android 现行（main@b377bd21，2026-10-02 push）本地备份 = **v2 归档
体系**：

- 生成：`LocalArchiveJob` → `LocalArchiver.export`（`backup/v2/local/`），
  libsignal message-backup 帧格式 + `EncryptedBackupWriter` 加密流。
- 恢复：注册流程 `AppRegistrationStorageController.restoreLocalBackupV2(
  backupUri, aep)`——用户选目录、以**账号 AEP** 派生密钥、
  `LocalArchiver.canDecryptMainArchive` 校验后 `importLocal`。
- v1（`FullBackupExporter`/`BackupFrame`/SQL statement 帧，`.backup` 文件 +
  30 位密码）已不再是本地备份生成路径，仅存 `restoreLocalBackupV1` 遗留恢复
  入口。

裁定 **v2**，理由：

1. **Android 可恢复的现役入口**：恢复链路 `restoreLocalBackupV2` 是官方当前
   生成本地备份的同一格式的逆向；v1 无现役生成方，帧集（SQL statement）与本批
   MVP 帧集（account/conversation/message）完全对不上。
2. **任务帧集语义对位**：version/header ← `BackupInfo`（首帧，携带 version）；
   account ← `AccountData`；conversation ← `Chat`；message ← `ChatItem`。
3. **复用 backup5 批资产**：同一 libsignal 帧面 proto（本仓 `src/proto/` 已
   vendor 生成代码）与密钥体系（`libsignal-account-keys` 已依赖），导出为
   backup5 解密的对称面。

## 2. 取证锚点表

| 锚点 | 来源 | 路径 |
|---|---|---|
| 体系总体（v2 本地归档生成/恢复） | Signal-Android main@b377bd21 | `app/src/main/java/org/thoughtcrime/securesms/jobs/LocalArchiveJob.kt`、`backup/v2/local/LocalArchiver.kt`、`backup/v2/local/ArchiveFileSystem.kt` |
| 加密容器写面（IV/CBC/gzip/pad/HMAC、帧断块） | 同上 | `lib/archive/src/main/java/org/signal/archive/stream/EncryptedBackupWriter.kt`、`PaddedGzipOutputStream.kt` |
| 加密容器读面（HMAC 覆盖域、帧解码） | 同上 | `lib/archive/.../EncryptedBackupReader.kt`；libsignal tag v0.99.0（=439f035d9，本仓钉定）`rust/message-backup/src/frame.rs` |
| 官方导出原语（gzip→pad→CBC(PKCS7)→HMAC(iv‖密文)） | libsignal v0.99.0 | `rust/message-backup/src/export.rs`（test-util，仅语义参照）、`src/lib.rs::padded_length` |
| 密钥派生（AEP→BackupKey→BackupId→MessageBackupKey、新旧 DST、已知测试向量） | libsignal v0.99.0 | `rust/message-backup/src/key.rs`、`rust/account-keys/src/backup.rs` |
| 本地备份 metadata key | libsignal v0.99.0 | `rust/account-keys/src/backup.rs::derive_local_backup_metadata_key`（info `20241011_SIGNAL_LOCAL_BACKUP_METADATA_KEY`） |
| 本地目录形态与文件名（SignalBackups/signal-backup-<ts>/{metadata,main,files}） | Signal-Android main@b377bd21 | `backup/v2/local/ArchiveFileSystem.kt`（常量 MAIN_DIRECTORY_NAME/BACKUP_DIRECTORY_PREFIX/MAIN_NAME/METADATA_NAME/FILES_NAME） |
| Metadata/FilesFrame proto | Signal-Android main@b377bd21 | `lib/archive/src/main/protowire/LocalArchive.proto`（含 metadata key HKDF 注释） |
| 帧面 proto（Frame/AccountData/Recipient/Chat/ChatItem/StandardMessage…） | Signal-Android main@b377bd21 + libsignal v0.99.0 | `lib/archive/src/main/protowire/Backup.proto`；本仓 `src/proto/mod.rs`（v0.99.0 生成代码，字段号同构） |
| 帧序规则与官方导出帧序 | Signal-Android main@b377bd21 | `Backup.proto` 头注释（规则 1-4）；`backup/v2/BackupRepository.kt::export`（BackupInfo→AccountData→Recipient×N→Chat×N→AdHocCall→StickerPack→NotificationProfile→ChatFolder→ChatItem×N） |
| 导入消费面（未识别帧跳过、FK 校验、缺 ALL folder 自动补、version≤1） | Signal-Android main@b377bd21 | `backup/v2/BackupRepository.kt::import/importLocal` |
| validator 口径（local/linking → libsignal validate，purpose=REMOTE_BACKUP/DEVICE_TRANSFER） | Signal-Android main@b377bd21 | `backup/v2/ArchiveValidator.kt`；libsignal v0.99.0 `rust/message-backup/src/backup.rs`（CompletionError：恰一 AccountData、恰一 Self recipient、ALL folder 空集容忍/重复拒绝、contact e164/aci/pni/username 与 group master key 去重） |
| 恢复入口（AEP 派生 key + 选目录） | Signal-Android main@b377bd21 | `app/src/main/java/org/thoughtcrime/securesms/registration/v2/AppRegistrationStorageController.kt`（verifyLocalBackupKey/restoreLocalBackupV2） |
| 官方逐帧黄金样本 | Signal-Android main@b377bd21 | `app/src/androidTest/assets/backupTests/*.binproto`（本仓夹具：`tests/fixtures/signal-backup-tests/`，7 个） |
| 引擎侧 AEP 来源（链接时 provisioning 落库 presage store kv） | presage rev 33dd1491（本仓钉定） | `presage/src/manager/linking.rs`、`presage-store-sqlite/src/lib.rs`（kv key=`account_entropy_pool`） |

## 3. 密钥派生链（从账号材料到帧加密钥）

```text
AEP（AccountEntropyPool，64 字符小写字母数字；链接时经 provisioning 同步、
     presage store kv 落库（key=account_entropy_pool））
  │ BackupKey::derive_from_account_entropy_pool   [libsignal-account-keys]
  ▼
BackupKey（32B）
  ├─ derive_backup_id(ACI) ──────────────► BackupId（16B）
  ├─ derive_local_backup_metadata_key() ─► metadata key（32B，HKDF-SHA256，
  │     info="20241011_SIGNAL_LOCAL_BACKUP_METADATA_KEY"）
  └─ (BackupKey, BackupId, None) ─ MessageBackupKey::derive ─► { hmac_key 32B,
        aes_key 32B }    [None = local/linking 变体：DST
        "20241007_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:"，无 forward-secrecy
        token；远程备份才用 NEW_DST + token]
```

- 三步派生全部走官方 pub API（libsignal v0.99.0，本仓既有依赖），不自实现。
- 引擎侧 AEP 来源：presage store 既有材料（`fetch_account_entropy_pool`），
  导出过程内存即用即弃，不新增任何持久化；内部 API 同时允许调用方显式注入
  （后续 connector 消费批可不经 presage 供给）。
- 校验向量：libsignal `key.rs` 官方已知向量
  （FAKE_ACCOUNT_ENTROPY_POOL → FAKE_MESSAGE_BACKUP_KEY_LEGACY）。

## 4. main 文件容器格式（加密帧流）

无 magic（local/linking 变体；远程备份才有 `SBACKUP\x01` 前缀与明文元数据）：

```text
main = IV(16B, 随机) ‖ AES-256-CBC( aes_key, IV, PKCS7, payload ) ‖ HMAC-SHA256( mac_key, IV ‖ CBC密文 )

payload = gzip( varint32(len) ‖ Frame ‖  varint32(len) ‖ Frame ‖ … ) ‖ 0x00 * pad
pad     = padded_length(gzip长度) - gzip长度    [libsignal v0.99.0::padded_length：
         max(541, floor(1.05^ceil(log_1.05(len))))；gzip 尾部补零对 reader 透明]
```

- 帧序断块（官方 `EncryptedBackupWriter`：header/account/recipient 后
  FULL_FLUSH、chatItem 区周期 flush）是**隐私面**（块间压缩独立），非互操作
  语义——MVP 单 gzip 流 + 尾部 pad（v0.99.0 官方 export 原语同款语义），差异
  登记见 §8。
- reader 侧验证顺序（官方 `FramesReader`）：HMAC 全量验证 → IV → CBC 解密 →
  gzip 解压 → varint 分帧 → proto 解码。
- `BackupInfo.version`（帧面 schema 版本）= 1；Android import 要求
  `header.version ≤ 1`。

## 5. 目录形态（Android 恢复输入）

```text
<用户选目录>/SignalBackups/signal-backup-<yyyy-MM-dd-HH-mm-ss>(UTC)/
  metadata  = Metadata proto { version=1,
                backupId { iv(12B), encryptedId(16B) } }   // 明文 proto 帧（非加密容器）
                encryptedId = AES-256-CTR32BE(metadata_key, iv, BackupId 16B)
  main      = §4 容器
  files/    = 附件目录（MVP：空）
```

- 快照写入临时名 `signal-backup-<ts>-tmp` 完成后 rename 去掉 `-tmp`（官方
  原子化纪律，引擎对齐）。
- Android 恢复：用户选目录 → 找 `SignalBackups` 子目录（或目录本身）→ 列快照
  → metadata 解出 BackupId → main 以 messageBackupKey 解密（backupId 不匹配
  即解密失败）→ 逐帧导入。
- `files/` 与 `Metadata.FilesFrame` 索引：MVP 附件面不做（空目录、不写
  files 索引文件）；恢复端对缺失附件按缺媒体处理。

## 6. MVP 帧集裁定（Android 可恢复最小闭环）

帧序遵循官方 Backup.proto 规则：恰一个 `AccountData` 且为第一帧；被引用帧
（Recipient）先于引用帧（Chat/ChatItem）；ChatItem 全局渲染序（按接收序）。
未设置 oneof 的帧 importers 跳过不炸（proto 注释明示）；导入端外键违规会炸
事务——**引用完整性由引擎帧序状态机保证**（§7 API）。

### 6.1 MVP 帧集（本批实现）

| 帧 | 内容 | 依据 |
|---|---|---|
| `BackupInfo`（首帧） | version=1、backupTimeMs、mediaRootBackupKey(32B)、currentAppVersion/firstAppVersion | 官方 export 首帧；version≤1 硬校验 |
| `AccountData` | profileKey、givenName/familyName、bioText/bioEmoji；**settings 必带最小合法面**（libsignal validator 实测硬校验：settings 缺失= `MissingSettings`，phone sharing/sent media quality/app theme/calls use less data 四枚举 UNKNOWN=硬错）——引擎帧序状态机缺省补 NOBODY / HIGH / SYSTEM / WIFI_AND_MOBILE_DATA（Signal-Android 对应默认值），已带 settings 时未设置的同款补默认 | 官方 `AccountDataArchiveProcessor.export` + libsignal v0.99.0 `backup/account_data.rs` 硬校验口径（本批 validator 抽验实证） |
| `Recipient{self}` | 恰一个（validator 强制 `MissingSelfRecipient`/去重） | libsignal v0.99.0 CompletionError |
| `Recipient{contact}` | aci(16B)/e164/profileKey/profile 名；aci/e164/pni/username 去重（validator） | 同上 + 官方 ContactArchiveExporter 字段面 |
| `Recipient{group}` | masterKey（去重） | 同上 |
| `Chat` | id、recipientId（引用已登记 Recipient）、expireTimerMs/expireTimerVersion（有则带） | 官方 ChatArchiveExporter |
| `ChatItem{standardMessage}` | text.body；directional：`Incoming{dateReceived,read}` / `Outgoing{dateReceived}`；authorId（self 用 Self recipient id）；dateSent | 官方 ChatItemArchiveExporter 最小面 |
| `ChatFolder{all}` | ALL folder 一帧（validator 空集容忍、Android 端缺省自动补；带上以贴近官方档案并通过 validator） | BackupRepository.import 兜底 + validator |

### 6.2 全量帧面清单（官方存在，本批不做，标后续）

- 帧类：`StickerPack`、`AdHocCall`、`NotificationProfile`、自定义 `ChatFolder`、
  `Recipient{distributionList,callLink,releaseNotes}`。
- ChatItem 变体：`ContactMessage`、`StickerMessage`、`RemoteDeletedMessage`、
  `ChatUpdateMessage`（成员变更/计时器更新/群改名等）、`PaymentNotification`、
  `GiftBadge`、`ViewOnceMessage`、`revisions`（编辑）、`pinDetails`。
- StandardMessage 扩展：`Quote`、`Reaction`、`MessageAttachment`（附件指针 +
  files/ 附件导出 + FilesFrame 索引）、`LinkPreview`、`longText`（CDN 指针）、
  `SendStatus` 细粒度。
- AccountData 扩展：username/usernameLink、svrPin（**凭据，引擎边界外，永不
  导出**）、AccountSettings 全量（见引擎 `setAccountConfiguration` 各真相源
  字段）、自定义 ChatStyle/chatColors、AutoDownloadSettings、订阅数据。
- 附件媒体面：`MediaName`（plaintextHash+localBackupKey）、
  `AttachmentCipherOutputStream` 加密 + PaddingInputStream、files/ 目录与
  FilesFrame 索引。

## 7. 流式导出面（内部 API，本批不做 JSON-RPC 方法）

**裁定：内部 Rust API。** 理由：备份导出数据面 = 调用方逐条注入消息内容，
JSON-RPC 一次性携带全量消息违反流式边界，逐条注入需要跨请求会话状态机——
两者都属 connector 契约演进面（ADR 0002：兼容层封顶，新能力走独立扩展方法 +
契约版本），在 connector 消费批立项时按消费形状共同定形；本批先交付纯函数
库面（单测门禁），避免单方面发明 IPC 契约后返工。

```rust
// src/backup/keys.rs —— 密钥派生（官方 pub API 薄封装）
pub struct BackupKeys { /* backup_key/backup_id/message_key(hmac,aes)/metadata_key */ }
pub fn derive_keys(aep: &AccountEntropyPool, aci: &Aci) -> BackupKeys;

// src/backup/container.rs —— 加密容器（流式，O(1) 帧内存）
pub struct BackupStreamWriter<W: Write> { … }
impl<W: Write> BackupStreamWriter<W> {
    pub fn new(w: W, message_key: &MessageBackupKey) -> io::Result<Self>;  // 随机 IV + 写 IV 进 MAC
    pub fn write_frame(&mut self, encoded: &[u8]) -> io::Result<()>;       // varint32(len) ‖ Frame
    pub fn finish(self) -> io::Result<W>;   // gzip finish → pad → PKCS7 → HMAC 尾，还底层写面
}

// src/backup/frames.rs —— 帧构造 + 帧序状态机（引用完整性闸；违规 fail-closed）
pub struct FrameSequencer { … }   // BackupInfo→AccountData→Recipient*→Chat*→ChatItem*/ChatFolder
impl FrameSequencer {
    pub fn new(backup_info: proto::BackupInfo) -> Self;
    pub fn backup_info(&mut self) -> Result<proto::BackupInfo, FrameSequenceError>;  // 恰一次
    pub fn account_data(&mut self, account: proto::AccountData) -> Result<proto::Frame, _>;
    pub fn self_recipient(&mut self) -> Result<(u64, proto::Frame), _>;              // id=1 恰一次
    pub fn contact_recipient(&mut self, Contact) -> Result<(u64, proto::Frame), _>;  // id 自 2 递增
    pub fn group_recipient(&mut self, master_key: [u8; 32]) -> Result<(u64, proto::Frame), _>;
    pub fn chat(&mut self, recipient_id: u64) -> Result<(u64, proto::Frame), _>;
    pub fn chat_item_text(&mut self, …) -> Result<proto::Frame, _>;
    pub fn all_chat_folder(&mut self) -> Result<proto::Frame, _>;
}

// src/backup/export.rs —— 目录编排（metadata + main 两文件 + tmp→rename）
pub struct LocalBackupParams { /* keys: BackupKeys、backup_time_ms、media_root_backup_key(32B)、app_version */ }
pub struct MainStreamWriter<W: Write> { /* sequencer + BackupStreamWriter 合体，产出帧即写即计 */ }
pub fn write_metadata<W: Write>(w: W, keys: &BackupKeys) -> Result<(), BackupError>;  // CTR32BE(iv‖0u32be)
pub fn write_main_stream<W: Write>(w: W, params: &LocalBackupParams, seed: impl FnOnce(&mut MainStreamWriter<W>) -> Result<(), BackupError>)
    -> Result<(W, MainStreamStats), BackupError>;
pub fn export_local_backup(dir: &Path, params: &LocalBackupParams,
    seed: impl FnOnce(&mut MainStreamWriter<BufWriter<File>>) -> Result<(), BackupError>)
    -> Result<ExportReport, BackupError>;   // 同步函数（无 store/state/async 依赖）；final 已存在 → SnapshotExists fail-closed
```

- **不落库边界**：导出模块无任何 store/state 写入；消息内容只经内存进帧、
  加密、写出即弃；AEP/密钥不落日志不落盘。
- API 收敛为「调用方给什么导什么」：引擎不向 Signal 服务端拉取任何聊天数据。

## 8. 与官方 writer 的已知差异（登记）

1. **gzip 分块 flush 不做**：官方 main 分支在 header/account/recipient 帧后
   FULL_FLUSH、chatItem 区周期 flush（PaddedGzipOutputStream，依赖
   MessageBackupSizing——v0.99.0 无该 API）。reader 对块结构零假设（gzip 流
   等价、尾部补零透明），互操作不受影响；影响仅块间压缩独立性（隐私面），
   依赖升版后可补。
2. **padding 口径**：v0.99.0 `padded_length`（min 541）；Android main 已改
   MessageBackupSizing（64KiB floor + 高斯 pad）。同为隐私面，互操作等价。
3. **svrPin 恒不导出**（引擎不持有/不外传凭据；官方导出带用户 PIN——引擎侧
   恒空串，官方缺省形态）。
4. **附件媒体面 MVP 空**（§6.2 后续）。

## 9. 验收分层（如实）

| 层 | 内容 | 本批状态 |
|---|---|---|
| ① 引擎单测/往返 | 密钥派生对官方已知向量；容器自写→官方 `FramesReader`+`VarintDelimitedReader` 读回逐帧等价；每帧类型构造单测（官方 binproto 样本 round-trip）；帧序状态机违规拒绝；metadata id 加解密往返；MVP 样例档案全链导出 | **本批门禁**，全绿 |
| ② validator 语义抽验 | libsignal `BackupReader::validate_all`（Purpose::DeviceTransfer 与 RemoteBackup 双跑，ArchiveValidator.kt 同款口径）对 MVP 样例档案通过 | 本批抽验（能对齐多少记多少） |
| ③ 真机 Android 恢复 | 硬验收：同号真机注册 → `restoreLocalBackupV2` 选目录导入成功、会话/消息可见 | **本批不可达**——待 connector 消费批接消息数据面 + 真机；不算完成 |

### 真机验收清单（后续批执行）

1. 真机（同账号）注册完成，AEP 与引擎派生同源（SVR 恢复）。
2. 引擎导出目录拷入真机（`SignalBackups/signal-backup-<ts>/`，含
   metadata+main）。
3. 触发恢复入口选目录 → `verifyLocalBackupKey` 通过（密钥匹配）。
4. 导入成功（无 FK 异常/无 hasInvalidBackupVersion），会话列表与文本消息
   可见，自发/收信方向正确。
5. 附件/引用消息缺失按 §6.2 边界预期（无崩溃、缺省呈现）。
