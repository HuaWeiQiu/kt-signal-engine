# Android 本地备份导出契约（引擎批②，官方 v2 本地归档格式）

状态：MVP 契约基线（先文档后代码）＋消费批一期接线（§10，2026-10-05）｜
依据：ADR 0001/0002/0005 ｜ 取证日期 2026-10-05

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
| `Recipient{group}` | masterKey（去重）；**snapshot 必带最小合法面**（消费批 validator 实证：缺席=硬错 `MissingSnapshot`）——引擎无群元数据真相源（§6.2 群全量面不做），状态机投影为空 avatarUrl/零 version/空 members 的占位快照（官方 group.rs 验收面全绿） | 同上 + libsignal v0.99.0 `backup/recipient/group.rs` 硬校验口径 |
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
（消费批一期的定形结论见 §10：文件工件交接 + `backup.export`/
`backup.exportStatus`。）

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

### 真机验收补充调研（2026-10-05，消费批一期落档；官方依据逐条给出）

6. **AEP 同源校验实操步骤**（官方依据：Signal-Android
   `AppRegistrationStorageController.kt`，main 现行——`verifyLocalBackupKey(
   backupUri, aep)` = `LocalArchiver.canDecryptMainArchive(
   SnapshotFileSystem(context, backupDir), aep.deriveMessageBackupKey())`，
   即**恢复端用自己持有的 AEP 现场派生 message backup key 试解 main**，密钥
   不同源唯一表现 = `IncorrectCredential`；恢复成功后
   `SignalStore.account.restoreAccountEntropyPool(aep)`——官方注释明言「输入
   的恢复密钥能解用户所选备份，故它总成为该账号的 AEP」）。实操步骤：
   ① 引擎所在桌面与真机为同一账号的链接关系（AEP 链接时经 provisioning
   同步，引擎从 presage store kv 读到的就是手机派发的那份——同源由链接协议
   保证）；② 真机进入注册流的「Restore or Transfer Account」→「From Signal
   Backups」（官方 support「Signal Secure Backups」#9708267671322 载明入口）；
   ③ 选目录（SAF；`scanLocalBackupFolder` 自动认 `SignalBackups` 子目录）→
   ④ 输入恢复密钥（64 字符）或同机未清数据时用本机 AEP →
   `canDecryptMainArchive` 通过即同源证明；失败形态只有 IncorrectCredential
   （密钥不同源）与 Error（导入层问题），无第三态。
7. **非 SVR 场景 AEP 一致性**（官方依据同上文件）：AEP 与 SVR 是两个独立
   体系——`commitRegistrationData` 里 PIN/SVR 分支（`SvrRepository.
   onRegistrationComplete`）与 AEP 落库（`applyAccountEntropyPool`）互不依
   赖；链接设备路径 `setAccountEntropyPoolFromPrimaryDevice` 直接采用主设备
   派发的 AEP，无需 SVR。真机重注册（清数据+同号短信）且无 PIN 时，旧 AEP
   服务器侧不可恢复，官方唯一入口是**手动输入 64 字符恢复密钥**（恢复密钥
   即 AEP；`restoreLocalBackupV2` 采纳输入密钥为账号 AEP）。结论：非 SVR
   真机验收必须走「同机未清数据（本机 AEP 原生在位）」或「原设备官方 UI 展
   示恢复密钥后手动输入」两种形态之一；引擎侧纪律不变——备份密钥不离开引擎
   进程（§7/§10.5），恢复密钥由用户经官方 UI 通道自持。
8. **备份文件送达真机方式**（官方依据：support「Android On-device
   Backups」#10066926526362——「Move the backup folder onto the internal
   storage of the device you'd like to restore to. Install Signal to enter
   the registration flow. This is where the restore option will be
   presented.」；`scanLocalBackupFolder` 实现：选中的目录含 `SignalBackups`
   子目录则自动下沉；V2 快照目录名必须逐字匹配
   `signal-backup-yyyy-MM-dd-HH-mm-ss`，**且父层必须存在 `files` 目录**，
   否则 V2 快照不被列出）。官方路径即「用户把整个快照目录拷入手机内部存储
   → 注册流选目录」，无推送通道；本引擎 MVP 导出形态（含空 `files/` 目录、
   官方同名规则 UTC 目录名）与该扫描面完全兼容。送达介质（USB/网盘/直通
   目录）官方未限定。

## 10. 消费接线（connector 消费批一期，2026-10-05）

本节固化 connector 消费批一期的接线裁定（契约先行，随后引擎实现批落地）。
§7 的「IPC 形状留给消费批共定」由此收敛。

### 10.1 IPC 形状裁定

**文件工件交接 + 引擎后台任务 + connector 轮询**：

```text
connector（store 真相源）
   │ ①把消息面导出为 ndjson 工件，写入引擎按账号的数据目录
   ▼
backup-export-input.ndjson（过渡性工件，§10.2）
   │ ②connector 调 backup.export 启动；引擎后台任务读工件→建帧→落快照
   ▼
SignalBackups/signal-backup-<ts>/（快照，官方 §5 形态）
   │ ③connector 轮询 backup.exportStatus；终态后经 backups.exportFile.*
   ▼   三连把 metadata/main 取回（connector 契约 1.47 §4.46 计划）
kt-desktop / Android 真机
```

理由：①备份模块刻意零 store 依赖（§7），消息真相源在 connector sqlite，
引擎侧无从直读；②引擎 JSON-RPC 单帧帽 160 MiB（compat §2）按单条消息
设计，内联全量消息 RPC 不可行；③文件工件有 backup5
`history-import.ndjson` 先例（引擎出方向、connector 消费，connector 契约
§4.38），本批是同一通道的反方向，先例形状
（存在性判定 + 诚实计数 + 轮询状态面）整套复用。

### 10.2 输入工件（过渡性，硬边界）

- **路径**：`<data-dir>/<account-rel>/backup-export-input.ndjson`。
  `<account-rel>` = engine-state.json 注册表里该号码的 `dir`（即 presage
  store 目录 `accounts/lk-<hex>`，compat §1）。与 backup5 出方向的
  `history-import.ndjson` 同目录（connector 契约 §4.38 同一解析路径），
  connector 两侧用同一注册表映射定位。
- **过渡性（硬边界，engine AGENTS.md:33-34）**：工件是 connector 写入的
  明文聊天内容，引擎不持久化——**导出任务离开 running 态（成功或任何失败）
  时引擎必须删除工件**；start 阶段同步拒收（工件缺失/首行非法/任务已在跑）
  时：非法工件（首行不可解析为 account）当场删除，缺失/已在跑无工件可删。
  任务仍在 running 或引擎进程崩溃中断时工件暂存，`backup.exportStatus` 以
  `artifactPresent` 如实报告，下一次导出尝试终态时删除。快照目录不在本条
  约束内（快照是密文，生命周期归 connector，§10.6）。
- **行格式**（D3）：一行一个 JSON 对象，`type` 判别字段；首行必须且仅一行
  account；16B 字段 = 32 位小写 hex、32B 字段 = 64 位小写 hex；对象内未列
  出的键一律拒收（fail-closed，引擎不发明字段、不静默忽略）。引用规则：
  连接器自选 `u64 ≥ 1` 作行引用 id（contact/group 的 `id`、chat 的 `id`），
  `0` 保留 = 引擎 self recipient（不可声明为 id）；被引用行必须先于引用行
  （帧序状态机强制，违反即任务失败）。

| type | 字段（? = 可选） | 对应引擎输入 |
|---|---|---|
| `account` | `profileKey`(hex64 必填)、`givenName`?、`familyName`?、`bioText`?、`bioEmoji`? | `account_data(AccountData)`；**必须为首行且仅一行** |
| `contact` | `id`(u64≥1)、`aci`?(hex32)、`pni`?(hex32)、`e164`?(u64≠0)、`username`?、`profileKey`?(hex64)、`givenName`?、`familyName`?、`registration`?("registered"\|"unregistered"；**缺省 = "registered"**——官方 validator 对缺席 registration 硬错 `ContactRegistrationUnknown`) | `contact_recipient(Contact)`；aci/pni/e164 至少一项；givenName/familyName → `profileGivenName`/`profileFamilyName`（Contact 无 bio 字段，bio 只属于 account 行）；`unregistered` 由引擎盖 `unregisteredTimestamp = backup_time_ms`（官方 validator 对 NotRegistered 的非零时间戳口径） |
| `group` | `id`(u64≥1)、`masterKey`(hex64 必填) | `group_recipient(master_key)` |
| `chat` | `id`(u64≥1)、`recipient`(引用已声明 contact/group id 或 0=self) | `chat(recipient_id)` |
| `chat_item` | `chat`(引用已声明 chat id)、`author`(引用已声明 contact id 或 0=self；group 不能作 author——官方 v2 归档群消息署名到个人，帧序状态机 `InvalidChatAuthor` 拒绝)、`dateSent`(u64 ms)、`direction`("incoming"\|"outgoing")、`dateReceived`(u64 ms 两方向都必填)、`read`?(bool，仅 incoming 有意义，缺省 false)、`body`(非空 str，≤128 KiB) | `chat_item_text(chat_id, author_id, date_sent_ms, direction, body)` |
| `all_chat_folder` | （无字段） | `all_chat_folder()`；可省略——缺省时引擎收尾自动补一帧（validator 通过面，§6.1），出现则恰一次 |

- self recipient 不是行类型：引擎在 account 行之后自动发出（恰一次、id=1，
  frames.rs 常量）。chat 的 `recipient: 0` 即 Note to Self。
- **引擎侧永不静默丢行**：未知 type、顺序违规、引用未声明、id 重复、id=0
  声明、hex 非法、body 空/超长、JSON 非法——一律任务失败
  `BACKUP_ARTIFACT_INVALID`（§10.7）。跳行决策只属于 connector 建工件时
  （其契约 §4.46 逐因计数），引擎只做「全部成帧或整体失败」。
- **帽值**（写死，两端文档与实现一致；超帽 fail-closed 同上）：

| 帽 | 值 | 依据 |
|---|---|---|
| 单行字节（含换行前） | 1 MiB = 1,048,576 B | 128 KiB body 的 JSON 转义最坏膨胀（~6×）后仍有余量；独立于引擎 JSON-RPC 帧帽（160 MiB，compat §2），只约束本 NDJSON 工件 |
| 总行数 | 4,000,000 行 | connector store 有界保留（2000 行/会话 ×8 会话帽 ×冗余系数）之上的纯 fail-closed 闸 |
| 工件总字节 | 2 GiB = 2,147,483,648 B | 同上， runaway 文件熔断 |
| chat_item body | 128 KiB = 131,072 B | frames.rs validator 前置（§7 API） |
| chat/contact/group id | u64，可声明域 ≥1；0 保留 self | §10.2 引用规则 |
| exportStatus 快照清单 | ≤32 条，新→旧 | 响应有界 |

### 10.3 任务模型与执行机制（含接收管线活性）

- **每账号同时至多一个导出任务**：重复 `backup.export` → 结构化错误
  `BACKUP_EXPORT_ALREADY_RUNNING`（§10.7）。任务注册表 = 引擎进程内存
  （HashMap<account, record>），**不落盘、不跨进程重启**——引擎重启后状态
  归 `idle`，快照仍在盘上（exportStatus 照实报告清单），半途工件按 §10.2
  处理。
- **状态机**：`running → completed | failed`（start 校验失败不留状态，同步
  回错）。`idle` = 本进程无该账号任务记录。
- **执行机制裁定：专用 OS 线程（`std::thread`）跑导出，密钥派生在
  LocalSet**。依据：
  1. 备份装配链（读工件 → `MainStreamWriter` → `export_local_backup`）是
     纯同步阻塞代码（§7 无 store/state/async 依赖），在 LocalSet 上直跑会
     饿死同一线程上的接收管线与请求 dispatch——**导出不得阻塞接收管线是
     硬要求**。
  2. 装配链全部输入是 `Send` 数据：`BackupKeys` = 纯字节数组聚合
     （`BackupKey([u8;32])`/`BackupId([u8;16])`/`MessageBackupKey{[u8;32],
     [u8;32]}`，libsignal v0.99.0 定义面），工件路径/参数/统计句柄均为
     `Send` 标准类型——整个任务闭包 `Send + 'static`，可安全移出 LocalSet。
     presage 的非 Send 面（Manager/接收流）只在 start 阶段的 AEP 读取用到，
     在 LocalSet 内完成（`fetch_account_entropy_pool` + registration data
     读回），派生出的密钥字节独占移交线程。
  3. 不用 `tokio::task::spawn_blocking`：任务完成通知走共享状态
     （exportStatus 轮询语义，无 async 唤醒需求），专用线程免运行时池耦合，
     与「备份模块零 tokio 依赖」的分层一致（线程体只触 std + backup 模块）。
  4. **活性证据**：导出期间接收管线（spawn_local 于同一 LocalSet）与请求
     dispatch 不被阻塞——由引擎测试锁定（任务启动后 LocalSet 上的独立任务
     持续推进、注册表状态自 `running` 起可被并发轮询直至终态）。

### 10.4 RPC 面（独立扩展方法，compat §3/§5 同步登记）

- `backup.export`：params `{account}`。start 校验（顺序）：账号已注册（注册
  表有 store 目录；fake/未注册 `-32603`）→ 工件存在（缺
  `BACKUP_ARTIFACT_MISSING`）→ 首行可解析为合法 account 行（否则删工件 +
  `BACKUP_ARTIFACT_INVALID`）→ AEP 可读（presage store kv；缺 `-32603`，
  导出不可为）→ 无在跑任务（`BACKUP_EXPORT_ALREADY_RUNNING`）。成功即后台
  执行，回 `{"status": "started"}`。
- `backup.exportStatus`：params `{account}`。result：

```json
{
  "account": "<echo>",
  "state": "idle|running|completed|failed",
  "artifactPresent": false,
  "startedAtMs": 0, "finishedAtMs": 0,
  "lines": 0, "frames": 0,
  "snapshot": {"name": "signal-backup-…", "mainBytes": 0, "frames": 0},
  "error": "BACKUP_ARTIFACT_INVALID",
  "snapshots": [{"name": "…", "mainBytes": 0, "frames": 0}]
}
```

  `lines`/`frames`/`snapshot`/`error`/时间戳仅在状态有对应物时出现（诚实
  缺席，不发明 0）；`snapshot.frames` 仅本进程任务产出的快照已知帧数，历史
  快照只报 `name`/`mainBytes`（不重读密文）。`snapshots` = 快照根目录清单
  （`signal-backup-*` 非 `-tmp`，新→旧，≤32 条），**引擎只报告不清理**
  （§10.6）。未注册账号 `-32603`（其他账号域方法同口径）。
- 两方法均不落调度日志、不产 stderr（贴纸/置顶面同款纪律）。

### 10.5 密钥边界

- AEP 从 presage store kv 读（`fetch_account_entropy_pool`），ACI 从
  registration data 读，`derive_keys`（§3）在 start 阶段 LocalSet 内完成；
  `keys.rs` 的显式注入钩子（`LocalBackupParams.keys`）保留——测试与后续
  批可不经 presage 供给。
- **备份密钥绝不离开引擎进程**：任何 RPC 不返回密钥材料；导出只交付文件，
  Android 端用自己的 AEP 现场恢复（§9 补充调研 6）。密钥不落日志不落盘
  （§7 纪律延续），线程即用即弃。
- `media_root_backup_key`：引擎无既定持久来源，**每导出随机生成 32B** 并
  在此登记。MVP `files/` 为空（§5），无媒体解密影响；**未来附件批必须重
  审**（官方语义是账号恒定值，随链接 provisioning 的 linkedDeviceData 分发
  ——Signal-Android `applyAccountData`；届时改为随工件携带或持久化）。
  **2026-10-08 翻案记档（云备份 upload 批，cloud-backup-contract §6.4）**：
  重审完成——官方实锤为「账号恒定、客户端持久随机」（桌面
  `crypto.preload.ts:33-39` itemStorage 取用、非派生），本节「随机生成」
  与官方语义相悖确认成立；引擎侧因持久落点缺席（presage 无通用 kv、
  provisioning 分发面未立项）+「密钥不落盘」纪律，v2 upload 产物维持
  随机生成并如实登记差异（messages 档无媒体帧引用，消费端无影响），
  恒定值落点归附件批。

### 10.6 快照生命周期

- **清理所有权 = connector**（履行 §5/`export_local_backup` 预留）：引擎
  除删除输入工件（§10.2）外不自删任何快照。策略：**保留最近一次快照，
  connector 在下次导出启动前清理更旧的快照**（含异常残留的 `-tmp` 目录——
  引擎失败路径已自清，崩溃残留由 connector 兜底）。
- 引擎在 `backup.exportStatus.snapshots` 报告清单（目录名/main 字节数/
  已知帧数）供 connector 决策；同秒两次导出会触发 `SnapshotExists`
  fail-closed（§7），由 connector 清理后重试，引擎不覆盖旧快照。

### 10.7 结构化错误码（connector 契约 1.47 §4.46 白名单计划对齐）

全部 `-1` + message 码位（§4.32/§4.33 既有形状，connector 逐字入白名单；
白名单登记归 connector 二期）：

| 码 | 面 | retryable（connector 映射计划） |
|---|---|---|
| `BACKUP_EXPORT_ALREADY_RUNNING` | start：该账号任务在跑；先轮询 exportStatus | false |
| `BACKUP_ARTIFACT_MISSING` | start：工件不存在（connector 先建工件） | false |
| `BACKUP_ARTIFACT_INVALID` | start 首行非法（工件当场删除）/运行中任何行级或帧序违规（§10.2 永不静默丢行） | false |
| `BACKUP_SNAPSHOT_EXISTS` | 运行：同秒快照目录冲突（§10.6，清理后重试） | true |
| `BACKUP_IO_FAILED` | 运行：文件系统失败 | true |

参数面（缺 `account`、未注册/fake 账号、AEP 缺失）不占用结构化码，一律
`-32603` fail-closed（置顶/贴纸面先例）。

## 11. 快照导入收向（官方桌面本地备份导入，B2 批，2026-10-08）

本节固化官方 Signal Desktop 本地备份快照的引擎收向面（`backup.importSnapshot`
/`backup.importSnapshotStatus`）：§10 是引擎**产出**快照（Android 恢复方向），
本节是引擎**消费**官方桌面产出的快照（connector → NDJSON → connector sqlite
的既有导入通路），两者共用 §3 密钥派生链与 §5 快照目录形态，方向互逆。

### 11.1 官方锚点（AGPL 只读参照零拷贝）

| 锚点 | 官方位置 | 语义 |
|---|---|---|
| 快照目录布局 | `SignalBackups/signal-backup-<yyyy-MM-dd-HH-mm-ss>/{metadata, main, files/}` | §5 同形态（本地变体无 `.bak` 后缀） |
| `importLocalBackup` | `ts/services/backups/index.preload.ts:572-595`（v8.31.0-alpha.1） | importFromDisk(main, `local-encrypted`) 后跑 metadata 校验 |
| `verifyLocalBackupMetadata` | `ts/services/backups/util/localBackup.node.ts` | 读 `<snapshotDir>/metadata` → decode `Signal.backup.local.Metadata` → strict `version === LOCAL_BACKUP_VERSION(1)` → backupId/iv/encryptedId 在场、iv 长 `LOCAL_BACKUP_BACKUP_ID_IV_LENGTH(12)` → AES-CTR 解密（计数器块 = iv(12B)‖4B 零）→ `Bytes.areEqual(backupId, 解密值)` |
| metadata proto | 官方 `Metadata.proto`（本仓自持生成码 `src/proto/local_archive.rs`） | `Metadata{version, backupId{iv, encryptedId}}` |
| main 容器 | 官方 `FramesReader`（HMAC-SHA256 尾验证 + AES-256-CBC + gzip + padding） | 与 §4 同原语，读方向 |

### 11.2 KT 裁定

- **校验序前移**：官方在 importFromDisk 内并行跑 metadata 校验与 main 解密；
  KT 把 metadata 校验（CTR 16B，微秒级）前移为 start 同步闸——错目录/错账号
  （`KeyMismatch`）当场短路，不空转解密整个 main。main 全链（解密/分帧/
  HMAC/投影）留后台任务。语义面与官方一致，顺序是性能裁定。
- **NDJSON 产物与存量历史迁移同一落点**：`<account_dir>/history-import.ndjson`
  （connector `ARCHIVE_FILE_NAME` 消费面，§4.38 通路）。逐帧投影复用
  `history::export_frames_loop`（transfer archive 导出与快照导入同一帧循环，
  行形状逐字一致），connector 的两遍有界导入零改动消费。落盘用临时名
  `history-import.ndjson-tmp` 写全后 rename 原子替换（POSIX rename 不打断
  并发读）；任务失败删半成品、既有 NDJSON 不动——失败形态即 NDJSON 缺席或
  旧内容，connector 按文件存在性判定。
- **执行机制：`spawn_local`（与 §10.3 专用线程不同）**：帧导出走官方
  `FramesReader`（async，内部持 Rc 非 Send 状态），future 进不了 OS 线程
  边界，与接收管线/transfer 下载同策略由 LocalSet 承载（dispatch 本就跑在
  LocalSet 内）。NDJSON 写出是每行小块 io，不构成 §10.3 意义上的阻塞面。
- **快照目录路径由 connector 传入**（`snapshotDir` 参数，同机路径交接，
  §10.2 artifact 反方向同款）；引擎不持久化该路径（任务注册表无此字段）。
- **metadata_key 派生面登记**：官方派生链里 `metadata_key` 只依赖 AEP
  （`BackupKey.derive_local_backup_metadata_key()` 不掺 ACI），`backup_id`
  依赖 (AEP, ACI)——同 AEP 异账号的快照解密可解（keystream 相同）但
  backupId 比对必不等，`KeyMismatch` 走的是比对而非解密失败路径。

### 11.3 RPC 面（compat §3/§5 同步登记）

- `backup.importSnapshot`：params `{account, snapshotDir}`。start 校验序：
  账号已注册（fake/未注册 `-32603`）→ AEP/ACI 可读（presage store，缺
  `-32603`）→ metadata 快校验（缺目录/metadata/main → `MISSING`；decode
  失败/version≠1/iv≠12B/encryptedId≠16B/缺 backupId → `METADATA_INVALID`；
  backupId 比对不等 → `KEY_MISMATCH`）→ 无在跑任务（`ALREADY_RUNNING`）。
  成功即后台执行，回 `{"status": "started"}`。
- `backup.importSnapshotStatus`：params `{account}`。result：

```json
{
  "account": "<echo>",
  "state": "idle|running|completed|failed",
  "ndjsonPresent": false,
  "startedAtMs": 0, "finishedAtMs": 0,
  "lines": 0, "frames": 0,
  "error": "BACKUP_SNAPSHOT_IO_FAILED"
}
```

  `lines` = NDJSON 投影行数（recipient+chat+message）；`frames` = 档案总帧数
  （含 BackupInfo 元帧）；其余诚实缺席纪律同 §10.4。`ndjsonPresent` 是盘面
  真值（引擎重启后 state 归 idle 而 NDJSON 仍在场，消费方自行判定重导）。
  未注册账号 `-32603`。不落调度日志零 stderr。

### 11.4 结构化错误码

全部 `-1` + message 码位（connector 1.78 白名单同步）：

| 码 | 面 | retryable（connector 映射） |
|---|---|---|
| `BACKUP_IMPORT_ALREADY_RUNNING` | start：该账号导入任务在跑 | false |
| `BACKUP_SNAPSHOT_MISSING` | start/运行：目录或 `metadata`/`main` 缺 | false |
| `BACKUP_SNAPSHOT_METADATA_INVALID` | start：decode 失败/version≠1/形状不合规 | false |
| `BACKUP_SNAPSHOT_KEY_MISMATCH` | start：backupId 比对不等（非本账号备份） | false |
| `BACKUP_SNAPSHOT_IO_FAILED` | 运行：main 解密/HMAC/分帧/落盘失败（半成品删，既有 NDJSON 不动） | true |

参数面（缺 `account`/`snapshotDir`、未注册/fake 账号、AEP 缺失）一律
`-32603` fail-closed（§10.7 同口径）。

### 11.5 密钥边界（§10.5 延续）

导入侧密钥材料（AEP → `derive_keys` → 校验/解密）全部活在 start 校验序与
任务 future 内，任务结束即 drop：不落日志、不落盘、不出进程。metadata 校验
解出的 backupId 是派生值的回显，不构成新的持久化面。
