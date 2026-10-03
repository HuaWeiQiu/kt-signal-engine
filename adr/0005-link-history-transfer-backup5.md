# ADR 0005：链接时存量历史迁移（backup5）自研立项

日期：2026-10-03 ｜ 状态：已接受（S1 PASS；S2 引擎侧已落地 0eedc8c，见 §5；下一步 S3 connector）

## 背景

产品决策（2026-10-03）：链接桌面后能看到手机 app 里已有的聊天记录内容（存量历史）是
**刚需**。当前边界：connector 只承诺自己持久化过的历史（connector implementation-plan
§6.6），存量会话/联系人骨架已由 connector bd891fc 启动补同步落地，但消息内容不回填。
pinned signal-cli 0.14.x 无此能力，官方桌面有。本文立项自研，并记录取证、阶段与风险。

## 取证（均为 2026-10-03 实查，来源标注）

### 1. 官方机制（Signal-Desktop v8.31.0-alpha.1 源码逐点核实）

1. 新设备链接二维码 URL 声明 `backup5` capability（`ts/textsecure/Provisioner.preload.ts`，
   仅当本机从未注册过才声明——`isLinkAndSyncEnabled` 以 registration 门控）。
2. 真机扫码后，`ProvisionEnvelope` 解出的配置信封携带 `ephemeralBackupKey`；手机侧把全量
   聊天打包为临时备份上传 Signal 备份服务。
3. 新链接设备用该钥匙从备份服务下载全量档案、解密逐帧导入本地库后才进主界面
   （`ts/services/backups/index.preload.ts`，`importFromDisk type:'remote'` + ephemeralKey）。
4. 官方自有约束（同样适用于我们）：手机必须在线配合；注册过的安装不可再导（防覆盖）。

### 2. 密钥与解析原语已在我们的依赖树内

- 官方 libsignal 多语言仓自带备份原语：`rust/message-backup` crate（帧解码/校验/生成）与
  `rust/account-keys/src/backup.rs`（`MessageBackupKey` 派生）。
- 这两处在我们传递钉定的 **v0.99.0** tag 即已存在（GitHub tree 实查），v0.103.1 同样存在；
  许可 AGPL-3.0-only 与本仓一致，直接以同 tag 加直接依赖即可，无升级连锁。
- 社区 PR AsamK/signal-cli#2134（GPLv3 Java，open、维护者零表态）在 Java 侧走通同一条链：
  `MessageBackupKey.deriveBackupSecrets(aci, null)` → `EncryptedHistoryReader`（分帧读档）→
  `messagebackup.BackupJsonExporter` 逐帧导出。**仅作行为参照，禁止抄码**（AGENTS.md 禁令，
  且 GPLv3 与 AGPL-3.0-only 单向不兼容）。

### 3. 待 spike 确认的三个未知数（阶段门槛，非阻塞项）

| # | 未知数 | 验证方式 | 失败后果 |
|---|---|---|---|
| a | 真机配合性：QR 带 `backup5` 后真机是否真的发起迁移（信封带 ephemeralBackupKey） | S1 真机取证：引擎 QR 加 capability，抓信封字段 + 档案可下载性 | 一票否决：整个 backup5 路线不成立，回落骨架方案 |
| b | 下载端点：libsignal-service-rs（presage 栈）无 `TransferArchive` 等价物 | S1 比对官方 libsignal-service Java 行为，按公开行为在引擎实现 REST | 无——本就是自研面 |
| c | presage 的 ProvisionMessage 是否暴露 `ephemeralBackupKey` 字段 | S1 编译期检查 | 无——presage 补丁（AGPL 合规）或引擎层自行解包信封 |

**S1 结果（2026-10-03）：三个未知数全部落定为可行**，详见 §4。

## S1 spike 真机取证记录（2026-10-03）

工具：`src/bin/backup5-spike.rs`（独立 bin，零改动现有链接链路；提交 3e7c77e）。方法：
绕过 presage `Manager::link_secondary_device`（其内部丢弃 `ephemeral_backup_key`），直调
`libsignal_service::provisioning::link_device`；在产出的 provisioning URL 上追加
`&capabilities=backup5` 生成二维码；真机扫码链接后捕获信封字段并设备鉴权下载档案。
全程不打印任何密钥值、不保存注册数据（store 落临时目录）。

证据三点（全部命中）：

1. **真机配合性（未知数 a）**：信封携带 `ephemeral_backup_key: Some([u8; 32])`——真机对
   第三方客户端 QR 声明的 `backup5` capability 照常发起迁移并上传备份。一票否决点通过。
2. **下载端点（未知数 b）**：`GET /v1/devices/transfer_archive?timeout=60`（设备鉴权，
   基础认证）返回 200 `{cdn: 3, key: "…"}`，随后 `GET {cdn3}/attachments/{key}` 200 下载
   成功。libsignal-service `PushService::request` + `Endpoint` 原样可用，无需任何上游改动。
3. **字段暴露（未知数 c）**：`NewDeviceRegistration.ephemeral_backup_key` 是 pub 字段，
   presage 只是高层封装丢弃它——**无需 presage 补丁**，引擎层直调 provisioning 层即可。

下载产物：6576 字节，落盘临时文件（sha256 `efb73508…0111c`，`file` 判为 data——加密档案，
解密留 S2）。档案量级与该 dev 账号消息量相符，仅作字节级验证。

边界实录：provisioning WS 空闲约 90 秒被服务端掐断（第一次扫码超时即此），S2 落地时
链接会话须有界并在超时后整会话作废重来，不得复用旧 QR。

## S2 设计定案（2026-10-03，实现前落档）

### 解密与分帧（全部官方 pub API，v0.99.0 `libsignal-message-backup`）

- 密钥派生（对齐官方 node bridge `from_backup_key_and_backup_id` 语义）：
  `BackupKey(ephemeral_backup_key)` → `derive_backup_id(&aci)` →
  `MessageBackupKey::derive(&key, &id, None)`（forward-secrecy token 为 None = 官方桌面
  同款 DST）。
- 解密面：`frame::FramesReader::new(&key, FileReaderFactory)`（官方：HMAC-SHA256 尾验证 +
  AES-256-CBC 解密 + gzip 解压 + padding 剥离，实现为 `AsyncRead`）→
  `parse::VarintDelimitedReader::read_next()` 循环（官方 varint 分帧）。首帧 `BackupInfo`，
  后续帧为 `backup.Frame`。
- 帧 proto：engine 自持 `backup.proto` 副本（官方开源协议 schema，1466 行，prost-build
  编译；协议定义非上游源码）。**不启用 `test-util`/`json` feature**（proto pub 面与 JSON
  导出面仅限 fixture 构建 dev-dependency）。
- 导出：逐帧流式转 receive-compatible 中间表示（NDJSON），O(1) 帧内存，符合 ADR 0002
  bounded 纪律。v1 范围：`Chat`（会话归属）、`ChatItem`/`StandardMessageItem`（text +
  附件 metadata（名称/大小/contentType，**不下载字节**）+ quote）、`RemoteDeletedMessage`；
  其余 item 类型（call/poll/sticker/viewOnce/payment/groupUpdate）计数跳过并上报。

### 链接集成（零 fork）

- presage `Manager::link_secondary_device` 内部丢弃 `ephemeral_backup_key`，且
  `RegistrationData.password/profile_key` 为 `pub(crate)` 无法直接构造。定案：engine 链接
  任务绕过该 Manager 入口，直调 `libsignal_service::provisioning::link_device`（S1 spike
  同款），拿到 `NewDeviceRegistration`（含 key）后，**经 serde 数据面构造
  `RegistrationData`**——该结构 `#[derive(Serialize, Deserialize)]`，serde 格式即 presage
  官方 store 落库格式（`SqliteStore::save_registration` 同源）：`signal_servers` 枚举名、
  `phone_number` E.164 串、`service_ids` flatten（aci/pni UUID）、`profile_key` 标准
  base64（serde_profile_key）。构造后 `store.save_registration` +
  `Manager::load_registered` 回到既有收发管线，后续逻辑零改动。
- 该路线零 fork、零 upstream 等待；格式稳定性等价 store 格式稳定性（presage 有 store
  版本迁移机制）；rev 钉定无漂移。往返单测锁死格式。
- QR 声明 `capabilities=backup5`（官方语义：链接新设备=从未注册，`isLinkAndSyncEnabled`
  门控恒真）；key 捕获失败/档案下载失败均 best-effort 不阻塞链接成功。

### 测试与 fixture

- 解密分帧往返：用官方 `export` 原语（gzip→pad→AES-CBC→HMAC，test-util feature 仅入
  dev-dependencies，不进运行时依赖图）构造加密备份 fixture，验证
  FramesReader→分帧→proto 解析→NDJSON 导出全链路。
- serde 构造：`RegistrationData` 序列化→反序列化往返单测（拿 spike 真机数据形态）。

## 决策

在 **kt-signal-engine** 自研 backup5 接收侧（链接能力声明、信封钥匙截获、迁移档案下载、
解密逐帧导出 receive-compatible 中间表示），connector 消费导入存量消息。**不 fork
signal-cli、不抄 signal-cli/官方桌面源码**；libsignal 原语按 v0.99.0 tag 直接依赖。
上游 AsamK/signal-cli#2134 保留为跟踪项：其一旦合入发版，重新评估「升级 pinned signal-cli
替代 S2/S3」是否更省——自研成果不浪费（engine 路线本就是替换 signal-cli 的主线，ADR 0003）。

## 阶段（README 阶段规划挂载点）

- **S1 spike（✅ 2026-10-03 完成，取证记录见 §4 上一节）**：真机一次会话闭环取证——QR 加
  `backup5` → 信封 `ephemeralBackupKey` 存在性 → 档案下载字节级成功（不解密）。
- **S2 引擎（✅ 2026-10-04，提交 0eedc8c）**：下载 + 解密 + 逐帧导出（`message-backup`
  crate）；附件 metadata-only（v1 不下载字节，沿 §6.7 哲学）。实现要点与定案的偏差
  记录：`RegistrationData` 的 phone_number serde 为 phonenumber crate derive 的嵌套
  对象（非预估的 E.164 串，以实查为准）；profile_key 借用反序列化限定 from_str 路径
  （from_value 直接报 borrowed string）；导出面 future 非 Send（上游 FramesReader 持
  Rc），下载任务走 spawn_local 与其他 presage 任务同策略。
- **S3 connector**：契约扩展（link.finish 携带导入结果或独立 history.import 状态方法，
  契约先行改 schema + implementation-plan §6.6/§6.5），store 有界批量导入（幂等、
  bounded、按 sent_at 去重），空骨架就地充实。
- **S4 桌面 + 真机验收**：导入进度/失败诚实态 UI，真机回归进 parity matrix §6.4。

## 风险与边界

- 仅首次链接可导；重链/已注册安装不可再导（官方同款语义，需在 UI 诚实呈现）。
- 手机侧版本兼容：`backup5` 是官方 2025 能力，旧手机 app 无此行为——spike 用最新真机。
- 迁移档案规模有界处理：分帧流式，禁全量驻内存（沿 ADR 0002 bounded 纪律）。
- 升级连锁：若 S2 需要新于 v0.99.0 的 libsignal 行为，按 ADR 0003 的 git-pin 模式升 tag，
  不走 crates.io。

## 备选否决记录

| 备选 | 否决理由 |
|---|---|
| fork signal-cli 打补丁 | GPLv3 维护面大、许可单向不兼容、与「engine 替换 signal-cli」主线冲突 |
| 等 upstream #2134 | 社区 PR、维护者零表态，时间不可控；保留跟踪 |
| 维持骨架方案 | 产品已定刚需，否决 |
