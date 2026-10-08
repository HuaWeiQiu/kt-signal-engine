# 云备份恢复契约（Cloud Backup Restore）——引擎侧

状态：草案 v1（2026-10-08，主 agent 三轮官方取证定稿；代码批未开工）
关联：`adr/0005`（backup5 传送链）、`src/backup/keys.rs`（AEP 派生单点）、
`docs/jsonrpc-compat.md`（本契约方法落 compat 的节待 813 在途批落地后并入，暂以本文档为契约真相源）
取证基线：官方 Signal-Desktop v8.31.0-alpha.1（端点/文件：行见各条）+ 引擎 pin libsignal tag v0.99.0

## 0. 依赖新增报备（AGENTS.md 纪律）

- `libsignal-net`、`libsignal-net-chat`：`git = "https://github.com/signalapp/libsignal", tag = "v0.99.0"`（与既有
  `libsignal-message-backup`/`libsignal-account-keys` 同源同 tag，Cargo.lock 已解析至同 commit）。
  用途：chat 未鉴权 gRPC-over-WS 面（`get_backup_cdn_credentials`/`get_message_backup_info`，
  官方 `rust/net/chat/src/grpc/backups.rs:284/:436`）。许可 AGPL-3.0-only，与本仓一致。
- 连接构造：`libsignal_net::chat::ChatConnection` → `Unauth<ChatConnection>`（`GrpcServiceProvider`
  对可 Clone GrpcService 自动实现，`net/chat/src/grpc.rs:87-99`）；官方同构先例
  `net/chat/src/registration/service.rs:16-73`。连接参数（chat.signal.org WS endpoint/TLS/keepalive）
  实施首日以 libsignal-net connect 入口实测钉定（本契约唯一机械未知项）。

## 1. 凭据链（全部官方实锤）

1. **签发**：`GET v1/certificate/auth/group?redemptionStartSeconds=&redemptionEndSeconds=`
   （官方 WebAPI.preload.ts:795）——7 天窗，响应同携群凭据与 `callLinkAuthCredentials`；
   云备份 restore 只取 messages 档备份凭据族（`WebAPI:1339` response schema）。
2. **bootstrap 自愈**：凭据请求 404 = backupId 未设置（**非付费、非错误**）——
   `PUT v1/archives/backupid`（WebAPI:813），body
   `{messagesBackupAuthCredentialRequest: b64, mediaBackupAuthCredentialRequest: b64}`
   （两族 request context 序列化，zkgroup `BackupAuthCredentialRequestContext::new::<V>(backup_key, aci)`
   → `.get_request()`），成功后重试①。引擎每账号一次性引导，成功标记落 store。
3. **兑换呈现**：`context.receive(response, &GenericServerPublicParams, expected_redemption_time)`
   → `credential.present(server_params, randomness)`（zkgroup `auth_credential.rs:211/257` 实读签名）。
   GenericServerPublicParams 固定常量（实施时在 pin 树 constants 族定位，唯一待定位点）。
4. **备份定位**：`get_message_backup_info(&BackupAuth, rng)`（未鉴权 gRPC）→
   `MessageBackupInfo { cdn, backup_dir, backup_name }`。官方注记：无效凭据与从未设 backupid
   服务端不可区分（同 `BackupAuthCredentialRejected`）——引导臂必须覆盖。
5. **下载**：`GET cdn{N}/backups/{backup_dir}/{backup_name}`（WebAPI:3183）+ ④的 headers；
   流式块读对齐 `download_transfer_archive` 既有写法（`src/service/mod.rs:3558` 模板），
   进度快照对齐 `transfer_progress` 先例。
6. **解密导入**：`backup::keys::derive_keys(store.fetch_account_entropy_pool(), aci).message_key`
   → `FramesReader` + `export_frames_loop`（`backup/receive.rs:107 export_snapshot_frames` 同款
   尾部 HMAC 校验）→ 复用 snapshot_import 消费链（NDJSON → connector 现行导入管线）。
   AEP 链接即落库（`link_flow` `store_account_entropy_pool` 在案），无新增密钥面。

## 2. JSON-RPC 方法面（KT 扩展，compat 修订随批）

- `backups.cloudStatus`（read，只读不落调度日志，对齐 `transfer.downloadStatus`/`backup.exportStatus`
  纪律）：`{account}` → `{present, phase?, bytesRead?, bytesTotal?}`——快照缺席
  `{present:false}`，不泄露注册状态。
- `backups.restoreFromCloud`（mutating，send lane per-account mutex）：`{account}` →
  成功 `{status:"restored", stats:{frames,chats,messages,...}}`（ExportStats 投影）；
  失败结构化：`BACKUP_NOT_PROVISIONED`（引导后仍无档=账号从未开启云备份，retryable=false）、
  `BACKUP_AUTH_FAILED`（凭据兑换被拒，retryable=false）、`BACKUP_DOWNLOAD_FAILED`/
  `BACKUP_DECRYPT_FAILED`（按族定 retryable）。进度经 `backups.cloudStatus` 观测，
  语义对齐 §4.38 progress（total 缺席不给假 0）。

## 3. 诚实边界

- messages 档（文本备份）**免费全账号**；media 字节档属订阅加购——本期只做 messages 档
  restore（与官方 `BackupCredentialType.Messages` 对位），media 档二期。
- 「无档」只在引导自愈后仍缺席时成立：`BACKUP_NOT_PROVISIONED` 诚实上报，UI 侧降级提示，
  不装死不误报付费。
- 恢复目标=当前链接账号本体；跨账号恢复不在面内（官方同口径）。

## 4. 测试与验收

- 单测三链：①凭据兑换 roundtrip（zkgroup 已知向量）②404→backupid 引导→重试状态机
  ③无档/凭据拒/下载断流三失败形态的结构化错误面
- 门禁：cargo fmt/clippy -D warnings/test 全绿 + bin 冒烟；契约节并入 jsonrpc-compat.md
  （813 落地后）+ 修订链顺延
- 真机回归：已开启云备份（免费档即可）的账号端到端 restore + 无档账号诚实降级

## 5. 实施切分

- 本批（引擎）：§0 依赖 + `src/service/cloud_backup.rs`（REST 面/zkgroup 链/CDN 下载/解密，
  future 非 Send 走 LocalSet 对齐 link_flow 纪律）+ §2 两方法 + 单测
- 后续批：connector 透传（cloudStatus/restoreFromCloud 收编 MUTATING/read 分族 + boundaryGates）
  → 桌面设置页备份分区入口（UI 复刻官方 PreferencesBackups 形态，免费档文案）
- 二期：media 档、`upload`（type:remote 导出+等级凭据）、手输 30 字节密钥恢复他档
