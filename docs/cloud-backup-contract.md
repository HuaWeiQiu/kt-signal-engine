# 云备份契约（Cloud Backup）——引擎侧

状态：v2（2026-10-08，§6 云备份 upload 面裁定落地；v1 restore 面已合栈
main：方法面并入 `docs/jsonrpc-compat.md` §3 逐方法表，compat 为方法面
真相源、本文档为凭据链/边界/测试真相源）
关联：`adr/0005`（backup5 传送链）、`src/backup/keys.rs`（AEP 派生单点）、
`docs/jsonrpc-compat.md`（§3 `backups.restoreFromCloud`/`backups.cloudStatus`/
`backups.uploadToCloud`/`backups.uploadStatus` 四行）
取证基线：官方 Signal-Desktop v8.31.0-alpha.1（abe80d3；端点/文件:行见各条）
+ 引擎 pin libsignal tag v0.99.0（439f035）

## 0. 依赖新增报备（AGENTS.md 纪律）

- `libsignal-net`、`libsignal-net-chat`：`git = "https://github.com/signalapp/libsignal", tag = "v0.99.0"`（与既有
  `libsignal-message-backup`/`libsignal-account-keys` 同源同 tag，Cargo.lock 已解析至同 commit）。
  用途：chat 未鉴权 gRPC-over-WS 面（`get_backup_cdn_credentials`/`get_message_backup_info`，
  官方 `rust/net/chat/src/grpc/backups.rs:284/:436`）。许可 AGPL-3.0-only，与本仓一致。
- 连接构造：`libsignal_net::chat::ChatConnection` → `Unauth<ChatConnection>`（`GrpcServiceProvider`
  对可 Clone GrpcService 自动实现，`net/chat/src/grpc.rs:87-99`）；官方同构先例
  `net/chat/src/registration/service.rs:16-73`。连接参数（chat.signal.org WS endpoint/TLS/keepalive）
  实施首日以 libsignal-net connect 入口实测钉定（本契约唯一机械未知项）。

> **§0.1 实测钉定（2026-10-08，worktree feat/cloud-restore）**：依赖已入 Cargo.toml 并
> cargo fetch 解析——`libsignal-net`/`libsignal-net-chat` 均解析至 tag v0.99.0 实树
> （checkout 439f035，注意与早先行为参照的 e8cc2dd 不同树，API 以 439f035 为准）。
> **连接配方实读（439f035 树）**：`ChatConnection`（rust/net/src/chat.rs:170）→
> `ChatConnection::start_connect_with(connection_resources, service, http_route_provider,
> endpoint_path, user_agent, ws_config, headers, log_tag)`（:269-286，返回
> PendingChatConnection）；`ConnectionResources` 构造同款先例 =
> **rust/net/examples/cdsi_lookup.rs:89-95**（`ConnectState::new(SUGGESTED_CONNECT_CONFIG)`
> + DnsResolver + `no_network_change_events()` + confirmation_header_name），
> 路由提供方 DirectOrProxyProvider（example :16 同款直连路由）；
> backups gRPC 面 439f035 在位：`grpc/backups.rs:242 get_backup_cdn_credentials` /
> `:393 get_message_backup_info`（BackupsAnonymousClient 路径 :408）。
> endpoint_path = chat WS 路径（`/v1/websocket`，与 presage 收发同端点），
> unauth 模式 headers=None（ChatHeaders::Auth 缺席）。
> **剩余实施项**：cloud_backup.rs 本体（REST+zkgroup+gRPC+CDN+解密）、methods.rs
> 两方法注册、进度快照面、单测三链——按 §1-§4 契约直写。

> **§0.2 第三片（gRPC 面）实施模板（官方 example 实读定稿）**：
> `rust/net/chat/examples/chat_request_scaffold.rs` = 全链官方模板——
> ①`libsignal_net::env::PROD`（生产 env；`env.chat_domain_config.connect.hostname =
> "grpc.chat.signal.org"` + `http_version = Some(HttpVersion::Http2)` + ip_v4/v6 清空）；
> ②`simple_chat_connection(&env, EnableDomainFronting::No, DirectOrProxyMode::DirectOnly,
> |_route| true).await?` → chat_connection（注意：该 helper 在
> `libsignal_net::chat::test_support` 命名空间——生产使用二选一：依赖开
> test-support feature，或按其函数体自写构造（内部即 ConnectionResources +
> start_connect_with 的装配，本文 §0.1 配方）；实施时读 helper 本体定夺）；
> ③`Unauth(chat_connection.shared_h2_connection().expect("H2"))` → grpc 面；
> ④api 调用（get_backup_cdn_credentials/get_message_backup_info 同 grpc/backups.rs
> :242/:393 签名，含 BackupsAnonymousClient 路径 :408）。
> CDN 下载段 = `GET cdn{N}/backups/{dir}/{name}`（§1.5）+ §3 头两件套；
> 解密导入段 = derive_keys(AEP,aci).message_key + FramesReader + export_frames_loop
> （backup/receive.rs:107 同款尾部 HMAC）。

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
  纪律）：`{account}` → `{present, phase?, bytesRead?, bytesTotal?, errorCode?}`——快照缺席
  `{present:false}`（未开始/已成功收尾/引擎重启过），不泄露注册状态。phase：
  `downloading`=在途（字节计数有效，total 缺席不给假 0）；`failed`=终态失败
  （`errorCode` 结构化透出下表同码，快照保留至下次 start 覆盖，不装死）。
  成功终态不留痕（清缺），产物走 NDJSON 消费链——transfer_archive
  「无论成败收尾清快照」同纪律。
- `backups.restoreFromCloud`（mutating，send lane per-account mutex）：`{account}` →
  受理 `{status:"started"}`（全链后台化：凭据拉取 404 引导自愈→zkgroup 兑换→
  生产 chat gRPC 定位→CDN 下载→解密导出 NDJSON；终态经 `backups.cloudStatus`
  观测，语义对齐 §4.38 progress）。同步失败结构化（码表七族，
  `CloudRestoreError::rpc_code` 单一映射源，cloudStatus failed 相位同表两端不漂）：
  `BACKUP_NOT_PROVISIONED`（引导自愈后仍无档=账号从未开启云备份，retryable=false）、
  `BACKUP_AUTH_FAILED`（AEP/registration/身份密钥缺席或 zkgroup 兑换被拒，
  retryable=false）、`BACKUP_RUNNING`（同引擎已有在途 restore 重入，retryable=false）；
  后台段失败经 cloudStatus failed 相位透出：`BACKUP_TRANSPORT_FAILED`（chat/REST
  传输，retryable=true）、`BACKUP_DOWNLOAD_FAILED`（CDN，retryable=true）、
  `BACKUP_DECRYPT_FAILED`/`BACKUP_EXPORT_FAILED`（retryable=false）。

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
- 二期：media 档、`upload`（type:remote 导出+等级凭据，已裁定落地=§6）、手输 30 字节密钥恢复他档

## 6. 云备份 upload 面（二期，2026-10-08 裁定落地）

upload = 云备份的出方向下半场：把账号备份产物（messages 档密文归档）上传
Signal 云存储，官方桌面同链（`Backups.upload`，index.preload.ts:310-338）。
本节固化五项裁定与实测钉定，实施批按此直写；media 档/订阅面明确出范围。

### 6.0 五项裁定（2026-10-08）

1. **对象存储/上传形状**：照搬官方校验——`cdn === 3` 强制（官方
   `createFetchForAttachmentUpload` WebAPI:3261 同款 strictAssert）、上传
   目标= form 的 `signedUploadLocation`（origin 不改写）、form `headers`
   透传不解析（凭据头引擎不读不落日志）；form 经 messages 档 BackupAuth →
   未鉴权 gRPC `get_upload_form`（pin 树 grpc/backups.rs:155，`upload_size`
   = 装配产物字节数）。
2. **凭据来源**：引擎自持——复用 §1 restore 同族凭据链（`v1/archives/auth`
   7 天窗签发 + 404→backupid 引导自愈 + zkgroup 兑换呈现，引擎 §1.1-§1.3
   既有单点）；**v2 范围钉 messages 档**，media 档与订阅查询面
   （`getBackupLevel`/`v1/subscription`）出范围——引擎不查订阅、不误报
   等级，`get_upload_form` 固定 `UploadType::Messages`（pin 树同款）。
3. **分片/断点**：接受**单发整文件上传、无断点续传**（失败=整链重来，新
   form 新文件；服务端以最新成功上传为准）；unknown outcome 永不自动重试
   （§2 restore 同族语义）。官方 tusProtocol 的 create-with-upload 单发
   POST 即此形态（钉定见 §6.1），KT 恰取该步、不做 HEAD/PATCH 续传臂。
4. **加密信封**：实测钉定结论=**remote 型产物与一期 local 容器逐字节同构、
   派生钥相同（nonce=None）**，无需新增信封写面（钉定证据 §6.1）；
   SBACKUP 前缀+明文 metadata 帧是 Android/SVRB 系写入形态，libsignal
   reader 双形态容忍（frame.rs:99-108），引擎 v2 写无 magic 形态（与官方
   桌面上传字节同构）。`media_root_backup_key` 审计结论见 §6.4。
5. **分工/时序**：与一期四片同构——引擎新方法臂（§6.2 两方法）+ send
   lane mutating + 进度快照轮询；`refresh_backup` 时机=引擎上传确认成功
   后直发（pin 树 grpc/backups.rs:318；upload-then-refresh 的
   unknown-outcome 窗口=POST 2xx 后 refresh 失败，备份可能已落云，语义=
   failed 终态如实透出、retry=整链重来）；connector 透传批与桌面 UI 批
   为后续批（connector 白名单登记新码位归该批）。

### 6.1 实测钉定（2026-10-08，实施首日，「不许猜」条款产物）

| 项 | 结论 | 证据（官方 abe80d3 / pin 树 439f035） |
|---|---|---|
| 派生 VERSION | `BackupKey::VERSION` = V1 = 1 | account-keys `src/backup.rs:41/:44`（`pub const VERSION: u8 = V1`，`BackupKey<const VERSION: u8 = LATEST>`） |
| nonce | **None**（桌面不传 token）→ OLD_DST `20241007_SIGNAL_BACKUP_ENCRYPT_MESSAGE_BACKUP:` | Node 绑定 `node/ts/MessageBackup.ts:55`（`forwardSecrecyToken?` 可选）+ :84-102（`forwardSecrecyToken?.contents ?? null`）；桌面 `crypto.preload.ts:66` 构造 `new MessageBackupKey({backupKey, backupId})` 不带 token → Rust `MessageBackupKey::derive(.., None)`（key.rs:41-47） |
| 容器 | remote 型 = local-encrypted 逐字节同构（无 SBACKUP、无明文 metadata 帧） | 桌面 `index.preload.ts:1216-1241` `#exportBackup` remote 分支管线 = recordStream→gzip→appendPaddingStream→AES-256-CBC→prepend(IV)→appendMac(macKey)，与 local-encrypted 同一分支；`importBackup` remote 分支同（两遍 HMAC，无 magic 处理） |
| SBACKUP 边界 | reader 双形态容忍；写面仅 Android/SVRB 系 | pin 树 `frame.rs:99-108`（8B magic 命中→`verify_metadata` 帧面，否则 ciphertext-only）+ `forward_secrecy.rs:19`（`SBACKUP\x01`） |
| 校验 purpose | 导出校验 = `Purpose.RemoteBackup` | 桌面 `validator.preload.ts:36/:78` |
| 上传协议 | cdn3 = TUS create-with-upload 单发 POST | 桌面 `uploadAttachment.preload.ts:47`（`CDNS_SUPPORTING_TUS = new Set([3])`）+ `tusProtocol.node.ts:83-148`（`POST {signedUploadLocation}`：头 = form headers + `Tus-Resumable: 1.0.0` + `Upload-Length: <size>` + `Upload-Metadata: filename <b64(form.key)>` + `Content-Type: application/offset+octet-stream`，body=整文件流；2xx=完成）；ok=false 才走 HEAD+PATCH 续传（KT 不做） |
| 官方向量 | Some/None 双向量在库 | pin 树 key.rs 测试面：`FAKE_BACKUP_FORWARD_SECRECY_TOKEN` + `FAKE_MESSAGE_BACKUP_KEY`（Some→NEW_DST）与 `FAKE_MESSAGE_BACKUP_KEY_LEGACY`（None→OLD_DST，引擎既有） |

复现命令（pin 树 checkout = `~/.cargo/git/checkouts/libsignal-2a193a9867decbc4/439f035`，
桌面 = `/Users/tanye/test/kt/upstream/Signal-Desktop` @ abe80d3）：

```sh
sed -n '32,60p' <pin>/rust/message-backup/src/key.rs            # derive + NEW_DST/OLD_DST
sed -n '35,45p' <pin>/rust/account-keys/src/backup.rs           # VERSION = V1
grep -n "forwardSecrecyToken" <pin>/node/ts/MessageBackup.ts    # Node 绑定 token 面
sed -n '1216,1245p' <desktop>/ts/services/backups/index.preload.ts  # remote 管线
sed -n '83,148p'  <desktop>/ts/util/uploads/tusProtocol.node.ts     # TUS 单发 POST
```

引擎侧钉死：`keys.rs` 单测双向量（Some(token)=FAKE_MESSAGE_BACKUP_KEY、
None=FAKE_MESSAGE_BACKUP_KEY_LEGACY）。

### 6.2 JSON-RPC 方法面（KT 扩展，compat §3 随实施批登记）

- `backups.uploadToCloud`（mutating，send lane per-account mutex；进程级
  单槽运行闸同 §2 restore）：`{account}` → 受理 `{"status":"started"}`
  （全链后台化：输入工件 → remote 归档装配（专用线程，§10.3 同款）→
  凭据拉取（404 引导自愈）→ zkgroup 兑换 → form gRPC → cdn3 TUS 单发
  上传 → `refresh_backup` → 临时产物即弃；终态经 `backups.uploadStatus`
  观测）。start 校验序：账号已注册（fake/未注册/AEP/签名密钥缺席 =
  `BACKUP_AUTH_FAILED`，restore 同口径）→ 工件存在（缺
  `BACKUP_ARTIFACT_MISSING`）→ 首行合法（非法当场删工件 +
  `BACKUP_ARTIFACT_INVALID`，§10.2 硬边界同款）→ 运行闸（在跑 =
  `BACKUP_UPLOAD_ALREADY_RUNNING`）。参数缺 account `-32603`。
- `backups.uploadStatus`（read，只读不落调度日志零 stderr；快照进程级
  单槽同 `cloudStatus` 纪律，account 仅契约形状校验不参与路由）：
  `{account}` → `{present:false}`（未开始/已成功收尾/引擎重启过）；
  `{present:true, phase, bytesRead(u64), bytesTotal?(u64), errorCode?}`。
  phase：`exporting`=装配段（无字节计数语义，bytesRead=0、total 缺席）；
  `uploading`=上行段（块流计数有效，total=装配产物字节数，不给假 0）；
  `failed`=终态失败（errorCode 透出同码表，快照保留至下次 start 覆盖）。
  成功终态清缺（restore 同纪律）。

结构化错误码（全 `-1` + message 码位；带 detail 族附 `data.detail`；
`*` = connector 白名单已登记码位，新码位白名单登记归 connector 透传批）：

| 码 | 面 | retryable |
|---|---|---|
| `BACKUP_AUTH_FAILED`* | start：未注册/fake/AEP/签名密钥缺席；后台：zkgroup 兑换被拒 | false |
| `BACKUP_ARTIFACT_MISSING`* | start：输入工件缺席 | false |
| `BACKUP_ARTIFACT_INVALID`* | start 首行非法（当场删）；后台装配行级/帧序违规（永不静默丢行，§10.2 同口径） | false |
| `BACKUP_UPLOAD_ALREADY_RUNNING` | start：同引擎已有在途 upload（进程级单槽） | false |
| `BACKUP_NOT_PROVISIONED`* | 后台：引导自愈后仍无档=账号从未开启云备份 | false |
| `BACKUP_TRANSPORT_FAILED`* | 后台：凭据拉取/form gRPC/refresh_backup 传输段失败 | true |
| `BACKUP_UPLOAD_FAILED` | 后台：CDN 上行失败（非 2xx/`cdn≠3`/产物超 1 GiB 帽/上行读盘失败） | true |
| `BACKUP_IO_FAILED`* | 后台：临时归档文件系统写失败 | false |

### 6.3 全链编排与有界纪律

```text
connector 写工件（backup-export-input.ndjson，§10.2 同一形状）
   │ backups.uploadToCloud {account}
   ▼
①装配段（专用 OS 线程，§10.3 同款）：工件 → FrameSequencer/MainStreamWriter
  → BackupStreamWriter（local/remote 同容器，§6.1）→ <account_dir>/
  cloud-upload-main-tmp（写全即止，任何终态必删）
   │ 装配完成（oneshot 通知 LocalSet watcher）
   ▼
②上行段（LocalSet）：凭据（§1 链，404 引导自愈）→ zkgroup 兑换 → BackupAuth
  → 未鉴权 gRPC get_upload_form(upload_size=产物字节) → cdn==3 强制
  → TUS 单发 POST（128 KiB 块流式读盘 body，进度按块计数）
   ▼
③refresh_backup（BackupAuth 同呈）→ 成功=快照清缺；失败=failed 终态
```

- **帽值**：装配产物 > 1 GiB = `BACKUP_UPLOAD_FAILED` fail-closed（文本档
  产物余量充分，runaway 熔断）；上行读盘块 128 KiB（内存 O(块)）；上传
  POST 请求级超时 900 s（PushService 客户端默认 65 s 不够长文上行，
  per-request 覆盖，显式有界）；进度快照与运行闸进程内存，不落盘。
- **临时产物**：`<account_dir>/cloud-upload-main-tmp` 密文归档，任何终态
  （成功/失败/引擎崩溃后的下次上传）即弃覆盖写 + finally 删除（官方
  `finally unlink` 同款，index.preload.ts:333-337）；明文永不落盘（§10.2
  纪律延续，工件是 connector 明文、归档是密文）。
- **凭据纪律**：BackupAuth/zkgroup 材料内存即用即弃，不落日志不落盘
  （§1/§10.5 同款）；form headers 是凭据头，透传不记日志。

### 6.4 诚实边界

- **media 档与订阅面出范围**：`getBackupLevel`/subscription 查询引擎不做
  （裁定 2）；`get_media_upload_form`/`v1/archives/media/delete`/
  `listMedia` 面 media 批另立项。免费档账号上传合法（messages 档免费全
  账号，§3 同口径）；付费校验归服务端（form/上传被拒如实透码，引擎不
  预判不误报等级）。
- **`media_root_backup_key` 审计结论（§10.5 挂账翻案）**：官方语义确认为
  **账号恒定值、客户端持久随机**（桌面 `crypto.preload.ts:33-39`
  `getBackupMediaRootKey` = itemStorage 持久取用，非派生；随链接
  provisioning linkedDeviceData 分发）——§10.5「随机生成」的判断需翻案
  成立。但引擎 v2 **无既定落点**：presage store 无通用 kv 可持久化、
  provisioning 分发面未立项，而新增明文密钥文件违反「密钥不落盘」纪律
  （§7/§10.5）。v2 裁定：**上传产物维持随机生成 + 本条差异如实登记**
  （messages 档产物 `files/` 恒空、无媒体帧引用该字段，消费端无解密
  影响）；恒定值落点=附件批随 provisioning/工件面一并接入。与 §8 差异
  1/2（flush/padding）同列「互操作不受影响、语义面登记」。
- **单发无断点**：上行中断（连接断/超时/非 2xx）= `BACKUP_UPLOAD_FAILED`
  终态；不续传不自动重试（裁定 3）；调用方 retry = 重新
  `backups.uploadToCloud`（新 form 新产物，服务端以最新成功上传为准）。
- **成功不留痕**：上传成功引擎不留任何本地痕迹（快照清缺、临时产物即
  弃）；云上是否已有备份的查询面=restore 链 `get_message_backup_info`
  既有语义，本面不新增。

### 6.5 测试与验收

- 单测三链：①装配产物官方 `BackupReader`（`Purpose::RemoteBackup`）校验
  通过（容器/帧面正确性）②错误码表 + 进度快照生命周期（exporting→
  uploading→成功清缺/failed 留痕）+ 运行闸互斥 ③帽值 fail-closed + 临时
  产物终态必删 + start 工件闸（缺失/首行非法当场删）
- 门禁：cargo fmt/clippy -D warnings/test 全绿 + bin 冒烟；compat §3 两行
  随实施批并入
- 真机回归（后续批）：已开启云备份账号端到端 upload → 官方客户端
  restore 回读一致；无档账号诚实 `BACKUP_NOT_PROVISIONED`

### 6.6 实施切分

- 本批（引擎）：§6 全部——`src/service/cloud_upload.rs`（装配线程+上传
  编排+进度面）+ §6.2 两方法 + keys.rs 双向量单测 + compat §3 两行
- 后续批：connector 透传（uploadToCloud/uploadStatus 收编分族 + 白名单
  新码位）→ 桌面设置页 upload 入口；media 档、手输 30/64 字节密钥恢复
  他档（§5 二期余项）另批
