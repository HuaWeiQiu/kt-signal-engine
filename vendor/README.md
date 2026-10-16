# vendor/ — 原样拷贝的上游依赖（Stories 批，2026-10-08）

本目录当前是两个上游仓在引擎钉定 rev 的**逐字节原样拷贝**，经根
`Cargo.toml` 的 `[patch]` 段替换对应 git 源。story 协议面的最小叠加已随
批落地，文件级 fork 改动清单见下，随 commit 递增登记（与上游的差异可
`diff -rq` 对照 checkout 复核，见清单头）。

| 目录 | 上游 | 钉定 rev | 许可 |
|---|---|---|---|
| `libsignal-service/` | github.com/whisperfish/libsignal-service-rs | `7aa49095efc0627aa7a59c34ef9d98042c35731b` | AGPL-3.0-only（LICENSE.md 随目录） |
| `presage/presage` | github.com/whisperfish/presage | `96f5e925c45b2d7a1c09afd0aee44733a2c3e671` | AGPL-3.0-only（LICENSE.md 随目录） |
| `presage/presage-store-sqlite` | 同上 | 同上 | AGPL-3.0-only（fork 叠加见下方清单） |

为什么 vendor：上游（含 presage 传递钉定的 libsignal-service）没有 story
发送面；任务口径 = 「薄 fork、只扩最小 story 发送面」。`[patch]` 指本地
路径后，engine 与 presage 两层都能直接用扩展面，同时保持其余上游代码
零漂移。

任务书原文写「[patch.crates-io] 指本地路径」：实际两仓均为 git 源依赖
（Cargo.lock `git+https://…` 形状），`[patch.crates-io]` 对 git 依赖不
生效，按事实改用对应 git 源 URL 的 `[patch."…"]` 段（批汇报偏差项）。

## fork 改动清单（Stories 批，随 commit 递增登记）

钉定 rev 之上的**全部**叠加改动（逐文件；其余上游代码零漂移，可
`diff -rq` 对照 checkout 复核）：

`vendor/libsignal-service/`（rev 9e6c08b）：

- `src/websocket/sender.rs` — 新增 `send_messages_unidentified_story`：
  `PUT /v1/messages/{dest}?story=true` 查询参、无 `Unidentified-Access-Key`
  头。官方锚：libsignal v0.99.0 `ws/messages.rs:141-150`（story 后缀）+
  `:39`（Story→无 user-based 授权头）；桌面 `WebAPI.preload.ts:3804/3850`
  同查询参、`OutgoingMessage.preload.ts:392-394` story 恒 sealed sender。
- `src/sender.rs` — ①`try_send_message` 增 `story: Option<&SenderCertificate>`
  参数（既有 6 处调用点全传 `None` = 上游行为逐字保持；Some 时加密
  cert = `story.or(unidentified_access.map(cert))`、分发走 story ws 方法、
  无 identified 降级）；②新增 pub `send_story_message`（单收件人 sealed
  sender + `?story=true` 传输面；不产 sent-transcript——官方
  sendStory.preload.ts:338-340「story 同步由收尾单独发」同口径）。
- `src/content.rs` — `Metadata` 增 `pub story: Option<bool>`（E2：官方
  Envelope.story=16 接收分类位透传，Signal-Desktop
  MessageReceiver.preload.ts:1517）。
- `src/cipher.rs` — `decrypt` 唯一 Metadata 构造点填
  `story: envelope.story`（E2）。
- `src/provisioning/pipe.rs`（2026-10-10 nopni2 批）— ①QR link URI 构造
  抽成 pub `link_uri`（原 process_request 内联拆出，UUID/pub_key 形状不变）；
  ②URI 补 `capabilities=nopni,nopni2`：Signal-Android 8.30+/8.31 扫码侧对
  numberless（无 PNI）账号强制 nopni2（LinkDeviceViewModel.kt
  onQrCodeScanned，上游 commit cdd27e08fa 把枚举值 "nopni"→"nopni2"），
  缺失即 OUTDATED_DEVICE 拒连；双报对齐官方桌面
  Provisioner.preload.ts:429-433。backup5 不报（引擎未实现 link-and-sync，
  报了手机会走 archive 同步链接流）。取证 docs/upstream-capability-nopni2.md；
  门禁测试 src/service/mod.rs tests::link_uri_advertises_numberless_capabilities。
- `src/provisioning/mod.rs`（2026-10-10 nopni2 批）— `pub use pipe::link_uri`
  （引擎侧门禁测试入口）。
- `src/messagepipe.rs`（2026-10-11 A 止血批③）— 信封 ack 不再在 yield 即
  回 200：`EnvelopeAck` 句柄穿 presage unfold 交消费方（解密+落库+投影
  完成后 ack，Android IncomingMessageObserver.kt 事务提交后逐条 ack /
  SignalWebSocket.kt「处理成功后由你 ack」同契约）；queue-empty 标记仍
  就地 ack。
- `src/account_manager.rs`（2026-10-11 B3 prekey 维护批）— 抽 pub
  `publish_fresh_pre_keys`（prekey 批生成+上传复用腿）与
  `rotate_signed_pre_key_bundle`（signed prekey 按龄轮换上传腿）。
- `src/push_service/mod.rs`（2026-10-11 P12/R3 批）— `KEEPALIVE_TIMEOUT_SECONDS`
  55s→30s：keepalive 上一拍未回即断，死链检测 55-110s → ≤60s（官方
  Desktop WebsocketResources.preload.ts:512 KEEPALIVE_TIMEOUT_MS=30s 同
  位）；30s 断言单测。
- `src/messagepipe.rs`（2026-10-11 P12/R3 批追加）— ack 单测补 `mut`
  （编译错，vendor 测试首次可跑时暴露）。
- `src/sender.rs`（2026-10-11 P12/R3 批追加）— `try_send_message` 加
  `#[allow(clippy::too_many_arguments)]`（Stories 批 `story` 参数超限；
  参数结构体重构另行立项）。
- `src/account_manager.rs`、`src/content.rs`、`src/sender.rs`、
  `src/push_service/mod.rs`（同批 `cargo fmt` 收口）— 稳定通道 rustfmt
  子集重排，语义零变化。
- `src/push_service/mod.rs`（2026-10-11 ws 升级 401 归型批）— `ws()` 升级
  响应 401/403 经纯函数 `classify_upgrade_response` 保留为
  `ServiceError::Unauthorized`（此前 `.into_websocket()` 把非 101 升级
  失败整体折叠 `WsError`，升级状态码丢弃，凭据死亡被黑盒化）；
  `upgrade_classify_tests` 单测。
- `src/content.rs`（2026-10-11 P15a 项2 批）— `Metadata` 增
  `pub ephemeral: Option<bool>` / `pub urgent: Option<bool>` /
  `pub report_spam_token: Option<Vec<u8>>`：Envelope 12/14/17 三位接收
  透传（官方桌面信封事件面 MessageReceiver.preload.ts:1177-1180 urgent
  原样透传 + reportingToken 消费面；engine 接收投影上抛，connector 侧
  举报前缀/优先级消费）。
- `src/cipher.rs`（同批）— `decrypt` 唯一 Metadata 构造点填
  `ephemeral/urgent: envelope.*`、`report_spam_token:
  envelope.report_spam_token.clone()`（P15a 项2）。
- `protobuf/SignalService.proto`（2026-10-11 P15a 项4 批）— `Blocked`
  消息同步上游 abe80d3 的 modern 三型：新增 `BlockedE164`(5)/
  `BlockedAci`(6)/`BlockedGroup`(7) 三消息与 repeated 字段（逐字对照
  Signal-Desktop `protos/SignalService.proto:581-605`，含 deprecated
  注释；wire additive-only，field 5/6/7 不与旧四位冲突）；引擎 Blocked
  同步收投影消费面（官方 `_handleBlocked` modern/legacy 家族判定）。
- `src/sender.rs`（2026-10-11 时序审计组四 E3 批）— 新增 pub
  `send_plaintext_content`：PlaintextContent（重试回执载体）按官方明文腿
  直发——Envelope type=8、不做会话加密；设备枚举同
  `create_encrypted_messages`（sub devices + 主设备 − 自身设备），无会话
  设备跳过（无会话即解不开原消息，重发请求对它无意义）。官方锚：Desktop
  requestResend → `PlaintextContent.from(DecryptionErrorMessage.forOriginal
  (...))` → `sendMessageProtoAndWait{proto: plaintext}`
  （handleRetry.preload.ts:659-689 / sendResendRequest.preload.ts:114-124），
  OutgoingMessage.preload.ts:108-121 Plaintext→type 8；libsignal v0.99
  `message_encrypt` 无明文分支，须 service 层自构 `OutgoingPushMessage`。
- `src/pre_keys.rs`（2026-10-11 时序审计组四 E1 批）— `replenish_pre_keys`
  增 `force_new_last_resort` 参数（按龄轮换时无条件新造 last-resort；原
  逻辑仅首装缺失才生成）；新增 `PRE_KEY_MAX_COUNT = 200` 常量（官方
  AccountManager.preload.ts:128 同值）。
- `src/account_manager.rs`（2026-10-11 时序审计组四 E1/E2 批）—
  ①`rotate_signed_pre_key_bundle` 改走 force 臂：轮换 PUT 同携新 signed +
  新 last-resort（官方 #doUpdateKeys 同 PUT 携带、双侧 DAY*1.5 轮换龄：
  AccountManager.preload.ts:115/:142 + #maybeUpdateLastResortKyberKey
  :868-920）；`publish_fresh_pre_keys` last-resort 上传面改「新生成优先、
  否则 stored newest-by-timestamp」（原 `last_resort_keys[0]` 按 rowid 取
  最旧行，多行时会重传旧键——轮换语义顺修）；`update_pre_key_bundle` 传
  force=false 行为不变。②水位闸补上限臂：服务器计数超 `PRE_KEY_MAX_COUNT`
  =200 也整批重发（官方 #doUpdateKeys `count < PRE_KEY_MINIMUM ||
  count > PRE_KEY_MAX_COUNT` 同判，一次性与 kyber 两族同闸）。
- `src/groups_v2/manager.rs`（2026-10-11 MRM/GSE 地基批）— `fetch_group`
  拆出 pub `fetch_encrypted_group_response`：完整 `GroupResponse` 包装面
  （携 group-send-endorsements 举权 blob 与加密群态，MRM 发送链 GSE 取数
  地基）；原单群 proto 投影面保持，取数改走 `get_group_response`。
- `src/websocket/account.rs`（2026-10-11 D5 能力位补齐+设备面注册批）—
  ①`DeviceCapabilities` 补 `versioned_expiration_timer`/
  `optional_phone_number` 两位（server 真相 DeviceCapability.java 全集
  对齐，serde camelCase，缺省 false）；②新增 `CapabilitiesUpload`（官方
  CapabilitiesUploadType 四键）与 `register_capabilities`（PUT
  v1/devices/capabilities 设备面注册端点）。
- `src/websocket/registration.rs`（2026-10-11 时序审计组四 E1-E4 收口批）
  — ACI/PNI 两 `replenish_pre_keys` 调用点补 `force_new_last_resort=false`
  位（pre_keys E1 签名扩参的编译面，注册链行为不变）。
- 上游升级（2026-10-12 presage 升 rev 批）— 随 presage 33dd149→96f5e92
  传递钉定 9e6c08b→7aa49095 重新 vendor + fork 清单重放（11 提交：
  错误处理解码面函数化重构 response.rs、`ServiceError` 新增约 20 个
  细化变体（additive）、发送路径 404 匹配改 `UnregisteredRecipient`
  （`NotFoundError` 变体保留）、DME 面 `UnidentifiedAccess` 改按引用、
  新增 `send_decryption_error_message`/`send_sender_key_decryption_
  error_message`；传递 libsignal 仍钉 tag v0.99.0，Cargo.lock 全图
  零漂移）。fork 清单 14 文件中 12 个逐字干净重放，两个融合点：
  - `src/sender.rs` — 上游新增 `send_sender_key_decryption_error_
    message` 内的 `try_send_message` 调用点（第 7 处）与 `send_message`
    内部调用点（新上游形状）补 story `None` 位（story 参数融合面）。
  - `src/websocket/sender.rs` — `send_messages_unidentified_story` 内部
    改走上游新 `request_json_with(request, put_messages_errors)`（原
    `request_json` 方法被上游移除，story 查询参/无 UAK 头语义不变）。
- `src/sender.rs`（2026-10-13 批次 T 发送侧 sync 副本批）— `send_message`
  尾部多设备转录上行改 best-effort：转录 `try_send_message` 失败降级为
  `warn!` 日志、主发送结局保留（上游原样 `?` 连坐——收件人已达但副本失败
  时整发报错，连接器重试队列会对已送达收件人重复投递；官方
  wrapWithSyncMessageSend.preload.ts / Android SignalServiceMessageSender
  均为副本失败不连坐主发送）。note-to-self 包裹腿（sync 即消息本体）不
  在此列，错误仍上抛。
- `protobuf/SignalService.proto`（2026-10-14 语音笔记波形+时长批）—
  `AttachmentPointer` 同步官方 proto 的语音元数据两位：`audioWaveform`(21,
  bytes,每字节一桶 0=静音 255=最响 至多 100 桶) 与
  `audioDurationSeconds`(22, float 秒)，注释逐字取自 Signal-Android
  `lib/libsignal-service/src/main/protowire/SignalService.proto`（wire
  权威在 Android 侧——语音笔记由手机端生成波形，桌面 abe80d3 proto 尚未
  同步这两位、按未知字段跳过并本地重算显示波形；引擎发送面按官方算法
  生成、收投影如实透传，engine 侧消费见 docs/jsonrpc-compat.md §5 同日
  批）。wire additive-only，不与既有 1-20 位冲突。
- `src/websocket/directory.rs`（2026-10-15 phoneLookup CDSI 批）—
  `discover_contacts` 的 CDSI 建连 RouteProvider 从硬编码
  `DirectOrProxyProvider::direct()` 改为 `DirectOrProxyMode::maybe_proxy`
  + 新私有 `proxy_from_environment()`：读 `ALL_PROXY`/`all_proxy`（大写
  优先，reqwest/hyper-util `get_first_env` 同序）经
  `ConnectionProxyConfig::from_parts` 构造代理路（socks/socks5/socks5h/
  http/https 均受支持，port 0 视为畸形），值缺失/畸形降级直连；
  `NO_PROXY` 不参与判定（进程出口策略是单全局变量，main.rs
  `setup_proxy_env` 统一落位）。背景：reqwest 传输面缺省读 ALL_PROXY，
  CDSI 建连走 libsignal-net 自有栈不吃该面，不改则 CDSI 流量绕过
  socks5 出口直连。
- `src/push_service/mod.rs`（2026-10-15 云备份/迁移下载面超时分档批）—
  request 预算按面分档：新增 pub 常量
  `CONTROL_PLANE_CONNECT_TIMEOUT`/`CONTROL_PLANE_TOTAL_TIMEOUT`
  （10s/65s，上游现值）与 `DOWNLOAD_CONNECT_TIMEOUT`/
  `DOWNLOAD_READ_STALL_TIMEOUT`（10s/45s）；`PushService` 增
  `download_client`（同 CA pin/UA/http1_only，**无总超时** + reqwest
  `read_timeout` 45s 读停滞判死——每次成功读即重置，健康慢流不触发）
  与 pub `download_request`（镜像 `request`，绑 download client；
  `request`/`ws` 控制面维持现预算不动）。背景：大备份/大附件流式 GET
  被一刀切 65s 总超时掐断，健康慢流被误报 cdn_unreachable。
  `request_budget_tests` 单测（控制面现预算钉形 + 下载面无总超时/
  停滞 30-60s 判死带）。
- `src/push_service/cdn.rs`（同批）— `get_from_cdn`（附件/头像/贴纸
  CDN GET 流式下载面）改走 `download_request`；上传/resume 面不动。
- `src/websocket/mod.rs`、`src/push_service/mod.rs`（2026-10-16 ka_diag
  保活可见性诊断批，**临时插桩可整体回退**）— 纯日志：`run` 保活循环
  加 8 条 `// KA-DIAG` warn 探针（tick 开火带 pending 计数、keepalive
  send 失败 info→warn、keepalive 应答非 2xx、全部 break/return 退出
  路径带字面量 reason、spawn 点 `ka_diag task spawned`），全部固定
  字面量无内容，不改任何行为逻辑（`?` 传播路径仍由既有
  `SignalWebSocket: {}` error 行覆盖）。背景：WS 管道服务器侧 1001
  idle timeout 与保活任务静默死亡无第一手证据（"sending keep-alive"
  为 debug 级不落 engine-stderr.log）。
- `src/websocket/slot.rs`（2026-10-16 单连接纪律批，新文件）— 进程级
  per-account websocket 槽位注册表（键 = credentials login）：每账号
  identified/unidentified 各至多一条活连接；重拨前先 terminate 旧连接
  （kill-before-create）；同账号重拨由槽级 dial_lock 串行（单飞）。
- `src/websocket/mod.rs`（同批）— ①`SignalWebSocketInner` 加
  `last_close_code`（服务器 close frame 记账）+ pub `close_code()`；
  ②`SignalWebSocket` 加 `kill: Arc<Notify>` + pub `terminate()`，
  `SignalWebSocketProcess::run` 加 kill 臂（close frame Away + 退出）与
  close-frame 4409 warn 字面量；③`Clone` 改手工 impl（去掉 derive 的
  `C: Clone` 界，泛型槽位克隆面）；④`pub mod slot` 挂载 + pub const
  `CLOSE_CODE_CONNECTED_ELSEWHERE = 4409`。
- `Cargo.toml`（同批）— tokio features 加 `"sync"`（Notify kill 开关）。
- `src/push_service/mod.rs`（2026-10-17 群态取回端点批）—
  `get_group_response` 路径 `/v1/groups/` → `/v2/groups/`（Android
  GROUPSV2_GROUP 同位）：/v1 应答裸 `Group`（GroupsV1Controller 剥包装、
  不回 endorsements），`GroupResponse` 解码必败 tag 0（活体
  2026-10-16 04:05/07:38Z 双臂同败）；v2 才是
  `GroupResponse{group=1, group_send_endorsements_response=2}` 包装面，
  GSE endorsements 自此真实回到取数面。engine 侧 `fetch_group_state`
  读面（groups.rs）保持 v1 + 裸 Group 解码不动（v1 契约自洽）。双契约以
  活体捕获字节钉入 `groups_v2::manager::live_group_endpoint_fixture`
  单测。

`vendor/presage/`（rev 33dd149）：

- `presage/src/manager/registered.rs` — ①新增 pub
  `Manager::send_story_message`（E1：注入发送者 profile_key 到
  StoryMessage、不落 store、不产 transcript，官方 profileKey 必需 + story
  不进本地 timeline 同口径）；②`send_message`/`send_message_to_group`
  两处 Metadata 字面量补 `story: None`（E2 字段新增的编译面，出站消息
  story 位恒无）；③（A③）接收 unfold 落库前 store 判重
  （thread_for_sender_and_timestamp 命中 → `Received::Duplicate` 仅 ack）
  + ack 时序改造（EnvelopeAck 交消费方）；④（A③/B1）解密失败不再静默
  ——`Received::DecryptionError` 上抛 + DERM 分档（回发
  DecryptionErrorMessage / 收重试请求按原始时间戳重发，找不到/超龄/超限
  clear_sessions 重建，防环计数 5 + 14 天新鲜度）；⑤（A⑤）
  `refresh_sender_certificate` + pub `unidentified_access_or_identified`
  （证书预轮换与发送 identified 保险丝）；⑥（B2）发送前刷群态
  （typing 免刷、取态失败 fail-open）+ `GroupSendOutcomes` 逐成员返回面
  + announcements-only 发送闸；⑦（B3）`refresh_pre_keys`/
  `rotate_signed_pre_keys`（2 天书签水位巡检 + 按龄轮换，kv 跨重启）；
  ⑧（B4）身份变更日志 drain（`take_identity_changes` 逐信封）+
  `Received::IdentityChange` 发射；接收流 refresh task **停发
  AccountAttributes PUT**（上游硬编码 `discoverable_by_phone_number: true`
  每次开流回写 = 隐私回写根因；pre-keys 注册腿保留；`set_account_attributes`
  删除）——官方 Signal-Desktop 链接设备从不 PUT attributes；
  ⑨（P9）收向同步请求三臂停答——`Request` Contacts/Keys/Blocked 全部
  warn + 跳过，不回任何表（官方链接设备只发请求不应答：
  syncRequests.preload.ts:11-33 只发三请求，MessageReceiver.preload.ts
  :3070-3074 收向 request 仅记日志即从缓存移除；原 Blocked 臂回全空表
  按官方 _handleBlocked 差集语义消费即「解除全部拉黑」（:3988-4044/
  :4207-4230），Keys 臂属 AEP 账号材料按需外泄，一并停答）；
  ⑩（P9）`send_message_with_profile_sharing` 拆面（`send_message` 委托
  恒传 true，行为不变）——`DataMessage.profile_key` 本机注入改调用方闸
  （官方 sendNormalMessage.preload.ts:183-185 profileSharing 位闸同位；
  `required_protocol_version` 保持无条件 = 线协议字段非同意面）；
  ⑪（P12/R1）`Registered` 增 `groups_manager_cache` 槽位——
  `groups_manager()` 改为槽位守卫复用（`Registered` 内
  `Arc<Mutex<Option<GroupsManager>>>`，惰性构建、跨操作复用），内部
  credentials cache 随之进程级存活：`/v1/certificate/auth/group` 一次
  拉取 [today, today+7d) 窗口后 6-7 天零凭据请求（官方
  groupCredentialFetcher 7 天窗对位；原每操作新建
  `InMemoryCredentialsCache` 必 miss 必发凭据请求）。接收流 init 臂改走
  `build_groups_manager()`（流内持有独立实例语义不变）；群头像/群发送/
  fetch_group_with_endorsements 三调用点改守卫解引用，行为面零变化；
  ⑫（P12/R2）接收批化解密+单事务批量落库——unfold 收信加官方 75ms/30
  批窗（`collect_window`，MessageReceiver.preload.ts:359-366），窗内逐信封
  分类为 slot 队列（DERM/Contacts/Duplicate/IdentityChange 直挂 Ready，
  Content 挂 to_save 下标）+ 窗毕 `save_messages_batch` 一次事务落库后按
  原序 yield（消费方所见事件流与 ack 契约不变）；批内判重 `batch_seen`
  补 store 判重盲区（落库在窗毕，窗内重复信封查不到）；mid-window
  QueueEmpty 仅转发事件、keys 同步请求留首拍专臂
  （`request_keys_from_primary_if_needed`）；`save_message` 拆
  `prepare_message_for_save`（profile-key/expire/远程删/delete-for-me 逐
  消息面原样）+ 单条壳保持原签名；`collect_window` 五例单测（tokio
  start_paused）。顺带收口：4 处机械 clippy（explicit_auto_deref×2/
  needless_borrow×2，历史批遗留）；移除无人引用的
  quickcheck/quickcheck_async dev-deps（syn1 full 特性断链，vendor 测试
  因此从未跑过）；dev tokio 加 test-util；本 manifest 加独立
  `[workspace]`+libsignal-service `[patch]`（脱离消费方 workspace 独立
  测试，接的是本 vendor 叉）。
- `presage/src/store.rs` — verified sync 存储路径 Metadata 字面量补
  `story: None`（E2，同上纯编译面）；（B4）`Store::take_identity_changes`
  默认空实现（身份变更日志 drain 面，其他 store 无感）；（P12/R2）
  `Store::save_messages(Vec<(Thread, Content)>)` 默认实现（循环
  save_message 兜底，事务型 store 应覆盖为单事务）。
- `presage/src/model/messages.rs` — ①（A③/B1）`Received::DecryptionError`
  变体 + `DecryptionFailure{RetryReceipt, RetryRequest}`；②（A③）
  `Received::Duplicate` 变体；③（B4）`Received::IdentityChange(ServiceId,
  Box<Content>, EnvelopeAck)` 变体（本体随信封照常投影、单次 ack）；
  （P12/R2）`cargo fmt` 收口重排（变体行超宽换行，语义零变化）。
- `presage/src/manager/mod.rs` — ①（A⑤）`pub use
  unidentified_access_or_identified`；②（B2）`pub use group_member_is_admin,
  group_refresh_needed`（群发送语义纯函数）。
- `presage/src/model/groups.rs` —（B2）`Group` serde 加
  `announcements_only`（缺省 false，官方群态位）；（2026-10-11 P5 批）
  `Group` 再加 `banned_members`（缺省空，解密侧 `members_banned` 映射）+
  新 `BannedMember` 模型（uuid/service_id_type/timestamp，PendingMember
  同形状）——引擎 listGroups 投影面。
- `presage/src/errors.rs` —（B2）`GroupAnnouncementsOnly` 错误臂。
- `presage/src/manager/registered.rs`（2026-10-11 P15a 项2 批追加）— 两处
  出站 Metadata 字面量补 `ephemeral/urgent/report_spam_token: None`
  （Metadata 新字段编译面，出站消息信封位恒无）。
- `presage/src/store.rs`（同批追加）— verified sync 存储路径 Metadata
  字面量补三 `None`（同上编译面）。
- `presage/src/model/messages.rs`（2026-10-11 P15a 项3 批）— `Received::
  Contacts(EnvelopeAck)` 改 `Received::Contacts(ContactSync, EnvelopeAck)`：
  新 `ContactSync { content: Box<Content>, complete: bool,
  contacts: Vec<Contact> }` 载体（源信封 + 解析且落库成功的联系人记录 +
  complete 旗标，官方桌面 ContactSyncEvent 同位 services/
  contactSync.preload.ts；Debug 手写不展开记录体）。
- `presage/src/manager/registered.rs`（同批）— contacts sync 臂收集解析+
  落库成功记录入 ContactSync 上抛（save 循环保持上游 break-on-error 语义，
  break 后剩余条目不投影；blob 下载仍走 `retrieve_contacts` 既有附件
  下载面，零新平行机制）。
- `presage/src/manager/registered.rs`（2026-10-11 时序审计组二#4 批）—
  接收重放闸 thread scoping：`(sender, client_timestamp)` 判重从跨
  thread 全局改为同 thread 才抑制（`is_thread_scoped_redelivery` 纯函数
  + `Thread::try_from(&content)` 入向线程推导比对——同 sender 同毫秒发
  两条到两个群，第二条不再被吞；线程推不出（try_from 失败）保守沿用旧
  全局抑制；batch_seen 去重键同步加 thread 维度）。单测
  `receive_batch_tests::redelivery_gate_is_thread_scoped`。
- `presage/src/manager/registered.rs`（2026-10-11 时序审计组四 E3 批追加）—
  新增 pub `Manager::send_plaintext_content`（单收件人明文腿包装：
  `new_message_sender` → `MessageSender::send_plaintext_content`；不注入
  profile key、不 restore thread timer、不落库——重试回执非 timeline
  消息，官方 sendResendRequest 同口径）。
- `presage/src/model/messages.rs`（2026-10-11 时序审计组四 E4 批）—
  `DecryptionFailure::RetryRequest` 从裸 `original_ts` 扩为
  `{ original_ts, ratchet_key, derm_device_id, requester_device }`（官方
  RetryRequestEvent 四元：MessageReceiver.preload.ts:2690-2698——
  derm_device_id = DERM 内嵌 device_id 点名我设备，requester_device =
  信封源设备即归档地址目标）。
- `presage/src/manager/registered.rs`（同批）— DERM 收信臂按上述四元构造
  RetryRequest；DERM 无 timestamp（不点名原消息）改 ack 即弃（不投影，
  原行为会以 None 失败体上抛）。
- `presage/src/manager/confirmation.rs`（2026-10-11 D5 批）— 注册链
  `DeviceCapabilities` 字面量补 `versioned_expiration_timer: false`/
  `optional_phone_number: false` 两位（D5 字段新增的编译面，链接注册
  声明位恒无）。
- 上游升级（2026-10-12 presage 升 rev 批）— 钉定 rev 33dd149→96f5e92
  （main HEAD；tag 0.7.0 落后于原钉 rev 不适用，最新发布点仍为 33dd149
  之后的 main。5 提交：AGENTS.md 纯文档、传递 libsignal-service bump、
  PNI 接收修复、profile 解密 unwrap→`?`、群 master key 长度异常
  panic→`ThreadError::InvalidGroupMasterKey`；传递 libsignal 仍钉
  tag v0.99.0）。fork 清单逐文件重放：`store.rs`/`Cargo.toml`/
  presage-store-sqlite 全部 9 文件逐字干净重放（store-sqlite 上游零
  触碰，重放后与原 fork 逐字节一致）；一个融合点：
  - `presage/src/manager/registered.rs` — 接收 unfold 批化区（P12/R2
    大改区）整体移植到新基线；PNI sealed-sender 判定跟进上游新 API
    形状（`envelope.source_service_id` 字段访问 → `parse_source_
    service_id()`，字段已标 deprecated，语义同值）；`send_message`
    主体的 `unidentified_access.as_ref()` 为上游自带的 lss 新签名
    适配（非 fork 叠加）。上游 5 提交中 messagepipe/envelope/协议
    存储面零触碰，QueueEmpty 时序/ack 契约/identity 事件三行为锚
    上游面零变化。
- `presage/src/manager/registered.rs`（2026-10-14 story 删除扇出批 S5）—
  `Manager::send_story_message` 加第 4 参 `include_profile_key: bool`：
  profile-key 注入面从恒 StoryMessage 扩为「StoryMessage 恒注入 +
  DataMessage 调用方显式选入」（S5 名单 story DOE 逐收件人腿 DataMessage
  载体走 story 通道，官方 sendMessageToServiceId deleteForEveryone 同携
  profileKey，注入由引擎 P9 profileSharing 闸裁定）；既有调用点
  （stories 发送批）传 `false` 行为零变化。
- `presage/src/manager/registered.rs`（2026-10-16 接收完整性批①信封保留
  重试）— 接收批循环 envelope open `Err` 不再无条件丢弃：非
  SealedSenderDecryptionError 族失败信封保留进接收流内存 failed 队列
  （`WindowEnvelope{envelope, ack, attempts}`，**不 ack**——未 ack 句柄
  使服务器队列在进程存活期保留该信封，进程死亡自然重放兜底），带错误
  分类字面量（`classify_open_error` 纯函数：frame/protocol/other，
  typed path 零 payload）与重试计数；下轮接收批窗口重试（新流量先到
  并入窗口并抢占定时器；静默流 2s 定时器触发重试批
  `OPEN_RETRY_DELAY`，`futures::future::select` 竞速不阻塞新信封）；
  有界 3 次（`OPEN_RETRY_MAX_ATTEMPTS`，官方 Desktop unprocessed
  attempts 有界同构——MessageReceiver.preload.ts:1075「keeping N failed
  envelopes」+ attempts 字段；值取任务口径）后永久跳过：warn 固定字面量
  「giving up on envelope after 3 failed decryption attempts; skipped
  permanently」+ ack 放行。SealedSenderDecryptionError 族既有
  DecryptionError 槽语义不变（DERM 自愈链路不动）。presage 上游无
  unprocessed 持久化面——最小实现为流内内存队列。`classify_open_error`
  分类与界限单测 `open_error_classification_and_retry_bound`。
- `presage/src/{errors.rs,manager/registered.rs,model/messages.rs}`
  （2026-10-16 接收完整性批②群取回分型降级）— 群 update/sync 信封触发的
  群 fetch/decrypt 不再静默回退旧群态：解码类失败
  （`group_fetch_decode_class` 纯函数：GroupsV2DecryptionError /
  ProtobufDecodeError / ZkGroupDeserializationFailure /
  ZkGroupVerificationFailure / GroupsV2Error）fetch+decrypt（upsert 路径
  连 save_group）整体重试 1 次（`GROUP_FETCH_MAX_ATTEMPTS = 2`），耗尽 →
  新 `Error::GroupSyncFailed { attempts }` 结构化上抛；HTTP 404
  （`group_fetch_gone`，官方 GROUP_NONEXISTENT）→ 新 `Error::GroupGone`；
  401/403 及其余失败保留既有 warn + 本地兜底。接收流群更新腿把两结构化
  错误转 `Received::GroupSyncFailed { master_key, kind, attempts }`
  （新 `GroupSyncFailureKind::{Decode, Gone}`）随批上抛（无 ack——单 ack
  契约由同信封 Content 槽承担）；两处旁路调用点（群头像路径、发送前群态
  刷新）保持 fail-open：结构化错误 warn + `store.group` 兜底（stale 名单
  胜过路径整体失败）。分类分界单测
  `group_fetch_failure_partition_and_bound`。

`vendor/presage/presage-store-sqlite/`：

- `src/content.rs` — Metadata 解构模式补 `story: _`（E2：story 位不落库
  ——收侧 story 本就不存库，普通消息位恒 None）。
- `src/data.rs` — Metadata 重放构造填 `story: None`（E2，同上）；
  （B2）群发送语义数据面（announcements-only/成员角色读出）；
  （2026-10-11 P5 批）`into_group` 重建面补 `banned_members: Vec::new()`
（行 schema 无封禁列，下次服务器群态刷新收敛，announcements_only 同款
  fail-open 注释纪律）。
- `src/content.rs`（2026-10-11 B1/B3 追加）— ①DERM 找回面（按发送方+
  原始时间戳查已发消息、群成员名单读面）；②`kt_kv_get`/`kt_kv_set`
  （引擎自有 kv 位，prekey 维护书签跨重启）；③（P12/R2）
  `save_message_on_tx` 抽出（单条插入挪到调用方管理的事务内）+
  `save_messages` 覆盖（一窗一事务循环 on_tx + 一次 commit，
  MessageReceiver.preload.ts:1003-1123 同位）+ 批量落库整窗读回测试；
  ④`.sqlx/` 离线缓存 3 条重生成（宏搬家后源位变化、SQL 文本缩进变化
  → sha256 键失配，describe 沿用原条目）。
- `src/protocol.rs`（2026-10-11 B4）— `save_identity` ReplacedExisting 时
  入身份变更日志（`Store::take_identity_changes` 数据源，接收流逐信封
  drain）。
- `src/lib.rs`（2026-10-11 B4）— `SqliteStore` 身份变更日志字段与
  `take_identity_changes` 覆盖（Arc 共享，clone 语义不变）；
  （P12/R2）`cargo fmt` 收口重排（take_identity_changes 表达式收行，
  语义零变化）。
- `src/protocol.rs`（2026-10-11 P9 身份安全闸批追加）— ①
  `is_trusted_identity` 存储钥不匹配时按方向裁定：Receiving 放行（收向
  换钥/事件链不变）、Sending 拒绝（官方 isTrustedForSending 不匹配 →
  false 同位；libsignal 建会话/加密两路以 Sending 咨询，false →
  UntrustedIdentity → 引擎 IDENTITY_UNVERIFIED 错误面）；首次信任建立
  两向恒放行；②新增方向闸单测；③（S4）`save_identity` 换钥 archive
  会话——存储钥与新钥真异（先读旧钥比对；上游 rows_affected 把「行
  存在」当「钥变」，同钥重存会误伤）→ 同事务 `DELETE FROM sessions`
  （官方 saveIdentity 换钥 archiveSiblingSessions 同位，
  SignalProtocolStore.preload.ts:2140-2192/:2188-2192），同钥重存保留
  会话；群 outbound sender key 复位不做（引擎无群成员面，取舍登记
  compat §5）；④换钥 archive/同钥保留单测；（P12/R2）`cargo fmt`
  收口重排（测试内超宽地址行，语义零变化）。
- `Cargo.toml`（2026-10-11 P12/R2）— 废 `cdsi` feature 移除：其
  `libsqlite3-sys/bundled-sqlcipher-custom-crypto` 要求在 crates.io
  libsqlite3-sys（0.36 无此 feature）上不可解析，仅声明即令本 crate 脱离
  消费方 workspace 的独立构建全部失败；引擎从不启用 cdsi（
  `default-features=false` 且无透传）。同批加独立 `[workspace]`+
  presage/libsignal-service `[patch]`（独立测试接本 vendor 叉）。
- `src/content.rs`（2026-10-11 P15a 项2 批追加）— save 路径 Metadata 解构
  补 `ephemeral: _`/`urgent: _`/`report_spam_token: _`（信封位不落库，
  story 位同款纪律）；批窗重放构造补三 `None`。
- `src/data.rs`（同批追加）— 重放 Metadata 构造补三 `None`（同上不落库
  纪律）。
- `migrations/20261011000000_add_pre_key_created_at.sql`（2026-10-11 时序
  审计组四 E2 批）— `pre_keys` 加 `created_at INTEGER NOT NULL DEFAULT 0`
  （存量行 0 = 最旧，官方 `createdAt || 0` 同语义，无回填）。
- `src/protocol.rs`（同批）— ①`save_pre_key` 写 created_at（宏改运行时
  query，`.sqlx/` 离线缓存不动）；②`SqliteProtocolStore` 增 `kt_` 前缀
  清单/批量删除六方法：one-time `(id, created_at)`、signed/kyber `(id,
  记录内嵌时间戳[, is_last_resort])`、三族按 id 批量 remove（kyber 可删
  last-resort 行，保留窗由调用方裁定）；③`kt_pre_key_inventory_and_bulk_
  removal` 单测（时间戳/旗标/批量删除钉形）。
- `src/protocol.rs`（2026-10-12 save_identity 误报修复批）—
  `save_identity` 的 ReplacedExisting 判定从 UPDATE `rows_affected`
  改为先 SELECT 比对钥匙字节：无档 INSERT、同钥不写库直返
  `NewOrUnchanged`（无变更事件）、异钥才 UPDATE + archive + 变更
  日志——补全 P9 批只把比对用在 archive 决策、返回值/变更日志仍吃
  `rows_affected` 的缺口（上游 main 96f5e925，2026-10-12 fetch 仍把
  「行存在」当「钥变」，值得向上游提 PR）。同钥重存（会话重建/第二封
  PreKey 消息）曾误报换钥 → 引擎 identity 臂置 pending → 发送闸全拒，
  即生产验收期「所有发送被安全号码闸拒绝」根因。两条宏查询改运行时
  query（save_pre_key 先例，`.sqlx/` 离线缓存不动）；新增
  `save_identity_same_key_not_reported_as_replacement` 回归单测（同钥
  两存 NewOrUnchanged+单行+零事件、异钥 ReplacedExisting+行更新+单事件）。
- `src/manager/registered.rs`（2026-10-16 单连接纪律批）— ①删除
  `Registered` 的 per-manager `identified_websocket`/
  `unidentified_websocket` 槽字段（多 Manager 各持槽 = 同账号多并行
  identified 连接、服务器 4409 互搏的根因）；②`identified_websocket()`/
  `unidentified_websocket()` 改走 libsignal-service 进程级
  `websocket::slot` 注册表（键 = credentials login，全 Manager 共享同
  账号单连接，kill-before-create + dial 串行）；③新增 pub
  `identified_websocket_close_code()` /
  `has_open_identified_websocket()`（引擎 4409 分支判定读数面，
  `docs/websocket-connection-ledger.md` §3）。

## 更新纪律

后续再动 `vendor/`：先 `diff -rq` 对照上表 rev 的 cargo checkout 确认
改动面，再在本文件登记文件级清单；上游升级 = 重新 vendor + 重放改动
清单。
