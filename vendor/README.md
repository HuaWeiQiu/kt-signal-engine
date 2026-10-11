# vendor/ — 原样拷贝的上游依赖（Stories 批，2026-10-08）

本目录当前是两个上游仓在引擎钉定 rev 的**逐字节原样拷贝**（可 `diff -rq`
对照 cargo git checkout 复核），经根 `Cargo.toml` 的 `[patch]` 段替换对应
git 源。story 协议面的最小叠加随本批后续 commit 落地，届时在本文件登记
文件级改动清单。

| 目录 | 上游 | 钉定 rev | 许可 |
|---|---|---|---|
| `libsignal-service/` | github.com/whisperfish/libsignal-service-rs | `9e6c08b8e6d413391831dc49065e5b05490d6c82` | AGPL-3.0-only（LICENSE.md 随目录） |
| `presage/presage` | github.com/whisperfish/presage | `33dd1491130793390e356e15a5711f1e6ca14fdb` | AGPL-3.0-only（LICENSE.md 随目录） |
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
  fetch_group_with_endorsements 三调用点改守卫解引用，行为面零变化。
- `presage/src/store.rs` — verified sync 存储路径 Metadata 字面量补
  `story: None`（E2，同上纯编译面）；（B4）`Store::take_identity_changes`
  默认空实现（身份变更日志 drain 面，其他 store 无感）。
- `presage/src/model/messages.rs` — ①（A③/B1）`Received::DecryptionError`
  变体 + `DecryptionFailure{RetryReceipt, RetryRequest}`；②（A③）
  `Received::Duplicate` 变体；③（B4）`Received::IdentityChange(ServiceId,
  Box<Content>, EnvelopeAck)` 变体（本体随信封照常投影、单次 ack）。
- `presage/src/manager/mod.rs` — ①（A⑤）`pub use
  unidentified_access_or_identified`；②（B2）`pub use group_member_is_admin,
  group_refresh_needed`（群发送语义纯函数）。
- `presage/src/model/groups.rs` —（B2）`Group` serde 加
  `announcements_only`（缺省 false，官方群态位）；（2026-10-11 P5 批）
  `Group` 再加 `banned_members`（缺省空，解密侧 `members_banned` 映射）+
  新 `BannedMember` 模型（uuid/service_id_type/timestamp，PendingMember
  同形状）——引擎 listGroups 投影面。
- `presage/src/errors.rs` —（B2）`GroupAnnouncementsOnly` 错误臂。

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
  （引擎自有 kv 位，prekey 维护书签跨重启）。
- `src/protocol.rs`（2026-10-11 B4）— `save_identity` ReplacedExisting 时
  入身份变更日志（`Store::take_identity_changes` 数据源，接收流逐信封
  drain）。
- `src/lib.rs`（2026-10-11 B4）— `SqliteStore` 身份变更日志字段与
  `take_identity_changes` 覆盖（Arc 共享，clone 语义不变）。
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
  compat §5）；④换钥 archive/同钥保留单测。

## 更新纪律

后续再动 `vendor/`：先 `diff -rq` 对照上表 rev 的 cargo checkout 确认
改动面，再在本文件登记文件级清单；上游升级 = 重新 vendor + 重放改动
清单。
