# JSON-RPC 兼容层契约（signal-cli 兼容子集）

状态：P2 逐方法对齐基线 ｜ 依据：ADR 0002/0004 ｜ 上游参照：connector
`docs/implementation-plan.md` §4/§5 已固化语义 + `tests/fixtures/fake-signal-cli.py`
（黄金对照，pinned 0.14.x 行为投影）

本文件逐方法列出引擎对外暴露的 signal-cli JSON-RPC 兼容子集的参数/返回/错误码
与对齐状态。**兼容层封顶**：只覆盖 connector 实际驱动的白名单方法；上游新增
方法不跟进；KT 新能力走独立扩展命名空间，不伪装成上游方法。

## 1. 启动契约

connector 以固定 argv 拉起引擎（`src/engine.rs::signal_cli_args` 的形状，
fixture 同款）：

```text
<engine> --data-dir <dir> jsonRpc --receive-mode on-start [--ignore-attachments] --ignore-stories --ignore-stickers
```

环境变量（可选）：

- `KT_SIGNAL_SOCKS_PROXY=host:port`：connector 注入的出口代理（与 signal-cli
  JVM 形态同一变量）。引擎在校验后映射为 `ALL_PROXY=socks5://host:port`，
  由 libsignal-service 的 reqwest 传输面（HTTP + WebSocket 共用同一 client，
  缺省读系统/环境代理）统一走 SOCKS5；要求依赖图含 reqwest `socks` feature
  （本仓显式声明）。非法值启动即 exit 2；空值视为未配置（直连）。

- `--data-dir`：引擎全部本地状态根（真实引擎为 presage SqliteStore 目录，
  `accounts/lk-<hex>/` 每账号一库 + `engine-state.json` 注册表；注册表里不落
  除号码/目录名以外的任何内容）。
- `--receive-mode on-start`：daemon 就绪即拉取服务器消息（P1b 起 presage
  websocket 生效：每账号一条接收管线，断流退避重连）。
- `--ignore-attachments` / `--ignore-stories` / `--ignore-stickers`：接受。
  媒体开关语义（ADR 0002：去掉 `--ignore-attachments` 即下载入站附件）
  在 P2 附件阶段生效，P1a 仅接受参数。
- JVM 相关环境（`JAVA_OPTS`/`JAVA_HOME`）：引擎非 JVM，**必须忽略**，
  不得因此失败（connector 现行启动器会设置它们）。
- stdio：stdin 逐行请求，stdout 逐行响应/通知，stderr 保持安静
  （connector watchdog 以 stderr ERROR 行判死活——引擎不得误发该形状日志；
  任何日志纪律：不落消息正文/号码/URI/密钥，P1a 直接零日志）。
- 退出码：stdin EOF（含宿主粗暴断开）= 干净退出 0；无法解析的致命状态
  非零。未知参数/缺 `jsonRpc` 子命令 = 非零快速失败（与上游对未知参数
  fail-closed 同口径）。

## 2. 分帧（framing）

- JSON-RPC 2.0，换行分帧，无批量数组（上游 daemon 同款）。
- 请求：`{"jsonrpc":"2.0","id":<string|number>,"method":"...","params":{...}}`。
- 响应：`{"jsonrpc":"2.0","id":...}` + `result` / `error` 二选一。
- 通知（无 id）：`{"jsonrpc":"2.0","method":"receive","params":{...}}`。
- 行超长/非法 JSON：回 `-32700`（id 可知则带 id，不可知不带），继续服务，
  不退出。
- 未知方法：`-32601`（上游真实行为；fixture 对未知方法回显 `{method}` 是
  夹具宽容，引擎不效仿，见 §5）。

## 3. 逐方法对齐表

错误码约定：上游业务错误用 `-1`（message 原文）；JSON-RPC 协议错误用
标准码（-32601/-32700/-32603）。`getUserStatus` 的账号解绑错误原文为
`-32603` + 「Failed to send message: Authorization failed!
(AuthorizationFailedException)」（上游把 AuthorizationFailedException 包进
UnexpectedErrorException 的形状，connector 1.21 靠它识别解绑）。

| 方法 | params（关键字段） | result | 错误 | 对齐状态 |
|---|---|---|---|---|
| `startLink` | `{}` | `{deviceLinkUri}`（real：官方 `sgnl://linkdevice?uuid=…&pub_key=…`） | — | **P1b real**：presage provisioning（设备名先占位，§5）；fake：夹具形状桩 |
| `finishLink` | `{deviceLinkUri, deviceName}` | `{number}` | `-1` `Unknown device link uri.` | **P1b real**：等手机确认（阻塞不饿死其他方法），设备名本地改写 + attributes 推送（ADR 0004）；fake：形状桩 |
| `listAccounts` | `{}` | `[{"number": …}]`；无账号时空数组 | — | **P1b real**：state v2 注册表（号码→store 目录） |
| `listContacts` | `{account}` | `[{number, name?}]`（store 同步面；未注册账号 `-32603`） | 未注册账号 `-32603` | **P2 real**：presage contacts 投影；profile 拆名 P2+ |
| `listGroups` | `{account}` | `[{id(base64), name, isMember, members:[{number}]}]` | 未注册账号 `-32603` | **P2 real**：presage 群组投影（id = master key 标准 base64，send 可回传） |
| `deleteLocalAccountData` | `{account}` | `{}` | — | **P1b real**：注册表摘除 + store 目录删除 + 接收管线停机 |
| `send` | `{account, recipient[]/groupId, message, quoteTimestamp?, quoteAuthor?, previewUrl?, previewTitle?, previewDescription?, previewImage?, mentions?: [{number, start, length}], attachments?: [dataURI], sticker?: {packId(hex), packKey(base64), stickerId, emoji?, image(dataURI), width?, height?}, viewOnce?}` | `{timestamp, results:[]}` | 收件人未解析/群不可解/quote 半参/quote 在场但 author 不可解析（文本与贴纸分支同口径）/mentions 条目缺字段/附件条目非字符串/sticker 缺字段或与 message·attachments·preview·mentions 同发（官方互斥；quote 与 sticker 可共存）/viewOnce 非布尔，或 viewOnce:true 违反官方 isValidTapToView 同构（body 非空·附件≠1·附件 mediatype 非 image\|video·与 quote·preview·mentions·sticker 同发——mentions 亦禁发，比官方更严）`-32603` | **P2 real**：直聊逐号码 + 群组 `send_message_to_group`；quote id/author→ACI + text 本机 store 找回（signal-cli 同行为，找不到缺省）；preview 面 + previewImage 随附件同批 CDN 上行（官方 Preview.image 同构）；mentions→BodyRange.MentionAci；attachments 为 connector 上游唯一形状（data URI 数组，`data:<type>[;filename=<pct>][;voice=true];base64,<payload>`，voice=true 映射官方 VOICE_MESSAGE flags（presage voice_note 同构，2026-10-04））→ presage 官方 upload_attachments 上行（≤32 条、单条 ≤100MB，超限 fail-closed）；sticker 贴纸面（2026-10-04，官方 sendStickerMessage 同构）：image data URI 上行（mediatype 必须 image/*，width/height 官方 AttachmentPointer 展示位透传），packId hex/packKey base64（官方 Bytes.fromHex/fromBase64 同构），body=undefined 无常规附件；sticker 与 quote 共存（2026-10-04，官方「贴纸回应」StagedStickerReply = 同一 DataMessage 带 sticker+quote，非 reaction——Reaction proto 无贴纸位）：quote 组装与文本分支同源（assemble_quote 单点），author 不可解析 fail-closed、被引正文 store 找回失败降级缺 text，body 维持 undefined；viewOnce 阅后即焚面（2026-10-04，官方 isViewOnce=14 同名直投）：`viewOnce: true` → body 空 + 恰 1 附件 + `is_view_once=Some(true)` 的 DataMessage（官方发送侧不 bump requiredProtocolVersion——构造面不设该字段，presage send 面注入默认 0 = 官方初值），直聊/群组两形态同构，与 disappearing messages 完全独立 |
| `remoteDelete` | `{account, targetTimestamp, recipient/groupId}` | `{}` | 同 send | **P2 real**：直聊 + 群组 |
| `sendReaction` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, emoji, remove}` | `{}` | 同 send | **P2 real**：targetAuthor 号码↔ACI 解析（本机号走注册数据，contacts 命中 E.164，UUID 形状直通） |
| `updateContact` | `{account, recipient(单字符串), name}` | `{}` | 未命中号码：`-32603` | **P1b real**：本地 store 命中即改名；服务器面 P2 |
| `sendTyping` | `{account, recipient[]/groupId, stop(显式布尔)}` | `{}` | 同 send | **P2 real**：TypingMessage Started/Stopped 直聊 + 群组 |
| `sendPinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, pinDurationSeconds?}` | `{}` | 同 send；pinDurationSeconds 超 u32 `-32603` | **P2 real**：官方 pinMessage=27；字段缺席/null=pinDurationForever |
| `sendUnpinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp}` | `{}` | 同 send | **P2 real**：官方 unpinMessage=28 |
| `sendAdminDelete` | `{account, groupId, targetAuthor, targetTimestamp}` | `{}` | 无 groupId `-32603`（直聊无 admin 概念） | **P2 real**：官方 adminDelete=29，仅群组 |
| `sendDeliveryReceipt` / `sendReadReceipt` / `sendViewedReceipt` | `{account, recipient(单作者), timestamps[]}` | `{}` | timestamps 空/缺/含非数字 `-32603` | **P2 real**：官方 ReceiptMessage type=delivery/read/viewed；逐作者直发（群组会话也直发原作者，官方不整组广播）；timestamps 按 CHUNK_SIZE=100 分帧 |
| `sendViewOnceOpen` | `{account, senderAci(ACI/号码), timestamp(原消息 upstream 时间戳)}` | `{}` | 同 send；senderAci 不可解析/参数缺失或形状错 `-32603` | **P2 real（2026-10-04 阅后即焚扩面）**：官方 syncViewOnceOpen 同构——向自己 ACI 发 SyncMessage.ViewOnceOpen{senderAci, timestamp}（content oneof viewOnceOpen=11，padded；多设备同步「该阅后即焚已被本机打开」），presage self-send 通路验证可行（§5，含钉定 rev 同内容二次转录重发怪癖）；与 sendViewedReceipt（作者面回执）双通道独立；fake：成功 `{}` |
| `getUserStatus` | `{account, recipient}` | `{"isRegistered": true}` | 解绑态 `-32603`（§3 头原文）；未注册账号/其他失败 `-32603` | **P2 real**：whoami 轻量鉴权探测（401→解绑原文，网络失败不误报解绑）；fake 恒已注册 |
| `getStickerPackManifest` | `{packId(hex 32 小写), packKey(base64 32B)}`（匿名 CDN 只读，无 account） | `{title, author, cover?: {id, emoji?, contentType?} \| null, stickers: [{id, emoji?, contentType?}], stickerCount}`（官方 parseStickerPackManifest 同构：≤1024 截断；cover 官方缺省 null 由端上兜底 stickers[0]；cover 与 stickers 都空 malformed） | 参数缺失/packId 非 32 位小写 hex/packKey 非 32B base64、解密 MAC 失败（与密文损坏密码学不可区分）→ `STICKER_PACK_KEY_INVALID`；CDN 失败 → `STICKER_PACK_FETCH_FAILED`；padding/proto 解码失败 → `STICKER_PACK_MALFORMED`；均 `-1` + message 码位（connector 1.36 白名单逐字，§4.32） | **P4 real**：cdn.signal.org `stickers/{packId}/manifest.proto` 匿名 GET + attachment_cipher 解密；fake：参数面 fail-closed 后 `STICKER_PACK_FETCH_FAILED` |
| `getStickerImage` | `{packId, packKey, stickerId}` | `{dataBase64, contentType, size}` | 同 getStickerPackManifest，另单图 >300KB → `STICKER_IMAGE_TOO_LARGE` | **P4 real**：`stickers/{packId}/{stickerId}` 匿名 GET + 解密 + WHATWG mimesniff image 魔数嗅探 contentType（未识别回退 `image/webp`）；fake 同 getStickerPackManifest |
| `getPinnedConversations` | `{account}` | `{pinned: [{conversationId, kind: contact\|group\|legacyGroup}]}`（存储序；存储条目映射不出引擎口径的跳过不炸面） | 未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P4 real**：Storage Service manifest → Account item → `AccountRecord.pinned_conversations` 投影；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{pinned: []}` |
| `setConversationPinned` | `{account, conversationId, kind: contact\|group\|legacyGroup, pinned(bool)}` | `{pinned: [...]}`（写后全量，同 getPinnedConversations 形状） | 云引用解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次/写面冲突重放中失败 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/未知 kind/未注册账号 `-32603` | **P4 real**：官方 upload 语义（读改写 AccountRecord 仅动 pinned 字段、prost round-trip 保真其余字段、新 16B item id、version+1/source_device/record_ikm 透传）+ `PUT v1/storage/` + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；写面为 presage 上游缺口自建薄封装（上游补齐后替换）；fake：成功回 `{pinned: []}` |
| `getStickerPackSyncs` | `{account}` | `{packs: [{packId(hex 32 小写), packKey(base64 32B)\|null, position, deletedAtTimestampMs}]}`（存储序 = manifest STICKER identifiers 首现序；坏条目跳过不炸面；墓碑 packKey=null、带 deletedAtTimestampMs） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real**：Storage Service manifest → 全部 type=6 STICKER identifier 批量读 → StickerPackRecord 投影（记录面为 presage 上游 proto 缺口本地定义 + 加解密 shim，§5）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{packs: []}` |
| `setStickerPackSync` | `{account, packId(hex 32 小写), installed(bool), packKey?(base64 32B), position?}`（install 必带有效 packKey；uninstall 忽略 packKey 内容与 position，官方墓碑形态两字段皆不写） | `{packs: [...]}`（写后全量，同 getStickerPackSyncs 形状） | PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（packId 非小写 hex、install 缺/短/非字符串 packKey、packKey 非字符串、position 超 u32 或非数值）/未注册账号 `-32603` | **P5 real**：官方 toStickerPackRecord 语义（install 幂等：同 key 已装且未要求挪位→no-op；position 缺省=保持现位/无现位本地 MAX+1；uninstall 墓碑 deletedAtTimestampMs=引擎时钟，不物理删旧 record）+ 读改写仅动该 pack record + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；fake：成功回 `{packs: []}` |

`receive` 通知：`{jsonrpc, method:"receive", params:{account, envelope}}`；
P2-A 起投影覆盖 connector 1.15/1.24/1.25 的全部消费面（钉定 signal-cli
0.14.8 JSON，见 connector implementation-plan §4.13/§4.19/§4.20）：

- 信封公共位：`sourceUuid`（ACI service id 字符串）、`sourceDevice`、
  `timestamp`；contacts 命中时附 `sourceNumber`（E.164）与 `sourceName`。
- `dataMessage`：`timestamp`、`message`、`quote {id, author, text?}`、
  `reaction {emoji, targetAuthor, targetSentTimestamp, isRemove}`、
  `remoteDelete {timestamp}`、`attachments[] {id, contentType, filename?,
  size?, width?, height?, caption?, isVoiceNote}`（≤32，元数据 only，
  字节永不下载）、`previews[] {url, title?, description?, image?}`（≤4）、
  `mentions[] {number, start, length}`（≤64，来自 bodyRanges）、
  `viewOnce: true`；viewOnce 行跑官方 isValidTapToView 同构校验，非法补
  `viewOnceInvalid: true`（2026-10-04，官方 isTapToViewInvalid 同位；引擎
  metadata-only 只标记不擦除，§5）。引用/反应作者号 contacts 命中投影
  E.164，未命中保留 ACI 字符串。textStyles：本引擎钉定的 presage proto
  版本无此面，不投影。
- `editMessage {targetSentTimestamp, dataMessage}`（直聊）。
- `typingMessage {action: STARTED|STOPPED, timestamp}`。
- `receiptMessage {timestamp[], when, isDelivery, isRead, isViewed}`
  （connector §4.19 三档面）。
- `pinMessage {targetAuthor, targetSentTimestamp, pinDurationSeconds?}` /
  `unpinMessage {targetAuthor, targetSentTimestamp}` / `adminDelete
  {targetAuthor, targetSentTimestamp}`（官方 27/28/29 投影，pinDuration
  缺席 = forever）。
- `sticker {packId(hex), packKey(base64), stickerId, emoji?, data?}`
  （官方 DataMessage.Sticker 序列化同构；data 走附件指针投影，元数据
  only，2026-10-04）。
- `syncMessage.sentMessage`：本机其他设备发出的直聊（timestamp/message/
  destinationNumber|destinationUuid + 同 dataMessage 控制面）。
- `syncMessage.viewOnceOpen {senderAci, timestamp}`（2026-10-04，官方
  content oneof viewOnceOpen=11 投影）：本机主设备/其他设备已打开某阅后
  即焚消息，上层据此把 (senderAci, timestamp) 命中的行置 Viewed 并执行
  字节焚毁；senderAci 解析 binary 优先 string 兜底（官方 parse_sender_aci
  同构），不可解析整事件丢弃（乱序兜底 warn-only，§5）；timestamp 缺席
  投影 0 不丢弃。「找不到目标行」的丢弃决策在 connector 消费面执行
  （warn-only、不暂存，§5）。
- 群组接收投影：dataMessage.groupV2 / sent 内群组带 `groupInfo {groupId}`
  （master key 标准 base64，signal-cli 同款；直聊不带该键）；群更新类消息
  （成员变更/权限面）仍跳过。call/story/null 体跳过；解密失败按事件丢弃。

## 4. 夹具专有动词（非引擎表面）

`hang/sendHang/crash/crashSend/malformed/oversized/stallStdin/
armDelayedReceive/emitReceive/emitStderr/duplicate/emitEnvelope` 是
connector 测试驱动夹具的控制面，**引擎不实现也不接受**（按 §2 未知方法
拒绝）。引擎自身的失败路径由其契约测试直接驱动（超长行/坏行/EOF 等）。

## 5. 已知差异登记

- 未知方法：上游 `-32601`；夹具回显。引擎取上游行为（-32601）。
- `listAccounts` 的多账号投影、群组 Sender Keys、附件下载：夹具用标记
  文件模拟的部分，引擎在对应阶段（P1b/P3/P4）以真实实现替换；替换前的
  桩行为都在本表「对齐状态」列注明，不得当成已完成。

P2 real 模式差异（connector 侧需知的边界，逐条 ADR 0004 对齐）：

- **群组面**：发送 P2 起 real（`send_message_to_group`，群 id = master key
  标准 base64）；接收投影仍跳过群组消息（connector 群组行是已声明边界，
  P3 补 groupInfo 投影后放开）。群 id 不可解/非法按 `-32603`。
- **号码解析冷启动窗口**：send 系方法的 recipient 号码 → ServiceId 经本地
  contacts 解析；链接后手机端自动推送 contacts，推送落地前的窗口内发送按
  `-32603` 内部失败。connector 侧行为不变（失败即报，不重试语义未变）。
- **reaction targetAuthor**：协议字段是 ACI UUID；本机号走注册数据（回应
  自己的消息），其余号码经 contacts 解析，UUID 形状直通；1:1 均未命中按
  内部失败，群内保留原样由协议侧报错。
- **作者号投影方向**（receive）：quote.author / reaction.targetAuthor 在
  contacts 命中时投影 E.164（signal-cli 行为），未命中保留 ACI 字符串。
- **设备名占位窗口**：startLink 即以占位名 `KT Signal Engine` 完成链接
  （provisioning 协议要求先给名字）；finishLink 的 `deviceName` 在本地
  store 改写 + 接收管线首推 attributes 覆盖为真名。窗口内设备管理器里
  可见占位名，属已知一次性噪音。
- **URI 形状**：real 模式发官方 `sgnl://linkdevice?uuid=…&pub_key=…`；
  fake 夹具保持 `sgnl://link?…`（仅测试）。连接器对 URI 不解析、只透传
  回 finishLink，两侧均不受影响。
- **per-recipient results**：成功仅含 `{recipientNumber, success:{timestamp}}`，
  群组发送 `results` 恒 `[]`（成员级投影 P3）；部分失败的 result 形状
  （上游 identity failure / unregistered user 等枚举）未对齐，遇到即整体
  `-32603`（P3 逐枚举对齐）。
- **updateContact 边界**：仅本地 store 已有该号码时改名；新联系人建立需要
  contacts 同步/服务器面（P2+），未命中 `-32603`。
- **getUserStatus**：P2 起 real——whoami 轻量探测；仅服务器 401 映射解绑态
  原文（`-32603`），网络等其他失败按 `-32603` "Internal error" 不误报解绑。
  recipient 维度的注册态需 CDSI 查询，未做（保持 `isRegistered: true` 面）。
- **textStyles**：本引擎钉定的 presage proto 版本无该面，receive 不投影
  （connector 侧为可选富体字段，缺失即无该键）。
- **finishLink 长等待**：等手机确认可阻塞数分钟；引擎在本线程 LocalSet
  并发 dispatch，其他方法不饿死。

P4 扩面差异（2026-10-04，connector 1.36 契约对齐）：

- **置顶写面是上游缺口**：libsignal-service-rs 只有 Storage Service 读面，
  `setConversationPinned` 的写（`PUT v1/storage/` + WriteOperation）为引擎
  自建薄封装（单点 `put_write_operation`），上游补齐后整段替换；路径与
  官方桌面 WebAPI.preload.ts:831 同一端点。
- **账号从未有 storage 状态**（无 master key 或 manifest 404）：读面回空
  列表；写面先生成 32B master key 落库，再以 version 1 + 最小
  AccountRecord（只含 `pinned_conversations`）建初始 manifest——与官方
  首次启用 Storage Service 的写方播种语义同构；手机侧并发首建的竞争由
  409 采纳重放兜底（服务器 manifest 为准）。
- **结构化错误码 8 枚**（`-1` + message，connector 1.36 白名单逐字）：
  贴纸 `STICKER_PACK_KEY_INVALID`/`STICKER_PACK_FETCH_FAILED`/
  `STICKER_PACK_MALFORMED`/`STICKER_IMAGE_TOO_LARGE`；置顶
  `CONVERSATION_NOT_RESOLVED`/`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`/
  `STORAGE_WRITE_FAILED`。参数缺失/未知 kind/未注册账号不占用结构化码，
  一律 `-32603` fail-closed。解密 MAC 失败与密文损坏密码学不可区分，统一
  `STICKER_PACK_KEY_INVALID`；截断密文同样落该码（MAC 域先于 padding 校验）。
- **置顶投影口径**：`AccountRecord.pinned_conversations` 数组序即置顶序；
  contact 条目以 serviceIdBinary/serviceId 任一命中反投影引擎会话标识
  （本机号走注册数据、号码经 contacts、UUID 直通），group/legacy 按标准
  base64/原样字节往返；`ReleaseNotes` 等非引擎口径条目在增删时原样保留、
  在读投影中跳过。

P5 扩面差异（2026-10-04，贴纸包安装状态 Storage Service 同步）：

- **贴纸包记录面是 presage 上游 proto 缺口**：本仓钉定 rev 的
  StorageService.proto 无 `STICKER_PACK=6` identifier 类型与
  StickerPackRecord 定义（`storage_record::Record` oneof 只到 tag 9，
  prost 解码跳过 tag 6），上游 read/decrypt 链路读不出贴纸记录。引擎
  本地持有 StickerPackRecord prost 定义 + StorageRecord tag=6 线格式
  镜像 + item 加解密 shim（与上游私有派生双向交叉验证钉定），读面以
  `read_raw_items` 原样批量读；Account 面仍走上游 pub API。上游补齐
  proto 后整段替换（单点 `service/storage_sticker.rs` + shim）。
- **position 口径**：写面 `position` 缺省/null = 保持现位（无现位 =
  本地最大 position + 1，首装即 0）；官方排序语义 = position 升序、
  packId hex 字典序并列——读投影按存储序（manifest STICKER identifiers
  首现序）原样输出，引擎层不重排，排序是消费方（connector/端上）职责。
- **墓碑语义**：uninstall 不物理删除旧 record——写
  `deletedAtTimestampMs` 墓碑（引擎时钟），packKey/position 皆不写
  （官方 toStickerPackRecord 墓碑形态同构）；读投影墓碑条目
  packKey=null 且保留位置，由 `deletedAtTimestampMs` 判定卸载态。
- **跳过即登记（零 stderr 纪律）**：manifest 中映射不出的坏条目
  （packId 空/安装态缺 packKey）读面静默跳过不炸面，不落任何日志
  （stderr 安静 = connector watchdog 活性判据），以本节登记替代
  运行时告警。
- **v1 单通道**：不做 legacy `syncMessage.stickerPackOperation` 同步
  通道；Storage Service 是贴纸包安装状态唯一真相源，connector 侧以
  `getStickerPackSyncs`/`setStickerPackSync` 为准。

阅后即焚扩面差异（2026-10-04，官方 view-once 对齐 E1-E4）：

- **E2 只标记不擦除**：官方对非法 incoming 阅后即焚 = `isTapToViewInvalid`
  标记 + `eraseMessageContents`（附件字节就地擦除）；引擎 metadata-only
  （附件字节永不下载）无字节可擦——非法行只投影 `viewOnceInvalid: true`，
  字节擦除责任在消费方（connector 落库面/端上）。官方对 sent（本机其他
  设备发出）的阅后即焚发出即擦内容，引擎的 sent 同步投影同样只标记不擦。
- **E2 校验面与官方 isValidTapToView 逐项同构**：body 空（官方 JS
  `if (body)` 同口径，空串即合法）∧ 恰 1 附件 ∧ 媒体族 image|video ∧ 无
  quote/sticker/contact/preview。媒体族判定官方为 Chrome 支持集枚举表
  （avif/bmp/gif/jpeg/webp/png/apng/ico 系 + mp4/ogg/webm），引擎
  metadata-only 无渲染面按 `image/*|video/*` 前缀族宽判（官方集外但手机端
  合法的 video/quicktime 等不被误标 invalid）。mentions 官方校验面不含，
  接收面同构不查；发送面（E1）比官方更严：mentions 亦禁发（-32603）。
- **E1 不 bump requiredProtocolVersion**：官方发送侧对 isViewOnce 不抬
  协议版本（requiredProtocolVersion 维持默认 0），收侧 requiredVersion>8
  的 unsupported 判定与 view-once 无关——引擎构造面不设该字段，presage
  send 面注入默认 0 = 官方初值。
- **E3 乱序兜底 warn-only、不暂存**：官方收到 viewOnceOpen sync 先进程内
  暂存（ViewOnceOpenSyncs map），迟到目标消息经 modifyTargetMessage 补
  匹配；引擎 metadata-only 不持消息行，暂存面归 connector——引擎只投影：
  senderAci 不可解析（binary 坏 + string 缺）的 sync 事件整体丢弃（零
  stderr，以本节登记替代运行时告警），timestamp 缺席投影 0 不丢弃；
  「找不到目标行」的丢弃决策由 connector 在消费投影时执行（warn-only，
  不暂存不重放）。
- **E4 presage self-send 验证结论：可行（不降级）**：presage
  `Manager::send_message` 对自 ACI 发 SyncMessage 即官方
  sendIndividualProto(ownAci) 同型线面（presage 上游 request_contacts 同
  路径），sendViewOnceOpen 保留官方双通道语义；与 sendViewedReceipt（作者
  面 VIEWED 回执）互相独立——官方 markViewOnceMessageViewed 对 incoming
  先发 VIEWED 再发 ViewOnceOpen sync（桌面 VIEWED 有开关门控，引擎不管
  开关，两通道都提供），收到 sync 的设备走 fromSync 路径不再外发。
  已知怪癖（钉定 rev）：libsignal-service `MessageSender::send_message`
  对「多设备 + 自发 SyncMessage」在首次发送后经 `needs_sync ||
  is_multi_device` 转录分支追加一次同内容重发——每次 sendViewOnceOpen
  其他设备会收到两条同内容 ViewOnceOpen 信封；接收端处理幂等（重复打开 =
  已 viewed no-op），语义不受影响，不做硬绕。
- **与 disappearing messages 完全独立**：阅后即焚面不读不写
  expireTimer/expireTimerVersion，不触碰 DisappearingMode 状态；两类
  「焚」互不赋值、互不触发。

## 6. fake 服务模式（仅测试）

`KT_ENGINE_FAKE_SERVICE=1` 显式开启（生产缺省永远走 real）：P1a 形状桩
原样保留——`startLink` 回夹具形状 URI、`finishLink` 合成 `+1555000xxxx`
号码、send 系方法只落调度日志不触网。用途：IPC 契约测试在无网络环境下
钉定连接器可见的形状/错误码/日志纪律（`tests/contract.rs`）。该开关不是
运行模式切换，连接器生产环境禁止设置。
