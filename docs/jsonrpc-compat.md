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
UnexpectedErrorException 的形状，connector 1.21 靠它识别解绑）。2026-10-10
起 send 系对服务端 401 凭据拒绝复用同一原文（`SendError::Unbound` 归型，
connector 分类器零改动命中，§5 发送链凭据失效归一批）。2026-10-11 起
send 链再结构化三面（错误码结构化批，§5）：服务端限流（429/413）
`-32603` + 「Failed to send message: Rate limited. (RateLimitExceeded)」、
人机验证（428）`-32603` + 「Failed to send message: Proof required.
(ProofRequiredException)」、服务端不可用（5xx）`-32603` + 「Failed to
send message: Signal server unavailable.
(ServerUnavailableException)」——均括注异常类名锚词与 connector 分类器
配套（限流/验证不自动重试、5xx 建议可重试，决策在 connector/desktop）。

| 方法 | params（关键字段） | result | 错误 | 对齐状态 |
|---|---|---|---|---|
| `startLink` | `{}` | `{deviceLinkUri}`（real：官方 `sgnl://linkdevice?uuid=…&pub_key=…`） | — | **P1b real**：presage provisioning（设备名先占位，§5）；fake：夹具形状桩 |
| `finishLink` | `{deviceLinkUri, deviceName}` | `{number}` | `-1` `Unknown device link uri.` | **P1b real**：等手机确认（阻塞不饿死其他方法），设备名本地改写 + attributes 推送（ADR 0004）；fake：形状桩 |
| `listAccounts` | `{}` | `[{"number": …}]`；无账号时空数组 | — | **P1b real**：state v2 注册表（号码→store 目录） |
| `listContacts` | `{account}` | `[{number, name?}]`（store 同步面；未注册账号 `-32603`） | 未注册账号 `-32603` | **P2 real**：presage contacts 投影；profile 拆名 P2+ |
| `listGroups` | `{account}` | `[{id(base64), name, isMember, members:[{number}], terminated(bool)}]` | 未注册账号 `-32603` | **P2 real**：presage 群组投影（id = master key 标准 base64，send 可回传）；`terminated` 位 2026-10-06 扩（KT 扩展批：进程内登记 + 服务器群态读面，§5 群终止批；fake 恒空列表该键不出现） |
| `deleteLocalAccountData` | `{account}` | `{}` | — | **P1b real**：注册表摘除 + store 目录删除 + 接收管线停机 |
| `send` | `{account, recipient[]/groupId, message, quoteTimestamp?, quoteAuthor?, previewUrl?, previewTitle?, previewDescription?, previewImage?, mentions?: [{number, start, length}], textStyle?: [{style(BOLD\|ITALIC\|SPOILER\|STRIKETHROUGH\|MONOSPACE), start(u32), length(u32)}], attachments?: [dataURI], sticker?: {packId(hex), packKey(base64), stickerId, emoji?, image(dataURI), width?, height?}, viewOnce?}` | `{timestamp, results:[]}` | 收件人未解析/群不可解/quote 半参/quote 在场但 author 不可解析（文本与贴纸分支同口径）/mentions 条目缺字段/textStyle 条目词表外（五值白名单；NONE 一并拒——官方 composer 永不发出的 no-op 标记）·缺字段·style 非字符串·start/length 超 u32·>64 条（cap 镜像整拒不截断）·非数组/附件条目非字符串/sticker 缺字段或与 message·attachments·preview·mentions 同发（官方互斥；quote 与 sticker 可共存）/viewOnce 非布尔，或 viewOnce:true 违反官方 isValidTapToView 同构（body 非空·附件≠1·附件 mediatype 非 image\|video·与 quote·preview·mentions·sticker 同发——mentions 亦禁发，比官方更严）`-32603`；群组形态目标已终止 `SEND_TARGET_TERMINATED`（`-1` + message，2026-10-06 群终止批，§5）；announcements-only 群非 admin 发送系整拒 `SEND_TARGET_ANNOUNCEMENTS_ONLY`（`-1` + message，2026-10-11 群发送语义批，§5）；账号解绑（服务端 401 凭据拒绝）`-32603` + getUserStatus 同款解绑原文（2026-10-10 凭据失效归一批，`SendError::Unbound`，确定性失败不重试，§5）；服务端限流（429/413）`-32603` + `Rate limited` 原文、人机验证（428）`-32603` + `Proof required` 原文（两者不自动重试）、服务端不可用（5xx）`-32603` + `Signal server unavailable` 原文（建议可重试——重试决策均在 connector/desktop；2026-10-11 错误码结构化批，§5）；对端身份钥变更未确认（身份拒发闸：协议层旧钥会话/换钥 bundle 拒加密 + 服务层 sidecar 挂起闸，直聊内容/撤回/回应/pin/unpin/直聊通话信令/回执面生效，typing/群发送/story 不在闸面）`-32603` + `Failed to send message: Identity unverified. (UntrustedIdentityException)` 原文（确定性失败不重试；确认钩子 = `trust` 写面，2026-10-11 P9 身份拒发批，§5） | **P2 real**：直聊逐号码 + 群组 `send_message_to_group`；quote id/author→ACI + text 本机 store 找回（signal-cli 同行为，找不到缺省）；preview 面 + previewImage 随附件同批 CDN 上行（官方 Preview.image 同构）；mentions→BodyRange.MentionAci；textStyle→BodyRange.Style 五值词表单点映射（2026-10-06 透传批，connector 契约 1.69 §4.62 引擎侧缺口闭合：UTF-16 offsets 透传不查 body 边界——官方 parseTextStyles 同口径，与 mentions 同款调用方纪律，详见 §5）；attachments 为 connector 上游唯一形状（data URI 数组，`data:<type>[;filename=<pct>][;voice=true];base64,<payload>`，voice=true 映射官方 VOICE_MESSAGE flags（presage voice_note 同构，2026-10-04））→ presage 官方 upload_attachments 上行（≤32 条、单条 ≤100MB，超限 fail-closed）；sticker 贴纸面（2026-10-04，官方 sendStickerMessage 同构）：image data URI 上行（mediatype 必须 image/*，width/height 官方 AttachmentPointer 展示位透传），packId hex/packKey base64（官方 Bytes.fromHex/fromBase64 同构），body=undefined 无常规附件；sticker 与 quote 共存（2026-10-04，官方「贴纸回应」StagedStickerReply = 同一 DataMessage 带 sticker+quote，非 reaction——Reaction proto 无贴纸位）：quote 组装与文本分支同源（assemble_quote 单点），author 不可解析 fail-closed、被引正文 store 找回失败降级缺 text，body 维持 undefined；viewOnce 阅后即焚面（2026-10-04，官方 isViewOnce=14 同名直投）：`viewOnce: true` → body 空 + 恰 1 附件 + `is_view_once=Some(true)` 的 DataMessage（官方发送侧不 bump requiredProtocolVersion——构造面不设该字段，presage send 面注入默认 0 = 官方初值），直聊/群组两形态同构，与 disappearing messages 完全独立；2026-10-11 群发送语义批（§5）：发送前刷群态（typing 免刷、取态失败回落本地 fail-open）+ 群形态 `results` 逐成员填充（成功/失败条目与直聊同构，type 四词表 additive）+ transcript 无论成败一律落库（群黑洞消除）+ 单成员 profile 行不可读降级 identified 不炸批 |
| `sendStory` | `{account, message(非空), recipients(非空字符串数组——名单展开集，成员解析归 connector), distributionListId?(非空字符串，缺省官方 MY_STORY_ID 零 UUID), allowsReplies?(布尔，缺省 false=线上 proto 缺席位), textStyle?(单词 DEFAULT\|REGULAR\|BOLD\|SERIF\|SCRIPT\|CONDENSED，缺省 DEFAULT=proto 字段省略), bodyRanges?: [{style(BOLD\|ITALIC\|SPOILER\|STRIKETHROUGH\|MONOSPACE), start(u32), length(u32)}] ≤64, previewImage?(非空 data URI), previewUrl?(非空 ≤2048 字符且 https:// 开头), previewTitle?(非空 ≤1024 UTF-8 字节)}` | `{timestamp, results:[{recipientNumber, success:{timestamp}}]}`（fake `results` 恒空） | account/message 缺失或空串/recipients 缺失·空批·单字符串形态·条目非字符串·空串/distributionListId 空串·非字符串/allowsReplies 非布尔/textStyle 词表外（含内联样式词与大小写漂移）·非字符串/bodyRanges 条目缺字段·style 词表外·>64 条（cap 镜像整拒不截断）·非数组/previewImage 非字符串·空串/previewUrl 非字符串·空串·非 `https://` 开头·>2048 字符/previewTitle 非字符串·空串·>1024 UTF-8 字节 `-32603`；号码不可解析/收件人发送失败/未注册账号 同 send 折叠 `-32603` | **2026-10-08 real（KT 扩展，Stories 发送批）**：官方 sendStory 行为对齐——text story 单面（群 story/文件 story v1 不做）；textAttachment{text, textStyle, preview.image=previewImage 随批 CDN 上行} 构造 + profileKey 由 fork send 面注入；preview 面 2026-10-09 扩：previewUrl/previewTitle 如实进 proto preview.url/preview.title（两键互相独立可选，image/url/title 三件任一在场即成 Preview——官方 preview 面 image 可空同构 `hydratedPreview.image && upload`；接收面投影见 Stories 接收批）；解析全量前置→按解析后 ServiceId 去重保首现序→单时间戳逐收件人 sealed-sender `?story=true` 分发（任一失败整体 `-32603`，per-recipient 部分失败形状未对齐纪律 §5）→≥1 成功后 best-effort 自同步 `SyncMessage.Sent{story_message, story_message_recipients}`（官方「≥1 成功且有其他设备」同闸，同步失败不回滚）；urgent 位钉定 rev proto 无此字段（偏差登记 §5）；详见 §5 Stories 发送批 |
| `remoteDelete` | `{account, targetTimestamp, recipient/groupId}` | `{}` | 同 send | **P2 real**：直聊 + 群组 |
| `sendReaction` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, emoji, remove}` | `{}` | 同 send | **P2 real**：targetAuthor 号码↔ACI 解析（本机号走注册数据，contacts 命中 E.164，UUID 形状直通） |
| `updateContact` | `{account, recipient(单字符串), name}` | `{}` | 未命中号码：`-32603` | **P1b real**：本地 store 命中即改名；服务器面 P2 |
| `sendTyping` | `{account, recipient[]/groupId, stop(显式布尔)}` | `{}` | 同 send | **P2 real**：TypingMessage Started/Stopped 直聊 + 群组 |
| `sendPinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, pinDurationSeconds?}` | `{}` | 同 send；pinDurationSeconds 超 u32 `-32603` | **P2 real**：官方 pinMessage=27；字段缺席/null=pinDurationForever |
| `sendUnpinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp}` | `{}` | 同 send | **P2 real**：官方 unpinMessage=28 |
| `sendAdminDelete` | `{account, groupId, targetAuthor, targetTimestamp}` | `{}` | 无 groupId `-32603`（直聊无 admin 概念） | **P2 real**：官方 adminDelete=29，仅群组 |
| `sendDeliveryReceipt` / `sendReadReceipt` / `sendViewedReceipt` | `{account, recipient(单作者), timestamps[]}` | `{}` | timestamps 空/缺/含非数字 `-32603` | **P2 real**：官方 ReceiptMessage type=delivery/read/viewed；逐作者直发（群组会话也直发原作者，官方不整组广播）；timestamps 按 CHUNK_SIZE=100 分帧 |
| `sendViewOnceOpen` | `{account, senderAci(ACI/号码), timestamp(原消息 upstream 时间戳)}` | `{}` | 同 send；senderAci 不可解析/参数缺失或形状错 `-32603` | **P2 real（2026-10-04 阅后即焚扩面）**：官方 syncViewOnceOpen 同构——向自己 ACI 发 SyncMessage.ViewOnceOpen{senderAci, timestamp}（content oneof viewOnceOpen=11，padded；多设备同步「该阅后即焚已被本机打开」），presage self-send 通路验证可行（§5，含钉定 rev 同内容二次转录重发怪癖）；与 sendViewedReceipt（作者面回执）双通道独立；fake：成功 `{}` |
| `sendMessageRequestResponse` | `{account, recipient(单值，ACI/号码)\|groupId(32B 标准 base64 群 master key), type}`（**两腿恰一**：direct = recipient、群 = groupId，同置/双缺 `-32603`；type = 官方驼峰词六值白名单 `accept\|delete\|block\|blockAndDelete\|spam\|blockAndSpam`，缺席/`unknown`/数字/词表外/大小写漂移 `-32603`） | `{}`（上游钉定面成功答空结果对象同款） | 参数缺失/type 闸/两腿闸/群腿非 base64 或 ≠32B/recipient 数组 `-32603`；未注册账号 `-32603` | **2026-10-06 real（KT 扩展，§4.56 写面债务批）**：官方桌面 getMessageRequestResponseSync 同构——向自己 ACI 发 SyncMessage.messageRequestResponse{threadAciBinary\|threadAci?, groupId?, type}（content oneof messageRequestResponse=14；ACI 可解析 16B 直写 binary 字段=官方 isProtoBinaryEncodingEnabled 现行面，群 master key 原样=官方 groupId Uint8Array 同位；presage self-send 通路，padding 同源）；与 §3 receive 通知面 `syncMessage.messageRequestResponse` 投影对称（本机写 → 多设备 echo 收）；connector §4.56 `conversations.acceptRequest`/`declineRequest` 的引擎面（上游钉定 `sendMessageRequestResponse` params 逐字对齐：connector 上游 recipient 腿 = 其 peer_key 直通本引擎双形态解析、groupId 腿 = 群 master key、type 由宿主盖章 accept/delete）；写结局即返回值、未知结局不重试纪律不变（connector §4.56 SEND_OUTCOME_UNKNOWN 终态语义的引擎侧对应位）；fake：成功 `{}`（落调度日志，协议上行面纪律） |
| `getUserStatus` | `{account, recipient}` | `{"isRegistered": true}` | 解绑态 `-32603`（§3 头原文）；未注册账号/其他失败 `-32603` | **P2 real**：whoami 轻量鉴权探测（401→解绑原文，网络失败不误报解绑）；fake 恒已注册 |
| `getStickerPackManifest` | `{packId(hex 32 小写), packKey(base64 32B)}`（匿名 CDN 只读，无 account） | `{title, author, cover?: {id, emoji?, contentType?} \| null, stickers: [{id, emoji?, contentType?}], stickerCount}`（官方 parseStickerPackManifest 同构：≤1024 截断；cover 官方缺省 null 由端上兜底 stickers[0]；cover 与 stickers 都空 malformed） | 参数缺失/packId 非 32 位小写 hex/packKey 非 32B base64、解密 MAC 失败（与密文损坏密码学不可区分）→ `STICKER_PACK_KEY_INVALID`；CDN 失败 → `STICKER_PACK_FETCH_FAILED`；padding/proto 解码失败 → `STICKER_PACK_MALFORMED`；均 `-1` + message 码位（connector 1.36 白名单逐字，§4.32） | **P4 real**：cdn.signal.org `stickers/{packId}/manifest.proto` 匿名 GET + attachment_cipher 解密；fake：参数面 fail-closed 后 `STICKER_PACK_FETCH_FAILED` |
| `getStickerImage` | `{packId, packKey, stickerId}` | `{dataBase64, contentType, size}` | 同 getStickerPackManifest，另单图 >300KB → `STICKER_IMAGE_TOO_LARGE` | **P4 real**：`stickers/{packId}/{stickerId}` 匿名 GET + 解密 + WHATWG mimesniff image 魔数嗅探 contentType（未识别回退 `image/webp`）；fake 同 getStickerPackManifest |
| `getPinnedConversations` | `{account}` | `{pinned: [{conversationId, kind: contact\|group\|legacyGroup}]}`（存储序；存储条目映射不出引擎口径的跳过不炸面） | 未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P4 real**：Storage Service manifest → Account item → `AccountRecord.pinned_conversations` 投影；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{pinned: []}` |
| `setConversationPinned` | `{account, conversationId, kind: contact\|group\|legacyGroup, pinned(bool)}` | `{pinned: [...]}`（写后全量，同 getPinnedConversations 形状） | 云引用解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次/写面冲突重放中失败 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/未知 kind/未注册账号 `-32603` | **P4 real**：官方 upload 语义（读改写 AccountRecord 仅动 pinned 字段、prost round-trip 保真其余字段、新 16B item id、version+1/source_device/record_ikm 透传）+ `PUT v1/storage/` + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；写面为 presage 上游缺口自建薄封装（上游补齐后替换）；fake：成功回 `{pinned: []}` |
| `getStickerPackSyncs` | `{account}` | `{packs: [{packId(hex 32 小写), packKey(base64 32B)\|null, position, deletedAtTimestampMs}]}`（存储序 = manifest STICKER identifiers 首现序；坏条目跳过不炸面；墓碑 packKey=null、带 deletedAtTimestampMs） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real**：Storage Service manifest → 全部 type=6 STICKER identifier 批量读 → StickerPackRecord 投影（记录面为 presage 上游 proto 缺口本地定义 + 加解密 shim，§5）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{packs: []}` |
| `setStickerPackSync` | `{account, packId(hex 32 小写), installed(bool), packKey?(base64 32B), position?}`（install 必带有效 packKey；uninstall 忽略 packKey 内容与 position，官方墓碑形态两字段皆不写） | `{packs: [...]}`（写后全量，同 getStickerPackSyncs 形状） | PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（packId 非小写 hex、install 缺/短/非字符串 packKey、packKey 非字符串、position 超 u32 或非数值）/未注册账号 `-32603` | **P5 real**：官方 toStickerPackRecord 语义（install 幂等：同 key 已装且未要求挪位→no-op；position 缺省=保持现位/无现位本地 MAX+1；uninstall 墓碑 deletedAtTimestampMs=引擎时钟，不物理删旧 record）+ 读改写仅动该 pack record + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；fake：成功回 `{packs: []}` |
| `setAccountConfiguration` | `{account, readReceipts?, typingIndicators?, phoneNumberSharingMode?: "everybody"\|"nobody", unlistedPhoneNumber?}`（只写提供的字段；至少一项） | `{}` | 提供字段已全部满足 → 幂等无写成功；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失（含零可写字段）/形状错（布尔字段非布尔、phoneNumberSharingMode 非法值或非字符串）/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateConfiguration 等价）**：真相源 = AccountRecord 的 readReceipts(6)/typingIndicators(8)/phoneNumberSharingMode(12)/unlistedPhoneNumber(13)，读改写仅动提供字段（字段保真铁面同款链路），+ 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；枚举 everybody=1/nobody=2（桌面 v8.31/Android 现行一致，旧 CONTACTS_ONLY 已从官方 proto 移除不收）；真相源单点（AccountRecord），其他设备经 storage sync 收敛，无额外同步广播；fake：成功 `{}` |
| `setProfile` | `{account, givenName, familyName?, about?, emoji?, avatar?}`（文字面全量写：givenName 必填非空，familyName/about/emoji 缺省=置空，官方 updateProfile 同口径；字节预算 UTF-8：given/family 各 ≤128B、about ≤512B、emoji ≤32B，超限 `-32603`；avatar 三态：缺省=保留现有、data URI=换新、null=移除） | `{"status":"saved"}` | 头像上行（profile PUT/CDN POST）失败 `-1 PROFILE_UPLOAD_FAILED`（data `{retryable:true, textSaved}`——textSaved=true 如实呈现文字面已提交的部分成功）；storage 链路同 `setAccountConfiguration` 四码；参数缺失/givenName 空、familyName/about/emoji 非字符串或超字节预算、avatar 非 image 族 data URI/非 base64/超 10MB/非字符串/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateProfile 等价）+ 对齐批②（2026-10-06 形状/错误/预算面，详见 §5）**：文字面走 presage `Manager::update_profile`（retain_avatar=true）；头像面为上游双层缺口自建薄封装（官方桌面 putProfile+uploadAvatar 行为对齐：`PUT /v1/profile` JSON `{version, name, about, aboutEmoji(加密 b64), avatar, sameAvatar:false, commitment(b64)}` → 响应上传属性 → 加密头像 multipart POST CDN0 根 → avatar path=响应 key，详见 §5）；写后 storage 面 AccountRecord.givenName/familyName/avatarUrlPath 同步；fake：成功 `{"status":"saved"}`，不落调度日志 |
| `setUniversalExpireTimer` | `{account, expirationInSeconds}`（0=关闭） | `{}` | 同值幂等无写成功；PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（timer 字符串/负数/小数/超 u32）/未注册账号 `-32603` | **P5 real（2026-10-05，E 批「不可达」结论勘误后落地）**：真相源 = AccountRecord.universalExpireTimer(17)（① vendored 官方 proto），读改写仅动该字段 + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；账户级默认值无广播（会话级计时器才广播 expiration-update），其他设备经 storage sync 收敛；fake：成功 `{}` |
| `getBlockedConversations` | `{account}` | `{blocked: [{conversationId, kind: contact\|group\|legacyGroup}]}`（存储序；条目形状与置顶面同构；v1 不含 blockedAtTimestamp） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real（2026-10-05 只读面）**：Storage Service manifest → 全部 type=1/2/3（CONTACT/GROUPV1/GROUPV2）identifier 批量原样读 → vendored proto 解码 → blocked 位过滤投影（contact 口径与置顶面同构：aciBinary 16B → UUID 串优先、字符串形态直通、e164 跳过；group=master key 标准 base64、legacyGroup=legacy id bytes）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；坏条目（缺 item/解密失败/解码失败/标识映射不出）跳过零 stderr；fake：恒 `{blocked: []}` |
| `getAvatar` | `{account, conversationId}`（形状分派：ACI/PNI ServiceId UUID 直通、手机号文本（E.164/裸数字）经 contacts 反查、群 = master key 标准 base64（listGroups id 同源）；三者皆不中 `-32603`） | `{dataUri, size}`（data URI = `data:<mime>;base64,<payload>`，MIME 魔数嗅探、未识别回退 `application/octet-stream`；size = 原始字节数） | 参数缺失/会话标识形状非法/未注册账号 `-32603`（参数面不占用结构化码）；无头像（未同步 profile_key/对端或群未设头像/服务器 403/404）→ `AVATAR_UNAVAILABLE`；网络/下载/解密失败 → `AVATAR_FETCH_FAILED`；超 10MB 帽 → `AVATAR_TOO_LARGE`（均 `-1` + message 码位） | **2026-10-06 real（KT 扩展）**：1:1 走 presage `retrieve_profile_avatar_by_uuid`（profileKey = presage store `profile_key(service_id)`，contacts 同步随行落库；未同步 → 诚实「无头像」）；群走 presage `retrieve_group_avatar`（master key 派生 GroupSecretParams 解密）；缓存/CDN/解密/入库由 presage 承担；10MB 帽照 presage 注释口径，超出 fail-closed；fake：`AVATAR_UNAVAILABLE`（不落调度日志，读面纪律） |
| `setConversationBlocked` | `{account, conversationId, kind: contact\|group\|legacyGroup, blocked(bool)}` | `{blocked: [...]}`（写后全量，同 getBlockedConversations 形状，存储序） | 云目标记录解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`（均 `-1` + message，同置顶面）；参数缺失/未知 kind/blocked 非布尔/未注册账号 `-32603` | **2026-10-06 real（KT 扩展）**：官方 blocked 位写语义（读改写目标会话记录仅动 blocked 位 + blockedAtTimestamp，官方 storageRecordOps 同语义：首次拉黑 = 引擎时刻、已拉黑幂等不重写他端时刻（含 0 原样保留）、解除清零；GV1 无时间戳字段）+ 新 16B item id + version+1 + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态：blocked=true → master key 播种 + version 1 最小 AccountRecord 与目标记录同批首写，blocked=false → 无可解除幂等成功不播种；纯拉黑不删会话（官方桌面同语义，消息/会话不清理）；fake：成功回 `{blocked: []}` |
| `reportSpam` | `{account, targetAci(ServiceId: ACI/PNI UUID), serverGuids[]（每项必须可解析 UUID，归一为小写连字形态入路径；urn:uuid:/花括号等宽松形态 fail-closed）, token?}`（单批 ≤1000，官方 reportSpamJobQueue zod max 同源） | `{reported, failures: [{serverGuid, status?}]}`（status = 服务器 HTTP 状态码；传输层失败该键缺席不发明 0） | 参数缺失/targetAci 非 ServiceId/serverGuids 缺失·非数组·空批·元素非 UUID/超 1000/token 非字符串形状/未注册账号 `-32603`（参数面不占用结构化码）；全部 serverGuid 均失败 → `REPORT_SPAM_FAILED`（`-1` + message） | **2026-10-06 real（KT 扩展）**：官方桌面同款 `POST v1/messages/report/{senderAci}/{serverGuid}` 逐条独立请求、JSON body `{token}`（token 缺省序列化为 `{}`，官方 jsonData 同构；本机凭据 basic auth）；部分失败诚实投影：单条 4xx/5xx/传输失败记入 failures（带状态码），至少一条成功即返回 `{reported, failures}`，全部失败才整体报错；fake：全量成功 `{reported: N, failures: []}`（落调度日志，协议上行面纪律） |
| `setExpirationTimer` | `{account, recipient(单字符串), expirationInSeconds}`（0=关闭） | `{timestamp}`（广播信封时间戳；同值 no-op = 处理时刻） | 参数缺失/形状错（recipient 数组、timer 字符串/负数/小数/超 u32）/未注册联系人/未注册账号 `-32603` | **E 批 real（2026-10-05 消失消息对齐）**：1:1 计时器设置 + expiration-update 广播（signal-cli `Manager.setExpirationTimer` 语义对位：先持久化 presage store Contact 面 (timer, version+1) 再广播；构造面不设 expire_timer，presage send 面注入，详见 §5）；fake：成功 `{"timestamp":u64}` |
| `sendCallMessage` | `{account, recipient(单号码/ACI)?\|group(32B 群 master key 标准 base64，listGroups/terminateGroup/updateGroup 的 groupId 同源口径)?——寻址两腿恰一互斥, data?(base64 opaque), urgency?("droppable"\|"handleImmediately" 缺省 droppable), legacy?({kind:"busy", id:正数} \| {kind:"hangup", id:正数, type?("normal"\|"accepted"\|"declined"\|"busy"\|"needPermission" 缺省 normal), deviceId?}), destinationDeviceId?}`（modern 面=data+urgency 构造 `CallMessage{opaque{data,urgency}}`；legacy 面=busy/hangup 官方裸帧族构造 `CallMessage{busy{id}}`/`CallMessage{hangup{id,type,deviceId?}}` 无 opaque 字段；**两面互斥**——同置或都缺 `-32603`；**两腿恰一**（2026-10-08 群臂批）——`group` 与 `recipient` 同置/都缺/任一腿形状非法 `-32603`；群腿仅 modern 面放行（官方群信令恒 opaque，legacy 裸帧族群向无先例；sender-key 群发无设备定向位——destinationDeviceId 与 group 同置 `-32603`）；`groupId` 键维持禁入（wire 参数名是 `group`）） | `{timestamp}`（信封时间戳） | 参数缺失/形状错（data 非 base64/空负载、urgency 非法值或非字符串、legacy 未知 kind/id 缺席·≤0/type 非法词/deviceId 超 u32、两面同置、destinationDeviceId 超 u32 或非数值、group 非法 base64/≠32B/非字符串、两腿同置/都缺、groupId 键在场、群腿携 legacy 或 destinationDeviceId）/未注册账号 `-32603`；群腿目标已终止 `SEND_TARGET_TERMINATED`（`-1` + message，2026-10-08 群臂批） | **2026-10-06 real（KT 扩展，通话消息面）**：官方 Content.callMessage=3 信封（Signal-RTC 层信令负载，引擎不解析只透传）；**2026-10-06 legacy 帧批（1.53 协同）**：legacy busy/hangup 按官方 proto 裸帧族编码（官方桌面 callingMessageToProto busy={id}/hangup={id,type}；RingRTC 2.72.1 出向 hangup/busy 实证无 opaque 材料；signal-cli 0.14.7 CallManager sendBusyViaSignal/sendHangupViaSignal 同构）——单测钉 prost 字节编码（busy=字段5{id}、hangup=字段7{id,type,deviceId}、两面无字段10）；presage `send_message` 通路（online=false 偏差与 sent-transcript 缺席见 §5 通话消息块）；**2026-10-08 群臂批（P4b，connector 契约 1.82 wire 面对位，engine-compat）**：`group` 腿 sender-key 群发（官方 #handleSendCallMessageToGroup calling.preload.ts:3338-3407 → sendCallingMessage.preload.ts:99-118 isGroup 臂 sendContentMessageToGroup 同落点），群头 = open_group_sender（terminated 硬拒，send 系群组形态同纪律）+ `send_message_to_group`（sendTyping 群臂同原语）；单测钉群臂 wire = `CallMessage{opaque{data,urgency}}` 字段10（无 busy/hangup/destinationDeviceId 位）；fake：成功 `{"timestamp":u64}` 两臂同口径（落调度日志，协议上行面纪律） |
| `calls.linkPresentation` | `{account, rootKey(标准 base64，解码后恰 16B(V0)/21B(V1)——RingRTC CallLinkRootKey 两版本定长，zkgroup 派生只取前 16B CRN)}` | `{presentation(标准 base64 = CallLinkAuthCredentialPresentation bincode 序列化字节，当天 UTC 兑换日呈现)}` | 参数缺失/rootKey 非 base64·解码后非 16/21B/未注册账号 `-32603`；服务器拒绝·zkgroup 兑换/呈现验证失败·当日凭据缺席 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? = HTTP 状态码诚实透传）；网络/传输面 `CALL_CREDENTIALS_UNAVAILABLE`（`-1` + message，retryable=true 语义）（**两枚新码位，connector 白名单登记归 connector 批**） | **2026-10-09 real（KT 扩展，P4c 切一 call-link 凭据材料面，§5 块）**：官方 getCallLinkAuthCredentialPresentation 同链（util/callLinks/zkgroup.preload.ts:16-49）——双族凭据缓存取当日 CallLinkAuthCredential（窗口算法逐字对齐官方 getDatesForRequest，§5 块）→ `present(ACI, 兑换日秒, GenericServerPublicParams, CallLinkSecretParams::derive_from_root_key)` → bincode 序列化出字节（官方返未序列化对象给 RingRTC 绑定，引擎字节化出 RPC）；凭据/呈现字节只内存转发零日志零落盘（relay-api-v2 §5 纪律）；fake：参数闸后形状桩 `{presentation}`（桩字节非有效 zk 材料仅钉形状），不落调度日志 |
| `calls.createLinkAuth` | `{account, rootKey(同 calls.linkPresentation 闸), roomId(标准 base64，解码后恰 32B——RingRTC `derive_room_id` HKDF-SHA256 固定 32B 输出)}` | `{presentation(标准 base64 = CreateCallLinkCredentialPresentation bincode 序列化), callLinkPublicParams(标准 base64 = CallLinkPublicParams bincode 序列化)}`（官方 createCallLink 凭据段双材料同形） | 参数缺失/rootKey·roomId 形状错/未注册账号 `-32603`；服务器拒绝·zkgroup 两段式兑换验证失败 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE`（同上行列） | **2026-10-09 real（同批）**：官方 createCallLink 凭据段同链（calling.preload.ts:860-940）——`CreateCallLinkCredentialRequestContext::new(roomId)` → `POST v1/call-link/create-auth`（chatService host 本机凭据，json `{createCallLinkCredentialRequest: b64}` → `{credential: b64}`，WebAPI.preload.ts:3439-3453 端点+形状、:1471-1473 schema；官方 2026-09-10 起加 `?v101=true` 切 V1 族，v0.99.0 钉不移植）→ `context.receive(ACI, GenericServerPublicParams)` → `present(roomId, ACI, generic, derive_from_root_key(rootKey))` + `get_public_params()` 双序列化；rootKey 生成/roomId 派生/adminPasskey 皆 RingRTC 侧职责引擎不收不做；单次 RPC 内两段式（无跨调用状态）；fake：参数闸后形状桩双键 |
| `calls.groupProof` | `{account, groupId(32B 标准 base64 群 master key，listGroups/terminateGroup 的 groupId 同源)}` | `{proof(token 字符串 = ExternalGroupCredential.token 原样透出；RingRTC setMembershipProof 消费其 UTF-8 字节)}` | 参数缺失/groupId 非 base64·≠32B/未注册账号 `-32603`；服务器拒绝（凭据无效/非群成员）·响应 proto 解码失败·当日凭据缺席 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE` | **2026-10-09 real（同批）**：官方 fetchMembershipProof 同链（groups.preload.ts:1782-1806 → WebAPI getExternalGroupCredential:4566-4586）——双族缓存取当日 AuthCredentialWithPni → present(group) → `GET v2/groups/token`（storageService host，basic auth = `hex(GroupPublicParams):hex(presentation)`，generateGroupAuth:4533-4540 同构）→ `ExternalGroupCredential{token=1}`（Groups.proto:285-287，presage::proto 生成面复用）解 token 透出；fake：参数闸后形状桩 `{proof}` |
| `calls.groupMembers` | `{account, groupId(同 calls.groupProof)}` | `{members: [{uuid(ACI 字符串), userIdCipherText(标准 base64 = UuidCiphertext bincode 序列化)}]}`（服务器群态成员序；RingRTC GroupMemberInfo{userId, userIdCipherText} 材料面） | 参数缺失/groupId 形状错/未注册账号 `-32603`；群态拉取被拒/解密失败 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE` | **2026-10-09 real（同批）**：官方 getMembershipList 同语义（groups.preload.ts:7611-7632：群成员逐一 encryptServiceId）——`GET v1/groups/` + decrypt_group（fetch_group_state 既有面复用）→ 每成员 `GroupSecretParams::encrypt_service_id(ACI)` → UuidCiphertext 序列化；uuid = ACI 字符串（relay-api-v2 §2 帧面对位）；fake：参数闸后 `{members: []}` |
| `calls.endorsementsPublicKey` | `{}`（常量派生面，无 account——getStickerPackManifest 匿名面同纪律） | `{endorsementPublicKey(标准 base64 = EndorsementPublicKey bincode 序列化)}` | 无（常量派生不触网，fake 同码路真实答） | **2026-10-09 real（同批）**：legacy ServerPublicParams 派生 `get_endorsement_public_key()`（zkgroup pin 树 server_params.rs:347；官方 `serverPublicParams.getEndorsementPublicKey()` 同位，消费位 calling.preload.ts:1134/:2018 = RingRTC getCallLinkCall 的 endorsementPublicKey 槽）；共享常量模块 `zkgroup_consts` 单一真相源（云备份面同源不重复嵌） |
| `backup.export` | `{account}` | `{"status": "started"}`（后台任务已启动，进度走 `backup.exportStatus`） | 未注册/fake 账号 `-32603`；该账号任务已在跑 `BACKUP_EXPORT_ALREADY_RUNNING`；工件缺失 `BACKUP_ARTIFACT_MISSING`；首行非法 `BACKUP_ARTIFACT_INVALID`（工件当场删除）——结构化码均 `-1` + message 码位（备份导出契约 §10.7，connector 1.47 白名单计划） | **消费批一期 real（2026-10-05）**：文件工件交接（备份导出契约 §10）——读 `<账号目录>/backup-export-input.ndjson`（connector 写入，引擎契约 §10.2 行格式/帽值）→ 专用 OS 线程装配 `MainStreamWriter` → `export_local_backup` 落官方 §5 快照；任务离开 running 即删工件（§10.2 硬边界）；每账号至多一任务；不落调度日志零 stderr；fake 模式账号无 store，按未注册 `-32603` |
| `backup.exportStatus` | `{account}` | `{account, state: idle\|running\|completed\|failed, artifactPresent, startedAtMs?, finishedAtMs?, lines?, frames?, snapshot?: {name, mainBytes, frames?}, error?, snapshots: [{name, mainBytes, frames?}]}`（快照清单 ≤32 条新→旧；`frames` 仅本进程任务产出已知；诚实缺席不发明 0） | 未注册账号 `-32603` | **消费批一期 real（2026-10-05）**：只读轮询面，仿 connector `history.importStatus` 语义（无推送事件，轮询到终态）；任务注册表进程内存不跨重启；快照清单只报告不清理（生命周期归 connector，备份导出契约 §10.6）；不落调度日志零 stderr |
| `backup.importSnapshot` | `{account, snapshotDir}`（官方桌面本地备份快照目录，官方布局 `signal-backup-<ts>/{metadata, main, files/}`；connector 传入的同机路径，引擎不持久化） | `{"status": "started"}`（后台任务已启动，进度走 `backup.importSnapshotStatus`） | 未注册/fake 账号/AEP 缺失 `-32603`；该账号导入任务在跑 `BACKUP_IMPORT_ALREADY_RUNNING`；目录或 metadata/main 缺 `BACKUP_SNAPSHOT_MISSING`；metadata decode 失败/version≠1/形状不合规 `BACKUP_SNAPSHOT_METADATA_INVALID`；backupId 比对不等（非本账号备份）`BACKUP_SNAPSHOT_KEY_MISMATCH`——结构化码均 `-1` + message 码位（备份导出契约 §11.4，connector 1.78 白名单） | **B2 批 real（2026-10-08，快照导入收向）**：官方 `importLocalBackup`/`verifyLocalBackupMetadata` 行为对齐（AGPL 只读参照零拷贝）——metadata 校验（官方 strict assert 面：version==1、iv 12B、encryptedId 16B、CTR 解密 backupId 逐字节比对）前移为 start 同步闸，main 全链（官方 FramesReader 解密+HMAC+分帧）后台 `spawn_local`（帧导出 future 非 Send），逐帧投影复用 `history::export_frames_loop` 落 `<账号目录>/history-import.ndjson`（connector §4.38 通路同一落点，临时名+rename 原子替换，失败删半成品既有 NDJSON 不动）；密钥边界 §10.5 延续（任务 future 内即用即弃）；fake 模式账号无 store，按未注册 `-32603`；不落调度日志零 stderr |
| `backup.importSnapshotStatus` | `{account}` | `{account, state: idle\|running\|completed\|failed, ndjsonPresent, startedAtMs?, finishedAtMs?, lines?, frames?, error?}`（lines = NDJSON 投影行数 recipient+chat+message；frames = 档案总帧数含 BackupInfo 元帧；诚实缺席不发明 0） | 未注册账号 `-32603` | **B2 批 real（2026-10-08）**：只读轮询面（backup.exportStatus 同语义）；注册表进程内存不跨重启（state 归 idle）；`ndjsonPresent` 是盘面真值（重启后 NDJSON 仍在场，消费方自行判定重导）；不落调度日志零 stderr |
| `transfer.downloadStatus` | `{account}` | `{present: bool}`；present=true 时 `{present, bytesRead(u64), bytesTotal?(u64)}`（快照缺席（未开始/已结束/引擎重启过）答 `{present:false}`，不猜） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态） | **2026-10-06 real（KT 扩展，契约 1.53 §4.38 扩展）**：transfer_archive 下载进度快照查询——`history.importStatus` progress 组合面的引擎侧只读数据源。进程内 per-account 注册表（`src/service/transfer_progress.rs`，std Mutex + Arc，不落盘不走文件），下载任务边读 CDN 响应边累计（块读）、content-length 有则记 total（拿不到只报 stage 不给假 0），任务收尾/失败/中途退出统一清快照；官方对齐（语义零拷贝）：Signal-Desktop Download 段 currentBytes/totalBytes 进度（`ts/state/ducks/installer.preload.ts` L143-152 + reducer L642-675）。只读不落调度日志零 stderr（backup.exportStatus 同纪律）
| `terminateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源)}` | `{status: "terminated"}`（幂等：已终止群再次调用同形状成功，不重复上传） | 参数缺失/groupId 非 base64 或长度 ≠32B `-32603`；未注册账号 `-32603`；服务器拒绝 `GROUP_TERMINATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批）**：官方桌面 buildTerminateChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、terminate_group action，change epoch 7），群凭据 basic auth（get_authorization_for_today 同语义链，§5 群终止批）；发送联动面：已终止群 send 系硬拒 `SEND_TARGET_TERMINATED`（`-1` + message）；receive 联动面：terminated change 投影 `groupTerminated` 事件（§5 群终止批）；fake：成功 `{status: "terminated"}`（落调度日志，协议上行面纪律） |
| `updateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源), expiration?, avatar?}`（expiration：0=关闭，u32；avatar 三态镜像 setProfile——缺省=保留、data URI `data:image/*;base64,` 规范 base64=换新、null=移除；解码后 ≤10MiB、非 image 族/非规范 base64/超帽/非字符串 `-32603`；expiration 与 avatar 至少一项在场，双缺 = 无操作整拒 `-32603` 不静默 no-op；expiration 单独在场或 avatar 单独在场均合法） | `{status: "sent"}`（群族成功答同纪律，connector 契约 1.45 §4.44 同形状） | 参数缺失/形状错（groupId 非 base64 或长度 ≠32B、expiration null·字符串·负数·小数·超 u32、avatar 非 image 族 data URI/非规范 base64/解码超 10MiB/非字符串、expiration 与 avatar 双缺）`-32603`；未注册账号 `-32603`；群头像上行链（avatar/form 拉取·属性校验·加密·CDN POST）失败 `GROUP_AVATAR_UPLOAD_FAILED`（`-1` + message，data `{retryable:true, textSaved:false}`——合成单 change 语义下伴生面未提交，见 §5）；服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批，群 expiration）**：官方桌面 buildDisappearingMessagesTimerChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、modify_disappearing_message_timer action（field 12），timer = 加密 `GroupAttributeBlob{disappearingMessagesDuration}`（uint32 秒，0=关）），群凭据 basic auth（§5 群终止批同链）；receive 联动面：expiration change 投影 `groupUpdated` 事件（§5 群 expiration 批）；connector 侧由 `conversations.setExpireTimer` 群形态路由至此（契约 1.45，kt-engine 面 INVALID_REQUEST 债务随本批解除）；**2026-10-09 real（群头像上行面批，§5 块）**：avatar 三态 + 合成单 change（expiration/avatar 同 version 双 action 一次 PATCH），上行链 = 群凭据 `GET v2/groups/avatar/form`（AvatarUploadAttributes protobuf）→ `GroupAttributeBlob{avatar}` 加密 → CDN0 multipart POST → avatar path 进 modify_avatar action（field 11，官方 buildUpdateAttributesChange/uploadAvatar/uploadGroupAvatar 行为对齐，镜像 setProfile 头像面先例）；receive 投影 avatar change 不投影（宁缺勿错，§5）；fake：expiration/avatar 缺省·null 臂成功 `{status: "sent"}`、data URI 臂失败 `GROUP_AVATAR_UPLOAD_FAILED`（两臂纪律 §5）；调度日志 avatar 键形状摘要化（data URI/base64 载荷零字节落盘） |
| `storage.write` | `{accountId, updates: [{type: "INSERT"\|"UPDATE"\|"DELETE", key(标准 base64，16B storage item key), record?(标准 base64，StorageRecord 明文 protobuf bytes)}]}`（字段名 `accountId` 为本扩展契约原形，非白名单面惯用的 `account`；INSERT/UPDATE 必带 record，DELETE 带 record 拒；updates 空批拒；批量内 key 重复拒） | `{version, results: [{type, key, writtenKey?, noop?}]}`（version = 写后 manifest version，全 no-op = 当前 version，从未有 storage 状态 = 0；results 与 updates 输入序对齐；`writtenKey`：INSERT = 原 key 回显、UPDATE = 引擎轮换出的新 16B item id——官方换写必轮换 item id 语义，旧 key 进 deleteKey；DELETE 无 writtenKey；no-op 条目带 `noop: true`） | 参数缺失/形状错（accountId 缺失·非字符串·空、updates 非数组·空批、条目非对象、type 非三枚举、key 非 base64·≠16B、INSERT/UPDATE record 缺失·null·非 base64·空、DELETE 带 record、批量 key 重复）`-32603`；record 解码不出白名单记录 arm（Contact=1/GroupV1=2/GroupV2=3/StoryDistributionList=5/StickerPack=6 之外，含 Account——账号记录归专用写面）`-32603`；INSERT 目标 key 已存在且内容不同/UPDATE 换记录类型（意图冲突 fail-closed）`-32603`；未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、item 读取/解密失败 `STORAGE_READ_FAILED`、PUT 失败 `STORAGE_WRITE_FAILED`（均 `-1` + message，置顶面同款）；版本冲突（服务器 409 = base version 失配）`STORAGE_VERSION_CONFLICT`（`-1` + message + data `{retryable: false, serverVersion?}`——写意图未落地，引擎**不自动重试**，调用方重读 manifest 后重放） | **2026-10-06 real（KT 扩展，storage 写面 P0）**：通用批量 manifest item 写（官方 WriteOperation 同构：manifest version+1、新 item 加密入 insertItem、换出 key 进 deleteKey、source_device/record_ikm 透传）；读侧 manifest version 解析复用既有面（`StorageService::manifest`/`decrypt_manifest`），写通道复用既有自建 PUT 薄封装（presage/libsignal-service 上游无写面，§5 storage 写面 P0 块）；幂等：INSERT 同 key 同内容/UPDATE 同内容/DELETE key 不在 = no-op（全 no-op 不 PUT 不 bump version，整批重放安全）；从未有 storage 状态：纯 UPDATE/DELETE 批 = 幂等成功不播种（version 0），含 INSERT → master key 播种 + version 1 最小 AccountRecord 同批首写（§5 首写分支同款）；fake：成功 `{version: 0, results: [...]}`，不落调度日志（storage 状态面纪律） |
| `getUsername` | `{account}` | `{username?, usernameLink?}`（键缺席 = 未设，不发明 null/空串；usernameLink 重建为完整 `https://signal.me/#eu/<b64url(entropy‖handle)>`；链接材料形状错该键缺席） | 参数缺失/非字符串 `-32603`；未注册账号 `-32603`；storage 读链：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（均 `-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，用户名读面批）**：真相源 = Storage Service AccountRecord.username(33)/usernameLink(35)（官方 proto 注释「仅 server confirm/delete 时更新」同语义；多设备经 storage sync 收敛，无第二真相源）；从未有 storage 状态（无 master key / manifest 404 / 无 Account item）→ `{}`（未设不报错）；fake：恒 `{}`，不落调度日志（读面纪律） |
| `usernameLookup` | `{account, usernames[1..20]}`（每条 1..64 字符——connector 契约 1.59 §4.55 帧界同源镜像，帧界而非语法界；条目 = 全名或 `https://signal.me/#eu/…` 链接） | `[{recipient, username?, uuid?, isRegistered}]`（输入序逐条投影；recipient = 条目回显；单条失败诚实降级 `{recipient, isRegistered:false}` 不炸整批——语法不符/链接打不开/解不开/未命中/网络皆同） | 参数缺失/形状错（usernames 缺席·非数组·空批·超 20、条目非字符串·空·超 64）`-32603`；未注册账号 `-32603` | **2026-10-08 real（KT 扩展，用户名读面批）**：官方同款服务端匿名 lookup 通路——全名条目 hash（libsignal usernames crate 权威语法面）→ `GET v1/accounts/username_hash/{hash}`（强制匿名，200 `{uuid}`/404）；链接条目 → `GET v1/accounts/username_link/{uuid}`（200 `{usernameLinkEncryptedValue}`）→ crate `decrypt_username`（entropy 系链接自带）→ 再走 hash lookup（官方 resolveUsernameLink 同序）；base64 一律 URL-safe 无 padding；匿名 PushService（带凭据反而 400，avatar CDN 先例同构）；fake：逐条回显 `{recipient, isRegistered:false}`，不落调度日志（读面纪律） |
| `usernameReserve` | `{account, username}`（全名 `nick.disc` 或裸昵称——缺 disc = 引擎生成 20 候选轮转；wire 界 38 字符 = connector `USERNAME_MAX_WIRE_CHARS` 同源镜像，帧界而非语法界，语法面归官方 usernames crate 硬语法） | `{username, usernameHash}`（username = 采纳候选全名；usernameHash = b64url 无 padding 32B，confirm 回传值） | 参数缺失/非字符串/空/超 38 `-32603`；crate 硬语法不符（单数字 disc、0 打头非恰两位、超位、非法字符）`-32603`；未注册账号 `-32603`；服务器 409（批内全占）`USERNAME_TAKEN`、413/429 `USERNAME_RATE_LIMITED`、其余 `USERNAME_SERVER_FAILED`（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方两相写面前相——候选 + 批 hash（crate 权威原语）→ `PUT v1/accounts/username_hash/reserve` `{usernameHashes:[…]}`（URL-safe 无 padding；200 `{usernameHash}` 采纳位，官方 AccountController 同码位）；reserve 无正式副作用（不落 username，官方同语义）；fake：本地候选 + hash 桩（真实 crate 原语）不触网，不落调度日志（storage 状态面纪律） |
| `usernameConfirm` | `{account, username, usernameHash}`（usernameHash = reserve 采纳值回传；引擎重验语法 + hash 失配 fail-closed——两相调用面被撕裂即拒） | `{username, usernameLink}`（完整 `https://signal.me/#eu/<b64url(entropy‖handle)>`；entropy 系本端新生成，仅此响应与 storage 35 字段两处落地） | 参数缺失/形状错（三参缺失·非字符串·空、语法不符、hash 失配）`-32603`；未注册账号 `-32603`；服务器 409 `USERNAME_RESERVATION_GONE`（预约过期/被占）、410 `USERNAME_TAKEN`、413/429 `USERNAME_RATE_LIMITED`、其余 `USERNAME_SERVER_FAILED`；服务器已确认而 storage 33/35 落盘失败 → STORAGE 族（诚实部分成功：服务器态已变更，调用方按 STORAGE 语义处置，getUsername/多设备 sync 收敛）（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方后相同序——`Username::proof`（ZK，随机 32B）+ `create_for_username`（新 entropy 加密负载）→ `PUT v1/accounts/username_hash/confirm` `{usernameHash, zkProof, encryptedUsername}`（200 `{usernameLinkHandle}` 重建完整链接）→ AccountRecord 33/35 storage 落盘（官方 confirm 落盘同位）；fake：本地 link 桩（随机 handle）不触网不落盘，不落调度日志 |
| `usernameDelete` | `{account}` | `{}`（幂等：本就未设照走服务器删除 + storage 清除，无写分支成功同形状） | 参数缺失 `-32603`；未注册账号 `-32603`；413/429 `USERNAME_RATE_LIMITED`、其余服务器失败 `USERNAME_SERVER_FAILED`；storage 33/35 清除失败 STORAGE 族（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方同序 `DELETE v1/accounts/username_hash`（服务器清 hash 并连带清 link 负载）→ AccountRecord 33/35 清除读改写；fake：`{}` 桩，不落调度日志 |
| `listIdentities` | `{account, number}`（单引用窄读：号码/ACI/PNI 形态直通本引擎 resolve 阶梯；`number` 必填——账号全量清单形态不提供，§5 安全码引擎面块） | `[{number?, uuid?, fingerprint(hex 66 字符 = 对端 identity key 33B 序列), safetyNumber(12 组×5 位空格分隔), scannableSafetyNumber?(标准 base64 = CombinedFingerprints protobuf，官方 QR tab 码内容), trustLevel("UNTRUSTED"\|"TRUSTED_UNVERIFIED"\|"TRUSTED_VERIFIED"), addedTimestamp(ms)}]`；对端 identity 未知 → `[]`（官方 SafetyNumberNotReady 态，非错误） | 参数缺失/number 非字符串/未注册账号 `-32603`；号码不可解析 → `[]`（窄读不命中 = 无 identity，非错误） | **2026-10-08 real（KT 扩展，安全码引擎面批）**：pinned signal-cli 同名动词原生面（connector §4.61 `identities.get` 上游形状逐字对齐，engine-compat）——数字串与 scannable 同源（libsignal `Fingerprint::new(version=2, iterations=5200, own_aci_binary, own_aci_key, their_sid_binary, their_key)`，官方 safetyNumber.preload.ts:34-72 同参数链，§5）；trust 态引擎自有 sidecar（`identity-trust.json`），见 trust 行与 §5 |
| `trust` | `{account, recipient, verifiedSafetyNumber? \| state?}`（**两臂恰一**：verified 臂 = `verifiedSafetyNumber` 字符串（空格剥离后 60 位数字串/66 位 hex fingerprint/base64 scannable 三形态，官方 IdentityVerificationCode.parse 同构）；clear 臂 = `state` 词表 `"unverified"\|"default"`（官方 setUnverified/setVerifiedDefault 语义）；两臂同置/双缺/`state` 其他值 `-32603`。pinned `trustAllKnownKeys` 测试模式不实现，§5） | `{}`（pinned 同款：命令无 JSON 输出） | verified 臂数字/指纹/scannable 与本端计算值不符或对端 identity 未知 `-1` + `Failed to set the trust for this number, make sure the number and the fingerprint/safety number are correct.`（pinned UserError 原文位）；clear 臂对端 identity 未知同上；参数面/未注册账号 `-32603` | **2026-10-08 real（KT 扩展，安全码引擎面批）**：verified 臂 = pinned `trust {verifiedSafetyNumber}` 原生面（connector §4.61 `identities.verify` 上游形状逐字，比对通过 → trust 态写 `verified`）；clear 臂 = 官方「将标记为已验证清除」补臂（KT 扩展，desktop `toggleVerified` 无 pinned 投影面的引擎侧闭合）——`unverified` → TrustLevel UNTRUSTED、`default` → TRUSTED_UNVERIFIED，无数字闸（官方 clear 同语义）；两臂成功后 best-effort 自发 `SyncMessage.verified{destinationAciBinary, identityKey, state}`（signal-cli SyncHelper.sendVerifiedMessage 同构，同步失败不回滚 store 写）；fake：成功 `{}`，不落调度日志 |
| `chatFolders.list` | `{account}` | `{folders: [ChatFolderRecordJson...]}`（存储序 = manifest CHAT_FOLDER=8 identifiers 首现序；坏条目跳过不炸面；形状见 §5 Storage 记录面块） | 未注册账号 `-32603`；storage 链路 `STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：Storage Service manifest → 全部 type=8 identifier 批量原样读 → 官方 vendored proto 解码 → ChatFolderRecord JSON 投影（键名对齐官方 fromChatFolderRecord 输出面，recipients 投影 `{conversationId, kind}` 会话引用对象，§5）；无 master key 或 manifest 404 → `{folders: []}`；fake：恒 `{folders: []}`，不落调度日志（读面纪律） |
| `chatFolders.setRecords` | `{account, records?: [ChatFolderRecordJson...], removeIds?: [uuid 字符串...]}`（**两参至少一非空**；record 形状 `{id(uuid), name, position(u32), showOnlyUnread, showMutedChats, includeAllIndividualChats, includeAllGroupChats, folderType("UNKNOWN"\|"ALL"\|"CUSTOM"), includedConversationIds?/excludedConversationIds?: [{conversationId, kind: contact\|group\|legacyGroup}...], deletedAtTimestampMs?(u64, 非 0 = 墓碑)}`；records 逐条按 id upsert——云上同 id 定位旧 identifier 则轮换新 16B item id 换写（官方同实体更新语义），无旧 = 新插；`deletedAtTimestampMs` 非 0 = 墓碑写（官方删除语义，24h 过期清理归调用方批）；removeIds = 官方 deleteKey 语义（identifier 出 manifest + deleteKey，expired-tombstone 清理面）；与云上现存完全同值的 record 幂等跳过不轮换 key） | `{folders: [...]}`（写后全量投影，存储序 = 写后 manifest identifiers 首现序，同 chatFolders.list 形状） | 参数缺失/形状错（records 非数组·条目非对象·id 非 UUID·name 缺失·position/布尔/词表/时间戳/recipient 条目形状错、removeIds 条目非 UUID、两参双缺或全空批）`-32603`；未注册账号 `-32603`；PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED` | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：官方 upload 语义（每实体独立 record + 新 16B item id 换入/旧 key deleteKey/version+1/source_device·record_ikm 透传 + 409 响应体采纳重放 [1s,5s,30s]，storage.preload.ts toChatFolderRecord 同构）；manifest 404 首写分支同置顶面（master key 播种 + version 1 最小 AccountRecord 同批）；record 内容不落引擎本地（读写直通，connector 持久化职责，§5）；fake：成功回 `{folders: []}`，不落调度日志（storage 状态面纪律） |
| `notificationProfiles.list` | `{account}` | `{profiles: [NotificationProfileRecordJson...]}`（存储序 = manifest NOTIFICATION_PROFILE=9 identifiers 首现序；形状见 §5） | 未注册账号 `-32603`；storage 链路同 chatFolders.list 两码 | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：同 chatFolders.list 链路，type=9 identifier + NotificationRecord 投影（键名对齐官方 mergeNotificationProfileRecord 输出面）；fake：恒 `{profiles: []}`，不落调度日志 |
| `notificationProfiles.setRecords` | `{account, records?: [NotificationProfileRecordJson...], removeIds?: [32 位 hex id...]}`（语义与 chatFolders.setRecords 同构：id upsert/墓碑/removeIds/幂等跳过；record 形状 `{id(32 位小写 hex), name(非空), emoji?, color(u32 0xAARRGGBB), createdAtMs(u64), allowAllCalls, allowAllMentions, allowedMembers?: [{conversationId, kind}...], scheduleEnabled, scheduleStartTime?/scheduleEndTime?(u32 0000-2359), scheduleDaysEnabled?: [1..7 整数...], deletedAtTimestampMs?}`） | `{profiles: [...]}`（写后全量，同 notificationProfiles.list 形状） | 参数面同 chatFolders.setRecords（id 非 32 位 hex、scheduleDaysEnabled 条目非 1..7 等）`-32603`；错误面同 chatFolders.setRecords 四码 | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：官方 toNotificationProfileRecord 同构写面（emoji/color/createdAtMs/allowAllCalls/allowAllMentions/allowedMembers/schedule 族全字段）；fake：成功回 `{profiles: []}`，不落调度日志 |
| `storyDistributionLists.list` | `{account}` | `{lists: [StoryDistributionListRecordJson...]}`（存储序 = manifest STORY_DISTRIBUTION_LIST=5 identifiers 首现序；坏条目跳过不炸面；形状见 §5 Stories 分布名单记录面块） | 未注册账号 `-32603`；storage 链路 `STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批，Stories 名单引擎半片）**：Storage Service manifest → 全部 type=5 identifier 批量原样读 → 官方 vendored proto 解码（StoryDistributionListRecord 七字段，官方 SignalStorage.proto:333-341；manifest 类型 STORY_DISTRIBUTION_LIST=5 :53、StorageRecord.storyDistributionList=5 :77）→ JSON 投影（键名对齐官方 record 输出面：`identifier`(16B uuid→串)/`name`/`recipientServiceIds`(service id 串面)/`recipientServiceIdsBinary`(binary 面，标准 base64)/`deletedAtTimestamp`(恒在，0=未删)/`allowsReplies`/`isBlockList`，§5）；无 master key 或 manifest 404 → `{lists: []}`；fake：恒 `{lists: []}`，不落调度日志（读面纪律） |
| `storyDistributionLists.setRecords` | `{account, records?: [StoryDistributionListRecordJson...], removeIds?: [uuid 字符串...]}`（**两参至少一非空**；record 形状 `{identifier(uuid), name(在场可空串——墓碑清名), recipientServiceIds?: [ACI uuid\|PNI:uuid...], recipientServiceIdsBinary?: [标准 base64(16B ACI/17B PNI)...], deletedAtTimestamp?(u64, 非 0 = 墓碑), allowsReplies, isBlockList}`；语义与 chatFolders.setRecords 同构：按 identifier upsert——云上同 id 定位旧 identifier 则轮换新 16B item id 换写，无旧 = 新插；同值幂等跳过不轮换 key；removeIds = 官方 deleteKey 语义（identifier 出 manifest + deleteKey）） | `{lists: [...]}`（写后全量投影，存储序 = 写后 manifest identifiers 首现序，同 storyDistributionLists.list 形状） | 参数缺失/形状错（records 非数组·条目非对象·identifier 非 UUID·name 缺失·allowsReplies/isBlockList 缺失或非布尔·成员串面条目解析不出·binary 面非 base64 或非 16/17B service id·deletedAtTimestamp 负数、removeIds 条目非 UUID、两参双缺或全空批、重复 id、records∩removeIds 交集）`-32603`；未注册账号 `-32603`；错误面同 chatFolders.setRecords 四码（`STORAGE_WRITE_FAILED`/`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批，Stories 名单引擎半片）**：官方 toStoryDistributionListRecord/mergeStoryDistributionListRecord 同构（storageRecordOps.preload.ts:796/:2299）；upsert/墓碑/幂等（decode→re-encode 全等判据）/409 采纳重放/首写播种 Account 全部与 chatFolders.setRecords 同链路；binary 面写入前按 ServiceId 归一化（16B ACI/17B PNI）；官方删除级联面（对该名单 story 走 deleteStoryForEveryone + 删 sender key）归调用方，引擎只管 record 面同步（§5）；同批放开 `storage.write` 通用写面白名单 StoryDistributionList arm（§5 storage 写面 P0 块修订，其余 arm fail-closed 不变）；fake：成功回 `{lists: []}`，不落调度日志（storage 状态面纪律） |
| `backups.restoreFromCloud` | `{account}` | `{"status": "started"}`（全链后台化：凭据拉取（404→backupId 引导自愈→重试一次）→zkgroup 兑换呈现→生产 chat 未鉴权 gRPC 定位→CDN 读凭据→下载→AEP 派生 MessageBackupKey 解密导出 NDJSON；终态经 `backups.cloudStatus` 观测） | account 缺失/非字符串 `-32603`；结构化码七族均 `-1` + message 码位（`CloudRestoreError::rpc_code` 单一映射源，带 detail 时附 `data.detail`；cloudStatus failed 相位同表两端不漂）：同步段 `BACKUP_AUTH_FAILED`（未注册/fake 账号、AEP/registration/身份密钥缺席或 zkgroup 兑换被拒，retryable=false）、`BACKUP_NOT_PROVISIONED`（引导自愈后仍无档=账号从未开启云备份，retryable=false，UI 诚实降级）、`BACKUP_RUNNING`（同引擎已有在途 restore 重入，retryable=false）；后台段失败经 cloudStatus failed 相位透出：`BACKUP_TRANSPORT_FAILED`（chat/REST 传输，retryable=true）/`BACKUP_DOWNLOAD_FAILED`（CDN，retryable=true）/`BACKUP_DECRYPT_FAILED`/`BACKUP_EXPORT_FAILED`（retryable=false）（cloud-backup-contract §2） | **2026-10-08 real（KT 扩展，云备份 restore 批）**：官方凭据链六步全实锤（GET v1/certificate/auth/group 7 天窗签发→404 引导 PUT v1/archives/backupid 自愈→zkgroup 兑换呈现（生产 GenericServerPublicParams 常量嵌入）→`get_message_backup_info` 定位→`GET cdn{N}/backups/{dir}/{name}` 流式块读→`backup::keys::derive_keys`(AEP,aci).message_key + FramesReader 尾部 HMAC 复用快照帧链落 NDJSON）；messages 档免费全账号、media 档二期（诚实边界 cloud-backup-contract §3）；运行闸进程级单槽 CAS（`try_begin_running`/`finish_running`）；进度快照 `Arc<Mutex>` 双持；send lane mutating；fake 账号无 store 同未注册答 `BACKUP_AUTH_FAILED`；不落调度日志零 stderr |
| `backups.cloudStatus` | `{account}`（仅契约形状校验，不参与路由——进度面进程级单槽，同引擎至多一个 restore） | `{present: bool}`；present=true 时 `{present, phase, bytesRead(u64), bytesTotal?(u64), errorCode?}`——phase：`downloading`=在途（字节计数有效，total 缺席不给假 0）/`failed`=终态失败（`errorCode` 透出同码表，快照保留至下次 start 覆盖，不装死）；快照缺席（未开始/已成功收尾/引擎重启过）= `{present:false}`（成功终态不留痕清缺，产物走 NDJSON 消费链——transfer_archive「无论成败收尾清快照」同纪律） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态，transfer.downloadStatus 同纪律） | **2026-10-08 real（KT 扩展，云备份 restore 批）**：只读轮询面（backup.exportStatus/transfer.downloadStatus 同语义，无推送事件轮询到终态）；快照进程内存不跨重启；只读不落调度日志零 stderr |
| `backups.uploadToCloud` | `{account}` | `{"status": "started"}`（全链后台化：装配段=专用 OS 线程（§10.3 同款，工件→main 容器流式写→临时产物 `<account_dir>/cloud-upload-main-tmp`）→上行段=LocalSet watcher：凭据拉取（restore §1 链同源，404 引导自愈）→zkgroup 兑换呈现→生产 chat 未鉴权 gRPC `get_upload_form`（cdn===3 强制）→TUS create-with-upload 单发 POST（`Tus-Resumable: 1.0.0` + `Upload-Length` + `Upload-Metadata: filename <b64(form.key)>` + form headers，body=密文产物 128 KiB 块流式读盘，请求级超时 900 s）→`refresh_backup`；终态经 `backups.uploadStatus` 观测；upload 面=官方桌面 `Backups.upload` 同链，产物 remote 型与一期 local 容器逐字节同构，派生钥=`derive_keys`+remote 臂（Some(forward-secrecy token)→NEW_DST），契约钉定 cloud-backup-contract §6.1） | account 缺失/非字符串 `-32603`；结构化码八族均 `-1` + message 码位（`CloudUploadError::rpc_code` 单一映射源，带 detail 时附 `data.detail`；uploadStatus failed 相位同表两端不漂）：同步段 `BACKUP_AUTH_FAILED`（未注册/fake 账号、AEP/registration/身份密钥缺席或 zkgroup 兑换被拒，retryable=false）、`BACKUP_UPLOAD_ALREADY_RUNNING`（同引擎已有在途 upload 重入，**新码位，白名单登记归 connector 透传批**，retryable=false）、`BACKUP_ARTIFACT_MISSING`/`BACKUP_ARTIFACT_INVALID`（工件缺席/首行非法，非法当场删工件，retryable=false）；后台段失败经 uploadStatus failed 相位透出：`BACKUP_NOT_PROVISIONED`（引导自愈后仍无档，retryable=false）/`BACKUP_TRANSPORT_FAILED`（凭据/form gRPC/refresh_backup，retryable=true）/`BACKUP_UPLOAD_FAILED`（**新码位，白名单登记归 connector 透传批**：cdn≠3/非 2xx/超帽/上行读盘，retryable=true——上传 POST 已离站=unknown-outcome 窗口，重试=整链重来）/`BACKUP_IO_FAILED`（临时产物写面，retryable=false）（cloud-backup-contract §6.2） | **2026-10-08 real（KT 扩展，云备份 upload 批）**：官方 TUS 单发协议实锤（桌面 tusProtocol.node.ts create-with-upload：`POST {signedUploadLocation}` 2xx=完成，无断点续传——引擎同口径单发，失败=failed 终态不自动重试）；上传 POST 用 `HttpAuthOverride::Unidentified`（预签名 URL 不附账号 basic auth）；凭据/header 不落日志不落盘；临时产物任何终态必删（官方 finally unlink 同款），崩溃残留由下次上传覆盖写收敛；产物 1 GiB 帽 fail-closed（form 段 `UploadTooLarge` 同映射 `BACKUP_TRANSPORT_FAILED`，契约 §6.1 裁定）；media 档出范围（messages 档密文归档；mediaRootBackupKey 每次随机 32B，契约差异登记同 §10.5）；运行闸进程级单槽 CAS；send lane mutating；fake 账号无 store 同未注册答 `BACKUP_AUTH_FAILED`；不落调度日志零 stderr |
| `backups.uploadStatus` | `{account}`（仅契约形状校验，不参与路由——进度面进程级单槽，同引擎至多一个 upload） | `{present: bool}`；present=true 时 `{present, phase, bytesRead(u64), bytesTotal?(u64), errorCode?}`——phase：`exporting`=装配段在途（无字节计数语义，bytesRead=0）/`uploading`=上行段（块计数有效，total=产物字节不给假 0）/`failed`=终态失败（`errorCode` 透出同码表，快照保留至下次 start 覆盖，不装死）；快照缺席（未开始/已成功收尾/引擎重启过）= `{present:false}`（成功终态不留痕清缺——restore cloudStatus 同纪律） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态，cloudStatus 同纪律） | **2026-10-08 real（KT 扩展，云备份 upload 批）**：只读轮询面（cloudStatus/transfer.downloadStatus 同语义）；快照进程内存不跨重启；只读不落调度日志零 stderr |
| `engine.protocolCapabilities` | `{}`（参数忽略，账号无关——无 `account`，未绑定账号照常应答） | `{deviceCapabilities: {storage, transfer, attachmentBackfill, spqr, profiles_v2, usernameChangeSyncMessage, versionedExpirationTimer, optionalPhoneNumber}, deviceCapabilitiesUpload: {attachmentBackfill, spqr, usernameChangeSyncMessage, optionalPhoneNumber}, linkCapabilities: {attachmentBackfill, spqr, usernameChangeSyncMessage}, baseline: {presageRev, libsignalVersion, libsignalCommit}}`（能力位 JSON 形状 = vendor serde 属性输出：camelCase、`profiles_v2` 按上游字面保持 snake——线上声明即此拼写；位值 = 引擎实际向服务器声明的协议能力，真相源与防漂移闸见 §5 引擎自证面块；`deviceCapabilitiesUpload` = D5 设备面注册位（官方 CapabilitiesUploadType 四键）；baseline = 编译期钉定的依赖锚，运行时不查网络） | 无（只读常量应答，无业务错误面；fake/real 同码路真实答） | **2026-10-10 real（KT 扩展，引擎自证面批）**：引擎以 linked device 身份的声明如实自报——账号属性面 DeviceCapabilities（presage 主注册链同值的钉定镜像）+ 链接面 LinkCapabilities（link_device 取 `LinkCapabilities::default()` 三位，运行时 live 复用 vendor 构造器）；spqr 恒 true（vendor 链已声明，上游 2026-03 强制过滤批量注销不达标客户端的核对面）；不落调度日志零 stderr。**2026-10-11 D5 additive**：deviceCapabilities 补 `versionedExpirationTimer`/`optionalPhoneNumber` 两位（server 真相 DeviceCapability.java 全集对齐，均 false），新增 `deviceCapabilitiesUpload` 面（engine.deviceCapabilities 的 PUT body 同形） |
| `engine.deviceCapabilities` | `{account}` | `{account, registered: true, capabilities: {attachmentBackfill, spqr, usernameChangeSyncMessage, optionalPhoneNumber}}`（capabilities = PUT /v1/devices/capabilities 的 body 同形回读，位值 = 钉定镜像，见 §5 D5 块） | 未知/未注册账号 `-32603`（open_registered 未命中折叠，不泄露注册态）；服务器 401 解绑 `-32603` + getUserStatus 解绑原文同款；其余服务器失败 `-32603` 折叠（位值声明幂等，重试安全） | **2026-10-11 real（KT 扩展，D5 设备面注册批）**：官方 registerCapabilities 同端点手动触发 + 上传面回读（宿主可查询）；fake 恒已注册（getUserStatus 同款）；接收管线建流另有自动注册（同一位值常量，每管线运行成功一次即止）；触网不落调度日志零 stderr |

`receive` 通知：`{jsonrpc, method:"receive", params:{account, envelope}}`；
P2-A 起投影覆盖 connector 1.15/1.24/1.25 的全部消费面（钉定 signal-cli
0.14.8 JSON，见 connector implementation-plan §4.13/§4.19/§4.20）：

- 信封公共位：`sourceUuid`（ACI service id 字符串）、`sourceDevice`、
  `timestamp`；contacts 命中时附 `sourceNumber`（E.164）与 `sourceName`。
  `serverGuid`（2026-10-06，reportSpam 面配套）：服务器为该条消息分配的
  UUID（举报 API 的路径参数），信封元数据公共位，有值才投（proto 只投影
  不发明）；envelope 级 reportSpamToken 经 presage Content 转换丢弃
  （Metadata 不携带），不在投影（§5 reportSpam 块）。
- `dataMessage`：`timestamp`、`message`、`quote {id, author, text?}`、
  `reaction {emoji, targetAuthor, targetSentTimestamp, isRemove}`、
  `remoteDelete {timestamp}`、`attachments[] {id, contentType, filename?,
  size?, width?, height?, caption?, isVoiceNote, locator?}`（≤32；元数据
  面 + 2026-10-07 起 `locator` 指针面随行——key/cdnKey/cdnNumber/
  uploadTimestamp/digest，proto 只投影不发明，全缺键不在，§5 receive
  信封附件 locator 投影块）、`previews[] {url, title?, description?,
  image?}`（≤4；image 同走附件指针投影）、
  `mentions[] {number, start, length}`（≤64，来自 bodyRanges）、
  `viewOnce: true`；viewOnce 行跑官方 isValidTapToView 同构校验，非法补
  `viewOnceInvalid: true`（2026-10-04，官方 isTapToViewInvalid 同位；引擎
  metadata-only 只标记不擦除，§5）。引用/反应作者号 contacts 命中投影
  E.164，未命中保留 ACI 字符串。textStyles：bodyRanges Style arm 投影
  （2026-10-06 收包投影批，形状/口径详见下方「bodyRanges 收包投影」块；
  旧登记「proto 无此面」系勘误，§5）。消失消息 E 批（2026-10-05）：
  `expiresInSeconds`
  （DataMessage.expire_timer=5 透出，秒，0=关闭原样透出）与
  `isExpirationUpdate: true`（DataMessage flag 位 EXPIRATION_TIMER_UPDATE=2
  投影，官方桌面收发两侧同 flag 机制），缺省键不在。
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
  destinationNumber|destinationUuid + 同 dataMessage 控制面）；消失消息
  E 批增 `expirationStartTimestamp`（Sent.expiration_start_timestamp=4，
  官方多设备精确起算同步位；0/缺席不投）。
- `syncMessage.viewOnceOpen {senderAci, timestamp}`（2026-10-04，官方
  content oneof viewOnceOpen=11 投影）：本机主设备/其他设备已打开某阅后
  即焚消息，上层据此把 (senderAci, timestamp) 命中的行置 Viewed 并执行
  字节焚毁；senderAci 解析 binary 优先 string 兜底（官方 parse_sender_aci
  同构），不可解析整事件丢弃（乱序兜底 warn-only，§5）；timestamp 缺席
  投影 0 不丢弃。「找不到目标行」的丢弃决策在 connector 消费面执行
  （warn-only、不暂存，§5）。
- 群组接收投影：dataMessage.groupV2 / sent 内群组带 `groupInfo {groupId}`
  （master key 标准 base64，signal-cli 同款；直聊不带该键）；群更新类消息
  （成员变更/权限面）仍跳过。story/null 体跳过；解密失败按事件丢弃。
- `groupTerminated {groupId, terminator?, timestamp}`（2026-10-06，KT 扩展
  批）：群更新消息内密文 GroupChange 含 terminated action 且 change epoch
  ≤7 → 整信封替换为该事件（groupId = master key 标准 base64，terminator
  = 编辑者 ACI 串；epoch >7 不处理，其余群更新消息照旧跳过）；命中同时
  登记进程内终止表（send 面硬拒联动，§5 群终止批）。
- `callMessage {opaque?(base64), urgency?("droppable"\|"handleImmediately",
  缺席=droppable), destinationDeviceId?, offer?/answer?/iceUpdate?/busy?/
  hangup?}`（2026-10-06 通话消息面，KT 扩展；官方 Content.callMessage=3
  投影）：现行线面 opaque 信令负载 base64 透传（引擎不解析，Signal-RTC
  职责）；offer/answer/iceUpdate（旧线面 1:1 信令，在场才投：offer
  `{id, type? "audio"|"video", opaque?(base64)}`、answer
  `{id, opaque?(base64)}`、iceUpdate `[{id, opaque?(base64)}]`、busy
  `{id}`、hangup `{id, type? "normal"|"accepted"|"declined"|"busy"|
  "needPermission", deviceId?}`）；空白 CallMessage（无可投影字段）整事件
  丢弃（宁缺勿错）。
- `syncMessage.callEvent {conversationId?, callId(字符串形态，官方
  longToString 同构), timestamp, type?, direction?, event?}`（2026-10-06
  通话消息面，官方 SyncMessage.callEvent=19 / 桌面 callEventSync 同位）：
  本机其他设备/主设备的通话记录写入同步；type ∈ audio|video|group|ad-hoc
  （UNKNOWN 不投）、direction ∈ incoming|outgoing、event ∈
  accepted|not-accepted|delete|observed（枚举缺席或枚举外值键省略，proto3
  未知值同缺省语义）；conversationId 按官方桌面
  conversationPeerIdInBytesSchema 口径（16B = 直聊 ACI UUID 串、否则群 =
  标准 base64；ad-hoc = 房间 id hex；空字节键省略）；callId 缺席 = 无记录
  主键整事件丢弃（warn-only，零 stderr）。CallEvent 经 presage 落库
  （registered.rs message_for_sync CallMessage/CallEvent 分支）——引擎
  store 有行，connector 消费面按自身会话管线取用。
- `syncMessage.messageRequestResponse {threadAci?, groupId?,
  messageRequestResponseType}`（2026-10-06，KT 扩展批，官方 content oneof
  messageRequestResponse=14 投影）：多设备 echo 的消息请求接受/拒绝终态
  ——connector §4.58 推导第一因子权重（messageRequestResponseType ===
  ACCEPT）的引擎侧真数据面。thread 双形 binary 优先 string 兜底（UUID
  串）；群 = 32B master key 标准 base64（send groupId 口径）；
  messageRequestResponseType = 数字枚举原值（缺席/枚举外强制
  UNKNOWN=0：UNKNOWN=0/ACCEPT=1/DELETE=2/BLOCK=3/BLOCK_AND_DELETE=4/
  SPAM=5/BLOCK_AND_SPAM=6，桌面 conversationAttrs
  messageRequestResponseType 同位词汇）；线程双缺或群长度非法整事件丢弃
  （宁缺勿错，零 stderr）。signal-cli 0.14.8 JSON-RPC 无此面（仅其纯
  文本路径处理，connector implementation-plan §4.58 记档锚
  ReceiveMessageHandler.java:413-424 / JsonSyncMessage.java:41-64），属
  引擎扩展投影（§5 messageRequestResponse 收包投影批）。
- `groupUpdated {groupId, revision, expireTimerSeconds, editor?}`
  （2026-10-06，KT 扩展批群 expiration）：群更新消息内密文 GroupChange 含
  expiration_timer action（`modify_disappearing_message_timer`，field 12）
  且 change epoch ≤7 → 整信封替换为该事件（groupId = master key 标准
  base64，revision = change version，expireTimerSeconds = 解密 blob 的
  disappearingMessagesDuration 秒数、0 = 关闭，editor = 编辑者 ACI 串）；
  epoch >7 / 非 expiration change 不处理，其余群更新消息照旧跳过（宁缺
  勿错，全投影另行立项，§5 群 expiration 批）。

## 4. 夹具专有动词（非引擎表面）

`hang/sendHang/crash/crashSend/malformed/oversized/stallStdin/
armDelayedReceive/emitReceive/emitStderr/duplicate/emitEnvelope` 是
connector 测试驱动夹具的控制面，**引擎不实现也不接受**（按 §2 未知方法
拒绝）。引擎自身的失败路径由其契约测试直接驱动（超长行/坏行/EOF 等）。

## 5. 已知差异登记

- 未知方法：上游 `-32601`；夹具回显。引擎取上游行为（-32601）。
- `listAccounts` 的多账号投影、群组 Sender Keys、附件下载：夹具用标记
  文件模拟的部分，引擎在对应阶段（P1b/P3/P4）以真实实现替换；替换前的
  桩行为都在本表「对齐状态」列注明，不得当成已完成。（附件下载批勘误
  2026-10-07：现行架构下引擎侧的对应面 = receive 信封附件 `locator`
  指针投影（见 §5 receive 信封附件 locator 投影块，已 real），下载执行
  面按 connector §4.54 归 connector transit 链——引擎保持 metadata-only
  立项红线，不新增下载方法。）

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
- **textStyles**：~~本引擎钉定的 presage proto 版本无该面，receive 不投影
  （connector 侧为可选富体字段，缺失即无该键）。~~（2026-10-06 勘误：钉定
  proto **有** BodyRange.Style——本仓当时只从 mention 维度看过 bodyRanges。
  发送侧同日闭合，见下方 textStyle 透传批；**receive 侧同日再闭合**，
  Style arm 投影 `textStyles` 键，见下方「bodyRanges 收包投影」块——本条
  差异登记整体销账。）
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

安全码引擎面差异（2026-10-08，`listIdentities`/`trust` 批）：

- **数字串与 scannable 同源钉定**：`Fingerprint::new(version=2,
  iterations=5200, own_id=本机 ACI service_id_binary(16B),
  own_key=ACI identity key, their_id=对端 ServiceId service_id_binary,
  their_key=对端 ACI identity key)`——官方 safetyNumber.preload.ts:16-18
  （ITERATION_COUNT=5200/SERVICE_ID_VERSION=2）与 :34-72 生成链同参数，
  signal-cli Utils.getFingerprint（NumericFingerprintGenerator(5200)
  .createFor(version=2, …)）同构；`display_string()` 60 位 → 12 组×5 位
  空格分隔（signal-cli Util.formatSafetyNumber:52-67 同排版），
  `scannable.serialize()` = CombinedFingerprints protobuf
  （官方 QR tab `QrCode data` 即该 bytes 的 base64，scannableSafetyNumber
  字段同官方 JsonIdentity 键）。
- **trust 态引擎自有 sidecar**：`<账号目录>/identity-trust.json`
  （`{identities: {"<service id 串>": {state, addedAtMs, changedAtMs}}}`，
  临时文件+rename 原子替换，storage-manifest-state.json 先例同构）。
  presage store 无 trust 级别与 identity 时间戳位（OnNewIdentity::Trust
  自动信任），因此：默认 trustLevel = `TRUSTED_UNVERIFIED`（入库即信任）；
  `addedTimestamp` = 引擎首次投影到该 identity 的时刻（**非入库时刻**，
  与 signal-cli dateAddedTimestamp 语义偏差如实登记——store 无历史可考，
  首读建档）。trust 写面只动 sidecar（协议层 presage OnNewIdentity::Trust
  语义不变），verified 臂以数字/scannable 比对为闸（signal-cli
  IdentityHelper.trustIdentityVerifiedSafetyNumber 同构：比对不过不落
  TRUSTED_VERIFIED）。
- **trust 写面收窄**：pinned `trustAllKnownKeys` 测试模式不实现（connector
  §4.61 从不发送，fail-closed）；`state` 臂无数字闸（官方 clear 语义）；
  clear 臂对端 identity 未知同样报错（官方桌面 setVerified(DEFAULT) 对
  未知 identity 亦可落，引擎要求 identity 已知——SafetyNumberNotReady 态
  无可清除对象）。
- **verified 同步面 best-effort**：trust 落 sidecar 后自发
  `SyncMessage.verified{destinationAciBinary(16B), identityKey(对端 key
  33B), state(VERIFIED=1/UNVERIFIED=2/DEFAULT=0)}`（signal-cli
  SyncHelper.sendVerifiedMessage:254-264 同构；桌面 sendVerifySyncMessage
  同语义）。同步发送失败不回滚、不报错（signal-cli trustIdentity 对
  sendVerifiedMessage 返回值不消费——store 写为权威突变，多设备收敛由
  下次 listIdentities 读面兜底）。
- **receive 侧**（2026-10-11 B 批身份事件面补齐）：`syncMessage.verified`
  下行投影已做——主设备 verify/clear 后其他链接设备同步收口（官方多设备
  信任态收敛）：sidecar 写为权威突变（`identity-trust.json`，比对闸已由
  发起设备完成，收侧直接落态，addedAtMs 跨态迁移保留）+ receive 事件投影
  `syncMessage.verified{destinationUuid, destinationNumber?, identityKey?,
  state}`（state = trust 词表小写 `verified\|unverified\|default`，镜像
  自发面；destinationAci/Binary 双形态解析，identityKey = 对端 key 33B
  hex）；destination 或 state 缺席整信封跳过不落不投（宁缺勿错）。读面
  listIdentities 恒以引擎 sidecar 为准不变。

Storage 跨设备同步批差异（2026-10-08，chatFolders/notificationProfiles 记录面）：

- **记录 proto = 官方 vendored 完整字段面**（`crate::proto::storage`
  ChatFolderRecord/NotificationProfile）：钉定 presage rev 的
  StorageService.proto 虽已有 CHAT_FOLDER=8/NOTIFICATION_PROFILE=9 臂，
  载荷读写仍走 vendored proto + 本模块 item 加解密 shim（Account/贴纸/
  拉黑面同一通道，字段保真铁面口径不变）。
- **每实体独立 record**：官方 upload 语义 = 每个文件夹/档案一条独立
  StorageRecord + 独立 manifest identifier（非全量混排单记录）；写面按
  record id 定位云上同实体旧 identifier → 新 16B item id 换写（旧 key
  进 deleteKey，官方同实体更新语义），无旧 = 新插。
- **删除 = 墓碑 record**：`deletedAtTimestampMs` 非 0 即删除态（官方
  markChatFolderDeleted 同语义：24h 窗口内墓碑照常上传，过期才由
  chatFolderCleanupService 从 manifest 移除）；引擎 `removeIds` 参数提供
  官方 deleteKey 面（identifier 出 manifest + deleteKey），过期清理由
  调用方（connector/桌面清理作业）驱动，引擎无后台作业。
- **recipients 投影口径**：record 内 Recipient 三形态与引擎会话引用双射
  ——值形状复用引擎既有会话引用对象 `{conversationId, kind}`（§4.33 拉黑
  面同款 taxonomy；键名仍对齐官方 includedConversationIds/
  excludedConversationIds/allowedMembers）：contact{serviceIdBinary 16B
  优先/serviceId 字符串} ↔ `{kind:"contact", id=ACI/PNI service id 串}`、
  groupMasterKey ↔ `{kind:"group", id=master key 标准 base64}`、
  legacyGroupId ↔ `{kind:"legacyGroup", id=原字节文本}`；e164-only
  contact 条目非引擎会话标识口径，读面跳过该条目（宁缺勿错），写面
  conversationId 解析不出对应形态整拒 `-32603`。
- **record 内容不落引擎**：ChatFolder/NotificationProfile 是账号设置面
  （非聊天内容），但引擎保持读写直通——读面每次 manifest+items 现读，
  写面只写 storage service，引擎本地零缓存零持久化（AGENTS 引擎内容
  纪律的保守面；桌面/connector 持久化职责）。唯一本地文件是既有
  storage-manifest-state.json（best-effort 排查面，本批不扩字段）。
- **幂等与字段保真**：与云上现存同值的 record 跳过不轮换 item id
  （decode→re-encode 结构全等判据；整批全跳过 = 无 PUT 不 bump version，
  重放安全）；record 变更 = 以调用方 JSON 重建整条 record（官方
  toChatFolderRecord 同构全量写）——云端比本仓 vendored proto 更新的
  未知字段在变更写时丢弃（官方桌面以 $unknown 字节往返保真，引擎以
  vendored 官方现行 proto 为界，与 Account 面同姿态如实登记）。

Stories 分布名单记录面（2026-10-08，storyDistributionLists 读写批，
engine-compat）：

- **官方锚点与 record 形状**：StoryDistributionListRecord 七字段（官方
  SignalStorage.proto:333-341——identifier=1/name=2/recipientServiceIds=3/
  deletedAtTimestamp=4/allowsReplies=5/isBlockList=6/
  recipientServiceIdsBinary=7）；manifest 类型 STORY_DISTRIBUTION_LIST=5
  （:53）、StorageRecord.storyDistributionList=5（:77）。引擎走官方
  vendored proto（`crate::proto::storage`，字段保真口径同 Account/
  ChatFolder 面）。操作面行为参照官方 duck
  `ts/state/ducks/storyDistributionLists.preload.ts`（AGPL 只读零拷贝）。
- **投影双面（读）**：JSON 键名对齐官方 record 输出面七键——
  `identifier`（16B uuid 字节→uuid 串，非 16B 整条跳过宁缺勿错）/`name`/
  `recipientServiceIds`（service id 串面，逐条 ServiceId 解析，解析不出
  条目跳过）/`recipientServiceIdsBinary`（binary 面，16B ACI/17B PNI →
  标准 base64，同款逐条过滤）/`deletedAtTimestamp`（恒在，0=未删——813 批
  folder 的 `deletedAtTimestampMs` 恒在键同姿态，注意本 record 官方字段名
  无 Ms 后缀）/`allowsReplies`/`isBlockList`。官方合并面（storageRecordOps
  mergeStoryDistributionListRecord:2330-2343）是 binary 优先/字符串兜底
  二选一收敛成本地面；引擎两面各自独立投影原样透出，调用方择面。
- **写面**：与 chatFolders.setRecords 完全同链路（按 identifier 定位云上
  同实体旧 identifier → 新 16B item id 换写；同值幂等跳过；removeIds =
  官方 deleteKey 语义；409 采纳重放；manifest 404 首写播种 Account）。
  写入前成员双面按 ServiceId 归一化（串面 service_id_string、binary 面
  service_id_binary），保证 decode→re-encode 幂等判据稳定。`name` 在场可
  空串——软删墓碑形态官方就是「置 deletedAtTimestamp + 清名 + 清成员」
  （deleteDistributionList:218-276），与 notificationProfile 非空名闸
  刻意不同。
- **级联面归调用方**：官方删除名单时级联 deleteStoryForEveryone +
  按 distributionId 删 sender key（:255-262）——是消息面/协议面动作，
  引擎只同步 record 本身；removeIds/墓碑之后的级联清理由调用方
  （connector/桌面）驱动，引擎无后台作业（813 批墓碑语义同构）。
- **My Story 不特判**：官方 MY_STORY_ID = 全零 uuid（Stories.std.ts:114），
  record identifier 字节与普通名单同形，引擎按原样投影（零 uuid 串），
  「我的故事」聚合语义归调用方。
- **通用写面白名单修订**：`storage.write` 的 `record_kind_of` 增
  StoryDistributionList=5 arm（修订前明确拒、测试钉死）；Account=4 归
  专用写面与 CallLink/ChatFolder/NotificationProfile 白名单外的
  fail-closed 姿态不变（§3 storage.write 行与 §5 storage 写面 P0 块同步
  修订，测试同步翻钉）。
- **读写直通不落引擎**：名单是账号设置面（非聊天内容），沿用记录面批
  纪律——读面每次 manifest+items 现读，写面只写 storage service，引擎
  本地零缓存零持久化。名单展开（官方发送时本地展开
  sendStoryMessage.preload.ts:87-116）与成员引用解析归调用方；storage
  service 同步只负责跨设备收敛名单本身，不参与发送展开（官方同构）。

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

backup5 历史导出 recipient 身份投影（2026-10-04，E5；消费方 connector
契约 1.39，implementation-plan §4.38）：

- **此前差异（本笔收敛为已补）**：S2（0eedc8c）NDJSON 导出的 recipient
  只投 `{id, kind, name}`，未投会话归属身份 `aci`/`e164`/`masterKey`——
  该缺口由 connector §4.38 登记（其消费面每账号导入全部会话如实计入
  `skippedChats`）。本笔补齐后 connector 侧零改动即通：其解析宽容
  （serde Value 探针式取字段，未知字段忽略；畸形行/超长行/未知类型计数
  跳过不致命），身份字段有则消费、缺则该会话诚实 skip。
- **投影口径（backup proto 只投影不发明）**：`aci` ← Contact.aci（proto
  16 字节）→ 小写连字 UUID 串，长度畸形省略；`e164` ← Contact.e164
  （proto u64）→ JSON number（connector normalize_e164 双形态兼容
  number/string，引擎按 proto 原形投 number），0/缺席省略（不发明
  `+0`）；`masterKey` ← Group.master_key → 标准 base64（与 live 群会话
  `groupInfo.groupId`/`listGroups` id 同形同值，connector 以该形作群
  peer_key 与 live 行按身份去重），空 key 省略。身份缺/畸形一律省略键：
  connector 视作缺席，会话归属失败由其如实计入 `skippedChats`
  （kind=other 的 call link/分发列表等本就不映射 connector 会话，v1
  边界不变）。
- **kind=self 无身份可投**：proto Self_ 只携带 avatar_color；connector
  以账号注册号码（engine-state.json registry 号码→目录映射）作 self
  会话归属与自发信源，档案无需重复携带身份。

send 面 textStyle 透传（2026-10-06，消费方 connector 契约 1.69 §4.62，
735 批 declared gap 闭合；与 wt-accept 收包投影并行批零交集）：

- **上游参数名/形状证据链（全部仓内可复核）**：signal-cli 0.14.7 pinned
  发行 `lib/signal-cli-0.14.7.jar`——`org/asamk/signal/commands/
  SendCommand.class` 常量池含 `--text-style`（帮助文「syntax:
  start:length:STYLE. Unit of start and length is UTF-16 code units, NOT
  Unicode code points.」）与 JSON-RPC 参数名 `textStyle`（camelCase，错误
  原文 `Invalid textStyle syntax (`）；`org/asamk/signal/json/
  JsonTextStyle.class` record 字段 `style`(String)/`start`/`length`；
  `org/asamk/signal/manager/api/TextStyle$Style.class` 枚举 NONE/BOLD/
  ITALIC/SPOILER/STRIKETHROUGH/MONOSPACE（switchmap 映射
  `org/whispersystems/signalservice/internal/push/BodyRange$Style` 线上
  枚举）；JSON-RPC 参数键 kebab→camel 归一 = `org/asamk/signal/commands/
  JsonRpcNamespace.class` 的 `dashSeparatedToCamelCaseString`（connector
  `upstream_text_send_params` 据此投 `textStyle` 数组——engine 侧键名与
  之一字不差）。线上枚举面：钉定 libsignal-service-rs（9e6c08b）
  `protobuf/SignalService.proto:983-1001` `BodyRange.Style`（NONE=0/BOLD=1/
  ITALIC=2/SPOILER=3/STRIKETHROUGH=4/MONOSPACE=5，start/length=uint32
  UTF-16 code units，style=field 4 oneof）。
- **勘误（§5 P2 差异清单旧条目）**：钉定 presage proto **有**
  BodyRange.Style 面（本仓 receive 侧 1.25 起即过滤 `AssociatedValue::
  Style(_)`——mentions 投影只取 mention arm）；旧登记「proto 无该面」误。
  receive 投影边界不变（收侧仍不投 style arm，属收包投影区，并行批领地）。
- **校验口径 = 透传为主 + 本地形状快校验（镜像 send 面既有先例）**：
  解析闸（methods.rs）五值白名单 fail-closed（NONE 一并拒——官方 composer
  永不发出的 no-op 标记，connector 1.69 同理由）、start/length 超 u32 /
  条目缺字段 / style 非字符串 fail-closed、>64 条整拒不截断（connector
  `MAX_SEND_TEXT_STYLES` 同值镜像）；**无 body 长度边界闸**——官方
  `parseTextStyles` 不查正文边界、官方桌面发送侧自行裁剪，引擎同款不发明
  （与 mentions 同款调用方纪律）。取「快校验」而非 connector 全量镜像的
  理由：connector 1.69 的闸就是五值+≤64+u32 形状三件（其错误体文
  INVALID_REQUEST 归 connector 发明，引擎侧按本仓 send 面惯例统一
  `-32603`），无遗漏语义可补。
- **构造面（service/mod.rs `build_text_data_message`）**：五值词表 →
  prost `body_range::Style` 单点映射 → `BodyRange.associated_value =
  Style(value)`（tag=4 enumeration，repr i32 值即线上枚举序数）；与
  mentions 同发时 MentionAci(tag=3)/Style(tag=4) 两 arm 共存 body_ranges
  （官方 bodyRanges 语义）；NONE 在构造面二次 fail-closed（服务层防御，
  解析闸之后的第二道闸）。无参数 = `body_ranges` 不产生 style 条目，
  上游字节与旧版逐字节一致（单测字节级锚）。
- **边界**：不做 receive 侧 style 投影（收包投影并行批领地）；不落
  engine 本地持久化（styled echo 行是 connector 侧 pending rich payload
  职责，引擎 metadata-only）；不改 guard 形状（textStyle 不参与
  sticker/viewOnce 互斥矩阵，纯文本面附加格式）；viewOnce 与 textStyle
  同发走 viewOnce 面构造（isViewOnce 消息不带 bodyRanges，官方同构）。

bodyRanges 收包投影（2026-10-06，`textStyles` wire 键三仓并行批钉定；
与 §4.62 发送面同日对合成对称面）：

- **wire 形状**：`receive` 通知的消息投影（dataMessage / editMessage
  内嵌 dataMessage / syncMessage.sentMessage，共用 `project_data_message`
  单点）新增 `textStyles` 键——数组，条目
  `{style: "BOLD"|"ITALIC"|"SPOILER"|"STRIKETHROUGH"|"MONOSPACE",
  start: u32, length: u32}`；style = proto 枚举词原样大写，start/length =
  UTF-16 code units（proto 原语义透传，不换算 code points）。
- **proto 证据**：钉定 libsignal-service-rs（9e6c08b）
  `protobuf/SignalService.proto:983-1001`——`BodyRange{start=1, length=2,
  oneof associatedValue{mentionAci=3, Style style=4, mentionAciBinary=5}}`，
  `BodyRange.Style` 枚举 NONE=0/BOLD=1/ITALIC=2/SPOILER=3/STRIKETHROUGH=4/
  MONOSPACE=5，start/length 注释即「UTF-16 code units」。
- **投影口径（与 §4.62 发送面对称）**：
  - **NONE 不投**（发送面词表拒收对称——官方 composer 永不发出的 no-op
    标记）；滤掉后无剩余条目 → 键整体缺席。
  - **未知枚举值整条丢弃，不 fatal**（proto 只投影不发明；prost oneof
    对未知 enumeration 值保留原 i32，投影侧 `try_from` 不命中即丢）。
  - **形状坏条目丢弃**：start+length（u32 缺省 0，proto 缺字段不发明）
    超 body UTF-16 长度（UTF-16 code units 口径，跨 BMP 代理对正确计长）
    的条目丢弃；body 缺席按 0 计。
  - **重复/重叠条目不做重叠闸**：官方渲染侧直接按序应用，投影侧只丢
    形状坏的，不发明官方没有的闸（与发送面「无 body 长度边界闸」同一
    不发明纪律的收侧镜像：收侧只挡确认越界的坏形状，不裁剪不重排）。
  - **无 Style 条目不携键**：缺席=纯文本，旧宿主零变化（mentions 键
    同款缺省键纪律）；条目保持 wire 顺序（官方渲染侧按序应用）。
  - mentions 与 textStyles 各取各的 arm（MentionAci(tag=3)/binary(tag=5)
    → mentions；Style(tag=4) → textStyles），同一 body_ranges 两键共存。
- **边界**：不落引擎持久化（metadata-only 纪律，与发送面同）；cap 不设
  新闸（发送面 ≤64 是解析闸镜像，收侧 wire 条目数即发方 ≤64 纪律的
  下游，投影不截断——与 mentions `.take(64)` 上限并存不冲突：mentions
  上限是 connector 1.25 消费面既有形状，textStyles 不引入新 cap）。

附件指针投影面（2026-10-06，H3 附件媒体字节 v2 方案 C ①；消费方
connector 契约 1.55 §4.38；调研全文
docs/2026-10-06-attachment-media-bytes-v2-research.md）：

- **NDJSON wire 契约批裁定（密钥敏感性）**：`locator.key` 是附件解密钥，
  属只经内存/工件文件中转的传输材料——与 v1 已透传的 contentType/size
  同级，引擎任何持久层不留存（export 是流式 O(1) 帧内存，NDJSON 工件由
  connector 消费后按其纪律处置）；connector 侧下载后落其附件存储，
  locator 不长驻引擎盘。transfer archive 容器内无媒体字节（官方桌面
  导入消费面无第二段可读，调研 §0/§1.2），媒体可取回性完全取决于本
  指针面的导出——v1 全丢是「metadata-only」与官方的真实差距。
- **投影面（v2，`attachment_record`/`file_pointer_record` 共用
  `project_pointer_common`；proto 只投影不发明，缺/空一律省略键）**：
  在 v1 基础面（contentType/fileName/flag/size）上扩：
  `width`/`height`（Some 原样，Some(0) 也是档案陈述）；`caption`/
  `blurHash`（非空才投）；`incrementalMac`+`chunkSize` 同进同退（mac
  标准 base64，官方 filePointers.preload.ts:84-87 两者齐备才保留同构）；
  `clientUuid`（MessageAttachment.client_uuid 16B → 小写连字 UUID，官方
  bytesToUuid 同形，畸形长度省略）。`locator` 对象（键名与官方
  `convertFilePointerToAttachment`（ts/services/backups/util/
  filePointers.preload.ts:51-168，AGPL 只读参照零拷贝）产出的附件属性
  同形）：`key` ← LocatorInfo.key 标准 base64；`cdnKey`/`cdnNumber` ←
  transit_cdn_key/transit_cdn_number（cdnNumber=0 是合法 CDN0 原样投）；
  `uploadTimestamp` ← transit_tier_upload_timestamp（0 缺省省略，官方
  falsy 同款）；`digest` ← integrity_check EncryptedDigest（标准
  base64，官方同形）/`plaintextHash` ← PlaintextHash（小写 hex，官方
  Bytes.toHex 同形），oneof 二选一只投命中 arm。`size` 维持 v1 顶层位
  不挪嵌套（connector v1 消费形状不变）。下载资格判定口径 = 官方
  `isDownloadableFromTransitTier`（ts/util/Attachment.std.ts:806-825）：
  key + integrity + cdnKey + cdnNumber 四件齐。
- **实有但不投（本批裁定，不发明）**：`media_tier_cdn_number`（付费备份
  media tier 定位）与 `local_key`（本地备份加密钥）——不在 H3 字段清单，
  connector 无媒体 tier / 本地备份消费面，投了无人消费；媒体 tier 长期
  兜底属后续批（调研 §2 方案 C 编排）。
- **官方 Locator::Invalid 形态 = `locator` 键缺席**：locator_info 整体
  缺席，或 key 空 + integrity 空（官方 libsignal `LocatorInfo` 校验器
  backup/file.rs:100-177：key 非空 ⇔ transit 或 plaintextHash 至少其一
  非空；空 locator 合法 = 附件已不可取）——此时消息行照出、附件只有
  v1 基础面，connector 侧如实降级为不可下载占位（官方同款「点按重试」）。

receive 信封附件 locator 投影（2026-10-07，live receive 数据面；消费方
connector 契约 1.61 §4.54，§4.54 原文裁定「live rows store the all-None
faces … until an engine ships the receive projection——该列无需再迁移即
自动填上，attachments.download 对新消息自动 live」的引擎侧缺口闭合；与
上方导出面共用 engine-compat 1.56 wire 消费形状）：

- **缺口核实（三面证据，2026-10-07 实读）**：① connector §4.54：live 行
  locator 列（schema 19）由 receive 信封附件对象的 `locator` 键 ingest
  落库，投影器是引擎侧 `project_attachment_pointer`——缺口在引擎；②
  connector `normalized_attachment`（src/engine.rs）已实现 ingest 闸
  （1.56 caps：base64 ≤512 字符、hex ≤128 小写、cdnNumber ≤u32、
  uploadTimestamp >0、wrong-shaped keys 逐键丢弃、全空 face 折叠成
  洞——引擎投真值，caps 归消费方，与导出面同分工）；③ 下载执行面在
  connector（`attachments.download` = §4.38 step-3 transit CDN
  下载→双层校验→落盘，引擎无网新面）——引擎侧零新方法，只有投影。
- **投影面（`project_attachment_pointer` v2 扩，dataMessage.attachments
  ≤32 / previews[].image / sticker.data 共一函数）**：钉定 rev 线上
  `AttachmentPointer`（prost 生成结构实查）字段只投影不发明，缺/空一律
  省略键：`key` ← pointer.key 标准 base64（64B 加密材料，信封内存中转，
  引擎不持久化——密钥敏感性与导出面 NDJSON 裁定同级）；`cdnKey` ←
  attachment_identifier CdnKey 字符串（空省略）；`cdnNumber` ←
  pointer.cdn_number（Some(0) 合法 CDN0 原样投）；`uploadTimestamp` ←
  pointer.upload_timestamp（0 无意义省略，官方 falsy 同款）；`digest` ←
  pointer.digest 标准 base64（whole-body SHA-256，connector transit
  终闸输入）。**plaintextHash 无从投影**：钉定 rev（9e6c08b）线上
  proto 的 AttachmentPointer 无该字段（backup transit 指针专属）——仅
  digest 腿即 connector `TransitFace` 可下载形态（其闸要求 key+cdnKey+
  cdnNumber + digest/plaintextHash 至少其一）。
- **legacy CdnId 指针不发明 cdnKey**：attachment_identifier = CdnId
  (fixed64) 的旧指针无字符串形态——`cdnKey` 键缺席（宁可少投，切勿多投，
  不发明字符串化数字），下载资格闸（官方 isDownloadableFromTransitTier
  同构四件）由消费方判，该附件如实成为不可下载洞。
- **wire 纯增量**：指针无任何定位材料（metadata-only 形态）→ `locator`
  键缺席，信封与旧版逐字节同形（v1 基础面 id/contentType/filename/size/
  width/height/caption/isVoiceNote 全不动）；单测锁两态。metadata-only
  立项红线不变：字节永不下载（引擎侧无下载执行面，缓存/落盘/重试纪律
  全归 connector §4.38 step-3 机器——有界/超时/不自动重试未知结局是其
  既有记档，引擎投影面无网络路径故无新增有界参数）。

消失消息 E 批（2026-10-05，1:1 计时器设置 + 接收/导出投影）：

- **`setExpirationTimer` 语义对位（signal-cli `Manager.setExpirationTimer`，
  ManagerImpl:1419 = ContactHelper 持久化 + asExpirationUpdate 广播）**：
  先持久化 presage store Contact 面 (timer, version+1)（version 单调，
  接收方据此做乱序丢弃；新联系人 version 1，缺行 version 0+1），再向对端
  广播 expiration-update。update_expire_timer 的 contact 分支只在
  version > 现值时应用且 contacts 缺行静默 no-op——未注册联系人前置
  fail-closed（`contact_by_id` 必须命中，否则 `-32603`），宁拒不哑改。
- **构造面不设 expire_timer（上游行为登记）**：expiration-update 信封只设
  `DataMessage.Flags.EXPIRATION_TIMER_UPDATE=2` 位 + timestamp；
  presage send 面 `restore_thread_timer` 对 expire_timer==None 的出站
  DataMessage 注入 store 里的 (timer, version)（store 为 0/无 → 不注入），
  恰为官方线面信封 = flag 位 + expireTimer + expireTimerVersion
  （signal-cli SendHelper.sendMessage 的 withExpiration/withExpireTimerVersion
  同型）。若构造面显式设置 timer，presage 会对已设值再 bump version
  （store/线面脱节）——单点持久化归 store，构造面保持 None。timer=0
  （关闭）经 presage 0→None 归一后线上 = 官方桌面关闭形态（无
  expireTimer 字段的 expiration-update，官方 updateExpirationTimer(undefined)
  同形状）。
- **同值 no-op 不广播**：timer 与 store 现值相同 → 不广播不 bump version，
  返回处理时刻 timestamp（signal-cli ContactHelper 同款不触发对端）。
- **接收投影三键（缺省键不在，proto 只投影不发明）**：
  `dataMessage.expiresInSeconds` ← DataMessage.expire_timer=5（秒；
  Some(0)=关闭原样透出）；`dataMessage.isExpirationUpdate: true` ←
  flag 位 EXPIRATION_TIMER_UPDATE=2——钉定 rev 的 SignalService.proto 无
  isExpirationUpdate bool（新版 field 38），官方桌面 v8.31 收
  （processDataMessage）/发（updateExpirationTimer）两侧都用 flag 位，
  signal-cli 0.14.8 面的 isExpirationUpdate 即其同义投影；
  `syncMessage.sentMessage.expirationStartTimestamp` ←
  Sent.expiration_start_timestamp=4（官方多设备精确起算同步位；0/缺席
  不投）。收侧 DataMessage 在钉定 rev 无 expirationStartTimestamp 同名
  字段（新版 field 37，prost 解码跳未知字段）——官方 1:1「已读起算」
  语义的收侧对位缺位，本引擎不发明（新版字段上游升版后再接）。
- **backup5 历史导出扩面**：chat 行 + `expirationTimerMs` ←
  Chat.expiration_timer_ms=5（Some 才投）/ `expireTimerVersion` ←
  Chat.expire_timer_version=10（>0 才投，proto3 标量 0=未设）；message
  行 + `expireStartDate` ← ChatItem.expire_start_date=4 / `expiresInMs`
  ← ChatItem.expires_in_ms=5（Some 且 >0 才投）；计时器变更 update 成
  行（官方导出 directionless 形态，行计入 stats.messages）：
  `expirationTimerChange {expiresInMs}` ← 1:1
  ExpirationTimerChatUpdate / 群 GroupChangeChatUpdate 批内第一个
  GroupExpirationTimerUpdate（expiresInMs 原样透出，0=关闭语义不吞；
  `updaterAci` ← updater_aci 16 字节 → 小写连字 UUID，缺席/畸形省略键）；
  其余 update 类型维持整行跳过（skippedItems 可见）。connector 拿去做
  system 行（会话归属 + 新 timer 值 + 0=关闭语义）。
- **universal timer（账户级默认计时器）——勘误（2026-10-05，P5 落地）**：
  本块初版结论「AccountRecord 无 universal timer 字段、引擎栈无 live 读写
  面本批不可达」是**错误结论**，两处事实纠正：①钉定 presage rev 与官方
  StorageService.proto 的 AccountRecord **均有** `universalExpireTimer`
  （field 17，uint32 秒，0=关闭），E 批调研时误判缺位；②「写面不可达」
  随 storage 写面泛化闭合——账号设置批的 `update_account_record` 编排
  骨架（seed master key → manifest 读取 → 读改写 → PUT → 409 采纳重放）
  即 universal timer 的 live 写面。落地：`setUniversalExpireTimer`
  {account, expirationInSeconds} → `{}`（0=关闭；同值幂等无写；真相源
  单点无广播，会话级计时器才广播 expiration-update；§3 行）。原调研仍
  成立的部分：presage store 无账户级计时器独立读写面（写面走 storage，
  不经 presage store）；backup proto
  `AccountSettings.universal_expire_timer_seconds`（field 7，0=无）仅
  档案只读、导出不投 Account 帧。
- **边界（立项红线）**：不做群计时器设置（群计时器走群组更新面另行
  立项）；不做 DAR；出站普通消息不加 expireTimer 字段（per-message
  携带会话计时器是收侧兼容语义，引擎不主动扩散）；不 bump
  requiredProtocolVersion（官方发送侧同）；不投影新版 proto 才有的
  字段（钉定 rev 缺位如实登记，不发明）。
- **fake 成功面**：fake 模式 `setExpirationTimer` 成功
  `{"timestamp":u64}`，只落调度日志不触网（fake 开关仅 IPC 契约测试用，
  §6）。

账号级设置写面（2026-10-05，官方 updateConfiguration 等价）：

- **新方法 `setAccountConfiguration`**：账号级 `readReceipts`/
  `typingIndicators`/`phoneNumberSharingMode`（`"everybody"`=1/`"nobody"`=2，
  桌面 v8.31 与 Android 现行枚举一致；旧 CONTACTS_ONLY 已从官方 proto
  移除，参数不收）/`unlistedPhoneNumber` → Storage Service AccountRecord
  写面。真相源单点 = AccountRecord，无额外同步广播（其他设备经 storage
  sync 收敛）——与 `setExpirationTimer`（会话级、要广播）的本质差异。
- **写编排骨架通用化**：置顶面 `write_pinned` 的 seed master key →
  manifest 读取（404 → 最小 AccountRecord 首写分支）→ mutate →
  plan_write → PUT → 409 采纳重放骨架收敛为 `update_account_record`
  通用编排，置顶/账号设置共用（mutate 回调 None = 幂等无写直接成功）；
  置顶面行为逐字节不变。提供字段已全部满足 → 幂等无写（不 PUT 不
  bump version）。
- **只写提供的字段**：读改写仅覆写提供的键，其余账号设置（含 52-60
  通知面）原样——① vendored 官方 proto 是该写面的字段保真前提（钉定
  presage rev 类型面 decode 即丢 52-60）。零可写字段（只有 account）
  的调用 fail-closed `-32603`（不静默 no-op）。fake 模式成功 `{}`，
  不落调度日志（置顶/贴纸面同款纪律）。

个人资料写面（2026-10-05，官方 updateProfile 等价）：

- **新方法 `setProfile`**：givenName（必填非空）/familyName/about/emoji +
  avatar 三态（缺省=保留、data URI=换新、null=移除）→ `{}`。文字面 =
  全量写（缺省键置空，官方 updateProfile 同口径——官方每次写都携带完整
  加密 profile，无部分写语义）；头像三态是引擎 API 面（官方桌面无此
  分态，sameAvatar 由端上字节对比得出）。
- **文字面走 presage `Manager::update_profile`**（上游
  `upload_versioned_profile_without_avatar`，retain_avatar=true）——官方
  线面（ProfileCipher 加密 name/about/aboutEmoji → PUT /v1/profile →
  retrieve_profile 回存 store）由 presage 承担。
- **头像面是上游双层缺口自建薄封装**：presage
  `upload_versioned_profile` 的 NewAvatar 分支 `unimplemented!()`、
  libsignal-service `write_profile` 的上传响应不解析（当 String 解必败
  ——真实响应是七字段对象）。引擎对齐官方桌面行为（putProfile +
  uploadAvatar + makePutParams，不抄码）：`PUT /v1/profile` JSON
  `{version(profileKeyVersion hex), name/about/aboutEmoji(加密 b64),
  avatar, sameAvatar:false, commitment(b64)}`（本机凭据 basic auth）→
  响应 = 头像上传属性 `{key, credential, acl, algorithm, date, policy,
  signature}`（verifyAttributes 同构七字段全非空校验，缺任一 fail-closed）
  → 头像加密（`iv(12B) || AES-256-GCM(profileKey, 无 AAD, 无 padding)`，
  桌面 encryptProfile 同构，上游 `ProfileCipher::decrypt_avatar` 交叉验证
  钉定）→ multipart/form-data POST CDN0 根 `https://cdn.signal.org/`（
  官方 makePutParams 字段序逐一同构：key/x-amz-credential/acl/
  x-amz-algorithm/x-amz-date/policy/x-amz-signature/Content-Type + file
  part（application/octet-stream），boundary=`----------------<uuid 去连
  字符>`）→ 头像 path = 响应 `key`。avatar=null（移除）= 同一 PUT
  `avatar:false`（响应体不消费），服务器面清头像。
- **写序与 storage 面同步**：server 面（presage 或自建 PUT/CDN）先行 →
  storage 面 AccountRecord.givenName/familyName/avatarUrlPath 同步（官方
  updateProfile 同序；多设备经 storage sync 收敛）。头像上行失败归
  `STORAGE_WRITE_FAILED`（写意图未完整落地，不新增结构化码）。
- **头像入参面**：data URI 必须 `image/*` 族 + base64 + 非空 + ≤10MB
  （官方 AvatarHelper 同量级，超出 fail-closed）；givenName 空、可选文本
  字段非字符串同 `-32603`。头像内容引擎不嗅探不缩放（官方桌面缩放到
  640×640 由连接器负责，引擎按原字节加密上行）。bincode 直依赖新增仅为
  上游同款 commitment 原始字节序列化（presage 传递已有 1.3.3，无新版本
  入图）。
- **fake 成功面**：fake 模式 `setProfile` 成功 `{}`，不落调度日志（置顶/
  贴纸/账号设置面同款纪律）。

个人资料写面·对齐批②（2026-10-06，connector 契约 1.46 §4.45 对齐）：

- **背景与裁定**：setProfile 实现本体（官方 updateProfile 等价 + 头像上行
  薄封装）2026-10-05 已落地（上一块）；本批对齐 connector 契约 1.46 的
  三处形状/错误面差异，并补字节预算闸。connector 形状实查以其
  docs/implementation-plan.md §4.45 为准（`givenName` required、可选
  `familyName`/`about`/`emoji`/`avatar`——参数名是 `emoji` 非
  `aboutEmoji`，引擎面与本批前实现一致）。
- **result 形状**：`{}` → `{"status":"saved"}`（connector 契约字面同形；
  `status` 键为引擎/connector 双侧稳定判定位）。
- **头像上行错误面**：`STORAGE_WRITE_FAILED` → `-1 PROFILE_UPLOAD_FAILED`，
  data `{retryable: true, textSaved: bool}`。语义可区分（卡在头像链而非
  storage 记录面）且与 connector 声明的 retryable=true 对齐；不占用
  connector 结构化码白名单之外的位（`-1` + message 码位，AVATAR 族同款）。
- **部分成功语义（诚实呈现）**：写序 = server 面文字先落（官方
  writeProfile 同序：putProfile 在 uploadAvatar 之前）→ 头像加密/CDN
  上行 → storage 面。头像链任一步失败返回 PROFILE_UPLOAD_FAILED：
  - Replace（data URI）：文字面已提交 → `textSaved: true`；多设备收敛为
    文字面新值 + 头像旧值（官方客户端同态，无撕裂）；本机 storage 面未
    同步，重试成功后补齐。
  - Remove（null）：`avatar:false` PUT 失败 → `textSaved: false`（文字面
    亦未提交——remove 分支的 profile 写与头像清除是同一次 PUT）。
  - Retain（缺省）：不走头像链，失败仍归 storage 四码/presage 错误面。
- **文字面字节预算（mirror connector §4.45）**：UTF-8 字节数 given ≤128、
  family ≤128、about ≤512、emoji ≤32（官方桌面 ProfileEditor.dom.tsx
  `maxBytes: 128` / bio `maxByteCount: 512` / emoji 仓内绑定 32 同源），
  超预算参数面 `-32603` fail-closed（不触网）。多字节字符按字节数判定。
- **头像协议形状裁定（avatar_form 不采用）**：任务参照的官方
  ProfileAvatarUploadHelper「avatar_form PUT multipart → avatarUrlPath」
  是 Android 新链；本仓行为锚（本地 Signal-Desktop checkout v8.31
  writeProfile/putProfile/uploadAvatar 实查）与钉定栈（presage 33dd149
  `update_profile` → libsignal-service 9e6c08b `write_profile`）均是经典
  链（PUT /v1/profile avatar:true/sameAvatar → 响应上传属性 → CDN0
  multipart POST → path=响应 key）。对齐以锚内实查为准：维持经典链
  （avatarUrlPath 等价物 = 上行属性 `key`，storage 面 avatarUrlPath 字段
  语义不变）；Android avatar_form 链待栈/桌面锚实际切换时按 §5 惯例另批。
- **钉定栈 pub 面结论（33dd149 / 9e6c08b 实查）**：文字面 pub API 在用
  （presage `Manager::update_profile`，retain_avatar=true 通路）；头像面
  上游双层缺口维持（presage `upload_versioned_profile` NewAvatar 分支
  `unimplemented!()`、libsignal-service `write_profile` 上传响应错解
  String——真实响应是七字段对象），自建薄封装继续；替换条件 = 上游补面
  （AvatarUploadAttributes 解析 + NewAvatar 上行），届时整段替换。
- **receive 投影边界（登记不做）**：本机 setProfile 后官方多设备镜像经
  syncMessage.profileKeyUpdate；钉定 rev 的 `SyncMessage.Content` oneof
  无该变体（实查生成 prost：仅 contacts/blocked/configuration/keys 等），
  引擎 receive 投影无从投影——其他设备的资料镜像（profile 变更收投影）
  另行立项，本批边界。
- **测试面**：单测 116→118（+2：文字预算逐键 128/129、512/513、32/33 与
  多字节按字节判定；UploadFailed text_saved 两态编码面）；契约 23→24
  （+1：调度零头像字节——fake 模式 setProfile 全程不触网不落盘，数据目录
  文本工件扫不到 data URI/base64 形；调度日志不落条目纪律原测试扩形状）；
  fake 成功形状三形态更新为 `{"status":"saved"}`，参数面新增三个预算超限
  形状，real 模式补未注册账号 setProfile 不触网折叠。

会话拉黑只读面（2026-10-05）：

- **新方法 `getBlockedConversations`**：`{account}` → `{blocked: [...]}`。
  真相源 = Storage Service 独立 Contact/GroupV1/GroupV2 记录的 `blocked`
  位（ContactRecord=9、GroupV1Record=2、GroupV2Record=2，① vendored 官方
  proto 字段保真读面）——拉黑状态不经 AccountRecord，与置顶面（账号记录
  内嵌数组）的存储形态本质不同，故读面走「manifest 全部三类 identifier
  批量原样读」（`read_raw_items` 复用，type=1/2/3 presage 枚举直判），
  不开新模块。条目形状与置顶面同构（`conversationId` + `kind`），contact
  口径与 `storage_to_engine` 对齐（aciBinary 16B → UUID 串优先、字符串
  形态直通、e164 跳过）。
- **投影口径**：group = master key 标准 base64（send groupId 口径）、
  legacyGroup = legacy id bytes（从 UTF-8 lossy）、contact = ACI UUID 串
  （listContacts uuid 同源）。存储序 = manifest identifiers 首现序。坏条
  目（服务器缺 item/解密 MAC 失败/proto 解码失败/标识映射不出）单条跳过
  零 stderr（daemon 静默纪律，与贴纸批量读同款），不炸整面。
- **v1 裁剪**：不投 `blockedAtTimestamp`（官方 ContactRecord=27/
  GroupV2Record=13 有该字段）——连接器消费面 v1 只需名单本身，时间戳面
  留待消费方提出再扩（不发明用途）。只读面：不落本地 store、不动
  manifest（无写序问题）。
- **fake 纪律**：fake 模式恒 `{blocked: []}` 不触网不落调度日志；real 模
  式未注册账号 → storage 上下文打不开折叠 `-32603`（置顶面同款）。

备份导出消费面（2026-10-05，connector 契约 1.47 §4.46；引擎契约
docs/backup-export-contract.md §10 为完整裁定，此处登记 RPC 面边界）：

- **新方法 `backup.export` / `backup.exportStatus`**（独立扩展方法，非
  signal-cli 兼容面）：文件工件交接——connector 把消息面导出为
  `backup-export-input.ndjson` 写入引擎按账号目录（与 backup5
  `history-import.ndjson` 同目录对称），`backup.export` 启动引擎后台任务
  （专用 OS 线程）读工件→帧序状态机→官方 §5 快照；`backup.exportStatus`
  轮询状态与快照清单。
- **接收管线活性**：导出在专用线程执行（装配链纯同步、全 `Send` 输入，
  备份导出契约 §10.3），LocalSet 上的接收管线与 dispatch 全程不被阻塞；
  AEP 读取（presage，非 Send 面）在 start 阶段 LocalSet 内完成，派生密钥
  字节独占移交线程。活性由引擎测试锁定。
- **工件删除硬边界**（engine AGENTS.md 消息内容不落引擎存储）：任务离开
  running（成功或任何失败）即删输入工件；start 阶段首行非法同步拒收时
  当场删除；崩溃残留由 `exportStatus.artifactPresent` 如实报告、下次导出
  终态删除。快照（密文）引擎不自删，清单在 `snapshots` 报告，清理归
  connector（保留最近一次，导出前清旧）。
- **密钥边界**：AEP 从 presage store kv 读、在引擎内派生即用即弃；任何
  RPC 不返回密钥材料（Android 端用自己的 AEP 现场恢复，备份导出契约
  §10.5/§9 调研 6）。`media_root_backup_key` 每导出随机 32B（§10.5，附件
  批重审）。
- **结构化错误码 5 枚**（`-1` + message 码位）：
  `BACKUP_EXPORT_ALREADY_RUNNING`/`BACKUP_ARTIFACT_MISSING`/
  `BACKUP_ARTIFACT_INVALID`/`BACKUP_SNAPSHOT_EXISTS`/`BACKUP_IO_FAILED`
  （备份导出契约 §10.7，含 retryable 映射计划；connector 白名单登记归其
  二期）。参数面（缺 account/未注册/fake/AEP 缺失）一律 `-32603`。
- **v1 裁剪**：chat 行不带计时器字段（frames.rs chat() 一期不扩展，
  connector 会话计时器面已在其自身链路处理；二期随 Chat.expireTimerMs 帧
  面一起开）；附件/引用/回应/编辑等富体面不在工件行集（§6.2 后续批），
  由 connector 侧逐因计数（其契约 §4.46）。

快照导入收向面（2026-10-08 B2 批，connector 契约 1.78；引擎契约
docs/backup-export-contract.md §11 为完整裁定，此处登记 RPC 面边界）：

- **新方法 `backup.importSnapshot` / `backup.importSnapshotStatus`**（独立
  扩展方法）：官方桌面本地备份快照收向——connector 传入用户选定的官方
  快照目录路径（同机路径交接，§10.2 artifact 反方向同款），引擎 start 同步
  跑官方 `verifyLocalBackupMetadata` 对齐校验（decode → version==1 → iv
  12B/encryptedId 16B → CTR 解密 backupId 逐字节比对），后台 `spawn_local`
  跑官方 `FramesReader` 解密分帧，逐帧投影 NDJSON 到
  `<账号目录>/history-import.ndjson`（§4.38 存量迁移通路同一落点）。与
  §10 导出面共用密钥派生链（§3）与快照目录形态（§5），方向互逆。
- **帧循环单点**：`history::export_frames_loop`（transfer archive 导出与
  快照导入同一帧循环）——NDJSON 行形状逐字一致，connector 两遍有界导入
  零改动消费。
- **原子落盘**：临时名 `history-import.ndjson-tmp` 写全后 rename 原子替换
  （POSIX rename 不打断并发读）；任务失败删半成品、既有 NDJSON 不动——
  失败形态即 NDJSON 缺席或旧内容，connector 按文件存在性判定。
- **执行机制（与 §10 导出不同）**：帧导出走官方 `FramesReader`（async、
  内部 Rc 非 Send），future 进不了 OS 线程边界 → `spawn_local` 承载
  （dispatch 本就跑在 LocalSet 内）；NDJSON 每行小块 io 不构成阻塞面。
- **metadata_key 派生面**：官方链里 metadata_key 只依赖 AEP（不掺 ACI），
  backup_id 依赖 (AEP, ACI)——同 AEP 异账号快照解密可解但比对必不等，
  `KEY_MISMATCH` 走比对路径（官方 Bytes.areEqual 同语义）。
- **结构化错误码 5 枚**（`-1` + message 码位）：
  `BACKUP_IMPORT_ALREADY_RUNNING`/`BACKUP_SNAPSHOT_MISSING`/
  `BACKUP_SNAPSHOT_METADATA_INVALID`/`BACKUP_SNAPSHOT_KEY_MISMATCH`/
  `BACKUP_SNAPSHOT_IO_FAILED`（备份导出契约 §11.4；connector 1.78 白名单
  同步）。参数面（缺 account/snapshotDir/未注册/fake/AEP 缺失）一律
  `-32603`。

头像下行 / 拉黑写面 / 举报（2026-10-06 扩展批，KT 扩展方法）：

- **`getAvatar`（1:1 与群统一入口）**：会话标识按形状分派（ACI/PNI
  ServiceId UUID 直通 → 手机号文本 contacts 反查 → 32B 标准 base64 群
  master key；三者皆不中 fail-closed `-32603`）。下行 = presage 官方头像
  API：1:1 `retrieve_profile_avatar_by_uuid`（profileKey 取 presage store
  `profile_key(service_id)`——contacts 同步随行落库，未同步过 → 诚实
  `AVATAR_UNAVAILABLE`「无头像」，不做 CDSI 反查）、群
  `retrieve_group_avatar`（master key 派生 GroupSecretParams 解密，
  master key 与 listGroups id 同源）。403/404 = 无头像（官方 1:1
  未 accept 会话不下载头像同语义）折叠 `AVATAR_UNAVAILABLE`；其余错误
  （网络/解密/存储）`AVATAR_FETCH_FAILED`；尺寸帽 10MB（presage 注释
  口径，Signal Android 分配量级）超出 `AVATAR_TOO_LARGE`。出参 data URI
  + 字节数：MIME 魔数嗅探（官方 sniffImageMimeType 同表），未识别回退
  `application/octet-stream`（官方桌面头像落盘不标 MIME；引擎 data URI
  必须给类型，不猜 image/*）。
- **`setConversationBlocked`（blocked 位写语义）**：官方桌面
  storageRecordOps 同语义——置拉黑写 `blockedAtTimestamp` = 引擎时钟
  （首次拉黑时刻）；已拉黑再拉黑幂等无写（**不重写他端时刻**，含 0=未知
  原样保留）；解除 = `blocked=false` + 时刻清零；GV1 无时间戳字段（官方
  proto 同构）仅翻位。写面复用置顶面骨架：decode→mutate 仅动 blocked
  面（vendored proto 字段保真）→ 新 16B item id 重加密 → version+1 →
  PUT → 409 响应体采纳重放；返回写后全量 blocked 名单（存储序，与
  getBlockedConversations 同形状）。目标云上无记录：blocked=true → 新建
  最小记录（标识 + blocked 面，e164 命中面可得则写；manifest 404 时
  master key 播种 + version 1 最小 AccountRecord 同批首写）；blocked=false
  → 无可解除，幂等成功不播种不建 manifest。**纯拉黑不删会话**：不动消息
  面/会话行（官方桌面同语义，拉黑只置位不清数据）。
- **拉黑面遗留（明确不在本批）**：`SyncMessage.Blocked` self-send（本机
  拉黑名单变更向自有其他设备同步的独立通道）不实现——Storage Service
  blocked 位写面已是官方桌面现行真相源（多设备经 storage sync 收敛），
  Blocked sync 消息是其 legacy 补充通道；消费方提出需求再立项。
- **`reportSpam`（逐条举报）**：官方桌面同款 REST——`POST
  v1/messages/report/{senderAci}/{serverGuid}` 逐条独立请求，JSON body
  `{token}`（envelope 级 reportSpamToken 透传；缺省序列化为 `{}`，官方
  jsonData 同构）；本机凭据 basic auth（put_profile 同通道）。**单批上限
  1000**：对齐官方 reportSpamJobQueue 入参 zod `serverGuids.max(1000)`
  （早期「256 或按行评审」提案作废，取官方现行常量）。**部分失败诚实
  面**：单条 4xx/5xx 记状态码、传输层失败无状态码（缺席键不在，不发明
  0），至少一条成功即返回 `{reported, failures}`，全部失败才整体
  `-1 REPORT_SPAM_FAILED`（不掩盖部分成功）。serverGuid 参数面每项必须
  可解析 UUID 且经长度（36/32）+ hex 字符白名单预检（`parse_str` 本身
  接受 urn:uuid:/花括号形态，预检堵住）后归一为小写连字形态入路径——
  防路径注入 fail-closed；`serverGuids` 空批 = 无意图调用 `-32603`（
  setAccountConfiguration 零可写字段先例）。
- **receive 投影配套**：信封公共位加 `serverGuid`（Metadata.server_guid
  有值才投；proto 只投影不发明）。**已知边界（token 不透投影）**：
  envelope 级 `reportSpamToken` 经 presage Content 转换丢弃（Metadata
  仅携带 server_guid），引擎收投影无 token 可透——调用方举报时可自选
  透传 token 参数，token 从 receive 面拿不到（上游 Metadata 升版携带后
  再接，不发明）。
- **fake 纪律**：`getAvatar` → `AVATAR_UNAVAILABLE`、
  `setConversationBlocked` → `{blocked: []}`、`reportSpam` → 全量成功
  `{reported: N, failures: []}`。调度日志：`reportSpam` 落（协议上行面，
  send 系/setExpirationTimer 同纪律），`getAvatar`/`setConversationBlocked`
  不落（读面/storage 状态面，置顶/贴纸面同纪律）。real 模式未注册账号
  一律 `-32603`（细节不外泄）。

群终止批（2026-10-06，官方桌面 terminate-group 行为对齐，KT 扩展方法）：

- **新方法 `terminateGroup`**：`{account, groupId}` → `{status:
  "terminated"}`。协议载体 = GroupChange.Actions 特殊 action
  `terminate_group`（`TerminateGroupAction {}`，官方 SUPPORTED_CHANGE_EPOCH
  = 7）；version = 当前 revision + 1（饱和不回卷；revision 未知退化 0
  继续，官方 `group.revision || 0` + 1 同口径）；上传 = `PATCH v2/groups`
  （storage 服务 host，body = GroupChange.Actions protobuf，Content-Type
  `application/x-protobuf`，官方 modifyGroup 同端点同形状）；群权限
  （仅 admin 可终止）由服务器裁决。同值幂等：进程内终止表命中或服务器
  群态 terminated 为真 → 不重复上传直接成功（setConversationBlocked
  幂等纪律先例）。
- **群凭据链是上游缺口自建等价实现**：官方 GroupsManager
  get_authorization_for_today 绑定 SignalWebSocket（本引擎不开常驻群
  ws），引擎以 zkgroup 公共 API 自建同语义链——`GET
  v1/certificate/auth/group?redemptionStartSeconds=<今日 UTC 零点>&
  redemptionEndSeconds=<+7 天>&pniAsServiceId=true`（本机凭据）→
  camelCase JSON `{credentials: [{credential, redemptionTime}]}` →
  AuthCredentialWithPniResponse receive → GroupSecretParams present →
  basic auth `username = GroupPublicParams bincode hex`、
  `password = presentation hex`（官方 GroupsManager.authorize 输出面同
  构）。上游补独立凭据面后整段替换（单点 `service/groups.rs`）。
- **发送联动面（send 硬拒）**：进程内终止表命中（real 模式另以服务器
  群态兜底，读面失败不阻塞发送——网络故障不误伤既有群会话，终止真值
  以登记表 + 服务器裁决为准）→ send 系全部群组形态返回
  `SEND_TARGET_TERMINATED`（`-1` + message）；sendError 单点折叠（此前
  send 系错误一律 `-32603` 内部失败的折叠面收拢为 helper，TargetTerminated
  之外的枚举行为不变）。
- **receive 联动面（groupTerminated 事件）**：dataMessage.groupV2 /
  sent 内密文 group_change 经 `decrypt_group_change` 解出 terminated
  action → 整信封替换为 `groupTerminated {groupId, terminator?,
  timestamp}` 并登记终止表；change epoch >7 不处理（官方
  isChangeEpochSupported 同语义：未知 epoch 的 change 含未知语义）；
  非 terminated 的群更新消息照旧跳过（宁缺勿错）。
- **已知边界**：① `presage::model::groups::Group` 无 terminated 字段
  （上游模型缺口），读侧绕行 libsignal-service `decrypt_group` 投影；
  ② 终止表进程内存、不跨重启——重启后 send 面由服务器群态读面兜底、
  listGroups `terminated` 位同源；③ receive 只投 terminated change 事件，
  其余群更新（成员/权限变更）继续跳过（群更新全投影另行立项）；
  ④ groupTerminated 事件不带群名/成员快照（终止后服务器群态即不可读，
  官方桌面终止后群视图同样只读墓碑）。

通话消息面（2026-10-06 扩展批，KT 扩展方法）：

- **`sendCallMessage`（1:1 通话信令外发）**：官方 Content.callMessage=3
  信封 `CallMessage{opaque{data, urgency}, destinationDeviceId?}`。opaque
  = Signal-RTC 层信令负载（现行桌面 offer/answer/ice/hangup/busy 全封装
  在 opaque 内），**引擎不解析只透传**（base64 出入参，元数据面不动负
  载字节）；urgency 缺省 droppable（proto「缺省按 DROPPABLE 处理」同语
  义）；destinationDeviceId 官方多设备定向投递位，缺省不写。收件人号码/
  UUID 双形态解析（send 系同口径）；返回信封 timestamp。
- **群臂（2026-10-08 P4b 群信令出网批，connector 契约 1.82 wire 面，
  engine-compat）**：`sendCallMessage` 增可选 `group`（32B 群 master key
  标准 base64，listGroups/terminateGroup/updateGroup 的 groupId 同源口径）
  与 `recipient` **两腿恰一互斥**（同置/都缺/任一腿形状非法 `-32603`；
  `groupId` 键维持禁入——历史 fail-closed 面保留，wire 参数名是 `group`）。
  群腿仅 modern 面放行：官方群信令恒 opaque（桌面 #handleSendCallMessage
  ToGroup calling.preload.ts:3338-3407 构 `CallingMessage{opaque{data}}` →
  callingMessageToProto 写 Opaque.urgency → sendCallingMessage.preload.ts:
  99-118 isGroup 臂 `sendContentMessageToGroup` sender-key 群发），legacy
  裸帧族群向无官方先例、sender-key 群发无设备定向位（destinationDeviceId
  仅 1:1 臂）——两者与 `group` 同置 fail-closed。发送面 = `open_group_
  sender`（群可解 + terminated 硬拒 `SEND_TARGET_TERMINATED`，send 系群组
  形态同纪律）+ `send_message_to_group`（sendTyping 群臂同原语；服务器群
  态不在库 UnknownGroup → `-32603` 折叠）。RingRTC 群发第四参
  overrideRecipients 子集发射本批不做（connector 无此参数面、relay v2
  §6-5 帧收值即拒，引擎参数面无此键）。接收投影零改动（信封级无群上下
  文，CallMessage proto 无群字段，opaque 内嵌群身份由 RingRTC 分路）；
  fake 模式群臂成功 `{"timestamp":u64}` 与 1:1 同口径，伪群 id（非法
  base64/≠32B）在参数层拒绝（不触服务层）。此前「群组通话不做（groupId
  形态 fail-closed）」边界由本批放开并作废——群通话信令与 1:1 共用
  CallMessage.opaque 通道（SFU 仅媒体面），非异构通道。
- **legacy 裸帧族（2026-10-06 legacy 帧批，connector 契约 1.53 协同）**：
  RingRTC 2.72.1 实证出向 **hangup/busy 帧不带 opaque 材料**（legacy 五值
  帧：Busy{id}/Hangup{id,type,deviceId}，官方 proto 裸帧族），modern
  面无法承载——sendCallMessage 增 `legacy` 可选参数：
  `{kind:"busy", id}` | `{kind:"hangup", id, type?, deviceId?}`（type 词汇
  normal/accepted/declined/busy/needPermission，缺省 normal；id 由调用方
  给正数，proto uint64 域）。按官方 proto 标准面构造（官方桌面
  callingMessageToProto busy={id}/hangup={id,type}；signal-cli 0.14.7
  CallManager sendBusyViaSignal/sendHangupViaSignal 同构；单测钉 prost
  字节编码：busy=字段5 长度分隔{id varint}、hangup=字段7{id,type,
  deviceId}、两面无 opaque 字段10）。**两面互斥**（data 与 legacy 同置或
  都缺都 `-32603`，官方语义无混面帧）；legacy 面忽略 urgency 参数（裸帧族
  无 opaque 信封，urgency 属 Opaque 子消息字段2——入参在场不报错，仅
  modern 面生效）。
- **接收投影**：CallMessage 体 → 信封 `callMessage`（opaque base64 +
  urgency + 旧线面 offer/answer/iceUpdate/busy/hangup 在场字段逐面投影，
  proto 只投影不发明；空白 CallMessage 整事件丢弃）；SyncMessage.CallEvent
  体 → `syncMessage.callEvent`（官方桌面 getCallEventForProto/
  conversationPeerIdInBytesSchema 行为对齐：conversationId 16B→UUID 串、
  群→标准 base64、ad-hoc→hex、空→省略；callId 字符串形态=官方
  longToString 同构、缺席整事件丢弃 warn-only）。此前「call/story/null
  体跳过」边界由本批放开 call 两面，story/null 维持跳过。
- **发送通路偏差（登记）**：官方桌面 call 消息 online=true（不排队，
  离线对端不收）；presage `Manager::send_message` 硬编码
  `online_only=false`（上游 TODO issue #252）——引擎发出的 call 信封在
  对端离线时会排队补投，属可接受偏差（信令面重复到达幂等），上游补
  online 选项后跟进。**不能自建 MessageSender 绕行**：
  `new_message_sender`/`identified_websocket`/credentials 均 pub(crate)
  （引擎拿不到凭据），官方 online 面待上游 API 后替换。
- **sent transcript 缺席（登记）**：CallMessage 无 sent transcript——
  libsignal-service `create_multi_device_sent_transcript_content` 对
  CallMessage 返回 None（presage 发送面 `if let Some` 守卫静默跳过），
  本机其他设备不收自发 call 记录（官方桌面自发 call 的多设备同步走
  CallEvent sync 通道，与 transcript 面独立）；不向 self 发 call
  （self-send 走 message_to_self 分支对 CallMessage 报 NoMessagesToSend，
  引擎无此调用形状）。**CallMessage/CallEvent 经 presage 接收面落库**
  （registered.rs message_for_sync 两分支 Some(message)）——引擎 store
  有行，与「消息内容不落引擎存储」边界的边界判例一致（信令/记录面元
  数据，非聊天正文）。
- **fake 纪律**：`sendCallMessage` 成功 `{"timestamp":u64}`，只落调度
  日志不触网（send 系同款）；fake 模式任意账号成功早退（形状桩语义），
  未注册账号错误面由 real 模式契约测试覆盖。

群 expiration 批（2026-10-06，官方桌面 buildDisappearingMessagesTimerChange
行为对齐，KT 扩展方法；connector 契约 1.45 §4.44 `conversations.setExpireTimer`
群形态的本引擎面债务批）：

- **新方法 `updateGroup {expiration}`**：`{account, groupId, expiration}`
  → `{status: "sent"}`（群族成功答同纪律）。协议载体 = GroupChange.Actions
  `modify_disappearing_message_timer`（`ModifyDisappearingMessageTimerAction
  { timer = 1 }`，field 12——编号依据 = 本仓钉定 libsignal-service-rs
  rev 9e6c08b 的 `protobuf/Groups.proto` 实查，官方 storageservice
  Groups.proto 同源同号，未发明）；timer = 加密
  `GroupAttributeBlob { disappearingMessagesDuration = 3 (uint32 秒，
  0 = 关闭) }`（官方桌面 toGroupActionsParams + buildDisappearingMessages
  TimerChange 同形状：blob 明文 protobuf → `encrypt_blob_with_padding(
  randomness, blob, 0)` = 桌面 encryptGroupBlob 同构）；expiration=0 =
  关闭（桌面 UI off 档同语义，线面 duration 0 原样入 blob 不转缺席）。
- **复用③b（群终止批）骨架**：群态读面取当前 revision（`GET v1/groups/`
  + decrypt_group，网络失败退化 0 继续写交服务器裁决，terminate 面同
  口径；其余读面错误上抛）→ version = revision + 1 饱和不回卷 → 群凭据
  basic auth → `PATCH v2/groups` 上传（同端点同 Content-Type）→ 成功。
  **不做同值幂等折叠**：群 revision 每次变更 +1 是官方写面语义，timer
  同值再写同样推进 revision 并广播，交服务器与成员侧收敛。
- **admin 权限服务器裁决**：非 admin 上传 → 403（GROUP_UPDATE_FAILED
  data.status=403）；409 = revision 竞争需重拉群态；401 凭据无效；
  400 形状被拒。错误族与 terminateGroup 同枚举同口径（TerminateError：
  UnknownAccount/UnknownGroup → -32603，Failed → -1 +
  GROUP_UPDATE_FAILED + data.status，Unavailable → -1 +
  STORAGE_UNAVAILABLE）。
- **receive 投影最小放开**：dataMessage.groupV2 / sent 内密文
  group_change 经 `decrypt_group_change` 解出 expiration_timer action →
  整信封替换为 `groupUpdated {groupId, revision, expireTimerSeconds,
  editor?}`（expireTimerSeconds = 明文 duration，0 = 关闭——解密侧
  `GroupChange::Timer` 的 `Option<Timer>` 对 0 duration 归一 None，投影
  `unwrap_or(0)` 还原，两端同语义）；change epoch >7 不处理；terminated
  优先（两投影同走 group_change 密文，内容互斥由 change 类型定，terminate
  命中即返回）；非 expiration 的群更新消息照旧跳过（宁缺勿错，全投影
  另行立项）。connector 消费面另行：桌面 timer 镜像走其既有
  conversation.changed 管线，本批不定义 connector 消费形状。
- **边界（本批红线）**：updateGroup 其余参数族（成员/权限/头像/名称/
  描述/邀请链接等，connector 契约 1.45 §4.44 全集）**不收**——本批只有
  expiration，防止与既有群管理面打架；expiration 缺席 = 无操作整拒
  `-32603`（不静默 no-op，零可写字段先例）。不做群 timer 幂等（见上）。
  fake 模式成功 `{status: "sent"}`，落调度日志（协议上行面，
  terminateGroup 同纪律）。

群头像上行面批（2026-10-09，官方桌面 buildUpdateAttributesChange/
uploadAvatar/uploadGroupAvatar 行为对齐，KT 扩展参数族；expiration 批
登记的「头像不收」红线由本批放开。connector 契约 groups.update avatar
参数接引为其并行批，本批只交付引擎面）：

- **updateGroup 增可选 `avatar`（三态，镜像 setProfile）**：缺省 = 保留
  现有头像（不发 avatar action）、data URI = 换新、null = 移除；data
  URI 形状与预算逐字复用 setProfile 先例（`data:image/*;base64,` 规范
  base64、解码后非空且 ≤10MiB、非 image 族/非规范 base64/超帽/非字符
  串 `-32603` fail-closed 不触网，解析面直接复用 `parse_avatar_data_uri`
  同一函数）。expiration 与 avatar 至少一项在场：双缺 = 无操作整拒
  `-32603`（零可写字段先例，expiration 批语义推广）；avatar 单独在场
  （expiration 缺席）合法——官方单 action change 同形状。
- **官方行为锚（Signal-Desktop v8.31 本地检出实查，行为对齐不抄码）**：
  三态裁定 = buildUpdateAttributesChange（groups.preload.ts:767-786）：
  `'avatar' in attributes` 三分支——字节在场 → `uploadAvatar` 后
  `modifyAvatar={avatar: uploadedAvatar.key}`；undefined（null）→
  `modifyAvatar={avatar: null}`；不在 attributes → 不改。上行链 =
  uploadAvatar（groups.preload.ts:360-399）：`GroupAttributeBlob
  {avatar: bytes}` → `encryptGroupBlob`（= `encrypt_blob_with_padding`
  ，expiration 批同构原语）→ uploadGroupAvatar（WebAPI.preload.ts:
  4638-4673）：群凭据 basic auth `GET v2/groups/avatar/form`（:824 端
  点表）→ 响应 `AvatarUploadAttributes` **protobuf** 解码 →
  verifyAttributes 七字段全非空 → makePutParams → CDN0 根 multipart
  POST（与 profile 面同一 makePutParams 函数，字段序逐一同构）→ 返回
  key（= avatar path）。
- **proto 形状（编号依据 = 本地官方 protos/Groups.proto 实查，与 vendored
  libsignal-service rev 9e6c08b protobuf/Groups.proto 逐字同号）**：
  `AvatarUploadAttributes{key,credential,acl,algorithm,date,policy,
  signature = 1..7}`（:11-19）、`ModifyAvatarAction{string avatar = 1}`
  （:209-211）、`modifyAvatar = 11`（:258）。prost 生成面已含两者（
  presage re-export `libsignal_service::proto`），引擎零 vendored 改动。
- **上游缺口与薄封装裁定**：presage rev 33dd149 `Manager` 无群写面
  （registered.rs pub 面仅 `retrieve_group_avatar` :456 读面）；钉定
  libsignal-service rev 9e6c08b `GroupsManager` 仅读面
  （get_authorization_for_today/fetch_encrypted_group/retrieve_avatar，
  groups_v2/manager.rs），vendor 内无 `groups/avatar` 端点。本批**不动
  vendor**：所需原语全部在引擎可用 pub 面（`GroupSecretParams::
  encrypt_blob_with_padding`、群凭据 `group_authorization`、`patch_group`
  均为本仓既有件；CDN multipart POST 为 setProfile 面既有自建件），上行
  链在引擎 `service/groups.rs` 组装，上游补群写面后整段替换（setProfile
  头像面同款登记）。
- **合成单 change 裁定**：一次 updateGroup 调用的 expiration + avatar
  合成**一个** GroupChange.Actions（同 version = revision+1，两个 action
  各就各位）单次 PATCH——协议线面 Actions 多 action 是官方正式形状
  （decrypt_group_change 解出 changes 数组）；官方桌面无 timer+avatar
  同调先例（两者是不同 UI 入口各自 modifyGroupV2），引擎 API 面合成裁定
  登记于此。写序 = 群态读面取 revision（Unavailable 退化 0，expiration
  批同口径）→ avatar 上行链（Replace 臂：form 拉取 → blob 加密 → CDN
  POST → key）→ PATCH 合成 change。avatar 移除（null）无上行链环节，
  modify_avatar action 以**空串**入线（proto3 string 无 presence，官方
  `{avatar: null}` 经 protobufjs 编码即空 message；服务器语义 = 清头像，
  与 presage 读侧 `group.avatar.is_empty()` 判定自洽）。
- **错误面（镜像 setProfile PROFILE_UPLOAD_FAILED 形状，值诚实偏离）**：
  新码位 `GROUP_AVATAR_UPLOAD_FAILED`（`-1` + message，data
  `{retryable: true, textSaved: false}`）——avatar 上行链任一步失败
  （form 拉取非 2xx/网络、属性解码/七字段校验失败、blob 加密、CDN POST
  非 2xx/网络）。**textSaved 恒 false 与 setProfile 的 Replace 臂
  （true）不同**：profile 面文字面先行单独提交（两次 PUT），头像失败时
  文字面已落地；群面是合成单 change，avatar 上行在 PATCH **之前**，失败
  时整个 change 从未上送——expiration 伴生面同样未提交，诚实呈现
  false（不抄形状抄语义）。PATCH 面错误维持 expiration 批四码：服务器
  4xx/5xx `GROUP_UPDATE_FAILED`（data.status 透传）、传输面
  `STORAGE_UNAVAILABLE`。参数面（avatar 形状/预算）不占用结构化码
  （`-32603`，setProfile 同纪律）。
- **调度日志纪律（avatar 键摘要化）**：updateGroup 方法级 dispatch 日志
  保留（expiration 批契约钉定，mutating 面同款），但 avatar 键落盘前
  形状摘要化（`"[avatar data uri: N bytes]"`）——头像 data URI/base64
  载荷零字节进日志（setProfile「调度零头像字节」同纪律；groupId/
  account/expiration 键照旧逐字保留）。
- **fake 两臂纪律**：fake 模式 avatar 缺省/null 臂成功 `{status: "sent"}`
  （PATCH 形状桩，expiration 批纪律照旧）；data URI 臂失败
  `GROUP_AVATAR_UPLOAD_FAILED`——fake 无 CDN/服务器上行面，上行链不可达
  即错误面（stickers「fake 模式走错误面」先例），成功/失败两臂均在 fake
  契约层可测。
- **入站帧闸层间事实（登记）**：引擎入站有界读帧帽 1MiB
  （main.rs `MAX_LINE_BYTES`，超帽帧以 parse error 拒收）——经 IPC 面
  avatar 解码量物理上限 ≈786KB，参数面 10MiB 超帽闸在 IPC 面不可达（该
  形状由 `parse_avatar_data_uri` 单测钉定，profile/群两族共用同一函数与
  闸位）；契约层 oversize 用例不再收（入站闸先拦，非 -32603 码位）。
- **receive 投影边界（登记不做）**：成员侧收到的 avatar change 继续跳过
  （宁缺勿错，expiration/terminate 最小面纪律）——头像变更投影与群头像
  缓存失效联动另行立项；本端写后 listGroups/getAvatar 以下一次群态读面
  为准。connector 消费面（groups.update avatar 参数接引、错误码白名单
  登记）为其并行批，本批不定义。

receive 链 profileKeyUpdate 批（2026-10-06，官方桌面 profileKeyUpdate /
profileKeyHarvest 行为对齐；setProfile 写面批（main 8eabf67）登记的
receive 面债务批）：

- **背景**：DataMessage.profileKey（field 6）是发送方随行的「本条消息
  作者当前 profile key」——对端改资料（新 key 版本）或首次私聊（key 采集）
  都经该位送达；profile key 是解对方 profile（含头像）的凭据，没存它
  getAvatar 的 1:1 面永远 `AVATAR_UNAVAILABLE`。setProfile 批只做了本机
  写面，receive 链对该字段无处理（投影层不读，静默丢弃）——本端联系人
  profile 身份验证面（safety number 变化感知/资料指纹刷新）不收敛。
- **解析层落点勘定**：钉定 presage rev 33dd149 的 proto 已携带该字段
  （生成 prost `DataMessage.profile_key: Option<Vec<u8>>`，tag 6，
  libsignal-service 9e6c08b 实查），presage Content 转换不丢——丢弃点在
  引擎投影层（`project_content`/`project_data_message` 不读该字段）。
  本批在投影层接入，无需动 vendored proto。
- **官方行为锚（Signal-Desktop v8.31，MessageReceiver.preload.ts
  handleDataMessage + background onProfileKey，行为对齐不抄码）**：
  ① 带 profileKey 的 1:1 DataMessage 一律先按 sender ACI 存储（官方
  `hasProfileKey` 分支，PROFILE_KEY_UPDATE flag 在场 = "profileKeyUpdate"
  事件、不在场 = "profileKeyHarvest" 事件，两态同存储行为）；② flag
  PROFILE_KEY_UPDATE=4 在场时消息体不进普通投影（官方对该行清 body/
  attachments，只走 profile key 事件）；③ key 与已存值相同 → 不触发
  profile 刷新（官方 `hasChanged` 门）；④ key 变化 → 触发该联系人
  profile 重取。
- **引擎落地（最小面）**：`project_content` 收侧从 DataMessage 提取
  profile_key（bytes）→ fail-closed 校验（协议定长 32 字节：缺席/空
  bytes 不处理；长度非法丢弃该字段并继续普通投影，不崩不发明）→ 按发送
  方 ACI（Metadata.sender）经 presage store 既有 profile key 位落库
  （`ContentsStore::upsert_profile_key`，presage sqlite `profile_keys`
  表 = contacts 同步随行落库的同一位，不发明新存储）→ 幂等闸：与已存
  key（`profile_key(service_id)`）逐字节相同则不再动；不同/首次 → 落库
  并挂 `contactProfileUpdated` 事件通知（连接器消费面），实际 profile
  重取由消费方经既有 getAvatar 面按需拉取（引擎不持 profile 缓存，刷新
  调度留既有机制，compat §5 惯例）。
- **投影差异**：① flag PROFILE_KEY_UPDATE=4 在场的 1:1 信封整事件替换
  为 `contactProfileUpdated`（对齐官方「不进普通消息面」——该行无用户
  可见正文）；② 无 flag 的普通消息携带 profileKey（采集形态）→ 照常
  走普通投影（正文不吞），profile key 落库与事件挂载静默并行；③ 群内
  带该字段不处理（profile key 是 1:1 作者面语义，群成员 key 面走群组
  更新，另行立项）；④ sent sync（本机其他设备发出的镜像）不处理（自己
  的 key 面归本机写面 setProfile）；⑤ 事件面形状
  `{method: "receive", params: {account, envelope: {timestamp,
  sourceUuid, contactProfileUpdated: {aci}}}}`——aci = 事件主体即发送方
  ACI（UUID 串，与 sourceUuid 同值，连接器免二次解析；不投 key 字节，
  密钥材料不出引擎存储面）。存储失败（store 错误）静默忽略不炸投影
  （receive 面零 stderr 纪律，下次消息重试自然收敛）。
- **边界（本批红线）**：不动 setProfile 写面；不做 CDSI 号码反查（与
  getAvatar 同口径）；不做 profile 主动重取调度器（presage 栈无后台
  profile 刷新任务面，消费方按需拉取）；不动 vendored proto。协议栈无
  读写面的能力不顺手做。

storage 写面 P0（2026-10-06，`storage.write` 批量 manifest item 写，KT
扩展方法）：

- **定位**：既有 storage 写面全部是「语义级」的——置顶/拉黑/账号设置/
  贴纸包各自封装官方 toXxxRecord + generateManifest 语义，调用方只能表达
  「把 X 设为 Y」；本批新增**通用**写面 `storage.write`：调用方自带
  StorageRecord 明文与 item key，引擎只做协议面翻译（加密 + WriteOperation
  组装 + PUT 上行），不感知业务字段。两者并存：语义级写面继续负责 409
  采纳重放（conflictBackOff 3 次），通用写面面向外部协调方，冲突诚实上抛。
- **参数命名**：`accountId`（本扩展契约原形，非白名单面惯用的 `account`
  ——通用写面契约先行定形时按扩展方法族 accounts.updateConfiguration 的
  accountId 命名惯例收敛）。
- **record 载荷与白名单闸**：record = 官方 vendored proto `StorageRecord`
  编码 bytes 的标准 base64。解码 + arm 白名单双闸：只收 Contact=1 /
  GroupV1=2 / GroupV2=3 / StoryDistributionList=5 / StickerPack=6 五类
  manifest 记录（StoryDistributionList=5 随 2026-10-08 Stories 名单面批
  放开，见该批记档块）；Account=4
  归专用写面（账号记录字段面引擎全量保真责任在语义级写面，通用写面不
  收）；其余 arm（CallLink/ChatFolder/
  NotificationProfile）与解码失败/无 arm 一律 fail-closed `-32603`。
  官方桌面 v8.31 的 storage record arm 集以 vendored proto
  （`storage_record::Record` 九 arm）为准。
- **官方行为锚（Signal-Desktop v8.31 `ts/services/storage.preload.ts`，
  行为对齐不抄码）**：① sync() 读到 `version <= localManifestVersion`
  即 refuse-and-resync（manifest version 是单调闸，写方必须 version+1）；
  ② upload() 先 sync 拿最新 manifest 再组装 WriteOperation PUT；③ PUT 409
  → conflictBackOff 自动重放（[1s,5s,30s]）。引擎差异登记：语义级写面
  保留官方 409 自动重放；`storage.write` **不自动重试**——mutating 语义
  且调用方意图（record 内容）引擎不可重推，409 答
  `STORAGE_VERSION_CONFLICT`（`-1` + message + data
  `{retryable: false, serverVersion?}`，serverVersion = 409 响应体
  StorageManifest.version 明文可读），调用方重读 manifest 后自行重放。
  文件名勘误：任务原文提到的 `ts/services/storageService` writeDelta 在
  v8.31 检出中不存在，写面对齐锚为 storage.preload.ts 的 sync/upload。
- **item id 轮换**：UPDATE 一律轮换新 16B item id（旧 key 进 deleteKey、
  新 key 入 identifiers）——官方换记录必轮换 generateStorageID，其他设备
  以 identifier diff 判变更（同 key = 未变不重拉），原地改写同 key 会静默
  失联。INSERT 用调用方自带 key 原样（调用方自担唯一性，撞已存在 key 见
  幂等/冲突条）。
- **幂等面（重放安全）**：内容比较需读现存 item 明文（`PUT
  /v1/storage/read` 批量 + item 解密 shim，与语义级写面同通道）。INSERT
  key 已在 manifest 且明文相同 → no-op；明文不同 → 意图冲突 fail-closed
  `-32603`（引擎不静默替换外部记录）。UPDATE 明文相同 → no-op；key 不在
  manifest → no-op（重放安全：上次写已生效或从未存在）。DELETE key 不在
  → no-op。全 no-op → 不 PUT 不 bump version，`version` = 当前 manifest
  version。从未有 storage 状态（master key 缺失）：纯 UPDATE/DELETE 批 =
  幂等成功不播种（version 0）；含 INSERT → master key 播种 + version 1
  最小 AccountRecord 同批首写（§5 首写分支同款，Account 记录用官方
  vendored proto 编码走 item 加密 shim）。UPDATE 换记录类型（同 key 在
  manifest 中为其他类型）→ 意图冲突 fail-closed。
- **fake 纪律**：参数闸全量生效后回 `{version: 0, results: [...]}` 桩
  （INSERT/UPDATE 回 writtenKey = 原 key，DELETE 无 writtenKey，无 no-op
  标注）；不落调度日志（storage 状态面纪律）。
- **边界（本批红线）**：不做 clear_all / resetWithNewKey / 账号重置面；
  不做业务字段校验（record 内容语义归调用方，引擎只验 arm 白名单）；无
  人工批大小上限（协议面无此闸，调用方自控）；PUT 成功后不做 fetch
  manifest 回读确认（200/204 即成功语义与官方 upload 同）；不感知
  sticker 面 item key 缓存（本地 manifest 状态文件 stickerItemKeys 沿用
  旧值，贴纸面读侧以服务器 manifest 为真相源不受影响）。

messageRequestResponse 收包投影批（2026-10-06，connector §4.58 收包面
债务批，KT 扩展投影）：

- **背景**：connector 1.65（§4.58）把会话摘要 `messageRequestAccepted`
  做成读时多因子推导，官方第一因子权重 = `messageRequestResponseType ===
  ACCEPT`；该因子此前在两条引擎上都无诚实来源——signal-cli 0.14.8 只在
  其纯文本路径处理该 sync（connector §4.58 pinned-tree 记档锚：
  ReceiveMessageHandler.java:413-424；JSON-RPC wire 面 JsonSyncMessage.
  java:41-64 整个省略该字段），本引擎投影层原先同样丢弃
  （`project_content` SynchronizeMessage 臂 `_ =>` fall-through）。
  connector 侧把该缺口记为「engine-side extension, never invented」的
  recorded follow-up（connector implementation-plan §4.58 Reachability
  record；connector store.rs `derive_message_request_accepted` 注释同
  位），本批落地收包面。
- **解析层落点勘定**：钉定 presage rev 33dd149 的上游 proto 已携带该面
  ——libsignal-service 9e6c08b `SignalService.proto` SyncMessage.content
  oneof `MessageRequestResponse messageRequestResponse = 14`（line 865），
  `MessageRequestResponse{threadAci=2, groupId=3, type=4,
  threadAciBinary=5}`（line 666-678；Type 枚举 UNKNOWN=0/ACCEPT=1/DELETE=
  2/BLOCK=3/BLOCK_AND_DELETE=4/SPAM=5/BLOCK_AND_SPAM=6；threadE164 已
  reserved 废止，线面只有 aci/groupId 两形态）；生成 prost
  `sync_message::Content::MessageRequestResponse` oneof 变体与
  `parse_thread_aci` helper 都在（proto.rs:178-184）；presage Content
  转换不丢（`impl_from_for_sync_message!(MessageRequestResponse)`，
  content.rs 同表）。引擎收包管线与 sent/viewOnceOpen/callEvent 同一
  oneof 直达投影层，本批在投影层接入，无需动 vendored proto。
- **官方行为锚（Signal-Desktop v8.31，第一手实读，行为对齐不抄码）**：
  MessageReceiver.preload.ts `#handleMessageRequestResponse`
  （:3274-3320；sync 分派 :3104-3110）：thread = fromAciUuidBytesOrString
  （binary 16B 优先、string 兜底）；groupId 必须 32 字节
  （GROUPV2_ID_LENGTH = :191）→ `Bytes.toBase64`，长度非法
  removeFromCache 整事件丢弃；type 经 isKnownProtoEnumMember 强制已知
  枚举否则 UNKNOWN（:3304-3309）——缺席/未来枚举值不丢弃。事件面
  messageRequestResponseType 为数字枚举原值（messageReceiverEvents.std.ts
  :387；会话 attr 同名 number 位 model-types.d.ts:413），消费面
  background.preload.ts onMessageRequestResponse（:3774-3810）→
  MessageRequests.onResponse 写会话 attr → isConversationAccepted
  （ts/util/isConversationAccepted.preload.ts:21-82，第一因子权重
  `messageRequestResponseType === ACCEPT` :38）。
- **引擎落地（最小面）**：`project_content` SynchronizeMessage 臂增
  MessageRequestResponse 分支 → `project_message_request_response` 投影
  `syncMessage.messageRequestResponse {threadAci?, groupId?,
  messageRequestResponseType}`：thread = `parse_thread_aci`（binary 优先
  string 兜底）→ ServiceId UUID 串；groupId = 32 字节闸 + 标准 base64
  （send/listGroups groupId 口径）；type 缺席/枚举外 → UNKNOWN=0（官方
  coercion 同构），已知值数字原值投影。线程双缺（无可路由会话）或群
  长度非法 → 整事件丢弃（宁缺勿错，零 stderr，receive 面纪律）。单测
  两面：helper 全口径（六枚举原值往返、双形、32B 闸、UNKNOWN 强制、
  丢弃面）+ project_content 端到端（accept/delete/群三形态投影形状、
  信封公共位纯增量、不可路由丢弃）。
- **投影差异（诚实边界）**：① 本面只覆盖**多设备 echo**（手机端/其他
  链接设备接受或拒绝后同步过来）；本机自己的接受/拒绝在 Signal 协议里
  是「发同步」不「收同步」——发起设备的 write outcome 即调用返回值，
  引擎收包面永远看不到自己的 echo，connector 的本机 §4.56 写面（上游
  sendMessageRequestResponse）终态仍以写调用结果为准（该写面本引擎已于
  同日落地，见下方 sendMessageRequestResponse 写面批——与 §4.56 对齐，
  本投影与其构成「本机写 → 多设备 echo 收」对称面）。
  ② 引擎不落会话摘要表、不做
  conversation-level accepted 查询面（§4.58 记录的另一半选项）——
  「accept 终态进 connector 摘要」的落库与推导消费在 connector 侧执行，
  引擎只投影事件原值不持久化。③ UNKNOWN=0 对推导零权重（官方因子只在
  精确 ACCEPT 时放行），投影 UNKNOWN 是官方 coercion 行为不是发明。
- **wire 兼容（键面纯增量）**：新键只出现在 `syncMessage` 对象内（既有
  键零改名零移除）；现行 connector `normalize_receive_fields` 对
  syncMessage 只读已知键（sentMessage/viewOnceOpen/callEvent…），仅含
  messageRequestResponse 的 syncMessage 落「contacts/groups/read sync —
  ignore」skip 臂（connector engine.rs:2912-2924 实读）——旧宿主无该键
  不炸、不落行，1.65 推导照旧走本地派生 fallback；新宿主消费面按 §4.58
  记录升级为权威读取，字段名 `messageRequestResponseType` 与官方因子
  词汇一字不差。
- **边界（本批红线）**：不动 §4.56 写面（引擎 sendMessageRequestResponse
  同日落地，见下方写面批）；不做会话级持久化/查询面；不动 vendored proto；
  不做 threadE164 面（上游已 reserved 废止）；协议栈无读写面的能力不顺手做。

sendMessageRequestResponse 写面（2026-10-06，connector §4.56 写面债务批，
KT 扩展方法；上批「收包投影批」红线登记的本面立项）：

- **背景与对齐口径（connector 钉定契约逐字核验）**：connector 1.63
  （§4.56）`conversations.acceptRequest`/`conversations.declineRequest`
  是钉定上游 `sendMessageRequestResponse`（signal-cli pinned checkout
  `v0.14.7-4-g472784e6`，`client/src/jsonrpc.rs:342`）之上的宿主薄封装，
  其上游 params 形状（connector service.rs
  `prepare_conversations_message_request` 实读）：`{account,
  recipient(单值)|groupId, type}`——direct 会话 = 上游 `recipient` 一腿
  （connector peer_key 直通）、群 = `groupId` 一腿（群 master key 原样，
  `listGroups` id 同源）、type 由宿主盖章 `accept`/`delete`
  （supervisor.rs `message_request_request` 实读）。本引擎面与该形状逐字
  对齐：参数名 `account`/`recipient`/`groupId`/`type` 一字不差；recipient
  腿收 ACI/号码双形态（引擎 send 系 `resolve_recipient` 既有口径，connector
  peer_key 两形状都是其子集）、groupId 腿 = 32B 标准 base64
  （`decode_group_id` 同源闸）。connector 收敛宿主词表只走 accept/delete
  （官方 CLI 词表仅此两值，`client/src/cli.rs:664-669`），引擎线面收全
  官方六值——这是引擎/宿主的词表差（引擎面是协议面，宿主面是产品面），
  非 shape 分叉：引擎多收的四个词全部映射官方枚举原值，connector 打到
  引擎的 accept/delete 两词行为逐字一致。
- **协议事实（钉定栈实查）**：libsignal-service 9e6c08b
  `SignalService.proto` SyncMessage.content oneof
  `MessageRequestResponse messageRequestResponse = 14`（line 865），
  `MessageRequestResponse{threadAci=2, groupId=3, type=4,
  threadAciBinary=5}`（line 666-679），Type 枚举 UNKNOWN=0/ACCEPT=1/
  DELETE=2/BLOCK=3/BLOCK_AND_DELETE=4/SPAM=5/BLOCK_AND_SPAM=6；
  threadE164（field 1）已 reserved 废止，线上只有 aci/groupId 两腿。
- **官方发送侧锚（Signal-Desktop v8.31 第一手实读，行为对齐不抄码）**：
  `ts/textsecure/SendMessage.preload.ts:2314-2360`
  `getMessageRequestResponseSync({threadAci?, groupId?, type})`——
  `MessageSender.padSyncMessage({content:{messageRequestResponse{type,
  groupId: options.groupId ? … : null, threadAciBinary/threadAci}}}`
  （`isProtoBinaryEncodingEnabled()` 现行面直写 threadAciBinary 16B raw
  UUID bytes，legacy 形态写 threadAci string；两形态互斥写），payload
  type `messageRequestSync`；调用方
  `ts/state/ducks/conversations.preload.ts:3781` `syncMessageRequestResponse`
  → :3821 `singleProtoJobQueue.add(MessageSender.getMessageRequestResponseSync
  ({threadAci, groupId: conversation.getGroupIdBuffer(), type: response}))`
  —— ACCEPT（acceptConversation :4001-4013）/DELETE（deleteConversation
  :4141-4147）/BLOCK（blockConversation :4089-4095）/SPAM（reportSpam
  :3899-3901）全族直传 proto 枚举序数，群腿 = 群 master key buffer。发送
  目标 = 自己 ACI（`serviceId: myAci`）——消息请求响应是「本机决策」的
  多设备同步，不走收件人会话。
- **构造面（service/mod.rs `build_message_request_response_sync`）**：
  thread 腿 ACI 可解析 16B → `thread_aci_binary` 直写（官方现行面同构），
  已闸非 UUID 串降写 `thread_aci` string 形态（官方 legacy 分支同位）；
  群腿 = `MessageRequestTarget::Group(master_key)` 原样 bytes；
  `SyncMessage::with_padding` 同源 padding（官方 padSyncMessage 同位）；
  oneof 变体 `Content::MessageRequestResponse`（与收包投影
  `project_message_request_response` 同一生成类型——本写面与其严格对称：
  本机写 → 服务器 → 多设备 echo 收，两端共用字段 2/3/4/5 与六值枚举）。
  presage self-send 通路与 sendViewOnceOpen 同款（compat §5 E4 已验证）；
  钉定 rev「多设备自发 SyncMessage 二次同内容转录重发」怪癖同面（接收端
  幂等，compat §5 E4 已登记）。单测字节级钉：thread 腿 = 字段5{16B UUID}+
  字段4{枚举}（prost 字段序升序）、群腿 = 字段3{32B master key}+字段4、
  string 兜底形态 = 字段2{utf8}+字段4，字段序与 tag 值对照官方 proto 手编。
- **参数闸（methods.rs，fail-closed `-32603`）**：type = 六值驼峰词
  白名单（`message_request_type_of` 单点映射官方枚举；`unknown`/数字
  （含 0/1/7 等越界）/词表外/大小写漂移（`ACCEPT`/`block_and_delete`）/
  布尔全拒——UNKNOWN=0 不在词表，官方发送侧无「发未知」语义）；两腿恰一
  （同置/双缺/`recipient` 数组形状/群腿非 base64/≠32B 全拒）；account
  缺失同拒。协议上行面落调度日志（send 系同纪律）；成功答空结果对象
  （上游钉定面成功无 JSON 输出同款，connector §4.56 `MessageRequestOutcome
  {status}` 宿主面之上的引擎诚实形态）。
- **返回值语义与未知结局纪律**：写结局即返回值——manager 打开失败
  （UnknownAccount）/腿解析失败/上行失败（Internal）按 SendError 上抛折叠
  `-32603`，成功 = 服务器已受理的信封上行；引擎不缓存不重发，未知结局
  （进程被杀/连接断）由调用方按终态处理——connector §4.56 的
  `SEND_OUTCOME_UNKNOWN` 不重试纪律（AGENTS.md 边界）在引擎侧的对应位
  就是「一次上行调用、无自动重试」，纪律不破。
- **fake 纪律**：参数闸全量生效后成功 `{}`，只落调度日志不触网（send 系
  同款）；real 模式未注册账号折叠 `-32603` 不触网。
- **边界（本批红线）**：不动收包投影（bbf3087 已落地，本面为其对称写面，
  两批零交集）；不做 accept 附带 profile-sharing 位写（官方
  SyncHelper.java:412-415 的 acceptConversation 语义在 signal-cli 形态下
  经 contacts 同步收敛，引擎面为纯 sync 写；桌面侧 flag 归宿主
  conversations.setBlocked/资料面组合）；不收 recipient 数组（上游钉定面
  recipients[] 在该方法的官方调用形状实为单值语义，connector 宿主面恒
  单值——引擎闸单字符串防误用）；不动 vendored proto；不做
  threadE164 面。

用户名读面批（2026-10-08，KT 扩展，getUsername/usernameLookup）：

- **真相源单点**：本账号用户名 = Storage Service AccountRecord.username(33)
  + usernameLink(35)（官方 proto 注释「仅 server confirm/delete 时更新」，
  多设备经 storage sync 收敛）。引擎不设第二真相源——presage 注册数据无
  username 字段，whoami 只回 hash 不可逆，桌面侧亦以 storage 面为同步
  真相（本地面仅模型缓存）。getUsername 读 33/35 投影；写面已落地，由
  写面经 `update_account_record` 维护同字段（写面批）。
- **lookup 方向锚定**：服务端公开 lookup 通路只有「hash/链接 → ACI」单向
  （`GET v1/accounts/username_hash/{hash}`、`GET
  v1/accounts/username_link/{uuid}`，均强制匿名——带凭据 400）；「ACI →
  username」无服务端通路。本面按服务端既有通路落（条目 = 全名/链接，
  回 ACI），与 connector 契约 1.59 §4.55 `username.lookup` 的条目形状
  （全名或链接）同构对齐，投影面补 `username` 键（链接条目解密所得）。
- **已注册校验为全批前置**：usernameLookup 的匿名 lookup 本身不需要账号
  凭据，但 account 参数照常走账号解析梯（未注册 `-32603`）——与一切
  account 参数面同口径，防引擎侧哑放行（getUserStatus 同款纪律）。
- **逐条降级不炸批**：单条解析/链接取载/解密/命中任一环失败 =
  `{recipient, isRegistered:false}`（与上游 getUserStatus 未命中同形状）；
  参数面形状错才整批拒。批帽 20/条帽 64 与 connector §4.55 帧界同源镜像。
- **crate 依赖**：`usernames`（libsignal v0.99.0 同 tag 直接依赖，图中
  既有解析，AGPL-3.0-only）——语法/hash/proof/link 加解密全走官方原语，
  引擎零自研密码学。

用户名写面批（2026-10-08，KT 扩展，usernameReserve/usernameConfirm/usernameDelete）：

- **两相编排归 connector**：官方 reserve→confirm 两相在引擎侧保持独立
  方法（与官方 AccountsManager 分步同构），`accounts.setUsername` 的
  reserve→confirm 编排由 connector 宿主面承担（KT 扩展方法两跳）。中间
  态（已 reserve 未 confirm）服务器无正式副作用（reserve 不落 username，
  官方同语义）；撕裂面由 confirm 的语法重验 + hash 失配 fail-closed 闸
  兜住。
- **四枚结构化错误码**（`-1` + message）：`USERNAME_TAKEN`（reserve 409 /
  confirm 410，retryable=false——用户名被占需换名）、
  `USERNAME_RESERVATION_GONE`（confirm 409，retryable=false——预约过期
  需重走 reserve）、`USERNAME_RATE_LIMITED`（413/429，retryable=true）、
  `USERNAME_SERVER_FAILED`（其余，retryable=true）。参数面（语法闸/hash
  失配）一律 `-32603`，不占结构化码。
- **诚实部分成功**：confirm 服务器已确认（200 `usernameLinkHandle`）而
  storage 33/35 落盘失败 → STORAGE 族透传浮出——服务器态已变更（username
  已生效），调用方按 STORAGE 语义处置后以 getUsername / 多设备 storage
  sync 收敛；引擎不缓存第二真相（读面批同锚）。
- **无 case-change 快捷路径**：官方桌面同名只换大小写走本地快捷改写（跳
  reserve）；引擎每次 setUsername 全走 reserve→confirm 两相（服务器是
  hash 真相，本地无既有 hash 可依赖，多一跳成本可忽略）——connector 侧
  同口径，不做快捷分支。
- **无 usernameChangeSyncMessage**：官方桌面 confirm 后向本账号其他设备
  发同步消息；引擎多设备经 Storage Service AccountRecord 33/35 storage
  sync 收敛（官方 proto「仅 server confirm/delete 时更新」同锚），不发
  第二信令（置顶面 storage-first 纪律同构）。
- **fake 纪律**：参数闸全量生效后形状桩——reserve 本地候选 + hash（真实
  crate 原语，形状与 real 一致）、confirm 本地 link（随机 handle 不落盘）、
  delete `{}`；全不触网不落调度日志（storage 状态面纪律）。
- **real 网络路径离线不可测**：无夹具服务器，reserve/confirm/delete 的
  服务器路径以 crate 原语单测（语法/hash/proof/link round-trip）+ fake
  形状契约测试覆盖；服务器码位（409/410/413/429）按上游
  AccountController 钉定（读面批同款锚定方式）。
**Stories 发送批（2026-10-08，KT 扩展 `sendStory`，E1）**

- **背景与口径**：引擎钉定栈（presage 33dd149 / libsignal-service 9e6c08b）
  无 story 发送面；任务口径 =「薄 fork、只扩最小 story 发送面」。vendor
  原样拷贝 + `[patch]` 本地接线（vendor/README.md）之上，fork 面只动三处
  （websocket/sender.rs 新增 `send_messages_unidentified_story`、sender.rs
  `try_send_message` 增 story 参数 + 新增 `send_story_message`、presage
  `Manager::send_story_message`），其余上游代码零漂移；改动清单逐文件登记
  于 vendor/README.md。
- **官方行为锚（Signal-Desktop v8.31 第一手实读，行为对齐不抄码）**：
  `ts/jobs/helpers/sendStory.preload.ts`——profileKey 必需（:72）进
  StoryMessage（:198-205 同构 presage send_message 注入 DataMessage）；
  textAttachment{text, textStyle, preview.image 随批 CDN 上行}（:161-184）；
  跨名单按 conversation 去重（`recipientsByServiceId` Map 保首现序，
  :217-246）；自同步仅在 ≥1 收件人成功且有其他设备时发送
  （storyMessageRecipients 空 / 单设备即跳过，:575-590）；story 恒走
  sealed sender 通道（`OutgoingMessage.preload.ts:392-394` auth='story'
  优先级最高）；`WebAPI.preload.ts:3804/3850` `PUT /v1/messages/{dest}?
  story=<bool>` 同查询参；libsignal v0.99.0
  `rust/net/chat/src/ws/messages.rs:141-150`（story 路径后缀）+ `:39`
  （Story→无 user-based 授权头）。
- **传输面（fork）**：单收件人 sealed-sender PUT 带 `?story=true`、**不带**
  `Unidentified-Access-Key` 头、identified 通道无降级（官方 story 优先级
  最高，失败按错误上抛不回退——引擎逐收件人失败整体 `-32603`）。
- **构造面（service/stories.rs）**：text story 单面——`StoryMessage{
  profile_key: None（send 面注入）, group: None（群 story v1 不做）,
  allows_replies, body_ranges, attachment: TextAttachment}`；textStyle 六值
  词表（官方 TextAttachment.Style 枚举词表同构）DEFAULT=字段省略；
  bodyRanges 五值内联样式与 send textStyle 同款双层闸（methods 词表白名单
  + 服务层 from_str_name 防御）；preview = image/url/title 三件组装
  （2026-10-09 扩面）：previewImage data URI 随批 CDN 上行指针（官方
  preview 同批上传同构）+ previewUrl/previewTitle 如实透传（methods 参数
  闸：url 非空 ≤2048 字符且 `https://` 开头——官方 isValidLink https-only
  同构 LinkPreview.std.ts:54-65，字符面计数；title 非空 ≤1024 UTF-8 字节，
  字节面计数；违规整拒不静默丢键）；三件任一在场即成 Preview，官方
  preview 面 image 可空同构（`hydratedPreview.image && uploadAttachment`）；
  description/date 官方 story 预览面无源恒缺席。
- **分发面**：解析全量前置（任何号码不可解析=无上行整拒）→解析后
  ServiceId 去重保首现序（号码/UUID 双形态收敛同一会话）→单时间戳逐收件
  人分发。per-recipient 失败整体 `-32603`（部分失败 result 形状未对齐，
  §5 既有 send 纪律）。
- **自同步面**：≥1 收件人成功后 best-effort 发 `SyncMessage.Sent{
  destination_service_id=own ACI, timestamp, story_message,
  story_message_recipients[destination_service_id, distribution_list_ids=
  [distributionListId], is_allowed_to_reply=allowsReplies]}`（官方收尾
  同构）；同步失败不回滚已成功上行（官方「同步失败不视为发送失败」同
  口径）；单设备 NoMessagesToSend 良性静默。
- **偏差登记**：①官方同步消息的 `isUpdate`/`urgent` 位在本仓钉定 proto
  无对应字段（ Sent 无 tag11/urgent），缺席即缺省——升级上游时需补钉；
  ②官方发送对象为 distribution list 成员展开集，引擎只收显式 recipients
  （成员解析归 connector，任务口径）；③`destination_e164` 不投（引擎其他
  sync 面同纪律，收端按 service id 路由）。
- **fake 纪律**：参数闸全量生效后成功 `{timestamp, results:[]}`，只落调度
  日志不触网（send 系同款）；real 模式未注册账号折叠 `-32603` 不触网。
- **边界（本批红线）**：群 story（GroupContextV2 位）/文件 story（image
  正文 attachment）/story 删除（remoteDelete story 形态）/24h 过期（纯客
  户端语义）不做；接收投影在 E2 批（下一 commit），两批零交集；
  `distributionListId` 引擎不校验 UUID 形状（官方钉定 id = MY_STORY_ID 零
  UUID，其余名单 id 为任意 UUID 串——引擎透传给同步面，connector 侧闸）。

**Stories 接收批（2026-10-08，E2，与发送批同分支双 commit）**

- **背景**：钉定栈接收面除 `Metadata.story` 位外零 fork——presage 收侧对
  StoryMessage 跳过存库但 `Received::Content` 照常 yield（registered.rs
  接收管线），投影位只需 Metadata 透传（fork 清单见 vendor/README.md，
  store-sqlite 两侧补字面量纯编译面、story 位不落库）。
- **官方分类锚（MessageReceiver.preload.ts:1517-1556 第一手实读，行为对齐
  不抄码）**：`isStory` = 体含 storyMessage；`isStoryReply` =
  dataMessage.storyContext；`isGroupStoryReply` = isStoryReply ∧
  dataMessage.groupV2；`isDeleteForEveryone` = dataMessage.delete。闸：
  `envelope.story ∧ ¬(isGroupStoryReply ∨ isStory) ∧ ¬isDeleteForEveryone`
  → 整信封丢弃；反向违例（体是 story/群回复但信封无位）官方仅 warn 照续。
  另有「stories 功能禁用即丢弃」腿——引擎无账号设置读面，不设（边界）。
- **引擎闸落点（project_content 头部，先于 profile-key 落库）**：metadata
  story 位 `Some(true)` 且体既非 StoryMessage、非群 story 回复、非 remote
  delete → 空向量整信封丢弃（官方同款）；位在场 = 信封级 `isStory: true`
  如实投影（群 story 回复与 story 发送都带位）。反向违例照投且 `isStory`
  键不在（零 stderr 纪律，位面如实不发明）。
- **storyMessage 投影**（信封级 `storyMessage` 键，story 不落普通会话流，
  connector 据此走 story 时间线）：`textAttachment{text, textStyle?,
  preview{image 指针, url?, title?}}` / `fileAttachment` → 附件指针投影
  （locator 面同款，connector §4.54 媒体边界：元数据 only 字节永不下载）；
  `allowsReplies` 恒投（bool 缺席=线上缺省 false 如实）；`profileKey`/
  `group` 群 story 母钥 base64（有值才投）。textStyle 缺席/未知枚举值不投
  键；DEFAULT=0 线上在场照投词（与发送面缺省省略的不对称是线上形状的
  如实两面）。preview.url/preview.title 引擎扩展键往返面（2026-10-09 批，
  发送面 previewUrl/previewTitle 对端回声）：Some 且非空才投字符串键，与
  image 同一 preview 对象并列；None/空串省键（缺席键不在）；preview 面全
  空 = preview 键不在。无 attachment 位 = 官方发送面不可能产出的
  malformed 行，整信封丢弃（宁缺勿错）。群 story（group 母钥位）接收面
  照投——发送面 v1 不做的不对称是既定边界（收得到、发不出）。
- **storyReply 投影**（dataMessage 级 `storyReply` 键）：
  `storyContext → {authorAci, sentTimestamp}`；author binary 16B UUID 优先、
  string 兜底（mentions 同款二进制优先纪律），两者皆缺/不可解析 = 整键
  缺席（宁缺勿错，官方 storyReply 面无 author 不可路由）。1:1 回复与群
  回复同形状（群回复另带既有 groupInfo 投影）。钉定 proto `Quote.Type`
  无 STORY_REPLY 枚举值（回复的 quote i32=3 prost 原样保留），投影零依赖
  该枚举。
- **不落库面**：presage 官方收侧已跳过 StoryMessage 存库（线程/历史无
  story 行）；storyReply 是普通 DataMessage 照常落库（官方同构——回复
  本身是 DM/群消息）。
- **偏差登记**：①「stories 功能禁用即丢弃」官方腿不设（引擎无账号设置
  读面，闸归 connector 宿主侧）；②envelope 级 `story` 原始 bool 不透出
  （以 `isStory: true` 在场性表达，缺席键=无位——proto 只投影不发明）；
  ③storyMessage 的 textAttachment.preview 原「只投 image 指针」已在
  2026-10-09 批扩为 url/title 同投（发送面扩展键往返面，Some 且非空才
  投——description/date 官方 story 预览面仍未消费，缺席键不在）。
- **边界（本批红线）**：24h 过期纯客户端语义不做（story 时间线过期归
  connector/宿主，引擎只投影不计时）；不做 story 删除（remoteDelete story
  形态）/已读回执/查看回执面；引擎零 stderr（官方两条 story warn 路径
  只保留判面行为，无日志输出）。

transfer 历史回填非文本扩面·第一类型 sticker（2026-10-08，B1 批；
消费方 connector 契约 1.76 §4.38 扩面）：

- **投影面（`history.rs::sticker_record`，message 行 `sticker` 键）**：
  `chat_item::Item::StickerMessage` 由 v1 整 item 跳过改为完整投影——
  `packId` ← Sticker.pack_id 16B 小写 hex（官方 `Bytes.toHex` 同形）、
  `packKey` ← pack_key 32B 标准 base64（官方 `Bytes.toBase64` 同形）、
  `stickerId` ← sticker_id（proto3 标量原样，0 透出）、`emoji`（Some 且
  非空才投，caption/blurHash 同款纪律）、`data` ← Sticker.data FilePointer
  经 `file_pointer_record` 附件指针公共面（locator 三件套同款面，字节
  不下载——附件媒体字节 v2 消费方口径不变）。官方行为对位（AGPL 只读
  参照零拷贝）：Signal-Desktop `import.preload.ts:2749-2789`——
  stickerMessage 要求 sticker 在场；packId/packKey 字节长 strictAssert
  恰 16/32（`Stickers.preload.ts:108-109`）；emoji/data 官方可缺席
  （emoji→undefined、`data ? convertFilePointerToAttachment(data) :
  undefined`）。行形状与 live 接收投影 `project_data_message` 的
  `sticker` 面同构（packId hex/packKey base64/stickerId/emoji?/data?），
  connector 侧 1.35 NormalizedSticker 身份闸 + §4.38
  import_attachments 描述符/locator 面直接消费。
- **身份畸形 = 整 item 跳过**：sticker 未设、packId ≠16B、packKey ≠32B
  → 返回 None 计入 skippedItems（官方 strictAssert 的 KT 落法——绝不投
  半个贴纸；导入永不致命、计数可见）。与 live 接收投影的差别（live 对
  身份长度不设闸、connector 侧 1.35 闸兜底）是既定分工：导出面一次性
  消费、上游闸前移可省 connector 侧 skip 噪音；emoji/data 缺席不跳
  （官方可缺席语义原样）。
- **边界（本批红线）**：贴纸字节永不下载（metadata-only 红线）；不做
  StickerPack 帧（贴纸包安装态是 Storage 面另行立项）；reactions 键不投
  （backup5 Reaction 投影面 v1 未立项，与 StandardMessage.reactions 同批
  缺位，如实记档不发明）。

transfer 历史回填非文本扩面·第二类型 contact（2026-10-08，B1b 批；
消费方 connector 契约 1.78 §4.38 扩面）：

- **投影面（`history.rs::contact_record`，message 行 `contact` 键）**：
  `chat_item::Item::ContactMessage` 由 v1 整 item 跳过改为完整投影——
  `name` 子字段面 givenName/familyName/prefix/suffix/middleName/nickname
  （空串子字段键不在，官方 `name.given || undefined` 同口径）、
  `number[]`/`email[]` 条目面 `{value, type, label?}`（value 空的整条丢弃、
  label 非空才投，官方 `!value → undefined` + `filter(isNotNil)` 同口径；
  type 词表 HOME/MOBILE/WORK/CUSTOM，UNKNOWN/缺省/词表外一律归 HOME，
  官方 `phoneToContactFormType`:191-214 / `emailToContactFormType`:216-240
  同口径）、`organization`（空串键不在）、`avatar` ← ContactAttachment.avatar
  FilePointer 经 `file_pointer_record` 附件指针公共面（locator 同款面，
  字节不下载）。官方行为对位（AGPL 只读参照零拷贝）：Signal-Desktop
  `import.preload.ts:2620-2705`——contactMessage strictAssert 要求 contact
  在场，逐面映射为 ContactForm 形状。
- **跳过纪律**：contact 未设 = 整 item 跳过（官方 strictAssert 的 KT 落法，
  计数可见不致命）；全零信息量 face（无任何可渲染字段）= 整 item 跳过
  （空卡片即占位，KT 永不投占位）。行形状无 live 接收对应面（引擎
  live 接收 dataMessage 无 contact 投影），消费方 connector 契约 1.78
  以 rich 附加通道落库 + avatar 并入描述符列表。
- **边界（本批红线）**：地址面（ContactAttachment.address PostalAddress
  repeated）本批不投——任务面=姓名/电话/邮箱字段指针面，如实记档 gap
  （桌面卡片 v1 不渲染地址，后续批按需扩）；avatar 字节永不下载
  （metadata-only 红线）；reactions 键不投（backup5 Reaction 投影面 v1
  未立项，与 StandardMessage.reactions 同批缺位，如实记档不发明）；
  ExportStats 增 `contacts` 细分计数（行落地才计）。

transfer 历史回填非文本扩面·第三类型 groupUpdate（2026-10-08，B1b 批；
消费方 connector 契约 1.79 §4.38 扩面）：

- **投影面（`history.rs::group_update_record` + `group_change_detail`，
  message 行 `groupUpdate` 键）**：`chat_item::Item::UpdateMessage` 的
  `GroupChange` 批由只投批内计时器改为逐条 detail 全词表投影——
  summary/create/title/avatar/description/access-members/access-member-label/
  access-attributes/announcements-only/member-privilege/member-remove/
  member-add/member-add-from-invite/member-add-from-link/
  member-add-from-admin-approval/pending-add-one/pending-add-many/
  pending-remove-one/pending-remove-many/admin-approval-add-one/
  admin-approval-remove-one/admin-approval-bounce/group-link-reset/
  group-link-add/group-link-remove/access-invite-link/terminated。官方行为
  对位（AGPL 只读参照零拷贝）：Signal-Desktop
  `import.preload.ts:3190-3764` `#fromGroupUpdateMessage` + 
  `parseGroupV2AccessLevel`:4608。键形逐臂对齐官方：`newTitle`/`removed`/
  `description`/`newPrivilege`（access 族）/`announcementsOnly`/`aci`/
  `serviceId`/`inviter`/`count`/`times`/`privilege`（group-link-add 专用，
  官方同形异键）。
- **updaterAci 随身投法**：官方逐条读 updaterAci 覆盖行级 `from` 变量，
  KT 按条随身投出（proto 带 updater_aci 字段的臂，16B 小写 UUID 形，畸形
  省略键），行级 actor 归消费方裁定；官方 from=self 的合成面
  （selfInvitedOtherUser）不发明键。
- **跳过纪律**：身份必需的 detail（官方 strictAssert/throw 面：member
  aci/removedAci/inviteeServiceId/newMemberAci/requestorAci 等）身份残缺 =
  该条 detail 丢弃（KT 永不致命），批内其余照投；invitation-revoked 名单
  空 = 条目丢弃（官方 throw 的 KT 落法）；一行零 detail 且零计时器面 =
  整 item 跳过计入 skippedItems。update 未设的条目按官方 proto 注释投
  summary 无 updaterAci。
- **migration 族归并**：GroupV2Migration{,SelfInvited,InvitedMembers,
  DroppedMembers}Update 四变体官方不落 detail（独立 group-v1-migration
  消息面），KT 归并为单条 `v1-migration` detail 追加批尾
  `{areWeInvited, invitedMemberCount, droppedMemberCount}`（官方
  `getDefaultMigrationMessage` 同面：缺省 false/0/0，逐位叠加）——KT
  face 命名与官方消息 type 字符串（group-v1-migration）不同，如实记档。
- **边界（本批红线）**：GroupExpirationTimerUpdate 不入 details（骑既有
  `expirationTimerChange` 面，同批合行双键）；官方 openApproval/bounce
  同名合并（isApprovalPending=true 的渲染侧 refine）KT 不做（消费方不
  消费该位，如实记档）；details 上限 32 条（真实变更批 1-5 条，超限属
  敌意行域，截断不致命）；ExportStats 增 `group_updates` 细分计数（行
  落地才计）。detail 词表全覆盖（contact 批的地址面同级 gap 在本类型
  不存在——proto 变体 36 个全部有落点或明确记档）。消费方 connector
  契约 1.79：presence + 首个 title detail 的 newTitle 作 group_name
  镜像，与 live 同形 system 通知行落库（同行双 face 拆两行，官方
  additionalMessages 语义）。

transfer 历史回填非文本扩面·递补批（2026-10-08，B1 递补批，历史回填
导出/导入/渲染三仓并行；消费方 connector 契约 §4.38 后续批扩面消费，
行键名与官方 import.preload.ts 消费面同语义）：

- **帧级 AdHocCall 通话记录行（`history.rs::adhoc_call_record`，新行型
  `{"type": "adhocCall", callId, recipientId, state, callTimestamp}`）**：
  `Frame.item` 第 6 臂由整帧落 skipped_frames 改为投影行——官方桌面导入
  会恢复通话记录行（`import.preload.ts:601-603` 帧分派 + `:3926-3973`
  `#fromAdHocCall`）。`callTimestamp` strictAssert 必须有效（0 = 官方
  throw 的 KT 落法：帧级跳过，`skipped_frames` 同桶计数，永不致命）；
  `callId` 0（legacy 无 id）官方本地生成 UUID 兜底、`recipientId` →
  CallLink recipient 解析（找不到官方 warn 丢行）均归消费方裁定，KT
  原样透出 proto3 标量；`state` 词表逐臂对位官方 parse face 输出
  （`:4439-4458` `fromAdHocCallStateProto` + `AdhocCallStatus` 字符串值，
  `ts/types/CallDisposition.std.ts:74,80`）：GENERIC → "GenericAdhocCall"、
  UNKNOWN_STATE/词表外 → "Unknown"。ExportStats 增 `calls` 细分计数
  （行落地才计）；快照导入 `lines` 统计同步计入 calls 行。官方
  callHistory 行的 mode/type/direction/peerId/ringerId 等常量位与解析
  面不入投影（消费方落库同语义）。
- **ChatItem 级六型补齐**（ChatItem.item oneof 全臂投影闭环，message 行
  新增 face 键，ExportStats 不再加细分、行计入 `messages`）：
  - `payment`（`payment_record`，`:2788-2806`）：`{amountMob?, feeMob?,
    note?, transactionDetailsBase64?}`——三个 optional string 官方
    dropNull/`?? null` 同口径（Some 才投，显式空串是档案陈述照投）；
    TransactionDetails 官方整体重编码 base64（`Bytes.toBase64(encode(td))`
    同形），KT 以同 schema prost 重编码，消费方按官方语义存 opaque 串；
    官方 `kind: Notification` 常量位不入 face。全零信息量 face = 整 item
    跳过（空卡片纪律）。移动支付（MobileCoin）已退场，行投影仍保真。
  - `giftBadge`（`gift_badge_record`，`:2809-2861`）：FAILED 短路只投
    `state: "Failed"`（presentation 在场也不投，官方提前 return 同形）；
    其余 state strictAssert presentation 非空（空 = 整 item 跳过）；state
    词表对位官方 `GiftBadgeStates`（`ts/types/GiftBadgeStates.std.ts:4-10`）：
    OPENED → "Opened"、REDEEMED → "Redeemed"、UNOPENED/词表外 →
    "Unopened"；presentation 标准 base64。官方对 presentation 的 zk 解析
    面（expiration/level）不入投影（引擎无该解析面，消费方后续批按需
    补，如实记档）。
  - `viewOnce` + `isErased` + `viewOnceInvalid`（`project_view_once_message`，
    `:2300-2331` + `filePointers.preload.ts:170-205`）：行键与 live 接收
    投影同形（`viewOnce` 顶层键 + `attachments` 单条数组，connector 一条
    消费通路）；attachment 缺（proto「Will be null for viewed messages」
    已查看形态）与 pointer 缺（官方转换 null → attachments 空）都投
    `isErased`（官方同键），非 invalid；媒体族校验按 live view-once 语义
    （2899e76，`crate::service::is_view_once_media` 单一真相源）违者补
    `viewOnceInvalid`——archive proto 无 body/quote 等其余校验腿，唯一
    可表达腿即媒体族（差异如实记档：官方导入路径不判 invalid，KT 与
    live 行对齐以便消费方单通路处理）。
  - `storyReply`（`story_reply_record`，`:2333-2372` + 分派闸
    `:1772-1794`）：reply oneof 未设与 directionless（官方两条
    strictAssert）= 整 item 跳过；textReply 臂 → `{body?, bodyAttachment?}`
    （body 非空才投、longText 指针公共面）、emoji 臂 → `{emoji}`（空串=
    官方 else-throw 落法跳过）；零信息量 textReply = 整 item 跳过；story
    作者解析（incoming → aboutMe.aci、outgoing → 会话 serviceId，官方
    `storyReplyContext.authorAci`）归消费方。
  - `poll`（`poll_record`，`:1795-1847`）：`{question?, options[]:
    {option, votes?: [{voterId, voteCount}]}, allowMultiple, hasEnded}`
    ——投票聚合（voterId → 会话解析、optionIndexes 归并、unknown voterId
    warn 丢弃）与 `terminatedAt`（`hasEnded ? dateSent : undefined`）归
    消费方；空 option 条目丢弃、空 question 键不在、全零信息量 = 整 item
    跳过；validator 面约束（libsignal `backup/chat/poll.rs`：question
    1..=200、option 1..=100、至少两 options、voter 必须 self|contact）是
    官方档案有效性判定，导出侧不复验。敌意行域截断不致命：options ≤ 32
    （真实 poll ≤ 10 项）、单 option 投票 ≤ 1001（官方 GroupsV2 群上限
    = 合法投票天花板）。
  - `adminDeleted`（message 行 face `{adminId}`，`:2714-2732`）：adminId
    原样透出（proto3 标量，0 照投）；官方 adminId → 会话 → serviceId
    ACI 解析（`deletedForEveryoneByAdminAci`）归消费方；行落 tombstone
    面由消费方路由，引擎不混既有 `remoteDeleted` 位。
- **NotificationProfile/ChatFolder 两 Frame 型刻意维持 skipped_frames**：
  官方导入消费它们（notificationProfile/chatFolder 分派臂），但桌面本机
  投影已自持（本机设置面，非消息面），NDJSON 消息流不投影，commit 如实
  记档。
- **边界（本批红线）**：reactions 键不投（viewOnce/poll/storyReply 三型
  proto 均带 reactions，官方导入 `#fromReactions` 消费——backup5 Reaction
  投影面 v1 未立项，与 StandardMessage.reactions 同批缺位，如实记档不
  发明）；payment/giftBadge 的字节面（MobileCoin blobs、presentation
  二进制）只随 opaque base64 中转，引擎任何持久层不留存；ExportStats
  只增 `calls` 字段不破坏既有（快照导入 `lines` 求和同步 +calls）。

call-link 凭据材料面（2026-10-09，KT 扩展，P4c 切一；消费方 =
kt-signal-calling relay-api-v2 §2/§5 帧族，引擎只产材料字节，relay 帧
组装/消费归 connector/calling 仓后续批）：

- **五方法面**：`calls.linkPresentation`（rootKey → 当天 CallLinkAuth
  CredentialPresentation 字节）、`calls.createLinkAuth`（rootKey+roomId →
  CreateCallLinkCredentialPresentation + CallLinkPublicParams 双字节）、
  `calls.groupProof`（群 master key → ExternalGroupCredential.token）、
  `calls.groupMembers`（群 master key → [{uuid, userIdCipherText}]）、
  `calls.endorsementsPublicKey`（常量派生 EndorsementPublicKey 字节）。
  凭据与 presentation 字节**只经内存转发，零日志、零落盘**（relay-api-v2
  §5 :95-97 纪律；引擎数据目录不落凭据材料文件）。
- **官方锚点逐链（Signal-Desktop v8.31.0-alpha.1 abe80d3 实读 + 28a0430c59
  轮换提交实读，AGPL 只读参照零拷贝）**：
  - **legacy/V1 族分叉（本批实测纠偏）**：官方 2026-09-10 提交 28a0430c59
    「Upgrade libsignal to 0.101.2 and request new zkc auth credentials」把
    `genericServerPublicParams`/`backupServerPublicParams` 轮换为 V1
    Standard 形（首字节 0x01），并在 `getGroupCredentials` 与
    `callLinkCreateAuth` 请求加 `v101=true` 触发服务器签发 V1 族凭据；
    本仓 zkgroup 钉 libsignal v0.99.0（439f035d，`GenericServerPublicParams`
    的 `ReservedByte = VersionByte<0>` 严格校验首字节，pin 树
    common/serialization.rs:98 + 上游自测 version_byte_wrong，V1 常量/
    凭据均不可解）。故引擎对位**轮换前 legacy 形状**：常量钉 V0 生产值
    （首字节 0x00，production.json 轮换前逐字节值，zkgroup_consts.rs 记
    档），两请求一律不带 `v101=true`，服务器对 legacy 客户端照旧签发
    legacy 族（旧客户端过渡窗内有效）。**附带修正前批死链**：已提交云备
    份面（cloud_backup.rs）前批嵌的是轮换后 V1 值（production.json 现值
    逐字节），在 v0.99.0 下常量反序列化必失败 = BackupAuth 兑换面潜在硬
    故障（该面真机回归未做，契约 §6.5 后续批）；本批常量提升共享模块时
    一并改挂 V0，云备份真机回归按 V0 常量执行。
  - 凭据拉取 = `GET v1/certificate/auth/group?redemptionStartSeconds=
    {秒}&redemptionEndSeconds={秒}&zkcCredential=true`
    （WebAPI.preload.ts 轮换前 :4542-4564 getGroupCredentials，chatService
    host 本机凭据），响应 `{pni?, credentials[], callLinkAuthCredentials[]}`
    （groupCredentialFetcher.preload.ts:205-209，条目 `{credential(b64),
    redemptionTime(秒)}`；callLinkAuthCredentials 字段经轮换提交
    background.preload.ts 清缓存 hunk（isBeforeVersion('v8.29.0-beta.1')）
    证实轮换前已存在，现树响应型别 = WebAPI.preload.ts:1035-1039，legacy
    族照发）；callLink 族兑换 = formatCallingCredential
    :259-276（`CallLinkAuthCredentialResponse.receive(ACI, redemptionTime
    秒, GenericServerPublicParams)` → serialize 按兑换日存）。
  - linkPresentation = ts/util/callLinks/zkgroup.preload.ts:16-49 同链：
    当日凭据 → `present(ACI, redemptionTime/SECOND, genericParams,
    CallLinkSecretParams.deriveFromRootKey(rootKey.bytes))`；官方把未序列化
    对象交 RingRTC 绑定层，引擎侧 `zkgroup::serialize` 字节化出 RPC。
  - createLinkAuth = calling.preload.ts:860-940 createCallLink 凭据段：
    `CreateCallLinkCredentialRequestContext.forRoomId(roomId)` →
    `POST v1/call-link/create-auth`（WebAPI.preload.ts 轮换前 :3439-3453
    无 urlParameters，jsonData `{createCallLinkCredentialRequest: b64}`，
    响应 `{credential: string}` schema :1471-1473）→ `context.receive(
    response, ACI, genericParams)` → `present(roomId, ACI, genericParams,
    secretParams)` + `secretParams.getPublicParams()` 各自 serialize。
  - groupProof = groups.preload.ts:1782-1806 fetchMembershipProof →
    WebAPI getExternalGroupCredential :4566-4586：`GET v2/groups/token`
    （storageService host，basic auth = base64(`hex(groupPublicParams)` +
    ":" + `hex(presentation)`)，generateGroupAuth :4533-4540 同构——与
    vendor push_service HttpAuth 的 basic_auth 逐字同形），响应 bytes →
    `ExternalGroupCredential{ token = 1 }`（Groups.proto:285-287，复用
    presage::proto 生成面，不新嵌 proto）；RingRTC 侧
    `setMembershipProof(Bytes.fromString(proof))` = token 字符串 UTF-8
    字节（calling.preload.ts:1495 区域）。
  - groupMembers = groups.preload.ts:7611-7632 getMembershipList 同语义：
    群成员逐一 `encryptServiceId(clientZkGroupCipher, aci)` →
    `{aci, uuidCiphertext}`；引擎侧源 = `GET v1/groups/` + decrypt_group
    解密群态成员（fetch_group_state 既有面），`GroupSecretParams::
    encrypt_service_id`（zkgroup group_params.rs:105）出 UuidCiphertext；
    RingRTC GroupMemberInfo = `{userId: 16B UUID bytes,
    userIdCipherText: bytes}`（ringrtc 2.72.1 dist Service.d.ts:511-515），
    relay 帧 `{uuid, userIdCipherText}` 的 uuid = ACI 字符串。
  - endorsementsPublicKey = legacy ServerPublicParams（生产常量，
    `SignalServers::Production` 配置派生）`get_endorsement_public_key()`
    （zkgroup pin 树 api/server_params.rs:347；官方 JS
    `serverPublicParams.getEndorsementPublicKey()` 同位，消费位
    calling.preload.ts:1134/:2018 = RingRTC getCallLinkCall 的
    endorsementPublicKey 槽）。
- **zkgroup pin 树对位**（libsignal tag v0.99.0 439f035d 实读）：
  `call_links::CallLinkAuthCredentialResponse::receive(ACI, Timestamp,
  &GenericServerPublicParams)`（内含日界对齐闸）→ `present(ACI, 兑换日,
  generic, &CallLinkSecretParams, randomness)`；`CreateCallLinkCredential
  RequestContext::new(room_id, randomness)` → `get_request()` →
  `receive(response, ACI, generic)`（同含日界闸）→ `present(room_id, ACI,
  generic, secret_params, randomness)`；序列化一律 `zkgroup::serialize`
  （bincode）；`RandomnessBytes = [u8; 32]`（RANDOMNESS_LEN=32）每调用
  新随机。
- **凭据缓存节奏对齐官方**：进程内存 per-account（ACI 键控）双族缓存——
  legacy AuthCredentialWithPni 与 callLink CallLinkAuthCredential 各自按
  兑换日秒 → 兑换后序列化字节存储（官方 itemStorage 同语义，ms/秒单位差
  在引擎侧统一为秒）；窗口算法逐字对齐官方 getDatesForRequest
  （groupCredentialFetcher.preload.ts:336-362）：缺今日或末条 <今日 →
  拉 {今日, 今日+6d}；末条 ≥今日+6d → 不拉；否则增量拉 {末条+1d,
  今日+6d}；**双族各算窗取 min(start)/max(end) 合并**（
  maybeFetchNewCredentials :175-196：requestDates/requestDatesCallLinks
  两窗并集，任一族缺料即拉）；合并时清 <今日 条目（:285-301，prev 过滤
  ≥today + new 拼接不去重，官方同形）。**官方 4h 主动定时器
  （:77 scheduleAnother）不移植**——引擎无后台调度面，每次材料调用先跑
  同一 maybeFetch 逻辑，使用时点新鲜度与官方等价（官方
  fetchMembershipProof 前亦调 maybeFetchNewCredentials，:1789），偏差
  如实记档。缓存不落盘不跨重启。
- **群凭据旧面改造（groups.rs group_authorization）**：端点参数拓宽为
  官方 legacy 族形状 `zkcCredential=true` 一次拉双族并入缓存（`v101=true`
  为 libsignal ≥0.101 Standard 族签发开关，钉 v0.99.0 不移植），当日 legacy
  条目由缓存供给（不再每次现取）；**窗口偏差修正**——旧实现请求窗
  today→+7d（8 天）改对齐官方 today→+6d（7 天），行为修正如实记档。
- **参数闸（fail-closed，-32603）**：rootKey = 标准 base64 解码后恰
  16B（V0）或 21B（V1）——RingRTC v2.72.1 `CallLinkRootKey` 两版本定长
  （root_key.rs：V0::LEN=CRN_LEN=16、V1::LEN=16+1+4=21），zkgroup
  `derive_from_root_key` 只取前 16B CRN（ROOT_KEY_MAX_BYTES_FOR_SHO=16），
  长度不符即拒不静默截断；roomId = 标准 base64 解码后恰 32B——
  `derive_room_id` = HKDF-SHA256（salt=None，info
  "20230501-Signal-CallLinkRootKey-RoomId"）固定 32B 输出（root_key.rs），
  zkgroup 侧无长度闸（`CreateCallLinkCredentialRequestContext::new` 收任意
  slice），引擎按官方派生定长收口；groupId 沿用 32B master key 闸。
- **错误码（新族两枚，connector 白名单登记归 connector 批）**：参数形状/
  未注册账号 `-32603`；`CALL_CREDENTIALS_FAILED`（`-1` + message +
  data.status? = 服务器 HTTP 状态码诚实透传：服务器拒绝、zkgroup 兑换/
  呈现验证失败、响应形状违例、当日凭据缺席同族，retryable=false 语义）；
  `CALL_CREDENTIALS_UNAVAILABLE`（`-1` + message，网络/传输面，
  retryable=true 语义）。
- **fake 纪律**：参数闸照常 fail-closed；四方法答形状桩（固定占位字节/
  固定 proof 串/空 members——桩字节**非有效 zk 材料**，仅钉契约形状），
  不触网不落调度日志（材料供给面非协议上行）；`calls.endorsementsPublicKey`
  常量派生不触网，fake/real 同码路真实答。
- **边界（本批红线）**：不做群通话连接/SFU/媒体面（RingRTC/Calling 职责）；
  rootKey 生成、roomId 派生、adminPasskey 生成皆 RingRTC 侧职责（
  `CallLinkRootKey.generate()/derive_room_id()/generate_admin_passkey()`），
  引擎只收成品字节；relay 帧组装与消费归 kt-signal-connector /
  kt-signal-calling 仓后续批；群通话状态同步（storage 面）不做（relay
  §6 诚实边界）；create-auth 的 POST 属凭据签发面（无用户可见状态变更），
  不按协议上行面落调度日志。

### 引擎协议能力自证面（engine.protocolCapabilities，2026-10-10 批）

Signal 服务器按客户端上报的 capability 做强制过滤（2026-03 曾无预告批量
注销 capability 不达标的客户端，signal-cli#1993）。本批给引擎加自证面：
运维/哨兵随时可查引擎当前向服务器声明的协议能力与依赖基线，上游下一轮
强制收紧时快速核对达标面。方法行为见 §3 表行，本块登记事实源结构与
防漂移闸。

- **两处声明，两个真相源策略**（引擎以 linked device 身份入网；账号属性
  面位值真相源 = 主注册链 confirmation.rs 构造点——该腿不在引擎链接
  路径上，字面钉定防漂移；B4 起接收管线停发 AccountAttributes PUT，
  引擎链接设备不再有账号属性推送面）：
  - 账号属性面 `DeviceCapabilities` 八位（D5 补齐后全集：六位基线
    storage/transfer/attachment_backfill/spqr/profiles_v2/
    username_change_sync_message + D5 两位 versioned_expiration_timer/
    optional_phone_number，位值裁定见下「协议能力补齐」块）：位值真相
    源 = vendored presage 主注册链 confirmation.rs 构造点（设备注册协议
    必需腿）。B4 起接收管线停发 AccountAttributes PUT（官方链接设备
    从不 PUT attributes；vendored registered.rs `set_account_attributes`
    构造点已删除，管线不再推送——本文初版「管线首步推送」表述随之
    失效）。该构造是 vendor 内联字面值、无导出常量可调用，引擎持**钉定
    镜像**（`service::capabilities::DEVICE_CAPABILITIES`，同型 vendor
    结构体 const 构造——vendor 增删字段时引擎编译期即断）。
  - 链接面 `LinkCapabilities` 三位：`link_device` 构造
    `LinkRequest.account_attributes.capabilities` 取
    `LinkCapabilities::default()`（vendored libsignal-service
    provisioning/mod.rs 构造点 + push_service/linking.rs Default 实现）。
    构造器 pub 可调用，引擎运行时 **live 复用**，零镜像。
- **反双真相源闸（四层）**：① 编译期——镜像位值 const 块钉死（位值漂移
  直接断编译），且镜像以 vendor 结构体显式全字段构造（vendor 增删字段
  同样编译期断）；② 单测扫 vendor 源码——
  `service::capabilities` 模块单测对 confirmation.rs 构造点（B4 起唯一
  剩余账号属性构造点）与 linking.rs Default 实现做空白归一字面断言（逐
  位值 + provisioning 构造点确用 `LinkCapabilities::default()`），vendor
  升级改位/改点即红灯，强制人工复核后同步镜像；③ 单测 live 对账——镜像与
  `LinkCapabilities::default()`/`DeviceCapabilities::default()` 运行时
  逐位比对（含两 Default 不得混用的 storage 位反证）；④ 基线锚钉定——
  单测硬编码钉死 presage rev/libsignal tag+commit，依赖升级必须撞闸
  重走能力位复核。
- **依赖基线**：build.rs 从本仓 Cargo.toml（presage 声明 rev——[patch]
  路径替换后 Cargo.lock 的 source 行不再回显 git rev，声明行才是上游钉
  定锚）与 Cargo.lock（`libsignal-protocol` 的 git source 行——解析后
  事实，tag + checkout commit 双值）提取，`cargo:rustc-env` 注入
  `KT_ENGINE_*` 常量，`env!` 编译期内嵌；运行时零网络零文件读取。锚缺失
  build.rs panic（fail-closed：自证面宁编译失败不报空）。
- **应答形状即 wire 形状**：能力位 JSON 由 vendor 结构体按其 serde 属性
  序列化产出（camelCase；`profiles_v2` 上游字面 snake，消费方不得
  "纠正"）——自证面输出与引擎线上声明逐键同形，不另立投影词表。
- **错误面**：无。只读常量应答，无参数闸（params 忽略）、无账号解析、
  不触网；唯一可能失败面 = JSON-RPC 协议层（§2）。
- **fake/real 同码路真实答**：常量派生面不进服务层（calls.endorsements
  PublicKey 同款），fake 开关不影响应答；读面不落调度日志零 stderr。
- **边界**：本方法只自证引擎声明面，不做服务器侧回读（whoami/profile
  capabilities 回显位是另一事实，不在本面）；引擎位值随 vendor 升级
  走闸同步，不提供运行时可写面（能力位是协议安全决策，ADR 0002 边界）。

### 群发送语义三件（2026-10-11 B 批，官方对齐 P8）

send 群形态三件语义对齐（官方 per-recipient 结果面/发送前刷态/
announcements-only 客户端闸；群态完全取不到时维持 UnknownGroup 既有失败，
闸 fail-open 不新增失败面）：

- **发送前刷群态**：DataMessage 群发送前强取服务器群态（成员名单不再吃
  本地陈旧态——新成员可达、被移出成员不再收信）；typing 免刷（官方 typing
  不带群态取用同语义，高频面零新增服务器负载）。取态失败回落本地已存
  群态继续发送（fail-open），本地也没有维持 `UnknownGroup`。
- **per-recipient 结果向量（wire additive）**：send 群形态 `results` 数组
  由恒 `[]` 改为逐成员填充（条目形状与直聊同构）——成功
  `{recipientNumber, success:{timestamp}}`、失败
  `{recipientNumber, failure:{type}}`；type 词表四值 additive：
  `UnregisteredFailure`（NotFound，成员已注销）、`UntrustedIdentityFailure`
  （身份钥不匹配）、`AuthorizationFailedFailure`（401 凭据失效，
  SendError::Unbound 同因）、`NetworkFailure`（网络/协议族折叠）；
  recipientNumber 经 contacts 映射，未命中回退 ACI uuid 串（listGroups
  成员面同纪律）。单个成员失败（含未注册）不再整体炸批，成功成员照常
  送达，transcript（含群首条消息）无论成败一律落库——群黑洞消除。
- **announcements-only 闸**：群态 `announcementsOnly=true` 且发送方无
  ADMINISTRATOR 角色时，群发送系（send/remoteDelete/reaction/typing/pin/
  callMessage 群腿）整拒新错误码 `SEND_TARGET_ANNOUNCEMENTS_ONLY`
  （`-1` + message，与 SEND_TARGET_TERMINATED 同形状）。官方客户端侧闸
  同语义（服务器不拦非 admin 投递）；群态取不到时不判闸（fail-open）。
- **成员面 fail-open**：单成员 profile 行不可读降级 identified 通道发送，
  不再炸整批（identified 回退保险丝同向）。
- **边界**：send 群形态以外的方法 results 面不变（仍 `{}` 无 results 键）；
  fake 模式群发送仍早退空 results；wire 仅 additive（results 填充 + 新
  错误码），既有键与错误码零改动。

### 身份事件面 + Unbound 停环（2026-10-11 B 批，官方对齐 P8）

B 批身份/凭据两事件面，全部 wire additive：既有方法与字段零改动，
connector 白名单投影不消费则零影响。

- **身份钥变更事件**：vendored presage 协议层在会话处理中替换既有
  identity（`save_identity` ReplacedExisting）时记入 store 变更日志
  （`Store::take_identity_changes`，trait 默认空实现，SqliteStore 覆盖）；
  接收流逐信封 drain，命中当前发送方 → `Received::IdentityChange(sender,
  content, ack)` 上抛引擎。本体消息解密成功照常投影，单次 ack（Content
  同契约）。引擎事件面：`{method: "receive", params: {account, envelope:
  {sourceUuid, timestamp, identityChanged: {}}}}`（解密失败 decryptionError
  同款加面纪律）。信任判定收向不动（OnNewIdentity::Trust 仅 Receiving 生
  效，收向照常换钥上事件）——事件为投影 + 拒发闸联动，强制闸已于
  2026-10-11 P9 身份拒发批闭合（见下方「P9 身份安全闸」块）。
- **verified 同步下行**：主设备 verify/clear 后其他链接设备收敛（官方
  桌面 consumeVerifiedSync 同语义）——sidecar 权威突变 + receive 事件
  `envelope.syncMessage.verified{destinationUuid, destinationNumber?,
  identityKey?, state}`（详见 §5 安全码引擎面块 receive 侧条目）。
- **Unbound 停环**：接收流建流遇 `ServiceError::Unauthorized`（401/403
  同归型，send 链凭据失效归一批同型）→ 接收循环停止（官方链接设备语义：
  凭据失效即停等重链，不无限退避重连），上抛一次
  `{method: "accountUnbound", params: {account}}`。恢复路径 = 重链
  （finishLink/deleteLocalAccountData 生命周期），无进程内自愈——服务端
  凭据不可原址复活。send 链 `SendError::Unbound` 归型不变。
- **AccountAttributes PUT 停发（vendored）**：接收流 refresh task 不再
  PUT account attributes（上游 presage 每次开流硬编码
  `discoverable_by_phone_number: true` 回写——A⑦隐私回写根因，服务器端
  primitive 缺省 true 下缺字段≠不改，Option None 无效，A 批⑦证伪改道的
  彻底修法）；pre-keys 注册腿保留。主设备注册路径（presage
  confirmation.rs 的 register_account PUT）不动——协议必需且引擎账号全为
  链接设备。官方 Signal-Desktop 链接设备从不 PUT attributes，同语义。

### 发送链凭据失效归一（SendError::Unbound，2026-10-10 批）

Signal 服务端对已吊销/失效凭据的账号返回 HTTP 401（纯文本 body），vendored
libsignal-service 归型 `ServiceError::Unauthorized`（push_service/
response.rs：401/403 同归型）。此前 send 链把 presage 错误一律折叠
`SendError::Internal`（-32603 泛化 internal_error），connector 解绑分类器
（按 error.message 含 AuthorizationFailedException / Authorization failed
识别）永远不命中，用户端只见「发送失败」无凭据失效徽标。本批闭合该缺口。

- **归型单点（`service::send_error_of`）**：send/上行各触网点（直聊
  send、remoteDelete、sendReaction、sendReceipt 族、sendTyping、
  sendPinMessage、sendViewOnceOpen、sendMessageRequestResponse、
  sendCallMessage 两腿、群组发送面、setExpirationTimer、附件/贴纸上行、
  sendStory 分发）的 presage 错误统一归型——`ServiceError::Unauthorized`
  的两层归型点都覆盖：直接 `presage::Error::ServiceError` 面（whoami 等
  推送服务调用，getUserStatus 同面）与 `presage::Error::
  MessageSenderError` 内 sender 同源再包一层（`send_message` 上行主路），
  都归 `SendError::Unbound`；其余（网络/协议/存储）维持 Internal 折叠。
  本地 store 面（contacts/groups 读写）不走该归型——无网络面不可能 401。
- **确定性失败不重试**：401 是服务端确定性拒绝，标失败即返回（与
  unknown-send 不自动重试纪律同向），不做退避/跨调用重试。
- **错误面**：`SendError::Unbound` → `-32603` + getUserStatus 同款解绑
  原文（§3 头同一常量，单一真相源，不发明新字符串）——connector 分类器
  按 message 形状直接命中，零 connector 改动；getUserStatus 面不变。
- **不误报边界**：404（`NotFoundError`，收件人未注册族，官方 response.rs
  同位注释）与 sender 其他失败不解绑——只有 Unauthorized 归型才报解绑态
  （getUserStatus「仅 401 判解绑」同纪律）。
- **单测**：归型两面包 + 反例（404/NoMessagesToSend/UnknownGroup 不误
  报）钉 `send_error_of`；sendError 解绑臂形状（-32603 + 原文 + 分类器
  锚词 AuthorizationFailedException/Authorization failed）钉 methods 面。

### 发送链错误码结构化三分类（RateLimited/ProofRequired/ServerUnavailable，2026-10-11 批，官方对齐 P8）

凭据失效归一批后，send 链其余服务端失败仍全折叠 `SendError::Internal`
（-32603 泛化面，细节不外泄），connector/desktop 无法区分限流与故障。
本批从发送链结构化出三类（wire additive：既有错误面与常量零改动，
`USER_UNBOUND_MESSAGE` 不动）。

- **归型（`service::send_error_of` 单点，两触点共用）**：ServiceError
  面抽单点分类器（直接推送服务调用与 sender 再包层两触点同路）——
  429/413 → `ServiceError::RateLimitExceeded`（vendored
  push_service/response.rs 同归型臂）→ `RateLimited`；428 →
  `ServiceError::ProofRequiredError` 直接臂 + sender 侧
  `MessageSenderError::ProofRequired` 再包层臂（vendored sender.rs 把
  428 归型后重包，token/options 归型即弃不上 wire）→ `ProofRequired`；
  5xx → `ServiceError::UnhandledResponseCode` 兜底臂 status≥500 →
  `ServerUnavailable`（vendored 无独立 ServerError 变体，5xx 全落兜底
  臂）；401/403 → `Unbound` 归型不变；<500 未处理码（400/404 等）维持
  Internal 折叠不误报不可用。
- **错误面（methods.rs 三常量，wire 字面量 owner 侧定死，一字不差）**：
  `RATE_LIMITED_MESSAGE` = "Failed to send message: Rate limited.
  (RateLimitExceeded)"、`PROOF_REQUIRED_MESSAGE` = "Failed to send
  message: Proof required. (ProofRequiredException)"、
  `SERVER_UNAVAILABLE_MESSAGE` = "Failed to send message: Signal server
  unavailable. (ServerUnavailableException)"。JSON-RPC code 一律
  `-32603`（同 USER_UNBOUND 面风格），括注异常类名锚词供 connector
  分类器命中（解绑面同款零 connector 改动纪律）；sendError 三臂
  methods 面加臂，`UnknownAccount`/`Internal` 折叠面不变。
- **可重试语义建议（重试决策在 connector/desktop，引擎侧维持不自动
  重试）**：`ServerUnavailable`（5xx，服务端瞬时故障）建议可重试；
  `RateLimited`/`ProofRequired` 不自动重试——限流需按节奏退避、验证
  需人工完成 captcha，引擎均不上报材料（retry-after/token 不出归型
  面），调用方按错误面自行决策。
- **覆盖面**：`send_error` 归型为 send 系全方法共享（§3「同 send」族
  行自动继承，含直聊/群组/远删/reaction/typing/pin/回执族/callMessage/
  sendMessageRequestResponse/sendStory）；群发送 per-recipient 结果
  向量词表不动（`group_send_failure_word` 折叠面独立，connector 消费
  词表零变化）。
- **单测**：归型（限流两触点、验证 428 两臂含 token/options 弃面、
  5xx 两态 + <500 反例，既有 Unbound 反例不回退）钉 `send_error_of`；
  sendError 三臂 wire 形状（-32603 + 常量逐字 + 锚词
  RateLimitExceeded/ProofRequiredException/ServerUnavailableException
  在场）钉 methods 面。

### 收信 ack 后移 + 投影去重 + 解密失败上抛（A 止血批，2026-10-11）

对齐官方收信契约（Signal-Android `IncomingMessageObserver.kt` 落库事务
提交后逐条 ack、`SignalWebSocket.kt`「处理成功后由你 ack」）：此前
vendored messagepipe 在信封 yield 即回 200，engine 解密失败静默丢弃，
服务器重复投递会重复投影 + 重复通知 connector。本批改三面，JSON-RPC
既有方法与字段零改动（仅新增事件面）。

- **ack 时序**：信封 ack 句柄（vendored `EnvelopeAck`）自 messagepipe
  穿 presage unfold 到引擎；引擎在解密 + presage 落库 + 投影/通知完成
  后 ack。成功/失败两条路都必达 ack（防服务器队列堆积）；句柄被丢弃
  （进程崩溃）= 不应答，服务器按未确认重投，与官方崩溃语义一致。
  queue-empty 标记非消息，仍在 messagepipe 层就地 ack。
- **投影去重**：presage unfold 在落库前查 store 既有行
  （`thread_for_sender_and_timestamp(sender, client_timestamp)`，即
  thread_messages 落库主键面）；命中 = 重复投递 → `Received::Duplicate`
  （仅 ack，不再投影/不再通知）。判重键 = (sender, client_ts)，与
  thread_messages INSERT OR REPLACE 行同源；typing/story 等不落库的
  瞬态体不去重（重复通知无害瞬态面）。serverGuid 不作为判重键：presage
  落库不持久化 server_guid（content.rs 解构丢弃），无账本可查，穿透
  无从判重（如实现见本登记为准）。
- **解密失败事件面**：`Received::DecryptionError` 不再静默——引擎 ack
  （官方解密失败同样 ack+记录）并上抛加面事件：
  `{method: "receive", params: {account, envelope: {sourceUuid,
  decryptionError: {}, timestamp?}}}`——timestamp 仅在已知时出现（DERM
  路径带发送方 client ts，sealed sender 失败路径无）。DERM（解密失败
  回发）属 B 批不做；事件为加面，连接器白名单投影不消费则零影响。
- **边界**：不动发送面；不做 DERM 回发；contacts 载荷不去重（presage
  幂等落库）；QueueEmpty 行为不变。

### 自动 delivery receipt（D 批 D1，2026-10-11，官方对齐）

官方桌面收到 sealed sender 消息后自动回发 DELIVERY 回执
（`ts/messages/handleDataMessage.preload.ts:374-398` +
`ts/util/handleEditMessage.preload.ts:267-286`），连接器此前经
`sendDeliveryReceipt` 手动补面。引擎现为入站行自动排队回执，行为加面、
既有方法/字段零改动；连接器侧改不改用自动面由其自行决定（其显式调用
仍有效，两者不互斥）。

- **排队闸**（官方条件面同位）：入站 DataMessage / EditMessage（sync/
  sent 自发回声不回）、sealed sender 信封
  （`unidentifiedDeliveryReceived` ← Metadata.unidentified_sender，明信封
  不回）、解密成功（引擎侧 = `Received::Content` 路径，对位官方
  `!hasErrors`；`Received::IdentityChange` 臂不排队，官方 hasErrors 同
  位）、非 story（`metadata.story`）、发送方为 ACI（官方
  `isAciString` strictAssert，PNI 不回）。回执时间戳 =
  dataMessage.timestamp（edit 用内层 dataMessage.timestamp，官方
  `editAttributes.message.timestamp` 同位；内层缺席 = unprocessable
  不回；dataMessage 时间戳缺席回退信封 client timestamp）。
- **批量**（官方 deliveryReceiptBatcher `wait: 500` / `maxSize: 100`，
  `ts/util/deliveryReceipt.preload.ts`）：500ms 批量窗 / 批满 100 即发，
  按发送方归组逐作者直发 `ReceiptMessage{DELIVERY, timestamps[]}`（原
  会话是群组也直发原作者，官方从不整组广播回执），经 sendReceipt 公共
  发送面（CHUNK_SIZE=100 分帧）。批量缓冲为管线生命周期——流断开重连
  不丢待发批次（下一条流批量窗续发）；flush 发送走 spawn_local 不阻塞
  收流，失败静默弃批（官方 sendReceipt 失败不重试同位；引擎零 stderr
  纪律，compat §1）。
- **偏差登记（引擎侧不可得的官方闸）**：官方发送回执前查
  `conversation.getAccepted()`（会话接受态）与拉黑会话——两者都是宿主
  /connector 侧会话模型状态，引擎无会话模型不可得。引擎对满足上述条
  件的入站行一律排队回执；若需该闸，由连接器在会话层断开
  autoReceipt 开关语义承接（未来加连接器侧开关属加面，另行登记）。

### syncMessage.read / viewed 收包投影（D 批 D2，2026-10-11，官方对齐）

官方桌面把多设备的已读/已看同步当独立事件消费
（`MessageReceiver.preload.ts:3075/:3117` → `#handleRead` :3437 /
`#handleViewed` :3472）；引擎此前在 SynchronizeMessage 投影兜底臂整体
丢弃（presage 亦不落库，多设备 read 同步无引擎面）。本批加投影面，
wire additive，既有方法/字段零改动。

- **线面**：`SyncMessage` 直接 repeated 字段 `read = 5` /
  `viewed = 16`（不在 content oneof；vendored proto 已有，无 vendor
  改动）。官方「read 专信封」调度序同位——既有 content oneof 各分支
  不动，投影由兜底臂承接（content 与 read/viewed 并存时官方同样 read
  优先独占处理）。
- **投影形状**：`envelope.syncMessage.read[]` / `.viewed[]` 条目对数组
  `{senderUuid, senderNumber?, timestamp}`——官方语义就是
  (senderAci, timestamp) 对（逐条独立、多发送方各占一条），比聚合成
  `read{timestamps[]}` 形状忠实；条目内 senderUuid 双形解析与官方
  `fromAciUuidBytesOrString` 同构（senderAciBinary 16B 优先、senderAci
  字符串兜底），senderNumber contacts 命中补面（receive 信封作者号同
  口径）。timestamp 缺席或发送方不可解析丢条目（宁缺勿错；官方
  `toNumber ?? 0` / senderAci null 由上层过滤同位）；空数组键缺席；
  read/viewed 全空整信封零投影（既有行为不变）。
- **消费面**：connector 现今两者都不消费（其 implementation-plan
  §read-sync 记档在案），纯加面待其接入；接收侧 presage 不落库
  （read/viewed 无引擎落库语义，投影即全部）。

### 群态 change 全投影（groupChanged，D 批 D3，2026-10-11，官方对齐）

群更新事件面此前只放开了两类：terminate → `groupTerminated`、
expiration timer → `groupUpdated`；其余群态 change（成员/权限/链接/
公告面）整体跳过。本批加 `groupChanged` 全投影：群更新消息携带的
change 批内含**非 timer/terminate** change 时投影

```
{method: "receive", params: {account, envelope: {timestamp, groupChanged:
 {groupId, revision, editor?, changes[], expireTimerSeconds?}}}}
```

- **词表**：`changes[]` 条目对齐官方桌面 GroupV2ChangeDetailType
  （`ts/types/groups.std.ts` 联合 27 类）与引擎既有 history.rs 键形——
  `title{newTitle?}`（空串同缺席）、`avatar{removed}`（空 = removed）、
  `description{description|removed}`（空/缺席 = removed:true）、
  `access-attributes/access-members/access-invite-link/
  access-member-label{newPrivilege}`（GroupV2AccessLevel 字符串名
  UNKNOWN/ANY/MEMBER/ADMINISTRATOR/UNSATISFIABLE）、
  `member-add{aci}`、`member-remove{aci}`、
  `member-privilege{aci,newPrivilege}`（UNKNOWN/DEFAULT/ADMINISTRATOR）、
  `pending-add-one{serviceId}`、`pending-remove-one{serviceId}`、
  `member-add-from-invite{aci|pni}`、
  `admin-approval-add-one{aci}`、
  `admin-approval-remove-one{aci}`、
  `member-add-from-admin-approval{aci}`、`group-link-reset`、
  `announcements-only{announcementsOnly}`。
- **边界与偏差（宁缺勿错）**：① timer 夹带不进 `changes[]`，走既有
  `expireTimerSeconds` 键形（0 = 关闭，`groupUpdated` 同语义）；纯
  timer 批不投影 `groupChanged`（既有面承接）。② terminate 优先级
  不变——批内含 terminate 仍整批投影 `groupTerminated`。③ 官方桌面
  不渲染类跳过并登记：`modifyMemberProfileKey`、ban 两族
  （add/delete）、`promotePendingPniAci`、`memberLabel` 内务四族
  （detail 词表无对应类）。④ `pending-remove-one` 不带 `inviter`、
  `member-add-from-invite` 不带 `inviter`：wire change 无该位，官方取
  自旧群态 diff，引擎不做旧态对账（群态落库由 presage upsert 面承
  担）。⑤ epoch ≤ 7 闸沿用（官方 isChangeEpochSupported 同语义）。
- **消费面**：connector 现今不消费 `groupChanged`（纯加面待其接入）；
  群态落库不因本批变化（presage registered 收到 groupV2 即
  upsert_group 拉服务器态，既有行为）。

### quote 附件元数据（quote.attachments[]，D 批 D4，2026-10-11，官方对齐）

引用消息此前只有文本/作者/id 三面：接收投影丢弃被引
`quote.attachments[]`（缩略占位不出，缩略图位永远空）；发送组装不出附
件条目（被引行即使带图，quote 也无缩略位）。本批双面对齐官方
makeQuote（`ts/util/makeQuote.preload.ts`），wire additive。

- **接收投影**：`envelope.dataMessage.quote.attachments[]` 条目投影
  `{contentType?, filename?, thumbnail?}`——官方 QuotedAttachment wire
  三键形，键缺席即协议缺省；thumbnail = AttachmentPointer 全投影
  （`project_attachment_pointer` 同源，引擎附件面同形）；空数组
  `attachments` 键缺席。
- **发送组装**：`send` 行 quote 找回被引消息后按官方
  `getQuoteAttachment`（makeQuote.preload.ts:69-138）优先级取一条出
  `attachments[]`：被引消息**首附件**（contentType/fileName 原值透传）
  > **preview 首图**（无 filename，官方 preview 图同位）> **sticker
  data**（无 filename）；各级各 take 1。view-once 被引行走官方
  isTapToView 短路（makeQuote.preload.ts:50-52）：固定
  `[{contentType: "image/jpeg"}]` 占位，不查附件面。被引消息找回失败
  （store 无该行）→ text 与 attachments 一并缺席，quote 仍有 id/author
  面（§3 既有降级同位，官方 quotedMessageNotFound 同款）。
- **偏差登记（metadata-only gap）**：官方 thumbnail 随 quote 上传本
  地缩略图数据（loadAttachmentData + url）；引擎无图像管线（无缩略
  图生成/无上传），发送侧 `thumbnail` 恒缺 = 收端引用行少缩略预览，
  文本/作者/附件类型面完整，协议面合法。接收侧 thumbnail 照投影透
  传（对端真实出的缩略指针，metadata-only 不渲染）。附件 wire 键
  `filename` 同引擎附件面（signal-cli 键形），非官方桌面
  `fileName` 驼峰——消费方以 connector 既有附件面键形为准。

### 协议能力补齐与设备面注册（engine.deviceCapabilities，D 批 D5，2026-10-11，官方对齐）

引擎能力自证面（2026-10-10 批）此后缺两件事：vendor `DeviceCapabilities`
结构体只有六位（server 真相 Signal-Server `DeviceCapability.java` 全集
还有 `versionedExpirationTimer`/`optionalPhoneNumber` 两位，vendor 注释
自证的 keep-in-sync 面缺口）；且引擎从未走官方**设备面注册**端点——
官方桌面每次链接启动（`background.preload.ts:2021`
afterEveryLinkedStartup → `registerCapabilities`，端点
`PUT v1/devices/capabilities`，WebAPI.preload.ts:811/:2301）都全量重登
记能力位，上游 2026-03 无预告批量注销 capability 不达标客户端的教训
正依赖该面保持最新。本批补齐（wire additive）。

- **结构体补齐**：vendored `DeviceCapabilities` 加
  `versioned_expiration_timer`/`optional_phone_number`（serde default
  false，JSON 形 `versionedExpirationTimer`/`optionalPhoneNumber`）；
  presage 主注册链构造点同值补齐（编译期断强制）；引擎
  `DEVICE_CAPABILITIES` 镜像与 vendor 扫描单测同步扩两位。
  `engine.protocolCapabilities.deviceCapabilities` 面 additive 出两键。
- **位值裁定（不发明）**：`versionedExpirationTimer` = false——官方桌
  面 CapabilitiesUploadType 不含该位（desktop 不声明，引擎同位）；
  `optionalPhoneNumber` = false——官方上传值是账号状态位
  （`getOptionalNumber() == null`，号码 unshare 态才 true），KT 引擎账
  号号码恒共享态。
- **设备面注册**：vendor 新 `CapabilitiesUpload`（官方
  CapabilitiesUploadType 四键 wire 同形：attachmentBackfill/spqr/
  usernameChangeSyncMessage/optionalPhoneNumber）+ `PUT
  v1/devices/capabilities` 客户端调用 + presage Manager
  `register_device_capabilities`。引擎两处触发：① 接收管线建流时
  best-effort 自动注册（官方每次链接启动同位；失败静默随重连重试，
  成功置位每管线运行一次即止——官方 afterEveryAuthConnect 不重发同
  口径）；② 新 KT 扩展方法 `engine.deviceCapabilities {account}`（宿
  主可查询/手动重发，成功回 `{account, registered, capabilities}` 上
  传面，错误面 = getUserStatus 同款解绑原文/折叠）。
- **偏差登记**：① `attachmentBackfill` 引擎设备面声明 false，官方桌
  面恒 true（desktop 已实现 backfill 语义）——引擎未实现 backfill 接
  收，宁缺勿错不声明未实现的特性（服务器不再向本设备派发 backfill
  面，恰为引擎所需）；② 设备面 body 复用四键官方形状，不携带账号属
  性面六位（官方两面本就不同型，不并面）；③ optionalPhoneNumber 状
  态位引擎无用户号共享模型，恒 false（账号号码共享态如实声明）。

### P9 身份安全闸（2026-10-11 批，官方对齐 Signal-Desktop abe80d3）

B 批「强制闸待桌面批」遗留闭合 + 身份防护三面，wire additive：新错误
字面量一枚（`send` 系拒发原文，见 §3 send 行），既有方法/事件零改动。

- **身份拒发闸（S1）**：对端 identity key 变更后未确认，拒发。官方
  三层（`isTrustedForSending` key 不匹配 → false、jobQueue
  `stoppedByMissingVerification`、阻断弹窗 + send-anyway 二次确认）的
  引擎侧等价两层：
  ① 协议层——vendored presage-store-sqlite `is_trusted_identity` 存储钥
  与在用钥不匹配时按方向裁定：`Direction::Receiving` 照常放行（官方
  `isTrustedIdentity` Receiving 臂恒 true 同位；换钥/事件链不受影
  响），`Direction::Sending` 返回 false（官方 `isTrustedForSending`
  不匹配 → false 同位，ts/SignalProtocolStore.preload.ts:1986-2023）。
  libsignal v0.99.0 建会话（session.rs process_prekey_bundle:192-199）
  与加密（session_management.rs:121-133）两路均以 Sending 方向咨询该
  面，false → `SignalProtocolError::UntrustedIdentity` → vendored
  sender 统一再包 `MessageSenderError::UntrustedIdentity` → 引擎
  `SendError::IdentityUnverified`。首次信任建立（无存储身份）两向恒放
  行——新联系人/首条消息不闸。
  ② 服务层——sidecar `pending` 挂起闸：接收循环 IdentityChange 臂对
  换钥发送方落标（`identity-trust.json` 条目 `pending: true`，verified
  态随换钥降级 unverified——官方 saveIdentity 换钥 VERIFIED/UNVERIFIED
  → UNVERIFIED 同位，ts/SignalProtocolStore.preload.ts:2160-2170），
  直聊内容发送面（send/撤回/回应/pin/unpin/直聊通话信令/回执公共面）
  逐收件人判定拒发。**确认钩子 = 既有 `trust` 写面**：verified 臂与
  `state: "default"`（官方 send-anyway 非阻断批准同位）清标放行，
  `state: "unverified"` 保持挂起；verified 同步下行（B4 面）同规则收
  敛。客户端确认动作面（弹窗/按钮）仍归桌面批，引擎钩子已够：上层收
  `identityChanged` 事件 → 查 `listIdentities`（trustLevel 换钥后如实
  显示降级）→ `trust` 确认。
- **错误面**：`SendError::IdentityUnverified` → `-32603` +
  `Failed to send message: Identity unverified. (UntrustedIdentityException)`
  （锚词 = 官方同因异常类 `UntrustedIdentityKeyException` 的 word
  形；connector 白名单登记归 connector 批）。确定性失败不自动重试。
- **闸面范围（行为改变面声明）**：生效 = send/撤回/回应/pin/unpin/
  直聊通话信令/回执（delivery·read·viewed 公共面，自动 delivery flush
  复用该面失败静默弃批——官方 jobQueue 对 untrusted 取消 Receipts 任
  务同位，conversationJobQueue.preload.ts:1141-1146）。不闸 = typing
  （无内容瞬态面，官方经 jobQueue 会被闸，此处偏差登记）、群发送
  （群成员身份闸未立项，后续批）、story/自同步面（self-send 无对端钥
  面）、DERM 回执/重发（协议自愈面：重发遇不匹配被协议层拒、静默，
  防环闸不变）。
- **sidecar 形状**：`identity-trust.json` 条目 additive 键
  `pending`（bool；存量条目缺键 = 未挂起，读面容错不变）。
- **同步请求停答（S2）**：vendored presage 收向 `Request{…}` 三臂
  （Contacts/Keys/Blocked）全部停答（warn + 跳过，不回任何表）。官方
  语义：链接设备只发请求不应答——ts/textsecure/syncRequests.preload.ts:
  11-33 链接侧只发三请求；ts/textsecure/MessageReceiver.preload.ts:
  3070-3074 收向 request 仅记日志后从缓存移除，官方桌面不充当应答方。
  此前 Blocked 臂回全空表，若被对端消费按官方 _handleBlocked 差集语义
  即等价「解除全部拉黑」（:3988-4044/:4207-4230）；Keys 臂属账号熵材料
  （AEP）按需过度同步，一并停答。防再触发：vendored 改动处注释引用
  官方 file:line。
- **profileSharing 闸（S3）**：直聊内容发送面（send/撤回/回应/pin/unpin）
  逐收件人查 Storage Service 联系人记录 `ContactRecord.whitelisted` 位
  （官方 storage proto field 10；引擎无本地会话库，位从云读，与 blocked
  读面同源）：位 false → 不注入本机 profile_key（官方
  sendNormalMessage.preload.ts:183-185 profileSharing 位闸同位——
  profile_key 外泄 = 对端可反查本机 profile，未同意即不外泄）；缺记录/
  位 true → 照常注入。读面 master key 缺失/manifest 404/网络/解密失败 →
  fail-open 照常注入（官方读本地库无失败面，偏差登记）。vendored 侧
  `send_message` 拆 `send_message_with_profile_sharing`（前者行为不变，
  后者注入位受参）；群发送/story 面不在本闸（vendored 群发送无本机
  profile_key 注入点）；typing/回执面无 profile_key 字段，无关。
- **会话 archive（S4）**：vendored presage `save_identity` 存储钥与新钥
  真异时，同事务清除该 address 名下会话（官方 saveIdentity 换钥
  archiveSiblingSessions 同位，ts/SignalProtocolStore.preload.ts:
  2140-2192/:2188-2192——旧钥下建的会话不得在换钥后存活，下一收发自新
  钥重建，DERM 重发/MRM 重试自愈面承接对端旧会话残留）。以先读旧钥
  比对为判定：上游 rows_affected 语义把「行存在」当「钥变」，同钥重存
  （如正常建会话路径）不得清会话。群 outbound sender key 复位未做
  （引擎无群成员面，取舍登记）；inbound sender key 不清（群收向自愈）。

## 6. fake 服务模式（仅测试）

`KT_ENGINE_FAKE_SERVICE=1` 显式开启（生产缺省永远走 real）：P1a 形状桩
原样保留——`startLink` 回夹具形状 URI、`finishLink` 合成 `+1555000xxxx`
号码、send 系方法只落调度日志不触网。用途：IPC 契约测试在无网络环境下
钉定连接器可见的形状/错误码/日志纪律（`tests/contract.rs`）。该开关不是
运行模式切换，连接器生产环境禁止设置。
