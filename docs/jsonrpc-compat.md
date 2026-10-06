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
| `listGroups` | `{account}` | `[{id(base64), name, isMember, members:[{number}], terminated(bool)}]` | 未注册账号 `-32603` | **P2 real**：presage 群组投影（id = master key 标准 base64，send 可回传）；`terminated` 位 2026-10-06 扩（KT 扩展批：进程内登记 + 服务器群态读面，§5 群终止批；fake 恒空列表该键不出现） |
| `deleteLocalAccountData` | `{account}` | `{}` | — | **P1b real**：注册表摘除 + store 目录删除 + 接收管线停机 |
| `send` | `{account, recipient[]/groupId, message, quoteTimestamp?, quoteAuthor?, previewUrl?, previewTitle?, previewDescription?, previewImage?, mentions?: [{number, start, length}], attachments?: [dataURI], sticker?: {packId(hex), packKey(base64), stickerId, emoji?, image(dataURI), width?, height?}, viewOnce?}` | `{timestamp, results:[]}` | 收件人未解析/群不可解/quote 半参/quote 在场但 author 不可解析（文本与贴纸分支同口径）/mentions 条目缺字段/附件条目非字符串/sticker 缺字段或与 message·attachments·preview·mentions 同发（官方互斥；quote 与 sticker 可共存）/viewOnce 非布尔，或 viewOnce:true 违反官方 isValidTapToView 同构（body 非空·附件≠1·附件 mediatype 非 image\|video·与 quote·preview·mentions·sticker 同发——mentions 亦禁发，比官方更严）`-32603`；群组形态目标已终止 `SEND_TARGET_TERMINATED`（`-1` + message，2026-10-06 群终止批，§5） | **P2 real**：直聊逐号码 + 群组 `send_message_to_group`；quote id/author→ACI + text 本机 store 找回（signal-cli 同行为，找不到缺省）；preview 面 + previewImage 随附件同批 CDN 上行（官方 Preview.image 同构）；mentions→BodyRange.MentionAci；attachments 为 connector 上游唯一形状（data URI 数组，`data:<type>[;filename=<pct>][;voice=true];base64,<payload>`，voice=true 映射官方 VOICE_MESSAGE flags（presage voice_note 同构，2026-10-04））→ presage 官方 upload_attachments 上行（≤32 条、单条 ≤100MB，超限 fail-closed）；sticker 贴纸面（2026-10-04，官方 sendStickerMessage 同构）：image data URI 上行（mediatype 必须 image/*，width/height 官方 AttachmentPointer 展示位透传），packId hex/packKey base64（官方 Bytes.fromHex/fromBase64 同构），body=undefined 无常规附件；sticker 与 quote 共存（2026-10-04，官方「贴纸回应」StagedStickerReply = 同一 DataMessage 带 sticker+quote，非 reaction——Reaction proto 无贴纸位）：quote 组装与文本分支同源（assemble_quote 单点），author 不可解析 fail-closed、被引正文 store 找回失败降级缺 text，body 维持 undefined；viewOnce 阅后即焚面（2026-10-04，官方 isViewOnce=14 同名直投）：`viewOnce: true` → body 空 + 恰 1 附件 + `is_view_once=Some(true)` 的 DataMessage（官方发送侧不 bump requiredProtocolVersion——构造面不设该字段，presage send 面注入默认 0 = 官方初值），直聊/群组两形态同构，与 disappearing messages 完全独立 |
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
| `setAccountConfiguration` | `{account, readReceipts?, typingIndicators?, phoneNumberSharingMode?: "everybody"\|"nobody", unlistedPhoneNumber?}`（只写提供的字段；至少一项） | `{}` | 提供字段已全部满足 → 幂等无写成功；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失（含零可写字段）/形状错（布尔字段非布尔、phoneNumberSharingMode 非法值或非字符串）/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateConfiguration 等价）**：真相源 = AccountRecord 的 readReceipts(6)/typingIndicators(8)/phoneNumberSharingMode(12)/unlistedPhoneNumber(13)，读改写仅动提供字段（字段保真铁面同款链路），+ 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；枚举 everybody=1/nobody=2（桌面 v8.31/Android 现行一致，旧 CONTACTS_ONLY 已从官方 proto 移除不收）；真相源单点（AccountRecord），其他设备经 storage sync 收敛，无额外同步广播；fake：成功 `{}` |
| `setProfile` | `{account, givenName, familyName?, about?, emoji?, avatar?}`（文字面全量写：givenName 必填非空，familyName/about/emoji 缺省=置空，官方 updateProfile 同口径；字节预算 UTF-8：given/family 各 ≤128B、about ≤512B、emoji ≤32B，超限 `-32603`；avatar 三态：缺省=保留现有、data URI=换新、null=移除） | `{"status":"saved"}` | 头像上行（profile PUT/CDN POST）失败 `-1 PROFILE_UPLOAD_FAILED`（data `{retryable:true, textSaved}`——textSaved=true 如实呈现文字面已提交的部分成功）；storage 链路同 `setAccountConfiguration` 四码；参数缺失/givenName 空、familyName/about/emoji 非字符串或超字节预算、avatar 非 image 族 data URI/非 base64/超 10MB/非字符串/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateProfile 等价）+ 对齐批②（2026-10-06 形状/错误/预算面，详见 §5）**：文字面走 presage `Manager::update_profile`（retain_avatar=true）；头像面为上游双层缺口自建薄封装（官方桌面 putProfile+uploadAvatar 行为对齐：`PUT /v1/profile` JSON `{version, name, about, aboutEmoji(加密 b64), avatar, sameAvatar:false, commitment(b64)}` → 响应上传属性 → 加密头像 multipart POST CDN0 根 → avatar path=响应 key，详见 §5）；写后 storage 面 AccountRecord.givenName/familyName/avatarUrlPath 同步；fake：成功 `{"status":"saved"}`，不落调度日志 |
| `setUniversalExpireTimer` | `{account, expirationInSeconds}`（0=关闭） | `{}` | 同值幂等无写成功；PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（timer 字符串/负数/小数/超 u32）/未注册账号 `-32603` | **P5 real（2026-10-05，E 批「不可达」结论勘误后落地）**：真相源 = AccountRecord.universalExpireTimer(17)（① vendored 官方 proto），读改写仅动该字段 + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；账户级默认值无广播（会话级计时器才广播 expiration-update），其他设备经 storage sync 收敛；fake：成功 `{}` |
| `getBlockedConversations` | `{account}` | `{blocked: [{conversationId, kind: contact\|group\|legacyGroup}]}`（存储序；条目形状与置顶面同构；v1 不含 blockedAtTimestamp） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real（2026-10-05 只读面）**：Storage Service manifest → 全部 type=1/2/3（CONTACT/GROUPV1/GROUPV2）identifier 批量原样读 → vendored proto 解码 → blocked 位过滤投影（contact 口径与置顶面同构：aciBinary 16B → UUID 串优先、字符串形态直通、e164 跳过；group=master key 标准 base64、legacyGroup=legacy id bytes）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；坏条目（缺 item/解密失败/解码失败/标识映射不出）跳过零 stderr；fake：恒 `{blocked: []}` |
| `getAvatar` | `{account, conversationId}`（形状分派：ACI/PNI ServiceId UUID 直通、手机号文本（E.164/裸数字）经 contacts 反查、群 = master key 标准 base64（listGroups id 同源）；三者皆不中 `-32603`） | `{dataUri, size}`（data URI = `data:<mime>;base64,<payload>`，MIME 魔数嗅探、未识别回退 `application/octet-stream`；size = 原始字节数） | 参数缺失/会话标识形状非法/未注册账号 `-32603`（参数面不占用结构化码）；无头像（未同步 profile_key/对端或群未设头像/服务器 403/404）→ `AVATAR_UNAVAILABLE`；网络/下载/解密失败 → `AVATAR_FETCH_FAILED`；超 10MB 帽 → `AVATAR_TOO_LARGE`（均 `-1` + message 码位） | **2026-10-06 real（KT 扩展）**：1:1 走 presage `retrieve_profile_avatar_by_uuid`（profileKey = presage store `profile_key(service_id)`，contacts 同步随行落库；未同步 → 诚实「无头像」）；群走 presage `retrieve_group_avatar`（master key 派生 GroupSecretParams 解密）；缓存/CDN/解密/入库由 presage 承担；10MB 帽照 presage 注释口径，超出 fail-closed；fake：`AVATAR_UNAVAILABLE`（不落调度日志，读面纪律） |
| `setConversationBlocked` | `{account, conversationId, kind: contact\|group\|legacyGroup, blocked(bool)}` | `{blocked: [...]}`（写后全量，同 getBlockedConversations 形状，存储序） | 云目标记录解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`（均 `-1` + message，同置顶面）；参数缺失/未知 kind/blocked 非布尔/未注册账号 `-32603` | **2026-10-06 real（KT 扩展）**：官方 blocked 位写语义（读改写目标会话记录仅动 blocked 位 + blockedAtTimestamp，官方 storageRecordOps 同语义：首次拉黑 = 引擎时刻、已拉黑幂等不重写他端时刻（含 0 原样保留）、解除清零；GV1 无时间戳字段）+ 新 16B item id + version+1 + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态：blocked=true → master key 播种 + version 1 最小 AccountRecord 与目标记录同批首写，blocked=false → 无可解除幂等成功不播种；纯拉黑不删会话（官方桌面同语义，消息/会话不清理）；fake：成功回 `{blocked: []}` |
| `reportSpam` | `{account, targetAci(ServiceId: ACI/PNI UUID), serverGuids[]（每项必须可解析 UUID，归一为小写连字形态入路径；urn:uuid:/花括号等宽松形态 fail-closed）, token?}`（单批 ≤1000，官方 reportSpamJobQueue zod max 同源） | `{reported, failures: [{serverGuid, status?}]}`（status = 服务器 HTTP 状态码；传输层失败该键缺席不发明 0） | 参数缺失/targetAci 非 ServiceId/serverGuids 缺失·非数组·空批·元素非 UUID/超 1000/token 非字符串形状/未注册账号 `-32603`（参数面不占用结构化码）；全部 serverGuid 均失败 → `REPORT_SPAM_FAILED`（`-1` + message） | **2026-10-06 real（KT 扩展）**：官方桌面同款 `POST v1/messages/report/{senderAci}/{serverGuid}` 逐条独立请求、JSON body `{token}`（token 缺省序列化为 `{}`，官方 jsonData 同构；本机凭据 basic auth）；部分失败诚实投影：单条 4xx/5xx/传输失败记入 failures（带状态码），至少一条成功即返回 `{reported, failures}`，全部失败才整体报错；fake：全量成功 `{reported: N, failures: []}`（落调度日志，协议上行面纪律） |
| `setExpirationTimer` | `{account, recipient(单字符串), expirationInSeconds}`（0=关闭） | `{timestamp}`（广播信封时间戳；同值 no-op = 处理时刻） | 参数缺失/形状错（recipient 数组、timer 字符串/负数/小数/超 u32）/未注册联系人/未注册账号 `-32603` | **E 批 real（2026-10-05 消失消息对齐）**：1:1 计时器设置 + expiration-update 广播（signal-cli `Manager.setExpirationTimer` 语义对位：先持久化 presage store Contact 面 (timer, version+1) 再广播；构造面不设 expire_timer，presage send 面注入，详见 §5）；fake：成功 `{"timestamp":u64}` |
| `sendCallMessage` | `{account, recipient(单号码/ACI), data?(base64 opaque), urgency?("droppable"\|"handleImmediately" 缺省 droppable), legacy?({kind:"busy", id:正数} \| {kind:"hangup", id:正数, type?("normal"\|"accepted"\|"declined"\|"busy"\|"needPermission" 缺省 normal), deviceId?}), destinationDeviceId?}`（modern 面=data+urgency 构造 `CallMessage{opaque{data,urgency}}`；legacy 面=busy/hangup 官方裸帧族构造 `CallMessage{busy{id}}`/`CallMessage{hangup{id,type,deviceId?}}` 无 opaque 字段；**两面互斥**——同置或都缺 `-32603`；1:1 only，groupId 形态 fail-closed——群组通话走 SFU 不在本面） | `{timestamp}`（信封时间戳） | 参数缺失/形状错（data 非 base64/空负载、urgency 非法值或非字符串、legacy 未知 kind/id 缺席·≤0/type 非法词/deviceId 超 u32、两面同置、destinationDeviceId 超 u32 或非数值、groupId 在场）/未注册账号 `-32603` | **2026-10-06 real（KT 扩展，通话消息面）**：官方 Content.callMessage=3 信封（Signal-RTC 层信令负载，引擎不解析只透传）；**2026-10-06 legacy 帧批（1.53 协同）**：legacy busy/hangup 按官方 proto 裸帧族编码（官方桌面 callingMessageToProto busy={id}/hangup={id,type}；RingRTC 2.72.1 出向 hangup/busy 实证无 opaque 材料；signal-cli 0.14.7 CallManager sendBusyViaSignal/sendHangupViaSignal 同构）——单测钉 prost 字节编码（busy=字段5{id}、hangup=字段7{id,type,deviceId}、两面无字段10）；presage `send_message` 通路（online=false 偏差与 sent-transcript 缺席见 §5 通话消息块）；fake：成功 `{"timestamp":u64}`（落调度日志，协议上行面纪律） |
| `backup.export` | `{account}` | `{"status": "started"}`（后台任务已启动，进度走 `backup.exportStatus`） | 未注册/fake 账号 `-32603`；该账号任务已在跑 `BACKUP_EXPORT_ALREADY_RUNNING`；工件缺失 `BACKUP_ARTIFACT_MISSING`；首行非法 `BACKUP_ARTIFACT_INVALID`（工件当场删除）——结构化码均 `-1` + message 码位（备份导出契约 §10.7，connector 1.47 白名单计划） | **消费批一期 real（2026-10-05）**：文件工件交接（备份导出契约 §10）——读 `<账号目录>/backup-export-input.ndjson`（connector 写入，引擎契约 §10.2 行格式/帽值）→ 专用 OS 线程装配 `MainStreamWriter` → `export_local_backup` 落官方 §5 快照；任务离开 running 即删工件（§10.2 硬边界）；每账号至多一任务；不落调度日志零 stderr；fake 模式账号无 store，按未注册 `-32603` |
| `backup.exportStatus` | `{account}` | `{account, state: idle\|running\|completed\|failed, artifactPresent, startedAtMs?, finishedAtMs?, lines?, frames?, snapshot?: {name, mainBytes, frames?}, error?, snapshots: [{name, mainBytes, frames?}]}`（快照清单 ≤32 条新→旧；`frames` 仅本进程任务产出已知；诚实缺席不发明 0） | 未注册账号 `-32603` | **消费批一期 real（2026-10-05）**：只读轮询面，仿 connector `history.importStatus` 语义（无推送事件，轮询到终态）；任务注册表进程内存不跨重启；快照清单只报告不清理（生命周期归 connector，备份导出契约 §10.6）；不落调度日志零 stderr |
| `transfer.downloadStatus` | `{account}` | `{present: bool}`；present=true 时 `{present, bytesRead(u64), bytesTotal?(u64)}`（快照缺席（未开始/已结束/引擎重启过）答 `{present:false}`，不猜） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态） | **2026-10-06 real（KT 扩展，契约 1.53 §4.38 扩展）**：transfer_archive 下载进度快照查询——`history.importStatus` progress 组合面的引擎侧只读数据源。进程内 per-account 注册表（`src/service/transfer_progress.rs`，std Mutex + Arc，不落盘不走文件），下载任务边读 CDN 响应边累计（块读）、content-length 有则记 total（拿不到只报 stage 不给假 0），任务收尾/失败/中途退出统一清快照；官方对齐（语义零拷贝）：Signal-Desktop Download 段 currentBytes/totalBytes 进度（`ts/state/ducks/installer.preload.ts` L143-152 + reducer L642-675）。只读不落调度日志零 stderr（backup.exportStatus 同纪律）
| `terminateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源)}` | `{status: "terminated"}`（幂等：已终止群再次调用同形状成功，不重复上传） | 参数缺失/groupId 非 base64 或长度 ≠32B `-32603`；未注册账号 `-32603`；服务器拒绝 `GROUP_TERMINATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批）**：官方桌面 buildTerminateChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、terminate_group action，change epoch 7），群凭据 basic auth（get_authorization_for_today 同语义链，§5 群终止批）；发送联动面：已终止群 send 系硬拒 `SEND_TARGET_TERMINATED`（`-1` + message）；receive 联动面：terminated change 投影 `groupTerminated` 事件（§5 群终止批）；fake：成功 `{status: "terminated"}`（落调度日志，协议上行面纪律） |
| `updateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源), expiration}`（本批只收 expiration 一个参数族，0=关闭；expiration 缺席 = 无操作整拒，不静默 no-op） | `{status: "sent"}`（群族成功答同纪律，connector 契约 1.45 §4.44 同形状） | 参数缺失/形状错（groupId 非 base64 或长度 ≠32B、expiration 缺席·null·字符串·负数·小数·超 u32）`-32603`；未注册账号 `-32603`；服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批，群 expiration）**：官方桌面 buildDisappearingMessagesTimerChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、modify_disappearing_message_timer action（field 12），timer = 加密 `GroupAttributeBlob{disappearingMessagesDuration}`（uint32 秒，0=关）），群凭据 basic auth（§5 群终止批同链）；receive 联动面：expiration change 投影 `groupUpdated` 事件（§5 群 expiration 批）；connector 侧由 `conversations.setExpireTimer` 群形态路由至此（契约 1.45，kt-engine 面 INVALID_REQUEST 债务随本批解除）；fake：成功 `{status: "sent"}`（落调度日志，协议上行面纪律） |
| `storage.write` | `{accountId, updates: [{type: "INSERT"\|"UPDATE"\|"DELETE", key(标准 base64，16B storage item key), record?(标准 base64，StorageRecord 明文 protobuf bytes)}]}`（字段名 `accountId` 为本扩展契约原形，非白名单面惯用的 `account`；INSERT/UPDATE 必带 record，DELETE 带 record 拒；updates 空批拒；批量内 key 重复拒） | `{version, results: [{type, key, writtenKey?, noop?}]}`（version = 写后 manifest version，全 no-op = 当前 version，从未有 storage 状态 = 0；results 与 updates 输入序对齐；`writtenKey`：INSERT = 原 key 回显、UPDATE = 引擎轮换出的新 16B item id——官方换写必轮换 item id 语义，旧 key 进 deleteKey；DELETE 无 writtenKey；no-op 条目带 `noop: true`） | 参数缺失/形状错（accountId 缺失·非字符串·空、updates 非数组·空批、条目非对象、type 非三枚举、key 非 base64·≠16B、INSERT/UPDATE record 缺失·null·非 base64·空、DELETE 带 record、批量 key 重复）`-32603`；record 解码不出白名单记录 arm（Contact=1/GroupV1=2/GroupV2=3/StickerPack=6 之外，含 Account——账号记录归专用写面）`-32603`；INSERT 目标 key 已存在且内容不同/UPDATE 换记录类型（意图冲突 fail-closed）`-32603`；未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、item 读取/解密失败 `STORAGE_READ_FAILED`、PUT 失败 `STORAGE_WRITE_FAILED`（均 `-1` + message，置顶面同款）；版本冲突（服务器 409 = base version 失配）`STORAGE_VERSION_CONFLICT`（`-1` + message + data `{retryable: false, serverVersion?}`——写意图未落地，引擎**不自动重试**，调用方重读 manifest 后重放） | **2026-10-06 real（KT 扩展，storage 写面 P0）**：通用批量 manifest item 写（官方 WriteOperation 同构：manifest version+1、新 item 加密入 insertItem、换出 key 进 deleteKey、source_device/record_ikm 透传）；读侧 manifest version 解析复用既有面（`StorageService::manifest`/`decrypt_manifest`），写通道复用既有自建 PUT 薄封装（presage/libsignal-service 上游无写面，§5 storage 写面 P0 块）；幂等：INSERT 同 key 同内容/UPDATE 同内容/DELETE key 不在 = no-op（全 no-op 不 PUT 不 bump version，整批重放安全）；从未有 storage 状态：纯 UPDATE/DELETE 批 = 幂等成功不播种（version 0），含 INSERT → master key 播种 + version 1 最小 AccountRecord 同批首写（§5 首写分支同款）；fake：成功 `{version: 0, results: [...]}`，不落调度日志（storage 状态面纪律） |

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
  size?, width?, height?, caption?, isVoiceNote}`（≤32，元数据 only，
  字节永不下载）、`previews[] {url, title?, description?, image?}`（≤4）、
  `mentions[] {number, start, length}`（≤64，来自 bodyRanges）、
  `viewOnce: true`；viewOnce 行跑官方 isValidTapToView 同构校验，非法补
  `viewOnceInvalid: true`（2026-10-04，官方 isTapToViewInvalid 同位；引擎
  metadata-only 只标记不擦除，§5）。引用/反应作者号 contacts 命中投影
  E.164，未命中保留 ACI 字符串。textStyles：本引擎钉定的 presage proto
  版本无此面，不投影。消失消息 E 批（2026-10-05）：`expiresInSeconds`
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
  UUID 双形态解析（send 系同口径）；返回信封 timestamp。**群组通话不
  做**（groupId 形态 fail-closed）：群 call 走 SFU 通道（GroupCall 选
  项 + CallLinkUpdate），与本面 1:1 Signal-RTC 信令完全异构，消费方提
  出需求再立项。
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
  GroupV1=2 / GroupV2=3 / StickerPack=6 四类 manifest 记录；Account=4
  归专用写面（账号记录字段面引擎全量保真责任在语义级写面，通用写面不
  收）；其余 arm（StoryDistributionList/CallLink/ChatFolder/
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
  sendMessageRequestResponse）终态仍以写调用结果为准（引擎亦无
  sendMessageRequestResponse 方法）。② 引擎不落会话摘要表、不做
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
  另行立项）；不做会话级持久化/查询面；不动 vendored proto；不做
  threadE164 面（上游已 reserved 废止）；协议栈无读写面的能力不顺手做。

## 6. fake 服务模式（仅测试）

`KT_ENGINE_FAKE_SERVICE=1` 显式开启（生产缺省永远走 real）：P1a 形状桩
原样保留——`startLink` 回夹具形状 URI、`finishLink` 合成 `+1555000xxxx`
号码、send 系方法只落调度日志不触网。用途：IPC 契约测试在无网络环境下
钉定连接器可见的形状/错误码/日志纪律（`tests/contract.rs`）。该开关不是
运行模式切换，连接器生产环境禁止设置。
