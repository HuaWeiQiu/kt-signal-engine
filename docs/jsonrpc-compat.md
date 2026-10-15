# JSON-RPC 兼容层契约（signal-cli 兼容子集）

状态：P2 起逐方法对齐基线，随批增补 ｜ 依据：ADR 0002/0004 ｜ 上游参照：connector
`docs/implementation-plan.md` §4/§5 已固化语义 + `tests/fixtures/fake-signal-cli.py`
（黄金对照，pinned 0.14.x 行为投影；文件在 connector 仓，本仓契约测试
自带 fake 服务直驱引擎，不经该夹具）

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

- `--data-dir`：引擎全部本地状态根（真实引擎为 presage SqliteStore 目录，
  `accounts/lk-<hex>/` 每账号一库 + `engine-state.json` 注册表；注册表里不落
  除号码/目录名以外的任何内容）。
- `--receive-mode on-start`：daemon 就绪即拉取服务器消息（P1b 起 presage
  websocket 生效：每账号一条接收管线，断流退避重连）。
- `--ignore-attachments` / `--ignore-stories` / `--ignore-stickers`：接受但
  **恒为 no-op**——仅为 argv 形状兼容而存在（ADR 0002 argv 兼容小节），引擎
  收发行为与三旗标无关，不是 signal-cli 时代「丢弃贴纸/故事」语义。接收面现状：
  贴纸 metadata-only 投影（字节永不下载）、story transient 投影不落库（§5
  story 块）、附件 locator 指针投影（§5 附件 locator 勘误，metadata-only
  立项红线，字节按需归 connector §4.54 transit 链）。
- JVM 相关环境（`JAVA_OPTS`/`JAVA_HOME`）：引擎非 JVM，**必须忽略**，
  不得因此失败（connector 现行启动器会设置它们）。
- stdio：stdin 逐行请求，stdout 逐行响应/通知，stderr 除两处启动期输出外
  保持安静：网络路径 INFO 行（`kt-signal-engine: INFO network path:
  <direct|explicit-env|std-env|system-http|system-socks> <地址或说明>`，恰
  一条）与 `KT_SIGNAL_SOCKS_PROXY` 非法时的错误行（exit 2 前打印）。connector
  watchdog 只把「websocketioexception 或 websocket+失败关键词」形状的行判为
  接收面死活信号，INFO 行刻意避开该形状；除此之外引擎不得再写 stderr。
  日志纪律：不落消息正文/号码/URI/密钥（代理 host:port 属网络配置，允许）。
- 退出码：stdin EOF（含宿主粗暴断开）= 干净退出 0；无法解析的致命状态
  非零。未知参数/缺 `jsonRpc` 子命令 = 非零快速失败（与上游对未知参数
  fail-closed 同口径）。

### 1.1 出口代理解析顺序

引擎启动同步段（任何 libsignal-service reqwest client 构建之前）按以下优先级
解析出口代理，**首个命中生效**，并在 stderr 打一条网络路径 INFO 行（见 stdio
条目）说明最终路径与具体地址，供排障对账：

1. **显式环境** `KT_SIGNAL_SOCKS_PROXY=host:port`：connector 注入的出口代理
   （与 signal-cli JVM 形态同一变量）。校验通过映射为
   `ALL_PROXY=socks5h://host:port`（socks5h = 域名在代理侧远端解析，防本地
   DNS 污染把 CONNECT 指到假 IP；早期文档误写 `socks5://`，以本条为准）。
   **覆写关系**：该变量命中时无条件覆写已存在的 `ALL_PROXY`（显式配置优先）。
   非空且非法启动即 exit 2（fail-closed，与上游对未知参数同口径）；空值视为
   未配置，落到下一级。日志标记 `explicit-env`。
2. **标准环境** `ALL_PROXY`（大写优先）否则 `all_proxy`：scheme 白名单
   `socks5://`、`socks5h://`、`http://`（scheme 大小写不敏感），命中即生效。
   引擎**不改写**该变量——reqwest 传输面原生读取同一变量、同一语义，日志
   标记 `std-env` 并回显原值。白名单外 scheme（`https://`/`socks4://` 等）或
   含空白的值不被本链采纳，引擎不采纳也不清除（reqwest 原生层对它的处理不受
   本链约束，日志按链决策输出）。
3. **系统代理**（仅 macOS；其他平台跳过此步）：spawn `scutil --proxy` 解析
   系统配置——`HTTPEnable=1` 且 `HTTPProxy`/`HTTPPort` 合法 → 映射为
   `ALL_PROXY=http://host:port`（日志 `system-http`）；否则 `SOCKSEnable=1`
   且 `SOCKSProxy`/`SOCKSPort` 合法 → 映射为 `ALL_PROXY=socks5h://host:port`
   （日志 `system-socks`）。HTTP 与 SOCKS 同时启用时优先 HTTP（http 代理可经
   CONNECT 覆盖 https/wss 流量）；某路使能但字段缺失/畸形（host 空·含空白·
   含 `:`·超 253 字节，端口非数值或 0）→ 该路跳过落到另一路，两路皆废 →
   降级下一级。scutil 进程失败（spawn 失败/超时 2s 被杀/非零退出/输出非
   UTF-8）整体降级下一级，**不 panic、不 exit**。与官方桌面一致：系统代理
   开关由用户在系统设置里拨，引擎零配置跟随。
4. **直连**：以上皆未命中（日志 `direct`）。

被选中的显式/系统代理统一经 `ALL_PROXY` 生效：libsignal-service 的 reqwest
传输面（HTTP + WebSocket 共用同一 client，缺省读环境代理面）统一走该出口；
要求依赖图含 reqwest `socks` feature（本仓显式声明）。reqwest 原生还会读取
`HTTPS_PROXY`/`HTTP_PROXY`/`NO_PROXY`（逐 scheme 优先级高于 `ALL_PROXY`，
`REQUEST_METHOD` 在环境时整体禁用 env 代理——hyper-util 语义，现状保持，
引擎不改写不拦截这些变量）。

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
| `version` | `{}`（零参数，params 忽略——wire 宽容，generateInviteLinkPassword 同口径；账号无关，未绑定账号照常应答） | `{name, version}`（双字符串键恒在：name = 引擎本名 `kt-signal-engine`、version = 引擎本仓版本——诚实自报不冒充上游名；libsignal 基线不在本面，`engine.protocolCapabilities.baseline` 所有，不自造字段） | 无（只读常量应答，无业务错误面；fake/real 同码路真实答） | **2026-10-14 real（白名单补齐批，wire additive，engine-compat）**：上游 signal-cli JSON-RPC 元方法——connector `runtime.doctor`（§4.79）版本探针的引擎腿，doctor 以空参/只读车道/3s 诊断预算调用并把应答**原样透传**（verbatim，不 re-key 不归一，doctor-contract.md 钉定）；缺该方法的引擎被 doctor 按「上游错误类」降级（`present:false` + 固定理由文案「an engine that does not carry it surfaces the upstream class」冒上桌面——真机验收发现的根因面）；读面不落调度日志零 stderr；fake：同码路真实答 |
| `startLink` | `{}` | `{deviceLinkUri}`（real：官方 `sgnl://linkdevice?uuid=…&pub_key=…` + **单个** `capabilities=nopni,nopni2,backup5` 参数，`qr_link_url` 收敛面） | — | **P1b real**：presage provisioning（设备名先占位，§5）；fake：夹具形状桩。**2026-10-15 real（转移弹窗修复批，engine-compat）**：capabilities 收敛为单参数并保证 `backup5` 在位——此前 `link_uri`（nopni,nopni2）+ `append_pair`（backup5）产出两个同名参数，Android 扫码侧 `Uri.getQueryParameter`（单数）只取第一个，`backup5` 被吞 → 手机判 VALID_WITHOUT_SYNC：链接成功但不弹「转移聊天记录」、不上传迁移档案 |
| `finishLink` | `{deviceLinkUri, deviceName}` | `{number, aci?}`（aci additive 2026-10-13，connector 契约 1.142 消费面，§5 批） | `-1` `Unknown device link uri.` | **P1b real**：等手机确认（阻塞不饿死其他方法），设备名本地改写 + attributes 推送（ADR 0004）；fake：形状桩 |
| `listAccounts` | `{}` | `[{"number", "aci"?}]`（aci additive 2026-10-13，connector 契约 1.142 消费面，§5 批）；无账号时空数组 | — | **P1b real**：state v2 注册表（号码→store 目录） |
| `listContacts` | `{account}` | `[{number, name?}]`（store 同步面；未注册账号 `-32603`） | 未注册账号 `-32603` | **P2 real**：presage contacts 投影；profile 拆名 P2+ |
| `listGroups` | `{account, groupId?: [32B 标准 base64 群 master key…]（可选过滤，P15b 审计项5 单群读投影：数组非空、条目 decode_group_id 闸；坏形状——非数组/空数组/条目非字符串·坏 base64·≠32B——fail-closed `-32603`（fake 面同口径）；well-formed 但 store 不在 → 掉出结果不报错（结果可空，connector fold 侧 `first()` → 空集诚实降级）；缺席/null = 全表旧行为）}` | `[{id(base64), name, revision, description?(Some 时在场), announcementsOnly, expireTimerSeconds(0 = 关), isMember(计算值：own ACI ∈ members), members:[{number, role}], pendingMembers:[{number, addedBy, timestamp}], requestingMembers:[{number, timestamp}], bannedMembers:[{number, timestamp}], accessControl?{attributes, members, addFromInviteLink, memberLabel}(词表 UNKNOWN/ANY/MEMBER/ADMINISTRATOR/UNSATISFIABLE，Some 时在场), terminated(bool)}]` | 未注册账号 `-32603` | **P2 real**：presage 群组投影（id = master key 标准 base64，send 可回传）；`terminated` 位 2026-10-06 扩（KT 扩展批：进程内登记 + 服务器群态读面，§5 群终止批；fake 恒空列表该键不出现）；**2026-10-11 real（群写面 P5 投影批，§5 块）**：投影族扩展（description/announcementsOnly/expireTimerSeconds/revision/members[].role/pending/requesting/banned 三成员面/accessControl 四族——vendored presage `Group` 补 `banned_members` 位，README fork 清单登记）；**isMember 诚实化修正**：改计算值（own ACI ∈ members）——早期「入库即成员恒 true」口径废止，被移出群的本地残留投影即 false（§5 登记偏差）；单群投影抽纯函数 `project_group_list_entry` 单测钉形；**2026-10-11 real（P15b 审计项5，§5 块）**：可选 `groupId` 过滤（单群读投影）——connector §4.53/§4.57/§4.62 三处 fold 读已派发 `listGroups {account, groupId: [id]}` 数组形并取 `first()` 当目标群，引擎此前忽略该参数返回全表，多群账号下 `first()` 读到任意群（requesting/pending 集合错群危害）；过滤判定抽纯函数 `group_key_passes_filter` 单测钉形 |
| `deleteLocalAccountData` | `{account}` | `{}` | — | **P1b real**：注册表摘除 + store 目录删除 + 接收管线停机 |
| `send` | `{account, recipient[]/groupId, message, quoteTimestamp?, quoteAuthor?, quoteType?("normal"\|"giftBadge" 缺省 normal——官方 isGiftBadge 同构；六问批 Q5), quoteBodyRanges?: [{number?, style?(BOLD\|ITALIC\|SPOILER\|STRIKETHROUGH\|MONOSPACE), start(u32), length(u32)}] ≤64（mention/style 恰一；两键须挂 quote 键对——quote 对象缺席无处挂载；六问批 Q5）, previewUrl?, previewTitle?, previewDescription?, previewImage?, previewDate?(u64 毫秒发布时刻——P14 Preview.date 两端批；须挂 previewUrl 键对，0 归缺席，非整数 `-32603`), mentions?: [{number, start, length}], textStyle?: [{style(BOLD\|ITALIC\|SPOILER\|STRIKETHROUGH\|MONOSPACE), start(u32), length(u32)}], attachments?: [dataURI], sticker?: {packId(hex), packKey(base64), stickerId, emoji?, image(dataURI), width?, height?}, storyReplyAuthor?, storyReplyTimestamp?(u64 毫秒——P14 组 4 storyContext 发送面；**双参同进退**：同置=story 回复（author ACI/号码，服务层解析不可解析 `-32603`；timestamp ≤0·非整数 `-32603`），单边在场 `-32603`——官方 storyContext 同位 SendMessage.preload.ts:565-577), viewOnce?}` | `{timestamp, results:[]}` | 收件人未解析/群不可解/quote 半参/quote 在场但 author 不可解析（文本与贴纸分支同口径）/storyReply 双参单边在场·author 缺席或空串·timestamp 缺席·≤0·非整数/storyReplyAuthor 不可解析（服务层 resolve_author_aci）/quoteType 词表外（含 poll——官方 Quote.Type POLL 枚举在，KT quote 发送面未扩，窗D A7 批记档）·非字符串、quoteBodyRanges 条目 mention/style 同置或都缺·缺 start/length·start/length 超 u32·style 词表外·>64 条（cap 镜像整拒不截断）·非数组、quoteType/quoteBodyRanges 与 quote 键对脱挂/mentions 条目缺字段/textStyle 条目词表外（五值白名单；NONE 一并拒——官方 composer 永不发出的 no-op 标记）·缺字段·style 非字符串·start/length 超 u32·>64 条（cap 镜像整拒不截断）·非数组/附件条目非字符串/sticker 缺字段或与 message·attachments·preview·mentions 同发（官方互斥；quote 与 sticker 可共存）/viewOnce 非布尔，或 viewOnce:true 违反官方 isValidTapToView 同构（body 非空·附件≠1·附件 mediatype 非 image\|video·与 quote·preview·mentions·sticker 同发——mentions 亦禁发，比官方更严）`-32603`；群组形态目标已终止 `SEND_TARGET_TERMINATED`（`-1` + message，2026-10-06 群终止批，§5）；announcements-only 群非 admin 发送系整拒 `SEND_TARGET_ANNOUNCEMENTS_ONLY`（`-1` + message，2026-10-11 群发送语义批，§5）；账号解绑（服务端 401 凭据拒绝）`-32603` + getUserStatus 同款解绑原文（2026-10-10 凭据失效归一批，`SendError::Unbound`，确定性失败不重试，§5）；服务端限流（429/413）`-32603` + `Rate limited` 原文、人机验证（428）`-32603` + `Proof required` 原文（两者不自动重试）、服务端不可用（5xx）`-32603` + `Signal server unavailable` 原文（建议可重试——重试决策均在 connector/desktop；2026-10-11 错误码结构化批，§5）；对端身份钥变更未确认（身份拒发闸：协议层旧钥会话/换钥 bundle 拒加密 + 服务层 sidecar 挂起闸，直聊内容/撤回/回应/pin/unpin/直聊通话信令/回执面生效，typing/群发送/story 不在闸面）`-32603` + `Failed to send message: Identity unverified. (UntrustedIdentityException)` 原文（确定性失败不重试；确认钩子 = `trust` 写面，2026-10-11 P9 身份拒发批，§5） | **P2 real**：直聊逐号码 + 群组 `send_message_to_group`；quote id/author→ACI + text 本机 store 找回（signal-cli 同行为，找不到缺省）；preview 面 + previewImage 随附件同批 CDN 上行（官方 Preview.image 同构）；mentions→BodyRange.MentionAci；textStyle→BodyRange.Style 五值词表单点映射（2026-10-06 透传批，connector 契约 1.69 §4.62 引擎侧缺口闭合：UTF-16 offsets 透传不查 body 边界——官方 parseTextStyles 同口径，与 mentions 同款调用方纪律，详见 §5）；attachments 为 connector 上游唯一形状（data URI 数组，`data:<type>[;filename=<pct>][;voice=true][;gif=true];base64,<payload>`，voice=true 映射官方 VOICE_MESSAGE flags + 官方算法波形/时长生成面（`AttachmentPointer.audioWaveform=21`/`audioDurationSeconds=22`，生成失败两字段全不带照发，2026-10-14 批 §5；presage voice_note 同构，2026-10-04）；gif=true 映射官方 Flags.GIF=8 位（flags 纯客户端线上陈述 CDN 上行面零涉及，engine 侧上传返回指针置位与官方消息组装期置位同构，与 voice/borderless 按位叠加；2026-10-14 GIF 批，§5））→ presage 官方 upload_attachments 上行（≤32 条、单条 ≤100MB，超限 fail-closed）；sticker 贴纸面（2026-10-04，官方 sendStickerMessage 同构）：image data URI 上行（mediatype 必须 image/*，width/height 官方 AttachmentPointer 展示位透传），packId hex/packKey base64（官方 Bytes.fromHex/fromBase64 同构），body=undefined 无常规附件；sticker 与 quote 共存（2026-10-04，官方「贴纸回应」StagedStickerReply = 同一 DataMessage 带 sticker+quote，非 reaction——Reaction proto 无贴纸位）：quote 组装与文本分支同源（assemble_quote 单点），author 不可解析 fail-closed、被引正文 store 找回失败降级缺 text，body 维持 undefined；viewOnce 阅后即焚面（2026-10-04，官方 isViewOnce=14 同名直投）：`viewOnce: true` → body 空 + 恰 1 附件 + `is_view_once=Some(true)` 的 DataMessage（官方发送侧不 bump requiredProtocolVersion——构造面不设该字段，presage send 面注入默认 0 = 官方初值），直聊/群组两形态同构，与 disappearing messages 完全独立；2026-10-11 群发送语义批（§5）：发送前刷群态（typing 免刷、取态失败回落本地 fail-open）+ 群形态 `results` 逐成员填充（成功/失败条目与直聊同构，type 四词表 additive）+ transcript 无论成败一律落库（群黑洞消除）+ 单成员 profile 行不可读降级 identified 不炸批；**2026-10-11 六问批 Q5（§5）**：quoteType/quoteBodyRanges 发送扩面——vendored SendQuote 扩 `gift_badge`/`body_ranges` 位（Quote.type=GIFT_BADGE 枚举 1/Quote.bodyRanges 字段 6——原文「Quote.type=2」系笔误，窗D A7 批订正），assemble_quote 单点同写 type（恒写 NORMAL/GIFTBADGE）与 BodyRange 条目，官方发送侧不 bump requiredProtocolVersion（构造面不设该字段，viewOnce 先例同构）；**2026-10-11 P15b 批（时序审计组二#9，§5，wire additive）**：直聊腿 per-recipient 成败收集——不再 fail-fast 丢弃部分成功（重试全批会对已达收件人重复投递）；混合结局（既有成功又有失败）返回 `{timestamp, results(成功条目原样), failures: [{recipientNumber, error: {code, message}}]}`（failures 空表键不在，旧宿主零变化；error 分类与整批错误面逐字同源——send_error_face 分类单点）；全败维持整批错误面（首错分类上抛，零已达收件人整批重试无重复风险）；附件/贴纸图上行失败仍整批早退（批级共享指针，未进逐收件人面）；群腿/editMessage/群发送系不变 |
| `editMessage` | `{account, message(必填非空——官方 canEditMessage body 非空闸), targetTimestamp(u64 必填——被编辑消息原 sent timestamp), recipient[]/groupId(32B 标准 base64 群 master key，listGroups id 同源)}`（**两腿恰一**（sendMessageRequestResponse 同构）：同置/都缺/腿形状非法（recipient 空批·条目非字符串·groupId 非 base64·≠32B）`-32603`） | `{timestamp, results:[]}`（同 send 形状，群组 results 逐成员填充） | 参数缺失/形状错（message 空·缺席·非字符串、targetTimestamp 缺席·非数字、两腿同置/都缺、recipient 形状非法、groupId 非 base64·≠32B）`-32603`；未注册账号/身份拒发闸/announcements-only 群/群已终止 同 send 折叠；24h 窗口/10 次上限是官方 UI 层闸（canEditMessage），wire 面不设 | **2026-10-11 real（KT 扩展，六问批 Q1，官方编辑发送面对位，engine-compat）**：官方编辑 = `Content.editMessage{targetSentTimestamp, dataMessage}`（SendMessage.preload.ts:1075-1092，内层 dataMessage 只携新 body+timestamp）；直聊腿 = vendored `send_message_with_profile_sharing` 逐收件人（P9 S3 profileSharing 闸 + 身份拒发闸沿 send_text 同纪律）；群腿 = vendored `send_message_to_group`（MRM 主路径只支持 DataMessage，EditMessage 不在其列）+ 内层 DataMessage 手注 group_v2（master key + revision——revision 经 fetch_group_with_endorsements best-effort，失败 None proto 缺席合法）；多设备转录：vendored create_multi_device_sent_transcript_content 已把 EditMessage 转 `Sent.editMessage`，本机其他设备收 `syncMessage.sentMessage.editMessage {targetSentTimestamp, dataMessage}` echo（§3 receive 面）；详见 §5 六问批；fake：成功 `{timestamp, results:[]}`（落调度日志，协议上行面纪律） |
| `sendStory` | `{account, message(非空——文本腿；attachment 在场时禁入，两腿恰一), recipients(非空字符串数组——名单展开集，成员解析归 connector), distributionListId?(非空字符串，缺省官方 MY_STORY_ID 零 UUID), allowsReplies?(布尔，缺省 false=线上 proto 缺席位), textStyle?(单词 DEFAULT\|REGULAR\|BOLD\|SERIF\|SCRIPT\|CONDENSED，缺省 DEFAULT=proto 字段省略——text 腿专属，attachment 在场禁入), bodyRanges?: [{style(BOLD\|ITALIC\|SPOILER\|STRIKETHROUGH\|MONOSPACE), start(u32), length(u32)}] ≤64（两腿共用）, previewImage?(非空 data URI——text 腿专属，attachment 在场禁入), previewUrl?(非空 ≤2048 字符且 https:// 开头——text 腿专属，attachment 在场禁入), previewTitle?(非空 ≤1024 UTF-8 字节——text 腿专属，attachment 在场禁入), previewDate?(u64 毫秒发布时刻——P14 Preview.date 两端批；须挂 previewUrl/previewTitle/previewImage 任一键对，0 归缺席，非整数 `-32603`——官方 sendStory hydratedPreview 同位透传；text 腿专属，attachment 在场禁入), attachment?(非空 data URI 单条——媒体 story 正文附件，2026-10-14 批；connector 上游附件形状同款 `data:<mediatype>[;filename=<pct>][;voice=true][;gif=true];base64,<payload>`，深解析/帽值归服务层上行闸；在场 = 文件 story 腿，缺席 = text story 现状), caption?(非空 ≤65536 UTF-8 字节——媒体 story caption，2026-10-15 批；官方 MAX_BODY_ATTACHMENT_BYTE_LENGTH 64KiB 同源字节面（longAttachment.std.ts:8，CompositionInput.dom.tsx:686-692 Buffer.byteLength 同构）；媒体腿专属——官方 caption 进 AttachmentPointer.caption=11（proto:940），textAttachment 无 caption 位，attachment 缺席禁入), group?(非空字符串——engine groupId 群 master key 标准 base64，与 sendTextGroup 群 id 同一口径，2026-10-15 批；32B 形状闸归服务层 decode_group_id；在场 = 群受众腿，与名单腿/内容两腿正交——官方受众双域共存 sendStoryMessage.preload.ts:40-63；recipients 可空当且仅当 group 在场 = 群单腿，group 缺席时空批维持现状整拒)}` | `{timestamp, results:[{recipientNumber, success:{timestamp}}], group?:{groupId, timestamp, fileAttachment?}, fileAttachment?}`（group 键 = 群腿应答面 2026-10-15 批 additive，缺席键不在；群单腿时 timestamp = 群腿 ts、results 恒空；fileAttachment = 媒体 story 指针回传批 2026-10-15 additive——媒体腿上传成功时该次上传 AttachmentPointer 全脸，名单腿 = 顶层键、群腿 = group.fileAttachment（混合受众两腿独立上传指针互异故分键），值形状与收投影 fileAttachment 脸同构（同一投影函数严格互逆，locator 面 key/cdnKey/cdnNumber/uploadTimestamp/digest）；text 腿/发送失败键缺席（no-null：缺席绝不序列化）；fake results 恒空 + 媒体腿合成桩脸）| account/message 缺失或空串（双缺 = 无载荷整拒）/recipients 缺失·非数组·单字符串形态·条目非字符串·空串/空批且 group 缺席/distributionListId 空串·非字符串/allowsReplies 非布尔/textStyle 词表外（含内联样式词与大小写漂移）·非字符串/bodyRanges 条目缺字段·style 词表外·>64 条（cap 镜像整拒不截断）·非数组/previewImage 非字符串·空串/previewUrl 非字符串·空串·非 `https://` 开头·>2048 字符/previewTitle 非字符串·空串·>1024 UTF-8 字节/attachment 非字符串·空串/attachment 与 message·textStyle·previewImage·previewUrl·previewTitle·previewDate 同置（两腿恰一，官方 oneof 同构）/caption 非字符串·空串·超 65536 UTF-8 字节/attachment 缺席且 caption 在场（媒体腿专属，官方 textAttachment 无 caption 位）/group 非字符串·空串`-32603`；名单腿：号码不可解析/收件人发送失败/未注册账号 同 send 折叠 `-32603`（现状面零变化）；群腿：groupId 非 32B base64/群已终止（`-1`+`SEND_TARGET_TERMINATED`）/announcements-only 非 admin（`-1`+`SEND_TARGET_ANNOUNCEMENTS_ONLY`）/群态不可得/成员发送失败 同群发送结构化错误面（send_error 单点）| **2026-10-08 real（KT 扩展，Stories 发送批）**：官方 sendStory 行为对齐——text story 单面（群 story 边界保留；文件 story 由 2026-10-14 媒体 story 批解除，见后）；textAttachment{text, textStyle, preview.image=previewImage 随批 CDN 上行} 构造 + profileKey 由 fork send 面注入；preview 面 2026-10-09 扩：previewUrl/previewTitle 如实进 proto preview.url/preview.title（两键互相独立可选，image/url/title 三件任一在场即成 Preview——官方 preview 面 image 可空同构 `hydratedPreview.image && upload`；接收面投影见 Stories 接收批）；解析全量前置→按解析后 ServiceId 去重保首现序→单时间戳逐收件人 sealed-sender `?story=true` 分发（任一失败整体 `-32603`，per-recipient 部分失败形状未对齐纪律 §5）→≥1 成功后 best-effort 自同步 `SyncMessage.Sent{story_message, story_message_recipients}`（官方「≥1 成功且有其他设备」同闸，同步失败不回滚）；urgent 位钉定 rev proto 无此字段（偏差登记 §5）；详见 §5 Stories 发送批；**2026-10-14 real（KT 扩展，媒体 story 发送面批，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat，§5 块）**：`attachment` 可选参数扩文件 story 腿——E1 批「文件 story v1 不做」边界翻案（理由记档 §5）；官方 `StoryMessage.attachment` oneof fileAttachment=3\|textAttachment=4 双臂（protos/SignalService.proto:476-479），media 腿 = sendStory.preload.ts:186-190 同构：attachment data URI 走既有 transit CDN 上行链（presage upload_attachments 官方原语），返回指针原样进 StoryMessage.fileAttachment（官方 getStoryMessage SendMessage.preload.ts:1043-1044 同位），bodyRanges 两腿共用（官方 :1059-1061 fileAttachment 分支同载）；两腿恰一（同置/text 专属键越腿 -32603）；attachment 缺席 = text story 现状零变化；与接收投影 fileAttachment locator 全脸严格互逆（往返单测钉定 §5）；自同步 story_message 载同一 story 值恰好一次不变；vendored proto/fork 发送路径零改动；群 story（GroupContextV2 位）边界保留；**2026-10-15 real（KT 扩展，群 story 发送面批，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat，§5 块）**：`group` 可选参数扩群受众腿——E1 批「群 story v1 不做」边界翻案（理由记档 §5）；官方发送面 = sendStoryMessage.preload.ts:40-63 受众双域（名单 ∧ 群，同一 send 共存）+ 群候选三闸（isGroupV2 :210/非 terminated :218/announcementsOnly 需 admin :226-232）；线格式 = `Content.storyMessage`（protos/SignalService.proto:115）内 `StoryMessage.group = GroupContextV2{masterKey=1, revision=2, groupChange=3}`（:474/:947-951，官方组装 SendMessage.preload.ts:1054-1060，groupChange 恒缺）；群腿 allowsReplies 恒 true（sendStory.preload.ts:199/:352-354，allowsReplies 参数只辖名单腿）；混合受众 = 名单腿 ts + 群腿 ts+1 官方 +index+1 同款、两 sync 官方两 job 同构（sendStoryMessage.preload.ts:327-332/:337-378）；群 sync 形状 = 空名单位 + 恒可回位 + destination 缺席（sendStory.preload.ts:587-589）；recipients 空批 + group 在场 = 群单腿（官方受众至少一域 :57-63 同构）；group 缺席 = 现状零变化；**2026-10-15 real（KT 扩展，媒体 story caption 批，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat，§5 块）**：`caption` 可选参数扩媒体腿——S6a 桌面媒体 story 批记档「引擎 media 腿无 caption 键」边界翻案；官方 caption 进 `AttachmentPointer.caption=11`（protos/SignalService.proto:940，uploadAttachment.preload.ts:99/:134 `caption || null` 随上传返回指针同载），帽 = 官方 MAX_BODY_ATTACHMENT_BYTE_LENGTH 64KiB 字节面；上行路径 = presage `AttachmentSpec.caption` spec 面直通返回指针（vendored sender.rs spec.caption→pointer.caption，零 vendor/fork 改动）；仅 attachment 腿可达（textAttachment 无 caption 位）；应答形状零变化，详见 §5 媒体 story caption 批；**2026-10-15 real（KT 扩展，媒体 story 指针回传批，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat，§5 块）**：sendStory 应答增可选 `fileAttachment` 键——媒体腿上传成功时该次上传 AttachmentPointer 全脸回传（官方发送端持有上传指针同事实面，uploadAttachment.preload.ts 返回值即发送态指针；KT 桌面 own 媒体 story 重启后无字节来源的必要充分路径——engine 零存档零自收、sync 只上 wire 到其他设备发送设备不自收，指针进应答 = 本机回看唯一字节面入口）；键名 fileAttachment 裁定 = 官方 StoryMessage oneof 臂名（protos/SignalService.proto:476-479）+ 与收投影 fileAttachment 脸同键同构（同一投影函数严格互逆，connector/桌面直投零翻译）；值形状全脸（id/contentType/filename/size/caption/blurHash/incrementalMac/chunkSize/isVoiceNote/isBorderless/isGif/waveform/duration + locator{key base64, cdnKey, cdnNumber, uploadTimestamp, digest base64}，缺席键不在——locator 无 plaintextHash：官方 proto 无该位（本地 TS 上传对象私有面不进 wire），诚实缺席）；指针共享面实读 = 名单腿单次上传单指针全体收件人共享（story.clone() 同一 fileAttachment）、群腿同构、混合受众两腿独立上传指针互异 → 顶层键（名单腿）+ group.fileAttachment（群腿）分键，群单腿顶层缺席；text 腿/发送失败键缺席（组装单点 send_story_success_body no-null：缺席与 null 折叠同一无键态，缺席 = pre-批应答逐字节同形）；fake = 合成桩脸镜像文档形状（cdnKey=fake-story-attachment/cdnNumber=3/uploadTimestamp=桩时刻/caption 入参在-cap-out，不发明 key/digest 加密材料）；详见 §5 媒体 story 指针回传批 |
| `remoteDelete` | `{account, targetTimestamp, recipient/groupId}` | `{}` | 同 send | **P2 real**：直聊 + 群组 |
| `sendReaction` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, emoji, remove}` | `{}` | 同 send | **P2 real**：targetAuthor 号码↔ACI 解析（本机号走注册数据，contacts 命中 E.164，UUID 形状直通） |
| `updateContact` | `{account, recipient(单字符串), name}` | `{}` | 未命中号码：`-32603` | **P1b real**：本地 store 命中即改名；服务器面 P2 |
| `sendTyping` | `{account, recipient[]/groupId, stop(显式布尔)}` | `{}` | 同 send | **P2 real**：TypingMessage Started/Stopped 直聊 + 群组 |
| `sendPinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, pinDurationSeconds?}` | `{}` | 同 send；pinDurationSeconds 超 u32 `-32603` | **P2 real**：官方 pinMessage=27；字段缺席/null=pinDurationForever |
| `sendUnpinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp}` | `{}` | 同 send | **P2 real**：官方 unpinMessage=28 |
| `sendAdminDelete` | `{account, groupId, targetAuthor, targetTimestamp}` | `{}` | 无 groupId `-32603`（直聊无 admin 概念） | **P2 real**：官方 adminDelete=29，仅群组 |
| `sendPollCreate` | `{account, groupId(32B 标准 base64 群 master key，参数层 decode_group_id 闸——fake 面同口径), question(非空 ≤200 字符 + 字节 ≤2048), options(2..=10 条字符串、每条非空 ≤100 字符 + 字节 ≤2048), allowMultiple?(布尔，缺省 false)}` | `{}` | 参数缺失/形状错（groupId 缺席·非 base64·≠32B、question 缺席·空·超 200 字符·超 2048 字节·非字符串、options 缺席·非数组·<2·>10·条目非字符串·空串·超 100 字符·超 2048 字节、allowMultiple 非布尔）/未注册账号 同 send 折叠；announcements-only/terminated 群 同群发送系（`-1` + message） | **2026-10-11 real（KT 扩展，窗D A7，官方 poll 发送面对位）**：官方 `enqueuePollCreateForSend.dom.ts` → `DataMessage.pollCreate{question, allowMultiple, options}`（field 24，SendMessage.preload.ts toProto pollCreate 同构）走 dispatch_group MRM 主路径；官方仅群聊形态（1:1 走 isPollSend1to1Enabled feature flag，默认关——引擎按默认面无 1:1 形态，§5）；接收投影 `dataMessage.poll` 见 §3 receive 面；fake：成功 `{}`（落调度日志） |
| `sendPollVote` | `{account, groupId(同 sendPollCreate 闸), targetAuthor(ACI/号码——被投 poll 的作者), targetTimestamp(正整数——被投 poll 原 sent timestamp), optionIndexes(u32 数组、每项 0..=9、≤10 条，空数组 = 清票动作合法), voteCount?(u32，缺省 1)}` | `{}` | 参数缺失/形状错（groupId 闸、targetAuthor/targetTimestamp 缺席·非正·非数字、optionIndexes 缺席·非数组·条目>9·条目非数值·>10 条、voteCount 超 u32·非数值）/未注册账号·群发送系闸 同 sendPollCreate | **2026-10-11 real（窗D A7 同批）**：官方 `enqueuePollVoteForSend.preload.ts` → `createDataMessageProtoForPollVote`（`DataMessage.pollVote{targetAuthorAciBinary, targetSentTimestamp, optionIndexes, voteCount}` field 26）走 sendToGroup 群腿同构（poll 只存在于群会话）；voteCount 官方语义 = 自身历史票 max+1（首票 1），由调用方（connector/desktop 持投票历史）计算传入，引擎缺省 1（§5 偏差）；optionIndexes 去重保首现序（发送幂等护栏，官方接收面 new Set 同构）；fake：成功 `{}`（落调度日志） |
| `sendPollTerminate` | `{account, groupId(同 sendPollCreate 闸), targetTimestamp(正整数——被终止 poll 原 sent timestamp)}` | `{}` | 参数缺失/形状错（groupId 闸、targetTimestamp 缺席·非正·非数字）/未注册账号·群发送系闸 同 sendPollCreate | **2026-10-11 real（窗D A7 同批）**：官方 `enqueuePollTerminateForSend.preload.ts` → `createDataMessageProtoForPollTerminate`（`DataMessage.pollTerminate{targetSentTimestamp}` field 25）；官方拒 GroupV1（isGroupV1 refus）——引擎群腿形状闸只收 GV2 32B master key，GV1 天然不可达（零偏差）；poll 终止者资格（creator/admin）是官方 UI 层闸，wire 面不设；fake：成功 `{}`（落调度日志） |
| `sendContact` | `{account, recipient(单号码/ACI)?\|groupId(32B 标准 base64 群 master key)?——寻址两腿恰一互斥, contact(名片对象，必填)}`（contact = 官方 EmbeddedContact/DataMessage.Contact 同构：`name?{givenName?, familyName?, prefix?, suffix?, middleName?, nickname?}`、`number?[{value(必填非空), type?("HOME"\|"MOBILE"\|"WORK"\|"CUSTOM"), label?}]`、`email?[同四值词表]`、`address?[{type?("HOME"\|"WORK"\|"CUSTOM"), label?, street?, pobox?, neighborhood?, city?, region?, postcode?, country?}]`、`avatar?(dataURI 单条)`、`organization?`——子键全可选但 contact 键本身必填；子键缺席/null 归缺席，organization 空串=官方 falsy 同位归缺席） | `{timestamp}`（信封时间戳） | 参数缺失/形状错（contact 缺席·非对象、name 非对象/子字段非字符串、number/email/address 非数组/条目非对象/value 缺席·空串·非字符串/type 词表外·type 非字符串/label·address 子字段非字符串、organization/avatar 非字符串、两腿同置/都缺、groupId 非法 base64/≠32B/非字符串、recipient 空串）`-32603`；未注册账号/avatar 上行失败 同 send 折叠 `-32603`；群腿目标已终止 `SEND_TARGET_TERMINATED`、announcements-only 群非 admin `SEND_TARGET_ANNOUNCEMENTS_ONLY`（`-1` + message，群发送系同口径） | **2026-10-12 real（KT 扩展，窗D B11，官方名片发送面对位，engine-compat）**：官方 `sendMessage {contact: [EmbeddedContactWithUploadedAvatar]}` → `DataMessage.contact=9` repeated 单条目投影（SendMessage.preload.ts:418-492 toProto contact 段同位）走 1:1 `send_message`（P9 身份拒发闸在闸面）/群 dispatch_group MRM 主路径（poll 群臂同款）；type 词→枚举单点映射（phone/email 两枚举同编号同词表共用；缺省投影 HOME=官方 numberTo*Type(undefined) 同位；词表外官方静默归 HOME，引擎按 textStyle 先例拒绝——偏差 §5 记档）；avatar data URI 随批 CDN 上行取回指针进 Contact.Avatar（官方 EmbeddedContactWithUploadedAvatar 上传面同构，isProfile 恒 false=引擎 wire 面无该键的官方默认值）；调度日志 contact.avatar 摘要化（调度零头像字节纪律）、其余键逐字保留；接收投影 `dataMessage.contact` 卡片面已有（Q2）；详见 §5 窗D B11 块；fake：成功 `{"timestamp":u64}` 两腿同口径（落调度日志） |
| `sendDeliveryReceipt` / `sendReadReceipt` / `sendViewedReceipt` | `{account, recipient(单作者), timestamps[]}` | `{}` | timestamps 空/缺/含非数字 `-32603` | **P2 real**：官方 ReceiptMessage type=delivery/read/viewed；逐作者直发（群组会话也直发原作者，官方不整组广播）；timestamps 按 CHUNK_SIZE=100 分帧 |
| `sendViewOnceOpen` | `{account, senderAci(ACI/号码), timestamp(原消息 upstream 时间戳)}` | `{}` | 同 send；senderAci 不可解析/参数缺失或形状错 `-32603` | **P2 real（2026-10-04 阅后即焚扩面）**：官方 syncViewOnceOpen 同构——向自己 ACI 发 SyncMessage.ViewOnceOpen{senderAci, timestamp}（content oneof viewOnceOpen=11，padded；多设备同步「该阅后即焚已被本机打开」），presage self-send 通路验证可行（§5，含钉定 rev 同内容二次转录重发怪癖）；与 sendViewedReceipt（作者面回执）双通道独立；fake：成功 `{}` |
| `markRead` / `markViewed` | `{account, reads?\|viewed?: [{sender(ACI/号码), timestamp(原消息 upstream 时间戳)}]}`（markRead 用 `reads` 键、markViewed 用 `viewed` 键——官方 syncReadMessages/syncView 参数名同形；条目非空数组，列表缺席/空/非数组/条目缺 sender 或 timestamp/timestamp 非数字/sender 非字符串 `-32603`） | `{}` | sender 号码/UUID 不可解析（contacts 兜底后仍失败）`-32603`（整拒不发半批——发送面参数闸纪律）；未注册账号 `-32603` | **2026-10-11 real（KT 扩展，P14 组 6 自发多设备读态写面）**：官方 syncReadMessages/syncView 同构（SendMessage.preload.ts:2097/:2145）——向自己 ACI 发 SyncMessage.read=5/viewed=16（直接 repeated 字段，非 content oneof，多设备同步「这些消息已读/已看」）；ACI binary+string 双写（官方 isProtoBinaryEncodingEnabled 现行面/旧字符串面双兼容，viewOnceOpen 双写先例同构）；100 条/chunk 分信封（官方 syncHelpers.preload.ts:25 CHUNK_SIZE=100 同构）；padded sync message；与 sendReadReceipt/sendViewedReceipt（作者面 ReceiptMessage 回执）双通道独立（官方 markConversationRead.preload.ts:252 注同位）；接收 echo 面 = §3 receive `syncMessage.read/viewed`（D2）；presage self-send 通路与 sendViewOnceOpen 同款；fake：成功 `{}`（落调度日志，reads/viewed 键逐字保留，协议上行面纪律） |
| `sendMessageRequestResponse` | `{account, recipient(单值，ACI/号码)\|groupId(32B 标准 base64 群 master key), type}`（**两腿恰一**：direct = recipient、群 = groupId，同置/双缺 `-32603`；type = 官方驼峰词六值白名单 `accept\|delete\|block\|blockAndDelete\|spam\|blockAndSpam`，缺席/`unknown`/数字/词表外/大小写漂移 `-32603`） | `{}`（上游钉定面成功答空结果对象同款） | 参数缺失/type 闸/两腿闸/群腿非 base64 或 ≠32B/recipient 数组 `-32603`；未注册账号 `-32603` | **2026-10-06 real（KT 扩展，§4.56 写面债务批）**：官方桌面 getMessageRequestResponseSync 同构——向自己 ACI 发 SyncMessage.messageRequestResponse{threadAciBinary\|threadAci?, groupId?, type}（content oneof messageRequestResponse=14；ACI 可解析 16B 直写 binary 字段=官方 isProtoBinaryEncodingEnabled 现行面，群 master key 原样=官方 groupId Uint8Array 同位；presage self-send 通路，padding 同源）；与 §3 receive 通知面 `syncMessage.messageRequestResponse` 投影对称（本机写 → 多设备 echo 收）；connector §4.56 `conversations.acceptRequest`/`declineRequest` 的引擎面（上游钉定 `sendMessageRequestResponse` params 逐字对齐：connector 上游 recipient 腿 = 其 peer_key 直通本引擎双形态解析、groupId 腿 = 群 master key、type 由宿主盖章 accept/delete）；写结局即返回值、未知结局不重试纪律不变（connector §4.56 SEND_OUTCOME_UNKNOWN 终态语义的引擎侧对应位）；fake：成功 `{}`（落调度日志，协议上行面纪律） |
| `getUserStatus` | `{account, recipient}` | `{"isRegistered": true}` | 解绑态 `-32603`（§3 头原文）；未注册账号/其他失败 `-32603` | **P2 real**：whoami 轻量鉴权探测（401→解绑原文，网络失败不误报解绑）；fake 恒已注册 |
| `getStickerPackManifest` | `{packId(hex 32 小写), packKey(base64 32B)}`（匿名 CDN 只读，无 account） | `{title, author, cover?: {id, emoji?, contentType?} \| null, stickers: [{id, emoji?, contentType?}], stickerCount}`（官方 parseStickerPackManifest 同构：≤1024 截断；cover 官方缺省 null 由端上兜底 stickers[0]；cover 与 stickers 都空 malformed） | 参数缺失/packId 非 32 位小写 hex/packKey 非 32B base64、解密 MAC 失败（与密文损坏密码学不可区分）→ `STICKER_PACK_KEY_INVALID`；CDN 失败 → `STICKER_PACK_FETCH_FAILED`；padding/proto 解码失败 → `STICKER_PACK_MALFORMED`；均 `-1` + message 码位（connector 1.36 白名单逐字，§4.32） | **P4 real**：cdn.signal.org `stickers/{packId}/manifest.proto` 匿名 GET + attachment_cipher 解密；fake：参数面 fail-closed 后 `STICKER_PACK_FETCH_FAILED` |
| `getStickerImage` | `{packId, packKey, stickerId}` | `{dataBase64, contentType, size}` | 同 getStickerPackManifest，另单图 >300KB → `STICKER_IMAGE_TOO_LARGE` | **P4 real**：`stickers/{packId}/{stickerId}` 匿名 GET + 解密 + WHATWG mimesniff image 魔数嗅探 contentType（未识别回退 `image/webp`）；fake 同 getStickerPackManifest |
| `getPinnedConversations` | `{account}` | `{pinned: [{conversationId, kind: contact\|group\|legacyGroup}]}`（存储序；存储条目映射不出引擎口径的跳过不炸面） | 未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P4 real**：Storage Service manifest → Account item → `AccountRecord.pinned_conversations` 投影；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{pinned: []}` |
| `setConversationPinned` | `{account, conversationId, kind: contact\|group\|legacyGroup, pinned(bool)}` | `{pinned: [...]}`（写后全量，同 getPinnedConversations 形状） | 云引用解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次/写面冲突重放中失败 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/未知 kind/未注册账号 `-32603` | **P4 real**：官方 upload 语义（读改写 AccountRecord 仅动 pinned 字段、prost round-trip 保真其余字段、新 16B item id、version+1/source_device/record_ikm 透传）+ `PUT v1/storage/` + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；写面为 presage 上游缺口自建薄封装（上游补齐后替换）；fake：成功回 `{pinned: []}` |
| `getStickerPackSyncs` | `{account}` | `{packs: [{packId(hex 32 小写), packKey(base64 32B)\|null, position, deletedAtTimestampMs}]}`（存储序 = manifest STICKER identifiers 首现序；坏条目跳过不炸面；墓碑 packKey=null、带 deletedAtTimestampMs） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real**：Storage Service manifest → 全部 type=6 STICKER identifier 批量读 → StickerPackRecord 投影（记录面为 presage 上游 proto 缺口本地定义 + 加解密 shim，§5）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；fake：恒 `{packs: []}` |
| `setStickerPackSync` | `{account, packId(hex 32 小写), installed(bool), packKey?(base64 32B), position?}`（install 必带有效 packKey；uninstall 忽略 packKey 内容与 position，官方墓碑形态两字段皆不写） | `{packs: [...]}`（写后全量，同 getStickerPackSyncs 形状） | PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（packId 非小写 hex、install 缺/短/非字符串 packKey、packKey 非字符串、position 超 u32 或非数值）/未注册账号 `-32603` | **P5 real**：官方 toStickerPackRecord 语义（install 幂等：同 key 已装且未要求挪位→no-op；position 缺省=保持现位/无现位本地 MAX+1；uninstall 墓碑 deletedAtTimestampMs=引擎时钟，不物理删旧 record）+ 读改写仅动该 pack record + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；fake：成功回 `{packs: []}` |
| `getConfiguration` | `{account}` | `{readReceipts, typingIndicators, linkPreviews, unlistedPhoneNumber, storiesDisabled, phoneNumberSharingMode?: "everybody"\|"nobody"}`（五布尔键恒在——false = 位缺省；phoneNumberSharingMode 词表内才投，UNKNOWN(0)/词表外 = 键缺席——未设口径 + 读面宁缺勿错不发明第三词；六键 = setAccountConfiguration 可写面镜像） | account 缺席·非字符串/未注册账号 `-32603`；storage 读链：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（均 `-1` + message，置顶面同款） | **2026-10-12 real（KT 扩展，窗D B6 读腿，§5 块）**：真相源 = Storage Service AccountRecord 配置字段面（官方 mergeAccountRecord 配置段消费同点；P15c 项2「JSON 读面无消费方不补」的偏差就此解除——B6 三批写穿补链的 connector/desktop 消费方在途）；从未有 storage 状态（无 master key / manifest 404 / Account item 缺席）= 全缺省面（诚实缺省不报错，getConversationState 缺省桩同口径）；fake：恒全缺省面，不落调度日志（读面纪律） |
| `setAccountConfiguration` | `{account, readReceipts?, typingIndicators?, phoneNumberSharingMode?: "everybody"\|"nobody", unlistedPhoneNumber?, storiesDisabled?, linkPreviews?}`（只写提供的字段；至少一项） | `{}` | 提供字段已全部满足 → 幂等无写成功；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失（含零可写字段）/形状错（布尔字段非布尔、phoneNumberSharingMode 非法值或非字符串）/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateConfiguration 等价）**：真相源 = AccountRecord 的 readReceipts(6)/typingIndicators(8)/phoneNumberSharingMode(12)/unlistedPhoneNumber(13)/storiesDisabled(29)（2026-10-12 窗D B5 增，§5 块）/linkPreviews(11)（2026-10-12 窗D B6 增，§5 块），读改写仅动提供字段（字段保真铁面同款链路），+ 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；枚举 everybody=1/nobody=2（桌面 v8.31/Android 现行一致，旧 CONTACTS_ONLY 已从官方 proto 移除不收）；真相源单点（AccountRecord），其他设备经 storage sync 收敛，无额外同步广播；fake：成功 `{}` |
| `setProfile` | `{account, givenName, familyName?, about?, emoji?, avatar?}`（文字面全量写：givenName 必填非空，familyName/about/emoji 缺省=置空，官方 updateProfile 同口径；字节预算 UTF-8：given/family 各 ≤128B、about ≤512B、emoji ≤32B，超限 `-32603`；avatar 三态：缺省=保留现有、data URI=换新、null=移除） | `{"status":"saved"}` | 头像上行（profile PUT/CDN POST）失败 `-1 PROFILE_UPLOAD_FAILED`（data `{retryable:true, textSaved}`——textSaved=true 如实呈现文字面已提交的部分成功）；storage 链路同 `setAccountConfiguration` 四码；参数缺失/givenName 空、familyName/about/emoji 非字符串或超字节预算、avatar 非 image 族 data URI/非 base64/超 10MB/非字符串/未注册账号 `-32603` | **P5 real（2026-10-05 官方 updateProfile 等价）+ 对齐批②（2026-10-06 形状/错误/预算面，详见 §5）**：文字面走 presage `Manager::update_profile`（retain_avatar=true）；头像面为上游双层缺口自建薄封装（官方桌面 putProfile+uploadAvatar 行为对齐：`PUT /v1/profile` JSON `{version, name, about, aboutEmoji(加密 b64), avatar, sameAvatar:false, commitment(b64)}` → 响应上传属性 → 加密头像 multipart POST CDN0 根 → avatar path=响应 key，详见 §5）；写后 storage 面 AccountRecord.givenName/familyName/avatarUrlPath 同步；fake：成功 `{"status":"saved"}`，不落调度日志 |
| `setUniversalExpireTimer` | `{account, expirationInSeconds}`（0=关闭） | `{}` | 同值幂等无写成功；PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`；参数缺失/形状错（timer 字符串/负数/小数/超 u32）/未注册账号 `-32603` | **P5 real（2026-10-05，E 批「不可达」结论勘误后落地）**：真相源 = AccountRecord.universalExpireTimer(17)（① vendored 官方 proto），读改写仅动该字段 + 409 响应体采纳重放；从未有 storage 状态 → master key 播种 + version 1 最小 AccountRecord（§5）；账户级默认值无广播（会话级计时器才广播 expiration-update），其他设备经 storage sync 收敛；fake：成功 `{}` |
| `getBlockedConversations` | `{account}` | `{blocked: [{conversationId, kind: contact\|group\|legacyGroup, blockedAtTimestampMs?(u64 毫秒)}]}`（存储序；条目形状与置顶面同构 + blockedAtTimestampMs 透出（2026-10-12 窗D B7，wire additive）——contact/GV2 记录 blockedAtTimestamp>0 在场、0=官方「拉黑时刻未知」口径与无该位的 GV1 记录键缺席） | 未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，connector 1.36 白名单逐字，§4.33） | **P5 real（2026-10-05 只读面）+ 2026-10-12 窗D B7（blockedAt 透出，§5 块）**：Storage Service manifest → 全部 type=1/2/3（CONTACT/GROUPV1/GROUPV2）identifier 批量原样读 → vendored proto 解码 → blocked 位过滤投影（contact 口径与置顶面同构：aciBinary 16B → UUID 串优先、字符串形态直通、e164 跳过；group=master key 标准 base64、legacyGroup=legacy id bytes）；无 master key 或 manifest 404（从未有 storage 状态）→ 空列表；坏条目（缺 item/解密失败/解码失败/标识映射不出）跳过零 stderr；fake：恒 `{blocked: []}` |
| `getAvatar` | `{account, conversationId}`（形状分派：ACI/PNI ServiceId UUID 直通、手机号文本（E.164/裸数字）经 contacts 反查、群 = master key 标准 base64（listGroups id 同源）；三者皆不中 `-32603`） | `{dataUri, size}`（data URI = `data:<mime>;base64,<payload>`，MIME 魔数嗅探、未识别回退 `application/octet-stream`；size = 原始字节数） | 参数缺失/会话标识形状非法/未注册账号 `-32603`（参数面不占用结构化码）；无头像（未同步 profile_key/对端或群未设头像/服务器 403/404）→ `AVATAR_UNAVAILABLE`；网络/下载/解密失败 → `AVATAR_FETCH_FAILED`；超 10MB 帽 → `AVATAR_TOO_LARGE`（均 `-1` + message 码位） | **2026-10-06 real（KT 扩展）**：1:1 走 presage `retrieve_profile_avatar_by_uuid`（profileKey = presage store `profile_key(service_id)`，contacts 同步随行落库；未同步 → 诚实「无头像」）；群走 presage `retrieve_group_avatar`（master key 派生 GroupSecretParams 解密）；缓存/CDN/解密/入库由 presage 承担；10MB 帽照 presage 注释口径，超出 fail-closed；fake：`AVATAR_UNAVAILABLE`（不落调度日志，读面纪律） |
| `setConversationBlocked` | `{account, conversationId, kind: contact\|group\|legacyGroup, blocked(bool)}` | `{blocked: [...]}`（写后全量，同 getBlockedConversations 形状含 blockedAtTimestampMs 透出——新拉黑目标条目携引擎时刻，存储序；2026-10-12 窗D B7 wire additive） | 云目标记录解析失败 `CONVERSATION_NOT_RESOLVED`；PUT 失败/409 conflictBackOff 连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED`（均 `-1` + message，同置顶面）；参数缺失/未知 kind/blocked 非布尔/未注册账号 `-32603` | **2026-10-06 real（KT 扩展）**：官方 blocked 位写语义（读改写目标会话记录仅动 blocked 位 + blockedAtTimestamp，官方 storageRecordOps 同语义：首次拉黑 = 引擎时刻、已拉黑幂等不重写他端时刻（含 0 原样保留）、解除清零；GV1 无时间戳字段）+ 新 16B item id + version+1 + 409 响应体采纳重放（置顶面同款）；从未有 storage 状态：blocked=true → master key 播种 + version 1 最小 AccountRecord 与目标记录同批首写，blocked=false → 无可解除幂等成功不播种；纯拉黑不删会话（官方桌面同语义，消息/会话不清理）；**2026-10-12 窗D B7**：写后回显名单携 blockedAtTimestampMs（补尾条目 = 引擎时刻，与写面真值同源，§5 块）；fake：成功回 `{blocked: []}` |
| `storage.mergeContacts` | `{account}` | `{recordsRead, skipped, identitiesNew, identitiesReplaced, profileKeysSet, contactsCreated, trustStatesSynced}`（七汇总字段；逐条 merge 幂等——同钥重跑全 no-op） | account 缺席·非字符串/未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message） | **2026-10-11 real（KT 扩展，窗D A4，官方 mergeContactRecord engine 腿，§5 块）**：Storage Service manifest → 全部 CONTACT identifier 批读 → ContactRecord 逐条 merge——identityKey 非空 → 身份库 save（官方 updateIdentityAfterSync 同构：absent 新增/同钥 no-op/异钥替换 + vendored 换钥 archive sessions）+ identityState 落 sidecar 信任态（Default/坏值归 default）；profileKey 非空且本地缺失 → upsert（官方 isFirstSync\|!localProfileKey 同闸）；联系人缺失 → contacts 建档（e164/given+family 姓名随建，已有档不动——姓名权威 = 手机 contacts sync）；本机 uuid/无 serviceId/坏形跳过（官方 shouldDrop 同位，skipped 汇总）；无引擎消费模型字段（unregisteredAt/hideStory/hidden/avatarColor/note/nickname 族）记档跳过，archived/markedUnread/muted/blocked 已有专用面不重复；无 master key 或 manifest 404 → 零汇总幂等成功；详见 §5 窗D A4 块；fake：恒七字段零汇总 |
| `storage.importGroups` | `{account}` | `{recordsRead, imported, alreadyKnown, failed}`（四汇总字段；幂等——本地已有 master key 零网络跳过） | account 缺席·非字符串/未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message） | **2026-10-11 real（KT 扩展，窗D A5，官方 mergeGroupV2Record engine 腿，§5 块）**：Storage Service manifest → 全部 GROUPV2 identifier 批读 → GroupV2Record masterKey 逐群导入——本地 `store.group()` 命中 → alreadyKnown 零网络跳过（增量纪律）；未知 → 既有 `fetch_group_state` 群态拉取（groups 服务器 GET v1/groups/）→ `save_group` 落 presage store（listGroups/open_group_sender 发现与发送面即刻可用）；master key 坏形（≠32B）/群态拉取失败/落库失败 → failed 逐群 best-effort 计数不炸批（官方桌面按需拉态，引擎 sidecar 无群快照惰性层 → 导入即拉态，偏差 §5 记档）；GV1 迁移面归窗D B8；无 master key 或 manifest 404 → 零汇总幂等成功；详见 §5 窗D A5 块；fake：恒四字段零汇总 |
| `storage.getCallLinks` | `{account}` | `{callLinks: [{rootKey(标准 base64 16/21B), adminPasskey?(标准 base64), deletedAtTimestampMs?(u64 毫秒)}]}`（存储序 = manifest CALL_LINK identifiers 首现序；坏条目跳过不炸面；deletedAtTimestampMs 在场即删除态，adminPasskey 恒缺——官方 isBadRemoteData「assume deleted」同位） | account 缺席·非字符串/未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message） | **2026-10-12 real（KT 扩展，窗D A6 读面，官方 mergeCallLinkRecord engine 读腿，§5 块）**：Storage Service manifest → 全部 CALL_LINK（=7）identifier 批读 → CallLinkRecord 三字段投影（vendored 生成 proto 已含 Type::CallLink/CallLinkRecord，与引擎自有 proto 同线格式双源）；官方以 roomId 定位同链接（RingRTC derive_room_id(rootKey) 纯函数），引擎以 rootKey 字节等价定位不复制派生面；无 master key 或 manifest 404 → 空列表幂等成功；fake：恒 `{callLinks: []}` |
| `storage.setCallLink` | `{account, rootKey(标准 base64，解码后恰 16B(V0)/21B(V1)——RingRTC CallLinkRootKey 两版本定长，calls.linkPresentation 闸同源), adminPasskey?(标准 base64 非空) \| deletedAtTimestampMs?(u64 正整数)}`（**两腿恰一**：active = adminPasskey、deleted = deletedAtTimestampMs，同置/双缺 `-32603`；deleted 腿 adminPasskey 恒不写——官方 toCallLinkRecord deleted 分支 adminKey 恒缺同形） | `{callLinks: [...]}`（写后全量，同 getCallLinks 形状，存储序） | 参数缺失/rootKey 非 base64·≠16·21B·非字符串/adminPasskey 非字符串·非法 base64·空解码/deletedAtTimestampMs 非数值·≤0·小数/两键同置（官方 isBadRemoteData 同位矛盾意图）/双缺（空意图——官方 upload 恒不写无 adminKey call link 同位）/未注册账号 `-32603`；storage 链路同置顶面：`STORAGE_UNAVAILABLE`/`STORAGE_WRITE_FAILED`（`-1` + message） | **2026-10-12 real（KT 扩展，窗D A6 写面，官方 toCallLinkRecord engine 写腿，§5 块）**：官方 upload 语义（storage.preload.ts callLinks 段）——读 manifest + 全部 CALL_LINK item → 定位 replaced key（rootKey 字节等价）→ 新 16B item key + 旧 key 进 deleteKey → PUT → 409 采纳重放（conflictBackOff 3 次，置顶/贴纸面同款）；云态已满足意图（投影等值）→ 幂等无写返回当前云态（官方 storageNeedsSync=false 同语义）；manifest 404 → master key 播种 + version 1 最小 AccountRecord 同批首写（置顶/贴纸面同口径）；RecordPayload::CallLink 进 storage_sync 统一 shim（StorageRecord tag=7 包裹）；fake：成功回 `{callLinks: []}` |
| `getDevices` | `{account}` | `{devices: [{deviceId, name?(解密后 UTF-8 设备名), registrationId?, lastSeenMs?, createdAtCiphertext?(服务器原样 base64 透传)}]}`（服务器返回序，含本机设备；加密名缺席/解密失败 → name 键 null——诚实缺省不炸面） | account 缺席·非字符串/未注册账号 `-32603`；服务器拒绝 `DEVICES_FAILED`（`-1` + message，data.status? 诚实透传）；传输面 `DEVICES_UNAVAILABLE`（`-1` + message） | **2026-10-12 real（KT 扩展，窗D B13 读面，官方 getDevices engine 腿，§5 块）**：GET v1/devices（本机凭据 basic auth，reportSpam 同通道）→ 服务器 DeviceInfo 投影；设备名引擎侧解密（官方 Crypto.node.ts decryptDeviceName 同构：DeviceName proto ephemeralPublic(DJB 0x05 前缀序列形 1)/syntheticIv(2)/ciphertext(3) → X25519(ephemeral, 本机 ACI 身份私钥) → HMAC 双链（cipher/auth）→ AES-256-CTR 零计数器解密 → synthetic IV 前 16B 验证；presage store 身份私钥取钥失败 → 全部 name 置空不炸面）；createdAtCiphertext 记档透传（官方 identityPublic.seal('deviceCreatedAt') 解密面 v0.99.0 rust 无暴露位）；fake：恒 `{devices: []}` |
| `unlinkDevice` | `{account, deviceId(u32 正整数)}` | `{}` | 参数缺失/deviceId 缺席·非数值·≤0·小数/未注册账号 `-32603`；服务器拒绝（越权/主设备删除/已不存在）`DEVICES_FAILED`（`-1` + message，data.status? 透传）；传输面 `DEVICES_UNAVAILABLE`（`-1` + message） | **2026-10-12 real（KT 扩展，窗D B13 同批，官方 unlink engine 腿，§5 块）**：DELETE v1/devices/{id} 直通（官方 WebAPI unlink 同端点；服务器裁决授权——primary 可解绑任意 linked、linked 可解绑自身；引擎不做客户端主设备闸，服务器拒绝原样透出）；解绑本机设备后后续调用 401 折叠既有一线（凭据失效归一批同语义）；fake：成功 `{}` |
| `reportSpam` | `{account, targetAci(ServiceId: ACI/PNI UUID), serverGuids[]（每项必须可解析 UUID，归一为小写连字形态入路径；urn:uuid:/花括号等宽松形态 fail-closed）, token?}`（单批 ≤1000，官方 reportSpamJobQueue zod max 同源） | `{reported, failures: [{serverGuid, status?}]}`（status = 服务器 HTTP 状态码；传输层失败该键缺席不发明 0） | 参数缺失/targetAci 非 ServiceId/serverGuids 缺失·非数组·空批·元素非 UUID/超 1000/token 非字符串形状/未注册账号 `-32603`（参数面不占用结构化码）；全部 serverGuid 均失败 → `REPORT_SPAM_FAILED`（`-1` + message） | **2026-10-06 real（KT 扩展）**：官方桌面同款 `POST v1/messages/report/{senderAci}/{serverGuid}` 逐条独立请求、JSON body `{token}`（token 缺省序列化为 `{}`，官方 jsonData 同构；本机凭据 basic auth）；部分失败诚实投影：单条 4xx/5xx/传输失败记入 failures（带状态码），至少一条成功即返回 `{reported, failures}`，全部失败才整体报错；fake：全量成功 `{reported: N, failures: []}`（落调度日志，协议上行面纪律） |
| `setExpirationTimer` | `{account, recipient(单字符串), expirationInSeconds}`（0=关闭） | `{timestamp}`（广播信封时间戳；同值 no-op = 处理时刻） | 参数缺失/形状错（recipient 数组、timer 字符串/负数/小数/超 u32）/未注册联系人/未注册账号 `-32603` | **E 批 real（2026-10-05 消失消息对齐）**：1:1 计时器设置 + expiration-update 广播（signal-cli `Manager.setExpirationTimer` 语义对位：先持久化 presage store Contact 面 (timer, version+1) 再广播；构造面不设 expire_timer，presage send 面注入，详见 §5）；fake：成功 `{"timestamp":u64}` |
| `sendCallMessage` | `{account, recipient(单号码/ACI)?\|group(32B 群 master key 标准 base64，listGroups/terminateGroup/updateGroup 的 groupId 同源口径)?——寻址两腿恰一互斥, data?(base64 opaque), urgency?("droppable"\|"handleImmediately" 缺省 droppable), legacy?({kind:"busy", id:正数} \| {kind:"hangup", id:正数, type?("normal"\|"accepted"\|"declined"\|"busy"\|"needPermission" 缺省 normal), deviceId?}), destinationDeviceId?}`（modern 面=data+urgency 构造 `CallMessage{opaque{data,urgency}}`；legacy 面=busy/hangup 官方裸帧族构造 `CallMessage{busy{id}}`/`CallMessage{hangup{id,type,deviceId?}}` 无 opaque 字段；**两面互斥**——同置或都缺 `-32603`；**两腿恰一**（2026-10-08 群臂批）——`group` 与 `recipient` 同置/都缺/任一腿形状非法 `-32603`；群腿仅 modern 面放行（官方群信令恒 opaque，legacy 裸帧族群向无先例；sender-key 群发无设备定向位——destinationDeviceId 与 group 同置 `-32603`）；`groupId` 键维持禁入（wire 参数名是 `group`）） | `{timestamp}`（信封时间戳） | 参数缺失/形状错（data 非 base64/空负载、urgency 非法值或非字符串、legacy 未知 kind/id 缺席·≤0/type 非法词/deviceId 超 u32、两面同置、destinationDeviceId 超 u32 或非数值、group 非法 base64/≠32B/非字符串、两腿同置/都缺、groupId 键在场、群腿携 legacy 或 destinationDeviceId）/未注册账号 `-32603`；群腿目标已终止 `SEND_TARGET_TERMINATED`（`-1` + message，2026-10-08 群臂批） | **2026-10-06 real（KT 扩展，通话消息面）**：官方 Content.callMessage=3 信封（Signal-RTC 层信令负载，引擎不解析只透传）；**2026-10-06 legacy 帧批（1.53 协同）**：legacy busy/hangup 按官方 proto 裸帧族编码（官方桌面 callingMessageToProto busy={id}/hangup={id,type}；RingRTC 2.72.1 出向 hangup/busy 实证无 opaque 材料；signal-cli 0.14.7 CallManager sendBusyViaSignal/sendHangupViaSignal 同构）——单测钉 prost 字节编码（busy=字段5{id}、hangup=字段7{id,type,deviceId}、两面无字段10）；presage `send_message` 通路（sent-transcript 缺席见 §5 通话消息块；online 位 2026-10-11 六问批勘误：官方桌面 call 发送链同样不设 online——仅 typing 消息设 online:true，**无偏差**，原「online=false 偏差」登记作废，§5 六问批）；**2026-10-08 群臂批（P4b，connector 契约 1.82 wire 面对位，engine-compat）**：`group` 腿 sender-key 群发（官方 #handleSendCallMessageToGroup calling.preload.ts:3338-3407 → sendCallingMessage.preload.ts:99-118 isGroup 臂 sendContentMessageToGroup 同落点），群头 = open_group_sender（terminated 硬拒，send 系群组形态同纪律）+ `send_message_to_group`（sendTyping 群臂同原语）；单测钉群臂 wire = `CallMessage{opaque{data,urgency}}` 字段10（无 busy/hangup/destinationDeviceId 位）；fake：成功 `{"timestamp":u64}` 两臂同口径（落调度日志，协议上行面纪律） |
| `sendGroupCallUpdate` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源；参数层 decode_group_id 闸——fake 面同口径), eraId(必填非空字符串)}` | `{}` | 参数缺失/形状错（groupId 缺席·非 base64·≠32B、eraId 缺席·空串·非字符串、account 缺席）/未注册账号 `-32603`；announcements-only 群非 admin `SEND_TARGET_ANNOUNCEMENTS_ONLY`、群已终止 `SEND_TARGET_TERMINATED`（`-1` + message，群发送系同口径） | **2026-10-11 real（KT 扩展，六问批 Q4，官方群通话开始/结束信令发送面对位）**：官方 `sendToGroup {groupCallUpdate:{eraId}, groupV2, timestamp}`（jobs/helpers/sendGroupCallUpdate.preload.ts:23-100 同构）——`DataMessage.groupCallUpdate{eraId}`（field 19）走 dispatch_group MRM 主路径（group_v2 上下文 + announcements 闸 + terminated 硬拒沿群发送系；官方不设 announcements 闸，KT 群发送闸族统一收口 = 偏差登记 §5）；1:1 通话信令无此形态（走 sendCallMessage）；接收投影 `dataMessage.groupCallUpdate {eraId}` 见 §3 receive 面；fake：成功 `{}`（落调度日志，协议上行面纪律） |
| `sendStoryReaction` | `{account, storyAuthor(必填非空——被回应 story 作者 ACI/号码), storyTimestamp(u64 必填 >0 毫秒——被回应 story 发送时刻), emoji(必填非空), groupId?(非空字符串——engine groupId 群 master key 标准 base64，与 sendStory group/sendTextGroup 同一口径，E-S7r 批；在场 = 群 story 反应腿，缺席/null = 1:1 作者直发腿现状零变化)}`（remove 无参——官方 story 反应恒 remove:false，sendNormalMessage reactionForSend 同位硬编码；目的地 = story 作者会话 1:1 直聊腿 / groupId 在场 = 群会话普通群通道） | `{}`（1:1 腿）· 群腿 `{group: {groupId, timestamp}}`（groupId 回显 + 群腿时刻 u64，E-S7r 批 additive——对齐 sendStory group 应答先例） | account/storyAuthor/emoji 缺失·空串·非字符串/storyTimestamp 缺席·0·非数/groupId 非字符串·空串 `-32603`；storyAuthor 不可解析（本机号走注册数据→contacts E.164→UUID 直通，sendReaction 同链）/未注册账号/身份拒发闸/profileSharing 折叠 同 send `-32603`；群腿：groupId 32B 解码失败 `-32603`/群已终止 `-1`+`SEND_TARGET_TERMINATED`/announcements-only 非 admin `-1`+`SEND_TARGET_ANNOUNCEMENTS_ONLY`/作者不可解析 `-32603`（结构化错误面同群发送，send_error 单点） | **2026-10-14 real（KT 扩展，story 反应发送批 S4，官方 reactToStory 行为对齐，wire additive，engine-compat，§5 块）**：官方 `stories.preload.ts:556 reactToStory → StoryViewer.dom.tsx:649-657` 同链——reaction 锚双写：`Reaction{emoji, targetAuthorAci, targetSentTimestamp, remove:false}` ∧ `StoryContext{authorAci, sentTimestamp}` 同一作者/同一 story 时刻（SendMessage.preload.ts toProto:637-649 同构）；1:1 story reaction 走普通直聊腿不走 story 通道（官方 sendNormalMessage.preload.ts:430「1:1 story replies should not set story=true」同构——信封无 story 位）；P9 profileSharing 闸 + 身份拒发闸在闸面；sync 面 = presage fork send_message 自带普通 sent 转录，无独立回执面（story 查看回执另批，边界）；群 story 腿（story 通道 groupV2 形态）v1 不做（**措辞失真与边界已由 E-S7r 批翻案订正，见后**）；fake：成功 `{}`（落调度日志，send 系同款）；**2026-10-15 real（KT 扩展，群 story 反应发送面批 E-S7r，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat，§5 块）**：`groupId` 可选参数扩群腿——S4 批「群 story 腿（story 通道 groupV2 形态）」措辞失真随本批订正：官方群 story 反应走**普通群通道 dataMessage** 非 Content.storyMessage（enqueueReactionForSend.preload.ts:84-91 群 story targetConversation = 群本身；sendNormalMessage 群臂 groupSendOptions 携 reaction+storyContext，remove 恒 false :711-723；toProto groupV2:642/storyContext:567-579/reaction:663-679 三键共存）；引擎群腿 = 共享群发送路径单点（open_group_sender 三闸 + dispatch_group group_v2 注入 {masterKey, revision, groupChange:None}）+ 复用 build_story_reaction_data_message；群腿不走直聊身份/profileSharing 闸（群发送面同口径）；sync 面 = 群发送路径 Sent 转录自带；fake：群腿成功 `{group:{groupId, timestamp}}`、缺席腿 `{}` 零变化（落调度日志，send 系同款） |
| `calls.linkPresentation` | `{account, rootKey(标准 base64，解码后恰 16B(V0)/21B(V1)——RingRTC CallLinkRootKey 两版本定长，zkgroup 派生只取前 16B CRN)}` | `{presentation(标准 base64 = CallLinkAuthCredentialPresentation bincode 序列化字节，当天 UTC 兑换日呈现)}` | 参数缺失/rootKey 非 base64·解码后非 16/21B/未注册账号 `-32603`；服务器拒绝·zkgroup 兑换/呈现验证失败·当日凭据缺席 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? = HTTP 状态码诚实透传）；网络/传输面 `CALL_CREDENTIALS_UNAVAILABLE`（`-1` + message，retryable=true 语义）（**两枚新码位，connector 白名单登记归 connector 批**） | **2026-10-09 real（KT 扩展，P4c 切一 call-link 凭据材料面，§5 块）**：官方 getCallLinkAuthCredentialPresentation 同链（util/callLinks/zkgroup.preload.ts:16-49）——双族凭据缓存取当日 CallLinkAuthCredential（窗口算法逐字对齐官方 getDatesForRequest，§5 块）→ `present(ACI, 兑换日秒, GenericServerPublicParams, CallLinkSecretParams::derive_from_root_key)` → bincode 序列化出字节（官方返未序列化对象给 RingRTC 绑定，引擎字节化出 RPC）；凭据/呈现字节只内存转发零日志零落盘（relay-api-v2 §5 纪律）；fake：参数闸后形状桩 `{presentation}`（桩字节非有效 zk 材料仅钉形状），不落调度日志 |
| `calls.createLinkAuth` | `{account, rootKey(同 calls.linkPresentation 闸), roomId(标准 base64，解码后恰 32B——RingRTC `derive_room_id` HKDF-SHA256 固定 32B 输出)}` | `{presentation(标准 base64 = CreateCallLinkCredentialPresentation bincode 序列化), callLinkPublicParams(标准 base64 = CallLinkPublicParams bincode 序列化)}`（官方 createCallLink 凭据段双材料同形） | 参数缺失/rootKey·roomId 形状错/未注册账号 `-32603`；服务器拒绝·zkgroup 两段式兑换验证失败 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE`（同上行列） | **2026-10-09 real（同批）**：官方 createCallLink 凭据段同链（calling.preload.ts:860-940）——`CreateCallLinkCredentialRequestContext::new(roomId)` → `POST v1/call-link/create-auth`（chatService host 本机凭据，json `{createCallLinkCredentialRequest: b64}` → `{credential: b64}`，WebAPI.preload.ts:3439-3453 端点+形状、:1471-1473 schema；官方 2026-09-10 起加 `?v101=true` 切 V1 族，v0.99.0 钉不移植）→ `context.receive(ACI, GenericServerPublicParams)` → `present(roomId, ACI, generic, derive_from_root_key(rootKey))` + `get_public_params()` 双序列化；rootKey 生成/roomId 派生/adminPasskey 皆 RingRTC 侧职责引擎不收不做；单次 RPC 内两段式（无跨调用状态）；fake：参数闸后形状桩双键 |
| `calls.groupProof` | `{account, groupId(32B 标准 base64 群 master key，listGroups/terminateGroup 的 groupId 同源)}` | `{proof(token 字符串 = ExternalGroupCredential.token 原样透出；RingRTC setMembershipProof 消费其 UTF-8 字节)}` | 参数缺失/groupId 非 base64·≠32B/未注册账号 `-32603`；服务器拒绝（凭据无效/非群成员）·响应 proto 解码失败·当日凭据缺席 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE` | **2026-10-09 real（同批）**：官方 fetchMembershipProof 同链（groups.preload.ts:1782-1806 → WebAPI getExternalGroupCredential:4566-4586）——双族缓存取当日 AuthCredentialWithPni → present(group) → `GET v2/groups/token`（storageService host，basic auth = `hex(GroupPublicParams):hex(presentation)`，generateGroupAuth:4533-4540 同构）→ `ExternalGroupCredential{token=1}`（Groups.proto:285-287，presage::proto 生成面复用）解 token 透出；fake：参数闸后形状桩 `{proof}` |
| `calls.groupMembers` | `{account, groupId(同 calls.groupProof)}` | `{members: [{uuid(ACI 字符串), userIdCipherText(标准 base64 = UuidCiphertext bincode 序列化)}]}`（服务器群态成员序；RingRTC GroupMemberInfo{userId, userIdCipherText} 材料面） | 参数缺失/groupId 形状错/未注册账号 `-32603`；群态拉取被拒/解密失败 `CALL_CREDENTIALS_FAILED`（`-1` + message，data.status? 透传）；网络面 `CALL_CREDENTIALS_UNAVAILABLE` | **2026-10-09 real（同批）**：官方 getMembershipList 同语义（groups.preload.ts:7611-7632：群成员逐一 encryptServiceId）——`GET v1/groups/` + decrypt_group（fetch_group_state 既有面复用）→ 每成员 `GroupSecretParams::encrypt_service_id(ACI)` → UuidCiphertext 序列化；uuid = ACI 字符串（relay-api-v2 §2 帧面对位）；fake：参数闸后 `{members: []}` |
| `calls.endorsementsPublicKey` | `{}`（常量派生面，无 account——getStickerPackManifest 匿名面同纪律） | `{endorsementPublicKey(标准 base64 = EndorsementPublicKey bincode 序列化)}` | 无（常量派生不触网，fake 同码路真实答） | **2026-10-09 real（同批）**：legacy ServerPublicParams 派生 `get_endorsement_public_key()`（zkgroup pin 树 server_params.rs:347；官方 `serverPublicParams.getEndorsementPublicKey()` 同位，消费位 calling.preload.ts:1134/:2018 = RingRTC getCallLinkCall 的 endorsementPublicKey 槽）；共享常量模块 `zkgroup_consts` 单一真相源（云备份面同源不重复嵌） |
| `calls.relays` | `{account}` | `{account, relays: [{username, password, urls?, urlsWithIps?, hostname?, ttl?}], fetchedAtMs?, expiresAtMs?, stale, lastError?}`（relays 条目 = 官方 IceServerGroupType 逐字段透传，未知字段丢弃不发明；`fetchedAtMs`/`expiresAtMs` 为 epoch 毫秒，无缓存空态整键缺席不出 null；条目内可选字段 None 整键缺席；`stale` = 无缓存恒 true，有条目 = now ≥ expiresAtMs；`lastError` = 最近一次重拉失败诚实原因） | 未知/未注册账号 `-32603`（折叠惯例）；账号凭据失效（服务器 401/403）`-32603` + getUserStatus 解绑原文同款（缓存不动）；网络/应答形状失败**不是错误应答**——空态或旧值 + `lastError` 如实成功应答（错误不连坐，§5 块） | **2026-10-13 real（KT 扩展，批次 G 第一腿，wire additive，【engine-compat】，§5 块）**：官方 getIceServers 同端点（WebAPI.preload.ts:796 路径 + :3064 调用，chatService identified 账号 Basic Auth——presage 链接账号凭据同类用法，代理随进程 ALL_PROXY 自动生效）；缓存 + TTL 官方 calling.preload.ts #getIceServers 同口径（TTL = relays[].ttl 最小秒值、任一缺失/0 不缓存、TTL 内零网络、空应答不覆盖旧缓存、失败诚实保旧 + lastError、401/403 归解绑）；进程内存 per-account 缓存不落盘（delete_account 摘除）；fake 面 = 空态（`{account, relays: [], stale: true}`）；**connector 消费面后续批次登记**（connector 仓另有 agent 在写，本批不动） |
| `sendCallEvent` | `{account, type("audio"\|"video"\|"group"\|"ad-hoc"), direction("incoming"\|"outgoing"), event("accepted"\|"not-accepted"\|"delete"\|"observed"), conversationId(形态随 type：直聊 = 号码/service-id 串，服务层 resolve_recipient 解析；group = 32B 群 master key 标准 base64；ad-hoc = roomId 小写 hex——与入向投影 hex_encode 互逆), callId?(十进制 u64 串——官方 longToString 同构；直聊/群腿必带，ad-hoc 可缺省), timestamp?(u64 毫秒正整数，缺省 = 信封时刻)}` | `{timestamp}`（信封时间戳） | 参数缺失/形状错（type/direction/event 词表外或缺席或非字符串、conversationId 缺席·空串·群腿非 base64·≠32B·ad-hoc 腿非小写 hex·奇数长、callId 非十进制串·负号·小数·hex·超 u64·数字 JSON 类型、直聊/群腿 callId 缺席、timestamp 0·负·小数·字符串）/未注册账号 `-32603`；直聊寻址不可解析 `-32603`（send 系同折叠） | **2026-10-14 real（KT 扩展，通话事件出向同步批，官方对齐 Signal-Desktop abe80d3 updateRemoteCallHistory/getDeleteCallEvent，wire additive，engine-compat，§5 块）**：SyncMessage.callEvent=19 每事件即发向自有设备（presage self-send 通路 sendViewOnceOpen 先例）；与入向投影 `syncMessage.callEvent`（§3 receive 行）同词表严格互逆（单测往返钉定）；触发时机归事件源（connector/sidecar 每事件即调，引擎无本地通话态）；fake：成功 `{timestamp:u64}` 三腿同口径（落调度日志，send 系同款） |
| `backup.export` | `{account}` | `{"status": "started"}`（后台任务已启动，进度走 `backup.exportStatus`） | 未注册/fake 账号 `-32603`；该账号任务已在跑 `BACKUP_EXPORT_ALREADY_RUNNING`；工件缺失 `BACKUP_ARTIFACT_MISSING`；首行非法 `BACKUP_ARTIFACT_INVALID`（工件当场删除）——结构化码均 `-1` + message 码位（备份导出契约 §10.7，connector 1.47 白名单计划） | **消费批一期 real（2026-10-05）**：文件工件交接（备份导出契约 §10）——读 `<账号目录>/backup-export-input.ndjson`（connector 写入，引擎契约 §10.2 行格式/帽值）→ 专用 OS 线程装配 `MainStreamWriter` → `export_local_backup` 落官方 §5 快照；任务离开 running 即删工件（§10.2 硬边界）；每账号至多一任务；不落调度日志零 stderr；fake 模式账号无 store，按未注册 `-32603` |
| `backup.exportStatus` | `{account}` | `{account, state: idle\|running\|completed\|failed, artifactPresent, startedAtMs?, finishedAtMs?, lines?, frames?, snapshot?: {name, mainBytes, frames?}, error?, snapshots: [{name, mainBytes, frames?}]}`（快照清单 ≤32 条新→旧；`frames` 仅本进程任务产出已知；诚实缺席不发明 0） | 未注册账号 `-32603` | **消费批一期 real（2026-10-05）**：只读轮询面，仿 connector `history.importStatus` 语义（无推送事件，轮询到终态）；任务注册表进程内存不跨重启；快照清单只报告不清理（生命周期归 connector，备份导出契约 §10.6）；不落调度日志零 stderr |
| `backup.importSnapshot` | `{account, snapshotDir}`（官方桌面本地备份快照目录，官方布局 `signal-backup-<ts>/{metadata, main, files/}`；connector 传入的同机路径，引擎不持久化） | `{"status": "started"}`（后台任务已启动，进度走 `backup.importSnapshotStatus`） | 未注册/fake 账号/AEP 缺失 `-32603`；该账号导入任务在跑 `BACKUP_IMPORT_ALREADY_RUNNING`；目录或 metadata/main 缺 `BACKUP_SNAPSHOT_MISSING`；metadata decode 失败/version≠1/形状不合规 `BACKUP_SNAPSHOT_METADATA_INVALID`；backupId 比对不等（非本账号备份）`BACKUP_SNAPSHOT_KEY_MISMATCH`——结构化码均 `-1` + message 码位（备份导出契约 §11.4，connector 1.78 白名单） | **B2 批 real（2026-10-08，快照导入收向）**：官方 `importLocalBackup`/`verifyLocalBackupMetadata` 行为对齐（AGPL 只读参照零拷贝）——metadata 校验（官方 strict assert 面：version==1、iv 12B、encryptedId 16B、CTR 解密 backupId 逐字节比对）前移为 start 同步闸，main 全链（官方 FramesReader 解密+HMAC+分帧）后台 `spawn_local`（帧导出 future 非 Send），逐帧投影复用 `history::export_frames_loop` 落 `<账号目录>/history-import.ndjson`（connector §4.38 通路同一落点，临时名+rename 原子替换，失败删半成品既有 NDJSON 不动）；密钥边界 §10.5 延续（任务 future 内即用即弃）；fake 模式账号无 store，按未注册 `-32603`；不落调度日志零 stderr |
| `backup.importSnapshotStatus` | `{account}` | `{account, state: idle\|running\|completed\|failed, ndjsonPresent, startedAtMs?, finishedAtMs?, lines?, frames?, error?}`（lines = NDJSON 投影行数 recipient+chat+message；frames = 档案总帧数含 BackupInfo 元帧；诚实缺席不发明 0） | 未注册账号 `-32603` | **B2 批 real（2026-10-08）**：只读轮询面（backup.exportStatus 同语义）；注册表进程内存不跨重启（state 归 idle）；`ndjsonPresent` 是盘面真值（重启后 NDJSON 仍在场，消费方自行判定重导）；不落调度日志零 stderr |
| `transfer.downloadStatus` | `{account}` | `{present: bool}`；present=true 时 `{present, phase, bytesRead(u64), bytesTotal?, errorCode?}`——phase：`downloading`=在途（字节计数有效，total 缺席不给假 0）/`failed`=终态失败（`errorCode` 短码透出，快照保留至清除触发器，不装死）；`errorCode` 词表：`no_archive`（204 重试耗尽 = 手机端没有可传档案，独立信息态非故障）/`network`（transfer_archive 长轮询面传输失败）/`cdn_unreachable`（CDN 下载段失败）/`decode_failed`（manifest 非法或档案解密导出失败）/`io`（本地文件面失败）；phase/errorCode 为 wire additive（1.53 形状零回归）。快照缺席（未开始/已成功收尾/终态已被清除/引擎重启过）答 `{present:false}`，不猜 | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态） | **2026-10-06 real（KT 扩展，契约 1.53 §4.38 扩展）；2026-10-14 失败显形批扩面（官方对齐 Signal-Desktop「失败带错误与重试」，wire additive，engine-compat）**：transfer_archive 下载进度快照查询——`history.importStatus` progress 组合面的引擎侧只读数据源。进程内 per-account 注册表（`src/service/transfer_progress.rs`，std Mutex + Arc，不落盘不走文件），下载任务边读 CDN 响应边累计（块读）、content-length 有则记 total（拿不到只报 stage 不给假 0）；**失败不再静默清快照**：任务任何一步失败保留 `failed` 终态（已计字节如实保留 + 短错误码），「手机端没传历史」= `no_archive` 独立信息态；终态清除触发器恰三：下次 begin（新会话）、`history.importCancel` 取消、账号重链。官方对齐（语义零拷贝）：Signal-Desktop Download 段 currentBytes/totalBytes 进度（`ts/state/ducks/installer.preload.ts` L143-152 + reducer L642-675）。只读不落调度日志零 stderr（backup.exportStatus 同纪律）
| `history.importCancel` | `{account}` | `{"cancelled": bool}`——true = 确实中止了在途导入任务；false = 无在途任务（幂等成功，不假装取消了什么）。取消动作：按账号中止在途 transfer_archive 下载与云备份 restore 两腿任务（abort 即在下一个 await 点弃任务，在途长轮询/CDN 流随 future 断），清理半成品（按腿：取消哪腿清哪腿——transfer 腿 `transfer-archive.bin`/`history-import.ndjson`、restore 腿 `cloud-backup/`/`history-import-cloud.ndjson`，被中止任务的在写 NDJSON 一并删：应答 cancelled:true 后被取消腿不残留完整 NDJSON，否则 connector 消费链会把「已取消」的导入悄悄补上；另一腿已完成待消费的产物不连坐）与快照面（在途 + failed 终态两腿同清）；restore 单槽属其他账号时不动其快照 | account 缺失/非字符串 `-32603`；无其他错误面（未注册/fake 账号答 `cancelled:false`：快照面是进程内观测，不泄露注册状态） | **2026-10-14 real（KT 扩展，官方行为基准对齐：Signal-Desktop 链接导入可取消——取消=立即中止下载并清理半成品；失败显形带重试，engine-compat）**：transfer 腿取消句柄 = per-account 任务注册表（`TransferDownloadRegistry`，单线程 LocalSet 语义下 abort 返回后任务不再执行任何指令，取消侧清理与任务写盘无竞争）；restore 腿取消句柄 = 进程级单槽（归属账号 + JoinHandle），运行闸挂 `RunningGuard`（Drop 释放——abort 路径闭包收尾不执行，闸不泄漏 BACKUP_RUNNING）；取消同时是 transfer failed 终态的清除触发器之一；不落调度日志零 stderr（backups.restoreFromCloud 同纪律）；fake：恒 `{"cancelled": false}` |
| `terminateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源)}` | `{status: "terminated"}`（幂等：已终止群再次调用同形状成功，不重复上传） | 参数缺失/groupId 非 base64 或长度 ≠32B `-32603`；未注册账号 `-32603`；服务器拒绝 `GROUP_TERMINATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批）**：官方桌面 buildTerminateChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、terminate_group action，change epoch 7），群凭据 basic auth（get_authorization_for_today 同语义链，§5 群终止批）；发送联动面：已终止群 send 系硬拒 `SEND_TARGET_TERMINATED`（`-1` + message）；receive 联动面：terminated change 投影 `groupTerminated` 事件（§5 群终止批）；fake：成功 `{status: "terminated"}`（落调度日志，协议上行面纪律） |
| `deleteStoryForEveryone` | `{account, targetTimestamp(u64 必填 >0 毫秒——被删 story 发送时刻), recipients?(非空字符串数组——逐收件人 DOE 腿名单，缺席/空批 = 无腿直落自同步腿——官方零收件人 Special case 同构), updatedStoryRecipients(必填非空数组——wire `SyncMessage.Sent.storyMessageRecipients` 扇出名单展开形态：{destinationUuid(必填非空字符串), distributionListIds(字符串数组，可缺省=空名单), isAllowedToReply(必填布尔)}；名单成员展开归 connector)}` | `{timestamp}`（DOE 信封时间戳） | account 缺失·空串/targetTimestamp 缺席·0·非数/recipients 非数组·单字符串形态·条目非字符串·空串/updatedStoryRecipients 缺失·空批·非数组·条目缺 destinationUuid·destinationUuid 空串·非字符串·isAllowedToReply 缺失·非布尔·distributionListIds 非数组·条目非字符串 `-32603`；号码不可解析/收件人发送失败/未注册账号 同 send 折叠 `-32603` | **2026-10-14 real（KT 扩展，story 删除扇出批 S5，官方 sendDeleteStoryForEveryone 行为对齐，wire additive，engine-compat，§5 块）**：名单 story（1:1 扇出）= 逐收件人 `DataMessage{delete{targetSentTimestamp}}` 走 story 通道（fork `send_story_message` DataMessage 载体 + `include_profile_key` 选入位，P9 profileSharing 闸裁定注入——官方逐收件人 sendMessage profileKey 携带同源）；解析全量前置 + 解析序去重（sendStory 同构）；per-recipient 失败整拒无 sync（sendStory 既有纪律；官方按 sendStatus 重试为偏差，§5）；之后恰好一次自同步 `SyncMessage.Sent{timestamp=story ts, isRecipientUpdate:true, storyMessageRecipients=updated}` 无 message/storyMessage 面（多设备官方 StoryRecipientUpdate 事件载体）；24h 窗官方 UI 层闸（isOlderThan DAY）wire 面不设；群 story 腿 = 普通 group DOE（既有 remoteDelete 覆盖）不在本面；接收面 `syncMessage.storyRecipientUpdate` 新脸见 §3 receive/§5 S5 块；fake：成功 `{timestamp}`（落调度日志，send 系同款） |
| `leaveGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源)}` | `{status: "left"}` | 参数缺失/groupId 非 base64 或长度 ≠32B `-32603`；未注册账号 `-32603`；self 既非成员也非待邀 `-32603`（官方桌面 leaveGroupV2 仅日志不行动的 GUI 语义，API 面整拒不静默，§5 登记偏差）；服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（群写面 P4 退群批，KT 扩展，§5 块）**：官方桌面 leaveGroupV2（models/conversations.preload.ts:2882）行为对齐——群态读面 self 分类（官方同序：ACI pending → 撤 pending 邀请 `delete_members_pending_profile_key`、ACI member → `delete_members[self ACI]` 退群、ACI 都不是且 PNI pending → 撤 PNI 邀请[官方 syncMessageOnly 面]、都不是 → `-32603`）；`PATCH v2/groups`（version = revision+1 饱和，vendored build_remove_member_action/build_remove_pending_member_action 原语）；写后本地群态刷新 best-effort（群登记不删，terminate 面同纪律）；fake：成功 `{status: "left"}`（落调度日志，协议上行面纪律） |
| `updateGroup` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源), expiration?, title?, avatar?, addMembers?: [service-id 串\|联系人号码…], removeMembers?: [同 addMembers 形状], addFromInviteLinkAccess?, inviteLinkPassword?}`（expiration：0=关闭，u32；title：非空 string，null/非字符串/空串 `-32603`——官方 `if (title)` 对空串 falsy no-op 的语义引擎不静默收，全缺整拒同纪律；addMembers：非空字符串数组（createGroup members 同解析——不可解析整拒 `-32603`，官方桌面静默跳过是 GUI 语义登记偏差；self 静默剔除；有凭据 = 全成员、凭据失败/无 key/PNI = pending 邀请）；removeMembers：非空字符串数组（同解析；群态 members 面 → delete_members、pending 面 → delete_members_pending_profile_key、审批申请者面 → delete_members_pending_admin_approval（P15b 审计项6 deny 臂，官方 denyPendingMembershipFromGroupV2 / signal-cli GroupV2Helper.refuseJoinRequestMembers 三向判别同构——此前 requesting 目标落整拒臂，connector §4.53 refuse 流端到端断链）、三者皆非整拒 `-32603`——官方 buildDeleteMemberChange 不设防的 GUI 前置，API 面不静默）；addFromInviteLinkAccess：词表字符串 `ANY`/`MEMBER`/`ADMINISTRATOR`/`UNSATISFIABLE`（UNKNOWN 是读面兜底词不作写意图，其他词/null/非字符串 `-32603`）；inviteLinkPassword：标准 base64 非空字符串（null/空串/坏 base64/非字符串 `-32603`；长度服务器裁决不本地硬闸）；avatar 三态镜像 setProfile——缺省=保留、data URI `data:image/*;base64,` 规范 base64=换新、null=移除；解码后 ≤10MiB、非 image 族/非规范 base64/超帽/非字符串 `-32603`；expiration/title/avatar/addMembers/removeMembers/链接两面至少一项在场，全缺 = 无操作整拒 `-32603` 不静默 no-op；单项或组合在场均合法（同调合成单 change 一次 PATCH）） | `{status: "sent"}`（群族成功答同纪律，connector 契约 1.45 §4.44 同形状） | 参数缺失/形状错（groupId 非 base64 或长度 ≠32B、expiration null·字符串·负数·小数·超 u32、avatar 非 image 族 data URI/非规范 base64/解码超 10MiB/非字符串、title null·非字符串·空串、addMembers/removeMembers 非数组·空数组·条目非字符串·成员不可解析、removeMembers 目标既非成员也非待邀也非审批申请者、addFromInviteLinkAccess 非法词（含 UNKNOWN）·null·非字符串、inviteLinkPassword null·空串·非字符串·坏 base64、expiration/title/avatar/addMembers/removeMembers/链接两面全缺）`-32603`；未注册账号 `-32603`；群头像上行链（avatar/form 拉取·属性校验·加密·CDN POST）失败 `GROUP_AVATAR_UPLOAD_FAILED`（`-1` + message，data `{retryable:true, textSaved:false}`——合成单 change 语义下伴生面未提交，见 §5）；服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403/401 非 admin/凭据无效、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE`（`-1` + message，STORAGE 族 retryable 语义） | **2026-10-06 real（KT 扩展批，群 expiration）**：官方桌面 buildDisappearingMessagesTimerChange 行为对齐——`PATCH v2/groups` 上传 GroupChange.Actions（version = revision+1 饱和、modify_disappearing_message_timer action（field 12），timer = 加密 `GroupAttributeBlob{disappearingMessagesDuration}`（uint32 秒，0=关）），群凭据 basic auth（§5 群终止批同链）；receive 联动面：expiration change 投影 `groupUpdated` 事件（§5 群 expiration 批）；connector 侧由 `conversations.setExpireTimer` 群形态路由至此（契约 1.45，kt-engine 面 INVALID_REQUEST 债务随本批解除）；**2026-10-09 real（群头像上行面批，§5 块）**：avatar 三态 + 合成单 change（expiration/avatar 同 version 双 action 一次 PATCH），上行链 = 群凭据 `GET v2/groups/avatar/form`（AvatarUploadAttributes protobuf）→ `GroupAttributeBlob{avatar}` 加密 → CDN0 multipart POST → avatar path 进 modify_avatar action（field 11，官方 buildUpdateAttributesChange/uploadAvatar/uploadGroupAvatar 行为对齐，镜像 setProfile 头像面先例）；receive 投影 avatar change 不投影（宁缺勿错，§5）；fake：expiration/avatar 缺省·null 臂成功 `{status: "sent"}`、data URI 臂失败 `GROUP_AVATAR_UPLOAD_FAILED`（两臂纪律 §5）；调度日志 avatar 键形状摘要化（data URI/base64 载荷零字节落盘）；**2026-10-11 real（群写面 P2 改名批，§5 块）**：title 臂 = modify_title action（field 10，加密 `GroupAttributeBlob{title}`，官方 buildUpdateAttributesChange `if (title)` + buildGroupTitleBuffer 同构），与 expiration/avatar 合成单 change 同 version 一次 PATCH；调度日志 title 键明文逐字保留（群名短标签非敏感载荷，avatar 摘要化纪律不外溢）；fake title 臂随缺省/null 臂成功形状桩；**2026-10-11 real（群写面 P3 加成员批，§5 块）**：addMembers 臂合成进同一 change（有凭据 = add_members presentation、否则 pending 邀请，官方 buildAddMembersChange 分流同构），成员解析 createGroup P1 同链；写后本地群态刷新全路径生效；fake addMembers 臂随缺省/null 臂成功形状桩；**2026-10-11 real（群写面 P4 移除成员批，§5 块）**：removeMembers 臂按群态分类（members 面 = delete_members[deleted_user_id = ACI 加密]、pending 面 = delete_members_pending_profile_key，vendored build_remove_member_action/build_remove_pending_member_action 原语，官方 buildDeleteMemberChange/buildDeletePendingMemberChange 行为对齐）；fake removeMembers 臂随缺省/null 臂成功形状桩；**2026-10-11 real（群链接写面批 P13/任务2，connector 契约 1.83 wire 面，engine-compat，§5 块）**：addFromInviteLinkAccess 臂 = modify_add_from_invite_link_access action（field 15，proto 枚举直通——官方 buildAccessControlAddFromInviteLinkChange/buildNewGroupLinkChange 同字段）、inviteLinkPassword 臂 = modify_invite_link_password action（field 19，原始密码字节直入——官方 buildInviteLinkPasswordChange `Bytes.fromBase64` 同位）；两面合成进同 version 单 change 单次 PATCH（官方 buildNewGroupLinkChange「启用即同改双面」同形）；单测钉 wire 字节（field 15 tag 0x7a、field 19 tag 0x9a 0x01）；fake 两臂随缺省/null 臂成功形状桩；调度日志两键逐字保留；**2026-10-11 real（P15b 审计项6，§5 块）**：removeMembers 第三臂 = 审批 deny（requesting 面 → delete_members_pending_admin_approval，官方 denyPendingMembershipFromGroupV2 / signal-cli GroupV2Helper.refuseJoinRequestMembers 三向判别同构；connector §4.53 refuse 流经此面，此前 requesting 目标整拒 `-32603` 端到端断链）；ban 写面求证 = `setConversationBlocked`（GroupV2Record.blocked 位，2026-10-06 批）已存在无缺口 |
| `createGroup` | `{account, title(必填非空字符串——官方 createGroupV2 name: string 必填同位), members?: [service-id 串(ACI/PNI)\|联系人号码…], expiration?(u32，0 = 官方 `if (expireTimer)` falsy 落缺省同语义：建群不带 timer action), avatar?(三态镜像 updateGroup——缺省=无、data URI=换新、null=无 avatar action；解码 ≤10MiB)}` | `{groupId(随机 32B master key 标准 base64，listGroups/updateGroup/terminateGroup 的 groupId 同源口径), revision(服务器 GroupResponse.group.version 回读，解码失败退化 0)}` | 参数缺失/形状错（title 缺席·空串·非字符串、members 非数组·空数组·条目非字符串、members 条目不可解析（非 service-id 且号码不在本机 contacts——官方桌面缺本地联系人静默跳过是 GUI 语义，API 面整拒）、expiration 非法形状、avatar 非 image 族/非规范 base64）`-32603`；未注册账号 `-32603`；self profile key 凭据取材失败（无 presentation 服务器必拒创建，不硬造材料）/服务器拒绝 `GROUP_CREATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：400 群超限/形状被拒、401 凭据无效；self 凭据失败 status 缺省）；avatar 上行链失败 `GROUP_AVATAR_UPLOAD_FAILED`（伴生面未提交，updateGroup 同 data）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 扩展批，群写面 P1）**：官方桌面 createGroupV2（groups.preload.ts:1810）行为对齐——随机 master key（32B CSPRNG）→ zkgroup 派生链（`GroupSecretParams::derive_from_master_key` → public params → groupId = `get_group_identifier`，官方 deriveGroupFields 同构；wire 面 groupId = master key 标准 base64 引擎口径，非官方桌面 conversation id 的 GroupIdentifier base64）→ self ExpiringProfileKeyCredential 取材（zkgroup 官方原语链：request context → unauth chat WS `GET /v1/profile/{aci}/{version}/{request}?credentialType=expiringProfileKey` + Unidentified-Access-Key → receive；失败 = 建群失败不硬造）→ 初始 Group proto（version 0 + 官方 accessControl 默认 attributes/members/memberLabel=MEMBER、addFromInviteLink=UNSATISFIABLE + 加密 title/timer + avatar path，官方 buildGroupProto/createGroupV2 同形状；members 分流：ACI+profile key+凭据 = 全成员 presentation、profile key 在凭据失败 = pending 降级、无 profile key/PNI = pending——官方 createGroupV2 成员分流同构）→ `PUT v1/groups`（storage host、群凭据 basic auth、body = 加密 Group proto，官方 createGroup WebAPI 同端点同形状）→ 服务器群态拉取落 presage store（本地群态初始化，best-effort）；客户端不做群人数上限闸（官方 getGroupSizeHardLimit 服务器 remote config 值，超限由服务器 400 诚实透传）；fake：title 臂成功形状桩 `{groupId(随机 32B 标准 base64), revision: 0}`、avatar data URI 臂失败 GROUP_AVATAR_UPLOAD_FAILED（两臂纪律同 updateGroup）
| `migrateGroup` | `{account, groupId(遗留 GV1 群 id 标准 base64 非空——形状闸 = 合法 base64 非空，不硬钉 16B：HKDF 派生原语对任意字节定义良好，官方 libsignal GV1 id 为 16B 但引擎不越权加闸), members(必填非空字符串数组——service-id 串\|联系人号码，createGroup 成员解析同链), title?(可选非空字符串，缺席 = 空串迁入), expiration?(u32，0 = off，createGroup 同形状), avatar?(三态镜像 createGroup——缺省=无、data URI=换新、null=无 avatar action；解码 ≤10MiB)}` | `{groupId(新 GV2 群 master key 标准 base64——HKDF 派生输出，非服务器回填，listGroups/updateGroup/terminateGroup 的 groupId 同源口径), revision(0=新建；已迁移兜底臂 = 服务器既有群 revision 回读)}` | 参数缺失/形状错（account 缺席、groupId 缺席·空串·非字符串·坏 base64·解码空、title 空串·非字符串、expiration 非法形状、members 缺席·非数组·空数组·条目非字符串·条目不可解析、avatar 非 image 族/非规范 base64）`-32603`；未注册账号 `-32603`；self profile key 凭据取材失败/服务器拒绝 `GROUP_MIGRATION_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传，self 凭据失败 status 缺省）；avatar 上行链失败 `GROUP_AVATAR_UPLOAD_FAILED`（伴生面未提交，createGroup 同 data）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-12 real（KT 扩展批，窗D B8，engine-compat，§5 块）**：官方 initiateMigrationToGroupV2（groups.preload.ts:2348）行为对齐——GV1 群 id 字节 → `deriveMasterKeyFromGroupV1`（ts/Crypto.node.ts:72 同式 HKDF-SHA256：salt = 32 零字节、info = "GV2 Migration"、OKM 首 32B = 新群 master key；无官方测试向量，单测钉输出稳定性作回归锚）→ 成员分流复用 createGroup 链（ACI+profile key+凭据 = 全成员、降级 = pending，官方 getGroupMigrationMembers :2193 同语义：有 ACI+profileKey+凭据 → membersV2、其余有 serviceId → pending（addedByUserId=self）、无 uuid → dropped）→ 初始 Group proto（revision 0 + 官方 accessControl 默认 attributes/members/memberLabel=MEMBER、addFromInviteLink=UNSATISFIABLE，官方同位）→ `PUT v1/groups`（官方建群同端点）→ **已迁移兜底**：put 失败后 `fetch_group_state` 读面探测（官方错误路径 hasV1GroupBeenMigrated 探测同语义简化版——派生 key 确定性，重复迁移 = 服务器已存群，诚实返回既有 {groupId, revision} 不报错）→ 本地群态落库。**记档偏差（§5）**：①官方迁移全员 ADMINISTRATOR（getGroupMigrationMembers 全员 Administrator 位），引擎复用 createGroup 加密链（encrypt_group_with_credentials）→ 非本人有凭据成员角色 DEFAULT、本人 ADMINISTRATOR（不手搓加密链——加密链材料正确性优先于角色位）；②title/expiration/avatar 官方从本地 GV1 群态继承（V1 title/expireTimer/avatar 迁入），headless 引擎无 GV1 本地群态 → 调用方显式入参（title 缺席 = 空串迁入）；③官方迁移本地面（migration 消息、拉黑位继承、endorsements/storage ID 重置）为桌面侧职责，引擎不做。调度日志 avatar 键形状摘要化（调度零头像字节纪律）、groupId/members/title 逐字保留；fake：成功形状桩 `{groupId(随机 32B 标准 base64), revision: 0}`、avatar data URI 臂失败 GROUP_AVATAR_UPLOAD_FAILED（两臂纪律同 createGroup）
| `generateInviteLinkPassword` | `{}`（无参数——纯 CSPRNG 材料面，官方 generateGroupInviteLinkPassword 不涉账号态同位；fake/real 同码路） | `{password(标准 base64，16 字节——官方 GROUP_INVITE_LINK_PASSWORD_LENGTH = 16 同长)}` | 形状面无（零参数方法，多余参数不设闸——wire 宽容） | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务1，connector 契约 1.83 wire 面，engine-compat）**：官方 groups.preload.ts:266 行为对齐；非协议上行不落调度日志；密码字节零日志（凭据面同纪律）。任务书记述「32 字节」与官方 16B 常量不符，以官方为准（§5 链接族批偏差登记）
| `buildInviteLink` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源), password?(标准 base64 非空——显式材料臂)}` | `{inviteLink("https://signal.group/#<websafe base64(GroupInviteLink proto)>"——官方 buildGroupLink/groupInvitesRoute.toWebUrl 同形；proto = contentsV1{groupMasterKey(32B), inviteLinkPassword})}` | 参数缺失/形状错（groupId 非 base64·≠32B、password 非 base64·空串·非字符串）`-32603`；未注册账号（缺省臂）`-32603`；群未设链接密码/群态不可得 `GROUP_INVITE_LINK_UNAVAILABLE`（`-1` + message，fake 缺省臂同码）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务1，connector 契约 1.83 wire 面，engine-compat）**：官方 groups.preload.ts:287 行为对齐——password 显式臂纯构造（fake/real 同码路，零网络）；缺省臂读服务器群态 `invite_link_password`（官方 conversation.groupInviteLinkPassword 同位，群成员凭据读面）；材料面非协议上行不落调度日志
| `parseInviteLink` | `{inviteLink(三种官方 URL 形 `https://signal.group/#…`/`sgnl://signal.group/#…`/`sgnl://joingroup#…` 或裸 inviteCode——官方桌面路由层摘 hash 后 parseGroupLink 直收 code 同口径)}` | `{groupId(32B master key 标准 base64，listGroups id 同源), password(标准 base64 原字节)}` | 参数缺失/形状错（inviteLink 缺席·空·无 hash 段·坏 base64·坏 proto·masterKey ≠32B·password 空 `-32603`；缺 contentsV1（官方 LINK_VERSION_ERROR 族——未来版本链接）同 `-32603` 折叠，无独立错误面） | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务1，connector 契约 1.83 wire 面，engine-compat）**：官方 groups.preload.ts:316 行为对齐（容错逐条 throw 面：contentsV1 缺、masterKey 空/长不符、password 空）；纯解析零网络，fake/real 同码路，不落调度日志
| `previewGroupViaLink` | `{account, inviteLink(同 parseInviteLink 三形+裸 code)}` | `{groupId(32B master key 标准 base64，listGroups id 同源), name?(群名——加密 blob 解密失败省略，官方桌面 GUI 兜底 unknownGroup 不搬，engine 省略键，§5 偏差), description?(同 name 省略纪律), memberCount, revision(= joinInfo.version), addFromInviteLink(词 ANY/MEMBER/ADMINISTRATOR/UNSATISFIABLE/UNKNOWN), approvalRequired(= addFromInviteLink == ADMINISTRATOR，官方 joinGroupV2ViaLink approvalRequired 同源), pendingAdminApproval}` | 参数缺失/形状错（account 缺席、inviteLink 缺席·坏形状——parseInviteLink 同闸收口）`-32603`；未注册账号 `-32603`；链接读面服务器拒绝/响应形状非法 `GROUP_INVITE_LINK_PREVIEW_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传，形状面无 status）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务3，connector 契约 1.83 wire 面，engine-compat）**：官方 getPreJoinGroupInfo（groups.preload.ts:272）+ getGroupFromLink（WebAPI.preload.ts:4892）行为对齐——`GET v2/groups/join/<websafe password>`（**路径后缀**非 query，官方 urlParameters 同位）+ 群凭据 basic auth（非成员可取任意群 auth credential，官方 makeRequestWithCredentials(deriveGroupFields(masterKey)) 同链）；GroupJoinInfo proto 投影（avatar URL 不投影——官方桌面 CDN 代理面取图，engine 读面无消费方，§5 gap 登记）；读面不落调度日志（材料面纪律）；fake 错误面形状桩 = `GROUP_INVITE_LINK_PREVIEW_FAILED` 无 status（fake 无链接读面，材料不可得即错误面，stickers 先例）
| `joinGroupViaLink` | `{account, inviteLink(同 parseInviteLink 三形+裸 code)}` | `{status: "joined"\|"requested"}`（direct 臂 = joined；审批臂 = requested——官方 joinGroupV2ViaLink 双臂同构：addFromInviteLink == ADMINISTRATOR → add_members_pending_admin_approval → requested，否则 add_members（Role DEFAULT，join_from_invite_link 缺省——官方 `joinFromInviteLink: null` proto3 同线）→ joined；version = joinInfo.version+1 饱和） | 参数缺失/形状错（account 缺席、inviteLink 缺席·坏形状——parseInviteLink 同闸收口）`-32603`；未注册账号 `-32603`；self profile key 凭据取材失败 `GROUP_UPDATE_FAILED`（无 status，createGroup「凭据失败不硬造材料」同纪律）；链接读面/写面服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status 诚实透传：404 链接失效、403 非准入、409 revision 竞争、400 形状被拒；**官方 400 + body `cannot ask to join via invite link if already asked to join` 吞为成功 requested**——官方桌面同款吞没强制本地 pending 态）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 批，P13/任务3，connector 契约 1.83 wire 面，engine-compat）**：官方 joinGroupV2ViaLink（conversations.preload.ts:2802）行为对齐——`PATCH v2/groups?inviteLinkPassword=<websafe>`（官方 modifyGroup inviteLinkBase64 同形状；申请者非成员无群态读面，version 以 join info 为基数，link password 即授权凭据）；审批臂 presentation = vendored `create_member_presentation(server_params, self 凭据)`（官方 buildAddPendingAdminApprovalMemberChange 同形）；写后本地群态刷新 best-effort；写面落调度日志（mutating 面纪律，inviteLink 键逐字保留）；fake 形状桩 `{status:"joined"}`（fake 无 join info 读面不裁定审批臂，§5 登记偏差）
| `cancelJoinRequest` | `{account, inviteLink(同 parseInviteLink 三形+裸 code)}` | `{status: "cancelled"}` | 参数缺失/形状错（account 缺席、inviteLink 缺席·坏形状——parseInviteLink 同闸收口）`-32603`；未注册账号 `-32603`；join info 读面/链接写面服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：404 链接失效、403 准入被拒、400 形状被拒；join info 响应形状非法同码无 status）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务4，connector 契约 1.83 wire 面，engine-compat）**：官方桌面 cancelJoinRequest（`#denyPendingApprovalRequest(ourAci)`，conversations.preload.ts:2878 同位）行为对齐——重拉 join info 取 version（申请者非成员读不到群态，joinGroupViaLink 同基数纪律）→ `delete_members_pending_admin_approval=[{deleted_user_id = own ACI 加密}]`（官方 buildDeletePendingAdminApprovalMemberChange，groups.preload.ts:1045 同形；ban 配对面不入本批——§5 群审批面块）→ `PATCH v2/groups?inviteLinkPassword=<websafe>` 链接授权写面单次 PATCH（与 join 共用 patch_group_via_link_raw，cancel 只认 2xx）；官方本地闸 isMemberRequestingToJoin 引擎无本地态不设——写意图交服务器裁决（**偏差登记**，§5）；写后本地群态刷新 best-effort；写面落调度日志（mutating 面纪律，inviteLink 键逐字保留）；fake 形状桩 `{status:"cancelled"}`（fake 无链接读/写面）
| `approveJoinRequest` | `{account, groupId(32B 标准 base64 群 master key，listGroups id 同源), recipient(service-id 串\|联系人号码——createGroup 成员解析同链)}` | `{status: "approved"}` | 参数缺失/形状错（account 缺席、groupId 非 base64·≠32B、recipient 缺席·空串·非字符串）`-32603`；未注册账号 `-32603`；recipient 不可解析·非 ACI（PNI 不受理，官方 strictAssert isAciString 同义）·不在群审批名单（官方 isMemberRequestingToJoin 本地闸同语义，API 面不静默）`-32603`；群态读面解密/形状失败 `GROUP_UPDATE_FAILED`（无 status）；服务器拒绝 `GROUP_UPDATE_FAILED`（`-1` + message，data.status = HTTP 状态码诚实透传：403 非 admin、409 revision 竞争、400 形状被拒）；网络面 `STORAGE_UNAVAILABLE` | **2026-10-11 real（KT 扩展批，群邀请链接族 P13/任务4，connector 契约 1.83 wire 面，engine-compat）**：官方 approvePendingMembershipFromGroupV2（ducks/conversations.preload.ts:3646）行为对齐——群态读 → `promote_members_pending_admin_approval=[{user_id = ACI 加密, role DEFAULT}]`（官方 buildPromotePendingAdminApprovalMemberChange，groups.preload.ts:1391 同形）→ 普通 PATCH（成员写无需 link param，官方 modifyGroupV2 `usingCredentialsFrom: []` 同款；version = 群态 revision+1 饱和）；写后本地群态刷新 best-effort；写面落调度日志（mutating 面纪律，recipient 键逐字保留）；fake 形状桩 `{status:"approved"}`（recipient ACI/号码两形同桩）
| `storage.write` | `{accountId, updates: [{type: "INSERT"\|"UPDATE"\|"DELETE", key(标准 base64，16B storage item key), record?(标准 base64，StorageRecord 明文 protobuf bytes)}]}`（字段名 `accountId` 为本扩展契约原形，非白名单面惯用的 `account`；INSERT/UPDATE 必带 record，DELETE 带 record 拒；updates 空批拒；批量内 key 重复拒） | `{version, results: [{type, key, writtenKey?, noop?}]}`（version = 写后 manifest version，全 no-op = 当前 version，从未有 storage 状态 = 0；results 与 updates 输入序对齐；`writtenKey`：INSERT = 原 key 回显、UPDATE = 引擎轮换出的新 16B item id——官方换写必轮换 item id 语义，旧 key 进 deleteKey；DELETE 无 writtenKey；no-op 条目带 `noop: true`） | 参数缺失/形状错（accountId 缺失·非字符串·空、updates 非数组·空批、条目非对象、type 非三枚举、key 非 base64·≠16B、INSERT/UPDATE record 缺失·null·非 base64·空、DELETE 带 record、批量 key 重复）`-32603`；record 解码不出白名单记录 arm（Contact=1/GroupV1=2/GroupV2=3/StoryDistributionList=5/StickerPack=6 之外，含 Account——账号记录归专用写面）`-32603`；INSERT 目标 key 已存在且内容不同/UPDATE 换记录类型（意图冲突 fail-closed）`-32603`；未注册账号 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、item 读取/解密失败 `STORAGE_READ_FAILED`、PUT 失败 `STORAGE_WRITE_FAILED`（均 `-1` + message，置顶面同款）；版本冲突（服务器 409 = base version 失配）`STORAGE_VERSION_CONFLICT`（`-1` + message + data `{retryable: false, serverVersion?}`——写意图未落地，引擎**不自动重试**，调用方重读 manifest 后重放） | **2026-10-06 real（KT 扩展，storage 写面 P0）**：通用批量 manifest item 写（官方 WriteOperation 同构：manifest version+1、新 item 加密入 insertItem、换出 key 进 deleteKey、source_device/record_ikm 透传）；读侧 manifest version 解析复用既有面（`StorageService::manifest`/`decrypt_manifest`），写通道复用既有自建 PUT 薄封装（presage/libsignal-service 上游无写面，§5 storage 写面 P0 块）；幂等：INSERT 同 key 同内容/UPDATE 同内容/DELETE key 不在 = no-op（全 no-op 不 PUT 不 bump version，整批重放安全）；从未有 storage 状态：纯 UPDATE/DELETE 批 = 幂等成功不播种（version 0），含 INSERT → master key 播种 + version 1 最小 AccountRecord 同批首写（§5 首写分支同款）；fake：成功 `{version: 0, results: [...]}`，不落调度日志（storage 状态面纪律） |
| `getUsername` | `{account}` | `{username?, usernameLink?}`（键缺席 = 未设，不发明 null/空串；usernameLink 重建为完整 `https://signal.me/#eu/<b64url(entropy‖handle)>`；链接材料形状错该键缺席） | 参数缺失/非字符串 `-32603`；未注册账号 `-32603`；storage 读链：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（均 `-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，用户名读面批）**：真相源 = Storage Service AccountRecord.username(33)/usernameLink(35)（官方 proto 注释「仅 server confirm/delete 时更新」同语义；多设备经 storage sync 收敛，无第二真相源）；从未有 storage 状态（无 master key / manifest 404 / 无 Account item）→ `{}`（未设不报错）；fake：恒 `{}`，不落调度日志（读面纪律） |
| `usernameLookup` | `{account, usernames[1..20]}`（每条 1..64 字符——connector 契约 1.59 §4.55 帧界同源镜像，帧界而非语法界；条目 = 全名或 `https://signal.me/#eu/…` 链接） | `[{recipient, username?, uuid?, isRegistered}]`（输入序逐条投影；recipient = 条目回显；单条失败诚实降级 `{recipient, isRegistered:false}` 不炸整批——语法不符/链接打不开/解不开/未命中/网络皆同） | 参数缺失/形状错（usernames 缺席·非数组·空批·超 20、条目非字符串·空·超 64）`-32603`；未注册账号 `-32603` | **2026-10-08 real（KT 扩展，用户名读面批）**：官方同款服务端匿名 lookup 通路——全名条目 hash（libsignal usernames crate 权威语法面）→ `GET v1/accounts/username_hash/{hash}`（强制匿名，200 `{uuid}`/404）；链接条目 → `GET v1/accounts/username_link/{uuid}`（200 `{usernameLinkEncryptedValue}`）→ crate `decrypt_username`（entropy 系链接自带）→ 再走 hash lookup（官方 resolveUsernameLink 同序）；base64 一律 URL-safe 无 padding；匿名 PushService（带凭据反而 400，avatar CDN 先例同构）；fake：逐条回显 `{recipient, isRegistered:false}`，不落调度日志（读面纪律） |
| `phoneLookup` | `{account, numbers[1..20]}`（每条 E.164 语法 `^\+\d{7,15}$`——语法面 fail-closed；帧界 20 条/批 = usernameLookup 同源镜像） | `[{number, aci?, pni?, isRegistered}]`（输入序逐条投影；number = 条目回显；aci/pni = CDSI 命中的服务 id UUID 串，单命中语义——双命中官方同款取 ACI；未命中/未注册诚实降级 `{number, isRegistered:false}` 不炸整批——usernameLookup 同款降级语义） | 参数缺失/形状错（numbers 缺席·非数组·空批·超 20、条目非字符串）`-32603`；E.164 语法非法 `PHONE_INVALID`（`-1` + message）；未注册账号 `-32603`；CDSI 限速 `PHONE_RATE_LIMITED`（`-1` + message，data.retryAfterSeconds = 服务器 retry_after 秒数诚实透传，退避决策在调用方）；其余服务端/连接失败 `PHONE_SERVER_FAILED`（`-1` + message） | **2026-10-15 real（KT 扩展，按手机号查找批 E，engine-compat，§5 块）**：CDSI（Contact Discovery Service）官方通路——vendored directory.rs 自动取凭据（`GET /v2/directory/auth`）→ libsignal-net SGX attested WS 查询（presage `cdsi` feature 本批开启，零新增 crate）；出口走进程 ALL_PROXY 同源注入（vendor fork 登记项，vendor/README.md）；结果**只投影即弃**——不落引擎库、不发通知（AGENTS.md 联系人纪律，号码→账号本地映射归 connector contacts 同步链）；fake：逐条回显 `{number, isRegistered:false}`，不落调度日志（读面纪律） |
| `usernameReserve` | `{account, username}`（全名 `nick.disc` 或裸昵称——缺 disc = 引擎生成 20 候选轮转；wire 界 38 字符 = connector `USERNAME_MAX_WIRE_CHARS` 同源镜像，帧界而非语法界，语法面归官方 usernames crate 硬语法） | `{username, usernameHash}`（username = 采纳候选全名；usernameHash = b64url 无 padding 32B，confirm 回传值） | 参数缺失/非字符串/空/超 38 `-32603`；crate 硬语法不符（单数字 disc、0 打头非恰两位、超位、非法字符）`-32603`；未注册账号 `-32603`；服务器 409（批内全占）`USERNAME_TAKEN`、413/429 `USERNAME_RATE_LIMITED`、其余 `USERNAME_SERVER_FAILED`（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方两相写面前相——候选 + 批 hash（crate 权威原语）→ `PUT v1/accounts/username_hash/reserve` `{usernameHashes:[…]}`（URL-safe 无 padding；200 `{usernameHash}` 采纳位，官方 AccountController 同码位）；reserve 无正式副作用（不落 username，官方同语义）；fake：本地候选 + hash 桩（真实 crate 原语）不触网，不落调度日志（storage 状态面纪律） |
| `usernameConfirm` | `{account, username, usernameHash}`（usernameHash = reserve 采纳值回传；引擎重验语法 + hash 失配 fail-closed——两相调用面被撕裂即拒） | `{username, usernameLink}`（完整 `https://signal.me/#eu/<b64url(entropy‖handle)>`；entropy 系本端新生成，仅此响应与 storage 35 字段两处落地） | 参数缺失/形状错（三参缺失·非字符串·空、语法不符、hash 失配）`-32603`；未注册账号 `-32603`；服务器 409 `USERNAME_RESERVATION_GONE`（预约过期/被占）、410 `USERNAME_TAKEN`、413/429 `USERNAME_RATE_LIMITED`、其余 `USERNAME_SERVER_FAILED`；服务器已确认而 storage 33/35 落盘失败 → STORAGE 族（诚实部分成功：服务器态已变更，调用方按 STORAGE 语义处置，getUsername/多设备 sync 收敛）（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方后相同序——`Username::proof`（ZK，随机 32B）+ `create_for_username`（新 entropy 加密负载）→ `PUT v1/accounts/username_hash/confirm` `{usernameHash, zkProof, encryptedUsername}`（200 `{usernameLinkHandle}` 重建完整链接）→ AccountRecord 33/35 storage 落盘（官方 confirm 落盘同位）；fake：本地 link 桩（随机 handle）不触网不落盘，不落调度日志 |
| `usernameDelete` | `{account}` | `{}`（幂等：本就未设照走服务器删除 + storage 清除，无写分支成功同形状） | 参数缺失 `-32603`；未注册账号 `-32603`；413/429 `USERNAME_RATE_LIMITED`、其余服务器失败 `USERNAME_SERVER_FAILED`；storage 33/35 清除失败 STORAGE 族（均 `-1` + message） | **2026-10-08 real（KT 扩展，用户名写面批）**：官方同序 `DELETE v1/accounts/username_hash`（服务器清 hash 并连带清 link 负载）→ AccountRecord 33/35 清除读改写；fake：`{}` 桩，不落调度日志 |
| `listIdentities` | `{account, number}`（单引用窄读：号码/ACI/PNI 形态直通本引擎 resolve 阶梯；`number` 必填——账号全量清单形态不提供，§5 安全码引擎面块） | `[{number?, uuid?, fingerprint(hex 66 字符 = 对端 identity key 33B 序列), safetyNumber(12 组×5 位空格分隔), scannableSafetyNumber?(标准 base64 = CombinedFingerprints protobuf，官方 QR tab 码内容), trustLevel("UNTRUSTED"\|"TRUSTED_UNVERIFIED"\|"TRUSTED_VERIFIED"), addedTimestamp(ms), pending(布尔——换钥未确认挂起位，sidecar 直投恒在)}]`；对端 identity 未知 → `[]`（官方 SafetyNumberNotReady 态，非错误） | 参数缺失/number 非字符串/未注册账号 `-32603`；号码不可解析 → `[]`（窄读不命中 = 无 identity，非错误） | **2026-10-08 real（KT 扩展，安全码引擎面批）**：pinned signal-cli 同名动词原生面（connector §4.61 `identities.get` 上游形状逐字对齐，engine-compat）——数字串与 scannable 同源（libsignal `Fingerprint::new(version=2, iterations=5200, own_aci_binary, own_aci_key, their_sid_binary, their_key)`，官方 safetyNumber.preload.ts:34-72 同参数链，§5）；trust 态引擎自有 sidecar（`identity-trust.json`），见 trust 行与 §5；**2026-10-12 real（P9 读面补链，wire additive，engine-compat）**：读投影 additive 增 `pending` 键（恒在布尔，sidecar 挂起位直投：换钥未确认 `mark_identity_replaced` 置位 = true、存量条目键缺席/首读建档/trust 写面确认钩子清标 = false——与 `send` 行身份拒发闸同源单点 [`sidecar_pending`]；connector §4.61 additive 透传，桌面横幅「首联系人挂起漏报」呈现判定消费） |
| `trust` | `{account, recipient, verifiedSafetyNumber? \| state?}`（**两臂恰一**：verified 臂 = `verifiedSafetyNumber` 字符串（空格剥离后 60 位数字串/66 位 hex fingerprint/base64 scannable 三形态，官方 IdentityVerificationCode.parse 同构）；clear 臂 = `state` 词表 `"unverified"\|"default"`（官方 setUnverified/setVerifiedDefault 语义）；两臂同置/双缺/`state` 其他值 `-32603`。pinned `trustAllKnownKeys` 测试模式不实现，§5） | `{}`（pinned 同款：命令无 JSON 输出） | verified 臂数字/指纹/scannable 与本端计算值不符或对端 identity 未知 `-1` + `Failed to set the trust for this number, make sure the number and the fingerprint/safety number are correct.`（pinned UserError 原文位）；clear 臂对端 identity 未知同上；参数面/未注册账号 `-32603` | **2026-10-08 real（KT 扩展，安全码引擎面批）**：verified 臂 = pinned `trust {verifiedSafetyNumber}` 原生面（connector §4.61 `identities.verify` 上游形状逐字，比对通过 → trust 态写 `verified`）；clear 臂 = 官方「将标记为已验证清除」补臂（KT 扩展，desktop `toggleVerified` 无 pinned 投影面的引擎侧闭合）——`unverified` → TrustLevel UNTRUSTED、`default` → TRUSTED_UNVERIFIED，无数字闸（官方 clear 同语义）；两臂成功后 best-effort 自发 `SyncMessage.verified{destinationAciBinary, identityKey, state}`（signal-cli SyncHelper.sendVerifiedMessage 同构，同步失败不回滚 store 写）；fake：成功 `{}`，不落调度日志 |
| `chatFolders.list` | `{account}` | `{folders: [ChatFolderRecordJson...]}`（存储序 = manifest CHAT_FOLDER=8 identifiers 首现序；坏条目跳过不炸面；形状见 §5 Storage 记录面块） | 未注册账号 `-32603`；storage 链路 `STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：Storage Service manifest → 全部 type=8 identifier 批量原样读 → 官方 vendored proto 解码 → ChatFolderRecord JSON 投影（键名对齐官方 fromChatFolderRecord 输出面，recipients 投影 `{conversationId, kind}` 会话引用对象，§5）；无 master key 或 manifest 404 → `{folders: []}`；fake：恒 `{folders: []}`，不落调度日志（读面纪律） |
| `chatFolders.setRecords` | `{account, records?: [ChatFolderRecordJson...], removeIds?: [uuid 字符串...]}`（**两参至少一非空**；record 形状 `{id(uuid), name, position(u32), showOnlyUnread, showMutedChats, includeAllIndividualChats, includeAllGroupChats, folderType("UNKNOWN"\|"ALL"\|"CUSTOM"), includedConversationIds?/excludedConversationIds?: [{conversationId, kind: contact\|group\|legacyGroup}...], deletedAtTimestampMs?(u64, 非 0 = 墓碑)}`；records 逐条按 id upsert——云上同 id 定位旧 identifier 则轮换新 16B item id 换写（官方同实体更新语义），无旧 = 新插；`deletedAtTimestampMs` 非 0 = 墓碑写（官方删除语义，24h 过期清理归调用方批）；removeIds = 官方 deleteKey 语义（identifier 出 manifest + deleteKey，expired-tombstone 清理面）；与云上现存完全同值的 record 幂等跳过不轮换 key） | `{folders: [...]}`（写后全量投影，存储序 = 写后 manifest identifiers 首现序，同 chatFolders.list 形状） | 参数缺失/形状错（records 非数组·条目非对象·id 非 UUID·name 缺失·position/布尔/词表/时间戳/recipient 条目形状错、removeIds 条目非 UUID、两参双缺或全空批）`-32603`；未注册账号 `-32603`；PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`；服务不可达 `STORAGE_UNAVAILABLE`；读取失败 `STORAGE_READ_FAILED` | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：官方 upload 语义（每实体独立 record + 新 16B item id 换入/旧 key deleteKey/version+1/source_device·record_ikm 透传 + 409 响应体采纳重放 [1s,5s,30s]，storage.preload.ts toChatFolderRecord 同构）；manifest 404 首写分支同置顶面（master key 播种 + version 1 最小 AccountRecord 同批）；record 内容不落引擎本地（读写直通，connector 持久化职责，§5）；fake：成功回 `{folders: []}`，不落调度日志（storage 状态面纪律） |
| `notificationProfiles.list` | `{account}` | `{profiles: [NotificationProfileRecordJson...]}`（存储序 = manifest NOTIFICATION_PROFILE=9 identifiers 首现序；形状见 §5） | 未注册账号 `-32603`；storage 链路同 chatFolders.list 两码 | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：同 chatFolders.list 链路，type=9 identifier + NotificationRecord 投影（键名对齐官方 mergeNotificationProfileRecord 输出面）；fake：恒 `{profiles: []}`，不落调度日志 |
| `notificationProfiles.setRecords` | `{account, records?: [NotificationProfileRecordJson...], removeIds?: [32 位 hex id...]}`（语义与 chatFolders.setRecords 同构：id upsert/墓碑/removeIds/幂等跳过；record 形状 `{id(32 位小写 hex), name(非空), emoji?, color(u32 0xAARRGGBB), createdAtMs(u64), allowAllCalls, allowAllMentions, allowedMembers?: [{conversationId, kind}...], scheduleEnabled, scheduleStartTime?/scheduleEndTime?(u32 0000-2359), scheduleDaysEnabled?: [1..7 整数...], deletedAtTimestampMs?}`） | `{profiles: [...]}`（写后全量，同 notificationProfiles.list 形状） | 参数面同 chatFolders.setRecords（id 非 32 位 hex、scheduleDaysEnabled 条目非 1..7 等）`-32603`；错误面同 chatFolders.setRecords 四码 | **2026-10-08 real（KT 扩展，Storage 跨设备同步批）**：官方 toNotificationProfileRecord 同构写面（emoji/color/createdAtMs/allowAllCalls/allowAllMentions/allowedMembers/schedule 族全字段）；fake：成功回 `{profiles: []}`，不落调度日志 |
| `storyDistributionLists.list` | `{account}` | `{lists: [StoryDistributionListRecordJson...]}`（存储序 = manifest STORY_DISTRIBUTION_LIST=5 identifiers 首现序；坏条目跳过不炸面；形状见 §5 Stories 分布名单记录面块） | 未注册账号 `-32603`；storage 链路 `STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`（`-1` + message，置顶面同款） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批，Stories 名单引擎半片）**：Storage Service manifest → 全部 type=5 identifier 批量原样读 → 官方 vendored proto 解码（StoryDistributionListRecord 七字段，官方 SignalStorage.proto:333-341；manifest 类型 STORY_DISTRIBUTION_LIST=5 :53、StorageRecord.storyDistributionList=5 :77）→ JSON 投影（键名对齐官方 record 输出面：`identifier`(16B uuid→串)/`name`/`recipientServiceIds`(service id 串面)/`recipientServiceIdsBinary`(binary 面，标准 base64)/`deletedAtTimestamp`(恒在，0=未删)/`allowsReplies`/`isBlockList`，§5）；无 master key 或 manifest 404 → `{lists: []}`；fake：恒 `{lists: []}`，不落调度日志（读面纪律） |
| `storyDistributionLists.setRecords` | `{account, records?: [StoryDistributionListRecordJson...], removeIds?: [uuid 字符串...]}`（**两参至少一非空**；record 形状 `{identifier(uuid), name(在场可空串——墓碑清名), recipientServiceIds?: [ACI uuid\|PNI:uuid...], recipientServiceIdsBinary?: [标准 base64(16B ACI/17B PNI)...], deletedAtTimestamp?(u64, 非 0 = 墓碑), allowsReplies, isBlockList}`；语义与 chatFolders.setRecords 同构：按 identifier upsert——云上同 id 定位旧 identifier 则轮换新 16B item id 换写，无旧 = 新插；同值幂等跳过不轮换 key；removeIds = 官方 deleteKey 语义（identifier 出 manifest + deleteKey）） | `{lists: [...]}`（写后全量投影，存储序 = 写后 manifest identifiers 首现序，同 storyDistributionLists.list 形状） | 参数缺失/形状错（records 非数组·条目非对象·identifier 非 UUID·name 缺失·allowsReplies/isBlockList 缺失或非布尔·成员串面条目解析不出·binary 面非 base64 或非 16/17B service id·deletedAtTimestamp 负数、removeIds 条目非 UUID、两参双缺或全空批、重复 id、records∩removeIds 交集）`-32603`；未注册账号 `-32603`；错误面同 chatFolders.setRecords 四码（`STORAGE_WRITE_FAILED`/`STORAGE_UNAVAILABLE`/`STORAGE_READ_FAILED`） | **2026-10-08 real（KT 扩展，Storage 跨设备同步批，Stories 名单引擎半片）**：官方 toStoryDistributionListRecord/mergeStoryDistributionListRecord 同构（storageRecordOps.preload.ts:796/:2299）；upsert/墓碑/幂等（decode→re-encode 全等判据）/409 采纳重放/首写播种 Account 全部与 chatFolders.setRecords 同链路；binary 面写入前按 ServiceId 归一化（16B ACI/17B PNI）；官方删除级联面（对该名单 story 走 deleteStoryForEveryone + 删 sender key）归调用方，引擎只管 record 面同步（§5）；同批放开 `storage.write` 通用写面白名单 StoryDistributionList arm（§5 storage 写面 P0 块修订，其余 arm fail-closed 不变）；fake：成功回 `{lists: []}`，不落调度日志（storage 状态面纪律） |
| `backups.restoreFromCloud` | `{account}` | `{"status": "started"}`（全链后台化：凭据拉取（404→backupId 引导自愈→重试一次）→zkgroup 兑换呈现→生产 chat 未鉴权 gRPC 定位→CDN 读凭据→下载→AEP 派生 MessageBackupKey 解密导出 NDJSON；终态经 `backups.cloudStatus` 观测） | account 缺失/非字符串 `-32603`；结构化码七族均 `-1` + message 码位（`CloudRestoreError::rpc_code` 单一映射源，带 detail 时附 `data.detail`；cloudStatus failed 相位同表两端不漂）：同步段 `BACKUP_AUTH_FAILED`（未注册/fake 账号、AEP/registration/身份密钥缺席或 zkgroup 兑换被拒，retryable=false）、`BACKUP_NOT_PROVISIONED`（引导自愈后仍无档=账号从未开启云备份，retryable=false，UI 诚实降级）、`BACKUP_RUNNING`（同引擎已有在途 restore 重入，retryable=false）；后台段失败经 cloudStatus failed 相位透出：`BACKUP_TRANSPORT_FAILED`（chat/REST 传输，retryable=true）/`BACKUP_DOWNLOAD_FAILED`（CDN，retryable=true）/`BACKUP_DECRYPT_FAILED`/`BACKUP_EXPORT_FAILED`（retryable=false）（cloud-backup-contract §2） | **2026-10-08 real（KT 扩展，云备份 restore 批）**：官方凭据链六步全实锤（GET v1/certificate/auth/group 7 天窗签发→404 引导 PUT v1/archives/backupid 自愈→zkgroup 兑换呈现（生产 GenericServerPublicParams 常量嵌入）→`get_message_backup_info` 定位→`GET cdn{N}/backups/{dir}/{name}` 流式块读→`backup::keys::derive_keys`(AEP,aci).message_key + FramesReader 尾部 HMAC 复用快照帧链落 NDJSON）；messages 档免费全账号、media 档二期（诚实边界 cloud-backup-contract §3）；运行闸进程级单槽 CAS（`try_begin_running`/`finish_running`）；进度快照 `Arc<Mutex>` 双持；send lane mutating；fake 账号无 store 同未注册答 `BACKUP_AUTH_FAILED`；不落调度日志零 stderr |
| `backups.cloudStatus` | `{account}`（仅契约形状校验，不参与路由——进度面进程级单槽，同引擎至多一个 restore） | `{present: bool}`；present=true 时 `{present, phase, bytesRead(u64), bytesTotal?(u64), errorCode?}`——phase：`downloading`=在途（字节计数有效，total 缺席不给假 0）/`failed`=终态失败（`errorCode` 透出同码表，快照保留至下次 start 覆盖，不装死）；快照缺席（未开始/已成功收尾/引擎重启过）= `{present:false}`（成功终态不留痕清缺，产物走 NDJSON 消费链——transfer_archive「无论成败收尾清快照」同纪律；`history.importCancel` 取消同清，按会话归属账号判定） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态，transfer.downloadStatus 同纪律） | **2026-10-08 real（KT 扩展，云备份 restore 批）**：只读轮询面（backup.exportStatus/transfer.downloadStatus 同语义，无推送事件轮询到终态）；快照进程内存不跨重启；只读不落调度日志零 stderr；**2026-10-14 起取消经 `history.importCancel`**（在途任务 abort + 半成品清理 + 快照/归属清缺，运行闸经 RunningGuard Drop 释放不泄漏） |
| `backups.uploadToCloud` | `{account}` | `{"status": "started"}`（全链后台化：装配段=专用 OS 线程（§10.3 同款，工件→main 容器流式写→临时产物 `<account_dir>/cloud-upload-main-tmp`）→上行段=LocalSet watcher：凭据拉取（restore §1 链同源，404 引导自愈）→zkgroup 兑换呈现→生产 chat 未鉴权 gRPC `get_upload_form`（cdn===3 强制）→TUS create-with-upload 单发 POST（`Tus-Resumable: 1.0.0` + `Upload-Length` + `Upload-Metadata: filename <b64(form.key)>` + form headers，body=密文产物 128 KiB 块流式读盘，请求级超时 900 s）→`refresh_backup`；终态经 `backups.uploadStatus` 观测；upload 面=官方桌面 `Backups.upload` 同链，产物 remote 型与一期 local 容器逐字节同构，派生钥=`derive_keys`+remote 臂（Some(forward-secrecy token)→NEW_DST），契约钉定 cloud-backup-contract §6.1） | account 缺失/非字符串 `-32603`；结构化码八族均 `-1` + message 码位（`CloudUploadError::rpc_code` 单一映射源，带 detail 时附 `data.detail`；uploadStatus failed 相位同表两端不漂）：同步段 `BACKUP_AUTH_FAILED`（未注册/fake 账号、AEP/registration/身份密钥缺席或 zkgroup 兑换被拒，retryable=false）、`BACKUP_UPLOAD_ALREADY_RUNNING`（同引擎已有在途 upload 重入，**新码位，白名单登记归 connector 透传批**，retryable=false）、`BACKUP_ARTIFACT_MISSING`/`BACKUP_ARTIFACT_INVALID`（工件缺席/首行非法，非法当场删工件，retryable=false）；后台段失败经 uploadStatus failed 相位透出：`BACKUP_NOT_PROVISIONED`（引导自愈后仍无档，retryable=false）/`BACKUP_TRANSPORT_FAILED`（凭据/form gRPC/refresh_backup，retryable=true）/`BACKUP_UPLOAD_FAILED`（**新码位，白名单登记归 connector 透传批**：cdn≠3/非 2xx/超帽/上行读盘，retryable=true——上传 POST 已离站=unknown-outcome 窗口，重试=整链重来）/`BACKUP_IO_FAILED`（临时产物写面，retryable=false）（cloud-backup-contract §6.2） | **2026-10-08 real（KT 扩展，云备份 upload 批）**：官方 TUS 单发协议实锤（桌面 tusProtocol.node.ts create-with-upload：`POST {signedUploadLocation}` 2xx=完成，无断点续传——引擎同口径单发，失败=failed 终态不自动重试）；上传 POST 用 `HttpAuthOverride::Unidentified`（预签名 URL 不附账号 basic auth）；凭据/header 不落日志不落盘；临时产物任何终态必删（官方 finally unlink 同款），崩溃残留由下次上传覆盖写收敛；产物 1 GiB 帽 fail-closed（form 段 `UploadTooLarge` 同映射 `BACKUP_TRANSPORT_FAILED`，契约 §6.1 裁定）；media 档出范围（messages 档密文归档；mediaRootBackupKey 每次随机 32B，契约差异登记同 §10.5）；运行闸进程级单槽 CAS；send lane mutating；fake 账号无 store 同未注册答 `BACKUP_AUTH_FAILED`；不落调度日志零 stderr |
| `backups.uploadStatus` | `{account}`（仅契约形状校验，不参与路由——进度面进程级单槽，同引擎至多一个 upload） | `{present: bool}`；present=true 时 `{present, phase, bytesRead(u64), bytesTotal?(u64), errorCode?}`——phase：`exporting`=装配段在途（无字节计数语义，bytesRead=0）/`uploading`=上行段（块计数有效，total=产物字节不给假 0）/`failed`=终态失败（`errorCode` 透出同码表，快照保留至下次 start 覆盖，不装死）；快照缺席（未开始/已成功收尾/引擎重启过）= `{present:false}`（成功终态不留痕清缺——restore cloudStatus 同纪律） | 无（参数缺失/未注册/fake 账号同样答 `{present:false}`：快照面是进程内观测，不泄露注册状态，cloudStatus 同纪律） | **2026-10-08 real（KT 扩展，云备份 upload 批）**：只读轮询面（cloudStatus/transfer.downloadStatus 同语义）；快照进程内存不跨重启；只读不落调度日志零 stderr |
| `engine.protocolCapabilities` | `{}`（参数忽略，账号无关——无 `account`，未绑定账号照常应答） | `{deviceCapabilities: {storage, transfer, attachmentBackfill, spqr, profiles_v2, usernameChangeSyncMessage, versionedExpirationTimer, optionalPhoneNumber}, deviceCapabilitiesUpload: {attachmentBackfill, spqr, usernameChangeSyncMessage, optionalPhoneNumber}, linkCapabilities: {attachmentBackfill, spqr, usernameChangeSyncMessage}, baseline: {presageRev, libsignalVersion, libsignalCommit}}`（能力位 JSON 形状 = vendor serde 属性输出：camelCase、`profiles_v2` 按上游字面保持 snake——线上声明即此拼写；位值 = 引擎实际向服务器声明的协议能力，真相源与防漂移闸见 §5 引擎自证面块；`deviceCapabilitiesUpload` = D5 设备面注册位（官方 CapabilitiesUploadType 四键）；baseline = 编译期钉定的依赖锚，运行时不查网络） | 无（只读常量应答，无业务错误面；fake/real 同码路真实答） | **2026-10-10 real（KT 扩展，引擎自证面批）**：引擎以 linked device 身份的声明如实自报——账号属性面 DeviceCapabilities（presage 主注册链同值的钉定镜像）+ 链接面 LinkCapabilities（link_device 取 `LinkCapabilities::default()` 三位，运行时 live 复用 vendor 构造器）；spqr 恒 true（vendor 链已声明，上游 2026-03 强制过滤批量注销不达标客户端的核对面）；不落调度日志零 stderr。**2026-10-11 D5 additive**：deviceCapabilities 补 `versionedExpirationTimer`/`optionalPhoneNumber` 两位（server 真相 DeviceCapability.java 全集对齐，均 false），新增 `deviceCapabilitiesUpload` 面（engine.deviceCapabilities 的 PUT body 同形） |
| `engine.deviceCapabilities` | `{account}` | `{account, registered: true, capabilities: {attachmentBackfill, spqr, usernameChangeSyncMessage, optionalPhoneNumber}}`（capabilities = PUT /v1/devices/capabilities 的 body 同形回读，位值 = 钉定镜像，见 §5 D5 块） | 未知/未注册账号 `-32603`（open_registered 未命中折叠，不泄露注册态）；服务器 401 解绑 `-32603` + getUserStatus 解绑原文同款；其余服务器失败 `-32603` 折叠（位值声明幂等，重试安全） | **2026-10-11 real（KT 扩展，D5 设备面注册批）**：官方 registerCapabilities 同端点手动触发 + 上传面回读（宿主可查询）；fake 恒已注册（getUserStatus 同款）；接收管线建流另有自动注册（同一位值常量，每管线运行成功一次即止）；触网不落调度日志零 stderr |
| `engine.perfMetrics` | `{}`（参数忽略，账号无关——无 `account`，未绑定账号照常应答） | `{uptimeMs, timings, counters, unknownKeyDropped}`（timings 条目 `{count, totalUs, maxUs, lastUs}` 微秒制、键序 BTreeMap 确定性；词表封闭见 §5 S2a 块——未命中的计时/计数键不出现，诚实缺席不发明 0） | 无（只读快照应答，无业务错误面；fake/real 同码路真实答——指标是进程真相，不分模式） | **2026-10-12 real（KT 扩展，S2a 性能基线打点层，【engine-compat】，§5 S2a 块）**：进程内有界指标注册表的只读投影（只测不改立项纪律，`kt-desktop-wt-signal/docs/plan/signal-runtime-followup-projects.md` §附四②）；快照进程内存不跨重启；词表外键计 `unknownKeyDropped` 丢弃（有界面）；消费面 = connector 白名单透传（**connector 切片遗留**）+ S2b 直连实测 harness；不落调度日志零 stderr |
| `deleteForMeConversation` | `{account, kind("contact"\|"group"), conversationId, localOnly?, mostRecentMessages?, mostRecentNonExpiringMessages?}`（会话级「为我删除」上行，官方 deleteConversation 发送面同位。kind：contact = service-id 串（ACI/PNI）\|E164（`+` + 1..15 位数字）双形态——官方 getConversationIdentifier aci/e164 优先序；group = 32B 标准 base64 群 master key（decode_group_id 闸，listGroups id 同源）；legacyGroup 不在词表——官方非 GV2 群 throw（syncIdentifiers.preload.ts:148-150）同拒。localOnly 缺省/null = false：全量清空臂，`mostRecentMessages` 必填非空数组、条目 ≤5（官方 slice(0,5) 上限镜像，超出 fail-closed 不静默截断破坏性意图），`mostRecentNonExpiringMessages` 可选 ≤5（官方 areAnyDisappearing 伴随面）；true = 仅本地臂，mostRecent* 必须缺席（官方 local-only else 分支无消息位）。条目 `{author, timestamp}`：author = service-id 串\|E164 直通（官方 toAddressableMessage 形态，引擎不做号码↔service id 二次变换——作者身份须与手机端消息行逐字匹配），timestamp 非零整数） | `{status: "sent"}`（群族成功答同纪律） | 参数缺失/形状错（account 缺席、kind 词表外、conversationId 形状错——contact 非 service-id·非 E164、group 非 base64·≠32B、localOnly 非布尔、全量清空臂 mostRecentMessages 缺席·空批·非数组·超 5、nonExpiring 超 5、仅本地臂 mostRecent* 在场、条目 author 缺失·不可解析、timestamp 缺失·0·负数·非整数）`-32603`；未注册账号 `-32603`；发送链错误面 = send 系同款（解绑 `-32603` + getUserStatus 解绑原文、限流/人机验证/服务端不可用同 send_error_of 分类） | **2026-10-11 real（KT 扩展，P15c 项3，官方对齐 conversations.preload.ts:5640-5663 + SendMessage.preload.ts:1840 getDeleteForMeSyncMessage，engine-compat，§5 P15c 块）**：会话级「为我删除」上行两变体——全量清空臂 = SyncMessage.deleteForMe.conversationDeletes[0]{conversation, isFullDelete: true, mostRecentMessages, mostRecentNonExpiringMessages?}（delete-conversation 语义：手机端按 mostRecent 定位并全量清空 + 服务器删除）、仅本地臂 = localOnlyConversationDeletes[0]{conversation}（delete-local-conversation：其他设备仅清本地行）；会话标识三形态 binary-first（threadServiceIdBinary 现行面/threadE164/threadGroupId = master key bytes，官方 toConversationIdentifier isProtoBinaryEncodingEnabled 同位）；padded sync message；desktop 上行接线归后续批；fake：两臂成功 `{status: "sent"}` 形状桩，参数闸同口径（契约测试钉定），落调度日志 |
| `getConversationState` | `{account, conversationId, kind: contact\|group\|legacyGroup}`（kind 词表与会话引用形状同 setConversationBlocked 面） | `{mutedUntilTimestamp, archived, markedUnread}`（三键恒在；目标记录缺席/无 storage 状态/manifest 404 = 全缺省 `0/false/false`，诚实幂等不报错） | 未注册账号 `-32603`；kind 缺失/词表外、会话不可解析/形状错 `-32603`；storage 链路：服务/auth 不可达 `STORAGE_UNAVAILABLE`、读取/解密失败 `STORAGE_READ_FAILED`（`-1` + message，blocked 面同款） | **2026-10-11 real（KT 扩展，P15b 项2，状态收敛审计 R1，wire additive，§5 P15b 块）**：真相源 = Storage Service record 字段（非 sync message）——ContactRecord.archived/markedUnread/mutedUntilTimestamp（SignalStorage.proto 11/12/13）与 GV2/GV1 同名位（4/5/6）；MuteExpiration.toProto 语义 = 绝对毫秒、0 = 未静音；fake：恒缺省态（契约测试钉定） |
| `setConversationState` | `{account, conversationId, kind: contact\|group\|legacyGroup, mutedUntilTimestamp?, archived?, markedUnread?}`（部分写意图——缺席位不动；mutedUntilTimestamp = 非负整数毫秒、两位布尔；null/形状错整拒；三 flag 全缺 = 无写意图 `-32603`） | `{mutedUntilTimestamp, archived, markedUnread}`（写后目标三 flag 投影，getConversationState 同形状；官方 upload 成功语义 = 本地写后面为准） | 参数形状错（见左）/kind 词表外/会话不可解析/未注册账号 `-32603`；storage 链路：PUT 失败/409 采纳重放连满 3 次 `STORAGE_WRITE_FAILED`、服务不可达 `STORAGE_UNAVAILABLE`、读取失败 `STORAGE_READ_FAILED`（均 `-1` + message，blocked 面同款） | **2026-10-11 real（KT 扩展，P15b 项2）**：复用 blocked/sticker 写面既有编排（read 改写 decode→mutate→encode 保真、新 16B item id、WriteOperation version+1、409 采纳重放）；同值幂等无写；目标记录缺席非缺省意图 = 最小记录首建（标识 + 三 flag，fresh_blocked_record 同构）、全缺省不空造记录；noteToSelf kind 不收（官方走 AccountRecord.noteToSelf* 位且无静音位，消费方提出再立项，偏差登记）；fake：缺省态叠意图回显（契约测试钉定） |

`receive` 通知：`{jsonrpc, method:"receive", params:{account, envelope}}`；
P2-A 起投影覆盖 connector 1.15/1.24/1.25 的全部消费面（钉定 signal-cli
0.14.8 JSON，见 connector implementation-plan §4.13/§4.19/§4.20）：

- 信封公共位：`sourceUuid`（ACI service id 字符串）、`sourceDevice`、
  `timestamp`；contacts 命中时附 `sourceNumber`（E.164）与 `sourceName`。
  `serverGuid`（2026-10-06，reportSpam 面配套）：服务器为该条消息分配的
  UUID（举报 API 的路径参数），信封元数据公共位，有值才投（proto 只投影
  不发明）。`urgent` / `ephemeral` / `reportingToken`（P15a 项2，官方
  信封事件面同位 MessageReceiver.preload.ts:1177-1180）：vendored
  Metadata 扩位捕获 Envelope.urgent(14)/ephemeral(12)/reportSpamToken(17)
  上抛——两标志 raw 位投影（wire 在场即投真值：urgent 缺席 = proto 缺省
  true、ephemeral 缺席 = false，缺席键不在）；reportingToken = token 非空
  时标准 base64（官方 Bytes.isNotEmpty 闸同构；键名沿官方事件面，proto
  字段 reportSpamToken——组5 adminKey 先例同款），connector 举报面
  （reportSpam 方法 token 参数）的引擎侧真数据源；ephemeral 官方桌面零
  消费（P14 组 7 核对结论），如实投位由消费方自判。
  `serverTimestamp`（2026-10-11，P15c 项5，官方信封事件面同位
  MessageReceiver.preload.ts:1178）：Envelope.serverTimestamp 恒投
  （毫秒，服务器为该条消息记录的接收时间；vendored
  Metadata.server_timestamp 同源捕获，wire 缺席 = proto 缺省 0，官方
  `?? 0n` 同口径照投 0 不省键）。消费面：48h 编辑窗判据
  （isTooOldToEditMessage，多设备编辑 echo 补齐面——P15c 项1 记档的
  键位由此落地）、消息行服务器时间/服务器停留时长（官方 getMessageAge
  同源）；dataMessage/editMessage/sent echo 同走本信封单键齐备。
- `dataMessage`：`timestamp`、`message`、`quote {id, author, text?, type(NORMAL\|GIFTBADGE——proto 词原值恒产，缺席/枚举外 coerce NORMAL，官方 isKnownProtoEnumMember 同构；六问批 Q5), bodyRanges?: [{number?, style?, start, length}] ≤64（mention 二进制优先 + author_numbers E.164 映射、Style 臂归同条目 style 键、NONE 丢、未知枚举整条丢；六问批 Q5）}`、
  `reaction {emoji, targetAuthor, targetSentTimestamp, isRemove}`、
  `storyReply {authorAci, sentTimestamp}`（2026-10-15 正条目补录——投影
  E2 批已落地，键名裁定见 §5：storyContext → 桌面既有消费形直投，官方
  DataMessage.storyContext=21 同字段名；author binary 16B UUID 优先、
  string 兜底（官方 isProtoBinaryEncodingEnabled 双写面 binary 权威，
  不一致时 binary 赢），两者皆缺/不可解析整键缺席宁缺勿错；story_
  context 缺席键不在 = pre-批帧逐字节同形；1:1/群/sent sync/edit 四腿
  同形状（群回复另带既有 groupInfo），reaction+storyReply 共在 = story
  锚定反应判别面——S4 判别语义见下）、
  `remoteDelete {timestamp}`、`attachments[] {id, contentType, filename?,
  size?, width?, height?, caption?, isVoiceNote, isBorderless, isGif,
  locator?}`（≤32；元数据
  面 + 2026-10-07 起 `locator` 指针面随行——key/cdnKey/cdnNumber/
  uploadTimestamp/digest，proto 只投影不发明，全缺键不在，§5 receive
  信封附件 locator 投影块；P14 收投影缺批增 `blurHash?`（非空才投）与
  `incrementalMac?`+`chunkSize?`（同进同退齐备才投，导出面
  project_pointer_common 同形——官方 filePointers.preload.ts:84-87
  同构；导出+live 两面派生自本批，§5）及 `isBorderless`/`isGif` 两
  flag 位恒投布尔（组 3，镜像 isVoiceNote 先例——官方 isGIF 另有
  contentType video/* 附加闸，引擎 flag-only，§5）及 `waveform?`/
  `duration?` 语音元数据两位（2026-10-14 批：`audioWaveform=21` 标准
  base64 非空才投、`audioDurationSeconds=22` float 秒在场即投——官方
  Android 发送侧生成的波形恒 100 桶 dB 尺度 0..255 与时长如实透传，
  收侧不加成对闸宁多投不误判；历史导入腿 archive `FilePointer` schema
  无此两位=键恒不在，§5））、`previews[] {url, title?,
  description?, date?, image?}`（≤4；image 同走附件指针投影；`date` 为
  P14 Preview.date 两端批——发布时刻毫秒，>0 才投，官方
  cleanLinkPreviewDate 同语义）、
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
  投影，官方桌面收发两侧同 flag 机制），缺省键不在。P14 收投影缺批增
  `expireTimerVersion?`（=23，>0 才投——0 系 presage 无计时器线程注入的
  INITIAL 初值零信息量；版本化语义 1 起）与 `requiredProtocolVersion?`
  （=12，>0 才投，**弱面**：官方 required > 本端支持即拒收的强校验，引擎
  只投影不 enforce——宁收勿丢宽容面，§5）。story 反应判别（story 反应
  发送批 S4，2026-10-14，§5 块）：reaction 行同信封携 `storyReply` 键
  （storyContext 投影，Stories 接收批）= story 锚定反应——宿主据此区分
  story 反应与普通反应（story 无会话行，按 reaction targetAuthor+
  targetSentTimestamp 归会话会空悬，storyReply 是锚定面）；1:1 story
  reaction 信封无 story 位（普通直聊腿照投，reaction+storyReply 共在、
  isStory 键不在）；信封 story 位 + reaction+storyContext 无 groupV2 =
  官方整信封丢弃（MessageReceiver.preload.ts:1534-1544 门序，E2 批闸
  同判）；信封 story 位 + reaction+storyContext+groupV2 = 群 story 反应
  豁免照投（isStory + reaction + storyReply + groupInfo 共在）。
- `editMessage {targetSentTimestamp, dataMessage}`（sent echo 三臂之一，
  本机其他设备自发编辑同步；直聊/群组同形状，群组 dataMessage 带
  groupInfo（groupV2 master key 投影）；六问批 Q1 前该臂整信封丢弃）。
- `contact {name?{givenName?, familyName?, prefix?, suffix?, middleName?,
  nickname?}(空串省略), number?/email?: [{value, type(MOBILE\|WORK\|CUSTOM；
  缺省/未知归 HOME), label?}], organization?, avatar?: {avatar:<附件指针
  投影>, isProfile}}`（六问批 Q2，官方 processContact 展开位 + backup 导出
  面 contact_record 键形同族；value 空条目整条丢、全空卡片整键缺席；
  address 面不投 = §5 gap 登记）。
- `poll {question, options, allowMultiple}` / `pollVote {targetAuthor,
  targetSentTimestamp, optionIndexes(Set 去重保序，空数组 = 清票照投),
  voteCount(0 透出)}` / `pollTerminate {targetSentTimestamp}`（六问批 Q3，
  官方 processDataMessage.preload.ts:395-441 行为对齐；vote targetAuthor
  二进制优先 + E.164 映射，pin/unpin 同口径）。
- `groupCallUpdate {eraId}`（六问批 Q4，官方群通话开始/结束信令接收位，
  background.preload.ts:3535-3543 只在群上下文消费；eraId 空/缺席整键
  缺席——官方 `eraId != null` 判面同域）。
- `typingMessage {action: STARTED|STOPPED, timestamp, groupId?}`（
  `groupId` 为 P14 收投影缺批：TypingMessage.groupId=3 群 typing 面位，
  master key 标准 base64，非空才投；1:1 缺席键不在，旧宿主零变化）。
- `receiptMessage {timestamp[], when, isDelivery, isRead, isViewed}`
  （connector §4.19 三档面）。
- `pinMessage {targetAuthor, targetSentTimestamp, pinDurationSeconds?}` /
  `unpinMessage {targetAuthor, targetSentTimestamp}` / `adminDelete
  {targetAuthor, targetSentTimestamp}`（官方 27/28/29 投影，pinDuration
  缺席 = forever）。
- `sticker {packId(hex), packKey(base64), stickerId, emoji?, data?}`
  （官方 DataMessage.Sticker 序列化同构；data 走附件指针投影，元数据
  only，2026-10-04）。
- `payment {kind(NOTIFICATION\|ACTIVATION_REQUEST\|ACTIVATION),
  note?(notification 臂恒产，无值 null 填位——官方 note ?? null 镜像)}` /
  `giftBadge {expiration?(毫秒——zkgroup 解析得，秒→毫秒同官方
  getReceiptExpirationTime()*SECOND), level?, receiptCredentialPresentation
  (标准 base64), state(UNOPENED——receive 时点恒初态，开启/兑换状态机归
  消费方)}`（P14 组 2 收投影缺批，官方 processPayment:171/processGiftBadge:476
  行为对齐：payment 缺席/item 缺席/activation type 缺省词表外整键不在；
  kind 词 SCREAMING 同 quote/contact 惯例；giftBadge presentation 缺席/空
  整键不在，解析失败降级只投 receiptCredentialPresentation+state 两键——
  官方构造 throw 炸整条消息处理，引擎宁收勿丢不镜像，§5）。
- `syncMessage.sentMessage`：本机其他设备发出的直聊（timestamp/message/
  destinationNumber|destinationUuid + 同 dataMessage 控制面）；sent 臂
  message/editMessage/storyMessage 三肢序取第一个在场位（六问批 Q6③——
  storyMessage 臂此前整信封丢弃），公共位三肢共用，全缺整信封丢；消失
  消息 E 批增 `expirationStartTimestamp`（Sent.expiration_start_timestamp=4，
  官方多设备精确起算同步位；0/缺席不投）；P14 收投影缺批增三位可选面
  （官方 SentEvent 同名直投，MessageReceiver.preload.ts:2097-2101）：
  `isRecipientUpdate: true`（=6，仅 true 投——线上缺省 false 同语义）/
  `unidentifiedStatus[] {destinationUuid?, unidentified?,
  destinationPniIdentityKey?}`（=5，destinationUuid binary 16/17B 优先
  字符串兜底，PNI 公钥字节标准 base64 非空才投）/
  `storyMessageRecipients[] {destinationUuid?, distributionListIds,
  isAllowedToReply?}`（=9，名单 id 原样数组空批照投）。
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
  store 有行，connector 消费面按自身会话管线取用。出向写面 =
  `sendCallEvent` 同词表严格互逆（2026-10-14 通话事件出向批，§3 行/
  §5 块）。
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
- `syncMessage.deleteForMe {messageDeletes?: [{conversationId?, messages:
  [{author, timestamp}]}], conversationDeletes?: [{conversationId,
  isFullDelete, mostRecentMessages: [{author, timestamp}],
  mostRecentNonExpiringMessages: [{author, timestamp}]}],
  localOnlyConversationDeletes?: [{conversationId}]}`（六问批 Q6② +
  P15a 项1，官方 SyncContent.deleteForMe=22「为我删除」多设备同步）：
  author 二进制（ACI/PNI 16/17B）优先、e164 直通，不可解析条目丢、组清空
  后整组丢、全坏整信封丢；conversationId 缺席键不在（serviceId/binary→
  UUID 串、groupId→master key base64、e164 原样）。messageDeletes 臂（
  Q6② 既有）组无 conversation 照投、conversationId 键省略；P15a 项1 补
  整会话清除族两臂（官方 handleDeleteForMeSync 同位
  MessageReceiver.preload.ts:3658-3722——手机「删除会话/清空聊天历史·为我
  删除」的桌面同步面）：conversationDeletes 组 conversation 缺席或
  mostRecentMessages 清空整组丢（官方 warn-drop 同口径），isFullDelete 恒
  布尔（官方 Boolean() 同构），mostRecentNonExpiringMessages 空数组照投；
  localOnlyConversationDeletes 条目 conversation 缺席丢；
  attachmentDeletes 臂不投（§5 gap 登记，信封只含该臂时整信封丢）；
  缺席臂键不在；vendored 接收面已消费 presage store（save_message 臂
  delete_message，仅 messageDeletes 臂），引擎投影 additive 上抛供
  connector 删自有库行（conversationDeletes 的引擎 store 行不删——vendored
  TODO 面，§5 如实登记）。
- `syncMessage.contacts {complete, records: [{aci, number?, name?,
  expireTimer?, expireTimerVersion?, inboxPosition?}]}`（P15a 项3，官方
  SyncMessage.contacts blob 消费面投影——handleContacts
  MessageReceiver.preload.ts:3934 → doContactSync services/
  contactSync.preload.ts upsert 同位）：blob 下载/解析走 vendored presage
  既有 `retrieve_contacts`（附件下载面复用，零新平行机制），投影解析+
  落库成功记录（parse 失败上游 filter_map 丢、store save 失败 break 后
  剩余不投影，§5 如实登记）；aci = ContactDetails aciBinary/aci 解析
  UUID 串；number = E.164（缺席键不在）；name 空串不投（官方 dropNull
  同构）；expireTimer/expireTimerVersion/inboxPosition = 0 不投（presage
  u32 模型 0 与缺席不可分，§5 记档）；avatar 字节不投影（gap 登记后续
  批）；complete = blob complete 旗标（官方 isFullSync 同位，connector
  据此决定全量重置语义）；空 records 照投事件（complete 旗标即信号，
  官方 contactSync:complete 空 blob 照发同构）；信封公共位三键同面。
- `syncMessage.blocked {blockedE164s?: [{e164, timestamp?}],
  blockedAcis?: [{aci, timestamp?}], blockedGroups?: [{groupId,
  timestamp?}]}` / `syncMessage.blocked {numbers?, acis?, groupIds?,
  acisBinary?}`（P15a 项4，官方 `_handleBlocked` 同位
  MessageReceiver.preload.ts:3988——此前落兜底臂整信封丢）：modern/
  legacy 家族选择 = 官方 areModernFieldsUsed/areLegacyFieldsUsed 判定
  同构（`areModernFieldsUsed || !areLegacyFieldsUsed`——modern 任一非空
  走 modern、否则 legacy；全空归 modern 空对象，信号面即事件）；
  modern 三型 = 上游 abe80d3 新增（vendored proto 同步，field 5/6/7
  additive，§5 proto 批登记）：blockedE164s 条目 e164 缺席丢、blockedAcis
  aciBinary 二进制→UUID 串（不可解析丢）、blockedGroups groupId 原字节
  标准 base64（引擎 groupId 口径）、timestamp 缺席键不在；legacy 四位 =
  numbers/acis 原样（空串条目滤）、groupIds 逐项 base64、acisBinary 逐项
  UUID 串（不可解析滤）；所选家族键全空 → 空对象照投；modern 在场时
  legacy 四位不投（官方单家族消费同构）。
- `syncMessage.configuration {readReceipts?, unidentifiedDeliveryIndicators?,
  typingIndicators?, linkPreviews?}` / `syncMessage.fetchLatest {type?}` /
  `syncMessage.callLinkUpdate {type?, rootKey?, adminKey?}` /
  `syncMessage.callLogEvent {type?, timestamp?, conversationId?, callId?}` /
  `syncMessage.deviceNameChange {deviceId?}` / `syncMessage.usernameChange {}` /
  `syncMessage.pniChangeNumber {registrationId?, newE164?}`（P14 组 5 sync
  七整臂收投影，此前七位全落兜底臂整信封丢弃；官方 handleConfiguration/
  handleFetchLatest/handleCallLinkUpdate/handleCallLogEvent/
  handleDeviceNameChangeSync/handleUsernameChangeSync/handlePNIChangeNumber
  dispatch 同位 MessageReceiver.preload.ts:3084-3157；type 词 SCREAMING
  as_str_name 惯例——fetchLatest UNKNOWN/LOCAL_PROFILE/STORAGE_MANIFEST/
  SUBSCRIPTION_STATUS、callLinkUpdate UPDATE、callLogEvent CLEAR/
  MARKED_AS_READ/MARKED_AS_READ_IN_CONVERSATION/CLEAR_IN_CONVERSATION；
  callLinkUpdate rootKey/adminPasskey 空字节省键，**adminKey 键名沿官方
  事件面**（proto 字段 adminPasskey）；callLogEvent.conversationId 原字节
  标准 base64，1:1/群/ad-hoc 三形由消费方自判；deviceNameChange 官方按
  本机 deviceId 过滤（非指向自己的丢弃），引擎恒投原值由消费方自判；
  pniChangeNumber 只投 registrationId/newE164 两键，密钥材料不出引擎
  边界、re-key 消费不做，偏差全落 §5 组 5 块）。
- `syncMessage.keys {mediaRootBackupKey?}`（P14 组 6 收投影，此前整信封
  丢弃；官方 handleKeys 事件面 MessageReceiver.preload.ts:3338-3358）：云
  媒体备份根钥（media backup 素材）标准 base64，非空才投（官方
  Bytes.isNotEmpty 同口径），键全缺照投空对象（信号面即事件）。官方三键
  面 {masterKey, accountEntropyPool, mediaRootBackupKey}，引擎只投
  mediaRootBackupKey——偏差落 §5 组 6 块；Request{Keys} 应答停答面
  （P9 S2）与此接收投影面互不相干。
- `syncMessage.storyRecipientUpdate {timestamp, storyMessageRecipients[]
  {destinationUuid?, distributionListIds, isAllowedToReply?}}`（story 删除
  扇出批 S5，2026-10-14，官方 StoryRecipientUpdateEvent 镜像
  MessageReceiver.preload.ts:3002-3030）：门序 = `Sent.isRecipientUpdate ∧
  storyMessageRecipients 非空` 判定**先于 storyMessage 面**（官方同序；
  editMessage 面在门之前——官方同序），命中 = 独立脸整体替换 sentMessage
  （isRecipientUpdate/storyMessageRecipients 公共位不落旧键——矛盾形状
  不产出）；timestamp = 信封 ts（官方 envelope.timestamp 同源），
  destination = 信封 destinationServiceId 恒自有 ACI 不投（sync 恒指向
  自有号纪律）；消费面 = `deleteStoryForEveryone` 自同步扇出腿 + story
  名单成员更新（多名单 story 发送 sync `isUpdate=true` 形状自此归本脸，
  行为变化 §5 S5 块记档）。

接收流共生三通知（独立方法名即事件，**非 envelope 载体**——流态观测/
流态事件无信封可挂；wire additive）：

- `receive.emptied {account}`（P15b 项7，时序审计 G3）：服务器历史队列
  排空标记 per-account 上抛（官方 ApiEmptyQueue→EmptyEvent 同位）；每轮
  排空都投，消费方幂等自便（如启动期「积压回放完成」推进度旗标）；§5
  P15b 项7。
- `accountUnbound {account}`（B4 Unbound 停环批）：服务器 401 凭据拒绝
  后接收循环停止，上抛一次——官方链接设备语义：凭据失效即停等重链，
  不无限退避重连；§5「身份事件面 + Unbound 停环」块。
- `receive.clockskew {account, skewMs, serverTimestamp}`（P15b 项8）：本地
  −服务器时钟偏移越阈（官方 ClockSkewDetector 24h 默认阈值）边缘触发
  告警一次，skewMs 带符号（正 = 本地快）、serverTimestamp = 观测源信封
  服务器毫秒；**积压免疫**（时序修复批，2026-10-12）：闸每流武装——
  新流入口解除武装、`receive.emptied` 排空标记到达后武装，武装前信封
  （可能出自服务器历史队列，server_timestamp 是入队时刻）一律不喂闸，
  对齐官方 WS 层恒新鲜服务器时间锚（官方从不逐信封喂判定，见 §5 项8
  修正）；拒绝面不做（偏差登记），消费自定；§5 P15b 项8。

Storage 合并共生通知（F 链路引擎腿，**非 envelope 载体**——存储态事件
无信封可挂；wire additive，词表第五词）：

- `receive.conversationState {account, conversations, overflow}`：入站
  Storage 合并（`storage.mergeContacts`/`storage.importGroups` 读面随行
  观测）发现他设备翻转的会话三 flag 时上抛，一次 merge 批的全部翻转
  合成一条。`conversations` 条目 `{conversationId, kind,
  mutedUntilTimestamp, archived, markedUnread}`——标识口径 =
  getConversationState 参数面同构（contact=ACI service id 串 / group=
  master key 标准 base64 / legacyGroup=id 文本），三 flag 键恒在 =
  读投影同形状（0/false = 位缺省），消费方拿到即落本机簿无需回读。
  `overflow` 键恒在布尔：批帽 256 截断时 true（尾标纪律，要全集走
  getConversationState 逐会话重读）。触发纪律：仅真实翻转发（首观测
  种子静默、值未变不发）；本机写（setConversationState / storage.write
  三类记录臂）与读投影应答先登记「消费方已知态」防回声；远端删记录
  不产事件（簿不清，重建与最后已知差分）；簿进程内存不跨重启。GV1
  记录无入站读取路径不观测（legacy 族记档）；语义/帽/取舍全量见 §5
  F 链路块。

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
  标准 base64）；~~接收投影仍跳过群组消息（connector 群组行是已声明边界，
  P3 补 groupInfo 投影后放开）。~~（2026-10-04 销账：群组接收投影已放开
  ——`dataMessage.groupV2`/sent 群组带 `groupInfo {groupId}` 投影落地，
  connector 管线零改动，见 §3 receive 面「群组接收投影」bullet。）群 id
  不可解/非法按 `-32603`。
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
- **URI 形状**：real 模式发官方 `sgnl://linkdevice?uuid=…&pub_key=…` + 单
  `capabilities=nopni,nopni2,backup5`（`qr_link_url` 收敛面，2026-10-15 起；
  重复同名参数会被 Android 扫码侧 getQueryParameter 单数语义吞掉 backup5）；
  fake 夹具保持 `sgnl://link?…`（仅测试）。连接器对 URI 不解析、只透传
  回 finishLink，两侧均不受影响。
- **per-recipient results**：~~成功仅含 `{recipientNumber, success:{timestamp}}`，
  群组发送 `results` 恒 `[]`（成员级投影 P3）；部分失败的 result 形状
  （上游 identity failure / unregistered user 等枚举）未对齐，遇到即整体
  `-32603`（P3 逐枚举对齐）。~~（2026-10-11 销账：群发送语义三件批——群
  形态 `results` 逐成员填充，失败条目 `{recipientNumber, failure:{type}}`
  type 词表四值 additive，单成员失败不再炸批，见 §5「群发送语义三件」块；
  `send` 直聊腿失败面同日 P15b 项4 additive `failures` 键闭合——本条
  差异登记整体销账。）
- **updateContact 边界**：仅本地 store 已有该号码时改名；新联系人建立需要
  contacts 同步/服务器面（P2+），未命中 `-32603`。
- **getUserStatus**：P2 起 real——whoami 轻量探测；仅服务器 401 映射解绑态
  原文（`-32603`），网络等其他失败按 `-32603` "Internal error" 不误报解绑。
  recipient 维度的注册态已实现（2026-10-15 起）：独立方法面 `phoneLookup`
  （CDSI，§3 phone 块/§5 同日批）；getUserStatus 自身仍恒 `isRegistered:
  true`（whoami 不附带查询），注册态需求由调用方显式走 phoneLookup——
  CDSI 服务器限速阈值不公开（`PHONE_RATE_LIMITED` 附 retryAfterSeconds
  诚实回传），匹配在 SGX enclave 内（官方隐私设计），结果不落引擎库。
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
  （`{identities: {"<service id 串>": {state, addedAtMs, changedAtMs,
  pending}}}`，临时文件+rename 原子替换，storage-manifest-state.json 先例
  同构；`pending` = P9 身份拒发批换钥挂起位——存量条目键缺席/无键 =
  false 读兜底，2026-10-12 起经 listIdentities 读投影 additive 透出）。
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
  `AVATAR_UNAVAILABLE`「无头像」；getAvatar 自身不做隐式 CDSI 反查（注册
  态反查已实现为独立方法面 `phoneLookup`，2026-10-15 批，§3 phone 块——
  读面按需显式调用，不在头像下载路径附带查询）、群
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
  有值才投；proto 只投影不发明）。**已知边界（token 不透投影）——
  P15a 项2 收口勘误**：envelope 级 `reportSpamToken` 经 vendored
  Metadata 扩位捕获投影（信封级 `reportingToken` 键，非空 base64，
  §3 信封公共位行），上边界「token 从 receive 面拿不到」就此废止；
  处置如 P14 组 7 核对批订正（vendored Metadata 扩位，story bit 同型
  fork，非「上游 Metadata 升版」）。
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
- **发送通路 online 位勘误（2026-10-11 六问批 Q6④；原「发送通路偏差
  （登记）」bullet 作废）**：官方桌面仅 typing 消息发送设 online:true
  （ts/models/conversations.preload.ts:1530 直聊 sendOptions、:1551 群腿
  sendContentMessageToGroup 参数，两处字面 `online: true`），call 消息
  发送链不设 online 位（sendUseCallMessage 前无写入）——引擎 presage
  `Manager::send_message` 硬编码 `online_only=false` 与官方 call 面实际
  行为一致，**无偏差**。原登记系把 typing 面的 online 语义误植到 call
  面，勘误撤回；§3 sendCallMessage 行同步勘误。presage 上游 online
  选项缺口仍在（TODO issue #252），对 call 面无影响；**不能自建
  MessageSender 绕行**的约束照旧（`new_message_sender`/
  `identified_websocket`/credentials 均 pub(crate)，引擎拿不到凭据）。
- **sent transcript 缺席（登记）**：CallMessage 无 sent transcript——
  libsignal-service `create_multi_device_sent_transcript_content` 对
  CallMessage 返回 None（presage 发送面 `if let Some` 守卫静默跳过），
  本机其他设备不收自发 call 记录（官方桌面自发 call 的多设备同步走
  CallEvent sync 通道，与 transcript 面独立；该通道引擎写面 =
  `sendCallEvent`，2026-10-14 通话事件出向批 §5 块）；不向 self 发 call
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

群写面 P1 建群批（2026-10-11，官方桌面 createGroupV2 行为对齐，KT 扩展
方法；四域功能审计实证的群写面阻塞缺口批之一——引擎账号此前无法建群，
只能被动被拉群）：

- **新方法 `createGroup {account, title, members?, expiration?, avatar?}`**
  → `{groupId, revision}`。groupId = 新群 master key 标准 base64（引擎
  全族 groupId 同源口径——send/updateGroup/terminateGroup/listGroups 可
  直接回传）；revision = 服务器 GroupResponse.group.version 回读（解码
  失败退化 0，后续群态读面收敛）。
- **官方行为锚（Signal-Desktop v8.31 abe80d3 实查，行为对齐不抄码）**：
  createGroupV2（groups.preload.ts:1810-2075）——随机 master key 32B
  CSPRNG → deriveGroupFields（:1724，secret/public params + groupId 派生
  链）→ 成员分流（contact 有 profileKey + profileKeyCredential = 全成员
  入 membersV2；否则 pendingMembersV2）→ avatar 建群前上传（uploadAvatar
  与成员解析并行）→ buildGroupProto（:454，version 0 + accessControl 默认
  attributes/members/memberLabel = MEMBER、addFromInviteLink =
  UNSATISFIABLE + 加密 title/timer）→ `PUT v1/groups`（WebAPI.preload.ts:
  4847 createGroup，storage host + 群凭据 basic auth + 加密 Group proto
  body）。title 必填（官方 `name: string` 必填同位）；members/expiration/
  avatar 可选；expiration = 0 落缺省（官方 `if (expireTimer)` falsy 同
  语义，建群无 timer action）。
- **凭据取材链（zkgroup 官方原语，不绕过不硬造）**：self 与成员的
  ExpiringProfileKeyCredential = `ServerPublicParams::
  create_profile_key_credential_request_context`（CSPRNG）→ unauth chat
  WS `GET /v1/profile/{aci}/{version}/{request}?
  credentialType=expiringProfileKey`（libsignal-net-chat
  UnauthenticatedChatApi::get_profile_key_credential，`Unidentified-Access-
  Key` 头 = `profile_key.derive_access_key()`，官方 unidentified access
  模型）→ `receive_expiring_profile_key_credential`（当日窗口 1..=7 天
  校验内建）。self 凭据失败 = 建群整失败 `GROUP_CREATE_FAILED`（无
  presentation 的创建服务器必拒）；成员凭据失败 = pending 降级（官方
  buildAddMembersChange/createGroupV2 分流同构）。presentation 上送走
  vendored `GroupOperations::encrypt_group_with_credentials`（libsignal-
  service rev 9e6c08b 既有原语，self Administrator + 成员 Default，
  joinedAtVersion 0 = 官方 buildGroupProto 同形状）。
- **错误面**：参数 fail-closed `-32603`（title 缺席/空串/非字符串——官方
  `if (name)` falsy 会静默 no-op，引擎 API 面整拒不静默；members 非数组/
  空数组/条目非字符串/不可解析整拒——官方缺本地联系人 assertDev 跳过是
  GUI 语义，API 面不静默，登记偏差）；服务器拒绝 `GROUP_CREATE_FAILED`
  （`-1` + data.status 诚实透传：400 = 群人数超限（官方
  getGroupSizeHardLimit 是服务器 remote config 值，客户端不做闸）/形状
  被拒、401 凭据无效）；avatar 上行链失败 `GROUP_AVATAR_UPLOAD_FAILED`
  （data `{retryable:true, textSaved:false}`，伴生建群未提交，updateGroup
  同 data）；网络面 `STORAGE_UNAVAILABLE`。
- **本地群态落库**：PUT 成功后服务器群态拉取（`GET v1/groups/` +
  decrypt_group 既有读面）落 presage store（save_group）——listGroups 即
  见新群；best-effort（读面/落库失败不回滚写面，后续群更新消息/发送前
  刷群态自愈，§5 群发送语义批同纪律）。
- **调度日志**：mutating 面落日志（terminateGroup/updateGroup 同纪律）；
  avatar 键落盘前形状摘要化（零 data URI/base64 字节进日志，updateGroup
  「调度零头像字节」同纪律）；title/members/expiration 键照旧逐字保留。
- **fake 两臂纪律**：title 臂（含 expiration/avatar=null 臂）成功形状桩
  `{groupId(随机 32B 标准 base64), revision: 0}`（不触网）；avatar data
  URI 臂失败 `GROUP_AVATAR_UPLOAD_FAILED`（updateGroup 两臂纪律同款）。

群写面 P2 改名批（2026-10-11，官方桌面 buildUpdateAttributesChange 行为
对齐，KT 扩展方法 updateGroup 参数族扩展；群写面四域批之二——引擎此前
无法改群名，改名走不到协议面）：

- **updateGroup 增可选 `title`（非空 string）**：change 臂 = modify_title
  action（Groups.proto field 10），title = 加密 `GroupAttributeBlob{title}`
（zkgroup encrypt_blob_with_padding，官方 buildGroupTitleBuffer 同构，
  build_update_change 内与 timer blob 同一加密纪律）。与 expiration/avatar
  合成单 change（同 version = revision+1 多 action）单次 PATCH，§5 合成
  单 change 裁定顺延。
- **官方行为锚（Signal-Desktop v8.31 abe80d3 实查，行为对齐不抄码）**：
  buildUpdateAttributesChange（groups.preload.ts:739）`if (title)` truthy
  → buildGroupTitleBuffer → modifyGroupV2；空串 falsy = 官方静默 no-op。
  引擎 API 面不静默收：title 键在场但 null/非字符串/空串 `-32603`
（createGroup title 必填同纪律——参数面不吞官方 GUI falsy 语义，登记
  偏差）。
- **调度日志**：title 键明文逐字保留（群名短标签非敏感载荷，与 avatar
  摘要化纪律分野——后者是可达 MB 级二进制）；groupId/account/expiration
  键照旧。
- **fake**：title 臂无独立上行链环节，随缺省/null 臂成功形状桩
  `{status: "sent"}`（avatar data URI 臂失败面不变）；契约测试钉成功两
  臂/拒绝六形状/调度日志 8 行。

群写面 P3 加成员批（2026-10-11，官方桌面 buildAddMembersChange 行为对齐，
KT 扩展方法 updateGroup 参数族扩展；群写面四域批之三——引擎此前无法向群
拉人）：

- **updateGroup 增可选 `addMembers`（非空字符串数组：service-id 串/联系
  人号码，createGroup members 同解析）**：合成进同一 GroupChange.Actions
（同 version 多 action 单次 PATCH，§5 合成裁定顺延）——有凭据候选 =
  add_members（zkgroup presentation，vendored
  build_add_member_action_with_credential，Role Default = 官方
  MemberRole.DEFAULT 同位）；凭据失败/无 profile key/PNI =
  add_members_pending_profile_key（vendored
  build_add_pending_member_action，added_by = 邀请者 own ACI 加密；
  timestamp/joined_at_version 0 = 服务器回填，vendored 原语语义——官方
  桌面客户端侧填 now/newGroupVersion，登记偏差）。
- **官方行为锚（Signal-Desktop v8.31 abe80d3 实查，行为对齐不抄码）**：
  buildAddMembersChange（groups.preload.ts:605）——profileKey+
  profileKeyCredential = 全成员/否则 pending（Android
  GroupsV2Operations 分流同构自证）；banned-unban 配对不入本批（后续
  群治理批）。
- **解析纪律（createGroup P1 同款，登记偏差）**：成员逐个解析
（service-id 直通/号码 contacts 数字位后缀匹配），不可解析整拒
  `-32603`（官方桌面 assertDev 静默跳过是 GUI 语义）；self 静默剔除
（self 入 change 服务器必 400）；重复条目去重。
- **凭据取材链**：ACI + 本机 profile key（presage store）→
  ExpiringProfileKeyCredential（P1 同一 fetch 链）；失败 pending 降级
  不整拒（官方分流同构）。
- **写后本地群态刷新**：refresh_local_group_state（best-effort）——P3
  批起 updateGroup 全路径生效（成员/属性写后本机 listGroups 投影收敛，
  与 createGroup/terminateGroup 落库纪律对齐）。
- **fake**：addMembers 臂随缺省/null 臂成功形状桩（fake 无解析链）；
  契约测试钉成功两臂/拒绝五形状/调度日志 7 行。

群写面 P4 移除成员/退群批（2026-10-11，官方桌面 buildDeleteMemberChange
/buildDeletePendingMemberChange/leaveGroupV2 行为对齐；群写面四域批之四
——引擎此前无法移除成员也无法退群）：

- **updateGroup 增可选 `removeMembers`（非空字符串数组，addMembers 同形
  状同解析）**：合成进同一 GroupChange.Actions（同 version 多 action 单
  次 PATCH）——先群态读面分类：目标在 members 面 = delete_members
（vendored build_remove_member_action，deleted_user_id = ACI 加密）、在
  pending 面 = delete_members_pending_profile_key（vendored
  build_remove_pending_member_action，ServiceId 直通加密）、两者皆非 =
  UnknownGroup `-32603` 整拒（官方桌面 buildDeleteMemberChange 不设防
  ——GUI 入口前置 isMember 校验，API 面不静默，登记偏差）；群态读面
  Unavailable 时 removeMembers 臂诚实透传 Unavailable（属性/add 臂仍
  退化 revision 0，expiration 批同口径）。
- **新方法 `leaveGroup {account, groupId}`** → `{status: "left"}`：官
  方桌面 leaveGroupV2（models/conversations.preload.ts:2882）同序分类——
  ACI pending → 撤 pending 邀请、ACI member → delete_members[self]、
  ACI 都不是且 PNI pending → 撤 PNI 邀请（官方 syncMessageOnly 面）、都
  不是 → `-32603`（官方仅日志不行动的 GUI 语义，API 面整拒不静默，登记
  偏差）；写后本地群态刷新 best-effort（群登记不删，terminate 面同纪
  律——退群后本机群态收敛为非成员投影，后续群更新消息照常接收）。
- **banned-unban 配对（登记不做）**：官方 buildDeleteMemberChange 携
  addMembersBanned/deleteMembersBanned 配对（移除即拉黑联动）；引擎两
  臂均不携带（群封禁面另行立项，removeMembers/leaveGroup 纯移除语义）。
- **fake**：removeMembers 臂随缺省/null 臂成功形状桩；leaveGroup fake
  成功 `{status: "left"}` 形状桩（terminateGroup 同纪律）；契约测试钉
  removeMembers 成功一臂/拒绝五形状/日志 6 行 + leaveGroup 成功一臂/拒
  绝四形状/日志 5 行。

listGroups 投影批（2026-10-11，官方桌面 getGroupBody 投影族行为对齐，
群写面 P5——读面随写面收敛）：

- **投影族扩展**：description（Some 在场）/announcementsOnly/
  expireTimerSeconds（0 = 关）/revision/members[].role（官方
  MemberRole 词表：UNKNOWN/DEFAULT/ADMINISTRATOR）/pendingMembers
（number/addedBy/timestamp）/requestingMembers（number/timestamp）/
  bannedMembers（number/timestamp）/accessControl 四族（
  attributes/members/addFromInviteLink/memberLabel，GroupV2AccessLevel
  词表 UNKNOWN/ANY/MEMBER/ADMINISTRATOR/UNSATISFIABLE 复用，Some 在
  场）。号码标签 = contacts 反查（找不到回退 service-id 串，群成员既有
  投影纪律）；时间戳 = 群态毫秒值原样。
- **vendored presage（README fork 清单登记）**：`Group` 补
  `banned_members`（serde default 空——旧存量行可反序列化；解密侧
  `members_banned` 映射，BannedMember uuid/service_id_type/timestamp
  同 PendingMember 形状）；presage-store-sqlite `into_group` 重建面补
  空 Vec（行 schema 无封禁列，announcements_only 同款 fail-open 注释
  纪律——下次服务器群态刷新收敛）。
- **isMember 诚实化修正（登记偏差）**：改计算值（own ACI ∈
  members）——早期「入库即成员恒 true」的登记口径废止：被移出群/退群
  后的本地群态残留（P4 批 leaveGroup 后群登记不删）投影即 false，
  listGroups 不再谎报成员面。
- **单测**：单群投影抽纯函数 `project_group_list_entry`——钉 id/revision/
  announcementsOnly/expireTimerSeconds/description/accessControl 词表/
  members[].role/三成员面时间戳/isMember 双臂（own 在与不在）。

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
- **边界（本批红线）**：不动 setProfile 写面；getAvatar/contactProfileUpdated
  自身不做隐式 CDSI 号码反查（与 getAvatar 同口径；注册态反查已实现为
  独立方法面 `phoneLookup`，2026-10-15 批，调用方显式调用）；不做 profile
  主动重取调度器（presage 栈无后台
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
  正文 attachment，2026-10-14 媒体 story 发送面批解除——见「媒体 story
  发送面」块）/story 删除（remoteDelete story 形态，S5 批解除——
  见「story 删除扇出面」块）/24h 过期（纯客
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
  preview{image 指针, url?, title?, date?}}` / `fileAttachment` → 附件指针投影
  （locator 面同款，connector §4.54 媒体边界：元数据 only 字节永不下载）；
  `allowsReplies` 恒投（bool 缺席=线上缺省 false 如实）；`profileKey`/
  `group` 群 story 母钥 base64（有值才投）。textStyle 缺席/未知枚举值不投
  键；DEFAULT=0 线上在场照投词（与发送面缺省省略的不对称是线上形状的
  如实两面）。preview.url/preview.title 引擎扩展键往返面（2026-10-09 批，
  发送面 previewUrl/previewTitle 对端回声）：Some 且非空才投字符串键，与
  image 同一 preview 对象并列；None/空串省键（缺席键不在）；preview 面全
  空 = preview 键不在；`date` 为 P14 Preview.date 两端批补投（>0 才投）。
  P14 组 4 扩面：`bodyRanges=6` 分流 textAttachment 级
  `mentions[] {number, start, length}`（mention 二进制 16B 优先 + E.164
  contacts 映射未命中保留 ACI，dataMessage mentions 同构）与
  `textStyles[] {style, start, length}`（五值词表 NONE 丢/未知丢，start+
  length 超 story 正文 UTF-16 长度的形状坏条目丢弃——dataMessage 同纪律）
  ；色位/背景面 `textForegroundColor?`/`textBackgroundColor?`（u32 原样，
  Some(0) 也是线上陈述）与 `background {gradient{startColor?, endColor?,
  angle?, colors[], positions[]}}` 或 `background {color}`（oneof 恰一，
  官方 TextAttachment.Params 同形键）。无 attachment 位 = 官方发送面不可能产出的
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
  形态，S5 批解除——见「story 删除扇出面」块）/已读回执/查看回执面；
  引擎零 stderr（官方两条 story warn 路径只保留判面行为，无日志输出）。

**story 反应收发面（S4，2026-10-14，KT 扩展 `sendStoryReaction`，
engine-compat）**

- **背景与口径**：Stories 块原边界「story 反应不做（DataMessage.Reaction
  面）」随本批解除——发送侧 = 官方 reactToStory 行为对齐；接收侧零 wire
  新增（reaction/storyReply 既有键共在即 story 锚定形状），只补测试钉形
  与记档。
- **官方发送锚（abe80d3 第一手实读，行为对齐不抄码）**：
  `stories.preload.ts:556 reactToStory` → `StoryViewer.dom.tsx:649-657` →
  `enqueueReactionForSend.preload.ts`（profileSharing 置位 :72-82）→
  `sendNormalMessage.preload.ts`（reactionForSend 硬编码 remove:false
  :715-722；:430 注释「1:1 story replies should not set story=true」）→
  `SendMessage.preload.ts toProto:637-649`（DataMessage 携
  `Reaction{emoji, targetAuthorAci, targetSentTimestamp, remove:false}` ∧
  `StoryContext{authorAci, sentTimestamp}` 同一作者/同一 story 时刻）。
  1:1 story reaction 腿 = 普通直聊消息**不走 story 通道**；群 story 腿才
  走 `sendToGroup story:true`（本批不做，边界）。
- **引擎发送面**：`sendStoryReaction{account, storyAuthor, storyTimestamp,
  emoji}` → 目的地 = story 作者会话（1:1 腿，号码/UUID 双形态收敛同一
  ServiceId）→ `send_message_with_profile_sharing`（P9 双闸：
  profileSharing whitelist fail-open + 身份拒发闸）；构造面
  `build_story_reaction_data_message` 纯函数单测钉形（Reaction 与
  StoryContext 双锚同值、remove 恒 Some(false)、string 形 authorAci 沿
  mentions/quote/storyReply 先例 binary 位不写）；remove 方法面不带参
  （官方恒 false 硬编码同位）；own-ACI guard 不自设（官方 wire 不拒，UI
  层闸归调用方）。
- **sync/回执面核实记档**：sync 面 = presage fork `send_message` 自带
  普通 sent 转录（自动产出，无独立形状）；无独立回执面（story 查看回执
  = 另一 feature，v1 边界不做）。
- **接收侧判别（零 wire 改动）**：判别键 = `dataMessage.storyReply`
  （E2 批既有投影）——1:1 story reaction 信封无 story 位 → 普通路径投影
  reaction+storyReply 共在、isStory 键不在；群 story reaction 信封 story
  位在场 + storyContext + groupV2 → E2 闸 is_group_story_reply 豁免照投
  （isStory + reaction + storyReply + groupInfo 共在）；malformed 腿（信
  封 story 位 + reaction+storyContext 无 groupV2）官方整信封丢弃
  （MessageReceiver.preload.ts:1534-1544），引擎闸现行同判。story 无会话
  行，宿主按 storyReply 键区分 story 反应与普通反应（按 reaction 锚归
  会话会空悬——锚定面 = storyReply）。
- **测试钉定**：build 钉形（story_reaction_data_message_build_shape）；
  接收三腿（receive_story_reaction_faces——1:1 共在/story 位丢弃/群豁免）；
  fake 契约（send_story_reaction_fake_shape_and_param_fail_closed——
  合法批 `{}`、四参闸 fail-closed、remove 键不读、调度日志、real 未注册
  折叠）。
- **偏差登记**：①群 story 反应发送腿不做（官方 sendToGroup story:true
  形态，v1 边界与群 story 发送同批处理）；②官方发送前 profileSharing
  本地置位（enqueueReactionForSend 落库）引擎无对应写面，由 P9 S3
  whitelist 读闸同语义承接（闸面差异既有登记）。

**story 删除扇出面（S5，2026-10-14，KT 扩展 `deleteStoryForEveryone` +
接收 `syncMessage.storyRecipientUpdate`，engine-compat）**

- **背景与口径**：Stories 块原边界「不做 story 删除（remoteDelete story
  形态）」随本批解除——发送侧 DOE 扇出面 + 接收侧两脸记档；多设备
  storyRecipientUpdate 脸为 wire additive 新增（§3 receive 行）。
- **官方发送锚（abe80d3 第一手实读）**：`sendDeleteStoryForEveryone.
  preload.ts` + `util/deleteStoryForEveryone.preload.ts`——群 story =
  普通 group DOE（既有 remoteDelete 覆盖）；名单 story（1:1 扇出）=
  逐收件人 `sendMessageToServiceId{deleteForEveryone:{isAdminDelete:
  false, targetSentTimestamp: story ts, targetAuthorAci: ourAci},
  timestamp, profileKey(若 profileSharing)}` 走 **story=true sealed-sender
  通道**；之后**恰好一次**自同步 `SyncMessage.Sent{destinationServiceId:
  own, timestamp: story ts, isRecipientUpdate: true,
  storyMessageRecipients: updatedStoryRecipients, expirationStartTimestamp:
  null, urgent: false}`（无 message/storyMessage 面）；零收件人官方仍发
  sync（Special case 腿）；24h 窗官方 throw（isOlderThan DAY）= UI 层
  闸，wire 面不设。
- **引擎发送面**：解析全量前置 + 解析序去重（sendStory 同构）→ 逐收件人
  身份拒发闸 + P9 profileSharing 闸 + fork `send_story_message`（
  DataMessage 载体 `{timestamp, delete{targetSentTimestamp}}`，story 通
  道）→ 全成功或零收件人 → 恰好一次自同步（`Sent{timestamp=story ts,
  isRecipientUpdate=true, storyMessageRecipients=updated}`，best-effort
  不回滚已成功上行，sendStory 同口径）；返回 `{timestamp}`（DOE 信封
  ts）；sync Sent.timestamp = targetTimestamp（官方 story ts 同源）。
- **fork 改动**：`Manager::send_story_message` 加第 4 参
  `include_profile_key: bool`（StoryMessage 恒注入 profile key 不变 +
  DataMessage 调用方显式选入——官方逐收件人 sendMessage profileKey 携带
  同源，注入与否由引擎 P9 profileSharing 闸裁定）；既有 story 发送调用
  点传 false 行为零变化；vendor/README.md fork 清单登记。
- **接收侧两脸**：①他人删除 story = dataMessage.delete + 信封 story 位 →
  E2 闸豁免（is_delete_for_everyone）→ `remoteDelete{timestamp}` +
  `isStory:true`（E2 批既有形状，本批补测试+记档）；②多设备自同步扇出
  = 新脸 `syncMessage.storyRecipientUpdate`（官方
  StoryRecipientUpdateEvent messageReceiverEvents.std.ts:737-750 镜像）—
  门序 = isRecipientUpdate ∧ storyMessageRecipients 非空，判定**先于
  storyMessage 面**（官方 MessageReceiver.preload.ts:3002-3030 同序镜像：
  editMessage → 门 → storyMessage）；timestamp = 信封 ts（官方
  envelope.timestamp 同源），destination 恒自有 ACI 不投；命中分支不落
  sentMessage 脸（isRecipientUpdate/storyMessageRecipients 键不落旧位
  ——矛盾形状不产出）。
- **行为变化记档（门序镜像的代价）**：官方多名单 story 发送 sync 可能
  `isUpdate=true`（sendStory.preload.ts:246/485 isSyncMessageUpdate =
  sentRecipientIds.length>0）——该 wire 形状此前投影
  `sentMessage.storyMessage` echo，门序镜像后归
  `syncMessage.storyRecipientUpdate` 新脸。engine 与 connector 同机同版
  部署、接收侧消费面在 connector 批衔接；跨版本窗口内 connector 对该
  形状按「storyMessage echo 或 storyRecipientUpdate 脸」双形态判读。
- **偏差登记**：①per-recipient 失败官方按 sendStatus 重试，引擎整拒无
  sync（sendStory 既有纪律镜像）；②官方 sync `urgent:false` 位钉定
  proto 无对应字段、`expirationStartTimestamp:null` 缺席即缺省——线面
  均缺席，升级上游需补钉（Stories 发送批偏差①同族）；③24h 窗/story
  查看回执不做（UI 层闸归 connector/宿主）。
- **测试钉定**：DOE body 构造钉形（story_delete_data_message_shape）；
  接收三腿（sent_sync_story_recipient_update_gate_faces——命中/优先级/
  负腿两形）；fake 契约（delete_story_for_everyone_fake_shape_and_param_
  fail_closed——合法批 `{timestamp}`、recipients 缺省空批、
  updatedStoryRecipients 条目逐字段闸、调度日志、real 未注册折叠）。

**媒体 story 发送面（2026-10-14，KT 扩展 `sendStory` attachment 腿，官方
对齐 Signal-Desktop abe80d3，wire additive，engine-compat）**

- **边界翻案记档**：Stories 发送批（E1）原定「文件 story（image 正文
  attachment）v1 不做」，本批明示翻案——①产品需要媒体 story 发布（桌面
  S6 批等它，协议前置）；②收发不对称只剩发送面缺口：接收投影（E2 批）
  早已投 fileAttachment 全脸（locator 指针族 key/cdnKey/cdnNumber/
  uploadTimestamp/digest），官方 `StoryMessage.attachment` 本就
  fileAttachment=3 | textAttachment=4 oneof 双臂（Signal-Desktop
  protos/SignalService.proto:476-479，vendored proto 同构已在）；③上传
  链/自同步面全部既有复用、vendored proto 与 fork 发送路径零改动即承载
  （fork `Manager::send_story_message` 本就 `impl Into<ContentBody>` 泛
  型——无需 S5 批式 fork 叠加，vendor 清单零新增）。**群 story
  （GroupContextV2 位）边界保留不动**（发送面恒 `group: None`，接收面
  群 story 照投的「收得到、发不出」不对称就此只剩群 story 一项）。
- **官方发送锚（abe80d3 第一手实读，行为对齐不抄码）**：媒体 story =
  `ts/jobs/helpers/sendStory.preload.ts:186-190`——正文附件
  `fileAttachment = await uploadAttachment(hydratedAttachment)`（与常规
  消息附件同一 CDN 上行链），`:198-205` 指针进
  `messaging.getStoryMessage`；proto 组装 =
  `ts/textsecure/SendMessage.preload.ts:1037-1064`——textAttachment 优
  先、否则 `attachment = { fileAttachment }`（oneof 恰一），bodyRanges
  恒载于 `StoryMessage.body_ranges=6`（fileAttachment 分支 `:1059-1061`
  同载）；跨名单去重/单时间戳/story 通道/自同步收尾与文本腿共用同一函
  数（E1 批既有面对位）。
- **wire 形状（additive）**：`sendStory` params 增可选 `attachment`（单
  条 data URI 字符串，connector 上游附件形状同款
  `data:<mediatype>[;filename=<pct>][;voice=true][;gif=true];base64,
  <payload>`——filename/voice/gif 参数族与 send 附件同语法同语义：voice
  = VOICE_MESSAGE flags + 官方算法波形/时长生成面、gif = Flags.GIF=8 位，
  指针置位先例全复用）；**两腿恰一**（官方 oneof 同构）：attachment 在
  场 = 文件 story——message 文本腿禁入（同置 -32603，沉默丢正文是
  fail-open）、textAttachment 专属键 textStyle/previewImage/previewUrl/
  previewTitle/previewDate 同禁（oneof 另一臂在场时 proto 无处挂载，
  fail-closed 不静默丢键）；双缺 = 无载荷整拒（message 必填现状口径）；
  null 归缺席（本方法各参数统一口径）；bodyRanges 两腿共用（官方
  fileAttachment 分支同载）。attachment 缺席 = text story 现状零变化
  （wire additive，零改名零移除，既有测试零改动通过）。
- **值域闸与错误面**：methods 层 attachment 非字符串/空串 `-32603`；
  data URI 深解析（mediatype 缺省 application/octet-stream/percent/
  base64/100MB 单条帽）归服务层既有上行闸（send 附件同纪律，超帽/坏形
  fail-closed `-32603` 整批早退）；上行网络失败 `send_error_of` 归一
  （同 send 附件错误面）；成功应答形状与 text 腿同款
  `{timestamp, results:[{recipientNumber, success:{timestamp}}]}`。
- **上传/指针面（与 §3 receive 行互逆）**：attachment data URI → 既有
  `upload_attachments_for_send`（entries 单条、无预览图；presage
  `upload_attachments` 官方 CDN v4 原语：64B 加密 key/padding/digest/
  cdnNumber/cdnKey/uploadTimestamp 全官方产出）→ 返回指针原样进
  `StoryMessage.fileAttachment`。指针字段族与接收投影严格互逆——
  `project_story_message` fileAttachment 臂（E2 批）→
  `project_attachment_pointer` locator 全脸（key 标准 base64/cdnKey 字
  符串/cdnNumber/uploadTimestamp/digest 标准 base64）+ 元数据
  contentType/filename/size 原样读回；sealed-sender 载荷与自同步
  `SyncMessage.Sent.story_message` echo 载同一 story 值，两侧同判。往返
  单测钉定：`media_story_pointer_round_trip_through_receive_projection`
  （上传形状指针 → build_media_story_message → 接收投影 locator 全脸逐
  字段读回 + prost encode/decode 往返后再投影）、
  `media_story_message_shape`（构造钉形：fileAttachment 臂/profileKey
  None 注入面/group None 群边界/bodyRanges 同载）。
- **自同步面复用点（恰好一次不变）**：解析全量前置/ServiceId 去重保首
  现序/逐收件人 story 通道分发/≥1 成功才发
  `SyncMessage.Sent{story_message, story_message_recipients}`/同步失败
  不回滚——E1 批既有全链零改动，媒体腿仅替换构造面
  （`build_media_story_message`）；sync `story_message` 载文件 story 同
  值（官方同函数同收尾），`storyMessageRecipients` 名单扇出面与文本腿
  同形状（destination/distributionListIds=[distributionListId]/
  isAllowedToReply）。
- **接收侧互逆边界（记档不扩收面）**：官方 file story 可携
  `StoryMessage.body_ranges`（引擎发送面照载透传），接收投影
  fileAttachment 臂当前不投影 mentions/textStyles（E2 批钉定形状——
  mentions/textStyles 投影面只挂 textAttachment 臂；不动收面，消费需求
  出现再立项）；本批互逆面 = 指针字段族（任务口径），文本臂投影零变化。
- **偏差登记**：①attachment 恒单条（官方 `[attachment] = attachments`
  取首条同构，无多附件 story 形态）；②voice/gif 参数族语义 = send 附
  件面同款透传（story 正文附件为语音/GIF 时指针位与官方客户端线上陈述
  同构）；③24h 过期/story 查看回执不做（既有边界，UI 层闸归
  connector/宿主）。
- **fake 纪律**：媒体腿参数闸全量生效后成功 `{timestamp, results:[]}`
  （send 系同款，data URI 深解析归 real 上行闸 fake 不可达）；real 模
  式未注册账号折叠 `-32603` 不触网。契约测试
  `send_story_fake_shape_and_param_fail_closed` 扩面（合法批/两腿恰一/
  text 专属键禁入/形状闸/调度日志 44 条）。

群 story 发送面（2026-10-15，KT 扩展 `sendStory` group 腿，官方对齐
Signal-Desktop abe80d3，wire additive，engine-compat）

- **边界翻案记档**：Stories 发送批（E1）原定「群 story（GroupContextV2
  位）v1 不做」，媒体 story 批（2026-10-14）再次确认保留，本批明示翻
  案——①官方发送面群受众是正式域：SendStoryModal.dom.tsx:55-58 受众三
  域 distributionLists ∧ groupConversations ∧ groupStories（:176-182
  群受众入选回显），发送编排 `sendStoryMessage.preload.ts:40-63`
  listIds ∧ conversationIds 双域同一 send；②接收投影（E2 批）早已投群
  story 全链（`project_story_message` group 位 masterKey base64 /
  `is_group_story_reply` 信封闸 / storyReply 回复路由 / sync echo
  storyMessage+storyMessageRecipients），「收得到、发不出」不对称只剩
  发送面缺口；③线格式 vendored proto 已在（`StoryMessage.group=2`
  oneof 同位），fork 发送原语（story 通道单收件人）既有复用、vendored
  proto 与 fork 零改动即承载。
- **官方发送锚（abe80d3 第一手实读，行为对齐不抄码）**：
  - 受众面：`ts/util/sendStoryMessage.preload.ts:40-63`（listIds +
    conversationIds；双缺 drop）、群候选三闸 :210（isGroupV2）/ :218
    （terminated 拒）/ :226-232（announcementsOnly 需 admin）；
    SendStoryModal.dom.tsx:518/625（选中群经 toggleGroupsForStorySend
    置 storySendMode=Always :238-251——宿主会话态，不在引擎面）。
  - 线格式：`Content.storyMessage = 9`（protos/SignalService.proto:115，
    非 dataMessage.groupContext 位）；`StoryMessage{profileKey=1,
    group=2, fileAttachment=3|textAttachment=4, allowsReplies=5,
    bodyRanges=6}`（:473-482）；`GroupContextV2{masterKey=1, revision=2,
    groupChange=3}`（:947-951）；官方组装 `getStoryMessage`
    SendMessage.preload.ts:1022-1067——`group: {masterKey, revision,
    groupChange ?? null}`（story 发送无 groupChange 传入源，恒缺）、
    群值源 `conversation.getGroupV2Info()` conversations.preload.ts:
    1412-1434（本地群态 masterKey+revision）。群定位键 = masterKey
    （32B；群 id 收方派生，processDataMessage.preload.ts:149-169
    processGroupV2Context → deriveGroupFields 同语义）+ revision；与
    storyRecipientUpdate 无关（该面 = 名单扇出 sync 载体，S5 批已接）。
  - 允许回复：群腿 `allowsReplies` 恒 true（sendStory.preload.ts:199
    `allowsReplies: true` 硬编码 + :352-354 `isGroupV2 ||
    Boolean(...)` 短路）——RPC allowsReplies 参数只辖名单腿。
  - 分发面：官方主路 = sender-key 群发（`sendContentMessageToGroup`
    sendToGroup.preload.ts:191-263 → sendToGroupViaSenderKey；story=true
    不用 GSE：:362 `groupId != null && !story`，isValidSenderKeyRecipient
    :1303-1330 story 臂走 story access key）；fallback = 逐收件人
    sendGroupProto（:240-263，官方合法回退）。引擎走逐成员 story 通道
    （fork `Manager::send_story_message` sealed-sender `?story=true`）=
    fallback 同构传输面，与名单腿同一 story 通道纪律（E1 既有偏差族）；
    群成员解析 = 引擎群态（缓存优先 miss 才 fetch，group_mrm R1 同构），
    不归 connector。
  - 自同步：官方群 story sync（sendStory.preload.ts:565-598 收尾 + 群
    分支）——`storyMessageRecipients` 每成员
    `{destinationServiceId, distributionListIds: [], isAllowedToReply:
    true}`（addDistributionListToServiceIdSent :219-244 listId=undefined
    → 空名单位；群 sendState isAllowedToReplyToStory:true
    sendStoryMessage.preload.ts:259-263 → 恒可回位），
    `destinationE164/destinationServiceId` 双缺（:587-589 注释明示群发
    送 undefined）；`storyMessage` 载含 group 的同一 story。引擎恰好一次
    best-effort（≥1 成员成功才发，同步失败不回滚；零成员 solo 群 =
    零发送零 sync，官方 storyMessageRecipients 空 → 不发 sync 同构）。
  - 混合受众：官方 = 每 audience 一个 story job（名单 job
    sendStoryMessage.preload.ts:327-332，群 job :337-378），群时间戳
    `timestamp + index + 1` 保跨腿唯一，各自收尾 sync（两 job 两 sync）。
    引擎同构：名单腿 ts、群腿 ts+1、两条 sync 先后发送；群单腿 =
    recipients 空批 + group 在场（官方受众至少一域 :57-63 双缺才 drop
    同构）。
- **wire 形状（additive）**：`sendStory` params 增可选 `group`（非空
  字符串 = engine groupId，群 master key 标准 base64，与 sendTextGroup
  群 id 同一口径）；应答增可选 `group:{groupId, timestamp}` 群腿面（群
  单腿时顶层 timestamp = 群腿 ts、results 恒空）；null 归缺席。group 与
  内容两腿正交（官方群 story text/media 双臂同源 getStoryMessage——
  text+group、media+group 均可达）；group 缺席 = 现状零变化。
- **fail-closed 闸（逐键理由）**：①group 非字符串/空串 `-32603`
  （静默丢受众是 fail-open）；②groupId 解码非 32B `-32603`（群 id 即
  master key 标准 base64，形状闸 = decode_group_id 既有单点；官方
  deriveGroupFields 对非 32B 直接抛错同构）；③群已终止 `-1` +
  `SEND_TARGET_TERMINATED`（官方 :218-224 拒发位；引擎群发送同款缓存化
  闸）；④群态不可得 `-32603`（revision 无源不发明——官方值源本地群态，
  引擎无群态即无发送面）；⑤announcementsOnly 非 admin `-1` +
  `SEND_TARGET_ANNOUNCEMENTS_ONLY`（官方 :226-232——官方按「跳过该群」
  降级，引擎单群参数面无剩余受众可降，fail-closed 上抛不静默吞，偏差
  ⑥登记）；⑥成员发送失败整体 `-32603`（sendStory 既有 per-recipient
  纪律）。错误面分工：群腿走 `send_error` 结构化单点（-1 词族群发送
  同款），名单腿维持现状 `-32603` 折叠零变化。
- **编排面**：`stories::send_story_group`（群腿独立编排——官方混合受众
  两 job 两 sync 同构，Service::send_story 先名单腿后群腿顺序编排）；
  构造面 `build_story_group_context`/`group_story_sync_recipients` 纯
  函数单测钉形；`build_story_message`/`build_media_story_message` 增
  `group` 透传位（名单腿传 None 零变化）。群 sync 形状接收侧已有面照投
  （`sent.storyMessage` 分支 + `storyMessageRecipients` 位 + story
  group 位，E2/P14 既有）——收侧零改动。
- **接收投影查漏结论（记档不发明）**：直收群 story →
  `storyMessage.group`（masterKey 标准 base64）已投（E2）；自发群 story
  多设备 echo → `sentMessage.storyMessage.group` +
  `storyMessageRecipients` 已投（P14）；群 story 回复路由 →
  dataMessage `group`（groupId）+ `storyReply` 已投（E2）；信封闸
  `is_group_story_reply` 已有（E2）。缺口 follow-up（不本批发明）：
  ①`story.group` 投影只携 masterKey，revision/groupChange 不投（官方
  processGroupV2Context 携带；connector 按 groupId 路由够用，revision
  面消费需求出现再扩）；②官方收侧拒收 blocked 群 story
  （MessageReceiver.preload.ts #handleStoryMessage #isGroupBlocked），
  引擎无群会话态不设闸（宿主裁量）；③群 story 的 replies/查看回执特殊
  语义（官方 handleDataMessage.preload.ts:505/617 群回复过期计时器随父
  story 同步等）刻意不做（任务口径），收侧 storyReply 位已够路由。
- **fake 纪律**：group 在场 = 参数闸全量生效后成功
  `{timestamp, results:[], group:{groupId, timestamp}}`（send 系同款；
  群态/成员解析归 real 面 fake 不可达）。契约测试
  `send_story_fake_shape_and_param_fail_closed` 扩面（群单腿/混合受众/
  group 形状闸/recipients 空批双态/群腿结构化错误面）。

群 story 反应发送面（E-S7r，2026-10-15，KT 扩展 `sendStoryReaction`
groupId 腿，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat）

- **边界翻案记档**：S4 批（2026-10-14）原定「群 story 反应发送腿不做」
  并把群 story 反应线格式记为「story 通道 groupV2 形态」——本批明示翻
  案并订正措辞：官方实锚是**普通群通道 dataMessage**（DataMessage 三键
  共存），非 Content.storyMessage（story 通道）——`enqueueReactionForSend
  .preload.ts:84-91` 群 story `targetConversation = 群本身`（仅 1:1 story
  才换作者会话 `get(targetAuthorAci)`），群臂走 sendToGroup
  （sendNormalMessage.preload.ts:355-383 groupSendOptions 携
  reaction+storyContext）。transport 的 story 位（`story:
  Boolean(storyContext)` :381——群发送 endorsement 跳过/access key XOR
  story 位）不对齐，传输细节，DataMessage 形状不变（边界①）。
- **官方发送锚（abe80d3 第一手实读，行为对齐不抄码）**：
  - 目的地：`enqueueReactionForSend.preload.ts:84-91`（群 story → 群
    本身；1:1 story → 作者会话，S4 批已落）。
  - remove 恒 false：`sendNormalMessage.preload.ts:711-723`
    （reactionForSend 硬编码 `remove: false`，群腿与 1:1 腿同源无
    remove 参）。
  - 三键共存：`SendMessage.preload.ts` toProto——groupV2 :642 /
    storyContext :567-579 / reaction :663-679 三独立键同一 DataMessage
    （DataMessage 字段位非 oneof，官方同帧共存即 wire 事实）。
  - 群上下文值源：群发送既有 `getGroupV2Info()` 本地群态同源（群 story
    发送面批已锚）——引擎侧 = dispatch_group 群态注入单点。
- **引擎发送面**：`sendStoryReaction` params 增可选 `groupId`（非空字符
  串 = engine groupId，32B master key 标准 base64，sendStory group/
  sendTextGroup 同一口径；缺失/null = 1:1 作者直发腿现状零变化逐字节；
  非字符串/空串 fail-closed）。群腿 = 共享群发送路径全单点：
  `open_group_sender`（32B 解码 fail-closed + terminated 硬拒）+
  `dispatch_group`（announcementsOnly 非 admin 拒 group_mrm:225-227 同
  源 + group_v2 自动注入 `{masterKey, revision, groupChange: None}`
  :211-216 同源——MRM 主路 + vendored failover + Sent 转录群发送同款）；
  reaction/storyContext 两键复用 `build_story_reaction_data_message`
  （1:1 腿构造单点，双锚同值、remove 恒 Some(false) 官方同位）；作者
  ACI 解析与 1:1 腿同链同严（本机号→contacts E.164→UUID 直通，不可解
  析 Internal 不透传号码）。群腿不走直聊身份拒发闸/profileSharing 白名
  单（群发送面同口径，sendReactionGroup 同构）；1:1 腿双闸维持现状。
- **应答面（additive）**：群腿 `{group: {groupId, timestamp}}`（groupId
  回显 + 群腿时刻 u64；对齐 sendStory group 应答先例——additive 键缺
  席不在，1:1 腿 `{}` 零变化，无顶层 timestamp/results 面）；群腿错误
  走 send_error 结构化单点（-1 词族 + -32603 折叠，群发送同款），1:1
  腿错误面零变化。
- **fail-closed 闸（逐键理由）**：①groupId 非字符串/空串 `-32603`
  （静默丢群腿是 fail-open）；②32B 解码失败 `-32603`（decode_group_id
  单点，官方 deriveGroupFields 非 32B 抛错同构）；③terminated `-1` +
  `SEND_TARGET_TERMINATED`；④announcementsOnly 非 admin `-1` +
  `SEND_TARGET_ANNOUNCEMENTS_ONLY`（官方 story 面按「跳过该群」降级 vs
  引擎 fail-closed 上抛——sendStory 群腿同款偏差，记档）；⑤作者不可解
  析 `-32603`（不透传号码）。
- **sync/回执面**：sync 面 = 共享群发送路径 Sent 转录自带
  （build_group_sent_transcript，message 载 group_v2+reaction+
  storyContext 同一 DataMessage），无独立回执面（story 查看回执既
  有边界）。接收侧零改动：直收群 story 反应（官方带信封 story 位 +
  三键）→ E2 闸 is_group_story_reply 豁免照投（既有）；本批自发腿信封
  无 story 位（普通群通道）→ 普通路径 reaction+storyReply+group 照投
  （E2 既有投影）。
- **边界记档**：①transport story 位（群发送 endorsement 跳过/access
  key XOR story 位）不对齐——传输细节，DataMessage 形状不变；②canReact
  系官方 UI 闸（allowsReplies/会话成员等）engine 不设，归桌面裁量；③
  remove 恒 false 维持（群腿无 remove 参，官方 reactionForSend 同位）；
  ④群 story 回复（sendText 群腿 + storyReply 两键）已端到端现成
  （05b1bce），本批不触碰。
- **测试钉定**：bin——`story_reaction_group_data_message_three_keys_
  shape`（三键共存逐键镜像钉形）/ `story_reaction_group_gate_faces_and_
  decode_gate`（三闸脸 -1 词族逐字 + 32B 解码闸 fail-closed）；contract
  ——`send_story_reaction_group_fake_shape_and_param_fail_closed`（群腿
  应答 group 面/缺席回归 `{}`/groupId 参数闸/调度日志/real 未注册折叠）。

媒体 story caption（2026-10-15，KT 扩展 `sendStory` caption 键，官方对齐
Signal-Desktop abe80d3，wire additive，engine-compat）

- **边界翻案记档**：S6a 桌面媒体 story 批（desktop `62b2c8f4`）记档边界
  「媒体 caption 不做——引擎 media 腿无键」，桌面 composer caption 输入臂
  与 connector caption 透传批（1.160，排队中）都以本批为前置——本批引擎
  加键翻案该边界。官方 caption 是媒体 story 的正式呈现位：媒体编辑器
  （MediaEditor）caption 输入与 bodyRanges 随媒体发布同批上行，接收端
  viewer 呈现（KT 桌面 S6c c1 件9 read-more 落点即 caption 腿）。
- **官方锚（abe80d3 第一手实读，行为对齐不抄码）**：
  - 线格式：`AttachmentPointer.caption = 11`（protos/SignalService.proto
    :940 `optional string caption = 11`，vendored proto 同构已在，prost
    面 `presage::proto::AttachmentPointer.caption` 既有）。story 媒体臂
    caption 载体 = `StoryMessage.fileAttachment` 指针自身——官方
    uploadAttachment.preload.ts:99 从附件解构 caption、:134
    `caption: caption || null` 随上传返回指针同载（指针即附件描述符，
    getStoryMessage `attachment = { fileAttachment }` 恰一
    SendMessage.preload.ts:1043-1044，无第二 caption 位）。
  - 长度帽：`MAX_BODY_ATTACHMENT_BYTE_LENGTH = 64 * KIBIBYTE = 65536`
    字节（ts/util/longAttachment.std.ts:8）——story caption 输入经
    CompositionInput.dom.tsx:686-692 `Buffer.byteLength(text) > 64 KiB`
    裁决（超长 undo + onTextTooLong toast）。字节面非字符面；引擎
    `str.len()` 即 UTF-8 字节，previewTitle ≤1024 字节纪律同构。
  - 腿归属：官方 textAttachment 无 caption 位（getTextAttachmentProto
    无此键——TextAttachment 只有 text/style/preview/background）——
    caption 恒媒体腿专属；caption 的样式族（MediaEditor
    captionBodyRanges）官方走 `StoryMessage.bodyRanges`（getStoryMessage
    :1059-1061 fileAttachment 分支同载），引擎 bodyRanges 两腿共用面既有
    零新增键。
- **上行路径裁决（实读后可行路径）**：presage 上行 API **原生支持**
  caption——vendored `AttachmentSpec.caption` 字段已在
 （libsignal-service sender.rs），`upload_attachment` 将 `spec.caption`
  原样落返回指针 `AttachmentPointer.caption`（sender.rs spec.caption 直
  通），与官方「caption 随上传面进出指针」同构。引擎走 spec 面传参：
  `upload_attachments_for_send` 增第四参 `caption`（apply_outgoing_
  caption 置位首条正文附件 spec；仅 story 媒体腿传 Some——entries 恒单
  条，send/preview 调用面传 None 零变化），**零 vendor/fork 改动**；
  `build_media_story_message` 维持指针透传零改动（官方返回指针即携
  caption 同构——caption 属上传面元数据，非构造面补键）。gif/voice 的
  「spec 无位→上传后置位」先例不适用：caption spec 有位，走原生面。
- **wire 形状（additive）**：`sendStory` params 增可选 `caption`（非空
  ≤65536 UTF-8 字节字符串）；null 归缺席（本方法各参数统一口径）；应答
  形状零变化（`{timestamp, results, group?}` 面原样——caption 不进应
  答）。caption 与 group 正交（官方群 story media 臂同源
  getStoryMessage）；caption 缺席 = pre-caption 帧逐字节同形（proto 字段
  11 缺席、投影无 caption 键，既有测试零改动通过）。
- **fail-closed 闸（逐键理由）**：①caption 非字符串/空串 `-32603`（静
  默丢 caption 是 fail-open，官方 `caption || null` 的 null 化是客户端
  输入面行为，RPC 参数面不收空串——全方法各字符串键统一纪律）；②超
  65536 UTF-8 字节 `-32603`（官方帽同源字节面；官方超长 undo+toast 是
  输入端软闸，引擎参数面整拒不静默截断——闸序差异记档，闸值同源）；
  ③attachment 缺席且 caption 在场 `-32603`（textAttachment 无 caption
  位 = 无处挂载，媒体专属键越腿同族禁入——与 message/textStyle/preview
  族两腿恰一闸同构的镜像臂）；④data URI 深解析/100MB 帽归服务层既有上
  行闸不变。
- **编排面**：methods 参数闸（帽 + 腿闸）→ `Service::send_story` /
  `stories::send_story`（名单腿）→ `stories::send_story_group`（群腿）
  → `upload_attachments_for_send(caption)` → spec 上行直通返回指针 →
  `build_media_story_message` 指针透传 → `StoryMessage.fileAttachment`。
  名单腿/群腿双编排面同参同闸；自同步 `Sent.story_message` 载同一 story
  值（caption 随指针同载，官方同函数同收尾）。
- **接收侧互逆（零收面改动）**：接收投影 `project_attachment_pointer`
  caption 位既有（E2 批，`caption` Some 才投）——发送面产出的带 caption
  指针原样读回；往返单测钉定（见下）。connector 消费侧：接收帧
  attachment face 已含 caption 键（既有）；发送透传待契约 1.160（排队
  中，以本批契约为准——params `caption` 键逐字过境，仅 attachment 腿可
  达同闸）。
- **测试钉定**：bin——`media_story_caption_pointer_shape_and_roundtrip`
  （caption 在场指针透传 + 接收投影读回 + prost 往返保持 / 缺席面
  pre-caption 帧同形三连）、`apply_outgoing_caption_spec_face`（spec 置
  位单点：Some 置位首条正文附件/None 零变化/preview 位不受染/空批安全）；
  contract——`send_story_caption_fake_shape_and_param_fail_closed`（合法
  批应答零变化/caption null 归缺席/群腿正交/帽边界 ascii+CJK 字节面/
  缺席回归/text 腿拒/caption 单独在场拒/超帽拒/类型闸/调度日志原样落参
  12 条/real 未注册折叠）。

媒体 story 指针回传（2026-10-15，KT 扩展 `sendStory` 应答 fileAttachment
键，官方对齐 Signal-Desktop abe80d3，wire additive，engine-compat）

- **边界记档（本批立项理由）**：engine sendStory 媒体腿上传后 CDN 指针
  （fileAttachment）此前只进线格式，应答不带 pointer；engine 本机零存档
  零自收（vendored presage fork registered.rs 记档「not saved to the
  local store…no sent-transcript」；sync 只上 wire 到其他设备，发送设备
  不自收）——KT 桌面 own 媒体 story 重启后无任何字节来源。官方本机回看
  靠本地盘 path（MyStories.dom.tsx:198-211 点击回看 viewStory(story.
  messageId)、:280-292 详情面同源——消费本地消息库 attachment 字节面），
  KT 无盘字节面（16KiB 帽、dataUri 不落盘是刻意），故**指针进应答是 KT
  本机回看的必要充分路径**（官方同构：单设备场景 sync 不自投；官方发送
  端持有上传指针这一事实本身即本批行为参照——uploadAttachment.preload.ts
  返回值即发送态指针）。
- **投影差记档（官方本地盘 vs KT 指针应答）**：官方 MyStories 回看面
  （MyStories.dom.tsx:198-211/:280-292 + StoryImage
  `attachment={story.attachment}`）消费本地盘 path 字节——桌面有完整本
  地附件存储；KT 桌面无盘字节面，own story 媒体回看缺字节源。本批把该
  差收敛为：engine 应答回传上传指针（= wire 上该 story 的字节定位面），
  桌面/connector 据此走 transit CDN 下载链（locator 面 = 收投影同款下载
  资格闸输入）——own story 消费面与接收 story 完全同构，零新下载语义。
- **键名裁定（fileAttachment）**：①官方 wire 域名——StoryMessage.
  attachment oneof fileAttachment 臂（protos/SignalService.proto:476-479），
  官方发送端持有的指针正是该域；②与收投影同键同构——project_story_
  message FileAttachment 臂键名 fileAttachment = 同一
  project_attachment_pointer 产出，「应答脸 ≡ 接收 own story 投影脸」字
  面恒等（bin 往返单测钉定）；③connector/桌面直投零翻译层——应答键值
  可直接当 own story 的 fileAttachment 消费。pointer/attachmentPointer
  （proto 类型名视角）不如 wire 语义名贴合消费面，不取。
- **指针共享面（实读结论）**：名单腿 = upload_attachments_for_send 单次
  上传单条正文附件 → 单指针，全体收件人共享同一指针（story.clone() 同一
  fileAttachment，逐收件人分发不重传）；群腿同构单指针。混合受众 = 名单
  腿/群腿两编排函数各自独立上传（官方两 job 同构）→ 两指针互异（每次上
  传独立生成加密材料）→ **分键**：顶层 fileAttachment = 名单腿指针，
  group.fileAttachment = 群腿指针；群单腿（recipients 空）顶层键缺席。
- **值形状（全脸，与收投影严格互逆）**：project_attachment_pointer 单点
  产出——id（client_uuid hyphenated，缺省空串投）/contentType/filename?/
  size?/width?/height?/caption?/blurHash?/incrementalMac?+chunkSize?（同
  进同退）/isVoiceNote/isBorderless/isGif（三布尔恒投）/waveform?/
  duration? + locator{key（64B 标准 base64）?, cdnKey?, cdnNumber?,
  uploadTimestamp?（>0 才投）, digest?（SHA-256 标准 base64）?}——缺席键
  不在（no-null）。诚实缺席记档：locator 无 plaintextHash——官方 proto
  AttachmentPointer 无该位（SignalService.proto:940-950 Next ID 21；
  uploadAttachment.preload.ts 的 plaintextHash 是官方本地上传对象私有面
  不进 wire），投影既有口径（v2 locator 批）不发明。互逆保证 = 应答脸与
  收投影（E2 批 fileAttachment 全脸）同一投影函数，构造→投影→应答脸→
  收投影还原整对象恒等（bin 单测钉定）。
- **缺席纪律（skip-if-absent / no-null）**：text 腿 = 键缺席；发送失败 =
  整批错误应答无成功体（失败臂天然缺席）；群单腿名单面 = 顶层键缺席；
  组装单点 send_story_success_body（methods，两分发臂共用）——Option
  None 绝不序列化，缺席与 null 折叠同一无键态，**缺席 = pre-批应答逐字
  节同形**（bin 单测 serde 文本层恒等断言）。
- **fake 面（镜像文档形状）**：合成桩脸 = 元数据脸（contentType 恒
  application/octet-stream、三布尔 false、id 空串）+ locator{cdnKey 恒
  fake-story-attachment, cdnNumber 恒 3, uploadTimestamp = 桩时刻}——不
  发明 key/digest 加密材料（宁缺勿错）；caption 入参在-cap-out（镜像
  「caption 随上传面进出指针」形状）。fake results 恒空纪律不变。
- **wire 形状（additive）**：应答增可选顶层 `fileAttachment`（对象）与
  `group.fileAttachment`（对象）；params 零变化（本批纯应答面）；既有调
  用方缺席形态 = pre-批逐字节同形（旧宿主零变化）。
- **测试钉定**：bin——`send_story_response_pointer_face_round_trip`（全
  脸指针构造→投影→应答脸→收投影还原整对象恒等 + locator 五件/元数据族
  逐字段 + fake 桩脸形状三连）、`send_story_success_body_absent_keys_
  same_shape`（缺席矩阵：双缺席 pre-批逐字节同形/群腿 face 在指针缺席/
  媒体名单腿顶层在/媒体群单腿分键面/no-null 序列化无 null 字面）；
  contract——`send_story_pointer_fake_face_mirrors_doc`（fake 镜像文档
  形状：媒体腿顶层全脸/caption 在-cap-out/text 腿缺席/群单腿分键面/混合
  双面/群腿 text 无指针/缺席回归）。
- **边界（本批红线）**：real CDN 端到端不可跑（无夹具服务器，与 7e3652e
  媒体批同边界——形状/互逆往返单测覆盖）；connector 透传臂与桌面消费是
  后续批（排队中——透传 = 应答键逐字过境，桌面 = own story 回看挂
  locator 下载链），本批零跨仓。

storyContext 接收投影键名裁定（2026-10-15，接收侧零 wire 改动，
tests-only 钉形批，engine-compat）

- **背景与口径（任务前提翻案）**：本批任务输入「storyContext 零消费」
  经开工 grep 实证翻案——接收投影 `project_data_message` 早在 E2 批
  （94bc6f7，2026-10-08）已把 DataMessage.storyContext 投为
  `storyReply {authorAci, sentTimestamp}`，且投影为函数级单点，1:1 腿
  （receive DataMessage 臂）/群腿（同臂 + groupInfo）/sent sync 腿
  （sentMessage 平铺）/editMessage 腿四腿同过；S4 批（4822e99）补
  reaction 共存三腿钉形（1:1 共在/信封 story 位 malformed 丢弃/群
  story 反应豁免）。本批逐腿核实覆盖完整性后**零代码行为变更**，只补
  缺口钉形（binary 优先裁定腿/缺席同形腿/sent sync 腿）与键名裁定
  记档。
- **官方锚（abe80d3 第一手实读，行为对齐不抄码）**：proto 面
  `DataMessage.storyContext = 21`（vendored SignalService.proto:436），
  `StoryContext{authorAci=1(string), sentTimestamp=2(uint64),
  authorAciBinary=3(bytes 16B UUID)}`（:355-359）。官方对 story 的反应
  与回复走**普通消息通道** `DataMessage{groupV2? + storyContext +
  (reaction | body)}` 三键共存（非 Content.storyMessage）：反应锚
  enqueueReactionForSend.preload.ts:84-91（targetConversation = story
  作者或群本身）、Reactions.preload.ts:606-611（群 story 反应走
  NormalMessage 作业）、sendNormalMessage.preload.ts:744-755
  （storyContext 恒带）；回复 = body+storyContext 同通道；1:1 与群同构
  （groupV2 在场与否）。vendored `DataMessage::is_story_reaction` 辅助
  （content/data_message.rs:5-7）同构在。引擎 reaction 投影与
  storyReply 投影同函数平级共在（reaction+storyReply 同信封即官方三键
  共存的投影面）——任务所称「反应脸/消息脸两处」在引擎现实中就是
  project_data_message 一处的两个加性臂，无第二投影点。
- **键名裁定（`storyReply {authorAci, sentTimestamp}` 维持）**：KT
  桌面既有消费位 `src/view/signal/SignalMessageBubble.vue` storyReply
  标记条（`v-if="message.storyReply"` 存在性判断 → 固定文案「回复了
  故事」，桌面扩展批 2026-10-08，本批 git show 只读实证）——消费面
  注释明文「引擎只投 {authorAci, sentTimestamp} 引用指针（无内容面），
  诚实简面」，桌面零翻译消费。候选形否决理由：①`{author,
  sentTimestamp}` 需引擎翻译 authorAci→author，发明翻译层（官方字段
  名直投纪律不容，且桌面注释已钉 authorAci）；②两平键
  storyReplyAuthor/storyReplyTimestamp 稀释 storyReply 单键在场性
  判别面（S4 批宿主判别键 = storyReply 键在场性——reaction 行携
  storyReply = story 锚定反应），破坏既有判别契约。author 解析 =
  binary 16B UUID 优先、string 兜底（官方 isProtoBinaryEncodingEnabled
  双写面 binary 权威，mentions/quote/reaction 先例同款；不一致时
  binary 赢），两者皆缺/不可解析整键缺席宁缺勿错。
- **本批测试钉定（tests 只增，零既有测试改动）**：
  `receive_story_reply_wire_additive_faces` 三腿——①1:1 腿 binary 与
  string 同在不一致 → binary 赢；②缺席同形：story_context 缺席的普通
  正文行与普通 reaction 行 storyReply 键都不在（pre-批帧逐字节同形，
  旧宿主零变化）；③sent sync 腿：自发 story 回复 echo 平铺
  sentMessage.storyReply 照投 + 自发 story 反应 echo reaction+
  storyReply 共在（四腿钉形收口——1:1 string 形/群豁免 binary 形/
  author 不可解析整键缺席/reaction 三腿既有测试已钉，本批不再重复）。
- **边界（本批红线）**：connector storyReply 键透传批与桌面气泡物化
  收尾是后续批（connector 占仓中；桌面标记条已上线，connector 侧
  storyReply 键透传待其契约批），本批零跨仓（desktop 仓只读 git
  show，零写入）；real 端到端不可跑（无夹具服务器）——形状/往返单测
  覆盖；storyReply 引用内容面（story 正文内容指针，桌面标记条固定
  文案不消费）与 story 查看回执仍是边界（E2 批既有边界不翻案）。

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
- **ws 升级层（vendored，2026-10-11 批）**：`ws()` 升级响应 401/403 现保留
  为 `ServiceError::Unauthorized`（vendored `classify_upgrade_response`
  单点）——此前升级失败经 `into_websocket` 整体折叠 `WsError`、状态码
  丢弃，建流遇凭据死亡只走退避重连不停环；修复后 `receive_stream_unbound`
  停环归型在升级层真正可达。wire 零变化不 bump 契约。

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
- **fork 台账**：本批 vendor 面（libsignal-service `websocket/account.rs`
  结构体+注册端点、presage `manager/confirmation.rs` 注册链构造点）已
  补登 `vendor/README.md` fork 改动清单；同批补登的还有此前漏登的
  `groups_v2/manager.rs`（MRM/GSE 地基批）与 `websocket/registration.rs`
  （时序组四）两文件。

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
  （如正常建会话路径）不得清会话。
- **群 outbound sender key 评估（2026-10-11 S4 复核批，结论 = 无需代码，
  维持不做）**：身份替换 archive 会话后，群 sender key 分发台账
 （group_mrm.rs SenderKeyInfo covered 表）按「有活会话的设备面」比对
  ——`session_device_ids` 只含有会话设备（load_session 过滤），换钥 archive
  即 covered 失配 → 下次群发送自动重建 SKDM 并经新会话重发，覆盖面自愈
  无需人工登记失效。outbound sender key 本体（sender_keys 表 own address
  行）不随对端身份换钥轮换：sender key 安全性不依赖对端身份钥（SKDM 按
  会话加密投递，新会话只需重新投递同一密钥材料），官方
  archiveSiblingSessions 同样不触 sender key——复位反而是过度操作（对端
  收到新 distributionId 会多一轮 SKDM 处理，无安全收益）。早期「引擎无
  群成员面」的取舍理由随群写面四域批失效，本条为现行机制论证；inbound
  sender key 不清（群收向自愈）不变。

### 六问批（2026-10-11，官方对齐 Signal-Desktop，wire additive）

发送新方法两枚（`editMessage`/`sendGroupCallUpdate`，§3 新行）+ 发送扩面
（`send` quoteType/quoteBodyRanges）+ 接收投影五面（contact/poll×3/
groupCallUpdate/deleteForMe/quote type+bodyRanges）+ sent echo 三臂重构 +
online 勘误一处。官方锚：processDataMessage.preload.ts:203-281/:395-441、
SendMessage.preload.ts:506-530/:1075-1092、
jobs/helpers/sendGroupCallUpdate.preload.ts:23-100。

- **Q1 editMessage（编辑发送面 + sent editMessage echo）**：新方法
  `editMessage {account, message(必填非空——官方 canEditMessage body 非空
  闸), targetTimestamp(u64), recipient[]|groupId}`，两腿恰一
  （sendMessageRequestResponse 同构，同置/都缺/腿形状非法 -32603）。直聊
  腿 = vendored `send_message_with_profile_sharing`（P9 S3 profileSharing
  闸 + 身份拒发闸沿 send_text 同纪律，逐收件人单时间戳）；群腿 = vendored
  `send_message_to_group`（MRM 主路径硬编码 Content::DataMessage，
  EditMessage 不在其列）+ 内层 DataMessage 手注 group_v2（master key +
  revision——revision 经 `fetch_group_with_endorsements` best-effort，失败
  None，proto 缺席合法、接收端按 0 归线程）。24h 窗口/10 次上限是官方 UI
  层闸（canEditMessage.dom.ts），wire 面不设（API 调用方自担）。sent
  echo：vendored create_multi_device_sent_transcript_content 已把
  EditMessage 转 `Sent.editMessage`（field 10），引擎 Sent 臂投影
  `sentMessage.editMessage {targetSentTimestamp, dataMessage}`——此前该臂
  整信封丢弃。
- **Q2 contact 卡片收投影**：`dataMessage.contact`（repeated）→
  `dataMessage.contact {name?{givenName?, familyName?, prefix?, suffix?,
  middleName?, nickname?}(空串省略，全空键不在), number?/email?: [{value,
  type, label?}], organization?, avatar?: {avatar:<附件指针投影>, isProfile}}`。
  type 词 MOBILE/WORK/CUSTOM 原词、缺省/未知归 HOME（官方
  contactFieldTypeName + Phone.Type switch 默认 HOME 同构；phone/email 两
  枚举同编号，词源统一取 phone）；value 空条目整条丢（官方 `!value →
  undefined` 同口径）、label 非空才投；avatar 指针走附件指针元数据面 +
  isProfile 恒投 Boolean（缺席=false）；全空卡片整键缺席。**address 面
  不投 = gap 登记**（官方 PostalAddress 投影面引擎未立项，后续批）。注意
  vendored `data_message::Contact` 子字段全 `Option<String>`（与
  history.rs contact_attachment 备份导出面的拆名建模不同源，两投影互不
  复用）。
- **Q3 poll 三臂收投影**：`poll {question, options, allowMultiple}` /
  `pollVote {targetAuthor, targetSentTimestamp, optionIndexes, voteCount}` /
  `pollTerminate {targetSentTimestamp}`（PollCreate=24/PollTerminate=25/
  PollVote=26；官方 processDataMessage.preload.ts:395-441 行为对齐）。
  vote targetAuthor 走 targeted_author_json 既有锚面（二进制优先 +
  E.164 映射，pin/unpin 同口径）；optionIndexes Set 去重保首现序（官方
  new Set 同构，空数组 = 清票动作照投空数组）；voteCount 0 透出（官方
  `||0` 同语义）。poll 发送面不做（引擎无 poll 发送方法；send quoteType
  词表也显式拒 poll——官方 quote.type 无 POLL 词）。
- **Q4 groupCallUpdate 收发两面**：收投影 `dataMessage.groupCallUpdate
  {eraId}`（field 19；eraId 空/缺席整键缺席——官方 `eraId != null` 判面
  同域；收侧 background.preload.ts:3535-3543 只在群上下文消费 eraId）。
  发送新方法 `sendGroupCallUpdate {account, groupId, eraId(必填非空)}` →
  dispatch_group（MRM 主路径：group_v2 上下文 + announcements 闸 +
  terminated 硬拒全部沿用群发送系口径）。**偏差登记**：官方
  sendGroupCallUpdate 走 sendToGroup 直发、不设 announcements 闸
  （sendGroupCallUpdate.preload.ts:23-100 无 announcementsOnly 分支），KT
  群发送闸族统一收口故设（同 editMessage 群腿/typing 群臂口径）——
  announcements-only 群非 admin 调用被引擎拒（官方桌面该场景 UI 不出
  口，API 面多一层闸）；groupId 参数层 decode_group_id 闸（32B 标准
  base64，fake 面同口径 -32603，sendCallMessage 群腿同源）。
- **Q5 quote.type + bodyRanges 双面**：收投影 `quote.type` 恒产词
  （NORMAL/GIFTBADGE proto 词原值；缺席/枚举外 coerce NORMAL——官方
  isKnownProtoEnumMember 同构，processDataMessage.preload.ts:227-229）+
  `quote.bodyRanges`（Quote.bodyRanges=6；mention 臂二进制 ACI 优先 +
  author_numbers E.164 映射、MentionAci 字符串兜底；Style 臂归同条目
  `style` 键（消息面 textStyles 词表同源）；NONE composer 永不出、投影
  丢弃对称；未知枚举整条丢；≤64 条与 mentions 收投影 cap 同源）。发送面
  `send` 新参数 `quoteType ("normal"|"giftBadge" 缺省 normal——官方
  isGiftBadge 三元同构)` + `quoteBodyRanges [{number?, style?, start,
  length}]`（mention/style 恰一——官方 toBodyRange 无关联值条目不产；
  词表五值白名单、start/length 超 u32、>64 条 fail-closed；两键须挂
  quote 键对，quote 键对缺席时报错——官方 quote 对象缺席则 type/
  bodyRanges 无处挂载）；vendored `SendQuote` 扩 `gift_badge`/
  `body_ranges` 位，`assemble_quote` 单点同写 type（恒写）与 BodyRange
  条目。官方发送侧不 bump requiredProtocolVersion（构造面不设该字段，
  viewOnce 先例同构）。
- **Q6 同步域四小修**：
  - ① usernameChangeSyncMessage 能力位：D5 批（§5）已置 true 并随设备面
    注册上报，本批核验零改动。
  - ② DeleteForMe 收投影：`syncMessage.deleteForMe {messageDeletes:
    [{conversationId?, messages: [{author, timestamp}]}]}`（官方
    SyncContent.deleteForMe=22）。vendored 接收面已对 presage store 做
    delete_message 消费（registered.rs save_message 臂）且
    `Received::Content` 无条件上抛（save_message 只做 store 消费副作用，
    不拦截上抛）——**零 vendor 改动、零 README fork 登记**，引擎投影纯
    additive 上抛供 connector 删自有库行。author 二进制（ACI/PNI 16/17B）
    优先、字符串/e164 直通；author 不可解析条目丢（vendored store 面
    warn-and-continue 同口径）、组清空后整组丢、全坏整信封丢。
    conversationId 缺席键不在。**v1 只投 messageDeletes 臂**——
    conversationDeletes/localOnlyConversationDeletes/attachmentDeletes
    三型不投（vendored 消费面同界，gap 登记后续批）。
    **P15a 项1 收口**：整会话清除族两臂补投（conversationDeletes/
    localOnlyConversationDeletes，官方 handleDeleteForMeSync 同位
    MessageReceiver.preload.ts:3658-3722，face 见 §3）——手机「删除会话/
    清空聊天历史·为我删除」桌面端会话不同步的缺口就此闭合；
    attachmentDeletes 臂仍不投（§5 gap 保留）。vendored store 消费面仍只有
    messageDeletes 臂（registered.rs `TODO: Conversations, local-only
    deletes, attachments` 上游原样）——conversationDeletes 收包后引擎
    store 行不删，connector 自有库行删除为准；author 解析纪律两臂同源
    （project_delete_for_me_message 单点）。
  - ③ sent echo 三臂重构：Sent 臂 message→editMessage→storyMessage 序取
    第一个在场位（proto oneof 本就互斥，序取即防御性表达）；公共位
    （timestamp/expirationStartTimestamp/destination）三臂共用；全缺整
    信封丢。storyMessage 臂（field 8）复用 story 投影单点（
    `sentMessage.storyMessage`），无 attachment 位 = malformed 整信封丢
    纪律照旧。
  - ④ online 位勘误：官方桌面**仅 typing 消息发送设 online:true**
    （ts/models/conversations.preload.ts:1530 直聊 sendOptions、:1551
    群腿参数，两处字面 `online: true`），call 消息发送链不设 online 位
    （sendUseCallMessage 前无写入）——引擎 presage
    `Manager::send_message` 硬编码 `online_only=false` 与官方 call 面实际
    行为一致，**无偏差**。§5 通话消息块原「发送通路偏差」bullet 与 §3
    sendCallMessage 行同步勘误撤回。

### 群邀请链接族读写面（P13/任务3，2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

预览 + 经链接入群两方法（§3 新行 `previewGroupViaLink`/`joinGroupViaLink`）。
官方锚：getPreJoinGroupInfo（groups.preload.ts:272）、getGroupFromLink
（WebAPI.preload.ts:4892）、joinGroupV2ViaLink（conversations.preload.ts:2802）。
任务书偏差登记两处：①链接密码长度 = **16 字节**（官方
GROUP_INVITE_LINK_PASSWORD_LENGTH = 16，任务书「32 字节」记述有误，§3
`generateInviteLinkPassword` 行同源）；②预览端点 = GET 路径后缀（任务书
「POST v2/groups/join」记述不准确，见下 wire 面对位）。

- **wire 面对位**：预览 = `GET v2/groups/join/<websafe password>`（任务书
  「POST v2/groups/join」记述不准确——官方 urlParameters 是**路径后缀**，
  GET + 群凭据 basic auth）；join/审批 = `PATCH
  v2/groups?inviteLinkPassword=<websafe>`（官方 modifyGroup
  inviteLinkBase64 同形状）。websafe = 标准 base64 `+/→-_`、去 `=`，还原
  补 `=` 到 4 倍数。
- **join 双臂**（官方同构）：`addFromInviteLink == ADMINISTRATOR`（判定 =
  官方 isAccessControlEnabled ANY||ADMINISTRATOR 词表的审批臂取值）→
  add_members_pending_admin_approval=[{added:{presentation}}]（官方
  buildAddPendingAdminApprovalMemberChange 同形，presentation = vendored
  `create_member_presentation(server_params, self 凭据)`，其余位 proto3
  缺省 = 服务器从 presentation 提取）→ `{status:"requested"}`；否则
  direct 臂 add_members（vendored `build_add_member_action_with_credential`，
  Role DEFAULT + join_from_invite_link:false——官方 `joinFromInviteLink:
  null` proto3 缺省同线）→ `{status:"joined"}`。version = joinInfo.version
  +1（饱和不回卷）——申请者非成员读不到群态，join info 即版本基数。
- **400 already-asked 吞没**：官方 400 + body
  `"cannot ask to join via invite link if already asked to join"` 吞为
  成功（requested 臂语义，官方桌面同款强制本地 pending 态）；引擎读响应
  body 匹配该句面吞没，其他 400 诚实 GROUP_UPDATE_FAILED + status。
- **预览投影**：GroupJoinInfo → groupId/name?/description?/memberCount/
  revision/addFromInviteLink 词/approvalRequired/pendingAdminApproval。
  偏差两处：①name/description 解密失败省略键（官方桌面 GUI 兜底
  unknownGroup 不搬，engine 省略键宁缺勿错）；②avatar URL 不投影（官方
  桌面 CDN 代理面取图，engine 读面无消费方，**gap 登记**后续批）。
- **receive 投影三面细化（P13/任务5，2026-10-11，官方 extractDiffs 对齐
  groups.preload.ts:4705-4749/:4841，isAccessControlEnabled =
  ANY\|\|ADMINISTRATOR，groups/util.std.ts:9）**：`groupChanged` changes[]
  链接面由「access-invite-link/group-link-reset 无条件各一条」改为批内
  组合裁定——access+password 同批（官方 buildNewGroupLinkChange「启用即
  双面同批」同构）→ enabled（access ≠ UNSATISFIABLE）投
  `group-link-add{privilege}`、disabled 投 `group-link-remove`，不另发
  reset；仅 password → `group-link-reset`；仅 access → enabled 投
  `access-invite-link{newPrivilege}`（官方「已启用改权限」形——wire 无
  旧态不区分初次启用/改权限，**偏差登记**）、disabled 投
  `group-link-remove`。官方按 old/new 态差分裁定（linkPreviouslyEnabled/
  linkCurrentlyEnabled），wire change 不携旧态，批内组合是近似——
  groupChanged 事件消费面足够。**member-add-from-link 面**：add_members
  `join_from_invite_link=true` → `member-add-from-link{aci}`（官方
  extractDiffs joinedFromLink，groups.preload.ts:4841，序在 pending 邀请
  /管理员批准之后）——vendored decrypt_group_change 丢该位，投影函数在
  解密消费前从原始 proto actions 二次解码补集（decrypt_service_id 回
  ACI，二次解码/解密失败空集兜底宁缺勿错，§3 面无新键）。
- **fake 两桩**：preview → `GROUP_INVITE_LINK_PREVIEW_FAILED` 无 status
  （fake 无链接读面，材料不可得即错误面）；join → `{status:"joined"}`
  direct 臂形状（fake 无 join info 读面不裁定审批臂——**登记偏差**，
  审批臂形状由 real 面承载）。调度日志：preview 读面不落（材料面纪律）、
  join 写面落（mutating 面纪律，inviteLink 键逐字保留——wire 面 20+ 调用
  方本就明文携链接，摘要有意不设）。

### 群审批面（P13/任务4，2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

撤回申请 + 管理员批准两方法（§3 新行 `cancelJoinRequest`/
`approveJoinRequest`）。官方锚：`#denyPendingApprovalRequest(ourAci)`
（conversations.preload.ts:683/:2878）、approvePendingMembershipFromGroupV2
（ducks/conversations.preload.ts:3646）、buildDeletePendingAdminApprovalMember
Change（groups.preload.ts:1045）、buildPromotePendingAdminApprovalMemberChange
（groups.preload.ts:1391）。

- **wire 面对位**：cancel = 重拉 join info 取 version（申请者非成员读不到
  群态——joinGroupViaLink 同基数纪律）→
  `delete_members_pending_admin_approval=[{deleted_user_id = own ACI
  加密}]` → `PATCH v2/groups?inviteLinkPassword=<websafe>` 链接授权写面
  （`patch_group_via_link_raw` 与 join 共用件——cancel 只认 2xx，join 的
  already-asked body 吞没面 cancel 不适用）；approve = 群态读 →
  `promote_members_pending_admin_approval=[{user_id = ACI 加密, role
  DEFAULT}]` → 普通 PATCH（成员写无需 link param，官方 modifyGroupV2
  `usingCredentialsFrom: []` 同款；version = 群态 revision+1 饱和）。
  两面写后本地群态刷新 + group_cache 失效（群写面纪律）。
- **闸面对称性（官方同位）**：approve 保留官方 isMemberRequestingToJoin
  本地闸同语义——recipient 非 ACI（官方 strictAssert isAciString 同义）或
  不在 `members_pending_admin_approval` 整拒（API 面不静默）；cancel 官方
  本地闸（conversations.preload.ts:693 同位）引擎不设——申请者非成员无群
  态读面，本地无从判定 pending 真值，写意图交服务器裁决（**偏差登记**：
  重复撤回 = 服务器 400 诚实 GROUP_UPDATE_FAILED，官方 GUI 前置闸是交互
  语义非 wire 语义）。
- **ban 配对面不入本批**：官方 buildDeletePendingAdminApprovalMemberChange
  携带 `_maybeBuildAddBannedMemberActions`（撤回即拉黑可选项），engine 不做
  ban 面（**gap 登记**后续批，与 updateGroup P3/P4 批「不做 ban」同口径）。
- **fake 两桩**：cancel → `{status:"cancelled"}`、approve →
  `{status:"approved"}`（leaveGroup 形状桩同纪律）；参数面 fail-closed 闸
  fake 同样生效（cancel 走 parseInviteLink 收口、approve 走 groupId 形状 +
  recipient 形状闸）。调度日志：两面皆写面落（mutating 面纪律，cancel
  inviteLink 键逐字、approve recipient 键逐字）。

### P14 proto 进阶投影收尾·组 1 收投影缺（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

proto 全字段穷举审计（七项）落地：全部为「收了未投影」或「少发可选字
段」的 additive 面，无删改既有键，旧宿主零变化。官方锚逐项：

- **TypingMessage.groupId=3**：群 typing 面位（官方 desktop 与 1:1 同信封
  分发，`isTypingMessage` 消费 groupId 归群会话；官方严格无 1:1 群位混用
  闸）。投影 master key 标准 base64 非空才投；1:1 缺席键不在。
- **Preview.date=5 两端**：链接预览文章发布时刻（毫秒）。收端 dataMessage
  previews[] 与 story textAttachment preview 同投（官方 story 预览同走
  processPreview——MessageReceiver.preload.ts:2183 → processDataMessage
  .preload.ts:297-310）；>0 才投（官方 cleanLinkPreviewDate falsy 同口
  径）。发端 `send.previewDate` / `sendStory.previewDate` 透传
  （SendMessage.preload.ts:687 同位），0 归缺席、非整数 fail-closed、须
  挂 preview 宿主键对（quoteType 同先例）。
- **expireTimerVersion=23**：计时器版本单调位（官方 updateExpirationTimer
  乱序丢弃判据）。>0 才投——0 系 presage restore_thread_timer 对无计时器
  线程注入的 INITIAL 初值（store/manager/registered.rs restore_thread_
  timer unwrap_or_default 全量注 Some(0)），全量透出会给每条无计时器
  消息加零信息键；版本化语义 1 起（E 批 setExpirationTimer version 单调
  同源）。
- **requiredProtocolVersion=12（弱面，仅登记）**：官方强校验 = required >
  本端支持即整消息拒收（processDataMessage 分发闸）。引擎维持「宁收勿
  丢」：不 enforce、不拒收，只把值投影给消费方自判；>0 才投（0 = INITIAL
  无要求语义；我们发送面从不 bump——构造面不设该字段，presage 注 0，
  viewOnce/quote 先例同构）。
- **blurHash / incrementalMac+chunkSize live 投影**：导出面
  `history.rs::project_pointer_common` 2026-10-06 已有，live 接收信封
  （`project_attachment_pointer`）本批补齐同形——blurHash 非空才投；
  incrementalMac（标准 base64）+chunkSize 同进同退、齐备才保留（官方
  filePointers.preload.ts:84-87 同构）。导出/消费面零改动。
- **Sent 三位**（官方 SentEvent 同名直投，MessageReceiver.preload.ts
  :2097-2101 解构同位）：`isRecipientUpdate=6`（仅 true 投）、
  `unidentifiedStatus=5`（条目 destinationUuid binary 16/17B 优先字符串
  兜底/unidentified Some 才投/destinationPniIdentityKey 公钥字节标准
  base64 非空才投）、`storyMessageRecipients=9`（destinationUuid 同上/
  distributionListIds 原样数组空批照投/isAllowedToReply Some 才投）。
  sent 三臂（message/editMessage/storyMessage）公共位共用。三位均为多
  设备发送状态的可选转录面，缺席键不在，旧宿主零变化。

### 官方强校验宽容面三点（宁收勿丢，登记不收紧）

官方三处强校验我方取宽容面，行为**不收紧**（收紧 = 丢消息，与「宁收勿
丢」红线冲突），本块补 §5 记录（此前散见无成套条目）：

- **dataMessage.timestamp ≡ 信封戳**：官方 processDataMessage.preload.ts
  :532-545 对 dataMessage.timestamp 缺席或 ≠ 信封 client timestamp 整消
  息抛错。引擎投影 `dataMessage.timestamp = unwrap_or(0)`（D1 自动回执
  缺席回退信封戳同域），从不因不匹配丢弃。
- **Sent.timestamp strict**：官方 #handleSentMessage `strictAssert
  (timestamp, 'Missing sent timestamp')`（MessageReceiver.preload.ts
  :2113）。引擎 `sent.timestamp.unwrap_or(envelope_ts)` 缺席回退信封戳，
  不丢弃。
- **未知 flag 抛错**：官方 assertDev flagCount ≤1（processDataMessage
  .preload.ts:612-622，dev 断言）。引擎 flag 位只读不 enforce（多 flag
  共存按位掩码各自命中，expiration_receive_projection_faces 单测钉），
  未知 flag 位不弃行。

### P14 proto 进阶投影收尾·组 2 payment+giftBadge 收投影（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

- **payment=20**（官方 processDataMessage.preload.ts:171 processPayment 行
  为对齐）：`{kind, note?}` 事件投影——notification 臂 = `NOTIFICATION`
  + note（官方 `note ?? null` null 填位镜像；receipt 字节官方事件面同样
  不投）；activation 臂官方折叠 kind 词：type REQUEST→
  `ACTIVATION_REQUEST`、ACTIVATED→`ACTIVATION`（SCREAMING 词表同
  quote/contact 惯例；官方 PaymentEventKind 是 TS 侧数字枚举 1/2/3，词形
  取投影惯例非直抄）；type 缺省/词表外 = 官方 fall-through 整键不在。
  payment 缺席/item 缺席整键不在。引擎 metadata-only 无 MobileCoin 节点/
  余额面，Amount 位官方通知事件同样不投（金额面归消费方）。
- **giftBadge=22**（官方 processGiftBadge:476 行为对齐）：
  receiptCredentialPresentation 缺席/空 = 官方 undefined 整键不在；在场
  则 zkgroup `ReceiptCredentialPresentation`（v0.99.0 同 tag 既有直依赖，
  云备份凭据同源——**零新增依赖**）解析取 expiration（秒→毫秒，官方
  `getReceiptExpirationTime() * SECOND` 同算）/level，连同标准 base64
  原件与 state。state 词 = `UNOPENED`（桌面 GiftBadgeStates.Unopened 的
  SCREAMING 投影形，词表惯例同 quote/contact；官方 'Unopened' 原词大小写
  漂移登记在案）——receive 时点恒初态，开启/兑换状态机归消费方；官方
  `id: undefined` JSON 面无键不投。
- **宽容面（不镜像官方 throw）**：官方解析失败 = ReceiptCredential
  Presentation 构造 throw（炸整条消息处理，消息丢弃）。引擎「宁收勿丢」
  红线不镜像——降级只投 `{receiptCredentialPresentation, state}` 两键
  （材料保留可补算），不弃消息不炸信封。
- **单测**：`data_message_projection_carries_payment_and_gift_badge_faces`
  ——测试内模拟 Signal 服务器签发全链（ServerSecretParams/ServerPublic
  Params deterministic randomness → request context → issue → receive →
  presentation）钉全四键形状 + 空 presentation 整键不在 + 坏字节降级两键。

### P14 proto 进阶投影收尾·组 3 附件 flags BORDERLESS/GIF（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

- **BORDERLESS=2 / GIF=8**：`project_attachment_pointer` live 面（接收信封
  + quote thumbnail + sticker data + preview image 同一投影函数全生效）
  增 `isBorderless`/`isGif` **恒投**布尔——镜像 isVoiceNote 先例（无
  flag 位 = false，三布尔同形消费面无需判键；多 flag 共存按位掩码各自
  命中，未知 flag 位宽容不弃行）。
- **官方 isGIF 附加闸不镜像**：官方 Attachment.std.ts:321 `isGIF` =
  flag 命中 **∧** `isVideoAttachment`（contentType video/*）双闸；引擎
  flag-only 如实投位，媒体类自判归消费方（宁多投不误判；gif 闸是官方
  UI 渲染语义非 wire 语义，消费方持 contentType 可同算）。
- **导出面零改动**：backup 导出 `attachment_record` 恒产原值 `flag`
  数位（历史形状），不引入布尔形——wire additive-only 不动既有键。

### P14 proto 进阶投影收尾·组 4 story 细节（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

- **story bodyRanges=6 收投影**：StoryMessage.message 级 `body_ranges`
  分流 textAttachment 级 `mentions[]`/`textStyles[]`（dataMessage 同构）——
  mention 臂 binary 16B 优先 string 兜底 + E.164 contacts 映射（新增
  `resolve_story_mention_numbers`，sent.storyMessage 多设备转录臂与
  StoryMessage 接收臂同解析）；style 臂五值词表 NONE 丢/未知枚举值丢，
  start+length 超 story 正文 UTF-16 长度的形状坏条目丢弃，条目保持 wire
  顺序。
- **textAttachment 色位/背景收投影**：`textForegroundColor=3`/
  `textBackgroundColor=4` u32 数值原样（Some(0) 也是线上陈述——显式 0 =
  合法色值，官方 `?? null` 面同语义）；`background` oneof（gradient=6 /
  color=7）→ `background {gradient{startColor?, endColor?, angle?, colors[],
  positions[]}}` 或 `background {color}`（官方 SendMessage TextAttachment
  Params 同形键；gradient 弃用位 startColor/endColor 在场照投）。
- **storyContext 发送面**：`send` 增 `storyReplyAuthor`/`storyReplyTimestamp`
  双参同进退（同置=story 回复、单边在场 `-32603`；timestamp ≤0/非整数/
  author 空串 `-32603`；author ACI/号码服务层 `resolve_author_aci` 再解析，
  不可解析 fail-closed）。DataMessage.story_context = `{sentTimestamp,
  authorAci}`——**author 形态偏差登记**：官方 binary-enabled 模式写
  `authorAciBinary`（16B raw uuid）、否则 `authorAci` 字符串（二选一），
  引擎沿 mentions MentionAci/quote author string 先例写 `authorAci`（收端
  binary/string 两侧都读，互通无差；预 P15 binary 面统一切换）。
  `send_text`/`send_text_group` 直聊/群两腿同参数（群 story 回复带
  groupContext 官方同构）；viewOnce 闸不拦 storyContext（官方
  isValidTapToView 不查该位，同构不越界）。
- **单测**：`story_projection_carries_body_ranges_and_color_faces`（分流+
  坏条目丢弃+gradient/color 两臂）、`story_context_send_build_faces`（构造
  三面：ACI 解析/零回归/不可解析 fail-closed）。

### P14 proto 进阶投影收尾·组 5 sync 七整臂收投影（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

syncMessage.content oneof 的七位整臂（Configuration/FetchLatest/
CallLinkUpdate/CallLogEvent/DeviceNameChange/UsernameChange/
PniChangeNumber）此前全部落兜底臂（read/viewed 面）整信封丢弃；本批
additive 接投影，官方 dispatch 七位同位（MessageReceiver.preload.ts
:3084/:3111/:3123/:3129/:3141/:3153/:1586+3362）。

- **configuration**：readReceipts/unidentifiedDeliveryIndicators/
  typingIndicators/linkPreviews 四旗 Some 才投（官方 handleConfiguration →
  ConfigurationEvent 原样携带 proto 对象，消费面 storage boots 同位）。
- **fetchLatest `{type?}`**：枚举词 SCREAMING（UNKNOWN/LOCAL_PROFILE/
  STORAGE_MANIFEST/SUBSCRIPTION_STATUS）；缺席/未知 i32 照投空对象
  （官方 FetchLatestEvent 原样透传 type、UNKNOWN 下游 no-op 同位——
  信号面即事件，不再发明语义）。
- **callLinkUpdate `{type?, rootKey?, adminKey?}`**：rootKey/adminPasskey
  空字节省键（官方 Bytes.isNotEmpty 同口径）；**adminKey 键名沿官方事件
  面** CallLinkUpdateSyncEvent（proto 字段叫 adminPasskey）。
  - **偏差①（宁收勿丢不镜像 throw）**：官方 type==null 与未知 type 均
    throw 整信封丢弃；引擎 type 缺席投空对象、未知 i32 丢词只投其余键。
- **callLogEvent `{type?, timestamp?, conversationId?, callId?}`**：官方
  getCallLogEventForProto 透传面同构（callDisposition.preload.ts:281）；
  type 词 SCREAMING；**timestamp 键名沿 proto**（官方 targetTimestamp 是
  zod 归一名，wire 材料同源）；conversationId 原字节标准 base64（1:1
  service-id/群 id/ad-hoc roomId 三形由消费方自判，官方 peerId 双路解释
  同构）；callId uint64 原值。
- **deviceNameChange `{deviceId?}`**：官方 handleDeviceNameChangeSync 按
  本机 deviceId 过滤（deviceId null 丢弃、≠localDeviceId「meant for other
  device」丢弃），引擎恒投原值由消费方自判（引擎无本机设备 id 判面）。
- **usernameChange `{}`**：空消息纯信号面（官方 handleUsernameChangeSync
  只发 UsernameChangeSyncEvent 触发 profile 重取，零字段消费——事件到达
  即信号，投影空对象保事件位）。
- **pniChangeNumber**：只投 `{registrationId?, newE164?}`。
  - **偏差②（密钥材料不出引擎边界）**：identityKeyPair/signedPreKey/
    lastResortKyberPreKey 三键不投（先例：backup key/凭据面同纪律）。
  - **偏差③（PNI re-key 消费不做）**：官方全键消费 + identity check +
    本机换钥全链，引擎 gap 不做（投影 additive 上抛，消费方另行立项）。
  - **偏差④（无 sourceSelf 闸）**：官方 envelope.sourceServiceId !==
    ourAci 即 throw；引擎信封面不自证，照投（宁收勿丢）。
- **wire 形状**：全部缺席键不在（旧宿主零变化）；七臂投影后不再落兜底
  臂（read/viewed direct-repeated 面与七臂互斥，线上不共存）。
- **单测**：`sync_seven_arm_projection_carries_p14_faces`（七臂形状 +
  FetchLatest/CallLinkUpdate/DeviceNameChange 缺席臂空对象 + PNI 密钥
  三键不在 + Configuration 四旗两在两缺）。

### P14 proto 进阶投影收尾·组 6 自发写面（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

- **Keys.mediaRootBackupKey 收投影**：`syncMessage.keys {mediaRootBackupKey?}`
  （content oneof keys 臂此前落兜底臂整信封丢弃）。官方 handleKeys 事件面
  三键 {masterKey, accountEntropyPool, mediaRootBackupKey}
  （MessageReceiver.preload.ts:3338-3358），引擎只投 mediaRootBackupKey：
  - **偏差⑤（密钥材料不出引擎边界）**：accountEntropyPool 不投（AEP =
    账号主熵材料，PNI 密钥/凭据面同纪律）。
  - **偏差⑥（钉定 proto 缺字段）**：masterKey 在钉定 rev 的 vendored
    proto 已 reserved 删字段（prost 跳未知字段无法投影）；官方线面若携
    master=1 亦只能丢弃，属 proto 缺口（P15 升版项）。
  - 空字节省键（官方 Bytes.isNotEmpty 同口径）；键全缺照投空对象（信号
    面即事件，fetchLatest 同纪律）。Request{Keys} 应答停答面（P9 S2）与
    此接收投影面互不相干。
- **markRead/markViewed 自发写面**（新 JSON-RPC 方法，官方
  syncReadMessages/syncView 同构 SendMessage.preload.ts:2097/:2145）：
  向自己 ACI 发 SyncMessage.read=5/viewed=16（直接 repeated 字段，非
  content oneof——接收 echo 面 = D2 兜底臂投影，本批接收臂/自发写面两端
  齐备）。params `{account, reads?|viewed?: [{sender, timestamp}]}`：
  - sender 号码/UUID 双解析（resolve_author_aci，viewOnceOpen 同口径）；
    **任一条目不可解析整拒 `-32603` 不发半批**（发送面参数闸纪律——官方
    normalizeAci throw 同口径；接收面宁缺勿丢逐条丢是收侧宽容，两面不对称
    属官方同构行为）。
  - ACI binary+string 双写（官方 isProtoBinaryEncodingEnabled 现行面/旧
    字符串面双兼容；E4 viewOnceOpen 双写先例同构）。
  - **100 条/chunk 分信封**（官方 syncHelpers.preload.ts:25 CHUNK_SIZE=100
    同构；每 chunk 独立 padded SyncMessage self-send）。
  - 与 sendReadReceipt/sendViewedReceipt（作者面 ReceiptMessage 回执，
    connector §4.29）**双通道独立**：本面向本机其他设备同步读态（官方
    markConversationRead.preload.ts:252 注同位——syncReadMessages 发自己
    设备、sendReadReceipts 发原作者）。
  - presage self-send 通路与 sendViewOnceOpen 同款（§5 E4 已验证；钉定
    rev 同内容二次转录重发怪癖同面——接收端 read sync 幂等，无副作用）。
  - fake：成功 `{}`，落调度日志（reads/viewed 键逐字保留，协议上行面纪律）。
- **单测**：`keys_projection_carries_media_root_backup_key`（base64 投位 +
  AEP 不投 + 全缺空对象）、contract `mark_read_mark_viewed_sync_contract`
  （fake 成功 + 调度日志逐字 + 参数面八形状 fail-closed）。

### P14 proto 进阶投影收尾·组 7 扩项核对批（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire 无变化不改码）

四项逐一核对结论（两项核实已闭合、两项 vendored 边界跳过归 P15）：

- **Envelope.sourceServiceIdBinary 投影——核对闭合，无需改码**：vendored
  解密面 `cipher.rs` `envelope.parse_source_service_id()`（binary 16B ACI/
  17B PNI 优先、字符串兜底 `parse_service_id_with_fallback`）→
  `Metadata.sender` → 引擎 `sourceUuid` 投影——官方 decodeEnvelope
  （MessageReceiver.preload.ts:301 `sourceServiceId || source`、binary 为
  现行面）同构。binary-only 信封（无字符串 source）照常解析，无缺口。
- **CallLinkUpdate 经 Storage 通道冗余送达——核对闭合，无需改码**：官方
  发侧双通道——① Storage Service 写 CallLinkRecord（manifest/item 持久
  态，storageRecordOps.preload.ts toCallLinkRecord/toPendingCallLinkRecord）
  + ② CallLinkUpdate sync message（rootKey/adminKey live 面）；官方收侧
  sync 面 = handleCallLinkUpdate 消费 rootKey/adminKey（组 5 已接投影），
  Storage 面 = storage sync 时把 CallLinkRecord 落本地库（持久态恢复/
  新设备初始化场景）。**结论：sync-message 通道即 rootKey/adminKey 的
  live 送达主通道，组 5 接投影后接收面已闭环**；Storage 通道为持久态
  冗余，引擎 Storage 读面只有 pinned/blocked 专用面、通用写面白名单显式
  fail-closed 拒收 CallLink arm（§3 storage.write 行同域）——CallLinkRecord
  消费不做的 gap 维持登记，消费方提出需求再立项。
- **Envelope.ephemeral/urgent 消费面——vendored 边界跳过，归 P15**：官方
  proto 注释锚 ephemeral=12「recipient offline 不持久化」（服务器离线
  队列语义为主，官方桌面 MessageReceiver 对该位零消费——grep 全文无命中）、
  urgent=14 default true（官方消费 = 桌面自身信封队列优先级位 +
  logUnexpectedUrgentValue 校验日志，后者 LOG_UNEXPECTED_URGENT_VALUES=
  false 缺省关闭——均非协议安全面）。引擎接收循环只见
  `Received::Content`（vendored `Received` 六变体无 Envelope 载体），两位
  在 vendored decrypt 的 Metadata 构造处丢弃（Metadata 无字段）；捕获投影
  需 vendored Metadata 扩位（story bit 同型 fork 先例）——**vendor/ 不动
  硬边界，跳过归 P15**。
- **report_spam_token 捕获投影——vendored 边界跳过，归 P15**：token 在
  vendored decrypt Metadata 构造处丢弃（Metadata 仅 server_guid/story），
  捕获同样需 vendored Metadata 扩位——同上跳过归 P15；上方 reportSpam
  块「token 不透投影」边界维持，处置由「上游 Metadata 升版」订正为
  「vendored Metadata 扩位（story bit 同型 fork），P15」。

### P15a 批（2026-10-11，官方对齐 Signal-Desktop abe80d3，wire additive）

- **项1 DeleteForMe 整会话清除族收投影**：`syncMessage.deleteForMe` 补
  `conversationDeletes` / `localOnlyConversationDeletes` 两臂（face 见
  §3；官方 handleDeleteForMeSync 同位 MessageReceiver.preload.ts:
  3658-3722）。手机「删除会话/清空聊天历史」选「为我删除」时桌面同步的
  缺口闭合。纯引擎投影（零 vendor 改动——vendored `Received::Content`
  已无条件上抛 DeleteForMe 体）；投影纪律：
  - conversationDeletes 组 = conversation（必需，缺席/不可解析整组丢，
    官方「No target conversation」warn-drop 同口径）+ isFullDelete（恒
    布尔，官方 `Boolean(item.isFullDelete)` 同构）+ mostRecentMessages
    （清空整组丢，官方同口径）+ mostRecentNonExpiringMessages（空数组
    照投，官方 map+filter 无空判同构）。
  - localOnlyConversationDeletes 条目 = conversation（必需，缺席丢）。
  - 与既有 messageDeletes 臂的偏差保持：messageDeletes 组无 conversation
    照投、conversationId 键省略（Q6② 既有口径不动，官方对该臂实为
    warn-drop——引擎更宽容，宁收勿丢）；两新臂从官方严格口径。
  - author/timestamp 解析抽 `project_delete_for_me_message` 单点（两新臂
    复用；author 二进制优先、e164 直通、不可解析条目丢、timestamp 缺省
    0——Q6② 既有口径）。
  - 全部投影臂皆无有效条目 → 整信封丢（官方「Nothing found in sync
    message」throw 同口径）；attachmentDeletes 臂仍不投（信封只含该臂时
    引擎整信封丢——gap 保留，消费方提出需求再立项）。
  - vendored store 消费面不动：registered.rs 仍只消费 messageDeletes
    （`TODO: Conversations, local-only deletes, attachments` 上游原样），
    conversationDeletes 收包后引擎 store 行不删、connector 自有库行删除
    为准（如实登记）。
- **项2 ephemeral/urgent/reportSpamToken 捕获投影（vendored Metadata
  扩位，story bit 同型 fork）**：P14 组 7 两项「vendored 边界跳过归
  P15」就此收口——`Metadata` 增 `ephemeral/urgent/report_spam_token`
  三位（libsignal-service `content.rs` + `cipher.rs` decrypt 构造点；
  presage/store-sqlite 五处字面量补 `None`/`_` 编译面，fork 清单
  vendor/README.md 登记），引擎 `project_content` 信封级投影
  `urgent`/`ephemeral`/`reportingToken`（face 见 §3 信封公共位行）。
  行为锚：官方信封事件面 MessageReceiver.preload.ts:1177-1180——urgent
  原样透传、reportingToken `Bytes.isNotEmpty` 闸、base64 落会话属性
  （conversations.preload.ts updateReportingToken）；官方桌面对
  ephemeral 零消费（服务器离线队列语义位），引擎如实投位由消费方自判。
  键名 `reportingToken` 沿官方事件面（proto 字段 reportSpamToken，组 5
  adminKey 先例同款）。两标志 raw 位投影、缺席键不在（urgent wire 缺席
  = proto 缺省 true、ephemeral 缺席 = false，旧宿主零变化）；不落库
  （store-sqlite destructure `_`，story 位同款——信封位只活接收现场）。
  reportSpam 块「token 不透投影」边界同步废止（§5 reportSpam 块勘误）。
- **项3 contacts sync 收投影（vendored `Received::Contacts` 载体扩位）**：
  官方联系人同步 blob（SyncMessage.contacts + attachment blob 下载解析，
  ContactsMessage→ContactDetails 处理路径）此前引擎零消费（仅 ack）——
  vendored presage 本就经 `retrieve_contacts`（既有附件下载面复用，零新
  平行机制）解析入库，但 `Received::Contacts(EnvelopeAck)` 载体把解析产物
  拦在 presage 内。本批改 `Received::Contacts(ContactSync, EnvelopeAck)`
  （载体 = 源信封 + complete 旗标 + 解析且落库成功记录，fork 清单
  vendor/README.md 登记），引擎投影 `syncMessage.contacts {complete,
  records[]}` 事件上抛（face 见 §3；官方 handleContacts→doContactSync
  upsert 同位）。偏差如实登记：①records 只含落库成功条目（vendored save
  循环 break-on-error 上游语义，break 后剩余条目不投影——store 面 =
  投影面同界）；②avatar 字节不投影（官方落盘会话头像，engine 读面无
  消费方，gap 登记后续批）；③expireTimer/expireTimerVersion/
  inboxPosition 0 与缺席不可分（presage Contact u32 模型），0 不投；
  ④无 aci 纯号码条目上游 `from_proto` MissingUuid 已滤（官方允许 e164-only
  条目——模型面偏差，§5 如实登记）；⑤blob 本体不投（二进制）。
- **项4 Blocked modern 三型（vendored proto 升级，§5 proto 变更登记）**：
  上游 abe80d3 的 `SyncMessage.Blocked` 新增 modern 三型
  BlockedE164(5)/BlockedAci(6)/BlockedGroup(7)（带 timestamp 位），引擎
  vendored proto 钉定 rev（libsignal-service 9e6c08b）无此三位——本批
  逐字对照 Signal-Desktop `protos/SignalService.proto:581-605` 同步进
  `vendor/libsignal-service/protobuf/SignalService.proto`（含 deprecated
  注释原样；**wire additive-only**：新增 field 5/6/7 与嵌套消息，旧四位
  号不动，prost 重生成后旧编码字节面零变化，fork 清单 vendor/README.md
  登记）。既有「§5 E 批已如实登记」先例同姿态：proto 钉定面与官方现行
  面的差值由 vendored proto 摘要如实补齐，引擎不发明字段。收投影实现
  Blocked 同步消费：官方 `_handleBlocked` modern/legacy 家族判定同构
  （`areModernFieldsUsed || !areLegacyFieldsUsed`），face 见 §3
  syncMessage.blocked 行；此前 Content::Blocked 落兜底臂整信封丢（引擎
  完全无 blocked 同步消费）。偏差登记：①blockedAt 时间戳语义（官方 diff
  拉黑集后按 timestamp 记 blockedAt）由消费方 connector 自行落库，引擎
  只投影原值；②P9 既有「收向 Request{Blocked} 不应答」面与本接收投影面
  互不相干（收投影 = 主设备推送拉黑态给链接设备）。
- **项5 AttachmentBackfillRequest 求证结论：官方桌面侧零接收消费，本引擎
  不实现（如实记档跳过）**。求证（upstream 全量仓 abe80d3）：官方桌面
  `#handleSyncMessage` 分派臂全集（MessageReceiver.preload.ts:2994-3152）
  **无 attachmentBackfillRequest 接收臂**——桌面收到该请求零消费（该臂
  在官方桌面收侧落默认丢弃）；桌面仅在**发送侧**构造该请求（
  `getAttachmentBackfillSyncMessage` SendMessage.preload.ts:1984-2016，
  由 jobs/helpers/attachmentBackfill.preload.ts `AttachmentBackfill.
  request` 触发——本地媒体下载缺失 + 能力闸 `attachmentBackfill` + 有
  其他设备时向手机要回填），响应臂 `attachmentBackfillResponse` 桌面有
  消费（AttachmentDownloadManager.handleBackfillResponse）。即该请求的
  **接收消费方是手机端（Signal-Android）**，引擎角色 = 桌面（linked
  device），无对应接收消费职责。引擎现状行为与官方桌面收侧一致：
  `Content::AttachmentBackfillRequest` 落兜底臂整信封丢（零投影）。发送
  侧亦不实现（引擎元数据投影无媒体落盘缺失场景；`engine.
  protocolCapabilities` 的 attachmentBackfill 位 = linked-device 能力
  声明面，与此请求消费面无关，勿混淆）。后续若 connector 提出媒体回填
  消费需求再立项。

### P15b 批（2026-10-11，时序自愈审计 + 状态收敛审计落地窗，wire additive）

- **项1 ack durability 收口（时序审计 G1，engine 内闭环，wire 零变化）**：
  receive 管线四个投影臂（Content/Contacts/IdentityChange/DecryptionError）
  的 ack 从「投影入队即 ack」后移到「投影已写进 stdout 并 flush（连接器
  至少收到字节）」——出站帧协议 Notice + Flush 背压栅栏（engine 内部，非
  wire）。此前 presage 已落库、连接器未收到的窗口内引擎死亡（watchdog/
  链式重启/OOM）时服务器队列已耗、连接器永不收到，且重启重放被 presage
  重放闸吞成 Duplicate——该批消息永久丢失。栅栏不可解（stdout 死）→ 不
  ack + 接收管线停环（StdioDead，不退避——重连会把重投信封喂进 Duplicate
  就地 ack 吞掉），信封留服务器队列待连接器重启引擎后全量重放补投影。
  连接器侧语义：receive 通知到达即「引擎已尽力投递」，ack 时序对 wire 面
  零感知；背压上收（连接器读得慢 ack 就慢，服务器侧流控自然生效）。
- **项2 会话状态三 flag 同步（状态收敛审计 R1，getConversationState/
  setConversationState，wire additive）**：静音/归档/标记未读三 flag 的
  多设备同步写读面——官方通道 = Storage Service record 字段（非 sync
  message）：ContactRecord.archived/markedUnread/mutedUntilTimestamp（
  SignalStorage.proto 11/12/13）与 GroupV2Record/GroupV1Record 同名位
  （GV2 4/5/6、GV1 4/5/6；storageRecordOps applyContactRecord/
  applyGroupV2Record 消费 + toContactRecord/toGroupV2Record 上行同位，
  MuteExpiration.toProto 语义 = 绝对毫秒、0 = 未静音）。复用 blocked/
  sticker 写面既有编排（read 改写 decode→mutate→encode 保真、新 16B
  item id、WriteOperation version+1、409 采纳重放 3 次、manifest 404
  播种分支）；部分写意图（缺席位不动，AccountConfigurationUpdate 同
  款）；同值幂等无写；目标记录缺席时非缺省意图 = 最小记录首建（标识 +
  三 flag，fresh_blocked_record 同构）、全缺省 = 不空造记录。
  - `getConversationState {account, conversationId, kind}` →
    `{mutedUntilTimestamp, archived, markedUnread}`（三键恒在；目标记录
    缺席/无 storage 状态/manifest 404 = 全缺省 0/false/false，诚实幂等）。
  - `setConversationState {account, conversationId, kind,
    mutedUntilTimestamp?, archived?, markedUnread?}` → 写后目标三 flag
    （getConversationState 同形状；官方 upload 成功语义 = 本地写后面为
    准）。mutedUntilTimestamp 非负整数、archived/markedUnread 布尔；
    null/形状错/负数 `-32603`（官方 falsy no-op 静默语义引擎不收，
    updateGroup title 同纪律）；三 flag 全缺 = 无写意图 `-32603`。
  - kind 词表 contact/group/legacyGroup（blocked 面同款；Note to Self 的
    archived/markedUnread 在官方走 AccountRecord.noteToSelf* 位且无静音
    位——本批不收 noteToSelf kind，消费方提出需求再立项，偏差登记）。
  - fake：get 恒缺省态；set 缺省态叠意图回显。错误面 = storage 四码
    （STORAGE_UNAVAILABLE/READ_FAILED/WRITE_FAILED/会话不可解析
    -32603，blocked 面同款）。
- **项4 send 直聊腿 per-recipient 成败收集（时序审计组二#9，wire
  additive）**：`send` 直聊形态不再 fail-fast——逐收件人失败（寻址/
  身份拒发闸/构造/上行）记入 `failures: [{recipientNumber, error:
  {code, message}}]` additive 键（空表键不在），成功条目照旧 `results`；
  混合结局返回 200 + results+failures（旧宿主把 results 当全量投递的
  风险由消费侧补——connector 只补未达方，消费不归本批）；全败维持整批
  错误面（首错分类上抛——零已达收件人整批重试无重复投递风险，旧连接器
  语义不变）；error 分类 = send_error_face 单点（与整批错误面逐字同源，
  send_error 收敛为该单点的包装）。附件/贴纸图上行失败仍整批早退（批级
  共享指针，不属 per-recipient 面）。群腿（send_text_group 既有逐成员
  results）/editMessage/群发送系不变。
- **项3 dedup 门 thread scoping（时序审计组二#4，vendored presage，fork
  登记 vendor/README.md）**：接收重放闸 `(sender, client_timestamp)` 从
  跨 thread 全局判重改为同 thread 才抑制（`is_thread_scoped_redelivery`
  + `Thread::try_from(&content)` 入向线程推导比对；入向线程推不出保守沿
  用旧全局抑制；批内 batch_seen 键同步加 thread 维度）——同 sender 同毫
  秒发两条到两个群，第二条不再被吞。store trait 与 store-sqlite SQL 零
  改动（查询本就返回 Thread，比对上收调用方）。
- **项7 QueueEmpty 排空上抛（时序审计 G3，wire additive）**：接收流历史
  队列排空标记此前为 no-op，现上抛 per-account 通知
  `{method: "receive.emptied", params: {account}}`（官方 ApiEmptyQueue→
  EmptyEvent 同位：MessageReceiver 收空队列即 dispatch empty 事件）。独立
  方法名非 envelope 载体——排空是流态事件，无信封可挂（accountUnbound
  同款加面纪律）。引擎只报态；连接器/desktop 消费自定（如启动期「积压
  回放完成」推进度旗标）。每轮排空都投（服务器每次报空即一次），消费方
  幂等自便。
- **项5 单群读投影（审计项5，wire additive）**：审计原文「updateGroup
  只收 expiration/title，群成员管理缺口」——求证结论：updateGroup 增参
  面已由既有批全数落地（expiration 2026-10-06 批、avatar 2026-10-09
  合成单 change 批、title 群写面 P2 批、addMembers P3 批、removeMembers
  P4 批、addFromInviteLinkAccess/inviteLinkPassword P13 批，均 §3
  updateGroup 行逐批登记），无剩余缺口；「groups.get 读投影」的真实缺
  口在 connector 契约 §4.53/§4.57/§4.62 三处 fold 读——connector 已派发
  `listGroups {account, groupId: [id]}`（数组形）并取 `first()` 当目标
  群，引擎此前忽略 `groupId` 返回全表，多群账号下 `first()` 读到任意群
  （审批/撤邀 fold 的 requesting/pending 集合错群危害）。落法：listGroups
  增可选 `groupId` 过滤（非空字符串数组、条目 32B 标准 base64 群
  master key decode_group_id 闸，坏形状 fail-closed `-32603`；well-formed
  但 store 不在 → 掉出结果不报错——读面只投影 store 实有，connector
  fold 侧 `first()` → 空集诚实降级为 JOIN_REQUEST_NO_LONGER_PENDING）；
  缺席/null = 全表（旧行为，wire additive）。新增 `groups.get` 独立方法
  不做——connector 的 groups.get 是其自有缓存读面（§4.8「never calls
  the upstream」），引擎侧无消费方，按 §4.50「不发明无消费方的读面」纪
  律收口。过滤判定抽纯函数 `group_key_passes_filter` 单测钉形；fake 面
  参数闸同口径（契约测试钉定）。
- **项8 时钟偏移闸 + keepalive stale（审计项8，wire additive）**：两个
  子项求证与落法——
  - **keepalive stale 检测（websocket 假活）**：vendored websocket 已实
    现（libsignal-service src/websocket/mod.rs `run()`：30s
    keepalive 节律（`KEEPALIVE_TIMEOUT_SECONDS`，官方 Android
    HealthMonitor 30s 同值）发出后未在下一拍前收到应答（`outgoing_keep_
    alive_set` 非空）→ `Websocket will be closed due to failed keepalives`
    → close + 管道结束 → 引擎重连退避循环接管）——官方 Signal-Android
    SignalWebSocketHealthMonitor onKeepAliveResponse/keepalive sender 状
    态机同位。零改动，如实记档。
  - **时钟偏移闸**：官方 Signal-Android `ClockSkewDetector` 行为对齐（
    不抄码）——锚位逐行核实修正（2026-10-12 时序修复批）：官方 skew 判
    定锚在 **WebSocket 层服务器时间**，Signal-Desktop（pinned abe80d3）
    ts/textsecure/SocketManager.preload.ts #handleServerTimestamp（:928）
    由 connectAuthenticated 的 onServerTimestamp 回调喂（WS 响应级服务
    器时间，恒新鲜，与积压信封无关）、#getSkew（:914）+ 阈值 remote
    config `client.maxAllowedClockSkewSeconds`（兜底 24h、下限 1h）；
    Signal-Android（pinned 92f5ccd）ClockSkewDetector.onServerTimeReceived
    同位（ChatConnectionListener.onServerTimestamp 经
    SignalWebSocketHealthMonitor + remote config fetch）。**官方从不拿
    信封内层 metadata.server_timestamp 喂判定**——初版落法（逐信封喂闸）
    与官方锚位不同，刚链接设备的积压信封携入队时刻时间戳必出「本机钟快
    数天」假告警，本批修正。skew 式 = `|本地 − 服务器|`；阈值 = 官方
    remote config `client.maxAllowedClockSkewSeconds` 默认 24h（engine 无
    remote config 面，取同默认编译期常量）。sidecar 等价设计（**积压免
    疫**）：presage receive_stream 暴露不了 WS 层服务器时间，改用武装闸
    ——每条新流入口先解除武装（observe no-op），排空标记
    `Received::QueueEmpty` 到达后武装（服务器侧
    RedisDynamoDbMessagePublisher「initial queue drain complete signal」
    ——初始态即假定两存储有消息，零存量建连也恒产出、每流恰一次，
    vendored presage 无条件透传不丢弃，武装必达）；武装后信封为实时相、
    喂闸锚恒新鲜，等价官方 WS 层锚；`alerted` 跨重连存活（边缘触发纪律
    不变）、`armed` 随每条新流重置。落法：`receive.clockskew` per-account
    additive 通知 `{account, skewMs（带符号 = 本地−服务器，正 = 本地快）,
    serverTimestamp}`，**边缘触发**（首越阈告警一次 → 持续越阈静默 → 回
    阈内重臂 → 再越阈重新告警；官方 detected 态 in-memory 自愈清除同语
    义；per-信封重复告警无增量信息故不逐条投）。**拒绝面不做**（偏差登
    记）：本地钟错不该丢真消息，官方超阈动作是「停保活 + 阻断屏」GUI
    语义（有用户可修钟）；sidecar 无用户可介入，停车/拒收即自 DoS——
    保守策略 = 只报态，连接器/desktop 消费自定（日志/告警/运维通知）。
    观测面在 Received::Content 与 IdentityChange 两臂（二者同携
    server_timestamp；DecryptionError 携 sender client 时钟非服务器权威，
    不入观测），武装前置闸统一在 observe 入口（disarmed 一律 no-op）。
- **项6 群审批 deny 原语（审计项6，wire 语义扩展无新键）**：审计「审批
  deny 原语 + ban 写面」求证：ban 写面 = GroupV2Record.blocked 位，由
  `setConversationBlocked`（2026-10-06 批）已覆盖，无缺口；deny 原语真
  实缺口在 updateGroup removeMembers 目标分类——此前两向判别（members
  面 → delete_members、pending 面 → delete_members_pending_profile_key），
  审批申请者（requesting）目标落整拒臂 `-32603`，而 connector §4.53
  groups.approveJoinRequests{approve:false} 的 refuse 流按契约把 fold 后
  的 requesting 存活者装入 `removeMembers` 数组——deny 端到端断链（fold
  越严引擎越必拒）。落法：官方三向判别同构（signal-cli
  GroupHelper.updateGroupV2 三向分流 GroupHelper.java:759-770 / 官方桌面
  denyPendingMembershipFromGroupV2 → buildDeletePendingAdminApprovalMember
  Change）——第三臂 = `delete_members_pending_admin_approval`
  （deleted_user_id = ACI 加密，与 cancelJoinRequest own-ACI 臂同 action
  同形，目标泛化为任意申请者）；`build_remove_members_change` 增第三参
  数，单测钉加密回读对账。独立 `denyJoinRequest` 方法不做——connector
  refuse 流按 §4.53 走 updateGroup 共享数组，无第二消费方，按 §4.50「
  不发明无消费方写面」纪律收口。

### P15c 批（2026-10-11，链组审计落地窗 B，wire additive / 记档）

- **项1 多设备编辑 echo 正文求证（链组一，审计过时记档，wire 零变化）**：
  审计原文「本机从手机端编辑消息后，编辑 echo 同步回桌面时正文丢失」——
  求证结论：**审计过时**。引擎 Sent echo 臂的 editMessage 投影已由六问批
  Q1（commit c20a310，2026-10-11 13:18，先于链组审计落地窗）全量落地：
  `syncMessage.sentMessage.editMessage{targetSentTimestamp,
  dataMessage{message(正文)/timestamp/groupInfo?/quote?/...}}`——正文经
  project_data_message 单点投影（`message` 键），编辑目标/时间戳双键齐备
  （sentMessage.editMessage.dataMessage.timestamp），单测
  `sent_sync_projects_edit_and_story_echoes` 钉形。对照官方编辑 echo 消费
  面（MessageReceiver.preload.ts #handleSentEditMessage → SentEvent →
  background.preload.ts:2886 Edits.onEdit → handleEditMessage.preload.ts）
  逐键核对：正文=editMessage.dataMessage.body ✓、编辑目标=
  targetSentTimestamp ✓、时间戳=dataMessage.timestamp ✓、quote/preview/
  bodyRanges ✓ 均随 project_data_message 投影；官方 handleEditMessage 另需
  serverTimestamp（48h 编辑窗 isTooOldToEditMessage 判据）——该键由本批
  项5 信封 serverTimestamp 投影补齐，编辑消费面由此完整。零代码改动。
- **项2 Storage Service 另四族核实（链组三中批 D2，三实一记档，wire 零
  变化）**：审计原文「NotificationProfile/StoryDistributionList/
  AccountRecord 配置/UniversalTimer 四族宿主零消费」——逐族求证结论：
  - **NotificationProfile / StoryDistributionList：引擎已解析+投影（审计
    过时，缺口在消费侧）**。引擎面 2026-10-08 已全量落地（commit 6bd9947
    六方法批 / 35a8de6 Stories 名单臂）：读投影 `notificationProfiles.list`
    /`storyDistributionLists.list`（manifest type=9/=5 identifiers 首现序
    → vendored proto 解码 → JSON 投影，键名对齐官方
    mergeNotificationProfileRecord/toStoryDistributionListRecord 输出面）
    + 写面 `notificationProfiles.setRecords`/`storyDistributionLists.
    setRecords`（upsert/墓碑/removeIds/幂等/409 采纳重放全编排）。官方桌
    面消费面实锤（storage.preload.ts validRecordTypes + merge switch 双臂
    在册：mergeNotificationProfileRecord/mergeStoryDistributionListRecord）。
    宿主消费缺口移交主线：connector/desktop 未接引擎这两对方法（消费侧
    接线归后续批）。
  - **AccountRecord 配置：写投影已有（审计过时），JSON 读投影无——不补，
    移交主线按需立项**。写面 2026-10-05 起逐批落地：setAccountConfiguration
    （readReceipts(6)/typingIndicators(8)/phoneNumberSharingMode(12)/
    unlistedPhoneNumber(13)，commit 76d1efc）+ setUniversalExpireTimer
    （universalExpireTimer(17)，d82e0ae）+ setProfile/username/置顶 同
    record 写族；记录体解析 read_account_record 内部面已在（官方 vendored
    proto 全字段保真）。官方桌面消费实锤（mergeAccountRecord 应用配置字
    段）。JSON-RPC 读面（getAccountConfiguration 形状）无消费方——按
    §4.50「不发明无消费方读面」纪律不补，connector/desktop 接线提出需求
    再立项（偏差登记）。〔2026-10-12 窗D B6 批1 解除：读面
    `getConfiguration` 随 B6 写穿补链落地（消费方 = connector/desktop
    后续批），见 §5 窗D B6 块。〕
  - **UniversalTimer：官方桌面也不消费（族不存在于现行协议面），记档
    跳过**。现行官方 SignalStorage.proto ManifestRecord.Identifier.Type
    词表（UNKNOWN/CONTACT/GROUPV1/GROUPV2/ACCOUNT/STORY_DISTRIBUTION_LIST/
    STICKER_PACK/CALL_LINK/CHAT_FOLDER/NOTIFICATION_PROFILE）无
    UNIVERSAL_TIMER 位；官方 storage.preload.ts validRecordTypes 与 merge
    switch 均无此臂；vendored 钉定 proto 同无。引擎已有的 universalExpire
    Timer 面 = AccountRecord.universalExpireTimer(17) 字段写面（非独立
    record 族），桌面 `pendingUniversalTimer` 字样属消息级计时器通知
    （cleanup 面），与 Storage record 族无涉。零代码改动。
- **项3 DeleteForMe 发送面（链组一 N1 发腿，deleteForMeConversation，
  wire additive）**：会话级「为我删除」此前只有收包投影（P15a 项1，
  conversationDeletes/localOnlyConversationDeletes 两臂投影给 connector
  删自有库行），上行发送面缺口——本机桌面删会话时向手机同步「把这些消息
  也删掉」。落法：新增 JSON-RPC `deleteForMeConversation`（§3 行），
  官方 conversations.preload.ts:5640-5663 deleteConversation 发送面 +
  SendMessage.preload.ts:1840 getDeleteForMeSyncMessage 构帧同位——
  - 全量清空臂（官方 delete-conversation）：`localOnly` 缺省/false +
    `mostRecentMessages`（必填非空 ≤5 条目——官方
    getMostRecentAddressableMessages().slice(0, 5) 上限镜像，超出
    fail-closed 不静默截断破坏性意图）+ 可选 `mostRecentNonExpiringMessages`
    （官方 areAnyDisappearing 伴随面 ≤5）→ SyncMessage.deleteForMe.
    conversationDeletes[0]{conversation, isFullDelete: true, ...}（手机端
    按 mostRecent 定位消息行执行服务器删除 + 全设备清空语义）；
  - 仅本地臂（官方 delete-local-conversation）：`localOnly: true`，
    mostRecent* 必须缺席 → localOnlyConversationDeletes[0]{conversation}
    （其他设备仅清本地行，无服务器删除；官方 mostRecentMessages 空批
    else 分支同语义）；
  - 会话标识三形态 binary-first（threadServiceIdBinary/threadE164/
    threadGroupId = 32B master key bytes，官方 toConversationIdentifier
    isProtoBinaryEncodingEnabled 现行面同位；legacyGroup 拒——官方
    getConversationIdentifier 非 GV2 群 throw 同构）；条目 author 直通
    （service-id/E164，官方 toAddressableMessage 形态，引擎不做号码↔
    service id 二次变换——作者身份须与手机端消息行逐字匹配，store 侧
    猜测映射会破坏 (author, sentTimestamp) 定位）。
  向自己 ACI 发 padded sync message（self-send 通路 E4 已验证同款）；
  协议上行面落调度日志。desktop 上行接线归后续批（connector/desktop
  消费缺口移交主线：桌面会话删除流现仅清自有库，多设备同步缺上行腿）。
- **项4 syncRequests 上行三请求（链组一 N1，sendSyncRequests 同位，wire
  additive 无新事件）**：求证结论：引擎链接完成流程（finish_link →
  spawn_pipeline → serve_account_stream）已有链后 sync 上行步，但仅
  contacts 一腿（presage `request_contacts`，60s 一次性 + 2s 沉降纪律，
  欠账 §10-1 批遗留）——官方 sendSyncRequests（syncRequests.preload.ts:
  11-34，afterEveryLinkedStartupOnNewVersion/auth socket 建流触发）是
  contacts/configuration/block 三请求并发。落法：既有 sync_pending 臂扩为
  `send_initial_sync_requests` 三腿（contacts 腿保持 presage 官方 API，
  configuration/block 腿 = SyncMessage.content.Request{CONFIGURATION=4/
  BLOCKED=3} 构帧上行——官方 getRequestConfigurationSyncMessage/
  getRequestBlockSyncMessage 同位，padded sync message）；既有 60s 一次性
  纪律保留（重连风暴不重复轰主设备，官方每次建流都发、引擎按欠账批既有
  取舍收敛——偏差登记：短重连窗口内 configuration/block 可能比官方晚一拍
  补发）；每腿独立 best-effort（官方 Promise.all 一损俱损，引擎零 stderr
  纪律下逐腿尽力——sync 请求幂等，失败靠下次链路建立重补）。上行 sync
  message 非投影，无新事件无新方法；主设备回传经接收流既有各面落库
  （contacts → Received::Contacts 投影、configuration/block → 收包臂既有
  投影）。单测 sync_request_frame_carries_type 钉枚举直通。
- **项5 信封 serverTimestamp 投影（收包面缺键补齐，wire additive）**：
  审计原文「官方 handleEditMessage 依赖 envelope serverTimestamp 判 48h
  编辑窗，引擎 receive 面缺该键」——求证结论：缺口属实。引擎信封公共位
  此前有 sourceUuid/sourceDevice/timestamp（client 时钟）/serverGuid/
  urgent/ephemeral/reportingToken，唯 serverTimestamp 缺席——P15b 项8
  时钟偏移闸已把该值用作服务器时间权威（Received::Content 与
  IdentityChange 两臂同携），却从未投影给 connector；官方桌面每个事件面
  均带 serverTimestamp（MessageReceiver.preload.ts:1178 收包解构同位），
  消费面三处：48h 编辑窗判据（isTooOldToEditMessage，P15c 项1 记档的
  编辑 echo 补齐键）、消息行服务器时间（多设备消息排序/展示）、服务器
  停留时长（getMessageAge）。落法：`project_content` 信封公共位加
  `serverTimestamp`（毫秒 u64，恒投）——值源 = vendored
  Metadata.server_timestamp（cipher 解密自 Envelope.server_timestamp
  proto 位，与官方 `ServerTimestampMs.fromBigInt(decoded.serverTimestamp
  ?? 0n)` 同源同权威）；wire 缺席 = proto 缺省 0，官方同口径照投 0 不省
  键不发明；dataMessage/editMessage/typingMessage/receiptMessage/sent
  echo 同走本信封单键齐备。边界：groupTerminated/groupUpdated/
  groupChanged/contactProfileUpdated/identityChanged 等最小事件信封不随
  批（serverGuid 先例同口径——信封公共位只落主投影面）；decryptionError
  不投（解密失败臂无服务器时间可取，P15b 项8 已登记该臂 client 时钟非
  权威）。单测 receive_envelope_projects_server_timestamp 钉 dataMessage
  与 editMessage 两体面恒投 + client/server 两时间戳不串。

### 组四批（2026-10-11，时序审计组四 E1-E4 + 组五记档，官方对齐 Signal-Desktop abe80d3，wire additive / 记档）

- **项1 DERM 重试回执明文腿（E3，wire additive）**：审计原文「回执腿走
  会话加密与官方不符」。求证结论：官方 requestResend
  （handleRetry.preload.ts:659-689）= `DecryptionErrorMessage.forOriginal`
  → `PlaintextContent.from` → ResendRequest job → `sendMessageProtoAndWait
  {proto: plaintext}`（sendResendRequest.preload.ts:114-124），OutgoingMessage
  对 PlaintextContent 直发 Envelope **type=8 PLAINTEXT_CONTENT**、不经
  getCiphertextMessage 会话加密（libsignal v0.99 `message_encrypt` 亦无
  明文分支，须 service 层自构 OutgoingPushMessage）；引擎此前走
  `send_message(ContentBody::DecryptionErrorMessage)`（type 1/3 会话加密）
  确与官方不符。落法：vendored libsignal-service `sender.rs` 新增
  `send_plaintext_content`（设备枚举同 `create_encrypted_messages`：sub
  devices + 主设备 − 自身；per-device 无会话跳过、有会话取
  destination_registration_id，content = base64(plaintext.serialized())，
  type=8 identified ws 发送）；vendored presage `registered.rs` 加同名包装
  （不注 profile key/不 restore timer/不落库）；引擎 `derm_send_receipt`
  改 `DecryptionErrorMessage::try_from(receipt)`（timestamp 恒有，缺失即
  坏回执静默弃）→ `PlaintextContent::from` → 明文腿。wire 变化（additive
  only）：DERM 重试回执信封 type 1/3 → **8**，体 = `0xC0` 前缀 +
  Content proto（内嵌原回执字节）+ `0x80` 边界——type 8 接收面官方与
  引擎 cipher 既有支持，旧版引擎收 type 8 不受影响。偏差登记：无会话
  设备跳过（无会话即解不开原消息，重发请求无意义）。单测
  derm_receipt_roundtrip_keeps_timestamp_and_device（坏回执拒收）、
  derm_plaintext_receipt_wire_shape（0xC0/0x80 钉形 + 对端官方 extract
  可逆找回）。
- **项2 signed/last-resort prekey 轮换龄 1.5 天（E1）**：审计原文「轮换
  龄与官方不符」。求证结论：官方 `SIGNED_PRE_KEY_ROTATION_AGE = DAY*1.5`
  （AccountManager.preload.ts:142）且 **last-resort kyber 同 1.5 天按龄
  轮换**（:115 + #maybeUpdateLastResortKyberKey :868-920），轮换 PUT 同携
  signed + last-resort（#doUpdateKeys）；引擎此前 signed 14 天档且
  last-resort 从不轮换。落法：engine `src/service/prekey.rs` 轮换龄
  14d→1.5d
  （SIGNED_PRE_KEY_MAX_AGE_MS），新增 last-resort 1.5d 判（缺键=归水位
  补装语义不越权，`last_resort_key_over_age`）；按龄轮换挂 30min
  quick_check 快拍（1.5d 龄细于 2 天 maintenance 节律，闸纯本地读零成本）
  + maintenance_tick 保留幂等 + 发送前超龄闸同步收紧；vendored
  `pre_keys.rs::replenish_pre_keys` 增 `force_new_last_resort`、
  `account_manager.rs::rotate_signed_pre_key_bundle` 改 force 臂（轮换
  PUT 同携新 signed + 新 last-resort，官方同 PUT 语义）、
  `update_pre_key_bundle` 行为不变；顺修 `publish_fresh_pre_keys`
  last-resort 上传取 `last_resort_keys[0]`（rowid 最旧行）会重传旧键的
  语义 bug → 新生成优先、否则 stored newest-by-timestamp。偏差登记：
  官方判据 = 最近 confirmed 键 lastUpdate，我方无 confirmed 位（store
  先于上传，视 stored = confirmed，记录内嵌时间戳即生成时刻）。单测
  signed_pre_key_age_gate_matches_official_15d /
  last_resort_age_gate_matches_official_15d / rotation_due_or_gate。
- **项3 prekey 服务器上限闸 + 本地保留窗（E2）**：审计原文「缺 >200
  整批重发与本地清理」。求证结论：官方 `PRE_KEY_MAX_COUNT = 200`
  （AccountManager.preload.ts:128）——服务器计数 <10 **或** >200 都整批
  重发（#doUpdateKeys）；本地清理四族 = 一次性 newest 200 + 窗外且超
  `PRE_KEY_ARCHIVE_AGE`=90d 才删（_cleanPreKeys/_cleanKyberPreKeys
  :1010-1064）、signed/last-resort newest 5 + 窗外且超
  getMessageQueueTime()（缺省 30d）删（:897-960/:962-1007）；引擎此前
  闸只判 `>= PRE_KEY_MINIMUM`、无本地清理。落法：vendored
  `update_pre_key_bundle` 水位闸补 `<= PRE_KEY_MAX_COUNT` 上限臂（两族
  同闸，超限走整批重发既有路径）；vendored store-sqlite 新 migration
  `20261011000000_add_pre_key_created_at.sql`（pre_keys 加 created_at，
  DEFAULT 0 = 存量行视最旧，官方 `createdAt || 0` 同语义无回填）+
  `save_pre_key` 写入（宏改运行时 query，`.sqlx/` 缓存不动）+
  `SqliteProtocolStore` 六个 `kt_` 方法（one-time/signed/kyber 清单 + 三
  族批量删除，kyber 可删 last-resort 行由调用方裁定）；engine
  `prekey.rs::cleanup_tick` 四族保留窗扫删（200/90d、5/{messageQueueTime
  窗}、5/90d 常规 kyber、5/{messageQueueTime 窗} last-resort），挂
  maintenance_tick（轮换后跑），`sweep_candidates` 纯函数判删（窗外且够
  老双条件，窗内/窗外年轻恒活——在役 signed/last-resort 按构造必在窗
  内）。原偏差「30d 窗取官方缺省（无 remoteConfig 读面）」已于 2026-10-12
  stretch 批收编（见下「窗C stretch 收编批」）。单测
  sweep_candidates_keep_newest_and_age_gate +
  signed_family_delete_age_ms_matches_official_max_floor（engine）+
  kt_pre_key_inventory_and_bulk_removal（store 级，时间戳/旗标/批量删除
  钉形）。
- **项4 DERM 档二兜底细化（E4）**：审计原文「失败路径一律 clear_sessions
  全设备删除，过重且超限也清，与官方不符」。求证结论：官方 onRetryRequest
  闸序（handleRetry.preload.ts:129-261）= 防环超限（RETRY_LIMIT=5）**早
  退无任何会话动作** → archiveSessionOnMatch（:338-368：DERM 内嵌
  device_id == 我设备 且携 ratchetKey → 比对 (requester, 其发送设备) 会话
  currentRatchetKeyMatches → 命中归档**该单一地址**）→ 超龄/找不到
  sentProto → sendDistributionMessageOrNullMessage（:370-440：群+有
  sender key 台账 → 重发 SKDM；否则归档过才发 NullMessage；否则不做）→
  找回则重发原 proto（原 timestamp）。落法：presage
  `DecryptionFailure::RetryRequest` 扩四元 `{original_ts, ratchet_key,
  derm_device_id, requester_device}`（官方 RetryRequestEvent 四元
  MessageReceiver.preload.ts:2690-2698；DERM 无 timestamp 改 ack 即弃）；
  引擎档二重构为官方闸序——超限早退（不再 clear_sessions）、新增
  archive-on-match 腿（`SessionRecord::current_ratchet_key_matches` +
  `archive_current_state` 单地址归档回存，任何前置不满足不动会话）、
  超龄/找不到/不允许 → 兜底面。偏差登记：官方兜底群臂重发 SKDM，我方
  SKDM 重发臂未建（sender-key 分发只随群发送自然发生）→ 群兜底与直聊
  同判「归档过发 NullMessage（新会话敲对端促重建），否则不做」，记档
  待 SKDM 独立重发面立项。单测 derm_archive_on_match_gates_refuse_
  without_match（五负路径 + 既有会话原封钉形；正路径完全委托上游
  current_ratchet_key_matches/archive_current_state 原语）。
- **项5 组通话 join/SFU 断层求证（组五，记档无代码）**：审计原文「群
  通话 join/SFU 断层」。求证结论：**断层不在 engine**。官方链 joinGroupCall
  （services/calling.preload.ts:1594-1650）→ connectGroupCall（:1429-1519，
  RingRTC.getGroupCall + observer requestMembershipProof→fetchMembershipProof
  →setMembershipProof）→ join()，其中 **sfuUrl = 桌面 app 静态配置**
  （config/production.json `https://sfu.voip.signal.org/`），非协议面——
  engine 不欠 sfuUrl 读面（§4.50 不发明无消费方读面纪律）。引擎三层
  证据全齐：sendCallMessage 群臂 + sendGroupCallUpdate 六问批 + calls.*
  五材料面（compat §3）；connector 1.82 群信令双向已落
  （kt-signal-connector implementation-plan.md §4.49.2）；calling sidecar
  P4a-P4d 全落（kt-signal-calling relay-api-v2.md 台账）。断点在桌面壳
  接线：relay-api-v2.md §8.3/§8.5 记档的前置①（壳无 groupControl
  caller）、②（壳无 `call.config` sfuUrl 下发——白名单无配置源全部 SFU
  HTTP fail-closed `sfu-url-missing`，connect 永停 Connecting）、③（材料
  链壳↔引擎应答面）——join 即因此不可用。移交：KTAIorg 桌面/壳仓按
  calling 仓台账补接线；引擎侧无代码变化。

### 窗D A7 Polls 发送三臂批（2026-10-11，官方对齐 Signal-Desktop，wire additive）

缺口总登记 A7（三层全缺，仅收投影 Q3 三臂在）的 engine 腿：sendPollCreate/
sendPollVote/sendPollTerminate 三新方法（§3 三行）。官方锚：enqueuePollCreate
ForSend.dom.ts（create）、enqueuePollVoteForSend.preload.ts → SendMessage.
preload.ts createDataMessageProtoForPollVote（vote）、enqueuePollTerminate
ForSend.preload.ts → createDataMessageProtoForPollTerminate（terminate）。
proto 支臂（pollCreate=24/pollTerminate=25/pollVote=26）vendored
SignalService.proto 既有，零 proto 改动。

- **群聊 only（1:1 形态缺席，偏差记档）**：官方桌面 poll create 仅群聊——
  1:1 走 `isPollSend1to1Enabled`（desktop.pollSend1to1.beta/prod feature
  flag）默认关；vote/terminate 只在群会话触发（poll 只存在于群）。引擎按
  默认面群聊 only：三方法 groupId 必填 + decode_group_id 闸，无直聊腿。
  官方 flag 打开后 1:1 形态仍在（偏差持续有效，待 flag 生产化再议扩面）。
- **voteCount 调用方计算（偏差记档）**：官方 enqueuePollVoteForSend 在
  桌面侧按消息内自身历史票 max+1（首票 1）计算 voteCount 后随发送任务
  下传；引擎作为协议 sidecar 不持投票历史（消息与票态落库归 connector），
  voteCount 由调用方计算传入，缺省 1（= 首票语义）。调用方不传且非首票
  时，对端按 voteCount 序取最新票可能把旧票当新——桌面接线须按官方语义
  计算传入。
- **长度计数 chars 非 graphemes（偏差记档）**：官方 PollCreateSchema 用
  hasAtMostGraphemes（用户感知字符）；引擎无分割依赖，用 chars().count()——
  合成序列（组合 emoji 等）chars 计数 ≥ graphemes，超限拒绝更保守
  （fail-closed 方向），不产生官方可发我方拒收之外的越限放行。字节 ≤2048
  闸（官方 MAX_MESSAGE_BODY_BYTE_LENGTH=2KiB）同域。
- **optionIndexes 域闸**：每项 0..=9、≤10 条、空数组 = 清票动作合法
  （官方 PollVoteSchema 同域）；服务层去重保首现序（发送幂等护栏，官方
  接收面 new Set 同构——发送面调用方已按本地票态去重）。
- **GV1 天然拒发（零偏差）**：官方 terminate 拒 GroupV1（isGroupV1 refus）
  ——引擎群腿形状闸（decode_group_id 只收 GV2 32B master key）使 GV1 形态
  在参数层不可达，语义等价。
- **群发送闸族沿系**：open_group_sender（terminated 硬拒）+ dispatch_group
  （announcements-only 非 admin 整拒 + MRM 主路径 + group_v2 上下文注入）
  与 sendAdminDelete/sendGroupCallUpdate 同链；终止者资格（creator/admin）
  是官方 UI 层闸，wire 面不设（同 editMessage 24h 窗先例）。
- **quoteType="poll" 引用词表不随本批扩**：官方 Quote.Type 有 POLL=2（回复
  poll 消息的引用形态），但引擎 send quoteType 词表（normal|giftBadge）维持
  既有登记不变——poll 引用面官方走 sendNormalMessage 常规分支，本批不扩
  引用词表（如需按官方 Quote.type=3 前缀协议补，另批）。
- **fake**：三方法 fake 成功 `{}` 落调度日志（send 系纪律）；参数层闸
  fake 同样生效（-32603 可契约测试）。
- **单测**：poll_send_proto_faces（三臂 proto 构造 + optionIndexes 去重保
  序钉形）+ contract poll_send_param_faces_and_dispatch_log（26 形状闸逐
  形 + fake 成功三臂 + 调度日志逐键）。

### 窗D A4 Storage ContactRecord merge/发现面批（2026-10-11，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 A4 的 engine 腿：新方法 `storage.mergeContacts`（§3 行）。
官方锚：`services/storageRecordOps.preload.ts` mergeContactRecord +
`SignalProtocolStore.preload.ts` updateIdentityAfterSync。此前 engine 的
Storage 同步面全部按需读写（置顶/拉黑/三 flag/配置/贴纸包等），没有把
ContactRecord 的身份材料 merge 进引擎本地库的通道——新链接设备/换机后
presage 身份库为空，对端身份钥只能靠首次发送时服务器 prekey 面旁路回填，
安全号计算、P9 信任闸、头像发现（profileKey）全部无从起步。

- **官方 merge 语义的 engine 协议面子集**（其余字段无引擎消费模型，记档
  跳过，见下）：
  1. identityKey 非空 → `save_identity`（官方 updateIdentityAfterSync 同
     构：absent→新增、同钥→no-op、异钥→替换；替换腿 archive sessions 由
     vendored save_identity 承担——窗口 C 批已修的同钥不误归档语义保持）；
     ACI/PNI 双库分流（官方 serviceId 路由同构）。identityState 随行落
     P9 sidecar 信任态（apply_synced_trust，verified 同步收口同通道语义；
     Default/坏值归 default——官方 identityState null→DEFAULT 同域）。
  2. profileKey 非空且本地缺失 → `upsert_profile_key`（官方
     isFirstSync || !localProfileKey 同闸，已有不覆盖）；≠32B 坏长度诚实
     跳过。
  3. 联系人缺失 → `save_contact` 建档（e164 解析 + given+family 复合名 +
     profileKey 随建）；**已有档不动**——手机 contacts sync 是姓名权威
     （P15a 项3 落库面），engine 不发明 system/nickname 建模。
  4. service id 解析：aciBinary 16B → aci 串（只收 ACI 形态）→ pniBinary
     → pni 串（untagged 裸 UUID 即 PNI，官方 fromPniUuidBytesOrUntagged-
     String 字段声明种类同构；"PNI:" 标签形宽容）。无 serviceId/本机
     uuid → 官方 shouldDrop 同位跳过（skipped 汇总，零 stderr）。
- **记档跳过（无引擎消费模型，官方有消费）**：unregisteredAtTimestamp
  （注册态引擎无建模）、hideStory/hidden（故事/隐藏面引擎无建模）、
  avatarColor/note/nickname/system*Name 族（引擎 contacts 模型无位）、
  username（引擎不持远端 username 缓存）、archived/markedUnread/muted/
  blocked（已有专用读写面 A2/拉黑批，不重复写）。这些位的 wire 消费归
  connector/desktop 侧直接读 storage.read 面（后续 C3/D4b 批）。**F 链路
  批起**：三 flag 随本面 merge 读入行入站观测（`receive.conversationState`
  通知，见 §5 F 链路块）——观测不写，本行「不重复写」语义不变。
- **幂等与安全面**：merge 幂等（同钥/已有档重跑全 no-op）；只读 Storage
  面 + 本地库写，零 Storage 写操作（不做 storage write-back——官方桌面
  merge 后也不回写 ContactRecord，写由主设备权威）；触发时机归调用方
  （connector 链接完成/manifest 变更时调用，引擎不自动后台跑——按需面
  纪律）。
- **错误面**：account 缺席/未注册 `-32603`；storage 链路 STORAGE_UNAVAILABLE/
  STORAGE_READ_FAILED（-1 + message，置顶面同码位）；无 master key 或
  manifest 404 → 七字段零汇总幂等成功（blocked 只读面同口径）。
- **fake**：恒七字段零汇总（读面纪律）。
- **单测**：service_id_resolution_precedence（binary 优先/aci 串 ACI-only/
  pni untagged 裸 UUID/标签形/全缺/invalid-aci 逐形）+ trust_state_mapping
  （三态直映 + 坏值归 default）+ contract storage_merge_contacts_fake_
  shapes_and_param_fail_closed（fake 零汇总 + 参数闸 + real 未注册折叠）。

### 窗D A5 Storage GroupV2Record 群导入批（2026-10-11，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 A5 的 engine 腿：新方法 `storage.importGroups`（§3 行）。
官方锚：`services/storageRecordOps.preload.ts` mergeGroupV2Record →
`getOrCreateGroupSnapshot` 按需拉群态。A4 之前 engine 无群发现通道——
新链接设备后 presage store 群库为空，listGroups 空、群发送（sender-key
群发/群 typing/poll 三臂）全部无从起步，本批把 manifest GROUPV2 master
key 变成可用的本地群库。

- **导入语义**：manifest 全部 GROUPV2 identifier 批读（A4 同款批读纪律）
  → GroupV2Record.masterKey 逐群：
  1. 本地 `store.group()` 命中 → already_known，零网络跳过（增量导入
     纪律——重复调用幂等，不重复拉群态）。
  2. 未知 master key → 既有 `fetch_group_state`（groups 服务器
     `GET v1/groups/<blob>` 面，join 群/头像面同通道）→ `save_group`
     落 presage store——listGroups/open_group_sender 发现与发送面即刻
     可用。
  3. master key 坏形（≠32B，官方 32B 定长同域）/群态拉取失败（服务器
     对该群无凭据/已退出等）/落库失败 → failed 计数，**逐群 best-effort
     不炸批**（官方桌面 merge 后按需拉态同样是单群失败不阻断全批）。
- **偏差记档（vs 官方桌面）**：官方桌面 mergeGroupV2Record 只登记
  master key，群态经 getOrCreateGroupSnapshot **按需惰性拉取**；引擎作
  sidecar 无群快照惰性层（无按需触发点），导入面直接拉取落库——网络
  行为差异（导入时拉 vs 首用时拉），最终库状态同形。导入触发时机归调
  用方（connector 链接完成/manifest 变更时调用，引擎不自动后台跑）。
- **GV1 记档**：manifest GROUPV1 identifier 不在本批——GV1 记录迁移
  GV2 面（官方 conversations.migration 语义）归窗D B8 批。
- **错误面**：account 缺席/未注册 `-32603`；storage 链路
  STORAGE_UNAVAILABLE/STORAGE_READ_FAILED（-1 + message，置顶面同码
  位）；无 master key 或 manifest 404 → 四字段零汇总幂等成功（A4 同
  口径）。
- **fake**：恒四字段零汇总（读面纪律）。
- **计数归真机回归**：manifest 拉取/群态导入/alreadyKnown/failed 计数
  需真机 storage 状态（fake 无 manifest 面），契约测试覆盖 fake 形状 +
  参数闸 + real 未注册折叠。

### 窗D A6 Storage CallLinkRecord 读写面批（2026-10-12，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 A6 的 engine 腿：新方法 `storage.getCallLinks`（读投影）+
`storage.setCallLink`（写面）（§3 两行）。官方锚：
`services/storageRecordOps.preload.ts` toCallLinkRecord（:847）/
mergeCallLinkRecord（:2626）+ `services/storage.preload.ts` upload
callLinks 段（:685-730）。引擎此前 call link 面只有凭据材料三方法
（calls.linkPresentation/createLinkAuth/groupProof，P4c 切一），无
storage 同步腿——其他设备创建/删除的 call link 状态对新链接设备不可见，
connector/desktop 侧无通道读写链接清单。

- **读投影（getCallLinks）**：manifest 全部 CALL_LINK（=7）identifier
  批读（A4/A5 同款批读纪律）→ CallLinkRecord 三字段投影：rootKey
  （16/21B 标准 base64）+ adminPasskey（active 态在场）+
  deletedAtTimestampMs（deleted 态在场）。删除态归一：deletedAt 在场
  即视为删除、adminPasskey 恒不透出（官方 mergeCallLinkRecord
  isBadRemoteData「assume deleted」同位 + toCallLinkRecord deleted
  分支 adminKey 恒缺同形——远端坏形 deletedAt+adminPasskey 并存同样
  归删除态）。rootKey 空 = 官方 shouldDrop 'no rootKey' 同位跳过。
- **写面（setCallLink）**：两腿恰一——active = rootKey+adminPasskey、
  deleted = rootKey+deletedAtTimestampMs（官方 deleted 分支 adminPasskey
  恒不写）；官方 upload 语义：定位 replaced key → 新 16B item key 换入
  + 旧 key 进 deleteKey → PUT → 409 采纳重放（conflictBackOff 3 次，
  置顶/贴纸面同款）；投影等值幂等无写（官方 storageNeedsSync=false 同
  语义）；manifest 404 → master key 播种 + version 1 最小 AccountRecord
  同批首写（置顶/贴纸面同口径）。
- **定位等价记档**：官方以 roomId 定位同链接（roomId = RingRTC
  derive_room_id(rootKey) HKDF 纯函数）；引擎以 rootKey 字节等价定位
  （同 rootKey ⇒ 同 roomId，单射派生），不复制 RingRTC HKDF 派生面——
  引擎 roomId 从 wire 直收（calls.createLinkAuth 参数闸），无本地派生
  需求。
- **proto 收敛点**：vendored libsignal-service 生成 proto 已含
  Type::CallLink=7/CallLinkRecord（tag 7 三字段），与引擎自有
  `crate::proto::storage` 同线格式；写面 RecordPayload::CallLink 进
  storage_sync 统一 shim（与 Account/Contact/GV1/GV2 同通道）；读面用
  引擎自有 proto（A4 同款）。
- **空意图闸**：仅 rootKey（两腿皆缺）不收 -32603——官方 upload 恒
  不写无 adminKey 的 call link（storage.preload.ts adminKey==null
  skip 同位），只读链接（无 adminKey 入库态）不构成写动作。
- **无偏差记档面**：name/restrictions/expiration/revoked 等 call link
  属性不在 CallLinkRecord（官方存本地 DB，storage 只同步 rootKey/
  adminPasskey/删除态三材料——官方 upload 段 storageRecord 构造同域，
  零偏差）。
- **错误面**：读 account 缺席/未注册 `-32603`，链路 STORAGE_UNAVAILABLE/
  STORAGE_READ_FAILED；写另加参数形状闸（rootKey 16/21B、adminPasskey
  非空 base64、deletedAtTimestampMs 正整数、两键互斥、空意图拒）与
  STORAGE_WRITE_FAILED；无 master key 或 manifest 404：读 → 空列表、
  写 → 播种首写（置顶/贴纸面同口径）。
- **fake**：读恒 `{callLinks: []}`；写成功回 `{callLinks: []}`（读面
  纪律）。
- **计数归真机回归**：manifest 拉取/写后全量投影需真机 storage 状态，
  契约测试覆盖 fake 形状 + 参数闸逐形 + real 未注册折叠；单测钉投影
  归一（deletedAt+adminPasskey 坏远端数据归删除态）/意图→记录构造
  （deleted 腿 adminPasskey 恒缺）/线格式往返/幂等无写四族。

### 窗D B13 设备清单读面 + 服务端 unlink 批（2026-10-12，官方对齐 Signal-Desktop WebAPI，wire additive）

缺口总登记 B13 的 engine 腿：新方法 `getDevices`（读面）+ `unlinkDevice`
（写面）（§3 两行）。官方锚：`textsecure/WebAPI.preload.ts`
getDevices（:3044，GET v1/devices）/ unlink（:3011，DELETE
v1/devices/{id}）+ `Crypto.node.ts` decryptDeviceName（:142）+
`protos/DeviceName.proto`。引擎此前无设备管理面：connector/desktop 无
通道看本账号挂了哪些设备、也无法从引擎侧发起服务端解绑。

- **读投影（getDevices）**：GET v1/devices（本机凭据 basic auth，
  reportSpam 同通道）→ 服务器 DeviceInfo 投影（deviceId/name/
  registrationId/lastSeenMs/createdAtCiphertext）。设备名引擎侧解密
  （官方 decryptDeviceName 同构）：DeviceName proto{ephemeralPublic
  (DJB 0x05 前缀序列形)/syntheticIv/ciphertext} → X25519(ephemeral,
  本机 ACI 身份私钥) → HMAC 双链（cipher→cipherKey、auth→synthetic IV
  前 16B 验证）→ AES-256-CTR（零计数器）。解密失败 → name 置空不炸面
  （官方桌面解密失败 warn 同域）；身份私钥取钥失败 → 全部 name 置空。
- **写面（unlinkDevice）**：DELETE v1/devices/{id} 直通——服务器裁决
  授权（primary 可解绑任意 linked、linked 可解绑自身），引擎不做客户端
  主设备闸，越权/主设备删除/不存在等拒绝原样透出（DEVICES_FAILED +
  data.status）；解绑本机设备后后续调用 401 折叠既有一线（凭据失效归一
  批同语义）。
- **记档**：createdAtCiphertext 官方以 identityPublic.seal
  ('deviceCreatedAt' + deviceId||registrationId 关联数据) 加密，v0.99.0
  rust crypto 无该 seal/unseal 暴露位——ciphertext 原样透传，解密面归
  消费方或另批。
- **错误面**：读/写 account 缺席/未注册 `-32603`；服务器拒绝
  DEVICES_FAILED（data.status?）；传输面 DEVICES_UNAVAILABLE（两方法
  同族码位）。
- **fake**：读恒 `{devices: []}`；写成功 `{}`（读面纪律）。
- **计数归真机回归**：服务器清单拉取/解绑结局需真机（fake 无服务器
  面），契约测试覆盖 fake 形状 + 参数闸逐形 + real 未注册折叠；单测钉
  设备名加解密往返（含 synthetic IV 篡改拒绝/裸 32B 无前缀拒绝）。


### 窗D B11 名片发送臂 sendContact 批（2026-10-12，官方对齐 Signal-Desktop sendMessage contact 面，wire additive）

缺口总登记 B11 的 engine 腿：新方法 `sendContact`（§3 一行）。官方锚：
`ts/textsecure/SendMessage.preload.ts` toProto contact 段（:418-492）+
`ts/types/EmbeddedContact.std.ts` numberToPhoneType/numberToEmailType/
numberToAddressType（:108-156）+ `protos/SignalService.proto`
DataMessage.Contact（:262-338，`DataMessage.contact`=9 repeated）。引擎
此前名片面只有收投影（Q2 `projectContactCard`），无发送臂。

- **proto 构造**：`DataMessage.contact` 单条目投影——name 六子字段、
  number/email `{value,type,label}`、address 九子字段、organization
  直投（官方 toProto 同位）；body/attachments 恒空（名片无正文位，官方
  body=undefined 同位）；多条名片 = 调用方拆次调用（官方 API 面数组形
  的单条目化，§5 记档）。
- **type 词表面**：wire 词 number/email 四值 HOME|MOBILE|WORK|CUSTOM、
  address 三值 HOME|WORK|CUSTOM（postal 枚举本无 MOBILE）；词→proto 枚
  举单点映射，phone/email 两枚举同编号（HOME=1/MOBILE=2/WORK=3/
  CUSTOM=4）共用一映射；type 缺省投影 HOME（官方 numberTo*Type(
  undefined)→HOME 默认同位）。**偏差记档**：词表外官方静默归 HOME，引
  擎按 textStyle 先例 fail-closed 拒绝（-32603）——静默改写调用方词比
  拒绝更危险，§3 行同记。
- **avatar 面**：单条 dataURI → `upload_attachments_for_send` 随批 CDN
  上行取回 AttachmentPointer 进 Contact.Avatar（官方
  EmbeddedContactWithUploadedAvatar 上传面同构；官方 avatar 位仅
  contactEntry.avatar?.avatar 在场时成 Avatar，引擎无头像 = Avatar 位
  缺席同位）；isProfile 恒 false（引擎 wire 面无该键——profile 头像
  回填是桌面 UI 层职责，proto 缺席与 false 接收投影同值）；上行失败整
  体 fail-closed（半发名片比拒绝更糟，send 附件面同纪律）。调度日志
  contact.avatar 摘要化（「调度零头像字节」setProfile/updateGroup 同
  纪律），contact 其余键逐字保留。
- **通路**：直聊腿 = `send_message` + P9 身份拒发闸（名片同为内容面）；
  群腿 = dispatch_group MRM 主路径（poll 群臂同款——open_group_sender
  terminated 硬拒 + announcements 闸 + sender-key 主路径 vendored 逐收
  件人 failover），群腿 outcomes 折叠（sendCallMessage 群臂同口径，应
  答恒 `{timestamp}`）。
- **记档**：桌面名片发送还带联系人间转发 UI 语义（联系人归属解析/
  EmbeddedContact 组装归 connector/调用方，引擎收即所得）；官方
  numberTo*Type 接受数字枚举面，引擎 wire 词面（textStyle 同构）。
- **fake**：成功 `{"timestamp":u64}` 两腿同口径；参数闸（词表/形状/
  两腿恰一）fake 同样生效（-32603 可契约测试）。
- **计数归真机回归**：CDN 上行/对端投递需真机；契约测试覆盖 fake 形状
  + 参数闸逐形 + 调度日志摘要化；单测钉 contact_proto 投影语义（词表
  映射/缺省 HOME/avatar 两态/isProfile 恒 false）。


### 窗D B7 blockedAt 读投影透出批（2026-10-12，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 B7 的 engine 腿：blocked 读面补齐 `blockedAtTimestampMs` 透出
（§3 getBlockedConversations/setConversationBlocked 两行 result 形状
additive）。官方锚：`protos/SignalStorage.proto`
ContactRecord.blockedAtTimestamp（field 27）/GroupV2Record.blockedAt-
Timestamp（field 13，注释「if `blocked` is true, 0 means unknown block
time」）+ `ts/services/storageRecordOps.preload.ts` 官方消费面（:395/:763
写面 blockedAt 进 record、:1058-1075 applyMessageRequestState 读面 →
conversation blockedAt）。引擎 blockedAtTimestamp 写面语义 2026-10-06
批已对齐官方（首次拉黑=引擎时刻/幂等保原值/解除清零），唯读投影 v1 裁
剪该位——本批闭合。

- **投影面**：`blocked_conversation_of` 单点扩 `BlockedConversation{id,
  blockedAtTimestamp}`——contact/GV2 记录 blockedAtTimestamp>0 →
  Some → wire `blockedAtTimestampMs` 在场；0（官方「未知」口径）→ None
  → 键缺席（getCallLinks deletedAtTimestampMs 同口径）；GV1 记录
  StorageService proto 历史无该位恒 None（官方同形，Backups.proto 面有
  ——引擎不复制备份面字段，§5 记档）。
- **写后回显**：project_blocked_after 补尾条目（目标置拉黑且不在读取态
  名单）携引擎时刻——flip_blocked/fresh_blocked_record 写面真值同源，
  写后回显即本地写后面；幂等重放轮走 project_blocked 读取态投影，原时
  刻随行。
- **wire additive**：`blockedAtTimestampMs` 仅在场时出现，旧宿主零变化；
  `Ms` 后缀沿 getCallLinks deletedAtTimestampMs 族例（毫秒显式）。
- **fake**：恒空名单（读面纪律）不变——透出位归真机回归；单测钉投影
  三态（>0 透出/0 未知归缺席/GV1 恒无）+ 写后补尾携时刻。


### 窗D B5 storiesDisabled 写面勘核批（2026-10-12，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 B5 的勘核结论：**缺口属实，本批闭合**——引擎 AccountRecord
写面（setAccountConfiguration）此前只收 readReceipts(6)/typingIndicators
(8)/phoneNumberSharingMode(12)/unlistedPhoneNumber(13) 四位，
storiesDisabled(29) 无写通道（vendored 生成 proto 既有该字段，仅缺写面
接线）。官方锚：`protos/SignalStorage.proto` AccountRecord.stories-
Disabled（field 29，注释位 field 28 为 reserved 历史位）+
`ts/services/storageRecordOps.preload.ts` 官方消费面（:702 toAccount-
Record 写面 = itemStorage 'hasStoriesDisabled' 镜像、:2109-2114 merge
读面 = itemStorage.put + onHasStoriesDisabledChange）。

- **闭合方式**：`setAccountConfiguration` 增可选 `storiesDisabled` 布尔
  位（参数闸沿 optional_bool fail-closed 惯例，非布尔 -32603；零可写字
  段闸同批次计入）——该方法是引擎既有 AccountRecord 部分写面（官方
  updateConfiguration 等价），storiesDisabled 是同真相源的账号级开关，
  并入即官方 toAccountRecord 同位写，不新开方法。
- **语义**：读改写仅动该位（字段保真铁面 decode→mutate→encode），同值
  幂等无写，其他设备经 storage sync 收敛（AccountRecord 单点真相源既
  有纪律）；官方读面消费（Stories 面显隐）是桌面 UI 层职责，引擎只承
  担跨设备写收敛。
- **fake**：成功 `{}` 不变（写面纪律）；契约测试补单写成功形状 + 非布
  尔拒绝；单测补写位生效/幂等两态。

### 窗D B8 GV1→GV2 迁移面批（2026-10-12，官方对齐 Signal-Desktop initiateMigrationToGroupV2，新方法）

缺口总登记 B8 的勘核结论：**缺口属实，本批闭合**——引擎群写面此前只
能自建新群（createGroup），遗留 GV1 群（16B legacy id）无迁移到 GV2 的
通道。官方锚：`ts/groups.preload.ts:2348` initiateMigrationToGroupV2
（派生 master key → deriveGroupFields → 成员分类 :2193
getGroupMigrationMembers → avatar 上行 → revision 0 + accessControl
默认 → buildGroupProto → `PUT v1/groups` 建群 → 本地面 → 群更新发送；
错误路径 hasV1GroupBeenMigrated 探测 = getGroup 非 GROUP_NONEXISTENT 即
已迁移）+ `ts/Crypto.node.ts:72` deriveMasterKeyFromGroupV1（HKDF-SHA256
派生式，见下）。

- **新方法 `migrateGroup`**：`{account, groupId(遗留 GV1 群 id 标准
  base64 非空——形状闸 = 合法 base64 非空，不硬钉 16B：HKDF 派生原语
  对任意字节定义良好，官方 libsignal GV1 id 为 16B 但引擎不越权加闸),
  members(必填非空字符串数组，createGroup 成员解析同链), title?(可选
  非空，缺席 = 空串迁入), expiration?(u32，0 = off), avatar?(三态镜像
  createGroup)}` → `{groupId(新 GV2 master key 标准 base64), revision}`。
- **派生链**：`derive_master_key_from_group_v1` = HKDF-SHA256（salt =
  32 零字节、info = `"GV2 Migration"`、OKM 首 32B = 新群 master key，
  官方 deriveMasterKeyFromGroupV1 同式；zkgroup crate 无现成 GV1 派生
  原语，hkdf/sha2 引擎既有直接依赖）；后续 secret params → public
  params → groupId 派生与 createGroup 共用主体
  （`create_group_with_master_key`，createGroup 随机 key 臂 / migrate
  派生 key 臂同链，成员分流/凭据取材/avatar 上行/初始 proto/`PUT
  v1/groups`/本地群态落库全复用）。
- **已迁移兜底**：`PUT v1/groups` 失败后 `fetch_group_state` 读面探测，
  群已在服务器 = 诚实返回既有 `{groupId, revision}`（官方错误路径
  hasV1GroupBeenMigrated 同语义简化版——派生 key 确定性，重复迁移 =
  服务器已存群；官方完整语义还含版本比对与本地群态合并，桌面侧职责，
  引擎不做）；createGroup 臂（随机 key 无冲突面）不探测，错误原样上抛。
- **记档偏差**：①官方迁移全员 ADMINISTRATOR（getGroupMigrationMembers
  全员 Administrator 位），引擎复用 createGroup 加密链
  （encrypt_group_with_credentials：本人 ADMINISTRATOR、其余有凭据成员
  硬编码 DEFAULT）→ 非本人成员角色 DEFAULT（不手搓加密链——加密链
  材料正确性优先于角色位，成员后续可经官方 promote 面升权）；②title/
  expiration/avatar 官方从本地 GV1 群态继承（V1 title/expireTimer/
  avatar 迁入），headless 引擎无 GV1 本地群态 → 调用方显式入参（title
  缺席 = 空串迁入）；③官方迁移本地面（migration 消息发送、拉黑位
  继承、endorsements/storage ID 重置）为桌面侧职责，引擎只承担服务器
  建群与本地群态初始化。
- **错误族**：参数面 fail-closed `-32603`（groupId 缺席·空串·坏
  base64、members 缺席·空数组·条目不可解析等）；服务器拒绝/self 凭据
  取材失败 → `GROUP_MIGRATION_FAILED`（`-1` + message，data.status 透
  传）；avatar 上行链失败 → `GROUP_AVATAR_UPLOAD_FAILED`（createGroup
  同 data）；网络面 → `STORAGE_UNAVAILABLE`。
- **调度日志**：协议上行面纪律（createGroup 同款）；avatar 键形状摘要
  化（调度零头像字节）、groupId/members/title 逐字保留，被拒调用同落。
- **fake**：成功形状桩 `{groupId(随机 32B 标准 base64), revision: 0}`、
  avatar data URI 臂失败 `GROUP_AVATAR_UPLOAD_FAILED`（两臂纪律同
  createGroup）；契约测试 `migrate_group_fake_shapes_and_param_fail_
  closed` 钉成功形状/参数闸逐形/日志摘要化/real 未注册账号折叠；单测
  `derive_master_key_from_group_v1_is_deterministic_and_length_agnostic`
  钉派生稳定性（无官方向量，回归锚）。

### 窗C stretch 收编批（2026-10-12，官方对齐 Signal-Desktop RemoteConfig；三项一收编两维持）

窗C 时序审计批遗留的三项 stretch 偏差逐项求证与处置：

- **项3 30d 窗 remoteConfig——收编**。官方 `getMessageQueueTime()`
  （ts/util/getMessageQueueTime.dom.ts）= `max(RemoteConfig.getValue(
  'global.messageQueueTimeInSeconds'), MONTH/SECOND)`，MONTH 下限 = 30
  天；prekey 本地保留窗（_cleanSignedPreKeys/_cleanKyberPreKeys）消费
  同值。落法：`prekey.rs` 增 remote config 读面
  `fetch_message_queue_time_seconds`（官方 WebAPI getConfig 行为对齐：
  chatService host `GET v2/config`、账号 basic auth
  （`HttpAuthOverride::NoOverride` = 注册凭据）、body =
  `{config: {name: value}}` JSON map，取
  `config["global.messageQueueTimeInSeconds"]` 字符串秒）；窗龄纯函数
  `signed_family_delete_age_ms` = `max(remote 秒值, 30 天 floor)`（与
  官方同式，floor 恒压过服务器调小）；`cleanup_tick` 挂读面（maintenance
  2 天一拍只多一次 GET，官方 desktop 有本地缓存 + ETag 增量拉，headless
  引擎拍频低无需同构）；读面任何失败（非 2xx/坏 JSON/键缺席/非数）=
  None 落 floor——官方 parseIntWithFallback 缺省同 fail-open 语义，行为
  与收编前完全一致。凭据构造抽 `push_service_from_registration` 共用面
  （storage_context 同源单点）。偏差原登记行作废。单测
  signed_family_delete_age_ms_matches_official_max_floor（缺席/低于/等于
  /高于 floor 四态）。
- **项4 SKDM 重发臂——维持记档，未收编**。官方兜底群臂（
  handleRetry.preload.ts:370-440 + conversationJobQueue
  sendSenderKeyDistribution）语义：群 id 来自**密封层 USMC 的
  groupContext**（MessageReceiver.preload.ts:1388
  `messageContent.groupId()`——envelope.groupId 出自 USMC.groupContext，
  非 Content 本体），向**请求者单独** 1:1 重发同 distributionId 的 SKDM
  （不要求 didArchive；NullMessage 才要求）。求证结论：vendored Rust 链
  两处断层——proto `Content` 无 group context 字段（对照官方
  SignalService.proto Content 同样无，群上下文只在 USMC 层）、vendored
  USMC 解密链不透出 groupContext——收编需跨三层 vendored 改造（cipher
  返回型 → presage 事件 → 引擎管线），波及面超出 stretch 语境。引擎
  现兜底「归档过发 NullMessage（新会话敲对端促重建），否则不做」正确性
  安全（SKDM 臂只加速群 sender key 重分发，NullMessage 路径同样触发对
  端重建）；后续立项 = vendored USMC groupContext 透出面 + 兜底群臂。
- **项2 confirmed 位——维持记档，未收编**。官方 signed prekey 轮换龄
  判据 = 最近 confirmed 键 lastUpdate；引擎视 stored = confirmed（记录
  内嵌时间戳即生成时刻）。求证结论：引擎 `update_pre_key_bundle` 单一
  漏斗 = 生成即上传（轮换 PUT 同携新 signed + 新 last-resort），生成到
  上传间隔为秒级同拍，两判据在正常路径等价；差异只在持续上传失败窗内
  出现，且该窗内引擎按生成龄轮换重传，收敛到的服务器状态与官方一致。
  收编成本（vendored store confirmed 位 + 上传成功回写 + 读面切换）大
  于语义收益，维持现登记。


### 窗D B6 linkPreviews 账号级写穿补链·批1（2026-10-12，官方对齐 Signal-Desktop storageRecordOps，wire additive）

缺口总登记 B6 的 engine 腿（三批硬串行批1；批2 connector、批3 desktop
不归本仓）：引擎 `AccountConfigurationUpdate` 此前只收 readReceipts(6)/
typingIndicators(8)/phoneNumberSharingMode(12)/unlistedPhoneNumber(13)/
storiesDisabled(29) 五位，linkPreviews(11) 无写通道（vendored 生成 proto
既有该字段——`src/proto/storage.rs` AccountRecord.link_previews tag 11，
round-trip 保真单测早已钉定，仅缺写面接线）。

- **官方锚**：`protos/SignalStorage.proto` AccountRecord.linkPreviews
  （field 11）+ `ts/services/storageRecordOps.preload.ts` 官方消费面
  （:630 toAccountRecord 写面 = `getLinkPreviewSetting()` itemStorage
  'linkPreviews' 镜像、:1786-1788 merge 读面 = typeof boolean 闸
  itemStorage.put 回灌）。
- **闭合方式**：`setAccountConfiguration` 增可选 `linkPreviews` 布尔位
  （参数闸沿 optional_bool fail-closed 惯例，非布尔 -32603；零可写字段
  闸同批并入）——该方法是引擎既有 AccountRecord 部分写面（官方
  updateConfiguration 等价），linkPreviews 是同真相源的账号级开关，并入
  即官方 toAccountRecord 同位写，不新开写方法。
- **读腿（新方法 getConfiguration）**：AccountRecord 配置字段面 →
  setAccountConfiguration 可写六位镜像 JSON 投影（五布尔键恒在，
  phoneNumberSharingMode 词表内才投——UNKNOWN(0)/词表外 = 键缺席，读面
  宁缺勿错不发明第三词；官方 merge 对未知值回落缺省是端上消费面，引擎
  如实缺席）。真相源 = Storage Service（多设备经 storage sync 收敛）；
  从未有 storage 状态 = 全缺省面（诚实缺省不报错）。**P15c 项2「JSON
  读面无消费方不补」的偏差就此解除**——B6 三批写穿补链的 connector/
  desktop 消费方在途，消费需求成立故立项（§4.50 纪律不受损）。
- **未知键静默忽略纪律不破坏（批1 ④）**：params 解析 = 白名单取键
  （params.get），未知键不参与闸、不炸面；契约测试钉「合法位 + 未知键
  同场照常生效」。
- **fake**：setAccountConfiguration 成功 `{}` 不变（写面纪律）；
  getConfiguration 恒全缺省面（五布尔键 + sharing mode 缺席）；写后回
  显链（写面产出记录 → 读投影回显写后值）由 bin 单测
  `account_configuration_face_projects_record_and_write_echo` 钉形
  （fake 无 storage 态不回显）；契约测试补 linkPreviews 单写成功形状 +
  非布尔拒绝 + getConfiguration 形状/参数面。

### S2a 性能基线打点层（2026-10-12，只测不改；进程内指标 + `engine.perfMetrics` 只读投影，wire additive【engine-compat】）

立项纪律（`kt-desktop-wt-signal/docs/plan/signal-runtime-followup-projects.md`
§附四②）：基线先行、优化另批——本层只落测量/打点面，零行为语义改动；
任何优化项（含 O2 解密基线身份库、O3 flush_barrier 批量化）须凭本层基线
数据另批向 owner 申请。

- **载体**：`src/service/perf_metrics.rs` 进程内有界指标注册表——计时序列
  `{count, totalUs, maxUs, lastUs}`（微秒）+ 计数器；词表封闭
  （`perf_metrics::keys`），未知键计 `unknownKeyDropped` 丢弃，内存有界
  不随运行增长；快照键序 BTreeMap 确定性；进程内存不跨重启。
- **测量点词表（封闭集，本批 31 键）**：
  - 计时：`receive_envelope`（收流信封到达→ack，五臂；QueueEmpty 非信封
    不计，StdioDead 停管线路径不采样）、`receive_project`
    （project_content 薄壳咽喉）、`flush_barrier`（G1 背压栅栏
    Service 咽喉——**O3 行为候选的基线测量面**）、`send_lock_wait`
    （常驻发送管理器锁等待，含 send_gate——P12 #1 观测面）、
    `send_text_total`（直聊族发送整体）、`send_group_total`
    （群发送 MRM/failover 汇总咽喉——P12 #11 观测面）、`rpc_dispatch`
    （JSON-RPC 请求分派整体，respond 单点包裹）、`prekey_quick_check` /
    `prekey_maintenance_tick` / `prekey_cleanup_tick`（P12 #8 节律）、
    `reconnect_backoff`（退避实际等待——P12 2c' 观测面）、
    `initial_sync_requests`（链接后补同步三请求）。
  - 计数：`receive_kind_*` 五信封族 + `queue_empty`、`flush_barrier_ok` /
    `flush_barrier_dead`、`send_text_ok` / `send_text_err`、
    `send_group_mrm` / `send_group_failover` / `send_group_repair_round`
    （409/410 修复轮）、`prekey_quick_check_time_trigger` /
    `prekey_quick_check_volume_trigger`、`stream_outcome_*` 四出口。
- **纪律对位**：零 stderr（compat §1 watchdog 判死活）；测量点全走单一
  咽喉（Service::flush_barrier / lock_registered_for_send /
  dispatch_group / failover / project_content 薄壳），调用序/返回值/控制流
  零变化；零新增依赖（std + serde_json 既有）。
- **fake/real**：同码路真实答——指标是进程真相，不分模式；fake 模式收发
  管线不拉起，序列天然近空。
- **消费面与遗留**：connector 白名单透传 `engine.perfMetrics` 属
  **connector 切片遗留**（连同 `receive_persistence_loop` 逐信封 ingest
  事务打点=最大瓶颈候选的 connector 侧测量）；S2b 实测批以直连 harness
  或经 connector 透传取快照。vendored presage 批窗内部（75ms/30 窗的
  窗填充/解密/批量落库三相细分）在 vendor 面，未随本批打点（vendor 纪律
  成本，engine 侧 `receive_envelope` 已含其端到端时延）——如需相细分另批
  按 vendor/README.md 纪律登记。

### F 链路入站观测批（2026-10-12，connector implementation-plan §4.113 缺口引擎腿，wire additive【engine-compat】）

缺口勘核（§4.113 实证原文）：引擎 Storage 面全是按需请求/应答，服务器
manifest 修订从不主动到达；入站 merge 路径（`storage_merge` 的
mergeContacts/importGroups 两族，§4.112 ingestion 面）落远端记录进本地
库、答汇总，但**不发任何每会话通知**、三 flag（mutedUntilTimestamp/
archived/markedUnread）不进任何汇总——他设备改的静音/归档/标未读到不了
connector/desktop（反方向本机 setConversationState 云写穿是通的）。本批
让「路过」变「观测」：merge 读到的每条会话记录与 per-account 簿差分，
真实翻转合成一条 `receive.conversationState` 通知（§3 通知词表第五词）。

- **观测面**（`storage_sync::conversation_observation_of` 单点）：
  mergeContacts/importGroups 循环内成功解密的记录逐条出 (引擎会话标识,
  三 flag) 观测对——Contact/GV1/GV2 三类，标识口径与写面 canonical/
  拉黑读投影逐臂同构；观测是内容驱动（manifest 标识类型不做准），
  merge 的 shouldDrop/本机 uuid 闸不影响观测（三 flag 不消费身份
  材料）。零额外网络（不复读 item，merge 手里已有明文）。
- **簿语义**（`storage_watch::ConversationStateBook`，进程内存 per-
  account，不落盘不跨重启）：簿 = 「消费方最后被告知的态」。三面登记
  ——入站观测差分（翻转才通知）、本机写后登记（setConversationState
  与 storage.write 三类记录 Written 臂，写方已知**防回声**）、本机读
  投影应答登记（getConversationState 读出态，防冗余通知）。首观测 =
  种子静默（引擎重启簿空，首批 merge 全量登记不发——启动风暴闸；
  消费方启动期收敛走既有按需读面）。值未变不发。远端删记录不清簿
  （本面不建模记录生命周期；重建时与最后已知差分，收敛正确）。账号
  删除摘簿。
- **合流与帽**：一次 merge 批的全部翻转合成一条通知（批内合流）；
  跨调用时间窗合流不做（merge 是 connector 驱动的离散批，启动期
  mergeContacts+importGroups 至多两帧——定时器合流引入后台任务与
  投递延迟，换不来的批量收益）。批帽 256（connector 侧 pinned 128/
  sticker 256 同族量级）：超帽**截断 + `overflow: true` 尾标**——取舍
  对「合并成全量失效信号」：截断保留已差分真值立即落消费方簿，尾标
  保溢出诚实（要全集走 getConversationState 逐会话重读）；失效信号把
  已知真值一并丢弃、消费代价同为全量重读，故取截断+尾标。簿帽 8192/
  账号：超帽新会话不登记（fail-open 盲点，现实账号量级不可及；已登记
  条目照常差分）。
- **覆盖边界（记档）**：GV1 记录无入站读取路径（两 merge 族分别只读
  Contact/GV2 identifiers）不观测——legacy 族三 flag 的远端翻转本面
  不到达，读/写面（§3 getConversationState 行）不受影响；merge 失败
  （Err）= 无观测无通知（下一轮 merge 重读收敛）；fake 模式不观测不
  通知（fake 无 storage 面）。
- **零内容纪律**：通知只携会话标识 + 三位值（无消息内容）；零 stderr
  （引擎面无日志设施，会话标识不落日志）。通知帧与响应帧同一 stdout
  writer 队列（既有序面）。
- **消费面**：connector/desktop 接线归各自仓批（§4.113 disposition =
  receive 车道 ControlReceive 拦截族；§4.114 的 account 域独立方法车道
  同族）。
- **单测**：storage_watch 六件（簿种子静默/翻转一次/同值不发/登记防
  回声/删记录不清簿重建差分、批差分只收真实翻转按序、簿帽 fail-open、
  通知形状钉定+批帽截断尾标、消费方测试替身严格解析）+ storage_sync
  观测对三族映射（canonical 口径/空守卫/非会话族）+ service 层接线
  （Service×出口帧：种子静默→翻转恰一帧→同值零帧→登记防回声→删
  账号摘簿）+ storage.write 预观测面（三类记录出对/DELETE/坏 record
  归 None）。契约测试不动（fake 面无观测）——68 基线原样。


### 批次 T 发送侧 sync 副本批（2026-10-13，官方对齐 wrapWithSyncMessageSend，wire additive / 记档【engine-compat】）

自发 `SyncMessage.Sent` 转录面收口（审计 T-1/T-8，ADR 0006 为决策记录，本条为
契约登记）：

- **事实基线（审计前提订正）**：直聊内容发送（send 文本/附件/贴纸/quote/预览/
  阅后即焚、editMessage、remoteDelete、sendReaction、pin/unpin、sendContact、
  timer update、note-to-self）**本就经 vendored `MessageSender::send_message`
  尾部咽喉自动产自发 sent 副本**（`create_multi_device_sent_transcript_content`：
  destination string+binary 双写、timestamp、message/editMessage、
  expirationStartTimestamp 按 expire_timer 在场、UDS 逐收件人）；引擎恒为链接
  设备（is_multi_device 恒真），机制恒在武装。直聊 timer update 副本与直聊消息
  副本 expirationStartTimestamp=发送时刻（官方 direct=发送即起算同语义，T-8）
  由同一构造器承载，本批零改码。
- **best-effort 化（vendored fork，vendor/README.md 登记）**：转录上行失败原样
  `?` 连坐主发送结局——收件人已达但副本失败时整发报错，connector 重试队列
  （§4.123）会对已送达收件人重复投递。改为失败记 warn 日志、主发送结局保留；
  note-to-self 包裹腿（sync 即消息本体）不在列，错误仍上抛。本条为发送 RPC
  结局语义变化（行为面，响应形状零变化）。
- **MRM 群转录补两位（wire additive）**：`build_group_sent_transcript` 纯函数
  收口群转录构造——`expirationStartTimestamp`（expire_timer 在场 → 发送时刻；
  timer 关闭/无 timer 群缺席）与 `unidentifiedStatus`（逐成功收件人 string+binary
  双写、unidentified 恒 true——MRM 全程 sealed sender、PNI identity key 恒无，
  官方 proto「Only set for PNI destinations」同位）；destination 双形态恒无
  （群线程身份由 message.groupV2 携带，官方群转录同位）；转录失败记 stderr
  不连坐群发送结局。消费方为其他链接设备（手机/官方桌面）send-state/计时显示。
  单测：`group_sent_transcript_pins_official_fields`/
  `group_sent_transcript_gates_expiration_start_and_uds`。
- **回声环结论（T-1 强制核查项，记档）**：无回声环——`create_encrypted_messages`
  剔除发送设备自身（服务器只向请求内设备投递，自发 sync 只扇出其他链接设备）；
  引擎收到的 sent 转录只可能来自其他自有设备，收侧镜像消费在 connector 已就绪，
  转录永不被引擎再转发；connector sent 镜像按 (destination, sent timestamp) 键
  合并 outgoing 行为幂等兜底。在案「同内容二次转录重发」怪癖（§5，sync-to-self
  信封尾部重发一次）由同一幂等性吸收，本批不动。

### T-2/T-9 storage manifest 拉取腿批（2026-10-13，官方 fetchLatest 消费 + 登录自动拉，engine 内闭环零 wire 新增【engine-compat】）

storage 腿合并实施（「加强版拉取」定案，ADR 0007 为决策记录，本条为契约登记）。
官方勘核结论先行：**官方没有 storage live 推送**——服务器 websocket 对链接设备只
推 E2E 信封 + 队列排空标记；官方「实时」= 改了 manifest 的设备发普通 E2E 信封
`SyncMessage.fetchLatest{STORAGE_MANIFEST}`，收方 HTTP 重拉。本批把「触发信号到站
即弃」改为「到站即拉」：

- **fetchLatest 消费臂（engine 内闭环，wire 零新增）**：receive pipeline
  `Received::Content` 臂收到 FetchLatest 且 type=STORAGE_MANIFEST → 防抖窗沿推后
  （500ms，官方 sync job debounce 同款、trailing 沿——防抖窗内多封合并恰一次拉取
  ；窗沿到期在收流 select 独立臂执行，不阻塞信封投影/ack 时序）→ manifest 重读 +
  merge 走 `pull_storage_manifest` 单点。合并面 = `storage.mergeContacts` /
  `storage.importGroups` **同一内部函数面**（一处权威：消费臂/登录自动拉/connector
  显式 RPC 三链共用，不造第三条 merge 路径）；翻转通知走既有 storage_watch 观测簿
  → `receive.conversationState`（词表第五词，无新词）。非 STORAGE_MANIFEST type
  （LOCAL_PROFILE/SUBSCRIPTION_STATUS/UNKNOWN/type 缺席）不触发；信号面投影
  （§5 P14 组5）照旧，投影与消费正交。IdentityChange 臂不触发（自发 sync 信封换
  钥解密路径现实不可达，消费单点收口）。
- **登录期自动拉（官方 afterFirstAuthSocketConnect 同位）**：建流成功 +2s 沉降发
  三 sync 请求（P15c 项4）同一时机带追加一次，60s 一次性闸同门（重连风暴不重复
  轰）；connector 登录期显式 merge 保留——读面幂等双链无叠加。会话列表完整性
  （无消息群会话登录后显示）依赖此步把 GV2 masterKey 灌 presage store——「群会话
  登录后不显示」主根因的引擎腿补口。
- **并发互斥结论（记档，不新造锁）**：自动拉为纯读面（manifest/items GET + 本地
  写）。引擎单线程 LocalSet 装配（dispatch 与收流管线同线程 await 点交错，无并行
  数据竞争面）；与 connector 驱动的显式 merge/写**交错**时：本地面 SQLite 库级串
  行 + 逐写幂等（同钥 save_identity no-op / profile_key 缺才写 / contact 缺才建 /
  group 命中零网络跳过），交错重复写收敛同值；服务器写面不经此路径（merge 不产
  WriteOperation），storage.write/置顶/拉黑/三 flag 写已有 409 版本闸 +
  conflictBackOff 重放；通知出口 = storage_watch 簿 StdMutex 单点差分，交错两份
  相同观测恰一次翻转。
- **24h 例行拉取不做（记档）**：官方桌面没有 background 周期性 manifest 拉取；
  收敛靠「每次建流登录拉 + fetchLatest 触发信封」两面，断链窗口修订由服务器队列
  重连补投信封触发，登录拉兜底。
- **失败语义**：逐腿 best-effort，打点 `storage_manifest_pull`（消费臂触发源）/
  `storage_manifest_pull_login`（登录触发源）/ `storage_manifest_pull_err`（逐腿
  失败计）——零 stderr 纪律下打点即日志；不重试不停车，下次触发/下次链路建立重补
  （读面幂等）。
- **单测（+4）**：触发判别面（STORAGE_MANIFEST 唯一触发；LOCAL_PROFILE/
  SUBSCRIPTION_STATUS/UNKNOWN/缺席/未知 raw/非 FetchLatest 体全不触发）、防抖器
  （burst 三封合并恰一拉、窗沿 trailing 推后旧沿作废、同窗不重复、火后新窗）、
  登录 60s 闸（首流 true/窗内跳过/超窗重臂——「自动拉恰一次」的闸腿；臂体一次性
  消费为结构保证）、分发面（未注册账号两腿 UnknownAccount 上浮 = 两权威方法恰各
  一调的分发证据；重复拉幂等；零通知出站）。重复拉不重复翻转通知由既有
  storage_watch/Service 出口测试钉定（同一 publish 单点）。契约测试不动（fake 面
  无 storage 触发链）。

### T-9b storage manifest 拉取结果事件批（2026-10-13，`engine.storagePull`，wire additive【engine-compat】）

b719997（T-2/T-9 批）落地后的两个信号缺口之第 1 缺的事件面补口：拉取失败此前只有
内部打点 `storage_manifest_pull_err`（进程内指标，桌面不可观测）；成功信号走
`receive.conversationState` 差分出口（本块 T-2/T-9 批），manifest 无变化时零通知——
桌面收件箱 gate 无法区分「拉取完成」与「拉取没发生」、更拿不到失败面。本批在
`pull_storage_manifest` 单点（两触发臂共用）解析后恒发一条结果事件：

- **事件形状**（契约 1.141 由 connector 侧登记，形状以本块为权威）：

  ```json
  {
    "jsonrpc": "2.0",
    "method": "engine.storagePull",
    "params": {
      "account": "+1555…",
      "trigger": "login" | "fetchLatest",
      "contactsOk": <bool>,
      "groupsOk": <bool>
    }
  }
  ```

  `account` 为通知通道惯例域（receive.emptied/receive.clockskew 同款，多账号归属
  必需）；`trigger` 与 T-9 打点触发键单射（`storage_manifest_pull_login` →
  `login`、`storage_manifest_pull` → `fetchLatest`）；`contactsOk`/`groupsOk` 两腿
  逐腿如实布尔（`false` = 该腿 Err，含未注册账号 UnknownAccount——失败如实上报
  正是本批目的）。规格三字段 `trigger/contactsOk/groupsOk` 逐字不动。
- **触发面恰两臂**：登录期自动拉（60s 闸臂体）与 fetchLatest 消费臂窗沿，都走
  `pull_storage_manifest` 单点 → 每拉恰一条，不重不漏。connector 显式 RPC 链
  （storage.mergeContacts/storage.importGroups 直调 Service 方法）不经本单点，
  **不产生本事件**（消费方勿把本事件当 RPC 应答面）。
- **与 conversationState 正交**：成功位不替代三 flag 差分通知（词表第五词照旧）
  ——本事件只答「拉取发生 + 两腿结局」，不携任何记录内容；manifest 无变化时
  conversationState 零帧但本事件照发，「完成」与「没发生」从此可分。
- **best-effort 纪律不变**：notify fire-and-forget（receive.emptied 同款），发射
  失败不连坐拉取结果、不重试；零 stderr 照旧（打点 + 事件即全部观测面）。
- **contacts 腿结论（记档，不实现对称事件）**：勘核结论 = contacts sync **有**
  干净的完成可签名点，且**已存在并已投影**——`Received::Contacts` 臂投影的
  `receive` 信封 `syncMessage.contacts.complete` 旗标（官方 handleContacts →
  contactSync:complete 同位；空记录照投、complete 旗标即信号，P15a 项3 既有
  语义），connector/desktop 以「complete=true 的 contacts 信封到达」为完成信号
  即可，`engine.contactsSync {ok:true}` 与之冗余不加。失败半边无廉价对称面：
  presage 栈内 blob parse 失败上游静默丢（filter_map）、store save 失败 break 后
  不产错误事件、主设备不回传在引擎层无超时建模（造 `ok:false` 需新加定时器/状态
  机，非「廉价对称面」）——contacts 半边失败由桌面 gate 自身超时兜底，引擎不硬造。
- **单测（+2，改 1）**：两触发臂各发恰一条 + trigger 单射映射 + 腿位如实
  （storage_pull_result_event_fires_per_trigger_with_truthy_legs）、通知帧消费方
  替身严格解析恰四键验型 + 词表外整拒
  （storage_pull_notification_shape_pins_wire_contract）；分发面测试改写——原
  「零通知出站」断言由结果事件契约取代（每拉恰一条 storagePull + 观测批空
  conversationState 零帧）。既有事件通道测试不动（receive.emptied/clockskew/
  conversationState 形状面原样）。

### finishLink/listAccounts 账号自有 ACI 透出批（2026-10-13，connector 契约 1.142 消费面，wire additive）

桌面呼叫链（call.self 帧源，桌面 F3）需要账号自有 ACI（self-UUID），引擎两
代 jsonRpc 面此前都不暴露——`finishLink` 只答 `{number}`、`listAccounts` 只答
`[{number}]`。connector 契约 1.142 已先行钉定消费面：解析两脸 number 旁可选
`aci` 键，canonical 归一梯（dashed 8-4-4-4-12 或裸 32hex、任意大小写 → dashed
lowercase；错长/错位/非 hex 一律非 ACI）在 connector 侧。本批闭合引擎供给腿。

- **真值源与形状**：presage 注册数据 `service_ids.aci`（`ServiceIds` serde
  flatten，store 落盘键名 `uuid`）**原生序列化形直投**——`Uuid` 的 serde
  落盘形与 Display 同为 dashed lowercase，引擎不二次变换、不重复归一（归一
  梯由 connector 1.142 消费侧承担）。真值来源恒为类型化 `Uuid`：键类型恒
  string，无垃圾态。
- **缺席语义（整键缺席，不出 null）**：store 不可读/注册数据缺席/并发幂等
  臂（finishLink 重试收口时首臂已收走会话槽，ACI 不为此回查 store）= 键
  不在场，重试消费方按可选键语义退回旧行为；fake 账号（dir=None）/fake
  模式恒无键。additive 位 best-effort（`account_aci` 单点）：宁缺勿炸，不
  连坐链接与列表主结局。
- **两脸**：`finishLink` result `{number, aci?}`；`listAccounts` 条目
  `{number, aci?}`——多账号各带各的（逐 store 读注册数据，注册表快照后放
  锁做盘 IO，锁纪律见 `list_accounts`）；finishLink 收口臂在既有设备名写
  臂后追加读取，链接主结局零回归。
- **fake**：成功形状 `{number}` / `[{number}]` 原样（无 store 即无 aci），
  契约测试锁整键缺席防回归。
- **计数归真机回归**：real 链接端到端需真机；单测钉 ACI 真值 dashed
  lowercase 直投、空 store/悬挂目录缺席、listAccounts 多账号各带各的。

### 通话中继配置面（calls.relays，2026-10-13 批，批次 G 第一腿，wire additive）

jsonRpc 面新增 `calls.relays {account}`（KT 扩展方法，命名随仓内 `calls.`
通话域前缀——任务书举例 `calling.relays`，仓内既有域名为准，偏差登记）：
认证 GET `/v2/calling/relays`（官方 getIceServers 同端点）拉取 ICE/TURN
配置，缓存 + TTL + 新鲜度如实暴露。官方桌面 ICE/TURN 不写死本地
（WebAPI.preload.ts:796 路径 + :3064 调用）；引擎握 presage 链接账号凭据，
同一 API 同类用法。

- **勘察结论（可行性，官方姿态）**：
  - 鉴权类 = chatService identified 账号 Basic Auth（`_ajax` outerParams
    默认 `user: username, password: password`，getIceServers 调用不带
    `unauthenticated`/`accessKey` 位，WebAPI.preload.ts:1856-1870/:3064）；
    引擎侧对应 `PushService::request(GET, Endpoint::service, NoOverride)`
    ——NoOverride 挂 presage 注册数据账号凭据，同一鉴权族。
  - 刷新节奏 = 按需（起呼 `#handleStartCall` → `#getIceServers`），无定时
    轮询；缓存 TTL = 应答 `relays[].ttl` 最小秒值，任一条目 ttl 缺失或 0
    → 0 = 不缓存（`reduce min(ttl ?? 0)`）；TTL 内零网络。
  - 失败姿态 = 缓存未过期不打网络；过期重拉失败错误上抛（起呼失败）；
    空应答（relays 缺席/空数组 → 转换列表空）不覆盖既有缓存（「列表非空
    才写缓存」）；官方应答校验 zodSchema = z.unknown() 未校验
    （TODO DESKTOP-8719）。
  - 应答形状 = `{relays?: [{username, password, urls?, urlsWithIps?,
    hostname?, ttl?}]}`（GetIceServersResultType/IceServerGroupType，
    WebAPI.preload.ts:1000-1013）。
- **与官方的取舍差异（登记）**：网络拉取失败时官方上抛、起呼失败；本引擎
  是后台缓存服务面，失败诚实保留旧缓存（`lastError` 如实记因）+ 新鲜度
  （`stale`/`fetchedAtMs`/`expiresAtMs`）暴露，无缓存则空态——错误不连坐
  其他链路（任务口径）。401/403 例外归解绑诚实错误面（凭据死了旧缓存同死，
  engine.deviceCapabilities 同款），缓存不动。
- **引擎侧落点**：`PushService` reqwest 客户端未禁代理，进程启动面把
  `KT_SIGNAL_SOCKS_PROXY` 覆写 `ALL_PROXY`（compat §1.1 解析顺序）→ 代理
  自动生效，零新增机制。缓存 per-account 进程内存（group_credentials 同款
  纪律），不落盘不跨重启，delete_account 摘除；并发过期重拉无防抖（幂等
  GET，last-write-wins）。
- **应答面**：`{account, relays: [...], fetchedAtMs?, expiresAtMs?, stale,
  lastError?}`——relays 条目官方形状逐字段透传（serde 未知字段丢弃不发明；
  可选字段 None 整键缺席不出 null）；无缓存空态 relays = `[]` 且
  fetchedAtMs/expiresAtMs 整键缺席；`stale` = 无缓存恒 true / 有条目
  now ≥ expiresAtMs（ttl 0 条目当拍即 stale）。
- **fake**：空态应答 `{account, relays: [], stale: true}`（无网络不伪装
  账号面，进程真实态 = 无缓存）。
- **connector 消费面后续批次登记**：connector 仓另有 agent 在写（批次 G
  第二腿），本批 engine 侧 additive，不动 connector/桌面/sidecar。
- **单测（+11）**：解析器钉官方应答形状（全字段/relays 缺席/空数组/未知
  字段丢弃/序列化整键缺席/形状违约 fail-closed）；TTL 语义（最小值/任一
  缺失归零/新鲜边界）；advance 矩阵（非空覆盖带 TTL/ttl0 当拍 stale/空
  应答保旧/失败保旧记因/成功清错因/无缓存失败维持空态）；face 三态投影
  （fresh/stale/空态 + lastError 穿透）。TTL 内不重拉/到期重拉由
  cache_fresh 纯函数 + 服务层直读臂承载（fresh 命中零网络）。契约测试：
  fake 空态形状、real 未注册 `-32603`。

### version 元方法白名单补齐批（2026-10-14，上游 signal-cli JSON-RPC 元方法，wire additive【engine-compat】）

KT 桌面真机验收发现：connector `runtime.doctor`（connector implementation-plan
§4.79，契约 1.96/1.101/1.133）对引擎做三只读探针（`version` /
`engine.protocolCapabilities` / `engine.perfMetrics`）时，`version` 探针回错误
——引擎白名单缺这个上游 signal-cli JSON-RPC 元方法，未知方法 `-32601` 上抛；
doctor 把「携带版本面的引擎不该缺」的该方法错误按**上游错误类**降级为
`present:false`，固定理由文案「the engine answered an error for the version
method (an engine that does not carry it surfaces the upstream class)」经
connector 原样冒上桌面。本批闭合引擎供给腿（connector/桌面零改动）。

- **契约结论（connector 侧只读调查，本批实现依据）**：
  - 方法名 `version`（裸名，上游 signal-cli JSON-RPC 白名单动词，非 KT 扩展
    命名空间）；调用点 `connector/src/doctor.rs::probe_engine_method`——空参
    `{}`、`CallClass::ReadOnly`、3 秒诊断预算（`DOCTOR_ENGINE_CALL_TIMEOUT`）。
  - 应答消费 = **原样透传**：doctor 把 result 装进 `EngineMethodFace.face`
    逐字上桌面（不 re-key、不归一、不重推导）；doctor-contract.md 对该
    face 的机器判据只有「是 JSON 对象」，字段级形状以上游 signal-cli
    `version` 应答为准 = `{name, version}` 双字符串键。
  - 错误语义 = 错误类降级：timeout/not-running/exited/backpressure 答
    「stopped answering mid-probe」，其余错误（含本批修的 `-32601`）答上游
    错误类固定理由——present:false + reason，诊断整体不失败。
- **实现面**：dispatch 常量面臂 `version`（零参数 params 忽略——wire 宽容，
  generateInviteLinkPassword 同口径；账号无关，未绑定账号照常应答；
  fake/real 同码路真实答——常量面无模式分叉，calls.endorsementsPublicKey
  同款）；读面不落调度日志零 stderr（engine.protocolCapabilities 同纪律）。
  应答 `{name: "kt-signal-engine", version: CARGO_PKG_VERSION}`——身份诚实
  自报引擎本名与本仓版本，不冒充上游名（doctor 原样呈现，冒名将制造
  「signal-cli 0.1.0」式误读，自证面「如实自报」同纪律）；libsignal 基线
  **不在本面**（`engine.protocolCapabilities.baseline` 的
  libsignalVersion/presageRev/libsignalCommit 所有），不自造字段、白名单
  其他方法零改动。
- **测试（+2）**：payload 形状单测（恰双键 + 本名钉定 + 版本非空）；契约
  测试 real/fake 双模式形状、doctor 同形空参 + 多余参数宽容臂、读面不落
  调度日志断言。未知方法 `-32601` 既有闸不动（词空间外动词仍拒）。

### 存量历史导入取消 + 失败显形批（2026-10-14，官方行为基准对齐 Signal-Desktop，wire additive【engine-compat】）

真机痛点：扫码关联后的存量历史下载（transfer archive）没有任何取消手段、
失败完全静默，桌面 UI 只能傻等。官方 Signal Desktop 行为基准：**取消 =
立即中止下载并清理半成品；失败 = 显形（带错误与重试）；「手机端没传历史」
是独立信息态不是错误**。本批闭合引擎侧三件事（connector/桌面消费批另批）。

- **新方法 `history.importCancel {account}` → `{"cancelled": bool}`**（KT
  扩展命名空间，dispatch 风格仿 `backups.restoreFromCloud`）：按账号中止
  在途导入任务两腿——transfer_archive 下载 + 云备份 restore——并清理半成品
  （`transfer-archive.bin`/`history-import.ndjson`/`cloud-backup/`/
  `history-import-cloud.ndjson`）与快照面（在途 + failed 终态同清）。
  `cancelled:true` = 确实中止了在途任务；`false` = 无在途任务（幂等成功，
  不假装取消了什么）。account 缺失/非字符串 `-32603`；无其他错误面（未
  注册/fake 账号答 `cancelled:false`——快照面是进程内观测，不泄露注册
  状态，transfer.downloadStatus 同纪律）。半成品按腿清理：取消哪腿清哪腿
  （被中止任务的在写 NDJSON 一并删，被取消的导入不被 connector 消费链
  悄悄补上）；另一腿已完成待消费的产物不连坐。
- **取消机制**：任务句柄注册表 + `JoinHandle::abort`（引擎单线程 LocalSet
  语义：abort 返回后任务不再执行任何指令，在途长轮询/CDN 流随 future 断，
  取消侧清理与任务写盘天然无竞争）。transfer 腿 = per-account 单槽注册表
  （`TransferDownloadRegistry`，重链覆盖旧句柄）；restore 腿 = 进程级单槽
  （归属账号 + JoinHandle），云备份运行闸改挂 `RunningGuard`（Drop 释放
  ——abort 路径闭包收尾不执行，原先「收尾释放」的闸在取消路径会永久泄漏
  BACKUP_RUNNING，守卫化后两路都不泄漏）。
- **失败显形（transfer_archive）**：`download_transfer_archive` 不再
  「任何一步失败只清快照 + return」——失败保留 `failed` 终态（已计字节
  如实保留 + 短错误码），`transfer.downloadStatus` wire additive 扩
  `phase`/`errorCode` 两键（1.53 形状零回归）。终态清除触发器恰三：下次
  begin（新会话整体重置）、`history.importCancel` 取消、账号重链
  （finish_link 同号重链时中止旧任务 + 清终态）。快照缺席语义不变 =
  无在途任务。`no_archive`（204 重试耗尽）按官方基准是独立信息态非故障，
  桌面据此呈现「手机端没有可传历史」而非报错。错误码短码词表（不含消息
  内容/路径/敏感信息——引擎仓红线）：`no_archive`/`network`（长轮询面
  传输失败）/`cdn_unreachable`（CDN 段失败，含新增的非 2xx 状态检查——
  此前 CDN 错误页会流进档案落到 decode_failed，语义归位）/`decode_failed`
  /`io`。云备份 restore 腿失败显形既有（cloudStatus failed 相位 BACKUP_*
  码表），本批不重复建设。
- **生命周期卫生**：`deleteLocalAccountData` 摘账号时取消两腿在途任务并
  清快照（终态无人消费）；restore 任务单槽与 cloudStatus 快照面补会话
  归属（owner，不投影 wire）——取消面按账号判定终态清不清，restore 单槽
  属其他账号时不动其快照。
- **fake**：`history.importCancel` 恒 `{"cancelled": false}`（无网络不
  伪装任务面）；契约测试钉幂等形状/参数闸/不落调度日志/未注册同形状；
  单测钉注册表取消语义（在途命中/幂等 false/已收尾 false/abort 即弃）、
  快照状态机（终态保留/冻结/begin 重置/清除触发器）、两腿半成品清理器。
  在途中止/CDN 流中断取消需真机，归真机回归。

### send 附件 GIF 旗标发送面（2026-10-14，官方行为对齐 Flags.GIF=8，wire additive【engine-compat】）

官方 GIF 消息 = 普通附件消息 `DataMessage.attachments[].flags` 带
`Flags.GIF=8` 位——P14 组 3 已闭合收侧投影（isGif 恒投布尔），本批补齐
发送侧，两端 flag 位同位闭环。

- **send 附件 data URI 参数面 additive 扩 `;gif=true`**（voice=true 同款
  参数面：裸 `gif` 与 `gif=true` 等价、`gif=false` 显式不带、非 bool 值
  fail-closed `-32603`）。无 mediatype 闸——官方 Tenor GIF 常为
  video/mp4 载体，GIF-ness 全由 flag 位陈述非内容嗅探（`image/gif`
  mediatype 也不自动带旗标，flag 是显式参数），媒体类自判归消费方，收
  投影 isGif flag-only 同口径。
- **实现位与依赖证据**：钉定 presage rev `96f5e92` 传递的 libsignal-service
  `7aa4909` 中 `AttachmentSpec` 无 gif 字段、`upload_attachment` 返回指针
  flags 仅从 voice_note/borderless 两源计算（vendor/libsignal-service/
  src/sender.rs:78-89/:271-281）——flags 不能经 spec 面表达。flags 是
  客户端线上陈述、CDN 上行面（form/URL/digest）零涉及：engine 在
  `upload_attachments_for_send` 上传返回后按 parsed 序对齐置位（presage
  `join_all` 保序 input=output），与官方「消息组装期置位」同构——vendor
  零改动，官方 CDN v4 面保持零 fork。
- **置位单点 `apply_outgoing_gif_flag`**：与既有位（voice/borderless）
  按位掩码叠加不挤占、重复置位幂等。直聊/群组/viewOnce/名片 avatar 四腿
  同经 upload_attachments_for_send 单点生效；贴纸图走
  upload_sticker_image 单条面不经此处（官方贴纸指针无 GIF 位）。
- **出向 transcript 落库的是置位后指针**：多设备同步副本/历史导出经
  同一 `project_attachment_pointer` isGif 如实读回，收发两端无投影面
  分叉。
- **单测**：gif 参数面（裸/true/false/fail-closed/voice+gif 共存）、置位
  面（平指针单 GIF/voice 叠加/幂等）；线上端到端置位需真机 send，归
  真机回归。

### send 语音笔记波形+时长元数据全链（2026-10-14，官方行为对齐 AttachmentPointer.audioWaveform=21/audioDurationSeconds=22，wire additive【engine-compat】）

官方语音笔记 wire 面 = 附件指针两位元数据：波形（每字节一桶、0=静音
255=最响、至多 100 桶）+ 时长（float 秒）。此前四路官方蒸馏的「proto
21/22」估记按本批 proto 核实订正如下，发送/接收两侧同批闭环。

- **proto 核实结论（以 proto 为准，蒸馏笔记三处订正）**：宿主消息 =
  `AttachmentPointer`（非「消息/附件 proto」模糊位——`DataMessage` 无
  波形字段）；字段编号 21/22 与蒸馏估记对上；桶数恒 **100**（非「约
  64 桶」——proto 注释「At most 100 bytes/bars」+ 官方 Android
  `AudioWaveFormGenerator.BAR_COUNT=100` 双锚）；时长 = **float 秒**
  （`audioDurationSeconds`，非整型）。字段原文逐字取自 Signal-Android
  `lib/libsignal-service/src/main/protowire/SignalService.proto`（wire
  权威在 Android 侧：语音笔记由手机端生成波形，本仓上游对照桌面
  abe80d3 的 proto 尚未同步这两位、收包按未知字段跳过并本地重算显示
  波形——桌面 `useComputePeaks.dom.ts`/`VoiceNotesPlaybackContext.dom
  .tsx` 均为本地 RMS 重算路径，非 wire 消费面）。vendored
  libsignal-service proto（旧快照，AttachmentPointer 至 Next ID: 21）
  已按上述两位同步（wire additive-only），fork 登记见 vendor/README.md。
- **发送面（`;voice=true` 路径）**：官方算法行为对齐（本仓自有实现
  `service/voice_note.rs`，不抄码）——音频解码 → 播放域时长均分 100 桶
  → 逐桶样本均方（全声道逐样本计入，16-bit 满幅 32768² 归一）→ dB 尺
  度映射：`db = 10·log10(meanSquare)`，−60 dB 噪声床以下归 0，
  `bar = round(255·(db+60)/60)` 截 [0,255]，空桶 0（官方
  `meanSquareToBar`/`BarAccumulator` 同构）；波形恒 100 字节、时长
  float 秒随 `audioWaveform=21`/`audioDurationSeconds=22` 置入出向指针。
  置位位 = `upload_attachments_for_send` 上传返回指针（presage
  voice_note 已置 VOICE_MESSAGE 位，GIF 旗标批同构叠加，`apply_outgoing_
  voice_metadata` 单点）；出向 transcript 落库同指针收发两端无投影分叉。
- **失败姿态（官方 catch-all 同构）**：`GenerateAudioWaveFormJob` 生成
  失败仅记日志照发——引擎不可解码 → `None` → **两字段全不带**照发
  （官方 wire 恒成对，不产「有时长无波形」半对形状）。解码覆盖矩阵：
  OGG/Opus（connector voiceNote 主格式）+ WAV/PCM 全解码；mp3/m4a/
  ogg-vorbis 等其余容器落失败姿态（后续按真实流量需要 additive 扩面）。
  OGG 时长基 = 最后 granule − pre-skip（官方容器时长同源），WAV =
  data 帧 ÷ 采样率。
- **依赖报备（AGENTS.md 纪律）**：全图此前零音频解码能力，新增两 crate
  均**纯 Rust、零传递依赖、无 C/FFI**：`ogg` 0.9（BSD-3-Clause，OGG
  页/包解包 + granule）、`opus-decoder` 0.1（MIT OR Apache-2.0，纯 Rust
  Opus 解码 RFC 8251 同构；symphonia 0.6 仍无 Opus 解码臂，C 绑定
  opus-sys 系违背本仓 bundled 纯构建姿态，故取纯 Rust 解码器）。WAV/
  PCM 仓内自解析不引依赖。用途/许可登记见根 Cargo.toml 同批注释。
- **接收面（收投影 additive）**：`project_attachment_pointer`（live
  接收信封 + quote thumbnail + sticker data + preview image 同一投影
  函数全生效）增 `waveform?`（标准 base64，非空才投——blurHash 同款，
  空字节串零信息量）与 `duration?`（float 秒，在场即投）。官方发送端
  恒成对，收侧各自独立如实投位不加成对闸（宁多投不误判）。官方
  isGIF 类媒体闸不适用——两位是纯数据透传，渲染/重算归消费方。
- **历史导入腿 proto 无源**：backup 档案 `FilePointer` schema（官方
  Backups.proto 同形）无波形/时长两位，`project_pointer_common` 无可
  投影源——导入行两键恒不在（非偷懒，schema 事实）；导出面同理无写
  位。live 两腿同源要求由 proto 事实豁免，后续官方 archive schema 若
  扩位随批补。
- **单测**：桶值 dB 向量（0/−10/−20/−30/−60/超噪床下）、WAV 全链
  （合成 PCM16 单声道/立体声 → 期望桶值+时长）、OGG/Opus 管线（合法
  OggS 流 + OpusHead + PLC 空包 → 恒 100 桶 + granule−preskip 时长）、
  失败姿态矩阵（非音频/无 OpusHead OGG/截断 WAV/零 granule 全 None）、
  置位面（voice+gif 共存互不挤占）、收投影（base64/空串不投/独立投/
  全缺键不在）。真机端到端（真语音附件 send/receive roundtrip）归真机
  回归。

### 通话事件出向同步批（2026-10-14，官方对齐 Signal-Desktop abe80d3 updateRemoteCallHistory/getDeleteCallEvent，wire additive【engine-compat】）

官方通话事件跨设备同步出向 = 通话接听/挂断/未接/删除/observed 等事件
发生时，客户端向自己其他关联设备发 `SyncMessage.callEvent`（content
oneof 字段 19），各设备会话据此落通话记录条。引擎此前只有入向投影
（2026-10-06 通话消息面批，§3 receive 行 `syncMessage.callEvent`——本批
核实其与 proto 一致，零改动只记档），出向半本批闭合。

- **proto 核实结论（vendor/libsignal-service/protobuf/SignalService.proto
  逐字，与既有蒸馏记档无出入）**：`SyncMessage.CallEvent` 六字段封闭——
  `conversationId=1`(bytes：1:1 = 对端 service id、群 = group id、ad-hoc =
  room id)、`callId=2`(uint64：1:1 直接生成、群/ad-hoc 自 era id 派生)、
  `timestamp=3`(uint64)、`type=4`、`direction=5`、`event=6`；枚举域
  `Type{UNKNOWN_TYPE=0, AUDIO_CALL=1, VIDEO_CALL=2, GROUP_CALL=3,
  AD_HOC_CALL=4}`、`Direction{UNKNOWN_DIRECTION=0, INCOMING=1,
  OUTGOING=2}`、`Event{UNKNOWN_EVENT=0, ACCEPTED=1, NOT_ACCEPTED=2,
  DELETE=3, OBSERVED=4}`；`SyncMessage.callEvent=19`。桌面
  `callEventNormalizeSchema`（CallDisposition.std.ts:287-311）与 proto
  同构：conversationId 16B → UUID 串/否则群标准 base64/ad-hoc hex、
  callId longToString 十进制串——引擎入向投影词表（§3）逐面命中。
- **触发时机（官方，事件源对位）**：`updateRemoteCallHistory` 每本地通话
  历史状态迁移即发（statusToProto 非空位才同步：Accepted → ACCEPTED、
  Declined → NOT_ACCEPTED、Deleted → DELETE、GenericAdhocCall →
  OBSERVED、JoinedAdhoc → ACCEPTED；missed/ringing/pending/joined 等
  中间态 null 不出线）；删除通话记录 = `getDeleteCallEvent` 单帧作业同
  形状（event=DELETE）。urgent:false、contentHint Resendable、padSync
  Message 包裹、sendIndividualProto(ourAci) 自发。
- **引擎发送面（`sendCallEvent`，KT 扩展）**：参数三词表 + 会话三腿
  （直聊号码/service-id、群 32B master key base64、ad-hoc roomId hex）
  与入向投影同词表严格互逆（往返单测钉定）；callId 直聊/群腿必带（官方
  getCallIdForProto 非 ad-hoc 严格面同位）、ad-hoc 可缺省（官方 call-link
  占位「only send the timestamp」同臂）；timestamp 缺省信封时刻（官方
  调用方 Date.now() 同位）。构造 `build_call_event` 单点六字段全显式，
  presage self-send 通路（sendViewOnceOpen 先例，compat §5 E4）：padded
  sync message 向自己 ACI 单发，`Manager::send_message` 硬编码
  online_only=false 与官方 call 面不设 online 位一致（通话消息块勘误同
  源）。无 sent-transcript、无群/设备定向位（CallEvent 六字段封闭）。
  直聊腿无身份拒发闸（自发同步面，对端加密不涉——与 read/viewed/
  viewOnceOpen 自发面同纪律）；直聊寻址 = resolve_recipient（send 系同
  口径），PNI 形 service id 原样 16B 直写（线面无 ACI/PNI 语义位，收端
  16B → UUID 串归一）。
- **触发时机归属（本批边界）**：引擎无本地通话态（RingRTC 侧职责），
  每事件即发的调用接线归 connector/sidecar 后续批——engine 侧只保证
  「一调用一 sync 信封、每事件即发」的发送面语义。
- **失败姿态**：参数面 fail-closed（-32603，§3 行）；上行失败沿 send 系
  错误族（Unbound/RateLimited/ProofRequired/ServerUnavailable/
  Internal，§3 结构化三分类），不重试不静默；fake 面成功形状桩
  `{timestamp}` 三腿同口径 + 落调度日志（send 系同纪律，payload 逐字
  保留——callEvent 参数是记录元数据非正文）。
- **单测**：直聊腿 wire 逐字节六字段号序（callId 用 2^53+1 钉 u64 全
  域）、三词表全枚举映射 + UNKNOWN 词拒绝、ad-hoc（callId 缺席臂 +
  hex 腿）/群（32B 闸 + callId 必带）两腿、参数闸 fail-closed 全臂、
  出向构造 ↔ 入向投影互逆往返；契约测试 fake 形状 + 25 形状拒绝 +
  调度日志纪律。真机回归归 connector/desktop 接线批。

### 按手机号查找批（2026-10-15，CDSI，phoneLookup，wire additive【engine-compat】）

- **定位**：KT 扩展方法 `phoneLookup`：号码批 → CDSI（Contact Discovery
  Service）→ `[{number, aci?, pni?, isRegistered}]` 输入序逐条投影。
  官方通路全在 vendored/上游原生面：directory.rs `GET /v2/directory/auth`
  自动取凭据 → libsignal-net SGX attested WS 查询（presage
  `Manager::discover_contacts_by_phone_number`，本批随 presage `cdsi`
  feature 开启启用；该 feature 只把 libsignal-net/net-infra 从 optional
  转必选——两者已在依赖图，Cargo.lock 零新增 crate；不触碰 sqlcipher，
  store-sqlite 的同名 feature 已在 P12/R2 批移除）。
- **出口代理（vendor fork 登记项）**：CDSI 建连原走 directory.rs 硬编码
  `DirectOrProxyProvider::direct()`，绕过进程 ALL_PROXY(socks5h) 出口
  直连；本批改 `DirectOrProxyMode::maybe_proxy` + `ALL_PROXY`/`all_proxy`
  环境读（大写优先，reqwest/hyper-util 同序；socks/socks5/socks5h/
  http/https 受支持，值缺失/畸形降级直连；`NO_PROXY` 不参与判定——进程
  出口策略是单全局变量，main.rs `setup_proxy_env` 统一落位）。
  vendor/README.md fork 清单登记在案。
- **参数与错误面**：`{account, numbers[1..20]}`（帧界 20 =
  usernameLookup 同源镜像）；E.164 语法闸 `^\+\d{7,15}$` fail-closed →
  `PHONE_INVALID`；批级失败两臂：CDSI 限速（服务器 `retry_after` 秒数
  诚实回传 `PHONE_RATE_LIMITED` + `data.retryAfterSeconds`，退避决策在
  调用方）与其余服务端/连接失败（`PHONE_SERVER_FAILED`，细节不外泄）；
  未注册账号 -32603（一切 account 参数面同口径）。
- **降级语义**：未命中/未注册条目 `{number, isRegistered: false}` 不炸
  整批（usernameLookup 同款）；aci/pni 单命中语义（官方 directory.rs
  双命中取 ACI + warn 同口径）。
- **隐私纪律（AGENTS.md 联系人红线）**：查询结果只做内存投影即弃——
  不写引擎库、不发 receive/联系人通知；号码→账号的本地映射仍归
  connector contacts 同步链。CDSI 匹配在服务器 SGX enclave 内进行
  （官方隐私设计），引擎侧不落任何查询痕迹。
- **fake**：逐条回显 `{number, isRegistered:false}`（参数面同闸，
  `KT_ENGINE_FAKE_SERVICE` 早退），不触网不落调度日志（读面纪律）。

## 6. fake 服务模式（仅测试）

`KT_ENGINE_FAKE_SERVICE=1` 显式开启（生产缺省永远走 real）：P1a 形状桩
原样保留——`startLink` 回夹具形状 URI、`finishLink` 合成 `+1555000xxxx`
号码、send 系方法只落调度日志不触网。用途：IPC 契约测试在无网络环境下
钉定连接器可见的形状/错误码/日志纪律（`tests/contract.rs`）。该开关不是
运行模式切换，连接器生产环境禁止设置。

参数层 fail-closed 闸在 fake 模式同样生效（`-32603` 可契约测试）：
`sendGroupCallUpdate` 的 groupId 32B 形状闸（decode_group_id）、
`sendCallEvent` 三词表/会话三腿/callId 域闸（parse_call_event，2026-10-14
通话事件出向批）、`editMessage` 两腿恰一/recipient 形状闸属此列——闸在
dispatch 参数解析层，先于服务层 fake 早退。服务层闸（open_group_sender 群态刷新/
announcements/terminated、收件人解析、profileSharing 白名单）仅 real
模式生效，fake 形状桩一律早退成功。
