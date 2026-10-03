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
| `send` | `{account, recipient[]/groupId, message, quoteTimestamp?, quoteAuthor?, previewUrl?, previewTitle?, previewDescription?, previewImage?, mentions?: [{number, start, length}], attachments?: [dataURI]}` | `{timestamp, results:[]}` | 收件人未解析/群不可解/quote 半参/mentions 条目缺字段/附件条目非字符串 `-32603` | **P2 real**：直聊逐号码 + 群组 `send_message_to_group`；quote id/author→ACI + text 本机 store 找回（signal-cli 同行为，找不到缺省）；preview 面 + previewImage 随附件同批 CDN 上行（官方 Preview.image 同构）；mentions→BodyRange.MentionAci；attachments 为 connector 上游唯一形状（data URI 数组，`data:<type>[;filename=<pct>];base64,<payload>`）→ presage 官方 upload_attachments 上行（≤32 条、单条 ≤100MB，超限 fail-closed） |
| `remoteDelete` | `{account, targetTimestamp, recipient/groupId}` | `{}` | 同 send | **P2 real**：直聊 + 群组 |
| `sendReaction` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, emoji, remove}` | `{}` | 同 send | **P2 real**：targetAuthor 号码↔ACI 解析（本机号走注册数据，contacts 命中 E.164，UUID 形状直通） |
| `updateContact` | `{account, recipient(单字符串), name}` | `{}` | 未命中号码：`-32603` | **P1b real**：本地 store 命中即改名；服务器面 P2 |
| `sendTyping` | `{account, recipient[]/groupId, stop(显式布尔)}` | `{}` | 同 send | **P2 real**：TypingMessage Started/Stopped 直聊 + 群组 |
| `sendPinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, pinDurationSeconds?}` | `{}` | 同 send；pinDurationSeconds 超 u32 `-32603` | **P2 real**：官方 pinMessage=27；字段缺席/null=pinDurationForever |
| `sendUnpinMessage` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp}` | `{}` | 同 send | **P2 real**：官方 unpinMessage=28 |
| `sendAdminDelete` | `{account, groupId, targetAuthor, targetTimestamp}` | `{}` | 无 groupId `-32603`（直聊无 admin 概念） | **P2 real**：官方 adminDelete=29，仅群组 |
| `sendDeliveryReceipt` / `sendReadReceipt` / `sendViewedReceipt` | `{account, recipient(单作者), timestamps[]}` | `{}` | timestamps 空/缺/含非数字 `-32603` | **P2 real**：官方 ReceiptMessage type=delivery/read/viewed；逐作者直发（群组会话也直发原作者，官方不整组广播）；timestamps 按 CHUNK_SIZE=100 分帧 |
| `getUserStatus` | `{account, recipient}` | `{"isRegistered": true}` | 解绑态 `-32603`（§3 头原文）；未注册账号/其他失败 `-32603` | **P2 real**：whoami 轻量鉴权探测（401→解绑原文，网络失败不误报解绑）；fake 恒已注册 |

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
  `viewOnce: true`。引用/反应作者号 contacts 命中投影 E.164，未命中保留
  ACI 字符串。textStyles：本引擎钉定的 presage proto 版本无此面，不投影。
- `editMessage {targetSentTimestamp, dataMessage}`（直聊）。
- `typingMessage {action: STARTED|STOPPED, timestamp}`。
- `receiptMessage {timestamp[], when, isDelivery, isRead, isViewed}`
  （connector §4.19 三档面）。
- `syncMessage.sentMessage`：本机其他设备发出的直聊（timestamp/message/
  destinationNumber|destinationUuid + 同 dataMessage 控制面）。
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

## 6. fake 服务模式（仅测试）

`KT_ENGINE_FAKE_SERVICE=1` 显式开启（生产缺省永远走 real）：P1a 形状桩
原样保留——`startLink` 回夹具形状 URI、`finishLink` 合成 `+1555000xxxx`
号码、send 系方法只落调度日志不触网。用途：IPC 契约测试在无网络环境下
钉定连接器可见的形状/错误码/日志纪律（`tests/contract.rs`）。该开关不是
运行模式切换，连接器生产环境禁止设置。
