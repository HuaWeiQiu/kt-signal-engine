# JSON-RPC 兼容层契约（signal-cli 兼容子集）

状态：P1a 逐方法对齐基线 ｜ 依据：ADR 0002 ｜ 上游参照：connector
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

- `--data-dir`：引擎全部本地状态根（真实引擎为 presage 存储；P1a 为引擎
  内部状态文件，命名空间 `engine-`，不与 signal-cli 布局混用）。
- `--receive-mode on-start`：daemon 就绪即拉取服务器消息（P1b 起 presage
  websocket 生效；P1a 仅接受该参数并保持语义占位）。
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
| `startLink` | `{}` | `{deviceLinkUri}`（`sgnl://link?uuid=…&pub_key=…` 形状） | — | P1a 形状对齐（服务桩）；P1b presage provisioning 真实 |
| `finishLink` | `{deviceLinkUri, deviceName}` | `{number}` | `-1` `Unknown device link uri.` | P1a 形状对齐（服务桩）；P1b presage provisioning 真实 |
| `listAccounts` | `{}` | `[{"number": …}]`；无账号时空数组 | — | P1a 状态机真实（link/delete 生命周期）；P1b 接 presage store |
| `listContacts` | `{account}` | `[{number, name?, profile:{givenName?, familyName?}}]` | — | P1a 桩（空列表/回显存储）；P2 真实同步 |
| `listGroups` | `{account}` | `[{id(base64), name, isMember, members:[{number}]}]` | — | P1a 桩；P2/P3 真实 |
| `deleteLocalAccountData` | `{account}` | `{}` | — | P1a 状态机真实 |
| `send` | `{account, recipient[]/groupId, message, attachments?, quoteTimestamp?, quoteAuthor?, previews?}` | `{timestamp, results:[]}` | — | P1a 形状对齐（调度日志落 `engine-send-log.jsonl` 供断言，同夹具纪律）；P1b 真实发送管线 |
| `remoteDelete` | `{account, targetTimestamp, recipient/groupId}` | `{}` | — | 同上 |
| `sendReaction` | `{account, recipient[]/groupId, targetAuthor, targetTimestamp, emoji, remove}` | `{}` | — | 同上 |
| `updateContact` | `{account, recipient(单字符串), name}` | `{}` | — | 同上 |
| `sendTyping` | `{account, recipient[]/groupId, stop(显式布尔)}` | `{}` | — | 同上 |
| `getUserStatus` | `{account, recipient}` | `{"isRegistered": true}` | 解绑态 `-32603`（§3 头原文） | P1a 形状对齐；P2 真实 |

`receive` 通知：`{jsonrpc, method:"receive", params:{account, envelope}}`；
P1a **不发任何 receive**——链接后同步消息投影依赖真实消息队列，桩阶段
伪造聊天流量违反仓规（夹具的 emit_receive_after 仅为测试驱动）；P1b 由
presage 接收管线产生真实投影（dataMessage 形状：source/timestamp/
dataMessage.message），reaction/remoteDelete/typing/edit/附件元数据等富
信封随后（connector 1.15 归一化契约的输入侧）。

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
