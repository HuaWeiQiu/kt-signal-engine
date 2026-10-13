# ADR 0006：自发消息 syncMessage.sent 副本——单一转录咽喉 + best-effort 语义

日期：2026-10-13 ｜ 状态：已接受 ｜ 批次 T（T-1/T-8 发送侧两项）

## 背景

批次 S 发送-同步粒度审计（对照官方 Signal-Desktop v8.31.0-alpha.1）把「我方桌面
发出的消息，手机与其他链接设备能否看到」列为方向性核查项：官方桌面每次发送都经
`wrapWithSyncMessageSend`（ts/util/wrapWithSyncMessageSend.preload.ts）向自己其他
设备补发一份 `SyncMessage.Sent` 转录副本，接收侧 `#handleSentMessage` 落为 outgoing
行；若引擎发送侧不产副本，其他设备永远看不到桌面发送的内容。

本仓逐行核对后的**事实基线**（与审计前提的出入一并记录）：

- 直聊发送面**已有**自发副本机制：vendored `MessageSender::send_message` 尾部
  （vendor/libsignal-service/src/sender.rs `if needs_sync || is_multi_device` 臂）在
  多设备账号下自动构造 `Sent` 转录（同文件 `create_multi_device_sent_transcript_
  content`：destination string+binary 双写、timestamp、message/editMessage 双形态、
  expirationStartTimestamp 按 expire_timer 在场、UDS 逐收件人）并向自己 ACI 上行。
  引擎全部直聊内容发送（send 文本/附件/贴纸/quote/预览/阅后即焚、editMessage、
  remoteDelete、sendReaction、pin/unpin、sendContact、setExpiration/timer update、
  note-to-self）都经 `Manager::send_message_with_profile_sharing` 落在这一处。
  引擎账号恒为链接设备（startLink 唯一注册路径，device_id ≥ 2），`is_multi_device`
  恒真——副本机制恒在武装状态。
- 群发送面：MRM 主路径在 group_mrm 自构转录；vendored `send_message_to_group`
  failover 路径由同一 `MessageSender` 咽喉自动转录。
- **真正的缺口**有两个：①转录上行失败会经 `?` 连坐主发送结局——收件人已达但副本
  失败时整发报错，connector 重试队列（§4.123）会对已送达收件人重复投递；②MRM 群
  转录缺 `expirationStartTimestamp` 与 `unidentifiedStatus` 两位。

## 决策

1. **副本生产保持单一咽喉，不在引擎层二次包装**：官方一处包装全覆盖的形态，本仓
   对应物就是 vendored `MessageSender::send_message` 尾部这一处。引擎层再叠一层
   包装会产双副本（接收端按 timestamp 去重虽幂等，但违背官方每发恰一副本的形状），
   且需散改全部调用点。所选改动：①尾部转录失败降级为日志、主发送结局保留
   （best-effort，官方 wrapWithSyncMessageSend / Android
   SignalServiceMessageSender 同语义；note-to-self 包裹腿是消息本体、错误仍上抛，
   不在此列）；②MRM 群转录补 `expirationStartTimestamp`（expire_timer 在场 →
   发送时刻）与 `unidentifiedStatus`（成功收件人、unidentified=true——MRM 全程
   sealed sender、PNI identity key 恒无），失败记 stderr 不连坐。
2. **无转录面维持官方同款**：story（官方 sendStory.preload.ts:338-340「story 同步
   由收尾单独发」，引擎 stories 自同步已实现）、通话信令（官方 CallEvent sync 通道，
   compat §5 通话消息块记档在案）、typing/receipt（官方本就不转录）。
3. **timer 直聊面**：timer update 经同一咽喉自动转录；直聊消息副本的
   expirationStartTimestamp = 发送时刻（expire_timer 在场即置，含 restore_thread_timer
   注入的线程 timer）——官方 direct=发送即起算同语义。timer 关闭臂 expire_timer 缺席
   → 副本该位缺席，同构。

## 回声环结论（T-1 强制核查项）

**无回声环，三层证据**：

1. 客户端扇出排除：`MessageSender::create_encrypted_messages` 显式「never try to
   send messages to the sender device」——向自己 serviceId 发 sync 时，发送设备自身
   从设备集中剔除（vendor/libsignal-service/src/sender.rs，`devices.remove(&self.
   device_id)` 位），服务器只向请求内设备投递，发送设备自身不收回复本。
2. 接收侧：引擎收到的 sent 转录只可能来自**其他**自有设备（手机/官方桌面），经既有
   `SyncMessage.Sent` 投影臂落为 `syncMessage.sentMessage` 事件，收侧镜像消费在
   connector 已就绪（文本/附件/sticker/quote/名片/编辑/DFE），转录本身永不被引擎
   再转发——无环。
3. 兜底：connector sent 镜像按 (destination, sent timestamp) 键合并 outgoing 行，
   即便出现同内容重复信封也不产重复行（幂等）。已知在案的「同内容二次转录重发」
   怪癖（compat §5，钉定 rev 对 sync-to-self 信封尾部重发一次）由同一幂等性吸收，
   本批不改动该怪癖面。

## 后果与边界

- 副本失败语义从「连坐」变「best-effort」：发送 RPC 结局只反映收件人面成败。
  直聊逐收件人成败收集（P15b 批）与 connector 重试队列不再因副本抖动误判重复投递。
- MRM 群转录新增两位（expirationStartTimestamp/unidentifiedStatus）为 wire additive
  （既有 Sent 信封补字段，官方接收端按缺席容忍），消费方为其他链接设备（手机/官方
  桌面）的 send-state/计时显示面。
- 直聊副本内容钉面（destination/timestamp/message/expirationStartTimestamp/UDS）
  由 vendored 上游构造器承载，锚点与规则见本文与 compat §5 批次 T 条目；群转录
  字段由 `build_group_sent_transcript` 纯函数承载并有单测钉定。
