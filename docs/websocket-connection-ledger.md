# WebSocket 连接台账（单连接纪律批，2026-10-16）

> 状态：随 `fix/engine-single-connection-discipline` 批生效。行号为该批
> 落地后的 file:line，后续漂移以函数名为锚。
>
> 背景实证（诊断批结论，本批直接采信）：引擎内曾存在多条并行长活
> identified WebSocket（预期 2 条：presage 缓存 identified + unidentified；
> 收尾实测 4 条），服务器对同设备多连接以 **4409 "Connected elsewhere"**
> 逐条踢除，形成「4409 → 双并发重拨 → 再被杀 → 补位」振荡；且观察到
> T5 任务恰在登录 +60s spawn，与 profile 刷新自动批触发时刻吻合。

## 0. 单连接纪律（本批裁决）

官方语义（Signal-Desktop `SocketManager.preload.ts:391-394`）：一台设备
一条连接；4409 = 有别处连接，官方**不重连**。引擎为连接器托管在线面，
在泄漏修复（本批）之前直接采纳官方「不重连」会产生死连接，故本批顺序 =
先封泄漏（【2】单例纪律 + 【3】重拨串行化），4409 分支语义按
【4】裁定（详见 §3）。

## 1. 创建点台账

`SignalWebSocket` 全部经由 `PushService::ws()`
（`vendor/libsignal-service/src/push_service/mod.rs:198-240`）创建，每次
创建 spawn 一个 keepalive 后台任务；该任务持有连接句柄，句柄克隆全部
消亡 + 任务退出后连接才真正断开。

| # | 创建点 | 触发条件 | 生命周期 | 是否长活 | 关闭时机 | 本批处置 |
|---|---|---|---|---|---|---|
| 1 | receive 管道 identified — `vendor/presage/presage/src/manager/registered.rs:718`（`receive_messages` → `identified_websocket(true)`） | 每账号接收管线建流（`src/service/mod.rs:5932` `manager.receive_messages()`，pipeline `src/service/mod.rs:5709`） | 流存活期（服务器队列排空后仍保持） | **长活（预期主连接）** | 断线/关停/流排空；4409 分支语义见 §3 | 归入进程级 slot 注册表（见 §2） |
| 2 | receive 流内 unidentified — `registered.rs:754`（`receive_messages` 内 `unidentified_websocket()`） | 与 receive 流同建 | 随 `StreamState` 存活 | 长活（预期第 2 条） | 流结束随 `StreamState` 释放句柄 | 同上（unidentified 槽） |
| 3 | 发送常驻管理器（send 缓存）identified — `vendor/presage/presage/src/manager/registered.rs:240`（`identified_websocket`）经 `src/service/mod.rs:10415` `open_registered` 缓存 Manager；发送路径调用点：`registered.rs:346,393,404,414,454,477,1490,1740,1996` 等 | 首次发送族操作惰性建；缓存 Manager 常驻 | **长活（曾与 receive 管道各建一条 identified——泄漏主源之一）** | 是 | 缓存摘除（delete/relink，`src/service/mod.rs:1808,1902`）或进程退出 | 同 Manager 槽位语义改进程级 slot：**同账号与 receive 管道共享同一条**，不再并存 |
| 4 | 发送流内 unidentified — `registered.rs:1997`（`new_message_sender`）等 | 每次 `new_message_sender` 建对 | 随 MessageSender 存活 | 短活为主 | 随 sender 释放 | 归入 slot；同账号复用同一 unidentified 连接 |
| 5 | profile_refresh 独立 Manager — `src/service/profile_refresh.rs:210`（`refresh_chunk` 每 chunk `load_registered`，≤3 并发） | 登录期自动批（`src/service/mod.rs:6015` spawn，+10s 沉降；60s 一次性闸）与显式 `profiles.refreshContacts` | **曾为常驻 identified 长活：refresh Manager 的 identified_websocket 槽在 manager 存活期内持有连接，块执行期间（最长 20s+10s）与 receive/发送连接并存——实测 4 连接的直接来源** | 曾是（chunk 期） | 曾无显式关闭（manager 随 chunk 结束释放，但 keepalive 任务活到连接被踢） | **本批修复**：Manager 仅作 store 连接隔离；其 websocket 取用与全引擎共用进程级 slot（`src/service/profile_refresh.rs:19-23,48-49` 注），不再自建独立连接 |
| 6 | profile.rs set_profile Retain 臂 — `src/service/profile.rs:297` `load_registered` | `accounts.setProfile`（avatar=retain） | 一次性 manager，调用完即弃 | 短活 | manager 释放（update_profile 内 identified_websocket 取用随用随建，keepalive 任务活到连接断/被踢） | 同 5：websocket 归 slot，随用随建共享不并存 |
| 7 | 证书维护 tick — `src/service/mod.rs:5883`（`certificate_maintenance_tick` 内 `load_registered` + `refresh_sender_certificate`） | 10min 周期（1h 内到期才触发刷新） | 一次性 manager 即弃 | 短活 | 即弃 | 同 5：走 slot 共享 |
| 8 | link 完成门禁 — `src/service/mod.rs:9187`（`Manager::load_registered` 仅验读） | 链接成功后验注册数据可读 | **零 websocket**（仅 store 读） | 否 | 即弃 | 无连接面，无需处置 |
| 9 | avatar 取头像 — `src/service/avatar.rs:124`（`open_registered` 缓存句柄）→ `retrieve_profile_avatar_by_uuid` | `contacts.getAvatar` | 复用发送常驻管理器（同 #3） | 随 #3 | 随 #3 | 随 #3 |
| 10 | stories/capabilities/groups/storage 等发送族 — 各 `open_registered(_with_dir)` 调用点（`src/service/mod.rs:1976,2118,2282,2369,2661,2704,2749,2792,2843,2923,2958,3007,3080,3149,3208,3323,3366,3554,3895,3920,3999,4041,4371,4405,4745`；`src/service/stories.rs:325,492,684`） | 各显式方法 | 复用发送常驻管理器（同 #3） | 随 #3 | 随 #3 | 随 #3 |
| 11 | cloud_backup 导出 | 纯本地面（加密导出/上传 CDN），**零 SignalWebSocket** | — | 否 | — | 无需处置 |
| 12 | provisioning（链接中）— `vendor/libsignal-service/src/provisioning/mod.rs:189`、`manager/registration.rs:97` | 仅链接/注册流程 | 一次性 | 否 | 流程结束即弃 | 流程性连接，不进 slot（账号未定，无 key 可锚） |

### 推送服务级旁证

- `vendor/libsignal-service/src/receiver.rs:35`（`create_message_pipe`）：
  presage 不经此路径（`receive_messages` 直用 `identified_websocket(true)`）。
- unauthenticated MRM 群发（`group_mrm`）走 libsignal-net chat 连接
  （`src/service/mod.rs:1566` 单槽缓存），**非 SignalWebSocket**，不占
  identified 设备连接额度，不参与 4409 互搏。

## 2. 单例纪律与重拨串行化实现（vendor fork 面）

`vendor/libsignal-service/src/websocket/slot.rs`（本批新增，fork 改动清单
见 `vendor/README.md`）：进程级 per-account 槽位注册表，键 =
`ServiceCredentials::login()`（uuid+device_id，同设备唯一）。

- **单例**：每账号 identified/unidentified 各至多一条活连接；任何
  Manager（receive 管道、发送缓存、profile_refresh、profile、证书 tick）
  取用同一槽。命中活连接直接复用；槽内连接已死/已被占用请求流才重拨。
- **kill-before-create**：重拨前先 `terminate()` 旧连接（close frame +
  进程退出），杜绝「新建不清旧」。`SignalWebSocket::terminate()`
  （`websocket/mod.rs:495`）+ `SignalWebSocketProcess` kill 臂
  （`websocket/mod.rs` run 循环）+ `close_code()` 记录
  （`websocket/mod.rs:487`）。
- **串行化（单飞）**：同账号同类型重拨由槽级 `dial_lock`（tokio Mutex）
  串行，并发 dial 排队等待而非各自建连——一次 4409 后不再出现 2 秒内
  两个并发重拨任务。
- presage `Manager` 的 per-manager websocket 槽字段已删除
  （`registered.rs:82-85` 注），`identified_websocket()` /
  `unidentified_websocket()`（`registered.rs:240,268`）改走 slot。

## 3. 4409 分支语义（引擎侧）

- 服务器 4409 close frame → vendored 进程记 close code（`websocket/mod.rs:354`）
  并落 warn 字面量日志（含 4409 释义行）。
- receive 流结束臂读判定面（`src/service/mod.rs:6288`）：
  `manager.identified_websocket_close_code()`（`registered.rs:286`）+
  `manager.has_open_identified_websocket()`（`registered.rs:294`）→
  纯函数判定单点 `resolve_identified_close`
  （`src/service/mod.rs:5436`，测试钉形 `src/service/mod.rs:18054`）：
  - `Some(4409)` + 进程内**无**其它活 identified = 外部客户端夺走（用户
    手机重连等）→ `StreamOutcome::Connected`（退避 reset，按既有
    Fibonacci 1→55s + jitter 重连）；
  - `Some(4409)` + 仍有其它活 identified = 自家互搏残影 →
    `StreamOutcome::Recoverable`（退避步进，不立刻重拨；共享槽位保证
    重拨必先拆旧）；
  - 非 4409 → 不进裁定，走既有流结局语义。
- 官方「4409 不重连」偏差记档：引擎为连接器托管在线面，外部夺走后必须
  重连恢复接收（否则死连接）；自家互搏臂已按官方语义 stand down。
  计数面 `stream_close_4409_external` / `stream_close_4409_self`
  （`src/service/perf_metrics.rs`）供 `engine.perfReport` 观测。

## 4. 残余风险与后续

- 4409 判定取自「槽内连接的 close code」：receive 流结束与 close frame
  处理存在毫秒级竞态（流先观察到 EOF、close code 稍后落账）。误判后果
  = 按非 4409 处理（Connected），与既有行为一致，无恶化。
- `identified_open_count` 只看槽内当前连接：send 缓存 Manager 与 receive
  管道共享同一槽后，理论上进程内 identified 连接恒 ≤1（每账号），判定
  面为未来并发路径留位。
- KA-DIAG 临时探针仍在（diag 分支引入，本批未摘）——现场观察任务结束
  后由上级决定整体回退批次。
