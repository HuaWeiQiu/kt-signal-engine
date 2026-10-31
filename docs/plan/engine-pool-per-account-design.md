# 每账号独立引擎进程（引擎池）设计

> 状态：设计稿 v1（待评审，未开工）
> 日期：2026-10-21
> 本文所在仓库：kt-signal-engine（分支 fix/admin-write @ 4814503）
> 交叉引用仓库：kt-signal-connector（本机检出：分支 fix/delete-sync-conversation @ 2f93425）
> 边界：**本文只做设计**。不改任何代码、不 commit、不 push。文中 `file:line` 均以本次检出的两个仓库为准；
> connector 侧改动按 AGENTS.md「跨仓改动走各自仓库流程」另行立项。
> 前置阅读：`README.md`、`adr/0001`、`adr/0002`、`docs/p1-engine-skeleton.md`、
> `docs/jsonrpc-compat.md` §1/§5、`docs/receive-link-incident-2026-10-21.md`、`docs/receive-silence-2026-10-21.md`

---

## 0. 结论摘要

**决策：每账号一个引擎进程（引擎池），由 connector 维护；不做「每账号一线程」。**

三条关键取舍（第 4 节给出量化对比）：

1. **隔离边界取「进程」不取「线程」。** 线程方案里所有账号仍共享一个地址空间与一个 abort 语义：
   一次 OOM、一次 `panic=abort`、一次 libsignal 内的段错误/栈溢出/死锁，仍然同时带走全部账号
   （`docs/jsonrpc-compat.md:6757-6759` 已把 `panic=abort` 这个边界写在纸面上）。进程池把
   「谁能弄死谁」降到 1:1，代价只是每个账号多一份进程底座。
2. **常驻接收 + 驻留上限 + 冷降级，而不是「全部按需冷启动」。** 引擎没有「暂停接收」模式
   （`src/main.rs:88-90` 接受 `--receive-mode` 但丢弃；`src/service/mod.rs:2172-2176` 对所有已登记账号
   无条件拉起接收管线），不驻留就收不到实时消息，只能等下次连接由服务器队列重放
   （`src/service/mod.rs:6936-6949`）。所以冷启动只能作为**明确的产品状态**（休眠账号），不能作为透明优化。
3. **健康面不新增引擎 wire 方法。** 进程与账号 1:1 之后，进程生命周期就是账号健康信号；接收态已有
   `receive.emptied` / `receive.unbound` / `engine.perfMetrics.accounts`（`src/service/mod.rs:6303-6312`、
   `src/service/mod.rs:6921-6933`、`src/methods.rs:4598`）。需要新增的是 **connector → desktop** 的
   additive 事件/字段与 schema 版本，不是引擎协议。

量化依据（第 8 节完整版）：

- 单账号引擎 RSS **64.3 MB**（用户实测）；空引擎 **9.6 MB RSS / 3.6 MB 私有 footprint**（本次实测）。
- 进程池相对线程模型的额外内存 ≈ 每个账号多一份进程底座：**（N−1）×9.6 MB RSS 上界、（N−1）×3.6 MB 物理**；
  10 账号分别是 **+86.4 MB / +32.4 MB**。这部分差值还是上界——RSS 会把共享代码页重复计入每个进程。
- 1/3/5/10 账号总内存（引擎池 + connector）：**87.6 / 216.2 / 344.8 / 666.3 MB**。
- 10 个空引擎并发冷启动到首个 RPC 应答 **25.0 ms**；真正的成本在建流（单账号 3.1–3.4 s，
  `docs/receive-link-incident-2026-10-21.md:25-30`）。

**最大实现风险：存量账号的数据目录拆分（第 9 节 S2）**——不可逆、引擎侧 fail-closed 闸门密布
（`src/state.rs:78-122`、`src/service/mod.rs:12601-12626`），且必须保证同一份 store 永远只被一个引擎进程持有。
缓解路径：S1 只让**新链接**账号走新布局（混合模式），S2 可长期延后；若执行 S2，按单账号粒度、可重入、
全程不删任何凭证（引擎的隔离区只移不删，`src/service/mod.rs:12607-12622`）。

关于 ADR：本决策不改变 ADR 0001 的「独立进程 + 聚合」许可模型（每账号一个 AGPL 进程只会让聚合边界更清晰），
也不改变 ADR 0002 的 IPC 形状（每个引擎仍是同一 JSON-RPC 兼容子集）。建议评审通过后补一份
ADR 0008 记录「每账号一进程」，本文只作为设计稿。

---

## 1. 现状：一个引擎进程承载全部账号

### 1.1 装配事实

| 事实 | 位置 |
|---|---|
| 进程入口 `#[tokio::main]`（多线程 runtime），业务主体在 `amain` | `src/main.rs:373-374` |
| stdout writer 在 runtime 上（Send 任务） | `src/main.rs:405-411` |
| stdin 由独立阻塞线程按行读，channel 桥进 tokio | `src/main.rs:414-439` |
| 全部账号的接收管线 + 全部 JSON-RPC dispatch 跑在**同一个** `LocalSet` | `src/main.rs:443-464` |
| 启动时给每个已登记账号拉接收管线 | `src/main.rs:448` → `src/service/mod.rs:2172-2222` |
| 每条请求 `spawn_local` 一个任务跑 `methods::respond` | `src/main.rs:454-457` → `src/methods.rs:137-170` |
| 账号接收管线同样 `spawn_local`（presage future 非 Send） | `src/service/mod.rs:2224-2251`（`spawn_local` 在 :2241） |
| 账号注册表是 data-dir 根目录的 `engine-state.json`，store 在 `accounts/lk-*/store.db` | `src/state.rs:33-45`、`src/state.rs:78-122` |
| 进程内共享的 Service（state / pipelines / send_managers / 各类缓存） | `src/service/mod.rs:1989-2053` |

也就是说：**一个 data-dir = 一个引擎进程 = 全部账号**，且这些账号的收流与请求在一个协作式调度器上交错执行。

### 1.2 今天已经隔离了什么

这些是有价值的既有资产，进程池直接继承，不要重造：

- **账号级 panic 隔离（仅接收管线）**：`supervision::run_account_supervisor` 用
  `AssertUnwindSafe(...).catch_unwind()` 包住单账号收流任务，panic 只收口本次运行并按账号级
  Fibonacci+jitter 重启（`src/service/supervision.rs:91-131`，退避序列 `src/service/mod.rs:6996-7009`）。
- **账号级出站背压**：全局有界队列 + 每账号信号量，单账号洪峰不会饿死其他账号
  （`src/service/outbound.rs:9-23`、`src/service/outbound.rs:34-37`）。
- **账号级重连退避与终止语义**（Unbound / StdioDead / 1000-4409 停环）：`src/service/mod.rs:6918-6963`。
- **账号级观测**：`account_metrics` 的崩溃计数/建流结局/分位延迟（`src/service/account_metrics.rs:1-40`），
  经 `engine.perfMetrics.accounts` 透出（`src/methods.rs:4598`）。

### 1.3 没有隔离的四类故障（本文的立论）

**(a) 请求侧的 panic 完全没有隔离。** 管线有 supervisor，dispatch 没有：`src/main.rs:454-457` 直接
`spawn_local`，panic 只会让该任务被丢弃——请求永不答复（连接器等自己的 15 s 超时），进程活着；
但一旦以 `panic=abort` 构建/发布，同样是全进程死亡（`docs/jsonrpc-compat.md:6757-6759`）。

**(b) 共享 LocalSet 的饥饿是跨账号的，而且已经真实发生过。** `docs/jsonrpc-compat.md:6309-6313` 记录：
解密风暴吃满 LocalSet，连接器的 `getUserStatus`（15 s 预算，`kt-signal-connector/src/supervisor.rs:82`）
排不进去而失败，连续两次失败即触发**整台引擎**重启。`docs/receive-silence-2026-10-21.md:57-64` 同样记录
「请求与收流共用 LocalSet，REST 会被接收侧挂起拖慢」。这正是「账号 A 的重活拖垮账号 B」的实证。

**(c) 资源压力阈值是进程级的。** 连接器按进程 RSS 采样，超过 512 MB 记 Elevated
（`kt-signal-connector/src/resource.rs:13-16`）。单进程装 8 个账号（每账号 ~55 MB 归属内存）就会逼近该阈值，
一个账号的洪峰让**所有**账号一起进压力态；进程池里每个引擎 64 MB，压力位才真正代表「这个账号出问题了」。

**(d) 重启是进程级的。** 连接器 watchdog 触发 `restart_engine`（`kt-signal-connector/src/supervisor.rs:2477-2512`），
重启的是整个引擎里的所有账号。`docs/receive-link-incident-2026-10-21.md:60-70` 的现场就是：
一条指向「引擎不存在的旧账号」的 liveness ping 失败，把健康的账号一起每 ~330 s 重启一次。

**(e) 内存随账号线性增长，但没有任何按账号的容量边界。** 见第 8 节。

### 1.4 代码与文档里已经承认这件事的三处

- `src/service/supervision.rs:5-8`：「S1 actor-per-account 未落地，全部账号共享同一 LocalSet 线程」。
- `src/service/account_metrics.rs:5-11`：「引擎没有『每账号 mailbox』这一实体（全部账号共享 stdin 单通道与 LocalSet）」。
- `docs/jsonrpc-compat.md:5210-5211`：「引擎单线程 LocalSet 装配（dispatch 与收流管线同线程 await 点交错，无并行数据竞争面）」。
  进程池落地后这句契约陈述必须改写（见附录 B）。

---

## 2. 关键前提：connector 已经是「引擎池」，只是粒度是 proxy group

这条决定了改造成本的大头**已经付过**：

| 既有能力 | 位置 |
|---|---|
| 运行时持有多个槽位，每槽一个 supervisor + 一个引擎 | `kt-signal-connector/src/registry.rs:155-162`、`:2093-2143` |
| 每槽独立 data-dir（`default` 根 + `proxy-groups/<groupId>`） | `kt-signal-connector/src/groups.rs:96-140` |
| 每槽独立占用锁（跨进程 flock，进程退出即释放） | `kt-signal-connector/src/datalock.rs:99-108` |
| **按账号路由已经存在**（account → 绑定 → 槽） | `kt-signal-connector/src/registry.rs:536-555`，全仓 121 处 `slot_for_account(` 调用点 |
| 每槽健康状态（state/pid/rss/pressure/account_count） | `kt-signal-connector/src/registry.rs:119-129`、`:308-350` |
| 每槽启动/停止/收口 | `kt-signal-connector/src/registry.rs:377-433` |
| 链路会话归属到槽 | `kt-signal-connector/src/registry.rs:476-528` |
| 每槽 watchdog（stderr 分类 + liveness ping + 重启） | `kt-signal-connector/src/supervisor.rs:749`、`:2512-2830` |
| 账号无关方法已有「默认槽」先例（贴纸包匿名 CDN） | `kt-signal-connector/src/registry.rs:906-921` |

上限常量目前是「≤8 个组、每组 ≤8 个账号」：`kt-signal-connector/src/groups.rs:22`、`:32`。

**因此本设计的实质是两件事：**

1. **把槽位粒度从 group 改成 account**（一个账号一个槽、一个进程、一个 data-dir）；
2. **把槽位从「启动期静态计划」改成「运行期动态注册表」**（账号是链接出来的，不可能在启动参数里枚举）。

路由、生命周期、健康、锁的骨架都可以沿用；新增的是动态注册、数据目录拆分、容量策略与契约演进。

---

## 3. 目标架构

### 3.1 组件与职责

~~~text
kt-desktop（闭源）
   │ host 契约（本次不改；第 7 节建议 additive 扩展）
kt-signal-connector（闭源 Rust）—— EnginePool
   ├─ 静态槽：legacy proxy-group 引擎（迁移期保留，可承载存量多账号）
   ├─ 动态槽 × N：AccountEngine(accountId)
   │     ├─ supervisor（生命周期 + watchdog + 状态）
   │     ├─ EngineHandle（stdin/stdout/actor，一次一进程）
   │     └─ DataDirLock(<signal-data>/engines/<accountId>)
   └─ 共享槽：anonymous 引擎（无账号，承接账号无关方法）
        │ 每槽一条 JSON-RPC over stdio（ADR 0002 形状不变）
kt-signal-engine × (N+1) 进程（各自 --data-dir）
   └─ 每进程内仍是「单账号 LocalSet」：收流管线 + 请求 dispatch
        │
Signal 服务端
~~~

职责切分：

- **connector**：账号 ↔ 引擎的映射、进程生命周期、data-dir 与占用锁、容量策略、健康聚合、host 契约。
- **引擎**：进程内仍然只做「一个 data-dir 里的账号」这件事；不感知池、不做跨账号调度。
  这保持了引擎的现有语义（一个 data-dir 一个注册表），也让引擎可以独立升级/回滚。

### 3.2 不变量（任何实现都必须满足）

| 编号 | 不变量 | 违反后果 |
|---|---|---|
| I1 | 一个账号在任一时刻只被**一个**引擎进程持有 | 同设备双连接 → 4409 / 消息错位 |
| I2 | 一个 data-dir 只被**一个**引擎进程打开 | `engine-state.json` 与 store 双写，P0-B fail-closed 误触发 |
| I3 | 一个 data-dir 里**恰好一个**注册账号（共享匿名槽除外，0 个） | 引擎启动清扫会把另一个账号的 store 移进隔离区（`src/service/mod.rs:12607-12613`） |
| I4 | 账号的 data-dir 路径由不可逆/不含号码的 accountId 派生 | 违反日志纪律（`docs/jsonrpc-compat.md:40` 与 AGENTS.md） |
| I5 | 停引擎 = 停接收 → drop Manager → SQLite 刷盘，再允许删目录 | 在飞投影写进已删目录（`src/service/mod.rs:2547-2555`） |

### 3.3 数据目录布局

~~~text
<signal-data>/
  engines/
    <accountId>/                 # 一个账号一个引擎 data-dir
      engine-state.json          # version 2，accounts 恰好 1 条（src/state.rs:33-36、:57-61）
      accounts/lk-<hex>/store.db # presage store（ADR 0004 命名不变）
      logs/engine-stderr.log     # 引擎滚动日志按 data-dir 落（src/logfile.rs:60、src/main.rs:384）
      engine-send-log.jsonl      # 调度日志（src/state.rs:34、:185-193）
    _anonymous/                  # 共享匿名槽（0 账号，无 accounts/）
  proxy-groups/<groupId>/        # 迁移期 legacy 槽，保留原布局
~~~

引擎对 data-dir 的要求不变：绝对路径、真实目录（非符号链接）、属主是当前用户、权限 0700
（连接器 `prepare_data_dir`：`kt-signal-connector/src/engine.rs:7670-7696`）。

**为什么不能「共享一个 data-dir + 让引擎只服务其中一个账号」**（这是最省事的诱惑，必须明确否掉）：

1. `engine-state.json` 是单文件注册表，落盘是「临时文件 + rename」的整文件替换（`src/state.rs:124-149`）。
   多进程同时 link/delete 会互相覆盖，最后写入者赢——这正是 P0-B（`src/state.rs:13-24`）在修的那类丢失。
2. 启动清扫按「注册表里没有 → 隔离」处理 `accounts/lk-*`（`src/service/mod.rs:12601-12634`）。
   引擎 B 的 store 在引擎 A 眼里就是孤儿，会被移进 `_quarantine`。
3. 引擎的隔离区非空会直接 fail closed 拒绝启动（`src/state.rs:342-358`），把「配置错误」升级成「整个账号不可用」。

结论：**一个账号一个 data-dir 是硬约束**，没有廉价替代。

### 3.4 proxy group 的角色变化：从「分片单位」降级为「代理策略单位」

代理是经环境变量注入引擎的：连接器把 `KT_SIGNAL_SOCKS_PROXY` 传进子进程，引擎在同步启动段解析并映射到
`ALL_PROXY`（`src/main.rs:104-107`、`:338-363`）。进程池下每个账号的引擎取**该账号所属 proxy group** 的代理即可，
proxy group 的语义（同组共用出口、组内账号数上限 8）仍然成立，只是不再决定「谁和谁共用一个进程」。

### 3.5 共享匿名槽

账号无关方法（第 5.2 节）需要一个稳定承接点。建议常驻一个 **0 账号的匿名引擎**：

- 成本极低：实测空引擎 3.6 MB 私有内存 / 9.6 MB RSS（第 8 节），且可惰性启动；
- 好处：匿名 CDN 浏览（贴纸包）不再依赖某个账号引擎的生死；doctor/版本探测也有稳定靶子；
- 注意：它没有接收管线，不拨 Signal 的接收 WS。

---

## 4. 备选方案：每账号一线程（保留对比）

方向已改为进程池，但线程方案是必须留档的对照物。本节给出设计要点、与进程池的差异，以及「先只移接收管线」
这一步是否可行。

### 4.1 线程方案的设计要点

~~~text
主线程（current_thread runtime + LocalSet）
  ├─ stdin 读线程（保留 src/main.rs:414-439 的形状）
  ├─ JSON-RPC 解析 + 按 account 路由 + 非账号类方法
  └─ 每账号：std::thread + current_thread runtime + LocalSet
        ├─ 收流管线（原 spawn_pipeline 原样搬入）
        ├─ 请求执行器（从邮箱取 AccountCommand，spawn_local 执行 methods::respond）
        └─ 该账号的 Manager / store / 缓存所有权
~~~

路由协议：主线程按 `params.account` 投递 `(id, method, params, oneshot)` 到账号邮箱（有界 mpsc）；
账号线程执行后把 `Value` 回传，主线程统一入 outbound 队列（保持 stdout 单写者与行序）。超时/取消/背压见 5.5。

可行性要点（都是已经存在的硬约束，不是推测）：

- presage 的收流/链接/帧导出 future 非 Send，必须留在所属线程 LocalSet（`docs/p1-engine-skeleton.md:75-77`、
  `src/service/mod.rs:2239-2240`、`src/history.rs:74-75`）。因此**跨线程只能传消息，不能传 future**。
- `tokio::task::spawn_local` 在没有 LocalSet 上下文时会 panic；线程方案要求**每一条**从路由进入的调用链
  都在 `local.run_until(...)` 内（现在之所以成立，是因为 `src/main.rs:443-464` 只有一个上下文）。
- 同一账号的 `Service` 字段必须按账号切分：`send_managers`、`conversation_state_books`、`calling_relays_cache`、
  `whitelist_cache`、`own_capabilities_cache`、`mrm_pool`（`src/service/mod.rs:2001-2052`）。
  共享 `Arc<AsyncMutex<...>>` 在类型上可行（presage 的 `Manager` 本身是 Send+Sync，
  `vendor/presage/presage/src/manager/mod.rs:38-66`），但「句柄可跨线程」不等于「语义可跨线程」：
  同账号的收流与发送本就共享一个进程级 WS 槽（`vendor/libsignal-service/src/websocket/slot.rs:1-20`），
  切分只会把隐式耦合变成显式锁。
- delete/relink 的顺序语义（`src/service/mod.rs:2534-2574`、`:2224-2266`）要变成跨线程编排：
  先让账号线程退出并 join，再删目录。**跨线程 await 一个 LocalSet 任务的 JoinHandle，如果那个线程已经死了，
  会永久挂起**——这是线程方案里最容易写错的一处。
- 现有 `spawn_local` 站点有 40 处（`grep -c spawn_local src`），每一处都要确认它落在正确的账号线程。

### 4.2 对比表

| 维度 | 现状（单进程） | 每账号一线程 | 每账号一进程（本设计） |
|---|---|---|---|
| 账号 A 阻塞/长尾拖慢 B | **会**（`docs/jsonrpc-compat.md:6309-6313`） | 不会（线程级） | 不会（进程级） |
| 账号 A 的 panic | 收流有 catch_unwind；请求无 | 需为请求执行器补 catch_unwind | 天然隔离 |
| OOM / abort / 段错误 / 栈溢出 / 死锁 | **全灭** | **全灭**（共享地址空间） | 只死一个账号 |
| 单账号内存/句柄泄漏 | 全进程受影响 | 全进程受影响 | 只影响自己，重启即回收 |
| 进程级资源阈值（512 MB，`resource.rs:13-16`） | 多账号共享，易误报 | 共享，阈值失去按账号含义 | 每进程独立，阈值恢复语义 |
| watchdog 重启粒度 | 整进程 | 整进程（要自己造 per-thread watchdog） | 每引擎（连接器已有 per-supervisor watchdog） |
| 引擎代码改动 | — | **大**：Service 切分、40 处 spawn_local 复核、生命周期跨线程 | **小**：仅瘦身（S5） |
| connector 改动 | — | 中：主线程路由器 + 邮箱 + 关停编排 | 中大：动态槽 + data-dir 拆分 + 容量/契约（骨架已有） |
| 数据迁移 | — | 无 | 有（S2；可延后） |
| 观测/排障 | 一份日志、一个 pid | 一份日志、多线程（栈难归因） | N 份日志、N 个 pid（反而更清晰） |
| 内存 | 最低 | 最低 + 每账号线程栈 | 最低 + 每账号进程底座（见 4.3） |

### 4.3 内存增量估算（线程 vs 进程）

用第 8 节的实测口径：空引擎 9.6 MB RSS / 3.6 MB 私有，单账号 live 引擎 64.3 MB。
把「单账号归属内存」记为 A ≈ 64.3 − 9.6 = 54.7 MB（这是**上界**：它把 TLS 根证书、二进制代码页、
运行时结构都算进了账号账上，单进程里这些是共享的）。

- 线程模型：`总 ≈ 9.6 + N × A`
- 进程模型：`总 ≈ N × 64.3`
- 差值：`(N−1) × 9.6 MB`（RSS 口径），物理口径约 `(N−1) × 3.6 MB`

| N | 线程模型（RSS 上界） | 进程池（RSS） | 差值 RSS | 差值物理 |
|---|---|---|---|---|
| 1 | 64.3 | 64.3 | 0 | 0 |
| 3 | 173.7 | 192.9 | +19.2 | +7.2 |
| 5 | 283.1 | 321.5 | +38.4 | +14.4 |
| 10 | 556.6 | 643.0 | +86.4 | +32.4 |

**结论：进程隔离的内存溢价上限是每账号 ~9.6 MB RSS / ~3.6 MB 物理，10 账号最多 86 MB / 32 MB。
用这个价钱买「任何单账号致命故障不波及其他账号」，在 5+ 账号的产品形态下是划算的。**
反之，线程方案省下的这几十 MB，需要以「重写引擎并发模型 + 自己造 per-thread 崩溃隔离」为代价，
而且永远买不到地址空间隔离。

### 4.4 线程方案的「先只移接收管线」是否可行

技术上可行，但只能是**过渡态**，不能作为终态：

- 可行的一面：接收管线所需的 `Manager` 与 store 都归该线程独占；请求仍走主线程时，只要保证
  「同一账号的 store 不被两处同时持有」——而现状本来就有两个 `Manager`（收流一个、`send_managers` 一个，
  `src/service/mod.rs:12292-12312`），所以把收流那一个搬到账号线程在 I2 上不冲突。
- 不可行的一面：接收侧维护臂会调用 `service` 的方法并 `spawn_local` 后台腿
  （`src/service/mod.rs:7349-7400`、`:12360-12410`）。这些后台腿一旦落在账号线程，就会和主线程上的
  dispatch 共享同一个 `Service` 的缓存与 `send_managers`；「迁移一半」会把今天隐式的单线程假设变成
  显式的跨线程共享，反而更难证明正确。
- 结论：若将来仍要走线程路线，应一次把「账号线程拥有该账号的全部 Service 状态」做完；
  半程拆分只适合做**观测性实验**，不适合做交付形态。

---

## 5. 路由与协议

### 5.1 账号相关方法

host 方法里的 `accountId` 是连接器自己的不透明账号 id（不是号码）；连接器先经共享 store 解析出该账号的
Signal 号码与绑定，再路由到槽（`kt-signal-connector/src/registry.rs:536-555` 的 `slot_for_account`，
内部走 `account_proxy_group(account_id)`）。全仓已有 **121 处** `slot_for_account(` 调用点，
所以「按 accountId 找引擎」的调用面不需要重写，只需要把实现从「查绑定 → 找 group 槽」改成
「查 accountId → 找动态槽」（绑定列退化成 accountId → engineId，或直接由 accountId 派生）：

~~~text
request(method, params)
  ├─ params.account / params.accountId 存在
  │    ├─ 槽存在且 engine Running → slot.supervisor.call(method, params)   （15s 超时，见 5.5）
  │    ├─ 槽存在但 engine 未启动（冷/未驻留） → 唤醒（6.1）后调用；超过唤醒预算 → ENGINE_WAKING（可重试）
  │    └─ 槽不存在（账号未分配引擎） → 新面 ENGINE_NOT_PROVISIONED（不可重试，账号数据可能已丢失）
  └─ 无 account 参数 → 5.2
~~~

引擎侧保持不变：`methods::dispatch` 仍然只看 `params.account`（`src/methods.rs:195-300` 一带），
不新增任何「池」概念。

### 5.2 账号无关方法

按引擎 dispatch 表逐个确认（`src/methods.rs`）：

| 方法 | 位置 | 是否有账号语义 | 池内落点 |
|---|---|---|---|
| `version` | `src/methods.rs:4575` | 无（纯常量） | 连接器本地缓存即可，不必打引擎 |
| `engine.protocolCapabilities` | `src/methods.rs:4586-4588` | 无（零网络） | 连接器启动时探测一次并缓存；doctor 按槽探测 |
| `engine.perfMetrics` | `src/methods.rs:4598` | **进程**语义 | 按槽读取（每槽=一个账号），连接器聚合 |
| `getStickerPackManifest` | `src/methods.rs:2368-2395` | 无（匿名 CDN） | 共享匿名槽（已有先例 `registry.rs:906-921`） |
| `getStickerImage` | `src/methods.rs:2396-2418` | 无（匿名 CDN） | 共享匿名槽 |
| `generateInviteLinkPassword` | `src/methods.rs:4170` | 无（纯随机） | 连接器本地 |
| `buildInviteLink` / `parseInviteLink` | `src/methods.rs:4174` / `:4226` | 无（纯编解码） | 连接器本地或共享槽 |
| `listAccounts` | `src/methods.rs:231-235` | 读 data-dir 注册表 | 每槽各答 1 条，连接器合并（已有 `registry.rs:436-444`） |

**不要**把账号无关方法随机路由到「任意一个活着的账号引擎」：那会把匿名浏览的可用性绑在某个账号的
崩溃/重启窗口上，而且让「哪个引擎答的」不可复现。

### 5.3 link / finishLink

进程池让链接流程天然变干净：**新账号的 store 从诞生起就落在它自己的 data-dir**。

~~~text
link.start(proxyGroup)
  ① 连接器预生成 accountId（不透明 id，不含号码）
  ② 分配 data-dir：<signal-data>/engines/<accountId>（此时还没有注册表）
  ③ 起一个「临时槽」：supervisor 指向该 data-dir，engine 启动（0 账号）
  ④ 调引擎 startLink → 拿 deviceLinkUri（引擎侧 src/service/mod.rs:2294-2338）
  ⑤ 记录「link 会话 → 槽」归属（沿用 registry.rs:476-528 的 session 路由）
link.finish(linkSessionId)
  ⑥ 引擎 finishLink 成功 → 引擎把账号写进**该 data-dir 的** engine-state.json
     （src/service/mod.rs:2409-2419），并就地拉起接收管线（:2460）
  ⑦ 连接器把临时槽转正：绑定 accountId → 槽，落 connector store
link.cancel / 失败
  ⑧ 停引擎 → 删该 data-dir（此时注册表为空或缺失，删掉不会丢凭证）
~~~

要点：

- **预分配 accountId 是关键**：避免「链接成功后把目录从 staging 改名成正式」——引擎持有的是启动时的
  `--data-dir` 字符串路径，目录被改名后，它后续 `open_store(` 会按旧字符串重建一个空目录/空库
  （`src/service/mod.rs:12303-12315` 用 dir.join("store.db")）。改名必须停进程再重启，
  预分配则完全免掉这次重启。
- 同号重链（同账号再扫一次）沿用引擎既有语义：`finish_link` 会摘旧 manager/缓存并 `spawn_pipeline`
  （`src/service/mod.rs:2420-2460`），进程池下等价于「停槽 → 换 data-dir 内容 → 起槽」。
  若重链沿用同一 data-dir，则不需要搬迁任何东西。

### 5.4 通知（receive 族）

引擎的接收通知都在 params 里带 `account`：`receive.emptied`（`src/service/mod.rs:6303-6312`）、
`receive.unbound`（`src/service/mod.rs:6921-6933`）、`receive.clockskew`（`:6463` 一带）、
`engine.storagePull`（`src/service/mod.rs:1824-1834`）。

池化后建议增加一条**廉价的保险丝**（connector 侧即可，不动引擎协议）：

> 任一来自槽 S 的通知，其 `params.account` 必须等于 S 绑定账号；不等 → 记协议告警并丢弃。

这让「跨账号错投」从静默变成可观测。当前单进程下这条不变量不存在（所有账号共用一个 stdout），
所以这是一条**新的、池化才获得的**正确性检查。

### 5.5 超时 / 取消 / 背压

连接器已有完整语义，池化后**按槽生效**，不需要新机制：

| 机制 | 位置 | 现状语义 | 池化后的变化 |
|---|---|---|---|
| 请求超时 | `kt-signal-connector/src/engine.rs:2777-2812` | 默认 15 s（`engine.rs:158-159`）；只读超时 → `Timeout`，变更超时 → `UnknownOutcome` | 不变；但**只影响该账号**（今天一个慢账号能占满全局命令队列） |
| 取消 | `kt-signal-connector/src/engine.rs:2796-2803` | 超时后向 actor 发 `Cancel` 丢弃等待；引擎侧操作不可取消 | 不变 |
| 背压 | `kt-signal-connector/src/engine.rs:28`（队列 128）、`:2891`（`try_send` Full → `Backpressure`） | 一次拒绝 | 不变；队列不再被别的账号挤占 |
| 接收侧背压 | `kt-signal-connector/src/engine.rs:2205-2243` | 每账号预算（按 `MAX_ACCOUNTS_PER_ENGINE+1` 建桶） | 每引擎 1 账号，桶退化为单桶，语义简化 |
| 引擎退避 | 连接器 `watchdog_liveness_backoff_base` 默认 10 s，cap 3 min（`supervisor.rs:94-104`） | 每槽 | 不变，天然按账号 |

**结论：超时/取消/背压不需要为进程池新增任何语义；收益是它们从「全局共享」变成「按账号隔离」。**

---

## 6. 生命周期

### 6.1 启动

~~~text
connector 启动
 ① 打开共享 store，列出账号（含迁移态/休眠态）
 ② 计算驻留集：resident = 按 lastActiveAt 排序取前 K（K 见 8.5）
 ③ 逐槽获取占用锁（datalock.rs:99-108；失败 → 该槽记 occupied，不炸整个 connector）
 ④ 起引擎：并发 2 + 每槽间隔 200 ms（避免 N 个进程同时拨 WS / 拉 prekey）
 ⑤ 每槽 spawn_watchdog / 媒体治理 / outbox 等既有后台任务（registry.rs:2127-2131 的形状）
 ⑥ 立即返回 runtime.status；**不阻塞等接收就绪**
 ⑦ 接收就绪另行判定：receive.emptied 或首次 liveness ping 成功（见 7.4）
~~~

耗时口径：

- 进程冷启动到首个 RPC 应答：**10.6 ms（1 个）/ 25.0 ms（10 个并发）**（第 8 节实测）。
- 真正的成本是建流：单账号 receive 就绪 **3.1–3.4 s**（`docs/receive-link-incident-2026-10-21.md:25-30`），
  且伴随 WS 拨号 + device capabilities PUT + prekey 拉取。
- 因此 ④ 的错峰主要保护的是**服务端与网络**，不是 CPU；最后启动的账号的「实时可达」时间 ≈ (N/并发) × 3.5 s。

### 6.2 停止与重链

顺序（每一步都在现有代码里有对位）：

~~~text
stop(slot)
 1. 连接器摘除该槽的在途链路会话（supervisor.rs:2056-2062 的 clear_link）
 2. EngineHandle::shutdown()（engine.rs:2902-2929）：
      actor 收 Shutdown → 关 stdin → 等子进程退出；超时 kill
 3. 引擎侧：stdin EOF → Service::shutdown_all()（src/main.rs:462）
      → 停全部收流（每账号 select 停旗不打断已开始的信封臂）
      → 等管线收口（PIPELINE_SHUTDOWN_GRACE = 10 s，src/service/mod.rs:2058）
      → Manager drop → SQLite 收尾 → exit 0
 4. 槽状态置 Stopped；data-dir 保留（冷账号）或删除（账号删除）
~~~

**已发现的宽限不一致（必须在池化前修）**：连接器给引擎的关停预算是
`shutdown_grace(3 s) + SHUTDOWN_TIMEOUT_MARGIN(2 s) = 5 s`（`kt-signal-connector/src/engine.rs:158-159`、
`:74`、`:2746`），而引擎内部的管线收口宽限是 **10 s**（`src/service/mod.rs:2058`）。
今天多账号时 `join_all` 并行等，所以最多也就是 10 s；池化后每个引擎都可能跑满 10 s，
连接器会在 5 s 时 SIGKILL，**把优雅收口截断成强杀**。建议：per-account 引擎的 `shutdown_grace` 提到
≥ 12 s（连接器侧改常量/配置即可），或者在引擎侧把宽限做成可配置。

**另一个已发现的放大点**：`ProxyGroupRuntime::shutdown` 是**串行**遍历槽（`registry.rs:418-433`），
每槽最多 5 s。N=10 时进程退出最坏 **50 s**。池化必须改成并行 + 全局预算（例如 8 s）。

### 6.3 崩溃与重启

~~~text
子进程退出 / 管道关闭
 → actor 观测到 EOF → EngineState::Exited/Faulted（engine.rs:2761-2766）
 → EngineEvent::StateChanged → registry.on_group_engine_event（registry.rs:259-271）
 → supervisor.watchdog_loop 收到 → 按 per-slot 退避重启（supervisor.rs:2512-2830、:2968-3020）
 → 只重启这一槽；其他槽不受影响
~~~

池化后的关键差异：

- **重启半径 = 1 个账号**（今天 = 整进程 N 个账号，见 `docs/receive-link-incident-2026-10-21.md:60-70`）。
- 重启预算按槽独立（`WATCHDOG_MAX_PING_FAILURES = 2`，`supervisor.rs:84`），但建议加一个
  **全池重启预算**（例如 5 分钟内总重启次数），防止 10 个账号同时进入退避重启的抖动。
- `restart_engine`（`supervisor.rs:2477-2512`）已经会自动重放 contacts resync / unknown-outcome reconcile /
  configuration gate / outbox drain，池化后这些重放变成「按账号」——语义更精确。

### 6.4 进程退出

~~~text
connector 收到退出信号
 ① 停止接受新 host 请求
 ② 并行 shutdown 全部槽（全局预算 8 s）
 ③ 每槽：优雅关停（6.2）→ 超时则 SIGKILL（engine.rs:2916-2928；子进程 kill_on_drop，engine.rs:2612）
 ④ 释放占用锁（drop DataDirLock）
 ⑤ 退出
~~~

引擎侧不需要为池化改退出语义：stdin EOF 是唯一干净退出路径（`src/main.rs:466-471`）。

### 6.5 占用锁与孤儿进程

- 每个 data-dir 一把 flock（`kt-signal-connector/src/datalock.rs:9-27`），进程退出（含 SIGKILL）由内核释放，
  无「陈旧锁」问题。动态槽必须在**起引擎前**取锁；取不到 → 该账号标 `occupied`，不影响其他槽。
- **孤儿引擎会自杀**：连接器死掉 → 引擎 stdin 管道关闭 → 引擎读到 EOF → `shutdown_all` → exit 0
  （`src/main.rs:414-439` 的读线程 + `:450-471`）。所以不需要额外的父进程守护；
  `kill_on_drop(true)`（`kt-signal-connector/src/engine.rs:2612`）只是兜底。
- 注意：这条自杀链依赖「父进程死亡一定关闭管道」。若未来连接器改成把 stdin 交给别的进程/线程池，
  要重新论证。

### 6.6 关键超时总表

| 项 | 值 | 位置 |
|---|---|---|
| 请求超时（默认） | 15 s | `kt-signal-connector/src/engine.rs:158-159`、`:2777-2812` |
| 连接器关停预算 | 5 s（3+2） | `kt-signal-connector/src/engine.rs:158-159`、`:74`、`:2746` |
| 引擎管线收口宽限 | 10 s | `src/service/mod.rs:2058` |
| 引擎 flush 栅栏 | 30 s | `src/service/mod.rs:2139-2156` |
| 接收侧维护调用上限 | 30 s | `src/service/mod.rs:7129-7132` |
| 建流阶段上限 | 90 s | `src/service/mod.rs:7134-7138` |
| 重连退避 | 1→55 s + 0..5 s jitter | `src/service/mod.rs:6996-7067` |
| watchdog 拍 | 30 s | `kt-signal-connector/src/engine.rs:162` |
| liveness 退避 | 10 s 起，cap 3 min | `kt-signal-connector/src/supervisor.rs:94-104` |

---

## 7. 健康上报（connector → desktop → UI）

### 7.1 现有面

| 面 | 形状 | 位置 |
|---|---|---|
| `runtime.status` | 聚合 + `proxyGroups[]`（每项 groupId/state/pid/rssBytes/resourcePressure/accountCount） | `registry.rs:119-139`、`:308-350` |
| `runtime.stateChanged` | 聚合状态 | `host.rs:1141-1146` |
| `proxyGroup.stateChanged` | 单组状态 | `host.rs:1147-1154`、schema 要求 groupId/state/resourcePressure |
| `runtime.resourcePressure` | state/pid/rssBytes/groupId | `host.rs:1155-1166` |
| doctor | 每槽探测 version/protocolCapabilities/perfMetrics | `kt-signal-connector/src/doctor.rs:386-500` |

schema 是**严格**的（`additionalProperties: false`，且 `proxyGroup.stateChanged` 的 data 必须含
`groupId/state/resourcePressure`，见 `kt-signal-connector/schemas/connector-api-v1.schema.json` 的
`allOf[event=proxyGroup.stateChanged]` 分支）。这意味着「加字段」不是纯前端改动，必须走 connector 契约版本。

### 7.2 已发现的聚合缺陷（池化必须一起改）

`aggregate_from` 的规则是「任一 Running → Running；否则任一 Faulted → Faulted；…」
（`registry.rs:327-350`）。池化后这意味着：**9 个账号正常、1 个账号引擎崩溃时，聚合仍显示 Running**，
单账号故障在顶层完全不可见。建议：

- 顶层聚合新增 `degraded`（部分引擎非 Running），或
- 顶层保持兼容（Running/…），但把「逐账号健康」作为 UI 的第一等公民（desktop 必须读 `engines[]`）。

### 7.3 建议的 additive 变更（connector → desktop）

| 变更 | 形状 | 兼容性 |
|---|---|---|
| `runtime.status` 增加 `engines[]` | `{accountId, state, pid?, rssBytes?, resourcePressure, receiveState, restarts, lastError?}` | additive 字段；旧 desktop 忽略 |
| 新事件 `engine.stateChanged` | `{accountId, state, resourcePressure}` | 新 event 值；旧 desktop 未订阅则无影响（需确认 desktop 对未知 event 的处理是忽略而非报错） |
| 新事件 `engine.receiveState`（可选） | `{accountId, state: connecting\|ready\|unbound\|dead}` | 若 desktop 需要「已连接/收不到」的区分 |
| 保留 `proxyGroups[]` | 迁移期 legacy 槽仍用它 | 混合模式必需 |

过渡方案（若契约排期紧）：把 `groupId` 填成 `acct:<accountId>` 复用 `proxyGroup.stateChanged`，
**零 schema 变更**，但语义上把账号伪装成代理组，只建议作为灰度期 1–2 个版本的手段。

### 7.4 账号级「接收就绪」判据

不需要新 wire 方法，用现有信号即可：

1. 引擎进程 Running（`engine.rs:2761-2766`）；
2. 收到 `receive.emptied`（服务器队列排空，`src/service/mod.rs:6303-6312`）或首次 liveness ping 成功
   （`watchdog_ping_plan`，`supervisor.rs:2845` 起）；
3. 若先收到 `receive.unbound`（401/403，`src/service/mod.rs:6921-6933`）→ `unbound`，不重连，等重链
   （与引擎语义一致，`src/service/mod.rs:6919-6935`）。

### 7.5 是否要引擎侧上报 per-account 健康状态？

**结论：不需要。** 理由：

- 进程与账号 1:1 之后，「账号健康」的外延就是「进程健康 + 接收态」。前者连接器已有（pid/exit code/信号），
  后者已有现成事件（7.4）。
- 唯一缺的是「引擎重启后，接收管线是否已重新就绪」——这同样由 `receive.emptied` / liveness ping 覆盖，
  不必新增方法。
- 新增 wire 方法要付 ADR 0002 的契约成本（命名空间、compat 表、fixture、connector 白名单），
  收益却只是省一次进程级判断。

**唯一建议的引擎侧加固（可选，S5）**：单账号模式下校验「通知里的 account 必须等于本进程唯一账号」，
不等则落一条结构化 warn 并丢弃——把跨账号错投变成可观测事件（见 5.4）。

---

## 8. 内存与容量

### 8.1 实测数据与口径

测量对象：`target/release/kt-signal-engine`（2026-10-21T03:19 构建，本仓 fix/admin-write 工作区），
macOS 27.0 / arm64 / 18 核。方法见附录 A。

| 口径 | 数值 | 说明 |
|---|---|---|
| 空引擎 RSS（0 账号、real 模式、闲 60 s） | **9.6 MB** | 0/5/15/30/60 s 五次采样不变 |
| 空引擎私有 footprint | **3.6 MB** | `vmmap/footprint`：Dirty 3.6 MB + Clean 2.5 MB |
| 空引擎线程数 | **20–21** | `#[tokio::main]` 多线程 runtime 按核数开 worker（`src/main.rs:373`） |
| 空引擎空闲 CPU | **0.00 s / 10 s** | 1 个与 10 个引擎都是 0（无轮询空转） |
| 冷启动到首个 RPC 应答 | **10.6 ms / 25.0 ms** | 1 个 / 10 个并发 |
| 10 个空引擎总 RSS | **95.8 MB** | 每进程 9.5–9.6 MB |
| 单账号 live 引擎 RSS | **64.3 MB** | 用户实测（线上单账号引擎） |
| connector RSS | **23.3 MB** | 用户实测 |
| 同进程 5 个 fake 账号的增量 | **+0.4 MB** | 仅注册表/路由；fake 账号无 store、无收流 |

两条读法：

1. **进程底座很薄**（3.6 MB 私有），池化的固定成本低；
2. **账号归属内存很厚**（64.3 − 9.6 ≈ 54.7 MB 上界），而它由 store（SQLite 页缓存）、WS/TLS 缓冲、
   prekey/会话/群凭据缓存构成——**这些在两种模型里都必须存在**，所以池化并不会让内存曲线变陡。

### 8.2 总内存估算（RSS）

| N | 引擎池 | 单进程多账号（上界） | 差值 | 差值（物理口径） | 池 + connector | 线程 + connector |
|---|---|---|---|---|---|---|
| 1 | 64.3 | 64.3 | 0 | 0 | **87.6** | 87.6 |
| 3 | 192.9 | 173.7 | +19.2 | +7.2 | **216.2** | 197.0 |
| 5 | 321.5 | 283.1 | +38.4 | +14.4 | **344.8** | 306.4 |
| 10 | 643.0 | 556.6 | +86.4 | +32.4 | **666.3** | 579.9 |

（单位 MB。「单进程多账号」= 9.6 + N×54.7，是上界；「差值物理」= (N−1)×3.6。）

**给产品的一句话**：5 个常驻账号约 **345 MB**，10 个约 **666 MB**。如果目标机型内存 8 GB，
默认驻留 5 个是合理的，10 个需要显式确认。

### 8.3 空闲内存能否收敛？

先说结论：**能压一部分，压不到常数；线性增长是账号状态的固有属性。**

可收敛的部分（都是机制已知的）：

| 手段 | 作用点 | 预期 | 代价 |
|---|---|---|---|
| 下调 SQLite 页缓存 | presage store 打开时固定 `journal_mode=Wal / synchronous=Full`（`vendor/presage/presage-store-sqlite/src/lib.rs:67-71`），未设 `cache_size`（SQLite 默认 −2000，即每连接 2 MB） | 每连接 2 MB → 1 MB | 读放大，需实测 |
| 下调 sqlx 连接池上限 | sqlx 默认 `max_connections = 10`（sqlx-core 0.9 `pool/options.rs:151`），引擎未覆盖 | 最坏 20 MB → 2–4 MB/库 | 并发查询排队（单账号场景影响很小） |
| 减少「每个操作新开 store」 | `open_store` 在 `account_aci`、`list_accounts`、证书维护等处各自开库（`src/service/mod.rs:12277-12315`、`:7103-7127`） | 少开 1–2 个 pool | 需要改造调用面 |
| `current_thread` runtime | `src/main.rs:373` | 线程 20–21 → 预计 4–7 | 长任务与 LocalSet 抢同一线程（可把证书维护改 `spawn_local`） |
| 空闲期主动收缩缓存 | 群凭据/白名单/能力位等 `HashMap` 缓存 | 小 | 冷启动重取 |

**libsignal 状态本身不是内存大头。** 会话（session）、prekey、身份钥、sender key、群态在引擎里都落在
SQLite 的 kv/结构化表里（presage store，`vendor/presage/presage-store-sqlite/src/lib.rs`），随库增长而不随驻留时间增长；
进程内常驻的只是少量派生缓存（群凭据 `group_credentials`、白名单、能力位、会话态观测簿，
`src/service/mod.rs:2023-2052`），量级是每账号几百 KB 到几 MB。真正的大头是 SQLite 页缓存与 WS/TLS 缓冲。

**「缓存」不是「泄漏」的证据**：空引擎 60 s 五次采样恒 9.6 MB；同进程加 5 个 fake 账号只 +0.4 MB
（附录 A）。64.3 MB 的 live 引擎是稳态常驻，不是随运行时间无界增长——所以调优能降一个台阶，但降不到常数。

不可收敛的部分：每个账号必须有的 store、会话/prekey、sender key、接收 WS 缓冲、TLS 连接状态。
**所以「内存随账号线性增长」是产品事实，不能靠调优消灭，只能靠容量策略约束。**

### 8.4 按需冷启动可行吗？

**能跑通，但产品上只能作为显式状态。**

- 引擎没有「暂停接收」：`--receive-mode` 被接受后丢弃（`src/main.rs:88-90`），
  `spawn_receive_pipelines` 对所有已登记账号无条件启动（`src/service/mod.rs:2172-2176`）。
  所以「引擎在跑但不收消息」做不到；要么进程在、收流在，要么进程不在。
- 不驻留的账号并非丢消息：Signal 服务器保留队列，下次建流时重放——引擎自己就把这条当成断线恢复的
  唯一兜底（`src/service/mod.rs:6936-6949` 的 StdioDead 分支明确写「信封留服务器队列，重启重放」）。
- 代价：① 没有实时性（发消息的人看不到「已送达」的即时反馈）；② 冷启动到能收 ≈ 3–4 s，若积压大还要加排空时间
  （`docs/jsonrpc-compat.md:6304-6307` 记录过一次 2 万封积压按 ~400 封/s 清空）；
  ③ 每个冷启动都是一次 WS 拨号 + prekey 检查，唤醒频繁时成本高于常驻；④ 产品需要向用户解释「休眠」。

结论：

| 策略 | 适用 | 取舍 |
|---|---|---|
| 全常驻 | 账号数 ≤ 驻留上限 K | 体验最好，内存 = N×64.3 |
| 常驻 + 冷降级 | 账号数 > K | 超过 K 的账号显式「休眠」，打开会话时唤醒；内存有界 |
| 全冷 | 不推荐 | 实时消息不可用，与 IM 产品语义冲突 |

**冷启动的唤醒预算**：进程 25 ms + 建流 3–4 s + 队列排空；建议 UI 在 5 s 内给出「正在唤醒」而不是报错。

### 8.5 上限与降级策略

| 参数 | 建议默认 | 依据 |
|---|---|---|
| `MAX_RESIDENT_ENGINES` | **5** | 5×64.3 ≈ 322 MB + connector 23 MB ≈ 345 MB，8 GB 机型的安全区 |
| 硬上限（拒绝/强提示） | **8** | 与既有 `MAX_ACCOUNTS_PER_ENGINE = 8`、`MAX_PROXY_GROUPS = 8` 对齐（`groups.rs:22`、`:32`）；8×64.3 ≈ 514 MB |
| 池级内存预算 | 合计 RSS 1.2 GB 或系统可用内存 25% | 触发 LRU 冷降级 |
| 冷降级触发 | 最久未活跃 > 30 min 且当前驻留 ≥ K | 不能驱逐有在途变更请求的账号 |
| 唤醒 | 首次进入会话 / 发送 / 收到 push（无 push 通道，只能由 UI 触发） | 见 8.4 |

**超上限时不要静默失败**：新链接的账号应当允许创建但标为「休眠（未驻留）」，
桌面在账号列表与设置页明确展示，并提供「提升为常驻（会占用约 65 MB 内存）」。

---

## 9. 迁移步骤（每步可独立交付）

> 仓边界：S1–S4 是 **kt-signal-connector** 的改动（按该仓流程立项/评审）；S5 是本仓改动。
> 本仓在本设计阶段只交付本文，不动代码。

### S1｜动态账号槽（开关灰度，只影响新链接）

- **目标**：池的骨架先落地；存量账号行为零变化（混合模式）。
- **改动面（connector）**：
  - 槽注册表从「启动计划 Vec」扩为「静态计划 + 动态 AccountEngine 注册表」；
  - 新链接走 5.3 的预分配 accountId + `<signal-data>/engines/<accountId>`；
  - 路由 `slot_for_account` 先查动态表、回落静态表（`registry.rs:536-555`）；
  - 开关：launcher 参数（默认关）。
- **验收**：
  1. 开关关：connector 行为与今天逐字节一致（现有集成测试全绿）。
  2. 开关开：新链接账号产生独立 data-dir、独立 pid；`accounts.list` 含它且 `proxyGroup` 仍正确。
  3. 该账号 send/receive/receipt/typing 全通；legacy 引擎的账号不受影响（同时收发验证）。
  4. `kill -9` 新引擎：只有该账号的请求失败并进入重启，legacy 账号收发不中断。
  5. 同一账号不会出现两个引擎（检查 4409 日志为零）。
- **回滚**：开关关 + 删除新账号的 data-dir（该账号是新建的，回滚即放弃）。

### S2｜存量账号 data-dir 拆分（可延后，可长期不做）

- **目标**：把 legacy 多账号 data-dir 拆成一账号一目录。
- **流程（每个账号一步，可重入）**：
  1. 进入维护：停 legacy 引擎（保证没有进程持有 store）；
  2. 迁移日志（journal）落 `<signal-data>/migration/<runId>.jsonl`；
  3. 为该账号创建 `engines/<accountId>/`，写**单条** `engine-state.json`；
  4. 用 `rename`（同卷）把 `accounts/lk-<hex>` 移入新目录；
  5. 从 legacy `engine-state.json` 删除该条（保持 legacy 注册表与磁盘一致，剩余账号仍可服务）；
  6. 起该账号引擎并验证 `listAccounts == 1` + `receive.emptied`；
  7. 下一个账号。
- **验收**：
  1. 迁移前后 `accounts.list` 条数一致、每账号 `getUserStatus` 成功；
  2. 每账号收到一条真实消息（队列重放即可），且旧队列无重复投影；
  3. 在任一步骤强杀进程，重启后从 journal 续跑，**不触发引擎 quarantine**（检查 `accounts/_quarantine` 为空）；
  4. 全量迁移后 legacy 目录只剩空壳，删除它不影响任何账号；
  5. 回滚：保留迁移前的 legacy 目录副本，开关关 + 恢复副本即回到 S1 前状态。
- **风险**：这是全设计里唯一「不可逆 + 触碰凭证」的步骤；建议先做 dry-run 模式（只校验不移动）。
- **可以不做**：若产品接受「老账号继续住在 legacy 组引擎、新账号独立」，S2 可无限期延后（见第 13 节问题 3）。

### S3｜每账号生命周期 + 健康面 + 契约

- **目标**：S1 的槽真正成为「一等公民」。
- **改动面**：
  - 动态槽的 start/stop/restart、per-slot watchdog、占用锁（6.1/6.2/6.5）；
  - 并行关停 + 全局预算（修 `registry.rs:418-433` 的串行放大）；
  - `list_accounts` 并行化（`registry.rs:436-444` 现在是串行，N 个引擎 × 每槽最多 2 s 的
    能力位网络读，最坏 O(N×2 s)）；
  - 聚合状态修 `degraded`（`registry.rs:327-350`）；
  - `runtime.status` 加 `engines[]` + 新事件（7.3）+ schema/契约版本；
  - desktop：账号级状态 UI（运行/唤醒中/休眠/崩溃/未绑定/占用）。
- **验收**：单账号崩溃在 UI 上 1 s 内可见且不影响其他账号；`runtime.status` 的 `engines[]`
  与 store 中账号集合一致；doctor 每槽可独立探测；旧 desktop 对未知字段/事件不崩（向前兼容验证）。
- **回滚**：事件/字段是 additive；desktop 回旧版即只剩聚合面。

### S4｜容量策略（驻留上限 / 冷降级 / 池级预算）

- **目标**：内存有界，超限有明确产品语义。
- **改动面**：`MAX_RESIDENT_ENGINES` + LRU + 唤醒 + 池级 RSS 预算 + 设置页提示（8.5）。
- **验收**：
  1. K+1 个账号时第 K+1 个自动休眠，UI 有状态；首次打开会话 5 s 内唤醒并收到消息；
  2. 驱逐永不在有在途变更请求时发生（用一个 15 s 的 send 验证）；
  3. 10 账号常驻时总 RSS 与表 8-2 偏差 < 15%；
  4. 池级预算触发后总 RSS 回落到预算内。

### S5｜引擎侧瘦身与加固（本仓，独立可交付）

- **目标**：降低每进程固定成本 + 修关停宽限 + 可选单账号护栏。
- **改动面**：
  1. `#[tokio::main(flavor = "current_thread")]`（`src/main.rs:373`）：线程 20–21 → 预计 4–7；
     需复核 `tokio::spawn` 两处（`src/main.rs:408`、`src/service/mod.rs:6825`）；
  2. SQLite 页缓存上限：`open_store` 传 `SqliteConnectOptions::pragma("cache_size", "-1024")`，
     走 `SqliteStore::open_with_options`（`vendor/presage/presage-store-sqlite/src/lib.rs:65` 是 pub API）；
  3. 关停宽限对齐：要么引擎宽限可配，要么连接器侧把 grace 提到 ≥12 s（二选一，见 6.2）；
  4. 可选护栏：`KT_ENGINE_SINGLE_ACCOUNT=1` 时，注册表 >1 账号则启动失败并落结构化错误——
     防止误把多账号 data-dir 指给池槽（与 I3 对齐）；
  5. 可选：通知 account 一致性校验（5.4/7.5）。
- **验收**：线程数 ≤ 8、空闲 RSS 下降 ≥ 10 MB；12 s 关停预算下无 SIGKILL（日志无
  `engine shutdown unanswered; killing`）；引擎全部门禁绿（fmt/clippy/test/contract）；
  **契约面无变化**（compat §1/§3/§5 不动，除附录 B 的陈述性修订）。
- **回滚**：逐项独立，可单项回退（都是编译期/启动参数级改动）。
- **落地记录（2026-10-21，fix/admin-write）**：四项均已实现——`current_thread`
  runtime、`cache_size=-1024`、`KT_ENGINE_SHUTDOWN_GRACE_MS`（默认 10000，
  `1..=300000`）、`KT_ENGINE_SINGLE_ACCOUNT=1` 启动护栏；`state.rs` 与清扫
  逻辑未改。同一附录 A 方法复测：空引擎 **9.02–9.03 MB / 3 线程**（改前
  9.42–9.44 MB / 20–21 线程）；单账号 store 离线复现 **23.2–23.8 MB /
  2–5 线程**（改前 24.0–24.6 MB / 20–23 线程）——该复现只开 1–2 个
  SQLite 连接、不含 live WS/TLS 缓冲，不能直接代表 64.3 MB 线上口径。
  **live 实测（2026-10-21 10:33–10:37，同一 dev 账号/data-dir，只换引擎
  二进制并让 connector 正常重启）**：改前 **33.9 MB / 27 线程 / 11 个
  store.db fd** → 改后 **32.5 MB / 8 线程 / 12 个 fd**（空闲 3.5 min；
  2 min 时 32.3 MB/8 线程/10 fd）。线程目标达标，但 **RSS 仅降约
  1.4–1.7 MB，未达 ≥10 MB**：本机 dev 实例的页缓存实际占用远低于
  2 MiB/连接上限，不能按「连接数 × 1 MiB」推算；64.3 MB 线上口径与更大
  store 的实例仍需单独复测，S5 的 RSS 验收在本机未达成。关停面：
  `KT_ENGINE_SHUTDOWN_GRACE_MS=12000` 下带 1 条接收管线的引擎在
  0–10.0 s 内退出（最坏为宽限耗尽后继续收口），日志保留
  `stage="engine_shutdown" grace_ms=12000 elapsed_ms=…`，无 SIGKILL。

---

## 10. 失败模式表

| # | 场景 | 触发 | 后果 | 防护 | 验收 |
|---|---|---|---|---|---|
| F1 | 数据目录拆分半途失败 | S2 断电/强杀 | 某账号「注册表指向已移走的 store」→ 引擎 fail closed 拒启动；或反之 store 无注册表 → 被隔离 | journal + 幂等续跑；每步先建新注册表再 rename；全程不删；dry-run | 注入 5 个断点各强杀一次，续跑后 0 quarantine、0 账号丢失 |
| F2 | 同一账号被两个引擎持有 | 复制而非移动 data-dir；孤儿进程；混合模式误路由 | 服务器 4409，双端抢连接，消息错位 | I1/I2；datalock 每目录 flock（`datalock.rs:99-108`）；迁移只 rename 不 copy；孤儿靠 stdin EOF 自杀（6.5） | 并发启动两个指向同目录的连接器，第二个必须启动失败 |
| F3 | 引擎崩溃时在途请求 | 进程被杀 | 该账号在途请求全部 `Exited`；变更类可能未知结局 | 既有语义（`engine.rs:2796-2807`）；池化后失败范围 = 1 账号 | 请求中途 kill -9：只有该账号的错误应答，其他账号 0 影响 |
| F4 | 重链竞态 | stop 与 start 交叉；旧引擎未退新引擎已起 | 4409 / 注册表双写 | 槽级串行（stop → 确认退出 → start）；datalock 先释放再取 | 连续 20 次重链，0 次 4409、0 次双进程 |
| F5 | 迁移期路由指向旧槽 | S2 过程中请求落到 legacy | 请求失败（账号已不在 legacy）或写旧库 | 迁移期对该账号路由加写锁/世代号；请求在迁移窗口内排队而非失败 | 迁移中持续打 send/receive，成功率高且无错库写入 |
| F6 | 关停被强杀截断 | 连接器 5 s < 引擎 10 s | SQLite 未收尾、在飞信封未 ack → 服务器重放（消息不丢但重复风险） | 6.2：grace ≥ 12 s 或引擎可配 | 慢盘/大积压下退出日志无 kill 行，重启后无重复投影 |
| F7 | 全池关停放大 | 串行 `shutdown`（`registry.rs:418-433`） | N=10 时退出最坏 50 s | 并行 + 8 s 全局预算 | 10 引擎退出 ≤ 8 s |
| F8 | 启动风暴 | N 引擎同时拨 WS + prekey + capabilities | 服务端限流 / 本机 CPU 峰值 / 首连失败 | 6.1 的并发 2 + 200 ms 错峰；失败按槽退避 | 10 账号冷启动：0 次 429/4409，全部在 60 s 内就绪 |
| F9 | `listAccounts` 串行放大 | `registry.rs:436-444` | host 列表延迟 O(N×2 s) | 并行 + 全局预算，超时降级为「仅本地行」 | 10 账号列表 P95 < 3 s |
| F10 | 状态事件热路径放大 | 每次 `stateChanged` 都 `aggregate_status` → 每槽一次 store 计数（`registry.rs:259-271`、`:308-350`） | 重启风暴时 store 打满 | 聚合改为事件驱动缓存/限频；account_count 用内存计数 | 10 引擎连续重启，store QPS 不升 |
| F11 | 冷账号被误判掉线 | UI/看门狗把「未驻留」当 Faulted | 误告警、误导用户 | 独立的 `cold/sleeping` 状态，不参与 watchdog ping | UI 状态与真实进程表一致 |
| F12 | 重启后账号指标归零 | `perfMetrics` 是进程内（`account_metrics.rs:1-40`） | 崩溃计数/延迟历史丢失 | 连接器按账号累加并持久化 | 重启 3 次后累计崩溃数正确 |
| F13 | 账号无关方法落到坏引擎 | 随机选引擎 | 匿名浏览随某账号崩溃抖动 | 固定共享匿名槽（3.5） | kill 任一账号引擎，贴纸包浏览不受影响 |

---

## 11. 与现有 watchdog / 重启 / connector 的关系

1. **watchdog 天然变成 per-account。** 现在每槽一个 `watchdog_loop`（`supervisor.rs:2512`），
   池化后一槽 = 一账号，重启半径自动收敛。**不需要重写 watchdog**。
2. **liveness 探针简化。** 今天 `watchdog_ping_plan` 要在组内挑候选账号
   （`supervisor.rs:2845`、`WATCHDOG_PING_ATTEMPT_LIMIT = MAX_ACCOUNTS_PER_ENGINE`），
   还要处理「所有候选都被引擎拒绝」的分支（`supervisor.rs:90-104`）。一账号一引擎后只剩 1 个候选，
   `docs/receive-link-incident-2026-10-21.md` 里那类「旧账号行导致整机重启」在结构上不可能再发生。
3. **重启请求必须按槽。** `watchdog_request_restart` 已有 per-slot 的 episode/backoff
   （`supervisor.rs:2968-3020`），池化后保留；新增**全池重启预算**防止 N 槽同时抖动。
4. **停止/启动是池级操作。** `runtime.start/stop/shutdown` 遍历槽（`registry.rs:377-433`）：
   池化后必须并行 + 全局预算（F7），且「停一个账号」不应走池级 API（已有按账号的 delete 路径，
   `registry.rs:559`）。
5. **doctor 面放大。** doctor 逐槽探测（`doctor.rs:367-500`）。N=10 时探测次数 ×10，
   要加每槽超时与总预算，否则 doctor 本身会把 UI 拖住。
6. **引擎侧是否需要主动上报？** 见 7.5：不需要。进程退出码 + `receive.*` 事件 + `perfMetrics` 已经足够；
   新增 wire 方法只会增加契约面。
7. **与 ADR 0001 的关系**：每账号一个 AGPL 进程仍然是「独立进程 + IPC 聚合」，许可结论不变；
   反而每个进程只承载一个账号，聚合边界更清晰。
8. **与 ADR 0002 的关系**：IPC 形状不变（每个引擎仍是同一 JSON-RPC 子集）；变化只在 connector 内部
   的槽数量与 host 契约的 additive 字段。

---

## 12. 风险与成本

### 12.1 内存

- 固定成本：每账号约 3.6 MB 私有 / 9.6 MB RSS（实测），10 账号 32/86 MB。
- 主要成本仍是账号状态本身（~54 MB/账号），**两种模型都躲不掉**。
- 若不做 S5 的缓存收敛，10 账号约 666 MB；做完预计 550–620 MB（需实测）。

### 12.2 启动总耗时

- 进程冷启动极快（10–25 ms）；**真正的耗时在建流**（3.1–3.4 s/账号）。
- 并发 2 + 200 ms 错峰：10 账号全部「可收」约 18–20 s（最后一槽 5×3.5 s ≈ 17.5 s）。
  若要更快，只能提高并发（换取服务端限流风险）。
- 用户感知的「打开应用就能收消息」会从「一个引擎 ~3.5 s」变成「依赖该账号所在批次」，需要 UI 用
  「连接中」而不是「离线」来表达。

### 12.3 connector 复杂度

- **已有资产**：121 处按账号路由、per-slot supervisor/watchdog/status/lock。
- **新增**：动态槽注册表、预分配 accountId 的链接流程、data-dir 拆分迁移器、并行 start/stop、
  聚合 degraded、`engines[]` 契约与 schema、desktop UI、容量策略。
- **大致改动面**：`registry.rs`（槽表/路由/聚合）、`supervisor.rs`（基本不动）、`engine.rs`（基本不动）、
  `groups.rs`（粒度语义）、`datalock.rs`（动态取锁）、`host.rs`（新事件）、`store.rs`（绑定列/迁移）、
  `schemas/connector-api-v1.schema.json`、desktop（独立仓）。
- **不建议**在这一批里顺手改 proxy group 的语义（它仍是代理策略单位），否则会把两个变更耦合。

### 12.4 测试成本

- 引擎的 **IPC 契约测试不受影响**：`tests/contract.rs` 起真实进程、按 data-dir 驱动
  （`tests/contract.rs:27-69`），池化后每个进程仍是同一契约；现有 90 个测试原样可用。
- connector 侧需要新增：多引擎测试夹具（N 个 fake 引擎）、崩溃注入（kill -9 单槽）、
  迁移器断点续跑、容量策略、契约 schema 兼容。
- 端到端：至少覆盖「2 账号 + 杀其中一个引擎 + 另一个继续收发」；「10 账号冷启动 + 总内存测量」；
  「迁移中途强杀 + 续跑」。

### 12.5 日志与排障

- **好处**：每账号一份 `logs/engine-stderr.log`（`src/logfile.rs:60`、`src/main.rs:384`），
  排障不再需要在多账号日志里筛账号哈希。
- **成本**：N 个 pid、N 份日志、N 个日志轮转；需要 connector 提供「池级诊断包」
  （把 N 份日志 + 状态快照 + doctor 汇总打成一个 zip）。现有 doctor（`doctor.rs`）可复用为骨架。
- 连接器每引擎一个 `engine-stdout-{pid}` 阻塞线程（`engine.rs:2650-2657`）；10 引擎 = 10 个线程，可接受，
  但要在池级预算里算进去（连接器线程数 = 10 × (stdout pump + actor + stderr + monitor)）。

### 12.6 平台与发布

- Windows：N 个子进程会被安全软件逐个扫描，首次启动可能变慢（需要实测）；
  安装包/签名不变（同一个 `kt-signal-engine.exe`）。
- 崩溃转储/错误上报需要按 pid 归集（今天是单 pid）。
- macOS/Linux 进程数限制远高于 10，不构成风险。

---

## 13. 待决策问题

1. **默认驻留数 K**：5（345 MB）还是 8（514 MB）？取决于目标机型的最低内存配置。
2. **超上限时**：允许创建并休眠，还是在链接入口直接拒绝（`MAX_ACCOUNTS_PER_ENGINE` 现有做法是拒绝）？
3. **混合模式是否长期保留**：legacy 组引擎继续承载老账号（省掉 S2），还是强制统一（要做迁移）？
4. **desktop 契约排期**：新事件 `engine.stateChanged` 与 desktop 版本谁先上（additive 也要确认旧版忽略行为）。
5. **池级内存预算**：用绝对阈值（1.2 GB）还是系统可用内存比例（25%）？
6. **冷账号的语义**：是否允许「休眠账号仍显示在会话列表并接收未读计数」——若允许，未读数从哪来
   （引擎不在就没有投影，只能靠服务器重放）。
7. **是否保留 anonymous 共享槽**：多一个进程（+3.6 MB）换匿名能力与账号解耦，是否值得。
8. **S5 的 `current_thread`**：是否接受「长任务与收流抢同一线程」的风险（证书维护可改 spawn_local 缓解）。

---

## 附录 A：实测方法与原始数据

环境：macOS 27.0.1 / arm64 / 18 核；二进制 `target/release/kt-signal-engine`
（2026-10-21T03:19:41 构建）；数据目录为新建临时目录；每个进程以
`--data-dir <tmp> jsonRpc --receive-mode on-start --ignore-attachments --ignore-stories --ignore-stickers` 启动，
通过 stdin 写一行 `version` 请求、stdout 读一行响应作为「就绪」。

采集方式：

- RSS：`ps -o rss= -p <pid>`；
- 私有 footprint：`vmmap -summary <pid>` / `footprint -p <pid>` 的 `Physical footprint`；
- 线程数：`ps -M -p <pid>` 行数 − 1；
- 空闲 CPU：`ps -o time= -p <pid>` 在 10 s 窗口的差值（1 与 10 进程均 0.00 s）；
- 空闲增长：0/5/15/30/60 s 五次采样 RSS 恒 9.6 MB。

原始数据：

| 场景 | 结果 |
|---|---|
| 1 进程 | first_ready 10.6 ms；RSS 9808 KB；21 线程 |
| 3 进程 | first_ready 13.5 ms；RSS 9776/9808/9808 KB；合计 28.7 MB；各 21 线程 |
| 5 进程 | first_ready 16.5 ms；合计 47.9 MB；各 21 线程 |
| 10 进程 | first_ready 25.0 ms；合计 95.8 MB；各 21 线程 |
| 空引擎（real，0 账号） | RSS 9.6 MB；footprint 3.6 MB；Dirty 3.6 MB / Clean 2.5 MB |
| fake 模式 5 个账号 | 9.5 MB → 9.9 MB（+0.4 MB，无 store/无收流） |

注意：上述数字来自 **WIP 工作区** 的 release 构建；S5 落地后需用同一方法复测并更新表 8-1/8-2。
「单账号 live 引擎 64.3 MB / connector 23.3 MB」是用户 2026-10-21 的线上实测值，本文按此口径做估算。

## 附录 B：需要同步修订的既有文档

| 文档 | 需要改的地方 |
|---|---|
| `docs/jsonrpc-compat.md:5210-5211` | 「引擎单线程 LocalSet 装配…无并行数据竞争面」——池化后是「每进程单账号单 LocalSet」，跨账号无共享 |
| `docs/jsonrpc-compat.md:6309-6313` | watchdog 段落的「单 LocalSet 饥饿」论述需注明池化后的新边界 |
| `docs/jsonrpc-compat.md:6757-6759` | panic=abort 的爆炸半径从「全账号」改为「单账号进程」 |
| `docs/p1-engine-skeleton.md:75-77` | 并发模型条目补「每账号一进程」的部署形态 |
| `src/service/supervision.rs:5-8`、`src/service/account_metrics.rs:5-11` | 注释里的「S1 未落地」前提失效（代码可后改，注释先记账） |
| `README.md` 阶段规划 | P5/P6 之后补「引擎池」演进条目 |
| `kt-signal-connector/schemas/connector-api-v1.schema.json` | `engines[]` / `engine.stateChanged`（S3） |
