# P1：引擎骨架——设计与验收

状态：P1a 动工基线 ｜ 依据：README 阶段规划、ADR 0001/0002/0003

## 范围切分（P1 = P1a + P1b）

- **P1a（本批）**：进程模型 + IPC 兼容子集 + link/delete 状态机 + 契约测试。
  服务面为**形状对齐桩**（`docs/jsonrpc-compat.md` 逐行注明），不含真实
  Signal 服务端流量。
- **P1b（随批）**：presage/libsignal-service 接入，`startLink/finishLink`
  换真实 provisioning，身份与会话建立在真实存储上落地；P1a 桩逐格替换，
  compat 表同步更新。

P1 门禁（README）：**契约测试对 fake 夹具全绿**——即本仓契约测试断言的
形状/错误码/事件投影与 `fake-signal-cli.py` 逐条一致（diff 见 compat 表）。

## 进程模型（ADR 0001 sidecar）

- 单二进制 `kt-signal-engine`，由 connector 以固定 argv 拉起
  （`--data-dir <dir> jsonRpc …`，见 compat §1），stdio 为唯一 IPC。
- 无网络监听、无固定端口、无线程池爆炸面：P1a 用 std 单线程读 stdin +
  有界 worker（链接后 receive 投影用独立线程，join 于退出）。
- 退出语义：stdin EOF = 0；argv 非法 = 2；内部致命 = 1。
- 零日志（stderr 静默），遵守不落正文/号码/URI/密钥的仓规；P2 引入
  分级日志时先过该红线。

## 模块划分

```
src/
  main.rs      argv 解析（严格模式：未知参数失败）+ 进程装配
  jsonrpc.rs   请求/响应/通知类型与分帧（-32700 容错、未知方法 -32601）
  state.rs     数据目录内 engine- 命名空间状态（link 生命周期/账号表/
               发送调度日志），全部写入受 --data-dir 约束
  methods.rs   白名单 dispatch（compat §3 逐方法），mutating 全部落调度日志
tests/
  contract.rs  起真实进程、tempdir 数据目录，逐方法断言形状/错误码/事件
```

依赖最小化：仅 `serde` + `serde_json`（均为 MIT/Apache-2.0）。不引
tokio/anyhow/thiserror——P1a 的 std 足够，P1b 接 presage 时随其依赖树
评审再放行。

## 验收标准（P1a 出口）

1. `cargo fmt --check`、`cargo clippy --all-targets -D warnings`、
   `cargo test` 全绿（仓规门禁）。
2. 契约测试覆盖 compat §3 全部方法行：result 形状、错误码与 message
   原文、finishLink 后的 receive 投影、deleteLocalAccountData 生命周期、
   send 系方法的调度日志断言（recipient 数组/单字符串/显式 stop 布尔等
   connector 侧依赖的参数形状）。
3. 协议健壮性：坏行 -32700 继续服务、未知方法 -32601、EOF 退出 0、
   JVM 环境变量存在不影响启动、media/群组忽略参数被接受。
4. 桩边界不撒谎：compat 表每个桩格在代码中集中于 `service` 注解点，
   P1b 替换时有机械可查的清单（`grep "P1a-stub"`）。
