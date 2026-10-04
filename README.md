# kt-signal-engine

KT 自研 Signal 协议引擎：以官方 **libsignal**（Rust）为协议内核的独立进程，长期目标替换
`signal-cli`，成为 kt-signal-connector 的协议引擎。

## 定位与动机

当前 KT Signal 链路 = `kt-desktop（闭源客户端） ↔ kt-signal-connector（闭源 Rust） ↔ signal-cli（GPLv3 外部引擎，钉定 v0.14.8 unmodified）`。

替换 signal-cli 的动机（按权重排序）：

1. **上游 bus-factor = 1**：signal-cli 为个人维护者项目，PR 周期以月计，服务端协议变更的
   跟进节奏不可控。
2. **能力天花板**：上游 JSON-RPC 全量 61 方法（v0.14.7 逐项核实）不含出站富文本
   （textStyles/bodyRanges）、静音、归档、转发、搜索。前四项上游无计划，搜索按官方语义
   本就属客户端本地行为。
3. **定制自由**：引擎侧行为（发送管线、存储、同步策略）由我们按官方桌面行为对齐，不再受
   上游取舍约束。

## 许可架构（本项目为开源仓）

本项目链接 **AGPL-3.0** 的 libsignal 并随 KT 桌面端分发，故**整个引擎以 AGPL-3.0 开源**
（见 `LICENSE`）。许可边界：

| 组件 | 许可 | 说明 |
|---|---|---|
| 本引擎（kt-signal-engine） | **AGPL-3.0 开源** | 链接 AGPL 的 libsignal 且对外分发，义务落在本仓 |
| libsignal（signalapp/libsignal） | AGPL-3.0（上游原样） | 以依赖形式引入；对它的修改（如有）须随本仓源码公开 |
| kt-signal-connector | KT 专有（闭源） | 与引擎独立进程、IPC 通信，属聚合关系，不传染 |
| kt-desktop | KT 专有（闭源） | 同上 |

AGPL §13 网络条款不触发：引擎是本机桌面进程，不对外提供网络服务。

## 架构

```
kt-desktop（闭源）
   │ host 契约 IPC（不变）
kt-signal-connector（闭源 Rust：store/会话/代理组/调度）
   │ 引擎 IPC：signal-cli JSON-RPC 兼容子集（ADR 0002）
kt-signal-engine（本仓，AGPL，Rust + libsignal）
   │
Signal 服务端
```

关键决策见 `adr/`：

- **ADR 0001** 进程模型与许可边界：引擎为独立 sidecar 进程；闭源侧经由进程隔离保持闭源。
- **ADR 0002** IPC 形状采用 **signal-cli JSON-RPC 兼容子集**：只实现 connector 已核实的
  白名单方法语义，最大化复用 connector 适配层、测试夹具（fake-signal-cli.py）与冒烟脚本；
  新能力一律经 connector 契约版本演进，不塞进兼容层。
- **ADR 0005** 链接时存量历史迁移（backup5）自研立项：产品刚需；S1 真机 spike 已通过
  （2026-10-03：信封 `ephemeralBackupKey` 命中、transfer_archive+CDN 下载字节级成功、
  无需 presage 补丁，取证记录见该文）；S2 引擎侧已落地（2026-10-04，提交 0eedc8c：transfer archive 解密分帧导出 + backup5 链接集成，存量档案落 history-import.ndjson），下一步 S3 connector。

## 复用清单（从现有链路继承，不重造）

- connector 的 store（SQLite schema 10 全部落库）、会话/账号管理、代理组、supervisor/watchdog、
  发送车道互斥纪律、契约版本化流程：**原样保留**。
- host 契约（contracts/signal-host-adapter.md 1.x）与 kt-desktop 客户端：**零改动**。
- 打包/供应链（bundle manifest SHA-256、smoke 门禁、soak 脚本）：**模式复用**，换打包对象。
- 测试夹具：`fake-signal-cli.py` 语义保留，作为引擎 IPC 兼容性的黄金对照。

## 迁移期核心问题（P0 必须回答）

**账号迁移**：现有 connector 账号是 signal-cli 数据目录里的 linked device。换引擎后：

- 方案 A：每账号重扫码链接一次（新设备身份，干净但客户有感）；
- 方案 B：读取 signal-cli 数据目录中的身份/注册材料做密钥迁移（客户无感，但依赖其
  私有存储格式，脆弱且需逐版核对）。

P0 spike 产出两种方案的可行性证据，再定 ADR 0003。

## 阶段规划

| 阶段 | 内容 | 门禁 |
|---|---|---|
| P0 | 可行性 spike：libsignal Rust crates 在 mac/win/linux 构建目标验证；FFI/桥接方式；signal-cli 数据目录密钥材料可读性评估（迁移 A/B 方案取证） | ✅ 完成，见 ADR 0003（2026-10-03） |
| P1 | 引擎骨架：进程模型、IPC 白名单子集（按 ADR 0002 逐方法）、身份与会话建立 | ✅ 完成：P1a 骨架 + P1b presage 真实链接/接收/直聊发送（2026-10-03） |
| P2 | 收发链路：文本/回执/typing/反应/编辑/删除，逐项对齐官方桌面行为 | ✅ 代码侧完成：接收投影全消费面/群组发送/联系人群组列表/getUserStatus 探测（2026-10-03）；2026-10-04 补齐——群组接收投影（groupInfo，connector 管线零改动）、回执发送三面（sendDeliveryReceipt/sendReadReceipt/sendViewedReceipt，官方逐作者直发+CHUNK 100）、send 扩面（quote+store 找回正文/preview/mentions）、存量联系人补同步（链路建立 2s 沉降 request_contacts，60s 一次性）、阅后即焚官方对齐（send `viewOnce` 参数 + 收投影 `viewOnceInvalid` 标记 + `syncMessage.viewOnceOpen` 投影 + sendViewOnceOpen 自同步面，官方 isValidTapToView/syncViewOnceOpen 同构，compat §5）；connector 接线已落地——`kt-engine` 实验模式 + 门控真机冒烟通过（fake 服务，2026-10-03）；实机收发联调待做 |
| P3 | 群组（Sender Keys）、多设备（Sesame） | 群组发送/接收投影 P2 已 real；成员级 results/群更新类消息（成员变更/权限）/多设备待做 |
| P4 | 附件 CDN、profile/sticker 密钥、链接预览 | ✅ 附件上行已 real（2026-10-04，829e09f：data URI→presage 官方 CDN v4 上行，≤32 条/单条 ≤100MB fail-closed，previewImage 同批挂 Preview.image）；✅ 贴纸包浏览 + 会话级置顶已 real（2026-10-04，a4a7bf0：getStickerPackManifest/getStickerImage 匿名 CDN 只读面 + getPinnedConversations/setConversationPinned Storage Service 同步面——读走 presage pub API、写为上游缺口自建 PUT 薄封装（409 conflictBackOff 重放），错误码与 connector 1.36 白名单逐字 8 码，compat §5 登记从未有 storage 状态分支）；字节下载（接收侧）待做；profile/sticker 密钥待做。另注：置顶写面在 presage 上游补齐后整段替换；presage 未接 Storage Service 读面，现代主设备对 contacts sync 可能回空 stub（真机现状：链接时主设备主动推送有效），storage-service 读面是后续加固项 |
| backup5 | 链接时存量历史迁移（并行专项，ADR 0005）：S1 真机 spike（一票否决点）→ S2 引擎接收/下载/解密 → S3 connector 契约+导入 → S4 桌面 UI+真机验收 | S1 PASS 2026-10-03（三未知数全落定）；S2 DONE 2026-10-04（0eedc8c：解密分帧导出 + backup5 链接集成，18 测试全绿）；下一步 S3 connector |
| P5 | 灰度替换：connector 引擎开关，soak 24h×N | 引擎开关已落地（connector `--signal-cli-engine kt-engine`，实验，proxy/media 能力 fail-closed）；soak PASS + 实机回归待做 |
| P6 | signal-cli 退役 | 全量切流 + 观察窗 |

## 对齐纪律（不可妥协）

1. **行为参照物 = 官方**：Signal-Desktop 源码（本地检出）+ libsignal 库 + Signal 协议公开
   文档。禁止凭记忆或猜测实现协议行为。
2. **契约先于代码**：每阶段先写接口文档与验收标准（本仓 `docs/`），评审后再动工。
3. **门禁全绿才能落**：fmt/test/clippy（Rust）、夹具契约测试、soak——沿用 connector 仓现行
   门禁标准。
4. **AGPL 文件头**：本仓所有源码文件带 AGPL-3.0 声明（见 `AGENTS.md`）。
