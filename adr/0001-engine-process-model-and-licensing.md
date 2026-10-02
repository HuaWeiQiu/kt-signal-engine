# ADR 0001：引擎进程模型与许可边界

日期：2026-10-02 ｜ 状态：已接受

## 背景

替换 signal-cli 的候选形态有三：(a) 在闭源 connector 内直接链接 libsignal；
(b) fork signal-cli；(c) 自研独立引擎进程，闭源 connector 经 IPC 与之通信。

libsignal 为 **AGPL-3.0**（已核实上游 LICENSE）。KT 的 connector 与客户端为专有闭源，
且运行时会随桌面端分发给客户。

## 决策

采用 **(c) 独立引擎进程（sidecar）**：本引擎以独立进程运行，闭源 connector 经本机 IPC
（ADR 0002：signal-cli JSON-RPC 兼容子集）与之通信。

## 理由

1. **许可安全**：AGPL 程序与闭源程序以独立进程 + IPC 方式交互属「聚合」（aggregate），
   各自许可独立；链接进同一可执行文件则组合作品整体被 AGPL 传染，闭源不可行。
2. **分发义务清晰且可控**：随客户端分发的引擎以 AGPL-3.0 公开本仓源码即履行义务
   （与今天分发原版 signal-cli 附上游源码 offer 同性质）；闭源侧零公开义务。
3. **爆炸半径**：引擎崩溃/升级不波及闭源 connector 进程（沿用现 supervisor/watchdog 模式）。
4. **排除项**：(a) 许可不可行；(b) fork 继承 GPLv3 + 个人维护者的存储/协议包袱，
   每次 rebase 成本翻倍，仅解决部分能力缺口，不解决 bus-factor。

## 后果

- 本仓必须 AGPL-3.0 开源；对 libsignal 的任何修改随本仓公开。
- AGPL §13 网络条款不触发（本机进程，不对外提供网络服务）。
- 若未来需要闭源引擎，唯一路径是向 Signal 采购 libsignal 商业授权（已知通道，
  暂不启用）。
- 法务复核点：进程隔离 vs 衍生作品认定在个别司法辖区存在边缘案例；正式对外分发前
  走一次法务评审（工程结论：聚合边界清晰，风险低）。
