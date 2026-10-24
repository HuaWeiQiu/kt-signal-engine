# ADR 0002：IPC 形状采用 signal-cli JSON-RPC 兼容子集

日期：2026-10-02 ｜ 状态：已接受

## 背景

闭源 connector 现有一整套围绕 signal-cli JSON-RPC 的资产：引擎适配层、白名单方法
语义核实记录（v0.14.7/v0.14.8 逐方法字节级）、测试夹具 `fake-signal-cli.py`、
smoke 脚本、契约测试。替换引擎时 IPC 形状有两个选项：

(a) 引擎实现 **signal-cli JSON-RPC 兼容子集**（只含 connector 实际使用的白名单方法）；
(b) 设计全新 IPC 形状，connector 适配层随之重写。

## 决策

采用 (a)：本引擎对外暴露 **signal-cli JSON-RPC 兼容子集**——仅限 connector 已核实的
白名单方法（sendText/sendTyping/sendReaction/remoteDelete/receive 事件投影等），
语义以 connector `docs/implementation-plan.md` 已固化的上游行为描述为准。

## 理由

1. connector 适配层近乎零改动 → 迁移期最大的两个风险源（协议实现 + 集成面重写）解耦，
   一次只动一个。
2. `fake-signal-cli.py` 成为引擎兼容性的黄金对照：夹具形状不变，引擎必须让既有
   契约测试原样通过——兼容性有机器判据，不靠人眼。
3. smoke/soak 脚本（含 4 道探活检查）原样可用。

## 后果与边界

- **兼容层封顶**：只覆盖现有白名单；上游 signal-cli 未来新增方法不跟进；KT 新能力
  （如出站富文本）一律走 connector 契约版本演进，在本仓以**独立扩展方法**实现
  （命名空间与 signal-cli 方法清晰区分），不伪装成上游方法。
- 兼容子集的**行为缺陷不做兼容**：上游明显违反官方桌面行为之处（如有），以官方行为
  为准并在兼容层文档登记差异，connector 侧同步对齐。
- 本仓维护一份 `docs/jsonrpc-compat.md`：逐方法列出参数/返回/错误码与 signal-cli
  的对齐状态（P1 起建）。

## argv 形状兼容：`--ignore-*` 系旗标恒为 no-op（2026-10-14 澄清）

connector 拉起引擎的 argv 沿用 signal-cli（JVM）启动形状，含
`--ignore-attachments`（可选）/`--ignore-stories`/`--ignore-stickers`。引擎
**接受这三个旗标但恒为 no-op**（`src/main.rs` 严格 argv 解析中空臂通过）：仅为
connector 启动器形状不变而存在，引擎的收发行为与它们**无关**。

这不是 signal-cli 时代的「贴纸/故事消息被主动丢弃」语义——引擎接收管线照常解析并
投影这两类消息，媒体字节一律不在引擎侧下载（metadata-only 立项红线，按需取字节
归 connector transit 链）：

- **贴纸**：`DataMessage.Sticker` metadata-only 投影（packId/packKey/stickerId/
  emoji/data 附件指针，字节永不下载）。
- **story**：transient 投影（信封级 `storyMessage`/`isStory`，presage 收侧不落
  StoryMessage，connector 据此走 story 时间线）；story 回复本身是普通 DataMessage
  照常落库（官方同构）。
- **附件**：`locator` 指针投影，字节按需由 connector 取。

逐方法对齐状态与差异登记见 `docs/jsonrpc-compat.md` §1（启动契约）与 §5（story
块、附件 locator 勘误）。

（澄清动机：2026-10-14 外部调研把三旗标误读为「引擎主动丢弃贴纸/故事消息」——
那是 signal-cli（JVM）时代的语义，引擎不沿用；本节钉定 no-op 口径防再读偏。）
