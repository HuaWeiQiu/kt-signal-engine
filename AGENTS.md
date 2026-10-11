# AGENTS.md — kt-signal-engine

本仓定义 AI Agent 在 kt-signal-engine 工作时必须遵守的规则。

## 本仓是什么

KT 自研 Signal 协议引擎（libsignal sidecar），AGPL-3.0 开源仓，目标是替换
kt-signal-connector 眼下的外部引擎 signal-cli。架构与许可边界见
`README.md` 与 `adr/0001`、`adr/0002`（动手前必读）。

## 开始前必读

1. `README.md`（许可架构、阶段规划、迁移期核心问题）
2. `adr/0001`、`adr/0002`
3. `docs/` 下对应阶段的契约/接口文档
4. 上游参照：`/Users/tanye/test/kt/kt-signal-connector/docs/implementation-plan.md`
   （现有白名单方法的语义固化记录）与 `/Users/tanye/test/kt/upstream/Signal-Desktop`
   （官方桌面源码，行为参照物）

## 工作边界

允许：

- 实现引擎进程、libsignal 桥接、ADR 0002 定义的 JSON-RPC 兼容子集与独立扩展方法。
- 编写各阶段契约/验收文档（先文档后代码）。
- 编写/维护夹具兼容测试（对照 `fake-signal-cli.py` 语义）。

禁止：

- **复制 signal-cli 源码**（GPLv3）：只允许对着其公开行为重新实现，一个字节都不抄。
- **复制 Signal-Desktop 源码**（AGPL）：同上，只做行为对齐；本仓虽同为 AGPL，抄桌面端
  代码会把无关实现细节和许可噪音带进引擎。
- 让引擎持有或外传任何账号明文聊天内容、联系人、 cookie/token（消息内容只经内存转发，
  落库是 connector 的职责）。
- 未经 ADR 变更许可压缩/注册/注册回填等协议安全相关决策。
- 直接修改 `kt-signal-connector` 或 `kt-desktop`（跨仓改动走各自仓库的流程）。

## 代码纪律

- Rust 2021+；`cargo fmt`/`clippy -D warnings`/`test` 全绿是提交门禁。
- 每个源码文件头部带 AGPL-3.0 声明（SPDX：`SPDX-License-Identifier: AGPL-3.0-only`）。
- 提交信息中文、前缀 `feat/fix/docs/refactor/test/chore(engine)`；契约相关另加
  `engine-compat` 标注。标注断档的历史提交不回改追溯；新提交凡 wire/契约
  面变化强制标注。
- 依赖最小化：新增 crate 需在 PR 描述说明用途与许可（禁止引入 GPL-ink 不兼容依赖）。
