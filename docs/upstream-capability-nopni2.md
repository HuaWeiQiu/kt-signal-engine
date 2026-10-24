# 上游 capability 取证：nopni2（numberless 账号链接门，扫码侧）

状态：取证结案 + 引擎补位已落地（vendor 叠加批，2026-10-10）｜
依据：ADR 0003（connector 0003 signal-cli-fallback-policy）、
scripts/upstream_sentinel.py ｜ 取证日期 2026-10-10

哨兵首跑（2026-10-10，signal-cli issues 源）命中 nopni2 相关
issue（AsamK/signal-cli#2140），引擎全仓 grep nopni2 零命中。本文固化取证
结论：语义、影响面、引擎构造链、缺位的后果与补位方案（已按"确凿构造点 +
可证测试"标准实施）。

## 1. 语义：nopni / nopni2 是什么

`nopni`（legacy）与 `nopni2` 是 **设备链接二维码 URI（`sgnl://linkdevice?…`）
的 `capabilities` 查询参数**里的声明位，语义 = 「本链接设备支持链接
**numberless 账号**」（Signal Login 注册的无手机号账号，无 PNI ——
Phone Number Identity，故 no-PNI）。它**不是** Signal-Server 侧强制校验的
registered capability（Signal-Server 全源 grep 零命中），而是**手机端扫码
时的本地校验门**：

Signal-Android `app/src/main/java/org/thoughtcrime/securesms/linkdevice/
LinkDeviceViewModel.kt` `onQrCodeScanned`（8.31 main 实查）：

```
val capabilities = uri.getCapabilities()      // 逗号拆 capabilities 查询参
when {
    Capability.Numberless !in capabilities && SignalStore.account.pni == null
        -> QrCodeState.OUTDATED_DEVICE        // 拒连："device is out of date"
    Capability.LinkAndSync in capabilities    // backup5
        -> QrCodeState.VALID_WITH_SYNC
    else -> QrCodeState.VALID_WITHOUT_SYNC
}
```

枚举（同文件，commit cdd27e08fa 起）：`LinkAndSync("backup5")`、
`Numberless("nopni2")`。

要点：

- **只对 numberless 账号生效**（`account.pni == null` 才检查）。正常手机号
  账号（有 PNI）不看这两个位，缺了照常链接。
- **`backup5` 是开关不是门**：带着 → 手机走「链接并同步」（transfer archive
  上传流）；不带 → 普通链接。引擎未实现 link-and-sync，不得报。
- 版本线：Android 8.28 beta 起 numberless 账号要求 URI 带 `nopni`；
  **8.30+/8.31 起改要求 `nopni2`**（不含 nopni2 即 OUTDATED_DEVICE）。
  官方桌面扫码发出端**双报** `nopni,nopni2` 兼容新旧两代手机
  （Signal-Desktop `ts/textsecure/Provisioner.preload.ts:429-433`：
  `'nopni' // legacy e164-less linking`、`'nopni2' // e164-less linking`、
  按 `isLinkAndSyncEnabled()` 追加 `'backup5'`）。

## 2. 时间线（证据链）

| 日期 | 事件 | 锚点 |
|---|---|---|
| 2026-09-17 | Android 8.28 beta 引入 numberless 账号，链接 URI 缺 `nopni` 即拒；signal-cli#2123 补 `capabilities=nopni` 支持面 | AsamK/signal-cli#2122 → #2123（closed 2026-09-20） |
| 2026-10-05 | Signal-Android commit **cdd27e08fa**「Update linking capabilities to nopni2.」：`Numberless` 枚举值 `"nopni"`→`"nopni2"`，同时自家发出端改报 `nopni,nopni2`（ProvisioningSocket.kt，Mode.Link）；随 8.30.x / 8.31.0 发布 | signalapp/Signal-Android@cdd27e08fa（触及 LinkDeviceViewModel.kt + ProvisioningSocket.kt 两文件） |
| 2026-10-08 | signal-cli v0.14.9 仍只报 `nopni` → numberless 链接再挂："The device you are trying to link is out of date"；#2140 开 | AsamK/signal-cli#2140 |
| 2026-10-08 | reporter 验证：URI 追加 `,nopni2`（双报）后 8.31.0 链接成功，收发正常，无需其他改动 | #2140 评论（aspeskin） |
| 2026-10-09 | #2140 关闭 | — |
| 2026-10-10 | 哨兵（scripts/upstream_sentinel.py）首跑命中 #2140；引擎取证并补位 | 本文件 |

## 3. 引擎构造链与缺位后果

引擎 QR 链接流（startLink → 手机扫码确认）：

```
methods.rs startLink
  → service/mod.rs start_link（:601）
    → link_flow（:3935）
      → libsignal-service provisioning::link_device（vendor …/provisioning/mod.rs:173）
        → ProvisioningPipe（vendor …/provisioning/pipe.rs）
          process_request 收 PUT /v1/address → 构造 sgnl://linkdevice URI
      ← SecondaryDeviceProvisioning::Url 原样透传
  ← startLink 返回 URI 字符串（connector 渲染 QR）
```

取证前状态：vendored libsignal-service（rev 9e6c08b）的 URI 只带
`uuid` + `pub_key`，**capabilities 查询参完全缺失**——比 signal-cli 修复前
（只报 `nopni`）还要少一代。

后果裁定：

- **numberless 账号（无 PNI）× Android 8.28+**：扫码即 OUTDATED_DEVICE
  拒连。8.30+/8.31 缺 `nopni2` 拒；旧版缺 `nopni` 拒。引擎缺位是全版本拒，
  不是仅新版本。
- **正常手机号账号**：不受影响（`pni != null` 短路第一分支）。
- 与 SPQR（`LinkCapabilities::default().spqr`，vendor
  `push_service/linking.rs:34-43`）**机制不同**：spqr 是 confirm_device 阶段
  **服务器侧** 409 强制（所有设备/所有账号）；nopni2 是扫码阶段**手机本地**
  校验（仅 numberless 账号）。哨兵关键词里的 spqr 与本轮 nopni2 是两条独立
  强制线，不可互相佐证。

## 4. 补位方案（已实施，2026-10-10 批）

构造点确凿（§3 链路唯一 URI 生成点）+ 测试可证，满足任务书改码标准，随本批
落地（与本文同 commit；改动登记见 vendor/README.md fork 清单）：

- `vendor/libsignal-service/src/provisioning/pipe.rs`：URI 构造抽成
  `pub fn link_uri(address, public_key) -> Url`，追加
  `capabilities=nopni,nopni2`（双报对齐官方桌面；**不报 backup5**）。
- `vendor/libsignal-service/src/provisioning/mod.rs`：`pub use pipe::link_uri`
  作引擎侧测试入口。
- 门禁测试 `src/service/mod.rs` `tests::link_uri_advertises_numberless_
  capabilities`：钉定 uuid/pub_key 透传 + nopni/nopni2 双报 + backup5 缺席。

### 后续注意

1. **升级哨兵**：下次同类异动的最快信号仍是 signal-cli issues（本轮 #2140
   早于任何官方公告）；`nopni2 → nopniN` 若再 bump，症状相同（OUTDATED_DEVICE），
   照桌面发出端形状追加即可。
2. **backup5**：~~引擎若实现 link-and-sync（transfer archive 链接同步），
   需评估后经 `link_uri` 追加 `backup5`——报了手机就走 VALID_WITH_SYNC 流，
   引擎必须承接 archive 上传，未承接前禁报。~~
   **（2026-10-05 翻案）** 引擎 ADR 0005 S2 已落地 transfer archive 下载侧
   （`download_transfer_archive`，0eedc8c），`backup5` 声明随之生效——但
   追加位置引发双参数缺陷，见 §5。
3. **spqr 线不动**：`LinkCapabilities::default()`（confirm_device 侧）与本次
   无关，spqr=true 维持。

## 5. 追加取证（2026-10-15）：双 `capabilities` 参数缺陷与修复

**症状**：手机扫码后链接成功，但不弹「是否转移聊天记录」、不上传迁移档案，
引擎 `download_transfer_archive` 空等至预算上限（真机 15:30 链接成功复现；
用户侧对照实验：解绑后重扫官方桌面二维码转移弹框正常出现）。

**根因**：0eedc8c 在引擎 `link_flow` 里对 `link_uri` 产物
`append_pair("capabilities", "backup5")`——而 26cc201 已让 `link_uri`
自带 `capabilities=nopni,nopni2`，最终 QR 出现**两个同名参数**：

```
sgnl://linkdevice?uuid=…&pub_key=…&capabilities=nopni,nopni2&capabilities=backup5
                                     ↑ 手机只读这个            ↑ 被吞
```

**手机侧裁定（真源码实查，Signal-Android main
`linkdevice/LinkDeviceViewModel.kt`）**：

```kotlin
private fun Uri.getCapabilities(): Set<Capability> {
  return this.getQueryParameter("capabilities")   // 单数 API：同名参数只取第一个
    ?.split(",")?.mapNotNull { … }?.toSet() ?: emptySet()
}
```

`Uri.getQueryParameter` 取**第一个**同名参数 → 手机只见 `nopni,nopni2` →
`LinkAndSync(backup5) !in capabilities` → `VALID_WITHOUT_SYNC`：链接继续、
转移不发起。官方桌面（`Provisioner.preload.ts`）capabilities 数组经
toAppUrl 一次成**单参数**，故无此问题。

**修复**：引擎 `src/service/mod.rs` `qr_link_url()`——渲染面把声明位收敛为
单个 `capabilities` 参数（去重保序 + 保证 `backup5` 在位），幂等（定点性质
有测试钉定：`qr_link_url_collapses_duplicate_capabilities_into_single_param`/
`…_appends_backup5_when_capabilities_absent`/`…_is_idempotent`）。编码面
`append_pair` 的 form-urlencoded 归一化（`,` → `%2C`）对手机侧
`getQueryParameter` 解码透明，与现行 `pub_key` 编码产出同源。
