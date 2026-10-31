# 链接 store 生命周期与失败恢复

## 官方行为

presage `Manager::link_secondary_device` 由调用方提供同一个 store：
`clear_registration()` → provisioning → 成功后写 `registration_data`。
官方单账号进程没有 KT 的多账号 `accounts/lk-*` + `engine-state.json` 注册表，
因此没有「失败链接 store 与注册表不一致」这一类故障。

## KT 多账号包装的根因

`startLink` 在 `accounts/lk-*` 创建临时 store。失败、取消或进程退出后，
如果目录没有 registration_data，它仍然会被 `EngineState::load` 当成
「注册表缺失但存在账号 store」而 fail-closed，后续所有 startLink 都失败。

## 根治规则

1. 保持官方 store 生命周期，不引入第二套 staging/提交协议。
2. 正常失败路径删除本次未完成的 store。
3. 进程启动时，在 `EngineState::load` 前回收**未注册且没有 registration_data**
   的 `accounts/lk-*` 目录；有注册数据或无法证明为空的目录保留，继续 fail-closed。
4. 只有 `finishLink` 成功后才把目录写入 registry；真实账号目录绝不批量删除。

这样同时覆盖正常失败、取消、进程崩溃和重启恢复，不需要人工清理 `lk-*`。