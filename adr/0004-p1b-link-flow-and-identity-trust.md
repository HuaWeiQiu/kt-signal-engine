# ADR 0004：P1b 链接流程——presage 公开入口 + finishLink 后改名

状态：已采纳（P1b，替代已否决的 vendored provisioning 方案） ｜ 依据：ADR
0002/0003、`docs/jsonrpc-compat.md` §1/§3

## 背景

IPC 契约（夹具/connector 对齐）是 `startLink {} → {deviceLinkUri}` +
`finishLink {deviceLinkUri, deviceName} → {number}`：设备名在 finishLink 才
到达。上游 presage 的现成入口 `Manager::link_secondary_device(store, servers,
device_name, tx)` 把 device_name 烘焙在调用时刻（startLink），二者时序错位。

曾被采纳的方案是把上游私有项（`ProvisioningPipe`、`replenish_pre_keys`，
`mod pipe` / `pub(crate)`）快照进本仓做延迟设备名编排。实现验证时发现墙比
预想深：`SignalWebSocket::take_request_stream/return_request_stream` 亦是
pub(crate)，provisioning WS 的消费面在 crate 外**无法**公共组装——vendor
方案需要 fork 上游或成片复制其 WS 层，维护成本与协议风险都越过本仓边界。

## 决策

1. **链接走 presage 公开入口** `Manager::link_secondary_device`，startLink
   时刻以固定占位名 `KT Signal Engine` 烘焙。URL（官方
   `sgnl://linkdevice?uuid=…&pub_key=…`，真机可扫，connector 对 URI 透传
   不解析）经 channel 回 startLink。
2. **finishLink 后改名**：协议侧设备名是本设备 account attributes 的
   `name` 字段（PUT /v1/accounts/attributes，`AccountManager::
   set_account_attributes` 公开），且 presage 接收管线每次启动都会用 store
   里 `RegistrationData.device_name` 重推 attributes。因此 finishLink 拿到
   设备名后：改写 store 中 `RegistrationData.device_name` →
   `save_registration_data` → 启动接收管线，管线首步即把用户输入的名字
   推上服务器，且此后每次重连保持一致。无需任何私有 API。
3. **占位名窗口的如实登记**：从扫码确认到接收管线首推 attributes 之间
   （秒级），手机端「已链接设备」列表短暂显示占位名。这是公开-API 路线
   的已知折衷；若上游未来公开 provisioning 流程（或提供延迟命名），可
   无痛替换。
4. **身份信任策略 = trust-on-first-use 自动信任**（`OnNewIdentity::Trust`，
   presage/gurk 默认，与 signal-cli 默认本地信任模型一致）；安全码变更
   告警等治理随 P2 事件面评估。
5. **账号目录布局**：`--data-dir/accounts/<lk-hex>/`（链接开始时以
   splitmix 时间熵命名，避开「先开店后知道号码」的重命名竞态）；
   `engine-state.json` 升 v2 登记 `{number, dir}`；v1（P1a 桩合成号码）
   重置为空不迁移。删除账号 = 删登记 + `remove_dir_all` + 停接收管线。
6. **发送管线**：每次发送短命 `Manager::load_registered(store.clone())`
   实例（接收实例长期持有 Manager，二者不共享）；直聊号码经 contacts
   同步入库的 number→ACI 映射解析（`Contact.phone_number`），冷启动窗口
   内未命中按内部失败处理。群组发送（Sender Keys）明确不在 P1b，返回
   显式错误（P3）。**反应目标作者**：协议字段是 `targetAuthorAci`
   （UUID），经同一 contacts 映射把连接器给的号码解析为 UUID；未命中
   按内部失败（不猜、不透传号码——协议要 UUID）。

## 后果

- 依赖树回到最小面：presage / presage-store-sqlite / tokio / futures /
  url / libsqlite3-sys(bundled) / serde(+json)。无 fork、无 vendor、
  无 GPL-ink 风险项；许可全部 MIT/Apache-2.0 或 AGPL-3.0-only（同本仓）。
- 升级 presage pin 时需复核：link_secondary_device 形状、
  RegistrationData 字段、接收管线 set_account_attributes 行为
  （compat §5 与本 ADR 同步）。
