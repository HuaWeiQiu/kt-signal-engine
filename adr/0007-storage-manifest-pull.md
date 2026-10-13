# ADR 0007：Storage manifest 拉取腿——fetchLatest 消费臂 + 登录期自动拉

日期：2026-10-13 ｜ 状态：已接受 ｜ 批次 T-2/T-9（storage 腿合并实施，「加强版拉取」方案定案）

## 背景

引擎 Storage 面此前全是按需请求/应答：connector 显式 RPC（storage.mergeContacts/
storage.importGroups 等）才发生 manifest 重读，引擎自身从不主动拉。批次 T-9 可行性
勘核实锤两条官方事实（Signal-Desktop 源码只读锚，AGPL 行为对齐非复制）：

1. **官方没有 storage live 推送**。服务器 websocket 对链接设备只推 E2E 信封与队列
   排空标记；Storage Service 修订不走任何推送通道。官方「实时」语义 = 改了
   manifest 的设备发**普通 E2E 信封** `SyncMessage.fetchLatest{STORAGE_MANIFEST}`
   ，收方收到后经 HTTP 重读 manifest + items（收方 HTTP 拉取，非服务器推送）。
2. **官方桌面首连即拉**（background afterFirstAuthSocketConnect：授权 socket 建立
   后立即重读 manifest 全量 merge）。会话列表完整性——含「没有消息的群会话也要在
   登录后显示」——依赖这一步把 GV2 masterKey 灌进本地库。

我方栈上的两处缺口随之清晰：①fetchLatest 触发线已通到引擎投影（`Content::FetchLatest`
投影臂，compat §5 P14 组5）与 connector host 事件，但**无人消费、无任何重拉发生**
——触发信号到站即弃；②引擎建流后不做登录期 manifest 自动拉——connector 登录期的
显式 merge 是 connector 时机驱动的补位，引擎侧建流即拉缺位，正是「群会话登录后不
显示」的主根因（无消息群会话的 GV2 记录只存在于 manifest，不随消息到达）。

## 决策

1. **fetchLatest 消费臂，engine 内闭环、零 wire 新增**：receive pipeline 收到
   `FetchLatest` 且 type=STORAGE_MANIFEST 时触发一次「manifest 重读 + merge」。
   合并面复用 connector 驱动的 `storage.mergeContacts` / `storage.importGroups`
   同一内部函数面（`pull_storage_manifest` 单点）——一处权威，消费臂/登录期自动拉
   /connector 显式 RPC 三链共用，不另造第三条 merge 路径。翻转通知走既有
   storage_watch 观测簿差分（`receive.conversationState`，词表第五词），无新通知词。
   非 STORAGE_MANIFEST type（LOCAL_PROFILE/SUBSCRIPTION_STATUS/UNKNOWN/type 缺席）
   不触发（信号面投影照旧——投影与消费正交）。
2. **防抖 500ms**（官方 sync job debounce 同款，trailing 沿）：每封触发信封把窗沿
   推后 500ms，防抖窗内多封合并为恰一次拉取；窗沿到期在收流 select 独立臂执行，
   不在信封咽喉内阻塞投影/ack 时序。
3. **登录期自动拉**：建流成功后与 +2s 沉降三 sync 请求（P15c 项4）同一时机带追加
   一次，对齐官方 afterFirstAuthSocketConnect；共享 60s 一次性闸（重连风暴不重复
   轰 storage）。connector 登录期显式 merge 调用保留——读面幂等，双链无叠加效应。
4. **并发互斥不新造锁**（验证结论记档）：自动拉是纯读面（manifest GET + items GET
   + 本地 presage/sidecar 写）。引擎为单线程 LocalSet 装配（main.rs：请求 dispatch
   与接收管线同线程协作式交错，await 点让出，无并行数据竞争面）；与 connector
   驱动的显式 merge/写**交错**时——
   - 本地面：SQLite 库级串行 + 逐写幂等（同钥 save_identity no-op、profile_key
     缺才写、contact 缺才建、group 命中零网络跳过），交错重复写收敛同值；
   - 服务器写面不经此路径（merge 不产 WriteOperation）；connector 的
     storage.write/置顶/拉黑/三 flag 写已有 409 版本闸 + conflictBackOff 重放，
     版本冲突 fail-visible 不静默覆盖；
   - 通知出口：storage_watch 簿 StdMutex 单点差分——交错两份相同观测恰一次翻转
     （后到者同值静默），不重不漏。
5. **24h 例行拉取不做**（记档）：官方桌面没有 background 周期性 manifest 拉取；
   我方收敛靠「每次建流登录拉 + fetchLatest 触发信封」两面。断链窗口的改动由服务
   器队列在重连后补投信封触发（信封入队不丢），登录拉兜底队列外修订。

## 后果与边界

- wire 零新增：消费臂纯 engine 内行为；旧 connector/桌面零感知。
- 拉取逐腿 best-effort：失败走打点（`storage_manifest_pull_err`），不重试不停车；
  下次触发/下次链路建立重补（读面幂等）。触发源打点分账
  （`storage_manifest_pull` / `storage_manifest_pull_login`）。
- 拉取在收流循环内串行执行（单任务 select，拉取期间信封处理暂停）——与官方
  singleProtoJobQueue 串行同型；防抖窗保证风暴合并，串行窗实际不可感。
- 「群会话登录后不显示」主根因补口：登录拉把 GV2 masterKey 导入 presage store，
  connector listGroups/open_group_sender 面即刻可用；无消息群会话不再依赖首条消息
  才现身。
