# ADR 0003：P0 spike 结论——libsignal 消费路径、桥接方式与账号迁移

日期：2026-10-03 ｜ 状态：已接受

## 背景

README P0 要求可行性 spike 回答三问：

1. libsignal Rust crates 在 mac/win/linux 构建目标能否编译；
2. FFI/桥接方式怎么定；
3. signal-cli 数据目录密钥材料可读性（迁移方案 A 重扫码 vs 方案 B 密钥迁移取证）。

本文记录 2026-10-03 spike 取证与决策。取证均来自当日实测：官方仓
`signalapp/libsignal` release v0.103.1、社区仓 GitHub API 实查、本机
rustc 1.94.0 实编译、本机 connector 自测产生的真实 signal-cli 数据目录
（pin 0.14.x，结构级检查，未读取任何密钥值）。

## 取证记录

### 1. 官方 libsignal 不走 crates.io，git tag 钉定是唯一/既定消费路径

- crates.io 上的 `libsignal-protocol` 0.1.0（2019-07）是无关第三方
  libsignal-protocol-c 封装（名称占位）；官方从未向 crates.io 发布。
- 官方以 GitHub release tag 分发：v0.103.1（2026-09-22），workspace
  `license = AGPL-3.0-only`，`rust-version = 1.93.1`（≤ 本机 1.94.0）。
- 社区服务层 `whisperfish/libsignal-service-rs` 自身同样以 git tag 钉定
  官方库（其 Cargo.toml：`libsignal-protocol/core/account-keys/net/
  signal-crypto/zkgroup/usernames = git tag v0.99.0`）——git-pin 为该
  生态既定模式。

### 2. 构建验证（本机实测）

- **aarch64-apple-darwin（宿主）：`cargo check` exit 0**，完整依赖图
  （libsignal-protocol + libsignal-net v0.103.1）全部编译通过。
- 构建期依赖：`libsignal-net` build.rs 经 prost-build 编译自带 .proto，
  **需要 `protoc`**（本机 brew 安装 protobuf 失败，改用官方预编译二进制
  后通过）；依赖树含 `ring`（C）与 `boring-sys`（signalapp/boring fork，
  BoringSSL，CMake 构建链）。
- **win/linux 交叉 check 在本机不可行**：`ring` 的 build 脚本要为目标
  编译 C——windows-msvc 缺 MSVC/SDK 头文件（`assert.h not found`）、
  linux-gnu 缺 `x86_64-linux-gnu-gcc`。失败点是本机交叉 C 工具链缺失，
  不是 libsignal 源码问题（宿主全绿已含 ring/boring-sys 原生编译）。
- 结论：三平台链接级验证放 **P1 CI 原生 runner 矩阵**
  （macos/windows/ubuntu-latest 各自带原生 C 工具链 + protoc + cmake）。

### 3. FFI/桥接：不需要

引擎已定 Rust（ADR 0001 sidecar）。官方 crates 为纯 Rust 消费，直接
cargo 依赖即可，**零 FFI**；`libsignal-bridge/ffi/jni/node` 系列是给非
Rust 宿主的绑定层，本仓一律不引入。README P0 的「FFI/桥接方式」问题
以语言选型消解。

### 4. 服务层缺口与补法

官方 workspace 无 **provisioning（设备链接）** crate（成员仅 protocol/
net/net-chat/crypto/account-keys/device-transfer/media/message-backup/
keytrans/usernames/zkgroup/zkcredential/poksho/attest 及 bridge 系）；
`libsignal-net` 只提供 chat server 基础设施，链接流程官方放在 TS/客户端层。

补法选 **whisperfish 双件**（均为 AGPL-3.0、活跃维护 2026-09、未被
archive）：

- `libsignal-service-rs`：Signal 服务协议（websocket/CDN/provisioning
  socket/发送管线）；
- `presage`：账号/会话管理器 + `presage-store-sqlite`，覆盖「作为从设备
  扫码链接（同 Signal Desktop）」「联系人/profile 同步」「群组」「附件
  拉取解密落盘」「接收」，gurk 等长期生产使用。

行为对齐纪律不变：presage/service-rs 行为与官方桌面不一致处，以官方为
参照、在本仓兼容层文档登记差异。

### 5. 迁移取证：signal-cli 数据目录（方案 A/B 证据）

本机真实数据目录（connector 自测账号，`--signal-data-dir` 产物）：

```
signal-data/
  data/
    accounts.json                  # {accounts, version}：账号注册表
    <accountId>                    # JSON：version/serviceEnvironment/
                                   #   registered/number/username/
                                   #   encryptedDeviceName/deviceId/
                                   #   isMultiDevice/password/
                                   #   aciAccountData/pniAccountData/
                                   #   registrationLockPin/pinMasterKey/
                                   #   storageKey/accountEntropyPool/
                                   #   mediaRootBackupKey/profileKey/…
    <accountId>.d/
      account.db                   # SQLite，WAL，user_version 30，未加密
      storage-manifest             # JSON：storage service 状态
```

`account.db` 实测表族（与官方桌面 SQLCipher schema 同源命名）与行数：

| 表 | 行数 | 说明 |
|---|---|---|
| identity | 4 | 身份密钥（本机 + 对端） |
| pre_key | 198 | 一次性预密钥 |
| signed_pre_key | 2 | 签名预密钥 |
| kyber_pre_key | 200 | PQX/后量子预密钥 |
| session | 6 | 会话状态 |
| sender_key(+shared) | 0 | 群发密钥（该账号无群） |
| key_value | 10 | 杂项账号值 |
| storage_id / recipient / sticker / message_send_log(+content) / cdsi / group_v1(v2)(+member) | — | 存储服务/收件人/贴纸/MSL 等 |

**关键结论：全部协议态本地明文可读，无加密障碍。** 且账号 JSON 含
`accountEntropyPool`（AEP，身份密钥熵源）、`password`/`deviceId`
（服务器凭据）、`registrationId` 类字段——身份密钥可由 AEP 确定性重建，
无需解析 signal-cli 私有序列化即可恢复主体身份。

## 决策

1. **消费路径**：git tag 钉定官方 libsignal。版本落在服务层兼容窗内
   （service-rs 现 pin v0.99.0，滞后官方最新；我们以 presage/service-rs
   可共存的版本为准，升级作为维护项跟进，可用 `[patch]` 尝试对齐，API
   不兼容则随层）。
2. **桥接**：纯 Rust cargo 依赖，零 FFI，不引入 bridge 系。
3. **服务层**：采用 presage + libsignal-service-rs；禁止复制其源码，
   仅依赖引用；行为偏差按对齐纪律登记。
4. **账号迁移（回答 README P0 核心问题）**：
   - **方案 B（密钥材料迁移）为主路径**：从账号 JSON 取
     AEP/password/deviceId/registrationId，从 account.db 取 prekeys
     （含 kyber）与 session 记录，转换进引擎存储。AEP 锚定身份重建，
     记录格式转换面从「私有序列化解析」收窄为「DB 记录 → 引擎存储结构」。
   - **session 记录必须一并迁移**：只迁身份+prekeys 时，旧会话入站在
     会话重建前不可解密，零丢失要求不满足。
   - **方案 A（重扫码）保留为逐账号回退**：迁移粒度=单账号，失败账号
     独立重链，不阻塞整体切换。
   - 迁移转换器为 P1 独立工作项；验收口径=迁移后实机收发零丢失 +
     对端安全码不变 + 无需对方任何操作。

## 后果

- 开发环境与 P1 CI 均需 `protoc`（net 层构建期依赖）+ `cmake`
  （boring-sys）；P1 起 CI 矩阵以原生 runner 落 win/linux 链接级验证。
- 依赖树引入 presage/service-rs 两个社区件：bus-factor 风险从
  signal-cli 转移且显著降低（两者是薄服务协议层，必要时可 fork 自持；
  维持「不向上游提 PR」纪律不变）。
- 迁移转换器进入 P1 排期；转换器落地前 P2 实机验证一律以方案 A 链接的
  新账号进行，不阻塞引擎主线。
- 本机 signal-cli 数据目录结构取证基于 pin 0.14.x（account.db
  user_version 30）；引擎替换期内 connector 侧 pin 不升级，schema 冻结
  有界。转换器动工前需用第二个真实账号复核一遍布局一致性。
