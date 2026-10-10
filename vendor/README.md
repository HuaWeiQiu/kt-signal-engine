# vendor/ — 原样拷贝的上游依赖（Stories 批，2026-10-08）

本目录当前是两个上游仓在引擎钉定 rev 的**逐字节原样拷贝**（可 `diff -rq`
对照 cargo git checkout 复核），经根 `Cargo.toml` 的 `[patch]` 段替换对应
git 源。story 协议面的最小叠加随本批后续 commit 落地，届时在本文件登记
文件级改动清单。

| 目录 | 上游 | 钉定 rev | 许可 |
|---|---|---|---|
| `libsignal-service/` | github.com/whisperfish/libsignal-service-rs | `9e6c08b8e6d413391831dc49065e5b05490d6c82` | AGPL-3.0-only（LICENSE.md 随目录） |
| `presage/presage` | github.com/whisperfish/presage | `33dd1491130793390e356e15a5711f1e6ca14fdb` | AGPL-3.0-only（LICENSE.md 随目录） |
| `presage/presage-store-sqlite` | 同上 | 同上 | AGPL-3.0-only，**零改动** |

为什么 vendor：上游（含 presage 传递钉定的 libsignal-service）没有 story
发送面；任务口径 = 「薄 fork、只扩最小 story 发送面」。`[patch]` 指本地
路径后，engine 与 presage 两层都能直接用扩展面，同时保持其余上游代码
零漂移。

任务书原文写「[patch.crates-io] 指本地路径」：实际两仓均为 git 源依赖
（Cargo.lock `git+https://…` 形状），`[patch.crates-io]` 对 git 依赖不
生效，按事实改用对应 git 源 URL 的 `[patch."…"]` 段（批汇报偏差项）。

## fork 改动清单（Stories 批，随 commit 递增登记）

钉定 rev 之上的**全部**叠加改动（逐文件；其余上游代码零漂移，可
`diff -rq` 对照 checkout 复核）：

`vendor/libsignal-service/`（rev 9e6c08b）：

- `src/websocket/sender.rs` — 新增 `send_messages_unidentified_story`：
  `PUT /v1/messages/{dest}?story=true` 查询参、无 `Unidentified-Access-Key`
  头。官方锚：libsignal v0.99.0 `ws/messages.rs:141-150`（story 后缀）+
  `:39`（Story→无 user-based 授权头）；桌面 `WebAPI.preload.ts:3804/3850`
  同查询参、`OutgoingMessage.preload.ts:392-394` story 恒 sealed sender。
- `src/sender.rs` — ①`try_send_message` 增 `story: Option<&SenderCertificate>`
  参数（既有 6 处调用点全传 `None` = 上游行为逐字保持；Some 时加密
  cert = `story.or(unidentified_access.map(cert))`、分发走 story ws 方法、
  无 identified 降级）；②新增 pub `send_story_message`（单收件人 sealed
  sender + `?story=true` 传输面；不产 sent-transcript——官方
  sendStory.preload.ts:338-340「story 同步由收尾单独发」同口径）。
- `src/content.rs` — `Metadata` 增 `pub story: Option<bool>`（E2：官方
  Envelope.story=16 接收分类位透传，Signal-Desktop
  MessageReceiver.preload.ts:1517）。
- `src/cipher.rs` — `decrypt` 唯一 Metadata 构造点填
  `story: envelope.story`（E2）。
- `src/provisioning/pipe.rs`（2026-10-10 nopni2 批）— ①QR link URI 构造
  抽成 pub `link_uri`（原 process_request 内联拆出，UUID/pub_key 形状不变）；
  ②URI 补 `capabilities=nopni,nopni2`：Signal-Android 8.30+/8.31 扫码侧对
  numberless（无 PNI）账号强制 nopni2（LinkDeviceViewModel.kt
  onQrCodeScanned，上游 commit cdd27e08fa 把枚举值 "nopni"→"nopni2"），
  缺失即 OUTDATED_DEVICE 拒连；双报对齐官方桌面
  Provisioner.preload.ts:429-433。backup5 不报（引擎未实现 link-and-sync，
  报了手机会走 archive 同步链接流）。取证 docs/upstream-capability-nopni2.md；
  门禁测试 src/service/mod.rs tests::link_uri_advertises_numberless_capabilities。
- `src/provisioning/mod.rs`（2026-10-10 nopni2 批）— `pub use pipe::link_uri`
  （引擎侧门禁测试入口）。

`vendor/presage/`（rev 33dd149）：

- `presage/src/manager/registered.rs` — ①新增 pub
  `Manager::send_story_message`（E1：注入发送者 profile_key 到
  StoryMessage、不落 store、不产 transcript，官方 profileKey 必需 + story
  不进本地 timeline 同口径）；②`send_message`/`send_message_to_group`
  两处 Metadata 字面量补 `story: None`（E2 字段新增的编译面，出站消息
  story 位恒无）。
- `presage/src/store.rs` — verified sync 存储路径 Metadata 字面量补
  `story: None`（E2，同上纯编译面）。

`vendor/presage/presage-store-sqlite/`：

- `src/content.rs` — Metadata 解构模式补 `story: _`（E2：story 位不落库
  ——收侧 story 本就不存库，普通消息位恒 None）。
- `src/data.rs` — Metadata 重放构造填 `story: None`（E2，同上）。

## 更新纪律

后续再动 `vendor/`：先 `diff -rq` 对照上表 rev 的 cargo checkout 确认
改动面，再在本文件登记文件级清单；上游升级 = 重新 vendor + 重放改动
清单。
