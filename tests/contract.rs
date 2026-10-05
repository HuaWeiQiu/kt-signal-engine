// SPDX-License-Identifier: AGPL-3.0-only

//! P1a/P1b 契约测试：起真实引擎进程（tempdir 数据目录），逐方法断言
//! `docs/jsonrpc-compat.md` §3 的形状/错误码/调度日志纪律。
//!
//! IPC 形状与生命周期契约跑 fake 服务（`KT_ENGINE_FAKE_SERVICE=1`，compat §6）；
//! real 模式只测无网络路径（无会话 finishLink、未知账号 send）。

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct Engine {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Engine {
    fn spawn(data_dir: &PathBuf) -> Self {
        Self::spawn_with_env(data_dir, &[])
    }

    fn spawn_with_env(data_dir: &PathBuf, envs: &[(&str, &str)]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kt-signal-engine"))
            .arg("--data-dir")
            .arg(data_dir)
            .arg("jsonRpc")
            .arg("--receive-mode")
            .arg("on-start")
            .arg("--ignore-attachments")
            .arg("--ignore-stories")
            .arg("--ignore-stickers")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .envs(envs.iter().copied())
            .spawn()
            .expect("engine spawns");
        let stdout = child.stdout.take().expect("stdout piped");
        let (tx, rx) = channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(text) => {
                        if tx.send(text).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let stdin = child.stdin.take().expect("stdin piped");
        Self {
            child,
            stdin,
            lines: rx,
        }
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let frame = self.raw_frame(method, params);
        self.next_response_of_frame(frame)
    }

    fn raw_frame(&mut self, method: &str, params: Value) -> String {
        static ID: AtomicU64 = AtomicU64::new(1);
        let id = ID.fetch_add(1, Ordering::Relaxed);
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        writeln!(self.stdin, "{frame}").expect("frame written");
        self.stdin.flush().expect("stdin flushed");
        frame.to_string()
    }

    fn next_response_of_frame(&mut self, _sent: String) -> Value {
        let line = match self.lines.recv_timeout(Duration::from_secs(10)) {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => panic!("timeout waiting for engine response"),
            Err(RecvTimeoutError::Disconnected) => panic!("engine stdout closed early"),
        };
        serde_json::from_str(&line).expect("engine emits valid JSON")
    }

    fn error_of(&mut self, method: &str, params: Value) -> (i64, String) {
        let response = self.request(method, params);
        let error = response.get("error").expect("error response").clone();
        (
            error.get("code").and_then(Value::as_i64).expect("code"),
            error
                .get("message")
                .and_then(Value::as_str)
                .expect("message")
                .to_owned(),
        )
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kt-engine-p1a-{tag}-{}-{}",
        std::process::id(),
        DIR_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("temp dir created");
    dir
}

fn assert_e164_number(number: &str) {
    assert!(number.starts_with('+'), "number {number} lacks E.164 plus");
    assert!(
        number[1..].chars().all(|c: char| c.is_ascii_digit()),
        "number {number} has non-digit body"
    );
}

#[test]
fn invalid_argv_fails_fast_with_usage_exit() {
    let cases: &[&[&str]] = &[
        &[],
        &["jsonRpc"],
        &["--data-dir"],
        &["--data-dir", "/tmp/kt-engine-missing-dir"],
        &["--data-dir", "/tmp/kt-engine-no-sub", "daemon"],
        &[
            "--data-dir",
            "/tmp/kt-engine-unknown-flag",
            "jsonRpc",
            "--bogus",
        ],
    ];
    for argv in cases.iter().copied() {
        let output = Command::new(env!("CARGO_BIN_EXE_kt-signal-engine"))
            .args(argv)
            .output()
            .expect("engine runs");
        assert_eq!(
            output.status.code(),
            Some(2),
            "argv {argv:?} should exit 2, got {:?}; stderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.stderr.is_empty(),
            "usage text expected on stderr for {argv:?}"
        );
    }
}

#[test]
fn jvm_environment_is_ignored() {
    let dir = temp_dir("jvm-env");
    let mut engine = Engine::spawn_with_env(
        &dir,
        &[
            ("JAVA_OPTS", "-Xmx64m -XX:+HeapDumpOnOutOfMemoryError"),
            ("JAVA_HOME", "/nonexistent"),
        ],
    );
    assert_eq!(
        engine.request("listAccounts", json!({}))["result"],
        json!([])
    );
}

const FAKE_SERVICE: &[(&str, &str)] = &[("KT_ENGINE_FAKE_SERVICE", "1")];

#[test]
fn start_and_finish_link_lifecycle() {
    let dir = temp_dir("link-lifecycle");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    let started = engine.request("startLink", json!({}));
    let uri = started["result"]["deviceLinkUri"]
        .as_str()
        .expect("deviceLinkUri string")
        .to_owned();
    assert!(uri.starts_with("sgnl://link?uuid="), "uri shape: {uri}");
    assert!(uri.contains("&pub_key="), "uri carries pub_key: {uri}");

    // 篡改 URI：上游错误码与原文逐字对齐（compat §3）。
    let (code, message) = engine.error_of(
        "finishLink",
        json!({ "deviceLinkUri": "sgnl://link?uuid=tampered&pub_key=tampered", "deviceName": "KT" }),
    );
    assert_eq!((code, message.as_str()), (-1, "Unknown device link uri."));

    let finished = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    );
    let number = finished["result"]["number"]
        .as_str()
        .expect("number string")
        .to_owned();
    assert_e164_number(&number);

    let accounts = engine.request("listAccounts", json!({}))["result"]
        .as_array()
        .expect("accounts array")
        .clone();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0]["number"], json!(number));

    assert_eq!(
        engine.request("deleteLocalAccountData", json!({ "account": number }))["result"],
        json!({})
    );
    assert_eq!(
        engine.request("listAccounts", json!({}))["result"],
        json!([])
    );
}

#[test]
fn real_mode_no_network_error_paths() {
    let dir = temp_dir("real-no-net");
    let mut engine = Engine::spawn(&dir);

    // 无进行中会话的 finishLink：契约错误原文（不触网，compat §3）。
    let (code, message) = engine.error_of(
        "finishLink",
        json!({ "deviceLinkUri": "sgnl://linkdevice?uuid=00000000-0000-0000-0000-000000000000&pub_key=AAA", "deviceName": "KT" }),
    );
    assert_eq!((code, message.as_str()), (-1, "Unknown device link uri."));

    // 未注册账号的 send：内部失败折叠（-32603，不触网，细节不外泄）。
    let (code, message) = engine.error_of(
        "send",
        json!({ "account": "+15559990000", "recipient": ["+15555550101"], "message": "hi" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的 listContacts/listGroups：同样折叠（store 打不开）。
    for method in ["listContacts", "listGroups"] {
        let (code, message) = engine.error_of(method, json!({ "account": "+15559990000" }));
        assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    }

    // 未注册账号的 getUserStatus：whoami 前置即内部失败（不触网）。
    let (code, message) = engine.error_of(
        "getUserStatus",
        json!({ "account": "+15559990000", "recipient": "+15555550101" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的置顶面：storage 上下文打不开（UnknownAccount）→ 引擎
    // 内部错误折叠（compat §4.33 四码只覆盖 storage 链路本身）。
    let (code, message) = engine.error_of(
        "getPinnedConversations",
        json!({ "account": "+15559990000" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    let (code, message) = engine.error_of(
        "setConversationPinned",
        json!({ "account": "+15559990000", "conversationId": "x", "kind": "contact", "pinned": true }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    // 未注册账号的拉黑只读面：storage 上下文打不开 → 同置顶面折叠。
    let (code, message) = engine.error_of(
        "getBlockedConversations",
        json!({ "account": "+15559990000" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的 sendViewOnceOpen：self-send 前置即内部失败（不触网）。
    let (code, message) = engine.error_of(
        "sendViewOnceOpen",
        json!({ "account": "+15559990000", "senderAci": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "timestamp": 42 }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的 setExpirationTimer：打开已注册 manager 前置即内部失败
    // （不触网；消失消息 E 批）。
    let (code, message) = engine.error_of(
        "setExpirationTimer",
        json!({ "account": "+15559990000", "recipient": "+15555550101", "expirationInSeconds": 60 }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的 sendCallMessage：send 系同折叠（打开已注册 manager 前置
    // 即内部失败，不触网；通话消息面 KT 扩展批）。
    let (code, message) = engine.error_of(
        "sendCallMessage",
        json!({ "account": "+15559990000", "recipient": "+15555550101", "data": "c2lnbmFsLXJ0Yw==" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 未注册账号的 setProfile：注册数据前置即内部失败（不触网；个人资料
    // 写面）。
    let (code, message) = engine.error_of(
        "setProfile",
        json!({ "account": "+15559990000", "givenName": "KT" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    assert_eq!(
        engine.request("listAccounts", json!({}))["result"],
        json!([])
    );
}

/// 贴纸包浏览（compat §4.32）：参数面 fail-closed 逐码位 + fake 模式网络面
/// FETCH_FAILED（匿名 CDN 只读，无 account 参数）。
#[test]
fn sticker_pack_browsing_param_and_fetch_error_surface() {
    let dir = temp_dir("sticker-browse");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let official_pack_id = "ae8fedafda4768fd3384d4b3b9db963d";
    let official_pack_key = "U/Sqi5Xhwuda+rIyj+Z+ttev+81PUM1NqJ38Ml28c8o=";

    // 参数缺失 → KEY_INVALID 码位（key 形状参数失败同码）。
    let (code, message) = engine.error_of("getStickerPackManifest", json!({}));
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_KEY_INVALID"));
    let (code, message) = engine.error_of("getStickerImage", json!({}));
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_KEY_INVALID"));

    // packKey 非 32 字节 base64 → 同码位。
    let (code, message) = engine.error_of(
        "getStickerPackManifest",
        json!({ "packId": official_pack_id, "packKey": "short" }),
    );
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_KEY_INVALID"));

    // packId 大写 hex → 拒绝（官方 [0-9a-f]{32} 仅小写）。
    let (code, message) = engine.error_of(
        "getStickerPackManifest",
        json!({ "packId": "AE8FEDAFDA4768FD3384D4B3B9DB963D", "packKey": official_pack_key }),
    );
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_KEY_INVALID"));

    // 参数合法：fake 模式无网络 → FETCH_FAILED（manifest 与单图同构）。
    let (code, message) = engine.error_of(
        "getStickerPackManifest",
        json!({ "packId": official_pack_id, "packKey": official_pack_key }),
    );
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_FETCH_FAILED"));
    let (code, message) = engine.error_of(
        "getStickerImage",
        json!({ "packId": official_pack_id, "packKey": official_pack_key, "stickerId": 0 }),
    );
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_FETCH_FAILED"));

    // stickerId 缺失/非数值 → KEY_INVALID 码位。
    let (code, message) = engine.error_of(
        "getStickerImage",
        json!({ "packId": official_pack_id, "packKey": official_pack_key }),
    );
    assert_eq!((code, message.as_str()), (-1, "STICKER_PACK_KEY_INVALID"));
}

/// 会话级置顶（compat §4.33）：fake 模式空列表成功面 + 参数面 fail-closed
/// （-32603，四码只覆盖 storage 链路本身，参数错误不占用结构化码）。
#[test]
fn conversation_pinned_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("pinned-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 读：空列表成功。
    assert_eq!(
        engine.request(
            "getPinnedConversations",
            json!({ "account": "+15550000000" })
        )["result"],
        json!({ "pinned": [] })
    );

    // fake 写：成功回写后列表（空）。
    assert_eq!(
        engine.request(
            "setConversationPinned",
            json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact", "pinned": true }),
        )["result"],
        json!({ "pinned": [] })
    );

    // 参数缺失/未知 kind fail-closed：conversationId、kind、pinned、account。
    let (code, _) = engine.error_of("setConversationPinned", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationPinned",
        json!({ "conversationId": "x", "kind": "contact", "pinned": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationPinned",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "bogus", "pinned": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationPinned",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact" }),
    );
    assert_eq!(code, -32603);
}

/// 拉黑只读面（P5 扩面，Storage Service 真相源）：fake 模式空列表成功面 +
/// 参数面 fail-closed（-32603，storage 四码只覆盖链路，参数错误不占用；
/// real 模式未注册账号折叠见 real_mode_no_network_error_paths）。
#[test]
fn blocked_conversations_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("blocked-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 读：空列表成功。
    assert_eq!(
        engine.request(
            "getBlockedConversations",
            json!({ "account": "+15550000000" })
        )["result"],
        json!({ "blocked": [] })
    );

    // 缺 account / account 非字符串形状 → -32603。
    let (code, _) = engine.error_of("getBlockedConversations", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("getBlockedConversations", json!({ "account": 7 }));
    assert_eq!(code, -32603);
}

/// 贴纸包安装状态同步（P5 扩面，Storage Service 真相源）：fake 模式空列表
/// 成功面 + 参数面 fail-closed 逐形状（-32603，结构化码只覆盖 storage 链路，
/// 参数错误不占用；uninstall 忽略 packKey 内容与 position）。
#[test]
fn sticker_pack_sync_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("sticker-sync-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let pack_id = "ae8fedafda4768fd3384d4b3b9db963d";
    let pack_key = "U/Sqi5Xhwuda+rIyj+Z+ttev+81PUM1NqJ38Ml28c8o=";

    // fake 读：空列表成功。
    assert_eq!(
        engine.request("getStickerPackSyncs", json!({ "account": "+15550000000" }))["result"],
        json!({ "packs": [] })
    );

    // fake 写：合法 install 成功回写后列表（空）。
    assert_eq!(
        engine.request(
            "setStickerPackSync",
            json!({ "account": "+15550000000", "packId": pack_id, "packKey": pack_key, "installed": true }),
        )["result"],
        json!({ "packs": [] })
    );

    // uninstall：packKey 内容与 position 被忽略（墓碑形态两字段皆不写），
    // 垃圾字符串照样成功。
    assert_eq!(
        engine.request(
            "setStickerPackSync",
            json!({ "account": "+15550000000", "packId": pack_id, "packKey": "garbage", "position": 3, "installed": false }),
        )["result"],
        json!({ "packs": [] })
    );

    // 缺 account / 缺 packId / 缺 installed → -32603。
    let (code, _) = engine.error_of("getStickerPackSyncs", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("setStickerPackSync", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "packId": pack_id, "installed": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id }),
    );
    assert_eq!(code, -32603);

    // packId 大写 hex → -32603（官方 [0-9a-f]{32} 仅小写）。
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id.to_uppercase(), "installed": true }),
    );
    assert_eq!(code, -32603);

    // install 缺 packKey / 短 packKey / 非字符串 packKey → -32603。
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "installed": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "packKey": "short", "installed": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "packKey": 7, "installed": true }),
    );
    assert_eq!(code, -32603);

    // position 形状错（超 u32 / 非数值；uninstall 同样 fail-closed）→ -32603。
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "packKey": pack_key, "position": 4294967296u64, "installed": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "packKey": pack_key, "position": "high", "installed": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setStickerPackSync",
        json!({ "account": "+15550000000", "packId": pack_id, "position": true, "installed": false }),
    );
    assert_eq!(code, -32603);
}

/// 头像下行（getAvatar，KT 扩展方法）：fake 模式可达「无头像」结构化错误
/// 面（AVATAR_UNAVAILABLE，-1 + message 码位）；参数面 fail-closed -32603
///（缺参/形状错/会话标识垃圾形状，细节不外泄）。
#[test]
fn avatar_fake_unavailable_and_param_fail_closed() {
    let dir = temp_dir("avatar-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 合法参数（1:1 / 号码 / 群三形状同口径）→ AVATAR_UNAVAILABLE。
    let conversation_ids = [
        "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
        "+15551234567",
        // 32B master key 标准 base64（listGroups groupId 同口径，44 字符）。
        "pqampqampqampqampqampqampqampqampqampqampqY=",
    ];
    for conversation_id in conversation_ids {
        let (code, message) = engine.error_of(
            "getAvatar",
            json!({ "account": "+15550000000", "conversationId": conversation_id }),
        );
        assert_eq!((code, message.as_str()), (-1, "AVATAR_UNAVAILABLE"));
    }

    // 缺 account / 缺 conversationId / conversationId 垃圾形状 / account
    // 非字符串 → -32603（参数面不占用结构化码）。
    let (code, _) = engine.error_of("getAvatar", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("getAvatar", json!({ "account": "+15550000000" }));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "getAvatar",
        json!({ "account": "+15550000000", "conversationId": "contact:xyz" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "getAvatar",
        json!({ "account": 7, "conversationId": "+15551234567" }),
    );
    assert_eq!(code, -32603);
}

/// 会话拉黑写面（setConversationBlocked，KT 扩展方法）：fake 模式成功回写
/// 后 blocked 名单（空表）；参数面 fail-closed -32603（置顶面同款：kind
/// 缺失/未知、blocked 非布尔，storage 四码只覆盖链路）。
#[test]
fn conversation_blocked_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("blocked-write-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 写：contact / group / legacyGroup 三 kind 同构成功面。
    for kind in ["contact", "group", "legacyGroup"] {
        assert_eq!(
            engine.request(
                "setConversationBlocked",
                json!({ "account": "+15550000000", "conversationId": "x", "kind": kind, "blocked": true }),
            )["result"],
            json!({ "blocked": [] })
        );
    }

    // 缺 account / 未知 kind / 缺 blocked / blocked 非布尔 → -32603。
    let (code, _) = engine.error_of("setConversationBlocked", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationBlocked",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "bogus", "blocked": true }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationBlocked",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationBlocked",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact", "blocked": "yes" }),
    );
    assert_eq!(code, -32603);
}

/// 举报垃圾消息（reportSpam，KT 扩展方法）：fake 模式全量成功投影
///（reported = 批大小 + failures 空表）；参数面 fail-closed -32603：缺
/// targetAci / targetAci 非 UUID / serverGuids 缺失·非数组·空批·元素非
/// UUID（urn:uuid: 形态同堵）/超官方单批上限 1000 / token 非字符串形状。
#[test]
fn report_spam_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("report-spam-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let guid_a = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";
    let guid_b = "F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6";

    // fake 合法批：大小写混合 serverGuids → reported = 2 + failures 空。
    assert_eq!(
        engine.request(
            "reportSpam",
            json!({
                "account": "+15550000000",
                "targetAci": "11111111-2222-4333-8444-555555555555",
                "serverGuids": [guid_a, guid_b],
            })
        )["result"],
        json!({ "reported": 2, "failures": [] })
    );

    // token 缺省/null 同成功形状；token 在场（string）同形状。
    for token in [None, Some(Value::Null), Some(json!("report-token"))] {
        let mut params = json!({
            "account": "+15550000000",
            "targetAci": "11111111-2222-4333-8444-555555555555",
            "serverGuids": [guid_a],
        });
        if let Some(token) = token {
            params["token"] = token;
        }
        assert_eq!(
            engine.request("reportSpam", params)["result"],
            json!({ "reported": 1, "failures": [] })
        );
    }

    // 缺 targetAci / targetAci 非 UUID → -32603。
    let (code, _) = engine.error_of("reportSpam", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "serverGuids": [guid_a] }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "targetAci": "not-a-uuid", "serverGuids": [guid_a] }),
    );
    assert_eq!(code, -32603);

    // serverGuids 缺失 / 非数组 / 空批 / 元素非 UUID（含 urn:uuid: 与垃圾
    // 串，防路径注入）→ -32603。
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "targetAci": "11111111-2222-4333-8444-555555555555" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "targetAci": "11111111-2222-4333-8444-555555555555", "serverGuids": "6a2cb17f" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "targetAci": "11111111-2222-4333-8444-555555555555", "serverGuids": [] }),
    );
    assert_eq!(code, -32603);
    for bad_guid in ["garbage", "urn:uuid:6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"] {
        let (code, _) = engine.error_of(
            "reportSpam",
            json!({ "account": "+15550000000", "targetAci": "11111111-2222-4333-8444-555555555555", "serverGuids": [bad_guid] }),
        );
        assert_eq!(code, -32603);
    }
    // 元素非字符串（数字）→ -32603。
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({ "account": "+15550000000", "targetAci": "11111111-2222-4333-8444-555555555555", "serverGuids": [42] }),
    );
    assert_eq!(code, -32603);

    // 超官方单批上限（1000，reportSpamJobQueue zod max 同源）→ -32603。
    let oversized: Vec<String> = (0..1001)
        .map(|index| format!("{index:08x}-0000-4000-8000-000000000000"))
        .collect();
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({
            "account": "+15550000000",
            "targetAci": "11111111-2222-4333-8444-555555555555",
            "serverGuids": oversized,
        }),
    );
    assert_eq!(code, -32603);

    // token 非字符串形状 → -32603。
    let (code, _) = engine.error_of(
        "reportSpam",
        json!({
            "account": "+15550000000",
            "targetAci": "11111111-2222-4333-8444-555555555555",
            "serverGuids": [guid_a],
            "token": 42,
        }),
    );
    assert_eq!(code, -32603);
}

/// 群终止（terminateGroup，KT 扩展方法）：fake 模式成功投影
/// `{status: "terminated"}`（落调度日志，协议上行面纪律）；参数面
/// fail-closed（-32603：缺 account / 缺 groupId / groupId 非 base64 /
/// 长度 ≠32 字节）；real 模式未注册账号折叠 -32603。
#[test]
fn terminate_group_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("terminate-group-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    // 32 字节群 master key 的标准 base64（44 字符，listGroups id 同源）。
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：成功 {status:"terminated"}（不触网）。
    assert_eq!(
        engine.request(
            "terminateGroup",
            json!({ "account": "+15550000000", "groupId": group_id })
        )["result"],
        json!({ "status": "terminated" })
    );

    // 参数面 fail-closed 逐形状：缺 account / 缺 groupId / groupId 非
    // base64 / groupId 长度 ≠32 字节 → -32603。
    let rejects = [
        json!({ "groupId": group_id }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!" }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG" }),
    ];
    for params in &rejects {
        let response = engine.request("terminateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "terminateGroup rejects {params}"
        );
    }

    // real 模式未注册账号：storage 上下文打不开折叠 -32603（置顶面同款，
    // 细节不外泄）。
    let real_dir = temp_dir("terminate-group-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "terminateGroup",
        json!({ "account": "+15559990000", "groupId": group_id }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // 调度日志：groupId 逐字保留（协议上行面纪律）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["method"] == "terminateGroup")
        .expect("terminateGroup logged");
    assert_eq!(entry["params"]["groupId"], json!(group_id));
    assert_eq!(entry["params"]["account"], json!("+15550000000"));
}

/// 群消失计时器（updateGroup {expiration}，KT 扩展方法）：fake 模式成功
/// 投影 `{status: "sent"}`（落调度日志，协议上行面纪律，groupId 逐字保留）；
/// 参数面 fail-closed（-32603：缺 account / 缺 groupId / groupId 非 base64
/// / 长度 ≠32 字节 / expiration 缺席（无操作整拒）/ expiration 非 u32 形状
/// ——负数、小数、超界、字符串）；real 模式未注册账号折叠 -32603。
#[test]
fn update_group_expiration_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("update-group-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    // 32 字节群 master key 的标准 base64（44 字符，listGroups id 同源）。
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：设值与 0=关闭同形成功 {status:"sent"}（不触网）。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "expiration": 86_400 })
        )["result"],
        json!({ "status": "sent" })
    );
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "expiration": 0 })
        )["result"],
        json!({ "status": "sent" })
    );

    // 参数面 fail-closed 逐形状（expiration 缺席 = 无操作整拒）。
    let rejects = [
        json!({ "groupId": group_id, "expiration": 3600 }),
        json!({ "account": "+15550000000", "expiration": 3600 }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "expiration": 3600 }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG", "expiration": 3600 }),
        json!({ "account": "+15550000000", "groupId": group_id }),
        json!({ "account": "+15550000000", "groupId": group_id, "expiration": null }),
        json!({ "account": "+15550000000", "groupId": group_id, "expiration": "3600" }),
        json!({ "account": "+15550000000", "groupId": group_id, "expiration": -1 }),
        json!({ "account": "+15550000000", "groupId": group_id, "expiration": 1.5 }),
        json!({ "account": "+15550000000", "groupId": group_id, "expiration": 4_294_967_296_u64 }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：groupId 逐字保留（协议上行面纪律；被拒的调用同样落日志
    // ——记录的是派发，mutating 面同款——成功两条 + 拒绝十条 = 12 行）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let set_entries: Vec<&Value> = entries
        .iter()
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(set_entries.len(), 12, "成功两条 + 拒绝十条均落日志");
    assert_eq!(set_entries[0]["params"]["groupId"], json!(group_id));
    assert_eq!(set_entries[0]["params"]["expiration"], json!(86_400));
    assert_eq!(set_entries[0]["params"]["account"], json!("+15550000000"));
    assert_eq!(set_entries[1]["params"]["groupId"], json!(group_id));
    assert_eq!(set_entries[1]["params"]["expiration"], json!(0));

    // real 模式未注册账号：storage 上下文打不开折叠 -32603（terminateGroup
    // 同款，细节不外泄）。
    let real_dir = temp_dir("update-group-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({ "account": "+15559990000", "groupId": group_id, "expiration": 3600 }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

#[test]
fn mutating_methods_log_dispatch_and_reply() {
    let dir = temp_dir("dispatch-log");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();

    let sent = engine.request(
        "send",
        json!({ "account": number, "recipient": ["+15555550101"], "message": "hello" }),
    );
    assert!(sent["result"]["timestamp"].is_u64(), "timestamp is u64");
    assert_eq!(sent["result"]["results"], json!([]));

    assert_eq!(
        engine.request(
            "sendTyping",
            json!({ "account": number, "recipient": ["+15555550101"], "stop": false })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "sendReaction",
            json!({ "account": number, "recipient": ["+15555550101"], "targetAuthor": "+15555550101", "targetTimestamp": 42, "emoji": "👍", "remove": false })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "remoteDelete",
            json!({ "account": number, "targetTimestamp": 42, "recipient": ["+15555550101"] })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "updateContact",
            json!({ "account": number, "recipient": "+15555550101", "name": "Alice" })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "sendReadReceipt",
            json!({ "account": number, "recipient": "+15555550101", "timestamps": [42, 43] })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "sendViewedReceipt",
            json!({ "account": number, "recipient": "+15555550101", "timestamps": [44] })
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "sendDeliveryReceipt",
            json!({ "account": number, "recipient": "+15555550101", "timestamps": [45] })
        )["result"],
        json!({})
    );

    // send 扩面：quote/preview/mentions 键原样进调度日志（connector 上游
    // 同名直投的契约面）。
    let expanded = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "reply with quote",
            "quoteTimestamp": 42,
            "quoteAuthor": "+15555550101",
            "previewUrl": "https://example.com",
            "previewTitle": "Example",
            "mentions": [{ "number": "+15555550101", "start": 0, "length": 2 }],
            "attachments": ["data:text/plain;filename=hello%20world.txt;base64,aGVsbG8="]
        }),
    );
    assert!(expanded["result"]["timestamp"].is_u64());
    // 附件条目非字符串 fail-closed。
    let bad_attachment = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "bad attachment",
            "attachments": [42]
        }),
    );
    assert_eq!(bad_attachment["error"]["code"], -32603);
    // 半参引用 fail-closed（quoteTimestamp 无 quoteAuthor）。
    let half_quote = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "bad quote",
            "quoteTimestamp": 42
        }),
    );
    assert_eq!(half_quote["error"]["code"], -32603);

    // 调度日志：逐行 {method, params}，参数形状供 connector 侧依赖断言。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let methods: Vec<&str> = entries
        .iter()
        .map(|entry| entry["method"].as_str().expect("method"))
        .collect();
    assert_eq!(
        methods,
        [
            "send",
            "sendTyping",
            "sendReaction",
            "remoteDelete",
            "updateContact",
            "sendReadReceipt",
            "sendViewedReceipt",
            "sendDeliveryReceipt",
            "send",
            "send",
            "send"
        ]
    );
    assert_eq!(
        entries[0]["params"]["recipient"],
        json!(["+15555550101"]),
        "send carries recipient array"
    );
    assert_eq!(
        entries[1]["params"]["stop"],
        json!(false),
        "sendTyping carries explicit stop boolean"
    );
    assert_eq!(
        entries[4]["params"]["recipient"],
        json!("+15555550101"),
        "updateContact carries single-string recipient"
    );
    // 回执面：单作者 recipient（官方按作者分组直发）+ 时间戳数组原样透传。
    assert_eq!(
        entries[5]["params"]["recipient"],
        json!("+15555550101"),
        "receipt carries single-author recipient"
    );
    assert_eq!(
        entries[5]["params"]["timestamps"],
        json!([42, 43]),
        "receipt carries timestamps array"
    );
    assert_eq!(entries[6]["params"]["timestamps"], json!([44]));
    assert_eq!(entries[7]["params"]["timestamps"], json!([45]));
    // send 扩面键在调度日志逐字保留（半参失败那次也落日志，记录的是派发）。
    assert_eq!(entries[8]["params"]["quoteTimestamp"], json!(42));
    assert_eq!(entries[8]["params"]["quoteAuthor"], json!("+15555550101"));
    assert_eq!(
        entries[8]["params"]["previewUrl"],
        json!("https://example.com")
    );
    assert_eq!(
        entries[8]["params"]["mentions"],
        json!([{ "number": "+15555550101", "start": 0, "length": 2 }])
    );
    // 坏附件那次（entries[9]）：非字符串条目被拒但派发记录原样保留。
    assert_eq!(entries[9]["params"]["attachments"], json!([42]));
    // 半参引用（entries[10]）：quoteTimestamp 无 quoteAuthor，记录的是派发。
    assert_eq!(entries[10]["params"]["quoteTimestamp"], json!(42));
    assert!(entries[10]
        .get("params")
        .and_then(|p| p.get("quoteAuthor"))
        .is_none());
    // 附件：data URI 数组逐字进调度日志（connector 上游唯一形状）。
    assert_eq!(
        entries[8]["params"]["attachments"],
        json!(["data:text/plain;filename=hello%20world.txt;base64,aGVsbG8="])
    );

    // 贴纸扩面：sticker 对象逐字进调度日志；与文本互斥 fail-closed；缺字段
    // fail-closed（官方 sendStickerMessage：body=undefined attachments=[]）。
    let sticker_ok = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "sticker": {
                "packId": "0123456789abcdef",
                "packKey": "QUJDREVG",
                "stickerId": 7,
                "emoji": "😅",
                "image": "data:image/webp;base64,aGVsbG8=",
                "width": 512,
                "height": 512
            }
        }),
    );
    assert!(sticker_ok["result"]["timestamp"].is_u64());
    // 贴纸回应（官方 StagedStickerReply = 同一 DataMessage 带 sticker+quote，
    // 非 reaction）：quote 与 sticker 共存成功，构造面 quote 进
    // DataMessage.quote（author 不可解析才与文本分支同口径 -32603）。
    let sticker_with_quote = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "quoteTimestamp": 50,
            "quoteAuthor": "+15555550101",
            "sticker": {
                "packId": "0123456789abcdef",
                "packKey": "QUJDREVG",
                "stickerId": 7,
                "image": "data:image/webp;base64,aGVsbG8="
            }
        }),
    );
    assert!(sticker_with_quote["result"]["timestamp"].is_u64());
    let sticker_with_text = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "nope",
            "sticker": {
                "packId": "0123456789abcdef",
                "packKey": "QUJDREVG",
                "stickerId": 7,
                "image": "data:image/webp;base64,aGVsbG8="
            }
        }),
    );
    assert_eq!(sticker_with_text["error"]["code"], -32603);
    let sticker_missing = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "sticker": { "packId": "0123456789abcdef", "stickerId": 7 }
        }),
    );
    assert_eq!(sticker_missing["error"]["code"], -32603);

    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let last = entries.last().expect("sticker entries appended");
    assert_eq!(
        last["params"]["sticker"]["packId"],
        json!("0123456789abcdef")
    );
    assert_eq!(last["params"]["sticker"]["stickerId"], json!(7));
    // 带 message 的贴纸拒绝也落日志（记录的是派发）。
    let rejected = entries
        .iter()
        .rev()
        .find(|entry| entry["params"].get("message").is_some())
        .expect("rejected sticker send logged");
    assert_eq!(rejected["params"]["message"], json!("nope"));
    assert!(rejected["params"]["sticker"].is_object());
    // 贴纸+quote 共存：调度日志两者齐备（keys 与文本分支同名直投）。
    let with_quote = entries
        .iter()
        .rev()
        .find(|entry| {
            entry["params"].get("quoteTimestamp").is_some()
                && entry["params"]["sticker"].is_object()
        })
        .expect("sticker+quote send logged");
    assert_eq!(with_quote["params"]["quoteTimestamp"], json!(50));
    assert_eq!(with_quote["params"]["quoteAuthor"], json!("+15555550101"));
    assert_eq!(
        with_quote["params"]["sticker"]["packId"],
        json!("0123456789abcdef")
    );
}

/// 阅后即焚（E1-E4）：send 的 viewOnce 参数面逐形状 fail-closed + 合法面
/// 成功 + 调度日志 viewOnce 键逐字保留；sendViewOnceOpen fake 形状成功 +
/// 参数面 fail-closed（-32603，缺失/形状错不触网）。
#[test]
fn view_once_send_param_faces_and_send_view_once_open_contract() {
    let dir = temp_dir("view-once");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();
    let image = "data:image/jpeg;base64,aGVsbG8=";
    let sender_aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

    // 合法面：viewOnce + 单 image 附件（无 body）直聊成功。
    let ok = engine.request(
        "send",
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image] }),
    );
    assert!(ok["result"]["timestamp"].is_u64(), "view-once send ok");

    // 群组形态合法面成功（32B master key 标准 base64）。
    let group_ok = engine.request(
        "send",
        json!({
            "account": number,
            "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "viewOnce": true,
            "attachments": [image]
        }),
    );
    assert!(
        group_ok["result"]["timestamp"].is_u64(),
        "group view-once ok"
    );

    // viewOnce:false 显式回普通文本面（message 必带照旧）。
    let plain = engine.request(
        "send",
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": false, "message": "plain" }),
    );
    assert!(plain["result"]["timestamp"].is_u64());

    // fail-closed 逐形状（官方 isValidTapToView 同构 + mentions 禁发）：
    // 非布尔 / 带 body / 0 附件 / 2 附件 / audio 附件 / quote / preview /
    // mentions / sticker。
    let rejects = [
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": "yes", "attachments": [image] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "hi", "viewOnce": true, "attachments": [image] }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image, image] }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": ["data:audio/mp4;base64,aGVsbG8="] }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image], "quoteTimestamp": 42, "quoteAuthor": "+15555550101" }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image], "previewUrl": "https://example.com" }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image], "mentions": [{ "number": "+15555550101", "start": 0, "length": 2 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "viewOnce": true, "attachments": [image], "sticker": { "packId": "0123456789abcdef", "packKey": "QUJDREVG", "stickerId": 7, "image": "data:image/webp;base64,aGVsbG8=" } }),
    ];
    for params in &rejects {
        let response = engine.request("send", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "view-once rejects {params}"
        );
    }

    // sendViewOnceOpen：fake 成功 {}；参数面缺失/形状错 fail-closed。
    let opened = engine.request(
        "sendViewOnceOpen",
        json!({ "account": number, "senderAci": sender_aci, "timestamp": 42 }),
    );
    assert_eq!(opened["result"], json!({}));
    let open_rejects = [
        json!({ "senderAci": sender_aci, "timestamp": 42 }),
        json!({ "account": number, "timestamp": 42 }),
        json!({ "account": number, "senderAci": sender_aci }),
        json!({ "account": number, "senderAci": sender_aci, "timestamp": "42" }),
    ];
    for params in &open_rejects {
        let response = engine.request("sendViewOnceOpen", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendViewOnceOpen rejects {params}"
        );
    }

    // 调度日志：viewOnce 键逐字保留；sendViewOnceOpen 带完整三元组。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let view_once_entry = entries
        .iter()
        .find(|entry| {
            entry["method"] == "send" && entry["params"].get("viewOnce") == Some(&json!(true))
        })
        .expect("view-once send logged");
    assert_eq!(view_once_entry["params"]["viewOnce"], json!(true));
    assert_eq!(
        view_once_entry["params"]["attachments"],
        json!([image]),
        "single image attachment verbatim"
    );
    let open_entry = entries
        .iter()
        .find(|entry| entry["method"] == "sendViewOnceOpen")
        .expect("sendViewOnceOpen logged");
    assert_eq!(open_entry["params"]["senderAci"], json!(sender_aci));
    assert_eq!(open_entry["params"]["timestamp"], json!(42));
    assert_eq!(open_entry["params"]["account"], json!(number));
}

/// 通话消息面（sendCallMessage，KT 扩展批）：fake 形状成功 {"timestamp":u64}；
/// 参数面 fail-closed 逐形状（-32603：account/recipient/data 缺失、data 非
/// base64/空、urgency 非法/非字符串、destinationDeviceId 超 u32/非数值、
/// groupId 形态禁入——群组通话走 SFU 不在本面）；协议上行面落调度日志。
#[test]
fn call_message_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("call-message");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();

    let data = "c2lnbmFsLXJ0Yw=="; // base64("signal-rtc")

    // fake 成功面：直聊单收件人 {"timestamp": u64}；urgency/destinationDeviceId
    // 缺省（droppable/不定向）与显式同形。
    for params in [
        json!({ "account": number, "recipient": "+15555550101", "data": data }),
        json!({ "account": number, "recipient": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "data": data, "urgency": "handleImmediately" }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "urgency": "droppable", "destinationDeviceId": 2 }),
    ] {
        let ok = engine.request("sendCallMessage", params.clone());
        assert!(
            ok["result"]["timestamp"].is_u64(),
            "fake sendCallMessage ok: {params}"
        );
    }

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "recipient": "+15555550101", "data": data }),
        json!({ "account": number, "data": data }),
        json!({ "account": number, "recipient": "+15555550101" }),
        json!({ "account": number, "recipient": "+15555550101", "data": "not@base64!" }),
        json!({ "account": number, "recipient": "+15555550101", "data": "" }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "urgency": "urgent" }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "urgency": 1 }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "destinationDeviceId": 4_294_967_296u64 }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "destinationDeviceId": "2" }),
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "data": data }),
    ];
    for params in &rejects {
        let response = engine.request("sendCallMessage", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendCallMessage rejects {params}"
        );
    }

    // 调度日志：data/urgency/destinationDeviceId 逐字保留（协议上行面纪律）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["method"] == "sendCallMessage")
        .expect("sendCallMessage logged");
    assert_eq!(entry["params"]["data"], json!(data));
    assert_eq!(entry["params"]["recipient"], json!("+15555550101"));
    assert_eq!(entry["params"]["account"], json!(number));
    let explicit = entries
        .iter()
        .find(|entry| {
            entry["method"] == "sendCallMessage"
                && entry["params"].get("destinationDeviceId") == Some(&json!(2))
        })
        .expect("explicit urgency+device call logged");
    assert_eq!(explicit["params"]["urgency"], json!("droppable"));
    assert_eq!(explicit["params"]["destinationDeviceId"], json!(2));
}

/// setExpirationTimer（消失消息 E 批）：fake 形状成功 {"timestamp":u64}
/// （0=关闭同形）；参数面 fail-closed（-32603：account/recipient/
/// expirationInSeconds 缺失、recipient 数组形状、timer 字符串/负数/小数/
/// 超 u32——不触网）；调度日志三元组逐字保留。
#[test]
fn set_expiration_timer_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("expire-timer");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();

    // fake 成功面：{"timestamp": u64}；timer=0（关闭语义）同形成功。
    let ok = engine.request(
        "setExpirationTimer",
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": 86_400 }),
    );
    assert!(
        ok["result"]["timestamp"].is_u64(),
        "fake setExpirationTimer ok"
    );
    let off = engine.request(
        "setExpirationTimer",
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": 0 }),
    );
    assert!(off["result"]["timestamp"].is_u64(), "timer=0 (off) ok");

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "recipient": "+15555550101", "expirationInSeconds": 60 }),
        json!({ "account": number, "expirationInSeconds": 60 }),
        json!({ "account": number, "recipient": "+15555550101" }),
        json!({ "account": number, "recipient": ["+15555550101"], "expirationInSeconds": 60 }),
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": "60" }),
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": -1 }),
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": 1.5 }),
        json!({ "account": number, "recipient": "+15555550101", "expirationInSeconds": 4_294_967_296_u64 }),
    ];
    for params in &rejects {
        let response = engine.request("setExpirationTimer", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "setExpirationTimer rejects {params}"
        );
    }

    // 调度日志：account/recipient/expirationInSeconds 逐字保留。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["method"] == "setExpirationTimer")
        .expect("setExpirationTimer logged");
    assert_eq!(entry["params"]["account"], json!(number));
    assert_eq!(entry["params"]["recipient"], json!("+15555550101"));
    assert_eq!(entry["params"]["expirationInSeconds"], json!(86_400));
}

/// setAccountConfiguration（P5 扩面，官方 updateConfiguration 等价）：fake
/// 形状成功 `{}`（只写提供字段的三形态）+ 参数面 fail-closed 逐形状
/// （-32603：缺 account、布尔字段非布尔、phoneNumberSharingMode 非法/
/// 非字符串、零可写字段——不触网）。
#[test]
fn set_account_configuration_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("account-config");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();

    // fake 成功面：{}（回执开关双写）。
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "readReceipts": true, "typingIndicators": false }),
        )["result"],
        json!({})
    );
    // 手机号可见性两形态 + unlisted。
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "phoneNumberSharingMode": "everybody", "unlistedPhoneNumber": true }),
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "phoneNumberSharingMode": "nobody" }),
        )["result"],
        json!({})
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "readReceipts": true }),
        json!({ "account": number, "readReceipts": "yes" }),
        json!({ "account": number, "typingIndicators": 1 }),
        json!({ "account": number, "unlistedPhoneNumber": "false" }),
        json!({ "account": number, "phoneNumberSharingMode": "contactsOnly" }),
        json!({ "account": number, "phoneNumberSharingMode": "" }),
        json!({ "account": number, "phoneNumberSharingMode": 1 }),
        json!({ "account": number }),
        json!({}),
    ];
    for params in &rejects {
        let response = engine.request("setAccountConfiguration", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "setAccountConfiguration rejects {params}"
        );
    }
}

/// setProfile（个人资料写面，官方 updateProfile 等价）：fake 形状成功
/// `{"status":"saved"}`（文字面/头像 data URI/null 三态）+ 参数面
/// fail-closed 逐形状（-32603：缺 account、givenName 缺/空/非字符串、可选
/// 文本字段非字符串、字节预算超限（given 129B/about 513B/emoji 33B）、
/// avatar 非图像 data URI/非字符串——不触网）+ fake 纪律：不落调度日志，
/// 零头像/密文字节外泄。
#[test]
fn set_profile_fake_shapes_and_param_fail_closed() {
    use base64::Engine as _;
    let dir = temp_dir("set-profile");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();
    let avatar = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(b"\x89PNG-fake")
    );

    // fake 成功面：{"status":"saved"}（文字面/换新头像/移除头像三形态）。
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "familyName": "Agent", "about": "hi", "emoji": "🤖" }),
        )["result"],
        json!({ "status": "saved" })
    );
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "avatar": avatar }),
        )["result"],
        json!({ "status": "saved" })
    );
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "avatar": null }),
        )["result"],
        json!({ "status": "saved" })
    );

    // 参数面 fail-closed 逐形状（含字节预算：129/513/33 字节各键超限）。
    let max_ok = "g".repeat(128);
    let over = |n: usize, ch: char| ch.to_string().repeat(n);
    let rejects = [
        json!({ "givenName": "KT" }),
        json!({ "account": number }),
        json!({ "account": number, "givenName": "" }),
        json!({ "account": number, "givenName": 7 }),
        json!({ "account": number, "givenName": "KT", "familyName": 7 }),
        json!({ "account": number, "givenName": "KT", "about": [] }),
        json!({ "account": number, "givenName": "KT", "emoji": true }),
        json!({ "account": number, "givenName": "KT", "familyName": over(129, 'f') }),
        json!({ "account": number, "givenName": "KT", "about": over(513, 'a') }),
        json!({ "account": number, "givenName": "KT", "emoji": over(33, 'e') }),
        json!({ "account": number, "givenName": max_ok, "avatar": "data:text/plain;base64,aGk=" }),
        json!({ "account": number, "givenName": "KT", "avatar": "data:image/png,notbase64payload" }),
        json!({ "account": number, "givenName": "KT", "avatar": "https://example.invalid/a.png" }),
        json!({ "account": number, "givenName": "KT", "avatar": 7 }),
    ];
    for params in &rejects {
        let response = engine.request("setProfile", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "setProfile rejects {params}"
        );
    }

    // fake 纪律：不落调度日志；头像 data URI（含 base64 载荷）零字节进日志。
    let log_path = dir.join("engine-send-log.jsonl");
    assert!(
        !log_path.exists(),
        "setProfile must not write dispatch log entries"
    );
}
#[test]
fn set_profile_avatar_uri_never_reaches_log() {
    use base64::Engine as _;
    let dir = temp_dir("set-profile-log");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();
    let avatar_bytes = b"\x89PNG-fake-avatar-payload";
    let avatar = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(avatar_bytes)
    );
    let response = engine.request(
        "setProfile",
        json!({ "account": number, "givenName": "KT", "avatar": avatar }),
    );
    assert_eq!(response["result"], json!({ "status": "saved" }));

    // 零头像字节纪律：数据目录全部文本工件扫不到 data URI 载荷与 base64
    // 形（fake 模式不上行不落盘，日志面同样零字节）。
    let mut scanned = 0;
    for entry in std::fs::read_dir(&dir).expect("data dir readable") {
        let path = entry.expect("entry").path();
        if path.is_file() {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            assert!(
                !text.contains("aVBORw0KGgo") && !text.contains("data:image"),
                "avatar bytes leaked into {}",
                path.display()
            );
            scanned += 1;
        }
    }
    assert!(scanned >= 1, "no files scanned in data dir");
}

/// setUniversalExpireTimer（P5 扩面，官方 universal timer 写面）：fake 形状
/// 成功 `{}`（0=关闭同形）+ 参数面 fail-closed 逐形状（-32603：缺 account/
/// expirationInSeconds、timer 字符串/负数/小数/超 u32——不触网）。
#[test]
fn set_universal_expire_timer_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("universal-timer");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .unwrap()
        .to_owned();

    // fake 成功面：{}（设值与 0=关闭两形态）。
    assert_eq!(
        engine.request(
            "setUniversalExpireTimer",
            json!({ "account": number, "expirationInSeconds": 86_400 }),
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "setUniversalExpireTimer",
            json!({ "account": number, "expirationInSeconds": 0 }),
        )["result"],
        json!({})
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "expirationInSeconds": 60 }),
        json!({ "account": number }),
        json!({ "account": number, "expirationInSeconds": "60" }),
        json!({ "account": number, "expirationInSeconds": -1 }),
        json!({ "account": number, "expirationInSeconds": 1.5 }),
        json!({ "account": number, "expirationInSeconds": 4_294_967_296_u64 }),
    ];
    for params in &rejects {
        let response = engine.request("setUniversalExpireTimer", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "setUniversalExpireTimer rejects {params}"
        );
    }
}

#[test]
fn read_only_stubs_and_user_status_shapes() {
    let dir = temp_dir("read-only");
    // 桩形状契约在 fake 模式钉定（real 模式 listContacts/listGroups 已真实，
    // 未注册账号走 -32603，见 real_mode_no_network_error_paths）。
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    assert_eq!(
        engine.request("listContacts", json!({ "account": "+15550000000" }))["result"],
        json!([])
    );
    assert_eq!(
        engine.request("listGroups", json!({ "account": "+15550000000" }))["result"],
        json!([])
    );
    let status = engine.request(
        "getUserStatus",
        json!({ "account": "+15550000000", "recipient": "+15555550101" }),
    );
    assert_eq!(status["result"], json!({ "isRegistered": true }));
}

/// 备份导出消费面（compat §5 注册块 + 契约 §10.4/§10.7）：参数面一律
/// fail-closed -32603（缺 account / 未注册 / fake 账号）；结构化码 -1 需要
/// 真工件与真 presage 材料，由 backup_task 单元测试覆盖；backup.* 命名
/// 空间外动词仍 -32601。
#[test]
fn backup_export_param_faces_fail_closed() {
    // real 模式：缺 account 与未注册账号，两方法都 -32603。
    let dir = temp_dir("backup-export-faces");
    let mut engine = Engine::spawn(&dir);
    let (code, message) = engine.error_of("backup.export", json!({}));
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    let (code, message) = engine.error_of("backup.exportStatus", json!({}));
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    let (code, _) = engine.error_of("backup.export", json!({ "account": "+15550000000" }));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("backup.exportStatus", json!({ "account": "+15550000000" }));
    assert_eq!(code, -32603);

    // fake 模式链接出的账号：backup.export 无真 AEP/工件可读，同样
    // -32603（fake 账号不进入真导出任务）。
    let fake_dir = temp_dir("backup-export-fake");
    let mut fake_engine = Engine::spawn_with_env(&fake_dir, FAKE_SERVICE);
    let started = fake_engine.request("startLink", json!({}));
    let uri = started["result"]["deviceLinkUri"]
        .as_str()
        .expect("deviceLinkUri string")
        .to_owned();
    let finished = fake_engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    );
    let number = finished["result"]["number"]
        .as_str()
        .expect("number string")
        .to_owned();
    let (code, message) = fake_engine.error_of("backup.export", json!({ "account": number }));
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));

    // backup.* 命名空间内未注册动词与任意未知方法同面。
    let (code, message) = engine.error_of("backup.exportX", json!({ "account": "+15550000000" }));
    assert_eq!((code, message.as_str()), (-32601, "Method not found"));
}

#[test]
fn unknown_method_is_rejected_32601() {
    let dir = temp_dir("unknown-method");
    let mut engine = Engine::spawn(&dir);
    // 夹具控制面动词对引擎同样是未知方法（compat §4）。
    let (code, _) = engine.error_of("emitReceive", json!({}));
    assert_eq!(code, -32601);
    let (code, message) = engine.error_of("no.such.method", json!({}));
    assert_eq!(code, -32601);
    assert_eq!(message, "Method not found");
}

#[test]
fn malformed_and_overlong_lines_do_not_kill_the_process() {
    let dir = temp_dir("bad-lines");
    let mut engine = Engine::spawn(&dir);

    writeln!(engine.stdin, "not-json").expect("raw line written");
    let response = engine.next_response_of_frame(String::new());
    assert_eq!(response["error"]["code"], json!(-32700));

    // 超限行（> 1 MiB）：回 -32700 后进程继续服务。
    let blob = "x".repeat(1024 * 1024 + 16);
    writeln!(engine.stdin, "{blob}").expect("overlong line written");
    let response = engine.next_response_of_frame(String::new());
    assert_eq!(response["error"]["code"], json!(-32700));

    assert_eq!(
        engine.request("listAccounts", json!({}))["result"],
        json!([])
    );
}

#[test]
fn stdin_eof_exits_zero() {
    let dir = temp_dir("eof");
    let mut child = Command::new(env!("CARGO_BIN_EXE_kt-signal-engine"))
        .arg("--data-dir")
        .arg(&dir)
        .arg("jsonRpc")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("engine spawns");
    drop(child.stdin.take());
    let output = child.wait_with_output().expect("engine exits");
    assert_eq!(output.status.code(), Some(0), "EOF is a clean exit");
}

/// storage.write（P0，KT 扩展方法，compat §3 storage.write 行 + §5「storage
/// 写面 P0」块）：fake 成功形状桩（version 0、逐条回显）+ 参数面 fail-closed
/// 逐形状（-32603：缺 accountId/空/非串、缺 updates/空批/非数组、条目非
/// 对象、type 未知、key 非 base64/15B、INSERT 缺 record、UPDATE record
/// null、DELETE 带 record、record 垃圾/Account arm、批量重复 key）；不落
/// 调度日志（storage 状态面纪律）；real 模式未注册账号 -32603。
#[test]
fn storage_write_fake_shapes_and_param_fail_closed() {
    use base64::Engine as _;
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let dir = temp_dir("storage-write");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let uri = engine.request("startLink", json!({}))["result"]["deviceLinkUri"]
        .as_str()
        .expect("deviceLinkUri string")
        .to_owned();
    let number = engine.request(
        "finishLink",
        json!({ "deviceLinkUri": uri, "deviceName": "KT" }),
    )["result"]["number"]
        .as_str()
        .expect("number string")
        .to_owned();

    // fake 成功面：{version: 0, results: [...]}，INSERT/UPDATE 回显原 key
    // 为 writtenKey，DELETE 无 writtenKey（桩不模拟幂等 no-op 标注）。
    let key_insert = b64(&[0x01; 16]);
    let key_update = b64(&[0x02; 16]);
    let key_delete = b64(&[0x03; 16]);
    assert_eq!(
        engine.request(
            "storage.write",
            json!({
                "accountId": number,
                "updates": [
                    { "type": "INSERT", "key": key_insert, "record": "CgA=" },
                    { "type": "UPDATE", "key": key_update, "record": "CgA=" },
                    { "type": "DELETE", "key": key_delete },
                ],
            })
        )["result"],
        json!({
            "version": 0,
            "results": [
                { "type": "INSERT", "key": key_insert, "writtenKey": key_insert },
                { "type": "UPDATE", "key": key_update, "writtenKey": key_update },
                { "type": "DELETE", "key": key_delete },
            ],
        })
    );

    // 参数面 fail-closed 逐形状（fake 模式同样全量过闸）。
    let rejects = [
        json!({ "accountId": number }),
        json!({ "accountId": number, "updates": [] }),
        json!({ "accountId": number, "updates": "nope" }),
        json!({ "updates": [{ "type": "DELETE", "key": key_delete }] }),
        json!({ "accountId": "", "updates": [{ "type": "DELETE", "key": key_delete }] }),
        json!({ "accountId": 7, "updates": [{ "type": "DELETE", "key": key_delete }] }),
        json!({ "accountId": number, "updates": [7] }),
        json!({ "accountId": number, "updates": [{ "type": "UPSERT", "key": key_delete }] }),
        json!({ "accountId": number, "updates": [{ "type": "DELETE" }] }),
        json!({ "accountId": number, "updates": [{ "type": "DELETE", "key": "!!!not-base64!!!" }] }),
        json!({ "accountId": number, "updates": [{ "type": "DELETE", "key": b64(&[0x09; 15]) }] }),
        json!({ "accountId": number, "updates": [{ "type": "INSERT", "key": key_insert }] }),
        json!({ "accountId": number, "updates": [{ "type": "UPDATE", "key": key_update, "record": serde_json::Value::Null }] }),
        json!({ "accountId": number, "updates": [{ "type": "DELETE", "key": key_delete, "record": "CgA=" }] }),
        json!({ "accountId": number, "updates": [{ "type": "INSERT", "key": key_insert, "record": "AAAA" }] }),
        // Account arm（field 4 LEN 0）归专用写面：通用写面拒。
        json!({ "accountId": number, "updates": [{ "type": "INSERT", "key": key_insert, "record": "IgA=" }] }),
        // 批量内 key 重复。
        json!({
            "accountId": number,
            "updates": [
                { "type": "DELETE", "key": key_delete },
                { "type": "DELETE", "key": key_delete },
            ],
        }),
    ];
    for params in &rejects {
        let response = engine.request("storage.write", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "storage.write rejects {params}"
        );
    }

    // fake 纪律：mutating 写面不落调度日志。
    let log_path = dir.join("engine-send-log.jsonl");
    assert!(
        !log_path.exists(),
        "storage.write must not write dispatch log entries"
    );

    // real 模式未注册账号：storage 上下文打不开（UnknownAccount）→ 引擎
    // 内部错误折叠（不触网）。
    let real_dir = temp_dir("storage-write-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "storage.write",
        json!({
            "accountId": "+15559990000",
            "updates": [{ "type": "INSERT", "key": key_insert, "record": "CgA=" }],
        }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}
