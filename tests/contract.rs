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

    // 未注册账号的 send + textStyle：send 系同折叠（不触网；textStyle 透传
    // 批，契约 1.69 §4.62）。
    let (code, message) = engine.error_of(
        "send",
        json!({
            "account": "+15559990000",
            "recipient": ["+15555550101"],
            "message": "hi",
            "textStyle": [{ "style": "BOLD", "start": 0, "length": 2 }]
        }),
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

/// 自建群（createGroup，KT 扩展方法，官方 createGroupV2 行为对齐）：fake
/// 模式形状桩 `{groupId(随机 32B master key 标准 base64), revision: 0}`
///（落调度日志，avatar 键摘要化）；avatar data URI 臂失败
/// GROUP_AVATAR_UPLOAD_FAILED（fake 无 CDN 上行面，updateGroup 两臂纪律）；
/// 参数面 fail-closed（-32603：缺 account / title 缺席·空串·非字符串 /
/// expiration 非法形状 / members 非数组·空数组·条目非字符串 / avatar 非
/// image 族·坏 base64）；real 模式未注册账号折叠 -32603。
#[test]
fn create_group_fake_shape_param_fail_closed_and_log_redaction() {
    use base64::Engine as _;
    let dir = temp_dir("create-group-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let avatar = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(b"\x89PNG-fake")
    );

    // fake 合法面：title 必填（官方 createGroupV2 name 必填同位），members
    // / expiration / avatar 缺省 → 形状桩 {groupId, revision: 0}。
    let created = engine.request(
        "createGroup",
        json!({ "account": "+15550000000", "title": "作战室" }),
    )["result"]
        .clone();
    let group_id = created["groupId"].as_str().expect("groupId string");
    assert_eq!(group_id.len(), 44, "32B master key 标准 base64");
    base64::engine::general_purpose::STANDARD
        .decode(group_id)
        .expect("groupId decodes");
    assert_eq!(created["revision"], json!(0));

    // expiration / avatar=null 臂同形成功。
    assert_eq!(
        engine.request(
            "createGroup",
            json!({ "account": "+15550000000", "title": "作战室", "expiration": 86_400, "avatar": null })
        )["result"]["revision"],
        json!(0)
    );

    // fake 失败臂：avatar data URI（换新，需上行链）→
    // GROUP_AVATAR_UPLOAD_FAILED data {retryable:true, textSaved:false}。
    let response = engine.request(
        "createGroup",
        json!({ "account": "+15550000000", "title": "作战室", "avatar": avatar }),
    );
    assert_eq!(response["error"]["code"], -1);
    assert_eq!(response["error"]["message"], "GROUP_AVATAR_UPLOAD_FAILED");
    assert_eq!(
        response["error"]["data"],
        json!({ "retryable": true, "textSaved": false })
    );

    // 参数面 fail-closed 逐形状（-32603）。
    let rejects = [
        json!({ "title": "作战室" }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "title": "" }),
        json!({ "account": "+15550000000", "title": 7 }),
        json!({ "account": "+15550000000", "title": "作战室", "expiration": "3600" }),
        json!({ "account": "+15550000000", "title": "作战室", "expiration": -1 }),
        json!({ "account": "+15550000000", "title": "作战室", "members": "not-array" }),
        json!({ "account": "+15550000000", "title": "作战室", "members": [] }),
        json!({ "account": "+15550000000", "title": "作战室", "members": [7] }),
        json!({ "account": "+15550000000", "title": "作战室", "avatar": "data:text/plain;base64,aGk=" }),
        json!({ "account": "+15550000000", "title": "作战室", "avatar": "data:image/png;base64,###" }),
    ];
    for params in &rejects {
        let response = engine.request("createGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "createGroup rejects {params}"
        );
    }

    // 调度日志：title/members 键逐字保留（协议上行面纪律），avatar 键
    // 形状摘要化（data URI 载荷零字节落盘）；被拒调用同样落日志。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .filter(|entry: &Value| entry["method"] == "createGroup")
        .collect();
    assert_eq!(
        entries.len(),
        14,
        "成功两条 + 失败一条 + 拒绝十一条均落日志"
    );
    assert_eq!(entries[0]["params"]["title"], json!("作战室"));
    assert!(
        entries[0]["params"]["members"].is_null(),
        "members 缺省原样保留"
    );
    let redacted = &entries[2]["params"]["avatar"];
    assert!(
        redacted.is_string()
            && redacted
                .as_str()
                .expect("avatar digest")
                .starts_with("[avatar data uri: "),
        "avatar key digested in dispatch log: {redacted}"
    );
    assert!(
        !log.contains("iVBO"),
        "avatar base64 payload leaked into log"
    );

    // real 模式未注册账号：storage 上下文打不开折叠 -32603（terminateGroup
    // 同款，细节不外泄）。
    let real_dir = temp_dir("create-group-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "createGroup",
        json!({ "account": "+15559990000", "title": "作战室" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
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

/// updateGroup 群头像上行面（KT 扩展参数族，官方 buildUpdateAttributesChange
/// 行为对齐）：fake 两臂——avatar 缺省/null 臂成功 `{status:"sent"}`（PATCH
/// 形状桩）、data URI 臂失败 `GROUP_AVATAR_UPLOAD_FAILED`（data
/// `{retryable:true, textSaved:false}`，合成单 change 语义下伴生面未提交
/// ——fake 无 CDN/服务器上行面，上行链不可达即错误面，stickers 先例）。
/// 参数面 fail-closed（-32603：avatar 非 image 族/非 base64 形状/坏 base64/
/// 超 10MiB/非字符串、expiration 与 avatar 双缺、groupId 形状坏）+ 零可写
/// 放开边界（avatar=null 单独在场合法）+ 调度日志 avatar 键形状摘要化
///（data URI/base64 载荷零字节落盘，groupId/expiration 键照旧逐字保留）。
#[test]
fn update_group_avatar_fake_arms_param_fail_closed_and_log_redaction() {
    use base64::Engine as _;
    let dir = temp_dir("update-group-avatar");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";
    let avatar = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(b"\x89PNG-fake")
    );

    // fake 成功臂：avatar=null（移除，无上行链环节）+ expiration 同调/单独
    // 在场均成功 {status:"sent"}。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "avatar": null, "expiration": 86_400 })
        )["result"],
        json!({ "status": "sent" })
    );
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "avatar": null })
        )["result"],
        json!({ "status": "sent" })
    );

    // fake 失败臂：avatar data URI（换新，需上行链）→ GROUP_AVATAR_UPLOAD_
    // FAILED data {retryable:true, textSaved:false}（单独在场与同调同臂）。
    for params in [
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": avatar }),
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": avatar, "expiration": 3600 }),
    ] {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(response["error"]["code"], -1, "upload arm errors {params}");
        assert_eq!(
            response["error"]["message"], "GROUP_AVATAR_UPLOAD_FAILED",
            "upload arm code {params}"
        );
        assert_eq!(
            response["error"]["data"],
            json!({ "retryable": true, "textSaved": false }),
            "upload arm partial-success data {params}"
        );
    }

    // 参数面 fail-closed 逐形状（-32603）。注：解码超 10MiB 的形状在本引擎
    // IPC 面不可达——入站帧闸（1MiB 行帽，main.rs MAX_LINE_BYTES）先以
    // parse error 拒收 >1MiB 的请求帧；超帽拒绝闸由 profile.rs 单测
    // avatar_data_uri_parse_faces 钉定（parse_avatar_data_uri 同一函数，
    // 群面/个人面共用）。
    let rejects = [
        // 非 image 族。
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": "data:text/plain;base64,aGk=" }),
        // 非 base64 形状（无 base64 标志）/ 坏 base64 / 空载荷。
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": "data:image/png,notbase64payload" }),
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": "data:image/png;base64,###" }),
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": "data:image/png;base64," }),
        // 非字符串。
        json!({ "account": "+15550000000", "groupId": group_id, "avatar": 7 }),
        // 双缺（expiration 与 avatar 都不在场）。
        json!({ "account": "+15550000000", "groupId": group_id }),
        // avatar 在场时 groupId 形状照旧闸（非 base64 / ≠32B）。
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "avatar": null }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG", "avatar": null }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：avatar data URI 键形状摘要化（零 data URI/base64 字节落盘），
    // groupId/expiration 键照旧逐字保留；成功臂 + 失败臂 + 拒绝臂均落日志
    //（mutating 面同款：成功 2 + 失败 2 + 拒绝 8 = 12 行 updateGroup 条目）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(entries.len(), 12, "成功两条 + 失败两条 + 拒绝八条均落日志");
    assert!(
        !log.contains("data:image") && !log.contains("iVBO"),
        "avatar data URI/base64 payload leaked into dispatch log"
    );
    let redacted = &entries[2]["params"]["avatar"];
    assert!(
        redacted.is_string()
            && redacted
                .as_str()
                .expect("avatar digest")
                .starts_with("[avatar data uri: "),
        "avatar key digested in dispatch log: {redacted}"
    );
    assert_eq!(entries[0]["params"]["groupId"], json!(group_id));
    assert_eq!(entries[0]["params"]["expiration"], json!(86_400));
    assert!(entries[0]["params"]["avatar"].is_null(), "null 臂原样保留");

    // real 模式未注册账号 + avatar 在场：storage 上下文打不开折叠 -32603
    //（参数闸全过后才触服务层，fake 之外的形状不再特判）。
    let real_dir = temp_dir("update-group-avatar-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({ "account": "+15559990000", "groupId": group_id, "avatar": null }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// updateGroup 群名写面（title 参数族，官方 buildUpdateAttributesChange
/// `if (title)` 行为对齐，加密 blob 上行）：fake 成功 `{status:"sent"}`；
/// title 与 expiration 同调组合合法；参数面 fail-closed（-32603：title
/// null/非字符串/空串——官方空串 falsy no-op 语义不静默收——title 在场时
/// groupId/account 形状照旧闸）；调度日志 title 键明文逐字保留（群名短标
/// 签非敏感载荷，avatar 摘要化纪律不外溢）。
#[test]
fn update_group_title_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("update-group-title");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：title 单独在场 / 与 expiration 同调均成功 {status:"sent"}。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "title": "新群名" })
        )["result"],
        json!({ "status": "sent" })
    );
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({ "account": "+15550000000", "groupId": group_id, "title": "更名", "expiration": 3600 })
        )["result"],
        json!({ "status": "sent" })
    );

    // 参数面 fail-closed 逐形状（null/非字符串/空串；groupId/account 闸照旧）。
    let rejects = [
        json!({ "account": "+15550000000", "groupId": group_id, "title": null }),
        json!({ "account": "+15550000000", "groupId": group_id, "title": 7 }),
        json!({ "account": "+15550000000", "groupId": group_id, "title": "" }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "title": "名" }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG", "title": "名" }),
        json!({ "groupId": group_id, "title": "名" }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：title 键明文逐字保留（成功 2 + 拒绝 6 = 8 行均落日志）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(entries.len(), 8, "成功两条 + 拒绝六条均落日志");
    assert_eq!(entries[0]["params"]["title"], json!("新群名"));
    assert_eq!(entries[0]["params"]["groupId"], json!(group_id));

    // real 模式未注册账号 + title 在场：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("update-group-title-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({ "account": "+15559990000", "groupId": group_id, "title": "名" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// updateGroup 加成员写面（addMembers 参数族，官方 buildAddMembersChange
/// 行为对齐）：fake 成功 `{status:"sent"}`（fake 无解析链，形状桩）；与
/// title 同调组合合法（合成单 change 语义）；参数面 fail-closed（-32603：
/// addMembers 非数组/空数组/条目非字符串，成员不可解析整拒——官方桌面缺
/// 本地联系人静默跳过是 GUI 语义，API 面不静默，addMembers 在场时
/// groupId/account 形状照旧闸）；调度日志 addMembers 键照旧逐字保留。
#[test]
fn update_group_add_members_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("update-group-add-members");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：addMembers 单独在场 / 与 title 同调均成功 {status:"sent"}。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "addMembers": ["+15550000001", "11111111-2222-4333-8444-555555555555"]
            })
        )["result"],
        json!({ "status": "sent" })
    );
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "addMembers": ["+15550000002"],
                "title": "扩员"
            })
        )["result"],
        json!({ "status": "sent" })
    );

    // 参数面 fail-closed 逐形状（非数组/空数组/条目非字符串；groupId/account
    // 闸照旧）。
    let rejects = [
        json!({ "account": "+15550000000", "groupId": group_id, "addMembers": "+15550000001" }),
        json!({ "account": "+15550000000", "groupId": group_id, "addMembers": [] }),
        json!({ "account": "+15550000000", "groupId": group_id, "addMembers": [7] }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "addMembers": ["+15550000001"] }),
        json!({ "groupId": group_id, "addMembers": ["+15550000001"] }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：addMembers 键照旧逐字保留（成功 2 + 拒绝 5 = 7 行均落日志）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(entries.len(), 7, "成功两条 + 拒绝五条均落日志");
    assert_eq!(
        entries[0]["params"]["addMembers"],
        json!(["+15550000001", "11111111-2222-4333-8444-555555555555"])
    );
    assert_eq!(entries[1]["params"]["title"], json!("扩员"));

    // real 模式未注册账号 + addMembers 在场：storage 上下文打不开折叠
    // -32603。
    let real_dir = temp_dir("update-group-add-members-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({
            "account": "+15559990000",
            "groupId": group_id,
            "addMembers": ["+15550000001"]
        }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// updateGroup 移除成员写面（removeMembers 参数族，官方
/// buildDeleteMemberChange/buildDeletePendingMemberChange 行为对齐）：fake
/// 成功 `{status:"sent"}`（fake 无解析链/群态读面，形状桩）；参数面
/// fail-closed（-32603：removeMembers 非数组/空数组/条目非字符串，
/// removeMembers 在场时 groupId/account 形状照旧闸）；调度日志
/// removeMembers 键照旧逐字保留。
#[test]
fn update_group_remove_members_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("update-group-remove-members");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：removeMembers 单独在场成功 {status:"sent"}。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "removeMembers": ["+15550000001"]
            })
        )["result"],
        json!({ "status": "sent" })
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "account": "+15550000000", "groupId": group_id, "removeMembers": "+15550000001" }),
        json!({ "account": "+15550000000", "groupId": group_id, "removeMembers": [] }),
        json!({ "account": "+15550000000", "groupId": group_id, "removeMembers": [7] }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "removeMembers": ["+15550000001"] }),
        json!({ "groupId": group_id, "removeMembers": ["+15550000001"] }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：removeMembers 键照旧逐字保留（成功 1 + 拒绝 5 = 6 行）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(entries.len(), 6, "成功一条 + 拒绝五条均落日志");
    assert_eq!(
        entries[0]["params"]["removeMembers"],
        json!(["+15550000001"])
    );

    // real 模式未注册账号 + removeMembers 在场：storage 上下文打不开折叠
    // -32603。
    let real_dir = temp_dir("update-group-remove-members-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({
            "account": "+15559990000",
            "groupId": group_id,
            "removeMembers": ["+15550000001"]
        }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// leaveGroup（群退出，KT 扩展方法，官方桌面 leaveGroupV2 行为对齐）：fake
/// 成功 `{status:"left"}`（形状桩，不触网）；参数面 fail-closed（-32603：
/// 缺 account / 缺 groupId / groupId 非 base64 / ≠32B）；调度日志 groupId
/// 逐字保留；real 模式未注册账号折叠 -32603。
#[test]
fn leave_group_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("leave-group-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    assert_eq!(
        engine.request(
            "leaveGroup",
            json!({ "account": "+15550000000", "groupId": group_id })
        )["result"],
        json!({ "status": "left" })
    );

    let rejects = [
        json!({ "groupId": group_id }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!" }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG" }),
    ];
    for params in &rejects {
        let response = engine.request("leaveGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "leaveGroup rejects {params}"
        );
    }

    // 调度日志：groupId 逐字保留（成功 1 + 拒绝 4 = 5 行）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "leaveGroup")
        .collect();
    assert_eq!(entries.len(), 5, "成功一条 + 拒绝四条均落日志");
    assert_eq!(entries[0]["params"]["groupId"], json!(group_id));

    // real 模式未注册账号：storage 上下文打不开折叠 -32603（terminateGroup
    // 同款）。
    let real_dir = temp_dir("leave-group-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "leaveGroup",
        json!({ "account": "+15559990000", "groupId": group_id }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// call-link 凭据材料面（calls.* 五方法，KT 扩展，P4c 切一）：fake 模式形状
/// 桩（桩字节非有效 zk 材料，仅钉契约形状）+ 参数闸 fail-closed（-32603：
/// rootKey 非 base64/非 16·21B、roomId 非 base64/≠32B、groupId 同
/// terminateGroup 闸）；材料供给面非协议上行——不落调度日志。
#[test]
fn call_link_materials_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("call-link-materials-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    // rootKey = RingRTC CallLinkRootKey 两版本定长（V0 16B / V1 21B）；
    // roomId = derive_room_id 定长 32B；groupId = 32B master key（同
    // terminateGroup 口径）。
    let root_key_v0 = "EREREREREREREREREREREQ==";
    let root_key_v1 = "ERERERERERERERERERERERERERER";
    let root_key_15b = "ERERERERERERERERERER";
    let room_id = "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=";
    let room_id_16b = "IiIiIiIiIiIiIiIiIiIiIg==";
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 合法面：五方法形状桩（不触网）。
    assert_eq!(
        engine.request(
            "calls.linkPresentation",
            json!({ "account": "+15550000000", "rootKey": root_key_v0 })
        )["result"],
        json!({ "presentation": "a3QtZmFrZS1jYWxsLWxpbmstcHJlc2VudGF0aW9u" })
    );
    assert_eq!(
        engine.request(
            "calls.createLinkAuth",
            json!({ "account": "+15550000000", "rootKey": root_key_v1, "roomId": room_id })
        )["result"],
        json!({
            "presentation": "a3QtZmFrZS1jcmVhdGUtbGluay1wcmVzZW50YXRpb24=",
            "callLinkPublicParams": "a3QtZmFrZS1jYWxsLWxpbmstcHVibGljLXBhcmFtcw==",
        })
    );
    assert_eq!(
        engine.request(
            "calls.groupProof",
            json!({ "account": "+15550000000", "groupId": group_id })
        )["result"],
        json!({ "proof": "kt-fake-group-proof" })
    );
    assert_eq!(
        engine.request(
            "calls.groupMembers",
            json!({ "account": "+15550000000", "groupId": group_id })
        )["result"],
        json!({ "members": [] })
    );
    // endorsementsPublicKey：常量派生面，fake 同码路真实答——键在场、非空
    // 字符串、跨调用确定。
    let first = engine.request("calls.endorsementsPublicKey", json!({}));
    let second = engine.request("calls.endorsementsPublicKey", json!({}));
    let key = first["result"]["endorsementPublicKey"]
        .as_str()
        .expect("endorsementPublicKey 字符串");
    assert!(!key.is_empty(), "常量派生非空");
    assert_eq!(first["result"], second["result"], "常量面跨调用确定");

    // 参数面 fail-closed 逐形状 → -32603。
    let rejects = [
        ("calls.linkPresentation", json!({ "rootKey": root_key_v0 })),
        (
            "calls.linkPresentation",
            json!({ "account": "+15550000000" }),
        ),
        (
            "calls.linkPresentation",
            json!({ "account": "+15550000000", "rootKey": "not-base64!!" }),
        ),
        (
            "calls.linkPresentation",
            json!({ "account": "+15550000000", "rootKey": root_key_15b }),
        ),
        (
            "calls.linkPresentation",
            json!({ "account": "+15550000000", "rootKey": room_id }),
        ),
        (
            "calls.createLinkAuth",
            json!({ "account": "+15550000000", "rootKey": root_key_v0 }),
        ),
        (
            "calls.createLinkAuth",
            json!({ "account": "+15550000000", "roomId": room_id }),
        ),
        (
            "calls.createLinkAuth",
            json!({ "account": "+15550000000", "rootKey": root_key_v0, "roomId": room_id_16b }),
        ),
        (
            "calls.createLinkAuth",
            json!({ "account": "+15550000000", "rootKey": root_key_15b, "roomId": room_id }),
        ),
        ("calls.groupProof", json!({ "groupId": group_id })),
        (
            "calls.groupProof",
            json!({ "account": "+15550000000", "groupId": "not-base64!!" }),
        ),
        (
            "calls.groupProof",
            json!({ "account": "+15550000000", "groupId": "QUJDREVG" }),
        ),
        ("calls.groupMembers", json!({ "groupId": group_id })),
        (
            "calls.groupMembers",
            json!({ "account": "+15550000000", "groupId": "QUJDREVG" }),
        ),
    ];
    for (method, params) in &rejects {
        let response = engine.request(method, params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "{method} rejects {params}"
        );
    }

    // 材料供给面非协议上行：不落调度日志（五个方法全部调用后日志文件仍
    // 不存在）。
    let log_path = dir.join("engine-send-log.jsonl");
    assert!(
        !log_path.exists(),
        "calls.* 材料面不落调度日志（compat §5 call-link 块纪律）"
    );
}

/// calls.* real 模式未注册账号：四方法（带 account 面）折叠 -32603（storage
/// 上下文打不开，细节不外泄，不触网）；endorsementsPublicKey 无 account 面
/// 常量派生照答。
#[test]
fn call_link_materials_real_unregistered_account() {
    let real_dir = temp_dir("call-link-materials-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let root_key = "EREREREREREREREREREREQ==";
    let room_id = "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=";
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    for (method, params) in [
        (
            "calls.linkPresentation",
            json!({ "account": "+15559990000", "rootKey": root_key }),
        ),
        (
            "calls.createLinkAuth",
            json!({ "account": "+15559990000", "rootKey": root_key, "roomId": room_id }),
        ),
        (
            "calls.groupProof",
            json!({ "account": "+15559990000", "groupId": group_id }),
        ),
        (
            "calls.groupMembers",
            json!({ "account": "+15559990000", "groupId": group_id }),
        ),
    ] {
        let (code, message) = real_engine.error_of(method, params);
        assert_eq!(
            (code, message.as_str()),
            (-32603, "Internal error"),
            "{method} 未注册账号折叠 -32603"
        );
    }

    let response = real_engine.request("calls.endorsementsPublicKey", json!({}));
    assert!(
        response["result"]["endorsementPublicKey"]
            .as_str()
            .is_some_and(|key| !key.is_empty()),
        "常量派生面不依赖账号"
    );
}

#[test]
fn send_story_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("send-story-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 合法批：成功形状 {timestamp, results: []}（send 系 fake 纪律：
    // 参数闸全量生效后成功桩，results 恒空）。全参数在场（previewUrl/
    // previewTitle 2026-10-09 扩面，双在场合法形态）。
    assert_eq!(
        engine
            .request(
                "sendStory",
                json!({
                    "account": "+15550000000",
                    "message": "story day",
                    "recipients": ["+15555550101", "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"],
                    "distributionListId": "00000000-0000-0000-0000-000000000000",
                    "allowsReplies": true,
                    "textStyle": "BOLD",
                    "bodyRanges": [{ "style": "BOLD", "start": 0, "length": 5 }],
                    "previewImage": "data:image/png;base64,iVBORw0KGgo=",
                    "previewUrl": "https://example.com/a",
                    "previewTitle": "Example"
                })
            )
            .pointer("/result/results"),
        Some(&json!([]))
    );
    // 缺省面：可缺参数全部缺席同形状（MY_STORY_ID 缺省 + 线上缺席位）。
    let minimal = engine.request(
        "sendStory",
        json!({ "account": "+15550000000", "message": "plain", "recipients": ["+15555550101"] }),
    );
    assert!(minimal["result"]["timestamp"].is_u64(), "timestamp is u64");
    assert_eq!(minimal["result"]["results"], json!([]));

    // 参数闸 fail-closed（-32603，compat §3 sendStory 行错误列）：
    // account/message 缺失或空串。
    for account in [None, Some(json!(""))] {
        let mut params = json!({ "message": "story", "recipients": ["+15555550101"] });
        if let Some(account) = account {
            params["account"] = account;
        }
        let (code, _) = engine.error_of("sendStory", params);
        assert_eq!(code, -32603);
    }
    for message in [None, Some(json!(""))] {
        let mut params = json!({ "account": "+15550000000", "recipients": ["+15555550101"] });
        if let Some(message) = message {
            params["message"] = message;
        }
        let (code, _) = engine.error_of("sendStory", params);
        assert_eq!(code, -32603);
    }
    // recipients：缺失/空批/单字符串形态（story 面恒数组）/条目非字符串/
    // 空串条目。
    let (code, _) = engine.error_of(
        "sendStory",
        json!({ "account": "+15550000000", "message": "story" }),
    );
    assert_eq!(code, -32603);
    for recipients in [json!([]), json!("+15555550101"), json!([42]), json!([""])] {
        let (code, _) = engine.error_of(
            "sendStory",
            json!({ "account": "+15550000000", "message": "story", "recipients": recipients }),
        );
        assert_eq!(code, -32603);
    }
    // distributionListId：空串/非字符串。
    for list_id in [json!(""), json!(42)] {
        let (code, _) = engine.error_of(
            "sendStory",
            json!({
                "account": "+15550000000",
                "message": "story",
                "recipients": ["+15555550101"],
                "distributionListId": list_id
            }),
        );
        assert_eq!(code, -32603);
    }
    // allowsReplies 非布尔。
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "allowsReplies": "yes"
        }),
    );
    assert_eq!(code, -32603);
    // textStyle：六值词表外（含 send 面内联样式词与大小写漂移）/非字符串。
    for style in ["ITALIC", "SPOILER", "bold", ""] {
        let (code, _) = engine.error_of(
            "sendStory",
            json!({
                "account": "+15550000000",
                "message": "story",
                "recipients": ["+15555550101"],
                "textStyle": style
            }),
        );
        assert_eq!(code, -32603);
    }
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "textStyle": 42
        }),
    );
    assert_eq!(code, -32603);
    // bodyRanges：条目缺字段/词表外（DEFAULT 是正文样式词不是内联样式）/
    // >64 条（cap 镜像整拒不截断）/非数组。
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "bodyRanges": [{ "style": "BOLD", "start": 0 }]
        }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "bodyRanges": [{ "style": "DEFAULT", "start": 0, "length": 1 }]
        }),
    );
    assert_eq!(code, -32603);
    let overflow: Vec<Value> = (0..65)
        .map(|index| json!({ "style": "BOLD", "start": index, "length": 1 }))
        .collect();
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "bodyRanges": overflow
        }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "bodyRanges": "BOLD"
        }),
    );
    assert_eq!(code, -32603);
    // previewImage 非字符串。
    let (code, _) = engine.error_of(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "previewImage": 42
        }),
    );
    assert_eq!(code, -32603);

    // previewUrl/previewTitle 参数闸（2026-10-09 扩面）：url 族——非字符串/
    // 空串/非 https（官方 isValidLink https-only 同构）/超 2048 字符；
    // title 族——非字符串/空串/超 1024 UTF-8 字节（342×「中」= 1026 字节，
    // 字符数 342 在帽内——钉字节面不是字符面）。
    for preview_url in [
        json!(42),
        json!(""),
        json!("http://example.com/a"),
        json!(format!("https://{}", "a".repeat(2041))),
    ] {
        let (code, _) = engine.error_of(
            "sendStory",
            json!({
                "account": "+15550000000",
                "message": "story",
                "recipients": ["+15555550101"],
                "previewUrl": preview_url
            }),
        );
        assert_eq!(code, -32603);
    }
    for preview_title in [
        json!(42),
        json!(""),
        json!("a".repeat(1025)),
        json!("中".repeat(342)),
    ] {
        let (code, _) = engine.error_of(
            "sendStory",
            json!({
                "account": "+15550000000",
                "message": "story",
                "recipients": ["+15555550101"],
                "previewTitle": preview_title
            }),
        );
        assert_eq!(code, -32603);
    }
    // 边界内合法：url 恰 2048 字符 + title 恰 1024 字节（双在场）成功；
    // previewUrl 单独在场（无 previewImage）亦成功——两键互相独立可选。
    let boundary = engine.request(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "previewUrl": format!("https://{}", "a".repeat(2040)),
            "previewTitle": "a".repeat(1024)
        }),
    );
    assert!(boundary["result"]["timestamp"].is_u64(), "timestamp is u64");
    let url_only = engine.request(
        "sendStory",
        json!({
            "account": "+15550000000",
            "message": "story",
            "recipients": ["+15555550101"],
            "previewUrl": "https://example.com/a"
        }),
    );
    assert_eq!(url_only["result"]["results"], json!([]));

    // 调度日志纪律：成功与被拒调用都落日志（send 系同款，updateGroup
    // 同构断言）；params 形状供 connector 侧依赖断言。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let story_entries: Vec<&Value> = entries
        .iter()
        .filter(|entry| entry["method"] == "sendStory")
        .collect();
    assert_eq!(
        story_entries.len(),
        34,
        "成功四条 + 拒绝三十条均落日志（append_dispatch 在参数闸之前）"
    );
    assert_eq!(
        story_entries[0]["params"]["recipients"],
        json!(["+15555550101", "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"])
    );
    assert_eq!(
        story_entries[0]["params"]["previewUrl"],
        json!("https://example.com/a"),
        "previewUrl/previewTitle 原样进日志（connector 侧依赖断言）"
    );
    assert_eq!(story_entries[0]["params"]["previewTitle"], json!("Example"));
    assert_eq!(
        story_entries[1]["params"]["distributionListId"],
        json!(null),
        "缺省参数原样进日志（缺省值归服务层）"
    );

    // real 模式未注册账号：折叠 -32603 不触网（send 系同折叠）。
    let real_dir = temp_dir("send-story-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "sendStory",
        json!({ "account": "+15559990000", "message": "story", "recipients": ["+15555550101"] }),
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

/// send textStyle 透传面（connector 契约 1.69 §4.62）：fake 合法面成功 +
/// 调度日志 textStyle 数组逐字保留（connector 上游同名直投的 IPC 形状锚）；
/// 参数面 fail-closed 逐形状（-32603：词表外（含 NONE——官方 composer 永不出
/// 的 no-op 标记）、条目缺字段、start/length 超 u32、>64 条、textStyle 非
/// 数组）；直聊/群组两形态；零回归锚 = 无参数时调度日志与旧版同形状。
#[test]
fn send_text_style_param_faces_and_dispatch_log() {
    let dir = temp_dir("text-style");
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

    // 合法面：直聊多条目（覆盖五值词表中 3 值 + offset 组合）。
    let ok = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "styled body",
            "textStyle": [
                { "style": "BOLD", "start": 0, "length": 6 },
                { "style": "SPOILER", "start": 7, "length": 4 },
                { "style": "MONOSPACE", "start": 0, "length": 11 }
            ]
        }),
    );
    assert!(ok["result"]["timestamp"].is_u64(), "styled send ok");

    // 群组形态合法面（32B master key 标准 base64）。
    let group_ok = engine.request(
        "send",
        json!({
            "account": number,
            "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "message": "group styled",
            "textStyle": [{ "style": "ITALIC", "start": 0, "length": 5 }]
        }),
    );
    assert!(group_ok["result"]["timestamp"].is_u64(), "group styled ok");

    // 空数组 = 无样式（合法，键保留原样落日志）。
    let empty = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "empty list",
            "textStyle": []
        }),
    );
    assert!(empty["result"]["timestamp"].is_u64(), "empty list ok");

    // fail-closed 逐形状。
    let rejects = [
        // 词表外值。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "MAGIC", "start": 0, "length": 1 }] }),
        // NONE：官方 composer 永不发出的 no-op 标记（词表外）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "NONE", "start": 0, "length": 1 }] }),
        // 小写词表（上游枚举面大小写敏感）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "bold", "start": 0, "length": 1 }] }),
        // 条目缺 style / 缺 start / 缺 length。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "start": 0, "length": 1 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "BOLD", "length": 1 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "BOLD", "start": 0 }] }),
        // start / length 超 u32。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "BOLD", "start": 4294967296u64, "length": 1 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": "BOLD", "start": 0, "length": 4294967296u64 }] }),
        // style 非字符串。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": [{ "style": 1, "start": 0, "length": 1 }] }),
        // textStyle 非数组。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": "BOLD:0:1" }),
        // >64 条（receive cap 镜像，不静默截断）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "textStyle": (0..65).map(|i| json!({ "style": "BOLD", "start": i, "length": 1 })).collect::<Vec<_>>() }),
    ];
    for params in &rejects {
        let response = engine.request("send", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "textStyle rejects {params}"
        );
    }

    // 零回归锚：无 textStyle 参数的 send 照旧成功（上游字节与旧版一致）。
    let plain = engine.request(
        "send",
        json!({ "account": number, "recipient": ["+15555550101"], "message": "plain" }),
    );
    assert!(plain["result"]["timestamp"].is_u64());

    // 调度日志：textStyle 数组逐字保留（IPC 形状锚——connector upstream
    // params 的 `textStyle` 键原样进引擎、原样落日志）；非法 send 的派发
    // 也落日志（记录的是派发，sticker 先例同款）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let styled = entries
        .iter()
        .find(|entry| {
            entry["params"]
                .get("textStyle")
                .is_some_and(|t| t.as_array().is_some_and(|a| a.len() == 3))
        })
        .expect("styled send logged");
    assert_eq!(
        styled["params"]["textStyle"],
        json!([
            { "style": "BOLD", "start": 0, "length": 6 },
            { "style": "SPOILER", "start": 7, "length": 4 },
            { "style": "MONOSPACE", "start": 0, "length": 11 }
        ]),
        "textStyle array verbatim"
    );
    let rejected = entries
        .iter()
        .find(|entry| {
            entry["params"]
                .get("textStyle")
                .is_some_and(|t| t.as_array().is_some_and(|a| a.len() == 65))
        })
        .expect("rejected over-cap send logged");
    assert_eq!(
        rejected["params"]["textStyle"].as_array().unwrap().len(),
        65
    );
}

/// 通话消息面（sendCallMessage，KT 扩展批）：fake 形状成功 {"timestamp":u64}；
/// 参数面 fail-closed 逐形状（-32603：account/recipient/data 缺失、data 非
/// base64/空、urgency 非法/非字符串、destinationDeviceId 超 u32/非数值、
/// groupId 键禁入——wire 群参数名是 `group`）；群臂（P4b 群信令出网批）：
/// `group`（32B master key 标准 base64）与 `recipient` 互斥恰一，fake 成功
/// {timestamp} 与 1:1 同口径，伪群 id（非法 base64/≠32B）/同置/legacy 或
/// destinationDeviceId 与 group 同置全拒；协议上行面落调度日志。
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
                                   // 32 字节群 master key 的标准 base64（44 字符，terminateGroup/updateGroup
                                   // 的 groupId 同源口径）。
    let group = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 成功面：直聊单收件人 {"timestamp": u64}；urgency/destinationDeviceId
    // 缺省（droppable/不定向）与显式同形。群臂同口径（sender-key 群发面）。
    for params in [
        json!({ "account": number, "recipient": "+15555550101", "data": data }),
        json!({ "account": number, "recipient": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "data": data, "urgency": "handleImmediately" }),
        json!({ "account": number, "recipient": "+15555550101", "data": data, "urgency": "droppable", "destinationDeviceId": 2 }),
        json!({ "account": number, "group": group, "data": data }),
        json!({ "account": number, "group": group, "data": data, "urgency": "handleImmediately" }),
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
        json!({ "account": number, "groupId": group, "data": data }),
        // 群臂闸：group/recipient 同置；伪群 id（非法 base64/≠32B/空串/非
        // 字符串）；legacy 裸帧族群向（官方群信令恒 opaque）；设备定向位群向
        //（sender-key 群发无 per-device 寻址）。
        json!({ "account": number, "recipient": "+15555550101", "group": group, "data": data }),
        json!({ "account": number, "group": "not-base64!!", "data": data }),
        json!({ "account": number, "group": "QUJDREVG", "data": data }),
        json!({ "account": number, "group": "", "data": data }),
        json!({ "account": number, "group": 42, "data": data }),
        json!({ "account": number, "group": group, "data": data, "legacy": { "kind": "busy", "id": 7 } }),
        json!({ "account": number, "group": group, "data": data, "destinationDeviceId": 2 }),
    ];
    for params in &rejects {
        let response = engine.request("sendCallMessage", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendCallMessage rejects {params}"
        );
    }

    // 调度日志：data/urgency/destinationDeviceId/group 逐字保留（协议上行
    // 面纪律）。
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
    let group_entry = entries
        .iter()
        .find(|entry| {
            entry["method"] == "sendCallMessage" && entry["params"].get("group").is_some()
        })
        .expect("group arm call logged");
    assert_eq!(group_entry["params"]["group"], json!(group));
    assert!(
        group_entry["params"].get("recipient").is_none(),
        "群臂 wire 面无 recipient 键"
    );
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

/// 云备份 upload 消费面（compat §3 注册块 + 契约 §6.2/§6.5）：参数面一律
/// fail-closed（缺 account -32603；未注册/fake 账号 -1 BACKUP_AUTH_FAILED，
/// restore 同口径）；uploadStatus 是进程内观测面（缺席={present:false}，
/// 不泄露注册状态）；backups.* 命名空间外动词仍 -32601。结构化上传链
/// （form gRPC/TUS/超帽）需要真凭据与真网络，由 cloud_upload 单元测试与
/// 契约 §6.4 诚实边界覆盖。
#[test]
fn cloud_upload_param_faces_fail_closed() {
    // real 模式：缺 account 两方法 -32603；未注册账号 uploadToCloud
    // -1 BACKUP_AUTH_FAILED（storage_context 失败同步终态）。
    let dir = temp_dir("cloud-upload-faces");
    let mut engine = Engine::spawn(&dir);
    let (code, message) = engine.error_of("backups.uploadToCloud", json!({}));
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    let (code, message) = engine.error_of("backups.uploadStatus", json!({}));
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
    let (code, message) = engine.error_of(
        "backups.uploadToCloud",
        json!({ "account": "+15550000000" }),
    );
    assert_eq!((code, message.as_str()), (-1, "BACKUP_AUTH_FAILED"));

    // fake 模式链接出的账号：无真 store/AEP 可读，同未注册口径
    // -1 BACKUP_AUTH_FAILED（fake 账号不进入真上传链）。
    let fake_dir = temp_dir("cloud-upload-fake");
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
    let (code, message) =
        fake_engine.error_of("backups.uploadToCloud", json!({ "account": number }));
    assert_eq!((code, message.as_str()), (-1, "BACKUP_AUTH_FAILED"));

    // uploadStatus：未注册/fake 账号同样答 {present:false}（快照面是
    // 进程内观测，不泄露注册状态）。
    assert_eq!(
        engine.request("backups.uploadStatus", json!({ "account": "+15550000000" }))["result"],
        json!({ "present": false })
    );
    assert_eq!(
        fake_engine.request("backups.uploadStatus", json!({ "account": number }))["result"],
        json!({ "present": false })
    );

    // backups.* 命名空间内未注册动词与任意未知方法同面。
    let (code, message) = engine.error_of("backups.uploadX", json!({ "account": "+15550000000" }));
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

/// sendMessageRequestResponse 写面（connector §4.56 债务批，compat §3/§5）：
/// 上游钉定面 params `{account, recipient?|groupId?, type}`——type 官方词汇
/// 白名单（signal-cli cli.rs:664-669 只收 "accept"/"delete"，全枚举面为官方
/// 桌面 syncMessageRequestResponse 的六值；UNKNOWN=0 一并拒——官方无发送
/// UNKNOWN 语义）、两腿恰一（direct = recipient 单值、group = groupId 32B
/// 标准 base64，connector prepare_conversations_message_request 同形状）。
/// fake 模式合法 ACCEPT/DELETE/BLOCK 各一成功 `{}`（上游成功答空结果对象
/// 同纪律）；形状面 fail-closed `-32603`；协议上行面落调度日志（send 系同
/// 纪律）；real 模式未注册账号折叠 `-32603` 不触网。
#[test]
fn send_message_request_response_faces_and_param_fail_closed() {
    use base64::Engine as _;
    let group_id = base64::engine::general_purpose::STANDARD.encode([0x11u8; 32]);
    let dir = temp_dir("mrr-send");
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

    // fake 合法面：direct/腿 + 群腿、ACCEPT/DELETE/BLOCK 各一（上游成功
    // 答空结果对象；block 族为官方桌面六值枚举的线面能力，connector 主用
    // accept/delete）。
    for params in [
        json!({ "account": number, "recipient": "+15555550101", "type": "accept" }),
        json!({
            "account": number,
            "recipient": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
            "type": "delete"
        }),
        json!({ "account": number, "groupId": group_id, "type": "block" }),
        json!({ "account": number, "groupId": group_id, "type": "blockAndDelete" }),
        json!({ "account": number, "groupId": group_id, "type": "spam" }),
        json!({ "account": number, "groupId": group_id, "type": "blockAndSpam" }),
    ] {
        let ok = engine.request("sendMessageRequestResponse", params.clone());
        assert_eq!(
            ok["result"],
            json!({}),
            "fake sendMessageRequestResponse ok: {params}"
        );
    }

    // 参数面 fail-closed 逐形状：缺 type/UNKNOWN=0/越界数字/词表外字符串/
    // 两腿同置/两腿双缺/腿形状非法（群非 base64、群 ≠32B、recipient 数组）。
    let rejects = [
        json!({ "account": number, "recipient": "+15555550101" }),
        json!({ "account": number, "recipient": "+15555550101", "type": "unknown" }),
        json!({ "account": number, "recipient": "+15555550101", "type": 0 }),
        json!({ "account": number, "recipient": "+15555550101", "type": 1 }),
        json!({ "account": number, "recipient": "+15555550101", "type": 7 }),
        json!({ "account": number, "recipient": "+15555550101", "type": "ACCEPT" }),
        json!({ "account": number, "recipient": "+15555550101", "type": "nope" }),
        json!({ "account": number, "recipient": "+15555550101", "type": true }),
        json!({
            "account": number,
            "recipient": "+15555550101",
            "groupId": group_id,
            "type": "accept"
        }),
        json!({ "account": number, "type": "accept" }),
        json!({
            "account": number,
            "recipient": "+15555550101",
            "groupId": "!!!not-base64!!!",
            "type": "accept"
        }),
        json!({
            "account": number,
            "recipient": "+15555550101",
            "groupId": base64::engine::general_purpose::STANDARD.encode([0x22u8; 31]),
            "type": "accept"
        }),
        json!({ "account": number, "recipient": ["+15555550101"], "type": "accept" }),
    ];
    for params in &rejects {
        let response = engine.request("sendMessageRequestResponse", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendMessageRequestResponse rejects {params}"
        );
    }

    // 调度日志：type/腿参数逐字保留（协议上行面纪律，send 系同款）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let direct = entries
        .iter()
        .find(|entry| {
            entry["method"] == "sendMessageRequestResponse"
                && entry["params"].get("recipient").is_some()
        })
        .expect("direct-leg MRR logged");
    assert_eq!(direct["params"]["type"], json!("accept"));
    assert_eq!(direct["params"]["recipient"], json!("+15555550101"));
    let group = entries
        .iter()
        .find(|entry| {
            entry["method"] == "sendMessageRequestResponse"
                && entry["params"].get("groupId").is_some()
        })
        .expect("group-leg MRR logged");
    assert_eq!(group["params"]["type"], json!("block"));
    assert_eq!(group["params"]["groupId"], json!(group_id));

    // real 模式未注册账号：打开已注册 manager 前置即内部失败（不触网；
    // send 系同折叠）。
    let real_dir = temp_dir("mrr-send-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "sendMessageRequestResponse",
        json!({ "account": "+15559990000", "recipient": "+15555550101", "type": "accept" }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// getUsername（用户名读面，KT 扩展方法）：fake 形状桩 `{}`（未设 = 键
/// 缺席，不发明 null/空串）+ 参数面 fail-closed（缺 account / 非字符串
/// -32603）+ real 模式未注册账号 -32603（不触网，注册数据前置）。
#[test]
fn get_username_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("get-username");
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

    // fake：未设用户名 = 空投影（两键均缺席）。
    assert_eq!(
        engine.request("getUsername", json!({ "account": number }))["result"],
        json!({})
    );

    // 参数面 fail-closed。
    assert_eq!(
        engine.error_of("getUsername", json!({})),
        (-32603, "Internal error".to_owned())
    );
    assert_eq!(
        engine.error_of("getUsername", json!({ "account": 7 })),
        (-32603, "Internal error".to_owned())
    );

    // real 模式未注册账号：注册数据前置即内部失败（不触网）。
    let real_dir = temp_dir("get-username-real");
    let mut real_engine = Engine::spawn(&real_dir);
    assert_eq!(
        real_engine.error_of("getUsername", json!({ "account": "+15559990000" })),
        (-32603, "Internal error".to_owned())
    );
}

/// usernameLookup（用户名解析读面，KT 扩展方法）：fake 逐条回显
/// `{recipient, isRegistered:false}`（输入序，不发明 username/uuid 键）+
/// 参数面 fail-closed（usernames 缺席/非数组/空批/超 20/条目非字符串/空/
/// 超 64 字符 -32603）+ real 模式未注册账号 -32603（不触网）。
#[test]
fn username_lookup_fake_echo_and_param_fail_closed() {
    let dir = temp_dir("username-lookup");
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

    // fake：逐条回显（输入序），单键形状。
    assert_eq!(
        engine.request(
            "usernameLookup",
            json!({ "account": number, "usernames": ["kt_pay.01", "https://signal.me/#eu/AAA"] }),
        )["result"],
        json!([
            { "recipient": "kt_pay.01", "isRegistered": false },
            { "recipient": "https://signal.me/#eu/AAA", "isRegistered": false },
        ])
    );

    // 参数面 fail-closed 逐形状。
    let over_cap = vec!["kt_pay.01"; 21];
    let rejects = [
        json!({ "account": number }),
        json!({ "account": number, "usernames": "kt_pay.01" }),
        json!({ "account": number, "usernames": [] }),
        json!({ "account": number, "usernames": over_cap }),
        json!({ "account": number, "usernames": [7] }),
        json!({ "account": number, "usernames": [""] }),
        json!({ "account": number, "usernames": ["k".repeat(65)] }),
    ];
    for params in rejects {
        assert_eq!(
            engine.error_of("usernameLookup", params),
            (-32603, "Internal error".to_owned()),
            "param face must reject"
        );
    }

    // 边界内放行：恰 20 条、恰 64 字符条目（fake 回显面）。
    let at_cap = ["kt_pay.01"; 20].to_vec();
    assert_eq!(
        engine.request(
            "usernameLookup",
            json!({ "account": number, "usernames": at_cap }),
        )["result"]
            .as_array()
            .unwrap()
            .len(),
        20
    );
    let wide = vec!["k".repeat(64)];
    assert_eq!(
        engine.request(
            "usernameLookup",
            json!({ "account": number, "usernames": wide }),
        )["result"][0]["recipient"],
        json!("k".repeat(64))
    );

    // real 模式未注册账号：账号解析前置即内部失败（不触网）。
    let real_dir = temp_dir("username-lookup-real");
    let mut real_engine = Engine::spawn(&real_dir);
    assert_eq!(
        real_engine.error_of(
            "usernameLookup",
            json!({ "account": "+15559990000", "usernames": ["kt_pay.01"] }),
        ),
        (-32603, "Internal error".to_owned())
    );
}

/// 用户名写面（usernameReserve/usernameConfirm/usernameDelete，KT 扩展
/// 方法）：fake 本地形状桩（语法/hash/link 加解密走同一 crate 面，无网络）
/// 与参数面 fail-closed 逐形状（-32603）、服务器态结构化码位的 fake 不可达
/// 面（fake 恒成功形状）、real 模式未注册账号 -32603（不触网）。
#[test]
fn username_write_faces_fake_shapes_and_param_fail_closed() {
    use base64::Engine as _;
    let dir = temp_dir("username-write");
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

    // reserve fake：全名 = 单候选原样回 + 32B hash（b64url）；裸昵称 =
    // 候选器首项（nick.DD 形态）。
    let reserved = engine.request(
        "usernameReserve",
        json!({ "account": number, "username": "KtPay.07" }),
    )["result"]
        .clone();
    assert_eq!(reserved["username"], json!("KtPay.07"));
    let hash = reserved["usernameHash"].as_str().expect("hash string");
    assert_eq!(
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(hash)
            .expect("b64url")
            .len(),
        32
    );

    let bare = engine.request(
        "usernameReserve",
        json!({ "account": number, "username": "kt_pay" }),
    )["result"]
        .clone();
    let bare_name = bare["username"].as_str().expect("candidate");
    assert!(
        bare_name.starts_with("kt_pay."),
        "candidate shape: {bare_name}"
    );
    assert!(usernames::Username::new(bare_name).is_ok());

    // confirm fake：reserve 采纳面回传（fake 不校 hash 值，只校形状）→
    // `{username, usernameLink}`，链接为完整 signal.me 形态。
    let confirmed = engine.request(
        "usernameConfirm",
        json!({
            "account": number,
            "username": bare_name,
            "usernameHash": bare["usernameHash"],
        }),
    )["result"]
        .clone();
    assert_eq!(confirmed["username"], json!(bare_name));
    let link = confirmed["usernameLink"].as_str().expect("link");
    assert!(link.starts_with("https://signal.me/#eu/"), "link: {link}");

    // delete fake：`{}`。
    assert_eq!(
        engine.request("usernameDelete", json!({ "account": number }))["result"],
        json!({})
    );

    // 参数面 fail-closed 逐形状（reserve 闸最严，confirm 校双参数形状，
    // delete 只校 account）。
    let reserve_rejects = [
        json!({ "account": number }),
        json!({ "account": number, "username": "" }),
        json!({ "account": number, "username": "k".repeat(39) }),
        json!({ "account": number, "username": 7 }),
        json!({ "username": "kt_pay.01" }),
        json!({ "account": number, "username": "ab.01" }),
        json!({ "account": number, "username": "kt_pay.0" }),
        json!({ "account": number, "username": "kt_pay.00" }),
        json!({ "account": number, "username": "kt_pay.007" }),
        json!({ "account": number, "username": "kt-pay.01" }),
        json!({ "account": number, "username": "名.01" }),
    ];
    for params in reserve_rejects {
        assert_eq!(
            engine.error_of("usernameReserve", params),
            (-32603, "Internal error".to_owned()),
            "reserve param face must reject"
        );
    }
    let confirm_rejects = [
        json!({ "account": number, "username": bare_name }),
        json!({ "account": number, "usernameHash": hash }),
        json!({ "account": number, "username": "bad grammar", "usernameHash": hash }),
        json!({ "account": number, "username": bare_name, "usernameHash": "" }),
    ];
    for params in confirm_rejects {
        assert_eq!(
            engine.error_of("usernameConfirm", params),
            (-32603, "Internal error".to_owned()),
            "confirm param face must reject"
        );
    }
    assert_eq!(
        engine.error_of("usernameDelete", json!({})),
        (-32603, "Internal error".to_owned())
    );

    // real 模式未注册账号：注册数据前置即内部失败（不触网）。
    let real_dir = temp_dir("username-write-real");
    let mut real_engine = Engine::spawn(&real_dir);
    for (method, params) in [
        (
            "usernameReserve",
            json!({ "account": "+15559990000", "username": "kt_pay.01" }),
        ),
        (
            "usernameConfirm",
            json!({
                "account": "+15559990000",
                "username": "kt_pay.01",
                "usernameHash": hash,
            }),
        ),
        ("usernameDelete", json!({ "account": "+15559990000" })),
    ] {
        assert_eq!(
            real_engine.error_of(method, params),
            (-32603, "Internal error".to_owned()),
            "{method} unregistered must fold to internal error"
        );
    }
}

/// 安全码 + Storage 记录面八方法（安全码引擎面批 / Storage 跨设备同步批 /
/// Stories 名单同步批）：fake 模式形状与参数 fail-closed 契约钉（-32603；
/// storage 四码只覆盖链路，参数错误不占用——参数闸在引擎侧，fake 模式同样
/// 生效）。
#[test]
fn identities_trust_and_storage_records_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("identity-records-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 读：listIdentities 空表（SafetyNumberNotReady 非错误口径）；
    // chatFolders/notificationProfiles 空列表成功面。
    assert_eq!(
        engine.request(
            "listIdentities",
            json!({ "account": "+15550000000", "number": "+15551234567" })
        )["result"],
        json!([])
    );
    assert_eq!(
        engine.request("chatFolders.list", json!({ "account": "+15550000000" }))["result"],
        json!({ "folders": [] })
    );
    assert_eq!(
        engine.request(
            "notificationProfiles.list",
            json!({ "account": "+15550000000" })
        )["result"],
        json!({ "profiles": [] })
    );

    // fake 写：合法批成功回写后全量（空）；trust 成功 {}（pinned 同款
    // 命令无 JSON 输出投影）。
    assert_eq!(
        engine.request(
            "chatFolders.setRecords",
            json!({
                "account": "+15550000000",
                "records": [{
                    "id": "11111111-2222-4333-8444-555555555555",
                    "name": "工作",
                    "position": 0,
                    "showOnlyUnread": false,
                    "showMutedChats": false,
                    "includeAllIndividualChats": true,
                    "includeAllGroupChats": false,
                    "folderType": "CUSTOM",
                }],
            }),
        )["result"],
        json!({ "folders": [] })
    );
    assert_eq!(
        engine.request(
            "notificationProfiles.setRecords",
            json!({
                "account": "+15550000000",
                "records": [{
                    "id": "0123456789abcdef0123456789abcdef",
                    "name": "睡眠",
                    "color": 0,
                    "createdAtMs": 0,
                    "allowAllCalls": false,
                    "allowAllMentions": false,
                    "scheduleEnabled": false,
                }],
                "removeIds": ["fedcba9876543210fedcba9876543210"],
            }),
        )["result"],
        json!({ "profiles": [] })
    );
    // fake 读：storyDistributionLists 空列表成功面。
    assert_eq!(
        engine.request(
            "storyDistributionLists.list",
            json!({ "account": "+15550000000" })
        )["result"],
        json!({ "lists": [] })
    );
    // fake 写：合法批（成员双面齐发）成功回写后全量（空）。
    assert_eq!(
        engine.request(
            "storyDistributionLists.setRecords",
            json!({
                "account": "+15550000000",
                "records": [{
                    "identifier": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                    "name": "好友",
                    "recipientServiceIds": ["6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae"],
                    "recipientServiceIdsBinary": ["aiyxf+98Sl2caW+LKx+Irg=="],
                    "deletedAtTimestamp": 0,
                    "allowsReplies": true,
                    "isBlockList": false,
                }],
            }),
        )["result"],
        json!({ "lists": [] })
    );
    assert_eq!(
        engine.request(
            "trust",
            json!({ "account": "+15550000000", "recipient": "+15551234567", "state": "default" }),
        )["result"],
        json!({})
    );

    // ---- 参数面 fail-closed（fake 模式同闸）----

    // listIdentities：account/number 缺失、number 非字符串。
    for params in [
        json!({}),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "number": 7 }),
    ] {
        let (code, _) = engine.error_of("listIdentities", params);
        assert_eq!(code, -32603, "listIdentities params gate");
    }

    // trust：双臂同置/双缺、state 词表外、recipient 缺失。
    for params in [
        json!({ "account": "+15550000000", "recipient": "+15551234567" }),
        json!({
            "account": "+15550000000",
            "recipient": "+15551234567",
            "verifiedSafetyNumber": "012345678901234567890123456789012345678901234567890123456789",
            "state": "default",
        }),
        json!({
            "account": "+15550000000",
            "recipient": "+15551234567",
            "state": "bogus",
        }),
        json!({ "account": "+15550000000", "verifiedSafetyNumber": "x" }),
    ] {
        let (code, _) = engine.error_of("trust", params);
        assert_eq!(code, -32603, "trust params gate");
    }

    // setRecords 共闸：account 缺失、两参双缺、全空批、records 非数组、
    // 条目非对象、id 坏、重复 id、records∩removeIds 交集、removeIds 非 id。
    let valid_folder = json!({
        "id": "11111111-2222-4333-8444-555555555555",
        "name": "工作",
        "position": 0,
        "showOnlyUnread": false,
        "showMutedChats": false,
        "includeAllIndividualChats": true,
        "includeAllGroupChats": false,
        "folderType": "CUSTOM",
    });
    for params in [
        json!({ "records": [valid_folder.clone()] }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "records": [], "removeIds": [] }),
        json!({ "account": "+15550000000", "records": "nope" }),
        json!({ "account": "+15550000000", "records": [7] }),
        json!({
            "account": "+15550000000",
            "records": [{ "id": "not-a-uuid", "name": "x", "position": 0,
                "showOnlyUnread": false, "showMutedChats": false,
                "includeAllIndividualChats": false, "includeAllGroupChats": false,
                "folderType": "ALL" }],
        }),
        json!({
            "account": "+15550000000",
            "records": [valid_folder.clone(), valid_folder.clone()],
        }),
        json!({
            "account": "+15550000000",
            "records": [valid_folder.clone()],
            "removeIds": ["11111111-2222-4333-8444-555555555555"],
        }),
        json!({
            "account": "+15550000000",
            "removeIds": ["not-a-uuid"],
        }),
    ] {
        let (code, _) = engine.error_of("chatFolders.setRecords", params);
        assert_eq!(code, -32603, "chatFolders.setRecords params gate");
    }

    // profile 面特有闸：id 非 32 位小写 hex、name 空串、scheduleDaysEnabled
    // 超 1..=7。
    let profile_base = json!({
        "id": "0123456789abcdef0123456789abcdef",
        "name": "睡眠",
        "color": 0,
        "createdAtMs": 0,
        "allowAllCalls": false,
        "allowAllMentions": false,
        "scheduleEnabled": false,
    });
    let mut bad_hex = profile_base.clone();
    bad_hex["id"] = json!("0123456789ABCDEF0123456789ABCDEF");
    let mut empty_name = profile_base.clone();
    empty_name["name"] = json!("");
    let mut bad_day = profile_base.clone();
    bad_day["scheduleDaysEnabled"] = json!([1, 8]);
    for record in [bad_hex, empty_name, bad_day] {
        let (code, _) = engine.error_of(
            "notificationProfiles.setRecords",
            json!({ "account": "+15550000000", "records": [record] }),
        );
        assert_eq!(code, -32603, "notificationProfiles.setRecords params gate");
    }

    // list 读面参数闸同款。
    let (code, _) = engine.error_of("chatFolders.list", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("notificationProfiles.list", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("storyDistributionLists.list", json!({}));
    assert_eq!(code, -32603);

    // storyDistributionLists.setRecords 参数闸：account 缺失、双参双缺、
    // identifier 非 UUID、allowsReplies 缺失、成员串面解析不出、binary 面
    // 坏 base64、removeIds 非 UUID。
    let valid_list = json!({
        "identifier": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "name": "好友",
        "allowsReplies": true,
        "isBlockList": false,
    });
    let mut no_replies = valid_list.clone();
    no_replies
        .as_object_mut()
        .expect("obj")
        .remove("allowsReplies");
    let mut bad_member = valid_list.clone();
    bad_member["recipientServiceIds"] = json!(["x"]);
    let mut bad_binary = valid_list.clone();
    bad_binary["recipientServiceIdsBinary"] = json!(["%%%"]);
    for params in [
        json!({ "records": [valid_list.clone()] }),
        json!({ "account": "+15550000000" }),
        json!({
            "account": "+15550000000",
            "records": [{ "identifier": "not-a-uuid", "name": "x",
                "allowsReplies": false, "isBlockList": false }],
        }),
        json!({ "account": "+15550000000", "records": [no_replies] }),
        json!({ "account": "+15550000000", "records": [bad_member] }),
        json!({ "account": "+15550000000", "records": [bad_binary] }),
        json!({
            "account": "+15550000000",
            "removeIds": ["not-a-uuid"],
        }),
    ] {
        let (code, _) = engine.error_of("storyDistributionLists.setRecords", params);
        assert_eq!(
            code, -32603,
            "storyDistributionLists.setRecords params gate"
        );
    }
}

/// engine.protocolCapabilities（引擎自证面，KT 扩展方法）：只读、零网络、
/// 账号无关——未绑定账号（全新数据目录，real 模式零账号）照常应答；
/// fake/real 同码路真实答（常量派生面，calls.endorsementsPublicKey 同款）；
/// 读面不落调度日志（engine-send-log.jsonl 不出现）。
#[test]
fn engine_protocol_capabilities_self_report_shape() {
    let assert_shape = |result: &Value| {
        let obj = result.as_object().expect("result object");
        assert_eq!(
            obj.keys().collect::<Vec<_>>().as_slice(),
            [
                "baseline",
                "deviceCapabilities",
                "deviceCapabilitiesUpload",
                "linkCapabilities"
            ]
        );

        let device = obj["deviceCapabilities"]
            .as_object()
            .expect("device object");
        assert_eq!(
            device.keys().collect::<Vec<_>>().as_slice(),
            [
                "attachmentBackfill",
                "optionalPhoneNumber",
                "profiles_v2",
                "spqr",
                "storage",
                "transfer",
                "usernameChangeSyncMessage",
                "versionedExpirationTimer"
            ]
        );
        assert_eq!(device["spqr"], json!(true));
        assert_eq!(device["storage"], json!(true));
        assert_eq!(device["transfer"], json!(false));
        assert_eq!(device["attachmentBackfill"], json!(false));
        assert_eq!(device["profiles_v2"], json!(false));
        assert_eq!(device["usernameChangeSyncMessage"], json!(true));
        assert_eq!(device["versionedExpirationTimer"], json!(false));
        assert_eq!(device["optionalPhoneNumber"], json!(false));
        // vendor serde 的 profiles_v2 字面拼写（线上声明即此形状）不得被
        // "纠正"成 camelCase。
        assert!(device.get("profilesV2").is_none());

        // 设备面注册位（D5，官方 CapabilitiesUploadType 四键 wire 同形）。
        let upload = obj["deviceCapabilitiesUpload"]
            .as_object()
            .expect("upload object");
        assert_eq!(
            upload.keys().collect::<Vec<_>>().as_slice(),
            [
                "attachmentBackfill",
                "optionalPhoneNumber",
                "spqr",
                "usernameChangeSyncMessage"
            ]
        );
        assert_eq!(upload["spqr"], json!(true));
        assert_eq!(upload["usernameChangeSyncMessage"], json!(true));
        assert_eq!(upload["attachmentBackfill"], json!(false));
        assert_eq!(upload["optionalPhoneNumber"], json!(false));

        let link = obj["linkCapabilities"].as_object().expect("link object");
        assert_eq!(
            link.keys().collect::<Vec<_>>().as_slice(),
            ["attachmentBackfill", "spqr", "usernameChangeSyncMessage"]
        );
        assert_eq!(link["spqr"], json!(true));
        assert_eq!(link["attachmentBackfill"], json!(false));
        assert_eq!(link["usernameChangeSyncMessage"], json!(true));

        let baseline = obj["baseline"].as_object().expect("baseline object");
        assert_eq!(
            baseline.keys().collect::<Vec<_>>().as_slice(),
            ["libsignalCommit", "libsignalVersion", "presageRev"]
        );
        for key in ["presageRev", "libsignalCommit"] {
            let value = baseline[key].as_str().expect("string");
            assert_eq!(value.len(), 40, "{key} is a full git sha");
            assert!(value.chars().all(|c| c.is_ascii_hexdigit()), "{key} hex");
        }
        let version = baseline["libsignalVersion"].as_str().expect("string");
        assert!(!version.is_empty(), "libsignal version present");
        assert!(version.chars().next().is_some_and(|c| c.is_ascii_digit()));
    };

    // real 模式 + 全新数据目录：账号无关可应答（未绑定账号边界）。
    let real_dir = temp_dir("capabilities-real");
    let mut real_engine = Engine::spawn(&real_dir);
    assert_shape(&real_engine.request("engine.protocolCapabilities", json!({}))["result"]);
    drop(real_engine);
    assert!(
        !real_dir.join("engine-send-log.jsonl").exists(),
        "read-only face must not write the dispatch log"
    );

    // fake 模式：同码路真实答（形状逐键一致）。
    let fake_dir = temp_dir("capabilities-fake");
    let mut fake_engine = Engine::spawn_with_env(&fake_dir, FAKE_SERVICE);
    assert_shape(&fake_engine.request("engine.protocolCapabilities", json!({}))["result"]);
}

/// engine.deviceCapabilities（D5 设备面能力注册，KT 扩展方法）：fake 模式
/// 恒已注册（getUserStatus 同款，不触网）回读上传面；real 模式未知账号
/// -32603 折叠（open_registered 未命中，解绑判定只属 401）。不落调度日志。
#[test]
fn engine_device_capabilities_register_and_report() {
    // fake：registered true + 上传面四键回读（位值 = 钉定镜像）。
    let fake_dir = temp_dir("device-caps-fake");
    let mut fake_engine = Engine::spawn_with_env(&fake_dir, FAKE_SERVICE);
    let result = &fake_engine.request(
        "engine.deviceCapabilities",
        json!({ "account": "+15550001111" }),
    )["result"];
    assert_eq!(result["registered"], json!(true));
    assert_eq!(result["account"], json!("+15550001111"));
    let capabilities = result["capabilities"].as_object().expect("upload object");
    assert_eq!(
        capabilities.keys().collect::<Vec<_>>().as_slice(),
        [
            "attachmentBackfill",
            "optionalPhoneNumber",
            "spqr",
            "usernameChangeSyncMessage"
        ]
    );
    assert_eq!(capabilities["spqr"], json!(true));
    assert_eq!(capabilities["attachmentBackfill"], json!(false));
    assert_eq!(capabilities["usernameChangeSyncMessage"], json!(true));
    assert_eq!(capabilities["optionalPhoneNumber"], json!(false));
    drop(fake_engine);
    assert!(
        !fake_dir.join("engine-send-log.jsonl").exists(),
        "capability registration must not write the dispatch log"
    );

    // real + 全新数据目录（无账号）：open_registered 未命中折叠 -32603，
    // 不触网（无 manager 可建）。
    let real_dir = temp_dir("device-caps-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, _) = real_engine.error_of(
        "engine.deviceCapabilities",
        json!({ "account": "+15550001111" }),
    );
    assert_eq!(code, -32603, "unknown account folds to internal error");
}
