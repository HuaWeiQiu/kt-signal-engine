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

/// setProfile（P5 扩面，官方 updateProfile 等价）：fake 形状成功 `{}`（文字
/// 面/头像 data URI/null 三态）+ 参数面 fail-closed 逐形状（-32603：缺
/// account、givenName 缺/空/非字符串、可选文本字段非字符串、avatar 非图
/// 像 data URI/非字符串——不触网）。
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

    // fake 成功面：{}（文字面/换新头像/移除头像三形态）。
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "familyName": "Agent", "about": "hi", "emoji": "🤖" }),
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "avatar": avatar }),
        )["result"],
        json!({})
    );
    assert_eq!(
        engine.request(
            "setProfile",
            json!({ "account": number, "givenName": "KT", "avatar": null }),
        )["result"],
        json!({})
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "givenName": "KT" }),
        json!({ "account": number }),
        json!({ "account": number, "givenName": "" }),
        json!({ "account": number, "givenName": 7 }),
        json!({ "account": number, "givenName": "KT", "familyName": 7 }),
        json!({ "account": number, "givenName": "KT", "about": [] }),
        json!({ "account": number, "givenName": "KT", "emoji": true }),
        json!({ "account": number, "givenName": "KT", "avatar": "data:text/plain;base64,aGk=" }),
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
