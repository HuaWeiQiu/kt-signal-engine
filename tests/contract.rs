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

/// storage.mergeContacts 契约面（窗D A4，官方 mergeContactRecord engine 腿）：
/// fake 零汇总形状（读面纪律）+ 参数面 fail-closed（-32603：account 缺席/
/// 非字符串；real 模式未注册账号折叠 -32603 不触网——无 storage 状态账号
/// 零汇总幂等面归真机验证）。
#[test]
fn storage_merge_contacts_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("merge-contacts-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake：零汇总幂等形状（七字段 camelCase 全键在场）。
    assert_eq!(
        engine.request(
            "storage.mergeContacts",
            json!({ "account": "+15550000000" })
        )["result"],
        json!({
            "recordsRead": 0,
            "skipped": 0,
            "identitiesNew": 0,
            "identitiesReplaced": 0,
            "profileKeysSet": 0,
            "contactsCreated": 0,
            "trustStatesSynced": 0,
        })
    );

    // 缺 account / account 非字符串形状 → -32603。
    let (code, _) = engine.error_of("storage.mergeContacts", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("storage.mergeContacts", json!({ "account": 7 }));
    assert_eq!(code, -32603);

    // real + 全新数据目录（无账号）：未注册折叠 -32603，不触网。
    let real_dir = temp_dir("merge-contacts-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, _) = real_engine.error_of(
        "storage.mergeContacts",
        json!({ "account": "+15550001111" }),
    );
    assert_eq!(code, -32603, "unknown account folds to internal error");
}

/// storage.importGroups 契约面（窗D A5，官方 mergeGroupV2Record engine 腿）：
/// fake 零汇总形状（读面纪律，四字段 camelCase 全键在场）+ 参数面
/// fail-closed（-32603：account 缺席/非字符串；real 模式未注册账号折叠
/// -32603 不触网——manifest 拉取/群态导入计数归真机验证）。
#[test]
fn storage_import_groups_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("import-groups-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake：零汇总幂等形状。
    assert_eq!(
        engine.request("storage.importGroups", json!({ "account": "+15550000000" }))["result"],
        json!({
            "recordsRead": 0,
            "imported": 0,
            "alreadyKnown": 0,
            "failed": 0,
        })
    );

    // 缺 account / account 非字符串形状 → -32603。
    let (code, _) = engine.error_of("storage.importGroups", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("storage.importGroups", json!({ "account": 7 }));
    assert_eq!(code, -32603);

    // real + 全新数据目录（无账号）：未注册折叠 -32603，不触网。
    let real_dir = temp_dir("import-groups-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, _) =
        real_engine.error_of("storage.importGroups", json!({ "account": "+15550001111" }));
    assert_eq!(code, -32603, "unknown account folds to internal error");
}

/// storage.getCallLinks / storage.setCallLink 契约面（窗D A6，官方
/// mergeCallLinkRecord/toCallLinkRecord engine 腿）：fake 空表形状（读面
/// 纪律）+ 参数面 fail-closed 逐形状（rootKey 16/21B 闸、adminPasskey/
/// deletedAtTimestampMs 两键互斥且各自形状闸、deletedAtTimestampMs 须正
/// 整数；-32603 参数面不占用结构化码；real 模式未注册账号折叠 -32603 不
/// 触网——manifest 拉取/写后全量投影归真机验证）。
#[test]
fn storage_call_link_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("call-link-sync-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let root_key_v0 = "EREREREREREREREREREREQ=="; // 16B（V0）
    let root_key_v1 = "ERERERERERERERERERERERERERER"; // 21B（V1）
    let passkey = "KQ==";

    // fake 读：空列表成功。
    assert_eq!(
        engine.request("storage.getCallLinks", json!({ "account": "+15550000000" }))["result"],
        json!({ "callLinks": [] })
    );

    // fake 写 active（V0/V1 双版本 rootKey 皆合法）：空表成功。
    assert_eq!(
        engine.request(
            "storage.setCallLink",
            json!({ "account": "+15550000000", "rootKey": root_key_v0, "adminPasskey": passkey }),
        )["result"],
        json!({ "callLinks": [] })
    );
    assert_eq!(
        engine.request(
            "storage.setCallLink",
            json!({ "account": "+15550000000", "rootKey": root_key_v1, "adminPasskey": passkey }),
        )["result"],
        json!({ "callLinks": [] })
    );

    // fake 写 deleted（adminPasskey 恒缺形态）：空表成功。
    assert_eq!(
        engine.request(
            "storage.setCallLink",
            json!({ "account": "+15550000000", "rootKey": root_key_v0, "deletedAtTimestampMs": 1720000000000u64 }),
        )["result"],
        json!({ "callLinks": [] })
    );

    // getCallLinks：缺 account / 非字符串 → -32603。
    let (code, _) = engine.error_of("storage.getCallLinks", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("storage.getCallLinks", json!({ "account": 7 }));
    assert_eq!(code, -32603);

    // setCallLink：缺 account / 缺 rootKey → -32603。
    let (code, _) = engine.error_of("storage.setCallLink", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "adminPasskey": passkey }),
    );
    assert_eq!(code, -32603);

    // rootKey 非 base64 / 长度不在 16/21B（15B/24B）/ 非字符串 → -32603。
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": "not-base64!!" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": "ERERERERERERERERERER" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": "ERERERERERERERERERERERERERERERERE" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": 7, "adminPasskey": passkey }),
    );
    assert_eq!(code, -32603);

    // adminPasskey 非字符串 / 非法 base64 / 空解码 → -32603。
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": root_key_v0, "adminPasskey": 7 }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": root_key_v0, "adminPasskey": "!!" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": root_key_v0, "adminPasskey": "" }),
    );
    assert_eq!(code, -32603);

    // deletedAtTimestampMs 非数值 / 0 / 负 / 小数 → -32603。
    for bad in [json!("soon"), json!(0), json!(-1), json!(1.5)] {
        let (code, _) = engine.error_of(
            "storage.setCallLink",
            json!({ "account": "+15550000000", "rootKey": root_key_v0, "deletedAtTimestampMs": bad }),
        );
        assert_eq!(code, -32603, "deletedAt shape {bad}");
    }

    // 两键同置（adminPasskey + deletedAtTimestampMs）→ -32603（官方
    // isBadRemoteData 同位矛盾意图）。
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": root_key_v0, "adminPasskey": passkey, "deletedAtTimestampMs": 1720000000000u64 }),
    );
    assert_eq!(code, -32603);

    // 空意图（仅 rootKey，active/deleted 两腿皆缺）→ -32603（无动作语义
    // 不收——同 setStickerPackSync installed 必填纪律）。
    let (code, _) = engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550000000", "rootKey": root_key_v0 }),
    );
    assert_eq!(code, -32603);

    // real + 全新数据目录（无账号）：未注册折叠 -32603，不触网。
    let real_dir = temp_dir("call-link-sync-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, _) =
        real_engine.error_of("storage.getCallLinks", json!({ "account": "+15550001111" }));
    assert_eq!(code, -32603, "unknown account folds to internal error");
    let (code, _) = real_engine.error_of(
        "storage.setCallLink",
        json!({ "account": "+15550001111", "rootKey": root_key_v0, "adminPasskey": passkey }),
    );
    assert_eq!(code, -32603, "unknown account folds to internal error");
}

/// getDevices / unlinkDevice 契约面（窗D B13，官方 getDevices/unlink
/// engine 腿）：fake 空表/成功桩形状（读面纪律）+ 参数面 fail-closed
/// （-32603：account 缺席/非字符串；deviceId 缺席/非数值/≤0；real 模式
/// 未注册账号折叠 -32603 不触网——服务器清单拉取/解绑归真机验证）。
#[test]
fn devices_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("devices-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 读：空列表成功。
    assert_eq!(
        engine.request("getDevices", json!({ "account": "+15550000000" }))["result"],
        json!({ "devices": [] })
    );

    // fake 写：成功空结果。
    assert_eq!(
        engine.request(
            "unlinkDevice",
            json!({ "account": "+15550000000", "deviceId": 2 })
        )["result"],
        json!({})
    );

    // getDevices：缺 account / 非字符串 → -32603。
    let (code, _) = engine.error_of("getDevices", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("getDevices", json!({ "account": 7 }));
    assert_eq!(code, -32603);

    // unlinkDevice：缺 account / 缺 deviceId / deviceId 非数值 / ≤0 / 小数
    // → -32603。
    let (code, _) = engine.error_of("unlinkDevice", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("unlinkDevice", json!({ "account": "+15550000000" }));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "unlinkDevice",
        json!({ "account": "+15550000000", "deviceId": "2" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "unlinkDevice",
        json!({ "account": "+15550000000", "deviceId": 0 }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "unlinkDevice",
        json!({ "account": "+15550000000", "deviceId": -1 }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "unlinkDevice",
        json!({ "account": "+15550000000", "deviceId": 2.5 }),
    );
    assert_eq!(code, -32603);

    // real + 全新数据目录（无账号）：未注册折叠 -32603，不触网。
    let real_dir = temp_dir("devices-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, _) = real_engine.error_of("getDevices", json!({ "account": "+15550001111" }));
    assert_eq!(code, -32603, "unknown account folds to internal error");
    let (code, _) = real_engine.error_of(
        "unlinkDevice",
        json!({ "account": "+15550001111", "deviceId": 2 }),
    );
    assert_eq!(code, -32603, "unknown account folds to internal error");
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

/// 会话状态三 flag 读写面（getConversationState/setConversationState，
/// P15b 项2）：fake 模式 get 恒缺省态 / set 缺省态叠意图回显；参数面
/// fail-closed -32603（kind 缺失/未知、三 flag 形状错、三 flag 全缺 =
/// 无写意图）。
#[test]
fn conversation_state_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("conversation-state-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // fake 读：三 kind 同构缺省态（三键恒在）。
    for kind in ["contact", "group", "legacyGroup"] {
        assert_eq!(
            engine.request(
                "getConversationState",
                json!({ "account": "+15550000000", "conversationId": "x", "kind": kind }),
            )["result"],
            json!({ "mutedUntilTimestamp": 0, "archived": false, "markedUnread": false })
        );
    }

    // fake 写：缺省态叠意图回显（部分意图位缺席 = 不动该位）。
    assert_eq!(
        engine.request(
            "setConversationState",
            json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact",
                    "mutedUntilTimestamp": 123, "archived": true }),
        )["result"],
        json!({ "mutedUntilTimestamp": 123, "archived": true, "markedUnread": false })
    );

    // 缺 account / 未知 kind / 缺 conversationId → -32603。
    let (code, _) = engine.error_of("getConversationState", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "getConversationState",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "bogus" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "getConversationState",
        json!({ "account": "+15550000000", "kind": "contact" }),
    );
    assert_eq!(code, -32603);

    // 写面形状错：mutedUntilTimestamp 非数字/null/负数、archived 非布尔
    // → -32603。
    let (code, _) = engine.error_of(
        "setConversationState",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact",
                "mutedUntilTimestamp": "soon" }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationState",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact",
                "mutedUntilTimestamp": -5 }),
    );
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of(
        "setConversationState",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact",
                "archived": "yes" }),
    );
    assert_eq!(code, -32603);

    // 三 flag 全缺 = 无写意图 → -32603（updateGroup 全缺整拒同纪律）。
    let (code, _) = engine.error_of(
        "setConversationState",
        json!({ "account": "+15550000000", "conversationId": "x", "kind": "contact" }),
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

/// 群迁移（migrateGroup，KT 扩展方法，官方 initiateMigrationToGroupV2
/// 行为对齐）：fake 模式形状桩 `{groupId(随机 32B master key 标准 base64),
/// revision: 0}`（落调度日志，avatar 键摘要化，遗留 GV1 groupId 逐字
/// 保留）；avatar data URI 臂失败 GROUP_AVATAR_UPLOAD_FAILED；参数面
/// fail-closed（-32603：缺 account / groupId 缺席·空串·非 base64·非字符
/// 串 / title 空串·非字符串 / expiration 非法形状 / members 缺席·非数组·
/// 空数组·条目非字符串 / avatar 非 image 族·坏 base64）；real 模式未注册
/// 账号折叠 -32603。
#[test]
fn migrate_group_fake_shapes_and_param_fail_closed() {
    use base64::Engine as _;
    let dir = temp_dir("migrate-group-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    // 遗留 GV1 群 id（官方 libsignal 形状 = 16B 标准 base64，24 字符）。
    let legacy_id = base64::engine::general_purpose::STANDARD.encode([0xABu8; 16]);
    let members = json!(["+15551112222"]);
    let avatar = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(b"\x89PNG-fake")
    );

    // fake 合法面：groupId + members 必填，title / expiration / avatar
    // 缺省 → 形状桩 {groupId, revision: 0}；新 groupId ≠ 遗留 id 且为
    // 32B 标准 base64。
    let migrated = engine.request(
        "migrateGroup",
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": members }),
    )["result"]
        .clone();
    let new_group_id = migrated["groupId"].as_str().expect("groupId string");
    assert_ne!(new_group_id, legacy_id, "新 GV2 id ≠ 遗留 GV1 id");
    assert_eq!(new_group_id.len(), 44, "32B master key 标准 base64");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(new_group_id)
        .expect("groupId decodes");
    assert_eq!(decoded.len(), 32);
    assert_eq!(migrated["revision"], json!(0));

    // title / expiration 臂同形成功（title 可选非空，缺席 = 空串迁入）。
    assert_eq!(
        engine.request(
            "migrateGroup",
            json!({ "account": "+15550000000", "groupId": legacy_id, "title": "旧群",
                    "expiration": 86_400, "members": members })
        )["result"]["revision"],
        json!(0)
    );

    // fake 失败臂：avatar data URI（需上行链）→ GROUP_AVATAR_UPLOAD_FAILED
    // data {retryable:true, textSaved:false}。
    let response = engine.request(
        "migrateGroup",
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": members,
                "avatar": avatar }),
    );
    assert_eq!(response["error"]["code"], -1);
    assert_eq!(response["error"]["message"], "GROUP_AVATAR_UPLOAD_FAILED");

    // 参数面 fail-closed 逐形状（-32603）。
    let rejects = [
        json!({ "groupId": legacy_id, "members": members }),
        json!({ "account": "+15550000000", "members": members }),
        json!({ "account": "+15550000000", "groupId": "", "members": members }),
        json!({ "account": "+15550000000", "groupId": "###", "members": members }),
        json!({ "account": "+15550000000", "groupId": 7, "members": members }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "title": "", "members": members }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "title": 7, "members": members }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "expiration": "3600", "members": members }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "expiration": -1, "members": members }),
        json!({ "account": "+15550000000", "groupId": legacy_id }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": "not-array" }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": [] }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": [7] }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": members, "avatar": "data:text/plain;base64,aGk=" }),
        json!({ "account": "+15550000000", "groupId": legacy_id, "members": members, "avatar": "data:image/png;base64,###" }),
    ];
    for params in &rejects {
        let response = engine.request("migrateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "migrateGroup rejects {params}"
        );
    }

    // 调度日志：groupId/members/title 键逐字保留（协议上行面纪律），
    // avatar 键形状摘要化（data URI 载荷零字节落盘）；被拒调用同样落日志。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .filter(|entry: &Value| entry["method"] == "migrateGroup")
        .collect();
    assert_eq!(
        entries.len(),
        18,
        "成功两条 + 失败一条 + 拒绝十五条均落日志"
    );
    assert_eq!(entries[0]["params"]["groupId"], json!(legacy_id));
    assert_eq!(entries[0]["params"]["members"], members);
    assert!(
        entries[0]["params"]["title"].is_null(),
        "title 缺省原样保留"
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

    // real 模式未注册账号：storage 上下文打不开折叠 -32603（createGroup
    // 同款，细节不外泄）。
    let real_dir = temp_dir("migrate-group-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "migrateGroup",
        json!({ "account": "+15559990000", "groupId": legacy_id, "members": members }),
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

/// updateGroup 群链接写面（addFromInviteLinkAccess + inviteLinkPassword 参数
/// 族，P13/任务2，官方 buildAccessControlAddFromInviteLinkChange/
/// buildInviteLinkPasswordChange/buildNewGroupLinkChange 行为对齐）：fake
/// 成功 `{status:"sent"}`（成员臂随桩成功同纪律，fake 无合成链）；双面同调
/// 合法（同 version 单 change，官方 buildNewGroupLinkChange 同形）；参数面
/// fail-closed（-32603：access 未知词/UNKNOWN/null/非字符串——UNKNOWN 是读
/// 面兜底词不作写意图——password null/空串/坏 base64/非字符串）；调度日志
/// 两键照旧逐字保留（链接密码非敏感载荷——20+ 调用方 wire 面本就明文携
/// 密码，摘要有意不设）。
#[test]
fn update_group_group_link_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("update-group-group-link");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";
    // 16B 密码标准 base64（官方 GROUP_INVITE_LINK_PASSWORD_LENGTH = 16）。
    let password_b64 = "qvUr2HnPjYWl3dBOYiF4mQ==";

    // fake 合法面：access 单独在场 / password 单独在场 / 双面同调均成功
    // {status:"sent"}。
    assert_eq!(
        engine.request(
            "updateGroup",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "addFromInviteLinkAccess": "ANY"
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
                "inviteLinkPassword": password_b64
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
                "addFromInviteLinkAccess": "UNSATISFIABLE",
                "inviteLinkPassword": password_b64
            })
        )["result"],
        json!({ "status": "sent" })
    );

    // 参数面 fail-closed 逐形状（access：未知词/UNKNOWN/小写词/null/非字符
    // 串；password：null/空串/坏 base64/非字符串；groupId 闸照旧）。
    let rejects = [
        json!({ "account": "+15550000000", "groupId": group_id, "addFromInviteLinkAccess": "ANYBODY" }),
        json!({ "account": "+15550000000", "groupId": group_id, "addFromInviteLinkAccess": "UNKNOWN" }),
        json!({ "account": "+15550000000", "groupId": group_id, "addFromInviteLinkAccess": "any" }),
        json!({ "account": "+15550000000", "groupId": group_id, "addFromInviteLinkAccess": null }),
        json!({ "account": "+15550000000", "groupId": group_id, "addFromInviteLinkAccess": 7 }),
        json!({ "account": "+15550000000", "groupId": group_id, "inviteLinkPassword": null }),
        json!({ "account": "+15550000000", "groupId": group_id, "inviteLinkPassword": "" }),
        json!({ "account": "+15550000000", "groupId": group_id, "inviteLinkPassword": "not base64!!" }),
        json!({ "account": "+15550000000", "groupId": group_id, "inviteLinkPassword": 42 }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "addFromInviteLinkAccess": "ANY" }),
    ];
    for params in &rejects {
        let response = engine.request("updateGroup", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "updateGroup rejects {params}"
        );
    }

    // 调度日志：两键照旧逐字保留（成功 3 + 拒绝 10 = 13 行均落日志）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "updateGroup")
        .collect();
    assert_eq!(entries.len(), 13, "成功三条 + 拒绝十条均落日志");
    assert_eq!(
        entries[0]["params"]["addFromInviteLinkAccess"],
        json!("ANY")
    );
    assert_eq!(
        entries[1]["params"]["inviteLinkPassword"],
        json!(password_b64)
    );

    // real 模式未注册账号 + 链接两面在场：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("update-group-group-link-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "updateGroup",
        json!({
            "account": "+15559990000",
            "groupId": group_id,
            "addFromInviteLinkAccess": "ANY",
            "inviteLinkPassword": password_b64
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

/// previewGroupViaLink（链接预览读面，P13/任务3，官方 getPreJoinGroupInfo
/// 行为对齐）：fake 错误面形状桩 = GROUP_INVITE_LINK_PREVIEW_FAILED
/// （`-1` 加 message，data 无 status——fake 无链接读面，材料不可得即错误
/// 面）；读面不落调度日志（材料面纪律）；参数面 fail-closed（-32603：缺
/// account/缺 inviteLink/非字符串/坏链接——parseInviteLink 同闸收口）；
/// real 模式未注册账号折叠 -32603。
#[test]
fn preview_group_via_link_fake_error_face_and_param_fail_closed() {
    let dir = temp_dir("preview-group-via-link");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    // 固定材料链接（masterKey = 0x11×32，password = 0x22×16，prost 同构编码）。
    let link = "https://signal.group/#CjQKIBEREREREREREREREREREREREREREREREREREREREREREhAiIiIiIiIiIiIiIiIiIiIi";

    // fake 错误面：GROUP_INVITE_LINK_PREVIEW_FAILED，无 status。
    let response = engine.request(
        "previewGroupViaLink",
        json!({ "account": "+15550000000", "inviteLink": link }),
    );
    assert_eq!(response["error"]["code"], -1);
    assert_eq!(
        response["error"]["message"],
        "GROUP_INVITE_LINK_PREVIEW_FAILED"
    );
    assert!(
        response["error"]["data"]["status"].is_null(),
        "fake 错误面无 HTTP status"
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "inviteLink": link }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "inviteLink": "" }),
        json!({ "account": "+15550000000", "inviteLink": "https://signal.group/#" }),
        json!({ "account": "+15550000000", "inviteLink": "https://signal.group/#@@@" }),
        json!({ "account": "+15550000000", "inviteLink": 7 }),
    ];
    for params in &rejects {
        let response = engine.request("previewGroupViaLink", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "previewGroupViaLink rejects {params}"
        );
    }

    // 读面不落调度日志（材料面纪律，与 generateInviteLinkPassword 同位）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        !log.lines().any(|line| line.contains("previewGroupViaLink")),
        "preview 读面零调度日志"
    );

    // real 模式未注册账号：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("preview-group-via-link-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "previewGroupViaLink",
        json!({ "account": "+15559990000", "inviteLink": link }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// joinGroupViaLink（经链接入群写面，P13/任务3，官方 joinGroupV2ViaLink
/// 行为对齐）：fake 形状桩 `{status:"joined"}`（direct 臂形状——fake 无
/// join info 读面不裁定审批臂，§5 登记偏差）；写面落调度日志（mutating
/// 面纪律，inviteLink 键照旧逐字保留）；参数面 fail-closed（-32603 同
/// preview 闸）；real 模式未注册账号折叠 -32603。
#[test]
fn join_group_via_link_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("join-group-via-link");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let link = "https://signal.group/#CjQKIBEREREREREREREREREREREREREREREREREREREREREREhAiIiIiIiIiIiIiIiIiIiIi";

    // fake 形状桩：{status:"joined"}（direct 臂）。
    assert_eq!(
        engine.request(
            "joinGroupViaLink",
            json!({ "account": "+15550000000", "inviteLink": link })
        )["result"],
        json!({ "status": "joined" })
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "inviteLink": link }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "inviteLink": "" }),
        json!({ "account": "+15550000000", "inviteLink": "sgnl://joingroup#not-base64!!" }),
    ];
    for params in &rejects {
        let response = engine.request("joinGroupViaLink", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "joinGroupViaLink rejects {params}"
        );
    }

    // 调度日志：写面落日志，inviteLink 键逐字保留（成功 1 + 拒绝 4 = 5 行）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "joinGroupViaLink")
        .collect();
    assert_eq!(entries.len(), 5, "成功一条 + 拒绝四条均落日志");
    assert_eq!(entries[0]["params"]["inviteLink"], json!(link));

    // real 模式未注册账号：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("join-group-via-link-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "joinGroupViaLink",
        json!({ "account": "+15559990000", "inviteLink": link }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// cancelJoinRequest（撤回入群申请，P13/任务4，官方桌面 cancelJoinRequest
/// 行为对齐）：fake 形状桩 `{status:"cancelled"}`（leaveGroup 同纪律）；
/// 写面落调度日志（inviteLink 键逐字保留）；参数面 fail-closed（-32603：
/// 缺 account/缺 inviteLink/坏形状——parseInviteLink 同闸收口）；real 模
/// 式未注册账号折叠 -32603。
#[test]
fn cancel_join_request_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("cancel-join-request");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let link = "https://signal.group/#CjQKIBEREREREREREREREREREREREREREREREREREREREREREhAiIiIiIiIiIiIiIiIiIiIi";

    // fake 形状桩：{status:"cancelled"}。
    assert_eq!(
        engine.request(
            "cancelJoinRequest",
            json!({ "account": "+15550000000", "inviteLink": link })
        )["result"],
        json!({ "status": "cancelled" })
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "inviteLink": link }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "inviteLink": "" }),
        json!({ "account": "+15550000000", "inviteLink": "sgnl://joingroup#@@@" }),
    ];
    for params in &rejects {
        let response = engine.request("cancelJoinRequest", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "cancelJoinRequest rejects {params}"
        );
    }

    // 调度日志：写面落日志（成功 1 + 拒绝 4 = 5 行），inviteLink 键逐字。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "cancelJoinRequest")
        .collect();
    assert_eq!(entries.len(), 5, "成功一条 + 拒绝四条均落日志");
    assert_eq!(entries[0]["params"]["inviteLink"], json!(link));

    // real 模式未注册账号：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("cancel-join-request-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "cancelJoinRequest",
        json!({ "account": "+15559990000", "inviteLink": link }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// approveJoinRequest（管理员批准入群申请，P13/任务4，官方
/// approvePendingMembershipFromGroupV2 行为对齐）：fake 形状桩
/// `{status:"approved"}`；写面落调度日志（groupId/recipient 键逐字保留）；
/// 参数面 fail-closed（-32603：缺 account/groupId 非 base64·≠32B/recipient
/// 缺席·空串·非字符串）；real 模式未注册账号折叠 -32603。
#[test]
fn approve_join_request_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("approve-join-request");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let group_id = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 形状桩：{status:"approved"}（recipient ACI/号码两形同桩）。
    assert_eq!(
        engine.request(
            "approveJoinRequest",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "recipient": "44444444-5555-4666-9777-888888888888"
            })
        )["result"],
        json!({ "status": "approved" })
    );
    assert_eq!(
        engine.request(
            "approveJoinRequest",
            json!({
                "account": "+15550000000",
                "groupId": group_id,
                "recipient": "+15550000002"
            })
        )["result"],
        json!({ "status": "approved" })
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "groupId": group_id, "recipient": "44444444-5555-4666-9777-888888888888" }),
        json!({ "account": "+15550000000", "recipient": "44444444-5555-4666-9777-888888888888" }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "recipient": "44444444-5555-4666-9777-888888888888" }),
        json!({ "account": "+15550000000", "groupId": "QUJDREVG", "recipient": "44444444-5555-4666-9777-888888888888" }),
        json!({ "account": "+15550000000", "groupId": group_id }),
        json!({ "account": "+15550000000", "groupId": group_id, "recipient": "" }),
        json!({ "account": "+15550000000", "groupId": group_id, "recipient": 42 }),
    ];
    for params in &rejects {
        let response = engine.request("approveJoinRequest", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "approveJoinRequest rejects {params}"
        );
    }

    // 调度日志：写面落日志（成功 2 + 拒绝 7 = 9 行），recipient 键逐字。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("log line JSON"))
        .filter(|entry| entry["method"] == "approveJoinRequest")
        .collect();
    assert_eq!(entries.len(), 9, "成功两条 + 拒绝七条均落日志");
    assert_eq!(
        entries[0]["params"]["recipient"],
        json!("44444444-5555-4666-9777-888888888888")
    );

    // real 模式未注册账号：storage 上下文打不开折叠 -32603。
    let real_dir = temp_dir("approve-join-request-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let (code, message) = real_engine.error_of(
        "approveJoinRequest",
        json!({
            "account": "+15559990000",
            "groupId": group_id,
            "recipient": "44444444-5555-4666-9777-888888888888"
        }),
    );
    assert_eq!((code, message.as_str()), (-32603, "Internal error"));
}

/// 群邀请链接族纯函数面（generateInviteLinkPassword/buildInviteLink/
/// parseInviteLink，KT 扩展批 P13/任务1，官方 generateGroupInviteLinkPass
/// word/buildGroupLink/parseGroupLink 行为对齐）：三方法皆 fake/real 同码路
/// 纯本地/显式材料臂；密码 16B（官方 GROUP_INVITE_LINK_PASSWORD_LENGTH 同
/// 长）；链接形 = `https://signal.group/#<websafe>`；解析 roundtrip 还原
/// masterKey+password；参数面 fail-closed（-32603：password 空·坏 base64·
/// 非字符串、groupId 非 32B、inviteLink 缺席·三种坏形状）；材料面非协议
/// 上行——不落调度日志；buildInviteLink 缺 password 臂 fake 无群态面 →
/// GROUP_INVITE_LINK_UNAVAILABLE。
#[test]
fn invite_link_materials_pure_faces_and_param_fail_closed() {
    use base64::Engine as _;
    let dir = temp_dir("invite-link-materials-fake");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    // 密码生成：16B 标准 base64（fake/real 同码路）。
    let generated = engine.request("generateInviteLinkPassword", json!({}))["result"].clone();
    let password = generated["password"].as_str().expect("password string");
    let password_bytes = base64::engine::general_purpose::STANDARD
        .decode(password)
        .expect("password decodes");
    assert_eq!(password_bytes.len(), 16, "官方 16B 密码同长");
    let other = engine.request("generateInviteLinkPassword", json!({}))["result"]["password"]
        .as_str()
        .expect("second password")
        .to_owned();
    assert_ne!(password, other, "CSPRNG 面两次不重合");

    // 链接构造（显式 password 臂）：官方 URL 形 + roundtrip。
    let master_key = "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXphYmNkZWY="; // 32B
    let built = engine.request(
        "buildInviteLink",
        json!({ "account": "+15550000000", "groupId": master_key, "password": password }),
    )["result"]["inviteLink"]
        .as_str()
        .expect("inviteLink string")
        .to_owned();
    assert!(
        built.starts_with("https://signal.group/#"),
        "官方 URL 形: {built}"
    );
    let code = built.rsplit('#').next().expect("code segment");
    assert!(
        !code.contains('+') && !code.contains('/') && !code.contains('='),
        "websafe 化"
    );

    // 解析 roundtrip（三种官方 URL 形 + 裸 code）。
    for value in [
        built.clone(),
        format!("sgnl://signal.group/#{code}"),
        format!("sgnl://joingroup#{code}"),
        code.to_owned(),
    ] {
        let parsed =
            engine.request("parseInviteLink", json!({ "inviteLink": value }))["result"].clone();
        assert_eq!(parsed["groupId"], json!(master_key));
        assert_eq!(parsed["password"], json!(password));
    }

    // buildInviteLink 缺 password 臂：fake 无群态面 →
    // GROUP_INVITE_LINK_UNAVAILABLE。
    let response = engine.request(
        "buildInviteLink",
        json!({ "account": "+15550000000", "groupId": master_key }),
    );
    assert_eq!(response["error"]["code"], -1);
    assert_eq!(
        response["error"]["message"],
        "GROUP_INVITE_LINK_UNAVAILABLE"
    );

    // 参数面 fail-closed（-32603）。
    let bad_b64 = base64::engine::general_purpose::STANDARD.encode(b"short");
    let rejects = [
        json!({ "groupId": master_key }),
        json!({ "account": "+15550000000" }),
        json!({ "account": "+15550000000", "groupId": "not-base64!!", "password": password }),
        json!({ "account": "+15550000000", "groupId": bad_b64, "password": password }),
        json!({ "account": "+15550000000", "groupId": master_key, "password": "" }),
        json!({ "account": "+15550000000", "groupId": master_key, "password": "###" }),
        json!({ "account": "+15550000000", "groupId": master_key, "password": 42 }),
        json!({}),
        json!({ "inviteLink": "" }),
        json!({ "inviteLink": "https://signal.group/#" }),
        json!({ "inviteLink": "https://signal.group/#@@@" }),
        json!({ "inviteLink": 42 }),
    ];
    for params in &rejects {
        let method = if params.get("inviteLink").is_some()
            || params.as_object().is_none_or(|m| m.is_empty())
        {
            "parseInviteLink"
        } else {
            "buildInviteLink"
        };
        let response = engine.request(method, params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "{method} rejects {params}"
        );
    }

    // 材料面非协议上行：零调度日志行。
    let log_path = dir.join("engine-send-log.jsonl");
    assert!(
        !log_path.exists(),
        "invite link materials are not protocol uplink: no dispatch log"
    );

    // real 模式显式 password 臂同码路（纯构造，零网络）。
    let real_dir = temp_dir("invite-link-materials-real");
    let mut real_engine = Engine::spawn(&real_dir);
    let real_built = real_engine.request(
        "buildInviteLink",
        json!({ "account": "+15559990000", "groupId": master_key, "password": password }),
    )["result"]["inviteLink"]
        .as_str()
        .expect("real-mode pure arm builds")
        .to_owned();
    assert_eq!(real_built, built);
    // real 模式缺 password 臂：未注册账号 storage 上下文打不开 → -32603。
    let (code_err, message) = real_engine.error_of(
        "buildInviteLink",
        json!({ "account": "+15559990000", "groupId": master_key }),
    );
    assert_eq!((code_err, message.as_str()), (-32603, "Internal error"));
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

/// 直聊 send per-recipient 成败收集（时序审计组二#9，wire additive）：
/// fake 全成功应答 `{timestamp, results: []}` 且无 failures 键（空表键不
/// 在——旧宿主零变化）；真实面部分失败时 failures 条目形状由
/// send_error_face 分类单测钉死（与整批错误面同源）。
#[test]
fn send_direct_fake_shape_has_no_failures_key_when_empty() {
    let dir = temp_dir("send-direct-failures-additive");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);

    let response = engine.request(
        "send",
        json!({
            "account": "+15550000000",
            "recipients": ["+15555550101"],
            "message": "hello"
        }),
    );
    let result = response.get("result").expect("success result");
    assert!(result.get("timestamp").and_then(Value::as_u64).is_some());
    assert_eq!(result.get("results"), Some(&json!([])));
    assert!(result.get("failures").is_none(), "空表键不在（additive）");
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

/// markRead/markViewed（P14 组 6，官方 syncReadMessages/syncView 同构）：
/// fake 成功 {} + 调度日志 reads/viewed 键逐字保留（IPC 形状锚）+ 参数面
/// fail-closed 逐形状（-32603：列表缺席/空/非数组、条目缺 sender/timestamp、
/// timestamp 非数字、sender 非字符串、account 缺席）。
#[test]
fn mark_read_mark_viewed_sync_contract() {
    let dir = temp_dir("mark-read");
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
    let sender_aci = "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae";

    // 合法面：markRead 单条目、markViewed 双条目（号码 + PNI 混排）成功 {}。
    let read_ok = engine.request(
        "markRead",
        json!({ "account": number, "reads": [{ "sender": sender_aci, "timestamp": 42 }] }),
    );
    assert_eq!(read_ok["result"], json!({}));
    let viewed_ok = engine.request(
        "markViewed",
        json!({ "account": number, "viewed": [
            { "sender": "+15555550101", "timestamp": 43 },
            { "sender": "PNI:00000000-0000-0000-0000-000000000001", "timestamp": 44 }
        ] }),
    );
    assert_eq!(viewed_ok["result"], json!({}));

    // fail-closed 逐形状。
    let rejects = [
        json!({ "account": number }),
        json!({ "account": number, "reads": [] }),
        json!({ "account": number, "reads": "x" }),
        json!({ "account": number, "reads": [{ "timestamp": 42 }] }),
        json!({ "account": number, "reads": [{ "sender": sender_aci }] }),
        json!({ "account": number, "reads": [{ "sender": sender_aci, "timestamp": "42" }] }),
        json!({ "account": number, "viewed": [{ "sender": 7, "timestamp": 42 }] }),
        json!({ "reads": [{ "sender": "+15555550101", "timestamp": 42 }] }),
    ];
    for params in &rejects {
        let response = engine.request("markRead", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "markRead rejects {params}"
        );
    }

    // 调度日志：reads/viewed 键逐字保留。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let read_entry = entries
        .iter()
        .find(|entry| entry["method"] == "markRead")
        .expect("markRead logged");
    assert_eq!(
        read_entry["params"]["reads"][0]["sender"],
        json!(sender_aci)
    );
    assert_eq!(read_entry["params"]["reads"][0]["timestamp"], json!(42));
    let viewed_entry = entries
        .iter()
        .find(|entry| entry["method"] == "markViewed")
        .expect("markViewed logged");
    assert_eq!(
        viewed_entry["params"]["viewed"][1]["sender"],
        json!("PNI:00000000-0000-0000-0000-000000000001")
    );
    assert_eq!(viewed_entry["params"]["viewed"][1]["timestamp"], json!(44));
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

/// 名片发送臂（sendContact，窗D B11）：fake 形状成功 {"timestamp":u64}（直
/// 聊/群两腿同口径）；参数面 fail-closed 逐形状（-32603）：contact 缺席/非
/// 对象、name 非对象/子字段非字符串、number/email/address 非数组/条目非对
/// 象/value 缺席·空串·非字符串/type 词表外（number/email 四值、address 三
/// 值）/label/address 子字段非字符串、organization 非字符串、avatar 非字
/// 符串；recipient/groupId 两腿恰一（同置/都缺/伪群 id）；协议上行面落调
/// 度日志（contact 逐字保留）。
#[test]
fn send_contact_fake_shapes_and_param_fail_closed() {
    let dir = temp_dir("send-contact");
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

    // 32 字节群 master key 的标准 base64（sendCallMessage group 同源口径）。
    let group = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";

    // fake 成功面：直聊/群两腿；最简（单 number 条目）与全卡片（name/
    // number/email/address/avatar/organization）同形。
    let full_contact = json!({
        "name": { "givenName": "Ada", "familyName": "Lovelace", "middleName": "Augusta" },
        "number": [
            { "value": "+15551234567", "type": "MOBILE", "label": "cell" },
            { "value": "+15557654321" }
        ],
        "email": [ { "value": "ada@example.test", "type": "WORK" } ],
        "address": [ {
            "type": "CUSTOM", "label": "lab", "street": "1 Analytical Way",
            "city": "London", "country": "UK"
        } ],
        "avatar": "data:image/png;base64,iVBORw0KGgo=",
        "organization": "Analytical Engines"
    });
    for params in [
        json!({ "account": number, "recipient": "+15555550101",
                "contact": { "number": [{ "value": "+15550001111" }] } }),
        json!({ "account": number, "recipient": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae",
                "contact": full_contact }),
        json!({ "account": number, "groupId": group, "contact": full_contact }),
    ] {
        let ok = engine.request("sendContact", params.clone());
        assert!(
            ok["result"]["timestamp"].is_u64(),
            "fake sendContact ok: {params}"
        );
    }

    // 参数面 fail-closed 逐形状。
    let contact = json!({ "number": [{ "value": "+15550001111" }] });
    let rejects = [
        json!({ "recipient": "+15555550101", "contact": contact }),
        json!({ "account": number, "contact": contact }),
        json!({ "account": number, "recipient": "+15555550101" }),
        json!({ "account": number, "recipient": "+15555550101", "contact": 42 }),
        json!({ "account": number, "recipient": "+15555550101", "contact": "card" }),
        // name 面：非对象/子字段非字符串。
        json!({ "account": number, "recipient": "+15555550101", "contact": { "name": "Ada", "number": [{ "value": "1" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "name": { "givenName": 7 }, "number": [{ "value": "1" }] } }),
        // number 面：非数组/条目非对象/value 缺席·空串·非字符串/type 词表
        // 外·非字符串/label 非字符串。
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": "15550001111" } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": ["15550001111"] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "type": "MOBILE" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "value": "" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "value": 15550001111u64 }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "value": "1", "type": "CELL" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "value": "1", "type": 2 }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "number": [{ "value": "1", "label": 3 }] } }),
        // email 同词表闸（与 number 共用四值，CELL 词表外）。
        json!({ "account": number, "recipient": "+15555550101", "contact": { "email": [{ "value": "a@b.test", "type": "CELL" }] } }),
        // address 面：type 词表外（MOBILE 非 postal 词）、子字段非字符串。
        json!({ "account": number, "recipient": "+15555550101", "contact": { "address": [{ "type": "MOBILE" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "address": [{ "city": 4 }] } }),
        // organization/avatar 面。
        json!({ "account": number, "recipient": "+15555550101", "contact": { "organization": 7, "number": [{ "value": "1" }] } }),
        json!({ "account": number, "recipient": "+15555550101", "contact": { "avatar": 9, "number": [{ "value": "1" }] } }),
        // 两腿恰一 + 群腿形状闸。
        json!({ "account": number, "recipient": "+15555550101", "groupId": group, "contact": contact }),
        json!({ "account": number, "groupId": "not-base64!!", "contact": contact }),
        json!({ "account": number, "groupId": "QUJDREVG", "contact": contact }),
        json!({ "account": number, "groupId": "", "contact": contact }),
        json!({ "account": number, "groupId": 42, "contact": contact }),
    ];
    for params in &rejects {
        let response = engine.request("sendContact", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendContact rejects {params}"
        );
    }

    // 调度日志：contact 逐字保留（协议上行面纪律），群腿无 recipient 键。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["method"] == "sendContact" && entry["params"].get("groupId").is_none())
        .expect("direct sendContact logged");
    assert_eq!(entry["params"]["recipient"], json!("+15555550101"));
    assert_eq!(entry["params"]["account"], json!(number));
    assert_eq!(
        entry["params"]["contact"]["number"][0]["value"],
        json!("+15550001111")
    );
    // avatar 载荷摘要化（调度零头像字节纪律），摘要保留量级——定位带
    // avatar 的全卡片直聊记录断言。
    let with_avatar = entries
        .iter()
        .find(|entry| {
            entry["method"] == "sendContact"
                && entry["params"]["contact"].get("avatar").is_some()
                && entry["params"].get("groupId").is_none()
        })
        .expect("avatar-bearing sendContact logged");
    let avatar_digest = with_avatar["params"]["contact"]["avatar"]
        .as_str()
        .expect("avatar digest");
    assert!(
        avatar_digest.starts_with("[avatar data uri: ") && avatar_digest.ends_with(" bytes]"),
        "avatar 摘要化: {avatar_digest}"
    );
    assert!(
        !avatar_digest.contains("iVBORw0KGgo"),
        "调度日志零头像字节: {avatar_digest}"
    );
    let group_entry = entries
        .iter()
        .find(|entry| entry["method"] == "sendContact" && entry["params"].get("groupId").is_some())
        .expect("group sendContact logged");
    assert_eq!(group_entry["params"]["groupId"], json!(group));
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
/// 形状成功 `{}`（只写提供字段的多形态，含 storiesDisabled B5/linkPreviews
/// B6 单写位 + 未知键静默忽略）+ 参数面 fail-closed 逐形状（-32603：缺
/// account、布尔字段非布尔、phoneNumberSharingMode 非法/非字符串、零可写
/// 字段——不触网）。
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
    // storiesDisabled 单写（窗D B5：AccountRecord.storiesDisabled(29)）。
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "storiesDisabled": true }),
        )["result"],
        json!({})
    );
    // linkPreviews 单写（窗D B6：AccountRecord.linkPreviews(11)）。
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "linkPreviews": true }),
        )["result"],
        json!({})
    );
    // 未知参数键静默忽略纪律（窗D B6 ④）：合法位 + 未知键同场 = 合法位
    // 照常生效、未知键不炸面。
    assert_eq!(
        engine.request(
            "setAccountConfiguration",
            json!({ "account": number, "linkPreviews": false, "unknownFutureKey": 1 }),
        )["result"],
        json!({})
    );

    // 参数面 fail-closed 逐形状。
    let rejects = [
        json!({ "readReceipts": true }),
        json!({ "account": number, "readReceipts": "yes" }),
        json!({ "account": number, "typingIndicators": 1 }),
        json!({ "account": number, "unlistedPhoneNumber": "false" }),
        json!({ "account": number, "storiesDisabled": "yes" }),
        json!({ "account": number, "linkPreviews": "yes" }),
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

/// getConfiguration（账号级设置读投影，窗D B6）：fake 模式恒全缺省面
///（五布尔键恒在 + phoneNumberSharingMode 键缺席——UNKNOWN=未设口径）；
/// 参数面 fail-closed -32603（account 缺席/非字符串）。写后回显链（写面
/// 产出记录 → 读投影回显）由 bin 单测 account_configuration_face_projects_
/// record_and_write_echo 钉形（fake 无 storage 态不回显）。
#[test]
fn get_configuration_fake_shape_and_param_fail_closed() {
    let dir = temp_dir("get-account-config");
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

    // fake 读：全缺省面（六位镜像形状钉定）。
    assert_eq!(
        engine.request("getConfiguration", json!({ "account": number }))["result"],
        json!({
            "readReceipts": false,
            "typingIndicators": false,
            "linkPreviews": false,
            "unlistedPhoneNumber": false,
            "storiesDisabled": false,
        })
    );

    // 参数面 fail-closed：account 缺席/非字符串 → -32603。
    let (code, _) = engine.error_of("getConfiguration", json!({}));
    assert_eq!(code, -32603);
    let (code, _) = engine.error_of("getConfiguration", json!({ "account": 1 }));
    assert_eq!(code, -32603);
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

/// listGroups 可选 groupId 过滤（P15b 审计项5 单群读投影，wire additive）：
/// 缺席/null = 全表（fake 恒空表）；well-formed 过滤过参数闸（fake 空
/// store 回空表）；坏形状 fail-closed -32603（fake 面同口径）——空数组/
/// 条目非字符串/坏 base64/≠32B/标量形/混合坏条目整拒。well-formed 但
/// store 不在的 id 掉出结果（不报错，connector fold 侧诚实降级）。
#[test]
fn list_groups_group_id_filter_param_face() {
    let dir = temp_dir("list-groups-filter");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let valid = "UVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVFRUVE="; // [0x51;32]
    let valid_b = "Ug=="; // 1 字节：合法 base64 但 ≠32B
    assert_eq!(
        engine.request("listGroups", json!({ "account": "+15550000000" }))["result"],
        json!([]),
        "缺席 = 全表"
    );
    assert_eq!(
        engine.request(
            "listGroups",
            json!({ "account": "+15550000000", "groupId": null })
        )["result"],
        json!([]),
        "显式 null 同缺席"
    );
    assert_eq!(
        engine.request(
            "listGroups",
            json!({ "account": "+15550000000", "groupId": [valid] })
        )["result"],
        json!([]),
        "well-formed 过滤过闸（fake 空 store 空表）"
    );
    for bad in [
        json!([]),
        json!([42]),
        json!(["not-base64!!"]),
        json!([valid_b]),
        json!(valid),
        json!([valid, valid_b]),
    ] {
        let (code, message) = engine.error_of(
            "listGroups",
            json!({ "account": "+15550000000", "groupId": bad }),
        );
        assert_eq!(
            (code, message.as_str()),
            (-32603, "Internal error"),
            "坏形状整拒：{bad}",
        );
    }
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

/// editMessage 契约面（2026-10-11 六问批，官方编辑发送面对位）：fake 合法面
/// （直聊/群组两腿）回 `{timestamp, results}` + 调度日志逐键保留；参数面
/// fail-closed 逐形状（-32603：message 空/缺席/非字符串、targetTimestamp
/// 缺席/非数字、account 缺席、两腿同置/都缺、recipient 条目非字符串）。
/// 24h 窗口/次数闸是官方 UI 层闸，wire 面不设（compat §5 登记）。
#[test]
fn edit_message_param_faces_and_dispatch_log() {
    let dir = temp_dir("edit-message");
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

    // 直聊合法面：{timestamp, results} 与 send 同构。
    let ok = engine.request(
        "editMessage",
        json!({ "account": number, "recipient": ["+15555550101"], "targetTimestamp": 42, "message": "edited body" }),
    );
    assert!(ok["result"]["timestamp"].is_u64(), "direct edit ok");
    assert!(ok["result"]["results"].is_array(), "results face");

    // 群组合法面（32B master key 标准 base64）。
    let group_ok = engine.request(
        "editMessage",
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "targetTimestamp": 42, "message": "group edited" }),
    );
    assert!(group_ok["result"]["timestamp"].is_u64(), "group edit ok");

    let rejects = [
        // message 空/缺席/非字符串（官方 canEditMessage body 非空闸）。
        json!({ "account": number, "recipient": ["+15555550101"], "targetTimestamp": 42, "message": "" }),
        json!({ "account": number, "recipient": ["+15555550101"], "targetTimestamp": 42 }),
        json!({ "account": number, "recipient": ["+15555550101"], "targetTimestamp": 42, "message": 7 }),
        // targetTimestamp 缺席/非数字。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x" }),
        json!({ "account": number, "recipient": ["+15555550101"], "targetTimestamp": "42", "message": "x" }),
        // account 缺席。
        json!({ "recipient": ["+15555550101"], "targetTimestamp": 42, "message": "x" }),
        // 两腿同置/都缺。
        json!({ "account": number, "recipient": ["+15555550101"], "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "targetTimestamp": 42, "message": "x" }),
        json!({ "account": number, "targetTimestamp": 42, "message": "x" }),
        // recipient 条目非字符串。
        json!({ "account": number, "recipient": [42], "targetTimestamp": 42, "message": "x" }),
    ];
    for params in &rejects {
        let response = engine.request("editMessage", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "editMessage rejects {params}"
        );
    }

    // 调度日志：合法派发逐键保留（含非法派发，send 系纪律同款）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| {
            entry["method"] == "editMessage" && entry["params"]["message"] == json!("edited body")
        })
        .expect("editMessage logged");
    assert_eq!(entry["params"]["targetTimestamp"], json!(42));
    assert_eq!(entry["params"]["recipient"], json!(["+15555550101"]));
    assert_eq!(entry["params"]["account"], json!(number));
}

/// sendGroupCallUpdate 契约面（同批，官方群通话开始/结束信令面）：fake 成功
/// `{}`（remoteDelete 同风格）+ 调度日志逐键保留；参数面 fail-closed 逐形状
/// （-32603：groupId 缺席/形状错、eraId 缺席/空串/非字符串、account 缺席）。
#[test]
fn send_group_call_update_param_faces_and_dispatch_log() {
    let dir = temp_dir("group-call-update");
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

    // 合法面：成功 `{}`。
    let ok = engine.request(
        "sendGroupCallUpdate",
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "eraId": "era-1" }),
    );
    assert_eq!(ok["result"], json!({}), "fake success (remoteDelete style)");

    let rejects = [
        json!({ "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "eraId": "era-1" }),
        json!({ "account": number, "eraId": "era-1" }),
        json!({ "account": number, "groupId": "not-base64!!", "eraId": "era-1" }),
        json!({ "account": number, "groupId": "QUJD", "eraId": "era-1" }),
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" }),
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "eraId": "" }),
        json!({ "account": number, "groupId": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "eraId": 7 }),
    ];
    for params in &rejects {
        let response = engine.request("sendGroupCallUpdate", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "sendGroupCallUpdate rejects {params}"
        );
    }

    // 调度日志：eraId 逐键保留。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["method"] == "sendGroupCallUpdate")
        .expect("sendGroupCallUpdate logged");
    assert_eq!(entry["params"]["eraId"], json!("era-1"));
    assert_eq!(entry["params"]["account"], json!(number));
}

/// Polls 发送三臂契约面（窗D A7，官方 poll 发送面对位）：fake 成功 `{}` +
/// 调度日志逐键保留；参数面 fail-closed 逐形状（-32603：groupId 缺席/形状
/// 错、question/options/optionIndexes 官方 PollCreateSchema·PollVoteSchema
/// 同域闸、targetTimestamp 非正、voteCount 超 u32、allowMultiple/voteCount
/// 非布尔非数值）。官方桌面仅群聊形态，三方法无直聊腿。
#[test]
fn poll_send_param_faces_and_dispatch_log() {
    let dir = temp_dir("poll-send");
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
    let group = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    // 合法面：create（allowMultiple 缺省 false）/ vote（voteCount 缺省 1、
    // 空数组 = 清票）/ terminate。
    let ok_create = engine.request(
        "sendPollCreate",
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen", "Salad"] }),
    );
    assert_eq!(ok_create["result"], json!({}), "fake create success");
    let ok_create_full = engine.request(
        "sendPollCreate",
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen", "Salad"], "allowMultiple": true }),
    );
    assert_eq!(ok_create_full["result"], json!({}));
    let ok_vote = engine.request(
        "sendPollVote",
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": [1, 2] }),
    );
    assert_eq!(ok_vote["result"], json!({}), "fake vote success");
    let ok_vote_clear = engine.request(
        "sendPollVote",
        json!({ "account": number, "groupId": group, "targetAuthor": "11111111-2222-4333-8444-555555555555", "targetTimestamp": 42, "optionIndexes": [], "voteCount": 3 }),
    );
    assert_eq!(ok_vote_clear["result"], json!({}), "clearing vote legal");
    let ok_terminate = engine.request(
        "sendPollTerminate",
        json!({ "account": number, "groupId": group, "targetTimestamp": 42 }),
    );
    assert_eq!(ok_terminate["result"], json!({}), "fake terminate success");

    let rejects = [
        // account 缺席（三臂同面，抽 create 代表）。
        json!({ "groupId": group, "question": "Lunch?", "options": ["Ramen", "Salad"] }),
        // groupId 缺席/非 base64/≠32B（无直聊腿，无 groupId 一律拒）。
        json!({ "account": number, "question": "Lunch?", "options": ["Ramen", "Salad"] }),
        json!({ "account": number, "groupId": "not-base64!!", "question": "Lunch?", "options": ["Ramen", "Salad"] }),
        json!({ "account": number, "groupId": "QUJD", "question": "Lunch?", "options": ["Ramen", "Salad"] }),
        // question：缺席/空/超 200 字符/非字符串。
        json!({ "account": number, "groupId": group, "options": ["Ramen", "Salad"] }),
        json!({ "account": number, "groupId": group, "question": "", "options": ["Ramen", "Salad"] }),
        json!({ "account": number, "groupId": group, "question": "x".repeat(201), "options": ["Ramen", "Salad"] }),
        json!({ "account": number, "groupId": group, "question": 7, "options": ["Ramen", "Salad"] }),
        // options：缺席/单条/11 条/空串条目/超 100 字符条目/非数组。
        json!({ "account": number, "groupId": group, "question": "Lunch?" }),
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen"] }),
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": (0..11).map(|i| i.to_string()).collect::<Vec<_>>() }),
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen", ""] }),
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen", "x".repeat(101)] }),
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": "Ramen" }),
        // allowMultiple 非布尔。
        json!({ "account": number, "groupId": group, "question": "Lunch?", "options": ["Ramen", "Salad"], "allowMultiple": "yes" }),
        // vote：targetAuthor/targetTimestamp/optionIndexes 缺席，optionIndexes
        // 条目超 9、条目非数值、>10 条，targetTimestamp 非正，voteCount 超 u32。
        json!({ "account": number, "groupId": group, "targetTimestamp": 42, "optionIndexes": [1] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "optionIndexes": [1] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42 }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": [10] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": ["1"] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 0, "optionIndexes": [1] }),
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": [1], "voteCount": 4294967296u64 }),
        // voteCount 非数值。
        json!({ "account": number, "groupId": group, "targetAuthor": "+15555550101", "targetTimestamp": 42, "optionIndexes": [1], "voteCount": "2" }),
        // terminate：targetTimestamp 缺席/非正。
        json!({ "account": number, "groupId": group }),
        json!({ "account": number, "groupId": group, "targetTimestamp": 0 }),
    ];
    let methods = [
        "sendPollCreate",
        "sendPollVote",
        "sendPollTerminate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollCreate",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollVote",
        "sendPollTerminate",
        "sendPollTerminate",
    ];
    assert_eq!(rejects.len(), methods.len(), "reject rows aligned");
    for (method, params) in methods.iter().zip(rejects.iter()) {
        let response = engine.request(method, params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "{method} rejects {params}"
        );
    }

    // 调度日志：三臂逐键保留（send 系纪律）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let create = entries
        .iter()
        .find(|entry| entry["method"] == "sendPollCreate")
        .expect("sendPollCreate logged");
    assert_eq!(create["params"]["question"], json!("Lunch?"));
    assert_eq!(create["params"]["options"], json!(["Ramen", "Salad"]));
    let vote = entries
        .iter()
        .find(|entry| entry["method"] == "sendPollVote" && entry["params"]["voteCount"] == json!(3))
        .expect("sendPollVote logged");
    assert_eq!(vote["params"]["optionIndexes"], json!([]));
    assert_eq!(
        vote["params"]["targetAuthor"],
        json!("11111111-2222-4333-8444-555555555555")
    );
    let terminate = entries
        .iter()
        .find(|entry| entry["method"] == "sendPollTerminate")
        .expect("sendPollTerminate logged");
    assert_eq!(terminate["params"]["targetTimestamp"], json!(42));
}

/// send quoteType/quoteBodyRanges 契约面（Q5 扩面，官方 Quote.type 恒写 +
/// bodyRanges toBodyRange 同构）：fake 合法面成功 + 调度日志逐键保留；参数面
/// fail-closed 逐形状（-32603：quoteType 词表外（含 poll——官方 Quote.Type
/// POLL 枚举在，KT quote 发送面未扩，窗D A7 批记档）/非字符串、quoteBodyRanges
/// 条目缺 start/length、start/length 超 u32、
/// mention/style 同置或都缺、style 词表外、>64 条、非数组、quote 键对缺席
/// 时挂 type/bodyRanges）。
#[test]
fn send_quote_type_and_body_ranges_param_faces_and_dispatch_log() {
    let dir = temp_dir("quote-type");
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

    // 合法面：giftBadge 词 + mention/style 混合条目。
    let ok = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "reply",
            "quoteTimestamp": 42,
            "quoteAuthor": "+15555550101",
            "quoteType": "giftBadge",
            "quoteBodyRanges": [
                { "number": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "start": 0, "length": 3 },
                { "style": "BOLD", "start": 4, "length": 2 }
            ]
        }),
    );
    assert!(ok["result"]["timestamp"].is_u64(), "quote type send ok");

    // 缺省词（normal）+ 空数组合法（无 bodyRanges 语义）。
    let plain = engine.request(
        "send",
        json!({
            "account": number,
            "recipient": ["+15555550101"],
            "message": "plain quote",
            "quoteTimestamp": 42,
            "quoteAuthor": "+15555550101",
            "quoteType": "normal",
            "quoteBodyRanges": []
        }),
    );
    assert!(plain["result"]["timestamp"].is_u64(), "normal word ok");

    let rejects = [
        // quoteType 词表外（poll 引擎无发送面）/非字符串。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteType": "poll" }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteType": 7 }),
        // quoteBodyRanges 条目缺 start/length。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "number": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "length": 3 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "style": "BOLD", "start": 0 }] }),
        // start/length 超 u32。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "style": "BOLD", "start": 4294967296u64, "length": 1 }] }),
        // mention/style 同置。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "number": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "style": "BOLD", "start": 0, "length": 1 }] }),
        // mention/style 都缺。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "start": 0, "length": 1 }] }),
        // style 词表外（含 NONE——composer 永不发出的 no-op 标记）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "style": "MAGIC", "start": 0, "length": 1 }] }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": [{ "style": "NONE", "start": 0, "length": 1 }] }),
        // 非数组。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": "x" }),
        // >64 条（cap 镜像整拒不截断）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteTimestamp": 42, "quoteAuthor": "+15555550101", "quoteBodyRanges": (0..65).map(|i| json!({ "style": "BOLD", "start": i, "length": 1 })).collect::<Vec<_>>() }),
        // quote 键对缺席时挂 type/bodyRanges（无处挂载 fail-closed）。
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteType": "giftBadge" }),
        json!({ "account": number, "recipient": ["+15555550101"], "message": "x", "quoteBodyRanges": [{ "style": "BOLD", "start": 0, "length": 1 }] }),
    ];
    for params in &rejects {
        let response = engine.request("send", params.clone());
        assert_eq!(
            response["error"]["code"], -32603,
            "quote faces reject {params}"
        );
    }

    // 调度日志：quoteType/quoteBodyRanges 数组逐字保留（IPC 形状锚）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let entry = entries
        .iter()
        .find(|entry| entry["params"].get("quoteType") == Some(&json!("giftBadge")))
        .expect("quote type send logged");
    assert_eq!(
        entry["params"]["quoteBodyRanges"],
        json!([
            { "number": "6a2cb17f-ef7c-4a5d-9c69-6f8b2b1f88ae", "start": 0, "length": 3 },
            { "style": "BOLD", "start": 4, "length": 2 }
        ]),
        "quoteBodyRanges array verbatim"
    );
}

/// deleteForMeConversation（P15c 项3，官方 deleteConversation 发送面同位）：
/// fake 模式两臂成功形状桩 `{status: "sent"}`；参数面 fail-closed -32603：
/// kind 词表（contact/group，legacyGroup 官方非 GV2 群不支持同拒）、contact
/// 标识 service-id/E164 双形态闸、group 32B master key 闸、localOnly 非布尔、
/// 全量清空臂 mostRecentMessages 必填非空 ≤5、仅本地臂 mostRecent* 必须缺席、
/// 条目 author/timestamp 形状闸。协议上行面落调度日志（send 系同纪律）。
#[test]
fn delete_for_me_conversation_faces_and_param_fail_closed() {
    let dir = temp_dir("delete-for-me-conversation");
    let mut engine = Engine::spawn_with_env(&dir, FAKE_SERVICE);
    let account = "+15550000000";
    let aci = "11111111-2222-4333-8444-555555555555";

    // 全量清空臂（contact service-id）：成功形状桩。
    assert_eq!(
        engine.request(
            "deleteForMeConversation",
            json!({ "account": account, "kind": "contact", "conversationId": aci,
                    "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        )["result"],
        json!({ "status": "sent" })
    );
    // E164 会话 + E164 作者 + nonExpiring 伴随数组。
    assert_eq!(
        engine.request(
            "deleteForMeConversation",
            json!({ "account": account, "kind": "contact", "conversationId": "+15555550101",
                    "mostRecentMessages": [{ "author": "+15555550101", "timestamp": 42 }],
                    "mostRecentNonExpiringMessages": [{ "author": aci, "timestamp": 43 }] }),
        )["result"],
        json!({ "status": "sent" })
    );
    // 群形态（32B master key）。
    let group_id = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    assert_eq!(
        engine.request(
            "deleteForMeConversation",
            json!({ "account": account, "kind": "group", "conversationId": group_id,
                    "mostRecentMessages": [
                        { "author": aci, "timestamp": 42 },
                        { "author": aci, "timestamp": 43 }
                    ] }),
        )["result"],
        json!({ "status": "sent" })
    );
    // 仅本地臂：mostRecent* 缺席成功。
    assert_eq!(
        engine.request(
            "deleteForMeConversation",
            json!({ "account": account, "kind": "contact", "conversationId": aci,
                    "localOnly": true }),
        )["result"],
        json!({ "status": "sent" })
    );

    let rejects = [
        // 缺 account / kind / conversationId。
        json!({ "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        json!({ "account": account, "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        json!({ "account": account, "kind": "contact",
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        // kind 词表外 / legacyGroup（官方非 GV2 群不支持）。
        json!({ "account": account, "kind": "bogus", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        json!({ "account": account, "kind": "legacyGroup", "conversationId": "abc",
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        // contact 标识既非 service-id 也非 E164。
        json!({ "account": account, "kind": "contact", "conversationId": "not-an-id",
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        // group 非 base64 / ≠32B。
        json!({ "account": account, "kind": "group", "conversationId": "%%%",
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        json!({ "account": account, "kind": "group", "conversationId": "AAA=",
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        // localOnly 非布尔。
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "localOnly": "yes" }),
        // 全量清空臂：mostRecentMessages 缺席/空批/非数组/超 5 条。
        json!({ "account": account, "kind": "contact", "conversationId": aci }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": "x" }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": (0..6).map(|i| json!({ "author": aci, "timestamp": i + 1 })).collect::<Vec<_>>() }),
        // 全量清空臂：mostRecentNonExpiringMessages 超 5 条。
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }],
                "mostRecentNonExpiringMessages": (0..6).map(|i| json!({ "author": aci, "timestamp": i + 1 })).collect::<Vec<_>>() }),
        // 条目形状：author 不可解析（非 service-id 非 E164）/缺失、timestamp
        // 缺失/0/负数/非整数、条目非对象、数组非对象形状。
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": "whoami", "timestamp": 42 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "timestamp": 42 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 0 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": -1 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [{ "author": aci, "timestamp": 1.5 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": [42] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "mostRecentMessages": { "author": aci } }),
        // 仅本地臂：mostRecent* 必须缺席（官方 local-only 分支无消息位）。
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "localOnly": true,
                "mostRecentMessages": [{ "author": aci, "timestamp": 42 }] }),
        json!({ "account": account, "kind": "contact", "conversationId": aci,
                "localOnly": true,
                "mostRecentNonExpiringMessages": [{ "author": aci, "timestamp": 42 }] }),
    ];
    for params in &rejects {
        assert_eq!(
            engine.error_of("deleteForMeConversation", params.clone()).0,
            -32603,
            "deleteForMeConversation reject {params}"
        );
    }

    // 调度日志：协议上行面逐字保留（send 系同纪律）。
    let log_path = dir.join("engine-send-log.jsonl");
    let log = std::fs::read_to_string(&log_path).expect("dispatch log exists");
    let entries: Vec<Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("log line JSON"))
        .collect();
    let logged = entries
        .iter()
        .find(|entry| entry["method"] == "deleteForMeConversation")
        .expect("deleteForMeConversation logged");
    assert_eq!(logged["params"]["kind"], json!("contact"));
    let logged_local = entries
        .iter()
        .find(|entry| {
            entry["method"] == "deleteForMeConversation"
                && entry["params"]["localOnly"] == json!(true)
        })
        .expect("localOnly arm logged");
    assert_eq!(logged_local["params"]["localOnly"], json!(true));
}
