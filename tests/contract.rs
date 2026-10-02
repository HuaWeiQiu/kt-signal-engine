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

    assert_eq!(
        engine.request("listAccounts", json!({}))["result"],
        json!([])
    );
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
            "updateContact"
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
