// SPDX-License-Identifier: AGPL-3.0-only

//! 进程装配：argv 解析（严格模式）+ 出口代理 env 映射 + 异步 stdio 主循环。
//!
//! 启动契约见 `docs/jsonrpc-compat.md` §1：connector 以
//! `--data-dir <dir> jsonRpc [--receive-mode on-start] [--ignore-attachments]
//! --ignore-stories --ignore-stickers` 拉起本进程；JVM 相关环境变量必须被
//! 忽略。可选 `KT_SIGNAL_SOCKS_PROXY=host:port`（connector 与桌面探测链路
//! 注入的同一变量）映射为 `ALL_PROXY=socks5://…`，由 libsignal-service 的
//! reqwest 传输面（HTTP + WebSocket 同一 client）统一走 SOCKS5 出口；非法
//! 值 exit 2。stdin EOF 是唯一干净退出路径（exit 0）；argv 非法 exit 2；
//! 内部致命 exit 1。stderr 保持安静（connector watchdog 以 stderr ERROR 行
//! 判死活）。
//!
//! P1b 装配：stdin 有界读搬到阻塞线程（P1a 逻辑原样），经 channel 桥进
//! tokio；响应与 receive 通知共用单一 stdout writer 队列（行序由队列保证，
//! 连接器按 id/有无 id 路由）；请求 dispatch 与接收管线跑在本线程 LocalSet
//! （presage future 持有非 Send 状态，进不了 tokio::spawn），finishLink
//! 阻塞不饿死其他方法；启动即为每个已登记账号拉起接收管线。

mod jsonrpc;
mod methods;
mod service;
mod state;

use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use methods::Inbound;
use state::EngineState;

/// 分帧上限（1 MiB）：connector 侧请求最大为 send 附件前文本，远小于此；
/// 超限行按 -32700 拒绝并跳到下一行，进程不死。
const MAX_LINE_BYTES: usize = 1024 * 1024;

const USAGE: &str = "usage: kt-signal-engine --data-dir <dir> jsonRpc \
[--receive-mode <mode>] [--ignore-attachments] [--ignore-stories] [--ignore-stickers]";

struct LaunchArgs {
    data_dir: PathBuf,
}

/// 严格 argv 解析：未知参数一律失败（与上游对未知参数 fail-closed 同口径）。
/// JVM 模式的 `-D…` 前缀参数是 native 启动器形态，引擎非 JVM，同样拒绝。
fn parse_launch_args<I: Iterator<Item = std::ffi::OsString>>(args: I) -> Result<LaunchArgs, ()> {
    let mut data_dir: Option<PathBuf> = None;
    let mut saw_json_rpc = false;
    let mut iter = args;
    while let Some(arg) = iter.next() {
        match arg.to_str() {
            Some("--data-dir") => {
                let value = iter.next().ok_or(())?;
                data_dir = Some(PathBuf::from(value));
            }
            Some("--receive-mode") => {
                iter.next().ok_or(())?;
            }
            Some("--ignore-attachments" | "--ignore-stories" | "--ignore-stickers") => {}
            Some("jsonRpc") => saw_json_rpc = true,
            _ => return Err(()),
        }
    }
    if !saw_json_rpc || data_dir.is_none() {
        return Err(());
    }
    Ok(LaunchArgs {
        data_dir: data_dir.unwrap_or_default(),
    })
}

/// 出口代理注入变量：connector 以同一变量把代理组代理传给引擎子进程（与
/// signal-cli JVM 形态共用环境面），桌面探测链路也写它。
const SOCKS_PROXY_ENV: &str = "KT_SIGNAL_SOCKS_PROXY";

/// 校验 host:port 并返回 `socks5://host:port` 代理 URL。口径与 connector
/// 侧 `SocksProxy::parse` 一致：host 非空、无空白与 ':'、≤253 字节；端口
/// 1-65535。
fn socks_proxy_url(value: &str) -> Result<String, &'static str> {
    let (host, port) = value.trim().split_once(':').ok_or("expected host:port")?;
    if host.is_empty() || host.len() > 253 || host.chars().any(|c| c.is_whitespace() || c == ':') {
        return Err("invalid proxy host");
    }
    let port: u16 = port.parse().map_err(|_| "invalid proxy port")?;
    if port == 0 {
        return Err("invalid proxy port");
    }
    Ok(format!("socks5://{host}:{port}"))
}

/// `KT_SIGNAL_SOCKS_PROXY` → `ALL_PROXY`。必须在任何 libsignal-service
/// reqwest client 构建之前生效（reqwest 缺省读系统/环境代理面，HTTP 与
/// WebSocket 共用同一 client），因此放在两段式 `main` 的同步段：此刻进程
/// 单线程（tokio worker 与 stdin 读线程都尚未启动），set_var 无并发读者。
/// 空值视为未配置（直连）。
fn setup_socks_proxy_env() -> Result<(), &'static str> {
    let Ok(value) = std::env::var(SOCKS_PROXY_ENV) else {
        return Ok(());
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(());
    }
    let url = socks_proxy_url(value)?;
    std::env::set_var("ALL_PROXY", url);
    Ok(())
}

fn main() -> ExitCode {
    if let Err(message) = setup_socks_proxy_env() {
        eprintln!("kt-signal-engine: invalid {SOCKS_PROXY_ENV}: {message}");
        return ExitCode::from(2);
    }
    amain()
}

#[tokio::main]
async fn amain() -> ExitCode {
    let args = match parse_launch_args(std::env::args_os().skip(1)) {
        Ok(args) => args,
        Err(()) => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let state = match EngineState::load(&args.data_dir) {
        Ok(state) => Arc::new(AsyncMutex::new(state)),
        Err(_) => return ExitCode::from(1),
    };

    // stdout writer：响应与 receive 通知的唯一出口（行序 = 队列序）。
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
        use tokio::io::AsyncWriteExt;
        while let Some(value) = outbound_rx.recv().await {
            let mut text = serde_json::to_string(&value).expect("line serializes");
            text.push('\n');
            if stdout.write_all(text.as_bytes()).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });

    // stdin 有界读（阻塞线程；P1a 逻辑原样）→ 桥进 tokio。
    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel::<Inbound>();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut line: Vec<u8> = Vec::new();
        loop {
            line.clear();
            match read_bounded_line(&stdin, &mut line) {
                ReadOutcome::Eof => break,
                ReadOutcome::Line => {
                    while matches!(line.last(), Some(b'\n' | b'\r')) {
                        line.pop();
                    }
                    let text = String::from_utf8_lossy(&line).to_string();
                    if inbound_tx.send(Inbound::Line(text)).is_err() {
                        break;
                    }
                }
                ReadOutcome::Overlong => {
                    if inbound_tx.send(Inbound::Overlong).is_err() {
                        break;
                    }
                }
            }
        }
    });

    // presage 的 Manager/接收流 future 持有非 Send 状态（ThreadRng、
    // dyn ProtocolStore），请求 dispatch 与接收管线统一跑在本线程 LocalSet。
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let service =
                service::Service::new(args.data_dir, Arc::clone(&state), outbound_tx.clone());
            service.spawn_receive_pipelines().await;

            while let Some(inbound) = inbound_rx.recv().await {
                let state = Arc::clone(&state);
                let service = Arc::clone(&service);
                let outbound = outbound_tx.clone();
                tokio::task::spawn_local(async move {
                    let response = methods::respond(&inbound, state, service).await;
                    let _ = outbound.send(response);
                });
            }
        })
        .await;

    // EOF：干净退出 0（在途请求不保证落盘——连接器仅在关闭引擎时断 stdin）。
    drop(outbound_tx);
    writer.abort();
    ExitCode::SUCCESS
}

enum ReadOutcome {
    Line,
    Eof,
    Overlong,
}

/// 有界按行读：不预分配无界缓冲；超限行吞到行尾后报 Overlong。
fn read_bounded_line(stdin: &std::io::Stdin, buf: &mut Vec<u8>) -> ReadOutcome {
    let mut handle = stdin.lock();
    loop {
        let mut byte = [0u8; 1];
        match handle.read(&mut byte) {
            Ok(0) => {
                return if buf.is_empty() {
                    ReadOutcome::Eof
                } else {
                    ReadOutcome::Line
                }
            }
            Ok(_) => {
                if byte[0] == b'\n' {
                    return ReadOutcome::Line;
                }
                buf.push(byte[0]);
                if buf.len() > MAX_LINE_BYTES {
                    // 丢弃到行尾（或 EOF），让读循环继续下一帧。
                    let mut sink = [0u8; 4096];
                    loop {
                        match handle.read(&mut sink) {
                            Ok(0) => return ReadOutcome::Overlong,
                            Ok(n) => {
                                if sink[..n].contains(&b'\n') {
                                    return ReadOutcome::Overlong;
                                }
                            }
                            Err(_) => return ReadOutcome::Overlong,
                        }
                    }
                }
            }
            Err(_) => {
                return if buf.is_empty() {
                    ReadOutcome::Eof
                } else {
                    ReadOutcome::Line
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::socks_proxy_url;

    #[test]
    fn socks_proxy_url_accepts_host_port() {
        assert_eq!(
            socks_proxy_url("127.0.0.1:7890").as_deref(),
            Ok("socks5://127.0.0.1:7890")
        );
        assert_eq!(
            socks_proxy_url(" relay.example.com:1080 ").as_deref(),
            Ok("socks5://relay.example.com:1080")
        );
    }

    #[test]
    fn socks_proxy_url_rejects_bad_shapes() {
        assert!(socks_proxy_url("").is_err());
        assert!(socks_proxy_url("host").is_err());
        assert!(socks_proxy_url("host:notaport").is_err());
        assert!(socks_proxy_url("host:0").is_err());
        assert!(socks_proxy_url(":7890").is_err());
        assert!(socks_proxy_url("host with space:7890").is_err());
        assert!(socks_proxy_url("socks5://host:7890").is_err());
    }
}
