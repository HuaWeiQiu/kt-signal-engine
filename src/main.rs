// SPDX-License-Identifier: AGPL-3.0-only

//! 进程装配：argv 解析（严格模式）+ 出口代理 env 映射 + 异步 stdio 主循环。
//!
//! 启动契约见 `docs/jsonrpc-compat.md` §1：connector 以
//! `--data-dir <dir> jsonRpc [--receive-mode on-start] [--ignore-attachments]
//! --ignore-stories --ignore-stickers` 拉起本进程；JVM 相关环境变量必须被
//! 忽略。可选 `KT_SIGNAL_SOCKS_PROXY=host:port`（connector 与桌面探测链路
//! 注入的同一变量）映射为 `ALL_PROXY=socks5h://…`——必须用 socks5h（远端
//! 解析）而非 socks5（本地解析）：被墙环境下本地 DNS 对 Signal 域名投毒，
//! 本地解析拿到假 IP 再经代理 CONNECT 仍连到假地址（JVM signal-cli 同坑）；
//! 远端解析把域名交给代理出口侧解析。由 libsignal-service 的
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

mod history;
mod jsonrpc;
mod methods;
mod proto;
mod service;
mod state;

// Android 本地备份导出面（engine-compat 备份批）：内部 Rust API，装配链由
// 消费批任务层（service::backup_task，契约 §10.3/§10.4）接线。
mod backup;

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

/// 校验 host:port 并返回 `socks5h://host:port` 代理 URL。口径与 connector
/// 侧 `SocksProxy::parse` 一致：host 非空、无空白与 ':'、≤253 字节；端口
/// 1-65535。socks5h = 域名在代理侧远端解析（reqwest 语义），防本地 DNS
/// 污染把 CONNECT 指到假 IP。
fn socks_proxy_url(value: &str) -> Result<String, &'static str> {
    let (host, port) = value.trim().split_once(':').ok_or("expected host:port")?;
    if host.is_empty() || host.len() > 253 || host.chars().any(|c| c.is_whitespace() || c == ':') {
        return Err("invalid proxy host");
    }
    let port: u16 = port.parse().map_err(|_| "invalid proxy port")?;
    if port == 0 {
        return Err("invalid proxy port");
    }
    Ok(format!("socks5h://{host}:{port}"))
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

    // stdout writer：响应与 receive 通知的唯一出口（行序 = 队列序）。Notice
    // 逐条写+flush；Flush 是背压栅栏（此前全部 Notice 已写进 stdout 并 flush
    // ——连接器至少收到字节——才解决 oneshot）。receive 管线 ack 前等栅栏
    // （时序审计 G1：ack 晚于「连接器可读」，连接器死亡窗口内引擎崩溃也不
    // 丢已投影消息——服务器队列未耗，重启重放）。
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<OutboundFrame>();
    let writer = tokio::spawn(run_outbound_writer(
        outbound_rx,
        tokio::io::BufWriter::new(tokio::io::stdout()),
    ));

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
                    let _ = outbound.send(OutboundFrame::Notice(response));
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

/// stdout 出站帧。`Notice` 是一行 JSON（响应或 receive 通知）；`Flush` 是
/// 时序审计 G1 的背压栅栏——writer 在此前全部 Notice 已写并 flush 后解决
/// oneshot，消费方（receive 管线 ack）据此把「连接器至少收到」变成显式
/// 等待点。
pub(crate) enum OutboundFrame {
    Notice(Value),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// writer 主循环：Notice 逐条 write+flush（任一步失败即退出，栅栏 oneshot
/// 随帧弃置自然落在等待方 false）；Flush 就地解决（FIFO 保证此前 Notice
/// 已 flush）。
async fn run_outbound_writer<W>(mut rx: mpsc::UnboundedReceiver<OutboundFrame>, mut out: W)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    while let Some(frame) = rx.recv().await {
        match frame {
            OutboundFrame::Notice(value) => {
                let mut text = serde_json::to_string(&value).expect("line serializes");
                text.push('\n');
                if out.write_all(text.as_bytes()).await.is_err() || out.flush().await.is_err() {
                    break;
                }
            }
            OutboundFrame::Flush(done) => {
                let _ = done.send(());
            }
        }
    }
}

/// flush 栅栏的通道侧实现（`Service::flush_barrier` 委托；自由函数便于
/// 单测直打协议面）。出口关闭立即 false；writer 已死则栅栏 oneshot 随
/// 接收端弃置，等待落 false。
pub(crate) async fn outbound_flush_barrier(tx: &mpsc::UnboundedSender<OutboundFrame>) -> bool {
    let (done, wait) = tokio::sync::oneshot::channel();
    if tx.send(OutboundFrame::Flush(done)).is_err() {
        return false;
    }
    wait.await.is_ok()
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
    use super::{outbound_flush_barrier, run_outbound_writer, OutboundFrame};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[test]
    fn socks_proxy_url_accepts_host_port() {
        assert_eq!(
            socks_proxy_url("127.0.0.1:7890").as_deref(),
            Ok("socks5h://127.0.0.1:7890")
        );
        assert_eq!(
            socks_proxy_url(" relay.example.com:1080 ").as_deref(),
            Ok("socks5h://relay.example.com:1080")
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

    /// G1 ack 时序面：栅栏在先入 Notice 落盘（write+flush）之后才解决——
    /// writer 逐条 flush，FIFO 队列序下 Flush 帧被弹出即代表此前全部通知
    /// 已出进程。栅栏等待方拿到 true 且字节已在 sink。
    #[tokio::test]
    async fn flush_barrier_resolves_only_after_prior_notices_flushed() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrame>();
        let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer_sink = std::sync::Arc::clone(&sink);
        let writer = tokio::spawn(async move {
            // Vec<u8> 直写无法断言中间态，包一层把 write 缓冲在 flush 时
            // 才并入共享 sink 的探针（flush 语义可断言）。
            struct Probe(std::sync::Arc<std::sync::Mutex<Vec<u8>>>, Vec<u8>);
            impl tokio::io::AsyncWrite for Probe {
                fn poll_write(
                    mut self: Pin<&mut Self>,
                    _cx: &mut Context<'_>,
                    buf: &[u8],
                ) -> Poll<std::io::Result<usize>> {
                    self.1.extend_from_slice(buf);
                    Poll::Ready(Ok(buf.len()))
                }
                fn poll_flush(
                    mut self: Pin<&mut Self>,
                    _cx: &mut Context<'_>,
                ) -> Poll<std::io::Result<()>> {
                    let inner = std::mem::take(&mut self.1);
                    self.0.lock().unwrap().extend(inner);
                    Poll::Ready(Ok(()))
                }
                fn poll_shutdown(
                    self: Pin<&mut Self>,
                    _cx: &mut Context<'_>,
                ) -> Poll<std::io::Result<()>> {
                    Poll::Ready(Ok(()))
                }
            }
            run_outbound_writer(rx, Probe(writer_sink, Vec::new())).await
        });
        tx.send(OutboundFrame::Notice(serde_json::json!({"m": 1})))
            .unwrap();
        assert!(outbound_flush_barrier(&tx).await);
        let written = sink.lock().unwrap().clone();
        assert_eq!(written, b"{\"m\":1}\n".to_vec());
        drop(tx);
        let _ = writer.await;
    }

    /// G1 ack 时序面：出口死亡（writer 因写失败退出 / 通道关闭）→ 栅栏
    /// 落 false——receive 管线据此放弃 ack，信封留服务器队列待重启重放。
    #[tokio::test]
    async fn flush_barrier_fails_when_outbound_dead() {
        // 通道关闭臂：接收端已弃。
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrame>();
        drop(rx);
        assert!(!outbound_flush_barrier(&tx).await);

        // writer 死亡臂：sink 写失败 → writer 退出 → 在途栅栏 oneshot 弃置。
        struct BrokenSink;
        impl tokio::io::AsyncWrite for BrokenSink {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                Poll::Ready(Err(std::io::Error::other("sink broken")))
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrame>();
        tokio::spawn(run_outbound_writer(rx, BrokenSink));
        tx.send(OutboundFrame::Notice(serde_json::json!({"m": 1})))
            .unwrap();
        let mut died = false;
        for _ in 0..100 {
            if !outbound_flush_barrier(&tx).await {
                died = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(died, "barrier must fail once the writer has died");
    }
}
