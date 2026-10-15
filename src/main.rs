// SPDX-License-Identifier: AGPL-3.0-only

//! 进程装配：argv 解析（严格模式）+ 出口代理解析链 + 异步 stdio 主循环。
//!
//! 启动契约见 `docs/jsonrpc-compat.md` §1：connector 以
//! `--data-dir <dir> jsonRpc [--receive-mode on-start] [--ignore-attachments]
//! --ignore-stories --ignore-stickers` 拉起本进程；JVM 相关环境变量必须被
//! 忽略。
//!
//! 出口代理解析链（compat §1.1，同步段一次定案，首个命中生效）：① 显式
//! `KT_SIGNAL_SOCKS_PROXY=host:port`（非空非法 exit 2）→ 覆写
//! `ALL_PROXY=socks5h://…`；② 标准 `ALL_PROXY`/`all_proxy`（大写优先，scheme
//! 白名单 socks5/socks5h/http，命中即原样生效不改写）；③ macOS `scutil
//! --proxy` 系统代理（HTTP 路优先于 SOCKS 路，字段缺失/畸形逐路降级，scutil
//! 进程失败整体降级，非 macOS 跳过此级）→ 写 `ALL_PROXY=http://…` 或
//! `socks5h://…`；④ 直连。socks5h 必须用（远端解析）而非 socks5（本地解析）：
//! 被墙环境下本地 DNS 对 Signal 域名投毒，本地解析拿到假 IP 再经代理 CONNECT
//! 仍连到假地址（JVM signal-cli 同坑）；远端解析把域名交给代理出口侧解析。
//! 显式/系统两级统一写 `ALL_PROXY`，由 libsignal-service 的 reqwest 传输面
//! （HTTP + WebSocket 同一 client）统一走该出口。最终网络路径在启动同步段以
//! 恰一条 stderr INFO 行声明（direct/explicit-env/std-env/system-http/
//! system-socks + 具体地址），供排障对账；解析失败只降级不 panic。stdin EOF
//! 是唯一干净退出路径（exit 0）；argv 非法 exit 2；内部致命 exit 1。stderr
//! 除该 INFO 行与代理配置错误行外保持安静（connector watchdog 只把
//! websocket 异常形状的行判死活，INFO 行刻意避开该形状；不落消息正文/号码/
//! URI/密钥）。
//!
//! P1b 装配：stdin 有界读搬到阻塞线程（P1a 逻辑原样），经 channel 桥进
//! tokio；响应与 receive 通知共用单一 stdout writer 队列（行序由队列保证，
//! 连接器按 id/有无 id 路由）；请求 dispatch 与接收管线跑在本线程 LocalSet
//! （presage future 持有非 Send 状态，进不了 tokio::spawn），finishLink
//! 阻塞不饿死其他方法；启动即为每个已登记账号拉起接收管线。

mod history;
mod jsonrpc;
mod logfile;
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
/// signal-cli JVM 形态共用环境面），桌面探测链路也写它。解析链中优先级最高
/// （compat §1.1 第 1 级）。
const SOCKS_PROXY_ENV: &str = "KT_SIGNAL_SOCKS_PROXY";

/// 标准代理环境变量（compat §1.1 第 2 级）：大写优先，小写兜底——与
/// reqwest/hyper-util 的 `get_first_env(["ALL_PROXY", "all_proxy"])` 同序，
/// 引擎决策与传输面实际行为不脱节。
const STD_PROXY_ENV: &str = "ALL_PROXY";
const STD_PROXY_ENV_FALLBACK: &str = "all_proxy";

/// 标准环境变量 scheme 白名单（大小写不敏感）。`https://`/`socks4://` 等
/// hyper-util 虽认但不在本链采纳范围：引擎只对自己能验证形状的值声明路径。
const STD_PROXY_SCHEMES: [&str; 3] = ["socks5", "socks5h", "http"];

/// 代理 host 校验口径（socks_proxy_url 与 scutil 系统代理解析共用）：非空、
/// 无空白与 ':'、≤253 字节——与 connector 侧 `SocksProxy::parse` 一致。
fn is_valid_proxy_host(host: &str) -> bool {
    !host.is_empty() && host.len() <= 253 && !host.chars().any(|c| c.is_whitespace() || c == ':')
}

/// 校验 host:port 并返回 `socks5h://host:port` 代理 URL。socks5h = 域名在
/// 代理侧远端解析（reqwest 语义），防本地 DNS 污染把 CONNECT 指到假 IP。
fn socks_proxy_url(value: &str) -> Result<String, &'static str> {
    let (host, port) = value.trim().split_once(':').ok_or("expected host:port")?;
    if !is_valid_proxy_host(host) {
        return Err("invalid proxy host");
    }
    let port: u16 = port.parse().map_err(|_| "invalid proxy port")?;
    if port == 0 {
        return Err("invalid proxy port");
    }
    Ok(format!("socks5h://{host}:{port}"))
}

/// 标准环境变量命中判定：非空、全值无空白（URI 不含裸空白，含则 reqwest 必
/// 拒——命中会造成日志路径失真）、scheme ∈ 白名单且 `://` 后余量非空。命中
/// 返回 trim 后的原值（原样生效，引擎不改写——reqwest 原生读同一变量）。
fn standard_proxy_env_hit(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(|c| c.is_whitespace()) {
        return None;
    }
    let (scheme, rest) = value.split_once("://")?;
    if rest.is_empty() {
        return None;
    }
    STD_PROXY_SCHEMES
        .iter()
        .any(|allowed| scheme.eq_ignore_ascii_case(allowed))
        .then_some(value)
}

/// `scutil --proxy` 输出解析出的单条系统代理：scheme 取 "http"（→
/// `http://host:port`）或 "socks5h"（→ `socks5h://host:port`）。
struct ScutilProxy {
    scheme: &'static str,
    host: String,
    port: u16,
}

/// 解析 `scutil --proxy` 输出（`key : value` 行的平面字典）。只认六键：
/// HTTPEnable/HTTPProxy/HTTPPort 与 SOCKSEnable/SOCKSProxy/SOCKSPort。HTTP 与
/// SOCKS 两路同时有效时优先 HTTP（http 代理经 CONNECT 同样覆盖 https/wss 流
/// 量）。使能位非 "1"、字段缺失、host 非法（is_valid_proxy_host 口径）或端
/// 口非法（非数值/0）→ 该路作废；HTTP 路作废落 SOCKS 路，两路皆废 → None
/// （调用方降级到链的下一级）。字典包裹行（`<dictionary> {`/`}`）、空白行与
/// 未知键（含 ProxyAutoConfig* PAC 面）自然忽略。
fn parse_scutil_proxy(output: &str) -> Option<ScutilProxy> {
    let mut http_enabled = false;
    let mut http_proxy: Option<&str> = None;
    let mut http_port: Option<&str> = None;
    let mut socks_enabled = false;
    let mut socks_proxy: Option<&str> = None;
    let mut socks_port: Option<&str> = None;
    for line in output.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let (key, value) = (key.trim(), value.trim());
        match key {
            "HTTPEnable" => http_enabled = value == "1",
            "HTTPProxy" => http_proxy = Some(value),
            "HTTPPort" => http_port = Some(value),
            "SOCKSEnable" => socks_enabled = value == "1",
            "SOCKSProxy" => socks_proxy = Some(value),
            "SOCKSPort" => socks_port = Some(value),
            _ => {}
        }
    }
    scutil_leg(http_enabled, http_proxy, http_port, "http")
        .or_else(|| scutil_leg(socks_enabled, socks_proxy, socks_port, "socks5h"))
}

/// scutil 单路（HTTP 或 SOCKS）命中判定：使能 + host/port 双合法，任一缺失
/// 或畸形即整路作废（None），不发明部分命中。
fn scutil_leg(
    enabled: bool,
    host: Option<&str>,
    port: Option<&str>,
    scheme: &'static str,
) -> Option<ScutilProxy> {
    if !enabled {
        return None;
    }
    let host = host?;
    if !is_valid_proxy_host(host) {
        return None;
    }
    let port: u16 = port?.parse().ok()?;
    (port != 0).then_some(ScutilProxy {
        scheme,
        host: host.to_owned(),
        port,
    })
}

/// 代理解析链的最终决策：来源 + 生效的代理地址（Direct 无地址）。
enum ProxyDecision {
    Direct,
    ExplicitEnv(String),
    StdEnv(String),
    SystemHttp(String),
    SystemSocks(String),
}

impl ProxyDecision {
    /// 日志标记词——compat §1.1 契约面，与文档逐一对应。
    fn label(&self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ExplicitEnv(_) => "explicit-env",
            Self::StdEnv(_) => "std-env",
            Self::SystemHttp(_) => "system-http",
            Self::SystemSocks(_) => "system-socks",
        }
    }

    /// 日志明细：代理地址或直连说明。
    fn detail(&self) -> &str {
        match self {
            Self::Direct => "no proxy configured",
            Self::ExplicitEnv(url)
            | Self::StdEnv(url)
            | Self::SystemHttp(url)
            | Self::SystemSocks(url) => url,
        }
    }
}

/// 代理解析链（compat §1.1，优先级从高到低，首个命中生效）。入参全部显式
/// 注入（env 读取与 scutil 读取留在 `setup_proxy_env`），纯函数可单测：
/// - `explicit`：`KT_SIGNAL_SOCKS_PROXY` 原值（None = 未设置）。非空非法 →
///   Err（启动 exit 2，fail-closed）；空值视为未配置。
/// - `all_proxy`/`all_proxy_fallback`：标准变量原值，大写优先。
/// - `scutil_output`：`scutil --proxy` 原始输出；None = 非 macOS 平台或读取
///   失败（该级整体跳过）。
fn resolve_proxy_chain(
    explicit: Option<&str>,
    all_proxy: Option<&str>,
    all_proxy_fallback: Option<&str>,
    scutil_output: Option<&str>,
) -> Result<ProxyDecision, &'static str> {
    if let Some(value) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(ProxyDecision::ExplicitEnv(socks_proxy_url(value)?));
    }
    if let Some(hit) = all_proxy
        .and_then(standard_proxy_env_hit)
        .or_else(|| all_proxy_fallback.and_then(standard_proxy_env_hit))
    {
        return Ok(ProxyDecision::StdEnv(hit.to_owned()));
    }
    if let Some(proxy) = scutil_output.and_then(parse_scutil_proxy) {
        let url = format!("{}://{}:{}", proxy.scheme, proxy.host, proxy.port);
        return Ok(match proxy.scheme {
            "http" => ProxyDecision::SystemHttp(url),
            _ => ProxyDecision::SystemSocks(url),
        });
    }
    Ok(ProxyDecision::Direct)
}

/// `scutil --proxy` 读取（仅 macOS 编译面）。带 2s 上限：scutil 挂死（罕见
/// 的 configd 卡死面）不得阻塞引擎启动——超时杀进程降级。spawn 失败（无
/// scutil）、非零退出、输出非 UTF-8（lossy 兜底后由解析面判废）均降级，
/// 不 panic。
#[cfg(target_os = "macos")]
fn read_scutil_proxy() -> Option<String> {
    use std::io::Read;
    const SCUTIL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    let mut child = std::process::Command::new("scutil")
        .arg("--proxy")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        let mut buffer = Vec::new();
        // 读失败按 EOF 处理：拿到多少算多少，废形交给解析面降级。
        let _ = stdout.read_to_end(&mut buffer);
        let _ = tx.send(buffer);
    });
    let buffer = match rx.recv_timeout(SCUTIL_TIMEOUT) {
        Ok(buffer) => {
            // stdout EOF 即进程已终（scutil 不 fork），就地回收避免僵尸。
            let exited = child.wait().map(|status| status.success()).unwrap_or(false);
            if !exited {
                return None;
            }
            buffer
        }
        Err(_) => {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    };
    Some(String::from_utf8_lossy(&buffer).into_owned())
}

/// 非 macOS：无 scutil 解析面，恒缺（链上第 3 级整体跳过）。
#[cfg(not(target_os = "macos"))]
fn read_scutil_proxy() -> Option<String> {
    None
}

/// 代理解析链执行 + `ALL_PROXY` 落位 + 启动 INFO 行。必须在任何
/// libsignal-service reqwest client 构建之前生效（reqwest 缺省读环境代理
/// 面，HTTP 与 WebSocket 共用同一 client），因此放在两段式 `main` 的同步
/// 段：此刻进程单线程（tokio worker 与 stdin 读线程都尚未启动），set_var
/// 无并发读者。StdEnv 命中不写（变量已在环境里，reqwest 原生读取同语义）；
/// Direct 不写也不清（维持现状语义）。
fn setup_proxy_env() -> Result<(), &'static str> {
    let explicit = std::env::var(SOCKS_PROXY_ENV).ok();
    let all_proxy = std::env::var(STD_PROXY_ENV).ok();
    let all_proxy_fallback = std::env::var(STD_PROXY_ENV_FALLBACK).ok();
    let scutil_output = read_scutil_proxy();
    let decision = resolve_proxy_chain(
        explicit.as_deref(),
        all_proxy.as_deref(),
        all_proxy_fallback.as_deref(),
        scutil_output.as_deref(),
    )?;
    match &decision {
        ProxyDecision::ExplicitEnv(url)
        | ProxyDecision::SystemHttp(url)
        | ProxyDecision::SystemSocks(url) => {
            std::env::set_var(STD_PROXY_ENV, url);
        }
        ProxyDecision::StdEnv(_) | ProxyDecision::Direct => {}
    }
    eprintln!(
        "kt-signal-engine: INFO network path: {} ({})",
        decision.label(),
        decision.detail()
    );
    Ok(())
}

fn main() -> ExitCode {
    if let Err(message) = setup_proxy_env() {
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
    // 引擎自身滚动文件日志（send 可观测批 2026-10-16，compat §1）：先于
    // 任何服务活动安装；失败静默降级（无 stderr 输出，IPC 服务不受影响）。
    logfile::init(&args.data_dir);
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
    use super::{
        outbound_flush_barrier, parse_scutil_proxy, resolve_proxy_chain, run_outbound_writer,
        standard_proxy_env_hit, OutboundFrame, ProxyDecision,
    };
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
        // host 上限 253 字节恰可过。
        assert!(socks_proxy_url(&format!("{}:7890", "a".repeat(253))).is_ok());
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
        // host 超 253 字节与 IPv6 裸字面量（含 ':'）同拒。
        assert!(socks_proxy_url(&format!("{}:7890", "a".repeat(254))).is_err());
        assert!(socks_proxy_url("::1:7890").is_err());
    }

    #[test]
    fn standard_proxy_env_accepts_whitelisted_schemes() {
        assert_eq!(
            standard_proxy_env_hit("socks5://h:1080"),
            Some("socks5://h:1080")
        );
        assert_eq!(
            standard_proxy_env_hit("socks5h://h:1080"),
            Some("socks5h://h:1080")
        );
        assert_eq!(
            standard_proxy_env_hit("http://h:8080"),
            Some("http://h:8080")
        );
        // scheme 大小写不敏感；trim 后原值返回（原样生效不改写）。
        assert_eq!(
            standard_proxy_env_hit("  SOCKS5H://relay.local:1080  "),
            Some("SOCKS5H://relay.local:1080")
        );
    }

    #[test]
    fn standard_proxy_env_rejects_out_of_whitelist_and_malformed() {
        // 白名单外 scheme：引擎链不采纳（reqwest 原生层行为不受链约束）。
        assert_eq!(standard_proxy_env_hit("https://h:443"), None);
        assert_eq!(standard_proxy_env_hit("socks4://h:1080"), None);
        assert_eq!(standard_proxy_env_hit("https://user:pass@h:443"), None);
        // 无 scheme / 空余量 / 空值 / 裸空白。
        assert_eq!(standard_proxy_env_hit("h:1080"), None);
        assert_eq!(standard_proxy_env_hit("socks5://"), None);
        assert_eq!(standard_proxy_env_hit(""), None);
        assert_eq!(standard_proxy_env_hit("   "), None);
        assert_eq!(
            standard_proxy_env_hit("socks5://host with space:1080"),
            None
        );
        // 内部空白（URI 不合法面）：不命中。
        assert_eq!(standard_proxy_env_hit("http://h :8080"), None);
    }

    /// scutil --proxy 真实输出形状（macOS Sequoia 系统代理开 + SOCKS 关）。
    const SCUTIL_HTTP_ON: &str = "<dictionary> {\n  HTTPEnable : 1\n  HTTPPort : 7890\n  HTTPProxy : 127.0.0.1\n  HTTPSEnable : 1\n  HTTPSPort : 7890\n  HTTPSProxy : 127.0.0.1\n  SOCKSEnable : 0\n  SOCKSPort : 1080\n  SOCKSProxy : 127.0.0.1\n}\n";

    #[test]
    fn scutil_parse_extracts_http_proxy() {
        let proxy = parse_scutil_proxy(SCUTIL_HTTP_ON).expect("http proxy parsed");
        assert_eq!(proxy.scheme, "http");
        assert_eq!(proxy.host, "127.0.0.1");
        assert_eq!(proxy.port, 7890);
    }

    #[test]
    fn scutil_parse_extracts_socks_proxy() {
        let output = "<dictionary> {\n  SOCKSEnable : 1\n  SOCKSProxy : socks.local\n  SOCKSPort : 1080\n}\n";
        let proxy = parse_scutil_proxy(output).expect("socks proxy parsed");
        assert_eq!(proxy.scheme, "socks5h");
        assert_eq!(proxy.host, "socks.local");
        assert_eq!(proxy.port, 1080);
    }

    #[test]
    fn scutil_parse_prefers_http_when_both_enabled() {
        let output = "HTTPEnable : 1\nHTTPProxy : h.local\nHTTPPort : 8080\nSOCKSEnable : 1\nSOCKSProxy : s.local\nSOCKSPort : 1080\n";
        let proxy = parse_scutil_proxy(output).expect("http leg wins");
        assert_eq!(proxy.scheme, "http");
        assert_eq!(proxy.host, "h.local");
    }

    #[test]
    fn scutil_parse_socks_fallback_when_http_leg_broken() {
        // HTTP 使能但端口缺失：该路作废，落 SOCKS 路。
        let output = "HTTPEnable : 1\nHTTPProxy : h.local\nSOCKSEnable : 1\nSOCKSProxy : s.local\nSOCKSPort : 1080\n";
        let proxy = parse_scutil_proxy(output).expect("socks fallback");
        assert_eq!(proxy.scheme, "socks5h");
        assert_eq!(proxy.host, "s.local");
    }

    #[test]
    fn scutil_parse_rejects_missing_and_malformed_fields() {
        // 使能位缺失 / 非 1。
        assert!(parse_scutil_proxy("HTTPProxy : h.local\nHTTPPort : 8080\n").is_none());
        assert!(
            parse_scutil_proxy("HTTPEnable : 0\nHTTPProxy : h.local\nHTTPPort : 8080\n").is_none()
        );
        // host 缺失 / 空值 / 含空白 / IPv6 裸字面量。
        assert!(parse_scutil_proxy("HTTPEnable : 1\nHTTPPort : 8080\n").is_none());
        assert!(parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : \nHTTPPort : 8080\n").is_none());
        assert!(
            parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h local\nHTTPPort : 8080\n").is_none()
        );
        assert!(parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : ::1\nHTTPPort : 8080\n").is_none());
        // 端口缺失 / 非数值 / 0 / 超出 u16。
        assert!(parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h.local\n").is_none());
        assert!(
            parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h.local\nHTTPPort : notaport\n")
                .is_none()
        );
        assert!(
            parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h.local\nHTTPPort : 0\n").is_none()
        );
        assert!(
            parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h.local\nHTTPPort : 70000\n").is_none()
        );
        // 两路全废（SOCKS 端口畸形收尾）与空输出。
        assert!(parse_scutil_proxy("HTTPEnable : 1\nHTTPProxy : h local\nSOCKSEnable : 1\nSOCKSProxy : s.local\nSOCKSPort : x\n").is_none());
        assert!(parse_scutil_proxy("").is_none());
        // PAC 面（ProxyAutoConfig*）不构成命中。
        assert!(parse_scutil_proxy(
            "ProxyAutoConfigEnable : 1\nProxyAutoConfigURLString : http://pac.local/proxy.pac\n"
        )
        .is_none());
    }

    #[test]
    fn chain_explicit_env_wins_over_everything() {
        let decision = resolve_proxy_chain(
            Some("10.0.0.1:1080"),
            Some("http://ignored:8080"),
            None,
            Some(SCUTIL_HTTP_ON),
        )
        .expect("explicit env valid");
        assert!(
            matches!(decision, ProxyDecision::ExplicitEnv(ref url) if url == "socks5h://10.0.0.1:1080")
        );
    }

    #[test]
    fn chain_explicit_env_invalid_is_fatal() {
        // 显式配置非法 fail-closed（exit 2），不静默降级。
        assert!(resolve_proxy_chain(Some("host:notaport"), None, None, None).is_err());
        assert!(resolve_proxy_chain(Some("socks5://host:1080"), None, None, None).is_err());
    }

    #[test]
    fn chain_explicit_env_empty_falls_through_to_std_env() {
        let decision = resolve_proxy_chain(Some("   "), Some("http://fall.local:8080"), None, None)
            .expect("std env hit");
        assert!(matches!(decision, ProxyDecision::StdEnv(url) if url == "http://fall.local:8080"));
    }

    #[test]
    fn chain_std_env_uppercase_wins_over_lowercase() {
        let decision = resolve_proxy_chain(
            None,
            Some("socks5h://upper.local:1080"),
            Some("http://lower.local:8080"),
            None,
        )
        .expect("uppercase wins");
        assert!(matches!(decision, ProxyDecision::StdEnv(ref url) if url.contains("upper.local")));

        // 大写缺位 / 大写非白名单值时小写兜底。
        let decision = resolve_proxy_chain(
            None,
            Some("https://upper.local:443"),
            Some("socks5://lower.local:1080"),
            None,
        )
        .expect("lowercase fallback");
        assert!(matches!(decision, ProxyDecision::StdEnv(ref url) if url.contains("lower.local")));
    }

    #[test]
    fn chain_std_env_beats_scutil() {
        let decision = resolve_proxy_chain(
            None,
            None,
            Some("socks5://std.local:1080"),
            Some(SCUTIL_HTTP_ON),
        )
        .expect("std env wins over system");
        assert!(matches!(decision, ProxyDecision::StdEnv(ref url) if url.contains("std.local")));
    }

    #[test]
    fn chain_scutil_http_then_socks_and_degrades_to_direct() {
        let decision =
            resolve_proxy_chain(None, None, None, Some(SCUTIL_HTTP_ON)).expect("system http");
        assert!(
            matches!(decision, ProxyDecision::SystemHttp(ref url) if url == "http://127.0.0.1:7890")
        );

        // HTTP 路废、SOCKS 路有效 → system-socks。
        let scutil_socks = "HTTPEnable : 1\nHTTPProxy : h local\nSOCKSEnable : 1\nSOCKSProxy : s.local\nSOCKSPort : 1080\n";
        let decision =
            resolve_proxy_chain(None, None, None, Some(scutil_socks)).expect("system socks");
        assert!(
            matches!(decision, ProxyDecision::SystemSocks(ref url) if url == "socks5h://s.local:1080")
        );

        // scutil 输出全废 → 直连；scutil 缺席（非 macOS / 读取失败）→ 直连。
        assert!(matches!(
            resolve_proxy_chain(None, None, None, Some("HTTPEnable : 1\n")).expect("degrade"),
            ProxyDecision::Direct
        ));
        assert!(matches!(
            resolve_proxy_chain(None, None, None, None).expect("direct"),
            ProxyDecision::Direct
        ));
    }

    #[test]
    fn chain_decision_labels_match_compat_doc_tokens() {
        // compat §1.1 日志标记词是契约面：direct/explicit-env/std-env/
        // system-http/system-socks。
        assert_eq!(ProxyDecision::Direct.label(), "direct");
        assert_eq!(
            ProxyDecision::ExplicitEnv("socks5h://h:1".into()).label(),
            "explicit-env"
        );
        assert_eq!(
            ProxyDecision::StdEnv("http://h:1".into()).label(),
            "std-env"
        );
        assert_eq!(
            ProxyDecision::SystemHttp("http://h:1".into()).label(),
            "system-http"
        );
        assert_eq!(
            ProxyDecision::SystemSocks("socks5h://h:1".into()).label(),
            "system-socks"
        );
        assert_eq!(ProxyDecision::Direct.detail(), "no proxy configured");
        assert_eq!(
            ProxyDecision::StdEnv("http://h:1".into()).detail(),
            "http://h:1"
        );
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
