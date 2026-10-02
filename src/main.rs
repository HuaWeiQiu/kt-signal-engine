// SPDX-License-Identifier: AGPL-3.0-only

//! 进程装配：argv 解析（严格模式）+ stdio 主循环。
//!
//! 启动契约见 `docs/jsonrpc-compat.md` §1：connector 以
//! `--data-dir <dir> jsonRpc [--receive-mode on-start] [--ignore-attachments]
//! --ignore-stories --ignore-stickers` 拉起本进程；JVM 相关环境变量必须被
//! 忽略。stdin EOF 是唯一干净退出路径（exit 0）；argv 非法 exit 2；内部
//! 致命 exit 1。stderr 保持安静（connector watchdog 以 stderr ERROR 行
//! 判死活），P1a 零日志。

mod jsonrpc;
mod methods;
mod state;

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

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

fn main() -> ExitCode {
    let args = match parse_launch_args(std::env::args_os().skip(1)) {
        Ok(args) => args,
        Err(()) => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let mut state = match EngineState::load(&args.data_dir) {
        Ok(state) => state,
        Err(_) => return ExitCode::from(1),
    };

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    let mut line: Vec<u8> = Vec::new();
    loop {
        line.clear();
        match read_bounded_line(&stdin, &mut line) {
            ReadOutcome::Eof => break,
            ReadOutcome::Line => {}
            ReadOutcome::Overlong => {
                // 超限行：已吞到行尾，按解析失败回 -32700（id 不可知）。
                if write_response(
                    &mut stdout,
                    &jsonrpc::parse_error_response(serde_json::Value::Null),
                )
                .is_err()
                {
                    return ExitCode::from(1);
                }
                continue;
            }
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        let request = jsonrpc::parse_line(&line);
        let response = methods::respond(&request, &mut state);
        if write_response(&mut stdout, &response).is_err() {
            return ExitCode::from(1);
        }
    }
    ExitCode::SUCCESS
}

fn write_response<W: Write>(out: &mut W, response: &serde_json::Value) -> std::io::Result<()> {
    let mut text = serde_json::to_string(response).expect("response serializes");
    text.push('\n');
    out.write_all(text.as_bytes())?;
    out.flush()
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
                    // 丢弃到行尾（或 EOF），让主循环继续下一帧。
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
