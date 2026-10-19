// SPDX-License-Identifier: AGPL-3.0-only

//! JSON-RPC 2.0 分帧：请求解析（容错）、响应/错误渲染。
//!
//! 形状契约见 `docs/jsonrpc-compat.md` §2：换行分帧、无批量数组；响应
//! `result`/`error` 二选一；未知方法 `-32601`；坏行/超限行 `-32700` 且
//! 进程不死。id 缺失按 `null` 回显（上游 daemon 同款宽容）。

use serde_json::{Map, Value};

pub const METHOD_NOT_FOUND: i64 = -32601;
pub const PARSE_ERROR: i64 = -32700;

pub struct Request {
    pub id: Value,
    pub method: String,
    pub params: Map<String, Value>,
}

pub enum ParseOutcome {
    Request(Request),
    /// 非法帧：携带尽力恢复的 id（可知则带，不可知为 null）。
    Malformed(Value),
}

pub fn parse_line(buf: &[u8]) -> ParseOutcome {
    let Ok(value) = serde_json::from_slice::<Value>(buf) else {
        return ParseOutcome::Malformed(Value::Null);
    };
    let Value::Object(mut map) = value else {
        return ParseOutcome::Malformed(Value::Null);
    };
    // 消费式取键（2026-10-20 帧帽对齐批的内存配套）：params 直接移出，
    // 不再整表 clone——大附件帧的 base64 字符串在 160MiB 帧帽下可达
    // 100+MiB，旧实现的值副本会让峰值多出一整份。
    let id = map.remove("id").unwrap_or(Value::Null);
    let Some(method) = map.remove("method").and_then(|value| match value {
        Value::String(text) => Some(text),
        _ => None,
    }) else {
        return ParseOutcome::Malformed(id);
    };
    let params = match map.remove("params") {
        Some(Value::Object(params)) => params,
        _ => Map::new(),
    };
    ParseOutcome::Request(Request { id, method, params })
}

pub struct RpcError {
    pub code: i64,
    pub message: String,
    /// 可选 data 面（JSON-RPC error.data；方法按需透传结构化细节，如
    /// terminateGroup 的服务器状态码）。None = data 键不在。
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: &str) -> Self {
        Self {
            code,
            message: message.to_owned(),
            data: None,
        }
    }

    pub fn with_data(code: i64, message: &str, data: Value) -> Self {
        Self {
            code,
            message: message.to_owned(),
            data: Some(data),
        }
    }
}

pub fn success_response(id: &Value, result: Value) -> Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

pub fn error_response(id: &Value, error: &RpcError) -> Value {
    let mut body = serde_json::json!({
        "code": error.code, "message": error.message,
    });
    if let Some(data) = &error.data {
        body["data"] = data.clone();
    }
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": body,
    })
}

pub fn parse_error_response(id: Value) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": PARSE_ERROR, "message": "Parse error" },
    })
}
