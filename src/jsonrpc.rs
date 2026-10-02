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
    let Value::Object(map) = value else {
        return ParseOutcome::Malformed(Value::Null);
    };
    let id = map.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = map.get("method").and_then(Value::as_str).map(str::to_owned) else {
        return ParseOutcome::Malformed(id);
    };
    let params = map
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    ParseOutcome::Request(Request { id, method, params })
}

pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i64, message: &str) -> Self {
        Self {
            code,
            message: message.to_owned(),
        }
    }
}

pub fn success_response(id: &Value, result: Value) -> Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

pub fn error_response(id: &Value, error: &RpcError) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": error.code, "message": error.message },
    })
}

pub fn parse_error_response(id: Value) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": PARSE_ERROR, "message": "Parse error" },
    })
}
