// SPDX-License-Identifier: AGPL-3.0-only

//! 白名单方法 dispatch（`docs/jsonrpc-compat.md` §3 逐方法）。
//!
//! P1a 边界：服务面为形状对齐桩——IPC 形状/错误码与夹具逐条一致，真实
//! Signal 服务流量在 P1b（presage）接入。所有桩集中标注 `P1a-stub`，
//! P1b 替换时按此清单机械排查。

use serde_json::{json, Map, Value};

use crate::jsonrpc::{self, ParseOutcome, Request, RpcError};
use crate::state::EngineState;

/// 上游对非法 deviceLinkUri 的错误原文（夹具逐字对齐）。
const UNKNOWN_LINK_URI: &str = "Unknown device link uri.";

pub fn respond(outcome: &ParseOutcome, state: &mut EngineState) -> Value {
    match outcome {
        ParseOutcome::Malformed(id) => jsonrpc::parse_error_response(id.clone()),
        ParseOutcome::Request(request) => dispatch(request, state),
    }
}

fn dispatch(request: &Request, state: &mut EngineState) -> Value {
    let Request { id, method, params } = request;
    match method.as_str() {
        "startLink" => start_link(id, state),
        "finishLink" => finish_link(id, params, state),
        "listAccounts" => jsonrpc::success_response(
            id,
            Value::Array(
                state
                    .accounts()
                    .iter()
                    .map(|number| json!({ "number": number }))
                    .collect(),
            ),
        ),
        // P1a-stub：联系人/群组真实同步在 P2（contacts）/P3（groups）。
        "listContacts" | "listGroups" => jsonrpc::success_response(id, json!([])),
        "deleteLocalAccountData" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // 上游对不存在账号同样幂等回 {}（夹具口径）。
            let _ = state.delete_account(account);
            jsonrpc::success_response(id, json!({}))
        }
        "send" => {
            state.append_dispatch(method, params);
            // P1a-stub：timestamp 为本机毫秒；真实值是发送管线的消息时间戳。
            jsonrpc::success_response(id, json!({ "timestamp": now_ms(), "results": [] }))
        }
        "remoteDelete" | "sendReaction" | "updateContact" | "sendTyping" => {
            state.append_dispatch(method, params);
            jsonrpc::success_response(id, json!({}))
        }
        // P1a-stub：真实注册态查询在 P2；解绑态 -32603 形状见 compat §3。
        "getUserStatus" => jsonrpc::success_response(id, json!({ "isRegistered": true })),
        _ => jsonrpc::error_response(
            id,
            &RpcError::new(jsonrpc::METHOD_NOT_FOUND, "Method not found"),
        ),
    }
}

fn start_link(id: &Value, state: &mut EngineState) -> Value {
    // P1a-stub：URI 为形状合法的本地占位（uuid/pub_key 十六进制段），
    // 不代表真实 provisioning 会话。
    let uri = stub_link_uri();
    state.pending_link_uri = Some(uri.clone());
    jsonrpc::success_response(id, json!({ "deviceLinkUri": uri }))
}

fn finish_link(id: &Value, params: &Map<String, Value>, state: &mut EngineState) -> Value {
    let presented = params.get("deviceLinkUri").and_then(Value::as_str);
    let matches = presented.is_some() && state.pending_link_uri.as_deref() == presented;
    if !matches {
        return jsonrpc::error_response(id, &RpcError::new(-1, UNKNOWN_LINK_URI));
    }
    state.pending_link_uri = None;
    match state.link_new_account() {
        Ok(number) => jsonrpc::success_response(id, json!({ "number": number })),
        Err(_) => jsonrpc::error_response(id, &RpcError::new(-32603, "Internal error")),
    }
}

fn stub_link_uri() -> String {
    // 无 rand 依赖：时间纳秒 + pid 过 splitmix64 得两段十六进制。
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ ((std::process::id() as u64) << 32);
    let mut next = || {
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    format!("sgnl://link?uuid={:016x}&pub_key={:016x}", next(), next())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
