// SPDX-License-Identifier: AGPL-3.0-only

//! 白名单方法 dispatch（`docs/jsonrpc-compat.md` §3 逐方法）。
//!
//! P1b 边界：startLink/finishLink/send 族走真实服务层（service::Service，
//! Real/Fake 双实现）；listContacts/listGroups/getUserStatus 仍为契约内
//! 占位（P2 真实，compat 表逐行注明）。send 族两模式同落调度日志。

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::jsonrpc::{self, ParseOutcome, Request, RpcError};
use crate::service::{LinkError, Service};
use crate::state::EngineState;

/// 上游对非法 deviceLinkUri 的错误原文（夹具逐字对齐）。
const UNKNOWN_LINK_URI: &str = "Unknown device link uri.";

/// P1b 明确边界（ADR 0004）：群组发送是 P3 面，引擎显式拒绝而非静默错发。
const GROUP_SEND_UNSUPPORTED: &str = "Group sends are not supported by this engine revision.";

pub(crate) enum Inbound {
    Line(String),
    Overlong,
}

pub(crate) async fn respond(
    inbound: &Inbound,
    state: Arc<AsyncMutex<EngineState>>,
    service: Arc<Service>,
) -> Value {
    match inbound {
        Inbound::Overlong => jsonrpc::parse_error_response(Value::Null),
        Inbound::Line(line) => match jsonrpc::parse_line(line.as_bytes()) {
            ParseOutcome::Malformed(id) => jsonrpc::parse_error_response(id),
            ParseOutcome::Request(request) => dispatch(&request, state, service).await,
        },
    }
}

async fn dispatch(
    request: &Request,
    state: Arc<AsyncMutex<EngineState>>,
    service: Arc<Service>,
) -> Value {
    let Request { id, method, params } = request;
    match method.as_str() {
        "startLink" => match service.start_link().await {
            Ok(uri) => jsonrpc::success_response(id, json!({ "deviceLinkUri": uri })),
            Err(_) => internal_error(id),
        },
        "finishLink" => {
            let uri = params.get("deviceLinkUri").and_then(Value::as_str);
            let name = params.get("deviceName").and_then(Value::as_str);
            match (uri, name) {
                (Some(uri), Some(name)) => match service.finish_link(uri, name).await {
                    Ok(number) => jsonrpc::success_response(id, json!({ "number": number })),
                    Err(LinkError::UnknownUri) => {
                        jsonrpc::error_response(id, &RpcError::new(-1, UNKNOWN_LINK_URI))
                    }
                    Err(LinkError::Internal) => internal_error(id),
                },
                // 参数缺失按非法链接会话处理（上游 fail-closed 同口径）。
                _ => jsonrpc::error_response(id, &RpcError::new(-1, UNKNOWN_LINK_URI)),
            }
        }
        "listAccounts" => {
            let state = state.lock().await;
            jsonrpc::success_response(
                id,
                Value::Array(
                    state
                        .accounts()
                        .iter()
                        .map(|entry| json!({ "number": entry.number }))
                        .collect(),
                ),
            )
        }
        // P2 契约内占位（compat §3）：联系人/群组真实同步在 P2/P3。
        "listContacts" | "listGroups" => jsonrpc::success_response(id, json!([])),
        "deleteLocalAccountData" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // 上游对不存在账号同样幂等回 {}（夹具口径）。
            match service.delete_account(account).await {
                Ok(_) => jsonrpc::success_response(id, json!({})),
                Err(_) => internal_error(id),
            }
        }
        "send" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            if params.get("groupId").is_some() {
                return jsonrpc::error_response(id, &RpcError::new(-1, GROUP_SEND_UNSUPPORTED));
            }
            let message = params.get("message").and_then(Value::as_str);
            match (account, message, recipients_of(params)) {
                (Some(account), Some(message), Ok(recipients)) => {
                    match service.send_text(account, &recipients, message).await {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(_) => internal_error(id),
                    }
                }
                // 空 account/message 或 recipient 形状非法：fail-closed。
                _ => internal_error(id),
            }
        }
        "remoteDelete" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            match (account, target, recipients_of(params)) {
                (Some(account), Some(target), Ok(recipients))
                    if params.get("groupId").is_none() =>
                {
                    match service
                        .send_remote_delete(account, &recipients, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(_), Some(_), Ok(_)) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, GROUP_SEND_UNSUPPORTED))
                }
                _ => internal_error(id),
            }
        }
        "sendReaction" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target_author = params.get("targetAuthor").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            let emoji = params.get("emoji").and_then(Value::as_str);
            let remove = params.get("remove").and_then(Value::as_bool);
            match (
                account,
                target_author,
                target,
                emoji,
                remove,
                recipients_of(params),
            ) {
                (
                    Some(account),
                    Some(target_author),
                    Some(target),
                    Some(emoji),
                    Some(remove),
                    Ok(recipients),
                ) if params.get("groupId").is_none() => {
                    match service
                        .send_reaction(account, &recipients, target_author, target, emoji, remove)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(_), Some(_), Some(_), Some(_), Some(_), Ok(_)) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, GROUP_SEND_UNSUPPORTED))
                }
                _ => internal_error(id),
            }
        }
        "updateContact" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let recipient = params.get("recipient").and_then(Value::as_str);
            let name = params.get("name").and_then(Value::as_str);
            match (account, recipient, name) {
                (Some(account), Some(recipient), Some(name)) => {
                    match service.update_contact(account, recipient, name).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendTyping" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let stop = params.get("stop").and_then(Value::as_bool);
            match (account, stop, recipients_of(params)) {
                (Some(account), Some(stop), Ok(recipients)) if params.get("groupId").is_none() => {
                    match service.send_typing(account, &recipients, stop).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(_), Some(_), Ok(_)) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, GROUP_SEND_UNSUPPORTED))
                }
                _ => internal_error(id),
            }
        }
        // P2 契约内占位（compat §3）：解绑态 -32603 形状见 compat §3 头。
        "getUserStatus" => jsonrpc::success_response(id, json!({ "isRegistered": true })),
        _ => jsonrpc::error_response(
            id,
            &RpcError::new(jsonrpc::METHOD_NOT_FOUND, "Method not found"),
        ),
    }
}

/// recipient 参数归一：数组（send/sendTyping/remoteDelete/sendReaction）或
/// 单字符串（updateContact 的 recipient）；缺省为空表。
fn recipients_of(params: &serde_json::Map<String, Value>) -> Result<Vec<String>, ()> {
    match params.get("recipient") {
        Some(Value::Array(entries)) => entries
            .iter()
            .map(|entry| entry.as_str().map(str::to_owned).ok_or(()))
            .collect(),
        Some(Value::String(single)) => Ok(vec![single.clone()]),
        None => Ok(Vec::new()),
        Some(_) => Err(()),
    }
}

fn internal_error(id: &Value) -> Value {
    jsonrpc::error_response(id, &RpcError::new(-32603, "Internal error"))
}
