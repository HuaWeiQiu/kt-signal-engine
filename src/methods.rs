// SPDX-License-Identifier: AGPL-3.0-only

//! 白名单方法 dispatch（`docs/jsonrpc-compat.md` §3 逐方法）。
//!
//! P2 边界：startLink/finishLink/send 族（含群组）、listContacts/listGroups
//! 走真实服务层（service::Service，Real/Fake 双实现）；getUserStatus 仍为
//! 契约内占位（真实鉴权检查需触网，compat 表逐行注明）。send 族两模式同
//! 落调度日志。

use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::jsonrpc::{self, ParseOutcome, Request, RpcError};
use crate::service::{LinkError, Service, UserStatusError};
use crate::state::EngineState;

/// 上游对非法 deviceLinkUri 的错误原文（夹具逐字对齐）。
const UNKNOWN_LINK_URI: &str = "Unknown device link uri.";

/// 解绑态错误原文（上游把 AuthorizationFailedException 包进
/// UnexpectedErrorException 的形状，connector 1.21 靠它识别解绑）。
const USER_UNBOUND_MESSAGE: &str =
    "Failed to send message: Authorization failed! (AuthorizationFailedException)";

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
        // listContacts/listGroups：store 投影（fake 模式回空表）；未注册
        // 账号 -32603（细节不外泄）。
        "listContacts" | "listGroups" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let rows = if method == "listContacts" {
                service.list_contacts(account).await
            } else {
                service.list_groups(account).await
            };
            match rows {
                Ok(rows) => jsonrpc::success_response(id, json!(rows)),
                Err(_) => internal_error(id),
            }
        }
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
            let message = params.get("message").and_then(Value::as_str);
            let group = params.get("groupId").and_then(Value::as_str);
            match (account, message, group, recipients_of(params)) {
                (Some(account), Some(message), Some(group), _) => {
                    match service.send_text_group(account, group, message).await {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(account), Some(message), None, Ok(recipients)) => {
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
            let group = params.get("groupId").and_then(Value::as_str);
            match (account, target, group, recipients_of(params)) {
                (Some(account), Some(target), Some(group), _) => {
                    match service
                        .send_remote_delete_group(account, group, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(account), Some(target), None, Ok(recipients)) => {
                    match service
                        .send_remote_delete(account, &recipients, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
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
                    _,
                ) if params.get("groupId").is_some() => {
                    match service
                        .send_reaction_group(
                            account,
                            params
                                .get("groupId")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            target_author,
                            target,
                            emoji,
                            remove,
                        )
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (
                    Some(account),
                    Some(target_author),
                    Some(target),
                    Some(emoji),
                    Some(remove),
                    Ok(recipients),
                ) => {
                    match service
                        .send_reaction(account, &recipients, target_author, target, emoji, remove)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendPinMessage" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target_author = params.get("targetAuthor").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            // 官方语义：字段缺席/null = pinDurationForever；数值超 u32 视为
            // 非法参数 fail-closed。
            let duration = match params.get("pinDurationSeconds") {
                None | Some(Value::Null) => Some(None),
                Some(value) => value
                    .as_u64()
                    .and_then(|seconds| u32::try_from(seconds).ok())
                    .map(Some),
            };
            let group = params.get("groupId").and_then(Value::as_str);
            match (
                account,
                target_author,
                target,
                duration,
                group,
                recipients_of(params),
            ) {
                (Some(account), Some(author), Some(target), Some(duration), Some(group), _) => {
                    match service
                        .send_pin_message_group(account, group, author, target, duration)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (
                    Some(account),
                    Some(author),
                    Some(target),
                    Some(duration),
                    None,
                    Ok(recipients),
                ) if !recipients.is_empty() => {
                    match service
                        .send_pin_message(account, &recipients, author, target, duration)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendUnpinMessage" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target_author = params.get("targetAuthor").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            let group = params.get("groupId").and_then(Value::as_str);
            match (account, target_author, target, group, recipients_of(params)) {
                (Some(account), Some(author), Some(target), Some(group), _) => {
                    match service
                        .send_unpin_message_group(account, group, author, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(account), Some(author), Some(target), None, Ok(recipients))
                    if !recipients.is_empty() =>
                {
                    match service
                        .send_unpin_message(account, &recipients, author, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendAdminDelete" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target_author = params.get("targetAuthor").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            let group = params.get("groupId").and_then(Value::as_str);
            // 直聊无 admin 概念（官方 adminDelete 仅群语义）：无 groupId
            // fail-closed。
            match (account, target_author, target, group) {
                (Some(account), Some(author), Some(target), Some(group)) => {
                    match service
                        .send_admin_delete_group(account, group, author, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
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
            let group = params.get("groupId").and_then(Value::as_str);
            match (account, stop, group, recipients_of(params)) {
                (Some(account), Some(stop), Some(group), _) => {
                    match service.send_typing_group(account, group, stop).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(account), Some(stop), None, Ok(recipients)) => {
                    match service.send_typing(account, &recipients, stop).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        // 回执发送三面：官方 receiptMessage type=delivery/read/viewed。
        // recipient 为单字符串（原消息作者；官方按作者分组直发，不整组
        // 广播）；timestamps 非空数字数组，元素非数字 fail-closed。
        "sendDeliveryReceipt" | "sendReadReceipt" | "sendViewedReceipt" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let recipient = params.get("recipient").and_then(Value::as_str);
            let timestamps = params
                .get("timestamps")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .map(Value::as_u64)
                        .collect::<Option<Vec<u64>>>()
                });
            let kind = match method.as_str() {
                "sendDeliveryReceipt" => presage::proto::receipt_message::Type::Delivery,
                "sendReadReceipt" => presage::proto::receipt_message::Type::Read,
                _ => presage::proto::receipt_message::Type::Viewed,
            };
            match (account, recipient, timestamps) {
                (Some(account), Some(recipient), Some(Some(timestamps))) => {
                    match service
                        .send_receipt(account, recipient, &timestamps, kind)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        // getUserStatus：real 模式 whoami 探测（解绑态契约原文见 §3 头）；
        // fake 模式恒已注册。
        "getUserStatus" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match service.get_user_status(account).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "isRegistered": true })),
                Err(UserStatusError::Unbound) => {
                    jsonrpc::error_response(id, &RpcError::new(-32603, USER_UNBOUND_MESSAGE))
                }
                Err(UserStatusError::Internal) => internal_error(id),
            }
        }
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
