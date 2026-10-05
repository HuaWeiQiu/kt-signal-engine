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
use crate::service::{
    storage_sync, LinkError, SendMention, SendPreview, SendQuote, SendSticker, Service,
    UserStatusError,
};
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
            // 引用/预览/mentions 扩面：键名与 connector 上游同名直投。
            // quote 半参（只有 timestamp 或只有 author）fail-closed；mentions
            // 条目缺字段/超 u32 fail-closed。
            let quote = match (params.get("quoteTimestamp"), params.get("quoteAuthor")) {
                (None, None) => None,
                (Some(ts), Some(author)) => match (ts.as_u64(), author.as_str()) {
                    (Some(ts), Some(author)) => Some(SendQuote {
                        timestamp: ts,
                        author: author.to_owned(),
                    }),
                    _ => return internal_error(id),
                },
                _ => return internal_error(id),
            };
            let preview = params
                .get("previewUrl")
                .and_then(Value::as_str)
                .map(|url| SendPreview {
                    url: url.to_owned(),
                    title: params
                        .get("previewTitle")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    description: params
                        .get("previewDescription")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    image: params
                        .get("previewImage")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            // 附件条目：connector 上游唯一形状（data URI 字符串数组）；
            // 非字符串条目 fail-closed。
            let attachments = match params.get("attachments") {
                None => Vec::new(),
                Some(Value::Array(entries)) => {
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry.as_str() {
                            Some(uri) => parsed.push(uri.to_owned()),
                            None => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            let mentions = match params.get("mentions") {
                None => Vec::new(),
                Some(Value::Array(entries)) => {
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        let number = entry.get("number").and_then(Value::as_str);
                        let start = entry
                            .get("start")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        let length = entry
                            .get("length")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        match (number, start, length) {
                            (Some(number), Some(start), Some(length)) => parsed.push(SendMention {
                                number: number.to_owned(),
                                start,
                                length,
                            }),
                            _ => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            // 贴纸扩面：对象形状，packId(hex)/packKey(base64)/stickerId/image
            // 必填，emoji/width/height 可选；缺字段/超 u32 fail-closed。
            let sticker = match params.get("sticker") {
                None => None,
                Some(value) if value.is_object() => {
                    let pack_id = value.get("packId").and_then(Value::as_str);
                    let pack_key = value.get("packKey").and_then(Value::as_str);
                    let sticker_id = value
                        .get("stickerId")
                        .and_then(Value::as_u64)
                        .and_then(|item| u32::try_from(item).ok());
                    let image = value.get("image").and_then(Value::as_str);
                    let width = value
                        .get("width")
                        .and_then(Value::as_u64)
                        .and_then(|item| u32::try_from(item).ok());
                    let height = value
                        .get("height")
                        .and_then(Value::as_u64)
                        .and_then(|item| u32::try_from(item).ok());
                    match (pack_id, pack_key, sticker_id, image) {
                        (Some(pack_id), Some(pack_key), Some(sticker_id), Some(image)) => {
                            Some(SendSticker {
                                pack_id: pack_id.to_owned(),
                                pack_key: pack_key.to_owned(),
                                sticker_id,
                                emoji: value
                                    .get("emoji")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                image: image.to_owned(),
                                width,
                                height,
                            })
                        }
                        _ => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            // 官方 sendStickerMessage 边界：贴纸与文本/附件/预览/mentions
            // 互斥（body=undefined、attachments=[]、无 preview/bodyRanges）；
            // quote 可共存（官方 quote 选项）。
            if sticker.is_some()
                && (!message.unwrap_or_default().is_empty()
                    || !attachments.is_empty()
                    || preview.is_some()
                    || !mentions.is_empty())
            {
                return internal_error(id);
            }
            // 阅后即焚扩面（E1，官方 isViewOnce 同名直投）：布尔参数缺省
            // false，非布尔 fail-closed。发送面 fail-closed 对齐官方
            // isValidTapToView 同构（+ mentions 禁发，任务口径）：body 空 ∧
            // 恰 1 附件 ∧ 无 quote/preview/mentions/sticker；附件 mediatype
            // 必须 image|video（data URI 前缀初检 + 服务层解析后 spec 复核，
            // fake 模式同口径 -32603）。
            let view_once = match params.get("viewOnce") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(flag)) => *flag,
                Some(_) => return internal_error(id),
            };
            if view_once
                && (!message.unwrap_or_default().is_empty()
                    || attachments.len() != 1
                    || quote.is_some()
                    || preview.is_some()
                    || !mentions.is_empty()
                    || sticker.is_some()
                    || attachments
                        .first()
                        .is_none_or(|uri| !has_image_or_video_mediatype(uri)))
            {
                return internal_error(id);
            }
            match (account, message, group, recipients_of(params)) {
                (Some(account), message, Some(group), _)
                    if sticker.is_some() || message.is_some() || view_once =>
                {
                    match service
                        .send_text_group(
                            account,
                            group,
                            message.unwrap_or_default(),
                            quote.as_ref(),
                            preview.as_ref(),
                            &mentions,
                            &attachments,
                            sticker.as_ref(),
                            view_once,
                        )
                        .await
                    {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(_) => internal_error(id),
                    }
                }
                (Some(account), message, None, Ok(recipients))
                    if sticker.is_some() || message.is_some() || view_once =>
                {
                    match service
                        .send_text(
                            account,
                            &recipients,
                            message.unwrap_or_default(),
                            quote.as_ref(),
                            preview.as_ref(),
                            &mentions,
                            &attachments,
                            sticker.as_ref(),
                            view_once,
                        )
                        .await
                    {
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
        // sendViewOnceOpen（E4）：官方 syncViewOnceOpen 同构——向自己 ACI 发
        // SyncMessage.ViewOnceOpen{senderAci, timestamp}（多设备同步「该阅后
        // 即焚已被本机打开」，presage self-send 通路验证可行，compat §5）；
        // 与 sendViewedReceipt（作者面 VIEWED 回执）双通道独立。senderAci
        // 号码/UUID 双形态（reaction targetAuthor 同解析口径）。
        "sendViewOnceOpen" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let sender = params.get("senderAci").and_then(Value::as_str);
            let timestamp = params.get("timestamp").and_then(Value::as_u64);
            match (account, sender, timestamp) {
                (Some(account), Some(sender), Some(timestamp)) => {
                    match service
                        .send_view_once_open(account, sender, timestamp)
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
        // ---- KT 扩展方法（独立命名空间，非 signal-cli 兼容面）----
        // setExpirationTimer（消失消息 E 批）：1:1 会话计时器设置 +
        // expiration-update 广播（signal-cli Manager.setExpirationTimer
        // 语义对位；updateContact 的 expiration 面独立成方法，本引擎
        // updateContact 维持上游 params 形状不扩）。参数 fail-closed：
        // recipient/expirationInSeconds 缺失或形状错（负数/小数/超 u32），
        // 未注册联系人与发送失败同码 -32603 折叠。
        "setExpirationTimer" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let recipient = params.get("recipient").and_then(Value::as_str);
            let timer = params
                .get("expirationInSeconds")
                .and_then(Value::as_u64)
                .and_then(|seconds| u32::try_from(seconds).ok());
            match (account, recipient, timer) {
                (Some(account), Some(recipient), Some(timer)) => {
                    match service
                        .set_expiration_timer(account, recipient, timer)
                        .await
                    {
                        Ok(timestamp) => {
                            jsonrpc::success_response(id, json!({ "timestamp": timestamp }))
                        }
                        Err(_) => internal_error(id),
                    }
                }
                _ => internal_error(id),
            }
        }
        // 贴纸包浏览：匿名 CDN 只读，不依赖账号（无 account 参数）。
        // 参数校验 fail-closed：packId 非 32 hex / packKey 非 32 字节按
        // 结构化错误 STICKER_PACK_KEY_INVALID 同码位的参数面回 -1。
        "getStickerPackManifest" => {
            let pack_id = params.get("packId").and_then(Value::as_str);
            let pack_key = params.get("packKey").and_then(Value::as_str);
            let (Some(pack_id), Some(pack_key)) = (pack_id, pack_key) else {
                return sticker_error(id, crate::service::stickers::StickerError::InvalidParams);
            };
            match service.sticker_pack_manifest(pack_id, pack_key).await {
                Ok(projection) => jsonrpc::success_response(
                    id,
                    json!({
                        "title": projection.title,
                        "author": projection.author,
                        "cover": projection.cover.map(|cover| json!({
                            "id": cover.id,
                            "emoji": cover.emoji,
                            "contentType": cover.content_type,
                        })),
                        "stickers": projection.stickers.iter().map(|sticker| json!({
                            "id": sticker.id,
                            "emoji": sticker.emoji,
                            "contentType": sticker.content_type,
                        })).collect::<Vec<_>>(),
                        "stickerCount": projection.sticker_count,
                    }),
                ),
                Err(error) => sticker_error(id, error),
            }
        }
        "getStickerImage" => {
            let pack_id = params.get("packId").and_then(Value::as_str);
            let pack_key = params.get("packKey").and_then(Value::as_str);
            let sticker_id = params
                .get("stickerId")
                .and_then(Value::as_u64)
                .and_then(|item| u32::try_from(item).ok());
            let (Some(pack_id), Some(pack_key), Some(sticker_id)) = (pack_id, pack_key, sticker_id)
            else {
                return sticker_error(id, crate::service::stickers::StickerError::InvalidParams);
            };
            match service.sticker_image(pack_id, pack_key, sticker_id).await {
                Ok(image) => jsonrpc::success_response(
                    id,
                    json!({
                        "dataBase64": image.data_base64,
                        "contentType": image.content_type,
                        "size": image.size,
                    }),
                ),
                Err(error) => sticker_error(id, error),
            }
        }
        // 会话级置顶：Storage Service 真相源（带 account 参数，风格对齐
        // send 系列的参数解析 fail-closed）。
        "getPinnedConversations" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.pinned_conversations(account).await {
                Ok(entries) => jsonrpc::success_response(id, pinned_payload(&entries)),
                // 账号参数不指向已链接账号：非 storage 面错误（compat §4.33
                // 四码只覆盖 storage 链路），按引擎通用内部错误 fail-closed。
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "setConversationPinned" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let conversation_id = params.get("conversationId").and_then(Value::as_str);
            let kind = params.get("kind").and_then(Value::as_str);
            let pinned = params.get("pinned").and_then(Value::as_bool);
            let Some(conversation) = conversation_of(conversation_id, kind) else {
                return internal_error(id);
            };
            let Some(pinned) = pinned else {
                return internal_error(id);
            };
            match service
                .set_conversation_pinned(account, conversation, pinned)
                .await
            {
                Ok(entries) => jsonrpc::success_response(id, pinned_payload(&entries)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 会话拉黑只读面（Storage Service 真相源，带 account 参数）：参数
        // 纪律与置顶面同款（fail-closed -32603，storage 四码只覆盖链路）。
        "getBlockedConversations" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.blocked_conversations(account).await {
                Ok(entries) => jsonrpc::success_response(id, blocked_payload(&entries)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 贴纸包安装状态同步（Storage Service 真相源，带 account 参数，风格
        // 对齐置顶面）：packId 解码/packKey 形状错走 -32603 fail-closed
        //（置顶面参数处理先例，不占用结构化码）。
        "getStickerPackSyncs" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.sticker_pack_syncs(account).await {
                Ok(packs) => jsonrpc::success_response(id, sticker_sync_payload(&packs)),
                // 账号参数不指向已链接账号：非 storage 面错误（结构化码只
                // 覆盖 storage 链路），按引擎通用内部错误 fail-closed。
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "setStickerPackSync" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(pack_id) = params
                .get("packId")
                .and_then(Value::as_str)
                .and_then(crate::service::stickers::decode_pack_id)
            else {
                return internal_error(id);
            };
            let Some(installed) = params.get("installed").and_then(Value::as_bool) else {
                return internal_error(id);
            };
            // 形状纪律（参数形状错一律 fail-closed，内容合法性按 installed
            // 分流）：packKey 只能是 string|null|缺省；position 只能是数值
            // |null|缺省且 ≤u32。
            let pack_key_text = match params.get("packKey") {
                None | Some(Value::Null) => None,
                Some(Value::String(text)) => Some(text.as_str()),
                Some(_) => return internal_error(id),
            };
            let position = match params.get("position") {
                None | Some(Value::Null) => None,
                Some(value) => match value.as_u64().and_then(|item| u32::try_from(item).ok()) {
                    Some(item) => Some(item),
                    None => return internal_error(id),
                },
            };
            // installed=true 必须带有效 packKey（32 字节 base64）；
            // installed=false 忽略 packKey 内容与 position（官方墓碑形态
            // 两字段皆不写）。
            let intent = if installed {
                let Some(pack_key) =
                    pack_key_text.and_then(crate::service::stickers::decode_pack_key)
                else {
                    return internal_error(id);
                };
                crate::service::storage_sticker::StickerPackWrite::Install {
                    pack_id,
                    pack_key,
                    position,
                }
            } else {
                crate::service::storage_sticker::StickerPackWrite::Uninstall { pack_id }
            };
            match service.set_sticker_pack_sync(account, intent).await {
                Ok(packs) => jsonrpc::success_response(id, sticker_sync_payload(&packs)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 账号级设置（官方 updateConfiguration 等价，Storage Service 真相源，
        // 带 account 参数，风格对齐置顶/贴纸面）：只写提供的字段；形状错
        // fail-closed -32603（置顶面参数处理先例，不占用结构化码）。
        "setAccountConfiguration" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let read_receipts = match optional_bool(params.get("readReceipts")) {
                Some(value) => value,
                None => return internal_error(id),
            };
            let typing_indicators = match optional_bool(params.get("typingIndicators")) {
                Some(value) => value,
                None => return internal_error(id),
            };
            let phone_number_sharing_mode = match params.get("phoneNumberSharingMode") {
                None | Some(Value::Null) => None,
                Some(Value::String(text)) => {
                    match storage_sync::phone_number_sharing_mode_of(text) {
                        Some(mode) => Some(mode),
                        None => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            let unlisted_phone_number = match optional_bool(params.get("unlistedPhoneNumber")) {
                Some(value) => value,
                None => return internal_error(id),
            };
            // 零可写字段 = 无意图调用，fail-closed（不静默 no-op）。
            if read_receipts.is_none()
                && typing_indicators.is_none()
                && phone_number_sharing_mode.is_none()
                && unlisted_phone_number.is_none()
            {
                return internal_error(id);
            }
            match service
                .set_account_configuration(
                    account,
                    storage_sync::AccountConfigurationUpdate {
                        read_receipts,
                        typing_indicators,
                        phone_number_sharing_mode,
                        unlisted_phone_number,
                    },
                )
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({})),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 个人资料编辑（官方 updateProfile 等价，带 account 参数）：文字面
        // 全量写（givenName 必填非空），avatar 三态（缺省=保留、data URI=
        // 换新、null=移除）；形状错 fail-closed -32603（置顶面参数处理先例）。
        "setProfile" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(given_name) = params.get("givenName").and_then(Value::as_str) else {
                return internal_error(id);
            };
            if given_name.is_empty() {
                return internal_error(id);
            }
            let optional_text = |key: &str| match params.get(key) {
                None | Some(Value::Null) => Some(None),
                Some(Value::String(text)) => Some(Some(text.to_owned())),
                Some(_) => None,
            };
            let Some(family_name) = optional_text("familyName") else {
                return internal_error(id);
            };
            let Some(about) = optional_text("about") else {
                return internal_error(id);
            };
            let Some(emoji) = optional_text("emoji") else {
                return internal_error(id);
            };
            let avatar = match params.get("avatar") {
                None => crate::service::profile::AvatarIntent::Retain,
                Some(Value::Null) => crate::service::profile::AvatarIntent::Remove,
                Some(Value::String(uri)) => {
                    match crate::service::profile::parse_avatar_data_uri(uri) {
                        Some((_, bytes)) => {
                            crate::service::profile::AvatarIntent::Replace { bytes }
                        }
                        None => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            match service
                .set_profile(
                    account,
                    crate::service::profile::ProfileUpdate {
                        given_name: given_name.to_owned(),
                        family_name,
                        about,
                        emoji,
                        avatar,
                    },
                )
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({})),
                Err(crate::service::profile::ProfileError::Storage(
                    storage_sync::StorageSyncError::UnknownAccount,
                )) => internal_error(id),
                Err(crate::service::profile::ProfileError::Storage(error)) => {
                    storage_error(id, &error)
                }
            }
        }
        // 账户级默认消失消息计时器（官方 universal timer 写面，Storage
        // Service 真相源，带 account 参数）：AccountRecord.universalExpireTimer
        // =17，0=关闭；参数形状错 fail-closed -32603（setExpirationTimer
        // timer 参数同款纪律）。
        "setUniversalExpireTimer" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(expiration_seconds) = params
                .get("expirationInSeconds")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
            else {
                return internal_error(id);
            };
            match service
                .set_universal_expire_timer(account, expiration_seconds)
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({})),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 头像下行（getAvatar，KT 扩展方法）：1:1 与群统一入口，读面不落
        // 调度日志（listContacts/getPinnedConversations 同纪律）。conversationId
        // 形状在服务层分派（ServiceId/号码/群 master key），形状非法与账号
        // 不可达同折叠 -32603（参数面 fail-closed，细节不外泄）；头像面错误
        // 折叠结构化码（avatar_error，compat §4.36 同款 -1 + message 码位）。
        "getAvatar" => {
            let account = params.get("account").and_then(Value::as_str);
            let conversation_id = params.get("conversationId").and_then(Value::as_str);
            match (account, conversation_id) {
                (Some(account), Some(conversation_id)) => {
                    match service.get_avatar(account, conversation_id).await {
                        Ok(payload) => jsonrpc::success_response(
                            id,
                            json!({ "dataUri": payload.data_uri, "size": payload.size }),
                        ),
                        Err(error) => avatar_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // 会话拉黑写面（setConversationBlocked，KT 扩展方法）：Storage
        // Service 真相源，参数纪律与置顶面同款（kind 缺失/未知、blocked 非
        // 布尔一律 fail-closed -32603，不占用结构化码）；返回写后全量
        // blocked 名单（getBlockedConversations 同形状）。
        "setConversationBlocked" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let conversation_id = params.get("conversationId").and_then(Value::as_str);
            let kind = params.get("kind").and_then(Value::as_str);
            let Some(blocked) = params.get("blocked").and_then(Value::as_bool) else {
                return internal_error(id);
            };
            let Some(conversation) = conversation_of(conversation_id, kind) else {
                return internal_error(id);
            };
            match service
                .set_conversation_blocked(account, conversation, blocked)
                .await
            {
                Ok(entries) => jsonrpc::success_response(id, blocked_payload(&entries)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 举报垃圾消息（reportSpam，KT 扩展方法）：协议上行面，落调度日志
        //（send 系/setExpirationTimer 同纪律）。serverGuids 每项必须可解析
        // UUID（fail-closed -32603，防路径注入）且单批 ≤1000（官方
        // reportSpamJobQueue zod max(1000)）；空批 = 无意图调用 fail-closed
        //（setAccountConfiguration 零可写字段先例）。token 只能是
        // string|null|缺省；部分失败诚实投影（failures 逐条带状态码）。
        "reportSpam" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let target_aci = params.get("targetAci").and_then(Value::as_str).and_then(
                presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string,
            );
            let token = match params.get("token") {
                None | Some(Value::Null) => None,
                Some(Value::String(text)) => Some(text.as_str()),
                Some(_) => return internal_error(id),
            };
            let server_guids = match params.get("serverGuids") {
                Some(Value::Array(entries)) => {
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry
                            .as_str()
                            .and_then(crate::service::report::normalize_server_guid)
                        {
                            Some(guid) => parsed.push(guid),
                            None => return internal_error(id),
                        }
                    }
                    parsed
                }
                _ => return internal_error(id),
            };
            if server_guids.is_empty()
                || server_guids.len() > crate::service::report::REPORT_SPAM_MAX_GUIDS
            {
                return internal_error(id);
            }
            match (account, target_aci) {
                (Some(account), Some(target_aci)) => {
                    match service
                        .report_spam(account, target_aci, server_guids, token.map(str::to_owned))
                        .await
                    {
                        Ok(outcome) => jsonrpc::success_response(
                            id,
                            json!({
                                "reported": outcome.reported,
                                "failures": outcome.failures.iter().map(|failure| {
                                    json!({
                                        "serverGuid": failure.server_guid,
                                        "status": failure.status,
                                    })
                                }).collect::<Vec<_>>(),
                            }),
                        ),
                        Err(crate::service::report::ReportSpamError::UnknownAccount) => {
                            internal_error(id)
                        }
                        Err(crate::service::report::ReportSpamError::AllFailed) => {
                            jsonrpc::error_response(id, &RpcError::new(-1, "REPORT_SPAM_FAILED"))
                        }
                    }
                }
                _ => internal_error(id),
            }
        }
        // 备份导出消费面（compat §3/§5「备份导出消费面」注册块 + 契约
        // §10.4）：参数面 fail-closed -32603（缺 account/未注册/fake 账号/
        // AEP 缺失）；结构化码 -1 + message 码位（契约 §10.7，connector 契约
        // 1.47 §4.46 白名单计划逐字）。两方法均不落调度日志、不产 stderr。
        "backup.export" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.backup_export(account).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "started" })),
                Err(crate::service::backup_task::BackupExportRejection::Structured(code)) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, code))
                }
                Err(crate::service::backup_task::BackupExportRejection::Internal) => {
                    internal_error(id)
                }
            }
        }
        "backup.exportStatus" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.backup_export_status(account).await {
                Ok(payload) => jsonrpc::success_response(id, payload),
                Err(()) => internal_error(id),
            }
        }
        _ => jsonrpc::error_response(
            id,
            &RpcError::new(jsonrpc::METHOD_NOT_FOUND, "Method not found"),
        ),
    }
}

/// 置顶返回体（getPinnedConversations / setConversationPinned 共用形状）：
/// 顺序 = 存储顺序；映射不出的条目跳过（不炸整面）。
fn pinned_payload(entries: &[storage_sync::PinnedEntry]) -> Value {
    json!({
        "pinned": entries
            .iter()
            .filter_map(|entry| {
                let id = entry.id.as_ref()?;
                Some(json!({
                    "conversationId": id.id_string(),
                    "kind": id.kind(),
                }))
            })
            .collect::<Vec<_>>(),
    })
}

/// 拉黑返回体（getBlockedConversations）：顺序 = 存储序；条目形状与置顶
/// 面同构（conversationId + kind）。
fn blocked_payload(entries: &[storage_sync::EngineConversationId]) -> Value {
    json!({
        "blocked": entries
            .iter()
            .map(|entry| {
                json!({
                    "conversationId": entry.id_string(),
                    "kind": entry.kind(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

/// 贴纸包同步返回体（getStickerPackSyncs / setStickerPackSync 共用形状）：
/// 顺序 = 存储序（manifest STICKER identifiers 首现序）；packId 16B → 32 位
/// 小写 hex、packKey 32B → 标准 base64（墓碑 → null）。
fn sticker_sync_payload(packs: &[crate::service::storage_sticker::StickerPackSync]) -> Value {
    use base64::Engine as _;
    json!({
        "packs": packs
            .iter()
            .map(|pack| {
                json!({
                    "packId": crate::service::hex_encode(&pack.pack_id),
                    "packKey": pack.pack_key.as_ref().map(|key| {
                        base64::engine::general_purpose::STANDARD.encode(key)
                    }),
                    "position": pack.position,
                    "deletedAtTimestampMs": pack.deleted_at_timestamp_ms,
                })
            })
            .collect::<Vec<_>>(),
    })
}

/// 可选布尔参数（setAccountConfiguration 面）：缺省/null = 不写该字段；
/// 非布尔形状 = None 外层（fail-closed）。
fn optional_bool(value: Option<&Value>) -> Option<Option<bool>> {
    match value {
        None | Some(Value::Null) => Some(None),
        Some(Value::Bool(v)) => Some(Some(*v)),
        Some(_) => None,
    }
}

/// conversationId + kind → 引擎会话标识（未知 kind fail-closed）。
fn conversation_of(
    conversation_id: Option<&str>,
    kind: Option<&str>,
) -> Option<storage_sync::EngineConversationId> {
    let conversation_id = conversation_id?;
    match kind? {
        "contact" => Some(storage_sync::EngineConversationId::Contact(
            conversation_id.to_owned(),
        )),
        "group" => Some(storage_sync::EngineConversationId::Group(
            conversation_id.to_owned(),
        )),
        "legacyGroup" => Some(storage_sync::EngineConversationId::LegacyGroup(
            conversation_id.to_owned(),
        )),
        _ => None,
    }
}

/// 贴纸包面错误 → JSON-RPC 错误码（compat §4.32，connector 白名单逐字）。
fn sticker_error(id: &Value, error: crate::service::stickers::StickerError) -> Value {
    let (code, message) = match error {
        // 参数缺失/形状错与 packKey 校验失败同码位：key 形状参数失败。
        crate::service::stickers::StickerError::InvalidParams => (-1, "STICKER_PACK_KEY_INVALID"),
        crate::service::stickers::StickerError::KeyInvalid => (-1, "STICKER_PACK_KEY_INVALID"),
        crate::service::stickers::StickerError::Fetch => (-1, "STICKER_PACK_FETCH_FAILED"),
        crate::service::stickers::StickerError::Malformed => (-1, "STICKER_PACK_MALFORMED"),
        crate::service::stickers::StickerError::ImageTooLarge => (-1, "STICKER_IMAGE_TOO_LARGE"),
    };
    jsonrpc::error_response(id, &RpcError::new(code, message))
}

/// 头像面错误 → JSON-RPC 错误码（-1 + message 码位，置顶/贴纸面白名单
/// 风格）：参数不指向可达目标（UnknownAccount/UnknownConversation）→
/// -32603（非头像面，细节不外泄）；无头像（未同步 profile_key/对端或群未
/// 设头像/服务器 403/404）→ AVATAR_UNAVAILABLE；网络/下载/解密失败 →
/// AVATAR_FETCH_FAILED；超 10MB 帽 → AVATAR_TOO_LARGE。
fn avatar_error(id: &Value, error: crate::service::avatar::AvatarError) -> Value {
    let (code, message) = match error {
        crate::service::avatar::AvatarError::UnknownAccount => (-32603, "Internal error"),
        crate::service::avatar::AvatarError::UnknownConversation => (-32603, "Internal error"),
        crate::service::avatar::AvatarError::Unavailable => (-1, "AVATAR_UNAVAILABLE"),
        crate::service::avatar::AvatarError::FetchFailed => (-1, "AVATAR_FETCH_FAILED"),
        crate::service::avatar::AvatarError::TooLarge => (-1, "AVATAR_TOO_LARGE"),
    };
    jsonrpc::error_response(id, &RpcError::new(code, message))
}

/// 置顶面错误 → JSON-RPC 错误码（compat §4.33，connector 白名单逐字；
/// UnknownAccount 已由调用点折叠为引擎内部错误）。
fn storage_error(id: &Value, error: &storage_sync::StorageSyncError) -> Value {
    let (code, message) = match error {
        storage_sync::StorageSyncError::ConversationNotResolved => {
            (-1, "CONVERSATION_NOT_RESOLVED")
        }
        storage_sync::StorageSyncError::Unavailable => (-1, "STORAGE_UNAVAILABLE"),
        storage_sync::StorageSyncError::ReadFailed => (-1, "STORAGE_READ_FAILED"),
        storage_sync::StorageSyncError::WriteFailed => (-1, "STORAGE_WRITE_FAILED"),
        storage_sync::StorageSyncError::UnknownAccount => (-32603, "Internal error"),
    };
    jsonrpc::error_response(id, &RpcError::new(code, message))
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

/// 阅后即焚附件初检（E1）：data URI 的 mediatype 段必须 image/|video/ 前缀
///（服务层对解析后的 spec 同规则复核；fake 模式服务早退，靠本检查保持
/// -32603 同口径可契约测试）。
fn has_image_or_video_mediatype(data_uri: &str) -> bool {
    data_uri
        .strip_prefix("data:")
        .and_then(|rest| rest.split(';').next())
        .is_some_and(|mediatype| mediatype.starts_with("image/") || mediatype.starts_with("video/"))
}

fn internal_error(id: &Value) -> Value {
    jsonrpc::error_response(id, &RpcError::new(-32603, "Internal error"))
}
