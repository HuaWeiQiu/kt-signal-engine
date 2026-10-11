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
    capabilities, parse_legacy_call_face, parse_send_contact, storage_sync, storage_write,
    DeleteForMeAuthor, DeleteForMeConversation, DeleteForMeEntry, DeviceCapabilitiesError,
    LinkError, MessageRequestTarget, SendMention, SendPreview, SendQuote, SendQuoteBodyRange,
    SendSticker, SendStoryContext, SendTextStyle, Service, UserStatusError, MAX_SEND_TEXT_STYLES,
};
use crate::state::EngineState;

/// 上游对非法 deviceLinkUri 的错误原文（夹具逐字对齐）。
const UNKNOWN_LINK_URI: &str = "Unknown device link uri.";

/// 解绑态错误原文（上游把 AuthorizationFailedException 包进
/// UnexpectedErrorException 的形状，connector 1.21 靠它识别解绑）。
const USER_UNBOUND_MESSAGE: &str =
    "Failed to send message: Authorization failed! (AuthorizationFailedException)";

/// 服务端限流错误原文（wire 字面量与 connector 侧配套；锚词
/// "(RateLimitExceeded)" 供分类器命中）。
const RATE_LIMITED_MESSAGE: &str = "Failed to send message: Rate limited. (RateLimitExceeded)";
/// 人机验证（proof required）错误原文（wire 字面量与 connector 侧配套；
/// 锚词 "(ProofRequiredException)" 供分类器命中）。
const PROOF_REQUIRED_MESSAGE: &str =
    "Failed to send message: Proof required. (ProofRequiredException)";
/// 服务端不可用错误原文（wire 字面量与 connector 侧配套；锚词
/// "(ServerUnavailableException)" 供分类器命中）。
const SERVER_UNAVAILABLE_MESSAGE: &str =
    "Failed to send message: Signal server unavailable. (ServerUnavailableException)";
/// 身份拒发闸错误原文（P9，wire 字面量与 connector 侧配套；锚词
/// "(UntrustedIdentityException)" 供分类器命中——官方桌面同因抛
/// UntrustedIdentityKeyException，对端身份钥变更未经确认拒发，
/// compat §3/§5）。
const IDENTITY_UNVERIFIED_MESSAGE: &str =
    "Failed to send message: Identity unverified. (UntrustedIdentityException)";

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
        // 账号 -32603（细节不外泄）。listGroups 可选 groupId 过滤（P15b
        // 审计项5 单群读投影，connector §4.53/§4.57/§4.62 fold 读派发的
        // `groupId: [id]` 数组形）：非空字符串数组、条目 32B 标准 base64
        // 群 master key（decode_group_id 闸），坏形状 fail-closed -32603
        //（fake 面同口径）；well-formed 但 store 不在 → 掉出结果（结果可
        // 空）。缺席/null = 全表（wire additive，旧行为不变）。
        "listContacts" | "listGroups" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let group_filter = if method == "listGroups" {
                match params.get("groupId") {
                    None | Some(Value::Null) => None,
                    Some(Value::Array(entries)) => {
                        if entries.is_empty() {
                            return internal_error(id);
                        }
                        let mut parsed = Vec::with_capacity(entries.len());
                        for entry in entries {
                            match entry.as_str().and_then(crate::service::decode_group_id) {
                                Some(key) => parsed.push(key),
                                None => return internal_error(id),
                            }
                        }
                        Some(parsed)
                    }
                    Some(_) => return internal_error(id),
                }
            } else {
                None
            };
            let rows = if method == "listContacts" {
                service.list_contacts(account).await
            } else {
                service.list_groups(account, group_filter.as_deref()).await
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
            // 条目缺字段/超 u32 fail-closed。quoteType/quoteBodyRanges（Q5
            // 扩面）：quoteType 词表 normal|giftBadge（缺省 normal——官方
            // isGiftBadge ? GIFT_BADGE : NORMAL 同语义）；quoteBodyRanges
            // 条目 {number?, style?, start, length} mention/style 恰一（官方
            // toBodyRange 无关联值条目不产），style 五值白名单、start/length
            // 超 u32、>64 条 fail-closed（mentions/textStyle 闸纪律）；两键
            // 须挂 quote 键对（quote 键对缺席时报错——官方 quote 对象缺席则
            // type/bodyRanges 无处挂载）。
            let quote_type = match params.get("quoteType") {
                None | Some(Value::Null) => None,
                Some(Value::String(word)) if word == "normal" || word == "giftBadge" => {
                    Some(word.clone())
                }
                Some(_) => return internal_error(id),
            };
            let quote_body_ranges = match params.get("quoteBodyRanges") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.len() > MAX_SEND_TEXT_STYLES {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        let mention = entry.get("number").and_then(Value::as_str);
                        let style = entry.get("style").and_then(Value::as_str);
                        let start = entry
                            .get("start")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        let length = entry
                            .get("length")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        let style = match style {
                            Some(
                                style @ ("BOLD" | "ITALIC" | "SPOILER" | "STRIKETHROUGH"
                                | "MONOSPACE"),
                            ) => Some(style.to_owned()),
                            Some(_) => return internal_error(id),
                            None => None,
                        };
                        match (mention.map(str::to_owned), style, start, length) {
                            (Some(number), None, Some(start), Some(length)) => {
                                parsed.push(SendQuoteBodyRange {
                                    mention: Some(number),
                                    style: None,
                                    start,
                                    length,
                                })
                            }
                            (None, Some(style), Some(start), Some(length)) => {
                                parsed.push(SendQuoteBodyRange {
                                    mention: None,
                                    style: Some(style),
                                    start,
                                    length,
                                })
                            }
                            _ => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            let quote = match (params.get("quoteTimestamp"), params.get("quoteAuthor")) {
                (None, None) => {
                    if quote_type.is_some() || !quote_body_ranges.is_empty() {
                        return internal_error(id);
                    }
                    None
                }
                (Some(ts), Some(author)) => match (ts.as_u64(), author.as_str()) {
                    (Some(ts), Some(author)) => Some(SendQuote {
                        timestamp: ts,
                        author: author.to_owned(),
                        gift_badge: quote_type.as_deref() == Some("giftBadge"),
                        body_ranges: quote_body_ranges,
                    }),
                    _ => return internal_error(id),
                },
                _ => return internal_error(id),
            };
            // previewDate（P14 Preview.date 两端批）：链接预览发布时刻毫秒
            // u64；非整数 fail-closed，0 无时刻语义归缺席（官方
            // cleanLinkPreviewDate falsy 同口径）。
            let preview_date = match params.get("previewDate") {
                None | Some(Value::Null) => None,
                Some(Value::Number(value)) => match value.as_u64() {
                    Some(date) if date > 0 => Some(date),
                    Some(_) => None,
                    None => return internal_error(id),
                },
                Some(_) => return internal_error(id),
            };
            // previewDate 须挂 preview 键对（quoteType 同先例：宿主对象缺席
            // 则无处挂载，fail-closed）。
            if !params.get("previewUrl").is_some() && preview_date.is_some() {
                return internal_error(id);
            }
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
                    date: preview_date,
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
            // textStyle 扩面（connector 契约 1.69 §4.62 同名直投）：条目
            // {style, start, length}——style 五值白名单（上游 TextStyle 枚举
            // 去 NONE）fail-closed，start/length 超 u32 fail-closed，>64 条
            // fail-closed（receive cap 镜像，不静默截断）。官方 parseTextStyles
            // 无 body 边界校验、官方桌面发送侧自行裁剪，引擎同款不设正文
            // 长度闸。无参数 = 上游字节与旧版逐字节一致（键缺席纪律）。
            let text_styles = match params.get("textStyle") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.len() > MAX_SEND_TEXT_STYLES {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        let style = entry.get("style").and_then(Value::as_str);
                        let start = entry
                            .get("start")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        let length = entry
                            .get("length")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        match (style, start, length) {
                            (
                                Some(
                                    style @ ("BOLD" | "ITALIC" | "SPOILER" | "STRIKETHROUGH"
                                    | "MONOSPACE"),
                                ),
                                Some(start),
                                Some(length),
                            ) => parsed.push(SendTextStyle {
                                style: style.to_owned(),
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
            // P14 组 4：story 回复发送面（storyReplyAuthor/storyReplyTimestamp
            // 双参同进退——同置或都缺合法，单边在场 fail-closed）。author 为
            // 被回复 story 作者 ACI/号码（服务层 resolve_author_aci 再解析，
            // 不可解析 fail-closed），timestamp 为被回复 story 发送时刻毫秒
            // u64（0 = 无所指 fail-closed；官方 storyContext.timestamp 同位）。
            let story_context = match (
                params.get("storyReplyAuthor"),
                params.get("storyReplyTimestamp"),
            ) {
                (None | Some(Value::Null), None | Some(Value::Null)) => None,
                (Some(Value::String(author)), Some(Value::Number(ts))) => {
                    match (author.is_empty(), ts.as_u64()) {
                        (false, Some(ts)) if ts > 0 => Some(SendStoryContext {
                            author: author.to_owned(),
                            sent_timestamp: ts,
                        }),
                        _ => return internal_error(id),
                    }
                }
                _ => return internal_error(id),
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
                            &text_styles,
                            &attachments,
                            sticker.as_ref(),
                            story_context.as_ref(),
                            view_once,
                        )
                        .await
                    {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(error) => send_error(id, error),
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
                            &text_styles,
                            &attachments,
                            sticker.as_ref(),
                            story_context.as_ref(),
                            view_once,
                        )
                        .await
                    {
                        Ok((timestamp, results, failures)) => {
                            let mut payload = json!({ "timestamp": timestamp, "results": results });
                            // 时序审计组二#9 additive：per-recipient 失败条目
                            //（空表键不在，旧宿主零变化）。
                            if !failures.is_empty() {
                                payload["failures"] = Value::Array(failures);
                            }
                            jsonrpc::success_response(id, payload)
                        }
                        Err(error) => send_error(id, error),
                    }
                }
                // 空 account/message 或 recipient 形状非法：fail-closed。
                _ => internal_error(id),
            }
        }
        // editMessage（2026-10-11 六问批，官方编辑发送面）：message 必填非空
        // （官方 canEditMessage body 非空闸），targetTimestamp 必填 u64；寻址
        // 两腿恰一（sendMessageRequestResponse 同构）：直聊腿（recipient，
        // recipients_of 归一非空）与群腿（groupId 32B 群 master key 标准
        // base64）同置/都缺/任一腿形状非法全拒。24h 窗口/次数闸是官方 UI 层
        // 闸，wire 面不设（§5 登记）。
        "editMessage" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let message = params
                .get("message")
                .and_then(Value::as_str)
                .filter(|body| !body.is_empty());
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            let recipient = match params.get("recipient") {
                None | Some(Value::Null) => {
                    if params.get("groupId").is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                _ => recipients_of(params)
                    .ok()
                    .filter(|list| !list.is_empty())
                    .map(Some),
            };
            let group = match params.get("groupId") {
                None | Some(Value::Null) => {
                    if recipient.is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) => {
                    Some(crate::service::decode_group_id(text).map(|_| text.as_str()))
                }
                Some(_) => None,
            };
            match (account, message, target, recipient, group) {
                (Some(account), Some(message), Some(target), None, Some(Some(group))) => {
                    match service
                        .send_edit_message_group(account, group, target, message)
                        .await
                    {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(error) => send_error(id, error),
                    }
                }
                (Some(account), Some(message), Some(target), Some(Some(recipients)), None) => {
                    match service
                        .send_edit_message(account, &recipients, target, message)
                        .await
                    {
                        Ok((timestamp, results)) => jsonrpc::success_response(
                            id,
                            json!({ "timestamp": timestamp, "results": results }),
                        ),
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // sendGroupCallUpdate（同批，官方群通话开始/结束信令面）：groupId 必填
        // （群通话信令无直聊形态——1:1 通话信令走 sendCallMessage）且参数层
        // 闸 32B 群 master key 标准 base64（decode_group_id，fake 面同样
        // fail-closed——sendCallMessage 群腿同源口径），eraId 必填非空（官方
        // eraId 字符串即 era 标识）。
        "sendGroupCallUpdate" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let group = params
                .get("groupId")
                .and_then(Value::as_str)
                .filter(|group| crate::service::decode_group_id(group).is_some());
            let era_id = params
                .get("eraId")
                .and_then(Value::as_str)
                .filter(|era| !era.is_empty());
            match (account, group, era_id) {
                (Some(account), Some(group), Some(era_id)) => {
                    match service.send_group_call_update(account, group, era_id).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
                    }
                }
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
                        Err(error) => send_error(id, error),
                    }
                }
                (Some(account), Some(target), None, Ok(recipients)) => {
                    match service
                        .send_remote_delete(account, &recipients, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // sendPollCreate/sendPollVote/sendPollTerminate（Polls 发送批，窗D
        // A7，官方 poll 发送三臂对位）：官方桌面仅群聊形态（create 1:1 走
        // isPollSend1to1Enabled feature flag 默认关；vote/terminate 只在群
        // 会话触发——poll 只存在于群），三方法无直聊形态，groupId 必填且
        // 参数层 decode_group_id 闸（sendGroupCallUpdate 同口径，fake 面
        // 同样 fail-closed）。内容闸按官方 PollCreateSchema（types/
        // Polls.dom.ts）：question 非空 ≤200 字符 + 字节 ≤2048（官方
        // MAX_MESSAGE_BODY_BYTE_LENGTH）；options 2..=10 条、每条非空
        // ≤100 字符 + 字节 ≤2048；optionIndexes 0..=9 且 ≤10 条（官方
        // PollVoteSchema 同域），空数组 = 清票动作合法；targetTimestamp
        // 正整数（官方 z.number().int().positive()）；voteCount 缺省 1。
        // 长度计数 chars 而非 graphemes（无分割依赖，合成序列计数更大、
        // 超限更保守——偏差记档 §5）。
        "sendPollCreate" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let group = params
                .get("groupId")
                .and_then(Value::as_str)
                .filter(|group| crate::service::decode_group_id(group).is_some());
            let question = params.get("question").and_then(poll_question_face);
            let options = params.get("options").and_then(poll_options_face);
            let allow_multiple = match params.get("allowMultiple") {
                None | Some(Value::Null) => Some(false),
                Some(value) => value.as_bool(),
            };
            match (account, group, question, options, allow_multiple) {
                (Some(account), Some(group), Some(question), Some(options), Some(allow)) => {
                    match service
                        .send_poll_create_group(account, group, &question, &options, allow)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendPollVote" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let group = params
                .get("groupId")
                .and_then(Value::as_str)
                .filter(|group| crate::service::decode_group_id(group).is_some());
            let target_author = params.get("targetAuthor").and_then(Value::as_str);
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            let option_indexes = params
                .get("optionIndexes")
                .and_then(poll_option_indexes_face);
            let vote_count = match params.get("voteCount") {
                None | Some(Value::Null) => Some(1u32),
                Some(value) => value.as_u64().and_then(|count| u32::try_from(count).ok()),
            };
            match (
                account,
                group,
                target_author,
                target,
                option_indexes,
                vote_count,
            ) {
                (
                    Some(account),
                    Some(group),
                    Some(author),
                    Some(target),
                    Some(indexes),
                    Some(count),
                ) if target > 0 => {
                    match service
                        .send_poll_vote_group(account, group, author, target, &indexes, count)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        "sendPollTerminate" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let group = params
                .get("groupId")
                .and_then(Value::as_str)
                .filter(|group| crate::service::decode_group_id(group).is_some());
            let target = params.get("targetTimestamp").and_then(Value::as_u64);
            match (account, group, target) {
                (Some(account), Some(group), Some(target)) if target > 0 => {
                    match service
                        .send_poll_terminate_group(account, group, target)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
                    }
                }
                (Some(account), Some(stop), None, Ok(recipients)) => {
                    match service.send_typing(account, &recipients, stop).await {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // markRead/markViewed（P14 组 6，自发多设备读态 sync 写面）：官方
        // syncReadMessages/syncView 同构——向自己 ACI 发 SyncMessage.read=5/
        // viewed=16（直接 repeated 字段），多设备同步「这些消息已读/已看」；
        // 与 sendReadReceipt/sendViewedReceipt（作者面 ReceiptMessage 回执）
        // 双通道独立（官方 markConversationRead.preload.ts:252 注同位）。
        // params `{account, reads?|viewed?: [{sender, timestamp}]}`：条目
        // 非空数组（sendReadReceipt timestamps 非空纪律同构）、条目
        // {sender, timestamp} 双键齐备且形状合法否则 fail-closed -32603、
        // sender 号码/UUID 双解析（viewOnceOpen 同口径，不可解析 -32603）、
        // 100 条/chunk 分信封（官方 syncHelpers CHUNK_SIZE=100 同构）；
        // 成功答空结果对象（写面惯例）；协议上行面落调度日志（send 系同纪律）。
        "markRead" | "markViewed" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let list_key = if method == "markRead" {
                "reads"
            } else {
                "viewed"
            };
            let entries = params.get(list_key).and_then(Value::as_array).map(|items| {
                items
                    .iter()
                    .map(|item| {
                        Some((
                            item.get("sender")?.as_str()?.to_owned(),
                            item.get("timestamp")?.as_u64()?,
                        ))
                    })
                    .collect::<Option<Vec<(String, u64)>>>()
            });
            match (account, entries) {
                (Some(account), Some(Some(entries))) if !entries.is_empty() => {
                    match service
                        .mark_read_state(account, method == "markViewed", entries)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // sendCallMessage（通话消息面，KT 扩展方法）：官方 Content.callMessage=3
        // 信封。协议上行面落调度日志（send 系同纪律）；参数面 fail-closed
        //（-32603）：recipient/group 两腿恰一闸、data 非 base64/空、urgency
        // 非法枚举、destinationDeviceId 超 u32、legacy/opacity 两面同置或都缺。
        // legacy 面（1.53 批）：`legacy: {kind: "busy", id}` / `{kind: "hangup",
        // id, type?, deviceId?}` 按官方 proto Busy{id}/Hangup{id,type,deviceId}
        // 裸帧族构造（无 opaque 字段）。
        // 群臂（P4b 群信令出网批，connector 契约 1.82 wire 面）：可选 `group`
        // = 32B 群 master key 标准 base64（listGroups/terminateGroup/updateGroup
        // 的 groupId 同源口径）与 `recipient` 互斥恰一；官方群信令恒 opaque
        //（桌面 #handleSendCallMessageToGroup calling.preload.ts:3338-3407 构
        // CallingMessage{opaque} sender-key 群发）——群腿仅 modern 面放行，
        // legacy 面/非空 destinationDeviceId 与 group 同置 fail-closed；
        // `groupId` 键维持禁入（历史 fail-closed 面，wire 参数名是 `group`）。
        "sendCallMessage" => {
            enum CallTarget<'a> {
                Recipient(&'a str),
                Group(&'a str),
            }
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let legacy = parse_legacy_call_face(params.get("legacy"));
            let data = params
                .get("data")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_call_data);
            let urgency = match params.get("urgency") {
                None | Some(Value::Null) => {
                    Some(presage::proto::call_message::opaque::Urgency::Droppable)
                }
                Some(Value::String(text)) => match text.as_str() {
                    "droppable" => Some(presage::proto::call_message::opaque::Urgency::Droppable),
                    "handleImmediately" => {
                        Some(presage::proto::call_message::opaque::Urgency::HandleImmediately)
                    }
                    _ => None,
                },
                Some(_) => None,
            };
            let destination = match params.get("destinationDeviceId") {
                None | Some(Value::Null) => Some(None),
                Some(value) => value
                    .as_u64()
                    .and_then(|item| u32::try_from(item).ok())
                    .map(Some),
            };
            if params.get("groupId").is_some() {
                return internal_error(id);
            }
            // 寻址两腿恰一：群腿（group，32B master key 闸）与单收件人腿
            //（recipient）同置/都缺/任一腿形状非法全拒。
            let recipient = match params.get("recipient") {
                None | Some(Value::Null) => {
                    if params.get("group").is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) if !text.is_empty() => Some(Some(text.as_str())),
                Some(_) => None,
            };
            let group = match params.get("group") {
                None | Some(Value::Null) => {
                    if recipient.is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) => {
                    Some(crate::service::decode_group_id(text).map(|_| text.as_str()))
                }
                Some(_) => None,
            };
            let target = match (recipient, group) {
                (Some(Some(recipient)), None) => Some(CallTarget::Recipient(recipient)),
                (None, Some(Some(group_id))) => Some(CallTarget::Group(group_id)),
                _ => None,
            };
            // 群臂面闸：官方群信令恒 opaque——legacy 裸帧族群向无官方先例；
            // sender-key 群发无设备定向位（destinationDeviceId 仅 1:1 臂）。
            if matches!(target, Some(CallTarget::Group(_)))
                && (legacy.is_some() || destination.is_some_and(|device| device.is_some()))
            {
                return internal_error(id);
            }
            // 两面解析：modern（data）与 legacy 互斥——同置或都缺都 fail-closed
            //（busy/hangup 官方裸帧无 opaque 字段，混面无官方语义）。
            let face = match (data, legacy) {
                (Some(data), None) => {
                    urgency.map(|urgency| crate::service::CallMessageFace::Opaque { data, urgency })
                }
                (None, Some(legacy)) => Some(legacy),
                _ => None,
            };
            match (account, target, face, destination) {
                (
                    Some(account),
                    Some(CallTarget::Recipient(recipient)),
                    Some(face),
                    Some(destination),
                ) => match service
                    .send_call_message(
                        account,
                        recipient,
                        crate::service::SendCallMessage {
                            face,
                            destination_device_id: destination,
                        },
                    )
                    .await
                {
                    Ok(timestamp) => {
                        jsonrpc::success_response(id, json!({ "timestamp": timestamp }))
                    }
                    Err(error) => send_error(id, error),
                },
                (Some(account), Some(CallTarget::Group(group_id)), Some(face), _) => {
                    match service
                        .send_call_message_group(
                            account,
                            group_id,
                            crate::service::SendCallMessage {
                                face,
                                destination_device_id: None,
                            },
                        )
                        .await
                    {
                        Ok(timestamp) => {
                            jsonrpc::success_response(id, json!({ "timestamp": timestamp }))
                        }
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // sendContact（名片发送臂，窗D B11）：官方 DataMessage.contact=9
        // repeated 单条目面（SendMessage.preload.ts:418-492 toProto contact
        // 段 + EmbeddedContact.std.ts numberTo*Type 词表面同位）。协议上行
        // 面落调度日志（send 系同纪律）；参数面 fail-closed（-32603）：
        // recipient/groupId 两腿恰一（sendMessageRequestResponse 同款）、
        // contact 缺席/非对象、name 在场非对象/子字段非字符串、
        // number/email/address 在场非数组/条目非对象/value 缺席·非字符串·
        // 空串/type 词表外（number/email 四值 HOME|MOBILE|WORK|CUSTOM、
        // address 三值 HOME|WORK|CUSTOM——官方 numberTo*Type 词表外静默归
        // HOME，引擎按 textStyle 先例拒绝，§5 偏差记档）/label·address 子
        // 字段非字符串/organization 非字符串（空串=官方 falsy 同位归缺
        // 席）/avatar 非字符串（data URI 形状归服务层上行闸，附件面同纪
        // 律）。
        "sendContact" => {
            enum ContactTarget {
                Recipient(String),
                Group(String),
            }
            // 调度日志先落（send 系同款：被拒调用同样记录派发）；
            // contact.avatar 键落盘前形状摘要化（头像 data URI 载荷零字
            // 节进日志——setProfile/updateGroup「调度零头像字节」同纪律，
            // 摘要以 URI 字符串长度记量级），contact 其余键逐字保留（名
            // 片文本面是调用方自有数据，非 bulk 载荷）。
            let mut logged_params = params.clone();
            if let Some(Value::Object(contact)) = logged_params.get_mut("contact") {
                if let Some(Value::String(uri)) = contact.get_mut("avatar") {
                    let digest = format!("[avatar data uri: {} bytes]", uri.len());
                    contact.insert("avatar".into(), json!(digest));
                }
            }
            state.lock().await.append_dispatch(method, &logged_params);
            let account = params.get("account").and_then(Value::as_str);
            let contact = parse_send_contact(params.get("contact"));
            let recipient = match params.get("recipient") {
                None | Some(Value::Null) => {
                    if params.get("groupId").is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) if !text.is_empty() => Some(Some(text.clone())),
                Some(_) => None,
            };
            let group = match params.get("groupId") {
                None | Some(Value::Null) => {
                    if recipient.is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) => {
                    Some(crate::service::decode_group_id(text).map(|_| text.clone()))
                }
                Some(_) => None,
            };
            let target = match (recipient, group) {
                (Some(Some(text)), None) => Some(ContactTarget::Recipient(text)),
                (None, Some(Some(group_id))) => Some(ContactTarget::Group(group_id)),
                _ => None,
            };
            match (account, target, contact) {
                (Some(account), Some(ContactTarget::Recipient(recipient)), Some(payload)) => {
                    match service.send_contact(account, &recipient, &payload).await {
                        Ok(timestamp) => {
                            jsonrpc::success_response(id, json!({ "timestamp": timestamp }))
                        }
                        Err(error) => send_error(id, error),
                    }
                }
                (Some(account), Some(ContactTarget::Group(group_id)), Some(payload)) => {
                    match service
                        .send_contact_group(account, &group_id, &payload)
                        .await
                    {
                        Ok(timestamp) => {
                            jsonrpc::success_response(id, json!({ "timestamp": timestamp }))
                        }
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // sendStory（Stories 发送批，KT 扩展方法）：text story 一面（v1，
        // 群 story / 文件 story 不做）。协议上行面落调度日志（send 系同纪
        // 律）；参数面 fail-closed（-32603）：account/message 非空、
        // recipients 非空字符串数组（story 面恒为名单展开集，无单字符串
        // 形态——成员解析归 connector）、distributionListId 非空字符串缺省
        // 官方 MY_STORY_ID、allowsReplies 布尔（缺省 false = 线上 proto 缺
        // 席位，官方值源为名单属性）、textStyle 单词六值词表（官方
        // TextAttachment.Style 词表，DEFAULT = proto 字段省略）、bodyRanges
        // 五值内联样式 {style,start,length} ≤64（send textStyle 同款
        // fail-closed 闸 + 服务层防御双保险）、previewImage 非空字符串
        //（缺失/null = 无预览，其他类型 fail-closed 不静默丢弃）、
        // previewUrl 非空 ≤2048 字符且 https:// 开头（官方 isValidLink
        // https-only 同构，LinkPreview.std.ts:54-65）、previewTitle 非空
        // ≤1024 UTF-8 字节——两键独立可选，任一在场即成 Preview（官方
        // preview 面 image 可空同构），违规 fail-closed 不静默丢键。
        "sendStory" => {
            state.lock().await.append_dispatch(method, params);
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty());
            let message = params
                .get("message")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty());
            let recipients = match params.get("recipients") {
                Some(Value::Array(entries)) if !entries.is_empty() => {
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry.as_str() {
                            Some(number) if !number.is_empty() => parsed.push(number.to_owned()),
                            _ => return internal_error(id),
                        }
                    }
                    parsed
                }
                _ => return internal_error(id),
            };
            let distribution_list_id = match params.get("distributionListId") {
                None | Some(Value::Null) => crate::service::stories::MY_STORY_ID,
                Some(Value::String(list_id)) if !list_id.is_empty() => list_id.as_str(),
                _ => return internal_error(id),
            };
            let allows_replies = match params.get("allowsReplies") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(flag)) => *flag,
                Some(_) => return internal_error(id),
            };
            let text_style = match params.get("textStyle") {
                None | Some(Value::Null) => None,
                Some(Value::String(word)) => {
                    match crate::service::stories::parse_story_text_style(word) {
                        Ok(style) => style,
                        Err(()) => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            let body_ranges = match params.get("bodyRanges") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.len() > MAX_SEND_TEXT_STYLES {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        let style = entry.get("style").and_then(Value::as_str);
                        let start = entry
                            .get("start")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        let length = entry
                            .get("length")
                            .and_then(Value::as_u64)
                            .and_then(|value| u32::try_from(value).ok());
                        match (style, start, length) {
                            (
                                Some(
                                    style @ ("BOLD" | "ITALIC" | "SPOILER" | "STRIKETHROUGH"
                                    | "MONOSPACE"),
                                ),
                                Some(start),
                                Some(length),
                            ) => parsed.push(SendTextStyle {
                                style: style.to_owned(),
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
            let preview_image = match params.get("previewImage") {
                None | Some(Value::Null) => None,
                Some(Value::String(uri)) if !uri.is_empty() => Some(uri.as_str()),
                Some(_) => return internal_error(id),
            };
            // previewUrl/previewTitle 扩面（2026-10-09 批）：url 官方
            // isValidLink https-only 同构 ≤2048 字符；title ≤1024 UTF-8
            // 字节（Rust str.len 即字节面）。违规整拒不静默丢键。
            let preview_url = match params.get("previewUrl") {
                None | Some(Value::Null) => None,
                Some(Value::String(url))
                    if !url.is_empty()
                        && url.chars().count() <= 2048
                        && url.starts_with("https://") =>
                {
                    Some(url.clone())
                }
                Some(_) => return internal_error(id),
            };
            let preview_title = match params.get("previewTitle") {
                None | Some(Value::Null) => None,
                Some(Value::String(title)) if !title.is_empty() && title.len() <= 1024 => {
                    Some(title.clone())
                }
                Some(_) => return internal_error(id),
            };
            // previewDate（P14 Preview.date 两端批）：语义与 send 的
            // previewDate 同面（毫秒 u64，非整数 fail-closed，0 归缺席）；
            // 须挂三 preview 扩展键之一（url/title/image——preview 对象
            // 三件任一在场即成，build_story_preview 组装闸同口径）。
            let preview_date = match params.get("previewDate") {
                None | Some(Value::Null) => None,
                Some(Value::Number(value)) => match value.as_u64() {
                    Some(date) if date > 0 => Some(date),
                    Some(_) => None,
                    None => return internal_error(id),
                },
                Some(_) => return internal_error(id),
            };
            if preview_date.is_some()
                && preview_url.is_none()
                && preview_title.is_none()
                && preview_image.is_none()
            {
                return internal_error(id);
            }
            match (account, message) {
                (Some(account), Some(message)) => {
                    match service
                        .send_story(
                            account,
                            &recipients,
                            distribution_list_id,
                            allows_replies,
                            message,
                            text_style,
                            &body_ranges,
                            preview_image,
                            preview_url,
                            preview_title,
                            preview_date,
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
                _ => internal_error(id),
            }
        }
        // sendMessageRequestResponse（消息请求写面，connector 契约 1.63
        // §4.56 债务批，KT 扩展方法）：上游钉定面 params `{account,
        // recipient?|groupId?, type}`（connector prepare_conversations_message_
        // request + supervisor type 盖章同形状）。参数闸 fail-closed
        //（-32603）：type 六值词表白名单（官方 MessageRequestResponse.Type
        // 驼峰词；UNKNOWN=0 缺席/越界/词表外同拒——官方发送侧无「发未知」
        // 语义）、两腿恰一（direct = recipient 单字符串，群 = groupId 32B
        // 标准 base64；同置/双缺/腿形状非法全拒）；协议上行面落调度日志
        //（send 系同纪律）；成功答空结果对象（上游同款）；未知结局不重试
        // 纪律由写调用结果即返回值承载（connector §4.56 对齐）。
        "sendMessageRequestResponse" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let request_type = params
                .get("type")
                .and_then(Value::as_str)
                .and_then(crate::service::message_request_type_of);
            let recipient = match params.get("recipient") {
                None | Some(Value::Null) => {
                    if params.get("groupId").is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) if !text.is_empty() => Some(Some(text.clone())),
                Some(_) => None,
            };
            let group = match params.get("groupId") {
                None | Some(Value::Null) => {
                    if recipient.is_some() {
                        None
                    } else {
                        Some(None)
                    }
                }
                Some(Value::String(text)) => {
                    Some(crate::service::decode_group_id(text).map(MessageRequestTarget::Group))
                }
                Some(_) => None,
            };
            let target = match (recipient, group) {
                // 两腿恰一：恰一腿解析成功才放行（另一腿必须缺席）。
                (Some(Some(text)), None) => Some(MessageRequestTarget::Thread(text)),
                (None, Some(Some(master_key))) => Some(master_key),
                _ => None,
            };
            match (account, target, request_type) {
                (Some(account), Some(target), Some(request_type)) => {
                    match service
                        .send_message_request_response(account, target, request_type)
                        .await
                    {
                        Ok(()) => jsonrpc::success_response(id, json!({})),
                        Err(error) => send_error(id, error),
                    }
                }
                _ => internal_error(id),
            }
        }
        // deleteForMeConversation（P15c 项3，官方 deleteConversation 发送面
        // 同位，compat §3 行 + §5 P15c 块）：会话级「为我删除」上行。kind
        // 词表 contact/group（legacyGroup 官方不支持——getConversation
        // Identifier 非 GV2 群 throw 同拒）；contact = service-id 串或 E164
        // （toConversationIdentifier aci/e164 双形态），group = 32B 标准
        // base64 master key（decode_group_id 闸）。localOnly 缺省 false =
        // 全量清空臂（mostRecentMessages 必填非空 ≤5 条目——官方 slice(0,5)
        // 上限镜像，超出 fail-closed 不静默截断破坏性意图；
        // mostRecentNonExpiringMessages 可选 ≤5）；true = 仅本地臂
        // （mostRecent* 必须缺席——官方 local-only 分支无消息位）。条目
        // {author, timestamp}：author = service-id 串或 E164 直通
        // （toAddressableMessage 形态，引擎不做号码↔service id 二次变换），
        // timestamp 正整数；任一条目形状错整拒 -32603 不发半批。协议上行
        // 面落调度日志（send 系同纪律）。
        "deleteForMeConversation" => {
            state.lock().await.append_dispatch(method, params);
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let local_only = match params.get("localOnly") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(value)) => *value,
                Some(_) => return internal_error(id),
            };
            let conversation = match (
                params.get("kind").and_then(Value::as_str),
                params.get("conversationId").and_then(Value::as_str),
            ) {
                (Some("contact"), Some(text)) if !text.is_empty() => {
                    if let Some(sid) =
                        presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(text)
                    {
                        DeleteForMeConversation::Contact(sid)
                    } else if crate::service::is_e164(text) {
                        DeleteForMeConversation::Phone(text.to_owned())
                    } else {
                        return internal_error(id);
                    }
                }
                (Some("group"), Some(text)) => match crate::service::decode_group_id(text) {
                    Some(master_key) => DeleteForMeConversation::Group(master_key),
                    None => return internal_error(id),
                },
                _ => return internal_error(id),
            };
            let parse_entries = |value: Option<&Value>| match value {
                None | Some(Value::Null) => Some(Vec::new()),
                Some(Value::Array(items)) => {
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        let author = item.get("author").and_then(Value::as_str)?;
                        let author =
                                if let Some(sid) = presage::libsignal_service::protocol::ServiceId::parse_from_service_id_string(author) {
                                    DeleteForMeAuthor::ServiceId(sid)
                                } else if crate::service::is_e164(author) {
                                    DeleteForMeAuthor::Phone(author.to_owned())
                                } else {
                                    return None;
                                };
                        let timestamp = item.get("timestamp").and_then(Value::as_u64)?;
                        if timestamp == 0 {
                            return None;
                        }
                        out.push(DeleteForMeEntry { author, timestamp });
                    }
                    Some(out)
                }
                Some(_) => None,
            };
            let (Some(most_recent), Some(most_recent_non_expiring)) = (
                parse_entries(params.get("mostRecentMessages")),
                parse_entries(params.get("mostRecentNonExpiringMessages")),
            ) else {
                return internal_error(id);
            };
            if local_only {
                if !most_recent.is_empty() || !most_recent_non_expiring.is_empty() {
                    return internal_error(id);
                }
            } else if most_recent.is_empty()
                || most_recent.len() > 5
                || most_recent_non_expiring.len() > 5
            {
                return internal_error(id);
            }
            match service
                .send_delete_for_me_conversation(
                    account,
                    conversation,
                    local_only,
                    most_recent,
                    most_recent_non_expiring,
                )
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "sent" })),
                Err(error) => send_error(id, error),
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
                        Err(error) => send_error(id, error),
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
        // storage.mergeContacts（窗D A4，官方 mergeContactRecord engine 腿）：
        // 单参 account（只读 Storage 面 + 本地 ingest，无参数形状分叉）；
        // fake 恒零汇总（读面纪律）。汇总字段名 camelCase wire 直投。
        "storage.mergeContacts" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.merge_storage_contacts(account).await {
                Ok(outcome) => jsonrpc::success_response(
                    id,
                    json!({
                        "recordsRead": outcome.records_read,
                        "skipped": outcome.skipped,
                        "identitiesNew": outcome.identities_new,
                        "identitiesReplaced": outcome.identities_replaced,
                        "profileKeysSet": outcome.profile_keys_set,
                        "contactsCreated": outcome.contacts_created,
                        "trustStatesSynced": outcome.trust_states_synced,
                    }),
                ),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // storage.importGroups（窗D A5，官方 mergeGroupV2Record engine 腿）：
        // 单参 account（manifest GROUPV2 → 群态拉取 + 本地 store 落库）；
        // fake 恒零汇总（读面纪律）。汇总字段名 camelCase wire 直投。
        "storage.importGroups" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.import_storage_groups(account).await {
                Ok(outcome) => jsonrpc::success_response(
                    id,
                    json!({
                        "recordsRead": outcome.records_read,
                        "imported": outcome.imported,
                        "alreadyKnown": outcome.already_known,
                        "failed": outcome.failed,
                    }),
                ),
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
        // storage.getCallLinks（窗D A6，官方 mergeCallLinkRecord 读腿）：单参
        // account（只读 Storage 面，无参数形状分叉）；fake 恒空表（读面纪律）。
        "storage.getCallLinks" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.get_call_links(account).await {
                Ok(links) => jsonrpc::success_response(id, call_link_sync_payload(&links)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // storage.setCallLink（窗D A6 同批，官方 toCallLinkRecord 写腿）：
        // active = rootKey+adminPasskey、deleted = rootKey+deletedAt-
        // TimestampMs（官方 deleted 分支 adminKey 恒缺同形）；两键同置 =
        // 官方 isBadRemoteData 同位矛盾意图，fail-closed -32603（参数面
        // 不占用结构化码）。deletedAtTimestampMs 须正整数（0 = 不删除，
        // 无动作语义不收）。
        "storage.setCallLink" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(root_key) = params
                .get("rootKey")
                .and_then(Value::as_str)
                .and_then(crate::service::call_links::decode_root_key)
            else {
                return internal_error(id);
            };
            let admin_passkey = match params.get("adminPasskey") {
                None | Some(Value::Null) => Some(None),
                Some(Value::String(text)) => {
                    crate::service::storage_call_link::decode_admin_passkey(text).map(Some)
                }
                Some(_) => None,
            };
            let deleted_at = match params.get("deletedAtTimestampMs") {
                None | Some(Value::Null) => Some(None),
                Some(value) => value.as_u64().filter(|ts| *ts > 0).map(Some),
            };
            let (Some(admin_passkey), Some(deleted_at)) = (admin_passkey, deleted_at) else {
                return internal_error(id);
            };
            if admin_passkey.is_some() && deleted_at.is_some() {
                return internal_error(id);
            }
            // 空意图（两腿皆缺）不收：官方 upload 恒不写无 adminKey 的
            // call link（storage.preload.ts adminKey==null skip 同位），
            // rootKey 单独无动作语义。
            if admin_passkey.is_none() && deleted_at.is_none() {
                return internal_error(id);
            }
            let intent = crate::service::storage_call_link::CallLinkWrite {
                root_key,
                admin_passkey,
                deleted_at_timestamp_ms: deleted_at,
            };
            match service.set_call_link(account, intent).await {
                Ok(links) => jsonrpc::success_response(id, call_link_sync_payload(&links)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // 安全码引擎面（KT 扩展方法，pinned signal-cli 同名动词原生面，
        // connector 契约 1.68 §4.61 上游形状逐字对位）：单引用窄读 +
        // 三形态比对闸写面。参数 fail-closed -32603；-1 两码原文位
        // （TrustCommand.java UserErrorException 逐字）见 identity.rs 常量。
        "listIdentities" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            // number 必填（账号全量清单形态不提供，compat §3 行）。
            let Some(number) = params.get("number").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service
                .list_identities(account, number, Some(number.to_string()))
                .await
            {
                Ok(identities) => jsonrpc::success_response(id, Value::Array(identities)),
                Err(()) => internal_error(id),
            }
        }
        "trust" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(recipient) = params.get("recipient").and_then(Value::as_str) else {
                return internal_error(id);
            };
            // 两臂恰一：verifiedSafetyNumber（三形态码比对闸）|
            // state（clear 词表，无数字闸）。
            let verified = params.get("verifiedSafetyNumber").and_then(Value::as_str);
            let state = params.get("state").and_then(Value::as_str);
            use crate::service::identity::{TrustIntent, TrustState};
            let intent = match (verified, state) {
                (Some(code), None) => TrustIntent::Verified {
                    code: code.to_string(),
                },
                (None, Some("unverified")) => TrustIntent::SetState(TrustState::Unverified),
                (None, Some("default")) => TrustIntent::SetState(TrustState::Default),
                // 同置/双缺/state 词表外 fail-closed。
                _ => return internal_error(id),
            };
            use crate::service::identity::TrustError;
            match service.trust_identity(account, recipient, intent).await {
                Ok(()) => jsonrpc::success_response(id, json!({})),
                Err(TrustError::Failed) => jsonrpc::error_response(
                    id,
                    &RpcError::new(-1, crate::service::identity::TRUST_FAILED_MESSAGE),
                ),
                Err(TrustError::InvalidCode) => jsonrpc::error_response(
                    id,
                    &RpcError::new(-1, crate::service::identity::TRUST_INVALID_CODE_MESSAGE),
                ),
                // 账号未注册 / sidecar 本地写失败：引擎通用内部错误。
                Err(TrustError::UnknownAccount) | Err(TrustError::Io) => internal_error(id),
            }
        }
        // Storage 跨设备同步批（KT 扩展方法；官方 chatFolder/
        // notificationProfile 记录面 upload 语义，compat §3 行 + §5 块）：
        // 参数 fail-closed -32603；storage 链路四码折叠 storage_error。
        "chatFolders.list" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.chat_folders(account).await {
                Ok(folders) => jsonrpc::success_response(id, json!({ "folders": folders })),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "chatFolders.setRecords" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            use crate::service::storage_records;
            let Some((records, remove_ids)) = parse_record_batch(
                params,
                storage_records::parse_folder,
                storage_records::uuid_bytes,
            ) else {
                return internal_error(id);
            };
            match service.set_chat_folders(account, records, remove_ids).await {
                Ok(folders) => jsonrpc::success_response(id, json!({ "folders": folders })),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "notificationProfiles.list" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.notification_profiles(account).await {
                Ok(profiles) => jsonrpc::success_response(id, json!({ "profiles": profiles })),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "notificationProfiles.setRecords" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            use crate::service::storage_records;
            let Some((records, remove_ids)) = parse_record_batch(
                params,
                storage_records::parse_profile,
                storage_records::hex_id_bytes,
            ) else {
                return internal_error(id);
            };
            match service
                .set_notification_profiles(account, records, remove_ids)
                .await
            {
                Ok(profiles) => jsonrpc::success_response(id, json!({ "profiles": profiles })),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        // Stories 分布名单面（KT 扩展方法，官方 storyDistributionList 记录
        // 面 upload 语义，compat §3 行 + §5 名单面块）：参数 fail-closed
        // -32603；storage 链路四码折叠 storage_error。
        "storyDistributionLists.list" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.story_distribution_lists(account).await {
                Ok(lists) => jsonrpc::success_response(id, json!({ "lists": lists })),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "storyDistributionLists.setRecords" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            use crate::service::storage_records;
            let Some((records, remove_ids)) = parse_record_batch(
                params,
                storage_records::parse_story_list,
                storage_records::uuid_bytes,
            ) else {
                return internal_error(id);
            };
            match service
                .set_story_distribution_lists(account, records, remove_ids)
                .await
            {
                Ok(lists) => jsonrpc::success_response(id, json!({ "lists": lists })),
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
        // 全量写（givenName 必填非空，字节预算 mirror connector 契约 1.46：
        // given/family 各 ≤128B、about ≤512B、emoji ≤32B，超预算 fail-closed），
        // avatar 三态（缺省=保留、data URI=换新、null=移除）；形状错
        // fail-closed -32603（置顶面参数处理先例）。
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
            let update = crate::service::profile::ProfileUpdate {
                given_name: given_name.to_owned(),
                family_name,
                about,
                emoji,
                avatar,
            };
            if !crate::service::profile::text_budget_ok(&update) {
                return internal_error(id);
            }
            match service.set_profile(account, update).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "saved" })),
                Err(crate::service::profile::ProfileError::Storage(
                    storage_sync::StorageSyncError::UnknownAccount,
                )) => internal_error(id),
                Err(crate::service::profile::ProfileError::Storage(error)) => {
                    storage_error(id, &error)
                }
                // 头像链失败：PROFILE_UPLOAD_FAILED（-1，retryable=true），
                // 文字面已提交时 data.textSaved=true（部分成功诚实呈现）。
                Err(crate::service::profile::ProfileError::UploadFailed { text_saved }) => {
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "PROFILE_UPLOAD_FAILED",
                            json!({ "retryable": true, "textSaved": text_saved }),
                        ),
                    )
                }
            }
        }
        // ---- 用户名面（KT 扩展方法，compat §3 用户名块）----
        // getUsername（读面）：AccountRecord.username(33)/usernameLink(35)
        // 投影 `{username?, usernameLink?}`（键缺席 = 未设）。storage 读链
        // 四码族（Unavailable/ReadFailed）；未注册账号 -32603。
        "getUsername" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.get_username(account).await {
                Ok(projection) => jsonrpc::success_response(id, projection),
                Err(error) => storage_error(id, &error),
            }
        }
        // usernameLookup（读面）：条目批 → 服务端匿名 lookup 通路 → 逐条
        // 投影（输入序）。参数面 fail-closed：usernames 非数组/空批/超 20/
        // 条目非字符串/空/超 64 字符一律 -32603；未注册账号 -32603。
        // 单条解析失败诚实降级 `{recipient, isRegistered:false}` 不炸整批。
        "usernameLookup" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let entries = match params.get("usernames") {
                Some(Value::Array(items)) => {
                    let mut entries = Vec::with_capacity(items.len());
                    for item in items {
                        match item.as_str() {
                            Some(entry)
                                if !entry.is_empty()
                                    && entry.chars().count()
                                        <= crate::service::username::USERNAME_LOOKUP_ENTRY_MAX_CHARS =>
                            {
                                entries.push(entry.to_owned())
                            }
                            _ => return internal_error(id),
                        }
                    }
                    if entries.is_empty()
                        || entries.len() > crate::service::username::USERNAME_LOOKUP_MAX_ENTRIES
                    {
                        return internal_error(id);
                    }
                    entries
                }
                _ => return internal_error(id),
            };
            match service.username_lookup(account, entries).await {
                Ok(rows) => jsonrpc::success_response(id, rows),
                Err(error) => storage_error(id, &error),
            }
        }
        // ---- 用户名写面（KT 扩展方法）----
        // usernameReserve：候选生成 + 批 hash → 服务端 reserve → 采纳候选
        // `{username, usernameHash}`。参数面（语法闸）-32603；服务器态四码
        // （TAKEN/RESERVATION_GONE/RATE_LIMITED/SERVER_FAILED）-1 + 码位。
        "usernameReserve" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(username) = params.get("username").and_then(Value::as_str) else {
                return internal_error(id);
            };
            if username.is_empty() || username.chars().count() > 38 {
                return internal_error(id);
            }
            match service.username_reserve(account, username).await {
                Ok(reservation) => jsonrpc::success_response(
                    id,
                    json!({
                        "username": reservation.username,
                        "usernameHash": reservation.username_hash,
                    }),
                ),
                Err(error) => username_write_error(id, &error),
            }
        }
        // usernameConfirm：proof + link 加密 → 服务端 confirm → 完整链接 →
        // AccountRecord 33/35 落盘 → `{username, usernameLink}`。usernameHash
        // 必带（reserve 采纳值回传）——缺失/非字符串/失配 -32603 fail-closed。
        "usernameConfirm" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(username) = params.get("username").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(username_hash) = params.get("usernameHash").and_then(Value::as_str) else {
                return internal_error(id);
            };
            if username.is_empty() || username_hash.is_empty() {
                return internal_error(id);
            }
            match service
                .username_confirm(account, username, username_hash)
                .await
            {
                Ok(result) => jsonrpc::success_response(id, result),
                Err(error) => username_write_error(id, &error),
            }
        }
        // usernameDelete：服务端 DELETE → AccountRecord 33/35 清除（本就
        // 未设 = 幂等无写）→ `{}`。
        "usernameDelete" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.username_delete(account).await {
                Ok(result) => jsonrpc::success_response(id, result),
                Err(error) => username_write_error(id, &error),
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
        // 会话状态三 flag 读写面（getConversationState/setConversationState，
        // P15b 项2 KT 扩展方法）：Storage Service 真相源（Contact/GV1/GV2
        // 记录 archived/markedUnread/mutedUntilTimestamp 位），参数纪律与
        // setConversationBlocked 同款（kind 缺失/未知 fail-closed -32603）。
        // set 臂三 flag 全缺（或全 null）= 无写意图整拒；写后返回目标三
        // flag 读投影（getConversationState 同形状）。
        "getConversationState" => {
            state.lock().await.append_dispatch(method, params);
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(conversation) = conversation_of(
                params.get("conversationId").and_then(Value::as_str),
                params.get("kind").and_then(Value::as_str),
            ) else {
                return internal_error(id);
            };
            match service.conversation_state(account, conversation).await {
                Ok(state) => jsonrpc::success_response(id, conversation_state_payload(&state)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(error) => storage_error(id, &error),
            }
        }
        "setConversationState" => {
            state.lock().await.append_dispatch(method, params);
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(conversation) = conversation_of(
                params.get("conversationId").and_then(Value::as_str),
                params.get("kind").and_then(Value::as_str),
            ) else {
                return internal_error(id);
            };
            // 三 flag 各自 Option（null/错误形状 = 该位非法整拒——官方
            // falsy no-op 的静默语义引擎不收，updateGroup title 同纪律）。
            let muted_until = match params.get("mutedUntilTimestamp") {
                None => Some(None),
                Some(Value::Number(value)) => value.as_u64().map(Some),
                Some(_) => None,
            };
            let archived = match params.get("archived") {
                None => Some(None),
                Some(Value::Bool(value)) => Some(Some(*value)),
                Some(_) => None,
            };
            let marked_unread = match params.get("markedUnread") {
                None => Some(None),
                Some(Value::Bool(value)) => Some(Some(*value)),
                Some(_) => None,
            };
            let (Some(muted_until), Some(archived), Some(marked_unread)) =
                (muted_until, archived, marked_unread)
            else {
                return internal_error(id);
            };
            let intent = storage_sync::ConversationStateWrite {
                muted_until_timestamp: muted_until,
                archived,
                marked_unread,
            };
            if intent == storage_sync::ConversationStateWrite::default() {
                // 三 flag 全缺 = 无写意图（updateGroup 全缺整拒同纪律）。
                return internal_error(id);
            }
            match service
                .set_conversation_state(account, conversation, intent)
                .await
            {
                Ok(written) => jsonrpc::success_response(id, conversation_state_payload(&written)),
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
        // getDevices（设备清单读面，窗D B13，KT 扩展方法）：单参 account
        // （GET v1/devices 投影 + 设备名引擎侧解密——本机 ACI 身份私钥）；
        // fake 恒空表（读面纪律）。参数面 fail-closed -32603；服务器拒绝
        // -1 DEVICES_FAILED（data.status? 诚实透传）；传输面 -1 DEVICES_
        // UNAVAILABLE。
        "getDevices" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.list_devices(account).await {
                Ok(devices) => jsonrpc::success_response(
                    id,
                    json!({
                        "devices": devices
                            .iter()
                            .map(|device| {
                                json!({
                                    "deviceId": device.device_id,
                                    "name": device.name,
                                    "registrationId": device.registration_id,
                                    "lastSeenMs": device.last_seen_ms,
                                    "createdAtCiphertext": device.created_at_ciphertext,
                                })
                            })
                            .collect::<Vec<_>>(),
                    }),
                ),
                Err(error) => device_error(id, &error),
            }
        }
        // unlinkDevice（服务端解绑，窗D B13 同批，KT 扩展方法）：deviceId
        // 正整数 → DELETE v1/devices/{id} 直通（官方 unlink 同端点；服务
        // 器裁决授权：primary 解绑任意 linked、linked 解绑自身，越权/主设
        // 备删除拒绝原样透出）；fake 成功桩。
        "unlinkDevice" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(device_id) = params
                .get("deviceId")
                .and_then(Value::as_u64)
                .and_then(|id| u32::try_from(id).ok())
                .filter(|id| *id > 0)
            else {
                return internal_error(id);
            };
            match service.unlink_device(account, device_id).await {
                Ok(()) => jsonrpc::success_response(id, json!({})),
                Err(error) => device_error(id, &error),
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
        // 官方桌面本地备份快照导入（docs/backup-export-contract.md §11，KT
        // 扩展方法）：`{account, snapshotDir}` → metadata 快校验（目录/metadata
        // 缺、形状不合规、错钥当场答结构化码）→ 后台帧导出（NDJSON 落盘与
        // 存量历史迁移同一落点，connector 消费）。成功 `{"status":"started"}`；
        // 参数面 -32603（缺 account/snapshotDir/未注册/AEP 缺）；结构化码
        // -1 + message（§11 五码位 + ALREADY_RUNNING）。快照目录路径由
        // connector 传入（同机路径交接，§10.2 artifact 反向同款），引擎不
        // 持久化。
        "backup.importSnapshot" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            let Some(snapshot_dir) = params.get("snapshotDir").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service
                .backup_import_snapshot(account, std::path::Path::new(snapshot_dir))
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "started" })),
                Err(crate::service::snapshot_import::SnapshotImportRejection::Structured(code)) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, code))
                }
                Err(crate::service::snapshot_import::SnapshotImportRejection::Internal) => {
                    internal_error(id)
                }
            }
        }
        "backup.importSnapshotStatus" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.backup_import_snapshot_status(account).await {
                Ok(payload) => jsonrpc::success_response(id, payload),
                Err(()) => internal_error(id),
            }
        }
        // 云备份 restore 启动（backups.restoreFromCloud，KT 扩展方法；
        // cloud-backup-contract §2）：send lane mutating。凭据拉取（404→
        // backupId 引导自愈→重试）→zkgroup 兑换→生产 chat 连接→备份定位→
        // CDN 读凭据→下载→解密导出 NDJSON 全链后台化，同步答 started；
        // 重入（运行中再启）答 BACKUP_RUNNING（-1，retryable=false）；
        // NotProvisioned=账号从未开启云备份（引导自愈后仍缺席，retryable=
        // false，UI 诚实降级提示）；AuthRejected=zkgroup 兑换被拒
        // （retryable=false）。错误族→BACKUP_* 码映射随批记 compat。
        "backups.restoreFromCloud" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.cloud_backup_restore_start(account).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "started" })),
                Err(e) => {
                    // 统一码表（cloud_backup::CloudRestoreError::rpc_code）：
                    // cloudStatus failed 相位同表，两端不漂。
                    let rpc = match e.detail() {
                        Some(detail) => {
                            RpcError::with_data(-1, e.rpc_code(), json!({ "detail": detail }))
                        }
                        None => RpcError::new(-1, e.rpc_code()),
                    };
                    jsonrpc::error_response(id, &rpc)
                }
            }
        }
        // 云备份 restore 进度/状态（backups.cloudStatus，KT 扩展方法）：
        // transfer.downloadStatus 同款快照读（缺席={present:false}；在跑=
        // {present:true, bytesRead, bytesTotal?}）；只读不落调度日志零 stderr
        // （storage 状态面纪律）。
        "backups.cloudStatus" => {
            // 进度面进程级单槽（同引擎至多一个 restore，运行闸全局）；account
            // 仅做契约形状校验，不参与路由。
            let Some(_account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.cloud_backup_status() {
                Some(snapshot) => {
                    let mut payload = json!({
                        "present": true,
                        "phase": snapshot.phase,
                        "bytesRead": snapshot.bytes_read,
                    });
                    if let Some(total) = snapshot.bytes_total {
                        payload["bytesTotal"] = json!(total);
                    }
                    if let Some(code) = snapshot.error_code {
                        payload["errorCode"] = json!(code);
                    }
                    jsonrpc::success_response(id, payload)
                }
                None => jsonrpc::success_response(id, json!({ "present": false })),
            }
        }
        // 云备份 upload 启动（backups.uploadToCloud，KT 扩展方法；cloud-
        // backup-contract §6）：send lane mutating。装配段（专用线程）→
        // 凭据→zkgroup→form gRPC→TUS 单发 POST→refresh_backup 全链后台化，
        // 同步答 started；重入答 BACKUP_UPLOAD_ALREADY_RUNNING（-1）；
        // 工件缺失/首行非法答 BACKUP_ARTIFACT_MISSING/INVALID（同步终态）。
        // 码表六族→BACKUP_* 映射随批记 compat §3（新码位白名单登记归
        // connector 透传批）。
        "backups.uploadToCloud" => {
            let Some(account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.cloud_backup_upload_start(account).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "started" })),
                Err(e) => {
                    // 统一码表（cloud_upload::CloudUploadError::rpc_code）：
                    // uploadStatus failed 相位同表，两端不漂。
                    let rpc = match e.detail() {
                        Some(detail) => {
                            RpcError::with_data(-1, e.rpc_code(), json!({ "detail": detail }))
                        }
                        None => RpcError::new(-1, e.rpc_code()),
                    };
                    jsonrpc::error_response(id, &rpc)
                }
            }
        }
        // 云备份 upload 进度/状态（backups.uploadStatus，KT 扩展方法）：
        // cloudStatus 同款快照读（缺席={present:false}；在跑={present:true,
        // phase, bytesRead, bytesTotal?}，failed 相位附 errorCode）；只读不落
        // 调度日志零 stderr（storage 状态面纪律）。account 仅做契约形状校验。
        "backups.uploadStatus" => {
            let Some(_account) = params.get("account").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match service.cloud_upload_status() {
                Some(snapshot) => {
                    let mut payload = json!({
                        "present": true,
                        "phase": snapshot.phase,
                        "bytesRead": snapshot.bytes_read,
                    });
                    if let Some(total) = snapshot.bytes_total {
                        payload["bytesTotal"] = json!(total);
                    }
                    if let Some(code) = snapshot.error_code {
                        payload["errorCode"] = json!(code);
                    }
                    jsonrpc::success_response(id, payload)
                }
                None => jsonrpc::success_response(id, json!({ "present": false })),
            }
        }
        // transfer_archive 下载进度快照查询（transfer.downloadStatus，KT 扩展
        // 方法）：`history.importStatus` progress 组合面的引擎侧只读数据源
        // （契约 1.53 §4.38 扩展）。快照缺席（未开始/已结束/引擎重启过）=
        // `{present: false}`；在跑 = `{present: true, bytesRead,
        // bytesTotal?}`——total 缺席是诚实形态（content-length 拿不到），
        // 消费方不给假 0。只读不落调度日志零 stderr，同 backup.exportStatus
        // 纪律。未注册/fake 账号同样答 `{present: false}`：快照面是进程内
        // 观测，不泄露注册状态。
        "transfer.downloadStatus" => {
            let account = params.get("account").and_then(Value::as_str).unwrap_or("");
            let snapshot = service.transfer_archive_snapshot(account);
            let payload = match snapshot {
                None => json!({ "present": false }),
                Some(snapshot) => {
                    let mut value = json!({
                        "present": true,
                        "bytesRead": snapshot.bytes_read,
                    });
                    if let Some(total) = snapshot.bytes_total {
                        value["bytesTotal"] = json!(total);
                    }
                    value
                }
            };
            jsonrpc::success_response(id, payload)
        }
        // 自建群（createGroup，KT 扩展方法，官方 createGroupV2 行为对齐）：
        // 协议上行面，落调度日志（terminateGroup 同纪律；avatar 键落盘前
        // 形状摘要化——updateGroup「调度零头像字节」同纪律）。title 必填
        // 非空（官方 createGroupV2 name: string 必填同位）；members 可选
        //（service-id 串/联系人号码，不可解析整拒 -32603——官方桌面缺本地
        // 联系人静默跳过是 GUI 语义，API 面不静默）；expiration/avatar
        // 三态与 updateGroup 同形状（expiration=0 = 官方 `if (expireTimer)`
        // falsy 落缺省同语义：建群不带 timer action）。参数面 fail-closed
        // -32603；服务器拒绝 → -1 GROUP_CREATE_FAILED + data.status 诚实
        // 透传；avatar 上行链失败 → GROUP_AVATAR_UPLOAD_FAILED（伴生面未
        // 提交，updateGroup 同 data）；网络面 → STORAGE_UNAVAILABLE；
        // self 凭据取材失败（无 presentation 服务器必拒）→ GROUP_CREATE_
        // FAILED data.status 缺省。fake 模式形状桩 {groupId(随机 master
        // key 标准 base64), revision: 0}（avatar data URI 臂失败同
        // updateGroup 两臂纪律）。
        "createGroup" => {
            let mut logged_params = params.clone();
            if let Some(Value::String(uri)) = logged_params.get_mut("avatar") {
                let digest = format!("[avatar data uri: {} bytes]", uri.len());
                *logged_params.get_mut("avatar").expect("avatar slot") = json!(digest);
            }
            state.lock().await.append_dispatch(method, &logged_params);
            let account = params.get("account").and_then(Value::as_str);
            let title = match params.get("title") {
                Some(Value::String(title)) if !title.is_empty() => title.to_owned(),
                Some(_) => return internal_error(id),
                None => return internal_error(id),
            };
            let expiration = params
                .get("expiration")
                .and_then(Value::as_u64)
                .and_then(|seconds| u32::try_from(seconds).ok());
            if params.get("expiration").is_some() && expiration.is_none() {
                return internal_error(id);
            }
            let avatar = match params.get("avatar") {
                None => None,
                Some(Value::Null) => Some(crate::service::groups::AvatarWrite::Remove),
                Some(Value::String(uri)) => {
                    match crate::service::profile::parse_avatar_data_uri(uri) {
                        Some((_, bytes)) => {
                            Some(crate::service::groups::AvatarWrite::Replace(bytes))
                        }
                        None => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            let members = match params.get("members") {
                None => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.is_empty() {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry.as_str() {
                            Some(text) => parsed.push(text.to_owned()),
                            None => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            let Some(account) = account else {
                return internal_error(id);
            };
            match service
                .create_group(account, &title, expiration, avatar, &members)
                .await
            {
                Ok((group_id, revision)) => jsonrpc::success_response(
                    id,
                    json!({ "groupId": group_id, "revision": revision }),
                ),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup)
                | Err(crate::service::groups::TerminateError::InviteLinkUnset) => {
                    internal_error(id)
                }
                Err(crate::service::groups::TerminateError::AvatarUpload) => {
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_AVATAR_UPLOAD_FAILED",
                            json!({ "retryable": true, "textSaved": false }),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_CREATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
            }
        }
        // 群终止（terminateGroup，KT 扩展方法）：协议上行面，落调度日志
        //（send 系/reportSpam 同纪律）。groupId 必须 32B 标准 base64 群
        // master key、缺 account/坏 groupId → 参数面 -32603 fail-closed；
        // 服务器拒绝 → -1 GROUP_TERMINATE_FAILED + data.status 诚实透传；
        // 网络面 → -1 STORAGE_UNAVAILABLE（STORAGE 族 retryable 语义）；
        // fake 模式成功 {status:"terminated"}。
        "terminateGroup" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            match service.terminate_group(account, master_key).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "terminated" })),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup)
                | Err(crate::service::groups::TerminateError::InviteLinkUnset) => {
                    internal_error(id)
                }
                // 群终止链路不触头像上行（变体穷尽性；同按写面失败码位折叠）。
                Err(crate::service::groups::TerminateError::AvatarUpload) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "GROUP_UPDATE_FAILED"))
                }
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_TERMINATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
            }
        }
        // 群更新写面（updateGroup，KT 扩展方法）：expiration + title + avatar
        // + addMembers + removeMembers 参数族，协议上行面，落调度日志
        //（terminateGroup 同纪律；avatar 键落盘前形状摘要化——data URI/base64
        // 载荷零字节进日志，setProfile 头像纪律；title/members 短标签照旧逐
        // 字保留）。expiration：0 = 关闭，u32 形状（null/字符串/负数/小数/
        // 超界 fail-closed）；title：非空 string（null/非字符串/空串
        // fail-closed -32603——官方 `if (title)` 对空串 falsy no-op 的语义本
        // 引擎不静默收，全缺整拒同纪律）；avatar 三态镜像 setProfile（缺省=
        // 保留、data:image/*;base64, 规范 base64=换新、null=移除；非 image
        // 族/非规范 base64/解码超 10MiB/非字符串 -32603）；addMembers：非空
        // 字符串数组（service-id 串/联系人号码，缺联系成员解析同 createGroup
        // ——不可解析整拒 -32603，官方桌面静默跳过是 GUI 语义；self 静默剔
        // 除——self 入 change 服务器必 400；有凭据 = 全成员、凭据失败/无
        // key/PNI = pending 邀请，官方 buildAddMembersChange 分流同构）；
        // removeMembers：非空字符串数组（同解析；群态 members 面 →
        // delete_members、pending 面 → delete_members_pending_profile_key、
        // 两者皆非整拒 -32603——官方 buildDeleteMemberChange 不设防的 GUI
        // 前置，API 面不静默）；addFromInviteLinkAccess：词表字符串
        // ANY/MEMBER/ADMINISTRATOR/UNSATISFIABLE（UNKNOWN 是读面兜底词不作
        // 写意图、未知词/null/非字符串 -32603——官方
        // buildAccessControlAddFromInviteLinkChange 枚举直通同位）；
        // inviteLinkPassword：标准 base64 非空字符串（null/空串/坏 base64
        // -32603；原始字节直入 modify_invite_link_password——官方
        // buildInviteLinkPasswordChange `Bytes.fromBase64` 同位，长度服务器
        // 裁决不本地硬闸）；全缺 = 无操作整拒 -32603（不静默 no-op），
        // 单项或组合在场均合法（同调合成单 change 一次 PATCH）；groupId/
        // account 纪律与 terminateGroup 同款；群头像上行链失败 → -1
        // GROUP_AVATAR_UPLOAD_FAILED + data {retryable:true, textSaved:false}
        //（合成单 change 语义下伴生面未提交，§5）；服务器拒绝 → -1
        // GROUP_UPDATE_FAILED + data.status 诚实透传；网络面 → -1
        // STORAGE_UNAVAILABLE；fake 两臂：缺省/null 臂成功 {status:"sent"}、
        // data URI 臂失败 GROUP_AVATAR_UPLOAD_FAILED（§5 fake 两臂纪律）。
        "updateGroup" => {
            // 调度日志先落（mutating 面同款：被拒调用同样记录派发）；
            // avatar 键落盘前形状摘要化（头像 data URI/base64 载荷零字节
            // 进日志，setProfile「调度零头像字节」同纪律；摘要以 URI 字符
            // 串长度记量级）。groupId/account/expiration 键照旧逐字保留。
            let mut logged_params = params.clone();
            if let Some(Value::String(uri)) = logged_params.get_mut("avatar") {
                let digest = format!("[avatar data uri: {} bytes]", uri.len());
                *logged_params.get_mut("avatar").expect("avatar slot") = json!(digest);
            }
            state.lock().await.append_dispatch(method, &logged_params);
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let expiration = params
                .get("expiration")
                .and_then(Value::as_u64)
                .and_then(|seconds| u32::try_from(seconds).ok());
            let title = params.get("title").and_then(Value::as_str);
            let add_members = match params.get("addMembers") {
                None => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.is_empty() {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry.as_str() {
                            Some(text) => parsed.push(text.to_owned()),
                            None => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            let remove_members = match params.get("removeMembers") {
                None => Vec::new(),
                Some(Value::Array(entries)) => {
                    if entries.is_empty() {
                        return internal_error(id);
                    }
                    let mut parsed = Vec::with_capacity(entries.len());
                    for entry in entries {
                        match entry.as_str() {
                            Some(text) => parsed.push(text.to_owned()),
                            None => return internal_error(id),
                        }
                    }
                    parsed
                }
                Some(_) => return internal_error(id),
            };
            let avatar = match params.get("avatar") {
                None => None,
                Some(Value::Null) => Some(crate::service::groups::AvatarWrite::Remove),
                Some(Value::String(uri)) => {
                    match crate::service::profile::parse_avatar_data_uri(uri) {
                        Some((_, bytes)) => {
                            Some(crate::service::groups::AvatarWrite::Replace(bytes))
                        }
                        None => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            // 链接写面两臂（P13/任务2）：形状非法（null/未知词/非字符串/
            // 空串/坏 base64）恒拒，与 title 键在场即闸同纪律。
            let add_from_invite_link_access = match params.get("addFromInviteLinkAccess") {
                None => None,
                Some(Value::String(word)) => {
                    use presage::libsignal_service::groups_v2::AccessRequired;
                    let access = match word.as_str() {
                        "ANY" => AccessRequired::Any,
                        "MEMBER" => AccessRequired::Member,
                        "ADMINISTRATOR" => AccessRequired::Administrator,
                        "UNSATISFIABLE" => AccessRequired::Unsatisfiable,
                        _ => return internal_error(id),
                    };
                    Some(access)
                }
                Some(_) => return internal_error(id),
            };
            let invite_link_password = match params.get("inviteLinkPassword") {
                None => None,
                Some(Value::String(text)) => {
                    use base64::Engine as _;
                    match base64::engine::general_purpose::STANDARD.decode(text) {
                        Ok(bytes) if !bytes.is_empty() => Some(bytes),
                        _ => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            // expiration 键在场但形状非法（null/字符串/负数/小数/超界）恒拒
            // ——与「合法缺席臂」（键不在场，avatar 在场时放行）区分。title
            // 键在场但 null/非字符串/空串恒拒（空串 = 官方 falsy no-op 语义
            // 不静默收）。
            if params.get("expiration").is_some() && expiration.is_none() {
                return internal_error(id);
            }
            if params.get("title").is_some() && title.map(str::is_empty).unwrap_or(true) {
                return internal_error(id);
            }
            // 全缺 = 无操作整拒（不静默 no-op）：expiration/title/avatar/
            // addMembers/removeMembers/链接两面至少一项在场。
            if expiration.is_none()
                && title.is_none()
                && avatar.is_none()
                && add_members.is_empty()
                && remove_members.is_empty()
                && add_from_invite_link_access.is_none()
                && invite_link_password.is_none()
            {
                return internal_error(id);
            }
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            let write = crate::service::groups::GroupUpdateWrite {
                expiration_secs: expiration,
                title: title.map(str::to_owned),
                avatar,
                add_members,
                remove_members,
                add_from_invite_link_access,
                invite_link_password,
            };
            match service
                .update_group_change(account, master_key, write)
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "sent" })),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup)
                | Err(crate::service::groups::TerminateError::InviteLinkUnset) => {
                    internal_error(id)
                }
                Err(crate::service::groups::TerminateError::AvatarUpload) => {
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_AVATAR_UPLOAD_FAILED",
                            json!({ "retryable": true, "textSaved": false }),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_UPDATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
            }
        }
        // 群退出写面（leaveGroup，KT 扩展方法，官方桌面 leaveGroupV2 行为
        // 对齐）：协议上行面，落调度日志（terminateGroup 同纪律）。参数
        // {account, groupId}——terminateGroup 同款闸（groupId = 32B 群 master
        // key 标准 base64）。群态读面 self 分类（官方同序：ACI pending → 撤
        // pending 邀请、ACI member → delete_members 退群、ACI 都不是且 PNI
        // pending → 撤 PNI 邀请[官方 syncMessageOnly 面]、都不是 → -32603
        // ——官方桌面仅日志不行动的 GUI 语义，API 面整拒不静默）；写后本地
        // 群态刷新（best-effort，群登记不删，terminate 面同纪律）；服务器拒
        // 绝 → -1 GROUP_UPDATE_FAILED + data.status 诚实透传；网络面 → -1
        // STORAGE_UNAVAILABLE；fake 成功 {status:"left"} 形状桩（§5 fake 两
        // 臂纪律）。
        "leaveGroup" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            match service.leave_group(account, master_key).await {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "left" })),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup)
                | Err(crate::service::groups::TerminateError::InviteLinkUnset) => {
                    internal_error(id)
                }
                Err(crate::service::groups::TerminateError::AvatarUpload) => internal_error(id),
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_UPDATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
            }
        }
        // 群邀请链接族（P13 链接批，KT 扩展方法，官方 generateGroupInvite
        // LinkPassword/buildGroupLink/parseGroupLink 行为对齐）：三方法皆
        // 密码/链接材料面——非协议上行，不落调度日志；密码/链接字节零日志
        //（凭据面同纪律）。generateInviteLinkPassword 无参数（纯 CSPRNG，
        // 16B，fake/real 同码路）；buildInviteLink {account, groupId,
        // password?}：password 显式臂纯构造（fake/real 同码路）、缺省臂读
        // 服务器群态 invite_link_password（fake 无群态面 → GROUP_INVITE_
        // LINK_UNAVAILABLE）；parseInviteLink {inviteLink} 纯解析（三种官方
        // URL 形 + 裸 inviteCode 容错，坏形状 -32603 fail-closed）。
        "generateInviteLinkPassword" => {
            let password = service.generate_invite_link_password();
            jsonrpc::success_response(id, json!({ "password": standard_b64(&password) }))
        }
        "buildInviteLink" => {
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let password = match params.get("password") {
                None => None,
                Some(Value::String(text)) => {
                    use base64::Engine as _;
                    match base64::engine::general_purpose::STANDARD.decode(text) {
                        Ok(bytes) if !bytes.is_empty() => Some(bytes),
                        _ => return internal_error(id),
                    }
                }
                Some(_) => return internal_error(id),
            };
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            match service
                .build_invite_link(account, &master_key, password)
                .await
            {
                Ok(invite_link) => {
                    jsonrpc::success_response(id, json!({ "inviteLink": invite_link }))
                }
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup) => internal_error(id),
                Err(crate::service::groups::TerminateError::InviteLinkUnset) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "GROUP_INVITE_LINK_UNAVAILABLE"))
                }
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_INVITE_LINK_UNAVAILABLE",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
                Err(crate::service::groups::TerminateError::AvatarUpload) => internal_error(id),
            }
        }
        "parseInviteLink" => {
            let Some(link) = params.get("inviteLink").and_then(Value::as_str) else {
                return internal_error(id);
            };
            match crate::service::group_link::parse_invite_link(link) {
                Ok(parsed) => jsonrpc::success_response(
                    id,
                    json!({
                        "groupId": standard_b64(&parsed.master_key),
                        "password": standard_b64(&parsed.password),
                    }),
                ),
                Err(
                    crate::service::group_link::LinkParseError::Empty
                    | crate::service::group_link::LinkParseError::Malformed
                    | crate::service::group_link::LinkParseError::UnsupportedVersion,
                ) => internal_error(id),
            }
        }
        // 群邀请链接族读写面（P13/任务3，KT 扩展方法，官方 getPreJoinGroup
        // Info/joinGroupV2ViaLink 行为对齐）：previewGroupViaLink 读面不落
        // 调度日志（材料面纪律）；joinGroupViaLink 写面落调度日志（mutating
        // 面纪律；inviteLink 键照旧逐字保留——wire 面 20+ 调用方本就明文携
        // 链接，摘要有意不设）。inviteLink 形状面 -32603（parseInviteLink
        // 同闸收口）。preview 服务器拒绝 → GROUP_INVITE_LINK_PREVIEW_FAILED
        // + data.status 诚实透传；join/审批面服务器拒绝 → GROUP_UPDATE_
        // FAILED + data.status；网络面 STORAGE_UNAVAILABLE。
        "previewGroupViaLink" => {
            let account = params.get("account").and_then(Value::as_str);
            let parsed = params
                .get("inviteLink")
                .and_then(Value::as_str)
                .and_then(|link| crate::service::group_link::parse_invite_link(link).ok());
            let (Some(account), Some(parsed)) = (account, parsed) else {
                return internal_error(id);
            };
            match service
                .preview_group_via_link(account, parsed.master_key, parsed.password)
                .await
            {
                Ok(preview) => {
                    let mut result = json!({
                        "groupId": preview.group_id,
                        "memberCount": preview.member_count,
                        "revision": preview.revision,
                        "addFromInviteLink": preview.add_from_invite_link,
                        "approvalRequired": preview.approval_required,
                        "pendingAdminApproval": preview.pending_admin_approval,
                    });
                    if let Some(name) = preview.name {
                        result["name"] = json!(name);
                    }
                    if let Some(description) = preview.description {
                        result["description"] = json!(description);
                    }
                    jsonrpc::success_response(id, result)
                }
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup) => internal_error(id),
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_INVITE_LINK_PREVIEW_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
                Err(
                    crate::service::groups::TerminateError::InviteLinkUnset
                    | crate::service::groups::TerminateError::AvatarUpload,
                ) => internal_error(id),
            }
        }
        "joinGroupViaLink" => {
            // 写面落调度日志（mutating 面先落后被拒同样记录派发）。
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let parsed = params
                .get("inviteLink")
                .and_then(Value::as_str)
                .and_then(|link| crate::service::group_link::parse_invite_link(link).ok());
            let (Some(account), Some(parsed)) = (account, parsed) else {
                return internal_error(id);
            };
            match service
                .join_group_via_link(account, parsed.master_key, parsed.password)
                .await
            {
                Ok(requested) => {
                    let status = if requested { "requested" } else { "joined" };
                    jsonrpc::success_response(id, json!({ "status": status }))
                }
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup) => internal_error(id),
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_UPDATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
                Err(
                    crate::service::groups::TerminateError::InviteLinkUnset
                    | crate::service::groups::TerminateError::AvatarUpload,
                ) => internal_error(id),
            }
        }
        // 群审批面（P13/任务4，KT 扩展方法，官方 cancelJoinRequest/
        // approvePendingMembershipFromGroupV2 行为对齐）：两面皆写面落调度
        // 日志（mutating 面纪律）。cancel 以 inviteLink 寻址（申请者非成员
        // 无群态寻址面——官方同位）；approve 以 groupId + recipient 寻址
        //（管理员有群态读面）。服务器拒绝 → GROUP_UPDATE_FAILED +
        // data.status；网络面 STORAGE_UNAVAILABLE；参数/本地闸面 -32603。
        "cancelJoinRequest" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let parsed = params
                .get("inviteLink")
                .and_then(Value::as_str)
                .and_then(|link| crate::service::group_link::parse_invite_link(link).ok());
            let (Some(account), Some(parsed)) = (account, parsed) else {
                return internal_error(id);
            };
            match service
                .cancel_join_request_via_link(account, parsed.master_key, parsed.password)
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "cancelled" })),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup) => internal_error(id),
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_UPDATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
                Err(
                    crate::service::groups::TerminateError::InviteLinkUnset
                    | crate::service::groups::TerminateError::AvatarUpload,
                ) => internal_error(id),
            }
        }
        "approveJoinRequest" => {
            state.lock().await.append_dispatch(method, params);
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let recipient = params.get("recipient").and_then(Value::as_str);
            let (Some(account), Some(master_key), Some(recipient)) =
                (account, master_key, recipient)
            else {
                return internal_error(id);
            };
            if recipient.trim().is_empty() {
                return internal_error(id);
            }
            match service
                .approve_join_request(account, master_key, recipient)
                .await
            {
                Ok(()) => jsonrpc::success_response(id, json!({ "status": "approved" })),
                Err(crate::service::groups::TerminateError::UnknownAccount)
                | Err(crate::service::groups::TerminateError::UnknownGroup) => internal_error(id),
                Err(crate::service::groups::TerminateError::Failed { status }) => {
                    let mut data = serde_json::Map::new();
                    if let Some(status) = status {
                        data.insert("status".to_owned(), json!(status));
                    }
                    jsonrpc::error_response(
                        id,
                        &jsonrpc::RpcError::with_data(
                            -1,
                            "GROUP_UPDATE_FAILED",
                            Value::Object(data),
                        ),
                    )
                }
                Err(crate::service::groups::TerminateError::Unavailable) => {
                    jsonrpc::error_response(id, &RpcError::new(-1, "STORAGE_UNAVAILABLE"))
                }
                Err(
                    crate::service::groups::TerminateError::InviteLinkUnset
                    | crate::service::groups::TerminateError::AvatarUpload,
                ) => internal_error(id),
            }
        }
        // calls.linkPresentation（call-link 凭据材料面，KT 扩展，P4c 切一；
        // compat §3 calls 族行 + §5 块）：材料供给面非协议上行，不落调度日志；
        // 凭据/呈现字节零日志零落盘（relay-api-v2 §5 纪律）。参数闸 fail-
        // closed -32603（rootKey 标准 base64 解码后恰 16/21B，RingRTC
        // CallLinkRootKey 两版本定长）；未注册账号 -32603；服务器/zkgroup 面
        // -1 CALL_CREDENTIALS_FAILED（data.status? 诚实透传）；网络面 -1
        // CALL_CREDENTIALS_UNAVAILABLE；fake 形状桩（非有效 zk 材料）。
        "calls.linkPresentation" => {
            let account = params.get("account").and_then(Value::as_str);
            let root_key = params
                .get("rootKey")
                .and_then(Value::as_str)
                .and_then(crate::service::call_links::decode_root_key);
            let (Some(account), Some(root_key)) = (account, root_key) else {
                return internal_error(id);
            };
            match service.call_link_presentation(account, &root_key).await {
                Ok(presentation) => jsonrpc::success_response(
                    id,
                    json!({ "presentation": calls_b64(&presentation) }),
                ),
                Err(error) => call_credential_error(id, &error),
            }
        }
        // calls.createLinkAuth（同批）：roomId = 标准 base64 解码后恰 32B
        //（RingRTC derive_room_id HKDF-SHA256 固定输出）；单次 RPC 内两段式
        //（create-auth POST → 兑换 → 呈现 + publicParams 双字节出）。
        "calls.createLinkAuth" => {
            let account = params.get("account").and_then(Value::as_str);
            let root_key = params
                .get("rootKey")
                .and_then(Value::as_str)
                .and_then(crate::service::call_links::decode_root_key);
            let room_id = params
                .get("roomId")
                .and_then(Value::as_str)
                .and_then(crate::service::call_links::decode_room_id);
            let (Some(account), Some(root_key), Some(room_id)) = (account, root_key, room_id)
            else {
                return internal_error(id);
            };
            match service.create_link_auth(account, &root_key, &room_id).await {
                Ok((presentation, public_params)) => jsonrpc::success_response(
                    id,
                    json!({
                        "presentation": calls_b64(&presentation),
                        "callLinkPublicParams": calls_b64(&public_params),
                    }),
                ),
                Err(error) => call_credential_error(id, &error),
            }
        }
        // calls.groupProof（同批）：groupId 32B master key 闸与 terminateGroup
        // 同款；proof = ExternalGroupCredential.token 字符串原样透出。
        "calls.groupProof" => {
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            match service.call_group_proof(account, master_key).await {
                Ok(proof) => jsonrpc::success_response(id, json!({ "proof": proof })),
                Err(error) => call_credential_error(id, &error),
            }
        }
        // calls.groupMembers（同批）：群成员 UuidCiphertext 材料清单（服务器
        // 群态成员序）；fake 空清单形状桩。
        "calls.groupMembers" => {
            let account = params.get("account").and_then(Value::as_str);
            let master_key = params
                .get("groupId")
                .and_then(Value::as_str)
                .and_then(crate::service::decode_group_id);
            let (Some(account), Some(master_key)) = (account, master_key) else {
                return internal_error(id);
            };
            match service.call_group_members(account, master_key).await {
                Ok(members) => jsonrpc::success_response(
                    id,
                    json!({
                        "members": members
                            .iter()
                            .map(|member| json!({
                                "uuid": member.uuid,
                                "userIdCipherText": calls_b64(&member.user_id_cipher_text),
                            }))
                            .collect::<Vec<_>>(),
                    }),
                ),
                Err(error) => call_credential_error(id, &error),
            }
        }
        // calls.endorsementsPublicKey（同批）：常量派生面，无 account
        //（getStickerPackManifest 匿名面同纪律）；不触网，fake 同码路真实答。
        "calls.endorsementsPublicKey" => jsonrpc::success_response(
            id,
            json!({ "endorsementPublicKey": calls_b64(&service.endorsements_public_key()) }),
        ),
        // storage.write（P0，KT 扩展方法）：通用批量 manifest item 写
        //（compat §3 storage.write 行 + §5「storage 写面 P0」块）。mutating
        // 语义不落调度日志（storage 状态面纪律）；参数闸 fail-closed
        // -32603（形状/白名单/重复 key 全拒）；版本冲突（服务器 409）答
        // STORAGE_VERSION_CONFLICT（retryable=false，不自动重试）；INSERT
        // 撞已存在 key 内容不同/UPDATE 换记录类型 → 意图冲突 -32603。
        "storage.write" => {
            let Ok((account, updates)) = storage_write::parse_request(params) else {
                return internal_error(id);
            };
            match service.storage_write(&account, updates).await {
                Ok(outcome) => jsonrpc::success_response(id, storage_write_payload(&outcome)),
                Err(storage_sync::StorageSyncError::UnknownAccount) => internal_error(id),
                Err(storage_sync::StorageSyncError::VersionConflict { server_version }) => {
                    jsonrpc::error_response(
                        id,
                        &storage_write::version_conflict_rpc(server_version),
                    )
                }
                Err(error) => storage_error(id, &error),
            }
        }
        // ---- 引擎自证面（KT 扩展方法，compat §3 引擎自证块）----
        // engine.protocolCapabilities：只读、零网络、账号无关（无 account
        // 参数，未绑定账号照常应答）——引擎向服务器声明的协议能力位 +
        // 依赖基线，上游强制过滤收紧时供运维/哨兵核对达标面（2026-03 上游
        // 无预告批量注销 capability 不达标客户端的教训）。真相源登记见
        // service::capabilities：链接面 live 复用 vendor 构造器，账号属性
        // 面钉定镜像（vendor 内联字面值无导出常量），镜像/基线漂移由单测
        // 扫 vendor 源码拦下。读面不落调度日志（listContacts 同纪律）；
        // 无业务错误面，fake/real 同码路真实答（calls.endorsementsPublicKey
        // 常量面同款）。
        "engine.protocolCapabilities" => {
            jsonrpc::success_response(id, capabilities::protocol_capabilities_payload())
        }
        // engine.deviceCapabilities（D5 设备面能力注册）：PUT
        // /v1/devices/capabilities（官方 registerCapabilities 同端点）并回
        // 读上传面。触网方法（fake 恒已注册，getUserStatus 同款）；仅 401
        // 判解绑（getUserStatus 解绑原文同款），其余失败折叠 -32603。幂等
        // 声明面：重复调用 = 服务器侧重复 PUT（官方每次启动同语义）。不落
        // 调度日志（engine.protocolCapabilities 同纪律）。
        "engine.deviceCapabilities" => {
            let account = params
                .get("account")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match service.register_device_capabilities(account).await {
                Ok(()) => jsonrpc::success_response(
                    id,
                    json!({
                        "account": account,
                        "registered": true,
                        "capabilities": capabilities::device_capabilities_upload_payload(),
                    }),
                ),
                Err(DeviceCapabilitiesError::Unbound) => {
                    jsonrpc::error_response(id, &RpcError::new(-32603, USER_UNBOUND_MESSAGE))
                }
                Err(DeviceCapabilitiesError::Internal) => internal_error(id),
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

/// 会话状态三 flag 读投影（getConversationState 应答 + setConversationState
/// 写后回显同形状；三键恒在——0/false = 位缺省，与 Storage record 位读出
/// 面一致）。
fn conversation_state_payload(state: &storage_sync::ConversationState) -> Value {
    json!({
        "mutedUntilTimestamp": state.muted_until_timestamp,
        "archived": state.archived,
        "markedUnread": state.marked_unread,
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

/// call link 投影 → wire（getCallLinks/setCallLink 返回形状；键缺省 =
/// null，贴纸包面同形）。
fn call_link_sync_payload(links: &[crate::service::storage_call_link::CallLinkSync]) -> Value {
    json!({
        "callLinks": links
            .iter()
            .map(|link| {
                json!({
                    "rootKey": standard_b64(&link.root_key),
                    "adminPasskey": link.admin_passkey.as_ref().map(|key| standard_b64(key)),
                    "deletedAtTimestampMs": link.deleted_at_timestamp_ms,
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

/// storage.write 返回体（compat §3 storage.write 行）：results 与输入序
/// 对齐；key/writtenKey 标准 base64（DELETE 无 writtenKey 键）；no-op 条目
/// 带 noop 标记。
fn storage_write_payload(outcome: &storage_write::WriteOutcome) -> Value {
    use base64::Engine as _;
    json!({
        "version": outcome.version,
        "results": outcome
            .results
            .iter()
            .map(|entry| {
                let mut item = serde_json::Map::new();
                item.insert("type".to_owned(), json!(entry.op.as_str()));
                item.insert(
                    "key".to_owned(),
                    json!(base64::engine::general_purpose::STANDARD.encode(&entry.key)),
                );
                match &entry.outcome {
                    // DELETE = Written 无 writtenKey（键缺省）；Noop 才带
                    // noop 标记。
                    storage_write::ItemOutcome::Written { written_key } => {
                        if let Some(written_key) = written_key {
                            item.insert(
                                "writtenKey".to_owned(),
                                json!(base64::engine::general_purpose::STANDARD.encode(written_key)),
                            );
                        }
                    }
                    storage_write::ItemOutcome::Noop => {
                        item.insert("noop".to_owned(), Value::Bool(true));
                    }
                }
                Value::Object(item)
            })
            .collect::<Vec<_>>(),
    })
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
        // storage.write 通用写面：意图冲突（INSERT 撞已存在 key 内容不同 /
        // UPDATE 换记录类型）→ -32603 fail-closed；VersionConflict 由
        // storage.write 调用点单独拦截渲染（带 data），此处仅为 match
        // exhaustive 兜底。
        storage_sync::StorageSyncError::IntentConflict => (-32603, "Internal error"),
        storage_sync::StorageSyncError::VersionConflict { .. } => (-1, "STORAGE_VERSION_CONFLICT"),
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

/// Polls 发送批内容闸（官方 PollCreateSchema 对位，types/Polls.dom.ts）：
/// question 非空 ≤200 字符 + 字节 ≤2048（官方 MAX_MESSAGE_BODY_BYTE_LENGTH
/// 同值）。长度计数 chars（合成序列较 graphemes 计数更大，超限更保守）。
fn poll_question_face(value: &Value) -> Option<String> {
    let question = value.as_str()?;
    let chars = question.chars().count();
    ((1..=200).contains(&chars) && question.len() <= 2048).then(|| question.to_owned())
}

/// poll options 闸：2..=10 条、每条非空字符串 ≤100 字符 + 字节 ≤2048。
fn poll_options_face(value: &Value) -> Option<Vec<String>> {
    let entries = value.as_array()?;
    if !(2..=10).contains(&entries.len()) {
        return None;
    }
    entries
        .iter()
        .map(|entry| {
            let option = entry.as_str()?;
            let chars = option.chars().count();
            ((1..=100).contains(&chars) && option.len() <= 2048).then(|| option.to_owned())
        })
        .collect()
}

/// poll optionIndexes 闸：每项 0..=9、≤10 条（官方 PollVoteSchema 同域；
/// 空数组 = 清票动作合法）。
fn poll_option_indexes_face(value: &Value) -> Option<Vec<u32>> {
    let entries = value.as_array()?;
    if entries.len() > 10 {
        return None;
    }
    entries
        .iter()
        .map(|entry| {
            entry
                .as_u64()
                .filter(|index| *index <= 9)
                .map(|index| index as u32)
        })
        .collect()
}

/// records/removeIds 双参批解析（chatFolders/notificationProfiles/
/// storyDistributionLists setRecords 共用，compat §3 各行错误面）：records
/// 条目逐条过 parse 闸；removeIds 条目过 id 闸（folder/story list=uuid 串 /
/// profile=32 位小写 hex 串，16B 实体 id 统一形态）；两参同缺或全空批
/// fail-closed；records 内重复 id / records∩removeIds 交集冲突 fail-closed。
/// 任一形状错 → None → 调用方 -32603。
fn parse_record_batch(
    params: &serde_json::Map<String, Value>,
    parse: fn(&Value) -> Option<crate::service::storage_records::RecordValue>,
    parse_id: fn(&Value) -> Option<Vec<u8>>,
) -> Option<(
    Vec<crate::service::storage_records::RecordValue>,
    Vec<Vec<u8>>,
)> {
    let records = match params.get("records") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items.iter().map(parse).collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    let remove_ids = match params.get("removeIds") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items.iter().map(parse_id).collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    // 两参至少一非空。
    if records.is_empty() && remove_ids.is_empty() {
        return None;
    }
    // 同批重复 id fail-closed。
    let mut seen = std::collections::HashSet::new();
    for record in &records {
        if !seen.insert(record.entity_id().to_vec()) {
            return None;
        }
    }
    // records∩removeIds 交集冲突 fail-closed。
    if records.iter().any(|record| {
        remove_ids
            .iter()
            .any(|id| id.as_slice() == record.entity_id())
    }) {
        return None;
    }
    Some((records, remove_ids))
}

fn internal_error(id: &Value) -> Value {
    jsonrpc::error_response(id, &RpcError::new(-32603, "Internal error"))
}

/// calls.* 材料字节 → 标准 base64（凭据/presentation 字节面统一编码点；
/// 字节内容零日志零落盘纪律不变，此处只编码出 RPC 响应）。
fn calls_b64(bytes: &[u8]) -> String {
    standard_b64(bytes)
}

/// 字节面 → 标准 base64（群邀请链接族 password/groupId 同编码点）。
fn standard_b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// calls.* 凭据材料面错误折叠（compat §3 calls 族行同表，P4c 切一）：
/// UnknownAccount/UnknownGroup → -32603（细节不外泄）；Failed → `-1` +
/// `CALL_CREDENTIALS_FAILED` + data.status 服务器状态码诚实透传；
/// Unavailable → `-1` + `CALL_CREDENTIALS_UNAVAILABLE`（retryable 语义）。
fn call_credential_error(
    id: &Value,
    error: &crate::service::call_links::CallCredentialError,
) -> Value {
    use crate::service::call_links::CallCredentialError as Error;
    match error {
        Error::UnknownAccount | Error::UnknownGroup => internal_error(id),
        Error::Failed { status } => {
            let mut data = serde_json::Map::new();
            if let Some(status) = status {
                data.insert("status".to_owned(), json!(status));
            }
            jsonrpc::error_response(
                id,
                &jsonrpc::RpcError::with_data(-1, "CALL_CREDENTIALS_FAILED", Value::Object(data)),
            )
        }
        Error::Unavailable => {
            jsonrpc::error_response(id, &RpcError::new(-1, "CALL_CREDENTIALS_UNAVAILABLE"))
        }
    }
}

/// 设备面错误 → JSON-RPC 码（getDevices/unlinkDevice 同族）：账号参数
/// 不指向已链接账号 -32603（参数面不占用结构化码）；服务器拒绝
/// `DEVICES_FAILED`（-1 + message，data.status? = HTTP 状态码诚实透传）；
/// 传输面 `DEVICES_UNAVAILABLE`（-1 + message）。
fn device_error(id: &Value, error: &crate::service::devices::DevicesError) -> Value {
    use crate::service::devices::DevicesError as Error;
    match error {
        Error::UnknownAccount => internal_error(id),
        Error::Failed { status } => {
            let mut data = serde_json::Map::new();
            if let Some(status) = status {
                data.insert("status".to_owned(), json!(status));
            }
            jsonrpc::error_response(
                id,
                &jsonrpc::RpcError::with_data(-1, "DEVICES_FAILED", Value::Object(data)),
            )
        }
        Error::Unavailable => {
            jsonrpc::error_response(id, &RpcError::new(-1, "DEVICES_UNAVAILABLE"))
        }
    }
}

/// 用户名写面错误 → JSON-RPC 错误码：参数面（语法闸/hash 失配）-32603；
/// 服务器态四码 `-1` + 结构化码位（connector 白名单逐字对位）：reserve 409
/// 全占 / confirm 410 已占 = `USERNAME_TAKEN`（同输入确定性失败）；confirm
/// 409 无预约 = `USERNAME_RESERVATION_GONE`（调用方重走 reserve）；413/429 =
/// `USERNAME_RATE_LIMITED`（可重试）；其余 HTTP 与网络 = `USERNAME_SERVER_
/// FAILED`；storage 链路复用 STORAGE 族四码。
fn username_write_error(id: &Value, error: &crate::service::username::UsernameWriteError) -> Value {
    use crate::service::username::UsernameWriteError as Error;
    let (code, message) = match error {
        Error::BadParams => return internal_error(id),
        Error::Taken => (-1, "USERNAME_TAKEN"),
        Error::ReservationGone => (-1, "USERNAME_RESERVATION_GONE"),
        Error::RateLimited => (-1, "USERNAME_RATE_LIMITED"),
        Error::ServerFailed => (-1, "USERNAME_SERVER_FAILED"),
        Error::Storage(storage) => {
            return storage_error(id, storage);
        }
    };
    jsonrpc::error_response(id, &RpcError::new(code, message))
}

/// send 系错误 → JSON-RPC 错误码：目标群已终止（官方 shouldSendToConversation
/// terminated 位硬拒同语义）→ `-1` + `SEND_TARGET_TERMINATED`；服务端凭据
/// 拒绝（401 解绑，确定性失败不重试）→ `-32603` + getUserStatus 同款解绑
/// 原文（同一常量，connector 分类器按 message 直接命中）；限流（429/413）/
/// 人机验证（428）/服务端 5xx 不可用 → `-32603` + 对应分类原文（锚词括注
/// 异常类名，connector 分类器命中；5xx 建议可重试，限流/验证不自动重试
/// ——决策在 connector/desktop，compat §3/§5）；其余（账号不可达/内部
/// 失败）→ -32603 细节不外泄。
fn send_error(id: &Value, error: crate::service::SendError) -> Value {
    let (code, message) = send_error_face(&error);
    jsonrpc::error_response(id, &RpcError::new(code, message))
}

/// SendError → (code, message) 分类单点：send 的整批错误面（send_error）
/// 与 per-recipient 失败条目（send failures additive 面）共用，同一错误
/// 两处分类逐字一致（connector 分类器按 message 命中，形状即识别面）。
pub(crate) fn send_error_face(error: &crate::service::SendError) -> (i64, &'static str) {
    use crate::service::SendError;
    match error {
        SendError::TargetTerminated => (-1, "SEND_TARGET_TERMINATED"),
        SendError::AnnouncementsOnly => (-1, "SEND_TARGET_ANNOUNCEMENTS_ONLY"),
        SendError::Unbound => (-32603, USER_UNBOUND_MESSAGE),
        SendError::RateLimited => (-32603, RATE_LIMITED_MESSAGE),
        SendError::ProofRequired => (-32603, PROOF_REQUIRED_MESSAGE),
        SendError::ServerUnavailable => (-32603, SERVER_UNAVAILABLE_MESSAGE),
        SendError::IdentityUnverified => (-32603, IDENTITY_UNVERIFIED_MESSAGE),
        SendError::UnknownAccount | SendError::Internal => (-32603, "Internal error"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// sendError 解绑臂形状面：`-32603` + getUserStatus 同款解绑原文
    /// （单一常量，不发明新字符串）。connector 分类器
    /// （upstream_error_is_unauthorized）按 error.message 含
    /// "AuthorizationFailedException" / "Authorization failed" 命中并驱动
    /// 解绑事件链——常量形状即识别面，改动即断。
    #[test]
    fn send_error_unbound_face_matches_user_status_face() {
        let response = send_error(&Value::from(7), crate::service::SendError::Unbound);
        assert_eq!(response.get("id").and_then(Value::as_i64), Some(7));
        let error = response.get("error").expect("error face");
        assert_eq!(error.get("code").and_then(Value::as_i64), Some(-32603));
        assert_eq!(
            error.get("message").and_then(Value::as_str),
            Some(USER_UNBOUND_MESSAGE)
        );
        assert!(USER_UNBOUND_MESSAGE.contains("AuthorizationFailedException"));
        assert!(USER_UNBOUND_MESSAGE.contains("Authorization failed"));
    }

    /// send failures additive 面分类一致性：per-recipient 失败条目的
    /// (code, message) 与整批错误面（send_error）逐字同源（send_error_face
    /// 单点）——connector 分类器按 message 命中，两处漂移即断。
    #[test]
    fn send_error_face_matches_whole_batch_error_face() {
        use crate::service::SendError;
        let cases = [
            SendError::TargetTerminated,
            SendError::AnnouncementsOnly,
            SendError::Unbound,
            SendError::RateLimited,
            SendError::ProofRequired,
            SendError::ServerUnavailable,
            SendError::IdentityUnverified,
            SendError::Internal,
        ];
        for error in cases {
            let (code, message) = send_error_face(&error);
            let response = send_error(&Value::from(1), error);
            let face = response.get("error").expect("error face");
            assert_eq!(face.get("code").and_then(Value::as_i64), Some(code));
            assert_eq!(face.get("message").and_then(Value::as_str), Some(message));
        }
    }

    /// 发送链结构化三面（限流/人机验证/服务端不可用）wire 形状：均
    /// `-32603` + 常量逐字 + 异常类名锚词在场（connector 分类器按锚词
    /// 命中分类——message 形状即契约，改动即断）。
    #[test]
    fn send_error_structured_faces_wire_shape() {
        let cases = [
            (
                crate::service::SendError::RateLimited,
                RATE_LIMITED_MESSAGE,
                "(RateLimitExceeded)",
            ),
            (
                crate::service::SendError::ProofRequired,
                PROOF_REQUIRED_MESSAGE,
                "(ProofRequiredException)",
            ),
            (
                crate::service::SendError::ServerUnavailable,
                SERVER_UNAVAILABLE_MESSAGE,
                "(ServerUnavailableException)",
            ),
            (
                crate::service::SendError::IdentityUnverified,
                IDENTITY_UNVERIFIED_MESSAGE,
                "(UntrustedIdentityException)",
            ),
        ];
        for (error, message, anchor) in cases {
            let response = send_error(&Value::from(9), error);
            assert_eq!(response.get("id").and_then(Value::as_i64), Some(9));
            let face = response.get("error").expect("error face");
            assert_eq!(face.get("code").and_then(Value::as_i64), Some(-32603));
            assert_eq!(face.get("message").and_then(Value::as_str), Some(message));
            assert!(message.contains(anchor));
        }
    }
}
