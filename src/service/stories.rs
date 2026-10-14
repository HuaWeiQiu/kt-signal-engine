// SPDX-License-Identifier: AGPL-3.0-only

//! Stories 发送面（E1，KT 扩展批 2026-10-08）：text story 构造 + 逐收件人
//! 分发 + best-effort 自同步。
//!
//! 官方行为参照（Signal-Desktop，行为对齐不抄码）：`ts/jobs/helpers/
//! sendStory.preload.ts`——textAttachment 构造（text/textStyle/preview 面
//! 随批 CDN 上行，preview.url/preview.title 引擎扩展键 2026-10-09 批）、
//! 跨名单按 conversation 去重（`recipientsByServiceId`
//! Map 保首现序）、自同步仅在 ≥1 收件人成功且有其他设备时发送
//! （`storyMessageRecipients` 空 / 单设备即跳过）；story 恒走 sealed sender
//! 通道（fork 已接线，见 vendor/libsignal-service
//! `MessageSender::send_story_message` 与 websocket/sender.rs
//! `send_messages_unidentified_story`，官方锚点登记于 vendor/README.md）。
//!
//! 媒体 story 发送面（2026-10-14 批，官方对齐 Signal-Desktop abe80d3，
//! wire additive）：`attachment` 可选参数扩文件 story——官方
//! `StoryMessage.attachment` 本就 `fileAttachment=3 | textAttachment=4`
//! oneof 双臂（protos/SignalService.proto:476-479），媒体腿 =
//! `sendStory.preload.ts:186-190` 同构：正文附件走既有 transit CDN 上行链
//! （presage `upload_attachments` 官方原语），上传返回指针原样进
//! `StoryMessage.fileAttachment`（官方 `getStoryMessage`
//! SendMessage.preload.ts:1043-1044 同位）；与文本腿两腿恰一（oneof 恰一
//! 同构）。**边界翻案记档**：E1 批原定「文件 story v1 不做」——产品需要
//! 媒体 story 发布（桌面 S6 批前置），且接收投影（E2 批）早已投
//! fileAttachment 全脸（locator 指针族），收发不对称只剩发送面缺口；
//! 上传链/自同步面全部既有复用、vendored proto 与 fork 发送路径零改动
//! 即可承载（fork `send_story_message` 本就 `impl Into<ContentBody>`
//! 泛型），故本批翻案落地，翻案理由随 compat §5 媒体 story 批记档。
//! 群 story 发送面（2026-10-15 批，官方对齐 Signal-Desktop abe80d3，
//! wire additive）：`group` 可选参数扩群受众腿——E1 批「群 story
//! （GroupContextV2 位）v1 不做」边界翻案（理由记档 compat §5 群 story
//! 批）。官方发送面 = `ts/util/sendStoryMessage.preload.ts:40-63` 受众
//! 双域（名单 listIds ∧ 群 conversationIds，同一 send 可共存）+ 群候选
//! 三闸（isGroupV2 :210 / 非 terminated :218 / announcementsOnly 需
//! admin :226-232）；线格式 = `Content.storyMessage`（proto
//! SignalService.proto:115）内 `StoryMessage.group = GroupContextV2`
//!（:474，`{masterKey=1, revision=2, groupChange=3}` :947-951），群定位
//! 键 = masterKey（32B，群 id 由收方派生，processDataMessage.preload.ts
//! :149-169 deriveGroupFields 同语义）+ revision（官方
//! `conversation.getGroupV2Info()` conversations.preload.ts:1412-1434
//! 本地群态同源；引擎 = 群态取面缓存优先 miss 才 fetch，group_mrm R1
//! 同构）；`groupChange` 官方 story 发送恒不带（getStoryMessage
//! SendMessage.preload.ts:1054-1060 `groupChange ?? null`，无传入源）。
//! 群腿 `allowsReplies` 线上恒 true（sendStory.preload.ts:199 硬编码 +
//! :352-354 `isGroupV2 ||` 短路；RPC allowsReplies 参数只辖名单腿）。
//! 分发面 = 逐成员 story 通道（fork `Manager::send_story_message`
//! sealed-sender `?story=true`）——官方主路 sender-key（story=true 不用
//! GSE，sendToGroup.preload.ts:362 `groupId != null && !story`）+
//! 逐收件人合法 fallback（:250-263 sendGroupProto）；引擎与名单腿同一
//! 传输纪律（E1 批既有偏差族，§5 记档）。自同步 = 官方群 story sync
//! 形状：`Sent{timestamp, story_message（载 group）,
//! story_message_recipients: 成员×{distributionListIds: [],
//! isAllowedToReply: true}}` 且 `destination_service_id` 缺席
//!（sendStory.preload.ts:587-589 群发送两 destination 位 undefined；
//! 空名单位 + 恒可回位 = addDistributionListToServiceIdSent listId=undefined
//! + 群 sendState isAllowedToReplyToStory:true 同构）。
//!
//! 边界（任务口径）：distribution list 成员解析归 connector（引擎只收显式
//! recipients）；群 story 成员解析 = 引擎群态（sendTextGroup 同源）；
//! 24h 过期纯客户端语义不在引擎面。
//!
//! 媒体 story caption（2026-10-15 批，官方对齐 Signal-Desktop abe80d3，
//! wire additive）：`caption` 可选参数扩媒体腿——官方 caption 进
//! `AttachmentPointer.caption = 11`（protos/SignalService.proto:940，
//! uploadAttachment.preload.ts:99/:134 `caption || null` 随上传返回指针同
//! 载），媒体编辑面输入帽 `MAX_BODY_ATTACHMENT_BYTE_LENGTH = 64 KiB`
//!（longAttachment.std.ts:8，CompositionInput.dom.tsx:686-692 字节面裁
//! 决）。caption 媒体腿专属（官方 textAttachment 无 caption 位）；上行
//! 路径 = presage `AttachmentSpec.caption` 上行面直通返回指针（vendored
//! sender.rs spec.caption→pointer.caption，零 vendor/fork 改动）——
//! S6a 桌面媒体 story 批记档「引擎 media 腿无 caption 键」边界就此翻案
//!（compat §5 媒体 story caption 批）。
//!
//! 纪律（compat §1）：本层不产生任何 stderr 输出。

use std::collections::HashSet;
use std::sync::Arc;

use presage::libsignal_service::content::ContentBody;
use presage::libsignal_service::prelude::Uuid;
use presage::libsignal_service::protocol::{Aci, ServiceId};
use presage::proto::{
    data_message, story_message, sync_message, text_attachment, BodyRange, DataMessage,
    GroupContextV2, Preview, StoryMessage, SyncMessage, TextAttachment,
};
use serde_json::{json, Value};

use super::{
    lock_registered_for_send, open_registered, resolve_recipient, send_error_of,
    upload_attachments_for_send, SendError, SendTextStyle, Service,
};

/// 官方 MY_STORY_ID（`ts/types/Stories.std.ts:114-115`）：distributionListId
/// 缺省值——本地「我的故事」名单的钉定 id。
pub(crate) const MY_STORY_ID: &str = "00000000-0000-0000-0000-000000000000";

/// story 正文样式词表（textAttachment.textStyle，proto `TextAttachment.Style`
/// 枚举词表同构）：六值，DEFAULT = 缺省 → proto 字段省略（线上 absent 枚举
/// 即 DEFAULT=0 官方同款）。返回线上 i32（prost enumeration 直传形状）；
/// 词表外 Err 调用方 fail-closed。
pub(crate) fn parse_story_text_style(word: &str) -> Result<Option<i32>, ()> {
    match text_attachment::Style::from_str_name(word) {
        Some(text_attachment::Style::Default) => Ok(None),
        Some(style) => Ok(Some(style as i32)),
        None => Err(()),
    }
}

/// 发送面 bodyRanges → proto BodyRange.Style：五值白名单同构映射（send
/// textStyle 透传批同款服务层防御——解析闸之后 NONE 与词表外一并
/// fail-closed，不静默丢样式）。
fn build_body_ranges(styles: &[SendTextStyle]) -> Result<Vec<BodyRange>, SendError> {
    let mut ranges = Vec::with_capacity(styles.len());
    for style in styles {
        let value = match presage::proto::body_range::Style::from_str_name(&style.style) {
            Some(
                value @ (presage::proto::body_range::Style::Bold
                | presage::proto::body_range::Style::Italic
                | presage::proto::body_range::Style::Spoiler
                | presage::proto::body_range::Style::Strikethrough
                | presage::proto::body_range::Style::Monospace),
            ) => value,
            _ => return Err(SendError::Internal),
        };
        ranges.push(BodyRange {
            start: Some(style.start),
            length: Some(style.length),
            associated_value: Some(presage::proto::body_range::AssociatedValue::Style(
                value as i32,
            )),
        });
    }
    Ok(ranges)
}

/// text story 构造（纯函数，单测钉形状）：
/// - `profile_key` 构造面 None——由 presage fork `send_story_message` 注入
///   发送者 profile key（与 presage `send_message` 注入 DataMessage 同构；
///   官方 sendStory profileKey 必需）；
/// - `attachment` 恒 TextAttachment（文件 story 走 build_media_story_message
///   另一构造面，两腿互斥）；
/// - preview = 三件套组装（见 build_story_preview）：preview.image = 调用
///   方已上行的 CDN 指针（官方 sendStory 同构：preview 数据随正文附件同批
///   上行），url/title = previewUrl/previewTitle 扩展键如实透传（解析闸
///   在 methods 参数面：https-only ≤2048 字符 / 非空 ≤1024 UTF-8 字节）；
/// - `allows_replies` 透传（官方值源 = distribution list 属性，缺省线上
///   false；connector 从名单记录展开后显式传入）；
/// - `group` 透传（群 story 腿传 Some(GroupContextV2)，官方 getStoryMessage
///   group 位同构；名单腿传 None）。群腿 allows_replies 恒 true 由编排面
///   传参裁定（官方 sendStory.preload.ts:199 同位），构造面不特判。
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_story_message(
    text: &str,
    text_style: Option<i32>,
    allows_replies: bool,
    body_ranges: &[SendTextStyle],
    preview_image: Option<presage::proto::AttachmentPointer>,
    preview_url: Option<String>,
    preview_title: Option<String>,
    preview_date: Option<u64>,
    group: Option<GroupContextV2>,
) -> Result<StoryMessage, SendError> {
    Ok(StoryMessage {
        profile_key: None,
        group,
        allows_replies: Some(allows_replies),
        body_ranges: build_body_ranges(body_ranges)?,
        attachment: Some(story_message::Attachment::TextAttachment(TextAttachment {
            text: Some(text.to_owned()),
            text_style,
            text_foreground_color: None,
            text_background_color: None,
            preview: build_story_preview(preview_image, preview_url, preview_title, preview_date),
            background: None,
        })),
    })
}

/// 媒体 story 构造（2026-10-14 批纯函数，单测钉形状）：官方 getStoryMessage
/// fileAttachment 臂同位（SendMessage.preload.ts:1043-1044
/// `attachment = { fileAttachment }`）——上传返回指针原样进
/// `StoryMessage.fileAttachment`，bodyRanges 同臂照载（官方 :1059-1061
/// fileAttachment 分支同构）；`profile_key` 构造面 None（send 面注入，与
/// text 臂同构）、`group` 透传（群 story 腿 Some，2026-10-15 批）。
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_media_story_message(
    allows_replies: bool,
    body_ranges: &[SendTextStyle],
    file_attachment: presage::proto::AttachmentPointer,
    group: Option<GroupContextV2>,
) -> Result<StoryMessage, SendError> {
    Ok(StoryMessage {
        profile_key: None,
        group,
        allows_replies: Some(allows_replies),
        body_ranges: build_body_ranges(body_ranges)?,
        attachment: Some(story_message::Attachment::FileAttachment(file_attachment)),
    })
}

/// 群 story 群上下文构造（2026-10-15 批纯函数，单测钉形状）：官方
/// getStoryMessage group 位同构（SendMessage.preload.ts:1054-1060）——
/// `{masterKey, revision, groupChange: null}`，proto GroupContextV2
/// `{masterKey=1, revision=2, groupChange=3}`
///（Signal-Desktop protos/SignalService.proto:947-951）；groupChange 官方
/// story 发送恒缺（无传入源），fail-closed 不发明。
pub(crate) fn build_story_group_context(master_key: Vec<u8>, revision: u32) -> GroupContextV2 {
    GroupContextV2 {
        master_key: Some(master_key),
        revision: Some(revision),
        group_change: None,
    }
}

/// 群 story 自同步扇出条目构造（2026-10-15 批纯函数，单测钉形状）：官方
/// 群 story sync 同构——每成员一条
/// `{destinationServiceId, distributionListIds: [], isAllowedToReply:
/// true}`（sendStory.preload.ts addDistributionListToServiceIdSent
/// 群臂 listId=undefined → 空名单位；群 sendState
/// isAllowedToReplyToStory:true → 恒可回位）；空成员批 = 空 vec（官方
/// storyMessageRecipients 空 → 整条 sync 不发，闸在编排面）。
pub(crate) fn group_story_sync_recipients(
    members: &[ServiceId],
) -> Vec<sync_message::sent::StoryMessageRecipient> {
    members
        .iter()
        .map(|service_id| sync_message::sent::StoryMessageRecipient {
            destination_service_id: Some(service_id.service_id_string()),
            distribution_list_ids: Vec::new(),
            is_allowed_to_reply: Some(true),
            destination_service_id_binary: None,
        })
        .collect()
}

/// story preview 组装（纯函数，单测钉形状）：image/url/title/date 四件任一
/// 在场即成 Preview（官方 sendStory preview 面 image 可空同构——
/// `hydratedPreview.image && uploadAttachment`，url/title 独立席位）；全缺
/// = None（线上缺席位，现状形态）。date = P14 Preview.date 两端批（链接
/// 预览发布时刻毫秒，>0 语义，官方 sendStory hydratedPreview 同位透传）。
/// description 官方 story 预览面无源，恒缺席。
fn build_story_preview(
    image: Option<presage::proto::AttachmentPointer>,
    url: Option<String>,
    title: Option<String>,
    date: Option<u64>,
) -> Option<Preview> {
    if image.is_none() && url.is_none() && title.is_none() && date.is_none() {
        return None;
    }
    Some(Preview {
        url,
        title,
        image,
        description: None,
        date,
    })
}

/// 收件人解析去重（保首现序，纯函数，单测钉语义）：官方 sendStory 跨名单
/// 去重同语义——同一 conversation 只发一次；键 = 解析后 ServiceId（号码与
/// UUID 双形态收敛到同一会话）。
fn dedup_resolved(recipients: Vec<(ServiceId, String)>) -> Vec<(ServiceId, String)> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(recipients.len());
    for (service_id, number) in recipients {
        if seen.insert(service_id) {
            out.push((service_id, number));
        }
    }
    out
}

/// sendStory 编排（compat §3 sendStory 行）：解析→去重→单时间戳逐收件人
/// 分发→best-effort 自同步。任一收件人发送失败整体 `-32603`（per-recipient
/// 部分失败形状未对齐，send 系既有纪律 §5）；解析面先全量完成再触网，
/// 任何号码不可解析 = 无上行直接内部失败。
///
/// `attachment`（媒体 story 发送面，2026-10-14 批）：Some = 文件 story——
/// data URI 走既有 transit CDN 上行链（entries 单条、无预览图），上传返回
/// 指针进 `StoryMessage.fileAttachment`（官方 sendStory.preload.ts:186-190
/// 同构）；缺席 = text story 现状零变化。两腿互斥闸在 methods 参数面。
/// `caption`（媒体 story caption 批，2026-10-15）：媒体腿专属——随正文
/// 附件 spec 上行直通返回指针（apply_outgoing_caption，官方
/// uploadAttachment caption 进指针同构），text 腿恒 None（官方
/// textAttachment 无 caption 位）。
/// 本函数 = 名单受众腿（2026-10-15 批拆分）：群受众腿独立编排于
/// `send_story_group`（混合受众官方 = 两 job 两 sync，sendStoryMessage
/// .preload.ts:327-332 名单 job / :337-378 群 job 各自带收尾 sync——引擎
/// 同构先后两腿，时间戳名单腿 ts、群腿 ts+1 官方 +index+1 同款），由
/// Service::send_story 编排调用。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_story(
    service: &Arc<Service>,
    account: &str,
    recipients: &[String],
    distribution_list_id: &str,
    allows_replies: bool,
    text: &str,
    text_style: Option<i32>,
    body_ranges: &[SendTextStyle],
    attachment: Option<&str>,
    caption: Option<&str>,
    preview_image: Option<&str>,
    preview_url: Option<String>,
    preview_title: Option<String>,
    preview_date: Option<u64>,
) -> Result<(u64, Vec<Value>), SendError> {
    if !service.real {
        return Ok((super::now_ms(), Vec::new()));
    }
    let shared = open_registered(service, account)
        .await
        .ok_or(SendError::UnknownAccount)?;
    let mut manager = lock_registered_for_send(&shared).await;
    let timestamp = super::now_ms();

    // 上传面：媒体腿 = 正文附件单条（官方正文附件随批 CDN 上行同构）；
    // 文本腿 = previewImage 随批上行（官方 preview.image 同批 CDN 上行
    // 同构）。两腿互斥（methods 参数闸），fail-closed 整批早退。
    let story = match attachment {
        Some(uri) => {
            let (pointers, _) =
                upload_attachments_for_send(&manager, &[uri.to_owned()], None, caption).await?;
            let file_pointer = pointers.into_iter().next().ok_or(SendError::Internal)?;
            build_media_story_message(allows_replies, body_ranges, file_pointer, None)?
        }
        None => {
            let (_, preview_pointer) = match preview_image {
                Some(uri) => upload_attachments_for_send(&manager, &[], Some(uri), None).await?,
                None => (Vec::new(), None),
            };
            build_story_message(
                text,
                text_style,
                allows_replies,
                body_ranges,
                preview_pointer,
                preview_url,
                preview_title,
                preview_date,
                None,
            )?
        }
    };

    // 解析全量前置（不可解析 = 无上行整拒），随后按解析序去重。
    let mut resolved = Vec::with_capacity(recipients.len());
    for number in recipients {
        let service_id = resolve_recipient(&manager, number)
            .await
            .ok_or(SendError::Internal)?;
        resolved.push((service_id, number.clone()));
    }
    let resolved = dedup_resolved(resolved);

    let mut results = Vec::with_capacity(resolved.len());
    for (recipient, number) in &resolved {
        manager
            .send_story_message(
                *recipient,
                ContentBody::StoryMessage(story.clone()),
                timestamp,
                false,
            )
            .await
            .map_err(send_error_of)?;
        results.push(json!({
            "recipientNumber": number,
            "success": { "timestamp": timestamp },
        }));
    }

    // 自同步（官方 sendStory 收尾同构）：≥1 收件人成功才发；best-effort
    // ——单设备无下游（NoMessagesToSend）与其他设备同步失败都不回滚已
    // 成功的 story 上行，官方「同步失败不视为发送失败」同口径。
    if !results.is_empty() {
        let own_aci = manager.registration_data().service_ids.aci();
        let sent = sync_message::Sent {
            destination_service_id: Some(own_aci.service_id_string()),
            timestamp: Some(timestamp),
            story_message: Some(story),
            story_message_recipients: resolved
                .iter()
                .map(
                    |(service_id, _)| sync_message::sent::StoryMessageRecipient {
                        destination_service_id: Some(service_id.service_id_string()),
                        distribution_list_ids: vec![distribution_list_id.to_owned()],
                        is_allowed_to_reply: Some(allows_replies),
                        destination_service_id_binary: None,
                    },
                )
                .collect(),
            ..Default::default()
        };
        let sync = SyncMessage {
            content: Some(sync_message::Content::Sent(sent)),
            ..Default::default()
        };
        let _ = manager
            .send_message(own_aci, ContentBody::SynchronizeMessage(sync), timestamp)
            .await;
    }

    Ok((timestamp, results))
}

/// sendStory 群受众腿（2026-10-15 批，compat §3 sendStory 行 + §5 群 story
/// 批）：官方群 story 发送行为对齐——
///
/// - **受众与闸面**（官方 sendStoryMessage.preload.ts:198-251 群候选三闸
///   同序镜像）：群 id fail-closed 解码（32B master key，decode_group_id
///   形状闸）→ terminated 硬拒（官方 :218-224 `terminated` 跳过位；引擎
///   群发送同款 `send_target_terminated` 缓存化闸）→ 群态取面（缓存优先
///   miss 才 fetch，group_mrm R1 同构；取不到 = 官方无群态发送面等价失败
///   fail-closed）→ announcementsOnly 需 admin（官方 :226-232；引擎群发送
///   同款 AnnouncementsOnly 错误面——官方 story 面按「跳过该群」降级，
///   引擎单群参数面无剩余受众可降，fail-closed 上抛不静默吞）。
/// - **线格式**：`StoryMessage.group = GroupContextV2{masterKey, revision,
///   groupChange: None}`（build_story_group_context）；revision = 群态
///   revision（官方 `conversation.getGroupV2Info()` 本地群态同源）。
/// - **分发面**：逐成员（群态 members 去自身，排序去重确定序）story 通道
///   （fork `Manager::send_story_message` sealed-sender `?story=true`）。
///   官方主路 = sender-key（story=true 不用 GSE，sendToGroup.preload.ts:
///   362 `groupId != null && !story`）+ 逐收件人 fallback（:250-263）；
///   引擎走 fallback 同构传输面，与名单腿同一 story 通道纪律（§5 记档）。
///   任一成员失败整体 `-32603`（sendStory 既有纪律）。
/// - **allowsReplies**：群腿线上恒 true（官方 sendStory.preload.ts:199
///   硬编码 + :352-354 `isGroupV2 ||` 短路；RPC allowsReplies 只辖名单腿）。
/// - `caption`（媒体 story caption 批，2026-10-15）：媒体腿专属，随正文
///   附件 spec 上行直通返回指针（名单腿同构；官方群 story media 臂同源
///   getStoryMessage），text 腿恒 None。
/// - **自同步**（官方群 story sync 形状，恰好一次）：≥1 成员发送成功才发
///   `SyncMessage.Sent{timestamp, story_message（载 group）,
///   story_message_recipients = group_story_sync_recipients（空名单位 +
///   恒可回位），destination_service_id 缺席}`（官方群发送两 destination
///   位 undefined，sendStory.preload.ts:587-589）；best-effort——同步失败
///   不回滚已成功上行（名单腿同口径）；零成员（ solo 群）= 零发送零
///   sync（官方 storyMessageRecipients 空 → 不发 sync 同构），成功形状
///   返回。
///
/// `timestamp` 由调用方给定：群单腿 = now_ms()，混合受众 = 名单腿 ts+1
/// （官方 groupTimestamp = timestamp + index + 1 同款保跨腿唯一）。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_story_group(
    service: &Arc<Service>,
    account: &str,
    group_id: &str,
    timestamp: u64,
    text: &str,
    text_style: Option<i32>,
    body_ranges: &[SendTextStyle],
    attachment: Option<&str>,
    caption: Option<&str>,
    preview_image: Option<&str>,
    preview_url: Option<String>,
    preview_title: Option<String>,
    preview_date: Option<u64>,
) -> Result<(u64, Value), SendError> {
    let group_face = json!({ "groupId": group_id, "timestamp": timestamp });
    if !service.real {
        return Ok((timestamp, group_face));
    }
    let shared = open_registered(service, account)
        .await
        .ok_or(SendError::UnknownAccount)?;
    let mut manager = lock_registered_for_send(&shared).await;

    // master key 形状闸（32B）：群 id 即 master key 标准 base64（引擎群面
    // 统一口径），decode_group_id 已含长度闸——非 32B = 内部失败，不发明
    // 群上下文。
    let master_key = super::decode_group_id(group_id).ok_or(SendError::Internal)?;
    // terminated 硬拒（官方群候选闸二；缓存化闸群发送同款零稳态探针）。
    if service.send_target_terminated(account, group_id).await {
        return Err(SendError::TargetTerminated);
    }
    // 群态取面：本地缓存（store 群态 + kv GSE 双校验）优先，miss 才 fetch
    // 并落缓存（group_mrm R1 同构；fetch 失败上抛不降级）。取不到群态 =
    // 官方无群态发送面等价失败（fail-closed，revision 无源不发明）。
    let fetched =
        match super::group_cache::cached_group_with_endorsements(&mut manager, &master_key).await {
            Some(hit) => Some(hit),
            None => {
                let fresh = manager
                    .fetch_group_with_endorsements(&master_key)
                    .await
                    .map_err(send_error_of)?;
                super::group_cache::store_endorsements(manager.store(), &master_key, &fresh).await;
                fresh
            }
        };
    let Some((group, _)) = fetched else {
        return Err(SendError::Internal);
    };
    // announcementsOnly 闸（官方群候选闸三）：发送方非 admin 拒发（引擎群
    // 发送同款错误面；官方 story 面按「跳过该群」降级，单群参数面无受众
    // 可降，fail-closed 上抛——§5 记档）。
    let own_aci = manager.registration_data().service_ids.aci();
    let own_uuid: Uuid = own_aci.into();
    let own_protocol_aci = Aci::from(own_uuid);
    let own_service_id = ServiceId::from(own_protocol_aci);
    if group.announcements_only
        && !presage::manager::group_member_is_admin(&group, &own_protocol_aci)
    {
        return Err(SendError::AnnouncementsOnly);
    }

    // 成员面：群态 members 去自身，排序去重（确定序，group_mrm 同构）。
    let mut targets: Vec<ServiceId> = group
        .members
        .iter()
        .map(|member| ServiceId::from(member.aci))
        .filter(|service_id| *service_id != own_service_id)
        .collect();
    targets.sort();
    targets.dedup();

    // 载荷构造（两内容腿与名单腿同构正交；群上下文进 StoryMessage.group，
    // 群腿 allowsReplies 恒 true 官方同位）。
    let story = match attachment {
        Some(uri) => {
            let (pointers, _) =
                upload_attachments_for_send(&manager, &[uri.to_owned()], None, caption).await?;
            let file_pointer = pointers.into_iter().next().ok_or(SendError::Internal)?;
            build_media_story_message(
                true,
                body_ranges,
                file_pointer,
                Some(build_story_group_context(master_key, group.revision)),
            )?
        }
        None => {
            let (_, preview_pointer) = match preview_image {
                Some(uri) => upload_attachments_for_send(&manager, &[], Some(uri), None).await?,
                None => (Vec::new(), None),
            };
            build_story_message(
                text,
                text_style,
                true,
                body_ranges,
                preview_pointer,
                preview_url,
                preview_title,
                preview_date,
                Some(build_story_group_context(master_key, group.revision)),
            )?
        }
    };

    // 逐成员 story 通道分发（任一失败整体失败，sendStory 既有纪律）。
    for target in &targets {
        manager
            .send_story_message(
                *target,
                ContentBody::StoryMessage(story.clone()),
                timestamp,
                false,
            )
            .await
            .map_err(send_error_of)?;
    }

    // 自同步（官方群 story sync 形状，恰好一次，零成员不发）：≥1 成员
    // 成功才发；destination_service_id 缺席（官方群发送同位）；best-effort
    // 不回滚。
    if !targets.is_empty() {
        let sent = sync_message::Sent {
            timestamp: Some(timestamp),
            story_message: Some(story),
            story_message_recipients: group_story_sync_recipients(&targets),
            ..Default::default()
        };
        let sync = SyncMessage {
            content: Some(sync_message::Content::Sent(sent)),
            ..Default::default()
        };
        let _ = manager
            .send_message(
                own_service_id,
                ContentBody::SynchronizeMessage(sync),
                timestamp,
            )
            .await;
    }

    Ok((timestamp, group_face))
}

/// story 删除扇出条目（S5，wire `SyncMessage.Sent.storyMessageRecipients`
/// 条目展开形态；connector 从名单记录展开后显式传入）。
pub(crate) struct SendStoryRecipientUpdate {
    pub destination: String,
    pub distribution_list_ids: Vec<String>,
    pub is_allowed_to_reply: bool,
}

/// story 删除 DOE 腿构造（S5 纯函数，单测钉形状）：官方名单 story 逐收件人
/// 腿同位——`deleteForEveryone {targetSentTimestamp: story ts}` DataMessage
/// 走 story 通道（fork `send_story_message`），timestamp = 删除消息自身
/// 时刻；无正文/附件/反应位（官方 sendMessageToServiceId deleteForEveryone
/// 载荷同构，ts/util/deleteStoryForEveryone.preload.ts）。
fn build_story_delete_data_message(target_timestamp: u64, timestamp: u64) -> DataMessage {
    DataMessage {
        timestamp: Some(timestamp),
        delete: Some(data_message::Delete {
            target_sent_timestamp: Some(target_timestamp),
        }),
        ..DataMessage::default()
    }
}

/// deleteStoryForEveryone 编排（S5，compat §3）：官方
/// sendDeleteStoryForEveryone + util/deleteStoryForEveryone 行为对齐——
/// 名单 story（1:1 扇出）= 逐收件人 DOE DataMessage 走 story 通道（群 story
/// 腿 = 普通 group DOE，既有 sendRemoteDelete 覆盖，v1 边界不做）；之后
/// 恰好一次自同步 `SyncMessage.Sent{timestamp: story ts, isRecipientUpdate:
/// true, storyMessageRecipients: updated}`——无 message/storyMessage 面
/// （多设备官方 StoryRecipientUpdate 事件载体），零收件人官方 Special
/// case 同构照发。per-recipient 失败整拒无 sync（sendStory 既有纪律；
/// 官方按 sendStatus 重试的偏差登记 compat §5）。解析全量前置 + 解析序
/// 去重（sendStory 同构）；24h 过期纯客户端语义不设引擎面。
pub(crate) async fn send_story_delete_for_everyone(
    service: &Arc<Service>,
    account: &str,
    target_timestamp: u64,
    recipients: &[String],
    updated: &[SendStoryRecipientUpdate],
) -> Result<u64, SendError> {
    if !service.real {
        return Ok(super::now_ms());
    }
    let (shared, account_dir) = super::open_registered_with_dir(service, account)
        .await
        .ok_or(SendError::UnknownAccount)?;
    let mut manager = lock_registered_for_send(&shared).await;
    let timestamp = super::now_ms();
    // P9 profileSharing 闸（S3）：整批读一次 whitelist 位（fail-open）。
    let whitelist = super::profile_sharing_whitelist(service, account).await;

    // 解析全量前置（不可解析 = 无上行整拒），随后按解析序去重（sendStory
    // 同构——同一 conversation 只发一次）。
    let mut seen = HashSet::new();
    let mut resolved = Vec::with_capacity(recipients.len());
    for number in recipients {
        let service_id = resolve_recipient(&manager, number)
            .await
            .ok_or(SendError::Internal)?;
        if seen.insert(service_id) {
            resolved.push(service_id);
        }
    }

    let body = build_story_delete_data_message(target_timestamp, timestamp);
    for recipient in &resolved {
        // P9 身份拒发闸（DOE 同为内容面）。
        super::identity_send_gate(&account_dir, recipient)?;
        // P9 profileSharing 闸（S3）：官方逐收件人 sendMessage profileKey
        // 携带同源，未同意 → 不注入本机 profile_key。
        let include_profile_key = super::include_profile_key_for(&whitelist, recipient);
        manager
            .send_story_message(*recipient, body.clone(), timestamp, include_profile_key)
            .await
            .map_err(send_error_of)?;
    }

    // 自同步（官方收尾同构，恰好一次，零收件人 Special case 照发）：
    // timestamp = story ts（官方同源），isRecipientUpdate = true，
    // expirationStartTimestamp 缺席（官方 null）。best-effort——单设备无
    // 下游（NoMessagesToSend）与同步失败都不回滚已成功的 DOE 上行。
    let own_aci = manager.registration_data().service_ids.aci();
    let sent = sync_message::Sent {
        destination_service_id: Some(own_aci.service_id_string()),
        timestamp: Some(target_timestamp),
        is_recipient_update: Some(true),
        story_message_recipients: updated
            .iter()
            .map(|update| sync_message::sent::StoryMessageRecipient {
                destination_service_id: Some(update.destination.clone()),
                distribution_list_ids: update.distribution_list_ids.clone(),
                is_allowed_to_reply: Some(update.is_allowed_to_reply),
                destination_service_id_binary: None,
            })
            .collect(),
        ..Default::default()
    };
    let sync = SyncMessage {
        content: Some(sync_message::Content::Sent(sent)),
        ..Default::default()
    };
    let _ = manager
        .send_message(own_aci, ContentBody::SynchronizeMessage(sync), timestamp)
        .await;

    Ok(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn style(word: &str, start: u32, length: u32) -> SendTextStyle {
        SendTextStyle {
            style: word.to_owned(),
            start,
            length,
        }
    }

    #[test]
    fn story_text_style_word_table() {
        // 六值词表：DEFAULT 缺省字段省略，其余直传线上枚举 i32。
        assert_eq!(parse_story_text_style("DEFAULT"), Ok(None));
        assert_eq!(parse_story_text_style("REGULAR"), Ok(Some(1)));
        assert_eq!(parse_story_text_style("BOLD"), Ok(Some(2)));
        assert_eq!(parse_story_text_style("SERIF"), Ok(Some(3)));
        assert_eq!(parse_story_text_style("SCRIPT"), Ok(Some(4)));
        assert_eq!(parse_story_text_style("CONDENSED"), Ok(Some(5)));
        // 词表外（含 send 面 bodyRange 词表与大小写漂移）fail-closed。
        for word in ["ITALIC", "SPOILER", "bold", "", "NONE"] {
            assert!(parse_story_text_style(word).is_err(), "{word}");
        }
    }

    #[test]
    fn story_message_shape() {
        let story = build_story_message("hello", Some(2), true, &[], None, None, None, None, None)
            .expect("shape");
        assert_eq!(story.profile_key, None, "profileKey 由 send 面注入");
        assert_eq!(story.group, None, "名单腿不携群位");
        assert_eq!(story.allows_replies, Some(true));
        assert!(story.body_ranges.is_empty());
        match story.attachment {
            Some(story_message::Attachment::TextAttachment(text)) => {
                assert_eq!(text.text.as_deref(), Some("hello"));
                assert_eq!(text.text_style, Some(2), "textStyle 透传线上 i32");
                assert!(text.preview.is_none(), "preview 三件全缺不携 preview");
                assert!(text.text_foreground_color.is_none());
                assert!(text.text_background_color.is_none());
                assert!(text.background.is_none());
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
    }

    #[test]
    fn story_message_default_style_omits_field() {
        let story = build_story_message("hi", None, false, &[], None, None, None, None, None)
            .expect("shape");
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        assert_eq!(text.text_style, None, "DEFAULT = proto 字段缺席");
        assert_eq!(story.allows_replies, Some(false));
    }

    #[test]
    fn story_message_body_ranges_and_preview() {
        let pointer = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            ..Default::default()
        };
        let story = build_story_message(
            "styled",
            None,
            true,
            &[style("BOLD", 0, 4), style("SPOILER", 5, 2)],
            Some(pointer),
            None,
            None,
            None,
            None,
        )
        .expect("shape");
        assert_eq!(story.body_ranges.len(), 2);
        assert_eq!(story.body_ranges[0].start, Some(0));
        assert_eq!(story.body_ranges[0].length, Some(4));
        assert_eq!(
            story.body_ranges[0].associated_value,
            Some(presage::proto::body_range::AssociatedValue::Style(
                presage::proto::body_range::Style::Bold as i32
            )),
        );
        assert_eq!(
            story.body_ranges[1].associated_value,
            Some(presage::proto::body_range::AssociatedValue::Style(
                presage::proto::body_range::Style::Spoiler as i32
            )),
        );
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        let preview = text.preview.expect("preview projected");
        assert_eq!(preview.image.as_ref().unwrap().cdn_number, Some(3));
        // 未携 previewUrl/previewTitle 参：url/title 仍 None（现状形态）。
        assert!(preview.url.is_none() && preview.title.is_none());
    }

    #[test]
    fn story_message_preview_url_title_faces() {
        // previewUrl/previewTitle 三态进 Preview（2026-10-09 扩面）：双在场
        // 如实透传、仅 url 独立成 Preview（image 缺席，官方 preview 面
        // image 可空同构）、缺席不发明。
        let pointer = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            ..Default::default()
        };
        let url = "https://example.com/a".to_owned();
        let title = "Example".to_owned();

        let story = build_story_message(
            "both",
            None,
            true,
            &[],
            Some(pointer.clone()),
            Some(url.clone()),
            Some(title.clone()),
            None,
            None,
        )
        .expect("shape");
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        let preview = text.preview.expect("preview projected");
        assert_eq!(preview.url.as_deref(), Some("https://example.com/a"));
        assert_eq!(preview.title.as_deref(), Some("Example"));
        assert_eq!(preview.image.as_ref().unwrap().cdn_number, Some(3));
        assert!(preview.description.is_none() && preview.date.is_none());

        // 仅 url：无 previewImage 也独立成 Preview，image 缺席。
        let story = build_story_message(
            "url only",
            None,
            true,
            &[],
            None,
            Some(url.clone()),
            None,
            None,
            None,
        )
        .expect("shape");
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        let preview = text.preview.expect("preview projected");
        assert_eq!(preview.url.as_deref(), Some("https://example.com/a"));
        assert!(preview.title.is_none() && preview.image.is_none());

        // 缺席：不发明 Preview。
        let story = build_story_message("bare", None, true, &[], None, None, None, None, None)
            .expect("shape");
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        assert!(text.preview.is_none());
    }

    #[test]
    fn body_ranges_reject_non_inline_words() {
        // story 正文样式词表与 bodyRange 词表不同一：DEFAULT（正文样式缺省
        // 词）不是内联样式，NONE 同理——fail-closed。
        assert!(build_body_ranges(&[style("DEFAULT", 0, 1)]).is_err());
        assert!(build_body_ranges(&[style("NONE", 0, 1)]).is_err());
        assert!(build_body_ranges(&[style("bold", 0, 1)]).is_err());
    }

    #[test]
    fn story_delete_data_message_shape() {
        // S5 DOE 腿构造钉形：delete.targetSentTimestamp = story ts，消息
        // timestamp = 删除腿自身时刻；无正文/附件/reaction/群位（官方
        // deleteForEveryone 载荷同构——remoteDelete 呈现面由收投影照投）。
        let dm = build_story_delete_data_message(1_700_000_000_000, 7_777);
        assert_eq!(dm.timestamp, Some(7_777));
        let delete = dm.delete.expect("delete set");
        assert_eq!(delete.target_sent_timestamp, Some(1_700_000_000_000));
        assert!(dm.body.is_none());
        assert!(dm.reaction.is_none());
        assert!(dm.story_context.is_none());
        assert!(dm.group_v2.is_none());
        assert!(dm.attachments.is_empty());
        assert!(dm.profile_key.is_none(), "profileKey 由 send 面闸定注入");
    }

    #[test]
    fn dedup_keeps_first_occurrence_order() {
        let uuid_a = "9d0652a1-8b1c-4e0d-9d24-4cbd1d5b4d01";
        let uuid_b = "f4b3a2c0-1d2e-4f5a-8b7c-6d5e4f3a2b10";
        let parse = |uuid: &str| ServiceId::parse_from_service_id_string(uuid).expect("uuid");
        let resolved = vec![
            (parse(uuid_a), "+15550001111".to_owned()),
            (parse(uuid_b), "+15550002222".to_owned()),
            // 同一 ServiceId 的号码形态：去重不重复发送，保首现序。
            (parse(uuid_a), uuid_a.to_owned()),
        ];
        let deduped = dedup_resolved(resolved);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].1, "+15550001111");
        assert_eq!(deduped[1].1, "+15550002222");
    }

    /// 媒体 story 构造钉形（2026-10-14 批）：上传返回指针原样进
    /// StoryMessage.fileAttachment（官方 getStoryMessage fileAttachment 臂
    /// 同位，SendMessage.preload.ts:1043-1044）——profileKey 构造面 None
    ///（send 面注入）、名单腿 group None、allowsReplies 透传、bodyRanges
    /// 同臂照载（官方 :1059-1061 fileAttachment 分支同构）。
    #[test]
    fn media_story_message_shape() {
        let pointer = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            content_type: Some("image/jpeg".to_owned()),
            ..Default::default()
        };
        let story =
            build_media_story_message(false, &[style("BOLD", 0, 4)], pointer, None).expect("shape");
        assert_eq!(story.profile_key, None, "profileKey 由 send 面注入");
        assert_eq!(story.group, None, "名单腿不携群位");
        assert_eq!(story.allows_replies, Some(false));
        assert_eq!(story.body_ranges.len(), 1, "bodyRanges 文件臂同载");
        match story.attachment {
            Some(story_message::Attachment::FileAttachment(pointer)) => {
                assert_eq!(pointer.cdn_number, Some(3));
                assert_eq!(pointer.content_type.as_deref(), Some("image/jpeg"));
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
    }

    /// 媒体 story 往返互逆钉定（2026-10-14 批，compat §5 媒体 story 批）：
    /// 发送面产出的指针字段族（上传形状 key/cdnKey/cdnNumber/uploadTimestamp/
    /// digest + 元数据 contentType/filename/size）必须被本仓接收投影
    /// （project_story_message → project_attachment_pointer，mod.rs E2 批
    /// fileAttachment 全脸）原样读回——sealed-sender 载荷与自同步
    /// `SyncMessage.Sent.story_message` echo 载同一 story 值，两侧同判。
    #[test]
    fn media_story_pointer_round_trip_through_receive_projection() {
        use base64::Engine as _;
        let key: Vec<u8> = (0..64).collect();
        let digest: Vec<u8> = (0..32).map(|byte| byte * 3 + 1).collect();
        let pointer = presage::proto::AttachmentPointer {
            content_type: Some("video/mp4".to_owned()),
            key: Some(key.clone()),
            size: Some(123_456),
            digest: Some(digest.clone()),
            file_name: Some("clip.mp4".to_owned()),
            upload_timestamp: Some(1_700_000_000_000),
            cdn_number: Some(3),
            attachment_identifier: Some(
                presage::proto::attachment_pointer::AttachmentIdentifier::CdnKey(
                    "AWESOME-CDN-KEY".to_owned(),
                ),
            ),
            ..Default::default()
        };
        let story = build_media_story_message(true, &[], pointer.clone(), None).expect("shape");

        // 接收投影（E2 批既有面，不动）读回。
        let out = crate::service::project_story_message(&story, &std::collections::HashMap::new())
            .expect("story projection");
        let face = &out["fileAttachment"];
        assert_eq!(face["contentType"], json!("video/mp4"));
        assert_eq!(face["filename"], json!("clip.mp4"));
        assert_eq!(face["size"], json!(123_456));
        assert_eq!(out["allowsReplies"], json!(true));
        // locator 指针族逐字段读回（上传产出形状 → 投影原样）。
        assert_eq!(
            face["locator"]["key"],
            json!(base64::engine::general_purpose::STANDARD.encode(&key))
        );
        assert_eq!(face["locator"]["cdnKey"], json!("AWESOME-CDN-KEY"));
        assert_eq!(face["locator"]["cdnNumber"], json!(3));
        assert_eq!(
            face["locator"]["uploadTimestamp"],
            json!(1_700_000_000_000_u64)
        );
        assert_eq!(
            face["locator"]["digest"],
            json!(base64::engine::general_purpose::STANDARD.encode(&digest))
        );

        // 自同步 echo 载同一 story 值：Sent.story_message 反序列化往返后
        // 投影不变（sync 面 = sealed-sender 载荷同值）。
        use prost::Message as _;
        let mut buf = Vec::new();
        story.encode(&mut buf).expect("encode");
        let echoed = StoryMessage::decode(buf.as_slice()).expect("decode");
        let out = crate::service::project_story_message(&echoed, &std::collections::HashMap::new())
            .expect("story projection");
        assert_eq!(
            out["fileAttachment"]["locator"]["cdnKey"],
            json!("AWESOME-CDN-KEY")
        );
        assert_eq!(
            out["fileAttachment"]["locator"]["key"],
            json!(base64::engine::general_purpose::STANDARD.encode(&key))
        );
    }

    /// 群上下文构造钉形（2026-10-15 批）：官方 getStoryMessage group 位
    /// 同构（SendMessage.preload.ts:1054-1060）——masterKey/revision 载入，
    /// groupChange 恒缺（官方 story 发送无传入源，fail-closed 不发明）。
    #[test]
    fn story_group_context_shape() {
        let key: Vec<u8> = (0..32).collect();
        let context = build_story_group_context(key.clone(), 7);
        assert_eq!(context.master_key.as_deref(), Some(key.as_slice()));
        assert_eq!(context.revision, Some(7));
        assert_eq!(context.group_change, None, "groupChange 官方恒缺");
    }

    /// 群 story 文本腿构造钉形（2026-10-15 批）：group 上下文原样进
    /// StoryMessage.group、群腿 allowsReplies 恒 true（官方
    /// sendStory.preload.ts:199/:352-354 同位）、文本位照常。
    #[test]
    fn group_story_text_message_shape() {
        let key: Vec<u8> = (0..32).collect();
        let context = build_story_group_context(key.clone(), 3);
        let story = build_story_message(
            "group story",
            Some(2),
            true,
            &[],
            None,
            None,
            None,
            None,
            Some(context),
        )
        .expect("shape");
        let group = story.group.expect("group carried");
        assert_eq!(group.master_key.as_deref(), Some(key.as_slice()));
        assert_eq!(group.revision, Some(3));
        assert_eq!(group.group_change, None);
        assert_eq!(story.allows_replies, Some(true), "群腿恒可回");
        match story.attachment {
            Some(story_message::Attachment::TextAttachment(text)) => {
                assert_eq!(text.text.as_deref(), Some("group story"));
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
    }

    /// 群 story 媒体腿构造钉形（2026-10-15 批）：与 text+group 正交组合
    /// （官方群 story 两内容臂同源 getStoryMessage），指针进
    /// fileAttachment、群上下文同载。
    #[test]
    fn group_story_media_message_shape() {
        let key: Vec<u8> = (0..32).collect();
        let pointer = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            content_type: Some("video/mp4".to_owned()),
            ..Default::default()
        };
        let story = build_media_story_message(
            true,
            &[style("MONOSPACE", 0, 2)],
            pointer,
            Some(build_story_group_context(key, 11)),
        )
        .expect("shape");
        let group = story.group.expect("group carried");
        assert_eq!(group.revision, Some(11));
        assert_eq!(story.allows_replies, Some(true));
        assert_eq!(story.body_ranges.len(), 1, "bodyRanges 群媒体腿同载");
        match story.attachment {
            Some(story_message::Attachment::FileAttachment(pointer)) => {
                assert_eq!(pointer.content_type.as_deref(), Some("video/mp4"));
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
    }

    /// 群 story 自同步扇出条目钉形（2026-10-15 批）：每成员
    /// {destinationServiceId, distributionListIds: [], isAllowedToReply:
    /// true}（官方群 story sync 同构——addDistributionListToServiceIdSent
    /// 群臂空名单位 + 群 sendState 恒可回位）；空成员批 = 空 vec（sync 整
    /// 条不发闸在编排面）；保传入序。
    #[test]
    fn group_story_sync_recipients_shape() {
        assert!(group_story_sync_recipients(&[]).is_empty(), "空批空 vec");
        let parse = |uuid: &str| ServiceId::parse_from_service_id_string(uuid).expect("uuid");
        let a = parse("9d0652a1-8b1c-4e0d-9d24-4cbd1d5b4d01");
        let b = parse("f4b3a2c0-1d2e-4f5a-8b7c-6d5e4f3a2b10");
        let entries = group_story_sync_recipients(&[a, b]);
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].destination_service_id.as_deref(),
            Some("9d0652a1-8b1c-4e0d-9d24-4cbd1d5b4d01")
        );
        assert!(
            entries[0].distribution_list_ids.is_empty(),
            "群位空名单位（官方同构）"
        );
        assert_eq!(entries[0].is_allowed_to_reply, Some(true), "恒可回");
        assert_eq!(
            entries[1].destination_service_id.as_deref(),
            Some("f4b3a2c0-1d2e-4f5a-8b7c-6d5e4f3a2b10")
        );
        assert_eq!(entries[1].distribution_list_ids, Vec::<String>::new());
        assert_eq!(entries[1].is_allowed_to_reply, Some(true));
    }

    /// 媒体 story caption 构造钉形（2026-10-15 批，compat §5 媒体 story
    /// caption 批）：caption 属上传面元数据——带 caption 的上传返回指针原样
    /// 进 StoryMessage.fileAttachment（官方 uploadAttachment `caption || null`
    /// 进指针同构 uploadAttachment.preload.ts:99/:134，getStoryMessage
    /// `attachment = { fileAttachment }` 恰一 SendMessage.preload.ts:
    /// 1043-1044）；接收投影 caption 位原样读回（收发互逆，E2 既有面）；
    /// prost encode/decode 往返（sealed-sender 载荷与 sync echo 同值面）
    /// caption 保持。
    #[test]
    fn media_story_caption_pointer_shape_and_roundtrip() {
        use prost::Message as _;
        let captioned = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            content_type: Some("image/jpeg".to_owned()),
            caption: Some("a caption".to_owned()),
            ..Default::default()
        };
        let story = build_media_story_message(true, &[], captioned.clone(), None).expect("shape");
        match &story.attachment {
            Some(story_message::Attachment::FileAttachment(pointer)) => {
                assert_eq!(
                    pointer.caption.as_deref(),
                    Some("a caption"),
                    "指针 caption 原样透传（构造面不补键不剥离）"
                );
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
        // 接收投影（E2 既有面）caption 位读回——收发互逆半边。
        let out = crate::service::project_story_message(&story, &std::collections::HashMap::new())
            .expect("story projection");
        assert_eq!(out["fileAttachment"]["caption"], json!("a caption"));
        // prost 往返（sync echo / sealed-sender 同值面）caption 保持。
        let mut buf = Vec::new();
        story.encode(&mut buf).expect("encode");
        let echoed = StoryMessage::decode(buf.as_slice()).expect("decode");
        assert_eq!(
            echoed.attachment,
            Some(story_message::Attachment::FileAttachment(captioned))
        );

        // 缺席面（pre-caption 帧同形）：无 caption 指针 → fileAttachment
        // caption 缺席（proto 字段 11 不在线上）→ 投影无 caption 键 →
        // encode/decode 往返保持缺席。
        let bare = presage::proto::AttachmentPointer {
            cdn_number: Some(3),
            ..Default::default()
        };
        let story = build_media_story_message(false, &[], bare, None).expect("shape");
        match &story.attachment {
            Some(story_message::Attachment::FileAttachment(pointer)) => {
                assert_eq!(pointer.caption, None, "缺席 = proto 字段缺席");
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
        let out = crate::service::project_story_message(&story, &std::collections::HashMap::new())
            .expect("story projection");
        assert!(
            out["fileAttachment"].get("caption").is_none(),
            "缺席键不在（pre-caption 帧零漂移）"
        );
        let mut buf = Vec::new();
        story.encode(&mut buf).expect("encode");
        let echoed = StoryMessage::decode(buf.as_slice()).expect("decode");
        match echoed.attachment {
            Some(story_message::Attachment::FileAttachment(pointer)) => {
                assert_eq!(pointer.caption, None);
            }
            other => panic!("unexpected attachment: {other:?}"),
        }
    }
}
