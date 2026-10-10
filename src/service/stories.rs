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
//! 边界（任务口径）：distribution list 成员解析归 connector（引擎只收显式
//! recipients）；群 story / 文件 story（image 正文）v1 不做；24h 过期纯客
//! 户端语义不在引擎面。
//!
//! 纪律（compat §1）：本层不产生任何 stderr 输出。

use std::collections::HashSet;
use std::sync::Arc;

use presage::libsignal_service::content::ContentBody;
use presage::libsignal_service::protocol::ServiceId;
use presage::proto::{
    story_message, sync_message, text_attachment, BodyRange, Preview, StoryMessage, SyncMessage,
    TextAttachment,
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
/// - `attachment` 恒 TextAttachment（文件 story v1 不做）；
/// - preview = 三件套组装（见 build_story_preview）：preview.image = 调用
///   方已上行的 CDN 指针（官方 sendStory 同构：preview 数据随正文附件同批
///   上行），url/title = previewUrl/previewTitle 扩展键如实透传（解析闸
///   在 methods 参数面：https-only ≤2048 字符 / 非空 ≤1024 UTF-8 字节）；
/// - `allows_replies` 透传（官方值源 = distribution list 属性，缺省线上
///   false；connector 从名单记录展开后显式传入）。
pub(crate) fn build_story_message(
    text: &str,
    text_style: Option<i32>,
    allows_replies: bool,
    body_ranges: &[SendTextStyle],
    preview_image: Option<presage::proto::AttachmentPointer>,
    preview_url: Option<String>,
    preview_title: Option<String>,
) -> Result<StoryMessage, SendError> {
    Ok(StoryMessage {
        profile_key: None,
        group: None,
        allows_replies: Some(allows_replies),
        body_ranges: build_body_ranges(body_ranges)?,
        attachment: Some(story_message::Attachment::TextAttachment(TextAttachment {
            text: Some(text.to_owned()),
            text_style,
            text_foreground_color: None,
            text_background_color: None,
            preview: build_story_preview(preview_image, preview_url, preview_title),
            background: None,
        })),
    })
}

/// story preview 组装（纯函数，单测钉形状）：image/url/title 三件任一在场
/// 即成 Preview（官方 sendStory preview 面 image 可空同构——
/// `hydratedPreview.image && uploadAttachment`，url/title 独立席位）；全缺
/// = None（线上缺席位，现状形态）。description/date 官方 story 预览面无
/// 源，恒缺席。
fn build_story_preview(
    image: Option<presage::proto::AttachmentPointer>,
    url: Option<String>,
    title: Option<String>,
) -> Option<Preview> {
    if image.is_none() && url.is_none() && title.is_none() {
        return None;
    }
    Some(Preview {
        url,
        title,
        image,
        description: None,
        date: None,
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
    preview_image: Option<&str>,
    preview_url: Option<String>,
    preview_title: Option<String>,
) -> Result<(u64, Vec<Value>), SendError> {
    if !service.real {
        return Ok((super::now_ms(), Vec::new()));
    }
    let shared = open_registered(service, account)
        .await
        .ok_or(SendError::UnknownAccount)?;
    let mut manager = lock_registered_for_send(&shared).await;
    let timestamp = super::now_ms();

    // previewImage 随批上行（官方 preview.image 同批 CDN 上行同构）；story
    // 无正文附件位，entries 恒空。
    let (_, preview_pointer) = match preview_image {
        Some(uri) => upload_attachments_for_send(&manager, &[], Some(uri)).await?,
        None => (Vec::new(), None),
    };
    let story = build_story_message(
        text,
        text_style,
        allows_replies,
        body_ranges,
        preview_pointer,
        preview_url,
        preview_title,
    )?;

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
        let story =
            build_story_message("hello", Some(2), true, &[], None, None, None).expect("shape");
        assert_eq!(story.profile_key, None, "profileKey 由 send 面注入");
        assert_eq!(story.group, None, "群 story v1 不做");
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
        let story = build_story_message("hi", None, false, &[], None, None, None).expect("shape");
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
        let story = build_story_message("url only", None, true, &[], None, Some(url.clone()), None)
            .expect("shape");
        let Some(story_message::Attachment::TextAttachment(text)) = story.attachment else {
            panic!("text attachment expected");
        };
        let preview = text.preview.expect("preview projected");
        assert_eq!(preview.url.as_deref(), Some("https://example.com/a"));
        assert!(preview.title.is_none() && preview.image.is_none());

        // 缺席：不发明 Preview。
        let story = build_story_message("bare", None, true, &[], None, None, None).expect("shape");
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
}
