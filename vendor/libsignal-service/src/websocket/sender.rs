use crate::{
    sender::{OutgoingPushMessages, SendMessageResponse},
    unidentified_access::UnidentifiedAccess,
    utils::BASE64_RELAXED,
};

use super::*;
use base64::Engine;

impl<C: WebSocketType> SignalWebSocket<C> {
    pub async fn send_messages(
        &mut self,
        messages: OutgoingPushMessages,
    ) -> Result<SendMessageResponse, ServiceError> {
        let request = WebSocketRequestMessage::new(Method::PUT)
            .path(format!(
                "/v1/messages/{}",
                messages.destination.service_id_string()
            ))
            .json(&messages)?;
        self.request_json(request).await
    }

    pub async fn send_messages_unidentified(
        &mut self,
        messages: OutgoingPushMessages,
        access: &UnidentifiedAccess,
    ) -> Result<SendMessageResponse, ServiceError> {
        let request = WebSocketRequestMessage::new(Method::PUT)
            .path(format!(
                "/v1/messages/{}",
                messages.destination.service_id_string()
            ))
            .header(
                "Unidentified-Access-Key",
                BASE64_RELAXED.encode(&access.key),
            )
            .json(&messages)?;
        self.request_json(request).await
    }

    /// KT fork (stories): story 发送的官方传输面——单收件人 sealed-sender
    /// 上行带 `?story=true` 查询参、**不带** `Unidentified-Access-Key`
    /// 头（story 模式无按收件人 UAK 授权，服务器按 story 端点鉴权）。
    /// 官方行为依据（行为对照，零代码拷贝）：libsignal v0.99.0
    /// `rust/net/chat/src/ws/messages.rs:141-150`（story 时路径后缀
    /// `?story=true`，否则无后缀）与 `:39`（`Story => None`，不加任何
    /// user-based 授权头）；Signal-Desktop `ts/textsecure/WebAPI.preload.ts`
    /// `:3804`/`:3850`（`/v1/messages/{dest}?story=<bool>` 同查询参）与
    /// `ts/textsecure/OutgoingMessage.preload.ts:392-394`（story 优先级
    /// 最高，恒走 unauthenticated sealed-sender 通道）。
    pub async fn send_messages_unidentified_story(
        &mut self,
        messages: OutgoingPushMessages,
    ) -> Result<SendMessageResponse, ServiceError> {
        let request = WebSocketRequestMessage::new(Method::PUT)
            .path(format!(
                "/v1/messages/{}?story=true",
                messages.destination.service_id_string()
            ))
            .json(&messages)?;
        self.request_json(request).await
    }
}
