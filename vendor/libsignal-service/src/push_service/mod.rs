use std::{sync::LazyLock, time::Duration};

use crate::{
    configuration::{Endpoint, ServiceCredentials, SignalServers},
    prelude::ServiceConfiguration,
    utils::serde_device_id_vec,
    websocket::{SignalWebSocket, WebSocketType},
};

use libsignal_core::DeviceId;
use protobuf::ProtobufResponseExt;
use reqwest::{Method, RequestBuilder, StatusCode};
use reqwest_websocket::Upgrade;
use serde::{Deserialize, Serialize};
use tracing::{debug_span, Instrument};

// KT (P12/R3): 55s→30s. A keepalive counts as failed only at the *next*
// tick, so dead-link detection was 55-110s; at 30s it is ≤60s while staying
// above typical NAT idle amortization for a healthy ack-driven link
// (official Desktop: WebsocketResources.preload.ts:512
// KEEPALIVE_TIMEOUT_MS = 30s).
pub const KEEPALIVE_TIMEOUT_SECONDS: Duration = Duration::from_secs(30);

// KT (2026-10-15 cloud-restore transport batch): request budgets are split
// per face. Control-plane API keeps the upstream 10s connect + 65s total
// deadline. Large CDN GET bodies (attachment/backup archives) legitimately
// stream longer than any fixed total deadline — a total timeout kills
// healthy slow streams mid-flight (surfacing as cdn_unreachable) — so the
// download client drops the total deadline and judges death by read stall
// instead (reqwest read_timeout resets after every successful read).
pub const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const CONTROL_PLANE_TOTAL_TIMEOUT: Duration = Duration::from_secs(65);
pub const DOWNLOAD_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Read-stall judge for the download face: 45s without a single arriving
/// byte kills the stream. A healthy (if slow) transfer never trips it;
/// a dead link surfaces in under a minute.
pub const DOWNLOAD_READ_STALL_TIMEOUT: Duration = Duration::from_secs(45);

pub static DEFAULT_DEVICE_ID: LazyLock<libsignal_core::DeviceId> =
    LazyLock::new(|| libsignal_core::DeviceId::try_from(1).unwrap());

mod account;
mod cdn;
mod error;
pub mod linking;
pub(crate) mod response;

pub use account::*;
pub use cdn::*;
pub use error::*;
pub(crate) use response::SignalServiceResponse;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProofRequired {
    pub token: String,
    pub options: Vec<String>,
}

#[derive(derive_more::Debug, Clone, Serialize, Deserialize)]
pub struct HttpAuth {
    pub username: String,
    #[debug(ignore)]
    pub password: String,
}

#[derive(Debug, Clone)]
pub enum HttpAuthOverride {
    NoOverride,
    Unidentified,
    Identified(HttpAuth),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AvatarWrite<C> {
    NewAvatar(C),
    RetainAvatar,
    NoAvatar,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MismatchedDevices {
    #[serde(with = "serde_device_id_vec")]
    pub missing_devices: Vec<DeviceId>,
    #[serde(with = "serde_device_id_vec")]
    pub extra_devices: Vec<DeviceId>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaleDevices {
    #[serde(with = "serde_device_id_vec")]
    pub stale_devices: Vec<DeviceId>,
}

#[derive(Clone)]
pub struct PushService {
    pub(crate) servers: SignalServers,
    cfg: ServiceConfiguration,
    credentials: Option<HttpAuth>,
    client: reqwest::Client,
    download_client: reqwest::Client,
}

fn pinned_client_builder(ca_pem: &str, user_agent: &str) -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new()
        .tls_certs_only([reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap()])
        .user_agent(user_agent)
        .http1_only()
}

impl PushService {
    pub fn new(
        env: SignalServers,
        credentials: Option<ServiceCredentials>,
        user_agent: impl AsRef<str>,
    ) -> Self {
        let cfg: ServiceConfiguration = env.into();

        // Use the ring provider except if the application already installed one.
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }

        let client = pinned_client_builder(&cfg.certificate_authority, user_agent.as_ref())
            .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
            .timeout(CONTROL_PLANE_TOTAL_TIMEOUT)
            .build()
            .unwrap();
        let download_client =
            pinned_client_builder(&cfg.certificate_authority, user_agent.as_ref())
                .connect_timeout(DOWNLOAD_CONNECT_TIMEOUT)
                .read_timeout(DOWNLOAD_READ_STALL_TIMEOUT)
                .build()
                .unwrap();

        Self {
            servers: env,
            cfg,
            credentials: credentials.and_then(|c| c.authorization()),
            client,
            download_client,
        }
    }

    #[tracing::instrument(skip(self), fields(endpoint = %endpoint))]
    pub fn request(
        &self,
        method: Method,
        endpoint: Endpoint,
        auth_override: HttpAuthOverride,
    ) -> Result<RequestBuilder, ServiceError> {
        self.request_on(&self.client, method, endpoint, auth_override)
    }

    /// Download-face request (large CDN GET bodies): no total deadline,
    /// death judged by [`DOWNLOAD_READ_STALL_TIMEOUT`]. Control-plane
    /// budgets live in [`PushService::request`].
    #[tracing::instrument(skip(self), fields(endpoint = %endpoint))]
    pub fn download_request(
        &self,
        method: Method,
        endpoint: Endpoint,
        auth_override: HttpAuthOverride,
    ) -> Result<RequestBuilder, ServiceError> {
        self.request_on(&self.download_client, method, endpoint, auth_override)
    }

    fn request_on(
        &self,
        client: &reqwest::Client,
        method: Method,
        endpoint: Endpoint,
        auth_override: HttpAuthOverride,
    ) -> Result<RequestBuilder, ServiceError> {
        let url = endpoint.into_url(&self.cfg)?;
        let mut builder = client.request(method, url);

        builder = match auth_override {
            HttpAuthOverride::NoOverride => {
                if let Some(HttpAuth { username, password }) =
                    self.credentials.as_ref()
                {
                    builder.basic_auth(username, Some(password))
                } else {
                    builder
                }
            },
            HttpAuthOverride::Identified(HttpAuth { username, password }) => {
                builder.basic_auth(username, Some(password))
            },
            HttpAuthOverride::Unidentified => builder,
        };

        Ok(builder)
    }

    pub async fn ws<C: WebSocketType>(
        &mut self,
        path: &str,
        keepalive_path: &str,
        additional_headers: &[(&'static str, &str)],
        credentials: Option<ServiceCredentials>,
    ) -> Result<SignalWebSocket<C>, ServiceError> {
        let span = debug_span!("websocket");

        let mut url = Endpoint::service(path).into_url(&self.cfg)?;
        url.set_scheme("wss").expect("valid https base url");

        let mut builder = self.client.get(url);
        for (key, value) in additional_headers {
            builder = builder.header(*key, *value);
        }

        if let Some(credentials) = credentials {
            builder =
                builder.basic_auth(credentials.login(), credentials.password);
        }

        let response = builder.upgrade().send().await?;
        classify_upgrade_response(response.status())?;
        let ws = response.into_websocket().instrument(span.clone()).await?;

        let unidentified_push_service = PushService {
            servers: self.servers,
            cfg: self.cfg.clone(),
            credentials: None,
            client: self.client.clone(),
            download_client: self.download_client.clone(),
        };
        let (ws, task) = SignalWebSocket::new(
            ws,
            keepalive_path.to_owned(),
            unidentified_push_service,
        );
        let task = task.instrument(span);
        tracing::warn!("ka_diag task spawned"); // KA-DIAG
        tokio::task::spawn(task);
        Ok(ws)
    }

    // KT (groups fetch): GET /v1/groups/ answers GroupResponse{group=1,
    // group_send_endorsements_response=2} (Android GroupsV2Api.getGroup);
    // decoding it as a bare Group lands the inner group bytes in
    // `publicKey` and drops the endorsements needed for group-send tokens.
    pub(crate) async fn get_group_response(
        &mut self,
        credentials: HttpAuth,
    ) -> Result<crate::proto::GroupResponse, ServiceError> {
        self.request(
            Method::GET,
            Endpoint::storage("/v1/groups/"),
            HttpAuthOverride::Identified(credentials),
        )?
        .send()
        .await?
        .service_error_for_status()
        .await?
        .protobuf()
        .await
    }
}

/// 401/403 on the WS upgrade are credential rejection, not transport failure.
fn classify_upgrade_response(status: StatusCode) -> Result<(), ServiceError> {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            Err(ServiceError::Unauthorized)
        },
        _ => Ok(()),
    }
}

pub(crate) mod protobuf {
    use async_trait::async_trait;
    use prost::{EncodeError, Message};
    use reqwest::{header, RequestBuilder, Response};

    use super::ServiceError;

    pub(crate) trait ProtobufRequestBuilderExt
    where
        Self: Sized,
    {
        /// Set the request payload encoded as protobuf.
        /// Sets the `Content-Type` header to `application/x-protobuf`
        #[allow(dead_code)]
        fn protobuf<T: Message + Default>(
            self,
            value: T,
        ) -> Result<Self, EncodeError>;
    }

    #[async_trait::async_trait]
    pub(crate) trait ProtobufResponseExt {
        /// Get the response body decoded from Protobuf
        async fn protobuf<T>(self) -> Result<T, ServiceError>
        where
            T: prost::Message + Default;
    }

    impl ProtobufRequestBuilderExt for RequestBuilder {
        fn protobuf<T: Message + Default>(
            self,
            value: T,
        ) -> Result<Self, EncodeError> {
            let mut buf = Vec::new();
            value.encode(&mut buf)?;
            let this =
                self.header(header::CONTENT_TYPE, "application/x-protobuf");
            Ok(this.body(buf))
        }
    }

    #[async_trait]
    impl ProtobufResponseExt for Response {
        async fn protobuf<T>(self) -> Result<T, ServiceError>
        where
            T: Message + Default,
        {
            let body = self.bytes().await?;
            let decoded = T::decode(body)?;
            Ok(decoded)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::SignalServers;
    use bytes::{Buf, Bytes};

    #[test]
    fn create_clients() {
        let environments = &[SignalServers::Staging, SignalServers::Production];

        for env in environments {
            let _ =
                super::PushService::new(*env, None, "libsignal-service test");
        }
    }

    #[test]
    fn serde_json_from_empty_reader() {
        // This fails, so we have handle empty response body separately in HyperPushService::json()
        let bytes: Bytes = "".into();
        assert!(
            serde_json::from_reader::<bytes::buf::Reader<Bytes>, String>(
                bytes.reader()
            )
            .is_err()
        );
    }

    #[test]
    fn serde_json_form_empty_vec() {
        // If we're trying to send and empty payload, serde_json must be able to make a Vec out of it
        assert!(serde_json::to_vec(b"").is_ok());
    }
}

#[cfg(test)]
mod keepalive_timeout_tests {
    use super::KEEPALIVE_TIMEOUT_SECONDS;

    // KT (P12/R3): the aggressive-dead-link-detection tightening must not
    // silently regress upward (nor drop below the NAT-amortization floor).
    #[test]
    fn keepalive_timeout_is_thirty_seconds() {
        assert_eq!(
            KEEPALIVE_TIMEOUT_SECONDS,
            std::time::Duration::from_secs(30)
        );
    }
}

#[cfg(test)]
mod request_budget_tests {
    use super::{
        CONTROL_PLANE_CONNECT_TIMEOUT, CONTROL_PLANE_TOTAL_TIMEOUT,
        DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_STALL_TIMEOUT,
    };
    use std::time::Duration;

    // KT: control-plane API keeps the upstream budget; regressions here
    // would silently change every non-download call in the engine.
    #[test]
    fn control_plane_keeps_upstream_budget() {
        assert_eq!(CONTROL_PLANE_CONNECT_TIMEOUT, Duration::from_secs(10));
        assert_eq!(CONTROL_PLANE_TOTAL_TIMEOUT, Duration::from_secs(65));
    }

    // KT: the download face must never carry a total deadline again (large
    // CDN bodies get killed mid-stream) and its stall judge must stay
    // inside the 30-60s band: long enough to ride out transient stalls,
    // short enough that a dead link surfaces quickly.
    #[test]
    fn download_face_is_stall_judged_without_total_deadline() {
        assert_eq!(DOWNLOAD_CONNECT_TIMEOUT, CONTROL_PLANE_CONNECT_TIMEOUT);
        assert!(DOWNLOAD_READ_STALL_TIMEOUT >= Duration::from_secs(30));
        assert!(DOWNLOAD_READ_STALL_TIMEOUT <= Duration::from_secs(60));
    }
}

#[cfg(test)]
mod upgrade_classify_tests {
    use super::classify_upgrade_response;
    use crate::push_service::ServiceError;
    use reqwest::StatusCode;

    #[test]
    fn credential_rejection_stays_unauthorized() {
        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            assert!(matches!(
                classify_upgrade_response(status),
                Err(ServiceError::Unauthorized)
            ));
        }
    }

    #[test]
    fn other_statuses_are_left_to_into_websocket() {
        for status in [
            StatusCode::SWITCHING_PROTOCOLS,
            StatusCode::OK,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(classify_upgrade_response(status).is_ok());
        }
    }
}
