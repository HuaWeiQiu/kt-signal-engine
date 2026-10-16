use std::{collections::HashMap, convert::TryInto};

use crate::{
    configuration::Endpoint,
    groups_v2::{
        model::{Group, GroupChanges},
        operations::{GroupDecodingError, GroupOperations},
    },
    prelude::{PushService, ServiceError},
    proto::GroupContextV2,
    push_service::{
        HttpAuth, HttpAuthOverride, ServiceIds, SignalServiceResponse,
    },
    utils::BASE64_RELAXED,
    websocket::{self, SignalWebSocket},
};

use base64::prelude::*;
use bytes::Bytes;
use chrono::{Days, NaiveDate, NaiveTime, Utc};
use futures::AsyncReadExt;
use rand::{CryptoRng, Rng};
use reqwest::Method;
use serde::Deserialize;
use zkgroup::{
    auth::{AuthCredentialWithPni, AuthCredentialWithPniResponse},
    groups::{GroupMasterKey, GroupSecretParams},
    ServerPublicParams,
};

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalCredential {
    credential: String,
    redemption_time: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialResponse {
    credentials: Vec<TemporalCredential>,
}

impl CredentialResponse {
    pub fn parse(
        self,
    ) -> Result<HashMap<u64, AuthCredentialWithPniResponse>, ServiceError> {
        self.credentials
            .into_iter()
            .map(|c| {
                let bytes = BASE64_RELAXED.decode(c.credential)?;
                let data = AuthCredentialWithPniResponse::new(&bytes)?;
                Ok((c.redemption_time, data))
            })
            .collect::<Result<_, ServiceError>>()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialsCacheError {
    #[error("failed to read values from cache: {0}")]
    ReadError(String),
    #[error("failed to write values from cache: {0}")]
    WriteError(String),
}

/// Global cache for groups v2 credentials, as demonstrated in the libsignal-service
/// java library of Signal-Android.
///
/// A basic in-memory implementation is provided with `InMemoryCredentialsCache`.
pub trait CredentialsCache {
    fn clear(&mut self) -> Result<(), CredentialsCacheError>;

    /// Get an entry of the cache, key usually represents the day number since EPOCH.
    fn get(
        &self,
        key: &u64,
    ) -> Result<Option<&AuthCredentialWithPniResponse>, CredentialsCacheError>;

    /// Overwrite the entire contents of the cache with new data.
    fn write(
        &mut self,
        map: HashMap<u64, AuthCredentialWithPniResponse>,
    ) -> Result<(), CredentialsCacheError>;
}

#[derive(Default)]
pub struct InMemoryCredentialsCache {
    map: HashMap<u64, AuthCredentialWithPniResponse>,
}

impl CredentialsCache for InMemoryCredentialsCache {
    fn clear(&mut self) -> Result<(), CredentialsCacheError> {
        self.map.clear();
        Ok(())
    }

    fn get(
        &self,
        key: &u64,
    ) -> Result<Option<&AuthCredentialWithPniResponse>, CredentialsCacheError>
    {
        Ok(self.map.get(key))
    }

    fn write(
        &mut self,
        map: HashMap<u64, AuthCredentialWithPniResponse>,
    ) -> Result<(), CredentialsCacheError> {
        self.map = map;
        Ok(())
    }
}

impl<T: CredentialsCache> CredentialsCache for &mut T {
    fn clear(&mut self) -> Result<(), CredentialsCacheError> {
        (**self).clear()
    }

    fn get(
        &self,
        key: &u64,
    ) -> Result<Option<&AuthCredentialWithPniResponse>, CredentialsCacheError>
    {
        (**self).get(key)
    }

    fn write(
        &mut self,
        map: HashMap<u64, AuthCredentialWithPniResponse>,
    ) -> Result<(), CredentialsCacheError> {
        (**self).write(map)
    }
}

pub struct GroupsManager<C: CredentialsCache> {
    service_ids: ServiceIds,
    identified_push_service: PushService,
    unidentified_websocket: SignalWebSocket<websocket::Unidentified>,
    credentials_cache: C,
    server_public_params: ServerPublicParams,
}

impl<C: CredentialsCache> GroupsManager<C> {
    pub fn new(
        service_ids: ServiceIds,
        identified_push_service: PushService,
        unidentified_websocket: SignalWebSocket<websocket::Unidentified>,
        credentials_cache: C,
        server_public_params: ServerPublicParams,
    ) -> Self {
        Self {
            service_ids,
            identified_push_service,
            unidentified_websocket,
            credentials_cache,
            server_public_params,
        }
    }

    pub async fn get_authorization_for_today<R: Rng + CryptoRng>(
        &mut self,
        csprng: &mut R,
        group_secret_params: GroupSecretParams,
    ) -> Result<HttpAuth, ServiceError> {
        let (today, today_plus_7_days) = current_days_seconds();

        let auth_credential_response = if let Some(auth_credential_response) =
            self.credentials_cache.get(&today)?
        {
            auth_credential_response
        } else {
            let path =
            format!("/v1/certificate/auth/group?redemptionStartSeconds={}&redemptionEndSeconds={}&pniAsServiceId=true", today, today_plus_7_days);

            let credentials_response: CredentialResponse = self
                .identified_push_service
                .request(
                    Method::GET,
                    Endpoint::service(path),
                    HttpAuthOverride::NoOverride,
                )?
                .send()
                .await?
                .service_error_for_status()
                .await?
                .json()
                .await?;
            self.credentials_cache
                .write(credentials_response.parse()?)?;
            self.credentials_cache.get(&today)?.ok_or({
                ServiceError::InvalidFrame {
                    reason:
                        "credentials received did not contain requested day",
                }
            })?
        };

        self.get_authorization_string(
            csprng,
            group_secret_params,
            auth_credential_response.clone(),
            today,
        )
    }

    fn get_authorization_string<R: Rng + CryptoRng>(
        &self,
        csprng: &mut R,
        group_secret_params: GroupSecretParams,
        credential_response: AuthCredentialWithPniResponse,
        today: u64,
    ) -> Result<HttpAuth, ServiceError> {
        let redemption_time = zkgroup::Timestamp::from_epoch_seconds(today);

        let auth_credential_bytes =
            zkgroup::serialize(&credential_response.receive(
                &self.server_public_params,
                self.service_ids.aci(),
                self.service_ids.pni(),
                redemption_time,
            )?);

        let auth_credential =
            AuthCredentialWithPni::new(&auth_credential_bytes)
                .expect("just validated");

        let mut random_bytes = [0u8; 32];
        csprng.fill_bytes(&mut random_bytes);

        let auth_credential_presentation =
            zkgroup::serialize(&auth_credential.present(
                &self.server_public_params,
                &group_secret_params,
                random_bytes,
            ));

        // see simpleapi.rs GroupSecretParams_getPublicParams, everything is bincode encoded
        // across the boundary of Rust/Java
        let username = hex::encode(bincode::serialize(
            &group_secret_params.get_public_params(),
        )?);

        let password = hex::encode(&auth_credential_presentation);

        Ok(HttpAuth { username, password })
    }

    pub async fn fetch_encrypted_group<R: Rng + CryptoRng>(
        &mut self,
        csprng: &mut R,
        master_key_bytes: &[u8],
    ) -> Result<crate::proto::Group, ServiceError> {
        Ok(self
            .fetch_encrypted_group_response(csprng, master_key_bytes)
            .await?
            .group
            .unwrap_or_default())
    }

    // KT (groups fetch): full GroupResponse wrapper, carrying the
    // group-send-endorsements blob alongside the encrypted group.
    pub async fn fetch_encrypted_group_response<R: Rng + CryptoRng>(
        &mut self,
        csprng: &mut R,
        master_key_bytes: &[u8],
    ) -> Result<crate::proto::GroupResponse, ServiceError> {
        let group_master_key = GroupMasterKey::new(
            master_key_bytes
                .try_into()
                .map_err(|_| ServiceError::GroupsV2Error)?,
        );
        let group_secret_params =
            GroupSecretParams::derive_from_master_key(group_master_key);
        let authorization = self
            .get_authorization_for_today(csprng, group_secret_params)
            .await?;
        self.identified_push_service
            .get_group_response(authorization)
            .await
    }

    #[tracing::instrument(
        skip(self, group_secret_params),
        fields(path = %path[..4.min(path.len())]),
    )]
    pub async fn retrieve_avatar(
        &mut self,
        path: &str,
        group_secret_params: GroupSecretParams,
    ) -> Result<Option<Vec<u8>>, ServiceError> {
        let mut encrypted_avatar = self
            .unidentified_websocket
            .retrieve_groups_v2_profile_avatar(path)
            .await?;
        let mut result = Vec::with_capacity(10 * 1024 * 1024);
        encrypted_avatar.read_to_end(&mut result).await?;
        Ok(GroupOperations::new(group_secret_params).decrypt_avatar(&result))
    }

    pub fn decrypt_group_context(
        &self,
        group_context: GroupContextV2,
    ) -> Result<Option<GroupChanges>, GroupDecodingError> {
        match (group_context.master_key, group_context.group_change) {
            (Some(master_key), Some(group_change)) => {
                let master_key_bytes: [u8; 32] = master_key
                    .try_into()
                    .map_err(|_| GroupDecodingError::WrongBlob)?;
                let group_master_key = GroupMasterKey::new(master_key_bytes);
                let group_secret_params =
                    GroupSecretParams::derive_from_master_key(group_master_key);
                let encrypted_group_change =
                    prost::Message::decode(Bytes::from(group_change))?;
                let group_change = GroupOperations::new(group_secret_params)
                    .decrypt_group_change(encrypted_group_change)?;
                Ok(Some(group_change))
            },
            _ => Ok(None),
        }
    }
}

pub fn decrypt_group(
    master_key_bytes: &[u8],
    encrypted_group: crate::proto::Group,
) -> Result<Group, ServiceError> {
    let group_master_key = GroupMasterKey::new(
        master_key_bytes
            .try_into()
            .expect("wrong master key bytes length"),
    );
    let group_secret_params =
        GroupSecretParams::derive_from_master_key(group_master_key);

    Ok(GroupOperations::new(group_secret_params)
        .decrypt_group(encrypted_group)?)
}

fn current_days_seconds() -> (u64, u64) {
    let days_seconds = |date: NaiveDate| {
        date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
            .and_utc()
            .timestamp() as u64
    };

    let today = Utc::now().naive_utc().date();
    let today_plus_7_days = today + Days::new(7);

    (days_seconds(today), days_seconds(today_plus_7_days))
}

#[cfg(test)]
mod live_group_endpoint_fixture {
    // Live capture (2026-10-16 one-shot diagnostic probe) of the storage
    // service answer for the operator's own test group, plus that group's
    // master key. Pins the dual endpoint contract: /v1/groups/ answers a
    // bare Group, while the same bytes fail GroupResponse with tag 0 —
    // the decode failure that moving get_group_response to /v2/groups/
    // fixed.
    use base64::prelude::*;
    use prost::Message;

    use super::decrypt_group;
    use crate::proto::{Group, GroupResponse};

    const BARE_GROUP_B64: &str = "CmEAG0vfAA6uuDEanZKlIJDDY7iKwDqZHkWHs5BouIUxBHbwC+gHzpR7gaK0K88czGLwvIX7xVQOYG/LLhqNeogvLiCj2WfQzq+qNiRMDFyovipxRSWbSejStGZ6B2+esnUxEiddWx0iXAuMU82AuMQcmi/hqqXXZB5g2nG+dVWo3K5/8DyuZRfAvQAqCAgCEAIYBCACMBA6sQEKQQAs2LWlNk6tqGGseY9byQe5lvA6IdeoJ9WrPoAoSGq5F5JfdqM/RyKtydu43h/dL/8ppUG/k770+wKq4SgZ2FEBEAIaQQBMGoHeOQ1GXw0zPCOxmGq3WdWqZcPHswRi4XvmuUk6Twp7LNC/jqAnCFup0MBn4rQY2k3fadCLABOQkzqm4VApOifgK8rE74mKRPJaeYJejUzHZ8nkEM+bh+g9ViO9Z7PqUjnFnqxlGgA6iAEKQQD6Mtet86gGvJOUorgnuJnj8aYX/jSR/M/nRbRZHh2gc+wmXLJScKHy9Knno6KT3XwBLlycZ5AU84ZC9RsjyysiEAEaQQDCXyJ6Syf57TP99Nv3Ndrp/FdfLL7sYk4T/KRp+WHbXoLhJeLsGF7Fi3TAL1Jj2fppVeGC1wn+J/F+bGp+anBjUhCn7J60rpum8IRcTOIL4R8BakoKQQB6InxEM1RxwhkCFg/oWj0MxXKjhmu3GJLpeLJrjBsFCwxKETmt2f5UrovreBWR1UntokvgM2n7XHQnjg5+eqdDEK2wmM6SNA==";

    const MASTER_KEY_B64: &str = "D4DyutMEtvpawXahRoFH/5KPjE/z7ThINwByaAh7Zfw=";

    #[test]
    fn v1_answer_is_a_bare_group_that_decodes_and_decrypts() {
        let bytes = BASE64_STANDARD
            .decode(BARE_GROUP_B64)
            .expect("fixture is valid base64");
        let group = Group::decode(bytes.as_slice())
            .expect("v1 groups answers a bare Group");
        assert_eq!(group.version, 16);
        assert_eq!(group.members.len(), 2);
        let master_key = BASE64_STANDARD
            .decode(MASTER_KEY_B64)
            .expect("master key fixture is valid base64");
        let group = decrypt_group(&master_key, group)
            .expect("bare Group decrypts with its master key");
        assert_eq!(group.title, "gggg");
        assert_eq!(group.members.len(), 2);
        assert!(!group.terminated);
    }

    #[test]
    fn v1_answer_rejected_as_group_response_with_tag_zero() {
        let bytes = BASE64_STANDARD
            .decode(BARE_GROUP_B64)
            .expect("fixture is valid base64");
        let error = GroupResponse::decode(bytes.as_slice())
            .expect_err("bare Group must not decode as GroupResponse");
        assert!(
            error.to_string().contains("invalid tag value: 0"),
            "unexpected decode error: {error}"
        );
    }
}
