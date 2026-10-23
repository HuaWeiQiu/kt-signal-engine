use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::TimeZone;
use futures::{future, AsyncReadExt, Stream, StreamExt};
use libsignal_service::prelude::SessionStoreExt;
use libsignal_service::protocol::{ProtocolAddress, SessionStore};
use libsignal_service::provisioning::ProvisioningSecrets;
use libsignal_service::{
    attachment_cipher::decrypt_in_place,
    cipher,
    configuration::{ServiceConfiguration, SignalServers},
    content::{Content, ContentBody, Metadata},
    groups_v2::{decrypt_group, GroupsManager, InMemoryCredentialsCache},
    messagepipe::{EnvelopeAck, Incoming, MessagePipe, ServiceCredentials},
    prelude::{phonenumber::PhoneNumber, MessageSenderError, ProtobufMessage, Uuid},
    profile_cipher::ProfileCipher,
    proto::{
        data_message::Delete,
        sync_message::{
            self, sticker_pack_operation, Content as SyncContent, StickerPackOperation,
        },
        AttachmentPointer, DataMessage, EditMessage, Envelope, GroupContextV2, NullMessage,
        SyncMessage, Verified,
    },
    protocol::{
        Aci, DeviceId, IdentityKeyStore, PlaintextContent, SenderCertificate, ServiceId,
        ServiceIdKind, Username,
    },
    provisioning::ProvisioningError,
    push_service::{PushService, ServiceError, ServiceIds, DEFAULT_DEVICE_ID},
    receiver::MessageReceiver,
    sender::{AttachmentSpec, AttachmentUploadError},
    sticker_cipher::derive_key,
    unidentified_access::UnidentifiedAccess,
    utils::TryIntoE164,
    websocket::{
        self,
        account::{CapabilitiesUpload, DeviceInfo, WhoAmIResponse},
        SignalWebSocket,
    },
    zkgroup::{
        groups::{GroupMasterKey, GroupSecretParams},
        profiles::ProfileKey,
    },
    AccountManager, Profile, ServiceIdExt,
};
use libsignal_service::{
    libsignal_account_keys::AccountEntropyPool, proto::addressable_message::Author,
};
use rand::rng;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};
use url::Url;

use crate::model::contacts::Contact;
use crate::model::messages::{ContactSync, DecryptionFailure, GroupSyncFailureKind};
use crate::serde::serde_profile_key;
use crate::store::{ContentsStore, Sticker, StickerPack, StickerPackManifest, Store, Thread};
use crate::{model::groups::Group, AvatarBytes, Error, Manager};

pub use crate::model::messages::Received;

type ServiceCipher<S> = cipher::ServiceCipher<S>;
type MessageSender<S> = libsignal_service::prelude::MessageSender<S>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationType {
    Primary,
    Secondary,
}

/// Manager state when the client is registered and can send and receive messages from Signal
#[derive(Clone)]
pub struct Registered {
    pub(crate) identified_push_service: OnceLock<PushService>,
    pub(crate) unidentified_push_service: OnceLock<PushService>,
    // KT (single-connection): the per-manager identified/unidentified
    // websocket slots were removed — websockets now live in the process-global
    // per-account registry (libsignal-service websocket::slot), shared by
    // every Manager of the same account.
    pub(crate) unidentified_sender_certificate: Arc<Mutex<Option<SenderCertificate>>>,
    // KT (group credentials reuse): GroupsManager slot so the credentials
    // cache survives across group operations on this manager (official
    // Desktop keeps group credentials in persistent storage with a 7-day
    // request window; a fresh `InMemoryCredentialsCache` per operation made
    // every group fetch re-request /v1/certificate/auth/group).
    pub(crate) groups_manager_cache: Arc<Mutex<Option<GroupsManager<InMemoryCredentialsCache>>>>,

    pub(crate) data: Arc<RegistrationData>,
}

impl fmt::Debug for Registered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registered").finish_non_exhaustive()
    }
}

impl Registered {
    pub(crate) fn with_data(data: RegistrationData) -> Self {
        Self {
            identified_push_service: Default::default(),
            unidentified_push_service: Default::default(),
            unidentified_sender_certificate: Default::default(),
            groups_manager_cache: Default::default(),
            data: Arc::new(data),
        }
    }

    fn servers(&self) -> SignalServers {
        self.data.signal_servers
    }

    fn service_configuration(&self) -> ServiceConfiguration {
        self.servers().into()
    }

    pub fn device_id(&self) -> DeviceId {
        self.data
            .device_id
            .and_then(|d| d.try_into().ok())
            .unwrap_or(*DEFAULT_DEVICE_ID)
    }

    pub(crate) fn identified_push_service(&self) -> PushService {
        self.identified_push_service
            .get_or_init(|| {
                PushService::new(self.servers(), Some(self.credentials()), crate::USER_AGENT)
            })
            .clone()
    }

    pub(crate) fn credentials(&self) -> ServiceCredentials {
        ServiceCredentials {
            aci: Some(self.data.service_ids.aci),
            pni: Some(self.data.service_ids.pni),
            phonenumber: (&self.data.phone_number)
                .try_into_e164()
                .expect("valid phone number"),
            password: Some(self.data.password.clone()),
            device_id: self.data.device_id.and_then(|d| d.try_into().ok()),
        }
    }
}

/// Registration data like device name, and credentials to connect to Signal
#[derive(Serialize, Deserialize, Clone)]
pub struct RegistrationData {
    pub signal_servers: SignalServers,
    pub device_name: Option<String>,
    pub phone_number: PhoneNumber,
    #[serde(flatten)]
    pub service_ids: ServiceIds,
    pub(crate) password: String,
    pub device_id: Option<u32>,
    pub registration_id: u32,
    #[serde(default)]
    pub pni_registration_id: Option<u32>,
    #[serde(with = "serde_profile_key")]
    pub(crate) profile_key: ProfileKey,
}

impl RegistrationData {
    /// Our own profile key
    pub fn profile_key(&self) -> ProfileKey {
        self.profile_key
    }

    /// The name of the device (if linked as secondary)
    pub fn device_name(&self) -> Option<&str> {
        self.device_name.as_deref()
    }
}

/// KT (send warmup): per-batch outcome of `warm_send_path`. `established`
/// counts recipients a session was newly built for (prekey fetch + bundle
/// processing), `existing` recipients that already had one, `failed`
/// recipients whose establishment errored.
#[derive(Debug, Default, Clone, Copy)]
pub struct SendWarmupStats {
    pub established: usize,
    pub existing: usize,
    pub failed: usize,
}

impl<S: Store> Manager<S, Registered> {
    /// Loads a previously registered account from the implemented [Store].
    ///
    /// Returns a instance of [Manager] you can use to send & receive messages.
    pub async fn load_registered(store: S) -> Result<Self, Error<S::Error>> {
        let registration_data = store
            .load_registration_data()
            .await?
            .ok_or(Error::NotYetRegisteredError)?;

        let registered = Registered::with_data(registration_data);

        if let Some(sender_certificate) = store.sender_certificate().await? {
            registered
                .unidentified_sender_certificate
                .lock()
                .await
                .replace(sender_certificate);
        }

        Ok(Self {
            store,
            state: registered,
        })
    }

    /// Returns a handle to the [Store] implementation.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns a handle on the [RegistrationData].
    pub fn registration_data(&self) -> &RegistrationData {
        &self.state.data
    }

    /// Returns a clone of a cached push service (with credentials).
    ///
    /// If no service is yet cached, it will create and cache one.
    fn identified_push_service(&self) -> PushService {
        self.state.identified_push_service()
    }

    /// Returns a clone of a cached push service (without credentials).
    ///
    /// If no service is yet cached, it will create and cache one.
    fn unidentified_push_service(&self) -> PushService {
        self.state
            .unidentified_push_service
            .get_or_init(|| PushService::new(self.state.servers(), None, crate::USER_AGENT))
            .clone()
    }

    /// Returns the current identified websocket, or creates a new one
    ///
    /// A new one is created if the current websocket is closed, or if there is none yet.
    ///
    /// KT (single-connection): the slot is process-global per account (keyed
    /// by credentials login), shared by every Manager instance of the same
    /// account; a dial terminates the previous connection first (see
    /// `websocket::slot`).
    async fn identified_websocket(
        &self,
        require_unused: bool,
    ) -> Result<SignalWebSocket<websocket::Identified>, Error<S::Error>> {
        let credentials = self.credentials();
        let key = credentials.login();
        websocket::slot::acquire_identified(&key, require_unused, async {
            let headers = &[("X-Signal-Receive-Stories", "false")];
            let mut push_service = self.identified_push_service();
            push_service
                .ws(
                    "/v1/websocket/",
                    "/v1/keepalive",
                    headers,
                    Some(credentials),
                )
                .await
        })
        .await
        .map_err(Error::ServiceError)
    }

    /// Returns the current unidentified websocket, or creates a new one
    ///
    /// A new one is created if the current websocket is closed, or if there is none yet.
    ///
    /// KT (single-connection): process-global slot per account, see
    /// `identified_websocket`.
    async fn unidentified_websocket(
        &self,
    ) -> Result<SignalWebSocket<websocket::Unidentified>, Error<S::Error>> {
        let key = self.credentials().login();
        websocket::slot::acquire_unidentified(&key, async {
            let mut push_service = self.unidentified_push_service();
            push_service
                .ws("/v1/websocket/", "/v1/keepalive", &[], None)
                .await
        })
        .await
        .map_err(Error::ServiceError)
    }

    /// KT (single-connection): close code observed on the account's shared
    /// identified websocket (last server close frame), if a websocket exists.
    /// Lets the application branch on 4409 (connected elsewhere) after the
    /// receive pipe ended.
    pub async fn identified_websocket_close_code(&self) -> Option<u16> {
        websocket::slot::identified_close_code(&self.credentials().login()).await
    }

    /// KT (single-connection): whether the account currently has a live
    /// shared identified websocket. After the receive pipe ended with 4409,
    /// a live websocket means another engine-internal connection took over
    /// (self conflict); none means an external client displaced us.
    pub async fn has_open_identified_websocket(&self) -> bool {
        websocket::slot::identified_open_count(&self.credentials().login()).await > 0
    }

    /// Request the primary device to encrypt & send all of its contacts.
    ///
    /// **Note**: If successful, the contacts are not yet received and stored, but will only be
    /// processed when they're received after polling on the
    pub async fn request_contacts(&mut self) -> Result<(), Error<S::Error>> {
        trace!("requesting contacts sync");
        let sync_message = SyncMessage {
            content: Some(SyncContent::Request(sync_message::Request {
                r#type: Some(sync_message::request::Type::Contacts.into()),
            })),
            ..SyncMessage::with_padding(&mut rand::rng())
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        self.send_message(self.state.data.service_ids.aci(), sync_message, timestamp)
            .await?;

        Ok(())
    }

    // KT (group MRM): pub — the engine's single-request MRM path builds the
    // USMC from this uuid-only certificate (Desktop sendToGroup same face).
    pub async fn sender_certificate(&self) -> Result<SenderCertificate, Error<S::Error>> {
        let needs_renewal = |sender_certificate: Option<&SenderCertificate>| -> bool {
            if sender_certificate.is_none() {
                return true;
            }

            let seconds_since_epoch = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs();

            if let Some(expiration) = sender_certificate.and_then(|s| s.expiration().ok()) {
                expiration.epoch_millis() / 1000 <= seconds_since_epoch + 600
            } else {
                true
            }
        };

        let mut unidentified_sender_certificate =
            self.state.unidentified_sender_certificate.lock().await;
        if needs_renewal(unidentified_sender_certificate.as_ref()) {
            // KT (send perf): the network arm only — a cache hit stays free.
            let cert_started = std::time::Instant::now();
            let sender_certificate = self
                .identified_websocket(false)
                .await?
                .get_uuid_only_sender_certificate()
                .await?;
            libsignal_service::perf_hook::emit(
                libsignal_service::perf_hook::keys::CERT_FETCH,
                cert_started.elapsed().as_micros() as u64,
            );
            self.store
                .save_sender_certificate(&sender_certificate)
                .await?;
            unidentified_sender_certificate.replace(sender_certificate);
        }

        Ok(unidentified_sender_certificate
            .clone()
            .expect("logic error"))
    }

    /// Sender certificate renewal, for background pre-rotation (KT): renews
    /// and persists per the `needs_renewal` threshold (600s); a fresh cache
    /// makes this a pure read with no network round trip. The send path never
    /// depends on this succeeding — see the identified fallback in
    /// `unidentified_access_or_identified`.
    pub async fn refresh_sender_certificate(
        &mut self,
    ) -> Result<SenderCertificate, Error<S::Error>> {
        self.sender_certificate().await
    }

    async fn account_entropy_pool(&self) -> Result<Option<AccountEntropyPool>, Error<S::Error>> {
        let from_store = self.store().fetch_account_entropy_pool().await?;

        if let Some(key) = from_store {
            Ok(Some(key))
        } else if self.registration_type() == RegistrationType::Primary {
            let key = AccountEntropyPool::generate(&mut rand::rng());
            self.store().store_account_entropy_pool(Some(&key)).await?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    pub async fn submit_recaptcha_challenge(
        &self,
        token: &str,
        captcha: &str,
    ) -> Result<(), Error<S::Error>> {
        let mut account_manager = AccountManager::new(
            self.identified_push_service(),
            self.identified_websocket(false).await?,
            None,
        );
        account_manager
            .submit_recaptcha_challenge(token, captcha)
            .await?;
        Ok(())
    }

    /// Fetches basic information on the registered device.
    pub async fn whoami(&self) -> Result<WhoAmIResponse, Error<S::Error>> {
        Ok(self.identified_websocket(false).await?.whoami().await?)
    }

    /// Registers this device's protocol capabilities with the server
    /// (`PUT /v1/devices/capabilities`). Linked clients re-register on
    /// every startup.
    pub async fn register_device_capabilities(
        &self,
        capabilities: CapabilitiesUpload,
    ) -> Result<(), Error<S::Error>> {
        self.identified_websocket(false)
            .await?
            .register_capabilities(capabilities)
            .await?;
        Ok(())
    }

    pub fn device_id(&self) -> DeviceId {
        self.state.device_id()
    }

    /// Fetches the profile (name, about, status emoji) of the registered user.
    pub async fn retrieve_profile(&mut self) -> Result<Profile, Error<S::Error>> {
        self.retrieve_profile_by_uuid(self.state.data.service_ids.aci, self.state.data.profile_key)
            .await
    }

    /// Fetches the profile of the provided user by UUID and profile key.
    pub async fn retrieve_profile_by_uuid(
        &mut self,
        aci: impl Into<Aci>,
        profile_key: ProfileKey,
    ) -> Result<Profile, Error<S::Error>> {
        let aci = aci.into();

        // Check if profile is cached.
        // TODO: Create a migration in the store removing all profiles.
        // TODO: Is there some way to know if this is outdated?
        if let Some(profile) = self
            .store
            .profile(aci.into(), profile_key)
            .await
            .ok()
            .flatten()
        {
            return Ok(profile);
        }

        let mut account_manager = AccountManager::new(
            self.identified_push_service(),
            self.identified_websocket(false).await?,
            Some(profile_key),
        );

        let profile = account_manager.retrieve_profile(aci).await?;

        let _ = self
            .store
            .save_profile(aci.into(), profile_key, profile.clone())
            .await;
        Ok(profile)
    }

    /// Updates the user's profile information.
    pub async fn update_profile(
        &mut self,
        name: libsignal_service::profile_name::ProfileName<String>,
        about: Option<String>,
        emoji: Option<String>,
    ) -> Result<(), Error<S::Error>> {
        let aci = self.state.data.service_ids.aci();
        let mut account_manager = AccountManager::new(
            self.identified_push_service(),
            self.identified_websocket(false).await?,
            Some(self.state.data.profile_key),
        );

        account_manager
            .upload_versioned_profile_without_avatar::<_, String>(
                aci,
                name,
                about,
                emoji,
                true, // retain_avatar
                &mut rand::rng(),
            )
            .await?;

        // Retrieve and save locally so we have the updated version
        let profile = account_manager.retrieve_profile(aci).await?;
        let _ = self
            .store
            .save_profile(aci.into(), self.state.data.profile_key, profile)
            .await;

        Ok(())
    }

    pub async fn retrieve_group_avatar(
        &mut self,
        context: GroupContextV2,
    ) -> Result<Option<AvatarBytes>, Error<S::Error>> {
        let master_key_bytes = context
            .master_key()
            .try_into()
            .expect("Master key bytes to be of size 32.");

        // Check if group avatar is cached.
        // TODO: Is there some way to know if this is outdated?
        if let Some(avatar) = self
            .store
            .group_avatar(master_key_bytes)
            .await
            .ok()
            .flatten()
        {
            return Ok(Some(avatar));
        }

        let mut gm = Box::pin(self.groups_manager()).await?;
        // KT (group fetch degradation): the avatar path stays fail-open — a
        // structured group fetch failure falls back to the stored group.
        let upserted = match upsert_group(
            &self.store,
            &mut gm,
            context.master_key(),
            &context.revision(),
        )
        .await
        {
            Ok(upserted) => upserted,
            Err(error @ (Error::GroupSyncFailed { .. } | Error::GroupGone)) => {
                warn!(%error, "group fetch failed; falling back to stored group for avatar");
                self.store.group(master_key_bytes).await?
            }
            Err(error) => return Err(error),
        };
        let Some(group) = upserted else {
            return Ok(None);
        };

        // Empty path means no avatar was set.
        if group.avatar.is_empty() {
            return Ok(None);
        }

        let avatar = gm
            .retrieve_avatar(
                &group.avatar,
                GroupSecretParams::derive_from_master_key(GroupMasterKey::new(master_key_bytes)),
            )
            .await?;
        if let Some(avatar) = &avatar {
            let _ = self.store.save_group_avatar(master_key_bytes, avatar).await;
        }
        Ok(avatar)
    }

    pub async fn retrieve_profile_avatar_by_uuid(
        &mut self,
        uuid: Uuid,
        profile_key: ProfileKey,
    ) -> Result<Option<AvatarBytes>, Error<S::Error>> {
        // Check if profile avatar is cached.
        // TODO: Is there some way to know if this is outdated?
        if let Some(avatar) = self
            .store
            .profile_avatar(uuid, profile_key)
            .await
            .ok()
            .flatten()
        {
            return Ok(Some(avatar));
        }

        let profile =
            if let Some(profile) = self.store.profile(uuid, profile_key).await.ok().flatten() {
                profile
            } else {
                self.retrieve_profile_by_uuid(uuid, profile_key).await?
            };

        let Some(avatar) = profile.avatar.as_ref() else {
            return Ok(None);
        };

        let mut websocket = self.unidentified_websocket().await?;

        let mut avatar_stream = websocket.retrieve_profile_avatar(avatar).await?;
        // 10MB is what Signal Android allocates
        let mut contents = Vec::with_capacity(10 * 1024 * 1024);
        let len = avatar_stream.read_to_end(&mut contents).await?;
        contents.truncate(len);

        let cipher = ProfileCipher::new(profile_key);

        let avatar = cipher.decrypt_avatar(&contents)?;
        let _ = self
            .store
            .save_profile_avatar(uuid, profile_key, &avatar)
            .await;
        Ok(Some(avatar))
    }

    /// KT (group credentials reuse): shared GroupsManager behind a per-manager
    /// slot — the credentials cache inside it persists across calls, matching
    /// the official groupCredentialFetcher window (one
    /// /v1/certificate/auth/group request covering [today, today+7d) serves
    /// subsequent operations until the window rolls over).
    async fn groups_manager(
        &self,
    ) -> Result<
        tokio::sync::MappedMutexGuard<'_, GroupsManager<InMemoryCredentialsCache>>,
        Error<S::Error>,
    > {
        let mut slot = self.state.groups_manager_cache.lock().await;
        if slot.is_none() {
            *slot = Some(Box::pin(self.build_groups_manager()).await?);
        }
        Ok(tokio::sync::MutexGuard::map(
            slot,
            |slot: &mut Option<GroupsManager<InMemoryCredentialsCache>>| {
                slot.as_mut().expect("groups manager slot just filled")
            },
        ))
    }

    async fn build_groups_manager(
        &self,
    ) -> Result<GroupsManager<InMemoryCredentialsCache>, Error<S::Error>> {
        let service_configuration = self.state.service_configuration();
        let server_public_params = service_configuration.zkgroup_server_public_params;

        let groups_credentials_cache = InMemoryCredentialsCache::default();
        let groups_manager = GroupsManager::new(
            self.state.data.service_ids.clone(),
            self.identified_push_service(),
            self.unidentified_websocket().await?,
            groups_credentials_cache,
            server_public_params,
        );

        Ok(groups_manager)
    }

    /// KT (group MRM): fetch a group together with its
    /// group-send-endorsements blob, saving the decrypted group exactly
    /// like `upsert_group`. A fetch failure falls back to the cached
    /// group, with the endorsements reported as `None`.
    pub async fn fetch_group_with_endorsements(
        &mut self,
        master_key_bytes: &[u8],
    ) -> Result<Option<(Group, Option<Vec<u8>>)>, Error<S::Error>> {
        let mut groups_manager = Box::pin(self.groups_manager()).await?;
        // KT (group fetch degradation): fetch+decrypt is one retry unit.
        // Decode-class failures retry once in-call, then surface as a
        // structured error instead of silently serving a stale group.
        let mut endorsements = None;
        let mut attempts = 0;
        loop {
            attempts += 1;
            let mut failure = None;
            match groups_manager
                .fetch_encrypted_group_response(&mut rand::rng(), master_key_bytes)
                .await
            {
                Ok(response) => {
                    endorsements = Some(response.group_send_endorsements_response.clone())
                        .filter(|bytes| !bytes.is_empty());
                    if let Some(encrypted_group) = response.group {
                        match decrypt_group(master_key_bytes, encrypted_group) {
                            Ok(group) => {
                                if let Err(error) = self
                                    .store
                                    .save_group(master_key_bytes.try_into()?, group.clone())
                                    .await
                                {
                                    error!(%error, "failed to save group");
                                }
                            }
                            // Decrypt failures are decode-class by construction
                            // (GroupsV2DecryptionError via GroupDecodingError).
                            Err(error) => failure = Some(error),
                        }
                    }
                }
                Err(error) if group_fetch_gone(&error) => return Err(Error::GroupGone),
                Err(error) if group_fetch_decode_class(&error) => failure = Some(error),
                Err(error) => {
                    warn!(%error, "failed to fetch encrypted group with endorsements")
                }
            }
            match failure {
                None => break,
                Some(error) if attempts < GROUP_FETCH_MAX_ATTEMPTS => {
                    warn!(%error, "decode-class failure, retrying once");
                }
                Some(_) => return Err(Error::GroupSyncFailed { attempts }),
            }
        }
        Ok(self
            .store
            .group(master_key_bytes.try_into()?)
            .await?
            .map(|group| (group, endorsements)))
    }

    // KT (receive batching): official Desktop collects envelopes in a 75ms
    // window capped at 30 messages, then persists the batch in a single
    // transaction (MessageReceiver.preload.ts:359-366, 1003-1123).
    const RECEIVE_BATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(75);
    const RECEIVE_BATCH_MAX: usize = 30;

    /// Starts receiving and storing messages.
    ///
    /// As a client, it is heavily recommended to process incoming messages and wait for the `Received::QueueEmpty` messages
    /// until giving the ability for users to send messages. That way, all possible updates (sessions, profile keys, sender keys)
    /// are processed _before_ trying to encrypt and send messages, which might get rejected by recipients otherwise.
    ///
    /// Returns a [futures::Stream] of messages to consume. Messages will also be stored by the implementation of the [Store].
    pub async fn receive_messages(
        &mut self,
    ) -> Result<impl Stream<Item = Received>, Error<S::Error>> {
        struct StreamState<Receiver, Store, AciStore, PniStore> {
            store: Store,
            identified_websocket: SignalWebSocket<websocket::Identified>,
            unidentified_websocket: SignalWebSocket<websocket::Unidentified>,
            encrypted_messages: Receiver,
            message_receiver: MessageReceiver,
            service_cipher_aci: ServiceCipher<AciStore>,
            service_cipher_pni: ServiceCipher<PniStore>,
            groups_manager: GroupsManager<InMemoryCredentialsCache>,
            service_ids: ServiceIds,
            message_sender: MessageSender<AciStore>,
            account_entropy_pool: Option<AccountEntropyPool>,
            registration_type: RegistrationType,
            // KT (receive batching): processed-but-not-yet-yielded items —
            // the stream yields them (in arrival order) before pulling new
            // envelopes, so the consumer-visible sequence is unchanged.
            pending: std::collections::VecDeque<Received>,
            // KT (envelope retention): envelopes whose open failed below the
            // retry bound, awaiting a later batch window. In-memory only —
            // presage has no upstream unprocessed store; the unacked handles
            // keep the server queue as the durability backstop.
            failed: std::collections::VecDeque<WindowEnvelope>,
            retry_due: Option<tokio::time::Instant>,
            // KT (receive storm): how many envelopes were dropped as
            // already-processed duplicates on this stream — first one is
            // logged at warn so the incident leaves one line behind, the
            // rest stay at debug (a flood must not become a log flood).
            duplicate_drops: u64,
            end_of_stream: bool,
        }

        let identified_push_service = self.identified_push_service();
        // NB: here, we initialise a *fresh* Signal websocket, which means any other use of the previous one will go into nirvana
        let identified_websocket = self.identified_websocket(true).await?;

        let mut account_manager = AccountManager::new(
            identified_push_service.clone(),
            identified_websocket.clone(),
            None,
        );

        let store_inner = self.store.clone();

        // We make a task to refresh pre keys as needed that will only yield a
        // value if the operation fails (stop signal).
        //
        // This is necessary because in this context, we can't do the classic tokio::spawn with a
        // oneshot::channel() or CancellationToken because of !Send constraints in the Store.
        //
        // KT: unlike upstream, this deliberately does NOT put account
        // attributes. Linked devices must not PUT attributes: the server
        // merges the payload with the primary's real settings and
        // `discoverable_by_phone_number` hard-coded `true` here would
        // silently flip the account's discoverability privacy setting.
        // Official Signal-Desktop linked devices never PUT attributes.
        // KT (handover race, §29.6): 兜底重试组件 —— 任务是 `async move`，
        // 无法借用 `self`，提前捕获拨号组件（消息管道还要用
        // `identified_push_service`，这里只进 clone）。
        let gate_push_service = identified_push_service.clone();
        let gate_credentials = self.credentials();
        let refresh_registration_task = async move {
            // KT (handover race, §29.6): 初次注册用现成 account_manager
            // （与消息管道共享刚拨的连接）；在途请求被后续 handover 杀掉时
            // （WsClosing）兜底重试：等 500ms 让 handover 稳定，经 slot
            // （require_unused=false，复用活连接/只顶死连接，不杀消息管道）
            // 拿连接重发，上限 2 次。pre-keys 上传幂等，重发安全；重试耗尽
            // 仍只落日志（消息流不因此停止）。语义同
            // `register_with_ws_handover_retry`（此处因 `&mut
            // account_manager` 借用进 future 的所有权结构而手写展开）。
            let mut result = register_pre_keys(&store_inner, &mut account_manager).await;
            let mut retries_done = 0usize;
            loop {
                match result {
                    Ok(()) => break,
                    Err(error)
                        if is_ws_handover_kill(&error)
                            && retries_done < PRE_KEYS_HANDOVER_RETRIES =>
                    {
                        retries_done += 1;
                        warn!(
                            %error,
                            attempt = retries_done,
                            retries_left = PRE_KEYS_HANDOVER_RETRIES - retries_done,
                            "pre-keys registration killed by ws handover; retrying on a fresh connection"
                        );
                        tokio::time::sleep(PRE_KEYS_HANDOVER_RETRY_DELAY).await;
                        result = match dial_pre_keys_connection(&gate_push_service, &gate_credentials)
                            .await
                        {
                            Ok(mut fresh) => register_pre_keys(&store_inner, &mut fresh).await,
                            Err(dial_error) => Err(Error::ServiceError(dial_error)),
                        };
                    }
                    Err(error) => {
                        error!(%error, "failed to register pre-keys, this is problematic and should never happen!");
                        break;
                    }
                }
            }

            // Never return, which keeps the messages stream alive.
            future::pending::<()>().await
        };

        let encrypted_messages = MessagePipe::from_socket(identified_websocket.clone());

        let init = StreamState {
            store: self.store.clone(),
            identified_websocket,
            unidentified_websocket: self.unidentified_websocket().await?,
            encrypted_messages: Box::pin(encrypted_messages.stream()),
            message_receiver: MessageReceiver::new(identified_push_service),
            service_cipher_aci: self.new_service_cipher_aci(),
            service_cipher_pni: self.new_service_cipher_pni(),
            groups_manager: Box::pin(self.build_groups_manager()).await?,
            service_ids: self.state.data.service_ids.clone(),
            message_sender: self.new_message_sender().await?,
            account_entropy_pool: self.account_entropy_pool().await?,
            registration_type: self.registration_type(),
            pending: Default::default(),
            failed: Default::default(),
            retry_due: None,
            duplicate_drops: 0,
            end_of_stream: false,
        };

        debug!("starting to consume incoming message stream");

        let incoming_messages_stream = futures::stream::unfold(init, |mut state| {
            async move {
                loop {
                    // KT (receive batching): emit already-processed items in
                    // arrival order before pulling new envelopes — the
                    // consumer-visible sequence is identical to the unbatched
                    // stream (per-envelope projection events, per-item ack).
                    if let Some(item) = state.pending.pop_front() {
                        return Some((item, state));
                    }
                    if state.end_of_stream {
                        return None;
                    }
                    // KT (envelope retention): with held envelopes, the retry
                    // timer races fresh traffic — fresh envelopes keep the
                    // normal batching path (and preempt the timer); a quiet
                    // stream retries on the timer so held envelopes cannot
                    // sit unacked forever.
                    enum Pull {
                        RetryTimer,
                        Item(Option<Result<Incoming, libsignal_service::prelude::ServiceError>>),
                    }
                    let pull = if state.failed.is_empty() {
                        Pull::Item(state.encrypted_messages.next().await)
                    } else {
                        let due = state
                            .retry_due
                            .unwrap_or_else(|| tokio::time::Instant::now() + OPEN_RETRY_DELAY);
                        let timer = std::pin::pin!(tokio::time::sleep_until(due));
                        let next = std::pin::pin!(state.encrypted_messages.next());
                        match futures::future::select(timer, next).await {
                            futures::future::Either::Left(_) => Pull::RetryTimer,
                            futures::future::Either::Right((item, _timer)) => Pull::Item(item),
                        }
                    };
                    let (window, queue_empty_seen, window_end) = match pull {
                        Pull::RetryTimer => {
                            state.retry_due = None;
                            // KT (receive storm): the retry window carries the
                            // original `attempts` forward. Re-entering with
                            // `attempts: 0` (as a fresh arrival would) made the
                            // bound unreachable — every retry looked like the
                            // first one, so a deterministic open failure was
                            // retried forever.
                            let retry_window: Vec<WindowEnvelope> =
                                state.failed.drain(..).collect();
                            (retry_window, false, BatchWindowEnd::WindowClosed)
                        }
                        Pull::Item(Some(Ok(Incoming::Envelope(envelope, ack)))) => {
                            // KT (receive batching): collect the official
                            // 75ms/30 window (MessageReceiver.preload.ts
                            // :359-366). Decryption stays sequential — the
                            // cipher takes &mut self with a shared CSPRNG on
                            // a !Send stream — so the batch wins come from
                            // the merged single-transaction save below.
                            let (incoming, end) = collect_window(
                                &mut state.encrypted_messages,
                                Incoming::Envelope(envelope, ack),
                                Self::RECEIVE_BATCH_WINDOW,
                                Self::RECEIVE_BATCH_MAX,
                                |item| matches!(item, Incoming::QueueEmpty),
                            )
                            .await;
                            let mut window: Vec<WindowEnvelope> =
                                Vec::with_capacity(incoming.len());
                            let mut queue_empty_seen = false;
                            for item in incoming {
                                match item {
                                    Incoming::Envelope(envelope, ack) => {
                                        window.push(WindowEnvelope {
                                            envelope,
                                            ack,
                                            attempts: 0,
                                        });
                                    }
                                    Incoming::QueueEmpty => queue_empty_seen = true,
                                }
                            }
                            (window, queue_empty_seen, end)
                        }
                        Pull::Item(Some(Ok(Incoming::QueueEmpty))) => {
                            debug!("got empty queue");
                            request_keys_from_primary_if_needed(
                                &state.account_entropy_pool,
                                &state.message_sender,
                            );
                            return Some((Received::QueueEmpty, state));
                        }
                        Pull::Item(Some(Err(error))) => {
                            error!(%error, "unexpected error in message receiving loop");
                            continue;
                        }
                        Pull::Item(None) => return None,
                    };
                    if window_end == BatchWindowEnd::EndOfStream {
                        state.end_of_stream = true;
                    }

                    // Per-envelope classification: contents queued
                    // for the batch save, ready items carried as-is.
                    // Save-before-yield ordering is preserved by
                    // flushing the save before emitting the slots.
                    let mut to_save: Vec<Content> = Vec::with_capacity(window.len());
                    enum BatchSlot {
                        /// Materializes to `Received::Content` after
                        /// the flush; `index` walks `to_save` in
                        /// ascending order.
                        Content {
                            index: usize,
                            ack: EnvelopeAck,
                        },
                        Ready(Received),
                    }
                    let mut slots: Vec<BatchSlot> = Vec::with_capacity(window.len());
                    let mut batch_seen: std::collections::HashSet<(String, u64, Option<Thread>)> =
                        Default::default();

                    for WindowEnvelope {
                        envelope,
                        ack,
                        attempts,
                    } in window
                    {
                        // KT (DERM): retry-receipt material captured before the
                        // envelope is consumed by decryption; only spent on the
                        // failure path below.
                        let retry_material = RetryReceiptMaterial::of(&envelope);
                        // KT (envelope retention): keep the wire envelope so a
                        // failed open can re-enter a later window; the copy is
                        // dropped with the decrypt result on success.
                        let wire_envelope = envelope.clone();
                        let envelope = {
                            // the permit is released at the end of the block (impl Drop)
                            match envelope.parse_destination_service_id() {
                                None | Some(ServiceId::Aci(_)) => {
                                    state
                                        .service_cipher_aci
                                        .open_envelope(envelope, &mut rand::rng())
                                        .await
                                }
                                Some(ServiceId::Pni(pni)) => {
                                    if pni == state.service_ids.pni()
                                        && envelope.parse_source_service_id().is_none()
                                    {
                                        warn!("Got a sealed sender message to our PNI? Invalid message, ignoring.");
                                        ack.ack();
                                        continue;
                                    }
                                    state
                                        .service_cipher_pni
                                        .open_envelope(envelope, &mut rng())
                                        .await
                                }
                            }
                        };
                        // KT (identity change): the decryption above may
                        // have replaced a stored identity (session
                        // re-established on a changed key). Drain the
                        // change log once per envelope; matched against
                        // the sender below.
                        let identity_changes = state.store.take_identity_changes();
                        match envelope {
                            Ok(Some(content)) => {
                                if let ContentBody::DecryptionErrorMessage(e) = &content.body {
                                    error!(
                                        error = ?e,
                                        "got error decrypting a message"
                                    );
                                    let timestamp = u64::try_from(
                                        content.metadata.client_timestamp.timestamp_millis(),
                                    )
                                    .ok();
                                    // KT (DERM): the embedded timestamp names the
                                    // peer's original message that failed here;
                                    // device ids/ratchet_key feed the
                                    // archive-on-match leg (official
                                    // RetryRequestEvent:
                                    // MessageReceiver.preload.ts:2690-2698).
                                    let failure = match e.timestamp {
                                        Some(ts) => DecryptionFailure::RetryRequest {
                                            original_ts: ts,
                                            ratchet_key: e.ratchet_key.clone(),
                                            derm_device_id: e.device_id.unwrap_or_default(),
                                            requester_device: content.metadata.sender_device.into(),
                                        },
                                        // DERM without a timestamp names no
                                        // original message; nothing to act
                                        // on — ack and drop (no projection).
                                        None => {
                                            ack.ack();
                                            continue;
                                        }
                                    };
                                    slots.push(BatchSlot::Ready(Received::DecryptionError(
                                        content.metadata.sender,
                                        timestamp,
                                        Some(failure),
                                        ack,
                                    )));
                                    continue;
                                }

                                if let ContentBody::SynchronizeMessage(SyncMessage {
                                    content: Some(SyncContent::Request(request)),
                                    ..
                                }) = &content.body
                                {
                                    use libsignal_service::content::sync_message::request::Type as RequestType;

                                    // KT (sync request stop-answer, P9): linked
                                    // devices only REQUEST syncs, they never
                                    // answer them — official Desktop neither
                                    // sends answers on request
                                    // (ts/textsecure/syncRequests.preload.ts:11-33
                                    // sends the three requests) nor processes
                                    // incoming requests beyond logging
                                    // (ts/textsecure/MessageReceiver.preload.ts:3070-3074
                                    // removes the envelope from cache). The
                                    // former Blocked arm replied with an EMPTY
                                    // block table, which a consumer applies as
                                    // a set-difference → unblock-everything
                                    // (official _handleBlocked diff semantics,
                                    // MessageReceiver.preload.ts:3988-4044 and
                                    // :4207-4230); the Keys arm over-shared
                                    // account entropy material on demand. All
                                    // arms now log and skip — no reply of any
                                    // table.
                                    match request.r#type() {
                                        RequestType::Contacts => {
                                            warn!("ignoring contacts sync request: linked devices only request syncs, never answer them (official syncRequests.preload.ts:11-33 / MessageReceiver.preload.ts:3070-3074)");
                                        }
                                        RequestType::Keys => {
                                            warn!("ignoring keys sync request: answering would over-share account entropy material; official clients never answer sync requests on a linked device (syncRequests.preload.ts:11-33)");
                                        }
                                        RequestType::Blocked => {
                                            warn!("ignoring blocked sync request: an empty-blocks reply would be consumed as unblock-everything (official _handleBlocked diff semantics, MessageReceiver.preload.ts:3988-4044); official linked devices never answer (syncRequests.preload.ts:11-33)");
                                        }
                                        t => {
                                            info!(type = ?t, "Got sync request of currently unhandled type")
                                        }
                                    }
                                }

                                // contacts synchronization sent from the primary device (happens after linking, or on demand)
                                if let ContentBody::SynchronizeMessage(SyncMessage {
                                    content: Some(SyncContent::Contacts(contacts)),
                                    ..
                                }) = &content.body
                                {
                                    // KT (P15a): parsed records ride along
                                    // for the engine projection; save loop
                                    // keeps the upstream break-on-error face.
                                    let complete = contacts.complete.unwrap_or(false);
                                    match state.message_receiver.retrieve_contacts(contacts).await {
                                        Ok(contacts) => {
                                            info!("saving contacts");
                                            let mut saved = Vec::new();
                                            for contact in contacts.filter_map(Result::ok) {
                                                let contact: Contact = contact.into();
                                                if let Err(error) =
                                                    state.store.save_contact(&contact).await
                                                {
                                                    warn!(%error, "failed to save contacts");
                                                    break;
                                                }
                                                saved.push(contact);
                                            }
                                            slots.push(BatchSlot::Ready(Received::Contacts(
                                                ContactSync {
                                                    content: Box::new(content.clone()),
                                                    complete,
                                                    contacts: saved,
                                                },
                                                ack,
                                            )));
                                            continue;
                                        }
                                        Err(error) => {
                                            warn!(%error, "failed to retrieve contacts");
                                        }
                                    }

                                    slots.push(BatchSlot::Ready(Received::Contacts(
                                        ContactSync {
                                            content: Box::new(content.clone()),
                                            complete,
                                            contacts: Vec::new(),
                                        },
                                        ack,
                                    )));
                                    continue;
                                }

                                // sticker pack operations
                                if let ContentBody::SynchronizeMessage(SyncMessage {
                                    sticker_pack_operation,
                                    ..
                                }) = &content.body
                                {
                                    for operation in sticker_pack_operation {
                                        match operation.r#type() {
                                            sticker_pack_operation::Type::Install => {
                                                let store = state.store.clone();
                                                let unidentified_websocket =
                                                    state.unidentified_websocket.clone();
                                                let operation = operation.clone();

                                                // download stickers in the background
                                                tokio::spawn(async move {
                                                    match download_sticker_pack(
                                                        store,
                                                        unidentified_websocket,
                                                        &operation,
                                                    )
                                                    .await
                                                    {
                                                        Ok(sticker_pack) => {
                                                            debug!(
                                                                "downloaded sticker pack: {} made by {}",
                                                                sticker_pack.manifest.title,
                                                                sticker_pack.manifest.author
                                                            );
                                                        }
                                                        Err(error) => error!(
                                                            %error,
                                                            "failed to download sticker pack"
                                                        ),
                                                    }
                                                });
                                            }
                                            sticker_pack_operation::Type::Remove => {
                                                match state
                                                    .store
                                                    .remove_sticker_pack(operation.pack_id())
                                                    .await
                                                {
                                                    Ok(was_present) => {
                                                        debug!(was_present, "removed stick pack")
                                                    }
                                                    Err(error) => {
                                                        error!(
                                                            %error,
                                                            "failed to remove sticker pack"
                                                        )
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                // key synchronization sent from the primary device
                                if let ContentBody::SynchronizeMessage(SyncMessage {
                                    content: Some(SyncContent::Keys(keys)),
                                    ..
                                }) = &content.body
                                {
                                    debug!("received key sync message");
                                    if state.registration_type == RegistrationType::Primary {
                                        warn!("received a key sync message as a primary device; ignoring")
                                    } else {
                                        match keys
                                            .account_entropy_pool
                                            .as_ref()
                                            .map(|s| AccountEntropyPool::from_str(s))
                                        {
                                            Some(Ok(aep)) => {
                                                if let Err(error) = state
                                                    .store
                                                    .store_account_entropy_pool(Some(&aep))
                                                    .await
                                                {
                                                    error!(%error, "failed to store account entropy pool");
                                                }
                                                state.account_entropy_pool = Some(aep);
                                            }
                                            Some(Err(error)) => {
                                                warn!(%error, "cannot convert account entropy pool from string")
                                            }
                                            None => {}
                                        }
                                    }
                                }

                                // group update
                                if let ContentBody::DataMessage(DataMessage {
                                    group_v2:
                                        Some(GroupContextV2 {
                                            master_key: Some(master_key_bytes),
                                            revision: Some(revision),
                                            ..
                                        }),
                                    ..
                                })
                                | ContentBody::SynchronizeMessage(SyncMessage {
                                    content:
                                        Some(SyncContent::Sent(sync_message::Sent {
                                            message:
                                                Some(DataMessage {
                                                    group_v2:
                                                        Some(GroupContextV2 {
                                                            master_key: Some(master_key_bytes),
                                                            revision: Some(revision),
                                                            ..
                                                        }),
                                                    ..
                                                }),
                                            ..
                                        })),
                                    ..
                                }) = &content.body
                                {
                                    // there's two things to implement: the group metadata (fetched from HTTP API)
                                    // and the group changes, which are part of the protobuf messages
                                    // this means we kinda need our own internal representation of groups inside of presage?
                                    // KT (group fetch degradation): structured fetch
                                    // failures surface as a dedicated Received; the
                                    // envelope itself still projects below and the
                                    // single ack stays with its Content slot.
                                    let mut push_sync_failure = |kind, attempts| {
                                        if let Ok(master_key) =
                                            <[u8; 32]>::try_from(master_key_bytes.as_slice())
                                        {
                                            slots.push(BatchSlot::Ready(
                                                Received::GroupSyncFailed {
                                                    master_key,
                                                    kind,
                                                    attempts,
                                                },
                                            ));
                                        }
                                    };
                                    match upsert_group(
                                        &state.store,
                                        &mut state.groups_manager,
                                        master_key_bytes,
                                        revision,
                                    )
                                    .await
                                    {
                                        Ok(Some(group)) => trace!(?group, "upserted group"),
                                        Ok(None) => {}
                                        Err(Error::GroupSyncFailed { attempts }) => {
                                            push_sync_failure(
                                                GroupSyncFailureKind::Decode,
                                                attempts,
                                            )
                                        }
                                        Err(Error::GroupGone) => {
                                            push_sync_failure(GroupSyncFailureKind::Gone, 1)
                                        }
                                        Err(error) => warn!(%error, "failed to upsert group"),
                                    }
                                }

                                // Redelivery gate (KT): a (sender, client_timestamp) row
                                // in the same thread means this envelope was stored and
                                // projected once; re-store/re-project would duplicate
                                // downstream notifications. Ack the handle instead.
                                // KT: the gate is thread-scoped — the same sender may
                                // legitimately emit two messages with one client
                                // timestamp into two different groups; only a row in
                                // the very thread this content belongs to is a
                                // redelivery. An underivable thread keeps the
                                // conservative global suppression.
                                let incoming_thread = Thread::try_from(&content).ok();
                                let timestamp = u64::try_from(
                                    content.metadata.client_timestamp.timestamp_millis(),
                                )
                                .ok();
                                let redelivered = {
                                    match timestamp {
                                        Some(timestamp) => {
                                            let stored = state
                                                .store
                                                .thread_for_sender_and_timestamp(
                                                    &content.metadata.sender,
                                                    timestamp,
                                                )
                                                .await
                                                .ok()
                                                .flatten();
                                            is_thread_scoped_redelivery(
                                                stored,
                                                incoming_thread.clone(),
                                            )
                                        }
                                        None => false,
                                    }
                                };
                                // KT (receive batching): the store lookup above
                                // runs before this batch is saved, so a duplicate
                                // inside the same window would slip past it — the
                                // in-batch set closes that gap (keyed per thread).
                                let batch_key = timestamp.map(|timestamp| {
                                    (
                                        content.metadata.sender.service_id_string(),
                                        timestamp,
                                        incoming_thread.clone(),
                                    )
                                });
                                if redelivered
                                    || batch_key
                                        .as_ref()
                                        .is_some_and(|key| batch_seen.contains(key))
                                {
                                    slots.push(BatchSlot::Ready(Received::Duplicate(ack)));
                                    continue;
                                }
                                if let Some(key) = batch_key {
                                    batch_seen.insert(key);
                                }

                                // KT (receive batching): contents are queued
                                // for the single-transaction batch save below;
                                // nothing is written per-message anymore.
                                // Save-before-yield ordering is kept because the
                                // flush runs before any slot is emitted.
                                let identity_changed = identity_changes.iter().any(|name| {
                                    *name == content.metadata.sender.service_id_string()
                                });
                                if identity_changed {
                                    // KT (identity change): this envelope
                                    // decrypted on a freshly replaced stored
                                    // identity — surface the change and let
                                    // the consumer project the message from
                                    // the same item (single ack contract).
                                    let sender = content.metadata.sender;
                                    to_save.push(content.clone());
                                    slots.push(BatchSlot::Ready(Received::IdentityChange(
                                        sender,
                                        Box::new(content),
                                        ack,
                                    )));
                                } else {
                                    to_save.push(content);
                                    slots.push(BatchSlot::Content {
                                        index: to_save.len() - 1,
                                        ack,
                                    });
                                }
                            }
                            Ok(None) => {
                                debug!("empty envelope, message will be skipped!");
                                ack.ack();
                            }
                            Err(error) => {
                                let failure = retry_material.and_then(|m| m.into_failure());
                                // KT (receive storm): `DuplicatedMessage` means
                                // the message key was already consumed, so no
                                // later attempt can open this envelope. Its
                                // meaning is "already handled": official
                                // Desktop removes the envelope from its cache
                                // on that error (MessageReceiver.preload.ts
                                // :2016-2020) before any retry/receipt logic,
                                // so the server may drop it too.
                                //
                                // This check comes FIRST, before the
                                // sealed-sender DERM arm below: the sealed
                                // sender layer wraps protocol failures into
                                // `SealedSenderDecryptionError` with the
                                // sender preserved, so a duplicate there would
                                // otherwise emit a retry receipt — the peer
                                // would resend the same unrecoverable bytes
                                // and the duplicate would come straight back
                                // (a second storm loop). Retrying a
                                // duplicate only burns CPU, starves the
                                // receive loop and gets the engine restarted
                                // by the watchdog — which redelivers the whole
                                // backlog.
                                let class = classify_open_error(&error);
                                if open_error_is_already_processed(&error) {
                                    state.duplicate_drops += 1;
                                    // One warn per stream keeps the incident
                                    // visible in the rolling log; the rest
                                    // stay at debug — a 20k-line flood is what
                                    // buried the watchdog evidence.
                                    if state.duplicate_drops == 1 {
                                        warn!(
                                            class,
                                            %error,
                                            "dropping already-processed envelope (ack); further duplicates on this stream stay at debug"
                                        );
                                    } else {
                                        debug!(class, %error, "dropping already-processed envelope");
                                    }
                                    // Drain progress: one warn line per 1000
                                    // drops so a long backlog stays visible in
                                    // the rolling file (which only keeps
                                    // warn+) without re-creating the flood.
                                    if state.duplicate_drops % 1000 == 0 {
                                        warn!(
                                            dropped = state.duplicate_drops,
                                            "still draining already-processed envelopes"
                                        );
                                    }
                                    ack.ack();
                                    continue;
                                }
                                if let libsignal_service::prelude::ServiceError::SealedSenderDecryptionError(libsignal_service::cipher::SealedSenderDecryptionError { sender: Some(sender), .. }) = &error {
                                        if let Some(sender) = ServiceId::parse_from_service_id_string(sender.name()) {
                                            slots.push(BatchSlot::Ready(Received::DecryptionError(
                                                sender,
                                                None,
                                                failure,
                                                ack,
                                            )));
                                            continue;
                                        }
                                    }
                                // KT (envelope retention, official
                                // unprocessed semantics — Desktop keeps
                                // failed envelopes with an attempts
                                // counter): the envelope re-enters a
                                // later window below the bound; the
                                // handle stays unacked so the server
                                // queue keeps it if the process dies.
                                let attempts = attempts + 1;
                                if envelope_retry_is_bounded(attempts) {
                                    warn!(
                                        attempts,
                                        class,
                                        %error,
                                        "error opening envelope, held for a bounded retry"
                                    );
                                    state.failed.push_back(WindowEnvelope {
                                        envelope: wire_envelope,
                                        ack,
                                        attempts,
                                    });
                                    state.retry_due.get_or_insert_with(|| {
                                        tokio::time::Instant::now() + OPEN_RETRY_DELAY
                                    });
                                } else {
                                    warn!(
                                                attempts,
                                                class,
                                                "giving up on envelope after 3 failed decryption attempts; skipped permanently"
                                            );
                                    ack.ack();
                                }
                            }
                        }
                    }

                    // KT (receive batching): one store transaction for
                    // the whole window (official Desktop
                    // MessageReceiver.preload.ts:1003-1123). A save
                    // failure must not lose the batch — the items are
                    // still yielded (and acked downstream), exactly like
                    // the per-message path logs and continues today.
                    if let Err(error) = save_messages_batch(
                        &mut state.store,
                        &mut state.identified_websocket,
                        to_save.clone(),
                    )
                    .await
                    {
                        error!(%error, "error saving message batch to store");
                    }

                    // Materialize the slots in arrival order; `Content`
                    // slots reference ascending positions in `to_save`.
                    let mut contents = to_save.into_iter();
                    let mut cursor = 0usize;
                    for slot in slots {
                        let received = match slot {
                            BatchSlot::Content { index, ack } => {
                                debug_assert_eq!(index, cursor);
                                cursor += 1;
                                Received::Content(
                                    Box::new(
                                        contents
                                            .next()
                                            .expect("content slots match to_save entries"),
                                    ),
                                    ack,
                                )
                            }
                            BatchSlot::Ready(received) => received,
                        };
                        state.pending.push_back(received);
                    }
                    // KT: a queue-empty seen mid-window is forwarded as
                    // an event only — the keys-request side effect stays
                    // on the first-of-pull arm below.
                    if queue_empty_seen {
                        state.pending.push_back(Received::QueueEmpty);
                    }
                }
            }
        });

        Ok(Box::pin(
            // We use the returning of the async closure in take_until as a stop signal
            // if the future resolves *anything* the stream will end.
            incoming_messages_stream.take_until(refresh_registration_task),
        ))
    }

    /// Uses Signal's SGX contact discovery service to resolve a phone number to its matching account identity
    #[cfg(feature = "cdsi")]
    pub async fn discover_contacts_by_phone_number<P: TryIntoE164>(
        &mut self,
        phone_numbers: impl IntoIterator<Item = P>,
    ) -> Result<Vec<(PhoneNumber, Option<ServiceId>)>, Error<S::Error>> {
        use libsignal_service::websocket::directory::LookupRequest;

        let mut ws = self.identified_websocket(false).await?;

        let lookup_request = LookupRequest {
            new_e164s: phone_numbers
                .into_iter()
                .filter_map(|p| p.try_into_e164().ok())
                .collect(),
            ..Default::default()
        };

        Ok(ws
            .discover_contacts(lookup_request)
            .await?
            .into_iter()
            .map(|(e164, service_id)| {
                use libsignal_service::utils::phonenumber_from_signal;
                (phonenumber_from_signal(&e164), service_id)
            })
            .collect())
    }

    /// Resolves a username (which has a text part and an additional random number) to its account identity
    /// for sending messages.
    pub async fn lookup_username(
        &mut self,
        username: &str,
    ) -> Result<Option<Aci>, Error<S::Error>> {
        let username = Username::new(username)?;
        let mut ws = self.unidentified_websocket().await?;
        let resolved_username = ws.look_up_username(&username).await?;
        Ok(resolved_username)
    }

    /// Sends a messages to the provided [ServiceId].
    /// The timestamp should be set to now and is used by Signal mobile apps
    /// to order messages later, and apply reactions.
    ///
    /// This method will automatically update the [DataMessage::expire_timer] if it is set to
    /// [None] such that the chat will keep the current expire timer. If the expire timer is set,
    /// it will be used as is, and the expire timer version will be incremented.
    pub async fn send_message(
        &mut self,
        recipient: impl Into<ServiceId>,
        message: impl Into<ContentBody>,
        timestamp: u64,
    ) -> Result<(), Error<S::Error>> {
        self.send_message_with_profile_sharing(recipient, message, timestamp, true)
            .await
    }

    /// KT fork (profileSharing gate, P9): same as [`Self::send_message`],
    /// but the caller decides whether **our** profile key is injected into
    /// the outgoing `DataMessage`. Official Desktop gates this on the
    /// conversation's `profileSharing` consent bit — a direct message
    /// without it must not leak our profile key to the recipient
    /// (ts/textsecure/sendNormalMessage.preload.ts:183-185; the consent
    /// record is `ContactRecord.whitelisted`, storage proto field 10).
    /// [`Self::send_message`] keeps the legacy always-share behavior.
    pub async fn send_message_with_profile_sharing(
        &mut self,
        recipient: impl Into<ServiceId>,
        message: impl Into<ContentBody>,
        timestamp: u64,
        include_profile_key: bool,
    ) -> Result<(), Error<S::Error>> {
        self.send_message_profile_sharing_inner(
            recipient,
            message,
            timestamp,
            include_profile_key,
            false,
        )
        .await
        .map(|_| ())
    }

    /// KT fork (deferred self sync, send-done semantics): like
    /// [`Self::send_message_with_profile_sharing`], but the multi-device
    /// sync transcript leg is **not** awaited — the prepared transcript body
    /// comes back for the caller to deliver in the background via
    /// [`Self::send_prepared_sync_transcript`]. Official Desktop resolves
    /// the send promise on the direct ack and runs the sync copy afterwards
    /// (ts/util/wrapWithSyncMessageSend.preload.ts).
    pub async fn send_message_with_profile_sharing_split_self(
        &mut self,
        recipient: impl Into<ServiceId>,
        message: impl Into<ContentBody>,
        timestamp: u64,
        include_profile_key: bool,
    ) -> Result<Option<ContentBody>, Error<S::Error>> {
        self.send_message_profile_sharing_inner(
            recipient,
            message,
            timestamp,
            include_profile_key,
            true,
        )
        .await
    }

    async fn send_message_profile_sharing_inner(
        &mut self,
        recipient: impl Into<ServiceId>,
        message: impl Into<ContentBody>,
        timestamp: u64,
        include_profile_key: bool,
        defer_self_sync: bool,
    ) -> Result<Option<ContentBody>, Error<S::Error>> {
        let mut sender = self.new_message_sender().await?;
        let recipient = recipient.into();

        let online_only = false;
        // TODO: Populate this flag based on the recipient information
        //
        // Issue <https://github.com/whisperfish/presage/issues/252>
        let include_pni_signature = false;
        let thread = Thread::Contact(recipient);
        let mut content_body: ContentBody = message.into();

        self.restore_thread_timer(&thread, &mut content_body).await;

        // KT (identified fuse): a certificate acquisition failure must not take
        // down the whole send — fall back to the identified path (official
        // Desktop same semantics). Background pre-rotation keeps the cache
        // fresh; sends normally hit the cache only.
        let sender_certificate = match self.sender_certificate().await {
            Ok(sender_certificate) => Some(sender_certificate),
            Err(error) => {
                warn!(%error, "sender certificate unavailable; falling back to identified send");
                None
            }
        };
        let unidentified_access = unidentified_access_or_identified(
            self.store.profile_key(&recipient).await?,
            sender_certificate,
        );

        // we need to put our profile key in DataMessage — gated by the
        // caller's profileSharing decision (KT P9; official
        // sendNormalMessage.preload.ts:183-185). required_protocol_version
        // stays unconditional: it is a wire-protocol field, not consent.
        if let ContentBody::DataMessage(message) = &mut content_body {
            if include_profile_key {
                message
                    .profile_key
                    .get_or_insert(self.state.data.profile_key().get_bytes().to_vec());
            }
            message.required_protocol_version = Some(0);
        }

        ensure_data_message_timestamp(&mut content_body, timestamp);

        // KT (deferred self sync): the split variant returns the prepared
        // transcript instead of awaiting the self leg.
        let sync_body = if defer_self_sync {
            let (result, sync_body) = sender
                .send_message_split_sync(
                    &recipient,
                    unidentified_access.as_ref(),
                    content_body.clone(),
                    timestamp,
                    include_pni_signature,
                    online_only,
                )
                .await;
            result?;
            sync_body
        } else {
            sender
                .send_message(
                    &recipient,
                    unidentified_access.as_ref(),
                    content_body.clone(),
                    timestamp,
                    include_pni_signature,
                    online_only,
                )
                .await?;
            None
        };

        // save the message
        let content = Content {
            metadata: Metadata {
                sender: self.state.data.service_ids.aci().into(),
                sender_device: self.state.device_id(),
                destination: recipient,
                server_guid: None,
                client_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
                // Note: Currently no way to get the timestamp the server received the message; just use our timestamp as a fallback.
                server_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
                needs_receipt: false,
                unidentified_sender: false,
                was_plaintext: false,
                pni_verified: None,
                story: None,
                ephemeral: None,
                urgent: None,
                report_spam_token: None,
            },
            body: content_body,
        };

        let mut identified_websocket = self.identified_websocket(false).await?;
        save_message(
            &mut self.store,
            &mut identified_websocket,
            content,
            Some(thread),
        )
        .await?;

        Ok(sync_body)
    }

    /// KT fork (deferred self sync): deliver a prepared sync transcript (as
    /// returned by [`Self::send_message_with_profile_sharing_split_self`] /
    /// [`Self::send_message_to_group_split_self`]) to the local devices.
    /// The outgoing message was already saved by the main send, so no store
    /// write happens here.
    pub async fn send_prepared_sync_transcript(
        &mut self,
        body: ContentBody,
        timestamp: u64,
    ) -> Result<(), Error<S::Error>> {
        let mut sender = self.new_message_sender().await?;
        sender.send_sync_transcript(body, timestamp).await?;
        Ok(())
    }

    /// KT fork (stories): send a story-channel message
    /// (`ContentBody::StoryMessage` or — since the story-delete batch S5 —
    /// a `ContentBody::DataMessage` carrier) to a single recipient over the
    /// official story transport face (sealed sender + `?story=true`, see
    /// libsignal-service `MessageSender::send_story_message`).
    ///
    /// Differences from [`Self::send_message`], each mirroring the official
    /// client (behavioral comparison, no code copied):
    /// - the sender's own profile key is injected into the `StoryMessage`
    ///   (Signal-Desktop `ts/jobs/helpers/sendStory.preload.ts:72` refuses
    ///   to send without it, `:198-205` puts it in the proto — same shape
    ///   `send_message` applies to `DataMessage`); `include_profile_key`
    ///   extends the injection to a `DataMessage` body (S5 story-DOE legs
    ///   mirror the official per-recipient sendMessage profileKey carry,
    ///   gated by the caller's profile-sharing state);
    /// - the message is **not** saved to the local store: stories do not
    ///   belong to the local timeline (upstream presage already skips
    ///   saving received `StoryMessage`s, and the desktop story flow keeps
    ///   the story out of the recipient chat list);
    /// - no sent-transcript is produced here; the story self-sync
    ///   (`SyncMessage.Sent` with `story_message` +
    ///   `story_message_recipients`) is the caller's responsibility, sent
    ///   once after all per-recipient sends (Signal-Desktop
    ///   `sendStory.preload.ts:338-340`, `:565-598`).
    pub async fn send_story_message(
        &mut self,
        recipient: impl Into<ServiceId>,
        message: impl Into<ContentBody>,
        timestamp: u64,
        include_profile_key: bool,
    ) -> Result<(), Error<S::Error>> {
        let recipient = recipient.into();
        let mut content_body: ContentBody = message.into();

        // we need to put our profile key in StoryMessage
        match &mut content_body {
            ContentBody::StoryMessage(message) => {
                message
                    .profile_key
                    .get_or_insert(self.state.data.profile_key().get_bytes().to_vec());
            }
            ContentBody::DataMessage(message) if include_profile_key => {
                message
                    .profile_key
                    .get_or_insert(self.state.data.profile_key().get_bytes().to_vec());
            }
            _ => {}
        }

        let sender_certificate = self.sender_certificate().await?;
        let mut sender = self.new_message_sender().await?;
        sender
            .send_story_message(&recipient, &sender_certificate, content_body, timestamp)
            .await?;

        Ok(())
    }

    /// KT fork (DERM plaintext leg, E3): send a `PlaintextContent` (the
    /// resend-request carrier, Envelope type PLAINTEXT_CONTENT) to a single
    /// recipient without session encryption. Official Desktop sends retry
    /// requests exactly this way (ts/jobs/helpers/sendResendRequest.preload.ts
    /// → sendMessageProtoAndWait{proto: plaintext}). Unlike
    /// [`Self::send_message`] there is no profile-key injection, no
    /// thread-timer restore and no local save — a resend request is not a
    /// timeline message.
    pub async fn send_plaintext_content(
        &mut self,
        recipient: impl Into<ServiceId>,
        plaintext: &PlaintextContent,
        timestamp: u64,
    ) -> Result<(), Error<S::Error>> {
        let recipient = recipient.into();
        let mut sender = self.new_message_sender().await?;
        sender
            .send_plaintext_content(&recipient, plaintext, timestamp)
            .await?;
        Ok(())
    }

    /// Uploads one attachment prior to linking them in a message.
    pub async fn upload_attachment(
        &self,
        spec: AttachmentSpec,
        contents: Vec<u8>,
    ) -> Result<Result<AttachmentPointer, AttachmentUploadError>, Error<S::Error>> {
        Ok(self
            .new_message_sender()
            .await?
            .upload_attachment(spec, contents, &mut rng())
            .await)
    }

    /// Uploads attachments prior to linking them in a message.
    pub async fn upload_attachments(
        &self,
        attachments: Vec<(AttachmentSpec, Vec<u8>)>,
    ) -> Result<Vec<Result<AttachmentPointer, AttachmentUploadError>>, Error<S::Error>> {
        if attachments.is_empty() {
            return Ok(Vec::new());
        }
        let sender = self.new_message_sender().await?;
        let upload = future::join_all(attachments.into_iter().map(move |(spec, contents)| {
            let mut sender = sender.clone();
            async move { sender.upload_attachment(spec, contents, &mut rng()).await }
        }));
        Ok(upload.await)
    }

    /// Sends one message in a group (v2). The `master_key_bytes` is required to have 32 elements.
    ///
    /// This method will automatically update the [DataMessage::expire_timer] if it is set to
    /// [None] such that the chat will keep the current expire timer.
    pub async fn send_message_to_group(
        &mut self,
        master_key_bytes: &[u8],
        message: impl Into<ContentBody>,
        timestamp: u64,
    ) -> Result<GroupSendOutcomes, Error<S::Error>> {
        self.send_message_to_group_inner(master_key_bytes, message, timestamp, false)
            .await
            .map(|(outcomes, _)| outcomes)
    }

    /// KT fork (deferred self sync, send-done semantics): group counterpart
    /// of [`Self::send_message_with_profile_sharing_split_self`] — per-
    /// member outcomes return without awaiting the sync transcript leg; the
    /// prepared transcript body (when one is due) is delivered by the caller
    /// in the background via [`Self::send_prepared_sync_transcript`].
    pub async fn send_message_to_group_split_self(
        &mut self,
        master_key_bytes: &[u8],
        message: impl Into<ContentBody>,
        timestamp: u64,
    ) -> Result<(GroupSendOutcomes, Option<ContentBody>), Error<S::Error>> {
        self.send_message_to_group_inner(master_key_bytes, message, timestamp, true)
            .await
    }

    async fn send_message_to_group_inner(
        &mut self,
        master_key_bytes: &[u8],
        message: impl Into<ContentBody>,
        timestamp: u64,
        defer_self_sync: bool,
    ) -> Result<(GroupSendOutcomes, Option<ContentBody>), Error<S::Error>> {
        let mut content_body = message.into();
        let master_key_bytes = master_key_bytes
            .try_into()
            .expect("Master key bytes to be of size 32.");
        let thread = Thread::Group(master_key_bytes);

        self.restore_thread_timer(&thread, &mut content_body).await;
        ensure_data_message_timestamp(&mut content_body, timestamp);

        let mut sender = self.new_message_sender().await?;

        // Slot guard scoped: the manager store is taken &mut below for the
        // outgoing transcript save.
        let upserted = {
            let mut groups_manager = Box::pin(self.groups_manager()).await?;
            // KT (group send latency, official semantics): sends dispatch on
            // the locally stored group state — revision 0 never forces a
            // network fetch. upsert_group still fetches exactly once when the
            // store holds no state for this group (no blind sends), and the
            // fetch-failure fallback below is unchanged (fail-open). The
            // stored state is kept current by the consumer's post-send
            // background refresh (official clients read local group state in
            // the send path and delegate refreshing to background group
            // sync; stale-membership window = until that refresh lands, the
            // same trade-off).
            let revision = 0;
            // KT (group fetch degradation): the send path stays fail-open —
            // a structured group fetch failure falls back to the stored
            // group (a stale recipient list beats a failed send).
            match upsert_group(
                &self.store,
                &mut groups_manager,
                &master_key_bytes,
                &revision,
            )
            .await
            {
                Ok(upserted) => upserted,
                Err(error @ (Error::GroupSyncFailed { .. } | Error::GroupGone)) => {
                    warn!(%error, "group fetch failed; falling back to stored group for send");
                    self.store.group(master_key_bytes).await?
                }
                Err(error) => return Err(error),
            }
        };
        let Some(group) = upserted else {
            return Err(Error::UnknownGroup);
        };

        // KT (group send semantics): official clients block non-admin posts
        // into announcement groups client-side (the server would not); a
        // group with no state at all fails earlier as UnknownGroup, so the
        // gate adds no new failure face.
        let own_aci = self.state.data.service_ids.aci();
        if group.announcements_only && !group_member_is_admin(&group, &own_aci) {
            return Err(Error::GroupAnnouncementsOnly);
        }

        // KT (identified fuse): same fallback as the 1:1 path — per-member
        // access drops to None (identified) when the certificate is missing.
        let sender_certificate = match self.sender_certificate().await {
            Ok(sender_certificate) => Some(sender_certificate),
            Err(error) => {
                warn!(%error, "sender certificate unavailable; falling back to identified send");
                None
            }
        };
        let mut recipient_ids = Vec::new();
        let mut recipients = Vec::new();
        for member in group
            .members
            .into_iter()
            .filter(|m| m.aci != self.state.data.service_ids.aci())
        {
            // KT (group send semantics): an unreadable profile row degrades
            // the member to the identified channel instead of failing the
            // whole batch.
            let profile_key = self
                .store
                .profile_key(&member.aci.into())
                .await
                .ok()
                .flatten();
            let unidentified_access =
                unidentified_access_or_identified(profile_key, sender_certificate.clone());
            let include_pni_signature = false;
            recipient_ids.push(member.aci.into());
            recipients.push((
                member.aci.into(),
                unidentified_access,
                include_pni_signature,
            ));
        }

        let online_only = false;
        // KT (deferred self sync): the split variant returns the prepared
        // transcript instead of awaiting the self leg.
        let (results, sync_body) = if defer_self_sync {
            sender
                .send_message_to_group_split_sync(
                    recipients,
                    content_body.clone(),
                    timestamp,
                    online_only,
                )
                .await
        } else {
            let results = sender
                .send_message_to_group(recipients, content_body.clone(), timestamp, online_only)
                .await;
            (results, None)
        };

        // KT (group send semantics): per-recipient outcomes — individual
        // failures (unregistered members included) no longer fail the whole
        // send (official per-recipient result semantics); the transcript is
        // saved regardless, so even the first message to a group lands in
        // the store.
        let outcomes: GroupSendOutcomes = recipient_ids
            .into_iter()
            .zip(results)
            .map(|(service_id, result)| (service_id, result.err()))
            .collect();

        let content = Content {
            metadata: Metadata {
                sender: self.state.data.service_ids.aci().into(),
                destination: self.state.data.service_ids.aci().into(),
                sender_device: self.state.device_id(),
                server_guid: None,
                client_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
                // Note: Currently no way to get the timestamp the server received the message; just use our timestamp as a fallback.
                server_timestamp: chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap(),
                needs_receipt: false, // TODO: this is just wrong
                unidentified_sender: false,
                was_plaintext: false,
                pni_verified: None,
                story: None,
                ephemeral: None,
                urgent: None,
                report_spam_token: None,
            },
            body: content_body,
        };

        let mut identified_websocket = self.identified_websocket(false).await?;
        save_message(
            &mut self.store,
            &mut identified_websocket,
            content,
            Some(thread),
        )
        .await?;

        Ok((outcomes, sync_body))
    }

    async fn restore_thread_timer(&mut self, thread: &Thread, content_body: &mut ContentBody) {
        let store_expire_timer = self.store.expire_timer(thread).await.unwrap_or_default();

        if let ContentBody::DataMessage(DataMessage {
            expire_timer: ref mut timer,
            expire_timer_version: ref mut version,
            ..
        }) = content_body
        {
            if timer.is_none() {
                *timer = store_expire_timer.and_then(|(t, _)| if t == 0 { None } else { Some(t) });
                *version = Some(store_expire_timer.map(|(_, v)| v).unwrap_or_default());
            } else {
                *version = Some(store_expire_timer.map(|(_, v)| v).unwrap_or_default() + 1);
            }
        }
    }

    /// Clears all sessions established with [recipient](ServiceId).
    pub async fn clear_sessions(&self, recipient: &ServiceId) -> Result<(), Error<S::Error>> {
        use libsignal_service::session_store::SessionStoreExt;
        self.store
            .aci_protocol_store()
            .delete_all_sessions(recipient)
            .await?;
        self.store
            .pni_protocol_store()
            .delete_all_sessions(recipient)
            .await?;
        Ok(())
    }

    /// KT (pre-key maintenance): server-watermark pre-key refresh — the same
    /// gated path the receive stream runs on open (official RefreshPreKeysJob
    /// semantics), exposed for the engine's periodic maintenance ticker so a
    /// long-lived connection still tops up one-time keys.
    pub async fn refresh_pre_keys(&mut self) -> Result<(), Error<S::Error>> {
        // KT (handover race, §29.6): 每次尝试统一走 require_unused=false
        // 的 slot 拨号（复用活连接/只顶死连接），不再经 account_manager()
        // 的 require_unused=true —— 那条路会 handover 杀掉占用 request
        // stream 的消息管道（本方法被 engine 的 pre-keys 时序闸与 prekey
        // 巡检共用；闸场景下接收流连接可能正占着 stream）。WsClosing 由
        // 骨架兜底重试（上限 2 次、间隔 500ms）。
        let push_service = self.identified_push_service();
        let credentials = self.credentials();
        let store = &self.store;
        register_with_ws_handover_retry(move || {
            let push_service = push_service.clone();
            let credentials = credentials.clone();
            async move {
                let mut account_manager = dial_pre_keys_connection(&push_service, &credentials)
                    .await
                    .map_err(Error::ServiceError)?;
                register_pre_keys(store, &mut account_manager).await
            }
        })
        .await
    }

    /// KT (pre-key maintenance): age-based signed pre-key rotation for both
    /// identity kinds (official clients rotate by age; the server-watermark
    /// gates otherwise pin a signed pre-key forever on healthy counts).
    pub async fn rotate_signed_pre_keys(&mut self) -> Result<(), Error<S::Error>> {
        let mut account_manager = self.account_manager().await?;
        account_manager
            .rotate_signed_pre_key_bundle(&mut self.store.aci_protocol_store(), ServiceIdKind::Aci)
            .await?;
        account_manager
            .rotate_signed_pre_key_bundle(&mut self.store.pni_protocol_store(), ServiceIdKind::Pni)
            .await?;
        Ok(())
    }

    async fn account_manager(&self) -> Result<AccountManager, Error<S::Error>> {
        // KT EXPERIMENT REVERTED (2026-10-23): the reuse (require_unused=false)
        // broke connector link tests (ACCOUNT_ALREADY_LINKED in
        // per_account_pool) — root fix deferred to the multiplexing rework.
        let identified_websocket = self.identified_websocket(true).await?;
        Ok(AccountManager::new(
            self.identified_push_service(),
            identified_websocket,
            None,
        ))
    }

    /// Downloads and decrypts a single attachment.
    pub async fn get_attachment(
        &self,
        attachment_pointer: &AttachmentPointer,
    ) -> Result<Vec<u8>, Error<S::Error>> {
        let expected_digest = attachment_pointer
            .digest
            .as_ref()
            .ok_or_else(|| Error::UnexpectedAttachmentChecksum)?;

        let mut service = self.identified_push_service();
        let mut attachment_stream = service.get_attachment(attachment_pointer).await?;

        let plaintext_len = attachment_pointer.size.and_then(|len| len.try_into().ok());

        // We need the whole file for the crypto to check out
        let mut ciphertext = Vec::with_capacity(plaintext_len.unwrap_or(0));
        let size_bytes = attachment_stream
            .stream
            .read_to_end(&mut ciphertext)
            .await?;
        trace!(size_bytes, "downloaded encrypted attachment");

        let digest = sha2::Sha256::digest(&ciphertext);
        if &digest[..] != expected_digest {
            return Err(Error::UnexpectedAttachmentChecksum);
        }

        let key: [u8; 64] = attachment_pointer.key().try_into()?;

        // Offload decryption of large attachments to another thread.
        // Chose arbitrary threshold here.
        const DECRYPT_IN_THREAD_THRESHOLD: usize = 100 * 1024;
        if ciphertext.len() > DECRYPT_IN_THREAD_THRESHOLD {
            ciphertext = tokio::task::spawn_blocking(move || {
                decrypt_in_place(key, &mut ciphertext).map(|_| ciphertext)
            })
            .await
            .expect("decryption in another thread")?;
        } else {
            decrypt_in_place(key, &mut ciphertext)?;
        };

        if let Some(len) = plaintext_len {
            if len < ciphertext.len() {
                // remove padding
                ciphertext.truncate(len);
            }
        }

        Ok(ciphertext)
    }

    /// Gets the metadata of a sticker
    pub async fn sticker_metadata(
        &mut self,
        pack_id: &[u8],
        sticker_id: u32,
    ) -> Result<Option<Sticker>, Error<S::Error>> {
        Ok(self.store.sticker_pack(pack_id).await?.and_then(|pack| {
            pack.manifest
                .stickers
                .iter()
                .find(|&x| x.id == sticker_id)
                .cloned()
        }))
    }

    /// Installs a sticker pack and notifies other registered devices
    pub async fn install_sticker_pack(
        &mut self,
        pack_id: &[u8],
        pack_key: &[u8],
    ) -> Result<(), Error<S::Error>> {
        let sticker_pack_operation = StickerPackOperation {
            pack_id: Some(pack_id.to_vec()),
            pack_key: Some(pack_key.to_vec()),
            r#type: Some(sticker_pack_operation::Type::Install as i32),
        };

        let unidentified_websocket = self.unidentified_websocket().await?;
        download_sticker_pack(
            self.store.clone(),
            unidentified_websocket,
            &sticker_pack_operation,
        )
        .await?;

        // Sync the change with the other devices
        let sync_message = SyncMessage {
            sticker_pack_operation: vec![sticker_pack_operation],
            ..Default::default()
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        self.send_message(self.state.data.service_ids.aci(), sync_message, timestamp)
            .await?;

        Ok(())
    }

    /// Removes an installed sticker pack
    pub async fn remove_sticker_pack(
        &mut self,
        pack_id: &[u8],
        pack_key: &[u8],
    ) -> Result<(), Error<S::Error>> {
        // Sync the change with the other clients
        let sync_message = SyncMessage {
            sticker_pack_operation: vec![StickerPackOperation {
                pack_id: Some(pack_id.to_vec()),
                pack_key: Some(pack_key.to_vec()), // The pack key might not be neccesary in the message
                r#type: Some(sticker_pack_operation::Type::Remove as i32),
            }],
            ..Default::default()
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        self.send_message(self.state.data.service_ids.aci(), sync_message, timestamp)
            .await?;

        self.store.remove_sticker_pack(pack_id).await?;

        Ok(())
    }

    pub async fn send_session_reset(
        &mut self,
        recipient: &ServiceId,
        timestamp: u64,
    ) -> Result<(), Error<S::Error>> {
        trace!(recipient = %recipient.service_id_string(), "resetting session for address");

        let mut store = self.store.aci_protocol_store();

        // Archive all sessions with all receiver devices.
        // Note that the "get_sub_device_sessions" does not include the main device, therefore add it.
        // Note that deleting the session is not equivalent to archiving:
        // If we deleted the session, we would also delete the information on the list of devices the peer has, failing message sending.
        for device in store
            .get_sub_device_sessions(recipient)
            .await?
            .into_iter()
            .chain(vec![*DEFAULT_DEVICE_ID])
        {
            let address = recipient
                .aci()
                .expect("Recipient to be given as ACI")
                .to_protocol_address(device)
                .expect("Could not construct protocol address from recipient");
            if let Some(mut session) = store.load_session(&address).await? {
                session.archive_current_state()?;
                store.store_session(&address, &session).await?;
            }
        }

        // TODO: Signal Android also deletes entries in some sender_key_shared table; we don't have such a table yet.
        // Does not seem necessary though.

        // Send a null message.
        let message = NullMessage::generate(&mut rand::rng());
        self.send_message(*recipient, message, timestamp).await?;

        Ok(())
    }

    fn credentials(&self) -> ServiceCredentials {
        self.state.credentials()
    }

    /// Creates a new message sender.
    async fn new_message_sender(&self) -> Result<MessageSender<S::AciStore>, Error<S::Error>> {
        // KT (send perf): reuse-or-dial of both websockets is the dominant
        // cold-start segment; the shared slot makes a live ws a cheap clone.
        let setup_started = std::time::Instant::now();
        let sender = self.new_message_sender_inner().await;
        libsignal_service::perf_hook::emit(
            libsignal_service::perf_hook::keys::SENDER_SETUP,
            setup_started.elapsed().as_micros() as u64,
        );
        sender
    }

    async fn new_message_sender_inner(
        &self,
    ) -> Result<MessageSender<S::AciStore>, Error<S::Error>> {
        let identified_websocket = self.identified_websocket(false).await?;
        let unidentified_websocket = self.unidentified_websocket().await?;

        let aci_protocol_store = self.store.aci_protocol_store();
        let aci_identity_keypair = aci_protocol_store.get_identity_key_pair().await?;
        let pni_identity_keypair = self
            .store
            .pni_protocol_store()
            .get_identity_key_pair()
            .await?;

        Ok(MessageSender::new(
            identified_websocket,
            unidentified_websocket,
            self.identified_push_service(),
            self.new_service_cipher_aci(),
            aci_protocol_store,
            self.state.data.service_ids.aci,
            self.state.data.service_ids.pni,
            aci_identity_keypair,
            Some(pni_identity_keypair),
            self.state.device_id(),
        ))
    }

    fn new_service_cipher_aci(&self) -> ServiceCipher<S::AciStore> {
        ServiceCipher::new(
            self.store.aci_protocol_store(),
            self.state
                .service_configuration()
                .unidentified_sender_trust_roots,
            ProtocolAddress::new(
                self.state.data.service_ids.aci.to_string(),
                self.state.device_id(),
            ),
        )
    }

    fn new_service_cipher_pni(&self) -> ServiceCipher<S::PniStore> {
        ServiceCipher::new(
            self.store.pni_protocol_store(),
            self.state
                .service_configuration()
                .unidentified_sender_trust_roots,
            ProtocolAddress::new(
                self.state.data.service_ids.pni.to_string(),
                self.state.device_id(),
            ),
        )
    }

    /// KT (send warmup): best-effort pre-establishment of the send path for
    /// `recipients` — dials both websockets via `new_message_sender`, makes
    /// the sender certificate a cache hit, then builds a session with every
    /// distinct non-self recipient (prekey fetch + bundle processing).
    /// Sends nothing; individual failures are counted, not propagated.
    pub async fn warm_send_path(
        &mut self,
        recipients: &[ServiceId],
    ) -> Result<SendWarmupStats, Error<S::Error>> {
        let mut sender = self.new_message_sender().await?;
        let _ = self.sender_certificate().await;

        let mut stats = SendWarmupStats::default();
        let own = ServiceId::Aci(self.state.data.service_ids.aci.into());
        let mut seen: Vec<&ServiceId> = Vec::with_capacity(recipients.len());
        for recipient in recipients {
            if *recipient == own || seen.contains(&recipient) {
                continue;
            }
            seen.push(recipient);
            match sender.ensure_session(recipient, *DEFAULT_DEVICE_ID).await {
                Ok(true) => stats.established += 1,
                Ok(false) => stats.existing += 1,
                Err(_) => stats.failed += 1,
            }
        }
        Ok(stats)
    }

    /// Returns the title of a thread (contact or group).
    pub async fn thread_title(&self, thread: &Thread) -> Result<String, Error<S::Error>> {
        match thread {
            Thread::Contact(service_id) => {
                let contact = match self.store.contact_by_id(service_id).await {
                    Ok(contact) => contact,
                    Err(error) => {
                        info!(%error, service_id =% service_id.service_id_string(), "error getting contact by id");
                        None
                    }
                };
                Ok(match contact {
                    Some(contact) => contact.name,
                    None => service_id.service_id_string(),
                })
            }
            Thread::Group(id) => match self.store.group(*id).await? {
                Some(group) => Ok(group.title),
                None => Ok("".to_string()),
            },
        }
    }

    /// Returns how this client was registered, either as a primary or secondary device.
    pub fn registration_type(&self) -> RegistrationType {
        if self.state.data.device_name.is_some() {
            RegistrationType::Secondary
        } else {
            RegistrationType::Primary
        }
    }

    /// As a primary device, link a secondary device.
    pub async fn link_secondary(&mut self, secondary: Url) -> Result<(), Error<S::Error>> {
        // XXX: What happens if secondary device? Possible to use static typing to make this method call impossible in that case?
        if self.registration_type() != RegistrationType::Primary {
            return Err(Error::NotPrimaryDevice);
        }

        let credentials = self.credentials();
        let mut account_manager = AccountManager::new(
            self.identified_push_service(),
            self.identified_websocket(false).await?,
            Some(self.state.data.profile_key),
        );

        account_manager
            .link_device(
                &mut rand::rng(),
                secondary,
                &self.store.aci_protocol_store(),
                &self.store.pni_protocol_store(),
                ProvisioningSecrets {
                    credentials,
                    account_entropy_pool: self
                        .account_entropy_pool()
                        .await?
                        .expect("Primary device to always have an account entropy pool"),
                    ephemeral_backup_key: None,
                    media_root_backup_key: None,
                },
            )
            .await?;
        Ok(())
    }

    /// As a primary device, unlink a secondary device.
    pub async fn unlink_secondary(
        &self,
        device_id: impl TryInto<DeviceId>,
    ) -> Result<(), Error<S::Error>> {
        // secondary devices cannot unlink themselves or other devices, it will fail with an unauthorized error
        if self.registration_type() != RegistrationType::Primary {
            return Err(Error::NotPrimaryDevice);
        }
        self.identified_websocket(false)
            .await?
            .unlink_device(device_id.try_into().map_err(|_| Error::InvalidDeviceId)?)
            .await?;
        Ok(())
    }

    /// As a primary device, list all the devices (including the current device).
    pub async fn devices(&self) -> Result<Vec<DeviceInfo>, Error<S::Error>> {
        let aci_protocol_store = self.store.aci_protocol_store();
        let mut account_manager = AccountManager::new(
            self.identified_push_service(),
            self.identified_websocket(false).await?,
            Some(self.state.data.profile_key),
        );

        Ok(account_manager.linked_devices(&aci_protocol_store).await?)
    }
}

/// Set the timestamp in any DataMessage so it matches its envelope's
fn ensure_data_message_timestamp(content_body: &mut ContentBody, timestamp: u64) {
    match content_body {
        ContentBody::DataMessage(message) => {
            message.timestamp = Some(timestamp);
        }
        ContentBody::EditMessage(EditMessage {
            data_message: Some(data_message),
            ..
        }) => {
            data_message.timestamp = Some(timestamp);
        }
        ContentBody::SynchronizeMessage(SyncMessage {
            content:
                Some(SyncContent::Sent(sync_message::Sent {
                    message: Some(data_message),
                    ..
                })),
            ..
        }) => {
            data_message.timestamp = Some(timestamp);
        }
        _ => (),
    }
}

/// KT (DERM): what is needed to build a retry receipt for a failed envelope,
/// captured before the envelope is consumed by decryption (Android
/// MessageDecryptor buildSendRetryReceiptJob: original content bytes, cipher
/// type, client timestamp, sender device).
struct RetryReceiptMaterial {
    content: Vec<u8>,
    message_type: libsignal_service::protocol::CiphertextMessageType,
    timestamp: u64,
    sender_device: u32,
}

impl RetryReceiptMaterial {
    fn of(envelope: &libsignal_service::proto::Envelope) -> Option<Self> {
        use libsignal_service::proto::envelope::Type;
        use libsignal_service::protocol::CiphertextMessageType;
        let content = envelope.content.as_ref()?.clone();
        let message_type = match envelope.r#type() {
            Type::DoubleRatchet => CiphertextMessageType::Whisper,
            Type::PrekeyMessage => CiphertextMessageType::PreKey,
            Type::UnidentifiedSender => CiphertextMessageType::SenderKey,
            _ => return None,
        };
        Some(Self {
            content,
            message_type,
            timestamp: envelope.client_timestamp(),
            sender_device: envelope.source_device_id(),
        })
    }

    /// The receipt names the original message (ratchet key + timestamp) so
    /// the sender can re-send it into a session we can actually decrypt.
    /// Plaintext / malformed originals yield None (no receipt, official
    /// fallback is the consumer's automatic session reset).
    fn into_failure(self) -> Option<DecryptionFailure> {
        use libsignal_service::protocol::{DecryptionErrorMessage, Timestamp};
        let receipt = DecryptionErrorMessage::for_original(
            &self.content,
            self.message_type,
            Timestamp::from_epoch_millis(self.timestamp),
            self.sender_device,
        )
        .ok()?;
        Some(DecryptionFailure::RetryReceipt(
            receipt.serialized().to_vec(),
        ))
    }
}

/// KT (group send semantics): per-recipient group send outcome — `None` =
/// delivered, `Some` = that recipient's failure. Official clients report
/// per-recipient results; individual failures don't fail the whole send.
pub type GroupSendOutcomes = Vec<(ServiceId, Option<MessageSenderError>)>;

/// KT (group send semantics): sender holds the ADMINISTRATOR role in this
/// group — the announcements-only send gate predicate. `pub` so the engine
/// workspace (this fork's only consumer) can pin the gate contract in its
/// test gate; not part of the upstream API surface.
pub fn group_member_is_admin(group: &Group, aci: &Aci) -> bool {
    group.members.iter().any(|member| {
        member.aci == *aci && member.role == libsignal_service::groups_v2::Role::Administrator
    })
}

async fn upsert_group<S: Store>(
    store: &S,
    groups_manager: &mut GroupsManager<InMemoryCredentialsCache>,
    master_key_bytes: &[u8],
    revision: &u32,
) -> Result<Option<Group>, Error<S::Error>> {
    let upsert_group = match store.group(master_key_bytes.try_into()?).await {
        Ok(Some(group)) => {
            debug!(group_name =% group.title, "loaded group from local db");
            group.revision < *revision
        }
        Ok(None) => true,
        Err(error) => {
            warn!(%error, "failed to retrieve group from local db");
            true
        }
    };

    if upsert_group {
        debug!("fetching and saving group");
        // KT (group fetch degradation): fetch+decrypt+save is one retry
        // unit; decode-class exhaustion surfaces as a structured error
        // instead of silently keeping a stale group.
        let mut attempts = 0;
        loop {
            attempts += 1;
            let failure = match groups_manager
                .fetch_encrypted_group(&mut rand::rng(), master_key_bytes)
                .await
            {
                Ok(encrypted_group) => {
                    match decrypt_group(master_key_bytes, encrypted_group) {
                        Ok(group) => {
                            if let Err(error) =
                                store.save_group(master_key_bytes.try_into()?, group).await
                            {
                                error!(%error, "failed to save group");
                            }
                            None
                        }
                        // Decrypt failures are decode-class by construction
                        // (GroupsV2DecryptionError via GroupDecodingError).
                        Err(error) => Some(error),
                    }
                }
                Err(error) if group_fetch_gone(&error) => return Err(Error::GroupGone),
                Err(error) if group_fetch_decode_class(&error) => Some(error),
                Err(error) => {
                    warn!(%error, "failed to fetch encrypted group");
                    None
                }
            };
            match failure {
                None => break,
                Some(error) if attempts < GROUP_FETCH_MAX_ATTEMPTS => {
                    warn!(%error, "decode-class failure, retrying once");
                }
                Some(_) => return Err(Error::GroupSyncFailed { attempts }),
            }
        }
    }

    Ok(store.group(master_key_bytes.try_into()?).await?)
}

/// Download and decrypt a sticker manifest
async fn download_sticker_pack<C: ContentsStore>(
    mut store: C,
    mut unidentified_websocket: SignalWebSocket<websocket::Unidentified>,
    operation: &StickerPackOperation,
) -> Result<StickerPack, Error<C::ContentsStoreError>> {
    debug!("downloading sticker pack");
    let pack_key = operation.pack_key();
    let pack_id = operation.pack_id();
    let key = derive_key(pack_key)?;

    let mut ciphertext = Vec::new();

    let size_bytes = unidentified_websocket
        .get_sticker_pack_manifest(&hex::encode(pack_id))
        .await?
        .read_to_end(&mut ciphertext)
        .await?;

    trace!(size_bytes, "downloaded encrypted sticker pack manifest");

    decrypt_in_place(key, &mut ciphertext)?;

    let mut sticker_pack_manifest: StickerPackManifest =
        libsignal_service::proto::Pack::decode(ciphertext.as_slice())
            .map_err(ProvisioningError::from)?
            .into();

    for sticker in &mut sticker_pack_manifest.stickers {
        match download_sticker::<C>(&mut unidentified_websocket, pack_id, pack_key, sticker.id)
            .await
        {
            Ok(decrypted_sticker_bytes) => {
                debug!(id = sticker.id, "downloaded sticker");
                sticker.bytes = Some(decrypted_sticker_bytes);
            }
            Err(error) => error!(sticker.id, %error,"failed to download sticker"),
        }
    }

    let sticker_pack = StickerPack {
        id: pack_id.to_vec(),
        key: pack_key.to_vec(),
        manifest: sticker_pack_manifest,
    };

    // save everything in store
    store.add_sticker_pack(&sticker_pack).await?;

    Ok(sticker_pack)
}

/// Downloads and decrypts a single sticker
async fn download_sticker<C: ContentsStore>(
    unidentified_websocket: &mut SignalWebSocket<websocket::Unidentified>,
    pack_id: &[u8],
    pack_key: &[u8],
    sticker_id: u32,
) -> Result<Vec<u8>, Error<C::ContentsStoreError>> {
    let key = derive_key(pack_key)?;

    let mut sticker_stream = unidentified_websocket
        .get_sticker(&hex::encode(pack_id), sticker_id)
        .await?;

    let mut ciphertext = Vec::new();
    let size_bytes = sticker_stream.read_to_end(&mut ciphertext).await?;

    trace!(size_bytes, "downloaded encrypted sticker");

    decrypt_in_place(key, &mut ciphertext)?;

    Ok(ciphertext)
}

/// Save a message into the store.
/// Note that `override_thread` can be used to specify the thread the message will be stored in.
/// This is required when storing outgoing messages, as in this case the appropriate storage place cannot be derived from the message itself.
/// KT (receive batching): all per-message preparation of [`save_message`] —
/// profile-key upsert, expire timer, remote-delete collapse, delete-for-me —
/// with the final insert factored out so a receive window can be persisted
/// in one store transaction via [`save_messages_batch`].
async fn prepare_message_for_save<S: Store>(
    store: &mut S,
    identified_websocket: &mut websocket::SignalWebSocket<websocket::Identified>,
    message: Content,
    override_thread: Option<Thread>,
) -> Result<Option<(Thread, Content)>, Error<S::Error>> {
    // derive the thread from the message type
    let thread = override_thread.unwrap_or(Thread::try_from(&message)?);

    // only save DataMessage and SynchronizeMessage (sent)
    let message = match message.body {
        ContentBody::DecryptionErrorMessage(e) => {
            warn!(error = ?e, "was asked to save a DecryptionErrorMessage; this should not happen");
            None
        }
        ContentBody::NullMessage(_) => Some(message),
        ContentBody::DataMessage(
            ref data_message @ DataMessage {
                ref profile_key, ..
            },
        )
        | ContentBody::SynchronizeMessage(SyncMessage {
            content:
                Some(SyncContent::Sent(sync_message::Sent {
                    message:
                        Some(
                            ref data_message @ DataMessage {
                                ref profile_key, ..
                            },
                        ),
                    ..
                })),
            ..
        }) => {
            // update recipient profile key if changed
            if let Some(profile_key_bytes) = profile_key.clone().and_then(|p| p.try_into().ok()) {
                let sender = message.metadata.sender;
                let profile_key = ProfileKey::create(profile_key_bytes);
                debug!(sender = %sender.service_id_string(), "inserting profile key for");

                // Either:
                // - insert a new contact with the profile information
                // - update the contact if the profile key has changed
                // TODO: mark this contact as "created by us" maybe to know whether we should update it or not
                // NOTE: this needs to happen in the background!
                let store_inner = store.clone();
                let websocket_inner = identified_websocket.clone();
                let data_message_inner = data_message.clone();
                tokio::spawn(async move {
                    if let Err(error) = upsert_contact_from_profile(
                        store_inner,
                        websocket_inner,
                        &data_message_inner,
                        sender,
                        profile_key,
                    )
                    .await
                    {
                        error!(%error, "failed to upsert newly seen contact!");
                    }
                });
            }

            // Note: The expire timer fields of data messages are only for contacts.
            // Expire timers are handled for groups via upsert_group due to a revision change.
            if let Thread::Contact(_) = thread {
                let version = data_message.expire_timer_version.unwrap_or(1);
                store
                    .update_expire_timer(
                        &thread,
                        data_message.expire_timer.unwrap_or_default(),
                        version,
                    )
                    .await?;
            }

            match data_message {
                DataMessage {
                    delete:
                        Some(Delete {
                            target_sent_timestamp: Some(ts),
                        }),
                    ..
                } => {
                    // replace an existing message by an empty NullMessage
                    if let Some(mut existing_msg) = store.message(&thread, *ts).await? {
                        existing_msg.metadata.sender = Aci::from(Uuid::nil()).into();
                        existing_msg.body = NullMessage::default().into();
                        store.save_message(&thread, existing_msg).await?;
                        debug!(%thread, ts, "message in thread deleted");
                        None
                    } else {
                        warn!(%thread, ts, "could not find message to delete in thread");
                        None
                    }
                }
                _ => Some(message),
            }
        }
        ContentBody::SynchronizeMessage(SyncMessage {
            content: Some(SyncContent::DeleteForMe(ref delete)),
            ..
        }) => {
            // TODO: Conversations, local-only deletes, attachments
            for d in delete.message_deletes.iter().flat_map(|m| &m.messages) {
                let sender = match &d.author {
                    Some(Author::AuthorServiceId(id)) => {
                        ServiceId::parse_from_service_id_string(id)
                    }
                    Some(Author::AuthorServiceIdBinary(id)) => {
                        ServiceId::parse_from_service_id_binary(id)
                    }
                    Some(Author::AuthorE164(_)) => None,
                    None => None,
                };
                let Some(sender) = sender else {
                    tracing::warn!("Could not parse author of delete-for-self message; ignoring.");
                    continue;
                };
                let Some(timestamp) = d.sent_timestamp else {
                    tracing::warn!("Timestamp of delete-for-self message not given; ignoring.");
                    continue;
                };
                let Ok(Some(thread)) = store
                    .thread_for_sender_and_timestamp(&sender, timestamp)
                    .await
                else {
                    tracing::warn!(
                        "Message referenced by delete-for-self message not found; ignoring."
                    );
                    continue;
                };
                // Note: Not marking the message as deleted, like when receiving deletion requests by others.
                // This matches the behavior of Signal Desktop, where the message completely disappears from the timeline.
                let result = store.delete_message(&thread, timestamp).await;
                if !result.is_ok_and(|d| d) {
                    tracing::warn!(
                        "Could not delete message referenced by delete-for-self message; ignoring."
                    );
                }
            }
            None
        }
        ContentBody::EditMessage(EditMessage {
            target_sent_timestamp: Some(_),
            data_message: Some(_),
        })
        | ContentBody::SynchronizeMessage(SyncMessage {
            content:
                Some(SyncContent::Sent(sync_message::Sent {
                    edit_message:
                        Some(EditMessage {
                            target_sent_timestamp: Some(_),
                            data_message: Some(_),
                        }),
                    ..
                })),
            ..
        }) => Some(message),
        ContentBody::CallMessage(_)
        | ContentBody::SynchronizeMessage(SyncMessage {
            content: Some(SyncContent::CallEvent(_)),
            ..
        }) => Some(message),
        ContentBody::SynchronizeMessage(msg) => {
            debug!(
                ?msg,
                "skipping saving sync message without interesting fields"
            );
            None
        }
        ContentBody::ReceiptMessage(_) => Some(message),
        ContentBody::TypingMessage(msg) => {
            debug!(?msg, "skipping saving typing message");
            None
        }
        ContentBody::SenderKeyDistributionMessage(msg) => {
            debug!(
                distribution_id = ?msg.distribution_id().ok(),
                "skipping saving sender key distribution message"
            );
            None
        }
        ContentBody::StoryMessage(msg) => {
            debug!(?msg, "skipping story message");
            None
        }
        ContentBody::EditMessage(msg) => {
            debug!(?msg, "invalid edited");
            None
        }
    };

    Ok(message.map(|message| (thread, message)))
}

async fn save_message<S: Store>(
    store: &mut S,
    identified_websocket: &mut websocket::SignalWebSocket<websocket::Identified>,
    message: Content,
    override_thread: Option<Thread>,
) -> Result<(), Error<S::Error>> {
    if let Some((thread, message)) =
        prepare_message_for_save(store, identified_websocket, message, override_thread).await?
    {
        store.save_message(&thread, message).await?;
    }
    Ok(())
}

/// KT (receive batching): persist a whole receive window with one store
/// transaction (official Desktop MessageReceiver.preload.ts:1003-1123).
/// Per-message preparation keeps its exact ordering and side effects; only
/// the final writes are merged. A batch failure is logged by the caller —
/// the messages are still yielded and acked downstream, same as the
/// per-message path.
async fn save_messages_batch<S: Store>(
    store: &mut S,
    identified_websocket: &mut websocket::SignalWebSocket<websocket::Identified>,
    messages: Vec<Content>,
) -> Result<(), Error<S::Error>> {
    let mut prepared = Vec::with_capacity(messages.len());
    for message in messages {
        if let Some(pair) =
            prepare_message_for_save(store, identified_websocket, message, None).await?
        {
            prepared.push(pair);
        }
    }
    store.save_messages(prepared).await?;
    Ok(())
}

/// KT (envelope retention): an envelope in the current processing window —
/// fresh arrivals start at `attempts == 0`; entries already held for retry
/// carry their failure count (official Desktop unprocessed `attempts` face).
struct WindowEnvelope {
    envelope: Envelope,
    ack: EnvelopeAck,
    attempts: u32,
}

/// KT (envelope retention): open-retry bound (official Desktop keeps failed
/// envelopes with a bounded attempts counter; the value follows the task
/// brief) and the quiet-stream delay between retry windows — fresh traffic
/// always preempts the timer.
const OPEN_RETRY_MAX_ATTEMPTS: u32 = 3;
const OPEN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// KT (envelope retention / receive storm): should this envelope be held for
/// another retry window? `attempts` is the count *after* this failure, carried
/// across windows by [`WindowEnvelope`].
///
/// Split out as a pure function because the non-obvious bug lived exactly
/// here: the retry window used to rebuild its entries as fresh arrivals
/// (`attempts: 0`), so every retry counted as the first one and the bound was
/// unreachable — an unbounded retry wearing a bound's clothes.
fn envelope_retry_is_bounded(attempts: u32) -> bool {
    attempts < OPEN_RETRY_MAX_ATTEMPTS
}

/// KT (envelope retention / receive storm): failure-class literal for
/// retention bookkeeping — typed path only, no payload.
///
/// `duplicate` is called out separately from the rest of the protocol family:
/// it is the one class that carries an "already handled" meaning (libsignal
/// `DuplicatedMessage` — `double_ratchet.rs:222`; the text is
/// `message with old counter {chain_index} / {counter}`), so it must never
/// enter the retry hold.
fn classify_open_error(error: &libsignal_service::prelude::ServiceError) -> &'static str {
    use libsignal_service::prelude::ServiceError;
    if open_error_is_already_processed(error) {
        return "duplicate";
    }
    match error {
        ServiceError::InvalidFrame { .. }
        | ServiceError::MacError
        | ServiceError::Padding(_)
        | ServiceError::PaddingVersion(_) => "frame",
        ServiceError::SignalProtocolError(_) => "protocol",
        _ => "other",
    }
}

/// KT (receive storm): this open failure can never succeed on a later attempt,
/// and its official meaning is "already processed" — acked and dropped on
/// sight instead of being held for the bounded retry.
///
/// Anchor: libsignal `SignalProtocolError::DuplicatedMessage` is raised when
/// the message key for that counter was already consumed (either the chain
/// advanced past it or the cached skipped key was taken), so the ciphertext is
/// unrecoverable by design. Official Desktop treats it as terminal — the
/// duplicate check runs before any receipt/retry bookkeeping and the envelope
/// leaves the cache (`MessageReceiver.preload.ts:2016-2020`, "We don't do
/// anything if it's just a duplicated message"). Retrying it is what turned a
/// post-restart redelivery backlog into a receive-storm.
///
/// Both shapes are matched: the bare protocol error (identified/legacy
/// envelope) and the sealed-sender wrapper, whose `inner` carries the same
/// error while `sender` is preserved.
fn open_error_is_already_processed(error: &libsignal_service::prelude::ServiceError) -> bool {
    fn is_duplicate(error: &libsignal_service::protocol::SignalProtocolError) -> bool {
        matches!(
            error,
            libsignal_service::protocol::SignalProtocolError::DuplicatedMessage(_, _)
        )
    }
    match error {
        libsignal_service::prelude::ServiceError::SignalProtocolError(inner) => is_duplicate(inner),
        libsignal_service::prelude::ServiceError::SealedSenderDecryptionError(wrapped) => {
            is_duplicate(&wrapped.inner)
        }
        _ => false,
    }
}

/// KT (group fetch degradation): in-call retry bound for decode-class group
/// fetch failures.
const GROUP_FETCH_MAX_ATTEMPTS: u32 = 2;

/// KT (group fetch degradation): decode-class group fetch failure — a fetch
/// or decrypt that failed structurally (payload/protobuf/zkgroup decode)
/// rather than transiently. Retried once in-call, then surfaced as
/// [`Error::GroupSyncFailed`] instead of silently serving a stale group.
fn group_fetch_decode_class(error: &libsignal_service::prelude::ServiceError) -> bool {
    use libsignal_service::prelude::ServiceError;
    matches!(
        error,
        ServiceError::GroupsV2DecryptionError(_)
            | ServiceError::ProtobufDecodeError(_)
            | ServiceError::ZkGroupDeserializationFailure(_)
            | ServiceError::ZkGroupVerificationFailure(_)
            | ServiceError::GroupsV2Error
    )
}

/// KT (group fetch degradation): the group no longer exists server-side
/// (HTTP 404 — official GROUP_NONEXISTENT).
fn group_fetch_gone(error: &libsignal_service::prelude::ServiceError) -> bool {
    matches!(
        error,
        libsignal_service::prelude::ServiceError::NotFoundError
    )
}

/// Why collecting a receive-batch window stopped.
#[derive(Debug, PartialEq, Eq)]
enum BatchWindowEnd {
    /// No more items arrived before the window closed (steady state).
    WindowClosed,
    /// The caller-supplied predicate matched (e.g. queue-empty marker).
    Stop,
    /// The upstream stream errored (item dropped; batch proceeds with what
    /// it has; the error is logged).
    StreamError,
    /// The upstream stream ended.
    EndOfStream,
}

/// KT (receive batching): pull up to `max - 1` more items within `window`
/// after `first`. Pure over the item type so the windowing stays unit
/// testable without protocol fixtures.
async fn collect_window<S, T, E>(
    stream: &mut S,
    first: T,
    window: std::time::Duration,
    max: usize,
    is_stop: impl Fn(&T) -> bool,
) -> (Vec<T>, BatchWindowEnd)
where
    S: futures::Stream<Item = Result<T, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut items = vec![first];
    let mut end = BatchWindowEnd::WindowClosed;
    while items.len() < max {
        match tokio::time::timeout(window, stream.next()).await {
            Ok(Some(Ok(item))) => {
                let stop = is_stop(&item);
                items.push(item);
                if stop {
                    end = BatchWindowEnd::Stop;
                    break;
                }
            }
            Ok(Some(Err(error))) => {
                tracing::error!(%error, "unexpected error in message receiving loop");
                end = BatchWindowEnd::StreamError;
                break;
            }
            Ok(None) => {
                end = BatchWindowEnd::EndOfStream;
                break;
            }
            Err(_) => break, // window closed
        }
    }
    (items, end)
}

/// KT (receive batching): keys-request side effect of a queue-empty marker
/// arriving first-of-pull. Mid-window queue-empty markers are forwarded as
/// events only — the sync request is a connect-time bootstrap, not a
/// per-marker obligation.
fn request_keys_from_primary_if_needed<AciStore>(
    account_entropy_pool: &Option<AccountEntropyPool>,
    message_sender: &MessageSender<AciStore>,
) where
    AciStore: Clone
        + 'static
        + libsignal_service::protocol::ProtocolStore
        + libsignal_service::protocol::SenderKeyStore
        + SessionStoreExt
        + Sync,
{
    if account_entropy_pool.is_none() {
        debug!("device does not have the needed keys; requesting from primary device");

        let mut message_sender = message_sender.clone();
        tokio::task::spawn_local(async move {
            let result = message_sender
                .send_sync_message(SyncMessage {
                    content: Some(SyncContent::Request(sync_message::Request {
                        r#type: Some(sync_message::request::Type::Keys.into()),
                    })),
                    ..SyncMessage::with_padding(&mut rand::rng())
                })
                .await;

            if let Err(error) = result {
                warn!(%error, "Error sending blocked contacts to other devices");
            }
        });
    }
}

async fn upsert_contact_from_profile<S: Store>(
    mut store: S,
    mut identified_websocket: SignalWebSocket<websocket::Identified>,
    data_message: &DataMessage,
    sender: ServiceId,
    profile_key: ProfileKey,
) -> Result<(), Error<<S as Store>::Error>> {
    if store.contact_by_id(&sender).await?.is_none()
        || store
            .profile_key(&sender)
            .await?
            .is_none_or(|p| p.bytes != profile_key.bytes)
    {
        if let Some(aci) = sender.aci() {
            let sender_uuid: Uuid = aci.into();
            let encrypted_profile = identified_websocket
                .retrieve_profile_by_id(aci, Some(profile_key))
                .await?;
            let profile_cipher = ProfileCipher::new(profile_key);
            let decrypted_profile = profile_cipher.decrypt(encrypted_profile)?;

            let contact = Contact {
                uuid: sender_uuid,
                phone_number: None,
                name: decrypted_profile
                    .name
                    // FIXME: this assumes [firstname] [lastname]
                    .map(|pn| {
                        if let Some(family_name) = pn.family_name {
                            format!("{} {}", pn.given_name, family_name)
                        } else {
                            pn.given_name
                        }
                    })
                    .unwrap_or_default(),
                profile_key: profile_key.bytes.to_vec(),
                expire_timer: data_message.expire_timer.unwrap_or_default(),
                expire_timer_version: data_message.expire_timer_version.unwrap_or(1),
                inbox_position: 0,
                avatar: None,
                verified: Verified::default(),
            };

            info!(%sender_uuid, "saved contact on first sight");
            store.save_contact(&contact).await?;
            store.upsert_profile_key(&sender_uuid, profile_key).await?;
        } else {
            debug!("not storing profile for PNI contact");
        }
    }
    Ok(())
}

/// KT (handover race, §29.6): pre-keys 注册被 WS handover 杀死后的重试
/// 参数。单连接纪律的 kill-before-create handover 不看前连接的在途请求：
/// 在途的 pre-keys 注册以 `WsClosing`（"Websocket closing: ..."）随连接
/// 死亡（2026-10-23 09:30 故障链：重链接 → 接收流拨号 handover →
/// "should never happen" ERROR + 3s 附件 UPSTREAM_ERROR 降级窗 + 推送
/// 延迟 16-24s）。pre-keys 上传幂等（服务器按内容补水位），等 handover
/// 稳定后在新连接上重发安全。
const PRE_KEYS_HANDOVER_RETRIES: usize = 2;
const PRE_KEYS_HANDOVER_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(500);

/// KT (handover race, §29.6): 判定错误是否为 WS 被单连接纪律 handover
/// 杀掉一类（在途请求以 `WsClosing` 告终）。其余错误（协议/存储/HTTP
/// 4xx-5xx）语义上不是断连，重试无意义。
fn is_ws_handover_kill<S: std::error::Error>(error: &Error<S>) -> bool {
    matches!(error, Error::ServiceError(ServiceError::WsClosing { .. }))
}

/// KT (handover race, §29.6): pre-keys 注册的 WsClosing 重试骨架。
/// `attempt` 每次调用都应拿到可用连接（初次 = 调用方现成连接；重试 =
/// 调用方经 slot 新拨/复用）。上限：初试 1 次 + 重试
/// [`PRE_KEYS_HANDOVER_RETRIES`] 次，重试前等
/// [`PRE_KEYS_HANDOVER_RETRY_DELAY`] 让 handover 稳定。骨架与拨号解耦，
/// 单测用假 attempt 注入断连序列（`prekeys_handover_retry_tests`）。
async fn register_with_ws_handover_retry<E, F, Fut>(
    mut attempt: F,
) -> Result<(), Error<E>>
where
    E: std::error::Error,
    F: FnMut() -> Fut,
    Fut: futures::Future<Output = Result<(), Error<E>>>,
{
    let mut retries_done: usize = 0;
    loop {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(error)
                if is_ws_handover_kill(&error) && retries_done < PRE_KEYS_HANDOVER_RETRIES =>
            {
                retries_done += 1;
                warn!(
                    %error,
                    attempt = retries_done,
                    retries_left = PRE_KEYS_HANDOVER_RETRIES - retries_done,
                    "pre-keys registration killed by ws handover; retrying on a fresh connection"
                );
                tokio::time::sleep(PRE_KEYS_HANDOVER_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// KT (handover race, §29.6): pre-keys 注册专用的连接供给 ——
/// `require_unused=false` 经单连接 slot 拨号：slot 里的活连接直接复用
/// （pre-keys 上传不需要 server-request stream），仅当连接已死才重拨
/// 替换；不会杀占用 request stream 的消息管道连接。组件显式传参
/// （credentials/push_service），供接收流的 `async move` 任务在无法借用
/// `self` 时复用同一拨号纪律。
async fn dial_pre_keys_connection(
    push_service: &PushService,
    credentials: &ServiceCredentials,
) -> Result<AccountManager, ServiceError> {
    let key = credentials.login();
    websocket::slot::acquire_identified(&key, false, async {
        let headers = &[("X-Signal-Receive-Stories", "false")];
        let mut push_service = push_service.clone();
        push_service
            .ws(
                "/v1/websocket/",
                "/v1/keepalive",
                headers,
                Some(credentials.clone()),
            )
            .await
    })
    .await
    .map(|ws| AccountManager::new(push_service.clone(), ws, None))
}

async fn register_pre_keys<S: Store>(
    store: &S,
    account_manager: &mut AccountManager,
) -> Result<(), Error<S::Error>> {
    trace!("registering pre keys");

    account_manager
        .update_pre_key_bundle(&mut store.aci_protocol_store(), ServiceIdKind::Aci, true)
        .await?;

    account_manager
        .update_pre_key_bundle(&mut store.pni_protocol_store(), ServiceIdKind::Pni, true)
        .await?;

    trace!("registered pre keys");
    Ok(())
}

/// Identified fuse (KT): (recipient profile key, acquired certificate) →
/// unidentified access. A missing certificate (acquisition error mapped to
/// `None` by the caller) yields `None`, which routes MessageSender through
/// its identified branch — an expired/unreachable certificate must not fail
/// the send. A missing profile key yields `None` as before.
/// `pub` so the engine workspace (this fork's only consumer) can pin the
/// fallback contract in its test gate; not part of the upstream API surface.
pub fn unidentified_access_or_identified(
    profile_key: Option<ProfileKey>,
    sender_certificate: Option<SenderCertificate>,
) -> Option<UnidentifiedAccess> {
    profile_key
        .zip(sender_certificate)
        .map(|(profile_key, certificate)| UnidentifiedAccess {
            key: profile_key.derive_access_key().to_vec(),
            certificate,
        })
}

/// KT: thread-scoped redelivery verdict for the (sender, client_timestamp)
/// gate — a stored row only suppresses re-projection when it lives in the
/// same thread as the incoming content (one timestamp, two groups is legal).
/// An underivable incoming thread keeps the conservative global suppression.
fn is_thread_scoped_redelivery(stored: Option<Thread>, incoming: Option<Thread>) -> bool {
    match (stored, incoming) {
        (Some(stored), Some(incoming)) => stored == incoming,
        (Some(_), None) => true,
        _ => false,
    }
}

#[cfg(test)]
mod receive_batch_tests {
    use super::*;
    use futures::StreamExt;
    use std::convert::Infallible;

    type TestStream = futures::stream::Map<
        futures::channel::mpsc::UnboundedReceiver<u32>,
        fn(u32) -> Result<u32, Infallible>,
    >;

    fn stream_of(rx: futures::channel::mpsc::UnboundedReceiver<u32>) -> TestStream {
        rx.map(Ok as fn(u32) -> Result<u32, Infallible>)
    }

    fn channel() -> (
        futures::channel::mpsc::UnboundedSender<u32>,
        futures::channel::mpsc::UnboundedReceiver<u32>,
    ) {
        futures::channel::mpsc::unbounded()
    }

    #[tokio::test(start_paused = true)]
    async fn collects_a_burst_then_the_window_closes() {
        let (tx, mut rx) = channel();
        for item in 1..=3 {
            tx.unbounded_send(item).unwrap();
        }
        let first = rx.next().await.unwrap();
        let mut stream = stream_of(rx);

        let (items, end) = collect_window(
            &mut stream,
            first,
            std::time::Duration::from_millis(75),
            30,
            |_| false,
        )
        .await;

        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(end, BatchWindowEnd::WindowClosed);
        // A late sender is not pulled once the window has closed.
        tx.unbounded_send(4).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_is_capped_at_max() {
        let (tx, mut rx) = channel();
        for item in 1..=40 {
            tx.unbounded_send(item).unwrap();
        }
        let first = rx.next().await.unwrap();
        let mut stream = stream_of(rx);

        let (items, end) = collect_window(
            &mut stream,
            first,
            std::time::Duration::from_millis(75),
            30,
            |_| false,
        )
        .await;

        assert_eq!(items.len(), 30);
        assert_eq!(end, BatchWindowEnd::WindowClosed);
        // The overflow stays buffered for the next window.
        assert_eq!(stream.next().await.unwrap().unwrap(), 31);
    }

    #[tokio::test(start_paused = true)]
    async fn the_stop_predicate_ends_the_window() {
        let (tx, mut rx) = channel();
        for item in [1, 2, 99, 4] {
            tx.unbounded_send(item).unwrap();
        }
        let first = rx.next().await.unwrap();
        let mut stream = stream_of(rx);

        let (items, end) = collect_window(
            &mut stream,
            first,
            std::time::Duration::from_millis(75),
            30,
            |item| *item == 99,
        )
        .await;

        assert_eq!(items, vec![1, 2, 99]);
        assert_eq!(end, BatchWindowEnd::Stop);
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_stream_ends_the_window() {
        let (tx, rx) = channel();
        drop(tx);
        let mut stream = stream_of(rx);

        let (items, end) = collect_window(
            &mut stream,
            1,
            std::time::Duration::from_millis(75),
            30,
            |_| false,
        )
        .await;

        assert_eq!(items, vec![1]);
        assert_eq!(end, BatchWindowEnd::EndOfStream);
    }

    #[tokio::test(start_paused = true)]
    async fn an_errored_stream_ends_the_window() {
        let mut stream = futures::stream::iter(vec![
            Err(std::io::Error::other("boom")),
            Ok::<u32, std::io::Error>(3),
        ]);

        let (items, end) = collect_window(
            &mut stream,
            1,
            std::time::Duration::from_millis(75),
            30,
            |_| false,
        )
        .await;

        assert_eq!(items, vec![1]);
        assert_eq!(end, BatchWindowEnd::StreamError);
    }

    #[test]
    fn redelivery_gate_is_thread_scoped() {
        let group_a = Thread::Group([1u8; 32]);
        let group_b = Thread::Group([2u8; 32]);
        let contact = Thread::Contact(ServiceId::Aci(Aci::from(Uuid::nil())));
        // same thread → redelivery (suppress)
        assert!(is_thread_scoped_redelivery(
            Some(group_a.clone()),
            Some(group_a.clone())
        ));
        // one timestamp, two groups → both project
        assert!(!is_thread_scoped_redelivery(Some(group_a), Some(group_b)));
        // nothing stored → project
        assert!(!is_thread_scoped_redelivery(None, Some(contact.clone())));
        assert!(!is_thread_scoped_redelivery(None, None));
        // underivable incoming thread → conservative global suppression
        assert!(is_thread_scoped_redelivery(Some(contact), None));
    }

    // KT (envelope retention): failure classes are fixed literals for the
    // bounded-retry bookkeeping (typed path, no payload); the give-up bound
    // is the constant the retention loop tests against.
    #[test]
    fn open_error_classification_and_retry_bound() {
        use libsignal_service::prelude::ServiceError;

        assert_eq!(
            classify_open_error(&ServiceError::MacError),
            "frame",
            "frame-integrity family"
        );
        assert_eq!(
            classify_open_error(&ServiceError::InvalidFrame { reason: "x" }),
            "frame"
        );
        assert_eq!(
            classify_open_error(&ServiceError::SignalProtocolError(
                libsignal_service::protocol::SignalProtocolError::InvalidArgument(
                    "live sample class".into()
                )
            )),
            "protocol"
        );
        assert_eq!(
            classify_open_error(&ServiceError::UnsupportedContent),
            "other",
            "everything else degrades to the catch-all class"
        );
        assert_eq!(OPEN_RETRY_MAX_ATTEMPTS, 3);
    }

    // KT (receive storm): `DuplicatedMessage` is the "already handled" class —
    // it is named apart from the rest of the protocol family and never enters
    // the retry hold. The live sample is `message with old counter 354 / 351`
    // (libsignal double_ratchet.rs:222: the message key was already consumed).
    #[test]
    fn duplicated_message_is_already_processed_and_not_retried() {
        use libsignal_service::prelude::ServiceError;
        use libsignal_service::protocol::SignalProtocolError;

        let duplicated =
            ServiceError::SignalProtocolError(SignalProtocolError::DuplicatedMessage(354, 351));
        assert_eq!(classify_open_error(&duplicated), "duplicate");
        assert!(open_error_is_already_processed(&duplicated));
        // Evidence pin: this is the exact text the live engine wrote 58524
        // times (`error=Protocol error: message with old counter 354 / 351`).
        // If libsignal ever rewords it, this assertion fails and points at the
        // classification the incident depended on.
        assert_eq!(
            duplicated.to_string(),
            "Protocol error: message with old counter 354 / 351"
        );

        // The sealed-sender layer wraps protocol failures while keeping the
        // sender — the same duplicate arrives in this shape and must classify
        // the same way, otherwise it would emit a retry receipt (DERM) and the
        // peer would resend bytes that can never decrypt.
        let wrapped = ServiceError::SealedSenderDecryptionError(
            libsignal_service::cipher::SealedSenderDecryptionError {
                inner: SignalProtocolError::DuplicatedMessage(354, 351),
                sender: None,
            },
        );
        assert_eq!(classify_open_error(&wrapped), "duplicate");
        assert!(open_error_is_already_processed(&wrapped));

        // A sealed-sender failure that is NOT a duplicate keeps the existing
        // DERM path (sender face), not the drop path.
        let sealed_other = ServiceError::SealedSenderDecryptionError(
            libsignal_service::cipher::SealedSenderDecryptionError {
                inner: SignalProtocolError::InvalidArgument("not a duplicate".into()),
                sender: None,
            },
        );
        assert_eq!(classify_open_error(&sealed_other), "other");
        assert!(!open_error_is_already_processed(&sealed_other));

        // Other protocol failures keep the bounded retry: they are not
        // "already processed" and may be transient (e.g. a session that a
        // later prekey message repairs).
        let other_protocol = ServiceError::SignalProtocolError(
            SignalProtocolError::InvalidArgument("not a duplicate".into()),
        );
        assert_eq!(classify_open_error(&other_protocol), "protocol");
        assert!(!open_error_is_already_processed(&other_protocol));

        assert!(!open_error_is_already_processed(&ServiceError::MacError));
        assert!(!open_error_is_already_processed(
            &ServiceError::InvalidFrame { reason: "x" }
        ));
    }

    // KT (receive storm): the retry bound must actually be reachable. The live
    // incident had 58524 `held for a bounded retry attempts=1` lines and ZERO
    // give-ups in three log generations: the counter restarted every window.
    // This walks one envelope across windows the way the loop does.
    #[test]
    fn retry_counter_reaches_the_bound_across_windows() {
        let mut attempts = 0u32;
        let mut window_entries = 0usize;
        // Each iteration = one window: open fails, we decide hold-or-give-up,
        // and the held entry carries the incremented counter to the next one.
        for window in 0..10 {
            attempts += 1;
            if envelope_retry_is_bounded(attempts) {
                window_entries += 1;
                assert!(
                    attempts < OPEN_RETRY_MAX_ATTEMPTS,
                    "held entry must be below the bound (window {window})"
                );
            } else {
                break;
            }
        }
        assert_eq!(
            window_entries,
            OPEN_RETRY_MAX_ATTEMPTS as usize - 1,
            "exactly two holds, then give up on the third failure"
        );
        assert_eq!(attempts, OPEN_RETRY_MAX_ATTEMPTS);
        assert!(!envelope_retry_is_bounded(OPEN_RETRY_MAX_ATTEMPTS));
    }

    // KT (group fetch degradation): the decode/gone partition decides the
    // retry-then-structured-error path vs the fail-open fallback at every
    // group fetch call site; the retry bound matches the in-call loop.
    #[test]
    fn group_fetch_failure_partition_and_bound() {
        use libsignal_service::prelude::ServiceError;

        assert!(group_fetch_decode_class(&ServiceError::GroupsV2Error));
        assert!(group_fetch_decode_class(
            &ServiceError::GroupsV2DecryptionError(
                libsignal_service::groups_v2::GroupDecodingError::WrongBlob
            )
        ));
        assert!(!group_fetch_decode_class(&ServiceError::Unauthorized));
        assert!(!group_fetch_decode_class(
            &ServiceError::RateLimitExceeded { retry_after: None }
        ));

        assert!(group_fetch_gone(&ServiceError::NotFoundError));
        assert!(!group_fetch_gone(&ServiceError::Unauthorized));

        assert_eq!(GROUP_FETCH_MAX_ATTEMPTS, 2);
    }
}


/// KT (handover race, §29.6): WsClosing 重试骨架的注入式单测 —— 不触网，
/// 用假 attempt 序列钉死「断连一次 → 新连接重试成功 / 非断连不重试 /
/// 重试上限后放弃」三条语义。
#[cfg(test)]
mod prekeys_handover_retry_tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    type TestError = Error<Infallible>;

    /// 故障链 2026-10-23 09:30 的原始错误形态：在途请求随被 handover 杀掉
    /// 的连接死亡（libsignal websocket/mod.rs:829 的 reason 字面量）。
    fn ws_closing() -> TestError {
        TestError::ServiceError(ServiceError::WsClosing {
            reason: "WebSocket closing while waiting for a response",
        })
    }

    #[tokio::test(start_paused = true)]
    async fn retries_once_after_a_websocket_closing_kill_and_succeeds() {
        // mock 断连一次：首次请求在途时连接被 handover 杀掉（WsClosing），
        // 第二次（新连接）成功 —— 引擎侧观测即「重试成功，无
        // should-never-happen ERROR」。
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let result: Result<(), TestError> = register_with_ws_handover_retry(move || {
            let attempt_no = observed.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt_no == 0 {
                    Err(ws_closing())
                } else {
                    Ok(())
                }
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "retry on a fresh connection must succeed: {result:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "exactly one retry expected (initial + 1)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn non_handover_errors_fail_fast_without_retry() {
        // 协议/存储/HTTP 类失败不是断连：重发无意义，必须原样上抛。
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let result: Result<(), TestError> = register_with_ws_handover_retry(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            async move { Err(TestError::UpdatePreKeyFailure) }
        })
        .await;

        assert!(matches!(result, Err(TestError::UpdatePreKeyFailure)));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no retry for non-handover errors");
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_the_retry_cap_on_persistent_handover_kills() {
        // 每次尝试都被 handover 杀：初试 1 + 重试 2 = 3 次后放弃，原错误
        // 上抛（引擎侧据此落降级态，不无限重试）。
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let result: Result<(), TestError> = register_with_ws_handover_retry(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            async move { Err(ws_closing()) }
        })
        .await;

        assert!(matches!(
            result,
            Err(TestError::ServiceError(ServiceError::WsClosing { .. }))
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1 + PRE_KEYS_HANDOVER_RETRIES,
            "initial attempt plus the bounded retries"
        );
    }
}
