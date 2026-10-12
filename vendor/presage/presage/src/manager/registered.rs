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
    push_service::{PushService, ServiceIds, DEFAULT_DEVICE_ID},
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
use crate::model::messages::{ContactSync, DecryptionFailure};
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
    pub(crate) identified_websocket: Arc<Mutex<Option<SignalWebSocket<websocket::Identified>>>>,
    pub(crate) unidentified_websocket: Arc<Mutex<Option<SignalWebSocket<websocket::Unidentified>>>>,
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
            identified_websocket: Default::default(),
            unidentified_websocket: Default::default(),
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
    async fn identified_websocket(
        &self,
        require_unused: bool,
    ) -> Result<SignalWebSocket<websocket::Identified>, Error<S::Error>> {
        let mut identified_ws = self.state.identified_websocket.lock().await;
        match identified_ws
            .as_ref()
            .filter(|ws| !ws.is_closed())
            .filter(|ws| !(require_unused && ws.is_used()))
        {
            Some(ws) => Ok(ws.clone()),
            None => {
                let headers = &[("X-Signal-Receive-Stories", "false")];
                let ws = self
                    .identified_push_service()
                    .ws(
                        "/v1/websocket/",
                        "/v1/keepalive",
                        headers,
                        Some(self.credentials()),
                    )
                    .await?;
                identified_ws.replace(ws.clone());
                debug!("initialized identified websocket");

                Ok(ws)
            }
        }
    }

    /// Returns the current unidentified websocket, or creates a new one
    ///
    /// A new one is created if the current websocket is closed, or if there is none yet.
    async fn unidentified_websocket(
        &self,
    ) -> Result<SignalWebSocket<websocket::Unidentified>, Error<S::Error>> {
        let mut unidentified_ws = self.state.unidentified_websocket.lock().await;
        match unidentified_ws.as_ref().filter(|ws| !ws.is_closed()) {
            Some(ws) => Ok(ws.clone()),
            None => {
                let ws = self
                    .unidentified_push_service()
                    .ws("/v1/websocket/", "/v1/keepalive", &[], None)
                    .await?;
                unidentified_ws.replace(ws.clone());
                debug!("initialized unidentified websocket");

                Ok(ws)
            }
        }
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
            let sender_certificate = self
                .identified_websocket(false)
                .await?
                .get_uuid_only_sender_certificate()
                .await?;
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
        let Some(group) = upsert_group(
            &self.store,
            &mut gm,
            context.master_key(),
            &context.revision(),
        )
        .await?
        else {
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
        let response = groups_manager
            .fetch_encrypted_group_response(&mut rand::rng(), master_key_bytes)
            .await;
        let endorsements = response
            .as_ref()
            .ok()
            .map(|response| response.group_send_endorsements_response.clone())
            .filter(|bytes| !bytes.is_empty());
        match response {
            Ok(response) => {
                if let Some(encrypted_group) = response.group {
                    let group = decrypt_group(master_key_bytes, encrypted_group)?;
                    if let Err(error) = self
                        .store
                        .save_group(master_key_bytes.try_into()?, group.clone())
                        .await
                    {
                        error!(%error, "failed to save group");
                    }
                }
            }
            Err(error) => {
                warn!(%error, "failed to fetch encrypted group with endorsements")
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
        let refresh_registration_task = async move {
            if let Err(error) = register_pre_keys(&store_inner, &mut account_manager).await {
                error!(%error, "failed to register pre-keys, this is problematic and should never happen!");
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
                    match state.encrypted_messages.next().await {
                        Some(Ok(Incoming::Envelope(envelope, ack))) => {
                            // KT (receive batching): collect the official
                            // 75ms/30 window (MessageReceiver.preload.ts
                            // :359-366). Decryption stays sequential — the
                            // cipher takes &mut self with a shared CSPRNG on
                            // a !Send stream — so the batch wins come from
                            // the merged single-transaction save below.
                            let (incoming, window_end) = collect_window(
                                &mut state.encrypted_messages,
                                Incoming::Envelope(envelope, ack),
                                Self::RECEIVE_BATCH_WINDOW,
                                Self::RECEIVE_BATCH_MAX,
                                |item| matches!(item, Incoming::QueueEmpty),
                            )
                            .await;
                            if window_end == BatchWindowEnd::EndOfStream {
                                state.end_of_stream = true;
                            }
                            let mut window: Vec<(Envelope, EnvelopeAck)> =
                                Vec::with_capacity(incoming.len());
                            let mut queue_empty_seen = false;
                            for item in incoming {
                                match item {
                                    Incoming::Envelope(envelope, ack) => {
                                        window.push((envelope, ack))
                                    }
                                    Incoming::QueueEmpty => queue_empty_seen = true,
                                }
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
                            let mut batch_seen: std::collections::HashSet<(
                                String,
                                u64,
                                Option<Thread>,
                            )> = Default::default();

                            for (envelope, ack) in window {
                                // KT (DERM): retry-receipt material captured before the
                                // envelope is consumed by decryption; only spent on the
                                // failure path below.
                                let retry_material = RetryReceiptMaterial::of(&envelope);
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
                                        if let ContentBody::DecryptionErrorMessage(e) =
                                            &content.body
                                        {
                                            error!(
                                                error = ?e,
                                                "got error decrypting a message"
                                            );
                                            let timestamp = u64::try_from(
                                                content
                                                    .metadata
                                                    .client_timestamp
                                                    .timestamp_millis(),
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
                                                    requester_device: content
                                                        .metadata
                                                        .sender_device
                                                        .into(),
                                                },
                                                // DERM without a timestamp names no
                                                // original message; nothing to act
                                                // on — ack and drop (no projection).
                                                None => {
                                                    ack.ack();
                                                    continue;
                                                }
                                            };
                                            slots.push(BatchSlot::Ready(
                                                Received::DecryptionError(
                                                    content.metadata.sender,
                                                    timestamp,
                                                    Some(failure),
                                                    ack,
                                                ),
                                            ));
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
                                            match state
                                                .message_receiver
                                                .retrieve_contacts(contacts)
                                                .await
                                            {
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
                                                    slots.push(BatchSlot::Ready(
                                                        Received::Contacts(
                                                            ContactSync {
                                                                content: Box::new(content.clone()),
                                                                complete,
                                                                contacts: saved,
                                                            },
                                                            ack,
                                                        ),
                                                    ));
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
                                                            .remove_sticker_pack(
                                                                operation.pack_id(),
                                                            )
                                                            .await
                                                        {
                                                            Ok(was_present) => {
                                                                debug!(
                                                                    was_present,
                                                                    "removed stick pack"
                                                                )
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
                                            if state.registration_type == RegistrationType::Primary
                                            {
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
                                                                    master_key:
                                                                        Some(master_key_bytes),
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
                                            if let Ok(Some(group)) = upsert_group(
                                                &state.store,
                                                &mut state.groups_manager,
                                                master_key_bytes,
                                                revision,
                                            )
                                            .await
                                            {
                                                trace!(?group, "upserted group");
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
                                        let identity_changed =
                                            identity_changes.iter().any(|name| {
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
                                        error!(%error, "error opening envelope, message will be skipped!");
                                        let failure = retry_material.and_then(|m| m.into_failure());
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
                                        ack.ack();
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
                        Some(Ok(Incoming::QueueEmpty)) => {
                            debug!("got empty queue");
                            request_keys_from_primary_if_needed(
                                &state.account_entropy_pool,
                                &state.message_sender,
                            );
                            return Some((Received::QueueEmpty, state));
                        }
                        Some(Err(error)) => {
                            error!(%error, "unexpected error in message receiving loop")
                        }
                        None => return None,
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

        Ok(())
    }

    /// KT fork (stories): send a story message (`ContentBody::StoryMessage`)
    /// to a single recipient over the official story transport face
    /// (sealed sender + `?story=true`, see libsignal-service
    /// `MessageSender::send_story_message`).
    ///
    /// Differences from [`Self::send_message`], each mirroring the official
    /// client (behavioral comparison, no code copied):
    /// - the sender's own profile key is injected into the `StoryMessage`
    ///   (Signal-Desktop `ts/jobs/helpers/sendStory.preload.ts:72` refuses
    ///   to send without it, `:198-205` puts it in the proto — same shape
    ///   `send_message` applies to `DataMessage`);
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
    ) -> Result<(), Error<S::Error>> {
        let recipient = recipient.into();
        let mut content_body: ContentBody = message.into();

        // we need to put our profile key in StoryMessage
        if let ContentBody::StoryMessage(message) = &mut content_body {
            message
                .profile_key
                .get_or_insert(self.state.data.profile_key().get_bytes().to_vec());
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
            // KT (group send semantics): message sends refresh the group state
            // first (the u32::MAX revision comparison always triggers the fetch),
            // so the recipient list no longer eats stale memberships; a failed
            // fetch falls back to the stored group (fail-open). Typing stays on
            // the local state — official typing carries no group fetch.
            let revision = if group_refresh_needed(&content_body) {
                u32::MAX
            } else {
                0
            };
            upsert_group(
                &self.store,
                &mut groups_manager,
                &master_key_bytes,
                &revision,
            )
            .await?
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
        let results = sender
            .send_message_to_group(recipients, content_body.clone(), timestamp, online_only)
            .await;

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

        Ok(outcomes)
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
        let mut account_manager = self.account_manager().await?;
        register_pre_keys(&self.store, &mut account_manager).await
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

/// KT (group send semantics): typing skips the group-state refresh (official
/// typing carries no group fetch); real sends refresh before dispatch.
/// `pub` so the engine workspace (this fork's only consumer) can pin the
/// refresh contract in its test gate; not part of the upstream API surface.
pub fn group_refresh_needed(body: &ContentBody) -> bool {
    !matches!(body, ContentBody::TypingMessage(_))
}

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
        match groups_manager
            .fetch_encrypted_group(&mut rand::rng(), master_key_bytes)
            .await
        {
            Ok(encrypted_group) => {
                let group = decrypt_group(master_key_bytes, encrypted_group)?;
                if let Err(error) = store.save_group(master_key_bytes.try_into()?, group).await {
                    error!(%error, "failed to save group");
                }
            }
            Err(error) => {
                warn!(%error, "failed to fetch encrypted group")
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
}
