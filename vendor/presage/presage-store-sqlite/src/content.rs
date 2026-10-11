use std::ops::{Bound, RangeBounds};

use presage::{
    AvatarBytes,
    libsignal_service::{
        Profile,
        content::Metadata,
        prelude::{Content, ProfileKey, Uuid},
        protocol::ServiceId,
        zkgroup::GroupMasterKeyBytes,
    },
    model::{contacts::Contact, groups::Group},
    proto::{Verified, verified},
    store::{ContentsStore, StickerPack, Thread},
};
use sqlx::{query, query_as, query_scalar, types::Json};

use crate::{
    SqliteStore, SqliteStoreError,
    data::{SqlContact, SqlGroup, SqlMessage, SqlProfile, SqlStickerPack},
    error::SqlxErrorExt,
};

impl ContentsStore for SqliteStore {
    type ContentsStoreError = SqliteStoreError;

    type ContactsIter =
        Box<dyn Iterator<Item = Result<Contact, Self::ContentsStoreError>> + Send + Sync>;

    type GroupsIter = Box<
        dyn Iterator<Item = Result<(GroupMasterKeyBytes, Group), Self::ContentsStoreError>>
            + Send
            + Sync,
    >;

    type MessagesIter =
        Box<dyn Iterator<Item = Result<Content, Self::ContentsStoreError>> + Send + Sync>;

    type StickerPacksIter =
        Box<dyn Iterator<Item = Result<StickerPack, Self::ContentsStoreError>> + Send + Sync>;

    async fn clear_profiles(&mut self) -> Result<(), Self::ContentsStoreError> {
        let mut transaction = self.db.begin().await.into_protocol_error()?;
        query!("DELETE FROM profiles")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM profile_keys")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM profile_avatars")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await.into_protocol_error()?;
        Ok(())
    }

    async fn clear_contents(&mut self) -> Result<(), Self::ContentsStoreError> {
        let mut transaction = self.db.begin().await.into_protocol_error()?;
        query!("DELETE FROM thread_messages")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM threads")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM contacts")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM contacts_verification_state")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM groups")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM group_avatars")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM sticker_packs")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await.into_protocol_error()?;
        Ok(())
    }

    async fn clear_messages(&mut self) -> Result<(), Self::ContentsStoreError> {
        let mut transaction = self.db.begin().await.into_protocol_error()?;
        query!("DELETE FROM thread_messages")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM threads")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await.into_protocol_error()?;
        Ok(())
    }

    async fn clear_thread(&mut self, thread: &Thread) -> Result<(), Self::ContentsStoreError> {
        let (group_master_key, recipient_id) = thread.unzip();
        query!(
            "DELETE FROM thread_messages WHERE thread_id = (
                SELECT id FROM threads WHERE group_master_key = ? OR recipient_id = ?)",
            group_master_key,
            recipient_id,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn save_message(
        &self,
        thread: &Thread,
        content: Content,
    ) -> Result<(), Self::ContentsStoreError> {
        let mut tx = self.db.begin().await?;
        save_message_on_tx(&mut tx, thread, content).await?;
        tx.commit().await?;
        Ok(())
    }

    /// KT (receive batching): a whole receive window commits as a single
    /// transaction (official Desktop MessageReceiver.preload.ts:1003-1123).
    async fn save_messages(
        &self,
        messages: Vec<(Thread, Content)>,
    ) -> Result<(), Self::ContentsStoreError> {
        let mut tx = self.db.begin().await?;
        for (thread, content) in messages {
            save_message_on_tx(&mut tx, &thread, content).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn delete_message(
        &mut self,
        thread: &Thread,
        timestamp: u64,
    ) -> Result<bool, Self::ContentsStoreError> {
        let timestamp: i64 = timestamp
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidFormat)?;
        let (group_master_key, recipient_id) = thread.unzip();
        let res = query!(
            "DELETE FROM thread_messages
            WHERE ts = ? AND thread_id = (
                SELECT id FROM threads WHERE group_master_key = ? OR recipient_id = ?)",
            timestamp,
            group_master_key,
            recipient_id,
        )
        .execute(&self.db)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn message(
        &self,
        thread: &Thread,
        timestamp: u64,
    ) -> Result<Option<Content>, Self::ContentsStoreError> {
        let timestamp: i64 = timestamp
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidFormat)?;
        let (group_master_key, recipient_id) = thread.unzip();
        let message = query_as!(
            SqlMessage,
            r#"SELECT
                ts AS "ts: _",
                server_ts AS "server_ts: _",
                sender_service_id,
                sender_device_id AS "sender_device_id: _",
                destination_service_id,
                needs_receipt,
                unidentified_sender,
                content_body,
                was_plaintext,
                pni_verified
            FROM thread_messages
            WHERE ts = ? AND thread_id = (
                SELECT id FROM threads WHERE group_master_key = ? OR recipient_id = ?)"#,
            timestamp,
            group_master_key,
            recipient_id,
        )
        .fetch_optional(&self.db)
        .await?;
        message.map(|m| m.try_into()).transpose()
    }

    async fn messages(
        &self,
        thread: &Thread,
        range: impl RangeBounds<u64>,
    ) -> Result<Self::MessagesIter, Self::ContentsStoreError> {
        let (group_master_key, recipient_id) = thread.unzip();

        let (start_incl, start_excl) = range.start_bound().into_sql_bound();
        let (end_incl, end_excl) = range.end_bound().into_sql_bound();

        let rows = query_as!(
            SqlMessage,
            r#"SELECT
                ts AS "ts: _",
                server_ts AS "server_ts: _",
                sender_service_id,
                sender_device_id AS "sender_device_id: _",
                destination_service_id,
                needs_receipt,
                unidentified_sender,
                content_body,
                was_plaintext,
                pni_verified
            FROM thread_messages
            WHERE thread_id = (
                SELECT id FROM threads WHERE group_master_key = ? OR recipient_id = ?)
                AND coalesce(ts > ?, ts >= ?, true)
                AND coalesce(ts < ?, ts <= ?, true)
            ORDER BY ts DESC"#,
            group_master_key,
            recipient_id,
            start_incl,
            start_excl,
            end_incl,
            end_excl
        )
        .fetch_all(&self.db)
        .await?;

        Ok(Box::new(rows.into_iter().map(TryInto::try_into)))
    }

    async fn clear_contacts(&mut self) -> Result<(), Self::ContentsStoreError> {
        let mut transaction = self.db.begin().await.into_protocol_error()?;
        query!("DELETE FROM contacts")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM contacts_verification_state")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await.into_protocol_error()?;
        Ok(())
    }

    async fn save_contact(&mut self, contact: &Contact) -> Result<(), Self::ContentsStoreError> {
        let profile_key: &[u8] = contact.profile_key.as_ref();
        let avatar_bytes = contact.avatar.as_ref().map(|a| a.reader.to_vec());
        let phone_number = contact.phone_number.as_ref().map(|p| p.to_string());

        let mut tx = self.db.begin().await?;

        query!(
            "INSERT OR REPLACE INTO contacts
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            contact.uuid,
            phone_number,
            contact.name,
            profile_key,
            contact.expire_timer,
            contact.expire_timer_version,
            contact.inbox_position,
            avatar_bytes,
        )
        .execute(&mut *tx)
        .await?;

        let Verified {
            destination_aci,
            identity_key,
            state,
            ..
        } = &contact.verified;
        let is_verified = match verified::State::try_from(state.unwrap_or_default()) {
            Err(_) | Ok(verified::State::Default) => None,
            Ok(verified::State::Unverified) => Some(false),
            Ok(verified::State::Verified) => Some(true),
        };

        if let Some((destination_aci, identity_key)) =
            destination_aci.as_ref().zip(identity_key.as_ref())
        {
            query!(
                "INSERT OR REPLACE INTO contacts_verification_state(
                    destination_aci, identity_key, is_verified
                ) VALUES(?, ?, ?)",
                destination_aci,
                identity_key,
                is_verified,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    async fn contacts(&self) -> Result<Self::ContactsIter, Self::ContentsStoreError> {
        let sql_contacts = query_as!(
            SqlContact,
            r#"SELECT
                uuid AS "uuid: _",
                phone_number,
                name,
                profile_key,
                expire_timer,
                expire_timer_version,
                inbox_position,
                avatar,
                destination_aci AS "destination_aci: _",
                identity_key,
                is_verified
            FROM contacts c
            LEFT JOIN contacts_verification_state cv ON c.uuid = cv.destination_aci
            ORDER BY c.inbox_position"#
        )
        .fetch_all(&self.db)
        .await?;
        Ok(Box::new(sql_contacts.into_iter().map(TryInto::try_into)))
    }

    async fn contact_by_id(
        &self,
        id: &ServiceId,
    ) -> Result<Option<Contact>, Self::ContentsStoreError> {
        let id = id.raw_uuid();
        query_as!(
            SqlContact,
            r#"SELECT
                uuid AS "uuid: _",
                phone_number,
                name,
                profile_key,
                expire_timer,
                expire_timer_version,
                inbox_position,
                avatar,
                destination_aci AS "destination_aci: _",
                identity_key,
                is_verified
            FROM contacts c
            LEFT JOIN contacts_verification_state cv ON c.uuid = cv.destination_aci
            WHERE c.uuid = ?"#,
            id
        )
        .fetch_optional(&self.db)
        .await?
        .map(TryInto::try_into)
        .transpose()
    }

    async fn clear_groups(&mut self) -> Result<(), Self::ContentsStoreError> {
        let mut transaction = self.db.begin().await.into_protocol_error()?;
        query!("DELETE FROM groups")
            .execute(&mut *transaction)
            .await?;
        query!("DELETE FROM group_avatars")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await.into_protocol_error()?;
        Ok(())
    }

    async fn save_group(
        &self,
        master_key: GroupMasterKeyBytes,
        group: impl Into<Group>,
    ) -> Result<(), Self::ContentsStoreError> {
        let g = SqlGroup::from_group(&master_key, group.into());
        let master_key = g.master_key.as_ref();
        query!(
            "INSERT OR REPLACE INTO groups VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            master_key,
            g.title,
            g.revision,
            g.invite_link_password,
            g.access_control,
            g.avatar,
            g.description,
            g.members,
            g.pending_members,
            g.requesting_members,
            g.disappearing_messages_timer,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn groups(&self) -> Result<Self::GroupsIter, Self::ContentsStoreError> {
        let sql_groups = query_as!(
            SqlGroup,
            r#"SELECT
                master_key,
                title,
                revision AS "revision: _",
                invite_link_password,
                access_control AS "access_control: _",
                avatar,
                description,
                members AS "members: _",
                pending_members AS "pending_members: _",
                requesting_members AS "requesting_members: _",
                disappearing_messages_timer
            FROM groups"#,
        )
        .fetch_all(&self.db)
        .await?;
        Ok(Box::new(sql_groups.into_iter().map(SqlGroup::into_group)))
    }

    async fn group(
        &self,
        master_key: GroupMasterKeyBytes,
    ) -> Result<Option<Group>, Self::ContentsStoreError> {
        let master_key_bytes = master_key.as_slice();
        query_as!(
            SqlGroup,
            r#"SELECT
                master_key,
                title,
                revision AS "revision: _",
                invite_link_password,
                access_control AS "access_control: _",
                avatar,
                description,
                members AS "members: _",
                pending_members AS "pending_members: _",
                requesting_members AS "requesting_members: _",
                disappearing_messages_timer
            FROM groups
            WHERE master_key = ?
            LIMIT 1"#,
            master_key_bytes,
        )
        .fetch_optional(&self.db)
        .await?
        .map(|g| g.into_group().map(|(_master_key, group)| group))
        .transpose()
    }

    async fn save_group_avatar(
        &self,
        master_key: GroupMasterKeyBytes,
        avatar: &AvatarBytes,
    ) -> Result<(), Self::ContentsStoreError> {
        let master_key_bytes = master_key.as_slice();
        query!(
            "INSERT OR REPLACE INTO group_avatars(group_master_key, bytes) VALUES (?, ?)",
            master_key_bytes,
            avatar,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn group_avatar(
        &self,
        master_key: GroupMasterKeyBytes,
    ) -> Result<Option<AvatarBytes>, Self::ContentsStoreError> {
        let master_key_bytes = master_key.as_slice();
        query_scalar!(
            "SELECT bytes FROM group_avatars WHERE group_master_key = ?",
            master_key_bytes,
        )
        .fetch_optional(&self.db)
        .await
        .map_err(From::from)
    }

    async fn upsert_profile_key(
        &mut self,
        uuid: &Uuid,
        key: ProfileKey,
    ) -> Result<bool, Self::ContentsStoreError> {
        let profile_key_bytes = key.bytes.as_slice();
        let res = query_scalar!(
            "INSERT OR REPLACE INTO profile_keys (uuid, key) VALUES (?, ?)",
            uuid,
            profile_key_bytes
        )
        .execute(&self.db)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn profile_key(
        &self,
        service_id: &ServiceId,
    ) -> Result<Option<ProfileKey>, Self::ContentsStoreError> {
        let uuid = service_id.raw_uuid();
        let profile_key = query_scalar!("SELECT key FROM profile_keys WHERE uuid = ?", uuid)
            .fetch_optional(&self.db)
            .await?
            .and_then(|bytes| bytes.try_into().ok().map(ProfileKey::create));
        Ok(profile_key)
    }

    async fn save_profile(
        &mut self,
        uuid: Uuid,
        key: ProfileKey,
        profile: Profile,
    ) -> Result<(), Self::ContentsStoreError> {
        self.upsert_profile_key(&uuid, key).await?;
        let Profile {
            name,
            about,
            about_emoji,
            avatar,
            unrestricted_unidentified_access,
        } = profile;
        let (given_name, family_name) = name.map(|n| (n.given_name, n.family_name)).unzip();
        let family_name = family_name.flatten();
        query!(
            "INSERT OR REPLACE INTO profiles VALUES (?, ?, ?, ?, ?, ?, ?)",
            uuid,
            given_name,
            family_name,
            about,
            about_emoji,
            avatar,
            unrestricted_unidentified_access,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn profile(
        &self,
        uuid: Uuid,
        key: ProfileKey,
    ) -> Result<Option<Profile>, Self::ContentsStoreError> {
        let profile_key_bytes = key.bytes.as_slice();
        let profile = query_as!(
            SqlProfile,
            "SELECT
                p.given_name,
                p.family_name,
                p.about,
                p.about_emoji,
                p.avatar,
                p.unrestricted_unidentified_access
            FROM profile_keys pk
            INNER JOIN profiles p ON p.uuid = pk.uuid
            WHERE pk.uuid = ? AND pk.key = ?",
            uuid,
            profile_key_bytes,
        )
        .fetch_optional(&self.db)
        .await?;
        Ok(profile.map(|p| p.into()))
    }

    async fn save_profile_avatar(
        &mut self,
        uuid: Uuid,
        _key: ProfileKey,
        profile: &AvatarBytes,
    ) -> Result<(), Self::ContentsStoreError> {
        query!(
            "INSERT OR REPLACE INTO profile_avatars(uuid, bytes) VALUES (?, ?)",
            uuid,
            profile,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn profile_avatar(
        &self,
        uuid: Uuid,
        _key: ProfileKey,
    ) -> Result<Option<AvatarBytes>, Self::ContentsStoreError> {
        query_scalar!("SELECT bytes FROM profile_avatars WHERE uuid = ?", uuid)
            .fetch_optional(&self.db)
            .await
            .map_err(From::from)
    }

    async fn add_sticker_pack(
        &mut self,
        pack: &StickerPack,
    ) -> Result<(), Self::ContentsStoreError> {
        let manifest_json = Json(&pack.manifest);
        query!(
            "INSERT OR REPLACE INTO sticker_packs(id, key, manifest) VALUES(?, ?, ?)",
            pack.id,
            pack.key,
            manifest_json,
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn sticker_pack(
        &self,
        id: &[u8],
    ) -> Result<Option<StickerPack>, Self::ContentsStoreError> {
        let pack = query_as!(
            SqlStickerPack,
            r#"SELECT id, key, manifest AS "manifest: _" FROM sticker_packs WHERE id = ?"#,
            id
        )
        .fetch_optional(&self.db)
        .await?;
        Ok(pack.map(From::from))
    }

    async fn remove_sticker_pack(&mut self, id: &[u8]) -> Result<bool, Self::ContentsStoreError> {
        let res = query!("DELETE FROM sticker_packs WHERE id = ?", id)
            .execute(&self.db)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn sticker_packs(&self) -> Result<Self::StickerPacksIter, Self::ContentsStoreError> {
        let sql_packs = query_as!(
            SqlStickerPack,
            r#"SELECT id, key, manifest AS "manifest: _" FROM sticker_packs"#,
        )
        .fetch_all(&self.db)
        .await?;
        Ok(Box::new(sql_packs.into_iter().map(|pack| Ok(pack.into()))))
    }

    async fn thread_for_sender_and_timestamp(
        &self,
        sender: &ServiceId,
        timestamp: u64,
    ) -> Result<Option<Thread>, Self::ContentsStoreError> {
        let timestamp: i64 = timestamp
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidFormat)?;
        let sender_service_id = sender.service_id_string();
        let thread = query!(r#"SELECT group_master_key, recipient_id AS "recipient_id: Uuid" FROM threads JOIN thread_messages ON threads.id = thread_messages.thread_id WHERE sender_service_id = ? AND ts = ?"#, sender_service_id, timestamp)
            .fetch_optional(&self.db).await?;

        let Some(thread) = thread else {
            return Ok(None);
        };

        match (thread.group_master_key, thread.recipient_id) {
            (Some(group_master_key), None) => {
                Ok(group_master_key.try_into().ok().map(Thread::Group))
            }
            (None, Some(contact)) => Ok(Some(Thread::Contact(ServiceId::Aci(contact.into())))),
            _ => panic!("Thread table stored both group master key and recipient"),
        }
    }
}

/// KT (receive batching): single-message insert within a caller-managed
/// transaction, shared by [`save_message`] and the batched
/// [`ContentsStore::save_messages`] override.
async fn save_message_on_tx(
    conn: &mut sqlx::SqliteConnection,
    thread: &Thread,
    Content { metadata, body }: Content,
) -> Result<(), SqliteStoreError> {
    let thread_id = match thread {
        Thread::Contact(service_id) => {
            let uuid = service_id.raw_uuid();
            query_scalar!(
                "INSERT INTO threads(recipient_id, group_master_key) VALUES (?1, NULL)
                ON CONFLICT DO UPDATE SET recipient_id = ?1 RETURNING id",
                uuid,
            )
            .fetch_one(&mut *conn)
            .await?
        }
        Thread::Group(master_key_bytes) => {
            let master_key_bytes = master_key_bytes.as_slice();
            query_scalar!(
                "INSERT INTO threads(recipient_id, group_master_key) VALUES (NULL, ?1)
                ON CONFLICT DO UPDATE SET group_master_key = ?1 RETURNING id",
                master_key_bytes
            )
            .fetch_one(&mut *conn)
            .await?
        }
    };

    let Metadata {
        sender,
        destination,
        sender_device,
        client_timestamp: timestamp,
        needs_receipt,
        unidentified_sender,
        server_guid: _,
        was_plaintext,
        server_timestamp: _,
        pni_verified,
        story: _, // KT fork (stories): not persisted (stories are not saved).
        ephemeral: _,
        urgent: _,
        report_spam_token: _, // KT fork (P15a): envelope bits, not persisted.
    } = metadata;

    let sender_device: u8 = sender_device.into();
    let sender_service_id = sender.service_id_string();
    let destination_service_id = destination.service_id_string();

    let proto_bytes = prost::Message::encode_to_vec(&body.into_proto());
    let timestamp = timestamp.timestamp_millis();

    query!(
        "INSERT OR REPLACE INTO thread_messages (
            ts,
            thread_id,
            sender_service_id,
            sender_device_id,
            destination_service_id,
            needs_receipt,
            unidentified_sender,
            content_body,
            was_plaintext,
            pni_verified
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        timestamp,
        thread_id,
        sender_service_id,
        sender_device,
        destination_service_id,
        needs_receipt,
        unidentified_sender,
        proto_bytes,
        was_plaintext,
        pni_verified.as_ref().map(|p| p.service_id_string())
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}

trait ThreadExt {
    fn group_master_key(&self) -> Option<&[u8]>;
    fn recipient_id(&self) -> Option<Uuid>;

    fn unzip(&self) -> (Option<&[u8]>, Option<Uuid>) {
        (self.group_master_key(), self.recipient_id())
    }
}

impl ThreadExt for Thread {
    fn group_master_key(&self) -> Option<&[u8]> {
        match self {
            Thread::Contact(_) => None,
            Thread::Group(master_key) => Some(master_key.as_slice()),
        }
    }

    fn recipient_id(&self) -> Option<Uuid> {
        match self {
            Thread::Contact(service_id) => Some(service_id.raw_uuid()),
            Thread::Group(_) => None,
        }
    }
}

trait BoundExt {
    fn into_sql_bound(self) -> (Option<i64>, Option<i64>);
}

impl BoundExt for Bound<&u64> {
    fn into_sql_bound(self) -> (Option<i64>, Option<i64>) {
        match self {
            Bound::Excluded(x) => (Some(*x as i64), None),
            Bound::Included(x) => (None, Some(*x as i64)),
            Bound::Unbounded => (None, None),
        }
    }
}

impl SqliteStore {
    /// KT (DERM): outgoing-message lookup for retry requests, mirroring the
    /// official Desktop `getSentProtoByRecipient` shape (by sending account +
    /// timestamp, thread recovered alongside). Retry receipts name the
    /// original message by timestamp only, so the consumer pairs the hit with
    /// its own requester/membership checks before re-sending.
    pub async fn sent_message_by_timestamp(
        &self,
        sender: &ServiceId,
        timestamp: u64,
    ) -> Result<Option<(Thread, Content)>, SqliteStoreError> {
        let timestamp: i64 = timestamp
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidFormat)?;
        let row = sqlx::query_as::<
            _,
            (
                Option<Vec<u8>>,
                Option<String>,
                i64,
                Option<i64>,
                String,
                i64,
                String,
                bool,
                bool,
                Vec<u8>,
                bool,
                Option<String>,
            ),
        >(
            r#"SELECT t.group_master_key, t.recipient_id,
                      m.ts, m.server_ts, m.sender_service_id, m.sender_device_id,
                      m.destination_service_id, m.needs_receipt, m.unidentified_sender,
                      m.content_body, m.was_plaintext, m.pni_verified
            FROM thread_messages m
            JOIN threads t ON t.id = m.thread_id
            WHERE m.ts = ? AND m.sender_service_id = ?"#,
        )
        .bind(timestamp)
        .bind(sender.service_id_string())
        .fetch_optional(&self.db)
        .await
        .into_protocol_error()?;
        let Some((
            group_master_key,
            recipient_id,
            ts,
            server_ts,
            sender_service_id,
            sender_device_id,
            destination_service_id,
            needs_receipt,
            unidentified_sender,
            content_body,
            was_plaintext,
            pni_verified,
        )) = row
        else {
            return Ok(None);
        };
        let thread = match (group_master_key, recipient_id) {
            (Some(key), _) => Thread::Group(
                key.try_into()
                    .map_err(|_| SqliteStoreError::InvalidFormat)?,
            ),
            (None, Some(recipient_id)) => {
                let service_id = ServiceId::parse_from_service_id_string(&recipient_id)
                    .ok_or(SqliteStoreError::InvalidFormat)?;
                Thread::Contact(service_id)
            }
            (None, None) => return Err(SqliteStoreError::InvalidFormat),
        };
        let message = SqlMessage {
            ts: u64::try_from(ts).map_err(|_| SqliteStoreError::InvalidFormat)?,
            server_ts: server_ts
                .map(u64::try_from)
                .transpose()
                .map_err(|_| SqliteStoreError::InvalidFormat)?,
            sender_service_id,
            sender_device_id: u8::try_from(sender_device_id)
                .map_err(|_| SqliteStoreError::InvalidFormat)?,
            destination_service_id,
            needs_receipt,
            unidentified_sender,
            content_body,
            was_plaintext,
            pni_verified,
        };
        let content: Content = message.try_into()?;
        Ok(Some((thread, content)))
    }

    /// KT (pre-key maintenance): generic kv read for engine-side cadence
    /// bookmarks (the `kv` table predates the fork; account_entropy_pool is
    /// the in-tree consumer). Values are plain text blobs.
    pub async fn kt_kv_get(&self, key: &str) -> Result<Option<String>, SqliteStoreError> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT value FROM kv WHERE key = ?1")
            .bind(key)
            .fetch_optional(&self.db)
            .await
            .into_protocol_error()?;
        Ok(row.map(|(value,)| String::from_utf8_lossy(&value).to_string()))
    }

    /// KT (pre-key maintenance): generic kv write, same table as
    /// [`SqliteStore::kt_kv_get`].
    pub async fn kt_kv_set(&self, key: &str, value: &str) -> Result<(), SqliteStoreError> {
        sqlx::query("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value.as_bytes())
            .execute(&self.db)
            .await
            .into_protocol_error()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::OnNewIdentity;
    use presage::libsignal_service::content::ContentBody;
    use presage::libsignal_service::protocol::DeviceId;
    use presage::proto::NullMessage;

    #[tokio::test]
    async fn upsert_profile_key_reports_the_write() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = SqliteStore::open(":memory:", OnNewIdentity::Trust).await?;
        let uuid = Uuid::nil();
        let key = ProfileKey::create([1u8; 32]);

        assert!(store.upsert_profile_key(&uuid, key).await?);
        assert!(store.upsert_profile_key(&uuid, key).await?);

        Ok(())
    }

    // KT (receive batching): the batched path must land every message of a
    // window — three saves in one call read back as three rows in one thread.
    #[tokio::test]
    async fn save_messages_persists_a_whole_window() -> Result<(), Box<dyn std::error::Error>> {
        let store = SqliteStore::open(":memory:", OnNewIdentity::Trust).await?;
        let sender = ServiceId::Aci(Uuid::from_u128(1).into());
        let thread = Thread::Contact(sender);

        let content = |ts: u64| {
            Content::from_body(
                ContentBody::NullMessage(NullMessage::default()),
                Metadata {
                    sender,
                    destination: sender,
                    sender_device: DeviceId::new(1).unwrap(),
                    pni_verified: None,
                    client_timestamp: chrono::DateTime::from_timestamp_millis(ts as i64).unwrap(),
                    server_timestamp: chrono::DateTime::from_timestamp_millis(ts as i64).unwrap(),
                    needs_receipt: false,
                    unidentified_sender: false,
                    was_plaintext: false,
                    server_guid: None,
                    story: None,
                    ephemeral: None,
                    urgent: None,
                    report_spam_token: None,
                },
            )
        };

        store
            .save_messages(vec![
                (thread.clone(), content(1_700_000_000_000)),
                (thread.clone(), content(1_700_000_000_001)),
                (thread.clone(), content(1_700_000_000_002)),
            ])
            .await?;

        for ts in [1_700_000_000_000u64, 1_700_000_000_001, 1_700_000_000_002] {
            assert!(
                store.message(&thread, ts).await?.is_some(),
                "message {ts} missing"
            );
        }
        assert_eq!(store.messages(&thread, ..).await?.count(), 3);

        Ok(())
    }
}
