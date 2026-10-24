-- KT (pre-key cleanup, 时序审计组四 E2): one-time prekeys get a creation
-- timestamp so the local store can run the official newest-N retention
-- (Signal-Desktop PRE_KEY_MAX_COUNT=200 / PRE_KEY_ARCHIVE_AGE=90d).
-- Pre-existing rows default to 0 = oldest (official `createdAt || 0` same
-- semantics); no backfill needed.
ALTER TABLE pre_keys ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0;
