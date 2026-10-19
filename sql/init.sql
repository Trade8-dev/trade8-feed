-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Feed
-- SPDX-License-Identifier: Apache-2.0

-- Trade8:Feed - quote sources, feed links, alert rules, fired alerts.
--
-- Design: ../feed/DESIGN.md §5,
-- data-model §2.1 (quote_source/feed_link) + §2.8.
-- Credentials are secret references - never values.

CREATE SCHEMA IF NOT EXISTS feed;

CREATE TABLE IF NOT EXISTS feed.quote_source (
 source_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 name TEXT NOT NULL,
 display_name TEXT,
 is_book BOOLEAN NOT NULL DEFAULT false,
 create_tos BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS feed.feed_link (
 link_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 kind TEXT NOT NULL CHECK (kind IN ('itch','idc','fix_md','crypto_ws','sim')),
 name TEXT NOT NULL,
 quote_source_id UUID NOT NULL REFERENCES feed.quote_source,
 config JSONB NOT NULL DEFAULT '{}',
 credentials_ref TEXT,
 generate_top_of_book BOOLEAN NOT NULL DEFAULT false,
 enable BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS feed.alert_rule (
 rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 usr_id TEXT NOT NULL,
 symbol_name TEXT NOT NULL,
 condition JSONB NOT NULL, -- {op, threshold, window_ms?}
 active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS feed.alert (
 alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 rule_id UUID NOT NULL REFERENCES feed.alert_rule,
 usr_id TEXT NOT NULL,
 symbol_name TEXT NOT NULL,
 quote JSONB NOT NULL, -- triggering snapshot
 condition_text TEXT NOT NULL,
 at TIMESTAMPTZ NOT NULL DEFAULT now(),
 ack_by TEXT,
 ack_at TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- RLS
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
DECLARE pol TEXT;
BEGIN
 FOREACH tbl IN ARRAY ARRAY[
 'feed.quote_source', 'feed.feed_link',
 'feed.alert_rule', 'feed.alert'
 ] LOOP
 BEGIN
 pol := replace(split_part(tbl, '.', 2), '"', '');
 EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
 EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
 -- Suffix BEFORE %I quoting: `limit` alone is fine but the concatenation
 -- `"limit"_select` is invalid SQL - build 'limit_select' unquoted
 -- (desk-store DO-block memory: reserved-word table names are the
 -- classic failure class).
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_select', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_insert', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_update', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_delete', tbl);
 EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
 END;
 END LOOP;
END $$;

-- Local outbox + audit journal (DBX §6.7 / §6.8 - shell-generated DDL).
CREATE TABLE IF NOT EXISTS feed.outbox (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 event_type TEXT NOT NULL,
 stream_family TEXT NOT NULL,
 payload JSONB NOT NULL,
 publisher_id_token TEXT,
 publisher_subject TEXT,
 relayed BOOLEAN NOT NULL DEFAULT false,
 relayed_at TIMESTAMPTZ,
 relay_attempts INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unrelayed
 ON feed.outbox (created_at) WHERE relayed = false;
ALTER TABLE feed.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE feed.outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_select ON feed.outbox;
CREATE POLICY outbox_select ON feed.outbox FOR SELECT
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_insert ON feed.outbox;
CREATE POLICY outbox_insert ON feed.outbox FOR INSERT
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_update ON feed.outbox;
CREATE POLICY outbox_update ON feed.outbox FOR UPDATE
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
CREATE OR REPLACE FUNCTION feed.feed_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
 PERFORM pg_notify('feed_outbox_changed', '');
 RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON feed.outbox;
CREATE TRIGGER outbox_notify_trigger
 AFTER INSERT ON feed.outbox
 FOR EACH ROW
 EXECUTE FUNCTION feed.feed_notify_outbox();

CREATE TABLE IF NOT EXISTS feed.audit_journal (
 id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 action TEXT NOT NULL,
 target_resource_type TEXT,
 target_resource_id TEXT,
 actor_user_id TEXT,
 actor_subject_type TEXT,
 severity TEXT NOT NULL DEFAULT 'info',
 correlation_id TEXT,
 before_state JSONB,
 after_state JSONB,
 details JSONB,
 pushed BOOLEAN NOT NULL DEFAULT false,
 pushed_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_feed_audit_journal_unpushed
 ON feed.audit_journal (created_at) WHERE pushed = false;
ALTER TABLE feed.audit_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE feed.audit_journal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_journal_dml ON feed.audit_journal;
CREATE POLICY audit_journal_dml ON feed.audit_journal
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());

-- ---------------------------------------------------------------------------
-- Per-service role grants + search_path (bank8-ledger pattern; the CLI
-- creates the role BEFORE this SQL - grant-before-role incident rule).
-- Grants live in db trade8 (this database); the scope→role map row is
-- upserted by `vertical enroll` from --section=pg_roles.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trade8_feed_service') THEN
  ALTER ROLE trade8_feed_service SET search_path = feed, public;
  GRANT USAGE ON SCHEMA feed TO trade8_feed_service;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA feed TO trade8_feed_service;
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA feed TO trade8_feed_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA feed
   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trade8_feed_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA feed
   GRANT USAGE, SELECT ON SEQUENCES TO trade8_feed_service;
 END IF;
END $$;
-- Declared cross-schema reads (SELECT-only) - mirrors registration.rs
-- PgRole.cross_schema_grants; `vertical enroll` owns their application,
-- this block only re-asserts them idempotently after a role recreation.

