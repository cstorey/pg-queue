-- This file is split on instances of "-- Split" followed by a newline, and each chunk
-- is run as a single batch.

CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL PRIMARY KEY,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now() ,
    body bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS log_consumer_positions (
    name TEXT PRIMARY KEY,
    position BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON logs (written_at);

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'logs'::regclass
            AND attname = 'tx_id'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE logs ADD COLUMN tx_id BIGINT DEFAULT txid_current();
        END IF;
    END
$$;

-- Split

CREATE INDEX IF NOT EXISTS logs_offset_idx ON logs(tx_id, id);

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'log_consumer_positions'::regclass
            AND attname = 'tx_position'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE log_consumer_positions ADD COLUMN tx_position BIGINT NOT NULL DEFAULT 0;
        END IF;
    END
$$;

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'logs'::regclass
            AND attname = 'key'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE logs ADD COLUMN key BYTEA NOT NULL DEFAULT bytea 'default';
        END IF;
    END
$$;

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'logs'::regclass
            AND attname = 'epoch'
            AND NOT attisdropped
        ) THEN
            -- BEGIN;
            ALTER TABLE logs ADD COLUMN epoch BIGINT;
            UPDATE logs SET epoch = 1 where epoch IS NULL;
            ALTER TABLE logs ALTER COLUMN epoch SET NOT NULL;
            -- COMMIT;
        END IF;
    END
$$;

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'log_consumer_positions'::regclass
            AND attname = 'epoch'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE log_consumer_positions ADD COLUMN epoch BIGINT;
            UPDATE log_consumer_positions SET epoch = 1 where epoch IS NULL;
            ALTER TABLE log_consumer_positions ALTER COLUMN epoch SET NOT NULL;
        END IF;
    END
$$;

-- Split

CREATE INDEX IF NOT EXISTS logs_epoch_offset_idx ON logs(epoch, tx_id, id);
DROP INDEX IF EXISTS logs_offset_idx;

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'logs'::regclass
            AND attname = 'meta'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE logs ADD COLUMN meta bytea;
        END IF;
    END
$$;

-- Split

ALTER TABLE logs ALTER COLUMN key SET DEFAULT NULL;

-- Split

CREATE OR REPLACE FUNCTION logs_text_key_migration_trigger_bytea_to_text_f()
  RETURNS trigger
  LANGUAGE plpgsql AS
$func$
BEGIN
    -- support newer consumer for older consumers
    NEW.key_text := convert_from(NEW.key, 'utf-8');
    RETURN NEW;
END
$func$;

-- Split

CREATE OR REPLACE FUNCTION logs_text_key_migration_trigger_text_to_bytea_f()
  RETURNS trigger
  LANGUAGE plpgsql AS
$func$
BEGIN
    -- support newer consumers for older producers
    NEW.key := convert_to(NEW.key_text, 'utf-8');
    RETURN NEW;
END
$func$;

-- Split

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'logs'::regclass
            AND attname = 'key_text'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE logs ADD COLUMN key_text TEXT;
            UPDATE logs SET key_text = convert_from(key, 'utf-8') WHERE key_text IS NULL;
            ALTER TABLE logs ALTER COLUMN key_text SET NOT NULL;
        END IF;
    END
$$;

-- Split

BEGIN;
DROP TRIGGER IF EXISTS logs_text_key_migration_trigger_text_to_bytea ON logs;
CREATE TRIGGER logs_text_key_migration_trigger_text_to_bytea
BEFORE INSERT ON logs
FOR EACH ROW
WHEN (NEW.key_text IS NOT NULL AND NEW.key IS NULL)
EXECUTE PROCEDURE logs_text_key_migration_trigger_text_to_bytea_f();
COMMIT;

-- Split

BEGIN;
DROP TRIGGER IF EXISTS logs_text_key_migration_trigger_bytea_to_text ON logs;
CREATE TRIGGER logs_text_key_migration_trigger_bytea_to_text
BEFORE INSERT ON logs
FOR EACH ROW
WHEN (NEW.key_text IS NULL AND NEW.key IS NOT NULL)
EXECUTE PROCEDURE logs_text_key_migration_trigger_bytea_to_text_f();
COMMIT;
