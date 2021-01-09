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

CREATE INDEX IF NOT EXISTS logs_offset_idx ON logs(tx_id, id);

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