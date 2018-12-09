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

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT true FROM pg_attribute
            WHERE attrelid = 'log_consumer_positions'::regclass
            AND attname = 'tx_position'
            AND NOT attisdropped
        ) THEN
            ALTER TABLE log_consumer_positions ADD COLUMN tx_position BIGINT NOT NULL;
        END IF;
    END
$$;