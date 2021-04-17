-- This file is split on instances of "-- Split" followed by a newline, and each chunk
-- is run as a single batch.

CREATE TABLE IF NOT EXISTS pg_queue_jobs (
    id BIGSERIAL NOT NULL,
    body bytea NOT NULL,
    last_tried_at TIMESTAMPTZ,
    retried_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);