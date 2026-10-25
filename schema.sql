CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.kafka_request_events (
    event_id UUID PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    api_id BIGINT NOT NULL,
    provider_id BIGINT,
    consumer_id BIGINT,
    plan_id BIGINT,
    api_key TEXT,
    endpoint TEXT NOT NULL,
    http_method TEXT NOT NULL,
    status_code INTEGER,
    response_time_ms DOUBLE PRECISION,
    is_error BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_kafka_events_api_time
    ON analytics.kafka_request_events (api_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_kafka_events_provider_api_time
    ON analytics.kafka_request_events
    (provider_id, api_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_kafka_events_consumer_api_time
    ON analytics.kafka_request_events
    (consumer_id, api_id, event_timestamp DESC);