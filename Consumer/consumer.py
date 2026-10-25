from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID
from aiokafka.structs import TopicPartition, OffsetAndMetadata

from aiokafka import AIOKafkaConsumer
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ConfigDict,
    field_validator,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("apiforge.kafka")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "api-request-events"
)
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID", "apiforge-ml-consumer"
)



DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
)



class ApiRequestEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventId")
    timestamp: datetime
    api_id: UUID = Field(alias="apiId")
    subscription_id: UUID = Field(alias="subscriptionId")
    endpoint: str
    http_method: str = Field(alias="httpMethod")
    status_code: int = Field(alias="statusCode")
    response_time_ms: float = Field(alias="responseTimeMs")
    rate_limit_hit: bool = Field(alias="rateLimitHit")
    error_type: str | None = Field(
        default=None, alias="errorType"
    )

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value.astimezone(timezone.utc)

    @field_validator("response_time_ms")
    @classmethod
    def latency_must_be_nonnegative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("responseTimeMs cannot be negative")
        return value

    



async def process_event(event: ApiRequestEvent) -> bool:
    bucket = event.timestamp.replace(second=0, microsecond=0)

    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                INSERT INTO analytics.request_events (
                    event_id, event_timestamp, api_id, subscription_id,
                    endpoint, http_method, status_code, response_time_ms,
                    rate_limit_hit, error_type
                )
                VALUES (
                    :event_id, :timestamp, :api_id, :subscription_id,
                    :endpoint, :http_method, :status_code, :latency,
                    :rate_limit_hit, :error_type
                )
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            """),
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "api_id": event.api_id,
                "subscription_id": event.subscription_id,
                "endpoint": event.endpoint,
                "http_method": event.http_method,
                "status_code": event.status_code,
                "latency": event.response_time_ms,
                "rate_limit_hit": event.rate_limit_hit,
                "error_type": event.error_type,
            },
        )

        # If this event was already stored, don't count it again.
        if result.scalar_one_or_none() is None:
            logger.info("Duplicate event ignored: %s", event.event_id)
            return False

        await conn.execute(
            text("""
                INSERT INTO analytics.usage_minute (
                    bucket_start, api_id, subscription_id,
                    request_count, error_count, rate_limited_count,
                    latency_sum_ms, latency_max_ms
                )
                VALUES (
                    :bucket, :api_id, :subscription_id,
                    1, :error_count, :rate_limited_count,
                    :latency, :latency
                )
                ON CONFLICT (bucket_start, api_id, subscription_id)
                DO UPDATE SET
                    request_count =
                        analytics.usage_minute.request_count + 1,
                    error_count =
                        analytics.usage_minute.error_count
                        + EXCLUDED.error_count,
                    rate_limited_count =
                        analytics.usage_minute.rate_limited_count
                        + EXCLUDED.rate_limited_count,
                    latency_sum_ms =
                        analytics.usage_minute.latency_sum_ms
                        + EXCLUDED.latency_sum_ms,
                    latency_max_ms = GREATEST(
                        analytics.usage_minute.latency_max_ms,
                        EXCLUDED.latency_max_ms
                    )
            """),
            {
                "bucket": bucket,
                "api_id": event.api_id,
                "subscription_id": event.subscription_id,
                "error_count": int(event.status_code >= 400),
                "rate_limited_count": int(event.rate_limit_hit),
                "latency": event.response_time_ms,
            },
        )

    logger.info("Event saved: %s", event.event_id)
    return True

async def quarantine_message(
    topic: str,
    partition: int,
    offset: int,
    raw_payload: str,
    error_message: str,
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO analytics.kafka_quarantine (
                    topic,
                    partition_id,
                    message_offset,
                    raw_payload,
                    error_message
                )
                VALUES (
                    :topic,
                    :partition_id,
                    :message_offset,
                    :raw_payload,
                    :error_message
                )
                ON CONFLICT (
                    topic, partition_id, message_offset
                ) DO NOTHING
            """),
            {
                "topic": topic,
                "partition_id": partition,
                "message_offset": offset,
                "raw_payload": raw_payload,
                "error_message": error_message,
            },
        )

async def consume_events() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: value,
    )

    await consumer.start()
    logger.info(
        "Listening on topic=%s brokers=%s group=%s",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_GROUP_ID,
    )

    try:
        async for message in consumer:
            
            try:
                payload = json.loads(message.value.decode("utf-8"))
                event = ApiRequestEvent.model_validate(payload)

                # Process once; commit only after successful DB processing.
                await process_event(event)

                await consumer.commit({
                    TopicPartition(message.topic, message.partition):
                        OffsetAndMetadata(message.offset + 1, "")
                })

            except (ValidationError, ValueError, TypeError) as exc:
                raw_payload = (
                    message.value.decode("utf-8", errors="replace")
                    if isinstance(message.value, bytes)
                    else str(message.value)
                )

                try:
                    await quarantine_message(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        raw_payload=raw_payload,
                        error_message=str(exc),
                    )

                    # Advance only after quarantine is saved.
                    await consumer.commit({
                        TopicPartition(message.topic, message.partition):
                            OffsetAndMetadata(message.offset + 1, "")
                    })

                    logger.warning(
                        "Invalid event quarantined: partition=%s offset=%s",
                        message.partition,
                        message.offset,
                    )

                except Exception:
                    logger.exception(
                        "Quarantine or offset commit failed: "
                        "partition=%s offset=%s",
                        message.partition,
                        message.offset,
                    )
                    raise

                # In production, send invalid events to a dead-letter
                # topic or durable quarantine before advancing offsets.
                # Do not silently commit and discard them.

    finally:
        await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(consume_events())
    except KeyboardInterrupt:
        logger.info("Kafka consumer stopped.")
