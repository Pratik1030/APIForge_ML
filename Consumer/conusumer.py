
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, Field, ValidationError

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


class ApiRequestEvent(BaseModel):
    event_id: str = Field(alias="eventId")
    timestamp: datetime
    api_id: int = Field(alias="apiId")
    subscription_id: int = Field(alias="subscriptionId")
    endpoint: str
    http_method: str = Field(alias="httpMethod")
    status_code: int = Field(alias="statusCode")
    response_time_ms: float = Field(alias="responseTimeMs")
    rate_limit_hit: bool = Field(alias="rateLimitHit")
    error_type: str | None = Field(
        default=None, alias="errorType"
    )

    class Config:
        populate_by_name = True


def process_event(event: ApiRequestEvent) -> None:
    """
    Replace this function with your analytics persistence
    and ML inference logic.
    """
    logger.info(
        "Request event received: id=%s api=%s subscription=%s "
        "method=%s status=%s latency_ms=%.2f rate_limited=%s",
        event.event_id,
        event.api_id,
        event.subscription_id,
        event.http_method,
        event.status_code,
        event.response_time_ms,
        event.rate_limit_hit,
    )

    # Example security signal; this is a rule, not an ML prediction.
    if event.rate_limit_hit:
        logger.warning(
            "Rate-limit event: api_id=%s subscription_id=%s",
            event.api_id,
            event.subscription_id,
        )

    # TODO:
    # 1. Store the event in your analytics database.
    # 2. Map available fields into your ML feature pipeline.
    # 3. Persist the model's anomaly score/prediction.


async def consume_events() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
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
                event = ApiRequestEvent.model_validate(
                    message.value
                )
                process_event(event)

                # Commit only after processing succeeds.
                await consumer.commit()

            except (ValidationError, ValueError, TypeError) as exc:
                logger.exception(
                    "Invalid event at partition=%s offset=%s: %s",
                    message.partition,
                    message.offset,
                    exc,
                )
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
