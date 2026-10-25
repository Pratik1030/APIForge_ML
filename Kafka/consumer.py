import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db.queries import record_request_event, record_quarantine, resolve_subscription_id

task = None
status = {
    "enabled": False,
    "connected": False,
    "last_error": None,
    "events_processed": 0,
}


def kafka_status():
    return dict(status)


async def normalize_event(event: dict) -> dict:
    required = ["apiId", "endpoint", "statusCode"]
    missing = [key for key in required if event.get(key) is None]
    if missing:
        raise ValueError(f"Missing required event fields: {missing}")

    event_id = str(event.get("eventId") or uuid.uuid4())

    raw_ts = event.get("timestamp") or datetime.now(timezone.utc)
    if isinstance(raw_ts, str):
        if raw_ts.endswith("Z"):
            raw_ts = raw_ts[:-1] + "+00:00"
        ts = datetime.fromisoformat(raw_ts)
    elif isinstance(raw_ts, datetime):
        ts = raw_ts
    else:
        ts = datetime.now(timezone.utc)

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    api_id = str(event["apiId"])
    status_code = int(event.get("statusCode") or 200)
    response_time = float(event.get("responseTime") or event.get("response_time_ms") or 0.0)

    # Resolve subscription_id if not explicitly provided
    subscription_id = event.get("subscriptionId") or event.get("subscription_id")
    if not subscription_id:
        subscription_id = await resolve_subscription_id(
            api_id=api_id,
            api_key=event.get("apiKey"),
            consumer_id=str(event.get("consumerId")) if event.get("consumerId") else None,
        )

    if not subscription_id:
        # Fallback to deterministic dummy UUID if no subscription row exists yet
        subscription_id = str(uuid.UUID(int=0))

    return {
        "eventId": event_id,
        "timestamp": ts,
        "apiId": api_id,
        "subscriptionId": subscription_id,
        "providerId": str(event["providerId"]) if event.get("providerId") else None,
        "consumerId": str(event["consumerId"]) if event.get("consumerId") else None,
        "planId": str(event["planId"]) if event.get("planId") else None,
        "apiKey": event.get("apiKey"),
        "endpoint": str(event.get("endpoint") or "/"),
        "method": str(event.get("method") or "GET").upper(),
        "statusCode": status_code,
        "responseTime": response_time,
        "rateLimitHit": bool(event.get("rateLimitHit", status_code == 429)),
        "isError": bool(event.get("isError", status_code >= 400)),
        "errorType": event.get("errorType") or (f"HTTP_{status_code}" if status_code >= 400 else None),
    }


async def consume_events():
    from aiokafka import AIOKafkaConsumer

    topic = os.getenv("KAFKA_TOPIC", "apiforge.request-events")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "apiforge-python-analytics")

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda val: json.loads(val.decode("utf-8")),
    )

    try:
        await consumer.start()
        status.update(enabled=True, connected=True, last_error=None)
        print(f"Kafka consumer connected to topic '{topic}' on {bootstrap}.")

        async for message in consumer:
            try:
                event = await normalize_event(message.value)
                await record_request_event(event)
                await consumer.commit()
                status["events_processed"] += 1
            except Exception as exc:
                err_msg = f"{type(exc).__name__}: {exc}"
                status["last_error"] = err_msg
                print(f"Kafka event processing error: {err_msg}")
                # Quarantine failed raw payload
                try:
                    await record_quarantine(
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        raw_payload=str(message.value),
                        error_message=err_msg,
                    )
                    await consumer.commit()
                except Exception as q_err:
                    print(f"Failed to quarantine message: {q_err}")

    except asyncio.CancelledError:
        print("Kafka consumer task cancelled.")
    except Exception as exc:
        status["last_error"] = f"{type(exc).__name__}: {exc}"
        print(f"Kafka connection failure: {status['last_error']}")
    finally:
        status["connected"] = False
        await consumer.stop()


async def start_consumer():
    global task
    enabled = os.getenv("KAFKA_ENABLED", "false").lower() in ("true", "1", "yes")
    if not enabled:
        print("Kafka consumer disabled (KAFKA_ENABLED != true). Set KAFKA_ENABLED=true in .env to enable.")
        return

    task = asyncio.create_task(consume_events())


async def stop_consumer():
    global task
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        task = None