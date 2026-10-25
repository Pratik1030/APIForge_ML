from fastapi import APIRouter
from db.queries import (
    get_consumer_usage,
    get_consumer_rate_limit,
    get_consumer_endpoints,
    get_consumer_errors,
    get_consumer_subscription,
    json_safe,
)

router = APIRouter(prefix="/analytics/consumer", tags=["Consumer Analytics"])


@router.get("/{consumer_id}/apis/{api_id}/usage")
async def consumer_usage(consumer_id: str, api_id: str):
    """Requests made today, this week, and this month with daily breakdown."""
    data = await get_consumer_usage(consumer_id, api_id)
    return json_safe(data)


@router.get("/{consumer_id}/apis/{api_id}/rate-limit")
async def consumer_rate_limit(consumer_id: str, api_id: str):
    """Current usage vs limit and remaining requests this minute."""
    data = await get_consumer_rate_limit(consumer_id, api_id)
    return json_safe(data)


@router.get("/{consumer_id}/apis/{api_id}/endpoints")
async def consumer_endpoints(consumer_id: str, api_id: str):
    """Which endpoints the consumer calls most with count and latency."""
    data = await get_consumer_endpoints(consumer_id, api_id)
    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        "endpoints": json_safe(data),
    }


@router.get("/{consumer_id}/apis/{api_id}/errors")
async def consumer_errors(consumer_id: str, api_id: str):
    """Consumer error rate, 4xx vs 5xx breakdown, and common error status codes."""
    data = await get_consumer_errors(consumer_id, api_id)
    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        **json_safe(data),
    }


@router.get("/{consumer_id}/apis/{api_id}/subscription")
async def consumer_subscription(consumer_id: str, api_id: str):
    """Plan details, key status, total paid, and subscription metadata."""
    data = await get_consumer_subscription(consumer_id, api_id)
    return json_safe(data)
