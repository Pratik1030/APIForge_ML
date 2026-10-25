from fastapi import APIRouter, Query
from db.queries import (
    get_provider_summary,
    get_provider_requests,
    get_provider_endpoints,
    get_provider_peak_hours,
    get_provider_revenue,
    get_provider_plans_performance,
    get_provider_errors,
    get_provider_response_time,
    get_provider_consumers,
    json_safe,
)

router = APIRouter(prefix="/analytics/provider", tags=["Provider Analytics"])


@router.get("/{provider_id}/apis/{api_id}/summary")
async def provider_summary(provider_id: str, api_id: str):
    """Total requests, total subscribers, active plans, error rate, and average latency."""
    data = await get_provider_summary(provider_id, api_id)
    return json_safe(data)


@router.get("/{provider_id}/apis/{api_id}/requests")
async def provider_requests(
    provider_id: str,
    api_id: str,
    period: str = Query("day", pattern="^(hour|day|week)$", description="Aggregation bucket period"),
    days: int = Query(30, ge=1, le=365, description="Number of days of history"),
):
    """Requests per hour, per day, or per week graph data."""
    data = await get_provider_requests(provider_id, api_id, period=period, days=days)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        "period": period,
        "days": days,
        "data": json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/endpoints")
async def provider_endpoints(provider_id: str, api_id: str):
    """Which endpoints are called most and least with latency & error metrics."""
    data = await get_provider_endpoints(provider_id, api_id)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        **json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/peak-hours")
async def provider_peak_hours(provider_id: str, api_id: str):
    """Traffic by hour of day (0-23) — when is the API busiest."""
    data = await get_provider_peak_hours(provider_id, api_id)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        **json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/revenue")
async def provider_revenue(provider_id: str, api_id: str):
    """Total earned, commission deducted, net amount per month from real transactions."""
    data = await get_provider_revenue(provider_id, api_id)
    return json_safe(data)


@router.get("/{provider_id}/apis/{api_id}/plans/performance")
async def provider_plans_performance(provider_id: str, api_id: str):
    """Which plan has most subscribers, most revenue, and rate limit performance."""
    data = await get_provider_plans_performance(provider_id, api_id)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        "plans": json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/errors")
async def provider_errors(provider_id: str, api_id: str):
    """Error rate, 4xx vs 5xx breakdown, and most failing endpoints."""
    data = await get_provider_errors(provider_id, api_id)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        **json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/response-time")
async def provider_response_time(
    provider_id: str,
    api_id: str,
    days: int = Query(30, ge=1, le=365),
):
    """Average, min, max, and p95 response time trends."""
    data = await get_provider_response_time(provider_id, api_id, days=days)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        "days": days,
        "trends": json_safe(data),
    }


@router.get("/{provider_id}/apis/{api_id}/consumers")
async def provider_consumers(provider_id: str, api_id: str):
    """Top consumers by request count, error count, and new vs returning status."""
    data = await get_provider_consumers(provider_id, api_id)
    return {
        "provider_id": provider_id,
        "api_id": api_id,
        **json_safe(data),
    }
