import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any
import uuid

import asyncpg
from fastapi import HTTPException

pool: asyncpg.Pool | None = None


def json_safe(value: Any) -> Any:
    """Recursively serialize dates, timestamps, decimals, UUIDs and numpy types."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if hasattr(value, "item"):  # numpy scalars
        return json_safe(value.item())
    if isinstance(value, float) and (value != value or value == float("inf") or value == float("-inf")):
        return None
    return value


async def init_pool():
    global pool
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is missing in environment.")
        return

    # asyncpg expects postgresql://
    db_url = database_url.replace("postgresql+asyncpg://", "postgresql://", 1)

    try:
        pool = await asyncpg.create_pool(
            db_url,
            min_size=1,
            max_size=10,
            command_timeout=25,
        )
        print("PostgreSQL connection pool established.")
    except Exception as exc:
        pool = None
        print(f"Database connection pool failed: {type(exc).__name__}: {exc}")


async def close_pool():
    global pool
    if pool is not None:
        await pool.close()
        pool = None
        print("PostgreSQL connection pool closed.")


async def fetch_all(query: str, *args: Any) -> list[dict[str, Any]]:
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Configure DATABASE_URL in .env.",
        )
    try:
        async with pool.acquire() as connection:
            records = await connection.fetch(query, *args)
            return [dict(record) for record in records]
    except HTTPException:
        raise
    except Exception as exc:
        print(f"Database fetch_all error ({type(exc).__name__}): {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Database query error: {type(exc).__name__}",
        ) from exc


async def fetch_one(query: str, *args: Any) -> dict[str, Any]:
    records = await fetch_all(query, *args)
    return records[0] if records else {}


async def execute(query: str, *args: Any) -> str:
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable.")
    try:
        async with pool.acquire() as connection:
            return await connection.execute(query, *args)
    except Exception as exc:
        print(f"Database execute error ({type(exc).__name__}): {exc}")
        raise HTTPException(status_code=500, detail=f"Database execute error: {type(exc).__name__}") from exc


# ---------------------------------------------------------------------------
# KAFKA INGESTION QUERIES
# ---------------------------------------------------------------------------

async def resolve_subscription_id(api_id: str, api_key: str | None, consumer_id: str | None) -> str | None:
    """Find subscription_id from public.subscriptions via api_key or (api_id, user_id)."""
    if not pool:
        return None
    try:
        if api_key:
            row = await fetch_one(
                "SELECT sub_id FROM public.subscriptions WHERE api_key = $1 LIMIT 1",
                api_key,
            )
            if row.get("sub_id"):
                return str(row["sub_id"])

        if api_id and consumer_id:
            row = await fetch_one(
                """
                SELECT sub_id FROM public.subscriptions
                WHERE api_id = $1::uuid AND user_id = $2::uuid
                ORDER BY status = 'ACTIVE' DESC
                LIMIT 1
                """,
                api_id,
                consumer_id,
            )
            if row.get("sub_id"):
                return str(row["sub_id"])
    except Exception:
        pass
    return None


async def record_request_event(event: dict) -> None:
    """
    Persist incoming Kafka request event into analytics.request_events
    and update analytics.usage_minute rollup.
    """
    event_id = str(event.get("eventId") or uuid.uuid4())
    event_timestamp = event.get("timestamp") or datetime.utcnow()
    api_id = str(event["apiId"])
    subscription_id = str(event.get("subscriptionId") or event.get("subscription_id") or uuid.UUID(int=0))
    endpoint = str(event.get("endpoint") or "/")
    method = str(event.get("method") or "GET").upper()
    status_code = int(event.get("statusCode") or 200)
    response_time_ms = float(event.get("responseTime") or 0.0)
    rate_limit_hit = bool(event.get("rateLimitHit", status_code == 429))
    error_type = event.get("errorType")
    if not error_type and status_code >= 400:
        error_type = f"HTTP_{status_code}"

    # 1. Insert into analytics.request_events
    await execute(
        """
        INSERT INTO analytics.request_events (
            event_id,
            event_timestamp,
            api_id,
            subscription_id,
            endpoint,
            http_method,
            status_code,
            response_time_ms,
            rate_limit_hit,
            error_type,
            received_at
        )
        VALUES (
            $1, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, NOW()
        )
        ON CONFLICT (event_id) DO NOTHING
        """,
        event_id,
        event_timestamp,
        api_id,
        subscription_id,
        endpoint,
        method,
        status_code,
        response_time_ms,
        rate_limit_hit,
        error_type,
    )

    # 2. Upsert into analytics.usage_minute
    is_err = status_code >= 400
    is_rl = rate_limit_hit or status_code == 429
    await execute(
        """
        INSERT INTO analytics.usage_minute (
            bucket_start,
            api_id,
            subscription_id,
            request_count,
            error_count,
            rate_limited_count,
            latency_sum_ms,
            latency_max_ms
        )
        VALUES (
            date_trunc('minute', $1::timestamptz),
            $2::uuid,
            $3::uuid,
            1,
            $4::bigint,
            $5::bigint,
            $6::double precision,
            $6::double precision
        )
        ON CONFLICT (bucket_start, api_id, subscription_id) DO UPDATE SET
            request_count = analytics.usage_minute.request_count + 1,
            error_count = analytics.usage_minute.error_count + EXCLUDED.error_count,
            rate_limited_count = analytics.usage_minute.rate_limited_count + EXCLUDED.rate_limited_count,
            latency_sum_ms = analytics.usage_minute.latency_sum_ms + EXCLUDED.latency_sum_ms,
            latency_max_ms = GREATEST(analytics.usage_minute.latency_max_ms, EXCLUDED.latency_max_ms)
        """,
        event_timestamp,
        api_id,
        subscription_id,
        1 if is_err else 0,
        1 if is_rl else 0,
        response_time_ms,
    )


async def record_quarantine(topic: str, partition: int, offset: int, raw_payload: str, error_message: str):
    await execute(
        """
        INSERT INTO analytics.kafka_quarantine (
            topic, partition_id, message_offset, raw_payload, error_message, quarantined_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (topic, partition_id, message_offset) DO NOTHING
        """,
        topic,
        partition,
        offset,
        raw_payload,
        error_message,
    )


# ---------------------------------------------------------------------------
# PROVIDER ANALYTICS QUERIES
# ---------------------------------------------------------------------------

async def get_provider_summary(provider_id: str, api_id: str) -> dict[str, Any]:
    # Aggregated metrics for provider's API
    event_stats = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total_requests,
            COUNT(*) FILTER (WHERE re.status_code >= 400 OR re.error_type IS NOT NULL)::bigint AS total_errors,
            COUNT(*) FILTER (WHERE re.rate_limit_hit)::bigint AS rate_limit_hits,
            COALESCE(AVG(re.response_time_ms), 0) AS avg_response_time_ms
        FROM analytics.request_events re
        WHERE re.api_id = $1::uuid
        """,
        api_id,
    )

    sub_stats = await fetch_one(
        """
        SELECT
            COUNT(DISTINCT user_id)::bigint AS total_subscribers,
            COUNT(DISTINCT sub_id)::bigint AS total_subscriptions
        FROM public.subscriptions
        WHERE api_id = $1::uuid AND status = 'ACTIVE'
        """,
        api_id,
    )

    plan_stats = await fetch_one(
        """
        SELECT COUNT(*)::bigint AS active_plans
        FROM public.api_plans
        WHERE api_id = $1::uuid AND status = 'ACTIVE'
        """,
        api_id,
    )

    total_requests = event_stats.get("total_requests") or 0
    total_errors = event_stats.get("total_errors") or 0
    error_rate = round((total_errors / total_requests * 100), 2) if total_requests > 0 else 0.0

    return {
        "provider_id": provider_id,
        "api_id": api_id,
        "total_requests": total_requests,
        "total_subscribers": sub_stats.get("total_subscribers") or 0,
        "active_plans": plan_stats.get("active_plans") or 0,
        "total_errors": total_errors,
        "error_rate_percent": error_rate,
        "rate_limit_hits": event_stats.get("rate_limit_hits") or 0,
        "avg_response_time_ms": round(float(event_stats.get("avg_response_time_ms") or 0), 2),
    }


async def get_provider_requests(provider_id: str, api_id: str, period: str = "day", days: int = 30) -> list[dict[str, Any]]:
    trunc = period if period in ("hour", "day", "week") else "day"
    rows = await fetch_all(
        f"""
        SELECT
            date_trunc('{trunc}', re.event_timestamp) AS bucket,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE re.status_code >= 400 OR re.error_type IS NOT NULL)::bigint AS error_count,
            COUNT(*) FILTER (WHERE re.rate_limit_hit)::bigint AS rate_limit_count,
            COALESCE(AVG(re.response_time_ms), 0) AS avg_latency_ms
        FROM analytics.request_events re
        WHERE re.api_id = $1::uuid
          AND re.event_timestamp >= NOW() - ($2 * INTERVAL '1 day')
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        api_id,
        days,
    )
    return rows


async def get_provider_endpoints(provider_id: str, api_id: str) -> dict[str, Any]:
    rows = await fetch_all(
        """
        SELECT
            endpoint,
            http_method AS method,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE status_code >= 400 OR error_type IS NOT NULL)::bigint AS error_count,
            COALESCE(AVG(response_time_ms), 0) AS avg_response_time_ms
        FROM analytics.request_events
        WHERE api_id = $1::uuid
        GROUP BY endpoint, http_method
        ORDER BY request_count DESC
        """,
        api_id,
    )
    return {
        "endpoints": rows,
        "most_called": rows[:5] if rows else [],
        "least_called": list(reversed(rows[-5:])) if rows else [],
    }


async def get_provider_peak_hours(provider_id: str, api_id: str) -> dict[str, Any]:
    rows = await fetch_all(
        """
        SELECT
            EXTRACT(HOUR FROM event_timestamp)::int AS hour,
            COUNT(*)::bigint AS request_count,
            COALESCE(AVG(response_time_ms), 0) AS avg_response_time_ms
        FROM analytics.request_events
        WHERE api_id = $1::uuid
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        api_id,
    )
    # Ensure all 24 hours are represented
    hour_map = {r["hour"]: r for r in rows}
    full_data = [
        hour_map.get(h, {"hour": h, "request_count": 0, "avg_response_time_ms": 0.0})
        for h in range(24)
    ]
    peak = max(full_data, key=lambda x: x["request_count"]) if full_data else None
    return {
        "traffic_by_hour": full_data,
        "peak_hour": peak["hour"] if peak and peak["request_count"] > 0 else None,
        "peak_request_count": peak["request_count"] if peak else 0,
    }


async def get_provider_revenue(provider_id: str, api_id: str) -> dict[str, Any]:
    totals = await fetch_one(
        """
        SELECT
            COALESCE(SUM(t.amount_paid), 0) AS total_earned,
            COALESCE(SUM(t.commission), 0) AS commission_deducted,
            COALESCE(SUM(t.provider_amount), 0) AS net_amount,
            COUNT(t.transaction_id)::bigint AS total_transactions
        FROM public.transactions t
        JOIN public.subscriptions s ON s.sub_id = t.subscription_id
        WHERE s.api_id = $1::uuid
        """,
        api_id,
    )

    monthly = await fetch_all(
        """
        SELECT
            to_char(t.timestamp, 'YYYY-MM') AS month,
            COALESCE(SUM(t.amount_paid), 0) AS earned,
            COALESCE(SUM(t.commission), 0) AS commission,
            COALESCE(SUM(t.provider_amount), 0) AS net,
            COUNT(t.transaction_id)::bigint AS transactions
        FROM public.transactions t
        JOIN public.subscriptions s ON s.sub_id = t.subscription_id
        WHERE s.api_id = $1::uuid
        GROUP BY 1
        ORDER BY 1 DESC
        """,
        api_id,
    )

    return {
        "provider_id": provider_id,
        "api_id": api_id,
        "total_earned": totals.get("total_earned", 0),
        "commission_deducted": totals.get("commission_deducted", 0),
        "net_amount": totals.get("net_amount", 0),
        "total_transactions": totals.get("total_transactions", 0),
        "monthly_breakdown": monthly,
    }


async def get_provider_plans_performance(provider_id: str, api_id: str) -> list[dict[str, Any]]:
    rows = await fetch_all(
        """
        SELECT
            p.plan_id,
            p.plan_name,
            p.price,
            p.rate_limit_per_minute,
            p.status,
            COUNT(DISTINCT s.sub_id)::bigint AS subscriber_count,
            COALESCE(SUM(t.amount_paid), 0) AS total_revenue,
            COALESCE(SUM(t.provider_amount), 0) AS provider_revenue
        FROM public.api_plans p
        LEFT JOIN public.subscriptions s ON s.plan_id = p.plan_id
        LEFT JOIN public.transactions t ON t.subscription_id = s.sub_id
        WHERE p.api_id = $1::uuid
        GROUP BY p.plan_id, p.plan_name, p.price, p.rate_limit_per_minute, p.status
        ORDER BY total_revenue DESC, subscriber_count DESC
        """,
        api_id,
    )
    return rows


async def get_provider_errors(provider_id: str, api_id: str) -> dict[str, Any]:
    summary = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total_requests,
            COUNT(*) FILTER (WHERE status_code BETWEEN 400 AND 499)::bigint AS errors_4xx,
            COUNT(*) FILTER (WHERE status_code BETWEEN 500 AND 599)::bigint AS errors_5xx,
            COUNT(*) FILTER (WHERE rate_limit_hit)::bigint AS rate_limit_errors,
            COUNT(*) FILTER (WHERE status_code >= 400 OR error_type IS NOT NULL)::bigint AS total_errors
        FROM analytics.request_events
        WHERE api_id = $1::uuid
        """,
        api_id,
    )
    total = summary.get("total_requests") or 0
    total_errors = summary.get("total_errors") or 0
    error_rate = round((total_errors * 100.0 / total), 2) if total else 0.0

    failing = await fetch_all(
        """
        SELECT
            endpoint,
            http_method,
            status_code,
            error_type,
            COUNT(*)::bigint AS failure_count
        FROM analytics.request_events
        WHERE api_id = $1::uuid AND (status_code >= 400 OR error_type IS NOT NULL)
        GROUP BY endpoint, http_method, status_code, error_type
        ORDER BY failure_count DESC
        LIMIT 10
        """,
        api_id,
    )

    return {
        "summary": {
            **summary,
            "error_rate_percent": error_rate,
        },
        "most_failing_endpoints": failing,
    }


async def get_provider_response_time(provider_id: str, api_id: str, days: int = 30) -> list[dict[str, Any]]:
    rows = await fetch_all(
        """
        SELECT
            date_trunc('hour', event_timestamp) AS bucket,
            COALESCE(AVG(response_time_ms), 0) AS average_ms,
            COALESCE(MIN(response_time_ms), 0) AS min_ms,
            COALESCE(MAX(response_time_ms), 0) AS max_ms,
            COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY response_time_ms), 0) AS p95_ms
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - ($2 * INTERVAL '1 day')
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        api_id,
        days,
    )
    return rows


async def get_provider_consumers(provider_id: str, api_id: str) -> dict[str, Any]:
    rows = await fetch_all(
        """
        SELECT
            u.user_id AS consumer_id,
            u.user_name AS consumer_name,
            u.user_email AS consumer_email,
            COUNT(re.event_id)::bigint AS request_count,
            COUNT(re.event_id) FILTER (WHERE re.status_code >= 400)::bigint AS error_count,
            MIN(re.event_timestamp) AS first_seen,
            MAX(re.event_timestamp) AS last_seen,
            CASE
                WHEN MIN(re.event_timestamp) >= NOW() - INTERVAL '7 days' THEN true
                ELSE false
            END AS is_new
        FROM public.subscriptions s
        JOIN public.base_user u ON u.user_id = s.user_id
        LEFT JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.api_id = $1::uuid
        GROUP BY u.user_id, u.user_name, u.user_email
        ORDER BY request_count DESC
        LIMIT 50
        """,
        api_id,
    )
    new_count = sum(1 for c in rows if c.get("is_new"))
    returning_count = len(rows) - new_count
    return {
        "total_consumers": len(rows),
        "new_consumers": new_count,
        "returning_consumers": returning_count,
        "consumers": rows,
    }


# ---------------------------------------------------------------------------
# CONSUMER ANALYTICS QUERIES
# ---------------------------------------------------------------------------

async def get_consumer_usage(consumer_id: str, api_id: str) -> dict[str, Any]:
    counts = await fetch_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE re.event_timestamp >= date_trunc('day', NOW()))::bigint AS requests_today,
            COUNT(*) FILTER (WHERE re.event_timestamp >= NOW() - INTERVAL '7 days')::bigint AS requests_this_week,
            COUNT(*) FILTER (WHERE re.event_timestamp >= date_trunc('month', NOW()))::bigint AS requests_this_month,
            COUNT(*)::bigint AS requests_all_time
        FROM public.subscriptions s
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
        """,
        consumer_id,
        api_id,
    )

    daily = await fetch_all(
        """
        SELECT
            date_trunc('day', re.event_timestamp) AS day,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE re.status_code >= 400)::bigint AS error_count
        FROM public.subscriptions s
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
          AND re.event_timestamp >= NOW() - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        consumer_id,
        api_id,
    )

    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        **counts,
        "daily_breakdown": daily,
    }


async def get_consumer_rate_limit(consumer_id: str, api_id: str) -> dict[str, Any]:
    # 1. Fetch configured limit from plan or API details
    sub_info = await fetch_one(
        """
        SELECT
            s.sub_id,
            COALESCE(p.rate_limit_per_minute, a.rate_limit_per_minute, 60) AS limit_per_minute
        FROM public.subscriptions s
        JOIN public.api_details a ON a.api_id = s.api_id
        LEFT JOIN public.api_plans p ON p.plan_id = s.plan_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
        LIMIT 1
        """,
        consumer_id,
        api_id,
    )

    limit = sub_info.get("limit_per_minute", 60) if sub_info else 60
    sub_id = sub_info.get("sub_id")

    # 2. Count requests in the current minute
    count_row = {"current_minute_requests": 0}
    if sub_id:
        count_row = await fetch_one(
            """
            SELECT COUNT(*)::bigint AS current_minute_requests
            FROM analytics.request_events
            WHERE subscription_id = $1::uuid
              AND event_timestamp >= date_trunc('minute', NOW())
            """,
            sub_id,
        )

    current = count_row.get("current_minute_requests") or 0
    remaining = max(0, limit - current)
    percentage = round((current / limit * 100), 1) if limit > 0 else 100.0

    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        "limit_per_minute": limit,
        "current_usage_this_minute": current,
        "remaining_requests_this_minute": remaining,
        "percentage_used": percentage,
        "is_rate_limited": current >= limit,
    }


async def get_consumer_endpoints(consumer_id: str, api_id: str) -> list[dict[str, Any]]:
    rows = await fetch_all(
        """
        SELECT
            re.endpoint,
            re.http_method AS method,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE re.status_code >= 400)::bigint AS error_count,
            COALESCE(AVG(re.response_time_ms), 0) AS avg_response_time_ms
        FROM public.subscriptions s
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
        GROUP BY re.endpoint, re.http_method
        ORDER BY request_count DESC
        """,
        consumer_id,
        api_id,
    )
    return rows


async def get_consumer_errors(consumer_id: str, api_id: str) -> dict[str, Any]:
    summary = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total_requests,
            COUNT(*) FILTER (WHERE re.status_code BETWEEN 400 AND 499)::bigint AS errors_4xx,
            COUNT(*) FILTER (WHERE re.status_code BETWEEN 500 AND 599)::bigint AS errors_5xx,
            COUNT(*) FILTER (WHERE re.rate_limit_hit)::bigint AS rate_limit_hits,
            COUNT(*) FILTER (WHERE re.status_code >= 400 OR re.error_type IS NOT NULL)::bigint AS total_errors
        FROM public.subscriptions s
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
        """,
        consumer_id,
        api_id,
    )
    total = summary.get("total_requests") or 0
    total_errors = summary.get("total_errors") or 0
    rate = round((total_errors * 100.0 / total), 2) if total else 0.0

    common = await fetch_all(
        """
        SELECT
            re.status_code,
            re.endpoint,
            re.error_type,
            COUNT(*)::bigint AS occurrences
        FROM public.subscriptions s
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
          AND (re.status_code >= 400 OR re.error_type IS NOT NULL)
        GROUP BY re.status_code, re.endpoint, re.error_type
        ORDER BY occurrences DESC
        LIMIT 10
        """,
        consumer_id,
        api_id,
    )

    return {
        "summary": {
            **summary,
            "error_rate_percent": rate,
        },
        "common_errors": common,
    }


async def get_consumer_subscription(consumer_id: str, api_id: str) -> dict[str, Any]:
    sub = await fetch_one(
        """
        SELECT
            s.sub_id,
            s.status AS key_status,
            s.api_key,
            p.plan_id,
            p.plan_name,
            p.price,
            p.rate_limit_per_minute,
            a.api_name
        FROM public.subscriptions s
        JOIN public.api_details a ON a.api_id = s.api_id
        LEFT JOIN public.api_plans p ON p.plan_id = s.plan_id
        WHERE s.user_id = $1::uuid AND s.api_id = $2::uuid
        ORDER BY s.status = 'ACTIVE' DESC
        LIMIT 1
        """,
        consumer_id,
        api_id,
    )

    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found for this consumer and API.")

    tx = await fetch_one(
        """
        SELECT
            COALESCE(SUM(amount_paid), 0) AS total_amount_paid,
            MAX(timestamp) AS last_payment_date,
            currency
        FROM public.transactions
        WHERE subscription_id = $1::uuid
        GROUP BY currency
        ORDER BY last_payment_date DESC
        LIMIT 1
        """,
        sub["sub_id"],
    )

    # Mask API key for security
    raw_key = sub.get("api_key") or ""
    masked_key = f"{raw_key[:4]}...{raw_key[-4:]}" if len(raw_key) > 8 else "********"

    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        "subscription_id": sub["sub_id"],
        "api_name": sub.get("api_name"),
        "key_status": sub.get("key_status"),
        "api_key_masked": masked_key,
        "plan_details": {
            "plan_id": sub.get("plan_id"),
            "plan_name": sub.get("plan_name"),
            "price": sub.get("price"),
            "rate_limit_per_minute": sub.get("rate_limit_per_minute"),
        },
        "amount_paid": tx.get("total_amount_paid", 0),
        "currency": tx.get("currency", "USD"),
        "last_payment_date": tx.get("last_payment_date"),
    }


# ---------------------------------------------------------------------------
# ML AND SMART ALERT QUERIES
# ---------------------------------------------------------------------------

async def get_recent_api_metrics(api_id: str, minutes: int = 60) -> dict[str, Any]:
    """Fetch current sliding window traffic vs historical baseline."""
    current = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS current_requests,
            COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS current_errors,
            COALESCE(AVG(response_time_ms), 0) AS current_avg_latency,
            COUNT(*) FILTER (WHERE rate_limit_hit)::bigint AS current_rate_limits
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - ($2 * INTERVAL '1 minute')
        """,
        api_id,
        minutes,
    )

    baseline = await fetch_one(
        """
        SELECT
            COALESCE(AVG(request_count), 0) AS baseline_hourly_requests,
            COALESCE(AVG(error_count), 0) AS baseline_hourly_errors,
            COALESCE(AVG(latency_sum_ms / NULLIF(request_count, 0)), 0) AS baseline_avg_latency
        FROM (
            SELECT
                date_trunc('hour', event_timestamp) AS hr,
                COUNT(*)::bigint AS request_count,
                COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count,
                SUM(response_time_ms) AS latency_sum_ms
            FROM analytics.request_events
            WHERE api_id = $1::uuid
              AND event_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY 1
        ) sub
        """,
        api_id,
    )

    return {
        "current": current,
        "baseline": baseline,
    }


async def get_consumer_recent_metrics(consumer_id: str, api_id: str, minutes: int = 60) -> dict[str, Any]:
    sub = await fetch_one(
        "SELECT sub_id FROM public.subscriptions WHERE user_id = $1::uuid AND api_id = $2::uuid LIMIT 1",
        consumer_id,
        api_id,
    )
    if not sub:
        return {"found": False}

    sub_id = sub["sub_id"]
    current = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS requests,
            COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS errors,
            COUNT(*) FILTER (WHERE rate_limit_hit)::bigint AS rate_limits,
            COALESCE(AVG(response_time_ms), 0) AS avg_latency
        FROM analytics.request_events
        WHERE subscription_id = $1::uuid
          AND event_timestamp >= NOW() - ($2 * INTERVAL '1 minute')
        """,
        sub_id,
        minutes,
    )

    history = await fetch_one(
        """
        SELECT
            COALESCE(AVG(req_per_hour), 0) AS avg_hourly_requests,
            COALESCE(STDDEV(req_per_hour), 0) AS std_hourly_requests
        FROM (
            SELECT
                date_trunc('hour', event_timestamp),
                COUNT(*)::bigint AS req_per_hour
            FROM analytics.request_events
            WHERE subscription_id = $1::uuid
              AND event_timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY 1
        ) h
        """,
        sub_id,
    )

    return {
        "found": True,
        "current": current,
        "history": history,
    }


async def get_provider_api_list(provider_id: str) -> list[dict[str, Any]]:
    return await fetch_all(
        """
        SELECT api_id, api_name, rate_limit_per_minute, status
        FROM public.api_details
        WHERE user_id = $1::uuid
        """,
        provider_id,
    )


async def get_hourly_series(api_id: str, limit_hours: int = 168) -> list[dict[str, Any]]:
    """Retrieve hourly request history for forecasting."""
    return await fetch_all(
        """
        SELECT
            date_trunc('hour', event_timestamp) AS hour_bucket,
            COUNT(*)::bigint AS request_count,
            COALESCE(AVG(response_time_ms), 0) AS avg_latency
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - ($2 * INTERVAL '1 hour')
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        api_id,
        limit_hours,
    )