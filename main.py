
import os
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import date, timedelta

import asyncpg
import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
MODEL_PATH = BASE_DIR / "ml_data" / "model" / "usage_forecast_model.joblib"

DATABASE_URL = os.getenv("DATABASE_URL")
pool = None
forecast_artifact = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool, forecast_artifact

    db_url = DATABASE_URL.replace(
    "postgresql+asyncpg://",
    "postgresql://",
    1
)

    if DATABASE_URL:
        pool = await asyncpg.create_pool(
            db_url,
            min_size=1,
            max_size=5,
            command_timeout=15,
        )

    if MODEL_PATH.exists():
        forecast_artifact = joblib.load(MODEL_PATH)

    yield

    if pool:
        await pool.close()


app = FastAPI(
    title="APIForge Analytics API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def fetch_all(query, *args):
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable. Configure DATABASE_URL in .env",
        )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Analytics database query failed: {type(exc).__name__}",
        )


def json_safe(value):
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def make_features(dates, origin_date):
    dates = pd.DatetimeIndex(dates)
    elapsed_days = (dates - pd.Timestamp(origin_date)).days.astype(float)
    trend = elapsed_days / 30.0
    day_num = dates.dayofyear.to_numpy(dtype=float)
    year_length = np.where(dates.is_leap_year, 366.0, 365.0)

    return np.column_stack([
        np.ones(len(dates)),
        trend,
        np.sin(2 * np.pi * day_num / year_length),
        np.cos(2 * np.pi * day_num / year_length),
    ])


@app.get("/")
async def root():
    return {
        "service": "APIForge Analytics API",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/analytics/health")
async def analytics_health():
    return {
        "status": "healthy",
        "database_configured": bool(DATABASE_URL),
        "database_connected": pool is not None,
        "forecast_model_loaded": forecast_artifact is not None,
    }


@app.get("/analytics/provider/summary")
async def provider_summary():
    rows = await fetch_all("""
        SELECT
            COUNT(*)::bigint AS total_requests,
            COUNT(*) FILTER (
                WHERE status_code >= 400
            )::bigint AS error_count,
            COUNT(*) FILTER (
                WHERE rate_limit_hit IS TRUE
            )::bigint AS rate_limited_count,
            COALESCE(AVG(response_time_ms), 0) AS avg_latency_ms,
            COALESCE(MAX(response_time_ms), 0) AS max_latency_ms
        FROM analytics.request_events
    """)

    result = rows[0]
    total = result["total_requests"] or 0
    errors = result["error_count"] or 0

    result["error_rate_percent"] = (
        round(errors * 100 / total, 2) if total else 0
    )
    return json_safe(result)


@app.get("/analytics/provider/usage-trend")
async def provider_usage_trend(
    days: int = Query(default=30, ge=1, le=365)
):
    rows = await fetch_all("""
        SELECT
            date_trunc('day', event_timestamp)::date AS day,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE status_code >= 400)::bigint
                AS error_count,
            COALESCE(AVG(response_time_ms), 0) AS avg_latency_ms
        FROM analytics.request_events
        WHERE event_timestamp >= NOW() - ($1 * INTERVAL '1 day')
        GROUP BY 1
        ORDER BY 1
    """, days)

    return {"days": days, "data": json_safe(rows)}


@app.get("/analytics/provider/apis")
async def provider_api_usage():
    rows = await fetch_all("""
        SELECT
            api_id,
            SUM(request_count)::bigint AS request_count,
            SUM(error_count)::bigint AS error_count,
            SUM(rate_limited_count)::bigint AS rate_limited_count,
            CASE WHEN SUM(request_count) > 0
                 THEN SUM(latency_sum_ms) / SUM(request_count)
                 ELSE 0 END AS avg_latency_ms,
            MAX(latency_max_ms) AS max_latency_ms
        FROM analytics.usage_minute
        GROUP BY api_id
        ORDER BY request_count DESC
    """)

    return {"data": json_safe(rows)}


@app.get("/analytics/provider/consumers")
async def provider_consumers():
    rows = await fetch_all("""
        SELECT
            subscription_id,
            SUM(request_count)::bigint AS request_count,
            SUM(error_count)::bigint AS error_count,
            SUM(rate_limited_count)::bigint AS rate_limited_count
        FROM analytics.usage_minute
        GROUP BY subscription_id
        ORDER BY request_count DESC
        LIMIT 100
    """)

    return {"data": json_safe(rows)}


@app.get("/analytics/provider/slow-endpoints")
async def slow_endpoints(
    limit: int = Query(default=10, ge=1, le=100)
):
    rows = await fetch_all("""
        SELECT
            endpoint,
            http_method,
            COUNT(*)::bigint AS request_count,
            AVG(response_time_ms) AS avg_latency_ms,
            MAX(response_time_ms) AS max_latency_ms
        FROM analytics.request_events
        GROUP BY endpoint, http_method
        ORDER BY avg_latency_ms DESC NULLS LAST
        LIMIT $1
    """, limit)

    return {"data": json_safe(rows)}


@app.get("/analytics/provider/peak-hours")
async def provider_peak_hours():
    rows = await fetch_all("""
        SELECT
            EXTRACT(HOUR FROM event_timestamp)::int AS hour,
            COUNT(*)::bigint AS request_count
        FROM analytics.request_events
        GROUP BY 1
        ORDER BY 1
    """)

    return {"data": json_safe(rows)}


@app.get("/analytics/consumer/{subscription_id}/usage")
async def consumer_usage(subscription_id: str):
    rows = await fetch_all("""
        SELECT
            bucket_start,
            api_id,
            request_count,
            error_count,
            rate_limited_count,
            latency_max_ms
        FROM analytics.usage_minute
        WHERE subscription_id = $1::uuid
        ORDER BY bucket_start DESC
        LIMIT 1000
    """, subscription_id)

    return {"subscription_id": subscription_id, "data": json_safe(rows)}


@app.get("/analytics/anomalies/{api_id}")
async def anomaly_results(api_id: str):
    # Anomaly model results are not currently persisted in a confirmed
    # analytics table. Return an explicit status instead of inventing data.
    return {
        "api_id": api_id,
        "data": [],
        "message": "Anomaly result storage and query are not yet connected.",
    }


@app.get("/analytics/prediction/{api_id}")
async def usage_prediction(
    api_id: str,
    days: int = Query(default=180, ge=1, le=365),
):
    if forecast_artifact is None:
        raise HTTPException(
            status_code=503,
            detail="Saved usage model not found. Train and save it first.",
        )

    last_day = pd.Timestamp(forecast_artifact["last_observed_day"])
    origin_date = pd.Timestamp(forecast_artifact["origin_date"])
    theta = np.asarray(forecast_artifact["theta"], dtype=float)

    dates = pd.date_range(
        last_day + pd.Timedelta(days=1),
        periods=days,
        freq="D",
    )
    X = make_features(dates, origin_date)
    predictions = np.maximum(X @ theta, 0)

    daily = pd.DataFrame({
        "date": dates,
        "predicted_requests": predictions,
    })
    daily["month"] = daily["date"].dt.to_period("M").astype(str)

    monthly = (
        daily.groupby("month", as_index=False)["predicted_requests"].sum()
    )

    return json_safe({
        "api_id": api_id,
        "model": forecast_artifact["model_type"],
        "training_days": forecast_artifact["training_days"],
        "testing_days": forecast_artifact["testing_days"],
        "evaluation_metrics": forecast_artifact.get("testing_metrics", {}),
        "daily": [
            {
                "date": row.date.strftime("%Y-%m-%d"),
                "predicted_requests": round(float(row.predicted_requests), 2),
            }
            for row in daily.itertuples(index=False)
        ],
        "monthly": [
            {
                "month": row.month,
                "predicted_requests": round(float(row.predicted_requests), 2),
            }
            for row in monthly.itertuples(index=False)
        ],
        "notice": (
            "The saved model is currently a platform-level model. "
            "api_id is a request parameter for frontend compatibility; "
            "predictions are not yet API-specific."
        ),
    })
