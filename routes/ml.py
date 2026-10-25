from fastapi import APIRouter, Query, HTTPException
from db.queries import (
    get_recent_api_metrics,
    get_consumer_recent_metrics,
    get_hourly_series,
    get_provider_api_list,
    fetch_one,
    fetch_all,
    json_safe,
)
from models.anomaly import (
    detect_api_anomaly,
    detect_consumer_anomaly,
    load_csv_anomaly_history,
    train_isolation_forest_for_api,
)
from models.prediction import (
    predict_next_hour,
    predict_next_24_hours,
    predict_peak_forecast,
)

router = APIRouter(prefix="/ml", tags=["ML and Predictive Analytics"])


# ---------------------------------------------------------------------------
# ANOMALY DETECTION ENDPOINTS
# ---------------------------------------------------------------------------

@router.get("/anomaly/{api_id}/status")
async def anomaly_status(api_id: str):
    """Check if this API is currently experiencing anomalous traffic."""
    metrics = await get_recent_api_metrics(api_id, minutes=60)
    result = detect_api_anomaly(api_id, metrics["current"], metrics["baseline"])
    return json_safe(result)


@router.get("/anomaly/{api_id}/history")
async def anomaly_history(
    api_id: str,
    limit: int = Query(50, ge=1, le=500),
):
    """List of past anomaly events with timestamps and details."""
    csv_history = load_csv_anomaly_history(api_id, limit=limit)
    if csv_history:
        return {
            "api_id": api_id,
            "source": "historical_anomaly_log",
            "count": len(csv_history),
            "events": json_safe(csv_history),
        }

    # Query recent hourly windows with error surges or rate limit spikes
    anomalous_windows = await fetch_all(
        """
        SELECT
            date_trunc('hour', event_timestamp) AS timestamp,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS error_count,
            COUNT(*) FILTER (WHERE rate_limit_hit)::bigint AS rate_limit_hits,
            COALESCE(AVG(response_time_ms), 0) AS avg_latency_ms
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - INTERVAL '30 days'
        GROUP BY 1
        HAVING (COUNT(*) FILTER (WHERE status_code >= 400) * 1.0 / NULLIF(COUNT(*), 0)) >= 0.20
            OR COUNT(*) FILTER (WHERE rate_limit_hit) >= 10
        ORDER BY 1 DESC
        LIMIT $2
        """,
        api_id,
        limit,
    )

    events = [
        {
            "timestamp": w["timestamp"],
            "type": "error_surge" if (w["error_count"] or 0) > (w["rate_limit_hits"] or 0) else "rate_limit_spike",
            "request_count": w["request_count"],
            "error_count": w["error_count"],
            "avg_latency_ms": round(float(w["avg_latency_ms"] or 0), 2),
            "severity": "HIGH" if (w["error_count"] or 0) > 20 else "MEDIUM",
        }
        for w in anomalous_windows
    ]

    return {
        "api_id": api_id,
        "source": "event_log_analytics",
        "count": len(events),
        "events": json_safe(events),
    }


@router.get("/anomaly/consumer/{consumer_id}/apis/{api_id}")
async def consumer_anomaly(consumer_id: str, api_id: str):
    """Evaluate whether a specific consumer is displaying anomalous behavior."""
    metrics = await get_consumer_recent_metrics(consumer_id, api_id, minutes=60)
    result = detect_consumer_anomaly(consumer_id, api_id, metrics)
    return json_safe(result)


@router.post("/anomaly/{api_id}/train")
async def train_anomaly_model(api_id: str):
    """Trigger manual retraining of the Isolation Forest model on latest API data."""
    records = await get_hourly_series(api_id, limit_hours=336)  # 14 days of data
    summary = train_isolation_forest_for_api(api_id, records)
    return json_safe(summary)


# ---------------------------------------------------------------------------
# USAGE PREDICTION ENDPOINTS
# ---------------------------------------------------------------------------

@router.get("/prediction/{api_id}/next-hour")
async def prediction_next_hour(api_id: str):
    """Predicted request volume for each of the next 60 minutes."""
    recent = await fetch_one(
        """
        SELECT COUNT(*)::bigint AS req_last_hour
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - INTERVAL '1 hour'
        """,
        api_id,
    )
    req_rate = recent.get("req_last_hour") or 30.0
    forecast = predict_next_hour(api_id, req_rate)
    return json_safe(forecast)


@router.get("/prediction/{api_id}/next-24-hours")
async def prediction_next_24_hours(api_id: str):
    """Hourly forecast for the next 24 hours with 90% confidence intervals."""
    hourly_history = await get_hourly_series(api_id, limit_hours=168)
    forecast = predict_next_24_hours(api_id, hourly_history)
    return json_safe(forecast)


@router.get("/prediction/{api_id}/peak-forecast")
async def prediction_peak(api_id: str):
    """Predict when the next traffic peak will occur and its estimated volume."""
    hourly_history = await get_hourly_series(api_id, limit_hours=168)
    peak = predict_peak_forecast(api_id, hourly_history)
    return json_safe(peak)


# ---------------------------------------------------------------------------
# SMART ALERTS ENDPOINTS
# ---------------------------------------------------------------------------

async def _build_api_alerts(api_id: str, api_name: str | None = None) -> list[dict]:
    alerts = []

    # 1. Unusual error spike (>15% in the last hour)
    err_stats = await fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS errors
        FROM analytics.request_events
        WHERE api_id = $1::uuid
          AND event_timestamp >= NOW() - INTERVAL '1 hour'
        """,
        api_id,
    )
    tot = err_stats.get("total") or 0
    errs = err_stats.get("errors") or 0
    if tot >= 10 and (errs / tot) >= 0.15:
        alerts.append({
            "api_id": api_id,
            "api_name": api_name,
            "type": "error_spike",
            "severity": "HIGH",
            "message": f"Unusual error spike: {errs} of {tot} requests ({errs/tot*100:.1f}%) failed in the last hour.",
        })

    # 2. Consumers approaching rate limit (>85% of limit in current minute)
    consumers_near_limit = await fetch_all(
        """
        SELECT
            s.user_id AS consumer_id,
            u.user_name AS consumer_name,
            COALESCE(p.rate_limit_per_minute, a.rate_limit_per_minute, 60) AS limit_val,
            COUNT(re.event_id)::bigint AS req_this_min
        FROM public.subscriptions s
        JOIN public.api_details a ON a.api_id = s.api_id
        LEFT JOIN public.api_plans p ON p.plan_id = s.plan_id
        JOIN public.base_user u ON u.user_id = s.user_id
        JOIN analytics.request_events re ON re.subscription_id = s.sub_id
        WHERE s.api_id = $1::uuid
          AND re.event_timestamp >= date_trunc('minute', NOW())
        GROUP BY s.user_id, u.user_name, p.rate_limit_per_minute, a.rate_limit_per_minute
        HAVING COUNT(re.event_id) >= (COALESCE(p.rate_limit_per_minute, a.rate_limit_per_minute, 60) * 0.85)
        """,
        api_id,
    )
    for c in consumers_near_limit:
        alerts.append({
            "api_id": api_id,
            "api_name": api_name,
            "type": "consumer_rate_limit_warning",
            "severity": "MEDIUM",
            "consumer_id": c["consumer_id"],
            "consumer_name": c["consumer_name"],
            "message": f"Consumer {c.get('consumer_name')} has reached {c['req_this_min']} requests/min (limit: {c['limit_val']}).",
        })

    # 3. Traffic surge anomaly
    metrics = await get_recent_api_metrics(api_id, minutes=60)
    ano = detect_api_anomaly(api_id, metrics["current"], metrics["baseline"])
    if ano["is_anomalous"]:
        alerts.append({
            "api_id": api_id,
            "api_name": api_name,
            "type": "traffic_anomaly_detected",
            "severity": ano["severity"],
            "message": "; ".join(ano["reasons"]),
        })

    # 4. Predicted traffic surge in next hour
    peak = predict_peak_forecast(api_id, [])
    if peak.get("surge_multiplier") and float(peak["surge_multiplier"].replace("x", "")) >= 1.7:
        alerts.append({
            "api_id": api_id,
            "api_name": api_name,
            "type": "predicted_traffic_surge",
            "severity": "MEDIUM",
            "message": f"Forecasted surge of {peak['predicted_peak_requests']} reqs at {peak['next_peak_timestamp']} ({peak['surge_multiplier']} normal).",
        })

    return alerts


@router.get("/alerts/{target_id}")
async def smart_alerts(target_id: str):
    """
    Intelligently resolves whether target_id is a Provider ID or an API ID
    and returns all active alerts.
    """
    # 1. Check if target_id is an API
    api_check = await fetch_one(
        "SELECT api_id, api_name, user_id AS provider_id FROM public.api_details WHERE api_id = $1::uuid",
        target_id,
    )
    if api_check:
        alerts = await _build_api_alerts(target_id, api_check.get("api_name"))
        return {
            "entity_type": "api",
            "api_id": target_id,
            "api_name": api_check.get("api_name"),
            "active_alert_count": len(alerts),
            "alerts": json_safe(alerts),
        }

    # 2. Check if target_id is a Provider
    provider_apis = await get_provider_api_list(target_id)
    if provider_apis:
        all_alerts = []
        for api in provider_apis:
            api_alerts = await _build_api_alerts(str(api["api_id"]), api.get("api_name"))
            all_alerts.extend(api_alerts)

        return {
            "entity_type": "provider",
            "provider_id": target_id,
            "apis_monitored": len(provider_apis),
            "active_alert_count": len(all_alerts),
            "alerts": json_safe(all_alerts),
        }

    # Fallback if no matching record in DB yet
    return {
        "target_id": target_id,
        "active_alert_count": 0,
        "alerts": [],
        "message": "No matching provider or API found.",
    }


@router.get("/alerts/provider/{provider_id}")
async def provider_smart_alerts(provider_id: str):
    """All active alerts across all provider's APIs."""
    provider_apis = await get_provider_api_list(provider_id)
    all_alerts = []
    for api in provider_apis:
        api_alerts = await _build_api_alerts(str(api["api_id"]), api.get("api_name"))
        all_alerts.extend(api_alerts)

    return {
        "provider_id": provider_id,
        "apis_monitored": len(provider_apis),
        "active_alert_count": len(all_alerts),
        "alerts": json_safe(all_alerts),
    }


@router.get("/alerts/api/{api_id}")
async def api_smart_alerts(api_id: str):
    """All active alerts for a specific API."""
    api_row = await fetch_one("SELECT api_name FROM public.api_details WHERE api_id = $1::uuid", api_id)
    name = api_row.get("api_name") if api_row else None
    alerts = await _build_api_alerts(api_id, name)
    return {
        "api_id": api_id,
        "api_name": name,
        "active_alert_count": len(alerts),
        "alerts": json_safe(alerts),
    }
