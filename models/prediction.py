import datetime
from pathlib import Path
from typing import Any
import joblib
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "ml_data" / "model" / "usage_forecast_model.joblib"

_saved_forecast_artifact: dict[str, Any] | None = None


def get_forecast_artifact() -> dict[str, Any] | None:
    global _saved_forecast_artifact
    if _saved_forecast_artifact is None and MODEL_PATH.exists():
        try:
            _saved_forecast_artifact = joblib.load(MODEL_PATH)
        except Exception as exc:
            print(f"Could not load {MODEL_PATH.name}: {exc}")
    return _saved_forecast_artifact


def predict_next_hour(api_id: str, recent_events_or_rate: float) -> dict[str, Any]:
    """
    Generate minute-by-minute forecast for the next 60 minutes.
    """
    now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    base_per_min = max(0.5, float(recent_events_or_rate) / 60.0) if recent_events_or_rate > 0 else 2.0

    predictions: list[dict[str, Any]] = []
    total_predicted = 0.0

    for m in range(1, 61):
        target_time = now + datetime.timedelta(minutes=m)
        # Small sine wave variance to represent minute fluctuation
        fluctuation = 1.0 + 0.15 * np.sin(2 * np.pi * m / 30.0)
        noise = np.random.uniform(-0.05, 0.05)
        pred_val = round(max(0.0, base_per_min * (fluctuation + noise)), 2)
        total_predicted += pred_val

        lower = round(max(0.0, pred_val * 0.8), 2)
        upper = round(pred_val * 1.25, 2)

        predictions.append({
            "minute_offset": m,
            "timestamp": target_time.isoformat() + "Z",
            "predicted_requests": pred_val,
            "lower_bound": lower,
            "upper_bound": upper,
        })

    return {
        "api_id": api_id,
        "forecast_window": "next_60_minutes",
        "predicted_total_requests": round(total_predicted, 2),
        "avg_predicted_per_minute": round(total_predicted / 60.0, 2),
        "minutes": predictions,
    }


def predict_next_24_hours(api_id: str, hourly_history: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Hourly forecast for the next 24 hours with confidence intervals.
    Incorporates diurnal traffic curve (busy day, quiet night).
    """
    now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    
    # Calculate baseline from history or fallback
    if hourly_history:
        counts = [float(h.get("request_count") or 0) for h in hourly_history]
        mean_hr = float(np.mean(counts)) if counts else 50.0
        std_hr = float(np.std(counts)) if len(counts) > 1 else max(10.0, mean_hr * 0.2)
    else:
        mean_hr = 50.0
        std_hr = 12.0

    # Diurnal multiplier: peaks around 14:00 - 18:00 UTC, dips around 03:00 - 05:00 UTC
    diurnal_profile = {
        0: 0.45, 1: 0.35, 2: 0.30, 3: 0.25, 4: 0.28, 5: 0.40,
        6: 0.65, 7: 0.85, 8: 1.10, 9: 1.30, 10: 1.45, 11: 1.50,
        12: 1.40, 13: 1.55, 14: 1.65, 15: 1.70, 16: 1.60, 17: 1.50,
        18: 1.35, 19: 1.20, 20: 1.05, 21: 0.85, 22: 0.65, 23: 0.50,
    }

    forecast: list[dict[str, Any]] = []
    total_24h = 0.0

    for i in range(1, 25):
        target_time = now + datetime.timedelta(hours=i)
        hour_of_day = target_time.hour
        mult = diurnal_profile.get(hour_of_day, 1.0)

        # Baseline combined with diurnal curve
        predicted = max(1.0, round(mean_hr * mult, 2))
        total_24h += predicted

        margin = max(5.0, round(std_hr * mult * 1.645, 2))  # 90% confidence interval
        lower = round(max(0.0, predicted - margin), 2)
        upper = round(predicted + margin, 2)

        forecast.append({
            "hour_offset": i,
            "timestamp": target_time.isoformat() + "Z",
            "hour_of_day": hour_of_day,
            "predicted_requests": predicted,
            "confidence_interval": {
                "lower": lower,
                "upper": upper,
                "confidence_level": "90%",
            },
        })

    return {
        "api_id": api_id,
        "forecast_window": "next_24_hours",
        "predicted_24h_total": round(total_24h, 2),
        "historical_hourly_mean": round(mean_hr, 2),
        "hourly_forecast": forecast,
    }


def predict_peak_forecast(api_id: str, hourly_history: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Finds when the next traffic peak will occur and estimated size.
    """
    f24 = predict_next_24_hours(api_id, hourly_history)
    hourly = f24["hourly_forecast"]

    peak_item = max(hourly, key=lambda x: x["predicted_requests"])
    baseline = f24["historical_hourly_mean"]
    multiplier = round(peak_item["predicted_requests"] / baseline, 2) if baseline > 0 else 1.0

    # Suggest operational guidance
    if multiplier > 1.8:
        action = "High surge anticipated: pre-warm backend instances and ensure database connection pool capacity."
    elif multiplier > 1.4:
        action = "Moderate peak expected: standard autoscaling policies sufficient."
    else:
        action = "Traffic within normal operating parameters: no special action required."

    return {
        "api_id": api_id,
        "next_peak_timestamp": peak_item["timestamp"],
        "hours_until_peak": peak_item["hour_offset"],
        "peak_hour_of_day": peak_item["hour_of_day"],
        "predicted_peak_requests": peak_item["predicted_requests"],
        "peak_upper_bound": peak_item["confidence_interval"]["upper"],
        "baseline_hourly_requests": baseline,
        "surge_multiplier": f"{multiplier}x",
        "recommended_action": action,
    }
