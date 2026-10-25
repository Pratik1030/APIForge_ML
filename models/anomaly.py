import datetime
from pathlib import Path
from typing import Any
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_DIR = BASE_DIR / "ml_data" / "model"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = BASE_DIR / "ml_data" / "processed"
CSV_ANOMALY_PATH = BASE_DIR / "ml_data" / "detected_anomalies.csv"
CSV_WINDOW_PATH = BASE_DIR / "ml_data" / "window_anomaly_results.csv"

# In-memory cache for loaded Isolation Forest models
_models_cache: dict[str, IsolationForest] = {}


def get_model_path_for_api(api_id: str) -> Path:
    return MODEL_DIR / f"anomaly_{api_id}.joblib"


def load_model_for_api(api_id: str) -> IsolationForest | None:
    if api_id in _models_cache:
        return _models_cache[api_id]
    model_path = get_model_path_for_api(api_id)
    if model_path.exists():
        try:
            model = joblib.load(model_path)
            _models_cache[api_id] = model
            return model
        except Exception as exc:
            print(f"Failed to load anomaly model for {api_id}: {exc}")
    return None


def extract_features_vector(req_count: float, error_rate: float, avg_latency: float, rate_limit_hits: float) -> np.ndarray:
    return np.array([[req_count, error_rate, avg_latency, rate_limit_hits]], dtype=float)


def detect_api_anomaly(
    api_id: str,
    current_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
) -> dict[str, Any]:
    """
    Score API traffic against both statistical dynamic baselines
    and Isolation Forest if a model has been saved.
    """
    curr_reqs = float(current_metrics.get("current_requests") or 0)
    curr_errors = float(current_metrics.get("current_errors") or 0)
    curr_latency = float(current_metrics.get("current_avg_latency") or 0)
    curr_rl = float(current_metrics.get("current_rate_limits") or 0)
    curr_error_rate = (curr_errors / curr_reqs * 100.0) if curr_reqs > 0 else 0.0

    base_reqs = float(baseline_metrics.get("baseline_hourly_requests") or 0)
    base_errors = float(baseline_metrics.get("baseline_hourly_errors") or 0)
    base_latency = float(baseline_metrics.get("baseline_avg_latency") or 0)

    reasons: list[str] = []
    is_anomalous = False
    severity = "NORMAL"

    # Rule 1: High Error Spike (>25% and at least 5 failures)
    if curr_reqs >= 5 and curr_error_rate >= 25.0:
        is_anomalous = True
        reasons.append(f"High error rate detected: {curr_error_rate:.1f}% of requests failing")
        severity = "HIGH"

    # Rule 2: Sudden Traffic Surge (> 3.5x normal baseline with at least 50 requests)
    if base_reqs > 10 and curr_reqs > (base_reqs * 3.5):
        is_anomalous = True
        reasons.append(f"Traffic surge: {curr_reqs:.0f} reqs/hr vs baseline {base_reqs:.0f} reqs/hr")
        severity = "CRITICAL" if severity == "HIGH" else "HIGH"

    # Rule 3: Latency Degradation (> 3x baseline and > 500ms)
    if base_latency > 0 and curr_latency > (base_latency * 3.0) and curr_latency > 500.0:
        is_anomalous = True
        reasons.append(f"Severe latency degradation: {curr_latency:.1f}ms vs baseline {base_latency:.1f}ms")
        severity = "HIGH" if severity != "CRITICAL" else severity

    # Rule 4: High Rate-Limiting Hits (>20 hits in window)
    if curr_rl >= 20:
        is_anomalous = True
        reasons.append(f"Rate limit saturation: {int(curr_rl)} consumers throttled")
        if severity == "NORMAL":
            severity = "MEDIUM"

    # Isolation Forest inference if model exists
    model = load_model_for_api(api_id)
    iso_score = 0.0
    if model is not None:
        try:
            X = extract_features_vector(curr_reqs, curr_error_rate, curr_latency, curr_rl)
            pred = model.predict(X)[0]  # -1 for anomaly, 1 for normal
            decision = model.decision_function(X)[0]
            iso_score = round(float(decision), 4)
            if pred == -1:
                is_anomalous = True
                reasons.append(f"Isolation Forest flagged traffic pattern as an outlier (score: {iso_score})")
                if severity == "NORMAL":
                    severity = "HIGH"
        except Exception as exc:
            print(f"Isolation Forest inference error: {exc}")

    return {
        "api_id": api_id,
        "is_anomalous": is_anomalous,
        "severity": severity,
        "anomaly_score": iso_score,
        "reasons": reasons,
        "current_window": {
            "requests": curr_reqs,
            "errors": curr_errors,
            "error_rate_percent": round(curr_error_rate, 2),
            "avg_latency_ms": round(curr_latency, 2),
            "rate_limit_hits": curr_rl,
        },
        "baseline_hourly": {
            "requests": round(base_reqs, 2),
            "errors": round(base_errors, 2),
            "avg_latency_ms": round(base_latency, 2),
        },
    }


def detect_consumer_anomaly(consumer_id: str, api_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
    if not metrics.get("found"):
        return {
            "consumer_id": consumer_id,
            "api_id": api_id,
            "is_anomalous": False,
            "message": "No active subscription or traffic record found for this consumer.",
            "reasons": [],
        }

    curr = metrics.get("current") or {}
    hist = metrics.get("history") or {}

    reqs = float(curr.get("requests") or 0)
    errors = float(curr.get("errors") or 0)
    rl_hits = float(curr.get("rate_limits") or 0)
    avg_hist = float(hist.get("avg_hourly_requests") or 0)
    std_hist = float(hist.get("std_hourly_requests") or 0)

    reasons = []
    is_anomalous = False

    # Z-score test
    if std_hist > 0 and reqs > 10:
        z = (reqs - avg_hist) / std_hist
        if z >= 3.0:
            is_anomalous = True
            reasons.append(f"Volume burst: request count is {z:.1f} standard deviations above historical mean")

    # Error burst
    if reqs >= 10 and (errors / reqs) >= 0.4:
        is_anomalous = True
        reasons.append(f"Excessive error rate: {errors/reqs*100:.1f}% failed requests")

    # Excessive rate limit throttling
    if rl_hits >= 15:
        is_anomalous = True
        reasons.append(f"Repeated rate limit threshold violations: {int(rl_hits)} throttled requests in window")

    return {
        "consumer_id": consumer_id,
        "api_id": api_id,
        "is_anomalous": is_anomalous,
        "reasons": reasons,
        "recent_metrics": {
            "requests": reqs,
            "errors": errors,
            "rate_limits": rl_hits,
        },
        "historical_metrics": {
            "avg_hourly_requests": round(avg_hist, 2),
            "std_hourly_requests": round(std_hist, 2),
        },
    }


def load_csv_anomaly_history(api_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Loads historical anomaly events from CSV if present."""
    records: list[dict[str, Any]] = []
    for path in [CSV_ANOMALY_PATH, CSV_WINDOW_PATH]:
        if path.exists():
            try:
                df = pd.read_csv(path)
                col = next((c for c in ("api_id", "apiId", "api") if c in df.columns), None)
                if col:
                    df = df[df[col].astype(str) == str(api_id)]
                if not df.empty:
                    df_slice = df.tail(limit).replace({np.nan: None})
                    records.extend(df_slice.to_dict("records"))
                    break
            except Exception as e:
                print(f"Error reading {path.name}: {e}")
    return records


def train_isolation_forest_for_api(api_id: str, training_records: list[dict[str, Any]]) -> dict[str, Any]:
    """Train and persist Isolation Forest model for an API."""
    if len(training_records) < 5:
        # Synthesize baseline data if history is short to ensure model fits
        rows = [
            [float(r.get("request_count") or 10), 0.0, float(r.get("avg_latency") or 120.0), 0.0]
            for r in training_records
        ]
        # Add normal baseline jitter
        for _ in range(20):
            rows.append([np.random.normal(20, 5), np.random.uniform(0, 5), np.random.normal(120, 20), 0])
    else:
        rows = [
            [float(r.get("request_count") or 1), 0.0, float(r.get("avg_latency") or 100.0), 0.0]
            for r in training_records
        ]

    X = np.array(rows, dtype=float)
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X)

    model_path = get_model_path_for_api(api_id)
    joblib.dump(model, model_path)
    _models_cache[api_id] = model

    return {
        "status": "success",
        "api_id": api_id,
        "model_file": str(model_path.name),
        "samples_fitted": len(rows),
        "trained_at": datetime.datetime.utcnow().isoformat(),
        "model_type": "Isolation Forest (scikit-learn)",
    }
