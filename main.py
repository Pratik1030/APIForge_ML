import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from db.queries import init_pool, close_pool, pool, fetch_one
from Kafka.consumer import start_consumer, stop_consumer, kafka_status
from routes.provider import router as provider_router
from routes.consumer import router as consumer_router
from routes.ml import router as ml_router

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "ml_data" / "model"
FORECAST_MODEL_PATH = MODEL_DIR / "usage_forecast_model.joblib"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_pool()
    await start_consumer()
    yield
    # Shutdown
    await stop_consumer()
    await close_pool()


app = FastAPI(
    title="APIForge Analytics & ML Service",
    version="2.0.0",
    description="Dedicated microservice for real-time Kafka event analytics, provider & consumer metrics, anomaly detection, usage forecasting and smart alerts.",
    lifespan=lifespan,
)

# CORS configuration
origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register route modules
app.include_router(provider_router)
app.include_router(consumer_router)
app.include_router(ml_router)


@app.get("/")
async def root():
    return {
        "service": "APIForge Analytics & ML Service",
        "version": "2.0.0",
        "status": "online",
        "docs": "/docs",
        "routes": {
            "provider_analytics": "/analytics/provider/{providerId}/apis/{apiId}/...",
            "consumer_analytics": "/analytics/consumer/{consumerId}/apis/{apiId}/...",
            "ml_anomaly": "/ml/anomaly/...",
            "ml_prediction": "/ml/prediction/...",
            "ml_alerts": "/ml/alerts/...",
        },
    }


@app.get("/health")
@app.get("/analytics/health")
async def health_check():
    db_connected = False
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                val = await conn.fetchval("SELECT 1")
                db_connected = (val == 1)
        except Exception:
            db_connected = False

    return {
        "status": "healthy" if db_connected else "degraded",
        "database": {
            "connected": db_connected,
            "configured": bool(os.getenv("DATABASE_URL")),
        },
        "kafka": kafka_status(),
        "models": {
            "forecast_model_saved": FORECAST_MODEL_PATH.exists(),
            "model_directory": str(MODEL_DIR),
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main.py:app", host="0.0.0.0", port=8000, reload=True)
