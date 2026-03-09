"""
FastAPI - Results API
Exposes aggregated pipeline results from the output PostgreSQL database.
Includes OpenTelemetry tracing and Prometheus metrics.
"""

import os
import time
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.requests import Request
from starlette.responses import Response

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# OpenTelemetry
# ─────────────────────────────────────────────

provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
            insecure=True,
        )
    )
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("results-api")

# ─────────────────────────────────────────────
# Prometheus metrics
# ─────────────────────────────────────────────

REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests",
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency",
    ["endpoint"]
)

# ─────────────────────────────────────────────
# DB helper
# ─────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("OUTPUT_DB_HOST",     "localhost"),
    "port":     int(os.getenv("OUTPUT_DB_PORT", "5434")),
    "dbname":   os.getenv("OUTPUT_DB_NAME",     "sales_results"),
    "user":     os.getenv("OUTPUT_DB_USER",     "results_user"),
    "password": os.getenv("OUTPUT_DB_PASSWORD", "results_pass"),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def query_db(sql: str, params=None) -> list[dict]:
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or [])
            return [dict(row) for row in cur.fetchall()]


# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Results API starting up...")
    yield
    logger.info("Results API shutting down.")


app = FastAPI(
    title="Data Kata - Results API",
    description="Exposes aggregated sales pipeline results (Mar 3-9, 2026)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FastAPIInstrumentor.instrument_app(app)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status_code=response.status_code,
    ).inc()
    REQUEST_LATENCY.labels(endpoint=request.url.path).observe(duration)
    return response


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "results-api"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/top-sales-city")
def top_sales_per_city(
    limit: int = Query(default=10, ge=1, le=50, description="Number of cities to return"),
    period_start: str = Query(default="2026-03-03"),
    period_end:   str = Query(default="2026-03-09"),
):
    """
    Returns the top cities by total sales amount for the given period.
    Pipeline A result.
    """
    with tracer.start_as_current_span("top_sales_per_city"):
        try:
            rows = query_db(
                """
                SELECT
                    rank_position,
                    city,
                    state,
                    total_amount,
                    total_orders,
                    period_start::text,
                    period_end::text,
                    processed_at::text
                FROM top_sales_per_city
                WHERE period_start = %s AND period_end = %s
                ORDER BY rank_position
                LIMIT %s
                """,
                (period_start, period_end, limit),
            )
            return {
                "pipeline": "top_sales_per_city",
                "period":   {"start": period_start, "end": period_end},
                "count":    len(rows),
                "results":  rows,
            }
        except Exception as e:
            logger.error(f"top_sales_city error: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/top-salesman")
def top_salesman(
    limit: int = Query(default=10, ge=1, le=50, description="Number of salesmen to return"),
    period_start: str = Query(default="2026-03-03"),
    period_end:   str = Query(default="2026-03-09"),
):
    """
    Returns the top salesmen by total sales amount nationwide for the given period.
    Pipeline B result.
    """
    with tracer.start_as_current_span("top_salesman"):
        try:
            rows = query_db(
                """
                SELECT
                    rank_position,
                    salesman_id,
                    salesman_name,
                    city,
                    state,
                    region,
                    total_amount,
                    total_orders,
                    period_start::text,
                    period_end::text,
                    processed_at::text
                FROM top_salesman
                WHERE period_start = %s AND period_end = %s
                ORDER BY rank_position
                LIMIT %s
                """,
                (period_start, period_end, limit),
            )
            return {
                "pipeline": "top_salesman",
                "period":   {"start": period_start, "end": period_end},
                "count":    len(rows),
                "results":  rows,
            }
        except Exception as e:
            logger.error(f"top_salesman error: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/pipeline-runs")
def pipeline_runs(pipeline_name: str | None = None):
    """Returns recent pipeline execution history."""
    sql = """
        SELECT run_id::text, pipeline_name, status, started_at::text,
               finished_at::text, rows_processed, error_message
        FROM pipeline_runs
        {}
        ORDER BY started_at DESC
        LIMIT 20
    """.format("WHERE pipeline_name = %s" if pipeline_name else "")
    params = (pipeline_name,) if pipeline_name else None
    try:
        return {"runs": query_db(sql, params)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
