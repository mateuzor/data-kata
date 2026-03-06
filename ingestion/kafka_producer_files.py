"""
Kafka Producer - Filesystem Source
Reads daily CSV/Parquet files and publishes to Kafka topic.
Watches for new files (simulates a daily batch job).
"""

import json
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_SALES     = "sales.filesystem"
DATA_DIR        = Path(os.getenv("DATA_DIR", "./sources/filesystem/data"))

# ─────────────────────────────────────────────
# OpenTelemetry setup
# ─────────────────────────────────────────────

provider = TracerProvider()
otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    insecure=True,
)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("kafka-producer-files")


def publish_file(producer: KafkaProducer, filepath: Path):
    """Read a CSV/Parquet file and publish each row to Kafka."""
    with tracer.start_as_current_span(f"publish_file:{filepath.name}") as span:
        span.set_attribute("file.path", str(filepath))
        span.set_attribute("file.source", "filesystem")

        if filepath.suffix == ".csv":
            df = pd.read_csv(filepath)
        elif filepath.suffix == ".parquet":
            df = pd.read_parquet(filepath)
        else:
            logger.warning(f"Unsupported file format: {filepath}")
            return

        records = df.to_dict(orient="records")
        for i, record in enumerate(records):
            record["source"] = "filesystem"
            key   = f"{record.get('sale_date','')}-{i}".encode()
            value = json.dumps(record, default=str).encode()
            producer.send(TOPIC_SALES, key=key, value=value)

        producer.flush()
        span.set_attribute("records.count", len(records))
        logger.info(f"[{filepath.name}] Published {len(records)} records → '{TOPIC_SALES}'")


def process_date_range(producer: KafkaProducer, start: date, end: date):
    """Process all CSV files within a date range."""
    current = start
    while current <= end:
        date_str  = current.strftime("%Y%m%d")
        csv_file  = DATA_DIR / f"sales_{date_str}.csv"
        if csv_file.exists():
            publish_file(producer, csv_file)
        else:
            logger.warning(f"File not found: {csv_file}")
        current += timedelta(days=1)


def main():
    start_date = date.fromisoformat(os.getenv("DATA_START_DATE", "2026-03-03"))
    end_date   = date.fromisoformat(os.getenv("DATA_END_DATE",   "2026-03-09"))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        retries=5,
        acks="all",
    )

    with tracer.start_as_current_span("filesystem-ingestion-batch"):
        process_date_range(producer, start_date, end_date)

    producer.close()
    logger.info("Filesystem ingestion complete.")


if __name__ == "__main__":
    main()
