"""
Kafka Producer - WS-* SOAP Source (Product Catalog)
Calls the SOAP service and publishes catalog data to Kafka.
Acts as a bridge between the legacy WS-* world and the modern streaming platform.
"""

import json
import logging
import os
import time

import zeep
from kafka import KafkaProducer
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_PRODUCTS   = "catalog.products"
TOPIC_SALESMEN   = "catalog.salesmen"

SOAP_HOST  = os.getenv("SOAP_WS_HOST", "localhost")
SOAP_PORT  = os.getenv("SOAP_WS_PORT", "8001")
WSDL_URL   = f"http://{SOAP_HOST}:{SOAP_PORT}/?wsdl"

# ─────────────────────────────────────────────
# OpenTelemetry setup
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
tracer = trace.get_tracer("kafka-producer-soap")


def get_soap_client(retries: int = 5, delay: int = 3) -> zeep.Client:
    """Get SOAP client with retry logic (service may take a moment to start)."""
    for attempt in range(retries):
        try:
            client = zeep.Client(wsdl=WSDL_URL)
            logger.info(f"Connected to SOAP service at {WSDL_URL}")
            return client
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{retries} - SOAP unavailable: {e}")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to SOAP service at {WSDL_URL}")


def fetch_and_publish_products(client: zeep.Client, producer: KafkaProducer):
    with tracer.start_as_current_span("soap.GetAllProducts") as span:
        products = client.service.GetAllProducts()
        records = []
        for p in (products or []):
            record = {
                "source":      "soap_ws",
                "product_id":  p.product_id,
                "name":        p.name,
                "category":    p.category,
                "unit_price":  float(p.unit_price),
                "is_active":   p.is_active,
            }
            records.append(record)
            key   = str(p.product_id).encode()
            value = json.dumps(record).encode()
            producer.send(TOPIC_PRODUCTS, key=key, value=value)

        producer.flush()
        span.set_attribute("products.count", len(records))
        logger.info(f"Published {len(records)} products to '{TOPIC_PRODUCTS}'")


def fetch_and_publish_salesmen(client: zeep.Client, producer: KafkaProducer):
    with tracer.start_as_current_span("soap.GetAllSalesmen") as span:
        salesmen = client.service.GetAllSalesmen()
        records = []
        for s in (salesmen or []):
            record = {
                "source":      "soap_ws",
                "salesman_id": s.salesman_id,
                "name":        s.name,
                "city":        s.city,
                "state":       s.state,
                "region":      s.region,
            }
            records.append(record)
            key   = str(s.salesman_id).encode()
            value = json.dumps(record).encode()
            producer.send(TOPIC_SALESMEN, key=key, value=value)

        producer.flush()
        span.set_attribute("salesmen.count", len(records))
        logger.info(f"Published {len(records)} salesmen to '{TOPIC_SALESMEN}'")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        retries=5,
        acks="all",
    )
    client = get_soap_client()

    with tracer.start_as_current_span("soap-catalog-ingestion"):
        fetch_and_publish_products(client, producer)
        fetch_and_publish_salesmen(client, producer)

    producer.close()
    logger.info("SOAP ingestion complete.")


if __name__ == "__main__":
    main()
