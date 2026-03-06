"""
Kafka Producer - Relational DB Source (PostgreSQL)
Reads sales data from the source DB and publishes to Kafka topic.
Also registers the Debezium CDC connector for real-time capture.
"""

import json
import logging
import os
import time
from datetime import date

import psycopg2
import psycopg2.extras
import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_SALES     = "sales.relational"

DB_CONFIG = {
    "host":     os.getenv("SOURCE_DB_HOST",     "localhost"),
    "port":     int(os.getenv("SOURCE_DB_PORT", "5433")),
    "dbname":   os.getenv("SOURCE_DB_NAME",     "sales_source"),
    "user":     os.getenv("SOURCE_DB_USER",     "sales_user"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "sales_pass"),
}

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")

DEBEZIUM_CONNECTOR_CONFIG = {
    "name": "sales-source-connector",
    "config": {
        "connector.class":                      "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname":                    os.getenv("SOURCE_DB_HOST", "source-db"),
        "database.port":                        "5432",
        "database.user":                        "sales_user",
        "database.password":                    "sales_pass",
        "database.dbname":                      "sales_source",
        "database.server.name":                 "sales_source",
        "table.include.list":                   "public.sales,public.salesmen,public.products",
        "plugin.name":                          "pgoutput",
        "publication.autocreate.mode":          "filtered",
        "topic.prefix":                         "cdc",
        "transforms":                           "unwrap",
        "transforms.unwrap.type":               "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones":    "false",
        "key.converter":                        "org.apache.kafka.connect.json.JsonConverter",
        "value.converter":                      "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":         "false",
        "value.converter.schemas.enable":       "false",
    },
}


def register_debezium_connector():
    """Register Debezium CDC connector via Kafka Connect REST API."""
    url = f"{KAFKA_CONNECT_URL}/connectors"
    try:
        resp = requests.post(url, json=DEBEZIUM_CONNECTOR_CONFIG, timeout=10)
        if resp.status_code in (200, 201):
            logger.info("Debezium connector registered successfully.")
        elif resp.status_code == 409:
            logger.info("Debezium connector already exists.")
        else:
            logger.warning(f"Connector registration returned {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Failed to register Debezium connector: {e}")


def fetch_sales(conn, start_date: date, end_date: date) -> list[dict]:
    query = """
        SELECT
            s.id,
            s.salesman_id,
            sm.name        AS salesman_name,
            sm.region,
            s.product_id,
            p.name         AS product_name,
            p.category,
            s.city,
            s.state,
            s.quantity,
            s.unit_price,
            s.total_amount,
            s.sale_date::text
        FROM sales s
        JOIN salesmen sm ON sm.id = s.salesman_id
        JOIN products p  ON p.id  = s.product_id
        WHERE s.sale_date BETWEEN %s AND %s
        ORDER BY s.sale_date, s.id
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (start_date, end_date))
        return [dict(row) for row in cur.fetchall()]


def publish_sales(producer: KafkaProducer, sales: list[dict]):
    for record in sales:
        record["source"] = "relational_db"
        key = str(record["id"]).encode()
        value = json.dumps(record, default=str).encode()
        producer.send(TOPIC_SALES, key=key, value=value)
    producer.flush()
    logger.info(f"Published {len(sales)} records to topic '{TOPIC_SALES}'")


def main():
    start_date = date.fromisoformat(os.getenv("DATA_START_DATE", "2026-03-03"))
    end_date   = date.fromisoformat(os.getenv("DATA_END_DATE",   "2026-03-09"))

    logger.info(f"Connecting to source DB at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    conn = psycopg2.connect(**DB_CONFIG)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        retries=5,
        acks="all",
    )

    # Register CDC connector for real-time future changes
    register_debezium_connector()

    # Batch historical data publish
    logger.info(f"Fetching sales from {start_date} to {end_date}")
    sales = fetch_sales(conn, start_date, end_date)
    publish_sales(producer, sales)

    conn.close()
    producer.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
