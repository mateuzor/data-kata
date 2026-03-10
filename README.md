# Data Kata — Modern Data Pipeline

> Presented at **Data Kata · 23 Mar 2026**

A fully containerized modern data pipeline demonstrating ingestion from 3 heterogeneous sources, stream processing with PySpark, data lineage with OpenLineage/Marquez, and observability with OpenTelemetry + Prometheus + Grafana.

---

## The Challenge

**Event:** Data Kata — 23 Mar 2026

Build a **Modern Data Pipeline** that satisfies:

| # | Requirement | Solution |
|---|---|---|
| 1 | Ingestion from 3 data sources: Relational DB, File System, WS-* | PostgreSQL (CDC), CSV/Parquet, SOAP WS |
| 2 | Modern stream processing | Apache Spark 3.5 (PySpark Structured Streaming) |
| 3 | Data Lineage | OpenLineage + Marquez |
| 4 | Observability | OpenTelemetry + Prometheus + Grafana |
| 5a | Pipeline: Top Sales per City | `pipeline_top_sales_city.py` |
| 5b | Pipeline: Top Salesman in the Country | `pipeline_top_salesman.py` |
| 6 | Aggregated results in a dedicated DB + API | PostgreSQL (output) + FastAPI |
| 7 | Restrictions: **Python only**, no RedShift, no Hadoop | 100% Python, Spark replaces Hadoop, PostgreSQL replaces Redshift |

**Dataset:** 7 days of sales data across 20 salesmen and 19 Brazilian cities

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   PostgreSQL (CDC)     CSV / Parquet        SOAP WS-*           │
│   sales + salesmen     daily exports        product catalog      │
└──────────┬──────────────────┬───────────────────┬───────────────┘
           │                  │                   │
┌──────────▼──────────────────▼───────────────────▼───────────────┐
│                      INGESTION LAYER                            │
│   Debezium (CDC)      Python File Reader    zeep SOAP Client    │
│                  └──────────────┴───────────────┘               │
│                          Apache Kafka                           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                  PROCESSING LAYER — PySpark                     │
│   Pipeline A: Top Sales per City                                │
│   Pipeline B: Top Salesman in the Country                       │
│                    OpenLineage → Marquez                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                       OUTPUT LAYER                              │
│   PostgreSQL (aggregated results)   FastAPI REST API            │
│   GET /top-sales-city               GET /top-salesman           │
└─────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│               OBSERVABILITY & LINEAGE                           │
│        Prometheus · Grafana · OpenTelemetry · Marquez           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Streaming | Apache Kafka 3.6 + Kafka Connect |
| CDC | Debezium 2.6 (PostgreSQL) |
| Processing | Apache Spark 3.5 (PySpark Structured Streaming) |
| WS-* Source | spyne (server) + zeep (client) |
| Data Lineage | OpenLineage + Marquez |
| Observability | OpenTelemetry + Prometheus + Grafana |
| Source DB | PostgreSQL 15 |
| Output DB | PostgreSQL 15 |
| API | FastAPI + uvicorn |
| Infra | Docker Compose |
| ❌ Not used | ~~RedShift~~, ~~Hadoop~~ |

---

## Data Sources

| # | Type | Description | Kafka Topic |
|---|---|---|---|
| 1 | **Relational DB** | PostgreSQL — sales transactions + salesmen (CDC via Debezium) | `sales.relational` |
| 2 | **File System** | Daily CSV/Parquet exports — additional sales channel | `sales.filesystem` |
| 3 | **WS-\*** | SOAP service — product catalog and salesman registry | `catalog.products`, `catalog.salesmen` |

---

## Pipelines

### Pipeline A — Top Sales per City
Aggregates total sales amount per city across all sources, ranked nationally.

**Result table:** `top_sales_per_city`

```json
{
  "rank_position": 1,
  "city": "São Paulo",
  "state": "SP",
  "total_amount": 284500.80,
  "total_orders": 42
}
```

### Pipeline B — Top Salesman in the Country
Joins sales data with the salesman catalog (from SOAP WS), aggregates total sales per salesman, ranked nationally.

**Result table:** `top_salesman`

```json
{
  "rank_position": 1,
  "salesman_name": "Carlos Andrade",
  "city": "São Paulo",
  "state": "SP",
  "region": "Sudeste",
  "total_amount": 98340.60,
  "total_orders": 18
}
```

---

## Project Structure

```
data-kata/
├── docker-compose.yml
├── requirements.txt
├── sources/
│   ├── relational/
│   │   └── init.sql                  # schema + seed data Mar 3-9
│   ├── filesystem/
│   │   └── generate_files.py         # generates daily CSV/Parquet
│   └── soap_ws/
│       ├── soap_server.py            # spyne SOAP server
│       └── soap_client_example.py    # zeep client example
├── ingestion/
│   ├── kafka_producer_db.py          # PostgreSQL → Kafka + Debezium CDC
│   ├── kafka_producer_files.py       # CSV/Parquet → Kafka
│   ├── kafka_producer_soap.py        # SOAP WS → Kafka
│   └── connectors/
│       └── debezium-postgres.json    # Debezium connector config
├── processing/
│   ├── pipeline_top_sales_city.py    # Pipeline A
│   └── pipeline_top_salesman.py      # Pipeline B
├── output_db/
│   └── init.sql                      # results schema
├── api/
│   ├── main.py                       # FastAPI app
│   ├── requirements.txt
│   └── Dockerfile
├── observability/
│   ├── prometheus.yml
│   └── grafana/
│       ├── datasources.yml
│       └── dashboards/
└── scripts/
    └── run_all.sh                    # run everything locally
```

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### Run

```bash
# Clone
git clone https://github.com/mateuzor/data-kata.git
cd data-kata

# Start everything
bash scripts/run_all.sh
```

### Services

| Service | URL |
|---|---|
| Results API (docs) | http://localhost:8080/docs |
| Spark UI | http://localhost:8090 |
| Marquez (Lineage UI) | http://localhost:3000 |
| Grafana | http://localhost:3001 — `admin / admin` |
| Prometheus | http://localhost:9090 |
| Kafka Connect | http://localhost:8083 |

### API Endpoints

```bash
# Top cities by sales (Mar 3-9)
curl "http://localhost:8080/top-sales-city?limit=10"

# Top salesmen nationwide (Mar 3-9)
curl "http://localhost:8080/top-salesman?limit=10"

# Pipeline run history
curl "http://localhost:8080/pipeline-runs"
```

---

## Data Lineage

Lineage is emitted via **OpenLineage** at pipeline start and completion.
Visualize the full dataset graph at **Marquez Web UI → http://localhost:3000**

```
kafka.sales.relational ──┐
                          ├─► pipeline_top_sales_per_city ─► postgres.top_sales_per_city
kafka.sales.filesystem ──┘

kafka.sales.relational ──┐
kafka.sales.filesystem ──┼─► pipeline_top_salesman ─► postgres.top_salesman
kafka.catalog.salesmen ──┘
```


