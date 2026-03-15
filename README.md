# Data Kata — Modern Data Pipeline

> Presented at **Data Kata · 23 Mar 2026**

A fully containerized modern data pipeline demonstrating ingestion from 3 heterogeneous sources, batch processing with Apache Flink, data lineage with OpenLineage/Marquez, and observability with Prometheus + Grafana.

---

## The Challenge

**Event:** Data Kata — 23 Mar 2026

Build a **Modern Data Pipeline** that satisfies:

| # | Requirement | Solution |
|---|---|---|
| 1 | Ingestion from 3 data sources: Relational DB, File System, WS-* | PostgreSQL (CDC), CSV, SOAP WS |
| 2 | Modern batch processing | Apache Flink 1.18 (batch mode) |
| 3 | Data Lineage | OpenLineage + Marquez |
| 4 | Observability | Prometheus + Grafana |
| 5a | Pipeline: Top Sales per City | `TopSalesCityJob` |
| 5b | Pipeline: Top Salesman in the Country | `TopSalesmanJob` |
| 6 | Aggregated results in a dedicated DB + API | PostgreSQL (output) + Scala HTTP API |
| 7 | Restrictions: no Python, no RedShift, no Hadoop | 100% Scala, Flink replaces Hadoop, PostgreSQL replaces Redshift |

**Dataset:** 7 days of sales data across 20 salesmen and 19 Brazilian cities

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   PostgreSQL (CDC)     CSV (filesystem)     SOAP WS-*           │
│   sales + salesmen     daily exports        product catalog      │
└──────────┬──────────────────┬───────────────────┬───────────────┘
           │                  │                   │
┌──────────▼──────────────────▼───────────────────▼───────────────┐
│                      INGESTION LAYER                            │
│   Debezium (CDC)    Scala File Producer    Scala SOAP Client    │
│                  └──────────────┴───────────────┘               │
│                          Apache Kafka                           │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│              PROCESSING LAYER — Apache Flink 1.18               │
│   Pipeline A: Top Sales per City    (batch)                     │
│   Pipeline B: Top Salesman in the Country    (batch)            │
│                    OpenLineage → Marquez                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                       OUTPUT LAYER                              │
│   PostgreSQL (aggregated results)   Scala REST API              │
│   GET /top-sales-city               GET /top-salesman           │
└─────────────────────────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│               OBSERVABILITY & LINEAGE                           │
│              Prometheus · Grafana · Marquez                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Scala 2.12 |
| Messaging | Apache Kafka 3.6 + Kafka Connect |
| CDC | Debezium 2.6 (PostgreSQL) |
| Processing | Apache Flink 1.18 (DataStream API, batch mode) |
| WS-* Source | Scala SOAP server + client (Java HttpServer + scala-xml) |
| Data Lineage | OpenLineage + Marquez |
| Observability | Prometheus + Grafana |
| Source DB | PostgreSQL 15 |
| Output DB | PostgreSQL 15 |
| API | Scala HTTP API (Java HttpServer + Prometheus Java client) |
| Build | SBT + sbt-assembly (fat JAR) |
| Infra | Docker Compose |
| ❌ Not used | ~~Python~~, ~~RedShift~~, ~~Hadoop~~ |

---

## Data Sources

| # | Type | Description | Kafka Topic |
|---|---|---|---|
| 1 | **Relational DB** | PostgreSQL — sales transactions + salesmen (CDC via Debezium) | `sales.relational` |
| 2 | **File System** | Daily CSV exports — additional sales channel | `sales.filesystem` |
| 3 | **WS-\*** | SOAP service — product catalog and salesman registry | `catalog.products`, `catalog.salesmen` |

---

## Pipelines

### Pipeline A — Top Sales per City
Reads all Kafka sales messages (bounded batch), aggregates total sales per city, ranks nationally and writes to PostgreSQL.

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
Loads salesman catalog from `catalog.salesmen`, then processes sales in batch mode, enriches with catalog data, aggregates and ranks nationally.

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
├── build.sbt                             # SBT build (Scala 2.12 + Flink 1.18)
├── Dockerfile                            # JRE image for API, SOAP, ingestion
├── docker-compose.yml
├── src/main/scala/com/datakata/
│   ├── config/AppConfig.scala            # env-based configuration
│   ├── model/Models.scala                # case classes
│   ├── lineage/LineageClient.scala       # OpenLineage HTTP client
│   ├── soap/SoapServer.scala             # WS-* SOAP server (Java HttpServer)
│   ├── datagen/GenerateFiles.scala       # generates daily CSV files
│   ├── ingest/
│   │   ├── RelationalDBProducer.scala    # PostgreSQL → Kafka
│   │   ├── FileSystemProducer.scala      # CSV → Kafka
│   │   └── SoapIngestionProducer.scala   # SOAP WS → Kafka
│   ├── jobs/
│   │   ├── TopSalesCityJob.scala         # Flink batch — Pipeline A
│   │   └── TopSalesmanJob.scala          # Flink batch — Pipeline B
│   └── api/ApiServer.scala               # REST API + /metrics
├── sources/
│   ├── relational/init.sql               # schema + seed data Mar 3-9
│   └── soap_ws/                          # WSDL reference (generated at runtime)
├── output_db/init.sql                    # results schema
├── observability/
│   ├── prometheus.yml
│   └── grafana/
└── scripts/
    └── run_all.sh
```

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- SBT (Scala Build Tool) — `brew install sbt`

### Run

```bash
git clone https://github.com/mateuzor/data-kata.git
cd data-kata
bash scripts/run_all.sh
```

`run_all.sh` will:
1. Build the fat JAR (`sbt assembly`)
2. Start infrastructure (PostgreSQL, Kafka, Marquez, Grafana)
3. Start the SOAP WS-* catalog server
4. Generate CSV data files
5. Start Flink cluster (JobManager + TaskManager)
6. Run 3 ingestion producers in parallel
7. Submit both Flink batch jobs
8. Start the Results API

### Services

| Service | URL |
|---|---|
| Results API | http://localhost:8080 |
| Flink UI | http://localhost:8081 |
| Marquez (Lineage UI) | http://localhost:3000 |
| Grafana | http://localhost:3001 — `admin / admin` |
| Prometheus | http://localhost:9090 |
| Kafka Connect | http://localhost:8083 |

### API Endpoints

```bash
curl http://localhost:8080/health
curl http://localhost:8080/top-sales-city
curl http://localhost:8080/top-salesman
curl http://localhost:8080/pipeline-runs
curl http://localhost:8080/metrics
```

---

## Data Lineage

Lineage events are emitted via **OpenLineage** at job start and completion.
Visualize the full dataset graph at **Marquez Web UI → http://localhost:3000**

```
kafka.sales.relational ──┐
                          ├─► TopSalesCityJob ─► postgres.top_sales_per_city
kafka.sales.filesystem ──┘

kafka.sales.relational ──┐
kafka.sales.filesystem ──┼─► TopSalesmanJob ─► postgres.top_salesman
kafka.catalog.salesmen ──┘
```
