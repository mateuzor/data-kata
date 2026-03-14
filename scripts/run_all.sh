#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_all.sh — Full stack startup for Data Kata (Scala + Flink batch edition)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
step()  { echo -e "\n${GREEN}══ $* ${NC}"; }

# ── 0. build fat JAR ─────────────────────────────────────────────────────────
step "Building Scala fat JAR"
command -v sbt &>/dev/null || error "sbt not found — install via 'brew install sbt'"
sbt assembly
JAR="target/scala-2.12/data-kata-assembly.jar"
[[ -f "$JAR" ]] || error "Assembly JAR not found at $JAR"
info "JAR built: $JAR"

# ── 1. infrastructure ─────────────────────────────────────────────────────────
step "Starting infrastructure (DB, Kafka, Zookeeper, Marquez, Grafana)"
docker compose up -d \
  source-db output-db \
  zookeeper kafka kafka-connect \
  marquez marquez-web \
  prometheus grafana

info "Waiting 30s for services to initialise…"
sleep 30

# ── 2. SOAP server ────────────────────────────────────────────────────────────
step "Starting SOAP WS-* catalog server (Scala)"
docker compose up -d soap-server
sleep 5

# ── 3. Generate filesystem data ───────────────────────────────────────────────
step "Generating filesystem CSV sales files"
docker compose run --rm generate-files
info "Files written to ./sources/filesystem/data"

# ── 4. Flink cluster ──────────────────────────────────────────────────────────
step "Starting Flink JobManager + TaskManager"
docker compose up -d flink-jobmanager flink-taskmanager
info "Waiting 15s for Flink cluster to be ready…"
sleep 15

# ── 5. Ingestion (3 sources in parallel) ─────────────────────────────────────
step "Running ingestion producers (DB, Filesystem, SOAP)"
docker compose up -d ingest-relational ingest-filesystem ingest-soap
info "Waiting 15s for ingestion to complete…"
sleep 15

# ── 6. Flink batch pipelines ──────────────────────────────────────────────────
step "Submitting Flink batch jobs"
docker compose up -d flink-pipeline-city flink-pipeline-salesman

# ── 7. Results API ────────────────────────────────────────────────────────────
step "Starting Results API"
docker compose up -d api

# ── 8. Register Debezium CDC connector ───────────────────────────────────────
step "Registering Debezium CDC connector"
sleep 10
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @ingestion/connectors/debezium-postgres.json \
  && info "CDC connector registered" \
  || warn "CDC registration failed (non-critical — batch ingestion already ran)"

# ── done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Data Kata — Scala + Flink Batch — All services up    ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo ""
echo "  Results API      → http://localhost:8080"
echo "    /health          → health check"
echo "    /top-sales-city  → Pipeline A results"
echo "    /top-salesman    → Pipeline B results"
echo "    /metrics         → Prometheus metrics"
echo ""
echo "  Flink UI         → http://localhost:8081"
echo "  Kafka Connect    → http://localhost:8083"
echo "  Marquez Lineage  → http://localhost:3000"
echo "  Prometheus       → http://localhost:9090"
echo "  Grafana          → http://localhost:3001  (admin/admin)"
echo ""
echo "  Source DB  → localhost:5433  (sales_source / sales_user / sales_pass)"
echo "  Output DB  → localhost:5434  (sales_results / results_user / results_pass)"
echo ""
