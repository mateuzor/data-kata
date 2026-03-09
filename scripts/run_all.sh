#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Run all components end-to-end (local dev)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

echo "==> Copying .env.example to .env (if not exists)"
[ -f .env ] || cp .env.example .env

echo "==> Starting infrastructure..."
docker compose up -d source-db output-db zookeeper kafka kafka-connect soap-server marquez marquez-web prometheus grafana

echo "==> Waiting for services to be healthy..."
sleep 30

echo "==> Generating filesystem data files (Mar 3-9, 2026)..."
pip install -q pandas pyarrow
python sources/filesystem/generate_files.py

echo "==> Starting Spark workers..."
docker compose up -d spark-master spark-worker

echo "==> Running Kafka producers (ingestion)..."
export $(cat .env | xargs)
python ingestion/kafka_producer_soap.py &
sleep 5
python ingestion/kafka_producer_db.py &
python ingestion/kafka_producer_files.py &
wait

echo "==> Submitting Spark pipelines..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-apps/processing/pipeline_top_sales_city.py &

docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-apps/processing/pipeline_top_salesman.py &

echo "==> Starting Results API..."
docker compose up -d api

echo ""
echo "============================================================"
echo " Data Kata - All systems running!"
echo "============================================================"
echo " Spark UI:       http://localhost:8090"
echo " Kafka Connect:  http://localhost:8083"
echo " Marquez API:    http://localhost:5000"
echo " Marquez Web:    http://localhost:3000"
echo " Prometheus:     http://localhost:9090"
echo " Grafana:        http://localhost:3001  (admin/admin)"
echo " Results API:    http://localhost:8080/docs"
echo "============================================================"
