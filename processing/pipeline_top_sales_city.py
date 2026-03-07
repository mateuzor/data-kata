"""
Pipeline A - Top Sales per City
Consumes sales data from Kafka (3 sources), aggregates by city,
and writes ranked results to the output PostgreSQL database.

Data Lineage emitted via OpenLineage to Marquez.
"""

import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.facet import (
    SqlJobFacet, SchemaDatasetFacet, SchemaField,
    DataSourceDatasetFacet, DocumentationJobFacet
)
from openlineage.client.dataset import Dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS           = "sales.relational,sales.filesystem"
OUTPUT_DB_URL    = (
    f"jdbc:postgresql://{os.getenv('OUTPUT_DB_HOST','output-db')}:"
    f"{os.getenv('OUTPUT_DB_PORT','5432')}/"
    f"{os.getenv('OUTPUT_DB_NAME','sales_results')}"
)
OUTPUT_DB_PROPS  = {
    "user":     os.getenv("OUTPUT_DB_USER",     "results_user"),
    "password": os.getenv("OUTPUT_DB_PASSWORD", "results_pass"),
    "driver":   "org.postgresql.Driver",
}
OPENLINEAGE_URL  = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
NAMESPACE        = os.getenv("OPENLINEAGE_NAMESPACE", "data-kata")
PERIOD_START     = os.getenv("DATA_START_DATE", "2026-03-03")
PERIOD_END       = os.getenv("DATA_END_DATE",   "2026-03-09")
CHECKPOINT_DIR   = "/opt/spark-apps/checkpoints/top_sales_city"

# ─────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────

SALES_SCHEMA = StructType([
    StructField("source",       StringType(),  True),
    StructField("sale_date",    StringType(),  True),
    StructField("salesman_id",  IntegerType(), True),
    StructField("salesman_name",StringType(),  True),
    StructField("product_id",   IntegerType(), True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("city",         StringType(),  True),
    StructField("state",        StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("total_amount", DoubleType(),  True),
])


# ─────────────────────────────────────────────
# OpenLineage helpers
# ─────────────────────────────────────────────

def emit_lineage(run_id: str, state: RunState, output_rows: int = 0):
    try:
        client = OpenLineageClient(url=OPENLINEAGE_URL)

        input_datasets = [
            Dataset(
                namespace=NAMESPACE,
                name="kafka.sales.relational",
                facets={
                    "schema": SchemaDatasetFacet(fields=[
                        SchemaField("city", "STRING"),
                        SchemaField("state", "STRING"),
                        SchemaField("total_amount", "DOUBLE"),
                    ]),
                    "dataSource": DataSourceDatasetFacet(
                        name="kafka", uri=f"kafka://{KAFKA_BOOTSTRAP}/sales.relational"
                    ),
                },
            ),
            Dataset(
                namespace=NAMESPACE,
                name="kafka.sales.filesystem",
                facets={
                    "dataSource": DataSourceDatasetFacet(
                        name="kafka", uri=f"kafka://{KAFKA_BOOTSTRAP}/sales.filesystem"
                    ),
                },
            ),
        ]

        output_dataset = Dataset(
            namespace=NAMESPACE,
            name="postgres.sales_results.top_sales_per_city",
            facets={
                "schema": SchemaDatasetFacet(fields=[
                    SchemaField("city",          "STRING"),
                    SchemaField("state",         "STRING"),
                    SchemaField("total_amount",  "DOUBLE"),
                    SchemaField("total_orders",  "INTEGER"),
                    SchemaField("rank_position", "INTEGER"),
                ]),
                "dataSource": DataSourceDatasetFacet(
                    name="postgresql", uri=OUTPUT_DB_URL
                ),
            },
        )

        event = RunEvent(
            eventType=state,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(
                namespace=NAMESPACE,
                name="pipeline_top_sales_per_city",
                facets={
                    "documentation": DocumentationJobFacet(
                        description="Aggregates total sales per city and ranks results."
                    ),
                },
            ),
            inputs=input_datasets,
            outputs=[output_dataset],
            producer="https://github.com/data-kata",
        )
        client.emit(event)
        logger.info(f"OpenLineage event emitted: {state.value}")
    except Exception as e:
        logger.warning(f"OpenLineage emit failed: {e}")


# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TopSalesPerCity")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Pipeline logic
# ─────────────────────────────────────────────

def read_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), SALES_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(F.col("city").isNotNull())
    )


def aggregate_by_city(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("city", "state")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_amount"),
            F.count("*").alias("total_orders"),
        )
        .withColumn(
            "rank_position",
            F.rank().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.orderBy(F.col("total_amount").desc())
            ),
        )
        .withColumn("period_start", F.lit(PERIOD_START))
        .withColumn("period_end",   F.lit(PERIOD_END))
        .withColumn("pipeline",     F.lit("top_sales_per_city"))
        .withColumn("processed_at", F.current_timestamp())
    )


def write_batch(df: DataFrame, epoch_id: int):
    """Write each micro-batch to PostgreSQL."""
    if df.isEmpty():
        return

    agg = aggregate_by_city(df)

    (
        agg.write
        .format("jdbc")
        .option("url", OUTPUT_DB_URL)
        .option("dbtable", "top_sales_per_city")
        .option("user",     OUTPUT_DB_PROPS["user"])
        .option("password", OUTPUT_DB_PROPS["password"])
        .option("driver",   OUTPUT_DB_PROPS["driver"])
        .mode("overwrite")
        .save()
    )
    logger.info(f"[epoch {epoch_id}] Wrote {agg.count()} city aggregations to output DB.")


def main():
    import uuid
    run_id = str(uuid.uuid4())

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    emit_lineage(run_id, RunState.START)

    try:
        raw_df = read_from_kafka(spark)

        query = (
            raw_df.writeStream
            .foreachBatch(write_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info("Pipeline A (Top Sales per City) is running...")
        query.awaitTermination()
        emit_lineage(run_id, RunState.COMPLETE)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        emit_lineage(run_id, RunState.FAIL)
        raise


if __name__ == "__main__":
    main()
