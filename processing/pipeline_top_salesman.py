"""
Pipeline B - Top Salesman in the Country
Consumes sales data from all 3 Kafka sources, joins with salesman
catalog from SOAP WS, aggregates total sales per salesman,
and writes ranked results to the output PostgreSQL database.

Data Lineage emitted via OpenLineage to Marquez.
"""

import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.facet import (
    SchemaDatasetFacet, SchemaField, DataSourceDatasetFacet, DocumentationJobFacet
)
from openlineage.client.dataset import Dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SALES_TOPICS     = "sales.relational,sales.filesystem"
CATALOG_TOPIC    = "catalog.salesmen"
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
OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
NAMESPACE       = os.getenv("OPENLINEAGE_NAMESPACE", "data-kata")
PERIOD_START    = os.getenv("DATA_START_DATE", "2026-03-03")
PERIOD_END      = os.getenv("DATA_END_DATE",   "2026-03-09")
CHECKPOINT_DIR  = "/opt/spark-apps/checkpoints/top_salesman"

# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────

SALES_SCHEMA = StructType([
    StructField("source",        StringType(),  True),
    StructField("sale_date",     StringType(),  True),
    StructField("salesman_id",   IntegerType(), True),
    StructField("salesman_name", StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("state",         StringType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("unit_price",    DoubleType(),  True),
    StructField("total_amount",  DoubleType(),  True),
])

SALESMAN_SCHEMA = StructType([
    StructField("source",      StringType(),  True),
    StructField("salesman_id", IntegerType(), True),
    StructField("name",        StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("state",       StringType(),  True),
    StructField("region",      StringType(),  True),
])


# ─────────────────────────────────────────────
# OpenLineage helpers
# ─────────────────────────────────────────────

def emit_lineage(run_id: str, state: RunState):
    try:
        client = OpenLineageClient(url=OPENLINEAGE_URL)
        event = RunEvent(
            eventType=state,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(
                namespace=NAMESPACE,
                name="pipeline_top_salesman",
                facets={
                    "documentation": DocumentationJobFacet(
                        description="Aggregates total sales per salesman nationwide and ranks them."
                    ),
                },
            ),
            inputs=[
                Dataset(
                    namespace=NAMESPACE, name="kafka.sales.relational",
                    facets={"dataSource": DataSourceDatasetFacet(
                        name="kafka", uri=f"kafka://{KAFKA_BOOTSTRAP}/sales.relational"
                    )},
                ),
                Dataset(
                    namespace=NAMESPACE, name="kafka.sales.filesystem",
                    facets={"dataSource": DataSourceDatasetFacet(
                        name="kafka", uri=f"kafka://{KAFKA_BOOTSTRAP}/sales.filesystem"
                    )},
                ),
                Dataset(
                    namespace=NAMESPACE, name="kafka.catalog.salesmen",
                    facets={"dataSource": DataSourceDatasetFacet(
                        name="kafka", uri=f"kafka://{KAFKA_BOOTSTRAP}/catalog.salesmen"
                    )},
                ),
            ],
            outputs=[
                Dataset(
                    namespace=NAMESPACE,
                    name="postgres.sales_results.top_salesman",
                    facets={
                        "schema": SchemaDatasetFacet(fields=[
                            SchemaField("salesman_id",   "INTEGER"),
                            SchemaField("salesman_name", "STRING"),
                            SchemaField("total_amount",  "DOUBLE"),
                            SchemaField("rank_position", "INTEGER"),
                        ]),
                        "dataSource": DataSourceDatasetFacet(
                            name="postgresql", uri=OUTPUT_DB_URL
                        ),
                    },
                )
            ],
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
        .appName("TopSalesmanCountry")
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Read static salesman catalog (from Kafka, compacted)
# ─────────────────────────────────────────────

def read_salesman_catalog(spark: SparkSession) -> DataFrame:
    """Read salesman catalog as a static batch from Kafka."""
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CATALOG_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), SALESMAN_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(F.col("salesman_id").isNotNull())
        .dropDuplicates(["salesman_id"])
    )


def read_sales_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SALES_TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), SALES_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(F.col("salesman_id").isNotNull())
    )


# ─────────────────────────────────────────────
# Aggregation
# ─────────────────────────────────────────────

def write_batch(salesman_catalog_df: DataFrame):
    """Returns a foreachBatch writer function with catalog joined."""

    def _write(df: DataFrame, epoch_id: int):
        if df.isEmpty():
            return

        # Join sales with salesman catalog (from SOAP WS)
        enriched = df.join(
            salesman_catalog_df.select(
                F.col("salesman_id").alias("cat_salesman_id"),
                F.col("name").alias("salesman_name_catalog"),
                F.col("city").alias("salesman_city"),
                F.col("state").alias("salesman_state"),
                F.col("region"),
            ),
            df.salesman_id == F.col("cat_salesman_id"),
            how="left",
        )

        name_col = F.coalesce(
            F.col("salesman_name_catalog"),
            F.col("salesman_name"),
        )

        agg = (
            enriched
            .groupBy(
                "salesman_id",
                name_col.alias("salesman_name"),
                F.coalesce(F.col("salesman_city"), F.col("city")).alias("city"),
                F.coalesce(F.col("salesman_state"), F.col("state")).alias("state"),
                F.coalesce(F.col("region"), F.lit("N/A")).alias("region"),
            )
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_amount"),
                F.count("*").alias("total_orders"),
            )
            .withColumn(
                "rank_position",
                F.rank().over(Window.orderBy(F.col("total_amount").desc())),
            )
            .withColumn("period_start", F.lit(PERIOD_START))
            .withColumn("period_end",   F.lit(PERIOD_END))
            .withColumn("pipeline",     F.lit("top_salesman"))
            .withColumn("processed_at", F.current_timestamp())
        )

        (
            agg.write
            .format("jdbc")
            .option("url",      OUTPUT_DB_URL)
            .option("dbtable",  "top_salesman")
            .option("user",     OUTPUT_DB_PROPS["user"])
            .option("password", OUTPUT_DB_PROPS["password"])
            .option("driver",   OUTPUT_DB_PROPS["driver"])
            .mode("overwrite")
            .save()
        )
        logger.info(f"[epoch {epoch_id}] Wrote {agg.count()} salesman rankings to output DB.")

    return _write


def main():
    import uuid
    run_id = str(uuid.uuid4())

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    emit_lineage(run_id, RunState.START)

    try:
        # Read salesman catalog (SOAP WS → Kafka → Spark batch read)
        catalog_df = read_salesman_catalog(spark)
        catalog_df.cache()
        logger.info(f"Loaded {catalog_df.count()} salesmen from catalog.")

        # Stream sales and join + aggregate
        sales_df = read_sales_stream(spark)

        query = (
            sales_df.writeStream
            .foreachBatch(write_batch(catalog_df))
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info("Pipeline B (Top Salesman) is running...")
        query.awaitTermination()
        emit_lineage(run_id, RunState.COMPLETE)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        emit_lineage(run_id, RunState.FAIL)
        raise


if __name__ == "__main__":
    main()
