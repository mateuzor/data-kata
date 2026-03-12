package com.datakata.jobs

import com.datakata.config.AppConfig
import com.datakata.lineage.LineageClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util.UUID

/**
 * Spark Structured Streaming job — Pipeline B: Top Salesman in the Country.
 *
 * Sources  : Kafka topics `sales.relational`, `sales.filesystem`, `catalog.salesmen`
 * Output   : PostgreSQL table `top_salesman` (overwrite each micro-batch)
 * Lineage  : OpenLineage events → Marquez
 *
 * Replaces processing/pipeline_top_salesman.py
 */
object TopSalesmanJob {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()
  private val runId  = UUID.randomUUID().toString

  private val saleSchema = StructType(Seq(
    StructField("sale_id",       IntegerType, nullable = true),
    StructField("sale_date",     StringType,  nullable = true),
    StructField("salesman_id",   IntegerType, nullable = true),
    StructField("salesman_name", StringType,  nullable = true),
    StructField("product_id",    IntegerType, nullable = true),
    StructField("product_name",  StringType,  nullable = true),
    StructField("category",      StringType,  nullable = true),
    StructField("city",          StringType,  nullable = true),
    StructField("state",         StringType,  nullable = true),
    StructField("region",        StringType,  nullable = true),
    StructField("quantity",      IntegerType, nullable = true),
    StructField("unit_price",    DoubleType,  nullable = true),
    StructField("total_amount",  DoubleType,  nullable = true),
    StructField("source",        StringType,  nullable = true)
  ))

  private val salesmanSchema = StructType(Seq(
    StructField("salesman_id", IntegerType, nullable = true),
    StructField("name",        StringType,  nullable = true),
    StructField("city",        StringType,  nullable = true),
    StructField("state",       StringType,  nullable = true),
    StructField("region",      StringType,  nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val lineage = new LineageClient(config)

    val spark = SparkSession.builder()
      .appName("TopSalesmanJob")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    log.info(s"TopSalesmanJob starting — runId=$runId")

    lineage.emitStart(
      runId, "TopSalesmanJob",
      inputs  = Seq("kafka://sales.relational", "kafka://sales.filesystem", "kafka://catalog.salesmen"),
      outputs = Seq("postgres://sales_results/top_salesman")
    )

    try {
      // ── static salesman catalog (batch read from Kafka) ──────────────────
      val catalogDf = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafkaServers)
        .option("subscribe", "catalog.salesmen")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), salesmanSchema).as("d"))
        .select(
          col("d.salesman_id").as("cat_id"),
          col("d.city").as("cat_city"),
          col("d.state").as("cat_state"),
          col("d.region").as("cat_region")
        )
        .distinct()

      catalogDf.cache()
      log.info(s"Loaded ${catalogDf.count()} salesmen from catalog topic")

      // ── streaming sales ─────────────────────────────────────────────────
      val kafkaStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafkaServers)
        .option("subscribe", "sales.relational,sales.filesystem")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()

      val salesStream = kafkaStream
        .select(from_json(col("value").cast("string"), saleSchema).as("d"))
        .select("d.*")
        .filter(col("salesman_id").isNotNull && col("total_amount").isNotNull)

      val query = salesStream.writeStream
        .foreachBatch { (batch: DataFrame, batchId: Long) =>
          if (!batch.isEmpty) {
            log.info(s"Batch $batchId — ${batch.count()} rows")

            // Enrich with catalog region/location when available
            val enriched = batch
              .join(catalogDf, batch("salesman_id") === catalogDf("cat_id"), "left")
              .withColumn("eff_city",   coalesce(col("cat_city"),   col("city")))
              .withColumn("eff_state",  coalesce(col("cat_state"),  col("state")))
              .withColumn("eff_region", coalesce(col("cat_region"), col("region")))

            val bySalesman = enriched
              .groupBy("salesman_id", "salesman_name", "eff_city", "eff_state", "eff_region")
              .agg(
                sum("total_amount").as("total_amount"),
                count("*").as("total_orders")
              )
              .withColumnRenamed("eff_city",   "city")
              .withColumnRenamed("eff_state",  "state")
              .withColumnRenamed("eff_region", "region")

            val ranked = bySalesman
              .withColumn("rank_position",
                rank().over(Window.orderBy(col("total_amount").desc)))
              .withColumn("period_start",  lit(config.dataStartDate))
              .withColumn("period_end",    lit(config.dataEndDate))
              .withColumn("processed_at", current_timestamp())

            ranked.write
              .format("jdbc")
              .option("url",      config.outputDbUrl)
              .option("dbtable",  "top_salesman")
              .option("user",     config.outputDbUser)
              .option("password", config.outputDbPassword)
              .option("driver",   "org.postgresql.Driver")
              .mode("overwrite")
              .save()

            lineage.emitComplete(runId, "TopSalesmanJob", ranked.count())
            log.info(s"Batch $batchId written — ${ranked.count()} salesmen ranked")
          }
        }
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/top_salesman")
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception =>
        lineage.emitFail(runId, "TopSalesmanJob", e.getMessage)
        log.error("TopSalesmanJob failed", e)
        throw e
    }
  }
}
