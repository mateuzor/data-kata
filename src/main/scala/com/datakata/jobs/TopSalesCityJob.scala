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
 * Spark Structured Streaming job — Pipeline A: Top Sales per City.
 *
 * Sources  : Kafka topics `sales.relational` and `sales.filesystem`
 * Output   : PostgreSQL table `top_sales_per_city` (overwrite each micro-batch)
 * Lineage  : OpenLineage events → Marquez
 *
 * Replaces processing/pipeline_top_sales_city.py
 */
object TopSalesCityJob {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()
  private val runId  = UUID.randomUUID().toString

  private val saleSchema = StructType(Seq(
    StructField("sale_id",       IntegerType,  nullable = true),
    StructField("sale_date",     StringType,   nullable = true),
    StructField("salesman_id",   IntegerType,  nullable = true),
    StructField("salesman_name", StringType,   nullable = true),
    StructField("product_id",    IntegerType,  nullable = true),
    StructField("product_name",  StringType,   nullable = true),
    StructField("category",      StringType,   nullable = true),
    StructField("city",          StringType,   nullable = true),
    StructField("state",         StringType,   nullable = true),
    StructField("region",        StringType,   nullable = true),
    StructField("quantity",      IntegerType,  nullable = true),
    StructField("unit_price",    DoubleType,   nullable = true),
    StructField("total_amount",  DoubleType,   nullable = true),
    StructField("source",        StringType,   nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val lineage = new LineageClient(config)

    val spark = SparkSession.builder()
      .appName("TopSalesCityJob")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    log.info(s"TopSalesCityJob starting — runId=$runId")

    lineage.emitStart(
      runId, "TopSalesCityJob",
      inputs  = Seq("kafka://sales.relational", "kafka://sales.filesystem"),
      outputs = Seq("postgres://sales_results/top_sales_per_city")
    )

    try {
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
        .filter(col("city").isNotNull && col("total_amount").isNotNull)

      val query = salesStream.writeStream
        .foreachBatch { (batch: DataFrame, batchId: Long) =>
          if (!batch.isEmpty) {
            log.info(s"Batch $batchId — ${batch.count()} rows")

            val byCity = batch
              .groupBy("city", "state")
              .agg(
                sum("total_amount").as("total_amount"),
                count("*").as("total_orders")
              )

            val ranked = byCity
              .withColumn("rank_position",
                rank().over(Window.orderBy(col("total_amount").desc)))
              .withColumn("period_start",  lit(config.dataStartDate))
              .withColumn("period_end",    lit(config.dataEndDate))
              .withColumn("processed_at", current_timestamp())

            ranked.write
              .format("jdbc")
              .option("url",      config.outputDbUrl)
              .option("dbtable",  "top_sales_per_city")
              .option("user",     config.outputDbUser)
              .option("password", config.outputDbPassword)
              .option("driver",   "org.postgresql.Driver")
              .mode("overwrite")
              .save()

            lineage.emitComplete(runId, "TopSalesCityJob", ranked.count())
            log.info(s"Batch $batchId written — ${ranked.count()} cities ranked")
          }
        }
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", "/opt/spark-apps/checkpoints/top_sales_city")
        .start()

      query.awaitTermination()

    } catch {
      case e: Exception =>
        lineage.emitFail(runId, "TopSalesCityJob", e.getMessage)
        log.error("TopSalesCityJob failed", e)
        throw e
    }
  }
}
