package com.datakata.jobs

import com.datakata.config.AppConfig
import com.datakata.lineage.LineageClient
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import java.sql.{DriverManager, Timestamp}
import java.time.Instant
import java.util.UUID
import scala.collection.JavaConverters._

/**
 * Flink batch job — Pipeline A: Top Sales per City.
 *
 * Reads all messages from Kafka topics (bounded), aggregates total sales per city,
 * ranks nationally and writes results to PostgreSQL.
 *
 * Replaces the former Spark Structured Streaming job.
 */
object TopSalesCityJob {

  private val log = LoggerFactory.getLogger(getClass)

  case class SaleRow(city: String, state: String, totalAmount: Double)
  case class CityAgg(city: String, state: String, totalAmount: Double, totalOrders: Long)

  def main(args: Array[String]): Unit = {
    val config  = AppConfig()
    val runId   = UUID.randomUUID().toString
    val lineage = new LineageClient(config)

    lineage.emitStart(
      runId, "TopSalesCityJob",
      inputs  = Seq("kafka://sales.relational", "kafka://sales.filesystem"),
      outputs = Seq("postgres://sales_results/top_sales_per_city")
    )

    try {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setRuntimeMode(RuntimeExecutionMode.BATCH)
      env.setParallelism(1)

      val kafkaSource = KafkaSource.builder[String]()
        .setBootstrapServers(config.kafkaServers)
        .setTopics("sales.relational", "sales.filesystem")
        .setGroupId("flink-batch-top-sales-city")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

      // Aggregate: keyBy (city, state) → reduce totals
      val aggStream = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-sales")
        .flatMap { json =>
          val city   = extractStr(json, "city")
          val state  = extractStr(json, "state")
          val amount = extractDbl(json, "total_amount")
          if (city.nonEmpty && amount > 0) Some(CityAgg(city, state, amount, 1L)) else None
        }
        .keyBy(r => s"${r.city}|${r.state}")
        .reduce((a, b) => a.copy(totalAmount = a.totalAmount + b.totalAmount,
                                  totalOrders  = a.totalOrders  + b.totalOrders))

      // Collect, sort, rank
      val iter    = aggStream.executeAndCollect()
      val results = iter.asScala.toSeq
      iter.close()

      val ranked = results
        .sortBy(-_.totalAmount)
        .zipWithIndex
        .map { case (r, i) => (r.city, r.state, r.totalAmount, r.totalOrders, i + 1) }

      writeToPg(ranked, config)
      lineage.emitComplete(runId, "TopSalesCityJob", ranked.size)
      log.info(s"TopSalesCityJob complete — ${ranked.size} cities written")

    } catch {
      case e: Exception =>
        lineage.emitFail(runId, "TopSalesCityJob", e.getMessage)
        log.error("TopSalesCityJob failed", e)
        throw e
    }
  }

  // ── JDBC write ────────────────────────────────────────────────────────────

  private def writeToPg(
    rows:   Seq[(String, String, Double, Long, Int)],
    config: AppConfig
  ): Unit = {
    val sql =
      """INSERT INTO top_sales_per_city
        |  (city, state, total_amount, total_orders, rank_position, period_start, period_end, processed_at)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        |ON CONFLICT (city, state, period_start, period_end)
        |DO UPDATE SET total_amount  = EXCLUDED.total_amount,
        |              total_orders  = EXCLUDED.total_orders,
        |              rank_position = EXCLUDED.rank_position,
        |              processed_at  = EXCLUDED.processed_at""".stripMargin

    val conn = DriverManager.getConnection(config.outputDbUrl, config.outputDbUser, config.outputDbPassword)
    try {
      val stmt = conn.prepareStatement(sql)
      val now  = Timestamp.from(Instant.now())
      rows.foreach { case (city, state, amount, orders, rank) =>
        stmt.setString(1, city)
        stmt.setString(2, state)
        stmt.setDouble(3, amount)
        stmt.setLong(4, orders)
        stmt.setInt(5, rank)
        stmt.setString(6, config.dataStartDate)
        stmt.setString(7, config.dataEndDate)
        stmt.setTimestamp(8, now)
        stmt.addBatch()
      }
      stmt.executeBatch()
      stmt.close()
    } finally {
      conn.close()
    }
  }

  // ── JSON helpers ──────────────────────────────────────────────────────────

  private def extractStr(json: String, key: String): String = {
    val pattern = s""""$key"\\s*:\\s*"([^"]*)"""".r
    pattern.findFirstMatchIn(json).map(_.group(1)).getOrElse("")
  }

  private def extractDbl(json: String, key: String): Double = {
    val pattern = s""""$key"\\s*:\\s*([\\d.]+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toDouble).getOrElse(0.0)
  }
}
