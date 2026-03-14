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
 * Flink batch job — Pipeline B: Top Salesman in the Country.
 *
 * Reads sales from Kafka (bounded batch), enriches with salesman catalog
 * (also from Kafka — sourced via SOAP WS-* ingestion), aggregates and ranks.
 *
 * Replaces the former Spark Structured Streaming job.
 */
object TopSalesmanJob {

  private val log = LoggerFactory.getLogger(getClass)

  case class SalesmanAgg(
    salesmanId:   Int,
    salesmanName: String,
    city:         String,
    state:        String,
    region:       String,
    totalAmount:  Double,
    totalOrders:  Long
  )

  case class CatalogEntry(id: Int, city: String, state: String, region: String)

  def main(args: Array[String]): Unit = {
    val config  = AppConfig()
    val runId   = UUID.randomUUID().toString
    val lineage = new LineageClient(config)

    lineage.emitStart(
      runId, "TopSalesmanJob",
      inputs  = Seq("kafka://sales.relational", "kafka://sales.filesystem", "kafka://catalog.salesmen"),
      outputs = Seq("postgres://sales_results/top_salesman")
    )

    try {
      // ── Step 1: load salesman catalog from Kafka (bounded) ────────────────
      val catalog = loadCatalog(config)
      log.info(s"Loaded ${catalog.size} salesmen from catalog topic")

      // ── Step 2: batch-process sales ───────────────────────────────────────
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setRuntimeMode(RuntimeExecutionMode.BATCH)
      env.setParallelism(1)

      val kafkaSource = KafkaSource.builder[String]()
        .setBootstrapServers(config.kafkaServers)
        .setTopics("sales.relational", "sales.filesystem")
        .setGroupId("flink-batch-top-salesman")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build()

      val aggStream = env
        .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-sales")
        .flatMap { json =>
          val sid    = extractInt(json, "salesman_id")
          val sname  = extractStr(json, "salesman_name")
          val amount = extractDbl(json, "total_amount")
          if (sid <= 0 || amount <= 0) return None

          // Enrich with catalog when available
          val cat    = catalog.getOrElse(sid, CatalogEntry(sid, extractStr(json, "city"),
                                                                extractStr(json, "state"),
                                                                extractStr(json, "region")))
          Some(SalesmanAgg(sid, sname, cat.city, cat.state, cat.region, amount, 1L))
        }
        .keyBy(_.salesmanId)
        .reduce((a, b) => a.copy(totalAmount = a.totalAmount + b.totalAmount,
                                  totalOrders  = a.totalOrders  + b.totalOrders))

      val iter    = aggStream.executeAndCollect()
      val results = iter.asScala.toSeq
      iter.close()

      val ranked = results
        .sortBy(-_.totalAmount)
        .zipWithIndex
        .map { case (r, i) => r.copy() -> (i + 1) }

      writeToPg(ranked, config)
      lineage.emitComplete(runId, "TopSalesmanJob", ranked.size)
      log.info(s"TopSalesmanJob complete — ${ranked.size} salesmen written")

    } catch {
      case e: Exception =>
        lineage.emitFail(runId, "TopSalesmanJob", e.getMessage)
        log.error("TopSalesmanJob failed", e)
        throw e
    }
  }

  // ── catalog loader (bounded Kafka read) ───────────────────────────────────

  private def loadCatalog(config: AppConfig): Map[Int, CatalogEntry] = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    env.setParallelism(1)

    val src = KafkaSource.builder[String]()
      .setBootstrapServers(config.kafkaServers)
      .setTopics("catalog.salesmen")
      .setGroupId("flink-catalog-loader")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setBounded(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val iter = env
      .fromSource(src, WatermarkStrategy.noWatermarks(), "kafka-catalog")
      .flatMap { json =>
        val id     = extractInt(json, "salesman_id")
        val city   = extractStr(json, "city")
        val state  = extractStr(json, "state")
        val region = extractStr(json, "region")
        if (id > 0) Some(CatalogEntry(id, city, state, region)) else None
      }
      .executeAndCollect()

    val m = iter.asScala.map(e => e.id -> e).toMap
    iter.close()
    m
  }

  // ── JDBC write ────────────────────────────────────────────────────────────

  private def writeToPg(rows: Seq[(SalesmanAgg, Int)], config: AppConfig): Unit = {
    val sql =
      """INSERT INTO top_salesman
        |  (salesman_id, salesman_name, city, state, region,
        |   total_amount, total_orders, rank_position,
        |   period_start, period_end, processed_at)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |ON CONFLICT (salesman_id, period_start, period_end)
        |DO UPDATE SET total_amount  = EXCLUDED.total_amount,
        |              total_orders  = EXCLUDED.total_orders,
        |              rank_position = EXCLUDED.rank_position,
        |              processed_at  = EXCLUDED.processed_at""".stripMargin

    val conn = DriverManager.getConnection(config.outputDbUrl, config.outputDbUser, config.outputDbPassword)
    try {
      val stmt = conn.prepareStatement(sql)
      val now  = Timestamp.from(Instant.now())
      rows.foreach { case (r, rank) =>
        stmt.setInt(1, r.salesmanId)
        stmt.setString(2, r.salesmanName)
        stmt.setString(3, r.city)
        stmt.setString(4, r.state)
        stmt.setString(5, r.region)
        stmt.setDouble(6, r.totalAmount)
        stmt.setLong(7, r.totalOrders)
        stmt.setInt(8, rank)
        stmt.setString(9, config.dataStartDate)
        stmt.setString(10, config.dataEndDate)
        stmt.setTimestamp(11, now)
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

  private def extractInt(json: String, key: String): Int = {
    val pattern = s""""$key"\\s*:\\s*(\\d+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(0)
  }
}
