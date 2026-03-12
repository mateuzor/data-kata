package com.datakata.api

import com.datakata.config.AppConfig
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.prometheus.client.{Counter, Histogram}
import io.prometheus.client.exporter.common.TextFormat
import org.slf4j.LoggerFactory

import java.io.{StringWriter, OutputStream}
import java.net.InetSocketAddress
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * Lightweight REST API exposing pipeline results.
 *
 * Endpoints:
 *   GET /health
 *   GET /metrics          — Prometheus text exposition
 *   GET /top-sales-city   — Top sales per city (Pipeline A)
 *   GET /top-salesman     — Top salesman country-wide (Pipeline B)
 *   GET /pipeline-runs    — Execution history
 *
 * Replaces api/main.py (FastAPI / Python).
 */
object ApiServer {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()

  // ── Prometheus metrics ────────────────────────────────────────────────────

  private val requestsTotal: Counter = Counter.build()
    .name("http_requests_total")
    .help("Total HTTP requests")
    .labelNames("method", "endpoint", "status")
    .register()

  private val requestDuration: Histogram = Histogram.build()
    .name("http_request_duration_seconds")
    .help("HTTP request duration in seconds")
    .labelNames("endpoint")
    .buckets(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
    .register()

  // ── query definitions ─────────────────────────────────────────────────────

  private val QUERY_TOP_CITY =
    """SELECT city, state, total_amount, total_orders, rank_position,
      |       period_start::text, period_end::text, processed_at::text
      |FROM   top_sales_per_city
      |ORDER  BY rank_position LIMIT 50""".stripMargin

  private val QUERY_TOP_SALESMAN =
    """SELECT salesman_id, salesman_name, city, state, region,
      |       total_amount, total_orders, rank_position,
      |       period_start::text, period_end::text, processed_at::text
      |FROM   top_salesman
      |ORDER  BY rank_position LIMIT 50""".stripMargin

  private val QUERY_PIPELINE_RUNS =
    """SELECT pipeline_name, status, started_at::text, finished_at::text,
      |       rows_processed, error_message
      |FROM   pipeline_runs
      |ORDER  BY started_at DESC LIMIT 20""".stripMargin

  // ── HTTP handlers ─────────────────────────────────────────────────────────

  private def handle(exchange: HttpExchange, endpoint: String,
                     fn: => (Int, String)): Unit = {
    val t0     = System.nanoTime()
    val timer  = requestDuration.labels(endpoint).startTimer()
    var status = 500
    try {
      val (s, body) = fn
      status = s
      val bytes = body.getBytes("UTF-8")
      exchange.getResponseHeaders.set("Content-Type", "application/json; charset=UTF-8")
      exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
      exchange.sendResponseHeaders(s, bytes.length)
      val out: OutputStream = exchange.getResponseBody
      out.write(bytes)
      out.close()
    } catch {
      case e: Exception =>
        log.error(s"Handler error for $endpoint: ${e.getMessage}", e)
        val msg = s"""{"error":"${e.getMessage.replace("\"","'")}"}"""
        val bytes = msg.getBytes("UTF-8")
        exchange.getResponseHeaders.set("Content-Type", "application/json; charset=UTF-8")
        exchange.sendResponseHeaders(500, bytes.length)
        val out: OutputStream = exchange.getResponseBody
        out.write(bytes)
        out.close()
    } finally {
      timer.observeDuration()
      requestsTotal.labels(exchange.getRequestMethod, endpoint, status.toString).inc()
    }
  }

  private def healthHandler = new HttpHandler {
    override def handle(ex: HttpExchange): Unit =
      ApiServer.handle(ex, "/health", {
        (200, """{"status":"ok","service":"data-kata-api","version":"1.0.0"}""")
      })
  }

  private def metricsHandler = new HttpHandler {
    override def handle(ex: HttpExchange): Unit = {
      val writer = new StringWriter()
      TextFormat.write004(writer, io.prometheus.client.CollectorRegistry.defaultRegistry.metricFamilySamples())
      val bytes = writer.toString.getBytes("UTF-8")
      ex.getResponseHeaders.set("Content-Type", TextFormat.CONTENT_TYPE_004)
      ex.sendResponseHeaders(200, bytes.length)
      val out: OutputStream = ex.getResponseBody
      out.write(bytes)
      out.close()
    }
  }

  private def queryHandler(endpoint: String, sql: String) = new HttpHandler {
    override def handle(ex: HttpExchange): Unit =
      ApiServer.handle(ex, endpoint, {
        val conn = getConnection
        try {
          val rs   = conn.createStatement().executeQuery(sql)
          val json = resultSetToJson(rs)
          rs.close()
          (200, json)
        } finally {
          conn.close()
        }
      })
  }

  // ── JSON serialisation ────────────────────────────────────────────────────

  private def resultSetToJson(rs: ResultSet): String = {
    val meta  = rs.getMetaData
    val cols  = (1 to meta.getColumnCount).map(meta.getColumnName)
    val rows  = scala.collection.mutable.ArrayBuffer[String]()

    while (rs.next()) {
      val fields = cols.map { col =>
        val raw = rs.getObject(col)
        val value = raw match {
          case null                           => "null"
          case n: java.lang.Number            => n.toString
          case b: java.lang.Boolean           => b.toString
          case s                              => "\"" + s.toString.replace("\\", "\\\\")
                                                              .replace("\"", "\\\"") + "\""
        }
        s""""$col":$value"""
      }
      rows += "{" + fields.mkString(",") + "}"
    }
    "[" + rows.mkString(",") + "]"
  }

  // ── DB connection ─────────────────────────────────────────────────────────

  private def getConnection: Connection =
    DriverManager.getConnection(config.outputDbUrl, config.outputDbUser, config.outputDbPassword)

  // ── entry point ───────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val server = HttpServer.create(new InetSocketAddress(config.apiPort), 0)

    server.createContext("/health",         healthHandler)
    server.createContext("/metrics",        metricsHandler)
    server.createContext("/top-sales-city", queryHandler("/top-sales-city", QUERY_TOP_CITY))
    server.createContext("/top-salesman",   queryHandler("/top-salesman",   QUERY_TOP_SALESMAN))
    server.createContext("/pipeline-runs",  queryHandler("/pipeline-runs",  QUERY_PIPELINE_RUNS))

    server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4))
    server.start()

    log.info(s"API server started on port ${config.apiPort}")
    log.info(s"  GET http://0.0.0.0:${config.apiPort}/health")
    log.info(s"  GET http://0.0.0.0:${config.apiPort}/top-sales-city")
    log.info(s"  GET http://0.0.0.0:${config.apiPort}/top-salesman")
    log.info(s"  GET http://0.0.0.0:${config.apiPort}/metrics")
  }
}
