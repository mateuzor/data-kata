package com.datakata.ingest

import com.datakata.config.AppConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import java.util.Properties

/**
 * Reads sales data from the source PostgreSQL database (JOIN sales + salesmen + products)
 * and publishes each row as a JSON message to the `sales.relational` Kafka topic.
 *
 * Replaces ingestion/kafka_producer_db.py
 */
object RelationalDBProducer {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()
  private val TOPIC  = "sales.relational"

  private val QUERY =
    """SELECT s.id          AS sale_id,
      |       s.sale_date,
      |       s.salesman_id,
      |       sm.name       AS salesman_name,
      |       s.product_id,
      |       p.name        AS product_name,
      |       p.category,
      |       s.city,
      |       s.state,
      |       s.region,
      |       s.quantity,
      |       s.unit_price,
      |       s.total_amount
      |FROM   sales s
      |JOIN   salesmen sm ON s.salesman_id = sm.id
      |JOIN   products  p  ON s.product_id  = p.id
      |WHERE  s.sale_date BETWEEN ? AND ?
      |ORDER  BY s.id""".stripMargin

  def main(args: Array[String]): Unit = {
    log.info(s"Starting relational DB ingestion → $TOPIC")

    val producer = buildProducer()
    var count    = 0

    val conn = DriverManager.getConnection(
      config.sourceDbUrl, config.sourceDbUser, config.sourceDbPassword)

    try {
      val stmt = conn.prepareStatement(QUERY)
      stmt.setString(1, config.dataStartDate)
      stmt.setString(2, config.dataEndDate)
      val rs = stmt.executeQuery()

      while (rs.next()) {
        val json = rowToJson(rs)
        val key  = rs.getString("sale_id")
        producer.send(new ProducerRecord[String, String](TOPIC, key, json))
        count += 1
        if (count % 100 == 0) log.info(s"Published $count records…")
      }
      rs.close()
      stmt.close()
    } finally {
      conn.close()
      producer.flush()
      producer.close()
    }

    log.info(s"Relational ingestion complete — $count records sent to $TOPIC")
  }

  private def rowToJson(rs: java.sql.ResultSet): String = {
    def esc(s: String) = if (s == null) "null" else "\"" + s.replace("\"", "\\\"") + "\""
    def num(v: Any)    = if (v == null) "null" else v.toString

    s"""{
       |"sale_id":${num(rs.getObject("sale_id"))},
       |"sale_date":${esc(rs.getString("sale_date"))},
       |"salesman_id":${num(rs.getObject("salesman_id"))},
       |"salesman_name":${esc(rs.getString("salesman_name"))},
       |"product_id":${num(rs.getObject("product_id"))},
       |"product_name":${esc(rs.getString("product_name"))},
       |"category":${esc(rs.getString("category"))},
       |"city":${esc(rs.getString("city"))},
       |"state":${esc(rs.getString("state"))},
       |"region":${esc(rs.getString("region"))},
       |"quantity":${num(rs.getObject("quantity"))},
       |"unit_price":${num(rs.getObject("unit_price"))},
       |"total_amount":${num(rs.getObject("total_amount"))},
       |"source":"relational"
       |}""".stripMargin.replaceAll("\n", "")
  }

  private def buildProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers",  config.kafkaServers)
    props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")
    new KafkaProducer[String, String](props)
  }
}
