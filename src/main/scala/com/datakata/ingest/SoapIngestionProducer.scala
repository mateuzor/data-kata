package com.datakata.ingest

import com.datakata.config.AppConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.Properties
import scala.xml.XML

/**
 * Calls the SOAP WS-* catalog service (SoapServer) to fetch products and salesmen,
 * then publishes each item to `catalog.products` and `catalog.salesmen` Kafka topics.
 *
 * Replaces ingestion/kafka_producer_soap.py
 */
object SoapIngestionProducer {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()
  private val TNS    = "http://data-kata.br/product-catalog"

  def main(args: Array[String]): Unit = {
    log.info(s"Starting SOAP catalog ingestion")
    val producer = buildProducer()

    val products = fetchProducts()
    products.zipWithIndex.foreach { case (json, i) =>
      producer.send(new ProducerRecord[String, String]("catalog.products", s"p-$i", json))
    }
    log.info(s"Published ${products.size} products → catalog.products")

    val salesmen = fetchSalesmen()
    salesmen.zipWithIndex.foreach { case (json, i) =>
      producer.send(new ProducerRecord[String, String]("catalog.salesmen", s"s-$i", json))
    }
    log.info(s"Published ${salesmen.size} salesmen → catalog.salesmen")

    producer.flush()
    producer.close()
    log.info("SOAP ingestion complete")
  }

  // ── SOAP calls ────────────────────────────────────────────────────────────

  private def fetchProducts(): Seq[String] = {
    val xml = callSoap("GetAllProducts",
      s"""<tns:GetAllProducts xmlns:tns="$TNS"/>""")
    (xml \\ "return").map { n =>
      val esc = (tag: String) => (n \ tag).text.replace("\"", "\\\"")
      s"""{"product_id":${(n \ "product_id").text},"name":"${esc("name")}",""" +
      s""""category":"${esc("category")}","unit_price":${(n \ "unit_price").text},""" +
      s""""is_active":"${esc("is_active")}"}"""
    }
  }

  private def fetchSalesmen(): Seq[String] = {
    val xml = callSoap("GetAllSalesmen",
      s"""<tns:GetAllSalesmen xmlns:tns="$TNS"/>""")
    (xml \\ "return").map { n =>
      val esc = (tag: String) => (n \ tag).text.replace("\"", "\\\"")
      s"""{"salesman_id":${(n \ "salesman_id").text},"name":"${esc("name")}",""" +
      s""""city":"${esc("city")}","state":"${esc("state")}","region":"${esc("region")}"}"""
    }
  }

  private def callSoap(action: String, bodyContent: String,
                        retries: Int = 5): scala.xml.Elem = {
    val soapEnvelope =
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
         |  <soap:Body>$bodyContent</soap:Body>
         |</soap:Envelope>""".stripMargin

    var lastError: Exception = null
    var attempt = 0
    while (attempt < retries) {
      try {
        val url  = new URL(s"${config.soapWsUrl}/")
        val conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setRequestMethod("POST")
        conn.setDoOutput(true)
        conn.setConnectTimeout(5000)
        conn.setReadTimeout(10000)
        conn.setRequestProperty("Content-Type", "text/xml; charset=UTF-8")
        conn.setRequestProperty("SOAPAction", action)

        val out = conn.getOutputStream
        out.write(soapEnvelope.getBytes("UTF-8"))
        out.close()

        val responseCode = conn.getResponseCode
        if (responseCode != 200) throw new RuntimeException(s"HTTP $responseCode from SOAP server")

        val reader = new BufferedReader(new InputStreamReader(conn.getInputStream, "UTF-8"))
        val sb     = new StringBuilder
        var line   = reader.readLine()
        while (line != null) { sb.append(line); line = reader.readLine() }
        reader.close()

        return XML.loadString(sb.toString())
      } catch {
        case e: Exception =>
          lastError = e
          log.warn(s"SOAP call attempt ${attempt + 1}/$retries failed: ${e.getMessage}. Retrying in 3s…")
          attempt += 1
          if (attempt < retries) Thread.sleep(3000)
      }
    }
    throw new RuntimeException(s"SOAP call $action failed after $retries attempts", lastError)
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
