package com.datakata.soap

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.slf4j.LoggerFactory

import java.io.{InputStream, OutputStream}
import java.net.InetSocketAddress
import scala.io.Source
import scala.xml.{Elem, XML}

/**
 * Minimal WS-* / SOAP 1.1 server implemented with Java's built-in HttpServer.
 * Exposes: GetAllProducts, GetProductById, GetProductsByCategory,
 *          GetAllSalesmen, GetSalesmanById
 *
 * Replaces sources/soap_ws/soap_server.py (spyne / Python).
 */
object SoapServer {

  private val log = LoggerFactory.getLogger(getClass)
  private val TNS = "http://data-kata.br/product-catalog"

  // ── static catalog data ──────────────────────────────────────────────────

  private val products = Seq(
    (1,  "Notebook Dell Inspiron 15",     "Eletrônicos",      3499.90, "Y"),
    (2,  "Smartphone Samsung Galaxy A54", "Eletrônicos",      1799.90, "Y"),
    (3,  "Monitor LG 27\" 4K",            "Eletrônicos",      2199.90, "Y"),
    (4,  "Teclado Mecânico Redragon",      "Periféricos",       399.90, "Y"),
    (5,  "Mouse Logitech MX Master 3",    "Periféricos",       599.90, "Y"),
    (6,  "Cadeira Gamer ThunderX3",       "Móveis",           1299.90, "Y"),
    (7,  "Headset JBL Quantum 400",       "Áudio",             449.90, "Y"),
    (8,  "Tablet iPad Air 5",             "Eletrônicos",      4999.90, "Y"),
    (9,  "Impressora HP LaserJet",        "Periféricos",       899.90, "Y"),
    (10, "SSD Kingston 1TB",              "Armazenamento",     399.90, "Y"),
    (11, "Webcam Logitech C920",          "Periféricos",       699.90, "Y"),
    (12, "Roteador TP-Link AX3000",       "Redes",             499.90, "Y"),
    (13, "Smart TV Samsung 55\" QLED",    "Eletrônicos",      4299.90, "Y"),
    (14, "Ar Condicionado Daikin 12k",    "Climatização",     3199.90, "Y"),
    (15, "Fritadeira Airfryer Philco",    "Eletrodomésticos",  399.90, "Y")
  )

  private val salesmen = Seq(
    (1,  "Carlos Andrade",    "São Paulo",      "SP", "Sudeste"),
    (2,  "Fernanda Lima",     "Rio de Janeiro", "RJ", "Sudeste"),
    (3,  "Roberto Souza",     "Belo Horizonte", "MG", "Sudeste"),
    (4,  "Patricia Mendes",   "Curitiba",       "PR", "Sul"),
    (5,  "Marcos Oliveira",   "Porto Alegre",   "RS", "Sul"),
    (6,  "Juliana Costa",     "Salvador",       "BA", "Nordeste"),
    (7,  "Anderson Ferreira", "Fortaleza",      "CE", "Nordeste"),
    (8,  "Camila Santos",     "Recife",         "PE", "Nordeste"),
    (9,  "Diego Alves",       "Manaus",         "AM", "Norte"),
    (10, "Thais Rodrigues",   "Brasília",       "DF", "Centro-Oeste")
  )

  // ── SOAP helpers ─────────────────────────────────────────────────────────

  private def envelope(body: String): String =
    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
       |               xmlns:tns="$TNS">
       |  <soap:Body>$body</soap:Body>
       |</soap:Envelope>""".stripMargin

  private def productXml(p: (Int, String, String, Double, String)): String =
    s"""<tns:return>
       |  <product_id>${p._1}</product_id>
       |  <name>${escape(p._2)}</name>
       |  <category>${escape(p._3)}</category>
       |  <unit_price>${p._4}</unit_price>
       |  <is_active>${p._5}</is_active>
       |</tns:return>""".stripMargin

  private def salesmanXml(s: (Int, String, String, String, String)): String =
    s"""<tns:return>
       |  <salesman_id>${s._1}</salesman_id>
       |  <name>${escape(s._2)}</name>
       |  <city>${escape(s._3)}</city>
       |  <state>${s._4}</state>
       |  <region>${escape(s._5)}</region>
       |</tns:return>""".stripMargin

  private def escape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
     .replace("\"", "&quot;").replace("'", "&apos;")

  // ── dispatch ─────────────────────────────────────────────────────────────

  private def dispatch(requestXml: String): String = {
    val body =
      try {
        val xml    = XML.loadString(requestXml)
        val action = (xml \\ "_").find(n => n.namespace == TNS).map(_.label).getOrElse("")

        action match {
          case "GetAllProducts" =>
            val items = products.map(productXml).mkString("\n")
            envelope(s"<tns:GetAllProductsResponse xmlns:tns=\"$TNS\">\n$items\n</tns:GetAllProductsResponse>")

          case "GetProductById" =>
            val id = ((xml \\ "product_id").headOption orElse (xml \\ "arg0").headOption)
              .map(_.text.trim.toInt).getOrElse(-1)
            val item = products.find(_._1 == id).map(productXml).getOrElse("")
            envelope(s"<tns:GetProductByIdResponse xmlns:tns=\"$TNS\">\n$item\n</tns:GetProductByIdResponse>")

          case "GetProductsByCategory" =>
            val cat = ((xml \\ "category").headOption orElse (xml \\ "arg0").headOption)
              .map(_.text.trim).getOrElse("")
            val items = products.filter(_._3.equalsIgnoreCase(cat)).map(productXml).mkString("\n")
            envelope(s"<tns:GetProductsByCategoryResponse xmlns:tns=\"$TNS\">\n$items\n</tns:GetProductsByCategoryResponse>")

          case "GetAllSalesmen" =>
            val items = salesmen.map(salesmanXml).mkString("\n")
            envelope(s"<tns:GetAllSalesmenResponse xmlns:tns=\"$TNS\">\n$items\n</tns:GetAllSalesmenResponse>")

          case "GetSalesmanById" =>
            val id = ((xml \\ "salesman_id").headOption orElse (xml \\ "arg0").headOption)
              .map(_.text.trim.toInt).getOrElse(-1)
            val item = salesmen.find(_._1 == id).map(salesmanXml).getOrElse("")
            envelope(s"<tns:GetSalesmanByIdResponse xmlns:tns=\"$TNS\">\n$item\n</tns:GetSalesmanByIdResponse>")

          case unknown =>
            envelope(s"""<soap:Fault><faultcode>soap:Client</faultcode>
                        |<faultstring>Unknown action: $unknown</faultstring></soap:Fault>""".stripMargin)
        }
      } catch {
        case e: Exception =>
          envelope(s"""<soap:Fault><faultcode>soap:Server</faultcode>
                      |<faultstring>${escape(e.getMessage)}</faultstring></soap:Fault>""".stripMargin)
      }
    body
  }

  // ── WSDL ─────────────────────────────────────────────────────────────────

  private val wsdl: String =
    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
       |             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
       |             xmlns:tns="$TNS"
       |             xmlns:xsd="http://www.w3.org/2001/XMLSchema"
       |             targetNamespace="$TNS"
       |             name="ProductCatalogService">
       |
       |  <types>
       |    <xsd:schema targetNamespace="$TNS">
       |      <xsd:complexType name="Product">
       |        <xsd:sequence>
       |          <xsd:element name="product_id" type="xsd:int"/>
       |          <xsd:element name="name"        type="xsd:string"/>
       |          <xsd:element name="category"    type="xsd:string"/>
       |          <xsd:element name="unit_price"  type="xsd:double"/>
       |          <xsd:element name="is_active"   type="xsd:string"/>
       |        </xsd:sequence>
       |      </xsd:complexType>
       |      <xsd:complexType name="Salesman">
       |        <xsd:sequence>
       |          <xsd:element name="salesman_id" type="xsd:int"/>
       |          <xsd:element name="name"         type="xsd:string"/>
       |          <xsd:element name="city"         type="xsd:string"/>
       |          <xsd:element name="state"        type="xsd:string"/>
       |          <xsd:element name="region"       type="xsd:string"/>
       |        </xsd:sequence>
       |      </xsd:complexType>
       |    </xsd:schema>
       |  </types>
       |
       |  <message name="GetAllProductsRequest"/>
       |  <message name="GetAllProductsResponse"><part name="return" type="tns:Product"/></message>
       |  <message name="GetAllSalesmenRequest"/>
       |  <message name="GetAllSalesmenResponse"><part name="return" type="tns:Salesman"/></message>
       |
       |  <portType name="ProductCatalogPortType">
       |    <operation name="GetAllProducts">
       |      <input  message="tns:GetAllProductsRequest"/>
       |      <output message="tns:GetAllProductsResponse"/>
       |    </operation>
       |    <operation name="GetAllSalesmen">
       |      <input  message="tns:GetAllSalesmenRequest"/>
       |      <output message="tns:GetAllSalesmenResponse"/>
       |    </operation>
       |  </portType>
       |
       |  <binding name="ProductCatalogBinding" type="tns:ProductCatalogPortType">
       |    <soap:binding style="document" transport="http://schemas.xmlsoap.org/soap/http"/>
       |    <operation name="GetAllProducts">
       |      <soap:operation soapAction="GetAllProducts"/>
       |      <input><soap:body use="literal"/></input>
       |      <output><soap:body use="literal"/></output>
       |    </operation>
       |    <operation name="GetAllSalesmen">
       |      <soap:operation soapAction="GetAllSalesmen"/>
       |      <input><soap:body use="literal"/></input>
       |      <output><soap:body use="literal"/></output>
       |    </operation>
       |  </binding>
       |
       |  <service name="ProductCatalogService">
       |    <port name="ProductCatalogPort" binding="tns:ProductCatalogBinding">
       |      <soap:address location="http://soap-server:8001/"/>
       |    </port>
       |  </service>
       |</definitions>""".stripMargin

  // ── HTTP handler ─────────────────────────────────────────────────────────

  private class SoapHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val method = exchange.getRequestMethod
      val query  = Option(exchange.getRequestURI.getQuery).getOrElse("")

      val (status, contentType, responseBody) =
        if (method == "GET" && query.contains("wsdl")) {
          (200, "text/xml; charset=UTF-8", wsdl)
        } else if (method == "POST") {
          val body = Source.fromInputStream(exchange.getRequestBody, "UTF-8").mkString
          (200, "text/xml; charset=UTF-8", dispatch(body))
        } else {
          (405, "text/plain", "Method Not Allowed")
        }

      val bytes = responseBody.getBytes("UTF-8")
      exchange.getResponseHeaders.set("Content-Type", contentType)
      exchange.sendResponseHeaders(status, bytes.length)
      val out: OutputStream = exchange.getResponseBody
      out.write(bytes)
      out.close()
    }
  }

  // ── entry point ──────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    val port = sys.env.getOrElse("SOAP_WS_PORT", "8001").toInt
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/", new SoapHandler)
    server.setExecutor(null)
    server.start()
    log.info(s"SOAP server listening on port $port")
    log.info(s"WSDL: http://0.0.0.0:$port/?wsdl")
  }
}
