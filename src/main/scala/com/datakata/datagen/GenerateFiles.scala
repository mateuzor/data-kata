package com.datakata.datagen

import com.datakata.config.AppConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.DayOfWeek

/**
 * Generates daily CSV + Parquet sales files for the filesystem source.
 * Replicates sources/filesystem/generate_files.py (removed — was Python).
 *
 * Run: spark-submit --class com.datakata.datagen.GenerateFiles data-kata.jar
 *
 * Uses Spark local mode — no cluster required.
 */
object GenerateFiles {

  private val log    = LoggerFactory.getLogger(getClass)
  private val config = AppConfig()

  private val cities = Seq(
    ("São Paulo", "SP", "Sudeste"), ("Rio de Janeiro", "RJ", "Sudeste"),
    ("Belo Horizonte", "MG", "Sudeste"), ("Curitiba", "PR", "Sul"),
    ("Porto Alegre", "RS", "Sul"), ("Salvador", "BA", "Nordeste"),
    ("Fortaleza", "CE", "Nordeste"), ("Recife", "PE", "Nordeste"),
    ("Manaus", "AM", "Norte"), ("Brasília", "DF", "Centro-Oeste"),
    ("Goiânia", "GO", "Centro-Oeste"), ("Belém", "PA", "Norte"),
    ("Florianópolis", "SC", "Sul"), ("São Luís", "MA", "Nordeste"),
    ("Maceió", "AL", "Nordeste"), ("Natal", "RN", "Nordeste"),
    ("Teresina", "PI", "Nordeste"), ("Campo Grande", "MS", "Centro-Oeste"),
    ("João Pessoa", "PB", "Nordeste")
  )

  private val products = Seq(
    (1, "Notebook Dell Inspiron 15",     "Eletrônicos",      3499.90),
    (2, "Smartphone Samsung Galaxy A54", "Eletrônicos",      1799.90),
    (3, "Monitor LG 27\" 4K",            "Eletrônicos",      2199.90),
    (4, "Teclado Mecânico Redragon",      "Periféricos",       399.90),
    (5, "Mouse Logitech MX Master 3",    "Periféricos",       599.90),
    (6, "Cadeira Gamer ThunderX3",       "Móveis",           1299.90),
    (7, "Headset JBL Quantum 400",       "Áudio",             449.90),
    (8, "Tablet iPad Air 5",             "Eletrônicos",      4999.90),
    (9, "Impressora HP LaserJet",        "Periféricos",       899.90),
    (10,"SSD Kingston 1TB",              "Armazenamento",     399.90)
  )

  private val salesmen = (1 to 20).map(i => (i, s"Vendedor $i"))

  private val rng = new scala.util.Random(42)

  case class SaleRow(
    source: String, sale_date: String,
    salesman_id: Int, salesman_name: String,
    product_id: Int, product_name: String, category: String,
    city: String, state: String, region: String,
    quantity: Int, unit_price: Double, total_amount: Double
  )

  private def generateDay(date: LocalDate): Seq[SaleRow] = {
    val isWeekend = date.getDayOfWeek == DayOfWeek.SATURDAY ||
                    date.getDayOfWeek == DayOfWeek.SUNDAY
    val count = if (isWeekend) 8 + rng.nextInt(5) else 20 + rng.nextInt(16)

    (1 to count).map { _ =>
      val (pid, pname, cat, price) = products(rng.nextInt(products.size))
      val (sid, sname)             = salesmen(rng.nextInt(salesmen.size))
      val (city, state, region)    = cities(rng.nextInt(cities.size))
      val qty                      = 1 + rng.nextInt(3)
      SaleRow("filesystem", date.toString, sid, sname, pid, pname, cat,
               city, state, region, qty, price, price * qty)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenerateFiles")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val start = LocalDate.parse(config.dataStartDate)
    val end   = LocalDate.parse(config.dataEndDate)
    val outDir = config.fsDataDir

    var date = start
    while (!date.isAfter(end)) {
      val rows = generateDay(date).toDS()

      rows.coalesce(1).write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv(s"$outDir/csv/sales_$date")

      rows.coalesce(1).write.mode(SaveMode.Overwrite)
        .parquet(s"$outDir/parquet/sales_$date")

      log.info(s"Generated ${rows.count()} rows for $date")
      date = date.plusDays(1)
    }

    log.info(s"Data generation complete → $outDir")
    spark.stop()
  }
}
