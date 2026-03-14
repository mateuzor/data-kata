package com.datakata.datagen

import com.datakata.config.AppConfig
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
import java.time.{DayOfWeek, LocalDate}

/**
 * Generates daily CSV sales files for the filesystem source.
 * Plain Scala I/O — no Spark or external dependencies required.
 *
 * Output: <FS_DATA_DIR>/sales_YYYY-MM-DD.csv  (one flat file per day)
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
    (1, "Notebook Dell Inspiron 15",     "Eletrônicos",   3499.90),
    (2, "Smartphone Samsung Galaxy A54", "Eletrônicos",   1799.90),
    (3, "Monitor LG 27 4K",              "Eletrônicos",   2199.90),
    (4, "Teclado Mecânico Redragon",      "Periféricos",    399.90),
    (5, "Mouse Logitech MX Master 3",    "Periféricos",    599.90),
    (6, "Cadeira Gamer ThunderX3",       "Móveis",        1299.90),
    (7, "Headset JBL Quantum 400",       "Áudio",          449.90),
    (8, "Tablet iPad Air 5",             "Eletrônicos",   4999.90),
    (9, "Impressora HP LaserJet",        "Periféricos",    899.90),
    (10,"SSD Kingston 1TB",              "Armazenamento",  399.90)
  )

  private val salesmen = (1 to 20).map(i => (i, s"Vendedor $i"))
  private val rng      = new scala.util.Random(42)

  private val HEADER =
    "source,sale_date,salesman_id,salesman_name,product_id,product_name," +
    "category,city,state,region,quantity,unit_price,total_amount"

  def main(args: Array[String]): Unit = {
    val outDir = new File(config.fsDataDir)
    outDir.mkdirs()

    val start = LocalDate.parse(config.dataStartDate)
    val end   = LocalDate.parse(config.dataEndDate)

    var date = start
    while (!date.isAfter(end)) {
      val file = new File(outDir, s"sales_$date.csv")
      val pw   = new PrintWriter(file, "UTF-8")
      try {
        pw.println(HEADER)
        generateDay(date).foreach(pw.println)
      } finally {
        pw.close()
      }
      log.info(s"Generated $file")
      date = date.plusDays(1)
    }

    log.info(s"Data generation complete → ${outDir.getAbsolutePath}")
  }

  private def generateDay(date: LocalDate): Seq[String] = {
    val isWeekend = date.getDayOfWeek == DayOfWeek.SATURDAY ||
                    date.getDayOfWeek == DayOfWeek.SUNDAY
    val count = if (isWeekend) 8 + rng.nextInt(5) else 20 + rng.nextInt(16)

    (1 to count).map { _ =>
      val (pid, pname, cat, price) = products(rng.nextInt(products.size))
      val (sid, sname)             = salesmen(rng.nextInt(salesmen.size))
      val (city, state, region)    = cities(rng.nextInt(cities.size))
      val qty                      = 1 + rng.nextInt(3)
      val total                    = price * qty
      s"filesystem,$date,$sid,$sname,$pid,$pname,$cat,$city,$state,$region,$qty,$price,$total"
    }
  }
}
