package com.datakata.config

/** All runtime configuration sourced from environment variables with sane defaults. */
case class AppConfig(
  // Source DB
  sourceDbUrl:      String = sys.env.getOrElse("SOURCE_DB_URL",      "jdbc:postgresql://source-db:5432/sales_source"),
  sourceDbUser:     String = sys.env.getOrElse("SOURCE_DB_USER",     "sales_user"),
  sourceDbPassword: String = sys.env.getOrElse("SOURCE_DB_PASSWORD", "sales_pass"),

  // Output DB
  outputDbUrl:      String = sys.env.getOrElse("OUTPUT_DB_URL",      "jdbc:postgresql://output-db:5432/sales_results"),
  outputDbUser:     String = sys.env.getOrElse("OUTPUT_DB_USER",     "results_user"),
  outputDbPassword: String = sys.env.getOrElse("OUTPUT_DB_PASSWORD", "results_pass"),

  // Kafka
  kafkaServers: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),

  // OpenLineage / Marquez
  openlineageUrl:       String = sys.env.getOrElse("OPENLINEAGE_URL",       "http://marquez:5000"),
  openlineageNamespace: String = sys.env.getOrElse("OPENLINEAGE_NAMESPACE", "data-kata"),

  // SOAP WS-*
  soapWsUrl: String = sys.env.getOrElse("SOAP_WS_URL", "http://soap-server:8001"),

  // API
  apiPort: Int = sys.env.getOrElse("API_PORT", "8080").toInt,

  // Data period
  dataStartDate: String = sys.env.getOrElse("DATA_START_DATE", "2026-03-03"),
  dataEndDate:   String = sys.env.getOrElse("DATA_END_DATE",   "2026-03-09"),

  // Filesystem data directory
  fsDataDir: String = sys.env.getOrElse("FS_DATA_DIR", "/opt/spark-apps/data")
)
