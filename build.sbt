name         := "data-kata"
version      := "1.0.0"
scalaVersion := "2.12.18"

val flinkVersion      = "1.18.1"
val flinkKafkaVersion = "3.1.0-1.18"

assembly / assemblyJarName := "data-kata-assembly.jar"

libraryDependencies ++= Seq(
  // Flink — provided by the cluster at runtime
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %  "flink-clients"          % flinkVersion % "provided",

  // Flink Kafka connector (bundled in fat-jar)
  "org.apache.flink" % "flink-connector-kafka" % flinkKafkaVersion,

  // Kafka client for ingestion producers
  "org.apache.kafka" % "kafka-clients" % "3.6.2",

  // PostgreSQL JDBC
  "org.postgresql" % "postgresql" % "42.7.3",

  // XML for SOAP server and client
  "org.scala-lang.modules" %% "scala-xml" % "2.2.0",

  // JSON parsing — explicit version for Flink's bundled Jackson
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.4",

  // Prometheus Java client for /metrics endpoint
  "io.prometheus" % "simpleclient"        % "0.16.0",
  "io.prometheus" % "simpleclient_common" % "0.16.0",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.slf4j"      % "slf4j-api"       % "1.7.36"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
  case PathList("META-INF", _ @_*)             => MergeStrategy.discard
  case "reference.conf"                        => MergeStrategy.concat
  case "log4j.properties"                      => MergeStrategy.first
  case _                                       => MergeStrategy.first
}

assembly / assemblyOption ~= {
  _.withIncludeScala(true)
}
