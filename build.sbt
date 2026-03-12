name         := "data-kata"
version      := "1.0.0"
scalaVersion := "2.12.18"

assembly / assemblyJarName := "data-kata-assembly.jar"

libraryDependencies ++= Seq(
  // Spark — provided by spark-submit / bitnami-spark at runtime
  "org.apache.spark" %% "spark-sql"            % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-core"           % "3.5.0" % "provided",

  // Kafka Spark connector (bundled in fat-jar for spark-submit --jars)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
    exclude("org.apache.kafka", "kafka-clients"),

  // Kafka client for ingestion producers
  "org.apache.kafka"           %  "kafka-clients"       % "3.6.2",

  // PostgreSQL JDBC
  "org.postgresql"             %  "postgresql"          % "42.7.3",

  // XML for SOAP server and client
  "org.scala-lang.modules"     %% "scala-xml"           % "2.2.0",

  // Prometheus Java client for /metrics endpoint
  "io.prometheus"              %  "simpleclient"        % "0.16.0",
  "io.prometheus"              %  "simpleclient_common" % "0.16.0",

  // Logging
  "ch.qos.logback"             %  "logback-classic"     % "1.4.14",
  "org.slf4j"                  %  "slf4j-api"           % "1.7.36"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @_*)   => MergeStrategy.concat
  case PathList("META-INF", _ @_*)               => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case "log4j.properties"                        => MergeStrategy.first
  case _                                         => MergeStrategy.first
}

// Java 17 module opens required by Spark/Arrow
assembly / assemblyOption ~= {
  _.withIncludeScala(true)
}
