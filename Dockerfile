# Runtime image for API, SOAP server and ingestion producers
# Spark jobs use bitnami/spark directly via spark-submit
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app
COPY target/scala-2.12/data-kata-assembly.jar /app/data-kata.jar

ENTRYPOINT ["java", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "-cp", "/app/data-kata.jar"]

# Override CMD in docker-compose per service, e.g.:
#   command: ["com.datakata.api.ApiServer"]
CMD ["com.datakata.api.ApiServer"]
