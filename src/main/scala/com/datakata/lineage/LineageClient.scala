package com.datakata.lineage

import com.datakata.config.AppConfig
import org.slf4j.LoggerFactory

import java.net.{HttpURLConnection, URL}
import java.time.Instant

/**
 * Minimal OpenLineage HTTP client.
 * Posts START / COMPLETE / FAIL events to the Marquez API.
 * Failures are logged as warnings so the pipeline never dies due to lineage issues.
 */
class LineageClient(config: AppConfig) {

  private val log = LoggerFactory.getLogger(getClass)

  def emitStart(runId: String, jobName: String,
                inputs: Seq[String], outputs: Seq[String]): Unit =
    post(buildEvent("START", runId, jobName, inputs, outputs))

  def emitComplete(runId: String, jobName: String, rowsProcessed: Long): Unit =
    post(buildEvent("COMPLETE", runId, jobName, Seq.empty, Seq.empty, rowsProcessed))

  def emitFail(runId: String, jobName: String, error: String): Unit =
    post(buildEvent("FAIL", runId, jobName, Seq.empty, Seq.empty, errorMsg = error))

  // ── private ──────────────────────────────────────────────────────────────

  private def post(json: String): Unit =
    try {
      val url  = new URL(s"${config.openlineageUrl}/api/v1/lineage")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.setDoOutput(true)
      conn.setConnectTimeout(3000)
      conn.setReadTimeout(3000)
      conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
      val out = conn.getOutputStream
      out.write(json.getBytes("UTF-8"))
      out.close()
      val code = conn.getResponseCode
      if (code >= 400) log.warn(s"Lineage API returned HTTP $code")
    } catch {
      case e: Exception =>
        log.warn(s"Lineage emit failed (${e.getMessage}) — pipeline continues normally")
    }

  private def buildEvent(
    eventType:    String,
    runId:        String,
    jobName:      String,
    inputs:       Seq[String],
    outputs:      Seq[String],
    rowsProcessed: Long   = 0,
    errorMsg:     String  = ""
  ): String = {
    val ns   = config.openlineageNamespace
    val now  = Instant.now().toString

    def datasetList(names: Seq[String]) =
      names.map(n => s"""{"namespace":"$ns","name":"$n"}""").mkString("[", ",", "]")

    val facets =
      if (rowsProcessed > 0)
        s""","rowCount":{"_producer":"data-kata","_schemaURL":"https://openlineage.io/spec/1-0-5/OpenLineage.json","rowCount":$rowsProcessed}"""
      else if (errorMsg.nonEmpty)
        s""","errorMessage":{"_producer":"data-kata","_schemaURL":"https://openlineage.io/spec/1-0-5/OpenLineage.json","message":"${errorMsg.replace("\"", "'")}"}"""
      else ""

    s"""{
       |  "eventType": "$eventType",
       |  "eventTime": "$now",
       |  "run": {
       |    "runId": "$runId",
       |    "facets": {
       |      "processingEngine": {
       |        "_producer": "data-kata",
       |        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ProcessingEngineFacet.json",
       |        "name": "spark", "version": "3.5.0"
       |      }
       |    }
       |  },
       |  "job": {
       |    "namespace": "$ns",
       |    "name": "$jobName",
       |    "facets": {}
       |  },
       |  "inputs":  ${datasetList(inputs)},
       |  "outputs": ${datasetList(outputs)}
       |}""".stripMargin
  }
}
