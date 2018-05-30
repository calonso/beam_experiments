package com.mrcalonso

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.BigQueryClient.{CACHE_ENABLED_KEY, PROJECT_KEY}
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.util.{Success, Try}

object RefreshingSideInput {

  private val Log = LoggerFactory.getLogger(getClass)

  private val DocTypeField = "doc_type"

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val projectId = args("project")
    val subscription = args("subscription")
    val dataset = args("dataset")

    val schemas = sc.customInput(
      "Tick", GenerateSequence.from(0).withRate(1, Duration.standardMinutes(2))
    ).withGlobalWindow(WindowOptions(
      trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    ))
      .flatMap { _ =>
        Log.info("Retrieving schemas...")
        sys.props(PROJECT_KEY) = projectId
        sys.props(CACHE_ENABLED_KEY) = "false"
        BigQueryClient.defaultInstance.getTables(projectId, dataset).map { tbl =>
          Log.debug(s"Retrieving schema for ${tbl.getTableId}")
          (tbl, BigQueryClient.defaultInstance.getTableSchema(tbl))
        }
      }.asMultiMapSideInput

    sc
      .pubsubSubscriptionWithAttributes[String](s"projects/$projectId/subscriptions/$subscription")
      .withSideInputs(schemas)
      .map { case (msgWithAttrs, sideCtx) =>
        typeToTableSpec(projectId, dataset, msgWithAttrs._2(DocTypeField)) match {
          case Success(tRef) =>
            val schemasInst = sideCtx(schemas)
            schemasInst.get(tRef) match {
              case Some(schema) =>
                Log.debug(s"Current schemas side has ${schemasInst.keys.size} keys")
                Log.info(s"Found schema for doc_type: ${tRef.getTableId} with ${schema.size} " +
                  s"versions. Last version has ${schema.last.getFields.size()} fields")
              case _ =>
            }
          case _ =>
        }
      }

    sc.close().waitUntilFinish()
  }

  private def typeToTableSpec(project: String, dataset: String, str: String): Try[TableReference] =
    Try(BigQueryHelpers.parseTableSpec(s"$project:$dataset.$str"))
}
