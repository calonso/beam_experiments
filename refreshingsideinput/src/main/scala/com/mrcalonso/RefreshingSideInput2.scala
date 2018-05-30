package com.mrcalonso

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.BigQueryClient.{CACHE_ENABLED_KEY, PROJECT_KEY}
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.apache.beam.sdk.transforms.{DoFn, ParDo, View}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.values.{KV, PCollectionView}
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

object RefreshingSideInput2 {

  private val Log = LoggerFactory.getLogger(getClass)

  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

    val projectId = args("project")
    val subscription = args("subscription")
    val dataset = args("dataset")

    val schemasView = sc.customInput(
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
          KV.of(tbl, BigQueryClient.defaultInstance.getTableSchema(tbl))
        }
      }.internal
      .apply(View.asMultimap[TableReference, TableSchema]())

    sc
      .pubsubSubscriptionWithAttributes[String](s"projects/$projectId/subscriptions/$subscription")
      .internal.apply(ParDo.of[(String, Map[String, String]), String]
      (new SchemaAssigner(schemasView, projectId, dataset)).withSideInputs(schemasView))
      .setCoder(StringUtf8Coder.of())

    sc.close().waitUntilFinish()
  }
}

class SchemaAssigner(schemasSide:
                     PCollectionView[java.util.Map[TableReference,
                       java.lang.Iterable[TableSchema]]], projectId: String, dataset: String)
  extends DoFn[(String, Map[String, String]), String] {

  private val Log = LoggerFactory.getLogger(getClass)

  private val DocTypeField = "doc_type"

  @ProcessElement
  def processElement(ctx: DoFn[(String, Map[String, String]), String]#ProcessContext): Unit = {
    val msgWithAttrs = ctx.element()
    val schemasInst = ctx.sideInput(schemasSide).asScala
    typeToTableSpec(projectId, dataset, msgWithAttrs._2(DocTypeField)) match {
      case Success(tRef) =>
        schemasInst.get(tRef) match {
          case Some(schema) =>
            Log.debug(s"Current schemas side has ${schemasInst.keys.size} keys")
            Log.info(s"Found schema for doc_type: ${tRef.getTableId} with ${schema.asScala.size} " +
              s"versions. Last version has ${schema.asScala.last.getFields.size()} fields")
          case _ =>
        }
      case _ =>
    }
  }

  private def typeToTableSpec(project: String, dataset: String, str: String): Try[TableReference] =
    Try(BigQueryHelpers.parseTableSpec(s"$project:$dataset.$str"))
}
