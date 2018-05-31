package com.mrcalonso

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.util.{Success, Try}

object RefreshingSideInput {

  private val Log = LoggerFactory.getLogger(getClass)

  private val DocTypeField = "doc_type"

  type SchemasList = KV[TableReference, Iterable[TableSchema]]

  def main(cmdlineArgs: Array[String]): Unit = {
    val opts = PipelineOptionsFactory
      .fromArgs(cmdlineArgs: _*)
      .withValidation()
      .as(classOf[RefreshingSideInputOptions])
    val sc = ScioContext(opts)

    val projectId = opts.getProject
    val subscription = opts.getSubscription
    val dataset = opts.getDataset
    val refreshFreq = opts.getRefreshFreq

    val ticks = sc.customInput(
      "Tick", GenerateSequence.from(0).withRate(1, Duration.standardMinutes(refreshFreq))
    )

    val main = sc
      .pubsubSubscriptionWithAttributes[String](s"projects/$projectId/subscriptions/$subscription")
      .map(_._2(DocTypeField))
      .map(t => Try(BigQueryHelpers.parseTableSpec(s"$projectId:$dataset.$t")))
      .collect { case Success(ref) =>
        ref
      }

    val bqSchemasRetriever = new LiveBQSchemasRetriever(projectId, dataset)

    pair(ticks, main, bqSchemasRetriever)

    sc.close().waitUntilFinish()
  }

  def pair(ticks: SCollection[java.lang.Long],
           main: SCollection[TableReference], bq: BQSchemasRetriever)
  : SCollection[SchemasList] = {
    val schemasSide = buildSchemasCollection(ticks, bq).asMultiMapSideInput

    main.withSideInputs(schemasSide)
      .flatMap { case (tRef, sideCtx) =>
        val schemasInst = sideCtx(schemasSide)
        schemasInst.get(tRef) match {
          case Some(schemas) =>
            Log.info(s"Found schema for doc_type: ${tRef.getTableId} with ${schemas.size} " +
              s"versions. Versions have ${schemas.map(_.getFields.size()).mkString(", ")} fields")
            Some(KV.of(tRef, schemas))
          case _ => None
        }
      }.toSCollection
  }

  def buildSchemasCollection(ticks: SCollection[java.lang.Long], bq: BQSchemasRetriever)
  : SCollection[(TableReference, TableSchema)] = {
    ticks.withGlobalWindow(WindowOptions(
      trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    ))
      .flatMap { _ =>
        bq.getSchemas.map { case (ref, schema) =>
          (ref, schema)
        }
      }
  }
}
