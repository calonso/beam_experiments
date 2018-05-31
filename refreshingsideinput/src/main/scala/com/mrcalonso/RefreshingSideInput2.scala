package com.mrcalonso

import com.google.api.client.json.jackson.JacksonFactory
import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.mrcalonso.RefreshingSideInput2.PairType
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.coders._
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

  type PairType = KV[String, java.lang.Iterable[String]]
  private val DocTypeField = "doc_type"

  def main(cmdArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdArgs)

    val projectId = args("project")
    val subscription = args("subscription")
    val dataset = args("dataset")
    val output = args("output")
    val refreshFreq = args("refreshFreq").toInt

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
      .saveAsTextFile(output)

    sc.close().waitUntilFinish()
  }

  def pair(ticks: SCollection[java.lang.Long], types: SCollection[TableReference],
           bq: BQSchemasRetriever)
  : SCollection[PairType] = {

    val schemasView = ticks.withGlobalWindow(WindowOptions(
      trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()),
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
    ))
      .flatMap { _ =>
        bq.getSchemas.map { case (ref, schema) =>
          KV.of(ref, schema)
        }
      }.internal
      .apply(View.asMultimap[TableReference, TableSchema]())

    val strings = types.internal.apply(ParDo.of[TableReference, PairType]
      (new SchemaAssigner(schemasView)).withSideInputs(schemasView))
      .setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())))

    types.context.wrap(strings)
  }
}

class SchemaAssigner(schemasSide:
                     PCollectionView[java.util.Map[TableReference,
                       java.lang.Iterable[TableSchema]]])
  extends DoFn[TableReference, PairType] {

  private val Log = LoggerFactory.getLogger(getClass)

  @ProcessElement
  def processElement(ctx: DoFn[TableReference, PairType]#ProcessContext): Unit = {
    val schemasInst = ctx.sideInput(schemasSide).asScala
    val tRef = ctx.element()
    schemasInst.get(tRef) match {
      case Some(schema) =>
        Log.info(s"Found schema for doc_type: ${tRef.getTableId} with ${schema.asScala.size} " +
          s"versions. Last version has ${schema.asScala.last.getFields.size()} fields")
        ctx.output(KV.of(tRef.getTableId, schema.asScala.map { s =>
          s.setFactory(new JacksonFactory)
          s.toString
        }.asJava))
      case _ =>
    }
  }
}
