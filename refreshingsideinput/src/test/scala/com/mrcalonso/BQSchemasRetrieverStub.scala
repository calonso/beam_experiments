package com.mrcalonso

import java.util.concurrent.atomic.AtomicInteger

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.BigQueryUtil
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers

class BQSchemasRetrieverStub(projectId: String, dataset: String, versions: Map[String, String]*)
  extends BQSchemasRetriever with Serializable {

  import BQSchemasRetrieverStub._

  @transient private lazy val parsedVersions = {
    versions.map { version =>
      version.map { case (table, schema) =>
        (BigQueryHelpers.parseTableSpec(s"$projectId:$dataset.$table"),
          BigQueryUtil.parseSchema(schema))
      }
    }
  }

  override def getTables: Seq[TableReference] = parsedVersions(i.incrementAndGet()).keys.toSeq

  override def getTableSchema(ref: TableReference): TableSchema = parsedVersions(i.get())(ref)
}

object BQSchemasRetrieverStub {
  // I don't know why, but within the same test case, the retriever instance changes thus
  // loosing the track of i. That's why I have it here in the companion.
  private val i: AtomicInteger = new AtomicInteger(-1)

  def resetVersions(): Unit = i.set(-1)
}
