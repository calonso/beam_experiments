package com.mrcalonso

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.BigQueryClient.{CACHE_ENABLED_KEY, PROJECT_KEY}
import org.slf4j.{Logger, LoggerFactory}

trait BQSchemasRetriever {

  protected val Log: Logger = LoggerFactory.getLogger(getClass)

  def getSchemas: Seq[(TableReference, TableSchema)] = {
    Log.info("Retrieving schemas...")
    getTables.map(tr => (tr, getTableSchema(tr)))
  }

  protected def getTables: Seq[TableReference]

  protected def getTableSchema(ref: TableReference): TableSchema
}

class LiveBQSchemasRetriever(projectId: String, dataset: String) extends BQSchemasRetriever {

  private lazy val BQ = {
    sys.props(PROJECT_KEY) = projectId
    sys.props(CACHE_ENABLED_KEY) = "false"
    BigQueryClient.defaultInstance()
  }

  override def getTables: Seq[TableReference] = BQ.getTables(projectId, dataset)

  override def getTableSchema(ref: TableReference): TableSchema = {
    Log.debug(s"Retrieving schema for ${ref.getTableId}")
    BQ.getTableSchema(ref)
  }
}
