package com.mrcalonso

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.spotify.scio.bigquery.BigQueryUtil
import com.spotify.scio.testing.testStreamOf
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.{Duration, Instant}

trait DataProvider {
  protected val ZeroTime: Instant = new Instant
  protected val RefreshFreq: Duration = Duration.standardMinutes(2)
  protected val DummyProject: String = "dummy-proj"
  protected val DummyDataset: String = "dummy-dset"
  protected val UserType: TableReference = toTableRef("User")
  protected val ClientType: TableReference = toTableRef("Client")
  protected val UserSchemaV1: String =
    """{"fields":[
      |  {
      |    "type": "STRING",
      |    "name": "user_id",
      |    "mode": "REQUIRED"
      |  }
      |]}
      |""".stripMargin

  protected val ticks: TestStream[java.lang.Long] = testStreamOf[java.lang.Long]
    .advanceWatermarkTo(ZeroTime)
    .addElements(TimestampedValue.of[java.lang.Long](0L, ZeroTime))
    .advanceProcessingTime(RefreshFreq)
    .addElements(TimestampedValue.of[java.lang.Long](1L, ZeroTime.plus(RefreshFreq)))
    .advanceWatermarkToInfinity()

  protected val mainInput: TestStream[TableReference] = testStreamOf[TableReference]
    .advanceWatermarkTo(ZeroTime)
    .addElements(TimestampedValue.of(UserType, ZeroTime))
    .addElements(TimestampedValue.of(ClientType, ZeroTime))
    .advanceProcessingTime(RefreshFreq.plus(1))
    .addElements(TimestampedValue.of(UserType, ZeroTime.plus(RefreshFreq).plus(1)))
    .addElements(TimestampedValue.of(ClientType, ZeroTime.plus(RefreshFreq).plus(1)))
    .advanceWatermarkToInfinity()

  protected def toTableRef(table: String): TableReference =
    BigQueryHelpers.parseTableSpec(s"$DummyProject:$DummyDataset.$table")

  protected def toSchema(schema: String): TableSchema = BigQueryUtil.parseSchema(schema)
}
