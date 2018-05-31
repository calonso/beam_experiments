package com.mrcalonso

import com.google.api.services.bigquery.model.TableReference
import com.mrcalonso.RefreshingSideInput2.PairType
import com.spotify.scio.bigquery.BigQueryUtil
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.values.{KV, TimestampedValue}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConverters._

class RefreshingSideInput2Test extends PipelineSpec {

  private val ZeroTime = new Instant
  private val RefreshFreq = Duration.standardMinutes(2)
  private val DummyProject = "dummy-proj"
  private val DummyDataset = "dummy-dset"
  private val UserType = BigQueryHelpers.parseTableSpec(s"$DummyProject:$DummyDataset.User")
  private val ClientType = BigQueryHelpers.parseTableSpec(s"$DummyProject:$DummyDataset.Client")
  private val UserSchemaV1 =
    """{"fields":[
      |  {
      |    "type": "STRING",
      |    "name": "user_id",
      |    "mode": "REQUIRED"
      |  }
      |]}
      |""".stripMargin

  private val ticks = testStreamOf[java.lang.Long]
    .advanceWatermarkTo(ZeroTime)
    .addElements(TimestampedValue.of[java.lang.Long](0L, ZeroTime))
    .advanceProcessingTime(RefreshFreq)
    .addElements(TimestampedValue.of[java.lang.Long](1L, ZeroTime.plus(RefreshFreq)))
    .advanceWatermarkToInfinity()

  private val mainInput = testStreamOf[TableReference]
    .advanceWatermarkTo(ZeroTime)
    .addElements(TimestampedValue.of(UserType, ZeroTime))
    .addElements(TimestampedValue.of(ClientType, ZeroTime))
    .advanceProcessingTime(RefreshFreq.plus(1))
    .addElements(TimestampedValue.of(UserType, ZeroTime.plus(RefreshFreq).plus(1)))
    .addElements(TimestampedValue.of(ClientType, ZeroTime.plus(RefreshFreq).plus(1)))
    .advanceWatermarkToInfinity()

  "RefreshingSideInput2" should "refresh schemas" in {
    BQSchemasRetrieverStub.resetVersions()
    val userSchemaV2 =
      """{"fields":[
        |  {
        |    "type": "STRING",
        |    "name": "user_id",
        |    "mode": "REQUIRED"
        |  },
        |  {
        |    "type": "STRING",
        |    "name": "name",
        |    "mode": "REQUIRED"
        |  }
        |]}
        |""".stripMargin

    val v1 = Map(UserType.getTableId -> UserSchemaV1)
    val v2 = Map(UserType.getTableId -> userSchemaV2)
    val bQSchemasRetriever = new BQSchemasRetrieverStub(DummyProject, DummyDataset, v1, v2)

    val expected = Iterable(
      KV.of("User", Iterable(UserSchemaV1).asJava),
      KV.of("User", Iterable(UserSchemaV1, userSchemaV2).asJava)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput2.pair(
        sc.testStream(ticks), sc.testStream(mainInput), bQSchemasRetriever).debug()

      out should haveSize(2)

      assertSchemas(expected, out)
    }
  }

  it should "add more schemas" in {
    BQSchemasRetrieverStub.resetVersions()
    val clientSchema =
      """{"fields":[
        |  {
        |    "type": "STRING",
        |    "name": "client_id",
        |    "mode": "REQUIRED"
        |  }
        |]}
        |""".stripMargin

    val v1 = Map(UserType.getTableId -> UserSchemaV1)
    val v2 = Map(UserType.getTableId -> UserSchemaV1, ClientType.getTableId -> clientSchema)

    val bQSchemasRetriever = new BQSchemasRetrieverStub(DummyProject, DummyDataset, v1, v2)

    val expected = Iterable(
      KV.of("User", Iterable(UserSchemaV1).asJava),
      KV.of("User", Iterable(UserSchemaV1).asJava),
      KV.of("Client", Iterable(clientSchema).asJava)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput2.pair(
        sc.testStream(ticks), sc.testStream(mainInput), bQSchemasRetriever)

      out should haveSize(3)

      assertSchemas(expected, out)
    }
  }

  private def assertSchemas(expected: Iterable[PairType], found: SCollection[PairType]): Unit = {
    found.map { kv =>
      KV.of(kv.getKey, kv.getValue.asScala.map(BigQueryUtil.parseSchema))
    } should containInAnyOrder(expected.map { kv =>
      KV.of(kv.getKey, kv.getValue.asScala.map(BigQueryUtil.parseSchema))
    })
  }
}
