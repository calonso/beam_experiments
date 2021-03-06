package com.mrcalonso

import java.{lang, util}

import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import com.mrcalonso.RefreshingSideInput2.PairType
import com.spotify.scio.bigquery.BigQueryUtil
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.values.KV

import scala.collection.JavaConverters._

class RefreshingSideInput2Test extends PipelineSpec with DataProvider {

  import RefreshingSideInput2Test._

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

  it should "successfully adds versions to the view" in {
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

    val expected = Map(
      UserType.getTableId -> Iterable(UserSchemaV1, userSchemaV2)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput2.buildSchemasView(sc.testStream(ticks), bQSchemasRetriever)

      PAssert.thatMultimap(out).satisfies(
        (input: util.Map[TableReference, lang.Iterable[TableSchema]]) => {
          assertSide(expected, input)
          null
        })
    }
  }

  it should "successfully adds schemas to the view" in {
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

    val expected = Map(
      "User" -> Iterable(UserSchemaV1),
      "Client" -> Iterable(clientSchema)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput2.buildSchemasView(sc.testStream(ticks), bQSchemasRetriever)

      PAssert.thatMultimap(out).satisfies(
        (input: util.Map[TableReference, lang.Iterable[TableSchema]]) => {
          assertSide(expected, input)
          null
        })
    }
  }
}

object RefreshingSideInput2Test extends PipelineSpec with DataProvider {
  def assertSide(expected: Map[String, Iterable[String]],
                 input: util.Map[TableReference, java.lang.Iterable[TableSchema]]): Unit = {
    expected.keys.map(toTableRef) should contain theSameElementsAs input.keySet().asScala

    expected.foreach { case (k, v) =>
      input.get(toTableRef(k)).asScala should contain theSameElementsAs v.map(toSchema)
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
