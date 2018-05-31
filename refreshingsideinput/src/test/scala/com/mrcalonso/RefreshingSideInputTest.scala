package com.mrcalonso

import com.spotify.scio.testing._
import org.apache.beam.sdk.values.KV

class RefreshingSideInputTest extends PipelineSpec with DataProvider {

  "RefreshingSideInput" should "refresh schemas" in {
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
      KV.of(UserType, Iterable(UserSchemaV1).map(toSchema)),
      KV.of(UserType, Iterable(UserSchemaV1, userSchemaV2).map(toSchema))
    )

    runWithContext { sc =>
      val out = RefreshingSideInput.pair(
        sc.testStream(ticks), sc.testStream(mainInput), bQSchemasRetriever).debug()

      out should containInAnyOrder(expected)
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
      KV.of(UserType, Iterable(toSchema(UserSchemaV1))),
      KV.of(UserType, Iterable(toSchema(UserSchemaV1))),
      KV.of(ClientType, Iterable(toSchema(clientSchema)))
    )

    runWithContext { sc =>
      val out = RefreshingSideInput.pair(
        sc.testStream(ticks), sc.testStream(mainInput), bQSchemasRetriever)

      out should containInAnyOrder(expected)
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

    val expected = Iterable(
      UserType -> toSchema(UserSchemaV1),
      UserType -> toSchema(userSchemaV2)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput.buildSchemasCollection(sc.testStream(ticks), bQSchemasRetriever)

      out should containInAnyOrder(expected)
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

    val expected = Iterable(
      UserType -> toSchema(UserSchemaV1),
      UserType -> toSchema(UserSchemaV1),
      ClientType -> toSchema(clientSchema)
    )

    runWithContext { sc =>
      val out = RefreshingSideInput.buildSchemasCollection(sc.testStream(ticks), bQSchemasRetriever)

      out should containInAnyOrder(expected)
    }
  }
}
