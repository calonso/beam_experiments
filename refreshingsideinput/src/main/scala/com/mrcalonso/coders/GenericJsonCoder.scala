package com.mrcalonso.coders

import java.io.{InputStream, OutputStream}

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.client.json.GenericJson
import org.apache.beam.sdk.coders.{CustomCoder, StringUtf8Coder}

class GenericJsonCoder[T <: GenericJson](cls: Class[T]) extends CustomCoder[T] {
  /////////////////////////////////////////////////////////////////////////////
  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
  // TableRow.
  private val MAPPER = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)

  override def encode(value: T, outStream: OutputStream): Unit = {
    val str = MAPPER.writeValueAsString(value)
    StringUtf8Coder.of().encode(str, outStream)
  }

  override def decode(inStream: InputStream): T = {
    val str = StringUtf8Coder.of().decode(inStream)
    MAPPER.readValue(str, cls)
  }
}

object GenericJsonCoder {
  def of[T <: GenericJson](cls: Class[T]): GenericJsonCoder[T] = new GenericJsonCoder[T](cls)
}
