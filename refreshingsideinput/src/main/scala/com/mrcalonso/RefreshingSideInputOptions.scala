package com.mrcalonso

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.{Description, Validation}

trait RefreshingSideInputOptions extends DataflowPipelineOptions {

  @Description("Pubsub subscription where to listen to")
  @Validation.Required
  def getSubscription: String

  def setSubscription(value: String): Unit

  @Description("Dataset from where to read schemas in BQ")
  @Validation.Required
  def getDataset: String

  def setDataset(value: String)

  @Description("How many minutes between refreshing schemas from BQ")
  @Validation.Required
  def getRefreshFreq: Int

  def setRefreshFreq(value: Int)
}
