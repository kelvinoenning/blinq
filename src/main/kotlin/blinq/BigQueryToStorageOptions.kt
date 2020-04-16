package blinq

import org.apache.beam.sdk.options.PipelineOptions

interface BigQueryToStorageOptions : PipelineOptions {
  var dataset: String
  var table: String
  var start: String
  var end: String
  var output: String
}