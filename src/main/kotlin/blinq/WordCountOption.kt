package blinq

import org.apache.beam.sdk.options.PipelineOptions

interface WordCountOption : PipelineOptions {
  var input: String;
  var output: String
}