package blinq

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

interface PubSubToRowOptions : DataflowPipelineOptions {
    var topic: String
}