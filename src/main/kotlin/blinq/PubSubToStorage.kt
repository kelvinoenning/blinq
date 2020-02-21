package blinq

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Repeatedly
import org.apache.beam.sdk.transforms.windowing.Window
import org.joda.time.Duration

object PubSubToStorage {
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(PubSubToRowOptions::class.java)
        val pipeline = Pipeline.create(options)
    
        pipeline.apply("ReadPubSubEvents", PubsubIO.readStrings().fromTopic(options.topic))
                .apply("Windowing",
                        Window.into<String>(
                            FixedWindows.of(Duration.standardMinutes(options.windowSize))))
                .apply("WriteToStorage",
                        TextIO.write()
                              .to(options.output)
                              .withWindowedWrites()
                              .withSuffix(".txt")
                              .withNumShards(1))

        pipeline.run()
    }
}