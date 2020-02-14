package blinq

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptors

object WordCount {
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).`as`(WordCountOption::class.java)
        val pipeline = Pipeline.create(options)

        pipeline.apply("Read Csv", TextIO.read().from(options.input))
                .apply("Split Csv Row",
                    FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(ProcessFunction<String, List<String>> { input -> input.split("[^\\p{L}]+").toList() }))
                .apply("Ignore Blank Input", Filter.by(SerializableFunction<String, Boolean> { input -> !input.isEmpty() }))
                .apply("Count Words", Count.perElement<String>())
                .apply("Map Word Count",
                    MapElements
                        .into(TypeDescriptors.strings())
                        .via(ProcessFunction<KV<String, Long>, String> { input -> "${input.key} : ${input.value}" }))
            .apply("Output", TextIO.write().to(options.output))

        pipeline.run().waitUntilFinish()
    }
}