package blinq

import blinq.builders.CalculateAgeFn
import blinq.builders.KvLead
import blinq.models.Lead
import com.beust.klaxon.Klaxon
import com.beust.klaxon.Parser
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.getCoder
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import javax.print.Doc


// TODO
//  - Processar idade
// - Agrupar media de idade por estado
// - Exportar para outra tabela do BQ

object BigQueryToStorage {
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(BigQueryToStorageOptions::class.java)

        var pipeline = Pipeline.create(options)

        val query = "SELECT * FROM test.${options.table} where date >= '${options.start}' AND date <= '${options.end}'"

        pipeline.apply("ReadFromBigQuery", BigQueryIO.readTableRows().fromQuery(query))
                .apply("Calculate Age", ParDo.of(CalculateAgeFn()))
                    .setCoder(CoderRegistry.createDefault().getCoder(TypeDescriptor.of(Lead::class.java)))
                .apply("Lead to KV<String,Lead>", ParDo.of(KvLead()))
                .apply("Group By Key", GroupByKey.create())
                .apply("Transform", MapElements.into(TypeDescriptors.strings())
                        .via(SerializableFunction { input: KV<String, Iterable<Int>> ->
                            var sumAges: Int = 0
                            var average: Double
                            input.value.forEach { i: Int -> sumAges += i }
                            average = (sumAges / input.value.count()).toDouble()
                            "{\"${input.key}\":\"${average}\"}"
                        }))
                .apply("WriteToStorage",
                        TextIO.write()
                                .to(options.output)
                                .withSuffix(".txt")
                                .withNumShards(1))

        pipeline.run()
    }
}