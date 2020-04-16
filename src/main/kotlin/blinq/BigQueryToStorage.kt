package blinq

import blinq.builders.CalculateAgeFn
import blinq.models.Event
import blinq.models.Lead
import com.beust.klaxon.Klaxon
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

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

                .apply("Transform", MapElements.into(TypeDescriptors.strings())
                        .via(SerializableFunction { leadStr: String -> Klaxon().parse<Lead>(leadStr.toString())?.age.toString() }))
                .apply("WriteToStorage",
                        TextIO.write()
                                .to(options.output)
                                .withSuffix(".txt")
                                .withNumShards(1))

        pipeline.run()
    }
}