package blinq

import com.catalyst.aurora.builders.RowBuilder
import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableSchema
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.ParDo

object PubSubToRow {
    fun getTableSpec(): TableReference? {
        return TableReference()
                  .setProjectId("synthesishq")
                  .setDatasetId("analytics")
                  .setTableId("events")
    }

    fun getSchema(): TableSchema {
        val fields = listOf<TableFieldSchema>()
        fields.plus(TableFieldSchema().setName("lead_id").setType("LONG"))
        fields.plus(TableFieldSchema().setName("workflow_uuid").setType("STRING"))
        fields.plus(TableFieldSchema().setName("tenant_id").setType("LONG"))
        fields.plus(TableFieldSchema().setName("created_at").setType("DATETIME"))
        fields.plus(TableFieldSchema().setName("attribution").setType("STRING"))

        return TableSchema().setFields(fields)
    }

    @JvmStatic
    fun main(args : Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(PubSubToRowOptions::class.java)
        val pipeline = Pipeline.create(options)

        pipeline.apply("ReadPubSubEvents", PubsubIO.readStrings().fromTopic(options.topic))
                .apply("ConvertBigQueryRow", ParDo.of(RowBuilder()))
                .apply("SinkToBigQuery",
                        BigQueryIO.writeTableRows()
                            .to(getTableSpec())
                            .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
                            .withSchema(getSchema()))
        pipeline.run()
    }
}