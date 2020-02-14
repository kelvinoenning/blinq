package blinq.builders

import com.beust.klaxon.Klaxon
import com.catalyst.aurora.models.Event
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.transforms.DoFn

class RowBuilder: DoFn<String, TableRow>() {
    @ProcessElement
    fun processElement(context: ProcessContext ) {
        val line = context.element()
        val event = Klaxon().parse<Event>(line.toString())

        val row = TableRow()
                    .set("lead_id", event?.leadId)
                    .set("workflow_uuid", event?.workflowId)
                    .set("tenant_id", event?.tenantId)
                    .set("attribution", event?.attribution)
                    .set("created_at", event?.eventDate)
                    .set("utm", event?.utm)

        context.output(row)
    }
}