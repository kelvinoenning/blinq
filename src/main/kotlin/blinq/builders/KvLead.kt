package blinq.builders

import blinq.models.Lead
import com.google.api.services.bigquery.model.TableRow
import org.apache.avro.data.Json
import org.apache.beam.sdk.transforms.DoFn
import java.time.LocalDate
import java.time.Month
import java.time.Period
import java.time.format.DateTimeFormatter
import com.beust.klaxon.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptor

class KvLead : DoFn<Lead, KV<String, Int>>() {
    @ProcessElement
    fun processElement(context: ProcessContext ) {
        val l = context.element()
        context.output(KV.of(l.state.toString(), l.age))
    }
}