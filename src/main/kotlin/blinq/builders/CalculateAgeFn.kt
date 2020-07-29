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

class CalculateAgeFn : DoFn<TableRow, Lead>() {
    @ProcessElement
    fun processElement(context: ProcessContext ) {
        val row = context.element()

        val email = row.getOrDefault("email", "default@default.com")
        val date = row.getOrDefault("date", "1900-01-01")
        val state = row.getOrDefault("state", "default")

        val today = LocalDate.now() //Today's date

        var split = date.toString().split("-")
        val birthday: LocalDate = LocalDate.of(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2].split(" ")[0])) //Birth date
        val p: Period = Period.between(birthday, today)

        val lead = Lead(email.toString(), p.years, state.toString())
        context.output(lead)
//        context.output(Klaxon().toJsonString(lead))
    }
}