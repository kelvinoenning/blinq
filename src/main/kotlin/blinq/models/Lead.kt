package blinq.models

import com.beust.klaxon.Json
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.coders.KvCoder

@DefaultCoder(AvroCoder::class)
data class Lead(
        @Json(name="email")
        val email: String? = "",

        @Json(name="age")
        val age: Int? = 0,

        @Json(name="state")
        val state: String? = ""
)