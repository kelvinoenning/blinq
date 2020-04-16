package blinq.models

import com.beust.klaxon.Json

data class Lead(
        @Json(name="email")
        val email: String? = "",

        @Json(name="age")
        val age: Int? = 0,

        @Json(name="state")
        val state: String? = ""
)