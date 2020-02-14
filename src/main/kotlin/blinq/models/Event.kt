package blinq.models

import com.beust.klaxon.Json

data class Event(
    @Json(name="tenant_id")
    val tenantId: Long,

    @Json(name="lead_id")
    val leadId: Long,

    @Json(name="attribution")
    val attribution: String,

    @Json(name="created_at")
    val eventDate: String,

    @Json(name="utm")
    val utm: String,

    @Json(name="workflow_uuid")
    val workflowId: String,

    @Json(name="campaign")
    val campaign: String
)