# CCE Collector Service — Data Dictionary

Consolidated reference for all database tables, columns, enums, CloudEvents fields, and Kafka message fields.

---

## 1. Database Tables

### 1.1 `inbound_event_log` — Request Audit Log & Rejection Tracking

Every HTTP request is persisted **as-is** before processing. Used for audit trail, primary deduplication, and rejection tracking. Rejected events are recorded directly on this table. The full CloudEvents payload is stored in `raw_payload` (JSONB) — individual CloudEvents fields are not denormalized into separate columns.

**Migrations:** `V1__create_inbound_event_log.sql`, `V2__add_event_time_to_inbound_event_log.sql`

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `UUID` | No | — | Primary key (UUIDv7, application-generated, time-ordered) |
| `cloudevents_id` | `VARCHAR(50)` | No | — | CloudEvents `id` from the source system |
| `source` | `VARCHAR(100)` | No | — | CloudEvents `source` (e.g., `rhie-mediator`, `ebuzima/kigali-south`) |
| `correlation_id` | `VARCHAR(100)` | Yes | — | Distributed tracing ID |
| `raw_payload` | `JSONB` | No | — | Full original request body (immutable) — contains all CloudEvents fields |
| `status` | `VARCHAR(20)` | No | `'RECEIVED'` | Processing status (see `InboundStatus` enum) |
| `rejection_reason` | `VARCHAR(50)` | Yes | — | Rejection reason code (if status = `REJECTED`; see `RejectionReason` enum) |
| `error_details` | `TEXT` | Yes | — | Stack trace or validation error messages |
| `event_time` | `TIMESTAMPTZ` | Yes | — | Clinical occurrence time — when the clinical act actually happened, extracted from the FHIR payload via `ClinicalEventTimeExtractor`. Falls back to the CloudEvents envelope `time`, then to `received_at`, when no clinical field is present (see §8). |
| `received_at` | `TIMESTAMPTZ` | No | `now()` | Server-side receipt timestamp (UTC) |
| `updated_at` | `TIMESTAMPTZ` | No | `now()` | Last modification timestamp (auto-updated via `@PreUpdate`) |

**Constraints:**
- `PK`: `id`
- `UNIQUE`: `(cloudevents_id, source)` — primary deduplication key

**Indexes:**
| Index | Columns | Purpose |
|-------|---------|---------|
| `uq_inbound_event_log_id_source` | `(cloudevents_id, source)` | Deduplication (unique constraint) |
| `idx_inbound_event_log_dedup` | `(cloudevents_id, source, received_at)` | Dedup lookback query — covers `existsBy...ReceivedAtAfter` (index-only scan) |
| `idx_inbound_event_log_source` | `source` | Source-filtered queries |
| `idx_inbound_event_log_status` | `status` | Status-based filtering and `countByStatus` queries |
| `idx_inbound_event_log_received` | `received_at` | Time-range queries |
| `idx_inbound_event_log_event_time` | `event_time` | Query/order by real-world clinical time rather than ingestion time |


---

## 2. Enum Values

### 2.1 `InboundStatus`

Status of an `inbound_event_log` record as it moves through the pipeline.

| Value | Description |
|-------|-------------|
| `RECEIVED` | Initial state — event persisted, not yet processed |
| `ACCEPTED` | Validation passed, event published to Kafka |
| `REJECTED` | Validation or Kafka publish failed — see `rejection_reason` for cause |
| `DUPLICATE` | Event already seen (same `cloudevents_id` + `source`) |

### 2.2 `RejectionReason`

Reason an event was rejected (stored on `inbound_event_log.rejection_reason`).

| Value | Description |
|-------|-------------|
| `INVALID_ENVELOPE` | Missing or invalid CloudEvents required fields (including missing `type`) |
| `INVALID_FHIR` | FHIR R4 payload failed structural validation (only when `datacontenttype` = `application/fhir+json`) |
| `INVALID_JSON` | Non-FHIR JSON payload is not valid JSON or is empty (when `datacontenttype` = `application/json`) |
| `UNSUPPORTED_CONTENT_TYPE` | `datacontenttype` is not `application/fhir+json` or `application/json` |
| `DUPLICATE` | Duplicate `(id, source)` detected within lookback window |
| `MISSING_SUBJECT` | `subject` field missing (required by CCE for patient routing) |
| `PAYLOAD_TOO_LARGE` | Request body exceeds `max-payload-size` (default 1 MB) |
| `DESERIALIZATION_ERROR` | Request body could not be parsed as JSON |
| `KAFKA_PUBLISH_FAILURE` | Kafka broker unavailable or publish timed out — HTTP 500 returned to caller |
| `INTERNAL_ERROR` | Unexpected failure during post-persist processing (safety net for orphaned RECEIVED records) |

---

## 3. CloudEvents Fields (Inbound HTTP)

Inbound requests use **lowercase** field names per the CloudEvents v1.0 specification.

### 3.1 Required Fields

| Field | Type | Validation | Example |
|-------|------|------------|---------|
| `specversion` | `string` | Must be `"1.0"` | `"1.0"` |
| `id` | `string` | Non-empty, max 50 chars | `"evt-eb010001-0001-4000-8000-000000000001"` |
| `source` | `string` | Non-empty URI or short identifier | `"rhie-mediator"` |
| `type` | `string` | Non-empty (mandatory CloudEvents attribute, no format restriction) | `"org.openphc.cce.encounter"` |
| `subject` | `string` | Non-empty patient UPID | `"260225-0002-5501"` |
| `data` | `object` | Valid JSON; FHIR validation applied only when `datacontenttype` = `application/fhir+json` | `{ "resourceType": "Encounter", ... }` |
| `datacontenttype` | `string` | Non-empty; must be `application/fhir+json` or `application/json` | `"application/fhir+json"` |

### 3.2 Recommended Fields

| Field | Type | Default if Absent | Example |
|-------|------|-------------------|---------|
| `time` | `string` | Server `received_at` | `"2026-02-25T08:00:00Z"` |
| `facilityid` | `string` | — | `"0002"` |
| `correlationid` | `string` | Generated `corr-<uuid>` | `"corr-1343872c-636d-506f-b041-1e571d426932"` |

### 3.3 Optional Extension Fields

| Field | Type | Description |
|-------|------|-------------|
| `sourceeventid` | `string` | Source system's internal event ID |
| `protocolinstanceid` | `string` | Pre-populated if source knows the target protocol instance |
| `protocoldefinitionid` | `string` | Pre-populated if source knows the target protocol |
| `actionid` | `string` | Pre-populated if source knows the target action/step |

---

## 4. Kafka Message Fields

Published to `cce.events.inbound` using **CloudEvents spec field names (lowercase)** — no field name translation is performed by the Collector. The Kafka message is the `EventIngestionRequest` DTO (enriched with server-side defaults), not the database entity.

| Field | Type | Nullable | Source |
|-------|------|----------|--------|
| `id` | `String` | No | Request `id` (CloudEvents id) |
| `source` | `String` | No | Request `source` |
| `type` | `String` | No | Request `type` (validated, passed through unchanged) |
| `specversion` | `String` | No | Always `"1.0"` |
| `subject` | `String` | No | Request `subject` — also the Kafka message key |
| `time` | `String` | No | Request `time` (enriched from server `receivedAt` if absent) |
| `datacontenttype` | `String` | No | Request `datacontenttype` |
| `correlationid` | `String` | No | Request `correlationid` (enriched as `corr-<uuid>` if absent) |
| `sourceeventid` | `String` | Yes | Request `sourceeventid` |
| `protocolinstanceid` | `String` | Yes | (usually null — Matcher Service resolves) |
| `protocoldefinitionid` | `String` | Yes | (usually null — Matcher Service resolves) |
| `actionid` | `String` | Yes | (usually null — Matcher Service resolves) |
| `facilityid` | `String` | Yes | Request `facilityid` |
| `data` | `JsonNode` | No | Request `data` (FHIR R4 resource or JSON object) |

---

## 5. Field Name Reference (HTTP → Database)

The Collector preserves CloudEvents lowercase field names end-to-end (HTTP → Kafka). The database promotes only three fields to dedicated columns (for deduplication and tracing queries); the entire request is serialized as-is into `raw_payload` (JSONB) with no enrichment.

| HTTP / Kafka Field (lowercase) | Database Column | Stored in DB? |
|-------------------------------|----------------|---------------|
| `id` | `cloudevents_id` | Yes — dedicated column (deduplication key) |
| `source` | `source` | Yes — dedicated column (deduplication key) |
| `correlationid` | `correlation_id` | Yes — dedicated column (tracing) |
| *(entire request)* | `raw_payload` | Yes — all CloudEvents fields serialized as-is |
| `specversion` | — | No (in `raw_payload`) |
| `type` | — | No (in `raw_payload`) |
| `subject` | — | No (in `raw_payload`) |
| `time` | — | No (in `raw_payload`) |
| `datacontenttype` | — | No (in `raw_payload`) |
| `data` | — | No (in `raw_payload`) |
| `facilityid` | — | No (in `raw_payload`) |
| `sourceeventid` | — | No (in `raw_payload`) |
| `protocolinstanceid` | — | No (in `raw_payload`) |
| `protocoldefinitionid` | — | No (in `raw_payload`) |
| `actionid` | — | No (in `raw_payload`) |

---

## 6. `type` Field

The `type` field is a mandatory CloudEvents v1.0 attribute. The Collector validates only that it is **present and non-empty** — it does not enforce any specific format, pattern, or whitelist. The emitter adaptor (openHIM mediator) sets the value; the Collector passes it through to Kafka unchanged.

> **Tier 1 structural matching** in the Matcher Service uses `data.resourceType` (payload), not the envelope `type`.

---

## 7. FHIR Resource Types

The Collector accepts any valid FHIR R4 resource when `datacontenttype` is `application/fhir+json`. These are the resource types commonly used in the RHIE clinical workflow:

| Resource Type | Clinical Context |
|---------------|------------------|
| `Encounter` | Visit registration, consultations, transfers |
| `Observation` | Vital signs, lab results, chief complaints |
| `Condition` | Diagnoses |
| `MedicationRequest` | Prescriptions (e-Prescription) |
| `MedicationDispense` | Pharmacy dispensing |
| `MedicationAdministration` | Medication given to patient |
| `ServiceRequest` | Lab orders, imaging orders, referrals |
| `Procedure` | Clinical procedures |
| `Immunization` | Vaccinations |
| `AllergyIntolerance` | Allergy records |
| `ImagingStudy` | Imaging results (DICOM) |
| `DiagnosticReport` | Lab and imaging reports |
| `Consent` | Patient consent records |
| `EpisodeOfCare` | Care episodes |
| `CarePlan` | Treatment plans |
| `Patient` | Patient demographics (UPID extracted from `identifier[]` matching configured system, fallback to `Patient.id`) |
| `RelatedPerson` | Related persons (UPID extracted from `patient` reference) |

---

## 8. Clinical Event Time Extraction (`event_time`)

The `event_time` column captures the **clinical occurrence time** — when the clinical act actually happened — as opposed to `received_at` (ingestion time) or the CloudEvents envelope `time` (the emitter adaptor's transmission clock). Basing downstream scheduling on clinical time means ingestion lag (offline sync, batch upload, retries, DLQ replay) does not shift due/overdue/missed dates in the Matcher Service.

`event_time` is populated during enrichment (`EventDefaultsEnricher`) with the following **precedence** — the first that resolves wins:

1. **Clinical time** extracted from the FHIR `data` payload by `ClinicalEventTimeExtractor` (see mapping below).
2. **CloudEvents envelope `time`** (already filled from `received_at` when the source omits it).
3. **`received_at`** (server receipt timestamp).

Extraction is **best-effort**: an unmapped resource type, a missing field, or an unparseable value yields `null` and the fallback chain takes over — so populating `event_time` can only ever *improve* accuracy where a clinical field is present, never regress or fail ingestion.

FHIR has no single "when did this happen" field; each resource type carries its own, and most are polymorphic choice types (`effective[x]`, `performed[x]`, `occurrence[x]`). The extractor probes an ordered list of concrete JSON fields per resource type and takes the first that parses. A `Period`'s `end` says when the encounter *finished* rather than when the clinical act occurred, so it is the **last resort** in each list — tried only after every other field, including that same Period's `start`.

The mapping is `ClinicalEventTimeExtractor` in
[cce-common-util](../../cce-common-util/src/main/java/org/openphc/cce/common/fhir/ClinicalEventTimeExtractor.java),
shared with the Matcher Service. That matters more than it looks: this service stamps `event_time` from
it, and the Matcher Service derives a completed step's `completed_at` — and so its SLA verdict — from
the same reading of the same payload. The two services held separate copies until 2.0.0, and they had
drifted: an `Encounter` carrying both bounds was recorded here at `period.end` and judged there at
`period.start`, so the audit trail and the SLA clock disagreed about when the visit happened. The
candidate order below is the reconciled one; sharing the class is what stops it drifting again.

| FHIR Resource Type | Candidate fields (in priority order) |
|--------------------|--------------------------------------|
| `Observation` | `effectiveDateTime` → `effectiveInstant` → `effectivePeriod.start` → `issued` → `effectivePeriod.end` |
| `Encounter` | `period.start` → `period.end` |
| `Procedure` | `performedDateTime` → `performedPeriod.start` → `performedPeriod.end` |
| `Immunization` | `occurrenceDateTime` |
| `MedicationAdministration` | `effectiveDateTime` → `effectivePeriod.start` → `effectivePeriod.end` |
| `Condition` | `onsetDateTime` → `onsetPeriod.start` → `recordedDate` |
| `ServiceRequest` | `occurrenceDateTime` → `authoredOn` → `occurrencePeriod.end` |
| `DiagnosticReport` | `effectiveDateTime` → `issued` → `effectivePeriod.end` |

A `Period`'s `end` bound is always the last-resort candidate in each row above — it reflects "when it finished," not when the clinical act occurred, so every other field (including that same Period's `start`, where present) is tried first.

Dates are parsed leniently via HAPI FHIR's `DateTimeType`, which handles partial precision (`2026`, `2026-03`, `2026-03-15`) as well as full timestamps with offsets. Values are normalized to UTC.

**Metrics** (Micrometer counters):
- `cce.clinical_time.unmapped{resourceType}` — resource type has no candidate mapping.
- `cce.clinical_time.unparseable{resourceType}` — a candidate field was present but could not be parsed.

> **Note:** `event_time` is a persistence-only column on `inbound_event_log`. It is **not** added to the Kafka message (§4) — downstream consumers derive clinical time from the FHIR `data` payload themselves.

---

## 9. Migrations

| Version | File | Description |
|---------|------|-------------|
| V1 | `V1__create_inbound_event_log.sql` | `inbound_event_log` table with dedup constraint + rejection tracking columns |
| V2 | `V2__add_event_time_to_inbound_event_log.sql` | Adds `event_time` column (clinical occurrence time) + `idx_inbound_event_log_event_time` index |
