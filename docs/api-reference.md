# CCE Collector Service — API Reference

## Base URL

```
http://<host>:8080/v1
```

All endpoints return JSON. Standard response envelopes are used for success (`ApiResponse`) and error (`ApiError`) responses.

---

## 1. Event Ingestion

### POST /v1/events

Ingest a single CloudEvents-formatted clinical event.

**Content-Type:** `application/json`

#### Request Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `specversion` | `string` | Yes | Must be `"1.0"` |
| `id` | `string` | Yes | Unique event identifier from source (max 50 chars) |
| `source` | `string` | Yes | Event source URI (e.g., `ebuzima/kigali-south`) |
| `type` | `string` | Yes | Event type (e.g., `cce.encounter.created`) |
| `subject` | `string` | Yes | Patient UPID (e.g., `260225-0002-5501`) |
| `time` | `string` | No | ISO-8601 timestamp (filled by server if absent) |
| `datacontenttype` | `string` | Yes | MIME type of data (`application/fhir+json` or `application/json`) |
| `data` | `object` | Yes | Event payload (FHIR R4 resource) |
| `correlationid` | `string` | No | Trace correlation ID (generated if absent) |
| `facilityid` | `string` | No | Facility identifier |
| `sourceeventid` | `string` | No | Source-system internal event identifier |
| `protocolinstanceid` | `string` | No | Protocol instance reference (set by Matcher Service) |
| `protocoldefinitionid` | `string` | No | Protocol definition reference (set by Matcher Service) |
| `actionid` | `string` | No | Action/step reference (set by Matcher Service) |

> **Note:** Field names use **lowercase** as per CloudEvents HTTP binding specification. Any fields not in the above list are captured via `@JsonAnySetter` as extension attributes.

#### Example Request

```json
{
  "specversion": "1.0",
  "id": "evt-encounter-2024-001234",
  "source": "ebuzima/kigali-south",
  "type": "cce.encounter.created",
  "subject": "260225-0002-5501",
  "time": "2025-01-15T09:30:00Z",
  "datacontenttype": "application/fhir+json",
  "facilityid": "facility/FAC-KGL-S-001",
  "correlationid": "corr-abc123-def456",
  "data": {
    "resourceType": "Encounter",
    "id": "enc-2024-001234",
    "status": "finished",
    "class": {
      "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
      "code": "AMB",
      "display": "ambulatory"
    },
    "subject": {
      "reference": "Patient/260225-0002-5501"
    },
    "period": {
      "start": "2025-01-15T09:00:00Z",
      "end": "2025-01-15T09:30:00Z"
    }
  }
}
```

#### Responses

**202 Accepted** — Event accepted and queued for processing

```json
{
  "data": {
    "eventId": "evt-encounter-2024-001234",
    "status": "accepted",
    "correlationId": "corr-abc123-def456",
    "timestamp": "2025-01-15T09:30:05Z"
  }
}
```

**200 OK** — Duplicate event (idempotent response)

```json
{
  "data": {
    "eventId": "evt-encounter-2024-001234",
    "status": "duplicate",
    "correlationId": "corr-abc123-def456",
    "timestamp": "2025-01-15T09:30:05Z"
  }
}
```

**400 Bad Request** — CloudEvents envelope validation failure

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "CloudEvents validation failed: id is required"
  }
}
```

**422 Unprocessable Entity** — FHIR payload validation failure

```json
{
  "error": {
    "code": "PAYLOAD_VALIDATION_ERROR",
    "message": "FHIR validation failed: Unable to parse FHIR resource"
  }
}
```

---

## 2. Health & Actuator Endpoints

Standard Spring Boot Actuator endpoints:

| Endpoint | Purpose |
|----------|---------|
| `GET /actuator/health` | Application health (UP/DOWN) |
| `GET /actuator/health/liveness` | Kubernetes liveness probe |
| `GET /actuator/health/readiness` | Kubernetes readiness probe |
| `GET /actuator/info` | Application metadata |
| `GET /actuator/prometheus` | Prometheus-formatted metrics |
| `GET /actuator/metrics` | Micrometer metrics index |

---

## 3. Response Envelopes

### Success Envelope (`ApiResponse`)

Wraps an `EventIngestionResponse` payload in a standard `{"data": ...}` envelope.

```json
{
  "data": { ... }
}
```

### Error Envelope (`ApiError`)

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable description"
  }
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | CloudEvents envelope validation failed |
| `PAYLOAD_VALIDATION_ERROR` | 422 | FHIR payload failed structural validation |
| `DUPLICATE_EVENT` | 200 | Event already received (idempotent) |
| `KAFKA_PUBLISH_FAILURE` | 500 | Failed to publish to Kafka — source system should retry |
| `INTERNAL_ERROR` | 500 | Unexpected server error |
| `NOT_FOUND` | 404 | Resource not found |

---

## 4. CloudEvents Field Name Conventions

The Collector preserves **CloudEvents spec field names (lowercase)** end-to-end — from HTTP inbound through to the Kafka message. No field name translation is performed.

Multi-word field names follow the CloudEvents convention of concatenated lowercase (e.g., `specversion`, `datacontenttype`, `correlationid`).

---

## 5. Validation Rules Summary

### CloudEvents Envelope

| Field | Rule |
|-------|------|
| `specversion` | Must be `"1.0"` |
| `id` | Required, non-blank, max 50 characters |
| `source` | Required, non-blank |
| `type` | Required, non-blank |
| `subject` | Required, non-blank (CCE-specific requirement — patient UPID) |
| `data` | Required, non-null |

### FHIR Payload (when `datacontenttype` = `application/fhir+json`)

| Check | Severity |
|-------|----------|
| Valid JSON structure | Error → reject |
| `resourceType` present and a recognized FHIR R4 resource type | Error → reject |
| HAPI FHIR parseable | Error → reject |
| `subject.reference` matches `subject` field | Error → reject |

---

## 6. Rate Limits & Constraints

| Constraint | Value | Env Var |
|-----------|-------|--------|
| Max event ID length | 50 characters | — |
| Dedup lookback window | 30 days (configurable) | `CCE_COLLECTOR_DEDUP_LOOKBACK_DAYS` |
| Kafka publish retries | 3 (producer-level) | `SPRING_KAFKA_PRODUCER_RETRIES` |
| Kafka publish timeout | 30 seconds | `CCE_COLLECTOR_KAFKA_PUBLISH_TIMEOUT_SECONDS` |
| Max payload size | 1 MB | `CCE_COLLECTOR_MAX_PAYLOAD_SIZE` |
